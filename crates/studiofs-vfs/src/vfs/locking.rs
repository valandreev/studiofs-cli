use bytes::Buf;
use opendal::options::WriteOptions;
use opendal::{ErrorKind, Operator};
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;
use studiofs_meta::MetaEngine;
use studiofs_meta::types::Ino;

use super::{StudioVfs, VfsError, VfsResult};
use crate::runtime::run_meta;
use crate::utils::now_unix_secs;

#[derive(Clone)]
pub(super) struct DistributedFlock {
    pub(super) op: Operator,
    pub(super) prefix: String,
    pub(super) client_id: String,
    pub(super) lease_ttl: Duration,
    pub(super) retry_base_ms: u64,
    pub(super) retry_max_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub(super) struct RemoteLockRecord {
    pub(super) lease_until_unix: i64,
    pub(super) exclusive_owner: Option<String>,
    pub(super) shared_owners: BTreeSet<String>,
}

/// File-level lock kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    /// Shared (read) lock.
    Shared,
    /// Exclusive (write) lock.
    Exclusive,
    /// Unlock.
    Unlock,
}

/// Internal state for a file-level lock.
#[derive(Debug)]
pub(super) struct FileLock {
    kind: LockKind,
    owner: u64,
    /// Number of shared locks held (only meaningful when `kind == Shared`).
    shared_count: u32,
}

pub(super) struct FileLockState {
    pub(super) file_locks: Mutex<HashMap<Ino, FileLock>>,
    pub(super) lock_notify: tokio::sync::Notify,
}

impl FileLockState {
    pub(super) fn new() -> Self {
        Self {
            file_locks: Mutex::new(HashMap::new()),
            lock_notify: tokio::sync::Notify::new(),
        }
    }
}

fn lock_record_path(cfg: &DistributedFlock, ino: Ino) -> String {
    format!("{}{}.lock", cfg.prefix, ino)
}

fn encode_remote_lock_record(state: &RemoteLockRecord) -> Vec<u8> {
    let exclusive = state.exclusive_owner.as_deref().unwrap_or("-");
    let shared = if state.shared_owners.is_empty() {
        "-".to_string()
    } else {
        state
            .shared_owners
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(",")
    };
    format!("{}\n{}\n{}\n", state.lease_until_unix, exclusive, shared).into_bytes()
}

fn decode_remote_lock_record(bytes: &[u8]) -> Option<RemoteLockRecord> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.lines();
    let lease_until_unix = lines.next()?.trim().parse::<i64>().ok()?;
    let exclusive_raw = lines.next().unwrap_or("-").trim();
    let shared_raw = lines.next().unwrap_or("-").trim();
    let exclusive_owner = if exclusive_raw.is_empty() || exclusive_raw == "-" {
        None
    } else {
        Some(exclusive_raw.to_string())
    };
    let mut shared_owners = BTreeSet::new();
    if !shared_raw.is_empty() && shared_raw != "-" {
        for item in shared_raw.split(',') {
            let v = item.trim();
            if !v.is_empty() {
                shared_owners.insert(v.to_string());
            }
        }
    }
    Some(RemoteLockRecord {
        lease_until_unix,
        exclusive_owner,
        shared_owners,
    })
}

fn normalize_remote_lock_state(state: &mut RemoteLockRecord, now: i64) {
    if state.lease_until_unix <= now {
        state.lease_until_unix = 0;
        state.exclusive_owner = None;
        state.shared_owners.clear();
    }
}

impl StudioVfs {
    pub(super) async fn acquire_distributed_flock(
        &self,
        cfg: &DistributedFlock,
        ino: Ino,
        op: LockKind,
    ) -> VfsResult<bool> {
        let path = lock_record_path(cfg, ino);
        let now = now_unix_secs();
        let lease_until = now.saturating_add(cfg.lease_ttl.as_secs() as i64);

        let mut retries = 0u32;
        loop {
            let mut retry_later = false;
            let (mut state, etag_opt) = match cfg.op.stat(&path).await {
                Ok(meta) => {
                    let etag = meta.etag().map(ToString::to_string);
                    match cfg.op.read(&path).await {
                        Ok(raw) => {
                            let parsed = {
                                let raw_chunk = raw.chunk();
                                if raw_chunk.len() == raw.remaining() {
                                    decode_remote_lock_record(raw_chunk)
                                } else {
                                    let raw = raw.to_bytes();
                                    decode_remote_lock_record(&raw)
                                }
                            }
                            .unwrap_or_default();
                            (parsed, etag)
                        }
                        Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                            retry_later = true;
                            (RemoteLockRecord::default(), None)
                        }
                        Err(err) => {
                            return Err(VfsError::Other(format!("lock read failed: {err}")));
                        }
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    (RemoteLockRecord::default(), None)
                }
                Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                    retry_later = true;
                    (RemoteLockRecord::default(), None)
                }
                Err(err) => return Err(VfsError::Other(format!("lock stat failed: {err}"))),
            };
            if retry_later {
                retries = retries.saturating_add(1);
                if retries >= 12 {
                    return Ok(false);
                }
                let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                    .min(cfg.retry_max_ms);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }

            normalize_remote_lock_state(&mut state, now);
            let mut next_state = state.clone();
            let client = cfg.client_id.clone();

            let allowed = match op {
                LockKind::Shared => {
                    if let Some(excl) = &state.exclusive_owner {
                        excl == &client
                    } else {
                        true
                    }
                }
                LockKind::Exclusive => {
                    let excl_ok = state
                        .exclusive_owner
                        .as_ref()
                        .map(|v| v == &client)
                        .unwrap_or(true);
                    let shared_ok = state.shared_owners.is_empty()
                        || (state.shared_owners.len() == 1
                            && state.shared_owners.contains(&client));
                    excl_ok && shared_ok
                }
                LockKind::Unlock => true,
            };

            if !allowed {
                return Ok(false);
            }

            match op {
                LockKind::Shared => {
                    next_state.exclusive_owner = None;
                    next_state.shared_owners.insert(client);
                }
                LockKind::Exclusive => {
                    next_state.exclusive_owner = Some(client);
                    next_state.shared_owners.clear();
                }
                LockKind::Unlock => {
                    if next_state.exclusive_owner.as_deref() == Some(cfg.client_id.as_str()) {
                        next_state.exclusive_owner = None;
                    }
                    next_state.shared_owners.remove(cfg.client_id.as_str());
                }
            }
            next_state.lease_until_unix = lease_until;

            let mut opts = WriteOptions::default();
            if let Some(etag) = etag_opt {
                opts.if_match = Some(etag);
            } else {
                opts.if_not_exists = true;
            }

            let payload = encode_remote_lock_record(&next_state);
            match cfg.op.write_options(&path, payload, opts).await {
                Ok(_) => return Ok(true),
                Err(err) if err.kind() == ErrorKind::ConditionNotMatch => {
                    retries = retries.saturating_add(1);
                    if retries >= 12 {
                        return Ok(false);
                    }
                    let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                        .min(cfg.retry_max_ms);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                    retries = retries.saturating_add(1);
                    if retries >= 12 {
                        return Ok(false);
                    }
                    let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                        .min(cfg.retry_max_ms);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(err) => return Err(VfsError::Other(format!("lock write failed: {err}"))),
            }
        }
    }

    /// Acquire or release a file-level lock (flock semantics).
    ///
    /// - `Shared`: multiple readers allowed, blocks if exclusive lock held by another owner.
    /// - `Exclusive`: single writer, blocks if any lock held by another owner.
    /// - `Unlock`: release the lock.
    ///
    /// If `blocking` is false and the lock cannot be acquired, returns `VfsError::WouldBlock`.
    pub async fn flock(&self, ino: Ino, op: LockKind, owner: u64, blocking: bool) -> VfsResult<()> {
        loop {
            if op == LockKind::Unlock {
                let had_local_lock = {
                    let mut locks = self.state.file_lock.file_locks.lock();
                    let mut had = false;
                    if let Some(entry) = locks.get_mut(&ino) {
                        match entry.kind {
                            LockKind::Shared => {
                                had = true;
                                entry.shared_count = entry.shared_count.saturating_sub(1);
                                if entry.shared_count == 0 {
                                    locks.remove(&ino);
                                }
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                had = true;
                                locks.remove(&ino);
                            }
                            _ => {}
                        }
                    }
                    had
                };

                if had_local_lock {
                    if let Some(cfg) = &self.distributed_flock {
                        self.flush(ino).await?;
                        let meta = self.meta_async.clone();
                        run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                            engine.flush_metadata()?;
                            Ok(())
                        })
                        .await?;
                        let _ = self
                            .acquire_distributed_flock(cfg, ino, LockKind::Unlock)
                            .await?;
                    }
                    self.state.file_lock.lock_notify.notify_waiters();
                }
                return Ok(());
            }

            let local_allowed = {
                let locks = self.state.file_lock.file_locks.lock();
                match locks.get(&ino) {
                    None => true,
                    Some(entry) => match op {
                        LockKind::Shared => {
                            entry.kind == LockKind::Shared
                                || (entry.kind == LockKind::Exclusive && entry.owner == owner)
                        }
                        LockKind::Exclusive => {
                            entry.kind == LockKind::Unlock
                                || (entry.kind == LockKind::Exclusive && entry.owner == owner)
                        }
                        LockKind::Unlock => true,
                    },
                }
            };
            if !local_allowed {
                if !blocking {
                    return Err(VfsError::WouldBlock);
                }
                self.state.file_lock.lock_notify.notified().await;
                continue;
            }

            if let Some(cfg) = &self.distributed_flock {
                let acquired = self.acquire_distributed_flock(cfg, ino, op).await?;
                if !acquired {
                    if !blocking {
                        return Err(VfsError::WouldBlock);
                    }
                    tokio::time::sleep(Duration::from_millis(cfg.retry_base_ms)).await;
                    continue;
                }
                let meta = self.meta_async.clone();
                run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.sync_metadata()?;
                    Ok(())
                })
                .await?;
            }

            let committed = {
                let mut locks = self.state.file_lock.file_locks.lock();
                match op {
                    LockKind::Shared => {
                        let entry = locks.entry(ino).or_insert(FileLock {
                            kind: LockKind::Unlock,
                            owner: 0,
                            shared_count: 0,
                        });
                        match entry.kind {
                            LockKind::Unlock | LockKind::Shared => {
                                entry.kind = LockKind::Shared;
                                entry.shared_count = entry.shared_count.saturating_add(1);
                                true
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                entry.kind = LockKind::Shared;
                                entry.shared_count = 1;
                                true
                            }
                            _ => false,
                        }
                    }
                    LockKind::Exclusive => {
                        let entry = locks.entry(ino).or_insert(FileLock {
                            kind: LockKind::Unlock,
                            owner: 0,
                            shared_count: 0,
                        });
                        match entry.kind {
                            LockKind::Unlock => {
                                *entry = FileLock {
                                    kind: LockKind::Exclusive,
                                    owner,
                                    shared_count: 0,
                                };
                                true
                            }
                            LockKind::Exclusive if entry.owner == owner => true,
                            _ => false,
                        }
                    }
                    LockKind::Unlock => true,
                }
            };
            if committed {
                return Ok(());
            }

            if let Some(cfg) = &self.distributed_flock {
                let _ = self
                    .acquire_distributed_flock(cfg, ino, LockKind::Unlock)
                    .await;
            }
            if !blocking {
                return Err(VfsError::WouldBlock);
            }
            self.state.file_lock.lock_notify.notified().await;
        }
    }
}

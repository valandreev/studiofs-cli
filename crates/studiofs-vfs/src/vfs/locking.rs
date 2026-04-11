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
    /// Per-owner shared lock reference counts (only meaningful when `kind == Shared`).
    shared_counts: HashMap<u64, u32>,
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
                let (had_local_lock, release_remote_lock) = {
                    let mut locks = self.state.file_lock.file_locks.lock();
                    let mut had = false;
                    let mut release_remote = false;
                    if let Some(entry) = locks.get_mut(&ino) {
                        match entry.kind {
                            LockKind::Shared => {
                                if let Some(shared_count) = entry.shared_counts.get_mut(&owner) {
                                    had = true;
                                    *shared_count = shared_count.saturating_sub(1);
                                    if *shared_count == 0 {
                                        entry.shared_counts.remove(&owner);
                                    }
                                }
                                if entry.shared_counts.is_empty() {
                                    release_remote = true;
                                    locks.remove(&ino);
                                }
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                had = true;
                                release_remote = true;
                                locks.remove(&ino);
                            }
                            _ => {}
                        }
                    }
                    (had, release_remote)
                };

                if had_local_lock {
                    if release_remote_lock && let Some(cfg) = &self.distributed_flock {
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
                        LockKind::Exclusive => match entry.kind {
                            LockKind::Unlock => true,
                            LockKind::Exclusive => entry.owner == owner,
                            LockKind::Shared => {
                                entry.shared_counts.len() == 1
                                    && entry.shared_counts.contains_key(&owner)
                            }
                        },
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
                            shared_counts: HashMap::new(),
                        });
                        match entry.kind {
                            LockKind::Unlock | LockKind::Shared => {
                                entry.kind = LockKind::Shared;
                                entry.owner = 0;
                                entry
                                    .shared_counts
                                    .entry(owner)
                                    .and_modify(|count| *count = count.saturating_add(1))
                                    .or_insert(1);
                                true
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                entry.kind = LockKind::Shared;
                                entry.owner = 0;
                                entry.shared_counts.clear();
                                entry.shared_counts.insert(owner, 1);
                                true
                            }
                            _ => false,
                        }
                    }
                    LockKind::Exclusive => {
                        let entry = locks.entry(ino).or_insert(FileLock {
                            kind: LockKind::Unlock,
                            owner: 0,
                            shared_counts: HashMap::new(),
                        });
                        match entry.kind {
                            LockKind::Unlock => {
                                *entry = FileLock {
                                    kind: LockKind::Exclusive,
                                    owner,
                                    shared_counts: HashMap::new(),
                                };
                                true
                            }
                            LockKind::Shared
                                if entry.shared_counts.len() == 1
                                    && entry.shared_counts.contains_key(&owner) =>
                            {
                                entry.kind = LockKind::Exclusive;
                                entry.owner = owner;
                                entry.shared_counts.clear();
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

#[cfg(test)]
mod tests {
    use super::super::{DataStoreConfig, VfsConfig};
    use super::*;
    use std::sync::Arc;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("locking-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("locking-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        vfs.start().expect("start vfs background workers");
        (rt, vfs, meta)
    }

    #[test]
    fn shared_unlock_requires_matching_owner() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "shared-unlock-owner");
        let file = vfs
            .create(1, b"shared.txt", 1000, 1000, 0o644)
            .expect("create file");

        rt.block_on(async {
            vfs.flock(file.ino, LockKind::Shared, 1, false)
                .await
                .expect("owner 1 shared lock");
            vfs.flock(file.ino, LockKind::Shared, 2, false)
                .await
                .expect("owner 2 shared lock");

            vfs.flock(file.ino, LockKind::Unlock, 3, false)
                .await
                .expect("foreign unlock should be a no-op");

            let blocked = vfs.flock(file.ino, LockKind::Exclusive, 4, false).await;
            assert!(matches!(blocked, Err(VfsError::WouldBlock)));

            vfs.flock(file.ino, LockKind::Unlock, 1, false)
                .await
                .expect("owner 1 unlock");
            let still_blocked = vfs.flock(file.ino, LockKind::Exclusive, 4, false).await;
            assert!(matches!(still_blocked, Err(VfsError::WouldBlock)));

            vfs.flock(file.ino, LockKind::Unlock, 2, false)
                .await
                .expect("owner 2 unlock");
            vfs.flock(file.ino, LockKind::Exclusive, 4, false)
                .await
                .expect("exclusive lock after final shared unlock");
        });
    }

    #[test]
    fn exclusive_upgrade_requires_sole_shared_owner() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "shared-upgrade-owner");
        let file = vfs
            .create(1, b"upgrade.txt", 1000, 1000, 0o644)
            .expect("create file");

        rt.block_on(async {
            vfs.flock(file.ino, LockKind::Shared, 1, false)
                .await
                .expect("owner 1 shared lock");
            vfs.flock(file.ino, LockKind::Shared, 2, false)
                .await
                .expect("owner 2 shared lock");

            let blocked = vfs.flock(file.ino, LockKind::Exclusive, 1, false).await;
            assert!(matches!(blocked, Err(VfsError::WouldBlock)));

            vfs.flock(file.ino, LockKind::Unlock, 2, false)
                .await
                .expect("owner 2 unlock");
            vfs.flock(file.ino, LockKind::Exclusive, 1, false)
                .await
                .expect("owner 1 can upgrade once sole remaining shared owner");
        });
    }
}

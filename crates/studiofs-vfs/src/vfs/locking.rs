use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use studiofs_meta::types::Ino;
use studiofs_meta::{FileLockLeaseAcquireOutcome, HardLockKind};

use super::{StudioVfs, VfsError, VfsResult};
use crate::VfsMeta;
use crate::runtime::run_meta;

#[derive(Clone)]
pub(super) struct DistributedFlock {
    pub(super) lease_ttl: Duration,
    pub(super) retry_base_ms: u64,
    pub(super) retry_max_ms: u64,
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

impl StudioVfs {
    pub(super) async fn acquire_distributed_flock(
        &self,
        cfg: &DistributedFlock,
        ino: Ino,
        op: LockKind,
    ) -> VfsResult<bool> {
        let meta = self.meta.clone();
        match op {
            LockKind::Shared | LockKind::Exclusive => {
                let kind = match op {
                    LockKind::Shared => HardLockKind::Shared,
                    LockKind::Exclusive => HardLockKind::Exclusive,
                    LockKind::Unlock => unreachable!("unlock handled above"),
                };
                let lease_ttl = cfg.lease_ttl;
                let outcome = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.acquire_file_lock_lease(ino, kind, lease_ttl)
                })
                .await?;
                Ok(matches!(outcome, FileLockLeaseAcquireOutcome::Acquired(_)))
            }
            LockKind::Unlock => {
                run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.release_file_lock_lease(ino)
                })
                .await?;
                Ok(true)
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
                        let meta = self.meta.clone();
                        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
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
                    let retry_delay_ms = cfg.retry_base_ms.min(cfg.retry_max_ms);
                    tokio::time::sleep(Duration::from_millis(retry_delay_ms)).await;
                    continue;
                }
                let meta = self.meta.clone();
                run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
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

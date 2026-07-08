//! redb implementation of MetaEngine.
//!
//! Uses redb as an embedded ACID KV store for all filesystem metadata.

use crate::engine::*;
use crate::types::*;
use redb::{
    Database, Durability, ReadOnlyDatabase, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, WriteTransaction,
};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::ErrorKind;
use std::ops::Deref;
use std::path::Path;
use std::sync::{
    Arc, Once, OnceLock, RwLock, RwLockReadGuard,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;
use tracing::{debug, warn};

pub(crate) mod layout;
pub use self::layout::RedbSnapshot;
use self::layout::*;

mod fs_ops;

static RELAXED_DURABILITY_WARN: Once = Once::new();
static RELAXED_DURABILITY_CLOCK_START: OnceLock<Instant> = OnceLock::new();
static RELAXED_DURABILITY_NEXT_BARRIER_MS: AtomicU64 =
    AtomicU64::new(REDB_RELAXED_DURABILITY_BARRIER_INTERVAL_MS);
const REDB_RELAXED_DURABILITY_BARRIER_INTERVAL_MS: u64 = 5_000;

/// Metadata engine backed by redb.
pub struct RedbMetaEngine {
    db: Arc<RwLock<Database>>,
}

pub(crate) struct CounterWriteTransaction<'a> {
    txn: &'a WriteTransaction,
}

pub(crate) struct RedbWriteTransaction<'a> {
    txn: WriteTransaction,
    _db: RwLockReadGuard<'a, Database>,
}

fn redb_relaxed_durability_enabled() -> bool {
    #[cfg(target_os = "linux")]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            std::env::var("STUDIOFS_REDB_RELAXED_DURABILITY_UNSAFE")
                .ok()
                .is_some_and(|value| {
                    matches!(
                        value.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

fn redb_relaxed_durability() -> Durability {
    let start = RELAXED_DURABILITY_CLOCK_START.get_or_init(Instant::now);
    let elapsed_ms = start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    redb_relaxed_durability_for_elapsed_ms(&RELAXED_DURABILITY_NEXT_BARRIER_MS, elapsed_ms)
}

// Durability::Immediate makes earlier Durability::None commits persistent in redb.
// This keeps relaxed mode fast while bounding local metadata loss to the last interval.
fn redb_relaxed_durability_for_elapsed_ms(
    next_barrier_ms: &AtomicU64,
    elapsed_ms: u64,
) -> Durability {
    loop {
        let next = next_barrier_ms.load(Ordering::Relaxed);
        if elapsed_ms < next {
            return Durability::None;
        }
        let new_next = elapsed_ms.saturating_add(REDB_RELAXED_DURABILITY_BARRIER_INTERVAL_MS);
        if next_barrier_ms
            .compare_exchange(next, new_next, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return Durability::Immediate;
        }
    }
}

fn begin_redb_write_txn(db: &Database) -> MetaResult<WriteTransaction> {
    let mut txn = db
        .begin_write()
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    if redb_relaxed_durability_enabled() {
        RELAXED_DURABILITY_WARN.call_once(|| {
            warn!(
                interval_ms = REDB_RELAXED_DURABILITY_BARRIER_INTERVAL_MS,
                "STUDIOFS_REDB_RELAXED_DURABILITY_UNSAFE is enabled; most local redb commits skip durable flushes, with periodic durable barriers"
            );
        });
        txn.set_durability(redb_relaxed_durability())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
    }
    Ok(txn)
}

fn begin_redb_read_txn(db: &Database) -> MetaResult<ReadTransaction> {
    db.begin_read()
        .map_err(|e| MetaError::Storage(e.to_string()))
}

impl CounterWriteTransaction<'_> {
    pub(crate) fn set(&self, name: &str, value: u64) -> MetaResult<()> {
        let mut table = self
            .txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .insert(name, value)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }
}

impl Deref for RedbWriteTransaction<'_> {
    type Target = WriteTransaction;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl RedbWriteTransaction<'_> {
    pub(crate) fn commit(self) -> Result<(), redb::CommitError> {
        let Self { txn, _db } = self;
        txn.commit()
    }

    pub(crate) fn abort(self) -> Result<(), redb::StorageError> {
        let Self { txn, _db } = self;
        txn.abort()
    }
}

impl RedbMetaEngine {
    pub(crate) fn read_db(&self) -> MetaResult<RwLockReadGuard<'_, Database>> {
        self.db
            .read()
            .map_err(|_| MetaError::Storage("redb lock poisoned".to_string()))
    }

    pub(crate) fn begin_write_txn(&self) -> MetaResult<RedbWriteTransaction<'_>> {
        let db = self.read_db()?;
        let txn = begin_redb_write_txn(&db)?;
        Ok(RedbWriteTransaction { txn, _db: db })
    }

    /// Open or create a metadata database at the given path.
    pub fn open(path: impl AsRef<Path>) -> MetaResult<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                MetaError::Storage(format!("failed to create directory for redb: {e}"))
            })?;
        }

        let db = Database::create(path.as_ref())
            .map_err(|e| MetaError::Storage(format!("failed to open redb: {e}")))?;

        {
            let txn = begin_redb_write_txn(&db)?;
            let _ = txn.open_table(SETTINGS_TABLE);
            let _ = txn.open_table(COUNTERS_TABLE);
            let _ = txn.open_table(INODES_TABLE);
            let _ = txn.open_table(BASE_INODES_TABLE);
            let _ = txn.open_table(INODE_LIFECYCLE_TABLE);
            let _ = txn.open_table(INODE_TOMBSTONES_TABLE);
            let _ = txn.open_table(DENTRIES_TABLE);
            let _ = txn.open_table(DENTRIES_BY_INODE_TABLE);
            let _ = txn.open_table(BASE_DENTRIES_TABLE);
            let _ = txn.open_table(BASE_DENTRIES_BY_INODE_TABLE);
            let _ = txn.open_table(DENTRY_TOMBSTONES_TABLE);
            let _ = txn.open_table(REGULAR_FILE_OVERLAY_TABLE);
            let _ = txn.open_table(REGULAR_FILE_BASE_TABLE);
            let _ = txn.open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE);
            let _ = txn.open_table(PENDING_DELETIONS_TABLE);
            let _ = txn.open_table(PENDING_CREATES_TABLE);
            let _ = txn.open_table(DEFERRED_DELETES_TABLE);
            let _ = txn.open_table(UNDO_LOG_TABLE);
            let _ = txn.open_table(SYMLINKS_TABLE);
            let _ = txn.open_table(BASE_SYMLINKS_TABLE);
            let _ = txn.open_table(SYMLINK_TOMBSTONES_TABLE);
            let _ = txn.open_table(XATTRS_TABLE);
            let _ = txn.open_table(BASE_XATTRS_TABLE);
            let _ = txn.open_table(XATTR_TOMBSTONES_TABLE);
            let _ = txn.open_table(ACL_RULES_TABLE);
            let _ = txn.open_table(BASE_ACL_RULES_TABLE);
            let _ = txn.open_table(ACL_RULE_TOMBSTONES_TABLE);
            let _ = txn.open_table(EVENT_LOG_TABLE);
            let _ = txn.open_table(SNAPSHOTS_TABLE);
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        debug!("opened metadata database at {:?}", path.as_ref());
        let engine = Self {
            db: Arc::new(RwLock::new(db)),
        };
        engine.ensure_dentry_by_inode_indexes()?;
        let _ = engine.recover_undo_logs()?;

        match engine.get_format_info() {
            Ok(format_info) if format_info.format_version == CURRENT_FORMAT_VERSION => {
                engine.ensure_inode_lifecycle_table()?;
                debug!(
                    "format version {} is up to date",
                    format_info.format_version
                );
            }
            Ok(info) => {
                return Err(MetaError::Storage(format!(
                    "unsupported format version: {}",
                    info.format_version
                )));
            }
            Err(MetaError::NotFormatted) => {
                debug!("new database (not yet formatted) — skipping migration check");
            }
            Err(e) => return Err(e),
        }

        Ok(engine)
    }

    /// Initialize a fresh filesystem with root inode.
    /// `uid` and `gid` are used as the owner of the root directory.
    pub fn init_root(&self, uid: u32, gid: u32) -> MetaResult<()> {
        if self.get_attr(ROOT_INO).is_ok() {
            debug!("root inode already exists, skipping init");
            return Ok(());
        }

        self.set_counter(counters::NEXT_INODE, ROOT_INO + 1)?;
        self.set_counter(counters::TOTAL_INODES, 1)?;
        self.set_counter(counters::USED_SPACE, 0)?;

        let root = InodeAttr::new_dir(ROOT_INO, ROOT_INO, uid, gid, 0o755);
        self.set_attr(&root)?;
        debug!("initialized root inode with uid={}, gid={}", uid, gid);
        Ok(())
    }

    pub(crate) fn write_counters<T, F>(&self, f: F) -> MetaResult<T>
    where
        F: FnOnce(&CounterWriteTransaction<'_>) -> MetaResult<T>,
    {
        let txn = self.begin_write_txn()?;
        let counters = CounterWriteTransaction { txn: &txn };
        let out = f(&counters)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(out)
    }
}

mod acl;
mod chunk_state;
mod format;
mod gc;
mod inodes;
mod namespace;
mod payload_objects;
mod recovery;
mod replication;
mod snapshots;
mod symlink;
mod txn;
mod xattr;

impl MetaEngineCore for RedbMetaEngine {
    fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool> {
        Ok(true)
    }

    fn release_global_janitor_lock(&self) -> MetaResult<()> {
        Ok(())
    }

    fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        Ok(HashSet::new())
    }

    fn acquire_file_lock_lease(
        &self,
        _ino: Ino,
        _kind: HardLockKind,
        _lease_ttl: std::time::Duration,
    ) -> MetaResult<FileLockLeaseAcquireOutcome> {
        Err(MetaError::Storage(
            "file lock leases require postgres metadata backend".to_string(),
        ))
    }

    fn release_file_lock_lease(&self, _ino: Ino) -> MetaResult<FileLockLeaseReleaseOutcome> {
        Err(MetaError::Storage(
            "file lock leases require postgres metadata backend".to_string(),
        ))
    }

    fn renew_file_lock_lease(
        &self,
        _ino: Ino,
        _lease_ttl: std::time::Duration,
    ) -> MetaResult<FileLockLeaseRenewOutcome> {
        Err(MetaError::Storage(
            "file lock leases require postgres metadata backend".to_string(),
        ))
    }

    fn current_file_lock_lease(&self, _ino: Ino) -> MetaResult<Option<FileLockLease>> {
        Ok(None)
    }

    fn known_file_lock_lease(&self, _ino: Ino) -> Option<FileLockLease> {
        None
    }
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

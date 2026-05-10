//! redb implementation of MetaEngine.
//!
//! Uses redb as an embedded ACID KV store for all filesystem metadata.

use crate::engine::*;
use crate::types::*;
use redb::{
    Database, ReadOnlyDatabase, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use tracing::debug;

mod layout;
pub use self::layout::RedbSnapshot;
use self::layout::*;

mod fs_ops;

/// Metadata engine backed by redb.
pub struct RedbMetaEngine {
    db: Arc<Database>,
}

pub(crate) struct CounterWriteTransaction<'a> {
    txn: &'a WriteTransaction,
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

impl RedbMetaEngine {
    /// Opens a metadata write transaction with redb crash-safety enabled.
    ///
    /// StudioFS treats redb metadata as the local recovery source of truth, so
    /// every local metadata mutation must survive unclean shutdowns once it has
    /// been accepted by a VFS durability boundary.
    pub(crate) fn begin_crash_safe_write(&self) -> MetaResult<WriteTransaction> {
        let mut txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        txn.set_two_phase_commit(true);
        Ok(txn)
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
            let txn = db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = txn.open_table(SETTINGS_TABLE);
            let _ = txn.open_table(COUNTERS_TABLE);
            let _ = txn.open_table(INODES_TABLE);
            let _ = txn.open_table(BASE_INODES_TABLE);
            let _ = txn.open_table(INODE_LIFECYCLE_TABLE);
            let _ = txn.open_table(INODE_TOMBSTONES_TABLE);
            let _ = txn.open_table(DENTRIES_TABLE);
            let _ = txn.open_table(BASE_DENTRIES_TABLE);
            let _ = txn.open_table(DENTRY_BY_INODE_TABLE);
            let _ = txn.open_table(BASE_DENTRY_BY_INODE_TABLE);
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
        let engine = Self { db: Arc::new(db) };
        let _ = engine.recover_undo_logs()?;

        match engine.get_format_info() {
            Ok(format_info) if format_info.format_version >= CURRENT_FORMAT_VERSION => {
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
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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

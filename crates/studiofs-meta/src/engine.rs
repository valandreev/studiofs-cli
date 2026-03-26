//! MetaEngine trait — abstract interface for metadata storage.
//!
//! This trait defines all metadata operations. The default implementation is `RedbMetaEngine`,
//! but the design supports swapping to PostgreSQL/Redis for multi-user mode.

use crate::types::*;
use std::collections::HashSet;

// =============================================================================
// Batch flush types
// =============================================================================

/// Input for one block in a batched flush operation.
///
/// The caller pre-allocates `object_id` via [`MetaEngine::next_block_ids`]
/// before calling [`MetaEngine::flush_blocks_batch`].
#[derive(Debug, Clone)]
pub struct BatchFlushEntry {
    pub ino: Ino,
    pub block_index: u32,
    pub object_id: u64,
    pub length: u32,
    pub flags: BlockFlags,
}

/// Result for one block in a batched flush operation.
#[derive(Debug, Clone)]
pub struct BatchFlushResult {
    /// The old block reference if the slot was occupied (caller should GC it).
    pub old_block: Option<BlockRef>,
}

// =============================================================================
// Errors
// =============================================================================
#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("inode not found: {0}")]
    InodeNotFound(Ino),

    #[error("entry not found: {name:?} in parent {parent}")]
    EntryNotFound { parent: Ino, name: String },

    #[error("entry already exists: {name:?} in parent {parent}")]
    EntryExists { parent: Ino, name: String },

    #[error("not a directory: inode {0}")]
    NotADirectory(Ino),

    #[error("directory not empty: inode {0}")]
    DirNotEmpty(Ino),

    #[error("is a directory: inode {0}")]
    IsADirectory(Ino),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("format info not found — run `studiofs init` first")]
    NotFormatted,

    #[error("unsupported format version: found {found}, expected {expected}")]
    UnsupportedFormatVersion { found: u16, expected: u16 },

    #[error("xattr value too large: actual={actual} bytes, limit={limit} bytes")]
    XattrValueTooLarge { actual: usize, limit: usize },

    #[error("disk UUID mismatch: local={local}, remote={remote}")]
    UuidMismatch { local: String, remote: String },

    #[error(
        "access denied: ino={ino}, principal={principal}, required={required:?}, actual={actual:?}"
    )]
    AccessDenied {
        ino: Ino,
        principal: String,
        required: AccessLevel,
        actual: AccessLevel,
    },
}

pub type MetaResult<T> = Result<T, MetaError>;

/// `setxattr` flag: create attribute, fail if key already exists.
pub const XATTR_CREATE_FLAG: u32 = 1;
/// `setxattr` flag: replace attribute, fail if key does not exist.
pub const XATTR_REPLACE_FLAG: u32 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataReplicationBacklog {
    pub pending_ops: u32,
    pub inflight_ops: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeferredDeleteRecord {
    pub ino: Ino,
    pub parent: Ino,
    pub name: Vec<u8>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PendingCreateRecord {
    pub ino: Ino,
    pub parent: Ino,
    pub name: Vec<u8>,
    pub kind: FileType,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UndoLogKind {
    PendingCreate,
    DeferredDelete,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UndoLogPhase {
    #[default]
    Started,
    Replaying,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UndoLogRecord {
    pub op_id: u64,
    pub kind: UndoLogKind,
    #[serde(default)]
    pub phase: UndoLogPhase,
    pub ino: Ino,
    pub parent: Ino,
    pub name: Vec<u8>,
    pub created_at: i64,
}

impl MetadataReplicationBacklog {
    pub fn total_ops(self) -> u32 {
        self.pending_ops.saturating_add(self.inflight_ops)
    }
}

/// Strict backend contract.
///
/// This trait is intentionally comprehensive: storage backends implement it once,
/// while user-facing capability traits below expose focused method groups.
///
/// All operations are synchronous (redb is sync). Async callers should use
/// `AsyncMetaEngine` (for example `SyncMetaEngineAdapter`) to establish an
/// explicit async boundary.
pub trait MetaEngineCore: Send + Sync {
    // ─── Counters ────────────────────────────────────────────────────────

    /// Get a counter value, returns 0 if not set.
    fn get_counter(&self, name: &str) -> MetaResult<u64>;

    /// Increment a counter atomically, returns the new value.
    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64>;

    /// Decrement a counter atomically (saturating at 0), returns the new value.
    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64>;

    /// Set a counter to a specific value.
    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()>;

    // ─── Inodes ──────────────────────────────────────────────────────────

    /// Allocate a new inode number.
    fn next_inode(&self) -> MetaResult<Ino>;

    /// Get inode attributes.
    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr>;

    /// Batch-fetch inode attributes.
    ///
    /// Default implementation calls [`MetaEngine::get_attr`] for each inode in
    /// order and returns results in the same order as `inos`.
    fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>> {
        let mut attrs = Vec::with_capacity(inos.len());
        for &ino in inos {
            attrs.push(self.get_attr(ino)?);
        }
        Ok(attrs)
    }

    /// Set (create or update) inode attributes.
    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()>;

    /// Remove an inode's attributes.
    fn remove_attr(&self, ino: Ino) -> MetaResult<()>;

    /// Enumerate existing inode attributes.
    ///
    /// Default implementation scans inode ids in `[ROOT_INO, NEXT_INODE)`.
    /// Backends can override with a direct table scan for better performance.
    fn list_inode_attrs(&self) -> MetaResult<Vec<InodeAttr>> {
        let next = self.get_counter(counters::NEXT_INODE)?;
        let mut out = Vec::new();
        for ino in ROOT_INO..next {
            match self.get_attr(ino) {
                Ok(attr) => out.push(attr),
                Err(MetaError::InodeNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(out)
    }

    // ─── Directory Entries ───────────────────────────────────────────────

    /// Look up a child entry by name in a parent directory.
    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue>;

    /// List all entries in a directory.
    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>>;

    /// Add a directory entry. Returns error if it already exists.
    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()>;

    /// Remove a directory entry by name. Returns the removed entry.
    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue>;

    // ─── File Blocks (New Architecture) ──────────────────────────────────

    /// Get a specific block reference for a file.
    /// Key: (inode, block_index)
    fn get_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>>;

    /// Set a block reference for a file.
    /// Returns the old block reference if it existed (for GC marking).
    fn set_file_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>>;

    /// Get all block references for a file.
    fn get_file_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>>;

    /// Remove all blocks for a file (used when deleting).
    /// Returns the list of removed block IDs for GC.
    fn remove_file_blocks(&self, ino: Ino) -> MetaResult<Vec<u64>>;

    /// Remove a single block for a file (used in truncate).
    /// Returns the removed block ID if it existed.
    fn remove_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<u64>>;

    /// Allocate a new sequential block ID.
    fn next_block_id(&self) -> MetaResult<u64>;

    /// Allocate `count` consecutive block IDs in a single transaction.
    ///
    /// Returns `count` IDs starting from the current counter value.
    /// Default implementation calls `next_block_id()` repeatedly.
    fn next_block_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        let mut ids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            ids.push(self.next_block_id()?);
        }
        Ok(ids)
    }

    /// Flush a batch of block writes in a single transaction.
    ///
    /// For each entry:
    /// - Sets the new `BlockRef` in `FILE_BLOCKS`
    /// - If an old block existed at `(ino, block_index)`, returns it to the
    ///   caller so remote-visible replacement/GC can be decided later
    /// - Increments `USED_SPACE` by the new block's length
    ///
    /// Returns one [`BatchFlushResult`] per entry (same order).
    ///
    /// Default implementation calls individual methods sequentially.
    fn flush_blocks_batch(&self, entries: &[BatchFlushEntry]) -> MetaResult<Vec<BatchFlushResult>> {
        let mut results = Vec::with_capacity(entries.len());
        for e in entries {
            let block_ref = BlockRef {
                object_id: e.object_id,
                offset: 0,
                length: e.length,
                flags: e.flags,
            };
            let old = self.set_file_block(e.ino, e.block_index, &block_ref)?;
            if let Some(ref old_block) = old {
                self.decr_counter(counters::USED_SPACE, old_block.length as u64)?;
            }
            self.incr_counter(counters::USED_SPACE, e.length as u64)?;
            results.push(BatchFlushResult { old_block: old });
        }
        Ok(results)
    }

    /// Mark a block as remote-visible after its object upload has completed.
    ///
    /// Remote-backed engines can use this to publish block visibility and
    /// advance remote-visible file size independently from the local write view.
    fn commit_uploaded_block(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool>;

    /// Atomically publish the current file manifest as the new committed head.
    ///
    /// This switches the committed manifest at file granularity and updates the
    /// inode's coarse remote upload state to `ready` when successful.
    fn publish_file_manifest(&self, ino: Ino, expected_epoch: u64) -> MetaResult<bool>;

    // ─── Pending Deletions (GC) ───────────────────────

    /// Add a block to pending deletions (mark for GC).
    /// The timestamp is used for grace period.
    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()>;

    /// Remove a block from pending deletions (after GC).
    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()>;

    /// Get all pending deletions (object_id, timestamp).
    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>>;

    /// Get currently referenced object IDs from FILE_BLOCKS metadata.
    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>>;

    /// Replay pending create intents and heal dangling namespace entries.
    ///
    /// Returns parent inode ids whose directory namespace was mutated during replay.
    fn recover_pending_creates(&self) -> MetaResult<Vec<Ino>>;

    /// Record a pending create intent for crash recovery.
    fn add_pending_create(&self, record: &PendingCreateRecord) -> MetaResult<()>;

    /// Remove a pending create intent.
    fn remove_pending_create(&self, ino: Ino) -> MetaResult<()>;

    /// List pending create intents.
    fn list_pending_creates(&self) -> MetaResult<Vec<PendingCreateRecord>>;

    /// Append an operation into the generic undo log.
    fn append_undo_log(&self, record: &UndoLogRecord) -> MetaResult<()>;

    /// Remove all undo-log records for the `(kind, ino)` tuple.
    fn remove_undo_logs_for(&self, kind: UndoLogKind, ino: Ino) -> MetaResult<()>;

    /// List undo-log records.
    fn list_undo_logs(&self) -> MetaResult<Vec<UndoLogRecord>>;

    /// Replay generic undo-log records and heal dangling namespace state.
    ///
    /// Returns parent inode ids whose directory namespace was mutated during replay.
    fn recover_undo_logs(&self) -> MetaResult<Vec<Ino>>;

    /// Try to acquire the global janitor lease before contacting remote object storage.
    ///
    /// Local-only engines may allow the janitor to proceed immediately. Distributed
    /// engines should return `Ok(false)` when another client already holds the lease.
    fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool>;

    /// Release the global janitor lease, if one was acquired.
    fn release_global_janitor_lock(&self) -> MetaResult<()>;

    /// Return globally visible snapshot-pinned object ids used to protect offline snapshots.
    fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>>;

    // ─── High-Level Operations ───────────────────────────────────────────

    /// Create a new file: allocate inode, set attrs, add dentry.
    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr>;

    /// Create a new directory: allocate inode, set attrs, add dentry, update parent nlink.
    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr>;

    /// Unlink a file: remove dentry, decrement nlink, clean up if nlink=0.
    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr>;

    /// Open-unlink transaction: remove dentry and decrement nlink, but keep inode alive.
    ///
    /// This is used by VFS when the file still has open handles and final inode/block
    /// cleanup is deferred until the last handle is released.
    fn unlink_open_handle(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr>;

    /// Record a deferred delete (open-unlink orphan) for crash-safe cleanup.
    fn add_deferred_delete(&self, record: &DeferredDeleteRecord) -> MetaResult<()>;

    /// Remove a deferred delete record after final cleanup.
    fn remove_deferred_delete(&self, ino: Ino) -> MetaResult<()>;

    /// List deferred delete records pending replay/recovery.
    fn list_deferred_deletes(&self) -> MetaResult<Vec<DeferredDeleteRecord>>;

    /// Remove an empty directory.
    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()>;

    /// Atomic rename: move a directory entry in a single transaction.
    ///
    /// Returns `Some(displaced_ino)` if the target entry was overwritten
    /// (the displaced inode's nlink was decremented and — if zero — its
    /// attr was removed).  The caller (VFS) is responsible for scheduling
    /// block GC for the displaced inode.
    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<RenameAtomicResult>;

    /// Rename a file or directory (delegates to `rename_atomic`).
    fn rename(&self, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8]) -> MetaResult<()> {
        self.rename_atomic(parent, name, new_parent, new_name)?;
        Ok(())
    }

    // ─── Hardlinks ───────────────────────────────────────────────────────

    /// Create a hard link in a single atomic transaction.
    ///
    /// Increments the inode's nlink and adds a new directory entry pointing
    /// to the existing inode.  Directories cannot be hardlinked.
    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr>;

    /// Create a hard link (validates and delegates to `link_atomic`).
    fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        let attr = self.get_attr(ino)?;
        if attr.kind == FileType::Directory {
            return Err(MetaError::IsADirectory(ino));
        }
        self.link_atomic(ino, new_parent, new_name)
    }

    // ─── Symlinks ────────────────────────────────────────────────────────

    /// Store a symlink target for the given inode.
    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()>;

    /// Get the symlink target for the given inode.
    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>>;

    // ─── Extended Attributes (xattr) ─────────────────────────────────────

    /// Get an extended attribute value.
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>>;

    /// Set an extended attribute.
    /// Flags:
    /// - `XATTR_CREATE_FLAG`: fail if key exists
    /// - `XATTR_REPLACE_FLAG`: fail if key is missing
    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()>;

    /// Remove an extended attribute.
    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()>;

    /// List all extended attribute names for an inode.
    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>>;

    // ─── Access Control (ACL) ────────────────────────────────────────────

    /// Set (create/update) explicit ACL rule for `(ino, principal)`.
    fn set_acl_rule(&self, ino: Ino, principal: &str, access_level: AccessLevel) -> MetaResult<()>;

    /// Remove explicit ACL rule for `(ino, principal)`.
    fn remove_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<()>;

    /// Get explicit ACL rule for `(ino, principal)`, if present.
    fn get_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<Option<AccessLevel>>;

    /// Create a symlink: allocate inode, store target, add dentry.
    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr>;

    // ─── Snapshots ────────────────────────────────────────────────────────

    /// Create an immutable snapshot rooted at `root_ino`.
    fn create_snapshot(&self, root_ino: Ino) -> MetaResult<MetadataSnapshot>;

    /// List known snapshots.
    fn list_snapshots(&self) -> MetaResult<Vec<MetadataSnapshot>>;

    /// Look up snapshot by id.
    fn get_snapshot(&self, snapshot_id: &str) -> MetaResult<Option<MetadataSnapshot>>;
}

pub trait MetaFormat: Send + Sync {
    /// Store format info (called during `studiofs init`).
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()>;

    /// Load format info.
    fn get_format_info(&self) -> MetaResult<FormatInfo>;
}

pub trait MetaInodes: MetaEngineCore {}

pub trait MetaNamespace: MetaEngineCore {}

pub trait MetaBlocks: MetaEngineCore {}

pub trait MetaGc: MetaEngineCore {}

pub trait MetaRecovery: MetaEngineCore {}

pub trait MetaSymlink: MetaEngineCore {}

pub trait MetaXattr: MetaEngineCore {}

pub trait MetaAcl: MetaEngineCore {}

pub trait MetaSnapshots: MetaEngineCore {}

pub trait MetaReplication: Send + Sync {
    /// Verify that the local metadata matches the remote state (e.g. UUID check).
    fn check_consistency(&self) -> MetaResult<()>;

    /// Flush pending metadata mutations to remote store (if any).
    fn flush_metadata(&self) -> MetaResult<()>;

    /// Force metadata synchronization from remote source (if applicable).
    fn sync_metadata(&self) -> MetaResult<()>;

    /// Stable identifier used for invalidation self-echo suppression.
    fn invalidation_sender_id(&self) -> Option<String>;

    /// Request a near-immediate flush of pending metadata mutations.
    fn request_eager_flush(&self);

    /// Best-effort metadata version token for change detection.
    fn metadata_version_token(&self) -> Option<String>;

    /// Best-effort namespace-only version token for dentry/path invalidation.
    fn namespace_version_token(&self) -> Option<String> {
        self.metadata_version_token()
    }

    /// Best-effort count of local metadata mutations not yet durably pushed to remote.
    fn replication_backlog(&self) -> MetadataReplicationBacklog;

    /// Return the estimated size of remote metadata in bytes, if applicable.
    fn remote_metadata_size(&self) -> MetaResult<Option<u64>>;
}

/// Public aggregate contract for consumers.
pub trait MetaEngine:
    MetaEngineCore
    + MetaFormat
    + MetaInodes
    + MetaNamespace
    + MetaBlocks
    + MetaGc
    + MetaRecovery
    + MetaSymlink
    + MetaXattr
    + MetaAcl
    + MetaSnapshots
    + MetaReplication
{
}

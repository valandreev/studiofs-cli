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
}

pub type MetaResult<T> = Result<T, MetaError>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataReplicationBacklog {
    pub pending_ops: u32,
    pub inflight_ops: u32,
}

impl MetadataReplicationBacklog {
    pub fn total_ops(self) -> u32 {
        self.pending_ops.saturating_add(self.inflight_ops)
    }
}

/// Abstract metadata engine interface.
///
/// All operations are synchronous (redb is sync). The VFS layer wraps them
/// in `spawn_blocking` for async compatibility.
pub trait MetaEngine: Send + Sync {
    /// Flush pending metadata mutations to remote store (if any).
    ///
    /// Local-only engines can keep the default no-op implementation.
    fn flush_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    /// Force metadata synchronization from remote source (if applicable).
    ///
    /// Used by strong-consistency paths (e.g. distributed file locks) to
    /// guarantee refresh-before-lock-grant semantics.
    fn sync_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    /// Stable identifier used for invalidation self-echo suppression.
    ///
    /// Engines that publish invalidation events should override this and return
    /// the same identifier they include in event payloads.
    fn invalidation_sender_id(&self) -> Option<String> {
        None
    }

    /// Request a near-immediate flush of pending metadata mutations.
    ///
    /// Called by the VFS after user-visible directory mutations (create,
    /// unlink, rmdir, rename, …) so that changes reach remote metadata within
    /// a short batching window (~250 ms) instead of waiting for the full
    /// `flush_interval` (default 60 s).  The default is a no-op.
    fn request_eager_flush(&self) {}

    /// Best-effort metadata version token for change detection.
    ///
    /// Remote-backed engines can expose a monotonic-ish token (for example,
    /// an object ETag) so invalidation listeners can tell whether a sync
    /// actually observed a newer remote snapshot.
    fn metadata_version_token(&self) -> Option<String> {
        None
    }

    /// Best-effort count of local metadata mutations not yet durably pushed to
    /// the shared remote metadata source.
    fn replication_backlog(&self) -> MetadataReplicationBacklog {
        MetadataReplicationBacklog::default()
    }

    // ─── Format / Settings ───────────────────────────────────────────────

    /// Store format info (called during `studiofs init`).
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()>;

    /// Load format info.
    fn get_format_info(&self) -> MetaResult<FormatInfo>;

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

    /// Set (create or update) inode attributes.
    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()>;

    /// Remove an inode's attributes.
    fn remove_attr(&self, ino: Ino) -> MetaResult<()>;

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
        _ino: Ino,
        _block_index: u32,
        _expected_epoch: u64,
        _block: &BlockRef,
    ) -> MetaResult<bool> {
        Ok(true)
    }

    /// Atomically publish the current file manifest as the new committed head.
    ///
    /// This switches the committed manifest at file granularity and updates the
    /// inode's coarse remote upload state to `ready` when successful.
    fn publish_file_manifest(&self, _ino: Ino, _expected_epoch: u64) -> MetaResult<bool> {
        Ok(false)
    }

    // ─── Pending Deletions (GC) ───────────────────────

    /// Add a block to pending deletions (mark for GC).
    /// The timestamp is used for grace period.
    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()>;

    /// Remove a block from pending deletions (after GC).
    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()>;

    /// Get all pending deletions (object_id, timestamp).
    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>>;

    /// Get currently referenced object IDs from FILE_BLOCKS metadata.
    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        Err(MetaError::Storage(
            "get_referenced_object_ids not implemented".to_string(),
        ))
    }

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
    /// Flags: 1 = XATTR_CREATE (fail if exists), 2 = XATTR_REPLACE (fail if not exists).
    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()>;

    /// Remove an extended attribute.
    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()>;

    /// List all extended attribute names for an inode.
    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>>;

    /// Create a symlink: allocate inode, store target, add dentry.
    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr>;
}

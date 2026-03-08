use crate::types::{BlockRef, DirEntryValue, InodeAttr};
use serde::{Deserialize, Serialize};

/// Represents a single mutation to the metadata state.
/// These events are appended to a local journal and eventually pushed to S3
/// to achieve Event Sourcing and Eventual Consistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaEvent {
    // ─── Core Inode Operations ───────────────────────────────────────────────
    /// Create or update an inode.
    SetAttr { attr: InodeAttr },

    /// Remove an inode entirely.
    RemoveAttr { ino: u64 },

    // ─── Directory Operations ────────────────────────────────────────────────
    /// Add a directory entry.
    AddEntry {
        parent: u64,
        name: Vec<u8>,
        entry: DirEntryValue,
    },

    /// Remove a directory entry.
    RemoveEntry { parent: u64, name: Vec<u8> },

    /// Atomically rename a directory entry. Replicas apply this only if the
    /// source dentry still points at the expected inode.
    RenameEntry {
        parent: u64,
        name: Vec<u8>,
        new_parent: u64,
        new_name: Vec<u8>,
        entry: DirEntryValue,
        displaced_set_attr: Option<InodeAttr>,
        displaced_remove_attr_ino: Option<u64>,
        moved_dir_attr: Option<InodeAttr>,
        old_parent_attr: Option<InodeAttr>,
        new_parent_attr: Option<InodeAttr>,
    },

    // ─── File Block Operations ───────────────────────────────────────────────
    /// Set a block reference for a file.
    SetFileBlock {
        ino: u64,
        block_index: u32,
        block: BlockRef,
    },

    /// Remove a specific file block (e.g., truncate).
    RemoveFileBlock { ino: u64, block_index: u32 },

    /// Remove all blocks for a file (e.g., unlink).
    RemoveFileBlocks { ino: u64 },

    /// Set multiple block references in one batch (used by drain_dirty_mem).
    SetFileBlocksBatch { blocks: Vec<(u64, u32, BlockRef)> },

    /// Publish a block that has finished uploading and is now remote-visible.
    CommitFileBlock {
        ino: u64,
        block_index: u32,
        block: BlockRef,
    },

    /// Atomically publish the committed manifest head for a file.
    PublishFileManifest {
        ino: u64,
        blocks: Vec<(u32, BlockRef)>,
    },

    // ─── Target & Extended Attributes ────────────────────────────────────────
    /// Set a symlink target.
    SetSymlinkTarget { ino: u64, target: Vec<u8> },

    /// Set an extended attribute.
    SetXattr {
        ino: u64,
        name: Vec<u8>,
        value: Vec<u8>,
        flags: u32, // XATTR_CREATE (1) or XATTR_REPLACE (2)
    },

    /// Remove an extended attribute.
    RemoveXattr { ino: u64, name: Vec<u8> },

    // ─── Garbage Collection ──────────────────────────────────────────────────
    /// Mark an object for garbage collection.
    AddPendingDeletion { object_id: u64, timestamp: i64 },

    /// Remove an object from pending deletions (GC complete).
    RemovePendingDeletion { object_id: u64 },
}

impl MetaEvent {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SetAttr { .. } => "set_attr",
            Self::RemoveAttr { .. } => "remove_attr",
            Self::AddEntry { .. } => "add_entry",
            Self::RemoveEntry { .. } => "remove_entry",
            Self::RenameEntry { .. } => "rename_entry",
            Self::SetFileBlock { .. } => "set_file_block",
            Self::RemoveFileBlock { .. } => "remove_file_block",
            Self::RemoveFileBlocks { .. } => "remove_file_blocks",
            Self::SetFileBlocksBatch { .. } => "set_file_blocks_batch",
            Self::CommitFileBlock { .. } => "commit_file_block",
            Self::PublishFileManifest { .. } => "publish_file_manifest",
            Self::SetSymlinkTarget { .. } => "set_symlink_target",
            Self::SetXattr { .. } => "set_xattr",
            Self::RemoveXattr { .. } => "remove_xattr",
            Self::AddPendingDeletion { .. } => "add_pending_deletion",
            Self::RemovePendingDeletion { .. } => "remove_pending_deletion",
        }
    }

    /// Returns the primary Inode associated with this event, if applicable.
    /// This is useful for partitioning or logical clocks.
    pub fn primary_ino(&self) -> Option<u64> {
        match self {
            Self::SetAttr { attr } => Some(attr.ino),
            Self::RemoveAttr { ino } => Some(*ino),
            Self::AddEntry { parent, .. } => Some(*parent),
            Self::RemoveEntry { parent, .. } => Some(*parent),
            Self::RenameEntry { parent, .. } => Some(*parent),
            Self::SetFileBlock { ino, .. } => Some(*ino),
            Self::RemoveFileBlock { ino, .. } => Some(*ino),
            Self::RemoveFileBlocks { ino } => Some(*ino),
            Self::SetFileBlocksBatch { blocks } => blocks.first().map(|(ino, _, _)| *ino),
            Self::CommitFileBlock { ino, .. } => Some(*ino),
            Self::PublishFileManifest { ino, .. } => Some(*ino),
            Self::SetSymlinkTarget { ino, .. } => Some(*ino),
            Self::SetXattr { ino, .. } => Some(*ino),
            Self::RemoveXattr { ino, .. } => Some(*ino),
            Self::AddPendingDeletion { .. } => None,
            Self::RemovePendingDeletion { .. } => None,
        }
    }
}

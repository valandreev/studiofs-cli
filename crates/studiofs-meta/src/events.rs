use crate::types::{BlockRef, DirEntryValue, InodeAttr, MetadataSnapshot, SnapshotState};
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
        displaced_set_attr: Box<Option<InodeAttr>>,
        displaced_remove_attr_ino: Option<u64>,
        moved_dir_attr: Box<Option<InodeAttr>>,
        old_parent_attr: Box<Option<InodeAttr>>,
        new_parent_attr: Box<Option<InodeAttr>>,
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

    /// Replace the local overlay slices for a regular file chunk batch.
    ///
    /// The payload carries the final row state for each chunk. An empty slice
    /// vector means the chunk row should be removed.
    SetRegularFileOverlaySlices {
        ino: u64,
        chunk_slices: Vec<(crate::schema::ChunkId, Vec<crate::schema::SliceRecord>)>,
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

    /// Set ACL rule for `(ino, principal)`.
    SetAclRule {
        ino: u64,
        principal: String,
        access_level: crate::types::AccessLevel,
    },

    /// Remove ACL rule for `(ino, principal)`.
    RemoveAclRule { ino: u64, principal: String },

    // ─── Garbage Collection ──────────────────────────────────────────────────
    /// Mark an object for garbage collection.
    AddPendingDeletion { object_id: u64, timestamp: i64 },

    /// Remove an object from pending deletions (GC complete).
    RemovePendingDeletion { object_id: u64 },

    // ─── Snapshots ───────────────────────────────────────────────────────────
    /// Create or update snapshot definition.
    CreateSnapshot { snapshot: MetadataSnapshot },

    /// Update snapshot replication state.
    UpdateSnapshotState { id: String, state: SnapshotState },
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
            Self::SetRegularFileOverlaySlices { .. } => "set_regular_file_overlay_slices",
            Self::SetSymlinkTarget { .. } => "set_symlink_target",
            Self::SetXattr { .. } => "set_xattr",
            Self::RemoveXattr { .. } => "remove_xattr",
            Self::SetAclRule { .. } => "set_acl_rule",
            Self::RemoveAclRule { .. } => "remove_acl_rule",
            Self::AddPendingDeletion { .. } => "add_pending_deletion",
            Self::RemovePendingDeletion { .. } => "remove_pending_deletion",
            Self::CreateSnapshot { .. } => "create_snapshot",
            Self::UpdateSnapshotState { .. } => "update_snapshot_state",
        }
    }

    pub fn is_namespace_mutation(&self) -> bool {
        matches!(
            self,
            Self::AddEntry { .. } | Self::RemoveEntry { .. } | Self::RenameEntry { .. }
        )
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
            Self::SetRegularFileOverlaySlices { ino, .. } => Some(*ino),
            Self::SetSymlinkTarget { ino, .. } => Some(*ino),
            Self::SetXattr { ino, .. } => Some(*ino),
            Self::RemoveXattr { ino, .. } => Some(*ino),
            Self::SetAclRule { ino, .. } => Some(*ino),
            Self::RemoveAclRule { ino, .. } => Some(*ino),
            Self::AddPendingDeletion { .. } => None,
            Self::RemovePendingDeletion { .. } => None,
            Self::CreateSnapshot { snapshot } => Some(snapshot.root_ino),
            Self::UpdateSnapshotState { .. } => None,
        }
    }
}

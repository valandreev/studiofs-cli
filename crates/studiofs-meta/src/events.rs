use crate::types::{BlockRef, DirEntryValue, InodeAttr, MetadataSnapshot, SnapshotState};
use serde::{Deserialize, Serialize};

/// Represents a single mutation to the metadata state.
/// These events are appended to a local journal and eventually pushed to S3
/// to achieve Event Sourcing and Eventual Consistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaEvent {
    // ─── Core Inode Operations ───────────────────────────────────────────────
    /// Create or update an inode.
    SetAttr {
        /// Full inode attributes that should become authoritative.
        attr: InodeAttr,
    },

    /// Remove an inode entirely.
    RemoveAttr {
        /// Inode to remove from metadata.
        ino: u64,
    },

    // ─── Directory Operations ────────────────────────────────────────────────
    /// Add a directory entry.
    AddEntry {
        /// Parent directory that should contain the entry.
        parent: u64,
        /// Entry name encoded as raw bytes.
        name: Vec<u8>,
        /// Dentry payload to store for the child.
        entry: DirEntryValue,
    },

    /// Remove a directory entry.
    RemoveEntry {
        /// Parent directory that currently contains the entry.
        parent: u64,
        /// Entry name encoded as raw bytes.
        name: Vec<u8>,
    },

    /// Atomically rename a directory entry. Replicas apply this only if the
    /// source dentry still points at the expected inode.
    RenameEntry {
        /// Source parent directory.
        parent: u64,
        /// Source entry name encoded as raw bytes.
        name: Vec<u8>,
        /// Destination parent directory.
        new_parent: u64,
        /// Destination entry name encoded as raw bytes.
        new_name: Vec<u8>,
        /// Entry payload that should appear at the destination.
        entry: DirEntryValue,
        /// Replacement attrs for an inode displaced at the destination, if any.
        displaced_set_attr: Box<Option<InodeAttr>>,
        /// Displaced inode that should be removed outright, if any.
        displaced_remove_attr_ino: Option<u64>,
        /// Updated attrs for a moved directory inode after the rename.
        moved_dir_attr: Box<Option<InodeAttr>>,
        /// Updated attrs for the source parent after the rename.
        old_parent_attr: Box<Option<InodeAttr>>,
        /// Updated attrs for the destination parent after the rename.
        new_parent_attr: Box<Option<InodeAttr>>,
    },

    // ─── File Block Operations ───────────────────────────────────────────────
    /// Legacy flat-block event: set one block reference for a file.
    ///
    /// Kept for compatibility with old journals/tests. New regular-file write
    /// paths should prefer slice-native regular-file graph events.
    SetFileBlock {
        /// File inode being updated.
        ino: u64,
        /// Logical block slot inside the file.
        block_index: u32,
        /// Block payload reference to persist.
        block: BlockRef,
    },

    /// Legacy flat-block event: remove one logical file block.
    ///
    /// Kept for compatibility with old journals/tests. New regular-file write
    /// paths should prefer slice-native regular-file graph events.
    RemoveFileBlock {
        /// File inode being updated.
        ino: u64,
        /// Logical block slot to clear.
        block_index: u32,
    },

    /// Remove all regular-file graph rows for a file (e.g., unlink).
    RemoveFileBlocks {
        /// File inode whose block mapping should be removed.
        ino: u64,
    },

    /// Legacy flat-block event: set multiple block references in one batch.
    ///
    /// Kept for compatibility with old journals/tests. New regular-file write
    /// paths should prefer slice-native regular-file graph events.
    SetFileBlocksBatch {
        /// Batch of `(ino, block_index, block)` updates to apply atomically.
        blocks: Vec<(u64, u32, BlockRef)>,
    },

    /// Legacy flat-block event: publish one uploaded block as remote-visible.
    ///
    /// Kept for compatibility with old journals/tests. New regular-file upload
    /// paths should prefer object/slice-native publication.
    CommitFileBlock {
        /// File inode being updated.
        ino: u64,
        /// Logical block slot inside the file.
        block_index: u32,
        /// Remote-visible block reference for the slot.
        block: BlockRef,
    },

    /// Legacy flat-manifest event: publish a full `(block_index, BlockRef)`
    /// manifest for a file.
    ///
    /// Kept for compatibility with old journals/tests. New regular-file
    /// publish paths should prefer `PublishRegularFileBaseSlices`.
    PublishFileManifest {
        /// File inode whose published manifest is being replaced.
        ino: u64,
        /// Full published manifest as `(block_index, block)` rows.
        blocks: Vec<(u32, BlockRef)>,
    },

    /// Replace the local overlay slices for a regular file chunk batch.
    ///
    /// The payload carries the final row state for each chunk. An empty slice
    /// vector means the chunk row should be removed.
    SetRegularFileOverlaySlices {
        /// File inode whose overlay chunks are being rewritten.
        ino: u64,
        /// Final chunk rows as `(chunk_id, slices)` pairs.
        chunk_slices: Vec<(crate::schema::ChunkId, Vec<crate::schema::SliceRecord>)>,
    },

    /// Atomically publish the remote-visible regular-file base graph.
    ///
    /// The payload carries the full published base rows for the inode after the
    /// publish barrier completes. This is the slice-native publish event used
    /// by regular-file graphs that can no longer be losslessly expressed as a
    /// flat `(block_index, BlockRef)` manifest.
    PublishRegularFileBaseSlices {
        /// File inode whose published base chunks are being replaced.
        ino: u64,
        /// Final published base rows as `(chunk_id, slices)` pairs.
        chunk_slices: Vec<(crate::schema::ChunkId, Vec<crate::schema::SliceRecord>)>,
    },

    // ─── Target & Extended Attributes ────────────────────────────────────────
    /// Set a symlink target.
    SetSymlinkTarget {
        /// Symlink inode being updated.
        ino: u64,
        /// Raw target bytes to persist.
        target: Vec<u8>,
    },

    /// Set an extended attribute.
    SetXattr {
        /// Inode that owns the xattr.
        ino: u64,
        /// Raw xattr name bytes.
        name: Vec<u8>,
        /// Raw xattr value bytes.
        value: Vec<u8>,
        /// POSIX-style create/replace flags for the mutation.
        flags: u32, // XATTR_CREATE (1) or XATTR_REPLACE (2)
    },

    /// Remove an extended attribute.
    RemoveXattr {
        /// Inode that owns the xattr.
        ino: u64,
        /// Raw xattr name bytes.
        name: Vec<u8>,
    },

    /// Set ACL rule for `(ino, principal)`.
    SetAclRule {
        /// Inode protected by the ACL rule.
        ino: u64,
        /// Principal receiving the rule.
        principal: String,
        /// Effective access level to persist.
        access_level: crate::types::AccessLevel,
    },

    /// Remove ACL rule for `(ino, principal)`.
    RemoveAclRule {
        /// Inode protected by the ACL rule.
        ino: u64,
        /// Principal whose explicit rule should be removed.
        principal: String,
    },

    // ─── Garbage Collection ──────────────────────────────────────────────────
    /// Mark an object for garbage collection.
    AddPendingDeletion {
        /// Object id scheduled for deferred deletion.
        object_id: u64,
        /// Unix timestamp when the object became pending deletion.
        timestamp: i64,
    },

    /// Remove an object from pending deletions (GC complete).
    RemovePendingDeletion {
        /// Object id that finished deferred deletion.
        object_id: u64,
    },

    // ─── Snapshots ───────────────────────────────────────────────────────────
    /// Create or update snapshot definition.
    CreateSnapshot {
        /// Snapshot descriptor to persist.
        snapshot: MetadataSnapshot,
    },

    /// Update snapshot replication state.
    UpdateSnapshotState {
        /// Snapshot id to update.
        id: String,
        /// New replication state for the snapshot.
        state: SnapshotState,
    },
}

impl MetaEvent {
    /// Return a stable lowercase identifier for this event kind.
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
            Self::PublishRegularFileBaseSlices { .. } => "publish_regular_file_base_slices",
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

    /// Return `true` when this event mutates the directory namespace.
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
            Self::PublishRegularFileBaseSlices { ino, .. } => Some(*ino),
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

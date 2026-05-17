/// Tracks how an inode's data upload currently stands in the VFS pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VfsUploadState {
    /// No active upload action is currently associated with this inode.
    #[default]
    Idle,
    /// A new or changed content upload is in progress.
    Uploading,
    /// Metadata is being updated after a completed upload.
    Updating,
}

/// Represents the committed state of an inode incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedIncarnation {
    /// Content epoch for the last committed version of the inode.
    pub content_epoch: u64,
    /// Size in bytes of the committed incarnation.
    pub size: u64,
}

/// Represents an in-flight incarnation waiting to be fully committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingIncarnation {
    /// Epoch value captured for the pending content version.
    pub content_epoch: u64,
    /// Size in bytes of the content currently being uploaded.
    pub size: u64,
    /// Base size before pending update, used to compute growth or shrinkage.
    pub base_size: u64,
}

/// Aggregates committed and pending incarnation metadata for a single inode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InodeIncarnationState {
    /// Last committed incarnation metadata, if available.
    pub committed_incarnation: Option<CommittedIncarnation>,
    /// Pending incarnation metadata, if an upload or update is in progress.
    pub pending_incarnation: Option<PendingIncarnation>,
    /// Current VFS upload state for this inode.
    pub upload_state: VfsUploadState,
}

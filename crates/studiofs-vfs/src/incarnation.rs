#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Enum $name.
pub enum VfsUploadState {
    #[default]
    /// Variant $name.
    Idle,
    /// Variant $name.
    Uploading,
    /// Variant $name.
    Updating,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CommittedIncarnation {
    /// Field $name.
    pub content_epoch: u64,
    /// Field $name.
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct PendingIncarnation {
    /// Field $name.
    pub content_epoch: u64,
    /// Field $name.
    pub size: u64,
    /// Field $name.
    pub base_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Struct $name.
pub struct InodeIncarnationState {
    /// Field $name.
    pub committed_incarnation: Option<CommittedIncarnation>,
    /// Field $name.
    pub pending_incarnation: Option<PendingIncarnation>,
    /// Field $name.
    pub upload_state: VfsUploadState,
}

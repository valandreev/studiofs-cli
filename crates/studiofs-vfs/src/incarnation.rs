#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VfsUploadState {
    #[default]
    Idle,
    Uploading,
    Updating,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedIncarnation {
    pub content_epoch: u64,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingIncarnation {
    pub content_epoch: u64,
    pub size: u64,
    pub base_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InodeIncarnationState {
    pub committed_incarnation: Option<CommittedIncarnation>,
    pub pending_incarnation: Option<PendingIncarnation>,
    pub upload_state: VfsUploadState,
}

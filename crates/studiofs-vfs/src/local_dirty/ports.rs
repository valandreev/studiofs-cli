use super::core::PersistedPayloadEntry;
use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct PayloadPersistConfig {
    pub(super) put_concurrency: usize,
    pub(super) local_sync_policy: LocalSyncPolicy,
}

pub(super) trait BlockIdAllocatorPort {
    async fn next_object_ids(&self, count: u32) -> VfsResult<Vec<u64>>;
}

pub(super) trait LocalPayloadWriterPort {
    async fn persist_payload(
        &self,
        persisted_blocks: &[PersistedPayloadEntry],
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<()>;

    async fn cleanup_uncommitted(
        &self,
        ino: Ino,
        blocks: &[PersistedPayloadEntry],
        stage: &'static str,
    );
}

pub(super) trait OverlayCommitPort {
    async fn commit_overlay(
        &self,
        ino: Ino,
        chunk_slices: Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
        attr: Option<InodeAttr>,
        wait_for_completion: bool,
    ) -> VfsResult<Option<InodeAttr>>;
}

pub(super) trait UploadSubmissionPort {
    // Upload publication is a post-commit step. Implementations must absorb
    // their own durability / retry mechanics so the drain core does not expose
    // a fallible post-commit boundary.
    async fn submit_uploads(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedPayloadEntry],
        content_epoch: u64,
    );
}

pub(super) trait DrainCommitObserverPort {
    fn on_metadata_committed(&self, ino: Ino, committed_attr: Option<&InodeAttr>);
}

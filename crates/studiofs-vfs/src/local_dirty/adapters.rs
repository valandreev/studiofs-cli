use super::core::PersistedPayloadEntry;
use super::ports::*;
use super::*;

#[derive(Clone, Copy)]
pub(super) struct VfsLocalPayloadWriter<'a> {
    pub(super) vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
pub(super) struct VfsOverlayCommitter<'a> {
    pub(super) vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
pub(super) struct VfsBlockIdAllocator<'a> {
    pub(super) vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
pub(super) struct VfsUploadSubmitter<'a> {
    pub(super) vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
pub(super) struct VfsDrainCommitObserver<'a> {
    pub(super) vfs: &'a StudioVfs,
}

impl BlockIdAllocatorPort for VfsBlockIdAllocator<'_> {
    async fn next_object_ids(&self, count: u32) -> VfsResult<Vec<u64>> {
        run_meta(self.vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.next_object_ids(count)
        })
        .await
    }
}

impl LocalPayloadWriterPort for VfsLocalPayloadWriter<'_> {
    async fn persist_payload(
        &self,
        persisted_blocks: &[PersistedPayloadEntry],
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<()> {
        let payload_entries: Vec<&PersistedPayloadEntry> = persisted_blocks
            .iter()
            .filter(|block| block.requires_payload_persist())
            .collect();
        if payload_entries.is_empty() {
            return Ok(());
        }
        let persisted_bytes: u64 = payload_entries.iter().map(|b| b.len() as u64).sum();
        let started = Instant::now();
        let result: VfsResult<()> = async {
            let persisted_object_ids: Vec<u64> = payload_entries
                .iter()
                .map(|block| block.object_id)
                .collect();
            for chunk in payload_entries.chunks(persist_config.put_concurrency.max(1)) {
                let batch: Vec<_> = chunk
                    .iter()
                    .map(|block| studiofs_cache::local::LocalPayloadPut {
                        object_id: block.object_id,
                        data: block
                            .payload_bytes()
                            .expect("payload entries keep inline bytes")
                            .clone(),
                        evictable: false,
                    })
                    .collect();
                self.vfs
                    .payload_store_put_batch_async(batch.clone())
                    .await?;
                self.vfs.cache_upload_hot_batch(batch);
            }
            self.vfs
                .maybe_payload_store_sync_data_async(
                    persisted_bytes,
                    persist_config.local_sync_policy,
                )
                .await?;
            if !persisted_object_ids.is_empty()
                && let Err(error) = self
                    .vfs
                    .upload_stage_link_batch_async(persisted_object_ids)
                    .await
            {
                warn!(
                    persisted_payload_entry_count = payload_entries.len(),
                    error = %error,
                    "failed to create zero-copy upload links for durable payload objects"
                );
            }
            Ok(())
        }
        .await;
        let completed_burst = result.is_ok();
        if persisted_bytes > 0 {
            self.vfs.record_payload_persist_observability(
                payload_entries.len(),
                persisted_bytes,
                started.elapsed(),
                completed_burst,
            );
        }
        result
    }

    async fn cleanup_uncommitted(
        &self,
        ino: Ino,
        blocks: &[PersistedPayloadEntry],
        stage: &'static str,
    ) {
        let object_ids: Vec<u64> = blocks
            .iter()
            .filter(|block| block.requires_payload_persist())
            .map(|block| block.object_id)
            .collect();
        if object_ids.is_empty() {
            return;
        }
        if let Err(error) = self.vfs.payload_store_remove_batch_async(&object_ids).await {
            warn!(
                ino,
                stage,
                object_count = object_ids.len(),
                error = %error,
                "local-dirty cleanup of uncommitted payload-store entries failed"
            );
        }
        if let Err(error) = self.vfs.upload_stage_remove_batch_async(&object_ids).await {
            warn!(
                ino,
                stage,
                object_count = object_ids.len(),
                error = %error,
                "local-dirty cleanup of uncommitted upload entries failed"
            );
        }
        self.vfs.upload_hot_remove_batch(&object_ids);
    }
}

impl OverlayCommitPort for VfsOverlayCommitter<'_> {
    async fn commit_overlay(
        &self,
        ino: Ino,
        chunk_slices: Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
        attr: Option<InodeAttr>,
    ) -> VfsResult<Option<InodeAttr>> {
        let attr = if let Some(attr) = attr {
            if chunk_slices.is_empty()
                && self
                    .vfs
                    .discard_stale_zero_size_dirty_attr_after_current_publish(ino, &attr)
                    .await?
            {
                return Ok(None);
            }
            Some(attr)
        } else {
            None
        };
        self.vfs
            .commit_regular_file_overlay_batch(studiofs_meta::RegularFileOverlayMutation {
                ino,
                chunk_slices,
                attr,
            })
            .await
    }
}

impl UploadSubmissionPort for VfsUploadSubmitter<'_> {
    async fn submit_uploads(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedPayloadEntry],
        content_epoch: u64,
    ) {
        let mut submit_ops = Vec::with_capacity(persisted_blocks.len());
        let mut live_extents = Vec::with_capacity(persisted_blocks.len());
        for block in persisted_blocks {
            let offset = u64::from(block.block_index).saturating_mul(self.vfs.block_size as u64);
            submit_ops.push((
                ino,
                offset,
                offset,
                block.object_id,
                content_epoch,
                block.len() as u64,
            ));
            live_extents.push((ino, block.block_index, block.object_id, content_epoch));
        }
        self.vfs.record_live_extents(&live_extents);
        if !submit_ops.is_empty() {
            self.vfs.cached_store.submit_uploads_batch(&submit_ops);
            self.vfs.state.file_session.note_background_transition(
                ino,
                FileSessionOperation::Upload,
                "queued",
            );
        }
    }
}

impl DrainCommitObserverPort for VfsDrainCommitObserver<'_> {
    fn on_metadata_committed(&self, ino: Ino, committed_attr: Option<&InodeAttr>) {
        if let Some(attr) = committed_attr {
            self.vfs.state.namespace.attr_read_cache.lock().remove(ino);
            self.vfs
                .refresh_inode_incarnation_state_from_attr(ino, attr);
        }
    }
}

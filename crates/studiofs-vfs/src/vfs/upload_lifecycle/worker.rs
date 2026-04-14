use super::super::{
    BlockRef, FileSessionOperation, Ino, InodeAttr, StudioVfs, VfsError, VfsResult,
    block_index_from_offset,
};
use crate::VfsMeta;
use crate::runtime::{TimeoutOrCancel, run_meta, run_with_timeout_or_cancel};
use crate::utils::now_unix_millis;
use bytes::Bytes;
use futures::future::join_all;
use opendal::Buffer;
use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use studiofs_cache::upload_queue::PendingUpload;
use studiofs_data::object::{
    CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE, create_object_buffer, format_object_fragment_keys,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const UPLOAD_WORKER_YIELD_EVERY_OPS: u32 = super::super::UPLOAD_WORKER_YIELD_EVERY_OPS;

type DelayedUploads = BTreeMap<u64, VecDeque<PendingUpload>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadOutcome {
    Completed,
    CancelledDeleted,
    CancelledStale,
    Requeued,
    QuarantinedInvalidRegularFileGraph,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SingleUploadOutcome {
    Completed,
    CancelledDeleted,
    CancelledStale,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteVisibility {
    Visible,
    NotVisible,
    Cancelled,
}

#[derive(Debug)]
enum UploadPutOutcome {
    Uploaded,
    Cancelled,
    Failed(VfsError),
}

trait UploadCoordinatorPort: Clone + Send + Sync + 'static {
    fn complete_upload(&self, op: &PendingUpload);
    fn requeue_upload(&self, op: PendingUpload) -> bool;
    fn cancel_upload(&self, op: &PendingUpload);
}

trait UploadLocalBlockPort: Clone + Send + Sync + 'static {
    fn mark_uploading(&self, object_id: u64);
    fn mark_upload_failed(&self, object_id: u64);
    async fn read_for_upload(&self, object_id: u64) -> VfsResult<Option<Bytes>>;
    async fn remove(&self, object_id: u64) -> VfsResult<()>;
    async fn mark_uploaded(&self, object_id: u64) -> VfsResult<()>;
    fn open_handle_count(&self, ino: Ino) -> usize;
    fn record_deferred_deleted_local_block(&self, ino: Ino, object_id: u64);
    fn clear_pending_eviction(&self, object_id: u64) -> bool;
}

trait UploadMetaPort: Clone + Send + Sync + 'static {
    async fn get_attr(&self, ino: Ino) -> VfsResult<Option<InodeAttr>>;
    async fn get_file_block(&self, ino: Ino, block_index: u32) -> VfsResult<Option<BlockRef>>;
    async fn attr_matches_content_epoch(&self, ino: Ino, content_epoch: u64) -> bool;
    async fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> VfsResult<()>;
}

trait RemoteUploadPort: Clone + Send + Sync + 'static {
    fn adaptive_penalty_ms(&self) -> u64;
    fn fragment_keys(&self, object_id: u64) -> Vec<String>;
    async fn all_fragments_visible(
        &self,
        op: &PendingUpload,
        fragment_keys: &[String],
        cancel_token: &CancellationToken,
    ) -> RemoteVisibility;
    async fn upload_fragments(
        &self,
        op: &PendingUpload,
        block_index: u32,
        raw_payload: Bytes,
        fragment_keys: &[String],
        cancel_token: &CancellationToken,
    ) -> UploadPutOutcome;
}

trait UploadShutdownPort: Clone + Send + Sync + 'static {
    fn uploader_shutdown_requested(&self) -> bool;
    fn shutdown_token(&self) -> CancellationToken;
    fn uploader_cancel_token_for_inode(&self, ino: Ino) -> CancellationToken;
    fn maybe_release_upload_cancel_token(&self, ino: Ino);
    fn deleted_inode_marked(&self, ino: Ino) -> bool;
    fn maybe_clear_deleted_inode_marker(&self, ino: Ino);
    fn record_canceled_deleted(&self);
    fn record_canceled_stale(&self);
    fn record_requeue(&self);
}

trait UploadPolicyPort: Clone + Send + Sync + 'static {
    fn is_invalid_regular_file_graph_error(&self, error: &VfsError) -> bool;
    async fn invalid_regular_file_graph_attr_size(&self, ino: Ino) -> u64;
    fn quarantine_invalid_regular_file_graph(&self, ino: Ino, size: u64, error: &VfsError);
    fn mark_upload_sticky_error(&self, ino: Ino, error: &VfsError);
    fn clear_upload_sticky_error(&self, ino: Ino);
    fn debug_pending_inode_state(&self, ino: Ino, event: &'static str);
    async fn refresh_incarnation_state(&self, ino: Ino);
}

#[derive(Clone)]
struct UploadProcessor<C, L, M, R, S, P> {
    coordinator: C,
    local_blocks: L,
    meta: M,
    remote: R,
    shutdown: S,
    policy: P,
    block_size: u32,
    upload_delay: Duration,
    upload_requeue_base: Duration,
    upload_requeue_max: Duration,
}

impl<C, L, M, R, S, P> UploadProcessor<C, L, M, R, S, P>
where
    C: UploadCoordinatorPort,
    L: UploadLocalBlockPort,
    M: UploadMetaPort,
    R: RemoteUploadPort,
    S: UploadShutdownPort,
    P: UploadPolicyPort,
{
    async fn process_upload(&self, op: PendingUpload) -> UploadOutcome {
        struct UploadTokenCleanup<S: UploadShutdownPort> {
            shutdown: S,
            ino: Ino,
        }

        impl<S: UploadShutdownPort> Drop for UploadTokenCleanup<S> {
            fn drop(&mut self) {
                self.shutdown.maybe_release_upload_cancel_token(self.ino);
            }
        }

        let _token_cleanup = UploadTokenCleanup {
            shutdown: self.shutdown.clone(),
            ino: op.ino,
        };
        let block_index = block_index_from_offset(op.offset, self.block_size);
        let upload_cancel_token = self.shutdown.uploader_cancel_token_for_inode(op.ino);

        match self
            .process_single_upload(&op, block_index, &upload_cancel_token)
            .await
        {
            Ok(SingleUploadOutcome::Completed) => UploadOutcome::Completed,
            Ok(SingleUploadOutcome::CancelledDeleted) => UploadOutcome::CancelledDeleted,
            Ok(SingleUploadOutcome::CancelledStale) => UploadOutcome::CancelledStale,
            Ok(SingleUploadOutcome::Shutdown) => UploadOutcome::Shutdown,
            Err(error) => {
                if self.shutdown.uploader_shutdown_requested() {
                    return UploadOutcome::Shutdown;
                }
                if self.policy.is_invalid_regular_file_graph_error(&error) {
                    let attr_size = self
                        .policy
                        .invalid_regular_file_graph_attr_size(op.ino)
                        .await;
                    warn!(
                        object_id = op.object_id,
                        ino = op.ino,
                        error = %error,
                        "upload failed due to invalid regular-file graph, quarantining inode"
                    );
                    self.local_blocks.mark_upload_failed(op.object_id);
                    self.policy
                        .quarantine_invalid_regular_file_graph(op.ino, attr_size, &error);
                    return UploadOutcome::QuarantinedInvalidRegularFileGraph;
                }
                if self.shutdown.deleted_inode_marked(op.ino) {
                    info!(
                        object_id = op.object_id,
                        ino = op.ino,
                        "upload failed for deleted inode, cancelling: {error}"
                    );
                    if let Err(cancel_error) = self.cancel_upload_for_deleted_inode(&op).await {
                        warn!(
                            object_id = op.object_id,
                            ino = op.ino,
                            error = %cancel_error,
                            "upload cancel cleanup failed for deleted inode"
                        );
                    }
                    self.shutdown.record_canceled_deleted();
                    self.shutdown.maybe_clear_deleted_inode_marker(op.ino);
                    return UploadOutcome::CancelledDeleted;
                }

                self.local_blocks.mark_upload_failed(op.object_id);
                self.policy.mark_upload_sticky_error(op.ino, &error);

                let mut retry_op = op;
                retry_op.retry_count = retry_op.retry_count.saturating_add(1);
                let delay = self.upload_requeue_delay(&retry_op);
                retry_op.not_before_unix_ms = now_unix_millis()
                    .saturating_add(delay.as_millis().clamp(0, u64::MAX as u128) as u64);
                self.shutdown.record_requeue();
                warn!(
                    object_id = retry_op.object_id,
                    ino = retry_op.ino,
                    retry_count = retry_op.retry_count,
                    delay_ms = delay.as_millis() as u64,
                    not_before_unix_ms = retry_op.not_before_unix_ms,
                    "upload processing error, scheduled for delayed requeue: {error}"
                );
                self.policy
                    .debug_pending_inode_state(retry_op.ino, "upload_requeued");

                if !self.shutdown.uploader_shutdown_requested()
                    && !self.coordinator.requeue_upload(retry_op)
                {
                    error!("failed to requeue upload op after error");
                }

                if !self.shutdown.uploader_shutdown_requested() {
                    UploadOutcome::Requeued
                } else {
                    UploadOutcome::Shutdown
                }
            }
        }
    }

    async fn process_single_upload(
        &self,
        op: &PendingUpload,
        block_index: u32,
        upload_cancel_token: &CancellationToken,
    ) -> VfsResult<SingleUploadOutcome> {
        if self.shutdown.uploader_shutdown_requested() {
            return Ok(SingleUploadOutcome::Shutdown);
        }

        let mut waited_upload_delay_ms = 0u64;
        if self.upload_delay > Duration::ZERO {
            let age_ms = now_unix_millis().saturating_sub(op.created_unix_ms);
            let delay_ms = self.upload_delay.as_millis() as u64;
            if age_ms < delay_ms {
                waited_upload_delay_ms = delay_ms - age_ms;
                let shutdown_token = self.shutdown.shutdown_token();
                tokio::select! {
                    _ = shutdown_token.cancelled() => return Ok(SingleUploadOutcome::Shutdown),
                    _ = upload_cancel_token.cancelled() => {
                        return if self.cancel_upload_if_inode_deleted(op).await? {
                            Ok(SingleUploadOutcome::CancelledDeleted)
                        } else {
                            Ok(SingleUploadOutcome::Shutdown)
                        };
                    }
                    _ = tokio::time::sleep(Duration::from_millis(waited_upload_delay_ms)) => {}
                }
            }
        }
        if self.shutdown.uploader_shutdown_requested() {
            return Ok(SingleUploadOutcome::Shutdown);
        }

        if self.cancel_upload_if_inode_deleted(op).await? {
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                "process_upload canceled: inode deleted (short-circuit)"
            );
            return Ok(SingleUploadOutcome::CancelledDeleted);
        }

        let adaptive_penalty_ms = self.remote.adaptive_penalty_ms();
        let effective_adaptive_penalty_ms =
            adaptive_penalty_ms.saturating_sub(waited_upload_delay_ms);
        if effective_adaptive_penalty_ms > 0 {
            let shutdown_token = self.shutdown.shutdown_token();
            tokio::select! {
                _ = shutdown_token.cancelled() => return Ok(SingleUploadOutcome::Shutdown),
                _ = upload_cancel_token.cancelled() => {
                    return if self.cancel_upload_if_inode_deleted(op).await? {
                        Ok(SingleUploadOutcome::CancelledDeleted)
                    } else {
                        Ok(SingleUploadOutcome::Shutdown)
                    };
                }
                _ = tokio::time::sleep(Duration::from_millis(effective_adaptive_penalty_ms)) => {}
            }
        }

        let attr = self.meta.get_attr(op.ino).await?;
        let Some(attr) = attr else {
            self.shutdown.record_canceled_deleted();
            self.cancel_upload_for_deleted_inode(op).await?;
            self.shutdown.maybe_clear_deleted_inode_marker(op.ino);
            return Ok(SingleUploadOutcome::CancelledDeleted);
        };

        if attr.content_epoch != op.content_epoch {
            self.cancel_stale_upload(op).await?;
            return Ok(SingleUploadOutcome::CancelledStale);
        }

        if let Some(current) = self.meta.get_file_block(op.ino, block_index).await?
            && current.object_id != op.object_id
        {
            self.cancel_stale_upload(op).await?;
            return Ok(SingleUploadOutcome::CancelledStale);
        }

        let t0 = Instant::now();
        self.local_blocks.mark_uploading(op.object_id);
        let fragment_keys = self.remote.fragment_keys(op.object_id);
        let mut remote_object_visible = false;
        if op.retry_count > 0 {
            match self
                .remote
                .all_fragments_visible(op, &fragment_keys, upload_cancel_token)
                .await
            {
                RemoteVisibility::Visible => {
                    remote_object_visible = true;
                }
                RemoteVisibility::NotVisible => {}
                RemoteVisibility::Cancelled => {
                    return if self.cancel_upload_if_inode_deleted(op).await? {
                        Ok(SingleUploadOutcome::CancelledDeleted)
                    } else {
                        Ok(SingleUploadOutcome::Shutdown)
                    };
                }
            }
        }

        let raw_payload: Option<Bytes> = if remote_object_visible {
            None
        } else {
            self.local_blocks.read_for_upload(op.object_id).await?
        };

        let raw_payload = match raw_payload {
            Some(data) => data,
            None if remote_object_visible => Bytes::new(),
            None => {
                if op.retry_count > 0 {
                    match self
                        .remote
                        .all_fragments_visible(op, &fragment_keys, upload_cancel_token)
                        .await
                    {
                        RemoteVisibility::Visible => {
                            remote_object_visible = true;
                            Bytes::new()
                        }
                        RemoteVisibility::NotVisible => {
                            if self.shutdown.deleted_inode_marked(op.ino) {
                                self.cancel_upload_for_deleted_inode(op).await?;
                                return Ok(SingleUploadOutcome::CancelledDeleted);
                            }
                            return Err(VfsError::Other(format!(
                                "object {} missing from local block backend",
                                op.object_id
                            )));
                        }
                        RemoteVisibility::Cancelled => {
                            return if self.cancel_upload_if_inode_deleted(op).await? {
                                Ok(SingleUploadOutcome::CancelledDeleted)
                            } else {
                                Ok(SingleUploadOutcome::Shutdown)
                            };
                        }
                    }
                } else {
                    if self.shutdown.deleted_inode_marked(op.ino) {
                        self.cancel_upload_for_deleted_inode(op).await?;
                        return Ok(SingleUploadOutcome::CancelledDeleted);
                    }
                    return Err(VfsError::Other(format!(
                        "object {} missing from local block backend",
                        op.object_id
                    )));
                }
            }
        };

        if !remote_object_visible {
            match self
                .remote
                .upload_fragments(
                    op,
                    block_index,
                    raw_payload,
                    &fragment_keys,
                    upload_cancel_token,
                )
                .await
            {
                UploadPutOutcome::Uploaded => {}
                UploadPutOutcome::Cancelled => {
                    return if self.cancel_upload_if_inode_deleted(op).await? {
                        Ok(SingleUploadOutcome::CancelledDeleted)
                    } else {
                        Ok(SingleUploadOutcome::Shutdown)
                    };
                }
                UploadPutOutcome::Failed(error) => return Err(error),
            }
        }

        let current = self.meta.get_file_block(op.ino, block_index).await?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0);
        match current {
            Some(ref block_ref)
                if block_ref.object_id == op.object_id
                    && self
                        .meta
                        .attr_matches_content_epoch(op.ino, op.content_epoch)
                        .await =>
            {
                self.coordinator.complete_upload(op);
            }
            _ => {
                self.meta
                    .add_pending_deletion(op.object_id, timestamp)
                    .await?;
                self.coordinator.complete_upload(op);
            }
        }

        self.policy.refresh_incarnation_state(op.ino).await;
        self.policy.clear_upload_sticky_error(op.ino);
        self.policy
            .debug_pending_inode_state(op.ino, "upload_completed");

        if self.local_blocks.clear_pending_eviction(op.object_id) {
            self.local_blocks.remove(op.object_id).await?;
        } else if let Some(current) = self.meta.get_file_block(op.ino, block_index).await?
            && current.object_id != op.object_id
        {
            self.local_blocks.remove(op.object_id).await?;
        } else {
            self.local_blocks.mark_uploaded(op.object_id).await?;
        }

        debug!(
            object_id = op.object_id,
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "background upload complete"
        );
        Ok(SingleUploadOutcome::Completed)
    }

    async fn cancel_upload_if_inode_deleted(&self, op: &PendingUpload) -> VfsResult<bool> {
        if !self.shutdown.deleted_inode_marked(op.ino) {
            return Ok(false);
        }
        self.shutdown.record_canceled_deleted();
        self.cancel_upload_for_deleted_inode(op).await?;
        self.shutdown.maybe_clear_deleted_inode_marker(op.ino);
        Ok(true)
    }

    async fn cancel_upload_for_deleted_inode(&self, op: &PendingUpload) -> VfsResult<()> {
        let has_open_handles = self.local_blocks.open_handle_count(op.ino) > 0;
        self.coordinator.cancel_upload(op);
        if has_open_handles {
            self.local_blocks
                .record_deferred_deleted_local_block(op.ino, op.object_id);
        } else {
            self.local_blocks.remove(op.object_id).await?;
        }
        self.local_blocks.clear_pending_eviction(op.object_id);
        Ok(())
    }

    async fn cancel_stale_upload(&self, op: &PendingUpload) -> VfsResult<()> {
        self.shutdown.record_canceled_stale();
        self.coordinator.cancel_upload(op);
        self.local_blocks.remove(op.object_id).await?;
        self.local_blocks.clear_pending_eviction(op.object_id);
        Ok(())
    }

    fn upload_requeue_delay(&self, op: &PendingUpload) -> Duration {
        let exp = op.retry_count.min(8);
        let base = self
            .upload_requeue_base
            .saturating_mul(2u32.saturating_pow(exp));
        let capped = base.min(self.upload_requeue_max);
        let jitter_max = (capped.as_millis() as u64 / 5).max(1);
        let jitter_seed = op.object_id ^ now_unix_millis();
        let jitter_ms = jitter_seed % jitter_max;
        capped.saturating_add(Duration::from_millis(jitter_ms))
    }
}

#[derive(Clone)]
struct VfsUploadCoordinatorPort {
    vfs: Arc<StudioVfs>,
}

impl UploadCoordinatorPort for VfsUploadCoordinatorPort {
    fn complete_upload(&self, op: &PendingUpload) {
        self.vfs.upload_coordinator.complete_upload(op);
    }

    fn requeue_upload(&self, op: PendingUpload) -> bool {
        self.vfs.upload_coordinator.requeue_upload(op)
    }

    fn cancel_upload(&self, op: &PendingUpload) {
        self.vfs.upload_coordinator.cancel_upload(op);
    }
}

#[derive(Clone)]
struct VfsUploadLocalBlockPort {
    vfs: Arc<StudioVfs>,
}

impl UploadLocalBlockPort for VfsUploadLocalBlockPort {
    fn mark_uploading(&self, object_id: u64) {
        self.vfs.local_block_backend.mark_uploading(object_id);
    }

    fn mark_upload_failed(&self, object_id: u64) {
        self.vfs.local_block_backend.mark_upload_failed(object_id);
    }

    async fn read_for_upload(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        let upload_io_cost = if self
            .vfs
            .state
            .read
            .foreground_read_active(now_unix_millis(), self.vfs.upload_user_idle_grace)
        {
            self.vfs
                .state
                .upload
                .upload_cache_io_foreground_cost
                .load(Ordering::Relaxed)
                .max(1)
        } else {
            1
        };
        let wait_started = Instant::now();
        let upload_io_permit = self
            .vfs
            .upload_cache_io_slots
            .clone()
            .acquire_many_owned(upload_io_cost)
            .await
            .map_err(|error| VfsError::QosFailure {
                stage: Cow::Borrowed("upload_cache_io_slots"),
                object_id,
                message: error.to_string(),
            })?;
        let wait_ns = wait_started.elapsed().as_nanos().clamp(0, u64::MAX as u128) as u64;
        let read_started = Instant::now();
        let read_result = self.vfs.block_store_get_for_upload_async(object_id).await?;
        let read_ns = read_started.elapsed().as_nanos().clamp(0, u64::MAX as u128) as u64;
        drop(upload_io_permit);

        self.vfs
            .state
            .upload
            .upload_fallback_wait_ns
            .fetch_add(wait_ns, Ordering::Relaxed);
        self.vfs
            .state
            .upload
            .upload_fallback_wait_max_ns
            .fetch_max(wait_ns, Ordering::Relaxed);
        self.vfs
            .state
            .upload
            .upload_fallback_read_ns
            .fetch_add(read_ns, Ordering::Relaxed);
        self.vfs
            .state
            .upload
            .upload_fallback_read_max_ns
            .fetch_max(read_ns, Ordering::Relaxed);
        self.vfs
            .state
            .upload
            .upload_fallback_rereads
            .fetch_add(1, Ordering::Relaxed);
        if let Some(payload) = &read_result {
            self.vfs
                .state
                .upload
                .upload_fallback_reread_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
        } else {
            self.vfs
                .state
                .upload
                .upload_fallback_reread_cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(read_result)
    }

    async fn remove(&self, object_id: u64) -> VfsResult<()> {
        self.vfs.block_store_remove_async(object_id).await
    }

    async fn mark_uploaded(&self, object_id: u64) -> VfsResult<()> {
        self.vfs.block_store_mark_uploaded_async(object_id).await
    }

    fn open_handle_count(&self, ino: Ino) -> usize {
        self.vfs.handle_table.open_count(ino) as usize
    }

    fn record_deferred_deleted_local_block(&self, ino: Ino, object_id: u64) {
        self.vfs.record_deferred_deleted_local_block(ino, object_id);
    }

    fn clear_pending_eviction(&self, object_id: u64) -> bool {
        self.vfs
            .state
            .upload
            .pending_evictions
            .lock()
            .remove(&object_id)
    }
}

#[derive(Clone)]
struct VfsUploadMetaPort {
    vfs: Arc<StudioVfs>,
}

impl UploadMetaPort for VfsUploadMetaPort {
    async fn get_attr(&self, ino: Ino) -> VfsResult<Option<InodeAttr>> {
        let meta = self.vfs.meta.clone();
        Ok(
            run_meta(meta, move |engine: Arc<dyn VfsMeta>| engine.get_attr(ino))
                .await
                .ok(),
        )
    }

    async fn get_file_block(&self, ino: Ino, block_index: u32) -> VfsResult<Option<BlockRef>> {
        let meta = self.vfs.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.get_file_block(ino, block_index)
        })
        .await
    }

    async fn attr_matches_content_epoch(&self, ino: Ino, content_epoch: u64) -> bool {
        let meta = self.vfs.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine
                .get_attr(ino)
                .map(|attr| attr.content_epoch == content_epoch)
        })
        .await
        .unwrap_or(false)
    }

    async fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> VfsResult<()> {
        let meta = self.vfs.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.add_pending_deletion(object_id, timestamp)
        })
        .await
    }
}

#[derive(Clone)]
struct VfsRemoteUploadPort {
    vfs: Arc<StudioVfs>,
}

impl VfsRemoteUploadPort {
    fn build_fragments(raw_payload: Bytes) -> VfsResult<Vec<Buffer>> {
        let mut fragments = Vec::with_capacity(CLOUD_FRAGMENT_COUNT);
        for fragment_index in 0..CLOUD_FRAGMENT_COUNT {
            let start = fragment_index * CLOUD_FRAGMENT_SIZE;
            let end = (start + CLOUD_FRAGMENT_SIZE).min(raw_payload.len());
            let fragment_payload = if start < raw_payload.len() {
                raw_payload.slice(start..end)
            } else {
                Bytes::new()
            };
            fragments.push(create_object_buffer(fragment_payload)?);
        }
        Ok(fragments)
    }
}

impl RemoteUploadPort for VfsRemoteUploadPort {
    fn adaptive_penalty_ms(&self) -> u64 {
        self.vfs.upload_scheduler.adaptive_penalty_ms()
    }

    fn fragment_keys(&self, object_id: u64) -> Vec<String> {
        format_object_fragment_keys(&self.vfs.data_prefix, object_id).into()
    }

    async fn all_fragments_visible(
        &self,
        op: &PendingUpload,
        fragment_keys: &[String],
        cancel_token: &CancellationToken,
    ) -> RemoteVisibility {
        let checks = fragment_keys.iter().map(|key| {
            let key = key.clone();
            let cancel_token = cancel_token.clone();
            async move {
                let result = run_with_timeout_or_cancel(
                    &cancel_token,
                    Duration::from_secs(5),
                    self.vfs.fetch_scheduler.data_store().exists(&key),
                )
                .await;
                (key, result)
            }
        });
        let results = join_all(checks).await;
        let mut all_visible = true;
        for (key, result) in results {
            match result {
                TimeoutOrCancel::Completed(Ok(true)) => {}
                TimeoutOrCancel::Completed(Ok(false)) => {
                    all_visible = false;
                    break;
                }
                TimeoutOrCancel::Completed(Err(error)) => {
                    all_visible = false;
                    debug!(
                        object_id = op.object_id,
                        key,
                        error = %error,
                        retry_count = op.retry_count,
                        "upload reconcile: remote exists() check failed, falling back to upload"
                    );
                    break;
                }
                TimeoutOrCancel::TimedOut => {
                    all_visible = false;
                    debug!(
                        object_id = op.object_id,
                        key,
                        retry_count = op.retry_count,
                        "upload reconcile: remote exists() check timed out, falling back to upload"
                    );
                    break;
                }
                TimeoutOrCancel::Cancelled => return RemoteVisibility::Cancelled,
            }
        }

        if all_visible {
            RemoteVisibility::Visible
        } else {
            RemoteVisibility::NotVisible
        }
    }

    async fn upload_fragments(
        &self,
        op: &PendingUpload,
        block_index: u32,
        raw_payload: Bytes,
        fragment_keys: &[String],
        cancel_token: &CancellationToken,
    ) -> UploadPutOutcome {
        let Some(_transit_credit) = self
            .vfs
            .transit_memory_budget
            .acquire_background_cancelable(raw_payload.len().max(1), cancel_token)
            .await
        else {
            return UploadPutOutcome::Cancelled;
        };
        let object_bytes = match Self::build_fragments(raw_payload) {
            Ok(fragments) => fragments,
            Err(error) => return UploadPutOutcome::Failed(error),
        };
        let upload_qos = match self.vfs.qos.acquire_upload().await {
            Ok(permit) => permit,
            Err(error) => {
                return UploadPutOutcome::Failed(VfsError::QosFailure {
                    stage: Cow::Borrowed("upload_qos"),
                    object_id: op.object_id,
                    message: error.to_string(),
                });
            }
        };
        let uploads = fragment_keys
            .iter()
            .zip(object_bytes.into_iter())
            .enumerate()
            .map(|(fragment_index, (key, payload))| {
                let key = key.clone();
                let cancel_token = cancel_token.clone();
                async move {
                    let err_key = key.clone();
                    self.vfs
                        .upload_scheduler
                        .put(&key, payload, &cancel_token)
                        .await
                        .map(|_| (fragment_index, key))
                        .map_err(|error| (fragment_index, err_key, error))
                }
            });
        let done = join_all(uploads).await;
        drop(upload_qos);

        let mut first_error = None;
        let mut saw_cancel = false;
        for result in done {
            match result {
                Ok((fragment_index, key)) => {
                    debug!(
                        ino = op.ino,
                        object_id = op.object_id,
                        block_index,
                        fragment_index,
                        key,
                        "process_upload fragment put completed"
                    );
                }
                Err((_, _, crate::upload::UploadError::Cancelled)) => {
                    saw_cancel = true;
                }
                Err((fragment_index, key, error)) => {
                    if first_error.is_none() {
                        first_error = Some((fragment_index, key, error));
                    }
                }
            }
        }
        if saw_cancel {
            return UploadPutOutcome::Cancelled;
        }
        if let Some((fragment_index, key, error)) = first_error {
            return UploadPutOutcome::Failed(VfsError::Other(format!(
                "fragment upload failed (idx={fragment_index}, key={key}): {error}"
            )));
        }
        UploadPutOutcome::Uploaded
    }
}

#[derive(Clone)]
struct VfsUploadShutdownPort {
    vfs: Arc<StudioVfs>,
}

impl UploadShutdownPort for VfsUploadShutdownPort {
    fn uploader_shutdown_requested(&self) -> bool {
        self.vfs.uploader_shutdown_requested()
    }

    fn shutdown_token(&self) -> CancellationToken {
        self.vfs.state.shutdown.uploader_shutdown_token.clone()
    }

    fn uploader_cancel_token_for_inode(&self, ino: Ino) -> CancellationToken {
        self.vfs.uploader_cancel_token_for_inode(ino)
    }

    fn maybe_release_upload_cancel_token(&self, ino: Ino) {
        self.vfs.maybe_release_upload_cancel_token(ino);
    }

    fn deleted_inode_marked(&self, ino: Ino) -> bool {
        self.vfs.state.upload.deleted_inodes.lock().contains(&ino)
    }

    fn maybe_clear_deleted_inode_marker(&self, ino: Ino) {
        self.vfs.maybe_clear_deleted_inode_marker(ino);
    }

    fn record_canceled_deleted(&self) {
        self.vfs
            .state
            .upload
            .upload_canceled_deleted
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_canceled_stale(&self) {
        self.vfs
            .state
            .upload
            .upload_canceled_stale
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_requeue(&self) {
        self.vfs
            .state
            .upload
            .upload_requeues
            .fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct VfsUploadPolicyPort {
    vfs: Arc<StudioVfs>,
}

impl UploadPolicyPort for VfsUploadPolicyPort {
    fn is_invalid_regular_file_graph_error(&self, error: &VfsError) -> bool {
        StudioVfs::is_invalid_regular_file_graph_error(error)
    }

    async fn invalid_regular_file_graph_attr_size(&self, ino: Ino) -> u64 {
        let meta = self.vfs.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| engine.get_attr(ino))
            .await
            .ok()
            .map(|attr| attr.size)
            .unwrap_or(0)
    }

    fn quarantine_invalid_regular_file_graph(&self, ino: Ino, size: u64, error: &VfsError) {
        self.vfs
            .quarantine_invalid_regular_file_graph(ino, size, error);
    }

    fn mark_upload_sticky_error(&self, ino: Ino, error: &VfsError) {
        self.vfs.mark_upload_sticky_error(ino, error);
    }

    fn clear_upload_sticky_error(&self, ino: Ino) {
        self.vfs.clear_upload_sticky_error(ino);
    }

    fn debug_pending_inode_state(&self, ino: Ino, event: &'static str) {
        self.vfs.debug_pending_inode_state(ino, event);
    }

    async fn refresh_incarnation_state(&self, ino: Ino) {
        let meta = self.vfs.meta.clone();
        if let Ok(attr) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| engine.get_attr(ino)).await
        {
            self.vfs
                .refresh_inode_incarnation_state_from_attr(ino, &attr);
        } else {
            self.vfs.clear_inode_incarnation(ino);
        }
    }
}

#[derive(Clone)]
pub(crate) struct UploadLifecycleWorker {
    vfs: Arc<StudioVfs>,
    processor: UploadProcessor<
        VfsUploadCoordinatorPort,
        VfsUploadLocalBlockPort,
        VfsUploadMetaPort,
        VfsRemoteUploadPort,
        VfsUploadShutdownPort,
        VfsUploadPolicyPort,
    >,
}

impl UploadLifecycleWorker {
    pub(crate) fn new(vfs: Arc<StudioVfs>) -> Self {
        let processor = UploadProcessor {
            coordinator: VfsUploadCoordinatorPort { vfs: vfs.clone() },
            local_blocks: VfsUploadLocalBlockPort { vfs: vfs.clone() },
            meta: VfsUploadMetaPort { vfs: vfs.clone() },
            remote: VfsRemoteUploadPort { vfs: vfs.clone() },
            shutdown: VfsUploadShutdownPort { vfs: vfs.clone() },
            policy: VfsUploadPolicyPort { vfs: vfs.clone() },
            block_size: vfs.block_size,
            upload_delay: vfs.upload_delay,
            upload_requeue_base: vfs.upload_requeue_base,
            upload_requeue_max: vfs.upload_requeue_max,
        };
        Self { vfs, processor }
    }

    pub(crate) async fn run(self, mut rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>) {
        info!("background uploader started");
        let worker_count = self.vfs.upload_worker_count.max(1);
        let permits = Arc::new(Semaphore::new(worker_count));
        let mut workers = JoinSet::new();
        info!(
            upload_worker_count = worker_count,
            "upload worker pool started"
        );

        let mut fair = crate::qos::FairInodeScheduler::<PendingUpload>::default();
        let mut delayed = DelayedUploads::default();
        let mut dispatched_since_yield = 0u32;
        let mut inbound_closed = false;
        loop {
            Self::reap_ready_workers(&mut workers);
            Self::promote_ready_uploads(&mut fair, &mut delayed);

            while !fair.is_empty() {
                Self::reap_ready_workers(&mut workers);
                let Some(permit) = permits.clone().try_acquire_owned().ok() else {
                    break;
                };
                let Some(op) = fair.pop() else {
                    drop(permit);
                    break;
                };
                debug!(
                    ino = op.ino,
                    object_id = op.object_id,
                    offset = op.offset,
                    content_epoch = op.content_epoch,
                    retry_count = op.retry_count,
                    not_before_unix_ms = op.not_before_unix_ms,
                    "uploader dispatch op"
                );
                let worker = self.clone();
                workers.spawn(async move {
                    worker.run_single_upload(op, permit).await;
                });
                dispatched_since_yield = dispatched_since_yield.saturating_add(1);
                if dispatched_since_yield >= UPLOAD_WORKER_YIELD_EVERY_OPS {
                    dispatched_since_yield = 0;
                    tokio::task::yield_now().await;
                    Self::reap_ready_workers(&mut workers);
                    Self::promote_ready_uploads(&mut fair, &mut delayed);
                }
            }

            if self.vfs.uploader_shutdown_requested() {
                break;
            }
            if inbound_closed && fair.is_empty() && delayed.is_empty() && workers.is_empty() {
                break;
            }

            let next_delayed_wait = if fair.is_empty() {
                Self::next_delayed_wait(&delayed)
            } else {
                None
            };
            let sleep_until_ready = async {
                match next_delayed_wait {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(sleep_until_ready);

            tokio::select! {
                biased;
                _ = self.vfs.state.shutdown.uploader_shutdown_token.cancelled() => break,
                Some(join_result) = workers.join_next(), if !workers.is_empty() => {
                    match join_result {
                        Ok(()) => {}
                        Err(error) if error.is_cancelled() => {}
                        Err(error) => warn!(error = %error, "uploader worker join failed"),
                    }
                }
                inbound = rx.recv(), if !inbound_closed => {
                    match inbound {
                        Some(op) => Self::enqueue_pending_upload(&mut fair, &mut delayed, op),
                        None => inbound_closed = true,
                    }
                }
                _ = &mut sleep_until_ready => {}
            }
        }

        self.drain_workers(&mut workers).await;
    }

    fn enqueue_pending_upload(
        fair: &mut crate::qos::FairInodeScheduler<PendingUpload>,
        delayed: &mut DelayedUploads,
        op: PendingUpload,
    ) {
        if op.not_before_unix_ms > now_unix_millis() {
            delayed
                .entry(op.not_before_unix_ms)
                .or_default()
                .push_back(op);
        } else {
            fair.push(op.ino, op);
        }
    }

    fn promote_ready_uploads(
        fair: &mut crate::qos::FairInodeScheduler<PendingUpload>,
        delayed: &mut DelayedUploads,
    ) {
        loop {
            let Some((&not_before_unix_ms, _)) = delayed.first_key_value() else {
                break;
            };
            if not_before_unix_ms > now_unix_millis() {
                break;
            }
            let Some((_, mut ready_ops)) = delayed.pop_first() else {
                break;
            };
            while let Some(op) = ready_ops.pop_front() {
                fair.push(op.ino, op);
            }
        }
    }

    fn next_delayed_wait(delayed: &DelayedUploads) -> Option<Duration> {
        delayed.first_key_value().map(|(&not_before_unix_ms, _)| {
            Duration::from_millis(not_before_unix_ms.saturating_sub(now_unix_millis()))
        })
    }

    fn reap_ready_workers(workers: &mut JoinSet<()>) {
        while let Some(join_result) = workers.try_join_next() {
            match join_result {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => warn!(error = %error, "uploader worker join failed"),
            }
        }
    }

    async fn drain_workers(&self, workers: &mut JoinSet<()>) {
        let join_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if workers.is_empty() {
                break;
            }
            let remaining = join_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                break;
            }
            match tokio::time::timeout(remaining, workers.join_next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(error))) if error.is_cancelled() => {}
                Ok(Some(Err(error))) => {
                    warn!(error = %error, "uploader worker join failed");
                }
                Ok(None) => break,
                Err(_) => {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    break;
                }
            }
        }
    }

    async fn run_single_upload(&self, op: PendingUpload, _permit: OwnedSemaphorePermit) {
        if self.vfs.uploader_shutdown_requested() {
            return;
        }

        self.vfs.state.file_session.note_background_transition(
            op.ino,
            FileSessionOperation::Upload,
            "start",
        );
        debug!(
            ino = op.ino,
            object_id = op.object_id,
            offset = op.offset,
            content_epoch = op.content_epoch,
            retry_count = op.retry_count,
            not_before_unix_ms = op.not_before_unix_ms,
            "process_upload start"
        );

        let outcome = self.processor.process_upload(op.clone()).await;
        if outcome == UploadOutcome::Completed {
            self.vfs.state.file_session.note_background_transition(
                op.ino,
                FileSessionOperation::Upload,
                "complete",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    type FakeBlockMap = HashMap<(Ino, u32), Option<BlockRef>>;
    type FakePendingInodes = Arc<Mutex<HashSet<Ino>>>;
    type FakeProcessor = UploadProcessor<
        FakeCoordinator,
        FakeLocalBlocks,
        FakeMeta,
        FakeRemote,
        FakeShutdown,
        FakePolicy,
    >;

    #[derive(Clone, Default)]
    struct FakeCoordinator {
        completed: Arc<Mutex<Vec<u64>>>,
        cancelled: Arc<Mutex<Vec<u64>>>,
        requeued: Arc<Mutex<Vec<PendingUpload>>>,
        pending_inodes: FakePendingInodes,
    }

    impl UploadCoordinatorPort for FakeCoordinator {
        fn complete_upload(&self, op: &PendingUpload) {
            self.completed.lock().push(op.object_id);
            self.pending_inodes.lock().remove(&op.ino);
        }

        fn requeue_upload(&self, op: PendingUpload) -> bool {
            self.requeued.lock().push(op);
            true
        }

        fn cancel_upload(&self, op: &PendingUpload) {
            self.cancelled.lock().push(op.object_id);
            self.pending_inodes.lock().remove(&op.ino);
        }
    }

    #[derive(Clone, Default)]
    struct FakeLocalBlocks {
        upload_reads: Arc<Mutex<HashMap<u64, Option<Bytes>>>>,
        removed: Arc<Mutex<Vec<u64>>>,
        uploaded: Arc<Mutex<Vec<u64>>>,
        failed: Arc<Mutex<Vec<u64>>>,
        deferred: Arc<Mutex<Vec<(Ino, u64)>>>,
        pending_evictions: Arc<Mutex<HashSet<u64>>>,
        open_handles: Arc<Mutex<HashMap<Ino, usize>>>,
    }

    impl UploadLocalBlockPort for FakeLocalBlocks {
        fn mark_uploading(&self, _object_id: u64) {}

        fn mark_upload_failed(&self, object_id: u64) {
            self.failed.lock().push(object_id);
        }

        async fn read_for_upload(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
            Ok(self
                .upload_reads
                .lock()
                .get(&object_id)
                .cloned()
                .unwrap_or_else(|| Some(Bytes::from_static(b"payload"))))
        }

        async fn remove(&self, object_id: u64) -> VfsResult<()> {
            self.removed.lock().push(object_id);
            Ok(())
        }

        async fn mark_uploaded(&self, object_id: u64) -> VfsResult<()> {
            self.uploaded.lock().push(object_id);
            Ok(())
        }

        fn open_handle_count(&self, ino: Ino) -> usize {
            self.open_handles.lock().get(&ino).copied().unwrap_or(0)
        }

        fn record_deferred_deleted_local_block(&self, ino: Ino, object_id: u64) {
            self.deferred.lock().push((ino, object_id));
        }

        fn clear_pending_eviction(&self, object_id: u64) -> bool {
            self.pending_evictions.lock().remove(&object_id)
        }
    }

    #[derive(Clone)]
    struct FakeMeta {
        attrs: Arc<Mutex<HashMap<Ino, Option<InodeAttr>>>>,
        blocks: Arc<Mutex<FakeBlockMap>>,
        pending_deletions: Arc<Mutex<Vec<u64>>>,
    }

    impl Default for FakeMeta {
        fn default() -> Self {
            Self {
                attrs: Arc::new(Mutex::new(HashMap::new())),
                blocks: Arc::new(Mutex::new(HashMap::new())),
                pending_deletions: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl UploadMetaPort for FakeMeta {
        async fn get_attr(&self, ino: Ino) -> VfsResult<Option<InodeAttr>> {
            Ok(self.attrs.lock().get(&ino).cloned().flatten())
        }

        async fn get_file_block(&self, ino: Ino, block_index: u32) -> VfsResult<Option<BlockRef>> {
            Ok(self
                .blocks
                .lock()
                .get(&(ino, block_index))
                .cloned()
                .flatten())
        }

        async fn attr_matches_content_epoch(&self, ino: Ino, content_epoch: u64) -> bool {
            self.attrs
                .lock()
                .get(&ino)
                .and_then(|attr| attr.as_ref())
                .map(|attr| attr.content_epoch == content_epoch)
                .unwrap_or(false)
        }

        async fn add_pending_deletion(&self, object_id: u64, _timestamp: i64) -> VfsResult<()> {
            self.pending_deletions.lock().push(object_id);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FakeRemote {
        adaptive_penalty_ms: u64,
        visibility: Arc<Mutex<RemoteVisibility>>,
        upload_result: Arc<Mutex<Option<UploadPutOutcome>>>,
    }

    impl Default for FakeRemote {
        fn default() -> Self {
            Self {
                adaptive_penalty_ms: 0,
                visibility: Arc::new(Mutex::new(RemoteVisibility::NotVisible)),
                upload_result: Arc::new(Mutex::new(Some(UploadPutOutcome::Uploaded))),
            }
        }
    }

    impl RemoteUploadPort for FakeRemote {
        fn adaptive_penalty_ms(&self) -> u64 {
            self.adaptive_penalty_ms
        }

        fn fragment_keys(&self, object_id: u64) -> Vec<String> {
            vec![format!("objects/{object_id}")]
        }

        async fn all_fragments_visible(
            &self,
            _op: &PendingUpload,
            _fragment_keys: &[String],
            _cancel_token: &CancellationToken,
        ) -> RemoteVisibility {
            *self.visibility.lock()
        }

        async fn upload_fragments(
            &self,
            _op: &PendingUpload,
            _block_index: u32,
            _raw_payload: Bytes,
            _fragment_keys: &[String],
            _cancel_token: &CancellationToken,
        ) -> UploadPutOutcome {
            self.upload_result
                .lock()
                .take()
                .unwrap_or(UploadPutOutcome::Uploaded)
        }
    }

    #[derive(Clone)]
    struct FakeShutdown {
        shutdown_requested: Arc<AtomicBool>,
        deleted_inodes: Arc<Mutex<HashSet<Ino>>>,
        pending_inodes: FakePendingInodes,
        cleared_deleted: Arc<Mutex<Vec<Ino>>>,
        released_tokens: Arc<Mutex<Vec<Ino>>>,
        canceled_deleted: Arc<AtomicU64>,
        canceled_stale: Arc<AtomicU64>,
        requeues: Arc<AtomicU64>,
        shutdown_token: CancellationToken,
    }

    impl Default for FakeShutdown {
        fn default() -> Self {
            Self {
                shutdown_requested: Arc::new(AtomicBool::new(false)),
                deleted_inodes: Arc::new(Mutex::new(HashSet::new())),
                pending_inodes: Arc::new(Mutex::new(HashSet::new())),
                cleared_deleted: Arc::new(Mutex::new(Vec::new())),
                released_tokens: Arc::new(Mutex::new(Vec::new())),
                canceled_deleted: Arc::new(AtomicU64::new(0)),
                canceled_stale: Arc::new(AtomicU64::new(0)),
                requeues: Arc::new(AtomicU64::new(0)),
                shutdown_token: CancellationToken::new(),
            }
        }
    }

    impl UploadShutdownPort for FakeShutdown {
        fn uploader_shutdown_requested(&self) -> bool {
            self.shutdown_requested.load(Ordering::Relaxed)
        }

        fn shutdown_token(&self) -> CancellationToken {
            self.shutdown_token.clone()
        }

        fn uploader_cancel_token_for_inode(&self, _ino: Ino) -> CancellationToken {
            CancellationToken::new()
        }

        fn maybe_release_upload_cancel_token(&self, ino: Ino) {
            if !self.pending_inodes.lock().contains(&ino) {
                self.released_tokens.lock().push(ino);
            }
        }

        fn deleted_inode_marked(&self, ino: Ino) -> bool {
            self.deleted_inodes.lock().contains(&ino)
        }

        fn maybe_clear_deleted_inode_marker(&self, ino: Ino) {
            self.cleared_deleted.lock().push(ino);
            self.deleted_inodes.lock().remove(&ino);
        }

        fn record_canceled_deleted(&self) {
            self.canceled_deleted.fetch_add(1, Ordering::Relaxed);
        }

        fn record_canceled_stale(&self) {
            self.canceled_stale.fetch_add(1, Ordering::Relaxed);
        }

        fn record_requeue(&self) {
            self.requeues.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Clone, Default)]
    struct FakePolicy {
        sticky_errors: Arc<Mutex<Vec<(Ino, String)>>>,
        quarantines: Arc<Mutex<Vec<(Ino, u64, String)>>>,
        refreshed: Arc<Mutex<Vec<Ino>>>,
        invalid_graph: Arc<AtomicBool>,
    }

    impl UploadPolicyPort for FakePolicy {
        fn is_invalid_regular_file_graph_error(&self, _error: &VfsError) -> bool {
            self.invalid_graph.load(Ordering::Relaxed)
        }

        async fn invalid_regular_file_graph_attr_size(&self, _ino: Ino) -> u64 {
            0
        }

        fn quarantine_invalid_regular_file_graph(&self, ino: Ino, size: u64, error: &VfsError) {
            self.quarantines.lock().push((ino, size, error.to_string()));
        }

        fn mark_upload_sticky_error(&self, ino: Ino, error: &VfsError) {
            self.sticky_errors.lock().push((ino, error.to_string()));
        }

        fn clear_upload_sticky_error(&self, _ino: Ino) {}

        fn debug_pending_inode_state(&self, _ino: Ino, _event: &'static str) {}

        async fn refresh_incarnation_state(&self, ino: Ino) {
            self.refreshed.lock().push(ino);
        }
    }

    fn make_attr(ino: Ino, content_epoch: u64) -> InodeAttr {
        let mut attr = InodeAttr::new_file(ino, 1, 0, 0, 0o644);
        attr.content_epoch = content_epoch;
        attr
    }

    fn processor_fixture() -> (
        FakeProcessor,
        FakeCoordinator,
        FakeLocalBlocks,
        FakeMeta,
        FakeRemote,
        FakeShutdown,
        FakePolicy,
    ) {
        let pending_inodes = Arc::new(Mutex::new(HashSet::new()));
        let coordinator = FakeCoordinator {
            pending_inodes: pending_inodes.clone(),
            ..FakeCoordinator::default()
        };
        let local_blocks = FakeLocalBlocks::default();
        let meta = FakeMeta::default();
        let remote = FakeRemote::default();
        let shutdown = FakeShutdown {
            pending_inodes: pending_inodes.clone(),
            ..FakeShutdown::default()
        };
        let policy = FakePolicy::default();
        let processor = UploadProcessor {
            coordinator: coordinator.clone(),
            local_blocks: local_blocks.clone(),
            meta: meta.clone(),
            remote: remote.clone(),
            shutdown: shutdown.clone(),
            policy: policy.clone(),
            block_size: 4096,
            upload_delay: Duration::ZERO,
            upload_requeue_base: Duration::ZERO,
            upload_requeue_max: Duration::ZERO,
        };
        (
            processor,
            coordinator,
            local_blocks,
            meta,
            remote,
            shutdown,
            policy,
        )
    }

    fn make_pending_upload() -> PendingUpload {
        PendingUpload {
            ino: 11,
            offset: 0,
            object_id: 77,
            content_epoch: 3,
            created_unix_ms: 0,
            not_before_unix_ms: 0,
            retry_count: 0,
        }
    }

    #[tokio::test]
    async fn process_upload_requeues_transient_failures() {
        let (mut processor, coordinator, local_blocks, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        let before_retry = now_unix_millis();
        processor.upload_requeue_base = Duration::from_millis(50);
        processor.upload_requeue_max = Duration::from_millis(50);
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.blocks
            .lock()
            .insert((op.ino, 0), Some(BlockRef::new_partial(op.object_id, 4096)));
        *remote.upload_result.lock() =
            Some(UploadPutOutcome::Failed(VfsError::Other("boom".into())));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::Requeued);
        assert_eq!(local_blocks.failed.lock().as_slice(), &[77]);
        assert_eq!(policy.sticky_errors.lock().len(), 1);
        assert_eq!(shutdown.requeues.load(Ordering::Relaxed), 1);
        let requeued = coordinator.requeued.lock();
        assert_eq!(requeued.len(), 1);
        assert_eq!(requeued[0].retry_count, 1);
        assert!(requeued[0].not_before_unix_ms >= before_retry.saturating_add(40));
        assert!(shutdown.released_tokens.lock().is_empty());
    }

    #[tokio::test]
    async fn process_upload_cancels_deleted_inodes_before_reading() {
        let (processor, coordinator, local_blocks, _meta, _remote, shutdown, _policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        shutdown.deleted_inodes.lock().insert(op.ino);

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::CancelledDeleted);
        assert_eq!(coordinator.cancelled.lock().as_slice(), &[77]);
        assert_eq!(local_blocks.removed.lock().as_slice(), &[77]);
        assert_eq!(shutdown.canceled_deleted.load(Ordering::Relaxed), 1);
        assert_eq!(shutdown.released_tokens.lock().as_slice(), &[11]);
    }

    #[tokio::test]
    async fn process_upload_cancels_stale_content_epoch() {
        let (processor, coordinator, local_blocks, meta, _remote, shutdown, _policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch + 1)));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::CancelledStale);
        assert_eq!(coordinator.cancelled.lock().as_slice(), &[77]);
        assert_eq!(local_blocks.removed.lock().as_slice(), &[77]);
        assert_eq!(shutdown.canceled_stale.load(Ordering::Relaxed), 1);
        assert_eq!(shutdown.released_tokens.lock().as_slice(), &[11]);
    }

    #[tokio::test]
    async fn delayed_retry_ops_wait_until_due_before_dispatch() {
        let mut fair = crate::qos::FairInodeScheduler::<PendingUpload>::default();
        let mut delayed = DelayedUploads::default();
        let mut op = make_pending_upload();
        op.not_before_unix_ms = now_unix_millis().saturating_add(20);

        UploadLifecycleWorker::enqueue_pending_upload(&mut fair, &mut delayed, op.clone());
        UploadLifecycleWorker::promote_ready_uploads(&mut fair, &mut delayed);

        assert!(fair.is_empty());
        assert_eq!(delayed.len(), 1);

        tokio::time::sleep(Duration::from_millis(25)).await;
        UploadLifecycleWorker::promote_ready_uploads(&mut fair, &mut delayed);

        let Some(ready) = fair.pop() else {
            panic!("expected delayed upload to become dispatchable");
        };
        assert_eq!(ready.object_id, op.object_id);
        assert!(delayed.is_empty());
    }
}

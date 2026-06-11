use super::super::{FileSessionOperation, Ino, InodeAttr, StudioVfs, VfsError, VfsResult};
use crate::VfsMeta;
use crate::runtime::{TimeoutOrCancel, run_meta, run_with_timeout_or_cancel};
use crate::utils::now_unix_millis;
use bytes::Bytes;
use futures::future::join_all;
use opendal::Buffer;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use studiofs_cache::upload_queue::PendingUpload;
use studiofs_data::object::{
    CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE, create_object_buffer, format_object_fragment_keys,
};
use studiofs_meta::schema::{
    ChunkId, DEFAULT_CHUNK_SIZE, FileType, PayloadRef, PayloadRefState, ResolvedSliceSource,
    ResolvedSliceSpan, SliceId, SliceOrder, SliceRange, SliceRecord, block_index_from_offset,
    chunk_id_from_offset, chunk_start_offset,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const UPLOAD_WORKER_YIELD_EVERY_OPS: u32 = super::super::UPLOAD_WORKER_YIELD_EVERY_OPS;
const FOREGROUND_UPLOAD_REREAD_LIMIT_BYTES_PER_SEC: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Default)]
struct ReadyUploads {
    per_inode: BTreeMap<Ino, VecDeque<PendingUpload>>,
    ready_inodes: VecDeque<Ino>,
    ready_set: HashSet<Ino>,
}

type DelayedUploads = BTreeMap<u64, VecDeque<PendingUpload>>;

impl ReadyUploads {
    fn push_back(&mut self, op: PendingUpload) {
        let ino = op.ino;
        self.per_inode.entry(ino).or_default().push_back(op);
        if self.ready_set.insert(ino) {
            self.ready_inodes.push_back(ino);
        }
    }

    fn pop_front(&mut self) -> Option<PendingUpload> {
        while let Some(ino) = self.ready_inodes.pop_front() {
            self.ready_set.remove(&ino);
            let mut remove_inode = false;
            let item = {
                let queue = self.per_inode.get_mut(&ino)?;
                let item = queue.pop_front();
                if queue.is_empty() {
                    remove_inode = true;
                } else {
                    self.ready_inodes.push_back(ino);
                    self.ready_set.insert(ino);
                }
                item
            };

            if remove_inode {
                self.per_inode.remove(&ino);
            }

            if item.is_some() {
                return item;
            }
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.per_inode.is_empty()
    }

    fn remove_inodes(&mut self, inos: &HashSet<Ino>) -> Vec<PendingUpload> {
        if inos.is_empty() {
            return Vec::new();
        }

        let mut removed = Vec::new();
        for ino in inos {
            if let Some(mut queue) = self.per_inode.remove(ino) {
                removed.extend(queue.drain(..));
                self.ready_set.remove(ino);
            }
        }
        if !removed.is_empty() {
            self.ready_inodes.retain(|ino| !inos.contains(ino));
        }
        removed
    }
}

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

#[derive(Clone, Copy, Debug)]
struct UploadedObjectReconcile {
    keep_local_object: bool,
    finalize_content_epoch: u64,
}

const POST_PUT_STALE_RECONCILE_ATTEMPTS: usize = 16;
const POST_PUT_STALE_RECONCILE_DELAY_MS: u64 = 5;
const POST_PUT_VISIBILITY_ATTEMPTS: usize = 8;
const POST_PUT_VISIBILITY_INITIAL_DELAY_MS: u64 = 25;
const POST_PUT_VISIBILITY_MAX_DELAY_MS: u64 = 1_000;

fn post_put_visibility_delay(attempt: usize) -> Duration {
    let multiplier = 1u64.checked_shl(attempt.min(20) as u32).unwrap_or(u64::MAX);
    Duration::from_millis(
        POST_PUT_VISIBILITY_INITIAL_DELAY_MS
            .saturating_mul(multiplier)
            .min(POST_PUT_VISIBILITY_MAX_DELAY_MS),
    )
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

fn upload_reread_idle_wait_window(
    foreground_active: bool,
    adaptive_penalty: Duration,
    idle_grace: Duration,
) -> Duration {
    if !foreground_active || adaptive_penalty.is_zero() {
        Duration::ZERO
    } else {
        adaptive_penalty.max(idle_grace)
    }
}

fn upload_disk_reread_defer_delay(
    foreground_active: bool,
    hot_payload_available: bool,
    adaptive_penalty: Duration,
    idle_grace: Duration,
) -> Option<Duration> {
    if hot_payload_available {
        return None;
    }
    let wait = upload_reread_idle_wait_window(foreground_active, adaptive_penalty, idle_grace);
    (!wait.is_zero()).then_some(wait)
}

fn foreground_upload_reread_slot_cost(
    foreground_active: bool,
    foreground_cost: u32,
    max_slots: u32,
) -> u32 {
    if foreground_active {
        foreground_cost.clamp(1, max_slots.max(1))
    } else {
        1
    }
}

fn foreground_upload_reread_pacing_delay(foreground_active: bool, reread_bytes: usize) -> Duration {
    if !foreground_active || reread_bytes == 0 {
        return Duration::ZERO;
    }
    let limit = FOREGROUND_UPLOAD_REREAD_LIMIT_BYTES_PER_SEC.max(1) as u128;
    let nanos = (reread_bytes as u128)
        .saturating_mul(1_000_000_000u128)
        .div_ceil(limit)
        .min(u64::MAX as u128) as u64;
    Duration::from_nanos(nanos)
}

fn uploaded_object_reattach_slices(
    spans: &[ResolvedSliceSpan],
    object_id: u64,
    offset: u64,
    payload_len: u64,
    block_size: u32,
) -> Option<Vec<(ChunkId, Vec<SliceRecord>)>> {
    let request_end = offset.checked_add(payload_len)?;
    let mut chunk_slices: BTreeMap<ChunkId, Vec<SliceRecord>> = BTreeMap::new();

    for span in spans {
        let span_start = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE)
            .checked_add(u64::from(span.range.start))?;
        let span_end = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE)
            .checked_add(u64::from(span.range.end))?;
        let start = span_start.max(offset);
        let end = span_end.min(request_end);
        if start >= end {
            continue;
        }

        let should_reattach = match span.source {
            ResolvedSliceSource::Hole => span.payload.is_none(),
            ResolvedSliceSource::Overlay | ResolvedSliceSource::Base => span
                .payload
                .as_ref()
                .is_some_and(|payload| payload.object_id == object_id),
            ResolvedSliceSource::OverlayTombstone | ResolvedSliceSource::BaseTombstone => {
                return None;
            }
        };

        if should_reattach {
            append_uploaded_object_reattach_slices(
                &mut chunk_slices,
                object_id,
                offset,
                start,
                end,
                block_size,
            )?;
        }
    }

    if chunk_slices.is_empty() {
        return None;
    }
    Some(chunk_slices.into_iter().collect())
}

fn append_uploaded_object_reattach_slices(
    chunk_slices: &mut BTreeMap<ChunkId, Vec<SliceRecord>>,
    object_id: u64,
    object_file_offset: u64,
    start: u64,
    end: u64,
    block_size: u32,
) -> Option<()> {
    let mut cursor = start;
    while cursor < end {
        let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
        let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
        let chunk_end = chunk_start.checked_add(u64::from(DEFAULT_CHUNK_SIZE))?;
        let part_end = end.min(chunk_end);
        let chunk_offset = u32::try_from(cursor.checked_sub(chunk_start)?).ok()?;
        let part_len = u32::try_from(part_end.checked_sub(cursor)?).ok()?;
        let object_offset = u32::try_from(cursor.checked_sub(object_file_offset)?).ok()?;
        let range = SliceRange::new(chunk_offset, chunk_offset.checked_add(part_len)?)?;
        let mut payload = if part_len == block_size {
            PayloadRef::new_full(object_id, part_len)
        } else {
            PayloadRef::new_partial(object_id, part_len)
        };
        payload.offset = object_offset;

        let mut slice = SliceRecord::new(SliceId(0), SliceOrder(0), range, payload);
        slice.payload_state = PayloadRefState::Uploaded;
        chunk_slices.entry(chunk_id).or_default().push(slice);
        cursor = part_end;
    }
    Some(())
}

trait CachedStorePort: Clone + Send + Sync + 'static {
    fn complete_upload(&self, op: &PendingUpload);
    fn requeue_upload(&self, op: PendingUpload) -> bool;
    fn cancel_upload(&self, op: &PendingUpload);
}

trait UploadLocalPayloadPort: Clone + Send + Sync + 'static {
    fn mark_uploading(&self, object_id: u64);
    fn mark_upload_failed(&self, object_id: u64);
    async fn prepare_local_priority_read(
        &self,
        object_id: u64,
        adaptive_penalty: Duration,
    ) -> VfsResult<()>;
    async fn read_for_upload(&self, object_id: u64) -> VfsResult<Option<Bytes>>;
    async fn remove(&self, object_id: u64) -> VfsResult<()>;
    async fn mark_uploaded(&self, object_id: u64) -> VfsResult<()>;
    async fn remove_upload_hot(&self, object_id: u64) -> VfsResult<()>;
    async fn remove_upload_stage(&self, object_id: u64) -> VfsResult<()>;
    fn open_handle_count(&self, ino: Ino) -> usize;
    fn record_deferred_deleted_local_payload(&self, ino: Ino, object_id: u64);
    fn clear_pending_eviction(&self, object_id: u64) -> bool;
}

trait UploadMetaPort: Clone + Send + Sync + 'static {
    async fn get_attr(&self, ino: Ino) -> VfsResult<Option<InodeAttr>>;
    async fn has_regular_file_overlay_object_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> VfsResult<bool>;
    async fn mark_regular_file_overlay_object_uploaded_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> VfsResult<bool>;
    async fn reattach_uploaded_object_if_uncovered(
        &self,
        ino: Ino,
        content_epoch: u64,
        object_id: u64,
        offset: u64,
        payload_len: u64,
        block_size: u32,
    ) -> VfsResult<Option<u64>>;
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
    fn record_canceled_stale_after_put(&self, bytes_uploaded: u64);
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
    async fn finalize_completed_upload_epoch(&self, ino: Ino, content_epoch: u64);
}

#[derive(Clone)]
struct UploadProcessor<C, L, M, R, S, P> {
    coordinator: C,
    local_payloads: L,
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
    C: CachedStorePort,
    L: UploadLocalPayloadPort,
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
                    self.local_payloads.mark_upload_failed(op.object_id);
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

                self.local_payloads.mark_upload_failed(op.object_id);
                self.policy.mark_upload_sticky_error(op.ino, &error);

                let mut retry_op = op;
                retry_op.retry_count = retry_op.retry_count.saturating_add(1);
                let delay = self.upload_requeue_delay(&retry_op);
                retry_op.not_before_unix_ms = now_unix_millis()
                    .saturating_add(delay.as_millis().clamp(0, u64::MAX as u128) as u64);
                self.shutdown.record_requeue();
                debug!(
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

        let overlay_object_current = self
            .meta
            .has_regular_file_overlay_object_if_matches(op.ino, op.content_epoch, op.object_id)
            .await?;

        if !overlay_object_current {
            self.cancel_stale_upload(op).await?;
            return Ok(SingleUploadOutcome::CancelledStale);
        }

        self.local_payloads
            .prepare_local_priority_read(op.object_id, Duration::from_millis(adaptive_penalty_ms))
            .await?;

        let t0 = Instant::now();
        self.local_payloads.mark_uploading(op.object_id);
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
            self.local_payloads.read_for_upload(op.object_id).await?
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
                                "object {} missing from local payload backend",
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
                        "object {} missing from local payload backend",
                        op.object_id
                    )));
                }
            }
        };

        let payload_len = raw_payload.len() as u64;
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
            let post_put_cancel_token = self.shutdown.shutdown_token();
            match self
                .wait_for_uploaded_fragments_visible(op, &fragment_keys, &post_put_cancel_token)
                .await
            {
                RemoteVisibility::Visible => {}
                RemoteVisibility::Cancelled => {
                    return if self.cancel_upload_if_inode_deleted(op).await? {
                        Ok(SingleUploadOutcome::CancelledDeleted)
                    } else {
                        Ok(SingleUploadOutcome::Shutdown)
                    };
                }
                RemoteVisibility::NotVisible => {
                    if self.cancel_upload_if_inode_deleted(op).await? {
                        return Ok(SingleUploadOutcome::CancelledDeleted);
                    }
                    return Err(VfsError::Other(format!(
                        "uploaded object {} fragments were not visible after remote put",
                        op.object_id
                    )));
                }
            }
        }

        if self.cancel_upload_if_inode_deleted(op).await? {
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                "process_upload canceled: inode deleted after remote put"
            );
            return Ok(SingleUploadOutcome::CancelledDeleted);
        }
        if upload_cancel_token.is_cancelled() {
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                "process_upload continuing metadata reconciliation after post-PUT upload token cancellation"
            );
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0);
        let mut reconcile = self
            .reconcile_uploaded_object_metadata(op, payload_len)
            .await?;
        if reconcile.is_none() && !self.shutdown.uploader_shutdown_requested() {
            for _ in 0..POST_PUT_STALE_RECONCILE_ATTEMPTS {
                let shutdown_token = self.shutdown.shutdown_token();
                tokio::select! {
                    _ = shutdown_token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(POST_PUT_STALE_RECONCILE_DELAY_MS)) => {}
                }
                if self.cancel_upload_if_inode_deleted(op).await? {
                    debug!(
                        object_id = op.object_id,
                        ino = op.ino,
                        "process_upload canceled: inode deleted during post-PUT metadata reconciliation"
                    );
                    return Ok(SingleUploadOutcome::CancelledDeleted);
                }
                reconcile = self
                    .reconcile_uploaded_object_metadata(op, payload_len)
                    .await?;
                if reconcile.is_some() {
                    break;
                }
            }
        }

        let (keep_local_object, finalize_content_epoch) = if let Some(reconcile) = reconcile {
            (
                reconcile.keep_local_object,
                reconcile.finalize_content_epoch,
            )
        } else {
            self.shutdown.record_canceled_stale_after_put(payload_len);
            self.meta
                .add_pending_deletion(op.object_id, timestamp)
                .await?;
            (false, op.content_epoch)
        };
        self.coordinator.complete_upload(op);

        self.policy.refresh_incarnation_state(op.ino).await;
        self.policy
            .finalize_completed_upload_epoch(op.ino, finalize_content_epoch)
            .await;
        self.policy.clear_upload_sticky_error(op.ino);
        self.policy
            .debug_pending_inode_state(op.ino, "upload_completed");

        if self.local_payloads.clear_pending_eviction(op.object_id) {
            self.local_payloads.remove(op.object_id).await?;
        } else if keep_local_object {
            self.local_payloads.mark_uploaded(op.object_id).await?;
        } else {
            self.local_payloads.remove(op.object_id).await?;
        }
        self.local_payloads.remove_upload_hot(op.object_id).await?;
        self.local_payloads
            .remove_upload_stage(op.object_id)
            .await?;

        debug!(
            object_id = op.object_id,
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "background upload complete"
        );
        Ok(SingleUploadOutcome::Completed)
    }

    async fn reconcile_uploaded_object_metadata(
        &self,
        op: &PendingUpload,
        payload_len: u64,
    ) -> VfsResult<Option<UploadedObjectReconcile>> {
        if self
            .meta
            .has_regular_file_overlay_object_if_matches(op.ino, op.content_epoch, op.object_id)
            .await?
            && self
                .meta
                .mark_regular_file_overlay_object_uploaded_if_matches(
                    op.ino,
                    op.content_epoch,
                    op.object_id,
                )
                .await?
        {
            return Ok(Some(UploadedObjectReconcile {
                keep_local_object: true,
                finalize_content_epoch: op.content_epoch,
            }));
        }

        if let Some(latest_attr) = self.meta.get_attr(op.ino).await?
            && latest_attr.content_epoch != op.content_epoch
            && self
                .meta
                .has_regular_file_overlay_object_if_matches(
                    op.ino,
                    latest_attr.content_epoch,
                    op.object_id,
                )
                .await?
            && self
                .meta
                .mark_regular_file_overlay_object_uploaded_if_matches(
                    op.ino,
                    latest_attr.content_epoch,
                    op.object_id,
                )
                .await?
        {
            return Ok(Some(UploadedObjectReconcile {
                keep_local_object: true,
                finalize_content_epoch: latest_attr.content_epoch,
            }));
        }

        if let Some(rebased_epoch) = self
            .meta
            .reattach_uploaded_object_if_uncovered(
                op.ino,
                op.content_epoch,
                op.object_id,
                op.offset,
                payload_len,
                self.block_size,
            )
            .await?
        {
            return Ok(Some(UploadedObjectReconcile {
                keep_local_object: true,
                finalize_content_epoch: rebased_epoch,
            }));
        }

        Ok(None)
    }

    async fn wait_for_uploaded_fragments_visible(
        &self,
        op: &PendingUpload,
        fragment_keys: &[String],
        upload_cancel_token: &CancellationToken,
    ) -> RemoteVisibility {
        for attempt in 0..POST_PUT_VISIBILITY_ATTEMPTS {
            match self
                .remote
                .all_fragments_visible(op, fragment_keys, upload_cancel_token)
                .await
            {
                RemoteVisibility::Visible => return RemoteVisibility::Visible,
                RemoteVisibility::Cancelled => return RemoteVisibility::Cancelled,
                RemoteVisibility::NotVisible => {}
            }

            if attempt + 1 == POST_PUT_VISIBILITY_ATTEMPTS {
                break;
            }

            let shutdown_token = self.shutdown.shutdown_token();
            tokio::select! {
                _ = shutdown_token.cancelled() => return RemoteVisibility::Cancelled,
                _ = upload_cancel_token.cancelled() => return RemoteVisibility::Cancelled,
                _ = tokio::time::sleep(post_put_visibility_delay(attempt)) => {}
            }
        }

        RemoteVisibility::NotVisible
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
        let has_open_handles = self.local_payloads.open_handle_count(op.ino) > 0;
        self.coordinator.cancel_upload(op);
        if has_open_handles {
            self.local_payloads
                .record_deferred_deleted_local_payload(op.ino, op.object_id);
        } else {
            self.local_payloads.remove(op.object_id).await?;
        }
        self.local_payloads.remove_upload_hot(op.object_id).await?;
        self.local_payloads
            .remove_upload_stage(op.object_id)
            .await?;
        self.local_payloads.clear_pending_eviction(op.object_id);
        self.policy.refresh_incarnation_state(op.ino).await;
        self.policy
            .finalize_completed_upload_epoch(op.ino, op.content_epoch)
            .await;
        Ok(())
    }

    async fn cancel_stale_upload(&self, op: &PendingUpload) -> VfsResult<()> {
        self.shutdown.record_canceled_stale();
        self.coordinator.cancel_upload(op);
        let still_referenced = match self.meta.get_attr(op.ino).await? {
            Some(attr) => {
                self.meta
                    .has_regular_file_overlay_object_if_matches(
                        op.ino,
                        attr.content_epoch,
                        op.object_id,
                    )
                    .await?
            }
            None => false,
        };
        if still_referenced {
            self.local_payloads.mark_upload_failed(op.object_id);
            self.local_payloads.clear_pending_eviction(op.object_id);
            return Ok(());
        }
        self.local_payloads.remove(op.object_id).await?;
        self.local_payloads.remove_upload_hot(op.object_id).await?;
        self.local_payloads
            .remove_upload_stage(op.object_id)
            .await?;
        self.local_payloads.clear_pending_eviction(op.object_id);
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
struct VfsCachedStorePort {
    vfs: Arc<StudioVfs>,
}

impl CachedStorePort for VfsCachedStorePort {
    fn complete_upload(&self, op: &PendingUpload) {
        self.vfs.cached_store.complete_upload(op);
    }

    fn requeue_upload(&self, op: PendingUpload) -> bool {
        self.vfs.cached_store.requeue_upload(op)
    }

    fn cancel_upload(&self, op: &PendingUpload) {
        self.vfs.cached_store.cancel_upload(op);
    }
}

#[derive(Clone)]
struct VfsUploadLocalPayloadPort {
    vfs: Arc<StudioVfs>,
}

impl UploadLocalPayloadPort for VfsUploadLocalPayloadPort {
    fn mark_uploading(&self, object_id: u64) {
        self.vfs.local_payload_backend.mark_uploading(object_id);
    }

    fn mark_upload_failed(&self, object_id: u64) {
        self.vfs.local_payload_backend.mark_upload_failed(object_id);
    }

    async fn prepare_local_priority_read(
        &self,
        object_id: u64,
        adaptive_penalty: Duration,
    ) -> VfsResult<()> {
        if self.vfs.upload_hot_get(object_id).is_some() {
            return Ok(());
        }
        if self.vfs.upload_stage_contains_async(object_id).await? {
            return Ok(());
        }
        match self
            .vfs
            .upload_stage_link_batch_async(vec![object_id])
            .await
        {
            Ok(()) if self.vfs.upload_stage_contains_async(object_id).await? => {
                self.vfs.debug_write_probe(
                    "H5",
                    concat!(file!(), ":", line!()),
                    "upload stage hardlink restored before local-priority reread fallback",
                    serde_json::json!({
                        "object_id": object_id,
                    }),
                );
                return Ok(());
            }
            Ok(()) => {}
            Err(error) => {
                self.vfs.debug_write_probe(
                    "H5",
                    concat!(file!(), ":", line!()),
                    "upload stage hardlink restore failed; falling back to bounded reread",
                    serde_json::json!({
                        "object_id": object_id,
                        "error": error.to_string(),
                    }),
                );
            }
        }
        let foreground_active = self
            .vfs
            .state
            .read
            .foreground_read_active(now_unix_millis(), self.vfs.upload_user_idle_grace);
        let defer_delay = upload_disk_reread_defer_delay(
            foreground_active,
            false,
            adaptive_penalty,
            self.vfs.upload_user_idle_grace,
        );
        if let Some(delay) = defer_delay {
            // #region debug log
            self.vfs.debug_write_probe(
                "H5",
                concat!(file!(), ":", line!()),
                "upload stage unavailable; skipping local-priority defer and using bounded reread",
                serde_json::json!({
                    "object_id": object_id,
                    "adaptive_penalty_ms": adaptive_penalty.as_millis().min(u128::from(u64::MAX)) as u64,
                    "would_defer_delay_ms": delay.as_millis().min(u128::from(u64::MAX)) as u64,
                    "foreground_reads_inflight": self.vfs.foreground_reads_inflight(),
                }),
            );
            // #endregion debug log
        }
        Ok(())
    }

    async fn read_for_upload(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        if let Some(hot_payload) = self.vfs.upload_hot_get(object_id) {
            self.vfs
                .state
                .upload
                .upload_local_payload_reads
                .fetch_add(1, Ordering::Relaxed);
            self.vfs
                .state
                .upload
                .upload_local_payload_read_bytes
                .fetch_add(hot_payload.len() as u64, Ordering::Relaxed);
            self.vfs
                .state
                .upload
                .upload_hot_payload_reads
                .fetch_add(1, Ordering::Relaxed);
            self.vfs
                .state
                .upload
                .upload_hot_payload_read_bytes
                .fetch_add(hot_payload.len() as u64, Ordering::Relaxed);
            self.vfs.debug_write_probe(
                "H5",
                concat!(file!(), ":", line!()),
                "upload read served from hot upload buffer",
                serde_json::json!({
                    "object_id": object_id,
                    "buffer_bytes": hot_payload.len(),
                }),
            );
            return Ok(Some(hot_payload));
        }
        let foreground_active = self
            .vfs
            .state
            .read
            .foreground_read_active(now_unix_millis(), self.vfs.upload_user_idle_grace);
        let foreground_cost = self
            .vfs
            .state
            .upload
            .upload_cache_io_foreground_cost
            .load(Ordering::Relaxed);
        let upload_io_cost = foreground_upload_reread_slot_cost(
            foreground_active,
            foreground_cost,
            self.vfs.upload_cache_io_slots_max,
        );
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
        let staged_payload = self.vfs.upload_stage_get_async(object_id).await?;
        if let Some(staged_payload) = staged_payload {
            let pacing_delay =
                foreground_upload_reread_pacing_delay(foreground_active, staged_payload.len());
            if !pacing_delay.is_zero() {
                self.vfs.debug_write_probe(
                    "H5",
                    concat!(file!(), ":", line!()),
                    "upload staged read paced under foreground reads",
                    serde_json::json!({
                        "object_id": object_id,
                        "staged_bytes": staged_payload.len(),
                        "pacing_delay_ms": pacing_delay.as_millis().min(u128::from(u64::MAX)) as u64,
                        "slot_cost": upload_io_cost,
                    }),
                );
                tokio::time::sleep(pacing_delay).await;
            }
            drop(upload_io_permit);
            self.vfs
                .state
                .upload
                .upload_local_payload_reads
                .fetch_add(1, Ordering::Relaxed);
            self.vfs
                .state
                .upload
                .upload_local_payload_read_bytes
                .fetch_add(staged_payload.len() as u64, Ordering::Relaxed);
            self.vfs
                .state
                .upload
                .upload_staged_payload_reads
                .fetch_add(1, Ordering::Relaxed);
            self.vfs
                .state
                .upload
                .upload_staged_payload_read_bytes
                .fetch_add(staged_payload.len() as u64, Ordering::Relaxed);
            self.vfs.debug_write_probe(
                "H5",
                concat!(file!(), ":", line!()),
                "upload read served from upload hardlink",
                serde_json::json!({
                    "object_id": object_id,
                    "buffer_bytes": staged_payload.len(),
                }),
            );
            return Ok(Some(staged_payload));
        }
        let read_started = Instant::now();
        let read_result = self
            .vfs
            .payload_store_get_for_upload_async(object_id)
            .await?;
        let read_ns = read_started.elapsed().as_nanos().clamp(0, u64::MAX as u128) as u64;
        let pacing_delay = read_result
            .as_ref()
            .map(|payload| foreground_upload_reread_pacing_delay(foreground_active, payload.len()))
            .unwrap_or(Duration::ZERO);
        if !pacing_delay.is_zero() {
            self.vfs.debug_write_probe(
                "H5",
                concat!(file!(), ":", line!()),
                "upload reread paced under foreground reads",
                serde_json::json!({
                    "object_id": object_id,
                    "reread_bytes": read_result.as_ref().map(|payload| payload.len()).unwrap_or(0),
                    "pacing_delay_ms": pacing_delay.as_millis().min(u128::from(u64::MAX)) as u64,
                    "slot_cost": upload_io_cost,
                }),
            );
            tokio::time::sleep(pacing_delay).await;
        }
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
                .upload_local_payload_reads
                .fetch_add(1, Ordering::Relaxed);
            self.vfs
                .state
                .upload
                .upload_local_payload_read_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
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
        self.vfs.payload_store_remove_async(object_id).await
    }

    async fn mark_uploaded(&self, object_id: u64) -> VfsResult<()> {
        self.vfs.payload_store_mark_uploaded_async(object_id).await
    }

    async fn remove_upload_hot(&self, object_id: u64) -> VfsResult<()> {
        self.vfs.upload_hot_remove(object_id);
        Ok(())
    }

    async fn remove_upload_stage(&self, object_id: u64) -> VfsResult<()> {
        self.vfs.upload_stage_remove_async(object_id).await
    }

    fn open_handle_count(&self, ino: Ino) -> usize {
        self.vfs.handle_table.open_count(ino) as usize
    }

    fn record_deferred_deleted_local_payload(&self, ino: Ino, object_id: u64) {
        self.vfs
            .record_deferred_deleted_local_payload(ino, object_id);
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

    async fn has_regular_file_overlay_object_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> VfsResult<bool> {
        let meta = self.vfs.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.has_regular_file_overlay_object_if_matches(ino, expected_epoch, object_id)
        })
        .await
    }

    async fn mark_regular_file_overlay_object_uploaded_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> VfsResult<bool> {
        let meta = self.vfs.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.mark_regular_file_overlay_object_uploaded_if_matches(
                ino,
                expected_epoch,
                object_id,
            )
        })
        .await
    }

    async fn reattach_uploaded_object_if_uncovered(
        &self,
        ino: Ino,
        content_epoch: u64,
        object_id: u64,
        offset: u64,
        payload_len: u64,
        block_size: u32,
    ) -> VfsResult<Option<u64>> {
        if payload_len == 0 {
            return Ok(None);
        }
        let Some(end_offset) = offset.checked_add(payload_len) else {
            return Ok(None);
        };
        let meta = self.vfs.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            let latest_attr = match engine.get_attr(ino) {
                Ok(attr) => attr,
                Err(studiofs_meta::MetaError::InodeNotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            if latest_attr.kind != FileType::RegularFile {
                return Ok(None);
            }
            let can_restore_stale_sparse_growth =
                latest_attr.size < end_offset && latest_attr.content_epoch < content_epoch;
            if latest_attr.size < end_offset && !can_restore_stale_sparse_growth {
                return Ok(None);
            }

            let spans = engine.lookup_regular_file_slices(ino, offset, payload_len)?;
            let chunk_slices = match uploaded_object_reattach_slices(
                &spans,
                object_id,
                offset,
                payload_len,
                block_size,
            ) {
                Some(chunk_slices) => chunk_slices,
                None if can_restore_stale_sparse_growth => {
                    let mut chunk_slices = BTreeMap::new();
                    append_uploaded_object_reattach_slices(
                        &mut chunk_slices,
                        object_id,
                        offset,
                        offset,
                        end_offset,
                        block_size,
                    )
                    .ok_or_else(|| {
                        studiofs_meta::MetaError::Storage(
                            "failed to rebuild stale sparse upload slices".to_string(),
                        )
                    })?;
                    chunk_slices.into_iter().collect()
                }
                None => return Ok(None),
            };

            let mut rebased_attr = latest_attr;
            if can_restore_stale_sparse_growth {
                rebased_attr.size = rebased_attr.size.max(end_offset);
            }
            let rebased_epoch = if content_epoch > rebased_attr.content_epoch {
                content_epoch
            } else {
                let Some(next_epoch) = rebased_attr.content_epoch.checked_add(1) else {
                    return Ok(None);
                };
                next_epoch
            };
            rebased_attr.content_epoch = rebased_epoch;
            rebased_attr.touch_mtime();

            let applied = engine.flush_regular_file_overlay_slices_with_attr(
                ino,
                &chunk_slices,
                Some(&rebased_attr),
            )?;
            Ok(applied.map(|attr| attr.content_epoch))
        })
        .await
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
        let uploads = fragment_keys.iter().zip(object_bytes).enumerate().map(
            |(fragment_index, (key, payload))| {
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
            },
        );
        let done = join_all(uploads).await;

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
        self.vfs
            .state
            .upload
            .upload_canceled_stale_before_put
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_canceled_stale_after_put(&self, bytes_uploaded: u64) {
        self.vfs
            .state
            .upload
            .upload_canceled_stale
            .fetch_add(1, Ordering::Relaxed);
        self.vfs
            .state
            .upload
            .upload_canceled_stale_after_put
            .fetch_add(1, Ordering::Relaxed);
        self.vfs
            .state
            .upload
            .upload_canceled_stale_after_put_bytes
            .fetch_add(bytes_uploaded, Ordering::Relaxed);
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

    async fn finalize_completed_upload_epoch(&self, ino: Ino, content_epoch: u64) {
        if self.vfs.shutdown_requested() || self.vfs.inode_has_pending_uploads(ino) {
            return;
        }
        self.vfs.queue_epoch_finalization(ino, content_epoch);
    }
}

#[derive(Clone)]
pub(crate) struct UploadLifecycleWorker {
    vfs: Arc<StudioVfs>,
    processor: UploadProcessor<
        VfsCachedStorePort,
        VfsUploadLocalPayloadPort,
        VfsUploadMetaPort,
        VfsRemoteUploadPort,
        VfsUploadShutdownPort,
        VfsUploadPolicyPort,
    >,
}

impl UploadLifecycleWorker {
    pub(crate) fn new(vfs: Arc<StudioVfs>) -> Self {
        let processor = UploadProcessor {
            coordinator: VfsCachedStorePort { vfs: vfs.clone() },
            local_payloads: VfsUploadLocalPayloadPort { vfs: vfs.clone() },
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
        debug!("background uploader started");
        let worker_count = self
            .vfs
            .upload_worker_count
            .max(1)
            .min(self.vfs.upload_scheduler.max_concurrency_hint());
        let permits = Arc::new(Semaphore::new(worker_count));
        let mut workers = JoinSet::new();
        debug!(
            upload_worker_count = worker_count,
            upload_max_concurrency_hint = self.vfs.upload_scheduler.max_concurrency_hint(),
            "upload worker pool started"
        );

        let mut ready = ReadyUploads::default();
        let mut delayed = DelayedUploads::default();
        let mut dispatched_since_yield = 0u32;
        let mut inbound_closed = false;
        loop {
            Self::reap_ready_workers(&mut workers);
            Self::promote_ready_uploads(&mut ready, &mut delayed);
            self.drop_deleted_queued_uploads(&mut ready, &mut delayed);

            while !ready.is_empty() {
                Self::reap_ready_workers(&mut workers);
                self.drop_deleted_queued_uploads(&mut ready, &mut delayed);
                let Some(permit) = permits.clone().try_acquire_owned().ok() else {
                    break;
                };
                let Some(op) = ready.pop_front() else {
                    drop(permit);
                    break;
                };
                if self.queued_upload_deleted_before_dispatch(op.ino) {
                    self.cancel_deleted_queued_upload(op);
                    drop(permit);
                    continue;
                }
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
                    Self::promote_ready_uploads(&mut ready, &mut delayed);
                    self.drop_deleted_queued_uploads(&mut ready, &mut delayed);
                }
            }

            if self.vfs.uploader_shutdown_requested() {
                break;
            }
            if inbound_closed && ready.is_empty() && delayed.is_empty() && workers.is_empty() {
                break;
            }

            let next_delayed_wait = if ready.is_empty() {
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
                        Some(op) => Self::enqueue_pending_upload(&mut ready, &mut delayed, op),
                        None => inbound_closed = true,
                    }
                }
                _ = self.vfs.state.upload.deleted_uploads_notify.notified() => {}
                _ = &mut sleep_until_ready => {}
            }
        }

        self.drain_workers(&mut workers).await;
    }

    fn enqueue_pending_upload(
        ready: &mut ReadyUploads,
        delayed: &mut DelayedUploads,
        op: PendingUpload,
    ) {
        if op.not_before_unix_ms > now_unix_millis() {
            delayed
                .entry(op.not_before_unix_ms)
                .or_default()
                .push_back(op);
        } else {
            ready.push_back(op);
        }
    }

    fn promote_ready_uploads(ready: &mut ReadyUploads, delayed: &mut DelayedUploads) {
        while let Some((&not_before_unix_ms, _)) = delayed.first_key_value() {
            if not_before_unix_ms > now_unix_millis() {
                break;
            }
            let Some((_, mut ready_ops)) = delayed.pop_first() else {
                break;
            };
            while let Some(op) = ready_ops.pop_front() {
                ready.push_back(op);
            }
        }
    }

    fn remove_deleted_uploads_from_queues(
        ready: &mut ReadyUploads,
        delayed: &mut DelayedUploads,
        deleted_inodes: &HashSet<Ino>,
    ) -> Vec<PendingUpload> {
        if deleted_inodes.is_empty() {
            return Vec::new();
        }

        let mut removed = ready.remove_inodes(deleted_inodes);
        let mut empty_delays = Vec::new();
        for (not_before_unix_ms, queue) in delayed.iter_mut() {
            let mut retained = VecDeque::with_capacity(queue.len());
            while let Some(op) = queue.pop_front() {
                if deleted_inodes.contains(&op.ino) {
                    removed.push(op);
                } else {
                    retained.push_back(op);
                }
            }
            *queue = retained;
            if queue.is_empty() {
                empty_delays.push(*not_before_unix_ms);
            }
        }
        for not_before_unix_ms in empty_delays {
            delayed.remove(&not_before_unix_ms);
        }
        removed
    }

    fn drop_deleted_queued_uploads(&self, ready: &mut ReadyUploads, delayed: &mut DelayedUploads) {
        let deleted_inodes = self.vfs.state.upload.deleted_inodes.lock().clone();
        let removed = Self::remove_deleted_uploads_from_queues(ready, delayed, &deleted_inodes);
        for op in removed {
            self.cancel_deleted_queued_upload(op);
        }
    }

    fn queued_upload_deleted_before_dispatch(&self, ino: Ino) -> bool {
        self.vfs.state.upload.deleted_inodes.lock().contains(&ino)
    }

    fn cancel_deleted_queued_upload(&self, op: PendingUpload) {
        self.vfs.cached_store.cancel_upload(&op);
        self.vfs
            .state
            .upload
            .pending_evictions
            .lock()
            .remove(&op.object_id);
        self.vfs
            .state
            .upload
            .upload_canceled_deleted
            .fetch_add(1, Ordering::Relaxed);
        self.vfs
            .debug_pending_inode_state(op.ino, "pending_upload_deleted_before_dispatch");
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
    use studiofs_data::DataStoreConfig;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig, UploadState};
    use studiofs_meta::{MetaEngine, MetaFormat, MetaInodes, MetaPayloadObjects, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    type FakePendingInodes = Arc<Mutex<HashSet<Ino>>>;
    type FakeProcessor = UploadProcessor<
        FakeCoordinator,
        FakeLocalPayloads,
        FakeMeta,
        FakeRemote,
        FakeShutdown,
        FakePolicy,
    >;

    #[test]
    fn post_put_visibility_delay_uses_bounded_exponential_backoff() {
        assert_eq!(post_put_visibility_delay(0), Duration::from_millis(25));
        assert_eq!(post_put_visibility_delay(1), Duration::from_millis(50));
        assert_eq!(post_put_visibility_delay(2), Duration::from_millis(100));
        assert_eq!(post_put_visibility_delay(6), Duration::from_millis(1_000));
        assert_eq!(post_put_visibility_delay(32), Duration::from_millis(1_000));
    }

    #[test]
    fn reattach_uploaded_object_marks_full_block_at_nonzero_object_offset_as_full() {
        let block_size = 4096;
        let object_id = 42;
        let spans = vec![ResolvedSliceSpan {
            chunk_id: ChunkId(0),
            range: SliceRange::new(block_size, block_size * 2).expect("hole range"),
            source: ResolvedSliceSource::Hole,
            slice_id: None,
            slice_order: None,
            payload: None,
        }];

        let reattached = uploaded_object_reattach_slices(
            &spans,
            object_id,
            0,
            u64::from(block_size * 2),
            block_size,
        )
        .expect("full block should reattach over a hole");
        let payload = reattached[0].1[0]
            .payload
            .as_ref()
            .expect("reattached slice should carry payload");

        assert_eq!(payload.object_id, object_id);
        assert_eq!(payload.offset, block_size);
        assert_eq!(payload.length, block_size);
        assert!(
            !payload.is_partial(),
            "logical full blocks remain full even when their object offset is non-zero"
        );
    }

    #[derive(Clone, Default)]
    struct FakeCoordinator {
        completed: Arc<Mutex<Vec<u64>>>,
        cancelled: Arc<Mutex<Vec<u64>>>,
        requeued: Arc<Mutex<Vec<PendingUpload>>>,
        pending_inodes: FakePendingInodes,
    }

    impl CachedStorePort for FakeCoordinator {
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
    struct FakeLocalPayloads {
        upload_reads: Arc<Mutex<HashMap<u64, Option<Bytes>>>>,
        removed: Arc<Mutex<Vec<u64>>>,
        upload_hot_removed: Arc<Mutex<Vec<u64>>>,
        upload_stage_removed: Arc<Mutex<Vec<u64>>>,
        uploaded: Arc<Mutex<Vec<u64>>>,
        failed: Arc<Mutex<Vec<u64>>>,
        deferred: Arc<Mutex<Vec<(Ino, u64)>>>,
        pending_evictions: Arc<Mutex<HashSet<u64>>>,
        open_handles: Arc<Mutex<HashMap<Ino, usize>>>,
    }

    impl UploadLocalPayloadPort for FakeLocalPayloads {
        fn mark_uploading(&self, _object_id: u64) {}

        fn mark_upload_failed(&self, object_id: u64) {
            self.failed.lock().push(object_id);
        }

        async fn prepare_local_priority_read(
            &self,
            _object_id: u64,
            _adaptive_penalty: Duration,
        ) -> VfsResult<()> {
            Ok(())
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

        async fn remove_upload_hot(&self, object_id: u64) -> VfsResult<()> {
            self.upload_hot_removed.lock().push(object_id);
            Ok(())
        }

        async fn remove_upload_stage(&self, object_id: u64) -> VfsResult<()> {
            self.upload_stage_removed.lock().push(object_id);
            Ok(())
        }

        fn open_handle_count(&self, ino: Ino) -> usize {
            self.open_handles.lock().get(&ino).copied().unwrap_or(0)
        }

        fn record_deferred_deleted_local_payload(&self, ino: Ino, object_id: u64) {
            self.deferred.lock().push((ino, object_id));
        }

        fn clear_pending_eviction(&self, object_id: u64) -> bool {
            self.pending_evictions.lock().remove(&object_id)
        }
    }

    type UploadedOverlayObjectRecord = (Ino, u64, u64);
    type ReattachedUploadRecord = (Ino, u64, u64, u64, u64, u32);

    #[derive(Clone)]
    struct FakeMeta {
        attrs: Arc<Mutex<HashMap<Ino, Option<InodeAttr>>>>,
        overlay_objects: Arc<Mutex<HashMap<Ino, HashSet<u64>>>>,
        pending_deletions: Arc<Mutex<Vec<u64>>>,
        uploaded_overlay_objects: Arc<Mutex<Vec<UploadedOverlayObjectRecord>>>,
        reattached_uploads: Arc<Mutex<Vec<ReattachedUploadRecord>>>,
        reattach_epoch: Arc<Mutex<Option<u64>>>,
        reattach_epoch_sequence: Arc<Mutex<Vec<Option<u64>>>>,
    }

    impl Default for FakeMeta {
        fn default() -> Self {
            Self {
                attrs: Arc::new(Mutex::new(HashMap::new())),
                overlay_objects: Arc::new(Mutex::new(HashMap::new())),
                pending_deletions: Arc::new(Mutex::new(Vec::new())),
                uploaded_overlay_objects: Arc::new(Mutex::new(Vec::new())),
                reattached_uploads: Arc::new(Mutex::new(Vec::new())),
                reattach_epoch: Arc::new(Mutex::new(None)),
                reattach_epoch_sequence: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl UploadMetaPort for FakeMeta {
        async fn get_attr(&self, ino: Ino) -> VfsResult<Option<InodeAttr>> {
            Ok(self.attrs.lock().get(&ino).cloned().flatten())
        }

        async fn has_regular_file_overlay_object_if_matches(
            &self,
            ino: Ino,
            expected_epoch: u64,
            object_id: u64,
        ) -> VfsResult<bool> {
            let epoch_matches = self
                .attrs
                .lock()
                .get(&ino)
                .and_then(|attr| attr.as_ref())
                .map(|attr| attr.content_epoch == expected_epoch)
                .unwrap_or(false);
            if !epoch_matches {
                return Ok(false);
            }
            Ok(self
                .overlay_objects
                .lock()
                .get(&ino)
                .is_some_and(|objects| objects.contains(&object_id)))
        }

        async fn mark_regular_file_overlay_object_uploaded_if_matches(
            &self,
            ino: Ino,
            expected_epoch: u64,
            object_id: u64,
        ) -> VfsResult<bool> {
            let matches = self
                .attrs
                .lock()
                .get(&ino)
                .and_then(|attr| attr.as_ref())
                .map(|attr| attr.content_epoch == expected_epoch)
                .unwrap_or(false)
                && self
                    .overlay_objects
                    .lock()
                    .get(&ino)
                    .is_some_and(|objects| objects.contains(&object_id));
            if matches {
                self.uploaded_overlay_objects
                    .lock()
                    .push((ino, expected_epoch, object_id));
            }
            Ok(matches)
        }

        async fn reattach_uploaded_object_if_uncovered(
            &self,
            ino: Ino,
            content_epoch: u64,
            object_id: u64,
            offset: u64,
            payload_len: u64,
            block_size: u32,
        ) -> VfsResult<Option<u64>> {
            self.reattached_uploads.lock().push((
                ino,
                content_epoch,
                object_id,
                offset,
                payload_len,
                block_size,
            ));
            let mut sequence = self.reattach_epoch_sequence.lock();
            if !sequence.is_empty() {
                return Ok(sequence.remove(0));
            }
            Ok(*self.reattach_epoch.lock())
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
        visibility_after_upload: Arc<Mutex<Option<RemoteVisibility>>>,
        upload_result: Arc<Mutex<Option<UploadPutOutcome>>>,
        remove_overlay_after_upload: Arc<Mutex<Option<(FakeMeta, Ino, u64)>>>,
        advance_epoch_after_upload: Arc<Mutex<Option<(FakeMeta, Ino, u64)>>>,
        mark_deleted_after_upload: Arc<Mutex<Option<(FakeShutdown, Ino)>>>,
        cancel_token_after_upload: Arc<AtomicBool>,
    }

    impl Default for FakeRemote {
        fn default() -> Self {
            Self {
                adaptive_penalty_ms: 0,
                visibility: Arc::new(Mutex::new(RemoteVisibility::NotVisible)),
                visibility_after_upload: Arc::new(Mutex::new(Some(RemoteVisibility::Visible))),
                upload_result: Arc::new(Mutex::new(Some(UploadPutOutcome::Uploaded))),
                remove_overlay_after_upload: Arc::new(Mutex::new(None)),
                advance_epoch_after_upload: Arc::new(Mutex::new(None)),
                mark_deleted_after_upload: Arc::new(Mutex::new(None)),
                cancel_token_after_upload: Arc::new(AtomicBool::new(false)),
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
            cancel_token: &CancellationToken,
        ) -> UploadPutOutcome {
            let outcome = self
                .upload_result
                .lock()
                .take()
                .unwrap_or(UploadPutOutcome::Uploaded);
            if matches!(outcome, UploadPutOutcome::Uploaded)
                && let Some(visibility) = self.visibility_after_upload.lock().take()
            {
                *self.visibility.lock() = visibility;
            }
            if matches!(outcome, UploadPutOutcome::Uploaded)
                && self
                    .cancel_token_after_upload
                    .swap(false, Ordering::Relaxed)
            {
                cancel_token.cancel();
            }
            if matches!(outcome, UploadPutOutcome::Uploaded)
                && let Some((meta, ino, object_id)) = self.remove_overlay_after_upload.lock().take()
                && let Some(objects) = meta.overlay_objects.lock().get_mut(&ino)
            {
                objects.remove(&object_id);
            }
            if matches!(outcome, UploadPutOutcome::Uploaded)
                && let Some((meta, ino, content_epoch)) =
                    self.advance_epoch_after_upload.lock().take()
                && let Some(Some(attr)) = meta.attrs.lock().get_mut(&ino)
            {
                attr.content_epoch = content_epoch;
            }
            if matches!(outcome, UploadPutOutcome::Uploaded)
                && let Some((shutdown, ino)) = self.mark_deleted_after_upload.lock().take()
            {
                shutdown.deleted_inodes.lock().insert(ino);
            }
            outcome
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
        canceled_stale_after_put: Arc<AtomicU64>,
        canceled_stale_after_put_bytes: Arc<AtomicU64>,
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
                canceled_stale_after_put: Arc::new(AtomicU64::new(0)),
                canceled_stale_after_put_bytes: Arc::new(AtomicU64::new(0)),
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

        fn record_canceled_stale_after_put(&self, bytes_uploaded: u64) {
            self.canceled_stale.fetch_add(1, Ordering::Relaxed);
            self.canceled_stale_after_put
                .fetch_add(1, Ordering::Relaxed);
            self.canceled_stale_after_put_bytes
                .fetch_add(bytes_uploaded, Ordering::Relaxed);
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
        finalized_upload_epochs: Arc<Mutex<Vec<(Ino, u64)>>>,
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

        async fn finalize_completed_upload_epoch(&self, ino: Ino, content_epoch: u64) {
            self.finalized_upload_epochs
                .lock()
                .push((ino, content_epoch));
        }
    }

    fn make_attr(ino: Ino, content_epoch: u64) -> InodeAttr {
        let mut attr = InodeAttr::new_file(ino, 1, 0, 0, 0o644);
        attr.content_epoch = content_epoch;
        attr
    }

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("upload-worker-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("upload-worker-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_unstarted_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            crate::VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        (rt, vfs, meta)
    }

    #[test]
    fn vfs_upload_meta_reattaches_newer_stale_upload_when_latest_range_is_hole() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "reattach-newer-stale-upload");
        let created = vfs
            .create(1, b"reattach-newer-stale-upload.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let mut ready_attr = created;
        ready_attr.size = 2_097_275;
        ready_attr.committed_size = ready_attr.size;
        ready_attr.content_epoch = 2;
        ready_attr.committed_content_epoch = 2;
        ready_attr.upload_state = UploadState::Ready;
        meta.set_attr(&ready_attr)
            .expect("seed published sparse attr");

        let port = VfsUploadMetaPort { vfs: vfs.clone() };
        let rebased_epoch = rt
            .block_on(port.reattach_uploaded_object_if_uncovered(
                ready_attr.ino,
                3,
                6,
                1_048_569,
                262,
                vfs.block_size,
            ))
            .expect("reattach uploaded payload");

        assert_eq!(
            rebased_epoch,
            Some(3),
            "a newer queued upload should be restored at its queued epoch when the latest graph still has a hole"
        );
        let attr = meta.get_attr(ready_attr.ino).expect("load rebased attr");
        assert_eq!(attr.content_epoch, 3);
        assert_eq!(attr.committed_content_epoch, 2);
        assert_eq!(attr.size, ready_attr.size);
        assert!(
            meta.has_regular_file_overlay_object_if_matches(ready_attr.ino, 3, 6)
                .expect("rebased overlay object present"),
            "rebased upload must become the current overlay object"
        );
    }

    #[test]
    fn vfs_upload_meta_reattaches_materialized_sparse_upload_over_uncovered_tail() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "reattach-materialized-sparse-upload");
        let created = vfs
            .create(1, b"reattach-materialized-sparse-upload.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let prefix_len = 233u32;
        let materialized_len = 1_048_831u64;
        let published_prefix_object = 4;
        let materialized_upload_object = 6;

        let mut published_attr = created;
        published_attr.size = u64::from(prefix_len);
        published_attr.committed_size = published_attr.size;
        published_attr.content_epoch = 1;
        published_attr.committed_content_epoch = 1;
        published_attr.upload_state = UploadState::Updating;
        meta.set_attr(&published_attr)
            .expect("seed prefix attr before publish");
        let prefix_slice = SliceRecord::new(
            SliceId(0),
            SliceOrder(0),
            SliceRange::new(0, prefix_len).expect("prefix range"),
            PayloadRef::new_partial(published_prefix_object, prefix_len),
        );
        meta.flush_regular_file_overlay_slices_with_attr(
            published_attr.ino,
            &[(ChunkId(0), vec![prefix_slice])],
            Some(&published_attr),
        )
        .expect("stage published prefix object");
        assert!(
            meta.mark_regular_file_overlay_object_uploaded_if_matches(
                published_attr.ino,
                1,
                published_prefix_object,
            )
            .expect("mark prefix uploaded")
        );
        assert!(
            meta.publish_regular_file_overlay_to_base_if_matches(published_attr.ino, 1)
                .expect("publish prefix to base")
        );

        let mut ready_attr = meta
            .get_attr(published_attr.ino)
            .expect("load published attr");
        ready_attr.size = 2_097_275;
        ready_attr.committed_size = ready_attr.size;
        ready_attr.content_epoch = 2;
        ready_attr.committed_content_epoch = 2;
        ready_attr.upload_state = UploadState::Ready;
        meta.set_attr(&ready_attr)
            .expect("seed sparse ready attr with published prefix");

        let port = VfsUploadMetaPort { vfs: vfs.clone() };
        let rebased_epoch = rt
            .block_on(port.reattach_uploaded_object_if_uncovered(
                ready_attr.ino,
                3,
                materialized_upload_object,
                0,
                materialized_len,
                vfs.block_size,
            ))
            .expect("reattach materialized sparse upload");

        assert_eq!(
            rebased_epoch,
            Some(3),
            "a materialized sparse stale upload should be restored over holes without replacing existing base data"
        );
        let spans = meta
            .lookup_regular_file_slices(ready_attr.ino, 0, materialized_len)
            .expect("load rebased sparse graph");
        assert!(
            spans.iter().any(|span| {
                span.source == ResolvedSliceSource::Base
                    && span.range == SliceRange::new(0, prefix_len).expect("prefix range")
                    && span
                        .payload
                        .as_ref()
                        .is_some_and(|payload| payload.object_id == published_prefix_object)
            }),
            "the already-published prefix must stay backed by the original base object"
        );

        let rebased_payloads = spans
            .iter()
            .filter_map(|span| {
                let payload = span.payload.as_ref()?;
                (payload.object_id == materialized_upload_object).then_some((span, payload))
            })
            .collect::<Vec<_>>();
        assert_eq!(rebased_payloads.len(), 1);
        let (span, payload) = rebased_payloads[0];
        assert_eq!(span.source, ResolvedSliceSource::Overlay);
        assert_eq!(
            span.range,
            SliceRange::new(prefix_len, u32::try_from(materialized_len).unwrap())
                .expect("tail range")
        );
        assert_eq!(payload.offset, prefix_len);
        assert_eq!(
            payload.length,
            u32::try_from(materialized_len - u64::from(prefix_len)).unwrap()
        );
    }

    #[test]
    fn vfs_upload_meta_reattaches_newer_stale_upload_when_same_object_is_still_overlay() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "reattach-retained-stale-upload");
        let created = vfs
            .create(1, b"reattach-retained-stale-upload.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let mut updating_attr = created;
        updating_attr.size = 1_048_831;
        updating_attr.committed_size = 233;
        updating_attr.content_epoch = 2;
        updating_attr.committed_content_epoch = 1;
        updating_attr.upload_state = UploadState::Updating;

        let mut slice = SliceRecord::new(
            SliceId(0),
            SliceOrder(0),
            SliceRange::new(1_048_569, 1_048_831).expect("boundary range"),
            PayloadRef::new_partial(6, 262),
        );
        slice.payload_state = PayloadRefState::LocalOnly;
        meta.flush_regular_file_overlay_slices_with_attr(
            updating_attr.ino,
            &[(ChunkId(0), vec![slice])],
            Some(&updating_attr),
        )
        .expect("seed retained local overlay");

        let port = VfsUploadMetaPort { vfs: vfs.clone() };
        let rebased_epoch = rt
            .block_on(port.reattach_uploaded_object_if_uncovered(
                updating_attr.ino,
                3,
                6,
                1_048_569,
                262,
                vfs.block_size,
            ))
            .expect("reattach uploaded payload");

        assert_eq!(
            rebased_epoch,
            Some(3),
            "a same-object LocalOnly span should not block post-PUT reattach"
        );
        let attr = meta.get_attr(updating_attr.ino).expect("load rebased attr");
        assert_eq!(attr.content_epoch, 3);
        assert_eq!(attr.committed_content_epoch, 1);
        assert!(
            meta.has_regular_file_overlay_object_if_matches(updating_attr.ino, 3, 6)
                .expect("rebased overlay object present"),
            "retained stale upload must become Uploaded in the current overlay epoch"
        );
    }

    #[test]
    fn vfs_upload_meta_reattaches_stale_sparse_growth_over_older_short_attr() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "reattach-stale-sparse-growth");
        let created = vfs
            .create(1, b"reattach-stale-sparse-growth.bin", 0, 0, 0o644)
            .expect("create sparse file");
        let mut short_attr = created;
        short_attr.size = 233;
        short_attr.committed_size = short_attr.size;
        short_attr.content_epoch = 1;
        short_attr.committed_content_epoch = 1;
        short_attr.upload_state = UploadState::Ready;
        meta.set_attr(&short_attr)
            .expect("seed older short published attr");

        let port = VfsUploadMetaPort { vfs: vfs.clone() };
        let object_id = 6;
        let tail_offset = 1_048_569;
        let tail_len = 262;
        let rebased_epoch = rt
            .block_on(port.reattach_uploaded_object_if_uncovered(
                short_attr.ino,
                2,
                object_id,
                tail_offset,
                tail_len,
                vfs.block_size,
            ))
            .expect("reattach stale sparse growth");

        assert_eq!(
            rebased_epoch,
            Some(2),
            "an uploaded sparse write from a newer epoch must restore EOF when an older attr won the race"
        );
        let attr = meta.get_attr(short_attr.ino).expect("load rebased attr");
        assert_eq!(attr.size, tail_offset + tail_len);
        assert_eq!(attr.content_epoch, 2);
        assert!(
            meta.has_regular_file_overlay_object_if_matches(short_attr.ino, 2, object_id)
                .expect("rebased sparse object present"),
            "stale sparse upload must be retained in the rebased overlay"
        );
    }

    fn processor_fixture() -> (
        FakeProcessor,
        FakeCoordinator,
        FakeLocalPayloads,
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
        let local_payloads = FakeLocalPayloads::default();
        let meta = FakeMeta::default();
        let remote = FakeRemote::default();
        let shutdown = FakeShutdown {
            pending_inodes: pending_inodes.clone(),
            ..FakeShutdown::default()
        };
        let policy = FakePolicy::default();
        let processor = UploadProcessor {
            coordinator: coordinator.clone(),
            local_payloads: local_payloads.clone(),
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
            local_payloads,
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
        let (mut processor, coordinator, local_payloads, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        let before_retry = now_unix_millis();
        processor.upload_requeue_base = Duration::from_millis(50);
        processor.upload_requeue_max = Duration::from_millis(50);
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));
        *remote.upload_result.lock() =
            Some(UploadPutOutcome::Failed(VfsError::Other("boom".into())));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::Requeued);
        assert_eq!(local_payloads.failed.lock().as_slice(), &[77]);
        assert_eq!(policy.sticky_errors.lock().len(), 1);
        assert_eq!(shutdown.requeues.load(Ordering::Relaxed), 1);
        let requeued = coordinator.requeued.lock();
        assert_eq!(requeued.len(), 1);
        assert_eq!(requeued[0].retry_count, 1);
        assert!(requeued[0].not_before_unix_ms >= before_retry.saturating_add(40));
        assert!(shutdown.released_tokens.lock().is_empty());
    }

    #[tokio::test]
    async fn process_upload_requeues_when_uploaded_fragments_are_not_visible() {
        let (processor, coordinator, local_payloads, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));
        *remote.visibility_after_upload.lock() = Some(RemoteVisibility::NotVisible);

        let outcome = processor.process_upload(op.clone()).await;

        assert_eq!(outcome, UploadOutcome::Requeued);
        assert!(
            meta.uploaded_overlay_objects.lock().is_empty(),
            "metadata must not reference a remote payload before all fragments are visible"
        );
        assert!(coordinator.completed.lock().is_empty());
        assert_eq!(coordinator.requeued.lock().len(), 1);
        assert_eq!(coordinator.requeued.lock()[0].object_id, op.object_id);
        assert_eq!(local_payloads.failed.lock().as_slice(), &[op.object_id]);
        assert!(local_payloads.uploaded.lock().is_empty());
        assert!(local_payloads.removed.lock().is_empty());
        assert_eq!(shutdown.requeues.load(Ordering::Relaxed), 1);
        assert_eq!(shutdown.canceled_stale_after_put.load(Ordering::Relaxed), 0);
        assert_eq!(policy.sticky_errors.lock().len(), 1);
        assert!(policy.finalized_upload_epochs.lock().is_empty());
    }

    #[tokio::test]
    async fn process_upload_cancels_deleted_inodes_before_reading() {
        let (processor, coordinator, local_payloads, _meta, _remote, shutdown, _policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        shutdown.deleted_inodes.lock().insert(op.ino);

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::CancelledDeleted);
        assert_eq!(coordinator.cancelled.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.removed.lock().as_slice(), &[77]);
        assert_eq!(shutdown.canceled_deleted.load(Ordering::Relaxed), 1);
        assert_eq!(shutdown.released_tokens.lock().as_slice(), &[11]);
    }

    #[tokio::test]
    async fn process_upload_cancels_deleted_inode_after_remote_put_before_uploaded_mark() {
        let (processor, coordinator, local_payloads, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        let ino = op.ino;
        let content_epoch = op.content_epoch;
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));
        *remote.mark_deleted_after_upload.lock() = Some((shutdown.clone(), op.ino));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::CancelledDeleted);
        assert_eq!(coordinator.cancelled.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.removed.lock().as_slice(), &[77]);
        assert!(meta.uploaded_overlay_objects.lock().is_empty());
        assert_eq!(
            *policy.finalized_upload_epochs.lock(),
            vec![(ino, content_epoch)]
        );
        assert_eq!(shutdown.canceled_deleted.load(Ordering::Relaxed), 1);
        assert_eq!(shutdown.released_tokens.lock().as_slice(), &[11]);
    }

    #[tokio::test]
    async fn process_upload_reconciles_supersede_cancel_after_remote_put() {
        let (processor, coordinator, local_payloads, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));
        remote
            .cancel_token_after_upload
            .store(true, Ordering::Relaxed);
        let upload_cancel_token = CancellationToken::new();

        let outcome = processor
            .process_single_upload(&op, 0, &upload_cancel_token)
            .await
            .expect("process upload");

        assert_eq!(outcome, SingleUploadOutcome::Completed);
        assert!(upload_cancel_token.is_cancelled());
        assert_eq!(coordinator.completed.lock().as_slice(), &[op.object_id]);
        assert_eq!(
            meta.uploaded_overlay_objects.lock().as_slice(),
            &[(op.ino, op.content_epoch, op.object_id)],
            "post-PUT supersede cancellation must still persist uploaded metadata"
        );
        assert!(meta.pending_deletions.lock().is_empty());
        assert_eq!(local_payloads.uploaded.lock().as_slice(), &[op.object_id]);
        assert!(local_payloads.removed.lock().is_empty());
        assert_eq!(shutdown.canceled_stale.load(Ordering::Relaxed), 0);
        assert_eq!(
            policy.finalized_upload_epochs.lock().as_slice(),
            &[(op.ino, op.content_epoch)]
        );
    }

    #[tokio::test]
    async fn deleted_upload_cleanup_finalizes_completed_upload_epoch_not_fresh_attr_epoch() {
        let (processor, coordinator, _local_payloads, meta, _remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        shutdown.deleted_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch + 1)));

        let outcome = processor.process_upload(op.clone()).await;

        assert_eq!(outcome, UploadOutcome::CancelledDeleted);
        assert_eq!(
            *policy.finalized_upload_epochs.lock(),
            vec![(op.ino, op.content_epoch)],
            "deleted-upload cleanup must finalize the completed upload epoch, not a newer attr epoch"
        );
    }

    #[tokio::test]
    async fn process_upload_cancels_stale_content_epoch() {
        let (processor, coordinator, local_payloads, meta, _remote, shutdown, _policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch + 1)));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::CancelledStale);
        assert_eq!(coordinator.cancelled.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.removed.lock().as_slice(), &[77]);
        assert_eq!(shutdown.canceled_stale.load(Ordering::Relaxed), 1);
        assert_eq!(shutdown.released_tokens.lock().as_slice(), &[11]);
    }

    #[tokio::test]
    async fn process_upload_records_durable_uploaded_progress_before_local_cleanup() {
        let (processor, coordinator, local_payloads, meta, _remote, _shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::Completed);
        assert_eq!(
            meta.uploaded_overlay_objects.lock().as_slice(),
            &[(11, 3, 77)],
            "successful upload should durably mark epoch/object progress before publish"
        );
        assert_eq!(local_payloads.uploaded.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.upload_hot_removed.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.upload_stage_removed.lock().as_slice(), &[77]);
        assert_eq!(policy.refreshed.lock().as_slice(), &[11]);
    }

    #[tokio::test]
    async fn process_upload_attempts_epoch_finalization_after_upload_completion() {
        let (processor, _coordinator, _local_payloads, meta, _remote, _shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));

        let outcome = processor.process_upload(op.clone()).await;

        assert_eq!(outcome, UploadOutcome::Completed);
        assert_eq!(
            *policy.finalized_upload_epochs.lock(),
            vec![(op.ino, op.content_epoch)],
            "completed uploads must immediately drive epoch finalization so crash-replayed data becomes peer-visible"
        );
    }

    #[test]
    fn vfs_policy_queues_finalization_only_after_last_pending_inode_upload() {
        let tmp = TempDir::new().expect("tempdir");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "finalize-last-pending");
        let ino = vfs
            .create(1, b"multi-block-upload.bin", 0, 0, 0o644)
            .expect("create test file")
            .ino;
        let content_epoch = 7;
        vfs.cached_store()
            .submit_upload(ino, 0, 7_101, content_epoch);
        vfs.cached_store()
            .submit_upload(ino, u64::from(vfs.block_size), 7_102, content_epoch);
        let policy = VfsUploadPolicyPort { vfs: vfs.clone() };
        let rt = Runtime::new().expect("test runtime");
        let pending = vfs.pending_uploads_snapshot();
        let first = pending
            .iter()
            .find(|op| op.object_id == 7_101)
            .expect("first pending upload")
            .clone();
        let second = pending
            .iter()
            .find(|op| op.object_id == 7_102)
            .expect("second pending upload")
            .clone();

        vfs.cached_store().complete_upload(&first);
        rt.block_on(policy.finalize_completed_upload_epoch(ino, content_epoch));
        assert!(
            vfs.state
                .upload
                .pending_epoch_finalizations
                .lock()
                .is_empty(),
            "finalization should wait until all pending uploads for the inode complete"
        );

        vfs.cached_store().complete_upload(&second);
        rt.block_on(policy.finalize_completed_upload_epoch(ino, content_epoch));
        assert_eq!(
            vfs.state
                .upload
                .pending_epoch_finalizations
                .lock()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![(ino, content_epoch)],
            "the final pending upload should enqueue epoch finalization"
        );
    }

    #[tokio::test]
    async fn process_upload_marks_fragmented_overlay_object_uploaded_before_local_cleanup() {
        let (processor, coordinator, local_payloads, meta, _remote, _shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::Completed);
        assert_eq!(
            meta.uploaded_overlay_objects.lock().as_slice(),
            &[(11, 3, 77)],
            "fragmented overlay upload should persist progress against the referenced object id"
        );
        assert!(
            meta.pending_deletions.lock().is_empty(),
            "still-referenced fragmented objects must remain live"
        );
        assert_eq!(coordinator.completed.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.uploaded.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.upload_hot_removed.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.upload_stage_removed.lock().as_slice(), &[77]);
        assert!(
            local_payloads.removed.lock().is_empty(),
            "current fragmented overlay object should stay in the local backend"
        );
        assert_eq!(policy.refreshed.lock().as_slice(), &[11]);
    }

    #[tokio::test]
    async fn process_upload_counts_stale_discovered_after_remote_put() {
        let (processor, coordinator, local_payloads, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = PendingUpload {
            retry_count: 1,
            ..make_pending_upload()
        };
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));
        local_payloads.upload_reads.lock().insert(
            op.object_id,
            Some(Bytes::from_static(b"remote stale payload")),
        );
        *remote.remove_overlay_after_upload.lock() = Some((meta.clone(), op.ino, op.object_id));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::Completed);
        assert_eq!(coordinator.completed.lock().as_slice(), &[77]);
        assert_eq!(meta.pending_deletions.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.removed.lock().as_slice(), &[77]);
        assert!(local_payloads.uploaded.lock().is_empty());
        assert!(policy.quarantines.lock().is_empty());
        assert_eq!(
            shutdown.canceled_stale.load(Ordering::Relaxed),
            1,
            "stale objects discovered only after PUT must be counted as stale remote traffic"
        );
        assert_eq!(shutdown.canceled_stale_after_put.load(Ordering::Relaxed), 1);
        assert_eq!(
            shutdown
                .canceled_stale_after_put_bytes
                .load(Ordering::Relaxed),
            "remote stale payload".len() as u64
        );
    }

    #[tokio::test]
    async fn process_upload_recovers_post_put_stale_after_transient_metadata_gap() {
        let (processor, coordinator, local_payloads, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));
        local_payloads
            .upload_reads
            .lock()
            .insert(op.object_id, Some(Bytes::from_static(b"transient")));
        *remote.remove_overlay_after_upload.lock() = Some((meta.clone(), op.ino, op.object_id));
        *meta.reattach_epoch_sequence.lock() = vec![None, Some(op.content_epoch + 1)];

        let outcome = processor.process_upload(op.clone()).await;

        assert_eq!(outcome, UploadOutcome::Completed);
        assert_eq!(coordinator.completed.lock().as_slice(), &[op.object_id]);
        assert!(meta.pending_deletions.lock().is_empty());
        assert!(local_payloads.removed.lock().is_empty());
        assert_eq!(local_payloads.uploaded.lock().as_slice(), &[op.object_id]);
        assert_eq!(
            policy.finalized_upload_epochs.lock().as_slice(),
            &[(op.ino, op.content_epoch + 1)]
        );
        assert_eq!(meta.reattached_uploads.lock().len(), 2);
        assert_eq!(shutdown.requeues.load(Ordering::Relaxed), 0);
        assert!(coordinator.requeued.lock().is_empty());
        assert_eq!(shutdown.canceled_stale.load(Ordering::Relaxed), 0);
        assert_eq!(shutdown.canceled_stale_after_put.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn process_upload_rebases_post_put_stale_object_when_latest_range_is_hole() {
        let (processor, coordinator, local_payloads, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = PendingUpload {
            offset: 1024,
            ..make_pending_upload()
        };
        let rebased_epoch = op.content_epoch + 1;
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));
        local_payloads.upload_reads.lock().insert(
            op.object_id,
            Some(Bytes::from_static(b"rebased stale payload")),
        );
        *meta.reattach_epoch.lock() = Some(rebased_epoch);
        *remote.remove_overlay_after_upload.lock() = Some((meta.clone(), op.ino, op.object_id));

        let outcome = processor.process_upload(op.clone()).await;

        assert_eq!(outcome, UploadOutcome::Completed);
        assert_eq!(coordinator.completed.lock().as_slice(), &[77]);
        assert_eq!(
            meta.reattached_uploads.lock().as_slice(),
            &[(
                op.ino,
                op.content_epoch,
                op.object_id,
                op.offset,
                "rebased stale payload".len() as u64,
                4096
            )],
            "post-PUT stale payloads should be offered for same/newer-epoch rebase before deletion"
        );
        assert!(
            meta.pending_deletions.lock().is_empty(),
            "rebased payload must not be queued for deletion"
        );
        assert_eq!(local_payloads.uploaded.lock().as_slice(), &[77]);
        assert!(
            local_payloads.removed.lock().is_empty(),
            "rebased payload must remain available locally"
        );
        assert_eq!(shutdown.canceled_stale_after_put.load(Ordering::Relaxed), 0);
        assert_eq!(
            *policy.finalized_upload_epochs.lock(),
            vec![(op.ino, rebased_epoch)]
        );
    }

    #[tokio::test]
    async fn process_upload_reconciles_post_put_stale_object_retained_by_new_epoch() {
        let (processor, coordinator, local_payloads, meta, remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        let retained_epoch = op.content_epoch + 1;
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));
        meta.overlay_objects
            .lock()
            .insert(op.ino, HashSet::from([op.object_id]));
        local_payloads
            .upload_reads
            .lock()
            .insert(op.object_id, Some(Bytes::from_static(b"retained payload")));
        *remote.advance_epoch_after_upload.lock() = Some((meta.clone(), op.ino, retained_epoch));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::Completed);
        assert_eq!(coordinator.completed.lock().as_slice(), &[77]);
        assert_eq!(
            meta.uploaded_overlay_objects.lock().as_slice(),
            &[(11, retained_epoch, 77)],
            "a successful PUT for a retained object must satisfy the current epoch"
        );
        assert!(
            meta.pending_deletions.lock().is_empty(),
            "retained current-epoch payloads must not be scheduled for deletion"
        );
        assert_eq!(local_payloads.uploaded.lock().as_slice(), &[77]);
        assert!(
            local_payloads.removed.lock().is_empty(),
            "retained current-epoch payloads must remain in the local backend"
        );
        assert_eq!(shutdown.canceled_stale_after_put.load(Ordering::Relaxed), 0);
        assert_eq!(
            *policy.finalized_upload_epochs.lock(),
            vec![(11, retained_epoch)]
        );
    }

    #[tokio::test]
    async fn process_upload_cancels_stale_fragmented_overlay_without_quarantine() {
        let (processor, coordinator, local_payloads, meta, _remote, shutdown, policy) =
            processor_fixture();
        let op = make_pending_upload();
        coordinator.pending_inodes.lock().insert(op.ino);
        meta.attrs
            .lock()
            .insert(op.ino, Some(make_attr(op.ino, op.content_epoch)));

        let outcome = processor.process_upload(op).await;

        assert_eq!(outcome, UploadOutcome::CancelledStale);
        assert_eq!(coordinator.cancelled.lock().as_slice(), &[77]);
        assert_eq!(local_payloads.removed.lock().as_slice(), &[77]);
        assert!(
            policy.quarantines.lock().is_empty(),
            "stale fragmented uploads should cancel cleanly instead of quarantining the inode"
        );
        assert_eq!(shutdown.canceled_stale.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn delayed_retry_ops_wait_until_due_before_dispatch() {
        let mut ready = ReadyUploads::default();
        let mut delayed = DelayedUploads::default();
        let mut op = make_pending_upload();
        op.not_before_unix_ms = now_unix_millis().saturating_add(20);

        UploadLifecycleWorker::enqueue_pending_upload(&mut ready, &mut delayed, op.clone());
        UploadLifecycleWorker::promote_ready_uploads(&mut ready, &mut delayed);

        assert!(ready.is_empty());
        assert_eq!(delayed.len(), 1);

        tokio::time::sleep(Duration::from_millis(25)).await;
        UploadLifecycleWorker::promote_ready_uploads(&mut ready, &mut delayed);

        let Some(ready_op) = ready.pop_front() else {
            panic!("expected delayed upload to become dispatchable");
        };
        assert_eq!(ready_op.object_id, op.object_id);
        assert!(delayed.is_empty());
    }

    #[test]
    fn worker_queues_drop_deleted_pending_uploads_before_dispatch() {
        let mut ready = ReadyUploads::default();
        let mut delayed = DelayedUploads::default();
        let mut ready_deleted = make_pending_upload();
        ready_deleted.object_id = 101;
        let mut delayed_deleted = make_pending_upload();
        delayed_deleted.object_id = 102;
        delayed_deleted.not_before_unix_ms = now_unix_millis().saturating_add(60_000);
        let mut retained = make_pending_upload();
        retained.ino = ready_deleted.ino + 1;
        retained.object_id = 201;

        UploadLifecycleWorker::enqueue_pending_upload(&mut ready, &mut delayed, ready_deleted);
        UploadLifecycleWorker::enqueue_pending_upload(&mut ready, &mut delayed, delayed_deleted);
        UploadLifecycleWorker::enqueue_pending_upload(&mut ready, &mut delayed, retained.clone());

        let removed = UploadLifecycleWorker::remove_deleted_uploads_from_queues(
            &mut ready,
            &mut delayed,
            &HashSet::from([make_pending_upload().ino]),
        );

        let mut removed_ids: Vec<_> = removed.into_iter().map(|op| op.object_id).collect();
        removed_ids.sort_unstable();
        assert_eq!(
            removed_ids,
            vec![101, 102],
            "deleted inode uploads must leave ready and delayed queues before dispatch"
        );

        let Some(retained_op) = ready.pop_front() else {
            panic!("non-deleted inode upload should remain ready");
        };
        assert_eq!(retained_op.object_id, retained.object_id);
        assert!(ready.is_empty());
        assert!(delayed.is_empty());
    }

    #[test]
    fn upload_reread_idle_wait_window_requires_penalty_and_foreground_pressure() {
        assert_eq!(
            upload_reread_idle_wait_window(
                false,
                Duration::from_millis(200),
                Duration::from_millis(50),
            ),
            Duration::ZERO
        );
        assert_eq!(
            upload_reread_idle_wait_window(true, Duration::ZERO, Duration::from_millis(50)),
            Duration::ZERO
        );
    }

    #[test]
    fn upload_reread_idle_wait_window_uses_idle_grace_as_floor() {
        assert_eq!(
            upload_reread_idle_wait_window(
                true,
                Duration::from_millis(20),
                Duration::from_millis(50),
            ),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn upload_reread_idle_wait_window_uses_penalty_when_larger() {
        assert_eq!(
            upload_reread_idle_wait_window(
                true,
                Duration::from_millis(240),
                Duration::from_millis(50),
            ),
            Duration::from_millis(240)
        );
    }

    #[test]
    fn upload_disk_reread_defer_delay_skips_hot_payloads() {
        assert_eq!(
            upload_disk_reread_defer_delay(
                true,
                true,
                Duration::from_millis(240),
                Duration::from_millis(500),
            ),
            None
        );
    }

    #[test]
    fn upload_disk_reread_defer_delay_uses_idle_window_for_disk_miss() {
        assert_eq!(
            upload_disk_reread_defer_delay(
                true,
                false,
                Duration::from_millis(240),
                Duration::from_millis(500),
            ),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn foreground_upload_reread_slot_cost_uses_adaptive_foreground_cost() {
        assert_eq!(foreground_upload_reread_slot_cost(false, 8, 32), 1);
        assert_eq!(foreground_upload_reread_slot_cost(true, 3, 8), 3);
        assert_eq!(
            foreground_upload_reread_slot_cost(true, 64, 8),
            8,
            "adaptive foreground cost must stay bounded by available upload I/O slots"
        );
        assert_eq!(foreground_upload_reread_slot_cost(true, 0, 8), 1);
    }

    #[test]
    fn foreground_upload_reread_pacing_delay_tracks_payload_size() {
        assert_eq!(
            foreground_upload_reread_pacing_delay(false, 4096),
            Duration::ZERO
        );
        assert_eq!(
            foreground_upload_reread_pacing_delay(true, 0),
            Duration::ZERO
        );
        assert_eq!(
            foreground_upload_reread_pacing_delay(true, 1024usize * 1024 * 1024),
            Duration::from_secs(1)
        );
        assert_eq!(
            foreground_upload_reread_pacing_delay(true, 4usize * 1024 * 1024),
            Duration::from_nanos(3_906_250)
        );
    }

    #[test]
    fn ready_uploads_rotate_across_inodes() {
        let mut ready = ReadyUploads::default();
        for (ino, object_id) in [(10, 101), (10, 102), (20, 201), (20, 202), (30, 301)] {
            ready.push_back(PendingUpload {
                ino,
                offset: 0,
                object_id,
                content_epoch: 1,
                created_unix_ms: object_id,
                not_before_unix_ms: 0,
                retry_count: 0,
            });
        }

        let mut order = Vec::new();
        while let Some(op) = ready.pop_front() {
            order.push(op.object_id);
        }

        assert_eq!(
            order,
            vec![101, 201, 301, 102, 202],
            "ready queue should rotate across inodes instead of letting one inode monopolize dispatch"
        );
    }
}

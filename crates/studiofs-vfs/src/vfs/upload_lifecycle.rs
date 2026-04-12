use super::{
    FileSessionOperation, Ino, StudioVfs, VfsError, VfsIngressStats, VfsResult,
    block_index_from_offset, debug, info, warn,
};
use bytes::Bytes;
use futures::future::join_all;
use opendal::Buffer;
use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use studiofs_cache::CancelUploadMode;
use studiofs_cache::upload_queue::PendingUpload;
use studiofs_data::object::{
    CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE, create_object_buffer, format_object_fragment_keys,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::VfsMeta;
use crate::runtime::{TimeoutOrCancel, run_meta, run_with_timeout_or_cancel};
use crate::utils::now_unix_millis;

impl StudioVfs {
    pub(super) fn track_background_task(&self, handle: JoinHandle<()>) {
        self.state.shutdown.background_tasks.lock().push(handle);
    }

    pub(super) fn set_uploader_task(&self, handle: JoinHandle<()>) {
        *self.state.shutdown.uploader_task.lock() = Some(handle);
    }

    pub(super) fn shutdown_requested(&self) -> bool {
        self.state
            .shutdown
            .shutdown_requested
            .load(Ordering::Acquire)
    }

    pub(super) fn uploader_shutdown_requested(&self) -> bool {
        self.state
            .shutdown
            .uploader_shutdown_requested
            .load(Ordering::Acquire)
    }

    pub(super) fn uploader_cancel_token_for_inode(&self, ino: Ino) -> CancellationToken {
        if self.state.upload.deleted_inodes.lock().contains(&ino) {
            let token = CancellationToken::new();
            token.cancel();
            return token;
        }

        let mut tokens = self.state.upload.upload_cancel_tokens.lock();
        tokens
            .entry(ino)
            .or_insert_with(|| self.state.shutdown.uploader_shutdown_token.child_token())
            .clone()
    }

    pub(super) fn mark_inode_deleted_for_uploads(&self, ino: Ino) {
        // Soft upload cancellation marker: stops background uploads for this inode.
        // Data for still-open unlinked files must remain locally readable until
        // the last handle is released (POSIX open-unlink semantics).
        self.state.upload.deleted_inodes.lock().insert(ino);
        if let Some(token) = self
            .state
            .upload
            .upload_cancel_tokens
            .lock()
            .get(&ino)
            .cloned()
        {
            token.cancel();
        }
    }

    pub(super) fn record_deferred_deleted_local_block(&self, ino: Ino, object_id: u64) {
        self.state
            .upload
            .deferred_deleted_local_blocks
            .lock()
            .entry(ino)
            .or_default()
            .insert(object_id);
    }

    pub(super) fn take_deferred_deleted_local_blocks(&self, ino: Ino) -> Vec<u64> {
        self.state
            .upload
            .deferred_deleted_local_blocks
            .lock()
            .remove(&ino)
            .map(|ids| ids.into_iter().collect())
            .unwrap_or_default()
    }

    pub(super) async fn cancel_upload_for_deleted_inode(
        &self,
        op: &PendingUpload,
    ) -> VfsResult<()> {
        let has_open_handles = self.handle_table.open_count(op.ino) > 0;
        let mode = if has_open_handles {
            CancelUploadMode::KeepBlock
        } else {
            CancelUploadMode::RemoveBlock
        };
        self.upload_coordinator.cancel_upload(op, mode);
        if has_open_handles {
            self.record_deferred_deleted_local_block(op.ino, op.object_id);
        } else {
            self.block_store_remove_async(op.object_id).await?;
        }
        self.state
            .upload
            .pending_evictions
            .lock()
            .remove(&op.object_id);
        Ok(())
    }

    pub(super) async fn cancel_upload_if_inode_deleted(
        &self,
        op: &PendingUpload,
    ) -> VfsResult<bool> {
        if !self.state.upload.deleted_inodes.lock().contains(&op.ino) {
            return Ok(false);
        }
        self.state
            .upload
            .upload_canceled_deleted
            .fetch_add(1, Ordering::Relaxed);
        self.cancel_upload_for_deleted_inode(op).await?;
        self.maybe_clear_deleted_inode_marker(op.ino);
        Ok(true)
    }

    pub(super) fn maybe_clear_deleted_inode_marker(&self, ino: Ino) {
        if !self.state.upload.deleted_inodes.lock().contains(&ino) {
            return;
        }
        if self.handle_table.open_count(ino) > 0 {
            return;
        }
        if self.upload_coordinator.has_pending_uploads_for_ino(ino) {
            return;
        }
        // Only clear after inode metadata is actually gone. This keeps the
        // marker stable for open-unlink inodes until final orphan cleanup.
        if self.sync_meta().get_attr(ino).is_ok() {
            return;
        }
        self.state.upload.deleted_inodes.lock().remove(&ino);
    }

    pub(super) fn maybe_release_upload_cancel_token(&self, ino: Ino) {
        let mut tokens = self.state.upload.upload_cancel_tokens.lock();
        if self.upload_coordinator.has_pending_uploads_for_ino(ino) {
            return;
        }
        tokens.remove(&ino);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_requested()
            || self.uploader_shutdown_requested()
            || self
                .state
                .shutdown
                .shutdown_completed
                .load(Ordering::Acquire)
    }

    fn request_uploader_shutdown(&self) {
        if !self
            .state
            .shutdown
            .uploader_shutdown_requested
            .swap(true, Ordering::AcqRel)
        {
            self.state.shutdown.uploader_shutdown_token.cancel();
        }
    }

    async fn join_background_tasks(&self, timeout: Duration) {
        let handles = {
            let mut handles = self.state.shutdown.background_tasks.lock();
            std::mem::take(&mut *handles)
        };
        for mut handle in handles {
            if !handle.is_finished() {
                match tokio::time::timeout(timeout, &mut handle).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) if e.is_cancelled() => {}
                    Ok(Err(e)) => {
                        warn!(error = %e, "background task join failed");
                    }
                    Err(_) => {
                        handle.abort();
                        let _ = handle.await;
                        warn!(
                            timeout_ms = timeout.as_millis() as u64,
                            "background task did not stop in time, aborted"
                        );
                    }
                }
            } else if let Err(e) = handle.await
                && !e.is_cancelled()
            {
                warn!(error = %e, "background task join failed");
            }
        }
    }

    async fn join_uploader_task(&self, timeout: Duration) {
        let handle = self.state.shutdown.uploader_task.lock().take();
        let Some(mut handle) = handle else {
            return;
        };
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.is_cancelled() => {}
            Ok(Err(e)) => {
                warn!(error = %e, "uploader task join failed");
            }
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "uploader task did not stop in time, aborted"
                );
            }
        }
    }

    pub(super) fn pending_age_stats_ms(&self) -> (u64, u64) {
        let now_ms = now_unix_millis();
        let mut ages: Vec<u64> = self
            .upload_coordinator
            .snapshot_pending_uploads()
            .into_iter()
            .map(|op| now_ms.saturating_sub(op.created_unix_ms))
            .collect();
        if ages.is_empty() {
            return (0, 0);
        }
        ages.sort_unstable();
        // Safe percentile calculation: use checked arithmetic to avoid overflow
        let p95_idx = ((ages.len() - 1).saturating_mul(95)) / 100;
        let p95 = ages[p95_idx];
        let max = *ages.last().unwrap_or(&0);
        (p95, max)
    }

    pub(super) fn upload_pipeline_is_idle(
        &self,
        pending_uploads: usize,
        ingress: &VfsIngressStats,
    ) -> bool {
        crate::upload::upload_log_is_idle(pending_uploads, self.upload_scheduler.in_flight())
            && ingress.dirty_mem_blocks == 0
            && ingress.dirty_mem_bytes == 0
            && ingress.spill_records == 0
            && ingress.spill_bytes == 0
            && ingress.checkpoint_pending_bytes == 0
            && ingress.checkpoint_pending_mutations == 0
    }

    pub(super) fn upload_pipeline_summary_is_active(
        pending_uploads: usize,
        pending_metadata_ops: u32,
    ) -> bool {
        pending_uploads != 0 || (pending_metadata_ops != 0 && pending_metadata_ops >= 10)
    }

    pub(super) fn format_upload_pipeline_pending_uploads(pending_upload_bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;
        const GB: u64 = 1024 * MB;

        if pending_upload_bytes == 0 {
            "0".to_string()
        } else if pending_upload_bytes >= GB {
            format!("{:.1}GB", pending_upload_bytes as f64 / GB as f64)
        } else if pending_upload_bytes >= MB {
            format!("{}MB", pending_upload_bytes / MB)
        } else if pending_upload_bytes >= KB {
            format!("{}KB", pending_upload_bytes / KB)
        } else {
            format!("{pending_upload_bytes}B")
        }
    }

    pub(super) fn upload_requeue_delay(&self, op: &PendingUpload) -> Duration {
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

    pub(super) async fn process_upload(&self, op: PendingUpload) -> VfsResult<()> {
        struct UploadTokenCleanup<'a> {
            vfs: &'a StudioVfs,
            ino: Ino,
        }
        impl Drop for UploadTokenCleanup<'_> {
            fn drop(&mut self) {
                self.vfs.maybe_release_upload_cancel_token(self.ino);
            }
        }

        let _token_cleanup = UploadTokenCleanup {
            vfs: self,
            ino: op.ino,
        };
        let block_index = block_index_from_offset(op.offset, self.block_size);
        self.state.file_session.note_background_transition(
            op.ino,
            FileSessionOperation::Upload,
            "start",
        );
        let upload_cancel_token = self.uploader_cancel_token_for_inode(op.ino);
        debug!(
            ino = op.ino,
            object_id = op.object_id,
            offset = op.offset,
            content_epoch = op.content_epoch,
            retry_count = op.retry_count,
            "process_upload start"
        );
        if self.uploader_shutdown_requested() {
            return Ok(());
        }
        let mut waited_upload_delay_ms = 0u64;
        if self.upload_delay > Duration::from_millis(0) {
            let age_ms = now_unix_millis().saturating_sub(op.created_unix_ms);
            let delay_ms = self.upload_delay.as_millis() as u64;
            if age_ms < delay_ms {
                waited_upload_delay_ms = delay_ms - age_ms;
                tokio::select! {
                    _ = upload_cancel_token.cancelled() => {
                        if self.cancel_upload_if_inode_deleted(&op).await? {
                            debug!(ino = op.ino, object_id = op.object_id, "process_upload cancelled during upload delay");
                        }
                        return Ok(());
                    }
                    _ = tokio::time::sleep(Duration::from_millis(waited_upload_delay_ms)) => {}
                }
            }
        }
        if self.uploader_shutdown_requested() {
            return Ok(());
        }

        // Fast-path first: if inode is already deleted, avoid any penalty
        // computation/sleep and cancel upload immediately.
        if self.cancel_upload_if_inode_deleted(&op).await? {
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                "process_upload canceled: inode deleted (short-circuit)"
            );
            return Ok(());
        }

        // Apply adaptive cool-down before the worker enters the local L2 read
        // phase so background uploads create real quiet windows for foreground
        // reads instead of a back-to-back disk conveyor.
        let adaptive_penalty_ms = self.upload_scheduler.adaptive_penalty_ms();
        let effective_adaptive_penalty_ms =
            adaptive_penalty_ms.saturating_sub(waited_upload_delay_ms);
        if effective_adaptive_penalty_ms > 0 {
            tokio::select! {
                _ = self.state.shutdown.uploader_shutdown_token.cancelled() => return Ok(()),
                _ = upload_cancel_token.cancelled() => {
                    if self.cancel_upload_if_inode_deleted(&op).await? {
                        debug!(
                            ino = op.ino,
                            object_id = op.object_id,
                            penalty_ms = effective_adaptive_penalty_ms,
                            raw_penalty_ms = adaptive_penalty_ms,
                            upload_delay_waited_ms = waited_upload_delay_ms,
                            "process_upload cancelled during adaptive pre-read cooldown"
                        );
                    } else {
                        debug!(
                            ino = op.ino,
                            object_id = op.object_id,
                            penalty_ms = effective_adaptive_penalty_ms,
                            raw_penalty_ms = adaptive_penalty_ms,
                            upload_delay_waited_ms = waited_upload_delay_ms,
                            "process_upload cancelled during adaptive pre-read cooldown (shutdown/token cancel)"
                        );
                    }
                    return Ok(());
                }
                _ = tokio::time::sleep(Duration::from_millis(effective_adaptive_penalty_ms)) => {}
            }
        }

        // Also check metadata: after a remount, `deleted_inodes` is empty but
        // the inode may have been deleted in a prior session.  If the inode no
        // longer exists in the metadata store, the upload is stale.
        let meta = self.meta.clone();
        let attr = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_attr(op.ino)
        })
        .await
        .ok();
        if attr.is_none() {
            self.state
                .upload
                .upload_canceled_deleted
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                "upload cancelled: inode deleted or missing"
            );
            self.cancel_upload_for_deleted_inode(&op).await?;
            self.maybe_clear_deleted_inode_marker(op.ino);
            debug!(
                ino = op.ino,
                object_id = op.object_id,
                "process_upload canceled: inode deleted or missing"
            );
            return Ok(());
        }

        // Coalescing: if this file block has already been replaced in metadata
        // with a newer object_id, skip uploading this stale one.
        if let Some(attr) = attr.as_ref()
            && attr.content_epoch != op.content_epoch
        {
            self.state
                .upload
                .upload_canceled_stale
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                block_index,
                queued_epoch = op.content_epoch,
                current_epoch = attr.content_epoch,
                "upload cancelled: stale content epoch"
            );
            self.upload_coordinator
                .cancel_upload(&op, CancelUploadMode::KeepBlock);
            self.block_store_remove_async(op.object_id).await?;
            self.state
                .upload
                .pending_evictions
                .lock()
                .remove(&op.object_id);
            debug!(
                ino = op.ino,
                object_id = op.object_id,
                block_index,
                queued_epoch = op.content_epoch,
                current_epoch = attr.content_epoch,
                "process_upload canceled: stale content epoch"
            );
            return Ok(());
        }
        if let Ok(Some(current)) = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_file_block(op.ino, block_index)
        })
        .await
            && current.object_id != op.object_id
        {
            self.state
                .upload
                .upload_canceled_stale
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                current_object_id = current.object_id,
                ino = op.ino,
                block_index,
                "upload cancelled: stale block superseded by newer object"
            );
            self.upload_coordinator
                .cancel_upload(&op, CancelUploadMode::KeepBlock);
            self.block_store_remove_async(op.object_id).await?;
            self.state
                .upload
                .pending_evictions
                .lock()
                .remove(&op.object_id);
            debug!(
                ino = op.ino,
                object_id = op.object_id,
                block_index,
                current_object_id = current.object_id,
                "process_upload canceled: stale superseded block"
            );
            return Ok(());
        }

        let t0 = Instant::now();
        self.local_block_backend.mark_uploading(op.object_id);
        let fragment_keys = format_object_fragment_keys(&self.data_prefix, op.object_id);
        let mut remote_object_visible = false;
        if op.retry_count > 0 {
            let checks = fragment_keys.iter().map(|key| {
                let cancel_token = upload_cancel_token.clone();
                async move {
                    let result = run_with_timeout_or_cancel(
                        &cancel_token,
                        Duration::from_secs(5),
                        self.fetch_scheduler.data_store().exists(key),
                    )
                    .await;
                    (key, result)
                }
            });
            let results = futures::future::join_all(checks).await;
            let mut all_visible = true;
            for (key, result) in results {
                match result {
                    TimeoutOrCancel::Completed(Ok(true)) => {}
                    TimeoutOrCancel::Completed(Ok(false)) => {
                        all_visible = false;
                        break;
                    }
                    TimeoutOrCancel::Completed(Err(err)) => {
                        all_visible = false;
                        debug!(
                            object_id = op.object_id,
                            key,
                            error = %err,
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
                    // Shutdown cancellation: keep DIRTY upload intent for
                    // next startup replay by exiting before completion.
                    TimeoutOrCancel::Cancelled => {
                        self.cancel_upload_if_inode_deleted(&op).await?;
                        return Ok(());
                    }
                }
            }
            if all_visible {
                remote_object_visible = true;
                debug!(
                    object_id = op.object_id,
                    ino = op.ino,
                    block_index,
                    retry_count = op.retry_count,
                    "upload reconcile: remote object already visible, skipping re-upload"
                );
            }
        }

        // Read raw block payload from the persisted local backend when we
        // really need to upload. Queue correctness is anchored on this reread
        // path rather than transient in-memory staging.
        // Note: we intentionally do not probe L1/staged payloads first here.
        // A single persisted-source path keeps restart replay and live uploads
        // behaviorally identical, trading some extra local I/O for simpler
        // correctness reasoning.
        let raw_payload: Option<Bytes> = if remote_object_visible {
            None
        } else {
            // Hold local upload I/O slots strictly around the backend reread
            // phase. This path must continue making progress during foreground
            // activity because startup replay reconstructs pending uploads from
            // metadata + persisted local blocks.
            let upload_io_cost = if self
                .state
                .read
                .foreground_read_active(now_unix_millis(), self.upload_user_idle_grace)
            {
                self.state
                    .upload
                    .upload_cache_io_foreground_cost
                    .load(Ordering::Relaxed)
                    .max(1)
            } else {
                1
            };
            let wait_started = Instant::now();
            let upload_io_permit = self
                .upload_cache_io_slots
                .clone()
                .acquire_many_owned(upload_io_cost)
                .await
                .map_err(|e| VfsError::QosFailure {
                    stage: Cow::Borrowed("upload_cache_io_slots"),
                    object_id: op.object_id,
                    message: e.to_string(),
                })?;
            let wait_ns = wait_started.elapsed().as_nanos().clamp(0, u64::MAX as u128) as u64;
            let read_started = Instant::now();
            let read_result = self.block_store_get_for_upload_async(op.object_id).await?;
            let read_ns = read_started.elapsed().as_nanos().clamp(0, u64::MAX as u128) as u64;
            drop(upload_io_permit);
            self.state
                .upload
                .upload_fallback_wait_ns
                .fetch_add(wait_ns, Ordering::Relaxed);
            self.state
                .upload
                .upload_fallback_wait_max_ns
                .fetch_max(wait_ns, Ordering::Relaxed);
            self.state
                .upload
                .upload_fallback_read_ns
                .fetch_add(read_ns, Ordering::Relaxed);
            self.state
                .upload
                .upload_fallback_read_max_ns
                .fetch_max(read_ns, Ordering::Relaxed);
            self.state
                .upload
                .upload_fallback_rereads
                .fetch_add(1, Ordering::Relaxed);
            if let Some(payload) = &read_result {
                self.state
                    .upload
                    .upload_fallback_reread_bytes
                    .fetch_add(payload.len() as u64, Ordering::Relaxed);
            } else {
                self.state
                    .upload
                    .upload_fallback_reread_cache_misses
                    .fetch_add(1, Ordering::Relaxed);
            }
            read_result
        };

        let raw_payload = match raw_payload {
            Some(data) => data,
            None if remote_object_visible => Bytes::new(),
            None => {
                // Block data is missing from the persisted local backend. If
                // remote already has this object, we can still continue with
                // commit reconcile.
                if op.retry_count > 0 {
                    let checks = fragment_keys.iter().map(|key| {
                        let cancel_token = upload_cancel_token.clone();
                        async move {
                            let result = run_with_timeout_or_cancel(
                                &cancel_token,
                                Duration::from_secs(5),
                                self.fetch_scheduler.data_store().exists(key),
                            )
                            .await;
                            (key, result)
                        }
                    });
                    let results = futures::future::join_all(checks).await;
                    let mut all_visible = true;
                    for (_key, result) in results {
                        match result {
                            TimeoutOrCancel::Completed(Ok(true)) => {}
                            TimeoutOrCancel::Completed(Ok(false))
                            | TimeoutOrCancel::Completed(Err(_))
                            | TimeoutOrCancel::TimedOut => {
                                all_visible = false;
                                break;
                            }
                            // Shutdown cancellation: exit quietly so worker
                            // loop does not requeue/log as transient failure.
                            TimeoutOrCancel::Cancelled => {
                                self.cancel_upload_if_inode_deleted(&op).await?;
                                return Ok(());
                            }
                        }
                    }
                    if all_visible {
                        debug!(
                            object_id = op.object_id,
                            ino = op.ino,
                            block_index,
                            retry_count = op.retry_count,
                            "upload reconcile: local backend missing block but remote object exists"
                        );
                        remote_object_visible = true;
                        Bytes::new()
                    } else {
                        // If the inode was deleted between our checks
                        // (race), cancel cleanly. Otherwise this remains a
                        // genuine error and will be retried.
                        if self.state.upload.deleted_inodes.lock().contains(&op.ino) {
                            self.upload_coordinator
                                .cancel_upload(&op, CancelUploadMode::KeepBlock);
                            self.block_store_remove_async(op.object_id).await?;
                            self.state
                                .upload
                                .pending_evictions
                                .lock()
                                .remove(&op.object_id);
                            return Ok(());
                        }
                        return Err(VfsError::Other(format!(
                            "object {} missing from local block backend",
                            op.object_id
                        )));
                    }
                } else {
                    // First attempt without a local persisted block is
                    // unexpected; retry through normal error path.
                    if self.state.upload.deleted_inodes.lock().contains(&op.ino) {
                        self.upload_coordinator
                            .cancel_upload(&op, CancelUploadMode::KeepBlock);
                        self.block_store_remove_async(op.object_id).await?;
                        self.state
                            .upload
                            .pending_evictions
                            .lock()
                            .remove(&op.object_id);
                        return Ok(());
                    }
                    return Err(VfsError::Other(format!(
                        "object {} missing from local block backend",
                        op.object_id
                    )));
                }
            }
        };

        let mut transit_credit = if remote_object_visible {
            None
        } else {
            Some(
                self.transit_memory_budget
                    .acquire_background(raw_payload.len().max(1))
                    .await,
            )
        };

        // Re-check visibility because the reconcile branch above may flip
        // `remote_object_visible = true` when cache is missing but remote
        // already has this object.
        let object_bytes: Option<Vec<Buffer>> = if remote_object_visible {
            None
        } else {
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
            Some(fragments)
        };

        if object_bytes.is_none() {
            let _ = transit_credit.take();
        }

        if let Some(object_bytes) = object_bytes {
            let _upload_qos =
                self.qos
                    .acquire_upload()
                    .await
                    .map_err(|e| VfsError::QosFailure {
                        stage: Cow::Borrowed("upload_qos"),
                        object_id: op.object_id,
                        message: e.to_string(),
                    })?;
            let uploads = fragment_keys
                .iter()
                .zip(object_bytes.into_iter())
                .enumerate()
                .map(|(fragment_index, (key, payload))| {
                    let cancel_token = upload_cancel_token.clone();
                    async move {
                        self.upload_scheduler
                            .put(key, payload, &cancel_token)
                            .await
                            .map(|_| (fragment_index, key.clone()))
                            .map_err(|e| (fragment_index, key.clone(), e))
                    }
                });
            let done = join_all(uploads).await;
            let mut first_error: Option<(usize, String, crate::upload::UploadError)> = None;
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
                    Err((fragment_index, key, e)) => {
                        if first_error.is_none() {
                            first_error = Some((fragment_index, key, e));
                        }
                    }
                }
            }
            // Treat cancellation as an intentional shutdown path:
            // do not mark sticky errors and do not trigger in-memory requeue.
            // Because `complete_upload` is not called, DIRTY state remains in
            // local backend for safe startup replay on next mount.
            if saw_cancel {
                self.cancel_upload_if_inode_deleted(&op).await?;
                return Ok(());
            }
            if let Some((fragment_index, key, e)) = first_error {
                return Err(VfsError::Other(format!(
                    "fragment upload failed (idx={fragment_index}, key={key}): {e}"
                )));
            }
            debug!(
                ino = op.ino,
                object_id = op.object_id,
                block_index,
                fragments = CLOUD_FRAGMENT_COUNT,
                "process_upload put completed"
            );
        }

        let current = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_file_block(op.ino, block_index)
        })
        .await?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        match current {
            Some(ref block_ref)
                if block_ref.object_id == op.object_id
                    && run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                        engine
                            .get_attr(op.ino)
                            .map(|attr| attr.content_epoch == op.content_epoch)
                    })
                    .await
                    .unwrap_or(false) =>
            {
                self.upload_coordinator.complete_upload(&op);
                debug!(
                    ino = op.ino,
                    object_id = op.object_id,
                    block_index,
                    "process_upload complete_upload on current block"
                );
            }
            _ => {
                run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                    engine.add_pending_deletion(op.object_id, timestamp)
                })
                .await?;
                debug!(
                    object_id = op.object_id,
                    ino = op.ino,
                    block_index,
                    "upload completed for stale block, marked for deletion"
                );
                self.upload_coordinator.complete_upload(&op);
                debug!(
                    ino = op.ino,
                    object_id = op.object_id,
                    block_index,
                    "process_upload complete_upload on stale block"
                );
            }
        }

        if let Ok(attr) = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_attr(op.ino)
        })
        .await
        {
            self.refresh_inode_incarnation_state_from_attr(op.ino, &attr);
        } else {
            self.clear_inode_incarnation(op.ino);
        }
        self.clear_upload_sticky_error(op.ino);

        // Post-upload cleanup: if the block was marked for deferred eviction
        // (inode was deleted while the upload was in flight), remove it now.
        if self
            .state
            .upload
            .pending_evictions
            .lock()
            .remove(&op.object_id)
        {
            self.block_store_remove_async(op.object_id).await?;
            debug!(
                object_id = op.object_id,
                "post-upload eviction: block removed (inode deleted)"
            );
        } else if let Ok(Some(current)) = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_file_block(op.ino, block_index)
        })
        .await
            && current.object_id != op.object_id
        {
            self.block_store_remove_async(op.object_id).await?;
        } else {
            self.block_store_mark_uploaded_async(op.object_id).await?;
        }

        debug!(
            object_id = op.object_id,
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "background upload complete"
        );
        self.state.file_session.note_background_transition(
            op.ino,
            FileSessionOperation::Upload,
            "complete",
        );
        Ok(())
    }

    /// Persist the cache checkpoint to disk.
    ///
    /// Called at shutdown so pending-upload state and local backend metadata
    /// stay consistent across remounts.
    pub async fn persist_cache(&self) {
        self.upload_coordinator.shutdown().await;
        if let Err(error) = self.local_block_backend.sync_data() {
            warn!(error = %error, "local cache backend sync failed during shutdown");
        } else {
            tracing::info!("local cache backend sync persisted");
        }
        tracing::info!("upload coordinator checkpoint persisted");
    }

    /// Best-effort graceful shutdown: drain dirty buffers, persist final
    /// checkpoint state, cancel uploader tasks, then persist upload state.
    pub async fn shutdown(&self, upload_drain_timeout: Duration) {
        if self
            .state
            .shutdown
            .shutdown_completed
            .load(Ordering::Acquire)
        {
            return;
        }
        if self
            .state
            .shutdown
            .shutdown_requested
            .swap(true, Ordering::AcqRel)
        {
            return;
        }
        let t0 = Instant::now();
        let background_join_timeout = Duration::from_secs(2);

        // Phase 0: quiesce non-uploader background work so shutdown can finish
        // the remaining accepted ingress without concurrent producers racing it.
        self.state
            .shutdown
            .slab_flusher_stopped
            .store(true, Ordering::Release);
        self.state
            .ingress
            .ingress_checkpoint_stopped
            .store(true, Ordering::Release);
        self.state.shutdown.shutdown_notify.notify_waiters();
        self.state
            .ingress
            .ingress_checkpoint_notify
            .notify_waiters();
        self.join_background_tasks(background_join_timeout).await;

        // Reset the active-access-class telemetry counters for any handles that
        // were never explicitly closed via release_fh (e.g. force-unmount).
        // The shutdown flag is already set, so no new open_file() calls can
        // increment the counters after this point.
        for class in self.handle_table.collect_open_access_classes() {
            self.state.read.decrement_active_access_class(class);
        }

        // Phase 1: flush pending WriteBuffer state inline into the local backend +
        // metadata so local durability is established before shutdown.
        let dirty_before = self.total_pending_write_buffer_bytes();
        info!(
            dirty_bytes = dirty_before,
            pending_uploads = self.upload_coordinator.pending_upload_count(),
            "VFS shutdown: flushing pending write buffer"
        );
        self.flush_pending_blocks_for_shutdown().await;
        info!(
            pending_uploads = self.upload_coordinator.pending_upload_count(),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: write buffer drain complete"
        );

        if let Err(e) =
            crate::ingress_wal::reset_ingress_artifacts_for_fast_mode(self.ingress_wal.spill_dir())
        {
            warn!("VFS shutdown: failed to remove ingress artifacts: {}", e);
        }

        // Phase 2: attempt graceful upload drain (bounded by user timeout),
        // then cancel uploader work so network-blocked requests are aborted.
        let pending = self.upload_coordinator.pending_upload_count();
        let mut drained = pending == 0;
        if pending > 0 {
            if upload_drain_timeout.is_zero() {
                info!(
                    pending,
                    "VFS shutdown: skipping upload drain and cancelling uploader immediately"
                );
            } else {
                info!(
                    pending,
                    timeout_ms = upload_drain_timeout.as_millis() as u64,
                    "VFS shutdown: waiting for uploads to drain before forced cancellation"
                );
                drained = self
                    .upload_coordinator
                    .wait_for_uploads_idle(upload_drain_timeout)
                    .await;
            }
        }

        self.request_uploader_shutdown();

        if !drained {
            let pending_ops = self.upload_coordinator.snapshot_pending_uploads();
            let sample_object_ids: Vec<u64> =
                pending_ops.iter().take(8).map(|op| op.object_id).collect();
            info!(
                pending = self.upload_coordinator.pending_upload_count(),
                pending_queue_ops = pending_ops.len(),
                pending_object_ids_sample = ?sample_object_ids,
                "VFS shutdown: uploader cancelled; remaining dirty uploads will replay on next startup"
            );
        }
        self.join_uploader_task(background_join_timeout).await;

        // Phase 4: persist metadata + cache checkpoint.
        info!(
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: persisting metadata"
        );
        let meta = self.sync_meta();
        let metadata_flush_timeout = self.shutdown_metadata_flush_timeout;
        match super::run_sync_with_timeout(metadata_flush_timeout, move || {
            meta.flush_metadata().map_err(VfsError::Meta)
        }) {
            Ok(Some(())) => {}
            Ok(None) => {
                warn!(
                    timeout_ms = metadata_flush_timeout.as_millis() as u64,
                    "VFS shutdown: metadata flush timed out, leaving remote sync for next session"
                );
            }
            Err(e) => {
                warn!("VFS shutdown: metadata flush failed: {}", e);
            }
        }
        // Stop any background metadata threads (e.g. PgMetaEngine poll) so
        // their Arc references are released before process exit, allowing
        // redb::Database::drop() to run cleanly.
        self.sync_meta().prepare_for_close();
        self.persist_cache().await;
        self.state
            .shutdown
            .shutdown_completed
            .store(true, Ordering::Release);
        info!(
            total_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: complete"
        );
    }
}

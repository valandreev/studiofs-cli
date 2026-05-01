mod worker;

pub(super) use worker::UploadLifecycleWorker;

use super::{Ino, StudioVfs, VfsError, VfsIngressStats, VfsZeroIoShutdownSnapshot, info, warn};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use studiofs_cache::upload_queue::PendingUpload;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::utils::now_unix_millis;

fn shutdown_recoverable_on_restart(
    dirty_after: usize,
    local_payload_sync_ok: bool,
    metadata_flush_outcome: &str,
) -> bool {
    dirty_after == 0 && local_payload_sync_ok && metadata_flush_outcome == "success"
}

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

    pub(super) fn record_deferred_deleted_local_payload(&self, ino: Ino, object_id: u64) {
        self.state
            .upload
            .deferred_deleted_local_payloads
            .lock()
            .entry(ino)
            .or_default()
            .insert(object_id);
    }

    pub(super) fn take_deferred_deleted_local_payloads(&self, ino: Ino) -> Vec<u64> {
        self.state
            .upload
            .deferred_deleted_local_payloads
            .lock()
            .remove(&ino)
            .map(|ids| ids.into_iter().collect())
            .unwrap_or_default()
    }

    pub(super) fn maybe_clear_deleted_inode_marker(&self, ino: Ino) {
        if !self.state.upload.deleted_inodes.lock().contains(&ino) {
            return;
        }
        if self.handle_table.open_count(ino) > 0 {
            return;
        }
        if self.inode_has_pending_uploads(ino) {
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
        if self.inode_has_pending_uploads(ino) {
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

    pub(super) fn pending_age_stats_ms(&self, pending_uploads: &[PendingUpload]) -> (u64, u64) {
        let now_ms = now_unix_millis();
        let mut ages: Vec<u64> = pending_uploads
            .iter()
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
    }

    pub(super) fn upload_pipeline_summary_is_active(
        pending_uploads: usize,
        active_uploads: usize,
        pending_metadata_ops: u32,
    ) -> bool {
        pending_uploads != 0
            || active_uploads != 0
            || (pending_metadata_ops != 0 && pending_metadata_ops >= 10)
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

    /// Persist the cache checkpoint to disk.
    ///
    /// Called at shutdown so pending-upload state and local backend metadata
    /// stay consistent across remounts.
    pub async fn persist_cache(&self) -> bool {
        self.cached_store.shutdown().await;
        if let Err(error) = self.local_payload_backend.sync_data() {
            warn!(error = %error, "local cache backend sync failed during shutdown");
            tracing::info!(
                "cached-store upload journal persisted, but local cache backend sync failed"
            );
            false
        } else {
            tracing::info!("local cache backend sync persisted");
            tracing::info!("cached-store upload journal persisted");
            true
        }
    }

    /// Best-effort graceful shutdown: drain pending local state, persist the
    /// cached-store upload journal, cancel uploader tasks, then persist upload state.
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
        self.state.shutdown.shutdown_notify.notify_waiters();
        self.join_background_tasks(background_join_timeout).await;

        // Reset the active-access-class telemetry counters for any handles that
        // were never explicitly closed via release_fh (e.g. force-unmount).
        // The shutdown flag is already set, so no new open_file() calls can
        // increment the counters after this point.
        for class in self.handle_table.collect_open_access_classes() {
            self.state.read.decrement_active_access_class(class);
        }

        // Phase 1: flush pending local dirty state inline into the local backend
        // + metadata so local durability is established before shutdown.
        let dirty_before = self.total_pending_write_buffer_bytes();
        info!(
            dirty_bytes = dirty_before,
            pending_uploads = self.pending_uploads_snapshot().len(),
            "VFS shutdown: flushing pending local dirty state"
        );
        self.flush_pending_payloads_for_shutdown().await;
        info!(
            pending_uploads = self.pending_uploads_snapshot().len(),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: local dirty drain complete"
        );

        // Phase 2: attempt graceful upload drain (bounded by user timeout),
        // then cancel uploader work so network-blocked requests are aborted.
        let pending = self.pending_uploads_snapshot().len();
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
                    .wait_for_pending_uploads_idle(upload_drain_timeout)
                    .await;
            }
        }

        self.request_uploader_shutdown();

        if !drained {
            let pending_ops = self.pending_uploads_snapshot();
            let sample_object_ids: Vec<u64> =
                pending_ops.iter().take(8).map(|op| op.object_id).collect();
            info!(
                pending = pending_ops.len(),
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
        let metadata_flush_outcome = match super::run_sync_with_timeout(
            metadata_flush_timeout,
            move || meta.flush_metadata().map_err(VfsError::Meta),
        ) {
            Ok(Some(())) => "success",
            Ok(None) => {
                warn!(
                    timeout_ms = metadata_flush_timeout.as_millis() as u64,
                    "VFS shutdown: metadata flush timed out, leaving remote sync for next session"
                );
                "timeout"
            }
            Err(e) => {
                warn!("VFS shutdown: metadata flush failed: {}", e);
                "failed"
            }
        };
        // Stop any background metadata threads (e.g. PgMetaEngine poll) so
        // their Arc references are released before process exit, allowing
        // redb::Database::drop() to run cleanly.
        self.sync_meta().prepare_for_close();
        let local_payload_sync_ok = self.persist_cache().await;
        let dirty_after = self.total_pending_write_buffer_bytes();
        let pending_uploads_after = self.pending_uploads_snapshot().len();
        let recoverable_on_restart = shutdown_recoverable_on_restart(
            dirty_after,
            local_payload_sync_ok,
            metadata_flush_outcome,
        );
        let shutdown_snapshot = VfsZeroIoShutdownSnapshot {
            generated_unix_ms: now_unix_millis(),
            accepted_local_dirty_bytes_before: dirty_before as u64,
            accepted_local_dirty_bytes_after: dirty_after as u64,
            pending_uploads_after,
            uploader_drained_before_cancel: drained,
            metadata_flush_outcome: metadata_flush_outcome.to_string(),
            local_payload_sync_ok,
            recoverable_on_restart,
        };
        self.record_runtime_event(
            "zeroio",
            "shutdown_local_state",
            "zeroio graceful shutdown local durability",
            serde_json::json!({
                "accepted_local_dirty_bytes_before": shutdown_snapshot.accepted_local_dirty_bytes_before,
                "accepted_local_dirty_bytes_after": shutdown_snapshot.accepted_local_dirty_bytes_after,
                "pending_uploads_after": shutdown_snapshot.pending_uploads_after,
                "uploader_drained_before_cancel": shutdown_snapshot.uploader_drained_before_cancel,
                "metadata_flush_outcome": shutdown_snapshot.metadata_flush_outcome,
                "local_payload_sync_ok": shutdown_snapshot.local_payload_sync_ok,
                "recoverable_on_restart": shutdown_snapshot.recoverable_on_restart,
            }),
        );
        self.persist_zeroio_shutdown_snapshot(&shutdown_snapshot);
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

#[cfg(test)]
mod tests {
    use super::shutdown_recoverable_on_restart;

    #[test]
    fn shutdown_recoverability_requires_metadata_flush_success() {
        assert!(shutdown_recoverable_on_restart(0, true, "success"));
        assert!(!shutdown_recoverable_on_restart(1, true, "success"));
        assert!(!shutdown_recoverable_on_restart(0, false, "success"));
        assert!(!shutdown_recoverable_on_restart(0, true, "timeout"));
        assert!(!shutdown_recoverable_on_restart(0, true, "failed"));
    }
}

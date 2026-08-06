mod worker;

pub(super) use worker::UploadLifecycleWorker;

use super::{
    Ino, ShutdownClaim, ShutdownError, ShutdownPhase, StudioVfs, VfsError, VfsIngressStats,
    VfsResult, VfsZeroIoShutdownSnapshot, info, warn,
};
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

struct ShutdownFinalDrainGuard<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for ShutdownFinalDrainGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct ShutdownOwnerGuard<'a> {
    state: &'a super::ShutdownState,
    completed: bool,
}

impl<'a> ShutdownOwnerGuard<'a> {
    fn new(state: &'a super::ShutdownState) -> Self {
        Self {
            state,
            completed: false,
        }
    }

    fn complete(mut self, error: Option<ShutdownError>) {
        self.state.complete(error);
        self.completed = true;
    }
}

impl Drop for ShutdownOwnerGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.state.abort();
        }
    }
}

struct BackgroundTaskJoinGuard<'a> {
    state: &'a super::ShutdownState,
    handles: Vec<JoinHandle<()>>,
}

impl<'a> BackgroundTaskJoinGuard<'a> {
    fn new(state: &'a super::ShutdownState, handles: Vec<JoinHandle<()>>) -> Self {
        Self { state, handles }
    }
}

impl Drop for BackgroundTaskJoinGuard<'_> {
    fn drop(&mut self) {
        if !self.handles.is_empty() {
            self.state
                .background_tasks
                .lock()
                .extend(std::mem::take(&mut self.handles));
        }
    }
}

struct UploaderTaskJoinGuard<'a> {
    state: &'a super::ShutdownState,
    handle: Option<JoinHandle<()>>,
}

impl<'a> UploaderTaskJoinGuard<'a> {
    fn new(state: &'a super::ShutdownState, handle: JoinHandle<()>) -> Self {
        Self {
            state,
            handle: Some(handle),
        }
    }
}

impl Drop for UploaderTaskJoinGuard<'_> {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let mut tracked = self.state.uploader_task.lock();
        if tracked.is_none() {
            *tracked = Some(handle);
        }
    }
}

fn replay_shutdown_result(error: Option<ShutdownError>) -> VfsResult<()> {
    match error {
        Some(error) => Err(error.into_vfs_error()),
        None => Ok(()),
    }
}

fn finish_shutdown_result(
    shutdown_owner: ShutdownOwnerGuard<'_>,
    shutdown_result: VfsResult<()>,
) -> VfsResult<()> {
    let shutdown_error = shutdown_result
        .as_ref()
        .err()
        .map(ShutdownError::from_vfs_error);
    shutdown_owner.complete(shutdown_error);
    shutdown_result
}

fn select_shutdown_result(
    deferred_namespace_error: Option<VfsError>,
    metadata_barrier_error: Option<VfsError>,
    local_payload_sync_result: VfsResult<()>,
) -> VfsResult<()> {
    deferred_namespace_error
        .or(metadata_barrier_error)
        .or_else(|| local_payload_sync_result.err())
        .map_or(Ok(()), Err)
}

fn select_metadata_barrier_error(
    local_dirty_drain_error: Option<VfsError>,
    metadata_close_error: Option<VfsError>,
    overlay_barrier_error: Option<VfsError>,
    overlay_health_error: Option<VfsError>,
) -> Option<VfsError> {
    let mut selected = None;
    for (barrier, error) in [
        ("local dirty drain", local_dirty_drain_error),
        ("metadata close", metadata_close_error),
        ("overlay barrier", overlay_barrier_error),
        ("overlay health", overlay_health_error),
    ] {
        let Some(error) = error else {
            continue;
        };
        if selected.is_some() {
            warn!(
                barrier,
                error = %error,
                "VFS shutdown: secondary metadata barrier error suppressed"
            );
        } else {
            selected = Some(error);
        }
    }
    selected
}

enum ShutdownWaitResult {
    Retry,
    Completed(Option<ShutdownError>),
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

    pub(super) fn cancel_upload_token_for_inode(&self, ino: Ino) {
        if let Some(token) = self.state.upload.upload_cancel_tokens.lock().remove(&ino) {
            token.cancel();
        }
    }

    pub(super) fn mark_inode_deleted_for_uploads(&self, ino: Ino) {
        // Soft upload cancellation marker: stops background uploads for this inode.
        // Data for still-open unlinked files must remain locally readable until
        // the last handle is released (POSIX open-unlink semantics).
        self.state.upload.deleted_inodes.lock().insert(ino);
        self.state.upload.deleted_uploads_notify.notify_waiters();
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
        // Inode IDs are monotonic within a VFS runtime. Keep this marker as a
        // runtime tombstone so uploader-channel backlog cannot start after the
        // metadata row and cached-store queue have already been cleaned up.
        let _ = ino;
    }

    pub(super) fn maybe_release_upload_cancel_token(&self, ino: Ino) {
        let mut tokens = self.state.upload.upload_cancel_tokens.lock();
        if self.inode_has_pending_uploads(ino) {
            return;
        }
        tokens.remove(&ino);
    }

    /// Returns `true` if the VFS is in the process of shutting down (or has
    /// already completed shutdown).
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_requested() || self.uploader_shutdown_requested()
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
        let mut guard = BackgroundTaskJoinGuard::new(&self.state.shutdown, handles);
        Self::join_background_task_handles(&mut guard.handles, timeout).await;
    }

    async fn join_background_task_handles(handles: &mut Vec<JoinHandle<()>>, timeout: Duration) {
        let join_result = tokio::time::timeout(timeout, async {
            while !handles.is_empty() {
                if let Err(e) = (&mut handles[0]).await
                    && !e.is_cancelled()
                {
                    warn!(error = %e, "background task join failed");
                }
                handles.remove(0);
            }
        })
        .await;
        if join_result.is_err() {
            let aborted = handles
                .iter()
                .filter(|handle| !handle.is_finished())
                .count();
            while !handles.is_empty() {
                if !handles[0].is_finished() {
                    handles[0].abort();
                    let _ = (&mut handles[0]).await;
                }
                handles.remove(0);
            }
            if aborted > 0 {
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    aborted, "background tasks did not stop in time, aborted"
                );
            }
        }
    }

    async fn join_uploader_task(&self, timeout: Duration) {
        let handle = self.state.shutdown.uploader_task.lock().take();
        let Some(handle) = handle else {
            return;
        };
        let mut guard = UploaderTaskJoinGuard::new(&self.state.shutdown, handle);
        let join_result = tokio::time::timeout(
            timeout,
            guard.handle.as_mut().expect("uploader handle is present"),
        )
        .await;
        match join_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.is_cancelled() => {}
            Ok(Err(e)) => {
                warn!(error = %e, "uploader task join failed");
            }
            Err(_) => {
                let handle = guard.handle.as_mut().expect("uploader handle is present");
                handle.abort();
                let _ = handle.await;
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "uploader task did not stop in time, aborted"
                );
            }
        }
        guard.handle.take();
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
        active_upload_workers: usize,
        pending_metadata_ops: u32,
    ) -> bool {
        pending_uploads != 0
            || active_uploads != 0
            || active_upload_workers != 0
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
        self.persist_cache_result().await.is_ok()
    }

    async fn persist_cache_result(&self) -> VfsResult<()> {
        self.cached_store.shutdown().await;
        if let Err(error) = self.local_payload_backend.sync_data() {
            warn!(error = %error, "local cache backend sync failed during shutdown");
            tracing::info!(
                "cached-store upload journal persisted, but local cache backend sync failed"
            );
            Err(VfsError::LocalPayload(error))
        } else {
            tracing::info!("local cache backend sync persisted");
            tracing::info!("cached-store upload journal persisted");
            Ok(())
        }
    }

    async fn wait_for_shutdown_result(&self) -> ShutdownWaitResult {
        loop {
            let notified = self.state.shutdown.shutdown_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let phase = self.state.shutdown.shutdown_phase.lock();
                match &*phase {
                    ShutdownPhase::Running | ShutdownPhase::Aborted => {
                        return ShutdownWaitResult::Retry;
                    }
                    ShutdownPhase::ShuttingDown => {}
                    ShutdownPhase::Completed(error) => {
                        return ShutdownWaitResult::Completed(error.clone());
                    }
                }
            }
            notified.as_mut().await;
        }
    }

    /// Graceful shutdown: drain accepted local state, persist the cached-store
    /// upload journal, cancel uploader tasks, then persist upload state.
    pub async fn shutdown(&self, upload_drain_timeout: Duration) -> VfsResult<()> {
        let shutdown_owner = loop {
            match self.state.shutdown.claim() {
                ShutdownClaim::Owner => break ShutdownOwnerGuard::new(&self.state.shutdown),
                ShutdownClaim::Waiter => match self.wait_for_shutdown_result().await {
                    ShutdownWaitResult::Retry => continue,
                    ShutdownWaitResult::Completed(error) => return replay_shutdown_result(error),
                },
                ShutdownClaim::Completed(error) => return replay_shutdown_result(error),
            }
        };
        let t0 = Instant::now();
        let background_join_timeout = Duration::from_secs(2);

        // Phase 0: quiesce non-uploader background work so shutdown can finish
        // the remaining accepted ingress without concurrent producers racing it.
        self.state
            .shutdown
            .slab_flusher_stopped
            .store(true, Ordering::Release);
        self.state.shutdown.background_shutdown_token.cancel();
        self.state.shutdown.shutdown_notify.notify_waiters();
        self.state.namespace.deferred.notify.notify_waiters();
        let deferred_namespace_result = self
            .wait_for_deferred_namespace_metadata_during_shutdown(background_join_timeout)
            .await;
        self.join_background_tasks(background_join_timeout).await;
        let deferred_namespace_error = deferred_namespace_result
            .err()
            .or_else(|| self.deferred_namespace_shutdown_error());

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
        self.state
            .shutdown
            .shutdown_final_drain_active
            .store(true, Ordering::Release);
        let local_dirty_drain_error = {
            let _shutdown_final_drain_guard =
                ShutdownFinalDrainGuard(&self.state.shutdown.shutdown_final_drain_active);
            self.flush_pending_payloads_for_shutdown().await.err()
        };
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

        // Stop remote metadata workers before the local durability barrier.
        // Their remote sync path takes the replication lock and must not hold
        // shutdown behind a PostgreSQL command timeout.
        let metadata_close_error = self.meta.run(|meta| meta.prepare_for_close()).await.err();

        // Phase 4: persist metadata + cache checkpoint.
        info!(
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: persisting metadata"
        );
        let meta = self.sync_meta();
        let overlay_barrier_error = self.wait_for_volatile_overlay_batches().await.err();
        let metadata_flush_timeout = self.shutdown_metadata_flush_timeout;
        let metadata_flush_result =
            super::run_sync_with_timeout(metadata_flush_timeout, move || {
                meta.sync_local_metadata().map_err(VfsError::Meta)
            });
        let overlay_health_error = self.ensure_volatile_overlay_metadata_healthy().err();
        let mut metadata_barrier_error = select_metadata_barrier_error(
            local_dirty_drain_error,
            metadata_close_error,
            overlay_barrier_error,
            overlay_health_error,
        );
        let metadata_flush_outcome = match metadata_flush_result {
            Ok(Some(()))
                if deferred_namespace_error.is_none() && metadata_barrier_error.is_none() =>
            {
                "success"
            }
            Ok(Some(())) => {
                warn!("VFS shutdown: local metadata drain failed before the final barrier");
                "failed"
            }
            Ok(None) => {
                warn!(
                    timeout_ms = metadata_flush_timeout.as_millis() as u64,
                    "VFS shutdown: metadata flush timed out, leaving remote sync for next session"
                );
                metadata_barrier_error.get_or_insert_with(|| {
                    VfsError::Other(format!(
                        "metadata durability barrier timed out after {} ms",
                        metadata_flush_timeout.as_millis()
                    ))
                });
                "timeout"
            }
            Err(e) => {
                warn!("VFS shutdown: metadata flush failed: {}", e);
                metadata_barrier_error.get_or_insert(e);
                "failed"
            }
        };
        let local_payload_sync_result = self.persist_cache_result().await;
        let local_payload_sync_ok = local_payload_sync_result.is_ok();
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
        if deferred_namespace_error.is_none()
            && metadata_barrier_error.is_none()
            && local_payload_sync_ok
        {
            self.finish_zeroio_session();
        }
        let shutdown_result = select_shutdown_result(
            deferred_namespace_error,
            metadata_barrier_error,
            local_payload_sync_result,
        );
        let shutdown_result = finish_shutdown_result(shutdown_owner, shutdown_result);
        info!(
            total_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: complete"
        );
        shutdown_result
    }
}

#[cfg(test)]
mod tests {
    use std::future;
    use std::time::Duration;

    use super::{
        BackgroundTaskJoinGuard, ShutdownError, ShutdownOwnerGuard, StudioVfs,
        UploaderTaskJoinGuard, finish_shutdown_result, replay_shutdown_result,
        select_metadata_barrier_error, select_shutdown_result, shutdown_recoverable_on_restart,
    };
    use crate::vfs::{ShutdownState, VfsError};
    use studiofs_cache::local::LocalPayloadBackendError;
    use studiofs_data::store::DataError;
    use studiofs_meta::MetaError;

    #[tokio::test(start_paused = true)]
    async fn background_tasks_share_one_join_deadline() {
        let mut handles = (0..3).map(|_| tokio::spawn(future::pending())).collect();
        let timeout = Duration::from_secs(2);
        let started = tokio::time::Instant::now();

        StudioVfs::join_background_task_handles(&mut handles, timeout).await;

        assert_eq!(started.elapsed(), timeout);
    }

    #[tokio::test]
    async fn aborted_background_tasks_are_finished_before_join_returns() {
        let (sender, mut receiver) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _sender = sender;
            future::pending::<()>().await;
        });

        let mut handles = vec![handle];
        StudioVfs::join_background_task_handles(&mut handles, Duration::ZERO).await;

        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test]
    async fn dropped_background_join_guard_restores_unjoined_handles() {
        let state = ShutdownState::new();
        let handle = tokio::spawn(future::pending::<()>());

        {
            let _guard = BackgroundTaskJoinGuard::new(&state, vec![handle]);
        }

        let handle = state
            .background_tasks
            .lock()
            .pop()
            .expect("cancelled join must restore the task handle");
        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn dropped_uploader_join_guard_restores_unjoined_handle() {
        let state = ShutdownState::new();
        let handle = tokio::spawn(future::pending::<()>());

        {
            let _guard = UploaderTaskJoinGuard::new(&state, handle);
        }

        let handle = state
            .uploader_task
            .lock()
            .take()
            .expect("cancelled join must restore the uploader handle");
        handle.abort();
        let _ = handle.await;
    }

    #[test]
    fn shutdown_recoverability_requires_metadata_flush_success() {
        assert!(shutdown_recoverable_on_restart(0, true, "success"));
        assert!(!shutdown_recoverable_on_restart(1, true, "success"));
        assert!(!shutdown_recoverable_on_restart(0, false, "success"));
        assert!(!shutdown_recoverable_on_restart(0, true, "timeout"));
        assert!(!shutdown_recoverable_on_restart(0, true, "failed"));
    }

    #[test]
    fn shutdown_result_reports_local_payload_sync_failure() {
        let local_payload_error = VfsError::LocalPayload(LocalPayloadBackendError::Other(
            Box::new(std::io::Error::other("local payload sync failed")),
        ));

        let result = select_shutdown_result(None, None, Err(local_payload_error));

        assert!(matches!(result, Err(VfsError::LocalPayload(_))));
    }

    #[test]
    fn metadata_barrier_preserves_local_dirty_drain_error() {
        let local_error = VfsError::Other("local dirty drain failed".to_string());
        let metadata_error = VfsError::Meta(MetaError::Storage(
            "postgres worker close timed out".to_string(),
        ));

        let result =
            select_metadata_barrier_error(Some(local_error), Some(metadata_error), None, None);

        assert!(matches!(
            result,
            Some(VfsError::Other(message)) if message == "local dirty drain failed"
        ));
    }

    #[test]
    fn shutdown_replay_preserves_typed_error_for_owner_and_waiter() {
        let original = VfsError::Meta(MetaError::Storage("metadata barrier failed".to_string()));
        let stored = ShutdownError::from_vfs_error(&original);

        let owner = replay_shutdown_result(Some(stored.clone()));
        let waiter = replay_shutdown_result(Some(stored));

        assert!(matches!(owner, Err(VfsError::Meta(_))));
        assert!(matches!(waiter, Err(VfsError::Meta(_))));
    }

    #[test]
    fn shutdown_replay_preserves_data_error_category() {
        let original = VfsError::Data(DataError::Storage("data barrier failed".to_string()));
        let stored = ShutdownError::from_vfs_error(&original);

        let owner = replay_shutdown_result(Some(stored.clone()));
        let waiter = replay_shutdown_result(Some(stored));

        assert!(matches!(owner, Err(VfsError::Data(_))));
        assert!(matches!(waiter, Err(VfsError::Data(_))));
    }

    #[test]
    fn shutdown_owner_returns_original_error_variant() {
        let state = ShutdownState::new();
        let owner = ShutdownOwnerGuard::new(&state);
        let result = finish_shutdown_result(
            owner,
            Err(VfsError::Fetch(crate::fetch::FetchError::NotFound {
                key: "missing".to_string(),
            })),
        );

        assert!(matches!(result, Err(VfsError::Fetch(_))));
    }
}

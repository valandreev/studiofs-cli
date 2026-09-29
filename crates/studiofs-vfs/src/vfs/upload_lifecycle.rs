mod worker;

pub(super) use worker::UploadLifecycleWorker;

use super::{
    BackgroundCleanupThreadTask, Ino, MetadataSyncOutcome, ShutdownClaim, ShutdownError,
    ShutdownPhase, StudioVfs, VfsError, VfsIngressStats, VfsResult, VfsZeroIoShutdownSnapshot,
    info, warn,
};
use crate::OrderedMutex;
use futures::FutureExt as _;
use std::future::Future;
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

struct TaskJoinGuard<'a, T> {
    target: &'a OrderedMutex<Vec<T>>,
    handles: Vec<T>,
}

impl<'a, T> TaskJoinGuard<'a, T> {
    fn new(target: &'a OrderedMutex<Vec<T>>, handles: Vec<T>) -> Self {
        Self { target, handles }
    }
}

impl<T> Drop for TaskJoinGuard<'_, T> {
    fn drop(&mut self) {
        if !self.handles.is_empty() {
            self.target.lock().extend(std::mem::take(&mut self.handles));
        }
    }
}

struct LocalCompactionTaskJoinGuard<'a> {
    state: &'a super::ShutdownState,
    handle: Option<JoinHandle<()>>,
}

impl<'a> LocalCompactionTaskJoinGuard<'a> {
    fn new(state: &'a super::ShutdownState, handle: JoinHandle<()>) -> Self {
        Self {
            state,
            handle: Some(handle),
        }
    }
}

impl Drop for LocalCompactionTaskJoinGuard<'_> {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let mut tracked = self.state.local_metadata_compaction_task.lock();
        if tracked.is_none() {
            *tracked = Some(handle);
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
    let shutdown_result = shutdown_result.map_err(|error| match error {
        VfsError::Join(error) => VfsError::ShutdownJoin(error.to_string()),
        error => error,
    });
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

#[derive(Clone, Copy)]
enum JoinTimeoutPolicy {
    Abort,
    Wait,
}

impl StudioVfs {
    pub(super) fn track_background_task(&self, handle: JoinHandle<()>) {
        let mut tasks = self.state.shutdown.background_tasks.lock();
        Self::reap_finished_background_task_handles(&mut tasks, "background");
        tasks.push(handle);
    }

    pub(super) fn spawn_background_task(&self, task: impl Future<Output = ()> + Send + 'static) {
        let mut tasks = self.state.shutdown.background_tasks.lock();
        Self::reap_finished_background_task_handles(&mut tasks, "background");
        tasks.push(tokio::spawn(task));
    }

    pub(super) fn spawn_background_cleanup_task_on(
        &self,
        runtime: &tokio::runtime::Handle,
        task: impl Future<Output = ()> + Send + 'static,
    ) {
        let mut tasks = self.state.shutdown.background_cleanup_tasks.lock();
        Self::reap_finished_background_task_handles(&mut tasks, "deferred cleanup");
        tasks.push(runtime.spawn(task));
    }

    pub(super) fn spawn_background_cleanup_thread(
        &self,
        name: String,
        cleanup: impl FnOnce() + Send + 'static,
    ) -> std::io::Result<()> {
        let mut tasks = self.state.shutdown.background_cleanup_threads.lock();
        Self::reap_finished_background_cleanup_threads(&mut tasks);
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new().name(name).spawn(move || {
            cleanup();
            let _ = completed_tx.send(());
        })?;
        tasks.push(BackgroundCleanupThreadTask {
            thread,
            completed: completed_rx,
        });
        Ok(())
    }

    fn reap_finished_background_task_handles(
        handles: &mut Vec<JoinHandle<()>>,
        task_name: &'static str,
    ) {
        handles.retain_mut(|handle| {
            if !handle.is_finished() {
                return true;
            }
            match (&mut *handle).now_or_never() {
                Some(Err(error)) => {
                    if !error.is_cancelled() {
                        warn!(task = task_name, error = %error, "background task join failed");
                    }
                    false
                }
                Some(Ok(())) => false,
                None => true,
            }
        });
    }

    fn reap_finished_background_cleanup_threads(handles: &mut Vec<BackgroundCleanupThreadTask>) {
        let mut index = 0;
        while index < handles.len() {
            if handles[index].thread.is_finished() {
                let task = handles.swap_remove(index);
                if task.thread.join().is_err() {
                    warn!("deferred cleanup thread panicked");
                }
            } else {
                index += 1;
            }
        }
    }

    pub(super) fn set_local_metadata_compaction_task(&self, handle: JoinHandle<()>) {
        *self.state.shutdown.local_metadata_compaction_task.lock() = Some(handle);
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
        loop {
            let handles = {
                let mut handles = self.state.shutdown.background_tasks.lock();
                std::mem::take(&mut *handles)
            };
            let cleanup_handles = {
                let mut handles = self.state.shutdown.background_cleanup_tasks.lock();
                std::mem::take(&mut *handles)
            };
            let cleanup_threads = {
                let mut handles = self.state.shutdown.background_cleanup_threads.lock();
                std::mem::take(&mut *handles)
            };
            if handles.is_empty() && cleanup_handles.is_empty() && cleanup_threads.is_empty() {
                return;
            }

            let mut task_guard = TaskJoinGuard::new(&self.state.shutdown.background_tasks, handles);
            Self::join_background_task_handles(
                &mut task_guard.handles,
                timeout,
                true,
                "background",
            )
            .await;

            let mut cleanup_guard = TaskJoinGuard::new(
                &self.state.shutdown.background_cleanup_tasks,
                cleanup_handles,
            );
            Self::join_background_task_handles(
                &mut cleanup_guard.handles,
                timeout,
                false,
                "deferred cleanup",
            )
            .await;

            let mut thread_guard = TaskJoinGuard::new(
                &self.state.shutdown.background_cleanup_threads,
                cleanup_threads,
            );
            Self::join_background_cleanup_thread_handles(&mut thread_guard.handles, timeout).await;
        }
    }

    async fn join_background_cleanup_thread_handles(
        handles: &mut Vec<BackgroundCleanupThreadTask>,
        timeout: Duration,
    ) {
        // This timeout is only the grace period before warning; shutdown must
        // keep owning threads that may still retain the local cache.
        if tokio::time::timeout(timeout, Self::wait_for_cleanup_threads(handles))
            .await
            .is_err()
        {
            warn!(
                timeout_ms = timeout.as_millis() as u64,
                "deferred cleanup threads are still running during shutdown; waiting for completion"
            );
            Self::wait_for_cleanup_threads(handles).await;
        }
    }

    async fn wait_for_cleanup_threads(handles: &mut Vec<BackgroundCleanupThreadTask>) {
        while !handles.is_empty() {
            let _ = (&mut handles[0].completed).await;
            let task = handles.swap_remove(0);
            // Joining can briefly block Tokio during thread teardown, but the
            // signal follows cleanup(), so storage-owning work has ended.
            // Keep the native handle tracked until thread teardown completes.
            if task.thread.join().is_err() {
                warn!("deferred cleanup thread panicked during shutdown");
            }
        }
    }

    async fn wait_for_join_handle(
        handle: &mut JoinHandle<()>,
        timeout: Option<Duration>,
        timeout_policy: JoinTimeoutPolicy,
        on_timeout: impl FnOnce(),
    ) -> Result<(), tokio::task::JoinError> {
        match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, &mut *handle).await {
                Ok(result) => result,
                Err(_) => {
                    if matches!(timeout_policy, JoinTimeoutPolicy::Abort) {
                        handle.abort();
                    }
                    on_timeout();
                    (&mut *handle).await
                }
            },
            None => (&mut *handle).await,
        }
    }

    async fn join_local_metadata_compaction_task(state: &super::ShutdownState, timeout: Duration) {
        let handle = state.local_metadata_compaction_task.lock().take();
        let Some(handle) = handle else {
            return;
        };
        let mut guard = LocalCompactionTaskJoinGuard::new(state, handle);
        let started = Instant::now();
        let join_result = Self::wait_for_join_handle(
            guard
                .handle
                .as_mut()
                .expect("compaction task join guard retains its handle"),
            Some(timeout),
            JoinTimeoutPolicy::Wait,
            || {
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "local metadata compaction worker is still running during shutdown"
                );
            },
        )
        .await;
        if join_result.is_ok() {
            info!(
                elapsed_ms = started.elapsed().as_millis() as u64,
                "local metadata compaction worker finished during shutdown"
            );
        }
        if let Err(error) = join_result
            && !error.is_cancelled()
        {
            warn!(error = %error, "local metadata compaction worker join failed");
        }
        guard.handle.take();
    }

    async fn join_background_task_handles(
        handles: &mut Vec<JoinHandle<()>>,
        timeout: Duration,
        abort_on_timeout: bool,
        task_name: &'static str,
    ) {
        let join_result = tokio::time::timeout(
            timeout,
            Self::wait_for_background_task_handles(handles, task_name),
        )
        .await;
        if join_result.is_err() {
            if abort_on_timeout {
                let aborted = handles
                    .iter()
                    .filter(|handle| !handle.is_finished())
                    .count();
                for handle in handles.iter().filter(|handle| !handle.is_finished()) {
                    handle.abort();
                }
                Self::wait_for_background_task_handles(handles, task_name).await;
                if aborted > 0 {
                    warn!(
                        timeout_ms = timeout.as_millis() as u64,
                        aborted, "background tasks did not stop in time, aborted"
                    );
                }
            } else {
                // This timeout is only a grace period before warning. Cleanup
                // tasks may still own redb or cache handles, so shutdown waits
                // until those owners finish before releasing local stores.
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "deferred cleanup tasks are still running during shutdown; waiting for completion"
                );
                Self::wait_for_background_task_handles(handles, task_name).await;
            }
        }
    }

    async fn wait_for_background_task_handles(
        handles: &mut Vec<JoinHandle<()>>,
        task_name: &'static str,
    ) {
        while !handles.is_empty() {
            let join_result =
                Self::wait_for_join_handle(&mut handles[0], None, JoinTimeoutPolicy::Wait, || {})
                    .await;
            if let Err(error) = join_result
                && !error.is_cancelled()
            {
                warn!(task = task_name, error = %error, "background task join failed");
            }
            handles.swap_remove(0);
        }
    }

    async fn join_uploader_task(&self, timeout: Duration) {
        let handle = self.state.shutdown.uploader_task.lock().take();
        let Some(handle) = handle else {
            return;
        };
        let mut guard = UploaderTaskJoinGuard::new(&self.state.shutdown, handle);
        let join_result = Self::wait_for_join_handle(
            guard.handle.as_mut().expect("uploader handle is present"),
            Some(timeout),
            JoinTimeoutPolicy::Abort,
            || {
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "uploader task did not stop in time, aborted"
                );
            },
        )
        .await;
        if let Err(error) = join_result
            && !error.is_cancelled()
        {
            warn!(error = %error, "uploader task join failed");
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
            tracing::info!("upload queue stopped, but local cache backend sync failed");
            Err(VfsError::LocalPayload(error))
        } else {
            tracing::info!("local cache backend sync persisted");
            tracing::info!("upload queue stopped and local cache backend sync persisted");
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

    /// Graceful shutdown: drain accepted local state, stop the cached-store
    /// upload queue, cancel uploader tasks, then persist upload state.
    ///
    /// If the local metadata barrier exceeds its configured timeout, shutdown
    /// returns a terminal error but does not cancel the running blocking sync.
    /// After any shutdown error, call
    /// [`join_pending_metadata_sync`](Self::join_pending_metadata_sync) before
    /// dropping this VFS if the process needs to reopen the same database.
    /// Dropping the VFS without joining leaves the task detached until it finishes;
    /// subsequent calls to `shutdown` replay the terminal error.
    ///
    /// # Errors
    /// Returns the first shutdown failure, including a metadata timeout.
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
        let background_join_timeout = Duration::from_secs(2);
        let _mutating_operation_admission = tokio::time::timeout(
            background_join_timeout,
            self.state.shutdown.mutating_operation_gate.write(),
        )
        .await
        .map_err(|_| VfsError::shutdown_admission_timeout())?;
        let t0 = Instant::now();
        self.stop_coherence();

        // Phase 0: quiesce non-uploader background work so shutdown can finish
        // the remaining accepted ingress without concurrent producers racing it.
        self.state
            .shutdown
            .slab_flusher_stopped
            .store(true, Ordering::Release);
        self.state.shutdown.background_shutdown_token.cancel();
        self.state.shutdown.shutdown_notify.notify_waiters();
        self.state.namespace.deferred.notify.notify_waiters();
        Self::join_local_metadata_compaction_task(&self.state.shutdown, background_join_timeout)
            .await;
        let deferred_namespace_result = self
            .wait_for_deferred_namespace_metadata_during_shutdown(background_join_timeout)
            .await;
        self.join_background_tasks(background_join_timeout).await;
        let mut deferred_namespace_error = deferred_namespace_result
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

        // The final dirty drain can schedule namespace and deleted-inode cleanup
        // after the phase-zero task snapshot. Join that work before closing local stores.
        if let Err(error) = self
            .wait_for_deferred_namespace_metadata_during_shutdown(background_join_timeout)
            .await
        {
            deferred_namespace_error.get_or_insert(error);
        }
        self.join_background_tasks(background_join_timeout).await;
        if deferred_namespace_error.is_none() {
            deferred_namespace_error = self.deferred_namespace_shutdown_error();
        }

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
        let metadata_flush_result = super::run_metadata_sync_with_timeout(
            metadata_flush_timeout,
            &self.state.shutdown.metadata_sync_task,
            move || meta.sync_local_metadata().map_err(VfsError::Meta),
        )
        .await;
        let overlay_health_error = self.ensure_volatile_overlay_metadata_healthy().err();
        let mut metadata_barrier_error = select_metadata_barrier_error(
            local_dirty_drain_error,
            metadata_close_error,
            overlay_barrier_error,
            overlay_health_error,
        );
        let metadata_flush_outcome = match metadata_flush_result {
            Ok(MetadataSyncOutcome::Completed)
                if deferred_namespace_error.is_none() && metadata_barrier_error.is_none() =>
            {
                "success"
            }
            Ok(MetadataSyncOutcome::Completed) => {
                warn!("VFS shutdown: local metadata drain failed before the final barrier");
                "failed"
            }
            Ok(MetadataSyncOutcome::TimedOut) => {
                warn!(
                    timeout_ms = metadata_flush_timeout.as_millis() as u64,
                    "VFS shutdown: local metadata flush timed out; local durability barrier remains incomplete"
                );
                metadata_barrier_error.get_or_insert_with(|| {
                    VfsError::shutdown_metadata_timeout(metadata_flush_timeout)
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

    /// Wait for and join a local metadata sync that outlived shutdown's timeout.
    ///
    /// Call after [`shutdown`](Self::shutdown) returns and before dropping this
    /// VFS if the process must reopen the same redb database. Returns immediately
    /// when no sync remains. The wait has no deadline; dropping this future
    /// preserves the handle so the caller may retry.
    ///
    /// # Errors
    /// Returns a metadata sync error or a task join error.
    pub async fn join_pending_metadata_sync(&self) -> VfsResult<()> {
        super::join_pending_metadata_sync_task(&self.state.shutdown.metadata_sync_task).await
    }
}

#[cfg(test)]
mod tests {
    use std::future::{self, Future as _};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing::instrument::WithSubscriber as _;

    use super::{
        JoinTimeoutPolicy, LocalCompactionTaskJoinGuard, ShutdownError, ShutdownOwnerGuard,
        StudioVfs, TaskJoinGuard, UploaderTaskJoinGuard, finish_shutdown_result,
        replay_shutdown_result, select_metadata_barrier_error, select_shutdown_result,
        shutdown_recoverable_on_restart,
    };
    use crate::vfs::{ShutdownClaim, ShutdownState, VfsError};
    use studiofs_cache::local::LocalPayloadBackendError;
    use studiofs_data::store::DataError;
    use studiofs_meta::MetaError;
    use tracing_subscriber::prelude::*;

    #[derive(Clone)]
    struct CompactionJoinEvents(Arc<Mutex<Vec<(tracing::Level, String)>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CompactionJoinEvents {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event
                .metadata()
                .target()
                .ends_with("::vfs::upload_lifecycle")
            {
                let mut visitor = MessageVisitor(None);
                event.record(&mut visitor);
                if let Some(message) = visitor.0 {
                    self.0
                        .lock()
                        .expect("lock captured events")
                        .push((*event.metadata().level(), message));
                }
            }
        }
    }

    struct MessageVisitor(Option<String>);

    impl tracing::field::Visit for MessageVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = Some(format!("{value:?}"));
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                self.0 = Some(value.to_owned());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn background_tasks_share_one_join_deadline() {
        let mut handles = (0..3).map(|_| tokio::spawn(future::pending())).collect();
        let timeout = Duration::from_secs(2);
        let started = tokio::time::Instant::now();

        StudioVfs::join_background_task_handles(&mut handles, timeout, true, "background").await;

        assert_eq!(started.elapsed(), timeout);
    }

    #[tokio::test]
    async fn finished_background_tasks_are_reaped_with_panics_reported() {
        let completed = tokio::spawn(async {});
        let panicked = tokio::spawn(async { panic!("expected reaped-task panic") });
        let pending = tokio::spawn(future::pending());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !completed.is_finished() || !panicked.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("finished tasks should settle");

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(CompactionJoinEvents(Arc::clone(&events)));
        let mut handles = vec![completed, panicked, pending];
        async {
            StudioVfs::reap_finished_background_task_handles(&mut handles, "background");
        }
        .with_subscriber(tracing::Dispatch::new(subscriber))
        .await;

        assert_eq!(handles.len(), 1, "only the active handle remains");
        assert!(
            events
                .lock()
                .expect("lock captured events")
                .iter()
                .any(|(_, message)| message.contains("background task join failed"))
        );
        handles[0].abort();
        let _ = (&mut handles[0]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn background_join_reports_finished_panics_after_timeout() {
        let pending = tokio::spawn(future::pending());
        let (panic_tx, panic_rx) = tokio::sync::oneshot::channel::<()>();
        let (panic_ready_tx, panic_ready_rx) = tokio::sync::oneshot::channel::<()>();
        let panicked = tokio::spawn(async move {
            panic_rx.await.expect("release panic task");
            let _ = panic_ready_tx.send(());
            panic!("expected timeout-branch panic");
        });
        let mut handles = vec![pending, panicked];
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(CompactionJoinEvents(Arc::clone(&events)));
        let mut join = Box::pin(
            StudioVfs::join_background_task_handles(
                &mut handles,
                Duration::from_millis(1),
                true,
                "background",
            )
            .with_subscriber(tracing::Dispatch::new(subscriber)),
        );
        let before_timeout =
            future::poll_fn(|cx| std::task::Poll::Ready(join.as_mut().poll(cx))).await;
        assert!(before_timeout.is_pending());

        panic_tx.send(()).expect("release panic task");
        panic_ready_rx.await.expect("panic task should be ready");
        tokio::time::advance(Duration::from_millis(2)).await;
        join.await;

        assert!(
            events
                .lock()
                .expect("lock captured events")
                .iter()
                .any(|(_, message)| message.contains("background task join failed"))
        );
        assert!(handles.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_cleanup_join_waits_past_timeout_without_aborting_task() {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            release_rx.await.expect("release cleanup task");
            completed_tx.send(()).expect("report cleanup completion");
        });
        let mut handles = vec![handle];
        let mut join = Box::pin(StudioVfs::join_background_task_handles(
            &mut handles,
            Duration::from_millis(1),
            false,
            "deferred cleanup",
        ));
        let before_timeout =
            future::poll_fn(|cx| std::task::Poll::Ready(join.as_mut().poll(cx))).await;
        assert!(before_timeout.is_pending());

        tokio::time::advance(Duration::from_millis(2)).await;
        let after_timeout =
            future::poll_fn(|cx| std::task::Poll::Ready(join.as_mut().poll(cx))).await;
        assert!(
            after_timeout.is_pending(),
            "cleanup join must keep waiting instead of aborting the task"
        );

        release_tx
            .send(())
            .expect("cleanup task must remain active");
        join.await;
        completed_rx
            .await
            .expect("cleanup task must complete normally");
        assert!(handles.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_thread_join_reaps_completed_threads_and_waits_past_timeout() {
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel::<()>();
        let completed_thread = std::thread::spawn(move || {
            let _completed_tx = completed_tx;
            panic!("expected cleanup-thread panic");
        });
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (running_tx, running_rx) = tokio::sync::oneshot::channel::<()>();
        let running_thread = std::thread::spawn(move || {
            let _ = start_tx.send(());
            let _ = release_rx.recv();
            let _ = running_tx.send(());
        });
        start_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("cleanup thread should start");
        tokio::time::timeout(Duration::from_secs(1), async {
            while !completed_thread.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first cleanup thread should exit");

        let mut handles = vec![
            crate::vfs::BackgroundCleanupThreadTask {
                thread: completed_thread,
                completed: completed_rx,
            },
            crate::vfs::BackgroundCleanupThreadTask {
                thread: running_thread,
                completed: running_rx,
            },
        ];
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(CompactionJoinEvents(Arc::clone(&events)));
        tracing::subscriber::with_default(subscriber, || {
            StudioVfs::reap_finished_background_cleanup_threads(&mut handles);
        });
        assert_eq!(handles.len(), 1, "the completed thread handle is reaped");
        assert!(
            events
                .lock()
                .expect("lock captured events")
                .iter()
                .any(|(_, message)| message.contains("deferred cleanup thread panicked"))
        );

        let mut join = Box::pin(StudioVfs::join_background_cleanup_thread_handles(
            &mut handles,
            Duration::from_millis(1),
        ));
        let before_timeout =
            future::poll_fn(|cx| std::task::Poll::Ready(join.as_mut().poll(cx))).await;
        assert!(before_timeout.is_pending());
        tokio::time::advance(Duration::from_millis(2)).await;
        let after_timeout =
            future::poll_fn(|cx| std::task::Poll::Ready(join.as_mut().poll(cx))).await;
        assert!(after_timeout.is_pending());

        release_tx.send(()).expect("release cleanup thread");
        join.await;
        assert!(handles.is_empty());
    }

    #[tokio::test]
    async fn aborted_background_tasks_are_finished_before_join_returns() {
        let (sender, mut receiver) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _sender = sender;
            future::pending::<()>().await;
        });

        let mut handles = vec![handle];
        StudioVfs::join_background_task_handles(&mut handles, Duration::ZERO, true, "background")
            .await;

        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn join_handle_waiter_aborts_when_timeout_policy_requests_it() {
        let mut handle = tokio::spawn(future::pending::<()>());
        let mut timed_out = false;

        let result = StudioVfs::wait_for_join_handle(
            &mut handle,
            Some(Duration::ZERO),
            JoinTimeoutPolicy::Abort,
            || timed_out = true,
        )
        .await;

        assert!(timed_out);
        assert!(
            result
                .expect_err("the timed-out task must be aborted")
                .is_cancelled()
        );
    }

    #[tokio::test]
    async fn dropped_background_join_guard_restores_unjoined_handles() {
        let state = ShutdownState::new();
        let handle = tokio::spawn(future::pending::<()>());

        {
            let _guard = TaskJoinGuard::new(&state.background_tasks, vec![handle]);
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
    async fn local_compaction_join_waits_for_task_completion() {
        let state = ShutdownState::new();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        *state.local_metadata_compaction_task.lock() = Some(handle);
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(CompactionJoinEvents(Arc::clone(&events)));

        let mut join = Box::pin(
            StudioVfs::join_local_metadata_compaction_task(&state, Duration::from_secs(2))
                .with_subscriber(tracing::Dispatch::new(subscriber)),
        );
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut join)
                .await
                .is_err()
        );
        assert!(
            !events
                .lock()
                .expect("lock captured events")
                .iter()
                .any(|(_, message)| message.contains("local metadata compaction worker finished")),
            "completion must not be logged while the worker is still running"
        );

        release_tx.send(()).expect("release compaction task");
        join.await;
        assert!(
            events
                .lock()
                .expect("lock captured events")
                .iter()
                .any(|(level, message)| *level == tracing::Level::INFO
                    && message.contains("local metadata compaction worker finished")),
            "normal completion must be logged after the worker joins"
        );
        assert!(state.local_metadata_compaction_task.lock().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn local_compaction_join_waits_past_timeout_without_aborting_task() {
        let state = ShutdownState::new();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            release_rx.await.expect("release compaction task");
            completed_tx.send(()).expect("report task completion");
        });
        *state.local_metadata_compaction_task.lock() = Some(handle);
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(CompactionJoinEvents(Arc::clone(&events)));

        let mut join = Box::pin(
            StudioVfs::join_local_metadata_compaction_task(&state, Duration::from_millis(1))
                .with_subscriber(tracing::Dispatch::new(subscriber)),
        );
        let before_timeout =
            future::poll_fn(|cx| std::task::Poll::Ready(join.as_mut().poll(cx))).await;
        assert!(before_timeout.is_pending());

        tokio::time::advance(Duration::from_millis(2)).await;
        let after_timeout =
            future::poll_fn(|cx| std::task::Poll::Ready(join.as_mut().poll(cx))).await;
        {
            let events = events.lock().expect("lock captured events");
            assert!(
                events
                    .iter()
                    .any(|(_, message)| message.contains("still running during shutdown")),
                "timeout branch must emit its compaction-wait event"
            );
            assert!(
                !events.iter().any(|(_, message)| {
                    message.contains("local metadata compaction worker finished")
                }),
                "timeout must not log completion before the worker finishes"
            );
        }
        assert!(
            after_timeout.is_pending(),
            "join must keep waiting for the task"
        );

        release_tx
            .send(())
            .expect("task must still be running after timeout");
        join.await;
        completed_rx
            .await
            .expect("compaction task must complete normally");
        assert!(
            events
                .lock()
                .expect("lock captured events")
                .iter()
                .any(|(level, message)| *level == tracing::Level::INFO
                    && message.contains("local metadata compaction worker finished")),
            "completion must be logged after the timed-out worker joins"
        );
        assert!(state.local_metadata_compaction_task.lock().is_none());
    }

    #[test]
    fn cancelled_local_compaction_join_does_not_warn() {
        let state = ShutdownState::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create test runtime");
        let handle = runtime.spawn(future::pending::<()>());
        handle.abort();
        *state.local_metadata_compaction_task.lock() = Some(handle);

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber =
            tracing_subscriber::registry().with(CompactionJoinEvents(Arc::clone(&events)));
        tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(StudioVfs::join_local_metadata_compaction_task(
                &state,
                Duration::from_secs(1),
            ));
        });

        let events = events.lock().expect("lock captured events");
        assert!(
            !events.iter().any(
                |(_, message)| message.contains("local metadata compaction worker join failed")
            ),
            "cancellation must not emit the compaction join-failure event"
        );
        assert!(
            !events
                .iter()
                .any(|(level, _)| *level == tracing::Level::WARN),
            "cancellation must not emit a compaction warning"
        );
        assert!(
            !events.iter().any(|(_, message)| {
                message.contains("local metadata compaction worker finished")
            }),
            "cancellation must not emit the compaction completion event"
        );
    }

    #[tokio::test]
    async fn dropped_local_compaction_join_guard_restores_handle() {
        let state = ShutdownState::new();
        let handle = tokio::spawn(future::pending::<()>());

        {
            let _guard = LocalCompactionTaskJoinGuard::new(&state, handle);
        }

        let handle = state
            .local_metadata_compaction_task
            .lock()
            .take()
            .expect("cancelled join must restore the compaction handle");
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

    #[tokio::test]
    async fn shutdown_replay_preserves_join_error_category_for_owner_and_waiter() {
        let state = ShutdownState::new();
        let task = tokio::spawn(future::pending::<()>());
        task.abort();
        let join_error = task
            .await
            .expect_err("aborted task must report join failure");
        let expected = join_error.to_string();
        let owner = finish_shutdown_result(
            ShutdownOwnerGuard::new(&state),
            Err(VfsError::Join(join_error)),
        );
        let ShutdownClaim::Completed(stored_error) = state.claim() else {
            panic!("shutdown result must be cached")
        };
        let waiter = replay_shutdown_result(stored_error);

        let Err(VfsError::ShutdownJoin(owner_error)) = owner else {
            panic!("shutdown owner should receive the stable join error category")
        };
        let Err(VfsError::ShutdownJoin(waiter_error)) = waiter else {
            panic!("shutdown waiter should receive the stable join error category")
        };
        assert_eq!(owner_error, expected);
        assert_eq!(waiter_error, expected);
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

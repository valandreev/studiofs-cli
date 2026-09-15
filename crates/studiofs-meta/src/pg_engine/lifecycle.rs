use super::{
    MetadataSyncSource, PgBackgroundWorker, PgBackgroundWorkerState, PgBackgroundWorkers,
    PgMetaEngine,
};
use crate::engine::{MetaError, MetaResult, PgSyncLifecycle};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};
use tracing::warn;

/// Maximum number of replay pages a startup session may apply while holding
/// `replication_lock`; a later poll resumes from the durable local cursor.
const MAX_STARTUP_CATCH_UP_PAGES: usize = 16;

/// Drain bounded replay pages until the local cursor reaches a frozen target.
///
/// The continuation callback is checked before each page, so cancellation or
/// a lifecycle transition stops the session cleanly. A successful page must
/// advance the local cursor unless it reaches the target; otherwise the
/// helper returns `MetaError::CatchUpStalled` instead of spinning.
pub(super) fn drain_startup_catch_up_pages(
    target_version: i64,
    mut local_version: impl FnMut() -> i64,
    mut should_continue: impl FnMut() -> bool,
    mut pull_page: impl FnMut() -> MetaResult<bool>,
) -> MetaResult<bool> {
    if target_version < 0 {
        return Err(MetaError::Storage(
            "postgres startup catch-up target must not be negative".to_string(),
        ));
    }

    let mut changed = false;
    let mut pages = 0;
    let mut current = local_version();
    while current < target_version {
        if !should_continue() {
            return Ok(changed);
        }
        if pages >= MAX_STARTUP_CATCH_UP_PAGES {
            return Err(MetaError::CatchUpBudgetExceeded {
                local_version: current,
                target_version,
                pages,
            });
        }

        changed |= pull_page()?;
        pages += 1;
        let next = local_version();
        if next >= target_version {
            return Ok(changed);
        }
        if next <= current {
            return Err(MetaError::CatchUpStalled {
                local_version: next,
                target_version,
            });
        }
        current = next;
    }
    Ok(changed)
}

/// Require a serving snapshot to be imported before refreshing the catch-up target.
pub(super) fn rebootstrap_startup_target(
    rebootstrap_error: MetaError,
    import_snapshot: impl FnOnce() -> MetaResult<bool>,
    frozen_target: impl FnOnce() -> MetaResult<i64>,
) -> MetaResult<i64> {
    if !import_snapshot()? {
        return Err(rebootstrap_error);
    }
    frozen_target()
}

impl PgBackgroundWorkers {
    pub(super) fn new() -> Self {
        Self {
            poll_shutdown: Arc::new(AtomicBool::new(false)),
            poll_wakeup: Arc::new((Mutex::new(false), Condvar::new())),
            checkpoint_wakeup: Arc::new((Mutex::new(false), Condvar::new())),
            lock_notify_cancel: tokio_util::sync::CancellationToken::new(),
            metadata_notify_cancel: tokio_util::sync::CancellationToken::new(),
            state: Mutex::new(PgBackgroundWorkerState {
                poll_handle: None,
                checkpoint_handle: None,
                metadata_notify_handle: None,
                lock_notify_handle: None,
                lock_renew_handle: None,
                shutdown_complete: false,
                shutdown_in_progress: false,
                shutdown_error: None,
            }),
            state_changed: Condvar::new(),
        }
    }

    pub(super) fn poll_shutdown(&self) -> Arc<AtomicBool> {
        self.poll_shutdown.clone()
    }

    pub(super) fn poll_wakeup(&self) -> Arc<(Mutex<bool>, Condvar)> {
        self.poll_wakeup.clone()
    }

    pub(super) fn checkpoint_wakeup(&self) -> Arc<(Mutex<bool>, Condvar)> {
        self.checkpoint_wakeup.clone()
    }

    pub(super) fn lock_notify_cancel(&self) -> tokio_util::sync::CancellationToken {
        self.lock_notify_cancel.clone()
    }

    pub(super) fn metadata_notify_cancel(&self) -> tokio_util::sync::CancellationToken {
        self.metadata_notify_cancel.clone()
    }

    pub(super) fn has_handle(&self, worker: PgBackgroundWorker) -> bool {
        self.state
            .lock()
            .map(|state| worker_slot(&state, worker).is_some())
            .unwrap_or(false)
    }

    #[cfg(test)]
    pub(super) fn handle_status(&self, worker: PgBackgroundWorker) -> (bool, bool) {
        self.state
            .lock()
            .ok()
            .and_then(|state| {
                worker_slot(&state, worker).map(|handle| (true, handle.is_finished()))
            })
            .unwrap_or((false, false))
    }

    pub(super) fn install_handle(
        &self,
        worker: PgBackgroundWorker,
        handle: std::thread::JoinHandle<()>,
    ) -> MetaResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| MetaError::Storage("pg background worker state poisoned".to_string()))?;
        if state.shutdown_complete {
            self.signal_stop();
            return Err(MetaError::Storage(
                "pg background workers are already shut down".to_string(),
            ));
        }
        let slot = worker_slot_mut(&mut state, worker);
        if slot.is_some() {
            return Err(MetaError::Storage(format!(
                "pg background worker handle already installed: {}",
                worker_name(worker)
            )));
        }
        *slot = Some(handle);
        Ok(())
    }

    pub(super) fn signal_poll_wakeup(&self) {
        let (wakeup_lock, wakeup_cv) = &*self.poll_wakeup;
        let mut wakeup_guard = wakeup_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *wakeup_guard = true;
        wakeup_cv.notify_one();
    }

    pub(super) fn signal_checkpoint_wakeup(&self) {
        let (wakeup_lock, wakeup_cv) = &*self.checkpoint_wakeup;
        let mut wakeup_guard = wakeup_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *wakeup_guard = true;
        wakeup_cv.notify_one();
    }

    pub(super) fn signal_stop(&self) {
        self.lock_notify_cancel.cancel();
        self.metadata_notify_cancel.cancel();
        self.poll_shutdown.store(true, Ordering::Release);
        self.signal_poll_wakeup();
        self.signal_checkpoint_wakeup();
    }

    pub(super) fn shutdown(&self, timeout: Duration) -> MetaResult<()> {
        let handles = {
            let mut state = self.state.lock().map_err(|_| {
                MetaError::Storage("pg background worker state poisoned".to_string())
            })?;
            if state.shutdown_complete {
                while state.shutdown_in_progress {
                    state = self.state_changed.wait(state).map_err(|_| {
                        MetaError::Storage("pg background worker state poisoned".to_string())
                    })?;
                }
                return match &state.shutdown_error {
                    Some(error) => Err(error.clone()),
                    None => Ok(()),
                };
            }

            self.signal_stop();
            state.shutdown_complete = true;
            state.shutdown_in_progress = true;
            [
                (PgBackgroundWorker::Poll, state.poll_handle.take()),
                (
                    PgBackgroundWorker::Checkpoint,
                    state.checkpoint_handle.take(),
                ),
                (
                    PgBackgroundWorker::MetadataNotify,
                    state.metadata_notify_handle.take(),
                ),
                (
                    PgBackgroundWorker::LockNotify,
                    state.lock_notify_handle.take(),
                ),
                (
                    PgBackgroundWorker::LockRenew,
                    state.lock_renew_handle.take(),
                ),
            ]
        };

        let deadline = Instant::now() + timeout;
        let mut first_error = None;
        for (worker, handle) in handles {
            let result =
                handle.and_then(|handle| join_worker_until(handle, deadline, worker).err());
            if first_error.is_none() {
                first_error = result;
            }
        }

        let mut state = self
            .state
            .lock()
            .map_err(|_| MetaError::Storage("pg background worker state poisoned".to_string()))?;
        state.shutdown_in_progress = false;
        state.shutdown_error = first_error.clone();
        self.state_changed.notify_all();
        drop(state);
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for PgBackgroundWorkers {
    fn drop(&mut self) {
        self.signal_stop();
    }
}

fn worker_slot(
    state: &PgBackgroundWorkerState,
    worker: PgBackgroundWorker,
) -> Option<&std::thread::JoinHandle<()>> {
    match worker {
        PgBackgroundWorker::Poll => state.poll_handle.as_ref(),
        PgBackgroundWorker::Checkpoint => state.checkpoint_handle.as_ref(),
        PgBackgroundWorker::MetadataNotify => state.metadata_notify_handle.as_ref(),
        PgBackgroundWorker::LockNotify => state.lock_notify_handle.as_ref(),
        PgBackgroundWorker::LockRenew => state.lock_renew_handle.as_ref(),
    }
}

fn worker_slot_mut(
    state: &mut PgBackgroundWorkerState,
    worker: PgBackgroundWorker,
) -> &mut Option<std::thread::JoinHandle<()>> {
    match worker {
        PgBackgroundWorker::Poll => &mut state.poll_handle,
        PgBackgroundWorker::Checkpoint => &mut state.checkpoint_handle,
        PgBackgroundWorker::MetadataNotify => &mut state.metadata_notify_handle,
        PgBackgroundWorker::LockNotify => &mut state.lock_notify_handle,
        PgBackgroundWorker::LockRenew => &mut state.lock_renew_handle,
    }
}

fn worker_name(worker: PgBackgroundWorker) -> &'static str {
    match worker {
        PgBackgroundWorker::Poll => "poll",
        PgBackgroundWorker::Checkpoint => "checkpoint",
        PgBackgroundWorker::MetadataNotify => "metadata-notify",
        PgBackgroundWorker::LockNotify => "notify",
        PgBackgroundWorker::LockRenew => "renew",
    }
}

fn join_worker_until(
    handle: std::thread::JoinHandle<()>,
    deadline: Instant,
    worker: PgBackgroundWorker,
) -> MetaResult<()> {
    while !handle.is_finished() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let worker_label = worker_name(worker);
            if matches!(worker, PgBackgroundWorker::Checkpoint) {
                warn!(
                    worker = worker_label,
                    "pg checkpoint thread exceeded shutdown join timeout; joining before releasing the metadata engine"
                );
                return join_worker(handle, worker_label);
            }
            warn!(
                worker = worker_label,
                "pg background thread did not stop before shutdown join timeout"
            );
            return Err(MetaError::Storage(format!(
                "pg {worker_label} background thread did not stop before shutdown join timeout"
            )));
        }
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }

    join_worker(handle, worker_name(worker))
}

fn join_worker(handle: std::thread::JoinHandle<()>, worker: &'static str) -> MetaResult<()> {
    handle.join().map_err(|_| {
        warn!(worker, "pg background thread panicked during shutdown join");
        MetaError::Storage(format!(
            "pg {worker} background thread panicked during shutdown join"
        ))
    })
}

impl PgMetaEngine {
    pub(super) fn default_prepare_for_close_join_timeout() -> Duration {
        Self::env_duration_ms("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", 2_000)
    }

    /// Run one interruptible startup catch-up session to the frozen target.
    pub(super) fn run_startup_catch_up_session(&self, shutdown: &AtomicBool) -> MetaResult<bool> {
        let remote_operation_recorded = Cell::new(false);
        let result = (|| {
            self.ensure_remote_format_validated()?;
            let _guard = self
                .replication_lock
                .lock()
                .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
            if shutdown.load(Ordering::Acquire)
                || !matches!(
                    self.current_pg_sync_lifecycle(),
                    PgSyncLifecycle::CatchingUp | PgSyncLifecycle::Degraded
                )
            {
                return Ok(false);
            }

            // Keep target capture and every page pull under one lock so a
            // concurrent NOTIFY sync cannot clear the target mid-session.
            let target_version = match self.frozen_sync_target_version() {
                Ok(target_version) => target_version,
                Err(error) => {
                    remote_operation_recorded.set(true);
                    return Err(error);
                }
            };
            let pull_page = |target_version| {
                let result = self.pull_foreign_events_locked_to_target(None, target_version);
                if result.is_err() {
                    remote_operation_recorded.set(true);
                }
                result
            };
            let drain_to_target = |target_version| {
                drain_startup_catch_up_pages(
                    target_version,
                    || self.local_version.load(Ordering::Acquire),
                    || {
                        !shutdown.load(Ordering::Acquire)
                            && matches!(
                                self.current_pg_sync_lifecycle(),
                                PgSyncLifecycle::CatchingUp | PgSyncLifecycle::Degraded
                            )
                    },
                    || pull_page(target_version),
                )
            };
            let changed = match drain_to_target(target_version) {
                Ok(changed) => changed,
                Err(rebootstrap_error @ MetaError::RebootstrapRequired { .. }) => {
                    let result = rebootstrap_startup_target(
                        rebootstrap_error,
                        || {
                            self.pull_snapshot_locked(
                                super::StartupSnapshotImportMode::PreserveLocalState,
                            )
                        },
                        || self.frozen_sync_target_version(),
                    );
                    if result.is_err() {
                        remote_operation_recorded.set(true);
                    }
                    let target_version = result?;
                    remote_operation_recorded.set(false);
                    drain_to_target(target_version)?;
                    true
                }
                Err(error) => return Err(error),
            };
            Ok(changed)
        })();
        let result = if result.is_ok() && self.current_pg_sync_lifecycle() == PgSyncLifecycle::Fatal
        {
            Err(MetaError::LifecycleFatal)
        } else {
            result
        };
        if matches!(result, Err(MetaError::CatchUpStalled { .. })) {
            self.reset_startup_catch_up_after_stall();
        }
        // Fresh-client completion reacquires replication_lock, so record the
        // aggregate session result only after the page-drain lock is released.
        self.record_metadata_sync_result(
            MetadataSyncSource::FallbackPoll,
            &result,
            remote_operation_recorded.get(),
        );
        result
    }

    /// Clear the stalled startup target and leave the engine degraded so a
    /// later poll can capture a fresh target and retry recovery.
    pub(super) fn reset_startup_catch_up_after_stall(&self) {
        self.metadata_startup_target_version
            .store(-1, Ordering::Release);
        self.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
    }

    pub(super) fn start_background_poll(
        engine: Weak<Self>,
        interval: Duration,
        shutdown: Arc<AtomicBool>,
        wakeup: Arc<(Mutex<bool>, Condvar)>,
    ) -> MetaResult<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("pg-meta-poll".into())
            .spawn(move || {
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    let (wakeup_lock, wakeup_cv) = &*wakeup;
                    let mut wakeup_guard = wakeup_lock
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let deadline = Instant::now() + interval;
                    while !*wakeup_guard {
                        if shutdown.load(Ordering::Acquire) {
                            return;
                        }
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining == Duration::ZERO {
                            break;
                        }
                        let nap = remaining.min(Duration::from_millis(250));
                        let wait_result = wakeup_cv.wait_timeout(wakeup_guard, nap);
                        wakeup_guard = match wait_result {
                            Ok((guard, _)) => guard,
                            Err(poisoned) => poisoned.into_inner().0,
                        };
                    }
                    *wakeup_guard = false;
                    drop(wakeup_guard);
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    let Some(engine) = engine.upgrade() else {
                        break;
                    };
                    let _ = engine.flush_due_pending();
                    let local_version_before = engine.local_version.load(Ordering::Acquire);
                    let sync_result = if matches!(
                        engine.current_pg_sync_lifecycle(),
                        PgSyncLifecycle::CatchingUp | PgSyncLifecycle::Degraded
                    ) {
                        engine.run_startup_catch_up_session(&shutdown)
                    } else {
                        engine.sync_metadata_with_source(MetadataSyncSource::FallbackPoll)
                    };
                    if let Ok(initial_changed) = sync_result {
                        if let Err(error) = engine.backfill_current_reference_ledger_if_needed() {
                            warn!(
                                error = %error,
                                volume_id = engine.volume_id,
                                "postgres snapshot reachability bootstrap did not complete"
                            );
                        }
                        let local_version_after = engine.local_version.load(Ordering::Acquire);
                        if initial_changed || local_version_after > local_version_before {
                            engine.metadata_invalidation_notify.notify_one();
                        }
                        if matches!(
                            engine.current_pg_sync_lifecycle(),
                            PgSyncLifecycle::Live | PgSyncLifecycle::TargetReached
                        ) && let Err(error) = engine.run_event_log_prune_once()
                        {
                            warn!(
                                error = %error,
                                volume_id = engine.volume_id,
                                "postgres background event-log pruning did not complete"
                            );
                        }
                    } else if let Err(error) = sync_result {
                        warn!(
                            error = %error,
                            volume_id = engine.volume_id,
                            "postgres background metadata synchronization did not complete"
                        );
                    }
                }
            })
            .map_err(|error| {
                MetaError::Storage(format!("failed to spawn pg-meta-poll thread: {error}"))
            })
    }
}

use super::{
    MetadataSyncSource, PgBackgroundWorker, PgBackgroundWorkerState, PgBackgroundWorkers,
    PgMetaEngine,
};
use crate::engine::{MetaError, MetaResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};
use tracing::warn;

impl PgBackgroundWorkers {
    pub(super) fn new() -> Self {
        Self {
            poll_shutdown: Arc::new(AtomicBool::new(false)),
            poll_wakeup: Arc::new((Mutex::new(false), Condvar::new())),
            lock_notify_cancel: tokio_util::sync::CancellationToken::new(),
            metadata_notify_cancel: tokio_util::sync::CancellationToken::new(),
            state: Mutex::new(PgBackgroundWorkerState {
                poll_handle: None,
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

    pub(super) fn signal_stop(&self) {
        self.lock_notify_cancel.cancel();
        self.metadata_notify_cancel.cancel();
        self.poll_shutdown.store(true, Ordering::Release);
        self.signal_poll_wakeup();
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
        PgBackgroundWorker::MetadataNotify => &mut state.metadata_notify_handle,
        PgBackgroundWorker::LockNotify => &mut state.lock_notify_handle,
        PgBackgroundWorker::LockRenew => &mut state.lock_renew_handle,
    }
}

fn worker_name(worker: PgBackgroundWorker) -> &'static str {
    match worker {
        PgBackgroundWorker::Poll => "poll",
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
            let worker = worker_name(worker);
            warn!(
                worker,
                "pg background thread did not stop before shutdown join timeout"
            );
            return Err(MetaError::Storage(format!(
                "pg {worker} background thread did not stop before shutdown join timeout"
            )));
        }
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }

    handle.join().map_err(|_| {
        let worker = worker_name(worker);
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
                    if let Ok(metadata_changed) =
                        engine.sync_metadata_with_source(MetadataSyncSource::FallbackPoll)
                    {
                        let _ = engine.backfill_current_reference_ledger();
                        if metadata_changed {
                            engine.metadata_invalidation_notify.notify_one();
                        }
                    }
                }
            })
            .map_err(|error| {
                MetaError::Storage(format!("failed to spawn pg-meta-poll thread: {error}"))
            })
    }
}

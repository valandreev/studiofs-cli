use super::{MetadataSyncSource, PgMetaEngine};
use crate::engine::{MetaError, MetaResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};
use tracing::warn;

impl PgMetaEngine {
    pub(super) fn default_prepare_for_close_join_timeout() -> Duration {
        Self::env_duration_ms("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", 2_000)
    }

    pub(super) fn join_background_thread_with_timeout(
        handle: std::thread::JoinHandle<()>,
        timeout: Duration,
        worker: &'static str,
    ) {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let _ = std::thread::Builder::new()
            .name(format!("pg-close-join-{worker}"))
            .spawn(move || {
                let _ = tx.send(handle.join());
            });

        match rx.recv_timeout(timeout) {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                warn!(worker, "pg background thread panicked during shutdown join");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                warn!(
                    worker,
                    timeout_ms = timeout.as_millis() as u64,
                    "pg background thread did not stop before shutdown join timeout"
                );
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                warn!(
                    worker,
                    "pg background thread join helper disconnected unexpectedly"
                );
            }
        }
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
                    let _ = engine.sync_metadata_with_source(MetadataSyncSource::FallbackPoll);
                }
            })
            .map_err(|error| {
                MetaError::Storage(format!("failed to spawn pg-meta-poll thread: {error}"))
            })
    }
}

use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use sysinfo::{MemoryRefreshKind, System};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const SAMPLE_TTL: Duration = Duration::from_secs(2);

fn sample_wait(elapsed: Duration) -> Duration {
    SAMPLE_INTERVAL.saturating_sub(elapsed)
}

fn available_memory_at(snapshot: &RwLock<Option<(Instant, u64)>>, now: Instant) -> Option<u64> {
    // Opens never wait for publication; contention uses the conservative fallback.
    let (sampled_at, available) = {
        let guard = snapshot.try_read().ok()?;
        (*guard)?
    };
    (now.saturating_duration_since(sampled_at) < SAMPLE_TTL && available > 0).then_some(available)
}

/// Owns the background system-memory sampler; callback reads never wait for sampling.
pub(super) struct SystemMemorySampler {
    snapshot: Arc<RwLock<Option<(Instant, u64)>>>,
    stop: mpsc::Sender<()>,
    worker: Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    fixed: bool,
}

impl SystemMemorySampler {
    /// Start an owned worker that samples host RAM outside the snapshot lock.
    pub(super) fn system() -> Self {
        let mut system = System::new();
        Self::start(move || {
            system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
            let available = system.available_memory();
            (available > 0).then_some(available)
        })
    }

    /// Run the supplied measurement on a worker until shutdown joins it.
    pub(super) fn start(mut sample: impl FnMut() -> Option<u64> + Send + 'static) -> Self {
        let snapshot = Arc::new(RwLock::new(None));
        let (stop, stopped) = mpsc::channel();
        let worker_snapshot = Arc::clone(&snapshot);
        let worker = thread::Builder::new()
            .name("studiofs-macfuse-memory".into())
            .spawn(move || {
                loop {
                    let sampled_at = Instant::now();
                    let available = sample();
                    *worker_snapshot
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = available
                        .filter(|bytes| *bytes > 0)
                        .map(|bytes| (sampled_at, bytes));
                    if stopped.recv_timeout(sample_wait(sampled_at.elapsed()))
                        != Err(mpsc::RecvTimeoutError::Timeout)
                    {
                        break;
                    }
                }
            })
            .inspect_err(|error| tracing::warn!(%error, "macFUSE memory sampler could not start"))
            .ok();
        Self {
            snapshot,
            stop,
            worker: Mutex::new(worker),
            #[cfg(test)]
            fixed: false,
        }
    }

    /// Supply a constant test estimate without a worker or expiry.
    #[cfg(test)]
    pub(super) fn fixed(available: Option<u64>) -> Self {
        let (stop, _) = mpsc::channel();
        Self {
            snapshot: Arc::new(RwLock::new(
                available
                    .filter(|bytes| *bytes > 0)
                    .map(|bytes| (Instant::now(), bytes)),
            )),
            stop,
            worker: Mutex::new(None),
            fixed: true,
        }
    }

    /// Read without waiting; unusable production telemetry returns `None`.
    pub(super) fn available_memory(&self) -> Option<u64> {
        #[cfg(test)]
        if self.fixed {
            return self
                .snapshot
                .try_read()
                .ok()?
                .map(|(_, available)| available);
        }
        available_memory_at(&self.snapshot, Instant::now())
    }

    /// Stop sampling and join any in-flight measurement; safe to call repeatedly.
    pub(super) fn shutdown(&self) {
        let _ = self.stop.send(());
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(worker) = worker.take()
            && worker.join().is_err()
        {
            tracing::warn!("macFUSE memory sampler panicked");
        }
    }
}

impl Drop for SystemMemorySampler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SAMPLE_INTERVAL, SAMPLE_TTL, SystemMemorySampler, available_memory_at, sample_wait,
    };
    use std::sync::{Arc, RwLock, mpsc};
    use std::thread;
    use std::time::{Duration, Instant};

    struct SampleOwner(mpsc::Sender<()>);

    impl Drop for SampleOwner {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn snapshot_handles_missing_zero_and_stale_measurements() {
        let sampled_at = Instant::now();
        let snapshot = RwLock::new(None);
        assert_eq!(available_memory_at(&snapshot, sampled_at), None);

        *snapshot.write().unwrap() = Some((sampled_at, 0));
        assert_eq!(available_memory_at(&snapshot, sampled_at), None);

        *snapshot.write().unwrap() = Some((sampled_at, 4096));
        assert_eq!(
            available_memory_at(
                &snapshot,
                sampled_at + SAMPLE_TTL - Duration::from_millis(1)
            ),
            Some(4096)
        );
        assert_eq!(
            available_memory_at(&snapshot, sampled_at + SAMPLE_TTL),
            None
        );

        *snapshot.write().unwrap() = None;
        assert_eq!(
            available_memory_at(&snapshot, sampled_at + SAMPLE_TTL),
            None
        );
    }

    #[test]
    fn reader_does_not_wait_for_snapshot_publication() {
        let snapshot = Arc::new(RwLock::new(Some((Instant::now(), 4096))));
        let publishing = snapshot.write().unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let reader_snapshot = Arc::clone(&snapshot);
        let reader = thread::spawn(move || {
            done_tx
                .send(available_memory_at(&reader_snapshot, Instant::now()))
                .unwrap();
        });
        let result = done_rx.recv_timeout(Duration::from_secs(2));
        drop(publishing);
        reader.join().unwrap();
        assert_eq!(
            result.expect("reader must finish before publication unlocks"),
            None
        );
    }

    #[test]
    fn readers_finish_while_sample_is_blocked_and_shutdown_joins_worker() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let (exited_tx, exited_rx) = mpsc::channel();
        let (owner_dropped_tx, owner_dropped_rx) = mpsc::channel();
        let owner = SampleOwner(owner_dropped_tx);
        let mut first_resume = Some(resume_rx);
        let sampler = SystemMemorySampler::start(move || {
            let _ = &owner;
            if let Some(resume_rx) = first_resume.take() {
                entered_tx.send(()).unwrap();
                let _ = resume_rx.recv_timeout(SAMPLE_INTERVAL * 5);
                exited_tx.send(()).unwrap();
            }
            Some(4096)
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let results = thread::scope(|scope| {
            let (done_tx, done_rx) = mpsc::channel();
            let readers: Vec<_> = (0..16)
                .map(|_| {
                    let done_tx = done_tx.clone();
                    let sampler = &sampler;
                    scope.spawn(move || done_tx.send(sampler.available_memory()).unwrap())
                })
                .collect();
            let results: Vec<_> = (0..16)
                .map(|_| done_rx.recv_timeout(Duration::from_secs(2)))
                .collect();
            resume_tx.send(()).unwrap();
            drop(resume_tx);
            for reader in readers {
                reader.join().unwrap();
            }
            results
        });

        sampler.shutdown();
        assert!(results.iter().all(|result| matches!(result, Ok(None))));
        exited_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(owner_dropped_rx.try_recv(), Ok(()));
    }

    #[test]
    fn dropping_sampler_joins_worker_and_drops_sampling_closure() {
        let (owner_dropped_tx, owner_dropped_rx) = mpsc::channel();
        let owner = SampleOwner(owner_dropped_tx);
        let sampler = SystemMemorySampler::start(move || {
            let _ = &owner;
            Some(4096)
        });

        drop(sampler);

        assert_eq!(owner_dropped_rx.try_recv(), Ok(()));
    }

    #[test]
    fn snapshot_remains_fresh_between_scheduled_samples() {
        let sampled_at = Instant::now();
        let snapshot = RwLock::new(Some((sampled_at, 4096)));
        assert_eq!(
            available_memory_at(
                &snapshot,
                sampled_at + SAMPLE_INTERVAL + Duration::from_millis(20)
            ),
            Some(4096)
        );
    }

    #[test]
    fn sample_wait_uses_only_the_remaining_interval() {
        assert_eq!(
            sample_wait(Duration::from_millis(300)),
            Duration::from_millis(700)
        );
        assert_eq!(sample_wait(SAMPLE_INTERVAL), Duration::ZERO);
        assert_eq!(
            sample_wait(SAMPLE_INTERVAL + Duration::from_millis(500)),
            Duration::ZERO
        );
    }

    #[test]
    fn fixed_sampler_returns_snapshot_without_worker() {
        let sampler = SystemMemorySampler::fixed(Some(4096));

        *sampler.snapshot.write().unwrap() = Some((Instant::now() - SAMPLE_TTL, 4096));
        assert_eq!(sampler.available_memory(), Some(4096));
        assert!(sampler.worker.lock().unwrap().is_none());
    }
}

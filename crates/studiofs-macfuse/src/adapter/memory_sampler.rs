use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use sysinfo::{MemoryRefreshKind, System};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const SAMPLE_TTL: Duration = Duration::from_secs(2);

#[derive(Default)]
struct MemorySnapshot {
    version: AtomicU64,
    available_bytes: AtomicU64,
    sampled_at_millis: AtomicU64,
}

impl MemorySnapshot {
    fn publish(&self, available: Option<u64>, sampled_at: Duration) {
        // One sampler publishes; each open makes one bounded read and rejects overlap.
        self.version.fetch_add(1, Ordering::SeqCst);
        self.available_bytes.store(
            available.filter(|value| *value > 0).unwrap_or(0),
            Ordering::SeqCst,
        );
        self.sampled_at_millis.store(
            duration_millis(sampled_at).saturating_add(1),
            Ordering::SeqCst,
        );
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    fn available_memory_at(&self, now: Duration) -> Option<u64> {
        let version = self.version.load(Ordering::SeqCst);
        if !version.is_multiple_of(2) {
            return None;
        }

        let available = self.available_bytes.load(Ordering::SeqCst);
        let sampled_at = self.sampled_at_millis.load(Ordering::SeqCst);
        if self.version.load(Ordering::SeqCst) != version || sampled_at == 0 {
            return None;
        }

        if duration_millis(now).saturating_sub(sampled_at - 1) >= duration_millis(SAMPLE_TTL) {
            return None;
        }
        (available != 0).then_some(available)
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX - 1)
}

/// Owns the background system-memory sampler; callback reads never wait for sampling.
pub(super) struct SystemMemorySampler {
    started_at: Instant,
    snapshot: Arc<MemorySnapshot>,
    stop: mpsc::Sender<()>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl SystemMemorySampler {
    pub(super) fn system() -> Self {
        let mut system = System::new();
        Self::start(move || {
            system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
            let available = system.available_memory();
            (available > 0).then_some(available)
        })
    }

    pub(super) fn start(mut sample: impl FnMut() -> Option<u64> + Send + 'static) -> Self {
        let started_at = Instant::now();
        let snapshot = Arc::new(MemorySnapshot::default());
        let (stop, stopped) = mpsc::channel();
        let worker_snapshot = Arc::clone(&snapshot);
        let worker = thread::Builder::new()
            .name("studiofs-macfuse-memory".into())
            .spawn(move || {
                loop {
                    let sampled_at = started_at.elapsed();
                    let available = sample();
                    worker_snapshot.publish(available, sampled_at);
                    match stopped.recv_timeout(SAMPLE_INTERVAL) {
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .inspect_err(|error| tracing::warn!(%error, "macFUSE memory sampler could not start"))
            .ok();
        Self {
            started_at,
            snapshot,
            stop,
            worker: Mutex::new(worker),
        }
    }

    pub(super) fn available_memory(&self) -> Option<u64> {
        self.snapshot.available_memory_at(self.started_at.elapsed())
    }

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
    use super::{MemorySnapshot, SAMPLE_INTERVAL, SAMPLE_TTL, SystemMemorySampler};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn snapshot_handles_missing_zero_and_stale_measurements() {
        let snapshot = MemorySnapshot::default();
        assert_eq!(snapshot.available_memory_at(Duration::ZERO), None);

        snapshot.publish(Some(0), Duration::ZERO);
        assert_eq!(snapshot.available_memory_at(Duration::ZERO), None);

        snapshot.publish(Some(4096), Duration::ZERO);
        assert_eq!(
            snapshot.available_memory_at(SAMPLE_TTL - Duration::from_millis(1)),
            Some(4096)
        );
        assert_eq!(snapshot.available_memory_at(SAMPLE_TTL), None);

        snapshot.publish(None, SAMPLE_TTL);
        assert_eq!(snapshot.available_memory_at(SAMPLE_TTL), None);
    }

    #[test]
    fn reader_rejects_snapshot_while_it_is_being_published() {
        let snapshot = MemorySnapshot::default();
        snapshot
            .version
            .store(1, std::sync::atomic::Ordering::SeqCst);
        snapshot
            .available_bytes
            .store(4096, std::sync::atomic::Ordering::SeqCst);
        snapshot
            .sampled_at_millis
            .store(1, std::sync::atomic::Ordering::SeqCst);

        assert_eq!(snapshot.available_memory_at(Duration::ZERO), None);
    }

    #[test]
    fn readers_finish_while_sample_is_blocked_and_shutdown_joins_worker() {
        struct SampleOwner(mpsc::Sender<()>);
        impl Drop for SampleOwner {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }

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
        struct SampleOwner(mpsc::Sender<()>);
        impl Drop for SampleOwner {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }

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
        let snapshot = MemorySnapshot::default();
        snapshot.publish(Some(4096), Duration::ZERO);

        assert_eq!(
            snapshot.available_memory_at(SAMPLE_INTERVAL + Duration::from_millis(20)),
            Some(4096)
        );
    }
}

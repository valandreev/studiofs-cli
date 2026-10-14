//! Background host-memory telemetry for macFUSE open cache decisions.
//!
//! The Linux adapter owns an equivalent sampler in
//! `studiofs-linux-fuse/src/cache_policy.rs`; keep pacing, expiry, and
//! publication semantics aligned when changing either one.

use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::low_level::catch_callback_unwind;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
/// Two intervals, so one late or slow sample keeps the previous measurement
/// usable while an older one still fails safe.
const SAMPLE_TTL: Duration = Duration::from_secs(2);

fn sample_wait(elapsed: Duration) -> Duration {
    SAMPLE_INTERVAL.saturating_sub(elapsed)
}

/// Page counts the kernel can hand to file data without compressing or
/// swapping anonymous memory.
#[derive(Clone, Copy, Debug)]
struct ReclaimablePages {
    free: u64,
    speculative: u64,
    inactive: u64,
}

/// Bytes available for kernel file caching.
///
/// `sysinfo` subtracts compressor pages from this sum and saturates to zero on
/// ordinary Macs with an active compressor, which would disable kernel caching
/// for every file. Compressed anonymous memory does not occupy the reclaimable
/// queues, so it is not subtracted here. Purgeable pages are excluded because
/// they are also counted in the active/inactive queues.
fn reclaimable_bytes(pages: ReclaimablePages, page_size: u64) -> u64 {
    pages
        .free
        .saturating_add(pages.speculative)
        .saturating_add(pages.inactive)
        .saturating_mul(page_size)
}

/// Host VM statistics through `host_statistics64`, reusing one host port.
struct HostMemory {
    host: libc::mach_port_t,
    page_size: u64,
}

impl HostMemory {
    fn new() -> Option<Self> {
        // SAFETY: `mach_host_self` takes no arguments and returns this task's
        // host name port; the sampler keeps it for its lifetime like `sysinfo`.
        #[expect(
            deprecated,
            reason = "libc defers Mach host APIs to mach2; one host-port lookup does not justify a new dependency"
        )]
        let host = unsafe { libc::mach_host_self() };
        // SAFETY: `sysconf` reads a process-wide constant and has no pointer arguments.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page_size = u64::try_from(page_size).ok().filter(|size| *size > 0)?;
        // `MACH_PORT_NULL` is declared as `i32` while ports are `u32`.
        (host != libc::MACH_PORT_NULL as libc::mach_port_t).then_some(Self { host, page_size })
    }

    fn sample(&self) -> Option<u64> {
        let mut stats = MaybeUninit::<libc::vm_statistics64>::zeroed();
        let mut count = libc::HOST_VM_INFO64_COUNT;
        // SAFETY: `stats` is a writable `vm_statistics64` and `count` is its
        // size in `integer_t` words; the kernel writes at most `count` words.
        let status = unsafe {
            libc::host_statistics64(
                self.host,
                libc::HOST_VM_INFO64,
                stats.as_mut_ptr().cast(),
                &mut count,
            )
        };
        if status != libc::KERN_SUCCESS {
            return None;
        }
        // SAFETY: the buffer was zero-initialized, every field is an integer,
        // and a successful call wrote a valid prefix.
        let stats = unsafe { stats.assume_init() };
        Some(reclaimable_bytes(
            ReclaimablePages {
                free: u64::from(stats.free_count),
                speculative: u64::from(stats.speculative_count),
                inactive: u64::from(stats.inactive_count),
            },
            self.page_size,
        ))
    }
}

/// Lock-free published measurement.
///
/// The writer stores bytes before the timestamp with `Release`; a reader loads
/// the timestamp with `Acquire` first. Seeing a timestamp therefore implies
/// seeing its bytes or newer ones, and newer bytes behind an older timestamp
/// only expire sooner. Readers never wait for the worker.
struct Snapshot {
    epoch: Instant,
    available_bytes: AtomicU64,
    /// Nanoseconds since `epoch` plus one; zero means never published.
    sampled_at: AtomicU64,
}

impl Snapshot {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
            available_bytes: AtomicU64::new(0),
            sampled_at: AtomicU64::new(0),
        }
    }

    fn nanos_since_epoch(&self, at: Instant) -> u64 {
        u64::try_from(at.saturating_duration_since(self.epoch).as_nanos()).unwrap_or(u64::MAX - 1)
    }

    fn publish(&self, sampled_at: Instant, available: Option<u64>) {
        self.available_bytes
            .store(available.unwrap_or(0), Ordering::Relaxed);
        self.sampled_at.store(
            self.nanos_since_epoch(sampled_at).saturating_add(1),
            Ordering::Release,
        );
    }

    /// Missing, zero, and expired measurements are all unusable.
    fn available_at(&self, now: Instant) -> Option<u64> {
        let sampled_at = self.sampled_at.load(Ordering::Acquire).checked_sub(1)?;
        let age = Duration::from_nanos(self.nanos_since_epoch(now).saturating_sub(sampled_at));
        if age >= SAMPLE_TTL {
            return None;
        }
        let available = self.available_bytes.load(Ordering::Relaxed);
        (available > 0).then_some(available)
    }
}

/// Log only health transitions so a failing source cannot flood the log.
#[derive(Default)]
struct SampleHealth {
    failing: bool,
}

impl SampleHealth {
    fn sample(&mut self, sample: &mut impl FnMut() -> Option<u64>) -> Option<u64> {
        let mut available = None;
        let panicked = catch_callback_unwind(|| available = sample());
        let available = available.filter(|bytes| *bytes > 0);
        match (available.is_some(), self.failing) {
            (false, false) => {
                tracing::warn!(
                    panicked,
                    "macFUSE memory sample unavailable; read-only opens use direct I/O"
                );
                self.failing = true;
            }
            (true, true) => {
                tracing::info!("macFUSE memory sampling recovered");
                self.failing = false;
            }
            _ => {}
        }
        available
    }
}

struct Worker {
    stop: mpsc::Sender<()>,
    thread: JoinHandle<()>,
}

enum Telemetry {
    Sampled {
        snapshot: Arc<Snapshot>,
        worker: Mutex<Option<Worker>>,
    },
    #[cfg(test)]
    Fixed(Option<u64>),
}

/// Owns background host-memory sampling; callback reads never wait for it.
pub(super) struct SystemMemorySampler {
    telemetry: Telemetry,
}

impl SystemMemorySampler {
    /// Sample host RAM once on the caller, then keep sampling on a worker.
    ///
    /// The first measurement is published before the mount serves callbacks,
    /// so early opens do not inherit a startup fallback for their lifetime.
    pub(super) fn system() -> Self {
        match HostMemory::new() {
            Some(host) => Self::started(move || host.sample()),
            None => {
                tracing::error!(
                    "macFUSE host memory statistics are unavailable; read-only opens use direct I/O"
                );
                Self::started(|| None)
            }
        }
    }

    /// Publish one synchronous measurement, then sample on the worker.
    pub(super) fn started(sample: impl FnMut() -> Option<u64> + Send + 'static) -> Self {
        Self::spawn(sample, true)
    }

    /// Run every measurement, including the first, on the worker.
    #[cfg(test)]
    pub(super) fn start(sample: impl FnMut() -> Option<u64> + Send + 'static) -> Self {
        Self::spawn(sample, false)
    }

    fn spawn(mut sample: impl FnMut() -> Option<u64> + Send + 'static, initial: bool) -> Self {
        let snapshot = Arc::new(Snapshot::new());
        let mut health = SampleHealth::default();
        let mut wait = Duration::ZERO;
        if initial {
            let sampled_at = Instant::now();
            snapshot.publish(sampled_at, health.sample(&mut sample));
            wait = sample_wait(sampled_at.elapsed());
        }
        let (stop, stopped) = mpsc::channel();
        let worker_snapshot = Arc::clone(&snapshot);
        let worker = thread::Builder::new()
            .name("studiofs-macfuse-memory".into())
            .spawn(move || {
                // Shutdown drops the sender, which also ends a pending wait.
                while stopped.recv_timeout(wait) == Err(mpsc::RecvTimeoutError::Timeout) {
                    let sampled_at = Instant::now();
                    worker_snapshot.publish(sampled_at, health.sample(&mut sample));
                    wait = sample_wait(sampled_at.elapsed());
                }
            })
            .inspect_err(|error| {
                tracing::error!(
                    %error,
                    "macFUSE memory sampler could not start; read-only opens use direct I/O"
                );
            })
            .ok()
            .map(|thread| Worker { stop, thread });
        Self {
            telemetry: Telemetry::Sampled {
                snapshot,
                worker: Mutex::new(worker),
            },
        }
    }

    /// Supply a constant test estimate without a worker or expiry.
    #[cfg(test)]
    pub(super) fn fixed(available: Option<u64>) -> Self {
        Self {
            telemetry: Telemetry::Fixed(available.filter(|bytes| *bytes > 0)),
        }
    }

    /// Read without waiting; unusable telemetry returns `None`.
    pub(super) fn available_memory(&self) -> Option<u64> {
        match &self.telemetry {
            Telemetry::Sampled { snapshot, .. } => snapshot.available_at(Instant::now()),
            #[cfg(test)]
            Telemetry::Fixed(available) => *available,
        }
    }

    /// Stop sampling and join any in-flight measurement; safe to call repeatedly.
    ///
    /// The join waits for one host statistics call at most, which does not
    /// block on filesystem or network state.
    pub(super) fn shutdown(&self) {
        let worker = match &self.telemetry {
            Telemetry::Sampled { worker, .. } => worker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
            #[cfg(test)]
            Telemetry::Fixed(_) => None,
        };
        if let Some(Worker { stop, thread }) = worker {
            drop(stop);
            if thread.join().is_err() {
                tracing::warn!("macFUSE memory sampler panicked");
            }
        }
    }

    #[cfg(test)]
    pub(super) fn is_running(&self) -> bool {
        match &self.telemetry {
            Telemetry::Sampled { worker, .. } => worker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some(),
            Telemetry::Fixed(_) => false,
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
        HostMemory, ReclaimablePages, SAMPLE_INTERVAL, SAMPLE_TTL, SampleHealth, Snapshot,
        SystemMemorySampler, reclaimable_bytes, sample_wait,
    };
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    struct SampleOwner(mpsc::Sender<()>);

    impl Drop for SampleOwner {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn snapshot_without_publication_is_unavailable() {
        let snapshot = Snapshot::new();

        assert_eq!(snapshot.available_at(Instant::now()), None);
    }

    #[test]
    fn snapshot_with_zero_measurement_is_unavailable() {
        let snapshot = Snapshot::new();
        let sampled_at = Instant::now();
        snapshot.publish(sampled_at, Some(0));

        assert_eq!(snapshot.available_at(sampled_at), None);
    }

    #[test]
    fn snapshot_with_failed_measurement_is_unavailable() {
        let snapshot = Snapshot::new();
        let sampled_at = Instant::now();
        snapshot.publish(sampled_at, Some(4096));
        snapshot.publish(sampled_at, None);

        assert_eq!(snapshot.available_at(sampled_at), None);
    }

    #[test]
    fn snapshot_is_fresh_until_ttl() {
        let snapshot = Snapshot::new();
        let sampled_at = Instant::now();
        snapshot.publish(sampled_at, Some(4096));

        assert_eq!(
            snapshot.available_at(sampled_at + SAMPLE_TTL - Duration::from_nanos(1)),
            Some(4096)
        );
    }

    #[test]
    fn snapshot_expires_at_ttl() {
        let snapshot = Snapshot::new();
        let sampled_at = Instant::now();
        snapshot.publish(sampled_at, Some(4096));

        assert_eq!(snapshot.available_at(sampled_at + SAMPLE_TTL), None);
    }

    #[test]
    fn snapshot_remains_fresh_between_scheduled_samples() {
        let snapshot = Snapshot::new();
        let sampled_at = Instant::now();
        snapshot.publish(sampled_at, Some(4096));

        assert_eq!(
            snapshot.available_at(sampled_at + SAMPLE_INTERVAL + Duration::from_millis(20)),
            Some(4096)
        );
    }

    #[test]
    fn reclaimable_bytes_ignore_compressor_occupancy() {
        // Counts captured from a 16 GiB Mac where `sysinfo` reported zero:
        // free 9412, inactive 246247, purgeable 15619, compressor 298047.
        let pages = ReclaimablePages {
            free: 9412,
            speculative: 1497,
            inactive: 246_247,
        };

        assert_eq!(
            reclaimable_bytes(pages, 16 * 1024),
            (9412 + 1497 + 246_247) * 16 * 1024
        );
    }

    #[test]
    fn reclaimable_bytes_saturate_instead_of_overflowing() {
        let pages = ReclaimablePages {
            free: u64::MAX,
            speculative: 1,
            inactive: 1,
        };

        assert_eq!(reclaimable_bytes(pages, 16 * 1024), u64::MAX);
    }

    #[test]
    #[ignore = "requires a macOS host with nonzero reclaimable memory"]
    fn host_memory_reports_nonzero_reclaimable_bytes() {
        let host = HostMemory::new().expect("host statistics port");

        assert!(host.sample().is_some_and(|bytes| bytes > 0));
    }

    #[test]
    fn sample_health_contains_panicking_measurement() {
        let mut health = SampleHealth::default();
        let mut calls = 0;
        let mut sample = || {
            calls += 1;
            if calls == 1 {
                panic!("injected memory sample panic");
            }
            Some(4096)
        };

        assert_eq!(health.sample(&mut sample), None);
        assert!(health.failing);
        assert_eq!(health.sample(&mut sample), Some(4096));
        assert!(!health.failing);
    }

    #[test]
    fn started_sampler_publishes_before_returning() {
        let sampler = SystemMemorySampler::started(|| Some(4096));

        assert_eq!(sampler.available_memory(), Some(4096));
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
        assert!(!sampler.is_running());
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
    fn fixed_sampler_returns_estimate_without_worker() {
        let sampler = SystemMemorySampler::fixed(Some(4096));

        assert_eq!(sampler.available_memory(), Some(4096));
        assert!(!sampler.is_running());
    }
}

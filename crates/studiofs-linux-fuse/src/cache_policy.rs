use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use studiofs_vfs::vfs::{OsCacheDisposition, OsCacheOpenDecision, OsCacheReason};

const MEMORY_SAMPLE_TTL: Duration = Duration::from_secs(1);
const MAX_SYNC_READ_BUFFERS: usize = 32;
const MAX_RETAINED_SYNC_READ_BUFFER_CAPACITY: usize = 8 * 1024 * 1024;

/// Linux FUSE operating-system cache policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LinuxFuseCachePolicy {
    Direct,
    Adaptive,
    Kernel,
}

impl LinuxFuseCachePolicy {
    /// Parse the configured policy, preserving whether a warning is required.
    pub(crate) fn parse(value: Option<&str>) -> CachePolicySelection {
        let Some(value) = value else {
            return CachePolicySelection::recognized(Self::Adaptive);
        };
        let policy = match value.trim().to_ascii_lowercase().as_str() {
            "" | "adaptive" | "auto" => Self::Adaptive,
            "direct" | "direct_io" | "dio" | "0" | "false" | "no" | "off" => Self::Direct,
            "kernel" | "cached" | "page_cache" | "1" | "true" | "yes" | "on" => Self::Kernel,
            _ => return CachePolicySelection::unrecognized_default(),
        };
        CachePolicySelection::recognized(policy)
    }
}

/// Parsed policy plus an indicator that the original value was unknown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CachePolicySelection {
    pub(crate) policy: LinuxFuseCachePolicy,
    pub(crate) warning_required: bool,
}

impl CachePolicySelection {
    const fn recognized(policy: LinuxFuseCachePolicy) -> Self {
        Self {
            policy,
            warning_required: false,
        }
    }

    const fn unrecognized_default() -> Self {
        Self {
            policy: LinuxFuseCachePolicy::Adaptive,
            warning_required: true,
        }
    }
}

/// Readahead component selected for one open handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadAheadIntent {
    Vfs,
    Kernel,
}

/// Cache-generation facts consumed by the Linux policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OpenCacheDecision {
    pub(crate) disposition: OsCacheDisposition,
    pub(crate) reason: OsCacheReason,
}

impl From<OsCacheOpenDecision> for OpenCacheDecision {
    fn from(value: OsCacheOpenDecision) -> Self {
        Self {
            disposition: value.disposition(),
            reason: value.reason(),
        }
    }
}

/// Linux FUSE flags and handle behavior selected for one open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LinuxOpenCacheDecision {
    pub(crate) direct_io: bool,
    pub(crate) keep_cache: bool,
    pub(crate) readahead: ReadAheadIntent,
    pub(crate) writeback_capable: bool,
    pub(crate) memory_bypassed: bool,
}

impl LinuxOpenCacheDecision {
    const fn direct(memory_bypassed: bool) -> Self {
        Self {
            direct_io: true,
            keep_cache: false,
            readahead: ReadAheadIntent::Vfs,
            writeback_capable: false,
            memory_bypassed,
        }
    }

    const fn kernel(keep_cache: bool, writeback_capable: bool) -> Self {
        Self {
            direct_io: false,
            keep_cache,
            readahead: ReadAheadIntent::Kernel,
            writeback_capable,
            memory_bypassed: false,
        }
    }
}

/// Select the Linux open behavior from policy, VFS generation safety, and memory.
pub(crate) fn decide_open_cache(
    policy: LinuxFuseCachePolicy,
    write_capable: bool,
    generation: impl Into<OpenCacheDecision>,
    file_size: u64,
    available_memory: Option<u64>,
) -> LinuxOpenCacheDecision {
    let generation = generation.into();
    let stable_generation = generation.disposition == OsCacheDisposition::Reuse
        && generation.reason == OsCacheReason::StableGeneration;

    match policy {
        LinuxFuseCachePolicy::Direct => LinuxOpenCacheDecision::direct(false),
        LinuxFuseCachePolicy::Adaptive if write_capable => LinuxOpenCacheDecision::direct(false),
        LinuxFuseCachePolicy::Adaptive
            if available_memory.is_some_and(|available| file_size > available) =>
        {
            LinuxOpenCacheDecision::direct(true)
        }
        LinuxFuseCachePolicy::Adaptive if available_memory.is_none() || !stable_generation => {
            // A new generation must be read through VFS before the kernel may
            // retain pages for reuse. This closes the race where an open
            // arrives before the asynchronous inode invalidation reaches the
            // kernel page cache.
            LinuxOpenCacheDecision::direct(false)
        }
        LinuxFuseCachePolicy::Adaptive => LinuxOpenCacheDecision::kernel(stable_generation, false),
        LinuxFuseCachePolicy::Kernel => {
            LinuxOpenCacheDecision::kernel(stable_generation && !write_capable, true)
        }
    }
}

#[derive(Default)]
struct MemorySnapshot {
    available_bytes: AtomicU64,
    sampled_at_millis: AtomicU64,
}

impl MemorySnapshot {
    fn publish(&self, available: Option<u64>, sampled_at: Duration) {
        self.available_bytes
            .store(available.unwrap_or(0), Ordering::Relaxed);
        self.sampled_at_millis.store(
            duration_millis(sampled_at).saturating_add(1),
            Ordering::Release,
        );
    }

    fn available_memory_at(&self, now: Duration) -> Option<u64> {
        let sampled_at = self.sampled_at_millis.load(Ordering::Acquire);
        if sampled_at == 0
            || duration_millis(now).saturating_sub(sampled_at - 1)
                >= duration_millis(MEMORY_SAMPLE_TTL)
        {
            return None;
        }
        normalize_available_memory(self.available_bytes.load(Ordering::Relaxed))
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX - 1)
}

fn normalize_available_memory(available_memory: u64) -> Option<u64> {
    (available_memory != 0).then_some(available_memory)
}

/// Owned background sampler; foreground readers only load the atomic snapshot.
pub(crate) struct SystemMemorySampler {
    started_at: Instant,
    snapshot: Arc<MemorySnapshot>,
    stop: mpsc::Sender<()>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl SystemMemorySampler {
    pub(crate) fn system() -> Self {
        let mut system = sysinfo::System::new();
        Self::start(move || {
            system.refresh_memory();
            normalize_available_memory(system.available_memory())
        })
    }

    pub(crate) fn start(mut sample: impl FnMut() -> Option<u64> + Send + 'static) -> Self {
        let started_at = Instant::now();
        let snapshot = Arc::new(MemorySnapshot::default());
        let (stop, stopped) = mpsc::channel();
        let worker_snapshot = Arc::clone(&snapshot);
        let worker = thread::Builder::new()
            .name("studiofs-fuse-memory".into())
            .spawn(move || {
                loop {
                    let sampled_at = started_at.elapsed();
                    let available = sample();
                    worker_snapshot.publish(available, sampled_at);
                    match stopped.recv_timeout(MEMORY_SAMPLE_TTL) {
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .inspect_err(
                |error| tracing::warn!(%error, "Linux FUSE memory sampler could not start"),
            )
            .ok();
        Self {
            started_at,
            snapshot,
            stop,
            worker: Mutex::new(worker),
        }
    }

    pub(crate) fn available_memory(&self) -> Option<u64> {
        self.snapshot.available_memory_at(self.started_at.elapsed())
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.stop.send(());
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(worker) = worker.take()
            && worker.join().is_err()
        {
            tracing::warn!("Linux FUSE memory sampler panicked");
        }
    }
}

impl Drop for SystemMemorySampler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Bounded reusable buffer pool for synchronous Linux reads.
#[derive(Default)]
pub(crate) struct SyncReadBufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
}

pub(crate) struct SyncReadBuffer {
    buffer: Vec<u8>,
    visible_len: usize,
}

impl SyncReadBuffer {
    pub(crate) fn truncate(&mut self, len: usize) {
        self.visible_len = self.visible_len.min(len);
    }
}

impl AsMut<[u8]> for SyncReadBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.visible_len]
    }
}

impl AsRef<[u8]> for SyncReadBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer[..self.visible_len]
    }
}

impl SyncReadBufferPool {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns initialized storage that may contain data from a previous read.
    /// Reply only with the prefix overwritten by a successful VFS read.
    pub(crate) fn take(&self, len: usize) -> SyncReadBuffer {
        let mut buffer = self
            .buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
            .unwrap_or_default();
        // Preserve initialized storage across short reads; only growth needs zero-fill.
        if buffer.len() < len {
            buffer.resize(len, 0);
        }
        SyncReadBuffer {
            buffer,
            visible_len: len,
        }
    }

    pub(crate) fn put(&self, buffer: SyncReadBuffer) {
        let buffer = buffer.buffer;
        if buffer.capacity() > MAX_RETAINED_SYNC_READ_BUFFER_CAPACITY {
            return;
        }
        let mut buffers = self
            .buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buffers.len() < MAX_SYNC_READ_BUFFERS {
            buffers.push(buffer);
        }
    }

    #[cfg(test)]
    fn retained_count(&self) -> usize {
        self.buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CachePolicySelection, LinuxFuseCachePolicy, MemorySnapshot, OpenCacheDecision,
        ReadAheadIntent, SyncReadBufferPool, SystemMemorySampler, decide_open_cache,
        normalize_available_memory,
    };
    use std::time::Duration;
    use studiofs_vfs::vfs::{OsCacheDisposition, OsCacheReason};

    fn generation(disposition: OsCacheDisposition, reason: OsCacheReason) -> OpenCacheDecision {
        OpenCacheDecision {
            disposition,
            reason,
        }
    }

    #[test]
    fn unset_empty_adaptive_and_auto_select_adaptive() {
        for value in [None, Some(""), Some(" adaptive "), Some("AUTO")] {
            assert_eq!(
                LinuxFuseCachePolicy::parse(value),
                CachePolicySelection::recognized(LinuxFuseCachePolicy::Adaptive)
            );
        }
    }

    #[test]
    fn direct_and_kernel_aliases_remain_compatible() {
        for value in ["direct", "direct_io", "dio", "0", "false", "no", "off"] {
            assert_eq!(
                LinuxFuseCachePolicy::parse(Some(value)),
                CachePolicySelection::recognized(LinuxFuseCachePolicy::Direct)
            );
        }
        for value in ["kernel", "cached", "page_cache", "1", "true", "yes", "on"] {
            assert_eq!(
                LinuxFuseCachePolicy::parse(Some(value)),
                CachePolicySelection::recognized(LinuxFuseCachePolicy::Kernel)
            );
        }
    }

    #[test]
    fn unknown_policy_defaults_to_adaptive_and_requests_warning() {
        assert_eq!(
            LinuxFuseCachePolicy::parse(Some("surprise")),
            CachePolicySelection::unrecognized_default()
        );
    }

    #[test]
    fn direct_policy_always_bypasses_kernel_cache() {
        let decision = decide_open_cache(
            LinuxFuseCachePolicy::Direct,
            false,
            generation(OsCacheDisposition::Reuse, OsCacheReason::StableGeneration),
            1,
            Some(1024),
        );

        assert!(decision.direct_io);
        assert!(!decision.keep_cache);
        assert_eq!(decision.readahead, ReadAheadIntent::Vfs);
        assert!(!decision.writeback_capable);
        assert!(!decision.memory_bypassed);
    }

    #[test]
    fn adaptive_unsafe_and_write_capable_opens_are_direct() {
        let unsafe_reasons = [
            OsCacheReason::Disabled,
            OsCacheReason::WriteCapable,
            OsCacheReason::UnknownState,
            OsCacheReason::UnknownHandle,
            OsCacheReason::NonRegular,
            OsCacheReason::LocalDirty,
            OsCacheReason::ActiveWriter,
        ];

        for reason in unsafe_reasons {
            let decision = decide_open_cache(
                LinuxFuseCachePolicy::Adaptive,
                false,
                generation(OsCacheDisposition::Replace, reason),
                1,
                Some(1024),
            );
            assert!(decision.direct_io, "{reason:?}");
            assert_eq!(decision.readahead, ReadAheadIntent::Vfs, "{reason:?}");
        }

        let write = decide_open_cache(
            LinuxFuseCachePolicy::Adaptive,
            true,
            generation(OsCacheDisposition::Reuse, OsCacheReason::StableGeneration),
            1,
            Some(1024),
        );
        assert!(write.direct_io);
        assert!(!write.writeback_capable);
    }

    #[test]
    fn adaptive_clean_generation_uses_vfs_until_generation_is_stable() {
        let first = decide_open_cache(
            LinuxFuseCachePolicy::Adaptive,
            false,
            generation(OsCacheDisposition::Replace, OsCacheReason::FirstObservation),
            64,
            Some(1024),
        );
        assert!(first.direct_io);
        assert!(!first.keep_cache);
        assert_eq!(first.readahead, ReadAheadIntent::Vfs);

        let changed = decide_open_cache(
            LinuxFuseCachePolicy::Adaptive,
            false,
            generation(
                OsCacheDisposition::Replace,
                OsCacheReason::GenerationChanged,
            ),
            64,
            Some(1024),
        );
        assert!(changed.direct_io);
        assert!(!changed.keep_cache);
        assert_eq!(changed.readahead, ReadAheadIntent::Vfs);

        let stable = decide_open_cache(
            LinuxFuseCachePolicy::Adaptive,
            false,
            generation(OsCacheDisposition::Reuse, OsCacheReason::StableGeneration),
            64,
            Some(1024),
        );
        assert!(stable.keep_cache);
        assert!(!stable.writeback_capable);
    }

    #[test]
    fn adaptive_file_larger_than_available_memory_is_direct() {
        let decision = decide_open_cache(
            LinuxFuseCachePolicy::Adaptive,
            false,
            generation(OsCacheDisposition::Reuse, OsCacheReason::StableGeneration),
            1025,
            Some(1024),
        );

        assert!(decision.direct_io);
        assert!(decision.memory_bypassed);
        assert_eq!(decision.readahead, ReadAheadIntent::Vfs);
    }

    #[test]
    fn adaptive_unavailable_memory_uses_direct_io_for_stable_generation() {
        let decision = decide_open_cache(
            LinuxFuseCachePolicy::Adaptive,
            false,
            generation(OsCacheDisposition::Reuse, OsCacheReason::StableGeneration),
            4096,
            None,
        );
        assert!(decision.direct_io);
        assert!(!decision.keep_cache);
    }

    #[test]
    fn adaptive_memory_measurement_failure_uses_vfs_for_first_read() {
        let decision = decide_open_cache(
            LinuxFuseCachePolicy::Adaptive,
            false,
            generation(OsCacheDisposition::Replace, OsCacheReason::FirstObservation),
            u64::MAX,
            None,
        );

        assert!(decision.direct_io);
        assert!(!decision.keep_cache);
        assert_eq!(decision.readahead, ReadAheadIntent::Vfs);
        assert!(!decision.memory_bypassed);
    }

    #[test]
    fn kernel_policy_uses_page_cache_and_enables_writeback_capability() {
        let unsafe_open = decide_open_cache(
            LinuxFuseCachePolicy::Kernel,
            true,
            generation(OsCacheDisposition::Replace, OsCacheReason::ActiveWriter),
            1,
            Some(0),
        );
        assert!(!unsafe_open.direct_io);
        assert!(!unsafe_open.keep_cache);
        assert_eq!(unsafe_open.readahead, ReadAheadIntent::Kernel);
        assert!(unsafe_open.writeback_capable);

        let stable = decide_open_cache(
            LinuxFuseCachePolicy::Kernel,
            false,
            generation(OsCacheDisposition::Reuse, OsCacheReason::StableGeneration),
            1,
            Some(0),
        );
        assert!(stable.keep_cache);
    }

    #[test]
    fn kernel_write_capable_open_never_keeps_cached_pages() {
        let decision = decide_open_cache(
            LinuxFuseCachePolicy::Kernel,
            true,
            generation(OsCacheDisposition::Reuse, OsCacheReason::StableGeneration),
            1,
            Some(1024),
        );

        assert!(!decision.direct_io);
        assert!(!decision.keep_cache);
        assert!(decision.writeback_capable);
    }

    #[test]
    fn memory_snapshot_expires_after_one_second_and_recovers() {
        let snapshot = MemorySnapshot::default();
        assert_eq!(snapshot.available_memory_at(Duration::ZERO), None);
        snapshot.publish(Some(4096), Duration::ZERO);
        assert_eq!(
            snapshot.available_memory_at(Duration::from_millis(999)),
            Some(4096)
        );
        assert_eq!(snapshot.available_memory_at(Duration::from_secs(1)), None);
        snapshot.publish(None, Duration::from_secs(1));
        assert_eq!(snapshot.available_memory_at(Duration::from_secs(1)), None);
        snapshot.publish(Some(8192), Duration::from_secs(2));
        assert_eq!(
            snapshot.available_memory_at(Duration::from_secs(2)),
            Some(8192)
        );
    }

    #[test]
    fn memory_snapshot_does_not_treat_slow_measurement_as_fresh() {
        let snapshot = MemorySnapshot::default();
        snapshot.publish(Some(4096), Duration::ZERO);
        assert_eq!(snapshot.available_memory_at(Duration::from_secs(2)), None);
    }

    #[test]
    fn memory_readers_finish_while_background_sample_is_blocked_and_drop_joins() {
        use std::sync::mpsc;
        use std::thread;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let (exited_tx, exited_rx) = mpsc::channel();
        let sampler = SystemMemorySampler::start(move || {
            entered_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            exited_tx.send(()).unwrap();
            Some(4096)
        });
        let snapshot = std::sync::Arc::downgrade(&sampler.snapshot);
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let results = thread::scope(|scope| {
            let (done_tx, done_rx) = mpsc::channel();
            let sampler = &sampler;
            let readers: Vec<_> = (0..16)
                .map(|_| {
                    let done_tx = done_tx.clone();
                    scope.spawn(move || done_tx.send(sampler.available_memory()).unwrap())
                })
                .collect();
            let results: Vec<_> = (0..16)
                .map(|_| done_rx.recv_timeout(Duration::from_secs(2)))
                .collect();
            resume_tx.send(()).unwrap();
            for reader in readers {
                reader.join().unwrap();
            }
            results
        });
        drop(sampler);
        assert!(snapshot.upgrade().is_none(), "drop must join the worker");
        exited_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(results.iter().all(|result| matches!(result, Ok(None))));
    }

    #[test]
    fn zero_available_memory_is_treated_as_measurement_failure() {
        assert_eq!(normalize_available_memory(0), None);
        assert_eq!(normalize_available_memory(1), Some(1));
    }

    #[test]
    fn sync_read_buffer_pool_reuses_buffers_and_is_bounded() {
        let pool = SyncReadBufferPool::new();
        let buffer = pool.take(4096);
        let allocation = buffer.as_ref().as_ptr();
        assert_eq!(buffer.as_ref().len(), 4096);
        pool.put(buffer);

        let buffer = pool.take(1024);
        assert_eq!(buffer.as_ref().as_ptr(), allocation);
        assert_eq!(buffer.as_ref().len(), 1024);
        pool.put(buffer);

        let buffers: Vec<_> = (0..40).map(|_| pool.take(64)).collect();
        for buffer in buffers {
            pool.put(buffer);
        }
        assert_eq!(pool.retained_count(), 32);
    }

    #[test]
    fn sync_read_buffer_pool_retains_storage_after_short_reads_and_eof() {
        let pool = SyncReadBufferPool::new();
        let mut buffer = pool.take(8);
        buffer.as_mut().copy_from_slice(b"abcdefgh");
        let allocation = buffer.as_ref().as_ptr();
        buffer.truncate(3);
        assert_eq!(buffer.as_ref(), b"abc");
        pool.put(buffer);

        let mut buffer = pool.take(2);
        assert_eq!(buffer.as_ref(), b"ab");
        assert_eq!(buffer.as_mut().len(), 2);
        buffer.truncate(0);
        assert!(buffer.as_ref().is_empty());
        pool.put(buffer);

        let buffer = pool.take(8);
        assert_eq!(buffer.as_ref().as_ptr(), allocation);
        assert_eq!(buffer.as_ref(), b"abcdefgh");
        pool.put(buffer);

        let buffer = pool.take(10);
        assert_eq!(buffer.as_ref(), b"abcdefgh\0\0");
    }

    #[test]
    fn sync_read_buffer_pool_discards_oversized_capacity() {
        let pool = SyncReadBufferPool::new();
        pool.put(pool.take(8 * 1024 * 1024 + 1));

        assert_eq!(pool.retained_count(), 0);
    }
}

use std::sync::Mutex;
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
        LinuxFuseCachePolicy::Adaptive if !stable_generation => {
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

pub(crate) trait AvailableMemorySource {
    fn sample(&mut self) -> Option<u64>;
}

impl<F> AvailableMemorySource for F
where
    F: FnMut() -> Option<u64>,
{
    fn sample(&mut self) -> Option<u64> {
        self()
    }
}

pub(crate) trait MonotonicClock {
    fn now(&self) -> Duration;
}

impl<F> MonotonicClock for F
where
    F: Fn() -> Duration,
{
    fn now(&self) -> Duration {
        self()
    }
}

#[derive(Clone, Copy, Debug)]
struct CachedMemoryMeasurement {
    sampled_at: Duration,
    available_memory: Option<u64>,
}

/// One-second cached available-memory sampler.
pub(crate) struct MemorySampler<S, C> {
    source: S,
    clock: C,
    cached: Option<CachedMemoryMeasurement>,
}

impl<S, C> MemorySampler<S, C>
where
    S: AvailableMemorySource,
    C: MonotonicClock,
{
    pub(crate) fn new(source: S, clock: C) -> Self {
        Self {
            source,
            clock,
            cached: None,
        }
    }

    pub(crate) fn available_memory(&mut self) -> Option<u64> {
        let now = self.clock.now();
        if let Some(cached) = self.cached
            && now.saturating_sub(cached.sampled_at) < MEMORY_SAMPLE_TTL
        {
            return cached.available_memory;
        }

        let available_memory = self.source.sample();
        self.cached = Some(CachedMemoryMeasurement {
            sampled_at: now,
            available_memory,
        });
        available_memory
    }
}

struct SystemAvailableMemory {
    system: sysinfo::System,
}

impl SystemAvailableMemory {
    fn new() -> Self {
        Self {
            system: sysinfo::System::new(),
        }
    }
}

impl AvailableMemorySource for SystemAvailableMemory {
    fn sample(&mut self) -> Option<u64> {
        self.system.refresh_memory();
        normalize_available_memory(self.system.available_memory())
    }
}

fn normalize_available_memory(available_memory: u64) -> Option<u64> {
    (available_memory != 0).then_some(available_memory)
}

struct SystemClock {
    started_at: Instant,
}

impl SystemClock {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }
}

impl MonotonicClock for SystemClock {
    fn now(&self) -> Duration {
        self.started_at.elapsed()
    }
}

/// Production available-memory sampler with an implementation-independent API.
pub(crate) struct SystemMemorySampler {
    sampler: MemorySampler<SystemAvailableMemory, SystemClock>,
}

impl SystemMemorySampler {
    pub(crate) fn system() -> Self {
        Self {
            sampler: MemorySampler::new(SystemAvailableMemory::new(), SystemClock::new()),
        }
    }

    pub(crate) fn available_memory(&mut self) -> Option<u64> {
        self.sampler.available_memory()
    }
}

/// Bounded reusable buffer pool for synchronous Linux reads.
#[derive(Default)]
pub(crate) struct SyncReadBufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
}

impl SyncReadBufferPool {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn take(&self, len: usize) -> Vec<u8> {
        let mut buffer = self
            .buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop()
            .unwrap_or_default();
        buffer.resize(len, 0);
        buffer
    }

    pub(crate) fn put(&self, mut buffer: Vec<u8>) {
        if buffer.capacity() > MAX_RETAINED_SYNC_READ_BUFFER_CAPACITY {
            return;
        }
        buffer.clear();
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
        CachePolicySelection, LinuxFuseCachePolicy, MemorySampler, OpenCacheDecision,
        ReadAheadIntent, SyncReadBufferPool, decide_open_cache, normalize_available_memory,
    };
    use std::cell::Cell;
    use std::rc::Rc;
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
    fn memory_sampler_caches_measurement_for_one_second() {
        let now = Rc::new(Cell::new(Duration::ZERO));
        let samples = Rc::new(Cell::new(0));
        let mut sampler = MemorySampler::new(
            {
                let samples = Rc::clone(&samples);
                move || {
                    samples.set(samples.get() + 1);
                    Some(4096)
                }
            },
            {
                let now = Rc::clone(&now);
                move || now.get()
            },
        );

        assert_eq!(sampler.available_memory(), Some(4096));
        assert_eq!(sampler.available_memory(), Some(4096));
        assert_eq!(samples.get(), 1);

        now.set(Duration::from_millis(999));
        assert_eq!(sampler.available_memory(), Some(4096));
        assert_eq!(samples.get(), 1);

        now.set(Duration::from_secs(1));
        assert_eq!(sampler.available_memory(), Some(4096));
        assert_eq!(samples.get(), 2);
    }

    #[test]
    fn memory_sampler_caches_measurement_failure() {
        let samples = Rc::new(Cell::new(0));
        let mut sampler = MemorySampler::new(
            {
                let samples = Rc::clone(&samples);
                move || {
                    samples.set(samples.get() + 1);
                    None
                }
            },
            || Duration::ZERO,
        );

        assert_eq!(sampler.available_memory(), None);
        assert_eq!(sampler.available_memory(), None);
        assert_eq!(samples.get(), 1);
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
        let allocation = buffer.as_ptr();
        assert_eq!(buffer.len(), 4096);
        pool.put(buffer);

        let buffer = pool.take(1024);
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(buffer.len(), 1024);
        pool.put(buffer);

        for _ in 0..40 {
            pool.put(Vec::with_capacity(64));
        }
        assert_eq!(pool.retained_count(), 32);
    }

    #[test]
    fn sync_read_buffer_pool_discards_oversized_capacity() {
        let pool = SyncReadBufferPool::new();
        pool.put(Vec::with_capacity(8 * 1024 * 1024 + 1));

        assert_eq!(pool.retained_count(), 0);
    }
}

//! Linux native page-cache and coherency policy.

use std::sync::atomic::{AtomicU64, Ordering};

/// Kernel page-cache action selected for an open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeOpenCacheAction {
    /// Reuse existing kernel pages for the current content generation.
    KeepCache,
    /// Let the kernel use normal cached I/O without promising old pages are valid.
    Cached,
    /// Invalidate stale pages before using normal cached I/O.
    InvalidateThenCached,
    /// Bypass kernel page cache for this handle.
    DirectIo,
}

/// Human-readable reason for a native open cache decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeOpenCacheReason {
    /// Content state matched the last trusted kernel-cache generation.
    Keep,
    /// No previously trusted kernel-cache generation is available.
    Cached,
    /// Content epoch, size, or mtime changed.
    StalePrevented,
    /// Dirty or active writer state makes kernel-page reuse unsafe.
    WriterBarrier,
    /// Local payload cache was evicted.
    LocalCacheEvicted,
}

/// Result of native Linux page-cache policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeOpenCacheDecision {
    /// Selected kernel-cache action.
    pub action: NativeOpenCacheAction,
    /// Observable reason for metrics and logs.
    pub reason: NativeOpenCacheReason,
}

/// Per-inode state used to decide whether kernel pages are still coherent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeInodeCacheState {
    /// Current StudioFS content epoch.
    pub content_epoch: u64,
    /// Last epoch trusted by the kernel page cache, if any.
    pub cached_content_epoch: Option<u64>,
    /// Current inode size.
    pub size: u64,
    /// Last size trusted by the kernel page cache, if any.
    pub cached_size: Option<u64>,
    /// Current mtime in nanoseconds.
    pub mtime_ns: u64,
    /// Last mtime trusted by the kernel page cache, if any.
    pub cached_mtime_ns: Option<u64>,
    /// Current StudioFS invalidation generation.
    pub invalidation_generation: u64,
    /// Last invalidation generation observed by the kernel-cache policy.
    pub observed_invalidation_generation: u64,
    /// Whether a writer is active for this inode.
    pub writer_active: bool,
    /// Whether local writer/cache state is dirty.
    pub dirty: bool,
    /// Whether the local payload cache can satisfy this generation.
    pub local_materialized: bool,
}

/// Monotonic counters for Linux native open cache decision reasons.
#[derive(Debug, Default)]
pub struct NativeOpenCacheMetrics {
    keep: AtomicU64,
    cached: AtomicU64,
    invalidated: AtomicU64,
    direct: AtomicU64,
    stale_prevented: AtomicU64,
    writer_barrier: AtomicU64,
    local_cache_evicted: AtomicU64,
}

/// Snapshot of native open cache decision counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeOpenCacheMetricsSnapshot {
    /// Open decisions that requested `KEEP_CACHE`.
    pub keep: u64,
    /// Open decisions that used normal cached I/O.
    pub cached: u64,
    /// Open decisions that required explicit invalidation first.
    pub invalidated: u64,
    /// Open decisions that requested direct I/O.
    pub direct: u64,
    /// Stale page-cache reuse prevented by content metadata.
    pub stale_prevented: u64,
    /// Decisions blocked by active writer or dirty state.
    pub writer_barrier: u64,
    /// Decisions caused by local payload cache eviction.
    pub local_cache_evicted: u64,
}

impl NativeOpenCacheMetrics {
    /// Records one open cache decision.
    pub fn record(&self, decision: NativeOpenCacheDecision) {
        match decision.action {
            NativeOpenCacheAction::KeepCache => {
                self.keep.fetch_add(1, Ordering::Relaxed);
            }
            NativeOpenCacheAction::Cached => {
                self.cached.fetch_add(1, Ordering::Relaxed);
            }
            NativeOpenCacheAction::InvalidateThenCached => {
                self.invalidated.fetch_add(1, Ordering::Relaxed);
            }
            NativeOpenCacheAction::DirectIo => {
                self.direct.fetch_add(1, Ordering::Relaxed);
            }
        }
        match decision.reason {
            NativeOpenCacheReason::StalePrevented => {
                self.stale_prevented.fetch_add(1, Ordering::Relaxed);
            }
            NativeOpenCacheReason::WriterBarrier => {
                self.writer_barrier.fetch_add(1, Ordering::Relaxed);
            }
            NativeOpenCacheReason::LocalCacheEvicted => {
                self.local_cache_evicted.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Returns a consistent-enough diagnostics snapshot.
    pub fn snapshot(&self) -> NativeOpenCacheMetricsSnapshot {
        NativeOpenCacheMetricsSnapshot {
            keep: self.keep.load(Ordering::Relaxed),
            cached: self.cached.load(Ordering::Relaxed),
            invalidated: self.invalidated.load(Ordering::Relaxed),
            direct: self.direct.load(Ordering::Relaxed),
            stale_prevented: self.stale_prevented.load(Ordering::Relaxed),
            writer_barrier: self.writer_barrier.load(Ordering::Relaxed),
            local_cache_evicted: self.local_cache_evicted.load(Ordering::Relaxed),
        }
    }
}

/// Chooses the Linux native kernel page-cache behavior for an open.
pub fn decide_native_open_cache(
    read_only: bool,
    state: NativeInodeCacheState,
) -> NativeOpenCacheDecision {
    if state.writer_active || state.dirty {
        return NativeOpenCacheDecision {
            action: NativeOpenCacheAction::DirectIo,
            reason: NativeOpenCacheReason::WriterBarrier,
        };
    }

    if !state.local_materialized {
        return NativeOpenCacheDecision {
            action: NativeOpenCacheAction::InvalidateThenCached,
            reason: NativeOpenCacheReason::LocalCacheEvicted,
        };
    }

    if state.cached_content_epoch.is_some()
        && (state.cached_content_epoch != Some(state.content_epoch)
            || state.cached_size != Some(state.size)
            || state.cached_mtime_ns != Some(state.mtime_ns)
            || state.observed_invalidation_generation != state.invalidation_generation)
    {
        return NativeOpenCacheDecision {
            action: NativeOpenCacheAction::InvalidateThenCached,
            reason: NativeOpenCacheReason::StalePrevented,
        };
    }

    if read_only
        && state.local_materialized
        && state.cached_content_epoch == Some(state.content_epoch)
        && state.cached_size == Some(state.size)
        && state.cached_mtime_ns == Some(state.mtime_ns)
        && state.observed_invalidation_generation == state.invalidation_generation
    {
        return NativeOpenCacheDecision {
            action: NativeOpenCacheAction::KeepCache,
            reason: NativeOpenCacheReason::Keep,
        };
    }

    NativeOpenCacheDecision {
        action: NativeOpenCacheAction::Cached,
        reason: NativeOpenCacheReason::Cached,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clean_trusted_state() -> NativeInodeCacheState {
        NativeInodeCacheState {
            content_epoch: 7,
            cached_content_epoch: Some(7),
            size: 4096,
            cached_size: Some(4096),
            mtime_ns: 123,
            cached_mtime_ns: Some(123),
            invalidation_generation: 3,
            observed_invalidation_generation: 3,
            writer_active: false,
            dirty: false,
            local_materialized: true,
        }
    }

    #[test]
    fn read_only_open_keeps_kernel_cache_when_epoch_size_mtime_and_generation_match() {
        let decision = decide_native_open_cache(true, clean_trusted_state());

        assert_eq!(
            decision,
            NativeOpenCacheDecision {
                action: NativeOpenCacheAction::KeepCache,
                reason: NativeOpenCacheReason::Keep,
            }
        );
    }

    #[test]
    fn read_only_open_invalidates_before_cached_io_when_content_epoch_changed() {
        let decision = decide_native_open_cache(
            true,
            NativeInodeCacheState {
                content_epoch: 8,
                ..clean_trusted_state()
            },
        );

        assert_eq!(
            decision,
            NativeOpenCacheDecision {
                action: NativeOpenCacheAction::InvalidateThenCached,
                reason: NativeOpenCacheReason::StalePrevented,
            }
        );
    }

    #[test]
    fn read_only_open_uses_direct_io_when_writer_state_is_dirty() {
        let decision = decide_native_open_cache(
            true,
            NativeInodeCacheState {
                dirty: true,
                ..clean_trusted_state()
            },
        );

        assert_eq!(
            decision,
            NativeOpenCacheDecision {
                action: NativeOpenCacheAction::DirectIo,
                reason: NativeOpenCacheReason::WriterBarrier,
            }
        );
    }

    #[test]
    fn read_only_open_invalidates_when_local_cache_was_evicted() {
        let decision = decide_native_open_cache(
            true,
            NativeInodeCacheState {
                local_materialized: false,
                ..clean_trusted_state()
            },
        );

        assert_eq!(
            decision,
            NativeOpenCacheDecision {
                action: NativeOpenCacheAction::InvalidateThenCached,
                reason: NativeOpenCacheReason::LocalCacheEvicted,
            }
        );
    }

    #[test]
    fn metrics_snapshot_counts_local_cache_eviction_reason() {
        let metrics = NativeOpenCacheMetrics::default();

        metrics.record(NativeOpenCacheDecision {
            action: NativeOpenCacheAction::InvalidateThenCached,
            reason: NativeOpenCacheReason::LocalCacheEvicted,
        });

        assert_eq!(metrics.snapshot().local_cache_evicted, 1);
    }
}

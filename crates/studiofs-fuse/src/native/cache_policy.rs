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

/// Inode content snapshot last trusted by the kernel page cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrustedCacheState {
    /// StudioFS content epoch trusted by the kernel page cache.
    pub content_epoch: u64,
    /// Inode size trusted by the kernel page cache.
    pub size: u64,
    /// Inode mtime in nanoseconds trusted by the kernel page cache.
    pub mtime_ns: u64,
    /// StudioFS invalidation generation observed with this trusted snapshot.
    pub invalidation_generation: u64,
}

/// Per-inode state used to decide whether kernel pages are still coherent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeInodeCacheState {
    /// Current StudioFS content epoch.
    pub content_epoch: u64,
    /// Current inode size.
    pub size: u64,
    /// Current mtime in nanoseconds.
    pub mtime_ns: u64,
    /// Current StudioFS invalidation generation.
    pub invalidation_generation: u64,
    /// Last content snapshot trusted by the kernel page cache, if any.
    pub trusted: Option<TrustedCacheState>,
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

/// Bounded native read-ahead policy for local-cache reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeReadAheadPolicy {
    /// Maximum bytes a single prefetch decision may request.
    pub max_prefetch_bytes: u64,
    /// Maximum gap still considered sequential.
    pub sequential_window_bytes: u64,
    /// Sequential reads required before prefetch starts.
    pub min_sequential_reads: u32,
}

/// Per-handle read-ahead state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeReadAheadState {
    last_end: Option<u64>,
    sequential_reads: u32,
    last_prefetch_end: Option<u64>,
}

/// File range selected for local-cache prefetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativePrefetchRange {
    /// File offset where prefetch should start.
    pub offset: u64,
    /// Number of bytes to prefetch.
    pub len: u64,
}

/// Decision returned after observing one read request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeReadAheadDecision {
    /// Optional bounded prefetch range.
    pub prefetch: Option<NativePrefetchRange>,
    /// Bytes prefetched beyond the caller's requested read.
    pub read_amplification_bytes: u64,
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
            // Keep/Cached reasons are already counted by the action counters above.
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

/// Updates read-ahead state and returns a bounded prefetch decision.
pub fn decide_native_read_ahead(
    policy: &NativeReadAheadPolicy,
    state: &mut NativeReadAheadState,
    offset: u64,
    size: u64,
    file_size: u64,
) -> NativeReadAheadDecision {
    let end = offset.saturating_add(size).min(file_size);
    let sequential = state.last_end.is_some_and(|last_end| {
        offset >= last_end && offset.saturating_sub(last_end) <= policy.sequential_window_bytes
    });
    state.sequential_reads = if sequential {
        state.sequential_reads.saturating_add(1)
    } else {
        state.last_prefetch_end = None;
        1
    };
    state.last_end = Some(end);

    if state.sequential_reads < policy.min_sequential_reads || end >= file_size {
        return NativeReadAheadDecision {
            prefetch: None,
            read_amplification_bytes: 0,
        };
    }

    if let Some(last_prefetch_end) = state.last_prefetch_end {
        let remaining_prefetched = last_prefetch_end.saturating_sub(end);
        if end < last_prefetch_end && remaining_prefetched > policy.max_prefetch_bytes / 2 {
            return NativeReadAheadDecision {
                prefetch: None,
                read_amplification_bytes: 0,
            };
        }
    }

    let prefetch_start = state.last_prefetch_end.unwrap_or(end).max(end);
    if prefetch_start >= file_size {
        return NativeReadAheadDecision {
            prefetch: None,
            read_amplification_bytes: 0,
        };
    }

    let available = file_size - prefetch_start;
    let len = available.min(policy.max_prefetch_bytes);
    if len == 0 {
        NativeReadAheadDecision {
            prefetch: None,
            read_amplification_bytes: 0,
        }
    } else {
        state.last_prefetch_end = Some(prefetch_start + len);
        NativeReadAheadDecision {
            prefetch: Some(NativePrefetchRange {
                offset: prefetch_start,
                len,
            }),
            read_amplification_bytes: len,
        }
    }
}

/// Chooses the Linux native kernel page-cache behavior for an open.
pub fn decide_native_open_cache(
    read_only: bool,
    state: &NativeInodeCacheState,
) -> NativeOpenCacheDecision {
    if state.writer_active || state.dirty {
        return NativeOpenCacheDecision {
            action: NativeOpenCacheAction::InvalidateThenCached,
            reason: NativeOpenCacheReason::WriterBarrier,
        };
    }

    if !state.local_materialized {
        return NativeOpenCacheDecision {
            action: NativeOpenCacheAction::InvalidateThenCached,
            reason: NativeOpenCacheReason::LocalCacheEvicted,
        };
    }

    let current = state.current_trusted_snapshot();
    if state.trusted.is_some_and(|trusted| trusted != current) {
        return NativeOpenCacheDecision {
            action: NativeOpenCacheAction::InvalidateThenCached,
            reason: NativeOpenCacheReason::StalePrevented,
        };
    }

    if read_only && state.trusted.is_some() {
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

impl NativeInodeCacheState {
    /// Returns the current inode snapshot shape used for trusted-cache comparison.
    pub fn current_trusted_snapshot(&self) -> TrustedCacheState {
        TrustedCacheState {
            content_epoch: self.content_epoch,
            size: self.size,
            mtime_ns: self.mtime_ns,
            invalidation_generation: self.invalidation_generation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    fn clean_trusted_state() -> NativeInodeCacheState {
        NativeInodeCacheState {
            content_epoch: 7,
            size: 4096,
            mtime_ns: 123,
            invalidation_generation: 3,
            trusted: Some(TrustedCacheState {
                content_epoch: 7,
                size: 4096,
                mtime_ns: 123,
                invalidation_generation: 3,
            }),
            writer_active: false,
            dirty: false,
            local_materialized: true,
        }
    }

    #[test]
    fn native_inode_cache_state_groups_trusted_snapshot_compactly() {
        assert!(size_of::<NativeInodeCacheState>() <= 80);

        let state = NativeInodeCacheState {
            content_epoch: 7,
            size: 4096,
            mtime_ns: 123,
            invalidation_generation: 3,
            trusted: Some(TrustedCacheState {
                content_epoch: 7,
                size: 4096,
                mtime_ns: 123,
                invalidation_generation: 3,
            }),
            writer_active: false,
            dirty: false,
            local_materialized: true,
        };

        assert_eq!(
            state.trusted,
            Some(TrustedCacheState {
                content_epoch: 7,
                size: 4096,
                mtime_ns: 123,
                invalidation_generation: 3,
            })
        );
    }

    #[test]
    fn read_only_open_keeps_kernel_cache_when_epoch_size_mtime_and_generation_match() {
        let state = clean_trusted_state();
        let decision = decide_native_open_cache(true, &state);

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
            &NativeInodeCacheState {
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
    fn read_only_open_invalidates_before_cached_io_when_writer_state_is_dirty() {
        let decision = decide_native_open_cache(
            true,
            &NativeInodeCacheState {
                dirty: true,
                ..clean_trusted_state()
            },
        );

        assert_eq!(
            decision,
            NativeOpenCacheDecision {
                action: NativeOpenCacheAction::InvalidateThenCached,
                reason: NativeOpenCacheReason::WriterBarrier,
            }
        );
    }

    #[test]
    fn read_only_open_invalidates_when_local_cache_was_evicted() {
        let decision = decide_native_open_cache(
            true,
            &NativeInodeCacheState {
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

    #[test]
    fn sequential_reads_get_bounded_readahead_after_minimum_streak() {
        let policy = NativeReadAheadPolicy {
            max_prefetch_bytes: 128 * 1024,
            sequential_window_bytes: 4096,
            min_sequential_reads: 2,
        };
        let mut history = NativeReadAheadState::default();

        let first = decide_native_read_ahead(&policy, &mut history, 0, 4096, 1024 * 1024);
        let second = decide_native_read_ahead(&policy, &mut history, 4096, 4096, 1024 * 1024);

        assert_eq!(first.prefetch, None);
        assert_eq!(
            second.prefetch,
            Some(NativePrefetchRange {
                offset: 8192,
                len: 128 * 1024,
            })
        );
    }

    #[test]
    fn sequential_reads_do_not_repeat_prefetch_inside_existing_window() {
        let policy = NativeReadAheadPolicy {
            max_prefetch_bytes: 16 * 1024,
            sequential_window_bytes: 4096,
            min_sequential_reads: 2,
        };
        let mut history = NativeReadAheadState::default();

        let first = decide_native_read_ahead(&policy, &mut history, 0, 4096, 1024 * 1024);
        let second = decide_native_read_ahead(&policy, &mut history, 4096, 4096, 1024 * 1024);
        let third = decide_native_read_ahead(&policy, &mut history, 8192, 4096, 1024 * 1024);
        let fourth = decide_native_read_ahead(&policy, &mut history, 12_288, 4096, 1024 * 1024);

        assert_eq!(first.prefetch, None);
        assert_eq!(
            second.prefetch,
            Some(NativePrefetchRange {
                offset: 8192,
                len: 16 * 1024,
            })
        );
        assert_eq!(third.prefetch, None);
        assert_eq!(
            fourth.prefetch,
            Some(NativePrefetchRange {
                offset: 24_576,
                len: 16 * 1024,
            })
        );
    }

    #[test]
    fn sparse_random_reads_do_not_trigger_prefetch_amplification() {
        let policy = NativeReadAheadPolicy {
            max_prefetch_bytes: 128 * 1024,
            sequential_window_bytes: 4096,
            min_sequential_reads: 2,
        };
        let mut history = NativeReadAheadState::default();

        let first = decide_native_read_ahead(&policy, &mut history, 0, 4096, 1024 * 1024);
        let second = decide_native_read_ahead(&policy, &mut history, 512 * 1024, 4096, 1024 * 1024);

        assert_eq!(first.prefetch, None);
        assert_eq!(second.prefetch, None);
        assert_eq!(second.read_amplification_bytes, 0);
    }
}

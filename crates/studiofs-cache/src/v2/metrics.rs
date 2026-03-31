//! Cache v2 runtime metrics and counters.
//!
//! [`CacheMetrics`] is a lightweight snapshot of all [`super::block_store::BlockStore`]
//! counters, suitable for logging, health-check endpoints, or tracing spans.
//! Detailed histograms and per-operation counters will be added as the
//! BlockStore matures.

use crate::v2::block_store::BlockStore;

/// Point-in-time snapshot of BlockStore health metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheMetrics {
    /// Bytes currently allocated to live blocks.
    pub allocated_bytes: u64,
    /// Number of cached blocks.
    pub block_count: u64,
    /// Number of dirty (not yet uploaded) blocks.
    pub dirty_count: u32,
    /// Total cache hits since store was opened.
    pub hit_count: u64,
    /// Total cache misses since store was opened.
    pub miss_count: u64,
}

impl CacheMetrics {
    /// Snapshot current metrics from `store`.
    pub fn snapshot(store: &BlockStore) -> Self {
        Self {
            allocated_bytes: store.allocated_bytes(),
            block_count: store.block_count(),
            dirty_count: store.dirty_count(),
            hit_count: store.hit_count(),
            miss_count: store.miss_count(),
        }
    }

    /// Hit ratio in the range `[0.0, 1.0]`, or `0.0` when no requests have
    /// been recorded yet.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.hit_count as f64 / total as f64
        }
    }
}

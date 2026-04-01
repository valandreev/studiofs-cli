//! Cache v2 runtime metrics and counters.
//!
//! [`CacheMetrics`] is a lightweight snapshot of all [`super::block_store::BlockStore`]
//! counters, suitable for logging, health-check endpoints, or tracing spans.
//! Values are lock-free snapshots intended for operational observability and
//! benchmark comparisons.

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
    /// Total evictions and reclaimed bytes.
    pub eviction_count: u64,
    pub eviction_bytes: u64,
    /// Background watermark eviction counters.
    pub background_eviction_count: u64,
    pub background_eviction_bytes: u64,
    /// Emergency low-free-space eviction counters.
    pub emergency_eviction_count: u64,
    pub emergency_eviction_bytes: u64,
    /// Number of corrupt entries dropped after integrity verification failures.
    pub corruption_count: u64,
    /// Read-side telemetry.
    pub read_ops: u64,
    pub read_bytes: u64,
    pub read_latency_total_ns: u64,
    pub read_latency_max_ns: u64,
    /// Write-side telemetry.
    pub write_ops: u64,
    pub write_bytes: u64,
    pub write_latency_total_ns: u64,
    pub write_latency_max_ns: u64,
    /// Number of successful post-read dont-need advisory hints.
    pub advisory_dont_need_count: u64,
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
            eviction_count: store.eviction_count(),
            eviction_bytes: store.eviction_bytes(),
            background_eviction_count: store.background_eviction_count(),
            background_eviction_bytes: store.background_eviction_bytes(),
            emergency_eviction_count: store.emergency_eviction_count(),
            emergency_eviction_bytes: store.emergency_eviction_bytes(),
            corruption_count: store.corruption_count(),
            read_ops: store.read_ops_count(),
            read_bytes: store.read_bytes(),
            read_latency_total_ns: store.read_latency_total_ns(),
            read_latency_max_ns: store.read_latency_max_ns(),
            write_ops: store.write_ops_count(),
            write_bytes: store.write_bytes(),
            write_latency_total_ns: store.write_latency_total_ns(),
            write_latency_max_ns: store.write_latency_max_ns(),
            advisory_dont_need_count: store.advisory_dont_need_count(),
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

    /// Miss ratio in the range `[0.0, 1.0]`.
    pub fn miss_ratio(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.miss_count as f64 / total as f64
        }
    }

    /// Effective read throughput in bytes/sec over observed successful reads
    /// since the current `BlockStore` instance was opened.
    pub fn read_throughput_bytes_per_sec(&self) -> f64 {
        if self.read_latency_total_ns == 0 {
            0.0
        } else {
            self.read_bytes as f64 / (self.read_latency_total_ns as f64 / 1_000_000_000.0)
        }
    }

    /// Effective write throughput in bytes/sec over observed successful writes
    /// since the current `BlockStore` instance was opened.
    pub fn write_throughput_bytes_per_sec(&self) -> f64 {
        if self.write_latency_total_ns == 0 {
            0.0
        } else {
            self.write_bytes as f64 / (self.write_latency_total_ns as f64 / 1_000_000_000.0)
        }
    }
}

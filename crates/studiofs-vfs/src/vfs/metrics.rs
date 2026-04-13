use super::file_session::VfsFileSessionStats;
use serde::{Deserialize, Serialize};

/// Filesystem statistics returned by `statfs()`.
#[derive(Debug, Clone)]
pub struct StatFs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsIngressStats {
    pub dirty_mem_bytes: usize,
    pub dirty_mem_blocks: usize,
    pub spill_bytes: usize,
    pub spill_records: usize,
    pub oldest_spill_age_ms: u64,
    pub write_backpressure_wait_ms: u64,
    pub write_backpressure_events: u64,
    pub fast_backpressure_wait_ms: u64,
    pub fast_backpressure_events: u64,
    pub fast_backpressure_stall_max_ms: u64,
    pub forced_spill_blocks: u64,
    pub drained_mem_blocks: u64,
    pub drained_mem_bytes: u64,
    pub drained_spill_blocks: u64,
    pub drained_spill_bytes: u64,
    pub prefetch_queue_depth: u64,
    pub checkpoint_persists: u64,
    pub checkpoint_failures: u64,
    pub last_checkpoint_age_ms: u64,
    pub last_checkpoint_segment_id: u64,
    pub last_checkpoint_offset: u64,
    pub checkpoint_pending_bytes: u64,
    pub checkpoint_pending_mutations: u64,
    pub checkpoint_active_segment_lag: u64,
    pub checkpoint_live_segments: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLatencyQuantilesMs {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLatencyQuantilesSaturated {
    pub p50: bool,
    pub p95: bool,
    pub p99: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsLatencyHistogramSnapshot {
    pub bucket_upper_bounds_ns: Vec<u64>,
    pub bucket_counts: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathSyncDataStats {
    pub calls: u64,
    pub calls_per_sec: f64,
    pub deferred_calls: u64,
    pub synced_bytes: u64,
    pub synced_gib: f64,
    pub calls_per_gib: f64,
    pub total_ms: f64,
    pub avg_ms: f64,
    pub max_ms: f64,
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    pub latency_histogram: VfsLatencyHistogramSnapshot,
    #[serde(default)]
    pub queue_wait_total_ms: f64,
    #[serde(default)]
    pub queue_wait_avg_ms: f64,
    #[serde(default)]
    pub queue_wait_max_ms: f64,
    #[serde(default)]
    pub queue_wait_latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    pub queue_wait_latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    #[serde(default)]
    pub queue_wait_latency_histogram: VfsLatencyHistogramSnapshot,
    #[serde(default)]
    pub barrier_interval_avg_ms: f64,
    #[serde(default)]
    pub barrier_interval_max_ms: f64,
    #[serde(default)]
    pub barrier_interval_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    pub barrier_interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    #[serde(default)]
    pub barrier_interval_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspCallbackStats {
    pub write: u64,
    #[serde(default)]
    pub overwrite: u64,
    #[serde(default)]
    pub cleanup: u64,
    pub flush: u64,
    #[serde(default)]
    pub close: u64,
    #[serde(default)]
    pub cleanup_flush_fh: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspForegroundFlushStats {
    pub cleanup: u64,
    pub flush: u64,
    pub close: u64,
    #[serde(default)]
    pub cleanup_skipped_clean: u64,
    #[serde(default)]
    pub flush_skipped_clean: u64,
    #[serde(default)]
    pub close_skipped_clean: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLocalCacheIoStats {
    pub execution_model: String,
    pub calls: u64,
    pub handoff_total_ms: f64,
    pub handoff_avg_ms: f64,
    pub handoff_max_ms: f64,
    pub backend_io_total_ms: f64,
    pub backend_io_avg_ms: f64,
    pub backend_io_max_ms: f64,
    pub handoff_histogram: VfsLatencyHistogramSnapshot,
    pub backend_io_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathBackpressureStats {
    pub events: u64,
    pub wait_total_ms: u64,
    pub stall_max_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathBurstStats {
    pub bursts: u64,
    pub bursts_per_sec: f64,
    pub total_blocks: u64,
    pub total_bytes: u64,
    pub avg_blocks_per_burst: f64,
    pub avg_bytes_per_burst: f64,
    pub max_blocks_per_burst: u64,
    pub max_bytes_per_burst: u64,
    pub avg_interval_ms: f64,
    pub max_interval_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathStageLatencyStats {
    pub calls: u64,
    pub calls_per_sec: f64,
    pub total_ms: f64,
    pub avg_ms: f64,
    pub max_ms: f64,
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    pub latency_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathCadenceStats {
    pub events: u64,
    pub events_per_sec: f64,
    pub avg_interval_ms: f64,
    pub max_interval_ms: f64,
    pub interval_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    pub interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    pub interval_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathEagerFlushStats {
    pub checks: u64,
    pub triggered: u64,
    pub skipped_empty: u64,
    pub skipped_below_threshold: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathTuningStats {
    pub eager_flush_threshold_bytes: usize,
    pub eager_flush_drain_chunk_bytes: usize,
    pub payload_put_concurrency: usize,
    pub sync_group_min_bytes: u64,
    pub sync_group_max_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathObservabilitySnapshot {
    pub generated_unix_ms: u64,
    pub uptime_secs: f64,
    pub backend_tag: String,
    pub flush_ordered_calls: u64,
    pub flush_ordered_calls_per_sec: f64,
    pub block_store_sync_data: VfsWritePathSyncDataStats,
    #[serde(default)]
    pub local_cache_io: VfsWritePathLocalCacheIoStats,
    pub winfsp_callbacks: VfsWritePathWinfspCallbackStats,
    #[serde(default)]
    pub winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats,
    pub write_backpressure: VfsWritePathBackpressureStats,
    #[serde(default)]
    pub payload_burst: VfsWritePathBurstStats,
    #[serde(default)]
    pub payload_persist: VfsWritePathStageLatencyStats,
    #[serde(default)]
    pub metadata_commit: VfsWritePathStageLatencyStats,
    #[serde(default)]
    pub metadata_commit_cadence: VfsWritePathCadenceStats,
    #[serde(default)]
    pub eager_flush: VfsWritePathEagerFlushStats,
    #[serde(default)]
    pub tuning: VfsWritePathTuningStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VfsIngressLane {
    DirtyMem,
    Spill,
    Backpressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VfsIngressPressure {
    pub next_bytes: usize,
    pub dirty_mem_bytes: usize,
    pub dirty_mem_limit: usize,
    pub spill_bytes: usize,
    pub spill_limit: usize,
    pub lane: VfsIngressLane,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsUploadStats {
    pub pending_uploads: usize,
    pub upload_requeues: u64,
    pub canceled_deleted: u64,
    pub canceled_stale: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsForegroundReadProtectionStats {
    pub p90_latency_ns: Option<u64>,
    pub throughput_bytes_per_sec: f64,
    pub speedometer_samples: usize,
    pub window_bytes: u64,
    pub window_coverage_ms: u64,
    pub dominant_access_class: Option<String>,
    pub dominant_prefetch_enabled: bool,
    pub dominant_cache_promotion_on_read: bool,
    pub dominant_readahead_base_width: u32,
    pub dominant_readahead_max_width: u32,
    pub dominant_readahead_min_prefetch: u32,
    pub dominant_upload_pressure_boost: u8,
    pub active_sequential_media_reads: u64,
    pub active_random_interactive_reads: u64,
    pub active_sequential_writers: u64,
    pub active_random_writers: u64,
    pub upload_fallback_deferrals: u64,
    pub upload_fallback_rereads: u64,
    pub upload_fallback_reread_bytes: u64,
    pub upload_fallback_reread_cache_misses: u64,
    pub upload_fallback_wait_ns: u64,
    pub upload_fallback_wait_max_ns: u64,
    pub upload_fallback_read_ns: u64,
    pub upload_fallback_read_max_ns: u64,
    pub upload_cache_io_foreground_cost: u32,
    pub upload_scheduler_penalty_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsNamespaceCacheSnapshot {
    pub hits: u64,
    pub protected_hits: u64,
    pub misses: u64,
    pub negative_hits: u64,
    pub evictions: u64,
    pub negative_hit_ratio: f64,
    pub hot_hit_ratio: f64,
    pub occupancy_entries: usize,
    pub occupancy_bytes: usize,
    pub protected_occupancy_entries: usize,
    pub protected_occupancy_bytes: usize,
    pub entry_budget: usize,
    pub byte_budget: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsAttrReadCacheSnapshot {
    pub hits: u64,
    pub protected_hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub hot_hit_ratio: f64,
    pub occupancy_entries: usize,
    pub occupancy_bytes: usize,
    pub protected_occupancy_entries: usize,
    pub protected_occupancy_bytes: usize,
    pub entry_budget: usize,
    pub byte_budget: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsTransitMemorySnapshot {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub available_bytes: usize,
    pub waiting_foreground: usize,
    pub waiting_background: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsCacheStateSnapshot {
    pub dentry_cache: VfsNamespaceCacheSnapshot,
    pub path_shortcut_cache: VfsNamespaceCacheSnapshot,
    pub attr_read_cache: VfsAttrReadCacheSnapshot,
    pub local_backend_dirty_objects: usize,
    pub prefetch_queue_depth: u64,
    pub transit_memory: VfsTransitMemorySnapshot,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsSynchronizationStateSnapshot {
    pub file_sessions: VfsFileSessionStats,
    pub foreground_reads: VfsForegroundReadProtectionStats,
    pub pending_metadata_ops: u32,
    pub inflight_metadata_ops: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsRuntimeEvent {
    pub sequence: u64,
    pub generated_unix_ms: u64,
    pub subsystem: String,
    pub kind: String,
    pub title: String,
    #[serde(default)]
    pub fields: serde_json::Value,
}

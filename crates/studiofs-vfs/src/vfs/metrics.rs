use super::file_session::VfsFileSessionStats;
use serde::{Deserialize, Serialize};
use studiofs_meta::schema::{InodeLifecycleState, PayloadRetentionClass};

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
    pub write_backpressure_wait_ms: u64,
    pub write_backpressure_events: u64,
    pub fast_backpressure_wait_ms: u64,
    pub fast_backpressure_events: u64,
    pub fast_backpressure_stall_max_ms: u64,
    pub prefetch_queue_depth: u64,
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
pub struct VfsWritePathWinfspWritePathStats {
    pub write_bytes: u64,
    pub avg_write_bytes: f64,
    #[serde(default)]
    pub strict_calls: u64,
    #[serde(default)]
    pub strict_bytes: u64,
    #[serde(default)]
    pub opportunistic_calls: u64,
    #[serde(default)]
    pub opportunistic_bytes: u64,
    #[serde(default)]
    pub callback: VfsWritePathStageLatencyStats,
    #[serde(default)]
    pub vfs_write: VfsWritePathStageLatencyStats,
    #[serde(default)]
    pub post_write_getattr: VfsWritePathStageLatencyStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathVfsWriteStats {
    pub calls: u64,
    pub write_bytes: u64,
    pub avg_write_bytes: f64,
    #[serde(default)]
    pub record_handle_writer_state: VfsWritePathStageLatencyStats,
    #[serde(default)]
    pub materialize_completed: VfsWritePathStageLatencyStats,
    #[serde(default)]
    pub materialize_strict: VfsWritePathStageLatencyStats,
    #[serde(default)]
    pub materialize_for_limits: VfsWritePathStageLatencyStats,
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
pub struct VfsWritePathDirtyPressureStats {
    pub pending_local_dirty_bytes: usize,
    pub handle_writer_dirty_bytes: usize,
    pub total_dirty_bytes: usize,
    pub dirty_limit_bytes: usize,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathObservabilitySnapshot {
    #[serde(default)]
    pub publisher_pid: u32,
    pub generated_unix_ms: u64,
    pub uptime_secs: f64,
    #[serde(default)]
    pub pending_uploads: usize,
    pub backend_tag: String,
    pub flush_barrier_calls: u64,
    pub flush_barrier_calls_per_sec: f64,
    pub block_store_sync_data: VfsWritePathSyncDataStats,
    #[serde(default)]
    pub local_cache_io: VfsWritePathLocalCacheIoStats,
    pub winfsp_callbacks: VfsWritePathWinfspCallbackStats,
    #[serde(default)]
    pub winfsp_write_path: VfsWritePathWinfspWritePathStats,
    #[serde(default)]
    pub vfs_write_path: VfsWritePathVfsWriteStats,
    #[serde(default)]
    pub winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats,
    pub write_backpressure: VfsWritePathBackpressureStats,
    #[serde(default)]
    pub dirty_pressure: VfsWritePathDirtyPressureStats,
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
    Backpressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VfsIngressPressure {
    pub next_bytes: usize,
    pub dirty_mem_bytes: usize,
    pub dirty_mem_limit: usize,
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
pub struct VfsZeroIoCacheBytesSnapshot {
    pub total_local_bytes: u64,
    pub backend_evictable_bytes: u64,
    pub backend_non_evictable_bytes: u64,
    pub pinned_unpublished_bytes: u64,
    pub publish_ready_retained_bytes: u64,
    pub published_warm_bytes: u64,
    pub deleted_before_publish_bytes: u64,
    pub unclassified_non_evictable_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoStartupReplaySnapshot {
    pub generated_unix_ms: u64,
    pub replayed_epochs: u64,
    pub replayed_blocks: u64,
    pub queued_publish_ready_epochs: u64,
    pub quarantined_inodes: u64,
    pub orphan_dirty_blocks_removed: u64,
    pub replay_failures: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoShutdownSnapshot {
    pub generated_unix_ms: u64,
    pub accepted_local_dirty_bytes_before: u64,
    pub accepted_local_dirty_bytes_after: u64,
    pub pending_uploads_after: usize,
    pub uploader_drained_before_cancel: bool,
    pub metadata_flush_outcome: String,
    pub local_payload_sync_ok: bool,
    pub recoverable_on_restart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoHealthSignal {
    pub severity: String,
    pub code: String,
    pub summary: String,
    #[serde(default)]
    pub ino: Option<u64>,
    #[serde(default)]
    pub content_epoch: Option<u64>,
    #[serde(default)]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoDrilldownEntry {
    pub ino: u64,
    #[serde(default)]
    pub path: Option<String>,
    pub lifecycle_state: InodeLifecycleState,
    pub retention: PayloadRetentionClass,
    pub local_content_epoch: u64,
    pub published_content_epoch: u64,
    pub local_visible_size: u64,
    pub published_size: u64,
    pub live_extent_count: usize,
    pub pending_upload_count: usize,
    pub pending_upload_bytes: u64,
    pub oldest_pending_upload_age_ms: u64,
    pub publish_ready_queued: bool,
    pub replayed_after_restart: bool,
    pub repeated_replay_count: u64,
    pub deferred_cleanup_object_count: usize,
    pub deferred_cleanup_bytes: u64,
    #[serde(default)]
    pub sticky_error: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoRuntimeSnapshot {
    pub generated_unix_ms: u64,
    pub unpublished_files: usize,
    pub unpublished_epochs: usize,
    pub unpublished_payload_bytes: u64,
    pub uploading_epochs: usize,
    pub replay_backlog_epochs: usize,
    pub replay_backlog_bytes: u64,
    pub publish_ready_backlog_epochs: usize,
    pub replayed_after_restart_epochs: u64,
    pub deferred_delete_backlog_epochs: usize,
    pub deferred_delete_backlog_objects: usize,
    pub deferred_delete_backlog_bytes: u64,
    pub replay_failures: u64,
    pub publish_failures: u64,
    pub cache_bytes: VfsZeroIoCacheBytesSnapshot,
    #[serde(default)]
    pub startup_replay: Option<VfsZeroIoStartupReplaySnapshot>,
    #[serde(default)]
    pub last_graceful_shutdown: Option<VfsZeroIoShutdownSnapshot>,
    #[serde(default)]
    pub health_signals: Vec<VfsZeroIoHealthSignal>,
    pub drilldown_truncated: usize,
    #[serde(default)]
    pub drilldown: Vec<VfsZeroIoDrilldownEntry>,
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
    pub upload_local_payload_reads: u64,
    pub upload_local_payload_read_bytes: u64,
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

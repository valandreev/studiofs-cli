use super::file_session::VfsFileSessionStats;
use serde::{Deserialize, Serialize};
use studiofs_meta::schema::{InodeLifecycleState, PayloadRetentionClass};

/// Filesystem statistics returned by `statfs()`.
#[derive(Debug, Clone)]
pub struct StatFs {
    /// Field $name.
    pub blocks: u64,
    /// Field $name.
    pub bfree: u64,
    /// Field $name.
    pub bavail: u64,
    /// Field $name.
    pub files: u64,
    /// Field $name.
    pub ffree: u64,
    /// Field $name.
    pub bsize: u32,
    /// Field $name.
    pub namelen: u32,
    /// Field $name.
    pub frsize: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsIngressStats {
    /// Field $name.
    pub dirty_mem_bytes: usize,
    /// Field $name.
    pub dirty_mem_blocks: usize,
    /// Field $name.
    pub write_backpressure_wait_ms: u64,
    /// Field $name.
    pub write_backpressure_events: u64,
    /// Field $name.
    pub fast_backpressure_wait_ms: u64,
    /// Field $name.
    pub fast_backpressure_events: u64,
    /// Field $name.
    pub fast_backpressure_stall_max_ms: u64,
    /// Field $name.
    pub prefetch_queue_depth: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathLatencyQuantilesMs {
    /// Field $name.
    pub p50: f64,
    /// Field $name.
    pub p95: f64,
    /// Field $name.
    pub p99: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathLatencyQuantilesSaturated {
    /// Field $name.
    pub p50: bool,
    /// Field $name.
    pub p95: bool,
    /// Field $name.
    pub p99: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsLatencyHistogramSnapshot {
    /// Field $name.
    pub bucket_upper_bounds_ns: Vec<u64>,
    /// Field $name.
    pub bucket_counts: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathSyncDataStats {
    /// Field $name.
    pub calls: u64,
    /// Field $name.
    pub calls_per_sec: f64,
    /// Field $name.
    pub deferred_calls: u64,
    /// Field $name.
    pub synced_bytes: u64,
    /// Field $name.
    pub synced_gib: f64,
    /// Field $name.
    pub calls_per_gib: f64,
    /// Field $name.
    pub total_ms: f64,
    /// Field $name.
    pub avg_ms: f64,
    /// Field $name.
    pub max_ms: f64,
    /// Field $name.
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    /// Field $name.
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Field $name.
    pub latency_histogram: VfsLatencyHistogramSnapshot,
    #[serde(default)]
    /// Field $name.
    pub queue_wait_total_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub queue_wait_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub queue_wait_max_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub queue_wait_latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    /// Field $name.
    pub queue_wait_latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    #[serde(default)]
    /// Field $name.
    pub queue_wait_latency_histogram: VfsLatencyHistogramSnapshot,
    #[serde(default)]
    /// Field $name.
    pub barrier_interval_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub barrier_interval_max_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub barrier_interval_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    /// Field $name.
    pub barrier_interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    #[serde(default)]
    /// Field $name.
    pub barrier_interval_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathWinfspCallbackStats {
    /// Field $name.
    pub write: u64,
    #[serde(default)]
    /// Field $name.
    pub overwrite: u64,
    #[serde(default)]
    /// Field $name.
    pub cleanup: u64,
    /// Field $name.
    pub flush: u64,
    #[serde(default)]
    /// Field $name.
    pub close: u64,
    #[serde(default)]
    /// Field $name.
    pub cleanup_flush_fh: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathWinfspWritePathStats {
    /// Field $name.
    pub write_bytes: u64,
    /// Field $name.
    pub avg_write_bytes: f64,
    #[serde(default)]
    /// Field $name.
    pub strict_calls: u64,
    #[serde(default)]
    /// Field $name.
    pub strict_bytes: u64,
    #[serde(default)]
    /// Field $name.
    pub write_through_calls: u64,
    #[serde(default)]
    /// Field $name.
    pub write_through_bytes: u64,
    #[serde(default)]
    /// Field $name.
    pub opportunistic_calls: u64,
    #[serde(default)]
    /// Field $name.
    pub opportunistic_bytes: u64,
    #[serde(default)]
    /// Field $name.
    pub callback: VfsWritePathStageLatencyStats,
    #[serde(default)]
    /// Field $name.
    pub vfs_write: VfsWritePathStageLatencyStats,
    #[serde(default)]
    /// Field $name.
    pub post_write_getattr: VfsWritePathStageLatencyStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathVfsWriteStats {
    /// Field $name.
    pub calls: u64,
    /// Field $name.
    pub write_bytes: u64,
    /// Field $name.
    pub avg_write_bytes: f64,
    #[serde(default)]
    /// Field $name.
    pub record_writer_stack_state: VfsWritePathStageLatencyStats,
    #[serde(default)]
    /// Field $name.
    pub materialize_completed: VfsWritePathStageLatencyStats,
    #[serde(default)]
    /// Field $name.
    pub materialize_strict: VfsWritePathStageLatencyStats,
    #[serde(default)]
    /// Field $name.
    pub materialize_for_limits: VfsWritePathStageLatencyStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathLandingZoneStats {
    /// Field $name.
    pub accepted_writes: u64,
    /// Field $name.
    pub accepted_bytes: u64,
    #[serde(default)]
    /// Field $name.
    pub accepted_mib_per_sec: f64,
    #[serde(default)]
    /// Field $name.
    pub recoverable_segments: usize,
    #[serde(default)]
    /// Field $name.
    pub accept_total_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub accept_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub accept_max_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub data_syncs: u64,
    #[serde(default)]
    /// Field $name.
    pub manifest_syncs: u64,
    #[serde(default)]
    /// Field $name.
    pub manifest_rewrites: u64,
    #[serde(default)]
    /// Field $name.
    pub sync_total_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub sync_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub sync_max_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathDrainLagStats {
    /// Field $name.
    pub current_bytes: usize,
    /// Field $name.
    pub steady_quota_bytes: usize,
    /// Field $name.
    pub soft_pacing_events: u64,
    /// Field $name.
    pub soft_pacing_total_ms: u64,
    /// Field $name.
    pub soft_pacing_max_ms: u64,
    #[serde(default)]
    /// Field $name.
    pub admission_ingress_bytes_per_sec: u64,
    #[serde(default)]
    /// Field $name.
    pub admission_drain_bytes_per_sec: u64,
    #[serde(default)]
    /// Field $name.
    pub admission_runway_ms: u64,
    #[serde(default)]
    /// Field $name.
    pub admission_last_delay_ms: u64,
    #[serde(default)]
    /// Field $name.
    pub admission_pressure_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub admission_budget_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub local_sync_unsynced_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathWinfspForegroundFlushStats {
    /// Field $name.
    pub cleanup: u64,
    /// Field $name.
    pub flush: u64,
    /// Field $name.
    pub close: u64,
    #[serde(default)]
    /// Field $name.
    pub cleanup_skipped_clean: u64,
    #[serde(default)]
    /// Field $name.
    pub flush_skipped_clean: u64,
    #[serde(default)]
    /// Field $name.
    pub close_skipped_clean: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathLocalCacheIoStats {
    /// Field $name.
    pub execution_model: String,
    /// Field $name.
    pub calls: u64,
    /// Field $name.
    pub handoff_total_ms: f64,
    /// Field $name.
    pub handoff_avg_ms: f64,
    /// Field $name.
    pub handoff_max_ms: f64,
    /// Field $name.
    pub backend_io_total_ms: f64,
    /// Field $name.
    pub backend_io_avg_ms: f64,
    /// Field $name.
    pub backend_io_max_ms: f64,
    /// Field $name.
    pub handoff_histogram: VfsLatencyHistogramSnapshot,
    /// Field $name.
    pub backend_io_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathBackpressureStats {
    /// Field $name.
    pub events: u64,
    /// Field $name.
    pub wait_total_ms: u64,
    /// Field $name.
    pub stall_max_ms: u64,
    #[serde(default)]
    /// Field $name.
    pub soft_throttle_events: u64,
    #[serde(default)]
    /// Field $name.
    pub hard_events: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathDirtyPressureStats {
    /// Field $name.
    pub pending_local_dirty_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub pending_persisted_slice_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub dirty_slice_count: usize,
    #[serde(default)]
    /// Field $name.
    pub frozen_slice_count: usize,
    /// Field $name.
    pub writer_stack_dirty_bytes: usize,
    /// Field $name.
    pub total_dirty_bytes: usize,
    /// Field $name.
    pub dirty_limit_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub pending_upload_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub cache_pressure_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub total_accounted_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub dirty_peak_bytes: usize,
    #[serde(default)]
    /// Field $name.
    pub staged_peak_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathBurstStats {
    /// Field $name.
    pub bursts: u64,
    /// Field $name.
    pub bursts_per_sec: f64,
    /// Field $name.
    pub total_payloads: u64,
    /// Field $name.
    pub total_bytes: u64,
    /// Field $name.
    pub avg_payloads_per_burst: f64,
    /// Field $name.
    pub avg_bytes_per_burst: f64,
    /// Field $name.
    pub max_payloads_per_burst: u64,
    /// Field $name.
    pub max_bytes_per_burst: u64,
    /// Field $name.
    pub avg_interval_ms: f64,
    /// Field $name.
    pub max_interval_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathStageLatencyStats {
    /// Field $name.
    pub calls: u64,
    /// Field $name.
    pub calls_per_sec: f64,
    /// Field $name.
    pub total_ms: f64,
    /// Field $name.
    pub avg_ms: f64,
    /// Field $name.
    pub max_ms: f64,
    /// Field $name.
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    /// Field $name.
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Field $name.
    pub latency_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathCadenceStats {
    /// Field $name.
    pub events: u64,
    /// Field $name.
    pub events_per_sec: f64,
    /// Field $name.
    pub avg_interval_ms: f64,
    /// Field $name.
    pub max_interval_ms: f64,
    /// Field $name.
    pub interval_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    /// Field $name.
    pub interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Field $name.
    pub interval_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathEagerFlushStats {
    /// Field $name.
    pub checks: u64,
    /// Field $name.
    pub triggered: u64,
    /// Field $name.
    pub skipped_empty: u64,
    /// Field $name.
    pub skipped_below_threshold: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathTuningStats {
    /// Field $name.
    pub eager_flush_threshold_bytes: usize,
    /// Field $name.
    pub eager_flush_drain_chunk_bytes: usize,
    /// Field $name.
    pub payload_put_concurrency: usize,
    /// Field $name.
    pub sync_group_min_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsFetchStats {
    /// Field $name.
    pub cache_hits: u64,
    /// Field $name.
    pub s3_fetches: u64,
    /// Field $name.
    pub s3_errors: u64,
    /// Field $name.
    pub singleflight_joins: u64,
    /// Field $name.
    pub retries: u64,
    /// Field $name.
    pub bytes_downloaded: u64,
    /// Field $name.
    pub prefetch_skips: u64,
    /// Field $name.
    pub local_cache_io_calls: u64,
    /// Field $name.
    pub local_cache_handoff_total_ms: f64,
    /// Field $name.
    pub local_cache_handoff_avg_ms: f64,
    /// Field $name.
    pub local_cache_handoff_max_ms: f64,
    /// Field $name.
    pub local_cache_backend_io_total_ms: f64,
    /// Field $name.
    pub local_cache_backend_io_avg_ms: f64,
    /// Field $name.
    pub local_cache_backend_io_max_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub s3_get_calls: u64,
    #[serde(default)]
    /// Field $name.
    pub s3_get_total_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub s3_get_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub s3_get_max_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub object_decode_calls: u64,
    #[serde(default)]
    /// Field $name.
    pub object_decode_total_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub object_decode_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub object_decode_max_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub assemble_copy_calls: u64,
    #[serde(default)]
    /// Field $name.
    pub assemble_copy_total_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub assemble_copy_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub assemble_copy_max_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub local_cache_put_calls: u64,
    #[serde(default)]
    /// Field $name.
    pub local_cache_put_total_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub local_cache_put_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub local_cache_put_max_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub fetch_total_calls: u64,
    #[serde(default)]
    /// Field $name.
    pub fetch_total_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub fetch_total_avg_ms: f64,
    #[serde(default)]
    /// Field $name.
    pub fetch_total_max_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsPortableAllocatorStats {
    /// Field $name.
    pub current_bytes: u64,
    /// Field $name.
    pub peak_bytes: u64,
    /// Field $name.
    pub allocated_total_bytes: u64,
    /// Field $name.
    pub deallocated_total_bytes: u64,
    /// Field $name.
    pub allocation_count: u64,
    /// Field $name.
    pub deallocation_count: u64,
    /// Field $name.
    pub reallocation_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
/// Struct $name.
pub struct VfsWritePathObservabilitySnapshot {
    #[serde(default)]
    /// Field $name.
    pub publisher_pid: u32,
    /// Field $name.
    pub generated_unix_ms: u64,
    /// Field $name.
    pub uptime_secs: f64,
    #[serde(default)]
    /// Field $name.
    pub pending_uploads: usize,
    /// Field $name.
    pub backend_tag: String,
    /// Field $name.
    pub flush_barrier_calls: u64,
    /// Field $name.
    pub flush_barrier_calls_per_sec: f64,
    /// Field $name.
    pub payload_store_sync_data: VfsWritePathSyncDataStats,
    #[serde(default)]
    /// Field $name.
    pub local_cache_io: VfsWritePathLocalCacheIoStats,
    /// Field $name.
    pub winfsp_callbacks: VfsWritePathWinfspCallbackStats,
    #[serde(default)]
    /// Field $name.
    pub winfsp_write_path: VfsWritePathWinfspWritePathStats,
    #[serde(default)]
    /// Field $name.
    pub vfs_write_path: VfsWritePathVfsWriteStats,
    #[serde(default)]
    /// Field $name.
    pub landing_zone: VfsWritePathLandingZoneStats,
    #[serde(default)]
    /// Field $name.
    pub drain_lag: VfsWritePathDrainLagStats,
    #[serde(default)]
    /// Field $name.
    pub winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats,
    /// Field $name.
    pub write_backpressure: VfsWritePathBackpressureStats,
    #[serde(default)]
    /// Field $name.
    pub dirty_pressure: VfsWritePathDirtyPressureStats,
    #[serde(default)]
    /// Field $name.
    pub upload: VfsUploadStats,
    #[serde(default)]
    /// Field $name.
    pub fetch: VfsFetchStats,
    #[serde(default)]
    /// Field $name.
    pub payload_burst: VfsWritePathBurstStats,
    #[serde(default)]
    /// Field $name.
    pub payload_persist: VfsWritePathStageLatencyStats,
    #[serde(default)]
    /// Field $name.
    pub metadata_commit: VfsWritePathStageLatencyStats,
    #[serde(default)]
    /// Field $name.
    pub metadata_commit_cadence: VfsWritePathCadenceStats,
    #[serde(default)]
    /// Field $name.
    pub eager_flush: VfsWritePathEagerFlushStats,
    #[serde(default)]
    /// Field $name.
    pub tuning: VfsWritePathTuningStats,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub allocator: Option<VfsPortableAllocatorStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enum $name.
pub enum VfsIngressLane {
    /// Variant $name.
    DirtyMem,
    /// Variant $name.
    Backpressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Struct $name.
pub struct VfsIngressPressure {
    /// Field $name.
    pub next_bytes: usize,
    /// Field $name.
    pub dirty_mem_bytes: usize,
    /// Field $name.
    pub dirty_mem_limit: usize,
    /// Field $name.
    pub lane: VfsIngressLane,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
/// Struct $name.
pub struct VfsUploadStats {
    /// Field $name.
    pub pending_uploads: usize,
    /// Field $name.
    pub pending_upload_bytes: u64,
    /// Field $name.
    pub oldest_pending_upload_age_ms: u64,
    /// Field $name.
    pub active_uploads: usize,
    /// Field $name.
    pub pending_retry_count: u64,
    /// Field $name.
    pub max_pending_retry_count: u32,
    /// Field $name.
    pub completed_uploads: u64,
    /// Field $name.
    pub completed_upload_bytes: u64,
    /// Field $name.
    pub completed_uploads_per_sec: f64,
    /// Field $name.
    pub completed_upload_bytes_per_sec: f64,
    /// Field $name.
    pub upload_requeues: u64,
    /// Field $name.
    pub canceled_deleted: u64,
    /// Field $name.
    pub canceled_stale: u64,
    /// Field $name.
    pub canceled_stale_before_put: u64,
    /// Field $name.
    pub canceled_stale_after_put: u64,
    /// Field $name.
    pub canceled_stale_after_put_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsZeroIoCacheBytesSnapshot {
    /// Field $name.
    pub total_local_bytes: u64,
    /// Field $name.
    pub backend_evictable_bytes: u64,
    /// Field $name.
    pub backend_non_evictable_bytes: u64,
    /// Field $name.
    pub pinned_unpublished_bytes: u64,
    /// Field $name.
    pub publish_ready_retained_bytes: u64,
    /// Field $name.
    pub published_warm_bytes: u64,
    /// Field $name.
    pub deleted_before_publish_bytes: u64,
    /// Field $name.
    pub unclassified_non_evictable_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsZeroIoStartupReplaySnapshot {
    /// Field $name.
    pub generated_unix_ms: u64,
    /// Field $name.
    pub replayed_epochs: u64,
    /// Field $name.
    pub replayed_blocks: u64,
    /// Field $name.
    pub replayed_bytes: u64,
    /// Field $name.
    pub queued_uploads: u64,
    /// Field $name.
    pub queued_publish_ready_epochs: u64,
    /// Field $name.
    pub quarantined_inodes: u64,
    /// Field $name.
    pub orphan_dirty_payloads_removed: u64,
    /// Field $name.
    pub unreferenced_local_payloads_removed: u64,
    /// Startup cannot inspect bytes that lived only in a crashed process.
    /// Non-zero values require future persistent shutdown/crash evidence.
    pub discarded_memory_only_bytes: u64,
    /// Field $name.
    pub replay_failures: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsZeroIoShutdownSnapshot {
    /// Field $name.
    pub generated_unix_ms: u64,
    /// Field $name.
    pub accepted_local_dirty_bytes_before: u64,
    /// Field $name.
    pub accepted_local_dirty_bytes_after: u64,
    /// Field $name.
    pub pending_uploads_after: usize,
    /// Field $name.
    pub uploader_drained_before_cancel: bool,
    /// Field $name.
    pub metadata_flush_outcome: String,
    /// Field $name.
    pub local_payload_sync_ok: bool,
    /// Field $name.
    pub recoverable_on_restart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsZeroIoHealthSignal {
    /// Field $name.
    pub severity: String,
    /// Field $name.
    pub code: String,
    /// Field $name.
    pub summary: String,
    #[serde(default)]
    /// Field $name.
    pub ino: Option<u64>,
    #[serde(default)]
    /// Field $name.
    pub content_epoch: Option<u64>,
    #[serde(default)]
    /// Field $name.
    pub path: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsZeroIoDrilldownEntry {
    /// Field $name.
    pub ino: u64,
    #[serde(default)]
    /// Field $name.
    pub path: Option<String>,
    /// Field $name.
    pub lifecycle_state: InodeLifecycleState,
    /// Field $name.
    pub retention: PayloadRetentionClass,
    /// Field $name.
    pub local_content_epoch: u64,
    /// Field $name.
    pub published_content_epoch: u64,
    /// Field $name.
    pub local_visible_size: u64,
    /// Field $name.
    pub published_size: u64,
    /// Field $name.
    pub live_extent_count: usize,
    /// Field $name.
    pub pending_upload_count: usize,
    /// Field $name.
    pub pending_upload_bytes: u64,
    /// Field $name.
    pub oldest_pending_upload_age_ms: u64,
    /// Field $name.
    pub publish_ready_queued: bool,
    /// Field $name.
    pub replayed_after_restart: bool,
    /// Field $name.
    pub repeated_replay_count: u64,
    /// Field $name.
    pub deferred_cleanup_object_count: usize,
    /// Field $name.
    pub deferred_cleanup_bytes: u64,
    #[serde(default)]
    /// Field $name.
    pub sticky_error: Option<String>,
    /// Field $name.
    pub status: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsZeroIoRuntimeSnapshot {
    /// Field $name.
    pub generated_unix_ms: u64,
    /// Field $name.
    pub unpublished_files: usize,
    /// Field $name.
    pub unpublished_epochs: usize,
    /// Field $name.
    pub unpublished_payload_bytes: u64,
    /// Field $name.
    pub uploading_epochs: usize,
    /// Field $name.
    pub replay_backlog_epochs: usize,
    /// Field $name.
    pub replay_backlog_bytes: u64,
    /// Field $name.
    pub publish_ready_backlog_epochs: usize,
    /// Field $name.
    pub replayed_after_restart_epochs: u64,
    /// Field $name.
    pub deferred_delete_backlog_epochs: usize,
    /// Field $name.
    pub deferred_delete_backlog_objects: usize,
    /// Field $name.
    pub deferred_delete_backlog_bytes: u64,
    /// Field $name.
    pub replay_failures: u64,
    /// Field $name.
    pub publish_failures: u64,
    /// Field $name.
    pub cache_bytes: VfsZeroIoCacheBytesSnapshot,
    #[serde(default)]
    /// Field $name.
    pub startup_replay: Option<VfsZeroIoStartupReplaySnapshot>,
    #[serde(default)]
    /// Field $name.
    pub last_graceful_shutdown: Option<VfsZeroIoShutdownSnapshot>,
    #[serde(default)]
    /// Field $name.
    pub health_signals: Vec<VfsZeroIoHealthSignal>,
    /// Field $name.
    pub drilldown_truncated: usize,
    #[serde(default)]
    /// Field $name.
    pub drilldown: Vec<VfsZeroIoDrilldownEntry>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsForegroundReadProtectionStats {
    /// Field $name.
    pub p90_latency_ns: Option<u64>,
    /// Field $name.
    pub throughput_bytes_per_sec: f64,
    /// Field $name.
    pub speedometer_samples: usize,
    /// Field $name.
    pub window_bytes: u64,
    /// Field $name.
    pub window_coverage_ms: u64,
    /// Field $name.
    pub dominant_access_class: Option<String>,
    /// Field $name.
    pub dominant_prefetch_enabled: bool,
    /// Field $name.
    pub dominant_cache_promotion_on_read: bool,
    /// Field $name.
    pub dominant_readahead_base_width: u32,
    /// Field $name.
    pub dominant_readahead_max_width: u32,
    /// Field $name.
    pub dominant_readahead_min_prefetch: u32,
    /// Field $name.
    pub dominant_upload_pressure_boost: u8,
    /// Field $name.
    pub active_sequential_media_reads: u64,
    /// Field $name.
    pub active_random_interactive_reads: u64,
    /// Field $name.
    pub active_sequential_writers: u64,
    /// Field $name.
    pub active_random_writers: u64,
    /// Field $name.
    pub upload_local_payload_reads: u64,
    /// Field $name.
    pub upload_local_payload_read_bytes: u64,
    /// Field $name.
    pub upload_fallback_deferrals: u64,
    /// Field $name.
    pub upload_fallback_rereads: u64,
    /// Field $name.
    pub upload_fallback_reread_bytes: u64,
    /// Field $name.
    pub upload_fallback_reread_cache_misses: u64,
    /// Field $name.
    pub upload_fallback_wait_ns: u64,
    /// Field $name.
    pub upload_fallback_wait_max_ns: u64,
    /// Field $name.
    pub upload_fallback_read_ns: u64,
    /// Field $name.
    pub upload_fallback_read_max_ns: u64,
    /// Field $name.
    pub upload_cache_io_foreground_cost: u32,
    /// Field $name.
    pub upload_scheduler_penalty_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsNamespaceCacheSnapshot {
    /// Field $name.
    pub hits: u64,
    /// Field $name.
    pub protected_hits: u64,
    /// Field $name.
    pub misses: u64,
    /// Field $name.
    pub negative_hits: u64,
    /// Field $name.
    pub evictions: u64,
    /// Field $name.
    pub negative_hit_ratio: f64,
    /// Field $name.
    pub hot_hit_ratio: f64,
    /// Field $name.
    pub occupancy_entries: usize,
    /// Field $name.
    pub occupancy_bytes: usize,
    /// Field $name.
    pub protected_occupancy_entries: usize,
    /// Field $name.
    pub protected_occupancy_bytes: usize,
    /// Field $name.
    pub entry_budget: usize,
    /// Field $name.
    pub byte_budget: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsAttrReadCacheSnapshot {
    /// Field $name.
    pub hits: u64,
    /// Field $name.
    pub protected_hits: u64,
    /// Field $name.
    pub misses: u64,
    /// Field $name.
    pub evictions: u64,
    /// Field $name.
    pub hot_hit_ratio: f64,
    /// Field $name.
    pub occupancy_entries: usize,
    /// Field $name.
    pub occupancy_bytes: usize,
    /// Field $name.
    pub protected_occupancy_entries: usize,
    /// Field $name.
    pub protected_occupancy_bytes: usize,
    /// Field $name.
    pub entry_budget: usize,
    /// Field $name.
    pub byte_budget: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsTransitMemorySnapshot {
    /// Field $name.
    pub budget_bytes: usize,
    /// Field $name.
    pub used_bytes: usize,
    /// Field $name.
    pub available_bytes: usize,
    /// Field $name.
    pub waiting_foreground: usize,
    /// Field $name.
    pub waiting_background: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsCacheStateSnapshot {
    /// Field $name.
    pub dentry_cache: VfsNamespaceCacheSnapshot,
    /// Field $name.
    pub path_shortcut_cache: VfsNamespaceCacheSnapshot,
    /// Field $name.
    pub attr_read_cache: VfsAttrReadCacheSnapshot,
    /// Field $name.
    pub local_backend_dirty_objects: usize,
    /// Field $name.
    pub prefetch_queue_depth: u64,
    /// Field $name.
    pub transit_memory: VfsTransitMemorySnapshot,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsSynchronizationStateSnapshot {
    /// Field $name.
    pub file_sessions: VfsFileSessionStats,
    /// Field $name.
    pub foreground_reads: VfsForegroundReadProtectionStats,
    /// Field $name.
    pub pending_metadata_ops: u32,
    /// Field $name.
    pub inflight_metadata_ops: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Struct $name.
pub struct VfsRuntimeEvent {
    /// Field $name.
    pub sequence: u64,
    /// Field $name.
    pub generated_unix_ms: u64,
    /// Field $name.
    pub subsystem: String,
    /// Field $name.
    pub kind: String,
    /// Field $name.
    pub title: String,
    #[serde(default)]
    /// Field $name.
    pub fields: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirty_pressure_serializes_slice_counts_for_mounted_reports() {
        let serialized = serde_json::to_value(VfsWritePathDirtyPressureStats::default())
            .expect("serialize dirty pressure");

        assert!(serialized["dirty_slice_count"].is_number());
        assert!(serialized["frozen_slice_count"].is_number());
    }
}

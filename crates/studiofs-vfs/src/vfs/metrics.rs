use super::file_session::VfsFileSessionStats;
use serde::{Deserialize, Serialize};
use studiofs_meta::schema::{InodeLifecycleState, PayloadRetentionClass};

/// Filesystem statistics returned by `statfs()`.
#[derive(Debug, Clone)]
pub struct StatFs {
    /// Total number of allocation units in the filesystem.
    pub blocks: u64,
    /// Number of free allocation units.
    pub bfree: u64,
    /// Allocation units available to unprivileged callers.
    pub bavail: u64,
    /// Total inode count available to the filesystem.
    pub files: u64,
    /// Number of free inodes.
    pub ffree: u64,
    /// Preferred filesystem block size in bytes.
    pub bsize: u32,
    /// Maximum filename length in bytes.
    pub namelen: u32,
    /// Fundamental filesystem block size in bytes.
    pub frsize: u32,
}

/// Ingress pressure counters for the write path.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsIngressStats {
    /// Dirty memory currently tracked by the ingress pipeline, in bytes.
    pub dirty_mem_bytes: usize,
    /// Number of dirty memory blocks currently tracked.
    pub dirty_mem_blocks: usize,
    /// Cumulative wait time in milliseconds caused by write backpressure.
    pub write_backpressure_wait_ms: u64,
    /// Number of write backpressure wait events.
    pub write_backpressure_events: u64,
    /// Cumulative wait time in milliseconds caused by fast backpressure.
    pub fast_backpressure_wait_ms: u64,
    /// Number of fast backpressure wait events.
    pub fast_backpressure_events: u64,
    /// Maximum observed fast backpressure stall duration in milliseconds.
    pub fast_backpressure_stall_max_ms: u64,
    /// Current prefetch queue depth.
    pub prefetch_queue_depth: u64,
}

/// Write-path latency quantile measurements in milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLatencyQuantilesMs {
    /// 50th percentile value.
    pub p50: f64,
    /// 95th percentile value.
    pub p95: f64,
    /// 99th percentile value.
    pub p99: f64,
}

/// Flags indicating whether quantile values are saturated.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLatencyQuantilesSaturated {
    /// Indicates saturation for `p50`.
    pub p50: bool,
    /// Indicates saturation for `p95`.
    pub p95: bool,
    /// Indicates saturation for `p99`.
    pub p99: bool,
}

/// Histogram snapshot with bucket bounds and counts.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsLatencyHistogramSnapshot {
    /// Upper bounds (nanoseconds) for histogram buckets.
    pub bucket_upper_bounds_ns: Vec<u64>,
    /// Count values for each bucket.
    pub bucket_counts: Vec<u64>,
}

/// Aggregated sync-data statistics for the write path.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathSyncDataStats {
    /// Total sync-data calls.
    pub calls: u64,
    /// Sync-data call rate in calls per second.
    pub calls_per_sec: f64,
    /// Number of deferred sync calls.
    pub deferred_calls: u64,
    /// Total synced bytes.
    pub synced_bytes: u64,
    /// Total synced bytes in GiB.
    pub synced_gib: f64,
    /// Calls per GiB value.
    pub calls_per_gib: f64,
    /// Total sync time in milliseconds.
    pub total_ms: f64,
    /// Average sync time in milliseconds.
    pub avg_ms: f64,
    /// Maximum sync time in milliseconds.
    pub max_ms: f64,
    /// Latency quantiles in milliseconds.
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for latency quantiles.
    #[serde(default)]
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Distribution histogram of sync latencies.
    pub latency_histogram: VfsLatencyHistogramSnapshot,
    /// Total queue wait time in milliseconds.
    #[serde(default)]
    pub queue_wait_total_ms: f64,
    /// Average queue wait time in milliseconds.
    #[serde(default)]
    pub queue_wait_avg_ms: f64,
    /// Maximum queue wait time in milliseconds.
    #[serde(default)]
    pub queue_wait_max_ms: f64,
    /// Queue latency quantiles in milliseconds.
    #[serde(default)]
    pub queue_wait_latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for queue quantiles.
    #[serde(default)]
    pub queue_wait_latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Histogram of queue wait latencies.
    #[serde(default)]
    pub queue_wait_latency_histogram: VfsLatencyHistogramSnapshot,
    /// Average barrier interval in milliseconds.
    #[serde(default)]
    pub barrier_interval_avg_ms: f64,
    /// Maximum barrier interval in milliseconds.
    #[serde(default)]
    pub barrier_interval_max_ms: f64,
    /// Barrier interval quantiles in milliseconds.
    #[serde(default)]
    pub barrier_interval_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for barrier interval quantiles.
    #[serde(default)]
    pub barrier_interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Histogram of barrier intervals.
    #[serde(default)]
    pub barrier_interval_histogram: VfsLatencyHistogramSnapshot,
}

/// WinFSP callback counters for the write path.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspCallbackStats {
    /// Number of write callbacks.
    pub write: u64,
    /// Number of overwrite callbacks.
    #[serde(default)]
    pub overwrite: u64,
    /// Number of cleanup callbacks.
    #[serde(default)]
    pub cleanup: u64,
    /// Number of flush callbacks.
    pub flush: u64,
    /// Number of close callbacks.
    #[serde(default)]
    pub close: u64,
    /// Number of cleanup+flush handle pairs.
    #[serde(default)]
    pub cleanup_flush_fh: u64,
}

/// WinFSP write-path stage metrics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspWritePathStats {
    /// Total write bytes handled by WinFSP callbacks.
    pub write_bytes: u64,
    /// Average write size in bytes.
    pub avg_write_bytes: f64,
    /// Number of strict write calls.
    #[serde(default)]
    pub strict_calls: u64,
    /// Bytes written by strict mode writes.
    #[serde(default)]
    pub strict_bytes: u64,
    /// Number of write-through calls.
    #[serde(default)]
    pub write_through_calls: u64,
    /// Bytes written by write-through calls.
    #[serde(default)]
    pub write_through_bytes: u64,
    /// Number of opportunistic write calls.
    #[serde(default)]
    pub opportunistic_calls: u64,
    /// Bytes written opportunistically.
    #[serde(default)]
    pub opportunistic_bytes: u64,
    /// Callback-stage latency metrics.
    #[serde(default)]
    pub callback: VfsWritePathStageLatencyStats,
    /// VFS write-stage latency metrics.
    #[serde(default)]
    pub vfs_write: VfsWritePathStageLatencyStats,
    /// Post-write getattr latency metrics.
    #[serde(default)]
    pub post_write_getattr: VfsWritePathStageLatencyStats,
}

/// VFS write-stage latency and throughput metrics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathVfsWriteStats {
    /// Total VFS write calls.
    pub calls: u64,
    /// Total bytes written in VFS path.
    pub write_bytes: u64,
    /// Average write size in bytes.
    pub avg_write_bytes: f64,
    /// Writer stack state recording stage metrics.
    #[serde(default)]
    pub record_writer_stack_state: VfsWritePathStageLatencyStats,
    /// Materialization completion stage metrics.
    #[serde(default)]
    pub materialize_completed: VfsWritePathStageLatencyStats,
    /// Strict materialization stage metrics.
    #[serde(default)]
    pub materialize_strict: VfsWritePathStageLatencyStats,
    /// Materialization-for-limits stage metrics.
    #[serde(default)]
    pub materialize_for_limits: VfsWritePathStageLatencyStats,
}

/// Landing-zone statistics for accepted writes and sync points.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLandingZoneStats {
    /// Number of accepted write operations.
    pub accepted_writes: u64,
    /// Total bytes accepted.
    pub accepted_bytes: u64,
    /// Accepted throughput in MiB/s.
    #[serde(default)]
    pub accepted_mib_per_sec: f64,
    /// Number of currently recoverable segments.
    #[serde(default)]
    pub recoverable_segments: usize,
    /// Total accept time in milliseconds.
    #[serde(default)]
    pub accept_total_ms: f64,
    /// Average accept time in milliseconds.
    #[serde(default)]
    pub accept_avg_ms: f64,
    /// Maximum accept time in milliseconds.
    #[serde(default)]
    pub accept_max_ms: f64,
    /// Count of data sync operations.
    #[serde(default)]
    pub data_syncs: u64,
    /// Count of manifest sync operations.
    #[serde(default)]
    pub manifest_syncs: u64,
    /// Count of manifest rewrites.
    #[serde(default)]
    pub manifest_rewrites: u64,
    /// Total sync time in milliseconds.
    #[serde(default)]
    pub sync_total_ms: f64,
    /// Average sync time in milliseconds.
    #[serde(default)]
    pub sync_avg_ms: f64,
    /// Maximum sync time in milliseconds.
    #[serde(default)]
    pub sync_max_ms: f64,
}

/// Drain lag and pacing details for write admission.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathDrainLagStats {
    /// Current lag in bytes.
    pub current_bytes: usize,
    /// Soft lag quota in bytes.
    pub steady_quota_bytes: usize,
    /// Count of soft pacing events.
    pub soft_pacing_events: u64,
    /// Total soft pacing wait time in milliseconds.
    pub soft_pacing_total_ms: u64,
    /// Maximum soft pacing wait in milliseconds.
    pub soft_pacing_max_ms: u64,
    /// Ingress bytes admitted per second.
    #[serde(default)]
    pub admission_ingress_bytes_per_sec: u64,
    /// Drain throughput in bytes per second.
    #[serde(default)]
    pub admission_drain_bytes_per_sec: u64,
    /// Remaining runway in milliseconds.
    #[serde(default)]
    pub admission_runway_ms: u64,
    /// Last admission delay in milliseconds.
    #[serde(default)]
    pub admission_last_delay_ms: u64,
    /// Current admission pressure in bytes.
    #[serde(default)]
    pub admission_pressure_bytes: usize,
    /// Current admission budget in bytes.
    #[serde(default)]
    pub admission_budget_bytes: usize,
    /// Unsynced bytes in local sync state.
    #[serde(default)]
    pub local_sync_unsynced_bytes: usize,
}

/// Foreground flush counters for cleanup/flush/close callbacks.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspForegroundFlushStats {
    /// Cleanup flush foreground calls.
    pub cleanup: u64,
    /// Flush foreground calls.
    pub flush: u64,
    /// Close foreground calls.
    pub close: u64,
    /// Clean cleanup callbacks skipped by guard logic.
    #[serde(default)]
    pub cleanup_skipped_clean: u64,
    /// Clean flush callbacks skipped by guard logic.
    #[serde(default)]
    pub flush_skipped_clean: u64,
    /// Clean close callbacks skipped by guard logic.
    #[serde(default)]
    pub close_skipped_clean: u64,
}

/// Local cache I/O latency and histogram breakdown.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLocalCacheIoStats {
    /// Execution model name used by local cache path.
    pub execution_model: String,
    /// Total local cache I/O calls.
    pub calls: u64,
    /// Total handoff time in milliseconds.
    pub handoff_total_ms: f64,
    /// Average handoff time in milliseconds.
    pub handoff_avg_ms: f64,
    /// Maximum handoff time in milliseconds.
    pub handoff_max_ms: f64,
    /// Total backend I/O time in milliseconds.
    pub backend_io_total_ms: f64,
    /// Average backend I/O time in milliseconds.
    pub backend_io_avg_ms: f64,
    /// Maximum backend I/O time in milliseconds.
    pub backend_io_max_ms: f64,
    /// Handoff latency histogram.
    pub handoff_histogram: VfsLatencyHistogramSnapshot,
    /// Backend I/O latency histogram.
    pub backend_io_histogram: VfsLatencyHistogramSnapshot,
}

/// Backpressure counters and stall metrics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathBackpressureStats {
    /// Backpressure event count.
    pub events: u64,
    /// Total backpressure wait time in milliseconds.
    pub wait_total_ms: u64,
    /// Maximum backpressure stall in milliseconds.
    pub stall_max_ms: u64,
    /// Number of soft throttle events.
    #[serde(default)]
    pub soft_throttle_events: u64,
    /// Number of hard throttle events.
    #[serde(default)]
    pub hard_events: u64,
}

/// Dirty-pressure metrics in write path memory and upload buffers.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathDirtyPressureStats {
    /// Pending local dirty bytes.
    pub pending_local_dirty_bytes: usize,
    /// Pending persisted slice bytes.
    #[serde(default)]
    pub pending_persisted_slice_bytes: usize,
    /// Dirty slice count.
    #[serde(default)]
    pub dirty_slice_count: usize,
    /// Frozen dirty slice count.
    #[serde(default)]
    pub frozen_slice_count: usize,
    /// Dirty bytes on writer stack.
    pub writer_stack_dirty_bytes: usize,
    /// Total dirty bytes.
    pub total_dirty_bytes: usize,
    /// Dirty bytes limit in bytes.
    pub dirty_limit_bytes: usize,
    /// Pending upload bytes.
    #[serde(default)]
    pub pending_upload_bytes: usize,
    /// Cache pressure bytes.
    #[serde(default)]
    pub cache_pressure_bytes: usize,
    /// Total accounted bytes.
    #[serde(default)]
    pub total_accounted_bytes: usize,
    /// Peak dirty bytes observed.
    #[serde(default)]
    pub dirty_peak_bytes: usize,
    /// Peak staged bytes observed.
    #[serde(default)]
    pub staged_peak_bytes: usize,
}

/// Burst telemetry for grouped payload writes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathBurstStats {
    /// Number of burst events.
    pub bursts: u64,
    /// Burst count per second.
    pub bursts_per_sec: f64,
    /// Total payload count across bursts.
    pub total_payloads: u64,
    /// Total bytes across bursts.
    pub total_bytes: u64,
    /// Average payload count per burst.
    pub avg_payloads_per_burst: f64,
    /// Average bytes per burst.
    pub avg_bytes_per_burst: f64,
    /// Maximum payloads in one burst.
    pub max_payloads_per_burst: u64,
    /// Maximum bytes in one burst.
    pub max_bytes_per_burst: u64,
    /// Average burst interval in milliseconds.
    pub avg_interval_ms: f64,
    /// Maximum burst interval in milliseconds.
    pub max_interval_ms: f64,
}

/// Latency statistics for a single stage in the write pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathStageLatencyStats {
    /// Total call count.
    pub calls: u64,
    /// Calls per second.
    pub calls_per_sec: f64,
    /// Total time in milliseconds.
    pub total_ms: f64,
    /// Average time in milliseconds.
    pub avg_ms: f64,
    /// Max time in milliseconds.
    pub max_ms: f64,
    /// Latency quantiles in milliseconds.
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for quantiles.
    #[serde(default)]
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Latency histogram.
    pub latency_histogram: VfsLatencyHistogramSnapshot,
}

/// Cadence metrics and interval distributions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathCadenceStats {
    /// Number of events.
    pub events: u64,
    /// Events per second.
    pub events_per_sec: f64,
    /// Average interval in milliseconds.
    pub avg_interval_ms: f64,
    /// Maximum interval in milliseconds.
    pub max_interval_ms: f64,
    /// Interval quantiles in milliseconds.
    pub interval_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    /// Saturation markers for interval quantiles.
    #[serde(default)]
    pub interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    /// Interval histogram.
    pub interval_histogram: VfsLatencyHistogramSnapshot,
}

/// Eager flush counters.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathEagerFlushStats {
    /// Total eager flush checks.
    pub checks: u64,
    /// Number of eager flushes triggered.
    pub triggered: u64,
    /// Checks skipped because queue was empty.
    pub skipped_empty: u64,
    /// Checks skipped because payload was below threshold.
    pub skipped_below_threshold: u64,
}

/// Runtime-tuning knobs reported for write path diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathTuningStats {
    /// Eager flush threshold in bytes.
    pub eager_flush_threshold_bytes: usize,
    /// Eager flush chunk size for drain in bytes.
    pub eager_flush_drain_chunk_bytes: usize,
    /// Maximum concurrent payload-put operations.
    pub payload_put_concurrency: usize,
    /// Minimum bytes required to sync a group.
    pub sync_group_min_bytes: u64,
}

/// Fetch metrics for read path operations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsFetchStats {
    /// Cache hits in fetch flow.
    pub cache_hits: u64,
    /// S3 fetch request count.
    pub s3_fetches: u64,
    /// S3 fetch error count.
    pub s3_errors: u64,
    /// Singleflight join count.
    pub singleflight_joins: u64,
    /// Retry attempt count.
    pub retries: u64,
    /// Total bytes downloaded.
    pub bytes_downloaded: u64,
    /// Prefetch skips count.
    pub prefetch_skips: u64,
    /// Local cache I/O calls count.
    pub local_cache_io_calls: u64,
    /// Total local cache handoff time in milliseconds.
    pub local_cache_handoff_total_ms: f64,
    /// Average local cache handoff time in milliseconds.
    pub local_cache_handoff_avg_ms: f64,
    /// Maximum local cache handoff time in milliseconds.
    pub local_cache_handoff_max_ms: f64,
    /// Total backend I/O time for local cache in milliseconds.
    pub local_cache_backend_io_total_ms: f64,
    /// Average backend I/O time for local cache in milliseconds.
    pub local_cache_backend_io_avg_ms: f64,
    /// Maximum backend I/O time for local cache in milliseconds.
    pub local_cache_backend_io_max_ms: f64,
    /// S3 GET call count.
    #[serde(default)]
    pub s3_get_calls: u64,
    /// Total S3 GET time in milliseconds.
    #[serde(default)]
    pub s3_get_total_ms: f64,
    /// Average S3 GET time in milliseconds.
    #[serde(default)]
    pub s3_get_avg_ms: f64,
    /// Maximum S3 GET time in milliseconds.
    #[serde(default)]
    pub s3_get_max_ms: f64,
    /// Object decode call count.
    #[serde(default)]
    pub object_decode_calls: u64,
    /// Total object decode time in milliseconds.
    #[serde(default)]
    pub object_decode_total_ms: f64,
    /// Average object decode time in milliseconds.
    #[serde(default)]
    pub object_decode_avg_ms: f64,
    /// Maximum object decode time in milliseconds.
    #[serde(default)]
    pub object_decode_max_ms: f64,
    /// Assemble/copy call count.
    #[serde(default)]
    pub assemble_copy_calls: u64,
    /// Total assemble/copy time in milliseconds.
    #[serde(default)]
    pub assemble_copy_total_ms: f64,
    /// Average assemble/copy time in milliseconds.
    #[serde(default)]
    pub assemble_copy_avg_ms: f64,
    /// Maximum assemble/copy time in milliseconds.
    #[serde(default)]
    pub assemble_copy_max_ms: f64,
    /// Local cache put call count.
    #[serde(default)]
    pub local_cache_put_calls: u64,
    /// Total local cache put time in milliseconds.
    #[serde(default)]
    pub local_cache_put_total_ms: f64,
    /// Average local cache put time in milliseconds.
    #[serde(default)]
    pub local_cache_put_avg_ms: f64,
    /// Maximum local cache put time in milliseconds.
    #[serde(default)]
    pub local_cache_put_max_ms: f64,
    /// Total fetch calls across all stages.
    #[serde(default)]
    pub fetch_total_calls: u64,
    /// Total fetch time in milliseconds.
    #[serde(default)]
    pub fetch_total_ms: f64,
    /// Average total fetch time in milliseconds.
    #[serde(default)]
    pub fetch_total_avg_ms: f64,
    /// Maximum total fetch time in milliseconds.
    #[serde(default)]
    pub fetch_total_max_ms: f64,
}

/// Portable allocator accounting snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsPortableAllocatorStats {
    /// Current bytes allocated.
    pub current_bytes: u64,
    /// Peak bytes observed.
    pub peak_bytes: u64,
    /// Total bytes allocated during process lifetime.
    pub allocated_total_bytes: u64,
    /// Total bytes deallocated during process lifetime.
    pub deallocated_total_bytes: u64,
    /// Number of allocation calls.
    pub allocation_count: u64,
    /// Number of deallocation calls.
    pub deallocation_count: u64,
    /// Number of reallocation calls.
    pub reallocation_count: u64,
}

/// Complete write-path observability snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathObservabilitySnapshot {
    /// Process identifier that produced the snapshot.
    #[serde(default)]
    pub publisher_pid: u32,
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Uptime in seconds.
    pub uptime_secs: f64,
    /// Current pending uploads count.
    #[serde(default)]
    pub pending_uploads: usize,
    /// Active backend tag.
    pub backend_tag: String,
    /// Total flush-barrier calls.
    pub flush_barrier_calls: u64,
    /// Flush-barrier call rate per second.
    pub flush_barrier_calls_per_sec: f64,
    /// Sync-data payload metrics.
    pub payload_store_sync_data: VfsWritePathSyncDataStats,
    /// Local cache I/O metrics.
    #[serde(default)]
    pub local_cache_io: VfsWritePathLocalCacheIoStats,
    /// WinFSP callback counters.
    pub winfsp_callbacks: VfsWritePathWinfspCallbackStats,
    /// WinFSP write-path stage metrics.
    #[serde(default)]
    pub winfsp_write_path: VfsWritePathWinfspWritePathStats,
    /// VFS write-stage metrics.
    #[serde(default)]
    pub vfs_write_path: VfsWritePathVfsWriteStats,
    /// Landing-zone metrics.
    #[serde(default)]
    pub landing_zone: VfsWritePathLandingZoneStats,
    /// Drain lag metrics.
    #[serde(default)]
    pub drain_lag: VfsWritePathDrainLagStats,
    /// Foreground flush counters.
    #[serde(default)]
    pub winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats,
    /// Overall write backpressure metrics.
    pub write_backpressure: VfsWritePathBackpressureStats,
    /// Dirty pressure metrics.
    #[serde(default)]
    pub dirty_pressure: VfsWritePathDirtyPressureStats,
    /// Upload metrics.
    #[serde(default)]
    pub upload: VfsUploadStats,
    /// Fetch metrics.
    #[serde(default)]
    pub fetch: VfsFetchStats,
    /// Payload burst metrics.
    #[serde(default)]
    pub payload_burst: VfsWritePathBurstStats,
    /// Payload persist stage latency.
    #[serde(default)]
    pub payload_persist: VfsWritePathStageLatencyStats,
    /// Metadata commit stage latency.
    #[serde(default)]
    pub metadata_commit: VfsWritePathStageLatencyStats,
    /// Metadata commit cadence metrics.
    #[serde(default)]
    pub metadata_commit_cadence: VfsWritePathCadenceStats,
    /// Eager-flush metrics.
    #[serde(default)]
    pub eager_flush: VfsWritePathEagerFlushStats,
    /// Tuning parameter snapshot.
    #[serde(default)]
    pub tuning: VfsWritePathTuningStats,
    /// Optional allocator snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocator: Option<VfsPortableAllocatorStats>,
}

/// Pressure lane classification for ingress scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VfsIngressLane {
    /// Dirty-memory pressure lane.
    DirtyMem,
    /// Generic backpressure lane.
    Backpressure,
}

/// Ingress pressure values used by scheduler decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VfsIngressPressure {
    /// Bytes queued in the next lane.
    pub next_bytes: usize,
    /// Dirty-memory bytes currently counted as pressure.
    pub dirty_mem_bytes: usize,
    /// Dirty-memory limit in bytes.
    pub dirty_mem_limit: usize,
    /// Lane chosen for current pressure accounting.
    pub lane: VfsIngressLane,
}

/// Upload-related counters and throughput metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct VfsUploadStats {
    /// Number of pending uploads.
    pub pending_uploads: usize,
    /// Total bytes of pending uploads.
    pub pending_upload_bytes: u64,
    /// Oldest pending upload age in milliseconds.
    pub oldest_pending_upload_age_ms: u64,
    /// Active uploads count.
    pub active_uploads: usize,
    /// Count of uploads waiting for retry.
    pub pending_retry_count: u64,
    /// Maximum pending-retry count observed.
    pub max_pending_retry_count: u32,
    /// Completed uploads count.
    pub completed_uploads: u64,
    /// Completed upload bytes.
    pub completed_upload_bytes: u64,
    /// Completed uploads per second.
    pub completed_uploads_per_sec: f64,
    /// Completed upload throughput in bytes per second.
    pub completed_upload_bytes_per_sec: f64,
    /// Number of upload requeue operations.
    pub upload_requeues: u64,
    /// Uploads canceled because underlying file was deleted.
    pub canceled_deleted: u64,
    /// Uploads canceled as stale.
    pub canceled_stale: u64,
    /// Stale uploads canceled before PUT.
    pub canceled_stale_before_put: u64,
    /// Stale uploads canceled after PUT.
    pub canceled_stale_after_put: u64,
    /// Bytes in stale uploads canceled after PUT.
    pub canceled_stale_after_put_bytes: u64,
}

/// Zero-IO cache byte accounting snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoCacheBytesSnapshot {
    /// Total local bytes in zero-IO cache.
    pub total_local_bytes: u64,
    /// Backend bytes that are evictable.
    pub backend_evictable_bytes: u64,
    /// Backend bytes that are non-evictable.
    pub backend_non_evictable_bytes: u64,
    /// Bytes pinned in unpublished state.
    pub pinned_unpublished_bytes: u64,
    /// Bytes retained in publish-ready state.
    pub publish_ready_retained_bytes: u64,
    /// Warm published bytes.
    pub published_warm_bytes: u64,
    /// Bytes deleted before publish.
    pub deleted_before_publish_bytes: u64,
    /// Unclassified non-evictable bytes.
    pub unclassified_non_evictable_bytes: u64,
}

/// Startup replay metrics for zero-IO mode.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoStartupReplaySnapshot {
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Replayed epochs count.
    pub replayed_epochs: u64,
    /// Replayed blocks count.
    pub replayed_blocks: u64,
    /// Replayed bytes count.
    pub replayed_bytes: u64,
    /// Queued uploads count after replay.
    pub queued_uploads: u64,
    /// Publish-ready queued epochs count.
    pub queued_publish_ready_epochs: u64,
    /// Quarantined inodes count.
    pub quarantined_inodes: u64,
    /// Orphan dirty payloads removed count.
    pub orphan_dirty_payloads_removed: u64,
    /// Unreferenced local payloads removed count.
    pub unreferenced_local_payloads_removed: u64,
    /// Startup cannot inspect bytes that lived only in a crashed process.
    /// Non-zero values require future persistent shutdown/crash evidence.
    pub discarded_memory_only_bytes: u64,
    /// Replay failures count.
    pub replay_failures: u64,
}

/// Shutdown snapshot for zero-IO mode.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoShutdownSnapshot {
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Accepted local dirty bytes before shutdown path.
    pub accepted_local_dirty_bytes_before: u64,
    /// Accepted local dirty bytes after shutdown path.
    pub accepted_local_dirty_bytes_after: u64,
    /// Pending uploads remaining after shutdown.
    pub pending_uploads_after: usize,
    /// Whether uploader drained before cancellation.
    pub uploader_drained_before_cancel: bool,
    /// Human-readable metadata flush outcome.
    pub metadata_flush_outcome: String,
    /// Whether local payload sync succeeded.
    pub local_payload_sync_ok: bool,
    /// Whether state is recoverable on restart.
    pub recoverable_on_restart: bool,
}

/// Health signal emitted for zero-IO diagnostics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoHealthSignal {
    /// Signal severity level.
    pub severity: String,
    /// Signal code.
    pub code: String,
    /// Short signal summary.
    pub summary: String,
    /// Optional inode identifier for the signal.
    #[serde(default)]
    pub ino: Option<u64>,
    /// Optional content epoch for the signal.
    #[serde(default)]
    pub content_epoch: Option<u64>,
    /// Optional file path for the signal.
    #[serde(default)]
    pub path: Option<String>,
}

/// Per-object drill-down item for zero-IO status.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoDrilldownEntry {
    /// Inode identifier.
    pub ino: u64,
    /// Optional human-readable path.
    #[serde(default)]
    pub path: Option<String>,
    /// Current inode lifecycle state.
    pub lifecycle_state: InodeLifecycleState,
    /// Payload retention class.
    pub retention: PayloadRetentionClass,
    /// Local content epoch.
    pub local_content_epoch: u64,
    /// Published content epoch.
    pub published_content_epoch: u64,
    /// Local visible file size.
    pub local_visible_size: u64,
    /// Published visible file size.
    pub published_size: u64,
    /// Number of live extents.
    pub live_extent_count: usize,
    /// Number of pending uploads.
    pub pending_upload_count: usize,
    /// Pending upload bytes.
    pub pending_upload_bytes: u64,
    /// Oldest pending upload age in milliseconds.
    pub oldest_pending_upload_age_ms: u64,
    /// Indicates inode is queued publish-ready.
    pub publish_ready_queued: bool,
    /// Indicates inode was replayed after restart.
    pub replayed_after_restart: bool,
    /// Repeated replay count.
    pub repeated_replay_count: u64,
    /// Deferred cleanup object count.
    pub deferred_cleanup_object_count: usize,
    /// Deferred cleanup bytes.
    pub deferred_cleanup_bytes: u64,
    /// Optional sticky error text.
    #[serde(default)]
    pub sticky_error: Option<String>,
    /// Current status string.
    pub status: String,
}

/// Runtime zero-IO snapshot with nested drill-down details.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsZeroIoRuntimeSnapshot {
    /// Snapshot generation timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Number of unpublished files.
    pub unpublished_files: usize,
    /// Number of unpublished epochs.
    pub unpublished_epochs: usize,
    /// Total unpublished payload bytes.
    pub unpublished_payload_bytes: u64,
    /// Uploading epoch count.
    pub uploading_epochs: usize,
    /// Replay backlog epoch count.
    pub replay_backlog_epochs: usize,
    /// Replay backlog bytes.
    pub replay_backlog_bytes: u64,
    /// Publish-ready backlog epoch count.
    pub publish_ready_backlog_epochs: usize,
    /// Epochs replayed after restart.
    pub replayed_after_restart_epochs: u64,
    /// Deferred delete backlog epoch count.
    pub deferred_delete_backlog_epochs: usize,
    /// Deferred delete backlog object count.
    pub deferred_delete_backlog_objects: usize,
    /// Deferred delete backlog bytes.
    pub deferred_delete_backlog_bytes: u64,
    /// Total replay failures.
    pub replay_failures: u64,
    /// Total publish failures.
    pub publish_failures: u64,
    /// Cache accounting snapshot.
    pub cache_bytes: VfsZeroIoCacheBytesSnapshot,
    /// Optional startup replay details.
    #[serde(default)]
    pub startup_replay: Option<VfsZeroIoStartupReplaySnapshot>,
    /// Optional latest graceful-shutdown snapshot.
    #[serde(default)]
    pub last_graceful_shutdown: Option<VfsZeroIoShutdownSnapshot>,
    /// Health signals present in runtime.
    #[serde(default)]
    pub health_signals: Vec<VfsZeroIoHealthSignal>,
    /// Number of truncated drilldown records.
    pub drilldown_truncated: usize,
    /// Optional drilldown entries.
    #[serde(default)]
    pub drilldown: Vec<VfsZeroIoDrilldownEntry>,
}

/// Foreground read protection metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsForegroundReadProtectionStats {
    /// 90th percentile read latency in nanoseconds.
    pub p90_latency_ns: Option<u64>,
    /// Throughput in bytes per second.
    pub throughput_bytes_per_sec: f64,
    /// Samples used by speedometer estimator.
    pub speedometer_samples: usize,
    /// Read window size in bytes.
    pub window_bytes: u64,
    /// Read window coverage in milliseconds.
    pub window_coverage_ms: u64,
    /// Dominant access class name.
    pub dominant_access_class: Option<String>,
    /// Dominant prefetch setting.
    pub dominant_prefetch_enabled: bool,
    /// Dominant cache-promotion-on-read flag.
    pub dominant_cache_promotion_on_read: bool,
    /// Dominant readahead base width.
    pub dominant_readahead_base_width: u32,
    /// Dominant readahead max width.
    pub dominant_readahead_max_width: u32,
    /// Dominant minimum prefetch size.
    pub dominant_readahead_min_prefetch: u32,
    /// Dominant upload-pressure boost.
    pub dominant_upload_pressure_boost: u8,
    /// Active sequential media reads.
    pub active_sequential_media_reads: u64,
    /// Active random interactive reads.
    pub active_random_interactive_reads: u64,
    /// Active sequential writers.
    pub active_sequential_writers: u64,
    /// Active random writers.
    pub active_random_writers: u64,
    /// Upload local payload reads count.
    pub upload_local_payload_reads: u64,
    /// Upload local payload read bytes.
    pub upload_local_payload_read_bytes: u64,
    /// Upload fallback deferrals count.
    pub upload_fallback_deferrals: u64,
    /// Upload fallback rereads count.
    pub upload_fallback_rereads: u64,
    /// Upload fallback reread bytes.
    pub upload_fallback_reread_bytes: u64,
    /// Upload fallback reread cache misses.
    pub upload_fallback_reread_cache_misses: u64,
    /// Upload fallback wait time in nanoseconds.
    pub upload_fallback_wait_ns: u64,
    /// Maximum upload fallback wait time in nanoseconds.
    pub upload_fallback_wait_max_ns: u64,
    /// Upload fallback read time in nanoseconds.
    pub upload_fallback_read_ns: u64,
    /// Maximum upload fallback read time in nanoseconds.
    pub upload_fallback_read_max_ns: u64,
    /// Foreground cache I/O cost metric.
    pub upload_cache_io_foreground_cost: u32,
    /// Scheduler penalty for upload in milliseconds.
    pub upload_scheduler_penalty_ms: u64,
}

/// Namespace cache accounting snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsNamespaceCacheSnapshot {
    /// Cache hit count.
    pub hits: u64,
    /// Protected cache hits count.
    pub protected_hits: u64,
    /// Cache miss count.
    pub misses: u64,
    /// Negative cache hit count.
    pub negative_hits: u64,
    /// Eviction count.
    pub evictions: u64,
    /// Negative hit ratio.
    pub negative_hit_ratio: f64,
    /// Hot-hit ratio.
    pub hot_hit_ratio: f64,
    /// Total entries.
    pub occupancy_entries: usize,
    /// Total bytes occupied.
    pub occupancy_bytes: usize,
    /// Protected occupancy entries.
    pub protected_occupancy_entries: usize,
    /// Protected occupancy bytes.
    pub protected_occupancy_bytes: usize,
    /// Entry budget.
    pub entry_budget: usize,
    /// Byte budget.
    pub byte_budget: usize,
}

/// Attribute cache accounting snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsAttrReadCacheSnapshot {
    /// Cache hit count.
    pub hits: u64,
    /// Protected cache hits.
    pub protected_hits: u64,
    /// Cache miss count.
    pub misses: u64,
    /// Eviction count.
    pub evictions: u64,
    /// Hot-hit ratio.
    pub hot_hit_ratio: f64,
    /// Total entries.
    pub occupancy_entries: usize,
    /// Total bytes occupied.
    pub occupancy_bytes: usize,
    /// Protected occupancy entries.
    pub protected_occupancy_entries: usize,
    /// Protected occupancy bytes.
    pub protected_occupancy_bytes: usize,
    /// Entry budget.
    pub entry_budget: usize,
    /// Byte budget.
    pub byte_budget: usize,
}

/// Transit memory accounting for ongoing work.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsTransitMemorySnapshot {
    /// Transit memory budget in bytes.
    pub budget_bytes: usize,
    /// Transit memory currently used in bytes.
    pub used_bytes: usize,
    /// Transit memory currently available in bytes.
    pub available_bytes: usize,
    /// Number of foreground operations waiting for memory.
    pub waiting_foreground: usize,
    /// Number of background operations waiting for memory.
    pub waiting_background: usize,
}

/// Cached state snapshot for the VFS.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsCacheStateSnapshot {
    /// Dentry cache metrics.
    pub dentry_cache: VfsNamespaceCacheSnapshot,
    /// Path shortcut cache metrics.
    pub path_shortcut_cache: VfsNamespaceCacheSnapshot,
    /// Attr read cache metrics.
    pub attr_read_cache: VfsAttrReadCacheSnapshot,
    /// Number of local backend dirty objects.
    pub local_backend_dirty_objects: usize,
    /// Current prefetch queue depth.
    pub prefetch_queue_depth: u64,
    /// Transit memory snapshot.
    pub transit_memory: VfsTransitMemorySnapshot,
}

/// Snapshot of synchronization state for active reads and writes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsSynchronizationStateSnapshot {
    /// Per-file session statistics.
    pub file_sessions: VfsFileSessionStats,
    /// Foreground read protection metrics.
    pub foreground_reads: VfsForegroundReadProtectionStats,
    /// Pending metadata operation count.
    pub pending_metadata_ops: u32,
    /// In-flight metadata operation count.
    pub inflight_metadata_ops: u32,
}

/// Runtime event emitted by metrics stream.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VfsRuntimeEvent {
    /// Monotonic sequence number.
    pub sequence: u64,
    /// Event timestamp in Unix milliseconds.
    pub generated_unix_ms: u64,
    /// Event subsystem.
    pub subsystem: String,
    /// Event kind.
    pub kind: String,
    /// Event title.
    pub title: String,
    /// Event fields payload.
    #[serde(default)]
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

use crate::cli::benchmark_profile_types::ProfileArtifactKind;
use serde::Serialize;
use std::collections::BTreeMap;
use studiofs_cache::metrics::CachePipelineMetricsSnapshot;
use studiofs_vfs::{
    RuntimeLatencyBucket, RuntimeStatsSection, RuntimeStatsSnapshot, VfsFetchStats, VfsUploadStats,
    VfsWritePathObservabilitySnapshot, VfsWritePathStageLatencyStats, VfsWritePathSyncDataStats,
};

#[derive(Debug, Serialize)]
pub(crate) struct OpReport {
    pub(crate) operations: u64,
    pub(crate) bytes: u64,
    pub(crate) elapsed_ms: f64,
    pub(crate) avg_latency_ms: f64,
    pub(crate) p50_ms: f64,
    pub(crate) p95_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) throughput_bytes_per_sec: f64,
    pub(crate) latency_histogram: LatencyHistogramReport,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) operations_log: Vec<OperationLogReport>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OperationLogReport {
    pub(crate) operation_index: u64,
    pub(crate) work_index: usize,
    pub(crate) worker_id: usize,
    pub(crate) target: String,
    pub(crate) size_class: String,
    pub(crate) bytes: u64,
    pub(crate) elapsed_ms: f64,
    pub(crate) throughput_bytes_per_sec: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkReport {
    pub(crate) run_id: String,
    pub(crate) mode: String,
    pub(crate) cache_dir: String,
    pub(crate) started_at: String,
    pub(crate) completed_at: String,
    pub(crate) total_elapsed_ms: f64,
    pub(crate) block_size: u64,
    pub(crate) read_size: u64,
    pub(crate) concurrency: usize,
    pub(crate) sequential_streams: bool,
    pub(crate) io_depth: usize,
    pub(crate) warmup_runs: u32,
    pub(crate) sizes: String,
    pub(crate) workload_repeat: usize,
    pub(crate) workload_files: usize,
    pub(crate) workload_small_files: usize,
    pub(crate) workload_medium_files: usize,
    pub(crate) workload_large_files: usize,
    pub(crate) access_pattern: String,
    pub(crate) l1_limit: u64,
    pub(crate) command: BenchmarkCommandMetadataReport,
    pub(crate) platform: BenchmarkPlatformMetadataReport,
    pub(crate) build: BenchmarkBuildMetadataReport,
    pub(crate) runtime_stats: BenchmarkRuntimeStatsReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) work_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mount_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) l1_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cache_pipeline: Option<CachePipelineReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) profile: Option<BenchmarkProfileReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) advanced_metrics: Option<AdvancedMetricsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) local_first_read_comparison: Option<MountedLocalFirstReadComparisonReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stream_results: Option<Vec<StreamResultReport>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mounted_memory: Option<MountedMemoryReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mounted_portable_profile: Option<MountedPortableProfileReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mounted_runtime: Option<MountedRuntimeReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mounted_observability: Option<MountedObservabilityReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mounted_write_path_window: Option<MountedWritePathWindowReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mounted_writeback_timeline: Option<MountedWritebackTimelineReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mounted_write_diagnostics: Option<MountedWriteDiagnosticsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) small_file_stats: Option<OpReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cleanup: Option<OpReport>,
    pub(crate) writes: OpReport,
    pub(crate) reads: OpReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkCommandMetadataReport {
    pub(crate) name: String,
    pub(crate) mode: String,
    pub(crate) run_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkPlatformMetadataReport {
    pub(crate) os: String,
    pub(crate) arch: String,
    pub(crate) family: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkBuildMetadataReport {
    pub(crate) package_name: String,
    pub(crate) package_version: String,
    pub(crate) debug_build: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) git_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) git_branch: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkRuntimeStatsReport {
    pub(crate) available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stats_path: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) before: Option<RuntimeStatsSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) after: Option<RuntimeStatsSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) whole_run: Option<RuntimeStatsDeltaReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) phases: Vec<RuntimeStatsPhaseDeltaReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeStatsPhaseDeltaReport {
    pub(crate) phase: String,
    pub(crate) delta: RuntimeStatsDeltaReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeStatsDeltaReport {
    pub(crate) schema_version_before: u16,
    pub(crate) schema_version_after: u16,
    pub(crate) interval_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) warnings: Vec<String>,
    pub(crate) sections: BTreeMap<String, RuntimeStatsSectionDeltaReport>,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct RuntimeStatsSectionDeltaReport {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) counters: BTreeMap<String, RuntimeStatsCounterDeltaReport>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) gauges: BTreeMap<String, RuntimeStatsGaugeDeltaReport>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) latencies: BTreeMap<String, RuntimeStatsLatencyDeltaReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeStatsCounterDeltaReport {
    pub(crate) before: u64,
    pub(crate) after: u64,
    pub(crate) delta: u64,
    pub(crate) unit: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeStatsGaugeDeltaReport {
    pub(crate) before: Option<u64>,
    pub(crate) after: u64,
    pub(crate) unit: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeStatsLatencyDeltaReport {
    pub(crate) count_before: u64,
    pub(crate) count_after: u64,
    pub(crate) count_delta: u64,
    pub(crate) sum_before: u64,
    pub(crate) sum_after: u64,
    pub(crate) sum_delta: u64,
    pub(crate) unit: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) buckets: Vec<RuntimeStatsLatencyBucketDeltaReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeStatsLatencyBucketDeltaReport {
    pub(crate) le_ns: u64,
    pub(crate) count_before: u64,
    pub(crate) count_after: u64,
    pub(crate) count_delta: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedRuntimeReport {
    pub(crate) mounted_path: String,
    pub(crate) cache_root: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata_root: Option<String>,
    pub(crate) block_size: u64,
    pub(crate) chunk_size: u64,
    pub(crate) read_size: u64,
    pub(crate) workload_shape: MountedWorkloadShapeReport,
    pub(crate) os: String,
    pub(crate) bridge_type: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedWorkloadShapeReport {
    pub(crate) sizes: String,
    pub(crate) repeat: usize,
    pub(crate) files: usize,
    pub(crate) small_files: usize,
    pub(crate) medium_files: usize,
    pub(crate) large_files: usize,
    pub(crate) access_pattern: String,
    pub(crate) concurrency: usize,
    pub(crate) sequential_streams: bool,
    pub(crate) io_depth: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedObservabilityReport {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) write_callback: Option<MountedWriteCallbackReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) writer: Option<MountedWriterStateReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cached_store: Option<MountedCachedStoreReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) upload: Option<MountedUploadReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) fetch: Option<MountedFetchReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) process: Option<MountedProcessReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) barriers: Option<MountedBarrierReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) post_upload_idle: Option<MountedPostUploadIdleReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedWriteCallbackReport {
    pub(crate) callbacks: u64,
    pub(crate) bytes: u64,
    pub(crate) throughput_bytes_per_sec: f64,
    pub(crate) latency: VfsWritePathStageLatencyStats,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedWriterStateReport {
    pub(crate) dirty_bytes: usize,
    pub(crate) dirty_slice_count: usize,
    pub(crate) frozen_slice_count: usize,
    pub(crate) staged_bytes: usize,
    pub(crate) dirty_limit_bytes: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedCachedStoreReport {
    pub(crate) staging_bytes: usize,
    pub(crate) staging_throughput_bytes_per_sec: f64,
    pub(crate) staging_latency: VfsWritePathStageLatencyStats,
    pub(crate) local_cache_backend_latency: studiofs_vfs::VfsWritePathLocalCacheIoStats,
    pub(crate) cache_counters_available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cache_counters_unavailable_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cache_hits: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cache_misses: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedUploadReport {
    pub(crate) backlog_count: usize,
    pub(crate) backlog_bytes: u64,
    pub(crate) throughput_bytes_per_sec: f64,
    pub(crate) retry_count: u64,
    pub(crate) active_uploads: usize,
    pub(crate) completed_uploads: u64,
    pub(crate) completed_upload_bytes: u64,
    pub(crate) raw: VfsUploadStats,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedFetchReport {
    pub(crate) cache_hits: u64,
    pub(crate) s3_fetches: u64,
    pub(crate) s3_errors: u64,
    pub(crate) singleflight_joins: u64,
    pub(crate) retries: u64,
    pub(crate) bytes_downloaded: u64,
    pub(crate) prefetch_skips: u64,
    pub(crate) raw: VfsFetchStats,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedProcessReport {
    pub(crate) mount_pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rss_bytes_steady_state: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rss_bytes_peak: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedBarrierReport {
    pub(crate) flush_barrier_calls: u64,
    pub(crate) cleanup_barrier_calls: u64,
    pub(crate) fsync_barrier_calls: u64,
    pub(crate) close_barrier_calls: u64,
    pub(crate) flush_fsync_close_latency: VfsWritePathSyncDataStats,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedPostUploadIdleReport {
    pub(crate) observed: bool,
    pub(crate) timed_out: bool,
    pub(crate) wait_ms: f64,
    pub(crate) pending_uploads_before: usize,
    pub(crate) pending_uploads_after: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedMemoryReport {
    pub(crate) mount_pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rss_bytes_steady_state: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rss_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dirty_bytes_steady_state: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dirty_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) staged_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dirty_limit_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MountedPortableProfileReport {
    pub(crate) process: MountedPortableProcessReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) allocator: Option<MountedPortableAllocatorReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) phases: Vec<MountedPortablePhaseProfileReport>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MountedPortablePhaseProfileReport {
    pub(crate) phase: String,
    pub(crate) process: MountedPortableProcessReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) allocator: Option<MountedPortableAllocatorReport>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MountedPortableProcessReport {
    pub(crate) mount_pid: u32,
    pub(crate) sample_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rss_bytes_steady_state: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rss_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) virtual_memory_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cpu_usage_percent_avg: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cpu_usage_percent_peak: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cpu_time_ms_delta: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) disk_read_bytes_delta: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) disk_write_bytes_delta: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MountedPortableAllocatorReport {
    pub(crate) current_bytes: u64,
    pub(crate) peak_bytes: u64,
    pub(crate) allocated_total_bytes: u64,
    pub(crate) deallocated_total_bytes: u64,
    pub(crate) allocation_count: u64,
    pub(crate) deallocation_count: u64,
    pub(crate) reallocation_count: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedLocalFirstReadComparisonReport {
    pub(crate) cache_path: String,
    pub(crate) upload_dir: String,
    pub(crate) phases: Vec<MountedLocalFirstReadPhaseReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedWritePathWindowReport {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) before: Option<VfsWritePathObservabilitySnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) after: Option<VfsWritePathObservabilitySnapshot>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedWritebackTimelineReport {
    pub(crate) bucket_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) samples: Vec<MountedWritebackTimelineSampleReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedWritebackTimelineSampleReport {
    pub(crate) elapsed_ms: u64,
    pub(crate) phase: String,
    pub(crate) foreground_write_bytes_per_sec: f64,
    pub(crate) payload_persist_bytes_per_sec: f64,
    pub(crate) upload_completed_bytes_per_sec: f64,
    pub(crate) writer_stack_dirty_bytes: u64,
    pub(crate) pending_persisted_slice_bytes: u64,
    pub(crate) pending_upload_bytes: u64,
    pub(crate) upload_queue_depth: usize,
    pub(crate) active_uploads: usize,
    pub(crate) backpressure_events_delta: u64,
    pub(crate) backpressure_wait_ms_delta: u64,
    pub(crate) soft_throttle_events_delta: u64,
    pub(crate) hard_backpressure_events_delta: u64,
    pub(crate) materialization_calls_delta: u64,
    pub(crate) materialization_total_ms_delta: f64,
    pub(crate) local_cache_io_calls_delta: u64,
    pub(crate) local_cache_backend_io_total_ms_delta: f64,
    pub(crate) local_cache_backend_io_cumulative_max_ms: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedWriteDiagnosticsReport {
    pub(crate) pass: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) blockers: Vec<String>,
    pub(crate) write_throughput_target_bytes_per_sec: f64,
    pub(crate) write_throughput_bytes_per_sec: f64,
    pub(crate) write_throughput_mib_per_sec: f64,
    pub(crate) write_throughput_target_met: bool,
    pub(crate) rss_peak_target_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rss_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rss_target_met: Option<bool>,
    pub(crate) logical_write_bytes: u64,
    pub(crate) winfsp_write_bytes_delta: u64,
    pub(crate) vfs_write_bytes_delta: u64,
    pub(crate) payload_persisted_bytes_delta: u64,
    pub(crate) payload_accounted_bytes_delta: u64,
    pub(crate) landing_accepted_bytes_delta: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) known_local_write_amplification: Option<f64>,
    pub(crate) landing_journal_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dirty_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) staged_bytes_peak: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dirty_limit_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) writer_stack_dirty_bytes_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pending_persisted_slice_bytes_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pending_upload_bytes_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cache_pressure_bytes_after: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) total_accounted_bytes_after: Option<u64>,
    pub(crate) payload_persist_calls_delta: u64,
    pub(crate) local_cache_io_calls_delta: u64,
    pub(crate) local_cache_backend_io_total_ms_delta: f64,
    pub(crate) hard_backpressure_events_delta: u64,
    pub(crate) soft_throttle_events_delta: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MountedLocalFirstReadPhaseReport {
    pub(crate) phase: String,
    pub(crate) wait_target: String,
    pub(crate) target_state_observed: bool,
    pub(crate) valid_for_target: bool,
    pub(crate) required_pending_uploads: usize,
    pub(crate) wait_ms: f64,
    pub(crate) pending_uploads_before: usize,
    pub(crate) pending_uploads_after: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) validation_message: Option<String>,
    pub(crate) results: OpReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct StreamResultReport {
    pub(crate) stream_id: usize,
    pub(crate) writes: OpReport,
    pub(crate) reads: OpReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdvancedMetricsReport {
    pub(crate) size_class_boundaries: SizeClassBoundaryReport,
    pub(crate) size_class_stats: SizeClassStatsReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cache_levels: Option<CacheLevelsReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SizeClassBoundaryReport {
    pub(crate) small_max_bytes: u64,
    pub(crate) medium_max_bytes: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct SizeClassStatsReport {
    pub(crate) writes: Vec<SizeClassOpReport>,
    pub(crate) reads: Vec<SizeClassOpReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SizeClassOpReport {
    pub(crate) class: String,
    pub(crate) operations: u64,
    pub(crate) bytes: u64,
    pub(crate) avg_latency_ms: f64,
    pub(crate) p50_ms: f64,
    pub(crate) p95_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) throughput_bytes_per_sec: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct CacheLevelsReport {
    pub(crate) l1: CacheLevelReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct CacheLevelReport {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) bytes_served: u64,
    pub(crate) throughput_bytes_per_sec: f64,
    pub(crate) avg_latency_ms: f64,
    pub(crate) p50_ms: f64,
    pub(crate) p95_ms: f64,
    pub(crate) p99_ms: f64,
    pub(crate) latency_histogram: LatencyHistogramReport,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LatencyHistogramReport {
    pub(crate) bucket_upper_bounds_ns: Vec<u64>,
    pub(crate) bucket_counts: Vec<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MaterializeBatchStatsReport {
    pub(crate) batches: u64,
    pub(crate) payloads: u64,
    pub(crate) bytes: u64,
    pub(crate) inflight_batches: usize,
    pub(crate) inflight_batches_peak: usize,
    pub(crate) duration_histogram: LatencyHistogramReport,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct UploadQueueStatsReport {
    pub(crate) pending_depth: usize,
    pub(crate) pending_depth_peak: usize,
    pub(crate) submitted: u64,
    pub(crate) completed: u64,
    pub(crate) completed_bytes: u64,
    pub(crate) requeued: u64,
    pub(crate) canceled: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct CachePipelineReport {
    pub(crate) materialize_batches: MaterializeBatchStatsReport,
    pub(crate) upload_queue: UploadQueueStatsReport,
    pub(crate) blocking_tasks: BlockingTaskStatsReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct BlockingTaskStatsReport {
    pub(crate) total: BlockingTaskSingleStatsReport,
    pub(crate) by_kind: Vec<BlockingTaskKindStatsReport>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BlockingTaskKindStatsReport {
    pub(crate) task_kind: String,
    pub(crate) stats: BlockingTaskSingleStatsReport,
}

#[derive(Debug, Serialize)]
pub(crate) struct BlockingTaskSingleStatsReport {
    pub(crate) active: usize,
    pub(crate) active_peak: usize,
    pub(crate) submitted: u64,
    pub(crate) started: u64,
    pub(crate) completed: u64,
    pub(crate) queue_wait_avg_ms: f64,
    pub(crate) queue_wait_max_ms: f64,
    pub(crate) execution_avg_ms: f64,
    pub(crate) execution_max_ms: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkProfileReport {
    pub(crate) enabled: bool,
    pub(crate) profile_mode: String,
    pub(crate) cpu_profile: CpuProfileReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint: Option<ProfileEndpointReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) artifacts: Vec<ProfileArtifactSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CpuProfileReport {
    pub(crate) total_wall_ms: f64,
    pub(crate) total_cpu_ms: f64,
    pub(crate) cpu_utilization_percent: f64,
    pub(crate) write_phase_cpu_percent: f64,
    pub(crate) read_phase_cpu_percent: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProfileEndpointReport {
    pub(crate) url: String,
    pub(crate) application_name: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProfileArtifactSummary {
    pub(crate) kind: ProfileArtifactKind,
    pub(crate) path: String,
    pub(crate) bytes: u64,
    pub(crate) sent_to_endpoint: bool,
}

pub(crate) fn benchmark_command_metadata(
    name: impl Into<String>,
    mode: impl Into<String>,
    run_id: impl Into<String>,
) -> BenchmarkCommandMetadataReport {
    BenchmarkCommandMetadataReport {
        name: name.into(),
        mode: mode.into(),
        run_id: run_id.into(),
    }
}

pub(crate) fn benchmark_platform_metadata() -> BenchmarkPlatformMetadataReport {
    BenchmarkPlatformMetadataReport {
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        family: std::env::consts::FAMILY.to_string(),
    }
}

pub(crate) fn benchmark_build_metadata() -> BenchmarkBuildMetadataReport {
    BenchmarkBuildMetadataReport {
        package_name: env!("CARGO_PKG_NAME").to_string(),
        package_version: env!("CARGO_PKG_VERSION").to_string(),
        debug_build: cfg!(debug_assertions),
        git_sha: option_env!("VERGEN_GIT_SHA")
            .or(option_env!("GIT_SHA"))
            .map(str::to_string),
        git_branch: option_env!("VERGEN_GIT_BRANCH")
            .or(option_env!("GIT_BRANCH"))
            .map(str::to_string),
    }
}

pub(crate) fn missing_runtime_stats_report(
    stats_path: Option<String>,
    warning: impl Into<String>,
) -> BenchmarkRuntimeStatsReport {
    BenchmarkRuntimeStatsReport {
        available: false,
        stats_path,
        warnings: vec![warning.into()],
        before: None,
        after: None,
        whole_run: None,
        phases: Vec::new(),
    }
}

pub(crate) fn runtime_stats_report(
    stats_path: String,
    before: RuntimeStatsSnapshot,
    after: RuntimeStatsSnapshot,
    phases: Vec<RuntimeStatsPhaseDeltaReport>,
    mut warnings: Vec<String>,
) -> BenchmarkRuntimeStatsReport {
    let whole_run = runtime_stats_delta("whole-run", &before, &after);
    warnings.extend(whole_run.warnings.iter().cloned());
    for phase in &phases {
        warnings.extend(phase.delta.warnings.iter().cloned());
    }
    BenchmarkRuntimeStatsReport {
        available: true,
        stats_path: Some(stats_path),
        warnings,
        before: Some(before),
        after: Some(after),
        whole_run: Some(whole_run),
        phases,
    }
}

pub(crate) fn runtime_stats_delta(
    label: &str,
    before: &RuntimeStatsSnapshot,
    after: &RuntimeStatsSnapshot,
) -> RuntimeStatsDeltaReport {
    let mut sections = BTreeMap::new();
    let mut warnings = Vec::new();
    for (name, before_section, after_section) in runtime_stats_sections(before, after) {
        sections.insert(
            name.to_string(),
            runtime_stats_section_delta(label, name, before_section, after_section, &mut warnings),
        );
    }
    RuntimeStatsDeltaReport {
        schema_version_before: before.schema_version,
        schema_version_after: after.schema_version,
        interval_ms: after
            .generated_unix_ms
            .saturating_sub(before.generated_unix_ms),
        warnings,
        sections,
    }
}

fn runtime_stats_sections<'a>(
    before: &'a RuntimeStatsSnapshot,
    after: &'a RuntimeStatsSnapshot,
) -> [(
    &'static str,
    &'a RuntimeStatsSection,
    &'a RuntimeStatsSection,
); 8] {
    let RuntimeStatsSnapshot {
        schema_version: _,
        generated_unix_ms: _,
        system: before_system,
        bridge: before_bridge,
        vfs_read: before_vfs_read,
        vfs_write: before_vfs_write,
        cache: before_cache,
        upload_queue: before_upload_queue,
        object_io: before_object_io,
        metadata: before_metadata,
    } = before;
    let RuntimeStatsSnapshot {
        schema_version: _,
        generated_unix_ms: _,
        system: after_system,
        bridge: after_bridge,
        vfs_read: after_vfs_read,
        vfs_write: after_vfs_write,
        cache: after_cache,
        upload_queue: after_upload_queue,
        object_io: after_object_io,
        metadata: after_metadata,
    } = after;
    [
        ("system", before_system, after_system),
        ("bridge", before_bridge, after_bridge),
        ("vfs_read", before_vfs_read, after_vfs_read),
        ("vfs_write", before_vfs_write, after_vfs_write),
        ("cache", before_cache, after_cache),
        ("upload_queue", before_upload_queue, after_upload_queue),
        ("object_io", before_object_io, after_object_io),
        ("metadata", before_metadata, after_metadata),
    ]
}

fn runtime_stats_section_delta(
    label: &str,
    section_name: &str,
    before: &RuntimeStatsSection,
    after: &RuntimeStatsSection,
    warnings: &mut Vec<String>,
) -> RuntimeStatsSectionDeltaReport {
    let mut report = RuntimeStatsSectionDeltaReport::default();
    for (name, metric) in &after.counters {
        let before_value = before.counters.get(name).map_or(0, |metric| metric.value);
        if metric.value < before_value {
            warnings.push(format!(
                "runtime stats reset detected for {section_name}.{name} during {label}; delta uses the post-reset value"
            ));
        }
        report.counters.insert(
            name.clone(),
            RuntimeStatsCounterDeltaReport {
                before: before_value,
                after: metric.value,
                delta: reset_aware_delta(before_value, metric.value),
                unit: metric.unit.to_string(),
            },
        );
    }
    for (name, metric) in &after.gauges {
        report.gauges.insert(
            name.clone(),
            RuntimeStatsGaugeDeltaReport {
                before: before.gauges.get(name).map(|metric| metric.value),
                after: metric.value,
                unit: metric.unit.to_string(),
            },
        );
    }
    for (name, histogram) in &after.latencies {
        let before_histogram = before.latencies.get(name);
        let count_before = before_histogram.map_or(0, |histogram| histogram.count);
        let sum_before = before_histogram.map_or(0, |histogram| histogram.sum);
        let histogram_reset = runtime_stats_latency_reset(
            before_histogram.map_or(&[][..], |histogram| histogram.buckets.as_slice()),
            histogram.buckets.as_slice(),
            count_before,
            histogram.count,
            sum_before,
            histogram.sum,
        );
        if histogram_reset {
            warnings.push(format!(
                "runtime stats latency reset detected for {section_name}.{name} during {label}; deltas use post-reset values"
            ));
        }
        let buckets = runtime_stats_latency_bucket_deltas(
            before_histogram.map_or(&[][..], |histogram| histogram.buckets.as_slice()),
            histogram.buckets.as_slice(),
        );
        report.latencies.insert(
            name.clone(),
            RuntimeStatsLatencyDeltaReport {
                count_before,
                count_after: histogram.count,
                count_delta: reset_aware_delta(count_before, histogram.count),
                sum_before,
                sum_after: histogram.sum,
                sum_delta: reset_aware_delta(sum_before, histogram.sum),
                unit: histogram.unit.to_string(),
                buckets,
            },
        );
    }
    report
}

fn runtime_stats_latency_bucket_deltas(
    before: &[RuntimeLatencyBucket],
    after: &[RuntimeLatencyBucket],
) -> Vec<RuntimeStatsLatencyBucketDeltaReport> {
    after
        .iter()
        .map(|bucket| {
            let count_before = runtime_stats_latency_bucket_count(before, bucket.le_ns);
            RuntimeStatsLatencyBucketDeltaReport {
                le_ns: bucket.le_ns,
                count_before,
                count_after: bucket.count,
                count_delta: reset_aware_delta(count_before, bucket.count),
            }
        })
        .collect()
}

fn runtime_stats_latency_reset(
    before: &[RuntimeLatencyBucket],
    after: &[RuntimeLatencyBucket],
    count_before: u64,
    count_after: u64,
    sum_before: u64,
    sum_after: u64,
) -> bool {
    if count_after < count_before || sum_after < sum_before {
        return true;
    }
    after.iter().any(|bucket| {
        before
            .binary_search_by_key(&bucket.le_ns, |before_bucket| before_bucket.le_ns)
            .is_ok_and(|index| bucket.count < before[index].count)
    })
}

fn runtime_stats_latency_bucket_count(before: &[RuntimeLatencyBucket], le_ns: u64) -> u64 {
    before
        .binary_search_by_key(&le_ns, |bucket| bucket.le_ns)
        .map(|index| before[index].count)
        .unwrap_or_default()
}

fn reset_aware_delta(before: u64, after: u64) -> u64 {
    if after >= before {
        after - before
    } else {
        after
    }
}

impl From<studiofs_cache::metrics::LatencyHistogramSnapshot> for LatencyHistogramReport {
    fn from(snapshot: studiofs_cache::metrics::LatencyHistogramSnapshot) -> Self {
        Self {
            bucket_upper_bounds_ns: snapshot.bucket_upper_bounds_ns,
            bucket_counts: snapshot.bucket_counts,
        }
    }
}

impl From<studiofs_cache::metrics::MaterializeBatchStatsSnapshot> for MaterializeBatchStatsReport {
    fn from(snapshot: studiofs_cache::metrics::MaterializeBatchStatsSnapshot) -> Self {
        Self {
            batches: snapshot.batches,
            payloads: snapshot.payloads,
            bytes: snapshot.bytes,
            inflight_batches: snapshot.inflight_batches,
            inflight_batches_peak: snapshot.inflight_batches_peak,
            duration_histogram: snapshot.duration_histogram.into(),
        }
    }
}

impl From<studiofs_cache::metrics::UploadQueueStatsSnapshot> for UploadQueueStatsReport {
    fn from(snapshot: studiofs_cache::metrics::UploadQueueStatsSnapshot) -> Self {
        Self {
            pending_depth: snapshot.pending_depth,
            pending_depth_peak: snapshot.pending_depth_peak,
            submitted: snapshot.submitted,
            completed: snapshot.completed,
            completed_bytes: snapshot.completed_bytes,
            requeued: snapshot.requeued,
            canceled: snapshot.canceled,
        }
    }
}

impl From<CachePipelineMetricsSnapshot> for CachePipelineReport {
    fn from(snapshot: CachePipelineMetricsSnapshot) -> Self {
        let to_report = |stats: &studiofs_cache::metrics::BlockingTaskStatsSnapshot| {
            BlockingTaskSingleStatsReport {
                active: stats.active,
                active_peak: stats.active_peak,
                submitted: stats.submitted,
                started: stats.started,
                completed: stats.completed,
                queue_wait_avg_ms: if stats.started == 0 {
                    0.0
                } else {
                    stats.queue_wait_total.as_secs_f64() * 1_000.0 / stats.started as f64
                },
                queue_wait_max_ms: stats.queue_wait_max.as_secs_f64() * 1_000.0,
                execution_avg_ms: if stats.completed == 0 {
                    0.0
                } else {
                    stats.execution_total.as_secs_f64() * 1_000.0 / stats.completed as f64
                },
                execution_max_ms: stats.execution_max.as_secs_f64() * 1_000.0,
            }
        };
        let mut by_kind: Vec<BlockingTaskKindStatsReport> = snapshot
            .blocking_tasks
            .by_kind
            .iter()
            .map(|item| BlockingTaskKindStatsReport {
                task_kind: item.task_kind.clone(),
                stats: to_report(&item.stats),
            })
            .collect();
        by_kind.sort_by_key(|item| std::cmp::Reverse(item.stats.submitted));

        Self {
            materialize_batches: snapshot.materialize_batches.into(),
            upload_queue: snapshot.upload_queue.into(),
            blocking_tasks: BlockingTaskStatsReport {
                total: to_report(&snapshot.blocking_tasks.total),
                by_kind,
            },
        }
    }
}

#[cfg(test)]
pub(crate) fn sample_report() -> BenchmarkReport {
    use super::workload::{DEFAULT_WORKLOAD_FILES, WORKLOAD_FILES_PER_TIER};
    use studiofs_meta::{DEFAULT_BLOCK_SIZE, schema::DEFAULT_CHUNK_SIZE};

    fn sample_latency_histogram() -> LatencyHistogramReport {
        LatencyHistogramReport {
            bucket_upper_bounds_ns: vec![50_000, 100_000, 250_000],
            bucket_counts: vec![2, 8, 6, 0],
        }
    }

    let workload_shape = MountedWorkloadShapeReport {
        sizes: "default".to_string(),
        repeat: 1,
        files: DEFAULT_WORKLOAD_FILES,
        small_files: WORKLOAD_FILES_PER_TIER,
        medium_files: WORKLOAD_FILES_PER_TIER,
        large_files: WORKLOAD_FILES_PER_TIER,
        access_pattern: "sequential".to_string(),
        concurrency: 1,
        sequential_streams: false,
        io_depth: 32,
    };

    BenchmarkReport {
        run_id: "run-123".to_string(),
        mode: "direct-shim".to_string(),
        cache_dir: r"D:\test-cache".to_string(),
        started_at: "2026-03-18T15:12:55Z".to_string(),
        completed_at: "2026-03-18T15:12:55Z".to_string(),
        total_elapsed_ms: 93.05,
        block_size: DEFAULT_BLOCK_SIZE as u64,
        read_size: DEFAULT_BLOCK_SIZE as u64,
        concurrency: 1,
        sequential_streams: false,
        io_depth: 32,
        warmup_runs: 0,
        sizes: "default".to_string(),
        workload_repeat: 1,
        workload_files: DEFAULT_WORKLOAD_FILES,
        workload_small_files: WORKLOAD_FILES_PER_TIER,
        workload_medium_files: WORKLOAD_FILES_PER_TIER,
        workload_large_files: WORKLOAD_FILES_PER_TIER,
        access_pattern: "sequential".to_string(),
        l1_limit: 2 * 1024 * 1024 * 1024,
        command: benchmark_command_metadata("studiofs test-cache", "direct-shim", "run-123"),
        platform: benchmark_platform_metadata(),
        build: benchmark_build_metadata(),
        runtime_stats: missing_runtime_stats_report(
            None,
            "runtime stats were not captured for this sample report",
        ),
        work_root: None,
        volume: None,
        mount_point: None,
        l1_bytes: Some(128 * 1024 * 1024),
        cache_pipeline: Some(CachePipelineReport {
            materialize_batches: MaterializeBatchStatsReport {
                batches: 4,
                payloads: 16,
                bytes: 128 * 1024 * 1024,
                inflight_batches: 0,
                inflight_batches_peak: 2,
                duration_histogram: LatencyHistogramReport {
                    bucket_upper_bounds_ns: vec![50_000],
                    bucket_counts: vec![4],
                },
            },
            upload_queue: UploadQueueStatsReport {
                pending_depth: 0,
                pending_depth_peak: 3,
                submitted: 16,
                completed: 16,
                completed_bytes: 128 * 1024 * 1024,
                requeued: 0,
                canceled: 0,
            },
            blocking_tasks: BlockingTaskStatsReport {
                total: BlockingTaskSingleStatsReport {
                    active: 0,
                    active_peak: 4,
                    submitted: 32,
                    started: 32,
                    completed: 32,
                    queue_wait_avg_ms: 0.2,
                    queue_wait_max_ms: 1.5,
                    execution_avg_ms: 3.1,
                    execution_max_ms: 9.4,
                },
                by_kind: vec![
                    BlockingTaskKindStatsReport {
                        task_kind: "shim_put_batch".to_string(),
                        stats: BlockingTaskSingleStatsReport {
                            active: 0,
                            active_peak: 3,
                            submitted: 16,
                            started: 16,
                            completed: 16,
                            queue_wait_avg_ms: 0.3,
                            queue_wait_max_ms: 1.5,
                            execution_avg_ms: 3.7,
                            execution_max_ms: 9.4,
                        },
                    },
                    BlockingTaskKindStatsReport {
                        task_kind: "shim_get_batch".to_string(),
                        stats: BlockingTaskSingleStatsReport {
                            active: 0,
                            active_peak: 2,
                            submitted: 16,
                            started: 16,
                            completed: 16,
                            queue_wait_avg_ms: 0.1,
                            queue_wait_max_ms: 0.6,
                            execution_avg_ms: 2.5,
                            execution_max_ms: 7.8,
                        },
                    },
                ],
            },
        }),
        profile: Some(BenchmarkProfileReport {
            enabled: true,
            profile_mode: "pyroscope".to_string(),
            cpu_profile: CpuProfileReport {
                total_wall_ms: 93.05,
                total_cpu_ms: 85.0,
                cpu_utilization_percent: 91.3,
                write_phase_cpu_percent: 92.0,
                read_phase_cpu_percent: 90.4,
            },
            endpoint: Some(ProfileEndpointReport {
                url: "http://pyroscope:4040".to_string(),
                application_name: "studiofs.test-cache.direct-shim".to_string(),
            }),
            artifacts: vec![ProfileArtifactSummary {
                kind: ProfileArtifactKind::Cpu,
                path: r"D:\test-cache\studiofs-benchmark-direct-shim-run-123-cpu.pprof.gz"
                    .to_string(),
                bytes: 2048,
                sent_to_endpoint: true,
            }],
            warnings: vec![],
        }),
        advanced_metrics: Some(AdvancedMetricsReport {
            size_class_boundaries: SizeClassBoundaryReport {
                small_max_bytes: 10 * 1024,
                medium_max_bytes: DEFAULT_BLOCK_SIZE as u64,
            },
            size_class_stats: SizeClassStatsReport {
                writes: vec![
                    SizeClassOpReport {
                        class: "small".to_string(),
                        operations: 6,
                        bytes: 48 * 1024,
                        avg_latency_ms: 0.8,
                        p50_ms: 0.7,
                        p95_ms: 1.1,
                        p99_ms: 1.3,
                        throughput_bytes_per_sec: 64.0 * 1024.0 * 1024.0,
                    },
                    SizeClassOpReport {
                        class: "medium".to_string(),
                        operations: 5,
                        bytes: 40 * 1024 * 1024,
                        avg_latency_ms: 4.0,
                        p50_ms: 3.8,
                        p95_ms: 6.2,
                        p99_ms: 6.8,
                        throughput_bytes_per_sec: 512.0 * 1024.0 * 1024.0,
                    },
                    SizeClassOpReport {
                        class: "large".to_string(),
                        operations: 5,
                        bytes: 88 * 1024 * 1024,
                        avg_latency_ms: 8.4,
                        p50_ms: 8.0,
                        p95_ms: 11.4,
                        p99_ms: 12.2,
                        throughput_bytes_per_sec: 768.0 * 1024.0 * 1024.0,
                    },
                ],
                reads: vec![
                    SizeClassOpReport {
                        class: "small".to_string(),
                        operations: 6,
                        bytes: 48 * 1024,
                        avg_latency_ms: 0.7,
                        p50_ms: 0.6,
                        p95_ms: 1.0,
                        p99_ms: 1.2,
                        throughput_bytes_per_sec: 70.0 * 1024.0 * 1024.0,
                    },
                    SizeClassOpReport {
                        class: "medium".to_string(),
                        operations: 5,
                        bytes: 40 * 1024 * 1024,
                        avg_latency_ms: 3.8,
                        p50_ms: 3.6,
                        p95_ms: 6.0,
                        p99_ms: 6.6,
                        throughput_bytes_per_sec: 540.0 * 1024.0 * 1024.0,
                    },
                    SizeClassOpReport {
                        class: "large".to_string(),
                        operations: 5,
                        bytes: 88 * 1024 * 1024,
                        avg_latency_ms: 8.0,
                        p50_ms: 7.8,
                        p95_ms: 11.0,
                        p99_ms: 11.8,
                        throughput_bytes_per_sec: 790.0 * 1024.0 * 1024.0,
                    },
                ],
            },
            cache_levels: Some(CacheLevelsReport {
                l1: CacheLevelReport {
                    hits: 9,
                    misses: 7,
                    bytes_served: 72 * 1024 * 1024,
                    throughput_bytes_per_sec: 774.0 * 1024.0 * 1024.0,
                    avg_latency_ms: 0.15,
                    p50_ms: 0.12,
                    p95_ms: 0.30,
                    p99_ms: 0.42,
                    latency_histogram: LatencyHistogramReport {
                        bucket_upper_bounds_ns: vec![50_000, 100_000],
                        bucket_counts: vec![3, 6, 7],
                    },
                },
            }),
        }),
        local_first_read_comparison: None,
        stream_results: None,
        mounted_memory: None,
        mounted_portable_profile: None,
        mounted_runtime: Some(MountedRuntimeReport {
            mounted_path: "/mnt/studiofs-benchmark".to_string(),
            cache_root: r"D:\test-cache\vol-123".to_string(),
            metadata_root: Some(r"D:\test-cache".to_string()),
            block_size: DEFAULT_BLOCK_SIZE as u64,
            chunk_size: DEFAULT_CHUNK_SIZE as u64,
            read_size: DEFAULT_BLOCK_SIZE as u64,
            workload_shape,
            os: std::env::consts::OS.to_string(),
            bridge_type: "winfsp".to_string(),
        }),
        mounted_observability: Some(MountedObservabilityReport {
            write_callback: Some(MountedWriteCallbackReport {
                callbacks: 16,
                bytes: 128 * 1024 * 1024,
                throughput_bytes_per_sec: 1_024.0 * 1024.0 * 1024.0,
                latency: VfsWritePathStageLatencyStats {
                    calls: 16,
                    calls_per_sec: 128.0,
                    total_ms: 80.0,
                    avg_ms: 5.0,
                    max_ms: 9.0,
                    latency_quantiles_ms: studiofs_vfs::VfsWritePathLatencyQuantilesMs {
                        p50: 4.7,
                        p95: 8.3,
                        p99: 8.8,
                    },
                    latency_histogram: studiofs_vfs::VfsLatencyHistogramSnapshot {
                        bucket_upper_bounds_ns: vec![50_000],
                        bucket_counts: vec![16, 0],
                    },
                    ..Default::default()
                },
            }),
            writer: Some(MountedWriterStateReport {
                dirty_bytes: 4 * 1024 * 1024,
                dirty_slice_count: 4,
                frozen_slice_count: 2,
                staged_bytes: 2 * 1024 * 1024,
                dirty_limit_bytes: 2 * 1024 * 1024 * 1024,
            }),
            cached_store: Some(MountedCachedStoreReport {
                staging_bytes: 2 * 1024 * 1024,
                staging_throughput_bytes_per_sec: 256.0 * 1024.0 * 1024.0,
                staging_latency: VfsWritePathStageLatencyStats {
                    calls: 4,
                    calls_per_sec: 32.0,
                    total_ms: 12.0,
                    avg_ms: 3.0,
                    max_ms: 5.0,
                    latency_histogram: studiofs_vfs::VfsLatencyHistogramSnapshot {
                        bucket_upper_bounds_ns: vec![50_000],
                        bucket_counts: vec![4, 0],
                    },
                    ..Default::default()
                },
                local_cache_backend_latency: Default::default(),
                cache_counters_available: true,
                cache_counters_unavailable_reason: None,
                cache_hits: Some(9),
                cache_misses: Some(7),
            }),
            upload: Some(MountedUploadReport {
                backlog_count: 0,
                backlog_bytes: 0,
                throughput_bytes_per_sec: 512.0 * 1024.0 * 1024.0,
                retry_count: 0,
                active_uploads: 0,
                completed_uploads: 16,
                completed_upload_bytes: 128 * 1024 * 1024,
                raw: VfsUploadStats {
                    completed_uploads: 16,
                    completed_upload_bytes: 128 * 1024 * 1024,
                    completed_upload_bytes_per_sec: 512.0 * 1024.0 * 1024.0,
                    ..Default::default()
                },
            }),
            fetch: Some(MountedFetchReport {
                cache_hits: 12,
                s3_fetches: 3,
                s3_errors: 0,
                singleflight_joins: 1,
                retries: 0,
                bytes_downloaded: 12 * 1024 * 1024,
                prefetch_skips: 2,
                raw: VfsFetchStats {
                    cache_hits: 12,
                    s3_fetches: 3,
                    singleflight_joins: 1,
                    bytes_downloaded: 12 * 1024 * 1024,
                    prefetch_skips: 2,
                    ..Default::default()
                },
            }),
            process: Some(MountedProcessReport {
                mount_pid: 4242,
                rss_bytes_steady_state: Some(1_400 * 1024 * 1024),
                rss_bytes_peak: Some(1_650 * 1024 * 1024),
            }),
            barriers: Some(MountedBarrierReport {
                flush_barrier_calls: 2,
                cleanup_barrier_calls: 1,
                fsync_barrier_calls: 1,
                close_barrier_calls: 1,
                flush_fsync_close_latency: VfsWritePathSyncDataStats {
                    calls: 2,
                    total_ms: 14.0,
                    avg_ms: 7.0,
                    max_ms: 9.0,
                    latency_histogram: studiofs_vfs::VfsLatencyHistogramSnapshot {
                        bucket_upper_bounds_ns: vec![50_000],
                        bucket_counts: vec![2, 0],
                    },
                    ..Default::default()
                },
            }),
            post_upload_idle: Some(MountedPostUploadIdleReport {
                observed: true,
                timed_out: false,
                wait_ms: 240.0,
                pending_uploads_before: 0,
                pending_uploads_after: 0,
                reason: None,
            }),
        }),
        mounted_write_path_window: None,
        mounted_writeback_timeline: None,
        mounted_write_diagnostics: None,
        small_file_stats: None,
        cleanup: None,
        writes: OpReport {
            operations: 16,
            bytes: 128 * 1024 * 1024,
            elapsed_ms: 80.0,
            avg_latency_ms: 5.0,
            p50_ms: 4.7,
            p95_ms: 8.3,
            p99_ms: 8.8,
            throughput_bytes_per_sec: 1_375.53 * 1024.0 * 1024.0,
            latency_histogram: sample_latency_histogram(),
            operations_log: Vec::new(),
        },
        reads: OpReport {
            operations: 16,
            bytes: 128 * 1024 * 1024,
            elapsed_ms: 78.0,
            avg_latency_ms: 4.8,
            p50_ms: 4.5,
            p95_ms: 8.1,
            p99_ms: 8.6,
            throughput_bytes_per_sec: 1_375.53 * 1024.0 * 1024.0,
            latency_histogram: sample_latency_histogram(),
            operations_log: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_vfs::{RuntimeLatencyHistogram, RuntimeScalarMetric};

    #[test]
    fn runtime_stats_report_computes_units_and_reset_aware_deltas() {
        let mut before = RuntimeStatsSnapshot {
            generated_unix_ms: 1_000,
            ..RuntimeStatsSnapshot::default()
        };
        before.bridge.counters.insert(
            "read_callbacks".to_string(),
            RuntimeScalarMetric {
                value: 10,
                unit: "count".into(),
            },
        );
        before.system.gauges.insert(
            "rss_bytes".to_string(),
            RuntimeScalarMetric {
                value: 4096,
                unit: "bytes".into(),
            },
        );
        before.metadata.latencies.insert(
            "commit".to_string(),
            RuntimeLatencyHistogram {
                count: 7,
                sum: 700,
                unit: "ns".into(),
                buckets: vec![RuntimeLatencyBucket {
                    le_ns: 1_000,
                    count: 4,
                }],
            },
        );
        before.metadata.latencies.insert(
            "rollback".to_string(),
            RuntimeLatencyHistogram {
                count: 10,
                sum: 1_000,
                unit: "ns".into(),
                buckets: vec![
                    RuntimeLatencyBucket {
                        le_ns: 1_000,
                        count: 5,
                    },
                    RuntimeLatencyBucket {
                        le_ns: 2_000,
                        count: 10,
                    },
                ],
            },
        );

        let mut after = before.clone();
        after.generated_unix_ms = 1_250;
        after
            .bridge
            .counters
            .get_mut("read_callbacks")
            .unwrap()
            .value = 3;
        after.system.gauges.get_mut("rss_bytes").unwrap().value = 8192;
        let commit = after.metadata.latencies.get_mut("commit").unwrap();
        commit.count = 9;
        commit.sum = 950;
        commit.buckets[0].count = 8;
        let rollback = after.metadata.latencies.get_mut("rollback").unwrap();
        rollback.count = 2;
        rollback.sum = 200;
        rollback.buckets[0].count = 1;
        rollback.buckets[1].count = 2;

        let phase = RuntimeStatsPhaseDeltaReport {
            phase: "write".to_string(),
            delta: runtime_stats_delta("write", &before, &after),
        };

        let report = runtime_stats_report(
            ".studiofs-runtime-stats".to_string(),
            before.clone(),
            after.clone(),
            vec![phase],
            Vec::new(),
        );

        let whole_run = report.whole_run.as_ref().expect("whole-run delta");
        assert_eq!(whole_run.interval_ms, 250);
        assert_eq!(
            whole_run.sections["bridge"].counters["read_callbacks"].delta,
            3
        );
        assert_eq!(
            whole_run.sections["bridge"].counters["read_callbacks"].unit,
            "count"
        );
        assert_eq!(
            whole_run.sections["system"].gauges["rss_bytes"].before,
            Some(4096)
        );
        assert_eq!(
            whole_run.sections["metadata"].latencies["commit"].sum_delta,
            250
        );
        assert_eq!(
            whole_run.sections["metadata"].latencies["commit"].buckets[0].count_delta,
            4
        );
        assert_eq!(
            whole_run.sections["metadata"].latencies["rollback"].count_delta,
            2
        );
        assert_eq!(
            whole_run.sections["metadata"].latencies["rollback"].sum_delta,
            200
        );
        assert_eq!(
            whole_run.sections["metadata"].latencies["rollback"].buckets[0].count_delta,
            1
        );
        assert_eq!(
            whole_run.sections["metadata"].latencies["rollback"].buckets[1].count_delta,
            2
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("bridge.read_callbacks"))
        );
        assert!(
            whole_run
                .warnings
                .iter()
                .any(|warning| warning.contains("bridge.read_callbacks"))
        );
        assert_eq!(
            whole_run
                .warnings
                .iter()
                .filter(|warning| warning.contains("metadata.rollback"))
                .count(),
            1
        );
        assert_eq!(report.phases[0].phase, "write");
    }
}

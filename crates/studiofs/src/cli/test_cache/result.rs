use crate::cli::test_cache_profile_types::ProfileArtifactKind;
use serde::Serialize;
use studiofs_cache::metrics::CachePipelineMetricsSnapshot;

#[derive(Debug, Serialize)]
pub(super) struct OpReport {
    pub(super) operations: u64,
    pub(super) bytes: u64,
    pub(super) elapsed_ms: f64,
    pub(super) avg_latency_ms: f64,
    pub(super) p50_ms: f64,
    pub(super) p95_ms: f64,
    pub(super) p99_ms: f64,
    pub(super) throughput_bytes_per_sec: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct BenchmarkReport {
    pub(super) run_id: String,
    pub(super) mode: String,
    pub(super) cache_dir: String,
    pub(super) started_at: String,
    pub(super) completed_at: String,
    pub(super) total_elapsed_ms: f64,
    pub(super) block_size: u64,
    pub(super) read_size: u64,
    pub(super) concurrency: usize,
    pub(super) sequential_streams: bool,
    pub(super) io_depth: usize,
    pub(super) warmup_runs: u32,
    pub(super) sizes: String,
    pub(super) workload_files: usize,
    pub(super) workload_small_files: usize,
    pub(super) workload_medium_files: usize,
    pub(super) workload_large_files: usize,
    pub(super) access_pattern: String,
    pub(super) l1_limit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) work_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) mount_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) l1_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cache_pipeline: Option<CachePipelineReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) profile: Option<BenchmarkProfileReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) advanced_metrics: Option<AdvancedMetricsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) local_first_read_comparison: Option<MountedLocalFirstReadComparisonReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) stream_results: Option<Vec<StreamResultReport>>,
    pub(super) writes: OpReport,
    pub(super) reads: OpReport,
}

#[derive(Debug, Serialize)]
pub(super) struct MountedLocalFirstReadComparisonReport {
    pub(super) cache_path: String,
    pub(super) upload_dir: String,
    pub(super) phases: Vec<MountedLocalFirstReadPhaseReport>,
}

#[derive(Debug, Serialize)]
pub(super) struct MountedLocalFirstReadPhaseReport {
    pub(super) phase: String,
    pub(super) wait_target: String,
    pub(super) target_state_observed: bool,
    pub(super) wait_ms: f64,
    pub(super) pending_uploads_before: usize,
    pub(super) pending_uploads_after: usize,
    pub(super) results: OpReport,
}

#[derive(Debug, Serialize)]
pub(super) struct StreamResultReport {
    pub(super) stream_id: usize,
    pub(super) writes: OpReport,
    pub(super) reads: OpReport,
}

#[derive(Debug, Serialize)]
pub(super) struct AdvancedMetricsReport {
    pub(super) size_class_boundaries: SizeClassBoundaryReport,
    pub(super) size_class_stats: SizeClassStatsReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cache_levels: Option<CacheLevelsReport>,
}

#[derive(Debug, Serialize)]
pub(super) struct SizeClassBoundaryReport {
    pub(super) small_max_bytes: u64,
    pub(super) medium_max_bytes: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct SizeClassStatsReport {
    pub(super) writes: Vec<SizeClassOpReport>,
    pub(super) reads: Vec<SizeClassOpReport>,
}

#[derive(Debug, Serialize)]
pub(super) struct SizeClassOpReport {
    pub(super) class: String,
    pub(super) operations: u64,
    pub(super) bytes: u64,
    pub(super) avg_latency_ms: f64,
    pub(super) p50_ms: f64,
    pub(super) p95_ms: f64,
    pub(super) p99_ms: f64,
    pub(super) throughput_bytes_per_sec: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct CacheLevelsReport {
    pub(super) l1: CacheLevelReport,
}

#[derive(Debug, Serialize)]
pub(super) struct CacheLevelReport {
    pub(super) hits: u64,
    pub(super) misses: u64,
    pub(super) bytes_served: u64,
    pub(super) throughput_bytes_per_sec: f64,
    pub(super) avg_latency_ms: f64,
    pub(super) p50_ms: f64,
    pub(super) p95_ms: f64,
    pub(super) p99_ms: f64,
    pub(super) latency_histogram: LatencyHistogramReport,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct LatencyHistogramReport {
    pub(super) bucket_upper_bounds_ns: Vec<u64>,
    pub(super) bucket_counts: Vec<u64>,
}

#[derive(Debug, Serialize)]
pub(super) struct MaterializeBatchStatsReport {
    pub(super) batches: u64,
    pub(super) blocks: u64,
    pub(super) bytes: u64,
    pub(super) inflight_batches: usize,
    pub(super) inflight_batches_peak: usize,
    pub(super) duration_histogram: LatencyHistogramReport,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct UploadQueueStatsReport {
    pub(super) pending_depth: usize,
    pub(super) pending_depth_peak: usize,
    pub(super) submitted: u64,
    pub(super) completed: u64,
    pub(super) requeued: u64,
    pub(super) canceled: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct CachePipelineReport {
    pub(super) materialize_batches: MaterializeBatchStatsReport,
    pub(super) upload_queue: UploadQueueStatsReport,
    pub(super) blocking_tasks: BlockingTaskStatsReport,
}

#[derive(Debug, Serialize)]
pub(super) struct BlockingTaskStatsReport {
    pub(super) total: BlockingTaskSingleStatsReport,
    pub(super) by_kind: Vec<BlockingTaskKindStatsReport>,
}

#[derive(Debug, Serialize)]
pub(super) struct BlockingTaskKindStatsReport {
    pub(super) task_kind: String,
    pub(super) stats: BlockingTaskSingleStatsReport,
}

#[derive(Debug, Serialize)]
pub(super) struct BlockingTaskSingleStatsReport {
    pub(super) active: usize,
    pub(super) active_peak: usize,
    pub(super) submitted: u64,
    pub(super) started: u64,
    pub(super) completed: u64,
    pub(super) queue_wait_avg_ms: f64,
    pub(super) queue_wait_max_ms: f64,
    pub(super) execution_avg_ms: f64,
    pub(super) execution_max_ms: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct BenchmarkProfileReport {
    pub(super) enabled: bool,
    pub(super) profile_mode: String,
    pub(super) cpu_profile: CpuProfileReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) endpoint: Option<ProfileEndpointReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) artifacts: Vec<ProfileArtifactSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct CpuProfileReport {
    pub(super) total_wall_ms: f64,
    pub(super) total_cpu_ms: f64,
    pub(super) cpu_utilization_percent: f64,
    pub(super) write_phase_cpu_percent: f64,
    pub(super) read_phase_cpu_percent: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct ProfileEndpointReport {
    pub(super) url: String,
    pub(super) application_name: String,
}

#[derive(Debug, Serialize)]
pub(super) struct ProfileArtifactSummary {
    pub(super) kind: ProfileArtifactKind,
    pub(super) path: String,
    pub(super) bytes: u64,
    pub(super) sent_to_endpoint: bool,
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
            blocks: snapshot.blocks,
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
    use studiofs_meta::DEFAULT_BLOCK_SIZE;

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
        workload_files: DEFAULT_WORKLOAD_FILES,
        workload_small_files: WORKLOAD_FILES_PER_TIER,
        workload_medium_files: WORKLOAD_FILES_PER_TIER,
        workload_large_files: WORKLOAD_FILES_PER_TIER,
        access_pattern: "sequential".to_string(),
        l1_limit: 2 * 1024 * 1024 * 1024,
        work_root: None,
        volume: None,
        mount_point: None,
        l1_bytes: Some(128 * 1024 * 1024),
        cache_pipeline: Some(CachePipelineReport {
            materialize_batches: MaterializeBatchStatsReport {
                batches: 4,
                blocks: 16,
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
                path: r"D:\test-cache\studiofs-test-cache-direct-shim-run-123-cpu.pprof.gz"
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
        writes: OpReport {
            operations: 16,
            bytes: 128 * 1024 * 1024,
            elapsed_ms: 80.0,
            avg_latency_ms: 5.0,
            p50_ms: 4.7,
            p95_ms: 8.3,
            p99_ms: 8.8,
            throughput_bytes_per_sec: 1_375.53 * 1024.0 * 1024.0,
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
        },
    }
}

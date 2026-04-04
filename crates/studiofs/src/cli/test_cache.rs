//! `studiofs test-cache` — benchmark the StudioFS data path.
//!
//! Supports:
//! - compatibility-shim benchmark mode (`direct-shim`) for the BlockStore-backed local path
//! - end-to-end benchmark mode (`e2e`) that runs copy/read on a mounted volume

use crate::cli::test_cache_profile::TestCacheProfileSession;
use crate::cli::test_cache_profile_types::{TestCacheProfileMode, TestCacheProfileResult};
use crate::cli::utils::{format_bytes, format_throughput};
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::Serialize;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use studiofs_cache::block_cache::{CacheReadLevel, DEFAULT_L1_SIZE};
use studiofs_cache::metrics::CachePipelineMetricsSnapshot;
use studiofs_cache::{BlockCache, BlockCacheConfig};
use studiofs_vfs::parse_size_bytes;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

const MAX_LATENCY_SAMPLES: usize = 200_000;
const WORKLOAD_FILES_PER_TIER: usize = 100;
const DEFAULT_WORKLOAD_FILES: usize = WORKLOAD_FILES_PER_TIER * 3;
const SMALL_FILE_MIN_BYTES: u64 = 1024;
const SMALL_FILE_MAX_BYTES: u64 = 10 * 1024;
const LARGE_FILE_MULTIPLIER: u64 = 8;
const DEFAULT_SMALL_CLASS_MAX_BYTES: u64 = SMALL_FILE_MAX_BYTES;
const DEFAULT_TEST_CACHE_BLOCK_SIZE_STR: &str = "16MB";

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
pub enum TestCacheAccessPattern {
    /// Sequential file/object order.
    Sequential,
    /// Fully random file/object order.
    Random,
    /// Mostly sequential with random local swaps.
    Mixed,
}

#[derive(Debug)]
struct TestCacheParams {
    target: PathBuf,
    block_size: u64,
    read_size: u64,
    size_class_small_max: u64,
    size_class_medium_max: u64,
    concurrency: usize,
    sequential_streams: bool,
    io_depth: usize,
    warmup_runs: u32,
    sizes: WorkloadSizeInput,
    access_pattern: TestCacheAccessPattern,
    l1_limit: u64,
    profile: bool,
    run_id: String,
}

#[derive(Clone, Debug)]
struct ActiveMount {
    mount_point: PathBuf,
    volume: String,
    cache_path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
enum ResolvedRunMode {
    DirectShim {
        cache_dir: PathBuf,
    },
    MountedE2E {
        volume: String,
        mount_point: PathBuf,
        work_root: PathBuf,
        cache_path: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SizeClass {
    Small,
    Medium,
    Large,
}

impl SizeClass {
    fn label(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }
}

#[derive(Clone, Debug)]
enum WorkloadSizeInput {
    Default,
    Percentages { small: u8, medium: u8, large: u8 },
    ExplicitList(Vec<u64>),
}

impl WorkloadSizeInput {
    fn display_label(&self) -> String {
        match self {
            Self::Default => "default".to_string(),
            Self::Percentages {
                small,
                medium,
                large,
            } => {
                format!("small={small}%,medium={medium}%,large={large}%")
            }
            Self::ExplicitList(items) => format!("explicit-list({})", items.len()),
        }
    }
}

#[derive(Clone, Debug)]
struct WorkloadPlan {
    sizes: Vec<u64>,
    source: String,
    small_files: usize,
    medium_files: usize,
    large_files: usize,
}

#[derive(Clone, Copy, Debug)]
struct SizeClassBounds {
    small_max: u64,
    medium_max: u64,
}

#[derive(Debug, Serialize)]
struct OpReport {
    operations: u64,
    bytes: u64,
    elapsed_ms: f64,
    avg_latency_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    throughput_bytes_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    run_id: String,
    mode: String,
    cache_dir: String,
    started_at: String,
    completed_at: String,
    total_elapsed_ms: f64,
    block_size: u64,
    read_size: u64,
    concurrency: usize,
    sequential_streams: bool,
    io_depth: usize,
    warmup_runs: u32,
    sizes: String,
    workload_files: usize,
    workload_small_files: usize,
    workload_medium_files: usize,
    workload_large_files: usize,
    access_pattern: String,
    l1_limit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    work_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mount_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    l1_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_pipeline: Option<CachePipelineReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile: Option<BenchmarkProfileReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    advanced_metrics: Option<AdvancedMetricsReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    buffered_read_comparison: Option<MountedBufferedReadComparisonReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_results: Option<Vec<StreamResultReport>>,
    writes: OpReport,
    reads: OpReport,
}

#[derive(Debug, Serialize)]
struct MountedBufferedReadComparisonReport {
    cache_path: String,
    upload_dir: String,
    phases: Vec<MountedBufferedReadPhaseReport>,
}

#[derive(Debug, Serialize)]
struct MountedBufferedReadPhaseReport {
    phase: String,
    wait_target: String,
    target_state_observed: bool,
    wait_ms: f64,
    pending_uploads_before: usize,
    pending_uploads_after: usize,
    results: OpReport,
}

#[derive(Debug, Serialize)]
struct StreamResultReport {
    stream_id: usize,
    writes: OpReport,
    reads: OpReport,
}

#[derive(Debug, Serialize)]
struct AdvancedMetricsReport {
    size_class_boundaries: SizeClassBoundaryReport,
    size_class_stats: SizeClassStatsReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_levels: Option<CacheLevelsReport>,
}

#[derive(Debug, Serialize)]
struct SizeClassBoundaryReport {
    small_max_bytes: u64,
    medium_max_bytes: u64,
}

#[derive(Debug, Serialize)]
struct SizeClassStatsReport {
    writes: Vec<SizeClassOpReport>,
    reads: Vec<SizeClassOpReport>,
}

#[derive(Debug, Serialize)]
struct SizeClassOpReport {
    class: String,
    operations: u64,
    bytes: u64,
    avg_latency_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    throughput_bytes_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct CacheLevelsReport {
    l1: CacheLevelReport,
}

#[derive(Debug, Serialize)]
struct CacheLevelReport {
    hits: u64,
    misses: u64,
    bytes_served: u64,
    throughput_bytes_per_sec: f64,
    avg_latency_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    latency_histogram: LatencyHistogramReport,
}

#[derive(Debug, Clone, Serialize)]
struct LatencyHistogramReport {
    bucket_upper_bounds_ns: Vec<u64>,
    bucket_counts: Vec<u64>,
}

#[derive(Debug, Serialize)]
struct MaterializeBatchStatsReport {
    batches: u64,
    blocks: u64,
    bytes: u64,
    inflight_batches: usize,
    inflight_batches_peak: usize,
    duration_histogram: LatencyHistogramReport,
}

#[derive(Debug, Clone, Serialize)]
struct UploadQueueStatsReport {
    pending_depth: usize,
    pending_depth_peak: usize,
    submitted: u64,
    completed: u64,
    requeued: u64,
    canceled: u64,
}

#[derive(Debug, Serialize)]
struct CachePipelineReport {
    materialize_batches: MaterializeBatchStatsReport,
    upload_queue: UploadQueueStatsReport,
    blocking_tasks: BlockingTaskStatsReport,
}

#[derive(Debug, Serialize)]
struct BlockingTaskStatsReport {
    total: BlockingTaskSingleStatsReport,
    by_kind: Vec<BlockingTaskKindStatsReport>,
}

#[derive(Debug, Serialize)]
struct BlockingTaskKindStatsReport {
    task_kind: String,
    stats: BlockingTaskSingleStatsReport,
}

#[derive(Debug, Serialize)]
struct BlockingTaskSingleStatsReport {
    active: usize,
    active_peak: usize,
    submitted: u64,
    started: u64,
    completed: u64,
    queue_wait_avg_ms: f64,
    queue_wait_max_ms: f64,
    execution_avg_ms: f64,
    execution_max_ms: f64,
}

#[derive(Debug, Serialize)]
struct BenchmarkProfileReport {
    enabled: bool,
    profile_mode: String,
    cpu_profile: CpuProfileReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<ProfileEndpointReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    artifacts: Vec<ProfileArtifactSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CpuProfileReport {
    total_wall_ms: f64,
    total_cpu_ms: f64,
    cpu_utilization_percent: f64,
    write_phase_cpu_percent: f64,
    read_phase_cpu_percent: f64,
}

#[derive(Debug, Serialize)]
struct ProfileEndpointReport {
    url: String,
    application_name: String,
}

#[derive(Debug, Serialize)]
struct ProfileArtifactSummary {
    kind: String,
    path: String,
    bytes: u64,
    sent_to_endpoint: bool,
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
                    stats.queue_wait_total_ns as f64 / stats.started as f64 / 1_000_000.0
                },
                queue_wait_max_ms: stats.queue_wait_max_ns as f64 / 1_000_000.0,
                execution_avg_ms: if stats.completed == 0 {
                    0.0
                } else {
                    stats.execution_total_ns as f64 / stats.completed as f64 / 1_000_000.0
                },
                execution_max_ms: stats.execution_max_ns as f64 / 1_000_000.0,
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
        by_kind.sort_by(|a, b| b.stats.submitted.cmp(&a.stats.submitted));

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

#[derive(Default, Clone)]
struct LatencySamples {
    samples_ns: Vec<u64>,
    total_ops: u64,
    total_bytes: u64,
    total_latency_ns: u128,
    sample_keep_every: u64,
}

impl LatencySamples {
    fn record(&mut self, elapsed: Duration, bytes: u64) {
        let ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.total_latency_ns = self.total_latency_ns.saturating_add(ns as u128);
        self.total_ops = self.total_ops.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(bytes);

        // Keep percentile estimation memory-bounded for long benchmark runs.
        let keep_every = self.sample_keep_every.max(1);
        if (self.total_ops - 1).is_multiple_of(keep_every) {
            self.samples_ns.push(ns);
        }

        if self.samples_ns.len() > MAX_LATENCY_SAMPLES {
            self.samples_ns = self.samples_ns.iter().step_by(2).copied().collect();
            self.sample_keep_every = keep_every.saturating_mul(2);
        }
    }

    fn total_duration_ms(&self) -> f64 {
        self.total_latency_ns as f64 / 1_000_000.0
    }

    fn avg_ns(&self) -> f64 {
        if self.total_ops == 0 {
            0.0
        } else {
            self.total_latency_ns as f64 / self.total_ops as f64
        }
    }

    fn percentile_ns(&self, p: f64) -> u64 {
        if self.samples_ns.is_empty() {
            return 0;
        }
        let mut samples = self.samples_ns.clone();
        samples.sort_unstable();
        let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
        samples[idx.min(samples.len().saturating_sub(1))]
    }

    fn to_report(&self, wall_elapsed_ms: f64) -> OpReport {
        let total_ms = self.total_duration_ms();
        let avg_ms = self.avg_ns() / 1_000_000.0;
        let p50_ms = self.percentile_ns(0.50) as f64 / 1_000_000.0;
        let p95_ms = self.percentile_ns(0.95) as f64 / 1_000_000.0;
        let p99_ms = self.percentile_ns(0.99) as f64 / 1_000_000.0;

        let throughput_bytes_per_sec = if wall_elapsed_ms <= 0.0 {
            0.0
        } else {
            self.total_bytes as f64 / (wall_elapsed_ms / 1000.0)
        };

        OpReport {
            operations: self.total_ops,
            bytes: self.total_bytes,
            elapsed_ms: total_ms,
            avg_latency_ms: avg_ms,
            p50_ms,
            p95_ms,
            p99_ms,
            throughput_bytes_per_sec,
        }
    }

    fn to_size_class_report(&self, class: SizeClass, wall_elapsed_ms: f64) -> SizeClassOpReport {
        let report = self.to_report(wall_elapsed_ms);
        SizeClassOpReport {
            class: class.label().to_string(),
            operations: report.operations,
            bytes: report.bytes,
            avg_latency_ms: report.avg_latency_ms,
            p50_ms: report.p50_ms,
            p95_ms: report.p95_ms,
            p99_ms: report.p99_ms,
            throughput_bytes_per_sec: report.throughput_bytes_per_sec,
        }
    }

    fn to_histogram_report(&self) -> LatencyHistogramReport {
        let bucket_upper_bounds_ns = vec![
            50_000,
            100_000,
            250_000,
            500_000,
            1_000_000,
            2_000_000,
            5_000_000,
            10_000_000,
            25_000_000,
            50_000_000,
            100_000_000,
            250_000_000,
            500_000_000,
            1_000_000_000,
            2_000_000_000,
            5_000_000_000,
        ];
        let mut bucket_counts = vec![0u64; bucket_upper_bounds_ns.len() + 1];
        for sample in &self.samples_ns {
            let idx = bucket_upper_bounds_ns
                .iter()
                .position(|upper| sample <= upper)
                .unwrap_or(bucket_counts.len() - 1);
            bucket_counts[idx] = bucket_counts[idx].saturating_add(1);
        }
        LatencyHistogramReport {
            bucket_upper_bounds_ns,
            bucket_counts,
        }
    }

    fn downsample_in_place(samples: &mut Vec<u64>, step: usize) {
        if step <= 1 || samples.len() <= 1 {
            return;
        }
        *samples = samples.iter().step_by(step).copied().collect();
    }

    fn effective_keep_every(&self) -> u64 {
        if self.total_ops == 0 {
            1
        } else {
            self.sample_keep_every.max(1)
        }
    }

    fn merge_from_ref(&mut self, other: &Self) {
        let self_keep = self.effective_keep_every();
        let other_keep = other.effective_keep_every();
        let mut target_keep = self_keep.max(other_keep);

        if self_keep < target_keep {
            let step = target_keep.div_ceil(self_keep) as usize;
            Self::downsample_in_place(&mut self.samples_ns, step);
        }

        if other_keep < target_keep {
            let step = target_keep.div_ceil(other_keep) as usize;
            self.samples_ns
                .extend(other.samples_ns.iter().step_by(step).copied());
        } else {
            self.samples_ns.extend_from_slice(&other.samples_ns);
        }

        self.total_ops = self.total_ops.saturating_add(other.total_ops);
        self.total_bytes = self.total_bytes.saturating_add(other.total_bytes);
        self.total_latency_ns = self.total_latency_ns.saturating_add(other.total_latency_ns);

        while self.samples_ns.len() > MAX_LATENCY_SAMPLES {
            Self::downsample_in_place(&mut self.samples_ns, 2);
            target_keep = target_keep.saturating_mul(2);
        }
        self.sample_keep_every = if self.total_ops == 0 { 0 } else { target_keep };
    }
}

#[derive(Default, Clone)]
struct SizeClassLatencySamples {
    small: LatencySamples,
    medium: LatencySamples,
    large: LatencySamples,
}

impl SizeClassLatencySamples {
    fn get_mut(&mut self, class: SizeClass) -> &mut LatencySamples {
        match class {
            SizeClass::Small => &mut self.small,
            SizeClass::Medium => &mut self.medium,
            SizeClass::Large => &mut self.large,
        }
    }

    fn to_reports(&self, wall_elapsed_ms: f64) -> Vec<SizeClassOpReport> {
        vec![
            self.small
                .to_size_class_report(SizeClass::Small, wall_elapsed_ms),
            self.medium
                .to_size_class_report(SizeClass::Medium, wall_elapsed_ms),
            self.large
                .to_size_class_report(SizeClass::Large, wall_elapsed_ms),
        ]
    }

    fn merge_from_ref(&mut self, other: &Self) {
        self.small.merge_from_ref(&other.small);
        self.medium.merge_from_ref(&other.medium);
        self.large.merge_from_ref(&other.large);
    }
}

#[derive(Default, Clone)]
struct PhaseTelemetry {
    overall: LatencySamples,
    by_size: SizeClassLatencySamples,
}

impl PhaseTelemetry {
    fn record(&mut self, class: SizeClass, elapsed: Duration, bytes: u64) {
        self.overall.record(elapsed, bytes);
        self.by_size.get_mut(class).record(elapsed, bytes);
    }

    fn merge_from_ref(&mut self, other: &Self) {
        self.overall.merge_from_ref(&other.overall);
        self.by_size.merge_from_ref(&other.by_size);
    }
}

#[derive(Default, Clone)]
struct CacheLevelTelemetryAccumulator {
    l1_hits: u64,
    l1_misses: u64,
    l1_bytes_served: u64,
    l1_lookup_latency: LatencySamples,
}

impl CacheLevelTelemetryAccumulator {
    fn record_l1(&mut self, hit: bool, elapsed: Duration, bytes_served: u64) {
        self.l1_lookup_latency.record(elapsed, bytes_served);
        if hit {
            self.l1_hits = self.l1_hits.saturating_add(1);
            self.l1_bytes_served = self.l1_bytes_served.saturating_add(bytes_served);
        } else {
            self.l1_misses = self.l1_misses.saturating_add(1);
        }
    }

    fn to_report(&self, wall_elapsed_ms: f64) -> CacheLevelsReport {
        let l1_report = self.l1_lookup_latency.to_report(wall_elapsed_ms);
        CacheLevelsReport {
            l1: CacheLevelReport {
                hits: self.l1_hits,
                misses: self.l1_misses,
                bytes_served: self.l1_bytes_served,
                throughput_bytes_per_sec: l1_report.throughput_bytes_per_sec,
                avg_latency_ms: l1_report.avg_latency_ms,
                p50_ms: l1_report.p50_ms,
                p95_ms: l1_report.p95_ms,
                p99_ms: l1_report.p99_ms,
                latency_histogram: self.l1_lookup_latency.to_histogram_report(),
            },
        }
    }

    fn merge_from_ref(&mut self, other: &Self) {
        self.l1_hits = self.l1_hits.saturating_add(other.l1_hits);
        self.l1_misses = self.l1_misses.saturating_add(other.l1_misses);
        self.l1_bytes_served = self.l1_bytes_served.saturating_add(other.l1_bytes_served);
        self.l1_lookup_latency
            .merge_from_ref(&other.l1_lookup_latency);
    }
}

#[derive(Default, Clone)]
struct DirectReadTelemetry {
    phase: PhaseTelemetry,
    cache_levels: CacheLevelTelemetryAccumulator,
}

impl DirectReadTelemetry {
    fn merge_from_ref(&mut self, other: &Self) {
        self.phase.merge_from_ref(&other.phase);
        self.cache_levels.merge_from_ref(&other.cache_levels);
    }
}

fn classify_size(size: u64, small_max: u64, medium_max: u64) -> SizeClass {
    if size <= small_max {
        SizeClass::Small
    } else if size <= medium_max {
        SizeClass::Medium
    } else {
        SizeClass::Large
    }
}

#[derive(Clone, Debug)]
struct WorkFile {
    path: PathBuf,
    size: u64,
    index: usize,
}

#[derive(Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9e3779b97f4a7c15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 7;
        x ^= x >> 9;
        x ^= x << 8;
        self.state = x;
        x
    }

    fn next_usize(&mut self, max_exclusive: usize) -> usize {
        if max_exclusive <= 1 {
            return 0;
        }
        (self.next_u64() % max_exclusive as u64) as usize
    }
}

fn parse_size_arg(label: &str, raw: &str) -> Result<u64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(anyhow!("{label} is empty"));
    }

    parse_size_bytes(raw)
        .or_else(|| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .with_context(|| {
            format!("invalid {label} '{raw}', expected value like 4KB, 2MB, 1GB or integer bytes")
        })
}

fn parse_sizes_arg(raw: Option<&str>) -> Result<WorkloadSizeInput> {
    let Some(raw) = raw else {
        return Ok(WorkloadSizeInput::Default);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(WorkloadSizeInput::Default);
    }

    if raw.contains('=') {
        let mut small = 0u8;
        let mut medium = 0u8;
        let mut large = 0u8;
        let mut seen_any = false;
        let mut seen_small = false;
        let mut seen_medium = false;
        let mut seen_large = false;

        for segment in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (name, value_raw) = segment.split_once('=').ok_or_else(|| {
                anyhow!("invalid --sizes segment '{segment}', expected key=value")
            })?;
            let value = value_raw
                .trim()
                .trim_end_matches('%')
                .parse::<u8>()
                .with_context(|| format!("invalid percentage in --sizes segment '{segment}'"))?;
            seen_any = true;
            match name.trim().to_ascii_lowercase().as_str() {
                "small" => {
                    if seen_small {
                        return Err(anyhow!("duplicate 'small' entry in --sizes"));
                    }
                    small = value;
                    seen_small = true;
                }
                "medium" => {
                    if seen_medium {
                        return Err(anyhow!("duplicate 'medium' entry in --sizes"));
                    }
                    medium = value;
                    seen_medium = true;
                }
                "large" => {
                    if seen_large {
                        return Err(anyhow!("duplicate 'large' entry in --sizes"));
                    }
                    large = value;
                    seen_large = true;
                }
                other => {
                    return Err(anyhow!(
                        "invalid size class '{other}' in --sizes; expected small, medium, large"
                    ));
                }
            }
        }

        if !seen_any {
            return Err(anyhow!(
                "--sizes percentages are empty; expected values like small=60,medium=30,large=10"
            ));
        }
        if u16::from(small) + u16::from(medium) + u16::from(large) != 100 {
            return Err(anyhow!(
                "--sizes percentages must add up to 100 (got {} + {} + {})",
                small,
                medium,
                large
            ));
        }
        return Ok(WorkloadSizeInput::Percentages {
            small,
            medium,
            large,
        });
    }

    let mut sizes = Vec::new();
    for value in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        sizes.push(parse_size_arg("sizes", value)?);
    }
    if sizes.is_empty() {
        return Err(anyhow!(
            "--sizes explicit list is empty; provide comma-separated sizes like 4KB,128KB,1MB"
        ));
    }
    Ok(WorkloadSizeInput::ExplicitList(sizes))
}

fn build_access_sequence(len: usize, pattern: TestCacheAccessPattern, seed: u64) -> Vec<usize> {
    let mut order: Vec<usize> = (0..len).collect();
    if len <= 1 {
        return order;
    }
    let mut rng = XorShift64::new(seed);

    match pattern {
        TestCacheAccessPattern::Sequential => {}
        TestCacheAccessPattern::Random => {
            for i in (1..order.len()).rev() {
                let j = rng.next_usize(i + 1);
                order.swap(i, j);
            }
        }
        TestCacheAccessPattern::Mixed => {
            let swaps = (order.len() / 4).max(1);
            for _ in 0..swaps {
                let a = rng.next_usize(order.len());
                let b = rng.next_usize(order.len());
                order.swap(a, b);
            }
        }
    }

    order
}

fn build_work_sizes(
    block_size: u64,
    size_input: &WorkloadSizeInput,
    size_class_bounds: SizeClassBounds,
) -> WorkloadPlan {
    let mut rng = XorShift64::new(0x5ed0_f00d_beef);
    let small_max = size_class_bounds.small_max.max(1);
    let medium_max = size_class_bounds
        .medium_max
        .max(small_max.saturating_add(1));
    let medium_size = block_size
        .max(1)
        .min(medium_max)
        .max(small_max.saturating_add(1));
    let large_size = block_size
        .max(1)
        .saturating_mul(LARGE_FILE_MULTIPLIER)
        .max(medium_max.saturating_add(1))
        .max(1);

    let mut generate_small = |out: &mut Vec<u64>, count: usize| {
        let lower = SMALL_FILE_MIN_BYTES.min(small_max).max(1);
        let upper = small_max.max(lower);
        let span = upper.saturating_sub(lower).saturating_add(1);
        for _ in 0..count {
            let size = lower + (rng.next_u64() % span);
            out.push(size);
        }
    };

    match size_input {
        WorkloadSizeInput::Default => {
            let mut sizes = Vec::with_capacity(DEFAULT_WORKLOAD_FILES);
            generate_small(&mut sizes, WORKLOAD_FILES_PER_TIER);
            for _ in 0..WORKLOAD_FILES_PER_TIER {
                sizes.push(medium_size);
            }
            for _ in 0..WORKLOAD_FILES_PER_TIER {
                sizes.push(large_size);
            }
            WorkloadPlan {
                sizes,
                source: size_input.display_label(),
                small_files: WORKLOAD_FILES_PER_TIER,
                medium_files: WORKLOAD_FILES_PER_TIER,
                large_files: WORKLOAD_FILES_PER_TIER,
            }
        }
        WorkloadSizeInput::Percentages {
            small,
            medium,
            large,
        } => {
            let total = DEFAULT_WORKLOAD_FILES as u64;
            let small_count = (total * u64::from(*small) / 100) as usize;
            let mut medium_count = (total * u64::from(*medium) / 100) as usize;
            let mut large_count = (total * u64::from(*large) / 100) as usize;
            let assigned = small_count + medium_count + large_count;
            if assigned < DEFAULT_WORKLOAD_FILES {
                medium_count += DEFAULT_WORKLOAD_FILES - assigned;
            } else if assigned > DEFAULT_WORKLOAD_FILES {
                let overshoot = assigned - DEFAULT_WORKLOAD_FILES;
                let trim = overshoot.min(medium_count);
                medium_count -= trim;
                let remain = overshoot - trim;
                if remain > 0 {
                    large_count = large_count.saturating_sub(remain);
                }
            }

            let mut sizes = Vec::with_capacity(DEFAULT_WORKLOAD_FILES);
            generate_small(&mut sizes, small_count);
            for _ in 0..medium_count {
                sizes.push(medium_size);
            }
            for _ in 0..large_count {
                sizes.push(large_size);
            }

            WorkloadPlan {
                sizes,
                source: size_input.display_label(),
                small_files: small_count,
                medium_files: medium_count,
                large_files: large_count,
            }
        }
        WorkloadSizeInput::ExplicitList(items) => {
            let mut small_files = 0usize;
            let mut medium_files = 0usize;
            let mut large_files = 0usize;
            for size in items {
                match classify_size(*size, small_max, medium_max) {
                    SizeClass::Small => small_files += 1,
                    SizeClass::Medium => medium_files += 1,
                    SizeClass::Large => large_files += 1,
                }
            }
            WorkloadPlan {
                sizes: items.clone(),
                source: size_input.display_label(),
                small_files,
                medium_files,
                large_files,
            }
        }
    }
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = XorShift64::new(seed ^ 0x6ac9_fe0f);
    let mut out = vec![0u8; len];
    for byte in &mut out {
        *byte = rng.next_u64() as u8;
    }
    out
}

fn build_file_worklist(work_sizes: &[u64], mount_root: &Path) -> Vec<WorkFile> {
    work_sizes
        .iter()
        .enumerate()
        .map(|(idx, size)| WorkFile {
            path: mount_root.join(format!("test_cache_file_{idx:06}.bin")),
            size: *size,
            index: idx,
        })
        .collect()
}

fn split_stream_segments(order: &[usize], stream_count: usize) -> Vec<Vec<usize>> {
    if order.is_empty() {
        return Vec::new();
    }
    let streams = stream_count.max(1).min(order.len());
    let base = order.len() / streams;
    let extra = order.len() % streams;
    let mut segments = Vec::with_capacity(streams);
    let mut start = 0usize;
    for stream_idx in 0..streams {
        let mut len = base;
        if stream_idx < extra {
            len += 1;
        }
        let end = start + len;
        segments.push(order[start..end].to_vec());
        start = end;
    }
    segments
}

fn stream_reports_from_telemetry(
    writes_by_stream: &[PhaseTelemetry],
    reads_by_stream: &[PhaseTelemetry],
    writes_wall_elapsed_ms: f64,
    reads_wall_elapsed_ms: f64,
) -> Option<Vec<StreamResultReport>> {
    if writes_by_stream.len() <= 1 || writes_by_stream.len() != reads_by_stream.len() {
        return None;
    }
    Some(
        writes_by_stream
            .iter()
            .zip(reads_by_stream.iter())
            .enumerate()
            .map(|(idx, (writes, reads))| StreamResultReport {
                stream_id: idx,
                writes: writes.overall.to_report(writes_wall_elapsed_ms),
                reads: reads.overall.to_report(reads_wall_elapsed_ms),
            })
            .collect(),
    )
}

#[derive(Clone, Copy)]
enum PendingUploadTarget {
    Active,
    Idle,
}

impl PendingUploadTarget {
    fn label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Idle => "idle",
        }
    }

    fn matches(self, pending_uploads: usize) -> bool {
        match self {
            Self::Active => pending_uploads > 0,
            Self::Idle => pending_uploads == 0,
        }
    }
}

struct PendingUploadObservation {
    target: PendingUploadTarget,
    observed: bool,
    waited: Duration,
    pending_uploads: usize,
}

async fn scan_pending_uploads(upload_dir: &Path) -> Result<usize> {
    let mut entries = match fs::read_dir(upload_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err).with_context(|| format!("read {}", upload_dir.display())),
    };

    let mut pending_uploads = 0usize;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("read {}", upload_dir.display()))?
    {
        if entry.path().extension().is_some_and(|ext| ext == "pending") {
            pending_uploads = pending_uploads.saturating_add(1);
        }
    }

    Ok(pending_uploads)
}

async fn wait_for_pending_upload_target(
    upload_dir: &Path,
    target: PendingUploadTarget,
    timeout: Duration,
) -> Result<PendingUploadObservation> {
    let start = Instant::now();
    loop {
        let pending_uploads = scan_pending_uploads(upload_dir).await?;
        if target.matches(pending_uploads) {
            return Ok(PendingUploadObservation {
                target,
                observed: true,
                waited: start.elapsed(),
                pending_uploads,
            });
        }
        if start.elapsed() >= timeout {
            return Ok(PendingUploadObservation {
                target,
                observed: false,
                waited: start.elapsed(),
                pending_uploads,
            });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run_mounted_write_phase(
    params: &TestCacheParams,
    files: &[WorkFile],
    order: &[usize],
    size_class_bounds: SizeClassBounds,
) -> Result<(PhaseTelemetry, Vec<PhaseTelemetry>)> {
    if params.sequential_streams {
        let writes_by_stream = run_filesystem_writes_sequential_streams(
            files,
            order,
            params.concurrency,
            params.io_depth,
            size_class_bounds,
        )
        .await?;
        let mut merged = PhaseTelemetry::default();
        for stream in &writes_by_stream {
            merged.merge_from_ref(stream);
        }
        Ok((merged, writes_by_stream))
    } else {
        Ok((
            run_filesystem_writes(
                files,
                order,
                params.io_depth,
                params.concurrency,
                size_class_bounds,
            )
            .await?,
            Vec::new(),
        ))
    }
}

async fn run_mounted_read_phase(
    params: &TestCacheParams,
    files: &[WorkFile],
    order: &[usize],
    size_class_bounds: SizeClassBounds,
) -> Result<(PhaseTelemetry, Vec<PhaseTelemetry>)> {
    if params.sequential_streams {
        let reads_by_stream = run_filesystem_reads_sequential_streams(
            files,
            order,
            params.concurrency,
            params.io_depth,
            params.read_size,
            size_class_bounds,
        )
        .await?;
        let mut merged = PhaseTelemetry::default();
        for stream in &reads_by_stream {
            merged.merge_from_ref(stream);
        }
        Ok((merged, reads_by_stream))
    } else {
        Ok((
            run_filesystem_reads(
                files,
                order,
                params.io_depth,
                params.concurrency,
                params.read_size,
                size_class_bounds,
            )
            .await?,
            Vec::new(),
        ))
    }
}

fn phase_telemetry_to_report(telemetry: &PhaseTelemetry, wall_elapsed: Duration) -> OpReport {
    telemetry
        .overall
        .to_report(wall_elapsed.as_secs_f64() * 1000.0)
}

async fn run_shim_writes(
    cache: Arc<BlockCache>,
    sizes: &[u64],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
    size_class_bounds: SizeClassBounds,
) -> Result<PhaseTelemetry> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(PhaseTelemetry::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let cache = Arc::clone(&cache);
        let order = order.to_vec();
        let sizes = sizes.to_vec();
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("shim write semaphore closed: {e}"))?;
                let _permit = permit;

                let data_len =
                    usize::try_from(sizes[work_index]).context("shim write size overflow")?;
                let object_id = (work_index as u64) + 1;
                let data = deterministic_bytes(
                    data_len,
                    object_id
                        .saturating_mul(31)
                        .saturating_add(worker_id as u64),
                );

                let start = Instant::now();
                cache
                    .put(object_id, Bytes::from(data))
                    .await
                    .context("shim put failed")?;
                let elapsed = start.elapsed();
                let class = classify_size(
                    sizes[work_index],
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );

                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("shim write latency stats lock poisoned"))?;
                s.record(class, elapsed, sizes[work_index]);
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .context("shim write worker panicked")?
            .context("shim write worker failed")?;
    }

    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("shim write worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("shim write latency stats lock poisoned"))
}

async fn run_shim_reads(
    cache: Arc<BlockCache>,
    sizes: &[u64],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
    read_size: u64,
    size_class_bounds: SizeClassBounds,
) -> Result<DirectReadTelemetry> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(DirectReadTelemetry::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let cache = Arc::clone(&cache);
        let order = order.to_vec();
        let sizes = sizes.to_vec();
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("shim read semaphore closed: {e}"))?;
                let _permit = permit;

                let object_id = (work_index as u64) + 1;
                let op_start = Instant::now();
                let (payload, trace) = cache.get_with_trace(object_id).await;
                let payload =
                    payload.with_context(|| format!("shim read missed object id {object_id}"))?;
                let read = usize::try_from(read_size.min(sizes[work_index]).max(1))
                    .context("shim read size overflow")?;
                if payload.is_empty() {
                    return Err(anyhow!(
                        "shim read returned empty payload for object id {object_id}"
                    ));
                }
                if payload.len() < read {
                    return Err(anyhow!(
                        "shim read requested {} bytes but object {} had only {} bytes",
                        read,
                        object_id,
                        payload.len()
                    ));
                }

                let elapsed = op_start.elapsed();
                let class = classify_size(
                    sizes[work_index],
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("shim read latency stats lock poisoned"))?;
                s.phase.record(class, elapsed, read as u64);
                match trace.level {
                    CacheReadLevel::L1 => {
                        s.cache_levels
                            .record_l1(true, trace.l1_lookup_latency, read as u64);
                    }
                    CacheReadLevel::Miss => {
                        s.cache_levels.record_l1(false, trace.l1_lookup_latency, 0);
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .context("shim read worker panicked")?
            .context("shim read worker failed")?;
    }

    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("shim read worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("shim read latency stats lock poisoned"))
}

async fn run_shim_writes_sequential_streams(
    cache: Arc<BlockCache>,
    sizes: &[u64],
    order: &[usize],
    stream_count: usize,
    io_depth: usize,
    size_class_bounds: SizeClassBounds,
) -> Result<Vec<PhaseTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let cache = Arc::clone(&cache);
        let sem = Arc::clone(&sem);
        let sizes = sizes.to_vec();
        handles.push(tokio::spawn(async move {
            let mut telemetry = PhaseTelemetry::default();
            for work_index in segment {
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("shim sequential write semaphore closed: {e}"))?;
                let _permit = permit;
                let data_len = usize::try_from(sizes[work_index])
                    .context("shim sequential write size overflow")?;
                let object_id = (work_index as u64) + 1;
                let data = deterministic_bytes(
                    data_len,
                    object_id
                        .saturating_mul(31)
                        .saturating_add(stream_id as u64),
                );
                let start = Instant::now();
                cache
                    .put(object_id, Bytes::from(data))
                    .await
                    .context("shim sequential put failed")?;
                let elapsed = start.elapsed();
                let class = classify_size(
                    sizes[work_index],
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                telemetry.record(class, elapsed, sizes[work_index]);
            }
            Ok::<_, anyhow::Error>((stream_id, telemetry))
        }));
    }

    let mut by_stream = vec![PhaseTelemetry::default(); handles.len()];
    for handle in handles {
        let (stream_id, telemetry) = handle
            .await
            .context("shim sequential write stream panicked")?
            .context("shim sequential write stream failed")?;
        by_stream[stream_id] = telemetry;
    }
    Ok(by_stream)
}

async fn run_shim_reads_sequential_streams(
    cache: Arc<BlockCache>,
    sizes: &[u64],
    order: &[usize],
    stream_count: usize,
    io_depth: usize,
    read_size: u64,
    size_class_bounds: SizeClassBounds,
) -> Result<Vec<DirectReadTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, DirectReadTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let cache = Arc::clone(&cache);
        let sem = Arc::clone(&sem);
        let sizes = sizes.to_vec();
        handles.push(tokio::spawn(async move {
            let mut telemetry = DirectReadTelemetry::default();
            for work_index in segment {
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("shim sequential read semaphore closed: {e}"))?;
                let _permit = permit;

                let object_id = (work_index as u64) + 1;
                let op_start = Instant::now();
                let (payload, trace) = cache.get_with_trace(object_id).await;
                let payload = payload.with_context(|| {
                    format!("shim sequential read missed object id {object_id}")
                })?;
                let read = usize::try_from(read_size.min(sizes[work_index]).max(1))
                    .context("shim sequential read size overflow")?;
                if payload.len() < read {
                    return Err(anyhow!(
                        "shim sequential read requested {} bytes but object {} had only {} bytes",
                        read,
                        object_id,
                        payload.len()
                    ));
                }
                let elapsed = op_start.elapsed();
                let class = classify_size(
                    sizes[work_index],
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                telemetry.phase.record(class, elapsed, read as u64);
                match trace.level {
                    CacheReadLevel::L1 => {
                        telemetry.cache_levels.record_l1(
                            true,
                            trace.l1_lookup_latency,
                            read as u64,
                        );
                    }
                    CacheReadLevel::Miss => {
                        telemetry
                            .cache_levels
                            .record_l1(false, trace.l1_lookup_latency, 0);
                    }
                }
            }
            Ok::<_, anyhow::Error>((stream_id, telemetry))
        }));
    }

    let mut by_stream = vec![DirectReadTelemetry::default(); handles.len()];
    for handle in handles {
        let (stream_id, telemetry) = handle
            .await
            .context("shim sequential read stream panicked")?
            .context("shim sequential read stream failed")?;
        by_stream[stream_id] = telemetry;
    }
    Ok(by_stream)
}

async fn run_filesystem_writes(
    files: &[WorkFile],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
    size_class_bounds: SizeClassBounds,
) -> Result<PhaseTelemetry> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(PhaseTelemetry::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let files = files.to_vec();
        let order = order.to_vec();
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let item = &files[work_index];
                let data = deterministic_bytes(
                    usize::try_from(item.size).context("file write size overflow")?,
                    item.index as u64 + 123,
                );
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem write semaphore closed: {e}"))?;
                let _permit = permit;

                let start = Instant::now();
                let mut out = fs::File::create(&item.path)
                    .await
                    .with_context(|| format!("create {}", item.path.display()))?;
                out.write_all(&data)
                    .await
                    .with_context(|| format!("write {}", item.path.display()))?;
                out.flush().await?;

                let elapsed = start.elapsed();
                let class = classify_size(
                    item.size,
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("filesystem write latency stats lock poisoned"))?;
                s.record(class, elapsed, item.size);
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .context("filesystem write worker panicked")?
            .context("filesystem write worker failed")?;
    }

    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("filesystem write worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("filesystem write latency stats lock poisoned"))
}

async fn run_filesystem_reads(
    files: &[WorkFile],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
    read_size: u64,
    size_class_bounds: SizeClassBounds,
) -> Result<PhaseTelemetry> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(PhaseTelemetry::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let files = files.to_vec();
        let order = order.to_vec();
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let item = &files[work_index];
                let read_len = read_size.min(item.size).max(1) as usize;

                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem read semaphore closed: {e}"))?;
                let _permit = permit;

                let start = Instant::now();
                let mut in_f = fs::File::open(&item.path)
                    .await
                    .with_context(|| format!("open {}", item.path.display()))?;
                let mut buf = vec![0u8; read_len];
                in_f.read_exact(&mut buf)
                    .await
                    .with_context(|| format!("read {}", item.path.display()))?;
                let elapsed = start.elapsed();
                let class = classify_size(
                    item.size,
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );

                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("filesystem read latency stats lock poisoned"))?;
                s.record(class, elapsed, read_len as u64);
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .context("filesystem read worker panicked")?
            .context("filesystem read worker failed")?;
    }

    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("filesystem read worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("filesystem read latency stats lock poisoned"))
}

async fn run_filesystem_writes_sequential_streams(
    files: &[WorkFile],
    order: &[usize],
    stream_count: usize,
    io_depth: usize,
    size_class_bounds: SizeClassBounds,
) -> Result<Vec<PhaseTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let files = files.to_vec();
        handles.push(tokio::spawn(async move {
            let mut telemetry = PhaseTelemetry::default();
            for work_index in segment {
                let item = &files[work_index];
                let data = deterministic_bytes(
                    usize::try_from(item.size).context("sequential file write size overflow")?,
                    item.index as u64 + 123 + stream_id as u64,
                );
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem sequential write semaphore closed: {e}"))?;
                let _permit = permit;
                let start = Instant::now();
                let mut out = fs::File::create(&item.path)
                    .await
                    .with_context(|| format!("create {}", item.path.display()))?;
                out.write_all(&data)
                    .await
                    .with_context(|| format!("write {}", item.path.display()))?;
                out.flush().await?;
                let elapsed = start.elapsed();
                let class = classify_size(
                    item.size,
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                telemetry.record(class, elapsed, item.size);
            }
            Ok::<_, anyhow::Error>((stream_id, telemetry))
        }));
    }

    let mut by_stream = vec![PhaseTelemetry::default(); handles.len()];
    for handle in handles {
        let (stream_id, telemetry) = handle
            .await
            .context("filesystem sequential write stream panicked")?
            .context("filesystem sequential write stream failed")?;
        by_stream[stream_id] = telemetry;
    }
    Ok(by_stream)
}

async fn run_filesystem_reads_sequential_streams(
    files: &[WorkFile],
    order: &[usize],
    stream_count: usize,
    io_depth: usize,
    read_size: u64,
    size_class_bounds: SizeClassBounds,
) -> Result<Vec<PhaseTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let files = files.to_vec();
        handles.push(tokio::spawn(async move {
            let mut telemetry = PhaseTelemetry::default();
            for work_index in segment {
                let item = &files[work_index];
                let read_len = read_size.min(item.size).max(1) as usize;
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem sequential read semaphore closed: {e}"))?;
                let _permit = permit;
                let start = Instant::now();
                let mut in_f = fs::File::open(&item.path)
                    .await
                    .with_context(|| format!("open {}", item.path.display()))?;
                let mut buf = vec![0u8; read_len];
                in_f.read_exact(&mut buf)
                    .await
                    .with_context(|| format!("read {}", item.path.display()))?;
                let elapsed = start.elapsed();
                let class = classify_size(
                    item.size,
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                telemetry.record(class, elapsed, read_len as u64);
            }
            Ok::<_, anyhow::Error>((stream_id, telemetry))
        }));
    }

    let mut by_stream = vec![PhaseTelemetry::default(); handles.len()];
    for handle in handles {
        let (stream_id, telemetry) = handle
            .await
            .context("filesystem sequential read stream panicked")?
            .context("filesystem sequential read stream failed")?;
        by_stream[stream_id] = telemetry;
    }
    Ok(by_stream)
}

fn clear_mount_artifacts(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path).with_context(|| format!("remove {}", path.display()))?;
    }
    std::fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))
}

fn normalize_path_for_compare(path: &Path) -> Option<String> {
    let raw = path.to_str()?;
    #[cfg(windows)]
    {
        Some(
            super::utils::normalize_mount_point(raw)
                .replace('/', "\\")
                .trim_end_matches('\\')
                .to_lowercase(),
        )
    }
    #[cfg(not(windows))]
    {
        Some(raw.trim_end_matches('/').to_string())
    }
}

fn cleanup_stale_mounts() -> Result<()> {
    let mounts_dir = super::utils::get_mounts_dir()?;
    for entry in std::fs::read_dir(&mounts_dir)
        .with_context(|| format!("read mounts dir {}", mounts_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let parsed: serde_json::Value =
            serde_json::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
        let Some(pid) = parsed.get("pid").and_then(serde_json::Value::as_u64) else {
            continue;
        };
        let Some(mount_point) = parsed
            .get("mount_point")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if !super::utils::is_process_running(pid as u32) {
            let _ = super::utils::remove_pid_file(mount_point);
        }
    }
    Ok(())
}

fn is_path_under_mount(target: &Path, mount_point: &Path) -> bool {
    let Some(target_norm) = normalize_path_for_compare(target) else {
        return false;
    };
    let Some(mount_norm) = normalize_path_for_compare(mount_point) else {
        return false;
    };
    target_norm == mount_norm
        || target_norm
            .strip_prefix(&mount_norm)
            .is_some_and(|tail| tail.starts_with('\\') || tail.starts_with('/'))
}

fn active_mounts() -> Result<Vec<ActiveMount>> {
    let mounts_dir = super::utils::get_mounts_dir()?;
    let mut mounts = Vec::new();
    for entry in std::fs::read_dir(&mounts_dir)
        .with_context(|| format!("read mounts dir {}", mounts_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let parsed: serde_json::Value =
            serde_json::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
        let Some(pid) = parsed.get("pid").and_then(serde_json::Value::as_u64) else {
            continue;
        };
        let Some(mount_point) = parsed
            .get("mount_point")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let volume = parsed
            .get("volume")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let cache_path = parsed
            .get("cache_path")
            .and_then(serde_json::Value::as_str)
            .map(PathBuf::from);
        if !super::utils::is_process_running(pid as u32) {
            continue;
        }
        mounts.push(ActiveMount {
            mount_point: PathBuf::from(mount_point),
            volume: volume.to_string(),
            cache_path,
        });
    }
    Ok(mounts)
}

fn resolve_run_mode(target: &Path) -> Result<ResolvedRunMode> {
    cleanup_stale_mounts()?;
    let mut best: Option<ActiveMount> = None;
    for mount in active_mounts()? {
        if is_path_under_mount(target, &mount.mount_point) {
            let keep = best.as_ref().is_none_or(|existing| {
                mount.mount_point.components().count() > existing.mount_point.components().count()
            });
            if keep {
                best = Some(mount);
            }
        }
    }

    if let Some(active) = best {
        Ok(ResolvedRunMode::MountedE2E {
            volume: active.volume,
            mount_point: active.mount_point,
            work_root: target.join(".studiofs-test-cache"),
            cache_path: active.cache_path,
        })
    } else {
        Ok(ResolvedRunMode::DirectShim {
            cache_dir: target.to_path_buf(),
        })
    }
}

fn process_cpu_time() -> Option<Duration> {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: `getrusage` initializes the provided pointer when it returns 0.
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if rc != 0 {
            return None;
        }
        // SAFETY: guarded by successful `getrusage` above.
        let usage = unsafe { usage.assume_init() };
        let user = Duration::new(
            usage.ru_utime.tv_sec as u64,
            (usage.ru_utime.tv_usec as u32).saturating_mul(1_000),
        );
        let sys = Duration::new(
            usage.ru_stime.tv_sec as u64,
            (usage.ru_stime.tv_usec as u32).saturating_mul(1_000),
        );
        Some(user.saturating_add(sys))
    }

    #[cfg(windows)]
    {
        #[repr(C)]
        #[derive(Default)]
        struct FileTime {
            dw_low_date_time: u32,
            dw_high_date_time: u32,
        }
        unsafe extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn GetProcessTimes(
                hProcess: *mut std::ffi::c_void,
                lpCreationTime: *mut FileTime,
                lpExitTime: *mut FileTime,
                lpKernelTime: *mut FileTime,
                lpUserTime: *mut FileTime,
            ) -> i32;
        }
        let mut creation = FileTime::default();
        let mut exit = FileTime::default();
        let mut kernel = FileTime::default();
        let mut user = FileTime::default();
        // SAFETY: pointers are valid and writable for this call.
        let ok = unsafe {
            GetProcessTimes(
                GetCurrentProcess(),
                &mut creation,
                &mut exit,
                &mut kernel,
                &mut user,
            )
        };
        if ok == 0 {
            tracing::warn!(
                target: "studiofs.cache.profile",
                "GetProcessTimes failed; CPU profile sample unavailable on this call"
            );
            return None;
        }
        let ft_to_u64 =
            |ft: FileTime| ((ft.dw_high_date_time as u64) << 32) | ft.dw_low_date_time as u64;
        let total_100ns = ft_to_u64(kernel).saturating_add(ft_to_u64(user));
        Some(Duration::from_nanos(total_100ns.saturating_mul(100)))
    }

    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

fn cpu_utilization_percent(cpu: Duration, wall: Duration) -> f64 {
    let wall_ns = wall.as_nanos() as f64;
    if wall_ns <= 0.0 {
        0.0
    } else {
        (cpu.as_nanos() as f64 / wall_ns) * 100.0
    }
}

async fn run_direct(params: &TestCacheParams, cache_dir: &Path) -> Result<BenchmarkReport> {
    let cache_config = BlockCacheConfig::new(cache_dir, params.l1_limit);
    let (cache, _rx) = BlockCache::open(cache_config)
        .with_context(|| format!("open cache dir {}", cache_dir.display()))?;
    cache.spawn_background_tasks();

    let size_class_bounds = SizeClassBounds {
        small_max: params.size_class_small_max,
        medium_max: params.size_class_medium_max,
    };
    let workload = build_work_sizes(params.block_size, &params.sizes, size_class_bounds);
    let work_sizes = workload.sizes.clone();
    let order = build_access_sequence(
        work_sizes.len(),
        params.access_pattern,
        0x1234_5678_9abc_def0,
    );

    for warmup in 0..params.warmup_runs {
        tracing::info!(
            target: "studiofs.cache.profile",
            run_id = %params.run_id,
            warmup = warmup + 1,
            total_warmups = params.warmup_runs,
            "cache benchmark warm-up cycle started"
        );
        if params.sequential_streams {
            let _ = run_shim_writes_sequential_streams(
                Arc::clone(&cache),
                &work_sizes,
                &order,
                params.concurrency,
                params.io_depth,
                size_class_bounds,
            )
            .await?;
            let _ = run_shim_reads_sequential_streams(
                Arc::clone(&cache),
                &work_sizes,
                &order,
                params.concurrency,
                params.io_depth,
                params.read_size,
                size_class_bounds,
            )
            .await?;
        } else {
            let _ = run_shim_writes(
                Arc::clone(&cache),
                &work_sizes,
                &order,
                params.io_depth,
                params.concurrency,
                size_class_bounds,
            )
            .await?;
            let _ = run_shim_reads(
                Arc::clone(&cache),
                &work_sizes,
                &order,
                params.io_depth,
                params.concurrency,
                params.read_size,
                size_class_bounds,
            )
            .await?;
        }
    }

    let started = Instant::now();
    let started_cpu = process_cpu_time();
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    tracing::info!(
        target: "studiofs.cache.profile",
        run_id = %params.run_id,
        phase = "direct-shim-write",
        "cache benchmark phase start"
    );
    let writes_wall_start = Instant::now();
    let writes_cpu_start = process_cpu_time();
    let (writes, writes_by_stream) = if params.sequential_streams {
        let writes_by_stream = run_shim_writes_sequential_streams(
            Arc::clone(&cache),
            &work_sizes,
            &order,
            params.concurrency,
            params.io_depth,
            size_class_bounds,
        )
        .await?;
        let mut merged = PhaseTelemetry::default();
        for stream in &writes_by_stream {
            merged.merge_from_ref(stream);
        }
        (merged, writes_by_stream)
    } else {
        (
            run_shim_writes(
                Arc::clone(&cache),
                &work_sizes,
                &order,
                params.io_depth,
                params.concurrency,
                size_class_bounds,
            )
            .await?,
            Vec::new(),
        )
    };
    let writes_wall = writes_wall_start.elapsed();
    let writes_cpu = writes_cpu_start
        .and_then(|start_cpu| process_cpu_time().map(|end_cpu| end_cpu.saturating_sub(start_cpu)))
        .unwrap_or_default();

    tracing::info!(
        target: "studiofs.cache.profile",
        run_id = %params.run_id,
        phase = "direct-shim-read",
        "cache benchmark phase start"
    );
    let reads_wall_start = Instant::now();
    let reads_cpu_start = process_cpu_time();
    let (reads, reads_by_stream) = if params.sequential_streams {
        let reads_by_stream = run_shim_reads_sequential_streams(
            Arc::clone(&cache),
            &work_sizes,
            &order,
            params.concurrency,
            params.io_depth,
            params.read_size,
            size_class_bounds,
        )
        .await?;
        let mut merged = DirectReadTelemetry::default();
        for stream in &reads_by_stream {
            merged.merge_from_ref(stream);
        }
        (merged, reads_by_stream)
    } else {
        (
            run_shim_reads(
                Arc::clone(&cache),
                &work_sizes,
                &order,
                params.io_depth,
                params.concurrency,
                params.read_size,
                size_class_bounds,
            )
            .await?,
            Vec::new(),
        )
    };
    let reads_wall = reads_wall_start.elapsed();
    let reads_cpu = reads_cpu_start
        .and_then(|start_cpu| process_cpu_time().map(|end_cpu| end_cpu.saturating_sub(start_cpu)))
        .unwrap_or_default();

    let completed = Instant::now();
    let total_elapsed_ms = (completed - started).as_secs_f64() * 1000.0;
    let cache_stats = cache.stats();
    let total_wall = completed - started;
    let total_cpu = started_cpu
        .and_then(|cpu_start| process_cpu_time().map(|cpu_end| cpu_end.saturating_sub(cpu_start)))
        .unwrap_or_default();
    let profile = params.profile.then_some(BenchmarkProfileReport {
        enabled: true,
        profile_mode: "pyroscope".to_string(),
        cpu_profile: CpuProfileReport {
            total_wall_ms: total_wall.as_secs_f64() * 1000.0,
            total_cpu_ms: total_cpu.as_secs_f64() * 1000.0,
            cpu_utilization_percent: cpu_utilization_percent(total_cpu, total_wall),
            write_phase_cpu_percent: cpu_utilization_percent(writes_cpu, writes_wall),
            read_phase_cpu_percent: cpu_utilization_percent(reads_cpu, reads_wall),
        },
        endpoint: None,
        artifacts: Vec::new(),
        warnings: Vec::new(),
    });
    let cache_pipeline: CachePipelineReport = cache.cache_pipeline_metrics().into();
    let advanced_metrics = build_advanced_metrics(
        size_class_bounds,
        &writes,
        &reads.phase,
        Some(&reads.cache_levels),
        Some(&cache_pipeline),
        writes_wall.as_secs_f64() * 1000.0,
        reads_wall.as_secs_f64() * 1000.0,
    );
    let reads_phase_by_stream: Vec<PhaseTelemetry> = reads_by_stream
        .iter()
        .map(|item| item.phase.clone())
        .collect();
    let stream_results = stream_reports_from_telemetry(
        &writes_by_stream,
        &reads_phase_by_stream,
        writes_wall.as_secs_f64() * 1000.0,
        reads_wall.as_secs_f64() * 1000.0,
    );

    Ok(BenchmarkReport {
        run_id: params.run_id.clone(),
        mode: "direct-shim".to_string(),
        cache_dir: cache_dir.display().to_string(),
        started_at,
        completed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        total_elapsed_ms,
        block_size: params.block_size,
        read_size: params.read_size,
        concurrency: params.concurrency,
        sequential_streams: params.sequential_streams,
        io_depth: params.io_depth,
        warmup_runs: params.warmup_runs,
        sizes: workload.source,
        workload_files: work_sizes.len(),
        workload_small_files: workload.small_files,
        workload_medium_files: workload.medium_files,
        workload_large_files: workload.large_files,
        access_pattern: format!("{:?}", params.access_pattern).to_lowercase(),
        l1_limit: params.l1_limit,
        volume: None,
        mount_point: None,
        l1_bytes: Some(cache_stats.l1_bytes),
        cache_pipeline: Some(cache_pipeline),
        profile,
        advanced_metrics: Some(advanced_metrics),
        buffered_read_comparison: None,
        stream_results,
        work_root: None,
        writes: writes.overall.to_report(writes_wall.as_secs_f64() * 1000.0),
        reads: reads
            .phase
            .overall
            .to_report(reads_wall.as_secs_f64() * 1000.0),
    })
}

async fn run_mounted_end_to_end(
    params: &TestCacheParams,
    volume: &str,
    mount_point: &Path,
    work_root: &Path,
    cache_path: Option<&Path>,
) -> Result<BenchmarkReport> {
    let benchmark_result = async {
        clear_mount_artifacts(work_root).context("prepare mount benchmark directory")?;

        let size_class_bounds = SizeClassBounds {
            small_max: params.size_class_small_max,
            medium_max: params.size_class_medium_max,
        };
        let workload = build_work_sizes(params.block_size, &params.sizes, size_class_bounds);
        if workload.sizes.is_empty() {
            anyhow::bail!("nothing to benchmark");
        }
        let files = build_file_worklist(&workload.sizes, work_root);
        let order =
            build_access_sequence(files.len(), params.access_pattern, 0x55aa_55aa_d00d_f00d);

        for warmup in 0..params.warmup_runs {
            tracing::info!(
                target: "studiofs.cache.profile",
                run_id = %params.run_id,
                warmup = warmup + 1,
                total_warmups = params.warmup_runs,
                "cache benchmark warm-up cycle started"
            );
            let _ = run_mounted_write_phase(params, &files, &order, size_class_bounds).await?;
            let _ = run_mounted_read_phase(params, &files, &order, size_class_bounds).await?;
        }

        let cache_upload_dir = cache_path.map(|path| path.join("upload"));
        let mut baseline_buffered_read_phase = None;
        if let Some(upload_dir) = cache_upload_dir.as_deref() {
            let cache_path_for_log = upload_dir.parent().unwrap_or(upload_dir);
            tracing::info!(
                target: "studiofs.cache.profile",
                run_id = %params.run_id,
                phase = "e2e-baseline-seed-write",
                cache_path = %cache_path_for_log.display(),
                upload_dir = %upload_dir.display(),
                "cache benchmark phase start"
            );
            let _ = run_mounted_write_phase(params, &files, &order, size_class_bounds).await?;
            let _ = wait_for_pending_upload_target(
                upload_dir,
                PendingUploadTarget::Active,
                Duration::from_secs(10),
            )
            .await?;
            let idle_before_baseline = wait_for_pending_upload_target(
                upload_dir,
                PendingUploadTarget::Idle,
                Duration::from_secs(60),
            )
            .await?;

            tracing::info!(
                target: "studiofs.cache.profile",
                run_id = %params.run_id,
                phase = "e2e-baseline-read",
                cache_path = %cache_path_for_log.display(),
                upload_dir = %upload_dir.display(),
                "cache benchmark phase start"
            );
            let baseline_wall_start = Instant::now();
            let (baseline_reads, _) =
                run_mounted_read_phase(params, &files, &order, size_class_bounds).await?;
            let baseline_wall = baseline_wall_start.elapsed();
            baseline_buffered_read_phase = Some(MountedBufferedReadPhaseReport {
                phase: "baseline-read".to_string(),
                wait_target: idle_before_baseline.target.label().to_string(),
                target_state_observed: idle_before_baseline.observed,
                wait_ms: idle_before_baseline.waited.as_secs_f64() * 1000.0,
                pending_uploads_before: idle_before_baseline.pending_uploads,
                pending_uploads_after: scan_pending_uploads(upload_dir).await?,
                results: phase_telemetry_to_report(&baseline_reads, baseline_wall),
            });
        }

        let started = Instant::now();
        let started_cpu = process_cpu_time();
        let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        tracing::info!(
            target: "studiofs.cache.profile",
            run_id = %params.run_id,
            phase = "e2e-write",
            "cache benchmark phase start"
        );
        let writes_wall_start = Instant::now();
        let writes_cpu_start = process_cpu_time();
        let (writes, writes_by_stream) =
            run_mounted_write_phase(params, &files, &order, size_class_bounds).await?;
        let writes_wall = writes_wall_start.elapsed();
        let writes_cpu = writes_cpu_start
            .and_then(|start_cpu| {
                process_cpu_time().map(|end_cpu| end_cpu.saturating_sub(start_cpu))
            })
            .unwrap_or_default();

        let during_upload_observation = if let Some(upload_dir) = cache_upload_dir.as_deref() {
            Some(
                wait_for_pending_upload_target(
                    upload_dir,
                    PendingUploadTarget::Active,
                    Duration::from_secs(10),
                )
                .await?,
            )
        } else {
            None
        };

        tracing::info!(
            target: "studiofs.cache.profile",
            run_id = %params.run_id,
            phase = "e2e-read",
            "cache benchmark phase start"
        );
        let reads_wall_start = Instant::now();
        let reads_cpu_start = process_cpu_time();
        let (reads, reads_by_stream) =
            run_mounted_read_phase(params, &files, &order, size_class_bounds).await?;
        let reads_wall = reads_wall_start.elapsed();
        let reads_cpu = reads_cpu_start
            .and_then(|start_cpu| {
                process_cpu_time().map(|end_cpu| end_cpu.saturating_sub(start_cpu))
            })
            .unwrap_or_default();

        let total_wall = Instant::now() - started;
        let total_cpu = started_cpu
            .and_then(|cpu_start| {
                process_cpu_time().map(|cpu_end| cpu_end.saturating_sub(cpu_start))
            })
            .unwrap_or_default();
        let profile = params.profile.then_some(BenchmarkProfileReport {
            enabled: true,
            profile_mode: "pyroscope".to_string(),
            cpu_profile: CpuProfileReport {
                total_wall_ms: total_wall.as_secs_f64() * 1000.0,
                total_cpu_ms: total_cpu.as_secs_f64() * 1000.0,
                cpu_utilization_percent: cpu_utilization_percent(total_cpu, total_wall),
                write_phase_cpu_percent: cpu_utilization_percent(writes_cpu, writes_wall),
                read_phase_cpu_percent: cpu_utilization_percent(reads_cpu, reads_wall),
            },
            endpoint: None,
            artifacts: Vec::new(),
            warnings: Vec::new(),
        });
        let buffered_read_comparison = match (
            cache_path,
            cache_upload_dir.as_deref(),
            baseline_buffered_read_phase,
        ) {
            (Some(cache_path), Some(upload_dir), Some(baseline_phase)) => {
                let during_phase = MountedBufferedReadPhaseReport {
                    phase: "during-upload-read".to_string(),
                    wait_target: during_upload_observation
                        .as_ref()
                        .map(|observation| observation.target.label().to_string())
                        .unwrap_or_else(|| PendingUploadTarget::Active.label().to_string()),
                    target_state_observed: during_upload_observation
                        .as_ref()
                        .map(|observation| observation.observed)
                        .unwrap_or(false),
                    wait_ms: during_upload_observation
                        .as_ref()
                        .map(|observation| observation.waited.as_secs_f64() * 1000.0)
                        .unwrap_or_default(),
                    pending_uploads_before: during_upload_observation
                        .as_ref()
                        .map(|observation| observation.pending_uploads)
                        .unwrap_or_default(),
                    pending_uploads_after: scan_pending_uploads(upload_dir).await?,
                    results: phase_telemetry_to_report(&reads, reads_wall),
                };

                let idle_before_post = wait_for_pending_upload_target(
                    upload_dir,
                    PendingUploadTarget::Idle,
                    Duration::from_secs(60),
                )
                .await?;
                let cache_path_for_log = upload_dir.parent().unwrap_or(upload_dir);
                tracing::info!(
                    target: "studiofs.cache.profile",
                    run_id = %params.run_id,
                    phase = "e2e-post-upload-read",
                    cache_path = %cache_path_for_log.display(),
                    upload_dir = %upload_dir.display(),
                    "cache benchmark phase start"
                );
                let post_wall_start = Instant::now();
                let (post_reads, _) =
                    run_mounted_read_phase(params, &files, &order, size_class_bounds).await?;
                let post_wall = post_wall_start.elapsed();
                let post_phase = MountedBufferedReadPhaseReport {
                    phase: "post-upload-read".to_string(),
                    wait_target: idle_before_post.target.label().to_string(),
                    target_state_observed: idle_before_post.observed,
                    wait_ms: idle_before_post.waited.as_secs_f64() * 1000.0,
                    pending_uploads_before: idle_before_post.pending_uploads,
                    pending_uploads_after: scan_pending_uploads(upload_dir).await?,
                    results: phase_telemetry_to_report(&post_reads, post_wall),
                };

                Some(MountedBufferedReadComparisonReport {
                    cache_path: cache_path.display().to_string(),
                    upload_dir: upload_dir.display().to_string(),
                    phases: vec![baseline_phase, during_phase, post_phase],
                })
            }
            _ => None,
        };
        let advanced_metrics = build_advanced_metrics(
            size_class_bounds,
            &writes,
            &reads,
            None,
            None,
            writes_wall.as_secs_f64() * 1000.0,
            reads_wall.as_secs_f64() * 1000.0,
        );
        let stream_results = stream_reports_from_telemetry(
            &writes_by_stream,
            &reads_by_stream,
            writes_wall.as_secs_f64() * 1000.0,
            reads_wall.as_secs_f64() * 1000.0,
        );

        let _ = fs::remove_dir_all(work_root).await;
        let total_elapsed_ms = (Instant::now() - started).as_secs_f64() * 1000.0;
        let completed_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        Ok::<_, anyhow::Error>(BenchmarkReport {
            run_id: params.run_id.clone(),
            mode: "e2e".to_string(),
            cache_dir: params.target.display().to_string(),
            started_at,
            completed_at,
            total_elapsed_ms,
            block_size: params.block_size,
            read_size: params.read_size,
            concurrency: params.concurrency,
            sequential_streams: params.sequential_streams,
            io_depth: params.io_depth,
            warmup_runs: params.warmup_runs,
            sizes: workload.source,
            workload_files: files.len(),
            workload_small_files: workload.small_files,
            workload_medium_files: workload.medium_files,
            workload_large_files: workload.large_files,
            access_pattern: format!("{:?}", params.access_pattern).to_lowercase(),
            l1_limit: params.l1_limit,
            work_root: Some(work_root.display().to_string()),
            volume: Some(volume.to_string()),
            mount_point: Some(mount_point.display().to_string()),
            l1_bytes: None,
            cache_pipeline: None,
            profile,
            advanced_metrics: Some(advanced_metrics),
            stream_results,
            buffered_read_comparison,
            writes: writes.overall.to_report(writes_wall.as_secs_f64() * 1000.0),
            reads: reads.overall.to_report(reads_wall.as_secs_f64() * 1000.0),
        })
    }
    .await;
    match benchmark_result {
        Ok(report) => Ok(report),
        Err(err) => Err(err),
    }
}

fn format_duration_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2} s", ms / 1000.0)
    } else {
        format!("{ms:.2} ms")
    }
}

fn build_advanced_metrics(
    size_class_bounds: SizeClassBounds,
    writes: &PhaseTelemetry,
    reads: &PhaseTelemetry,
    cache_levels: Option<&CacheLevelTelemetryAccumulator>,
    _cache_pipeline: Option<&CachePipelineReport>,
    writes_wall_elapsed_ms: f64,
    reads_wall_elapsed_ms: f64,
) -> AdvancedMetricsReport {
    AdvancedMetricsReport {
        size_class_boundaries: SizeClassBoundaryReport {
            small_max_bytes: size_class_bounds.small_max,
            medium_max_bytes: size_class_bounds.medium_max,
        },
        size_class_stats: SizeClassStatsReport {
            writes: writes.by_size.to_reports(writes_wall_elapsed_ms),
            reads: reads.by_size.to_reports(reads_wall_elapsed_ms),
        },
        cache_levels: cache_levels.map(|levels| levels.to_report(reads_wall_elapsed_ms)),
    }
}

fn format_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    for row in rows {
        for (idx, width) in widths.iter_mut().enumerate() {
            let cell = row.get(idx).map(String::as_str).unwrap_or("");
            *width = (*width).max(cell.len());
        }
    }

    let mut out = String::new();
    let write_border = |out: &mut String| {
        out.push('+');
        for width in &widths {
            for _ in 0..(*width + 2) {
                out.push('-');
            }
            out.push('+');
        }
        out.push('\n');
    };

    let write_row = |out: &mut String, cells: &[String]| {
        out.push('|');
        for (idx, width) in widths.iter().enumerate() {
            let cell = cells.get(idx).map(String::as_str).unwrap_or("");
            let _ = write!(out, " {:<width$} |", cell, width = width);
        }
        out.push('\n');
    };

    write_border(&mut out);
    let header_cells: Vec<String> = headers.iter().map(|header| (*header).to_string()).collect();
    write_row(&mut out, &header_cells);
    write_border(&mut out);
    for row in rows {
        write_row(&mut out, row);
    }
    write_border(&mut out);
    out
}

fn slugify_report_segment(value: &str) -> String {
    let mut out = String::new();
    let mut last_was_dash = false;
    for ch in value.chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            last_was_dash = false;
        } else if !last_was_dash {
            out.push('-');
            last_was_dash = true;
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "report".to_string()
    } else {
        trimmed.to_string()
    }
}

fn default_report_file_name(report: &BenchmarkReport, now: DateTime<Utc>) -> String {
    format!(
        "studiofs-test-cache-{}-{}.json",
        slugify_report_segment(&report.mode),
        now.format("%Y%m%d-%H%M%S"),
    )
}

fn next_available_path(path: &Path, suffix: usize) -> PathBuf {
    let parent = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("studiofs-test-cache");
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    let candidate_name = if extension.is_empty() {
        format!("{stem}-{suffix}")
    } else {
        format!("{stem}-{suffix}.{extension}")
    };
    parent.join(candidate_name)
}

fn resolve_report_target(
    report: &BenchmarkReport,
    json_output: Option<&Path>,
    current_dir: &Path,
    now: DateTime<Utc>,
) -> (PathBuf, bool) {
    if let Some(json_output) = json_output {
        let explicit = if json_output.is_absolute() {
            json_output.to_path_buf()
        } else {
            current_dir.join(json_output)
        };
        if explicit.is_dir() {
            return (explicit.join(default_report_file_name(report, now)), true);
        }
        return (explicit, false);
    }

    (
        current_dir.join(default_report_file_name(report, now)),
        true,
    )
}

fn write_report_file(report_path: &Path, json: &[u8], auto_named: bool) -> Result<PathBuf> {
    if !auto_named {
        if let Some(parent) = report_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create report dir {}", parent.display()))?;
        }
        std::fs::write(report_path, json)
            .with_context(|| format!("write benchmark report {}", report_path.display()))?;
        return Ok(report_path.to_path_buf());
    }

    let mut candidate = report_path.to_path_buf();
    let mut suffix = 2usize;
    loop {
        if let Some(parent) = candidate.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create report dir {}", parent.display()))?;
        }

        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(mut file) => {
                file.write_all(json)
                    .with_context(|| format!("write benchmark report {}", candidate.display()))?;
                file.flush()
                    .with_context(|| format!("flush benchmark report {}", candidate.display()))?;
                return Ok(candidate);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                candidate = next_available_path(report_path, suffix);
                suffix = suffix.saturating_add(1);
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("create benchmark report {}", candidate.display()));
            }
        }
    }
}

fn save_benchmark_report_in(
    report: &BenchmarkReport,
    json_output: Option<&Path>,
    current_dir: &Path,
    now: DateTime<Utc>,
) -> Result<PathBuf> {
    let json =
        serde_json::to_vec_pretty(report).context("serialize benchmark report as JSON report")?;
    let (report_path, auto_named) = resolve_report_target(report, json_output, current_dir, now);
    write_report_file(&report_path, &json, auto_named)
}

fn save_benchmark_report(report: &BenchmarkReport, json_output: Option<&Path>) -> Result<PathBuf> {
    let current_dir = std::env::current_dir().context("resolve current working directory")?;
    save_benchmark_report_in(report, json_output, &current_dir, Utc::now())
}

fn render_human_summary(report: &BenchmarkReport) -> String {
    let mut out = String::new();
    let target_label = if report.mode == "direct-shim" {
        "Local path"
    } else {
        "Target"
    };

    let _ = writeln!(&mut out, "StudioFS data-path benchmark");
    let _ = writeln!(&mut out, "Run ID:    {}", report.run_id);
    let _ = writeln!(&mut out, "{target_label}: {}", report.cache_dir);
    let _ = writeln!(&mut out, "Mode:      {}", report.mode);
    if let Some(volume) = &report.volume {
        let _ = writeln!(&mut out, "Volume:    {volume}");
    }
    if let Some(mount_point) = &report.mount_point {
        let _ = writeln!(&mut out, "Mount:     {mount_point}");
    }
    if let Some(work_root) = &report.work_root {
        let _ = writeln!(&mut out, "Work root: {work_root}");
    }
    let _ = writeln!(&mut out, "Started:   {}", report.started_at);
    let _ = writeln!(&mut out, "Completed: {}", report.completed_at);
    let _ = writeln!(
        &mut out,
        "Elapsed:   {}",
        format_duration_ms(report.total_elapsed_ms)
    );
    let _ = writeln!(
        &mut out,
        "Workload:  {} files (small {}, medium {}, large {}), read {}, concurrency {}, io-depth {}, pattern {}",
        report.workload_files,
        report.workload_small_files,
        report.workload_medium_files,
        report.workload_large_files,
        format_bytes(report.read_size),
        report.concurrency,
        report.io_depth,
        report.access_pattern,
    );
    let _ = writeln!(
        &mut out,
        "Config:    sizes {}, warmups {}, sequential-streams {}",
        report.sizes, report.warmup_runs, report.sequential_streams
    );
    if report.sequential_streams {
        let effective = report.concurrency.min(report.io_depth.max(1));
        let _ = writeln!(
            &mut out,
            "Effective: sequential streams cap in-flight I/O at min(concurrency, io-depth) = {}",
            effective
        );
    }
    if let Some(advanced) = &report.advanced_metrics {
        let _ = writeln!(
            &mut out,
            "Size classes: small <= {}, medium <= {}, large > {}",
            format_bytes(advanced.size_class_boundaries.small_max_bytes),
            format_bytes(advanced.size_class_boundaries.medium_max_bytes),
            format_bytes(advanced.size_class_boundaries.medium_max_bytes),
        );
    }
    let _ = writeln!(&mut out, "Limits:    L1 {}", format_bytes(report.l1_limit),);
    if report.l1_bytes.is_some() {
        let _ = writeln!(
            &mut out,
            "Resident:  L1 {}",
            report
                .l1_bytes
                .map(format_bytes)
                .unwrap_or_else(|| "-".to_string()),
        );
    }
    out.push('\n');
    let _ = writeln!(&mut out, "Operation results");
    let operation_rows = vec![
        vec![
            "Write".to_string(),
            report.writes.operations.to_string(),
            format_bytes(report.writes.bytes),
            format_throughput(report.writes.throughput_bytes_per_sec),
            format_duration_ms(report.writes.avg_latency_ms),
            format_duration_ms(report.writes.p50_ms),
            format_duration_ms(report.writes.p95_ms),
            format_duration_ms(report.writes.p99_ms),
        ],
        vec![
            "Read".to_string(),
            report.reads.operations.to_string(),
            format_bytes(report.reads.bytes),
            format_throughput(report.reads.throughput_bytes_per_sec),
            format_duration_ms(report.reads.avg_latency_ms),
            format_duration_ms(report.reads.p50_ms),
            format_duration_ms(report.reads.p95_ms),
            format_duration_ms(report.reads.p99_ms),
        ],
    ];
    let _ = write!(
        &mut out,
        "{}",
        format_table(
            &[
                "Phase",
                "Ops",
                "Data",
                "Throughput",
                "Avg",
                "P50",
                "P95",
                "P99"
            ],
            &operation_rows,
        )
    );

    if let Some(comparison) = &report.buffered_read_comparison {
        out.push('\n');
        let _ = writeln!(&mut out, "Buffered-first read comparison");
        let _ = writeln!(&mut out, "Cache path: {}", comparison.cache_path);
        let _ = writeln!(&mut out, "Upload dir: {}", comparison.upload_dir);
        let rows: Vec<Vec<String>> = comparison
            .phases
            .iter()
            .map(|phase| {
                vec![
                    phase.phase.clone(),
                    phase.results.operations.to_string(),
                    format_bytes(phase.results.bytes),
                    format_throughput(phase.results.throughput_bytes_per_sec),
                    format_duration_ms(phase.results.p95_ms),
                    phase.wait_target.clone(),
                    if phase.target_state_observed {
                        "yes".to_string()
                    } else {
                        "no".to_string()
                    },
                    format_duration_ms(phase.wait_ms),
                    phase.pending_uploads_before.to_string(),
                    phase.pending_uploads_after.to_string(),
                ]
            })
            .collect();
        let _ = write!(
            &mut out,
            "{}",
            format_table(
                &[
                    "Phase",
                    "Ops",
                    "Data",
                    "Throughput",
                    "P95",
                    "Target",
                    "Observed",
                    "Wait",
                    "Pending before",
                    "Pending after"
                ],
                &rows,
            )
        );
    }

    if let Some(advanced) = &report.advanced_metrics {
        out.push('\n');
        let _ = writeln!(&mut out, "Size-class latency and throughput");
        let mut rows = Vec::new();
        for item in &advanced.size_class_stats.writes {
            rows.push(vec![
                "Write".to_string(),
                item.class.clone(),
                item.operations.to_string(),
                format_bytes(item.bytes),
                format_throughput(item.throughput_bytes_per_sec),
                format_duration_ms(item.p50_ms),
                format_duration_ms(item.p95_ms),
                format_duration_ms(item.p99_ms),
            ]);
        }
        for item in &advanced.size_class_stats.reads {
            rows.push(vec![
                "Read".to_string(),
                item.class.clone(),
                item.operations.to_string(),
                format_bytes(item.bytes),
                format_throughput(item.throughput_bytes_per_sec),
                format_duration_ms(item.p50_ms),
                format_duration_ms(item.p95_ms),
                format_duration_ms(item.p99_ms),
            ]);
        }
        let _ = write!(
            &mut out,
            "{}",
            format_table(
                &[
                    "Phase",
                    "Class",
                    "Ops",
                    "Data",
                    "Throughput",
                    "P50",
                    "P95",
                    "P99"
                ],
                &rows,
            )
        );

        if let Some(levels) = &advanced.cache_levels {
            let _ = writeln!(&mut out, "Cache levels");
            let level_rows = vec![vec![
                "L1".to_string(),
                levels.l1.hits.to_string(),
                levels.l1.misses.to_string(),
                format_bytes(levels.l1.bytes_served),
                format_throughput(levels.l1.throughput_bytes_per_sec),
                format_duration_ms(levels.l1.p50_ms),
                format_duration_ms(levels.l1.p95_ms),
                format_duration_ms(levels.l1.p99_ms),
            ]];
            let _ = write!(
                &mut out,
                "{}",
                format_table(
                    &[
                        "Level",
                        "Hits",
                        "Misses",
                        "Bytes served",
                        "Throughput",
                        "P50",
                        "P95",
                        "P99"
                    ],
                    &level_rows,
                )
            );
        }
    }

    if let Some(streams) = &report.stream_results {
        out.push('\n');
        let _ = writeln!(&mut out, "Per-stream results");
        let rows: Vec<Vec<String>> = streams
            .iter()
            .flat_map(|stream| {
                [
                    vec![
                        "Write".to_string(),
                        stream.stream_id.to_string(),
                        stream.writes.operations.to_string(),
                        format_bytes(stream.writes.bytes),
                        format_throughput(stream.writes.throughput_bytes_per_sec),
                        format_duration_ms(stream.writes.p95_ms),
                    ],
                    vec![
                        "Read".to_string(),
                        stream.stream_id.to_string(),
                        stream.reads.operations.to_string(),
                        format_bytes(stream.reads.bytes),
                        format_throughput(stream.reads.throughput_bytes_per_sec),
                        format_duration_ms(stream.reads.p95_ms),
                    ],
                ]
            })
            .collect();
        let _ = write!(
            &mut out,
            "{}",
            format_table(
                &["Phase", "Stream", "Ops", "Data", "Throughput", "P95"],
                &rows
            )
        );
    }

    if let Some(pipeline) = &report.cache_pipeline {
        out.push('\n');
        let _ = writeln!(&mut out, "Cache pipeline");
        let _ = writeln!(
            &mut out,
            "Upload queue: depth {} (peak {}), submitted {}, completed {}, requeued {}, canceled {}",
            pipeline.upload_queue.pending_depth,
            pipeline.upload_queue.pending_depth_peak,
            pipeline.upload_queue.submitted,
            pipeline.upload_queue.completed,
            pipeline.upload_queue.requeued,
            pipeline.upload_queue.canceled,
        );
        let _ = writeln!(
            &mut out,
            "Materialize: {} batches, {} blocks, {}, inflight {} (peak {})",
            pipeline.materialize_batches.batches,
            pipeline.materialize_batches.blocks,
            format_bytes(pipeline.materialize_batches.bytes),
            pipeline.materialize_batches.inflight_batches,
            pipeline.materialize_batches.inflight_batches_peak,
        );
        let _ = writeln!(
            &mut out,
            "Blocking tasks: active {} (peak {}), submitted {}, started {}, completed {}, queue wait avg {}, max {}, exec avg {}, max {}",
            pipeline.blocking_tasks.total.active,
            pipeline.blocking_tasks.total.active_peak,
            pipeline.blocking_tasks.total.submitted,
            pipeline.blocking_tasks.total.started,
            pipeline.blocking_tasks.total.completed,
            format_duration_ms(pipeline.blocking_tasks.total.queue_wait_avg_ms),
            format_duration_ms(pipeline.blocking_tasks.total.queue_wait_max_ms),
            format_duration_ms(pipeline.blocking_tasks.total.execution_avg_ms),
            format_duration_ms(pipeline.blocking_tasks.total.execution_max_ms),
        );
        if !pipeline.blocking_tasks.by_kind.is_empty() {
            let _ = writeln!(&mut out, "Blocking tasks by kind:");
            for kind in &pipeline.blocking_tasks.by_kind {
                let _ = writeln!(
                    &mut out,
                    "- {}: submitted {}, queue wait avg {}, max {}",
                    kind.task_kind,
                    kind.stats.submitted,
                    format_duration_ms(kind.stats.queue_wait_avg_ms),
                    format_duration_ms(kind.stats.queue_wait_max_ms),
                );
            }
        }
    }

    if let Some(profile) = &report.profile {
        out.push('\n');
        let _ = writeln!(&mut out, "On-demand profile");
        let _ = writeln!(&mut out, "Mode: {}", profile.profile_mode);
        let _ = writeln!(
            &mut out,
            "CPU util: total {:.1}% (write {:.1}%, read {:.1}%), wall {}, CPU {}",
            profile.cpu_profile.cpu_utilization_percent,
            profile.cpu_profile.write_phase_cpu_percent,
            profile.cpu_profile.read_phase_cpu_percent,
            format_duration_ms(profile.cpu_profile.total_wall_ms),
            format_duration_ms(profile.cpu_profile.total_cpu_ms),
        );
        if let Some(endpoint) = &profile.endpoint {
            let _ = writeln!(
                &mut out,
                "Pyroscope endpoint: {} ({})",
                endpoint.url, endpoint.application_name
            );
        }
        if !profile.artifacts.is_empty() {
            let _ = writeln!(&mut out, "Profile artifacts:");
            for artifact in &profile.artifacts {
                let _ = writeln!(
                    &mut out,
                    "- {}: {} ({} bytes, sent: {})",
                    artifact.kind,
                    artifact.path,
                    artifact.bytes,
                    if artifact.sent_to_endpoint {
                        "yes"
                    } else {
                        "no"
                    }
                );
            }
        }
        if !profile.warnings.is_empty() {
            let _ = writeln!(&mut out, "Profile warnings:");
            for warning in &profile.warnings {
                let _ = writeln!(&mut out, "- {}", warning);
            }
        }
    }

    out
}

fn merge_profile_report(
    mut report: BenchmarkReport,
    profile_report: Option<TestCacheProfileResult>,
) -> BenchmarkReport {
    let Some(profile_report) = profile_report else {
        return report;
    };

    let artifacts = profile_report
        .artifacts
        .into_iter()
        .map(|artifact| ProfileArtifactSummary {
            kind: artifact.kind,
            path: artifact.path,
            bytes: artifact.bytes,
            sent_to_endpoint: artifact.sent_to_endpoint,
        })
        .collect();

    let profile_mode = "pyroscope".to_string();
    let endpoint = profile_report
        .endpoint
        .map(|endpoint| ProfileEndpointReport {
            url: endpoint.url,
            application_name: endpoint.application_name,
        });

    match report.profile.take() {
        Some(existing) => {
            report.profile = Some(BenchmarkProfileReport {
                enabled: existing.enabled,
                profile_mode,
                cpu_profile: existing.cpu_profile,
                endpoint,
                artifacts,
                warnings: profile_report.warnings,
            });
        }
        None => {
            report.profile = Some(BenchmarkProfileReport {
                enabled: true,
                profile_mode,
                cpu_profile: CpuProfileReport {
                    total_wall_ms: 0.0,
                    total_cpu_ms: 0.0,
                    cpu_utilization_percent: 0.0,
                    write_phase_cpu_percent: 0.0,
                    read_phase_cpu_percent: 0.0,
                },
                endpoint,
                artifacts,
                warnings: profile_report.warnings,
            });
        }
    }

    report
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    target: PathBuf,
    block_size: Option<String>,
    read_size: Option<String>,
    small_max: Option<String>,
    medium_max: Option<String>,
    concurrency: usize,
    sequential_streams: bool,
    io_depth: usize,
    warmup_runs: u32,
    sizes: Option<String>,
    access_pattern: TestCacheAccessPattern,
    l1_limit: Option<String>,
    profile: Option<TestCacheProfileMode>,
    json_output: Option<PathBuf>,
) -> Result<()> {
    if io_depth == 0 {
        return Err(anyhow!("--io-depth must be >= 1"));
    }
    if concurrency == 0 {
        return Err(anyhow!("--concurrency must be >= 1"));
    }
    let block_size = parse_size_arg(
        "block-size",
        block_size
            .as_deref()
            .unwrap_or(DEFAULT_TEST_CACHE_BLOCK_SIZE_STR),
    )?;
    let block_size_default = block_size.to_string();
    let l1_limit_default = DEFAULT_L1_SIZE.to_string();
    let small_max_default = DEFAULT_SMALL_CLASS_MAX_BYTES.to_string();
    let medium_max_default = block_size.to_string();
    let read_size = parse_size_arg(
        "read-size",
        read_size
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&block_size_default),
    )?
    .max(1);
    let l1_limit = parse_size_arg("l1-limit", l1_limit.as_deref().unwrap_or(&l1_limit_default))?;
    let size_class_small_max = parse_size_arg(
        "small-max",
        small_max
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&small_max_default),
    )?;
    let size_class_medium_max = parse_size_arg(
        "medium-max",
        medium_max
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&medium_max_default),
    )?;
    let sizes = parse_sizes_arg(sizes.as_deref())?;

    if l1_limit < 1 {
        return Err(anyhow!("cache limit must be > 0"));
    }
    if size_class_small_max == 0 {
        return Err(anyhow!("--small-max must be > 0"));
    }
    if size_class_medium_max <= size_class_small_max {
        return Err(anyhow!("--medium-max must be greater than --small-max"));
    }
    if sequential_streams {
        let effective = concurrency.min(io_depth.max(1));
        tracing::warn!(
            target: "studiofs.cache.profile",
            requested_concurrency = concurrency,
            requested_io_depth = io_depth,
            effective_inflight_io = effective,
            "sequential-streams mode processes each stream serially; effective in-flight IO is bounded by min(concurrency, io-depth)"
        );
    }

    let run_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(
        target: "studiofs.cache.profile",
        run_id = %run_id,
        profile_enabled = profile.is_some(),
        "test-cache run started"
    );
    let params = TestCacheParams {
        target: target.clone(),
        block_size,
        read_size,
        size_class_small_max,
        size_class_medium_max,
        concurrency,
        sequential_streams,
        io_depth,
        warmup_runs,
        sizes,
        access_pattern,
        l1_limit,
        profile: profile.is_some(),
        run_id: run_id.clone(),
    };

    let resolved_mode = resolve_run_mode(&target)?;
    let benchmark_mode = match &resolved_mode {
        ResolvedRunMode::DirectShim { .. } => "direct-shim",
        ResolvedRunMode::MountedE2E { .. } => "e2e",
    };
    let report_dir = json_output
        .as_ref()
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .unwrap_or(std::env::current_dir().context("resolve current directory")?);
    let profile_session = TestCacheProfileSession::start(profile, &run_id, benchmark_mode)?;

    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    let report = rt.block_on(async {
        match &resolved_mode {
            ResolvedRunMode::DirectShim { cache_dir } => run_direct(&params, cache_dir).await,
            ResolvedRunMode::MountedE2E {
                volume,
                mount_point,
                work_root,
                cache_path,
            } => {
                run_mounted_end_to_end(
                    &params,
                    volume,
                    mount_point,
                    work_root,
                    cache_path.as_deref(),
                )
                .await
            }
        }
    })?;

    let profile_report = match profile_session.map(|session| session.finish(&report_dir)) {
        Some(Ok(report)) => Some(report),
        Some(Err(err)) => {
            tracing::warn!(
                target: "studiofs.cache.profile",
                run_id = %run_id,
                error = %err,
                "profile session failed to finish; benchmark report will still be written"
            );
            Some(TestCacheProfileResult {
                endpoint: None,
                artifacts: Vec::new(),
                warnings: vec![format!(
                    "profile session failed to finish: {err:#}; benchmark metrics are still valid"
                )],
            })
        }
        None => None,
    };
    let report = merge_profile_report(report, profile_report);

    print!("{}", render_human_summary(&report));
    let report_path = save_benchmark_report(&report, json_output.as_deref())?;
    println!("JSON report: {}", report_path.display());
    tracing::info!(
        target: "studiofs.cache.profile",
        run_id = %run_id,
        report_path = %report_path.display(),
        "test-cache run completed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use studiofs_meta::DEFAULT_BLOCK_SIZE;
    use tempfile::tempdir;

    fn sample_report() -> BenchmarkReport {
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
                    kind: "cpu".to_string(),
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
            buffered_read_comparison: None,
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

    #[test]
    fn workload_profile_has_expected_tiers() {
        let block_size = DEFAULT_BLOCK_SIZE as u64;
        let bounds = SizeClassBounds {
            small_max: SMALL_FILE_MAX_BYTES,
            medium_max: block_size,
        };
        let plan = build_work_sizes(block_size, &WorkloadSizeInput::Default, bounds);
        let sizes = plan.sizes;
        assert_eq!(sizes.len(), WORKLOAD_FILES_PER_TIER * 3);

        let small = &sizes[..WORKLOAD_FILES_PER_TIER];
        let medium = &sizes[WORKLOAD_FILES_PER_TIER..WORKLOAD_FILES_PER_TIER * 2];
        let large = &sizes[WORKLOAD_FILES_PER_TIER * 2..];

        assert!(
            small
                .iter()
                .all(|size| *size >= SMALL_FILE_MIN_BYTES && *size <= SMALL_FILE_MAX_BYTES)
        );
        assert!(medium.iter().all(|size| *size == block_size));
        assert!(
            large
                .iter()
                .all(|size| *size == block_size * LARGE_FILE_MULTIPLIER)
        );
    }

    #[test]
    fn sizes_percentages_parse_and_sum_validation() {
        let parsed = parse_sizes_arg(Some("small=60,medium=30,large=10")).unwrap();
        assert!(matches!(
            parsed,
            WorkloadSizeInput::Percentages {
                small: 60,
                medium: 30,
                large: 10
            }
        ));
        assert!(parse_sizes_arg(Some("small=40,medium=40,large=10")).is_err());
    }

    #[test]
    fn sizes_percentages_allow_missing_classes_as_zero() {
        let parsed = parse_sizes_arg(Some("small=50,large=50")).unwrap();
        assert!(matches!(
            parsed,
            WorkloadSizeInput::Percentages {
                small: 50,
                medium: 0,
                large: 50
            }
        ));
        assert!(parse_sizes_arg(Some("small=50,small=50")).is_err());
    }

    #[test]
    fn sizes_explicit_list_parses() {
        let parsed = parse_sizes_arg(Some("4KB,1MB,16MB")).unwrap();
        match parsed {
            WorkloadSizeInput::ExplicitList(values) => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], 4 * 1024);
                assert_eq!(values[1], 1024 * 1024);
                assert_eq!(values[2], 16 * 1024 * 1024);
            }
            other => panic!("expected explicit list, got {other:?}"),
        }
    }

    #[test]
    fn workload_respects_custom_size_class_bounds() {
        let bounds = SizeClassBounds {
            small_max: 2 * 1024,
            medium_max: 6 * 1024,
        };
        let plan = build_work_sizes(
            DEFAULT_BLOCK_SIZE as u64,
            &WorkloadSizeInput::ExplicitList(vec![1024, 4 * 1024, 8 * 1024]),
            bounds,
        );
        assert_eq!(plan.small_files, 1);
        assert_eq!(plan.medium_files, 1);
        assert_eq!(plan.large_files, 1);

        let default_plan = build_work_sizes(
            DEFAULT_BLOCK_SIZE as u64,
            &WorkloadSizeInput::Default,
            bounds,
        );
        assert!(
            default_plan.sizes[..WORKLOAD_FILES_PER_TIER]
                .iter()
                .all(|size| *size <= bounds.small_max)
        );
    }

    #[test]
    fn latency_merge_upsamples_keep_every_progressively() {
        let mut left = LatencySamples {
            samples_ns: (0..MAX_LATENCY_SAMPLES as u64).collect(),
            total_ops: MAX_LATENCY_SAMPLES as u64,
            total_bytes: 0,
            total_latency_ns: 0,
            sample_keep_every: 2,
        };
        let right = LatencySamples {
            samples_ns: (0..MAX_LATENCY_SAMPLES as u64).collect(),
            total_ops: MAX_LATENCY_SAMPLES as u64,
            total_bytes: 0,
            total_latency_ns: 0,
            sample_keep_every: 2,
        };
        left.merge_from_ref(&right);
        assert_eq!(left.sample_keep_every, 4);
    }

    #[test]
    fn latency_merge_aligns_sampling_density_before_append() {
        let mut left = LatencySamples {
            samples_ns: vec![1, 2, 3, 4],
            total_ops: 4,
            total_bytes: 0,
            total_latency_ns: 0,
            sample_keep_every: 1,
        };
        let right = LatencySamples {
            samples_ns: vec![10, 20, 30, 40],
            total_ops: 8,
            total_bytes: 0,
            total_latency_ns: 0,
            sample_keep_every: 2,
        };
        left.merge_from_ref(&right);
        assert_eq!(left.sample_keep_every, 2);
        assert_eq!(left.samples_ns, vec![1, 3, 10, 20, 30, 40]);
    }

    #[test]
    fn mount_path_detection_handles_nested_paths() {
        let mount = PathBuf::from("/mnt/studiofs");
        let target = PathBuf::from("/mnt/studiofs/cache-bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[test]
    fn mount_path_detection_rejects_sibling_paths() {
        let mount = PathBuf::from("/mnt/studiofs");
        let target = PathBuf::from("/mnt/studiofs-other/cache-bench");
        assert!(!is_path_under_mount(&target, &mount));
    }

    #[cfg(windows)]
    #[test]
    fn mount_path_detection_normalizes_extended_drive_prefix() {
        let mount = PathBuf::from(r"C:\StudioFS");
        let target = PathBuf::from(r"\\?\C:\StudioFS\bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[cfg(windows)]
    #[test]
    fn mount_path_detection_normalizes_unc_prefix() {
        let mount = PathBuf::from(r"\\server\share\studiofs");
        let target = PathBuf::from(r"\\?\UNC\server\share\studiofs\bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[test]
    fn default_report_path_uses_current_dir_and_avoids_collisions() {
        let temp = tempdir().unwrap();
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let first = save_benchmark_report_in(&report, None, temp.path(), now).unwrap();
        assert_eq!(
            first,
            temp.path()
                .join("studiofs-test-cache-direct-shim-20260319-151255.json")
        );

        let second = save_benchmark_report_in(&report, None, temp.path(), now).unwrap();
        assert_eq!(
            second,
            temp.path()
                .join("studiofs-test-cache-direct-shim-20260319-151255-2.json")
        );
    }

    #[test]
    fn save_benchmark_report_writes_pretty_json_to_requested_path() {
        let temp = tempdir().unwrap();
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved = save_benchmark_report_in(
            &report,
            Some(Path::new("reports/cache-run.json")),
            temp.path(),
            now,
        )
        .unwrap();

        assert_eq!(saved, temp.path().join("reports/cache-run.json"));

        let content = std::fs::read_to_string(saved).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["mode"], "direct-shim");
        assert!(content.contains("\n  \"mode\""));
    }

    #[test]
    fn render_human_summary_is_table_oriented_without_json_blob() {
        let summary = render_human_summary(&sample_report());
        assert!(summary.contains("Operation results"));
        assert!(summary.contains("| Phase |"));
        assert!(summary.contains("Size-class latency and throughput"));
        assert!(summary.contains("Cache levels"));
        assert!(summary.contains("Cache pipeline"));
        assert!(summary.contains("Upload queue:"));
        assert!(!summary.contains("\"cache_pipeline\""));
        assert!(!summary.contains("\"mode\""));
    }

    #[test]
    fn render_human_summary_includes_buffered_read_comparison_when_present() {
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.buffered_read_comparison = Some(MountedBufferedReadComparisonReport {
            cache_path: r"D:\test-cache\vol-123".to_string(),
            upload_dir: r"D:\test-cache\vol-123\upload".to_string(),
            phases: vec![
                MountedBufferedReadPhaseReport {
                    phase: "baseline-read".to_string(),
                    wait_target: "idle".to_string(),
                    target_state_observed: true,
                    wait_ms: 120.0,
                    pending_uploads_before: 0,
                    pending_uploads_after: 0,
                    results: OpReport {
                        operations: 16,
                        bytes: 128 * 1024 * 1024,
                        elapsed_ms: 72.0,
                        avg_latency_ms: 4.4,
                        p50_ms: 4.1,
                        p95_ms: 7.3,
                        p99_ms: 7.8,
                        throughput_bytes_per_sec: 1_700.0 * 1024.0 * 1024.0,
                    },
                },
                MountedBufferedReadPhaseReport {
                    phase: "during-upload-read".to_string(),
                    wait_target: "active".to_string(),
                    target_state_observed: true,
                    wait_ms: 18.0,
                    pending_uploads_before: 3,
                    pending_uploads_after: 2,
                    results: OpReport {
                        operations: 16,
                        bytes: 128 * 1024 * 1024,
                        elapsed_ms: 81.0,
                        avg_latency_ms: 5.0,
                        p50_ms: 4.8,
                        p95_ms: 8.4,
                        p99_ms: 9.0,
                        throughput_bytes_per_sec: 1_500.0 * 1024.0 * 1024.0,
                    },
                },
                MountedBufferedReadPhaseReport {
                    phase: "post-upload-read".to_string(),
                    wait_target: "idle".to_string(),
                    target_state_observed: true,
                    wait_ms: 240.0,
                    pending_uploads_before: 0,
                    pending_uploads_after: 0,
                    results: OpReport {
                        operations: 16,
                        bytes: 128 * 1024 * 1024,
                        elapsed_ms: 74.0,
                        avg_latency_ms: 4.5,
                        p50_ms: 4.2,
                        p95_ms: 7.5,
                        p99_ms: 7.9,
                        throughput_bytes_per_sec: 1_650.0 * 1024.0 * 1024.0,
                    },
                },
            ],
        });

        let summary = render_human_summary(&report);

        assert!(summary.contains("Buffered-first read comparison"));
        assert!(summary.contains("baseline-read"));
        assert!(summary.contains("during-upload-read"));
        assert!(summary.contains("post-upload-read"));
        assert!(summary.contains("Pending before"));
    }
}

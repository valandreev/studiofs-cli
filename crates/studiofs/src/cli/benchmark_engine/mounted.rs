use crate::cli::WRITE_PATH_OBSERVABILITY_FILE;
use anyhow::{Context, Result, anyhow, bail};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use studiofs_vfs::VfsWritePathObservabilitySnapshot;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio::sync::watch;

use super::BenchmarkConfig;
use super::profiling::{cpu_utilization_percent, process_cpu_time};
use super::progress::{ProgressBar, ProgressCounter};
use super::result::{
    BenchmarkProfileReport, BenchmarkReport, CpuProfileReport, MountedBarrierReport,
    MountedCachedStoreReport, MountedFetchReport, MountedLocalFirstReadComparisonReport,
    MountedLocalFirstReadPhaseReport, MountedMemoryReport, MountedObservabilityReport,
    MountedPortableAllocatorReport, MountedPortablePhaseProfileReport,
    MountedPortableProcessReport, MountedPortableProfileReport, MountedPostUploadIdleReport,
    MountedProcessReport, MountedRuntimeReport, MountedUploadReport, MountedWorkloadShapeReport,
    MountedWriteCallbackReport, MountedWriteDiagnosticsReport, MountedWritePathWindowReport,
    MountedWritebackTimelineReport, MountedWritebackTimelineSampleReport, MountedWriterStateReport,
    OpReport,
};
use super::telemetry::{
    PhaseOperationLog, PhaseTelemetry, build_advanced_metrics, phase_telemetry_to_report,
    stream_reports_from_telemetry,
};
use super::workload::{
    SizeClass, SizeClassBounds, WorkFile, WorkloadPlan, build_access_sequence, build_file_worklist,
    build_work_sizes, classify_size, deterministic_bytes, split_stream_segments,
};
use studiofs_vfs::VfsPortableAllocatorStats;

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

    fn matches(self, pending_uploads: usize, required_pending_uploads: usize) -> bool {
        match self {
            Self::Active => pending_uploads >= required_pending_uploads.max(1),
            Self::Idle => pending_uploads == 0,
        }
    }
}

struct PendingUploadObservation {
    target: PendingUploadTarget,
    observed: bool,
    required_pending_uploads: usize,
    waited: Duration,
    pending_uploads: usize,
}

const MOUNTED_MEMORY_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const OPERATOR_WRITE_THROUGHPUT_TARGET_BYTES_PER_SEC: f64 = 1_000.0 * 1024.0 * 1024.0;
const OPERATOR_RSS_PEAK_TARGET_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const OPERATOR_KNOWN_WRITE_AMPLIFICATION_TARGET: f64 = 1.20;
const MAX_TEST_WRITE_CHUNK_BYTES: usize = 64 * 1024 * 1024;

struct MeasuredMountedPhaseResult {
    started: Instant,
    started_at: String,
    writes: PhaseTelemetry,
    writes_by_stream: Vec<PhaseTelemetry>,
    writes_wall: Duration,
    writes_cpu: Duration,
    reads: PhaseTelemetry,
    reads_by_stream: Vec<PhaseTelemetry>,
    reads_wall: Duration,
    reads_cpu: Duration,
    stats: PhaseTelemetry,
    stats_wall: Duration,
    total_wall: Duration,
    total_cpu: Duration,
    during_upload_observation: Option<PendingUploadObservation>,
}

#[derive(Default)]
struct MountedMemoryAccumulator {
    mount_pid: u32,
    rss_bytes_steady_state: Option<u64>,
    rss_bytes_peak: u64,
    dirty_bytes_steady_state: Option<u64>,
    dirty_bytes_peak: u64,
    staged_bytes_peak: u64,
    dirty_limit_bytes: u64,
}

impl MountedMemoryAccumulator {
    fn new(mount_pid: u32) -> Self {
        Self {
            mount_pid,
            ..Self::default()
        }
    }

    fn observe_rss(&mut self, rss_bytes: u64) {
        self.rss_bytes_steady_state = Some(rss_bytes);
        self.rss_bytes_peak = self.rss_bytes_peak.max(rss_bytes);
    }

    fn observe_dirty_pressure(
        &mut self,
        dirty_bytes: u64,
        dirty_limit_bytes: u64,
        staged_peak_bytes: u64,
    ) {
        self.dirty_bytes_steady_state = Some(dirty_bytes);
        self.dirty_bytes_peak = self.dirty_bytes_peak.max(dirty_bytes);
        self.staged_bytes_peak = self.staged_bytes_peak.max(staged_peak_bytes);
        self.dirty_limit_bytes = self.dirty_limit_bytes.max(dirty_limit_bytes);
    }

    fn into_report(self) -> Option<MountedMemoryReport> {
        if self.rss_bytes_steady_state.is_none() && self.dirty_bytes_steady_state.is_none() {
            return None;
        }
        Some(MountedMemoryReport {
            mount_pid: self.mount_pid,
            rss_bytes_steady_state: self.rss_bytes_steady_state,
            rss_bytes_peak: self.rss_bytes_steady_state.map(|_| self.rss_bytes_peak),
            dirty_bytes_steady_state: self.dirty_bytes_steady_state,
            dirty_bytes_peak: self.dirty_bytes_steady_state.map(|_| self.dirty_bytes_peak),
            staged_bytes_peak: self
                .dirty_bytes_steady_state
                .map(|_| self.staged_bytes_peak),
            dirty_limit_bytes: self
                .dirty_bytes_steady_state
                .map(|_| self.dirty_limit_bytes),
        })
    }
}

#[derive(Clone)]
struct MountedPortablePhaseControl {
    phase_tx: watch::Sender<String>,
}

impl MountedPortablePhaseControl {
    fn set_phase(&self, phase: &str) {
        let _ = self.phase_tx.send(phase.to_string());
    }
}

#[derive(Debug, Clone)]
struct MountedProcessTelemetrySample {
    rss_bytes: u64,
    virtual_memory_bytes: u64,
    cpu_usage_percent: f64,
    cpu_time_ms: u64,
    disk_read_bytes_total: u64,
    disk_write_bytes_total: u64,
}

#[derive(Default)]
struct MountedPortableProcessAccumulator {
    mount_pid: u32,
    sample_count: u64,
    rss_bytes_steady_state: Option<u64>,
    rss_bytes_peak: u64,
    virtual_memory_bytes_peak: u64,
    cpu_usage_percent_sum: f64,
    cpu_usage_percent_peak: f64,
    first_cpu_time_ms: Option<u64>,
    last_cpu_time_ms: Option<u64>,
    first_disk_read_bytes_total: Option<u64>,
    last_disk_read_bytes_total: Option<u64>,
    first_disk_write_bytes_total: Option<u64>,
    last_disk_write_bytes_total: Option<u64>,
}

impl MountedPortableProcessAccumulator {
    fn new(mount_pid: u32) -> Self {
        Self {
            mount_pid,
            ..Self::default()
        }
    }

    fn observe(&mut self, sample: &MountedProcessTelemetrySample) {
        self.sample_count = self.sample_count.saturating_add(1);
        self.rss_bytes_steady_state = Some(sample.rss_bytes);
        self.rss_bytes_peak = self.rss_bytes_peak.max(sample.rss_bytes);
        self.virtual_memory_bytes_peak = self
            .virtual_memory_bytes_peak
            .max(sample.virtual_memory_bytes);
        self.cpu_usage_percent_sum += sample.cpu_usage_percent;
        self.cpu_usage_percent_peak = self.cpu_usage_percent_peak.max(sample.cpu_usage_percent);
        self.first_cpu_time_ms.get_or_insert(sample.cpu_time_ms);
        self.last_cpu_time_ms = Some(sample.cpu_time_ms);
        self.first_disk_read_bytes_total
            .get_or_insert(sample.disk_read_bytes_total);
        self.last_disk_read_bytes_total = Some(sample.disk_read_bytes_total);
        self.first_disk_write_bytes_total
            .get_or_insert(sample.disk_write_bytes_total);
        self.last_disk_write_bytes_total = Some(sample.disk_write_bytes_total);
    }

    fn into_report(self) -> Option<MountedPortableProcessReport> {
        if self.sample_count == 0 {
            return None;
        }
        Some(MountedPortableProcessReport {
            mount_pid: self.mount_pid,
            sample_count: self.sample_count,
            rss_bytes_steady_state: self.rss_bytes_steady_state,
            rss_bytes_peak: self.rss_bytes_steady_state.map(|_| self.rss_bytes_peak),
            virtual_memory_bytes_peak: Some(self.virtual_memory_bytes_peak),
            cpu_usage_percent_avg: Some(self.cpu_usage_percent_sum / self.sample_count as f64),
            cpu_usage_percent_peak: Some(self.cpu_usage_percent_peak),
            cpu_time_ms_delta: match (self.first_cpu_time_ms, self.last_cpu_time_ms) {
                (Some(first), Some(last)) => Some(last.saturating_sub(first)),
                _ => None,
            },
            disk_read_bytes_delta: match (
                self.first_disk_read_bytes_total,
                self.last_disk_read_bytes_total,
            ) {
                (Some(first), Some(last)) => Some(last.saturating_sub(first)),
                _ => None,
            },
            disk_write_bytes_delta: match (
                self.first_disk_write_bytes_total,
                self.last_disk_write_bytes_total,
            ) {
                (Some(first), Some(last)) => Some(last.saturating_sub(first)),
                _ => None,
            },
        })
    }
}

#[derive(Default)]
struct MountedPortableAllocatorAccumulator {
    latest: Option<VfsPortableAllocatorStats>,
    peak_bytes: u64,
}

impl MountedPortableAllocatorAccumulator {
    fn observe(&mut self, stats: &VfsPortableAllocatorStats) {
        self.peak_bytes = self.peak_bytes.max(stats.peak_bytes);
        self.latest = Some(stats.clone());
    }

    fn into_report(self) -> Option<MountedPortableAllocatorReport> {
        self.latest.map(|latest| MountedPortableAllocatorReport {
            current_bytes: latest.current_bytes,
            peak_bytes: latest.peak_bytes.max(self.peak_bytes),
            allocated_total_bytes: latest.allocated_total_bytes,
            deallocated_total_bytes: latest.deallocated_total_bytes,
            allocation_count: latest.allocation_count,
            deallocation_count: latest.deallocation_count,
            reallocation_count: latest.reallocation_count,
        })
    }
}

#[derive(Default)]
struct MountedPortablePhaseAccumulator {
    process: MountedPortableProcessAccumulator,
    allocator: MountedPortableAllocatorAccumulator,
}

impl MountedPortablePhaseAccumulator {
    fn new(mount_pid: u32) -> Self {
        Self {
            process: MountedPortableProcessAccumulator::new(mount_pid),
            allocator: MountedPortableAllocatorAccumulator::default(),
        }
    }
}

struct MountedPortableTelemetryAccumulator {
    process: MountedPortableProcessAccumulator,
    allocator: MountedPortableAllocatorAccumulator,
    phases: BTreeMap<String, MountedPortablePhaseAccumulator>,
}

impl MountedPortableTelemetryAccumulator {
    fn new(mount_pid: u32) -> Self {
        Self {
            process: MountedPortableProcessAccumulator::new(mount_pid),
            allocator: MountedPortableAllocatorAccumulator::default(),
            phases: BTreeMap::new(),
        }
    }

    fn observe_process(&mut self, phase: &str, sample: &MountedProcessTelemetrySample) {
        self.process.observe(sample);
        self.phases
            .entry(phase.to_string())
            .or_insert_with(|| MountedPortablePhaseAccumulator::new(self.process.mount_pid))
            .process
            .observe(sample);
    }

    fn observe_allocator(&mut self, phase: &str, stats: &VfsPortableAllocatorStats) {
        self.allocator.observe(stats);
        self.phases
            .entry(phase.to_string())
            .or_insert_with(|| MountedPortablePhaseAccumulator::new(self.process.mount_pid))
            .allocator
            .observe(stats);
    }

    fn into_report(self) -> Option<MountedPortableProfileReport> {
        let process = self.process.into_report()?;
        let phases = self
            .phases
            .into_iter()
            .filter_map(|(phase, accumulator)| {
                let process = accumulator.process.into_report()?;
                Some(MountedPortablePhaseProfileReport {
                    phase,
                    process,
                    allocator: accumulator.allocator.into_report(),
                })
            })
            .collect();
        Some(MountedPortableProfileReport {
            process,
            allocator: self.allocator.into_report(),
            phases,
        })
    }
}

struct MountedProfileAccumulator {
    memory: MountedMemoryAccumulator,
    portable: MountedPortableTelemetryAccumulator,
    writeback_timeline: MountedWritebackTimelineAccumulator,
}

impl MountedProfileAccumulator {
    fn new(mount_pid: u32) -> Self {
        Self {
            memory: MountedMemoryAccumulator::new(mount_pid),
            portable: MountedPortableTelemetryAccumulator::new(mount_pid),
            writeback_timeline: MountedWritebackTimelineAccumulator::new(1000),
        }
    }

    fn observe_process(&mut self, phase: &str, sample: &MountedProcessTelemetrySample) {
        self.memory.observe_rss(sample.rss_bytes);
        self.portable.observe_process(phase, sample);
    }

    fn observe_snapshot(&mut self, phase: &str, snapshot: &VfsWritePathObservabilitySnapshot) {
        self.memory.observe_dirty_pressure(
            snapshot.dirty_pressure.total_dirty_bytes as u64,
            snapshot.dirty_pressure.dirty_limit_bytes as u64,
            snapshot.dirty_pressure.staged_peak_bytes as u64,
        );
        if let Some(allocator) = &snapshot.allocator {
            self.portable.observe_allocator(phase, allocator);
        }
        self.writeback_timeline.observe(phase, snapshot);
    }

    fn into_reports(
        self,
    ) -> (
        Option<MountedMemoryReport>,
        Option<MountedPortableProfileReport>,
        Option<MountedWritebackTimelineReport>,
    ) {
        (
            self.memory.into_report(),
            self.portable.into_report(),
            self.writeback_timeline.into_report(),
        )
    }
}

struct MountedWritebackTimelineAccumulator {
    bucket_ms: u64,
    first_generated_unix_ms: Option<u64>,
    previous: Option<VfsWritePathObservabilitySnapshot>,
    active: Option<MountedWritebackTimelineBucket>,
    samples: Vec<MountedWritebackTimelineSampleReport>,
}

impl MountedWritebackTimelineAccumulator {
    fn new(bucket_ms: u64) -> Self {
        Self {
            bucket_ms: bucket_ms.max(1),
            first_generated_unix_ms: None,
            previous: None,
            active: None,
            samples: Vec::new(),
        }
    }

    fn observe(&mut self, phase: &str, snapshot: &VfsWritePathObservabilitySnapshot) {
        let first = *self
            .first_generated_unix_ms
            .get_or_insert(snapshot.generated_unix_ms);
        let Some(previous) = self.previous.as_ref() else {
            self.previous = Some(snapshot.clone());
            return;
        };
        let delta_ms = snapshot
            .generated_unix_ms
            .saturating_sub(previous.generated_unix_ms);
        if delta_ms == 0 {
            self.previous = Some(snapshot.clone());
            return;
        }

        let elapsed_ms = snapshot.generated_unix_ms.saturating_sub(first);
        let bucket_end_ms = elapsed_ms
            .saturating_add(self.bucket_ms - 1)
            .saturating_div(self.bucket_ms)
            .saturating_mul(self.bucket_ms)
            .max(self.bucket_ms);
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.elapsed_ms != bucket_end_ms)
            && let Some(active) = self.active.take()
        {
            self.samples.push(active.into_sample());
        }

        self.active
            .get_or_insert_with(|| MountedWritebackTimelineBucket::new(bucket_end_ms, phase))
            .observe_delta(phase, previous, snapshot, delta_ms);
        self.previous = Some(snapshot.clone());
    }

    fn into_report(mut self) -> Option<MountedWritebackTimelineReport> {
        if let Some(active) = self.active.take() {
            self.samples.push(active.into_sample());
        }
        (!self.samples.is_empty()).then_some(MountedWritebackTimelineReport {
            bucket_ms: self.bucket_ms,
            samples: self.samples,
        })
    }
}

struct MountedWritebackTimelineBucket {
    elapsed_ms: u64,
    phase: String,
    duration_ms: u64,
    foreground_write_bytes_delta: u64,
    payload_persist_bytes_delta: u64,
    upload_completed_bytes_delta: u64,
    writer_stack_dirty_bytes: u64,
    pending_persisted_slice_bytes: u64,
    pending_upload_bytes: u64,
    upload_queue_depth: usize,
    active_uploads: usize,
    backpressure_events_delta: u64,
    backpressure_wait_ms_delta: u64,
    soft_throttle_events_delta: u64,
    hard_backpressure_events_delta: u64,
    materialization_calls_delta: u64,
    materialization_total_ms_delta: f64,
    local_cache_io_calls_delta: u64,
    local_cache_backend_io_total_ms_delta: f64,
    local_cache_backend_io_cumulative_max_ms: f64,
}

impl MountedWritebackTimelineBucket {
    fn new(elapsed_ms: u64, phase: &str) -> Self {
        Self {
            elapsed_ms,
            phase: phase.to_string(),
            duration_ms: 0,
            foreground_write_bytes_delta: 0,
            payload_persist_bytes_delta: 0,
            upload_completed_bytes_delta: 0,
            writer_stack_dirty_bytes: 0,
            pending_persisted_slice_bytes: 0,
            pending_upload_bytes: 0,
            upload_queue_depth: 0,
            active_uploads: 0,
            backpressure_events_delta: 0,
            backpressure_wait_ms_delta: 0,
            soft_throttle_events_delta: 0,
            hard_backpressure_events_delta: 0,
            materialization_calls_delta: 0,
            materialization_total_ms_delta: 0.0,
            local_cache_io_calls_delta: 0,
            local_cache_backend_io_total_ms_delta: 0.0,
            local_cache_backend_io_cumulative_max_ms: 0.0,
        }
    }

    fn observe_delta(
        &mut self,
        phase: &str,
        before: &VfsWritePathObservabilitySnapshot,
        after: &VfsWritePathObservabilitySnapshot,
        delta_ms: u64,
    ) {
        self.phase = phase.to_string();
        self.duration_ms = self.duration_ms.saturating_add(delta_ms);
        self.foreground_write_bytes_delta = self.foreground_write_bytes_delta.saturating_add(
            after
                .winfsp_write_path
                .write_bytes
                .saturating_sub(before.winfsp_write_path.write_bytes),
        );
        self.payload_persist_bytes_delta = self.payload_persist_bytes_delta.saturating_add(
            after
                .payload_burst
                .total_bytes
                .saturating_sub(before.payload_burst.total_bytes),
        );
        self.upload_completed_bytes_delta = self.upload_completed_bytes_delta.saturating_add(
            after
                .upload
                .completed_upload_bytes
                .saturating_sub(before.upload.completed_upload_bytes),
        );
        self.writer_stack_dirty_bytes = after.dirty_pressure.writer_stack_dirty_bytes as u64;
        self.pending_persisted_slice_bytes =
            after.dirty_pressure.pending_persisted_slice_bytes as u64;
        self.pending_upload_bytes = after.dirty_pressure.pending_upload_bytes as u64;
        self.upload_queue_depth = after.upload.pending_uploads;
        self.active_uploads = after.upload.active_uploads;
        self.backpressure_events_delta = self.backpressure_events_delta.saturating_add(
            after
                .write_backpressure
                .events
                .saturating_sub(before.write_backpressure.events),
        );
        self.backpressure_wait_ms_delta = self.backpressure_wait_ms_delta.saturating_add(
            after
                .write_backpressure
                .wait_total_ms
                .saturating_sub(before.write_backpressure.wait_total_ms),
        );
        self.soft_throttle_events_delta = self.soft_throttle_events_delta.saturating_add(
            after
                .write_backpressure
                .soft_throttle_events
                .saturating_sub(before.write_backpressure.soft_throttle_events),
        );
        self.hard_backpressure_events_delta = self.hard_backpressure_events_delta.saturating_add(
            after
                .write_backpressure
                .hard_events
                .saturating_sub(before.write_backpressure.hard_events),
        );
        self.materialization_calls_delta = self.materialization_calls_delta.saturating_add(
            after
                .payload_persist
                .calls
                .saturating_sub(before.payload_persist.calls),
        );
        self.materialization_total_ms_delta +=
            (after.payload_persist.total_ms - before.payload_persist.total_ms).max(0.0);
        self.local_cache_io_calls_delta = self.local_cache_io_calls_delta.saturating_add(
            after
                .local_cache_io
                .calls
                .saturating_sub(before.local_cache_io.calls),
        );
        self.local_cache_backend_io_total_ms_delta += (after.local_cache_io.backend_io_total_ms
            - before.local_cache_io.backend_io_total_ms)
            .max(0.0);
        self.local_cache_backend_io_cumulative_max_ms = after.local_cache_io.backend_io_max_ms;
    }

    fn bytes_per_sec(bytes: u64, duration_ms: u64) -> f64 {
        if duration_ms == 0 {
            0.0
        } else {
            bytes as f64 * 1000.0 / duration_ms as f64
        }
    }

    fn into_sample(self) -> MountedWritebackTimelineSampleReport {
        MountedWritebackTimelineSampleReport {
            elapsed_ms: self.elapsed_ms,
            phase: self.phase,
            foreground_write_bytes_per_sec: Self::bytes_per_sec(
                self.foreground_write_bytes_delta,
                self.duration_ms,
            ),
            payload_persist_bytes_per_sec: Self::bytes_per_sec(
                self.payload_persist_bytes_delta,
                self.duration_ms,
            ),
            upload_completed_bytes_per_sec: Self::bytes_per_sec(
                self.upload_completed_bytes_delta,
                self.duration_ms,
            ),
            writer_stack_dirty_bytes: self.writer_stack_dirty_bytes,
            pending_persisted_slice_bytes: self.pending_persisted_slice_bytes,
            pending_upload_bytes: self.pending_upload_bytes,
            upload_queue_depth: self.upload_queue_depth,
            active_uploads: self.active_uploads,
            backpressure_events_delta: self.backpressure_events_delta,
            backpressure_wait_ms_delta: self.backpressure_wait_ms_delta,
            soft_throttle_events_delta: self.soft_throttle_events_delta,
            hard_backpressure_events_delta: self.hard_backpressure_events_delta,
            materialization_calls_delta: self.materialization_calls_delta,
            materialization_total_ms_delta: self.materialization_total_ms_delta,
            local_cache_io_calls_delta: self.local_cache_io_calls_delta,
            local_cache_backend_io_total_ms_delta: self.local_cache_backend_io_total_ms_delta,
            local_cache_backend_io_cumulative_max_ms: self.local_cache_backend_io_cumulative_max_ms,
        }
    }
}

fn sample_mount_process(system: &mut System, pid: Pid) -> Option<MountedProcessTelemetrySample> {
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing()
            .with_memory()
            .with_cpu()
            .with_disk_usage(),
    );
    system.process(pid).map(|process| {
        let disk_usage = process.disk_usage();
        MountedProcessTelemetrySample {
            rss_bytes: process.memory(),
            virtual_memory_bytes: process.virtual_memory(),
            cpu_usage_percent: process.cpu_usage() as f64,
            cpu_time_ms: process.accumulated_cpu_time(),
            disk_read_bytes_total: disk_usage.total_read_bytes,
            disk_write_bytes_total: disk_usage.total_written_bytes,
        }
    })
}

async fn read_write_path_observability_snapshot(
    metrics_path: &Path,
    expected_publisher_pid: u32,
) -> Result<Option<VfsWritePathObservabilitySnapshot>> {
    match fs::read(metrics_path).await {
        Ok(bytes) => {
            let snapshot = serde_json::from_slice::<VfsWritePathObservabilitySnapshot>(&bytes)
                .context("parse write-path observability snapshot")?;
            if snapshot.publisher_pid != expected_publisher_pid {
                return Ok(None);
            }
            Ok(Some(snapshot))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {}", metrics_path.display())),
    }
}

#[cfg(test)]
fn spawn_mounted_memory_sampler(
    mount_pid: u32,
    metrics_path: Option<&Path>,
) -> (
    watch::Sender<bool>,
    tokio::task::JoinHandle<MountedMemoryAccumulator>,
) {
    let (stop_tx, _phase_control, handle) = spawn_mounted_profile_sampler(mount_pid, metrics_path);
    let handle = tokio::spawn(async move {
        let accumulator = handle
            .await
            .expect("mounted profile sampler task should not panic");
        accumulator.memory
    });
    (stop_tx, handle)
}

fn spawn_mounted_profile_sampler(
    mount_pid: u32,
    metrics_path: Option<&Path>,
) -> (
    watch::Sender<bool>,
    MountedPortablePhaseControl,
    tokio::task::JoinHandle<MountedProfileAccumulator>,
) {
    let pid = Pid::from_u32(mount_pid);
    let metrics_path = metrics_path.map(Path::to_path_buf);
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let (phase_tx, phase_rx) = watch::channel("measured".to_string());
    let phase_control = MountedPortablePhaseControl { phase_tx };
    let handle = tokio::spawn(async move {
        let mut system = System::new();
        let mut accumulator = MountedProfileAccumulator::new(mount_pid);
        let mut warned_metrics_error = false;

        loop {
            let phase = phase_rx.borrow().clone();
            if let Some(process_sample) = sample_mount_process(&mut system, pid) {
                accumulator.observe_process(&phase, &process_sample);
            }
            if let Some(metrics_path) = metrics_path.as_deref() {
                match read_write_path_observability_snapshot(metrics_path, mount_pid).await {
                    Ok(Some(snapshot)) => accumulator.observe_snapshot(&phase, &snapshot),
                    Ok(None) => {}
                    Err(error) => {
                        if !warned_metrics_error {
                            tracing::warn!(
                                target: "studiofs.cache.profile",
                                mount_pid,
                                metrics_path = %metrics_path.display(),
                                error = %error,
                                "mounted memory sampler failed to read write-path observability snapshot"
                            );
                            warned_metrics_error = true;
                        }
                    }
                }
            }

            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(MOUNTED_MEMORY_SAMPLE_INTERVAL) => {}
            }
        }

        let phase = phase_rx.borrow().clone();
        if let Some(process_sample) = sample_mount_process(&mut system, pid) {
            accumulator.observe_process(&phase, &process_sample);
        }
        if let Some(metrics_path) = metrics_path.as_deref()
            && let Ok(Some(snapshot)) =
                read_write_path_observability_snapshot(metrics_path, mount_pid).await
        {
            accumulator.observe_snapshot(&phase, &snapshot);
        }

        accumulator
    });
    (stop_tx, phase_control, handle)
}

#[cfg(test)]
async fn sample_mounted_memory_during<F, Fut, T>(
    mount_pid: u32,
    metrics_path: Option<&Path>,
    work: F,
) -> Result<(T, Option<MountedMemoryReport>)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let (mounted_memory_stop, mounted_memory_handle) =
        spawn_mounted_memory_sampler(mount_pid, metrics_path);
    let result = work().await;
    let _ = mounted_memory_stop.send(true);
    let mounted_memory = mounted_memory_handle
        .await
        .map_err(|error| anyhow!("mounted memory sampler panicked: {error}"))?
        .into_report();
    result.map(|value| (value, mounted_memory))
}

async fn sample_mounted_profile_during<F, Fut, T>(
    mount_pid: u32,
    metrics_path: Option<&Path>,
    work: F,
) -> Result<(
    T,
    Option<MountedMemoryReport>,
    Option<MountedPortableProfileReport>,
    Option<MountedWritebackTimelineReport>,
)>
where
    F: FnOnce(MountedPortablePhaseControl) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let (mounted_profile_stop, phase_control, mounted_profile_handle) =
        spawn_mounted_profile_sampler(mount_pid, metrics_path);
    let result = work(phase_control).await;
    let _ = mounted_profile_stop.send(true);
    let (mounted_memory, mounted_portable_profile, mounted_writeback_timeline) =
        mounted_profile_handle
            .await
            .map_err(|error| anyhow!("mounted profile sampler panicked: {error}"))?
            .into_reports();
    result.map(|value| {
        (
            value,
            mounted_memory,
            mounted_portable_profile,
            mounted_writeback_timeline,
        )
    })
}

async fn scan_legacy_pending_upload_artifacts(upload_dir: &Path) -> Result<usize> {
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

async fn scan_pending_uploads(
    metrics_path: Option<&Path>,
    expected_publisher_pid: u32,
    upload_dir: Option<&Path>,
) -> Result<usize> {
    if let Some(metrics_path) = metrics_path
        && let Some(snapshot) =
            read_write_path_observability_snapshot(metrics_path, expected_publisher_pid).await?
    {
        return Ok(snapshot.pending_uploads);
    }

    if let Some(upload_dir) = upload_dir {
        return scan_legacy_pending_upload_artifacts(upload_dir).await;
    }

    Ok(0)
}

async fn wait_for_pending_upload_target(
    metrics_path: Option<&Path>,
    expected_publisher_pid: u32,
    upload_dir: Option<&Path>,
    target: PendingUploadTarget,
    required_pending_uploads: usize,
    timeout: Duration,
) -> Result<PendingUploadObservation> {
    let start = Instant::now();
    loop {
        let pending_uploads =
            scan_pending_uploads(metrics_path, expected_publisher_pid, upload_dir).await?;
        if target.matches(pending_uploads, required_pending_uploads) {
            return Ok(PendingUploadObservation {
                target,
                observed: true,
                required_pending_uploads,
                waited: start.elapsed(),
                pending_uploads,
            });
        }
        if start.elapsed() >= timeout {
            return Ok(PendingUploadObservation {
                target,
                observed: false,
                required_pending_uploads,
                waited: start.elapsed(),
                pending_uploads,
            });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn pending_upload_validation_message(observation: &PendingUploadObservation) -> Option<String> {
    if observation.observed {
        return None;
    }

    Some(match observation.target {
        PendingUploadTarget::Active => format!(
            "did not observe pending_uploads >= {} before timeout",
            observation.required_pending_uploads.max(1)
        ),
        PendingUploadTarget::Idle => "did not observe pending_uploads == 0 before timeout".into(),
    })
}

fn build_mounted_write_path_window(
    before: Option<VfsWritePathObservabilitySnapshot>,
    after: Option<VfsWritePathObservabilitySnapshot>,
) -> Option<MountedWritePathWindowReport> {
    if before.is_none() && after.is_none() {
        return None;
    }
    Some(MountedWritePathWindowReport { before, after })
}

fn append_mounted_writeback_timeline(
    target: &mut Option<MountedWritebackTimelineReport>,
    extra: Option<MountedWritebackTimelineReport>,
) {
    let Some(mut extra) = extra else {
        return;
    };
    if extra.samples.is_empty() {
        return;
    }
    let Some(target) = target else {
        *target = Some(extra);
        return;
    };

    let offset_ms = target
        .samples
        .last()
        .map(|sample| sample.elapsed_ms)
        .unwrap_or_default();
    for sample in &mut extra.samples {
        sample.elapsed_ms = sample.elapsed_ms.saturating_add(offset_ms);
    }
    target.samples.append(&mut extra.samples);
}

fn snapshot_delta_u64(
    before: Option<&VfsWritePathObservabilitySnapshot>,
    after: Option<&VfsWritePathObservabilitySnapshot>,
    get: impl Fn(&VfsWritePathObservabilitySnapshot) -> u64,
) -> u64 {
    let before = before.map(&get).unwrap_or_default();
    let after = after.map(&get).unwrap_or_default();
    after.saturating_sub(before)
}

fn snapshot_delta_f64(
    before: Option<&VfsWritePathObservabilitySnapshot>,
    after: Option<&VfsWritePathObservabilitySnapshot>,
    get: impl Fn(&VfsWritePathObservabilitySnapshot) -> f64,
) -> f64 {
    let before = before.map(&get).unwrap_or_default();
    let after = after.map(&get).unwrap_or_default();
    (after - before).max(0.0)
}

fn after_dirty_pressure_u64(
    after: Option<&VfsWritePathObservabilitySnapshot>,
    get: impl Fn(&studiofs_vfs::VfsWritePathDirtyPressureStats) -> usize,
) -> Option<u64> {
    after.map(|snapshot| get(&snapshot.dirty_pressure) as u64)
}

fn build_mounted_write_diagnostics_report(
    writes: &OpReport,
    mounted_memory: Option<&MountedMemoryReport>,
    before: Option<&VfsWritePathObservabilitySnapshot>,
    after: Option<&VfsWritePathObservabilitySnapshot>,
) -> MountedWriteDiagnosticsReport {
    let winfsp_write_bytes_delta = snapshot_delta_u64(before, after, |snapshot| {
        snapshot.winfsp_write_path.write_bytes
    });
    let vfs_write_bytes_delta = snapshot_delta_u64(before, after, |snapshot| {
        snapshot.vfs_write_path.write_bytes
    });
    let payload_persisted_bytes_delta =
        snapshot_delta_u64(before, after, |snapshot| snapshot.payload_burst.total_bytes);
    let upload_accounted_bytes_delta = snapshot_delta_u64(before, after, |snapshot| {
        snapshot
            .upload
            .completed_upload_bytes
            .saturating_add(snapshot.upload.pending_upload_bytes)
    });
    let payload_accounted_bytes_delta = if payload_persisted_bytes_delta > 0 {
        payload_persisted_bytes_delta
    } else {
        upload_accounted_bytes_delta
    };
    let landing_accepted_bytes_delta = snapshot_delta_u64(before, after, |snapshot| {
        snapshot.landing_zone.accepted_bytes
    });
    let payload_persist_calls_delta =
        snapshot_delta_u64(before, after, |snapshot| snapshot.payload_persist.calls);
    let local_cache_io_calls_delta =
        snapshot_delta_u64(before, after, |snapshot| snapshot.local_cache_io.calls);
    let local_cache_backend_io_total_ms_delta = snapshot_delta_f64(before, after, |snapshot| {
        snapshot.local_cache_io.backend_io_total_ms
    });
    let hard_backpressure_events_delta = snapshot_delta_u64(before, after, |snapshot| {
        snapshot.write_backpressure.hard_events
    });
    let soft_throttle_events_delta = snapshot_delta_u64(before, after, |snapshot| {
        snapshot.write_backpressure.soft_throttle_events
    });
    let known_local_write_amplification = (writes.bytes > 0).then(|| {
        (payload_accounted_bytes_delta + landing_accepted_bytes_delta) as f64 / writes.bytes as f64
    });
    let write_throughput_target_met =
        writes.throughput_bytes_per_sec >= OPERATOR_WRITE_THROUGHPUT_TARGET_BYTES_PER_SEC;
    let rss_bytes_peak = mounted_memory.and_then(|memory| memory.rss_bytes_peak);
    let rss_target_met = rss_bytes_peak.map(|rss| rss <= OPERATOR_RSS_PEAK_TARGET_BYTES);
    let landing_journal_active = landing_accepted_bytes_delta > 0;

    let mut blockers = Vec::new();
    if !write_throughput_target_met {
        blockers.push("write throughput below 1000 MiB/s target".to_string());
    }
    match rss_target_met {
        Some(true) => {}
        Some(false) => blockers.push("peak RSS exceeded 2 GiB target".to_string()),
        None => blockers.push("peak RSS unavailable".to_string()),
    }
    if landing_journal_active {
        blockers.push("landing journal accepted bytes during mounted write".to_string());
    }
    match known_local_write_amplification {
        Some(amplification) if amplification <= OPERATOR_KNOWN_WRITE_AMPLIFICATION_TARGET => {}
        Some(_) => blockers.push("known local write amplification exceeded 1.20x".to_string()),
        None => blockers.push("known local write amplification unavailable".to_string()),
    }

    MountedWriteDiagnosticsReport {
        pass: blockers.is_empty(),
        blockers,
        write_throughput_target_bytes_per_sec: OPERATOR_WRITE_THROUGHPUT_TARGET_BYTES_PER_SEC,
        write_throughput_bytes_per_sec: writes.throughput_bytes_per_sec,
        write_throughput_mib_per_sec: writes.throughput_bytes_per_sec / (1024.0 * 1024.0),
        write_throughput_target_met,
        rss_peak_target_bytes: OPERATOR_RSS_PEAK_TARGET_BYTES,
        rss_bytes_peak,
        rss_target_met,
        logical_write_bytes: writes.bytes,
        winfsp_write_bytes_delta,
        vfs_write_bytes_delta,
        payload_persisted_bytes_delta,
        payload_accounted_bytes_delta,
        landing_accepted_bytes_delta,
        known_local_write_amplification,
        landing_journal_active,
        dirty_bytes_peak: mounted_memory.and_then(|memory| memory.dirty_bytes_peak),
        staged_bytes_peak: mounted_memory.and_then(|memory| memory.staged_bytes_peak),
        dirty_limit_bytes: mounted_memory.and_then(|memory| memory.dirty_limit_bytes),
        writer_stack_dirty_bytes_after: after_dirty_pressure_u64(after, |pressure| {
            pressure.writer_stack_dirty_bytes
        }),
        pending_persisted_slice_bytes_after: after_dirty_pressure_u64(after, |pressure| {
            pressure.pending_persisted_slice_bytes
        }),
        pending_upload_bytes_after: after_dirty_pressure_u64(after, |pressure| {
            pressure.pending_upload_bytes
        }),
        cache_pressure_bytes_after: after_dirty_pressure_u64(after, |pressure| {
            pressure.cache_pressure_bytes
        }),
        total_accounted_bytes_after: after_dirty_pressure_u64(after, |pressure| {
            pressure.total_accounted_bytes
        }),
        payload_persist_calls_delta,
        local_cache_io_calls_delta,
        local_cache_backend_io_total_ms_delta,
        hard_backpressure_events_delta,
        soft_throttle_events_delta,
    }
}

fn mounted_bridge_type() -> &'static str {
    #[cfg(windows)]
    {
        "winfsp"
    }
    #[cfg(not(windows))]
    {
        "fuse"
    }
}

fn metadata_root_from_cache_path(cache_path: &Path) -> Option<String> {
    let cache_base = cache_path.parent()?;
    if cache_base.file_name().and_then(|name| name.to_str()) != Some("cache") {
        return None;
    }
    cache_base.parent().map(|path| path.display().to_string())
}

fn build_mounted_runtime_report(
    params: &BenchmarkConfig,
    mount_point: &Path,
    cache_path: Option<&Path>,
    workload: &WorkloadPlan,
) -> MountedRuntimeReport {
    MountedRuntimeReport {
        mounted_path: mount_point.display().to_string(),
        cache_root: cache_path
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "unavailable".to_string()),
        metadata_root: cache_path.and_then(metadata_root_from_cache_path),
        block_size: params.block_size,
        chunk_size: studiofs_meta::schema::DEFAULT_CHUNK_SIZE as u64,
        read_size: params.read_size,
        workload_shape: MountedWorkloadShapeReport {
            sizes: workload.source.clone(),
            repeat: params.workload_repeat,
            files: workload.sizes.len(),
            small_files: workload.small_files,
            medium_files: workload.medium_files,
            large_files: workload.large_files,
            access_pattern: format!("{:?}", params.access_pattern).to_lowercase(),
            concurrency: params.concurrency,
            sequential_streams: params.sequential_streams,
            io_depth: params.io_depth,
        },
        os: std::env::consts::OS.to_string(),
        bridge_type: mounted_bridge_type().to_string(),
    }
}

fn per_second_f64(value: u64, uptime_secs: f64) -> f64 {
    if uptime_secs <= f64::EPSILON {
        0.0
    } else {
        value as f64 / uptime_secs
    }
}

fn mounted_post_upload_idle_report(
    comparison: Option<&MountedLocalFirstReadComparisonReport>,
) -> Option<MountedPostUploadIdleReport> {
    let comparison = comparison?;
    let phase = comparison
        .phases
        .iter()
        .find(|phase| phase.phase == "upload-drain-read")
        .or_else(|| {
            comparison
                .phases
                .iter()
                .find(|phase| phase.phase == "post-upload-read")
        })?;
    Some(MountedPostUploadIdleReport {
        observed: phase.target_state_observed,
        timed_out: !phase.target_state_observed,
        wait_ms: phase.wait_ms,
        pending_uploads_before: phase.pending_uploads_before,
        pending_uploads_after: phase.pending_uploads_after,
        reason: phase.validation_message.clone(),
    })
}

fn build_mounted_observability_report(
    snapshot: Option<&VfsWritePathObservabilitySnapshot>,
    mounted_memory: Option<&MountedMemoryReport>,
    comparison: Option<&MountedLocalFirstReadComparisonReport>,
) -> Option<MountedObservabilityReport> {
    let post_upload_idle = mounted_post_upload_idle_report(comparison);
    let process = mounted_memory.map(|memory| MountedProcessReport {
        mount_pid: memory.mount_pid,
        rss_bytes_steady_state: memory.rss_bytes_steady_state,
        rss_bytes_peak: memory.rss_bytes_peak,
    });
    let Some(snapshot) = snapshot else {
        if post_upload_idle.is_none() && process.is_none() {
            return None;
        }
        return Some(MountedObservabilityReport {
            write_callback: None,
            writer: None,
            cached_store: None,
            upload: None,
            fetch: None,
            process,
            barriers: None,
            post_upload_idle,
        });
    };

    Some(MountedObservabilityReport {
        write_callback: Some(MountedWriteCallbackReport {
            callbacks: snapshot.winfsp_callbacks.write,
            bytes: snapshot.winfsp_write_path.write_bytes,
            throughput_bytes_per_sec: per_second_f64(
                snapshot.winfsp_write_path.write_bytes,
                snapshot.uptime_secs,
            ),
            latency: snapshot.winfsp_write_path.callback.clone(),
        }),
        writer: Some(MountedWriterStateReport {
            dirty_bytes: snapshot.dirty_pressure.total_dirty_bytes,
            dirty_slice_count: snapshot.dirty_pressure.dirty_slice_count,
            frozen_slice_count: snapshot.dirty_pressure.frozen_slice_count,
            staged_bytes: snapshot.dirty_pressure.pending_persisted_slice_bytes,
            dirty_limit_bytes: snapshot.dirty_pressure.dirty_limit_bytes,
        }),
        cached_store: Some(MountedCachedStoreReport {
            staging_bytes: snapshot.dirty_pressure.pending_persisted_slice_bytes,
            staging_throughput_bytes_per_sec: per_second_f64(
                snapshot.payload_burst.total_bytes,
                snapshot.uptime_secs,
            ),
            staging_latency: snapshot.payload_persist.clone(),
            local_cache_backend_latency: snapshot.local_cache_io.clone(),
            cache_counters_available: false,
            cache_counters_unavailable_reason: Some(
                "mounted write-path snapshot does not expose read-cache hit/miss counters"
                    .to_string(),
            ),
            cache_hits: None,
            cache_misses: None,
        }),
        upload: Some(MountedUploadReport {
            backlog_count: snapshot.upload.pending_uploads,
            backlog_bytes: snapshot.upload.pending_upload_bytes,
            throughput_bytes_per_sec: snapshot.upload.completed_upload_bytes_per_sec,
            retry_count: snapshot.upload.pending_retry_count,
            active_uploads: snapshot.upload.active_uploads,
            completed_uploads: snapshot.upload.completed_uploads,
            completed_upload_bytes: snapshot.upload.completed_upload_bytes,
            raw: snapshot.upload.clone(),
        }),
        fetch: Some(MountedFetchReport {
            cache_hits: snapshot.fetch.cache_hits,
            s3_fetches: snapshot.fetch.s3_fetches,
            s3_errors: snapshot.fetch.s3_errors,
            singleflight_joins: snapshot.fetch.singleflight_joins,
            retries: snapshot.fetch.retries,
            bytes_downloaded: snapshot.fetch.bytes_downloaded,
            prefetch_skips: snapshot.fetch.prefetch_skips,
            raw: snapshot.fetch.clone(),
        }),
        process,
        barriers: Some(MountedBarrierReport {
            flush_barrier_calls: snapshot.flush_barrier_calls,
            cleanup_barrier_calls: snapshot.winfsp_foreground_flushes.cleanup,
            fsync_barrier_calls: snapshot.payload_store_sync_data.calls,
            close_barrier_calls: snapshot.winfsp_foreground_flushes.close,
            flush_fsync_close_latency: snapshot.payload_store_sync_data.clone(),
        }),
        post_upload_idle,
    })
}

fn mounted_write_chunk_size(block_size: u64) -> usize {
    usize::try_from(block_size)
        .unwrap_or(MAX_TEST_WRITE_CHUNK_BYTES)
        .clamp(1, MAX_TEST_WRITE_CHUNK_BYTES)
}

fn mounted_read_chunk_size(read_size: u64) -> usize {
    usize::try_from(read_size)
        .unwrap_or(MAX_TEST_WRITE_CHUNK_BYTES)
        .clamp(1, MAX_TEST_WRITE_CHUNK_BYTES)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MountedReadPlan {
    bytes_to_read: u64,
    chunk_size: usize,
}

#[derive(Clone, Copy)]
struct MountedWriteOptions {
    chunk_size: usize,
    run_seed: u64,
}

#[derive(Clone, Copy)]
struct MountedReadValidation {
    seed: u64,
    write_chunk_size: usize,
}

#[derive(Clone, Copy)]
struct MountedReadOptions {
    read_size: u64,
    full_file_reads: bool,
    write_chunk_size: usize,
    run_seed: u64,
}

fn mounted_read_plan(file_size: u64, read_size: u64, full_file_read: bool) -> MountedReadPlan {
    MountedReadPlan {
        bytes_to_read: if full_file_read {
            file_size
        } else {
            read_size.min(file_size).max(1)
        },
        chunk_size: mounted_read_chunk_size(read_size),
    }
}

fn deterministic_write_chunks(total_size: u64, max_chunk_size: usize) -> Vec<(u64, usize)> {
    let max_chunk_size = max_chunk_size.max(1) as u64;
    let mut chunks = Vec::new();
    let mut offset = 0u64;
    while offset < total_size {
        let len = (total_size - offset).min(max_chunk_size) as usize;
        chunks.push((offset, len));
        offset += len as u64;
    }
    chunks
}

fn deterministic_run_seed(run_id: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in run_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn deterministic_write_seed(item: &WorkFile, run_seed: u64, stream_id: Option<usize>) -> u64 {
    run_seed
        .wrapping_add(item.index as u64)
        .wrapping_add(123)
        .wrapping_add(stream_id.unwrap_or_default() as u64)
}

async fn write_deterministic_file(
    path: &Path,
    total_size: u64,
    seed: u64,
    max_chunk_size: usize,
) -> Result<()> {
    let chunks = deterministic_write_chunks(total_size, max_chunk_size);
    let max_len = chunks.iter().map(|(_, len)| *len).max().unwrap_or_default();
    let buffer = deterministic_bytes(max_len, seed);
    let mut out = fs::File::create(path)
        .await
        .with_context(|| format!("create {}", path.display()))?;
    for (_, len) in chunks {
        out.write_all(&buffer[..len])
            .await
            .with_context(|| format!("write {}", path.display()))?;
    }
    out.flush().await?;
    Ok(())
}

fn validate_deterministic_read_chunk(
    path: &Path,
    actual: &[u8],
    file_offset: u64,
    expected_cycle: &[u8],
) -> Result<()> {
    if actual.is_empty() || expected_cycle.is_empty() {
        return Ok(());
    }

    let mut checked = 0usize;
    while checked < actual.len() {
        let cycle_offset = ((file_offset as usize).wrapping_add(checked)) % expected_cycle.len();
        let take = (actual.len() - checked).min(expected_cycle.len() - cycle_offset);
        let actual_part = &actual[checked..checked + take];
        let expected_part = &expected_cycle[cycle_offset..cycle_offset + take];
        if actual_part != expected_part {
            let mismatch = actual_part
                .iter()
                .zip(expected_part)
                .position(|(actual, expected)| actual != expected)
                .unwrap_or(0);
            let absolute_offset = file_offset
                .saturating_add(checked as u64)
                .saturating_add(mismatch as u64);
            bail!(
                "deterministic content mismatch reading {} at offset {}: expected 0x{:02x}, got 0x{:02x}",
                path.display(),
                absolute_offset,
                expected_part[mismatch],
                actual_part[mismatch]
            );
        }
        checked += take;
    }

    Ok(())
}

async fn read_file_with_plan(
    path: &Path,
    plan: MountedReadPlan,
    validation: MountedReadValidation,
) -> Result<u64> {
    let mut input = fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    if plan.bytes_to_read == 0 {
        return Ok(0);
    }

    let mut buffer = vec![0u8; plan.chunk_size];
    let expected_cycle = deterministic_bytes(validation.write_chunk_size.max(1), validation.seed);
    let mut remaining = plan.bytes_to_read;
    let mut offset = 0u64;
    while remaining > 0 {
        let len = remaining.min(plan.chunk_size as u64) as usize;
        input
            .read_exact(&mut buffer[..len])
            .await
            .with_context(|| format!("read {}", path.display()))?;
        validate_deterministic_read_chunk(path, &buffer[..len], offset, &expected_cycle)?;
        remaining -= len as u64;
        offset = offset.saturating_add(len as u64);
    }
    Ok(plan.bytes_to_read)
}

async fn run_mounted_write_phase(
    params: &BenchmarkConfig,
    files: &[WorkFile],
    order: &[usize],
    size_class_bounds: SizeClassBounds,
) -> Result<(PhaseTelemetry, Vec<PhaseTelemetry>)> {
    let progress = ProgressBar::start("StudioFS test write", order.len());
    let counter = progress.counter();
    let write_options = MountedWriteOptions {
        chunk_size: mounted_write_chunk_size(params.block_size),
        run_seed: deterministic_run_seed(&params.run_id),
    };
    if params.sequential_streams {
        let writes_by_stream = run_filesystem_writes_sequential_streams(
            files,
            order,
            params.concurrency,
            params.io_depth,
            write_options,
            size_class_bounds,
            counter,
        )
        .await;
        progress.finish();
        let writes_by_stream = writes_by_stream?;
        let mut merged = PhaseTelemetry::default();
        for stream in &writes_by_stream {
            merged += stream;
        }
        Ok((merged, writes_by_stream))
    } else {
        let writes = run_filesystem_writes(
            files,
            order,
            params.io_depth,
            params.concurrency,
            write_options,
            size_class_bounds,
            counter,
        )
        .await;
        progress.finish();
        Ok((writes?, Vec::new()))
    }
}

async fn run_mounted_read_phase(
    params: &BenchmarkConfig,
    files: &[WorkFile],
    order: &[usize],
    size_class_bounds: SizeClassBounds,
) -> Result<(PhaseTelemetry, Vec<PhaseTelemetry>)> {
    let progress = ProgressBar::start("StudioFS test read", order.len());
    let counter = progress.counter();
    let read_options = MountedReadOptions {
        read_size: params.read_size,
        full_file_reads: params.full_file_reads,
        write_chunk_size: mounted_write_chunk_size(params.block_size),
        run_seed: deterministic_run_seed(&params.run_id),
    };
    if params.sequential_streams {
        let reads_by_stream = run_filesystem_reads_sequential_streams(
            files,
            order,
            params.concurrency,
            params.io_depth,
            read_options,
            size_class_bounds,
            counter,
        )
        .await;
        progress.finish();
        let reads_by_stream = reads_by_stream?;
        let mut merged = PhaseTelemetry::default();
        for stream in &reads_by_stream {
            merged += stream;
        }
        Ok((merged, reads_by_stream))
    } else {
        let reads = run_filesystem_reads(
            files,
            order,
            params.io_depth,
            params.concurrency,
            read_options,
            size_class_bounds,
            counter,
        )
        .await;
        progress.finish();
        Ok((reads?, Vec::new()))
    }
}

fn operation_log_targets(files: &[WorkFile]) -> Arc<[Arc<str>]> {
    files
        .iter()
        .map(|file| Arc::<str>::from(file.path.display().to_string()))
        .collect::<Vec<_>>()
        .into()
}

fn abort_unit_workers(handles: &[tokio::task::JoinHandle<Result<()>>]) {
    for handle in handles {
        handle.abort();
    }
}

async fn wait_for_unit_workers(
    mut handles: Vec<tokio::task::JoinHandle<Result<()>>>,
    panic_context: &'static str,
    failure_context: &'static str,
) -> Result<()> {
    while let Some(handle) = handles.pop() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                abort_unit_workers(&handles);
                return Err(err).context(failure_context);
            }
            Err(err) => {
                abort_unit_workers(&handles);
                return Err(err).context(panic_context);
            }
        }
    }
    Ok(())
}

fn abort_stream_workers(handles: &[tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>]) {
    for handle in handles {
        handle.abort();
    }
}

async fn collect_stream_workers(
    mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>>,
    stream_count: usize,
    panic_context: &'static str,
    failure_context: &'static str,
) -> Result<Vec<PhaseTelemetry>> {
    let mut by_stream = vec![PhaseTelemetry::default(); stream_count];
    while let Some(handle) = handles.pop() {
        match handle.await {
            Ok(Ok((stream_id, telemetry))) => {
                by_stream[stream_id] = telemetry;
            }
            Ok(Err(err)) => {
                abort_stream_workers(&handles);
                return Err(err).context(failure_context);
            }
            Err(err) => {
                abort_stream_workers(&handles);
                return Err(err).context(panic_context);
            }
        }
    }
    Ok(by_stream)
}

async fn run_mounted_stat_phase(
    files: &[WorkFile],
    order: &[usize],
    concurrency: usize,
    size_class_bounds: SizeClassBounds,
) -> Result<PhaseTelemetry> {
    let stat_order: Vec<usize> = order
        .iter()
        .copied()
        .filter(|work_index| {
            classify_size(
                files[*work_index].size,
                size_class_bounds.small_max,
                size_class_bounds.medium_max,
            ) == SizeClass::Small
        })
        .collect();
    let progress = ProgressBar::start("StudioFS test stat", stat_order.len());
    let counter = progress.counter();
    if stat_order.is_empty() {
        progress.finish();
        return Ok(PhaseTelemetry::default());
    }

    let worker_count = concurrency.max(1).min(stat_order.len());
    let stats = Arc::new(Mutex::new(PhaseTelemetry::default()));
    let next_operation = Arc::new(AtomicUsize::new(0));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let targets = operation_log_targets(&files);
    let stat_order: Arc<[usize]> = Arc::from(stat_order);
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let files = Arc::clone(&files);
        let targets = Arc::clone(&targets);
        let stat_order = Arc::clone(&stat_order);
        let stats = Arc::clone(&stats);
        let next_operation = Arc::clone(&next_operation);
        let counter = counter.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = next_operation.fetch_add(1, Ordering::Relaxed);
                if op_index >= stat_order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = stat_order[op_index];
                let item = &files[work_index];
                let start = Instant::now();
                let metadata = fs::metadata(&item.path)
                    .await
                    .with_context(|| format!("stat {}", item.path.display()))?;
                let elapsed = start.elapsed();
                if metadata.len() != item.size {
                    bail!(
                        "stat {} reported size {}, expected {}",
                        item.path.display(),
                        metadata.len(),
                        item.size
                    );
                }
                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("filesystem stat latency stats lock poisoned"))?;
                s.record_operation(PhaseOperationLog {
                    operation_index: op_index as u64,
                    work_index,
                    worker_id,
                    target: Arc::clone(&targets[work_index]),
                    size_class: SizeClass::Small,
                    bytes: 0,
                    elapsed,
                });
                counter.inc();
            }
        }));
    }

    wait_for_unit_workers(
        handles,
        "filesystem stat worker panicked",
        "filesystem stat worker failed",
    )
    .await?;
    progress.finish();
    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("filesystem stat worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("filesystem stat latency stats lock poisoned"))
}

async fn cleanup_mount_artifacts_report(path: &Path) -> Result<OpReport> {
    let mut telemetry = PhaseTelemetry::default();
    let started = Instant::now();
    match fs::remove_dir_all(path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("remove {}", path.display())),
    }
    let elapsed = started.elapsed();
    telemetry.record(SizeClass::Small, elapsed, 0);
    Ok(phase_telemetry_to_report(&telemetry, elapsed))
}

async fn cleanup_mount_artifacts_best_effort(path: &Path, run_id: &str) -> Option<OpReport> {
    match cleanup_mount_artifacts_report(path).await {
        Ok(report) => Some(report),
        Err(error) => {
            tracing::warn!(
                target: "studiofs.cache.profile",
                run_id,
                error = %error,
                work_root = %path.display(),
                "cache benchmark cleanup failed"
            );
            None
        }
    }
}

async fn run_filesystem_writes(
    files: &[WorkFile],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
    write_options: MountedWriteOptions,
    size_class_bounds: SizeClassBounds,
    progress: ProgressCounter,
) -> Result<PhaseTelemetry> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(PhaseTelemetry::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let targets = operation_log_targets(&files);
    let order: Arc<[usize]> = Arc::from(order.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let files = Arc::clone(&files);
        let targets = Arc::clone(&targets);
        let order = Arc::clone(&order);
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);
        let progress = progress.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let item = &files[work_index];
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem write semaphore closed: {e}"))?;
                let _permit = permit;
                let start = Instant::now();
                write_deterministic_file(
                    &item.path,
                    item.size,
                    deterministic_write_seed(item, write_options.run_seed, None),
                    write_options.chunk_size,
                )
                .await
                .with_context(|| format!("write {}", item.path.display()))?;

                let elapsed = start.elapsed();
                let class = classify_size(
                    item.size,
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("filesystem write latency stats lock poisoned"))?;
                s.record_operation(PhaseOperationLog {
                    operation_index: op_index as u64,
                    work_index,
                    worker_id,
                    target: Arc::clone(&targets[work_index]),
                    size_class: class,
                    bytes: item.size,
                    elapsed,
                });
                progress.inc();
            }
        }));
    }

    wait_for_unit_workers(
        handles,
        "filesystem write worker panicked",
        "filesystem write worker failed",
    )
    .await?;

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
    read_options: MountedReadOptions,
    size_class_bounds: SizeClassBounds,
    progress: ProgressCounter,
) -> Result<PhaseTelemetry> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(PhaseTelemetry::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let targets = operation_log_targets(&files);
    let order: Arc<[usize]> = Arc::from(order.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let files = Arc::clone(&files);
        let targets = Arc::clone(&targets);
        let order = Arc::clone(&order);
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);
        let progress = progress.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let item = &files[work_index];
                let read_plan = mounted_read_plan(
                    item.size,
                    read_options.read_size,
                    read_options.full_file_reads,
                );

                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem read semaphore closed: {e}"))?;
                let _permit = permit;

                let validation = MountedReadValidation {
                    seed: deterministic_write_seed(item, read_options.run_seed, None),
                    write_chunk_size: read_options.write_chunk_size,
                };
                let start = Instant::now();
                let bytes_read = read_file_with_plan(&item.path, read_plan, validation)
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
                s.record_operation(PhaseOperationLog {
                    operation_index: op_index as u64,
                    work_index,
                    worker_id,
                    target: Arc::clone(&targets[work_index]),
                    size_class: class,
                    bytes: bytes_read,
                    elapsed,
                });
                progress.inc();
            }
        }));
    }

    wait_for_unit_workers(
        handles,
        "filesystem read worker panicked",
        "filesystem read worker failed",
    )
    .await?;

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
    write_options: MountedWriteOptions,
    size_class_bounds: SizeClassBounds,
    progress: ProgressCounter,
) -> Result<Vec<PhaseTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let stream_slots = segments.len();
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let targets = operation_log_targets(&files);
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let files = Arc::clone(&files);
        let targets = Arc::clone(&targets);
        let progress = progress.clone();
        handles.push(tokio::spawn(async move {
            let mut telemetry = PhaseTelemetry::default();
            for work_index in segment {
                let item = &files[work_index];
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem sequential write semaphore closed: {e}"))?;
                let _permit = permit;
                let start = Instant::now();
                write_deterministic_file(
                    &item.path,
                    item.size,
                    deterministic_write_seed(item, write_options.run_seed, Some(stream_id)),
                    write_options.chunk_size,
                )
                .await
                .with_context(|| format!("write {}", item.path.display()))?;
                let elapsed = start.elapsed();
                let class = classify_size(
                    item.size,
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                telemetry.record_operation(PhaseOperationLog {
                    operation_index: work_index as u64,
                    work_index,
                    worker_id: stream_id,
                    target: Arc::clone(&targets[work_index]),
                    size_class: class,
                    bytes: item.size,
                    elapsed,
                });
                progress.inc();
            }
            Ok::<_, anyhow::Error>((stream_id, telemetry))
        }));
    }

    collect_stream_workers(
        handles,
        stream_slots,
        "filesystem sequential write stream panicked",
        "filesystem sequential write stream failed",
    )
    .await
}

async fn run_filesystem_reads_sequential_streams(
    files: &[WorkFile],
    order: &[usize],
    stream_count: usize,
    io_depth: usize,
    read_options: MountedReadOptions,
    size_class_bounds: SizeClassBounds,
    progress: ProgressCounter,
) -> Result<Vec<PhaseTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let stream_slots = segments.len();
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let targets = operation_log_targets(&files);
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let files = Arc::clone(&files);
        let targets = Arc::clone(&targets);
        let progress = progress.clone();
        handles.push(tokio::spawn(async move {
            let mut telemetry = PhaseTelemetry::default();
            for work_index in segment {
                let item = &files[work_index];
                let read_plan = mounted_read_plan(
                    item.size,
                    read_options.read_size,
                    read_options.full_file_reads,
                );
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem sequential read semaphore closed: {e}"))?;
                let _permit = permit;
                let validation = MountedReadValidation {
                    seed: deterministic_write_seed(item, read_options.run_seed, Some(stream_id)),
                    write_chunk_size: read_options.write_chunk_size,
                };
                let start = Instant::now();
                let bytes_read = read_file_with_plan(&item.path, read_plan, validation)
                    .await
                    .with_context(|| format!("read {}", item.path.display()))?;
                let elapsed = start.elapsed();
                let class = classify_size(
                    item.size,
                    size_class_bounds.small_max,
                    size_class_bounds.medium_max,
                );
                telemetry.record_operation(PhaseOperationLog {
                    operation_index: work_index as u64,
                    work_index,
                    worker_id: stream_id,
                    target: Arc::clone(&targets[work_index]),
                    size_class: class,
                    bytes: bytes_read,
                    elapsed,
                });
                progress.inc();
            }
            Ok::<_, anyhow::Error>((stream_id, telemetry))
        }));
    }

    collect_stream_workers(
        handles,
        stream_slots,
        "filesystem sequential read stream panicked",
        "filesystem sequential read stream failed",
    )
    .await
}

async fn clear_mount_artifacts(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("remove {}", path.display())),
    }
    fs::create_dir_all(path)
        .await
        .with_context(|| format!("create {}", path.display()))
}

pub(crate) async fn run_mounted_end_to_end(
    params: &BenchmarkConfig,
    mount_pid: u32,
    volume: &str,
    mount_point: &Path,
    work_root: &Path,
    cache_path: Option<&Path>,
) -> Result<BenchmarkReport> {
    let benchmark_result = async {
        clear_mount_artifacts(work_root)
            .await
            .context("prepare mount benchmark directory")?;

        let size_class_bounds = SizeClassBounds {
            small_max: params.size_class_small_max,
            medium_max: params.size_class_medium_max,
        };
        let workload = build_work_sizes(
            params.block_size,
            &params.sizes,
            size_class_bounds,
            params.workload_repeat,
        );
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
        let write_path_observability_path =
            cache_path.map(|path| path.join(WRITE_PATH_OBSERVABILITY_FILE));

        let measured_write_path_before = match write_path_observability_path.as_deref() {
            Some(path) => read_write_path_observability_snapshot(path, mount_pid).await?,
            None => None,
        };
        let measured_write_path_before_for_report = measured_write_path_before.clone();
        let files_ref = &files;
        let order_ref = &order;
        let cache_upload_dir_ref = &cache_upload_dir;
        let write_path_observability_path_ref = &write_path_observability_path;
        let (measured, mounted_memory, mounted_portable_profile, mounted_writeback_timeline) =
            sample_mounted_profile_during(
                mount_pid,
                write_path_observability_path.as_deref(),
                move |portable_profile| async move {
                    let started = Instant::now();
                    let started_cpu = process_cpu_time();
                    let started_at =
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

                    tracing::info!(
                        target: "studiofs.cache.profile",
                        run_id = %params.run_id,
                        phase = "e2e-write",
                        "cache benchmark phase start"
                    );
                    portable_profile.set_phase("write");
                    let writes_wall_start = Instant::now();
                    let writes_cpu_start = process_cpu_time();
                    let (writes, writes_by_stream) =
                        run_mounted_write_phase(params, files_ref, order_ref, size_class_bounds)
                            .await?;
                    let writes_wall = writes_wall_start.elapsed();
                    let writes_cpu = writes_cpu_start
                        .and_then(|start_cpu| {
                            process_cpu_time().map(|end_cpu| end_cpu.saturating_sub(start_cpu))
                        })
                        .unwrap_or_default();

                    let during_upload_observation =
                        if let Some(upload_dir) = cache_upload_dir_ref.as_deref() {
                            Some(
                                wait_for_pending_upload_target(
                                    write_path_observability_path_ref.as_deref(),
                                    mount_pid,
                                    Some(upload_dir),
                                    PendingUploadTarget::Active,
                                    params.during_upload_min_pending,
                                    params.during_upload_timeout,
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
                    portable_profile.set_phase("during-upload-read");
                    let reads_wall_start = Instant::now();
                    let reads_cpu_start = process_cpu_time();
                    let (reads, reads_by_stream) =
                        run_mounted_read_phase(params, files_ref, order_ref, size_class_bounds)
                            .await?;
                    let reads_wall = reads_wall_start.elapsed();
                    let reads_cpu = reads_cpu_start
                        .and_then(|start_cpu| {
                            process_cpu_time().map(|end_cpu| end_cpu.saturating_sub(start_cpu))
                        })
                        .unwrap_or_default();

                    tracing::info!(
                        target: "studiofs.cache.profile",
                        run_id = %params.run_id,
                        phase = "e2e-stat",
                        "cache benchmark phase start"
                    );
                    portable_profile.set_phase("stat");
                    let stats_wall_start = Instant::now();
                    let stats = run_mounted_stat_phase(
                        files_ref,
                        order_ref,
                        params.concurrency,
                        size_class_bounds,
                    )
                    .await?;
                    let stats_wall = stats_wall_start.elapsed();

                    let total_wall = Instant::now() - started;
                    let total_cpu = started_cpu
                        .and_then(|cpu_start| {
                            process_cpu_time().map(|cpu_end| cpu_end.saturating_sub(cpu_start))
                        })
                        .unwrap_or_default();

                    Ok::<_, anyhow::Error>(MeasuredMountedPhaseResult {
                        started,
                        started_at,
                        writes,
                        writes_by_stream,
                        writes_wall,
                        writes_cpu,
                        reads,
                        reads_by_stream,
                        reads_wall,
                        reads_cpu,
                        stats,
                        stats_wall,
                        total_wall,
                        total_cpu,
                        during_upload_observation,
                    })
                },
            )
            .await?;
        let mut measured_write_path_after_for_report =
            match write_path_observability_path.as_deref() {
                Some(path) => read_write_path_observability_snapshot(path, mount_pid).await?,
                None => None,
            };
        let mut mounted_writeback_timeline = mounted_writeback_timeline;

        let profile = params.profile.then_some(BenchmarkProfileReport {
            enabled: true,
            profile_mode: "pyroscope".to_string(),
            cpu_profile: CpuProfileReport {
                total_wall_ms: measured.total_wall.as_secs_f64() * 1000.0,
                total_cpu_ms: measured.total_cpu.as_secs_f64() * 1000.0,
                cpu_utilization_percent: cpu_utilization_percent(
                    measured.total_cpu,
                    measured.total_wall,
                ),
                write_phase_cpu_percent: cpu_utilization_percent(
                    measured.writes_cpu,
                    measured.writes_wall,
                ),
                read_phase_cpu_percent: cpu_utilization_percent(
                    measured.reads_cpu,
                    measured.reads_wall,
                ),
            },
            endpoint: None,
            artifacts: Vec::new(),
            warnings: Vec::new(),
        });
        let local_first_read_comparison = match (cache_path, cache_upload_dir.as_deref()) {
            (Some(cache_path), Some(upload_dir)) => {
                let during_phase = MountedLocalFirstReadPhaseReport {
                    phase: "during-upload-read".to_string(),
                    wait_target: measured
                        .during_upload_observation
                        .as_ref()
                        .map(|observation| observation.target.label().to_string())
                        .unwrap_or_else(|| PendingUploadTarget::Active.label().to_string()),
                    target_state_observed: measured
                        .during_upload_observation
                        .as_ref()
                        .map(|observation| observation.observed)
                        .unwrap_or(false),
                    valid_for_target: measured
                        .during_upload_observation
                        .as_ref()
                        .map(|observation| observation.observed)
                        .unwrap_or(false),
                    required_pending_uploads: measured
                        .during_upload_observation
                        .as_ref()
                        .map(|observation| observation.required_pending_uploads)
                        .unwrap_or(params.during_upload_min_pending),
                    wait_ms: measured
                        .during_upload_observation
                        .as_ref()
                        .map(|observation| observation.waited.as_secs_f64() * 1000.0)
                        .unwrap_or_default(),
                    pending_uploads_before: measured
                        .during_upload_observation
                        .as_ref()
                        .map(|observation| observation.pending_uploads)
                        .unwrap_or_default(),
                    pending_uploads_after: scan_pending_uploads(
                        write_path_observability_path.as_deref(),
                        mount_pid,
                        Some(upload_dir),
                    )
                    .await?,
                    validation_message: measured
                        .during_upload_observation
                        .as_ref()
                        .and_then(pending_upload_validation_message),
                    results: phase_telemetry_to_report(&measured.reads, measured.reads_wall),
                };
                let cache_path_for_log = upload_dir.parent().unwrap_or(upload_dir);
                let metrics_path = write_path_observability_path.as_deref();
                let ((comparison, final_snapshot), _, _, drain_timeline) =
                    sample_mounted_profile_during(
                        mount_pid,
                        metrics_path,
                        |portable_profile| async move {
                            let pending_uploads_before =
                                scan_pending_uploads(metrics_path, mount_pid, Some(upload_dir))
                                    .await?;
                            tracing::info!(
                                target: "studiofs.cache.profile",
                                run_id = %params.run_id,
                                phase = "e2e-upload-drain-wait",
                                pending_uploads_before,
                                "cache benchmark waiting for upload idle"
                            );
                            portable_profile.set_phase("upload-drain-wait");
                            let idle_observation = wait_for_pending_upload_target(
                                metrics_path,
                                mount_pid,
                                Some(upload_dir),
                                PendingUploadTarget::Idle,
                                0,
                                Duration::from_secs(60),
                            )
                            .await?;
                            let drain_phase = MountedLocalFirstReadPhaseReport {
                                phase: "upload-drain-wait".to_string(),
                                wait_target: idle_observation.target.label().to_string(),
                                target_state_observed: idle_observation.observed,
                                valid_for_target: idle_observation.observed,
                                required_pending_uploads: idle_observation.required_pending_uploads,
                                wait_ms: idle_observation.waited.as_secs_f64() * 1000.0,
                                pending_uploads_before,
                                pending_uploads_after: idle_observation.pending_uploads,
                                validation_message: pending_upload_validation_message(
                                    &idle_observation,
                                ),
                                results: phase_telemetry_to_report(
                                    &PhaseTelemetry::default(),
                                    idle_observation.waited,
                                ),
                            };

                            tracing::info!(
                                target: "studiofs.cache.profile",
                                run_id = %params.run_id,
                                phase = "e2e-post-upload-read",
                                cache_path = %cache_path_for_log.display(),
                                upload_dir = %upload_dir.display(),
                                "cache benchmark phase start"
                            );
                            portable_profile.set_phase("post-upload-read");
                            let post_wall_start = Instant::now();
                            let (post_reads, _) = run_mounted_read_phase(
                                params,
                                files_ref,
                                order_ref,
                                size_class_bounds,
                            )
                            .await?;
                            let post_wall = post_wall_start.elapsed();
                            let post_phase = MountedLocalFirstReadPhaseReport {
                                phase: "post-upload-read".to_string(),
                                wait_target: idle_observation.target.label().to_string(),
                                target_state_observed: idle_observation.observed,
                                valid_for_target: idle_observation.observed,
                                required_pending_uploads: idle_observation.required_pending_uploads,
                                wait_ms: 0.0,
                                pending_uploads_before: idle_observation.pending_uploads,
                                pending_uploads_after: scan_pending_uploads(
                                    metrics_path,
                                    mount_pid,
                                    Some(upload_dir),
                                )
                                .await?,
                                validation_message: pending_upload_validation_message(
                                    &idle_observation,
                                ),
                                results: phase_telemetry_to_report(&post_reads, post_wall),
                            };

                            let final_snapshot = match metrics_path {
                                Some(path) => {
                                    read_write_path_observability_snapshot(path, mount_pid).await?
                                }
                                None => None,
                            };

                            Ok::<_, anyhow::Error>((
                                MountedLocalFirstReadComparisonReport {
                                    cache_path: cache_path.display().to_string(),
                                    upload_dir: upload_dir.display().to_string(),
                                    phases: vec![during_phase, drain_phase, post_phase],
                                },
                                final_snapshot,
                            ))
                        },
                    )
                    .await?;
                append_mounted_writeback_timeline(&mut mounted_writeback_timeline, drain_timeline);
                measured_write_path_after_for_report = final_snapshot;
                Some(comparison)
            }
            _ => None,
        };
        let mounted_write_path_window = build_mounted_write_path_window(
            measured_write_path_before,
            measured_write_path_after_for_report.clone(),
        );
        let advanced_metrics = build_advanced_metrics(
            size_class_bounds,
            &measured.writes,
            &measured.reads,
            None,
            None,
            measured.writes_wall.as_secs_f64() * 1000.0,
            measured.reads_wall.as_secs_f64() * 1000.0,
        );
        let stream_results = stream_reports_from_telemetry(
            &measured.writes_by_stream,
            &measured.reads_by_stream,
            measured.writes_wall.as_secs_f64() * 1000.0,
            measured.reads_wall.as_secs_f64() * 1000.0,
        );
        let mounted_runtime = Some(build_mounted_runtime_report(
            params,
            mount_point,
            cache_path,
            &workload,
        ));
        let mounted_observability = build_mounted_observability_report(
            measured_write_path_after_for_report.as_ref(),
            mounted_memory.as_ref(),
            local_first_read_comparison.as_ref(),
        );
        let writes_report = phase_telemetry_to_report(&measured.writes, measured.writes_wall);
        let reads_report = phase_telemetry_to_report(&measured.reads, measured.reads_wall);
        let mounted_write_diagnostics = Some(build_mounted_write_diagnostics_report(
            &writes_report,
            mounted_memory.as_ref(),
            measured_write_path_before_for_report.as_ref(),
            measured_write_path_after_for_report.as_ref(),
        ));

        let cleanup = cleanup_mount_artifacts_best_effort(work_root, &params.run_id).await;
        let total_elapsed_ms = (Instant::now() - measured.started).as_secs_f64() * 1000.0;
        let completed_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        Ok::<_, anyhow::Error>(BenchmarkReport {
            run_id: params.run_id.clone(),
            mode: "e2e".to_string(),
            cache_dir: params.target.display().to_string(),
            started_at: measured.started_at,
            completed_at,
            total_elapsed_ms,
            block_size: params.block_size,
            read_size: params.read_size,
            concurrency: params.concurrency,
            sequential_streams: params.sequential_streams,
            io_depth: params.io_depth,
            warmup_runs: params.warmup_runs,
            sizes: workload.source,
            workload_repeat: params.workload_repeat,
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
            mounted_memory,
            mounted_runtime,
            mounted_observability,
            mounted_write_path_window,
            mounted_write_diagnostics,
            mounted_writeback_timeline,
            mounted_portable_profile,
            local_first_read_comparison,
            small_file_stats: Some(phase_telemetry_to_report(
                &measured.stats,
                measured.stats_wall,
            )),
            cleanup,
            writes: writes_report,
            reads: reads_report,
        })
    }
    .await;
    match benchmark_result {
        Ok(report) => Ok(report),
        Err(err) => {
            let _ = fs::remove_dir_all(work_root).await;
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use studiofs_vfs::vfs::{VfsWritePathVfsWriteStats, VfsWritePathWinfspWritePathStats};
    use studiofs_vfs::{
        VfsPortableAllocatorStats, VfsWritePathBackpressureStats, VfsWritePathBurstStats,
        VfsWritePathDirtyPressureStats, VfsWritePathLandingZoneStats,
        VfsWritePathLocalCacheIoStats, VfsWritePathObservabilitySnapshot,
        VfsWritePathStageLatencyStats,
    };

    fn target_tempdir() -> tempfile::TempDir {
        let target = std::env::current_dir()
            .expect("current dir")
            .join("target")
            .join("studiofs-mounted-tests");
        std::fs::create_dir_all(&target).expect("create target test dir");
        tempfile::Builder::new()
            .prefix("mounted-")
            .tempdir_in(target)
            .expect("tempdir")
    }

    fn benchmark_config_with_block_size(block_size: u64) -> BenchmarkConfig {
        BenchmarkConfig {
            target: PathBuf::from("/mnt/studiofs-benchmark"),
            block_size,
            read_size: 128 * 1024,
            size_class_small_max: 10 * 1024,
            size_class_medium_max: block_size,
            workload_repeat: 1,
            concurrency: 1,
            sequential_streams: false,
            io_depth: 1,
            warmup_runs: 0,
            sizes: super::super::workload::WorkloadSizeInput::ExplicitList(vec![4096]),
            access_pattern: super::super::BenchmarkAccessPattern::Sequential,
            during_upload_min_pending: 1,
            during_upload_timeout: Duration::from_secs(1),
            l1_limit: 64 * 1024 * 1024,
            profile: false,
            full_file_reads: false,
            run_id: "test-run".to_string(),
        }
    }

    fn single_file_workload() -> WorkloadPlan {
        WorkloadPlan {
            sizes: vec![4096],
            source: "explicit-list(1)".to_string(),
            small_files: 1,
            medium_files: 0,
            large_files: 0,
        }
    }

    async fn write_snapshot(
        path: &Path,
        publisher_pid: u32,
        total_dirty_bytes: usize,
        dirty_limit_bytes: usize,
    ) {
        write_snapshot_with_pressure(
            path,
            publisher_pid,
            total_dirty_bytes,
            dirty_limit_bytes,
            0,
            0,
        )
        .await;
    }

    async fn write_snapshot_with_pressure(
        path: &Path,
        publisher_pid: u32,
        total_dirty_bytes: usize,
        dirty_limit_bytes: usize,
        pending_uploads: usize,
        staged_peak_bytes: usize,
    ) {
        let snapshot = VfsWritePathObservabilitySnapshot {
            publisher_pid,
            dirty_pressure: VfsWritePathDirtyPressureStats {
                total_dirty_bytes,
                dirty_limit_bytes,
                staged_peak_bytes,
                ..Default::default()
            },
            pending_uploads,
            ..Default::default()
        };
        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        fs::write(path, bytes).await.expect("write snapshot");
    }

    async fn write_snapshot_with_allocator(
        path: &Path,
        publisher_pid: u32,
        current_bytes: u64,
        peak_bytes: u64,
    ) {
        let snapshot = VfsWritePathObservabilitySnapshot {
            publisher_pid,
            allocator: Some(VfsPortableAllocatorStats {
                current_bytes,
                peak_bytes,
                allocated_total_bytes: peak_bytes,
                deallocated_total_bytes: peak_bytes.saturating_sub(current_bytes),
                allocation_count: 7,
                deallocation_count: 3,
                reallocation_count: 1,
            }),
            ..Default::default()
        };
        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        fs::write(path, bytes).await.expect("write snapshot");
    }

    #[tokio::test]
    async fn read_write_path_observability_snapshot_ignores_mismatched_publisher_pid() {
        let temp = target_tempdir();
        let metrics_path = temp.path().join("write_path_observability.json");
        write_snapshot(&metrics_path, 7, 512, 1024).await;

        let snapshot = read_write_path_observability_snapshot(&metrics_path, 42)
            .await
            .expect("read snapshot");

        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn sample_mounted_memory_during_scopes_collection_to_measured_run() {
        let temp = target_tempdir();
        let metrics_path = temp.path().join("write_path_observability.json");
        let mount_pid = std::process::id();

        write_snapshot(&metrics_path, mount_pid, 64, 1024).await;
        let (_, mounted_memory) =
            sample_mounted_memory_during(mount_pid, Some(&metrics_path), || async {
                write_snapshot_with_pressure(&metrics_path, mount_pid, 256, 1024, 0, 512).await;
                tokio::time::sleep(MOUNTED_MEMORY_SAMPLE_INTERVAL * 2).await;
                Ok::<_, anyhow::Error>(())
            })
            .await
            .expect("sample measured run");

        write_snapshot(&metrics_path, mount_pid, 4096, 1024).await;
        tokio::time::sleep(MOUNTED_MEMORY_SAMPLE_INTERVAL * 2).await;

        let mounted_memory = mounted_memory.expect("mounted memory");
        assert_eq!(mounted_memory.dirty_bytes_peak, Some(256));
        assert_eq!(mounted_memory.staged_bytes_peak, Some(512));
        assert_eq!(mounted_memory.dirty_limit_bytes, Some(1024));
    }

    #[tokio::test]
    async fn read_file_with_plan_rejects_corrupt_deterministic_payload() {
        let temp = target_tempdir();
        let path = temp.path().join("benchmark_file_000000.bin");
        fs::write(&path, vec![0xAA; 4096])
            .await
            .expect("write corrupt test payload");

        let plan = mounted_read_plan(4096, 1024, true);
        let result = read_file_with_plan(
            &path,
            plan,
            MountedReadValidation {
                seed: 123,
                write_chunk_size: 1024,
            },
        )
        .await;

        assert!(
            result.is_err(),
            "mounted reads should validate deterministic workload contents, not just byte counts"
        );
    }

    #[tokio::test]
    async fn read_file_with_plan_accepts_deterministic_payload_across_write_chunks() {
        let temp = target_tempdir();
        let path = temp.path().join("benchmark_file_000000.bin");
        write_deterministic_file(&path, 4096 + 257, 123, 1024)
            .await
            .expect("write deterministic test payload");

        let bytes_read = read_file_with_plan(
            &path,
            mounted_read_plan(4096 + 257, 1536, true),
            MountedReadValidation {
                seed: 123,
                write_chunk_size: 1024,
            },
        )
        .await
        .expect("read deterministic test payload");

        assert_eq!(bytes_read, 4096 + 257);
    }

    #[tokio::test]
    async fn mounted_stat_phase_records_only_small_files() {
        let temp = target_tempdir();
        let small_path = temp.path().join("small.bin");
        let medium_path = temp.path().join("medium.bin");
        fs::write(&small_path, vec![0x11; 1024])
            .await
            .expect("write small file");
        fs::write(&medium_path, vec![0x22; 8192])
            .await
            .expect("write medium file");
        let files = vec![
            WorkFile {
                path: small_path,
                size: 1024,
                index: 0,
            },
            WorkFile {
                path: medium_path,
                size: 8192,
                index: 1,
            },
        ];

        let telemetry = run_mounted_stat_phase(
            &files,
            &[0, 1],
            4,
            SizeClassBounds {
                small_max: 1024,
                medium_max: 8192,
            },
        )
        .await
        .expect("stat phase");
        let report = phase_telemetry_to_report(&telemetry, Duration::from_millis(1));

        assert_eq!(report.operations, 1);
        assert_eq!(report.bytes, 0);
        assert_eq!(report.operations_log.len(), 1);
        assert_eq!(report.operations_log[0].work_index, 0);
        assert_eq!(report.operations_log[0].size_class, "small");
    }

    #[tokio::test]
    async fn cleanup_mount_artifacts_best_effort_keeps_report_on_cleanup_failure() {
        let temp = target_tempdir();
        let file_path = temp.path().join("not-a-directory");
        fs::write(&file_path, b"still here")
            .await
            .expect("write file");

        let cleanup = cleanup_mount_artifacts_best_effort(&file_path, "run-1").await;

        assert!(cleanup.is_none());
        assert!(file_path.exists());
    }

    #[tokio::test]
    async fn sample_mounted_profile_during_tracks_phase_and_allocator_peaks() {
        let temp = target_tempdir();
        let metrics_path = temp.path().join("write_path_observability.json");
        let mount_pid = std::process::id();

        write_snapshot_with_allocator(&metrics_path, mount_pid, 64, 128).await;
        let metrics_path_ref = &metrics_path;
        let (_, mounted_memory, portable_profile, _) = sample_mounted_profile_during(
            mount_pid,
            Some(&metrics_path),
            move |profile| async move {
                profile.set_phase("write");
                write_snapshot_with_allocator(metrics_path_ref, mount_pid, 256, 512).await;
                tokio::time::sleep(MOUNTED_MEMORY_SAMPLE_INTERVAL * 2).await;
                profile.set_phase("during-upload-read");
                write_snapshot_with_allocator(metrics_path_ref, mount_pid, 128, 1024).await;
                tokio::time::sleep(MOUNTED_MEMORY_SAMPLE_INTERVAL * 2).await;
                Ok::<_, anyhow::Error>(())
            },
        )
        .await
        .expect("sample mounted profile");

        assert!(mounted_memory.is_some());
        let portable_profile = portable_profile.expect("portable profile");
        assert_eq!(
            portable_profile.allocator.as_ref().unwrap().peak_bytes,
            1024
        );
        assert!(
            portable_profile
                .phases
                .iter()
                .any(|phase| phase.phase == "write"
                    && phase.allocator.as_ref().unwrap().peak_bytes == 512)
        );
        assert!(
            portable_profile
                .phases
                .iter()
                .any(|phase| phase.phase == "during-upload-read"
                    && phase.allocator.as_ref().unwrap().peak_bytes == 1024)
        );
    }

    #[test]
    fn pending_upload_validation_message_explains_missing_active_uploads() {
        let observation = PendingUploadObservation {
            target: PendingUploadTarget::Active,
            observed: false,
            required_pending_uploads: 4,
            waited: Duration::from_secs(10),
            pending_uploads: 0,
        };

        assert_eq!(
            pending_upload_validation_message(&observation).as_deref(),
            Some("did not observe pending_uploads >= 4 before timeout")
        );
    }

    #[test]
    fn pending_upload_validation_message_explains_missing_idle_uploads() {
        let observation = PendingUploadObservation {
            target: PendingUploadTarget::Idle,
            observed: false,
            required_pending_uploads: 0,
            waited: Duration::from_secs(60),
            pending_uploads: 2,
        };

        assert_eq!(
            pending_upload_validation_message(&observation).as_deref(),
            Some("did not observe pending_uploads == 0 before timeout")
        );
    }

    #[tokio::test]
    async fn scan_pending_uploads_prefers_live_snapshot_pending_count() {
        let temp = target_tempdir();
        let metrics_path = temp.path().join("write_path_observability.json");
        let upload_dir = temp.path().join("upload");
        fs::create_dir_all(&upload_dir)
            .await
            .expect("create legacy upload dir");
        fs::write(upload_dir.join("legacy.pending"), b"legacy")
            .await
            .expect("write legacy pending artifact");
        write_snapshot_with_pressure(&metrics_path, 42, 0, 1024, 7, 0).await;

        assert_eq!(
            scan_pending_uploads(Some(&metrics_path), 42, Some(&upload_dir))
                .await
                .expect("scan pending uploads"),
            7
        );
    }

    #[test]
    fn mounted_write_path_window_is_absent_when_both_snapshots_missing() {
        assert!(build_mounted_write_path_window(None, None).is_none());
    }

    #[test]
    fn mounted_runtime_report_uses_schema_chunk_size_not_block_size() {
        let block_size = studiofs_meta::schema::DEFAULT_CHUNK_SIZE as u64 / 2;
        let params = benchmark_config_with_block_size(block_size);
        let workload = single_file_workload();

        let report = build_mounted_runtime_report(
            &params,
            Path::new("/mnt/studiofs-benchmark"),
            None,
            &workload,
        );

        assert_eq!(report.block_size, block_size);
        assert_eq!(
            report.chunk_size,
            studiofs_meta::schema::DEFAULT_CHUNK_SIZE as u64
        );
    }

    #[test]
    fn mounted_observability_staging_throughput_uses_cumulative_payload_bytes() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            uptime_secs: 2.0,
            dirty_pressure: VfsWritePathDirtyPressureStats {
                pending_persisted_slice_bytes: 999,
                ..Default::default()
            },
            payload_burst: VfsWritePathBurstStats {
                total_bytes: 4096,
                ..Default::default()
            },
            ..Default::default()
        };

        let report = build_mounted_observability_report(Some(&snapshot), None, None)
            .expect("observability report");
        let cached_store = report.cached_store.expect("cached store report");

        assert_eq!(cached_store.staging_bytes, 999);
        assert_eq!(cached_store.staging_throughput_bytes_per_sec, 2048.0);
    }

    #[test]
    fn mounted_writeback_timeline_accumulates_second_deltas() {
        let mut timeline = MountedWritebackTimelineAccumulator::new(1000);
        let before = VfsWritePathObservabilitySnapshot {
            generated_unix_ms: 10_000,
            winfsp_write_path: VfsWritePathWinfspWritePathStats {
                write_bytes: 10 * 1024 * 1024,
                ..Default::default()
            },
            payload_burst: VfsWritePathBurstStats {
                total_bytes: 5 * 1024 * 1024,
                ..Default::default()
            },
            payload_persist: VfsWritePathStageLatencyStats {
                calls: 2,
                total_ms: 6.0,
                ..Default::default()
            },
            local_cache_io: VfsWritePathLocalCacheIoStats {
                calls: 2,
                backend_io_total_ms: 5.0,
                backend_io_max_ms: 3.0,
                ..Default::default()
            },
            write_backpressure: VfsWritePathBackpressureStats {
                events: 1,
                wait_total_ms: 4,
                soft_throttle_events: 1,
                hard_events: 0,
                ..Default::default()
            },
            upload: studiofs_vfs::VfsUploadStats {
                completed_upload_bytes: 1024,
                ..Default::default()
            },
            fetch: studiofs_vfs::VfsFetchStats {
                ..Default::default()
            },
            ..Default::default()
        };
        let after = VfsWritePathObservabilitySnapshot {
            generated_unix_ms: 11_000,
            winfsp_write_path: VfsWritePathWinfspWritePathStats {
                write_bytes: 74 * 1024 * 1024,
                ..Default::default()
            },
            payload_burst: VfsWritePathBurstStats {
                total_bytes: 37 * 1024 * 1024,
                ..Default::default()
            },
            payload_persist: VfsWritePathStageLatencyStats {
                calls: 10,
                total_ms: 50.0,
                ..Default::default()
            },
            local_cache_io: VfsWritePathLocalCacheIoStats {
                calls: 11,
                backend_io_total_ms: 45.0,
                backend_io_max_ms: 12.0,
                ..Default::default()
            },
            write_backpressure: VfsWritePathBackpressureStats {
                events: 4,
                wait_total_ms: 29,
                soft_throttle_events: 3,
                hard_events: 1,
                ..Default::default()
            },
            dirty_pressure: VfsWritePathDirtyPressureStats {
                writer_stack_dirty_bytes: 64 * 1024 * 1024,
                pending_persisted_slice_bytes: 128 * 1024 * 1024,
                pending_upload_bytes: 256 * 1024 * 1024,
                ..Default::default()
            },
            upload: studiofs_vfs::VfsUploadStats {
                pending_uploads: 4,
                active_uploads: 2,
                completed_upload_bytes: 17 * 1024 * 1024 + 1024,
                ..Default::default()
            },
            fetch: studiofs_vfs::VfsFetchStats {
                ..Default::default()
            },
            ..Default::default()
        };

        timeline.observe("write", &before);
        timeline.observe("write", &after);
        let report = timeline.into_report().expect("timeline report");

        assert_eq!(report.bucket_ms, 1000);
        assert_eq!(report.samples.len(), 1);
        let sample = &report.samples[0];
        assert_eq!(sample.elapsed_ms, 1000);
        assert_eq!(sample.phase, "write");
        assert_eq!(
            sample.foreground_write_bytes_per_sec,
            64.0 * 1024.0 * 1024.0
        );
        assert_eq!(sample.payload_persist_bytes_per_sec, 32.0 * 1024.0 * 1024.0);
        assert_eq!(
            sample.upload_completed_bytes_per_sec,
            17.0 * 1024.0 * 1024.0
        );
        assert_eq!(sample.pending_upload_bytes, 256 * 1024 * 1024);
        assert_eq!(sample.upload_queue_depth, 4);
        assert_eq!(sample.active_uploads, 2);
        assert_eq!(sample.backpressure_events_delta, 3);
        assert_eq!(sample.backpressure_wait_ms_delta, 25);
        assert_eq!(sample.soft_throttle_events_delta, 2);
        assert_eq!(sample.hard_backpressure_events_delta, 1);
        assert_eq!(sample.materialization_calls_delta, 8);
        assert_eq!(sample.materialization_total_ms_delta, 44.0);
        assert_eq!(sample.local_cache_io_calls_delta, 9);
        assert_eq!(sample.local_cache_backend_io_total_ms_delta, 40.0);
        assert_eq!(sample.local_cache_backend_io_cumulative_max_ms, 12.0);
    }

    #[test]
    fn post_upload_idle_report_uses_visible_upload_drain_read_phase() {
        let comparison = MountedLocalFirstReadComparisonReport {
            cache_path: r"D:\cache".to_string(),
            upload_dir: r"D:\cache\upload".to_string(),
            phases: vec![
                MountedLocalFirstReadPhaseReport {
                    phase: "upload-drain-read".to_string(),
                    wait_target: "idle".to_string(),
                    target_state_observed: true,
                    valid_for_target: true,
                    required_pending_uploads: 0,
                    wait_ms: 1234.0,
                    pending_uploads_before: 9,
                    pending_uploads_after: 0,
                    validation_message: None,
                    results: super::super::result::OpReport {
                        operations: 2,
                        bytes: 8 * 1024 * 1024,
                        elapsed_ms: 100.0,
                        avg_latency_ms: 50.0,
                        p50_ms: 50.0,
                        p95_ms: 50.0,
                        p99_ms: 50.0,
                        throughput_bytes_per_sec: 80.0 * 1024.0 * 1024.0,
                        latency_histogram: super::super::result::LatencyHistogramReport {
                            bucket_upper_bounds_ns: vec![50_000],
                            bucket_counts: vec![2, 0],
                        },
                        operations_log: Vec::new(),
                    },
                },
                MountedLocalFirstReadPhaseReport {
                    phase: "post-upload-read".to_string(),
                    wait_target: "idle".to_string(),
                    target_state_observed: true,
                    valid_for_target: true,
                    required_pending_uploads: 0,
                    wait_ms: 0.0,
                    pending_uploads_before: 0,
                    pending_uploads_after: 0,
                    validation_message: None,
                    results: super::super::result::OpReport {
                        operations: 1,
                        bytes: 8 * 1024 * 1024,
                        elapsed_ms: 50.0,
                        avg_latency_ms: 50.0,
                        p50_ms: 50.0,
                        p95_ms: 50.0,
                        p99_ms: 50.0,
                        throughput_bytes_per_sec: 160.0 * 1024.0 * 1024.0,
                        latency_histogram: super::super::result::LatencyHistogramReport {
                            bucket_upper_bounds_ns: vec![50_000],
                            bucket_counts: vec![1, 0],
                        },
                        operations_log: Vec::new(),
                    },
                },
            ],
        };

        let report = mounted_post_upload_idle_report(Some(&comparison)).expect("idle report");

        assert_eq!(report.wait_ms, 1234.0);
        assert_eq!(report.pending_uploads_before, 9);
        assert_eq!(report.pending_uploads_after, 0);
    }

    #[test]
    fn deterministic_write_chunks_cover_file_without_full_file_buffer() {
        let chunks = deterministic_write_chunks(10, 4);

        assert_eq!(chunks, vec![(0, 4), (4, 4), (8, 2)]);
        assert!(chunks.iter().all(|(_, len)| *len <= 4));
    }

    #[test]
    fn mounted_operator_read_plan_reads_full_file_in_read_size_chunks() {
        let plan = mounted_read_plan(8 * 1024 * 1024 * 1024, 4 * 1024 * 1024, true);

        assert_eq!(plan.bytes_to_read, 8 * 1024 * 1024 * 1024);
        assert_eq!(plan.chunk_size, 4 * 1024 * 1024);
    }

    #[test]
    fn mounted_test_cache_read_plan_keeps_single_sample_read() {
        let plan = mounted_read_plan(8 * 1024 * 1024 * 1024, 4 * 1024 * 1024, false);

        assert_eq!(plan.bytes_to_read, 4 * 1024 * 1024);
        assert_eq!(plan.chunk_size, 4 * 1024 * 1024);
    }

    #[test]
    fn mounted_write_diagnostics_uses_upload_accounting_when_payload_burst_is_unavailable() {
        let after = VfsWritePathObservabilitySnapshot {
            winfsp_write_path: VfsWritePathWinfspWritePathStats {
                write_bytes: 1024,
                ..Default::default()
            },
            upload: studiofs_vfs::VfsUploadStats {
                pending_upload_bytes: 400,
                completed_upload_bytes: 624,
                ..Default::default()
            },
            ..Default::default()
        };
        let memory = MountedMemoryReport {
            mount_pid: 4242,
            rss_bytes_steady_state: Some(512),
            rss_bytes_peak: Some(512),
            dirty_bytes_steady_state: Some(0),
            dirty_bytes_peak: Some(0),
            staged_bytes_peak: Some(0),
            dirty_limit_bytes: Some(2 * 1024 * 1024 * 1024),
        };
        let writes = super::super::result::OpReport {
            operations: 1,
            bytes: 1024,
            elapsed_ms: 1.0,
            avg_latency_ms: 1.0,
            p50_ms: 1.0,
            p95_ms: 1.0,
            p99_ms: 1.0,
            throughput_bytes_per_sec: 1_200.0 * 1024.0 * 1024.0,
            latency_histogram: super::super::result::LatencyHistogramReport {
                bucket_upper_bounds_ns: vec![50_000],
                bucket_counts: vec![1, 0],
            },
            operations_log: Vec::new(),
        };

        let diagnostics =
            build_mounted_write_diagnostics_report(&writes, Some(&memory), None, Some(&after));

        assert_eq!(diagnostics.payload_accounted_bytes_delta, 1024);
        assert_eq!(diagnostics.known_local_write_amplification, Some(1.0));
        assert!(diagnostics.pass);
    }

    #[test]
    fn mounted_write_diagnostics_reports_gate_metrics_from_write_path_window() {
        let before = VfsWritePathObservabilitySnapshot {
            winfsp_write_path: VfsWritePathWinfspWritePathStats {
                write_bytes: 100,
                ..Default::default()
            },
            vfs_write_path: VfsWritePathVfsWriteStats {
                write_bytes: 100,
                ..Default::default()
            },
            payload_burst: VfsWritePathBurstStats {
                total_bytes: 100,
                ..Default::default()
            },
            landing_zone: VfsWritePathLandingZoneStats {
                accepted_bytes: 0,
                ..Default::default()
            },
            payload_persist: VfsWritePathStageLatencyStats {
                calls: 2,
                ..Default::default()
            },
            local_cache_io: VfsWritePathLocalCacheIoStats {
                calls: 4,
                backend_io_total_ms: 11.0,
                ..Default::default()
            },
            write_backpressure: VfsWritePathBackpressureStats {
                hard_events: 1,
                soft_throttle_events: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        let after = VfsWritePathObservabilitySnapshot {
            winfsp_write_path: VfsWritePathWinfspWritePathStats {
                write_bytes: 1124,
                ..Default::default()
            },
            vfs_write_path: VfsWritePathVfsWriteStats {
                write_bytes: 1124,
                ..Default::default()
            },
            payload_burst: VfsWritePathBurstStats {
                total_bytes: 1124,
                ..Default::default()
            },
            landing_zone: VfsWritePathLandingZoneStats {
                accepted_bytes: 0,
                ..Default::default()
            },
            payload_persist: VfsWritePathStageLatencyStats {
                calls: 10,
                ..Default::default()
            },
            local_cache_io: VfsWritePathLocalCacheIoStats {
                calls: 14,
                backend_io_total_ms: 51.0,
                ..Default::default()
            },
            write_backpressure: VfsWritePathBackpressureStats {
                hard_events: 3,
                soft_throttle_events: 7,
                ..Default::default()
            },
            dirty_pressure: VfsWritePathDirtyPressureStats {
                writer_stack_dirty_bytes: 64,
                pending_persisted_slice_bytes: 128,
                pending_upload_bytes: 256,
                cache_pressure_bytes: 384,
                total_accounted_bytes: 512,
                ..Default::default()
            },
            ..Default::default()
        };
        let memory = MountedMemoryReport {
            mount_pid: 4242,
            rss_bytes_steady_state: Some(768 * 1024 * 1024),
            rss_bytes_peak: Some(1024 * 1024 * 1024),
            dirty_bytes_steady_state: Some(128),
            dirty_bytes_peak: Some(256),
            staged_bytes_peak: Some(512),
            dirty_limit_bytes: Some(2 * 1024 * 1024 * 1024),
        };
        let writes = super::super::result::OpReport {
            operations: 1,
            bytes: 1024,
            elapsed_ms: 1.0,
            avg_latency_ms: 1.0,
            p50_ms: 1.0,
            p95_ms: 1.0,
            p99_ms: 1.0,
            throughput_bytes_per_sec: 1_200.0 * 1024.0 * 1024.0,
            latency_histogram: super::super::result::LatencyHistogramReport {
                bucket_upper_bounds_ns: vec![50_000],
                bucket_counts: vec![1, 0],
            },
            operations_log: Vec::new(),
        };

        let diagnostics = build_mounted_write_diagnostics_report(
            &writes,
            Some(&memory),
            Some(&before),
            Some(&after),
        );

        assert!(diagnostics.pass);
        assert!(diagnostics.write_throughput_target_met);
        assert_eq!(diagnostics.rss_target_met, Some(true));
        assert_eq!(diagnostics.payload_persisted_bytes_delta, 1024);
        assert_eq!(diagnostics.landing_accepted_bytes_delta, 0);
        assert_eq!(diagnostics.known_local_write_amplification, Some(1.0));
        assert!(!diagnostics.landing_journal_active);
        assert_eq!(diagnostics.total_accounted_bytes_after, Some(512));
        assert_eq!(diagnostics.hard_backpressure_events_delta, 2);
        assert_eq!(diagnostics.soft_throttle_events_delta, 5);
        assert!(diagnostics.blockers.is_empty());
    }
}

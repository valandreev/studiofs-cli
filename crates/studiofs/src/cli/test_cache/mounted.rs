use crate::cli::WRITE_PATH_OBSERVABILITY_FILE;
use anyhow::{Context, Result, anyhow};
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

use super::TestCacheParams;
use super::profiling::{cpu_utilization_percent, process_cpu_time};
use super::progress::{ProgressBar, ProgressCounter};
use super::result::{
    BenchmarkProfileReport, BenchmarkReport, CpuProfileReport,
    MountedLocalFirstReadComparisonReport, MountedLocalFirstReadPhaseReport, MountedMemoryReport,
    MountedWritePathWindowReport,
};
use super::telemetry::{
    PhaseOperationLog, PhaseTelemetry, build_advanced_metrics, phase_telemetry_to_report,
    stream_reports_from_telemetry,
};
use super::workload::{
    SizeClassBounds, WorkFile, build_access_sequence, build_file_worklist, build_work_sizes,
    classify_size, deterministic_bytes, split_stream_segments,
};

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

    fn observe_dirty_pressure(&mut self, dirty_bytes: u64, dirty_limit_bytes: u64) {
        self.dirty_bytes_steady_state = Some(dirty_bytes);
        self.dirty_bytes_peak = self.dirty_bytes_peak.max(dirty_bytes);
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
            dirty_limit_bytes: self
                .dirty_bytes_steady_state
                .map(|_| self.dirty_limit_bytes),
        })
    }
}

fn sample_mount_process_rss(system: &mut System, pid: Pid) -> Option<u64> {
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    system.process(pid).map(|process| process.memory())
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

fn spawn_mounted_memory_sampler(
    mount_pid: u32,
    metrics_path: Option<&Path>,
) -> (
    watch::Sender<bool>,
    tokio::task::JoinHandle<MountedMemoryAccumulator>,
) {
    let pid = Pid::from_u32(mount_pid);
    let metrics_path = metrics_path.map(Path::to_path_buf);
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let mut system = System::new();
        let mut accumulator = MountedMemoryAccumulator::new(mount_pid);
        let mut warned_metrics_error = false;

        loop {
            if let Some(rss_bytes) = sample_mount_process_rss(&mut system, pid) {
                accumulator.observe_rss(rss_bytes);
            }
            if let Some(metrics_path) = metrics_path.as_deref() {
                match read_write_path_observability_snapshot(metrics_path, mount_pid).await {
                    Ok(Some(snapshot)) => accumulator.observe_dirty_pressure(
                        snapshot.dirty_pressure.total_dirty_bytes as u64,
                        snapshot.dirty_pressure.dirty_limit_bytes as u64,
                    ),
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

        if let Some(rss_bytes) = sample_mount_process_rss(&mut system, pid) {
            accumulator.observe_rss(rss_bytes);
        }
        if let Some(metrics_path) = metrics_path.as_deref()
            && let Ok(Some(snapshot)) =
                read_write_path_observability_snapshot(metrics_path, mount_pid).await
        {
            accumulator.observe_dirty_pressure(
                snapshot.dirty_pressure.total_dirty_bytes as u64,
                snapshot.dirty_pressure.dirty_limit_bytes as u64,
            );
        }

        accumulator
    });
    (stop_tx, handle)
}

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

async fn run_mounted_write_phase(
    params: &TestCacheParams,
    files: &[WorkFile],
    order: &[usize],
    size_class_bounds: SizeClassBounds,
) -> Result<(PhaseTelemetry, Vec<PhaseTelemetry>)> {
    let progress = ProgressBar::start("StudioFS test write", order.len());
    let counter = progress.counter();
    if params.sequential_streams {
        let writes_by_stream = run_filesystem_writes_sequential_streams(
            files,
            order,
            params.concurrency,
            params.io_depth,
            size_class_bounds,
            counter,
        )
        .await;
        progress.finish();
        let writes_by_stream = writes_by_stream?;
        let mut merged = PhaseTelemetry::default();
        for stream in &writes_by_stream {
            merged.merge_from_ref(stream);
        }
        Ok((merged, writes_by_stream))
    } else {
        let writes = run_filesystem_writes(
            files,
            order,
            params.io_depth,
            params.concurrency,
            size_class_bounds,
            counter,
        )
        .await;
        progress.finish();
        Ok((writes?, Vec::new()))
    }
}

async fn run_mounted_read_phase(
    params: &TestCacheParams,
    files: &[WorkFile],
    order: &[usize],
    size_class_bounds: SizeClassBounds,
) -> Result<(PhaseTelemetry, Vec<PhaseTelemetry>)> {
    let progress = ProgressBar::start("StudioFS test read", order.len());
    let counter = progress.counter();
    if params.sequential_streams {
        let reads_by_stream = run_filesystem_reads_sequential_streams(
            files,
            order,
            params.concurrency,
            params.io_depth,
            params.read_size,
            size_class_bounds,
            counter,
        )
        .await;
        progress.finish();
        let reads_by_stream = reads_by_stream?;
        let mut merged = PhaseTelemetry::default();
        for stream in &reads_by_stream {
            merged.merge_from_ref(stream);
        }
        Ok((merged, reads_by_stream))
    } else {
        let reads = run_filesystem_reads(
            files,
            order,
            params.io_depth,
            params.concurrency,
            params.read_size,
            size_class_bounds,
            counter,
        )
        .await;
        progress.finish();
        Ok((reads?, Vec::new()))
    }
}

async fn run_filesystem_writes(
    files: &[WorkFile],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
    size_class_bounds: SizeClassBounds,
    progress: ProgressCounter,
) -> Result<PhaseTelemetry> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(PhaseTelemetry::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let order: Arc<[usize]> = Arc::from(order.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let files = Arc::clone(&files);
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
                let data = deterministic_bytes(
                    usize::try_from(item.size).context("file write size overflow")?,
                    item.index as u64 + 123,
                );

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
                s.record_operation(PhaseOperationLog {
                    operation_index: op_index as u64,
                    work_index,
                    worker_id,
                    target: item.path.display().to_string(),
                    size_class: class,
                    bytes: item.size,
                    elapsed,
                });
                progress.inc();
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
    progress: ProgressCounter,
) -> Result<PhaseTelemetry> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(PhaseTelemetry::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let order: Arc<[usize]> = Arc::from(order.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let files = Arc::clone(&files);
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
                s.record_operation(PhaseOperationLog {
                    operation_index: op_index as u64,
                    work_index,
                    worker_id,
                    target: item.path.display().to_string(),
                    size_class: class,
                    bytes: read_len as u64,
                    elapsed,
                });
                progress.inc();
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
    progress: ProgressCounter,
) -> Result<Vec<PhaseTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let files = Arc::clone(&files);
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
                let data = deterministic_bytes(
                    usize::try_from(item.size).context("sequential file write size overflow")?,
                    item.index as u64 + 123 + stream_id as u64,
                );
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
                telemetry.record_operation(PhaseOperationLog {
                    operation_index: work_index as u64,
                    work_index,
                    worker_id: stream_id,
                    target: item.path.display().to_string(),
                    size_class: class,
                    bytes: item.size,
                    elapsed,
                });
                progress.inc();
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
    progress: ProgressCounter,
) -> Result<Vec<PhaseTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let files = Arc::clone(&files);
        let progress = progress.clone();
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
                telemetry.record_operation(PhaseOperationLog {
                    operation_index: work_index as u64,
                    work_index,
                    worker_id: stream_id,
                    target: item.path.display().to_string(),
                    size_class: class,
                    bytes: read_len as u64,
                    elapsed,
                });
                progress.inc();
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

pub(super) async fn run_mounted_end_to_end(
    params: &TestCacheParams,
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
        let mut baseline_local_first_read_phase = None;
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
                write_path_observability_path.as_deref(),
                mount_pid,
                Some(upload_dir),
                PendingUploadTarget::Active,
                params.during_upload_min_pending,
                params.during_upload_timeout,
            )
            .await?;
            let idle_before_baseline = wait_for_pending_upload_target(
                write_path_observability_path.as_deref(),
                mount_pid,
                Some(upload_dir),
                PendingUploadTarget::Idle,
                0,
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
            baseline_local_first_read_phase = Some(MountedLocalFirstReadPhaseReport {
                phase: "baseline-read".to_string(),
                wait_target: idle_before_baseline.target.label().to_string(),
                target_state_observed: idle_before_baseline.observed,
                valid_for_target: idle_before_baseline.observed,
                required_pending_uploads: idle_before_baseline.required_pending_uploads,
                wait_ms: idle_before_baseline.waited.as_secs_f64() * 1000.0,
                pending_uploads_before: idle_before_baseline.pending_uploads,
                pending_uploads_after: scan_pending_uploads(
                    write_path_observability_path.as_deref(),
                    mount_pid,
                    Some(upload_dir),
                )
                .await?,
                validation_message: pending_upload_validation_message(&idle_before_baseline),
                results: phase_telemetry_to_report(&baseline_reads, baseline_wall),
            });
        }

        let measured_write_path_before = match write_path_observability_path.as_deref() {
            Some(path) => read_write_path_observability_snapshot(path, mount_pid).await?,
            None => None,
        };
        let (measured, mounted_memory) = sample_mounted_memory_during(
            mount_pid,
            write_path_observability_path.as_deref(),
            || async {
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

                let during_upload_observation =
                    if let Some(upload_dir) = cache_upload_dir.as_deref() {
                        Some(
                            wait_for_pending_upload_target(
                                write_path_observability_path.as_deref(),
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
                    total_wall,
                    total_cpu,
                    during_upload_observation,
                })
            },
        )
        .await?;
        let measured_write_path_after = match write_path_observability_path.as_deref() {
            Some(path) => read_write_path_observability_snapshot(path, mount_pid).await?,
            None => None,
        };
        let mounted_write_path_window =
            build_mounted_write_path_window(measured_write_path_before, measured_write_path_after);

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
        let local_first_read_comparison = match (
            cache_path,
            cache_upload_dir.as_deref(),
            baseline_local_first_read_phase,
        ) {
            (Some(cache_path), Some(upload_dir), Some(baseline_phase)) => {
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

                let idle_before_post = wait_for_pending_upload_target(
                    write_path_observability_path.as_deref(),
                    mount_pid,
                    Some(upload_dir),
                    PendingUploadTarget::Idle,
                    0,
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
                let post_phase = MountedLocalFirstReadPhaseReport {
                    phase: "post-upload-read".to_string(),
                    wait_target: idle_before_post.target.label().to_string(),
                    target_state_observed: idle_before_post.observed,
                    valid_for_target: idle_before_post.observed,
                    required_pending_uploads: idle_before_post.required_pending_uploads,
                    wait_ms: idle_before_post.waited.as_secs_f64() * 1000.0,
                    pending_uploads_before: idle_before_post.pending_uploads,
                    pending_uploads_after: scan_pending_uploads(
                        write_path_observability_path.as_deref(),
                        mount_pid,
                        Some(upload_dir),
                    )
                    .await?,
                    validation_message: pending_upload_validation_message(&idle_before_post),
                    results: phase_telemetry_to_report(&post_reads, post_wall),
                };

                Some(MountedLocalFirstReadComparisonReport {
                    cache_path: cache_path.display().to_string(),
                    upload_dir: upload_dir.display().to_string(),
                    phases: vec![baseline_phase, during_phase, post_phase],
                })
            }
            _ => None,
        };
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

        let _ = fs::remove_dir_all(work_root).await;
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
            mounted_write_path_window,
            local_first_read_comparison,
            writes: phase_telemetry_to_report(&measured.writes, measured.writes_wall),
            reads: phase_telemetry_to_report(&measured.reads, measured.reads_wall),
        })
    }
    .await;
    match benchmark_result {
        Ok(report) => Ok(report),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_vfs::{VfsWritePathDirtyPressureStats, VfsWritePathObservabilitySnapshot};
    use tempfile::tempdir;

    async fn write_snapshot(
        path: &Path,
        publisher_pid: u32,
        total_dirty_bytes: usize,
        dirty_limit_bytes: usize,
    ) {
        write_snapshot_with_pending(path, publisher_pid, total_dirty_bytes, dirty_limit_bytes, 0)
            .await;
    }

    async fn write_snapshot_with_pending(
        path: &Path,
        publisher_pid: u32,
        total_dirty_bytes: usize,
        dirty_limit_bytes: usize,
        pending_uploads: usize,
    ) {
        let snapshot = VfsWritePathObservabilitySnapshot {
            publisher_pid,
            dirty_pressure: VfsWritePathDirtyPressureStats {
                total_dirty_bytes,
                dirty_limit_bytes,
                ..Default::default()
            },
            pending_uploads,
            ..Default::default()
        };
        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        fs::write(path, bytes).await.expect("write snapshot");
    }

    #[tokio::test]
    async fn read_write_path_observability_snapshot_ignores_mismatched_publisher_pid() {
        let temp = tempdir().expect("tempdir");
        let metrics_path = temp.path().join("write_path_observability.json");
        write_snapshot(&metrics_path, 7, 512, 1024).await;

        let snapshot = read_write_path_observability_snapshot(&metrics_path, 42)
            .await
            .expect("read snapshot");

        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn sample_mounted_memory_during_scopes_collection_to_measured_run() {
        let temp = tempdir().expect("tempdir");
        let metrics_path = temp.path().join("write_path_observability.json");
        let mount_pid = std::process::id();

        write_snapshot(&metrics_path, mount_pid, 64, 1024).await;
        let (_, mounted_memory) =
            sample_mounted_memory_during(mount_pid, Some(&metrics_path), || async {
                write_snapshot(&metrics_path, mount_pid, 256, 1024).await;
                tokio::time::sleep(MOUNTED_MEMORY_SAMPLE_INTERVAL * 2).await;
                Ok::<_, anyhow::Error>(())
            })
            .await
            .expect("sample measured run");

        write_snapshot(&metrics_path, mount_pid, 4096, 1024).await;
        tokio::time::sleep(MOUNTED_MEMORY_SAMPLE_INTERVAL * 2).await;

        let mounted_memory = mounted_memory.expect("mounted memory");
        assert_eq!(mounted_memory.dirty_bytes_peak, Some(256));
        assert_eq!(mounted_memory.dirty_limit_bytes, Some(1024));
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

    #[tokio::test]
    async fn scan_pending_uploads_prefers_live_snapshot_pending_count() {
        let temp = tempdir().expect("tempdir");
        let metrics_path = temp.path().join("write_path_observability.json");
        let upload_dir = temp.path().join("upload");
        fs::create_dir_all(&upload_dir)
            .await
            .expect("create legacy upload dir");
        fs::write(upload_dir.join("legacy.pending"), b"legacy")
            .await
            .expect("write legacy pending artifact");
        write_snapshot_with_pending(&metrics_path, 42, 0, 1024, 7).await;

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
}

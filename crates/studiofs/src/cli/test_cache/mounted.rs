use anyhow::{Context, Result, anyhow};
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

use super::TestCacheParams;
use super::profiling::{cpu_utilization_percent, process_cpu_time};
use super::result::{
    BenchmarkProfileReport, BenchmarkReport, CpuProfileReport, MountedBufferedReadComparisonReport,
    MountedBufferedReadPhaseReport,
};
use super::telemetry::{
    PhaseTelemetry, build_advanced_metrics, phase_telemetry_to_report,
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
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let order: Arc<[usize]> = Arc::from(order.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let files = Arc::clone(&files);
        let order = Arc::clone(&order);
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
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let order: Arc<[usize]> = Arc::from(order.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let files = Arc::clone(&files);
        let order = Arc::clone(&order);
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
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let files = Arc::clone(&files);
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
    let files: Arc<[WorkFile]> = Arc::from(files.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let files = Arc::clone(&files);
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

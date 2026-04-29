use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Instant;
use studiofs_cache::{BenchmarkCache, BenchmarkCacheConfig, CacheReadLevel};
use tokio::sync::Semaphore;

use super::TestCacheParams;
use super::profiling::{cpu_utilization_percent, process_cpu_time};
use super::result::{
    BenchmarkProfileReport, BenchmarkReport, CachePipelineReport, CpuProfileReport,
};
use super::telemetry::{
    DirectReadTelemetry, PhaseOperationLog, PhaseTelemetry, build_advanced_metrics,
    phase_telemetry_to_report, stream_reports_from_telemetry,
};
use super::workload::{
    SizeClassBounds, build_access_sequence, build_work_sizes, classify_size, deterministic_bytes,
    split_stream_segments,
};

async fn run_shim_writes(
    cache: Arc<BenchmarkCache>,
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
    let order: Arc<[usize]> = Arc::from(order.to_vec());
    let sizes: Arc<[u64]> = Arc::from(sizes.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let cache = Arc::clone(&cache);
        let order = Arc::clone(&order);
        let sizes = Arc::clone(&sizes);
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
                s.record_operation(PhaseOperationLog {
                    operation_index: op_index as u64,
                    work_index,
                    worker_id,
                    target: format!("object:{object_id}"),
                    size_class: class,
                    bytes: sizes[work_index],
                    elapsed,
                });
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
    cache: Arc<BenchmarkCache>,
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
    let order: Arc<[usize]> = Arc::from(order.to_vec());
    let sizes: Arc<[u64]> = Arc::from(sizes.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let cache = Arc::clone(&cache);
        let order = Arc::clone(&order);
        let sizes = Arc::clone(&sizes);
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
                s.phase.record_operation(PhaseOperationLog {
                    operation_index: op_index as u64,
                    work_index,
                    worker_id,
                    target: format!("object:{object_id}"),
                    size_class: class,
                    bytes: read as u64,
                    elapsed,
                });
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
    cache: Arc<BenchmarkCache>,
    sizes: &[u64],
    order: &[usize],
    stream_count: usize,
    io_depth: usize,
    size_class_bounds: SizeClassBounds,
) -> Result<Vec<PhaseTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let sizes: Arc<[u64]> = Arc::from(sizes.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, PhaseTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let cache = Arc::clone(&cache);
        let sem = Arc::clone(&sem);
        let sizes = Arc::clone(&sizes);
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
                telemetry.record_operation(PhaseOperationLog {
                    operation_index: work_index as u64,
                    work_index,
                    worker_id: stream_id,
                    target: format!("object:{object_id}"),
                    size_class: class,
                    bytes: sizes[work_index],
                    elapsed,
                });
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
    cache: Arc<BenchmarkCache>,
    sizes: &[u64],
    order: &[usize],
    stream_count: usize,
    io_depth: usize,
    read_size: u64,
    size_class_bounds: SizeClassBounds,
) -> Result<Vec<DirectReadTelemetry>> {
    let segments = split_stream_segments(order, stream_count);
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let sizes: Arc<[u64]> = Arc::from(sizes.to_vec());
    let mut handles: Vec<tokio::task::JoinHandle<Result<(usize, DirectReadTelemetry)>>> =
        Vec::with_capacity(segments.len());

    for (stream_id, segment) in segments.into_iter().enumerate() {
        let cache = Arc::clone(&cache);
        let sem = Arc::clone(&sem);
        let sizes = Arc::clone(&sizes);
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
                telemetry.phase.record_operation(PhaseOperationLog {
                    operation_index: work_index as u64,
                    work_index,
                    worker_id: stream_id,
                    target: format!("object:{object_id}"),
                    size_class: class,
                    bytes: read as u64,
                    elapsed,
                });
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

pub(super) async fn run_direct(
    params: &TestCacheParams,
    cache_dir: &Path,
) -> Result<BenchmarkReport> {
    let cache_config = BenchmarkCacheConfig::new(cache_dir, params.l1_limit);
    let cache = BenchmarkCache::open(cache_config)
        .with_context(|| format!("open cache dir {}", cache_dir.display()))?;

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
        workload_repeat: params.workload_repeat,
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
        local_first_read_comparison: None,
        stream_results,
        mounted_memory: None,
        mounted_portable_profile: None,
        mounted_runtime: None,
        mounted_observability: None,
        mounted_write_path_window: None,
        mounted_writeback_timeline: None,
        mounted_write_diagnostics: None,
        work_root: None,
        writes: phase_telemetry_to_report(&writes, writes_wall),
        reads: phase_telemetry_to_report(&reads.phase, reads_wall),
    })
}

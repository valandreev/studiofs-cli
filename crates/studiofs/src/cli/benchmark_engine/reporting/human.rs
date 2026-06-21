use crate::cli::utils::{format_bytes, format_throughput};
use std::fmt::Write as _;

use super::super::result::{
    BenchmarkReport, MountedObservabilityReport, MountedPortableProfileReport,
    MountedRuntimeReport, MountedWriteDiagnosticsReport, MountedWritebackTimelineReport,
};

fn format_duration_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2} s", ms / 1000.0)
    } else {
        format!("{ms:.2} ms")
    }
}

fn format_optional_bytes(value: Option<u64>) -> String {
    value.map(format_bytes).unwrap_or_else(|| "n/a".to_string())
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

fn render_mounted_runtime(out: &mut String, runtime: &MountedRuntimeReport) {
    out.push('\n');
    let _ = writeln!(out, "Mounted runtime");
    let _ = writeln!(out, "Mounted path:  {}", runtime.mounted_path);
    let _ = writeln!(out, "Cache root:    {}", runtime.cache_root);
    let _ = writeln!(
        out,
        "Metadata root: {}",
        runtime.metadata_root.as_deref().unwrap_or("n/a")
    );
    let _ = writeln!(
        out,
        "Platform:      {} / {}",
        runtime.os, runtime.bridge_type
    );
    let _ = writeln!(
        out,
        "Block/chunk/read: {}/{}/{}",
        format_bytes(runtime.block_size),
        format_bytes(runtime.chunk_size),
        format_bytes(runtime.read_size),
    );
}

fn render_mounted_observability(out: &mut String, observability: &MountedObservabilityReport) {
    out.push('\n');
    let _ = writeln!(out, "Mounted observability");
    if let Some(writer) = &observability.writer {
        let _ = writeln!(
            out,
            "Writer dirty: {}, slices {}, frozen {}, staged {}, limit {}",
            format_bytes(writer.dirty_bytes as u64),
            writer.dirty_slice_count,
            writer.frozen_slice_count,
            format_bytes(writer.staged_bytes as u64),
            format_bytes(writer.dirty_limit_bytes as u64),
        );
    }
    if let Some(cached_store) = &observability.cached_store
        && !cached_store.cache_counters_available
        && let Some(reason) = &cached_store.cache_counters_unavailable_reason
    {
        let _ = writeln!(out, "Cache counters: n/a ({reason})");
    }
    if let Some(upload) = &observability.upload {
        let _ = writeln!(
            out,
            "Upload: backlog {} ({}), active {}, completed {} ({}), throughput {}, retries {}",
            upload.backlog_count,
            format_bytes(upload.backlog_bytes),
            upload.active_uploads,
            upload.completed_uploads,
            format_bytes(upload.completed_upload_bytes),
            format_throughput(upload.throughput_bytes_per_sec),
            upload.retry_count,
        );
        if upload.raw.canceled_stale > 0 {
            let _ = writeln!(
                out,
                "Upload stale: total {}, before PUT {}, after PUT {} ({})",
                upload.raw.canceled_stale,
                upload.raw.canceled_stale_before_put,
                upload.raw.canceled_stale_after_put,
                format_bytes(upload.raw.canceled_stale_after_put_bytes),
            );
        }
        let _ = writeln!(
            out,
            "Upload crypto: encrypted objects {}, encrypted bytes {}, failures {}",
            upload.raw.encrypted_objects_written,
            format_bytes(upload.raw.encrypted_bytes_written),
            upload.raw.encrypted_object_write_failures,
        );
    }
    if let Some(fetch) = &observability.fetch {
        let _ = writeln!(
            out,
            "Fetch: cache hits {}, S3 GETs {}, downloaded {}, singleflight {}, prefetch skips {}",
            fetch.cache_hits,
            fetch.s3_fetches,
            format_bytes(fetch.bytes_downloaded),
            fetch.singleflight_joins,
            fetch.prefetch_skips,
        );
        let _ = writeln!(
            out,
            "Fetch crypto: encrypted objects {}, encrypted bytes {}, failures {}, malformed {}",
            fetch.raw.encrypted_objects_read,
            format_bytes(fetch.raw.encrypted_bytes_read),
            fetch.raw.encrypted_object_read_failures,
            fetch.raw.malformed_encrypted_objects,
        );
        let _ = writeln!(
            out,
            "Fetch stages avg/max: S3 GET {}/{}, decode {}/{}, assemble copy {}/{}, cache put {}/{}, total {}/{}",
            format_duration_ms(fetch.raw.s3_get_avg_ms),
            format_duration_ms(fetch.raw.s3_get_max_ms),
            format_duration_ms(fetch.raw.object_decode_avg_ms),
            format_duration_ms(fetch.raw.object_decode_max_ms),
            format_duration_ms(fetch.raw.assemble_copy_avg_ms),
            format_duration_ms(fetch.raw.assemble_copy_max_ms),
            format_duration_ms(fetch.raw.local_cache_put_avg_ms),
            format_duration_ms(fetch.raw.local_cache_put_max_ms),
            format_duration_ms(fetch.raw.fetch_total_avg_ms),
            format_duration_ms(fetch.raw.fetch_total_max_ms),
        );
    }
    if let Some(barriers) = &observability.barriers {
        let _ = writeln!(
            out,
            "Barriers: flush {}, fsync {}, close {}, cleanup {}, avg {} p95 {}",
            barriers.flush_barrier_calls,
            barriers.fsync_barrier_calls,
            barriers.close_barrier_calls,
            barriers.cleanup_barrier_calls,
            format_duration_ms(barriers.flush_fsync_close_latency.avg_ms),
            format_duration_ms(barriers.flush_fsync_close_latency.latency_quantiles_ms.p95),
        );
    }
    if let Some(idle) = &observability.post_upload_idle {
        let _ = writeln!(
            out,
            "Post-upload idle: observed {}, timed out {}, wait {}, pending {}/{}{}",
            if idle.observed { "yes" } else { "no" },
            if idle.timed_out { "yes" } else { "no" },
            format_duration_ms(idle.wait_ms),
            idle.pending_uploads_before,
            idle.pending_uploads_after,
            idle.reason
                .as_ref()
                .map(|reason| format!(", reason: {reason}"))
                .unwrap_or_default(),
        );
    }
}

fn render_mounted_write_diagnostics(out: &mut String, diagnostics: &MountedWriteDiagnosticsReport) {
    out.push('\n');
    let _ = writeln!(out, "Mounted write diagnostics");
    let _ = writeln!(
        out,
        "Gate: {} (write {:.2} MiB/s, RSS peak {}, amplification {})",
        if diagnostics.pass { "pass" } else { "fail" },
        diagnostics.write_throughput_mib_per_sec,
        format_optional_bytes(diagnostics.rss_bytes_peak),
        diagnostics
            .known_local_write_amplification
            .map(|value| format!("{value:.2}x"))
            .unwrap_or_else(|| "n/a".to_string()),
    );
    let _ = writeln!(
        out,
        "Write path delta: logical {}, WinFSP {}, VFS {}, payload {}, landing {}",
        format_bytes(diagnostics.logical_write_bytes),
        format_bytes(diagnostics.winfsp_write_bytes_delta),
        format_bytes(diagnostics.vfs_write_bytes_delta),
        format_bytes(diagnostics.payload_accounted_bytes_delta),
        format_bytes(diagnostics.landing_accepted_bytes_delta),
    );
    let _ = writeln!(
        out,
        "Accounted after: writer {}, staged {}, pending upload {}, cache pressure {}, total {}",
        format_optional_bytes(diagnostics.writer_stack_dirty_bytes_after),
        format_optional_bytes(diagnostics.pending_persisted_slice_bytes_after),
        format_optional_bytes(diagnostics.pending_upload_bytes_after),
        format_optional_bytes(diagnostics.cache_pressure_bytes_after),
        format_optional_bytes(diagnostics.total_accounted_bytes_after),
    );
    if !diagnostics.blockers.is_empty() {
        let _ = writeln!(out, "Blockers: {}", diagnostics.blockers.join("; "));
    }
}

fn render_mounted_writeback_timeline(out: &mut String, timeline: &MountedWritebackTimelineReport) {
    if timeline.samples.is_empty() {
        return;
    }
    out.push('\n');
    let _ = writeln!(
        out,
        "Mounted writeback timeline ({} ms buckets)",
        timeline.bucket_ms
    );
    let Some(last) = timeline.samples.last() else {
        return;
    };
    let max_backpressure_wait_ms = timeline
        .samples
        .iter()
        .map(|sample| sample.backpressure_wait_ms_delta)
        .max()
        .unwrap_or_default();
    let max_local_cache_io_ms = timeline
        .samples
        .iter()
        .map(|sample| sample.local_cache_backend_io_total_ms_delta)
        .fold(0.0, f64::max);
    let _ = writeln!(
        out,
        "At T+{} ms [{}]: fg {}, payload {}, upload {}, writer {}, staged {}, pending upload {}",
        last.elapsed_ms,
        last.phase,
        format_throughput(last.foreground_write_bytes_per_sec),
        format_throughput(last.payload_persist_bytes_per_sec),
        format_throughput(last.upload_completed_bytes_per_sec),
        format_bytes(last.writer_stack_dirty_bytes),
        format_bytes(last.pending_persisted_slice_bytes),
        format_bytes(last.pending_upload_bytes),
    );
    let _ = writeln!(
        out,
        "Backpressure: last events {} wait {}, worst wait {}; local cache IO last {} calls/{}, worst bucket {}",
        last.backpressure_events_delta,
        format_duration_ms(last.backpressure_wait_ms_delta as f64),
        format_duration_ms(max_backpressure_wait_ms as f64),
        last.local_cache_io_calls_delta,
        format_duration_ms(last.local_cache_backend_io_total_ms_delta),
        format_duration_ms(max_local_cache_io_ms),
    );
}

fn render_mounted_portable_profile(out: &mut String, profile: &MountedPortableProfileReport) {
    out.push('\n');
    let _ = writeln!(out, "Mounted portable profile");
    let _ = writeln!(
        out,
        "Process: samples {}, RSS peak {}, virtual peak {}, CPU avg/peak {}/{}, disk read/write {}/{}",
        profile.process.sample_count,
        format_optional_bytes(profile.process.rss_bytes_peak),
        format_optional_bytes(profile.process.virtual_memory_bytes_peak),
        profile
            .process
            .cpu_usage_percent_avg
            .map(|value| format!("{value:.2}%"))
            .unwrap_or_else(|| "n/a".to_string()),
        profile
            .process
            .cpu_usage_percent_peak
            .map(|value| format!("{value:.2}%"))
            .unwrap_or_else(|| "n/a".to_string()),
        format_optional_bytes(profile.process.disk_read_bytes_delta),
        format_optional_bytes(profile.process.disk_write_bytes_delta),
    );
    if let Some(allocator) = &profile.allocator {
        let _ = writeln!(
            out,
            "Allocator: current {}, peak {}, allocated {}, deallocated {}, alloc/dealloc/realloc {}/{}/{}",
            format_bytes(allocator.current_bytes),
            format_bytes(allocator.peak_bytes),
            format_bytes(allocator.allocated_total_bytes),
            format_bytes(allocator.deallocated_total_bytes),
            allocator.allocation_count,
            allocator.deallocation_count,
            allocator.reallocation_count,
        );
    }
    if !profile.phases.is_empty() {
        let rows: Vec<Vec<String>> = profile
            .phases
            .iter()
            .map(|phase| {
                vec![
                    phase.phase.clone(),
                    phase.process.sample_count.to_string(),
                    format_optional_bytes(phase.process.rss_bytes_peak),
                    phase
                        .process
                        .cpu_usage_percent_avg
                        .map(|value| format!("{value:.2}%"))
                        .unwrap_or_else(|| "n/a".to_string()),
                    phase
                        .allocator
                        .as_ref()
                        .map(|allocator| format_bytes(allocator.peak_bytes))
                        .unwrap_or_else(|| "n/a".to_string()),
                    format_optional_bytes(phase.process.disk_write_bytes_delta),
                ]
            })
            .collect();
        let _ = write!(
            out,
            "{}",
            format_table(
                &[
                    "Phase",
                    "Samples",
                    "RSS peak",
                    "CPU avg",
                    "Heap peak",
                    "Disk write"
                ],
                &rows,
            )
        );
    }
}

pub(crate) fn render_human_summary(report: &BenchmarkReport) -> String {
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
        "Config:    sizes {}, repeat {}, warmups {}, sequential-streams {}",
        report.sizes, report.workload_repeat, report.warmup_runs, report.sequential_streams
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
    let _ = writeln!(&mut out, "Limits:    L1 {}", format_bytes(report.l1_limit));
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
    if let Some(runtime) = &report.mounted_runtime {
        render_mounted_runtime(&mut out, runtime);
    }
    if let Some(observability) = &report.mounted_observability {
        render_mounted_observability(&mut out, observability);
    }
    if let Some(diagnostics) = &report.mounted_write_diagnostics {
        render_mounted_write_diagnostics(&mut out, diagnostics);
    }
    if let Some(timeline) = &report.mounted_writeback_timeline {
        render_mounted_writeback_timeline(&mut out, timeline);
    }
    if let Some(portable_profile) = &report.mounted_portable_profile {
        render_mounted_portable_profile(&mut out, portable_profile);
    }
    out.push('\n');
    let _ = writeln!(&mut out, "Operation results");
    let mut operation_rows = vec![
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
    if let Some(stats) = &report.small_file_stats {
        operation_rows.push(vec![
            "Stat".to_string(),
            stats.operations.to_string(),
            format_bytes(stats.bytes),
            format_throughput(stats.throughput_bytes_per_sec),
            format_duration_ms(stats.avg_latency_ms),
            format_duration_ms(stats.p50_ms),
            format_duration_ms(stats.p95_ms),
            format_duration_ms(stats.p99_ms),
        ]);
    }
    if let Some(cleanup) = &report.cleanup {
        operation_rows.push(vec![
            "Cleanup".to_string(),
            cleanup.operations.to_string(),
            format_bytes(cleanup.bytes),
            format_throughput(cleanup.throughput_bytes_per_sec),
            format_duration_ms(cleanup.avg_latency_ms),
            format_duration_ms(cleanup.p50_ms),
            format_duration_ms(cleanup.p95_ms),
            format_duration_ms(cleanup.p99_ms),
        ]);
    }
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

    if let Some(comparison) = &report.local_first_read_comparison {
        out.push('\n');
        let _ = writeln!(&mut out, "Local-First read comparison");
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
                    if phase.valid_for_target {
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
                    "Valid",
                    "Wait",
                    "Pending before",
                    "Pending after",
                ],
                &rows,
            )
        );
        for phase in comparison
            .phases
            .iter()
            .filter(|phase| !phase.valid_for_target)
        {
            let _ = writeln!(
                &mut out,
                "Phase {} invalid: {}",
                phase.phase,
                phase
                    .validation_message
                    .clone()
                    .unwrap_or_else(|| "target state was not observed".to_string())
            );
        }
    }

    if let Some(memory) = &report.mounted_memory {
        out.push('\n');
        let _ = writeln!(&mut out, "Mounted memory");
        let rows = vec![vec![
            memory.mount_pid.to_string(),
            format_optional_bytes(memory.rss_bytes_steady_state),
            format_optional_bytes(memory.rss_bytes_peak),
            format_optional_bytes(memory.dirty_bytes_steady_state),
            format_optional_bytes(memory.dirty_bytes_peak),
            format_optional_bytes(memory.staged_bytes_peak),
            format_optional_bytes(memory.dirty_limit_bytes),
        ]];
        let _ = write!(
            &mut out,
            "{}",
            format_table(
                &[
                    "PID",
                    "Steady RSS",
                    "Peak RSS",
                    "Steady dirty",
                    "Peak dirty",
                    "Peak staged",
                    "Dirty limit",
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
                        "P99",
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
            "Materialize: {} batches, {} payloads, {}, inflight {} (peak {})",
            pipeline.materialize_batches.batches,
            pipeline.materialize_batches.payloads,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::benchmark_engine::result::{
        LatencyHistogramReport, MountedLocalFirstReadComparisonReport,
        MountedLocalFirstReadPhaseReport, MountedMemoryReport, MountedWritebackTimelineReport,
        MountedWritebackTimelineSampleReport, OpReport, sample_report,
    };

    fn test_latency_histogram() -> LatencyHistogramReport {
        LatencyHistogramReport {
            bucket_upper_bounds_ns: vec![50_000],
            bucket_counts: vec![1, 0],
        }
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
        assert!(summary.contains("Mounted runtime"));
        assert!(summary.contains("Mounted observability"));
        assert!(!summary.contains("\"cache_pipeline\""));
        assert!(!summary.contains("\"mode\""));
    }

    #[test]
    fn render_human_summary_includes_local_first_read_comparison_when_present() {
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.local_first_read_comparison = Some(MountedLocalFirstReadComparisonReport {
            cache_path: r"D:\test-cache\vol-123".to_string(),
            upload_dir: r"D:\test-cache\vol-123\upload".to_string(),
            phases: vec![
                MountedLocalFirstReadPhaseReport {
                    phase: "during-upload-read".to_string(),
                    wait_target: "active".to_string(),
                    target_state_observed: true,
                    valid_for_target: true,
                    required_pending_uploads: 1,
                    wait_ms: 18.0,
                    pending_uploads_before: 3,
                    pending_uploads_after: 2,
                    validation_message: None,
                    results: OpReport {
                        operations: 16,
                        bytes: 128 * 1024 * 1024,
                        elapsed_ms: 81.0,
                        avg_latency_ms: 5.0,
                        p50_ms: 4.8,
                        p95_ms: 8.4,
                        p99_ms: 9.0,
                        throughput_bytes_per_sec: 1_500.0 * 1024.0 * 1024.0,
                        latency_histogram: test_latency_histogram(),
                        operations_log: Vec::new(),
                    },
                },
                MountedLocalFirstReadPhaseReport {
                    phase: "post-upload-read".to_string(),
                    wait_target: "idle".to_string(),
                    target_state_observed: true,
                    valid_for_target: true,
                    required_pending_uploads: 0,
                    wait_ms: 240.0,
                    pending_uploads_before: 0,
                    pending_uploads_after: 0,
                    validation_message: None,
                    results: OpReport {
                        operations: 16,
                        bytes: 128 * 1024 * 1024,
                        elapsed_ms: 74.0,
                        avg_latency_ms: 4.5,
                        p50_ms: 4.2,
                        p95_ms: 7.5,
                        p99_ms: 7.9,
                        throughput_bytes_per_sec: 1_650.0 * 1024.0 * 1024.0,
                        latency_histogram: test_latency_histogram(),
                        operations_log: Vec::new(),
                    },
                },
            ],
        });

        let summary = render_human_summary(&report);

        assert!(summary.contains("Local-First read comparison"));
        assert!(!summary.contains("baseline-read"));
        assert!(summary.contains("during-upload-read"));
        assert!(summary.contains("post-upload-read"));
        assert!(summary.contains("Pending before"));
    }

    #[test]
    fn render_human_summary_includes_mounted_memory_gate_surface_when_present() {
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.mounted_memory = Some(MountedMemoryReport {
            mount_pid: 4242,
            rss_bytes_steady_state: Some(1_400 * 1024 * 1024),
            rss_bytes_peak: Some(1_650 * 1024 * 1024),
            dirty_bytes_steady_state: Some(512 * 1024 * 1024),
            dirty_bytes_peak: Some(768 * 1024 * 1024),
            staged_bytes_peak: Some(256 * 1024 * 1024),
            dirty_limit_bytes: Some(2 * 1024 * 1024 * 1024),
        });

        let summary = render_human_summary(&report);

        assert!(summary.contains("Mounted memory"));
        assert!(summary.contains("PID"));
        assert!(summary.contains("Steady RSS"));
        assert!(summary.contains("Peak RSS"));
        assert!(summary.contains("Steady dirty"));
        assert!(summary.contains("Peak staged"));
        assert!(summary.contains("Dirty limit"));
    }

    #[test]
    fn render_human_summary_includes_stat_and_cleanup_rows_when_present() {
        let mut report = sample_report();
        report.small_file_stats = Some(OpReport {
            operations: 100,
            bytes: 0,
            elapsed_ms: 25.0,
            avg_latency_ms: 0.25,
            p50_ms: 0.2,
            p95_ms: 0.4,
            p99_ms: 0.5,
            throughput_bytes_per_sec: 0.0,
            latency_histogram: test_latency_histogram(),
            operations_log: Vec::new(),
        });
        report.cleanup = Some(OpReport {
            operations: 1,
            bytes: 0,
            elapsed_ms: 10.0,
            avg_latency_ms: 10.0,
            p50_ms: 10.0,
            p95_ms: 10.0,
            p99_ms: 10.0,
            throughput_bytes_per_sec: 0.0,
            latency_histogram: test_latency_histogram(),
            operations_log: Vec::new(),
        });

        let summary = render_human_summary(&report);

        assert!(summary.contains("| Stat"));
        assert!(summary.contains("| Cleanup"));
    }

    #[test]
    fn render_human_summary_includes_writeback_timeline_when_present() {
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.mounted_writeback_timeline = Some(MountedWritebackTimelineReport {
            bucket_ms: 1000,
            samples: vec![MountedWritebackTimelineSampleReport {
                elapsed_ms: 1000,
                phase: "write".to_string(),
                foreground_write_bytes_per_sec: 1024.0 * 1024.0 * 1024.0,
                payload_persist_bytes_per_sec: 768.0 * 1024.0 * 1024.0,
                upload_completed_bytes_per_sec: 256.0 * 1024.0 * 1024.0,
                writer_stack_dirty_bytes: 64 * 1024 * 1024,
                pending_persisted_slice_bytes: 128 * 1024 * 1024,
                pending_upload_bytes: 256 * 1024 * 1024,
                upload_queue_depth: 4,
                active_uploads: 2,
                backpressure_events_delta: 3,
                backpressure_wait_ms_delta: 25,
                soft_throttle_events_delta: 2,
                hard_backpressure_events_delta: 1,
                materialization_calls_delta: 8,
                materialization_total_ms_delta: 44.0,
                local_cache_io_calls_delta: 8,
                local_cache_backend_io_total_ms_delta: 40.0,
                local_cache_backend_io_cumulative_max_ms: 12.0,
            }],
        });

        let summary = render_human_summary(&report);

        assert!(summary.contains("Mounted writeback timeline"));
        assert!(summary.contains("At T+1000 ms"));
        assert!(!summary.contains("Last 1000 ms"));
        assert!(summary.contains("Backpressure:"));
        assert!(summary.contains("local cache IO"));
    }

    #[test]
    fn render_human_summary_marks_unavailable_mounted_memory_samples() {
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.mounted_memory = Some(MountedMemoryReport {
            mount_pid: 4242,
            rss_bytes_steady_state: Some(1_400 * 1024 * 1024),
            rss_bytes_peak: Some(1_650 * 1024 * 1024),
            dirty_bytes_steady_state: None,
            dirty_bytes_peak: None,
            staged_bytes_peak: None,
            dirty_limit_bytes: None,
        });

        let summary = render_human_summary(&report);

        assert!(summary.contains("Steady dirty"));
        assert!(summary.contains("n/a"));
    }
}

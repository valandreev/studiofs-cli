use crate::cli::utils::{format_bytes, format_throughput};
use std::fmt::Write as _;

use super::super::result::BenchmarkReport;

fn format_duration_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2} s", ms / 1000.0)
    } else {
        format!("{ms:.2} ms")
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
                    "Pending after",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::test_cache::result::{
        MountedLocalFirstReadComparisonReport, MountedLocalFirstReadPhaseReport, OpReport,
        sample_report,
    };

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
    fn render_human_summary_includes_local_first_read_comparison_when_present() {
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.local_first_read_comparison = Some(MountedLocalFirstReadComparisonReport {
            cache_path: r"D:\test-cache\vol-123".to_string(),
            upload_dir: r"D:\test-cache\vol-123\upload".to_string(),
            phases: vec![
                MountedLocalFirstReadPhaseReport {
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
                MountedLocalFirstReadPhaseReport {
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
                MountedLocalFirstReadPhaseReport {
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

        assert!(summary.contains("Local-First read comparison"));
        assert!(summary.contains("baseline-read"));
        assert!(summary.contains("during-upload-read"));
        assert!(summary.contains("post-upload-read"));
        assert!(summary.contains("Pending before"));
    }
}

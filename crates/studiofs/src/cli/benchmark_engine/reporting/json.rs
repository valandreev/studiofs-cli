use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::fs::File;
use std::io::ErrorKind;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::NamedTempFile;

use super::super::result::BenchmarkReport;

const WINDOWS_TEMPFILE_PERSIST_RETRIES: usize = 20;
const WINDOWS_TEMPFILE_PERSIST_RETRY_DELAY: Duration = Duration::from_millis(10);

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
        "studiofs-benchmark-{}-{}.json",
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
        .unwrap_or("studiofs-benchmark");
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
        let output_text = json_output.to_string_lossy();
        let explicit = if json_output.is_absolute() {
            json_output.to_path_buf()
        } else {
            current_dir.join(json_output)
        };
        let default_report_dir = current_dir.join("target").join("perf");
        let treat_as_dir = explicit == default_report_dir
            || explicit.is_dir()
            || output_text.ends_with('/')
            || output_text.ends_with('\\');
        if treat_as_dir {
            return (explicit.join(default_report_file_name(report, now)), true);
        }
        return (explicit, false);
    }

    (
        current_dir
            .join("target")
            .join("perf")
            .join(default_report_file_name(report, now)),
        true,
    )
}

fn write_json_report(
    temp_file: &mut NamedTempFile,
    report: &BenchmarkReport,
    report_path: &Path,
) -> Result<()> {
    let mut writer = BufWriter::new(temp_file.as_file_mut());
    serde_json::to_writer_pretty(&mut writer, report)
        .with_context(|| format!("serialize benchmark report {}", report_path.display()))?;
    writer
        .flush()
        .with_context(|| format!("flush benchmark report {}", report_path.display()))
}

fn write_report_file(
    report_path: &Path,
    report: &BenchmarkReport,
    auto_named: bool,
) -> Result<PathBuf> {
    let parent = report_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create report dir {}", parent.display()))?;

    if !auto_named {
        let mut temp_file = NamedTempFile::new_in(parent)
            .with_context(|| format!("create temporary report file in {}", parent.display()))?;
        write_json_report(&mut temp_file, report, report_path)?;
        persist_named_temp_file(temp_file, report_path, "replace benchmark report")?;
        return Ok(report_path.to_path_buf());
    }

    let mut candidate = report_path.to_path_buf();
    let mut suffix = 2usize;
    while candidate.exists() {
        candidate = next_available_path(report_path, suffix);
        suffix = suffix.saturating_add(1);
    }
    loop {
        let mut temp_file = NamedTempFile::new_in(parent)
            .with_context(|| format!("create temporary report file in {}", parent.display()))?;
        write_json_report(&mut temp_file, report, &candidate)?;
        match persist_named_temp_file_noclobber(temp_file, &candidate)? {
            Some(_) => return Ok(candidate),
            None => {
                candidate = next_available_path(report_path, suffix);
                suffix = suffix.saturating_add(1);
            }
        }
    }
}

fn persist_named_temp_file(
    mut temp_file: NamedTempFile,
    path: &Path,
    context: &'static str,
) -> Result<File> {
    let mut retries = 0;
    loop {
        match temp_file.persist(path) {
            Ok(file) => return Ok(file),
            Err(error) => {
                if should_retry_tempfile_persist(&error.error, retries) {
                    temp_file = error.file;
                    retries += 1;
                    std::thread::sleep(WINDOWS_TEMPFILE_PERSIST_RETRY_DELAY);
                    continue;
                }

                return Err(error.error).with_context(|| format!("{context} {}", path.display()));
            }
        }
    }
}

fn persist_named_temp_file_noclobber(
    mut temp_file: NamedTempFile,
    path: &Path,
) -> Result<Option<File>> {
    let mut retries = 0;
    loop {
        match temp_file.persist_noclobber(path) {
            Ok(file) => return Ok(Some(file)),
            Err(error) if error.error.kind() == ErrorKind::AlreadyExists => {
                let _ = error.file.close();
                return Ok(None);
            }
            Err(error) => {
                if should_retry_tempfile_persist(&error.error, retries) {
                    temp_file = error.file;
                    retries += 1;
                    std::thread::sleep(WINDOWS_TEMPFILE_PERSIST_RETRY_DELAY);
                    continue;
                }

                return Err(error.error)
                    .with_context(|| format!("create benchmark report {}", path.display()));
            }
        }
    }
}

fn should_retry_tempfile_persist(error: &std::io::Error, retries: usize) -> bool {
    cfg!(windows)
        && retries < WINDOWS_TEMPFILE_PERSIST_RETRIES
        && error.kind() == ErrorKind::PermissionDenied
}

fn save_benchmark_report_in(
    report: &BenchmarkReport,
    json_output: Option<&Path>,
    current_dir: &Path,
    now: DateTime<Utc>,
) -> Result<PathBuf> {
    let (report_path, auto_named) = resolve_report_target(report, json_output, current_dir, now);
    write_report_file(&report_path, report, auto_named)
}

pub(crate) fn save_benchmark_report(
    report: &BenchmarkReport,
    json_output: Option<&Path>,
) -> Result<PathBuf> {
    let current_dir = std::env::current_dir().context("resolve current working directory")?;
    save_benchmark_report_in(report, json_output, &current_dir, Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::benchmark_engine::result::{
        LatencyHistogramReport, MountedMemoryReport, MountedPortableAllocatorReport,
        MountedPortablePhaseProfileReport, MountedPortableProcessReport,
        MountedPortableProfileReport, MountedWriteDiagnosticsReport,
        MountedWritebackTimelineReport, MountedWritebackTimelineSampleReport, OpReport,
        PublicBenchDuringUploadReadReport, PublicBenchReport, PublicBenchUploadDrainReport,
        PublicBenchWorkloadReport, PublicBenchWriteMetricsReport, SizeClassOpReport, sample_report,
    };
    use chrono::TimeZone;

    #[test]
    fn default_report_path_uses_target_perf_and_avoids_collisions() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let first = save_benchmark_report_in(&report, None, temp.path(), now).unwrap();
        assert_eq!(
            first,
            temp.path()
                .join("target")
                .join("perf")
                .join("studiofs-benchmark-direct-shim-20260319-151255.json")
        );

        let second = save_benchmark_report_in(&report, None, temp.path(), now).unwrap();
        assert_eq!(
            second,
            temp.path()
                .join("target")
                .join("perf")
                .join("studiofs-benchmark-direct-shim-20260319-151255-2.json")
        );
    }

    #[test]
    fn save_benchmark_report_overwrites_requested_file_path() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();
        let existing = temp.path().join("reports/cache-run.json");
        std::fs::create_dir_all(existing.parent().unwrap()).unwrap();
        std::fs::write(&existing, "stale report").unwrap();

        let saved = save_benchmark_report_in(
            &report,
            Some(Path::new("reports/cache-run.json")),
            temp.path(),
            now,
        )
        .unwrap();

        assert_eq!(saved, existing);
        let content = std::fs::read_to_string(saved).unwrap();
        assert!(content.contains("\n  \"mode\""));
        assert!(!content.contains("stale report"));
    }

    #[test]
    fn save_benchmark_report_writes_pretty_json_to_requested_path() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        assert_eq!(parsed["command"]["name"], "studiofs test-cache");
        assert!(parsed["platform"]["os"].is_string());
        assert!(parsed["build"]["package_version"].is_string());
        assert_eq!(parsed["runtime_stats"]["available"], false);
        assert!(parsed["runtime_stats"]["warnings"][0].is_string());
        assert!(content.contains("\n  \"mode\""));
    }

    #[test]
    fn save_benchmark_report_writes_bare_requested_file_path() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("report.json")), temp.path(), now)
                .unwrap();

        assert_eq!(saved, temp.path().join("report.json"));
        assert!(saved.is_file());
    }

    #[cfg(windows)]
    #[test]
    fn save_benchmark_report_retries_windows_overwrite_access_denied() {
        use std::fs::OpenOptions;
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_SHARE_READ: u32 = 0x0000_0001;
        const FILE_SHARE_WRITE: u32 = 0x0000_0002;

        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();
        let path = temp.path().join("report.json");
        std::fs::write(&path, "stale report").unwrap();

        let blocking_handle = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .open(&path)
            .expect("open destination without delete sharing");
        let release_handle = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(blocking_handle);
        });

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("report.json")), temp.path(), now)
                .expect("save report after transient destination access denied");
        release_handle.join().expect("release blocking handle");

        assert_eq!(saved, path);
        let content = std::fs::read_to_string(saved).unwrap();
        assert!(content.contains("\n  \"mode\""));
        assert!(!content.contains("stale report"));
    }

    #[test]
    fn save_benchmark_report_writes_extensionless_requested_path_as_file() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved = save_benchmark_report_in(
            &report,
            Some(Path::new("target/new_reports_dir")),
            temp.path(),
            now,
        )
        .unwrap();

        assert_eq!(saved, temp.path().join("target").join("new_reports_dir"));
        assert!(saved.is_file());
    }

    #[test]
    fn save_benchmark_report_treats_default_perf_output_as_directory() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("target/perf")), temp.path(), now)
                .unwrap();

        assert_eq!(
            saved,
            temp.path()
                .join("target")
                .join("perf")
                .join("studiofs-benchmark-direct-shim-20260319-151255.json")
        );
        assert!(saved.is_file());
        assert!(temp.path().join("target").join("perf").is_dir());
    }

    #[test]
    fn save_benchmark_report_includes_sfs299_mounted_observability_surface() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("cache-run.json")), temp.path(), now)
                .unwrap();

        let content = std::fs::read_to_string(saved).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert!(parsed["writes"]["latency_histogram"].is_object());
        assert!(parsed["reads"]["latency_histogram"].is_object());
        assert!(parsed["mounted_runtime"]["mounted_path"].is_string());
        assert!(parsed["mounted_runtime"]["cache_root"].is_string());
        assert!(parsed["mounted_runtime"]["metadata_root"].is_string());
        assert!(parsed["mounted_runtime"]["os"].is_string());
        assert!(parsed["mounted_runtime"]["bridge_type"].is_string());
        assert!(parsed["mounted_runtime"]["workload_shape"].is_object());
        assert_eq!(
            parsed["mounted_runtime"]["chunk_size"],
            studiofs_meta::schema::DEFAULT_CHUNK_SIZE
        );

        let observability = &parsed["mounted_observability"];
        assert!(observability["writer"]["dirty_bytes"].is_number());
        assert!(observability["writer"]["dirty_slice_count"].is_number());
        assert!(observability["writer"]["frozen_slice_count"].is_number());
        assert!(observability["writer"]["staged_bytes"].is_number());
        assert!(observability["writer"]["dirty_limit_bytes"].is_number());
        assert!(observability["upload"]["backlog_count"].is_number());
        assert!(observability["upload"]["backlog_bytes"].is_number());
        assert!(observability["upload"]["throughput_bytes_per_sec"].is_number());
        assert!(observability["upload"]["retry_count"].is_number());
        assert!(observability["fetch"]["cache_hits"].is_number());
        assert!(observability["fetch"]["s3_fetches"].is_number());
        assert!(observability["fetch"]["bytes_downloaded"].is_number());
        assert!(observability["post_upload_idle"]["observed"].is_boolean());
        assert!(observability["post_upload_idle"]["timed_out"].is_boolean());
        assert!(observability["barriers"]["flush_fsync_close_latency"].is_object());
    }

    #[test]
    fn save_benchmark_report_includes_mounted_write_diagnostics_surface() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.mounted_write_diagnostics = Some(MountedWriteDiagnosticsReport {
            pass: true,
            blockers: Vec::new(),
            write_throughput_target_bytes_per_sec: 1_000.0 * 1024.0 * 1024.0,
            write_throughput_bytes_per_sec: 1_200.0 * 1024.0 * 1024.0,
            write_throughput_mib_per_sec: 1_200.0,
            write_throughput_target_met: true,
            rss_peak_target_bytes: 2 * 1024 * 1024 * 1024,
            rss_bytes_peak: Some(1024 * 1024 * 1024),
            rss_target_met: Some(true),
            logical_write_bytes: 1024,
            winfsp_write_bytes_delta: 1024,
            vfs_write_bytes_delta: 1024,
            payload_persisted_bytes_delta: 1024,
            payload_accounted_bytes_delta: 1024,
            landing_accepted_bytes_delta: 0,
            known_local_write_amplification: Some(1.0),
            landing_journal_active: false,
            dirty_bytes_peak: Some(256),
            staged_bytes_peak: Some(512),
            dirty_limit_bytes: Some(2 * 1024 * 1024 * 1024),
            writer_stack_dirty_bytes_after: Some(64),
            pending_persisted_slice_bytes_after: Some(128),
            pending_upload_bytes_after: Some(256),
            cache_pressure_bytes_after: Some(384),
            total_accounted_bytes_after: Some(512),
            payload_persist_calls_delta: 8,
            local_cache_io_calls_delta: 10,
            local_cache_backend_io_total_ms_delta: 40.0,
            hard_backpressure_events_delta: 2,
            soft_throttle_events_delta: 5,
        });
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("cache-run.json")), temp.path(), now)
                .unwrap();

        let content = std::fs::read_to_string(saved).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        let diagnostics = &parsed["mounted_write_diagnostics"];
        assert_eq!(diagnostics["pass"], true);
        assert_eq!(diagnostics["write_throughput_target_met"], true);
        assert_eq!(diagnostics["rss_target_met"], true);
        assert_eq!(diagnostics["known_local_write_amplification"], 1.0);
        assert_eq!(diagnostics["payload_persisted_bytes_delta"], 1024);
        assert_eq!(diagnostics["landing_accepted_bytes_delta"], 0);
        assert_eq!(diagnostics["total_accounted_bytes_after"], 512);
    }

    #[test]
    fn save_benchmark_report_includes_public_bench_write_and_upload_drain_surface() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.command.name = "studiofs bench".to_string();
        report.public_bench = Some(PublicBenchReport {
            workload: PublicBenchWorkloadReport {
                small_files: 1000,
                medium_files: 80,
                large_files: 1,
                large_file_size_bytes: 4 * 1024 * 1024 * 1024 + 37 * 1024 * 1024,
            },
            write_metrics: PublicBenchWriteMetricsReport {
                small_files: SizeClassOpReport {
                    class: "small".to_string(),
                    operations: 1000,
                    bytes: 140 * 1024 * 1024,
                    avg_latency_ms: 0.8,
                    p50_ms: 0.7,
                    p95_ms: 1.1,
                    p99_ms: 1.3,
                    throughput_bytes_per_sec: 200.0 * 1024.0 * 1024.0,
                },
                medium_files: SizeClassOpReport {
                    class: "medium".to_string(),
                    operations: 80,
                    bytes: 512 * 1024 * 1024,
                    avg_latency_ms: 4.0,
                    p50_ms: 3.8,
                    p95_ms: 6.2,
                    p99_ms: 6.8,
                    throughput_bytes_per_sec: 512.0 * 1024.0 * 1024.0,
                },
                large_file: SizeClassOpReport {
                    class: "large".to_string(),
                    operations: 1,
                    bytes: 4 * 1024 * 1024 * 1024 + 37 * 1024 * 1024,
                    avg_latency_ms: 8000.0,
                    p50_ms: 8000.0,
                    p95_ms: 8000.0,
                    p99_ms: 8000.0,
                    throughput_bytes_per_sec: 768.0 * 1024.0 * 1024.0,
                },
            },
            during_upload_read: PublicBenchDuringUploadReadReport {
                phase: "during-upload-read".to_string(),
                metrics: OpReport {
                    operations: 1,
                    bytes: 64 * 1024 * 1024,
                    elapsed_ms: 120.0,
                    avg_latency_ms: 120.0,
                    p50_ms: 120.0,
                    p95_ms: 120.0,
                    p99_ms: 120.0,
                    throughput_bytes_per_sec: 533.0 * 1024.0 * 1024.0,
                    latency_histogram: LatencyHistogramReport {
                        bucket_upper_bounds_ns: vec![50_000],
                        bucket_counts: vec![0, 1],
                    },
                    operations_log: Vec::new(),
                },
                wait_target: "active".to_string(),
                target_state_observed: true,
                valid_for_target: true,
                wait_ms: 20.0,
                pending_uploads_before: 2,
                pending_uploads_after: 1,
                validation_message: None,
            },
            upload_drain: PublicBenchUploadDrainReport {
                observed_idle: true,
                timed_out: false,
                wait_ms: 240.0,
                pending_uploads_before: 1,
                pending_uploads_after: 0,
                reason: None,
            },
        });
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("bench-run.json")), temp.path(), now)
                .unwrap();

        let content = std::fs::read_to_string(saved).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        let public_bench = &parsed["public_bench"];
        assert_eq!(public_bench["workload"]["small_files"], 1000);
        assert_eq!(public_bench["workload"]["medium_files"], 80);
        assert_eq!(public_bench["workload"]["large_files"], 1);
        assert!(
            public_bench["workload"]["large_file_size_bytes"]
                .as_u64()
                .unwrap()
                >= 4 * 1024 * 1024 * 1024
        );
        assert_eq!(
            public_bench["write_metrics"]["small_files"]["class"],
            "small"
        );
        assert_eq!(
            public_bench["write_metrics"]["medium_files"]["class"],
            "medium"
        );
        assert_eq!(
            public_bench["write_metrics"]["large_file"]["class"],
            "large"
        );
        assert!(
            public_bench["write_metrics"]["large_file"]["bytes"]
                .as_u64()
                .unwrap()
                >= 4 * 1024 * 1024 * 1024
        );
        assert_eq!(
            public_bench["during_upload_read"]["phase"],
            "during-upload-read"
        );
        assert_eq!(public_bench["during_upload_read"]["wait_target"], "active");
        assert_eq!(
            public_bench["during_upload_read"]["target_state_observed"],
            true
        );
        assert_eq!(public_bench["upload_drain"]["observed_idle"], true);
        assert_eq!(public_bench["upload_drain"]["pending_uploads_after"], 0);
    }

    #[test]
    fn save_benchmark_report_includes_sfs306_writeback_timeline_surface() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.mounted_writeback_timeline = Some(MountedWritebackTimelineReport {
            bucket_ms: 1000,
            samples: vec![MountedWritebackTimelineSampleReport {
                elapsed_ms: 1000,
                phase: "write".to_string(),
                foreground_write_bytes_per_sec: 1_100.0 * 1024.0 * 1024.0,
                payload_persist_bytes_per_sec: 900.0 * 1024.0 * 1024.0,
                upload_completed_bytes_per_sec: 300.0 * 1024.0 * 1024.0,
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
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("cache-run.json")), temp.path(), now)
                .unwrap();

        let content = std::fs::read_to_string(saved).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        let timeline = &parsed["mounted_writeback_timeline"];
        assert_eq!(timeline["bucket_ms"], 1000);
        assert_eq!(timeline["samples"][0]["phase"], "write");
        assert_eq!(
            timeline["samples"][0]["pending_upload_bytes"],
            256 * 1024 * 1024
        );
        assert_eq!(timeline["samples"][0]["backpressure_events_delta"], 3);
        assert_eq!(
            timeline["samples"][0]["local_cache_backend_io_total_ms_delta"],
            40.0
        );
        assert_eq!(
            timeline["samples"][0]["local_cache_backend_io_cumulative_max_ms"],
            12.0
        );
        assert!(
            timeline["samples"][0]["local_cache_backend_io_max_ms"].is_null(),
            "bucket-local max should not be serialized from the cumulative VFS max"
        );
    }

    #[test]
    fn save_benchmark_report_includes_portable_profile_surface() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let mut report = sample_report();
        report.mode = "e2e".to_string();
        report.mounted_portable_profile = Some(MountedPortableProfileReport {
            process: MountedPortableProcessReport {
                mount_pid: 42,
                sample_count: 3,
                rss_bytes_steady_state: Some(256),
                rss_bytes_peak: Some(512),
                virtual_memory_bytes_peak: Some(1024),
                cpu_usage_percent_avg: Some(12.5),
                cpu_usage_percent_peak: Some(25.0),
                cpu_time_ms_delta: Some(140),
                disk_read_bytes_delta: Some(2048),
                disk_write_bytes_delta: Some(4096),
            },
            allocator: Some(MountedPortableAllocatorReport {
                current_bytes: 128,
                peak_bytes: 512,
                allocated_total_bytes: 2048,
                deallocated_total_bytes: 1920,
                allocation_count: 7,
                deallocation_count: 3,
                reallocation_count: 1,
            }),
            phases: vec![MountedPortablePhaseProfileReport {
                phase: "write".to_string(),
                process: MountedPortableProcessReport {
                    mount_pid: 42,
                    sample_count: 2,
                    rss_bytes_steady_state: Some(256),
                    rss_bytes_peak: Some(512),
                    virtual_memory_bytes_peak: Some(1024),
                    cpu_usage_percent_avg: Some(10.0),
                    cpu_usage_percent_peak: Some(20.0),
                    cpu_time_ms_delta: Some(100),
                    disk_read_bytes_delta: Some(0),
                    disk_write_bytes_delta: Some(4096),
                },
                allocator: Some(MountedPortableAllocatorReport {
                    current_bytes: 128,
                    peak_bytes: 512,
                    allocated_total_bytes: 2048,
                    deallocated_total_bytes: 1920,
                    allocation_count: 7,
                    deallocation_count: 3,
                    reallocation_count: 1,
                }),
            }],
        });
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("cache-run.json")), temp.path(), now)
                .unwrap();

        let content = std::fs::read_to_string(saved).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        let portable = &parsed["mounted_portable_profile"];
        assert_eq!(portable["process"]["mount_pid"], 42);
        assert_eq!(portable["process"]["rss_bytes_peak"], 512);
        assert_eq!(portable["process"]["disk_write_bytes_delta"], 4096);
        assert_eq!(portable["allocator"]["peak_bytes"], 512);
        assert_eq!(portable["phases"][0]["phase"], "write");
        assert_eq!(portable["phases"][0]["allocator"]["peak_bytes"], 512);
    }

    #[test]
    fn save_benchmark_report_omits_unavailable_dirty_pressure_fields() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved =
            save_benchmark_report_in(&report, Some(Path::new("cache-run.json")), temp.path(), now)
                .unwrap();

        let content = std::fs::read_to_string(saved).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["mounted_memory"]["mount_pid"], 4242);
        assert!(
            parsed["mounted_memory"]
                .get("rss_bytes_steady_state")
                .is_some()
        );
        assert!(
            parsed["mounted_memory"]
                .get("dirty_bytes_steady_state")
                .is_none()
        );
        assert!(parsed["mounted_memory"].get("dirty_bytes_peak").is_none());
        assert!(parsed["mounted_memory"].get("dirty_limit_bytes").is_none());
    }
}

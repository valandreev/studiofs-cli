use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use super::super::result::BenchmarkReport;

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
    use crate::cli::test_cache::result::{MountedMemoryReport, sample_report};
    use chrono::TimeZone;
    use tempfile::tempdir;

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
    fn save_benchmark_report_omits_unavailable_dirty_pressure_fields() {
        let temp = tempdir().unwrap();
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

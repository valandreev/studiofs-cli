use crate::cli::test_cache_profile_types::ProfileArtifactKind;
use crate::cli::test_cache_profile_types::ProfileArtifactReport;
use crate::cli::utils::format_bytes;
use anyhow::{Context, Result};
use std::convert::TryFrom;
use std::fs;
use std::path::Path;
use studiofs_vfs::VfsWritePathObservabilitySnapshot;

const WINFSP_CLEANUP_REGRESSION_ISSUE: &str = "SFS-192";

pub(crate) fn collect_write_path_artifacts(
    output_dir: &Path,
    run_id: &str,
    benchmark_mode: &str,
    write_path_observability_path: Option<&Path>,
    warnings: &mut Vec<String>,
) -> Result<Vec<ProfileArtifactReport>> {
    let mut artifacts = Vec::new();
    let Some(metrics_path) = write_path_observability_path else {
        return Ok(artifacts);
    };

    match fs::read(metrics_path) {
        Ok(bytes) => {
            let artifact = write_artifact(
                output_dir,
                run_id,
                benchmark_mode,
                "write-path-observability",
                "json",
                &bytes,
            )?;
            artifacts.push(ProfileArtifactReport {
                kind: ProfileArtifactKind::WritePathObservability,
                path: artifact.display().to_string(),
                bytes: u64::try_from(bytes.len())
                    .context("write-path observability artifact is too large")?,
                sent_to_endpoint: false,
            });
            match render_write_path_summary(&bytes) {
                Ok(summary) => {
                    let summary_bytes = summary.into_bytes();
                    let summary_artifact = write_artifact(
                        output_dir,
                        run_id,
                        benchmark_mode,
                        "write-path-summary",
                        "txt",
                        &summary_bytes,
                    )?;
                    artifacts.push(ProfileArtifactReport {
                        kind: ProfileArtifactKind::WritePathSummary,
                        path: summary_artifact.display().to_string(),
                        bytes: u64::try_from(summary_bytes.len())
                            .context("write-path summary artifact is too large")?,
                        sent_to_endpoint: false,
                    });
                }
                Err(err) => warnings.push(format!(
                    "Failed to render write-path summary from '{}': {err}",
                    metrics_path.display()
                )),
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            warnings.push(format!(
                "Write-path observability snapshot not found: {}",
                metrics_path.display()
            ));
        }
        Err(err) => warnings.push(format!(
            "Failed to read write-path observability snapshot '{}': {err}",
            metrics_path.display()
        )),
    }

    Ok(artifacts)
}

fn write_artifact(
    output_dir: &Path,
    run_id: &str,
    benchmark_mode: &str,
    kind: &str,
    suffix: &str,
    bytes: &[u8],
) -> Result<std::path::PathBuf> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("create profile output directory {}", output_dir.display()))?;
    let filename = format!("studiofs-test-cache-{benchmark_mode}-{run_id}-{kind}.{suffix}");
    let path = output_dir.join(filename);
    fs::write(&path, bytes)
        .with_context(|| format!("write profile artifact {}", path.display()))?;
    Ok(path)
}

fn has_cleanup_barrier_attribution(
    stats: &studiofs_vfs::VfsWritePathWinfspForegroundFlushStats,
) -> bool {
    stats.cleanup > 0 || stats.cleanup_skipped_clean > 0
}

fn winfsp_cleanup_validation_state(snapshot: &VfsWritePathObservabilitySnapshot) -> &'static str {
    if has_cleanup_barrier_attribution(&snapshot.winfsp_foreground_flushes)
        || snapshot.winfsp_callbacks.cleanup_flush_fh > 0
    {
        "cleanup-follow-up"
    } else if snapshot.winfsp_callbacks.cleanup > 0 {
        "cleanup-deferred"
    } else {
        "not-observed"
    }
}

pub(crate) fn render_write_path_summary(bytes: &[u8]) -> Result<String> {
    let snapshot: VfsWritePathObservabilitySnapshot =
        serde_json::from_slice(bytes).context("parse write-path observability snapshot")?;
    let mut out = String::new();
    out.push_str(&format!("backend: {}\n", snapshot.backend_tag));
    out.push_str(&format!(
        "eager_flush_threshold: {}\n",
        format_bytes(
            u64::try_from(snapshot.tuning.eager_flush_threshold_bytes)
                .context("eager flush threshold does not fit in u64")
                .unwrap_or(u64::MAX)
        )
    ));
    out.push_str(&format!(
        "eager_flush_drain_chunk: {}\n",
        format_bytes(
            u64::try_from(snapshot.tuning.eager_flush_drain_chunk_bytes)
                .context("eager flush drain chunk does not fit in u64")
                .unwrap_or(u64::MAX)
        )
    ));
    out.push_str(&format!(
        "payload_put_concurrency: {}\n",
        snapshot.tuning.payload_put_concurrency
    ));
    out.push_str(&format!(
        "sync_group_min: {}\n",
        format_bytes(snapshot.tuning.sync_group_min_bytes)
    ));
    out.push_str(&format!(
        "sync_group_max_delay_ms: {}\n",
        snapshot.tuning.sync_group_max_delay_ms
    ));
    out.push_str(&format!(
        "payload_burst_avg: {}\n",
        format_bytes(snapshot.payload_burst.avg_bytes_per_burst.round() as u64)
    ));
    out.push_str(&format!(
        "payload_burst_max: {}\n",
        format_bytes(snapshot.payload_burst.max_bytes_per_burst)
    ));
    out.push_str(&format!(
        "payload_persist_p95_ms: {:.2}\n",
        snapshot.payload_persist.latency_quantiles_ms.p95
    ));
    out.push_str(&format!(
        "metadata_commit_p95_ms: {:.2}\n",
        snapshot.metadata_commit.latency_quantiles_ms.p95
    ));
    out.push_str(&format!(
        "metadata_commit_interval_avg_ms: {:.2}\n",
        snapshot.metadata_commit_cadence.avg_interval_ms
    ));
    out.push_str(&format!(
        "metadata_commit_interval_max_ms: {:.2}\n",
        snapshot.metadata_commit_cadence.max_interval_ms
    ));
    out.push_str(&format!(
        "sync_data_calls_per_gib: {:.2}\n",
        snapshot.block_store_sync_data.calls_per_gib
    ));
    out.push_str(&format!(
        "winfsp_callbacks: write={} overwrite={} cleanup={} cleanup_flush_fh={} flush={} close={}\n",
        snapshot.winfsp_callbacks.write,
        snapshot.winfsp_callbacks.overwrite,
        snapshot.winfsp_callbacks.cleanup,
        snapshot.winfsp_callbacks.cleanup_flush_fh,
        snapshot.winfsp_callbacks.flush,
        snapshot.winfsp_callbacks.close
    ));
    out.push_str(&format!(
        "winfsp_foreground_flushes: cleanup={} cleanup_skipped_clean={} flush={} flush_skipped_clean={} close={} close_skipped_clean={}\n",
        snapshot.winfsp_foreground_flushes.cleanup,
        snapshot.winfsp_foreground_flushes.cleanup_skipped_clean,
        snapshot.winfsp_foreground_flushes.flush,
        snapshot.winfsp_foreground_flushes.flush_skipped_clean,
        snapshot.winfsp_foreground_flushes.close,
        snapshot.winfsp_foreground_flushes.close_skipped_clean
    ));
    out.push_str(&format!(
        "winfsp_validation: cleanup_non_delete={} cleanup_non_delete_barriers={} cleanup_non_delete_skipped_clean={} cleanup_flush_fh={} flush_barriers={} close_barriers={}\n",
        winfsp_cleanup_validation_state(&snapshot),
        snapshot.winfsp_foreground_flushes.cleanup,
        snapshot.winfsp_foreground_flushes.cleanup_skipped_clean,
        snapshot.winfsp_callbacks.cleanup_flush_fh,
        snapshot.winfsp_foreground_flushes.flush,
        snapshot.winfsp_foreground_flushes.close,
    ));
    if snapshot.winfsp_foreground_flushes.cleanup > 0 {
        out.push_str(&format!(
            "winfsp_follow_up: non-delete cleanup regression attributed {} foreground barrier(s); record follow-up evidence for {}\n",
            snapshot.winfsp_foreground_flushes.cleanup,
            WINFSP_CLEANUP_REGRESSION_ISSUE,
        ));
    }
    if snapshot.winfsp_foreground_flushes.cleanup_skipped_clean > 0 {
        out.push_str(&format!(
            "winfsp_follow_up: non-delete cleanup regression attributed {} skipped-clean barrier(s); record follow-up evidence for {}\n",
            snapshot.winfsp_foreground_flushes.cleanup_skipped_clean,
            WINFSP_CLEANUP_REGRESSION_ISSUE,
        ));
    }
    if snapshot.winfsp_callbacks.cleanup_flush_fh > 0 {
        out.push_str(&format!(
            "winfsp_follow_up: cleanup routed through flush_fh {} time(s); inspect WinFSP callback sequencing and record follow-up evidence for {}\n",
            snapshot.winfsp_callbacks.cleanup_flush_fh,
            WINFSP_CLEANUP_REGRESSION_ISSUE,
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{collect_write_path_artifacts, render_write_path_summary};
    use crate::cli::test_cache_profile_types::ProfileArtifactKind;
    use studiofs_vfs::{
        VfsWritePathObservabilitySnapshot, VfsWritePathWinfspCallbackStats,
        VfsWritePathWinfspForegroundFlushStats,
    };

    #[test]
    fn render_write_path_summary_includes_winfsp_callback_breakdown() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-block-runtime".to_string(),
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                write: 11,
                overwrite: 2,
                cleanup: 7,
                cleanup_flush_fh: 0,
                flush: 5,
                close: 7,
            },
            winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats {
                cleanup: 0,
                flush: 2,
                close: 3,
                cleanup_skipped_clean: 0,
                flush_skipped_clean: 3,
                close_skipped_clean: 4,
            },
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        let summary = render_write_path_summary(&bytes).expect("render summary");

        assert!(summary.contains(
            "winfsp_callbacks: write=11 overwrite=2 cleanup=7 cleanup_flush_fh=0 flush=5 close=7"
        ));
        assert!(summary.contains("winfsp_foreground_flushes: cleanup=0 cleanup_skipped_clean=0 flush=2 flush_skipped_clean=3 close=3 close_skipped_clean=4"));
        assert!(summary.contains("winfsp_validation: cleanup_non_delete=cleanup-deferred cleanup_non_delete_barriers=0 cleanup_non_delete_skipped_clean=0 cleanup_flush_fh=0 flush_barriers=2 close_barriers=3"));
    }

    #[test]
    fn render_write_path_summary_emits_follow_up_for_cleanup_barrier_regressions() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-block-runtime".to_string(),
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                cleanup: 5,
                ..Default::default()
            },
            winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats {
                cleanup: 2,
                ..Default::default()
            },
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        let summary = render_write_path_summary(&bytes).expect("render summary");

        assert!(summary.contains("winfsp_follow_up: non-delete cleanup regression attributed 2 foreground barrier(s); record follow-up evidence for SFS-192"));
    }

    #[test]
    fn render_write_path_summary_emits_follow_up_for_cleanup_skipped_clean_regressions() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-block-runtime".to_string(),
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                cleanup: 6,
                ..Default::default()
            },
            winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats {
                cleanup_skipped_clean: 3,
                ..Default::default()
            },
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        let summary = render_write_path_summary(&bytes).expect("render summary");

        assert!(summary.contains("winfsp_validation: cleanup_non_delete=cleanup-follow-up cleanup_non_delete_barriers=0 cleanup_non_delete_skipped_clean=3 cleanup_flush_fh=0"));
        assert!(summary.contains("winfsp_follow_up: non-delete cleanup regression attributed 3 skipped-clean barrier(s); record follow-up evidence for SFS-192"));
    }

    #[test]
    fn render_write_path_summary_emits_follow_up_for_cleanup_flush_fh_callbacks() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-block-runtime".to_string(),
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                cleanup: 4,
                cleanup_flush_fh: 2,
                ..Default::default()
            },
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        let summary = render_write_path_summary(&bytes).expect("render summary");

        assert!(summary.contains("winfsp_follow_up: cleanup routed through flush_fh 2 time(s); inspect WinFSP callback sequencing and record follow-up evidence for SFS-192"));
    }

    #[test]
    fn collect_write_path_artifacts_writes_json_and_summary_artifacts() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-block-runtime".to_string(),
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                cleanup: 3,
                close: 1,
                ..Default::default()
            },
            winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats {
                close: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let temp = tempfile::tempdir().expect("tempdir");
        let metrics_path = temp.path().join("write_path_observability.json");
        std::fs::write(
            &metrics_path,
            serde_json::to_vec(&snapshot).expect("serialize snapshot"),
        )
        .expect("write metrics snapshot");

        let output_dir = temp.path().join("artifacts");
        let mut warnings = Vec::new();
        let artifacts = collect_write_path_artifacts(
            &output_dir,
            "run-123",
            "e2e",
            Some(&metrics_path),
            &mut warnings,
        )
        .expect("collect write path artifacts");

        assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
        assert_eq!(artifacts.len(), 2);
        assert_eq!(
            artifacts[0].kind,
            ProfileArtifactKind::WritePathObservability
        );
        assert_eq!(artifacts[1].kind, ProfileArtifactKind::WritePathSummary);
        let summary = std::fs::read_to_string(&artifacts[1].path).expect("read summary");
        assert!(summary.contains("winfsp_validation: cleanup_non_delete=cleanup-deferred"));
    }
}

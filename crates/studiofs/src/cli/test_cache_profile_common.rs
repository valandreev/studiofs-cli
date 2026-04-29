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
        "landing_zone: accepted_writes={} accepted_bytes={} accepted_mib_per_sec={:.2} recoverable_segments={}\n",
        snapshot.landing_zone.accepted_writes,
        format_bytes(snapshot.landing_zone.accepted_bytes),
        snapshot.landing_zone.accepted_mib_per_sec,
        snapshot.landing_zone.recoverable_segments
    ));
    out.push_str(&format!(
        "landing_zone_accept: total_ms={:.2} avg_ms={:.3} max_ms={:.3}\n",
        snapshot.landing_zone.accept_total_ms,
        snapshot.landing_zone.accept_avg_ms,
        snapshot.landing_zone.accept_max_ms
    ));
    out.push_str(&format!(
        "landing_zone_syncs: data_syncs={} manifest_syncs={} manifest_rewrites={} total_ms={:.2} avg_ms={:.3} max_ms={:.3}\n",
        snapshot.landing_zone.data_syncs,
        snapshot.landing_zone.manifest_syncs,
        snapshot.landing_zone.manifest_rewrites,
        snapshot.landing_zone.sync_total_ms,
        snapshot.landing_zone.sync_avg_ms,
        snapshot.landing_zone.sync_max_ms
    ));
    out.push_str(&format!(
        "drain_lag: current={} steady_quota={} soft_pacing_events={} soft_pacing_total_ms={} soft_pacing_max_ms={} admission_ingress={}/s admission_drain={}/s admission_runway_ms={} admission_last_delay_ms={} admission_pressure={} admission_budget={} local_sync_unsynced={}\n",
        format_bytes(
            u64::try_from(snapshot.drain_lag.current_bytes)
                .context("drain lag bytes does not fit in u64")
                .unwrap_or(u64::MAX)
        ),
        format_bytes(
            u64::try_from(snapshot.drain_lag.steady_quota_bytes)
                .context("drain lag steady quota does not fit in u64")
                .unwrap_or(u64::MAX)
        ),
        snapshot.drain_lag.soft_pacing_events,
        snapshot.drain_lag.soft_pacing_total_ms,
        snapshot.drain_lag.soft_pacing_max_ms,
        format_bytes(snapshot.drain_lag.admission_ingress_bytes_per_sec),
        format_bytes(snapshot.drain_lag.admission_drain_bytes_per_sec),
        snapshot.drain_lag.admission_runway_ms,
        snapshot.drain_lag.admission_last_delay_ms,
        format_bytes(
            u64::try_from(snapshot.drain_lag.admission_pressure_bytes)
                .context("admission pressure bytes does not fit in u64")?
        ),
        format_bytes(
            u64::try_from(snapshot.drain_lag.admission_budget_bytes)
                .context("admission budget bytes does not fit in u64")?
        ),
        format_bytes(
            u64::try_from(snapshot.drain_lag.local_sync_unsynced_bytes)
                .context("local sync unsynced bytes does not fit in u64")?
        )
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
        "upload_stale: total={} before_put={} after_put={} after_put_bytes={}\n",
        snapshot.upload.canceled_stale,
        snapshot.upload.canceled_stale_before_put,
        snapshot.upload.canceled_stale_after_put,
        format_bytes(snapshot.upload.canceled_stale_after_put_bytes)
    ));
    out.push_str(&format!(
        "fetch: cache_hits={} s3_fetches={} s3_errors={} singleflight={} retries={} bytes_downloaded={} prefetch_skips={}\n",
        snapshot.fetch.cache_hits,
        snapshot.fetch.s3_fetches,
        snapshot.fetch.s3_errors,
        snapshot.fetch.singleflight_joins,
        snapshot.fetch.retries,
        format_bytes(snapshot.fetch.bytes_downloaded),
        snapshot.fetch.prefetch_skips
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
        snapshot.payload_store_sync_data.calls_per_gib
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
        VfsFetchStats, VfsUploadStats, VfsWritePathDrainLagStats, VfsWritePathLandingZoneStats,
        VfsWritePathObservabilitySnapshot, VfsWritePathWinfspCallbackStats,
        VfsWritePathWinfspForegroundFlushStats,
    };

    #[test]
    fn render_write_path_summary_includes_winfsp_callback_breakdown() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-payload-runtime".to_string(),
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
    fn render_write_path_summary_includes_stale_upload_and_fetch_counters() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-payload-runtime".to_string(),
            upload: VfsUploadStats {
                canceled_stale: 7,
                canceled_stale_before_put: 4,
                canceled_stale_after_put: 3,
                canceled_stale_after_put_bytes: 12 * 1024 * 1024,
                ..Default::default()
            },
            fetch: VfsFetchStats {
                cache_hits: 11,
                s3_fetches: 5,
                s3_errors: 1,
                singleflight_joins: 2,
                retries: 1,
                bytes_downloaded: 20 * 1024 * 1024,
                prefetch_skips: 9,
                ..Default::default()
            },
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        let summary = render_write_path_summary(&bytes).expect("render summary");

        assert!(
            summary
                .contains("upload_stale: total=7 before_put=4 after_put=3 after_put_bytes=12.0MiB")
        );
        assert!(summary.contains(
            "fetch: cache_hits=11 s3_fetches=5 s3_errors=1 singleflight=2 retries=1 bytes_downloaded=20.0MiB prefetch_skips=9"
        ));
    }

    #[test]
    fn render_write_path_summary_includes_landing_and_drain_lag_evidence() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-payload-runtime".to_string(),
            landing_zone: VfsWritePathLandingZoneStats {
                accepted_writes: 12,
                accepted_bytes: 48 * 1024 * 1024,
                accepted_mib_per_sec: 1536.0,
                recoverable_segments: 5,
                accept_total_ms: 24.0,
                accept_avg_ms: 2.0,
                accept_max_ms: 6.0,
                data_syncs: 2,
                manifest_syncs: 3,
                manifest_rewrites: 1,
                sync_total_ms: 15.0,
                sync_avg_ms: 3.0,
                sync_max_ms: 9.0,
            },
            drain_lag: VfsWritePathDrainLagStats {
                current_bytes: 20 * 1024 * 1024,
                steady_quota_bytes: 4 * 1024 * 1024,
                soft_pacing_events: 3,
                soft_pacing_total_ms: 9,
                soft_pacing_max_ms: 4,
                admission_ingress_bytes_per_sec: 1_600 * 1024 * 1024,
                admission_drain_bytes_per_sec: 1_200 * 1024 * 1024,
                admission_runway_ms: 1_250,
                admission_last_delay_ms: 2,
                admission_pressure_bytes: 3 * 1024 * 1024 * 1024,
                admission_budget_bytes: 4 * 1024 * 1024 * 1024,
                local_sync_unsynced_bytes: 512 * 1024 * 1024,
            },
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        let summary = render_write_path_summary(&bytes).expect("render summary");

        assert!(summary.contains(
            "landing_zone: accepted_writes=12 accepted_bytes=48.0MiB accepted_mib_per_sec=1536.00 recoverable_segments=5"
        ));
        assert!(summary.contains("landing_zone_accept: total_ms=24.00 avg_ms=2.000 max_ms=6.000"));
        assert!(summary.contains("landing_zone_syncs: data_syncs=2 manifest_syncs=3 manifest_rewrites=1 total_ms=15.00 avg_ms=3.000 max_ms=9.000"));
        assert!(summary.contains("drain_lag: current=20.0MiB steady_quota=4.0MiB soft_pacing_events=3 soft_pacing_total_ms=9 soft_pacing_max_ms=4"));
        assert!(summary.contains("admission_ingress=1.6GiB/s admission_drain=1.2GiB/s admission_runway_ms=1250 admission_last_delay_ms=2"));
        assert!(summary.contains("admission_pressure=3.0GiB admission_budget=4.0GiB"));
        assert!(summary.contains("local_sync_unsynced=512.0MiB"));
    }

    #[test]
    fn render_write_path_summary_emits_follow_up_for_cleanup_barrier_regressions() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "local-payload-runtime".to_string(),
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
            backend_tag: "local-payload-runtime".to_string(),
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
            backend_tag: "local-payload-runtime".to_string(),
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
            backend_tag: "local-payload-runtime".to_string(),
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

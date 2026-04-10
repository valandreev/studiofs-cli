use crate::cli::test_cache_profile_types::{
    ProfileArtifactReport, ProfileEndpointReport, TestCacheProfileMode, TestCacheProfileResult,
};
use crate::cli::utils::format_bytes;
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use flate2::{Compression, write::GzEncoder};
use prost::Message as _;
use pyroscope::backend::pprof::PprofConfig;
use pyroscope::backend::{BackendConfig, BackendReady, ReportBatch, ReportData, pprof_backend};
use pyroscope::encode::r#gen::push::{PushRequest, RawProfileSeries, RawSample};

use pyroscope::encode::r#gen::types::LabelPair;
use pyroscope::encode::pprof;
use reqwest::Url;
use std::env;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;
use studiofs_vfs::VfsWritePathObservabilitySnapshot;

const DEFAULT_APP_NAME: &str = "studiofs.test-cache";
const DEFAULT_SAMPLE_RATE: u32 = 100;

pub struct TestCacheProfileSession {
    run_id: String,
    benchmark_mode: String,
    started_at: DateTime<Utc>,
    sample_rate: u32,
    cpu_backend: Option<pyroscope::backend::BackendImpl<BackendReady>>,
    memory_backend: Option<pyroscope::backend::BackendImpl<BackendReady>>,
    endpoint: Option<ProfileEndpoint>,
    write_path_observability_path: Option<PathBuf>,
    app_name: String,
    warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct ProfileEndpoint {
    url: String,
    username: Option<String>,
    password: Option<String>,
    tenant_id: Option<String>,
}

impl TestCacheProfileSession {
    pub fn start(
        mode: Option<TestCacheProfileMode>,
        run_id: &str,
        benchmark_mode: &str,
        write_path_observability_path: Option<PathBuf>,
    ) -> Result<Option<Self>> {
        if mode.is_none() {
            return Ok(None);
        }

        let sample_rate = read_sample_rate();
        let app_name = read_app_name(benchmark_mode);
        let endpoint = read_endpoint();
        let started_at = Utc::now();
        let mut warnings = Vec::new();

        let cpu_config = PprofConfig { sample_rate };
        let cpu_backend = match pprof_backend(cpu_config, BackendConfig::default()).initialize() {
            Ok(backend) => Some(backend),
            Err(err) => {
                warnings.push(format!("CPU profile unavailable: {err}"));
                None
            }
        };

        let memory_backend = match pyroscope::backend::jemalloc_backend().initialize() {
            Ok(backend) => Some(backend),
            Err(err) => {
                warnings.push(format!("Memory profile unavailable: {err}"));
                None
            }
        };

        Ok(Some(Self {
            run_id: run_id.to_string(),
            benchmark_mode: benchmark_mode.to_string(),
            started_at,
            sample_rate,
            cpu_backend,
            memory_backend,
            endpoint,
            write_path_observability_path,
            app_name,
            warnings,
        }))
    }

    pub fn finish(mut self, output_dir: &Path) -> Result<TestCacheProfileResult> {
        let mut artifacts = Vec::new();
        let finished_at = Utc::now();

        if let Some(metrics_path) = self.write_path_observability_path.as_ref() {
            match fs::read(metrics_path) {
                Ok(bytes) => {
                    let artifact = write_artifact(
                        output_dir,
                        &self.run_id,
                        &self.benchmark_mode,
                        "write-path-observability",
                        "json",
                        &bytes,
                    )?;
                    artifacts.push(ProfileArtifactReport {
                        kind: "write-path-observability".to_string(),
                        path: artifact.display().to_string(),
                        bytes: bytes.len() as u64,
                        sent_to_endpoint: false,
                    });
                    match render_write_path_summary(&bytes) {
                        Ok(summary) => {
                            let summary_bytes = summary.into_bytes();
                            let summary_artifact = write_artifact(
                                output_dir,
                                &self.run_id,
                                &self.benchmark_mode,
                                "write-path-summary",
                                "txt",
                                &summary_bytes,
                            )?;
                            artifacts.push(ProfileArtifactReport {
                                kind: "write-path-summary".to_string(),
                                path: summary_artifact.display().to_string(),
                                bytes: summary_bytes.len() as u64,
                                sent_to_endpoint: false,
                            });
                        }
                        Err(err) => {
                            self.warnings.push(format!(
                                "Failed to render write-path summary from '{}': {err}",
                                metrics_path.display()
                            ));
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    self.warnings.push(format!(
                        "Write-path observability snapshot not found: {}",
                        metrics_path.display()
                    ));
                }
                Err(err) => {
                    self.warnings.push(format!(
                        "Failed to read write-path observability snapshot '{}': {err}",
                        metrics_path.display()
                    ));
                }
            }
        }

        if let Some(mut backend) = self.cpu_backend.take() {
            match backend.report() {
                Ok(batch) => {
                    match render_cpu_pprof(&batch, self.sample_rate, self.started_at, finished_at) {
                        Ok(bytes) => {
                            let artifact = write_artifact(
                                output_dir,
                                &self.run_id,
                                &self.benchmark_mode,
                                "cpu",
                                "pprof.gz",
                                &bytes,
                            )?;
                            let sent = match self.endpoint.as_ref() {
                                Some(endpoint) => match send_profile(
                                    endpoint,
                                    &self.app_name,
                                    &self.run_id,
                                    &self.benchmark_mode,
                                    batch.profile_type.as_str(),
                                    bytes.as_slice(),
                                ) {
                                    Ok(()) => true,
                                    Err(err) => {
                                        self.warnings
                                            .push(format!("CPU profile upload failed: {err}"));
                                        false
                                    }
                                },
                                None => false,
                            };
                            artifacts.push(ProfileArtifactReport {
                                kind: "cpu".to_string(),
                                path: artifact.display().to_string(),
                                bytes: bytes.len() as u64,
                                sent_to_endpoint: sent,
                            });
                        }
                        Err(err) => {
                            self.warnings
                                .push(format!("CPU profile export failed: {err}"));
                        }
                    }
                }
                Err(err) => {
                    self.warnings
                        .push(format!("CPU profile collection failed: {err}"));
                }
            }
        }

        if let Some(mut backend) = self.memory_backend.take() {
            match backend.report() {
                Ok(batch) => {
                    let bytes = render_memory_pprof(&batch)?;
                    let artifact = write_artifact(
                        output_dir,
                        &self.run_id,
                        &self.benchmark_mode,
                        "memory",
                        "pprof.gz",
                        &bytes,
                    )?;
                    let sent = match self.endpoint.as_ref() {
                        Some(endpoint) => match send_profile(
                            endpoint,
                            &self.app_name,
                            &self.run_id,
                            &self.benchmark_mode,
                            batch.profile_type.as_str(),
                            bytes.as_slice(),
                        ) {
                            Ok(()) => true,
                            Err(err) => {
                                self.warnings
                                    .push(format!("Memory profile upload failed: {err}"));
                                false
                            }
                        },
                        None => false,
                    };
                    artifacts.push(ProfileArtifactReport {
                        kind: "memory".to_string(),
                        path: artifact.display().to_string(),
                        bytes: bytes.len() as u64,
                        sent_to_endpoint: sent,
                    });
                }
                Err(err) => {
                    self.warnings
                        .push(format!("Memory profile collection failed: {err}"));
                }
            }
        }

        Ok(TestCacheProfileResult {
            endpoint: self
                .endpoint
                .as_ref()
                .map(|endpoint| ProfileEndpointReport {
                    url: endpoint.url.clone(),
                    application_name: self.app_name.clone(),
                }),
            artifacts,
            warnings: self.warnings,
        })
    }
}

fn read_sample_rate() -> u32 {
    env::var("STUDIOFS_PYROSCOPE_SAMPLE_RATE")
        .or_else(|_| env::var("PYROSCOPE_SAMPLE_RATE"))
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SAMPLE_RATE)
}

fn read_app_name(benchmark_mode: &str) -> String {
    env::var("STUDIOFS_PYROSCOPE_APP_NAME")
        .or_else(|_| env::var("PYROSCOPE_APPLICATION_NAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("{DEFAULT_APP_NAME}.{benchmark_mode}"))
}

fn read_endpoint() -> Option<ProfileEndpoint> {
    let url = env::var("STUDIOFS_PYROSCOPE_URL")
        .or_else(|_| env::var("PYROSCOPE_URL"))
        .ok()?;
    let url = url.trim();
    if url.is_empty() {
        return None;
    }
    Some(ProfileEndpoint {
        url: url.to_string(),
        username: env::var("STUDIOFS_PYROSCOPE_USERNAME")
            .or_else(|_| env::var("PYROSCOPE_USERNAME"))
            .ok()
            .filter(|value| !value.trim().is_empty()),
        password: env::var("STUDIOFS_PYROSCOPE_PASSWORD")
            .or_else(|_| env::var("PYROSCOPE_PASSWORD"))
            .ok()
            .filter(|value| !value.trim().is_empty()),
        tenant_id: env::var("STUDIOFS_PYROSCOPE_TENANT_ID")
            .or_else(|_| env::var("PYROSCOPE_TENANT_ID"))
            .ok()
            .filter(|value| !value.trim().is_empty()),
    })
}

fn write_artifact(
    output_dir: &Path,
    run_id: &str,
    benchmark_mode: &str,
    kind: &str,
    suffix: &str,
    bytes: &[u8],
) -> Result<PathBuf> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("create profile output directory {}", output_dir.display()))?;
    let filename = format!("studiofs-test-cache-{benchmark_mode}-{run_id}-{kind}.{suffix}");
    let path = output_dir.join(filename);
    fs::write(&path, bytes)
        .with_context(|| format!("write profile artifact {}", path.display()))?;
    Ok(path)
}

fn render_cpu_pprof(
    batch: &ReportBatch,
    sample_rate: u32,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
) -> Result<Vec<u8>> {
    let reports = match &batch.data {
        ReportData::Reports(reports) => reports,
        ReportData::RawPprof(bytes) => return Ok(bytes.clone()),
    };
    let profile = pprof::encode(
        reports,
        sample_rate,
        started_at.timestamp_nanos_opt().unwrap_or_default() as u64,
        (finished_at - started_at)
            .num_nanoseconds()
            .unwrap_or_default()
            .max(0) as u64,
    );
    let mut encoded = Vec::new();
    profile.encode(&mut encoded)?;
    gzip_bytes(&encoded)
}

fn render_memory_pprof(batch: &ReportBatch) -> Result<Vec<u8>> {
    match &batch.data {
        ReportData::RawPprof(bytes) => Ok(bytes.clone()),
        ReportData::Reports(reports) => {
            let profile = pprof::encode(reports, DEFAULT_SAMPLE_RATE, 0, 0);
            let mut encoded = Vec::new();
            profile.encode(&mut encoded)?;
            Ok(gzip_bytes(&encoded)?)
        }
    }
}

fn gzip_bytes(input: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input)?;
    Ok(encoder.finish()?)
}

fn winfsp_cleanup_validation_state(snapshot: &VfsWritePathObservabilitySnapshot) -> &'static str {
    if snapshot.winfsp_foreground_flushes.cleanup > 0
        || snapshot.winfsp_callbacks.cleanup_flush_fh > 0
    {
        "cleanup-follow-up"
    } else if snapshot.winfsp_callbacks.cleanup > 0 {
        "cleanup-deferred"
    } else {
        "not-observed"
    }
}

fn render_write_path_summary(bytes: &[u8]) -> Result<String> {
    let snapshot: VfsWritePathObservabilitySnapshot =
        serde_json::from_slice(bytes).context("parse write-path observability snapshot")?;
    let mut out = String::new();
    out.push_str(&format!("backend: {}\n", snapshot.backend_tag));
    out.push_str(&format!(
        "eager_flush_threshold: {}\n",
        format_bytes(snapshot.tuning.eager_flush_threshold_bytes as u64)
    ));
    out.push_str(&format!(
        "eager_flush_drain_chunk: {}\n",
        format_bytes(snapshot.tuning.eager_flush_drain_chunk_bytes as u64)
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
        "winfsp_validation: cleanup_non_delete={} cleanup_non_delete_barriers={} cleanup_flush_fh={} flush_barriers={} close_barriers={}\n",
        winfsp_cleanup_validation_state(&snapshot),
        snapshot.winfsp_foreground_flushes.cleanup,
        snapshot.winfsp_callbacks.cleanup_flush_fh,
        snapshot.winfsp_foreground_flushes.flush,
        snapshot.winfsp_foreground_flushes.close,
    ));
    if snapshot.winfsp_foreground_flushes.cleanup > 0 {
        out.push_str(&format!(
            "winfsp_follow_up: non-delete cleanup still owns {} foreground barrier(s); feed this run into SFS-192 before closing bridge validation\n",
            snapshot.winfsp_foreground_flushes.cleanup
        ));
    }
    if snapshot.winfsp_callbacks.cleanup_flush_fh > 0 {
        out.push_str(&format!(
            "winfsp_follow_up: cleanup routed through flush_fh {} time(s); inspect WinFSP callback sequencing before closing SFS-192\n",
            snapshot.winfsp_callbacks.cleanup_flush_fh
        ));
    }
    Ok(out)
}

fn send_profile(
    endpoint: &ProfileEndpoint,
    application_name: &str,
    run_id: &str,
    benchmark_mode: &str,
    profile_type: &str,
    raw_profile: &[u8],
) -> Result<()> {
    let client = reqwest::blocking::Client::builder()
        .build()
        .context("failed to build reqwest client")?;
    let labels = vec![
        LabelPair {
            name: "service_name".to_string(),
            value: application_name.to_string(),
        },
        LabelPair {
            name: "__name__".to_string(),
            value: profile_type.to_string(),
        },
        LabelPair {
            name: "run_id".to_string(),
            value: run_id.to_string(),
        },
        LabelPair {
            name: "benchmark_mode".to_string(),
            value: benchmark_mode.to_string(),
        },
    ];
    let req = PushRequest {
        series: vec![RawProfileSeries {
            labels,
            samples: vec![RawSample {
                raw_profile: raw_profile.to_vec(),
                id: uuid::Uuid::new_v4().to_string(),
            }],
        }],
    };

    let mut url = Url::parse(&endpoint.url).context("invalid Pyroscope endpoint URL")?;
    url.path_segments_mut()
        .map_err(|_| anyhow!("Pyroscope endpoint URL cannot be used as a base"))?
        .push("push.v1.PusherService")
        .push("Push");

    let mut request = client
        .post(url.as_str())
        .header("User-Agent", "pyroscope-rs/studiofs reqwest".to_string())
        .header("Content-Type", "application/proto")
        .header("Content-Encoding", "gzip")
        .timeout(Duration::from_secs(10));

    if let (Some(username), Some(password)) = (&endpoint.username, &endpoint.password) {
        request = request.basic_auth(username.clone(), Some(password.clone()));
    }
    if let Some(tenant_id) = &endpoint.tenant_id {
        request = request.header("X-Scope-OrgID", tenant_id);
    }

    let mut encoded_req = Vec::new();
    req.encode(&mut encoded_req)?;
    let gzipped_req = gzip_bytes(&encoded_req)?;

    let response = request
        .body(gzipped_req)
        .send()
        .context("send Pyroscope profile")?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "Pyroscope endpoint returned HTTP {}",
            response.status()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::render_write_path_summary;
    use studiofs_vfs::{
        VfsWritePathObservabilitySnapshot, VfsWritePathWinfspCallbackStats,
        VfsWritePathWinfspForegroundFlushStats,
    };

    #[test]
    fn render_write_path_summary_includes_winfsp_callback_breakdown() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "cache-v3".to_string(),
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                write: 11,
                overwrite: 2,
                cleanup: 7,
                cleanup_flush_fh: 0,
                flush: 5,
                close: 7,
                ..Default::default()
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

        assert!(
            summary.contains(
                "winfsp_callbacks: write=11 overwrite=2 cleanup=7 cleanup_flush_fh=0 flush=5 close=7"
            ),
            "summary should include the WinFSP callback counts: {summary}"
        );
        assert!(
            summary.contains(
                "winfsp_foreground_flushes: cleanup=0 cleanup_skipped_clean=0 flush=2 flush_skipped_clean=3 close=3 close_skipped_clean=4"
            ),
            "summary should include the WinFSP foreground flush attribution: {summary}"
        );
        assert!(
            summary.contains(
                "winfsp_validation: cleanup_non_delete=cleanup-deferred cleanup_non_delete_barriers=0 cleanup_flush_fh=0 flush_barriers=2 close_barriers=3"
            ),
            "summary should describe the WinFSP validation posture: {summary}"
        );
    }

    #[test]
    fn render_write_path_summary_emits_follow_up_for_cleanup_barrier_regressions() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "cache-v3".to_string(),
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

        assert!(
            summary.contains(
                "winfsp_follow_up: non-delete cleanup still owns 2 foreground barrier(s); feed this run into SFS-192 before closing bridge validation"
            ),
            "summary should flag unexpected cleanup-owned barriers as follow-up evidence: {summary}"
        );
    }

    #[test]
    fn render_write_path_summary_emits_follow_up_for_cleanup_flush_fh_callbacks() {
        let snapshot = VfsWritePathObservabilitySnapshot {
            backend_tag: "cache-v3".to_string(),
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                cleanup: 4,
                cleanup_flush_fh: 2,
                ..Default::default()
            },
            ..Default::default()
        };

        let bytes = serde_json::to_vec(&snapshot).expect("serialize snapshot");
        let summary = render_write_path_summary(&bytes).expect("render summary");

        assert!(
            summary.contains(
                "winfsp_follow_up: cleanup routed through flush_fh 2 time(s); inspect WinFSP callback sequencing before closing SFS-192"
            ),
            "summary should flag unexpected cleanup_flush_fh callbacks as follow-up evidence: {summary}"
        );
    }
}

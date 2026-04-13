use crate::cli::mount_profile_types::MountProfileMode;
use crate::diagnostics::{
    ActiveRuntimeProfilingWindow, ProfilingBackendStartResult, ProfilingBackendStopResult,
    RuntimeProfilingBackend, RuntimeProfilingStopper, StartProfilingWindowRequest,
};
use crate::investigation::{ProfilingCaptureMode, ProfilingWindowStatus};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use flate2::{Compression, write::GzEncoder};
use prost::Message as _;
use pyroscope::backend::pprof::PprofConfig;
use pyroscope::backend::{
    BackendConfig, BackendImpl, BackendReady, ReportBatch, ReportData, pprof_backend,
};
use pyroscope::encode::r#gen::push::{PushRequest, RawProfileSeries, RawSample};
use pyroscope::encode::r#gen::types::LabelPair;
use pyroscope::encode::pprof;
use reqwest::Url;
use std::env;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const DEFAULT_APP_NAME: &str = "studiofs.mount";
const DEFAULT_SAMPLE_RATE: u32 = 100;
const PROFILE_DIR_NAME: &str = "profiles";

#[derive(Debug, Clone)]
struct ProfileEndpoint {
    url: String,
    username: Option<String>,
    password: Option<String>,
    tenant_id: Option<String>,
}

pub struct MountProfileSession {
    backend: BackendImpl<BackendReady>,
    started_at: DateTime<Utc>,
    sample_rate: u32,
    endpoint: Option<ProfileEndpoint>,
    app_name: String,
    tags: Vec<(String, String)>,
    output_dir: PathBuf,
}

impl MountProfileSession {
    pub fn start(
        mode: Option<MountProfileMode>,
        volume_name: &str,
        platform: &str,
        bridge: &str,
        mount_mode: &str,
        cache_mode: &str,
        cache_path: &Path,
    ) -> Option<Self> {
        let env_mode = read_env_profile_mode();
        let enabled = match mode.or(env_mode) {
            Some(MountProfileMode::Pyroscope) => true,
            Some(MountProfileMode::Off) => false,
            None => read_endpoint().is_some(),
        };

        if !enabled {
            if mode == Some(MountProfileMode::Off)
                || matches!(env_mode, Some(MountProfileMode::Off))
            {
                info!("Pyroscope profiling disabled for this mount");
            }
            return None;
        }

        let started_at = Utc::now();
        let sample_rate = read_sample_rate();
        let app_name = read_app_name();
        let endpoint = read_endpoint();
        let output_dir = cache_path.join(PROFILE_DIR_NAME);
        let mut tags = vec![
            ("platform".to_string(), platform.to_string()),
            ("volume".to_string(), volume_name.to_string()),
            ("mount_mode".to_string(), mount_mode.to_string()),
            ("cache_mode".to_string(), cache_mode.to_string()),
            ("bridge".to_string(), bridge.to_string()),
            ("runtime".to_string(), "mount".to_string()),
        ];
        tags = merge_tags(tags, read_custom_tags());

        let endpoint_desc = endpoint
            .as_ref()
            .map(|endpoint| endpoint.url.as_str())
            .unwrap_or("local artifact only");
        info!(
            app_name = %app_name,
            sample_rate,
            endpoint = %endpoint_desc,
            output_dir = %output_dir.display(),
            tags = %format_tags(&tags),
            "Pyroscope profiling enabled for mount runtime"
        );

        Some(Self {
            backend: pprof_backend(PprofConfig { sample_rate }, BackendConfig::default())
                .initialize()
                .map_err(|err| {
                    warn!("Pyroscope CPU profiler unavailable: {err}");
                })
                .ok()?,
            started_at,
            sample_rate,
            endpoint,
            app_name,
            tags,
            output_dir,
        })
    }

    pub fn finish(self) {
        let MountProfileSession {
            mut backend,
            started_at,
            sample_rate,
            endpoint,
            app_name,
            tags,
            output_dir,
        } = self;
        let finished_at = Utc::now();

        let mut report = match backend.report() {
            Ok(report) => Some(report),
            Err(err) => {
                warn!("Pyroscope CPU profile collection failed: {err}");
                None
            }
        };

        if let Some(report) = report.as_mut() {
            match render_cpu_pprof(report, sample_rate, started_at, finished_at) {
                Ok(bytes) => {
                    match write_artifact(
                        &output_dir,
                        &app_name,
                        "mount",
                        &tags,
                        finished_at,
                        bytes.as_slice(),
                    ) {
                        Ok(path) => {
                            info!(
                                path = %path.display(),
                                bytes = bytes.len(),
                                "wrote mount Pyroscope profile artifact"
                            );
                        }
                        Err(err) => {
                            warn!("failed to write mount Pyroscope profile artifact: {err}");
                        }
                    }

                    if let Some(endpoint) = endpoint.as_ref()
                        && let Err(err) = send_profile(
                            endpoint,
                            &app_name,
                            report.profile_type.as_str(),
                            &tags,
                            bytes.as_slice(),
                        )
                    {
                        warn!("mount Pyroscope profile upload failed: {err}");
                    }
                }
                Err(err) => {
                    warn!("mount Pyroscope profile export failed: {err}");
                }
            }
        }

        if let Err(err) = backend.shutdown() {
            warn!("Pyroscope CPU profiler shutdown failed: {err}");
        }
    }
}

struct MountRuntimeProfilingBackend {
    volume_name: String,
    platform: String,
    bridge: String,
    mount_mode: String,
    cache_mode: String,
    output_dir: PathBuf,
    app_name: String,
    endpoint: Option<ProfileEndpoint>,
}

struct MountRuntimeProfileStopper {
    backend: BackendImpl<BackendReady>,
    started_at: DateTime<Utc>,
    sample_rate: u32,
    endpoint: Option<ProfileEndpoint>,
    app_name: String,
    tags: Vec<(String, String)>,
    source: String,
    output_dir: PathBuf,
}

impl RuntimeProfilingBackend for MountRuntimeProfilingBackend {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode> {
        vec![
            ProfilingCaptureMode::WallBlocking,
            ProfilingCaptureMode::Cpu,
        ]
    }

    fn start_window(&self, request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        let sample_rate = request.budget.sample_rate_hz.unwrap_or(DEFAULT_SAMPLE_RATE);
        let tags = merge_tags(
            vec![
                ("platform".to_string(), self.platform.clone()),
                ("volume".to_string(), self.volume_name.clone()),
                ("mount_mode".to_string(), self.mount_mode.clone()),
                ("cache_mode".to_string(), self.cache_mode.clone()),
                ("bridge".to_string(), self.bridge.clone()),
                ("runtime".to_string(), "attach".to_string()),
                (
                    "capture_mode".to_string(),
                    match request.capture_mode {
                        ProfilingCaptureMode::WallBlocking => "wall_blocking",
                        ProfilingCaptureMode::Cpu => "cpu",
                    }
                    .to_string(),
                ),
                (
                    "preset".to_string(),
                    format!("{:?}", request.preset).to_ascii_lowercase(),
                ),
            ],
            read_custom_tags(),
        );

        let backend = match pprof_backend(PprofConfig { sample_rate }, BackendConfig::default())
            .initialize()
        {
            Ok(backend) => backend,
            Err(error) => {
                return ProfilingBackendStartResult::Unsupported {
                    error: format!("Pyroscope CPU profiler unavailable: {error}"),
                };
            }
        };

        let source = match request.capture_mode {
            ProfilingCaptureMode::WallBlocking => "wall-blocking-window",
            ProfilingCaptureMode::Cpu => "cpu-window",
        }
        .to_string();

        ProfilingBackendStartResult::Active(ActiveRuntimeProfilingWindow {
            sample_rate_hz: Some(sample_rate),
            stopper: Box::new(MountRuntimeProfileStopper {
                backend,
                started_at: Utc::now(),
                sample_rate,
                endpoint: self.endpoint.clone(),
                app_name: self.app_name.clone(),
                tags,
                source,
                output_dir: self.output_dir.clone(),
            }),
        })
    }
}

impl RuntimeProfilingStopper for MountRuntimeProfileStopper {
    fn stop(mut self: Box<Self>) -> Result<ProfilingBackendStopResult> {
        let finished_at = Utc::now();
        let mut artifact = None;
        let mut warnings = Vec::new();

        let report = self
            .backend
            .report()
            .map_err(|error| anyhow!("Pyroscope CPU profile collection failed: {error}"))?;
        let mut report = report;
        let bytes = render_cpu_pprof(&mut report, self.sample_rate, self.started_at, finished_at)
            .map_err(|error| anyhow!("Pyroscope CPU profile export failed: {error}"))?;

        match write_artifact(
            &self.output_dir,
            &self.app_name,
            &self.source,
            &self.tags,
            finished_at,
            bytes.as_slice(),
        ) {
            Ok(path) => {
                let bytes_written = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
                artifact = Some(ProfileArtifactRecord {
                    kind: "pprof".to_string(),
                    path: path.to_string_lossy().into_owned(),
                    bytes: bytes_written,
                });
            }
            Err(error) => warnings.push(error.to_string()),
        }

        if let Some(endpoint) = self.endpoint.as_ref()
            && let Err(error) = send_profile(
                endpoint,
                &self.app_name,
                report.profile_type.as_str(),
                &self.tags,
                bytes.as_slice(),
            )
        {
            warnings.push(format!("mount Pyroscope profile upload failed: {error}"));
        }

        if let Err(error) = self.backend.shutdown() {
            warnings.push(format!("Pyroscope CPU profiler shutdown failed: {error}"));
        }

        Ok(ProfilingBackendStopResult {
            actual_duration_ms: Some(
                (finished_at - self.started_at)
                    .num_milliseconds()
                    .max(0)
                    .try_into()
                    .unwrap_or(u64::MAX),
            ),
            status: ProfilingWindowStatus::Completed,
            artifact,
            error: (!warnings.is_empty()).then(|| warnings.join("; ")),
        })
    }
}

pub(crate) fn runtime_profiling_backend(
    volume_name: &str,
    platform: &str,
    bridge: &str,
    mount_mode: &str,
    cache_mode: &str,
    cache_path: &Path,
) -> Arc<dyn RuntimeProfilingBackend> {
    Arc::new(MountRuntimeProfilingBackend {
        volume_name: volume_name.to_string(),
        platform: platform.to_string(),
        bridge: bridge.to_string(),
        mount_mode: mount_mode.to_string(),
        cache_mode: cache_mode.to_string(),
        output_dir: cache_path.join(PROFILE_DIR_NAME),
        app_name: read_app_name(),
        endpoint: read_endpoint(),
    })
}

fn read_env_profile_mode() -> Option<MountProfileMode> {
    let value = env::var("STUDIOFS_PROFILE").ok()?;
    parse_profile_mode(&value)
}

fn parse_profile_mode(value: &str) -> Option<MountProfileMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "pyroscope" | "on" | "true" | "1" => Some(MountProfileMode::Pyroscope),
        "off" | "false" | "0" | "disable" | "disabled" => Some(MountProfileMode::Off),
        _ => None,
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

fn read_app_name() -> String {
    env::var("STUDIOFS_PYROSCOPE_APP_NAME")
        .or_else(|_| env::var("PYROSCOPE_APPLICATION_NAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_APP_NAME.to_string())
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

fn read_custom_tags() -> Vec<(String, String)> {
    let raw = env::var("STUDIOFS_PYROSCOPE_TAGS")
        .or_else(|_| env::var("PYROSCOPE_TAGS"))
        .ok();
    match raw {
        Some(raw) => parse_tag_pairs(&raw),
        None => Vec::new(),
    }
}

fn parse_tag_pairs(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() || value.is_empty() {
                return None;
            }
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

fn merge_tags(
    defaults: Vec<(String, String)>,
    custom: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut merged = defaults;
    for (key, value) in custom {
        if let Some(existing) = merged
            .iter_mut()
            .find(|(existing_key, _)| existing_key == &key)
        {
            existing.1 = value;
        } else {
            merged.push((key, value));
        }
    }
    merged
}

fn format_tags(tags: &[(String, String)]) -> String {
    tags.iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn sanitize_component(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "volume".to_string()
    } else {
        sanitized
    }
}

fn write_artifact(
    output_dir: &Path,
    app_name: &str,
    source: &str,
    tags: &[(String, String)],
    finished_at: DateTime<Utc>,
    bytes: &[u8],
) -> Result<PathBuf> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("create profile output directory {}", output_dir.display()))?;
    let volume = tags
        .iter()
        .find(|(key, _)| key == "volume")
        .map(|(_, value)| sanitize_component(value))
        .unwrap_or_else(|| "volume".to_string());
    let mount_mode = tags
        .iter()
        .find(|(key, _)| key == "mount_mode")
        .map(|(_, value)| sanitize_component(value))
        .unwrap_or_else(|| "mount".to_string());
    let cache_mode = tags
        .iter()
        .find(|(key, _)| key == "cache_mode")
        .map(|(_, value)| sanitize_component(value))
        .unwrap_or_else(|| "cache".to_string());
    let platform = tags
        .iter()
        .find(|(key, _)| key == "platform")
        .map(|(_, value)| sanitize_component(value))
        .unwrap_or_else(|| "platform".to_string());
    let bridge = tags
        .iter()
        .find(|(key, _)| key == "bridge")
        .map(|(_, value)| sanitize_component(value))
        .unwrap_or_else(|| "bridge".to_string());
    let timestamp = finished_at.format("%Y-%m-%dT%H-%M-%SZ");
    let filename = format!(
        "{}-{}-{}-{}-{}-{}-{}.{}.pprof.gz",
        sanitize_component(app_name),
        source,
        volume,
        mount_mode,
        cache_mode,
        platform,
        bridge,
        timestamp,
    );
    let path = output_dir.join(filename);
    fs::write(&path, bytes)
        .with_context(|| format!("write mount profile artifact {}", path.display()))?;
    Ok(path)
}

fn render_cpu_pprof(
    batch: &mut ReportBatch,
    sample_rate: u32,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
) -> Result<Vec<u8>> {
    let reports = match &batch.data {
        ReportData::Reports(reports) => reports,
        ReportData::RawPprof(bytes) => return Ok(bytes.clone()),
    };
    let start_nanos = started_at
        .timestamp_nanos_opt()
        .and_then(|nanos| u64::try_from(nanos).ok())
        .unwrap_or_default();
    let elapsed_nanos = (finished_at - started_at)
        .num_nanoseconds()
        .and_then(|nanos| u64::try_from(nanos).ok())
        .unwrap_or_default();
    let profile = pprof::encode(reports, sample_rate, start_nanos, elapsed_nanos);
    let mut encoded = Vec::new();
    profile.encode(&mut encoded)?;
    gzip_bytes(&encoded)
}

fn gzip_bytes(input: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input)?;
    Ok(encoder.finish()?)
}

fn send_profile(
    endpoint: &ProfileEndpoint,
    application_name: &str,
    profile_type: &str,
    tags: &[(String, String)],
    raw_profile: &[u8],
) -> Result<()> {
    let client = reqwest::blocking::Client::builder()
        .build()
        .context("failed to build reqwest client")?;

    let mut labels = Vec::with_capacity(2 + tags.len());
    labels.push(LabelPair {
        name: "service_name".to_string(),
        value: application_name.to_string(),
    });
    labels.push(LabelPair {
        name: "__name__".to_string(),
        value: profile_type.to_string(),
    });
    for (key, value) in tags {
        labels.push(LabelPair {
            name: key.clone(),
            value: value.clone(),
        });
    }

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
        request = request.basic_auth(username, Some(password));
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

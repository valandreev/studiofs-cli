use crate::cli::utils;
use crate::investigation::{
    BuildIdentity, DiagnosticsSettingsFingerprint, EnvironmentFingerprint, InvestigationCase,
    InvestigationRun, InvestigationStore, MachineFingerprint, NewInvestigationCase,
    NewInvestigationRun, NewTimelineEntry, PlatformFingerprint, RuntimeConfigurationFingerprint,
    TimelineEntry, TimelineEntryKind, TimelineEntrySource, WorkloadIdentityFingerprint,
};
use anyhow::{Context, Result, anyhow};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use sha2::{Digest, Sha256};
use std::fs;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use studiofs_meta::FormatInfo;
use studiofs_vfs::vfs::VfsFileSessionStats;
use studiofs_vfs::{
    StudioVfs, VfsForegroundReadProtectionStats, VfsIngressStats, VfsUploadStats,
    VfsWritePathObservabilitySnapshot,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tracing::warn;
use uuid::Uuid;

#[cfg(windows)]
use std::{ffi::c_void, mem::size_of};
#[cfg(windows)]
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
#[cfg(windows)]
use windows::{
    Win32::{
        Foundation::{HLOCAL, LocalFree},
        Security::Authorization::{
            ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
        },
        Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES},
    },
    core::w,
};

const DIAGNOSTICS_PROTOCOL_VERSION: u32 = 1;
const DEFAULT_LEASE_TTL_MS: u64 = 5_000;
const MAX_LEASE_TTL_MS: u64 = 60_000;
const CONNECT_RETRY_ATTEMPTS: usize = 40;
const CONNECT_RETRY_DELAY_MS: u64 = 25;
const MAX_DIAGNOSTICS_REQUEST_FRAME_BYTES: usize = 64 * 1024;
const MAX_DIAGNOSTICS_RESPONSE_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_DIAGNOSTICS_CONNECTIONS: usize = 16;
#[cfg(unix)]
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;

#[cfg(test)]
static TEST_START_FAILURE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub(crate) enum LocalDiagnosticsEndpoint {
    #[cfg(unix)]
    UnixSocket { path: String },
    #[cfg(windows)]
    WindowsNamedPipe { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LocalDiagnosticsDiscovery {
    pub protocol_version: u32,
    pub session_id: String,
    pub volume: String,
    pub volume_uuid: String,
    #[serde(default)]
    pub bridge: String,
    pub mount_point: String,
    pub pid: u32,
    pub cache_path: String,
    pub read_only: bool,
    pub started_unix_ms: u64,
    pub endpoint: LocalDiagnosticsEndpoint,
    pub auth_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ControllerLease {
    pub lease_id: String,
    pub client_name: String,
    pub ttl_ms: u64,
    pub expires_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsSessionInfo {
    pub protocol_version: u32,
    pub session_id: String,
    pub volume: String,
    pub volume_uuid: String,
    #[serde(default)]
    pub bridge: String,
    pub mount_point: String,
    pub pid: u32,
    pub cache_path: String,
    pub read_only: bool,
    pub started_unix_ms: u64,
    pub controller: Option<ControllerLease>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsHealthSnapshot {
    pub generated_unix_ms: u64,
    pub ingress: VfsIngressStats,
    pub upload: VfsUploadStats,
    pub file_sessions: VfsFileSessionStats,
    pub foreground_reads: VfsForegroundReadProtectionStats,
    pub write_path: VfsWritePathObservabilitySnapshot,
}

pub(crate) trait DiagnosticsDataSource: Send + Sync + 'static {
    fn session_info(&self) -> DiagnosticsSessionInfo;
    fn health_snapshot(&self) -> DiagnosticsHealthSnapshot;
    fn environment_fingerprint(
        &self,
        case_workload_identity: Option<&str>,
        run_workload_identity: Option<&str>,
    ) -> EnvironmentFingerprint;
}

#[derive(Debug, Clone)]
pub(crate) struct LiveMountRuntimeFingerprint {
    pub bridge: String,
    pub cache_size: Option<String>,
    pub endpoint_override: Option<String>,
    pub snapshot_id: Option<String>,
    pub vfs_config: serde_json::Value,
}

pub(crate) struct LiveMountDiagnosticsSource {
    identity: DiagnosticsSessionInfo,
    base_fingerprint: EnvironmentFingerprint,
    vfs: Arc<StudioVfs>,
}

impl LiveMountDiagnosticsSource {
    pub(crate) fn new(
        format_info: &FormatInfo,
        mount_point: &str,
        cache_path: &Path,
        pid: u32,
        read_only: bool,
        runtime_fingerprint: LiveMountRuntimeFingerprint,
        vfs: Arc<StudioVfs>,
    ) -> Self {
        let session_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:{mount_point}:{pid}", format_info.uuid).as_bytes(),
        )
        .to_string();
        let identity = DiagnosticsSessionInfo {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            session_id,
            volume: format_info.name.clone(),
            volume_uuid: format_info.uuid.clone(),
            bridge: runtime_fingerprint.bridge.clone(),
            mount_point: mount_point.to_string(),
            pid,
            cache_path: cache_path.to_string_lossy().into_owned(),
            read_only,
            started_unix_ms: now_unix_millis(),
            controller: None,
        };
        let base_fingerprint = base_environment_fingerprint(&identity, &runtime_fingerprint);
        Self {
            identity,
            base_fingerprint,
            vfs,
        }
    }
}

impl DiagnosticsDataSource for LiveMountDiagnosticsSource {
    fn session_info(&self) -> DiagnosticsSessionInfo {
        self.identity.clone()
    }

    fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
        DiagnosticsHealthSnapshot {
            generated_unix_ms: now_unix_millis(),
            ingress: self.vfs.ingress_stats(),
            upload: self.vfs.upload_stats(),
            file_sessions: self.vfs.file_session_stats(),
            foreground_reads: self.vfs.foreground_read_protection_stats(),
            write_path: self.vfs.write_path_observability_snapshot(),
        }
    }

    fn environment_fingerprint(
        &self,
        case_workload_identity: Option<&str>,
        run_workload_identity: Option<&str>,
    ) -> EnvironmentFingerprint {
        let mut fingerprint = self.base_fingerprint.clone();
        fingerprint.workload_identity = WorkloadIdentityFingerprint {
            case_workload: case_workload_identity.map(str::to_string),
            run_workload: run_workload_identity.map(str::to_string),
        };
        fingerprint
    }
}

fn base_environment_fingerprint(
    identity: &DiagnosticsSessionInfo,
    runtime_fingerprint: &LiveMountRuntimeFingerprint,
) -> EnvironmentFingerprint {
    EnvironmentFingerprint {
        platform: PlatformFingerprint {
            os: std::env::consts::OS.to_string(),
            family: std::env::consts::FAMILY.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        },
        bridge: identity.bridge.clone(),
        build: BuildIdentity {
            package_name: env!("CARGO_PKG_NAME").to_string(),
            package_version: env!("CARGO_PKG_VERSION").to_string(),
            executable_path: std::env::current_exe()
                .ok()
                .map(|path| path.to_string_lossy().into_owned()),
            debug_build: cfg!(debug_assertions),
        },
        runtime_configuration: RuntimeConfigurationFingerprint {
            volume: identity.volume.clone(),
            volume_uuid: identity.volume_uuid.clone(),
            mount_point: identity.mount_point.clone(),
            cache_path: identity.cache_path.clone(),
            read_only: identity.read_only,
            cache_size: runtime_fingerprint.cache_size.clone(),
            endpoint_override: runtime_fingerprint.endpoint_override.clone(),
            snapshot_id: runtime_fingerprint.snapshot_id.clone(),
            vfs_config: runtime_fingerprint.vfs_config.clone(),
        },
        workload_identity: WorkloadIdentityFingerprint {
            case_workload: None,
            run_workload: None,
        },
        machine: static_machine_fingerprint(),
        diagnostics: DiagnosticsSettingsFingerprint {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            default_lease_ttl_ms: DEFAULT_LEASE_TTL_MS,
            max_lease_ttl_ms: MAX_LEASE_TTL_MS,
            max_connections: MAX_DIAGNOSTICS_CONNECTIONS,
        },
    }
}

fn static_machine_fingerprint() -> MachineFingerprint {
    static MACHINE_FINGERPRINT: OnceLock<MachineFingerprint> = OnceLock::new();
    MACHINE_FINGERPRINT
        .get_or_init(|| {
            let system = sysinfo::System::new_all();
            MachineFingerprint {
                host_name: sysinfo::System::host_name(),
                system_name: sysinfo::System::name(),
                kernel_version: sysinfo::System::kernel_version(),
                os_version: sysinfo::System::os_version(),
                total_memory_bytes: system.total_memory(),
                cpu_count: system.cpus().len(),
            }
        })
        .clone()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DiagnosticsRequest {
    SessionInfo {
        auth_token: String,
    },
    Health {
        auth_token: String,
    },
    AcquireController {
        auth_token: String,
        client_name: String,
        ttl_ms: Option<u64>,
    },
    RenewController {
        auth_token: String,
        lease_id: String,
        ttl_ms: Option<u64>,
    },
    CreateCase {
        auth_token: String,
        lease_id: String,
        title: String,
        problem_statement: Option<String>,
        workload_identity: Option<String>,
        target_environment: Option<String>,
    },
    CreateRun {
        auth_token: String,
        lease_id: String,
        case_id: String,
        run_workload_identity: Option<String>,
    },
    AppendTimelineEntry {
        auth_token: String,
        lease_id: String,
        run_id: String,
        source: TimelineEntrySource,
        kind: TimelineEntryKind,
        title: String,
        body: Option<String>,
        actor: Option<String>,
        reason: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DiagnosticsResponse {
    SessionInfo {
        session: DiagnosticsSessionInfo,
    },
    Health {
        health: Box<DiagnosticsHealthSnapshot>,
    },
    ControllerLease {
        lease: ControllerLease,
    },
    InvestigationCase {
        case: Box<InvestigationCase>,
    },
    InvestigationRun {
        run: Box<InvestigationRun>,
    },
    TimelineEntry {
        entry: TimelineEntry,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Default)]
struct ControllerLeaseManager {
    state: Mutex<Option<ActiveControllerLease>>,
}

#[derive(Debug, Clone)]
struct ActiveControllerLease {
    lease: ControllerLease,
    expires_at: Instant,
}

impl ControllerLeaseManager {
    fn snapshot(&self) -> Option<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        state.as_ref().map(|lease| lease.lease.clone())
    }

    fn acquire(&self, client_name: String, ttl_ms: Option<u64>) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        if let Some(active) = state.as_ref() {
            return Err(anyhow!(
                "controller lease already held by '{}' until {}",
                active.lease.client_name,
                active.lease.expires_unix_ms
            ));
        }
        let ttl_ms = ttl_ms
            .unwrap_or(DEFAULT_LEASE_TTL_MS)
            .clamp(1, MAX_LEASE_TTL_MS);
        let lease = ControllerLease {
            lease_id: Uuid::new_v4().to_string(),
            client_name,
            ttl_ms,
            expires_unix_ms: now_unix_millis().saturating_add(ttl_ms),
        };
        *state = Some(ActiveControllerLease {
            lease: lease.clone(),
            expires_at: now + Duration::from_millis(ttl_ms),
        });
        Ok(lease)
    }

    fn renew(&self, lease_id: &str, ttl_ms: Option<u64>) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        let active = state
            .as_mut()
            .ok_or_else(|| anyhow!("controller lease is not currently held"))?;
        if active.lease.lease_id != lease_id {
            return Err(anyhow!("controller lease '{lease_id}' is not current"));
        }
        active.lease.ttl_ms = ttl_ms
            .unwrap_or(active.lease.ttl_ms)
            .clamp(1, MAX_LEASE_TTL_MS);
        active.expires_at = now + Duration::from_millis(active.lease.ttl_ms);
        active.lease.expires_unix_ms = now_unix_millis().saturating_add(active.lease.ttl_ms);
        Ok(active.lease.clone())
    }

    fn require_current(&self, lease_id: &str) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        let active = state
            .as_ref()
            .ok_or_else(|| anyhow!("controller lease is not currently held"))?;
        if active.lease.lease_id != lease_id {
            return Err(anyhow!("controller lease '{lease_id}' is not current"));
        }
        Ok(active.lease.clone())
    }

    fn expire_if_needed(state: &mut Option<ActiveControllerLease>, now: Instant) {
        if state.as_ref().is_some_and(|lease| lease.expires_at <= now) {
            *state = None;
        }
    }
}

pub(crate) struct LocalDiagnosticsHost {
    #[cfg(unix)]
    endpoint_path: PathBuf,
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl LocalDiagnosticsHost {
    pub(crate) fn start(
        runtime: &Handle,
        mount_point: &str,
        source: Arc<dyn DiagnosticsDataSource>,
    ) -> Result<Self> {
        #[cfg(test)]
        if let Some(message) = TEST_START_FAILURE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .clone()
        {
            return Err(anyhow!(message));
        }

        let mounts_dir = utils::get_mounts_dir()?;
        let base_name = utils::sanitize_mount_point(mount_point);
        let endpoint = endpoint_for(&mounts_dir, &base_name, source.session_info().pid);
        #[cfg(unix)]
        let endpoint_path = bind_unix_listener_path(&endpoint)?;
        let discovery = discovery_from_source(source.session_info(), endpoint.clone());
        publish_discovery(mount_point, &discovery)?;

        let lease_manager = Arc::new(ControllerLeaseManager::default());
        let investigation_store = match InvestigationStore::default_store() {
            Ok(store) => Some(Arc::new(store)),
            Err(error) => {
                warn!(
                    mount_point,
                    error = %error,
                    "investigation store unavailable; run metadata will not persist"
                );
                None
            }
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = runtime.spawn(run_server(
            endpoint,
            discovery.auth_token.clone(),
            source,
            lease_manager,
            investigation_store,
            shutdown_rx,
        ));

        Ok(Self {
            #[cfg(unix)]
            endpoint_path,
            shutdown_tx,
            task,
        })
    }

    pub(crate) fn start_best_effort(
        runtime: &Handle,
        mount_point: &str,
        source: Arc<dyn DiagnosticsDataSource>,
    ) -> Option<Self> {
        match Self::start(runtime, mount_point, source) {
            Ok(host) => Some(host),
            Err(error) => {
                warn!(
                    mount_point,
                    error = %error,
                    "diagnostics host unavailable; continuing without live attach"
                );
                None
            }
        }
    }

    pub(crate) fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        self.task.abort();
        #[cfg(unix)]
        let _ = fs::remove_file(self.endpoint_path);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LocalDiagnosticsClient {
    discovery: LocalDiagnosticsDiscovery,
}

impl LocalDiagnosticsClient {
    pub(crate) fn new(discovery: LocalDiagnosticsDiscovery) -> Self {
        Self { discovery }
    }

    pub(crate) async fn session_info(&self) -> Result<DiagnosticsSessionInfo> {
        match self
            .send(DiagnosticsRequest::SessionInfo {
                auth_token: self.discovery.auth_token.clone(),
            })
            .await?
        {
            DiagnosticsResponse::SessionInfo { session } => Ok(session),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn health(&self) -> Result<DiagnosticsHealthSnapshot> {
        match self
            .send(DiagnosticsRequest::Health {
                auth_token: self.discovery.auth_token.clone(),
            })
            .await?
        {
            DiagnosticsResponse::Health { health } => Ok(*health),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn acquire_controller(
        &self,
        client_name: &str,
        ttl: Duration,
    ) -> Result<ControllerLease> {
        match self
            .send(DiagnosticsRequest::AcquireController {
                auth_token: self.discovery.auth_token.clone(),
                client_name: client_name.to_string(),
                ttl_ms: Some(ttl.as_millis().try_into().unwrap_or(MAX_LEASE_TTL_MS)),
            })
            .await?
        {
            DiagnosticsResponse::ControllerLease { lease } => Ok(lease),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn renew_controller(
        &self,
        lease_id: &str,
        ttl: Duration,
    ) -> Result<ControllerLease> {
        match self
            .send(DiagnosticsRequest::RenewController {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                ttl_ms: Some(ttl.as_millis().try_into().unwrap_or(MAX_LEASE_TTL_MS)),
            })
            .await?
        {
            DiagnosticsResponse::ControllerLease { lease } => Ok(lease),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn create_case(
        &self,
        lease_id: &str,
        title: &str,
        problem_statement: Option<String>,
        workload_identity: Option<String>,
        target_environment: Option<String>,
    ) -> Result<InvestigationCase> {
        match self
            .send(DiagnosticsRequest::CreateCase {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                title: title.to_string(),
                problem_statement,
                workload_identity,
                target_environment,
            })
            .await?
        {
            DiagnosticsResponse::InvestigationCase { case } => Ok(*case),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn create_run(
        &self,
        lease_id: &str,
        case_id: &str,
        run_workload_identity: Option<String>,
    ) -> Result<InvestigationRun> {
        match self
            .send(DiagnosticsRequest::CreateRun {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                case_id: case_id.to_string(),
                run_workload_identity,
            })
            .await?
        {
            DiagnosticsResponse::InvestigationRun { run } => Ok(*run),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn append_timeline_entry(
        &self,
        lease_id: &str,
        run_id: &str,
        entry: NewTimelineEntry,
    ) -> Result<TimelineEntry> {
        match self
            .send(DiagnosticsRequest::AppendTimelineEntry {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                run_id: run_id.to_string(),
                source: entry.source,
                kind: entry.kind,
                title: entry.title,
                body: entry.body,
                actor: entry.actor,
                reason: entry.reason,
            })
            .await?
        {
            DiagnosticsResponse::TimelineEntry { entry } => Ok(entry),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    async fn send(&self, request: DiagnosticsRequest) -> Result<DiagnosticsResponse> {
        #[cfg(unix)]
        {
            let LocalDiagnosticsEndpoint::UnixSocket { path } = &self.discovery.endpoint;
            let stream = connect_unix_with_retry(path).await?;
            send_request(stream, request).await
        }

        #[cfg(windows)]
        {
            let LocalDiagnosticsEndpoint::WindowsNamedPipe { name } = &self.discovery.endpoint;
            let stream = connect_named_pipe_with_retry(name).await?;
            send_request(stream, request).await
        }
    }
}

pub(crate) fn discover_local_sessions() -> Result<Vec<LocalDiagnosticsDiscovery>> {
    discover_sessions_in(&utils::get_mounts_dir()?, utils::is_process_running)
}

fn discover_sessions_in(
    dir: &Path,
    is_process_running: impl Fn(u32) -> bool,
) -> Result<Vec<LocalDiagnosticsDiscovery>> {
    let mut sessions = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if !file_type.is_file() {
            continue;
        }
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = match fs::read(entry.path()) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let record = match serde_json::from_slice::<MountRecord>(&bytes) {
            Ok(record) => record,
            Err(_) => continue,
        };
        let Some(discovery) = record.diagnostics else {
            continue;
        };
        if is_process_running(discovery.pid) {
            sessions.push(discovery);
        }
    }
    sessions.sort_by(|left, right| left.mount_point.cmp(&right.mount_point));
    Ok(sessions)
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct MountRecord {
    pid: u32,
    mount_point: String,
    volume: String,
    #[serde(default)]
    cache_path: Option<String>,
    #[serde(default)]
    diagnostics: Option<LocalDiagnosticsDiscovery>,
}

fn publish_discovery(mount_point: &str, discovery: &LocalDiagnosticsDiscovery) -> Result<()> {
    let record_path = utils::pid_file_path(mount_point)?;
    if let Some(parent) = record_path.parent() {
        ensure_owner_only_directory(parent)?;
    }
    let bytes = fs::read(&record_path).with_context(|| {
        format!(
            "read mount record '{}' before publishing diagnostics",
            record_path.display()
        )
    })?;
    let mut record: MountRecord = serde_json::from_slice(&bytes)?;
    record.diagnostics = Some(discovery.clone());
    fs::write(&record_path, serde_json::to_vec_pretty(&record)?)?;
    ensure_owner_only_file(&record_path)?;
    Ok(())
}

fn discovery_from_source(
    session: DiagnosticsSessionInfo,
    endpoint: LocalDiagnosticsEndpoint,
) -> LocalDiagnosticsDiscovery {
    LocalDiagnosticsDiscovery {
        protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
        session_id: session.session_id,
        volume: session.volume,
        volume_uuid: session.volume_uuid,
        bridge: session.bridge,
        mount_point: session.mount_point,
        pid: session.pid,
        cache_path: session.cache_path,
        read_only: session.read_only,
        started_unix_ms: session.started_unix_ms,
        endpoint,
        auth_token: Uuid::new_v4().to_string(),
    }
}

fn endpoint_for(_mounts_dir: &Path, base_name: &str, pid: u32) -> LocalDiagnosticsEndpoint {
    #[cfg(unix)]
    {
        let socket_path = unix_socket_path(_mounts_dir, base_name, pid);
        LocalDiagnosticsEndpoint::UnixSocket {
            path: socket_path.to_string_lossy().into_owned(),
        }
    }

    #[cfg(windows)]
    {
        LocalDiagnosticsEndpoint::WindowsNamedPipe {
            name: format!(r"\\.\pipe\studiofs-{base_name}-{pid}-diagnostics"),
        }
    }
}

#[cfg(unix)]
fn unix_socket_path(mounts_dir: &Path, base_name: &str, pid: u32) -> PathBuf {
    let preferred = mounts_dir.join(format!("{base_name}.{pid}.sock"));
    if preferred.as_os_str().as_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
        return preferred;
    }

    let digest = Sha256::digest(base_name.as_bytes());
    mounts_dir.join(format!("sfsdiag-{pid}-{}.sock", hex::encode(&digest[..8])))
}

fn now_unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn bind_unix_listener_path(endpoint: &LocalDiagnosticsEndpoint) -> Result<PathBuf> {
    let LocalDiagnosticsEndpoint::UnixSocket { path } = endpoint;
    let path = PathBuf::from(path);
    if path.as_os_str().as_bytes().len() > MAX_UNIX_SOCKET_PATH_BYTES {
        return Err(anyhow!(
            "diagnostics socket path '{}' exceeds unix socket limit",
            path.display()
        ));
    }
    if path.exists() {
        fs::remove_file(&path)
            .with_context(|| format!("remove stale diagnostics socket '{}'", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        ensure_owner_only_directory(parent)?;
    }
    Ok(path)
}

async fn run_server(
    endpoint: LocalDiagnosticsEndpoint,
    auth_token: String,
    source: Arc<dyn DiagnosticsDataSource>,
    lease_manager: Arc<ControllerLeaseManager>,
    investigation_store: Option<Arc<InvestigationStore>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let connection_limit = Arc::new(Semaphore::new(MAX_DIAGNOSTICS_CONNECTIONS));

    #[cfg(unix)]
    {
        let LocalDiagnosticsEndpoint::UnixSocket { path } = &endpoint;
        let listener = match UnixListener::bind(path) {
            Ok(listener) => listener,
            Err(error) => {
                warn!(path, error = %error, "failed to bind diagnostics socket");
                return;
            }
        };
        if let Err(error) = fs::set_permissions(path, fs::Permissions::from_mode(0o600)) {
            warn!(path, error = %error, "failed to restrict diagnostics socket permissions");
            let _ = fs::remove_file(path);
            return;
        }
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        break;
                    }
                }
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, _)) => {
                            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                                warn!(path, limit = MAX_DIAGNOSTICS_CONNECTIONS, "diagnostics connection limit reached");
                                drop(stream);
                                continue;
                            };
                            let auth_token = auth_token.clone();
                            let source = source.clone();
                            let lease_manager = lease_manager.clone();
                            let investigation_store = investigation_store.clone();
                            tokio::spawn(async move {
                                if let Err(error) = handle_client(stream, auth_token, source, lease_manager, investigation_store, permit).await {
                                    warn!(error = %error, "diagnostics request failed");
                                }
                            });
                        }
                        Err(error) => {
                            warn!(path, error = %error, "diagnostics accept failed");
                            break;
                        }
                    }
                }
            }
        }
    }

    #[cfg(windows)]
    {
        let LocalDiagnosticsEndpoint::WindowsNamedPipe { name } = &endpoint;
        let pipe_factory = match WindowsPipeFactory::new(name) {
            Ok(factory) => factory,
            Err(error) => {
                warn!(name, error = %error, "failed to configure diagnostics pipe security");
                return;
            }
        };
        let mut server = match pipe_factory.create_server(true) {
            Ok(server) => server,
            Err(error) => {
                warn!(name, error = %error, "failed to create diagnostics pipe");
                return;
            }
        };
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        break;
                    }
                }
                connect = server.connect() => {
                    match connect {
                        Ok(()) => {
                            let next_server = match pipe_factory.create_server(false) {
                                Ok(server) => server,
                                Err(error) => {
                                    warn!(name, error = %error, "failed to create diagnostics pipe");
                                    break;
                                }
                            };
                            let connected_server = server;
                            server = next_server;
                            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                                warn!(name, limit = MAX_DIAGNOSTICS_CONNECTIONS, "diagnostics connection limit reached");
                                drop(connected_server);
                                continue;
                            };
                            let auth_token = auth_token.clone();
                            let source = source.clone();
                            let lease_manager = lease_manager.clone();
                            let investigation_store = investigation_store.clone();
                            tokio::spawn(async move {
                                if let Err(error) = handle_client(connected_server, auth_token, source, lease_manager, investigation_store, permit).await {
                                    warn!(error = %error, "diagnostics request failed");
                                }
                            });
                        }
                        Err(error) => {
                            warn!(name, error = %error, "diagnostics pipe connect failed");
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(unix)]
async fn connect_unix_with_retry(path: &str) -> Result<UnixStream> {
    let mut last_error = None;
    for _ in 0..CONNECT_RETRY_ATTEMPTS {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(stream),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(CONNECT_RETRY_DELAY_MS)).await;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("connect diagnostics socket '{path}'"));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "diagnostics socket did not become ready in time",
        )
    }))
    .with_context(|| format!("connect diagnostics socket '{path}'"))
}

#[cfg(windows)]
async fn connect_named_pipe_with_retry(
    name: &str,
) -> Result<tokio::net::windows::named_pipe::NamedPipeClient> {
    use tokio::net::windows::named_pipe::ClientOptions;

    let mut last_error = None;
    for _ in 0..CONNECT_RETRY_ATTEMPTS {
        match ClientOptions::new().open(name) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(CONNECT_RETRY_DELAY_MS)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "diagnostics pipe did not become ready in time",
        )
    }))
    .with_context(|| format!("connect diagnostics pipe '{name}'"))
}

#[cfg(test)]
pub(crate) fn set_test_start_failure(message: Option<&str>) {
    *TEST_START_FAILURE.get_or_init(|| Mutex::new(None)).lock() = message.map(str::to_string);
}

async fn handle_client<S>(
    stream: S,
    auth_token: String,
    source: Arc<dyn DiagnosticsDataSource>,
    lease_manager: Arc<ControllerLeaseManager>,
    investigation_store: Option<Arc<InvestigationStore>>,
    _permit: OwnedSemaphorePermit,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    loop {
        let line = match read_frame_line(&mut reader, MAX_DIAGNOSTICS_REQUEST_FRAME_BYTES).await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(FrameReadError::TooLarge) => {
                let response = DiagnosticsResponse::Error {
                    code: "request_too_large".to_string(),
                    message: format!(
                        "diagnostics request exceeded {} bytes",
                        MAX_DIAGNOSTICS_REQUEST_FRAME_BYTES
                    ),
                };
                writer
                    .write_all(serde_json::to_string(&response)?.as_bytes())
                    .await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
                break;
            }
            Err(FrameReadError::Io(error)) => return Err(error.into()),
            Err(FrameReadError::Utf8(error)) => return Err(error.into()),
        };
        let request: DiagnosticsRequest = serde_json::from_str(line.trim_end())?;
        let response = execute_request(
            request,
            &auth_token,
            source.as_ref(),
            &lease_manager,
            investigation_store.as_deref(),
        );
        writer
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    Ok(())
}

enum FrameReadError {
    Io(std::io::Error),
    TooLarge,
    Utf8(std::string::FromUtf8Error),
}

impl From<std::io::Error> for FrameReadError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<std::string::FromUtf8Error> for FrameReadError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Self::Utf8(error)
    }
}

async fn read_frame_line<R>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> std::result::Result<Option<String>, FrameReadError>
where
    R: AsyncBufRead + Unpin,
{
    let mut buffer = Vec::new();

    loop {
        let (consumed, too_large, eof) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                (0, false, true)
            } else if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
                let take = newline + 1;
                if buffer.len() + take > max_frame_bytes {
                    (take, true, false)
                } else {
                    buffer.extend_from_slice(&available[..take]);
                    (take, false, false)
                }
            } else {
                let take = available.len();
                if buffer.len() + take > max_frame_bytes {
                    (take, true, false)
                } else {
                    buffer.extend_from_slice(available);
                    (take, false, false)
                }
            }
        };
        if eof {
            if buffer.is_empty() {
                return Ok(None);
            }
            break;
        }

        reader.consume(consumed);
        if too_large {
            return Err(FrameReadError::TooLarge);
        }
        if buffer.last() == Some(&b'\n') {
            break;
        }
    }

    while matches!(buffer.last(), Some(b'\n' | b'\r')) {
        buffer.pop();
    }

    Ok(Some(String::from_utf8(buffer)?))
}

fn execute_request(
    request: DiagnosticsRequest,
    auth_token: &str,
    source: &dyn DiagnosticsDataSource,
    lease_manager: &ControllerLeaseManager,
    investigation_store: Option<&InvestigationStore>,
) -> DiagnosticsResponse {
    fn unauthorized() -> DiagnosticsResponse {
        DiagnosticsResponse::Error {
            code: "unauthorized".to_string(),
            message: "diagnostics auth token did not match".to_string(),
        }
    }

    fn investigation_unavailable() -> DiagnosticsResponse {
        DiagnosticsResponse::Error {
            code: "investigation_unavailable".to_string(),
            message: "investigation persistence is unavailable for this session".to_string(),
        }
    }

    fn store_error(error: anyhow::Error) -> DiagnosticsResponse {
        DiagnosticsResponse::Error {
            code: "investigation_store_error".to_string(),
            message: error.to_string(),
        }
    }

    fn lease_error(error: anyhow::Error) -> DiagnosticsResponse {
        DiagnosticsResponse::Error {
            code: "lease_invalid".to_string(),
            message: error.to_string(),
        }
    }

    match request {
        DiagnosticsRequest::SessionInfo {
            auth_token: provided,
        } => {
            if provided != auth_token {
                return unauthorized();
            }
            let mut session = source.session_info();
            session.controller = lease_manager.snapshot();
            DiagnosticsResponse::SessionInfo { session }
        }
        DiagnosticsRequest::Health {
            auth_token: provided,
        } => {
            if provided != auth_token {
                return unauthorized();
            }
            DiagnosticsResponse::Health {
                health: Box::new(source.health_snapshot()),
            }
        }
        DiagnosticsRequest::AcquireController {
            auth_token: provided,
            client_name,
            ttl_ms,
        } => {
            if provided != auth_token {
                return unauthorized();
            }
            match lease_manager.acquire(client_name, ttl_ms) {
                Ok(lease) => DiagnosticsResponse::ControllerLease { lease },
                Err(error) => DiagnosticsResponse::Error {
                    code: "controller_busy".to_string(),
                    message: error.to_string(),
                },
            }
        }
        DiagnosticsRequest::RenewController {
            auth_token: provided,
            lease_id,
            ttl_ms,
        } => {
            if provided != auth_token {
                return unauthorized();
            }
            match lease_manager.renew(&lease_id, ttl_ms) {
                Ok(lease) => DiagnosticsResponse::ControllerLease { lease },
                Err(error) => DiagnosticsResponse::Error {
                    code: "lease_invalid".to_string(),
                    message: error.to_string(),
                },
            }
        }
        DiagnosticsRequest::CreateCase {
            auth_token: provided,
            lease_id,
            title,
            problem_statement,
            workload_identity,
            target_environment,
        } => {
            if provided != auth_token {
                return unauthorized();
            }
            if let Err(error) = lease_manager.require_current(&lease_id) {
                return lease_error(error);
            }
            let Some(store) = investigation_store else {
                return investigation_unavailable();
            };
            match store.create_case(NewInvestigationCase {
                title,
                problem_statement,
                workload_identity,
                target_environment,
            }) {
                Ok(case) => DiagnosticsResponse::InvestigationCase {
                    case: Box::new(case),
                },
                Err(error) => store_error(error),
            }
        }
        DiagnosticsRequest::CreateRun {
            auth_token: provided,
            lease_id,
            case_id,
            run_workload_identity,
        } => {
            if provided != auth_token {
                return unauthorized();
            }
            if let Err(error) = lease_manager.require_current(&lease_id) {
                return lease_error(error);
            }
            let Some(store) = investigation_store else {
                return investigation_unavailable();
            };
            let case = match store.load_case(&case_id) {
                Ok(case) => case,
                Err(error) => return store_error(error),
            };
            let fingerprint = source.environment_fingerprint(
                case.workload_identity.as_deref(),
                run_workload_identity.as_deref(),
            );
            match store.create_run(NewInvestigationRun {
                case_id,
                session_id: source.session_info().session_id,
                fingerprint,
            }) {
                Ok(run) => DiagnosticsResponse::InvestigationRun { run: Box::new(run) },
                Err(error) => store_error(error),
            }
        }
        DiagnosticsRequest::AppendTimelineEntry {
            auth_token: provided,
            lease_id,
            run_id,
            source,
            kind,
            title,
            body,
            actor,
            reason,
        } => {
            if provided != auth_token {
                return unauthorized();
            }
            if let Err(error) = lease_manager.require_current(&lease_id) {
                return lease_error(error);
            }
            let Some(store) = investigation_store else {
                return investigation_unavailable();
            };
            match store.append_timeline_entry(
                &run_id,
                NewTimelineEntry {
                    source,
                    kind,
                    title,
                    body,
                    actor,
                    reason,
                },
            ) {
                Ok(entry) => DiagnosticsResponse::TimelineEntry { entry },
                Err(error) => store_error(error),
            }
        }
    }
}

async fn send_request<S>(stream: S, request: DiagnosticsRequest) -> Result<DiagnosticsResponse>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    writer
        .write_all(serde_json::to_string(&request)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    let line = match read_frame_line(&mut reader, MAX_DIAGNOSTICS_RESPONSE_FRAME_BYTES).await {
        Ok(Some(line)) => line,
        Ok(None) => return Err(anyhow!("diagnostics peer closed without responding")),
        Err(FrameReadError::TooLarge) => {
            return Err(anyhow!(
                "diagnostics response exceeded {} bytes",
                MAX_DIAGNOSTICS_RESPONSE_FRAME_BYTES
            ));
        }
        Err(FrameReadError::Io(error)) => return Err(error.into()),
        Err(FrameReadError::Utf8(error)) => return Err(error.into()),
    };
    Ok(serde_json::from_str(line.trim_end())?)
}

#[cfg(unix)]
fn ensure_owner_only_directory(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restrict directory permissions for '{}'", path.display()))
}

#[cfg(not(unix))]
fn ensure_owner_only_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn ensure_owner_only_file(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict file permissions for '{}'", path.display()))
}

#[cfg(not(unix))]
fn ensure_owner_only_file(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
struct WindowsPipeFactory {
    name: String,
    security_descriptor: WindowsSecurityDescriptor,
}

#[cfg(windows)]
impl WindowsPipeFactory {
    fn new(name: &str) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            security_descriptor: WindowsSecurityDescriptor::same_user_only()?,
        })
    }

    fn create_server(&self, first_pipe_instance: bool) -> Result<NamedPipeServer> {
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(first_pipe_instance)
            .reject_remote_clients(true);

        let mut attrs = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.security_descriptor.as_ptr().0,
            bInheritHandle: false.into(),
        };

        unsafe {
            options.create_with_security_attributes_raw(
                &self.name,
                &mut attrs as *mut _ as *mut c_void,
            )
        }
        .with_context(|| format!("create diagnostics pipe '{}'", self.name))
    }
}

#[cfg(windows)]
struct WindowsSecurityDescriptor {
    descriptor: PSECURITY_DESCRIPTOR,
}

#[cfg(windows)]
impl WindowsSecurityDescriptor {
    fn same_user_only() -> Result<Self> {
        let mut descriptor = PSECURITY_DESCRIPTOR::default();
        unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                w!("D:P(A;;GA;;;OW)"),
                SDDL_REVISION_1,
                &mut descriptor,
                None,
            )
        }
        .context("build same-user security descriptor for diagnostics pipe")?;

        Ok(Self { descriptor })
    }

    fn as_ptr(&self) -> PSECURITY_DESCRIPTOR {
        self.descriptor
    }
}

#[cfg(windows)]
impl Drop for WindowsSecurityDescriptor {
    fn drop(&mut self) {
        unsafe {
            let _ = LocalFree(Some(HLOCAL(self.descriptor.0)));
        }
    }
}

#[cfg(windows)]
unsafe impl Send for WindowsSecurityDescriptor {}

#[cfg(windows)]
unsafe impl Sync for WindowsSecurityDescriptor {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[derive(Clone)]
    struct StaticDiagnosticsSource {
        session: DiagnosticsSessionInfo,
        health: DiagnosticsHealthSnapshot,
        fingerprint: EnvironmentFingerprint,
    }

    impl DiagnosticsDataSource for StaticDiagnosticsSource {
        fn session_info(&self) -> DiagnosticsSessionInfo {
            self.session.clone()
        }

        fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
            self.health.clone()
        }

        fn environment_fingerprint(
            &self,
            case_workload_identity: Option<&str>,
            run_workload_identity: Option<&str>,
        ) -> EnvironmentFingerprint {
            let mut fingerprint = self.fingerprint.clone();
            fingerprint.workload_identity.case_workload =
                case_workload_identity.map(str::to_string);
            fingerprint.workload_identity.run_workload = run_workload_identity.map(str::to_string);
            fingerprint
        }
    }

    fn static_source() -> StaticDiagnosticsSource {
        StaticDiagnosticsSource {
            session: DiagnosticsSessionInfo {
                protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
                session_id: "session-123".to_string(),
                volume: "demo".to_string(),
                volume_uuid: "uuid".to_string(),
                bridge: "WinFSP".to_string(),
                mount_point: "Q:".to_string(),
                pid: 42,
                cache_path: "C:/cache".to_string(),
                read_only: false,
                started_unix_ms: 123,
                controller: None,
            },
            health: DiagnosticsHealthSnapshot::default(),
            fingerprint: EnvironmentFingerprint {
                platform: PlatformFingerprint {
                    os: "windows".to_string(),
                    family: "windows".to_string(),
                    arch: "x86_64".to_string(),
                },
                bridge: "WinFSP".to_string(),
                build: BuildIdentity {
                    package_name: "studiofs".to_string(),
                    package_version: "0.1.1".to_string(),
                    executable_path: Some("C:/studiofs.exe".to_string()),
                    debug_build: true,
                },
                runtime_configuration: RuntimeConfigurationFingerprint {
                    volume: "demo".to_string(),
                    volume_uuid: "uuid".to_string(),
                    mount_point: "Q:".to_string(),
                    cache_path: "C:/cache".to_string(),
                    read_only: false,
                    cache_size: Some("64GB".to_string()),
                    endpoint_override: Some("http://127.0.0.1:9000".to_string()),
                    snapshot_id: None,
                    vfs_config: serde_json::json!({"principal":"*","cache_safety_mode":"fast"}),
                },
                workload_identity: WorkloadIdentityFingerprint {
                    case_workload: None,
                    run_workload: None,
                },
                machine: MachineFingerprint {
                    host_name: Some("workstation".to_string()),
                    system_name: Some("Windows".to_string()),
                    kernel_version: Some("10.0".to_string()),
                    os_version: Some("11".to_string()),
                    total_memory_bytes: 64 * 1024 * 1024 * 1024,
                    cpu_count: 24,
                },
                diagnostics: DiagnosticsSettingsFingerprint {
                    protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
                    default_lease_ttl_ms: DEFAULT_LEASE_TTL_MS,
                    max_lease_ttl_ms: MAX_LEASE_TTL_MS,
                    max_connections: MAX_DIAGNOSTICS_CONNECTIONS,
                },
            },
        }
    }

    fn mounts_dir_temp() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn write_mount_record(
        dir: &Path,
        mount_point: &str,
        discovery: Option<LocalDiagnosticsDiscovery>,
        pid: u32,
    ) {
        let path = dir.join(format!("{}.json", utils::sanitize_mount_point(mount_point)));
        let record = MountRecord {
            pid,
            mount_point: mount_point.to_string(),
            volume: "demo".to_string(),
            cache_path: Some("cache".to_string()),
            diagnostics: discovery,
        };
        fs::write(
            path,
            serde_json::to_vec_pretty(&record).expect("serialize record"),
        )
        .expect("write record");
    }

    #[test]
    fn discover_sessions_filters_to_live_diagnostics_records() {
        let dir = mounts_dir_temp();
        let discovery = LocalDiagnosticsDiscovery {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            session_id: "s1".to_string(),
            volume: "demo".to_string(),
            volume_uuid: "uuid".to_string(),
            bridge: "WinFSP".to_string(),
            mount_point: "X:".to_string(),
            pid: 123,
            cache_path: "cache".to_string(),
            read_only: false,
            started_unix_ms: 1,
            endpoint: endpoint_for(dir.path(), "x", 123),
            auth_token: "secret".to_string(),
        };
        write_mount_record(dir.path(), "X:", Some(discovery.clone()), 123);
        write_mount_record(dir.path(), "Y:", None, 456);

        let sessions = discover_sessions_in(dir.path(), |pid| pid == 123).expect("discover");

        assert_eq!(sessions, vec![discovery]);
    }

    #[test]
    fn discover_sessions_skips_malformed_records() {
        let dir = mounts_dir_temp();
        fs::write(dir.path().join("broken.json"), b"{not-json").expect("write malformed record");
        write_mount_record(dir.path(), "X:", None, 123);

        let sessions = discover_sessions_in(dir.path(), |_| true).expect("discover");

        assert!(sessions.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn endpoint_for_shortens_long_unix_socket_paths() {
        let dir = mounts_dir_temp();
        let base_name = "a".repeat(256);
        let LocalDiagnosticsEndpoint::UnixSocket { path } =
            endpoint_for(dir.path(), &base_name, 42);

        assert!(
            path.len()
                < dir
                    .path()
                    .join(format!("{base_name}.42.sock"))
                    .to_string_lossy()
                    .len()
        );
        assert!(Path::new(&path).as_os_str().as_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES);
    }

    #[test]
    fn mutating_requests_require_current_controller_lease() {
        let source = static_source();
        let lease_manager = ControllerLeaseManager::default();
        let temp = mounts_dir_temp();
        let store = InvestigationStore::new(temp.path().join("investigations"));

        let response = execute_request(
            DiagnosticsRequest::CreateCase {
                auth_token: "secret".to_string(),
                lease_id: "lease-1".to_string(),
                title: "AJA slowdown".to_string(),
                problem_statement: None,
                workload_identity: Some("aja".to_string()),
                target_environment: None,
            },
            "secret",
            &source,
            &lease_manager,
            Some(&store),
        );

        assert!(matches!(
            response,
            DiagnosticsResponse::Error { code, .. } if code == "lease_invalid"
        ));
    }

    #[test]
    fn mutating_requests_persist_run_fingerprint_and_timeline() {
        let source = static_source();
        let lease_manager = ControllerLeaseManager::default();
        let temp = mounts_dir_temp();
        let store = InvestigationStore::new(temp.path().join("investigations"));

        let lease = match execute_request(
            DiagnosticsRequest::AcquireController {
                auth_token: "secret".to_string(),
                client_name: "tester".to_string(),
                ttl_ms: Some(250),
            },
            "secret",
            &source,
            &lease_manager,
            Some(&store),
        ) {
            DiagnosticsResponse::ControllerLease { lease } => lease,
            other => panic!("unexpected lease response: {other:?}"),
        };

        let case = match execute_request(
            DiagnosticsRequest::CreateCase {
                auth_token: "secret".to_string(),
                lease_id: lease.lease_id.clone(),
                title: "AJA slowdown".to_string(),
                problem_statement: Some("Copy stalls after warmup".to_string()),
                workload_identity: Some("aja".to_string()),
                target_environment: Some("artist workstation".to_string()),
            },
            "secret",
            &source,
            &lease_manager,
            Some(&store),
        ) {
            DiagnosticsResponse::InvestigationCase { case } => *case,
            other => panic!("unexpected case response: {other:?}"),
        };

        let run = match execute_request(
            DiagnosticsRequest::CreateRun {
                auth_token: "secret".to_string(),
                lease_id: lease.lease_id.clone(),
                case_id: case.id.clone(),
                run_workload_identity: Some("copy-pass-1".to_string()),
            },
            "secret",
            &source,
            &lease_manager,
            Some(&store),
        ) {
            DiagnosticsResponse::InvestigationRun { run } => *run,
            other => panic!("unexpected run response: {other:?}"),
        };

        let _ = execute_request(
            DiagnosticsRequest::AppendTimelineEntry {
                auth_token: "secret".to_string(),
                lease_id: lease.lease_id.clone(),
                run_id: run.id.clone(),
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "start copy".to_string(),
                body: Some("Started the manual copy workload".to_string()),
                actor: Some("developer".to_string()),
                reason: Some("begin workload".to_string()),
            },
            "secret",
            &source,
            &lease_manager,
            Some(&store),
        );
        let _ = execute_request(
            DiagnosticsRequest::AppendTimelineEntry {
                auth_token: "secret".to_string(),
                lease_id: lease.lease_id,
                run_id: run.id.clone(),
                source: TimelineEntrySource::Mcp,
                kind: TimelineEntryKind::Note,
                title: "cache miss burst".to_string(),
                body: Some("MCP adapter observed a foreground read spike".to_string()),
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("annotate anomaly".to_string()),
            },
            "secret",
            &source,
            &lease_manager,
            Some(&store),
        );

        let persisted_run = store.load_run(&run.id).expect("load run");

        assert_eq!(persisted_run.case_id, case.id);
        assert_eq!(persisted_run.session_id, "session-123");
        assert_eq!(persisted_run.fingerprint.bridge, "WinFSP");
        assert_eq!(
            persisted_run
                .fingerprint
                .workload_identity
                .case_workload
                .as_deref(),
            Some("aja")
        );
        assert_eq!(
            persisted_run
                .fingerprint
                .workload_identity
                .run_workload
                .as_deref(),
            Some("copy-pass-1")
        );
        assert_eq!(persisted_run.timeline.len(), 2);
        assert_eq!(persisted_run.timeline[0].source, TimelineEntrySource::Cli);
        assert_eq!(persisted_run.timeline[1].source, TimelineEntrySource::Mcp);
    }

    #[test]
    fn client_accepts_large_response_frames() {
        let mut source = static_source();
        source.session.cache_path = "C:/".to_string() + &"a".repeat(96 * 1024);

        crate::runtime_bridge::block_on(async move {
            let (client, server) = tokio::io::duplex(256 * 1024);
            let permit = Arc::new(Semaphore::new(1))
                .acquire_owned()
                .await
                .map_err(anyhow::Error::from)?;
            let server_task = tokio::spawn(handle_client(
                server,
                "secret".to_string(),
                Arc::new(source),
                Arc::new(ControllerLeaseManager::default()),
                None,
                permit,
            ));

            let response = send_request(
                client,
                DiagnosticsRequest::SessionInfo {
                    auth_token: "secret".to_string(),
                },
            )
            .await?;

            match response {
                DiagnosticsResponse::SessionInfo { session } => {
                    assert!(session.cache_path.len() > MAX_DIAGNOSTICS_REQUEST_FRAME_BYTES);
                }
                other => panic!("unexpected response: {other:?}"),
            }

            server_task.await.map_err(anyhow::Error::from)??;
            Ok::<(), anyhow::Error>(())
        })
        .expect("large response exchange succeeds");
    }
}

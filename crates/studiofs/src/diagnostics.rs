use crate::cli::utils;
use anyhow::{Context, Result, anyhow};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use studiofs_meta::FormatInfo;
use studiofs_vfs::vfs::VfsFileSessionStats;
use studiofs_vfs::{
    StudioVfs, VfsForegroundReadProtectionStats, VfsIngressStats, VfsUploadStats,
    VfsWritePathObservabilitySnapshot,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tracing::warn;
use uuid::Uuid;

#[cfg(test)]
use std::sync::OnceLock;

const DIAGNOSTICS_PROTOCOL_VERSION: u32 = 1;
const DEFAULT_LEASE_TTL_MS: u64 = 5_000;
const MAX_LEASE_TTL_MS: u64 = 60_000;
const CONNECT_RETRY_ATTEMPTS: usize = 40;
const CONNECT_RETRY_DELAY_MS: u64 = 25;

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
}

pub(crate) struct LiveMountDiagnosticsSource {
    identity: DiagnosticsSessionInfo,
    vfs: Arc<StudioVfs>,
}

impl LiveMountDiagnosticsSource {
    pub(crate) fn new(
        format_info: &FormatInfo,
        mount_point: &str,
        cache_path: &Path,
        pid: u32,
        read_only: bool,
        vfs: Arc<StudioVfs>,
    ) -> Self {
        let session_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:{mount_point}:{pid}", format_info.uuid).as_bytes(),
        )
        .to_string();
        Self {
            identity: DiagnosticsSessionInfo {
                protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
                session_id,
                volume: format_info.name.clone(),
                volume_uuid: format_info.uuid.clone(),
                mount_point: mount_point.to_string(),
                pid,
                cache_path: cache_path.to_string_lossy().into_owned(),
                read_only,
                started_unix_ms: now_unix_millis(),
                controller: None,
            },
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
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Default)]
struct ControllerLeaseManager {
    state: Mutex<Option<ControllerLease>>,
}

impl ControllerLeaseManager {
    fn snapshot(&self) -> Option<ControllerLease> {
        let now = now_unix_millis();
        let mut state = self.state.lock();
        if state
            .as_ref()
            .is_some_and(|lease| lease.expires_unix_ms <= now)
        {
            *state = None;
        }
        state.clone()
    }

    fn acquire(&self, client_name: String, ttl_ms: Option<u64>) -> Result<ControllerLease> {
        let now = now_unix_millis();
        let mut state = self.state.lock();
        if state
            .as_ref()
            .is_some_and(|lease| lease.expires_unix_ms <= now)
        {
            *state = None;
        }
        if let Some(active) = state.as_ref() {
            return Err(anyhow!(
                "controller lease already held by '{}' until {}",
                active.client_name,
                active.expires_unix_ms
            ));
        }
        let ttl_ms = ttl_ms
            .unwrap_or(DEFAULT_LEASE_TTL_MS)
            .clamp(1, MAX_LEASE_TTL_MS);
        let lease = ControllerLease {
            lease_id: Uuid::new_v4().to_string(),
            client_name,
            ttl_ms,
            expires_unix_ms: now.saturating_add(ttl_ms),
        };
        *state = Some(lease.clone());
        Ok(lease)
    }

    fn renew(&self, lease_id: &str, ttl_ms: Option<u64>) -> Result<ControllerLease> {
        let now = now_unix_millis();
        let mut state = self.state.lock();
        if state
            .as_ref()
            .is_some_and(|lease| lease.expires_unix_ms <= now)
        {
            *state = None;
        }
        let lease = state
            .as_mut()
            .ok_or_else(|| anyhow!("controller lease is not currently held"))?;
        if lease.lease_id != lease_id {
            return Err(anyhow!("controller lease '{lease_id}' is not current"));
        }
        lease.ttl_ms = ttl_ms.unwrap_or(lease.ttl_ms).clamp(1, MAX_LEASE_TTL_MS);
        lease.expires_unix_ms = now.saturating_add(lease.ttl_ms);
        Ok(lease.clone())
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
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = runtime.spawn(run_server(
            endpoint,
            discovery.auth_token.clone(),
            source,
            lease_manager,
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
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = fs::read(entry.path())?;
        let Some(discovery) = serde_json::from_slice::<MountRecord>(&bytes)?.diagnostics else {
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
    let bytes = fs::read(&record_path).with_context(|| {
        format!(
            "read mount record '{}' before publishing diagnostics",
            record_path.display()
        )
    })?;
    let mut record: MountRecord = serde_json::from_slice(&bytes)?;
    record.diagnostics = Some(discovery.clone());
    fs::write(&record_path, serde_json::to_vec_pretty(&record)?)?;
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
        LocalDiagnosticsEndpoint::UnixSocket {
            path: _mounts_dir
                .join(format!("{base_name}.{pid}.sock"))
                .to_string_lossy()
                .into_owned(),
        }
    }

    #[cfg(windows)]
    {
        LocalDiagnosticsEndpoint::WindowsNamedPipe {
            name: format!(r"\\.\pipe\studiofs-{base_name}-{pid}-diagnostics"),
        }
    }
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
    if path.exists() {
        fs::remove_file(&path)
            .with_context(|| format!("remove stale diagnostics socket '{}'", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(path)
}

async fn run_server(
    endpoint: LocalDiagnosticsEndpoint,
    auth_token: String,
    source: Arc<dyn DiagnosticsDataSource>,
    lease_manager: Arc<ControllerLeaseManager>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
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
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
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
                            let auth_token = auth_token.clone();
                            let source = source.clone();
                            let lease_manager = lease_manager.clone();
                            tokio::spawn(async move {
                                if let Err(error) = handle_client(stream, auth_token, source, lease_manager).await {
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
        use tokio::net::windows::named_pipe::ServerOptions;

        let LocalDiagnosticsEndpoint::WindowsNamedPipe { name } = &endpoint;
        loop {
            let server = match ServerOptions::new().create(name) {
                Ok(server) => server,
                Err(error) => {
                    warn!(name, error = %error, "failed to create diagnostics pipe");
                    return;
                }
            };
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        break;
                    }
                }
                connect = server.connect() => {
                    match connect {
                        Ok(()) => {
                            let auth_token = auth_token.clone();
                            let source = source.clone();
                            let lease_manager = lease_manager.clone();
                            tokio::spawn(async move {
                                if let Err(error) = handle_client(server, auth_token, source, lease_manager).await {
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
    *TEST_START_FAILURE
        .get_or_init(|| Mutex::new(None))
        .lock() = message.map(str::to_string);
}

async fn handle_client<S>(
    stream: S,
    auth_token: String,
    source: Arc<dyn DiagnosticsDataSource>,
    lease_manager: Arc<ControllerLeaseManager>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line).await?;
        if read == 0 {
            break;
        }
        let request: DiagnosticsRequest = serde_json::from_str(line.trim_end())?;
        let response = execute_request(request, &auth_token, source.as_ref(), &lease_manager);
        writer
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    Ok(())
}

fn execute_request(
    request: DiagnosticsRequest,
    auth_token: &str,
    source: &dyn DiagnosticsDataSource,
    lease_manager: &ControllerLeaseManager,
) -> DiagnosticsResponse {
    fn unauthorized() -> DiagnosticsResponse {
        DiagnosticsResponse::Error {
            code: "unauthorized".to_string(),
            message: "diagnostics auth token did not match".to_string(),
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
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(line.trim_end())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
}

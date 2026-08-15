use super::*;

pub(crate) fn discover_local_sessions() -> Result<Vec<LocalDiagnosticsDiscovery>> {
    discover_sessions_in(&utils::get_mounts_dir()?, utils::mount_is_active)
}

pub(super) fn discover_sessions_in(
    dir: &Path,
    is_mount_active: impl Fn(&str) -> Result<bool>,
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
        if is_mount_active(&record.mount_point)? {
            sessions.push(discovery);
        }
    }
    sessions.sort_by(|left, right| left.mount_point.cmp(&right.mount_point));
    Ok(sessions)
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(super) struct MountRecord {
    #[serde(default)]
    pub(super) record_version: u32,
    pub(super) pid: u32,
    pub(super) mount_point: String,
    pub(super) volume: String,
    #[serde(default)]
    pub(super) cache_path: Option<String>,
    #[serde(default)]
    pub(super) instance_id: Option<String>,
    #[serde(default)]
    pub(super) diagnostics: Option<LocalDiagnosticsDiscovery>,
    #[serde(flatten)]
    pub(super) extra: BTreeMap<String, serde_json::Value>,
}

pub(super) fn publish_discovery(
    mount_point: &str,
    discovery: &LocalDiagnosticsDiscovery,
) -> Result<()> {
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

pub(super) fn discovery_from_source(
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

pub(super) fn endpoint_for(
    _mounts_dir: &Path,
    base_name: &str,
    pid: u32,
) -> LocalDiagnosticsEndpoint {
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
pub(super) fn unix_socket_path(mounts_dir: &Path, base_name: &str, pid: u32) -> PathBuf {
    let preferred = mounts_dir.join(format!("{base_name}.{pid}.sock"));
    if preferred.as_os_str().as_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
        return preferred;
    }

    let digest = Sha256::digest(base_name.as_bytes());
    let socket_name = format!("sfsdiag-{pid}-{}.sock", hex::encode(&digest[..8]));
    let temporary = std::env::temp_dir()
        .join("studiofs-diagnostics")
        .join(&socket_name);
    if temporary.as_os_str().as_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
        return temporary;
    }
    PathBuf::from("/tmp")
        .join("studiofs-diagnostics")
        .join(socket_name)
}

pub(super) fn now_unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(unix)]
pub(super) fn bind_unix_listener_path(endpoint: &LocalDiagnosticsEndpoint) -> Result<PathBuf> {
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

pub(super) async fn run_server(
    endpoint: LocalDiagnosticsEndpoint,
    auth_token: String,
    source: Arc<dyn DiagnosticsDataSource>,
    lease_manager: Arc<ControllerLeaseManager>,
    investigation_store: Option<Arc<InvestigationStore>>,
    profiling: Option<Arc<DiagnosticsProfilingCoordinator>>,
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
                            let profiling = profiling.clone();
                            tokio::spawn(async move {
                                if let Err(error) = handle_client(stream, auth_token, source, lease_manager, investigation_store, profiling, permit).await {
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
                            let profiling = profiling.clone();
                            tokio::spawn(async move {
                                if let Err(error) = handle_client(connected_server, auth_token, source, lease_manager, investigation_store, profiling, permit).await {
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
pub(super) async fn connect_unix_with_retry(path: &str) -> Result<UnixStream> {
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
pub(super) async fn connect_named_pipe_with_retry(
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

#[cfg(test)]
pub(crate) fn set_test_safe_action_audit_failure(message: Option<&str>) {
    *TEST_SAFE_ACTION_AUDIT_FAILURE
        .get_or_init(|| Mutex::new(None))
        .lock() = message.map(str::to_string);
}

pub(super) async fn handle_client<S>(
    stream: S,
    auth_token: String,
    source: Arc<dyn DiagnosticsDataSource>,
    lease_manager: Arc<ControllerLeaseManager>,
    investigation_store: Option<Arc<InvestigationStore>>,
    profiling: Option<Arc<DiagnosticsProfilingCoordinator>>,
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
        let auth_token_for_request = auth_token.clone();
        let source_for_request = source.clone();
        let lease_manager_for_request = lease_manager.clone();
        let investigation_store_for_request = investigation_store.clone();
        let profiling_for_request = profiling.clone();
        let response = tokio::task::spawn_blocking(move || {
            execute_request(
                request,
                &auth_token_for_request,
                source_for_request.as_ref(),
                &lease_manager_for_request,
                investigation_store_for_request.as_deref(),
                profiling_for_request.as_deref(),
            )
        })
        .await
        .map_err(|error| anyhow!("diagnostics request task failed: {error}"))?;
        writer
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    Ok(())
}

pub(super) enum FrameReadError {
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

pub(super) async fn read_frame_line<R>(
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

pub(super) fn execute_request(
    request: DiagnosticsRequest,
    auth_token: &str,
    source: &dyn DiagnosticsDataSource,
    lease_manager: &ControllerLeaseManager,
    investigation_store: Option<&InvestigationStore>,
    profiling: Option<&DiagnosticsProfilingCoordinator>,
) -> DiagnosticsResponse {
    let service = DiagnosticsSessionService::new(DiagnosticsSessionDeps {
        source,
        leases: lease_manager,
        store: investigation_store,
        profiling,
    });
    dispatch_protocol(&service, auth_token, request)
}

pub(super) async fn send_request<S>(
    stream: S,
    request: DiagnosticsRequest,
) -> Result<DiagnosticsResponse>
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
pub(super) fn ensure_owner_only_directory(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restrict directory permissions for '{}'", path.display()))
}

#[cfg(not(unix))]
pub(super) fn ensure_owner_only_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
pub(super) fn ensure_owner_only_file(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict file permissions for '{}'", path.display()))
}

#[cfg(not(unix))]
pub(super) fn ensure_owner_only_file(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
pub(super) struct WindowsPipeFactory {
    name: String,
    security_descriptor: WindowsSecurityDescriptor,
}

#[cfg(windows)]
impl WindowsPipeFactory {
    pub(super) fn new(name: &str) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            security_descriptor: WindowsSecurityDescriptor::same_user_only()?,
        })
    }

    pub(super) fn create_server(&self, first_pipe_instance: bool) -> Result<NamedPipeServer> {
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
pub(super) struct WindowsSecurityDescriptor {
    descriptor: PSECURITY_DESCRIPTOR,
}

#[cfg(windows)]
impl WindowsSecurityDescriptor {
    pub(super) fn same_user_only() -> Result<Self> {
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

    pub(super) fn as_ptr(&self) -> PSECURITY_DESCRIPTOR {
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

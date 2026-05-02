//! Multi-client process controller for mounted E2E scenarios.

use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use std::{
    collections::HashSet, collections::hash_map::DefaultHasher, ffi::OsString, hash::Hasher,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::infra::ServicePort;

pub const DEFAULT_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
#[cfg(windows)]
const DEFAULT_WINDOWS_MOUNT_LETTERS: &[char] = &['Z', 'Y', 'X', 'W'];
const MAX_COMMAND_OUTPUT_CHARS: usize = 4096;
const S3_BUCKET_NAME_MAX_CHARS: usize = 63;
const E2E_BUCKET_PREFIX: &str = "studiofs-e2e-";
const E2E_VOLUME_PREFIX: &str = "sfs-e2e-";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientControllerConfig {
    pub studiofs_bin: PathBuf,
    pub sandbox: PathBuf,
    pub volume_name: String,
    pub meta_dsn: String,
    pub s3: S3Config,
    pub mount_ready_timeout: Duration,
    pub mount_exit_timeout: Duration,
    pub poll_interval: Duration,
    pub mount_points: Vec<PathBuf>,
}

impl ClientControllerConfig {
    pub fn from_compose_services(
        studiofs_bin: PathBuf,
        sandbox: PathBuf,
        run_id: &str,
        seed: u64,
        ports: &[ServicePort],
    ) -> Result<Self, ClientControllerError> {
        let postgres = service_port(ports, "postgres")?;
        let rustfs = service_port(ports, "rustfs")?;
        let sanitized_run_id = sanitize_dns_label(run_id);
        let bucket = prefixed_dns_name(
            E2E_BUCKET_PREFIX,
            &sanitized_run_id,
            S3_BUCKET_NAME_MAX_CHARS,
        );
        let volume_stem = prefixed_dns_name(
            E2E_VOLUME_PREFIX,
            &format!("{sanitized_run_id}-{seed}"),
            S3_BUCKET_NAME_MAX_CHARS,
        );
        Ok(Self {
            studiofs_bin,
            sandbox,
            volume_name: volume_stem,
            meta_dsn: format!(
                "postgres://studiofs:studiofs@{}:{}",
                postgres.host, postgres.host_port
            ),
            s3: S3Config {
                endpoint: format!("http://{}:{}", rustfs.host, rustfs.host_port),
                bucket,
                access_key: "rustfsadmin".to_string(),
                secret_key: "rustfsadmin".to_string(),
                region: "us-east-1".to_string(),
            },
            mount_ready_timeout: Duration::from_secs(30),
            mount_exit_timeout: Duration::from_secs(15),
            poll_interval: Duration::from_millis(100),
            mount_points: Vec::new(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientLifecycleState {
    Planned,
    Initialized,
    Discovered,
    Mounted,
    Unmounted,
    Killed,
    Exited,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientSpec {
    pub name: String,
    pub client_root: PathBuf,
    pub local_metadata_dir: PathBuf,
    pub local_metadata_db: PathBuf,
    pub cache_dir: PathBuf,
    pub mount_metadata_dir: PathBuf,
    pub mount_point: PathBuf,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
    pub tracing_log: PathBuf,
    pub process_id: Option<u32>,
    pub state: ClientLifecycleState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCommand {
    pub client: String,
    pub action: String,
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub client_root: PathBuf,
    pub cache_dir: PathBuf,
    pub mount_point: PathBuf,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
    pub tracing_log: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCommandOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeBootstrapOptions {
    pub client_names: Vec<String>,
    pub clean_existing_local_metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientBootstrapRecord {
    pub name: String,
    pub local_metadata_db: PathBuf,
    pub state: ClientLifecycleState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeBootstrapSummary {
    pub volume_name: String,
    pub init_client: String,
    pub discovered_clients: Vec<String>,
    pub clients: Vec<ClientBootstrapRecord>,
}

pub trait ClientProcess {
    fn id(&self) -> u32;
    fn try_wait(&mut self) -> io::Result<Option<i32>>;
    fn kill(&mut self) -> io::Result<()>;
}

pub trait ClientProcessExecutor {
    fn run(&self, command: &ClientCommand) -> io::Result<ClientCommandOutput>;
    fn spawn(&self, command: &ClientCommand) -> io::Result<Box<dyn ClientProcess>>;
    fn is_mount_ready(&self, command: &ClientCommand) -> io::Result<bool>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClientProcessExecutor;

impl ClientProcessExecutor for SystemClientProcessExecutor {
    fn run(&self, command: &ClientCommand) -> io::Result<ClientCommandOutput> {
        let output = base_command(command).output()?;
        append_log(&command.stdout_log, &output.stdout)?;
        append_log(&command.stderr_log, &output.stderr)?;
        Ok(ClientCommandOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }

    fn spawn(&self, command: &ClientCommand) -> io::Result<Box<dyn ClientProcess>> {
        if let Some(parent) = command.stdout_log.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = command.stderr_log.parent() {
            fs::create_dir_all(parent)?;
        }
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&command.stdout_log)?;
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&command.stderr_log)?;
        let child = base_command(command)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        Ok(Box::new(SystemClientProcess { child }))
    }

    fn is_mount_ready(&self, command: &ClientCommand) -> io::Result<bool> {
        mount_point_is_active(&command.mount_point)
    }
}

struct SystemClientProcess {
    child: Child,
}

impl ClientProcess for SystemClientProcess {
    fn id(&self) -> u32 {
        self.child.id()
    }

    fn try_wait(&mut self) -> io::Result<Option<i32>> {
        Ok(self
            .child
            .try_wait()?
            .map(|status| status.code().unwrap_or(-1)))
    }

    fn kill(&mut self) -> io::Result<()> {
        self.child.kill()
    }
}

#[derive(Debug, Error)]
pub enum ClientControllerError {
    #[error("no E2E clients have been created")]
    NoClients,
    #[error("cannot replace E2E clients while mount processes are still tracked")]
    ClientsStillMounted,
    #[error("client name must be a safe single directory name: {0}")]
    UnsafeClientName(String),
    #[error("unknown E2E client: {0}")]
    UnknownClient(String),
    #[error("E2E client index {index} is out of range for {len} clients")]
    ClientIndexOutOfRange { index: usize, len: usize },
    #[error("Docker compose service port is missing for {0}")]
    MissingServicePort(String),
    #[error(
        "client command {action} for {client} failed with exit status {status}: stderr: {stderr}; stdout: {stdout}"
    )]
    CommandFailed {
        client: String,
        action: String,
        status: i32,
        stdout: String,
        stderr: String,
    },
    #[error("E2E client {client} did not create local metadata DB: {path}")]
    MissingLocalMetadataDb { client: String, path: PathBuf },
    #[error("E2E local metadata DB path is shared by multiple clients: {0}")]
    DuplicateLocalMetadataDb(PathBuf),
    #[error("failed to clean local metadata for E2E client {client} at {path}: {source}")]
    CleanLocalMetadataFailed {
        client: String,
        path: PathBuf,
        source: io::Error,
    },
    #[error("client controller I/O failed: {0}")]
    Io(#[from] io::Error),
}

pub struct MultiClientController<E> {
    config: ClientControllerConfig,
    executor: E,
    clients: Vec<ClientRuntime>,
}

impl<E: ClientProcessExecutor> MultiClientController<E> {
    pub fn new(config: ClientControllerConfig, executor: E) -> Self {
        Self {
            config,
            executor,
            clients: Vec::new(),
        }
    }

    pub fn create_clients<I, N>(&mut self, names: I) -> Result<(), ClientControllerError>
    where
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        if self.clients.iter().any(|runtime| runtime.process.is_some()) {
            return Err(ClientControllerError::ClientsStillMounted);
        }
        let names: Vec<String> = names
            .into_iter()
            .map(|name| name.as_ref().to_string())
            .collect();
        for name in &names {
            validate_client_name(name)?;
        }

        self.clients.clear();
        for (index, name) in names.iter().enumerate() {
            let spec = self.create_client_spec(index, name)?;
            self.clients.push(ClientRuntime {
                spec,
                process: None,
            });
        }
        Ok(())
    }

    pub fn clients(&self) -> Vec<&ClientSpec> {
        self.clients
            .iter()
            .map(|runtime| &runtime.spec)
            .collect::<Vec<_>>()
    }

    pub fn client_at(&self, index: usize) -> Result<&ClientSpec, ClientControllerError> {
        self.client(index)
    }

    pub fn init_first_client(&mut self) -> Result<(), ClientControllerError> {
        if self.clients.is_empty() {
            return Err(ClientControllerError::NoClients);
        }
        let command = self.init_command(0)?;
        let output = self.executor.run(&command)?;
        ensure_success(&command, output)?;
        self.clients[0].spec.state = ClientLifecycleState::Initialized;
        Ok(())
    }

    pub fn discover_additional_clients(&mut self) -> Result<(), ClientControllerError> {
        if self.clients.is_empty() {
            return Err(ClientControllerError::NoClients);
        }
        for index in 1..self.clients.len() {
            let command = self.discover_command(index)?;
            let output = self.executor.run(&command)?;
            ensure_success(&command, output)?;
            self.clients[index].spec.state = ClientLifecycleState::Discovered;
        }
        Ok(())
    }

    pub fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ClientControllerError> {
        self.create_clients(options.client_names)?;
        if options.clean_existing_local_metadata {
            self.clean_local_metadata_dirs()?;
        }
        self.init_first_client()?;
        self.discover_additional_clients()?;
        self.verify_local_metadata_dbs()?;

        let clients = self
            .clients
            .iter()
            .map(|runtime| ClientBootstrapRecord {
                name: runtime.spec.name.clone(),
                local_metadata_db: runtime.spec.local_metadata_db.clone(),
                state: runtime.spec.state,
            })
            .collect::<Vec<_>>();
        let init_client = clients
            .first()
            .map(|client| client.name.clone())
            .ok_or(ClientControllerError::NoClients)?;
        let discovered_clients = clients
            .iter()
            .skip(1)
            .map(|client| client.name.clone())
            .collect();

        Ok(VolumeBootstrapSummary {
            volume_name: self.config.volume_name.clone(),
            init_client,
            discovered_clients,
            clients,
        })
    }

    pub fn mount_all(&mut self) -> Result<(), ClientControllerError> {
        for index in 0..self.clients.len() {
            self.mount_index(index)?;
        }
        Ok(())
    }

    pub fn restart(&mut self, client: &str) -> Result<(), ClientControllerError> {
        let index = self.client_index(client)?;
        self.unmount_index(index)?;
        self.mount_index(index)
    }

    pub fn unmount_all(&mut self) -> Result<(), ClientControllerError> {
        for index in 0..self.clients.len() {
            self.unmount_index(index)?;
        }
        Ok(())
    }

    pub fn force_kill_all(&mut self) -> Result<(), ClientControllerError> {
        for runtime in &mut self.clients {
            if let Some(process) = runtime.process.as_mut() {
                process.kill()?;
            }
            runtime.process = None;
            runtime.spec.process_id = None;
            runtime.spec.state = ClientLifecycleState::Killed;
        }
        Ok(())
    }

    fn mount_index(&mut self, index: usize) -> Result<(), ClientControllerError> {
        let command = self.mount_command(index)?;
        let mut process = self.executor.spawn(&command)?;
        let process_id = process.id();
        if let Err(err) = self.wait_until_mount_ready(&command, process.as_mut()) {
            if let Err(kill_error) = process.kill() {
                debug_log_kill_error(&command.client, "mount readiness failure", &kill_error);
            }
            self.clients[index].spec.state =
                if matches!(err, ClientControllerError::CommandFailed { .. }) {
                    ClientLifecycleState::Exited
                } else {
                    ClientLifecycleState::Killed
                };
            return Err(err);
        }
        self.clients[index].spec.process_id = Some(process_id);
        self.clients[index].spec.state = ClientLifecycleState::Mounted;
        self.clients[index].process = Some(process);
        Ok(())
    }

    fn clean_local_metadata_dirs(&self) -> Result<(), ClientControllerError> {
        for runtime in &self.clients {
            let path = &runtime.spec.local_metadata_dir;
            if path.exists() {
                fs::remove_dir_all(path).map_err(|source| {
                    ClientControllerError::CleanLocalMetadataFailed {
                        client: runtime.spec.name.clone(),
                        path: path.clone(),
                        source,
                    }
                })?;
            }
        }
        Ok(())
    }

    fn verify_local_metadata_dbs(&self) -> Result<(), ClientControllerError> {
        let mut seen = HashSet::<OsString>::new();
        for runtime in &self.clients {
            let path = &runtime.spec.local_metadata_db;
            if !path.exists() {
                return Err(ClientControllerError::MissingLocalMetadataDb {
                    client: runtime.spec.name.clone(),
                    path: path.clone(),
                });
            }
            let unique_path = path.canonicalize()?.into_os_string();
            if !seen.insert(unique_path) {
                return Err(ClientControllerError::DuplicateLocalMetadataDb(
                    path.clone(),
                ));
            }
        }
        Ok(())
    }

    fn unmount_index(&mut self, index: usize) -> Result<(), ClientControllerError> {
        let command = self.unmount_command(index)?;
        let output = self.executor.run(&command)?;
        ensure_success(&command, output)?;
        if let Some(process) = self.clients[index].process.as_mut() {
            wait_for_process_exit(
                process.as_mut(),
                self.config.mount_exit_timeout,
                self.config.poll_interval,
            )?;
        }
        self.clients[index].process = None;
        self.clients[index].spec.process_id = None;
        self.clients[index].spec.state = ClientLifecycleState::Unmounted;
        Ok(())
    }

    fn client_index(&self, client: &str) -> Result<usize, ClientControllerError> {
        self.clients
            .iter()
            .position(|runtime| runtime.spec.name == client)
            .ok_or_else(|| ClientControllerError::UnknownClient(client.to_string()))
    }

    fn create_client_spec(
        &self,
        index: usize,
        name: &str,
    ) -> Result<ClientSpec, ClientControllerError> {
        let mount_point = self
            .config
            .mount_points
            .get(index)
            .cloned()
            .unwrap_or_else(|| default_client_mount_point(&self.config.sandbox, name, index));
        let spec = client_spec_for(
            &self.config.sandbox,
            &self.config.volume_name,
            name,
            mount_point,
        );
        fs::create_dir_all(&spec.client_root)?;
        fs::create_dir_all(&spec.cache_dir)?;
        fs::create_dir_all(&spec.mount_metadata_dir)?;
        write_mount_metadata(&spec)?;
        if should_create_mount_point_dir(&spec.mount_point, &spec.mount_metadata_dir) {
            fs::create_dir_all(&spec.mount_point)?;
        }
        Ok(spec)
    }

    fn init_command(&self, index: usize) -> Result<ClientCommand, ClientControllerError> {
        let args = vec![
            "init".to_string(),
            self.config.volume_name.clone(),
            "--endpoint".to_string(),
            self.config.s3.endpoint.clone(),
            "--bucket".to_string(),
            self.config.s3.bucket.clone(),
            "--access-key".to_string(),
            self.config.s3.access_key.clone(),
            "--secret-key".to_string(),
            self.config.s3.secret_key.clone(),
            "--region".to_string(),
            self.config.s3.region.clone(),
            "--meta".to_string(),
            self.config.meta_dsn.clone(),
        ];
        self.command(index, "init", args)
    }

    fn discover_command(&self, index: usize) -> Result<ClientCommand, ClientControllerError> {
        let args = vec![
            "discover".to_string(),
            self.config.volume_name.clone(),
            "--meta".to_string(),
            self.config.meta_dsn.clone(),
        ];
        self.command(index, "discover", args)
    }

    fn mount_command(&self, index: usize) -> Result<ClientCommand, ClientControllerError> {
        let client = self.client(index)?;
        let args = vec![
            "mount".to_string(),
            self.config.volume_name.clone(),
            client.mount_point.display().to_string(),
            "--cache-dir".to_string(),
            client.cache_dir.display().to_string(),
            "--foreground".to_string(),
        ];
        self.command(index, "mount", args)
    }

    fn unmount_command(&self, index: usize) -> Result<ClientCommand, ClientControllerError> {
        let client = self.client(index)?;
        let args = vec![
            "umount".to_string(),
            client.mount_point.display().to_string(),
        ];
        self.command(index, "umount", args)
    }

    fn command(
        &self,
        index: usize,
        action: &str,
        args: Vec<String>,
    ) -> Result<ClientCommand, ClientControllerError> {
        let client = self.client(index)?;
        Ok(ClientCommand {
            client: client.name.clone(),
            action: action.to_string(),
            program: self.config.studiofs_bin.display().to_string(),
            args,
            env: client_env(client),
            client_root: client.client_root.clone(),
            cache_dir: client.cache_dir.clone(),
            mount_point: client.mount_point.clone(),
            stdout_log: client.stdout_log.clone(),
            stderr_log: client.stderr_log.clone(),
            tracing_log: client.tracing_log.clone(),
        })
    }

    fn client(&self, index: usize) -> Result<&ClientSpec, ClientControllerError> {
        self.clients.get(index).map(|runtime| &runtime.spec).ok_or(
            ClientControllerError::ClientIndexOutOfRange {
                index,
                len: self.clients.len(),
            },
        )
    }

    fn wait_until_mount_ready(
        &self,
        command: &ClientCommand,
        process: &mut dyn ClientProcess,
    ) -> Result<(), ClientControllerError> {
        let deadline = Instant::now() + self.config.mount_ready_timeout;
        loop {
            if let Some(exit_code) = process.try_wait()? {
                return Err(ClientControllerError::CommandFailed {
                    client: command.client.clone(),
                    action: command.action.clone(),
                    status: exit_code,
                    stdout: String::new(),
                    stderr: format!(
                        "mount process exited before readiness with status {exit_code}"
                    ),
                });
            }
            if self.executor.is_mount_ready(command)? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ClientControllerError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "mount point for {} was not ready: {}",
                        command.client,
                        command.mount_point.display()
                    ),
                )));
            }
            std::thread::sleep(nonzero_poll_interval(self.config.poll_interval));
        }
    }
}

impl<E> Drop for MultiClientController<E> {
    fn drop(&mut self) {
        for runtime in &mut self.clients {
            if let Some(process) = runtime.process.as_mut() {
                if let Err(error) = process.kill() {
                    debug_log_kill_error(&runtime.spec.name, "controller drop", &error);
                }
            }
        }
    }
}

struct ClientRuntime {
    spec: ClientSpec,
    process: Option<Box<dyn ClientProcess>>,
}

fn ensure_success(
    command: &ClientCommand,
    output: ClientCommandOutput,
) -> Result<(), ClientControllerError> {
    if output.status == 0 {
        Ok(())
    } else {
        Err(ClientControllerError::CommandFailed {
            client: command.client.clone(),
            action: command.action.clone(),
            status: output.status,
            stdout: diagnostic_excerpt(&output.stdout),
            stderr: diagnostic_excerpt(&output.stderr),
        })
    }
}

pub fn client_artifact_dir(sandbox: &Path, client: &str) -> PathBuf {
    sandbox.join("artifacts").join("clients").join(client)
}

pub fn client_root_dir(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("client-root")
}

pub fn client_cache_dir(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("cache")
}

pub fn client_mount_point(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("mount")
}

pub fn client_stdout_log(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("studiofs.stdout.log")
}

pub fn client_stderr_log(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("studiofs.stderr.log")
}

pub fn client_tracing_log(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("studiofs-tracing.log")
}

pub fn client_spec_for(
    sandbox: &Path,
    volume_name: &str,
    client: &str,
    mount_point: PathBuf,
) -> ClientSpec {
    let client_root = client_root_dir(sandbox, client);
    let local_metadata_dir = client_root.join("meta").join(volume_name);
    let mount_metadata_dir = client_mount_point(sandbox, client);
    ClientSpec {
        name: client.to_string(),
        local_metadata_db: local_metadata_dir.join(format!("{volume_name}.db")),
        local_metadata_dir,
        client_root,
        cache_dir: client_cache_dir(sandbox, client),
        mount_metadata_dir,
        mount_point,
        stdout_log: client_stdout_log(sandbox, client),
        stderr_log: client_stderr_log(sandbox, client),
        tracing_log: client_tracing_log(sandbox, client),
        process_id: None,
        state: ClientLifecycleState::Planned,
    }
}

fn wait_for_process_exit(
    process: &mut dyn ClientProcess,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), ClientControllerError> {
    let deadline = Instant::now() + timeout;
    loop {
        if process.try_wait()?.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ClientControllerError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "mount process did not exit after graceful unmount",
            )));
        }
        std::thread::sleep(nonzero_poll_interval(poll_interval));
    }
}

fn client_env(client: &ClientSpec) -> Vec<(String, String)> {
    vec![
        (
            "STUDIOFS_CLIENT_ROOT".to_string(),
            client.client_root.display().to_string(),
        ),
        (
            "STUDIOFS_DAEMON_LOG_DIR".to_string(),
            client
                .tracing_log
                .parent()
                .unwrap_or(&client.client_root)
                .display()
                .to_string(),
        ),
        (
            "STUDIOFS_DAEMON_LOG_STEM".to_string(),
            "studiofs-tracing".to_string(),
        ),
    ]
}

fn base_command(command: &ClientCommand) -> Command {
    let mut process = Command::new(&command.program);
    process.args(&command.args);
    process.envs(
        command
            .env
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    process
}

fn append_log(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

fn validate_client_name(name: &str) -> Result<(), ClientControllerError> {
    let safe = !name.is_empty()
        && name != "."
        && name != ".."
        && !name.starts_with('.')
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'));
    if safe {
        Ok(())
    } else {
        Err(ClientControllerError::UnsafeClientName(name.to_string()))
    }
}

fn service_port<'a>(
    ports: &'a [ServicePort],
    service: &str,
) -> Result<&'a ServicePort, ClientControllerError> {
    ports
        .iter()
        .find(|port| port.service == service)
        .ok_or_else(|| ClientControllerError::MissingServicePort(service.to_string()))
}

fn sanitize_dns_label(value: &str) -> String {
    let mut sanitized = String::new();
    let mut last_was_dash = false;
    for ch in value.chars() {
        let next = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if next == '-' {
            if !last_was_dash && !sanitized.is_empty() {
                sanitized.push(next);
            }
            last_was_dash = true;
        } else {
            sanitized.push(next);
            last_was_dash = false;
        }
    }
    while sanitized.ends_with('-') {
        sanitized.pop();
    }
    if sanitized.is_empty() {
        "run".to_string()
    } else {
        sanitized
    }
}

fn prefixed_dns_name(prefix: &str, stem: &str, max_chars: usize) -> String {
    let mut name = format!("{prefix}{stem}");
    if name.len() <= max_chars {
        return name;
    }

    let mut hasher = DefaultHasher::new();
    hasher.write(name.as_bytes());
    let suffix = format!("-{:016x}", hasher.finish());
    let keep = max_chars.saturating_sub(suffix.len());
    name.truncate(keep);
    while name.ends_with('-') {
        name.pop();
    }
    name.push_str(&suffix);
    name
}

fn write_mount_metadata(spec: &ClientSpec) -> Result<(), ClientControllerError> {
    let payload = serde_json::json!({
        "client": spec.name,
        "mount_point": spec.mount_point.display().to_string(),
    });
    fs::write(
        spec.mount_metadata_dir.join("mount-point.json"),
        serde_json::to_vec_pretty(&payload).map_err(io::Error::other)?,
    )?;
    Ok(())
}

#[cfg(windows)]
fn default_client_mount_point(sandbox: &Path, client: &str, index: usize) -> PathBuf {
    DEFAULT_WINDOWS_MOUNT_LETTERS
        .get(index)
        .map(|letter| PathBuf::from(format!("{letter}:")))
        .unwrap_or_else(|| client_mount_point(sandbox, client))
}

#[cfg(not(windows))]
fn default_client_mount_point(sandbox: &Path, client: &str, _index: usize) -> PathBuf {
    client_mount_point(sandbox, client)
}

fn should_create_mount_point_dir(mount_point: &Path, mount_metadata_dir: &Path) -> bool {
    mount_point == mount_metadata_dir || !is_windows_drive_mount_point(mount_point)
}

fn nonzero_poll_interval(interval: Duration) -> Duration {
    if interval.is_zero() {
        Duration::from_millis(1)
    } else {
        interval
    }
}

fn diagnostic_excerpt(value: &str) -> String {
    let mut chars = value.chars();
    let excerpt: String = chars.by_ref().take(MAX_COMMAND_OUTPUT_CHARS).collect();
    if chars.next().is_some() {
        format!(
            "{excerpt}\n[truncated after {MAX_COMMAND_OUTPUT_CHARS} chars; full output is in the per-client log files]"
        )
    } else {
        excerpt
    }
}

#[cfg(debug_assertions)]
fn debug_log_kill_error(client: &str, context: &str, error: &io::Error) {
    eprintln!("studiofs-e2e: failed to kill client {client} during {context}: {error}");
}

#[cfg(not(debug_assertions))]
fn debug_log_kill_error(_client: &str, _context: &str, _error: &io::Error) {}

#[cfg(unix)]
fn mount_point_is_active(path: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    let Some(parent) = path.parent() else {
        return Ok(false);
    };
    let parent = fs::metadata(parent)?;
    Ok(metadata.dev() != parent.dev())
}

#[cfg(windows)]
fn mount_point_is_active(path: &Path) -> io::Result<bool> {
    let Some(root) = windows_drive_root(path) else {
        return Ok(false);
    };
    Ok(root.exists())
}

#[cfg(windows)]
fn is_windows_drive_mount_point(path: &Path) -> bool {
    windows_drive_root(path).is_some()
}

#[cfg(not(windows))]
fn is_windows_drive_mount_point(_path: &Path) -> bool {
    false
}

#[cfg(windows)]
fn windows_drive_root(path: &Path) -> Option<PathBuf> {
    let value = path.as_os_str().to_string_lossy();
    let bytes = value.as_bytes();
    let drive = bytes.first().copied()?;
    if !drive.is_ascii_alphabetic() || bytes.get(1).copied() != Some(b':') {
        return None;
    }
    let suffix = &value[2..];
    if !matches!(suffix, "" | "\\" | "/") {
        return None;
    }
    Some(PathBuf::from(format!("{}:\\", char::from(drive))))
}

#[cfg(not(any(unix, windows)))]
fn mount_point_is_active(path: &Path) -> io::Result<bool> {
    Ok(path.exists())
}

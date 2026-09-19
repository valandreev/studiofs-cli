//! Multi-client process controller for mounted E2E scenarios.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
#[cfg(target_os = "macos")]
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use std::{
    collections::BTreeMap, collections::HashSet, collections::hash_map::DefaultHasher,
    ffi::OsString, hash::Hasher,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::infra::ServicePort;
use crate::process_diagnostics::capture_process_diagnostics_best_effort;
use crate::process_job::ChildJob;
#[cfg(target_os = "macos")]
use crate::process_job::{CHILD_TERMINATION_GRACE, wait_for_child_exit};

/// Default client names used by the E2E mounted scenarios.
pub const DEFAULT_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
#[cfg(windows)]
const DEFAULT_WINDOWS_MOUNT_LETTERS: &[char] = &[
    'Z', 'Y', 'X', 'W', 'V', 'U', 'T', 'S', 'R', 'Q', 'P', 'O', 'N', 'M', 'L', 'K',
];
const MAX_COMMAND_OUTPUT_CHARS: usize = 4096;
#[cfg(target_os = "macos")]
const MACOS_STALE_UNMOUNT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(target_os = "macos")]
const MACOS_DISKUTIL_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(target_os = "macos")]
const MACOS_MOUNT_TABLE_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(windows)]
const TASKKILL_FALLBACK_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(windows)]
const WINDOWS_STALE_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(windows)]
const WINDOWS_STALE_UNMOUNT_POLL_INTERVAL: Duration = Duration::from_millis(100);
#[cfg(windows)]
const WINDOWS_STALE_UNMOUNT_STABLE_PROBES: usize = 3;
const S3_BUCKET_NAME_MAX_CHARS: usize = 63;
const E2E_BUCKET_PREFIX: &str = "studiofs-e2e-";
const E2E_VOLUME_PREFIX: &str = "sfs-e2e-";
const E2E_MOUNT_ROOT_ENV: &str = "STUDIOFS_E2E_MOUNT_ROOT";
const WINFSP_FILE_INFO_TIMEOUT_ENV: &str = "STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS";
const WINFSP_DIR_INFO_TIMEOUT_ENV: &str = "STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS";
const WINFSP_VOLUME_INFO_TIMEOUT_ENV: &str = "STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS";
const WINFSP_SECURITY_TIMEOUT_ENV: &str = "STUDIOFS_WINFSP_SECURITY_TIMEOUT_MS";
const WINFSP_STREAM_INFO_TIMEOUT_ENV: &str = "STUDIOFS_WINFSP_STREAM_INFO_TIMEOUT_MS";
const WINFSP_EXTENDED_ATTRIBUTE_TIMEOUT_ENV: &str = "STUDIOFS_WINFSP_EXTENDED_ATTRIBUTE_TIMEOUT_MS";
const E2E_WINFSP_METADATA_TIMEOUT_MS: &str = "1000";

#[derive(Debug, Clone, PartialEq, Eq)]
/// Shared configuration used to create and run a multi-client controller.
pub struct ClientControllerConfig {
    /// Path to the `studiofs` binary used to run each client command.
    pub studiofs_bin: PathBuf,
    /// Base sandbox path shared by all clients.
    pub sandbox: PathBuf,
    /// Name of the mounted volume shared by the controller.
    pub volume_name: String,
    /// PostgreSQL DSN for local metadata service.
    pub meta_dsn: String,
    /// S3-compatible storage credentials and endpoint.
    pub s3: S3Config,
    /// Timeout while waiting for a mount point to become ready.
    pub mount_ready_timeout: Duration,
    /// Timeout while waiting for a mounted process to exit.
    pub mount_exit_timeout: Duration,
    /// Poll cadence used for readiness/exit polling loops.
    pub poll_interval: Duration,
    /// Pre-configured mount points (1-1 with client names when provided).
    pub mount_points: Vec<PathBuf>,
    /// Per-client environment variable overrides.
    pub client_env_overrides: BTreeMap<String, Vec<(String, String)>>,
    /// Per-client extra arguments appended to `studiofs mount`.
    pub mount_arg_overrides: BTreeMap<String, Vec<String>>,
}

impl ClientControllerConfig {
    /// Build a controller configuration from compose service ports.
    pub fn from_compose_services(
        studiofs_bin: PathBuf,
        sandbox: PathBuf,
        run_id: &str,
        seed: u64,
        ports: &[ServicePort],
    ) -> Result<Self, ClientControllerError> {
        Self::from_compose_services_inner(studiofs_bin, sandbox, run_id, seed, None, ports)
    }

    /// Build a controller configuration from compose service ports and scenario name.
    pub fn from_compose_services_for_scenario(
        studiofs_bin: PathBuf,
        sandbox: PathBuf,
        run_id: &str,
        seed: u64,
        scenario_name: &str,
        ports: &[ServicePort],
    ) -> Result<Self, ClientControllerError> {
        Self::from_compose_services_inner(
            studiofs_bin,
            sandbox,
            run_id,
            seed,
            Some(scenario_name),
            ports,
        )
    }

    /// Build a controller configuration from repository `.env` / process environment.
    pub fn from_external_env_for_scenario(
        studiofs_bin: PathBuf,
        sandbox: PathBuf,
        run_id: &str,
        seed: u64,
        scenario_name: &str,
    ) -> Result<Self, ClientControllerError> {
        let env_file = load_dotenv_values()?;
        let mount_points = configured_mount_points(
            optional_external_env(&env_file, E2E_MOUNT_ROOT_ENV).as_deref(),
            DEFAULT_CLIENT_NAMES,
        )?;
        let sanitized_run_id = sanitize_dns_label(run_id);
        let scenario = sanitize_dns_label(scenario_name);
        let volume_stem = prefixed_dns_name(
            E2E_VOLUME_PREFIX,
            &format!("{sanitized_run_id}-{seed}-{scenario}"),
            S3_BUCKET_NAME_MAX_CHARS,
        );
        Ok(Self {
            studiofs_bin,
            sandbox,
            volume_name: volume_stem,
            meta_dsn: required_external_env(&env_file, "STUDIOFS_TEST_PG_DSN")?,
            s3: S3Config {
                endpoint: required_external_env(&env_file, "STUDIOFS_S3_ENDPOINT")?,
                bucket: required_external_env(&env_file, "STUDIOFS_S3_BUCKET")?,
                access_key: required_external_env(&env_file, "STUDIOFS_S3_ACCESS_KEY")?,
                secret_key: required_external_env(&env_file, "STUDIOFS_S3_SECRET_KEY")?,
                region: optional_external_env(&env_file, "STUDIOFS_S3_REGION")
                    .unwrap_or_else(|| "us-east-1".to_string()),
            },
            mount_ready_timeout: Duration::from_secs(30),
            mount_exit_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(100),
            mount_points,
            client_env_overrides: BTreeMap::new(),
            mount_arg_overrides: BTreeMap::new(),
        })
    }

    fn from_compose_services_inner(
        studiofs_bin: PathBuf,
        sandbox: PathBuf,
        run_id: &str,
        seed: u64,
        scenario_name: Option<&str>,
        ports: &[ServicePort],
    ) -> Result<Self, ClientControllerError> {
        let env_file = load_dotenv_values()?;
        let postgres = service_port(ports, "postgres")?;
        let rustfs = service_port(ports, "rustfs")?;
        let sanitized_run_id = sanitize_dns_label(run_id);
        let volume_discriminator = scenario_name.map(sanitize_dns_label);
        let bucket = prefixed_dns_name(
            E2E_BUCKET_PREFIX,
            &sanitized_run_id,
            S3_BUCKET_NAME_MAX_CHARS,
        );
        let volume_stem_input = match volume_discriminator {
            Some(scenario) => format!("{sanitized_run_id}-{seed}-{scenario}"),
            None => format!("{sanitized_run_id}-{seed}"),
        };
        let volume_stem = prefixed_dns_name(
            E2E_VOLUME_PREFIX,
            &volume_stem_input,
            S3_BUCKET_NAME_MAX_CHARS,
        );
        let mount_points = configured_mount_points(
            optional_external_env(&env_file, E2E_MOUNT_ROOT_ENV).as_deref(),
            DEFAULT_CLIENT_NAMES,
        )?;
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
            mount_exit_timeout: default_compose_mount_exit_timeout(),
            poll_interval: Duration::from_millis(100),
            mount_points,
            client_env_overrides: BTreeMap::new(),
            mount_arg_overrides: BTreeMap::new(),
        })
    }
}

fn default_compose_mount_exit_timeout() -> Duration {
    if cfg!(target_os = "macos") {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(15)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// S3 connection parameters consumed by client bootstrap and mount commands.
pub struct S3Config {
    /// S3 endpoint URL.
    pub endpoint: String,
    /// Bucket name for scenario metadata and test objects.
    pub bucket: String,
    /// Access key for S3 authentication.
    pub access_key: String,
    /// Secret key for S3 authentication.
    pub secret_key: String,
    /// AWS region used by the S3 backend.
    pub region: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle phases used by `ClientSpec` and bootstrap summaries.
pub enum ClientLifecycleState {
    /// Client was declared but no action performed yet.
    Planned,
    /// Client initialization has been executed.
    Initialized,
    /// Client has been discovered for an existing volume.
    Discovered,
    /// Client mount process is running.
    Mounted,
    /// Client has been unmounted.
    Unmounted,
    /// Client was force-killed.
    Killed,
    /// Process exited before success or without explicit kill.
    Exited,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Runtime specification for a single E2E client.
pub struct ClientSpec {
    /// Client identifier.
    pub name: String,
    /// Volume name assigned to this client.
    pub volume_name: String,
    /// Root working directory used by the CLI for this client.
    pub client_root: PathBuf,
    /// Local metadata directory for the client.
    pub local_metadata_dir: PathBuf,
    /// Path to the local metadata sqlite database for the client.
    pub local_metadata_db: PathBuf,
    /// Cache directory used by mounted process.
    pub cache_dir: PathBuf,
    /// Directory where mount metadata files are stored.
    pub mount_metadata_dir: PathBuf,
    /// Mount point path for this client.
    pub mount_point: PathBuf,
    /// Path to stdout log for child command.
    pub stdout_log: PathBuf,
    /// Path to stderr log for child command.
    pub stderr_log: PathBuf,
    /// Path to tracing log for child process.
    pub tracing_log: PathBuf,
    /// PID of the mounted child process if present.
    pub process_id: Option<u32>,
    /// Current lifecycle state.
    pub state: ClientLifecycleState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Fully resolved command payload passed to clients.
pub struct ClientCommand {
    /// Client name.
    pub client: String,
    /// Action label used for diagnostics.
    pub action: String,
    /// Executable name/path (typically `studiofs`).
    pub program: String,
    /// Arguments forwarded to the executable.
    pub args: Vec<String>,
    /// Environment overrides for child command.
    pub env: Vec<(String, String)>,
    /// Client root directory.
    pub client_root: PathBuf,
    /// Client cache directory.
    pub cache_dir: PathBuf,
    /// Mount point path.
    pub mount_point: PathBuf,
    /// Stdout log path.
    pub stdout_log: PathBuf,
    /// Stderr log path.
    pub stderr_log: PathBuf,
    /// Tracing log path.
    pub tracing_log: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Captured command output used by success checks.
pub struct ClientCommandOutput {
    /// Exit status code.
    pub status: i32,
    /// Captured stdout (UTF-8 lossy string).
    pub stdout: String,
    /// Captured stderr (UTF-8 lossy string).
    pub stderr: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Options for bootstrapping a single shared volume.
pub struct VolumeBootstrapOptions {
    /// Client names to create.
    pub client_names: Vec<String>,
    /// Remove pre-existing local metadata before bootstrap.
    pub clean_existing_local_metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Options for bootstrapping multiple peer volumes.
pub struct PeerVolumeBootstrapOptions {
    /// Client names participating in peer bootstrap.
    pub client_names: Vec<String>,
    /// Remove per-client metadata roots before bootstrap.
    pub clean_existing_local_metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Client record included in bootstrap summaries.
pub struct ClientBootstrapRecord {
    /// Client name.
    pub name: String,
    /// Local metadata database path for this client.
    pub local_metadata_db: PathBuf,
    /// Current lifecycle state captured in bootstrap summary.
    pub state: ClientLifecycleState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Summary for a single volume bootstrap invocation.
pub struct VolumeBootstrapSummary {
    /// Name of the bootstrapped volume.
    pub volume_name: String,
    /// Initializing client name.
    pub init_client: String,
    /// Clients joined after the initializer.
    pub discovered_clients: Vec<String>,
    /// Full client records for the volume.
    pub clients: Vec<ClientBootstrapRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Summary for peer volume bootstrap across multiple owners.
pub struct PeerVolumeBootstrapSummary {
    /// Base/original volume name used for peer peers.
    pub base_volume_name: String,
    /// Summaries for each peer volume.
    pub volumes: Vec<VolumeBootstrapSummary>,
}

/// Abstraction for a running client process.
pub trait ClientProcess {
    /// Returns the child process identifier.
    fn id(&self) -> u32;
    /// Polls process wait status without blocking.
    fn try_wait(&mut self) -> io::Result<Option<i32>>;
    /// Sends a best-effort kill request to the process.
    fn kill(&mut self) -> io::Result<()>;
    /// Terminates process and known process tree.
    fn kill_process_tree(&mut self, _context: &str) -> io::Result<()> {
        self.kill()
    }
}

/// Executor abstraction for spawning and observing client commands.
pub trait ClientProcessExecutor {
    /// Run a command and capture output.
    fn run(&self, command: &ClientCommand) -> io::Result<ClientCommandOutput>;
    /// Spawn a command and return a live process handle.
    fn spawn(&self, command: &ClientCommand) -> io::Result<Box<dyn ClientProcess>>;
    /// Probe if a client mount point is already active.
    fn is_mount_ready(&self, command: &ClientCommand) -> io::Result<bool>;
    /// Probe if a spawned client has completed native mount readiness.
    ///
    /// The default keeps test and non-native executors compatible: their
    /// mount probe already represents the complete readiness contract.
    fn is_mount_ready_for_process(
        &self,
        command: &ClientCommand,
        _process_id: u32,
    ) -> io::Result<bool> {
        self.is_mount_ready(command)
    }
    /// Remove stale mounts that may remain from previous runs.
    fn cleanup_stale_mount_before_remount(&self, _command: &ClientCommand) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
/// Default process executor used in mounted E2E scenario runs.
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
        let mut process = base_command(command);
        process
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        ChildJob::prepare_command(&mut process)?;
        let child = process.spawn()?;
        Ok(Box::new(SystemClientProcess::new(child)?))
    }

    fn is_mount_ready(&self, command: &ClientCommand) -> io::Result<bool> {
        #[cfg(target_os = "macos")]
        let active = macos_mount_is_active(&command.mount_point)?;
        #[cfg(not(target_os = "macos"))]
        let active = mount_point_is_active(&command.mount_point)?;
        Ok(active)
    }

    fn is_mount_ready_for_process(
        &self,
        command: &ClientCommand,
        process_id: u32,
    ) -> io::Result<bool> {
        #[cfg(windows)]
        {
            if !mount_point_is_active(&command.mount_point)? {
                return Ok(false);
            }
            if !windows_mount_lifecycle_is_ready(
                &command.client_root,
                &command.mount_point,
                process_id,
            )? {
                return Ok(false);
            }
            let write_capable = !command
                .args
                .iter()
                .any(|argument| argument == "--read-only");
            windows_mount_io_is_ready(&command.mount_point, write_capable)
        }

        #[cfg(not(windows))]
        {
            let _ = process_id;
            self.is_mount_ready(command)
        }
    }

    fn cleanup_stale_mount_before_remount(&self, command: &ClientCommand) -> io::Result<()> {
        cleanup_stale_mount_before_remount(&command.mount_point)
    }
}

struct SystemClientProcess {
    child: Child,
    job: ChildJob,
}

impl SystemClientProcess {
    fn new(mut child: Child) -> io::Result<Self> {
        let job = match ChildJob::assign_to_child(&child) {
            Ok(job) => job,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io::Error::new(
                    error.kind(),
                    format!("failed to isolate mount process in job object: {error}"),
                ));
            }
        };
        Ok(Self { child, job })
    }
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
        capture_process_diagnostics_best_effort(self.child.id(), "direct client process kill");
        self.child.kill()
    }

    fn kill_process_tree(&mut self, context: &str) -> io::Result<()> {
        capture_process_diagnostics_best_effort(
            self.child.id(),
            &format!("client process tree termination: {context}"),
        );
        let job_terminate_result = self.job.terminate();
        let job_force_result = self.job.force_kill();
        match (
            job_terminate_result,
            job_force_result,
            force_kill_child_process_tree(&mut self.child, context),
        ) {
            (Ok(true), _, _) | (_, Ok(true), _) => Ok(()),
            (Ok(false), Ok(false), child_result) => child_result,
            (Err(job_error), _, Ok(())) | (_, Err(job_error), Ok(())) => {
                debug_log_process_job_error(context, &job_error);
                Ok(())
            }
            (Err(terminate_error), Err(force_error), Err(child_error)) => {
                Err(io::Error::other(format!(
                    "failed to terminate process job and child process tree for {context}: job termination: {terminate_error}; job force-kill: {force_error}; child process tree termination: {child_error}"
                )))
            }
            (Err(job_error), Ok(false), Err(child_error))
            | (Ok(false), Err(job_error), Err(child_error)) => Err(io::Error::other(format!(
                "failed to terminate process job and child process tree for {context}: job termination: {job_error}; child process tree termination: {child_error}"
            ))),
        }
    }
}

fn force_kill_child_process_tree(child: &mut Child, context: &str) -> io::Result<()> {
    let pid = child.id();
    #[cfg(windows)]
    {
        let tree_result = run_taskkill_with_timeout(pid, TASKKILL_FALLBACK_TIMEOUT);
        let primary_result = child.kill();
        match (tree_result, primary_result) {
            (Ok(output), _) if windows_taskkill_status_is_success(output.status.code()) => Ok(()),
            (Ok(_), Ok(())) => Ok(()),
            (Ok(output), Err(primary)) => Err(io::Error::other(format!(
                "failed to terminate process tree for {context} pid {pid}: taskkill exit: {}; stdout: {}; stderr: {}; primary kill: {primary}",
                output.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&output.stdout).trim(),
                String::from_utf8_lossy(&output.stderr).trim()
            ))),
            (Err(_), Ok(())) => Ok(()),
            (Err(tree), Err(primary)) => Err(io::Error::other(format!(
                "failed to terminate process tree for {context} pid {pid}: taskkill failed: {tree}; primary kill: {primary}"
            ))),
        }
    }

    #[cfg(not(windows))]
    {
        let _ = context;
        let _ = pid;
        child.kill()
    }
}

#[cfg(windows)]
pub(crate) fn run_taskkill_with_timeout(
    pid: u32,
    timeout: Duration,
) -> io::Result<std::process::Output> {
    let mut child = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output();
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("taskkill timed out after {} ms", timeout.as_millis()),
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(windows)]
#[doc(hidden)]
/// Checks whether a `taskkill` status code should be treated as success.
pub fn windows_taskkill_status_is_success(status_code: Option<i32>) -> bool {
    status_code == Some(0) || status_code == Some(128)
}

#[derive(Debug, Error)]
/// Errors returned by `MultiClientController`.
pub enum ClientControllerError {
    /// No clients are configured.
    #[error("no E2E clients have been created")]
    NoClients,
    /// Attempted to replace or mutate while clients are still mounted.
    #[error("cannot replace E2E clients while mount processes are still tracked")]
    ClientsStillMounted,
    /// Provided client name is not safe for filesystem/service use.
    #[error("client name must be a safe single directory name: {0}")]
    UnsafeClientName(
        /// Provided client name string that failed validation.
        String,
    ),
    /// Configured macOS E2E mount root was not an absolute path.
    #[error("configured E2E mount root must be absolute: {0}")]
    InvalidMountRoot(
        /// Mount root path read from `.env` or the process environment.
        PathBuf,
    ),
    /// Unknown client name was requested.
    #[error("unknown E2E client: {0}")]
    UnknownClient(
        /// Requested client name that is not present in current configuration.
        String,
    ),
    /// Requested client index is outside the configured range.
    #[error("E2E client index {index} is out of range for {len} clients")]
    ClientIndexOutOfRange {
        /// Requested index that does not match any configured client.
        index: usize,
        /// Total number of clients in the current controller configuration.
        len: usize,
    },
    /// Mountpoint was already active when a mount was attempted.
    #[error("E2E client {client} mount point is already active before mount: {path}")]
    MountPointAlreadyActive {
        /// Client name for which mountpoint readiness was checked.
        client: String,
        /// Path that was already active at mount attempt time.
        path: PathBuf,
    },
    /// A compose service port was not found.
    #[error("Docker compose service port is missing for {0}")]
    MissingServicePort(
        /// Service name without an available port binding.
        String,
    ),
    /// Required environment-backed infrastructure value is missing.
    #[error("required external E2E env var is missing or empty: {0}")]
    MissingExternalEnvVar(
        /// Environment variable key.
        String,
    ),
    /// Spawned command failed with non-zero status.
    #[error(
        "client command {action} for {client} failed with exit status {status}: stderr: {stderr}; stdout: {stdout}"
    )]
    CommandFailed {
        /// Client name for the failed command.
        client: String,
        /// Command action.
        action: String,
        /// Raw exit status.
        status: i32,
        /// Command stdout excerpt.
        stdout: String,
        /// Command stderr excerpt.
        stderr: String,
    },
    /// Local metadata DB was not created for the client.
    #[error("E2E client {client} did not create local metadata DB: {path}")]
    MissingLocalMetadataDb {
        /// Client name without a created local metadata DB.
        client: String,
        /// Expected local metadata database path.
        path: PathBuf,
    },
    /// Same local metadata DB path was reused across clients.
    #[error("E2E local metadata DB path is shared by multiple clients: {0}")]
    DuplicateLocalMetadataDb(
        /// Shared local metadata DB path used by more than one client.
        PathBuf,
    ),
    /// Metadata DB could not be canonicalized.
    #[error("failed to canonicalize local metadata DB for E2E client {client} at {path}: {source}")]
    CanonicalizeLocalMetadataDbFailed {
        /// Client name.
        client: String,
        /// Declared local metadata DB path.
        path: PathBuf,
        /// Underlying OS error.
        source: io::Error,
    },
    /// Local metadata could not be cleaned.
    #[error("failed to clean local metadata for E2E client {client} at {path}: {source}")]
    CleanLocalMetadataFailed {
        /// Client name.
        client: String,
        /// Metadata path attempted to clean.
        path: PathBuf,
        /// Underlying OS error.
        source: io::Error,
    },
    /// Underlying I/O failure.
    #[error("client controller I/O failed: {0}")]
    Io(
        /// Underlying OS and stdlib error returned by I/O operations.
        #[from]
        io::Error,
    ),
}

/// Controller for creating, mounting, discovering and killing multiple clients.
pub struct MultiClientController<E> {
    config: ClientControllerConfig,
    executor: E,
    clients: Vec<ClientRuntime>,
}

impl<E: ClientProcessExecutor> MultiClientController<E> {
    /// Creates a new controller from configuration and executor.
    pub fn new(config: ClientControllerConfig, executor: E) -> Self {
        Self {
            config,
            executor,
            clients: Vec::new(),
        }
    }

    /// Replaces configured clients with names.
    ///
    /// Existing mounted clients are rejected to prevent unsafe mutation.
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
        let mount_root = configured_mount_root_from_environment()?;
        for (index, name) in names.iter().enumerate() {
            let spec = self.create_client_spec(index, name, mount_root.as_deref())?;
            self.clients.push(ClientRuntime {
                spec,
                process: None,
                mount_root: mount_root.clone(),
            });
        }
        Ok(())
    }

    /// Returns specs for all tracked clients.
    pub fn clients(&self) -> Vec<&ClientSpec> {
        self.clients
            .iter()
            .map(|runtime| &runtime.spec)
            .collect::<Vec<_>>()
    }

    /// Returns the client by index.
    pub fn client_at(&self, index: usize) -> Result<&ClientSpec, ClientControllerError> {
        self.client(index)
    }

    /// Sets environment variable overrides for a named client.
    pub fn set_client_env_overrides<I, K, V>(
        &mut self,
        client: &str,
        overrides: I,
    ) -> Result<(), ClientControllerError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.client_index(client)?;
        let overrides = overrides
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<Vec<_>>();
        self.config
            .client_env_overrides
            .insert(client.to_string(), overrides);
        Ok(())
    }

    /// Sets extra `studiofs mount` arguments for a named client.
    pub fn set_client_mount_arg_overrides<I, V>(
        &mut self,
        client: &str,
        overrides: I,
    ) -> Result<(), ClientControllerError>
    where
        I: IntoIterator<Item = V>,
        V: Into<String>,
    {
        self.client_index(client)?;
        let overrides = overrides.into_iter().map(Into::into).collect::<Vec<_>>();
        self.config
            .mount_arg_overrides
            .insert(client.to_string(), overrides);
        Ok(())
    }

    /// Initializes the first client (`client-0`) and marks it as initialized.
    pub fn init_first_client(&mut self) -> Result<(), ClientControllerError> {
        if self.clients.is_empty() {
            return Err(ClientControllerError::NoClients);
        }
        let command = self.init_command(0)?;
        let output = self.executor.run(&command)?;
        ensure_success(&command, &output)?;
        self.clients[0].spec.state = ClientLifecycleState::Initialized;
        Ok(())
    }

    /// Discovers all additional clients after the first one.
    pub fn discover_additional_clients(&mut self) -> Result<(), ClientControllerError> {
        if self.clients.is_empty() {
            return Err(ClientControllerError::NoClients);
        }
        for index in 1..self.clients.len() {
            let command = self.discover_command(index)?;
            let output = self.executor.run(&command)?;
            ensure_success(&command, &output)?;
            self.clients[index].spec.state = ClientLifecycleState::Discovered;
        }
        Ok(())
    }

    /// Discovers a single named client.
    pub fn discover(&mut self, client: &str) -> Result<(), ClientControllerError> {
        let index = self.client_index(client)?;
        let command = self.discover_command(index)?;
        let output = self.executor.run(&command)?;
        ensure_success(&command, &output)?;
        self.clients[index].spec.state = ClientLifecycleState::Discovered;
        Ok(())
    }

    /// Bootstraps a single shared volume for the configured clients.
    pub fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ClientControllerError> {
        self.create_clients(options.client_names)?;
        if options.clean_existing_local_metadata {
            self.clean_client_metadata_roots()?;
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

    /// Bootstraps peer volume variants for each client as owner.
    pub fn bootstrap_peer_volumes(
        &mut self,
        options: PeerVolumeBootstrapOptions,
    ) -> Result<PeerVolumeBootstrapSummary, ClientControllerError> {
        self.create_clients(options.client_names)?;
        if options.clean_existing_local_metadata {
            self.clean_client_metadata_roots()?;
        }
        if self.clients.is_empty() {
            return Err(ClientControllerError::NoClients);
        }

        let volume_names = self.peer_volume_names();
        let mut summaries = Vec::with_capacity(volume_names.len());
        for (owner_index, volume_name) in volume_names.iter().enumerate() {
            let init_command = self.init_command_for_volume(owner_index, volume_name)?;
            let output = self.executor.run(&init_command)?;
            ensure_success(&init_command, &output)?;
            self.clients[owner_index].spec.state = ClientLifecycleState::Initialized;

            let mut discovered_clients = Vec::new();
            for index in 0..self.clients.len() {
                if index == owner_index {
                    continue;
                }
                let discover_command = self.discover_command_for_volume(index, volume_name)?;
                let output = self.executor.run(&discover_command)?;
                ensure_success(&discover_command, &output)?;
                self.clients[index].spec.state = ClientLifecycleState::Discovered;
                discovered_clients.push(self.clients[index].spec.name.clone());
            }

            let clients = self
                .clients
                .iter()
                .map(|runtime| {
                    let spec = self.spec_for_client_volume(&runtime.spec.name, volume_name);
                    ClientBootstrapRecord {
                        name: runtime.spec.name.clone(),
                        local_metadata_db: spec.local_metadata_db,
                        state: if runtime.spec.name == self.clients[owner_index].spec.name {
                            ClientLifecycleState::Initialized
                        } else {
                            ClientLifecycleState::Discovered
                        },
                    }
                })
                .collect::<Vec<_>>();
            verify_local_metadata_db_records(&clients)?;
            summaries.push(VolumeBootstrapSummary {
                volume_name: volume_name.clone(),
                init_client: self.clients[owner_index].spec.name.clone(),
                discovered_clients,
                clients,
            });
        }

        Ok(PeerVolumeBootstrapSummary {
            base_volume_name: self.config.volume_name.clone(),
            volumes: summaries,
        })
    }

    /// Re-targets existing clients to a new volume name.
    pub fn select_volume_for_clients(
        &mut self,
        volume_name: &str,
    ) -> Result<(), ClientControllerError> {
        if self.clients.is_empty() {
            return Err(ClientControllerError::NoClients);
        }
        if self.clients.iter().any(|runtime| runtime.process.is_some()) {
            return Err(ClientControllerError::ClientsStillMounted);
        }
        let names = self
            .clients
            .iter()
            .map(|runtime| runtime.spec.name.clone())
            .collect::<Vec<_>>();
        for (index, name) in names.iter().enumerate() {
            let mount_root = self.clients[index].mount_root.clone();
            self.clients[index].spec =
                self.create_client_volume_spec(index, name, volume_name, mount_root.as_deref())?;
        }
        Ok(())
    }

    /// Mounts all configured clients.
    pub fn mount_all(&mut self) -> Result<(), ClientControllerError> {
        for index in 0..self.clients.len() {
            self.mount_index(index)?;
        }
        Ok(())
    }

    /// Mounts a single named client.
    pub fn mount(&mut self, client: &str) -> Result<(), ClientControllerError> {
        let index = self.client_index(client)?;
        self.mount_index(index)
    }

    /// Runs `studiofs status` for a named client and returns its diagnostics output.
    pub fn status(&self, client: &str) -> Result<ClientCommandOutput, ClientControllerError> {
        let index = self.client_index(client)?;
        let command = self.status_command(index)?;
        let output = self.executor.run(&command)?;
        ensure_success(&command, &output)?;
        Ok(output)
    }

    /// Restarts (unmount then mount) a named client.
    pub fn restart(&mut self, client: &str) -> Result<(), ClientControllerError> {
        let index = self.client_index(client)?;
        self.unmount_index(index)?;
        self.mount_index(index)
    }

    /// Force-kills a named client process and marks it as killed.
    pub fn force_kill(&mut self, client: &str) -> Result<(), ClientControllerError> {
        let index = self.client_index(client)?;
        if let Some(process) = self.clients[index].process.as_mut() {
            force_kill_process_and_wait(
                process.as_mut(),
                "force_kill",
                self.config.mount_exit_timeout,
                self.config.poll_interval,
            )?;
        }
        self.cleanup_stale_mount(index)?;
        self.clients[index].process = None;
        self.clients[index].spec.process_id = None;
        self.clients[index].spec.state = ClientLifecycleState::Killed;
        Ok(())
    }

    /// Cleans up stale mount artifacts and remounts a named client.
    pub fn remount(&mut self, client: &str) -> Result<(), ClientControllerError> {
        let index = self.client_index(client)?;
        #[cfg(target_os = "macos")]
        if self.clients[index].spec.state != ClientLifecycleState::Killed {
            self.cleanup_stale_mount(index)?;
        } else {
            self.wait_for_macos_mount_lock_release(index)?;
        }
        #[cfg(not(target_os = "macos"))]
        self.executor
            .cleanup_stale_mount_before_remount(&self.mount_command(index)?)?;
        #[cfg(not(target_os = "macos"))]
        {
            self.mount_index(index)
        }
        #[cfg(target_os = "macos")]
        {
            let was_killed = self.clients[index].spec.state == ClientLifecycleState::Killed;
            let retry_deadline = Instant::now() + self.config.mount_exit_timeout;
            loop {
                let result = self.mount_index(index);
                if was_killed
                    && Instant::now() < retry_deadline
                    && matches!(
                        &result,
                        Err(ClientControllerError::CommandFailed { action, .. }) if action == "mount"
                    )
                {
                    // A killed macFUSE client can release its lifecycle lock just after the
                    // preflight check. Wait for that late release before retrying the remount.
                    self.wait_for_macos_mount_lock_release(index)?;
                    continue;
                }
                return result;
            }
        }
    }

    /// Unmounts all mounted clients.
    pub fn unmount_all(&mut self) -> Result<(), ClientControllerError> {
        let mut unmount_error = None;
        for index in 0..self.clients.len() {
            if let Err(error) = self.unmount_index(index) {
                unmount_error.get_or_insert(error);
            }
        }
        if let Some(error) = unmount_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    /// Unmounts a single named client.
    pub fn unmount(&mut self, client: &str) -> Result<(), ClientControllerError> {
        let index = self.client_index(client)?;
        self.unmount_index(index)
    }

    /// Force-kills all tracked client processes.
    pub fn force_kill_all(&mut self) -> Result<(), ClientControllerError> {
        let mut kill_error = None;
        for index in 0..self.clients.len() {
            if let Some(process) = self.clients[index].process.as_mut()
                && let Err(err) = force_kill_process_and_wait(
                    process.as_mut(),
                    "force_kill_all",
                    self.config.mount_exit_timeout,
                    self.config.poll_interval,
                )
            {
                kill_error.get_or_insert(err);
            }
            if kill_error.is_none()
                && let Err(err) = self.cleanup_stale_mount(index)
            {
                kill_error.get_or_insert(err);
            }
            self.clients[index].process = None;
            self.clients[index].spec.process_id = None;
            self.clients[index].spec.state = ClientLifecycleState::Killed;
        }
        if let Some(err) = kill_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    /// Sends best-effort termination to all tracked client processes without
    /// waiting for exit. This is reserved for blocked mounted I/O paths where
    /// waiting for process exit can keep the scenario child alive.
    pub fn emergency_kill_all_no_wait(
        &mut self,
        context: &str,
    ) -> Result<(), ClientControllerError> {
        let mut kill_error = None;
        for index in 0..self.clients.len() {
            if let Some(process) = self.clients[index].process.as_mut()
                && let Err(err) = process.kill_process_tree(context)
            {
                kill_error.get_or_insert(ClientControllerError::Io(err));
            }
            self.clients[index].process = None;
            self.clients[index].spec.process_id = None;
            self.clients[index].spec.state = ClientLifecycleState::Killed;
        }
        if let Some(err) = kill_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    fn mount_index(&mut self, index: usize) -> Result<(), ClientControllerError> {
        let command = self.mount_command(index)?;
        let mount_active = self.executor.is_mount_ready(&command)?;
        #[cfg(target_os = "macos")]
        let mount_active =
            mount_active || macos_mount_lock_is_held(&command.client_root, &command.mount_point)?;
        if mount_active {
            return Err(ClientControllerError::MountPointAlreadyActive {
                client: command.client,
                path: command.mount_point,
            });
        }
        let mut process = self.executor.spawn(&command)?;
        let process_id = process.id();
        if let Err(err) = self.wait_until_mount_ready(&command, process.as_mut()) {
            if let Err(kill_error) = force_kill_process_and_wait(
                process.as_mut(),
                "mount readiness failure",
                self.config.mount_exit_timeout,
                self.config.poll_interval,
            ) {
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
        write_mount_metadata(
            &self.clients[index].spec,
            self.clients[index].mount_root.as_deref(),
        )?;
        self.clients[index].process = Some(process);
        Ok(())
    }

    fn clean_client_metadata_roots(&self) -> Result<(), ClientControllerError> {
        for runtime in &self.clients {
            let meta_root = runtime.spec.client_root.join("meta");
            if meta_root.exists() {
                for entry in fs::read_dir(&meta_root).map_err(|source| {
                    ClientControllerError::CleanLocalMetadataFailed {
                        client: runtime.spec.name.clone(),
                        path: meta_root.clone(),
                        source,
                    }
                })? {
                    let path = entry
                        .map_err(|source| ClientControllerError::CleanLocalMetadataFailed {
                            client: runtime.spec.name.clone(),
                            path: meta_root.clone(),
                            source,
                        })?
                        .path();
                    fs::remove_dir_all(&path).map_err(|source| {
                        ClientControllerError::CleanLocalMetadataFailed {
                            client: runtime.spec.name.clone(),
                            path,
                            source,
                        }
                    })?;
                }
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
            let unique_path =
                canonical_local_metadata_db_path(&runtime.spec.name, path)?.into_os_string();
            if !seen.insert(unique_path) {
                return Err(ClientControllerError::DuplicateLocalMetadataDb(
                    path.clone(),
                ));
            }
        }
        Ok(())
    }

    fn peer_volume_names(&self) -> Vec<String> {
        self.clients
            .iter()
            .map(|runtime| peer_volume_name(&self.config.volume_name, &runtime.spec.name))
            .collect()
    }

    fn spec_for_client_volume(&self, client: &str, volume_name: &str) -> ClientSpec {
        let mount_point = self
            .clients
            .iter()
            .find(|runtime| runtime.spec.name == client)
            .map(|runtime| runtime.spec.mount_point.clone())
            .unwrap_or_else(|| client_mount_point(&self.config.sandbox, client));
        client_spec_for(&self.config.sandbox, volume_name, client, mount_point)
    }

    fn unmount_index(&mut self, index: usize) -> Result<(), ClientControllerError> {
        if self.clients[index].process.is_none() {
            self.cleanup_stale_mount(index)?;
            self.clients[index].spec.process_id = None;
            self.clients[index].spec.state = ClientLifecycleState::Unmounted;
            return Ok(());
        }
        let command = self.unmount_command(index)?;
        let output = self.executor.run(&command)?;
        ensure_success(&command, &output)?;
        if let Some(process) = self.clients[index].process.as_mut()
            && let Err(primary) = wait_for_process_exit(
                process.as_mut(),
                self.config.mount_exit_timeout,
                self.config.poll_interval,
            )
        {
            match force_kill_process_and_wait(
                process.as_mut(),
                "graceful unmount timeout",
                self.config.mount_exit_timeout,
                self.config.poll_interval,
            ) {
                Ok(()) => {}
                Err(error) => {
                    debug_log_kill_error(
                        &self.clients[index].spec.name,
                        "graceful unmount timeout",
                        &error,
                    );
                }
            }
            self.clients[index].process = None;
            self.clients[index].spec.process_id = None;
            self.clients[index].spec.state = ClientLifecycleState::Killed;
            return Err(primary);
        }
        self.cleanup_stale_mount(index)?;
        self.clients[index].process = None;
        self.clients[index].spec.process_id = None;
        self.clients[index].spec.state = ClientLifecycleState::Unmounted;
        Ok(())
    }

    #[cfg(target_os = "macos")]
    fn cleanup_stale_mount(&self, index: usize) -> Result<(), ClientControllerError> {
        let command = self.mount_command(index)?;
        self.executor
            .cleanup_stale_mount_before_remount(&command)
            .map_err(ClientControllerError::Io)?;
        self.wait_for_macos_mount_lock_release(index)
    }

    #[cfg(target_os = "macos")]
    fn wait_for_macos_mount_lock_release(&self, index: usize) -> Result<(), ClientControllerError> {
        let command = self.mount_command(index)?;
        wait_for_macos_mount_lock_release(
            &command.client_root,
            &command.mount_point,
            self.config.mount_exit_timeout,
            self.config.poll_interval,
        )
        .map_err(ClientControllerError::Io)
    }

    #[cfg(not(target_os = "macos"))]
    fn cleanup_stale_mount(&self, _index: usize) -> Result<(), ClientControllerError> {
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
        mount_root: Option<&Path>,
    ) -> Result<ClientSpec, ClientControllerError> {
        self.create_client_volume_spec(index, name, &self.config.volume_name, mount_root)
    }

    fn create_client_volume_spec(
        &self,
        index: usize,
        name: &str,
        volume_name: &str,
        mount_root: Option<&Path>,
    ) -> Result<ClientSpec, ClientControllerError> {
        let mount_point = self
            .config
            .mount_points
            .get(index)
            .cloned()
            .unwrap_or_else(|| default_client_mount_point(&self.config.sandbox, name, index));
        let mount_point = product_mount_path(volume_name, mount_point);
        let spec = client_spec_for(&self.config.sandbox, volume_name, name, mount_point);
        fs::create_dir_all(&spec.client_root)?;
        fs::create_dir_all(&spec.cache_dir)?;
        fs::create_dir_all(&spec.mount_metadata_dir)?;
        write_mount_metadata(&spec, mount_root)?;
        if should_create_mount_point_dir(&spec.mount_point, &spec.mount_metadata_dir) {
            fs::create_dir_all(&spec.mount_point)?;
        }
        Ok(spec)
    }

    fn init_command(&self, index: usize) -> Result<ClientCommand, ClientControllerError> {
        self.init_command_for_volume(index, &self.config.volume_name)
    }

    fn init_command_for_volume(
        &self,
        index: usize,
        volume_name: &str,
    ) -> Result<ClientCommand, ClientControllerError> {
        let args = vec![
            "init".to_string(),
            volume_name.to_string(),
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
        self.discover_command_for_volume(index, &self.config.volume_name)
    }

    fn discover_command_for_volume(
        &self,
        index: usize,
        volume_name: &str,
    ) -> Result<ClientCommand, ClientControllerError> {
        let args = vec![
            "discover".to_string(),
            volume_name.to_string(),
            "--meta".to_string(),
            self.config.meta_dsn.clone(),
        ];
        self.command(index, "discover", args)
    }

    fn mount_command(&self, index: usize) -> Result<ClientCommand, ClientControllerError> {
        let client = self.client(index)?;
        let mut args = vec![
            "mount".to_string(),
            client.volume_name.clone(),
            client.mount_point.display().to_string(),
            "--cache-dir".to_string(),
            client.cache_dir.display().to_string(),
            "--foreground".to_string(),
        ];
        if let Some(overrides) = self.config.mount_arg_overrides.get(client.name.as_str()) {
            args.extend(overrides.iter().cloned());
        }
        self.command(index, "mount", args)
    }

    fn status_command(&self, index: usize) -> Result<ClientCommand, ClientControllerError> {
        let client = self.client(index)?;
        let args = vec![
            "status".to_string(),
            client.volume_name.clone(),
            "--cache-dir".to_string(),
            client.cache_dir.display().to_string(),
        ];
        self.command(index, "status", args)
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
            env: client_env_with_overrides(
                client,
                self.config
                    .client_env_overrides
                    .get(client.name.as_str())
                    .map(Vec::as_slice),
            ),
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
            if self
                .executor
                .is_mount_ready_for_process(command, process.id())?
            {
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
            if let Some(process) = runtime.process.as_mut()
                && let Err(error) = force_kill_process_and_wait(
                    process.as_mut(),
                    "controller drop",
                    self.config.mount_exit_timeout,
                    self.config.poll_interval,
                )
            {
                debug_log_kill_error(&runtime.spec.name, "controller drop", &error);
            }
        }
    }
}

struct ClientRuntime {
    spec: ClientSpec,
    process: Option<Box<dyn ClientProcess>>,
    mount_root: Option<PathBuf>,
}

fn ensure_success(
    command: &ClientCommand,
    output: &ClientCommandOutput,
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

fn load_dotenv_values() -> Result<BTreeMap<String, String>, ClientControllerError> {
    let Some(path) = find_dotenv_path()? else {
        return Ok(BTreeMap::new());
    };
    let text = fs::read_to_string(path)?;
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        values.insert(key.to_string(), normalize_external_env_value(value));
    }
    Ok(values)
}

pub(crate) fn configured_mount_root_from_environment()
-> Result<Option<PathBuf>, ClientControllerError> {
    let env_file = load_dotenv_values()?;
    configured_mount_root(optional_external_env(&env_file, E2E_MOUNT_ROOT_ENV).as_deref())
}

fn configured_mount_points(
    value: Option<&str>,
    clients: &[&str],
) -> Result<Vec<PathBuf>, ClientControllerError> {
    let Some(root) = configured_mount_root(value)? else {
        return Ok(Vec::new());
    };
    Ok(clients.iter().map(|client| root.join(client)).collect())
}

fn configured_mount_root(value: Option<&str>) -> Result<Option<PathBuf>, ClientControllerError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let path = PathBuf::from(value);
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        Ok(None)
    }
    #[cfg(target_os = "macos")]
    {
        if !path.is_absolute() {
            return Err(ClientControllerError::InvalidMountRoot(path));
        }
        Ok(Some(path))
    }
}

fn find_dotenv_path() -> Result<Option<PathBuf>, ClientControllerError> {
    let mut current = env::current_dir()?;
    loop {
        let candidate = current.join(".env");
        if candidate.exists() {
            return Ok(Some(candidate));
        }
        if !current.pop() {
            return Ok(None);
        }
    }
}

fn strip_dotenv_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'')
        {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn normalize_external_env_value(value: &str) -> String {
    let value = strip_dotenv_quotes(value.trim());
    if value.starts_with('<') && value.ends_with('>') && !value.contains(' ') {
        return value[1..value.len() - 1].to_string();
    }
    value
        .split_whitespace()
        .map(|token| {
            if let Some((key, inner)) = token
                .split_once("=<")
                .and_then(|(key, rest)| rest.strip_suffix('>').map(|inner| (key, inner)))
            {
                format!("{key}={inner}")
            } else {
                token.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn optional_external_env(values: &BTreeMap<String, String>, key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| normalize_external_env_value(&value))
        .or_else(|| {
            values
                .get(key)
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
}

fn required_external_env(
    values: &BTreeMap<String, String>,
    key: &str,
) -> Result<String, ClientControllerError> {
    optional_external_env(values, key)
        .ok_or_else(|| ClientControllerError::MissingExternalEnvVar(key.to_string()))
}

/// Returns the base artifact directory for a client.
pub fn client_artifact_dir(sandbox: &Path, client: &str) -> PathBuf {
    sandbox.join("artifacts").join("clients").join(client)
}

/// Returns the default client root directory for a client.
pub fn client_root_dir(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("client-root")
}

/// Returns the default client cache directory for a client.
pub fn client_cache_dir(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("cache")
}

/// Returns the default mount-point directory for a client.
pub fn client_mount_point(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("mount")
}

/// Returns an unused mount point for a scenario-owned read-only snapshot mount.
pub fn available_snapshot_mount_point(sandbox: &Path) -> Option<PathBuf> {
    #[cfg(windows)]
    let _ = sandbox;
    #[cfg(windows)]
    {
        DEFAULT_WINDOWS_MOUNT_LETTERS
            .iter()
            .copied()
            .filter_map(windows_available_drive_root)
            .next()
    }
    #[cfg(not(windows))]
    {
        Some(sandbox.join("snapshot-mount"))
    }
}

/// Returns the default stdout log path for a client.
pub fn client_stdout_log(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("studiofs.stdout.log")
}

/// Returns the default stderr log path for a client.
pub fn client_stderr_log(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("studiofs.stderr.log")
}

/// Returns the default tracing log path for a client.
pub fn client_tracing_log(sandbox: &Path, client: &str) -> PathBuf {
    client_artifact_dir(sandbox, client).join("studiofs-tracing.log")
}

/// Builds a fully populated `ClientSpec` for a client and volume.
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
        volume_name: volume_name.to_string(),
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

fn force_kill_process_and_wait(
    process: &mut dyn ClientProcess,
    context: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), ClientControllerError> {
    process.kill_process_tree(context)?;
    wait_for_process_exit(process, timeout, poll_interval).map_err(|error| match error {
        ClientControllerError::Io(source) if source.kind() == io::ErrorKind::TimedOut => {
            ClientControllerError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "mount process did not exit after force termination within {} ms during {context}",
                    timeout.as_millis()
                ),
            ))
        }
        other => other,
    })
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
        (
            "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS".to_string(),
            "250".to_string(),
        ),
        // Keep native WinFSP kernel metadata cache expiration (TTL) within the
        // bounded 5-second convergence window used by mounted E2E scenarios,
        // matching the Linux and macOS bridge defaults.
        (
            WINFSP_FILE_INFO_TIMEOUT_ENV.to_string(),
            E2E_WINFSP_METADATA_TIMEOUT_MS.to_string(),
        ),
        (
            WINFSP_DIR_INFO_TIMEOUT_ENV.to_string(),
            E2E_WINFSP_METADATA_TIMEOUT_MS.to_string(),
        ),
        (
            WINFSP_VOLUME_INFO_TIMEOUT_ENV.to_string(),
            E2E_WINFSP_METADATA_TIMEOUT_MS.to_string(),
        ),
        (
            WINFSP_SECURITY_TIMEOUT_ENV.to_string(),
            E2E_WINFSP_METADATA_TIMEOUT_MS.to_string(),
        ),
        (
            WINFSP_STREAM_INFO_TIMEOUT_ENV.to_string(),
            E2E_WINFSP_METADATA_TIMEOUT_MS.to_string(),
        ),
        (
            WINFSP_EXTENDED_ATTRIBUTE_TIMEOUT_ENV.to_string(),
            E2E_WINFSP_METADATA_TIMEOUT_MS.to_string(),
        ),
    ]
}

fn client_env_with_overrides(
    client: &ClientSpec,
    overrides: Option<&[(String, String)]>,
) -> Vec<(String, String)> {
    let mut env = client_env(client);
    if let Some(overrides) = overrides {
        for (key, value) in overrides {
            env.retain(|(existing_key, _)| existing_key != key);
            env.push((key.clone(), value.clone()));
        }
    }
    env
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

fn peer_volume_name(base_volume_name: &str, client: &str) -> String {
    prefixed_dns_name(
        "",
        &format!("{base_volume_name}-{client}"),
        S3_BUCKET_NAME_MAX_CHARS,
    )
}

fn canonical_local_metadata_db_path(
    client: &str,
    path: &Path,
) -> Result<PathBuf, ClientControllerError> {
    path.canonicalize().map_err(
        |source| ClientControllerError::CanonicalizeLocalMetadataDbFailed {
            client: client.to_string(),
            path: path.to_path_buf(),
            source,
        },
    )
}

fn verify_local_metadata_db_records(
    clients: &[ClientBootstrapRecord],
) -> Result<(), ClientControllerError> {
    let mut seen = HashSet::<OsString>::new();
    for client in clients {
        let path = &client.local_metadata_db;
        if !path.exists() {
            return Err(ClientControllerError::MissingLocalMetadataDb {
                client: client.name.clone(),
                path: path.clone(),
            });
        }
        let unique_path = canonical_local_metadata_db_path(&client.name, path)?.into_os_string();
        if !seen.insert(unique_path) {
            return Err(ClientControllerError::DuplicateLocalMetadataDb(
                path.clone(),
            ));
        }
    }
    Ok(())
}

fn write_mount_metadata(
    spec: &ClientSpec,
    mount_root: Option<&Path>,
) -> Result<(), ClientControllerError> {
    let payload = serde_json::json!({
        "client": spec.name,
        "mount_point": spec.mount_point.display().to_string(),
        "mount_root": mount_root.map(|path| path.display().to_string()),
        "process_id": spec.process_id,
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
        .iter()
        .copied()
        .filter_map(windows_available_drive_root)
        .nth(index)
        .unwrap_or_else(|| client_mount_point(sandbox, client))
}

#[cfg(not(windows))]
fn default_client_mount_point(sandbox: &Path, client: &str, _index: usize) -> PathBuf {
    client_mount_point(sandbox, client)
}

pub(crate) fn product_mount_path(volume_name: &str, requested_mount_point: PathBuf) -> PathBuf {
    #[cfg(windows)]
    if is_windows_drive_mount_point(&requested_mount_point) {
        return requested_mount_point;
    }

    if requested_mount_point.ends_with(volume_name) {
        requested_mount_point
    } else {
        requested_mount_point.join(volume_name)
    }
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
fn debug_log_kill_error(client: &str, context: &str, error: &impl std::fmt::Display) {
    eprintln!("studiofs-e2e: failed to kill client {client} during {context}: {error}");
}

#[cfg(not(debug_assertions))]
fn debug_log_kill_error(_client: &str, _context: &str, _error: &impl std::fmt::Display) {}

#[cfg(debug_assertions)]
fn debug_log_process_job_error(context: &str, error: &impl std::fmt::Display) {
    eprintln!("studiofs-e2e: process job termination failed during {context}: {error}");
}

#[cfg(not(debug_assertions))]
fn debug_log_process_job_error(_context: &str, _error: &impl std::fmt::Display) {}

#[cfg(target_os = "linux")]
pub(crate) fn mount_point_is_active(path: &Path) -> io::Result<bool> {
    let mountinfo = fs::read("/proc/self/mountinfo")?;
    Ok(mountinfo_contains_mount_point(&mountinfo, path))
}

#[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
pub(crate) fn mount_point_is_active(path: &Path) -> io::Result<bool> {
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
    if metadata.dev() != parent.dev() {
        return Ok(true);
    }
    Ok(false)
}

#[cfg(target_os = "macos")]
pub(crate) fn mount_point_is_active(path: &Path) -> io::Result<bool> {
    macos_mount_is_active(path)
}

#[cfg(target_os = "macos")]
fn macos_mount_is_active(mount_point: &Path) -> io::Result<bool> {
    // A stale FSKit vnode can block fs::metadata forever after its worker exits.
    // Read the mount table instead, and keep the probe itself bounded.
    let output =
        run_macos_command_with_timeout("/sbin/mount", &[], None, MACOS_MOUNT_TABLE_TIMEOUT)?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "/sbin/mount exited with {}; stderr: {}",
            output.status.code().unwrap_or(-1),
            diagnostic_excerpt(&String::from_utf8_lossy(&output.stderr))
        )));
    }
    macos_mount_is_active_with(mount_point, || Ok(output.stdout))
}

#[cfg(target_os = "macos")]
fn macos_mount_is_active_with(
    mount_point: &Path,
    list_mounts: impl FnOnce() -> io::Result<Vec<u8>>,
) -> io::Result<bool> {
    Ok(macos_mount_table_contains_mount_point(
        &list_mounts()?,
        mount_point,
    ))
}

#[cfg(target_os = "macos")]
fn macos_mount_table_contains_mount_point(mount_table: &[u8], expected: &Path) -> bool {
    let marker = format!(" on {} (", expected.display());
    String::from_utf8_lossy(mount_table)
        .lines()
        .any(|line| line.contains(&marker))
}

#[cfg(target_os = "macos")]
fn macos_mount_lock_is_held(client_root: &Path, mount_point: &Path) -> io::Result<bool> {
    use fs2::FileExt as _;

    // Keep this path layout aligned with studiofs::cli::utils mount records.
    let mounts_dir = client_root.join("mounts");
    fs::create_dir_all(&mounts_dir)?;
    let lock_path = mounts_dir.join(format!(
        "{}.lock",
        mount_point
            .to_string_lossy()
            .replace(['\\', '/', ':'], "_")
            .trim_matches('_')
            .to_lowercase()
    ));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    match lock.try_lock_exclusive() {
        Ok(()) => Ok(false),
        Err(error) if error.kind() == fs2::lock_contended_error().kind() => Ok(true),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "macos")]
fn wait_for_macos_mount_lock_release(
    client_root: &Path,
    mount_point: &Path,
    timeout: Duration,
    poll_interval: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let mount_active = macos_mount_is_active(mount_point)?;
        let lock_held = macos_mount_lock_is_held(client_root, mount_point)?;
        if !mount_active && !lock_held {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "macOS mount or lifecycle lock was not released for '{}' within {} ms",
                    mount_point.display(),
                    timeout.as_millis()
                ),
            ));
        }
        std::thread::sleep(poll_interval.max(Duration::from_millis(1)));
    }
}

#[cfg(any(target_os = "linux", test))]
fn mountinfo_contains_mount_point(mountinfo: &[u8], path: &Path) -> bool {
    mountinfo.split(|byte| *byte == b'\n').any(|line| {
        let Some(encoded_mount_point) = line
            .split(|byte| *byte == b' ')
            .filter(|field| !field.is_empty())
            .nth(4)
        else {
            return false;
        };
        decode_mountinfo_path(encoded_mount_point)
            .is_some_and(|decoded_mount_point| mountinfo_path_matches(&decoded_mount_point, path))
    })
}

#[cfg(any(target_os = "linux", test))]
fn mountinfo_path_matches(decoded_mount_point: &Path, expected: &Path) -> bool {
    decoded_mount_point.components().eq(expected.components())
}

#[cfg(any(target_os = "linux", test))]
fn decode_mountinfo_path(value: &[u8]) -> Option<PathBuf> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        // Linux mountinfo path fields escape bytes as backslash plus three
        // octal digits; see proc_pid_mountinfo(5).
        if value[index] == b'\\' && index + 3 < value.len() {
            let octal = &value[index + 1..index + 4];
            if octal.iter().all(|byte| matches!(byte, b'0'..=b'7')) {
                let byte = octal
                    .iter()
                    .fold(0, |value, digit| (value << 3) + (digit - b'0'));
                decoded.push(byte);
                index += 4;
                continue;
            }
        }
        decoded.push(value[index]);
        index += 1;
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;

        Some(PathBuf::from(OsString::from_vec(decoded)))
    }

    #[cfg(not(unix))]
    {
        String::from_utf8(decoded).ok().map(PathBuf::from)
    }
}

#[cfg(windows)]
pub(crate) fn mount_point_is_active(path: &Path) -> io::Result<bool> {
    let Some(root) = windows_drive_root(path) else {
        return Ok(false);
    };
    Ok(root.exists())
}

#[cfg(windows)]
#[derive(Debug, Deserialize)]
struct WindowsMountLifecycleRecord {
    pid: u32,
    status: String,
}

#[cfg(windows)]
fn windows_mount_lifecycle_is_ready(
    client_root: &Path,
    mount_point: &Path,
    process_id: u32,
) -> io::Result<bool> {
    let mount_name = mount_point
        .to_string_lossy()
        .replace(['\\', '/', ':'], "_")
        .trim_matches('_')
        .to_lowercase();
    let mount_name = if mount_name.is_empty() {
        "root"
    } else {
        mount_name.as_str()
    };
    let record_path = client_root
        .join("mounts")
        .join(format!("{mount_name}.json"));
    let data = match fs::read_to_string(record_path) {
        Ok(data) => data,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let record = match serde_json::from_str::<WindowsMountLifecycleRecord>(&data) {
        Ok(record) => record,
        Err(_) => return Ok(false),
    };

    Ok(record.pid == process_id
        && matches!(record.status.as_str(), "mounted" | "live" | "degraded"))
}

#[cfg(windows)]
fn windows_mount_io_is_ready(mount_point: &Path, write_capable: bool) -> io::Result<bool> {
    let probe_path = mount_point.join(format!(
        ".studiofs-e2e-readiness-{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    windows_mount_io_is_ready_with(|| {
        let mut options = OpenOptions::new();
        options.read(!write_capable).write(write_capable);
        options.open(&probe_path).map(|_| ())
    })
}

#[cfg(windows)]
fn windows_mount_io_is_ready_with<F>(probe: F) -> io::Result<bool>
where
    F: FnOnce() -> io::Result<()>,
{
    match probe() {
        Ok(()) => Ok(true),
        // The probe name is unique and is expected not to exist. A native
        // not-found response proves that the mount accepted the open request
        // and reached the filesystem callback.
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(windows)]
fn is_windows_drive_mount_point(path: &Path) -> bool {
    windows_drive_root(path).is_some()
}

#[cfg(not(windows))]
fn is_windows_drive_mount_point(_path: &Path) -> bool {
    false
}

#[cfg(target_os = "linux")]
pub(crate) fn cleanup_stale_mount_before_remount(path: &Path) -> io::Result<()> {
    match mount_point_is_active(path) {
        Ok(false) => Ok(()),
        Ok(true) => lazy_unmount_stale_mount(path, None),
        Err(err) => lazy_unmount_stale_mount(path, Some(err)),
    }
}

#[cfg(windows)]
pub(crate) fn cleanup_stale_mount_before_remount(path: &Path) -> io::Result<()> {
    // A force-killed WinFSP process can make the drive root disappear before
    // the kernel and Mount Manager have finished detaching it. The inactive
    // result therefore still needs a bounded stability wait.
    let readiness_error = mount_point_is_active(path).err();
    force_unmount_stale_windows_mount(path, readiness_error)
}

#[cfg(target_os = "macos")]
pub(crate) fn cleanup_stale_mount_before_remount(path: &Path) -> io::Result<()> {
    match mount_point_is_active(path) {
        Ok(false) => Ok(()),
        Ok(true) => force_unmount_stale_macos_mount(path, None),
        Err(err) => force_unmount_stale_macos_mount(path, Some(err)),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn cleanup_stale_mount_before_remount(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_stale_unmount_attempts() -> &'static [(&'static str, &'static [&'static str], Duration)] {
    &[
        ("umount", &["-f"], MACOS_STALE_UNMOUNT_ATTEMPT_TIMEOUT),
        ("umount", &[], MACOS_STALE_UNMOUNT_ATTEMPT_TIMEOUT),
        (
            "diskutil",
            &["unmount", "force"],
            MACOS_DISKUTIL_UNMOUNT_TIMEOUT,
        ),
    ]
}

#[cfg(target_os = "macos")]
fn create_macos_unmount_output_file(label: &str) -> io::Result<fs::File> {
    let dir = PathBuf::from("target").join("studiofs-e2e-unmount-output");
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("{label}-{}.log", uuid::Uuid::new_v4()));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    fs::remove_file(&path)?;
    Ok(file)
}

#[cfg(target_os = "macos")]
fn read_macos_unmount_output_file(file: &mut fs::File) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut output)?;
    Ok(output)
}

#[cfg(target_os = "macos")]
fn run_macos_unmount_attempt_with_timeout(
    program: &str,
    args: &[&str],
    path: &Path,
    timeout: Duration,
) -> io::Result<std::process::Output> {
    run_macos_command_with_timeout(program, args, Some(path), timeout)
}

#[cfg(target_os = "macos")]
fn run_macos_command_with_timeout(
    program: &str,
    args: &[&str],
    path: Option<&Path>,
    timeout: Duration,
) -> io::Result<std::process::Output> {
    let mut stdout_file = create_macos_unmount_output_file("stdout")?;
    let mut stderr_file = create_macos_unmount_output_file("stderr")?;
    let mut command = Command::new(program);
    command
        .args(args)
        .stdout(Stdio::from(stdout_file.try_clone()?))
        .stderr(Stdio::from(stderr_file.try_clone()?));
    if let Some(path) = path {
        command.arg(path);
    }
    ChildJob::prepare_command(&mut command)?;
    let mut child = command.spawn()?;
    let job = match ChildJob::assign_to_child(&child) {
        Ok(job) => job,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let target = path
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<none>".to_owned());
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            let stdout = read_macos_unmount_output_file(&mut stdout_file)?;
            let stderr = read_macos_unmount_output_file(&mut stderr_file)?;
            return Ok(std::process::Output {
                status,
                stdout,
                stderr,
            });
        }
        if started.elapsed() >= timeout {
            // Give Disk Arbitration clients a chance to cancel an in-flight
            // request before force-killing the process group. A hard kill can
            // leave the daemon-side unmount request active after this helper
            // returns, allowing it to detach a subsequent remount.
            let _ = job.terminate();
            if !wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE).unwrap_or(false) {
                let _ = job.force_kill();
                let _ = child.kill();
                let _ = wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE);
            }
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{} {} {} timed out after {} ms",
                    program,
                    args.join(" "),
                    target,
                    timeout.as_millis()
                ),
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(target_os = "macos")]
fn force_unmount_stale_macos_mount(
    path: &Path,
    readiness_error: Option<io::Error>,
) -> io::Result<()> {
    let mut diagnostics = Vec::new();
    for (program, args, timeout) in macos_stale_unmount_attempts() {
        match run_macos_unmount_attempt_with_timeout(program, args, path, *timeout) {
            Ok(output) if output.status.success() => match mount_point_is_active(path) {
                Ok(false) => return Ok(()),
                Ok(true) => diagnostics.push(format!(
                    "{} {} {} exited successfully but the mount is still active",
                    program,
                    args.join(" "),
                    path.display()
                )),
                Err(err) => diagnostics.push(format!(
                    "{} {} {} exited successfully but readiness probe failed after cleanup: {}",
                    program,
                    args.join(" "),
                    path.display(),
                    err
                )),
            },
            Ok(output) => diagnostics.push(format!(
                "{} {} {} exited with {}; stderr: {}; stdout: {}",
                program,
                args.join(" "),
                path.display(),
                output.status.code().unwrap_or(-1),
                diagnostic_excerpt(&String::from_utf8_lossy(&output.stderr)),
                diagnostic_excerpt(&String::from_utf8_lossy(&output.stdout))
            )),
            Err(err) if err.kind() == io::ErrorKind::NotFound => diagnostics.push(format!(
                "{} was not found while cleaning stale macOS mount {}",
                program,
                path.display()
            )),
            Err(err) => diagnostics.push(format!(
                "{} failed while cleaning stale macOS mount {}: {}",
                program,
                path.display(),
                err
            )),
        }
    }

    let readiness = readiness_error
        .map(|err| format!("; mount readiness check failed before cleanup: {err}"))
        .unwrap_or_default();
    let final_probe = match mount_point_is_active(path) {
        Ok(false) => return Ok(()),
        Ok(true) => String::new(),
        Err(err) => format!("; final mount readiness check failed after cleanup: {err}"),
    };
    Err(io::Error::other(format!(
        "failed to force-unmount stale macOS mount point {}{}{}; attempts: {}",
        path.display(),
        readiness,
        final_probe,
        diagnostics.join(" | ")
    )))
}

#[cfg(windows)]
fn force_unmount_stale_windows_mount(
    path: &Path,
    readiness_error: Option<io::Error>,
) -> io::Result<()> {
    let Some(root) = windows_drive_root(path) else {
        return Ok(());
    };
    let drive = windows_drive_arg(&root)?;
    let root_arg = root.display().to_string();
    let attempts: &[(&str, &[&str])] = &[
        ("net", &["use", drive.as_str(), "/delete", "/y"]),
        ("mountvol", &[root_arg.as_str(), "/d"]),
    ];

    let mut diagnostics = Vec::new();
    if mount_point_is_active(&root).unwrap_or(true) {
        for (program, args) in attempts {
            match Command::new(program).args(*args).output() {
                Ok(output) if output.status.success() => diagnostics.push(format!(
                    "{} {} exited successfully; waiting for {} to detach",
                    program,
                    args.join(" "),
                    root.display()
                )),
                Ok(output) => diagnostics.push(format!(
                    "{} {} exited with {}; stderr: {}; stdout: {}",
                    program,
                    args.join(" "),
                    output.status.code().unwrap_or(-1),
                    diagnostic_excerpt(&String::from_utf8_lossy(&output.stderr)),
                    diagnostic_excerpt(&String::from_utf8_lossy(&output.stdout))
                )),
                Err(err) if err.kind() == io::ErrorKind::NotFound => diagnostics.push(format!(
                    "{program} was not found while cleaning stale Windows mount {}",
                    root.display()
                )),
                Err(err) => diagnostics.push(format!(
                    "{program} failed while cleaning stale Windows mount {}: {err}",
                    root.display()
                )),
            }
        }
    }

    if let Err(error) = wait_for_windows_mount_detached_with(
        WINDOWS_STALE_UNMOUNT_TIMEOUT,
        WINDOWS_STALE_UNMOUNT_POLL_INTERVAL,
        || mount_point_is_active(&root),
    ) {
        let readiness = readiness_error
            .map(|err| format!("; readiness probe error: {err}"))
            .unwrap_or_default();
        return Err(io::Error::other(format!(
            "failed to force-unmount stale Windows mount {}{}; {}; attempts: {}",
            root.display(),
            readiness,
            error,
            diagnostics.join(" | ")
        )));
    }

    Ok(())
}

#[cfg(windows)]
fn wait_for_windows_mount_detached_with<F>(
    timeout: Duration,
    poll_interval: Duration,
    mut is_active: F,
) -> io::Result<()>
where
    F: FnMut() -> io::Result<bool>,
{
    let deadline = Instant::now() + timeout;
    let mut inactive_probes = 0;
    loop {
        if is_active()? {
            inactive_probes = 0;
        } else {
            inactive_probes += 1;
            if inactive_probes >= WINDOWS_STALE_UNMOUNT_STABLE_PROBES {
                return Ok(());
            }
        }

        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "Windows mount did not remain detached for {} consecutive probes within {} ms",
                    WINDOWS_STALE_UNMOUNT_STABLE_PROBES,
                    timeout.as_millis()
                ),
            ));
        }
        std::thread::sleep(poll_interval.max(Duration::from_millis(1)));
    }
}

#[cfg(windows)]
fn windows_drive_arg(root: &Path) -> io::Result<String> {
    root.display()
        .to_string()
        .strip_suffix('\\')
        .map(str::to_string)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "expected Windows drive root ending with '\\': {}",
                    root.display()
                ),
            )
        })
}

#[cfg(target_os = "linux")]
fn lazy_unmount_stale_mount(path: &Path, readiness_error: Option<io::Error>) -> io::Result<()> {
    let attempts: &[(&str, &[&str])] = &[
        ("fusermount3", &["-u", "-z"]),
        ("fusermount", &["-u", "-z"]),
        ("umount", &["-l"]),
    ];
    let mut diagnostics = Vec::new();
    for (program, args) in attempts {
        match Command::new(program).args(*args).arg(path).output() {
            Ok(output) if output.status.success() => return Ok(()),
            Ok(output) => diagnostics.push(format!(
                "{} {} {} exited with {}; stderr: {}; stdout: {}",
                program,
                args.join(" "),
                path.display(),
                output.status.code().unwrap_or(-1),
                diagnostic_excerpt(&String::from_utf8_lossy(&output.stderr)),
                diagnostic_excerpt(&String::from_utf8_lossy(&output.stdout))
            )),
            Err(err) if err.kind() == io::ErrorKind::NotFound => diagnostics.push(format!(
                "{} was not found while cleaning stale mount {}",
                program,
                path.display()
            )),
            Err(err) => diagnostics.push(format!(
                "{} failed while cleaning stale mount {}: {}",
                program,
                path.display(),
                err
            )),
        }
    }
    let readiness = readiness_error
        .map(|err| format!("; mount readiness check failed before cleanup: {err}"))
        .unwrap_or_default();
    Err(io::Error::other(format!(
        "failed to lazily unmount stale mount point {}{}; attempts: {}",
        path.display(),
        readiness,
        diagnostics.join(" | ")
    )))
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

#[cfg(windows)]
fn windows_available_drive_root(letter: char) -> Option<PathBuf> {
    (!windows_drive_letter_in_use(letter)).then(|| PathBuf::from(format!("{letter}:\\")))
}

#[cfg(windows)]
fn windows_drive_letter_in_use(letter: char) -> bool {
    windows_drive_letter_in_mask(letter, windows_logical_drive_mask())
}

#[cfg(windows)]
fn windows_drive_letter_in_mask(letter: char, mask: u32) -> bool {
    let upper = letter.to_ascii_uppercase();
    if !upper.is_ascii_uppercase() {
        return true;
    }
    let index = u32::from(upper) - u32::from('A');
    mask & (1_u32 << index) != 0
}

#[cfg(windows)]
fn windows_logical_drive_mask() -> u32 {
    unsafe extern "system" {
        fn GetLogicalDrives() -> u32;
    }

    // SAFETY: `GetLogicalDrives` takes no pointers and only returns the
    // process-visible drive-letter bitmask.
    unsafe { GetLogicalDrives() }
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn mount_point_is_active(path: &Path) -> io::Result<bool> {
    Ok(path.exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_metadata_persists_the_external_mount_root() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let spec = client_spec_for(
            temp.path(),
            "projectfs",
            "client-a",
            PathBuf::from("/Volumes/studiofs-review/client-a/projectfs"),
        );
        fs::create_dir_all(&spec.mount_metadata_dir).unwrap();

        write_mount_metadata(&spec, Some(Path::new("/Volumes/studiofs-review"))).unwrap();

        let metadata: serde_json::Value = serde_json::from_slice(
            &fs::read(spec.mount_metadata_dir.join("mount-point.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            metadata["mount_root"],
            serde_json::Value::String("/Volumes/studiofs-review".to_string())
        );
    }

    #[test]
    fn canonical_local_metadata_db_path_errors_include_client_and_path() {
        let path = Path::new("missing-client-root")
            .join("meta")
            .join("projectfs")
            .join("projectfs.db");

        let err = canonical_local_metadata_db_path("client-a", &path).unwrap_err();
        let message = err.to_string();

        assert!(matches!(
            err,
            ClientControllerError::CanonicalizeLocalMetadataDbFailed { .. }
        ));
        assert!(message.contains("client-a"));
        assert!(message.contains(&path.display().to_string()));
    }

    #[test]
    fn compose_mount_exit_timeout_matches_platform_policy() {
        let timeout = default_compose_mount_exit_timeout();

        if cfg!(target_os = "macos") {
            assert_eq!(timeout, Duration::from_secs(60));
        } else {
            assert_eq!(timeout, Duration::from_secs(15));
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_unmount_output_file_is_unlinked_immediately() {
        let label = format!("unlink-on-open-{}", uuid::Uuid::new_v4());
        let _file = create_macos_unmount_output_file(&label).expect("create output file");
        let dir = PathBuf::from("target").join("studiofs-e2e-unmount-output");
        let leaked = fs::read_dir(dir)
            .expect("read output directory")
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().starts_with(&label));

        assert!(
            !leaked,
            "an open unmount output file must not retain a directory entry"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_stale_mount_cleanup_uses_force_unmount_attempts() {
        let attempts = macos_stale_unmount_attempts();

        assert_eq!(
            attempts.first(),
            Some(&("umount", &["-f"][..], MACOS_STALE_UNMOUNT_ATTEMPT_TIMEOUT)),
            "macOS stale mount cleanup should prefer a synchronous forced unmount"
        );
        assert!(
            attempts.iter().any(|(program, args, timeout)| {
                *program == "diskutil"
                    && *args == ["unmount", "force"]
                    && *timeout == Duration::from_secs(60)
            }),
            "diskutil cleanup must wait for its delegated Disk Arbitration request"
        );
        assert!(
            attempts
                .iter()
                .any(|(program, args, _)| *program == "umount" && *args == ["-f"]),
            "macOS stale mount cleanup should fall back to forced umount"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_mount_activity_probe_matches_exact_mount_table_entry() {
        let mount_point = Path::new("/Volumes/studiofs/client-a/projectfs");
        let mount_table = b"/dev/disk5s1 on /Volumes/Storage (apfs, local)\n\\
macfuse on /Volumes/studiofs/client-a/projectfs (macfuse, local)\n";

        assert!(
            macos_mount_is_active_with(mount_point, || Ok(mount_table.to_vec()))
                .expect("probe mount table")
        );
        assert!(
            !macos_mount_is_active_with(
                Path::new("/Volumes/studiofs/client-a/projectfs/child"),
                || Ok(mount_table.to_vec())
            )
            .expect("probe child path")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_mount_activity_probe_handles_mountpoint_delimiters() {
        let mount_point = Path::new("/Volumes/studiofs/on (call)/projectfs");
        let mount_table = b"macfuse on /Volumes/studiofs/on (call)/projectfs (macfuse, local)\n";

        assert!(
            macos_mount_is_active_with(mount_point, || Ok(mount_table.to_vec()))
                .expect("probe mount table")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_mount_readiness_ignores_lifecycle_lock_after_fuse_detaches() {
        use fs2::FileExt as _;

        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let client_root = temp.path().join("client-root");
        let lock_path = client_root
            .join("mounts")
            .join("tmp_studiofs-e2e-lock-probe.lock");
        fs::create_dir_all(lock_path.parent().expect("lock parent"))
            .expect("create client mounts directory");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .expect("open lifecycle lock");
        lock.try_lock_exclusive().expect("hold lifecycle lock");
        let mount_point = Path::new("/tmp/studiofs-e2e-lock-probe");

        assert!(!macos_mount_is_active(mount_point).expect("probe active mount"));
        let error = wait_for_macos_mount_lock_release(
            &client_root,
            mount_point,
            Duration::ZERO,
            Duration::ZERO,
        )
        .expect_err("held lifecycle lock should not be reported as released");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        lock.unlock().expect("release lifecycle lock");
        drop(lock);
        assert!(!macos_mount_is_active(mount_point).expect("probe inactive mount"));
        wait_for_macos_mount_lock_release(
            &client_root,
            mount_point,
            Duration::ZERO,
            Duration::ZERO,
        )
        .expect("released lifecycle lock should be reusable");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_remount_waits_for_mount_detach_after_lifecycle_lock_release() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let client_root = temp.path().join("client-root");
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mount_point = manifest_dir
            .ancestors()
            .find(|path| macos_mount_is_active(path).expect("probe mount activity"))
            .expect("test checkout must be on a mounted filesystem")
            .to_path_buf();

        let error = wait_for_macos_mount_lock_release(
            &client_root,
            &mount_point,
            Duration::ZERO,
            Duration::ZERO,
        )
        .expect_err("remount must wait for an active mount even after its lock is free");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_stale_mount_cleanup_attempts_are_timeout_bounded() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let started = Instant::now();
        let err = run_macos_unmount_attempt_with_timeout(
            "sh",
            &["-c", "while :; do :; done"],
            temp.path(),
            Duration::from_millis(50),
        )
        .expect_err("blocking cleanup command should time out");

        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "cleanup timeout should bound a stuck unmount helper"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_stale_mount_cleanup_timeout_gracefully_terminates_helper() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let marker = temp.path().join("graceful-termination-marker");
        let error = run_macos_unmount_attempt_with_timeout(
            "sh",
            &[
                "-c",
                "trap 'touch \"$0\"; exit 0' TERM; while :; do :; done",
            ],
            &marker,
            Duration::from_millis(50),
        )
        .expect_err("blocking cleanup command should time out");

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            marker.exists(),
            "a timed-out cleanup command should receive termination before force-kill"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_stale_mount_cleanup_timeout_terminates_helper_descendants() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let marker = temp.path().join("descendant-marker");
        let error = run_macos_unmount_attempt_with_timeout(
            "sh",
            &[
                "-c",
                "nohup sh -c 'sleep 1; touch \"$1\"' sh \"$0\" >/dev/null 2>&1 & while :; do :; done",
            ],
            &marker,
            Duration::from_millis(50),
        )
        .expect_err("blocking cleanup command should time out");

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        std::thread::sleep(Duration::from_secs(2));
        assert!(
            !marker.exists(),
            "a timed-out cleanup command must not leave descendants running"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_stale_mount_cleanup_attempts_drain_large_output() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let output = run_macos_unmount_attempt_with_timeout(
            "sh",
            &["-c", "yes studiofs-unmount-output | head -c 200000"],
            temp.path(),
            Duration::from_secs(2),
        )
        .expect("large-output cleanup command should not block on full pipes");

        assert!(output.status.success());
        assert_eq!(output.stdout.len(), 200_000);
    }

    #[test]
    fn client_env_bounds_winfsp_metadata_timeouts_for_invalidation_runs() {
        let client = client_spec_for(
            Path::new("target/e2e-env-test"),
            "projectfs",
            "client-a",
            PathBuf::from("target/e2e-env-test/mount"),
        );
        let env = client_env(&client);
        let lookup = |key: &str| {
            env.iter()
                .find(|(candidate, _)| candidate == key)
                .map(|(_, value)| value.as_str())
        };

        for key in [
            WINFSP_FILE_INFO_TIMEOUT_ENV,
            WINFSP_DIR_INFO_TIMEOUT_ENV,
            WINFSP_VOLUME_INFO_TIMEOUT_ENV,
            WINFSP_SECURITY_TIMEOUT_ENV,
            WINFSP_STREAM_INFO_TIMEOUT_ENV,
            WINFSP_EXTENDED_ATTRIBUTE_TIMEOUT_ENV,
        ] {
            assert_eq!(
                lookup(key),
                Some(E2E_WINFSP_METADATA_TIMEOUT_MS),
                "{key} must be bounded to 1000ms for mounted E2E scenarios"
            );
        }
    }

    #[test]
    fn client_env_overrides_replace_winfsp_directory_timeout() {
        let client = client_spec_for(
            Path::new("target/e2e-env-test"),
            "projectfs",
            "client-a",
            PathBuf::from("target/e2e-env-test/mount"),
        );
        let overrides = [(WINFSP_DIR_INFO_TIMEOUT_ENV.to_string(), "0".to_string())];
        let env = client_env_with_overrides(&client, Some(&overrides));

        assert_eq!(
            env.iter()
                .find(|(key, _)| key == WINFSP_DIR_INFO_TIMEOUT_ENV)
                .map(|(_, value)| value.as_str()),
            Some("0")
        );
    }

    #[test]
    fn external_env_normalization_strips_angle_wrapped_dotenv_values() {
        assert_eq!(
            normalize_external_env_value("host=<100.123.245.11> port=5432 user=<studiofs>"),
            "host=100.123.245.11 port=5432 user=studiofs"
        );
        assert_eq!(
            normalize_external_env_value("\"<https://s3.example.test>\""),
            "https://s3.example.test"
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn configured_mount_root_assigns_one_external_root_per_client() {
        let mount_points =
            configured_mount_points(Some("/Volumes/studiofs"), &["client-a", "client-b"]).unwrap();

        assert_eq!(
            mount_points,
            [
                PathBuf::from("/Volumes/studiofs/client-a"),
                PathBuf::from("/Volumes/studiofs/client-b"),
            ]
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn configured_mount_root_requires_an_absolute_path() {
        let error = configured_mount_points(Some("target/e2e-mounts"), &["client-a"]).unwrap_err();

        assert!(matches!(
            error,
            ClientControllerError::InvalidMountRoot(path)
                if path == Path::new("target/e2e-mounts")
        ));
    }

    #[test]
    fn linux_mountinfo_fallback_matches_escaped_mount_point() {
        let path = Path::new("/var/mint-workspace/target/e2e-runs/run with space/mount");
        let mountinfo = "\
42 31 0:49 / / rw,relatime - overlay overlay rw\n\
88 42 0:77 / /var/mint-workspace/target/e2e-runs/run\\040with\\040space/mount rw,nosuid,nodev - fuse.studiofs studiofs rw\n";

        assert!(mountinfo_contains_mount_point(mountinfo.as_bytes(), path));
    }

    #[test]
    fn linux_mountinfo_fallback_ignores_trailing_slash_difference() {
        let path = Path::new("/var/mint-workspace/target/e2e-runs/run/mount");
        let mountinfo = "\
42 31 0:49 / / rw,relatime - overlay overlay rw\n\
88 42 0:77 / /var/mint-workspace/target/e2e-runs/run/mount/ rw,nosuid,nodev - fuse.studiofs studiofs rw\n";

        assert!(mountinfo_contains_mount_point(mountinfo.as_bytes(), path));
    }

    #[cfg(unix)]
    #[test]
    fn linux_mountinfo_detection_does_not_require_existing_mount_point_path() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let path = temp.path().join("missing-mount");
        let mountinfo = format!(
            "42 31 0:49 / / rw,relatime - overlay overlay rw\n\
88 42 0:77 / {} rw,nosuid,nodev - fuse.studiofs studiofs rw\n",
            path.display()
        );

        assert!(!path.exists());
        assert!(mountinfo_contains_mount_point(mountinfo.as_bytes(), &path));
    }

    #[cfg(unix)]
    #[test]
    fn linux_mountinfo_fallback_matches_non_utf8_mount_point_bytes() {
        use std::os::unix::ffi::OsStringExt as _;

        let path = PathBuf::from(OsString::from_vec(
            b"/var/mint-workspace/mount-\xff".to_vec(),
        ));
        let mountinfo = b"42 31 0:49 / / rw,relatime - overlay overlay rw\n\
88 42 0:77 / /var/mint-workspace/mount-\xff rw,nosuid,nodev - fuse.studiofs studiofs rw\n";

        assert!(mountinfo_contains_mount_point(mountinfo, &path));
    }

    #[cfg(not(unix))]
    #[test]
    fn decode_mountinfo_path_rejects_invalid_utf8_without_lossy_replacement() {
        assert!(decode_mountinfo_path(b"/var/mint-workspace/mount-\xff").is_none());
    }

    #[cfg(windows)]
    #[test]
    fn windows_drive_letter_mask_treats_mapped_letters_as_unavailable() {
        let y_bit = 1_u32 << (u32::from('Y') - u32::from('A'));
        let mask = y_bit;

        assert!(windows_drive_letter_in_mask('Y', mask));
        assert!(windows_drive_letter_in_mask('y', mask));
        assert!(!windows_drive_letter_in_mask('X', mask));
    }

    #[cfg(windows)]
    #[test]
    fn windows_drive_arg_strips_root_backslash_for_net_use() {
        assert_eq!(windows_drive_arg(Path::new("Z:\\")).unwrap(), "Z:");
    }

    #[cfg(windows)]
    #[test]
    fn windows_mount_readiness_requires_current_process_lifecycle_record() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let mount_point = PathBuf::from("Z:\\");
        let record_path = temp.path().join("mounts").join("z.json");
        fs::create_dir_all(record_path.parent().expect("record parent")).unwrap();
        fs::write(
            &record_path,
            serde_json::json!({"pid": 42, "status": "starting"}).to_string(),
        )
        .unwrap();

        assert!(!windows_mount_lifecycle_is_ready(temp.path(), &mount_point, 42).unwrap());

        fs::write(
            &record_path,
            serde_json::json!({"pid": 42, "status": "mounted"}).to_string(),
        )
        .unwrap();
        assert!(windows_mount_lifecycle_is_ready(temp.path(), &mount_point, 42).unwrap());
        assert!(!windows_mount_lifecycle_is_ready(temp.path(), &mount_point, 43).unwrap());
    }

    #[cfg(windows)]
    #[test]
    fn windows_mount_io_readiness_treats_driver_access_errors_as_transient() {
        assert!(windows_mount_io_is_ready_with(|| Ok(())).unwrap());
        assert!(
            !windows_mount_io_is_ready_with(|| {
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            })
            .unwrap()
        );
        assert!(
            windows_mount_io_is_ready_with(|| { Err(io::Error::from(io::ErrorKind::NotFound)) })
                .unwrap()
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_stale_mount_detach_requires_stable_inactive_observations() {
        let mut observations = [false, true, false, false, false].into_iter();
        let mut probes = 0;

        wait_for_windows_mount_detached_with(Duration::from_secs(1), Duration::ZERO, || {
            probes += 1;
            Ok(observations.next().unwrap_or(false))
        })
        .expect("mount should become stably inactive");

        assert_eq!(
            probes, 5,
            "a transient inactive probe must not finish detach"
        );
    }
}

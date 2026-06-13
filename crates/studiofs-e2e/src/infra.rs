//! Docker Compose infrastructure for real E2E runs.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, IsTerminal as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{ChildStderr, ChildStdout, Command, Stdio};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::process_diagnostics::capture_process_diagnostics_best_effort;
use crate::process_job::{CHILD_TERMINATION_GRACE, ChildJob, wait_for_child_exit};

const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 120;
const DEFAULT_POSTGRES_PORT: u16 = 25_432;
const DEFAULT_RUSTFS_PORT: u16 = 29_000;
const DEFAULT_RUSTFS_CONSOLE_PORT: u16 = 29_001;
const DEFAULT_TOXIPROXY_API_PORT: u16 = 28_474;
const COMPOSE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const COMPOSE_DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(30);
const PRIMARY_EXIT_OUTPUT_DRAIN_GRACE: Duration = Duration::from_millis(500);
#[cfg(windows)]
const TASKKILL_FALLBACK_TIMEOUT: Duration = Duration::from_secs(5);
const STREAM_OUTPUT_ENV: &str = "STUDIOFS_E2E_STREAM_OUTPUT";
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_YELLOW: &str = "\x1b[33m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_RED: &str = "\x1b[31m";

/// Compose project-name prefix used by infra helpers.
pub const COMPOSE_PROJECT_PREFIX: &str = "studiofs-e2e-";
/// Default service name for PostgreSQL in compose-based tests.
pub const POSTGRES_SERVICE_NAME: &str = "postgres";
/// Default service name for RustFS in compose-based tests.
pub const RUSTFS_SERVICE_NAME: &str = "rustfs";
const RUSTFS_CONSOLE_SERVICE_NAME: &str = "rustfs-console";
const TOXIPROXY_API_SERVICE_NAME: &str = "toxiproxy-api";

const SERVICE_PORTS: &[ServicePortSpec] = &[
    ServicePortSpec {
        service: POSTGRES_SERVICE_NAME,
        default_host_port: DEFAULT_POSTGRES_PORT,
        container_port: 5432,
        env_var: "STUDIOFS_E2E_POSTGRES_PORT",
    },
    ServicePortSpec {
        service: RUSTFS_SERVICE_NAME,
        default_host_port: DEFAULT_RUSTFS_PORT,
        container_port: 9000,
        env_var: "STUDIOFS_E2E_RUSTFS_PORT",
    },
    ServicePortSpec {
        service: RUSTFS_CONSOLE_SERVICE_NAME,
        default_host_port: DEFAULT_RUSTFS_CONSOLE_PORT,
        container_port: 9001,
        env_var: "STUDIOFS_E2E_RUSTFS_CONSOLE_PORT",
    },
    ServicePortSpec {
        service: TOXIPROXY_API_SERVICE_NAME,
        default_host_port: DEFAULT_TOXIPROXY_API_PORT,
        container_port: 8474,
        env_var: "STUDIOFS_E2E_TOXIPROXY_API_PORT",
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ServicePortSpec {
    service: &'static str,
    default_host_port: u16,
    container_port: u16,
    env_var: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Supported infra launch modes for E2E environment setup.
pub enum InfraMode {
    /// Infra stack is disabled.
    Disabled,
    /// Infra stack is managed via `docker compose`.
    Compose,
    /// Infra stack is provided by repository `.env` / process environment.
    Env,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Static configuration for compose stack creation.
pub struct ComposeConfig {
    /// Stable run id for naming and artifact grouping.
    pub run_id: String,
    /// Sandbox directory used to store run artifacts.
    pub sandbox: PathBuf,
    /// Resolved compose file path.
    pub compose_file: PathBuf,
    /// Optional explicit host port range.
    pub port_range: Option<String>,
    /// Timeout in seconds for compose wait operations.
    pub wait_timeout_secs: u64,
}

impl ComposeConfig {
    /// Builds a config from a run identifier and sandbox path.
    pub fn for_run(run_id: &str, sandbox: &Path, port_range: Option<String>) -> Self {
        Self {
            run_id: run_id.to_string(),
            sandbox: sandbox.to_path_buf(),
            compose_file: default_compose_file(),
            port_range,
            wait_timeout_secs: DEFAULT_WAIT_TIMEOUT_SECS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Bindings resolved for a compose service.
pub struct ServicePort {
    /// Compose service name.
    pub service: String,
    /// Bound host interface.
    pub host: String,
    /// Allocated host port number.
    pub host_port: u16,
    /// Exposed container port number.
    pub container_port: u16,
    /// Environment variable key used for this port.
    pub env_var: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Service image declaration discovered from compose metadata.
pub struct ComposeServiceImage {
    /// Compose service name.
    pub service: String,
    /// Image name in compose file.
    pub image: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Command specification used by infra helpers.
pub struct CommandSpec {
    /// Executable name/path.
    pub program: String,
    /// Arguments for the executable.
    pub args: Vec<String>,
    /// Environment variables injected for the process.
    pub env: Vec<(String, String)>,
    /// Optional working directory.
    pub cwd: Option<PathBuf>,
    /// Optional command timeout.
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Command execution output.
pub struct CommandOutput {
    /// Exit status code.
    pub status: i32,
    /// UTF-8 decoded standard output.
    pub stdout: String,
    /// UTF-8 decoded standard error.
    pub stderr: String,
}

/// Abstraction for executing infra commands.
pub trait CommandExecutor {
    /// Runs a prepared command and captures output.
    fn run(&self, command: &CommandSpec) -> std::io::Result<CommandOutput>;
}

#[derive(Debug, Clone, Copy, Default)]
/// Default executor used by infra helpers.
pub struct SystemCommandExecutor;

impl CommandExecutor for SystemCommandExecutor {
    fn run(&self, command: &CommandSpec) -> std::io::Result<CommandOutput> {
        let mut process = Command::new(&command.program);
        process.args(&command.args);
        process.envs(
            command
                .env
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        process.stdin(Stdio::null());
        if let Some(cwd) = &command.cwd {
            process.current_dir(cwd);
        }
        let stream_output = command_streams_output(command);
        process.stdout(Stdio::piped());
        process.stderr(Stdio::piped());
        run_command(
            process,
            command.timeout,
            stream_output,
            command_diagnostic_label(command),
        )
    }
}

/// Reads command env to determine whether output should be streamed.
pub fn command_streams_output(command: &CommandSpec) -> bool {
    command
        .env
        .iter()
        .any(|(key, value)| key == STREAM_OUTPUT_ENV && value == "1")
}

fn run_command(
    mut command: Command,
    timeout: Option<Duration>,
    stream_output: bool,
    diagnostic_label: String,
) -> std::io::Result<CommandOutput> {
    ChildJob::prepare_command(&mut command)?;
    let mut child = command.spawn()?;
    let job = match ChildJob::assign_to_child(&child) {
        Ok(job) => job,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::new(
                error.kind(),
                format!("failed to isolate command child in job object: {error}"),
            ));
        }
    };
    let mut stdout_reader = child
        .stdout
        .take()
        .map(|stdout| spawn_stdout_reader(stdout, stream_output));
    let mut stderr_reader = child
        .stderr
        .take()
        .map(|stderr| spawn_stderr_reader(stderr, stream_output));
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            let status = child.wait()?;
            terminate_descendants_after_primary_exit(&job);
            let stdout = collect_reader_after_primary_exit(stdout_reader.take())?;
            let stderr = collect_reader_after_primary_exit(stderr_reader.take())?;
            return Ok(CommandOutput {
                status: status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&stdout).into_owned(),
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            });
        }
        if timeout.is_some_and(|timeout| started.elapsed() >= timeout) {
            capture_process_diagnostics_best_effort(
                child.id(),
                &format!("command timeout: {diagnostic_label}"),
            );
            let job_terminate_error = job.terminate().err();
            let mut job_force_kill_error = None;
            let mut process_tree_error = None;
            let mut child_kill_error = None;
            let child_exit_detail = match wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE) {
                Ok(true) => None,
                Ok(false) => {
                    job_force_kill_error = job.force_kill().err();
                    process_tree_error = force_kill_process_tree(child.id()).err();
                    child_kill_error = child.kill().err();
                    match wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE) {
                        Ok(true) => None,
                        Ok(false) => Some("child still running after kill attempts".to_string()),
                        Err(error) => Some(format!(
                            "child exit probe failed after kill attempts: {error}"
                        )),
                    }
                }
                Err(error) => Some(format!(
                    "child exit probe failed after termination request: {error}"
                )),
            };
            discard_timed_out_reader(stdout_reader.take());
            discard_timed_out_reader(stderr_reader.take());
            let mut details = Vec::new();
            if let Some(error) = job_terminate_error {
                details.push(format!("job termination request failed: {error}"));
            }
            if let Some(error) = job_force_kill_error {
                details.push(format!("job force-kill failed: {error}"));
            }
            if let Some(error) = process_tree_error {
                details.push(format!("process-tree kill fallback failed: {error}"));
            }
            if let Some(error) = child_kill_error {
                details.push(format!("direct child kill failed: {error}"));
            }
            if let Some(detail) = child_exit_detail {
                details.push(detail);
            }
            let detail = if details.is_empty() {
                String::new()
            } else {
                format!("; {}", details.join("; "))
            };
            let timeout = timeout.expect("timeout checked above");
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("command timed out after {} ms{detail}", timeout.as_millis()),
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn command_diagnostic_label(command: &CommandSpec) -> String {
    let mut label = command.program.clone();
    if !command.args.is_empty() {
        label.push(' ');
        label.push_str(&command.args.join(" "));
    }
    label
}

fn terminate_descendants_after_primary_exit(job: &ChildJob) {
    let _ = job.terminate();
    let _ = job.force_kill();
}

#[cfg(windows)]
fn force_kill_process_tree(pid: u32) -> Result<(), String> {
    let mut taskkill = Command::new(taskkill_program())
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| err.to_string())?;
    let started = Instant::now();
    let status = loop {
        if let Some(status) = taskkill.try_wait().map_err(|err| err.to_string())? {
            break status;
        }
        if started.elapsed() >= TASKKILL_FALLBACK_TIMEOUT {
            let _ = taskkill.kill();
            let _ = taskkill.wait();
            return Err(format!(
                "taskkill timed out after {} ms",
                TASKKILL_FALLBACK_TIMEOUT.as_millis()
            ));
        }
        thread::sleep(Duration::from_millis(25));
    };
    match status.code() {
        Some(0 | 128) => Ok(()),
        _ => Err(format!("taskkill exited with status {status}")),
    }
}

#[cfg(windows)]
fn taskkill_program() -> PathBuf {
    std::env::var_os("SystemRoot")
        .map(PathBuf::from)
        .map(|root| root.join("System32").join("taskkill.exe"))
        .unwrap_or_else(|| PathBuf::from("taskkill.exe"))
}

#[cfg(not(windows))]
fn force_kill_process_tree(_pid: u32) -> Result<(), String> {
    Ok(())
}

fn spawn_stdout_reader(
    stdout: ChildStdout,
    stream_output: bool,
) -> JoinHandle<io::Result<Vec<u8>>> {
    thread::spawn(move || read_stream(stdout, OutputSink::Stdout, stream_output))
}

fn spawn_stderr_reader(
    stderr: ChildStderr,
    stream_output: bool,
) -> JoinHandle<io::Result<Vec<u8>>> {
    thread::spawn(move || read_stream(stderr, OutputSink::Stderr, stream_output))
}

#[derive(Debug, Clone, Copy)]
enum OutputSink {
    Stdout,
    Stderr,
}

fn read_stream<R: io::Read>(
    mut reader: R,
    sink: OutputSink,
    stream_output: bool,
) -> io::Result<Vec<u8>> {
    if !stream_output {
        let mut output = Vec::new();
        reader.read_to_end(&mut output)?;
        return Ok(output);
    }
    match sink {
        OutputSink::Stdout => read_stream_with_writer(reader, |bytes| {
            let mut stdout = io::stdout().lock();
            stdout.write_all(bytes)?;
            stdout.flush()
        }),
        OutputSink::Stderr => {
            let color_progress =
                io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none();
            read_stream_with_writer(reader, |bytes| {
                let display;
                let bytes = if color_progress {
                    display = colorize_progress_statuses_bytes(bytes);
                    display.as_slice()
                } else {
                    bytes
                };
                let mut stderr = io::stderr().lock();
                stderr.write_all(bytes)?;
                stderr.flush()
            })
        }
    }
}

fn read_stream_with_writer<R, F>(mut reader: R, mut write_chunk: F) -> io::Result<Vec<u8>>
where
    R: io::Read,
    F: FnMut(&[u8]) -> io::Result<()>,
{
    let mut output = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let read = reader.read(&mut chunk)?;
        if read == 0 {
            return Ok(output);
        }
        let bytes = &chunk[..read];
        output.extend_from_slice(bytes);
        write_chunk(bytes)?;
    }
}

fn colorize_progress_statuses_bytes(bytes: &[u8]) -> Vec<u8> {
    let Ok(output) = std::str::from_utf8(bytes) else {
        return bytes.to_vec();
    };
    colorize_progress_statuses(output).into_bytes()
}

fn colorize_progress_statuses(output: &str) -> String {
    output
        .lines()
        .map(colorize_progress_status_line)
        .collect::<Vec<_>>()
        .join("\n")
        + if output.ends_with('\n') { "\n" } else { "" }
}

fn colorize_progress_status_line(line: &str) -> String {
    for (status, color) in [
        ("started", ANSI_YELLOW),
        ("passed", ANSI_GREEN),
        ("failed", ANSI_RED),
    ] {
        let needle = format!("status={status}");
        if line.contains(&needle) {
            return line.replace(&needle, &format!("status={color}{status}{ANSI_RESET}"));
        }
    }
    line.to_string()
}

fn collect_reader_after_primary_exit(
    reader: Option<JoinHandle<io::Result<Vec<u8>>>>,
) -> io::Result<Vec<u8>> {
    let Some(reader) = reader else {
        return Ok(Vec::new());
    };
    let deadline = Instant::now() + PRIMARY_EXIT_OUTPUT_DRAIN_GRACE;
    // JoinHandle has no timeout; poll briefly so an inherited stdout/stderr
    // pipe cannot deadlock runner teardown after the primary child exits.
    while !reader.is_finished() {
        if Instant::now() >= deadline {
            discard_timed_out_reader(Some(reader));
            return Ok(Vec::new());
        }
        thread::sleep(Duration::from_millis(10));
    }
    reader
        .join()
        .map_err(|_| io::Error::other("command output reader thread panicked"))?
}

fn discard_timed_out_reader(reader: Option<JoinHandle<io::Result<Vec<u8>>>>) {
    drop(reader);
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Result of an infra preflight check.
pub struct InfraCheck {
    /// Human-readable check name.
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// Human-readable detail string for reporters.
    pub detail: String,
}

#[derive(Debug, Error)]
/// Infra-level errors emitted by helper operations.
pub enum InfraError {
    /// Compose file was expected but missing.
    #[error("compose file does not exist: {0}")]
    MissingComposeFile(PathBuf),
    /// Port range cannot be parsed.
    #[error("invalid port range: {0}")]
    InvalidPortRange(String),
    /// Declared range has too few ports for required services.
    #[error("port range {range} has {available} ports, but {needed} are required")]
    PortRangeTooSmall {
        /// Input range string.
        range: String,
        /// Number of available ports in range.
        available: usize,
        /// Number of required ports for all services.
        needed: usize,
    },
    /// Subprocess exited unsuccessfully.
    #[error("{action} failed with exit status {status}: {stderr}")]
    CommandFailed {
        /// Action description used for caller logs.
        action: String,
        /// Exit code from command.
        status: i32,
        /// Captured stderr payload.
        stderr: String,
    },
    /// Underlying I/O failure.
    #[error("infra I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// JSON encoding/decoding failure.
    #[error("infra JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// YAML encoding/decoding failure.
    #[error("infra YAML failed: {0}")]
    Yaml(#[from] serde_saphyr::Error),
}

#[derive(Debug, Clone)]
/// Docker compose stack helper used by real E2E scenarios.
pub struct DockerComposeStack {
    run_id: String,
    project_name: String,
    sandbox: PathBuf,
    compose_file: PathBuf,
    ports: Vec<ServicePort>,
    wait_timeout_secs: u64,
}

impl DockerComposeStack {
    /// Creates and validates a docker-compose stack state holder.
    pub fn new(config: ComposeConfig) -> Result<Self, InfraError> {
        if !config.compose_file.exists() {
            return Err(InfraError::MissingComposeFile(config.compose_file));
        }
        let compose_file = config.compose_file.canonicalize()?;
        let ports = allocate_ports(config.port_range.as_deref())?;
        Ok(Self {
            project_name: compose_project_name(&config.run_id),
            run_id: config.run_id,
            sandbox: config.sandbox,
            compose_file,
            ports,
            wait_timeout_secs: config.wait_timeout_secs,
        })
    }

    /// Returns the docker-compose project name.
    pub fn project_name(&self) -> &str {
        &self.project_name
    }

    /// Returns allocated ports for compose services.
    pub fn ports(&self) -> &[ServicePort] {
        &self.ports
    }

    /// Returns composed service images from the resolved compose file.
    pub fn compose_images(&self) -> Result<Vec<ComposeServiceImage>, InfraError> {
        Ok(compose_service_images(&fs::read_to_string(
            &self.compose_file,
        )?)?)
    }

    /// Prepares compose and port artifacts under the sandbox.
    pub fn prepare_artifacts(&self) -> Result<(), InfraError> {
        fs::create_dir_all(self.docker_artifact_dir())?;
        fs::copy(&self.compose_file, self.artifact_compose_file())?;
        fs::write(
            self.sandbox.join("ports.json"),
            serde_json::to_vec_pretty(&serde_json::json!({ "ports": self.ports }))?,
        )?;
        Ok(())
    }

    /// Runs preflight checks for compose and required network resources.
    pub fn preflight(&self, executor: &dyn CommandExecutor) -> Vec<InfraCheck> {
        let mut checks = Vec::new();
        checks.push(self.compose_contract_check());
        checks.push(self.command_check(
            "docker version",
            executor,
            CommandSpec {
                program: "docker".to_string(),
                args: vec![
                    "version".to_string(),
                    "--format".to_string(),
                    "{{.Server.Version}}".to_string(),
                ],
                env: Vec::new(),
                cwd: None,
                timeout: Some(COMPOSE_DIAGNOSTIC_TIMEOUT),
            },
        ));
        checks.push(self.command_check(
            "docker compose version",
            executor,
            CommandSpec {
                program: "docker".to_string(),
                args: vec!["compose".to_string(), "version".to_string()],
                env: Vec::new(),
                cwd: None,
                timeout: Some(COMPOSE_DIAGNOSTIC_TIMEOUT),
            },
        ));
        checks.extend(self.ports.iter().map(port_check));
        checks
    }

    fn compose_contract_check(&self) -> InfraCheck {
        let Ok(compose) = fs::read_to_string(&self.compose_file) else {
            return InfraCheck {
                name: "compose baseline contract".to_string(),
                passed: false,
                detail: format!("failed to read {}", self.compose_file.display()),
            };
        };
        let images = match compose_service_images(&compose) {
            Ok(images) => images,
            Err(error) => {
                return InfraCheck {
                    name: "compose baseline contract".to_string(),
                    passed: false,
                    detail: format!("failed to parse compose YAML: {error}"),
                };
            }
        };
        let mut errors = Vec::new();
        if !images
            .iter()
            .any(|image| image.image == "postgres:18-alpine")
        {
            errors.push("PostgreSQL image must stay pinned to postgres:18-alpine");
        }
        if !images
            .iter()
            .any(|image| image.image == "rustfs/rustfs:1.0.0-alpha.94")
        {
            errors.push("RustFS image must stay pinned to rustfs/rustfs:1.0.0-alpha.94");
        }
        if images
            .iter()
            .any(|image| image.image == "rustfs/rustfs:latest")
        {
            errors.push("RustFS latest is rejected by the Local First baseline gate");
        }

        InfraCheck {
            name: "compose baseline contract".to_string(),
            passed: errors.is_empty(),
            detail: if errors.is_empty() {
                "PostgreSQL and RustFS images match the Local First baseline".to_string()
            } else {
                errors.join("; ")
            },
        }
    }

    /// Returns the command spec for running `docker compose up`.
    pub fn up_command(&self) -> CommandSpec {
        let mut command = self.compose_command();
        command.args.extend([
            "up".to_string(),
            "--detach".to_string(),
            "--wait".to_string(),
            "--wait-timeout".to_string(),
            self.wait_timeout_secs.to_string(),
        ]);
        command
    }

    /// Executes compose up and waits for services to become ready.
    pub fn start(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        run_success(executor, "docker compose up --wait", self.up_command())?;
        Ok(())
    }

    /// Stops all compose services.
    pub fn stop(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.push("stop".to_string());
        run_success(executor, "docker compose stop", command)?;
        Ok(())
    }

    /// Restarts all compose services.
    pub fn restart(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.push("restart".to_string());
        run_success(executor, "docker compose restart", command)?;
        self.start(executor)?;
        Ok(())
    }

    /// Stops one service in this stack.
    pub fn stop_service(
        &self,
        service: &str,
        executor: &dyn CommandExecutor,
    ) -> Result<(), InfraError> {
        self.service_control().stop_service(service, executor)
    }

    /// Starts one service in this stack.
    pub fn start_service(
        &self,
        service: &str,
        executor: &dyn CommandExecutor,
    ) -> Result<(), InfraError> {
        self.service_control().start_service(service, executor)
    }

    /// Collects logs into artifact files and returns the stdout file path.
    pub fn collect_logs(&self, executor: &dyn CommandExecutor) -> Result<PathBuf, InfraError> {
        fs::create_dir_all(self.docker_artifact_dir())?;
        let mut command = self.compose_command();
        command.timeout = Some(COMPOSE_CLEANUP_TIMEOUT);
        command.args.extend([
            "logs".to_string(),
            "--no-color".to_string(),
            "--timestamps".to_string(),
        ]);
        let output = run_success(executor, "docker compose logs", command)?;
        let log_path = self.docker_artifact_dir().join("compose.log");
        fs::write(&log_path, output.stdout)?;
        fs::write(
            self.docker_artifact_dir().join("compose.stderr.log"),
            output.stderr,
        )?;
        Ok(log_path)
    }

    /// Collects optional diagnostic artifacts when needed.
    pub fn collect_optional_diagnostics(
        &self,
        executor: &dyn CommandExecutor,
    ) -> Result<Vec<String>, InfraError> {
        fs::create_dir_all(self.docker_artifact_dir())?;
        let mut collected = Vec::new();
        if self.collect_optional_command(
            executor,
            "postgres metadata dump",
            &[
                "exec",
                "-T",
                POSTGRES_SERVICE_NAME,
                "pg_dump",
                "--username",
                "studiofs",
                "--dbname",
                "studiofs",
                "--schema-only",
            ],
            "postgres-schema.sql",
            "postgres-schema.stderr.log",
            "postgres-schema.status.json",
        )? {
            collected.push("postgres metadata dump".to_string());
        }
        if self.collect_optional_command(
            executor,
            "rustfs object listing",
            &[
                "exec",
                "-T",
                RUSTFS_SERVICE_NAME,
                "sh",
                "-c",
                "find /data -mindepth 1 -maxdepth 8 -print | sort",
            ],
            "rustfs-object-listing.txt",
            "rustfs-object-listing.stderr.log",
            "rustfs-object-listing.status.json",
        )? {
            collected.push("rustfs object listing".to_string());
        }
        Ok(collected)
    }

    /// Captures per-volume PostgreSQL metadata state after a scenario finishes.
    ///
    /// The artifact is intentionally keyed by `vol`/volume name so long runs can
    /// prove that one scenario's volume did not bleed into another scenario's
    /// metadata stream.
    pub fn collect_post_scenario_postgres_state(
        &self,
        executor: &dyn CommandExecutor,
        scenario: &str,
        scenario_status: &str,
    ) -> Result<PathBuf, InfraError> {
        let artifact = self
            .sandbox
            .join("artifacts")
            .join("scenarios")
            .join(scenario)
            .join("post-scenario-postgres-state.json");
        let sql = r#"
SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object(
            'vol', v.vol,
            'name', v.name,
            'uuid', v.uuid,
            'metadata_version', ms.version,
            'event_count', COALESCE(events.event_count, 0),
            'max_seq_id', events.max_seq_id,
            'last_event_created_at', events.last_event_created_at,
            'event_types', COALESCE(event_types.items, '[]'::jsonb),
            'recent_events', COALESCE(recent_events.items, '[]'::jsonb)
        )
        ORDER BY v.name
    ),
    '[]'::jsonb
)::text
FROM volumes v
LEFT JOIN metadata_state ms ON ms.vol = v.vol
LEFT JOIN (
    SELECT
        vol,
        COUNT(*) AS event_count,
        MAX(seq_id) AS max_seq_id,
        MAX(created_at) AS last_event_created_at
    FROM event_log
    GROUP BY vol
) events ON events.vol = v.vol
LEFT JOIN LATERAL (
    SELECT jsonb_agg(
        jsonb_build_object('event_type', event_type, 'count', event_count)
        ORDER BY event_type
    ) AS items
    FROM (
        SELECT event_type, COUNT(*) AS event_count
        FROM event_log
        WHERE vol = v.vol
        GROUP BY event_type
    ) grouped_event_types
) event_types ON TRUE
LEFT JOIN LATERAL (
    SELECT jsonb_agg(
        jsonb_build_object(
            'seq_id', seq_id,
            'event_type', event_type,
            'client_id', client_id,
            'created_at', created_at
        )
        ORDER BY seq_id
    ) AS items
    FROM (
        SELECT seq_id, event_type, client_id, created_at
        FROM event_log
        WHERE vol = v.vol
        ORDER BY seq_id DESC
        LIMIT 20
    ) recent
) recent_events ON TRUE;
"#;
        let mut command = self.compose_command();
        command.timeout = Some(COMPOSE_DIAGNOSTIC_TIMEOUT);
        command.args.extend([
            "exec".to_string(),
            "-T".to_string(),
            POSTGRES_SERVICE_NAME.to_string(),
            "psql".to_string(),
            "--username".to_string(),
            "studiofs".to_string(),
            "--dbname".to_string(),
            "studiofs".to_string(),
            "--tuples-only".to_string(),
            "--no-align".to_string(),
            "--command".to_string(),
            sql.to_string(),
        ]);
        let output = executor.run(&command)?;
        let stdout = output.stdout.trim();
        let volumes = if output.status == 0 {
            serde_json::from_str::<serde_json::Value>(stdout)
                .unwrap_or_else(|_| serde_json::Value::String(stdout.to_string()))
        } else {
            serde_json::Value::Null
        };
        if let Some(parent) = artifact.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            &artifact,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "run_id": &self.run_id,
                "compose_project": &self.project_name,
                "scenario": scenario,
                "scenario_status": scenario_status,
                "postgres_query_status": output.status,
                "volumes": volumes,
                "stdout": output.stdout,
                "stderr": output.stderr,
            }))?,
        )?;
        Ok(artifact)
    }

    /// Runs compose down and removes volumes/orphans with cleanup timeout.
    pub fn teardown(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.timeout = Some(COMPOSE_CLEANUP_TIMEOUT);
        command.args.extend([
            "down".to_string(),
            "--volumes".to_string(),
            "--remove-orphans".to_string(),
        ]);
        run_success(
            executor,
            "docker compose down --volumes --remove-orphans",
            command,
        )?;
        Ok(())
    }

    fn collect_optional_command(
        &self,
        executor: &dyn CommandExecutor,
        action: &str,
        args: &[&str],
        stdout_file: &str,
        stderr_file: &str,
        status_file: &str,
    ) -> Result<bool, InfraError> {
        let mut command = self.compose_command();
        command.timeout = Some(COMPOSE_DIAGNOSTIC_TIMEOUT);
        command
            .args
            .extend(args.iter().map(|arg| (*arg).to_string()));
        let output = executor.run(&command)?;
        let artifact_dir = self.docker_artifact_dir();
        let collected = output.status == 0;
        fs::write(artifact_dir.join(stdout_file), &output.stdout)?;
        fs::write(artifact_dir.join(stderr_file), &output.stderr)?;
        fs::write(
            artifact_dir.join(status_file),
            serde_json::to_vec_pretty(&serde_json::json!({
                "action": action,
                "status": output.status,
                "collected": collected,
            }))?,
        )?;
        Ok(collected)
    }

    fn compose_command(&self) -> CommandSpec {
        self.service_control().compose_command()
    }

    fn compose_environment(&self) -> Vec<(String, String)> {
        let mut env = vec![("STUDIOFS_E2E_RUN_ID".to_string(), self.run_id.clone())];
        env.extend(
            self.ports
                .iter()
                .map(|port| (port.env_var.clone(), port.host_port.to_string())),
        );
        env
    }

    fn command_check(
        &self,
        name: &str,
        executor: &dyn CommandExecutor,
        command: CommandSpec,
    ) -> InfraCheck {
        match executor.run(&command) {
            Ok(output) if output.status == 0 => InfraCheck {
                name: name.to_string(),
                passed: true,
                detail: output.stdout.trim().to_string(),
            },
            Ok(output) => InfraCheck {
                name: name.to_string(),
                passed: false,
                detail: command_failure_detail(output.status, &output.stderr),
            },
            Err(err) => InfraCheck {
                name: name.to_string(),
                passed: false,
                detail: err.to_string(),
            },
        }
    }

    fn docker_artifact_dir(&self) -> PathBuf {
        self.sandbox.join("artifacts/docker")
    }

    fn artifact_compose_file(&self) -> PathBuf {
        self.docker_artifact_dir().join("compose.e2e.yml")
    }

    fn service_control(&self) -> ComposeServiceControl {
        ComposeServiceControl {
            project_name: self.project_name.clone(),
            compose_file: self.compose_file.clone(),
            wait_timeout_secs: self.wait_timeout_secs,
            env: self.compose_environment(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ComposeFile<'a> {
    #[serde(borrow, default)]
    services: BTreeMap<Cow<'a, str>, ComposeService<'a>>,
}

#[derive(Debug, Deserialize)]
struct ComposeService<'a> {
    #[serde(borrow)]
    image: Option<Cow<'a, str>>,
}

fn compose_service_images(compose: &str) -> Result<Vec<ComposeServiceImage>, serde_saphyr::Error> {
    let compose_file: ComposeFile<'_> = serde_saphyr::from_str(compose)?;
    Ok(compose_file
        .services
        .into_iter()
        .filter_map(|(service_name, service)| {
            service.image.map(|image| ComposeServiceImage {
                service: service_name.into_owned(),
                image: image.into_owned(),
            })
        })
        .collect())
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Compose service control handle for an existing project.
pub struct ComposeServiceControl {
    /// Compose project name.
    pub project_name: String,
    /// Absolute or relative compose file path used by operations.
    pub compose_file: PathBuf,
    /// Default wait timeout for compose commands.
    pub wait_timeout_secs: u64,
    env: Vec<(String, String)>,
}

impl ComposeServiceControl {
    /// Creates a compose control handle for the provided project name.
    pub fn for_project(project_name: String) -> Result<Self, InfraError> {
        Self::for_project_with_environment(project_name, Vec::new())
    }

    /// Creates a compose control handle with explicit environment variables.
    pub fn for_project_with_environment(
        project_name: String,
        env: Vec<(String, String)>,
    ) -> Result<Self, InfraError> {
        let compose_file = default_compose_file();
        if !compose_file.exists() {
            return Err(InfraError::MissingComposeFile(compose_file));
        }
        Ok(Self {
            project_name,
            compose_file: compose_file.canonicalize()?,
            wait_timeout_secs: DEFAULT_WAIT_TIMEOUT_SECS,
            env,
        })
    }

    /// Stops a compose service in this project.
    pub fn stop_service(
        &self,
        service: &str,
        executor: &dyn CommandExecutor,
    ) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command
            .args
            .extend(["stop".to_string(), service.to_string()]);
        run_success(executor, &format!("docker compose stop {service}"), command)?;
        Ok(())
    }

    /// Starts a compose service in this project.
    pub fn start_service(
        &self,
        service: &str,
        executor: &dyn CommandExecutor,
    ) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.extend([
            "up".to_string(),
            "--detach".to_string(),
            "--wait".to_string(),
            "--wait-timeout".to_string(),
            self.wait_timeout_secs.to_string(),
            service.to_string(),
        ]);
        run_success(
            executor,
            &format!("docker compose up --wait {service}"),
            command,
        )?;
        Ok(())
    }

    /// Tears down this compose project and cleans up resources.
    pub fn teardown(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.timeout = Some(COMPOSE_CLEANUP_TIMEOUT);
        command.args.extend([
            "down".to_string(),
            "--volumes".to_string(),
            "--remove-orphans".to_string(),
        ]);
        run_success(
            executor,
            "docker compose down --volumes --remove-orphans",
            command,
        )?;
        Ok(())
    }

    fn compose_command(&self) -> CommandSpec {
        CommandSpec {
            program: "docker".to_string(),
            args: vec![
                "compose".to_string(),
                "--project-name".to_string(),
                self.project_name.clone(),
                "--file".to_string(),
                self.compose_file.display().to_string(),
            ],
            env: self.env.clone(),
            cwd: self.compose_file.parent().map(Path::to_path_buf),
            timeout: None,
        }
    }
}

fn run_success(
    executor: &dyn CommandExecutor,
    action: &str,
    command: CommandSpec,
) -> Result<CommandOutput, InfraError> {
    let output = executor.run(&command)?;
    if output.status == 0 {
        Ok(output)
    } else {
        Err(InfraError::CommandFailed {
            action: action.to_string(),
            status: output.status,
            stderr: output.stderr,
        })
    }
}

fn port_check(port: &ServicePort) -> InfraCheck {
    match TcpListener::bind((port.host.as_str(), port.host_port)) {
        Ok(listener) => {
            drop(listener);
            InfraCheck {
                name: format!("port {} {}", port.service, port.host_port),
                passed: true,
                detail: "available on 127.0.0.1".to_string(),
            }
        }
        Err(err) => InfraCheck {
            name: format!("port {} {}", port.service, port.host_port),
            passed: false,
            detail: err.to_string(),
        },
    }
}

fn command_failure_detail(status: i32, stderr: &str) -> String {
    let stderr = stderr.trim();
    if stderr.is_empty() {
        format!("exit status {status}")
    } else {
        format!("exit status {status}: {stderr}")
    }
}

fn allocate_ports(range: Option<&str>) -> Result<Vec<ServicePort>, InfraError> {
    let host_ports = if let Some(range) = range {
        parse_port_range(range, SERVICE_PORTS.len())?
    } else {
        SERVICE_PORTS
            .iter()
            .map(|spec| spec.default_host_port)
            .collect()
    };
    Ok(SERVICE_PORTS
        .iter()
        .zip(host_ports)
        .map(|(spec, host_port)| service_port(*spec, host_port))
        .collect())
}

fn parse_port_range(range: &str, needed: usize) -> Result<Vec<u16>, InfraError> {
    let Some((start, end)) = range.split_once('-') else {
        return Err(InfraError::InvalidPortRange(range.to_string()));
    };
    let start = start
        .trim()
        .parse::<u16>()
        .map_err(|_| InfraError::InvalidPortRange(range.to_string()))?;
    let end = end
        .trim()
        .parse::<u16>()
        .map_err(|_| InfraError::InvalidPortRange(range.to_string()))?;
    if start > end {
        return Err(InfraError::InvalidPortRange(range.to_string()));
    }
    let available = usize::from(end - start) + 1;
    if available < needed {
        return Err(InfraError::PortRangeTooSmall {
            range: range.to_string(),
            available,
            needed,
        });
    }
    Ok((start..).take(needed).collect())
}

fn service_port(spec: ServicePortSpec, host_port: u16) -> ServicePort {
    ServicePort {
        service: spec.service.to_string(),
        host: "127.0.0.1".to_string(),
        host_port,
        container_port: spec.container_port,
        env_var: spec.env_var.to_string(),
    }
}

fn compose_project_name(run_id: &str) -> String {
    let normalized: String = run_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    format!("{COMPOSE_PROJECT_PREFIX}{normalized}")
}

fn default_compose_file() -> PathBuf {
    const COMPOSE_FILE: &str = "docker/compose.e2e.yml";
    if let Some(path) = std::env::var_os("STUDIOFS_E2E_COMPOSE_FILE") {
        return PathBuf::from(path);
    }
    if let Ok(cwd) = std::env::current_dir() {
        let from_cwd = cwd.join(COMPOSE_FILE);
        if from_cwd.exists() {
            return from_cwd;
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        for ancestor in exe.ancestors() {
            let candidate = ancestor.join(COMPOSE_FILE);
            if candidate.exists() {
                return candidate;
            }
        }
    }
    PathBuf::from(COMPOSE_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    #[test]
    fn system_command_executor_returns_timed_out_for_hung_command() {
        let _guard = crate::process_job::child_job_test_lock();
        let temp = tempfile::tempdir().unwrap();
        let diagnostics_dir = temp.path().join("process-diagnostics");
        crate::process_diagnostics::set_process_diagnostics_dir(diagnostics_dir.clone()).unwrap();
        let command = hung_command(Duration::from_millis(250));
        let started = Instant::now();

        let err = SystemCommandExecutor.run(&command).unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(4500),
            "timeout-aware executor should not wait for the hung command to finish; elapsed={elapsed:?}"
        );
        let diagnostics = std::fs::read_dir(diagnostics_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert!(
            diagnostics
                .iter()
                .any(|path| path.extension() == Some(std::ffi::OsStr::new("json"))),
            "command timeout should capture process diagnostics before kill"
        );
    }

    #[test]
    fn timeout_cleanup_detaches_blocked_output_reader() {
        let blocked_reader = thread::spawn(|| {
            thread::sleep(Duration::from_secs(5));
            Ok(Vec::new())
        });
        let started = Instant::now();

        discard_timed_out_reader(Some(blocked_reader));

        assert!(
            started.elapsed() < Duration::from_millis(200),
            "timeout cleanup must not join blocked output reader threads"
        );
    }

    #[test]
    fn primary_exit_cleanup_detaches_blocked_output_reader() {
        let blocked_reader = thread::spawn(|| {
            thread::sleep(Duration::from_secs(5));
            Ok(Vec::new())
        });
        let started = Instant::now();

        let output = collect_reader_after_primary_exit(Some(blocked_reader))
            .expect("blocked reader should be detached after primary exit");

        assert!(output.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "primary-exit cleanup must not join reader threads held open by escaped descendants"
        );
    }

    #[test]
    fn streaming_reader_does_not_hold_output_lock_while_waiting_for_pipe() {
        let output_lock = Arc::new(Mutex::new(()));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let reader = BlockingReader {
            entered: Some(entered_tx),
            release: release_rx,
        };
        let writer_lock = Arc::clone(&output_lock);

        let reader_thread = thread::spawn(move || {
            read_stream_with_writer(reader, |_bytes| {
                let _guard = writer_lock.lock().expect("output lock poisoned");
                Ok(())
            })
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reader should enter the blocking pipe read");
        let guard = output_lock
            .try_lock()
            .expect("streaming reader must not hold the output lock while blocked on pipe read");
        drop(guard);

        release_tx
            .send(())
            .expect("blocking reader release channel should be open");
        let output = reader_thread
            .join()
            .expect("reader thread should not panic")
            .expect("reader should finish cleanly");
        assert!(output.is_empty());
    }

    #[test]
    fn blocked_streaming_stderr_reader_does_not_block_later_stderr_writes() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let reader = BlockingReader {
            entered: Some(entered_tx),
            release: release_rx,
        };

        let reader_thread = thread::spawn(move || read_stream(reader, OutputSink::Stderr, true));

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stderr reader should enter the blocking pipe read");

        let (wrote_tx, wrote_rx) = mpsc::channel();
        let writer_thread = thread::spawn(move || {
            let mut stderr = io::stderr().lock();
            let result = writeln!(stderr, "studiofs-e2e stderr lock probe");
            let _ = wrote_tx.send(result);
        });

        wrote_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocked streaming reader must not hold stderr lock")
            .expect("stderr lock probe should write successfully");

        release_tx
            .send(())
            .expect("blocking reader release channel should be open");
        let output = reader_thread
            .join()
            .expect("reader thread should not panic")
            .expect("reader should finish cleanly");
        assert!(output.is_empty());
        writer_thread
            .join()
            .expect("writer thread should not panic");
    }

    #[test]
    fn timeout_executor_drains_large_stdout_while_waiting() {
        let _guard = crate::process_job::child_job_test_lock();
        let command = large_stdout_command(Duration::from_secs(10));

        let output = SystemCommandExecutor
            .run(&command)
            .expect("large-output command should finish before timeout");

        assert_eq!(output.status, 0);
        assert!(output.stdout.len() >= 1_000_000);
    }

    #[test]
    fn executor_does_not_wait_for_descendant_pipe_handles_after_primary_exit() {
        let _guard = crate::process_job::child_job_test_lock();
        let command = exits_with_descendant_holding_stdio(Duration::from_secs(10));
        let started = Instant::now();

        let output = SystemCommandExecutor
            .run(&command)
            .expect("primary child exit should be reported without waiting for descendants");

        assert_eq!(output.status, 7);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "executor must close the child job before joining output reader threads"
        );
    }

    #[test]
    fn command_streams_output_only_when_enabled() {
        let mut command = large_stdout_command(Duration::from_secs(5));

        assert!(!command_streams_output(&command));

        command
            .env
            .push(("STUDIOFS_E2E_STREAM_OUTPUT".to_string(), "1".to_string()));

        assert!(command_streams_output(&command));
    }

    #[test]
    fn streaming_command_still_captures_output_without_timeout() {
        let _guard = crate::process_job::child_job_test_lock();
        let mut command = small_output_command(None);
        command
            .env
            .push(("STUDIOFS_E2E_STREAM_OUTPUT".to_string(), "1".to_string()));

        let output = SystemCommandExecutor
            .run(&command)
            .expect("streamed command should still complete");

        assert_eq!(output.status, 0);
        assert_eq!(output.stdout.trim(), "streamed stdout");
        assert_eq!(output.stderr.trim(), "streamed stderr");
    }

    #[test]
    fn stream_progress_colorizer_colors_status_values_and_keeps_heartbeat_raw() {
        assert_eq!(
            String::from_utf8(colorize_progress_statuses_bytes(
                b"timestamp=t studiofs-e2e phase=run status=started\n\
timestamp=t studiofs-e2e phase=run status=passed\n\
timestamp=t studiofs-e2e phase=run status=failed\n",
            ))
            .unwrap(),
            "timestamp=t studiofs-e2e phase=run status=\x1b[33mstarted\x1b[0m\n\
timestamp=t studiofs-e2e phase=run status=\x1b[32mpassed\x1b[0m\n\
timestamp=t studiofs-e2e phase=run status=\x1b[31mfailed\x1b[0m\n",
        );
        assert_eq!(
            colorize_progress_statuses_bytes(b"\r****"),
            b"\r****".to_vec()
        );
    }

    struct BlockingReader {
        entered: Option<mpsc::Sender<()>>,
        release: mpsc::Receiver<()>,
    }

    impl io::Read for BlockingReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            let _ = self.release.recv();
            Ok(0)
        }
    }

    #[cfg(windows)]
    fn hung_command(timeout: Duration) -> CommandSpec {
        CommandSpec {
            program: "powershell".to_string(),
            args: vec![
                "-NoLogo".to_string(),
                "-NoProfile".to_string(),
                "-NonInteractive".to_string(),
                "-Command".to_string(),
                "[System.Threading.Thread]::Sleep(5000)".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            timeout: Some(timeout),
        }
    }

    #[cfg(windows)]
    fn large_stdout_command(timeout: Duration) -> CommandSpec {
        CommandSpec {
            program: "powershell".to_string(),
            args: vec![
                "-NoProfile".to_string(),
                "-Command".to_string(),
                "$bytes = [byte[]]::new(1048576); [Console]::OpenStandardOutput().Write($bytes, 0, $bytes.Length)".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            timeout: Some(timeout),
        }
    }

    #[cfg(windows)]
    fn small_output_command(timeout: Option<Duration>) -> CommandSpec {
        CommandSpec {
            program: "powershell".to_string(),
            args: vec![
                "-NoProfile".to_string(),
                "-Command".to_string(),
                "[Console]::Out.WriteLine('streamed stdout'); [Console]::Error.WriteLine('streamed stderr')".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            timeout,
        }
    }

    #[cfg(windows)]
    fn exits_with_descendant_holding_stdio(timeout: Duration) -> CommandSpec {
        CommandSpec {
            program: "cmd".to_string(),
            args: vec![
                "/C".to_string(),
                "start /B powershell -NoLogo -NoProfile -NonInteractive -Command Start-Sleep -Seconds 5 & exit /B 7".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            timeout: Some(timeout),
        }
    }

    #[cfg(unix)]
    fn hung_command(timeout: Duration) -> CommandSpec {
        CommandSpec {
            program: "sh".to_string(),
            args: vec!["-c".to_string(), "sleep 5".to_string()],
            env: Vec::new(),
            cwd: None,
            timeout: Some(timeout),
        }
    }

    #[cfg(unix)]
    fn large_stdout_command(timeout: Duration) -> CommandSpec {
        CommandSpec {
            program: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "dd if=/dev/zero bs=1024 count=1024 2>/dev/null".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            timeout: Some(timeout),
        }
    }

    #[cfg(unix)]
    fn small_output_command(timeout: Option<Duration>) -> CommandSpec {
        CommandSpec {
            program: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "printf 'streamed stdout\n'; printf 'streamed stderr\n' >&2".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            timeout,
        }
    }

    #[cfg(unix)]
    fn exits_with_descendant_holding_stdio(timeout: Duration) -> CommandSpec {
        CommandSpec {
            program: "sh".to_string(),
            args: vec!["-c".to_string(), "sleep 5 & exit 7".to_string()],
            env: Vec::new(),
            cwd: None,
            timeout: Some(timeout),
        }
    }
}

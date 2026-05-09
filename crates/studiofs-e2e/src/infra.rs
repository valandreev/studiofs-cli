//! Docker Compose infrastructure for real E2E runs.

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 120;
const HOST_HEALTH_RETRY_DEADLINE: Duration = Duration::from_secs(60);
const HOST_HEALTH_ATTEMPT_TIMEOUT_SECS: u64 = 2;
const HOST_HEALTH_RETRY_DELAY: Duration = Duration::from_secs(1);
const COMPOSE_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(120);
const TIMED_OUT_COMMAND_EXIT_WAIT: Duration = Duration::from_secs(5);
const DEFAULT_POSTGRES_PORT: u16 = 25_432;
const DEFAULT_RUSTFS_PORT: u16 = 29_000;
const DEFAULT_RUSTFS_CONSOLE_PORT: u16 = 29_001;
const DEFAULT_TOXIPROXY_API_PORT: u16 = 28_474;

/// Constant $name.
pub const POSTGRES_SERVICE_NAME: &str = "postgres";
/// Constant $name.
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
/// Enum $name.
pub enum InfraMode {
    /// Variant $name.
    Disabled,
    /// Variant $name.
    Compose,
    /// Variant $name.
    ExternalEnv,
}

impl InfraMode {
    /// Function $name.
    pub fn cli_value(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Compose => "compose",
            Self::ExternalEnv => "external-env",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct ComposeConfig {
    /// Field $name.
    pub run_id: String,
    /// Field $name.
    pub sandbox: PathBuf,
    /// Field $name.
    pub compose_file: PathBuf,
    /// Field $name.
    pub port_range: Option<String>,
    /// Field $name.
    pub wait_timeout_secs: u64,
}

impl ComposeConfig {
    /// Function $name.
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
/// Struct $name.
pub struct ServicePort {
    /// Field $name.
    pub service: String,
    /// Field $name.
    pub host: String,
    /// Field $name.
    pub host_port: u16,
    /// Field $name.
    pub container_port: u16,
    /// Field $name.
    pub env_var: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CommandSpec {
    /// Field $name.
    pub program: String,
    /// Field $name.
    pub args: Vec<String>,
    /// Field $name.
    pub env: Vec<(String, String)>,
    /// Field $name.
    pub cwd: Option<PathBuf>,
    /// Field $name.
    pub timeout: Option<Duration>,
    /// Field $name.
    pub stdout_path: Option<PathBuf>,
    /// Field $name.
    pub stderr_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CommandOutput {
    /// Field $name.
    pub status: i32,
    /// Field $name.
    pub stdout: String,
    /// Field $name.
    pub stderr: String,
}

/// Trait $name.
pub trait CommandExecutor {
    /// Function $name.
    fn run(&self, command: &CommandSpec) -> std::io::Result<CommandOutput>;

    /// Runs a command while periodically invoking a caller-provided heartbeat.
    fn run_with_heartbeat(
        &self,
        command: &CommandSpec,
        heartbeat_interval: Duration,
        heartbeat: &mut dyn FnMut(),
    ) -> std::io::Result<CommandOutput> {
        let _ = heartbeat_interval;
        let _ = heartbeat;
        self.run(command)
    }
}

#[derive(Debug, Clone, Copy, Default)]
/// Struct $name.
pub struct SystemCommandExecutor;

impl CommandExecutor for SystemCommandExecutor {
    fn run(&self, command: &CommandSpec) -> std::io::Result<CommandOutput> {
        self.run_inner(command, None)
    }

    fn run_with_heartbeat(
        &self,
        command: &CommandSpec,
        heartbeat_interval: Duration,
        heartbeat: &mut dyn FnMut(),
    ) -> std::io::Result<CommandOutput> {
        self.run_inner(
            command,
            Some(CommandHeartbeat::new(heartbeat_interval, heartbeat)),
        )
    }
}

impl SystemCommandExecutor {
    fn run_inner(
        &self,
        command: &CommandSpec,
        mut heartbeat: Option<CommandHeartbeat<'_>>,
    ) -> std::io::Result<CommandOutput> {
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
        let capture_output = command.stdout_path.is_none() && command.stderr_path.is_none();
        if capture_output {
            process.stdout(Stdio::piped());
            process.stderr(Stdio::piped());
        } else {
            if let Some(stdout_path) = &command.stdout_path {
                if let Some(parent) = stdout_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                process.stdout(Stdio::from(fs::File::create(stdout_path)?));
            } else {
                process.stdout(Stdio::null());
            }
            if let Some(stderr_path) = &command.stderr_path {
                if let Some(parent) = stderr_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                process.stderr(Stdio::from(fs::File::create(stderr_path)?));
            } else {
                process.stderr(Stdio::null());
            }
        }
        let mut child = process.spawn()?;
        if !capture_output {
            let status = wait_for_child(&mut child, command.timeout, heartbeat.as_mut())?;
            return Ok(CommandOutput {
                status,
                stdout: String::new(),
                stderr: if status == -1 {
                    command
                        .timeout
                        .map(|timeout| format!("command timed out after {}ms", timeout.as_millis()))
                        .unwrap_or_default()
                } else {
                    String::new()
                },
            });
        }
        let output = if let Some(timeout) = command.timeout {
            let started = Instant::now();
            loop {
                if child.try_wait()?.is_some() {
                    break child.wait_with_output()?;
                }
                if started.elapsed() >= timeout {
                    terminate_child_on_timeout(&mut child);
                    let exited = wait_after_timeout_termination(&mut child)?;
                    if !exited {
                        return Ok(CommandOutput {
                            status: -1,
                            stdout: String::new(),
                            stderr: timed_out_command_message(timeout, false),
                        });
                    }
                    let output = child.wait_with_output()?;
                    return Ok(CommandOutput {
                        status: -1,
                        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                        stderr: timed_out_command_message(timeout, true),
                    });
                }
                if let Some(heartbeat) = &mut heartbeat {
                    heartbeat.maybe_emit();
                }
                thread::sleep(Duration::from_millis(50));
            }
        } else {
            child.wait_with_output()?
        };
        Ok(CommandOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

struct CommandHeartbeat<'a> {
    interval: Duration,
    last_emit: Instant,
    emit: &'a mut dyn FnMut(),
}

impl<'a> CommandHeartbeat<'a> {
    fn new(interval: Duration, emit: &'a mut dyn FnMut()) -> Self {
        Self {
            interval,
            last_emit: Instant::now(),
            emit,
        }
    }

    fn maybe_emit(&mut self) {
        if self.last_emit.elapsed() >= self.interval {
            (self.emit)();
            self.last_emit = Instant::now();
        }
    }
}

fn wait_for_child(
    child: &mut std::process::Child,
    timeout: Option<Duration>,
    mut heartbeat: Option<&mut CommandHeartbeat<'_>>,
) -> std::io::Result<i32> {
    if timeout.is_none() && heartbeat.is_none() {
        return Ok(child.wait()?.code().unwrap_or(-1));
    }

    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status.code().unwrap_or(-1));
        }
        if let Some(timeout) = timeout {
            if started.elapsed() >= timeout {
                terminate_child_on_timeout(child);
                let exited = wait_after_timeout_termination(child)?;
                if !exited {
                    return Ok(-1);
                }
                let _ = child.wait()?;
                return Ok(-1);
            }
        }
        if let Some(heartbeat) = &mut heartbeat {
            heartbeat.maybe_emit();
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn terminate_child_on_timeout(child: &mut Child) {
    terminate_process_tree(child.id());
    let _ = child.kill();
}

#[cfg(windows)]
fn terminate_process_tree(pid: u32) {
    let _ = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[cfg(not(windows))]
fn terminate_process_tree(_pid: u32) {}

fn wait_after_timeout_termination(child: &mut Child) -> std::io::Result<bool> {
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }
        if started.elapsed() >= TIMED_OUT_COMMAND_EXIT_WAIT {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn timed_out_command_message(timeout: Duration, exited: bool) -> String {
    let mut message = format!("command timed out after {}ms", timeout.as_millis());
    if !exited {
        message.push_str("; process tree did not exit after termination request");
    }
    message
}

fn command_spec(
    program: impl Into<String>,
    args: Vec<String>,
    env: Vec<(String, String)>,
    cwd: Option<PathBuf>,
    timeout: Option<Duration>,
) -> CommandSpec {
    CommandSpec {
        program: program.into(),
        args,
        env,
        cwd,
        timeout,
        stdout_path: None,
        stderr_path: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct InfraCheck {
    /// Field $name.
    pub name: String,
    /// Field $name.
    pub passed: bool,
    /// Field $name.
    pub detail: String,
}

#[derive(Debug, Error)]
/// Enum $name.
pub enum InfraError {
    #[error("compose file does not exist: {0}")]
    /// Variant $name.
    MissingComposeFile(PathBuf),
    #[error("invalid port range: {0}")]
    /// Variant $name.
    InvalidPortRange(String),
    #[error("port range {range} has {available} ports, but {needed} are required")]
    /// Variant $name.
    PortRangeTooSmall {
        /// Field $name.
        range: String,
        /// Field $name.
        available: usize,
        /// Field $name.
        needed: usize,
    },
    #[error("{action} failed with exit status {status}: {stderr}")]
    /// Variant $name.
    CommandFailed {
        /// Field $name.
        action: String,
        /// Field $name.
        status: i32,
        /// Field $name.
        stderr: String,
    },
    #[error("infra I/O failed: {0}")]
    /// Variant $name.
    Io(#[from] std::io::Error),
    #[error("infra JSON failed: {0}")]
    /// Variant $name.
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
/// Struct $name.
pub struct DockerComposeStack {
    run_id: String,
    project_name: String,
    sandbox: PathBuf,
    compose_file: PathBuf,
    port_range: Option<String>,
    ports: Vec<ServicePort>,
    wait_timeout_secs: u64,
}

impl DockerComposeStack {
    /// Function $name.
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
            port_range: config.port_range,
            ports,
            wait_timeout_secs: config.wait_timeout_secs,
        })
    }

    /// Function $name.
    pub fn project_name(&self) -> &str {
        &self.project_name
    }

    /// Function $name.
    pub fn ports(&self) -> &[ServicePort] {
        &self.ports
    }

    /// Function $name.
    pub fn advance_port_block(&mut self) -> Result<bool, InfraError> {
        let Some(range) = self.port_range.as_deref() else {
            return Ok(false);
        };
        let Some(current_start) = self.ports.first().map(|port| port.host_port) else {
            return Ok(false);
        };
        let (_, end) = parse_port_range_bounds(range)?;
        let max_start = end.saturating_sub(SERVICE_PORTS.len() as u16 - 1);
        if max_start <= current_start {
            return Ok(false);
        }
        let next_start = max_start;
        self.ports = SERVICE_PORTS
            .iter()
            .enumerate()
            .map(|(offset, spec)| service_port(*spec, next_start + offset as u16))
            .collect();
        Ok(true)
    }

    /// Function $name.
    pub fn prepare_artifacts(&self) -> Result<(), InfraError> {
        fs::create_dir_all(self.docker_artifact_dir())?;
        fs::copy(&self.compose_file, self.artifact_compose_file())?;
        fs::write(
            self.sandbox.join("ports.json"),
            serde_json::to_vec_pretty(&serde_json::json!({ "ports": self.ports }))?,
        )?;
        Ok(())
    }

    /// Function $name.
    pub fn preflight(&self, executor: &dyn CommandExecutor) -> Vec<InfraCheck> {
        let mut checks = Vec::new();
        checks.push(self.command_check(
            "docker version",
            executor,
            command_spec(
                "docker",
                vec![
                    "version".to_string(),
                    "--format".to_string(),
                    "{{.Server.Version}}".to_string(),
                ],
                Vec::new(),
                None,
                None,
            ),
        ));
        checks.push(self.command_check(
            "docker compose version",
            executor,
            command_spec(
                "docker",
                vec!["compose".to_string(), "version".to_string()],
                Vec::new(),
                None,
                None,
            ),
        ));
        checks.extend(self.ports.iter().map(port_check));
        checks
    }

    /// Function $name.
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

    /// Function $name.
    pub fn start(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        run_success(executor, "docker compose up --wait", self.up_command())?;
        Ok(())
    }

    /// Function $name.
    pub fn post_start_preflight(&self, executor: &dyn CommandExecutor) -> Vec<InfraCheck> {
        self.post_start_preflight_with_heartbeat(
            executor,
            Duration::from_secs(0),
            &mut |_port, _attempts, _elapsed| {},
        )
    }

    /// Function $name.
    pub fn post_start_preflight_with_heartbeat(
        &self,
        executor: &dyn CommandExecutor,
        heartbeat_interval: Duration,
        heartbeat: &mut dyn FnMut(&ServicePort, usize, Duration),
    ) -> Vec<InfraCheck> {
        self.ports
            .iter()
            .filter(|port| port.service == RUSTFS_SERVICE_NAME)
            .map(|port| host_health_check(port, executor, heartbeat_interval, heartbeat))
            .collect()
    }

    /// Function $name.
    pub fn stop(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.push("stop".to_string());
        run_success(executor, "docker compose stop", command)?;
        Ok(())
    }

    /// Function $name.
    pub fn restart(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.push("restart".to_string());
        run_success(executor, "docker compose restart", command)?;
        self.start(executor)?;
        Ok(())
    }

    /// Function $name.
    pub fn stop_service(
        &self,
        service: &str,
        executor: &dyn CommandExecutor,
    ) -> Result<(), InfraError> {
        self.service_control().stop_service(service, executor)
    }

    /// Function $name.
    pub fn start_service(
        &self,
        service: &str,
        executor: &dyn CommandExecutor,
    ) -> Result<(), InfraError> {
        self.service_control().start_service(service, executor)
    }

    /// Function $name.
    pub fn collect_logs(&self, executor: &dyn CommandExecutor) -> Result<PathBuf, InfraError> {
        fs::create_dir_all(self.docker_artifact_dir())?;
        let log_path = self.docker_artifact_dir().join("compose.log");
        let stderr_path = self.docker_artifact_dir().join("compose.stderr.log");
        let mut command = self.compose_command();
        command.args.extend([
            "logs".to_string(),
            "--no-color".to_string(),
            "--timestamps".to_string(),
        ]);
        command.timeout = Some(Duration::from_secs(120));
        command.stdout_path = Some(log_path.clone());
        command.stderr_path = Some(stderr_path.clone());
        let output = run_success(executor, "docker compose logs", command)?;
        write_command_output_fallback(&log_path, &output.stdout)?;
        write_command_output_fallback(&stderr_path, &output.stderr)?;
        Ok(log_path)
    }

    /// Function $name.
    pub fn collect_optional_diagnostics(
        &self,
        executor: &dyn CommandExecutor,
    ) -> Result<Vec<String>, InfraError> {
        fs::create_dir_all(self.docker_artifact_dir())?;
        let mut collected = Vec::new();
        if self.collect_optional_command(
            executor,
            "postgres metadata schema dump",
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
            collected.push("postgres metadata schema dump".to_string());
        }
        if self.collect_optional_command(
            executor,
            "postgres metadata data dump",
            &[
                "exec",
                "-T",
                POSTGRES_SERVICE_NAME,
                "pg_dump",
                "--username",
                "studiofs",
                "--dbname",
                "studiofs",
                "--data-only",
                "--inserts",
                "--table",
                "public.volumes",
                "--table",
                "public.event_log",
                "--table",
                "public.metadata_state",
                "--table",
                "public.counters",
                "--table",
                "public.snapshots",
                "--table",
                "public.file_lock_leases",
                "--table",
                "public.acl_rules",
            ],
            "postgres-data.sql",
            "postgres-data.stderr.log",
            "postgres-data.status.json",
        )? {
            collected.push("postgres metadata data dump".to_string());
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

    /// Tears down a previously recorded Docker Compose project.
    pub fn teardown(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.extend([
            "down".to_string(),
            "--volumes".to_string(),
            "--remove-orphans".to_string(),
        ]);
        command.timeout = Some(COMPOSE_TEARDOWN_TIMEOUT);
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
        command
            .args
            .extend(args.iter().map(|arg| (*arg).to_string()));
        command.timeout = Some(Duration::from_secs(120));
        let artifact_dir = self.docker_artifact_dir();
        let stdout_path = artifact_dir.join(stdout_file);
        let stderr_path = artifact_dir.join(stderr_file);
        command.stdout_path = Some(stdout_path.clone());
        command.stderr_path = Some(stderr_path.clone());
        let output = executor.run(&command)?;
        let collected = output.status == 0;
        write_command_output_fallback(&stdout_path, &output.stdout)?;
        write_command_output_fallback(&stderr_path, &output.stderr)?;
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

fn write_command_output_fallback(path: &Path, output: &str) -> std::io::Result<()> {
    if !path.exists() {
        fs::write(path, output)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct ComposeServiceControl {
    /// Field $name.
    pub project_name: String,
    /// Field $name.
    pub compose_file: PathBuf,
    /// Field $name.
    pub wait_timeout_secs: u64,
    env: Vec<(String, String)>,
}

impl ComposeServiceControl {
    /// Function $name.
    pub fn for_project(project_name: String) -> Result<Self, InfraError> {
        Self::for_project_with_environment(project_name, Vec::new())
    }

    /// Function $name.
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

    /// Function $name.
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

    /// Function $name.
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

    /// Tears down the Docker Compose project and removes runner-owned volumes.
    pub fn teardown(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.extend([
            "down".to_string(),
            "--volumes".to_string(),
            "--remove-orphans".to_string(),
        ]);
        command.timeout = Some(COMPOSE_TEARDOWN_TIMEOUT);
        run_success(
            executor,
            "docker compose down --volumes --remove-orphans",
            command,
        )?;
        Ok(())
    }

    fn compose_command(&self) -> CommandSpec {
        command_spec(
            "docker",
            vec![
                "compose".to_string(),
                "--project-name".to_string(),
                self.project_name.clone(),
                "--file".to_string(),
                self.compose_file.display().to_string(),
            ],
            self.env.clone(),
            self.compose_file.parent().map(Path::to_path_buf),
            None,
        )
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

fn host_health_check(
    port: &ServicePort,
    executor: &dyn CommandExecutor,
    heartbeat_interval: Duration,
    heartbeat: &mut dyn FnMut(&ServicePort, usize, Duration),
) -> InfraCheck {
    let url = format!("http://{}:{}/health", port.host, port.host_port);
    let started = Instant::now();
    let mut last_heartbeat = Instant::now();
    let mut attempts = 0usize;
    let mut last_failure: Option<String>;

    loop {
        attempts += 1;
        let command = command_spec(
            "curl",
            vec![
                "--fail".to_string(),
                "--silent".to_string(),
                "--show-error".to_string(),
                "--max-time".to_string(),
                HOST_HEALTH_ATTEMPT_TIMEOUT_SECS.to_string(),
                url.clone(),
            ],
            Vec::new(),
            None,
            Some(Duration::from_secs(HOST_HEALTH_ATTEMPT_TIMEOUT_SECS)),
        );
        match executor.run(&command) {
            Ok(output) if output.status == 0 => {
                return InfraCheck {
                    name: format!("host health {} {}", port.service, port.host_port),
                    passed: true,
                    detail: format!("{url} returned success after {attempts} attempt(s)"),
                };
            }
            Ok(output) => {
                last_failure = Some(command_failure_detail(output.status, &output.stderr));
            }
            Err(err) => {
                last_failure = Some(err.to_string());
            }
        }
        if started.elapsed() >= HOST_HEALTH_RETRY_DEADLINE {
            break;
        }
        if last_heartbeat.elapsed() >= heartbeat_interval {
            heartbeat(port, attempts, started.elapsed());
            last_heartbeat = Instant::now();
        }
        std::thread::sleep(HOST_HEALTH_RETRY_DELAY);
    }

    InfraCheck {
        name: format!("host health {} {}", port.service, port.host_port),
        passed: false,
        detail: format!(
            "{url} did not become reachable from host after {attempts} attempt(s) over {}s after docker compose up --wait passed; last error: {last_failure}",
            started.elapsed().as_secs(),
            last_failure = last_failure.unwrap_or_else(|| "not attempted".to_string())
        ),
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
    let (start, end) = parse_port_range_bounds(range)?;
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

fn parse_port_range_bounds(range: &str) -> Result<(u16, u16), InfraError> {
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
    Ok((start, end))
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
    format!("studiofs-e2e-{normalized}")
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

    #[test]
    fn redirected_command_emits_heartbeats_while_waiting() {
        let temp = tempfile::tempdir().unwrap();
        let (program, args) = slow_command();
        let command = CommandSpec {
            program,
            args,
            env: Vec::new(),
            cwd: None,
            timeout: Some(Duration::from_secs(5)),
            stdout_path: Some(temp.path().join("stdout.log")),
            stderr_path: Some(temp.path().join("stderr.log")),
        };
        let executor = SystemCommandExecutor;
        let mut heartbeat_count = 0;

        let output = executor
            .run_with_heartbeat(&command, Duration::from_millis(20), &mut || {
                heartbeat_count += 1;
            })
            .unwrap();

        assert_eq!(output.status, 0);
        assert!(
            heartbeat_count > 0,
            "redirected command should emit progress while the parent waits"
        );
    }

    #[cfg(windows)]
    fn slow_command() -> (String, Vec<String>) {
        (
            "powershell".to_string(),
            vec![
                "-NoProfile".to_string(),
                "-Command".to_string(),
                "Start-Sleep -Milliseconds 250".to_string(),
            ],
        )
    }

    #[cfg(not(windows))]
    fn slow_command() -> (String, Vec<String>) {
        (
            "sh".to_string(),
            vec!["-c".to_string(), "sleep 1".to_string()],
        )
    }
}

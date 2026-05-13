//! Docker Compose infrastructure for real E2E runs.

use std::fs;
use std::io::{self, Read as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{ChildStderr, ChildStdout, Command, Stdio};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 120;
const DEFAULT_POSTGRES_PORT: u16 = 25_432;
const DEFAULT_RUSTFS_PORT: u16 = 29_000;
const DEFAULT_RUSTFS_CONSOLE_PORT: u16 = 29_001;
const DEFAULT_TOXIPROXY_API_PORT: u16 = 28_474;
const COMPOSE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const COMPOSE_DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(30);

pub const COMPOSE_PROJECT_PREFIX: &str = "studiofs-e2e-";
pub const POSTGRES_SERVICE_NAME: &str = "postgres";
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
pub enum InfraMode {
    Disabled,
    Compose,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeConfig {
    pub run_id: String,
    pub sandbox: PathBuf,
    pub compose_file: PathBuf,
    pub port_range: Option<String>,
    pub wait_timeout_secs: u64,
}

impl ComposeConfig {
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
pub struct ServicePort {
    pub service: String,
    pub host: String,
    pub host_port: u16,
    pub container_port: u16,
    pub env_var: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComposeServiceImage {
    pub service: String,
    pub image: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<PathBuf>,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub status: i32,
    pub stdout: String,
    pub stderr: String,
}

pub trait CommandExecutor {
    fn run(&self, command: &CommandSpec) -> std::io::Result<CommandOutput>;
}

#[derive(Debug, Clone, Copy, Default)]
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
        if let Some(timeout) = command.timeout {
            process.stdout(Stdio::piped());
            process.stderr(Stdio::piped());
            return run_command_with_timeout(process, timeout);
        }
        let output = process.output()?;
        Ok(CommandOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

fn run_command_with_timeout(
    mut command: Command,
    timeout: Duration,
) -> std::io::Result<CommandOutput> {
    let mut child = command.spawn()?;
    let mut stdout_reader = child.stdout.take().map(spawn_stdout_reader);
    let mut stderr_reader = child.stderr.take().map(spawn_stderr_reader);
    let started = Instant::now();
    loop {
        if child.try_wait()?.is_some() {
            let status = child.wait()?;
            let stdout = collect_reader(stdout_reader.take())?;
            let stderr = collect_reader(stderr_reader.take())?;
            return Ok(CommandOutput {
                status: status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&stdout).into_owned(),
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            });
        }
        if started.elapsed() >= timeout {
            force_kill_process_tree(child.id());
            let _ = child.kill();
            let _ = child.wait();
            let _ = collect_reader(stdout_reader.take());
            let _ = collect_reader(stderr_reader.take());
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("command timed out after {} ms", timeout.as_millis()),
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(windows)]
fn force_kill_process_tree(pid: u32) {
    let _ = Command::new("taskkill.exe")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[cfg(not(windows))]
fn force_kill_process_tree(_pid: u32) {}

fn spawn_stdout_reader(mut stdout: ChildStdout) -> JoinHandle<io::Result<Vec<u8>>> {
    thread::spawn(move || {
        let mut buffer = Vec::new();
        stdout.read_to_end(&mut buffer)?;
        Ok(buffer)
    })
}

fn spawn_stderr_reader(mut stderr: ChildStderr) -> JoinHandle<io::Result<Vec<u8>>> {
    thread::spawn(move || {
        let mut buffer = Vec::new();
        stderr.read_to_end(&mut buffer)?;
        Ok(buffer)
    })
}

fn collect_reader(reader: Option<JoinHandle<io::Result<Vec<u8>>>>) -> io::Result<Vec<u8>> {
    let Some(reader) = reader else {
        return Ok(Vec::new());
    };
    reader
        .join()
        .map_err(|_| io::Error::other("command output reader thread panicked"))?
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InfraCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Error)]
pub enum InfraError {
    #[error("compose file does not exist: {0}")]
    MissingComposeFile(PathBuf),
    #[error("invalid port range: {0}")]
    InvalidPortRange(String),
    #[error("port range {range} has {available} ports, but {needed} are required")]
    PortRangeTooSmall {
        range: String,
        available: usize,
        needed: usize,
    },
    #[error("{action} failed with exit status {status}: {stderr}")]
    CommandFailed {
        action: String,
        status: i32,
        stderr: String,
    },
    #[error("infra I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("infra JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("infra YAML failed: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

#[derive(Debug, Clone)]
pub struct DockerComposeStack {
    run_id: String,
    project_name: String,
    sandbox: PathBuf,
    compose_file: PathBuf,
    ports: Vec<ServicePort>,
    wait_timeout_secs: u64,
}

impl DockerComposeStack {
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

    pub fn project_name(&self) -> &str {
        &self.project_name
    }

    pub fn ports(&self) -> &[ServicePort] {
        &self.ports
    }

    pub fn compose_images(&self) -> Result<Vec<ComposeServiceImage>, InfraError> {
        Ok(compose_service_images(&fs::read_to_string(
            &self.compose_file,
        )?)?)
    }

    pub fn prepare_artifacts(&self) -> Result<(), InfraError> {
        fs::create_dir_all(self.docker_artifact_dir())?;
        fs::copy(&self.compose_file, self.artifact_compose_file())?;
        fs::write(
            self.sandbox.join("ports.json"),
            serde_json::to_vec_pretty(&serde_json::json!({ "ports": self.ports }))?,
        )?;
        Ok(())
    }

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

    pub fn start(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        run_success(executor, "docker compose up --wait", self.up_command())?;
        Ok(())
    }

    pub fn stop(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.push("stop".to_string());
        run_success(executor, "docker compose stop", command)?;
        Ok(())
    }

    pub fn restart(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.push("restart".to_string());
        run_success(executor, "docker compose restart", command)?;
        self.start(executor)?;
        Ok(())
    }

    pub fn stop_service(
        &self,
        service: &str,
        executor: &dyn CommandExecutor,
    ) -> Result<(), InfraError> {
        self.service_control().stop_service(service, executor)
    }

    pub fn start_service(
        &self,
        service: &str,
        executor: &dyn CommandExecutor,
    ) -> Result<(), InfraError> {
        self.service_control().start_service(service, executor)
    }

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

fn compose_service_images(compose: &str) -> Result<Vec<ComposeServiceImage>, serde_yaml::Error> {
    let document: serde_yaml::Value = serde_yaml::from_str(compose)?;
    let Some(services) = mapping_value(&document, "services") else {
        return Ok(Vec::new());
    };
    let Some(services) = services.as_mapping() else {
        return Ok(Vec::new());
    };
    let mut images = Vec::new();
    for (name, service) in services {
        let Some(service_name) = name.as_str() else {
            continue;
        };
        let Some(image) = mapping_value(service, "image").and_then(serde_yaml::Value::as_str)
        else {
            continue;
        };
        images.push(ComposeServiceImage {
            service: service_name.to_string(),
            image: image.to_string(),
        });
    }
    images.sort_by(|left, right| left.service.cmp(&right.service));
    Ok(images)
}

fn mapping_value<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    value
        .as_mapping()?
        .get(serde_yaml::Value::String(key.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeServiceControl {
    pub project_name: String,
    pub compose_file: PathBuf,
    pub wait_timeout_secs: u64,
    env: Vec<(String, String)>,
}

impl ComposeServiceControl {
    pub fn for_project(project_name: String) -> Result<Self, InfraError> {
        Self::for_project_with_environment(project_name, Vec::new())
    }

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
    use std::time::{Duration, Instant};

    #[test]
    fn system_command_executor_returns_timed_out_for_hung_command() {
        let command = hung_command(Duration::from_millis(50));
        let started = Instant::now();

        let err = SystemCommandExecutor.run(&command).unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout-aware executor should not wait for the hung command to finish"
        );
    }

    #[test]
    fn timeout_executor_drains_large_stdout_while_waiting() {
        let command = large_stdout_command(Duration::from_secs(5));

        let output = SystemCommandExecutor
            .run(&command)
            .expect("large-output command should finish before timeout");

        assert_eq!(output.status, 0);
        assert!(output.stdout.len() >= 1_000_000);
    }

    #[cfg(windows)]
    fn hung_command(timeout: Duration) -> CommandSpec {
        CommandSpec {
            program: "powershell".to_string(),
            args: vec![
                "-NoProfile".to_string(),
                "-Command".to_string(),
                "Start-Sleep -Seconds 5".to_string(),
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
                "[Console]::Out.Write(('x' * 1048576))".to_string(),
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
}

//! Docker Compose infrastructure for real E2E runs.

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_WAIT_TIMEOUT_SECS: u64 = 120;
const DEFAULT_POSTGRES_PORT: u16 = 25_432;
const DEFAULT_RUSTFS_PORT: u16 = 29_000;
const DEFAULT_RUSTFS_CONSOLE_PORT: u16 = 29_001;
const DEFAULT_TOXIPROXY_API_PORT: u16 = 28_474;

const SERVICE_PORTS: &[ServicePortSpec] = &[
    ServicePortSpec {
        service: "postgres",
        default_host_port: DEFAULT_POSTGRES_PORT,
        container_port: 5432,
        env_var: "STUDIOFS_E2E_POSTGRES_PORT",
    },
    ServicePortSpec {
        service: "rustfs",
        default_host_port: DEFAULT_RUSTFS_PORT,
        container_port: 9000,
        env_var: "STUDIOFS_E2E_RUSTFS_PORT",
    },
    ServicePortSpec {
        service: "rustfs-console",
        default_host_port: DEFAULT_RUSTFS_CONSOLE_PORT,
        container_port: 9001,
        env_var: "STUDIOFS_E2E_RUSTFS_CONSOLE_PORT",
    },
    ServicePortSpec {
        service: "toxiproxy-api",
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<PathBuf>,
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
        process.envs(command.env.iter().map(|(key, value)| (key, value)));
        if let Some(cwd) = &command.cwd {
            process.current_dir(cwd);
        }
        let output = process.output()?;
        Ok(CommandOutput {
            status: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
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
        let ports = allocate_ports(config.port_range.as_deref())?;
        Ok(Self {
            project_name: compose_project_name(&config.run_id),
            run_id: config.run_id,
            sandbox: config.sandbox,
            compose_file: config.compose_file,
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
            },
        ));
        checks.extend(self.ports.iter().map(port_check));
        checks
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
        self.run_success(executor, "docker compose up --wait", self.up_command())?;
        Ok(())
    }

    pub fn stop(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.push("stop".to_string());
        self.run_success(executor, "docker compose stop", command)?;
        Ok(())
    }

    pub fn restart(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.push("restart".to_string());
        self.run_success(executor, "docker compose restart", command)?;
        self.start(executor)?;
        Ok(())
    }

    pub fn collect_logs(&self, executor: &dyn CommandExecutor) -> Result<PathBuf, InfraError> {
        fs::create_dir_all(self.docker_artifact_dir())?;
        let mut command = self.compose_command();
        command.args.extend([
            "logs".to_string(),
            "--no-color".to_string(),
            "--timestamps".to_string(),
        ]);
        let output = self.run_success(executor, "docker compose logs", command)?;
        let log_path = self.docker_artifact_dir().join("compose.log");
        fs::write(&log_path, output.stdout)?;
        fs::write(
            self.docker_artifact_dir().join("compose.stderr.log"),
            output.stderr,
        )?;
        Ok(log_path)
    }

    pub fn teardown(&self, executor: &dyn CommandExecutor) -> Result<(), InfraError> {
        let mut command = self.compose_command();
        command.args.extend([
            "down".to_string(),
            "--volumes".to_string(),
            "--remove-orphans".to_string(),
        ]);
        self.run_success(
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
            env: self.compose_environment(),
            cwd: default_repo_root(),
        }
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

    fn run_success(
        &self,
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

    fn docker_artifact_dir(&self) -> PathBuf {
        self.sandbox.join("artifacts/docker")
    }

    fn artifact_compose_file(&self) -> PathBuf {
        self.docker_artifact_dir().join("compose.e2e.yml")
    }
}

fn port_check(port: &ServicePort) -> InfraCheck {
    match TcpListener::bind(("127.0.0.1", port.host_port)) {
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
        .parse::<u16>()
        .map_err(|_| InfraError::InvalidPortRange(range.to_string()))?;
    let end = end
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
    format!("studiofs-e2e-{normalized}")
}

fn default_compose_file() -> PathBuf {
    default_repo_root()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("docker/compose.e2e.yml")
}

fn default_repo_root() -> Option<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
}

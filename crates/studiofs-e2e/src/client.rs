//! Multi-client process controller for mounted E2E scenarios.

use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientControllerConfig {
    pub studiofs_bin: PathBuf,
    pub sandbox: PathBuf,
    pub volume_name: String,
    pub meta_dsn: String,
    pub s3: S3Config,
    pub mount_ready_timeout: Duration,
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

pub trait ClientProcess {
    fn id(&self) -> u32;
    fn try_wait(&mut self) -> io::Result<Option<i32>>;
    fn kill(&mut self) -> io::Result<()>;
}

pub trait ClientProcessExecutor {
    fn run(&self, command: &ClientCommand) -> io::Result<ClientCommandOutput>;
    fn spawn(&self, command: &ClientCommand) -> io::Result<Box<dyn ClientProcess>>;
    fn wait_until_mount_ready(&self, command: &ClientCommand, timeout: Duration) -> io::Result<()>;
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

    fn wait_until_mount_ready(&self, command: &ClientCommand, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if command.mount_point.exists() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "mount point for {} was not ready: {}",
                        command.client,
                        command.mount_point.display()
                    ),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
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
    #[error("unknown E2E client: {0}")]
    UnknownClient(String),
    #[error("client command {action} for {client} failed with exit status {status}: {stderr}")]
    CommandFailed {
        client: String,
        action: String,
        status: i32,
        stderr: String,
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
        self.clients.clear();
        for name in names {
            let spec = self.create_client_spec(name.as_ref())?;
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
        if let Some(exit_code) = process.try_wait()? {
            self.clients[index].spec.state = ClientLifecycleState::Exited;
            return Err(ClientControllerError::CommandFailed {
                client: command.client,
                action: command.action,
                status: exit_code,
                stderr: format!("mount process exited before readiness with status {exit_code}"),
            });
        }
        self.executor
            .wait_until_mount_ready(&command, self.config.mount_ready_timeout)?;
        self.clients[index].spec.process_id = Some(process_id);
        self.clients[index].spec.state = ClientLifecycleState::Mounted;
        self.clients[index].process = Some(process);
        Ok(())
    }

    fn unmount_index(&mut self, index: usize) -> Result<(), ClientControllerError> {
        let command = self.unmount_command(index)?;
        let output = self.executor.run(&command)?;
        ensure_success(&command, output)?;
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

    fn create_client_spec(&self, name: &str) -> Result<ClientSpec, ClientControllerError> {
        let client_dir = self
            .config
            .sandbox
            .join("artifacts")
            .join("clients")
            .join(name);
        let client_root = client_dir.join("client-root");
        let local_metadata_dir = client_root.join("meta").join(&self.config.volume_name);
        let local_metadata_db = local_metadata_dir.join(format!("{}.db", self.config.volume_name));
        let cache_dir = client_dir.join("cache");
        let mount_point = client_dir.join("mount");
        fs::create_dir_all(&client_root)?;
        fs::create_dir_all(&local_metadata_dir)?;
        fs::create_dir_all(&cache_dir)?;
        fs::create_dir_all(&mount_point)?;
        Ok(ClientSpec {
            name: name.to_string(),
            client_root,
            local_metadata_dir,
            local_metadata_db,
            cache_dir,
            mount_point,
            stdout_log: client_dir.join("studiofs.stdout.log"),
            stderr_log: client_dir.join("studiofs.stderr.log"),
            tracing_log: client_dir.join("studiofs-tracing.log"),
            process_id: None,
            state: ClientLifecycleState::Planned,
        })
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
        self.clients
            .get(index)
            .map(|runtime| &runtime.spec)
            .ok_or(ClientControllerError::NoClients)
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
            stderr: output.stderr,
        })
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

fn append_log(path: &PathBuf, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

//! Unit tests for scenario workload modules.

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use super::*;
use crate::client::{ClientLifecycleState, ClientSpec, S3Config, client_spec_for};

fn test_isolation(root: &str) -> ScenarioIsolation {
    ScenarioIsolation {
        root: root.to_string(),
        payload_marker: format!("test-marker-{root}"),
    }
}

fn read_helper_progress(scenario_dir: &Path) -> String {
    let progress_dir = scenario_dir.join("hotspot-helper-progress");
    if !progress_dir.exists() {
        return String::new();
    }
    let mut progress = String::new();
    for entry in std::fs::read_dir(progress_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            progress.push_str(&std::fs::read_to_string(entry.path()).unwrap());
        }
    }
    progress
}

fn replace_dir_contents(source: &Path, destination: &Path) -> Result<(), ScenarioError> {
    if destination.exists() {
        for entry in std::fs::read_dir(destination)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                std::fs::remove_dir_all(path)?;
            } else {
                std::fs::remove_file(path)?;
            }
        }
    }
    copy_dir_contents(source, destination)
}

fn copy_dir_contents(source: &Path, destination: &Path) -> Result<(), ScenarioError> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_contents(&source_path, &destination_path)?;
        } else {
            std::fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

fn _test_config(root: &Path) -> ClientControllerConfig {
    ClientControllerConfig {
        studiofs_bin: PathBuf::from("studiofs-test-bin"),
        sandbox: root.join("run-1"),
        volume_name: "projectfs".to_string(),
        meta_dsn: "postgres://studiofs:studiofs@127.0.0.1:25432".to_string(),
        s3: S3Config {
            endpoint: "http://127.0.0.1:29000".to_string(),
            bucket: "studiofs-e2e".to_string(),
            access_key: "studiofs".to_string(),
            secret_key: "studiofs123".to_string(),
            region: "us-east-1".to_string(),
        },
        mount_ready_timeout: Duration::from_secs(1),
        mount_exit_timeout: Duration::from_secs(1),
        poll_interval: Duration::from_millis(10),
        mount_points: Vec::new(),
        client_env_overrides: std::collections::BTreeMap::new(),
        mount_arg_overrides: std::collections::BTreeMap::new(),
    }
}

mod advanced_mounted;
mod chaos_barriers;
mod crash_restart;
mod namespace_collision;
mod outage;

struct RecordingSmokeDriver {
    clients: Vec<ClientSpec>,
    mounted: Cell<bool>,
    unmounted: Cell<bool>,
    unmount_error: Option<&'static str>,
}

impl RecordingSmokeDriver {
    fn new(clients: Vec<ClientSpec>) -> Self {
        Self {
            clients,
            mounted: Cell::new(false),
            unmounted: Cell::new(false),
            unmount_error: None,
        }
    }

    fn with_unmount_error(mut self, error: &'static str) -> Self {
        self.unmount_error = Some(error);
        self
    }
}

impl MountedSmokeDriver for RecordingSmokeDriver {
    fn bootstrap_peer_volumes(
        &mut self,
        options: PeerVolumeBootstrapOptions,
    ) -> Result<PeerVolumeBootstrapSummary, ClientControllerError> {
        assert_eq!(
            options.client_names,
            SMOKE_CLIENT_NAMES
                .iter()
                .copied()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        let volumes = self
            .clients
            .iter()
            .map(|owner| {
                let volume_name = format!("projectfs-{}", owner.name);
                VolumeBootstrapSummary {
                    volume_name: volume_name.clone(),
                    init_client: owner.name.clone(),
                    discovered_clients: self
                        .clients
                        .iter()
                        .filter(|client| client.name != owner.name)
                        .map(|client| client.name.clone())
                        .collect(),
                    clients: self
                        .clients
                        .iter()
                        .map(|client| crate::client::ClientBootstrapRecord {
                            name: client.name.clone(),
                            local_metadata_db: client
                                .client_root
                                .join("meta")
                                .join(&volume_name)
                                .join(format!("{volume_name}.db")),
                            state: if client.name == owner.name {
                                ClientLifecycleState::Initialized
                            } else {
                                ClientLifecycleState::Discovered
                            },
                        })
                        .collect(),
                }
            })
            .collect();
        Ok(PeerVolumeBootstrapSummary {
            base_volume_name: "projectfs".to_string(),
            volumes,
        })
    }

    fn select_volume_for_clients(
        &mut self,
        volume_name: &str,
    ) -> Result<(), ClientControllerError> {
        for client in &mut self.clients {
            client.volume_name = volume_name.to_string();
            client.local_metadata_dir = client.client_root.join("meta").join(volume_name);
            client.local_metadata_db = client.local_metadata_dir.join(format!("{volume_name}.db"));
        }
        Ok(())
    }

    fn mount_all(&mut self) -> Result<(), ClientControllerError> {
        self.mounted.set(true);
        Ok(())
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.clients.clone()
    }

    fn after_smoke_file_written(
        &mut self,
        _volume_name: &str,
        smoke_file: &Path,
    ) -> Result<(), ClientControllerError> {
        let file_name = smoke_file.file_name().ok_or_else(|| {
            ClientControllerError::Io(io::Error::other("missing smoke file name"))
        })?;
        let payload = fs::read(smoke_file)?;
        for client in &self.clients {
            fs::write(client.mount_point.join(file_name), &payload)?;
        }
        Ok(())
    }

    fn unmount_all(&mut self) -> Result<(), ClientControllerError> {
        self.unmounted.set(true);
        if let Some(error) = self.unmount_error {
            return Err(ClientControllerError::Io(std::io::Error::other(error)));
        }
        Ok(())
    }
}

struct RecordingOutageDriver {
    clients: Vec<ClientSpec>,
    actions: RefCell<Vec<&'static str>>,
    publish_writer_on_remount: bool,
    lose_writer_payload_on_remount: bool,
    keep_lost_writer_payload_local_on_remount: bool,
    corrupt_writer_payload_on_remount: bool,
    start_postgres_delay: Duration,
}

impl RecordingOutageDriver {
    fn new(clients: Vec<ClientSpec>) -> Self {
        Self {
            clients,
            actions: RefCell::new(Vec::new()),
            publish_writer_on_remount: false,
            lose_writer_payload_on_remount: false,
            keep_lost_writer_payload_local_on_remount: false,
            corrupt_writer_payload_on_remount: false,
            start_postgres_delay: Duration::from_millis(0),
        }
    }

    fn publish_writer_on_remount(mut self) -> Self {
        self.publish_writer_on_remount = true;
        self
    }

    fn lose_writer_payload_on_remount(mut self) -> Self {
        self.publish_writer_on_remount = true;
        self.lose_writer_payload_on_remount = true;
        self
    }

    fn keep_lost_writer_payload_local_on_remount(mut self) -> Self {
        self.keep_lost_writer_payload_local_on_remount = true;
        self
    }

    fn corrupt_writer_payload_on_remount(mut self) -> Self {
        self.corrupt_writer_payload_on_remount = true;
        self
    }

    fn with_start_postgres_delay(mut self, delay: Duration) -> Self {
        self.start_postgres_delay = delay;
        self
    }
}

impl PostgresOutageDriver for RecordingOutageDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        self.actions.borrow_mut().push("bootstrap");
        assert_eq!(
            options.client_names,
            POSTGRES_OUTAGE_CLIENT_NAMES
                .iter()
                .copied()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        Ok(VolumeBootstrapSummary {
            volume_name: "projectfs".to_string(),
            init_client: POSTGRES_OUTAGE_CLIENT_NAMES[0].to_string(),
            discovered_clients: POSTGRES_OUTAGE_CLIENT_NAMES[1..]
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            clients: self
                .clients
                .iter()
                .map(|client| crate::client::ClientBootstrapRecord {
                    name: client.name.clone(),
                    local_metadata_db: client.local_metadata_db.clone(),
                    state: ClientLifecycleState::Discovered,
                })
                .collect(),
        })
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("mount_all");
        Ok(())
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.clients.clone()
    }

    fn after_initial_write(
        &mut self,
        clients: &[ClientSpec],
        relative_path: &str,
    ) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("after_initial_write");
        if let Some(first) = clients.first() {
            let payload = std::fs::read(first.mount_point.join(relative_path))?;
            for client in clients.iter().skip(1) {
                std::fs::write(client.mount_point.join(relative_path), &payload)?;
            }
        }
        Ok(())
    }

    fn stop_postgres(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("stop_postgres");
        Ok(())
    }

    fn start_postgres(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("start_postgres");
        thread::sleep(self.start_postgres_delay);
        Ok(())
    }

    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError> {
        assert_eq!(client, POSTGRES_OUTAGE_CLIENT_NAMES[0]);
        self.actions.borrow_mut().push("force_kill_writer");
        Ok(())
    }

    fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
        assert_eq!(client, POSTGRES_OUTAGE_CLIENT_NAMES[0]);
        self.actions.borrow_mut().push("remount_writer");
        if !self.publish_writer_on_remount {
            return Ok(());
        }
        let writer = self
            .clients
            .iter()
            .find(|candidate| candidate.name == client)
            .expect("recording writer client exists");
        let writer_path = writer.mount_point.join(POSTGRES_OUTAGE_FILE_NAME);
        if self.lose_writer_payload_on_remount && self.keep_lost_writer_payload_local_on_remount {
            std::fs::write(&writer_path, [])?;
        } else if self.lose_writer_payload_on_remount {
            match std::fs::remove_file(&writer_path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        if self.corrupt_writer_payload_on_remount {
            std::fs::write(
                writer.mount_point.join(POSTGRES_OUTAGE_FILE_NAME),
                vec![b'x'; POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len()],
            )?;
        }
        if self.keep_lost_writer_payload_local_on_remount && self.lose_writer_payload_on_remount {
            return Ok(());
        }
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(&writer.mount_point)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                entries.push((entry.file_name(), std::fs::read(entry.path())?));
            }
        }
        for peer in self.clients.iter().filter(|peer| peer.name != client) {
            for (name, payload) in &entries {
                std::fs::write(peer.mount_point.join(name), payload)?;
            }
        }
        Ok(())
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("unmount_all");
        Ok(())
    }
}

struct RecordingRustfsOutageDriver {
    clients: Vec<ClientSpec>,
    actions: RefCell<Vec<&'static str>>,
    peer_probe_delay: Duration,
}

impl RecordingRustfsOutageDriver {
    fn new(clients: Vec<ClientSpec>) -> Self {
        Self {
            clients,
            actions: RefCell::new(Vec::new()),
            peer_probe_delay: Duration::from_millis(0),
        }
    }

    fn with_peer_probe_delay(mut self, delay: Duration) -> Self {
        self.peer_probe_delay = delay;
        self
    }
}

impl RustfsOutageDriver for RecordingRustfsOutageDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        self.actions.borrow_mut().push("bootstrap");
        assert_eq!(
            options.client_names,
            RUSTFS_OUTAGE_CLIENT_NAMES
                .iter()
                .copied()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        Ok(VolumeBootstrapSummary {
            volume_name: "projectfs".to_string(),
            init_client: RUSTFS_OUTAGE_CLIENT_NAMES[0].to_string(),
            discovered_clients: RUSTFS_OUTAGE_CLIENT_NAMES[1..]
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            clients: self
                .clients
                .iter()
                .map(|client| crate::client::ClientBootstrapRecord {
                    name: client.name.clone(),
                    local_metadata_db: client.local_metadata_db.clone(),
                    state: ClientLifecycleState::Discovered,
                })
                .collect(),
        })
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("mount_all");
        Ok(())
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.clients.clone()
    }

    fn after_initial_write(
        &mut self,
        clients: &[ClientSpec],
        relative_path: &str,
    ) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("after_initial_write");
        let payload = std::fs::read(clients[0].mount_point.join(relative_path))?;
        for client in clients.iter().skip(1) {
            std::fs::write(client.mount_point.join(relative_path), &payload)?;
        }
        Ok(())
    }

    fn stop_rustfs(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("stop_rustfs");
        Ok(())
    }

    fn assert_peer_not_visible(
        &mut self,
        peer: &ClientSpec,
        relative_path: &str,
        forbidden_checksum: &str,
    ) -> Result<Option<String>, ScenarioError> {
        self.actions.borrow_mut().push("assert_peer_not_visible");
        if !self.peer_probe_delay.is_zero() {
            thread::sleep(self.peer_probe_delay);
        }
        assert_peer_not_visible(peer, relative_path, forbidden_checksum)
    }

    fn start_rustfs(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("start_rustfs");
        let source = self.clients[0].mount_point.join(RUSTFS_OUTAGE_FILE_NAME);
        let payload = std::fs::read(source)?;
        for client in self.clients.iter().skip(1) {
            std::fs::write(client.mount_point.join(RUSTFS_OUTAGE_FILE_NAME), &payload)?;
        }
        Ok(())
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("unmount_all");
        Ok(())
    }
}

struct RecordingCrashRestartDriver {
    clients: Vec<ClientSpec>,
    actions: RefCell<Vec<String>>,
    corrupt_local_on_remount: bool,
    corrupt_converged_payload_after_remount: bool,
    remove_local_on_remount: bool,
    publish_current_file_on_remount: bool,
    publish_current_file_before_convergence: bool,
}

impl RecordingCrashRestartDriver {
    fn new(clients: Vec<ClientSpec>) -> Self {
        Self {
            clients,
            actions: RefCell::new(Vec::new()),
            corrupt_local_on_remount: false,
            corrupt_converged_payload_after_remount: false,
            remove_local_on_remount: false,
            publish_current_file_on_remount: false,
            publish_current_file_before_convergence: false,
        }
    }

    fn corrupt_local_on_remount(mut self) -> Self {
        self.corrupt_local_on_remount = true;
        self
    }

    fn corrupt_converged_payload_after_remount(mut self) -> Self {
        self.corrupt_converged_payload_after_remount = true;
        self
    }

    fn remove_local_on_remount(mut self) -> Self {
        self.remove_local_on_remount = true;
        self
    }

    fn publish_current_file_on_remount(mut self) -> Self {
        self.publish_current_file_on_remount = true;
        self
    }

    fn publish_current_file_before_convergence(mut self) -> Self {
        self.publish_current_file_before_convergence = true;
        self
    }
}

impl CrashRestartDriver for RecordingCrashRestartDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        self.actions.borrow_mut().push("bootstrap".to_string());
        assert_eq!(
            options.client_names,
            CRASH_RESTART_CLIENT_NAMES
                .iter()
                .copied()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        Ok(VolumeBootstrapSummary {
            volume_name: "projectfs".to_string(),
            init_client: CRASH_RESTART_CLIENT_NAMES[0].to_string(),
            discovered_clients: CRASH_RESTART_CLIENT_NAMES[1..]
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            clients: self
                .clients
                .iter()
                .map(|client| crate::client::ClientBootstrapRecord {
                    name: client.name.clone(),
                    local_metadata_db: client.local_metadata_db.clone(),
                    state: ClientLifecycleState::Discovered,
                })
                .collect(),
        })
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("mount_all".to_string());
        Ok(())
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.clients.clone()
    }

    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError> {
        self.actions
            .borrow_mut()
            .push(format!("force_kill_{client}"));
        Ok(())
    }

    fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push(format!("remount_{client}"));
        if self.corrupt_local_on_remount {
            let client = self
                .clients
                .iter()
                .find(|candidate| candidate.name == client)
                .expect("recording driver client exists");
            std::fs::write(
                client
                    .mount_point
                    .join(CRASH_RESTART_LOCAL_DURABLE_FILE_NAME),
                b"corrupted payload after restart\n",
            )?;
        }
        if self.remove_local_on_remount {
            let client = self
                .clients
                .iter()
                .find(|candidate| candidate.name == client)
                .expect("recording driver client exists");
            for relative_path in [
                CRASH_RESTART_LOCAL_DURABLE_FILE_NAME,
                CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
            ] {
                let path = client.mount_point.join(relative_path);
                if let Err(error) = std::fs::remove_file(&path) {
                    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
                }
            }
        }
        if self.publish_current_file_on_remount {
            let crashed = self
                .clients
                .iter()
                .find(|candidate| candidate.name == client)
                .expect("recording driver client exists");
            for relative_path in [
                CRASH_RESTART_LOCAL_DURABLE_FILE_NAME,
                CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
            ] {
                let source = crashed.mount_point.join(relative_path);
                if let Ok(payload) = std::fs::read(source) {
                    for peer in self.clients.iter().filter(|peer| peer.name != client) {
                        std::fs::write(peer.mount_point.join(relative_path), &payload)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn before_peer_convergence_wait(
        &mut self,
        client: &str,
        relative_path: &str,
    ) -> Result<(), ScenarioError> {
        if self.corrupt_converged_payload_after_remount {
            for client in &self.clients {
                std::fs::write(
                    client.mount_point.join(relative_path),
                    b"wrong converged payload after restart\n",
                )?;
            }
        }
        if self.publish_current_file_before_convergence {
            let crashed = self
                .clients
                .iter()
                .find(|candidate| candidate.name == client)
                .expect("recording driver client exists");
            let source = crashed.mount_point.join(relative_path);
            let payload = std::fs::read(source)?;
            for peer in self.clients.iter().filter(|peer| peer.name != client) {
                std::fs::write(peer.mount_point.join(relative_path), &payload)?;
            }
        }
        Ok(())
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("unmount_all".to_string());
        Ok(())
    }
}

struct RecordingNamespaceCollisionDriver {
    clients: Vec<ClientSpec>,
    actions: RefCell<Vec<&'static str>>,
    unmount_error: Option<&'static str>,
    emergency_error: Option<&'static str>,
}

impl RecordingNamespaceCollisionDriver {
    fn new(clients: Vec<ClientSpec>) -> Self {
        Self {
            clients,
            actions: RefCell::new(Vec::new()),
            unmount_error: None,
            emergency_error: None,
        }
    }

    fn with_unmount_error(mut self, error: &'static str) -> Self {
        self.unmount_error = Some(error);
        self
    }

    fn with_emergency_error(mut self, error: &'static str) -> Self {
        self.emergency_error = Some(error);
        self
    }

    fn replicate_from_first_client(&self) -> Result<(), ScenarioError> {
        let source = &self.clients[0].mount_point;
        let mut errors = Vec::new();
        for client in self.clients.iter().skip(1) {
            if let Err(error) = replace_dir_contents(source, &client.mount_point) {
                errors.push(format!("{}: {error}", client.name));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ScenarioError::ReplicationFailure { errors })
        }
    }
}

impl NamespaceCollisionDriver for RecordingNamespaceCollisionDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        self.actions.borrow_mut().push("bootstrap");
        assert_eq!(
            options.client_names,
            COLLISION_CLIENT_NAMES
                .iter()
                .copied()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        Ok(VolumeBootstrapSummary {
            volume_name: "projectfs".to_string(),
            init_client: COLLISION_CLIENT_NAMES[0].to_string(),
            discovered_clients: COLLISION_CLIENT_NAMES[1..]
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            clients: self
                .clients
                .iter()
                .map(|client| crate::client::ClientBootstrapRecord {
                    name: client.name.clone(),
                    local_metadata_db: client.local_metadata_db.clone(),
                    state: ClientLifecycleState::Discovered,
                })
                .collect(),
        })
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("mount_all");
        Ok(())
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.clients.clone()
    }

    fn after_conflicting_operations(
        &mut self,
        _kind: NamespaceCollisionKind,
        _clients: &[ClientSpec],
    ) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("replicate");
        self.replicate_from_first_client()
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("unmount_all");
        if let Some(error) = self.unmount_error {
            return Err(ScenarioError::Io(std::io::Error::other(error)));
        }
        Ok(())
    }

    fn emergency_kill_all_no_wait(&mut self, _context: &str) -> Result<(), ScenarioError> {
        self.actions.borrow_mut().push("emergency_kill_all_no_wait");
        if let Some(error) = self.emergency_error {
            return Err(ScenarioError::Io(std::io::Error::other(error)));
        }
        Ok(())
    }
}

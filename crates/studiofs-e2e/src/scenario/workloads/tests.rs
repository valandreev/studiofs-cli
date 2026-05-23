//! Unit tests for scenario workload modules.

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use tempfile::tempdir;

use super::*;
use crate::client::{ClientLifecycleState, ClientSpec, S3Config, client_spec_for};
use crate::scenario::{available_scenarios, resolve_scenario};

fn test_isolation(root: &str) -> ScenarioIsolation {
    ScenarioIsolation {
        root: root.to_string(),
        payload_marker: format!("test-marker-{root}"),
    }
}

#[test]
fn advanced_mounted_isolation_uses_volume_specific_roots_and_payloads() {
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox: PathBuf::from("sandbox"),
        scenario_dir: PathBuf::from("scenario"),
        client_controller_config: None,
    };

    let first = ScenarioIsolation::for_advanced(
        &context,
        AdvancedMountedKind::HotspotChurn,
        "projectfs-volume-a",
    );
    let second = ScenarioIsolation::for_advanced(
        &context,
        AdvancedMountedKind::HotspotChurn,
        "projectfs-volume-b",
    );

    assert_ne!(first.root, "hotspot");
    assert_ne!(first.root, second.root);
    assert!(first.path("slot-0.txt").starts_with("hotspot-"));
    assert_ne!(
        first.payload("overwrite", "slot-0.txt", "body"),
        second.payload("overwrite", "slot-0.txt", "body"),
        "payloads must vary with the mounted volume identity"
    );
}

#[test]
fn isolated_sparse_ranges_fit_final_logical_size() {
    let context = ScenarioContext {
        run_id: "focused-13-17-20260515".to_string(),
        seed: 377,
        sandbox: PathBuf::from("sandbox"),
        scenario_dir: PathBuf::from("scenario"),
        client_controller_config: None,
    };
    let isolation = ScenarioIsolation::for_advanced(
        &context,
        AdvancedMountedKind::SparseRange,
        "projectfs-volume-sparse-range-contract",
    );
    let sparse_path = isolation.path(
        SPARSE_FILE_PATH
            .strip_prefix("sparse/")
            .unwrap_or(SPARSE_FILE_PATH),
    );
    let ranges = sparse_expected_range_specs(Some((&isolation, &sparse_path)));
    let final_logical_size = sparse_final_logical_size(&ranges);

    assert!(
        final_logical_size > SPARSE_FINAL_LOGICAL_SIZE,
        "realistic isolated payloads should extend beyond the legacy fixed EOF"
    );

    for range in ranges {
        let end = range.offset + range.payload.len() as u64;
        assert!(
            end <= final_logical_size,
            "{} range ends at {end}, beyond sparse logical size {final_logical_size}",
            range.label
        );
    }
}

#[test]
fn isolated_sparse_ranges_fit_each_phase_logical_size() {
    let context = ScenarioContext {
        run_id: "repro-s17-sparse-after-contract-fix".to_string(),
        seed: 0,
        sandbox: PathBuf::from("sandbox"),
        scenario_dir: PathBuf::from("scenario"),
        client_controller_config: None,
    };
    let isolation = ScenarioIsolation::for_advanced(
        &context,
        AdvancedMountedKind::SparseRange,
        "sfs-e2e-repro-s17-sparse-after-contract-fix-0-28afb97ba53ab496",
    );
    let sparse_path = isolation.path(
        SPARSE_FILE_PATH
            .strip_prefix("sparse/")
            .unwrap_or(SPARSE_FILE_PATH),
    );
    let ranges = sparse_expected_range_specs(Some((&isolation, &sparse_path)));

    for (phase, logical_size, expected_ranges) in [
        (
            "initial sparse ranges",
            sparse_logical_size(SPARSE_INITIAL_LOGICAL_SIZE, &ranges[..2]),
            &ranges[..2],
        ),
        (
            "shrunk sparse ranges",
            sparse_logical_size(SPARSE_SHRUNK_LOGICAL_SIZE, &ranges[..2]),
            &ranges[..2],
        ),
        (
            "final sparse ranges",
            sparse_final_logical_size(&ranges),
            &ranges[..],
        ),
    ] {
        for range in expected_ranges {
            let end = range.offset + range.payload.len() as u64;
            assert!(
                end <= logical_size,
                "{phase}: {} range ends at {end}, beyond sparse logical size {logical_size}",
                range.label
            );
        }
    }
}

#[test]
fn sparse_wait_timeout_writes_diagnostics_artifacts() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let clients = ["client-a", "client-b"]
        .into_iter()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-sparse-diagnostics".to_string(),
        seed: 377,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let expected_ranges = [SparseRangeSpec {
        label: "prefix",
        offset: 0,
        payload: b"expected".to_vec(),
    }];
    let hole_specs: [SparseHoleSpec; 0] = [];

    let error = wait_for_sparse_expected_until(SparseExpectedWait {
        context: &context,
        clients: &clients,
        sparse_path: "sparse/shared.bin",
        logical_size: 64,
        expected_ranges: &expected_ranges,
        hole_specs: &hole_specs,
        deadline: Instant::now(),
        poll_interval: Duration::from_millis(1),
        label: "initial sparse ranges",
    })
    .unwrap_err();

    assert!(
            error.to_string().contains(
                "diagnostics: artifacts/scenarios/mounted-sparse-file-range-convergence/sparse-initial-sparse-ranges-timeout-diagnostics.json"
            ),
            "{error}"
        );
    let diagnostics_path = context
        .scenario_dir
        .join("sparse-initial-sparse-ranges-timeout-diagnostics.json");
    let diagnostics: serde_json::Value =
        serde_json::from_slice(&std::fs::read(diagnostics_path).unwrap()).unwrap();
    assert_eq!(diagnostics["schema_version"], 1);
    assert_eq!(diagnostics["run_id"], "run-sparse-diagnostics");
    assert_eq!(diagnostics["label"], "initial sparse ranges");
    assert_eq!(diagnostics["clients"].as_array().unwrap().len(), 2);
    assert_eq!(diagnostics["clients"][0]["parent_visible"], false);
    assert_eq!(
        diagnostics["clients"][0]["metadata"]["error"]["kind"],
        "NotFound"
    );

    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("sparse_timeout_diagnostics"));
    assert!(operations_log.contains("sparse-initial-sparse-ranges-timeout-diagnostics.json"));
}

#[test]
fn mounted_smoke_writes_file_converges_three_mounts_and_unmounts() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(BASIC_MULTI_CLIENT_SMOKE);
    let clients = SMOKE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingSmokeDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 324,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_mounted_smoke_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.peer_bootstrap.volumes.len(), 3);
    assert_eq!(summary.volume_validations.len(), 3);
    assert_eq!(
        summary.volume_validations[0].convergence.compared_clients,
        SMOKE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>()
    );
    assert!(
        summary
            .volume_validations
            .iter()
            .all(|validation| validation.convergence.converged)
    );
    assert!(driver.mounted.get());
    assert!(driver.unmounted.get());

    for client in driver.clients {
        for validation in &summary.volume_validations {
            let file_name = PathBuf::from(&validation.written_file)
                .file_name()
                .unwrap()
                .to_owned();
            let payload = std::fs::read(client.mount_point.join(file_name)).unwrap();
            assert_eq!(payload, b"studiofs e2e mounted multi-client smoke\n");
        }
    }
    assert!(context.scenario_dir.join("smoke-summary.json").exists());
}

#[test]
fn mounted_smoke_validation_reads_legacy_summary_shape() {
    let temp = tempdir().unwrap();
    let summary_path = temp.path().join("smoke-summary.json");
    let legacy = LegacyMountedSmokeRunSummary {
        bootstrap: VolumeBootstrapSummary {
            volume_name: "projectfs".to_string(),
            init_client: "client-a".to_string(),
            discovered_clients: vec!["client-b".to_string(), "client-c".to_string()],
            clients: Vec::new(),
        },
        written_file: "Z:\\studiofs-e2e-smoke.txt".to_string(),
        convergence: TreeComparison {
            converged: true,
            reference_client: Some("client-a".to_string()),
            compared_clients: vec!["client-a".to_string()],
            differences: Vec::new(),
        },
        attempts: 2,
        elapsed_ms: 42,
    };
    std::fs::write(&summary_path, serde_json::to_vec(&legacy).unwrap()).unwrap();

    let validations = read_mounted_smoke_volume_validations(&summary_path).unwrap();

    assert_eq!(validations.len(), 1);
    assert_eq!(validations[0].volume_name, "projectfs");
    assert_eq!(validations[0].written_file, "Z:\\studiofs-e2e-smoke.txt");
    assert_eq!(validations[0].attempts, 2);
    assert!(validations[0].convergence.converged);
}

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
        if self.corrupt_converged_payload_after_remount {
            let clients = self.clients.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(20));
                for client in clients {
                    std::fs::write(
                        client
                            .mount_point
                            .join(CRASH_RESTART_LOCAL_DURABLE_FILE_NAME),
                        b"wrong converged payload after restart\n",
                    )
                    .unwrap();
                }
            });
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
}

impl RecordingNamespaceCollisionDriver {
    fn new(clients: Vec<ClientSpec>) -> Self {
        Self {
            clients,
            actions: RefCell::new(Vec::new()),
            unmount_error: None,
        }
    }

    fn with_unmount_error(mut self, error: &'static str) -> Self {
        self.unmount_error = Some(error);
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
        Ok(())
    }
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

#[test]
fn mounted_chaos_initial_barrier_rejects_unreplicated_seed_tree() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CHAOS_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("chaos-test");
    seed_mounted_chaos_initial_tree(&clients[0], &isolation).unwrap();

    let err = wait_for_mounted_chaos_initial_tree(
        &context,
        &clients,
        &isolation,
        Duration::from_millis(25),
        Duration::from_millis(5),
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(
        err.to_string()
            .contains("mounted chaos initial tree did not become visible")
    );
}

#[test]
fn mounted_chaos_initial_seed_rejects_stale_zero_byte_file() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let writer = client_spec_for(
        &sandbox,
        "projectfs",
        "client-a",
        temp.path().join("mounts").join("client-a"),
    );
    let isolation = test_isolation("chaos-test");
    let stale_path = writer.mount_point.join(isolation.path("rename-source.txt"));
    std::fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
    std::fs::write(&stale_path, []).unwrap();

    let err = seed_mounted_chaos_initial_tree(&writer, &isolation).unwrap_err();

    let err_text = err.to_string().to_ascii_lowercase();
    assert!(
        err_text.contains("mounted chaos initial tree") && err_text.contains("file exists"),
        "expected stale warmup file to be rejected before convergence validation, got {err:?}"
    );
    assert_eq!(
        std::fs::read(&stale_path).unwrap(),
        Vec::<u8>::new(),
        "stale warmup files must not be overwritten in place"
    );
}

#[test]
fn stale_remount_warm_history_rejects_stale_zero_byte_nested_file() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = temp.path().join("scenario");
    let shared_mount = temp.path().join("mounts").join("shared");
    std::fs::create_dir_all(&scenario_dir).unwrap();
    std::fs::create_dir_all(&shared_mount).unwrap();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox: sandbox.clone(),
        scenario_dir,
        client_controller_config: None,
    };
    let clients = STALE_REMOUNT_WARM_WRITER_NAMES
        .iter()
        .copied()
        .map(|name| client_spec_for(&sandbox, "projectfs", name, shared_mount.clone()))
        .collect::<Vec<_>>();
    let isolation = test_isolation("stale-remount-test");
    let nested_path = shared_mount.join(isolation.path(COLD_OBSERVER_NESTED_FILE));
    std::fs::create_dir_all(nested_path.parent().unwrap()).unwrap();
    std::fs::write(&nested_path, []).unwrap();

    let err =
        run_stale_remount_warm_history(&context, Instant::now(), &clients, &isolation, 3, &[])
            .unwrap_err();

    assert!(
        err.to_string()
            .contains(&isolation.path(COLD_OBSERVER_NESTED_FILE)),
        "expected stale nested file rejection, got {err:?}"
    );
    assert_eq!(
        std::fs::read(&nested_path).unwrap(),
        Vec::<u8>::new(),
        "stale warm-history files must not be overwritten in place"
    );
}

#[test]
fn stale_remount_warm_history_uses_bounded_helper_for_rename() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = temp.path().join("scenario");
    let shared_mount = temp.path().join("mounts").join("shared");
    std::fs::create_dir_all(&scenario_dir).unwrap();
    std::fs::create_dir_all(&shared_mount).unwrap();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox: sandbox.clone(),
        scenario_dir: scenario_dir.clone(),
        client_controller_config: None,
    };
    let clients = STALE_REMOUNT_WARM_WRITER_NAMES
        .iter()
        .copied()
        .map(|name| client_spec_for(&sandbox, "projectfs", name, shared_mount.clone()))
        .collect::<Vec<_>>();
    let isolation = test_isolation("stale-remount-helper-test");

    let (operations, _summary) =
        run_stale_remount_warm_history(&context, Instant::now(), &clients, &isolation, 3, &[])
            .unwrap();

    assert!(
        operations
            .iter()
            .any(|operation| { operation.operation == "rename" && operation.result == "accepted" })
    );
    let progress_dir = scenario_dir.join("hotspot-helper-progress");
    let rename_progress_log = std::fs::read_dir(progress_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .find(|log| log.contains("\"operation\":\"rename\""))
        .expect("stale-remount rename should run through bounded helper");
    assert!(rename_progress_log.contains("\"phase\":\"before_rename\""));
    assert!(rename_progress_log.contains("\"phase\":\"after_rename\""));
}

#[test]
fn mounted_initial_payload_barrier_rejects_wrong_peer_payload() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    write_mounted_initial_file(
        clients[0].mount_point.join("seed/file.txt"),
        b"expected payload\n",
        "test initial tree",
    )
    .unwrap();
    write_mounted_initial_file(
        clients[1].mount_point.join("seed/file.txt"),
        b"wrong payload\n",
        "test initial tree",
    )
    .unwrap();

    let missing = clients
        .iter()
        .filter_map(|client| {
            match mounted_initial_payload_probe(
                client.clone(),
                "seed/file.txt".to_string(),
                b"expected payload\n".to_vec(),
            ) {
                MountedPayloadProbeStatus::Present => None,
                MountedPayloadProbeStatus::Missing(path) => Some(path),
            }
        })
        .collect::<Vec<_>>();

    assert!(
        missing.iter().any(|entry| {
            entry.contains("client-b:seed/file.txt")
                && entry.contains("expected 17 bytes sha256=")
                && entry.contains("read 14 bytes sha256=")
        }),
        "wrong peer payload must be reported in missing initial payloads, got {missing:?}"
    );
}

#[test]
fn mounted_initial_payload_probe_timeout_reports_blocked_client_without_waiting_forever() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let clients = ["client-a", "client-b"]
        .into_iter()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let required_payloads = [("seed/file.txt", b"expected payload\n".as_slice())];
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    let started = Instant::now();

    let missing = missing_mounted_initial_payloads_with_probe(
        &clients,
        &required_payloads,
        &probe_limiter,
        Duration::from_millis(20),
        |client, _relative_path, _expected_payload| {
            if client.name == "client-b" {
                thread::sleep(Duration::from_secs(5));
            }
            MountedPayloadProbeStatus::Present
        },
    );

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "blocked mounted probe must not consume the scenario-child timeout"
    );
    assert_eq!(
        missing,
        vec!["client-b:seed/file.txt (probe timed out after 20 ms)"]
    );
}

#[test]
fn mounted_probe_timeout_poison_skips_remaining_validation_probes() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let clients = ["client-a", "client-b"]
        .into_iter()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let required_payloads = [
        ("seed/file-a.txt", b"expected payload\n".as_slice()),
        ("seed/file-b.txt", b"expected payload\n".as_slice()),
    ];
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    let started = Instant::now();

    let missing = missing_mounted_initial_payloads_with_probe(
        &clients,
        &required_payloads,
        &probe_limiter,
        Duration::from_millis(20),
        |client, _relative_path, _expected_payload| {
            if client.name == "client-a" {
                thread::sleep(Duration::from_secs(5));
            }
            MountedPayloadProbeStatus::Present
        },
    );

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "remaining mounted probes must fail fast after the first blocked probe timeout"
    );
    assert_eq!(
            missing,
            vec![
                "client-a:seed/file-a.txt (probe timed out after 20 ms)".to_string(),
                "client-a:seed/file-b.txt (probe skipped because a previous mounted probe timed out; active probe workers 1/1)".to_string(),
                "client-b:seed/file-a.txt (probe skipped because a previous mounted probe timed out; active probe workers 1/1)".to_string(),
                "client-b:seed/file-b.txt (probe skipped because a previous mounted probe timed out; active probe workers 1/1)".to_string(),
            ]
        );
}

#[test]
fn open_handle_initial_seed_rejects_stale_zero_byte_file() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let writer = client_spec_for(
        &sandbox,
        "projectfs",
        "client-a",
        temp.path().join("mounts").join("client-a"),
    );
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_OPEN_HANDLE_CONVERGENCE);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("open-handles-test");
    let stale_path = writer.mount_point.join(open_handle_isolated_path(
        &isolation,
        OPEN_HANDLE_RENAME_SOURCE,
    ));
    std::fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
    std::fs::write(&stale_path, []).unwrap();

    let err =
        seed_open_handle_initial_tree(&context, &[writer], &isolation, Duration::from_secs(1))
            .unwrap_err();

    let err_text = err.to_string().to_ascii_lowercase();
    assert!(
        err_text.contains("open handle seed") && err_text.contains("file exists"),
        "expected stale open-handle seed file to be rejected, got {err:?}"
    );
}

#[test]
fn mounted_chaos_initial_readback_waits_for_delayed_visibility() {
    let temp = tempdir().unwrap();
    let file_path = temp.path().join("chaos").join("delayed.txt");
    std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
    std::fs::write(&file_path, b"").unwrap();
    let payload = b"delayed mounted readback\n".to_vec();
    let writer_path = file_path.clone();
    let writer_payload = payload.clone();

    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(40));
        std::fs::write(writer_path, writer_payload).unwrap();
    });

    let readback = wait_for_mounted_payload_readback(
        &file_path,
        &payload,
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .unwrap();
    writer.join().unwrap();

    assert_eq!(readback, payload);
}

#[test]
fn mounted_payload_readback_distinguishes_missing_path_from_empty_file() {
    let temp = tempdir().unwrap();
    let missing_path = temp.path().join("missing-empty.txt");

    let err = wait_for_mounted_payload_readback(
        &missing_path,
        b"",
        Duration::from_millis(10),
        Duration::from_millis(1),
    )
    .unwrap_err();

    assert!(
        matches!(&err, ScenarioError::Io(error) if error.kind() == io::ErrorKind::NotFound),
        "missing path with empty expected payload must not pass readback, got {err:?}"
    );
}

#[test]
fn mounted_chaos_initial_barrier_accepts_replicated_seed_tree() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CHAOS_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("chaos-test");
    seed_mounted_chaos_initial_tree(&clients[0], &isolation).unwrap();
    for peer in clients.iter().skip(1) {
        replace_dir_contents(&clients[0].mount_point, &peer.mount_point).unwrap();
    }

    let convergence = wait_for_mounted_chaos_initial_tree(
        &context,
        &clients,
        &isolation,
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .unwrap();

    assert!(convergence.comparison.converged);
    assert_eq!(convergence.comparison.differences, Vec::new());
}

#[test]
fn hotspot_churn_initial_barrier_rejects_unreplicated_hotspot_dir() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("hotspot-test");
    std::fs::create_dir_all(clients[0].mount_point.join(&isolation.root)).unwrap();

    let err = wait_for_hotspot_initial_tree(
        &context,
        &clients,
        &isolation.root,
        Duration::from_millis(25),
        Duration::from_millis(5),
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(
        err.to_string()
            .contains("hotspot initial tree did not become visible")
    );
}

#[test]
fn hotspot_churn_initial_barrier_accepts_replicated_hotspot_dir() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(mount.join("hotspot-test")).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let convergence = wait_for_hotspot_initial_tree(
        &context,
        &clients,
        "hotspot-test",
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .unwrap();

    assert!(convergence.comparison.converged);
    assert_eq!(convergence.comparison.differences, Vec::new());
}

#[test]
fn advanced_mounted_validate_rejects_failed_long_history_oracle() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-long-history-validation");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let summary = AdvancedMountedSummary {
        scenario: MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE.to_string(),
        bootstrap: None,
        clients: 4,
        operations: Vec::new(),
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE}/operations.ndjson"
        ),
        final_validator: TreeComparison {
            converged: true,
            reference_client: Some("client-a".to_string()),
            compared_clients: vec![
                "client-a".to_string(),
                "client-b".to_string(),
                "client-c".to_string(),
                "client-d".to_string(),
            ],
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
        lww_oracle: None,
        sparse_ranges: None,
        cold_observer: None,
        long_history: Some(LongHistorySummary {
            bounded_operation_count: 11,
            expected_present_paths: vec!["missing.txt".to_string()],
            expected_absent_paths: Vec::new(),
            final_checksum_sha256: "bad-checksum".to_string(),
        }),
        long_history_validation: Some(LongHistoryValidation {
            matched: false,
            missing_present_paths: vec!["client-a:missing.txt".to_string()],
            unexpected_present_paths: Vec::new(),
            checksum_mismatches: Vec::new(),
        }),
        catchup: Some(planned_catchup_summary()),
        invalidation_dropout: None,
        symlink_attributes: None,
    };
    std::fs::write(
        scenario_dir.join("stale-remount-cold-discover-summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    let context = ScenarioContext {
        run_id: "run-long-history-validation".to_string(),
        seed: 418,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let scenario =
        AdvancedMountedScenario::new(AdvancedMountedKind::StaleRemountColdDiscoverCatchup);

    let result = scenario.validate(&context);
    assert!(result.is_err());
    let validation: serde_json::Value = serde_json::from_slice(
        &std::fs::read(context.scenario_dir.join("validation.json")).unwrap(),
    )
    .unwrap();

    assert_eq!(validation["status"], "failed");
}

#[test]
fn open_handle_initial_barrier_rejects_unreplicated_initial_tree() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_OPEN_HANDLE_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("open-handles-test");
    seed_open_handle_initial_tree(&context, &clients, &isolation, Duration::from_secs(1)).unwrap();

    let err = wait_for_open_handle_initial_tree(
        &context,
        &clients,
        &isolation,
        Duration::from_millis(25),
        Duration::from_millis(5),
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(
        err.to_string()
            .contains("open handle initial tree did not become visible")
    );
}

#[test]
fn open_handle_initial_barrier_accepts_replicated_initial_tree() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_OPEN_HANDLE_CONVERGENCE);
    let isolation = test_isolation("open-handles-test");
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            let unlink_target = open_handle_isolated_path(&isolation, OPEN_HANDLE_UNLINK_TARGET);
            let rename_source = open_handle_isolated_path(&isolation, OPEN_HANDLE_RENAME_SOURCE);
            write_parented(
                mount.join(&unlink_target),
                &isolation.payload(
                    "initial_unlink_target",
                    &unlink_target,
                    &String::from_utf8_lossy(OPEN_HANDLE_UNLINK_TARGET_PAYLOAD),
                ),
            )
            .unwrap();
            write_parented(
                mount.join(&rename_source),
                &isolation.payload(
                    "initial_rename_source",
                    &rename_source,
                    &String::from_utf8_lossy(OPEN_HANDLE_RENAME_SOURCE_PAYLOAD),
                ),
            )
            .unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let convergence = wait_for_open_handle_initial_tree(
        &context,
        &clients,
        &isolation,
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .unwrap();

    assert!(convergence.comparison.converged);
    assert_eq!(convergence.comparison.differences, Vec::new());
}

#[test]
fn mounted_smoke_reports_unmount_error_when_body_also_fails() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(BASIC_MULTI_CLIENT_SMOKE);
    let mut driver = RecordingSmokeDriver::new(Vec::new()).with_unmount_error("umount failed");
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 324,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_mounted_smoke_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    let message = err.to_string();
    assert!(matches!(
        err,
        ScenarioError::MountedSmokeUnmountAfterFailure { .. }
    ));
    assert!(message.contains("requires at least 3 clients"));
    assert!(message.contains("umount failed"));
}

#[test]
fn namespace_collision_records_operations_and_validates_three_client_convergence() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_RENAME_VS_UNLINK);
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingNamespaceCollisionDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 328,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_namespace_collision_with_driver(
        &context,
        NamespaceCollisionKind::RenameVsUnlink,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.scenario, COLLISION_RENAME_VS_UNLINK);
    assert_eq!(summary.clients, 3);
    assert_eq!(summary.operations.len(), 2);
    assert!(summary.final_validator.converged);
    assert!(summary.namespace_violations.is_empty());
    assert_eq!(
        driver.actions.borrow().as_slice(),
        [
            "bootstrap",
            "mount_all",
            "replicate",
            "replicate",
            "unmount_all"
        ]
    );
    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("\"operation\":\"rename\""));
    assert!(operations_log.contains("\"operation\":\"unlink\""));
    assert!(
        context
            .scenario_dir
            .join("namespace-collision-summary.json")
            .exists()
    );
}

#[test]
fn namespace_collision_preserves_primary_error_when_unmount_also_fails() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_RENAME_VS_UNLINK);
    let mut driver =
        RecordingNamespaceCollisionDriver::new(Vec::new()).with_unmount_error("umount failed");
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 328,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_namespace_collision_with_driver(
        &context,
        NamespaceCollisionKind::RenameVsUnlink,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    let message = err.to_string();
    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionUnmountAfterFailure { .. }
    ));
    assert!(message.contains("requires at least 3 clients"));
    assert!(message.contains("umount failed"));
}

#[test]
fn namespace_collision_recording_replication_collects_errors_after_all_clients() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let source_mount = temp.path().join("mounts").join("client-a");
    let bad_mount = temp.path().join("mounts").join("client-b");
    let copied_mount = temp.path().join("mounts").join("client-c");
    std::fs::create_dir_all(&source_mount).unwrap();
    std::fs::write(source_mount.join("copied.txt"), b"payload").unwrap();
    std::fs::create_dir_all(bad_mount.parent().unwrap()).unwrap();
    std::fs::write(&bad_mount, b"not a directory").unwrap();
    std::fs::create_dir_all(&copied_mount).unwrap();
    let driver = RecordingNamespaceCollisionDriver::new(vec![
        client_spec_for(&sandbox, "projectfs", "client-a", source_mount),
        client_spec_for(&sandbox, "projectfs", "client-b", bad_mount),
        client_spec_for(&sandbox, "projectfs", "client-c", copied_mount.clone()),
    ]);

    let err = driver.replicate_from_first_client().unwrap_err();

    assert!(matches!(err, ScenarioError::ReplicationFailure { .. }));
    assert_eq!(
        std::fs::read(copied_mount.join("copied.txt")).unwrap(),
        b"payload"
    );
}

#[test]
fn namespace_collision_operation_timestamps_are_portable_u64_values() {
    fn accepts_u64(_value: u64) {}

    let operations = planned_namespace_collision_operations(
        NamespaceCollisionKind::RenameVsUnlink,
        Instant::now(),
    );

    accepts_u64(operations[0].timestamp_ms);
}

#[test]
fn namespace_collision_operation_timeout_reports_pending_without_waiting_forever() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-collision-timeout");
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
    let started = Instant::now();

    let err = run_concurrent_namespace_operations(
        started,
        vec![NamespaceCollisionTask::new(
            1,
            client,
            "rename",
            "shared/file.txt -> shared/renamed-a.txt",
            |_client| {
                thread::sleep(Duration::from_millis(200));
                Ok(())
            },
        )],
        Duration::from_millis(20),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert!(
        err.to_string()
            .contains("client-a rename shared/file.txt -> shared/renamed-a.txt")
    );
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "timeout must not wait for the hung namespace worker to finish"
    );
}

#[test]
fn namespace_collision_timeout_does_not_wait_for_blocked_mount_force_kill() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-collision-timeout-cleanup");
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingNamespaceCollisionDriver::new(clients);
    let primary = ScenarioError::NamespaceCollisionOperationTimeout {
        timeout_ms: 30_000,
        pending: vec!["client-a rename shared/file.txt".to_string()],
    };
    let started = Instant::now();

    let err = handle_namespace_collision_operation_timeout::<NamespaceCollisionSummary>(
        primary,
        &mut driver,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "timeout cleanup should return to the parent process boundary instead of waiting for blocked mount force-kill"
    );
    assert_eq!(
        driver.actions.borrow().as_slice(),
        ["emergency_kill_all_no_wait"],
        "operation timeout should delegate cleanup to the parent process boundary"
    );
}

#[test]
fn namespace_collision_collect_timeout_allows_helper_cleanup_grace() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-collision-helper-grace");
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
    let started = Instant::now();

    let operations = run_concurrent_namespace_operations(
        started,
        vec![
            NamespaceCollisionTask::new(1, client, "rename", "shared/file.txt", |_client| {
                thread::sleep(Duration::from_millis(80));
                Ok(())
            })
            .with_completion_grace(Duration::from_millis(100)),
        ],
        Duration::from_millis(20),
    )
    .unwrap();

    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].result, "accepted");
    assert!(
        started.elapsed() >= Duration::from_millis(80),
        "helper-backed namespace operations must be allowed to finish bounded cleanup after the operation timeout"
    );
}

#[test]
fn namespace_collision_initial_tree_writes_directly() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let sandbox = temp.path().join("run-collision-seed-direct");
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);

    establish_collision_initial_tree(NamespaceCollisionKind::RenameVsUnlink, &client).unwrap();

    let payload = std::fs::read(client.mount_point.join("shared/file.txt")).unwrap();
    assert_eq!(payload, b"initial namespace payload\n");
}

#[test]
fn open_handle_sync_retry_accepts_transient_not_found() {
    let mut attempts = 0;

    let retries = sync_open_handle_with_retry(
        || {
            attempts += 1;
            if attempts < 3 {
                Err(io::Error::new(io::ErrorKind::NotFound, "transient"))
            } else {
                Ok(())
            }
        },
        Duration::from_millis(100),
        Duration::from_millis(1),
    )
    .unwrap();

    assert_eq!(retries, 2);
    assert_eq!(attempts, 3);
}

#[test]
fn open_handle_sync_retry_preserves_non_not_found_error() {
    let err = sync_open_handle_with_retry(
        || Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied")),
        Duration::from_millis(100),
        Duration::from_millis(1),
    )
    .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn open_handle_helper_records_progress_for_old_handle_sequence() {
    let temp = tempdir().unwrap();
    let client_a_mount = temp.path().join("mounts").join("client-a");
    let client_b_mount = temp.path().join("mounts").join("client-b");
    let unlink_path = "open-handles/unlink-target.txt";
    let rename_source = "open-handles/rename-source.txt";
    let rename_dest = "open-handles/renamed-destination.txt";
    let append_payload_path = temp.path().join("append.payload");
    let progress = temp.path().join("progress.jsonl");

    std::fs::create_dir_all(client_a_mount.join("open-handles")).unwrap();
    std::fs::create_dir_all(client_b_mount.join("open-handles")).unwrap();
    std::fs::write(client_a_mount.join(unlink_path), b"initial\n").unwrap();
    std::fs::write(client_b_mount.join(unlink_path), b"initial\n").unwrap();
    std::fs::write(client_b_mount.join(rename_source), b"rename source\n").unwrap();
    std::fs::write(&append_payload_path, b"append through old handle\n").unwrap();

    run_open_handle_op_helper(
        client_a_mount.clone(),
        client_b_mount.clone(),
        unlink_path,
        rename_source,
        rename_dest,
        &append_payload_path,
        Some(&progress),
    )
    .unwrap();

    assert!(!client_b_mount.join(unlink_path).exists());
    assert!(client_b_mount.join(rename_dest).exists());
    assert_eq!(
        std::fs::read(client_a_mount.join(unlink_path)).unwrap(),
        b"initial\nappend through old handle\n"
    );
    let progress_log = std::fs::read_to_string(progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_open_old_handle\""));
    assert!(progress_log.contains("\"phase\":\"after_remove_file\""));
    assert!(progress_log.contains("\"phase\":\"after_rename\""));
    assert!(progress_log.contains("\"phase\":\"after_sync_all\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_churn_operation_timeout_reports_pending_without_waiting_forever() {
    let active_workers = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let err = run_timed_hotspot_io_with_worker_limit(
        Duration::from_millis(20),
        "client-a mkdir hotspot".to_string(),
        move || {
            release_rx
                .recv()
                .map_err(|_| io::Error::other("hotspot worker release channel closed"))?;
            Ok(())
        },
        Arc::clone(&active_workers),
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert!(err.to_string().contains("client-a mkdir hotspot"));
    assert_eq!(
        active_workers.load(Ordering::SeqCst),
        1,
        "hotspot timeout must return while the mounted operation worker is still pending"
    );

    release_tx.send(()).unwrap();
    for _ in 0..50 {
        if active_workers.load(Ordering::SeqCst) == 0 {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(active_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn hotspot_rename_source_barrier_accepts_visible_source() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-hotspot-source-visible");
    let mount = temp.path().join("mounts").join("client-a");
    let source = mount.join("hotspot").join("slot-0.txt");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, b"payload").unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);

    wait_for_client_path_visible_until(
        &client,
        "hotspot/slot-0.txt",
        Instant::now() + Duration::from_secs(1),
        Duration::from_millis(5),
        "hotspot rename source",
    )
    .unwrap();
}

#[test]
fn hotspot_rename_source_barrier_rejects_missing_source_before_rename() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-hotspot-source-missing");
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);

    let err = wait_for_client_path_visible_until(
        &client,
        "hotspot/slot-0.txt",
        Instant::now() + Duration::from_millis(20),
        Duration::from_millis(5),
        "hotspot rename source",
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(err.to_string().contains("hotspot rename source"));
    assert!(err.to_string().contains("client-a:hotspot/slot-0.txt"));
}

#[test]
fn hotspot_churn_operation_timeout_bounds_pending_workers() {
    let active_workers = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let err = run_timed_hotspot_io_with_worker_limit(
        Duration::from_millis(20),
        "client-a mkdir hotspot".to_string(),
        move || {
            release_rx
                .recv()
                .map_err(|_| io::Error::other("hotspot worker release channel closed"))?;
            Ok(())
        },
        Arc::clone(&active_workers),
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert_eq!(active_workers.load(Ordering::SeqCst), 1);

    let started = Instant::now();
    let err = run_timed_hotspot_io_with_worker_limit(
        Duration::from_secs(1),
        "client-b append hotspot/slot-1.txt".to_string(),
        || Ok(()),
        Arc::clone(&active_workers),
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert!(err.to_string().contains("hotspot worker limit reached"));
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "saturated hotspot worker limit must fail without spawning another worker"
    );

    release_tx.send(()).unwrap();
    for _ in 0..50 {
        if active_workers.load(Ordering::SeqCst) == 0 {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(active_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn hotspot_churn_completed_operations_release_worker_before_next_operation() {
    let active_workers = Arc::new(AtomicUsize::new(0));

    for index in 0..256 {
        run_timed_hotspot_io_with_worker_limit(
            Duration::from_secs(1),
            format!("completed hotspot operation {index}"),
            || Ok(()),
            Arc::clone(&active_workers),
            HOTSPOT_IO_MAX_ACTIVE_WORKERS,
        )
        .unwrap();

        assert_eq!(
            active_workers.load(Ordering::SeqCst),
            0,
            "completed hotspot operation {index} returned before releasing its worker permit"
        );
    }
}

#[test]
fn hotspot_helper_internal_timeout_leaves_parent_kill_reserve() {
    assert_eq!(
        hotspot_helper_internal_timeout(Duration::from_secs(15)),
        Duration::from_millis(12_250)
    );
    assert_eq!(
        hotspot_helper_internal_timeout(Duration::from_secs(1)),
        Duration::from_secs(1),
        "short operation timeouts should not underflow"
    );
}

#[test]
fn hotspot_helper_io_failure_is_recorded_as_rejected_operation() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-helper-reject");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let mount = temp.path().join("mount");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
    let context = ScenarioContext {
        run_id: "run-helper-reject".to_string(),
        seed: 415,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let record = timed_hotspot_helper_fs_op(
        &context,
        Instant::now(),
        1,
        &client,
        "rename",
        "missing.txt",
        Duration::from_secs(5),
        None,
        Some("renamed.txt"),
    )
    .unwrap();

    assert_eq!(record.result, "rejected");
    assert!(record.error.unwrap().contains("hotspot helper failed"));
}

#[test]
fn required_hotspot_churn_operation_is_logged_before_failing() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-required-helper-reject");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let context = ScenarioContext {
        run_id: "run-required-helper-reject".to_string(),
        seed: 415,
        sandbox,
        scenario_dir: scenario_dir.clone(),
        client_controller_config: None,
    };
    let operation = NamespaceCollisionOperation {
        order: 1,
        client: "client-a".to_string(),
        operation: "rename".to_string(),
        path: "hotspot/slot-0.txt".to_string(),
        result: "rejected".to_string(),
        timestamp_ms: 0,
        error: Some("source disappeared".to_string()),
    };
    let mut operations = Vec::new();

    let err = record_required_sparse_operation(&context, &mut operations, operation)
        .expect_err("rejected hotspot churn operations must fail the scenario");

    assert!(err.to_string().contains("source disappeared"));
    assert_eq!(operations.len(), 1);
    let log = std::fs::read_to_string(scenario_dir.join("operations.ndjson")).unwrap();
    assert!(log.contains("\"result\":\"rejected\""));
    assert!(log.contains("source disappeared"));
}

#[test]
fn hotspot_overwrite_can_use_barrier_helper_while_recording_overwrite() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-helper-barrier");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let mount = temp.path().join("mount");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount.clone());
    let context = ScenarioContext {
        run_id: "run-helper-barrier".to_string(),
        seed: 415,
        sandbox,
        scenario_dir: scenario_dir.clone(),
        client_controller_config: None,
    };

    let record = timed_hotspot_helper_fs_op_with_helper(
        &context,
        Instant::now(),
        1,
        &client,
        "overwrite",
        "overwrite-barrier",
        "hotspot/slot-1.txt",
        Duration::from_secs(5),
        Some(b"barrier payload"),
        None,
    )
    .unwrap();

    assert_eq!(record.operation, "overwrite");
    assert_eq!(record.result, "accepted");
    assert_eq!(
        std::fs::read(mount.join("hotspot").join("slot-1.txt")).unwrap(),
        b"barrier payload"
    );

    let progress_dir = scenario_dir.join("hotspot-helper-progress");
    let progress_file = std::fs::read_dir(progress_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.to_string_lossy().contains("overwrite-barrier"))
        .expect("barrier helper progress file");
    let progress_log = std::fs::read_to_string(progress_file).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_open_barrier_write\""));
    assert!(progress_log.contains("\"phase\":\"after_readback\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_helper_stat_records_exact_metadata_probe() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mount");
    let progress = temp.path().join("progress.jsonl");
    std::fs::create_dir_all(mount.join("hotspot")).unwrap();
    std::fs::write(mount.join("hotspot").join("slot-1.txt"), b"payload").unwrap();

    run_hotspot_op_helper(
        "stat",
        mount,
        "hotspot/slot-1.txt",
        None,
        None,
        Some(&progress),
    )
    .unwrap();

    let progress_log = std::fs::read_to_string(progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_metadata\""));
    assert!(progress_log.contains("\"phase\":\"after_metadata\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_helper_unlink_removes_file_and_records_progress() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mount");
    let progress = temp.path().join("progress.jsonl");
    let target = mount.join("hotspot").join("unlink-target.txt");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, b"payload").unwrap();

    run_hotspot_op_helper(
        "unlink",
        mount,
        "hotspot/unlink-target.txt",
        None,
        None,
        Some(&progress),
    )
    .unwrap();

    assert!(!target.exists());
    let progress_log = std::fs::read_to_string(progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_remove_file\""));
    assert!(progress_log.contains("\"phase\":\"after_remove_file\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_helper_records_progress_for_sync_barrier_phases() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mount");
    let payload = temp.path().join("payload.bin");
    let progress = temp.path().join("progress.jsonl");
    std::fs::create_dir_all(&mount).unwrap();
    std::fs::write(&payload, b"hotspot payload").unwrap();

    run_hotspot_op_helper(
        "overwrite",
        mount,
        "hotspot/slot-1.txt",
        None,
        Some(&payload),
        Some(&progress),
    )
    .unwrap();

    let progress_log = std::fs::read_to_string(progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_write\""));
    assert!(progress_log.contains("\"phase\":\"after_write\""));
    assert!(progress_log.contains("\"phase\":\"before_sync_all\""));
    assert!(progress_log.contains("\"phase\":\"after_sync_all\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_helper_progress_records_complete_ndjson_lines() {
    let temp = tempdir().unwrap();
    let progress = temp.path().join("progress.jsonl");
    let path = temp.path().join("mount").join("hotspot").join("slot.txt");

    record_hotspot_helper_progress(
        Some(&progress),
        "before_write",
        "overwrite",
        &path,
        Some("target.txt"),
        Some(42),
    )
    .unwrap();
    record_hotspot_helper_progress(
        Some(&progress),
        "after_write",
        "overwrite",
        &path,
        None,
        None,
    )
    .unwrap();

    let progress_log = std::fs::read_to_string(progress).unwrap();
    let lines = progress_log.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    for line in lines {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(value["operation"], "overwrite");
        assert!(value["timestamp_ms"].is_u64());
    }
    assert!(
        progress_log.ends_with('\n'),
        "progress log should end each NDJSON record with a newline"
    );
}

#[test]
fn overwrite_vs_read_collision_records_repeated_read_loop() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_OVERWRITE_VS_READ);
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingNamespaceCollisionDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 328,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_namespace_collision_with_driver(
        &context,
        NamespaceCollisionKind::OverwriteVsRead,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    let read_loop = summary
        .operations
        .iter()
        .find(|operation| operation.operation == "read_loop")
        .expect("overwrite/read scenario should record repeated reads");
    assert_eq!(read_loop.result, "accepted");
}

#[test]
fn mounted_smoke_reports_expected_and_actual_client_count() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(BASIC_MULTI_CLIENT_SMOKE);
    let clients = SMOKE_CLIENT_NAMES[..2]
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingSmokeDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 324,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_mounted_smoke_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::InsufficientMountedClients {
            expected: 3,
            actual: 2
        }
    ));
}

#[test]
fn registry_exposes_postgres_outage_recovery_scenario() {
    let scenarios = available_scenarios();

    assert!(scenarios.iter().any(|scenario| {
        scenario.name == POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY
            && scenario.requires_studiofs_bin
            && scenario.description.contains("PostgreSQL outage")
    }));
    assert!(resolve_scenario(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY).is_some());
    assert!(resolve_scenario("postgres-outage").is_some());
}

#[test]
fn registry_exposes_rustfs_outage_recovery_scenario() {
    let scenarios = available_scenarios();

    assert!(scenarios.iter().any(|scenario| {
        scenario.name == RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY
            && scenario.requires_studiofs_bin
            && scenario.description.contains("RustFS/S3 outage")
    }));
    assert!(resolve_scenario(RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY).is_some());
    assert!(resolve_scenario("rustfs-outage").is_some());
    assert!(resolve_scenario("s3-outage").is_some());
}

#[test]
fn registry_exposes_mounted_client_crash_restart_recovery_scenario() {
    let scenarios = available_scenarios();

    assert!(scenarios.iter().any(|scenario| {
        scenario.name == MOUNTED_CLIENT_CRASH_RESTART_RECOVERY
            && scenario.requires_studiofs_bin
            && scenario.description.contains("crash/restart")
    }));
    assert!(resolve_scenario(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY).is_some());
    assert!(resolve_scenario("client-crash-restart").is_some());
    assert!(resolve_scenario("crash-restart").is_some());
}

#[test]
fn crash_restart_recovery_runs_both_modes_reuses_client_root_and_converges() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let original_client_root = clients[0].client_root.clone();
    let mut driver =
        RecordingCrashRestartDriver::new(clients).publish_current_file_before_convergence();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.bootstrap.init_client, "client-a");
    assert_eq!(summary.modes.len(), 2);
    assert_eq!(
        summary.modes[0].mode,
        CrashRestartMode::AfterLocalDurabilityBoundary
    );
    assert_eq!(
        summary.modes[1].mode,
        CrashRestartMode::BeforeRemotePublicationCompletes
    );
    assert!(summary.modes.iter().all(|mode| {
        mode.restart_client_root == original_client_root.display().to_string()
            && mode.local_read_checksum_sha256 == mode.final_checksum_sha256
            && mode.final_validator.converged
    }));
    assert!(summary.modes[0].peer_visible_before_convergence.is_none());
    assert!(summary.modes[1].peer_visible_before_convergence.is_none());
    assert_eq!(
        driver.actions.borrow().as_slice(),
        [
            "bootstrap",
            "mount_all",
            "force_kill_client-a",
            "remount_client-a",
            "force_kill_client-a",
            "remount_client-a",
            "unmount_all",
        ]
    );

    let summary_json: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            context
                .scenario_dir
                .join("crash-restart-recovery-summary.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(summary_json["modes"].as_array().unwrap().len(), 2);
    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("after_local_durability_boundary"));
    assert!(operations_log.contains("before_remote_publication_completes"));
}

#[test]
fn crash_restart_recovery_accepts_peer_visible_before_convergence() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-peer-visible");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients).publish_current_file_on_remount();
    let context = ScenarioContext {
        run_id: "run-peer-visible".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(summary.modes[0].peer_visible_before_convergence.is_none());
    assert_eq!(
        summary.modes[1].peer_visible_before_convergence.as_deref(),
        summary.modes[1].local_read_checksum_sha256.as_deref()
    );
    assert!(
        summary
            .modes
            .iter()
            .all(|mode| mode.final_validator.converged)
    );
}

#[test]
fn crash_restart_before_convergence_publish_reports_missing_source() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-missing-source");
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingCrashRestartDriver::new(clients).publish_current_file_before_convergence();

    let err = driver
        .before_peer_convergence_wait(CRASH_RESTART_CLIENT_NAMES[0], "missing.txt")
        .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::Io(error) if error.kind() == std::io::ErrorKind::NotFound
    ));
}

#[test]
fn crash_restart_recovery_uses_recovered_writer_checksum_as_oracle() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-recovered-writer");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients)
        .corrupt_local_on_remount()
        .publish_current_file_on_remount();
    let context = ScenarioContext {
        run_id: "run-recovered-writer".to_string(),
        seed: 404,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    let recovered_checksum = bytes_sha256(b"corrupted payload after restart\n");
    assert_eq!(
        summary.modes[0].local_read_checksum_sha256,
        Some(recovered_checksum.clone())
    );
    assert_eq!(
        summary.modes[0].final_checksum_sha256,
        Some(recovered_checksum)
    );
    assert!(summary.modes[0].final_validator.converged);
}

#[test]
fn crash_restart_before_publication_requires_final_peer_convergence() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-before-publication-writer-only");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingCrashRestartDriver::new(clients.clone()).publish_current_file_before_convergence();
    let context = ScenarioContext {
        run_id: "run-before-publication-writer-only".to_string(),
        seed: 405,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_mode(
        &context,
        &mut driver,
        &clients,
        CrashRestartMode::BeforeRemotePublicationCompletes,
        CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
        b"writer-only before publication\n",
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(
        summary.final_validator.compared_clients,
        CRASH_RESTART_CLIENT_NAMES
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        summary.local_read_checksum_sha256,
        summary.final_checksum_sha256
    );
}

#[test]
fn crash_restart_before_publication_fails_when_recovered_state_never_converges() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-before-publication-missing-peer");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients.clone());
    let context = ScenarioContext {
        run_id: "run-before-publication-missing-peer".to_string(),
        seed: 405,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_crash_restart_mode(
        &context,
        &mut driver,
        &clients,
        CrashRestartMode::BeforeRemotePublicationCompletes,
        CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
        b"writer-only before publication\n",
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::CrashRestartPeerConvergenceFailed { .. }
    ));
}

#[test]
fn crash_restart_reports_peer_convergence_failure_when_recovered_state_is_not_published() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients).corrupt_local_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::CrashRestartPeerConvergenceFailed { .. }
    ));
}

#[test]
fn crash_restart_reports_unrecoverable_loss_when_writer_cannot_read_after_restart() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-unrecoverable");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients).remove_local_on_remount();
    let context = ScenarioContext {
        run_id: "run-unrecoverable".to_string(),
        seed: 404,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(summary.modes.iter().all(|mode| {
        mode.recovery_outcome == CrashRestartRecoveryOutcome::UnrecoverableLoss
            && mode.local_read_checksum_sha256.is_none()
            && mode.final_checksum_sha256.is_none()
            && mode.final_validator.converged
    }));
}

#[test]
fn crash_restart_reports_corrupt_advertised_file_when_final_peer_differs_from_recovered_writer() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingCrashRestartDriver::new(clients).corrupt_converged_payload_after_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::CrashRestartCorruptAdvertisedFile { .. }
    ));
}

#[test]
fn crash_restart_convergence_includes_every_available_client() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let client_names = ["client-a", "client-b", "client-c"];
    let clients = client_names
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingCrashRestartDriver::new(clients).publish_current_file_before_convergence();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(
        summary.modes[0].final_validator.compared_clients,
        client_names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        summary.modes[1].final_validator.compared_clients,
        client_names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>()
    );
}

#[test]
fn postgres_outage_records_local_operations_and_recovery_metrics() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients).publish_writer_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.bootstrap.init_client, "client-a");
    assert_eq!(
        summary.outage_file,
        "studiofs-e2e-postgres-outage-writer.txt"
    );
    assert_eq!(summary.writer_recovery_client, "client-a");
    assert_eq!(
        summary.writer_recovery_status,
        PostgresOutageWriterRecoveryStatus::Recovered
    );
    assert_eq!(
        summary.writer_recovery_bytes,
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len()
    );
    assert_eq!(summary.operations_accepted_during_outage, 1);
    assert!(summary.outage_duration_ms > 0);
    assert!(summary.writer_recovery_time_ms > 0);
    assert!(summary.peer_observation_time_ms > 0);
    assert!(summary.final_validator.converged);
    assert_eq!(
        driver.actions.borrow().as_slice(),
        [
            "bootstrap",
            "mount_all",
            "stop_postgres",
            "force_kill_writer",
            "start_postgres",
            "remount_writer",
            "unmount_all"
        ]
    );

    let summary_json: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            context
                .scenario_dir
                .join("postgres-outage-recovery-summary.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(summary_json["operations_accepted_during_outage"], 1);
    assert_eq!(summary_json["writer_recovery_client"], "client-a");
    assert_eq!(summary_json["writer_recovery_status"], "recovered");
    assert_eq!(
        summary_json["writer_recovery_bytes"],
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len()
    );
    assert!(summary_json["expected_writer_recovery_checksum_sha256"].is_string());
    assert!(summary_json["writer_recovery_checksum_sha256"].is_string());
    assert!(summary_json["peer_observation_time_ms"].is_number());
    assert!(
        context
            .scenario_dir
            .join("outage-operations.ndjson")
            .exists()
    );
    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("\"accepted\":true"));
    assert!(operations_log.contains("\"client\":\"client-a\""));
    assert!(!operations_log.contains("\"client\":\"client-b\""));
}

#[test]
fn postgres_outage_allows_missing_writer_recovery_after_unclean_shutdown() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients).lose_writer_payload_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 403,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(
        summary.writer_recovery_status,
        PostgresOutageWriterRecoveryStatus::LostAfterUncleanShutdown
    );
    assert_eq!(summary.writer_recovery_bytes, 0);
    assert_eq!(summary.writer_recovery_checksum_sha256, bytes_sha256(&[]));
    assert!(summary.final_validator.converged);
    assert_eq!(
        summary.final_validator.compared_clients,
        POSTGRES_OUTAGE_CLIENT_NAMES
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert!(summary.peer_observation_time_ms > 0);
}

#[test]
fn postgres_outage_empty_writer_placeholder_fails_final_convergence() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients)
        .lose_writer_payload_on_remount()
        .keep_lost_writer_payload_local_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 404,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    let ScenarioError::PostgresOutagePeerConvergenceFailed { detail } = err else {
        panic!("expected peer convergence failure, got {err:?}");
    };
    assert!(
        detail.contains("did not converge"),
        "lost writer placeholder should fail final convergence: {detail}"
    );
}

#[test]
fn postgres_outage_writer_recovery_reports_equal_length_corruption() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients)
        .publish_writer_on_remount()
        .corrupt_writer_payload_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 403,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    let ScenarioError::PostgresOutageWriterRecoveryFailed { detail, .. } = err else {
        panic!("expected writer recovery error");
    };
    assert!(detail.contains("expected checksum"));
    assert!(detail.contains("recovered checksum"));
}

#[test]
fn postgres_outage_duration_includes_postgres_restart_wait() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients)
        .publish_writer_on_remount()
        .with_start_postgres_delay(Duration::from_millis(40));
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 403,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(
        summary.outage_duration_ms >= 30,
        "outage duration should include PostgreSQL restart wait, got {}ms",
        summary.outage_duration_ms
    );
}

#[test]
fn rustfs_outage_keeps_local_writes_private_until_recovery_and_records_checksums() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = RUSTFS_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingRustfsOutageDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 326,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_rustfs_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.bootstrap.init_client, "client-a");
    assert_eq!(summary.outage_file, "studiofs-e2e-rustfs-outage.txt");
    assert_eq!(
        summary.local_read_checksum_sha256,
        summary.final_checksum_sha256
    );
    let initial_checksum = bytes_sha256(RUSTFS_OUTAGE_INITIAL_PAYLOAD);
    assert_eq!(summary.peer_visibility_during_outage.len(), 3);
    assert!(summary.peer_visibility_during_outage.iter().all(|probe| {
        probe.visible_checksum_sha256.as_deref() == Some(initial_checksum.as_str())
    }));
    assert_eq!(
        summary.peer_visible_during_outage.as_deref(),
        Some(initial_checksum.as_str())
    );
    assert_eq!(summary.final_client_checksums.len(), 4);
    assert!(
        summary
            .final_client_checksums
            .iter()
            .all(|entry| entry.checksum_sha256 == summary.local_read_checksum_sha256)
    );
    assert!(summary.final_validator.converged);
    assert_eq!(
        driver.actions.borrow().as_slice(),
        [
            "bootstrap",
            "mount_all",
            "after_initial_write",
            "stop_rustfs",
            "assert_peer_not_visible",
            "assert_peer_not_visible",
            "assert_peer_not_visible",
            "start_rustfs",
            "unmount_all"
        ]
    );

    let summary_json: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            context
                .scenario_dir
                .join("rustfs-outage-recovery-summary.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(summary_json["upload_recovery_time_ms"].is_number());
    assert_eq!(
        summary_json["local_read_checksum_sha256"],
        summary_json["final_checksum_sha256"]
    );
    assert_eq!(
        summary_json["peer_visibility_during_outage"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        summary_json["final_client_checksums"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert!(
        context
            .scenario_dir
            .join("rustfs-outage-operations.ndjson")
            .exists()
    );
    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("\"scenario\":\"rustfs-outage-multi-client-recovery\""));
}

#[test]
fn rustfs_outage_records_all_peer_visibility_and_final_checksums() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-four-client");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = ["client-a", "client-b", "client-c", "client-d"]
        .into_iter()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingRustfsOutageDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-four-client".to_string(),
        seed: 326,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_rustfs_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.peer_visibility_during_outage.len(), 3);
    assert_eq!(summary.final_client_checksums.len(), 4);
    assert!(
        summary
            .final_client_checksums
            .iter()
            .all(|entry| entry.checksum_sha256 == summary.local_read_checksum_sha256)
    );
}

#[test]
fn rustfs_outage_duration_excludes_peer_visibility_probe_latency() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = RUSTFS_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingRustfsOutageDriver::new(clients).with_peer_probe_delay(Duration::from_millis(200));
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 326,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_rustfs_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(
        summary.outage_duration_ms < 100,
        "outage duration should exclude the peer visibility probe latency, got {}ms",
        summary.outage_duration_ms
    );
}

#[test]
fn rustfs_peer_visibility_probe_propagates_unexpected_io_errors() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let mount = temp.path().join("mounts").join("client-b");
    std::fs::create_dir_all(mount.join(RUSTFS_OUTAGE_FILE_NAME)).unwrap();
    let peer = client_spec_for(&sandbox, "projectfs", "client-b", mount);

    let err = assert_peer_not_visible(&peer, RUSTFS_OUTAGE_FILE_NAME, "forbidden").unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
}

#[test]
fn compose_control_reports_missing_run_manifest_path() {
    let temp = tempdir().unwrap();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox: temp.path().join("run-1"),
        scenario_dir: temp.path().join("scenario"),
        client_controller_config: None,
    };

    let err = compose_control_from_manifest(&context).unwrap_err();

    assert!(matches!(err, ScenarioError::MissingRunManifest { .. }));
    assert!(err.to_string().contains("run.json"));
}

#[test]
fn outage_operations_log_rejected_writer_write_before_failing_expected_operation() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let rejected_mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(rejected_mount.parent().unwrap()).unwrap();
    std::fs::write(&rejected_mount, "not a directory").unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", rejected_mount);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    std::fs::create_dir_all(&context.scenario_dir).unwrap();

    let err = perform_postgres_writer_outage_operation(
        &context,
        &client,
        POSTGRES_OUTAGE_FILE_NAME,
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::RejectedOutageOperations {
            rejected: 1,
            total: 1
        }
    ));
    let log =
        std::fs::read_to_string(context.scenario_dir.join("outage-operations.ndjson")).unwrap();
    assert!(log.contains("\"accepted\":false"));
    assert!(log.contains("\"client\":\"client-a\""));
}

#[test]
fn outage_operations_require_existing_scenario_dir() {
    let temp = tempdir().unwrap();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox: temp.path().join("run-1"),
        scenario_dir: temp.path().join("missing-scenario"),
        client_controller_config: None,
    };
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&context.sandbox, "projectfs", "client-a", mount);

    let err = perform_postgres_writer_outage_operation(
        &context,
        &client,
        POSTGRES_OUTAGE_FILE_NAME,
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD,
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(!context.scenario_dir.exists());
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
    }
}

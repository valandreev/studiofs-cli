//! Scenario registry and lifecycle hooks for mounted E2E runs.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::client::{
    ClientControllerConfig, ClientControllerError, ClientSpec, MultiClientController,
    SystemClientProcessExecutor, VolumeBootstrapOptions, VolumeBootstrapSummary,
};
use crate::mounted_tree::{
    ClientMount, ConvergenceOutcome, MountedTreeError, TreeComparison, wait_for_convergence,
};

pub const BASIC_MULTI_CLIENT_SMOKE: &str = "basic-multi-client-smoke";
const SMOKE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const SMOKE_FILE_NAME: &str = "studiofs-e2e-smoke.txt";
const SMOKE_FILE_PAYLOAD: &[u8] = b"studiofs e2e mounted multi-client smoke\n";
const SMOKE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const SMOKE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[cfg(test)]
pub(crate) const TEST_FAILING_VALIDATE: &str = "__test-failing-validate";
#[cfg(test)]
pub(crate) const TEST_FAILING_VALIDATE_AND_CLEANUP: &str = "__test-failing-validate-and-cleanup";
#[cfg(test)]
pub(crate) const TEST_PASSING: &str = "__test-passing";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScenarioDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub requires_studiofs_bin: bool,
}

#[derive(Debug, Clone)]
pub struct ScenarioContext {
    pub run_id: String,
    pub seed: u64,
    pub sandbox: PathBuf,
    pub scenario_dir: PathBuf,
    pub client_controller_config: Option<ClientControllerConfig>,
}

#[derive(Debug, Error)]
pub enum ScenarioError {
    #[error("scenario I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("scenario JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("scenario client controller failed: {0}")]
    Client(#[from] ClientControllerError),
    #[error("mounted smoke scenario failed: {primary}; unmount also failed: {cleanup}")]
    MountedSmokeUnmountAfterFailure { primary: String, cleanup: String },
    #[error("mounted tree validation failed: {0}")]
    MountedTree(#[from] MountedTreeError),
    #[error("mounted smoke scenario requires at least {expected} clients, got {actual}")]
    InsufficientMountedClients { expected: usize, actual: usize },
}

pub trait Scenario {
    fn definition(&self) -> ScenarioDefinition;
    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError>;
    fn cleanup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
}

pub fn available_scenarios() -> Vec<ScenarioDefinition> {
    vec![BasicMultiClientSmoke.definition()]
}

pub(crate) fn resolve_scenario(name: &str) -> Option<Box<dyn Scenario>> {
    match name {
        BASIC_MULTI_CLIENT_SMOKE | "smoke" => Some(Box::new(BasicMultiClientSmoke)),
        #[cfg(test)]
        TEST_FAILING_VALIDATE => Some(Box::new(TestFailingScenario {
            cleanup_fails: false,
        })),
        #[cfg(test)]
        TEST_FAILING_VALIDATE_AND_CLEANUP => Some(Box::new(TestFailingScenario {
            cleanup_fails: true,
        })),
        #[cfg(test)]
        TEST_PASSING => Some(Box::new(TestPassingScenario)),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct BasicMultiClientSmoke;

impl Scenario for BasicMultiClientSmoke {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: BASIC_MULTI_CLIENT_SMOKE,
            description: "mounted multi-client smoke scenario",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut controller =
                MultiClientController::new(config.clone(), SystemClientProcessExecutor);
            run_mounted_smoke_with_driver(
                context,
                &mut controller,
                SMOKE_CONVERGENCE_TIMEOUT,
                SMOKE_CONVERGENCE_POLL_INTERVAL,
            )?;
            return Ok(());
        }

        let workload = json!({
            "scenario": BASIC_MULTI_CLIENT_SMOKE,
            "run_id": context.run_id,
            "seed": context.seed,
            "workload": "placeholder",
        });
        fs::write(
            context.scenario_dir.join("workload.json"),
            serde_json::to_vec_pretty(&workload)?,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let bootstrap_summary = context.scenario_dir.join("bootstrap-summary.json");
        let smoke_summary = context.scenario_dir.join("smoke-summary.json");
        let validation = if bootstrap_summary.exists() {
            let summary: VolumeBootstrapSummary =
                serde_json::from_slice(&fs::read(&bootstrap_summary)?)?;
            let mut validation = json!({
                "scenario": BASIC_MULTI_CLIENT_SMOKE,
                "run_id": context.run_id,
                "seed": context.seed,
                "status": "passed",
                "workflow": "volume-bootstrap-discovery",
                "volume_name": summary.volume_name,
                "init_client": summary.init_client,
                "discovered_clients": summary.discovered_clients,
                "local_metadata_dbs": summary.clients
                    .iter()
                    .map(|client| client.local_metadata_db.display().to_string())
                    .collect::<Vec<_>>(),
            });
            if smoke_summary.exists() {
                let summary: MountedSmokeRunSummary =
                    serde_json::from_slice(&fs::read(&smoke_summary)?)?;
                validation["workflow"] = json!("mounted-multi-client-smoke");
                validation["written_file"] = json!(summary.written_file);
                validation["convergence"] = json!(summary.convergence);
            }
            validation
        } else {
            json!({
                "scenario": BASIC_MULTI_CLIENT_SMOKE,
                "run_id": context.run_id,
                "seed": context.seed,
                "status": "passed",
                "notes": "placeholder validation for the initial runner crate",
            })
        };

        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        let mut artifact_paths = Vec::new();
        if bootstrap_summary.exists() {
            artifact_paths.push(format!(
                "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/bootstrap-summary.json"
            ));
        }
        if smoke_summary.exists() {
            artifact_paths.push(format!(
                "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/smoke-summary.json"
            ));
        }
        artifact_paths.push(format!(
            "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/validation.json"
        ));
        Ok(artifact_paths)
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn write_bootstrap_summary(
    context: &ScenarioContext,
    summary: &VolumeBootstrapSummary,
) -> Result<(), ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    fs::write(
        context.scenario_dir.join("bootstrap-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

trait MountedSmokeDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ClientControllerError>;
    fn mount_all(&mut self) -> Result<(), ClientControllerError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn unmount_all(&mut self) -> Result<(), ClientControllerError>;
}

impl<E: crate::client::ClientProcessExecutor> MountedSmokeDriver for MultiClientController<E> {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ClientControllerError> {
        MultiClientController::bootstrap_volume(self, options)
    }

    fn mount_all(&mut self) -> Result<(), ClientControllerError> {
        MultiClientController::mount_all(self)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        MultiClientController::clients(self)
            .into_iter()
            .cloned()
            .collect()
    }

    fn unmount_all(&mut self) -> Result<(), ClientControllerError> {
        MultiClientController::unmount_all(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MountedSmokeRunSummary {
    bootstrap: VolumeBootstrapSummary,
    written_file: String,
    convergence: TreeComparison,
    attempts: usize,
    elapsed_ms: u128,
}

fn run_mounted_smoke_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn MountedSmokeDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedSmokeRunSummary, ScenarioError> {
    let result = run_mounted_smoke_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::MountedSmokeUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

fn run_mounted_smoke_body(
    context: &ScenarioContext,
    driver: &mut dyn MountedSmokeDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedSmokeRunSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: SMOKE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;

    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < SMOKE_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: SMOKE_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }

    let smoke_file = clients[0].mount_point.join(SMOKE_FILE_NAME);
    fs::write(&smoke_file, SMOKE_FILE_PAYLOAD)?;
    let mounts = clients
        .iter()
        .take(SMOKE_CLIENT_NAMES.len())
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let summary = mounted_smoke_summary(bootstrap, smoke_file, convergence);
    write_smoke_summary(context, &summary)?;
    Ok(summary)
}

fn mounted_smoke_summary(
    bootstrap: VolumeBootstrapSummary,
    smoke_file: PathBuf,
    convergence: ConvergenceOutcome,
) -> MountedSmokeRunSummary {
    MountedSmokeRunSummary {
        bootstrap,
        written_file: smoke_file.display().to_string(),
        convergence: convergence.comparison,
        attempts: convergence.attempts,
        elapsed_ms: convergence.elapsed_ms,
    }
}

fn write_smoke_summary(
    context: &ScenarioContext,
    summary: &MountedSmokeRunSummary,
) -> Result<(), ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    fs::write(
        context.scenario_dir.join("smoke-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct TestFailingScenario {
    cleanup_fails: bool,
}

#[cfg(test)]
impl Scenario for TestFailingScenario {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: if self.cleanup_fails {
                TEST_FAILING_VALIDATE_AND_CLEANUP
            } else {
                TEST_FAILING_VALIDATE
            },
            description: "test-only failing scenario",
            requires_studiofs_bin: false,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }

    fn validate(&self, _context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        Err(ScenarioError::Io(std::io::Error::other(
            "validation failed",
        )))
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        if self.cleanup_fails {
            Err(ScenarioError::Io(std::io::Error::other("cleanup failed")))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct TestPassingScenario;

#[cfg(test)]
impl Scenario for TestPassingScenario {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: TEST_PASSING,
            description: "test-only passing scenario",
            requires_studiofs_bin: false,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }

    fn validate(&self, _context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        Ok(Vec::new())
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::path::{Path, PathBuf};
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::client::{ClientLifecycleState, ClientSpec, S3Config, client_spec_for};

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
        let source_mount = temp.path().join("mounts").join(SMOKE_CLIENT_NAMES[0]);
        let replica_mounts = SMOKE_CLIENT_NAMES
            .iter()
            .copied()
            .skip(1)
            .map(|name| temp.path().join("mounts").join(name))
            .collect::<Vec<_>>();
        thread::spawn(move || {
            let source = source_mount.join("studiofs-e2e-smoke.txt");
            for _ in 0..100 {
                if let Ok(payload) = std::fs::read(&source) {
                    for replica in &replica_mounts {
                        std::fs::write(replica.join("studiofs-e2e-smoke.txt"), &payload).unwrap();
                    }
                    return;
                }
                thread::sleep(Duration::from_millis(5));
            }
        });
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

        assert_eq!(summary.bootstrap.init_client, "client-a");
        assert_eq!(
            summary.bootstrap.discovered_clients,
            SMOKE_CLIENT_NAMES[1..]
                .iter()
                .map(|name| (*name).to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            summary.convergence.compared_clients,
            SMOKE_CLIENT_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect::<Vec<_>>()
        );
        assert!(summary.convergence.converged);
        assert!(driver.mounted.get());
        assert!(driver.unmounted.get());

        for client in driver.clients {
            let payload = std::fs::read(client.mount_point.join("studiofs-e2e-smoke.txt")).unwrap();
            assert_eq!(payload, b"studiofs e2e mounted multi-client smoke\n");
        }
        assert!(context.scenario_dir.join("smoke-summary.json").exists());
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
        fn bootstrap_volume(
            &mut self,
            options: VolumeBootstrapOptions,
        ) -> Result<VolumeBootstrapSummary, ClientControllerError> {
            assert_eq!(
                options.client_names,
                SMOKE_CLIENT_NAMES
                    .iter()
                    .copied()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            );
            Ok(VolumeBootstrapSummary {
                volume_name: "projectfs".to_string(),
                init_client: SMOKE_CLIENT_NAMES[0].to_string(),
                discovered_clients: SMOKE_CLIENT_NAMES[1..]
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

        fn mount_all(&mut self) -> Result<(), ClientControllerError> {
            self.mounted.set(true);
            Ok(())
        }

        fn clients(&self) -> Vec<ClientSpec> {
            self.clients.clone()
        }

        fn unmount_all(&mut self) -> Result<(), ClientControllerError> {
            self.unmounted.set(true);
            if let Some(error) = self.unmount_error {
                return Err(ClientControllerError::Io(std::io::Error::other(error)));
            }
            Ok(())
        }
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
        }
    }
}

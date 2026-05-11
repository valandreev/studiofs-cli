//! Scenario registry and lifecycle hooks for mounted E2E runs.

use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::client::{
    ClientControllerConfig, ClientControllerError, ClientSpec, MultiClientController,
    PeerVolumeBootstrapOptions, PeerVolumeBootstrapSummary, SystemClientProcessExecutor,
    VolumeBootstrapOptions, VolumeBootstrapSummary,
};
use crate::infra::{
    CommandExecutor, ComposeServiceControl, InfraError, POSTGRES_SERVICE_NAME, RUSTFS_SERVICE_NAME,
    SystemCommandExecutor,
};
use crate::mounted_tree::{
    ClientMount, ConvergenceOutcome, MountedTreeError, NamespaceInvariantViolation, TreeComparison,
    validate_namespace_invariants, wait_for_convergence,
};
use crate::report::RunManifest;

pub const BASIC_MULTI_CLIENT_SMOKE: &str = "basic-multi-client-smoke";
pub const POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY: &str = "postgres-outage-multi-client-recovery";
pub const RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY: &str = "rustfs-outage-multi-client-recovery";
pub const MOUNTED_CLIENT_CRASH_RESTART_RECOVERY: &str = "mounted-client-crash-restart-recovery";
pub const COLLISION_RENAME_VS_RENAME: &str = "collision-rename-vs-rename";
pub const COLLISION_RENAME_VS_UNLINK: &str = "collision-rename-vs-unlink";
pub const COLLISION_UNLINK_VS_APPEND: &str = "collision-unlink-vs-append";
pub const COLLISION_OVERWRITE_VS_READ: &str = "collision-overwrite-vs-read";
pub const COLLISION_MKDIR_RMDIR_VS_CREATE: &str = "collision-mkdir-rmdir-vs-create";
pub const COLLISION_READDIR_STAT_DURING_MUTATION: &str = "collision-readdir-stat-during-mutation";
const SMOKE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const COLLISION_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const POSTGRES_OUTAGE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const RUSTFS_OUTAGE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
const CRASH_RESTART_CLIENT_NAMES: &[&str] = &["client-a", "client-b"];
const SMOKE_FILE_NAME: &str = "studiofs-e2e-smoke.txt";
const SMOKE_FILE_PAYLOAD: &[u8] = b"studiofs e2e mounted multi-client smoke\n";
const SMOKE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const SMOKE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OUTAGE_INITIAL_FILE_NAME: &str = "studiofs-e2e-outage-initial.txt";
const POSTGRES_OUTAGE_FILE_NAME: &str = "studiofs-e2e-postgres-outage-writer.txt";
const POSTGRES_OUTAGE_RECOVERED_PAYLOAD: &[u8] =
    b"accepted writer-local PostgreSQL outage payload\n";
const OUTAGE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const OUTAGE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const RUSTFS_OUTAGE_FILE_NAME: &str = "studiofs-e2e-rustfs-outage.txt";
const RUSTFS_OUTAGE_INITIAL_PAYLOAD: &[u8] = b"initial healthy RustFS payload\n";
const RUSTFS_OUTAGE_RECOVERED_PAYLOAD: &[u8] =
    b"initial healthy RustFS payload\naccepted local payload while RustFS is down\n";
const CRASH_RESTART_LOCAL_DURABLE_FILE_NAME: &str = "studiofs-e2e-crash-after-local-durability.txt";
const CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME: &str =
    "studiofs-e2e-crash-before-remote-publication.txt";
const CRASH_RESTART_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const CRASH_RESTART_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const COLLISION_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const COLLISION_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const COLLISION_OLD_COMMITTED_PAYLOAD: &[u8] = b"old committed payload\n";
const COLLISION_NEW_COMMITTED_PAYLOAD: &[u8] = b"new committed payload\n";
const OVERWRITE_READ_ATTEMPTS: u32 = 8;

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
    #[error("PostgreSQL outage scenario failed: {primary}; unmount also failed: {cleanup}")]
    PostgresOutageUnmountAfterFailure { primary: String, cleanup: String },
    #[error("RustFS/S3 outage scenario failed: {primary}; unmount also failed: {cleanup}")]
    RustfsOutageUnmountAfterFailure { primary: String, cleanup: String },
    #[error(
        "mounted client crash/restart scenario failed: {primary}; unmount also failed: {cleanup}"
    )]
    CrashRestartUnmountAfterFailure { primary: String, cleanup: String },
    #[error("mounted tree validation failed: {0}")]
    MountedTree(#[from] MountedTreeError),
    #[error("scenario infrastructure failed: {0}")]
    Infra(#[from] InfraError),
    #[error("PostgreSQL outage scenario requires compose infrastructure")]
    MissingComposeInfrastructure,
    #[error("missing E2E run manifest: {path}")]
    MissingRunManifest { path: PathBuf },
    #[error("PostgreSQL outage operations rejected {rejected} of {total} expected local writes")]
    RejectedOutageOperations { rejected: usize, total: usize },
    #[error("PostgreSQL outage writer recovery failed for {client}:{path}: {detail}")]
    PostgresOutageWriterRecoveryFailed {
        client: String,
        path: String,
        detail: String,
    },
    #[error("PostgreSQL outage writer remount failed for {client}: {detail}")]
    PostgresOutageWriterRemountFailed { client: String, detail: String },
    #[error("PostgreSQL outage peer convergence failed after writer publication: {detail}")]
    PostgresOutagePeerConvergenceFailed { detail: String },
    #[error("peer client observed the RustFS/S3 outage write before recovery: {checksum}")]
    PeerObservedOutageWrite { checksum: String },
    #[error("mounted smoke scenario requires at least {expected} clients, got {actual}")]
    InsufficientMountedClients { expected: usize, actual: usize },
    #[error("PostgreSQL outage scenario requires at least {expected} clients, got {actual}")]
    InsufficientOutageClients { expected: usize, actual: usize },
    #[error(
        "mounted client crash/restart scenario requires at least {expected} clients, got {actual}"
    )]
    InsufficientCrashRestartClients { expected: usize, actual: usize },
    #[error("mounted client crash/restart publication failed for {path}: {detail}")]
    CrashRestartPublicationFailed { path: String, detail: String },
    #[error("mounted client crash/restart peer convergence failed for {path}: {detail}")]
    CrashRestartPeerConvergenceFailed { path: String, detail: String },
    #[error(
        "mounted client crash/restart corrupt advertised file for {client}:{path}: expected recovered checksum {expected_checksum}, got {actual_checksum}"
    )]
    CrashRestartCorruptAdvertisedFile {
        client: String,
        path: String,
        expected_checksum: String,
        actual_checksum: String,
    },
    #[error("namespace collision scenario requires at least {expected} clients, got {actual}")]
    InsufficientCollisionClients { expected: usize, actual: usize },
    #[error("namespace collision scenario failed: {primary}; unmount also failed: {cleanup}")]
    NamespaceCollisionUnmountAfterFailure { primary: String, cleanup: String },
    #[error("namespace collision validation failed: {violations:?}")]
    NamespaceInvariantViolations {
        violations: Vec<NamespaceInvariantViolation>,
    },
    #[cfg(test)]
    #[error("namespace collision recording replication failed: {errors:?}")]
    ReplicationFailure { errors: Vec<String> },
}

pub trait Scenario {
    fn definition(&self) -> ScenarioDefinition;
    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError>;
    fn cleanup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
}

pub fn available_scenarios() -> Vec<ScenarioDefinition> {
    vec![
        BasicMultiClientSmoke.definition(),
        RustfsOutageMultiClientRecovery.definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsRename).definition(),
        PostgresOutageMultiClientRecovery.definition(),
        MountedClientCrashRestartRecovery.definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsUnlink).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::UnlinkVsAppend).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::OverwriteVsRead).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::MkdirRmdirVsCreate).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::ReaddirStatDuringMutation)
            .definition(),
    ]
}

pub(crate) fn resolve_scenario(name: &str) -> Option<Box<dyn Scenario>> {
    match name {
        BASIC_MULTI_CLIENT_SMOKE | "smoke" => Some(Box::new(BasicMultiClientSmoke)),
        POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY | "postgres-outage" => {
            Some(Box::new(PostgresOutageMultiClientRecovery))
        }
        RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY | "rustfs-outage" | "s3-outage" => {
            Some(Box::new(RustfsOutageMultiClientRecovery))
        }
        MOUNTED_CLIENT_CRASH_RESTART_RECOVERY | "client-crash-restart" | "crash-restart" => {
            Some(Box::new(MountedClientCrashRestartRecovery))
        }
        COLLISION_RENAME_VS_RENAME | "rename-vs-rename" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsRename),
        )),
        COLLISION_RENAME_VS_UNLINK | "rename-vs-unlink" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsUnlink),
        )),
        COLLISION_UNLINK_VS_APPEND | "unlink-vs-append" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::UnlinkVsAppend),
        )),
        COLLISION_OVERWRITE_VS_READ | "overwrite-vs-read" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::OverwriteVsRead),
        )),
        COLLISION_MKDIR_RMDIR_VS_CREATE | "mkdir-rmdir-vs-create" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::MkdirRmdirVsCreate),
        )),
        COLLISION_READDIR_STAT_DURING_MUTATION | "readdir-stat-during-mutation" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::ReaddirStatDuringMutation),
        )),
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
        let peer_bootstrap_summary = context.scenario_dir.join("peer-bootstrap-summary.json");
        let bootstrap_summary = context.scenario_dir.join("bootstrap-summary.json");
        let smoke_summary = context.scenario_dir.join("smoke-summary.json");
        let validation = if peer_bootstrap_summary.exists() {
            let summary: PeerVolumeBootstrapSummary =
                serde_json::from_slice(&fs::read(&peer_bootstrap_summary)?)?;
            let mut validation = json!({
                "scenario": BASIC_MULTI_CLIENT_SMOKE,
                "run_id": context.run_id,
                "seed": context.seed,
                "status": "passed",
                "workflow": "mounted-peer-volume-discovery",
                "base_volume_name": summary.base_volume_name,
                "volumes": summary.volumes,
            });
            if smoke_summary.exists() {
                validation["volume_validations"] =
                    json!(read_mounted_smoke_volume_validations(&smoke_summary)?);
            }
            validation
        } else if bootstrap_summary.exists() {
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
                validation["workflow"] = json!("mounted-multi-client-smoke");
                validation["volume_validations"] =
                    json!(read_mounted_smoke_volume_validations(&smoke_summary)?);
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
        if peer_bootstrap_summary.exists() {
            artifact_paths.push(format!(
                "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/peer-bootstrap-summary.json"
            ));
        }
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

#[derive(Debug, Clone, Copy)]
struct PostgresOutageMultiClientRecovery;

impl Scenario for PostgresOutageMultiClientRecovery {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
            description: "mounted multi-client PostgreSQL outage recovery scenario",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let Some(config) = &context.client_controller_config else {
            return Err(ScenarioError::MissingComposeInfrastructure);
        };
        let compose_control = compose_control_from_manifest(context)?;
        let mut driver = RealPostgresOutageDriver {
            controller: MultiClientController::new(config.clone(), SystemClientProcessExecutor),
            compose_control,
            executor: SystemCommandExecutor,
        };
        run_postgres_outage_with_driver(
            context,
            &mut driver,
            OUTAGE_CONVERGENCE_TIMEOUT,
            OUTAGE_CONVERGENCE_POLL_INTERVAL,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("postgres-outage-recovery-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: PostgresOutageRecoverySummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged { "passed" } else { "failed" },
            "outage_duration_ms": summary.outage_duration_ms,
            "recovery_time_ms": summary.recovery_time_ms,
            "writer_recovery_client": summary.writer_recovery_client,
            "writer_recovery_time_ms": summary.writer_recovery_time_ms,
            "writer_recovery_checksum_sha256": summary.writer_recovery_checksum_sha256,
            "peer_observation_time_ms": summary.peer_observation_time_ms,
            "operations_accepted_during_outage": summary.operations_accepted_during_outage,
            "final_validator": summary.final_validator,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY}/outage-operations.ndjson"
            ),
            format!(
                "artifacts/scenarios/{POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY}/postgres-outage-recovery-summary.json"
            ),
            format!("artifacts/scenarios/{POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct RustfsOutageMultiClientRecovery;

impl Scenario for RustfsOutageMultiClientRecovery {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
            description: "mounted multi-client RustFS/S3 outage recovery scenario",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let Some(config) = &context.client_controller_config else {
            return Err(ScenarioError::MissingComposeInfrastructure);
        };
        let compose_control = compose_control_from_manifest(context)?;
        let mut driver = RealRustfsOutageDriver {
            controller: MultiClientController::new(config.clone(), SystemClientProcessExecutor),
            compose_control,
            executor: SystemCommandExecutor,
        };
        run_rustfs_outage_with_driver(
            context,
            &mut driver,
            OUTAGE_CONVERGENCE_TIMEOUT,
            OUTAGE_CONVERGENCE_POLL_INTERVAL,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("rustfs-outage-recovery-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: RustfsOutageRecoverySummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged { "passed" } else { "failed" },
            "outage_duration_ms": summary.outage_duration_ms,
            "upload_recovery_time_ms": summary.upload_recovery_time_ms,
            "outage_file": summary.outage_file,
            "local_read_checksum_sha256": summary.local_read_checksum_sha256,
            "peer_visible_during_outage": summary.peer_visible_during_outage,
            "final_checksum_sha256": summary.final_checksum_sha256,
            "final_validator": summary.final_validator,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY}/rustfs-outage-operations.ndjson"
            ),
            format!(
                "artifacts/scenarios/{RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY}/rustfs-outage-recovery-summary.json"
            ),
            format!("artifacts/scenarios/{RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct MountedClientCrashRestartRecovery;

impl Scenario for MountedClientCrashRestartRecovery {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
            description: "mounted multi-client crash/restart recovery scenario",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let Some(config) = &context.client_controller_config else {
            return Err(ScenarioError::MissingComposeInfrastructure);
        };
        let mut driver = RealCrashRestartDriver(MultiClientController::new(
            config.clone(),
            SystemClientProcessExecutor,
        ));
        run_crash_restart_with_driver(
            context,
            &mut driver,
            CRASH_RESTART_CONVERGENCE_TIMEOUT,
            CRASH_RESTART_CONVERGENCE_POLL_INTERVAL,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("crash-restart-recovery-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: CrashRestartRecoverySummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.modes.iter().all(|mode| mode.final_validator.converged) {
                "passed"
            } else {
                "failed"
            },
            "modes": summary.modes,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{MOUNTED_CLIENT_CRASH_RESTART_RECOVERY}/operations.ndjson"
            ),
            format!(
                "artifacts/scenarios/{MOUNTED_CLIENT_CRASH_RESTART_RECOVERY}/crash-restart-recovery-summary.json"
            ),
            format!("artifacts/scenarios/{MOUNTED_CLIENT_CRASH_RESTART_RECOVERY}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn compose_control_from_manifest(
    context: &ScenarioContext,
) -> Result<ComposeServiceControl, ScenarioError> {
    let manifest_path = context.sandbox.join("run.json");
    if !manifest_path.exists() {
        return Err(ScenarioError::MissingRunManifest {
            path: manifest_path,
        });
    }
    let manifest: RunManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    let Some(project) = manifest.compose_project.as_deref() else {
        return Err(ScenarioError::MissingComposeInfrastructure);
    };
    let mut env = vec![("STUDIOFS_E2E_RUN_ID".to_string(), manifest.run_id)];
    env.extend(
        manifest
            .allocated_ports
            .into_iter()
            .map(|port| (port.env_var, port.host_port.to_string())),
    );
    Ok(ComposeServiceControl::for_project_with_environment(
        project.to_string(),
        env,
    )?)
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

fn write_peer_bootstrap_summary(
    context: &ScenarioContext,
    summary: &PeerVolumeBootstrapSummary,
) -> Result<(), ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    fs::write(
        context.scenario_dir.join("peer-bootstrap-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

trait MountedSmokeDriver {
    fn bootstrap_peer_volumes(
        &mut self,
        options: PeerVolumeBootstrapOptions,
    ) -> Result<PeerVolumeBootstrapSummary, ClientControllerError>;
    fn select_volume_for_clients(&mut self, volume_name: &str)
    -> Result<(), ClientControllerError>;
    fn mount_all(&mut self) -> Result<(), ClientControllerError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn unmount_all(&mut self) -> Result<(), ClientControllerError>;
}

impl<E: crate::client::ClientProcessExecutor> MountedSmokeDriver for MultiClientController<E> {
    fn bootstrap_peer_volumes(
        &mut self,
        options: PeerVolumeBootstrapOptions,
    ) -> Result<PeerVolumeBootstrapSummary, ClientControllerError> {
        MultiClientController::bootstrap_peer_volumes(self, options)
    }

    fn select_volume_for_clients(
        &mut self,
        volume_name: &str,
    ) -> Result<(), ClientControllerError> {
        MultiClientController::select_volume_for_clients(self, volume_name)
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
    peer_bootstrap: PeerVolumeBootstrapSummary,
    volume_validations: Vec<MountedSmokeVolumeValidation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyMountedSmokeRunSummary {
    bootstrap: VolumeBootstrapSummary,
    written_file: String,
    convergence: TreeComparison,
    attempts: usize,
    elapsed_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MountedSmokeVolumeValidation {
    volume_name: String,
    written_file: String,
    convergence: TreeComparison,
    attempts: usize,
    elapsed_ms: u128,
}

fn read_mounted_smoke_volume_validations(
    path: &Path,
) -> Result<Vec<MountedSmokeVolumeValidation>, ScenarioError> {
    let artifact: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    if artifact.get("volume_validations").is_some() {
        let summary: MountedSmokeRunSummary = serde_json::from_value(artifact)?;
        return Ok(summary.volume_validations);
    }
    let summary: LegacyMountedSmokeRunSummary = serde_json::from_value(artifact)?;
    Ok(vec![MountedSmokeVolumeValidation {
        volume_name: summary.bootstrap.volume_name,
        written_file: summary.written_file,
        convergence: summary.convergence,
        attempts: summary.attempts,
        elapsed_ms: summary.elapsed_ms,
    }])
}

fn run_mounted_smoke_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn MountedSmokeDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedSmokeRunSummary, ScenarioError> {
    let result = run_mounted_smoke_body(context, driver, convergence_timeout, poll_interval);
    match result {
        Ok(summary) => Ok(summary),
        Err(primary) => match driver.unmount_all() {
            Ok(()) => Err(primary),
            Err(cleanup) => Err(ScenarioError::MountedSmokeUnmountAfterFailure {
                primary: primary.to_string(),
                cleanup: cleanup.to_string(),
            }),
        },
    }
}

fn run_mounted_smoke_body(
    context: &ScenarioContext,
    driver: &mut dyn MountedSmokeDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedSmokeRunSummary, ScenarioError> {
    let peer_bootstrap = driver.bootstrap_peer_volumes(PeerVolumeBootstrapOptions {
        client_names: SMOKE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_peer_bootstrap_summary(context, &peer_bootstrap)?;
    if peer_bootstrap.volumes.len() < SMOKE_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: SMOKE_CLIENT_NAMES.len(),
            actual: peer_bootstrap.volumes.len(),
        });
    }

    let mut volume_validations = Vec::with_capacity(peer_bootstrap.volumes.len());
    for volume in &peer_bootstrap.volumes {
        driver.select_volume_for_clients(&volume.volume_name)?;
        driver.mount_all()?;
        let clients = driver.clients();
        if clients.len() < SMOKE_CLIENT_NAMES.len() {
            return Err(ScenarioError::InsufficientMountedClients {
                expected: SMOKE_CLIENT_NAMES.len(),
                actual: clients.len(),
            });
        }

        let writer = clients
            .iter()
            .find(|client| client.name == volume.init_client)
            .expect("init_client for volume should always be in the list of clients");
        let smoke_file = writer
            .mount_point
            .join(format!("{}-{SMOKE_FILE_NAME}", volume.volume_name));
        fs::write(&smoke_file, SMOKE_FILE_PAYLOAD)?;
        let mounts = clients
            .iter()
            .take(SMOKE_CLIENT_NAMES.len())
            .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
            .collect::<Vec<_>>();
        let convergence = wait_for_convergence(
            &mounts,
            context
                .scenario_dir
                .join("validation")
                .join(&volume.volume_name),
            convergence_timeout,
            poll_interval,
        )?;
        volume_validations.push(mounted_smoke_volume_validation(
            volume.volume_name.clone(),
            smoke_file,
            convergence,
        ));
        driver.unmount_all()?;
    }

    let summary = MountedSmokeRunSummary {
        peer_bootstrap,
        volume_validations,
    };
    write_smoke_summary(context, &summary)?;
    Ok(summary)
}

fn mounted_smoke_volume_validation(
    volume_name: String,
    smoke_file: PathBuf,
    convergence: ConvergenceOutcome,
) -> MountedSmokeVolumeValidation {
    MountedSmokeVolumeValidation {
        volume_name,
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

trait PostgresOutageDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn stop_postgres(&mut self) -> Result<(), ScenarioError>;
    fn start_postgres(&mut self) -> Result<(), ScenarioError>;
    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn remount(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealPostgresOutageDriver<E> {
    controller: MultiClientController<SystemClientProcessExecutor>,
    compose_control: ComposeServiceControl,
    executor: E,
}

impl<E: CommandExecutor> PostgresOutageDriver for RealPostgresOutageDriver<E> {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.controller.bootstrap_volume(options)?)
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.mount_all()?)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.controller.clients().into_iter().cloned().collect()
    }

    fn stop_postgres(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .stop_service(POSTGRES_SERVICE_NAME, &self.executor)?)
    }

    fn start_postgres(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .start_service(POSTGRES_SERVICE_NAME, &self.executor)?)
    }

    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.controller.force_kill(client)?)
    }

    fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.controller.remount(client)?)
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.unmount_all()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OutageOperationRecord {
    client: String,
    path: String,
    bytes: usize,
    accepted: bool,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PostgresOutageRecoverySummary {
    bootstrap: VolumeBootstrapSummary,
    outage_file: String,
    outage_duration_ms: u128,
    operations_accepted_during_outage: usize,
    writer_recovery_client: String,
    writer_recovery_status: PostgresOutageWriterRecoveryStatus,
    writer_recovery_time_ms: u128,
    writer_recovery_bytes: usize,
    expected_writer_recovery_checksum_sha256: String,
    writer_recovery_checksum_sha256: String,
    peer_observation_time_ms: u128,
    recovery_time_ms: u128,
    final_validator: TreeComparison,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PostgresOutageWriterRecoveryStatus {
    Recovered,
    LostAfterUncleanShutdown,
}

fn run_postgres_outage_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn PostgresOutageDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<PostgresOutageRecoverySummary, ScenarioError> {
    let result = run_postgres_outage_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::PostgresOutageUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_postgres_outage_body(
    context: &ScenarioContext,
    driver: &mut dyn PostgresOutageDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<PostgresOutageRecoverySummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: POSTGRES_OUTAGE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;

    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < POSTGRES_OUTAGE_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientOutageClients {
            expected: POSTGRES_OUTAGE_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }

    fs::write(
        clients[0].mount_point.join(OUTAGE_INITIAL_FILE_NAME),
        b"initial healthy PostgreSQL write\n",
    )?;

    let writer = &clients[0];
    let outage_started = Instant::now();
    driver.stop_postgres()?;
    let accepted_operations = perform_postgres_writer_outage_operation(
        context,
        writer,
        POSTGRES_OUTAGE_FILE_NAME,
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD,
    )?;
    driver.force_kill(&writer.name)?;

    // `start_postgres` uses `docker compose up --wait`, so this includes
    // PostgreSQL healthcheck readiness before mounted convergence polling.
    let recovery_started = Instant::now();
    driver.start_postgres()?;
    let outage_duration_ms = elapsed_millis_nonzero(outage_started);
    let writer_recovery_started = Instant::now();
    driver.remount(&writer.name).map_err(|error| {
        ScenarioError::PostgresOutageWriterRemountFailed {
            client: writer.name.clone(),
            detail: error.to_string(),
        }
    })?;
    let writer_payload =
        fs::read(writer.mount_point.join(POSTGRES_OUTAGE_FILE_NAME)).map_err(|error| {
            ScenarioError::PostgresOutageWriterRecoveryFailed {
                client: writer.name.clone(),
                path: POSTGRES_OUTAGE_FILE_NAME.to_string(),
                detail: error.to_string(),
            }
        })?;
    let expected_checksum = bytes_sha256(POSTGRES_OUTAGE_RECOVERED_PAYLOAD);
    let recovered_checksum = bytes_sha256(&writer_payload);
    let writer_recovery_status = if writer_payload == POSTGRES_OUTAGE_RECOVERED_PAYLOAD {
        PostgresOutageWriterRecoveryStatus::Recovered
    } else if writer_payload.is_empty() {
        PostgresOutageWriterRecoveryStatus::LostAfterUncleanShutdown
    } else {
        let recovered_checksum = bytes_sha256(&writer_payload);
        return Err(ScenarioError::PostgresOutageWriterRecoveryFailed {
            client: writer.name.clone(),
            path: POSTGRES_OUTAGE_FILE_NAME.to_string(),
            detail: format!(
                "recovered payload differs: expected {} bytes with expected checksum {}, got {} bytes with recovered checksum {}",
                POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len(),
                expected_checksum,
                writer_payload.len(),
                recovered_checksum
            ),
        });
    };
    let writer_recovery_time_ms = elapsed_millis_nonzero(writer_recovery_started);
    let (peer_observation_time_ms, final_validator) =
        if writer_recovery_status == PostgresOutageWriterRecoveryStatus::Recovered {
            let mounts = clients
                .iter()
                .take(POSTGRES_OUTAGE_CLIENT_NAMES.len())
                .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
                .collect::<Vec<_>>();
            let peer_observation_started = Instant::now();
            let convergence = wait_for_convergence(
                &mounts,
                context.scenario_dir.join("validation"),
                convergence_timeout,
                poll_interval,
            )
            .map_err(|error| ScenarioError::PostgresOutagePeerConvergenceFailed {
                detail: error.to_string(),
            })?;
            (
                elapsed_millis_nonzero(peer_observation_started),
                convergence.comparison,
            )
        } else {
            (
                0,
                TreeComparison {
                    converged: true,
                    reference_client: Some(writer.name.clone()),
                    compared_clients: vec![writer.name.clone()],
                    differences: Vec::new(),
                },
            )
        };
    let recovery_time_ms = elapsed_millis_nonzero(recovery_started);

    let summary = PostgresOutageRecoverySummary {
        bootstrap,
        outage_file: POSTGRES_OUTAGE_FILE_NAME.to_string(),
        outage_duration_ms,
        operations_accepted_during_outage: accepted_operations,
        writer_recovery_client: writer.name.clone(),
        writer_recovery_status,
        writer_recovery_time_ms,
        writer_recovery_bytes: writer_payload.len(),
        expected_writer_recovery_checksum_sha256: expected_checksum,
        writer_recovery_checksum_sha256: recovered_checksum,
        peer_observation_time_ms,
        recovery_time_ms,
        final_validator,
    };
    fs::write(
        context
            .scenario_dir
            .join("postgres-outage-recovery-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

fn perform_postgres_writer_outage_operation(
    context: &ScenarioContext,
    client: &ClientSpec,
    relative: &str,
    payload: &[u8],
) -> Result<usize, ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("outage-operations.ndjson"))?;
    let mut operations_log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    let write_result = fs::write(client.mount_point.join(relative), payload);
    let error = write_result.err().map(|err| err.to_string());
    let operation_accepted = error.is_none();
    let record = OutageOperationRecord {
        client: client.name.clone(),
        path: relative.to_string(),
        bytes: payload.len(),
        accepted: operation_accepted,
        error,
    };
    serde_json::to_writer(&mut log, &record)?;
    log.write_all(b"\n")?;
    serde_json::to_writer(&mut operations_log, &record)?;
    operations_log.write_all(b"\n")?;
    if operation_accepted {
        Ok(1)
    } else {
        Err(ScenarioError::RejectedOutageOperations {
            rejected: 1,
            total: 1,
        })
    }
}

trait RustfsOutageDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    // Test drivers use this hook to make the healthy pre-outage payload visible
    // before the normal mounted-tree convergence gate.
    fn after_initial_write(
        &mut self,
        clients: &[ClientSpec],
        relative_path: &str,
    ) -> Result<(), ScenarioError>;
    fn stop_rustfs(&mut self) -> Result<(), ScenarioError>;
    fn assert_peer_not_visible(
        &mut self,
        peer: &ClientSpec,
        relative_path: &str,
        forbidden_checksum: &str,
    ) -> Result<Option<String>, ScenarioError>;
    fn start_rustfs(&mut self) -> Result<(), ScenarioError>;
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealRustfsOutageDriver<E> {
    controller: MultiClientController<SystemClientProcessExecutor>,
    compose_control: ComposeServiceControl,
    executor: E,
}

impl<E: CommandExecutor> RustfsOutageDriver for RealRustfsOutageDriver<E> {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.controller.bootstrap_volume(options)?)
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.mount_all()?)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.controller.clients().into_iter().cloned().collect()
    }

    fn after_initial_write(
        &mut self,
        _clients: &[ClientSpec],
        _relative_path: &str,
    ) -> Result<(), ScenarioError> {
        Ok(())
    }

    fn stop_rustfs(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .stop_service(RUSTFS_SERVICE_NAME, &self.executor)?)
    }

    fn assert_peer_not_visible(
        &mut self,
        peer: &ClientSpec,
        relative_path: &str,
        forbidden_checksum: &str,
    ) -> Result<Option<String>, ScenarioError> {
        assert_peer_not_visible(peer, relative_path, forbidden_checksum)
    }

    fn start_rustfs(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .start_service(RUSTFS_SERVICE_NAME, &self.executor)?)
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.unmount_all()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RustfsPeerVisibility {
    client: String,
    visible_checksum_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClientPayloadChecksum {
    client: String,
    checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RustfsOutageRecoverySummary {
    bootstrap: VolumeBootstrapSummary,
    outage_file: String,
    outage_duration_ms: u128,
    upload_recovery_time_ms: u128,
    local_read_checksum_sha256: String,
    peer_visible_during_outage: Option<String>,
    peer_visibility_during_outage: Vec<RustfsPeerVisibility>,
    final_checksum_sha256: String,
    final_client_checksums: Vec<ClientPayloadChecksum>,
    final_validator: TreeComparison,
}

fn run_rustfs_outage_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn RustfsOutageDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<RustfsOutageRecoverySummary, ScenarioError> {
    let result = run_rustfs_outage_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::RustfsOutageUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_rustfs_outage_body(
    context: &ScenarioContext,
    driver: &mut dyn RustfsOutageDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<RustfsOutageRecoverySummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: RUSTFS_OUTAGE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;

    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < RUSTFS_OUTAGE_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientOutageClients {
            expected: RUSTFS_OUTAGE_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }

    let relative_path = RUSTFS_OUTAGE_FILE_NAME;
    let source_file = clients[0].mount_point.join(relative_path);
    fs::write(&source_file, RUSTFS_OUTAGE_INITIAL_PAYLOAD)?;
    driver.after_initial_write(&clients, relative_path)?;
    let initial_mounts = clients
        .iter()
        .take(RUSTFS_OUTAGE_CLIENT_NAMES.len())
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &initial_mounts,
        context.scenario_dir.join("healthy-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let outage_started = Instant::now();
    driver.stop_rustfs()?;
    fs::write(&source_file, RUSTFS_OUTAGE_RECOVERED_PAYLOAD)?;
    let local_read_checksum_sha256 = bytes_sha256(&fs::read(&source_file)?);
    let outage_duration_ms = outage_started.elapsed().as_millis();
    let mut peer_visibility_during_outage = Vec::new();
    for peer in clients
        .iter()
        .skip(1)
        .take(RUSTFS_OUTAGE_CLIENT_NAMES.len() - 1)
    {
        let visible_checksum_sha256 =
            driver.assert_peer_not_visible(peer, relative_path, &local_read_checksum_sha256)?;
        peer_visibility_during_outage.push(RustfsPeerVisibility {
            client: peer.name.clone(),
            visible_checksum_sha256,
        });
    }
    let peer_visible_during_outage = peer_visibility_during_outage
        .first()
        .and_then(|probe| probe.visible_checksum_sha256.clone());

    let recovery_started = Instant::now();
    driver.start_rustfs()?;
    let convergence = wait_for_convergence(
        &initial_mounts,
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let upload_recovery_time_ms = recovery_started.elapsed().as_millis();
    let mut final_client_checksums = Vec::new();
    for client in clients.iter().take(RUSTFS_OUTAGE_CLIENT_NAMES.len()) {
        let checksum = bytes_sha256(&fs::read(client.mount_point.join(relative_path))?);
        if checksum != local_read_checksum_sha256 {
            return Err(ScenarioError::Io(std::io::Error::other(format!(
                "recovery convergence failure: {} read checksum {} but expected {} for {relative_path}",
                client.name, checksum, local_read_checksum_sha256
            ))));
        }
        final_client_checksums.push(ClientPayloadChecksum {
            client: client.name.clone(),
            checksum_sha256: checksum,
        });
    }
    let final_checksum_sha256 = final_client_checksums
        .first()
        .map(|entry| entry.checksum_sha256.clone())
        .unwrap_or_else(|| local_read_checksum_sha256.clone());

    let summary = RustfsOutageRecoverySummary {
        bootstrap,
        outage_file: relative_path.to_string(),
        outage_duration_ms,
        upload_recovery_time_ms,
        local_read_checksum_sha256,
        peer_visible_during_outage,
        peer_visibility_during_outage,
        final_checksum_sha256,
        final_client_checksums,
        final_validator: convergence.comparison,
    };
    write_rustfs_outage_operation(context, &summary)?;
    fs::write(
        context
            .scenario_dir
            .join("rustfs-outage-recovery-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

fn assert_peer_not_visible(
    peer: &ClientSpec,
    relative_path: &str,
    forbidden_checksum: &str,
) -> Result<Option<String>, ScenarioError> {
    let Some(checksum) = read_peer_checksum(peer, relative_path)? else {
        return Ok(None);
    };
    if checksum == forbidden_checksum {
        Err(ScenarioError::PeerObservedOutageWrite { checksum })
    } else {
        Ok(Some(checksum))
    }
}

fn read_peer_checksum(
    peer: &ClientSpec,
    relative_path: &str,
) -> Result<Option<String>, ScenarioError> {
    match fs::read(peer.mount_point.join(relative_path)) {
        Ok(payload) => Ok(Some(bytes_sha256(&payload))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn write_rustfs_outage_operation(
    context: &ScenarioContext,
    summary: &RustfsOutageRecoverySummary,
) -> Result<(), ScenarioError> {
    let record = json!({
        "scenario": RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
        "client": RUSTFS_OUTAGE_CLIENT_NAMES[0],
        "path": summary.outage_file,
        "bytes": RUSTFS_OUTAGE_RECOVERED_PAYLOAD.len(),
        "accepted": true,
        "local_read_checksum_sha256": summary.local_read_checksum_sha256,
        "peer_visible_during_outage": summary.peer_visible_during_outage,
        "peer_visibility_during_outage": summary.peer_visibility_during_outage,
        "final_checksum_sha256": summary.final_checksum_sha256,
        "final_client_checksums": summary.final_client_checksums,
    });
    for artifact in ["rustfs-outage-operations.ndjson", "operations.ndjson"] {
        let mut log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(context.scenario_dir.join(artifact))?;
        serde_json::to_writer(&mut log, &record)?;
        log.write_all(b"\n")?;
    }
    Ok(())
}

trait CrashRestartDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn remount(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealCrashRestartDriver(MultiClientController<SystemClientProcessExecutor>);

impl CrashRestartDriver for RealCrashRestartDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.0.bootstrap_volume(options)?)
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.mount_all()?)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.0.clients().into_iter().cloned().collect()
    }

    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.0.force_kill(client)?)
    }

    fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.0.remount(client)?)
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.unmount_all()?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CrashRestartMode {
    AfterLocalDurabilityBoundary,
    BeforeRemotePublicationCompletes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CrashRestartModeSummary {
    mode: CrashRestartMode,
    restarted_client: String,
    restart_client_root: String,
    relative_path: String,
    bytes: usize,
    local_read_checksum_sha256: Option<String>,
    peer_visible_before_convergence: Option<String>,
    final_checksum_sha256: Option<String>,
    restart_elapsed_ms: u128,
    convergence_elapsed_ms: u128,
    intended_checksum_sha256: String,
    recovery_outcome: CrashRestartRecoveryOutcome,
    recovery_detail: Option<String>,
    final_validator: TreeComparison,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CrashRestartRecoveryOutcome {
    RecoverableWriterState,
    UnrecoverableLoss,
}

enum CrashRestartRecoveredPayload {
    Recovered(Vec<u8>),
    UnrecoverableLoss { detail: String },
}

enum CrashRestartOutcomeDraft {
    Recovered {
        local_read_payload: Vec<u8>,
        local_read_checksum_sha256: String,
        peer_visible_before_convergence: Option<String>,
    },
    UnrecoverableLoss {
        detail: String,
        peer_visible_before_convergence: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CrashRestartRecoverySummary {
    bootstrap: VolumeBootstrapSummary,
    modes: Vec<CrashRestartModeSummary>,
}

fn run_crash_restart_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn CrashRestartDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<CrashRestartRecoverySummary, ScenarioError> {
    let result = run_crash_restart_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::CrashRestartUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_crash_restart_body(
    context: &ScenarioContext,
    driver: &mut dyn CrashRestartDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<CrashRestartRecoverySummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: CRASH_RESTART_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;

    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < CRASH_RESTART_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientCrashRestartClients {
            expected: CRASH_RESTART_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }

    let modes = vec![
        run_crash_restart_mode(
            context,
            driver,
            &clients,
            CrashRestartMode::AfterLocalDurabilityBoundary,
            CRASH_RESTART_LOCAL_DURABLE_FILE_NAME,
            b"accepted local durable payload before abrupt crash\n",
            convergence_timeout,
            poll_interval,
        )?,
        run_crash_restart_mode(
            context,
            driver,
            &clients,
            CrashRestartMode::BeforeRemotePublicationCompletes,
            CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
            b"accepted local payload killed before remote publication completes\n",
            convergence_timeout,
            poll_interval,
        )?,
    ];

    let summary = CrashRestartRecoverySummary { bootstrap, modes };
    fs::write(
        context
            .scenario_dir
            .join("crash-restart-recovery-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn run_crash_restart_mode(
    context: &ScenarioContext,
    driver: &mut dyn CrashRestartDriver,
    clients: &[ClientSpec],
    mode: CrashRestartMode,
    relative_path: &str,
    payload: &[u8],
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<CrashRestartModeSummary, ScenarioError> {
    let crashed = &clients[0];
    write_local_durable_payload(crashed.mount_point.join(relative_path), payload)?;
    let restart_started = Instant::now();
    driver.force_kill(&crashed.name)?;
    driver.remount(&crashed.name)?;
    let recovered_payload = read_crash_restart_recovered_payload(crashed, relative_path)?;
    let intended_checksum_sha256 = bytes_sha256(payload);
    let restart_elapsed_ms = restart_started.elapsed().as_millis();

    let convergence_started = Instant::now();
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let validation_dir = context
        .scenario_dir
        .join(format!("validation-{}", crash_restart_mode_slug(mode)));

    let outcome = match recovered_payload {
        CrashRestartRecoveredPayload::Recovered(local_read_payload) => {
            let local_read_checksum_sha256 = bytes_sha256(&local_read_payload);
            let peer_visible_before_convergence =
                if mode == CrashRestartMode::BeforeRemotePublicationCompletes {
                    read_peer_checksum(&clients[1], relative_path)?
                        .filter(|checksum| checksum == &local_read_checksum_sha256)
                } else {
                    None
                };

            CrashRestartOutcomeDraft::Recovered {
                local_read_payload,
                local_read_checksum_sha256,
                peer_visible_before_convergence,
            }
        }
        CrashRestartRecoveredPayload::UnrecoverableLoss { detail } => {
            let peer_visible_before_convergence =
                if mode == CrashRestartMode::BeforeRemotePublicationCompletes {
                    read_peer_checksum(&clients[1], relative_path)?
                } else {
                    None
                };
            CrashRestartOutcomeDraft::UnrecoverableLoss {
                detail,
                peer_visible_before_convergence,
            }
        }
    };

    let convergence =
        wait_for_convergence(&mounts, validation_dir, convergence_timeout, poll_interval)
            .map_err(|error| crash_restart_convergence_error(relative_path, error))?;
    let convergence_elapsed_ms = convergence_started.elapsed().as_millis();

    let summary = match outcome {
        CrashRestartOutcomeDraft::Recovered {
            local_read_payload,
            local_read_checksum_sha256,
            peer_visible_before_convergence,
        } => {
            let final_payload = fs::read(clients[1].mount_point.join(relative_path))?;
            let final_checksum_sha256 = bytes_sha256(&final_payload);
            if final_checksum_sha256 != local_read_checksum_sha256 {
                return Err(ScenarioError::CrashRestartCorruptAdvertisedFile {
                    client: clients[1].name.clone(),
                    path: relative_path.to_string(),
                    expected_checksum: local_read_checksum_sha256,
                    actual_checksum: final_checksum_sha256,
                });
            }

            CrashRestartModeSummary {
                mode,
                restarted_client: crashed.name.clone(),
                restart_client_root: crashed.client_root.display().to_string(),
                relative_path: relative_path.to_string(),
                bytes: local_read_payload.len(),
                local_read_checksum_sha256: Some(local_read_checksum_sha256),
                peer_visible_before_convergence,
                final_checksum_sha256: Some(final_checksum_sha256),
                restart_elapsed_ms,
                convergence_elapsed_ms,
                intended_checksum_sha256,
                recovery_outcome: CrashRestartRecoveryOutcome::RecoverableWriterState,
                recovery_detail: None,
                final_validator: convergence.comparison,
            }
        }
        CrashRestartOutcomeDraft::UnrecoverableLoss {
            detail,
            peer_visible_before_convergence,
        } => {
            if let Some(final_checksum_sha256) = read_peer_checksum(&clients[1], relative_path)? {
                return Err(ScenarioError::CrashRestartCorruptAdvertisedFile {
                    client: clients[1].name.clone(),
                    path: relative_path.to_string(),
                    expected_checksum: "unrecoverable_loss".to_string(),
                    actual_checksum: final_checksum_sha256,
                });
            }

            CrashRestartModeSummary {
                mode,
                restarted_client: crashed.name.clone(),
                restart_client_root: crashed.client_root.display().to_string(),
                relative_path: relative_path.to_string(),
                bytes: 0,
                local_read_checksum_sha256: None,
                peer_visible_before_convergence,
                final_checksum_sha256: None,
                restart_elapsed_ms,
                convergence_elapsed_ms,
                intended_checksum_sha256,
                recovery_outcome: CrashRestartRecoveryOutcome::UnrecoverableLoss,
                recovery_detail: Some(detail),
                final_validator: convergence.comparison,
            }
        }
    };
    write_crash_restart_operation(context, &summary)?;
    Ok(summary)
}

fn read_crash_restart_recovered_payload(
    client: &ClientSpec,
    relative_path: &str,
) -> Result<CrashRestartRecoveredPayload, ScenarioError> {
    let path = client.mount_point.join(relative_path);
    match fs::read(&path) {
        Ok(payload) => Ok(CrashRestartRecoveredPayload::Recovered(payload)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(CrashRestartRecoveredPayload::UnrecoverableLoss {
                detail: format!(
                    "{} could not read recovered bytes for {relative_path} after restart",
                    client.name
                ),
            })
        }
        Err(error) => Err(ScenarioError::Io(error)),
    }
}

fn crash_restart_convergence_error(relative_path: &str, error: MountedTreeError) -> ScenarioError {
    match error {
        MountedTreeError::ConvergenceTimedOut { .. } => {
            ScenarioError::CrashRestartPeerConvergenceFailed {
                path: relative_path.to_string(),
                detail: error.to_string(),
            }
        }
        error => ScenarioError::CrashRestartPublicationFailed {
            path: relative_path.to_string(),
            detail: error.to_string(),
        },
    }
}

fn write_crash_restart_operation(
    context: &ScenarioContext,
    summary: &CrashRestartModeSummary,
) -> Result<(), ScenarioError> {
    let record = json!({
        "scenario": MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
        "mode": summary.mode,
        "client": summary.restarted_client,
        "path": summary.relative_path,
        "bytes": summary.bytes,
        "accepted": true,
        "recovery_outcome": summary.recovery_outcome,
        "recovery_detail": summary.recovery_detail,
        "intended_checksum_sha256": summary.intended_checksum_sha256,
        "local_read_checksum_sha256": summary.local_read_checksum_sha256,
        "peer_visible_before_convergence": summary.peer_visible_before_convergence,
        "final_checksum_sha256": summary.final_checksum_sha256,
    });
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    serde_json::to_writer(&mut log, &record)?;
    log.write_all(b"\n")?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NamespaceCollisionKind {
    RenameVsRename,
    RenameVsUnlink,
    UnlinkVsAppend,
    OverwriteVsRead,
    MkdirRmdirVsCreate,
    ReaddirStatDuringMutation,
}

impl NamespaceCollisionKind {
    fn name(self) -> &'static str {
        match self {
            Self::RenameVsRename => COLLISION_RENAME_VS_RENAME,
            Self::RenameVsUnlink => COLLISION_RENAME_VS_UNLINK,
            Self::UnlinkVsAppend => COLLISION_UNLINK_VS_APPEND,
            Self::OverwriteVsRead => COLLISION_OVERWRITE_VS_READ,
            Self::MkdirRmdirVsCreate => COLLISION_MKDIR_RMDIR_VS_CREATE,
            Self::ReaddirStatDuringMutation => COLLISION_READDIR_STAT_DURING_MUTATION,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::RenameVsRename => "mounted namespace collision: rename vs rename",
            Self::RenameVsUnlink => "mounted namespace collision: rename vs unlink",
            Self::UnlinkVsAppend => "mounted namespace collision: unlink vs append/write",
            Self::OverwriteVsRead => "mounted namespace collision: overwrite vs repeated reads",
            Self::MkdirRmdirVsCreate => "mounted namespace collision: mkdir/rmdir vs create",
            Self::ReaddirStatDuringMutation => {
                "mounted namespace collision: repeated readdir/stat during mutations"
            }
        }
    }

    fn initial_paths(self) -> Vec<(&'static str, &'static [u8])> {
        match self {
            Self::RenameVsRename | Self::RenameVsUnlink => {
                vec![("shared/file.txt", b"initial namespace payload\n")]
            }
            Self::UnlinkVsAppend | Self::OverwriteVsRead => {
                vec![("shared/data.txt", COLLISION_OLD_COMMITTED_PAYLOAD)]
            }
            Self::MkdirRmdirVsCreate => Vec::new(),
            Self::ReaddirStatDuringMutation => {
                vec![("scan/a.txt", b"a\n"), ("scan/b.txt", b"b\n")]
            }
        }
    }

    fn expected_absent_paths(self) -> &'static [&'static str] {
        match self {
            Self::RenameVsRename => &["shared/file.txt"],
            Self::RenameVsUnlink => &["shared/file.txt"],
            Self::UnlinkVsAppend => &[],
            Self::OverwriteVsRead => &[],
            Self::MkdirRmdirVsCreate => &[],
            Self::ReaddirStatDuringMutation => &["scan/a.txt"],
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct NamespaceCollisionScenario {
    kind: NamespaceCollisionKind,
}

impl NamespaceCollisionScenario {
    fn new(kind: NamespaceCollisionKind) -> Self {
        Self { kind }
    }
}

impl Scenario for NamespaceCollisionScenario {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: self.kind.name(),
            description: self.kind.description(),
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut driver = RealNamespaceCollisionDriver(MultiClientController::new(
                config.clone(),
                SystemClientProcessExecutor,
            ));
            run_namespace_collision_with_driver(
                context,
                self.kind,
                &mut driver,
                COLLISION_CONVERGENCE_TIMEOUT,
                COLLISION_CONVERGENCE_POLL_INTERVAL,
            )?;
        } else {
            write_namespace_collision_plan_artifacts(context, self.kind)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("namespace-collision-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: NamespaceCollisionSummary = serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": self.kind.name(),
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged
                && summary.namespace_violations.is_empty() { "passed" } else { "failed" },
            "final_validator": summary.final_validator,
            "namespace_violations": summary.namespace_violations,
            "operation_log": summary.operation_log,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{}/namespace-collision-summary.json",
                self.kind.name()
            ),
            format!("artifacts/scenarios/{}/validation.json", self.kind.name()),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

trait NamespaceCollisionDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn after_conflicting_operations(
        &mut self,
        _kind: NamespaceCollisionKind,
        _clients: &[ClientSpec],
    ) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealNamespaceCollisionDriver(MultiClientController<SystemClientProcessExecutor>);

impl NamespaceCollisionDriver for RealNamespaceCollisionDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.0.bootstrap_volume(options)?)
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.mount_all()?)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.0.clients().into_iter().cloned().collect()
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.unmount_all()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NamespaceCollisionOperation {
    order: usize,
    client: String,
    operation: String,
    path: String,
    result: String,
    timestamp_ms: u64,
    error: Option<String>,
}

type NamespaceCollisionAction = Box<dyn FnOnce(&ClientSpec) -> io::Result<()> + Send + 'static>;

struct NamespaceCollisionTask {
    order: usize,
    client: ClientSpec,
    operation: &'static str,
    path: &'static str,
    action: NamespaceCollisionAction,
}

impl NamespaceCollisionTask {
    fn new(
        order: usize,
        client: ClientSpec,
        operation: &'static str,
        path: &'static str,
        action: impl FnOnce(&ClientSpec) -> io::Result<()> + Send + 'static,
    ) -> Self {
        Self {
            order,
            client,
            operation,
            path,
            action: Box::new(action),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NamespaceCollisionSummary {
    scenario: String,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: usize,
    operations: Vec<NamespaceCollisionOperation>,
    operation_log: String,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
}

fn run_namespace_collision_with_driver(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    driver: &mut dyn NamespaceCollisionDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    let result =
        run_namespace_collision_body(context, kind, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_namespace_collision_body(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    driver: &mut dyn NamespaceCollisionDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: COLLISION_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < COLLISION_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientCollisionClients {
            expected: COLLISION_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }
    let clients = clients
        .into_iter()
        .take(COLLISION_CLIENT_NAMES.len())
        .collect::<Vec<_>>();

    establish_collision_initial_tree(kind, &clients[0])?;
    driver.after_conflicting_operations(kind, &clients)?;
    let initial_mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &initial_mounts,
        context.scenario_dir.join("initial-validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let operations = perform_namespace_collision_operations(context, kind, &clients)?;
    driver.after_conflicting_operations(kind, &clients)?;
    let summary = finish_namespace_collision(
        context,
        kind,
        Some(bootstrap),
        clients,
        operations,
        convergence_timeout,
        poll_interval,
    )?;
    Ok(summary)
}

fn write_namespace_collision_plan_artifacts(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let operations = planned_namespace_collision_operations(kind, started);
    write_namespace_collision_operations(context, &operations)?;
    let summary = NamespaceCollisionSummary {
        scenario: kind.name().to_string(),
        bootstrap: None,
        clients: COLLISION_CLIENT_NAMES.len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: TreeComparison {
            converged: true,
            reference_client: Some(COLLISION_CLIENT_NAMES[0].to_string()),
            compared_clients: COLLISION_CLIENT_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
    };
    write_namespace_collision_summary(context, &summary)?;
    Ok(summary)
}

fn establish_collision_initial_tree(
    kind: NamespaceCollisionKind,
    client: &ClientSpec,
) -> Result<(), ScenarioError> {
    if kind == NamespaceCollisionKind::MkdirRmdirVsCreate {
        fs::create_dir_all(client.mount_point.join("race"))?;
    }
    for (relative_path, payload) in kind.initial_paths() {
        let path = client.mount_point.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, payload)?;
    }
    Ok(())
}

fn perform_namespace_collision_operations(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    clients: &[ClientSpec],
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let operations = match kind {
        NamespaceCollisionKind::RenameVsRename => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(
                    1,
                    clients[0].clone(),
                    "rename",
                    "shared/file.txt -> shared/renamed-a.txt",
                    |client| {
                        fs::rename(
                            client.mount_point.join("shared/file.txt"),
                            client.mount_point.join("shared/renamed-a.txt"),
                        )
                    },
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[1].clone(),
                    "rename",
                    "shared/file.txt -> shared/renamed-b.txt",
                    |client| {
                        fs::rename(
                            client.mount_point.join("shared/file.txt"),
                            client.mount_point.join("shared/renamed-b.txt"),
                        )
                    },
                ),
            ],
        ),
        NamespaceCollisionKind::RenameVsUnlink => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(
                    1,
                    clients[0].clone(),
                    "rename",
                    "shared/file.txt -> shared/renamed.txt",
                    |client| {
                        fs::rename(
                            client.mount_point.join("shared/file.txt"),
                            client.mount_point.join("shared/renamed.txt"),
                        )
                    },
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[1].clone(),
                    "unlink",
                    "shared/file.txt",
                    |client| fs::remove_file(client.mount_point.join("shared/file.txt")),
                ),
            ],
        ),
        NamespaceCollisionKind::UnlinkVsAppend => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(
                    1,
                    clients[0].clone(),
                    "unlink",
                    "shared/data.txt",
                    |client| fs::remove_file(client.mount_point.join("shared/data.txt")),
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[1].clone(),
                    "append",
                    "shared/data.txt",
                    |client| {
                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(client.mount_point.join("shared/data.txt"))?;
                        file.write_all(b"appended payload\n")
                    },
                ),
            ],
        ),
        NamespaceCollisionKind::OverwriteVsRead => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(
                    1,
                    clients[1].clone(),
                    "read_loop",
                    "shared/data.txt",
                    |client| repeated_committed_read(client, "shared/data.txt"),
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[0].clone(),
                    "overwrite",
                    "shared/data.txt",
                    |client| {
                        fs::write(
                            client.mount_point.join("shared/data.txt"),
                            COLLISION_NEW_COMMITTED_PAYLOAD,
                        )
                    },
                ),
                NamespaceCollisionTask::new(
                    3,
                    clients[2].clone(),
                    "read_loop",
                    "shared/data.txt",
                    |client| repeated_committed_read(client, "shared/data.txt"),
                ),
            ],
        ),
        NamespaceCollisionKind::MkdirRmdirVsCreate => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(
                    1,
                    clients[0].clone(),
                    "mkdir",
                    "race/subdir",
                    |client| fs::create_dir_all(client.mount_point.join("race/subdir")),
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[1].clone(),
                    "create",
                    "race/child.txt",
                    |client| fs::write(client.mount_point.join("race/child.txt"), b"child\n"),
                ),
                NamespaceCollisionTask::new(3, clients[2].clone(), "rmdir", "race", |client| {
                    fs::remove_dir(client.mount_point.join("race"))
                }),
            ],
        ),
        NamespaceCollisionKind::ReaddirStatDuringMutation => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(1, clients[1].clone(), "readdir", "scan", |client| {
                    fs::read_dir(client.mount_point.join("scan")).map(|_| ())
                }),
                NamespaceCollisionTask::new(
                    2,
                    clients[0].clone(),
                    "rename",
                    "scan/a.txt -> scan/c.txt",
                    |client| {
                        fs::rename(
                            client.mount_point.join("scan/a.txt"),
                            client.mount_point.join("scan/c.txt"),
                        )
                    },
                ),
                NamespaceCollisionTask::new(
                    3,
                    clients[2].clone(),
                    "stat",
                    "scan/b.txt",
                    |client| fs::metadata(client.mount_point.join("scan/b.txt")).map(|_| ()),
                ),
            ],
        ),
    };
    write_namespace_collision_operations(context, &operations)?;
    Ok(operations)
}

fn run_concurrent_namespace_operations(
    started: Instant,
    tasks: Vec<NamespaceCollisionTask>,
) -> Vec<NamespaceCollisionOperation> {
    let barrier = Arc::new(Barrier::new(tasks.len()));
    let mut handles = Vec::with_capacity(tasks.len());
    for task in tasks {
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            record_fs_operation(
                started,
                task.order,
                &task.client,
                task.operation,
                task.path,
                || (task.action)(&task.client),
            )
        }));
    }

    let mut operations = handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .expect("namespace collision worker thread should not panic")
        })
        .collect::<Vec<_>>();
    operations.sort_by_key(|operation| operation.order);
    operations
}

fn repeated_committed_read(client: &ClientSpec, relative_path: &str) -> io::Result<()> {
    for _ in 0..OVERWRITE_READ_ATTEMPTS {
        let payload = fs::read(client.mount_point.join(relative_path))?;
        if payload != COLLISION_OLD_COMMITTED_PAYLOAD && payload != COLLISION_NEW_COMMITTED_PAYLOAD
        {
            return Err(io::Error::other(format!(
                "read observed partial or unexpected payload for {relative_path}"
            )));
        }
        thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn planned_namespace_collision_operations(
    kind: NamespaceCollisionKind,
    started: Instant,
) -> Vec<NamespaceCollisionOperation> {
    let spec = match kind {
        NamespaceCollisionKind::RenameVsRename => vec![
            (
                "client-a",
                "rename",
                "shared/file.txt -> shared/renamed-a.txt",
            ),
            (
                "client-b",
                "rename",
                "shared/file.txt -> shared/renamed-b.txt",
            ),
        ],
        NamespaceCollisionKind::RenameVsUnlink => vec![
            (
                "client-a",
                "rename",
                "shared/file.txt -> shared/renamed.txt",
            ),
            ("client-b", "unlink", "shared/file.txt"),
        ],
        NamespaceCollisionKind::UnlinkVsAppend => vec![
            ("client-a", "unlink", "shared/data.txt"),
            ("client-b", "append", "shared/data.txt"),
        ],
        NamespaceCollisionKind::OverwriteVsRead => vec![
            ("client-b", "read_loop", "shared/data.txt"),
            ("client-a", "overwrite", "shared/data.txt"),
            ("client-c", "read_loop", "shared/data.txt"),
        ],
        NamespaceCollisionKind::MkdirRmdirVsCreate => vec![
            ("client-a", "mkdir", "race/subdir"),
            ("client-b", "create", "race/child.txt"),
            ("client-c", "rmdir", "race"),
        ],
        NamespaceCollisionKind::ReaddirStatDuringMutation => vec![
            ("client-b", "readdir", "scan"),
            ("client-a", "rename", "scan/a.txt -> scan/c.txt"),
            ("client-c", "stat", "scan/b.txt"),
        ],
    };
    spec.into_iter()
        .enumerate()
        .map(
            |(index, (client, operation, path))| NamespaceCollisionOperation {
                order: index + 1,
                client: client.to_string(),
                operation: operation.to_string(),
                path: path.to_string(),
                result: "planned".to_string(),
                timestamp_ms: elapsed_millis_u64(started),
                error: None,
            },
        )
        .collect()
}

fn record_fs_operation(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    f: impl FnOnce() -> std::io::Result<()>,
) -> NamespaceCollisionOperation {
    match f() {
        Ok(()) => NamespaceCollisionOperation {
            order,
            client: client.name.clone(),
            operation: operation.to_string(),
            path: path.to_string(),
            result: "accepted".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: None,
        },
        Err(error) => NamespaceCollisionOperation {
            order,
            client: client.name.clone(),
            operation: operation.to_string(),
            path: path.to_string(),
            result: "rejected".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: Some(error.to_string()),
        },
    }
}

fn elapsed_millis_u64(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn elapsed_millis_nonzero(started: Instant) -> u128 {
    started.elapsed().as_millis().max(1)
}

fn finish_namespace_collision(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    operations: Vec<NamespaceCollisionOperation>,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<NamespaceCollisionSummary, ScenarioError> {
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let mut namespace_violations = Vec::new();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(
            snapshot,
            kind.expected_absent_paths(),
        ));
    }
    if !namespace_violations.is_empty() {
        return Err(ScenarioError::NamespaceInvariantViolations {
            violations: namespace_violations,
        });
    }
    let summary = NamespaceCollisionSummary {
        scenario: kind.name().to_string(),
        bootstrap,
        clients: clients.len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: convergence.comparison,
        namespace_violations,
    };
    write_namespace_collision_summary(context, &summary)?;
    Ok(summary)
}

fn write_namespace_collision_operations(
    context: &ScenarioContext,
    operations: &[NamespaceCollisionOperation],
) -> Result<(), ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(false)
        .truncate(true)
        .write(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    for operation in operations {
        serde_json::to_writer(&mut log, operation)?;
        log.write_all(b"\n")?;
    }
    Ok(())
}

fn write_namespace_collision_summary(
    context: &ScenarioContext,
    summary: &NamespaceCollisionSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("namespace-collision-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

fn write_local_durable_payload(path: PathBuf, payload: &[u8]) -> Result<(), ScenarioError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(payload)?;
    file.sync_all()?;
    Ok(())
}

fn crash_restart_mode_slug(mode: CrashRestartMode) -> &'static str {
    match mode {
        CrashRestartMode::AfterLocalDurabilityBoundary => "after-local-durability-boundary",
        CrashRestartMode::BeforeRemotePublicationCompletes => "before-remote-publication-completes",
    }
}

fn bytes_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
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
    use std::cell::{Cell, RefCell};
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
        let mount_roots = SMOKE_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| temp.path().join("mounts").join(name))
            .collect::<Vec<_>>();
        thread::spawn(move || {
            for _ in 0..200 {
                for source_mount in &mount_roots {
                    let entries = std::fs::read_dir(source_mount)
                        .into_iter()
                        .flatten()
                        .filter_map(Result::ok)
                        .filter(|entry| entry.path().is_file())
                        .collect::<Vec<_>>();
                    if entries.is_empty() {
                        continue;
                    }
                    for entry in entries {
                        let file_name = entry.file_name();
                        if let Ok(payload) = std::fs::read(entry.path()) {
                            if payload.is_empty() {
                                continue;
                            }
                            for replica in &mount_roots {
                                let _ = std::fs::write(replica.join(&file_name), &payload);
                            }
                        }
                    }
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
                client.local_metadata_db =
                    client.local_metadata_dir.join(format!("{volume_name}.db"));
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
            if self.lose_writer_payload_on_remount {
                std::fs::write(writer.mount_point.join(POSTGRES_OUTAGE_FILE_NAME), [])?;
            }
            if self.corrupt_writer_payload_on_remount {
                std::fs::write(
                    writer.mount_point.join(POSTGRES_OUTAGE_FILE_NAME),
                    vec![b'x'; POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len()],
                )?;
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
        let source_mount = temp
            .path()
            .join("mounts")
            .join(CRASH_RESTART_CLIENT_NAMES[0]);
        let peer_mount = temp
            .path()
            .join("mounts")
            .join(CRASH_RESTART_CLIENT_NAMES[1]);
        thread::spawn(move || {
            let files = [
                CRASH_RESTART_LOCAL_DURABLE_FILE_NAME,
                CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
            ];
            for file in files {
                let source = source_mount.join(file);
                for _ in 0..100 {
                    if let Ok(payload) = std::fs::read(&source) {
                        thread::sleep(Duration::from_millis(50));
                        std::fs::write(peer_mount.join(file), &payload).unwrap();
                        break;
                    }
                    thread::sleep(Duration::from_millis(5));
                }
            }
        });
        let original_client_root = clients[0].client_root.clone();
        let mut driver = RecordingCrashRestartDriver::new(clients);
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
        let mut driver =
            RecordingCrashRestartDriver::new(clients).publish_current_file_on_remount();
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
    fn crash_restart_reports_corrupt_advertised_file_when_final_peer_differs_from_recovered_writer()
    {
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
        let source_mount = temp.path().join("mounts").join("client-a");
        let peer_mounts = ["client-b", "client-c"]
            .into_iter()
            .map(|name| temp.path().join("mounts").join(name))
            .collect::<Vec<_>>();
        thread::spawn(move || {
            let files = [
                CRASH_RESTART_LOCAL_DURABLE_FILE_NAME,
                CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
            ];
            for file in files {
                let source = source_mount.join(file);
                for _ in 0..100 {
                    if let Ok(payload) = std::fs::read(&source) {
                        thread::sleep(Duration::from_millis(50));
                        for peer in &peer_mounts {
                            std::fs::write(peer.join(file), &payload).unwrap();
                        }
                        break;
                    }
                    thread::sleep(Duration::from_millis(5));
                }
            }
        });
        let mut driver = RecordingCrashRestartDriver::new(clients);
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

        assert!(summary.modes.iter().all(|mode| {
            mode.final_validator.compared_clients
                == client_names
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect::<Vec<_>>()
        }));
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
    fn postgres_outage_allows_empty_writer_recovery_after_unclean_shutdown() {
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
        assert_eq!(summary.final_validator.compared_clients, ["client-a"]);
        assert_eq!(summary.peer_observation_time_ms, 0);
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
        let mut driver = RecordingRustfsOutageDriver::new(clients)
            .with_peer_probe_delay(Duration::from_millis(200));
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
        }
    }
}

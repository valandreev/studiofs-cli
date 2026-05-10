//! Scenario registry and lifecycle hooks for mounted E2E runs.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
#[cfg(not(test))]
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use studiofs_scenario::{
    OperationLogError, OperationLogWriter, OperationOutcome, ScenarioOperation as SharedOperation,
    ScenarioOperationRecord as SharedOperationRecord,
};
use thiserror::Error;

use crate::client::{
    ClientControllerConfig, ClientControllerError, ClientLifecycleState, ClientSpec,
    MultiClientController, SystemClientProcessExecutor, VolumeBootstrapOptions,
    VolumeBootstrapSummary,
};
use crate::infra::{
    CommandExecutor, CommandSpec, ComposeServiceControl, InfraError, POSTGRES_SERVICE_NAME,
    RUSTFS_SERVICE_NAME, SystemCommandExecutor,
};
use crate::mounted_tree::{
    ClientMount, ConvergenceOutcome, MountedTreeError, NamespaceInvariantViolation, TreeComparison,
    validate_namespace_invariants, wait_for_convergence,
};
use crate::report::RunManifest;

/// Constant $name.
pub const BASIC_MULTI_CLIENT_SMOKE: &str = "basic-multi-client-smoke";
/// Constant $name.
pub const POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY: &str = "postgres-outage-multi-client-recovery";
/// Constant $name.
pub const RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY: &str = "rustfs-outage-multi-client-recovery";
/// Constant $name.
pub const MOUNTED_CLIENT_CRASH_RESTART_RECOVERY: &str = "mounted-client-crash-restart-recovery";
/// Constant $name.
pub const MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX: &str =
    "mounted-publication-boundary-crash-matrix";
/// Constant $name.
pub const COLLISION_RENAME_VS_RENAME: &str = "collision-rename-vs-rename";
/// Constant $name.
pub const COLLISION_RENAME_VS_UNLINK: &str = "collision-rename-vs-unlink";
/// Constant $name.
pub const COLLISION_UNLINK_VS_APPEND: &str = "collision-unlink-vs-append";
/// Constant $name.
pub const COLLISION_OVERWRITE_VS_READ: &str = "collision-overwrite-vs-read";
/// Constant $name.
pub const COLLISION_MKDIR_RMDIR_VS_CREATE: &str = "collision-mkdir-rmdir-vs-create";
/// Constant $name.
pub const COLLISION_READDIR_STAT_DURING_MUTATION: &str = "collision-readdir-stat-during-mutation";
/// Constant $name.
pub const MOUNTED_CHAOS_CONVERGENCE: &str = "mounted-chaos-convergence";
/// Constant $name.
pub const MOUNTED_HOTSPOT_CHURN_CONVERGENCE: &str = "mounted-hotspot-churn-convergence";
/// Constant $name.
pub const MOUNTED_FAULT_CHAOS_CONVERGENCE: &str = "mounted-fault-chaos-convergence";
/// Constant $name.
pub const MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE: &str = "mounted-lww-reconnect-chaos-convergence";
/// Constant $name.
pub const MOUNTED_OPEN_HANDLE_CONVERGENCE: &str = "mounted-open-handle-convergence";
/// First eight mounted scenarios used as the baseline gate before deeper chaos layers.
pub const FIRST_EIGHT_MOUNTED_BASELINE: &[&str] = &[
    BASIC_MULTI_CLIENT_SMOKE,
    POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
    RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
    MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
    MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX,
    COLLISION_RENAME_VS_RENAME,
    COLLISION_RENAME_VS_UNLINK,
    COLLISION_UNLINK_VS_APPEND,
];
const SMOKE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const COLLISION_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const CHAOS_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const FAULT_CHAOS_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
const FAULT_CHAOS_WARM_CLIENTS: usize = 3;
const LWW_RECONNECT_CLIENT_NAMES: &[&str] =
    &["client-a", "client-b", "client-c", "client-d", "client-e"];
const LWW_RECONNECT_WARM_CLIENTS: usize = 4;
const LWW_RECONNECT_STALE_WRITER_INDEX: usize = 2;
const LWW_RECONNECT_COLD_OBSERVER_INDEX: usize = 4;
const POSTGRES_OUTAGE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const RUSTFS_OUTAGE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
const CRASH_RESTART_CLIENT_NAMES: &[&str] = &["client-a", "client-b"];
const SMOKE_FILE_NAME: &str = "studiofs-e2e-smoke.txt";
const SMOKE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const SMOKE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OUTAGE_INITIAL_FILE_NAME: &str = "studiofs-e2e-outage-initial.txt";
const OUTAGE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const OUTAGE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const RUSTFS_OUTAGE_GATE_FILE_NAME: &str = "studiofs-e2e-rustfs-outage-gate.txt";
const RUSTFS_OUTAGE_FILE_NAME: &str = "studiofs-e2e-rustfs-outage-recovered.txt";
const CRASH_RESTART_LOCAL_DURABLE_FILE_NAME: &str = "studiofs-e2e-crash-after-local-durability.txt";
const CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME: &str =
    "studiofs-e2e-crash-before-remote-publication.txt";
const CRASH_RESTART_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const CRASH_RESTART_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PUBLICATION_BOUNDARY_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const PUBLICATION_BOUNDARY_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const PUBLICATION_BOUNDARY_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PUBLICATION_BOUNDARY_RELEASE_CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const PUBLICATION_BOUNDARY_RELEASE_CLOSE_DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(25);
const PUBLICATION_BOUNDARY_DIAGNOSTICS_TIMEOUT: Duration = Duration::from_secs(5);
const COLLISION_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const COLLISION_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OVERWRITE_READ_ATTEMPTS: u32 = 8;
const CHAOS_ROUNDS: usize = 12;
const CHAOS_READ_ATTEMPTS: u32 = 4;
const HOTSPOT_CHURN_ROUNDS: usize = 10;
const HOTSPOT_WORKING_SET: usize = 6;
const HOTSPOT_PROBE_ATTEMPTS: u32 = 4;
const HOTSPOT_PROBE_INTERVAL: Duration = Duration::from_millis(5);
const CHAOS_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(90);
const CHAOS_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const CHAOS_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const CHAOS_HELPER_START_TIMEOUT: Duration = Duration::from_secs(60);
const MOUNTED_WRITE_READBACK_TIMEOUT: Duration = Duration::from_secs(5);
const MOUNTED_WRITE_READBACK_POLL_INTERVAL: Duration = Duration::from_millis(25);
#[cfg(all(not(test), not(windows)))]
const CHAOS_HELPER_KILL_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const FAULT_CHAOS_CONFLICT_ROUNDS: usize = 6;
const FAULT_CHAOS_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
const FAULT_CHAOS_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const LWW_RECONNECT_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
const LWW_RECONNECT_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OPEN_HANDLE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const OPEN_HANDLE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const OPEN_HANDLE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OPEN_UNLINK_PATH: &str = "open-handles/unlink-target.txt";

#[cfg(test)]
pub(crate) const TEST_FAILING_VALIDATE: &str = "__test-failing-validate";
#[cfg(test)]
pub(crate) const TEST_FAILING_VALIDATE_AND_CLEANUP: &str = "__test-failing-validate-and-cleanup";
#[cfg(test)]
pub(crate) const TEST_PASSING: &str = "__test-passing";
#[cfg(test)]
pub(crate) const TEST_HANGING_RUN: &str = "__test-hanging-run";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Struct $name.
pub struct ScenarioDefinition {
    /// Field $name.
    pub name: &'static str,
    /// Field $name.
    pub description: &'static str,
    /// Field $name.
    pub requires_studiofs_bin: bool,
    /// Field $name.
    pub requires_mount: bool,
}

#[derive(Debug, Clone)]
/// Struct $name.
pub struct ScenarioContext {
    /// Field $name.
    pub run_id: String,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub sandbox: PathBuf,
    /// Field $name.
    pub scenario_dir: PathBuf,
    /// Field $name.
    pub client_controller_config: Option<ClientControllerConfig>,
}

#[derive(Debug, Error)]
/// Enum $name.
pub enum ScenarioError {
    #[error("scenario I/O failed: {0}")]
    /// Variant $name.
    Io(#[from] std::io::Error),
    #[error("scenario JSON failed: {0}")]
    /// Variant $name.
    Json(#[from] serde_json::Error),
    #[error("scenario client controller failed: {0}")]
    /// Variant $name.
    Client(#[from] ClientControllerError),
    #[error("mounted smoke scenario failed: {primary}; unmount also failed: {cleanup}")]
    /// Variant $name.
    MountedSmokeUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error("PostgreSQL outage scenario failed: {primary}; unmount also failed: {cleanup}")]
    /// Field $name.
    PostgresOutageUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error("RustFS/S3 outage scenario failed: {primary}; unmount also failed: {cleanup}")]
    /// Field $name.
    RustfsOutageUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error(
        "mounted client crash/restart scenario failed: {primary}; unmount also failed: {cleanup}"
    )]
    /// Field $name.
    CrashRestartUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error(
        "mounted publication boundary crash matrix failed: {primary}; unmount also failed: {cleanup}"
    )]
    /// A publication-boundary run failed and cleanup also failed.
    PublicationBoundaryUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error("publication boundary peer refresh failed: {errors}")]
    /// One or more observer peers failed to refresh at the publication gate.
    PublicationBoundaryPeerRefreshFailed {
        /// The collected refresh errors.
        errors: String,
    },
    #[error("mounted tree validation failed: {0}")]
    /// Variant $name.
    MountedTree(#[from] MountedTreeError),
    #[error("mounted chaos helper failed: {0}")]
    /// Variant $name.
    MountedChaosHelper(String),
    #[error("scenario infrastructure failed: {0}")]
    /// Variant $name.
    Infra(#[from] InfraError),
    #[error("scenario operation log failed: {0}")]
    /// Variant $name.
    OperationLog(#[from] OperationLogError),
    #[error("PostgreSQL outage scenario requires compose infrastructure")]
    /// Variant $name.
    MissingComposeInfrastructure,
    #[error("missing E2E run manifest: {path}")]
    /// Field $name.
    MissingRunManifest {
        /// The path value.
        path: PathBuf,
    },
    #[error("PostgreSQL outage operations rejected {rejected} of {total} expected local writes")]
    /// Field $name.
    RejectedOutageOperations {
        /// The rejected value.
        rejected: usize,
        /// The total value.
        total: usize,
    },
    #[error("peer client observed the RustFS/S3 outage write before recovery: {checksum}")]
    /// Field $name.
    PeerObservedOutageWrite {
        /// The checksum value.
        checksum: String,
    },
    #[error("mounted smoke scenario requires at least {expected} clients, got {actual}")]
    /// Variant $name.
    InsufficientMountedClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error("PostgreSQL outage scenario requires at least {expected} clients, got {actual}")]
    /// Variant $name.
    InsufficientOutageClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error(
        "mounted client crash/restart scenario requires at least {expected} clients, got {actual}"
    )]
    /// Field $name.
    InsufficientCrashRestartClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error(
        "mounted publication boundary crash matrix requires at least {expected} clients, got {actual}"
    )]
    /// The publication-boundary matrix did not have enough mounted clients.
    InsufficientPublicationBoundaryClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error("namespace collision scenario requires at least {expected} clients, got {actual}")]
    /// Field $name.
    InsufficientCollisionClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error("mounted chaos scenario requires at least {expected} clients, got {actual}")]
    /// Field $name.
    InsufficientChaosClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error("mounted fault chaos scenario requires at least {expected} clients, got {actual}")]
    /// Field $name.
    InsufficientFaultChaosClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error(
        "mounted LWW reconnect chaos scenario requires at least {expected} clients, got {actual}"
    )]
    /// Field $name.
    InsufficientLwwReconnectClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error("namespace collision scenario failed: {primary}; unmount also failed: {cleanup}")]
    /// Field $name.
    NamespaceCollisionUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error("mounted chaos scenario failed: {primary}; unmount also failed: {cleanup}")]
    /// Variant $name.
    MountedChaosUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error("mounted hot-spot churn scenario failed: {primary}; unmount also failed: {cleanup}")]
    /// Field $name.
    MountedHotspotChurnUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error("mounted fault chaos scenario failed: {primary}; unmount also failed: {cleanup}")]
    /// Field $name.
    MountedFaultChaosUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error(
        "mounted LWW reconnect chaos scenario failed: {primary}; unmount also failed: {cleanup}"
    )]
    /// Field $name.
    MountedLwwReconnectUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error("mounted open-handle scenario failed: {primary}; unmount also failed: {cleanup}")]
    /// Field $name.
    MountedOpenHandleUnmountAfterFailure {
        /// The primary value.
        primary: String,
        /// The cleanup value.
        cleanup: String,
    },
    #[error("mounted open-handle scenario requires at least {expected} clients, got {actual}")]
    /// Field $name.
    InsufficientOpenHandleClients {
        /// The expected value.
        expected: usize,
        /// The actual value.
        actual: usize,
    },
    #[error("mounted open-handle validation failed: {checks:?}")]
    /// Field $name.
    MountedOpenHandleValidation {
        /// The checks value.
        checks: Vec<OpenHandleCheck>,
    },
    #[error("namespace collision validation failed: {violations:?}")]
    /// Variant $name.
    NamespaceInvariantViolations {
        /// Field $name.
        violations: Vec<NamespaceInvariantViolation>,
    },
    #[error("mounted chaos validation failed: {violations:?}")]
    /// Variant $name.
    MountedChaosInvariantViolations {
        /// Field $name.
        violations: Vec<NamespaceInvariantViolation>,
    },
    #[error("mounted hot-spot churn validation failed: {violations:?}")]
    /// Variant $name.
    MountedHotspotChurnInvariantViolations {
        /// Field $name.
        violations: Vec<NamespaceInvariantViolation>,
    },
    #[error(
        "mounted chaos round {round} timed out after {timeout_ms}ms; pending operations: {pending:?}"
    )]
    /// Variant $name.
    MountedChaosOperationTimeout {
        /// Field $name.
        round: usize,
        /// Field $name.
        timeout_ms: u128,
        /// Field $name.
        pending: Vec<String>,
    },
    #[error("mounted chaos helper launch failures: {errors:?}")]
    /// Variant $name.
    MountedChaosLaunchFailures {
        /// The errors value.
        errors: Vec<String>,
    },
    #[error("mounted fault chaos validation failed: {violations:?}")]
    /// Variant $name.
    MountedFaultChaosInvariantViolations {
        /// Field $name.
        violations: Vec<NamespaceInvariantViolation>,
    },
    #[error("mounted LWW reconnect chaos validation failed: {violations:?}")]
    /// Variant $name.
    MountedLwwReconnectInvariantViolations {
        /// Field $name.
        violations: Vec<NamespaceInvariantViolation>,
    },
    #[error(
        "mounted LWW reconnect oracle mismatch on {path}: expected {expected_checksum_sha256}, mismatches: {mismatches:?}"
    )]
    /// Variant $name.
    MountedLwwReconnectOracleMismatch {
        /// Field $name.
        path: String,
        /// Field $name.
        expected_checksum_sha256: String,
        /// Field $name.
        mismatches: Vec<LwwOracleClientResult>,
    },
    #[cfg(test)]
    #[error("namespace collision recording replication failed: {errors:?}")]
    /// Variant $name.
    ReplicationFailure {
        /// The errors value.
        errors: Vec<String>,
    },
}

/// Trait $name.
pub trait Scenario: Send {
    /// Function $name.
    fn definition(&self) -> ScenarioDefinition;
    /// Function $name.
    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    /// Function $name.
    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    /// Function $name.
    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError>;
    /// Function $name.
    fn cleanup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
}

/// Function $name.
pub fn available_scenarios() -> Vec<ScenarioDefinition> {
    vec![
        BasicMultiClientSmoke.definition(),
        PostgresOutageMultiClientRecovery.definition(),
        RustfsOutageMultiClientRecovery.definition(),
        MountedClientCrashRestartRecovery.definition(),
        MountedPublicationBoundaryCrashMatrix.definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsRename).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsUnlink).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::UnlinkVsAppend).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::OverwriteVsRead).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::MkdirRmdirVsCreate).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::ReaddirStatDuringMutation)
            .definition(),
        MountedChaosConvergence.definition(),
        MountedHotspotChurnConvergence.definition(),
        MountedFaultChaosConvergence.definition(),
        MountedLwwReconnectChaosConvergence.definition(),
        MountedOpenHandleConvergence.definition(),
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
        MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX
        | "publication-boundary-crash"
        | "publication-boundary" => Some(Box::new(MountedPublicationBoundaryCrashMatrix)),
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
        MOUNTED_CHAOS_CONVERGENCE | "mounted-chaos" | "chaos" => {
            Some(Box::new(MountedChaosConvergence))
        }
        MOUNTED_HOTSPOT_CHURN_CONVERGENCE | "hotspot-churn" | "hot-dir-churn" => {
            Some(Box::new(MountedHotspotChurnConvergence))
        }
        MOUNTED_FAULT_CHAOS_CONVERGENCE | "fault-chaos" | "chaos-faults" => {
            Some(Box::new(MountedFaultChaosConvergence))
        }
        MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE | "lww-chaos" | "reconnect-lww" => {
            Some(Box::new(MountedLwwReconnectChaosConvergence))
        }
        MOUNTED_OPEN_HANDLE_CONVERGENCE | "open-handle" | "open-unlink-rename" => {
            Some(Box::new(MountedOpenHandleConvergence))
        }
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
        #[cfg(test)]
        TEST_HANGING_RUN => Some(Box::new(TestHangingRunScenario)),
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
            requires_mount: true,
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

#[derive(Debug, Clone, Copy)]
struct PostgresOutageMultiClientRecovery;

impl Scenario for PostgresOutageMultiClientRecovery {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
            description: "mounted multi-client PostgreSQL outage recovery scenario",
            requires_studiofs_bin: true,
            requires_mount: true,
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
            "status": if summary.initial_publication_gate.converged
                && summary.final_validator.converged
            {
                "passed"
            } else {
                "failed"
            },
            "outage_duration_ms": summary.outage_duration_ms,
            "recovery_time_ms": summary.recovery_time_ms,
            "operations_accepted_during_outage": summary.operations_accepted_during_outage,
            "initial_publication_gate": summary.initial_publication_gate,
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
            requires_mount: true,
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
            requires_mount: true,
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

#[derive(Debug, Clone, Copy)]
struct MountedPublicationBoundaryCrashMatrix;

impl Scenario for MountedPublicationBoundaryCrashMatrix {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX,
            description: "mounted publication boundary crash matrix scenario",
            requires_studiofs_bin: true,
            requires_mount: true,
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
        let mut driver = RealPublicationBoundaryDriver(MultiClientController::new(
            config.clone(),
            SystemClientProcessExecutor,
        ));
        run_publication_boundary_crash_matrix_with_driver(
            context,
            &mut driver,
            PUBLICATION_BOUNDARY_CONVERGENCE_TIMEOUT,
            PUBLICATION_BOUNDARY_CONVERGENCE_POLL_INTERVAL,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("publication-boundary-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: PublicationBoundaryCrashMatrixSummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary
                .boundaries
                .iter()
                .all(|boundary| boundary.final_validator.converged)
            {
                "passed"
            } else {
                "failed"
            },
            "boundaries": summary.boundaries,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX}/operations.ndjson"
            ),
            format!(
                "artifacts/scenarios/{MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX}/publication-boundary-summary.json"
            ),
            format!(
                "artifacts/scenarios/{MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX}/validation.json"
            ),
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

fn operation_log_path(context: &ScenarioContext) -> PathBuf {
    context.scenario_dir.join("operations.ndjson")
}

fn shared_operation_record(
    writer: &OperationLogWriter,
    scenario: &str,
    operation: SharedOperation,
) -> SharedOperationRecord {
    writer.record(scenario, operation)
}

fn append_shared_record(
    writer: &mut OperationLogWriter,
    record: &SharedOperationRecord,
) -> Result<(), ScenarioError> {
    writer.append_record(record)?;
    Ok(())
}

trait MountedSmokeDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ClientControllerError>;
    fn mount_all(&mut self) -> Result<(), ClientControllerError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn after_smoke_write(
        &mut self,
        _clients: &[ClientSpec],
        _relative_path: &str,
    ) -> Result<(), ClientControllerError> {
        Ok(())
    }
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

    let relative_path = run_unique_path(context, "smoke", SMOKE_FILE_NAME);
    let smoke_file = clients[0].mount_point.join(&relative_path);
    write_mounted_file(
        &smoke_file,
        &run_unique_payload(context, BASIC_MULTI_CLIENT_SMOKE, "smoke"),
    )?;
    driver.after_smoke_write(&clients, &relative_path)?;
    let mounts = client_mounts(context, &clients[..SMOKE_CLIENT_NAMES.len()]);
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

trait PostgresOutageDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn after_initial_write(
        &mut self,
        _clients: &[ClientSpec],
        _relative_path: &str,
    ) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn stop_postgres(&mut self) -> Result<(), ScenarioError>;
    fn start_postgres(&mut self) -> Result<(), ScenarioError>;
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

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.unmount_all()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PostgresOutageRecoverySummary {
    bootstrap: VolumeBootstrapSummary,
    initial_publication_gate: TreeComparison,
    outage_duration_ms: u128,
    operations_accepted_during_outage: usize,
    recovery_time_ms: u128,
    final_validator: TreeComparison,
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

    let initial_path = run_unique_path(context, "postgres-outage", OUTAGE_INITIAL_FILE_NAME);
    write_mounted_file(
        &clients[0].mount_point.join(&initial_path),
        &run_unique_payload(context, POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY, "initial"),
    )?;
    driver.after_initial_write(&clients, &initial_path)?;
    let mounts = client_mounts(context, &clients[..POSTGRES_OUTAGE_CLIENT_NAMES.len()]);
    let initial_publication_gate = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("healthy-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let outage_started = Instant::now();
    driver.stop_postgres()?;
    append_postgres_runtime_operation(context, SharedOperation::PostgresOutage)?;
    let accepted_operations = perform_outage_operations(context, &clients[1..3])?;
    let outage_duration_ms = elapsed_millis_nonzero(outage_started);

    // `start_postgres` uses `docker compose up --wait`, so this includes
    // PostgreSQL healthcheck readiness before mounted convergence polling.
    let recovery_started = Instant::now();
    driver.start_postgres()?;
    append_postgres_runtime_operation(context, SharedOperation::PostgresRecovery)?;
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let recovery_time_ms = elapsed_millis_nonzero(recovery_started);

    let summary = PostgresOutageRecoverySummary {
        bootstrap,
        initial_publication_gate: initial_publication_gate.comparison,
        outage_duration_ms,
        operations_accepted_during_outage: accepted_operations,
        recovery_time_ms,
        final_validator: convergence.comparison,
    };
    fs::write(
        context
            .scenario_dir
            .join("postgres-outage-recovery-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

fn perform_outage_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
) -> Result<usize, ScenarioError> {
    if !context.scenario_dir.exists() {
        return Err(ScenarioError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "missing scenario directory: {}",
                context.scenario_dir.display()
            ),
        )));
    }
    let outage_log = context.scenario_dir.join("outage-operations.ndjson");
    let operations_log = operation_log_path(context);
    let mut outage_writer = OperationLogWriter::open_append(&outage_log)?;
    let mut operations_writer = OperationLogWriter::open_append(&operations_log)?;
    let mut accepted = 0;
    for (index, client) in clients.iter().enumerate() {
        let relative = run_unique_path(
            context,
            "postgres-outage",
            &format!("outage-local-write-{}.txt", index + 1),
        );
        let payload = format!(
            "accepted local-first outage write from {} in {} seed={}\n",
            client.name, context.run_id, context.seed
        );
        let target = client.mount_point.join(&relative);
        let write_result = (|| -> io::Result<()> {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&target, payload.as_bytes())
        })();
        let error = write_result.err().map(|err| err.to_string());
        let operation_accepted = error.is_none();
        if operation_accepted {
            accepted += 1;
        }
        let outcome = match error {
            Some(error) => OperationOutcome::Rejected { error },
            None => OperationOutcome::Accepted,
        };
        let operation = SharedOperation::Write {
            path: relative.clone(),
            offset: Some(0),
            bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            checksum_sha256: Some(bytes_sha256(payload.as_bytes())),
        };
        for writer in [&mut outage_writer, &mut operations_writer] {
            let record = shared_operation_record(
                writer,
                POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
                operation.clone(),
            )
            .with_client(&client.name)
            .with_outcome(outcome.clone())
            .with_metadata("phase", "postgres_outage");
            append_shared_record(writer, &record)?;
        }
    }
    let rejected = clients.len() - accepted;
    if rejected > 0 {
        return Err(ScenarioError::RejectedOutageOperations {
            rejected,
            total: clients.len(),
        });
    }
    Ok(accepted)
}

fn append_postgres_runtime_operation(
    context: &ScenarioContext,
    operation: SharedOperation,
) -> Result<(), ScenarioError> {
    let outage_log = context.scenario_dir.join("outage-operations.ndjson");
    let operations_log = operation_log_path(context);
    let mut outage_writer = OperationLogWriter::open_append(&outage_log)?;
    let mut operations_writer = OperationLogWriter::open_append(&operations_log)?;
    for writer in [&mut outage_writer, &mut operations_writer] {
        let record = shared_operation_record(
            writer,
            POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
            operation.clone(),
        )
        .with_outcome(OperationOutcome::Observed);
        append_shared_record(writer, &record)?;
    }
    Ok(())
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

    let gate_relative_path = rustfs_outage_gate_relative_path(context);
    let gate_file = clients[0].mount_point.join(&gate_relative_path);
    let initial_payload = rustfs_outage_initial_payload(context);
    write_mounted_file(&gate_file, &initial_payload)?;
    driver.after_initial_write(&clients, &gate_relative_path)?;
    let initial_mounts = client_mounts(context, &clients[..RUSTFS_OUTAGE_CLIENT_NAMES.len()]);
    wait_for_convergence(
        &initial_mounts,
        context.scenario_dir.join("healthy-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let outage_started = Instant::now();
    driver.stop_rustfs()?;
    let relative_path = rustfs_outage_relative_path(context);
    let source_file = clients[0].mount_point.join(&relative_path);
    let recovered_payload = rustfs_outage_recovered_payload(context);
    write_mounted_file(&source_file, &recovered_payload)?;
    let local_read_checksum_sha256 = bytes_sha256(&fs::read(&source_file)?);
    let outage_duration_ms = elapsed_millis_nonzero(outage_started);
    let mut peer_visibility_during_outage = Vec::new();
    for peer in clients
        .iter()
        .skip(1)
        .take(RUSTFS_OUTAGE_CLIENT_NAMES.len() - 1)
    {
        let visible_checksum_sha256 =
            driver.assert_peer_not_visible(peer, &relative_path, &local_read_checksum_sha256)?;
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
    let upload_recovery_time_ms = elapsed_millis_nonzero(recovery_started);
    let mut final_client_checksums = Vec::new();
    for client in clients.iter().take(RUSTFS_OUTAGE_CLIENT_NAMES.len()) {
        let checksum = bytes_sha256(&fs::read(client.mount_point.join(&relative_path))?);
        if checksum != local_read_checksum_sha256 {
            return Err(ScenarioError::Io(std::io::Error::other(format!(
                "recovery convergence failure: {} read checksum {} but expected {} for {}",
                client.name, checksum, local_read_checksum_sha256, relative_path
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
        outage_file: relative_path,
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

fn rustfs_outage_relative_path(context: &ScenarioContext) -> String {
    run_unique_path(context, "rustfs-outage", RUSTFS_OUTAGE_FILE_NAME)
}

fn rustfs_outage_gate_relative_path(context: &ScenarioContext) -> String {
    run_unique_path(context, "rustfs-outage", RUSTFS_OUTAGE_GATE_FILE_NAME)
}

fn rustfs_outage_initial_payload(context: &ScenarioContext) -> Vec<u8> {
    run_unique_payload(context, "rustfs-outage", "initial")
}

fn rustfs_outage_recovered_payload(context: &ScenarioContext) -> Vec<u8> {
    run_unique_payload(context, "rustfs-outage", "recovered-local-write")
}

fn run_unique_path(context: &ScenarioContext, namespace: &str, leaf: &str) -> String {
    format!(
        "{}/{}/{}",
        namespace,
        scenario_path_token(&context.run_id),
        leaf
    )
}

fn run_unique_dir(context: &ScenarioContext, namespace: &str) -> String {
    format!("{}/{}", namespace, scenario_path_token(&context.run_id))
}

fn run_unique_payload(context: &ScenarioContext, scenario: &str, label: &str) -> Vec<u8> {
    format!(
        "studiofs e2e scenario={scenario} label={label} run_id={} seed={}\n",
        context.run_id, context.seed
    )
    .into_bytes()
}

fn scenario_path_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
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

fn write_mounted_file(path: &Path, payload: &[u8]) -> Result<(), ScenarioError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    write_payload_with_publication_barrier(path, payload)?;
    let readback = wait_for_mounted_payload_readback(
        path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "mounted write readback mismatch for {}: expected {} bytes, read {} bytes",
            path.display(),
            payload.len(),
            readback.len()
        ))));
    }
    Ok(())
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
    let outage_log = context.scenario_dir.join("rustfs-outage-operations.ndjson");
    let operations_log = operation_log_path(context);
    let mut outage_writer = OperationLogWriter::open_append(&outage_log)?;
    let mut operations_writer = OperationLogWriter::open_append(&operations_log)?;
    for writer in [&mut outage_writer, &mut operations_writer] {
        for (operation, client, outcome) in [
            (
                SharedOperation::ObjectStoreOutage,
                None,
                OperationOutcome::Observed,
            ),
            (
                SharedOperation::Write {
                    path: summary.outage_file.clone(),
                    offset: Some(0),
                    bytes: u64::try_from(rustfs_outage_recovered_payload(context).len())
                        .unwrap_or(u64::MAX),
                    checksum_sha256: Some(summary.local_read_checksum_sha256.clone()),
                },
                Some(RUSTFS_OUTAGE_CLIENT_NAMES[0]),
                OperationOutcome::Accepted,
            ),
            (
                SharedOperation::Read {
                    path: summary.outage_file.clone(),
                    bytes: Some(
                        u64::try_from(rustfs_outage_recovered_payload(context).len())
                            .unwrap_or(u64::MAX),
                    ),
                    checksum_sha256: Some(summary.local_read_checksum_sha256.clone()),
                },
                Some(RUSTFS_OUTAGE_CLIENT_NAMES[0]),
                OperationOutcome::Observed,
            ),
            (
                SharedOperation::ObjectStoreRecovery,
                None,
                OperationOutcome::Observed,
            ),
        ] {
            let mut record =
                shared_operation_record(writer, RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY, operation)
                    .with_outcome(outcome)
                    .with_metadata(
                        "final_checksum_sha256",
                        summary.final_checksum_sha256.clone(),
                    );
            if let Some(client) = client {
                record = record.with_client(client);
            }
            append_shared_record(writer, &record)?;
        }
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
    fn before_convergence_validation(
        &mut self,
        _mode: CrashRestartMode,
        _relative_path: &str,
        _clients: &[ClientSpec],
    ) -> Result<(), ScenarioError> {
        Ok(())
    }
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
    local_read_checksum_sha256: String,
    peer_visible_before_convergence: Option<String>,
    final_checksum_sha256: String,
    restart_elapsed_ms: u128,
    convergence_elapsed_ms: u128,
    final_validator: TreeComparison,
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
            &crash_restart_relative_path(context, CrashRestartMode::AfterLocalDurabilityBoundary),
            &run_unique_payload(
                context,
                MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
                "after-local-durability-boundary",
            ),
            convergence_timeout,
            poll_interval,
        )?,
        run_crash_restart_mode(
            context,
            driver,
            &clients,
            CrashRestartMode::BeforeRemotePublicationCompletes,
            &crash_restart_relative_path(
                context,
                CrashRestartMode::BeforeRemotePublicationCompletes,
            ),
            &run_unique_payload(
                context,
                MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
                "before-remote-publication-completes",
            ),
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

fn crash_restart_relative_path(context: &ScenarioContext, mode: CrashRestartMode) -> String {
    let file_name = match mode {
        CrashRestartMode::AfterLocalDurabilityBoundary => CRASH_RESTART_LOCAL_DURABLE_FILE_NAME,
        CrashRestartMode::BeforeRemotePublicationCompletes => {
            CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME
        }
    };
    run_unique_path(context, "crash-restart", file_name)
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
    let durable_payload_handle = write_local_durable_payload_and_keep_handle(
        crashed.mount_point.join(relative_path),
        payload,
    )?;
    let restart_started = Instant::now();
    driver.force_kill(&crashed.name)?;
    drop(durable_payload_handle);
    driver.remount(&crashed.name)?;
    let local_read_payload = fs::read(crashed.mount_point.join(relative_path))?;
    if local_read_payload != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "durability failure: data read after restart does not match written payload for {relative_path}"
        ))));
    }
    let local_read_checksum_sha256 = bytes_sha256(&local_read_payload);
    let restart_elapsed_ms = elapsed_millis_nonzero(restart_started);

    let peer_visible_before_convergence =
        if mode == CrashRestartMode::BeforeRemotePublicationCompletes {
            read_peer_checksum(&clients[1], relative_path)?
                .filter(|checksum| checksum == &local_read_checksum_sha256)
        } else {
            None
        };
    driver.before_convergence_validation(mode, relative_path, clients)?;

    let convergence_started = Instant::now();
    let mounts = client_mounts(context, clients);
    let convergence = wait_for_convergence(
        &mounts,
        context
            .scenario_dir
            .join(format!("validation-{}", crash_restart_mode_slug(mode))),
        convergence_timeout,
        poll_interval,
    )?;
    let convergence_elapsed_ms = elapsed_millis_nonzero(convergence_started);
    let final_payload = fs::read(clients[1].mount_point.join(relative_path))?;
    if final_payload != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "convergence failure: peer data does not match written payload for {relative_path}"
        ))));
    }
    let final_checksum_sha256 = bytes_sha256(&final_payload);

    let summary = CrashRestartModeSummary {
        mode,
        restarted_client: crashed.name.clone(),
        restart_client_root: crashed.client_root.display().to_string(),
        relative_path: relative_path.to_string(),
        bytes: payload.len(),
        local_read_checksum_sha256,
        peer_visible_before_convergence,
        final_checksum_sha256,
        restart_elapsed_ms,
        convergence_elapsed_ms,
        final_validator: convergence.comparison,
    };
    write_crash_restart_operation(context, &summary)?;
    Ok(summary)
}

fn write_crash_restart_operation(
    context: &ScenarioContext,
    summary: &CrashRestartModeSummary,
) -> Result<(), ScenarioError> {
    let path = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path)?;
    let bytes = u64::try_from(summary.bytes).unwrap_or(u64::MAX);
    let mode = crash_restart_mode_log_name(summary.mode);
    for operation in [
        SharedOperation::Write {
            path: summary.relative_path.clone(),
            offset: Some(0),
            bytes,
            checksum_sha256: Some(summary.local_read_checksum_sha256.clone()),
        },
        SharedOperation::Fsync {
            path: summary.relative_path.clone(),
        },
        SharedOperation::ClientCrash,
        SharedOperation::ClientRestart,
    ] {
        let record =
            shared_operation_record(&writer, MOUNTED_CLIENT_CRASH_RESTART_RECOVERY, operation)
                .with_client(&summary.restarted_client)
                .with_outcome(OperationOutcome::Accepted)
                .with_metadata("mode", mode)
                .with_metadata(
                    "final_checksum_sha256",
                    summary.final_checksum_sha256.clone(),
                );
        append_shared_record(&mut writer, &record)?;
    }
    Ok(())
}

trait PublicationBoundaryDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn graceful_shutdown(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn remount(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn publish_for_convergence(
        &mut self,
        _boundary: PublicationBoundary,
        _clients: &[ClientSpec],
        _relative_path: &str,
    ) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealPublicationBoundaryDriver(MultiClientController<SystemClientProcessExecutor>);

impl PublicationBoundaryDriver for RealPublicationBoundaryDriver {
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

    fn graceful_shutdown(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.0.unmount(client)?)
    }

    fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.0.remount(client)?)
    }

    fn publish_for_convergence(
        &mut self,
        _boundary: PublicationBoundary,
        clients: &[ClientSpec],
        _relative_path: &str,
    ) -> Result<(), ScenarioError> {
        let mut errors = Vec::new();
        for peer in clients.iter().skip(1) {
            if let Err(error) = self.0.restart(&peer.name) {
                errors.push(format!("{}: {error}", peer.name));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ScenarioError::PublicationBoundaryPeerRefreshFailed {
                errors: errors.join("; "),
            })
        }
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.unmount_all()?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PublicationBoundary {
    Flush,
    Fsync,
    ReleaseClose,
    ShutdownDrain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PublicationBoundaryPeerVisibility {
    client: String,
    visible_checksum_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PublicationBoundarySummary {
    boundary: PublicationBoundary,
    restarted_client: String,
    restart_client_root: String,
    relative_path: String,
    bytes: usize,
    local_read_checksum_sha256: String,
    peer_visibility_at_publication_gate: Vec<PublicationBoundaryPeerVisibility>,
    final_checksum_sha256: String,
    restart_elapsed_ms: u128,
    convergence_elapsed_ms: u128,
    final_validator: TreeComparison,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PublicationBoundaryCrashMatrixSummary {
    scenario: String,
    bootstrap: VolumeBootstrapSummary,
    boundaries: Vec<PublicationBoundarySummary>,
}

fn run_publication_boundary_crash_matrix_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn PublicationBoundaryDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<PublicationBoundaryCrashMatrixSummary, ScenarioError> {
    let result = run_publication_boundary_crash_matrix_body(
        context,
        driver,
        convergence_timeout,
        poll_interval,
    );
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => {
            Err(ScenarioError::PublicationBoundaryUnmountAfterFailure {
                primary: primary.to_string(),
                cleanup: cleanup.to_string(),
            })
        }
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_publication_boundary_crash_matrix_body(
    context: &ScenarioContext,
    driver: &mut dyn PublicationBoundaryDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<PublicationBoundaryCrashMatrixSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: PUBLICATION_BOUNDARY_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;

    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < PUBLICATION_BOUNDARY_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientPublicationBoundaryClients {
            expected: PUBLICATION_BOUNDARY_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }

    let mut boundaries = Vec::new();
    for boundary in [
        PublicationBoundary::Flush,
        PublicationBoundary::Fsync,
        PublicationBoundary::ReleaseClose,
        PublicationBoundary::ShutdownDrain,
    ] {
        boundaries.push(run_publication_boundary(
            context,
            driver,
            &clients,
            boundary,
            convergence_timeout,
            poll_interval,
        )?);
    }

    let summary = PublicationBoundaryCrashMatrixSummary {
        scenario: MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX.to_string(),
        bootstrap,
        boundaries,
    };
    fs::write(
        context
            .scenario_dir
            .join("publication-boundary-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

fn run_publication_boundary(
    context: &ScenarioContext,
    driver: &mut dyn PublicationBoundaryDriver,
    clients: &[ClientSpec],
    boundary: PublicationBoundary,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<PublicationBoundarySummary, ScenarioError> {
    let writer = &clients[0];
    let relative_path = publication_boundary_file_name(context, boundary);
    let payload = publication_boundary_payload(context, boundary);
    let publication_boundary_handle =
        apply_publication_boundary(writer, &relative_path, &payload, boundary)?;
    if boundary == PublicationBoundary::ReleaseClose {
        wait_for_publication_boundary_release_close_drain(
            context,
            writer,
            &relative_path,
            &payload,
        )?;
    } else {
        wait_for_publication_boundary_readback(writer, &relative_path, &payload, boundary)?;
    }

    let restart_started = Instant::now();
    if boundary == PublicationBoundary::ShutdownDrain {
        driver.graceful_shutdown(&writer.name)?;
    } else {
        driver.force_kill(&writer.name)?;
    }
    drop(publication_boundary_handle);

    driver.remount(&writer.name)?;
    let local_read_payload = wait_for_mounted_payload_readback(
        &writer.mount_point.join(&relative_path),
        &payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if local_read_payload != payload {
        write_publication_boundary_readback_mismatch_artifacts(
            context,
            boundary,
            &relative_path,
            &payload,
            &local_read_payload,
        );
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "publication boundary durability failure: data read after restart does not match written payload for {relative_path}: expected_len={} expected_checksum={} actual_len={} actual_checksum={}",
            payload.len(),
            bytes_sha256(&payload),
            local_read_payload.len(),
            bytes_sha256(&local_read_payload)
        ))));
    }
    let local_read_checksum_sha256 = bytes_sha256(&local_read_payload);
    let restart_elapsed_ms = elapsed_millis_nonzero(restart_started);

    driver.publish_for_convergence(boundary, clients, &relative_path)?;
    let peer_visibility_at_publication_gate =
        publication_boundary_peer_visibility(&clients[1..], &relative_path)?;
    let convergence_started = Instant::now();
    let mounts = client_mounts(context, clients);
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join(format!(
            "validation-{}",
            publication_boundary_slug(boundary)
        )),
        convergence_timeout,
        poll_interval,
    )?;
    let convergence_elapsed_ms = elapsed_millis_nonzero(convergence_started);
    let final_payload = fs::read(clients[1].mount_point.join(&relative_path))?;
    if final_payload != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "publication boundary convergence failure: peer data does not match written payload for {relative_path}"
        ))));
    }
    let final_checksum_sha256 = bytes_sha256(&final_payload);

    let summary = PublicationBoundarySummary {
        boundary,
        restarted_client: writer.name.clone(),
        restart_client_root: writer.client_root.display().to_string(),
        relative_path,
        bytes: payload.len(),
        local_read_checksum_sha256,
        peer_visibility_at_publication_gate,
        final_checksum_sha256,
        restart_elapsed_ms,
        convergence_elapsed_ms,
        final_validator: convergence.comparison,
    };
    write_publication_boundary_operations(context, &summary)?;
    Ok(summary)
}

fn wait_for_publication_boundary_readback(
    client: &ClientSpec,
    relative_path: &str,
    payload: &[u8],
    boundary: PublicationBoundary,
) -> Result<(), ScenarioError> {
    let path = client.mount_point.join(relative_path);
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "publication boundary {} readback did not match written payload for {relative_path}: expected_len={} expected_checksum={} actual_len={} actual_checksum={}",
            publication_boundary_log_name(boundary),
            payload.len(),
            bytes_sha256(payload),
            readback.len(),
            bytes_sha256(&readback)
        ))));
    }
    Ok(())
}

fn write_publication_boundary_readback_mismatch_artifacts(
    context: &ScenarioContext,
    boundary: PublicationBoundary,
    relative_path: &str,
    expected_payload: &[u8],
    actual_payload: &[u8],
) {
    let slug = publication_boundary_slug(boundary);
    let _ = fs::create_dir_all(&context.scenario_dir);
    let expected_path = context
        .scenario_dir
        .join(format!("publication-boundary-{slug}-expected.bin"));
    let actual_path = context
        .scenario_dir
        .join(format!("publication-boundary-{slug}-actual.bin"));
    let diagnostic_path = context.scenario_dir.join(format!(
        "publication-boundary-{slug}-readback-mismatch.json"
    ));
    let _ = fs::write(&expected_path, expected_payload);
    let _ = fs::write(&actual_path, actual_payload);
    let diagnostic = json!({
        "boundary": boundary,
        "relative_path": relative_path,
        "expected_len": expected_payload.len(),
        "expected_checksum_sha256": bytes_sha256(expected_payload),
        "actual_len": actual_payload.len(),
        "actual_checksum_sha256": bytes_sha256(actual_payload),
        "expected_artifact": expected_path.display().to_string(),
        "actual_artifact": actual_path.display().to_string(),
    });
    let _ = fs::write(
        diagnostic_path,
        serde_json::to_vec_pretty(&diagnostic).unwrap_or_default(),
    );
}

fn apply_publication_boundary(
    client: &ClientSpec,
    relative_path: &str,
    payload: &[u8],
    boundary: PublicationBoundary,
) -> Result<Option<File>, ScenarioError> {
    let path = client.mount_point.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)?;
    file.write_all(payload)?;
    match boundary {
        PublicationBoundary::Flush => {
            file.sync_data()?;
            Ok(Some(file))
        }
        PublicationBoundary::Fsync => {
            file.sync_all()?;
            Ok(Some(file))
        }
        PublicationBoundary::ReleaseClose => {
            file.flush()?;
            drop(file);
            Ok(None)
        }
        PublicationBoundary::ShutdownDrain => {
            file.sync_all()?;
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicationBoundaryDrainSnapshot {
    pending_local_dirty_bytes: u64,
    pending_persisted_slice_bytes: u64,
    writer_stack_dirty_bytes: u64,
    total_dirty_bytes: u64,
    local_sync_unsynced_bytes: u64,
}

impl PublicationBoundaryDrainSnapshot {
    fn from_diagnostics_json(value: &Value) -> Result<Self, ScenarioError> {
        Ok(Self {
            pending_local_dirty_bytes: required_json_u64(
                value,
                "/runtime_snapshot/write_path/dirty_pressure/pending_local_dirty_bytes",
            )?,
            pending_persisted_slice_bytes: required_json_u64(
                value,
                "/runtime_snapshot/write_path/dirty_pressure/pending_persisted_slice_bytes",
            )?,
            writer_stack_dirty_bytes: required_json_u64(
                value,
                "/runtime_snapshot/write_path/dirty_pressure/writer_stack_dirty_bytes",
            )?,
            total_dirty_bytes: required_json_u64(
                value,
                "/runtime_snapshot/write_path/dirty_pressure/total_dirty_bytes",
            )?,
            local_sync_unsynced_bytes: required_json_u64(
                value,
                "/runtime_snapshot/write_path/drain_lag/local_sync_unsynced_bytes",
            )?,
        })
    }

    fn is_locally_drained(&self) -> bool {
        self.pending_local_dirty_bytes == 0
            && self.pending_persisted_slice_bytes == 0
            && self.writer_stack_dirty_bytes == 0
            && self.total_dirty_bytes == 0
            && self.local_sync_unsynced_bytes == 0
    }
}

fn wait_for_publication_boundary_release_close_drain(
    context: &ScenarioContext,
    client: &ClientSpec,
    relative_path: &str,
    payload: &[u8],
) -> Result<(), ScenarioError> {
    let path = client.mount_point.join(relative_path);
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "publication boundary release-close readback did not match written payload for {relative_path}"
        ))));
    }

    let Some(config) = &context.client_controller_config else {
        return Ok(());
    };
    let executor = SystemCommandExecutor;
    wait_for_publication_boundary_local_drain(
        &executor,
        config,
        client,
        PUBLICATION_BOUNDARY_RELEASE_CLOSE_DRAIN_TIMEOUT,
        PUBLICATION_BOUNDARY_RELEASE_CLOSE_DRAIN_POLL_INTERVAL,
    )
}

fn wait_for_publication_boundary_local_drain(
    executor: &dyn CommandExecutor,
    config: &ClientControllerConfig,
    client: &ClientSpec,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = publication_boundary_drain_snapshot(executor, config, client)?;
        if snapshot.is_locally_drained() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ScenarioError::Io(std::io::Error::other(format!(
                "publication boundary release-close local drain did not settle before crash for {}: last_snapshot={:?}",
                client.name, snapshot
            ))));
        }
        thread::sleep(std::cmp::max(poll_interval, Duration::from_millis(1)));
    }
}

fn publication_boundary_drain_snapshot(
    executor: &dyn CommandExecutor,
    config: &ClientControllerConfig,
    client: &ClientSpec,
) -> Result<PublicationBoundaryDrainSnapshot, ScenarioError> {
    let command = CommandSpec {
        program: config.studiofs_bin.to_string_lossy().into_owned(),
        args: vec![
            "internal".to_string(),
            "diagnostics-attach".to_string(),
            client.mount_point.to_string_lossy().into_owned(),
            "--recent-events-limit".to_string(),
            "1".to_string(),
        ],
        env: vec![(
            "STUDIOFS_CLIENT_ROOT".to_string(),
            client.client_root.to_string_lossy().into_owned(),
        )],
        cwd: None,
        timeout: Some(PUBLICATION_BOUNDARY_DIAGNOSTICS_TIMEOUT),
        stdout_path: None,
        stderr_path: None,
    };
    let output = executor.run(&command)?;
    if output.status != 0 {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "publication boundary diagnostics attach failed for {} with status {}: {}",
            client.name,
            output.status,
            output.stderr.trim()
        ))));
    }
    let value = serde_json::from_str::<Value>(&output.stdout)?;
    PublicationBoundaryDrainSnapshot::from_diagnostics_json(&value)
}

fn required_json_u64(value: &Value, pointer: &str) -> Result<u64, ScenarioError> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ScenarioError::Io(std::io::Error::other(format!(
                "publication boundary diagnostics missing numeric field {pointer}"
            )))
        })
}

fn publication_boundary_peer_visibility(
    peers: &[ClientSpec],
    relative_path: &str,
) -> Result<Vec<PublicationBoundaryPeerVisibility>, ScenarioError> {
    peers
        .iter()
        .map(|peer| {
            read_peer_checksum(peer, relative_path).map(|visible_checksum_sha256| {
                PublicationBoundaryPeerVisibility {
                    client: peer.name.clone(),
                    visible_checksum_sha256,
                }
            })
        })
        .collect()
}

fn write_publication_boundary_operations(
    context: &ScenarioContext,
    summary: &PublicationBoundarySummary,
) -> Result<(), ScenarioError> {
    let path = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path)?;
    let bytes = u64::try_from(summary.bytes).unwrap_or(u64::MAX);
    let boundary = publication_boundary_log_name(summary.boundary);
    let mut operations = vec![SharedOperation::Write {
        path: summary.relative_path.clone(),
        offset: Some(0),
        bytes,
        checksum_sha256: Some(summary.local_read_checksum_sha256.clone()),
    }];
    operations.extend(publication_boundary_operations(
        summary.boundary,
        &summary.relative_path,
    ));
    operations.push(SharedOperation::SchedulerCheckpoint {
        name: "publication_boundary_checkpoint".to_string(),
    });
    if summary.boundary == PublicationBoundary::ShutdownDrain {
        operations.push(SharedOperation::MountedOnly {
            name: "graceful_shutdown_drain".to_string(),
            reason: "real mounted graceful shutdown boundary".to_string(),
        });
        operations.push(SharedOperation::ClientRestart);
    } else {
        operations.extend([SharedOperation::ClientCrash, SharedOperation::ClientRestart]);
    }

    for operation in operations {
        let record = shared_operation_record(
            &writer,
            MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX,
            operation,
        )
        .with_client(&summary.restarted_client)
        .with_outcome(OperationOutcome::Accepted)
        .with_metadata("boundary", boundary)
        .with_metadata(
            "final_checksum_sha256",
            summary.final_checksum_sha256.clone(),
        );
        append_shared_record(&mut writer, &record)?;
    }
    Ok(())
}

fn publication_boundary_operations(
    boundary: PublicationBoundary,
    path: &str,
) -> Vec<SharedOperation> {
    match boundary {
        PublicationBoundary::Flush => vec![SharedOperation::Flush {
            path: Some(path.to_string()),
        }],
        PublicationBoundary::Fsync => vec![SharedOperation::Fsync {
            path: path.to_string(),
        }],
        PublicationBoundary::ReleaseClose => vec![
            SharedOperation::Close {
                path: Some(path.to_string()),
            },
            SharedOperation::Release {
                path: Some(path.to_string()),
            },
        ],
        PublicationBoundary::ShutdownDrain => vec![SharedOperation::Fsync {
            path: path.to_string(),
        }],
    }
}

fn publication_boundary_file_name(
    context: &ScenarioContext,
    boundary: PublicationBoundary,
) -> String {
    run_unique_path(
        context,
        "publication-boundary",
        publication_boundary_leaf(boundary),
    )
}

fn publication_boundary_leaf(boundary: PublicationBoundary) -> &'static str {
    match boundary {
        PublicationBoundary::Flush => "publication-boundary/flush.txt",
        PublicationBoundary::Fsync => "publication-boundary/fsync.txt",
        PublicationBoundary::ReleaseClose => "publication-boundary/release-close.txt",
        PublicationBoundary::ShutdownDrain => "publication-boundary/shutdown-drain.txt",
    }
}

fn publication_boundary_payload(
    context: &ScenarioContext,
    boundary: PublicationBoundary,
) -> Vec<u8> {
    format!(
        "publication boundary {} payload for run {} seed={}\n",
        publication_boundary_log_name(boundary),
        context.run_id,
        context.seed
    )
    .into_bytes()
}

fn publication_boundary_slug(boundary: PublicationBoundary) -> &'static str {
    match boundary {
        PublicationBoundary::Flush => "flush",
        PublicationBoundary::Fsync => "fsync",
        PublicationBoundary::ReleaseClose => "release-close",
        PublicationBoundary::ShutdownDrain => "shutdown-drain",
    }
}

fn publication_boundary_log_name(boundary: PublicationBoundary) -> &'static str {
    match boundary {
        PublicationBoundary::Flush => "flush",
        PublicationBoundary::Fsync => "fsync",
        PublicationBoundary::ReleaseClose => "release_close",
        PublicationBoundary::ShutdownDrain => "shutdown_drain",
    }
}

#[derive(Debug, Clone, Copy)]
struct MountedChaosConvergence;

impl Scenario for MountedChaosConvergence {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_CHAOS_CONVERGENCE,
            description: "seeded mounted multi-client chaos convergence scenario",
            requires_studiofs_bin: true,
            requires_mount: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut driver = RealMountedChaosDriver(MultiClientController::new(
                config.clone(),
                SystemClientProcessExecutor,
            ));
            run_mounted_chaos_with_driver(
                context,
                &mut driver,
                CHAOS_CONVERGENCE_TIMEOUT,
                CHAOS_CONVERGENCE_POLL_INTERVAL,
            )?;
        } else {
            write_mounted_chaos_plan_artifacts(context)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context.scenario_dir.join("mounted-chaos-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: MountedChaosSummary = serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": MOUNTED_CHAOS_CONVERGENCE,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged
                && summary.namespace_violations.is_empty() { "passed" } else { "failed" },
            "rounds": summary.rounds,
            "accepted_operations": summary.accepted_operations,
            "rejected_operations": summary.rejected_operations,
            "final_validator": summary.final_validator,
            "namespace_violations": summary.namespace_violations,
            "operation_log": summary.operation_log,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!("artifacts/scenarios/{MOUNTED_CHAOS_CONVERGENCE}/mounted-chaos-summary.json"),
            format!("artifacts/scenarios/{MOUNTED_CHAOS_CONVERGENCE}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

trait MountedChaosDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn after_initial_tree(&mut self, _clients: &[ClientSpec]) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn after_round(&mut self, _round: usize, _clients: &[ClientSpec]) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealMountedChaosDriver(MultiClientController<SystemClientProcessExecutor>);

impl MountedChaosDriver for RealMountedChaosDriver {
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
struct MountedChaosOperation {
    round: usize,
    order: usize,
    client: String,
    operation: String,
    path: String,
    shared_operation: SharedOperation,
    result: String,
    timestamp_ms: u64,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum MountedChaosAction {
    Rename {
        from: String,
        to: String,
    },
    Unlink {
        path: String,
    },
    Append {
        path: String,
        payload: Vec<u8>,
    },
    Write {
        path: String,
        payload: Vec<u8>,
    },
    CreateFile {
        path: String,
        payload: Vec<u8>,
    },
    Mkdir {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Read {
        path: String,
    },
    ReadLoop {
        path: String,
    },
    ProbeLoop {
        dir: String,
        stat_path: String,
        read_path: String,
    },
    Stat {
        path: String,
    },
    Readdir {
        path: String,
    },
    #[cfg(test)]
    Sleep {
        millis: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MountedChaosTask {
    round: usize,
    order: usize,
    client: ClientSpec,
    operation: String,
    path: String,
    shared_operation: SharedOperation,
    action: MountedChaosAction,
}

impl MountedChaosTask {
    fn new(
        round: usize,
        order: usize,
        client: ClientSpec,
        operation: impl Into<String>,
        path: impl Into<String>,
        shared_operation: SharedOperation,
        action: MountedChaosAction,
    ) -> Self {
        Self {
            round,
            order,
            client,
            operation: operation.into(),
            path: path.into(),
            shared_operation,
            action,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MountedChaosSummary {
    scenario: String,
    seed: u64,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: usize,
    rounds: usize,
    operations: Vec<MountedChaosOperation>,
    accepted_operations: usize,
    rejected_operations: usize,
    operation_log: String,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChaosFamily {
    RenameVsRename,
    RenameVsUnlink,
    UnlinkVsAppend,
    OverwriteVsRead,
    MkdirRmdirVsCreate,
    ReaddirStatDuringMutation,
}

impl ChaosFamily {
    fn for_round(seed: u64, round: usize) -> Self {
        match (round + seed as usize) % 6 {
            0 => Self::RenameVsRename,
            1 => Self::RenameVsUnlink,
            2 => Self::UnlinkVsAppend,
            3 => Self::OverwriteVsRead,
            4 => Self::MkdirRmdirVsCreate,
            _ => Self::ReaddirStatDuringMutation,
        }
    }
}

#[derive(Debug, Clone)]
struct ChaosRng {
    state: u64,
}

impl ChaosRng {
    fn new(seed: u64) -> Self {
        let state = seed ^ 0x9e37_79b9_7f4a_7c15;
        Self {
            state: if state == 0 {
                0xa5a5_5a5a_d3c1_b2a0
            } else {
                state
            },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        if upper == 0 {
            0
        } else {
            (self.next_u64() as usize) % upper
        }
    }
}

fn run_mounted_chaos_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn MountedChaosDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedChaosSummary, ScenarioError> {
    let result = run_mounted_chaos_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::MountedChaosUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_mounted_chaos_body(
    context: &ScenarioContext,
    driver: &mut dyn MountedChaosDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedChaosSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: CHAOS_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < CHAOS_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientChaosClients {
            expected: CHAOS_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }
    let clients = clients
        .into_iter()
        .take(CHAOS_CLIENT_NAMES.len())
        .collect::<Vec<_>>();

    establish_mounted_chaos_initial_tree(context, &clients[0])?;
    driver.after_initial_tree(&clients)?;
    let mounts = client_mounts(context, &clients);
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("initial-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let started = Instant::now();
    let mut operations = Vec::new();
    for round in 0..CHAOS_ROUNDS {
        let shuffled = shuffled_clients(context.seed, round, &clients);
        let tasks = mounted_chaos_tasks(context, round, &shuffled);
        let planned_operations = planned_mounted_chaos_operations_from_tasks(started, &tasks);
        write_mounted_chaos_operations(context, &planned_operations)?;
        let started_operations = started_mounted_chaos_operations_from_tasks(started, &tasks);
        write_mounted_chaos_operations(context, &started_operations)?;
        let mut round_operations = run_concurrent_mounted_chaos_operations(started, tasks)?;
        write_mounted_chaos_operations(context, &round_operations)?;
        operations.append(&mut round_operations);
        driver.after_round(round, &clients)?;
    }

    finish_mounted_chaos(
        context,
        Some(bootstrap),
        clients,
        operations,
        convergence_timeout,
        poll_interval,
    )
}

fn establish_mounted_chaos_initial_tree(
    context: &ScenarioContext,
    client: &ClientSpec,
) -> Result<(), ScenarioError> {
    let relative_root = run_unique_dir(context, "chaos");
    let root = client.mount_point.join(&relative_root);
    fs::create_dir_all(&root)?;
    for round in 0..CHAOS_ROUNDS {
        write_mounted_chaos_initial_file(
            root.join(format!("race-{round:02}.txt")),
            &chaos_payload(context, round, "race-initial"),
        )?;
        write_mounted_chaos_initial_file(
            root.join(format!("data-{round:02}.txt")),
            &chaos_payload(context, round, "data-initial"),
        )?;
        fs::create_dir_all(root.join(format!("tree-{round:02}")))?;
        let scan_dir = root.join(format!("scan-{round:02}"));
        fs::create_dir_all(&scan_dir)?;
        write_mounted_chaos_initial_file(
            scan_dir.join("a.txt"),
            &chaos_payload(context, round, "scan-a"),
        )?;
        write_mounted_chaos_initial_file(
            scan_dir.join("b.txt"),
            &chaos_payload(context, round, "scan-b"),
        )?;
    }
    Ok(())
}

fn write_mounted_chaos_initial_file(path: PathBuf, payload: &[u8]) -> Result<(), ScenarioError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = mounted_write_barrier_open_options();
    let mut file = options.create_new(true).open(&path)?;
    file.write_all(payload)?;
    file.sync_all()?;
    drop(file);
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "mounted chaos initial write readback mismatch for {}: expected {} bytes, read {} bytes{}",
            path.display(),
            payload.len(),
            readback.len(),
            if readback.is_empty() {
                "; no payload became readable before timeout"
            } else {
                ""
            }
        ))));
    }
    Ok(())
}

fn wait_for_mounted_payload_readback(
    path: &Path,
    payload: &[u8],
    timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<u8>, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let mut last_readback = Vec::new();
    loop {
        match fs::read(path) {
            Ok(readback) if readback == payload => return Ok(readback),
            Ok(readback) => last_readback = readback,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            return Ok(last_readback);
        }
        thread::sleep(std::cmp::max(poll_interval, Duration::from_millis(1)));
    }
}

fn perform_mounted_chaos_round(
    context: &ScenarioContext,
    started: Instant,
    round: usize,
    clients: &[ClientSpec],
) -> Result<Vec<MountedChaosOperation>, ScenarioError> {
    let shuffled = shuffled_clients(context.seed, round, clients);
    let tasks = mounted_chaos_tasks(context, round, &shuffled);
    run_concurrent_mounted_chaos_operations(started, tasks)
}

fn shuffled_clients(seed: u64, round: usize, clients: &[ClientSpec]) -> Vec<ClientSpec> {
    let round_mix = (round as u64 + 1).wrapping_mul(0x517c_c1b7_2722_0a95);
    let mut rng = ChaosRng::new(seed ^ round_mix);
    let mut shuffled = clients.to_vec();
    for index in (1..shuffled.len()).rev() {
        let swap = rng.next_usize(index + 1);
        shuffled.swap(index, swap);
    }
    shuffled
}

fn mounted_chaos_tasks(
    context: &ScenarioContext,
    round: usize,
    clients: &[ClientSpec],
) -> Vec<MountedChaosTask> {
    let c0 = clients[0].clone();
    let c1 = clients[1].clone();
    let c2 = clients[2].clone();
    let root = run_unique_dir(context, "chaos");
    match ChaosFamily::for_round(context.seed, round) {
        ChaosFamily::RenameVsRename => {
            let source = format!("{root}/race-{round:02}.txt");
            let dest_a = format!("{root}/race-{round:02}-{}.txt", c0.name);
            let dest_b = format!("{root}/race-{round:02}-{}.txt", c1.name);
            vec![
                rename_task(round, 1, c0, source.clone(), dest_a),
                rename_task(round, 2, c1, source.clone(), dest_b),
                stat_task(round, 3, c2, source),
            ]
        }
        ChaosFamily::RenameVsUnlink => {
            let source = format!("{root}/race-{round:02}.txt");
            let dest = format!("{root}/race-{round:02}-renamed.txt");
            vec![
                rename_task(round, 1, c0, source.clone(), dest),
                unlink_task(round, 2, c1, source.clone()),
                stat_task(round, 3, c2, source),
            ]
        }
        ChaosFamily::UnlinkVsAppend => {
            let path = format!("{root}/data-{round:02}.txt");
            vec![
                unlink_task(round, 1, c0, path.clone()),
                append_task(context, round, 2, c1, path.clone()),
                read_task(round, 3, c2, path),
            ]
        }
        ChaosFamily::OverwriteVsRead => {
            let path = format!("{root}/data-{round:02}.txt");
            vec![
                read_loop_task(round, 1, c0, path.clone()),
                write_task(context, round, 2, c1, path.clone()),
                read_loop_task(round, 3, c2, path),
            ]
        }
        ChaosFamily::MkdirRmdirVsCreate => {
            let dir = format!("{root}/tree-{round:02}");
            let subdir = format!("{dir}/subdir");
            let child = format!("{dir}/child.txt");
            vec![
                mkdir_task(round, 1, c0, subdir),
                create_file_task(context, round, 2, c1, child),
                rmdir_task(round, 3, c2, dir),
            ]
        }
        ChaosFamily::ReaddirStatDuringMutation => {
            let dir = format!("{root}/scan-{round:02}");
            let source = format!("{dir}/a.txt");
            let dest = format!("{dir}/c.txt");
            let stat = format!("{dir}/b.txt");
            vec![
                readdir_task(round, 1, c0, dir),
                rename_task(round, 2, c1, source, dest),
                stat_task(round, 3, c2, stat),
            ]
        }
    }
}

fn rename_task(
    round: usize,
    order: usize,
    client: ClientSpec,
    from: String,
    to: String,
) -> MountedChaosTask {
    let path = format!("{from} -> {to}");
    MountedChaosTask::new(
        round,
        order,
        client,
        "rename",
        path,
        SharedOperation::Rename {
            from: from.clone(),
            to: to.clone(),
        },
        MountedChaosAction::Rename { from, to },
    )
}

fn unlink_task(round: usize, order: usize, client: ClientSpec, path: String) -> MountedChaosTask {
    MountedChaosTask::new(
        round,
        order,
        client,
        "unlink",
        path.clone(),
        SharedOperation::Unlink { path: path.clone() },
        MountedChaosAction::Unlink { path },
    )
}

fn append_task(
    context: &ScenarioContext,
    round: usize,
    order: usize,
    client: ClientSpec,
    path: String,
) -> MountedChaosTask {
    let payload = chaos_payload(context, round, "append");
    let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    MountedChaosTask::new(
        round,
        order,
        client,
        "append",
        path.clone(),
        SharedOperation::Append {
            path: path.clone(),
            bytes,
            checksum_sha256: Some(bytes_sha256(&payload)),
        },
        MountedChaosAction::Append { path, payload },
    )
}

fn write_task(
    context: &ScenarioContext,
    round: usize,
    order: usize,
    client: ClientSpec,
    path: String,
) -> MountedChaosTask {
    let payload = chaos_payload(context, round, "overwrite");
    let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    MountedChaosTask::new(
        round,
        order,
        client,
        "write",
        path.clone(),
        SharedOperation::Write {
            path: path.clone(),
            offset: Some(0),
            bytes,
            checksum_sha256: Some(bytes_sha256(&payload)),
        },
        MountedChaosAction::Write { path, payload },
    )
}

fn create_file_task(
    context: &ScenarioContext,
    round: usize,
    order: usize,
    client: ClientSpec,
    path: String,
) -> MountedChaosTask {
    let payload = chaos_payload(context, round, "create");
    let bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    MountedChaosTask::new(
        round,
        order,
        client,
        "create",
        path.clone(),
        SharedOperation::CreateFile {
            path: path.clone(),
            bytes: Some(bytes),
        },
        MountedChaosAction::CreateFile { path, payload },
    )
}

fn mkdir_task(round: usize, order: usize, client: ClientSpec, path: String) -> MountedChaosTask {
    MountedChaosTask::new(
        round,
        order,
        client,
        "mkdir",
        path.clone(),
        SharedOperation::CreateDirectory { path: path.clone() },
        MountedChaosAction::Mkdir { path },
    )
}

fn rmdir_task(round: usize, order: usize, client: ClientSpec, path: String) -> MountedChaosTask {
    MountedChaosTask::new(
        round,
        order,
        client,
        "rmdir",
        path.clone(),
        SharedOperation::Rmdir { path: path.clone() },
        MountedChaosAction::Rmdir { path },
    )
}

fn read_task(round: usize, order: usize, client: ClientSpec, path: String) -> MountedChaosTask {
    MountedChaosTask::new(
        round,
        order,
        client,
        "read",
        path.clone(),
        SharedOperation::Read {
            path: path.clone(),
            bytes: None,
            checksum_sha256: None,
        },
        MountedChaosAction::Read { path },
    )
}

fn read_loop_task(
    round: usize,
    order: usize,
    client: ClientSpec,
    path: String,
) -> MountedChaosTask {
    MountedChaosTask::new(
        round,
        order,
        client,
        "read_loop",
        path.clone(),
        SharedOperation::Read {
            path: path.clone(),
            bytes: None,
            checksum_sha256: None,
        },
        MountedChaosAction::ReadLoop { path },
    )
}

fn stat_task(round: usize, order: usize, client: ClientSpec, path: String) -> MountedChaosTask {
    MountedChaosTask::new(
        round,
        order,
        client,
        "stat",
        path.clone(),
        SharedOperation::Stat { path: path.clone() },
        MountedChaosAction::Stat { path },
    )
}

fn readdir_task(round: usize, order: usize, client: ClientSpec, path: String) -> MountedChaosTask {
    MountedChaosTask::new(
        round,
        order,
        client,
        "readdir",
        path.clone(),
        SharedOperation::Readdir { path: path.clone() },
        MountedChaosAction::Readdir { path },
    )
}

fn run_concurrent_mounted_chaos_operations(
    started: Instant,
    tasks: Vec<MountedChaosTask>,
) -> Result<Vec<MountedChaosOperation>, ScenarioError> {
    #[cfg(test)]
    {
        let launcher = InProcessMountedChaosProcessLauncher;
        run_concurrent_mounted_chaos_operations_with_launcher(
            started,
            tasks,
            CHAOS_OPERATION_TIMEOUT,
            &launcher,
        )
    }
    #[cfg(not(test))]
    {
        let launcher = SystemMountedChaosProcessLauncher::new()?;
        run_concurrent_mounted_chaos_operations_with_launcher(
            started,
            tasks,
            CHAOS_OPERATION_TIMEOUT,
            &launcher,
        )
    }
}

fn run_concurrent_mounted_chaos_operations_with_launcher(
    started: Instant,
    tasks: Vec<MountedChaosTask>,
    timeout: Duration,
    launcher: &dyn MountedChaosProcessLauncher,
) -> Result<Vec<MountedChaosOperation>, ScenarioError> {
    let round = tasks.first().map(|task| task.round).unwrap_or_default();
    let work_dir = std::env::temp_dir().join(format!(
        "studiofs-e2e-chaos-{}-{}",
        std::process::id(),
        started.elapsed().as_nanos()
    ));
    fs::create_dir_all(&work_dir)?;
    let start_file = work_dir.join("start");
    let result = (|| {
        let mut processes = Vec::with_capacity(tasks.len());
        let mut launch_errors = Vec::new();
        for task in &tasks {
            let label = mounted_chaos_task_label(task);
            match launcher.launch(task, &start_file, &work_dir) {
                Ok(process) => processes.push(LaunchedMountedChaosProcess {
                    label: label.clone(),
                    process,
                    done: false,
                }),
                Err(error) => launch_errors.push(format!("{label}: {error}")),
            }
        }
        if !launch_errors.is_empty() {
            let pending = kill_pending_mounted_chaos_processes(&mut processes);
            if !pending.is_empty() {
                launch_errors.push(format!(
                    "launched helpers killed after launch failure: {pending:?}"
                ));
            }
            return Err(ScenarioError::MountedChaosLaunchFailures {
                errors: launch_errors,
            });
        }
        fs::write(&start_file, b"start")?;

        let deadline = Instant::now() + timeout;
        let mut operations = Vec::with_capacity(processes.len());
        while operations.len() < processes.len() {
            for process in processes.iter_mut().filter(|process| !process.done) {
                match process.process.try_collect(started) {
                    Ok(Some(operation)) => {
                        process.done = true;
                        operations.push(operation);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let pending = kill_pending_mounted_chaos_processes(&mut processes);
                        if pending.is_empty() {
                            return Err(error);
                        }
                        return Err(ScenarioError::MountedChaosHelper(format!(
                            "{error}; pending helpers killed after collect failure: {pending:?}"
                        )));
                    }
                }
            }
            if operations.len() == processes.len() {
                break;
            }
            if Instant::now() >= deadline {
                let pending = kill_pending_mounted_chaos_processes(&mut processes);
                return Err(ScenarioError::MountedChaosOperationTimeout {
                    round,
                    timeout_ms: timeout.as_millis(),
                    pending,
                });
            }
            thread::sleep(Duration::from_millis(5));
        }
        operations.sort_by_key(|operation| (operation.round, operation.order));
        Ok(operations)
    })();
    match (result, fs::remove_dir_all(&work_dir)) {
        (Ok(operations), Ok(())) => Ok(operations),
        (Ok(_), Err(cleanup)) => Err(cleanup.into()),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => {
            eprintln!(
                "mounted chaos helper work dir cleanup failed after primary error: {}; cleanup: {}",
                primary, cleanup
            );
            Err(primary)
        }
    }
}

struct LaunchedMountedChaosProcess {
    label: String,
    process: Box<dyn MountedChaosProcess>,
    done: bool,
}

fn kill_pending_mounted_chaos_processes(
    processes: &mut [LaunchedMountedChaosProcess],
) -> Vec<String> {
    let mut pending = Vec::new();
    for process in processes.iter_mut().filter(|process| !process.done) {
        pending.push(process.label.clone());
        if let Err(error) = process.process.kill() {
            pending.push(format!("{} (kill failed: {error})", process.label));
        }
    }
    pending
}

trait MountedChaosProcessLauncher {
    fn launch(
        &self,
        task: &MountedChaosTask,
        start_file: &Path,
        work_dir: &Path,
    ) -> Result<Box<dyn MountedChaosProcess>, ScenarioError>;
}

trait MountedChaosProcess {
    fn try_collect(
        &mut self,
        started: Instant,
    ) -> Result<Option<MountedChaosOperation>, ScenarioError>;
    fn kill(&mut self) -> io::Result<()>;
}

#[cfg(not(test))]
struct SystemMountedChaosProcessLauncher {
    helper_bin: PathBuf,
}

#[cfg(not(test))]
impl SystemMountedChaosProcessLauncher {
    fn new() -> Result<Self, ScenarioError> {
        Ok(Self {
            helper_bin: std::env::current_exe()?,
        })
    }
}

#[cfg(not(test))]
impl MountedChaosProcessLauncher for SystemMountedChaosProcessLauncher {
    fn launch(
        &self,
        task: &MountedChaosTask,
        start_file: &Path,
        work_dir: &Path,
    ) -> Result<Box<dyn MountedChaosProcess>, ScenarioError> {
        let task_path = work_dir.join(format!(
            "task-{}-{}-{}.json",
            task.round, task.order, task.client.name
        ));
        let stdout_path = work_dir.join(format!(
            "task-{}-{}-{}.stdout.json",
            task.round, task.order, task.client.name
        ));
        let stderr_path = work_dir.join(format!(
            "task-{}-{}-{}.stderr.log",
            task.round, task.order, task.client.name
        ));
        fs::write(&task_path, serde_json::to_vec(task)?)?;
        let stdout = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&stdout_path)?;
        let stderr = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&stderr_path)?;
        let child = Command::new(&self.helper_bin)
            .arg("__mounted-chaos-op")
            .arg("--task")
            .arg(&task_path)
            .arg("--start-file")
            .arg(start_file)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        Ok(Box::new(SystemMountedChaosProcess {
            child,
            task: task.clone(),
            stdout_path,
            stderr_path,
        }))
    }
}

#[cfg(not(test))]
struct SystemMountedChaosProcess {
    child: Child,
    task: MountedChaosTask,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

#[cfg(not(test))]
impl MountedChaosProcess for SystemMountedChaosProcess {
    fn try_collect(
        &mut self,
        started: Instant,
    ) -> Result<Option<MountedChaosOperation>, ScenarioError> {
        let Some(status) = self.child.try_wait()? else {
            return Ok(None);
        };
        let stdout = fs::read_to_string(&self.stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&self.stderr_path).unwrap_or_default();
        let parsed = stdout
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .and_then(|line| serde_json::from_str::<MountedChaosOperation>(line).ok());
        let mut operation = parsed.unwrap_or_else(|| MountedChaosOperation {
            round: self.task.round,
            order: self.task.order,
            client: self.task.client.name.clone(),
            operation: self.task.operation.clone(),
            path: self.task.path.clone(),
            shared_operation: self.task.shared_operation.clone(),
            result: "error".to_string(),
            timestamp_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            error: Some(format!(
                "mounted chaos helper exited without a valid operation record: status={status}, stderr={}",
                stderr.trim()
            )),
        });
        operation.timestamp_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(Some(operation))
    }

    fn kill(&mut self) -> io::Result<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }

        #[cfg(windows)]
        {
            let pid = self.child.id();
            let pid_arg = pid.to_string();
            let tree_result = Command::new("taskkill")
                .args(["/PID", &pid_arg, "/T", "/F"])
                .output();
            let primary_result = self.child.kill();
            match (tree_result, primary_result) {
                (Ok(output), _)
                    if crate::client::windows_taskkill_status_is_success(output.status.code()) =>
                {
                    Ok(())
                }
                (Ok(_), Ok(())) if !crate::client::windows_pid_is_running(pid) => Ok(()),
                (Ok(output), Ok(())) => Err(io::Error::other(format!(
                    "failed to terminate mounted chaos helper {}: taskkill exit {}; stdout: {}; stderr: {}; primary kill: ok",
                    self.task.operation,
                    output.status.code().unwrap_or(-1),
                    String::from_utf8_lossy(&output.stdout).trim(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ))),
                (Ok(output), Err(primary)) => Err(io::Error::other(format!(
                    "failed to terminate mounted chaos helper {}: taskkill exit {}; stdout: {}; stderr: {}; primary kill: {primary}",
                    self.task.operation,
                    output.status.code().unwrap_or(-1),
                    String::from_utf8_lossy(&output.stdout).trim(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ))),
                (Err(tree), Ok(())) => Err(io::Error::other(format!(
                    "failed to terminate mounted chaos helper {}: taskkill failed: {tree}; primary kill: ok",
                    self.task.operation
                ))),
                (Err(tree), Err(primary)) => Err(io::Error::other(format!(
                    "failed to terminate mounted chaos helper {}: taskkill failed: {tree}; primary kill: {primary}",
                    self.task.operation
                ))),
            }
        }

        #[cfg(not(windows))]
        {
            self.child.kill()?;

            let deadline = Instant::now() + CHAOS_HELPER_KILL_WAIT_TIMEOUT;
            while self.child.try_wait()?.is_none() {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "mounted chaos helper {} did not exit after force termination within {} ms",
                            self.task.operation,
                            CHAOS_HELPER_KILL_WAIT_TIMEOUT.as_millis()
                        ),
                    ));
                }
                thread::sleep(Duration::from_millis(5));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
struct InProcessMountedChaosProcessLauncher;

#[cfg(test)]
impl MountedChaosProcessLauncher for InProcessMountedChaosProcessLauncher {
    fn launch(
        &self,
        task: &MountedChaosTask,
        _start_file: &Path,
        _work_dir: &Path,
    ) -> Result<Box<dyn MountedChaosProcess>, ScenarioError> {
        Ok(Box::new(InProcessMountedChaosProcess {
            task: Some(task.clone()),
            result: None,
        }))
    }
}

#[cfg(test)]
struct InProcessMountedChaosProcess {
    task: Option<MountedChaosTask>,
    result: Option<MountedChaosOperation>,
}

#[cfg(test)]
impl MountedChaosProcess for InProcessMountedChaosProcess {
    fn try_collect(
        &mut self,
        started: Instant,
    ) -> Result<Option<MountedChaosOperation>, ScenarioError> {
        if self.result.is_none() {
            let task = self
                .task
                .take()
                .expect("task must be present before collect");
            self.result = Some(record_chaos_fs_operation(started, task));
        }
        Ok(self.result.take())
    }

    fn kill(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn run_mounted_chaos_operation_helper(
    task_path: &Path,
    start_file: &Path,
) -> Result<(), ScenarioError> {
    let task: MountedChaosTask = serde_json::from_slice(&fs::read(task_path)?)?;
    let deadline = Instant::now() + CHAOS_HELPER_START_TIMEOUT;
    while !start_file.exists() {
        if Instant::now() >= deadline {
            return Err(ScenarioError::MountedChaosHelper(format!(
                "timed out waiting for chaos helper start file {}",
                start_file.display()
            )));
        }
        thread::sleep(Duration::from_millis(5));
    }
    let operation = record_chaos_fs_operation(Instant::now(), task);
    println!("{}", serde_json::to_string(&operation)?);
    Ok(())
}

fn mounted_chaos_task_label(task: &MountedChaosTask) -> String {
    format!("{} {} {}", task.client.name, task.operation, task.path)
}

fn record_chaos_fs_operation(started: Instant, task: MountedChaosTask) -> MountedChaosOperation {
    match execute_mounted_chaos_action(&task.client, &task.action) {
        Ok(()) => MountedChaosOperation {
            round: task.round,
            order: task.order,
            client: task.client.name,
            operation: task.operation.to_string(),
            path: task.path,
            shared_operation: task.shared_operation,
            result: "accepted".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: None,
        },
        Err(error) => MountedChaosOperation {
            round: task.round,
            order: task.order,
            client: task.client.name,
            operation: task.operation.to_string(),
            path: task.path,
            shared_operation: task.shared_operation,
            result: "rejected".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: Some(error.to_string()),
        },
    }
}

fn execute_mounted_chaos_action(
    client: &ClientSpec,
    action: &MountedChaosAction,
) -> io::Result<()> {
    match action {
        MountedChaosAction::Rename { from, to } => {
            fs::rename(client.mount_point.join(from), client.mount_point.join(to))
        }
        MountedChaosAction::Unlink { path } => fs::remove_file(client.mount_point.join(path)),
        MountedChaosAction::Append { path, payload } => {
            let mut options = mounted_write_barrier_open_options();
            let mut file = options
                .create(true)
                .append(true)
                .open(client.mount_point.join(path))?;
            file.write_all(payload)?;
            file.sync_all()
        }
        MountedChaosAction::Write { path, payload }
        | MountedChaosAction::CreateFile { path, payload } => {
            write_payload_with_publication_barrier(&client.mount_point.join(path), payload)
        }
        MountedChaosAction::Mkdir { path } => fs::create_dir_all(client.mount_point.join(path)),
        MountedChaosAction::Rmdir { path } => fs::remove_dir(client.mount_point.join(path)),
        MountedChaosAction::Read { path } => fs::read(client.mount_point.join(path)).map(|_| ()),
        MountedChaosAction::ReadLoop { path } => repeated_chaos_read(client, path),
        MountedChaosAction::ProbeLoop {
            dir,
            stat_path,
            read_path,
        } => repeated_hotspot_probe(client, dir, stat_path, read_path),
        MountedChaosAction::Stat { path } => {
            fs::metadata(client.mount_point.join(path)).map(|_| ())
        }
        MountedChaosAction::Readdir { path } => {
            fs::read_dir(client.mount_point.join(path)).map(|_| ())
        }
        #[cfg(test)]
        MountedChaosAction::Sleep { millis } => {
            thread::sleep(Duration::from_millis(*millis));
            Ok(())
        }
    }
}

fn write_payload_with_publication_barrier(path: &Path, payload: &[u8]) -> io::Result<()> {
    let mut options = mounted_write_barrier_open_options();
    let mut file = options.create(true).truncate(true).open(path)?;
    file.set_len(0)?;
    file.write_all(payload)?;
    file.sync_all()
}

fn mounted_write_barrier_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
        options.custom_flags(FILE_FLAG_WRITE_THROUGH);
    }
    options
}

fn repeated_chaos_read(client: &ClientSpec, relative_path: &str) -> io::Result<()> {
    for _ in 0..CHAOS_READ_ATTEMPTS {
        fs::read(client.mount_point.join(relative_path))?;
        thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn repeated_hotspot_probe(
    client: &ClientSpec,
    dir: &str,
    stat_path: &str,
    read_path: &str,
) -> io::Result<()> {
    for _ in 0..HOTSPOT_PROBE_ATTEMPTS {
        let mut entries_seen = 0;
        for entry in fs::read_dir(client.mount_point.join(dir))? {
            entry?;
            entries_seen += 1;
        }
        if entries_seen == 0 {
            return Err(io::Error::other(format!(
                "hotspot probe observed empty directory listing for {dir}"
            )));
        }
        fs::metadata(client.mount_point.join(stat_path))?;
        let payload = fs::read(client.mount_point.join(read_path))?;
        validate_hotspot_payload(read_path, &payload)?;
        thread::sleep(HOTSPOT_PROBE_INTERVAL);
    }
    Ok(())
}

fn validate_hotspot_payload(relative_path: &str, payload: &[u8]) -> io::Result<()> {
    let text = std::str::from_utf8(payload).map_err(|error| {
        io::Error::other(format!(
            "hotspot probe read non-UTF-8 payload from {relative_path}: {error}"
        ))
    })?;
    if text.starts_with("studiofs hotspot seed=") || text.starts_with("studiofs chaos seed=") {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "hotspot probe read unexpected payload from {relative_path}"
        )))
    }
}

fn write_mounted_chaos_plan_artifacts(
    context: &ScenarioContext,
) -> Result<MountedChaosSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let mut operations = Vec::new();
    for round in 0..CHAOS_ROUNDS {
        let mut round_operations = planned_mounted_chaos_round(context, started, round);
        write_mounted_chaos_operations(context, &round_operations)?;
        operations.append(&mut round_operations);
    }
    let summary = mounted_chaos_summary(
        context,
        None,
        CHAOS_CLIENT_NAMES.len(),
        operations,
        TreeComparison {
            converged: true,
            reference_client: Some(CHAOS_CLIENT_NAMES[0].to_string()),
            compared_clients: CHAOS_CLIENT_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            differences: Vec::new(),
        },
        Vec::new(),
    );
    write_mounted_chaos_summary(context, &summary)?;
    Ok(summary)
}

fn planned_mounted_chaos_round(
    context: &ScenarioContext,
    started: Instant,
    round: usize,
) -> Vec<MountedChaosOperation> {
    let clients = shuffled_client_names(context.seed, round);
    let spec = planned_mounted_chaos_spec(context, round, &clients);
    spec.into_iter()
        .enumerate()
        .map(
            |(index, (client, operation, path, shared_operation))| MountedChaosOperation {
                round,
                order: index + 1,
                client,
                operation: operation.to_string(),
                path,
                shared_operation,
                result: "planned".to_string(),
                timestamp_ms: elapsed_millis_u64(started),
                error: None,
            },
        )
        .collect()
}

fn planned_mounted_chaos_operations_from_tasks(
    started: Instant,
    tasks: &[MountedChaosTask],
) -> Vec<MountedChaosOperation> {
    tasks
        .iter()
        .map(|task| mounted_chaos_operation_from_task(started, task, "planned", None))
        .collect()
}

fn started_mounted_chaos_operations_from_tasks(
    started: Instant,
    tasks: &[MountedChaosTask],
) -> Vec<MountedChaosOperation> {
    tasks
        .iter()
        .map(|task| mounted_chaos_operation_from_task(started, task, "started", None))
        .collect()
}

fn mounted_chaos_operation_from_task(
    started: Instant,
    task: &MountedChaosTask,
    result: &str,
    error: Option<String>,
) -> MountedChaosOperation {
    MountedChaosOperation {
        round: task.round,
        order: task.order,
        client: task.client.name.clone(),
        operation: task.operation.to_string(),
        path: task.path.clone(),
        shared_operation: task.shared_operation.clone(),
        result: result.to_string(),
        timestamp_ms: elapsed_millis_u64(started),
        error,
    }
}

fn shuffled_client_names(seed: u64, round: usize) -> Vec<String> {
    let mut names = CHAOS_CLIENT_NAMES
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let round_mix = (round as u64 + 1).wrapping_mul(0x517c_c1b7_2722_0a95);
    let mut rng = ChaosRng::new(seed ^ round_mix);
    for index in (1..names.len()).rev() {
        let swap = rng.next_usize(index + 1);
        names.swap(index, swap);
    }
    names
}

fn planned_mounted_chaos_spec(
    context: &ScenarioContext,
    round: usize,
    clients: &[String],
) -> Vec<(String, &'static str, String, SharedOperation)> {
    let root = run_unique_dir(context, "chaos");
    match ChaosFamily::for_round(context.seed, round) {
        ChaosFamily::RenameVsRename => {
            let source = format!("{root}/race-{round:02}.txt");
            let dest_a = format!("{root}/race-{round:02}-{}.txt", clients[0]);
            let dest_b = format!("{root}/race-{round:02}-{}.txt", clients[1]);
            vec![
                (
                    clients[0].clone(),
                    "rename",
                    format!("{source} -> {dest_a}"),
                    SharedOperation::Rename {
                        from: source.clone(),
                        to: dest_a,
                    },
                ),
                (
                    clients[1].clone(),
                    "rename",
                    format!("{source} -> {dest_b}"),
                    SharedOperation::Rename {
                        from: source.clone(),
                        to: dest_b,
                    },
                ),
                (
                    clients[2].clone(),
                    "stat",
                    source.clone(),
                    SharedOperation::Stat { path: source },
                ),
            ]
        }
        ChaosFamily::RenameVsUnlink => {
            let source = format!("{root}/race-{round:02}.txt");
            let dest = format!("{root}/race-{round:02}-renamed.txt");
            vec![
                (
                    clients[0].clone(),
                    "rename",
                    format!("{source} -> {dest}"),
                    SharedOperation::Rename {
                        from: source.clone(),
                        to: dest,
                    },
                ),
                (
                    clients[1].clone(),
                    "unlink",
                    source.clone(),
                    SharedOperation::Unlink {
                        path: source.clone(),
                    },
                ),
                (
                    clients[2].clone(),
                    "stat",
                    source.clone(),
                    SharedOperation::Stat { path: source },
                ),
            ]
        }
        ChaosFamily::UnlinkVsAppend => {
            let path = format!("{root}/data-{round:02}.txt");
            let append_payload = chaos_payload(context, round, "append");
            vec![
                (
                    clients[0].clone(),
                    "unlink",
                    path.clone(),
                    SharedOperation::Unlink { path: path.clone() },
                ),
                (
                    clients[1].clone(),
                    "append",
                    path.clone(),
                    SharedOperation::Append {
                        path: path.clone(),
                        bytes: u64::try_from(append_payload.len()).unwrap_or(u64::MAX),
                        checksum_sha256: Some(bytes_sha256(&append_payload)),
                    },
                ),
                (
                    clients[2].clone(),
                    "read",
                    path.clone(),
                    SharedOperation::Read {
                        path,
                        bytes: None,
                        checksum_sha256: None,
                    },
                ),
            ]
        }
        ChaosFamily::OverwriteVsRead => {
            let path = format!("{root}/data-{round:02}.txt");
            let overwrite_payload = chaos_payload(context, round, "overwrite");
            vec![
                (
                    clients[0].clone(),
                    "read_loop",
                    path.clone(),
                    SharedOperation::Read {
                        path: path.clone(),
                        bytes: None,
                        checksum_sha256: None,
                    },
                ),
                (
                    clients[1].clone(),
                    "write",
                    path.clone(),
                    SharedOperation::Write {
                        path: path.clone(),
                        offset: Some(0),
                        bytes: u64::try_from(overwrite_payload.len()).unwrap_or(u64::MAX),
                        checksum_sha256: Some(bytes_sha256(&overwrite_payload)),
                    },
                ),
                (
                    clients[2].clone(),
                    "read_loop",
                    path.clone(),
                    SharedOperation::Read {
                        path,
                        bytes: None,
                        checksum_sha256: None,
                    },
                ),
            ]
        }
        ChaosFamily::MkdirRmdirVsCreate => {
            let dir = format!("{root}/tree-{round:02}");
            let subdir = format!("{dir}/subdir");
            let child = format!("{dir}/child.txt");
            let payload = chaos_payload(context, round, "create");
            vec![
                (
                    clients[0].clone(),
                    "mkdir",
                    subdir.clone(),
                    SharedOperation::CreateDirectory { path: subdir },
                ),
                (
                    clients[1].clone(),
                    "create",
                    child.clone(),
                    SharedOperation::CreateFile {
                        path: child,
                        bytes: Some(u64::try_from(payload.len()).unwrap_or(u64::MAX)),
                    },
                ),
                (
                    clients[2].clone(),
                    "rmdir",
                    dir.clone(),
                    SharedOperation::Rmdir { path: dir },
                ),
            ]
        }
        ChaosFamily::ReaddirStatDuringMutation => {
            let dir = format!("{root}/scan-{round:02}");
            let source = format!("{dir}/a.txt");
            let dest = format!("{dir}/c.txt");
            let stat = format!("{dir}/b.txt");
            vec![
                (
                    clients[0].clone(),
                    "readdir",
                    dir.clone(),
                    SharedOperation::Readdir { path: dir },
                ),
                (
                    clients[1].clone(),
                    "rename",
                    format!("{source} -> {dest}"),
                    SharedOperation::Rename {
                        from: source,
                        to: dest,
                    },
                ),
                (
                    clients[2].clone(),
                    "stat",
                    stat.clone(),
                    SharedOperation::Stat { path: stat },
                ),
            ]
        }
    }
}

fn finish_mounted_chaos(
    context: &ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    operations: Vec<MountedChaosOperation>,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedChaosSummary, ScenarioError> {
    let convergence = wait_for_convergence(
        &client_mounts(context, &clients),
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let mut namespace_violations = Vec::new();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(snapshot, &[]));
    }
    if !namespace_violations.is_empty() {
        return Err(ScenarioError::MountedChaosInvariantViolations {
            violations: namespace_violations,
        });
    }
    let summary = mounted_chaos_summary(
        context,
        bootstrap,
        clients.len(),
        operations,
        convergence.comparison,
        namespace_violations,
    );
    write_mounted_chaos_summary(context, &summary)?;
    Ok(summary)
}

fn mounted_chaos_summary(
    context: &ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: usize,
    operations: Vec<MountedChaosOperation>,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
) -> MountedChaosSummary {
    let accepted_operations = operations
        .iter()
        .filter(|operation| operation.result == "accepted")
        .count();
    let rejected_operations = operations
        .iter()
        .filter(|operation| operation.result == "rejected")
        .count();
    MountedChaosSummary {
        scenario: MOUNTED_CHAOS_CONVERGENCE.to_string(),
        seed: context.seed,
        bootstrap,
        clients,
        rounds: CHAOS_ROUNDS,
        operations,
        accepted_operations,
        rejected_operations,
        operation_log: format!("artifacts/scenarios/{MOUNTED_CHAOS_CONVERGENCE}/operations.ndjson"),
        final_validator,
        namespace_violations,
    }
}

fn client_mounts(context: &ScenarioContext, clients: &[ClientSpec]) -> Vec<ClientMount> {
    clients
        .iter()
        .map(|client| {
            let mut mount = ClientMount::new(client.name.clone(), client.mount_point.clone());
            if let Some(config) = &context.client_controller_config {
                mount = mount
                    .with_diagnostics_probe(config.studiofs_bin.clone(), client.client_root.clone())
                    .with_remote_metadata_probe(
                        config.meta_dsn.clone(),
                        config.volume_name.clone(),
                    );
            }
            mount
        })
        .collect()
}

fn write_mounted_chaos_operations(
    context: &ScenarioContext,
    operations: &[MountedChaosOperation],
) -> Result<(), ScenarioError> {
    let path = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path)?;
    for operation in operations {
        let mut record = shared_operation_record(
            &writer,
            MOUNTED_CHAOS_CONVERGENCE,
            operation.shared_operation.clone(),
        )
        .with_client(&operation.client)
        .with_timestamp_ms(operation.timestamp_ms)
        .with_outcome(mounted_chaos_outcome(operation))
        .with_metadata("round", operation.round.to_string())
        .with_metadata("operation", operation.operation.clone())
        .with_metadata("result", &operation.result);
        if let Some(error) = &operation.error {
            record = record.with_metadata("error", error);
        }
        append_shared_record(&mut writer, &record)?;
    }
    Ok(())
}

fn mounted_chaos_outcome(operation: &MountedChaosOperation) -> OperationOutcome {
    match operation.result.as_str() {
        "planned" => OperationOutcome::Planned,
        "accepted" => OperationOutcome::Accepted,
        "rejected" => OperationOutcome::Rejected {
            error: operation
                .error
                .clone()
                .unwrap_or_else(|| "operation rejected".to_string()),
        },
        "started" => OperationOutcome::Observed,
        _ => OperationOutcome::Observed,
    }
}

fn write_mounted_chaos_summary(
    context: &ScenarioContext,
    summary: &MountedChaosSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("mounted-chaos-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

fn chaos_payload(context: &ScenarioContext, round: usize, label: &str) -> Vec<u8> {
    format!(
        "studiofs chaos seed={} run_id={} round={round} label={label}\n",
        context.seed, context.run_id
    )
    .into_bytes()
}

#[derive(Debug, Clone, Copy)]
struct MountedHotspotChurnConvergence;

impl Scenario for MountedHotspotChurnConvergence {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_HOTSPOT_CHURN_CONVERGENCE,
            description: "mounted hot-spot directory churn convergence scenario",
            requires_studiofs_bin: true,
            requires_mount: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut driver = RealMountedChaosDriver(MultiClientController::new(
                config.clone(),
                SystemClientProcessExecutor,
            ));
            run_mounted_hotspot_churn_with_driver(
                context,
                &mut driver,
                CHAOS_CONVERGENCE_TIMEOUT,
                CHAOS_CONVERGENCE_POLL_INTERVAL,
            )?;
        } else {
            write_mounted_hotspot_churn_plan_artifacts(context)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("mounted-hotspot-churn-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: MountedHotspotChurnSummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": MOUNTED_HOTSPOT_CHURN_CONVERGENCE,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged
                && summary.namespace_violations.is_empty() { "passed" } else { "failed" },
            "rounds": summary.rounds,
            "working_set": summary.working_set,
            "accepted_operations": summary.accepted_operations,
            "rejected_operations": summary.rejected_operations,
            "probe_operations": summary.probe_operations,
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
                "artifacts/scenarios/{MOUNTED_HOTSPOT_CHURN_CONVERGENCE}/mounted-hotspot-churn-summary.json"
            ),
            format!("artifacts/scenarios/{MOUNTED_HOTSPOT_CHURN_CONVERGENCE}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MountedHotspotChurnSummary {
    scenario: String,
    seed: u64,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: usize,
    rounds: usize,
    working_set: usize,
    observer_client: String,
    operations: Vec<MountedChaosOperation>,
    accepted_operations: usize,
    rejected_operations: usize,
    probe_operations: usize,
    operation_log: String,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
}

fn run_mounted_hotspot_churn_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn MountedChaosDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedHotspotChurnSummary, ScenarioError> {
    let result =
        run_mounted_hotspot_churn_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => {
            Err(ScenarioError::MountedHotspotChurnUnmountAfterFailure {
                primary: primary.to_string(),
                cleanup: cleanup.to_string(),
            })
        }
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_mounted_hotspot_churn_body(
    context: &ScenarioContext,
    driver: &mut dyn MountedChaosDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedHotspotChurnSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: CHAOS_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < CHAOS_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientChaosClients {
            expected: CHAOS_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }
    let clients = clients
        .into_iter()
        .take(CHAOS_CLIENT_NAMES.len())
        .collect::<Vec<_>>();

    establish_hotspot_initial_tree(context, &clients[0])?;
    driver.after_initial_tree(&clients)?;
    wait_for_convergence(
        &client_mounts(context, &clients),
        context.scenario_dir.join("initial-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let started = Instant::now();
    let mut operations = Vec::new();
    for round in 0..HOTSPOT_CHURN_ROUNDS {
        let tasks = hotspot_churn_tasks(context, round, &clients);
        let planned = planned_mounted_chaos_operations_from_tasks(started, &tasks);
        write_mounted_hotspot_churn_operations(context, &planned)?;
        let started_operations = started_mounted_chaos_operations_from_tasks(started, &tasks);
        write_mounted_hotspot_churn_operations(context, &started_operations)?;
        let mut round_operations = run_concurrent_mounted_chaos_operations(started, tasks)?;
        write_mounted_hotspot_churn_operations(context, &round_operations)?;
        operations.append(&mut round_operations);
        driver.after_round(round, &clients)?;
    }

    finish_mounted_hotspot_churn(
        context,
        Some(bootstrap),
        clients,
        operations,
        convergence_timeout,
        poll_interval,
    )
}

fn establish_hotspot_initial_tree(
    context: &ScenarioContext,
    client: &ClientSpec,
) -> Result<(), ScenarioError> {
    let relative_root = run_unique_dir(context, "hotspot");
    let root = client.mount_point.join(&relative_root);
    fs::create_dir_all(&root)?;
    for slot in 0..HOTSPOT_WORKING_SET {
        write_mounted_file(
            &root.join(format!("slot-{slot:02}.txt")),
            &hotspot_payload(context, 0, slot, "initial"),
        )?;
    }
    Ok(())
}

fn hotspot_churn_tasks(
    context: &ScenarioContext,
    round: usize,
    clients: &[ClientSpec],
) -> Vec<MountedChaosTask> {
    assert!(
        clients.len() >= CHAOS_CLIENT_NAMES.len(),
        "hotspot churn requires at least {} clients",
        CHAOS_CLIENT_NAMES.len()
    );
    let writer_a = clients[round % 2].clone();
    let writer_b = clients[(round + 1) % 2].clone();
    let observer = clients[2].clone();
    let slot = round % HOTSPOT_WORKING_SET;
    let next_slot = (round + 1) % HOTSPOT_WORKING_SET;
    let root = run_unique_dir(context, "hotspot");
    let path = hotspot_slot_path(&root, slot);
    let next_path = hotspot_slot_path(&root, next_slot);
    match round % 5 {
        0 => vec![
            write_task(context, round, 1, writer_a, path.clone()),
            create_file_task(context, round, 2, writer_b, next_path.clone()),
            readdir_task(round, 3, observer.clone(), root.clone()),
            stat_task(round, 4, observer.clone(), next_path.clone()),
            read_task(round, 5, observer, path),
        ],
        1 => {
            let renamed = format!("{root}/slot-{slot:02}-renamed.txt");
            vec![
                rename_task(round, 1, writer_a, path.clone(), renamed),
                create_file_task(context, round, 2, writer_b, path.clone()),
                readdir_task(round, 3, observer.clone(), root.clone()),
                stat_task(round, 4, observer.clone(), path.clone()),
                read_task(round, 5, observer, path),
            ]
        }
        2 => vec![
            unlink_task(round, 1, writer_a, path.clone()),
            create_file_task(context, round, 2, writer_b, path.clone()),
            readdir_task(round, 3, observer.clone(), root.clone()),
            stat_task(round, 4, observer.clone(), next_path.clone()),
            read_task(round, 5, observer, next_path),
        ],
        3 => vec![
            append_task(context, round, 1, writer_a, path.clone()),
            write_task(context, round, 2, writer_b, next_path.clone()),
            readdir_task(round, 3, observer.clone(), root.clone()),
            stat_task(round, 4, observer.clone(), path.clone()),
            read_task(round, 5, observer, path),
        ],
        _ => vec![
            create_file_task(context, round, 1, writer_a, path.clone()),
            rename_task(round, 2, writer_b, next_path.clone(), path.clone()),
            MountedChaosTask::new(
                round,
                3,
                observer.clone(),
                "probe_loop",
                root.clone(),
                SharedOperation::Readdir { path: root.clone() },
                MountedChaosAction::ProbeLoop {
                    dir: root.clone(),
                    stat_path: path.clone(),
                    read_path: path,
                },
            ),
            stat_task(round, 4, observer.clone(), next_path.clone()),
            read_task(round, 5, observer, next_path),
        ],
    }
}

fn hotspot_slot_path(root: &str, slot: usize) -> String {
    format!("{root}/slot-{slot:02}.txt")
}

fn finish_mounted_hotspot_churn(
    context: &ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    operations: Vec<MountedChaosOperation>,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedHotspotChurnSummary, ScenarioError> {
    let convergence = wait_for_convergence(
        &client_mounts(context, &clients),
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let namespace_violations = validate_hotspot_churn_namespace(&convergence.snapshots)?;
    let summary = mounted_hotspot_churn_summary(
        context,
        bootstrap,
        clients.len(),
        clients[2].name.clone(),
        operations,
        convergence.comparison,
        namespace_violations,
    );
    write_mounted_hotspot_churn_summary(context, &summary)?;
    Ok(summary)
}

fn validate_hotspot_churn_namespace(
    snapshots: &[crate::mounted_tree::MountedTreeSnapshot],
) -> Result<Vec<NamespaceInvariantViolation>, ScenarioError> {
    let mut namespace_violations = Vec::new();
    for snapshot in snapshots {
        namespace_violations.extend(validate_namespace_invariants(snapshot, &[]));
    }
    if namespace_violations.is_empty() {
        Ok(namespace_violations)
    } else {
        Err(ScenarioError::MountedHotspotChurnInvariantViolations {
            violations: namespace_violations,
        })
    }
}

fn mounted_hotspot_churn_summary(
    context: &ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: usize,
    observer_client: String,
    operations: Vec<MountedChaosOperation>,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
) -> MountedHotspotChurnSummary {
    let accepted_operations = operations
        .iter()
        .filter(|operation| operation.result == "accepted")
        .count();
    let rejected_operations = operations
        .iter()
        .filter(|operation| operation.result == "rejected")
        .count();
    let probe_operations = operations
        .iter()
        .filter(|operation| hotspot_operation_role(operation) == "observer")
        .count();
    MountedHotspotChurnSummary {
        scenario: MOUNTED_HOTSPOT_CHURN_CONVERGENCE.to_string(),
        seed: context.seed,
        bootstrap,
        clients,
        rounds: HOTSPOT_CHURN_ROUNDS,
        working_set: HOTSPOT_WORKING_SET,
        observer_client,
        operations,
        accepted_operations,
        rejected_operations,
        probe_operations,
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_HOTSPOT_CHURN_CONVERGENCE}/operations.ndjson"
        ),
        final_validator,
        namespace_violations,
    }
}

fn write_mounted_hotspot_churn_plan_artifacts(
    context: &ScenarioContext,
) -> Result<MountedHotspotChurnSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let planned_clients = CHAOS_CLIENT_NAMES
        .iter()
        .enumerate()
        .map(|(index, name)| {
            client_spec_for_plan(
                &context.sandbox,
                name,
                context
                    .sandbox
                    .join("planned-mounts")
                    .join(format!("client-{index}")),
            )
        })
        .collect::<Vec<_>>();
    let mut operations = Vec::new();
    for round in 0..HOTSPOT_CHURN_ROUNDS {
        let tasks = hotspot_churn_tasks(context, round, &planned_clients);
        let mut planned = planned_mounted_chaos_operations_from_tasks(started, &tasks);
        write_mounted_hotspot_churn_operations(context, &planned)?;
        operations.append(&mut planned);
    }
    let summary = mounted_hotspot_churn_summary(
        context,
        None,
        CHAOS_CLIENT_NAMES.len(),
        CHAOS_CLIENT_NAMES[2].to_string(),
        operations,
        TreeComparison {
            converged: true,
            reference_client: Some(CHAOS_CLIENT_NAMES[0].to_string()),
            compared_clients: CHAOS_CLIENT_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            differences: Vec::new(),
        },
        Vec::new(),
    );
    write_mounted_hotspot_churn_summary(context, &summary)?;
    Ok(summary)
}

fn client_spec_for_plan(sandbox: &Path, name: &str, mount_point: PathBuf) -> ClientSpec {
    ClientSpec {
        name: name.to_string(),
        client_root: sandbox.join("planned-clients").join(name),
        local_metadata_dir: sandbox.join("planned-clients").join(name).join("meta"),
        local_metadata_db: sandbox
            .join("planned-clients")
            .join(name)
            .join("meta")
            .join("projectfs.db"),
        cache_dir: sandbox.join("planned-clients").join(name).join("cache"),
        mount_metadata_dir: sandbox.join("planned-clients").join(name).join("mount"),
        mount_point,
        stdout_log: sandbox
            .join("planned-clients")
            .join(name)
            .join("stdout.log"),
        stderr_log: sandbox
            .join("planned-clients")
            .join(name)
            .join("stderr.log"),
        tracing_log: sandbox
            .join("planned-clients")
            .join(name)
            .join("studiofs-tracing.log"),
        process_id: None,
        state: ClientLifecycleState::Planned,
    }
}

fn write_mounted_hotspot_churn_operations(
    context: &ScenarioContext,
    operations: &[MountedChaosOperation],
) -> Result<(), ScenarioError> {
    let path = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path)?;
    for operation in operations {
        let mut record = shared_operation_record(
            &writer,
            MOUNTED_HOTSPOT_CHURN_CONVERGENCE,
            operation.shared_operation.clone(),
        )
        .with_client(&operation.client)
        .with_timestamp_ms(operation.timestamp_ms)
        .with_outcome(mounted_chaos_outcome(operation))
        .with_metadata("round", operation.round.to_string())
        .with_metadata("operation", operation.operation.clone())
        .with_metadata("result", &operation.result)
        .with_metadata("role", hotspot_operation_role(operation));
        if let Some(error) = &operation.error {
            record = record.with_metadata("error", error);
        }
        append_shared_record(&mut writer, &record)?;
    }
    Ok(())
}

fn hotspot_operation_role(operation: &MountedChaosOperation) -> &'static str {
    match operation.operation.as_str() {
        "readdir" | "stat" | "read" | "probe_loop" | "read_loop" => "observer",
        _ => "mutator",
    }
}

fn write_mounted_hotspot_churn_summary(
    context: &ScenarioContext,
    summary: &MountedHotspotChurnSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("mounted-hotspot-churn-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

fn hotspot_payload(context: &ScenarioContext, round: usize, slot: usize, label: &str) -> Vec<u8> {
    format!(
        "studiofs hotspot seed={} run_id={} round={round} slot={slot} label={label}\n",
        context.seed, context.run_id
    )
    .into_bytes()
}

#[derive(Debug, Clone, Copy)]
struct MountedFaultChaosConvergence;

impl Scenario for MountedFaultChaosConvergence {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_FAULT_CHAOS_CONVERGENCE,
            description: "seeded mounted fault chaos convergence scenario",
            requires_studiofs_bin: true,
            requires_mount: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let compose_control = compose_control_from_manifest(context)?;
            let mut driver = RealMountedFaultChaosDriver {
                controller: MultiClientController::new(config.clone(), SystemClientProcessExecutor),
                compose_control,
                executor: SystemCommandExecutor,
            };
            run_mounted_fault_chaos_with_driver(
                context,
                &mut driver,
                FAULT_CHAOS_CONVERGENCE_TIMEOUT,
                FAULT_CHAOS_CONVERGENCE_POLL_INTERVAL,
            )?;
        } else {
            write_mounted_fault_chaos_plan_artifacts(context)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("mounted-fault-chaos-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: MountedFaultChaosSummary = serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": MOUNTED_FAULT_CHAOS_CONVERGENCE,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged
                && summary.namespace_violations.is_empty() { "passed" } else { "failed" },
            "warm_clients": summary.warm_clients,
            "cold_observer": summary.cold_observer,
            "restarted_client": summary.restarted_client,
            "accepted_operations": summary.accepted_operations,
            "rejected_operations": summary.rejected_operations,
            "skipped_operations": summary.skipped_operations,
            "postgres_outage_duration_ms": summary.postgres_outage_duration_ms,
            "rustfs_outage_duration_ms": summary.rustfs_outage_duration_ms,
            "restart_elapsed_ms": summary.restart_elapsed_ms,
            "final_convergence_elapsed_ms": summary.final_convergence_elapsed_ms,
            "final_validator": summary.final_validator,
            "namespace_violations": summary.namespace_violations,
            "final_payload_checksums": summary.final_payload_checksums,
            "operation_log": summary.operation_log,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{MOUNTED_FAULT_CHAOS_CONVERGENCE}/mounted-fault-chaos-summary.json"
            ),
            format!("artifacts/scenarios/{MOUNTED_FAULT_CHAOS_CONVERGENCE}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

trait MountedFaultChaosDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_client(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn stop_postgres(&mut self) -> Result<(), ScenarioError>;
    fn start_postgres(&mut self) -> Result<(), ScenarioError>;
    fn stop_rustfs(&mut self) -> Result<(), ScenarioError>;
    fn start_rustfs(&mut self) -> Result<(), ScenarioError>;
    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn remount(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn after_phase(
        &mut self,
        _phase: MountedFaultChaosPhase,
        _clients: &[ClientSpec],
    ) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealMountedFaultChaosDriver<E> {
    controller: MultiClientController<SystemClientProcessExecutor>,
    compose_control: ComposeServiceControl,
    executor: E,
}

impl<E: CommandExecutor> MountedFaultChaosDriver for RealMountedFaultChaosDriver<E> {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.controller.bootstrap_volume(options)?)
    }

    fn mount_client(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.controller.mount(client)?)
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

    fn stop_rustfs(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .stop_service(RUSTFS_SERVICE_NAME, &self.executor)?)
    }

    fn start_rustfs(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .start_service(RUSTFS_SERVICE_NAME, &self.executor)?)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MountedFaultChaosPhase {
    HealthyWarmup,
    HealthyConflictBurst,
    PostgresOutage,
    CrashRestart,
    RustfsOutage,
    ColdObserverBootstrap,
}

impl MountedFaultChaosPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::HealthyWarmup => "healthy_warmup",
            Self::HealthyConflictBurst => "healthy_conflict_burst",
            Self::PostgresOutage => "postgres_outage",
            Self::CrashRestart => "crash_restart",
            Self::RustfsOutage => "rustfs_outage",
            Self::ColdObserverBootstrap => "cold_observer_bootstrap",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MountedFaultChaosSummary {
    scenario: String,
    seed: u64,
    bootstrap: Option<VolumeBootstrapSummary>,
    warm_clients: Vec<String>,
    cold_observer: String,
    restarted_client: String,
    operations: Vec<MountedChaosOperation>,
    accepted_operations: usize,
    rejected_operations: usize,
    skipped_operations: usize,
    postgres_outage_duration_ms: u128,
    rustfs_outage_duration_ms: u128,
    restart_elapsed_ms: u128,
    final_convergence_elapsed_ms: u128,
    operation_log: String,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
    final_payload_checksums: Vec<ClientPayloadChecksum>,
}

fn run_mounted_fault_chaos_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn MountedFaultChaosDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedFaultChaosSummary, ScenarioError> {
    let result = run_mounted_fault_chaos_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::MountedFaultChaosUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_mounted_fault_chaos_body(
    context: &ScenarioContext,
    driver: &mut dyn MountedFaultChaosDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedFaultChaosSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: FAULT_CHAOS_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    for name in FAULT_CHAOS_CLIENT_NAMES
        .iter()
        .take(FAULT_CHAOS_WARM_CLIENTS)
    {
        driver.mount_client(name)?;
    }
    let all_clients = driver.clients();
    if all_clients.len() < FAULT_CHAOS_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientFaultChaosClients {
            expected: FAULT_CHAOS_CLIENT_NAMES.len(),
            actual: all_clients.len(),
        });
    }
    let warm_clients = all_clients
        .iter()
        .take(FAULT_CHAOS_WARM_CLIENTS)
        .cloned()
        .collect::<Vec<_>>();
    let cold_observer = all_clients[FAULT_CHAOS_WARM_CLIENTS].clone();

    establish_mounted_chaos_initial_tree(context, &warm_clients[0])?;
    establish_fault_chaos_initial_tree(context, &warm_clients[0])?;
    driver.after_phase(MountedFaultChaosPhase::HealthyWarmup, &warm_clients)?;
    wait_for_convergence(
        &client_mounts(context, &warm_clients),
        context.scenario_dir.join("warmup-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let started = Instant::now();
    let mut operations = Vec::new();
    for round in 0..FAULT_CHAOS_CONFLICT_ROUNDS {
        let mut round_operations =
            perform_mounted_chaos_round(context, started, round, &warm_clients)?;
        for operation in &mut round_operations {
            operation.path = format!("healthy_conflict_burst:{}", operation.path);
        }
        write_mounted_fault_chaos_operations(
            context,
            MountedFaultChaosPhase::HealthyConflictBurst,
            &round_operations,
        )?;
        operations.append(&mut round_operations);
    }
    driver.after_phase(MountedFaultChaosPhase::HealthyConflictBurst, &warm_clients)?;

    let postgres_outage_duration_ms =
        run_fault_chaos_postgres_phase(context, driver, started, &warm_clients, &mut operations)?;
    driver.after_phase(MountedFaultChaosPhase::PostgresOutage, &warm_clients)?;

    let killed_index = (context.seed as usize) % warm_clients.len();
    let restarted_client = warm_clients[killed_index].name.clone();
    let restart_elapsed_ms = run_fault_chaos_crash_phase(
        context,
        driver,
        started,
        &warm_clients,
        &restarted_client,
        &mut operations,
    )?;
    driver.after_phase(MountedFaultChaosPhase::CrashRestart, &warm_clients)?;

    let rustfs_outage_duration_ms =
        run_fault_chaos_rustfs_phase(context, driver, started, &warm_clients, &mut operations)?;
    driver.after_phase(MountedFaultChaosPhase::RustfsOutage, &warm_clients)?;

    driver.mount_client(&cold_observer.name)?;
    let mut final_clients = driver.clients();
    final_clients.truncate(FAULT_CHAOS_CLIENT_NAMES.len());
    driver.after_phase(
        MountedFaultChaosPhase::ColdObserverBootstrap,
        &final_clients,
    )?;

    finish_mounted_fault_chaos(
        context,
        Some(bootstrap),
        final_clients,
        cold_observer.name,
        restarted_client,
        operations,
        MountedFaultChaosTimings {
            postgres_outage_duration_ms,
            rustfs_outage_duration_ms,
            restart_elapsed_ms,
        },
        convergence_timeout,
        poll_interval,
    )
}

fn establish_fault_chaos_initial_tree(
    context: &ScenarioContext,
    client: &ClientSpec,
) -> Result<(), ScenarioError> {
    let root = client
        .mount_point
        .join(run_unique_dir(context, "fault-chaos"));
    write_mounted_chaos_initial_file(
        root.join("postgres-outage-local.txt"),
        &fault_payload(context, "postgres-initial"),
    )?;
    write_mounted_chaos_initial_file(
        root.join("crash-restart.txt"),
        &fault_payload(context, "crash-initial"),
    )?;
    write_mounted_chaos_initial_file(
        root.join("peer-while-crashed.txt"),
        &fault_payload(context, "peer-initial"),
    )?;
    write_mounted_chaos_initial_file(
        root.join("rustfs-outage-payload.bin"),
        &fault_payload(context, "rustfs-initial"),
    )?;
    Ok(())
}

fn run_fault_chaos_postgres_phase(
    context: &ScenarioContext,
    driver: &mut dyn MountedFaultChaosDriver,
    started: Instant,
    clients: &[ClientSpec],
    operations: &mut Vec<MountedChaosOperation>,
) -> Result<u128, ScenarioError> {
    let outage_started = Instant::now();
    driver.stop_postgres()?;
    let outage_record = fault_chaos_observed_operation(
        started,
        MountedFaultChaosPhase::PostgresOutage,
        "postgres_outage",
        SharedOperation::PostgresOutage,
    );
    write_mounted_fault_chaos_operations(
        context,
        MountedFaultChaosPhase::PostgresOutage,
        std::slice::from_ref(&outage_record),
    )?;
    operations.push(outage_record);

    let path = run_unique_path(context, "fault-chaos", "postgres-outage-local.txt");
    let payload = fault_payload(context, "postgres-outage-local-write");
    let write_result = fs::write(clients[1].mount_point.join(&path), &payload);
    let write_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::PostgresOutage,
        2,
        &clients[1],
        "write",
        &path,
        SharedOperation::Write {
            path: path.clone(),
            offset: Some(0),
            bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            checksum_sha256: Some(bytes_sha256(&payload)),
        },
        write_result.err(),
    );
    write_mounted_fault_chaos_operations(
        context,
        MountedFaultChaosPhase::PostgresOutage,
        std::slice::from_ref(&write_operation),
    )?;
    if write_operation.result != "accepted" {
        driver.start_postgres()?;
        return Err(ScenarioError::RejectedOutageOperations {
            rejected: 1,
            total: 1,
        });
    }
    operations.push(write_operation);

    let local_read = fs::read(clients[1].mount_point.join(&path))?;
    let read_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::PostgresOutage,
        3,
        &clients[1],
        "read",
        &path,
        SharedOperation::Read {
            path: path.clone(),
            bytes: Some(u64::try_from(local_read.len()).unwrap_or(u64::MAX)),
            checksum_sha256: Some(bytes_sha256(&local_read)),
        },
        None,
    );
    write_mounted_fault_chaos_operations(
        context,
        MountedFaultChaosPhase::PostgresOutage,
        std::slice::from_ref(&read_operation),
    )?;
    operations.push(read_operation);
    let outage_duration_ms = elapsed_millis_nonzero(outage_started);

    driver.start_postgres()?;
    let recovery_record = fault_chaos_observed_operation(
        started,
        MountedFaultChaosPhase::PostgresOutage,
        "postgres_recovery",
        SharedOperation::PostgresRecovery,
    );
    write_mounted_fault_chaos_operations(
        context,
        MountedFaultChaosPhase::PostgresOutage,
        std::slice::from_ref(&recovery_record),
    )?;
    operations.push(recovery_record);
    Ok(outage_duration_ms)
}

fn run_fault_chaos_crash_phase(
    context: &ScenarioContext,
    driver: &mut dyn MountedFaultChaosDriver,
    started: Instant,
    clients: &[ClientSpec],
    restarted_client: &str,
    operations: &mut Vec<MountedChaosOperation>,
) -> Result<u128, ScenarioError> {
    let crashed = clients
        .iter()
        .find(|client| client.name == restarted_client)
        .ok_or_else(|| {
            ScenarioError::Io(std::io::Error::other(format!(
                "missing selected restart client {restarted_client}"
            )))
        })?;
    let path = run_unique_path(context, "fault-chaos", "crash-restart.txt");
    let payload = fault_payload(context, "crash-before-publication");
    write_local_durable_payload(crashed.mount_point.join(&path), &payload)?;
    let write_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::CrashRestart,
        1,
        crashed,
        "write",
        &path,
        SharedOperation::Write {
            path: path.clone(),
            offset: Some(0),
            bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            checksum_sha256: Some(bytes_sha256(&payload)),
        },
        None,
    );
    let fsync_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::CrashRestart,
        2,
        crashed,
        "fsync",
        &path,
        SharedOperation::Fsync { path: path.clone() },
        None,
    );
    let restart_started = Instant::now();
    driver.force_kill(restarted_client)?;
    let crash_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::CrashRestart,
        3,
        crashed,
        "client_crash",
        restarted_client,
        SharedOperation::ClientCrash,
        None,
    );
    let peer = clients
        .iter()
        .find(|client| client.name != restarted_client)
        .expect("at least one peer client must be available");
    let peer_path = run_unique_path(context, "fault-chaos", "peer-while-crashed.txt");
    let peer_payload = fault_payload(context, "peer-while-crashed");
    let peer_write_result = fs::write(peer.mount_point.join(&peer_path), &peer_payload);
    let peer_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::CrashRestart,
        4,
        peer,
        "peer_write_while_crashed",
        &peer_path,
        SharedOperation::Write {
            path: peer_path.clone(),
            offset: Some(0),
            bytes: u64::try_from(peer_payload.len()).unwrap_or(u64::MAX),
            checksum_sha256: Some(bytes_sha256(&peer_payload)),
        },
        peer_write_result.err(),
    );
    driver.remount(restarted_client)?;
    let restart_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::CrashRestart,
        5,
        crashed,
        "client_restart",
        restarted_client,
        SharedOperation::ClientRestart,
        None,
    );
    let local_read = fs::read(crashed.mount_point.join(&path))?;
    if local_read != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "fault chaos restart durability failure: data read after restart does not match {path}"
        ))));
    }
    let restart_elapsed_ms = elapsed_millis_nonzero(restart_started);
    let phase_operations = vec![
        write_operation,
        fsync_operation,
        crash_operation,
        peer_operation,
        restart_operation,
    ];
    write_mounted_fault_chaos_operations(
        context,
        MountedFaultChaosPhase::CrashRestart,
        &phase_operations,
    )?;
    operations.extend(phase_operations);
    Ok(restart_elapsed_ms)
}

fn run_fault_chaos_rustfs_phase(
    context: &ScenarioContext,
    driver: &mut dyn MountedFaultChaosDriver,
    started: Instant,
    clients: &[ClientSpec],
    operations: &mut Vec<MountedChaosOperation>,
) -> Result<u128, ScenarioError> {
    let outage_started = Instant::now();
    driver.stop_rustfs()?;
    let outage_record = fault_chaos_observed_operation(
        started,
        MountedFaultChaosPhase::RustfsOutage,
        "object_store_outage",
        SharedOperation::ObjectStoreOutage,
    );
    let path = run_unique_path(context, "fault-chaos", "rustfs-outage-payload.bin");
    let payload = fault_payload(context, "rustfs-outage-payload-pressure").repeat(8);
    let write_result = fs::write(clients[2].mount_point.join(&path), &payload);
    let write_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::RustfsOutage,
        2,
        &clients[2],
        "write",
        &path,
        SharedOperation::Write {
            path: path.clone(),
            offset: Some(0),
            bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            checksum_sha256: Some(bytes_sha256(&payload)),
        },
        write_result.err(),
    );
    if write_operation.result != "accepted" {
        write_mounted_fault_chaos_operations(
            context,
            MountedFaultChaosPhase::RustfsOutage,
            &[outage_record.clone(), write_operation.clone()],
        )?;
        operations.extend([outage_record, write_operation]);
        driver.start_rustfs()?;
        return Err(ScenarioError::RejectedOutageOperations {
            rejected: 1,
            total: 1,
        });
    }
    let local_read = fs::read(clients[2].mount_point.join(&path))?;
    let read_operation = fault_chaos_client_operation(
        started,
        MountedFaultChaosPhase::RustfsOutage,
        3,
        &clients[2],
        "read",
        &path,
        SharedOperation::Read {
            path: path.clone(),
            bytes: Some(u64::try_from(local_read.len()).unwrap_or(u64::MAX)),
            checksum_sha256: Some(bytes_sha256(&local_read)),
        },
        None,
    );
    let outage_duration_ms = elapsed_millis_nonzero(outage_started);
    driver.start_rustfs()?;
    let recovery_record = fault_chaos_observed_operation(
        started,
        MountedFaultChaosPhase::RustfsOutage,
        "object_store_recovery",
        SharedOperation::ObjectStoreRecovery,
    );
    let phase_operations = vec![
        outage_record,
        write_operation,
        read_operation,
        recovery_record,
    ];
    write_mounted_fault_chaos_operations(
        context,
        MountedFaultChaosPhase::RustfsOutage,
        &phase_operations,
    )?;
    operations.extend(phase_operations);
    Ok(outage_duration_ms)
}

struct MountedFaultChaosTimings {
    postgres_outage_duration_ms: u128,
    rustfs_outage_duration_ms: u128,
    restart_elapsed_ms: u128,
}

#[allow(clippy::too_many_arguments)]
fn finish_mounted_fault_chaos(
    context: &ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    cold_observer: String,
    restarted_client: String,
    operations: Vec<MountedChaosOperation>,
    timings: MountedFaultChaosTimings,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedFaultChaosSummary, ScenarioError> {
    let convergence_started = Instant::now();
    let convergence = wait_for_convergence(
        &client_mounts(context, &clients),
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let final_convergence_elapsed_ms = elapsed_millis_nonzero(convergence_started);
    let mut namespace_violations = Vec::new();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(snapshot, &[]));
    }
    if !namespace_violations.is_empty() {
        return Err(ScenarioError::MountedFaultChaosInvariantViolations {
            violations: namespace_violations,
        });
    }
    let final_payload_checksums = collect_fault_chaos_final_checksums(
        &clients,
        &run_unique_path(context, "fault-chaos", "rustfs-outage-payload.bin"),
    )?;
    let summary = mounted_fault_chaos_summary(
        context,
        bootstrap,
        clients,
        cold_observer,
        restarted_client,
        operations,
        timings,
        final_convergence_elapsed_ms,
        convergence.comparison,
        namespace_violations,
        final_payload_checksums,
    );
    write_mounted_fault_chaos_summary(context, &summary)?;
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn mounted_fault_chaos_summary(
    context: &ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    cold_observer: String,
    restarted_client: String,
    operations: Vec<MountedChaosOperation>,
    timings: MountedFaultChaosTimings,
    final_convergence_elapsed_ms: u128,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
    final_payload_checksums: Vec<ClientPayloadChecksum>,
) -> MountedFaultChaosSummary {
    let accepted_operations = operations
        .iter()
        .filter(|operation| operation.result == "accepted")
        .count();
    let rejected_operations = operations
        .iter()
        .filter(|operation| operation.result == "rejected")
        .count();
    let skipped_operations = operations
        .iter()
        .filter(|operation| operation.result == "skipped")
        .count();
    MountedFaultChaosSummary {
        scenario: MOUNTED_FAULT_CHAOS_CONVERGENCE.to_string(),
        seed: context.seed,
        bootstrap,
        warm_clients: clients
            .iter()
            .take(FAULT_CHAOS_WARM_CLIENTS)
            .map(|client| client.name.clone())
            .collect(),
        cold_observer,
        restarted_client,
        operations,
        accepted_operations,
        rejected_operations,
        skipped_operations,
        postgres_outage_duration_ms: timings.postgres_outage_duration_ms,
        rustfs_outage_duration_ms: timings.rustfs_outage_duration_ms,
        restart_elapsed_ms: timings.restart_elapsed_ms,
        final_convergence_elapsed_ms,
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_FAULT_CHAOS_CONVERGENCE}/operations.ndjson"
        ),
        final_validator,
        namespace_violations,
        final_payload_checksums,
    }
}

fn write_mounted_fault_chaos_plan_artifacts(
    context: &ScenarioContext,
) -> Result<MountedFaultChaosSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let mut operations = Vec::new();
    for round in 0..FAULT_CHAOS_CONFLICT_ROUNDS {
        let mut round_operations = planned_mounted_chaos_round(context, started, round);
        write_mounted_fault_chaos_operations(
            context,
            MountedFaultChaosPhase::HealthyConflictBurst,
            &round_operations,
        )?;
        operations.append(&mut round_operations);
    }
    let planned_records = [
        (
            MountedFaultChaosPhase::PostgresOutage,
            fault_chaos_observed_operation(
                started,
                MountedFaultChaosPhase::PostgresOutage,
                "postgres_outage",
                SharedOperation::PostgresOutage,
            ),
        ),
        (
            MountedFaultChaosPhase::PostgresOutage,
            fault_chaos_observed_operation(
                started,
                MountedFaultChaosPhase::PostgresOutage,
                "postgres_recovery",
                SharedOperation::PostgresRecovery,
            ),
        ),
        (
            MountedFaultChaosPhase::CrashRestart,
            fault_chaos_observed_operation(
                started,
                MountedFaultChaosPhase::CrashRestart,
                "client_crash",
                SharedOperation::ClientCrash,
            ),
        ),
        (
            MountedFaultChaosPhase::CrashRestart,
            fault_chaos_observed_operation(
                started,
                MountedFaultChaosPhase::CrashRestart,
                "client_restart",
                SharedOperation::ClientRestart,
            ),
        ),
        (
            MountedFaultChaosPhase::RustfsOutage,
            fault_chaos_observed_operation(
                started,
                MountedFaultChaosPhase::RustfsOutage,
                "object_store_outage",
                SharedOperation::ObjectStoreOutage,
            ),
        ),
        (
            MountedFaultChaosPhase::RustfsOutage,
            fault_chaos_observed_operation(
                started,
                MountedFaultChaosPhase::RustfsOutage,
                "object_store_recovery",
                SharedOperation::ObjectStoreRecovery,
            ),
        ),
        (
            MountedFaultChaosPhase::ColdObserverBootstrap,
            fault_chaos_observed_operation(
                started,
                MountedFaultChaosPhase::ColdObserverBootstrap,
                "cold_observer_mount",
                SharedOperation::MountedOnly {
                    name: "cold_observer_mount".to_string(),
                    reason: "mounting a late cold observer is mounted-E2E only".to_string(),
                },
            ),
        ),
    ];
    for (phase, mut record) in planned_records {
        record.result = "planned".to_string();
        write_mounted_fault_chaos_operations(context, phase, std::slice::from_ref(&record))?;
        operations.push(record);
    }
    let summary = MountedFaultChaosSummary {
        scenario: MOUNTED_FAULT_CHAOS_CONVERGENCE.to_string(),
        seed: context.seed,
        bootstrap: None,
        warm_clients: FAULT_CHAOS_CLIENT_NAMES
            .iter()
            .take(FAULT_CHAOS_WARM_CLIENTS)
            .map(|name| (*name).to_string())
            .collect(),
        cold_observer: FAULT_CHAOS_CLIENT_NAMES[FAULT_CHAOS_WARM_CLIENTS].to_string(),
        restarted_client: FAULT_CHAOS_CLIENT_NAMES
            [(context.seed as usize) % FAULT_CHAOS_WARM_CLIENTS]
            .to_string(),
        accepted_operations: 0,
        rejected_operations: 0,
        skipped_operations: 0,
        operations,
        postgres_outage_duration_ms: 0,
        rustfs_outage_duration_ms: 0,
        restart_elapsed_ms: 0,
        final_convergence_elapsed_ms: 0,
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_FAULT_CHAOS_CONVERGENCE}/operations.ndjson"
        ),
        final_validator: TreeComparison {
            converged: true,
            reference_client: Some(FAULT_CHAOS_CLIENT_NAMES[0].to_string()),
            compared_clients: FAULT_CHAOS_CLIENT_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
        final_payload_checksums: Vec::new(),
    };
    write_mounted_fault_chaos_summary(context, &summary)?;
    Ok(summary)
}

fn fault_chaos_observed_operation(
    started: Instant,
    phase: MountedFaultChaosPhase,
    operation: &'static str,
    shared_operation: SharedOperation,
) -> MountedChaosOperation {
    MountedChaosOperation {
        round: phase as usize,
        order: 0,
        client: String::new(),
        operation: operation.to_string(),
        path: operation.to_string(),
        shared_operation,
        result: "observed".to_string(),
        timestamp_ms: elapsed_millis_u64(started),
        error: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn fault_chaos_client_operation(
    started: Instant,
    phase: MountedFaultChaosPhase,
    order: usize,
    client: &ClientSpec,
    operation: &'static str,
    path: &str,
    shared_operation: SharedOperation,
    error: Option<io::Error>,
) -> MountedChaosOperation {
    let error = error.map(|error| error.to_string());
    MountedChaosOperation {
        round: phase as usize,
        order,
        client: client.name.clone(),
        operation: operation.to_string(),
        path: path.to_string(),
        shared_operation,
        result: if error.is_some() {
            "rejected".to_string()
        } else {
            "accepted".to_string()
        },
        timestamp_ms: elapsed_millis_u64(started),
        error,
    }
}

fn write_mounted_fault_chaos_operations(
    context: &ScenarioContext,
    phase: MountedFaultChaosPhase,
    operations: &[MountedChaosOperation],
) -> Result<(), ScenarioError> {
    let path = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path)?;
    for operation in operations {
        let mut record = shared_operation_record(
            &writer,
            MOUNTED_FAULT_CHAOS_CONVERGENCE,
            operation.shared_operation.clone(),
        )
        .with_timestamp_ms(operation.timestamp_ms)
        .with_outcome(mounted_chaos_outcome(operation))
        .with_metadata("phase", phase.as_str())
        .with_metadata("operation", operation.operation.clone());
        if !operation.client.is_empty() {
            record = record.with_client(&operation.client);
        }
        if operation.error.is_some() {
            record = record.with_metadata("result", &operation.result);
        }
        append_shared_record(&mut writer, &record)?;
    }
    Ok(())
}

fn collect_fault_chaos_final_checksums(
    clients: &[ClientSpec],
    relative_path: &str,
) -> Result<Vec<ClientPayloadChecksum>, ScenarioError> {
    clients
        .iter()
        .map(|client| {
            let checksum = bytes_sha256(&fs::read(client.mount_point.join(relative_path))?);
            Ok(ClientPayloadChecksum {
                client: client.name.clone(),
                checksum_sha256: checksum,
            })
        })
        .collect()
}

fn write_mounted_fault_chaos_summary(
    context: &ScenarioContext,
    summary: &MountedFaultChaosSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("mounted-fault-chaos-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

fn fault_payload(context: &ScenarioContext, label: &str) -> Vec<u8> {
    format!(
        "studiofs fault chaos seed={} run_id={} label={label}\n",
        context.seed, context.run_id
    )
    .into_bytes()
}

#[derive(Debug, Clone, Copy)]
struct MountedLwwReconnectChaosConvergence;

impl Scenario for MountedLwwReconnectChaosConvergence {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE,
            description: "mounted LWW reconnect chaos scenario with stale writer and cold observer",
            requires_studiofs_bin: true,
            requires_mount: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut driver = RealMountedLwwReconnectDriver(MultiClientController::new(
                config.clone(),
                SystemClientProcessExecutor,
            ));
            run_mounted_lww_reconnect_with_driver(
                context,
                &mut driver,
                LWW_RECONNECT_CONVERGENCE_TIMEOUT,
                LWW_RECONNECT_CONVERGENCE_POLL_INTERVAL,
            )?;
        } else {
            write_mounted_lww_reconnect_plan_artifacts(context)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("mounted-lww-reconnect-chaos-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: MountedLwwReconnectSummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged
                && summary.namespace_violations.is_empty()
                && summary.oracle.matched { "passed" } else { "failed" },
            "warm_clients": summary.warm_clients,
            "stale_writer": summary.stale_writer,
            "cold_observer": summary.cold_observer,
            "accepted_operations": summary.accepted_operations,
            "rejected_operations": summary.rejected_operations,
            "offline_elapsed_ms": summary.offline_elapsed_ms,
            "restart_elapsed_ms": summary.restart_elapsed_ms,
            "final_convergence_elapsed_ms": summary.final_convergence_elapsed_ms,
            "expected_winner": summary.expected_winner,
            "oracle": summary.oracle,
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
                "artifacts/scenarios/{MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE}/mounted-lww-reconnect-chaos-summary.json"
            ),
            format!(
                "artifacts/scenarios/{MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE}/validation.json"
            ),
            format!(
                "artifacts/scenarios/{MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE}/lww-oracle.json"
            ),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

trait MountedLwwReconnectDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_client(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn remount(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn after_phase(
        &mut self,
        _phase: MountedLwwReconnectPhase,
        _clients: &[ClientSpec],
    ) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealMountedLwwReconnectDriver(MultiClientController<SystemClientProcessExecutor>);

impl MountedLwwReconnectDriver for RealMountedLwwReconnectDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.0.bootstrap_volume(options)?)
    }

    fn mount_client(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.0.mount(client)?)
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
enum MountedLwwReconnectPhase {
    HealthyWarmup,
    StaleWriterOffline,
    OnlineWriters,
    StaleWriterReturn,
    ReplayCrashRestart,
    ColdObserverBootstrap,
    OracleValidation,
}

impl MountedLwwReconnectPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::HealthyWarmup => "healthy_warmup",
            Self::StaleWriterOffline => "stale_writer_offline",
            Self::OnlineWriters => "online_writers",
            Self::StaleWriterReturn => "stale_writer_return",
            Self::ReplayCrashRestart => "replay_crash_restart",
            Self::ColdObserverBootstrap => "cold_observer_bootstrap",
            Self::OracleValidation => "oracle_validation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MountedLwwReconnectSummary {
    scenario: String,
    seed: u64,
    bootstrap: Option<VolumeBootstrapSummary>,
    warm_clients: Vec<String>,
    stale_writer: String,
    cold_observer: String,
    operations: Vec<MountedChaosOperation>,
    accepted_operations: usize,
    rejected_operations: usize,
    offline_elapsed_ms: u128,
    restart_elapsed_ms: u128,
    final_convergence_elapsed_ms: u128,
    operation_log: String,
    expected_winner: LwwExpectedWinner,
    oracle: LwwOracleReport,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LwwExpectedWinner {
    client: String,
    path: String,
    checksum_sha256: String,
    bytes: u64,
    oracle_order: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LwwOracleReport {
    path: String,
    expected_client: String,
    expected_checksum_sha256: String,
    expected_oracle_order: u32,
    matched: bool,
    clients: Vec<LwwOracleClientResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct LwwOracleClientResult {
    client: String,
    checksum_sha256: Option<String>,
    matched: bool,
    error: Option<String>,
}

fn run_mounted_lww_reconnect_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn MountedLwwReconnectDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedLwwReconnectSummary, ScenarioError> {
    let result =
        run_mounted_lww_reconnect_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => {
            Err(ScenarioError::MountedLwwReconnectUnmountAfterFailure {
                primary: primary.to_string(),
                cleanup: cleanup.to_string(),
            })
        }
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_mounted_lww_reconnect_body(
    context: &ScenarioContext,
    driver: &mut dyn MountedLwwReconnectDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedLwwReconnectSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: LWW_RECONNECT_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    for name in LWW_RECONNECT_CLIENT_NAMES
        .iter()
        .take(LWW_RECONNECT_WARM_CLIENTS)
    {
        driver.mount_client(name)?;
    }

    let all_clients = driver.clients();
    if all_clients.len() < LWW_RECONNECT_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientLwwReconnectClients {
            expected: LWW_RECONNECT_CLIENT_NAMES.len(),
            actual: all_clients.len(),
        });
    }
    let warm_clients = all_clients
        .iter()
        .take(LWW_RECONNECT_WARM_CLIENTS)
        .cloned()
        .collect::<Vec<_>>();
    let stale_writer = warm_clients[LWW_RECONNECT_STALE_WRITER_INDEX].clone();
    let cold_observer = all_clients[LWW_RECONNECT_COLD_OBSERVER_INDEX].clone();

    establish_lww_reconnect_initial_tree(context, &warm_clients[0])?;
    driver.after_phase(MountedLwwReconnectPhase::HealthyWarmup, &warm_clients)?;
    wait_for_convergence(
        &client_mounts(context, &warm_clients),
        context.scenario_dir.join("warmup-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let started = Instant::now();
    let offline_started = Instant::now();
    driver.force_kill(&stale_writer.name)?;
    let mut operations = vec![lww_reconnect_client_operation(
        started,
        MountedLwwReconnectPhase::StaleWriterOffline,
        1,
        &stale_writer,
        "stale_writer_offline",
        &stale_writer.name,
        SharedOperation::ClientCrash,
        None,
    )];
    write_mounted_lww_reconnect_operations(
        context,
        MountedLwwReconnectPhase::StaleWriterOffline,
        &operations,
    )?;
    driver.after_phase(MountedLwwReconnectPhase::StaleWriterOffline, &warm_clients)?;

    let online_clients = warm_clients
        .iter()
        .filter(|client| client.name != stale_writer.name)
        .cloned()
        .collect::<Vec<_>>();
    let online_writer_a = &online_clients[0];
    let online_writer_b = &online_clients[1];
    let write_a = lww_reconnect_write(context, started, 1, online_writer_a)?;
    write_mounted_lww_reconnect_operations(
        context,
        MountedLwwReconnectPhase::OnlineWriters,
        std::slice::from_ref(&write_a),
    )?;
    operations.push(write_a);
    driver.after_phase(MountedLwwReconnectPhase::OnlineWriters, &online_clients)?;
    wait_for_convergence(
        &client_mounts(context, &online_clients),
        context
            .scenario_dir
            .join("online-writers-order-1-validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let write_b = lww_reconnect_write(context, started, 2, online_writer_b)?;
    write_mounted_lww_reconnect_operations(
        context,
        MountedLwwReconnectPhase::OnlineWriters,
        std::slice::from_ref(&write_b),
    )?;
    operations.push(write_b);
    driver.after_phase(MountedLwwReconnectPhase::OnlineWriters, &online_clients)?;
    wait_for_convergence(
        &client_mounts(context, &online_clients),
        context.scenario_dir.join("online-writers-validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let offline_elapsed_ms = elapsed_millis_nonzero(offline_started);

    let restart_started = Instant::now();
    driver.remount(&stale_writer.name)?;
    let restart_operation = lww_reconnect_client_operation(
        started,
        MountedLwwReconnectPhase::StaleWriterReturn,
        1,
        &stale_writer,
        "stale_writer_restart",
        &stale_writer.name,
        SharedOperation::ClientRestart,
        None,
    );
    write_mounted_lww_reconnect_operations(
        context,
        MountedLwwReconnectPhase::StaleWriterReturn,
        std::slice::from_ref(&restart_operation),
    )?;
    operations.push(restart_operation);
    driver.after_phase(MountedLwwReconnectPhase::StaleWriterReturn, &warm_clients)?;

    let expected_winner_payload = lww_reconnect_payload(context, &stale_writer.name, 3);
    let write_c = lww_reconnect_write(context, started, 3, &stale_writer)?;
    write_mounted_lww_reconnect_operations(
        context,
        MountedLwwReconnectPhase::StaleWriterReturn,
        std::slice::from_ref(&write_c),
    )?;
    operations.push(write_c);

    driver.force_kill(&stale_writer.name)?;
    let replay_crash = lww_reconnect_client_operation(
        started,
        MountedLwwReconnectPhase::ReplayCrashRestart,
        1,
        &stale_writer,
        "stale_writer_replay_crash",
        &stale_writer.name,
        SharedOperation::ClientCrash,
        None,
    );
    driver.remount(&stale_writer.name)?;
    let replay_restart = lww_reconnect_client_operation(
        started,
        MountedLwwReconnectPhase::ReplayCrashRestart,
        2,
        &stale_writer,
        "stale_writer_replay_restart",
        &stale_writer.name,
        SharedOperation::ClientRestart,
        None,
    );
    let replay_operations = vec![replay_crash, replay_restart];
    write_mounted_lww_reconnect_operations(
        context,
        MountedLwwReconnectPhase::ReplayCrashRestart,
        &replay_operations,
    )?;
    operations.extend(replay_operations);
    let oracle_path = lww_reconnect_oracle_path(context);
    let local_read = fs::read(stale_writer.mount_point.join(&oracle_path))?;
    if local_read != expected_winner_payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "LWW reconnect replay durability failure: data read after restart does not match {}",
            oracle_path
        ))));
    }
    let restart_elapsed_ms = elapsed_millis_nonzero(restart_started);
    driver.after_phase(MountedLwwReconnectPhase::ReplayCrashRestart, &warm_clients)?;

    driver.mount_client(&cold_observer.name)?;
    let cold_mount = lww_reconnect_observed_operation(
        started,
        MountedLwwReconnectPhase::ColdObserverBootstrap,
        "cold_observer_mount",
        SharedOperation::MountedOnly {
            name: "cold_observer_mount".to_string(),
            reason: "mounting a late cold observer is mounted-E2E only".to_string(),
        },
    );
    write_mounted_lww_reconnect_operations(
        context,
        MountedLwwReconnectPhase::ColdObserverBootstrap,
        std::slice::from_ref(&cold_mount),
    )?;
    operations.push(cold_mount);

    let mut final_clients = driver.clients();
    final_clients.truncate(LWW_RECONNECT_CLIENT_NAMES.len());
    driver.after_phase(
        MountedLwwReconnectPhase::ColdObserverBootstrap,
        &final_clients,
    )?;

    finish_mounted_lww_reconnect(
        context,
        Some(bootstrap),
        final_clients,
        stale_writer.name,
        cold_observer.name,
        operations,
        LwwReconnectTimings {
            offline_elapsed_ms,
            restart_elapsed_ms,
        },
        convergence_timeout,
        poll_interval,
    )
}

fn establish_lww_reconnect_initial_tree(
    context: &ScenarioContext,
    client: &ClientSpec,
) -> Result<(), ScenarioError> {
    let root = client
        .mount_point
        .join(run_unique_dir(context, "lww-chaos"));
    fs::create_dir_all(&root)?;
    fs::write(
        root.join("winner.txt"),
        lww_reconnect_payload(context, "initial", 0),
    )?;
    fs::write(
        root.join("observer.txt"),
        lww_reconnect_payload(context, "observer", 0),
    )?;
    Ok(())
}

fn lww_reconnect_write(
    context: &ScenarioContext,
    started: Instant,
    oracle_order: u32,
    client: &ClientSpec,
) -> Result<MountedChaosOperation, ScenarioError> {
    let payload = lww_reconnect_payload(context, &client.name, oracle_order);
    let oracle_path = lww_reconnect_oracle_path(context);
    write_local_durable_payload(client.mount_point.join(&oracle_path), &payload)?;
    Ok(lww_reconnect_client_operation(
        started,
        if oracle_order == 3 {
            MountedLwwReconnectPhase::StaleWriterReturn
        } else {
            MountedLwwReconnectPhase::OnlineWriters
        },
        oracle_order as usize,
        client,
        lww_write_operation_name(oracle_order),
        &oracle_path,
        SharedOperation::Write {
            path: oracle_path.clone(),
            offset: Some(0),
            bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            checksum_sha256: Some(bytes_sha256(&payload)),
        },
        None,
    ))
}

fn lww_write_operation_name(oracle_order: u32) -> &'static str {
    match oracle_order {
        1 => "lww_write_order_1",
        2 => "lww_write_order_2",
        3 => "lww_write_order_3",
        _ => "lww_write_order_unknown",
    }
}

struct LwwReconnectTimings {
    offline_elapsed_ms: u128,
    restart_elapsed_ms: u128,
}

#[allow(clippy::too_many_arguments)]
fn finish_mounted_lww_reconnect(
    context: &ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    stale_writer: String,
    cold_observer: String,
    operations: Vec<MountedChaosOperation>,
    timings: LwwReconnectTimings,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedLwwReconnectSummary, ScenarioError> {
    let convergence_started = Instant::now();
    let convergence = wait_for_convergence(
        &client_mounts(context, &clients),
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let final_convergence_elapsed_ms = elapsed_millis_nonzero(convergence_started);
    let mut namespace_violations = Vec::new();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(snapshot, &[]));
    }
    if !namespace_violations.is_empty() {
        return Err(ScenarioError::MountedLwwReconnectInvariantViolations {
            violations: namespace_violations,
        });
    }

    let expected_winner = lww_expected_winner(context, &stale_writer);
    let oracle = lww_oracle_report(&convergence.snapshots, &expected_winner);
    write_lww_oracle_report(context, &oracle)?;
    if !oracle.matched {
        let mismatches = oracle
            .clients
            .iter()
            .filter(|client| !client.matched)
            .cloned()
            .collect();
        return Err(ScenarioError::MountedLwwReconnectOracleMismatch {
            path: expected_winner.path,
            expected_checksum_sha256: expected_winner.checksum_sha256,
            mismatches,
        });
    }

    let summary = mounted_lww_reconnect_summary(
        context,
        bootstrap,
        clients,
        stale_writer,
        cold_observer,
        operations,
        timings,
        final_convergence_elapsed_ms,
        expected_winner,
        oracle,
        convergence.comparison,
        namespace_violations,
    );
    write_mounted_lww_reconnect_summary(context, &summary)?;
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn mounted_lww_reconnect_summary(
    context: &ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    stale_writer: String,
    cold_observer: String,
    operations: Vec<MountedChaosOperation>,
    timings: LwwReconnectTimings,
    final_convergence_elapsed_ms: u128,
    expected_winner: LwwExpectedWinner,
    oracle: LwwOracleReport,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
) -> MountedLwwReconnectSummary {
    let accepted_operations = operations
        .iter()
        .filter(|operation| operation.result == "accepted")
        .count();
    let rejected_operations = operations
        .iter()
        .filter(|operation| operation.result == "rejected")
        .count();
    MountedLwwReconnectSummary {
        scenario: MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE.to_string(),
        seed: context.seed,
        bootstrap,
        warm_clients: clients
            .iter()
            .take(LWW_RECONNECT_WARM_CLIENTS)
            .map(|client| client.name.clone())
            .collect(),
        stale_writer,
        cold_observer,
        operations,
        accepted_operations,
        rejected_operations,
        offline_elapsed_ms: timings.offline_elapsed_ms,
        restart_elapsed_ms: timings.restart_elapsed_ms,
        final_convergence_elapsed_ms,
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE}/operations.ndjson"
        ),
        expected_winner,
        oracle,
        final_validator,
        namespace_violations,
    }
}

fn write_mounted_lww_reconnect_plan_artifacts(
    context: &ScenarioContext,
) -> Result<MountedLwwReconnectSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let planned_clients = LWW_RECONNECT_CLIENT_NAMES
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let mount = context
                .sandbox
                .join("planned-mounts")
                .join(format!("client-{index}"));
            ClientSpec {
                name: (*name).to_string(),
                client_root: context.sandbox.join("planned-clients").join(name),
                local_metadata_dir: context
                    .sandbox
                    .join("planned-clients")
                    .join(name)
                    .join("meta"),
                local_metadata_db: context
                    .sandbox
                    .join("planned-clients")
                    .join(name)
                    .join("meta")
                    .join("projectfs.db"),
                cache_dir: context
                    .sandbox
                    .join("planned-clients")
                    .join(name)
                    .join("cache"),
                mount_metadata_dir: context
                    .sandbox
                    .join("planned-clients")
                    .join(name)
                    .join("mount"),
                mount_point: mount,
                stdout_log: context
                    .sandbox
                    .join("planned-clients")
                    .join(name)
                    .join("stdout.log"),
                stderr_log: context
                    .sandbox
                    .join("planned-clients")
                    .join(name)
                    .join("stderr.log"),
                tracing_log: context
                    .sandbox
                    .join("planned-clients")
                    .join(name)
                    .join("studiofs-tracing.log"),
                process_id: None,
                state: ClientLifecycleState::Planned,
            }
        })
        .collect::<Vec<_>>();
    let stale_writer = planned_clients[LWW_RECONNECT_STALE_WRITER_INDEX].clone();
    let cold_observer = planned_clients[LWW_RECONNECT_COLD_OBSERVER_INDEX].clone();
    let mut operations = vec![lww_reconnect_client_operation(
        started,
        MountedLwwReconnectPhase::StaleWriterOffline,
        1,
        &stale_writer,
        "stale_writer_offline",
        &stale_writer.name,
        SharedOperation::ClientCrash,
        None,
    )];
    operations.push(lww_reconnect_planned_write(
        context,
        started,
        1,
        &planned_clients[0],
    ));
    operations.push(lww_reconnect_planned_write(
        context,
        started,
        2,
        &planned_clients[1],
    ));
    operations.push(lww_reconnect_client_operation(
        started,
        MountedLwwReconnectPhase::StaleWriterReturn,
        1,
        &stale_writer,
        "stale_writer_restart",
        &stale_writer.name,
        SharedOperation::ClientRestart,
        None,
    ));
    operations.push(lww_reconnect_planned_write(
        context,
        started,
        3,
        &stale_writer,
    ));
    operations.push(lww_reconnect_client_operation(
        started,
        MountedLwwReconnectPhase::ReplayCrashRestart,
        1,
        &stale_writer,
        "stale_writer_replay_crash",
        &stale_writer.name,
        SharedOperation::ClientCrash,
        None,
    ));
    operations.push(lww_reconnect_client_operation(
        started,
        MountedLwwReconnectPhase::ReplayCrashRestart,
        2,
        &stale_writer,
        "stale_writer_replay_restart",
        &stale_writer.name,
        SharedOperation::ClientRestart,
        None,
    ));
    operations.push(lww_reconnect_observed_operation(
        started,
        MountedLwwReconnectPhase::ColdObserverBootstrap,
        "cold_observer_mount",
        SharedOperation::MountedOnly {
            name: "cold_observer_mount".to_string(),
            reason: "mounting a late cold observer is mounted-E2E only".to_string(),
        },
    ));
    for operation in &mut operations {
        operation.result = "planned".to_string();
        write_mounted_lww_reconnect_operations(
            context,
            lww_phase_for_operation(operation),
            std::slice::from_ref(operation),
        )?;
    }

    let expected_winner = lww_expected_winner(context, &stale_writer.name);
    let oracle = LwwOracleReport {
        path: expected_winner.path.clone(),
        expected_client: expected_winner.client.clone(),
        expected_checksum_sha256: expected_winner.checksum_sha256.clone(),
        expected_oracle_order: expected_winner.oracle_order,
        matched: true,
        clients: LWW_RECONNECT_CLIENT_NAMES
            .iter()
            .map(|name| LwwOracleClientResult {
                client: (*name).to_string(),
                checksum_sha256: Some(expected_winner.checksum_sha256.clone()),
                matched: true,
                error: None,
            })
            .collect(),
    };
    write_lww_oracle_report(context, &oracle)?;
    let summary = MountedLwwReconnectSummary {
        scenario: MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE.to_string(),
        seed: context.seed,
        bootstrap: None,
        warm_clients: LWW_RECONNECT_CLIENT_NAMES
            .iter()
            .take(LWW_RECONNECT_WARM_CLIENTS)
            .map(|name| (*name).to_string())
            .collect(),
        stale_writer: stale_writer.name,
        cold_observer: cold_observer.name,
        accepted_operations: 0,
        rejected_operations: 0,
        operations,
        offline_elapsed_ms: 0,
        restart_elapsed_ms: 0,
        final_convergence_elapsed_ms: 0,
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE}/operations.ndjson"
        ),
        expected_winner,
        oracle,
        final_validator: TreeComparison {
            converged: true,
            reference_client: Some(LWW_RECONNECT_CLIENT_NAMES[0].to_string()),
            compared_clients: LWW_RECONNECT_CLIENT_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
    };
    write_mounted_lww_reconnect_summary(context, &summary)?;
    Ok(summary)
}

fn lww_reconnect_planned_write(
    context: &ScenarioContext,
    started: Instant,
    oracle_order: u32,
    client: &ClientSpec,
) -> MountedChaosOperation {
    let payload = lww_reconnect_payload(context, &client.name, oracle_order);
    let oracle_path = lww_reconnect_oracle_path(context);
    lww_reconnect_client_operation(
        started,
        if oracle_order == 3 {
            MountedLwwReconnectPhase::StaleWriterReturn
        } else {
            MountedLwwReconnectPhase::OnlineWriters
        },
        oracle_order as usize,
        client,
        lww_write_operation_name(oracle_order),
        &oracle_path,
        SharedOperation::Write {
            path: oracle_path.clone(),
            offset: Some(0),
            bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            checksum_sha256: Some(bytes_sha256(&payload)),
        },
        None,
    )
}

fn lww_phase_for_operation(operation: &MountedChaosOperation) -> MountedLwwReconnectPhase {
    match operation.operation.as_str() {
        "stale_writer_offline" => MountedLwwReconnectPhase::StaleWriterOffline,
        "stale_writer_restart" | "lww_write_order_3" => MountedLwwReconnectPhase::StaleWriterReturn,
        "stale_writer_replay_crash" | "stale_writer_replay_restart" => {
            MountedLwwReconnectPhase::ReplayCrashRestart
        }
        "cold_observer_mount" => MountedLwwReconnectPhase::ColdObserverBootstrap,
        _ => MountedLwwReconnectPhase::OnlineWriters,
    }
}

fn lww_reconnect_observed_operation(
    started: Instant,
    phase: MountedLwwReconnectPhase,
    operation: &'static str,
    shared_operation: SharedOperation,
) -> MountedChaosOperation {
    MountedChaosOperation {
        round: phase as usize,
        order: 0,
        client: String::new(),
        operation: operation.to_string(),
        path: operation.to_string(),
        shared_operation,
        result: "observed".to_string(),
        timestamp_ms: elapsed_millis_u64(started),
        error: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn lww_reconnect_client_operation(
    started: Instant,
    phase: MountedLwwReconnectPhase,
    order: usize,
    client: &ClientSpec,
    operation: &'static str,
    path: &str,
    shared_operation: SharedOperation,
    error: Option<io::Error>,
) -> MountedChaosOperation {
    let error = error.map(|error| error.to_string());
    MountedChaosOperation {
        round: phase as usize,
        order,
        client: client.name.clone(),
        operation: operation.to_string(),
        path: path.to_string(),
        shared_operation,
        result: if error.is_some() {
            "rejected".to_string()
        } else {
            "accepted".to_string()
        },
        timestamp_ms: elapsed_millis_u64(started),
        error,
    }
}

fn write_mounted_lww_reconnect_operations(
    context: &ScenarioContext,
    phase: MountedLwwReconnectPhase,
    operations: &[MountedChaosOperation],
) -> Result<(), ScenarioError> {
    let path = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path)?;
    for operation in operations {
        let mut record = shared_operation_record(
            &writer,
            MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE,
            operation.shared_operation.clone(),
        )
        .with_timestamp_ms(operation.timestamp_ms)
        .with_outcome(mounted_chaos_outcome(operation))
        .with_metadata("phase", phase.as_str())
        .with_metadata("operation", operation.operation.clone());
        if !operation.client.is_empty() {
            record = record.with_client(&operation.client);
        }
        if let Some(order) = operation
            .operation
            .strip_prefix("lww_write_order_")
            .and_then(|order| order.parse::<u32>().ok())
        {
            record = record.with_metadata("oracle_order", order.to_string());
            if order == 3 {
                record = record.with_metadata("expected_winner", "true");
            }
        }
        if operation.error.is_some() {
            record = record.with_metadata("result", &operation.result);
        }
        append_shared_record(&mut writer, &record)?;
    }
    Ok(())
}

fn lww_expected_winner(context: &ScenarioContext, client: &str) -> LwwExpectedWinner {
    let payload = lww_reconnect_payload(context, client, 3);
    LwwExpectedWinner {
        client: client.to_string(),
        path: lww_reconnect_oracle_path(context),
        checksum_sha256: bytes_sha256(&payload),
        bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        oracle_order: 3,
    }
}

fn lww_oracle_report(
    snapshots: &[crate::mounted_tree::MountedTreeSnapshot],
    expected: &LwwExpectedWinner,
) -> LwwOracleReport {
    let clients = snapshots
        .iter()
        .map(|snapshot| {
            let entry = snapshot
                .entries
                .iter()
                .find(|entry| entry.path == expected.path);
            match entry {
                Some(entry) => {
                    let checksum = entry.checksum_sha256.clone();
                    let matched = checksum.as_deref() == Some(expected.checksum_sha256.as_str());
                    LwwOracleClientResult {
                        client: snapshot.client.clone(),
                        checksum_sha256: checksum,
                        matched,
                        error: if matched {
                            None
                        } else {
                            Some("checksum did not match expected LWW winner".to_string())
                        },
                    }
                }
                None => LwwOracleClientResult {
                    client: snapshot.client.clone(),
                    checksum_sha256: None,
                    matched: false,
                    error: Some("expected LWW winner path is missing".to_string()),
                },
            }
        })
        .collect::<Vec<_>>();
    LwwOracleReport {
        path: expected.path.clone(),
        expected_client: expected.client.clone(),
        expected_checksum_sha256: expected.checksum_sha256.clone(),
        expected_oracle_order: expected.oracle_order,
        matched: clients.iter().all(|client| client.matched),
        clients,
    }
}

fn write_lww_oracle_report(
    context: &ScenarioContext,
    oracle: &LwwOracleReport,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("lww-oracle.json"),
        serde_json::to_vec_pretty(oracle)?,
    )?;
    Ok(())
}

fn write_mounted_lww_reconnect_summary(
    context: &ScenarioContext,
    summary: &MountedLwwReconnectSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("mounted-lww-reconnect-chaos-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

fn lww_reconnect_oracle_path(context: &ScenarioContext) -> String {
    run_unique_path(context, "lww-chaos", "winner.txt")
}

fn lww_reconnect_payload(context: &ScenarioContext, client: &str, oracle_order: u32) -> Vec<u8> {
    format!(
        "studiofs lww reconnect seed={} run_id={} client={client} oracle_order={oracle_order}\n",
        context.seed, context.run_id
    )
    .into_bytes()
}

struct MountedOpenHandleConvergence;

impl Scenario for MountedOpenHandleConvergence {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_OPEN_HANDLE_CONVERGENCE,
            description: "mounted open-handle convergence: unlink and rename while handles stay open",
            requires_studiofs_bin: true,
            requires_mount: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut driver = RealOpenHandleDriver(MultiClientController::new(
                config.clone(),
                SystemClientProcessExecutor,
            ));
            run_open_handle_with_driver(
                context,
                &mut driver,
                OPEN_HANDLE_CONVERGENCE_TIMEOUT,
                OPEN_HANDLE_CONVERGENCE_POLL_INTERVAL,
            )?;
        } else {
            write_open_handle_plan_artifacts(context)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("mounted-open-handle-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: OpenHandleSummary = serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": MOUNTED_OPEN_HANDLE_CONVERGENCE,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged && summary.checks.iter().all(|check| check.passed) {
                "passed"
            } else {
                "failed"
            },
            "open_unlink": summary.open_unlink,
            "open_rename": summary.open_rename,
            "checks": summary.checks,
            "final_validator": summary.final_validator,
            "operation_log": summary.operation_log,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{MOUNTED_OPEN_HANDLE_CONVERGENCE}/mounted-open-handle-summary.json"
            ),
            format!("artifacts/scenarios/{MOUNTED_OPEN_HANDLE_CONVERGENCE}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

trait OpenHandleDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn after_initial_write(&mut self, _clients: &[ClientSpec]) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn after_namespace_mutation(&mut self, _clients: &[ClientSpec]) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

struct RealOpenHandleDriver(MultiClientController<SystemClientProcessExecutor>);

impl OpenHandleDriver for RealOpenHandleDriver {
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
struct OpenHandleCaseSummary {
    name: String,
    handle_client: String,
    mutator_client: String,
    observer_client: String,
    original_path: String,
    renamed_path: Option<String>,
    bytes_read_from_open_handle: u64,
    expected_bytes_from_open_handle: u64,
    read_checksum_sha256: String,
    expected_checksum_sha256: String,
    deleted_path_absent: bool,
    old_path_absent: bool,
    renamed_path_visible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Public validation check emitted when mounted open-handle convergence fails.
pub struct OpenHandleCheck {
    /// Stable check identifier.
    name: String,
    /// Whether the check passed.
    passed: bool,
    /// Human-readable diagnostic detail for reports and errors.
    detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OpenHandleOperation {
    order: usize,
    client: String,
    operation: String,
    path: String,
    shared_operation: SharedOperation,
    result: String,
    timestamp_ms: u64,
    error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OpenHandleSummary {
    scenario: String,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: usize,
    open_unlink: OpenHandleCaseSummary,
    open_rename: OpenHandleCaseSummary,
    operations: Vec<OpenHandleOperation>,
    operation_log: String,
    final_validator: TreeComparison,
    checks: Vec<OpenHandleCheck>,
}

fn run_open_handle_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn OpenHandleDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<OpenHandleSummary, ScenarioError> {
    let result = run_open_handle_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::MountedOpenHandleUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn run_open_handle_body(
    context: &ScenarioContext,
    driver: &mut dyn OpenHandleDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<OpenHandleSummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: OPEN_HANDLE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < OPEN_HANDLE_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientOpenHandleClients {
            expected: OPEN_HANDLE_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }
    let clients = clients
        .into_iter()
        .take(OPEN_HANDLE_CLIENT_NAMES.len())
        .collect::<Vec<_>>();

    establish_open_handle_initial_tree(context, &clients[0])?;
    driver.after_initial_write(&clients)?;
    wait_for_convergence(
        &client_mounts(context, &clients),
        context.scenario_dir.join("initial-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let started = Instant::now();
    let mut operations = Vec::new();
    let open_unlink = execute_open_unlink_case(context, started, &clients, &mut operations)?;
    driver.after_namespace_mutation(&clients)?;
    wait_for_open_handle_phase_convergence(
        context,
        started,
        &clients,
        "open_unlink",
        convergence_timeout,
        poll_interval,
    )?;
    let open_rename = execute_open_rename_case(context, started, &clients, &mut operations)?;
    driver.after_namespace_mutation(&clients)?;
    write_open_handle_operations(context, &operations)?;
    finish_open_handle(OpenHandleFinishInput {
        context,
        bootstrap: Some(bootstrap),
        clients,
        open_unlink,
        open_rename,
        operations,
        convergence_timeout,
        poll_interval,
    })
}

fn write_open_handle_plan_artifacts(
    context: &ScenarioContext,
) -> Result<OpenHandleSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let operations = planned_open_handle_operations(context, started);
    write_open_handle_operations(context, &operations)?;
    let unlink_path = open_unlink_path(context);
    let rename_path = open_rename_path(context);
    let rename_dest = open_rename_dest(context);
    let open_unlink = planned_open_handle_case(
        context,
        "open_unlink",
        &unlink_path,
        None,
        true,
        true,
        false,
    );
    let open_rename = planned_open_handle_case(
        context,
        "open_rename",
        &rename_path,
        Some(&rename_dest),
        false,
        true,
        true,
    );
    let checks = open_handle_checks(&open_unlink, &open_rename, true);
    let summary = OpenHandleSummary {
        scenario: MOUNTED_OPEN_HANDLE_CONVERGENCE.to_string(),
        bootstrap: None,
        clients: OPEN_HANDLE_CLIENT_NAMES.len(),
        open_unlink,
        open_rename,
        operations,
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_OPEN_HANDLE_CONVERGENCE}/operations.ndjson"
        ),
        final_validator: TreeComparison {
            converged: true,
            reference_client: Some(OPEN_HANDLE_CLIENT_NAMES[0].to_string()),
            compared_clients: OPEN_HANDLE_CLIENT_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            differences: Vec::new(),
        },
        checks,
    };
    write_open_handle_summary(context, &summary)?;
    Ok(summary)
}

fn wait_for_open_handle_phase_convergence(
    context: &ScenarioContext,
    started: Instant,
    clients: &[ClientSpec],
    phase: &str,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    record_open_handle_checkpoint(
        context,
        started,
        None,
        &format!("{phase}.convergence.started"),
        None,
    )?;
    let convergence = wait_for_convergence(
        &client_mounts(context, clients),
        context.scenario_dir.join(format!("{phase}-validation")),
        convergence_timeout,
        poll_interval,
    )?;
    record_open_handle_checkpoint(
        context,
        started,
        None,
        &format!("{phase}.convergence.finished"),
        None,
    )?;
    Ok(convergence)
}

fn establish_open_handle_initial_tree(
    context: &ScenarioContext,
    client: &ClientSpec,
) -> Result<(), ScenarioError> {
    for relative_path in [open_unlink_path(context), open_rename_path(context)] {
        write_mounted_chaos_initial_file(
            client.mount_point.join(relative_path),
            &open_handle_initial_payload(context),
        )?;
    }
    Ok(())
}

fn execute_open_unlink_case(
    context: &ScenarioContext,
    started: Instant,
    clients: &[ClientSpec],
    operations: &mut Vec<OpenHandleOperation>,
) -> Result<OpenHandleCaseSummary, ScenarioError> {
    let handle_client = &clients[0];
    let mutator_client = &clients[1];
    let observer_client = &clients[2];
    let unlink_path = open_unlink_path(context);
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_unlink.open.started",
        Some(&unlink_path),
    )?;
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(handle_client.mount_point.join(&unlink_path))?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_unlink.open.finished",
        Some(&unlink_path),
    )?;
    push_open_handle_operation(
        started,
        operations,
        handle_client,
        "open_handle",
        &unlink_path,
        SharedOperation::MountedOnly {
            name: "open_handle".to_string(),
            reason: "open file handle state is mounted-product behavior".to_string(),
        },
    );
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_unlink.append.started",
        Some(&unlink_path),
    )?;
    append_to_open_handle(
        context,
        started,
        operations,
        handle_client,
        &unlink_path,
        &mut file,
    )?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_unlink.append.finished",
        Some(OPEN_UNLINK_PATH),
    )?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(mutator_client),
        "open_unlink.unlink.started",
        Some(&unlink_path),
    )?;
    fs::remove_file(mutator_client.mount_point.join(&unlink_path))?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(mutator_client),
        "open_unlink.unlink.finished",
        Some(&unlink_path),
    )?;
    push_open_handle_operation(
        started,
        operations,
        mutator_client,
        "unlink",
        &unlink_path,
        SharedOperation::Unlink {
            path: unlink_path.clone(),
        },
    );
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_unlink.flush_sync_read.started",
        Some(&unlink_path),
    )?;
    let readback =
        flush_sync_read_open_handle(started, operations, handle_client, &unlink_path, &mut file)?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_unlink.flush_sync_read.finished",
        Some(&unlink_path),
    )?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_unlink.release.started",
        Some(&unlink_path),
    )?;
    drop(file);
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_unlink.release.finished",
        Some(&unlink_path),
    )?;
    push_open_handle_operation(
        started,
        operations,
        handle_client,
        "release",
        &unlink_path,
        SharedOperation::Release {
            path: Some(unlink_path.clone()),
        },
    );
    let deleted_path_absent = !observer_client.mount_point.join(&unlink_path).exists();
    Ok(OpenHandleCaseSummary {
        name: "open_unlink".to_string(),
        handle_client: handle_client.name.clone(),
        mutator_client: mutator_client.name.clone(),
        observer_client: observer_client.name.clone(),
        original_path: unlink_path,
        renamed_path: None,
        bytes_read_from_open_handle: readback.bytes,
        expected_bytes_from_open_handle: expected_open_handle_payload_bytes(context),
        read_checksum_sha256: readback.checksum_sha256,
        expected_checksum_sha256: expected_open_handle_payload_checksum(context),
        deleted_path_absent,
        old_path_absent: deleted_path_absent,
        renamed_path_visible: false,
    })
}

fn execute_open_rename_case(
    context: &ScenarioContext,
    started: Instant,
    clients: &[ClientSpec],
    operations: &mut Vec<OpenHandleOperation>,
) -> Result<OpenHandleCaseSummary, ScenarioError> {
    let handle_client = &clients[0];
    let mutator_client = &clients[1];
    let observer_client = &clients[2];
    let rename_path = open_rename_path(context);
    let rename_dest = open_rename_dest(context);
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_rename.open.started",
        Some(&rename_path),
    )?;
    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .open(handle_client.mount_point.join(&rename_path))?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_rename.open.finished",
        Some(&rename_path),
    )?;
    push_open_handle_operation(
        started,
        operations,
        handle_client,
        "open_handle",
        &rename_path,
        SharedOperation::MountedOnly {
            name: "open_handle".to_string(),
            reason: "open file handle state is mounted-product behavior".to_string(),
        },
    );
    record_open_handle_checkpoint(
        context,
        started,
        Some(mutator_client),
        "open_rename.rename.started",
        Some(&rename_path),
    )?;
    fs::rename(
        mutator_client.mount_point.join(&rename_path),
        mutator_client.mount_point.join(&rename_dest),
    )?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(mutator_client),
        "open_rename.rename.finished",
        Some(&rename_dest),
    )?;
    push_open_handle_operation(
        started,
        operations,
        mutator_client,
        "rename",
        &format!("{rename_path} -> {rename_dest}"),
        SharedOperation::Rename {
            from: rename_path.clone(),
            to: rename_dest.clone(),
        },
    );
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_rename.append.started",
        Some(&rename_path),
    )?;
    append_to_open_handle(
        context,
        started,
        operations,
        handle_client,
        &rename_path,
        &mut file,
    )?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_rename.append.finished",
        Some(&rename_path),
    )?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_rename.flush_sync_read.started",
        Some(&rename_path),
    )?;
    let readback =
        flush_sync_read_open_handle(started, operations, handle_client, &rename_path, &mut file)?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_rename.flush_sync_read.finished",
        Some(&rename_path),
    )?;
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_rename.release.started",
        Some(&rename_path),
    )?;
    drop(file);
    record_open_handle_checkpoint(
        context,
        started,
        Some(handle_client),
        "open_rename.release.finished",
        Some(&rename_path),
    )?;
    push_open_handle_operation(
        started,
        operations,
        handle_client,
        "release",
        &rename_path,
        SharedOperation::Release {
            path: Some(rename_path.clone()),
        },
    );
    let old_path_absent = !observer_client.mount_point.join(&rename_path).exists();
    let renamed_path_visible = observer_client.mount_point.join(&rename_dest).exists();
    Ok(OpenHandleCaseSummary {
        name: "open_rename".to_string(),
        handle_client: handle_client.name.clone(),
        mutator_client: mutator_client.name.clone(),
        observer_client: observer_client.name.clone(),
        original_path: rename_path,
        renamed_path: Some(rename_dest),
        bytes_read_from_open_handle: readback.bytes,
        expected_bytes_from_open_handle: expected_open_handle_payload_bytes(context),
        read_checksum_sha256: readback.checksum_sha256,
        expected_checksum_sha256: expected_open_handle_payload_checksum(context),
        deleted_path_absent: false,
        old_path_absent,
        renamed_path_visible,
    })
}

struct OpenHandleFinishInput<'a> {
    context: &'a ScenarioContext,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: Vec<ClientSpec>,
    open_unlink: OpenHandleCaseSummary,
    open_rename: OpenHandleCaseSummary,
    operations: Vec<OpenHandleOperation>,
    convergence_timeout: Duration,
    poll_interval: Duration,
}

fn finish_open_handle(
    input: OpenHandleFinishInput<'_>,
) -> Result<OpenHandleSummary, ScenarioError> {
    let OpenHandleFinishInput {
        context,
        bootstrap,
        clients,
        mut open_unlink,
        mut open_rename,
        operations,
        convergence_timeout,
        poll_interval,
    } = input;
    let convergence = wait_for_convergence(
        &client_mounts(context, &clients),
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let unlink_path = open_unlink_path(context);
    let rename_path = open_rename_path(context);
    let rename_dest = open_rename_dest(context);
    open_unlink.deleted_path_absent = snapshots_path_absent(&convergence.snapshots, &unlink_path);
    open_unlink.old_path_absent = open_unlink.deleted_path_absent;
    open_rename.old_path_absent = snapshots_path_absent(&convergence.snapshots, &rename_path);
    open_rename.renamed_path_visible = snapshots_path_visible(&convergence.snapshots, &rename_dest);
    let checks = open_handle_checks(&open_unlink, &open_rename, convergence.comparison.converged);
    if checks.iter().any(|check| !check.passed) {
        return Err(ScenarioError::MountedOpenHandleValidation { checks });
    }
    let summary = OpenHandleSummary {
        scenario: MOUNTED_OPEN_HANDLE_CONVERGENCE.to_string(),
        bootstrap,
        clients: clients.len(),
        open_unlink,
        open_rename,
        operations,
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_OPEN_HANDLE_CONVERGENCE}/operations.ndjson"
        ),
        final_validator: convergence.comparison,
        checks,
    };
    write_open_handle_summary(context, &summary)?;
    Ok(summary)
}

fn snapshots_path_absent(
    snapshots: &[crate::mounted_tree::MountedTreeSnapshot],
    path: &str,
) -> bool {
    snapshots
        .iter()
        .all(|snapshot| snapshot.entries.iter().all(|entry| entry.path != path))
}

fn snapshots_path_visible(
    snapshots: &[crate::mounted_tree::MountedTreeSnapshot],
    path: &str,
) -> bool {
    !snapshots.is_empty()
        && snapshots
            .iter()
            .all(|snapshot| snapshot.entries.iter().any(|entry| entry.path == path))
}

struct OpenHandleReadback {
    bytes: u64,
    checksum_sha256: String,
}

fn append_to_open_handle(
    context: &ScenarioContext,
    started: Instant,
    operations: &mut Vec<OpenHandleOperation>,
    client: &ClientSpec,
    path: &str,
    file: &mut fs::File,
) -> Result<(), ScenarioError> {
    let append_payload = open_handle_append_payload(context);
    file.write_all(&append_payload)?;
    push_open_handle_operation(
        started,
        operations,
        client,
        "append_open_handle",
        path,
        SharedOperation::Append {
            path: path.to_string(),
            bytes: u64::try_from(append_payload.len()).unwrap_or(u64::MAX),
            checksum_sha256: Some(bytes_sha256(&append_payload)),
        },
    );
    Ok(())
}

fn flush_sync_read_open_handle(
    started: Instant,
    operations: &mut Vec<OpenHandleOperation>,
    client: &ClientSpec,
    path: &str,
    file: &mut fs::File,
) -> Result<OpenHandleReadback, ScenarioError> {
    file.flush()?;
    push_open_handle_operation(
        started,
        operations,
        client,
        "flush",
        path,
        SharedOperation::Flush {
            path: Some(path.to_string()),
        },
    );
    file.sync_all()?;
    push_open_handle_operation(
        started,
        operations,
        client,
        "fsync",
        path,
        SharedOperation::Fsync {
            path: path.to_string(),
        },
    );
    file.seek(SeekFrom::Start(0))?;
    let mut payload = Vec::new();
    file.read_to_end(&mut payload)?;
    let checksum_sha256 = bytes_sha256(&payload);
    push_open_handle_operation(
        started,
        operations,
        client,
        "read_open_handle",
        path,
        SharedOperation::Read {
            path: path.to_string(),
            bytes: Some(u64::try_from(payload.len()).unwrap_or(u64::MAX)),
            checksum_sha256: Some(checksum_sha256.clone()),
        },
    );
    Ok(OpenHandleReadback {
        bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
        checksum_sha256,
    })
}

fn planned_open_handle_case(
    context: &ScenarioContext,
    name: &str,
    original_path: &str,
    renamed_path: Option<&str>,
    deleted_path_absent: bool,
    old_path_absent: bool,
    renamed_path_visible: bool,
) -> OpenHandleCaseSummary {
    OpenHandleCaseSummary {
        name: name.to_string(),
        handle_client: "client-a".to_string(),
        mutator_client: "client-b".to_string(),
        observer_client: "client-c".to_string(),
        original_path: original_path.to_string(),
        renamed_path: renamed_path.map(str::to_string),
        bytes_read_from_open_handle: u64::try_from(open_handle_expected_payload(context).len())
            .unwrap_or(u64::MAX),
        expected_bytes_from_open_handle: expected_open_handle_payload_bytes(context),
        read_checksum_sha256: expected_open_handle_payload_checksum(context),
        expected_checksum_sha256: expected_open_handle_payload_checksum(context),
        deleted_path_absent,
        old_path_absent,
        renamed_path_visible,
    }
}

fn expected_open_handle_payload_bytes(context: &ScenarioContext) -> u64 {
    u64::try_from(open_handle_expected_payload(context).len()).unwrap_or(u64::MAX)
}

fn expected_open_handle_payload_checksum(context: &ScenarioContext) -> String {
    bytes_sha256(&open_handle_expected_payload(context))
}

fn open_unlink_path(context: &ScenarioContext) -> String {
    run_unique_path(context, "open-handles", "unlink-target.txt")
}

fn open_rename_path(context: &ScenarioContext) -> String {
    run_unique_path(context, "open-handles", "rename-target.txt")
}

fn open_rename_dest(context: &ScenarioContext) -> String {
    run_unique_path(context, "open-handles", "renamed-target.txt")
}

fn open_handle_initial_payload(context: &ScenarioContext) -> Vec<u8> {
    run_unique_payload(context, MOUNTED_OPEN_HANDLE_CONVERGENCE, "initial")
}

fn open_handle_append_payload(context: &ScenarioContext) -> Vec<u8> {
    run_unique_payload(context, MOUNTED_OPEN_HANDLE_CONVERGENCE, "append")
}

fn open_handle_expected_payload(context: &ScenarioContext) -> Vec<u8> {
    let mut payload = open_handle_initial_payload(context);
    payload.extend_from_slice(&open_handle_append_payload(context));
    payload
}

fn planned_open_handle_operations(
    context: &ScenarioContext,
    started: Instant,
) -> Vec<OpenHandleOperation> {
    let unlink_path = open_unlink_path(context);
    let rename_path = open_rename_path(context);
    let rename_dest = open_rename_dest(context);
    let append_payload = open_handle_append_payload(context);
    [
        (
            "client-a",
            "open_handle",
            unlink_path.clone(),
            SharedOperation::MountedOnly {
                name: "open_handle".to_string(),
                reason: "open file handle state is mounted-product behavior".to_string(),
            },
        ),
        (
            "client-a",
            "append_open_handle",
            unlink_path.clone(),
            SharedOperation::Append {
                path: unlink_path.clone(),
                bytes: u64::try_from(append_payload.len()).unwrap_or(u64::MAX),
                checksum_sha256: Some(bytes_sha256(&append_payload)),
            },
        ),
        (
            "client-b",
            "unlink",
            unlink_path.clone(),
            SharedOperation::Unlink {
                path: unlink_path.clone(),
            },
        ),
        (
            "client-a",
            "flush",
            unlink_path.clone(),
            SharedOperation::Flush {
                path: Some(unlink_path.clone()),
            },
        ),
        (
            "client-a",
            "fsync",
            unlink_path.clone(),
            SharedOperation::Fsync {
                path: unlink_path.clone(),
            },
        ),
        (
            "client-a",
            "release",
            unlink_path.clone(),
            SharedOperation::Release {
                path: Some(unlink_path.clone()),
            },
        ),
        (
            "client-a",
            "open_handle",
            rename_path.clone(),
            SharedOperation::MountedOnly {
                name: "open_handle".to_string(),
                reason: "open file handle state is mounted-product behavior".to_string(),
            },
        ),
        (
            "client-b",
            "rename",
            rename_path.clone(),
            SharedOperation::Rename {
                from: rename_path.clone(),
                to: rename_dest.clone(),
            },
        ),
        (
            "client-a",
            "append_open_handle",
            rename_path.clone(),
            SharedOperation::Append {
                path: rename_path.clone(),
                bytes: u64::try_from(append_payload.len()).unwrap_or(u64::MAX),
                checksum_sha256: Some(bytes_sha256(&append_payload)),
            },
        ),
        (
            "client-a",
            "flush",
            rename_path.clone(),
            SharedOperation::Flush {
                path: Some(rename_path.clone()),
            },
        ),
        (
            "client-a",
            "fsync",
            rename_path.clone(),
            SharedOperation::Fsync {
                path: rename_path.clone(),
            },
        ),
        (
            "client-a",
            "release",
            rename_path.clone(),
            SharedOperation::Release {
                path: Some(rename_path.clone()),
            },
        ),
    ]
    .into_iter()
    .enumerate()
    .map(
        |(index, (client, operation, path, shared_operation))| OpenHandleOperation {
            order: index + 1,
            client: client.to_string(),
            operation: operation.to_string(),
            path,
            shared_operation,
            result: "planned".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: None,
        },
    )
    .collect()
}

fn push_open_handle_operation(
    started: Instant,
    operations: &mut Vec<OpenHandleOperation>,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    shared_operation: SharedOperation,
) {
    operations.push(OpenHandleOperation {
        order: operations.len() + 1,
        client: client.name.clone(),
        operation: operation.to_string(),
        path: path.to_string(),
        shared_operation,
        result: "accepted".to_string(),
        timestamp_ms: elapsed_millis_u64(started),
        error: None,
    });
}

fn open_handle_checks(
    open_unlink: &OpenHandleCaseSummary,
    open_rename: &OpenHandleCaseSummary,
    converged: bool,
) -> Vec<OpenHandleCheck> {
    let mut checks = vec![
        OpenHandleCheck {
            name: "final_namespace_converged".to_string(),
            passed: converged,
            detail: "all mounted clients have the same final namespace".to_string(),
        },
        OpenHandleCheck {
            name: "unlink_deleted_path_absent_after_release".to_string(),
            passed: open_unlink.deleted_path_absent,
            detail: open_unlink.original_path.clone(),
        },
        OpenHandleCheck {
            name: "rename_old_path_absent_after_release".to_string(),
            passed: open_rename.old_path_absent,
            detail: open_rename.original_path.clone(),
        },
        OpenHandleCheck {
            name: "rename_destination_visible_after_release".to_string(),
            passed: open_rename.renamed_path_visible,
            detail: open_rename.renamed_path.clone().unwrap_or_default(),
        },
    ];
    checks.extend(open_handle_payload_checks("unlink", open_unlink));
    checks.extend(open_handle_payload_checks("rename", open_rename));
    checks
}

fn open_handle_payload_checks(prefix: &str, case: &OpenHandleCaseSummary) -> [OpenHandleCheck; 2] {
    [
        OpenHandleCheck {
            name: format!("{prefix}_handle_read_expected_bytes"),
            passed: case.bytes_read_from_open_handle == case.expected_bytes_from_open_handle,
            detail: format!(
                "{} bytes read, expected {}",
                case.bytes_read_from_open_handle, case.expected_bytes_from_open_handle
            ),
        },
        OpenHandleCheck {
            name: format!("{prefix}_handle_checksum_matches"),
            passed: case.read_checksum_sha256 == case.expected_checksum_sha256,
            detail: format!(
                "read {}, expected {}",
                case.read_checksum_sha256, case.expected_checksum_sha256
            ),
        },
    ]
}

fn write_open_handle_operations(
    context: &ScenarioContext,
    operations: &[OpenHandleOperation],
) -> Result<(), ScenarioError> {
    let path = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path)?;
    for operation in operations {
        let record = shared_operation_record(
            &writer,
            MOUNTED_OPEN_HANDLE_CONVERGENCE,
            operation.shared_operation.clone(),
        )
        .with_client(&operation.client)
        .with_timestamp_ms(operation.timestamp_ms)
        .with_outcome(open_handle_outcome(operation))
        .with_metadata("operation", operation.operation.clone());
        append_shared_record(&mut writer, &record)?;
    }
    Ok(())
}

fn record_open_handle_checkpoint(
    context: &ScenarioContext,
    started: Instant,
    client: Option<&ClientSpec>,
    name: &str,
    path: Option<&str>,
) -> Result<(), ScenarioError> {
    let path_on_disk = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path_on_disk)?;
    let mut record = shared_operation_record(
        &writer,
        MOUNTED_OPEN_HANDLE_CONVERGENCE,
        SharedOperation::SchedulerCheckpoint {
            name: name.to_string(),
        },
    )
    .with_timestamp_ms(elapsed_millis_u64(started))
    .with_outcome(OperationOutcome::Observed)
    .with_metadata("checkpoint", name.to_string());
    if let Some(client) = client {
        record = record.with_client(&client.name);
    }
    if let Some(path) = path {
        record = record.with_metadata("path", path.to_string());
    }
    append_shared_record(&mut writer, &record)
}

fn open_handle_outcome(operation: &OpenHandleOperation) -> OperationOutcome {
    match operation.result.as_str() {
        "planned" => OperationOutcome::Planned,
        "accepted" => OperationOutcome::Accepted,
        "rejected" => OperationOutcome::Rejected {
            error: operation
                .error
                .clone()
                .unwrap_or_else(|| "operation rejected".to_string()),
        },
        _ => OperationOutcome::Observed,
    }
}

fn write_open_handle_summary(
    context: &ScenarioContext,
    summary: &OpenHandleSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("mounted-open-handle-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
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

    fn initial_paths(self, context: &ScenarioContext) -> Vec<(String, Vec<u8>)> {
        let root = namespace_collision_root(context);
        match self {
            Self::RenameVsRename | Self::RenameVsUnlink => {
                vec![(
                    format!("{root}/shared/file.txt"),
                    run_unique_payload(context, self.name(), "initial-shared-file"),
                )]
            }
            Self::UnlinkVsAppend | Self::OverwriteVsRead => {
                vec![(
                    format!("{root}/shared/data.txt"),
                    collision_payload(context, "old-committed"),
                )]
            }
            Self::MkdirRmdirVsCreate => Vec::new(),
            Self::ReaddirStatDuringMutation => {
                vec![
                    (
                        format!("{root}/scan/a.txt"),
                        run_unique_payload(context, self.name(), "scan-a"),
                    ),
                    (
                        format!("{root}/scan/b.txt"),
                        run_unique_payload(context, self.name(), "scan-b"),
                    ),
                ]
            }
        }
    }

    fn expected_absent_paths(self, context: &ScenarioContext) -> Vec<String> {
        let root = namespace_collision_root(context);
        match self {
            Self::RenameVsRename | Self::RenameVsUnlink => {
                vec![format!("{root}/shared/file.txt")]
            }
            Self::ReaddirStatDuringMutation => vec![format!("{root}/scan/a.txt")],
            Self::UnlinkVsAppend | Self::OverwriteVsRead | Self::MkdirRmdirVsCreate => Vec::new(),
        }
    }
}

fn namespace_collision_root(context: &ScenarioContext) -> String {
    run_unique_dir(context, "collision")
}

fn collision_payload(context: &ScenarioContext, label: &str) -> Vec<u8> {
    run_unique_payload(context, "namespace-collision", label)
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
            requires_mount: true,
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
    shared_operation: SharedOperation,
    result: String,
    timestamp_ms: u64,
    error: Option<String>,
}

type NamespaceCollisionAction = Box<dyn FnOnce(&ClientSpec) -> io::Result<()> + Send + 'static>;

struct NamespaceCollisionTask {
    order: usize,
    client: ClientSpec,
    operation: &'static str,
    path: String,
    shared_operation: SharedOperation,
    action: NamespaceCollisionAction,
}

impl NamespaceCollisionTask {
    fn new(
        order: usize,
        client: ClientSpec,
        operation: &'static str,
        path: impl Into<String>,
        shared_operation: SharedOperation,
        action: impl FnOnce(&ClientSpec) -> io::Result<()> + Send + 'static,
    ) -> Self {
        Self {
            order,
            client,
            operation,
            path: path.into(),
            shared_operation,
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

    establish_collision_initial_tree(context, kind, &clients[0])?;
    driver.after_conflicting_operations(kind, &clients)?;
    let initial_mounts = client_mounts(context, &clients);
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
    let operations = planned_namespace_collision_operations(context, kind, started);
    write_namespace_collision_operations(context, kind, &operations)?;
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
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    client: &ClientSpec,
) -> Result<(), ScenarioError> {
    let root = namespace_collision_root(context);
    if kind == NamespaceCollisionKind::MkdirRmdirVsCreate {
        fs::create_dir_all(client.mount_point.join(format!("{root}/race")))?;
    }
    for (relative_path, payload) in kind.initial_paths(context) {
        let path = client.mount_point.join(relative_path);
        write_mounted_chaos_initial_file(path, &payload)?;
    }
    Ok(())
}

fn perform_namespace_collision_operations(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    clients: &[ClientSpec],
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let root = namespace_collision_root(context);
    let shared_file = format!("{root}/shared/file.txt");
    let renamed_a = format!("{root}/shared/renamed-a.txt");
    let renamed_b = format!("{root}/shared/renamed-b.txt");
    let renamed = format!("{root}/shared/renamed.txt");
    let shared_data = format!("{root}/shared/data.txt");
    let race_dir = format!("{root}/race");
    let race_subdir = format!("{race_dir}/subdir");
    let race_child = format!("{race_dir}/child.txt");
    let scan_dir = format!("{root}/scan");
    let scan_a = format!("{scan_dir}/a.txt");
    let scan_b = format!("{scan_dir}/b.txt");
    let scan_c = format!("{scan_dir}/c.txt");
    let operations = match kind {
        NamespaceCollisionKind::RenameVsRename => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(
                    1,
                    clients[0].clone(),
                    "rename",
                    format!("{shared_file} -> {renamed_a}"),
                    SharedOperation::Rename {
                        from: shared_file.clone(),
                        to: renamed_a.clone(),
                    },
                    {
                        let shared_file = shared_file.clone();
                        let renamed_a = renamed_a.clone();
                        move |client| {
                            fs::rename(
                                client.mount_point.join(shared_file),
                                client.mount_point.join(renamed_a),
                            )
                        }
                    },
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[1].clone(),
                    "rename",
                    format!("{shared_file} -> {renamed_b}"),
                    SharedOperation::Rename {
                        from: shared_file.clone(),
                        to: renamed_b.clone(),
                    },
                    {
                        let shared_file = shared_file.clone();
                        let renamed_b = renamed_b.clone();
                        move |client| {
                            fs::rename(
                                client.mount_point.join(shared_file),
                                client.mount_point.join(renamed_b),
                            )
                        }
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
                    format!("{shared_file} -> {renamed}"),
                    SharedOperation::Rename {
                        from: shared_file.clone(),
                        to: renamed.clone(),
                    },
                    {
                        let shared_file = shared_file.clone();
                        let renamed = renamed.clone();
                        move |client| {
                            fs::rename(
                                client.mount_point.join(shared_file),
                                client.mount_point.join(renamed),
                            )
                        }
                    },
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[1].clone(),
                    "unlink",
                    shared_file.clone(),
                    SharedOperation::Unlink {
                        path: shared_file.clone(),
                    },
                    {
                        let shared_file = shared_file.clone();
                        move |client| fs::remove_file(client.mount_point.join(shared_file))
                    },
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
                    shared_data.clone(),
                    SharedOperation::Unlink {
                        path: shared_data.clone(),
                    },
                    {
                        let shared_data = shared_data.clone();
                        move |client| fs::remove_file(client.mount_point.join(shared_data))
                    },
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[1].clone(),
                    "append",
                    shared_data.clone(),
                    SharedOperation::Append {
                        path: shared_data.clone(),
                        bytes: u64::try_from(collision_payload(context, "append").len())
                            .unwrap_or(u64::MAX),
                        checksum_sha256: Some(bytes_sha256(&collision_payload(context, "append"))),
                    },
                    {
                        let shared_data = shared_data.clone();
                        let payload = collision_payload(context, "append");
                        move |client| {
                            let mut file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(client.mount_point.join(shared_data))?;
                            file.write_all(&payload)
                        }
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
                    shared_data.clone(),
                    SharedOperation::Read {
                        path: shared_data.clone(),
                        bytes: None,
                        checksum_sha256: None,
                    },
                    {
                        let shared_data = shared_data.clone();
                        let old_payload = collision_payload(context, "old-committed");
                        let new_payload = collision_payload(context, "new-committed");
                        move |client| {
                            repeated_committed_read(
                                client,
                                &shared_data,
                                &old_payload,
                                &new_payload,
                            )
                        }
                    },
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[0].clone(),
                    "overwrite",
                    shared_data.clone(),
                    SharedOperation::Write {
                        path: shared_data.clone(),
                        offset: Some(0),
                        bytes: u64::try_from(collision_payload(context, "new-committed").len())
                            .unwrap_or(u64::MAX),
                        checksum_sha256: Some(bytes_sha256(&collision_payload(
                            context,
                            "new-committed",
                        ))),
                    },
                    {
                        let shared_data = shared_data.clone();
                        let payload = collision_payload(context, "new-committed");
                        move |client| fs::write(client.mount_point.join(shared_data), &payload)
                    },
                ),
                NamespaceCollisionTask::new(
                    3,
                    clients[2].clone(),
                    "read_loop",
                    shared_data.clone(),
                    SharedOperation::Read {
                        path: shared_data.clone(),
                        bytes: None,
                        checksum_sha256: None,
                    },
                    {
                        let shared_data = shared_data.clone();
                        let old_payload = collision_payload(context, "old-committed");
                        let new_payload = collision_payload(context, "new-committed");
                        move |client| {
                            repeated_committed_read(
                                client,
                                &shared_data,
                                &old_payload,
                                &new_payload,
                            )
                        }
                    },
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
                    race_subdir.clone(),
                    SharedOperation::CreateDirectory {
                        path: race_subdir.clone(),
                    },
                    {
                        let race_subdir = race_subdir.clone();
                        move |client| fs::create_dir_all(client.mount_point.join(race_subdir))
                    },
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[1].clone(),
                    "create",
                    race_child.clone(),
                    SharedOperation::CreateFile {
                        path: race_child.clone(),
                        bytes: Some(
                            u64::try_from(collision_payload(context, "child").len())
                                .unwrap_or(u64::MAX),
                        ),
                    },
                    {
                        let race_child = race_child.clone();
                        let payload = collision_payload(context, "child");
                        move |client| fs::write(client.mount_point.join(race_child), &payload)
                    },
                ),
                NamespaceCollisionTask::new(
                    3,
                    clients[2].clone(),
                    "rmdir",
                    race_dir.clone(),
                    SharedOperation::Rmdir {
                        path: race_dir.clone(),
                    },
                    {
                        let race_dir = race_dir.clone();
                        move |client| fs::remove_dir(client.mount_point.join(race_dir))
                    },
                ),
            ],
        ),
        NamespaceCollisionKind::ReaddirStatDuringMutation => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(
                    1,
                    clients[1].clone(),
                    "readdir",
                    scan_dir.clone(),
                    SharedOperation::Readdir {
                        path: scan_dir.clone(),
                    },
                    {
                        let scan_dir = scan_dir.clone();
                        move |client| fs::read_dir(client.mount_point.join(scan_dir)).map(|_| ())
                    },
                ),
                NamespaceCollisionTask::new(
                    2,
                    clients[0].clone(),
                    "rename",
                    format!("{scan_a} -> {scan_c}"),
                    SharedOperation::Rename {
                        from: scan_a.clone(),
                        to: scan_c.clone(),
                    },
                    {
                        let scan_a = scan_a.clone();
                        let scan_c = scan_c.clone();
                        move |client| {
                            fs::rename(
                                client.mount_point.join(scan_a),
                                client.mount_point.join(scan_c),
                            )
                        }
                    },
                ),
                NamespaceCollisionTask::new(
                    3,
                    clients[2].clone(),
                    "stat",
                    scan_b.clone(),
                    SharedOperation::Stat {
                        path: scan_b.clone(),
                    },
                    {
                        let scan_b = scan_b.clone();
                        move |client| fs::metadata(client.mount_point.join(scan_b)).map(|_| ())
                    },
                ),
            ],
        ),
    };
    write_namespace_collision_operations(context, kind, &operations)?;
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
                &task.path,
                task.shared_operation,
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

fn repeated_committed_read(
    client: &ClientSpec,
    relative_path: &str,
    old_payload: &[u8],
    new_payload: &[u8],
) -> io::Result<()> {
    for _ in 0..OVERWRITE_READ_ATTEMPTS {
        let payload = fs::read(client.mount_point.join(relative_path))?;
        if payload != old_payload && payload != new_payload {
            return Err(io::Error::other(format!(
                "read observed partial or unexpected payload for {relative_path}"
            )));
        }
        thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn planned_namespace_collision_operations(
    context: &ScenarioContext,
    kind: NamespaceCollisionKind,
    started: Instant,
) -> Vec<NamespaceCollisionOperation> {
    let root = namespace_collision_root(context);
    let shared_file = format!("{root}/shared/file.txt");
    let renamed_a = format!("{root}/shared/renamed-a.txt");
    let renamed_b = format!("{root}/shared/renamed-b.txt");
    let renamed = format!("{root}/shared/renamed.txt");
    let shared_data = format!("{root}/shared/data.txt");
    let race_dir = format!("{root}/race");
    let race_subdir = format!("{race_dir}/subdir");
    let race_child = format!("{race_dir}/child.txt");
    let scan_dir = format!("{root}/scan");
    let scan_a = format!("{scan_dir}/a.txt");
    let scan_b = format!("{scan_dir}/b.txt");
    let scan_c = format!("{scan_dir}/c.txt");
    let spec = match kind {
        NamespaceCollisionKind::RenameVsRename => vec![
            (
                "client-a",
                "rename",
                format!("{shared_file} -> {renamed_a}"),
                SharedOperation::Rename {
                    from: shared_file.clone(),
                    to: renamed_a,
                },
            ),
            (
                "client-b",
                "rename",
                format!("{shared_file} -> {renamed_b}"),
                SharedOperation::Rename {
                    from: shared_file.clone(),
                    to: renamed_b,
                },
            ),
        ],
        NamespaceCollisionKind::RenameVsUnlink => vec![
            (
                "client-a",
                "rename",
                format!("{shared_file} -> {renamed}"),
                SharedOperation::Rename {
                    from: shared_file.clone(),
                    to: renamed,
                },
            ),
            (
                "client-b",
                "unlink",
                shared_file.clone(),
                SharedOperation::Unlink {
                    path: shared_file.clone(),
                },
            ),
        ],
        NamespaceCollisionKind::UnlinkVsAppend => vec![
            (
                "client-a",
                "unlink",
                shared_data.clone(),
                SharedOperation::Unlink {
                    path: shared_data.clone(),
                },
            ),
            (
                "client-b",
                "append",
                shared_data.clone(),
                SharedOperation::Append {
                    path: shared_data.clone(),
                    bytes: u64::try_from(collision_payload(context, "append").len())
                        .unwrap_or(u64::MAX),
                    checksum_sha256: Some(bytes_sha256(&collision_payload(context, "append"))),
                },
            ),
        ],
        NamespaceCollisionKind::OverwriteVsRead => vec![
            (
                "client-b",
                "read_loop",
                shared_data.clone(),
                SharedOperation::Read {
                    path: shared_data.clone(),
                    bytes: None,
                    checksum_sha256: None,
                },
            ),
            (
                "client-a",
                "overwrite",
                shared_data.clone(),
                SharedOperation::Write {
                    path: shared_data.clone(),
                    offset: Some(0),
                    bytes: u64::try_from(collision_payload(context, "new-committed").len())
                        .unwrap_or(u64::MAX),
                    checksum_sha256: Some(bytes_sha256(&collision_payload(
                        context,
                        "new-committed",
                    ))),
                },
            ),
            (
                "client-c",
                "read_loop",
                shared_data.clone(),
                SharedOperation::Read {
                    path: shared_data.clone(),
                    bytes: None,
                    checksum_sha256: None,
                },
            ),
        ],
        NamespaceCollisionKind::MkdirRmdirVsCreate => vec![
            (
                "client-a",
                "mkdir",
                race_subdir.clone(),
                SharedOperation::CreateDirectory { path: race_subdir },
            ),
            (
                "client-b",
                "create",
                race_child.clone(),
                SharedOperation::CreateFile {
                    path: race_child,
                    bytes: Some(
                        u64::try_from(collision_payload(context, "child").len())
                            .unwrap_or(u64::MAX),
                    ),
                },
            ),
            (
                "client-c",
                "rmdir",
                race_dir.clone(),
                SharedOperation::Rmdir { path: race_dir },
            ),
        ],
        NamespaceCollisionKind::ReaddirStatDuringMutation => vec![
            (
                "client-b",
                "readdir",
                scan_dir.clone(),
                SharedOperation::Readdir { path: scan_dir },
            ),
            (
                "client-a",
                "rename",
                format!("{scan_a} -> {scan_c}"),
                SharedOperation::Rename {
                    from: scan_a,
                    to: scan_c,
                },
            ),
            (
                "client-c",
                "stat",
                scan_b.clone(),
                SharedOperation::Stat { path: scan_b },
            ),
        ],
    };
    spec.into_iter()
        .enumerate()
        .map(
            |(index, (client, operation, path, shared_operation))| NamespaceCollisionOperation {
                order: index + 1,
                client: client.to_string(),
                operation: operation.to_string(),
                path: path.to_string(),
                shared_operation,
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
    shared_operation: SharedOperation,
    f: impl FnOnce() -> std::io::Result<()>,
) -> NamespaceCollisionOperation {
    match f() {
        Ok(()) => NamespaceCollisionOperation {
            order,
            client: client.name.clone(),
            operation: operation.to_string(),
            path: path.to_string(),
            shared_operation,
            result: "accepted".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: None,
        },
        Err(error) => NamespaceCollisionOperation {
            order,
            client: client.name.clone(),
            operation: operation.to_string(),
            path: path.to_string(),
            shared_operation,
            result: "rejected".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: Some(error.to_string()),
        },
    }
}

fn elapsed_millis_u64(started: Instant) -> u64 {
    u64::try_from(elapsed_millis_nonzero(started)).unwrap_or(u64::MAX)
}

fn elapsed_millis_nonzero(started: Instant) -> u128 {
    let elapsed = started.elapsed();
    let elapsed_ms = elapsed.as_millis();
    if elapsed_ms == 0 && !elapsed.is_zero() {
        1
    } else {
        elapsed_ms
    }
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
    let mounts = client_mounts(context, &clients);
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    let mut namespace_violations = Vec::new();
    let expected_absent_paths = kind.expected_absent_paths(context);
    let expected_absent_path_refs = expected_absent_paths
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(
            snapshot,
            &expected_absent_path_refs,
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
    kind: NamespaceCollisionKind,
    operations: &[NamespaceCollisionOperation],
) -> Result<(), ScenarioError> {
    let path = operation_log_path(context);
    let mut writer = OperationLogWriter::open_append(&path)?;
    for operation in operations {
        let record = namespace_collision_shared_operation(&writer, kind, operation);
        append_shared_record(&mut writer, &record)?;
    }
    Ok(())
}

fn namespace_collision_shared_operation(
    writer: &OperationLogWriter,
    kind: NamespaceCollisionKind,
    operation: &NamespaceCollisionOperation,
) -> SharedOperationRecord {
    let mut record =
        shared_operation_record(writer, kind.name(), operation.shared_operation.clone())
            .with_client(&operation.client)
            .with_timestamp_ms(operation.timestamp_ms)
            .with_outcome(namespace_collision_outcome(operation));
    if operation.error.is_some() {
        record = record.with_metadata("result", &operation.result);
    }
    record
}

fn namespace_collision_outcome(operation: &NamespaceCollisionOperation) -> OperationOutcome {
    match operation.result.as_str() {
        "planned" => OperationOutcome::Planned,
        "accepted" => OperationOutcome::Accepted,
        "rejected" => OperationOutcome::Rejected {
            error: operation
                .error
                .clone()
                .unwrap_or_else(|| "operation rejected".to_string()),
        },
        _ => OperationOutcome::Observed,
    }
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
    drop(write_local_durable_payload_and_keep_handle(path, payload)?);
    Ok(())
}

fn write_local_durable_payload_and_keep_handle(
    path: PathBuf,
    payload: &[u8],
) -> Result<File, ScenarioError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = mounted_write_barrier_open_options();
    let mut file = options.create(true).truncate(true).open(&path)?;
    file.write_all(payload)?;
    file.sync_all()?;
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(std::io::Error::other(format!(
            "local durable write readback mismatch for {}: expected {} bytes, read {} bytes{}",
            path.display(),
            payload.len(),
            readback.len(),
            if readback.is_empty() {
                "; no payload became readable before timeout"
            } else {
                ""
            }
        ))));
    }
    Ok(file)
}

fn crash_restart_mode_slug(mode: CrashRestartMode) -> &'static str {
    match mode {
        CrashRestartMode::AfterLocalDurabilityBoundary => "after-local-durability-boundary",
        CrashRestartMode::BeforeRemotePublicationCompletes => "before-remote-publication-completes",
    }
}

fn crash_restart_mode_log_name(mode: CrashRestartMode) -> &'static str {
    match mode {
        CrashRestartMode::AfterLocalDurabilityBoundary => "after_local_durability_boundary",
        CrashRestartMode::BeforeRemotePublicationCompletes => "before_remote_publication_completes",
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
            requires_mount: false,
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
            requires_mount: false,
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
#[derive(Debug, Clone, Copy)]
struct TestHangingRunScenario;

#[cfg(test)]
impl Scenario for TestHangingRunScenario {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: TEST_HANGING_RUN,
            description: "test-only hanging run scenario",
            requires_studiofs_bin: false,
            requires_mount: false,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        thread::sleep(Duration::from_millis(500));
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
    use crate::client::{
        ClientCommandTimeouts, ClientLifecycleState, ClientSpec, S3Config, client_spec_for,
    };
    use crate::infra::CommandOutput;
    use studiofs_scenario::read_operation_log;

    fn test_context(root: &Path, run_id: &str, scenario: &str, seed: u64) -> ScenarioContext {
        let sandbox = root.join(run_id);
        ScenarioContext {
            run_id: run_id.to_string(),
            seed,
            scenario_dir: sandbox.join("artifacts").join("scenarios").join(scenario),
            sandbox,
            client_controller_config: None,
        }
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

        let relative_path = run_unique_path(&context, "smoke", SMOKE_FILE_NAME);
        for client in driver.clients {
            let payload = std::fs::read(client.mount_point.join(&relative_path)).unwrap();
            assert_eq!(
                payload,
                run_unique_payload(&context, BASIC_MULTI_CLIENT_SMOKE, "smoke")
            );
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

        fn after_smoke_write(
            &mut self,
            clients: &[ClientSpec],
            relative_path: &str,
        ) -> Result<(), ClientControllerError> {
            let Some(source_client) = clients.first() else {
                return Ok(());
            };
            let payload = std::fs::read(source_client.mount_point.join(relative_path))?;
            for replica in clients.iter().skip(1) {
                let target = replica.mount_point.join(relative_path);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(target, &payload)?;
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
    }

    impl RecordingOutageDriver {
        fn new(clients: Vec<ClientSpec>) -> Self {
            Self {
                clients,
                actions: RefCell::new(Vec::new()),
            }
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
            let payload = std::fs::read(clients[0].mount_point.join(relative_path))?;
            for replica in clients.iter().skip(1) {
                let target = replica.mount_point.join(relative_path);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(target, &payload)?;
            }
            Ok(())
        }

        fn stop_postgres(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("stop_postgres");
            Ok(())
        }

        fn start_postgres(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("start_postgres");
            let mut entries = Vec::new();
            for client in &self.clients {
                collect_test_files(&client.mount_point, &client.mount_point, &mut entries);
            }
            for client in &self.clients {
                for (relative_path, payload) in &entries {
                    let target = client.mount_point.join(relative_path);
                    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
                    std::fs::write(target, payload).unwrap();
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
        outage_path: RefCell<Option<String>>,
    }

    impl RecordingRustfsOutageDriver {
        fn new(clients: Vec<ClientSpec>) -> Self {
            Self {
                clients,
                actions: RefCell::new(Vec::new()),
                peer_probe_delay: Duration::from_millis(0),
                outage_path: RefCell::new(None),
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
                let target = client.mount_point.join(relative_path);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(target, &payload)?;
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
            self.outage_path
                .borrow_mut()
                .replace(relative_path.to_string());
            if !self.peer_probe_delay.is_zero() {
                thread::sleep(self.peer_probe_delay);
            }
            assert_peer_not_visible(peer, relative_path, forbidden_checksum)
        }

        fn start_rustfs(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("start_rustfs");
            let outage_path = self
                .outage_path
                .borrow()
                .clone()
                .unwrap_or_else(|| RUSTFS_OUTAGE_FILE_NAME.to_string());
            let source = self.clients[0].mount_point.join(&outage_path);
            let payload = std::fs::read(source)?;
            for client in self.clients.iter().skip(1) {
                let target = client.mount_point.join(&outage_path);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(target, &payload)?;
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
        publish_current_file_on_remount: bool,
        publish_current_file_before_convergence: bool,
        #[cfg(windows)]
        require_open_payload_handle_during_kill: Option<String>,
    }

    impl RecordingCrashRestartDriver {
        fn new(clients: Vec<ClientSpec>) -> Self {
            Self {
                clients,
                actions: RefCell::new(Vec::new()),
                corrupt_local_on_remount: false,
                corrupt_converged_payload_after_remount: false,
                publish_current_file_on_remount: false,
                publish_current_file_before_convergence: false,
                #[cfg(windows)]
                require_open_payload_handle_during_kill: None,
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

        fn publish_current_file_on_remount(mut self) -> Self {
            self.publish_current_file_on_remount = true;
            self
        }

        fn publish_current_file_before_convergence(mut self) -> Self {
            self.publish_current_file_before_convergence = true;
            self
        }

        #[cfg(windows)]
        fn require_open_payload_handle_during_kill(mut self, relative_path: &str) -> Self {
            self.require_open_payload_handle_during_kill = Some(relative_path.to_string());
            self
        }

        #[cfg(windows)]
        fn assert_payload_handle_is_open_during_kill(&self) -> Result<(), ScenarioError> {
            let Some(relative_path) = self.require_open_payload_handle_during_kill.as_deref()
            else {
                return Ok(());
            };
            let path = self.clients[0].mount_point.join(relative_path);
            use std::os::windows::fs::OpenOptionsExt as _;
            match OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(0)
                .open(&path)
            {
                Ok(_) => Err(ScenarioError::Io(std::io::Error::other(format!(
                    "crash-restart test handle for {} was already closed before force_kill",
                    path.display()
                )))),
                Err(error)
                    if error.kind() == std::io::ErrorKind::PermissionDenied
                        || error.raw_os_error() == Some(32) =>
                {
                    Ok(())
                }
                Err(error) => Err(ScenarioError::Io(std::io::Error::other(format!(
                    "crash-restart handle check for {} failed with unexpected error: {error}",
                    path.display()
                )))),
            }
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
            #[cfg(windows)]
            self.assert_payload_handle_is_open_during_kill()?;
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
                    find_named_file(&client.mount_point, CRASH_RESTART_LOCAL_DURABLE_FILE_NAME)
                        .unwrap_or_else(|| {
                            client
                                .mount_point
                                .join(CRASH_RESTART_LOCAL_DURABLE_FILE_NAME)
                        }),
                    b"corrupted payload after restart\n",
                )?;
            }
            if self.corrupt_converged_payload_after_remount {
                self.actions
                    .borrow_mut()
                    .push("defer_corrupt_converged_payload".to_string());
            }
            if self.publish_current_file_on_remount {
                let crashed = self
                    .clients
                    .iter()
                    .find(|candidate| candidate.name == client)
                    .expect("recording driver client exists");
                for peer in self.clients.iter().filter(|peer| peer.name != client) {
                    copy_test_tree(&crashed.mount_point, &peer.mount_point);
                }
            }
            Ok(())
        }

        fn before_convergence_validation(
            &mut self,
            _mode: CrashRestartMode,
            relative_path: &str,
            clients: &[ClientSpec],
        ) -> Result<(), ScenarioError> {
            if self.corrupt_converged_payload_after_remount {
                for client in clients {
                    let target = client.mount_point.join(relative_path);
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(target, b"wrong converged payload after restart\n")?;
                }
            }
            if self.publish_current_file_before_convergence {
                let Some(source_client) = clients.first() else {
                    return Ok(());
                };
                let payload = std::fs::read(source_client.mount_point.join(relative_path))?;
                for peer in clients.iter().skip(1) {
                    let target = peer.mount_point.join(relative_path);
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(target, &payload)?;
                }
            }
            Ok(())
        }

        fn unmount_all(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("unmount_all".to_string());
            Ok(())
        }
    }

    struct RecordingPublicationBoundaryDriver {
        clients: Vec<ClientSpec>,
        actions: RefCell<Vec<String>>,
        leak_premature_peer_file: bool,
        refresh_failure_client: Option<String>,
        #[cfg(windows)]
        require_open_boundary_handle_during_kill: Option<PublicationBoundary>,
    }

    impl RecordingPublicationBoundaryDriver {
        fn new(clients: Vec<ClientSpec>) -> Self {
            Self {
                clients,
                actions: RefCell::new(Vec::new()),
                leak_premature_peer_file: false,
                refresh_failure_client: None,
                #[cfg(windows)]
                require_open_boundary_handle_during_kill: None,
            }
        }

        fn leak_premature_peer_file(mut self) -> Self {
            self.leak_premature_peer_file = true;
            self
        }

        fn fail_refresh_for(mut self, client: &str) -> Self {
            self.refresh_failure_client = Some(client.to_string());
            self
        }

        #[cfg(windows)]
        fn require_open_boundary_handle_during_kill(
            mut self,
            boundary: PublicationBoundary,
        ) -> Self {
            self.require_open_boundary_handle_during_kill = Some(boundary);
            self
        }

        fn replicate_from_writer(&self) -> Result<(), ScenarioError> {
            let source = &self.clients[0].mount_point;
            for client in self.clients.iter().skip(1) {
                copy_dir_contents(source, &client.mount_point)?;
            }
            Ok(())
        }
    }

    impl PublicationBoundaryDriver for RecordingPublicationBoundaryDriver {
        fn bootstrap_volume(
            &mut self,
            options: VolumeBootstrapOptions,
        ) -> Result<VolumeBootstrapSummary, ScenarioError> {
            self.actions.borrow_mut().push("bootstrap".to_string());
            assert_eq!(
                options.client_names,
                PUBLICATION_BOUNDARY_CLIENT_NAMES
                    .iter()
                    .copied()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            );
            Ok(VolumeBootstrapSummary {
                volume_name: "projectfs".to_string(),
                init_client: PUBLICATION_BOUNDARY_CLIENT_NAMES[0].to_string(),
                discovered_clients: PUBLICATION_BOUNDARY_CLIENT_NAMES[1..]
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
            #[cfg(windows)]
            self.assert_boundary_handle_is_open_during_kill()?;
            self.maybe_leak_premature_peer_file()?;
            Ok(())
        }

        fn graceful_shutdown(&mut self, client: &str) -> Result<(), ScenarioError> {
            self.actions
                .borrow_mut()
                .push(format!("graceful_shutdown_{client}"));
            self.maybe_leak_premature_peer_file()?;
            Ok(())
        }

        fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push(format!("remount_{client}"));
            Ok(())
        }

        fn publish_for_convergence(
            &mut self,
            boundary: PublicationBoundary,
            clients: &[ClientSpec],
            _relative_path: &str,
        ) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push(format!(
                "publish_{}",
                publication_boundary_log_name(boundary)
            ));
            let mut errors = Vec::new();
            for peer in clients.iter().skip(1) {
                self.actions
                    .borrow_mut()
                    .push(format!("refresh_{}_for_convergence", peer.name));
                if self.refresh_failure_client.as_deref() == Some(peer.name.as_str()) {
                    errors.push(format!("refresh failed for {}", peer.name));
                }
            }
            if !errors.is_empty() {
                return Err(ScenarioError::PublicationBoundaryPeerRefreshFailed {
                    errors: errors.join("; "),
                });
            }
            self.replicate_from_writer()
        }

        fn unmount_all(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("unmount_all".to_string());
            Ok(())
        }
    }

    impl RecordingPublicationBoundaryDriver {
        #[cfg(windows)]
        fn assert_boundary_handle_is_open_during_kill(&self) -> Result<(), ScenarioError> {
            let Some(boundary) = self.require_open_boundary_handle_during_kill else {
                return Ok(());
            };
            let path = self.clients[0]
                .mount_point
                .join(publication_boundary_leaf(boundary));
            let path = find_named_file(
                &self.clients[0].mount_point,
                Path::new(publication_boundary_leaf(boundary))
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(""),
            )
            .unwrap_or(path);
            use std::os::windows::fs::OpenOptionsExt as _;
            match OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(0)
                .open(&path)
            {
                Ok(_) => Err(ScenarioError::Io(std::io::Error::other(format!(
                    "publication boundary test handle for {} was already closed before force_kill",
                    path.display()
                )))),
                Err(error)
                    if error.kind() == std::io::ErrorKind::PermissionDenied
                        || error.raw_os_error() == Some(32) =>
                {
                    Ok(())
                }
                Err(error) => Err(ScenarioError::Io(std::io::Error::other(format!(
                    "publication boundary handle check for {} failed with unexpected error: {error}",
                    path.display()
                )))),
            }
        }

        fn maybe_leak_premature_peer_file(&self) -> Result<(), ScenarioError> {
            if !self.leak_premature_peer_file {
                return Ok(());
            }
            let source_path = find_named_file(&self.clients[0].mount_point, "flush.txt");
            let relative_path = source_path
                .as_ref()
                .and_then(|path| path.strip_prefix(&self.clients[0].mount_point).ok())
                .map(Path::to_path_buf)
                .unwrap_or_else(|| {
                    PathBuf::from(publication_boundary_leaf(PublicationBoundary::Flush))
                });
            for peer in self.clients.iter().skip(1) {
                let path = peer.mount_point.join(&relative_path);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(path, b"partial premature payload\n")?;
            }
            Ok(())
        }
    }

    struct RecordingMountedFaultChaosDriver {
        clients: Vec<ClientSpec>,
        actions: RefCell<Vec<&'static str>>,
    }

    impl RecordingMountedFaultChaosDriver {
        fn new(clients: Vec<ClientSpec>) -> Self {
            Self {
                clients,
                actions: RefCell::new(Vec::new()),
            }
        }
    }

    impl MountedFaultChaosDriver for RecordingMountedFaultChaosDriver {
        fn bootstrap_volume(
            &mut self,
            options: VolumeBootstrapOptions,
        ) -> Result<VolumeBootstrapSummary, ScenarioError> {
            self.actions.borrow_mut().push("bootstrap");
            assert_eq!(
                options.client_names,
                FAULT_CHAOS_CLIENT_NAMES
                    .iter()
                    .copied()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            );
            Ok(VolumeBootstrapSummary {
                volume_name: "projectfs".to_string(),
                init_client: FAULT_CHAOS_CLIENT_NAMES[0].to_string(),
                discovered_clients: FAULT_CHAOS_CLIENT_NAMES[1..]
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

        fn mount_client(&mut self, _client: &str) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("mount_client");
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
            Ok(())
        }

        fn stop_rustfs(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("stop_rustfs");
            Ok(())
        }

        fn start_rustfs(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("start_rustfs");
            Ok(())
        }

        fn force_kill(&mut self, _client: &str) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("force_kill");
            Ok(())
        }

        fn remount(&mut self, _client: &str) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("remount");
            Ok(())
        }

        fn unmount_all(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("unmount_all");
            Ok(())
        }
    }

    fn fault_chaos_clients(
        sandbox: &Path,
        temp_root: &Path,
        bad_postgres_write_index: Option<usize>,
        bad_rustfs_write_index: Option<usize>,
    ) -> Vec<ClientSpec> {
        FAULT_CHAOS_CLIENT_NAMES
            .iter()
            .take(FAULT_CHAOS_WARM_CLIENTS)
            .enumerate()
            .map(|(index, name)| {
                let mount = temp_root.join("mounts").join(name);
                if Some(index) == bad_postgres_write_index || Some(index) == bad_rustfs_write_index
                {
                    std::fs::create_dir_all(mount.parent().unwrap()).unwrap();
                    std::fs::write(&mount, b"not a directory").unwrap();
                } else {
                    std::fs::create_dir_all(mount.join("fault-chaos")).unwrap();
                }
                client_spec_for(sandbox, "projectfs", name, mount)
            })
            .collect()
    }

    #[test]
    fn fault_chaos_initial_tree_rejects_stale_zero_byte_warmup_file() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-fault-chaos");
        let context = ScenarioContext {
            run_id: "run-fault-chaos".to_string(),
            seed: 376,
            sandbox: sandbox.clone(),
            scenario_dir: sandbox
                .join("artifacts")
                .join("scenarios")
                .join(MOUNTED_FAULT_CHAOS_CONVERGENCE),
            client_controller_config: None,
        };
        let mount = temp.path().join("mounts").join("client-a");
        let stale_file = mount
            .join(run_unique_dir(&context, "fault-chaos"))
            .join("postgres-outage-local.txt");
        std::fs::create_dir_all(stale_file.parent().unwrap()).unwrap();
        std::fs::write(&stale_file, []).unwrap();
        let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);

        let err = establish_fault_chaos_initial_tree(&context, &client).unwrap_err();

        assert!(
            matches!(&err, ScenarioError::Io(error) if error.kind() == io::ErrorKind::AlreadyExists),
            "expected stale warmup file to be rejected before convergence validation, got {err:?}"
        );
    }

    #[test]
    fn namespace_collision_initial_tree_rejects_stale_initial_file() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-collision");
        let context = ScenarioContext {
            run_id: "run-collision".to_string(),
            seed: 328,
            sandbox: sandbox.clone(),
            scenario_dir: sandbox
                .join("artifacts")
                .join("scenarios")
                .join(COLLISION_UNLINK_VS_APPEND),
            client_controller_config: None,
        };
        let mount = temp.path().join("mounts").join("client-a");
        let stale_file = mount
            .join(namespace_collision_root(&context))
            .join("shared")
            .join("data.txt");
        std::fs::create_dir_all(stale_file.parent().unwrap()).unwrap();
        std::fs::write(&stale_file, []).unwrap();
        let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);

        let err = establish_collision_initial_tree(
            &context,
            NamespaceCollisionKind::UnlinkVsAppend,
            &client,
        )
        .unwrap_err();

        assert!(
            matches!(&err, ScenarioError::Io(error) if error.kind() == io::ErrorKind::AlreadyExists),
            "expected stale collision initial file to be rejected before convergence validation, got {err:?}"
        );
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

    struct RecordingMountedChaosDriver {
        clients: Vec<ClientSpec>,
        actions: RefCell<Vec<String>>,
        unmount_error: Option<&'static str>,
    }

    impl RecordingMountedChaosDriver {
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
            for client in self.clients.iter().skip(1) {
                replace_dir_contents(source, &client.mount_point)?;
            }
            Ok(())
        }
    }

    impl MountedChaosDriver for RecordingMountedChaosDriver {
        fn bootstrap_volume(
            &mut self,
            options: VolumeBootstrapOptions,
        ) -> Result<VolumeBootstrapSummary, ScenarioError> {
            self.actions.borrow_mut().push("bootstrap".to_string());
            assert_eq!(
                options.client_names,
                CHAOS_CLIENT_NAMES
                    .iter()
                    .copied()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            );
            Ok(VolumeBootstrapSummary {
                volume_name: "projectfs".to_string(),
                init_client: CHAOS_CLIENT_NAMES[0].to_string(),
                discovered_clients: CHAOS_CLIENT_NAMES[1..]
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

        fn after_initial_tree(&mut self, _clients: &[ClientSpec]) -> Result<(), ScenarioError> {
            self.actions
                .borrow_mut()
                .push("replicate_initial".to_string());
            self.replicate_from_first_client()
        }

        fn after_round(
            &mut self,
            round: usize,
            _clients: &[ClientSpec],
        ) -> Result<(), ScenarioError> {
            self.actions
                .borrow_mut()
                .push(format!("replicate_round_{round}"));
            self.replicate_from_first_client()
        }

        fn unmount_all(&mut self) -> Result<(), ScenarioError> {
            self.actions.borrow_mut().push("unmount_all".to_string());
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
        let operations =
            read_operation_log(context.scenario_dir.join("operations.ndjson")).unwrap();
        let root = namespace_collision_root(&context);
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::Rename { from, to }
                if from == &format!("{root}/shared/file.txt")
                    && to == &format!("{root}/shared/renamed.txt")
        )));
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::Unlink { path } if path == &format!("{root}/shared/file.txt")
        )));
        assert!(
            context
                .scenario_dir
                .join("namespace-collision-summary.json")
                .exists()
        );
    }

    #[test]
    fn mounted_chaos_records_seeded_operations_and_validates_convergence() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-chaos");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_CHAOS_CONVERGENCE);
        let clients = CHAOS_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| {
                let mount = temp.path().join("mounts").join(name);
                std::fs::create_dir_all(&mount).unwrap();
                client_spec_for(&sandbox, "projectfs", name, mount)
            })
            .collect::<Vec<_>>();
        let mut driver = RecordingMountedChaosDriver::new(clients);
        let context = ScenarioContext {
            run_id: "run-chaos".to_string(),
            seed: 360,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        let summary = run_mounted_chaos_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap();

        assert_eq!(summary.scenario, MOUNTED_CHAOS_CONVERGENCE);
        assert_eq!(summary.seed, 360);
        assert_eq!(summary.rounds, CHAOS_ROUNDS);
        assert_eq!(
            summary.operations.len(),
            CHAOS_ROUNDS * CHAOS_CLIENT_NAMES.len()
        );
        assert!(summary.accepted_operations > 0);
        assert!(summary.final_validator.converged);
        assert!(summary.namespace_violations.is_empty());
        assert_eq!(
            driver.actions.borrow().first().map(String::as_str),
            Some("bootstrap")
        );
        assert!(
            context
                .scenario_dir
                .join("mounted-chaos-summary.json")
                .exists()
        );

        let operations =
            read_operation_log(context.scenario_dir.join("operations.ndjson")).unwrap();
        assert_eq!(
            operations
                .iter()
                .filter(|record| matches!(record.outcome, OperationOutcome::Accepted))
                .count(),
            CHAOS_ROUNDS * CHAOS_CLIENT_NAMES.len()
        );
        assert!(
            operations
                .iter()
                .any(|record| matches!(&record.operation, SharedOperation::Rename { .. }))
        );
        assert!(operations.iter().any(|record| {
            record
                .metadata
                .get("round")
                .is_some_and(|round| round == "0")
        }));
    }

    #[test]
    fn mounted_chaos_initial_files_are_written_through_durable_boundary() {
        let temp = tempdir().unwrap();
        let file_path = temp.path().join("chaos").join("data-01.txt");
        let context = test_context(
            temp.path(),
            "run-chaos-payload",
            MOUNTED_CHAOS_CONVERGENCE,
            376,
        );
        let payload = chaos_payload(&context, 1, "data-initial");

        write_mounted_chaos_initial_file(file_path.clone(), &payload).unwrap();

        assert_eq!(std::fs::read(file_path).unwrap(), payload);
    }

    #[test]
    fn mounted_chaos_initial_readback_waits_for_delayed_visibility() {
        let temp = tempdir().unwrap();
        let file_path = temp.path().join("chaos").join("delayed.txt");
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::write(&file_path, b"").unwrap();
        let context = test_context(
            temp.path(),
            "run-chaos-readback",
            MOUNTED_CHAOS_CONVERGENCE,
            376,
        );
        let payload = chaos_payload(&context, 7, "delayed-readback");
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
    fn mounted_chaos_records_planned_and_started_operations_before_round_finishes() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-chaos-started");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_CHAOS_CONVERGENCE);
        let clients = CHAOS_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| {
                let mount = temp.path().join("mounts").join(name);
                std::fs::create_dir_all(&mount).unwrap();
                client_spec_for(&sandbox, "projectfs", name, mount)
            })
            .collect::<Vec<_>>();
        let mut driver = RecordingMountedChaosDriver::new(clients);
        let context = ScenarioContext {
            run_id: "run-chaos-started".to_string(),
            seed: 360,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        run_mounted_chaos_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap();

        let operations =
            read_operation_log(context.scenario_dir.join("operations.ndjson")).unwrap();
        let round_zero = operations
            .iter()
            .filter(|record| {
                record
                    .metadata
                    .get("round")
                    .is_some_and(|round| round == "0")
            })
            .collect::<Vec<_>>();
        assert!(
            round_zero
                .iter()
                .any(|record| matches!(record.outcome, OperationOutcome::Planned)),
            "round must be recorded as planned before any mounted worker can hang"
        );
        assert!(
            round_zero.iter().any(|record| {
                matches!(record.outcome, OperationOutcome::Observed)
                    && record
                        .metadata
                        .get("result")
                        .is_some_and(|result| result == "started")
            }),
            "round must be recorded as started before waiting for mounted workers"
        );
        assert!(
            round_zero
                .iter()
                .any(|record| matches!(record.outcome, OperationOutcome::Accepted)),
            "successful round must still record accepted results"
        );
    }

    #[test]
    fn mounted_chaos_round_times_out_instead_of_joining_forever() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-chaos-timeout");
        let mount = temp.path().join("mounts").join("client-a");
        std::fs::create_dir_all(&mount).unwrap();
        let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
        let task = MountedChaosTask::new(
            7,
            1,
            client,
            "mounted-only-hang",
            "chaos/hang.txt",
            SharedOperation::MountedOnly {
                name: "mounted-only-hang".to_string(),
                reason: "test hang".to_string(),
            },
            MountedChaosAction::Sleep { millis: 250 },
        );
        let launcher = RecordingMountedChaosProcessLauncher::pending();
        let started = Instant::now();

        let err = run_concurrent_mounted_chaos_operations_with_launcher(
            started,
            vec![task],
            Duration::from_millis(20),
            &launcher,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("mounted chaos round 7 timed out"));
        assert!(message.contains("client-a mounted-only-hang chaos/hang.txt"));
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "timeout must not wait for the hung worker to finish"
        );
        assert!(
            launcher.work_dirs().iter().all(|path| !path.exists()),
            "timeout cleanup must remove the helper work directory"
        );
    }

    struct RecordingMountedChaosProcessLauncher {
        killed: std::rc::Rc<RefCell<Vec<String>>>,
        work_dirs: std::rc::Rc<RefCell<Vec<PathBuf>>>,
        fail_launch_orders: Vec<usize>,
        fail_collect_orders: Vec<usize>,
    }

    impl RecordingMountedChaosProcessLauncher {
        fn pending() -> Self {
            Self {
                killed: std::rc::Rc::new(RefCell::new(Vec::new())),
                work_dirs: std::rc::Rc::new(RefCell::new(Vec::new())),
                fail_launch_orders: Vec::new(),
                fail_collect_orders: Vec::new(),
            }
        }

        fn failing_launches(fail_launch_orders: Vec<usize>) -> Self {
            Self {
                killed: std::rc::Rc::new(RefCell::new(Vec::new())),
                work_dirs: std::rc::Rc::new(RefCell::new(Vec::new())),
                fail_launch_orders,
                fail_collect_orders: Vec::new(),
            }
        }

        fn failing_collects(fail_collect_orders: Vec<usize>) -> Self {
            Self {
                killed: std::rc::Rc::new(RefCell::new(Vec::new())),
                work_dirs: std::rc::Rc::new(RefCell::new(Vec::new())),
                fail_launch_orders: Vec::new(),
                fail_collect_orders,
            }
        }

        fn killed_processes(&self) -> Vec<String> {
            self.killed.borrow().clone()
        }

        fn work_dirs(&self) -> Vec<PathBuf> {
            self.work_dirs.borrow().clone()
        }
    }

    impl MountedChaosProcessLauncher for RecordingMountedChaosProcessLauncher {
        fn launch(
            &self,
            task: &MountedChaosTask,
            _start_file: &Path,
            work_dir: &Path,
        ) -> Result<Box<dyn MountedChaosProcess>, ScenarioError> {
            self.work_dirs.borrow_mut().push(work_dir.to_path_buf());
            if self.fail_launch_orders.contains(&task.order) {
                return Err(ScenarioError::MountedChaosHelper(format!(
                    "launch failed for order {}",
                    task.order
                )));
            }
            Ok(Box::new(RecordingMountedChaosProcess {
                label: mounted_chaos_task_label(task),
                fail_collect: self.fail_collect_orders.contains(&task.order),
                killed: std::rc::Rc::clone(&self.killed),
            }))
        }
    }

    struct RecordingMountedChaosProcess {
        label: String,
        fail_collect: bool,
        killed: std::rc::Rc<RefCell<Vec<String>>>,
    }

    impl MountedChaosProcess for RecordingMountedChaosProcess {
        fn try_collect(
            &mut self,
            _started: Instant,
        ) -> Result<Option<MountedChaosOperation>, ScenarioError> {
            if self.fail_collect {
                return Err(ScenarioError::MountedChaosHelper(format!(
                    "{} collect failed",
                    self.label
                )));
            }
            Ok(None)
        }

        fn kill(&mut self) -> io::Result<()> {
            self.killed.borrow_mut().push(self.label.clone());
            Ok(())
        }
    }

    #[test]
    fn mounted_chaos_collect_failure_kills_pending_operation_processes() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-chaos-collect-failure");
        let mount = temp.path().join("mounts").join("client-a");
        std::fs::create_dir_all(&mount).unwrap();
        let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
        let tasks = (1..=3)
            .map(|order| {
                let path = format!("chaos/task-{order}.txt");
                MountedChaosTask::new(
                    12,
                    order,
                    client.clone(),
                    "stat",
                    path.clone(),
                    SharedOperation::Stat { path: path.clone() },
                    MountedChaosAction::Stat { path },
                )
            })
            .collect::<Vec<_>>();
        let launcher = RecordingMountedChaosProcessLauncher::failing_collects(vec![2]);

        let err = run_concurrent_mounted_chaos_operations_with_launcher(
            Instant::now(),
            tasks,
            Duration::from_millis(20),
            &launcher,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("client-a stat chaos/task-2.txt collect failed"));
        assert!(message.contains("pending helpers killed after collect failure"));
        assert_eq!(
            launcher.killed_processes(),
            vec![
                "client-a stat chaos/task-1.txt".to_string(),
                "client-a stat chaos/task-2.txt".to_string(),
                "client-a stat chaos/task-3.txt".to_string(),
            ]
        );
        assert!(
            launcher.work_dirs().iter().all(|path| !path.exists()),
            "collect failure cleanup must remove the helper work directory"
        );
    }

    #[test]
    fn mounted_chaos_timeout_kills_isolated_operation_process() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-chaos-isolation-timeout");
        let mount = temp.path().join("mounts").join("client-a");
        std::fs::create_dir_all(&mount).unwrap();
        let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
        let task = MountedChaosTask::new(
            9,
            1,
            client,
            "stat",
            "chaos/blocked.txt",
            SharedOperation::Stat {
                path: "chaos/blocked.txt".to_string(),
            },
            MountedChaosAction::Stat {
                path: "chaos/blocked.txt".to_string(),
            },
        );
        let launcher = RecordingMountedChaosProcessLauncher::pending();

        let err = run_concurrent_mounted_chaos_operations_with_launcher(
            Instant::now(),
            vec![task],
            Duration::from_millis(20),
            &launcher,
        )
        .unwrap_err();

        assert!(err.to_string().contains("mounted chaos round 9 timed out"));
        assert_eq!(
            launcher.killed_processes(),
            vec!["client-a stat chaos/blocked.txt".to_string()]
        );
        assert!(
            launcher.work_dirs().iter().all(|path| !path.exists()),
            "timeout cleanup must remove the helper work directory"
        );
    }

    #[test]
    fn mounted_chaos_launch_failures_collect_all_errors_and_cleanup_started_helpers() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-chaos-launch-failure");
        let mount = temp.path().join("mounts").join("client-a");
        std::fs::create_dir_all(&mount).unwrap();
        let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
        let tasks = (1..=3)
            .map(|order| {
                let path = format!("chaos/task-{order}.txt");
                MountedChaosTask::new(
                    11,
                    order,
                    client.clone(),
                    "stat",
                    path.clone(),
                    SharedOperation::Stat { path: path.clone() },
                    MountedChaosAction::Stat { path },
                )
            })
            .collect::<Vec<_>>();
        let launcher = RecordingMountedChaosProcessLauncher::failing_launches(vec![2, 3]);

        let err = run_concurrent_mounted_chaos_operations_with_launcher(
            Instant::now(),
            tasks,
            Duration::from_millis(20),
            &launcher,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("mounted chaos helper launch failures"));
        assert!(message.contains("client-a stat chaos/task-2.txt"));
        assert!(message.contains("client-a stat chaos/task-3.txt"));
        assert_eq!(
            launcher.killed_processes(),
            vec!["client-a stat chaos/task-1.txt".to_string()]
        );
        assert!(
            launcher.work_dirs().iter().all(|path| !path.exists()),
            "launch failure cleanup must remove the helper work directory"
        );
    }

    #[test]
    fn mounted_chaos_preserves_primary_error_when_unmount_also_fails() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-chaos");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_CHAOS_CONVERGENCE);
        let mut driver =
            RecordingMountedChaosDriver::new(Vec::new()).with_unmount_error("umount failed");
        let context = ScenarioContext {
            run_id: "run-chaos".to_string(),
            seed: 360,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        let err = run_mounted_chaos_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(matches!(
            err,
            ScenarioError::MountedChaosUnmountAfterFailure { .. }
        ));
        assert!(message.contains("requires at least 3 clients"));
        assert!(message.contains("umount failed"));
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

        let temp = tempdir().unwrap();
        let context = test_context(
            temp.path(),
            "run-collision-plan",
            COLLISION_RENAME_VS_UNLINK,
            328,
        );
        let operations = planned_namespace_collision_operations(
            &context,
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
    fn registry_exposes_mounted_publication_boundary_crash_matrix_scenario() {
        let scenarios = available_scenarios();

        assert!(scenarios.iter().any(|scenario| {
            scenario.name == "mounted-publication-boundary-crash-matrix"
                && scenario.requires_studiofs_bin
                && scenario.requires_mount
                && scenario.description.contains("publication boundary")
        }));
        assert!(resolve_scenario("mounted-publication-boundary-crash-matrix").is_some());
        assert!(resolve_scenario("publication-boundary-crash").is_some());
        assert!(resolve_scenario("publication-boundary").is_some());
    }

    #[test]
    fn registry_exposes_mounted_chaos_convergence_scenario() {
        let scenarios = available_scenarios();

        assert!(scenarios.iter().any(|scenario| {
            scenario.name == MOUNTED_CHAOS_CONVERGENCE
                && scenario.requires_studiofs_bin
                && scenario.description.contains("chaos")
        }));
        assert!(resolve_scenario(MOUNTED_CHAOS_CONVERGENCE).is_some());
        assert!(resolve_scenario("mounted-chaos").is_some());
        assert!(resolve_scenario("chaos").is_some());
    }

    #[test]
    fn registry_exposes_mounted_hotspot_churn_scenario() {
        let scenarios = available_scenarios();

        assert!(scenarios.iter().any(|scenario| {
            scenario.name == MOUNTED_HOTSPOT_CHURN_CONVERGENCE
                && scenario.requires_studiofs_bin
                && scenario.requires_mount
                && scenario.description.contains("hot-spot")
        }));
        assert!(resolve_scenario(MOUNTED_HOTSPOT_CHURN_CONVERGENCE).is_some());
        assert!(resolve_scenario("hotspot-churn").is_some());
        assert!(resolve_scenario("hot-dir-churn").is_some());
    }

    #[test]
    fn mounted_hotspot_churn_plan_records_bounded_mutations_and_observer_probes() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-hotspot-plan");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
        let context = ScenarioContext {
            run_id: "run-hotspot-plan".to_string(),
            seed: 376,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        write_mounted_hotspot_churn_plan_artifacts(&context).unwrap();
        MountedHotspotChurnConvergence
            .validate(&context)
            .expect("plan artifacts should validate without a real mount");

        let operations =
            read_operation_log(context.scenario_dir.join("operations.ndjson")).unwrap();
        let hotspot_root = run_unique_dir(&context, "hotspot");
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::CreateFile { path, .. } if path.starts_with("hotspot/")
        )));
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::Unlink { path } if path.starts_with("hotspot/")
        )));
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::Rename { from, to }
                if from.starts_with("hotspot/") && to.starts_with("hotspot/")
        )));
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::Write { path, .. } if path.starts_with("hotspot/")
        )));
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::Readdir { path } if path == &hotspot_root
        )));
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::Stat { path } if path.starts_with("hotspot/")
        )));
        assert!(operations.iter().any(|record| matches!(
            &record.operation,
            SharedOperation::Read { path, .. } if path.starts_with("hotspot/")
        )));
        assert!(operations.iter().any(|record| {
            record
                .metadata
                .get("role")
                .is_some_and(|role| role == "observer")
        }));
        assert!(
            operations.iter().all(|record| match &record.operation {
                SharedOperation::CreateFile { path, .. }
                | SharedOperation::Unlink { path }
                | SharedOperation::Write { path, .. }
                | SharedOperation::Read { path, .. }
                | SharedOperation::Stat { path }
                | SharedOperation::Readdir { path } =>
                    path == &hotspot_root
                        || path
                            .strip_prefix(&format!("{hotspot_root}/"))
                            .and_then(|rest| rest.split('/').next())
                            .is_some_and(|name| name.starts_with("slot-")),
                SharedOperation::Rename { from, to } => [from, to].iter().all(|path| {
                    path.strip_prefix(&format!("{hotspot_root}/"))
                        .and_then(|rest| rest.split('/').next())
                        .is_some_and(|name| name.starts_with("slot-"))
                }),
                _ => true,
            }),
            "hotspot churn must stay inside the bounded working set"
        );
        assert!(
            context
                .scenario_dir
                .join("mounted-hotspot-churn-summary.json")
                .exists()
        );
        assert!(context.scenario_dir.join("validation.json").exists());
    }

    #[test]
    fn hotspot_probe_rejects_empty_listing_and_unexpected_payload() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-hotspot-probe");
        let mount = temp.path().join("mounts").join("client-c");
        let hotspot = mount.join("hotspot");
        std::fs::create_dir_all(&hotspot).unwrap();
        let client = client_spec_for(&sandbox, "projectfs", "client-c", mount.clone());

        let empty_err = repeated_hotspot_probe(
            &client,
            "hotspot",
            "hotspot/slot-00.txt",
            "hotspot/slot-00.txt",
        )
        .unwrap_err();
        assert!(empty_err.to_string().contains("empty directory listing"));

        std::fs::write(hotspot.join("slot-00.txt"), b"corrupt payload").unwrap();
        let payload_err = repeated_hotspot_probe(
            &client,
            "hotspot",
            "hotspot/slot-00.txt",
            "hotspot/slot-00.txt",
        )
        .unwrap_err();
        assert!(payload_err.to_string().contains("unexpected payload"));

        let context = test_context(
            temp.path(),
            "run-hotspot-probe",
            MOUNTED_HOTSPOT_CHURN_CONVERGENCE,
            376,
        );
        std::fs::write(
            hotspot.join("slot-00.txt"),
            hotspot_payload(&context, 0, 0, "initial"),
        )
        .unwrap();
        repeated_hotspot_probe(
            &client,
            "hotspot",
            "hotspot/slot-00.txt",
            "hotspot/slot-00.txt",
        )
        .unwrap();
    }

    #[test]
    fn mounted_hotspot_write_actions_preserve_payload_semantics() {
        let temp = tempdir().unwrap();
        let mount = temp.path().join("mount");
        std::fs::create_dir_all(mount.join("hotspot")).unwrap();
        let client = client_spec_for_plan(temp.path(), "client-a", mount.clone());

        execute_mounted_chaos_action(
            &client,
            &MountedChaosAction::CreateFile {
                path: "hotspot/slot-00.txt".to_string(),
                payload: b"first".to_vec(),
            },
        )
        .unwrap();
        execute_mounted_chaos_action(
            &client,
            &MountedChaosAction::Append {
                path: "hotspot/slot-00.txt".to_string(),
                payload: b"-second".to_vec(),
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read(mount.join("hotspot/slot-00.txt")).unwrap(),
            b"first-second"
        );

        execute_mounted_chaos_action(
            &client,
            &MountedChaosAction::Write {
                path: "hotspot/slot-00.txt".to_string(),
                payload: b"replacement".to_vec(),
            },
        )
        .unwrap();
        assert_eq!(
            std::fs::read(mount.join("hotspot/slot-00.txt")).unwrap(),
            b"replacement"
        );
    }

    #[test]
    fn mounted_hotspot_churn_preserves_primary_error_when_unmount_also_fails() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-hotspot-unmount");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
        let mut driver =
            RecordingMountedChaosDriver::new(Vec::new()).with_unmount_error("umount failed");
        let context = ScenarioContext {
            run_id: "run-hotspot-unmount".to_string(),
            seed: 376,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        let err = run_mounted_hotspot_churn_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(matches!(
            err,
            ScenarioError::MountedHotspotChurnUnmountAfterFailure { .. }
        ));
        assert!(message.contains("requires at least 3 clients"));
        assert!(message.contains("umount failed"));
    }

    #[test]
    fn mounted_hotspot_churn_reports_specific_namespace_invariant_error() {
        let snapshots = vec![crate::mounted_tree::MountedTreeSnapshot {
            schema_version: 1,
            client: "client-a".to_string(),
            mount_point: "mounts/client-a".to_string(),
            entries: vec![crate::mounted_tree::TreeEntry {
                path: "hotspot/slot-00.txt".to_string(),
                entry_type: crate::mounted_tree::TreeEntryKind::File,
                size: Some(12),
                modified_unix_ms: None,
                checksum_sha256: Some(bytes_sha256(b"payload")),
                symlink_target: None,
            }],
        }];

        let err = validate_hotspot_churn_namespace(&snapshots).unwrap_err();

        assert!(matches!(
            err,
            ScenarioError::MountedHotspotChurnInvariantViolations { .. }
        ));
        assert!(err.to_string().contains("missing parent directory"));
    }

    #[test]
    fn registry_exposes_mounted_fault_chaos_convergence_scenario() {
        let scenarios = available_scenarios();

        assert!(scenarios.iter().any(|scenario| {
            scenario.name == MOUNTED_FAULT_CHAOS_CONVERGENCE
                && scenario.requires_studiofs_bin
                && scenario.description.contains("fault chaos")
        }));
        assert!(resolve_scenario(MOUNTED_FAULT_CHAOS_CONVERGENCE).is_some());
        assert!(resolve_scenario("fault-chaos").is_some());
        assert!(resolve_scenario("chaos-faults").is_some());
    }

    #[test]
    fn registry_exposes_mounted_lww_reconnect_chaos_convergence_scenario() {
        let scenarios = available_scenarios();

        assert!(scenarios.iter().any(|scenario| {
            scenario.name == MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE
                && scenario.requires_studiofs_bin
                && scenario.description.contains("LWW reconnect")
        }));
        assert!(resolve_scenario(MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE).is_some());
        assert!(resolve_scenario("lww-chaos").is_some());
        assert!(resolve_scenario("reconnect-lww").is_some());
    }

    #[test]
    fn lww_reconnect_oracle_detects_converged_wrong_winner() {
        let temp = tempdir().unwrap();
        let context = test_context(
            temp.path(),
            "run-lww-oracle",
            MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE,
            360,
        );
        let expected = lww_expected_winner(&context, "client-c");
        let wrong_checksum = bytes_sha256(&lww_reconnect_payload(&context, "client-b", 2));
        let snapshots = ["client-a", "client-b", "client-c"]
            .iter()
            .map(|client| crate::mounted_tree::MountedTreeSnapshot {
                schema_version: 1,
                client: (*client).to_string(),
                mount_point: format!("mounts/{client}"),
                entries: vec![crate::mounted_tree::TreeEntry {
                    path: expected.path.clone(),
                    entry_type: crate::mounted_tree::TreeEntryKind::File,
                    size: Some(expected.bytes),
                    modified_unix_ms: None,
                    checksum_sha256: Some(wrong_checksum.clone()),
                    symlink_target: None,
                }],
            })
            .collect::<Vec<_>>();

        let report = lww_oracle_report(&snapshots, &expected);

        assert!(!report.matched);
        assert_eq!(report.clients.len(), 3);
        assert!(report.clients.iter().all(|client| !client.matched));
    }

    #[test]
    fn mounted_lww_reconnect_waits_for_online_order_one_before_order_two() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-1");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE);
        let clients = LWW_RECONNECT_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| {
                let mount = temp.path().join("mounts").join(name);
                std::fs::create_dir_all(&mount).unwrap();
                client_spec_for(&sandbox, "projectfs", name, mount)
            })
            .collect::<Vec<_>>();
        let mut driver = RecordingLwwReconnectDriver::new(clients);
        let context = ScenarioContext {
            run_id: "run-1".to_string(),
            seed: 360,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        let summary = run_mounted_lww_reconnect_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap();

        assert_eq!(summary.expected_winner.client, "client-c");
        assert_eq!(
            driver.online_writer_barriers.into_inner(),
            vec![
                Some(bytes_sha256(&lww_reconnect_payload(
                    &context, "client-a", 1
                ))),
                Some(bytes_sha256(&lww_reconnect_payload(
                    &context, "client-b", 2
                ))),
            ],
            "the online-writer phase must wait for order 1 to become visible before order 2 is issued"
        );
    }

    struct RecordingLwwReconnectDriver {
        clients: Vec<ClientSpec>,
        mounted_clients: RefCell<Vec<String>>,
        online_writer_barriers: RefCell<Vec<Option<String>>>,
    }

    impl RecordingLwwReconnectDriver {
        fn new(clients: Vec<ClientSpec>) -> Self {
            Self {
                clients,
                mounted_clients: RefCell::new(Vec::new()),
                online_writer_barriers: RefCell::new(Vec::new()),
            }
        }

        fn sync_lww_payload_from_latest_writer(&self, clients: &[ClientSpec]) {
            let source = clients
                .iter()
                .filter_map(|client| {
                    let path = find_test_lww_oracle_path(&client.mount_point)?;
                    let payload = std::fs::read(&path).ok()?;
                    Some((test_lww_oracle_order(&payload), path))
                })
                .max_by_key(|(oracle_order, _)| *oracle_order);
            let Some((_, source_path)) = source else {
                return;
            };
            let payload = std::fs::read(&source_path).unwrap();
            let relative_source_path = clients
                .iter()
                .find_map(|client| source_path.strip_prefix(&client.mount_point).ok())
                .expect("source path must live under one of the client mounts")
                .to_path_buf();
            for client in clients {
                let target = client.mount_point.join(&relative_source_path);
                std::fs::create_dir_all(target.parent().unwrap()).unwrap();
                std::fs::write(target, &payload).unwrap();
            }
        }

        fn sync_tree_from_first_client(&self, clients: &[ClientSpec]) {
            let Some(source) = clients.first() else {
                return;
            };
            for replica in clients.iter().skip(1) {
                copy_test_tree(&source.mount_point, &replica.mount_point);
            }
        }
    }

    impl MountedLwwReconnectDriver for RecordingLwwReconnectDriver {
        fn bootstrap_volume(
            &mut self,
            options: VolumeBootstrapOptions,
        ) -> Result<VolumeBootstrapSummary, ScenarioError> {
            Ok(VolumeBootstrapSummary {
                volume_name: "projectfs".to_string(),
                init_client: options.client_names[0].clone(),
                discovered_clients: options.client_names[1..].to_vec(),
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

        fn mount_client(&mut self, client: &str) -> Result<(), ScenarioError> {
            let mut mounted = self.mounted_clients.borrow_mut();
            if !mounted.iter().any(|name| name == client) {
                mounted.push(client.to_string());
            }
            Ok(())
        }

        fn clients(&self) -> Vec<ClientSpec> {
            self.clients.clone()
        }

        fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError> {
            self.mounted_clients
                .borrow_mut()
                .retain(|name| name != client);
            Ok(())
        }

        fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
            self.mount_client(client)
        }

        fn after_phase(
            &mut self,
            phase: MountedLwwReconnectPhase,
            clients: &[ClientSpec],
        ) -> Result<(), ScenarioError> {
            if matches!(
                phase,
                MountedLwwReconnectPhase::HealthyWarmup
                    | MountedLwwReconnectPhase::ColdObserverBootstrap
            ) {
                self.sync_tree_from_first_client(clients);
            } else {
                self.sync_lww_payload_from_latest_writer(clients);
            }
            if phase == MountedLwwReconnectPhase::OnlineWriters {
                let checksum = clients.first().and_then(|client| {
                    find_test_lww_oracle_path(&client.mount_point)
                        .and_then(|path| std::fs::read(path).ok())
                        .map(|payload| bytes_sha256(&payload))
                });
                self.online_writer_barriers.borrow_mut().push(checksum);
            }
            Ok(())
        }

        fn unmount_all(&mut self) -> Result<(), ScenarioError> {
            self.mounted_clients.borrow_mut().clear();
            Ok(())
        }
    }

    fn copy_test_tree(source: &Path, target: &Path) {
        std::fs::create_dir_all(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let source_path = entry.path();
            let target_path = target.join(entry.file_name());
            if source_path.is_dir() {
                copy_test_tree(&source_path, &target_path);
            } else {
                std::fs::create_dir_all(target_path.parent().unwrap()).unwrap();
                std::fs::copy(source_path, target_path).unwrap();
            }
        }
    }

    fn collect_test_files(root: &Path, current: &Path, files: &mut Vec<(PathBuf, Vec<u8>)>) {
        let Ok(entries) = std::fs::read_dir(current) else {
            return;
        };
        for entry in entries {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                collect_test_files(root, &path, files);
            } else {
                let relative_path = path.strip_prefix(root).unwrap().to_path_buf();
                files.push((relative_path, std::fs::read(path).unwrap()));
            }
        }
    }

    fn test_lww_oracle_order(payload: &[u8]) -> u32 {
        std::str::from_utf8(payload)
            .ok()
            .and_then(|text| text.split("oracle_order=").nth(1))
            .and_then(|suffix| suffix.trim().parse().ok())
            .unwrap_or(0)
    }

    fn find_test_lww_oracle_path(mount: &Path) -> Option<PathBuf> {
        let root = mount.join("lww-chaos");
        find_named_file(&root, "winner.txt")
    }

    fn find_named_file(root: &Path, name: &str) -> Option<PathBuf> {
        for entry in std::fs::read_dir(root).ok()? {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(found) = find_named_file(&path, name) {
                    return Some(found);
                }
            } else if entry.file_name() == name {
                return Some(path);
            }
        }
        None
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
        let operations =
            read_operation_log(context.scenario_dir.join("operations.ndjson")).unwrap();
        assert!(operations.iter().any(|record| {
            record
                .metadata
                .get("mode")
                .is_some_and(|mode| mode == "after_local_durability_boundary")
        }));
        assert!(operations.iter().any(|record| {
            record
                .metadata
                .get("mode")
                .is_some_and(|mode| mode == "before_remote_publication_completes")
        }));
    }

    #[cfg(windows)]
    #[test]
    fn crash_restart_keeps_durable_payload_handle_open_until_crash() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-crash-restart-handle-lifetime");
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
        let mut driver = RecordingCrashRestartDriver::new(clients.clone())
            .publish_current_file_before_convergence()
            .require_open_payload_handle_during_kill(CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME);
        let context = ScenarioContext {
            run_id: "run-crash-restart-handle-lifetime".to_string(),
            seed: 327,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        run_crash_restart_mode(
            &context,
            &mut driver,
            &clients,
            CrashRestartMode::BeforeRemotePublicationCompletes,
            CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
            b"accepted local payload killed before remote publication completes\n",
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap();
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
            Some(summary.modes[1].local_read_checksum_sha256.as_str())
        );
        assert!(
            summary
                .modes
                .iter()
                .all(|mode| mode.final_validator.converged)
        );
    }

    #[test]
    fn crash_restart_fails_when_restart_read_differs_from_written_payload() {
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

        assert!(err.to_string().contains("does not match written payload"));
    }

    #[test]
    fn crash_restart_fails_when_final_peer_payload_differs_from_written_payload() {
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

        assert!(
            err.to_string()
                .contains("peer data does not match written payload"),
            "{err}"
        );
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

        assert!(summary.modes.iter().all(|mode| {
            mode.final_validator.compared_clients
                == client_names
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect::<Vec<_>>()
        }));
    }

    #[test]
    fn publication_boundary_crash_matrix_covers_boundaries_and_blocks_premature_peer_visibility() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-publication-boundary");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX);
        let clients = PUBLICATION_BOUNDARY_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| {
                let mount = temp.path().join("mounts").join(name);
                std::fs::create_dir_all(&mount).unwrap();
                client_spec_for(&sandbox, "projectfs", name, mount)
            })
            .collect::<Vec<_>>();
        let original_client_root = clients[0].client_root.clone();
        let mut driver = RecordingPublicationBoundaryDriver::new(clients);
        let context = ScenarioContext {
            run_id: "run-publication-boundary".to_string(),
            seed: 375,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        let summary = run_publication_boundary_crash_matrix_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap();

        assert_eq!(summary.scenario, MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX);
        assert_eq!(summary.boundaries.len(), 4);
        assert_eq!(
            summary
                .boundaries
                .iter()
                .map(|boundary| boundary.boundary)
                .collect::<Vec<_>>(),
            vec![
                PublicationBoundary::Flush,
                PublicationBoundary::Fsync,
                PublicationBoundary::ReleaseClose,
                PublicationBoundary::ShutdownDrain,
            ]
        );
        assert!(summary.boundaries.iter().all(|boundary| {
            boundary.restart_client_root == original_client_root.display().to_string()
                && boundary.local_read_checksum_sha256 == boundary.final_checksum_sha256
                && boundary
                    .peer_visibility_at_publication_gate
                    .iter()
                    .all(|visibility| visibility.visible_checksum_sha256.is_some())
                && boundary.final_validator.converged
        }));
        assert_eq!(
            driver.actions.borrow().as_slice(),
            [
                "bootstrap",
                "mount_all",
                "force_kill_client-a",
                "remount_client-a",
                "publish_flush",
                "refresh_client-b_for_convergence",
                "refresh_client-c_for_convergence",
                "force_kill_client-a",
                "remount_client-a",
                "publish_fsync",
                "refresh_client-b_for_convergence",
                "refresh_client-c_for_convergence",
                "force_kill_client-a",
                "remount_client-a",
                "publish_release_close",
                "refresh_client-b_for_convergence",
                "refresh_client-c_for_convergence",
                "graceful_shutdown_client-a",
                "remount_client-a",
                "publish_shutdown_drain",
                "refresh_client-b_for_convergence",
                "refresh_client-c_for_convergence",
                "unmount_all",
            ]
        );

        assert!(
            context
                .scenario_dir
                .join("publication-boundary-summary.json")
                .exists()
        );
        let operations =
            read_operation_log(context.scenario_dir.join("operations.ndjson")).unwrap();
        for boundary in ["flush", "fsync", "release_close", "shutdown_drain"] {
            assert!(operations.iter().any(|record| {
                record
                    .metadata
                    .get("boundary")
                    .is_some_and(|value| value == boundary)
                    && matches!(
                        record.operation,
                        SharedOperation::ClientCrash | SharedOperation::ClientRestart
                    )
            }));
        }
        assert!(
            operations
                .iter()
                .any(|record| matches!(record.operation, SharedOperation::Flush { .. }))
        );
        assert!(
            operations
                .iter()
                .any(|record| matches!(record.operation, SharedOperation::Fsync { .. }))
        );
        assert!(
            operations
                .iter()
                .any(|record| matches!(record.operation, SharedOperation::Release { .. }))
        );
        assert!(
            operations
                .iter()
                .any(|record| matches!(record.operation, SharedOperation::Close { .. }))
        );
        assert!(operations.iter().any(|record| {
            matches!(
                &record.operation,
                SharedOperation::SchedulerCheckpoint { name }
                    if name == "publication_boundary_checkpoint"
            )
        }));
        assert!(!operations.iter().any(|record| {
            matches!(record.operation, SharedOperation::ClientCrash)
                && record
                    .metadata
                    .get("boundary")
                    .is_some_and(|value| value == "shutdown_drain")
        }));
    }

    #[test]
    fn publication_boundary_crash_matrix_records_peer_visibility_at_publication_gate() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-publication-boundary-leak");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX);
        let clients = PUBLICATION_BOUNDARY_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| {
                let mount = temp.path().join("mounts").join(name);
                std::fs::create_dir_all(&mount).unwrap();
                client_spec_for(&sandbox, "projectfs", name, mount)
            })
            .collect::<Vec<_>>();
        let mut driver = RecordingPublicationBoundaryDriver::new(clients);
        let context = ScenarioContext {
            run_id: "run-publication-boundary-leak".to_string(),
            seed: 375,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        let summary = run_publication_boundary_crash_matrix_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap();

        let flush = summary
            .boundaries
            .iter()
            .find(|boundary| boundary.boundary == PublicationBoundary::Flush)
            .unwrap();
        assert!(
            flush
                .peer_visibility_at_publication_gate
                .iter()
                .all(|visibility| visibility.visible_checksum_sha256
                    == Some(flush.final_checksum_sha256.clone()))
        );
    }

    #[test]
    fn publication_boundary_crash_matrix_allows_product_publication_before_scenario_gate() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-publication-boundary-product-visible");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX);
        let clients = PUBLICATION_BOUNDARY_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| {
                let mount = temp.path().join("mounts").join(name);
                std::fs::create_dir_all(&mount).unwrap();
                client_spec_for(&sandbox, "projectfs", name, mount)
            })
            .collect::<Vec<_>>();
        let mut driver =
            RecordingPublicationBoundaryDriver::new(clients).leak_premature_peer_file();
        let context = ScenarioContext {
            run_id: "run-publication-boundary-product-visible".to_string(),
            seed: 382,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        let summary = run_publication_boundary_crash_matrix_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap();

        assert_eq!(summary.boundaries.len(), 4);
        assert!(summary.boundaries.iter().all(|boundary| {
            boundary
                .peer_visibility_at_publication_gate
                .iter()
                .all(|visibility| visibility.visible_checksum_sha256.is_some())
        }));
    }

    #[cfg(windows)]
    #[test]
    fn publication_boundary_flush_keeps_handle_open_until_crash() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-publication-boundary-handle-lifetime");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX);
        std::fs::create_dir_all(&scenario_dir).unwrap();
        let clients = PUBLICATION_BOUNDARY_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| {
                let mount = temp.path().join("mounts").join(name);
                std::fs::create_dir_all(&mount).unwrap();
                client_spec_for(&sandbox, "projectfs", name, mount)
            })
            .collect::<Vec<_>>();
        let mut driver = RecordingPublicationBoundaryDriver::new(clients.clone())
            .require_open_boundary_handle_during_kill(PublicationBoundary::Flush);
        let context = ScenarioContext {
            run_id: "run-publication-boundary-handle-lifetime".to_string(),
            seed: 375,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        run_publication_boundary(
            &context,
            &mut driver,
            &clients,
            PublicationBoundary::Flush,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap();
    }

    #[test]
    fn publication_boundary_refresh_attempts_all_peers_before_reporting_failure() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-publication-boundary-refresh-failure");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_PUBLICATION_BOUNDARY_CRASH_MATRIX);
        let clients = PUBLICATION_BOUNDARY_CLIENT_NAMES
            .iter()
            .copied()
            .map(|name| {
                let mount = temp.path().join("mounts").join(name);
                std::fs::create_dir_all(&mount).unwrap();
                client_spec_for(&sandbox, "projectfs", name, mount)
            })
            .collect::<Vec<_>>();
        let mut driver =
            RecordingPublicationBoundaryDriver::new(clients).fail_refresh_for("client-b");
        let context = ScenarioContext {
            run_id: "run-publication-boundary-refresh-failure".to_string(),
            seed: 382,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        let err = run_publication_boundary_crash_matrix_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap_err();

        assert!(
            err.to_string().contains("refresh failed for client-b"),
            "{err}"
        );
        assert!(
            driver
                .actions
                .borrow()
                .contains(&"refresh_client-c_for_convergence".to_string()),
            "{:?}",
            driver.actions.borrow()
        );
    }

    #[test]
    fn publication_boundary_operations_map_boundaries_to_specific_shared_operations() {
        assert!(matches!(
            publication_boundary_operations(PublicationBoundary::Flush, "boundary/file.txt")
                .as_slice(),
            [SharedOperation::Flush { path }] if path.as_deref() == Some("boundary/file.txt")
        ));
        assert!(matches!(
            publication_boundary_operations(PublicationBoundary::Fsync, "boundary/file.txt")
                .as_slice(),
            [SharedOperation::Fsync { path }] if path == "boundary/file.txt"
        ));
        assert!(matches!(
            publication_boundary_operations(
                PublicationBoundary::ReleaseClose,
                "boundary/file.txt"
            )
            .as_slice(),
            [
                SharedOperation::Close { path: close_path },
                SharedOperation::Release { path: release_path },
            ] if close_path.as_deref() == Some("boundary/file.txt")
                && release_path.as_deref() == Some("boundary/file.txt")
        ));
        assert!(matches!(
            publication_boundary_operations(
                PublicationBoundary::ShutdownDrain,
                "boundary/file.txt"
            )
            .as_slice(),
            [SharedOperation::Fsync { path }] if path == "boundary/file.txt"
        ));
    }

    struct FakeDrainExecutor {
        outputs: RefCell<Vec<CommandOutput>>,
        commands: RefCell<Vec<CommandSpec>>,
    }

    impl CommandExecutor for FakeDrainExecutor {
        fn run(&self, command: &CommandSpec) -> std::io::Result<CommandOutput> {
            self.commands.borrow_mut().push(command.clone());
            Ok(self.outputs.borrow_mut().remove(0))
        }
    }

    #[test]
    fn publication_boundary_release_close_waits_for_observed_local_drain() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-publication-boundary-drain");
        let client = client_spec_for(
            &sandbox,
            "projectfs",
            "client-a",
            temp.path().join("mounts").join("client-a"),
        );
        let config = test_client_controller_config(&sandbox);
        let executor = FakeDrainExecutor {
            outputs: RefCell::new(vec![
                publication_boundary_drain_output(32),
                publication_boundary_drain_output(0),
            ]),
            commands: RefCell::new(Vec::new()),
        };

        wait_for_publication_boundary_local_drain(
            &executor,
            &config,
            &client,
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .unwrap();

        let commands = executor.commands.borrow();
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].args[0], "internal");
        assert_eq!(commands[0].args[1], "diagnostics-attach");
        assert_eq!(
            commands[0].env,
            vec![(
                "STUDIOFS_CLIENT_ROOT".to_string(),
                client.client_root.to_string_lossy().into_owned()
            )]
        );
    }

    #[test]
    fn publication_boundary_release_close_waits_for_pending_local_sync_bytes() {
        let temp = tempdir().unwrap();
        let sandbox = temp
            .path()
            .join("run-publication-boundary-local-sync-drain");
        let client = client_spec_for(
            &sandbox,
            "projectfs",
            "client-a",
            temp.path().join("mounts").join("client-a"),
        );
        let config = test_client_controller_config(&sandbox);
        let executor = FakeDrainExecutor {
            outputs: RefCell::new(vec![
                publication_boundary_drain_output_with_local_sync(0, 32),
                publication_boundary_drain_output_with_local_sync(0, 0),
            ]),
            commands: RefCell::new(Vec::new()),
        };

        wait_for_publication_boundary_local_drain(
            &executor,
            &config,
            &client,
            Duration::from_secs(1),
            Duration::from_millis(1),
        )
        .unwrap();

        assert_eq!(
            executor.commands.borrow().len(),
            2,
            "release-close drain must wait for payload-store local sync bytes, not only dirty pressure"
        );
    }

    #[test]
    fn publication_boundary_drain_snapshot_requires_dirty_pressure_fields() {
        let err = PublicationBoundaryDrainSnapshot::from_diagnostics_json(&json!({
            "runtime_snapshot": {
                "write_path": {}
            }
        }))
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("/runtime_snapshot/write_path/dirty_pressure"),
            "{err}"
        );
    }

    fn publication_boundary_drain_output(total_dirty_bytes: u64) -> CommandOutput {
        publication_boundary_drain_output_with_local_sync(total_dirty_bytes, 0)
    }

    fn publication_boundary_drain_output_with_local_sync(
        total_dirty_bytes: u64,
        local_sync_unsynced_bytes: u64,
    ) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: json!({
                "runtime_snapshot": {
                    "write_path": {
                        "dirty_pressure": {
                            "pending_local_dirty_bytes": total_dirty_bytes,
                            "pending_persisted_slice_bytes": total_dirty_bytes,
                            "writer_stack_dirty_bytes": total_dirty_bytes,
                            "total_dirty_bytes": total_dirty_bytes
                        },
                        "drain_lag": {
                            "local_sync_unsynced_bytes": local_sync_unsynced_bytes
                        }
                    }
                }
            })
            .to_string(),
            stderr: String::new(),
        }
    }

    #[test]
    fn client_mounts_scope_remote_metadata_probe_to_configured_volume() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-1");
        let config = test_client_controller_config(&sandbox);
        let context = ScenarioContext {
            run_id: "run-1".to_string(),
            seed: 389,
            sandbox: sandbox.clone(),
            scenario_dir: sandbox.join("artifacts").join("scenarios").join("unit"),
            client_controller_config: Some(config.clone()),
        };
        let clients = vec![client_spec_for(
            &sandbox,
            &config.volume_name,
            "client-a",
            temp.path().join("mount-a"),
        )];

        let mounts = client_mounts(&context, &clients);

        let probe = mounts[0].remote_metadata_probe.as_ref().unwrap();
        assert_eq!(probe.meta_dsn, config.meta_dsn);
        assert_eq!(probe.volume_name, config.volume_name);
    }

    fn test_client_controller_config(sandbox: &Path) -> ClientControllerConfig {
        ClientControllerConfig {
            studiofs_bin: PathBuf::from("studiofs"),
            sandbox: sandbox.to_path_buf(),
            volume_name: "test-volume".to_string(),
            meta_dsn: "postgres://studiofs:studiofs@127.0.0.1:5432".to_string(),
            s3: S3Config {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "studiofs-e2e-test".to_string(),
                access_key: "rustfsadmin".to_string(),
                secret_key: "rustfsadmin".to_string(),
                region: "us-east-1".to_string(),
            },
            init_retry_timeout: Duration::from_secs(1),
            mount_ready_timeout: Duration::from_secs(1),
            mount_exit_timeout: Duration::from_secs(1),
            command_timeouts: ClientCommandTimeouts::default(),
            poll_interval: Duration::from_millis(1),
            mount_points: Vec::new(),
        }
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
        let mut driver = RecordingOutageDriver::new(clients);
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
        assert_eq!(summary.operations_accepted_during_outage, 2);
        assert!(summary.initial_publication_gate.converged);
        assert!(summary.outage_duration_ms > 0);
        assert!(summary.recovery_time_ms > 0);
        assert!(summary.final_validator.converged);
        assert_eq!(
            driver.actions.borrow().as_slice(),
            [
                "bootstrap",
                "mount_all",
                "after_initial_write",
                "stop_postgres",
                "start_postgres",
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
        assert_eq!(summary_json["initial_publication_gate"]["converged"], true);
        assert_eq!(summary_json["operations_accepted_during_outage"], 2);
        assert!(
            context
                .scenario_dir
                .join("outage-operations.ndjson")
                .exists()
        );
        let operations =
            read_operation_log(context.scenario_dir.join("operations.ndjson")).unwrap();
        assert!(
            operations
                .iter()
                .any(|record| matches!(&record.outcome, OperationOutcome::Accepted))
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
        assert_eq!(
            summary.outage_file,
            "rustfs-outage/run-1/studiofs-e2e-rustfs-outage-recovered.txt"
        );
        assert_eq!(
            summary.local_read_checksum_sha256,
            summary.final_checksum_sha256
        );
        assert_eq!(summary.peer_visibility_during_outage.len(), 3);
        assert!(
            summary
                .peer_visibility_during_outage
                .iter()
                .all(|probe| { probe.visible_checksum_sha256.is_none() })
        );
        assert_eq!(summary.peer_visible_during_outage, None);
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
    fn rustfs_outage_uses_run_unique_path_and_payload() {
        let first = run_recorded_rustfs_outage_for_id("run-one");
        let second = run_recorded_rustfs_outage_for_id("run-two");

        assert_ne!(first.outage_file, second.outage_file);
        assert!(first.outage_file.contains("run-one"));
        assert!(second.outage_file.contains("run-two"));
        assert_ne!(
            first.local_read_checksum_sha256,
            second.local_read_checksum_sha256
        );
        assert_ne!(first.final_checksum_sha256, second.final_checksum_sha256);
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

    fn run_recorded_rustfs_outage_for_id(run_id: &str) -> RustfsOutageRecoverySummary {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join(run_id);
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
            run_id: run_id.to_string(),
            seed: 326,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };

        run_rustfs_outage_with_driver(
            &context,
            &mut driver,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .unwrap()
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
    fn outage_operations_log_rejected_write_before_failing_expected_operation() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-1");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
        let accepted_mount = temp.path().join("mounts").join("client-b");
        let rejected_mount = temp.path().join("mounts").join("client-c");
        std::fs::create_dir_all(&accepted_mount).unwrap();
        std::fs::write(&rejected_mount, "not a directory").unwrap();
        let clients = vec![
            client_spec_for(&sandbox, "projectfs", "client-b", accepted_mount),
            client_spec_for(&sandbox, "projectfs", "client-c", rejected_mount),
        ];
        let context = ScenarioContext {
            run_id: "run-1".to_string(),
            seed: 325,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };
        std::fs::create_dir_all(&context.scenario_dir).unwrap();

        let err = perform_outage_operations(&context, &clients).unwrap_err();

        assert!(matches!(
            err,
            ScenarioError::RejectedOutageOperations {
                rejected: 1,
                total: 2
            }
        ));
        let log =
            read_operation_log(context.scenario_dir.join("outage-operations.ndjson")).unwrap();
        assert!(
            log.iter()
                .any(|record| matches!(&record.outcome, OperationOutcome::Accepted))
        );
        assert!(
            log.iter()
                .any(|record| matches!(&record.outcome, OperationOutcome::Rejected { .. }))
        );
    }

    #[test]
    fn mounted_fault_chaos_restarts_postgres_before_rejected_outage_failure() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-fault-chaos");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_FAULT_CHAOS_CONVERGENCE);
        let clients = fault_chaos_clients(&sandbox, temp.path(), Some(1), None);
        let mut driver = RecordingMountedFaultChaosDriver::new(clients.clone());
        let context = ScenarioContext {
            run_id: "run-fault-chaos".to_string(),
            seed: 362,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };
        std::fs::create_dir_all(&context.scenario_dir).unwrap();

        let err = run_fault_chaos_postgres_phase(
            &context,
            &mut driver,
            Instant::now(),
            &clients,
            &mut Vec::new(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ScenarioError::RejectedOutageOperations {
                rejected: 1,
                total: 1
            }
        ));
        assert_eq!(
            driver.actions.borrow().as_slice(),
            ["stop_postgres", "start_postgres"]
        );
    }

    #[test]
    fn mounted_fault_chaos_restarts_rustfs_before_rejected_outage_failure() {
        let temp = tempdir().unwrap();
        let sandbox = temp.path().join("run-fault-chaos");
        let scenario_dir = sandbox
            .join("artifacts")
            .join("scenarios")
            .join(MOUNTED_FAULT_CHAOS_CONVERGENCE);
        let clients = fault_chaos_clients(&sandbox, temp.path(), None, Some(2));
        let mut driver = RecordingMountedFaultChaosDriver::new(clients.clone());
        let context = ScenarioContext {
            run_id: "run-fault-chaos".to_string(),
            seed: 362,
            sandbox,
            scenario_dir,
            client_controller_config: None,
        };
        std::fs::create_dir_all(&context.scenario_dir).unwrap();

        let err = run_fault_chaos_rustfs_phase(
            &context,
            &mut driver,
            Instant::now(),
            &clients,
            &mut Vec::new(),
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ScenarioError::RejectedOutageOperations {
                rejected: 1,
                total: 1
            }
        ));
        assert_eq!(
            driver.actions.borrow().as_slice(),
            ["stop_rustfs", "start_rustfs"]
        );
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

        let err = perform_outage_operations(&context, &[]).unwrap_err();

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
            init_retry_timeout: Duration::from_secs(1),
            mount_ready_timeout: Duration::from_secs(1),
            mount_exit_timeout: Duration::from_secs(1),
            command_timeouts: Default::default(),
            poll_interval: Duration::from_millis(10),
            mount_points: Vec::new(),
        }
    }
}

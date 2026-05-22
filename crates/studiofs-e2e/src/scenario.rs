//! Scenario registry and lifecycle hooks for mounted E2E runs.

use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
#[cfg(not(test))]
use std::process::{Command, Stdio};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::client::{
    ClientControllerConfig, ClientControllerError, ClientSpec, MultiClientController,
    PeerVolumeBootstrapOptions, PeerVolumeBootstrapSummary, SystemClientProcessExecutor,
    VolumeBootstrapOptions, VolumeBootstrapSummary,
};
#[cfg(all(windows, not(test)))]
use crate::client::{run_taskkill_with_timeout, windows_taskkill_status_is_success};
use crate::infra::{
    CommandExecutor, ComposeServiceControl, InfraError, POSTGRES_SERVICE_NAME, RUSTFS_SERVICE_NAME,
    SystemCommandExecutor,
};
use crate::mounted_tree::{
    ClientMount, ConvergenceOutcome, MountedTreeError, NamespaceInvariantViolation, TreeComparison,
    validate_namespace_invariants, wait_for_convergence,
};
#[cfg(all(windows, not(test)))]
use crate::process_diagnostics::capture_process_diagnostics_best_effort;
use crate::process_job::CHILD_TERMINATION_GRACE;
#[cfg(not(test))]
use crate::process_job::{ChildJob, wait_for_child_exit};
use crate::report::RunManifest;

/// Basic multi-client smoke scenario identifier.
pub const BASIC_MULTI_CLIENT_SMOKE: &str = "basic-multi-client-smoke";
/// Multi-client recovery scenario for PostgreSQL outages.
pub const POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY: &str = "postgres-outage-multi-client-recovery";
/// Multi-client recovery scenario for RustFS/S3 outages.
pub const RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY: &str = "rustfs-outage-multi-client-recovery";
/// Multi-client recovery scenario for mounted client crash and restart.
pub const MOUNTED_CLIENT_CRASH_RESTART_RECOVERY: &str = "mounted-client-crash-restart-recovery";
/// Publication-boundary scenario before a write reaches full remote acceptance.
pub const MOUNTED_CRASH_BEFORE_WRITE_ACCEPTANCE: &str = "mounted-crash-before-write-acceptance";
/// Publication-boundary scenario with partial remote write acceptance before crash.
pub const MOUNTED_CRASH_AFTER_PARTIAL_WRITE_ACCEPTANCE: &str =
    "mounted-crash-after-partial-write-acceptance";
/// Publication-boundary scenario with full remote write acceptance before crash.
pub const MOUNTED_CRASH_AFTER_FULL_WRITE_ACCEPTANCE: &str =
    "mounted-crash-after-full-write-acceptance";
/// Namespace-collision scenario for rename-vs-rename races.
pub const COLLISION_RENAME_VS_RENAME: &str = "collision-rename-vs-rename";
/// Namespace-collision scenario for rename-vs-unlink races.
pub const COLLISION_RENAME_VS_UNLINK: &str = "collision-rename-vs-unlink";
/// Namespace-collision scenario for unlink-vs-append races.
pub const COLLISION_UNLINK_VS_APPEND: &str = "collision-unlink-vs-append";
/// Namespace-collision scenario for overwrite-vs-read races.
pub const COLLISION_OVERWRITE_VS_READ: &str = "collision-overwrite-vs-read";
/// Namespace-collision scenario for mkdir/rmdir-vs-create races.
pub const COLLISION_MKDIR_RMDIR_VS_CREATE: &str = "collision-mkdir-rmdir-vs-create";
/// Namespace-collision scenario for readdir/stat races during mutation.
pub const COLLISION_READDIR_STAT_DURING_MUTATION: &str = "collision-readdir-stat-during-mutation";
/// Chaos convergence scenario for mounted clients.
pub const MOUNTED_CHAOS_CONVERGENCE: &str = "mounted-chaos-convergence";
/// Chaos convergence scenario for hotspot churn workloads.
pub const MOUNTED_HOTSPOT_CHURN_CONVERGENCE: &str = "mounted-hotspot-churn-convergence";
/// Chaos convergence scenario for injected fault handling.
pub const MOUNTED_FAULT_CHAOS_CONVERGENCE: &str = "mounted-fault-chaos-convergence";
/// Chaos convergence scenario for LWW reconnect behavior.
pub const MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE: &str = "mounted-lww-reconnect-chaos-convergence";
/// Open-handle convergence scenario under concurrent mutations.
pub const MOUNTED_OPEN_HANDLE_CONVERGENCE: &str = "mounted-open-handle-convergence";
/// Sparse-range convergence scenario.
pub const MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE: &str = "mounted-sparse-file-range-convergence";
/// Long-history cold-observer convergence scenario.
pub const MOUNTED_COLD_OBSERVER_LONG_HISTORY_CONVERGENCE: &str =
    "mounted-cold-observer-long-history-convergence";
/// Stale-remount cold-discover catchup convergence scenario.
pub const MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE: &str =
    "mounted-stale-remount-cold-discover-catchup-convergence";
/// Invalidation-dropout fallback convergence scenario.
pub const MOUNTED_INVALIDATION_DROPOUT_FALLBACK_CONVERGENCE: &str =
    "mounted-invalidation-dropout-fallback-convergence";
/// Symlink attribute convergence scenario.
pub const MOUNTED_SYMLINK_ATTRIBUTE_CONVERGENCE: &str = "mounted-symlink-attribute-convergence";
const SMOKE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const COLLISION_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const ADVANCED_MOUNTED_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const FAULT_CHAOS_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
const LWW_RECONNECT_CLIENT_NAMES: &[&str] =
    &["client-a", "client-b", "client-c", "client-d", "client-e"];
const COLD_OBSERVER_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
const COLD_OBSERVER_WARM_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const STALE_REMOUNT_WARM_WRITER_NAMES: &[&str] = &["client-a", "client-b"];
const STALE_REMOUNT_INITIAL_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const INVALIDATION_DROPOUT_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const INVALIDATION_DROPOUT_WRITER_CLIENTS: &[&str] = &["client-a", "client-b"];
const INVALIDATION_DROPOUT_OBSERVER_CLIENT: &str = "client-c";
const SYMLINK_ATTRIBUTE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
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
const COLLISION_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const COLLISION_OLD_COMMITTED_PAYLOAD: &[u8] = b"old committed payload\n";
const COLLISION_NEW_COMMITTED_PAYLOAD: &[u8] = b"new committed payload\n";
const OVERWRITE_READ_ATTEMPTS: u32 = 8;
const ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(90);
const ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const MOUNTED_WRITE_READBACK_TIMEOUT: Duration = Duration::from_secs(5);
const MOUNTED_WRITE_READBACK_POLL_INTERVAL: Duration = Duration::from_millis(25);
const MOUNTED_VALIDATION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const MOUNTED_VALIDATION_MAX_ACTIVE_PROBES: usize = 1;
const HOTSPOT_CHURN_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const HOTSPOT_HELPER_CANCEL_GRACE: Duration = Duration::from_millis(750);
const HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER: &str = "hotspot helper internal timeout";
const OPEN_HANDLE_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const OPEN_HANDLE_SYNC_RETRY_TIMEOUT: Duration = Duration::from_secs(2);
const OPEN_HANDLE_SYNC_RETRY_POLL_INTERVAL: Duration = Duration::from_millis(25);
const HOTSPOT_IO_MAX_ACTIVE_WORKERS: usize = 1;
static HOTSPOT_HELPER_PAYLOAD_ID: AtomicUsize = AtomicUsize::new(0);
const LWW_RECONNECT_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
const FAULT_CHAOS_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
const MOUNTED_CHAOS_INITIAL_TREE: &[(&str, &[u8])] = &[
    ("chaos/rename-source.txt", b"rename source\n"),
    ("chaos/unlink-target.txt", b"unlink target\n"),
    ("chaos/append-target.txt", b"append target\n"),
    ("chaos/read-target.txt", COLLISION_OLD_COMMITTED_PAYLOAD),
];
const OPEN_HANDLE_UNLINK_TARGET: &str = "open-handles/unlink-target.txt";
const OPEN_HANDLE_RENAME_SOURCE: &str = "open-handles/rename-source.txt";
const OPEN_HANDLE_RENAME_DEST: &str = "open-handles/renamed-destination.txt";
const OPEN_HANDLE_UNLINK_TARGET_PAYLOAD: &[u8] = b"open handle initial\n";
const OPEN_HANDLE_RENAME_SOURCE_PAYLOAD: &[u8] = b"rename source\n";
const SPARSE_FILE_PATH: &str = "sparse/shared.bin";
const SPARSE_INITIAL_LOGICAL_SIZE: u64 = 2_097_275;
const SPARSE_SHRUNK_LOGICAL_SIZE: u64 = 1_048_833;
const SPARSE_FINAL_LOGICAL_SIZE: u64 = 3_146_061;
const SPARSE_FIRST_BOUNDARY_OFFSET: u64 = 1_048_569;
const SPARSE_SECOND_BOUNDARY_OFFSET: u64 = 2_097_147;
const SPARSE_FINAL_TAIL_OFFSET: u64 = 3_145_900;
const SPARSE_PREFIX_PAYLOAD: &[u8] = b"sparse-prefix-sfs-377\n";
const SPARSE_FIRST_BOUNDARY_PAYLOAD: &[u8] = b"crosses-first-chunk-boundary-sfs-377\n";
const SPARSE_SECOND_BOUNDARY_PAYLOAD: &[u8] = b"crosses-second-chunk-boundary-sfs-377\n";
const SPARSE_FINAL_TAIL_PAYLOAD: &[u8] = b"sparse-final-tail-sfs-377\n";
const COLD_OBSERVER_DELETE_TARGET: &str = "delete-target.txt";
const COLD_OBSERVER_RENAME_SOURCE: &str = "rename-source.txt";
const COLD_OBSERVER_RENAME_DEST: &str = "renamed-final.txt";
const COLD_OBSERVER_OVERWRITE_TARGET: &str = "overwrite-target.txt";
const COLD_OBSERVER_APPEND_TARGET: &str = "append-target.txt";
const COLD_OBSERVER_NESTED_FILE: &str = "nested/child.txt";
const SYMLINK_ATTRIBUTE_TARGET_V1: &str = "target-v1.txt";
const SYMLINK_ATTRIBUTE_TARGET_V2: &str = "target-v2.txt";
const SYMLINK_ATTRIBUTE_LINK: &str = "link-to-target";
const SYMLINK_ATTRIBUTE_RENAMED_LINK: &str = "renamed-link";
const SYMLINK_ATTRIBUTE_READONLY_FILE: &str = "readonly-attribute.txt";
const SYMLINK_ATTRIBUTE_NEARBY_FILE: &str = "near-symlink-write.txt";
const INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS: u64 = 3_000;
const INVALIDATION_DROPOUT_STALE_WINDOW_MS: u64 = 500;
const INVALIDATION_DROPOUT_BASELINE_FILE: &str = "baseline.txt";
const INVALIDATION_DROPOUT_TARGET_FILE: &str = "fallback-target.txt";
const INVALIDATION_DROPOUT_DELETE_TARGET: &str = "delete-target.txt";

#[cfg(test)]
/// Scenario identifier for test helper that expects validation failures.
pub(crate) const TEST_FAILING_VALIDATE: &str = "__test-failing-validate";
#[cfg(test)]
/// Scenario identifier for test helper that expects validation failures with cleanup failures.
pub(crate) const TEST_FAILING_VALIDATE_AND_CLEANUP: &str = "__test-failing-validate-and-cleanup";
#[cfg(test)]
/// Scenario identifier for test helper that expects passing validation.
pub(crate) const TEST_PASSING: &str = "__test-passing";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Static definition of a scenario, including user-facing metadata.
pub struct ScenarioDefinition {
    /// Scenario name.
    pub name: &'static str,
    /// Human-readable description.
    pub description: &'static str,
    /// Whether this scenario requires `studiofs` binary explicitly.
    pub requires_studiofs_bin: bool,
}

#[derive(Debug, Clone)]
/// Shared context provided to scenario setup, execution, validation, and cleanup.
pub struct ScenarioContext {
    /// Identifier for this run.
    pub run_id: String,
    /// Deterministic random seed for this run.
    pub seed: u64,
    /// Sandbox directory for this run.
    pub sandbox: PathBuf,
    /// Scenario artifact directory.
    pub scenario_dir: PathBuf,
    /// Optional client controller configuration override.
    pub client_controller_config: Option<ClientControllerConfig>,
}

#[derive(Debug, Error)]
/// Error surface for scenario orchestration.
pub enum ScenarioError {
    /// Underlying I/O error while manipulating files or process output.
    #[error("scenario I/O failed: {0}")]
    Io(
        /// Source I/O error.
        #[from]
        std::io::Error,
    ),
    /// JSON serialization or deserialization failed.
    #[error("scenario JSON failed: {0}")]
    Json(
        /// Source JSON encoding/decoding error.
        #[from]
        serde_json::Error,
    ),
    /// Client controller returned an operational failure.
    #[error("scenario client controller failed: {0}")]
    Client(
        /// Source client controller error.
        #[from]
        ClientControllerError,
    ),
    /// Mounted smoke scenario failed and unmount also failed.
    #[error("mounted smoke scenario failed: {primary}; unmount also failed: {cleanup}")]
    MountedSmokeUnmountAfterFailure {
        /// Primary scenario failure.
        primary: String,
        /// Unmount and cleanup failure while tearing down.
        cleanup: String,
    },
    /// PostgreSQL outage scenario failed and unmount also failed.
    #[error("PostgreSQL outage scenario failed: {primary}; unmount also failed: {cleanup}")]
    PostgresOutageUnmountAfterFailure {
        /// Primary scenario failure.
        primary: String,
        /// Unmount and cleanup failure while tearing down.
        cleanup: String,
    },
    /// RustFS outage scenario failed and unmount also failed.
    #[error("RustFS/S3 outage scenario failed: {primary}; unmount also failed: {cleanup}")]
    RustfsOutageUnmountAfterFailure {
        /// Primary scenario failure.
        primary: String,
        /// Unmount and cleanup failure while tearing down.
        cleanup: String,
    },
    /// Crash/restart scenario failed and unmount also failed.
    #[error(
        "mounted client crash/restart scenario failed: {primary}; unmount also failed: {cleanup}"
    )]
    CrashRestartUnmountAfterFailure {
        /// Primary scenario failure.
        primary: String,
        /// Unmount and cleanup failure while tearing down.
        cleanup: String,
    },
    /// Mounted tree validation error.
    #[error("mounted tree validation failed: {0}")]
    MountedTree(
        /// Source mounted-tree validation error.
        #[from]
        MountedTreeError,
    ),
    /// Infrastructure-related failure.
    #[error("scenario infrastructure failed: {0}")]
    Infra(
        /// Source infrastructure error.
        #[from]
        InfraError,
    ),
    /// PostgreSQL outage scenario requires compose infrastructure.
    #[error("PostgreSQL outage scenario requires compose infrastructure")]
    MissingComposeInfrastructure,
    /// Scenario manifest path is missing.
    #[error("missing E2E run manifest: {path}")]
    MissingRunManifest {
        /// Path to the expected run manifest.
        path: PathBuf,
    },
    /// Outage operation rejected by scenario coordinator.
    #[error("PostgreSQL outage operations rejected {rejected} of {total} expected local writes")]
    RejectedOutageOperations {
        /// Number of rejected outage operations.
        rejected: usize,
        /// Total outage operations attempted.
        total: usize,
    },
    /// PostgreSQL outage writer recovery failed.
    #[error("PostgreSQL outage writer recovery failed for {client}:{path}: {detail}")]
    PostgresOutageWriterRecoveryFailed {
        /// Client that failed recovery.
        client: String,
        /// Recovered file path.
        path: String,
        /// Recovery failure details.
        detail: String,
    },
    /// Writer remount failed during outage recovery.
    #[error("PostgreSQL outage writer remount failed for {client}: {detail}")]
    PostgresOutageWriterRemountFailed {
        /// Client that could not remount.
        client: String,
        /// Remount failure details.
        detail: String,
    },
    /// Peer convergence failed after writer publication.
    #[error("PostgreSQL outage peer convergence failed after writer publication: {detail}")]
    PostgresOutagePeerConvergenceFailed {
        /// Peer convergence failure details.
        detail: String,
    },
    /// A peer observed outage write visibility too early.
    #[error("peer client observed the RustFS/S3 outage write before recovery: {checksum}")]
    PeerObservedOutageWrite {
        /// Observed checksum value.
        checksum: String,
    },
    /// Insufficient clients provided for smoke scenarios.
    #[error("mounted smoke scenario requires at least {expected} clients, got {actual}")]
    InsufficientMountedClients {
        /// Minimum number of clients required.
        expected: usize,
        /// Number of clients actually provided.
        actual: usize,
    },
    /// Insufficient clients provided for outage scenarios.
    #[error("PostgreSQL outage scenario requires at least {expected} clients, got {actual}")]
    InsufficientOutageClients {
        /// Minimum number of clients required.
        expected: usize,
        /// Number of clients actually provided.
        actual: usize,
    },
    /// Insufficient clients provided for crash/restart scenarios.
    #[error(
        "mounted client crash/restart scenario requires at least {expected} clients, got {actual}"
    )]
    InsufficientCrashRestartClients {
        /// Minimum number of clients required.
        expected: usize,
        /// Number of clients actually provided.
        actual: usize,
    },
    /// Writer publication failed.
    #[error("mounted client crash/restart publication failed for {path}: {detail}")]
    CrashRestartPublicationFailed {
        /// Recovered file path.
        path: String,
        /// Publication failure details.
        detail: String,
    },
    /// Peer convergence failed after crash/restart recovery.
    #[error("mounted client crash/restart peer convergence failed for {path}: {detail}")]
    CrashRestartPeerConvergenceFailed {
        /// Recovered file path.
        path: String,
        /// Peer convergence failure details.
        detail: String,
    },
    /// Crash/restart produced a corrupted advertised file checksum.
    #[error(
        "mounted client crash/restart corrupt advertised file for {client}:{path}: expected recovered checksum {expected_checksum}, got {actual_checksum}"
    )]
    CrashRestartCorruptAdvertisedFile {
        /// Client with the corrupted advertised file.
        client: String,
        /// Advertised file path.
        path: String,
        /// Expected checksum from the writer.
        expected_checksum: String,
        /// Observed checksum after recovery.
        actual_checksum: String,
    },
    /// Namespace collision scenarios require enough clients.
    #[error("namespace collision scenario requires at least {expected} clients, got {actual}")]
    InsufficientCollisionClients {
        /// Minimum number of clients required.
        expected: usize,
        /// Number of clients actually provided.
        actual: usize,
    },
    /// Namespace collision scenario failed and cleanup unmount also failed.
    #[error("namespace collision scenario failed: {primary}; unmount also failed: {cleanup}")]
    NamespaceCollisionUnmountAfterFailure {
        /// Primary scenario failure.
        primary: String,
        /// Unmount and cleanup failure while tearing down.
        cleanup: String,
    },
    /// Advanced mounted scenario failed and cleanup unmount also failed.
    #[error(
        "advanced mounted scenario {scenario} failed: {primary}; unmount also failed: {cleanup}"
    )]
    AdvancedMountedUnmountAfterFailure {
        /// Scenario name.
        scenario: String,
        /// Primary error detail.
        primary: String,
        /// Cleanup error detail.
        cleanup: String,
    },
    /// Namespace collision operations exceeded timeout.
    #[error("namespace collision operations timed out after {timeout_ms} ms; pending: {pending:?}")]
    NamespaceCollisionOperationTimeout {
        /// Timeout in milliseconds.
        timeout_ms: u128,
        /// Pending operations when timeout was hit.
        pending: Vec<String>,
    },
    /// Namespace invariant validation failed.
    #[error("namespace collision validation failed: {violations:?}")]
    NamespaceInvariantViolations {
        /// Set of invariant violations.
        violations: Vec<NamespaceInvariantViolation>,
    },
    /// Test-only replication failure emitted by namespace collision fixtures.
    #[cfg(test)]
    #[error("namespace collision recording replication failed: {errors:?}")]
    ReplicationFailure {
        /// Assertion error messages produced while validating the replication recording.
        errors: Vec<String>,
    },
}

/// Contract implemented by scenario implementations used by mounted E2E runs.
pub trait Scenario {
    /// Returns scenario definition metadata.
    fn definition(&self) -> ScenarioDefinition;
    /// Runs setup actions before scenario execution.
    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    /// Executes the scenario under test.
    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
    /// Validates final scenario behavior and returns warnings if any.
    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError>;
    /// Cleans scenario state and unmounts clients.
    fn cleanup(&self, context: &ScenarioContext) -> Result<(), ScenarioError>;
}

/// Returns the full registry of available scenario definitions.
pub fn available_scenarios() -> Vec<ScenarioDefinition> {
    vec![
        BasicMultiClientSmoke.definition(),
        PostgresOutageMultiClientRecovery.definition(),
        RustfsOutageMultiClientRecovery.definition(),
        MountedClientCrashRestartRecovery.definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::CrashBeforeWriteAcceptance).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::CrashAfterPartialWriteAcceptance)
            .definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::CrashAfterFullWriteAcceptance)
            .definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsRename).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsUnlink).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::UnlinkVsAppend).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::OverwriteVsRead).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::MkdirRmdirVsCreate).definition(),
        NamespaceCollisionScenario::new(NamespaceCollisionKind::ReaddirStatDuringMutation)
            .definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::MountedChaos).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::HotspotChurn).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::FaultChaos).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::LwwReconnectChaos).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::OpenHandle).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::SparseRange).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::ColdObserverLongHistory).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::StaleRemountColdDiscoverCatchup)
            .definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::InvalidationDropoutFallback).definition(),
        AdvancedMountedScenario::new(AdvancedMountedKind::SymlinkAttribute).definition(),
    ]
}

/// Resolves a scenario name to a concrete scenario implementation.
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
        MOUNTED_CRASH_BEFORE_WRITE_ACCEPTANCE
        | "crash-before-write-acceptance"
        | "publication-boundary-before-write" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::CrashBeforeWriteAcceptance,
        ))),
        MOUNTED_CRASH_AFTER_PARTIAL_WRITE_ACCEPTANCE
        | "crash-after-partial-write-acceptance"
        | "publication-boundary-partial-write" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::CrashAfterPartialWriteAcceptance,
        ))),
        MOUNTED_CRASH_AFTER_FULL_WRITE_ACCEPTANCE
        | "crash-after-full-write-acceptance"
        | "publication-boundary"
        | "publication-boundary-crash" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::CrashAfterFullWriteAcceptance,
        ))),
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
        MOUNTED_CHAOS_CONVERGENCE | "mounted-chaos" | "chaos" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::MountedChaos),
        )),
        MOUNTED_HOTSPOT_CHURN_CONVERGENCE | "hotspot-churn" | "hot-dir-churn" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::HotspotChurn),
        )),
        MOUNTED_FAULT_CHAOS_CONVERGENCE | "fault-chaos" | "chaos-faults" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::FaultChaos),
        )),
        MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE | "lww-chaos" | "reconnect-lww" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::LwwReconnectChaos),
        )),
        MOUNTED_OPEN_HANDLE_CONVERGENCE | "open-handle" | "open-unlink-rename" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::OpenHandle),
        )),
        MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE | "sparse-range" | "sparse-file-range" => {
            Some(Box::new(AdvancedMountedScenario::new(
                AdvancedMountedKind::SparseRange,
            )))
        }
        MOUNTED_COLD_OBSERVER_LONG_HISTORY_CONVERGENCE
        | "cold-observer"
        | "cold-observer-long-history" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::ColdObserverLongHistory,
        ))),
        MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE
        | "stale-remount"
        | "cold-discover-catchup" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
        ))),
        MOUNTED_INVALIDATION_DROPOUT_FALLBACK_CONVERGENCE
        | "invalidation-dropout"
        | "fallback-sync" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::InvalidationDropoutFallback,
        ))),
        MOUNTED_SYMLINK_ATTRIBUTE_CONVERGENCE | "symlink-attribute" | "symlink-attrs" => {
            Some(Box::new(AdvancedMountedScenario::new(
                AdvancedMountedKind::SymlinkAttribute,
            )))
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
    fn after_smoke_file_written(
        &mut self,
        _volume_name: &str,
        _smoke_file: &Path,
    ) -> Result<(), ClientControllerError> {
        Ok(())
    }
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
        driver.after_smoke_file_written(&volume.volume_name, &smoke_file)?;
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
    let convergence_clients =
        if writer_recovery_status == PostgresOutageWriterRecoveryStatus::LostAfterUncleanShutdown {
            &clients[1..POSTGRES_OUTAGE_CLIENT_NAMES.len()]
        } else {
            &clients[..POSTGRES_OUTAGE_CLIENT_NAMES.len()]
        };
    let mounts = convergence_clients
        .iter()
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
    let peer_observation_time_ms = elapsed_millis_nonzero(peer_observation_started);
    let final_validator = convergence.comparison;
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
    write_mounted_initial_file(
        source_file.clone(),
        RUSTFS_OUTAGE_INITIAL_PAYLOAD,
        "rustfs outage initial payload",
    )?;
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
    let local_read = wait_for_mounted_payload_readback(
        &source_file,
        RUSTFS_OUTAGE_RECOVERED_PAYLOAD,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
    .map_err(scenario_error_to_io)?;
    let local_read_checksum_sha256 = bytes_sha256(&local_read);
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
    wait_for_mounted_initial_payloads_until(
        &clients[..RUSTFS_OUTAGE_CLIENT_NAMES.len()],
        &[(relative_path, RUSTFS_OUTAGE_RECOVERED_PAYLOAD)],
        Instant::now() + convergence_timeout,
        poll_interval,
        "rustfs outage recovered payload",
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
    fn before_peer_convergence_wait(
        &mut self,
        _client: &str,
        _relative_path: &str,
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

    driver.before_peer_convergence_wait(&crashed.name, relative_path)?;

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
            if mode != CrashRestartMode::BeforeRemotePublicationCompletes
                && let Some(final_checksum_sha256) = read_peer_checksum(&clients[1], relative_path)?
            {
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
    fn emergency_kill_all_no_wait(&mut self, context: &str) -> Result<(), ScenarioError>;
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

    fn emergency_kill_all_no_wait(&mut self, context: &str) -> Result<(), ScenarioError> {
        Ok(self.0.emergency_kill_all_no_wait(context)?)
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
    completion_grace: Duration,
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
            completion_grace: Duration::ZERO,
        }
    }

    #[cfg(test)]
    fn with_completion_grace(mut self, completion_grace: Duration) -> Self {
        self.completion_grace = completion_grace;
        self
    }
}

struct NamespaceCollisionHelperSpec {
    order: usize,
    client: ClientSpec,
    operation: &'static str,
    helper_operation: &'static str,
    path: &'static str,
    target: Option<&'static str>,
    payload: Option<&'static [u8]>,
}

fn namespace_collision_helper_task(
    context: &ScenarioContext,
    spec: NamespaceCollisionHelperSpec,
) -> NamespaceCollisionTask {
    let context = context.clone();
    NamespaceCollisionTask::new(
        spec.order,
        spec.client,
        spec.operation,
        spec.path,
        move |client| {
            let pending_label = match spec.target {
                Some(target) => {
                    format!(
                        "{} {} {} -> {target}",
                        client.name, spec.operation, spec.path
                    )
                }
                None => format!("{} {} {}", client.name, spec.operation, spec.path),
            };
            run_hotspot_helper_process(
                &context,
                COLLISION_OPERATION_TIMEOUT,
                client,
                spec.helper_operation,
                spec.path,
                spec.target,
                spec.payload,
                pending_label,
            )
            .map_err(scenario_error_to_io)
        },
    )
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
    let initial_validation_deadline = Instant::now() + convergence_timeout;
    let initial_paths = kind.initial_paths();
    wait_for_mounted_initial_payloads_until(
        &clients,
        &initial_paths,
        initial_validation_deadline,
        poll_interval,
        "namespace collision initial tree",
    )?;
    let required_initial_paths = collision_required_initial_paths(kind);
    if !required_initial_paths.is_empty() {
        wait_for_required_paths_visible_until(
            &clients,
            &required_initial_paths,
            initial_validation_deadline,
            poll_interval,
            "namespace collision initial tree",
        )?;
    }
    let initial_validation_timeout = remaining_initial_validation_timeout(
        initial_validation_deadline,
        "namespace collision initial tree",
    )?;
    let initial_mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &initial_mounts,
        context.scenario_dir.join("initial-validation"),
        initial_validation_timeout,
        poll_interval,
    )?;
    let operations = match perform_namespace_collision_operations(context, kind, &clients) {
        Ok(operations) => operations,
        Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => {
            return handle_namespace_collision_operation_timeout(error, driver);
        }
        Err(error) => return Err(error),
    };
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

fn handle_namespace_collision_operation_timeout<T>(
    error: ScenarioError,
    driver: &mut dyn NamespaceCollisionDriver,
) -> Result<T, ScenarioError> {
    match driver.emergency_kill_all_no_wait("namespace collision operation timeout") {
        Ok(()) => Err(error),
        Err(cleanup) => Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
            primary: error.to_string(),
            cleanup: cleanup.to_string(),
        }),
    }
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
        write_mounted_initial_file(
            client.mount_point.join(relative_path),
            payload,
            "namespace collision initial tree",
        )?;
    }
    Ok(())
}

fn collision_required_initial_paths(kind: NamespaceCollisionKind) -> Vec<&'static str> {
    if kind == NamespaceCollisionKind::MkdirRmdirVsCreate {
        vec!["race"]
    } else {
        Vec::new()
    }
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
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[0].clone(),
                        operation: "rename",
                        helper_operation: "rename",
                        path: "shared/file.txt",
                        target: Some("shared/renamed-a.txt"),
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[1].clone(),
                        operation: "rename",
                        helper_operation: "rename",
                        path: "shared/file.txt",
                        target: Some("shared/renamed-b.txt"),
                        payload: None,
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::RenameVsUnlink => run_concurrent_namespace_operations(
            started,
            vec![
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[0].clone(),
                        operation: "rename",
                        helper_operation: "rename",
                        path: "shared/file.txt",
                        target: Some("shared/renamed.txt"),
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[1].clone(),
                        operation: "unlink",
                        helper_operation: "delete",
                        path: "shared/file.txt",
                        target: None,
                        payload: None,
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::UnlinkVsAppend => run_concurrent_namespace_operations(
            started,
            vec![
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[0].clone(),
                        operation: "unlink",
                        helper_operation: "delete",
                        path: "shared/data.txt",
                        target: None,
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[1].clone(),
                        operation: "append",
                        helper_operation: "append",
                        path: "shared/data.txt",
                        target: None,
                        payload: Some(b"appended payload\n"),
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
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
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[0].clone(),
                        operation: "overwrite",
                        helper_operation: "overwrite-barrier",
                        path: "shared/data.txt",
                        target: None,
                        payload: Some(COLLISION_NEW_COMMITTED_PAYLOAD),
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
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::MkdirRmdirVsCreate => run_concurrent_namespace_operations(
            started,
            vec![
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 1,
                        client: clients[0].clone(),
                        operation: "mkdir",
                        helper_operation: "mkdir",
                        path: "race/subdir",
                        target: None,
                        payload: None,
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[1].clone(),
                        operation: "create",
                        helper_operation: "create",
                        path: "race/child.txt",
                        target: None,
                        payload: Some(b"child\n"),
                    },
                ),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 3,
                        client: clients[2].clone(),
                        operation: "rmdir",
                        helper_operation: "rmdir",
                        path: "race",
                        target: None,
                        payload: None,
                    },
                ),
            ],
            COLLISION_OPERATION_TIMEOUT,
        ),
        NamespaceCollisionKind::ReaddirStatDuringMutation => run_concurrent_namespace_operations(
            started,
            vec![
                NamespaceCollisionTask::new(1, clients[1].clone(), "readdir", "scan", |client| {
                    fs::read_dir(client.mount_point.join("scan")).map(|_| ())
                }),
                namespace_collision_helper_task(
                    context,
                    NamespaceCollisionHelperSpec {
                        order: 2,
                        client: clients[0].clone(),
                        operation: "rename",
                        helper_operation: "rename",
                        path: "scan/a.txt",
                        target: Some("scan/c.txt"),
                        payload: None,
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
            COLLISION_OPERATION_TIMEOUT,
        ),
    }?;
    write_namespace_collision_operations(context, &operations)?;
    Ok(operations)
}

fn run_concurrent_namespace_operations(
    started: Instant,
    tasks: Vec<NamespaceCollisionTask>,
    timeout: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    if tasks.is_empty() {
        return Ok(Vec::new());
    }
    let barrier = Arc::new(Barrier::new(tasks.len()));
    let (tx, rx) = mpsc::channel();
    let mut pending = tasks
        .iter()
        .map(|task| (task.order, namespace_collision_task_label(task)))
        .collect::<Vec<_>>();
    let expected_operations = pending.len();
    let completion_grace = tasks
        .iter()
        .map(|task| task.completion_grace)
        .max()
        .unwrap_or(Duration::ZERO);
    let mut handles = Vec::with_capacity(tasks.len());
    for task in tasks {
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let operation = record_fs_operation(
                started,
                task.order,
                &task.client,
                task.operation,
                task.path,
                || (task.action)(&task.client),
            );
            let _ = tx.send(operation);
        }));
    }
    drop(tx);

    let deadline = Instant::now() + timeout + completion_grace;
    let mut operations = Vec::with_capacity(expected_operations);
    while operations.len() < expected_operations {
        let now = Instant::now();
        if now >= deadline {
            return Err(namespace_collision_operation_timeout(timeout, pending));
        }
        match rx.recv_timeout(deadline.saturating_duration_since(now)) {
            Ok(operation) => {
                pending.retain(|(order, _)| *order != operation.order);
                operations.push(operation);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(namespace_collision_operation_timeout(timeout, pending));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(ScenarioError::Io(std::io::Error::other(
                    "namespace collision worker channel closed before all operations completed",
                )));
            }
        }
    }

    for handle in handles {
        handle
            .join()
            .expect("namespace collision worker thread should not panic");
    }
    operations.sort_by_key(|operation| operation.order);
    Ok(operations)
}

fn namespace_collision_operation_timeout(
    timeout: Duration,
    pending: Vec<(usize, String)>,
) -> ScenarioError {
    ScenarioError::NamespaceCollisionOperationTimeout {
        timeout_ms: timeout.as_millis(),
        pending: pending.into_iter().map(|(_, label)| label).collect(),
    }
}

fn namespace_collision_task_label(task: &NamespaceCollisionTask) -> String {
    format!("{} {} {}", task.client.name, task.operation, task.path)
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

fn append_namespace_collision_operation(
    context: &ScenarioContext,
    operation: &NamespaceCollisionOperation,
) -> Result<(), ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    serde_json::to_writer(&mut log, operation)?;
    log.write_all(b"\n")?;
    Ok(())
}

fn record_sparse_operation(
    context: &ScenarioContext,
    operations: &mut Vec<NamespaceCollisionOperation>,
    operation: NamespaceCollisionOperation,
) -> Result<(), ScenarioError> {
    append_namespace_collision_operation(context, &operation)?;
    operations.push(operation);
    Ok(())
}

#[cfg(test)]
fn record_required_sparse_operation(
    context: &ScenarioContext,
    operations: &mut Vec<NamespaceCollisionOperation>,
    operation: NamespaceCollisionOperation,
) -> Result<(), ScenarioError> {
    let required = required_record(operation.clone()).map(|_| ());
    record_sparse_operation(context, operations, operation)?;
    required
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScenarioIsolation {
    root: String,
    payload_marker: String,
}

impl ScenarioIsolation {
    fn for_advanced(
        context: &ScenarioContext,
        kind: AdvancedMountedKind,
        volume_name: &str,
    ) -> Self {
        let marker_input = format!(
            "{}:{}:{}:{}",
            context.run_id,
            context.seed,
            kind.name(),
            volume_name
        );
        let marker_hash = bytes_sha256(marker_input.as_bytes());
        let nonce = &marker_hash[..16];
        Self {
            root: format!("{}-{nonce}", kind.path_root()),
            payload_marker: format!("studiofs-e2e:{marker_input}:{nonce}"),
        }
    }

    fn path(&self, relative_path: &str) -> String {
        let relative_path = relative_path.trim_matches('/');
        if relative_path.is_empty() {
            self.root.clone()
        } else {
            format!("{}/{}", self.root, relative_path)
        }
    }

    fn payload(&self, label: &str, logical_path: &str, body: &str) -> Vec<u8> {
        format!(
            "{}\nlabel={label}\npath={logical_path}\n{body}",
            self.payload_marker
        )
        .into_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvancedMountedKind {
    CrashBeforeWriteAcceptance,
    CrashAfterPartialWriteAcceptance,
    CrashAfterFullWriteAcceptance,
    MountedChaos,
    HotspotChurn,
    FaultChaos,
    LwwReconnectChaos,
    OpenHandle,
    SparseRange,
    ColdObserverLongHistory,
    StaleRemountColdDiscoverCatchup,
    InvalidationDropoutFallback,
    SymlinkAttribute,
}

impl AdvancedMountedKind {
    fn name(self) -> &'static str {
        match self {
            Self::CrashBeforeWriteAcceptance => MOUNTED_CRASH_BEFORE_WRITE_ACCEPTANCE,
            Self::CrashAfterPartialWriteAcceptance => MOUNTED_CRASH_AFTER_PARTIAL_WRITE_ACCEPTANCE,
            Self::CrashAfterFullWriteAcceptance => MOUNTED_CRASH_AFTER_FULL_WRITE_ACCEPTANCE,
            Self::MountedChaos => MOUNTED_CHAOS_CONVERGENCE,
            Self::HotspotChurn => MOUNTED_HOTSPOT_CHURN_CONVERGENCE,
            Self::FaultChaos => MOUNTED_FAULT_CHAOS_CONVERGENCE,
            Self::LwwReconnectChaos => MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE,
            Self::OpenHandle => MOUNTED_OPEN_HANDLE_CONVERGENCE,
            Self::SparseRange => MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE,
            Self::ColdObserverLongHistory => MOUNTED_COLD_OBSERVER_LONG_HISTORY_CONVERGENCE,
            Self::StaleRemountColdDiscoverCatchup => {
                MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE
            }
            Self::InvalidationDropoutFallback => MOUNTED_INVALIDATION_DROPOUT_FALLBACK_CONVERGENCE,
            Self::SymlinkAttribute => MOUNTED_SYMLINK_ATTRIBUTE_CONVERGENCE,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::CrashBeforeWriteAcceptance => "mounted crash before write acceptance recovery",
            Self::CrashAfterPartialWriteAcceptance => {
                "mounted crash after partial write acceptance recovery"
            }
            Self::CrashAfterFullWriteAcceptance => {
                "mounted crash after full write acceptance recovery"
            }
            Self::MountedChaos => "seeded mounted namespace chaos convergence",
            Self::HotspotChurn => "mounted hot-spot churn convergence",
            Self::FaultChaos => "mounted fault chaos convergence",
            Self::LwwReconnectChaos => "mounted LWW reconnect chaos convergence",
            Self::OpenHandle => "mounted open-handle convergence",
            Self::SparseRange => "mounted sparse-file range convergence",
            Self::ColdObserverLongHistory => "mounted cold-observer long-history convergence",
            Self::StaleRemountColdDiscoverCatchup => {
                "mounted stale-remount and cold-discover catch-up convergence"
            }
            Self::InvalidationDropoutFallback => {
                "mounted invalidation-dropout fallback-sync convergence"
            }
            Self::SymlinkAttribute => "mounted symlink and metadata-attribute convergence",
        }
    }

    fn path_root(self) -> &'static str {
        match self {
            Self::CrashBeforeWriteAcceptance => "crash-before-write-acceptance",
            Self::CrashAfterPartialWriteAcceptance => "crash-after-partial-write-acceptance",
            Self::CrashAfterFullWriteAcceptance => "crash-after-full-write-acceptance",
            Self::MountedChaos => "chaos",
            Self::HotspotChurn => "hotspot",
            Self::FaultChaos => "fault-chaos",
            Self::LwwReconnectChaos => "lww-chaos",
            Self::OpenHandle => "open-handles",
            Self::SparseRange => "sparse",
            Self::ColdObserverLongHistory => "cold-observer",
            Self::StaleRemountColdDiscoverCatchup => "stale-remount-cold-discover",
            Self::InvalidationDropoutFallback => "invalidation-dropout",
            Self::SymlinkAttribute => "symlink-attribute",
        }
    }

    fn summary_file(self) -> &'static str {
        match self {
            Self::CrashBeforeWriteAcceptance
            | Self::CrashAfterPartialWriteAcceptance
            | Self::CrashAfterFullWriteAcceptance => "write-acceptance-crash-summary.json",
            Self::MountedChaos => "mounted-chaos-summary.json",
            Self::HotspotChurn => "mounted-hotspot-churn-summary.json",
            Self::FaultChaos => "mounted-fault-chaos-summary.json",
            Self::LwwReconnectChaos => "mounted-lww-reconnect-chaos-summary.json",
            Self::OpenHandle => "mounted-open-handle-summary.json",
            Self::SparseRange => "sparse-range-summary.json",
            Self::ColdObserverLongHistory => "cold-observer-long-history-summary.json",
            Self::StaleRemountColdDiscoverCatchup => "stale-remount-cold-discover-summary.json",
            Self::InvalidationDropoutFallback => "invalidation-dropout-summary.json",
            Self::SymlinkAttribute => "symlink-attribute-summary.json",
        }
    }

    fn client_names(self) -> &'static [&'static str] {
        match self {
            Self::FaultChaos => FAULT_CHAOS_CLIENT_NAMES,
            Self::LwwReconnectChaos => LWW_RECONNECT_CLIENT_NAMES,
            Self::InvalidationDropoutFallback => INVALIDATION_DROPOUT_CLIENT_NAMES,
            Self::SymlinkAttribute => SYMLINK_ATTRIBUTE_CLIENT_NAMES,
            Self::ColdObserverLongHistory | Self::StaleRemountColdDiscoverCatchup => {
                COLD_OBSERVER_CLIENT_NAMES
            }
            _ => ADVANCED_MOUNTED_CLIENT_NAMES,
        }
    }

    fn convergence_timeout(self) -> Duration {
        match self {
            Self::FaultChaos => FAULT_CHAOS_CONVERGENCE_TIMEOUT,
            Self::LwwReconnectChaos => LWW_RECONNECT_CONVERGENCE_TIMEOUT,
            _ => ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AdvancedMountedScenario {
    kind: AdvancedMountedKind,
}

impl AdvancedMountedScenario {
    fn new(kind: AdvancedMountedKind) -> Self {
        Self { kind }
    }
}

impl Scenario for AdvancedMountedScenario {
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
            let mut controller =
                MultiClientController::new(config.clone(), SystemClientProcessExecutor);
            run_advanced_mounted_with_controller(context, self.kind, &mut controller)?;
        } else {
            write_advanced_mounted_plan_artifacts(context, self.kind)?;
        }
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context.scenario_dir.join(self.kind.summary_file());
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: AdvancedMountedSummary = serde_json::from_slice(&fs::read(&summary_path)?)?;
        let lww_oracle_matches = summary
            .lww_oracle
            .as_ref()
            .map(|oracle| oracle.matched)
            .unwrap_or(true);
        let long_history_matches = summary
            .long_history_validation
            .as_ref()
            .map(|validation| validation.matched)
            .unwrap_or_else(|| summary.long_history.is_none());
        let invalidation_dropout_matches = summary
            .invalidation_dropout
            .as_ref()
            .map(InvalidationDropoutSummary::matched)
            .unwrap_or(self.kind != AdvancedMountedKind::InvalidationDropoutFallback);
        let validation_passed = summary.final_validator.converged
            && summary.namespace_violations.is_empty()
            && lww_oracle_matches
            && long_history_matches
            && invalidation_dropout_matches
            && sparse_ranges_match(self.kind, summary.sparse_ranges.as_ref());
        let validation = json!({
            "scenario": self.kind.name(),
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if validation_passed { "passed" } else { "failed" },
            "final_validator": summary.final_validator,
            "namespace_violations": summary.namespace_violations,
            "operation_log": summary.operation_log,
            "lww_oracle": summary.lww_oracle,
            "sparse_ranges": summary.sparse_ranges,
            "cold_observer": summary.cold_observer,
            "long_history": summary.long_history,
            "long_history_validation": summary.long_history_validation,
            "catchup": summary.catchup,
            "invalidation_dropout": summary.invalidation_dropout,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        let mut artifacts = vec![
            format!(
                "artifacts/scenarios/{}/{}",
                self.kind.name(),
                self.kind.summary_file()
            ),
            format!("artifacts/scenarios/{}/validation.json", self.kind.name()),
        ];
        if self.kind == AdvancedMountedKind::LwwReconnectChaos {
            artifacts.push(format!(
                "artifacts/scenarios/{}/lww-oracle.json",
                self.kind.name()
            ));
        }
        if self.kind == AdvancedMountedKind::ColdObserverLongHistory {
            artifacts.push(format!(
                "artifacts/scenarios/{}/cold-observer-diagnostics.json",
                self.kind.name()
            ));
        }
        if self.kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup {
            artifacts.push(format!(
                "artifacts/scenarios/{}/catchup-diagnostics.json",
                self.kind.name()
            ));
        }
        if self.kind == AdvancedMountedKind::InvalidationDropoutFallback {
            artifacts.push(format!(
                "artifacts/scenarios/{}/invalidation-fallback-diagnostics.json",
                self.kind.name()
            ));
        }
        if !validation_passed {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "{} validation failed; see {}",
                self.kind.name(),
                context.scenario_dir.join("validation.json").display()
            ))));
        }
        Ok(artifacts)
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LwwReconnectOracle {
    expected_payload_sha256: String,
    actual_payload_sha256: String,
    matched: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SparseExpectedRange {
    label: String,
    offset: u64,
    len: usize,
    expected_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SparseHoleProbe {
    client: String,
    offset: u64,
    len: usize,
    all_zero: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SparseReadProbe {
    client: String,
    offset: u64,
    len: usize,
    actual_checksum_sha256: String,
    matched: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SparseRangeSummary {
    relative_path: String,
    logical_size: u64,
    final_checksum_sha256: String,
    expected_byte_ranges: Vec<SparseExpectedRange>,
    zero_filled_holes: Vec<SparseHoleProbe>,
    read_probes: Vec<SparseReadProbe>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ColdObserverSummary {
    client: String,
    local_metadata_db: PathBuf,
    cache_dir: PathBuf,
    discovered_after_history: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LongHistorySummary {
    bounded_operation_count: usize,
    expected_present_paths: Vec<String>,
    expected_absent_paths: Vec<String>,
    final_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LongHistoryValidation {
    matched: bool,
    missing_present_paths: Vec<String>,
    unexpected_present_paths: Vec<String>,
    checksum_mismatches: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CatchupSummary {
    warm_writers: Vec<String>,
    stale_remount_client: String,
    cold_discover_client: String,
    stale_remount_after_history: bool,
    cold_discover_after_history: bool,
    final_validation_result: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct InvalidationDropoutSummary {
    observer_client: String,
    writer_clients: Vec<String>,
    notify_suppressed: bool,
    fallback_interval_ms: u64,
    stale_window_ms: u64,
    stale_window_observed: bool,
    delete_stale_window_observed: bool,
    fallback_resumed: bool,
    final_observer_path: String,
    final_observer_checksum_sha256: String,
    final_validation_result: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SymlinkAttributeSummary {
    symlink_supported: bool,
    symlink_skip_reason: Option<String>,
    readonly_attribute_supported: bool,
    readonly_attribute_skip_reason: Option<String>,
    final_symlink_path: String,
    final_symlink_target: String,
    readonly_path: String,
    readonly_observed_clients: Vec<String>,
    final_validation_result: String,
}

impl InvalidationDropoutSummary {
    fn matched(&self) -> bool {
        self.notify_suppressed
            && self.stale_window_observed
            && self.delete_stale_window_observed
            && self.fallback_resumed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AdvancedMountedSummary {
    scenario: String,
    bootstrap: Option<VolumeBootstrapSummary>,
    clients: usize,
    operations: Vec<NamespaceCollisionOperation>,
    operation_log: String,
    final_validator: TreeComparison,
    namespace_violations: Vec<NamespaceInvariantViolation>,
    lww_oracle: Option<LwwReconnectOracle>,
    sparse_ranges: Option<SparseRangeSummary>,
    #[serde(default)]
    cold_observer: Option<ColdObserverSummary>,
    #[serde(default)]
    long_history: Option<LongHistorySummary>,
    #[serde(default)]
    long_history_validation: Option<LongHistoryValidation>,
    #[serde(default)]
    catchup: Option<CatchupSummary>,
    #[serde(default)]
    invalidation_dropout: Option<InvalidationDropoutSummary>,
    #[serde(default)]
    symlink_attributes: Option<SymlinkAttributeSummary>,
}

struct AdvancedMountedExecution {
    operations: Vec<NamespaceCollisionOperation>,
    lww_oracle: Option<LwwReconnectOracle>,
    sparse_ranges: Option<SparseRangeSummary>,
    cold_observer: Option<ColdObserverSummary>,
    long_history: Option<LongHistorySummary>,
    catchup: Option<CatchupSummary>,
    invalidation_dropout: Option<InvalidationDropoutSummary>,
    symlink_attributes: Option<SymlinkAttributeSummary>,
}

impl LongHistoryValidation {
    fn matched() -> Self {
        Self {
            matched: true,
            missing_present_paths: Vec::new(),
            unexpected_present_paths: Vec::new(),
            checksum_mismatches: Vec::new(),
        }
    }
}

fn run_advanced_mounted_with_controller(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    let result = run_advanced_mounted_body(context, kind, controller);
    if let Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) = result {
        if let Err(cleanup) = controller.emergency_kill_all_no_wait("advanced mounted timeout") {
            return Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
                scenario: kind.name().to_string(),
                primary: error.to_string(),
                cleanup: cleanup.to_string(),
            });
        }
        return Err(error);
    }

    let unmount_result = controller.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
            scenario: kind.name().to_string(),
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

fn run_advanced_mounted_body(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    if kind == AdvancedMountedKind::ColdObserverLongHistory {
        return run_cold_observer_long_history_body(context, controller);
    }
    if kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup {
        return run_stale_remount_cold_discover_body(context, controller);
    }

    let bootstrap = controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: kind
            .client_names()
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    if kind == AdvancedMountedKind::InvalidationDropoutFallback {
        controller.set_client_env_overrides(
            INVALIDATION_DROPOUT_OBSERVER_CLIENT,
            invalidation_dropout_observer_env_overrides(),
        )?;
    }
    controller.mount_all()?;
    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    if clients.len() < kind.client_names().len() {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: kind.client_names().len(),
            actual: clients.len(),
        });
    }

    let isolation = ScenarioIsolation::for_advanced(context, kind, &bootstrap.volume_name);
    let execution =
        execute_advanced_mounted_operations(context, kind, controller, &clients, &isolation)?;
    if kind != AdvancedMountedKind::SparseRange {
        write_namespace_collision_operations(context, &execution.operations)?;
    }
    let summary = finish_advanced_mounted(
        context,
        kind,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations: execution.operations,
            lww_oracle: execution.lww_oracle,
            sparse_ranges: execution.sparse_ranges,
            cold_observer: execution.cold_observer,
            long_history: execution.long_history,
            catchup: execution.catchup,
            invalidation_dropout: execution.invalidation_dropout,
            symlink_attributes: execution.symlink_attributes,
        },
    )?;
    Ok(summary)
}

fn write_advanced_mounted_plan_artifacts(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let operations = planned_advanced_mounted_operations(kind, started);
    write_namespace_collision_operations(context, &operations)?;
    let lww_oracle = (kind == AdvancedMountedKind::LwwReconnectChaos).then(|| {
        let checksum = bytes_sha256(b"planned lww reconnect winner\n");
        LwwReconnectOracle {
            expected_payload_sha256: checksum.clone(),
            actual_payload_sha256: checksum,
            matched: true,
        }
    });
    let sparse_ranges =
        (kind == AdvancedMountedKind::SparseRange).then(planned_sparse_range_summary);
    if let Some(oracle) = &lww_oracle {
        write_lww_oracle(context, oracle)?;
    }
    if kind == AdvancedMountedKind::ColdObserverLongHistory {
        write_planned_cold_observer_diagnostics(context)?;
    }
    if kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup {
        write_planned_catchup_diagnostics(context)?;
    }
    if kind == AdvancedMountedKind::InvalidationDropoutFallback {
        write_planned_invalidation_dropout_diagnostics(
            context,
            &planned_invalidation_dropout_summary(),
        )?;
    }
    let compared_clients = kind
        .client_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let summary = AdvancedMountedSummary {
        scenario: kind.name().to_string(),
        bootstrap: None,
        clients: kind.client_names().len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: TreeComparison {
            converged: true,
            reference_client: compared_clients.first().cloned(),
            compared_clients,
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
        lww_oracle,
        sparse_ranges,
        cold_observer: (kind == AdvancedMountedKind::ColdObserverLongHistory)
            .then(planned_cold_observer_summary),
        long_history: match kind {
            AdvancedMountedKind::ColdObserverLongHistory => Some(planned_long_history_summary()),
            AdvancedMountedKind::StaleRemountColdDiscoverCatchup => {
                Some(planned_stale_remount_long_history_summary())
            }
            _ => None,
        },
        long_history_validation: match kind {
            AdvancedMountedKind::ColdObserverLongHistory
            | AdvancedMountedKind::StaleRemountColdDiscoverCatchup => {
                Some(LongHistoryValidation::matched())
            }
            _ => None,
        },
        catchup: (kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup)
            .then(planned_catchup_summary),
        invalidation_dropout: (kind == AdvancedMountedKind::InvalidationDropoutFallback)
            .then(planned_invalidation_dropout_summary),
        symlink_attributes: (kind == AdvancedMountedKind::SymlinkAttribute)
            .then(planned_symlink_attribute_summary),
    };
    write_advanced_mounted_summary(context, kind, &summary)?;
    Ok(summary)
}

fn execute_advanced_mounted_operations(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<AdvancedMountedExecution, ScenarioError> {
    match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance
        | AdvancedMountedKind::CrashAfterPartialWriteAcceptance
        | AdvancedMountedKind::CrashAfterFullWriteAcceptance => Ok(AdvancedMountedExecution {
            operations: run_write_acceptance_crash_operations(
                kind, controller, clients, isolation,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::MountedChaos => Ok(AdvancedMountedExecution {
            operations: run_mounted_chaos_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::HotspotChurn => Ok(AdvancedMountedExecution {
            operations: run_hotspot_churn_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::FaultChaos => Ok(AdvancedMountedExecution {
            operations: run_fault_chaos_operations(context, controller, clients, isolation)?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::LwwReconnectChaos => {
            let (operations, oracle) =
                run_lww_reconnect_operations(context, controller, clients, isolation)?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: Some(oracle),
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::OpenHandle => Ok(AdvancedMountedExecution {
            operations: run_open_handle_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::SparseRange => {
            let (operations, summary) = run_sparse_range_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: Some(summary),
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::InvalidationDropoutFallback => {
            let (operations, summary) = run_invalidation_dropout_fallback_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: Some(summary),
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::SymlinkAttribute => {
            let (operations, summary) = run_symlink_attribute_operations(
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: Some(summary),
            })
        }
        AdvancedMountedKind::ColdObserverLongHistory => unreachable!(
            "cold-observer long-history uses delayed discovery and is handled before common bootstrap"
        ),
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup => unreachable!(
            "stale-remount cold-discover catch-up uses delayed remount/discovery and is handled before common bootstrap"
        ),
    }
}

fn run_cold_observer_long_history_body(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    controller.create_clients(COLD_OBSERVER_CLIENT_NAMES.iter().copied())?;
    controller.init_first_client()?;
    for client in &COLD_OBSERVER_WARM_CLIENT_NAMES[1..] {
        controller.discover(client)?;
    }
    for client in COLD_OBSERVER_WARM_CLIENT_NAMES {
        controller.mount(client)?;
    }

    let warm_clients = controller
        .clients()
        .into_iter()
        .filter(|client| COLD_OBSERVER_WARM_CLIENT_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let bootstrap = VolumeBootstrapSummary {
        volume_name: warm_clients
            .first()
            .map(|client| client.volume_name.clone())
            .unwrap_or_else(|| "projectfs".to_string()),
        init_client: COLD_OBSERVER_WARM_CLIENT_NAMES[0].to_string(),
        discovered_clients: COLD_OBSERVER_WARM_CLIENT_NAMES[1..]
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        clients: warm_clients
            .iter()
            .map(|client| crate::client::ClientBootstrapRecord {
                name: client.name.clone(),
                local_metadata_db: client.local_metadata_db.clone(),
                state: client.state,
            })
            .collect(),
    };
    write_bootstrap_summary(context, &bootstrap)?;

    let isolation = ScenarioIsolation::for_advanced(
        context,
        AdvancedMountedKind::ColdObserverLongHistory,
        &bootstrap.volume_name,
    );
    let started = Instant::now();
    let (mut operations, mut long_history) =
        run_cold_observer_warm_history(started, context, &warm_clients, &isolation)?;

    controller.discover("client-d")?;
    let cold_client = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-d")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold observer client-d")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_client,
        "cold_discover_after_history",
        "client-d",
        Ok(()),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_client,
        "cold_mount_after_history",
        "client-d",
        controller.mount("client-d").map_err(ScenarioError::from),
    )?);
    long_history.bounded_operation_count = operations.len();
    write_namespace_collision_operations(context, &operations)?;
    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let cold = clients
        .iter()
        .find(|client| client.name == "client-d")
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold observer client-d")))?;
    let cold_observer = ColdObserverSummary {
        client: cold.name.clone(),
        local_metadata_db: cold.local_metadata_db.clone(),
        cache_dir: cold.cache_dir.clone(),
        discovered_after_history: true,
    };

    finish_advanced_mounted(
        context,
        AdvancedMountedKind::ColdObserverLongHistory,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: Some(cold_observer),
            long_history: Some(long_history),
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        },
    )
}

fn run_stale_remount_cold_discover_body(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    controller.create_clients(COLD_OBSERVER_CLIENT_NAMES.iter().copied())?;
    controller.init_first_client()?;
    for client in &STALE_REMOUNT_INITIAL_CLIENT_NAMES[1..] {
        controller.discover(client)?;
    }
    for client in STALE_REMOUNT_INITIAL_CLIENT_NAMES {
        controller.mount(client)?;
    }

    let initial_clients = controller
        .clients()
        .into_iter()
        .filter(|client| STALE_REMOUNT_INITIAL_CLIENT_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let bootstrap = VolumeBootstrapSummary {
        volume_name: initial_clients
            .first()
            .map(|client| client.volume_name.clone())
            .unwrap_or_else(|| "projectfs".to_string()),
        init_client: "client-a".to_string(),
        discovered_clients: vec!["client-b".to_string(), "client-c".to_string()],
        clients: initial_clients
            .iter()
            .map(|client| crate::client::ClientBootstrapRecord {
                name: client.name.clone(),
                local_metadata_db: client.local_metadata_db.clone(),
                state: client.state,
            })
            .collect(),
    };
    write_bootstrap_summary(context, &bootstrap)?;

    let isolation = ScenarioIsolation::for_advanced(
        context,
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
        &bootstrap.volume_name,
    );
    let started = Instant::now();
    let mut operations = Vec::new();
    let stale_client = initial_clients
        .iter()
        .find(|client| client.name == "client-c")
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing stale remount client-c")))?;
    let baseline_path = isolation.path("baseline.txt");
    let baseline_payload = isolation.payload("initial_baseline", &baseline_path, "baseline");
    operations.push(timed_cold_observer_helper_fs_op(
        context,
        started,
        1,
        stale_client,
        "initial_baseline",
        "create",
        &baseline_path,
        &baseline_path,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&baseline_payload),
        None,
    )?);
    write_namespace_collision_operations(context, &operations)?;
    wait_for_mounted_initial_payloads_until(
        &initial_clients,
        &[(&baseline_path, baseline_payload.as_slice())],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "stale remount initial baseline",
    )?;
    operations.push(required_op(
        started,
        2,
        stale_client,
        "stale_client_unmount",
        "client-c",
        controller.unmount("client-c").map_err(ScenarioError::from),
    )?);
    write_namespace_collision_operations(context, &operations)?;

    let warm_clients = controller
        .clients()
        .into_iter()
        .filter(|client| STALE_REMOUNT_WARM_WRITER_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let (mut warm_history, long_history) = run_stale_remount_warm_history(
        context,
        started,
        &warm_clients,
        &isolation,
        operations.len() + 1,
        &operations,
    )?;
    operations.append(&mut warm_history);
    write_namespace_collision_operations(context, &operations)?;

    let stale_after_history = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-c")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing stale remount client-c")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &stale_after_history,
        "stale_remount_after_history",
        "client-c",
        controller.remount("client-c").map_err(ScenarioError::from),
    )?);
    controller.discover("client-d")?;
    let cold_after_discover = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-d")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold discover client-d")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_after_discover,
        "cold_discover_after_history",
        "client-d",
        controller.mount("client-d").map_err(ScenarioError::from),
    )?);
    write_namespace_collision_operations(context, &operations)?;

    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let catchup = CatchupSummary {
        warm_writers: STALE_REMOUNT_WARM_WRITER_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        stale_remount_client: "client-c".to_string(),
        cold_discover_client: "client-d".to_string(),
        stale_remount_after_history: true,
        cold_discover_after_history: true,
        final_validation_result: "mounted_tree_convergence".to_string(),
    };
    write_catchup_diagnostics(context, &catchup)?;
    finish_advanced_mounted(
        context,
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: Some(long_history),
            catchup: Some(catchup),
            invalidation_dropout: None,
            symlink_attributes: None,
        },
    )
}

fn run_stale_remount_warm_history(
    context: &ScenarioContext,
    started: Instant,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    first_order: usize,
    previously_recorded: &[NamespaceCollisionOperation],
) -> Result<(Vec<NamespaceCollisionOperation>, LongHistorySummary), ScenarioError> {
    let mut operations = Vec::new();
    let mut persisted_operations = previously_recorded.to_vec();
    let delete_target = isolation.path(COLD_OBSERVER_DELETE_TARGET);
    let rename_source = isolation.path(COLD_OBSERVER_RENAME_SOURCE);
    let rename_dest = isolation.path(COLD_OBSERVER_RENAME_DEST);
    let overwrite_target = isolation.path(COLD_OBSERVER_OVERWRITE_TARGET);
    let append_target = isolation.path(COLD_OBSERVER_APPEND_TARGET);
    let nested_file = isolation.path(COLD_OBSERVER_NESTED_FILE);
    let payload = |label: &str, path: &str| isolation.payload(label, path, label);
    let rename_payload = payload("rename_source", &rename_source);
    let append_payload_bytes = payload("append", &append_target);
    let nested_payload = payload("nested_child", &nested_file);

    for (offset, (client_index, operation, path, log_path, action)) in [
        (
            0,
            "create",
            delete_target.clone(),
            delete_target.clone(),
            Some(payload("delete_target", &delete_target)),
        ),
        (
            0,
            "delete",
            delete_target.clone(),
            delete_target.clone(),
            None,
        ),
        (
            0,
            "create",
            rename_source.clone(),
            rename_source.clone(),
            Some(rename_payload.clone()),
        ),
        (
            1,
            "rename",
            rename_dest.clone(),
            format!("{rename_source} -> {rename_dest}"),
            None,
        ),
        (
            0,
            "overwrite",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v2", &overwrite_target)),
        ),
        (
            1,
            "append",
            append_target.clone(),
            append_target.clone(),
            Some(append_payload_bytes.clone()),
        ),
        (
            0,
            "create",
            nested_file.clone(),
            nested_file.clone(),
            Some(nested_payload.clone()),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let client = &clients[client_index];
        let operation_path = if operation == "rename" {
            rename_source.as_str()
        } else {
            path.as_str()
        };
        let target = (operation == "rename").then_some(rename_dest.as_str());
        let helper_operation = if operation == "overwrite" {
            "overwrite-barrier"
        } else {
            operation
        };
        let recorded = timed_cold_observer_helper_fs_op(
            context,
            started,
            first_order + offset,
            client,
            operation,
            helper_operation,
            operation_path,
            &log_path,
            HOTSPOT_CHURN_OPERATION_TIMEOUT,
            action.as_deref(),
            target,
        )?;
        record_sparse_operation(context, &mut persisted_operations, recorded.clone())?;
        operations.push(recorded);
        if matches!(operation, "create" | "overwrite") && path != delete_target {
            wait_for_mounted_initial_payloads_until(
                clients,
                &[(&path, action.as_deref().unwrap())],
                Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                "stale remount warm history",
            )?;
        }
    }

    let final_payload = payload("overwrite_v2", &overwrite_target);
    let bounded_operation_count = first_order + operations.len() - 1;
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&rename_dest, rename_payload.as_slice()),
            (&overwrite_target, final_payload.as_slice()),
            (&append_target, append_payload_bytes.as_slice()),
            (&nested_file, nested_payload.as_slice()),
        ],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "stale remount warm history",
    )?;
    Ok((
        operations,
        LongHistorySummary {
            bounded_operation_count,
            expected_present_paths: vec![
                rename_dest,
                overwrite_target.clone(),
                append_target,
                nested_file,
            ],
            expected_absent_paths: vec![delete_target, rename_source],
            final_checksum_sha256: bytes_sha256(&final_payload),
        },
    ))
}

fn run_cold_observer_warm_history(
    started: Instant,
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<(Vec<NamespaceCollisionOperation>, LongHistorySummary), ScenarioError> {
    let mut operations = Vec::new();
    let nested_dir = isolation.path("nested");
    let delete_target = isolation.path(COLD_OBSERVER_DELETE_TARGET);
    let rename_source = isolation.path(COLD_OBSERVER_RENAME_SOURCE);
    let rename_dest = isolation.path(COLD_OBSERVER_RENAME_DEST);
    let overwrite_target = isolation.path(COLD_OBSERVER_OVERWRITE_TARGET);
    let append_target = isolation.path(COLD_OBSERVER_APPEND_TARGET);
    let nested_file = isolation.path(COLD_OBSERVER_NESTED_FILE);

    let payload = |label: &str, path: &str| isolation.payload(label, path, label);
    let nested_payload = payload("nested_child", &nested_file);
    for (order, (client_index, operation, path, log_path, action)) in (1..).zip([
        (0, "mkdir", nested_dir.clone(), nested_dir.clone(), None),
        (
            0,
            "create",
            delete_target.clone(),
            delete_target.clone(),
            Some(payload("delete_target", &delete_target)),
        ),
        (
            1,
            "delete",
            delete_target.clone(),
            delete_target.clone(),
            None,
        ),
        (
            0,
            "create",
            rename_source.clone(),
            rename_source.clone(),
            Some(payload("rename_source", &rename_source)),
        ),
        (
            1,
            "rename",
            rename_dest.clone(),
            format!("{rename_source} -> {rename_dest}"),
            None,
        ),
        (
            2,
            "create",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v1", &overwrite_target)),
        ),
        (
            0,
            "overwrite",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v2", &overwrite_target)),
        ),
        (
            1,
            "append",
            append_target.clone(),
            append_target.clone(),
            Some(payload("append", &append_target)),
        ),
        (
            2,
            "create",
            nested_file.clone(),
            nested_file.clone(),
            Some(nested_payload.clone()),
        ),
    ]) {
        let client = &clients[client_index];
        let operation_path = if operation == "rename" {
            rename_source.as_str()
        } else {
            path.as_str()
        };
        let target = (operation == "rename").then_some(rename_dest.as_str());
        let helper_operation = if operation == "overwrite" {
            "overwrite-barrier"
        } else {
            operation
        };
        let operation_record = timed_cold_observer_helper_fs_op(
            context,
            started,
            order,
            client,
            operation,
            helper_operation,
            operation_path,
            &log_path,
            HOTSPOT_CHURN_OPERATION_TIMEOUT,
            action.as_deref(),
            target,
        )?;
        operations.push(operation_record);
        match operation {
            "create" if path == delete_target || path == rename_source => {
                wait_for_mounted_initial_payloads_until(
                    clients,
                    &[(&path, action.as_deref().unwrap())],
                    Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                    ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                    "cold observer warm history",
                )?;
            }
            "create" | "overwrite" if path == overwrite_target => {
                wait_for_mounted_initial_payloads_until(
                    clients,
                    &[(&path, action.as_deref().unwrap())],
                    Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                    ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                    "cold observer warm history",
                )?;
            }
            _ => {}
        }
    }

    let final_payload = payload("overwrite_v2", &overwrite_target);
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&overwrite_target, final_payload.as_slice()),
            (&nested_file, nested_payload.as_slice()),
        ],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "cold observer warm history",
    )?;
    fs::write(
        context.scenario_dir.join("cold-observer-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "cold_observer": "client-d",
            "warm_clients": COLD_OBSERVER_WARM_CLIENT_NAMES,
            "discovered_after_history": true,
        }))?,
    )?;

    let bounded_operation_count = operations.len();
    Ok((
        operations,
        LongHistorySummary {
            bounded_operation_count,
            expected_present_paths: vec![
                rename_dest,
                overwrite_target.clone(),
                append_target,
                nested_file,
            ],
            expected_absent_paths: vec![delete_target, rename_source],
            final_checksum_sha256: bytes_sha256(&final_payload),
        },
    ))
}

#[allow(clippy::too_many_arguments)]
fn timed_cold_observer_helper_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    helper_operation: &str,
    path: &str,
    log_path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let label = format!("{} {} {}", client.name, operation, log_path);
    run_hotspot_helper_process(
        context,
        timeout,
        client,
        helper_operation,
        path,
        target,
        payload,
        label,
    )?;
    required_op(started, order, client, operation, log_path, Ok(()))
}

fn run_write_acceptance_crash_operations(
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let writer = &clients[0];
    let mut operations = Vec::new();
    let phase = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => "attempt_create_before_crash",
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => "partial_write",
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => "full_write",
        _ => unreachable!("write acceptance crash scenario expected"),
    };
    let file_name = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => "before-write.txt",
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => "partial-write.txt",
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => "full-write.txt",
        _ => unreachable!("write acceptance crash scenario expected"),
    };
    let path = isolation.path(file_name);
    let payload = isolation.payload(
        phase,
        &path,
        &format!("write acceptance crash phase {phase}\n"),
    );
    let expected = expected_write_acceptance_payload(kind, &payload);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        phase,
        &path,
        match expected.as_deref() {
            Some(accepted_payload) => {
                write_acceptance_payload(writer.mount_point.join(&path), accepted_payload)
                    .map_err(|error| error.to_string())
            }
            None => Ok(()),
        },
    ))?);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "force_kill",
        &path,
        controller
            .force_kill(&writer.name)
            .map_err(|error| error.to_string()),
    ))?);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "remount",
        &path,
        controller
            .remount(&writer.name)
            .map_err(|error| error.to_string()),
    ))?);
    let recovered = fs::read(writer.mount_point.join(&path));
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "readback",
        &path,
        match expected.as_deref() {
            Some(expected) => recovered
                .map_err(|error| error.to_string())
                .and_then(|bytes| {
                    if bytes == expected {
                        Ok(())
                    } else {
                        Err(format!(
                            "readback mismatch for {path}: expected checksum {}, got {}",
                            bytes_sha256(expected),
                            bytes_sha256(&bytes)
                        ))
                    }
                }),
            None => match recovered {
                Ok(bytes) => Err(format!(
                    "readback unexpectedly found {path}: {} bytes, checksum {}",
                    bytes.len(),
                    bytes_sha256(&bytes)
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.to_string()),
            },
        },
    ))?);
    operations.sort_by_key(|operation| operation.order);
    Ok(operations)
}

fn expected_write_acceptance_payload(kind: AdvancedMountedKind, payload: &[u8]) -> Option<Vec<u8>> {
    match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => None,
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => {
            Some(payload[..payload.len() / 2].to_vec())
        }
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => Some(payload.to_vec()),
        _ => unreachable!("write acceptance crash scenario expected"),
    }
}

fn write_acceptance_payload(path: PathBuf, accepted_payload: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
        options.custom_flags(FILE_FLAG_WRITE_THROUGH);
    }
    let mut file = options.open(path)?;
    if !accepted_payload.is_empty() {
        file.write_all(accepted_payload)?;
    }
    file.sync_all()
}

fn run_mounted_chaos_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    let active_workers = Arc::new(AtomicUsize::new(0));
    let writer = &clients[0];
    seed_mounted_chaos_initial_tree(writer, isolation)?;
    wait_for_mounted_chaos_initial_tree(
        context,
        clients,
        isolation,
        initial_convergence_timeout,
        poll_interval,
    )?;
    for &(path, _) in MOUNTED_CHAOS_INITIAL_TREE {
        let path = isolation.path(path.strip_prefix("chaos/").unwrap_or(path));
        wait_for_hotspot_helper_stat_until(
            context,
            writer,
            &path,
            Instant::now() + HOTSPOT_CHURN_OPERATION_TIMEOUT,
            poll_interval,
            "mounted chaos initial path before conflicting operations",
            Arc::clone(&active_workers),
        )?;
    }
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        1,
        &clients[0],
        "rename",
        &isolation.path("rename-source.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        Some(&isolation.path("renamed-by-a.txt")),
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        2,
        &clients[1],
        "unlink",
        &isolation.path("unlink-target.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let append_payload = isolation.payload("append", "append-target.txt", "appended by chaos\n");
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        3,
        &clients[2],
        "append",
        &isolation.path("append-target.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&append_payload),
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let overwrite_payload =
        isolation.payload("overwrite", "read-target.txt", "new committed payload\n");
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        4,
        &clients[0],
        "overwrite",
        &isolation.path("read-target.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&overwrite_payload),
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        5,
        &clients[1],
        "mkdir",
        &isolation.path("dir"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        6,
        &clients[2],
        "readdir",
        &isolation.root,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    Ok(operations)
}

fn seed_mounted_chaos_initial_tree(
    writer: &ClientSpec,
    isolation: &ScenarioIsolation,
) -> Result<(), ScenarioError> {
    for &(path, payload) in MOUNTED_CHAOS_INITIAL_TREE {
        let logical_path = path.strip_prefix("chaos/").unwrap_or(path);
        let mounted_path = isolation.path(logical_path);
        let payload =
            isolation.payload("initial", &mounted_path, &String::from_utf8_lossy(payload));
        write_mounted_initial_file(
            writer.mount_point.join(mounted_path),
            &payload,
            "mounted chaos initial tree",
        )?;
    }
    Ok(())
}

fn write_mounted_initial_file(
    path: PathBuf,
    payload: &[u8],
    label: &str,
) -> Result<(), ScenarioError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = mounted_write_barrier_open_options();
    let mut file = options.create_new(true).open(&path).map_err(|error| {
        ScenarioError::Io(io::Error::new(
            error.kind(),
            format!("{label}: open {}: {error}", path.display()),
        ))
    })?;
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
        return Err(ScenarioError::Io(io::Error::other(format!(
            "{label} write readback mismatch for {}: expected {} bytes, read {} bytes",
            path.display(),
            payload.len(),
            readback.len()
        ))));
    }
    Ok(())
}

fn scenario_error_to_io(error: ScenarioError) -> io::Error {
    match error {
        ScenarioError::Io(error) => error,
        error => io::Error::other(error),
    }
}

fn wait_for_mounted_payload_readback(
    path: &Path,
    payload: &[u8],
    timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<u8>, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let mut last_readback = None;
    loop {
        match fs::read(path) {
            Ok(readback) if readback == payload => return Ok(readback),
            Ok(readback) => last_readback = Some(readback),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            return last_readback.ok_or_else(|| {
                ScenarioError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("mounted payload readback did not find {}", path.display()),
                ))
            });
        }
        thread::sleep(poll_interval.max(Duration::from_millis(1)));
    }
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

fn wait_for_mounted_chaos_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let required_paths = MOUNTED_CHAOS_INITIAL_TREE
        .iter()
        .map(|(path, payload)| {
            let logical_path = path.strip_prefix("chaos/").unwrap_or(path);
            let mounted_path = isolation.path(logical_path);
            let payload =
                isolation.payload("initial", &mounted_path, &String::from_utf8_lossy(payload));
            (mounted_path, payload)
        })
        .collect::<Vec<_>>();
    let required_paths = required_paths
        .iter()
        .map(|(path, payload)| (path.as_str(), payload.as_slice()))
        .collect::<Vec<_>>();
    wait_for_mounted_initial_payloads_until(
        clients,
        &required_paths,
        deadline,
        poll_interval,
        "mounted chaos initial tree",
    )?;
    let remaining = remaining_initial_validation_timeout(deadline, "mounted chaos initial tree")?;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation-initial-chaos"),
        remaining,
        poll_interval,
    )
    .map_err(ScenarioError::from)
}

fn run_hotspot_churn_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    run_hotspot_churn_operations_with_timeout(
        context,
        clients,
        isolation,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        initial_convergence_timeout,
        poll_interval,
    )
}

fn run_hotspot_churn_operations_with_timeout(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    operation_timeout: Duration,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    let mkdir_client = clients[0].clone();
    run_hotspot_helper_process(
        context,
        operation_timeout,
        &mkdir_client,
        "mkdir",
        &isolation.root,
        None,
        None,
        format!("{} mkdir {}", mkdir_client.name, isolation.root),
    )?;
    wait_for_hotspot_initial_tree(
        context,
        clients,
        &isolation.root,
        initial_convergence_timeout,
        poll_interval,
    )?;
    for round in 0..10 {
        let slot = isolation.path(&format!("slot-{}.txt", round % 6));
        let renamed = isolation.path(&format!("slot-{}-renamed.txt", round % 6));
        let write_path = slot.clone();
        let write_payload = isolation.payload(
            "overwrite",
            &write_path,
            &format!("hotspot round {round}\n"),
        );
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[0],
            "overwrite",
            &slot,
            operation_timeout,
            Some(&write_payload),
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        let append_path = slot.clone();
        let append_payload_bytes =
            isolation.payload("append", &append_path, &format!("append round {round}\n"));
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[1],
            "append",
            &slot,
            operation_timeout,
            Some(&append_payload_bytes),
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        wait_for_client_path_visible_until(
            &clients[0],
            &slot,
            Instant::now() + operation_timeout,
            poll_interval,
            "hotspot rename source",
        )?;
        let rename_target = renamed.clone();
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[0],
            "rename",
            &slot,
            operation_timeout,
            None,
            Some(&rename_target),
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[2],
            "readdir",
            &isolation.root,
            operation_timeout,
            None,
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
    }
    Ok(operations)
}

fn wait_for_hotspot_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    root: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    wait_for_required_paths_visible_until(
        clients,
        &[root],
        deadline,
        poll_interval,
        "hotspot initial tree",
    )?;
    let remaining = remaining_initial_validation_timeout(deadline, "hotspot initial tree")?;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation-initial-hotspot"),
        remaining,
        poll_interval,
    )
    .map_err(ScenarioError::from)
}

#[allow(clippy::too_many_arguments)]
fn timed_hotspot_helper_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    timed_hotspot_helper_fs_op_with_helper(
        context, started, order, client, operation, operation, path, timeout, payload, target,
    )
}

#[allow(clippy::too_many_arguments)]
fn timed_hotspot_helper_fs_op_with_helper(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    helper_operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let label = format!("{} {} {}", client.name, operation, path);
    match run_hotspot_helper_process(
        context,
        timeout,
        client,
        helper_operation,
        path,
        target,
        payload,
        label,
    ) {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => Err(error),
        Err(error) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Err(error.to_string()),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn timed_hotspot_mount_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
    active_workers: Arc<AtomicUsize>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let _ = active_workers;
    match run_hotspot_helper_process(
        context,
        timeout,
        client,
        operation,
        path,
        target,
        payload,
        format!("{} {} {}", client.name, operation, path),
    ) {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => Err(error),
        Err(error) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Err(error.to_string()),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_hotspot_mount_operation_with_timeout(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
    active_workers: Arc<AtomicUsize>,
) -> Result<(), ScenarioError> {
    let payload_path = write_helper_payload(context, client, operation, payload)?;
    let progress_path = helper_progress_path(context, client, operation, path)?;
    let mount_point = client.mount_point.clone();
    let path_owned = path.to_string();
    let target_owned = target.map(str::to_string);
    let operation_owned = operation.to_string();
    let pending_label = format!("{} {} {}", client.name, operation, path);
    let result = run_timed_hotspot_io_with_worker_limit(
        timeout,
        pending_label.clone(),
        move || {
            run_hotspot_op_helper(
                &operation_owned,
                mount_point,
                &path_owned,
                target_owned.as_deref(),
                payload_path.as_deref(),
                Some(&progress_path),
            )
            .map_err(io::Error::other)
        },
        active_workers,
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    );
    match result {
        Ok(()) => Ok(()),
        Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => {
            #[cfg(not(test))]
            let _ = terminate_blocked_hotspot_client_process(client, &pending_label);
            Err(error)
        }
        Err(error) => Err(ScenarioError::Io(io::Error::other(format!(
            "{pending_label}: {error}"
        )))),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_hotspot_helper_process(
    context: &ScenarioContext,
    timeout: Duration,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    target: Option<&str>,
    payload: Option<&[u8]>,
    pending_label: String,
) -> Result<(), ScenarioError> {
    let payload_path = write_helper_payload(context, client, operation, payload)?;
    let progress_path = helper_progress_path(context, client, operation, path)?;

    #[cfg(test)]
    {
        let _ = timeout;
        let _ = pending_label;
        let result = run_hotspot_op_helper(
            operation,
            client.mount_point.clone(),
            path,
            target,
            payload_path.as_deref(),
            Some(&progress_path),
        );
        result.map_err(|error| {
            ScenarioError::Io(io::Error::other(format!(
                "hotspot helper failed for {pending_label}: {error}"
            )))
        })
    }

    #[cfg(not(test))]
    {
        let exe = prepare_hotspot_helper_exe(context)?;
        let mut command = Command::new(exe);
        command
            .arg("hotspot-op")
            .arg("--operation")
            .arg(operation)
            .arg("--sandbox")
            .arg(&context.sandbox)
            .arg("--mount-point")
            .arg(&client.mount_point)
            .arg("--path")
            .arg(path)
            .arg("--progress")
            .arg(&progress_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(target) = target {
            command.arg("--target").arg(target);
        }
        if let Some(payload_path) = &payload_path {
            command.arg("--payload").arg(payload_path);
        }
        command
            .arg("--timeout-ms")
            .arg(duration_millis_u64(hotspot_helper_internal_timeout(timeout)).to_string());

        ChildJob::prepare_command(&mut command)?;
        let mut child = command.spawn()?;
        write_hotspot_helper_process_metadata(
            context,
            child.id(),
            client,
            operation,
            path,
            target,
        )?;
        let job = ChildJob::assign_to_child(&child).map_err(|error| {
            let _ = child.kill();
            let _ = child.wait();
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!("failed to isolate hotspot helper process: {error}"),
            ))
        })?;

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                let stderr = child
                    .stderr
                    .take()
                    .and_then(|mut stderr| {
                        let mut output = String::new();
                        stderr.read_to_string(&mut output).ok().map(|_| output)
                    })
                    .unwrap_or_default();
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                let progress_suffix = progress
                    .as_ref()
                    .map(|progress| format!("; progress={progress}"))
                    .unwrap_or_default();
                if stderr.contains(HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER) {
                    return Err(namespace_collision_operation_timeout(
                        timeout,
                        vec![(1, format!("{pending_label}{progress_suffix}"))],
                    ));
                }
                return Err(ScenarioError::Io(io::Error::other(format!(
                    "hotspot helper failed for {pending_label}: status={status}; stderr={}; progress={}",
                    stderr.trim(),
                    progress.as_deref().unwrap_or("<none>")
                ))));
            }
            if Instant::now() >= deadline {
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                let progress_suffix = progress
                    .as_ref()
                    .map(|progress| format!("; progress={progress}"))
                    .unwrap_or_default();
                eprintln!(
                    "studiofs-e2e: mounted helper operation timed out after {} ms; terminating helper process: {}{}",
                    timeout.as_millis(),
                    pending_label,
                    progress_suffix
                );
                let mut cleanup_error =
                    terminate_blocked_hotspot_client_process(client, &pending_label)
                        .err()
                        .map(|error| error.to_string());
                let _ = job.terminate();
                if !wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE).unwrap_or(false) {
                    let _ = job.force_kill();
                    if let Err(error) =
                        terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                    {
                        cleanup_error.get_or_insert_with(|| error.to_string());
                    }
                    let _ = child.kill();
                    let _ = wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE);
                }
                if let Err(error) = terminate_blocked_hotspot_client_process(client, &pending_label)
                {
                    cleanup_error.get_or_insert_with(|| error.to_string());
                }
                let primary = namespace_collision_operation_timeout(
                    timeout,
                    vec![(1, format!("{pending_label}{progress_suffix}"))],
                );
                if let Some(cleanup) = cleanup_error {
                    return Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
                        primary: primary.to_string(),
                        cleanup,
                    });
                }
                return Err(primary);
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

fn hotspot_helper_internal_timeout(parent_timeout: Duration) -> Duration {
    let reserve = CHILD_TERMINATION_GRACE + HOTSPOT_HELPER_CANCEL_GRACE;
    parent_timeout
        .checked_sub(reserve)
        .unwrap_or(parent_timeout)
}

#[cfg(all(windows, not(test)))]
fn terminate_blocked_hotspot_helper_process(pid: u32, pending_label: &str) -> io::Result<()> {
    eprintln!(
        "studiofs-e2e: force terminating hotspot helper process {pid} to release blocked I/O: {pending_label}"
    );
    match terminate_windows_pid_tree(pid, "blocked hotspot helper process", pending_label) {
        Ok(()) => Ok(()),
        Err(error) => {
            eprintln!(
                "studiofs-e2e: failed to terminate blocked hotspot helper process {pid}: {error}"
            );
            Err(error)
        }
    }
}

#[cfg(all(not(windows), not(test)))]
fn terminate_blocked_hotspot_helper_process(_pid: u32, _pending_label: &str) -> io::Result<()> {
    Ok(())
}

#[cfg(all(windows, not(test)))]
fn terminate_blocked_hotspot_client_process(
    client: &ClientSpec,
    pending_label: &str,
) -> io::Result<()> {
    let Some(pid) = client.process_id else {
        return Ok(());
    };
    eprintln!(
        "studiofs-e2e: terminating mounted client process {pid} to release blocked hotspot worker I/O: {pending_label}"
    );
    match terminate_windows_pid_tree(pid, "blocked mounted client process", pending_label) {
        Ok(()) => Ok(()),
        Err(error) => {
            eprintln!(
                "studiofs-e2e: failed to terminate blocked mounted client process {pid}: {error}"
            );
            Err(error)
        }
    }
}

#[cfg(all(not(windows), not(test)))]
fn terminate_blocked_hotspot_client_process(
    _client: &ClientSpec,
    _pending_label: &str,
) -> io::Result<()> {
    Ok(())
}

#[cfg(all(windows, not(test)))]
fn terminate_windows_pid_tree(pid: u32, label: &str, pending_label: &str) -> io::Result<()> {
    capture_process_diagnostics_best_effort(pid, &format!("{label}: {pending_label}"));
    match run_taskkill_with_timeout(pid, Duration::from_secs(5)) {
        Ok(output) if windows_taskkill_status_is_success(output.status.code()) => Ok(()),
        Ok(output) => {
            eprintln!(
                "studiofs-e2e: taskkill for {label} {pid} exited with {}; stdout={}; stderr={}",
                output.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&output.stdout).trim(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
            Err(io::Error::other(format!(
                "taskkill for {label} {pid} exited with {}; {pending_label}",
                output.status.code().unwrap_or(-1)
            )))
        }
        Err(error) => {
            eprintln!("studiofs-e2e: taskkill for {label} {pid} failed: {error}");
            Err(error)
        }
    }
}

#[cfg(not(test))]
fn prepare_hotspot_helper_exe(context: &ScenarioContext) -> io::Result<PathBuf> {
    let source = std::env::current_exe()?;
    let helper_dir = context.scenario_dir.join("hotspot-helper-bin");
    fs::create_dir_all(&helper_dir)?;
    let extension = source
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("exe");
    let helper = helper_dir.join(format!("studiofs-e2e-hotspot-helper.{extension}"));
    if !helper.exists() {
        fs::copy(source, &helper)?;
    }
    Ok(helper)
}

fn write_helper_payload(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    payload: Option<&[u8]>,
) -> Result<Option<PathBuf>, ScenarioError> {
    match payload {
        Some(payload) => {
            let payload_dir = context.scenario_dir.join("hotspot-op-payloads");
            fs::create_dir_all(&payload_dir)?;
            let id = HOTSPOT_HELPER_PAYLOAD_ID.fetch_add(1, Ordering::SeqCst);
            let payload_path =
                payload_dir.join(format!("{}-{}-{}.payload", id, client.name, operation));
            fs::write(&payload_path, payload)?;
            Ok(Some(payload_path))
        }
        None => Ok(None),
    }
}

fn helper_progress_path(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    path: &str,
) -> Result<PathBuf, ScenarioError> {
    let progress_dir = context.scenario_dir.join("hotspot-helper-progress");
    fs::create_dir_all(&progress_dir)?;
    let id = HOTSPOT_HELPER_PAYLOAD_ID.fetch_add(1, Ordering::SeqCst);
    Ok(progress_dir.join(format!(
        "{id:06}-{}-{}-{}.jsonl",
        client.name,
        sanitize_hotspot_progress_component(operation),
        sanitize_hotspot_progress_component(path)
    )))
}

#[cfg(not(test))]
fn write_hotspot_helper_process_metadata(
    context: &ScenarioContext,
    pid: u32,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    target: Option<&str>,
) -> io::Result<()> {
    let process_dir = context.scenario_dir.join("hotspot-helper-processes");
    fs::create_dir_all(&process_dir)?;
    let metadata = json!({
        "process_id": pid,
        "process_name": "studiofs-e2e-hotspot-helper",
        "client": client.name,
        "operation": operation,
        "path": path,
        "target": target,
    });
    fs::write(
        process_dir.join(format!("{pid}.json")),
        serde_json::to_vec_pretty(&metadata).map_err(io::Error::other)?,
    )
}

#[cfg(not(test))]
fn read_hotspot_helper_progress_tail(progress: &Path) -> Option<String> {
    let contents = fs::read_to_string(progress).ok()?;
    contents
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(str::to_string)
}

fn run_timed_hotspot_io_with_worker_limit<T: Send + 'static>(
    timeout: Duration,
    pending_label: String,
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
    active_workers: Arc<AtomicUsize>,
    max_active_workers: usize,
) -> Result<T, ScenarioError> {
    let Some(permit) = HotspotIoWorkerPermit::try_acquire(active_workers, max_active_workers)
    else {
        let pending_label = format!(
            "hotspot worker limit reached; previous timed operation may still be blocked before {pending_label}"
        );
        eprintln!(
            "studiofs-e2e: {pending_label}; refusing to spawn another mounted hotspot worker"
        );
        return Err(namespace_collision_operation_timeout(
            timeout,
            vec![(1, pending_label)],
        ));
    };
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name("studiofs-e2e-hotspot-io".to_string())
        .spawn(move || {
            let result = f();
            drop(permit);
            let _ = tx.send(result);
        })?;
    match rx.recv_timeout(timeout) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.into()),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            eprintln!(
                "studiofs-e2e: mounted hotspot operation timed out after {} ms; worker thread may remain blocked until process exit: {}",
                timeout.as_millis(),
                pending_label
            );
            Err(namespace_collision_operation_timeout(
                timeout,
                vec![(1, pending_label)],
            ))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(ScenarioError::Io(io::Error::other(
            "hotspot operation worker channel closed before completion",
        ))),
    }
}

struct HotspotIoWorkerPermit {
    active_workers: Arc<AtomicUsize>,
}

impl HotspotIoWorkerPermit {
    fn try_acquire(active_workers: Arc<AtomicUsize>, max_active_workers: usize) -> Option<Self> {
        let mut current = active_workers.load(Ordering::Acquire);
        loop {
            if current >= max_active_workers {
                return None;
            }
            match active_workers.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Self { active_workers }),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for HotspotIoWorkerPermit {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn run_fault_chaos_operations(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = run_mounted_chaos_operations(
        context,
        &clients[..3],
        isolation,
        ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
    )?;
    let compose = compose_control_from_manifest(context)?;
    let executor = SystemCommandExecutor;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[0],
        "stop_postgres",
        "compose/postgres",
        compose
            .stop_service(POSTGRES_SERVICE_NAME, &executor)
            .map_err(ScenarioError::from),
    )?);
    let postgres_outage_path = isolation.path("postgres-outage.txt");
    let postgres_outage_payload = isolation.payload(
        "postgres_outage",
        &postgres_outage_path,
        "accepted while postgres is down\n",
    );
    operations.push(required_record(fs_op(
        started,
        operations.len() + 1,
        &clients[0],
        "local_write_during_pg_outage",
        &postgres_outage_path,
        || {
            write_parented(
                clients[0].mount_point.join(&postgres_outage_path),
                &postgres_outage_payload,
            )
        },
    ))?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[0],
        "start_postgres",
        "compose/postgres",
        compose
            .start_service(POSTGRES_SERVICE_NAME, &executor)
            .map_err(ScenarioError::from),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[0],
        "crash_remount",
        clients[0].name.as_str(),
        controller
            .force_kill(&clients[0].name)
            .and_then(|()| controller.remount(&clients[0].name))
            .map_err(ScenarioError::from),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[1],
        "stop_rustfs",
        "compose/rustfs",
        compose
            .stop_service(RUSTFS_SERVICE_NAME, &executor)
            .map_err(ScenarioError::from),
    )?);
    let rustfs_outage_path = isolation.path("rustfs-outage.txt");
    let rustfs_outage_payload = isolation.payload(
        "rustfs_outage",
        &rustfs_outage_path,
        "accepted while rustfs is down\n",
    );
    operations.push(required_record(fs_op(
        started,
        operations.len() + 1,
        &clients[1],
        "local_write_during_s3_outage",
        &rustfs_outage_path,
        || {
            write_parented(
                clients[1].mount_point.join(&rustfs_outage_path),
                &rustfs_outage_payload,
            )
        },
    ))?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &clients[1],
        "start_rustfs",
        "compose/rustfs",
        compose
            .start_service(RUSTFS_SERVICE_NAME, &executor)
            .map_err(ScenarioError::from),
    )?);
    operations.push(fs_op(
        started,
        operations.len() + 1,
        &clients[3],
        "cold_observer_readdir",
        &isolation.root,
        || fs::read_dir(clients[3].mount_point.join(&isolation.root)).map(|_| ()),
    ));
    Ok(operations)
}

fn run_lww_reconnect_operations(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<(Vec<NamespaceCollisionOperation>, LwwReconnectOracle), ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    let path = isolation.path("winner.txt");
    let client_a_payload = isolation.payload("write_client_a", &path, "version from client-a\n");
    operations.push(required_record(fs_op(
        started,
        1,
        &clients[0],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[0].mount_point.join(&path),
                &client_a_payload,
                "lww reconnect client-a write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        2,
        &clients[2],
        "stale_writer_offline",
        clients[2].name.as_str(),
        controller
            .force_kill(&clients[2].name)
            .map_err(ScenarioError::from),
    )?);
    let client_b_payload = isolation.payload("write_client_b", &path, "version from client-b\n");
    operations.push(required_record(fs_op(
        started,
        3,
        &clients[1],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[1].mount_point.join(&path),
                &client_b_payload,
                "lww reconnect client-b write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    let client_d_payload = isolation.payload("write_client_d", &path, "version from client-d\n");
    operations.push(required_record(fs_op(
        started,
        4,
        &clients[3],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[3].mount_point.join(&path),
                &client_d_payload,
                "lww reconnect client-d write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        5,
        &clients[2],
        "stale_writer_reconnect",
        clients[2].name.as_str(),
        controller
            .remount(&clients[2].name)
            .map_err(ScenarioError::from),
    )?);
    let winner = isolation.payload(
        "write_lww_winner",
        &path,
        "explicit lww winner from reconnected client-c\n",
    );
    operations.push(required_record(fs_op(
        started,
        6,
        &clients[2],
        "write_lww_winner",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[2].mount_point.join(&path),
                &winner,
                "lww reconnect winner write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        7,
        &clients[2],
        "winner_crash_restart",
        clients[2].name.as_str(),
        controller
            .force_kill(&clients[2].name)
            .and_then(|()| controller.remount(&clients[2].name))
            .map_err(ScenarioError::from),
    )?);
    let actual = wait_for_mounted_payload_readback(
        &clients[2].mount_point.join(&path),
        &winner,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
    .map_err(scenario_error_to_io)?;
    let oracle = LwwReconnectOracle {
        expected_payload_sha256: bytes_sha256(&winner),
        actual_payload_sha256: bytes_sha256(&actual),
        matched: actual == winner,
    };
    write_lww_oracle(context, &oracle)?;
    Ok((operations, oracle))
}

fn run_open_handle_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    seed_open_handle_initial_tree(context, clients, isolation, OPEN_HANDLE_OPERATION_TIMEOUT)?;
    wait_for_open_handle_initial_tree(
        context,
        clients,
        isolation,
        initial_convergence_timeout,
        poll_interval,
    )?;
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    let rename_dest = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_DEST);
    let append_payload = isolation.payload(
        "append_old_handle",
        &unlink_target,
        "append through old handle\n",
    );
    let mut handle = OpenOptions::new()
        .append(true)
        .open(clients[0].mount_point.join(&unlink_target))?;
    operations.push(fs_op(
        started,
        1,
        &clients[1],
        "unlink",
        &unlink_target,
        || fs::remove_file(clients[1].mount_point.join(&unlink_target)),
    ));
    operations.push(fs_op(
        started,
        2,
        &clients[1],
        "rename",
        &rename_source,
        || {
            fs::rename(
                clients[1].mount_point.join(&rename_source),
                clients[1].mount_point.join(&rename_dest),
            )
        },
    ));
    operations.push(required_record(record_advanced_operation(
        started,
        3,
        &clients[0],
        "append_old_handle",
        &unlink_target,
        handle
            .write_all(&append_payload)
            .and_then(|()| handle.sync_all())
            .map_err(|error| error.to_string()),
    ))?);
    drop(handle);
    Ok(operations)
}

fn seed_open_handle_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    operation_timeout: Duration,
) -> Result<(), ScenarioError> {
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    let unlink_payload = isolation.payload(
        "initial_unlink_target",
        &unlink_target,
        &String::from_utf8_lossy(OPEN_HANDLE_UNLINK_TARGET_PAYLOAD),
    );
    let rename_payload = isolation.payload(
        "initial_rename_source",
        &rename_source,
        &String::from_utf8_lossy(OPEN_HANDLE_RENAME_SOURCE_PAYLOAD),
    );
    let active_workers = Arc::new(AtomicUsize::new(0));
    run_hotspot_mount_operation_with_timeout(
        context,
        &clients[0],
        "create",
        &unlink_target,
        operation_timeout,
        Some(&unlink_payload),
        None,
        Arc::clone(&active_workers),
    )
    .map_err(|error| ScenarioError::Io(io::Error::other(format!("open handle seed: {error}"))))?;
    run_hotspot_mount_operation_with_timeout(
        context,
        &clients[0],
        "create",
        &rename_source,
        operation_timeout,
        Some(&rename_payload),
        None,
        Arc::clone(&active_workers),
    )
    .map_err(|error| ScenarioError::Io(io::Error::other(format!("open handle seed: {error}"))))?;
    Ok(())
}

fn wait_for_open_handle_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    let required_payloads = [
        (
            unlink_target,
            isolation.payload(
                "initial_unlink_target",
                &open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET),
                &String::from_utf8_lossy(OPEN_HANDLE_UNLINK_TARGET_PAYLOAD),
            ),
        ),
        (
            rename_source,
            isolation.payload(
                "initial_rename_source",
                &open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE),
                &String::from_utf8_lossy(OPEN_HANDLE_RENAME_SOURCE_PAYLOAD),
            ),
        ),
    ];
    let required_payloads = required_payloads
        .iter()
        .map(|(path, payload)| (path.as_str(), payload.as_slice()))
        .collect::<Vec<_>>();
    wait_for_mounted_initial_payloads_until(
        clients,
        &required_payloads,
        deadline,
        poll_interval,
        "open handle initial tree",
    )?;
    let remaining = remaining_initial_validation_timeout(deadline, "open handle initial tree")?;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation-initial-open-handle"),
        remaining,
        poll_interval,
    )
    .map_err(ScenarioError::from)
}

fn open_handle_isolated_path(isolation: &ScenarioIsolation, legacy_path: &str) -> String {
    isolation.path(
        legacy_path
            .strip_prefix("open-handles/")
            .unwrap_or(legacy_path),
    )
}

fn wait_for_mounted_initial_payloads_until(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let last_missing =
            missing_mounted_initial_payloads(clients, required_payloads, &probe_limiter);
        if last_missing.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

fn wait_for_mounted_paths_absent_until(
    clients: &[ClientSpec],
    absent_paths: &[&str],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let present = present_mounted_paths(clients, absent_paths, &probe_limiter);
        if present.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; still present: {}",
                    present.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not disappear before timeout; still present: {}",
                    present.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

fn present_mounted_paths(
    clients: &[ClientSpec],
    absent_paths: &[&str],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    let mut present = Vec::new();
    for client in clients {
        for relative_path in absent_paths {
            let client = client.clone();
            let relative_path = (*relative_path).to_string();
            let client_name = client.name.clone();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(
                probe_limiter,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                move || present_mounted_path_probe(client, relative_path),
            ) {
                Ok(Some(present_path)) => present.push(present_path),
                Ok(None) => {}
                Err(error) => present.push(format!(
                    "{}:{} ({})",
                    client_name,
                    timeout_path,
                    error.detail()
                )),
            }
        }
    }
    present
}

fn present_mounted_path_probe(client: ClientSpec, relative_path: String) -> Option<String> {
    let _ = refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path);
    match fs::metadata(client.mount_point.join(&relative_path)) {
        Ok(_) => Some(format!("{}:{relative_path}", client.name)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => Some(format!("{}:{relative_path} ({error})", client.name)),
    }
}

fn missing_mounted_initial_payloads(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    missing_mounted_initial_payloads_with_probe(
        clients,
        required_payloads,
        probe_limiter,
        MOUNTED_VALIDATION_PROBE_TIMEOUT,
        mounted_initial_payload_probe,
    )
}

fn missing_mounted_initial_payloads_with_probe<F>(
    clients: &[ClientSpec],
    required_payloads: &[(&str, &[u8])],
    probe_limiter: &MountedProbeLimiter,
    probe_timeout: Duration,
    probe: F,
) -> Vec<String>
where
    F: Fn(ClientSpec, String, Vec<u8>) -> MountedPayloadProbeStatus + Copy + Send + 'static,
{
    let mut missing = Vec::new();
    for client in clients {
        for (relative_path, expected_payload) in required_payloads {
            let client = client.clone();
            let client_name = client.name.clone();
            let relative_path = (*relative_path).to_string();
            let expected_payload = expected_payload.to_vec();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(probe_limiter, probe_timeout, move || {
                probe(client, relative_path, expected_payload)
            }) {
                Ok(MountedPayloadProbeStatus::Present) => {}
                Ok(MountedPayloadProbeStatus::Missing(path)) => missing.push(path),
                Err(error) => {
                    missing.push(format!("{client_name}:{timeout_path} ({})", error.detail()));
                }
            }
        }
    }
    missing
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MountedPayloadProbeStatus {
    Present,
    Missing(String),
}

fn mounted_initial_payload_probe(
    client: ClientSpec,
    relative_path: String,
    expected_payload: Vec<u8>,
) -> MountedPayloadProbeStatus {
    let path = client.mount_point.join(&relative_path);
    let parent_contains_child =
        refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path);
    if !parent_contains_child {
        let metadata_detail = match fs::metadata(&path) {
            Ok(metadata) => format!("metadata_len={}", metadata.len()),
            Err(error) => format!("metadata_error={error}"),
        };
        return MountedPayloadProbeStatus::Missing(format!(
            "{}:{relative_path} (parent_contains_child=false; {metadata_detail})",
            client.name
        ));
    }
    match fs::read(&path) {
        Ok(payload) if payload == expected_payload => MountedPayloadProbeStatus::Present,
        Ok(payload) => MountedPayloadProbeStatus::Missing(format!(
            "{}:{relative_path} (parent_contains_child=true; expected {} bytes sha256={}, read {} bytes sha256={})",
            client.name,
            expected_payload.len(),
            bytes_sha256(&expected_payload),
            payload.len(),
            bytes_sha256(&payload)
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let metadata_detail = match fs::metadata(&path) {
                Ok(metadata) => format!("metadata_len={}", metadata.len()),
                Err(metadata_error) => format!("metadata_error={metadata_error}"),
            };
            MountedPayloadProbeStatus::Missing(format!(
                "{}:{relative_path} (parent_contains_child=true; read_error={error}; {metadata_detail})",
                client.name
            ))
        }
        Err(error) => {
            let metadata_detail = match fs::metadata(&path) {
                Ok(metadata) => format!("metadata_len={}", metadata.len()),
                Err(metadata_error) => format!("metadata_error={metadata_error}"),
            };
            MountedPayloadProbeStatus::Missing(format!(
                "{}:{relative_path} (parent_contains_child=true; read_error={error}; {metadata_detail})",
                client.name
            ))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountedProbeFailure {
    TimedOut {
        timeout: Duration,
    },
    WorkerExited,
    WorkerLimitReached {
        active_workers: usize,
        max_active_workers: usize,
    },
}

impl MountedProbeFailure {
    fn detail(self) -> String {
        match self {
            Self::TimedOut { timeout } => {
                format!("probe timed out after {} ms", timeout.as_millis())
            }
            Self::WorkerExited => "probe worker exited without result".to_string(),
            Self::WorkerLimitReached {
                active_workers,
                max_active_workers,
            } => format!(
                "probe skipped because a previous mounted probe timed out; active probe workers {active_workers}/{max_active_workers}"
            ),
        }
    }
}

#[derive(Debug, Clone)]
struct MountedProbeLimiter {
    active_workers: Arc<AtomicUsize>,
    poisoned: Arc<AtomicBool>,
    max_active_workers: usize,
}

impl MountedProbeLimiter {
    fn new(max_active_workers: usize) -> Self {
        Self {
            active_workers: Arc::new(AtomicUsize::new(0)),
            poisoned: Arc::new(AtomicBool::new(false)),
            max_active_workers,
        }
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    fn mark_poisoned(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    fn try_acquire(&self) -> Result<MountedProbePermit, MountedProbeFailure> {
        if self.is_poisoned() {
            return Err(self.worker_limit_reached());
        }

        loop {
            let active_workers = self.active_workers.load(Ordering::Acquire);
            if active_workers >= self.max_active_workers {
                return Err(self.worker_limit_reached());
            }
            if self
                .active_workers
                .compare_exchange(
                    active_workers,
                    active_workers.saturating_add(1),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(MountedProbePermit {
                    active_workers: Arc::clone(&self.active_workers),
                });
            }
        }
    }

    fn worker_limit_reached(&self) -> MountedProbeFailure {
        MountedProbeFailure::WorkerLimitReached {
            active_workers: self.active_workers.load(Ordering::Acquire),
            max_active_workers: self.max_active_workers,
        }
    }
}

struct MountedProbePermit {
    active_workers: Arc<AtomicUsize>,
}

impl Drop for MountedProbePermit {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn run_mounted_probe_with_timeout<T, F>(
    probe_limiter: &MountedProbeLimiter,
    timeout: Duration,
    probe: F,
) -> Result<T, MountedProbeFailure>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit = probe_limiter.try_acquire()?;
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = probe();
        drop(permit);
        let _ = tx.send(result);
    });
    match rx.recv_timeout(timeout) {
        Ok(result) => Ok(result),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            probe_limiter.mark_poisoned();
            Err(MountedProbeFailure::TimedOut { timeout })
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(MountedProbeFailure::WorkerExited),
    }
}

fn refresh_relative_path_parent_contains_child(mount_point: &Path, relative_path: &str) -> bool {
    let relative = Path::new(relative_path);
    let Some(file_name) = relative.file_name() else {
        return true;
    };
    let mut current = mount_point.to_path_buf();
    let _ = drain_directory(&current);
    let Some(parent) = relative.parent() else {
        return directory_contains_child(&current, file_name);
    };
    let mut components = parent.components().peekable();
    while let Some(component) = components.next() {
        current.push(component.as_os_str());
        if components.peek().is_some() {
            if !drain_directory(&current) {
                return false;
            }
        } else {
            return directory_contains_child(&current, file_name);
        }
    }
    directory_contains_child(&current, file_name)
}

fn directory_contains_child(parent: &Path, file_name: &std::ffi::OsStr) -> bool {
    let Ok(entries) = fs::read_dir(parent) else {
        return false;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            return false;
        };
        if entry.file_name() == file_name {
            return true;
        }
    }
    false
}

fn refresh_relative_path_ancestors(mount_point: &Path, relative_path: &str) {
    let mut current = mount_point.to_path_buf();
    let _ = drain_directory(&current);
    let relative = Path::new(relative_path);
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            current.push(component.as_os_str());
            if !drain_directory(&current) {
                break;
            }
        }
    }
}

fn drain_directory(path: &Path) -> bool {
    let Ok(entries) = fs::read_dir(path) else {
        return false;
    };
    for entry in entries {
        if entry.is_err() {
            return false;
        }
    }
    true
}

fn wait_for_required_paths_visible_until(
    clients: &[ClientSpec],
    required_paths: &[&str],
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    loop {
        let last_missing = missing_required_paths(clients, required_paths, &probe_limiter);
        if last_missing.is_empty() {
            return Ok(());
        }
        if probe_limiter.is_poisoned() {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} mounted validation probe timed out; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}",
                    last_missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

fn wait_for_client_path_visible_until(
    client: &ClientSpec,
    relative_path: &str,
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
) -> Result<(), ScenarioError> {
    let probe_limiter = MountedProbeLimiter::new(1);
    loop {
        match client_path_missing(client, relative_path, &probe_limiter) {
            Ok(None) => return Ok(()),
            Ok(Some(missing)) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(ScenarioError::Io(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "{label} did not become visible before timeout; missing: {missing}"
                        ),
                    )));
                }
                thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
            }
            Err(error) => {
                return Err(ScenarioError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "{label} mounted validation probe timed out for {}:{relative_path}: {}",
                        client.name,
                        error.detail()
                    ),
                )));
            }
        }
    }
}

fn wait_for_hotspot_helper_stat_until(
    context: &ScenarioContext,
    client: &ClientSpec,
    relative_path: &str,
    deadline: Instant,
    poll_interval: Duration,
    label: &str,
    active_workers: Arc<AtomicUsize>,
) -> Result<(), ScenarioError> {
    let mut last_error = None;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout for {}:{relative_path}; last error: {}",
                    client.name,
                    last_error.unwrap_or_else(|| "none".to_string())
                ),
            )));
        }

        let remaining = deadline.saturating_duration_since(now);
        match run_hotspot_mount_operation_with_timeout(
            context,
            client,
            "stat",
            relative_path,
            remaining,
            None,
            None,
            Arc::clone(&active_workers),
        ) {
            Ok(()) => return Ok(()),
            Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => {
                return Err(error);
            }
            Err(error) => {
                last_error = Some(error.to_string());
                thread::sleep(
                    poll_interval.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
        }
    }
}

fn remaining_initial_validation_timeout(
    deadline: Instant,
    label: &str,
) -> Result<Duration, ScenarioError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(ScenarioError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{label} became visible, but no time remained for convergence validation"),
        )))
    } else {
        Ok(remaining)
    }
}

fn missing_required_paths(
    clients: &[ClientSpec],
    required_paths: &[&str],
    probe_limiter: &MountedProbeLimiter,
) -> Vec<String> {
    let mut missing = Vec::new();
    for client in clients {
        for relative_path in required_paths {
            let client = client.clone();
            let relative_path = (*relative_path).to_string();
            let client_name = client.name.clone();
            let timeout_path = relative_path.clone();
            match run_mounted_probe_with_timeout(
                probe_limiter,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                move || missing_required_path_probe(client, relative_path),
            ) {
                Ok(Some(missing_path)) => missing.push(missing_path),
                Ok(None) => {}
                Err(error) => missing.push(format!(
                    "{}:{} ({})",
                    client_name,
                    timeout_path,
                    error.detail()
                )),
            }
        }
    }
    missing
}

fn client_path_missing(
    client: &ClientSpec,
    relative_path: &str,
    probe_limiter: &MountedProbeLimiter,
) -> Result<Option<String>, MountedProbeFailure> {
    let client = client.clone();
    let relative_path = relative_path.to_string();
    run_mounted_probe_with_timeout(probe_limiter, MOUNTED_VALIDATION_PROBE_TIMEOUT, move || {
        missing_required_path_probe(client, relative_path)
    })
}

fn missing_required_path_probe(client: ClientSpec, relative_path: String) -> Option<String> {
    if !refresh_relative_path_parent_contains_child(&client.mount_point, &relative_path) {
        return Some(format!("{}:{relative_path}", client.name));
    }
    let path = client.mount_point.join(&relative_path);
    match fs::metadata(&path) {
        Ok(_) => None,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Some(format!("{}:{relative_path}", client.name))
        }
        Err(error) => Some(format!("{}:{relative_path} ({error})", client.name)),
    }
}

struct AdvancedMountedFinishInput {
    bootstrap: Option<VolumeBootstrapSummary>,
    operations: Vec<NamespaceCollisionOperation>,
    lww_oracle: Option<LwwReconnectOracle>,
    sparse_ranges: Option<SparseRangeSummary>,
    cold_observer: Option<ColdObserverSummary>,
    long_history: Option<LongHistorySummary>,
    catchup: Option<CatchupSummary>,
    invalidation_dropout: Option<InvalidationDropoutSummary>,
    symlink_attributes: Option<SymlinkAttributeSummary>,
}

fn finish_advanced_mounted(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    isolation: Option<&ScenarioIsolation>,
    clients: &[ClientSpec],
    input: AdvancedMountedFinishInput,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    let AdvancedMountedFinishInput {
        bootstrap,
        operations,
        lww_oracle,
        sparse_ranges,
        cold_observer,
        long_history,
        catchup,
        invalidation_dropout,
        symlink_attributes,
    } = input;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        kind.convergence_timeout(),
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
    )?;
    let expected_absent_paths = expected_absent_paths_for(kind, isolation);
    let expected_absent_paths = expected_absent_paths
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut namespace_violations = Vec::new();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(
            snapshot,
            &expected_absent_paths,
        ));
    }
    if !namespace_violations.is_empty() {
        return Err(ScenarioError::NamespaceInvariantViolations {
            violations: namespace_violations,
        });
    }
    let summary = AdvancedMountedSummary {
        scenario: kind.name().to_string(),
        bootstrap,
        clients: clients.len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: convergence.comparison,
        namespace_violations,
        lww_oracle,
        sparse_ranges,
        cold_observer,
        long_history_validation: long_history
            .as_ref()
            .map(|history| validate_long_history_snapshots(&convergence.snapshots, history)),
        long_history,
        catchup,
        invalidation_dropout,
        symlink_attributes,
    };
    write_advanced_mounted_summary(context, kind, &summary)?;
    Ok(summary)
}

fn expected_absent_paths_for(
    kind: AdvancedMountedKind,
    isolation: Option<&ScenarioIsolation>,
) -> Vec<String> {
    match (kind, isolation) {
        (AdvancedMountedKind::OpenHandle, Some(isolation)) => {
            vec![open_handle_isolated_path(
                isolation,
                OPEN_HANDLE_UNLINK_TARGET,
            )]
        }
        (AdvancedMountedKind::OpenHandle, None) => vec![OPEN_HANDLE_UNLINK_TARGET.to_string()],
        (AdvancedMountedKind::ColdObserverLongHistory, Some(isolation)) => vec![
            isolation.path(COLD_OBSERVER_DELETE_TARGET),
            isolation.path(COLD_OBSERVER_RENAME_SOURCE),
        ],
        (AdvancedMountedKind::ColdObserverLongHistory, None) => vec![
            format!(
                "{}/{}",
                AdvancedMountedKind::ColdObserverLongHistory.path_root(),
                COLD_OBSERVER_DELETE_TARGET
            ),
            format!(
                "{}/{}",
                AdvancedMountedKind::ColdObserverLongHistory.path_root(),
                COLD_OBSERVER_RENAME_SOURCE
            ),
        ],
        (AdvancedMountedKind::StaleRemountColdDiscoverCatchup, Some(isolation)) => vec![
            isolation.path(COLD_OBSERVER_DELETE_TARGET),
            isolation.path(COLD_OBSERVER_RENAME_SOURCE),
        ],
        (AdvancedMountedKind::StaleRemountColdDiscoverCatchup, None) => vec![
            format!(
                "{}/{}",
                AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root(),
                COLD_OBSERVER_DELETE_TARGET
            ),
            format!(
                "{}/{}",
                AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root(),
                COLD_OBSERVER_RENAME_SOURCE
            ),
        ],
        (AdvancedMountedKind::InvalidationDropoutFallback, Some(isolation)) => {
            vec![isolation.path(INVALIDATION_DROPOUT_DELETE_TARGET)]
        }
        (AdvancedMountedKind::InvalidationDropoutFallback, None) => vec![format!(
            "{}/{}",
            AdvancedMountedKind::InvalidationDropoutFallback.path_root(),
            INVALIDATION_DROPOUT_DELETE_TARGET
        )],
        _ => Vec::new(),
    }
}

fn validate_long_history_snapshots(
    snapshots: &[crate::mounted_tree::MountedTreeSnapshot],
    history: &LongHistorySummary,
) -> LongHistoryValidation {
    let mut missing_present_paths = Vec::new();
    let mut unexpected_present_paths = Vec::new();
    let mut checksum_mismatches = Vec::new();
    let checksum_path = long_history_checksum_path(history);

    for snapshot in snapshots {
        for expected_path in &history.expected_present_paths {
            let Some(entry) = snapshot
                .entries
                .iter()
                .find(|entry| entry.path == *expected_path)
            else {
                missing_present_paths.push(format!("{}:{expected_path}", snapshot.client));
                continue;
            };
            if checksum_path.as_deref() == Some(expected_path.as_str())
                && entry.checksum_sha256.as_deref() != Some(history.final_checksum_sha256.as_str())
            {
                checksum_mismatches.push(format!(
                    "{}:{expected_path}: expected {}, got {}",
                    snapshot.client,
                    history.final_checksum_sha256,
                    entry.checksum_sha256.as_deref().unwrap_or("<missing>")
                ));
            }
        }
        for absent_path in &history.expected_absent_paths {
            if snapshot
                .entries
                .iter()
                .any(|entry| entry.path == *absent_path)
            {
                unexpected_present_paths.push(format!("{}:{absent_path}", snapshot.client));
            }
        }
    }

    LongHistoryValidation {
        matched: missing_present_paths.is_empty()
            && unexpected_present_paths.is_empty()
            && checksum_mismatches.is_empty(),
        missing_present_paths,
        unexpected_present_paths,
        checksum_mismatches,
    }
}

fn long_history_checksum_path(history: &LongHistorySummary) -> Option<String> {
    history
        .expected_present_paths
        .iter()
        .find(|path| path.ends_with(COLD_OBSERVER_OVERWRITE_TARGET))
        .cloned()
        .or_else(|| history.expected_present_paths.first().cloned())
}

fn planned_advanced_mounted_operations(
    kind: AdvancedMountedKind,
    started: Instant,
) -> Vec<NamespaceCollisionOperation> {
    let spec = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => vec![(
            "client-a",
            "before_write",
            "crash-before-write-acceptance/before-write.txt",
        )],
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => vec![(
            "client-a",
            "partial_write",
            "crash-after-partial-write-acceptance/partial-write.txt",
        )],
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => vec![(
            "client-a",
            "full_write",
            "crash-after-full-write-acceptance/full-write.txt",
        )],
        AdvancedMountedKind::MountedChaos => vec![
            ("client-a", "rename", "chaos/rename-source.txt"),
            ("client-b", "unlink", "chaos/unlink-target.txt"),
            ("client-c", "append", "chaos/append-target.txt"),
        ],
        AdvancedMountedKind::HotspotChurn => vec![
            ("client-a", "overwrite", "hotspot/slot-0.txt"),
            ("client-b", "append", "hotspot/slot-0.txt"),
            ("client-c", "readdir", "hotspot"),
        ],
        AdvancedMountedKind::FaultChaos => vec![
            ("client-a", "stop_postgres", "compose/postgres"),
            (
                "client-a",
                "local_write_during_pg_outage",
                "fault-chaos/postgres-outage.txt",
            ),
            ("client-b", "stop_rustfs", "compose/rustfs"),
            ("client-d", "cold_observer_readdir", "fault-chaos"),
        ],
        AdvancedMountedKind::LwwReconnectChaos => vec![
            ("client-c", "stale_writer_offline", "client-c"),
            ("client-b", "write", "lww-chaos/winner.txt"),
            ("client-c", "write_lww_winner", "lww-chaos/winner.txt"),
            ("client-e", "late_observer_read", "lww-chaos/winner.txt"),
        ],
        AdvancedMountedKind::OpenHandle => vec![
            ("client-a", "open", "open-handles/unlink-target.txt"),
            ("client-b", "unlink", "open-handles/unlink-target.txt"),
            ("client-b", "rename", "open-handles/rename-source.txt"),
            (
                "client-a",
                "append_old_handle",
                "open-handles/unlink-target.txt",
            ),
        ],
        AdvancedMountedKind::SparseRange => vec![
            ("client-a", "write_range", "sparse/shared.bin@0"),
            ("client-a", "write_range", "sparse/shared.bin@1048569"),
            ("client-b", "read_probe", "sparse/shared.bin@65536"),
            ("client-b", "truncate_shrink", "sparse/shared.bin@1048833"),
            ("client-c", "truncate_extend", "sparse/shared.bin@3146061+"),
            ("client-c", "write_range", "sparse/shared.bin@2097147"),
            ("client-a", "read_probe", "sparse/shared.bin@3145900"),
        ],
        AdvancedMountedKind::ColdObserverLongHistory => vec![
            ("client-a", "mkdir", "cold-observer/nested"),
            ("client-a", "create", "cold-observer/delete-target.txt"),
            ("client-b", "delete", "cold-observer/delete-target.txt"),
            ("client-a", "create", "cold-observer/rename-source.txt"),
            (
                "client-b",
                "rename",
                "cold-observer/rename-source.txt -> cold-observer/renamed-final.txt",
            ),
            ("client-c", "create", "cold-observer/overwrite-target.txt"),
            (
                "client-a",
                "overwrite",
                "cold-observer/overwrite-target.txt",
            ),
            ("client-b", "append", "cold-observer/append-target.txt"),
            ("client-c", "create", "cold-observer/nested/child.txt"),
            ("client-d", "cold_discover_after_history", "client-d"),
            ("client-d", "cold_mount_after_history", "client-d"),
        ],
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup => vec![
            (
                "client-c",
                "initial_baseline",
                "stale-remount-cold-discover/baseline.txt",
            ),
            ("client-c", "stale_client_unmount", "client-c"),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/delete-target.txt",
            ),
            (
                "client-a",
                "delete",
                "stale-remount-cold-discover/delete-target.txt",
            ),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/rename-source.txt",
            ),
            (
                "client-b",
                "rename",
                "stale-remount-cold-discover/rename-source.txt -> stale-remount-cold-discover/renamed-final.txt",
            ),
            (
                "client-a",
                "overwrite",
                "stale-remount-cold-discover/overwrite-target.txt",
            ),
            (
                "client-b",
                "append",
                "stale-remount-cold-discover/append-target.txt",
            ),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/nested/child.txt",
            ),
            ("client-c", "stale_remount_after_history", "client-c"),
            ("client-d", "cold_discover_after_history", "client-d"),
        ],
        AdvancedMountedKind::InvalidationDropoutFallback => vec![
            ("client-c", "observer_dropout_start", "client-c"),
            (
                "client-a",
                "writer_create",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-b",
                "writer_overwrite",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-a",
                "writer_delete",
                "invalidation-dropout/delete-target.txt",
            ),
            (
                "client-c",
                "observer_stale_window",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-c",
                "observer_delete_stale_window",
                "invalidation-dropout/delete-target.txt",
            ),
            (
                "client-c",
                "fallback_sync_resume",
                "invalidation-dropout/fallback-target.txt",
            ),
        ],
        AdvancedMountedKind::SymlinkAttribute => vec![
            (
                "client-a",
                "symlink_target_write",
                "symlink-attribute/target-v1.txt",
            ),
            (
                "client-a",
                "symlink_create",
                "symlink-attribute/link-to-target -> target-v1.txt",
            ),
            (
                "client-b",
                "symlink_rename",
                "symlink-attribute/link-to-target -> symlink-attribute/renamed-link",
            ),
            (
                "client-b",
                "symlink_target_update",
                "symlink-attribute/renamed-link -> target-v2.txt",
            ),
            (
                "client-c",
                "readonly_attribute_set",
                "symlink-attribute/readonly-attribute.txt",
            ),
            (
                "client-a",
                "near_symlink_write",
                "symlink-attribute/near-symlink-write.txt",
            ),
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

fn planned_symlink_attribute_summary() -> SymlinkAttributeSummary {
    SymlinkAttributeSummary {
        symlink_supported: true,
        symlink_skip_reason: None,
        readonly_attribute_supported: true,
        readonly_attribute_skip_reason: None,
        final_symlink_path: format!(
            "{}/{}",
            AdvancedMountedKind::SymlinkAttribute.path_root(),
            SYMLINK_ATTRIBUTE_RENAMED_LINK
        ),
        final_symlink_target: SYMLINK_ATTRIBUTE_TARGET_V2.to_string(),
        readonly_path: format!(
            "{}/{}",
            AdvancedMountedKind::SymlinkAttribute.path_root(),
            SYMLINK_ATTRIBUTE_READONLY_FILE
        ),
        readonly_observed_clients: SYMLINK_ATTRIBUTE_CLIENT_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

struct SparseRangeSpec {
    label: &'static str,
    offset: u64,
    payload: Vec<u8>,
}

struct SparseHoleSpec {
    offset: u64,
    len: usize,
}

fn invalidation_dropout_observer_env_overrides() -> Vec<(String, String)> {
    vec![
        (
            "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL".to_string(),
            "1".to_string(),
        ),
        (
            "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS".to_string(),
            INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS.to_string(),
        ),
    ]
}

fn run_invalidation_dropout_fallback_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, InvalidationDropoutSummary), ScenarioError> {
    let started = Instant::now();
    let writer_a = advanced_client_by_name(clients, "client-a")?;
    let writer_b = advanced_client_by_name(clients, "client-b")?;
    let observer = advanced_client_by_name(clients, INVALIDATION_DROPOUT_OBSERVER_CLIENT)?;
    let writer_clients = clients
        .iter()
        .filter(|client| INVALIDATION_DROPOUT_WRITER_CLIENTS.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut operations = Vec::new();

    let baseline_path = isolation.path(INVALIDATION_DROPOUT_BASELINE_FILE);
    let target_path = isolation.path(INVALIDATION_DROPOUT_TARGET_FILE);
    let delete_path = isolation.path(INVALIDATION_DROPOUT_DELETE_TARGET);
    let baseline_payload = isolation.payload("baseline", &baseline_path, "baseline visible");
    let create_payload = isolation.payload("writer_create", &target_path, "created before dropout");
    let final_payload = isolation.payload("writer_overwrite", &target_path, "fallback final");
    let delete_payload = isolation.payload("writer_delete", &delete_path, "delete before fallback");

    write_mounted_file_with_readback(
        writer_a.mount_point.join(&baseline_path),
        &baseline_payload,
        "invalidation dropout baseline",
        true,
    )?;
    write_mounted_file_with_readback(
        writer_a.mount_point.join(&delete_path),
        &delete_payload,
        "invalidation dropout delete baseline",
        true,
    )?;
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&baseline_path, baseline_payload.as_slice()),
            (&delete_path, delete_payload.as_slice()),
        ],
        Instant::now() + timeout,
        poll_interval,
        "invalidation dropout baseline",
    )?;

    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_dropout_start",
        INVALIDATION_DROPOUT_OBSERVER_CLIENT,
        Ok(()),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_a,
        "writer_create",
        &target_path,
        write_mounted_file_with_readback(
            writer_a.mount_point.join(&target_path),
            &create_payload,
            "invalidation dropout writer create",
            true,
        ),
    )?);
    wait_for_mounted_initial_payloads_until(
        &writer_clients,
        &[(&target_path, create_payload.as_slice())],
        Instant::now() + timeout,
        poll_interval,
        "invalidation dropout writer create",
    )?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_b,
        "writer_overwrite",
        &target_path,
        write_mounted_file_with_readback(
            writer_b.mount_point.join(&target_path),
            &final_payload,
            "invalidation dropout writer overwrite",
            false,
        ),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_a,
        "writer_delete",
        &delete_path,
        remove_mounted_file(writer_a.mount_point.join(&delete_path)),
    )?);

    let stale_window_observed = assert_observer_stays_stale_for_window(
        observer,
        &target_path,
        &final_payload,
        Duration::from_millis(INVALIDATION_DROPOUT_STALE_WINDOW_MS),
        poll_interval,
    )
    .is_ok();
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_stale_window",
        &target_path,
        stale_window_observed.then_some(()).ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "observer saw final payload before stale window elapsed",
            ))
        }),
    )?);
    let delete_stale_window_observed = assert_observer_keeps_payload_for_window(
        observer,
        &delete_path,
        &delete_payload,
        Duration::from_millis(INVALIDATION_DROPOUT_STALE_WINDOW_MS),
        poll_interval,
    )
    .is_ok();
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_delete_stale_window",
        &delete_path,
        delete_stale_window_observed.then_some(()).ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "observer lost deleted path before stale window elapsed",
            ))
        }),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "fallback_sync_resume",
        &target_path,
        wait_for_invalidation_fallback_resume(
            clients,
            &target_path,
            final_payload.as_slice(),
            &delete_path,
            timeout,
            poll_interval,
        ),
    )?);

    let summary = InvalidationDropoutSummary {
        observer_client: observer.name.clone(),
        writer_clients: INVALIDATION_DROPOUT_WRITER_CLIENTS
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        notify_suppressed: true,
        fallback_interval_ms: INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS,
        stale_window_ms: INVALIDATION_DROPOUT_STALE_WINDOW_MS,
        stale_window_observed,
        delete_stale_window_observed,
        fallback_resumed: true,
        final_observer_path: target_path,
        final_observer_checksum_sha256: bytes_sha256(&final_payload),
        final_validation_result: "mounted_tree_convergence".to_string(),
    };
    write_invalidation_dropout_diagnostics(context, &summary)?;
    Ok((operations, summary))
}

fn advanced_client_by_name<'a>(
    clients: &'a [ClientSpec],
    name: &str,
) -> Result<&'a ClientSpec, ScenarioError> {
    clients
        .iter()
        .find(|client| client.name == name)
        .ok_or_else(|| ScenarioError::Io(io::Error::other(format!("missing client {name}"))))
}

fn run_symlink_attribute_operations(
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, SymlinkAttributeSummary), ScenarioError> {
    let started = Instant::now();
    let writer = &clients[0];
    let renamer = &clients[1];
    let attr_writer = &clients[2];
    let mut operations = Vec::new();

    let target_v1 = isolation.path(SYMLINK_ATTRIBUTE_TARGET_V1);
    let target_v2 = isolation.path(SYMLINK_ATTRIBUTE_TARGET_V2);
    let link = isolation.path(SYMLINK_ATTRIBUTE_LINK);
    let renamed_link = isolation.path(SYMLINK_ATTRIBUTE_RENAMED_LINK);
    let readonly_path = isolation.path(SYMLINK_ATTRIBUTE_READONLY_FILE);
    let nearby_path = isolation.path(SYMLINK_ATTRIBUTE_NEARBY_FILE);

    let target_v1_payload = isolation.payload("target_v1", &target_v1, "symlink target v1");
    let target_v2_payload = isolation.payload("target_v2", &target_v2, "symlink target v2");
    let readonly_payload = isolation.payload("readonly", &readonly_path, "readonly metadata attr");
    let nearby_payload = isolation.payload("nearby", &nearby_path, "write near symlink path");

    operations.push(required_op(
        started,
        operations.len() + 1,
        writer,
        "symlink_target_write",
        &target_v1,
        write_mounted_file_without_barrier_with_readback(
            writer.mount_point.join(&target_v1),
            &target_v1_payload,
            "symlink target v1",
        ),
    )?);

    let link_result =
        create_mounted_file_symlink(writer.mount_point.join(&link), SYMLINK_ATTRIBUTE_TARGET_V1);
    let (symlink_supported, symlink_skip_reason) = match link_result {
        Ok(()) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                writer,
                "symlink_create",
                &format!("{link} -> {}", SYMLINK_ATTRIBUTE_TARGET_V1),
                Ok(()),
            )?);
            wait_for_symlink_target_until(
                clients,
                &link,
                SYMLINK_ATTRIBUTE_TARGET_V1,
                Instant::now() + timeout,
                poll_interval,
            )?;
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_rename",
                &format!("{link} -> {renamed_link}"),
                retry_mounted_io(
                    || {
                        fs::rename(
                            renamer.mount_point.join(&link),
                            renamer.mount_point.join(&renamed_link),
                        )
                    },
                    MOUNTED_WRITE_READBACK_TIMEOUT,
                    MOUNTED_WRITE_READBACK_POLL_INTERVAL,
                )
                .map_err(ScenarioError::from),
            )?);
            wait_for_symlink_target_until(
                clients,
                &renamed_link,
                SYMLINK_ATTRIBUTE_TARGET_V1,
                Instant::now() + timeout,
                poll_interval,
            )?;
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_target_replacement_write",
                &target_v2,
                write_mounted_file_without_barrier_with_readback(
                    renamer.mount_point.join(&target_v2),
                    &target_v2_payload,
                    "symlink target v2",
                ),
            )?);
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_target_update",
                &format!("{renamed_link} -> {}", SYMLINK_ATTRIBUTE_TARGET_V2),
                replace_mounted_file_symlink(
                    renamer.mount_point.join(&renamed_link),
                    SYMLINK_ATTRIBUTE_TARGET_V2,
                ),
            )?);
            wait_for_symlink_target_until(
                clients,
                &renamed_link,
                SYMLINK_ATTRIBUTE_TARGET_V2,
                Instant::now() + timeout,
                poll_interval,
            )?;
            (true, None)
        }
        Err(error) if is_supported_symlink_skip_error(&error) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                writer,
                "symlink_create_skip",
                &link,
                Ok(()),
            )?);
            (false, Some(error.to_string()))
        }
        Err(error) => return Err(ScenarioError::Io(error)),
    };

    operations.push(required_op(
        started,
        operations.len() + 1,
        attr_writer,
        "readonly_attribute_target_write",
        &readonly_path,
        write_mounted_file_without_barrier_with_readback(
            attr_writer.mount_point.join(&readonly_path),
            &readonly_payload,
            "readonly attribute target",
        ),
    )?);
    let readonly_result = set_mounted_readonly(attr_writer.mount_point.join(&readonly_path));
    let (readonly_attribute_supported, readonly_attribute_skip_reason) = match readonly_result {
        Ok(()) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                attr_writer,
                "readonly_attribute_set",
                &readonly_path,
                Ok(()),
            )?);
            (true, None)
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
            ) =>
        {
            operations.push(required_op(
                started,
                operations.len() + 1,
                attr_writer,
                "readonly_attribute_skip",
                &readonly_path,
                Ok(()),
            )?);
            (false, Some(error.to_string()))
        }
        Err(error) => return Err(ScenarioError::Io(error)),
    };

    operations.push(required_op(
        started,
        operations.len() + 1,
        writer,
        "near_symlink_write",
        &nearby_path,
        write_mounted_file_without_barrier_with_readback(
            writer.mount_point.join(&nearby_path),
            &nearby_payload,
            "near symlink write",
        ),
    )?);

    let readonly_observed_clients = if readonly_attribute_supported {
        wait_for_readonly_attribute_until(
            clients,
            &readonly_path,
            Instant::now() + timeout,
            poll_interval,
        )?
    } else {
        Vec::new()
    };

    Ok((
        operations,
        SymlinkAttributeSummary {
            symlink_supported,
            symlink_skip_reason,
            readonly_attribute_supported,
            readonly_attribute_skip_reason,
            final_symlink_path: renamed_link,
            final_symlink_target: SYMLINK_ATTRIBUTE_TARGET_V2.to_string(),
            readonly_path,
            readonly_observed_clients,
            final_validation_result: "mounted_tree_convergence".to_string(),
        },
    ))
}

fn write_mounted_file_with_readback(
    path: PathBuf,
    payload: &[u8],
    label: &str,
    create_new: bool,
) -> Result<(), ScenarioError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = mounted_write_barrier_open_options();
    options.create(true).truncate(true);
    if create_new {
        options.create_new(true);
    }
    let mut file = options.open(&path)?;
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
        return Err(ScenarioError::Io(io::Error::other(format!(
            "{label} write readback mismatch for {}: expected {} bytes, read {} bytes",
            path.display(),
            payload.len(),
            readback.len()
        ))));
    }
    Ok(())
}

fn write_mounted_file_without_barrier_with_readback(
    path: PathBuf,
    payload: &[u8],
    label: &str,
) -> Result<(), ScenarioError> {
    retry_mounted_io(
        || write_parented(path.clone(), payload),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(io::Error::other(format!(
            "{label} write readback mismatch for {}: expected {} bytes, read {} bytes",
            path.display(),
            payload.len(),
            readback.len()
        ))));
    }
    Ok(())
}

fn remove_mounted_file(path: PathBuf) -> Result<(), ScenarioError> {
    fs::remove_file(path)?;
    Ok(())
}

fn replace_mounted_file_symlink(path: PathBuf, target: &str) -> Result<(), ScenarioError> {
    retry_mounted_io(
        || fs::remove_file(&path),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    create_mounted_file_symlink(path, target)?;
    Ok(())
}

fn create_mounted_file_symlink(path: PathBuf, target: &str) -> io::Result<()> {
    retry_mounted_io(
        || {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            create_platform_file_symlink(Path::new(target), &path)
        },
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
}

#[cfg(windows)]
fn create_platform_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(unix)]
fn create_platform_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

fn set_mounted_readonly(path: PathBuf) -> io::Result<()> {
    retry_mounted_io(
        || {
            let mut permissions = fs::metadata(&path)?.permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&path, permissions)
        },
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
}

fn retry_mounted_io(
    mut action: impl FnMut() -> io::Result<()>,
    timeout: Duration,
    poll_interval: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match action() {
            Ok(()) => return Ok(()),
            Err(error) if is_transient_mounted_io_error(&error) && Instant::now() < deadline => {
                thread::sleep(poll_interval);
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_transient_mounted_io_error(error: &io::Error) -> bool {
    #[cfg(windows)]
    {
        const ERROR_GEN_FAILURE: i32 = 31;
        error.raw_os_error() == Some(ERROR_GEN_FAILURE)
    }
    #[cfg(not(windows))]
    {
        let _ = error;
        false
    }
}

fn is_supported_symlink_skip_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
    ) || is_transient_mounted_io_error(error)
}

fn wait_for_symlink_target_until(
    clients: &[ClientSpec],
    relative_path: &str,
    expected_target: &str,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    loop {
        let mut mismatches = Vec::new();
        for client in clients {
            match fs::read_link(client.mount_point.join(relative_path)) {
                Ok(target) if target == Path::new(expected_target) => {}
                Ok(target) => mismatches.push(format!(
                    "{}:{} expected {}, got {}",
                    client.name,
                    relative_path,
                    expected_target,
                    target.display()
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    mismatches.push(format!("{}:{relative_path} missing", client.name));
                }
                Err(error) => {
                    mismatches.push(format!("{}:{relative_path} ({error})", client.name));
                }
            }
        }
        if mismatches.is_empty() {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "symlink target did not converge for {relative_path}: {mismatches:?}"
            ))));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

fn wait_for_readonly_attribute_until(
    clients: &[ClientSpec],
    relative_path: &str,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<Vec<String>, ScenarioError> {
    loop {
        let mut observed = Vec::new();
        let mut pending = Vec::new();
        for client in clients {
            match fs::metadata(client.mount_point.join(relative_path)) {
                Ok(metadata) if metadata.permissions().readonly() => {
                    observed.push(client.name.clone());
                }
                Ok(_) => pending.push(format!("{}:{relative_path} not readonly", client.name)),
                Err(error) => pending.push(format!("{}:{relative_path} ({error})", client.name)),
            }
        }
        if observed.len() == clients.len() {
            return Ok(observed);
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "readonly attribute did not converge for {relative_path}: {pending:?}"
            ))));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

fn wait_for_invalidation_fallback_resume(
    clients: &[ClientSpec],
    target_path: &str,
    final_payload: &[u8],
    delete_path: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    wait_for_mounted_initial_payloads_until(
        clients,
        &[(target_path, final_payload)],
        deadline,
        poll_interval,
        "invalidation dropout fallback payload sync",
    )?;
    wait_for_mounted_paths_absent_until(
        clients,
        &[delete_path],
        deadline,
        poll_interval,
        "invalidation dropout fallback delete sync",
    )
}

fn assert_observer_stays_stale_for_window(
    observer: &ClientSpec,
    relative_path: &str,
    final_payload: &[u8],
    stale_window: Duration,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + stale_window;
    loop {
        if observer_sees_payload(observer, relative_path, final_payload)? {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "{} observed final payload for {relative_path} before the {} ms stale window elapsed",
                observer.name,
                stale_window.as_millis()
            ))));
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

fn assert_observer_keeps_payload_for_window(
    observer: &ClientSpec,
    relative_path: &str,
    expected_payload: &[u8],
    stale_window: Duration,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + stale_window;
    loop {
        if !observer_sees_payload(observer, relative_path, expected_payload)? {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "{} stopped seeing stale payload for {relative_path} before the {} ms stale window elapsed",
                observer.name,
                stale_window.as_millis()
            ))));
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

fn observer_sees_payload(
    observer: &ClientSpec,
    relative_path: &str,
    expected_payload: &[u8],
) -> Result<bool, ScenarioError> {
    let _ = refresh_relative_path_parent_contains_child(&observer.mount_point, relative_path);
    match fs::read(observer.mount_point.join(relative_path)) {
        Ok(payload) => Ok(payload == expected_payload),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn sparse_expected_range_specs(
    isolation: Option<(&ScenarioIsolation, &str)>,
) -> Vec<SparseRangeSpec> {
    [
        SparseRangeSpec {
            label: "prefix",
            offset: 0,
            payload: sparse_range_payload(isolation, "prefix", SPARSE_PREFIX_PAYLOAD),
        },
        SparseRangeSpec {
            label: "first_chunk_boundary",
            offset: SPARSE_FIRST_BOUNDARY_OFFSET,
            payload: sparse_range_payload(
                isolation,
                "first_chunk_boundary",
                SPARSE_FIRST_BOUNDARY_PAYLOAD,
            ),
        },
        SparseRangeSpec {
            label: "second_chunk_boundary",
            offset: SPARSE_SECOND_BOUNDARY_OFFSET,
            payload: sparse_range_payload(
                isolation,
                "second_chunk_boundary",
                SPARSE_SECOND_BOUNDARY_PAYLOAD,
            ),
        },
        SparseRangeSpec {
            label: "final_tail",
            offset: SPARSE_FINAL_TAIL_OFFSET,
            payload: sparse_range_payload(isolation, "final_tail", SPARSE_FINAL_TAIL_PAYLOAD),
        },
    ]
    .into_iter()
    .collect()
}

fn sparse_range_payload(
    isolation: Option<(&ScenarioIsolation, &str)>,
    label: &str,
    payload: &[u8],
) -> Vec<u8> {
    match isolation {
        Some((isolation, logical_path)) => {
            isolation.payload(label, logical_path, &String::from_utf8_lossy(payload))
        }
        None => payload.to_vec(),
    }
}

fn sparse_hole_specs() -> &'static [SparseHoleSpec] {
    &[
        SparseHoleSpec {
            offset: 65_536,
            len: 4_096,
        },
        SparseHoleSpec {
            offset: 1_572_864,
            len: 4_096,
        },
    ]
}

fn sparse_logical_size(base_logical_size: u64, ranges: &[SparseRangeSpec]) -> u64 {
    ranges.iter().fold(base_logical_size, |size, range| {
        size.max(range.offset + range.payload.len() as u64)
    })
}

fn sparse_final_logical_size(ranges: &[SparseRangeSpec]) -> u64 {
    sparse_logical_size(SPARSE_FINAL_LOGICAL_SIZE, ranges)
}

fn run_sparse_range_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, SparseRangeSummary), ScenarioError> {
    let started = Instant::now();
    let writer = &clients[0];
    let reader = &clients[1];
    let extender = &clients[2];
    let mut operations = Vec::new();
    let sparse_path = isolation.path(
        SPARSE_FILE_PATH
            .strip_prefix("sparse/")
            .unwrap_or(SPARSE_FILE_PATH),
    );
    let sparse_ranges = sparse_expected_range_specs(Some((isolation, &sparse_path)));
    let initial_logical_size =
        sparse_logical_size(SPARSE_INITIAL_LOGICAL_SIZE, &sparse_ranges[..2]);
    let shrunk_logical_size = sparse_logical_size(SPARSE_SHRUNK_LOGICAL_SIZE, &sparse_ranges[..2]);
    let final_logical_size = sparse_final_logical_size(&sparse_ranges);

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            1,
            writer,
            "write_range",
            &format!("{sparse_path}@0"),
            write_sparse_file_range(
                writer.mount_point.join(&sparse_path),
                0,
                &sparse_ranges[0].payload,
                true,
            )
            .map_err(ScenarioError::from),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            2,
            writer,
            "write_range",
            &format!("{sparse_path}@1048569"),
            write_sparse_file_range(
                writer.mount_point.join(&sparse_path),
                SPARSE_FIRST_BOUNDARY_OFFSET,
                &sparse_ranges[1].payload,
                false,
            )
            .map_err(ScenarioError::from),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            3,
            writer,
            "truncate_extend",
            &format!("{sparse_path}@{initial_logical_size}"),
            truncate_sparse_file(writer.mount_point.join(&sparse_path), initial_logical_size)
                .map_err(ScenarioError::from),
        )?,
    )?;
    wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: initial_logical_size,
        expected_ranges: &sparse_ranges[..2],
        hole_specs: &[],
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "initial sparse ranges",
    })?;

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            4,
            reader,
            "read_probe",
            &format!("{sparse_path}@65536"),
            verify_sparse_hole(reader.mount_point.join(&sparse_path), 65_536, 4_096),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            5,
            reader,
            "truncate_shrink",
            &format!("{sparse_path}@{shrunk_logical_size}"),
            truncate_sparse_file(reader.mount_point.join(&sparse_path), shrunk_logical_size)
                .map_err(ScenarioError::from),
        )?,
    )?;
    wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: shrunk_logical_size,
        expected_ranges: &sparse_ranges[..2],
        hole_specs: &[],
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "shrunk sparse ranges",
    })?;

    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            6,
            extender,
            "truncate_extend",
            &format!("{sparse_path}@{final_logical_size}"),
            truncate_sparse_file(extender.mount_point.join(&sparse_path), final_logical_size)
                .map_err(ScenarioError::from),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            7,
            extender,
            "write_range",
            &format!("{sparse_path}@2097147"),
            write_sparse_file_range(
                extender.mount_point.join(&sparse_path),
                SPARSE_SECOND_BOUNDARY_OFFSET,
                &sparse_ranges[2].payload,
                false,
            )
            .map_err(ScenarioError::from),
        )?,
    )?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            8,
            writer,
            "write_range",
            &format!("{sparse_path}@3145900"),
            write_sparse_file_range(
                writer.mount_point.join(&sparse_path),
                SPARSE_FINAL_TAIL_OFFSET,
                &sparse_ranges[3].payload,
                false,
            )
            .map_err(ScenarioError::from),
        )?,
    )?;

    let (read_probes, zero_filled_holes) = wait_for_sparse_expected_until(SparseExpectedWait {
        context,
        clients,
        sparse_path: &sparse_path,
        logical_size: final_logical_size,
        expected_ranges: &sparse_ranges,
        hole_specs: sparse_hole_specs(),
        deadline: Instant::now() + timeout,
        poll_interval,
        label: "final sparse ranges",
    })?;
    record_sparse_operation(
        context,
        &mut operations,
        required_op(
            started,
            9,
            writer,
            "read_probe",
            &format!("{sparse_path}@3145900"),
            verify_sparse_range(
                writer.mount_point.join(&sparse_path),
                SPARSE_FINAL_TAIL_OFFSET,
                &sparse_ranges[3].payload,
            ),
        )?,
    )?;

    let final_payload = fs::read(writer.mount_point.join(&sparse_path))?;
    let expected_payload = expected_sparse_payload(&sparse_ranges, final_logical_size);
    if final_payload != expected_payload {
        return Err(ScenarioError::Io(io::Error::other(format!(
            "sparse final payload mismatch: expected checksum {}, got {}",
            bytes_sha256(&expected_payload),
            bytes_sha256(&final_payload)
        ))));
    }

    let summary = SparseRangeSummary {
        relative_path: sparse_path,
        logical_size: final_logical_size,
        final_checksum_sha256: bytes_sha256(&final_payload),
        expected_byte_ranges: sparse_expected_ranges(&sparse_ranges),
        zero_filled_holes,
        read_probes,
    };
    Ok((operations, summary))
}

struct SparseExpectedWait<'a> {
    context: &'a ScenarioContext,
    clients: &'a [ClientSpec],
    sparse_path: &'a str,
    logical_size: u64,
    expected_ranges: &'a [SparseRangeSpec],
    hole_specs: &'a [SparseHoleSpec],
    deadline: Instant,
    poll_interval: Duration,
    label: &'a str,
}

fn wait_for_sparse_expected_until(
    wait: SparseExpectedWait<'_>,
) -> Result<(Vec<SparseReadProbe>, Vec<SparseHoleProbe>), ScenarioError> {
    let wait_started = Instant::now();
    let SparseExpectedWait {
        context,
        clients,
        sparse_path,
        logical_size,
        expected_ranges,
        hole_specs,
        deadline,
        poll_interval,
        label,
    } = wait;
    loop {
        let observation = observe_sparse_ranges(
            clients,
            sparse_path,
            logical_size,
            expected_ranges,
            hole_specs,
        );
        if observation.missing.is_empty() {
            return Ok((observation.read_probes, observation.zero_filled_holes));
        }

        let now = Instant::now();
        if now >= deadline {
            let artifact = write_sparse_timeout_diagnostics(
                context,
                SparseTimeoutDiagnosticsInput {
                    label,
                    sparse_path,
                    logical_size,
                    expected_ranges,
                    hole_specs,
                    poll_interval,
                    elapsed: wait_started.elapsed(),
                    observation: &observation,
                },
            );
            let artifact_note = match artifact {
                Ok(artifact) => format!(
                    "; diagnostics: artifacts/scenarios/{}/{}",
                    AdvancedMountedKind::SparseRange.name(),
                    artifact
                ),
                Err(error) => format!("; diagnostics write failed: {error}"),
            };
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "{label} did not become visible before timeout; missing: {}{}",
                    observation.missing.join(", "),
                    artifact_note
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

#[derive(Debug, Serialize)]
struct SparseTimeoutDiagnostics {
    schema_version: u32,
    run_id: String,
    scenario: String,
    label: String,
    sparse_path: String,
    logical_size: u64,
    elapsed_ms: u64,
    poll_interval_ms: u64,
    missing: Vec<String>,
    expected_ranges: Vec<SparseTimeoutRangeSpec>,
    expected_holes: Vec<SparseTimeoutHoleSpec>,
    environment: Vec<SparseEnvironmentValue>,
    clients: Vec<SparseClientObservation>,
}

#[derive(Debug, Serialize)]
struct SparseTimeoutRangeSpec {
    label: String,
    offset: u64,
    len: usize,
    expected_checksum_sha256: String,
}

#[derive(Debug, Serialize)]
struct SparseTimeoutHoleSpec {
    offset: u64,
    len: usize,
}

#[derive(Debug, Serialize)]
struct SparseEnvironmentValue {
    name: &'static str,
    value: Option<String>,
}

struct SparseTimeoutDiagnosticsInput<'a> {
    label: &'a str,
    sparse_path: &'a str,
    logical_size: u64,
    expected_ranges: &'a [SparseRangeSpec],
    hole_specs: &'a [SparseHoleSpec],
    poll_interval: Duration,
    elapsed: Duration,
    observation: &'a SparseObservation,
}

struct SparseObservation {
    read_probes: Vec<SparseReadProbe>,
    zero_filled_holes: Vec<SparseHoleProbe>,
    missing: Vec<String>,
    clients: Vec<SparseClientObservation>,
}

#[derive(Debug, Clone, Serialize)]
struct SparseClientObservation {
    client: String,
    mount_point: String,
    path: String,
    parent_visible: bool,
    metadata: SparseMetadataObservation,
    open: SparseOpenObservation,
    ranges: Vec<SparseRangeObservation>,
    holes: Vec<SparseHoleObservation>,
}

#[derive(Debug, Clone, Serialize)]
struct SparseMetadataObservation {
    ok: bool,
    len: Option<u64>,
    is_file: Option<bool>,
    error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
struct SparseOpenObservation {
    ok: bool,
    error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
struct SparseRangeObservation {
    label: String,
    offset: u64,
    len: usize,
    expected_checksum_sha256: String,
    actual_checksum_sha256: Option<String>,
    matched: Option<bool>,
    error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
struct SparseHoleObservation {
    offset: u64,
    len: usize,
    all_zero: Option<bool>,
    error: Option<SparseIoErrorObservation>,
}

#[derive(Debug, Clone, Serialize)]
struct SparseIoErrorObservation {
    kind: String,
    message: String,
}

fn observe_sparse_ranges(
    clients: &[ClientSpec],
    sparse_path: &str,
    logical_size: u64,
    expected_ranges: &[SparseRangeSpec],
    hole_specs: &[SparseHoleSpec],
) -> SparseObservation {
    let mut read_probes = Vec::new();
    let mut hole_results = Vec::new();
    let mut missing = Vec::new();
    let mut client_observations = Vec::new();

    for client in clients {
        let path = client.mount_point.join(sparse_path);
        refresh_relative_path_ancestors(&client.mount_point, sparse_path);
        let mut client_observation = SparseClientObservation {
            client: client.name.clone(),
            mount_point: client.mount_point.display().to_string(),
            path: path.display().to_string(),
            parent_visible: refresh_relative_path_parent_contains_child(
                &client.mount_point,
                sparse_path,
            ),
            metadata: SparseMetadataObservation {
                ok: false,
                len: None,
                is_file: None,
                error: None,
            },
            open: SparseOpenObservation {
                ok: false,
                error: None,
            },
            ranges: Vec::new(),
            holes: Vec::new(),
        };
        match fs::metadata(&path) {
            Ok(metadata) if metadata.len() == logical_size => {
                client_observation.metadata = SparseMetadataObservation {
                    ok: true,
                    len: Some(metadata.len()),
                    is_file: Some(metadata.is_file()),
                    error: None,
                };
            }
            Ok(metadata) => {
                client_observation.metadata = SparseMetadataObservation {
                    ok: true,
                    len: Some(metadata.len()),
                    is_file: Some(metadata.is_file()),
                    error: None,
                };
                missing.push(format!(
                    "{}:{sparse_path} logical size expected {logical_size}, got {}",
                    client.name,
                    metadata.len()
                ));
            }
            Err(error) => {
                client_observation.metadata.error = Some(sparse_io_error_observation(&error));
                missing.push(format!("{}:{sparse_path} ({error})", client.name));
                client_observations.push(client_observation);
                continue;
            }
        }

        let mut file = match fs::File::open(&path) {
            Ok(file) => {
                client_observation.open = SparseOpenObservation {
                    ok: true,
                    error: None,
                };
                file
            }
            Err(error) => {
                client_observation.open.error = Some(sparse_io_error_observation(&error));
                missing.push(format!("{}:{sparse_path} ({error})", client.name));
                client_observations.push(client_observation);
                continue;
            }
        };

        for range in expected_ranges {
            match read_exact_range_from(&mut file, range.offset, range.payload.len()) {
                Ok(bytes) => {
                    let expected_checksum = bytes_sha256(&range.payload);
                    let actual_checksum = bytes_sha256(&bytes);
                    let matched = bytes.as_slice() == range.payload.as_slice();
                    if !matched {
                        missing.push(format!(
                            "{}:{sparse_path}@{} expected {}, got {}",
                            client.name, range.offset, expected_checksum, actual_checksum
                        ));
                    }
                    client_observation.ranges.push(SparseRangeObservation {
                        label: range.label.to_string(),
                        offset: range.offset,
                        len: range.payload.len(),
                        expected_checksum_sha256: expected_checksum,
                        actual_checksum_sha256: Some(actual_checksum.clone()),
                        matched: Some(matched),
                        error: None,
                    });
                    read_probes.push(SparseReadProbe {
                        client: client.name.clone(),
                        offset: range.offset,
                        len: range.payload.len(),
                        actual_checksum_sha256: actual_checksum,
                        matched,
                    });
                }
                Err(error) => {
                    client_observation.ranges.push(SparseRangeObservation {
                        label: range.label.to_string(),
                        offset: range.offset,
                        len: range.payload.len(),
                        expected_checksum_sha256: bytes_sha256(&range.payload),
                        actual_checksum_sha256: None,
                        matched: None,
                        error: Some(sparse_io_error_observation(&error)),
                    });
                    missing.push(format!(
                        "{}:{sparse_path}@{} ({error})",
                        client.name, range.offset
                    ));
                }
            }
        }

        for hole in hole_specs {
            match read_exact_range_from(&mut file, hole.offset, hole.len) {
                Ok(bytes) => {
                    let all_zero = bytes.iter().all(|byte| *byte == 0);
                    if !all_zero {
                        missing.push(format!(
                            "{}:{sparse_path}@{} expected zero-filled hole",
                            client.name, hole.offset
                        ));
                    }
                    client_observation.holes.push(SparseHoleObservation {
                        offset: hole.offset,
                        len: hole.len,
                        all_zero: Some(all_zero),
                        error: None,
                    });
                    hole_results.push(SparseHoleProbe {
                        client: client.name.clone(),
                        offset: hole.offset,
                        len: hole.len,
                        all_zero,
                    });
                }
                Err(error) => {
                    client_observation.holes.push(SparseHoleObservation {
                        offset: hole.offset,
                        len: hole.len,
                        all_zero: None,
                        error: Some(sparse_io_error_observation(&error)),
                    });
                    missing.push(format!(
                        "{}:{sparse_path}@{} ({error})",
                        client.name, hole.offset
                    ));
                }
            }
        }
        client_observations.push(client_observation);
    }

    SparseObservation {
        read_probes,
        zero_filled_holes: hole_results,
        missing,
        clients: client_observations,
    }
}

fn write_sparse_timeout_diagnostics(
    context: &ScenarioContext,
    input: SparseTimeoutDiagnosticsInput<'_>,
) -> Result<String, ScenarioError> {
    let artifact = sparse_timeout_diagnostics_file_name(input.label);
    let diagnostics = SparseTimeoutDiagnostics {
        schema_version: 1,
        run_id: context.run_id.clone(),
        scenario: AdvancedMountedKind::SparseRange.name().to_string(),
        label: input.label.to_string(),
        sparse_path: input.sparse_path.to_string(),
        logical_size: input.logical_size,
        elapsed_ms: duration_millis_u64(input.elapsed),
        poll_interval_ms: duration_millis_u64(input.poll_interval),
        missing: input.observation.missing.clone(),
        expected_ranges: input
            .expected_ranges
            .iter()
            .map(|range| SparseTimeoutRangeSpec {
                label: range.label.to_string(),
                offset: range.offset,
                len: range.payload.len(),
                expected_checksum_sha256: bytes_sha256(&range.payload),
            })
            .collect(),
        expected_holes: input
            .hole_specs
            .iter()
            .map(|hole| SparseTimeoutHoleSpec {
                offset: hole.offset,
                len: hole.len,
            })
            .collect(),
        environment: sparse_timeout_environment(),
        clients: input.observation.clients.clone(),
    };
    fs::write(
        context.scenario_dir.join(&artifact),
        serde_json::to_vec_pretty(&diagnostics)?,
    )?;
    append_sparse_timeout_diagnostics_event(context, input.label, &artifact, &diagnostics)?;
    Ok(artifact)
}

fn append_sparse_timeout_diagnostics_event(
    context: &ScenarioContext,
    label: &str,
    artifact: &str,
    diagnostics: &SparseTimeoutDiagnostics,
) -> Result<(), ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    serde_json::to_writer(
        &mut log,
        &json!({
            "run_id": context.run_id,
            "scenario": AdvancedMountedKind::SparseRange.name(),
            "operation": "sparse_timeout_diagnostics",
            "label": label,
            "path": diagnostics.sparse_path,
            "logical_size": diagnostics.logical_size,
            "artifact": format!(
                "artifacts/scenarios/{}/{}",
                AdvancedMountedKind::SparseRange.name(),
                artifact
            ),
            "missing": diagnostics.missing,
            "clients": diagnostics.clients,
        }),
    )?;
    log.write_all(b"\n")?;
    Ok(())
}

fn sparse_timeout_diagnostics_file_name(label: &str) -> String {
    let slug = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    format!("sparse-{slug}-timeout-diagnostics.json")
}

fn sparse_timeout_environment() -> Vec<SparseEnvironmentValue> {
    [
        "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS",
        "STUDIOFS_PG_SYNC_POLL_SECS",
        "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL",
    ]
    .into_iter()
    .map(|name| SparseEnvironmentValue {
        name,
        value: std::env::var(name).ok(),
    })
    .collect()
}

fn sparse_io_error_observation(error: &io::Error) -> SparseIoErrorObservation {
    SparseIoErrorObservation {
        kind: format!("{:?}", error.kind()),
        message: error.to_string(),
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    let millis = duration.as_millis();
    millis.min(u128::from(u64::MAX)) as u64
}

fn write_sparse_file_range(
    path: PathBuf,
    offset: u64,
    payload: &[u8],
    create: bool,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut options = mounted_write_barrier_open_options();
    let mut file = options.create(create).read(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(payload)?;
    file.sync_all()
}

fn truncate_sparse_file(path: PathBuf, len: u64) -> io::Result<()> {
    let options = mounted_write_barrier_open_options();
    let file = options.open(path)?;
    file.set_len(len)?;
    file.sync_all()
}

fn verify_sparse_range(path: PathBuf, offset: u64, payload: &[u8]) -> Result<(), ScenarioError> {
    let actual = read_exact_range(&path, offset, payload.len())?;
    if actual == payload {
        Ok(())
    } else {
        Err(ScenarioError::Io(io::Error::other(format!(
            "sparse range mismatch for {}@{offset}: expected {}, got {}",
            path.display(),
            bytes_sha256(payload),
            bytes_sha256(&actual)
        ))))
    }
}

fn verify_sparse_hole(path: PathBuf, offset: u64, len: usize) -> Result<(), ScenarioError> {
    let actual = read_exact_range(&path, offset, len)?;
    if actual.iter().all(|byte| *byte == 0) {
        Ok(())
    } else {
        Err(ScenarioError::Io(io::Error::other(format!(
            "sparse hole mismatch for {}@{offset}: expected {len} zero bytes",
            path.display()
        ))))
    }
}

fn read_exact_range(path: &Path, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    read_exact_range_from(&mut file, offset, len)
}

fn read_exact_range_from(file: &mut fs::File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = vec![0; len];
    file.read_exact(&mut buffer)?;
    Ok(buffer)
}

fn expected_sparse_payload(ranges: &[SparseRangeSpec], logical_size: u64) -> Vec<u8> {
    let mut payload = vec![0; logical_size as usize];
    for range in ranges {
        let start = range.offset as usize;
        let end = start + range.payload.len();
        payload[start..end].copy_from_slice(&range.payload);
    }
    payload
}

fn sparse_expected_ranges(ranges: &[SparseRangeSpec]) -> Vec<SparseExpectedRange> {
    ranges
        .iter()
        .map(|range| SparseExpectedRange {
            label: range.label.to_string(),
            offset: range.offset,
            len: range.payload.len(),
            expected_checksum_sha256: bytes_sha256(&range.payload),
        })
        .collect()
}

fn sparse_hole_summaries(
    clients: &[&str],
    hole_specs: &[SparseHoleSpec],
    all_zero: bool,
) -> Vec<SparseHoleProbe> {
    clients
        .iter()
        .flat_map(|client| {
            hole_specs.iter().map(move |hole| SparseHoleProbe {
                client: (*client).to_string(),
                offset: hole.offset,
                len: hole.len,
                all_zero,
            })
        })
        .collect()
}

fn planned_sparse_range_summary() -> SparseRangeSummary {
    let ranges = sparse_expected_range_specs(None);
    let final_logical_size = sparse_final_logical_size(&ranges);
    let expected_payload = expected_sparse_payload(&ranges, final_logical_size);
    SparseRangeSummary {
        relative_path: SPARSE_FILE_PATH.to_string(),
        logical_size: final_logical_size,
        final_checksum_sha256: bytes_sha256(&expected_payload),
        expected_byte_ranges: sparse_expected_ranges(&ranges),
        zero_filled_holes: sparse_hole_summaries(
            ADVANCED_MOUNTED_CLIENT_NAMES,
            sparse_hole_specs(),
            true,
        ),
        read_probes: ADVANCED_MOUNTED_CLIENT_NAMES
            .iter()
            .flat_map(|client| {
                ranges.iter().map(move |range| SparseReadProbe {
                    client: (*client).to_string(),
                    offset: range.offset,
                    len: range.payload.len(),
                    actual_checksum_sha256: bytes_sha256(&range.payload),
                    matched: true,
                })
            })
            .collect(),
    }
}

fn planned_cold_observer_summary() -> ColdObserverSummary {
    ColdObserverSummary {
        client: "client-d".to_string(),
        local_metadata_db: PathBuf::from(
            "artifacts/clients/client-d/client-root/meta/projectfs/projectfs.db",
        ),
        cache_dir: PathBuf::from("artifacts/clients/client-d/cache"),
        discovered_after_history: true,
    }
}

fn planned_catchup_summary() -> CatchupSummary {
    CatchupSummary {
        warm_writers: STALE_REMOUNT_WARM_WRITER_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        stale_remount_client: "client-c".to_string(),
        cold_discover_client: "client-d".to_string(),
        stale_remount_after_history: true,
        cold_discover_after_history: true,
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

fn planned_invalidation_dropout_summary() -> InvalidationDropoutSummary {
    let root = AdvancedMountedKind::InvalidationDropoutFallback.path_root();
    let target_path = format!("{root}/{INVALIDATION_DROPOUT_TARGET_FILE}");
    let final_payload =
        format!("planned invalidation dropout\npath={target_path}\nbody=fallback final\n")
            .into_bytes();
    InvalidationDropoutSummary {
        observer_client: INVALIDATION_DROPOUT_OBSERVER_CLIENT.to_string(),
        writer_clients: INVALIDATION_DROPOUT_WRITER_CLIENTS
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        notify_suppressed: true,
        fallback_interval_ms: INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS,
        stale_window_ms: INVALIDATION_DROPOUT_STALE_WINDOW_MS,
        stale_window_observed: true,
        delete_stale_window_observed: true,
        fallback_resumed: true,
        final_observer_path: target_path,
        final_observer_checksum_sha256: bytes_sha256(&final_payload),
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

fn write_planned_cold_observer_diagnostics(context: &ScenarioContext) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("cold-observer-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "cold_observer": "client-d",
            "warm_clients": COLD_OBSERVER_WARM_CLIENT_NAMES,
            "discovered_after_history": true,
            "mode": "planned",
        }))?,
    )?;
    Ok(())
}

fn write_planned_catchup_diagnostics(context: &ScenarioContext) -> Result<(), ScenarioError> {
    write_catchup_diagnostics(context, &planned_catchup_summary())
}

fn write_planned_invalidation_dropout_diagnostics(
    context: &ScenarioContext,
    summary: &InvalidationDropoutSummary,
) -> Result<(), ScenarioError> {
    write_invalidation_dropout_diagnostics(context, summary)
}

fn write_catchup_diagnostics(
    context: &ScenarioContext,
    catchup: &CatchupSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("catchup-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "warm_writers": catchup.warm_writers,
            "stale_remount_client": catchup.stale_remount_client,
            "cold_discover_client": catchup.cold_discover_client,
            "stale_remount_after_history": catchup.stale_remount_after_history,
            "cold_discover_after_history": catchup.cold_discover_after_history,
            "final_validation_result": catchup.final_validation_result,
        }))?,
    )?;
    Ok(())
}

fn write_invalidation_dropout_diagnostics(
    context: &ScenarioContext,
    summary: &InvalidationDropoutSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("invalidation-fallback-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "observer_client": summary.observer_client,
            "writer_clients": summary.writer_clients,
            "notify_suppressed": summary.notify_suppressed,
            "fallback_interval_ms": summary.fallback_interval_ms,
            "stale_window_ms": summary.stale_window_ms,
            "stale_window_observed": summary.stale_window_observed,
            "delete_stale_window_observed": summary.delete_stale_window_observed,
            "fallback_resumed": summary.fallback_resumed,
            "final_observer_path": summary.final_observer_path,
            "final_observer_checksum_sha256": summary.final_observer_checksum_sha256,
            "final_validation_result": summary.final_validation_result,
        }))?,
    )?;
    Ok(())
}

fn planned_long_history_summary() -> LongHistorySummary {
    let overwrite_target = format!(
        "{}/{}",
        AdvancedMountedKind::ColdObserverLongHistory.path_root(),
        COLD_OBSERVER_OVERWRITE_TARGET
    );
    let final_payload = cold_observer_planned_payload("overwrite_v2", &overwrite_target);
    LongHistorySummary {
        bounded_operation_count: planned_advanced_mounted_operations(
            AdvancedMountedKind::ColdObserverLongHistory,
            Instant::now(),
        )
        .len(),
        expected_present_paths: vec![
            "cold-observer/renamed-final.txt".to_string(),
            "cold-observer/overwrite-target.txt".to_string(),
            "cold-observer/append-target.txt".to_string(),
            "cold-observer/nested/child.txt".to_string(),
        ],
        expected_absent_paths: vec![
            "cold-observer/delete-target.txt".to_string(),
            "cold-observer/rename-source.txt".to_string(),
        ],
        final_checksum_sha256: bytes_sha256(&final_payload),
    }
}

fn planned_stale_remount_long_history_summary() -> LongHistorySummary {
    let root = AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root();
    let overwrite_target = format!("{root}/{COLD_OBSERVER_OVERWRITE_TARGET}");
    let final_payload = cold_observer_planned_payload("overwrite_v2", &overwrite_target);
    LongHistorySummary {
        bounded_operation_count: planned_advanced_mounted_operations(
            AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
            Instant::now(),
        )
        .len(),
        expected_present_paths: vec![
            format!("{root}/{COLD_OBSERVER_RENAME_DEST}"),
            format!("{root}/{COLD_OBSERVER_OVERWRITE_TARGET}"),
            format!("{root}/{COLD_OBSERVER_APPEND_TARGET}"),
            format!("{root}/{COLD_OBSERVER_NESTED_FILE}"),
        ],
        expected_absent_paths: vec![
            format!("{root}/{COLD_OBSERVER_DELETE_TARGET}"),
            format!("{root}/{COLD_OBSERVER_RENAME_SOURCE}"),
        ],
        final_checksum_sha256: bytes_sha256(&final_payload),
    }
}

fn cold_observer_planned_payload(label: &str, logical_path: &str) -> Vec<u8> {
    format!("planned\nlabel={label}\npath={logical_path}\n{label}").into_bytes()
}

fn sparse_ranges_match(kind: AdvancedMountedKind, summary: Option<&SparseRangeSummary>) -> bool {
    match (kind, summary) {
        (AdvancedMountedKind::SparseRange, Some(summary)) => {
            let final_logical_size = summary
                .expected_byte_ranges
                .iter()
                .fold(SPARSE_FINAL_LOGICAL_SIZE, |size, range| {
                    size.max(range.offset + range.len as u64)
                });
            summary.logical_size == final_logical_size
                && summary.read_probes.iter().all(|probe| probe.matched)
                && summary.zero_filled_holes.iter().all(|hole| hole.all_zero)
        }
        (AdvancedMountedKind::SparseRange, None) => false,
        (_, _) => true,
    }
}

fn fs_op(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    f: impl FnOnce() -> io::Result<()>,
) -> NamespaceCollisionOperation {
    record_advanced_operation(
        started,
        order,
        client,
        operation,
        path,
        f().map_err(|error| error.to_string()),
    )
}

fn required_op(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    result: Result<(), ScenarioError>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    match result {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error) => {
            let operation_record = record_advanced_operation(
                started,
                order,
                client,
                operation,
                path,
                Err(error.to_string()),
            );
            Err(ScenarioError::Io(io::Error::other(format!(
                "{} failed for {}: {:?}",
                operation, path, operation_record.error
            ))))
        }
    }
}

fn required_record(
    operation: NamespaceCollisionOperation,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    if let Some(error) = &operation.error {
        Err(ScenarioError::Io(io::Error::other(format!(
            "{} failed for {}: {}",
            operation.operation, operation.path, error
        ))))
    } else {
        Ok(operation)
    }
}

fn record_advanced_operation(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    result: Result<(), String>,
) -> NamespaceCollisionOperation {
    match result {
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
            error: Some(error),
        },
    }
}

fn write_parented(path: PathBuf, payload: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, payload)?;
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    file.sync_all()
}

/// Executes one mounted hotspot filesystem operation in a helper process.
pub fn run_hotspot_op_helper_with_timeout(
    operation: &str,
    mount_point: PathBuf,
    path: &str,
    target: Option<&str>,
    payload: Option<&Path>,
    progress: Option<&Path>,
    timeout: Option<Duration>,
) -> Result<(), anyhow::Error> {
    #[cfg(windows)]
    if let Some(timeout) = timeout {
        return run_hotspot_op_helper_with_windows_timeout(
            operation,
            mount_point,
            path,
            target,
            payload,
            progress,
            timeout,
        );
    }

    let _ = timeout;
    run_hotspot_op_helper(operation, mount_point, path, target, payload, progress)
}

#[cfg(windows)]
fn run_hotspot_op_helper_with_windows_timeout(
    operation: &str,
    mount_point: PathBuf,
    path: &str,
    target: Option<&str>,
    payload: Option<&Path>,
    progress: Option<&Path>,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let operation_for_worker = operation.to_string();
    let operation_for_timeout = operation.to_string();
    let path_for_worker = path.to_string();
    let path_for_timeout = path.to_string();
    let target_for_worker = target.map(str::to_string);
    let target_for_timeout = target.map(str::to_string);
    let payload_for_worker = payload.map(Path::to_path_buf);
    let progress_for_worker = progress.map(Path::to_path_buf);
    let progress_for_timeout = progress.map(Path::to_path_buf);
    let mount_for_worker = mount_point.clone();
    let mount_for_timeout = mount_point;
    let (tx, rx) = mpsc::channel();

    let worker = thread::Builder::new()
        .name("studiofs-e2e-hotspot-helper-io".to_string())
        .spawn(move || {
            let result = run_hotspot_op_helper(
                &operation_for_worker,
                mount_for_worker,
                &path_for_worker,
                target_for_worker.as_deref(),
                payload_for_worker.as_deref(),
                progress_for_worker.as_deref(),
            );
            let _ = tx.send(result);
        })?;

    match rx.recv_timeout(timeout) {
        Ok(result) => {
            join_hotspot_helper_worker(worker)?;
            result
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let mounted_path = mount_for_timeout.join(&path_for_timeout);
            let _ = record_hotspot_helper_progress(
                progress_for_timeout.as_deref(),
                "timeout_cancel_requested",
                &operation_for_timeout,
                &mounted_path,
                target_for_timeout.as_deref(),
                None,
            );
            let cancel_result = cancel_hotspot_helper_worker_io(&worker);
            let cancel_detail = match cancel_result {
                Ok(()) => "CancelSynchronousIo requested".to_string(),
                Err(error) => format!("CancelSynchronousIo failed: {error}"),
            };
            match rx.recv_timeout(HOTSPOT_HELPER_CANCEL_GRACE) {
                Ok(result) => {
                    join_hotspot_helper_worker(worker)?;
                    Err(anyhow::anyhow!(
                        "{HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER}: operation timed out after {} ms; {cancel_detail}; result after cancellation: {}",
                        timeout.as_millis(),
                        match result {
                            Ok(()) => "completed".to_string(),
                            Err(error) => error.to_string(),
                        }
                    ))
                }
                Err(_) => Err(anyhow::anyhow!(
                    "{HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER}: operation timed out after {} ms; {cancel_detail}; worker did not finish within {} ms",
                    timeout.as_millis(),
                    HOTSPOT_HELPER_CANCEL_GRACE.as_millis()
                )),
            }
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            join_hotspot_helper_worker(worker)?;
            Err(anyhow::anyhow!(
                "hotspot helper worker exited without result"
            ))
        }
    }
}

#[cfg(windows)]
fn join_hotspot_helper_worker(worker: thread::JoinHandle<()>) -> Result<(), anyhow::Error> {
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("hotspot helper worker panicked"))
}

#[cfg(windows)]
fn cancel_hotspot_helper_worker_io(worker: &thread::JoinHandle<()>) -> io::Result<()> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle as _;

    unsafe extern "system" {
        fn CancelSynchronousIo(hThread: *mut c_void) -> i32;
    }

    let ok = unsafe { CancelSynchronousIo(worker.as_raw_handle().cast()) };
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Executes one mounted hotspot filesystem operation without an internal timeout guard.
pub fn run_hotspot_op_helper(
    operation: &str,
    mount_point: PathBuf,
    path: &str,
    target: Option<&str>,
    payload: Option<&Path>,
    progress: Option<&Path>,
) -> Result<(), anyhow::Error> {
    let path = mount_point.join(path);
    record_hotspot_helper_progress(progress, "started", operation, &path, target, None)?;
    match operation {
        "mkdir" => {
            record_hotspot_helper_progress(
                progress,
                "before_create_dir_all",
                operation,
                &path,
                target,
                None,
            )?;
            fs::create_dir_all(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_create_dir_all",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "create" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                target,
                Some(payload.len()),
            )?;
            write_parented_with_barrier_progress(
                &path,
                &payload,
                progress,
                operation,
                target,
                BarrierWriteMode::CreateNew,
            )?;
        }
        "overwrite-barrier" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                target,
                Some(payload.len()),
            )?;
            write_parented_with_barrier_progress(
                &path,
                &payload,
                progress,
                operation,
                target,
                BarrierWriteMode::Overwrite,
            )?;
        }
        "write" | "overwrite" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                target,
                Some(payload.len()),
            )?;
            write_parented_with_progress(&path, &payload, progress, operation, target)?;
        }
        "append" => {
            let payload_path = payload.ok_or_else(|| anyhow::anyhow!("missing payload"))?;
            record_hotspot_helper_progress(
                progress,
                "before_read_payload",
                operation,
                &path,
                target,
                None,
            )?;
            let payload = fs::read(payload_path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_payload",
                operation,
                &path,
                target,
                Some(payload.len()),
            )?;
            append_payload_with_progress(&path, &payload, progress, operation, target)?;
        }
        "delete" | "unlink" => {
            record_hotspot_helper_progress(
                progress,
                "before_remove_file",
                operation,
                &path,
                target,
                None,
            )?;
            fs::remove_file(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_remove_file",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "rmdir" => {
            record_hotspot_helper_progress(
                progress,
                "before_remove_dir",
                operation,
                &path,
                target,
                None,
            )?;
            fs::remove_dir(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_remove_dir",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "rename" => {
            let target = target.ok_or_else(|| anyhow::anyhow!("missing rename target"))?;
            record_hotspot_helper_progress(
                progress,
                "before_rename",
                operation,
                &path,
                Some(target),
                None,
            )?;
            fs::rename(&path, mount_point.join(target))?;
            record_hotspot_helper_progress(
                progress,
                "after_rename",
                operation,
                &path,
                Some(target),
                None,
            )?;
        }
        "readdir" => {
            record_hotspot_helper_progress(
                progress,
                "before_read_dir",
                operation,
                &path,
                target,
                None,
            )?;
            fs::read_dir(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_read_dir",
                operation,
                &path,
                target,
                None,
            )?;
        }
        "stat" => {
            record_hotspot_helper_progress(
                progress,
                "before_metadata",
                operation,
                &path,
                target,
                None,
            )?;
            fs::metadata(&path)?;
            record_hotspot_helper_progress(
                progress,
                "after_metadata",
                operation,
                &path,
                target,
                None,
            )?;
        }
        other => anyhow::bail!("unsupported hotspot operation: {other}"),
    }
    record_hotspot_helper_progress(progress, "completed", operation, &path, target, None)?;
    Ok(())
}

/// Executes the mounted open-handle mutation sequence in a helper process.
pub fn run_open_handle_op_helper(
    client_a_mount: PathBuf,
    client_b_mount: PathBuf,
    unlink_path: &str,
    rename_source: &str,
    rename_dest: &str,
    append_payload: &Path,
    progress: Option<&Path>,
) -> Result<(), anyhow::Error> {
    let operation = "open-handle";
    let client_a_unlink_path = client_a_mount.join(unlink_path);
    let client_b_unlink_path = client_b_mount.join(unlink_path);
    let client_b_rename_source = client_b_mount.join(rename_source);
    let client_b_rename_dest = client_b_mount.join(rename_dest);
    record_hotspot_helper_progress(
        progress,
        "started",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_read_payload",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    let append_payload = fs::read(append_payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_read_payload",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        Some(append_payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_open_old_handle",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    let mut handle = OpenOptions::new()
        .append(true)
        .open(&client_a_unlink_path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_old_handle",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_remove_file",
        operation,
        &client_b_unlink_path,
        Some(rename_dest),
        None,
    )?;
    fs::remove_file(&client_b_unlink_path)?;
    record_hotspot_helper_progress(
        progress,
        "after_remove_file",
        operation,
        &client_b_unlink_path,
        Some(rename_dest),
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_rename",
        operation,
        &client_b_rename_source,
        Some(rename_dest),
        None,
    )?;
    fs::rename(&client_b_rename_source, &client_b_rename_dest)?;
    record_hotspot_helper_progress(
        progress,
        "after_rename",
        operation,
        &client_b_rename_source,
        Some(rename_dest),
        None,
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_write_all",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        Some(append_payload.len()),
    )?;
    handle.write_all(&append_payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write_all",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        Some(append_payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_sync_all",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    let sync_retries = sync_open_handle_with_retry(
        || handle.sync_all(),
        OPEN_HANDLE_SYNC_RETRY_TIMEOUT,
        OPEN_HANDLE_SYNC_RETRY_POLL_INTERVAL,
    )?;
    if sync_retries > 0 {
        record_hotspot_helper_progress(
            progress,
            "after_sync_all_retry",
            operation,
            &client_a_unlink_path,
            Some(rename_dest),
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "after_sync_all",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    drop(handle);
    record_hotspot_helper_progress(
        progress,
        "completed",
        operation,
        &client_a_unlink_path,
        Some(rename_dest),
        None,
    )?;
    Ok(())
}

fn sync_open_handle_with_retry(
    mut sync_all: impl FnMut() -> io::Result<()>,
    timeout: Duration,
    poll_interval: Duration,
) -> io::Result<usize> {
    let deadline = Instant::now() + timeout;
    let mut retries = 0;
    loop {
        match sync_all() {
            Ok(()) => return Ok(retries),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(error);
                }
                retries += 1;
                thread::sleep(poll_interval.min(remaining));
            }
            Err(error) => return Err(error),
        }
    }
}

fn write_parented_with_progress(
    path: &Path,
    payload: &[u8],
    progress: Option<&Path>,
    operation: &str,
    target: Option<&str>,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        record_hotspot_helper_progress(
            progress,
            "before_create_parent",
            operation,
            path,
            target,
            None,
        )?;
        fs::create_dir_all(parent)?;
        record_hotspot_helper_progress(
            progress,
            "after_create_parent",
            operation,
            path,
            target,
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "before_write",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    fs::write(path, payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(progress, "before_reopen", operation, path, target, None)?;
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    record_hotspot_helper_progress(progress, "after_reopen", operation, path, target, None)?;
    record_hotspot_helper_progress(progress, "before_sync_all", operation, path, target, None)?;
    file.sync_all()?;
    record_hotspot_helper_progress(progress, "after_sync_all", operation, path, target, None)
}

enum BarrierWriteMode {
    CreateNew,
    Overwrite,
}

fn write_parented_with_barrier_progress(
    path: &Path,
    payload: &[u8],
    progress: Option<&Path>,
    operation: &str,
    target: Option<&str>,
    mode: BarrierWriteMode,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        record_hotspot_helper_progress(
            progress,
            "before_create_parent",
            operation,
            path,
            target,
            None,
        )?;
        fs::create_dir_all(parent)?;
        record_hotspot_helper_progress(
            progress,
            "after_create_parent",
            operation,
            path,
            target,
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "before_open_barrier_write",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    let mut options = mounted_write_barrier_open_options();
    match mode {
        BarrierWriteMode::CreateNew => {
            options.create_new(true);
        }
        BarrierWriteMode::Overwrite => {
            options.create(true).truncate(true).read(true);
        }
    }
    let mut file = options.open(path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_barrier_write",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_write_all",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    file.write_all(payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write_all",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(progress, "before_sync_all", operation, path, target, None)?;
    file.sync_all()?;
    record_hotspot_helper_progress(progress, "after_sync_all", operation, path, target, None)?;
    drop(file);
    record_hotspot_helper_progress(progress, "before_readback", operation, path, target, None)?;
    let readback = wait_for_mounted_payload_readback(
        path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
    .map_err(io::Error::other)?;
    record_hotspot_helper_progress(
        progress,
        "after_readback",
        operation,
        path,
        target,
        Some(readback.len()),
    )
}

fn append_payload_with_progress(
    path: &Path,
    payload: &[u8],
    progress: Option<&Path>,
    operation: &str,
    target: Option<&str>,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        record_hotspot_helper_progress(
            progress,
            "before_create_parent",
            operation,
            path,
            target,
            None,
        )?;
        fs::create_dir_all(parent)?;
        record_hotspot_helper_progress(
            progress,
            "after_create_parent",
            operation,
            path,
            target,
            None,
        )?;
    }
    record_hotspot_helper_progress(
        progress,
        "before_open_append",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    record_hotspot_helper_progress(
        progress,
        "after_open_append",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(
        progress,
        "before_write_all",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    file.write_all(payload)?;
    record_hotspot_helper_progress(
        progress,
        "after_write_all",
        operation,
        path,
        target,
        Some(payload.len()),
    )?;
    record_hotspot_helper_progress(progress, "before_sync_all", operation, path, target, None)?;
    file.sync_all()?;
    record_hotspot_helper_progress(progress, "after_sync_all", operation, path, target, None)
}

fn record_hotspot_helper_progress(
    progress: Option<&Path>,
    phase: &str,
    operation: &str,
    path: &Path,
    target: Option<&str>,
    payload_len: Option<usize>,
) -> io::Result<()> {
    let Some(progress) = progress else {
        return Ok(());
    };
    if let Some(parent) = progress.parent() {
        fs::create_dir_all(parent)?;
    }
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(progress)?;
    serde_json::to_writer(
        &mut file,
        &json!({
            "timestamp_ms": timestamp_ms,
            "phase": phase,
            "operation": operation,
            "path": path.display().to_string(),
            "target": target,
            "payload_len": payload_len,
        }),
    )
    .map_err(io::Error::other)?;
    writeln!(file)?;
    file.flush()
}

fn sanitize_hotspot_progress_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    sanitized.trim_matches('_').chars().take(80).collect()
}

fn write_advanced_mounted_summary(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    summary: &AdvancedMountedSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join(kind.summary_file()),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

fn write_lww_oracle(
    context: &ScenarioContext,
    oracle: &LwwReconnectOracle,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("lww-oracle.json"),
        serde_json::to_vec_pretty(oracle)?,
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
    use std::cell::{Cell, RefCell};
    use std::path::{Path, PathBuf};
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::client::{ClientLifecycleState, ClientSpec, S3Config, client_spec_for};

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
            if self.lose_writer_payload_on_remount {
                std::fs::write(writer.mount_point.join(POSTGRES_OUTAGE_FILE_NAME), [])?;
            }
            if self.corrupt_writer_payload_on_remount {
                std::fs::write(
                    writer.mount_point.join(POSTGRES_OUTAGE_FILE_NAME),
                    vec![b'x'; POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len()],
                )?;
            }
            if self.keep_lost_writer_payload_local_on_remount && self.lose_writer_payload_on_remount
            {
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

        assert!(operations.iter().any(|operation| {
            operation.operation == "rename" && operation.result == "accepted"
        }));
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
        seed_open_handle_initial_tree(&context, &clients, &isolation, Duration::from_secs(1))
            .unwrap();

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
                let unlink_target =
                    open_handle_isolated_path(&isolation, OPEN_HANDLE_UNLINK_TARGET);
                let rename_source =
                    open_handle_isolated_path(&isolation, OPEN_HANDLE_RENAME_SOURCE);
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
        let mut driver = RecordingCrashRestartDriver::new(clients.clone())
            .publish_current_file_before_convergence();
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
        assert_eq!(
            summary.final_validator.compared_clients,
            POSTGRES_OUTAGE_CLIENT_NAMES[1..]
                .iter()
                .copied()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        assert!(summary.peer_observation_time_ms > 0);
    }

    #[test]
    fn postgres_outage_empty_writer_recovery_does_not_require_peer_empty_file() {
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
        assert_eq!(
            summary.final_validator.compared_clients,
            POSTGRES_OUTAGE_CLIENT_NAMES[1..]
                .iter()
                .copied()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        assert!(summary.final_validator.converged);
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
            client_env_overrides: std::collections::BTreeMap::new(),
        }
    }
}

//! Shared scenario contracts and result types.

use std::path::PathBuf;

use thiserror::Error;

use crate::client::{ClientControllerConfig, ClientControllerError};
use crate::infra::InfraError;
use crate::mounted_tree::{MountedTreeError, NamespaceInvariantViolation};

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
    /// Mounted filesystem operation exceeded its per-call timeout.
    #[error("bounded mounted I/O timed out for {label} after {timeout_ms} ms; {cancel_detail}")]
    MountedIoTimeout {
        /// Operation label.
        label: String,
        /// Timeout in milliseconds.
        timeout_ms: u128,
        /// Best-effort cancellation result.
        cancel_detail: String,
    },
    /// A previous bounded mounted I/O operation is still running.
    #[error(
        "bounded mounted I/O worker limit reached before {label}; active workers {active_workers}/{max_workers}; previous timed operation may still be blocked after {timeout_ms} ms timeout budget"
    )]
    MountedIoWorkerLimit {
        /// Operation label.
        label: String,
        /// Current active worker count.
        active_workers: usize,
        /// Maximum active workers allowed.
        max_workers: usize,
        /// Requested timeout in milliseconds.
        timeout_ms: u128,
    },
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
    /// The scenario and its cleanup both failed; retain both typed errors.
    #[error("{context} failed: {primary}; cleanup failed: {cleanup}")]
    CleanupAfterFailure {
        /// Scenario context used to identify the failed operation.
        context: String,
        /// Primary scenario error.
        primary: Box<ScenarioError>,
        /// Cleanup error.
        cleanup: Box<ScenarioError>,
    },
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

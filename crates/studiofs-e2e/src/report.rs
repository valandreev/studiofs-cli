//! JSON report and manifest types for E2E runs.

use serde::{Deserialize, Serialize};

use crate::infra::{InfraMode, ServicePort};
use crate::runner::{RetentionPolicy, RunStatus, ScenarioStatus};

/// Metadata describing a single E2E run manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    /// Manifest schema version.
    pub schema_version: u32,
    /// Crate that generated the run manifest.
    pub runner_crate: String,
    /// Stable run identifier for artifacts and logs.
    pub run_id: String,
    /// PRNG seed used to generate the run.
    pub seed: u64,
    /// Sandbox selection used for the run.
    pub sandbox: String,
    /// Scenario names included in the run.
    pub scenarios: Vec<String>,
    /// Retention policy applied to generated artifacts.
    pub retention: RetentionPolicy,
    /// Selected runtime platform label.
    pub selected_platform: String,
    /// Infra mode used for this run.
    pub infra: InfraMode,
    /// Optional docker-compose project name.
    pub compose_project: Option<String>,
    /// Ports reserved for the run services.
    pub allocated_ports: Vec<ServicePort>,
    /// Optional StudioFS binary path override.
    pub studiofs_bin: Option<String>,
    /// Optional port range override.
    pub port_range: Option<String>,
    /// Optional execution timeout override.
    pub timeout: Option<String>,
    /// Explicit client roots provided by the caller.
    pub explicit_client_roots: Vec<String>,
    /// Paths exempt from automatic cleanup.
    pub cleanup_allowlist: Vec<String>,
    /// Run creation timestamp in RFC3339 string form.
    pub created_at: String,
}

/// Aggregate JSON report for a full E2E run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    /// Schema version of the report format.
    pub schema_version: u32,
    /// Run identifier shared across artifacts.
    pub run_id: String,
    /// PRNG seed used for randomized test input.
    pub seed: u64,
    /// Final status of the run.
    pub status: RunStatus,
    /// Process exit code for the run.
    pub exit_code: i32,
    /// UTC timestamp when run execution started.
    pub started_at: String,
    /// UTC timestamp when run execution finished.
    pub finished_at: String,
    /// Total run duration in milliseconds.
    pub duration_ms: u64,
    /// Runner binary metadata.
    pub runner: RunnerReport,
    /// Host environment metadata.
    pub host: HostReport,
    /// Sandbox metadata.
    pub sandbox: SandboxReport,
    /// Runtime environment metadata.
    pub environment: EnvironmentReport,
    /// Preflight checks report.
    pub preflight: PreflightReport,
    /// Scenario-level reports for each executed scenario.
    pub scenarios: Vec<ScenarioReport>,
    /// Collected artifacts for the run.
    pub artifacts: Vec<ArtifactReport>,
    /// Cleanup actions and errors.
    pub cleanup: CleanupReport,
}

/// Runner binary metadata embedded in report outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerReport {
    /// Runner crate name.
    #[serde(rename = "crate")]
    pub crate_name: String,
    /// Runner crate version.
    pub version: String,
    /// Runner commit SHA used for the run.
    pub git_sha: String,
}

/// Host platform metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostReport {
    /// Host operating system name.
    pub os: String,
    /// CPU architecture string.
    pub arch: String,
    /// Full platform label.
    pub platform: String,
}

/// Sandbox metadata for the E2E run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxReport {
    /// Sandbox root path.
    pub root: String,
    /// Retention policy used for sandbox artifacts.
    pub retention: RetentionPolicy,
}

/// Runtime environment details used to reproduce the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentReport {
    /// Run identifier associated with the environment.
    pub run_id: String,
    /// Scenario names selected for execution.
    pub scenarios: Vec<String>,
    /// PRNG seed for deterministic environment generation.
    pub seed: u64,
    /// Optional StudioFS binary path.
    pub studiofs_bin: Option<String>,
    /// Optional docker-compose project name.
    pub compose_project: Option<String>,
    /// Observed compose image set.
    pub compose_images: Vec<ComposeImageReport>,
    /// Optional compose image discovery error.
    pub compose_images_error: Option<String>,
    /// Reserved service ports.
    pub allocated_ports: Vec<ServicePort>,
    /// Runtime client root directories.
    pub client_roots: Vec<String>,
    /// Runtime mount root directories.
    pub mount_roots: Vec<String>,
    /// Client root ownership checks.
    pub client_root_ownership: Vec<PathOwnershipReport>,
    /// Mount root ownership checks.
    pub mount_root_ownership: Vec<PathOwnershipReport>,
    /// Optional WinFSP version string.
    pub winfsp_version: Option<String>,
    /// Optional macFUSE version string.
    pub macfuse_version: Option<String>,
}

/// Single docker-compose service image entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComposeImageReport {
    /// Compose service name.
    pub service: String,
    /// Image reference for the service.
    pub image: String,
}

/// Ownership probe result for a filesystem path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathOwnershipReport {
    /// Path that was probed.
    pub path: String,
    /// Optional resolved owner string.
    pub owner: Option<String>,
    /// Optional error string when ownership check failed.
    pub error: Option<String>,
}

/// Preflight checks aggregated for the run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightReport {
    /// Aggregate preflight status.
    pub status: RunStatus,
    /// List of checks and their outcomes.
    pub checks: Vec<PreflightCheckReport>,
}

/// Result of a single preflight check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightCheckReport {
    /// Check name.
    pub name: String,
    /// Check execution status.
    pub status: RunStatus,
    /// Human-readable check details.
    pub detail: String,
}

/// Scenario-level report item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    /// Scenario name.
    pub name: String,
    /// Scenario seed for deterministic behavior.
    pub seed: u64,
    /// Scenario execution status.
    pub status: ScenarioStatus,
    /// UTC timestamp when scenario started.
    pub started_at: String,
    /// UTC timestamp when scenario finished.
    pub finished_at: String,
    /// Scenario duration in milliseconds.
    pub duration_ms: u64,
    /// Paths of artifacts produced by the scenario.
    pub artifacts: Vec<String>,
    /// Optional scenario failure detail.
    pub error: Option<String>,
}

/// Produced artifact metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactReport {
    /// Artifact name.
    pub name: String,
    /// Artifact file system path.
    pub path: String,
    /// Whether artifact is required for successful completion.
    pub required: bool,
}

/// Cleanup execution report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupReport {
    /// Overall cleanup status.
    pub status: RunStatus,
    /// Cleanup actions performed.
    pub actions: Vec<String>,
    /// Cleanup errors encountered.
    pub errors: Vec<String>,
}

/// Generic event record emitted by the report pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    /// Event timestamp.
    pub timestamp: String,
    /// Run identifier this event belongs to.
    pub run_id: String,
    /// Optional scenario identifier.
    pub scenario: Option<String>,
    /// Hook name that emitted the event.
    pub hook: String,
    /// Phase label for the event.
    pub phase: String,
    /// Optional event detail payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_duration_fields_accept_u64_values() {
        let duration: u64 = 42;
        let scenario = ScenarioReport {
            name: "basic-multi-client-smoke".to_string(),
            seed: 1,
            status: ScenarioStatus::Passed,
            started_at: "2026-05-01T00:00:00Z".to_string(),
            finished_at: "2026-05-01T00:00:01Z".to_string(),
            duration_ms: duration,
            artifacts: Vec::new(),
            error: None,
        };
        let report = RunReport {
            schema_version: 1,
            run_id: "run-1".to_string(),
            seed: 1,
            status: RunStatus::Passed,
            exit_code: 0,
            started_at: "2026-05-01T00:00:00Z".to_string(),
            finished_at: "2026-05-01T00:00:01Z".to_string(),
            duration_ms: duration,
            runner: RunnerReport {
                crate_name: "studiofs-e2e".to_string(),
                version: "0.1.0".to_string(),
                git_sha: "unknown".to_string(),
            },
            host: HostReport {
                os: "windows".to_string(),
                arch: "x86_64".to_string(),
                platform: "windows-winfsp".to_string(),
            },
            sandbox: SandboxReport {
                root: "target/e2e-runs/run-1".to_string(),
                retention: RetentionPolicy::Always,
            },
            environment: EnvironmentReport {
                run_id: "run-1".to_string(),
                scenarios: vec!["basic-multi-client-smoke".to_string()],
                seed: 1,
                studiofs_bin: None,
                compose_project: None,
                compose_images: Vec::new(),
                compose_images_error: None,
                allocated_ports: Vec::new(),
                client_roots: Vec::new(),
                mount_roots: Vec::new(),
                client_root_ownership: Vec::new(),
                mount_root_ownership: Vec::new(),
                winfsp_version: None,
                macfuse_version: None,
            },
            preflight: PreflightReport {
                status: RunStatus::Passed,
                checks: Vec::new(),
            },
            scenarios: vec![scenario],
            artifacts: Vec::new(),
            cleanup: CleanupReport {
                status: RunStatus::Passed,
                actions: Vec::new(),
                errors: Vec::new(),
            },
        };

        assert_eq!(report.duration_ms, duration);
        assert_eq!(report.scenarios[0].duration_ms, duration);
    }
}

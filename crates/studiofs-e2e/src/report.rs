//! JSON report and manifest types for E2E runs.

use serde::{Deserialize, Serialize};

use crate::infra::{InfraMode, ServicePort};
use crate::runner::{RetentionPolicy, RunStatus, ScenarioStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct RunManifest {
    /// Field $name.
    pub schema_version: u32,
    /// Field $name.
    pub runner_crate: String,
    /// Field $name.
    pub run_id: String,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub sandbox: String,
    /// Field $name.
    pub scenarios: Vec<String>,
    /// Field $name.
    pub retention: RetentionPolicy,
    /// Field $name.
    pub selected_platform: String,
    /// Field $name.
    pub infra: InfraMode,
    /// Field $name.
    pub compose_project: Option<String>,
    /// Field $name.
    pub allocated_ports: Vec<ServicePort>,
    /// Field $name.
    pub studiofs_bin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub studiofs_bin_fingerprint: Option<StudiofsBinaryReport>,
    /// Field $name.
    pub port_range: Option<String>,
    /// Field $name.
    pub timeout: Option<String>,
    /// Field $name.
    pub explicit_client_roots: Vec<String>,
    /// Field $name.
    pub cleanup_allowlist: Vec<String>,
    /// Field $name.
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct RunReport {
    /// Field $name.
    pub schema_version: u32,
    /// Field $name.
    pub run_id: String,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub status: RunStatus,
    /// Field $name.
    pub exit_code: i32,
    /// Field $name.
    pub started_at: String,
    /// Field $name.
    pub finished_at: String,
    /// Field $name.
    pub duration_ms: u64,
    /// Field $name.
    pub runner: RunnerReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub studiofs_bin: Option<StudiofsBinaryReport>,
    /// Field $name.
    pub host: HostReport,
    /// Field $name.
    pub sandbox: SandboxReport,
    /// Field $name.
    pub preflight: PreflightReport,
    /// Field $name.
    pub scenarios: Vec<ScenarioReport>,
    /// Field $name.
    pub artifacts: Vec<ArtifactReport>,
    /// Field $name.
    pub cleanup: CleanupReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct RunnerReport {
    #[serde(rename = "crate")]
    /// Field $name.
    pub crate_name: String,
    /// Field $name.
    pub version: String,
    /// Field $name.
    pub git_sha: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Product binary fingerprint recorded for reproducible E2E diagnostics.
pub struct StudiofsBinaryReport {
    /// Original path passed through `--studiofs-bin`.
    pub path: String,
    /// Canonical path resolved by the runner.
    pub canonical_path: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Last modification time in Unix milliseconds.
    pub modified_unix_ms: u64,
    /// SHA-256 digest of the executable bytes.
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct HostReport {
    /// Field $name.
    pub os: String,
    /// Field $name.
    pub arch: String,
    /// Field $name.
    pub platform: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct SandboxReport {
    /// Field $name.
    pub root: String,
    /// Field $name.
    pub retention: RetentionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct PreflightReport {
    /// Field $name.
    pub status: RunStatus,
    /// Field $name.
    pub checks: Vec<PreflightCheckReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct PreflightCheckReport {
    /// Field $name.
    pub name: String,
    /// Field $name.
    pub status: RunStatus,
    /// Field $name.
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct ScenarioReport {
    /// Field $name.
    pub name: String,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub status: ScenarioStatus,
    /// Field $name.
    pub started_at: String,
    /// Field $name.
    pub finished_at: String,
    /// Field $name.
    pub duration_ms: u64,
    /// Field $name.
    pub artifacts: Vec<String>,
    /// Field $name.
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
/// Struct $name.
pub struct ArtifactReport {
    /// Field $name.
    pub name: String,
    /// Field $name.
    pub path: String,
    /// Field $name.
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct CleanupReport {
    /// Field $name.
    pub status: RunStatus,
    /// Field $name.
    pub actions: Vec<String>,
    /// Field $name.
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct $name.
pub struct EventRecord {
    /// Field $name.
    pub timestamp: String,
    /// Field $name.
    pub run_id: String,
    /// Field $name.
    pub scenario: Option<String>,
    /// Field $name.
    pub hook: String,
    /// Field $name.
    pub phase: String,
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
            studiofs_bin: None,
            host: HostReport {
                os: "windows".to_string(),
                arch: "x86_64".to_string(),
                platform: "windows-winfsp".to_string(),
            },
            sandbox: SandboxReport {
                root: "target/e2e-runs/run-1".to_string(),
                retention: RetentionPolicy::Always,
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

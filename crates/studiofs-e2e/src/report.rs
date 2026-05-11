//! JSON report and manifest types for E2E runs.

use serde::{Deserialize, Serialize};

use crate::infra::{InfraMode, ServicePort};
use crate::runner::{RetentionPolicy, RunStatus, ScenarioStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub schema_version: u32,
    pub runner_crate: String,
    pub run_id: String,
    pub seed: u64,
    pub sandbox: String,
    pub scenarios: Vec<String>,
    pub retention: RetentionPolicy,
    pub selected_platform: String,
    pub infra: InfraMode,
    pub compose_project: Option<String>,
    pub allocated_ports: Vec<ServicePort>,
    pub studiofs_bin: Option<String>,
    pub port_range: Option<String>,
    pub timeout: Option<String>,
    pub explicit_client_roots: Vec<String>,
    pub cleanup_allowlist: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub schema_version: u32,
    pub run_id: String,
    pub seed: u64,
    pub status: RunStatus,
    pub exit_code: i32,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u64,
    pub runner: RunnerReport,
    pub host: HostReport,
    pub sandbox: SandboxReport,
    pub environment: EnvironmentReport,
    pub preflight: PreflightReport,
    pub scenarios: Vec<ScenarioReport>,
    pub artifacts: Vec<ArtifactReport>,
    pub cleanup: CleanupReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerReport {
    #[serde(rename = "crate")]
    pub crate_name: String,
    pub version: String,
    pub git_sha: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostReport {
    pub os: String,
    pub arch: String,
    pub platform: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxReport {
    pub root: String,
    pub retention: RetentionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentReport {
    pub run_id: String,
    pub scenarios: Vec<String>,
    pub seed: u64,
    pub studiofs_bin: Option<String>,
    pub compose_project: Option<String>,
    pub compose_images: Vec<ComposeImageReport>,
    pub allocated_ports: Vec<ServicePort>,
    pub client_roots: Vec<String>,
    pub mount_roots: Vec<String>,
    pub winfsp_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComposeImageReport {
    pub service: String,
    pub image: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightReport {
    pub status: RunStatus,
    pub checks: Vec<PreflightCheckReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightCheckReport {
    pub name: String,
    pub status: RunStatus,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub name: String,
    pub seed: u64,
    pub status: ScenarioStatus,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u64,
    pub artifacts: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactReport {
    pub name: String,
    pub path: String,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupReport {
    pub status: RunStatus,
    pub actions: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub timestamp: String,
    pub run_id: String,
    pub scenario: Option<String>,
    pub hook: String,
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
                allocated_ports: Vec::new(),
                client_roots: Vec::new(),
                mount_roots: Vec::new(),
                winfsp_version: None,
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

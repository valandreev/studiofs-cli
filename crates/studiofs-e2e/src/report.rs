//! JSON report and manifest types for E2E runs.

use serde::{Deserialize, Serialize};

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
    pub duration_ms: u128,
    pub runner: RunnerReport,
    pub host: HostReport,
    pub sandbox: SandboxReport,
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
    pub duration_ms: u128,
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

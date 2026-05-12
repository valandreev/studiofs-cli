//! Runner orchestration, sandbox creation, reports, and cleanup.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::client::{
    ClientControllerConfig, ClientControllerError, DEFAULT_CLIENT_NAMES, client_cache_dir,
    client_mount_point, client_root_dir,
};
use crate::infra::{
    CommandExecutor, CommandSpec, ComposeConfig, DockerComposeStack, InfraCheck, InfraError,
    InfraMode, SystemCommandExecutor,
};
use crate::report::{
    ArtifactReport, CleanupReport, ComposeImageReport, EnvironmentReport, EventRecord, HostReport,
    PathOwnershipReport, PreflightCheckReport, PreflightReport, RunManifest, RunReport,
    RunnerReport, SandboxReport, ScenarioReport,
};
use crate::scenario::{self, ScenarioContext};

const SCHEMA_VERSION: u32 = 1;
const RUNNER_CRATE: &str = "studiofs-e2e";
const RUNNER_MARKER_FILE: &str = ".studiofs-e2e-runner.json";
const PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetentionPolicy {
    Always,
    OnFailure,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlatformSelection {
    Auto,
    LinuxFuse,
    MacosFuse,
    WindowsWinfsp,
}

impl PlatformSelection {
    fn resolved(self) -> &'static str {
        match self {
            Self::Auto => host_platform(),
            Self::LinuxFuse => "linux-fuse",
            Self::MacosFuse => "macos-fuse",
            Self::WindowsWinfsp => "windows-winfsp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Passed,
    Failed,
    Skipped,
    PreflightFailed,
    CleanupFailed,
    TimedOut,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioStatus {
    Passed,
    Failed,
    Skipped,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRequest {
    pub root: PathBuf,
    pub run_id: Option<String>,
    pub scenarios: Vec<String>,
    pub seed: u64,
    pub studiofs_bin: Option<PathBuf>,
    pub platform: PlatformSelection,
    pub retention: RetentionPolicy,
    pub report: Option<PathBuf>,
    pub port_range: Option<String>,
    pub timeout: Option<String>,
    pub infra: InfraMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanRequest {
    pub root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanOutcome {
    pub removed: Vec<PathBuf>,
    pub skipped: Vec<CleanSkipped>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanSkipped {
    pub path: PathBuf,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioOutcome {
    pub name: String,
    pub status: ScenarioStatus,
    pub artifacts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    pub run_id: String,
    pub status: RunStatus,
    pub exit_code: i32,
    pub sandbox: PathBuf,
    pub report_path: PathBuf,
    pub scenarios: Vec<ScenarioOutcome>,
}

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("unknown E2E scenario: {0}")]
    UnknownScenario(String),
    #[error("E2E scenario {scenario} requires --studiofs-bin when --infra compose is used")]
    MissingStudiofsBin { scenario: String },
    #[error("run ID must not be empty")]
    EmptyRunId,
    #[error("run ID must be a safe single directory name: {0}")]
    UnsafeRunId(String),
    #[error("unsafe cleanup root: {0}")]
    UnsafeCleanupRoot(String),
    #[error("E2E artifact path is outside the run sandbox")]
    ArtifactPathOutsideSandbox { path: PathBuf, sandbox: PathBuf },
    #[error("E2E artifact path contains a non-UTF-8 component")]
    NonUtf8ArtifactPath { path: PathBuf },
    #[error("E2E artifact path must be relative and must not contain parent components")]
    UnsafeArtifactPath { path: PathBuf },
    #[error("runner I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("runner JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("scenario hook failed: {0}")]
    Scenario(#[from] scenario::ScenarioError),
    #[error("runner client controller failed: {0}")]
    Client(#[from] ClientControllerError),
    #[error("runner infra failed: {0}")]
    Infra(#[from] InfraError),
}

#[derive(Debug)]
struct ScenarioLifecycleOutcome {
    report: ScenarioReport,
    outcome: ScenarioOutcome,
    cleanup_error: Option<String>,
}

struct ResolvedScenario {
    definition: scenario::ScenarioDefinition,
    scenario: Box<dyn scenario::Scenario>,
}

pub fn run(request: RunRequest) -> Result<RunOutcome, RunnerError> {
    let executor = SystemCommandExecutor;
    run_with_executor(request, &executor)
}

pub fn run_with_executor(
    request: RunRequest,
    executor: &dyn CommandExecutor,
) -> Result<RunOutcome, RunnerError> {
    let started_instant = Instant::now();
    let started_at = timestamp();
    let root = absolutize(&request.root)?;
    let run_id = match request.run_id {
        Some(run_id) if run_id.trim().is_empty() => return Err(RunnerError::EmptyRunId),
        Some(run_id) => run_id,
        None => generated_run_id(),
    };
    validate_run_id(&run_id)?;
    let scenario_names = if request.scenarios.is_empty() {
        vec![scenario::BASIC_MULTI_CLIENT_SMOKE.to_string()]
    } else {
        request.scenarios
    };
    let scenarios = resolve_scenarios(&scenario_names)?;
    if request.infra == InfraMode::Compose && request.studiofs_bin.is_none() {
        if let Some(definition) = scenarios
            .iter()
            .map(|resolved| resolved.definition)
            .find(|definition| definition.requires_studiofs_bin)
        {
            return Err(RunnerError::MissingStudiofsBin {
                scenario: definition.name.to_string(),
            });
        }
    }
    let sandbox = root.join(&run_id);
    let selected_platform = request.platform.resolved().to_string();
    let infra_mode = request.infra;
    let compose_stack = match infra_mode {
        InfraMode::Disabled => None,
        InfraMode::Compose => Some(DockerComposeStack::new(ComposeConfig::for_run(
            &run_id,
            &sandbox,
            request.port_range.clone(),
        ))?),
    };
    let compose_project = compose_stack
        .as_ref()
        .map(|stack| stack.project_name().to_string());
    let allocated_ports = compose_stack
        .as_ref()
        .map(|stack| stack.ports().to_vec())
        .unwrap_or_default();

    create_sandbox_layout(&sandbox, &scenarios)?;
    let manifest = build_manifest(ManifestInput {
        run_id: &run_id,
        seed: request.seed,
        sandbox: &sandbox,
        scenarios: &scenario_names,
        retention: request.retention,
        selected_platform: &selected_platform,
        infra: infra_mode,
        compose_project: compose_project.as_deref(),
        allocated_ports: allocated_ports.clone(),
        studiofs_bin: request.studiofs_bin.as_deref(),
        port_range: request.port_range.as_deref(),
        timeout: request.timeout.as_deref(),
        created_at: &started_at,
    });
    write_pretty_json(&sandbox.join("run.json"), &manifest)?;
    write_runner_marker(&sandbox, &run_id)?;
    write_runner_cleanup_boundary(&sandbox, &manifest)?;
    write_log_files(&sandbox)?;

    emit_progress(
        &run_id,
        "runner",
        None,
        "preflight",
        ProgressStatus::Started,
    );
    let prepared_preflight = prepare_preflight(
        &root,
        &sandbox,
        request.platform,
        compose_stack.as_ref(),
        executor,
    )?;
    let preflight = prepared_preflight.report;
    emit_progress(
        &run_id,
        "runner",
        None,
        "preflight",
        if preflight.status == RunStatus::Passed {
            ProgressStatus::Passed
        } else {
            ProgressStatus::Failed
        },
    );
    let mut compose_cleanup_guard = ComposeCleanupGuard::new(
        if prepared_preflight.compose_start_attempted {
            compose_stack.as_ref()
        } else {
            None
        },
        executor,
    );
    if preflight.status == RunStatus::PreflightFailed {
        write_pretty_json(&sandbox.join("preflight.json"), &preflight)?;
        let mut cleanup_actions = Vec::new();
        let mut cleanup_errors = Vec::new();
        emit_progress(&run_id, "runner", None, "cleanup", ProgressStatus::Started);
        compose_cleanup_guard.collect_into(&mut cleanup_actions, &mut cleanup_errors);
        let cleanup = CleanupReport {
            status: if cleanup_errors.is_empty() {
                RunStatus::Passed
            } else {
                RunStatus::CleanupFailed
            },
            actions: cleanup_actions,
            errors: cleanup_errors,
        };
        write_pretty_json(&sandbox.join("cleanup.json"), &cleanup)?;
        emit_progress(
            &run_id,
            "runner",
            None,
            "cleanup",
            if cleanup.status == RunStatus::Passed {
                ProgressStatus::Passed
            } else {
                ProgressStatus::Failed
            },
        );
        return finish_run(FinishRunInput {
            run_id: run_id.clone(),
            seed: request.seed,
            status: RunStatus::PreflightFailed,
            exit_code: 2,
            started_at,
            duration_ms: duration_millis(started_instant),
            selected_platform,
            sandbox: sandbox.clone(),
            retention: request.retention,
            report_copy: request.report,
            environment: build_environment_report(EnvironmentInput {
                run_id: &run_id,
                scenarios: &scenario_names,
                seed: request.seed,
                sandbox: &sandbox,
                studiofs_bin: request.studiofs_bin.as_deref(),
                compose_stack: compose_stack.as_ref(),
                compose_project: compose_project.as_deref(),
                allocated_ports: allocated_ports.clone(),
            }),
            preflight,
            scenario_reports: Vec::new(),
            scenario_outcomes: Vec::new(),
            cleanup,
        });
    }
    write_pretty_json(&sandbox.join("preflight.json"), &preflight)?;

    let events_path = sandbox.join("events.ndjson");
    if let Err(primary_error) = record_event(&events_path, &run_id, None, "run", "started") {
        let primary_error_detail = primary_error.to_string();
        write_runner_failure_boundary(
            &sandbox,
            "post-preflight",
            Some(&events_path),
            &primary_error_detail,
        )?;
        let mut cleanup_actions = Vec::new();
        let mut cleanup_errors = Vec::new();
        emit_progress(&run_id, "runner", None, "cleanup", ProgressStatus::Started);
        compose_cleanup_guard.collect_into(&mut cleanup_actions, &mut cleanup_errors);
        let cleanup = CleanupReport {
            status: if cleanup_errors.is_empty() {
                RunStatus::Passed
            } else {
                RunStatus::CleanupFailed
            },
            actions: cleanup_actions,
            errors: cleanup_errors,
        };
        write_pretty_json(&sandbox.join("cleanup.json"), &cleanup)?;
        emit_progress(
            &run_id,
            "runner",
            None,
            "cleanup",
            if cleanup.status == RunStatus::Passed {
                ProgressStatus::Passed
            } else {
                ProgressStatus::Failed
            },
        );
        finish_run(FinishRunInput {
            run_id: run_id.clone(),
            seed: request.seed,
            status: RunStatus::Failed,
            exit_code: 1,
            started_at,
            duration_ms: duration_millis(started_instant),
            selected_platform,
            sandbox: sandbox.clone(),
            retention: request.retention,
            report_copy: request.report,
            environment: build_environment_report(EnvironmentInput {
                run_id: &run_id,
                scenarios: &scenario_names,
                seed: request.seed,
                sandbox: &sandbox,
                studiofs_bin: request.studiofs_bin.as_deref(),
                compose_stack: compose_stack.as_ref(),
                compose_project: compose_project.as_deref(),
                allocated_ports,
            }),
            preflight,
            scenario_reports: Vec::new(),
            scenario_outcomes: Vec::new(),
            cleanup,
        })?;
        return Err(primary_error);
    }

    let mut scenario_reports = Vec::new();
    let mut scenario_outcomes = Vec::new();
    let mut cleanup_errors = Vec::new();
    let scenario_total = scenarios.len();
    for (scenario_index, resolved) in scenarios.into_iter().enumerate() {
        let definition = resolved.definition;
        let scenario_progress = ScenarioProgress {
            index: scenario_index + 1,
            total: scenario_total,
        };
        emit_progress(
            &run_id,
            definition.name,
            Some(scenario_progress),
            "scenario",
            ProgressStatus::Started,
        );
        let context = ScenarioContext {
            run_id: run_id.clone(),
            seed: request.seed,
            sandbox: sandbox.clone(),
            scenario_dir: sandbox
                .join("artifacts")
                .join("scenarios")
                .join(definition.name),
            client_controller_config: match (infra_mode, request.studiofs_bin.as_ref()) {
                (InfraMode::Compose, Some(studiofs_bin)) => {
                    Some(ClientControllerConfig::from_compose_services_for_scenario(
                        studiofs_bin.clone(),
                        sandbox.clone(),
                        &run_id,
                        request.seed,
                        definition.name,
                        &allocated_ports,
                    )?)
                }
                _ => None,
            },
        };

        let lifecycle = run_scenario_lifecycle(
            &events_path,
            &run_id,
            resolved.scenario.as_ref(),
            &context,
            scenario_progress,
        )?;
        if let Some(cleanup_error) = &lifecycle.cleanup_error {
            cleanup_errors.push(format!("{}: {cleanup_error}", lifecycle.outcome.name));
        }
        emit_progress(
            &run_id,
            definition.name,
            Some(scenario_progress),
            "scenario",
            if lifecycle.outcome.status == ScenarioStatus::Passed {
                ProgressStatus::Passed
            } else {
                ProgressStatus::Failed
            },
        );
        scenario_reports.push(lifecycle.report);
        scenario_outcomes.push(lifecycle.outcome);
    }

    record_event(&events_path, &run_id, None, "run", "finished")?;
    let mut cleanup_actions = Vec::new();
    emit_progress(&run_id, "runner", None, "cleanup", ProgressStatus::Started);
    compose_cleanup_guard.collect_into(&mut cleanup_actions, &mut cleanup_errors);

    let cleanup = CleanupReport {
        status: if cleanup_errors.is_empty() {
            RunStatus::Passed
        } else {
            RunStatus::CleanupFailed
        },
        actions: cleanup_actions,
        errors: cleanup_errors,
    };
    write_pretty_json(&sandbox.join("cleanup.json"), &cleanup)?;
    emit_progress(
        &run_id,
        "runner",
        None,
        "cleanup",
        if cleanup.status == RunStatus::Passed {
            ProgressStatus::Passed
        } else {
            ProgressStatus::Failed
        },
    );

    let scenario_failed = scenario_outcomes
        .iter()
        .any(|scenario| scenario.status != ScenarioStatus::Passed);
    let cleanup_failed = cleanup.status == RunStatus::CleanupFailed;
    let status = if scenario_failed {
        RunStatus::Failed
    } else if cleanup_failed {
        RunStatus::CleanupFailed
    } else {
        RunStatus::Passed
    };
    let exit_code = if scenario_failed {
        1
    } else if cleanup_failed {
        4
    } else {
        0
    };
    finish_run(FinishRunInput {
        run_id: run_id.clone(),
        seed: request.seed,
        status,
        exit_code,
        started_at,
        duration_ms: duration_millis(started_instant),
        selected_platform,
        sandbox: sandbox.clone(),
        retention: request.retention,
        report_copy: request.report,
        environment: build_environment_report(EnvironmentInput {
            run_id: &run_id,
            scenarios: &scenario_names,
            seed: request.seed,
            sandbox: &sandbox,
            studiofs_bin: request.studiofs_bin.as_deref(),
            compose_stack: compose_stack.as_ref(),
            compose_project: compose_project.as_deref(),
            allocated_ports,
        }),
        preflight,
        scenario_reports,
        scenario_outcomes,
        cleanup,
    })
}

struct ComposeCleanupGuard<'a> {
    stack: Option<&'a DockerComposeStack>,
    executor: &'a dyn CommandExecutor,
}

impl<'a> ComposeCleanupGuard<'a> {
    fn new(stack: Option<&'a DockerComposeStack>, executor: &'a dyn CommandExecutor) -> Self {
        Self { stack, executor }
    }

    fn collect_into(
        &mut self,
        cleanup_actions: &mut Vec<String>,
        cleanup_errors: &mut Vec<String>,
    ) {
        if let Some(stack) = self.stack.take() {
            collect_compose_cleanup(stack, self.executor, cleanup_actions, cleanup_errors);
        }
    }
}

impl Drop for ComposeCleanupGuard<'_> {
    fn drop(&mut self) {
        if let Some(stack) = self.stack.take() {
            let mut cleanup_actions = Vec::new();
            let mut cleanup_errors = Vec::new();
            collect_compose_cleanup(
                stack,
                self.executor,
                &mut cleanup_actions,
                &mut cleanup_errors,
            );
        }
    }
}

struct FinishRunInput {
    run_id: String,
    seed: u64,
    status: RunStatus,
    exit_code: i32,
    started_at: String,
    duration_ms: u64,
    selected_platform: String,
    sandbox: PathBuf,
    retention: RetentionPolicy,
    report_copy: Option<PathBuf>,
    environment: EnvironmentReport,
    preflight: PreflightReport,
    scenario_reports: Vec<ScenarioReport>,
    scenario_outcomes: Vec<ScenarioOutcome>,
    cleanup: CleanupReport,
}

fn finish_run(input: FinishRunInput) -> Result<RunOutcome, RunnerError> {
    write_pretty_json(&input.sandbox.join("environment.json"), &input.environment)?;
    let report = RunReport {
        schema_version: SCHEMA_VERSION,
        run_id: input.run_id.clone(),
        seed: input.seed,
        status: input.status,
        exit_code: input.exit_code,
        started_at: input.started_at,
        finished_at: timestamp(),
        duration_ms: input.duration_ms,
        runner: RunnerReport {
            crate_name: RUNNER_CRATE.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: option_env!("GITHUB_SHA").unwrap_or("unknown").to_string(),
        },
        host: HostReport {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            platform: input.selected_platform,
        },
        sandbox: SandboxReport {
            root: input.sandbox.display().to_string(),
            retention: input.retention,
        },
        environment: input.environment,
        preflight: input.preflight,
        scenarios: input.scenario_reports,
        artifacts: collect_report_artifacts(&input.sandbox, &input.scenario_outcomes)?,
        cleanup: input.cleanup,
    };

    let report_path = input.sandbox.join("report.json");
    write_pretty_json(&report_path, &report)?;
    if let Some(copy_target) = input.report_copy {
        write_pretty_json(&copy_target, &report)?;
    }

    Ok(RunOutcome {
        run_id: input.run_id,
        status: input.status,
        exit_code: input.exit_code,
        sandbox: input.sandbox,
        report_path,
        scenarios: input.scenario_outcomes,
    })
}

struct EnvironmentInput<'a> {
    run_id: &'a str,
    scenarios: &'a [String],
    seed: u64,
    sandbox: &'a Path,
    studiofs_bin: Option<&'a Path>,
    compose_stack: Option<&'a DockerComposeStack>,
    compose_project: Option<&'a str>,
    allocated_ports: Vec<crate::infra::ServicePort>,
}

fn build_environment_report(input: EnvironmentInput<'_>) -> EnvironmentReport {
    let compose_images_result = input
        .compose_stack
        .map(|stack| stack.compose_images().map_err(|err| err.to_string()));
    let (compose_images, compose_images_error) = match compose_images_result {
        Some(Ok(images)) => (
            images
                .into_iter()
                .map(|image| ComposeImageReport {
                    service: image.service,
                    image: image.image,
                })
                .collect(),
            None,
        ),
        Some(Err(error)) => (Vec::new(), Some(error)),
        None => (Vec::new(), None),
    };
    let client_roots: Vec<_> = DEFAULT_CLIENT_NAMES
        .iter()
        .map(|client| client_root_dir(input.sandbox, client))
        .collect();
    let mount_roots: Vec<_> = DEFAULT_CLIENT_NAMES
        .iter()
        .map(|client| client_mount_point(input.sandbox, client))
        .collect();
    let ownership = path_ownership_reports(&client_roots, &mount_roots);
    EnvironmentReport {
        run_id: input.run_id.to_string(),
        scenarios: input.scenarios.to_vec(),
        seed: input.seed,
        studiofs_bin: input.studiofs_bin.map(|path| path.display().to_string()),
        compose_project: input.compose_project.map(str::to_string),
        compose_images,
        compose_images_error,
        allocated_ports: input.allocated_ports,
        client_roots: client_roots
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        mount_roots: mount_roots
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        client_root_ownership: ownership.client_roots,
        mount_root_ownership: ownership.mount_roots,
        winfsp_version: detect_winfsp_version(),
    }
}

struct EnvironmentOwnership {
    client_roots: Vec<PathOwnershipReport>,
    mount_roots: Vec<PathOwnershipReport>,
}

fn path_ownership_reports(
    client_roots: &[PathBuf],
    mount_roots: &[PathBuf],
) -> EnvironmentOwnership {
    let mut paths = Vec::with_capacity(client_roots.len() + mount_roots.len());
    paths.extend(client_roots.iter().cloned());
    paths.extend(mount_roots.iter().cloned());
    let reports = detect_path_owners(&paths);
    let mut reports = reports.into_iter();
    EnvironmentOwnership {
        client_roots: reports.by_ref().take(client_roots.len()).collect(),
        mount_roots: reports.collect(),
    }
}

fn fallback_path_ownership_reports(paths: &[PathBuf]) -> Vec<PathOwnershipReport> {
    paths
        .iter()
        .map(|path| PathOwnershipReport {
            path: path.display().to_string(),
            owner: None,
            error: None,
        })
        .collect()
}

fn detect_winfsp_version() -> Option<String> {
    if let Some(version) = std::env::var("STUDIOFS_E2E_WINFSP_VERSION")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(version);
    }
    detect_winfsp_version_from_host()
}

#[cfg(windows)]
fn detect_winfsp_version_from_host() -> Option<String> {
    let output = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            "$paths = @('HKLM:\\SOFTWARE\\WinFsp', 'HKLM:\\SOFTWARE\\WOW6432Node\\WinFsp'); foreach ($path in $paths) { if (Test-Path $path) { $item = Get-ItemProperty -Path $path; foreach ($name in @('Version', 'ProductVersion')) { $value = $item.$name; if ($value) { Write-Output $value; exit 0 } } } }; exit 0",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
}

#[cfg(not(windows))]
fn detect_winfsp_version_from_host() -> Option<String> {
    None
}

#[cfg(windows)]
fn detect_path_owners(paths: &[PathBuf]) -> Vec<PathOwnershipReport> {
    let paths_json = match serde_json::to_string(
        &paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>(),
    ) {
        Ok(paths_json) => paths_json,
        Err(err) => {
            return paths
                .iter()
                .map(|path| PathOwnershipReport {
                    path: path.display().to_string(),
                    owner: None,
                    error: Some(err.to_string()),
                })
                .collect();
        }
    };
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/get-path-ownership.ps1");
    let script_arg = script.display().to_string();
    let output = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            &script_arg,
        ])
        .env("STUDIOFS_E2E_OWNERSHIP_PATHS_JSON", paths_json)
        .output()
        .ok();
    let Some(output) = output else {
        return fallback_path_ownership_reports(paths);
    };
    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return paths
            .iter()
            .map(|path| PathOwnershipReport {
                path: path.display().to_string(),
                owner: None,
                error: Some(if error.is_empty() {
                    format!("path ownership probe exited with status {}", output.status)
                } else {
                    error.clone()
                }),
            })
            .collect();
    }
    serde_json::from_slice(&output.stdout).unwrap_or_else(|err| {
        paths
            .iter()
            .map(|path| PathOwnershipReport {
                path: path.display().to_string(),
                owner: None,
                error: Some(err.to_string()),
            })
            .collect()
    })
}

#[cfg(unix)]
fn detect_path_owners(paths: &[PathBuf]) -> Vec<PathOwnershipReport> {
    use std::os::unix::fs::MetadataExt as _;

    paths
        .iter()
        .map(|path| match fs::metadata(path) {
            Ok(metadata) => PathOwnershipReport {
                path: path.display().to_string(),
                owner: Some(format!("uid:{} gid:{}", metadata.uid(), metadata.gid())),
                error: None,
            },
            Err(err) => PathOwnershipReport {
                path: path.display().to_string(),
                owner: None,
                error: Some(err.to_string()),
            },
        })
        .collect()
}

#[cfg(not(any(windows, unix)))]
fn detect_path_owners(paths: &[PathBuf]) -> Vec<PathOwnershipReport> {
    fallback_path_ownership_reports(paths)
}

struct PreparedPreflight {
    report: PreflightReport,
    compose_start_attempted: bool,
}

fn prepare_preflight(
    root: &Path,
    sandbox: &Path,
    platform: PlatformSelection,
    compose_stack: Option<&DockerComposeStack>,
    executor: &dyn CommandExecutor,
) -> Result<PreparedPreflight, RunnerError> {
    let mut runner_checks = runner_preflight_checks(root, platform, executor);
    let Some(stack) = compose_stack else {
        write_pretty_json(
            &sandbox.join("ports.json"),
            &serde_json::json!({ "ports": [] }),
        )?;
        runner_checks.push(PreflightCheckReport {
            name: "runner-skeleton".to_string(),
            status: RunStatus::Passed,
            detail: "external infrastructure disabled for this run".to_string(),
        });
        let status = if runner_checks
            .iter()
            .any(|check| check.status != RunStatus::Passed)
        {
            RunStatus::PreflightFailed
        } else {
            RunStatus::Passed
        };
        return Ok(PreparedPreflight {
            report: PreflightReport {
                status,
                checks: runner_checks,
            },
            compose_start_attempted: false,
        });
    };

    stack.prepare_artifacts()?;
    let mut checks = runner_checks;
    checks.extend(
        stack
            .preflight(executor)
            .into_iter()
            .map(preflight_check_report),
    );
    if checks.iter().any(|check| check.status != RunStatus::Passed) {
        return Ok(PreparedPreflight {
            report: PreflightReport {
                status: RunStatus::PreflightFailed,
                checks,
            },
            compose_start_attempted: false,
        });
    }

    match stack.start(executor) {
        Ok(()) => checks.push(PreflightCheckReport {
            name: "docker compose up --wait".to_string(),
            status: RunStatus::Passed,
            detail: "required services are running or healthy".to_string(),
        }),
        Err(err) => {
            checks.push(PreflightCheckReport {
                name: "docker compose up --wait".to_string(),
                status: RunStatus::PreflightFailed,
                detail: err.to_string(),
            });
            return Ok(PreparedPreflight {
                report: PreflightReport {
                    status: RunStatus::PreflightFailed,
                    checks,
                },
                compose_start_attempted: true,
            });
        }
    }

    Ok(PreparedPreflight {
        report: PreflightReport {
            status: RunStatus::Passed,
            checks,
        },
        compose_start_attempted: true,
    })
}

fn runner_preflight_checks(
    root: &Path,
    platform: PlatformSelection,
    executor: &dyn CommandExecutor,
) -> Vec<PreflightCheckReport> {
    let Some(command) = stale_process_cleanup_command(root, platform) else {
        return Vec::new();
    };
    vec![match executor.run(&command) {
        Ok(output) if output.status == 0 => PreflightCheckReport {
            name: "runner-owned stale process cleanup".to_string(),
            status: RunStatus::Passed,
            detail: stale_process_cleanup_detail(&output.stdout),
        },
        Ok(output) => PreflightCheckReport {
            name: "runner-owned stale process cleanup".to_string(),
            status: RunStatus::PreflightFailed,
            detail: command_failure_detail(output.status, &output.stderr),
        },
        Err(err) => PreflightCheckReport {
            name: "runner-owned stale process cleanup".to_string(),
            status: RunStatus::PreflightFailed,
            detail: err.to_string(),
        },
    }]
}

fn stale_process_cleanup_detail(stdout: &str) -> String {
    let detail = stdout.trim();
    if detail.is_empty() {
        "no runner-owned stale StudioFS processes found".to_string()
    } else {
        format!("cleaned runner-owned stale StudioFS processes:\n{detail}")
    }
}

fn command_failure_detail(status: i32, stderr: &str) -> String {
    let stderr = stderr.trim();
    if stderr.is_empty() {
        format!("exit status {status}")
    } else {
        format!("exit status {status}: {stderr}")
    }
}

fn stale_process_cleanup_command(root: &Path, platform: PlatformSelection) -> Option<CommandSpec> {
    let root = absolute_without_requiring_exists(root);
    let (program, args) = match platform.resolved() {
        "windows-winfsp" => {
            let script =
                Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/check-stale-processes.ps1");
            (
                "powershell".to_string(),
                vec![
                    "-NoProfile".to_string(),
                    "-ExecutionPolicy".to_string(),
                    "Bypass".to_string(),
                    "-File".to_string(),
                    script.display().to_string(),
                ],
            )
        }
        "linux-fuse" | "macos-fuse" => {
            let script =
                Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/check-stale-processes.sh");
            ("sh".to_string(), vec![script.display().to_string()])
        }
        _ => return None,
    };
    Some(CommandSpec {
        program,
        args,
        env: vec![
            (
                "STUDIOFS_E2E_RUNNER_ROOT".to_string(),
                root.display().to_string(),
            ),
            (
                "STUDIOFS_E2E_SELF_PID".to_string(),
                std::process::id().to_string(),
            ),
            (
                "STUDIOFS_E2E_CLEAN_STALE_PROCESSES".to_string(),
                "1".to_string(),
            ),
        ],
        cwd: None,
    })
}

fn preflight_check_report(check: InfraCheck) -> PreflightCheckReport {
    PreflightCheckReport {
        name: check.name,
        status: if check.passed {
            RunStatus::Passed
        } else {
            RunStatus::PreflightFailed
        },
        detail: check.detail,
    }
}

fn collect_compose_cleanup(
    stack: &DockerComposeStack,
    executor: &dyn CommandExecutor,
    cleanup_actions: &mut Vec<String>,
    cleanup_errors: &mut Vec<String>,
) {
    match stack.collect_logs(executor) {
        Ok(_) => cleanup_actions.push("docker compose logs".to_string()),
        Err(err) => cleanup_errors.push(format!("docker compose logs: {err}")),
    }
    match stack.collect_optional_diagnostics(executor) {
        Ok(actions) => cleanup_actions.extend(actions),
        Err(err) => cleanup_actions.push(format!("optional docker diagnostics skipped: {err}")),
    }
    match stack.teardown(executor) {
        Ok(()) => {
            cleanup_actions.push("docker compose down --volumes --remove-orphans".to_string());
        }
        Err(err) => cleanup_errors.push(format!("docker compose down: {err}")),
    }
}

pub fn clean(request: CleanRequest) -> Result<CleanOutcome, RunnerError> {
    if !request.root.exists() {
        return Ok(CleanOutcome {
            removed: Vec::new(),
            skipped: Vec::new(),
        });
    }
    let root = canonicalize_cleanup_root(&request.root)?;
    let mut removed = Vec::new();
    let mut skipped = Vec::new();
    for entry_result in fs::read_dir(&root)? {
        let entry = match entry_result {
            Ok(entry) => entry,
            Err(err) => {
                skipped.push(CleanSkipped {
                    path: root.clone(),
                    error: err.to_string(),
                });
                continue;
            }
        };
        let path = entry.path();
        match clean_entry(entry, &root) {
            Ok(Some(name)) => removed.push(name),
            Ok(None) => {}
            Err(err) => skipped.push(CleanSkipped {
                path: path
                    .strip_prefix(&root)
                    .map(Path::to_path_buf)
                    .unwrap_or(path),
                error: err.to_string(),
            }),
        }
    }
    removed.sort();
    skipped.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(CleanOutcome { removed, skipped })
}

fn clean_entry(entry: fs::DirEntry, root: &Path) -> Result<Option<PathBuf>, RunnerError> {
    if !entry.file_type()?.is_dir() {
        return Ok(None);
    }
    let path = entry.path();
    if !is_runner_owned_sandbox(&path)? {
        return Ok(None);
    }
    let canonical = path.canonicalize()?;
    if !canonical.starts_with(root) {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "{} escapes {}",
            canonical.display(),
            root.display()
        )));
    }
    let name = entry.file_name();
    fs::remove_dir_all(&path)?;
    Ok(Some(PathBuf::from(name)))
}

fn resolve_scenarios(names: &[String]) -> Result<Vec<ResolvedScenario>, RunnerError> {
    names
        .iter()
        .map(|name| {
            let scenario = scenario::resolve_scenario(name)
                .ok_or_else(|| RunnerError::UnknownScenario(name.clone()))?;
            Ok(ResolvedScenario {
                definition: scenario.definition(),
                scenario,
            })
        })
        .collect()
}

fn create_sandbox_layout(
    sandbox: &Path,
    scenarios: &[ResolvedScenario],
) -> Result<(), RunnerError> {
    fs::create_dir_all(sandbox)?;
    fs::create_dir_all(sandbox.join("artifacts/docker"))?;
    fs::create_dir_all(sandbox.join("artifacts/commands"))?;
    for client in DEFAULT_CLIENT_NAMES {
        fs::create_dir_all(client_root_dir(sandbox, client))?;
        fs::create_dir_all(client_cache_dir(sandbox, client))?;
        fs::create_dir_all(client_mount_point(sandbox, client))?;
        fs::write(
            crate::client::client_stdout_log(sandbox, client),
            "studiofs client stdout log reserved\n",
        )?;
        fs::write(crate::client::client_stderr_log(sandbox, client), "")?;
        fs::write(crate::client::client_tracing_log(sandbox, client), "")?;
    }
    for scenario in scenarios {
        fs::create_dir_all(
            sandbox
                .join("artifacts/scenarios")
                .join(scenario.definition.name),
        )?;
    }
    Ok(())
}

struct ManifestInput<'a> {
    run_id: &'a str,
    seed: u64,
    sandbox: &'a Path,
    scenarios: &'a [String],
    retention: RetentionPolicy,
    selected_platform: &'a str,
    infra: InfraMode,
    compose_project: Option<&'a str>,
    allocated_ports: Vec<crate::infra::ServicePort>,
    studiofs_bin: Option<&'a Path>,
    port_range: Option<&'a str>,
    timeout: Option<&'a str>,
    created_at: &'a str,
}

fn build_manifest(input: ManifestInput<'_>) -> RunManifest {
    let explicit_client_roots: Vec<_> = DEFAULT_CLIENT_NAMES
        .iter()
        .map(|client| client_root_dir(input.sandbox, client).display().to_string())
        .collect();
    let mut cleanup_allowlist = Vec::with_capacity(explicit_client_roots.len() + 1);
    cleanup_allowlist.push(input.sandbox.display().to_string());
    cleanup_allowlist.extend(explicit_client_roots.iter().cloned());
    RunManifest {
        schema_version: SCHEMA_VERSION,
        runner_crate: RUNNER_CRATE.to_string(),
        run_id: input.run_id.to_string(),
        seed: input.seed,
        sandbox: input.sandbox.display().to_string(),
        scenarios: input.scenarios.to_vec(),
        retention: input.retention,
        selected_platform: input.selected_platform.to_string(),
        infra: input.infra,
        compose_project: input.compose_project.map(str::to_string),
        allocated_ports: input.allocated_ports,
        studiofs_bin: input.studiofs_bin.map(|path| path.display().to_string()),
        port_range: input.port_range.map(str::to_string),
        timeout: input.timeout.map(str::to_string),
        explicit_client_roots,
        cleanup_allowlist,
        created_at: input.created_at.to_string(),
    }
}

fn run_scenario_lifecycle(
    events_path: &Path,
    run_id: &str,
    scenario: &dyn scenario::Scenario,
    context: &ScenarioContext,
    progress: ScenarioProgress,
) -> Result<ScenarioLifecycleOutcome, RunnerError> {
    let definition = scenario.definition();
    let started_at = timestamp();
    let started_instant = Instant::now();
    let operation_log = context.scenario_dir.join("operations.ndjson");
    let operation_artifact = scenario_artifact_path(definition.name, "operations.ndjson");

    fs::create_dir_all(&context.scenario_dir)?;
    let result = run_scenario_body(
        events_path,
        &operation_log,
        run_id,
        scenario,
        context,
        progress,
    );
    let cleanup_result = run_hook(
        events_path,
        &operation_log,
        run_id,
        definition.name,
        Some(progress),
        "cleanup",
        || scenario.cleanup(context),
    );

    match (result, cleanup_result) {
        (Ok(mut artifacts), Ok(())) => {
            let finished_at = timestamp();
            artifacts.insert(0, operation_artifact);
            Ok(ScenarioLifecycleOutcome {
                report: ScenarioReport {
                    name: definition.name.to_string(),
                    seed: context.seed,
                    status: ScenarioStatus::Passed,
                    started_at,
                    finished_at,
                    duration_ms: duration_millis(started_instant),
                    artifacts: artifacts.clone(),
                    error: None,
                },
                outcome: ScenarioOutcome {
                    name: definition.name.to_string(),
                    status: ScenarioStatus::Passed,
                    artifacts,
                },
                cleanup_error: None,
            })
        }
        (body_result, cleanup_result) => {
            let scenario_failed = body_result.is_err();
            let primary_error = body_result.err().map(|err| err.to_string());
            let cleanup_error = cleanup_result.err().map(|err| err.to_string());
            let finished_at = timestamp();
            let artifacts = vec![operation_artifact];
            Ok(ScenarioLifecycleOutcome {
                report: ScenarioReport {
                    name: definition.name.to_string(),
                    seed: context.seed,
                    status: if primary_error.is_some() {
                        ScenarioStatus::Failed
                    } else {
                        ScenarioStatus::Passed
                    },
                    started_at,
                    finished_at,
                    duration_ms: duration_millis(started_instant),
                    artifacts: artifacts.clone(),
                    error: primary_error,
                },
                outcome: ScenarioOutcome {
                    name: definition.name.to_string(),
                    status: if scenario_failed {
                        ScenarioStatus::Failed
                    } else {
                        ScenarioStatus::Passed
                    },
                    artifacts,
                },
                cleanup_error,
            })
        }
    }
}

fn run_scenario_body(
    events_path: &Path,
    operation_log: &Path,
    run_id: &str,
    scenario: &dyn scenario::Scenario,
    context: &ScenarioContext,
    progress: ScenarioProgress,
) -> Result<Vec<String>, RunnerError> {
    let definition = scenario.definition();
    run_hook(
        events_path,
        operation_log,
        run_id,
        definition.name,
        Some(progress),
        "setup",
        || scenario.setup(context),
    )?;
    run_hook(
        events_path,
        operation_log,
        run_id,
        definition.name,
        Some(progress),
        "run",
        || scenario.run(context),
    )?;
    run_hook(
        events_path,
        operation_log,
        run_id,
        definition.name,
        Some(progress),
        "validate",
        || scenario.validate(context),
    )
}

fn run_hook<T>(
    events_path: &Path,
    operation_log: &Path,
    run_id: &str,
    scenario: &str,
    progress: Option<ScenarioProgress>,
    hook: &str,
    f: impl FnOnce() -> Result<T, scenario::ScenarioError>,
) -> Result<T, RunnerError> {
    emit_progress(run_id, scenario, progress, hook, ProgressStatus::Started);
    record_event(events_path, run_id, Some(scenario), hook, "started")?;
    record_event(operation_log, run_id, Some(scenario), hook, "started")?;
    let heartbeat = ProgressHeartbeat::start();
    let result = f();
    heartbeat.stop();
    let phase = if result.is_ok() { "finished" } else { "failed" };
    record_event(events_path, run_id, Some(scenario), hook, phase)?;
    record_event(operation_log, run_id, Some(scenario), hook, phase)?;
    emit_progress(
        run_id,
        scenario,
        progress,
        hook,
        if result.is_ok() {
            ProgressStatus::Passed
        } else {
            ProgressStatus::Failed
        },
    );
    Ok(result?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressStatus {
    Started,
    Passed,
    Failed,
}

impl ProgressStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScenarioProgress {
    index: usize,
    total: usize,
}

fn progress_line(
    timestamp: &str,
    run_id: &str,
    task: &str,
    progress: Option<ScenarioProgress>,
    phase: &str,
    status: ProgressStatus,
) -> String {
    let progress = progress
        .map(|progress| format!(" scenario_progress={}/{}", progress.index, progress.total))
        .unwrap_or_default();
    format!(
        "timestamp={timestamp} studiofs-e2e run_id={run_id} task={task}{progress} phase={phase} status={}",
        status.as_str()
    )
}

fn emit_progress(
    run_id: &str,
    task: &str,
    progress: Option<ScenarioProgress>,
    phase: &str,
    status: ProgressStatus,
) {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(
        stderr,
        "{}",
        progress_line(&timestamp(), run_id, task, progress, phase, status)
    );
    let _ = stderr.flush();
}

fn emit_progress_heartbeat(tick: usize) {
    let pulse = "*".repeat(tick);
    let mut stderr = std::io::stderr().lock();
    let _ = write!(stderr, "\r{pulse}");
    let _ = stderr.flush();
}

fn clear_progress_heartbeat(width: usize) {
    if width == 0 {
        return;
    }
    let mut stderr = std::io::stderr().lock();
    let _ = write!(stderr, "\r{}\r", " ".repeat(width));
    let _ = stderr.flush();
}

struct ProgressHeartbeat {
    stop: mpsc::Sender<()>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ProgressHeartbeat {
    fn start() -> Self {
        let (stop, stop_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let mut tick = 0usize;
            while stop_rx.recv_timeout(PROGRESS_HEARTBEAT_INTERVAL).is_err() {
                tick = tick.saturating_add(1);
                emit_progress_heartbeat(tick);
            }
            clear_progress_heartbeat(tick);
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn stop(mut self) {
        let _ = self.stop.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn record_event(
    events_path: &Path,
    run_id: &str,
    scenario: Option<&str>,
    hook: &str,
    phase: &str,
) -> Result<(), RunnerError> {
    let record = EventRecord {
        timestamp: timestamp(),
        run_id: run_id.to_string(),
        scenario: scenario.map(str::to_string),
        hook: hook.to_string(),
        phase: phase.to_string(),
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(events_path)?;
    serde_json::to_writer(&mut file, &record)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn collect_report_artifacts(
    sandbox: &Path,
    scenarios: &[ScenarioOutcome],
) -> Result<Vec<ArtifactReport>, RunnerError> {
    let mut artifacts = Vec::new();
    let mut seen = BTreeSet::new();
    for artifact in required_artifacts(scenarios) {
        add_artifact(&mut artifacts, &mut seen, artifact);
    }
    let required_count = artifacts.len();
    collect_existing_artifacts(
        sandbox,
        &sandbox.join("artifacts"),
        &mut artifacts,
        &mut seen,
    )?;
    artifacts[required_count..].sort_by(|left, right| left.path.cmp(&right.path));
    Ok(artifacts)
}

fn required_artifacts(scenarios: &[ScenarioOutcome]) -> Vec<ArtifactReport> {
    let mut artifacts = vec![
        ArtifactReport {
            name: RUNNER_MARKER_FILE.to_string(),
            path: RUNNER_MARKER_FILE.to_string(),
            required: true,
        },
        ArtifactReport {
            name: "report.json".to_string(),
            path: "report.json".to_string(),
            required: true,
        },
        ArtifactReport {
            name: "events.ndjson".to_string(),
            path: "events.ndjson".to_string(),
            required: true,
        },
        ArtifactReport {
            name: "stdout.log".to_string(),
            path: "stdout.log".to_string(),
            required: true,
        },
        ArtifactReport {
            name: "stderr.log".to_string(),
            path: "stderr.log".to_string(),
            required: true,
        },
        ArtifactReport {
            name: "run.json".to_string(),
            path: "run.json".to_string(),
            required: true,
        },
        ArtifactReport {
            name: "preflight.json".to_string(),
            path: "preflight.json".to_string(),
            required: true,
        },
        ArtifactReport {
            name: "ports.json".to_string(),
            path: "ports.json".to_string(),
            required: true,
        },
        ArtifactReport {
            name: "cleanup.json".to_string(),
            path: "cleanup.json".to_string(),
            required: true,
        },
        ArtifactReport {
            name: "environment.json".to_string(),
            path: "environment.json".to_string(),
            required: true,
        },
    ];
    for scenario in scenarios {
        for path in &scenario.artifacts {
            artifacts.push(ArtifactReport {
                name: artifact_name(path),
                path: path.clone(),
                required: false,
            });
        }
    }
    artifacts
}

fn collect_existing_artifacts(
    sandbox: &Path,
    dir: &Path,
    artifacts: &mut Vec<ArtifactReport>,
    seen: &mut BTreeSet<String>,
) -> Result<(), RunnerError> {
    if !dir.exists() {
        return Ok(());
    }
    for entry_result in fs::read_dir(dir)? {
        let entry = entry_result?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_existing_artifacts(sandbox, &path, artifacts, seen)?;
        } else if file_type.is_file() || file_type.is_symlink() {
            let relative = normalize_relative_path(&path, sandbox)?;
            add_artifact(
                artifacts,
                seen,
                ArtifactReport {
                    name: artifact_name(&relative),
                    path: relative,
                    required: false,
                },
            );
        }
    }
    Ok(())
}

fn add_artifact(
    artifacts: &mut Vec<ArtifactReport>,
    seen: &mut BTreeSet<String>,
    artifact: ArtifactReport,
) {
    if seen.insert(artifact.path.clone()) {
        artifacts.push(artifact);
    }
}

fn artifact_name(path: &str) -> String {
    let normalized = path.replace('\\', "/");
    if normalized.trim_matches('/').is_empty() {
        "artifact".to_string()
    } else {
        normalized
    }
}

fn scenario_artifact_path(scenario: &str, artifact: &str) -> String {
    format!("artifacts/scenarios/{scenario}/{artifact}")
}

fn normalize_relative_path(path: &Path, root: &Path) -> Result<String, RunnerError> {
    let relative =
        path.strip_prefix(root)
            .map_err(|_| RunnerError::ArtifactPathOutsideSandbox {
                path: path.to_path_buf(),
                sandbox: root.to_path_buf(),
            })?;
    let mut components = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| RunnerError::NonUtf8ArtifactPath {
                        path: path.to_path_buf(),
                    })?;
                components.push(value.to_string());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(RunnerError::UnsafeArtifactPath {
                    path: relative.to_path_buf(),
                });
            }
        }
    }
    if components.is_empty() {
        return Err(RunnerError::UnsafeArtifactPath {
            path: relative.to_path_buf(),
        });
    }
    Ok(components.join("/"))
}

fn write_log_files(sandbox: &Path) -> Result<(), RunnerError> {
    fs::write(sandbox.join("stdout.log"), "studiofs-e2e runner\n")?;
    fs::write(sandbox.join("stderr.log"), "")?;
    Ok(())
}

fn write_pretty_json<T: Serialize>(path: &Path, value: &T) -> Result<(), RunnerError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn write_runner_marker(sandbox: &Path, run_id: &str) -> Result<(), RunnerError> {
    write_pretty_json(
        &sandbox.join(RUNNER_MARKER_FILE),
        &serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "runner_crate": RUNNER_CRATE,
            "run_id": run_id,
            "sandbox": sandbox.display().to_string(),
        }),
    )
}

fn write_runner_cleanup_boundary(
    sandbox: &Path,
    manifest: &RunManifest,
) -> Result<(), RunnerError> {
    write_pretty_json(
        &sandbox.join("artifacts/runner/cleanup-boundary.json"),
        &serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "run_id": &manifest.run_id,
            "sandbox": &manifest.sandbox,
            "cleanup_allowlist": &manifest.cleanup_allowlist,
            "explicit_client_roots": &manifest.explicit_client_roots,
            "compose_project": &manifest.compose_project,
        }),
    )
}

fn write_runner_failure_boundary(
    sandbox: &Path,
    phase: &str,
    event_log: Option<&Path>,
    primary_error: &str,
) -> Result<(), RunnerError> {
    write_pretty_json(
        &sandbox.join("artifacts/runner/failure-boundary.json"),
        &serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "phase": phase,
            "event_log": event_log.map(|path| path.display().to_string()),
            "primary_error": primary_error,
        }),
    )
}

fn is_runner_owned_sandbox(path: &Path) -> Result<bool, RunnerError> {
    let manifest = path.join("run.json");
    let marker = path.join(RUNNER_MARKER_FILE);
    if !manifest.exists() || !marker.exists() {
        return Ok(false);
    }
    let value: serde_json::Value = serde_json::from_slice(&fs::read(manifest)?)?;
    let marker_value: serde_json::Value = serde_json::from_slice(&fs::read(marker)?)?;
    if value["runner_crate"] != RUNNER_CRATE || marker_value["runner_crate"] != RUNNER_CRATE {
        return Ok(false);
    }
    let Some(run_id) = value["run_id"].as_str() else {
        return Ok(false);
    };
    if marker_value["run_id"] != run_id {
        return Ok(false);
    }
    if path.file_name().and_then(|name| name.to_str()) != Some(run_id) {
        return Ok(false);
    }
    let Some(manifest_sandbox) = value["sandbox"].as_str() else {
        return Ok(false);
    };
    if !Path::new(manifest_sandbox).exists()
        || Path::new(manifest_sandbox).canonicalize()? != path.canonicalize()?
    {
        return Ok(false);
    }
    for required in [
        RUNNER_MARKER_FILE,
        "run.json",
        "report.json",
        "events.ndjson",
        "preflight.json",
        "cleanup.json",
        "artifacts",
    ] {
        if !path.join(required).exists() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn canonicalize_cleanup_root(root: &Path) -> Result<PathBuf, RunnerError> {
    let canonical = root.canonicalize()?;
    if canonical.parent().is_none() {
        return Err(RunnerError::UnsafeCleanupRoot(
            canonical.display().to_string(),
        ));
    }
    Ok(canonical)
}

fn validate_run_id(run_id: &str) -> Result<(), RunnerError> {
    let safe = !run_id.is_empty()
        && run_id != "."
        && run_id != ".."
        && !run_id.starts_with('.')
        && run_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'));
    if safe {
        Ok(())
    } else {
        Err(RunnerError::UnsafeRunId(run_id.to_string()))
    }
}

fn absolutize(path: &Path) -> Result<PathBuf, RunnerError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn absolute_without_requiring_exists(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

fn generated_run_id() -> String {
    let suffix = Uuid::new_v4().simple().to_string();
    format!("{}-{}", Utc::now().format("%Y%m%dT%H%M%SZ"), &suffix[..6])
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn duration_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn host_platform() -> &'static str {
    match std::env::consts::OS {
        "windows" => "windows-winfsp",
        "macos" => "macos-fuse",
        "linux" => "linux-fuse",
        _ => "auto",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::{CommandExecutor, CommandOutput, InfraMode};
    use std::cell::{Cell, RefCell};

    #[derive(Debug)]
    struct FailingValidateScenario {
        calls: RefCell<Vec<&'static str>>,
        cleaned: Cell<bool>,
    }

    impl FailingValidateScenario {
        fn new() -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
                cleaned: Cell::new(false),
            }
        }
    }

    impl scenario::Scenario for FailingValidateScenario {
        fn definition(&self) -> scenario::ScenarioDefinition {
            scenario::ScenarioDefinition {
                name: "failing-validate",
                description: "test scenario",
                requires_studiofs_bin: false,
            }
        }

        fn setup(&self, _context: &ScenarioContext) -> Result<(), scenario::ScenarioError> {
            self.calls.borrow_mut().push("setup");
            Ok(())
        }

        fn run(&self, _context: &ScenarioContext) -> Result<(), scenario::ScenarioError> {
            self.calls.borrow_mut().push("run");
            Ok(())
        }

        fn validate(
            &self,
            _context: &ScenarioContext,
        ) -> Result<Vec<String>, scenario::ScenarioError> {
            self.calls.borrow_mut().push("validate");
            Err(scenario::ScenarioError::Json(serde_json::Error::io(
                std::io::Error::other("validation failed"),
            )))
        }

        fn cleanup(&self, _context: &ScenarioContext) -> Result<(), scenario::ScenarioError> {
            self.calls.borrow_mut().push("cleanup");
            self.cleaned.set(true);
            Ok(())
        }
    }

    #[test]
    fn scenario_lifecycle_runs_cleanup_after_validate_failure() {
        let temp = tempfile::tempdir().unwrap();
        let events_path = temp.path().join("events.ndjson");
        let context = ScenarioContext {
            run_id: "run-1".to_string(),
            seed: 1,
            sandbox: temp.path().to_path_buf(),
            scenario_dir: temp.path().join("scenario"),
            client_controller_config: None,
        };
        let scenario = FailingValidateScenario::new();

        let outcome = run_scenario_lifecycle(
            &events_path,
            "run-1",
            &scenario,
            &context,
            ScenarioProgress { index: 1, total: 1 },
        )
        .unwrap();

        assert_eq!(outcome.outcome.status, ScenarioStatus::Failed);
        assert!(outcome.report.error.unwrap().contains("validation failed"));
        assert!(scenario.cleaned.get());
        assert_eq!(
            scenario.calls.borrow().as_slice(),
            ["setup", "run", "validate", "cleanup"]
        );
    }

    #[test]
    fn progress_line_names_task_phase_and_status() {
        assert_eq!(
            progress_line(
                "2026-05-11T16:40:00.000Z",
                "run-1",
                "collision-rename-vs-rename",
                Some(ScenarioProgress { index: 3, total: 8 }),
                "validate",
                ProgressStatus::Failed
            ),
            "timestamp=2026-05-11T16:40:00.000Z studiofs-e2e run_id=run-1 task=collision-rename-vs-rename scenario_progress=3/8 phase=validate status=failed"
        );
    }

    #[test]
    fn progress_line_omits_scenario_progress_for_runner_tasks() {
        assert_eq!(
            progress_line(
                "2026-05-11T16:40:00.000Z",
                "run-1",
                "runner",
                None,
                "preflight",
                ProgressStatus::Started
            ),
            "timestamp=2026-05-11T16:40:00.000Z studiofs-e2e run_id=run-1 task=runner phase=preflight status=started"
        );
    }

    #[test]
    fn artifact_report_names_include_relative_path_context() {
        let artifacts = required_artifacts(&[
            ScenarioOutcome {
                name: "a".to_string(),
                status: ScenarioStatus::Passed,
                artifacts: vec!["artifacts/scenarios/a/logs.txt".to_string()],
            },
            ScenarioOutcome {
                name: "b".to_string(),
                status: ScenarioStatus::Passed,
                artifacts: vec!["artifacts/scenarios/b/logs.txt".to_string()],
            },
        ]);
        let names: Vec<_> = artifacts
            .iter()
            .filter(|artifact| artifact.path.ends_with("logs.txt"))
            .map(|artifact| artifact.name.as_str())
            .collect();

        assert_eq!(
            names,
            [
                "artifacts/scenarios/a/logs.txt",
                "artifacts/scenarios/b/logs.txt"
            ]
        );
    }

    #[test]
    fn normalize_relative_path_rejects_paths_outside_sandbox() {
        let temp = tempfile::tempdir().unwrap();
        let sandbox = temp.path().join("run-1");
        let outside = temp.path().join("outside.log");

        let err = normalize_relative_path(&outside, &sandbox).unwrap_err();

        assert!(matches!(
            err,
            RunnerError::ArtifactPathOutsideSandbox { .. }
        ));
    }

    #[test]
    fn normalize_relative_path_rejects_parent_components() {
        let temp = tempfile::tempdir().unwrap();
        let sandbox = temp.path().join("run-1");
        let traversal = sandbox.join("artifacts").join("..").join("outside.log");

        let err = normalize_relative_path(&traversal, &sandbox).unwrap_err();

        assert!(matches!(err, RunnerError::UnsafeArtifactPath { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn normalize_relative_path_rejects_non_utf8_components() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;

        let temp = tempfile::tempdir().unwrap();
        let sandbox = temp.path().join("run-1");
        let invalid = OsString::from_vec(b"invalid-\xFF.log".to_vec());
        let artifact = sandbox.join("artifacts").join(invalid);

        let err = normalize_relative_path(&artifact, &sandbox).unwrap_err();

        assert!(matches!(err, RunnerError::NonUtf8ArtifactPath { .. }));
    }

    #[test]
    fn record_event_requires_existing_parent_directory() {
        let temp = tempfile::tempdir().unwrap();
        let missing_parent_event = temp.path().join("missing").join("events.ndjson");

        let err = record_event(&missing_parent_event, "run-1", None, "run", "started")
            .expect_err("event writer should not create missing parent directories");

        assert!(matches!(err, RunnerError::Io(_)));
        assert!(!missing_parent_event.exists());
    }

    #[cfg(unix)]
    #[test]
    fn report_artifact_discovery_includes_symlink_entries() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let sandbox = temp.path().join("run-1");
        let artifact_dir = sandbox.join("artifacts/scenarios/smoke");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::write(artifact_dir.join("target.txt"), "target").unwrap();
        symlink("target.txt", artifact_dir.join("linked-target.txt")).unwrap();

        let artifacts = collect_report_artifacts(&sandbox, &[]).unwrap();

        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.path == "artifacts/scenarios/smoke/linked-target.txt")
        );
    }

    #[test]
    fn run_writes_report_when_scenario_fails() {
        let temp = tempfile::tempdir().unwrap();
        let outcome = run(RunRequest {
            root: temp.path().join("e2e-runs"),
            run_id: Some("failed-run".to_string()),
            scenarios: vec![scenario::TEST_FAILING_VALIDATE.to_string()],
            seed: 1,
            studiofs_bin: None,
            platform: PlatformSelection::Auto,
            retention: RetentionPolicy::Always,
            report: None,
            port_range: None,
            timeout: None,
            infra: InfraMode::Disabled,
        })
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        assert_eq!(outcome.exit_code, 1);
        assert!(outcome.report_path.exists());

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["status"], "failed");
        assert_eq!(report["exit_code"], 1);
        assert_eq!(report["scenarios"][0]["status"], "failed");
        assert!(
            report["scenarios"][0]["error"]
                .as_str()
                .unwrap()
                .contains("validation failed")
        );
    }

    #[test]
    fn run_preserves_primary_scenario_error_when_cleanup_also_fails() {
        let temp = tempfile::tempdir().unwrap();
        let outcome = run(RunRequest {
            root: temp.path().join("e2e-runs"),
            run_id: Some("failed-with-cleanup-run".to_string()),
            scenarios: vec![scenario::TEST_FAILING_VALIDATE_AND_CLEANUP.to_string()],
            seed: 1,
            studiofs_bin: None,
            platform: PlatformSelection::Auto,
            retention: RetentionPolicy::Always,
            report: None,
            port_range: None,
            timeout: None,
            infra: InfraMode::Disabled,
        })
        .unwrap();

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["status"], "failed");
        assert_eq!(report["exit_code"], 1);
        assert!(
            report["scenarios"][0]["error"]
                .as_str()
                .unwrap()
                .contains("validation failed")
        );
        assert_eq!(report["cleanup"]["status"], "cleanup_failed");
        assert!(
            report["cleanup"]["errors"][0]
                .as_str()
                .unwrap()
                .contains("cleanup failed")
        );
    }

    #[test]
    fn compose_basic_smoke_requires_product_binary() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor::default();

        let err = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("missing-studiofs-bin".to_string()),
                scenarios: vec![scenario::BASIC_MULTI_CLIENT_SMOKE.to_string()],
                seed: 324,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33200-33210".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .unwrap_err();

        assert!(matches!(err, RunnerError::MissingStudiofsBin { scenario }
            if scenario == scenario::BASIC_MULTI_CLIENT_SMOKE));
    }

    #[test]
    fn run_with_compose_infra_waits_collects_logs_and_tears_down() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("compose-runner-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 319,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33000-33010".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Passed);
        assert!(
            outcome
                .sandbox
                .join("artifacts/docker/compose.e2e.yml")
                .exists()
        );
        assert_eq!(
            std::fs::read_to_string(outcome.sandbox.join("artifacts/docker/compose.log")).unwrap(),
            "compose logs\n"
        );

        let ports: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.sandbox.join("ports.json")).unwrap())
                .unwrap();
        assert_eq!(ports["ports"][0]["service"], "postgres");
        assert_eq!(ports["ports"][0]["host_port"], 33000);

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert!(
            report["preflight"]["checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|check| check["name"] == "docker compose up --wait"
                    && check["status"] == "passed")
        );
        assert!(
            report["cleanup"]["actions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|action| action == "docker compose down --volumes --remove-orphans")
        );
        assert_eq!(
            report["environment"]["compose_project"],
            "studiofs-e2e-compose-runner-run"
        );
        assert_eq!(
            report["environment"]["allocated_ports"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
        assert!(
            report["environment"]["compose_images"]
                .as_array()
                .unwrap()
                .iter()
                .any(|image| image["service"] == "postgres"
                    && image["image"] == "postgres:18-alpine")
        );

        let calls = executor.calls.borrow();
        let up_index = calls
            .iter()
            .position(|call| call.args.contains(&"up".to_string()))
            .unwrap();
        let logs_index = calls
            .iter()
            .position(|call| call.args.contains(&"logs".to_string()))
            .unwrap();
        let down_index = calls
            .iter()
            .position(|call| call.args.contains(&"down".to_string()))
            .unwrap();
        assert!(up_index < logs_index);
        assert!(logs_index < down_index);
    }

    #[test]
    fn stale_process_cleanup_command_selects_platform_helper() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");

        let linux = stale_process_cleanup_command(&root, PlatformSelection::LinuxFuse).unwrap();
        assert_eq!(linux.program, "sh");
        assert!(
            linux
                .args
                .iter()
                .any(|arg| arg.ends_with("check-stale-processes.sh"))
        );

        let macos = stale_process_cleanup_command(&root, PlatformSelection::MacosFuse).unwrap();
        assert_eq!(macos.program, "sh");
        assert!(
            macos
                .args
                .iter()
                .any(|arg| arg.ends_with("check-stale-processes.sh"))
        );

        let windows =
            stale_process_cleanup_command(&root, PlatformSelection::WindowsWinfsp).unwrap();
        assert_eq!(windows.program, "powershell");
        assert!(
            windows
                .args
                .iter()
                .any(|arg| arg.ends_with("check-stale-processes.ps1"))
        );
    }

    #[test]
    fn runner_run_cleans_stale_runner_processes_before_preflight() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("stale-cleanup-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 406,
                studiofs_bin: None,
                platform: PlatformSelection::LinuxFuse,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: None,
                infra: InfraMode::Disabled,
            },
            &executor,
        )
        .unwrap();

        let calls = executor.calls.borrow();
        let cleanup_call = calls
            .iter()
            .find(|call| {
                call.program == "sh"
                    && call
                        .args
                        .iter()
                        .any(|arg| arg.ends_with("check-stale-processes.sh"))
            })
            .expect("unix stale cleanup command");
        assert!(
            cleanup_call.env.iter().any(|(key, value)| {
                key == "STUDIOFS_E2E_CLEAN_STALE_PROCESSES" && value == "1"
            })
        );
        assert!(
            cleanup_call
                .env
                .iter()
                .any(|(key, _)| key == "STUDIOFS_E2E_RUNNER_ROOT")
        );

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert!(
            report["preflight"]["checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(
                    |check| check["name"] == "runner-owned stale process cleanup"
                        && check["status"] == "passed"
                )
        );
    }

    #[test]
    fn run_with_compose_infra_tears_down_when_post_start_io_fails() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let run_id = "compose-post-start-error";
        let sandbox = root.join(run_id);
        let executor = RecordingExecutor {
            poison_events_path_after_up: RefCell::new(Some(sandbox.join("events.ndjson"))),
            ..RecordingExecutor::default()
        };

        let err = run_with_executor(
            RunRequest {
                root,
                run_id: Some(run_id.to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 319,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33100-33110".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .unwrap_err();

        assert!(matches!(err, RunnerError::Io(_)));
        let calls = executor.calls.borrow();
        assert!(
            calls
                .iter()
                .any(|call| call.args.contains(&"up".to_string()))
        );
        assert!(
            calls
                .iter()
                .any(|call| call.args.contains(&"down".to_string())),
            "compose stack should be torn down after post-start runner errors"
        );
    }

    #[test]
    fn compose_start_failure_records_cleanup_boundary_artifact() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor {
            fail_up: Cell::new(true),
            ..RecordingExecutor::default()
        };

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("compose-partial-launch-failure".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 400,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33300-33310".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::PreflightFailed);
        assert_eq!(outcome.exit_code, 2);
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&outcome.report_path).unwrap()).unwrap();
        assert!(
            report["artifacts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|artifact| artifact["path"] == "artifacts/runner/cleanup-boundary.json"),
            "cleanup boundary artifact should be discoverable from report.json"
        );

        let boundary: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                outcome
                    .sandbox
                    .join("artifacts/runner/cleanup-boundary.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(boundary["run_id"], "compose-partial-launch-failure");
        assert_eq!(
            boundary["compose_project"],
            "studiofs-e2e-compose-partial-launch-failure"
        );
        let allowlist = boundary["cleanup_allowlist"].as_array().unwrap();
        assert!(allowlist.iter().all(|path| {
            path.as_str()
                .unwrap()
                .starts_with(outcome.sandbox.to_str().unwrap())
        }));
        assert_eq!(
            boundary["explicit_client_roots"].as_array().unwrap().len(),
            4
        );

        let calls = executor.calls.borrow();
        let up_index = calls
            .iter()
            .position(|call| call.args.contains(&"up".to_string()))
            .unwrap();
        let logs_index = calls
            .iter()
            .position(|call| call.args.contains(&"logs".to_string()))
            .unwrap();
        let down_index = calls
            .iter()
            .position(|call| call.args.contains(&"down".to_string()))
            .unwrap();
        assert!(up_index < logs_index);
        assert!(logs_index < down_index);
    }

    #[test]
    fn post_start_runner_failure_writes_best_effort_report() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let run_id = "post-start-best-effort-report";
        let sandbox = root.join(run_id);
        let executor = RecordingExecutor {
            poison_events_path_after_up: RefCell::new(Some(sandbox.join("events.ndjson"))),
            ..RecordingExecutor::default()
        };

        let err = run_with_executor(
            RunRequest {
                root,
                run_id: Some(run_id.to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 400,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33400-33410".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .unwrap_err();

        assert!(matches!(err, RunnerError::Io(_)));
        let report_path = sandbox.join("report.json");
        assert!(
            report_path.exists(),
            "post-start failure must preserve report.json"
        );
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
        assert_eq!(report["status"], "failed");
        assert_eq!(report["exit_code"], 1);
        assert_eq!(report["cleanup"]["status"], "passed");
        assert!(
            report["cleanup"]["actions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|action| action == "docker compose down --volumes --remove-orphans")
        );
        assert!(
            report["artifacts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|artifact| artifact["path"] == "artifacts/runner/failure-boundary.json")
        );
        let failure: serde_json::Value = serde_json::from_slice(
            &std::fs::read(sandbox.join("artifacts/runner/failure-boundary.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(failure["phase"], "post-preflight");
        assert!(
            failure["event_log"]
                .as_str()
                .unwrap()
                .contains("events.ndjson")
        );
        assert!(!failure["primary_error"].as_str().unwrap().is_empty());
    }

    #[derive(Default)]
    struct RecordingExecutor {
        calls: RefCell<Vec<crate::infra::CommandSpec>>,
        poison_events_path_after_up: RefCell<Option<PathBuf>>,
        fail_up: Cell<bool>,
    }

    impl CommandExecutor for RecordingExecutor {
        fn run(&self, command: &crate::infra::CommandSpec) -> std::io::Result<CommandOutput> {
            self.calls.borrow_mut().push(command.clone());
            if command.args.contains(&"up".to_string()) {
                if self.fail_up.get() {
                    return Ok(CommandOutput {
                        status: 1,
                        stdout: String::new(),
                        stderr: "compose helper launch failed".to_string(),
                    });
                }
                if let Some(events_path) = self.poison_events_path_after_up.borrow_mut().take() {
                    std::fs::create_dir_all(events_path)?;
                }
            }
            let stdout = if command.args.contains(&"logs".to_string()) {
                "compose logs\n"
            } else {
                ""
            };
            Ok(CommandOutput {
                status: 0,
                stdout: stdout.to_string(),
                stderr: String::new(),
            })
        }
    }
}

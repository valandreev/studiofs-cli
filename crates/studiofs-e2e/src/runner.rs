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
use studiofs_scenario::{
    OperationLogError, OperationLogWriter, OperationOutcome, ScenarioOperation,
};
use thiserror::Error;
use uuid::Uuid;

use crate::client::{
    ClientControllerConfig, ClientControllerError, DEFAULT_CLIENT_NAMES, client_cache_dir,
    client_mount_point, client_root_dir,
};
use crate::infra::{
    CommandExecutor, ComposeConfig, DockerComposeStack, InfraCheck, InfraError, InfraMode,
    SystemCommandExecutor,
};
use crate::report::{
    ArtifactReport, CleanupReport, EventRecord, HostReport, PreflightCheckReport, PreflightReport,
    RunManifest, RunReport, RunnerReport, SandboxReport, ScenarioReport,
};
use crate::scenario::{self, ScenarioContext};

const SCHEMA_VERSION: u32 = 1;
const RUNNER_CRATE: &str = "studiofs-e2e";
const RUNNER_MARKER_FILE: &str = ".studiofs-e2e-runner.json";

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
    #[error(
        "E2E scenario {scenario} requires --studiofs-bin when real mounted infrastructure is used"
    )]
    MissingStudiofsBin { scenario: String },
    #[error("run ID must not be empty")]
    EmptyRunId,
    #[error("run ID must be a safe single directory name: {0}")]
    UnsafeRunId(String),
    #[error(
        "invalid E2E --timeout value {value:?}: expected <number>ms, <number>s, <number>m, or <number>h"
    )]
    InvalidTimeout { value: String },
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
    #[error("runner operation log failed: {0}")]
    OperationLog(#[from] OperationLogError),
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

#[derive(Debug, Clone, Copy)]
struct RunDeadline {
    timeout: Duration,
    started: Instant,
}

impl RunDeadline {
    fn new(timeout: Duration, started: Instant) -> Self {
        Self { timeout, started }
    }

    fn elapsed(self) -> Duration {
        self.started.elapsed()
    }

    fn remaining(self) -> Option<Duration> {
        self.timeout.checked_sub(self.elapsed())
    }
}

pub fn run(request: RunRequest) -> Result<RunOutcome, RunnerError> {
    let executor = SystemCommandExecutor;
    run_with_executor(request, &executor)
}

pub fn run_with_executor(
    request: RunRequest,
    executor: &dyn CommandExecutor,
) -> Result<RunOutcome, RunnerError> {
    run_with_executor_and_mount_lock_dir(request, executor, &default_mount_lock_dir())
}

fn run_with_executor_and_mount_lock_dir(
    request: RunRequest,
    executor: &dyn CommandExecutor,
    mount_lock_dir: &Path,
) -> Result<RunOutcome, RunnerError> {
    let started_instant = Instant::now();
    let started_at = timestamp();
    let run_deadline = request
        .timeout
        .as_deref()
        .map(parse_run_timeout)
        .transpose()?
        .map(|timeout| RunDeadline::new(timeout, started_instant));
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
    e2e_progress(&format!(
        "run {run_id} starting: {} scenario(s), seed={}, infra={:?}",
        scenario_names.len(),
        request.seed,
        request.infra
    ));
    let scenarios = resolve_scenarios(&scenario_names)?;
    if matches!(request.infra, InfraMode::Compose | InfraMode::ExternalEnv)
        && request.studiofs_bin.is_none()
    {
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
    let mut compose_stack = match infra_mode {
        InfraMode::Disabled => None,
        InfraMode::Compose => Some(DockerComposeStack::new(ComposeConfig::for_run(
            &run_id,
            &sandbox,
            request.port_range.clone(),
        ))?),
        InfraMode::ExternalEnv => None,
    };
    let external_client_config = if let (InfraMode::ExternalEnv, Some(studiofs_bin)) =
        (infra_mode, request.studiofs_bin.as_ref())
    {
        Some(ClientControllerConfig::from_external_env(
            studiofs_bin.clone(),
            sandbox.clone(),
            &run_id,
            request.seed,
        )?)
    } else {
        None
    };
    let compose_project = compose_stack
        .as_ref()
        .map(|stack| stack.project_name().to_string());
    let mut allocated_ports = compose_stack
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
    write_log_files(&sandbox)?;

    let mut default_mount_lock = None;
    let mount_lock_failure = if requires_default_windows_mount_lock(
        infra_mode,
        &selected_platform,
        scenarios.iter().map(|scenario| scenario.definition),
    ) {
        match acquire_default_mount_lock(mount_lock_dir, &run_id) {
            Ok(lock) => {
                default_mount_lock = Some(lock);
                None
            }
            Err(err) => Some(err.to_string()),
        }
    } else {
        None
    };
    let prepared_preflight = if let Some(detail) = mount_lock_failure {
        e2e_progress(&format!("run {run_id} preflight: mount lock failed"));
        prepare_failed_mount_lock_preflight(&sandbox, compose_stack.as_ref(), detail)?
    } else {
        e2e_progress(&format!("run {run_id} preflight: starting"));
        prepare_preflight(
            &sandbox,
            infra_mode,
            compose_stack.as_mut(),
            executor,
            default_mount_lock.as_ref(),
        )?
    };
    let preflight = prepared_preflight.report;
    e2e_progress(&format!("run {run_id} preflight: {:?}", preflight.status));
    allocated_ports = compose_stack
        .as_ref()
        .map(|stack| stack.ports().to_vec())
        .unwrap_or_default();
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
        return finish_run(FinishRunInput {
            run_id,
            seed: request.seed,
            status: RunStatus::PreflightFailed,
            exit_code: 2,
            started_at,
            duration_ms: duration_millis(started_instant),
            selected_platform,
            sandbox,
            retention: request.retention,
            report_copy: request.report,
            preflight,
            scenario_reports: Vec::new(),
            scenario_outcomes: Vec::new(),
            cleanup,
        });
    }
    write_pretty_json(&sandbox.join("preflight.json"), &preflight)?;

    let events_path = sandbox.join("events.ndjson");
    record_event(&events_path, &run_id, None, "run", "started")?;

    let mut scenario_reports = Vec::new();
    let mut scenario_outcomes = Vec::new();
    let mut cleanup_errors = Vec::new();
    let total_scenarios = scenarios.len();
    for (index, resolved) in scenarios.into_iter().enumerate() {
        let definition = resolved.definition;
        e2e_progress(&format!(
            "run {run_id} scenario {}/{} {}: starting",
            index + 1,
            total_scenarios,
            definition.name
        ));
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
                    let allocated_ports = compose_stack
                        .as_ref()
                        .map(|stack| stack.ports().to_vec())
                        .unwrap_or_default();
                    Some(ClientControllerConfig::from_compose_services(
                        studiofs_bin.clone(),
                        sandbox.clone(),
                        &run_id,
                        request.seed,
                        &allocated_ports,
                    )?)
                }
                (InfraMode::ExternalEnv, _) => external_client_config.clone(),
                _ => None,
            },
        };

        let lifecycle = run_scenario_lifecycle_with_timeout(
            &events_path,
            &run_id,
            resolved,
            context,
            run_deadline,
        )?;
        e2e_progress(&format!(
            "run {run_id} scenario {}/{} {}: {:?}",
            index + 1,
            total_scenarios,
            definition.name,
            lifecycle.outcome.status
        ));
        if let Some(cleanup_error) = &lifecycle.cleanup_error {
            cleanup_errors.push(format!("{}: {cleanup_error}", lifecycle.outcome.name));
        }
        scenario_reports.push(lifecycle.report);
        scenario_outcomes.push(lifecycle.outcome);
        if scenario_outcomes
            .last()
            .is_some_and(|scenario| scenario.status == ScenarioStatus::TimedOut)
        {
            record_event(&events_path, &run_id, None, "run", "timed_out")?;
            break;
        }
    }

    if !scenario_outcomes
        .iter()
        .any(|scenario| scenario.status == ScenarioStatus::TimedOut)
    {
        record_event(&events_path, &run_id, None, "run", "finished")?;
    }
    let mut cleanup_actions = Vec::new();
    e2e_progress(&format!("run {run_id} cleanup: starting"));
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

    let scenario_failed = scenario_outcomes
        .iter()
        .any(|scenario| scenario.status != ScenarioStatus::Passed);
    let run_timed_out = scenario_outcomes
        .iter()
        .any(|scenario| scenario.status == ScenarioStatus::TimedOut);
    let cleanup_failed = cleanup.status == RunStatus::CleanupFailed;
    let status = if run_timed_out {
        RunStatus::TimedOut
    } else if scenario_failed {
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
        run_id,
        seed: request.seed,
        status,
        exit_code,
        started_at,
        duration_ms: duration_millis(started_instant),
        selected_platform,
        sandbox,
        retention: request.retention,
        report_copy: request.report,
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
    preflight: PreflightReport,
    scenario_reports: Vec<ScenarioReport>,
    scenario_outcomes: Vec<ScenarioOutcome>,
    cleanup: CleanupReport,
}

fn finish_run(input: FinishRunInput) -> Result<RunOutcome, RunnerError> {
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

struct PreparedPreflight {
    report: PreflightReport,
    compose_start_attempted: bool,
}

fn prepare_preflight(
    sandbox: &Path,
    infra: InfraMode,
    compose_stack: Option<&mut DockerComposeStack>,
    executor: &dyn CommandExecutor,
    default_mount_lock: Option<&DefaultMountLock>,
) -> Result<PreparedPreflight, RunnerError> {
    let Some(stack) = compose_stack else {
        write_pretty_json(
            &sandbox.join("ports.json"),
            &serde_json::json!({ "ports": [] }),
        )?;
        return Ok(PreparedPreflight {
            report: PreflightReport {
                status: RunStatus::Passed,
                checks: vec![PreflightCheckReport {
                    name: "runner-skeleton".to_string(),
                    status: RunStatus::Passed,
                    detail: match infra {
                        InfraMode::Disabled => "external infrastructure disabled for this run",
                        InfraMode::ExternalEnv => {
                            "using caller-provided PostgreSQL and S3/RustFS settings from environment or .env"
                        }
                        InfraMode::Compose => "compose infrastructure was not started",
                    }
                    .to_string(),
                }],
            },
            compose_start_attempted: false,
        });
    };

    stack.prepare_artifacts()?;
    let mut checks: Vec<_> = Vec::new();
    if let Some(lock) = default_mount_lock {
        checks.push(PreflightCheckReport {
            name: "default Windows mounted E2E run lock".to_string(),
            status: RunStatus::Passed,
            detail: format!("held at {}", lock.path.display()),
        });
    }
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
    let post_start_checks = stack.post_start_preflight(executor);
    if post_start_checks.iter().all(|check| check.passed) {
        checks.extend(post_start_checks.into_iter().map(preflight_check_report));
    } else {
        let failed_details = post_start_checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect::<Vec<_>>()
            .join("; ");
        match stack.teardown(executor).and_then(|()| {
            let advanced_ports = stack.advance_port_block()?;
            stack.prepare_artifacts()?;
            stack.start(executor)?;
            Ok(advanced_ports)
        }) {
            Ok(advanced_ports) => checks.push(PreflightCheckReport {
                name: "docker compose recreate after failed host health".to_string(),
                status: RunStatus::Passed,
                detail: if advanced_ports {
                    format!(
                        "recreated stack on higher port block after host readiness failure: {failed_details}"
                    )
                } else {
                    format!(
                        "recreated stack after host readiness failure: {failed_details}"
                    )
                },
            }),
            Err(err) => {
                checks.push(PreflightCheckReport {
                    name: "docker compose recreate after failed host health".to_string(),
                    status: RunStatus::PreflightFailed,
                    detail: format!(
                        "failed to recreate stack after host readiness failure ({failed_details}): {err}"
                    ),
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
        checks.extend(
            stack
                .post_start_preflight(executor)
                .into_iter()
                .map(preflight_check_report),
        );
    }
    if checks.iter().any(|check| check.status != RunStatus::Passed) {
        return Ok(PreparedPreflight {
            report: PreflightReport {
                status: RunStatus::PreflightFailed,
                checks,
            },
            compose_start_attempted: true,
        });
    }

    Ok(PreparedPreflight {
        report: PreflightReport {
            status: RunStatus::Passed,
            checks,
        },
        compose_start_attempted: true,
    })
}

fn prepare_failed_mount_lock_preflight(
    sandbox: &Path,
    compose_stack: Option<&DockerComposeStack>,
    detail: String,
) -> Result<PreparedPreflight, RunnerError> {
    if let Some(stack) = compose_stack {
        stack.prepare_artifacts()?;
    } else {
        write_pretty_json(
            &sandbox.join("ports.json"),
            &serde_json::json!({ "ports": [] }),
        )?;
    }
    Ok(PreparedPreflight {
        report: PreflightReport {
            status: RunStatus::PreflightFailed,
            checks: vec![PreflightCheckReport {
                name: "default Windows mounted E2E run lock".to_string(),
                status: RunStatus::PreflightFailed,
                detail,
            }],
        },
        compose_start_attempted: false,
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

#[derive(Debug)]
struct DefaultMountLock {
    path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct DefaultMountLockHolder {
    runner: String,
    scope: String,
    pid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessStatus {
    Running,
    NotRunning,
    Unknown,
}

impl Drop for DefaultMountLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn requires_default_windows_mount_lock(
    infra: InfraMode,
    selected_platform: &str,
    scenarios: impl IntoIterator<Item = scenario::ScenarioDefinition>,
) -> bool {
    infra != InfraMode::Disabled
        && selected_platform == "windows-winfsp"
        && scenarios
            .into_iter()
            .any(|scenario| scenario.requires_mount)
}

fn default_mount_lock_dir() -> PathBuf {
    #[cfg(windows)]
    if let Some(program_data) = std::env::var_os("ProgramData") {
        return PathBuf::from(program_data).join("StudioFS").join("e2e");
    }

    std::env::temp_dir().join("studiofs-e2e")
}

fn acquire_default_mount_lock(
    lock_dir: &Path,
    run_id: &str,
) -> Result<DefaultMountLock, std::io::Error> {
    acquire_default_mount_lock_with_process_checker(lock_dir, run_id, process_is_running)
}

fn acquire_default_mount_lock_with_process_checker(
    lock_dir: &Path,
    run_id: &str,
    process_checker: impl Fn(u32) -> ProcessStatus + Copy,
) -> Result<DefaultMountLock, std::io::Error> {
    fs::create_dir_all(lock_dir)?;
    let path = lock_dir.join("default-windows-mount-points.lock");
    let holder = serde_json::json!({
        "runner": RUNNER_CRATE,
        "scope": "default-windows-mount-points",
        "run_id": run_id,
        "pid": std::process::id(),
        "created_at": timestamp(),
    });
    let payload = serde_json::to_vec_pretty(&holder).map_err(std::io::Error::other)?;
    acquire_default_mount_lock_with_writer(&path, run_id, &payload, write_lock_payload).or_else(
        |err| {
            if err.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(err);
            }
            if reclaim_stale_default_mount_lock(&path, process_checker)? {
                return acquire_default_mount_lock_with_writer(
                    &path,
                    run_id,
                    &payload,
                    write_lock_payload,
                );
            }
            Err(err)
        },
    )
}

fn acquire_default_mount_lock_with_writer(
    path: &Path,
    run_id: &str,
    payload: &[u8],
    write_payload: impl FnOnce(fs::File, &[u8]) -> std::io::Result<()>,
) -> Result<DefaultMountLock, std::io::Error> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => {
            if let Err(err) = write_payload(file, payload) {
                let _ = fs::remove_file(path);
                return Err(err);
            }
            Ok(DefaultMountLock {
                path: path.to_path_buf(),
            })
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            let holder = default_mount_lock_holder_diagnostic(path);
            Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "default Windows mounted E2E run lock is already held at {}; current run {run_id} cannot safely reuse default drive-letter mount points; holder: {holder}",
                    path.display()
                ),
            ))
        }
        Err(err) => Err(err),
    }
}

fn reclaim_stale_default_mount_lock(
    path: &Path,
    process_checker: impl Fn(u32) -> ProcessStatus,
) -> std::io::Result<bool> {
    let Some(holder) = read_default_mount_lock_holder(path)? else {
        return Ok(false);
    };
    if holder.runner != RUNNER_CRATE || holder.scope != "default-windows-mount-points" {
        return Ok(false);
    }
    match process_checker(holder.pid) {
        ProcessStatus::NotRunning => {}
        ProcessStatus::Running | ProcessStatus::Unknown => return Ok(false),
    }
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err),
    }
}

fn read_default_mount_lock_holder(path: &Path) -> std::io::Result<Option<DefaultMountLockHolder>> {
    let holder = fs::read_to_string(path)?;
    if holder.trim().is_empty() {
        return Ok(None);
    }
    serde_json::from_str(&holder)
        .map(Some)
        .map_err(std::io::Error::other)
}

fn default_mount_lock_holder_diagnostic(path: &Path) -> String {
    match fs::read_to_string(path) {
        Ok(holder) if holder.trim().is_empty() => "<empty lock holder file>".to_string(),
        Ok(holder) => holder,
        Err(read_err) => format!("<failed to read existing lock holder: {read_err}>"),
    }
}

fn process_is_running(pid: u32) -> ProcessStatus {
    if pid == std::process::id() {
        return ProcessStatus::Running;
    }
    platform_process_status(pid)
}

#[cfg(windows)]
fn platform_process_status(pid: u32) -> ProcessStatus {
    type Handle = *mut std::ffi::c_void;

    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const ERROR_INVALID_PARAMETER: u32 = 87;

    unsafe extern "system" {
        fn OpenProcess(dwDesiredAccess: u32, bInheritHandle: i32, dwProcessId: u32) -> Handle;
        fn CloseHandle(hObject: Handle) -> i32;
        fn GetLastError() -> u32;
    }

    // SAFETY: OpenProcess only observes whether the PID can be opened.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        // SAFETY: GetLastError reads the calling thread's last OS error.
        return match unsafe { GetLastError() } {
            ERROR_INVALID_PARAMETER => ProcessStatus::NotRunning,
            _ => ProcessStatus::Unknown,
        };
    }
    // SAFETY: handle was returned by OpenProcess and must be closed once.
    unsafe {
        CloseHandle(handle);
    }
    ProcessStatus::Running
}

#[cfg(not(windows))]
fn platform_process_status(_pid: u32) -> ProcessStatus {
    ProcessStatus::Unknown
}

fn write_lock_payload(mut file: fs::File, payload: &[u8]) -> std::io::Result<()> {
    file.write_all(payload)?;
    file.sync_all()
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

fn run_scenario_lifecycle_with_timeout(
    events_path: &Path,
    run_id: &str,
    resolved: ResolvedScenario,
    context: ScenarioContext,
    deadline: Option<RunDeadline>,
) -> Result<ScenarioLifecycleOutcome, RunnerError> {
    let Some(deadline) = deadline else {
        return run_scenario_lifecycle(events_path, run_id, resolved.scenario.as_ref(), &context);
    };
    let definition = resolved.definition;
    let seed = context.seed;
    let started_at = timestamp();
    let started_instant = Instant::now();
    let remaining = deadline.remaining().unwrap_or_default();
    let (tx, rx) = mpsc::channel();
    let events_path = events_path.to_path_buf();
    let run_id = run_id.to_string();
    let worker_events_path = events_path.clone();
    let worker_run_id = run_id.clone();

    thread::spawn(move || {
        let result = run_scenario_lifecycle(
            &worker_events_path,
            &worker_run_id,
            resolved.scenario.as_ref(),
            &context,
        );
        let _ = tx.send(result);
    });

    match rx.recv_timeout(remaining) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let active_hook = active_scenario_hook(&events_path, definition.name)
                .unwrap_or_else(|| "unknown".to_string());
            let elapsed_ms = deadline.elapsed().as_millis();
            let timeout_ms = deadline.timeout.as_millis();
            let error = format!(
                "whole-run timeout exceeded after {elapsed_ms}ms (limit {timeout_ms}ms); active scenario {}; active hook {active_hook}; cleanup attempted",
                definition.name
            );
            e2e_progress(&format!(
                "run {run_id} scenario {} timed out in hook {active_hook}",
                definition.name
            ));
            let _ = record_event(
                &events_path,
                &run_id,
                Some(definition.name),
                &active_hook,
                "timed_out",
            );
            let operation_artifact = scenario_artifact_path(definition.name, "operations.ndjson");
            Ok(ScenarioLifecycleOutcome {
                report: ScenarioReport {
                    name: definition.name.to_string(),
                    seed,
                    status: ScenarioStatus::TimedOut,
                    started_at,
                    finished_at: timestamp(),
                    duration_ms: duration_millis(started_instant),
                    artifacts: vec![operation_artifact.clone()],
                    error: Some(error),
                },
                outcome: ScenarioOutcome {
                    name: definition.name.to_string(),
                    status: ScenarioStatus::TimedOut,
                    artifacts: vec![operation_artifact],
                },
                cleanup_error: None,
            })
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(RunnerError::Io(std::io::Error::other(
            "scenario watchdog worker exited without a result",
        ))),
    }
}

fn parse_run_timeout(value: &str) -> Result<Duration, RunnerError> {
    let trimmed = value.trim();
    let (digits, unit) = trimmed
        .find(|ch: char| !ch.is_ascii_digit())
        .map(|index| trimmed.split_at(index))
        .unwrap_or((trimmed, "s"));
    if digits.is_empty() || unit.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(RunnerError::InvalidTimeout {
            value: value.to_string(),
        });
    }
    let amount = digits
        .parse::<u64>()
        .map_err(|_| RunnerError::InvalidTimeout {
            value: value.to_string(),
        })?;
    if amount == 0 {
        return Err(RunnerError::InvalidTimeout {
            value: value.to_string(),
        });
    }
    match unit {
        "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => amount
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| RunnerError::InvalidTimeout {
                value: value.to_string(),
            }),
        "h" => amount
            .checked_mul(60 * 60)
            .map(Duration::from_secs)
            .ok_or_else(|| RunnerError::InvalidTimeout {
                value: value.to_string(),
            }),
        _ => Err(RunnerError::InvalidTimeout {
            value: value.to_string(),
        }),
    }
}

fn active_scenario_hook(events_path: &Path, scenario: &str) -> Option<String> {
    let content = fs::read_to_string(events_path).ok()?;
    let mut active = BTreeSet::new();
    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let Ok(event) = serde_json::from_str::<EventRecord>(line) else {
            continue;
        };
        if event.scenario.as_deref() != Some(scenario) {
            continue;
        }
        match event.phase.as_str() {
            "started" => {
                active.insert(event.hook);
            }
            "finished" | "failed" | "timed_out" => {
                active.remove(&event.hook);
            }
            _ => {}
        }
    }
    active.into_iter().next_back()
}

fn run_scenario_lifecycle(
    events_path: &Path,
    run_id: &str,
    scenario: &dyn scenario::Scenario,
    context: &ScenarioContext,
) -> Result<ScenarioLifecycleOutcome, RunnerError> {
    let definition = scenario.definition();
    let started_at = timestamp();
    let started_instant = Instant::now();
    let operation_log = context.scenario_dir.join("operations.ndjson");
    let operation_artifact = scenario_artifact_path(definition.name, "operations.ndjson");

    fs::create_dir_all(&context.scenario_dir)?;
    let mut operation_writer = OperationLogWriter::open_append(&operation_log)?;
    let result = run_scenario_body(
        events_path,
        &mut operation_writer,
        run_id,
        scenario,
        context,
    );
    let cleanup_result = run_hook(
        events_path,
        &mut operation_writer,
        run_id,
        definition.name,
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
    operation_writer: &mut OperationLogWriter,
    run_id: &str,
    scenario: &dyn scenario::Scenario,
    context: &ScenarioContext,
) -> Result<Vec<String>, RunnerError> {
    let definition = scenario.definition();
    run_hook(
        events_path,
        operation_writer,
        run_id,
        definition.name,
        "setup",
        || scenario.setup(context),
    )?;
    run_hook(
        events_path,
        operation_writer,
        run_id,
        definition.name,
        "run",
        || scenario.run(context),
    )?;
    run_hook(
        events_path,
        operation_writer,
        run_id,
        definition.name,
        "validate",
        || scenario.validate(context),
    )
}

fn run_hook<T>(
    events_path: &Path,
    operation_writer: &mut OperationLogWriter,
    run_id: &str,
    scenario: &str,
    hook: &str,
    f: impl FnOnce() -> Result<T, scenario::ScenarioError>,
) -> Result<T, RunnerError> {
    record_event(events_path, run_id, Some(scenario), hook, "started")?;
    record_operation_checkpoint(operation_writer, scenario, hook, "started")?;
    e2e_progress(&format!(
        "run {run_id} scenario {scenario} hook {hook}: starting"
    ));
    let result = f();
    let phase = if result.is_ok() { "finished" } else { "failed" };
    e2e_progress(&format!(
        "run {run_id} scenario {scenario} hook {hook}: {phase}"
    ));
    record_event(events_path, run_id, Some(scenario), hook, phase)?;
    record_operation_checkpoint(operation_writer, scenario, hook, phase)?;
    Ok(result?)
}

fn record_operation_checkpoint(
    writer: &mut OperationLogWriter,
    scenario: &str,
    hook: &str,
    phase: &str,
) -> Result<(), RunnerError> {
    writer.refresh_next_sequence()?;
    let record = writer
        .record(
            scenario,
            ScenarioOperation::SchedulerCheckpoint {
                name: format!("{hook}.{phase}"),
            },
        )
        .with_outcome(OperationOutcome::Observed);
    writer.append_record(&record)?;
    Ok(())
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

fn generated_run_id() -> String {
    let suffix = Uuid::new_v4().simple().to_string();
    format!("{}-{}", Utc::now().format("%Y%m%dT%H%M%SZ"), &suffix[..6])
}

fn e2e_progress(message: &str) {
    println!("[studiofs-e2e][{}] {message}", timestamp());
    let _ = std::io::stdout().flush();
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
                requires_mount: false,
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

        let outcome = run_scenario_lifecycle(&events_path, "run-1", &scenario, &context).unwrap();

        assert_eq!(outcome.outcome.status, ScenarioStatus::Failed);
        assert!(outcome.report.error.unwrap().contains("validation failed"));
        assert!(scenario.cleaned.get());
        assert_eq!(
            scenario.calls.borrow().as_slice(),
            ["setup", "run", "validate", "cleanup"]
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
    fn run_timeout_marks_active_scenario_timed_out_and_writes_artifacts() {
        let temp = tempfile::tempdir().unwrap();
        let outcome = run(RunRequest {
            root: temp.path().join("e2e-runs"),
            run_id: Some("timed-out-run".to_string()),
            scenarios: vec![scenario::TEST_HANGING_RUN.to_string()],
            seed: 370,
            studiofs_bin: None,
            platform: PlatformSelection::Auto,
            retention: RetentionPolicy::Always,
            report: None,
            port_range: None,
            timeout: Some("150ms".to_string()),
            infra: InfraMode::Disabled,
        })
        .unwrap();

        assert_eq!(outcome.status, RunStatus::TimedOut);
        assert_eq!(outcome.exit_code, 1);
        assert_eq!(outcome.scenarios[0].status, ScenarioStatus::TimedOut);
        assert!(outcome.sandbox.join("events.ndjson").exists());
        assert!(outcome.sandbox.join("cleanup.json").exists());
        assert!(outcome.report_path.exists());

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["status"], "timed_out");
        assert_eq!(report["scenarios"][0]["status"], "timed_out");
        let error = report["scenarios"][0]["error"].as_str().unwrap();
        assert!(error.contains("whole-run timeout exceeded"));
        assert!(error.contains("active scenario __test-hanging-run"));
        assert!(error.contains("active hook "));
    }

    #[test]
    fn parse_run_timeout_supports_documented_units() {
        assert_eq!(
            parse_run_timeout("500ms").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(parse_run_timeout("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_run_timeout("10m").unwrap(), Duration::from_secs(600));
        assert_eq!(parse_run_timeout("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn parse_run_timeout_rejects_unknown_or_zero_values() {
        assert!(matches!(
            parse_run_timeout("0s"),
            Err(RunnerError::InvalidTimeout { .. })
        ));
        assert!(matches!(
            parse_run_timeout("1d"),
            Err(RunnerError::InvalidTimeout { .. })
        ));
        assert!(matches!(
            parse_run_timeout("soon"),
            Err(RunnerError::InvalidTimeout { .. })
        ));
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

        let outcome = run_with_executor_and_mount_lock_dir(
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
            &temp.path().join("mount-locks"),
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
    fn run_with_compose_infra_tears_down_when_post_start_io_fails() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let run_id = "compose-post-start-error";
        let sandbox = root.join(run_id);
        let executor = RecordingExecutor {
            poison_events_path_after_up: RefCell::new(Some(sandbox.join("events.ndjson"))),
            ..RecordingExecutor::default()
        };

        let err = run_with_executor_and_mount_lock_dir(
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
            &temp.path().join("mount-locks"),
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
    fn default_mount_lock_reports_existing_holder_before_parallel_windows_run() {
        let temp = tempfile::tempdir().unwrap();
        let lock_dir = temp.path().join("locks");

        let first = acquire_default_mount_lock(&lock_dir, "first-run").unwrap();
        let err = acquire_default_mount_lock(&lock_dir, "second-run").unwrap_err();

        let message = err.to_string();
        assert!(message.contains("default Windows mounted E2E run lock is already held"));
        assert!(message.contains("first-run"));
        assert!(message.contains("second-run"));
        assert!(message.contains(&lock_dir.display().to_string()));

        drop(first);
        let second = acquire_default_mount_lock(&lock_dir, "second-run").unwrap();
        drop(second);
    }

    #[test]
    fn default_mount_lock_removes_file_after_payload_write_error() {
        let temp = tempfile::tempdir().unwrap();
        let lock_path = temp
            .path()
            .join("locks")
            .join("default-windows-mount-points.lock");
        std::fs::create_dir_all(lock_path.parent().unwrap()).unwrap();

        let err = acquire_default_mount_lock_with_writer(
            &lock_path,
            "failed-run",
            b"holder",
            |file, _payload| {
                drop(file);
                Err(std::io::Error::other("injected write failure"))
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("injected write failure"));
        assert!(!lock_path.exists());
        let lock = acquire_default_mount_lock(lock_path.parent().unwrap(), "next-run").unwrap();
        drop(lock);
    }

    #[test]
    fn default_mount_lock_reclaims_stale_holder_when_pid_is_not_running() {
        let temp = tempfile::tempdir().unwrap();
        let lock_dir = temp.path().join("locks");
        std::fs::create_dir_all(&lock_dir).unwrap();
        let lock_path = lock_dir.join("default-windows-mount-points.lock");
        std::fs::write(
            &lock_path,
            serde_json::json!({
                "runner": RUNNER_CRATE,
                "scope": "default-windows-mount-points",
                "run_id": "stale-run",
                "pid": 4_294_967_294u32,
                "created_at": timestamp(),
            })
            .to_string(),
        )
        .unwrap();

        let lock = acquire_default_mount_lock_with_process_checker(&lock_dir, "new-run", |_| {
            ProcessStatus::NotRunning
        })
        .unwrap();

        let holder = std::fs::read_to_string(&lock_path).unwrap();
        assert!(holder.contains("new-run"));
        assert!(!holder.contains("stale-run"));
        drop(lock);
    }

    #[test]
    fn default_mount_lock_keeps_holder_when_pid_status_is_unknown() {
        let temp = tempfile::tempdir().unwrap();
        let lock_dir = temp.path().join("locks");
        std::fs::create_dir_all(&lock_dir).unwrap();
        let lock_path = lock_dir.join("default-windows-mount-points.lock");
        std::fs::write(
            &lock_path,
            serde_json::json!({
                "runner": RUNNER_CRATE,
                "scope": "default-windows-mount-points",
                "run_id": "inaccessible-run",
                "pid": 42u32,
                "created_at": timestamp(),
            })
            .to_string(),
        )
        .unwrap();

        let err = acquire_default_mount_lock_with_process_checker(&lock_dir, "blocked-run", |_| {
            ProcessStatus::Unknown
        })
        .unwrap_err();

        let holder = std::fs::read_to_string(&lock_path).unwrap();
        assert!(err.to_string().contains("inaccessible-run"));
        assert!(holder.contains("inaccessible-run"));
        assert!(!holder.contains("blocked-run"));
    }

    #[test]
    fn default_mount_lock_reports_empty_holder_file_explicitly() {
        let temp = tempfile::tempdir().unwrap();
        let lock_dir = temp.path().join("locks");
        std::fs::create_dir_all(&lock_dir).unwrap();
        std::fs::write(lock_dir.join("default-windows-mount-points.lock"), "").unwrap();

        let err = acquire_default_mount_lock(&lock_dir, "blocked-run").unwrap_err();

        assert!(err.to_string().contains("<empty lock holder file>"));
    }

    #[test]
    fn windows_compose_run_fails_preflight_when_default_mount_lock_is_held() {
        let temp = tempfile::tempdir().unwrap();
        let lock_dir = temp.path().join("locks");
        let _held = acquire_default_mount_lock(&lock_dir, "first-run").unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("second-run".to_string()),
                scenarios: vec![scenario::BASIC_MULTI_CLIENT_SMOKE.to_string()],
                seed: 364,
                studiofs_bin: Some(temp.path().join("studiofs.exe")),
                platform: PlatformSelection::WindowsWinfsp,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33300-33310".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
            &lock_dir,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::PreflightFailed);
        assert_eq!(outcome.exit_code, 2);
        assert!(executor.calls.borrow().is_empty());

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["preflight"]["status"], "preflight_failed");
        let check = &report["preflight"]["checks"][0];
        assert_eq!(check["name"], "default Windows mounted E2E run lock");
        assert_eq!(check["status"], "preflight_failed");
        assert!(check["detail"].as_str().unwrap().contains("first-run"));
        assert!(check["detail"].as_str().unwrap().contains("second-run"));
    }

    #[test]
    fn windows_compose_run_without_mounted_scenarios_does_not_take_default_mount_lock() {
        let temp = tempfile::tempdir().unwrap();
        let lock_dir = temp.path().join("locks");
        let _held = acquire_default_mount_lock(&lock_dir, "mounted-run").unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("non-mounted-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 364,
                studiofs_bin: None,
                platform: PlatformSelection::WindowsWinfsp,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33320-33330".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
            &lock_dir,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Passed);
        assert!(
            executor
                .calls
                .borrow()
                .iter()
                .any(|call| call.args.contains(&"up".to_string()))
        );
    }

    #[derive(Default)]
    struct RecordingExecutor {
        calls: RefCell<Vec<crate::infra::CommandSpec>>,
        poison_events_path_after_up: RefCell<Option<PathBuf>>,
    }

    impl CommandExecutor for RecordingExecutor {
        fn run(&self, command: &crate::infra::CommandSpec) -> std::io::Result<CommandOutput> {
            self.calls.borrow_mut().push(command.clone());
            if command.args.contains(&"up".to_string()) {
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

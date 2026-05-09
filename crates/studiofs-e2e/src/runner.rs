//! Runner orchestration, sandbox creation, reports, and cleanup.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{BufRead as _, BufReader, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Component, Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{SecondsFormat, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
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
    CommandExecutor, CommandSpec, ComposeConfig, ComposeServiceControl, DockerComposeStack,
    InfraCheck, InfraError, InfraMode, SystemCommandExecutor,
};
use crate::report::{
    ArtifactReport, CleanupReport, EventRecord, HostReport, PreflightCheckReport, PreflightReport,
    RunManifest, RunReport, RunnerReport, SandboxReport, ScenarioReport, StudiofsBinaryReport,
};
use crate::scenario::{self, ScenarioContext};

const SCHEMA_VERSION: u32 = 1;
const RUNNER_CRATE: &str = "studiofs-e2e";
const RUNNER_MARKER_FILE: &str = ".studiofs-e2e-runner.json";
const HOOK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Enum $name.
pub enum RetentionPolicy {
    /// Variant $name.
    Always,
    /// Variant $name.
    OnFailure,
    /// Variant $name.
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Enum $name.
pub enum PlatformSelection {
    /// Variant $name.
    Auto,
    /// Variant $name.
    LinuxFuse,
    /// Variant $name.
    MacosFuse,
    /// Variant $name.
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
/// Enum $name.
pub enum RunStatus {
    /// Variant $name.
    Passed,
    /// Variant $name.
    Failed,
    /// Variant $name.
    Skipped,
    /// Variant $name.
    PreflightFailed,
    /// Variant $name.
    CleanupFailed,
    /// Variant $name.
    TimedOut,
    /// Variant $name.
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum ScenarioStatus {
    /// Variant $name.
    Passed,
    /// Variant $name.
    Failed,
    /// Variant $name.
    Skipped,
    /// Variant $name.
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct RunRequest {
    /// Field $name.
    pub root: PathBuf,
    /// Field $name.
    pub run_id: Option<String>,
    /// Field $name.
    pub scenarios: Vec<String>,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub studiofs_bin: Option<PathBuf>,
    /// Field $name.
    pub platform: PlatformSelection,
    /// Field $name.
    pub retention: RetentionPolicy,
    /// Field $name.
    pub report: Option<PathBuf>,
    /// Field $name.
    pub port_range: Option<String>,
    /// Field $name.
    pub timeout: Option<String>,
    /// Field $name.
    pub infra: InfraMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CleanRequest {
    /// Field $name.
    pub root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct RunOneRequest {
    /// Field $name.
    pub root: PathBuf,
    /// Field $name.
    pub run_id: String,
    /// Field $name.
    pub scenario: String,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub studiofs_bin: Option<PathBuf>,
    /// Field $name.
    pub infra: InfraMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CleanOutcome {
    /// Field $name.
    pub removed: Vec<PathBuf>,
    /// Field $name.
    pub skipped: Vec<CleanSkipped>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CleanSkipped {
    /// Field $name.
    pub path: PathBuf,
    /// Field $name.
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct ScenarioOutcome {
    /// Field $name.
    pub name: String,
    /// Field $name.
    pub status: ScenarioStatus,
    /// Field $name.
    pub artifacts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct RunOutcome {
    /// Field $name.
    pub run_id: String,
    /// Field $name.
    pub status: RunStatus,
    /// Field $name.
    pub exit_code: i32,
    /// Field $name.
    pub sandbox: PathBuf,
    /// Field $name.
    pub report_path: PathBuf,
    /// Field $name.
    pub scenarios: Vec<ScenarioOutcome>,
}

#[derive(Debug, Error)]
/// Enum $name.
pub enum RunnerError {
    #[error("unknown E2E scenario: {0}")]
    /// Variant $name.
    UnknownScenario(String),
    #[error(
        "E2E scenario {scenario} requires --studiofs-bin when real mounted infrastructure is used"
    )]
    /// Field $name.
    MissingStudiofsBin {
        /// The scenario value.
        scenario: String,
    },
    #[error("run ID must not be empty")]
    /// Variant $name.
    EmptyRunId,
    #[error("run ID must be a safe single directory name: {0}")]
    /// Variant $name.
    UnsafeRunId(String),
    #[error(
        "invalid E2E --timeout value {value:?}: expected <number>ms, <number>s, <number>m, or <number>h"
    )]
    /// Variant $name.
    InvalidTimeout {
        /// The value value.
        value: String,
    },
    #[error("unsafe cleanup root: {0}")]
    /// Variant $name.
    UnsafeCleanupRoot(String),
    #[error("E2E artifact path is outside the run sandbox")]
    /// Field $name.
    ArtifactPathOutsideSandbox {
        /// The path value.
        path: PathBuf,
        /// The sandbox value.
        sandbox: PathBuf,
    },
    #[error("E2E artifact path contains a non-UTF-8 component")]
    /// Variant $name.
    NonUtf8ArtifactPath {
        /// The path value.
        path: PathBuf,
    },
    #[error("E2E artifact path must be relative and must not contain parent components")]
    /// Field $name.
    UnsafeArtifactPath {
        /// The path value.
        path: PathBuf,
    },
    #[error("scenario child exited successfully without report: {path}")]
    /// Variant $name.
    MissingScenarioReport {
        /// The path value.
        path: PathBuf,
    },
    #[error("runner I/O failed: {0}")]
    /// Variant $name.
    Io(#[from] std::io::Error),
    #[error("runner JSON failed: {0}")]
    /// Variant $name.
    Json(#[from] serde_json::Error),
    #[error("scenario hook failed: {0}")]
    /// Variant $name.
    Scenario(#[from] scenario::ScenarioError),
    #[error("runner client controller failed: {0}")]
    /// Variant $name.
    Client(#[from] ClientControllerError),
    #[error("runner infra failed: {0}")]
    /// Variant $name.
    Infra(#[from] InfraError),
    #[error("runner operation log failed: {0}")]
    /// Variant $name.
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

/// Function $name.
pub fn run(request: RunRequest) -> Result<RunOutcome, RunnerError> {
    let executor = SystemCommandExecutor;
    run_with_executor(request, &executor)
}

/// Function $name.
pub fn run_with_executor(
    request: RunRequest,
    executor: &dyn CommandExecutor,
) -> Result<RunOutcome, RunnerError> {
    run_with_executor_and_mount_lock_dir(request, executor, &default_mount_lock_dir())
}

/// Function $name.
pub fn run_one(request: RunOneRequest) -> Result<i32, RunnerError> {
    let root = absolutize(&request.root)?;
    validate_run_id(&request.run_id)?;
    let sandbox = root.join(&request.run_id);
    let manifest: RunManifest = read_json_file(&sandbox.join("run.json"))?;
    let resolved = resolve_scenario(&request.scenario)?;
    let events_path = sandbox.join("events.ndjson");
    let scenario_dir = scenario_artifact_dir(&sandbox, resolved.definition.name);
    let client_controller_config =
        run_one_client_controller_config(&request, &manifest, &sandbox, resolved.definition.name)?;
    let context = ScenarioContext {
        run_id: request.run_id.clone(),
        seed: request.seed,
        sandbox: sandbox.clone(),
        scenario_dir,
        client_controller_config,
    };

    let lifecycle_result = run_scenario_lifecycle(
        &events_path,
        &request.run_id,
        resolved.scenario.as_ref(),
        &context,
        1,
        1,
    );
    let lifecycle = lifecycle_result.unwrap_or_else(|error| {
        failed_lifecycle(
            resolved.definition.name,
            request.seed,
            format!("scenario lifecycle failed before report was written: {error}"),
        )
    });
    write_scenario_report(&context.scenario_dir, &lifecycle.report)?;
    Ok(if lifecycle.outcome.status == ScenarioStatus::Passed {
        0
    } else {
        1
    })
}

fn run_one_client_controller_config(
    request: &RunOneRequest,
    manifest: &RunManifest,
    sandbox: &Path,
    scenario_name: &str,
) -> Result<Option<ClientControllerConfig>, RunnerError> {
    let Some(studiofs_bin) = request.studiofs_bin.as_ref() else {
        return Ok(None);
    };
    let client_sandbox = scenario_client_runtime_sandbox(sandbox, scenario_name);
    match request.infra {
        InfraMode::Compose => Ok(Some(ClientControllerConfig::from_compose_services(
            studiofs_bin.clone(),
            client_sandbox,
            &request.run_id,
            request.seed,
            &manifest.allocated_ports,
        )?)),
        InfraMode::ExternalEnv => Ok(Some(ClientControllerConfig::from_external_env(
            studiofs_bin.clone(),
            client_sandbox,
            &request.run_id,
            request.seed,
        )?)),
        InfraMode::Disabled => Ok(None),
    }
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
    let run_id = match &request.run_id {
        Some(run_id) if run_id.trim().is_empty() => return Err(RunnerError::EmptyRunId),
        Some(run_id) => run_id.clone(),
        None => generated_run_id(),
    };
    validate_run_id(&run_id)?;
    let scenario_names = if request.scenarios.is_empty() {
        vec![scenario::BASIC_MULTI_CLIENT_SMOKE.to_string()]
    } else {
        request.scenarios.clone()
    };
    e2e_progress_fields(&[
        ("run", run_id.clone()),
        ("phase", "starting".to_string()),
        ("scenarios", scenario_names.len().to_string()),
        ("seed", request.seed.to_string()),
        ("infra", format!("{:?}", request.infra)),
    ]);
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
    let (studiofs_bin_fingerprint, studiofs_bin_check) =
        studiofs_bin_preflight_check(request.studiofs_bin.as_deref());
    let sandbox = root.join(&run_id);
    e2e_progress_fields(&[
        ("run", run_id.clone()),
        ("phase", "sandbox".to_string()),
        ("sandbox", sandbox.display().to_string()),
    ]);
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
        studiofs_bin_fingerprint: studiofs_bin_fingerprint.clone(),
        port_range: request.port_range.as_deref(),
        timeout: request.timeout.as_deref(),
        created_at: &started_at,
    });
    write_pretty_json(&sandbox.join("run.json"), &manifest)?;
    write_runner_marker(&sandbox, &run_id)?;
    write_log_files(&sandbox)?;
    let events_path = sandbox.join("events.ndjson");
    record_event(&events_path, &run_id, None, "run", "started")?;

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
        e2e_progress_fields(&[
            ("run", run_id.clone()),
            ("stage", "preflight".to_string()),
            ("phase", "mount_lock_failed".to_string()),
        ]);
        prepare_failed_mount_lock_preflight(
            &run_id,
            &sandbox,
            compose_stack.as_ref(),
            studiofs_bin_check.clone(),
            detail,
        )?
    } else {
        e2e_progress_fields(&[
            ("run", run_id.clone()),
            ("stage", "preflight".to_string()),
            ("phase", "starting".to_string()),
        ]);
        prepare_preflight(PreparePreflightInput {
            run_id: &run_id,
            sandbox: &sandbox,
            root: &root,
            infra: infra_mode,
            compose_stack: compose_stack.as_mut(),
            executor,
            default_mount_lock: default_mount_lock.as_ref(),
            studiofs_bin_check: studiofs_bin_check.clone(),
        })?
    };
    let preflight = prepared_preflight.report;
    e2e_progress_fields(&[
        ("run", run_id.clone()),
        ("stage", "preflight".to_string()),
        ("status", format!("{:?}", preflight.status)),
    ]);
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
        studiofs_bin_fingerprint: studiofs_bin_fingerprint.clone(),
        port_range: request.port_range.as_deref(),
        timeout: request.timeout.as_deref(),
        created_at: &started_at,
    });
    write_pretty_json(&sandbox.join("run.json"), &manifest)?;
    let mut compose_cleanup_guard = ComposeCleanupGuard::new(
        &run_id,
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
        drop(default_mount_lock.take());
        e2e_progress_fields(&[
            ("run", run_id.clone()),
            ("stage", "cleanup".to_string()),
            ("phase", "starting".to_string()),
        ]);
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
        e2e_progress_fields(&[
            ("run", run_id.clone()),
            ("stage", "cleanup".to_string()),
            ("status", format!("{:?}", cleanup.status)),
        ]);
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
            studiofs_bin_fingerprint,
        });
    }
    write_pretty_json(&sandbox.join("preflight.json"), &preflight)?;

    let mut scenario_reports = Vec::new();
    let mut scenario_outcomes = Vec::new();
    let mut cleanup_errors = Vec::new();
    let total_scenarios = scenarios.len();
    for (index, resolved) in scenarios.into_iter().enumerate() {
        let definition = resolved.definition;
        e2e_progress_fields(&[
            ("run", run_id.clone()),
            ("scenario", format!("{}/{}", index + 1, total_scenarios)),
            ("scenario_name", definition.name.to_string()),
            ("phase", "starting".to_string()),
        ]);
        let lifecycle_result = if total_scenarios > 1 {
            run_scenario_child_with_timeout(ScenarioChildRun {
                sandbox: &sandbox,
                events_path: &events_path,
                run_id: &run_id,
                definition,
                scenario_index: index + 1,
                scenario_total: total_scenarios,
                executor,
                request: &request,
                deadline: run_deadline,
            })
        } else {
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

            run_scenario_lifecycle_with_timeout(
                &events_path,
                &run_id,
                resolved,
                context,
                run_deadline,
                index + 1,
                total_scenarios,
            )
        };
        let lifecycle = lifecycle_result.unwrap_or_else(|error| {
            failed_lifecycle(
                definition.name,
                request.seed,
                format!("scenario supervisor failed before report was written: {error}"),
            )
        });
        e2e_progress_fields(&[
            ("run", run_id.clone()),
            ("scenario", format!("{}/{}", index + 1, total_scenarios)),
            ("scenario_name", definition.name.to_string()),
            ("status", format!("{:?}", lifecycle.outcome.status)),
        ]);
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
    let (run_timed_out, scenario_failed) = scenario_outcome_flags(&scenario_outcomes);
    if scenario_failed {
        let interim_cleanup = CleanupReport {
            status: RunStatus::Interrupted,
            actions: vec!["cleanup pending".to_string()],
            errors: Vec::new(),
        };
        let (interim_status, interim_exit_code) =
            run_status_and_exit_code(run_timed_out, scenario_failed, false);
        finish_run(FinishRunInput {
            run_id: run_id.clone(),
            seed: request.seed,
            status: interim_status,
            exit_code: interim_exit_code,
            started_at: started_at.clone(),
            duration_ms: duration_millis(started_instant),
            selected_platform: selected_platform.clone(),
            sandbox: sandbox.clone(),
            retention: request.retention,
            report_copy: request.report.clone(),
            preflight: preflight.clone(),
            scenario_reports: scenario_reports.clone(),
            scenario_outcomes: scenario_outcomes.clone(),
            cleanup: interim_cleanup,
            studiofs_bin_fingerprint: studiofs_bin_fingerprint.clone(),
        })?;
    }
    let mut cleanup_actions = Vec::new();
    drop(default_mount_lock.take());
    e2e_progress_fields(&[
        ("run", run_id.clone()),
        ("stage", "cleanup".to_string()),
        ("phase", "starting".to_string()),
    ]);
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
    e2e_progress_fields(&[
        ("run", run_id.clone()),
        ("stage", "cleanup".to_string()),
        ("status", format!("{:?}", cleanup.status)),
    ]);

    let cleanup_failed = cleanup.status == RunStatus::CleanupFailed;
    let (status, exit_code) =
        run_status_and_exit_code(run_timed_out, scenario_failed, cleanup_failed);
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
        studiofs_bin_fingerprint,
    })
}

struct ComposeCleanupGuard<'a> {
    run_id: String,
    stack: Option<&'a DockerComposeStack>,
    executor: &'a dyn CommandExecutor,
}

impl<'a> ComposeCleanupGuard<'a> {
    fn new(
        run_id: &str,
        stack: Option<&'a DockerComposeStack>,
        executor: &'a dyn CommandExecutor,
    ) -> Self {
        Self {
            run_id: run_id.to_string(),
            stack,
            executor,
        }
    }

    fn collect_into(
        &mut self,
        cleanup_actions: &mut Vec<String>,
        cleanup_errors: &mut Vec<String>,
    ) {
        if let Some(stack) = self.stack.take() {
            collect_compose_cleanup(
                &self.run_id,
                stack,
                self.executor,
                cleanup_actions,
                cleanup_errors,
            );
        }
    }
}

impl Drop for ComposeCleanupGuard<'_> {
    fn drop(&mut self) {
        if let Some(stack) = self.stack.take() {
            let mut cleanup_actions = Vec::new();
            let mut cleanup_errors = Vec::new();
            collect_compose_cleanup(
                &self.run_id,
                stack,
                self.executor,
                &mut cleanup_actions,
                &mut cleanup_errors,
            );
        }
    }
}

fn run_status_and_exit_code(
    run_timed_out: bool,
    scenario_failed: bool,
    cleanup_failed: bool,
) -> (RunStatus, i32) {
    match (run_timed_out, scenario_failed, cleanup_failed) {
        (true, _, _) => (RunStatus::TimedOut, 1),
        (_, true, _) => (RunStatus::Failed, 1),
        (_, _, true) => (RunStatus::CleanupFailed, 4),
        _ => (RunStatus::Passed, 0),
    }
}

fn scenario_outcome_flags(scenario_outcomes: &[ScenarioOutcome]) -> (bool, bool) {
    scenario_outcomes.iter().fold(
        (false, false),
        |(run_timed_out, scenario_failed), scenario| {
            (
                run_timed_out || scenario.status == ScenarioStatus::TimedOut,
                scenario_failed || scenario.status != ScenarioStatus::Passed,
            )
        },
    )
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
    studiofs_bin_fingerprint: Option<StudiofsBinaryReport>,
}

fn finish_run(input: FinishRunInput) -> Result<RunOutcome, RunnerError> {
    let artifacts = collect_report_artifacts_best_effort(&input.sandbox, &input.scenario_outcomes)?;
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
        studiofs_bin: input.studiofs_bin_fingerprint,
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
        artifacts,
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

fn studiofs_bin_preflight_check(
    studiofs_bin: Option<&Path>,
) -> (Option<StudiofsBinaryReport>, Option<PreflightCheckReport>) {
    let Some(path) = studiofs_bin else {
        return (None, None);
    };
    match studiofs_bin_fingerprint(path) {
        Ok(fingerprint) => {
            let detail = studiofs_bin_fingerprint_detail(&fingerprint);
            (
                Some(fingerprint),
                Some(PreflightCheckReport {
                    name: "studiofs binary fingerprint".to_string(),
                    status: RunStatus::Passed,
                    detail,
                }),
            )
        }
        Err(error) => (
            None,
            Some(PreflightCheckReport {
                name: "studiofs binary fingerprint".to_string(),
                status: RunStatus::PreflightFailed,
                detail: format!("path={} error={error}", path.display()),
            }),
        ),
    }
}

fn studiofs_bin_fingerprint(path: &Path) -> Result<StudiofsBinaryReport, RunnerError> {
    let canonical_path = path.canonicalize()?;
    let metadata = fs::metadata(&canonical_path)?;
    let modified_unix_ms = system_time_unix_ms(metadata.modified()?);
    let mut file = fs::File::open(&canonical_path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let sha256 = hex_lower(&hasher.finalize());
    Ok(StudiofsBinaryReport {
        path: path.display().to_string(),
        canonical_path: canonical_path.display().to_string(),
        size_bytes: metadata.len(),
        modified_unix_ms,
        sha256,
    })
}

fn studiofs_bin_fingerprint_detail(fingerprint: &StudiofsBinaryReport) -> String {
    format!(
        "path={} canonical_path={} size_bytes={} modified_unix_ms={} sha256={}",
        fingerprint.path,
        fingerprint.canonical_path,
        fingerprint.size_bytes,
        fingerprint.modified_unix_ms,
        fingerprint.sha256
    )
}

fn system_time_unix_ms(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

struct PreparePreflightInput<'a> {
    run_id: &'a str,
    sandbox: &'a Path,
    root: &'a Path,
    infra: InfraMode,
    compose_stack: Option<&'a mut DockerComposeStack>,
    executor: &'a dyn CommandExecutor,
    default_mount_lock: Option<&'a DefaultMountLock>,
    studiofs_bin_check: Option<PreflightCheckReport>,
}

fn prepare_preflight(input: PreparePreflightInput<'_>) -> Result<PreparedPreflight, RunnerError> {
    let PreparePreflightInput {
        run_id,
        sandbox,
        root,
        infra,
        compose_stack,
        executor,
        default_mount_lock,
        studiofs_bin_check,
    } = input;

    let Some(stack) = compose_stack else {
        let mut checks = Vec::new();
        if let Some(check) = studiofs_bin_check {
            push_preflight_step_check(&mut checks, run_id, "studiofs_bin_fingerprint", check);
        }
        checks.push(PreflightCheckReport {
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
        });
        let status = if checks.iter().any(|check| check.status != RunStatus::Passed) {
            RunStatus::PreflightFailed
        } else {
            RunStatus::Passed
        };
        write_pretty_json(
            &sandbox.join("ports.json"),
            &serde_json::json!({ "ports": [] }),
        )?;
        return Ok(PreparedPreflight {
            report: PreflightReport { status, checks },
            compose_start_attempted: false,
        });
    };

    stack.prepare_artifacts()?;
    let mut checks: Vec<_> = Vec::new();
    if let Some(check) = studiofs_bin_check {
        push_preflight_step_check(&mut checks, run_id, "studiofs_bin_fingerprint", check);
    }
    if let Some(lock) = default_mount_lock {
        checks.push(PreflightCheckReport {
            name: "default Windows mounted E2E run lock".to_string(),
            status: RunStatus::Passed,
            detail: format!("held at {}", lock.path.display()),
        });
    }
    if let Some(detail) = stale_infra_preflight_detail(root, sandbox)? {
        checks.push(PreflightCheckReport {
            name: "stale runner-owned E2E infrastructure".to_string(),
            status: RunStatus::PreflightFailed,
            detail,
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

    e2e_stage_step_progress(run_id, "preflight", "docker_compose_up", "started");
    match stack.start(executor) {
        Ok(()) => {
            e2e_stage_step_progress(run_id, "preflight", "docker_compose_up", "Passed");
            checks.push(PreflightCheckReport {
                name: "docker compose up --wait".to_string(),
                status: RunStatus::Passed,
                detail: "required services are running or healthy".to_string(),
            });
        }
        Err(err) => {
            e2e_stage_step_progress(run_id, "preflight", "docker_compose_up", "Failed");
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
    e2e_stage_step_progress(run_id, "preflight", "host_health", "started");
    let post_start_checks =
        post_start_preflight_with_progress(stack, executor, run_id, "host_health");
    if post_start_checks.iter().all(|check| check.passed) {
        e2e_stage_step_progress(run_id, "preflight", "host_health", "Passed");
        checks.extend(post_start_checks.into_iter().map(preflight_check_report));
    } else {
        e2e_stage_step_progress(run_id, "preflight", "host_health", "Failed");
        let failed_details = post_start_checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect::<Vec<_>>()
            .join("; ");
        e2e_stage_step_progress(run_id, "preflight", "docker_compose_recreate", "started");
        match stack.teardown(executor).and_then(|()| {
            stack.prepare_artifacts()?;
            stack.start(executor)?;
            Ok(())
        }) {
            Ok(()) => {
                e2e_stage_step_progress(run_id, "preflight", "docker_compose_recreate", "Passed");
                checks.push(PreflightCheckReport {
                    name: "docker compose recreate after failed host health".to_string(),
                    status: RunStatus::Passed,
                    detail: format!(
                        "recreated stack on the same host ports after host readiness failure: {failed_details}"
                    ),
                });
            }
            Err(err) => {
                e2e_stage_step_progress(run_id, "preflight", "docker_compose_recreate", "Failed");
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
        e2e_stage_step_progress(run_id, "preflight", "host_health_after_recreate", "started");
        checks.extend(
            post_start_preflight_with_progress(
                stack,
                executor,
                run_id,
                "host_health_after_recreate",
            )
            .into_iter()
            .map(preflight_check_report),
        );
        let post_recreate_status = if checks.iter().all(|check| check.status == RunStatus::Passed) {
            "Passed"
        } else {
            "Failed"
        };
        e2e_stage_step_progress(
            run_id,
            "preflight",
            "host_health_after_recreate",
            post_recreate_status,
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

fn post_start_preflight_with_progress(
    stack: &DockerComposeStack,
    executor: &dyn CommandExecutor,
    run_id: &str,
    step: &str,
) -> Vec<InfraCheck> {
    let mut heartbeat = |port: &crate::infra::ServicePort, attempts: usize, elapsed: Duration| {
        e2e_progress_fields(&[
            ("run", run_id.to_string()),
            ("stage", "preflight".to_string()),
            ("step", step.to_string()),
            ("phase", "running".to_string()),
            ("service", port.service.clone()),
            ("elapsed", format!("{}s", elapsed.as_secs())),
            ("attempts", attempts.to_string()),
        ]);
    };
    stack.post_start_preflight_with_heartbeat(executor, HOOK_HEARTBEAT_INTERVAL, &mut heartbeat)
}

fn prepare_failed_mount_lock_preflight(
    run_id: &str,
    sandbox: &Path,
    compose_stack: Option<&DockerComposeStack>,
    studiofs_bin_check: Option<PreflightCheckReport>,
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
    let mut checks = Vec::new();
    if let Some(check) = studiofs_bin_check {
        push_preflight_step_check(&mut checks, run_id, "studiofs_bin_fingerprint", check);
    }
    checks.push(PreflightCheckReport {
        name: "default Windows mounted E2E run lock".to_string(),
        status: RunStatus::PreflightFailed,
        detail,
    });
    Ok(PreparedPreflight {
        report: PreflightReport {
            status: RunStatus::PreflightFailed,
            checks,
        },
        compose_start_attempted: false,
    })
}

fn push_preflight_step_check(
    checks: &mut Vec<PreflightCheckReport>,
    run_id: &str,
    step: &str,
    check: PreflightCheckReport,
) {
    e2e_stage_step_progress(
        run_id,
        "preflight",
        step,
        preflight_step_status(check.status),
    );
    checks.push(check);
}

fn preflight_step_status(status: RunStatus) -> &'static str {
    if status == RunStatus::Passed {
        "Passed"
    } else {
        "Failed"
    }
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
    run_id: &str,
    stack: &DockerComposeStack,
    executor: &dyn CommandExecutor,
    cleanup_actions: &mut Vec<String>,
    cleanup_errors: &mut Vec<String>,
) {
    e2e_stage_step_progress(run_id, "cleanup", "docker_compose_logs", "started");
    match stack.collect_logs(executor) {
        Ok(_) => {
            e2e_stage_step_progress(run_id, "cleanup", "docker_compose_logs", "Passed");
            cleanup_actions.push("docker compose logs".to_string());
        }
        Err(err) => {
            e2e_stage_step_progress(run_id, "cleanup", "docker_compose_logs", "Failed");
            cleanup_errors.push(format!("docker compose logs: {err}"));
        }
    }
    e2e_stage_step_progress(run_id, "cleanup", "docker_diagnostics", "started");
    match stack.collect_optional_diagnostics(executor) {
        Ok(actions) => {
            e2e_stage_step_progress(run_id, "cleanup", "docker_diagnostics", "Passed");
            cleanup_actions.extend(actions);
        }
        Err(err) => {
            e2e_stage_step_progress(run_id, "cleanup", "docker_diagnostics", "Failed");
            cleanup_actions.push(format!("optional docker diagnostics skipped: {err}"));
        }
    }
    e2e_stage_step_progress(run_id, "cleanup", "docker_compose_down", "started");
    match stack.teardown(executor) {
        Ok(()) => {
            e2e_stage_step_progress(run_id, "cleanup", "docker_compose_down", "Passed");
            cleanup_actions.push("docker compose down --volumes --remove-orphans".to_string());
        }
        Err(err) => {
            e2e_stage_step_progress(run_id, "cleanup", "docker_compose_down", "Failed");
            cleanup_errors.push(format!("docker compose down: {err}"));
        }
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
    run_id: Option<String>,
    pid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessStatus {
    Running,
    #[cfg_attr(
        not(windows),
        expect(
            dead_code,
            reason = "Windows PID probes are the only production source"
        )
    )]
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
    reclaim_stale_default_mount_lock_for_holder(path, &holder, process_checker)
}

fn reclaim_stale_default_mount_lock_for_holder(
    path: &Path,
    holder: &DefaultMountLockHolder,
    process_checker: impl Fn(u32) -> ProcessStatus,
) -> std::io::Result<bool> {
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

/// Removes runner-owned E2E sandboxes and recovers their recorded infrastructure.
pub fn clean(request: CleanRequest) -> Result<CleanOutcome, RunnerError> {
    clean_with_executor_and_mount_lock_dir(
        request,
        &SystemCommandExecutor,
        &default_mount_lock_dir(),
        process_is_running,
    )
}

#[cfg(test)]
fn clean_with_executor(
    request: CleanRequest,
    executor: &dyn CommandExecutor,
) -> Result<CleanOutcome, RunnerError> {
    clean_with_executor_and_mount_lock_dir(
        request,
        executor,
        &default_mount_lock_dir(),
        process_is_running,
    )
}

fn clean_with_executor_and_mount_lock_dir(
    request: CleanRequest,
    executor: &dyn CommandExecutor,
    mount_lock_dir: &Path,
    process_checker: impl Fn(u32) -> ProcessStatus + Copy,
) -> Result<CleanOutcome, RunnerError> {
    if !request.root.exists() {
        let mut skipped = Vec::new();
        clean_default_mount_lock(mount_lock_dir, None, process_checker, &mut skipped);
        return Ok(CleanOutcome {
            removed: Vec::new(),
            skipped,
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
        match clean_entry(entry, &root, executor) {
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
    clean_default_mount_lock(mount_lock_dir, Some(&root), process_checker, &mut skipped);
    removed.sort();
    skipped.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(CleanOutcome { removed, skipped })
}

fn clean_default_mount_lock(
    mount_lock_dir: &Path,
    root: Option<&Path>,
    process_checker: impl Fn(u32) -> ProcessStatus + Copy,
    skipped: &mut Vec<CleanSkipped>,
) {
    let path = mount_lock_dir.join("default-windows-mount-points.lock");
    if !path.exists() {
        return;
    }
    if let Err(err) = reclaim_stale_default_mount_lock_after_clean(&path, root, process_checker) {
        skipped.push(CleanSkipped {
            path,
            error: format!("default Windows mounted E2E run lock cleanup failed: {err}"),
        });
    }
}

fn reclaim_stale_default_mount_lock_after_clean(
    path: &Path,
    root: Option<&Path>,
    process_checker: impl Fn(u32) -> ProcessStatus,
) -> std::io::Result<bool> {
    let Some(holder) = read_default_mount_lock_holder(path)? else {
        return Ok(false);
    };
    if let Some(run_id) = &holder.run_id {
        if validate_run_id(run_id).is_err() || root.is_some_and(|root| root.join(run_id).exists()) {
            return Ok(false);
        }
    }
    reclaim_stale_default_mount_lock_for_holder(path, &holder, process_checker)
}

fn clean_entry(
    entry: fs::DirEntry,
    root: &Path,
    executor: &dyn CommandExecutor,
) -> Result<Option<PathBuf>, RunnerError> {
    if !entry.file_type()?.is_dir() {
        return Ok(None);
    }
    let path = entry.path();
    let Some(manifest) = read_runner_owned_manifest(&path)? else {
        return Ok(None);
    };
    let canonical = path.canonicalize()?;
    if !canonical.starts_with(root) {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "{} escapes {}",
            canonical.display(),
            root.display()
        )));
    }
    recover_stale_infra(&manifest, executor)?;
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

fn resolve_scenario(name: &str) -> Result<ResolvedScenario, RunnerError> {
    let scenario = scenario::resolve_scenario(name)
        .ok_or_else(|| RunnerError::UnknownScenario(name.to_string()))?;
    Ok(ResolvedScenario {
        definition: scenario.definition(),
        scenario,
    })
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
        fs::create_dir_all(scenario_artifact_dir(sandbox, scenario.definition.name))?;
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
    studiofs_bin_fingerprint: Option<StudiofsBinaryReport>,
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
        studiofs_bin_fingerprint: input.studiofs_bin_fingerprint,
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
    scenario_index: usize,
    scenario_total: usize,
) -> Result<ScenarioLifecycleOutcome, RunnerError> {
    let Some(deadline) = deadline else {
        return run_scenario_lifecycle(
            events_path,
            run_id,
            resolved.scenario.as_ref(),
            &context,
            scenario_index,
            scenario_total,
        );
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
            scenario_index,
            scenario_total,
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
            e2e_progress_fields(&[
                ("run", run_id.clone()),
                ("scenario_name", definition.name.to_string()),
                ("hook", active_hook.clone()),
                ("status", "timed_out".to_string()),
            ]);
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

struct ScenarioChildRun<'a> {
    sandbox: &'a Path,
    events_path: &'a Path,
    run_id: &'a str,
    definition: scenario::ScenarioDefinition,
    scenario_index: usize,
    scenario_total: usize,
    executor: &'a dyn CommandExecutor,
    request: &'a RunRequest,
    deadline: Option<RunDeadline>,
}

fn run_scenario_child_with_timeout(
    child: ScenarioChildRun<'_>,
) -> Result<ScenarioLifecycleOutcome, RunnerError> {
    let ScenarioChildRun {
        sandbox,
        events_path,
        run_id,
        definition,
        scenario_index,
        scenario_total,
        executor,
        request,
        deadline,
    } = child;
    let started_at = timestamp();
    let started_instant = Instant::now();
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(definition.name);
    fs::create_dir_all(&scenario_dir)?;
    let report_path = scenario_report_path(&scenario_dir);
    let operation_log = scenario_dir.join("operations.ndjson");
    let child_stdout = scenario_dir.join("child.stdout.log");
    let child_stderr = scenario_dir.join("child.stderr.log");
    let timeout = if let Some(deadline) = deadline {
        match deadline.remaining() {
            Some(remaining) => Some(remaining),
            None => {
                let active_hook = active_scenario_hook(events_path, definition.name)
                    .unwrap_or_else(|| "not_started".to_string());
                let error = format!(
                    "whole-run timeout already elapsed before starting scenario child {}; active hook {active_hook}; cleanup attempted",
                    definition.name
                );
                let _ = record_event(
                    events_path,
                    run_id,
                    Some(definition.name),
                    &active_hook,
                    "timed_out",
                );
                return Ok(timed_out_lifecycle(
                    definition.name,
                    request.seed,
                    started_at,
                    started_instant,
                    error,
                ));
            }
        }
    } else {
        None
    };
    let mut args = vec![
        "__run-one".to_string(),
        definition.name.to_string(),
        "--root".to_string(),
        request.root.display().to_string(),
        "--run-id".to_string(),
        run_id.to_string(),
        "--seed".to_string(),
        request.seed.to_string(),
        "--infra".to_string(),
        request.infra.cli_value().to_string(),
    ];
    if let Some(studiofs_bin) = &request.studiofs_bin {
        args.push("--studiofs-bin".to_string());
        args.push(studiofs_bin.display().to_string());
    }
    let command = CommandSpec {
        program: std::env::current_exe()?.display().to_string(),
        args,
        env: Vec::new(),
        cwd: None,
        timeout,
        stdout_path: Some(child_stdout.clone()),
        stderr_path: Some(child_stderr.clone()),
    };
    let mut heartbeat = || {
        let active_hook = active_scenario_hook(events_path, definition.name)
            .unwrap_or_else(|| "not_started".to_string());
        let last_event = latest_operation_event(&operation_log)
            .unwrap_or_else(|| format!("{active_hook}.started"));
        let snapshot = HookProgressSnapshot {
            run_id,
            scenario_name: definition.name,
            scenario_index,
            scenario_total,
            hook: &active_hook,
            phase: "running",
            elapsed: started_instant.elapsed(),
            last_event: &last_event,
            sandbox: None,
        };
        let _ = write_hook_progress_snapshot(&snapshot);
    };
    let output = executor.run_with_heartbeat(&command, HOOK_HEARTBEAT_INTERVAL, &mut heartbeat)?;

    if output.status == -1 {
        let active_hook = active_scenario_hook(events_path, definition.name)
            .unwrap_or_else(|| "unknown".to_string());
        let error = format!(
            "scenario child timed out after {}ms; active hook {active_hook}; cleanup attempted",
            timeout.unwrap_or_default().as_millis()
        );
        let _ = record_event(
            events_path,
            run_id,
            Some(definition.name),
            &active_hook,
            "timed_out",
        );
        return Ok(timed_out_lifecycle(
            definition.name,
            request.seed,
            started_at,
            started_instant,
            error,
        ));
    }

    let report = if report_path.exists() {
        read_json_file::<ScenarioReport>(&report_path)?
    } else if output.status == 0 {
        return Err(RunnerError::MissingScenarioReport { path: report_path });
    } else {
        ScenarioReport {
            name: definition.name.to_string(),
            seed: request.seed,
            status: ScenarioStatus::Failed,
            started_at,
            finished_at: timestamp(),
            duration_ms: duration_millis(started_instant),
            artifacts: vec![scenario_artifact_path(definition.name, "operations.ndjson")],
            error: Some(format!(
                "scenario child terminated abnormally with status {}; stderr: {}; stdout: {}",
                output.status,
                read_text_snippet(&child_stderr)?.trim(),
                read_text_snippet(&child_stdout)?.trim()
            )),
        }
    };
    let outcome = ScenarioOutcome {
        name: report.name.clone(),
        status: report.status,
        artifacts: report.artifacts.clone(),
    };
    Ok(ScenarioLifecycleOutcome {
        report,
        outcome,
        cleanup_error: None,
    })
}

fn timed_out_lifecycle(
    scenario_name: &str,
    seed: u64,
    started_at: String,
    started_instant: Instant,
    error: String,
) -> ScenarioLifecycleOutcome {
    let operation_artifact = scenario_artifact_path(scenario_name, "operations.ndjson");
    ScenarioLifecycleOutcome {
        report: ScenarioReport {
            name: scenario_name.to_string(),
            seed,
            status: ScenarioStatus::TimedOut,
            started_at,
            finished_at: timestamp(),
            duration_ms: duration_millis(started_instant),
            artifacts: vec![operation_artifact.clone()],
            error: Some(error),
        },
        outcome: ScenarioOutcome {
            name: scenario_name.to_string(),
            status: ScenarioStatus::TimedOut,
            artifacts: vec![operation_artifact],
        },
        cleanup_error: None,
    }
}

fn failed_lifecycle(scenario_name: &str, seed: u64, error: String) -> ScenarioLifecycleOutcome {
    let operation_artifact = scenario_artifact_path(scenario_name, "operations.ndjson");
    let now = timestamp();
    ScenarioLifecycleOutcome {
        report: ScenarioReport {
            name: scenario_name.to_string(),
            seed,
            status: ScenarioStatus::Failed,
            started_at: now.clone(),
            finished_at: now,
            duration_ms: 0,
            artifacts: vec![operation_artifact.clone()],
            error: Some(error),
        },
        outcome: ScenarioOutcome {
            name: scenario_name.to_string(),
            status: ScenarioStatus::Failed,
            artifacts: vec![operation_artifact],
        },
        cleanup_error: None,
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
    let file = fs::File::open(events_path).ok()?;
    let reader = BufReader::new(file);
    let mut active = BTreeSet::new();
    for line in reader
        .lines()
        .map_while(Result::ok)
        .filter(|line| !line.trim().is_empty())
    {
        let Ok(event) = serde_json::from_str::<EventRecord>(&line) else {
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
    scenario_index: usize,
    scenario_total: usize,
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
        ScenarioProgressContext {
            operation_log: &operation_log,
            scenario_index,
            scenario_total,
        },
    );
    let cleanup_result = run_hook(
        events_path,
        &mut operation_writer,
        run_id,
        HookContext {
            scenario: definition.name,
            hook: "cleanup",
            scenario_index,
            scenario_total,
            operation_log: &operation_log,
        },
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
    progress: ScenarioProgressContext<'_>,
) -> Result<Vec<String>, RunnerError> {
    let definition = scenario.definition();
    run_hook(
        events_path,
        operation_writer,
        run_id,
        HookContext {
            scenario: definition.name,
            hook: "setup",
            scenario_index: progress.scenario_index,
            scenario_total: progress.scenario_total,
            operation_log: progress.operation_log,
        },
        || scenario.setup(context),
    )?;
    run_hook(
        events_path,
        operation_writer,
        run_id,
        HookContext {
            scenario: definition.name,
            hook: "run",
            scenario_index: progress.scenario_index,
            scenario_total: progress.scenario_total,
            operation_log: progress.operation_log,
        },
        || scenario.run(context),
    )?;
    run_hook(
        events_path,
        operation_writer,
        run_id,
        HookContext {
            scenario: definition.name,
            hook: "validate",
            scenario_index: progress.scenario_index,
            scenario_total: progress.scenario_total,
            operation_log: progress.operation_log,
        },
        || scenario.validate(context),
    )
}

#[derive(Clone, Copy)]
struct ScenarioProgressContext<'a> {
    scenario_index: usize,
    scenario_total: usize,
    operation_log: &'a Path,
}

#[derive(Clone, Copy)]
struct HookContext<'a> {
    scenario: &'a str,
    hook: &'a str,
    scenario_index: usize,
    scenario_total: usize,
    operation_log: &'a Path,
}

fn run_hook<T>(
    events_path: &Path,
    operation_writer: &mut OperationLogWriter,
    run_id: &str,
    hook_context: HookContext<'_>,
    f: impl FnOnce() -> Result<T, scenario::ScenarioError>,
) -> Result<T, RunnerError> {
    run_hook_with_heartbeat_interval(
        events_path,
        operation_writer,
        run_id,
        hook_context,
        HOOK_HEARTBEAT_INTERVAL,
        f,
    )
}

fn run_hook_with_heartbeat_interval<T>(
    events_path: &Path,
    operation_writer: &mut OperationLogWriter,
    run_id: &str,
    hook_context: HookContext<'_>,
    heartbeat_interval: Duration,
    f: impl FnOnce() -> Result<T, scenario::ScenarioError>,
) -> Result<T, RunnerError> {
    let scenario = hook_context.scenario;
    let hook = hook_context.hook;
    record_event(events_path, run_id, Some(scenario), hook, "started")?;
    record_operation_checkpoint(operation_writer, scenario, hook, "started")?;
    let started = Instant::now();
    let started_event = format!("{hook}.started");
    let start_snapshot = HookProgressSnapshot {
        run_id,
        scenario_name: scenario,
        scenario_index: hook_context.scenario_index,
        scenario_total: hook_context.scenario_total,
        hook,
        phase: "started",
        elapsed: Duration::ZERO,
        last_event: &started_event,
        sandbox: None,
    };
    write_hook_progress_snapshot(&start_snapshot)?;
    let _heartbeat = HookHeartbeat::start(HookHeartbeatInput {
        run_id: run_id.to_string(),
        scenario_name: scenario.to_string(),
        scenario_index: hook_context.scenario_index,
        scenario_total: hook_context.scenario_total,
        hook: hook.to_string(),
        sandbox: None,
        operation_log: hook_context.operation_log.to_path_buf(),
        interval: heartbeat_interval,
    });
    let result = f();
    let phase = if result.is_ok() { "finished" } else { "failed" };
    let last_event = latest_operation_event(hook_context.operation_log)
        .unwrap_or_else(|| format!("{hook}.{phase}"));
    let finish_snapshot = HookProgressSnapshot {
        run_id,
        scenario_name: scenario,
        scenario_index: hook_context.scenario_index,
        scenario_total: hook_context.scenario_total,
        hook,
        phase,
        elapsed: started.elapsed(),
        last_event: &last_event,
        sandbox: None,
    };
    write_hook_progress_snapshot(&finish_snapshot)?;
    record_event(events_path, run_id, Some(scenario), hook, phase)?;
    record_operation_checkpoint(operation_writer, scenario, hook, phase)?;
    Ok(result?)
}

struct HookHeartbeatInput {
    run_id: String,
    scenario_name: String,
    scenario_index: usize,
    scenario_total: usize,
    hook: String,
    sandbox: Option<PathBuf>,
    operation_log: PathBuf,
    interval: Duration,
}

struct HookHeartbeat {
    stop: Sender<()>,
    handle: Option<thread::JoinHandle<()>>,
}

impl HookHeartbeat {
    fn start(input: HookHeartbeatInput) -> Self {
        let started = Instant::now();
        let (stop, rx) = mpsc::channel();
        let handle = thread::Builder::new()
            .name("e2e-heartbeat".to_string())
            .spawn(move || {
                loop {
                    match rx.recv_timeout(input.interval) {
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {
                            let last_event = latest_operation_event(&input.operation_log)
                                .unwrap_or_else(|| format!("{}.started", input.hook));
                            let snapshot = HookProgressSnapshot {
                                run_id: &input.run_id,
                                scenario_name: &input.scenario_name,
                                scenario_index: input.scenario_index,
                                scenario_total: input.scenario_total,
                                hook: &input.hook,
                                phase: "running",
                                elapsed: started.elapsed(),
                                last_event: &last_event,
                                sandbox: input.sandbox.as_deref(),
                            };
                            let mut stdout = std::io::stdout().lock();
                            let _ = write_hook_progress_line(&mut stdout, &snapshot);
                        }
                    }
                }
            })
            .ok();
        Self { stop, handle }
    }
}

impl Drop for HookHeartbeat {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct HookProgressSnapshot<'a> {
    run_id: &'a str,
    scenario_name: &'a str,
    scenario_index: usize,
    scenario_total: usize,
    hook: &'a str,
    phase: &'a str,
    elapsed: Duration,
    last_event: &'a str,
    sandbox: Option<&'a Path>,
}

fn write_hook_progress_line(
    mut writer: impl std::io::Write,
    snapshot: &HookProgressSnapshot<'_>,
) -> std::io::Result<()> {
    write!(
        writer,
        "[studiofs-e2e][{}] run={} scenario={}/{} scenario_name={} hook={} phase={} elapsed={}s last_event={}",
        timestamp(),
        format_progress_value(snapshot.run_id),
        snapshot.scenario_index,
        snapshot.scenario_total,
        format_progress_value(snapshot.scenario_name),
        format_progress_value(snapshot.hook),
        format_progress_value(snapshot.phase),
        snapshot.elapsed.as_secs(),
        format_progress_value(snapshot.last_event)
    )?;
    if let Some(sandbox) = snapshot.sandbox {
        write!(
            writer,
            " sandbox={}",
            format_progress_value(&sandbox.display().to_string())
        )?;
    }
    writeln!(writer)?;
    writer.flush()
}

fn write_hook_progress_snapshot(snapshot: &HookProgressSnapshot<'_>) -> std::io::Result<()> {
    let mut stdout = std::io::stdout().lock();
    write_hook_progress_line(&mut stdout, snapshot)
}

fn latest_operation_event(path: &Path) -> Option<String> {
    read_last_nonempty_line(path)
        .ok()
        .flatten()
        .and_then(|line| operation_event_from_line(&line))
}

fn read_last_nonempty_line(path: &Path) -> std::io::Result<Option<String>> {
    const TAIL_CHUNK_SIZE: u64 = 8192;

    let mut file = fs::File::open(path)?;
    let mut position = file.metadata()?.len();
    let mut tail = Vec::new();

    while position > 0 {
        let read_len = TAIL_CHUNK_SIZE.min(position);
        position -= read_len;
        file.seek(SeekFrom::Start(position))?;
        let mut chunk = vec![0; usize::try_from(read_len).unwrap_or(usize::MAX)];
        file.read_exact(&mut chunk)?;
        chunk.extend_from_slice(&tail);
        tail = chunk;

        if let Some(line) = last_complete_nonempty_line(&tail, position == 0) {
            return Ok(Some(line));
        }
    }

    Ok(None)
}

fn last_complete_nonempty_line(bytes: &[u8], at_file_start: bool) -> Option<String> {
    let end = bytes
        .iter()
        .rposition(|byte| !matches!(byte, b'\n' | b'\r' | b' ' | b'\t'))?
        + 1;
    let trimmed = &bytes[..end];
    let start = trimmed
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1);
    if start == 0 && !at_file_start {
        return None;
    }
    let line = String::from_utf8_lossy(&trimmed[start..])
        .trim_end_matches('\r')
        .to_string();
    if line.trim().is_empty() {
        None
    } else {
        Some(line)
    }
}

fn operation_event_from_line(line: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    #[derive(Deserialize)]
    struct ProgressOperationRecord {
        sequence: u64,
        client: Option<String>,
        operation: OperationTag,
    }

    #[derive(Deserialize)]
    struct OperationTag {
        #[serde(rename = "type")]
        operation_type: String,
    }

    let record = serde_json::from_str::<ProgressOperationRecord>(line).ok()?;
    let operation_type = record.operation.operation_type;
    let mut event = format!("seq={},op={operation_type}", record.sequence);
    if let Some(client) = record.client {
        event.push_str(",client=");
        event.push_str(&client);
    }
    Some(event)
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

fn collect_report_artifacts_best_effort(
    sandbox: &Path,
    scenarios: &[ScenarioOutcome],
) -> Result<Vec<ArtifactReport>, RunnerError> {
    match collect_report_artifacts(sandbox, scenarios) {
        Ok(artifacts) => Ok(artifacts),
        Err(error) => {
            let diagnostic = format!("artifact collection failed: {error}\n");
            let diagnostic_path = sandbox.join("artifact-collection-error.txt");
            let diagnostic_written = fs::write(&diagnostic_path, diagnostic).is_ok();
            let mut artifacts = required_artifacts(scenarios);
            if diagnostic_written {
                artifacts.push(ArtifactReport {
                    name: "artifact-collection-error.txt".to_string(),
                    path: "artifact-collection-error.txt".to_string(),
                    required: false,
                });
            }
            Ok(artifacts)
        }
    }
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

fn scenario_artifact_dir(sandbox: &Path, scenario: &str) -> PathBuf {
    sandbox.join("artifacts").join("scenarios").join(scenario)
}

fn scenario_client_runtime_sandbox(sandbox: &Path, scenario: &str) -> PathBuf {
    scenario_artifact_dir(sandbox, scenario).join("client-runtime")
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

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, RunnerError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn read_text_snippet(path: &Path) -> Result<String, RunnerError> {
    const MAX_SNIPPET_BYTES: usize = 8192;
    if !path.exists() {
        return Ok(String::new());
    }
    let bytes = fs::read(path)?;
    let start = bytes.len().saturating_sub(MAX_SNIPPET_BYTES);
    Ok(String::from_utf8_lossy(&bytes[start..]).into_owned())
}

fn scenario_report_path(scenario_dir: &Path) -> PathBuf {
    scenario_dir.join("scenario-report.json")
}

fn write_scenario_report(scenario_dir: &Path, report: &ScenarioReport) -> Result<(), RunnerError> {
    write_pretty_json(&scenario_report_path(scenario_dir), report)
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

fn read_runner_owned_manifest(path: &Path) -> Result<Option<RunManifest>, RunnerError> {
    let manifest = path.join("run.json");
    let marker = path.join(RUNNER_MARKER_FILE);
    if !manifest.exists() || !marker.exists() {
        return Ok(None);
    }
    let manifest_value: serde_json::Value = serde_json::from_slice(&fs::read(&manifest)?)?;
    let marker_value: serde_json::Value = serde_json::from_slice(&fs::read(marker)?)?;
    if marker_value["runner_crate"] != RUNNER_CRATE {
        return Ok(None);
    }
    if manifest_value["runner_crate"] != RUNNER_CRATE {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "cannot prove runner ownership for {}: run.json runner_crate is not {RUNNER_CRATE}",
            path.display()
        )));
    }
    let Some(run_id) = manifest_value["run_id"].as_str() else {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "cannot prove runner ownership for {}: run.json is missing run_id",
            path.display()
        )));
    };
    if marker_value["run_id"] != run_id {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "cannot prove runner ownership for {}: marker run_id does not match run.json",
            path.display()
        )));
    }
    if path.file_name().and_then(|name| name.to_str()) != Some(run_id) {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "cannot prove runner ownership for {}: directory name does not match run_id {run_id}",
            path.display()
        )));
    }
    let Some(manifest_sandbox) = manifest_value["sandbox"].as_str() else {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "cannot prove runner ownership for {}: run.json is missing sandbox",
            path.display()
        )));
    };
    if !Path::new(manifest_sandbox).exists()
        || Path::new(manifest_sandbox).canonicalize()? != path.canonicalize()?
    {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "cannot prove runner ownership for {}: manifest sandbox does not match candidate",
            path.display()
        )));
    }
    for required in [RUNNER_MARKER_FILE, "run.json", "artifacts"] {
        if !path.join(required).exists() {
            return Err(RunnerError::UnsafeCleanupRoot(format!(
                "cannot prove runner ownership for {}: missing required sandbox path {required}",
                path.display()
            )));
        }
    }
    let manifest = serde_json::from_value(manifest_value)?;
    Ok(Some(manifest))
}

fn recover_stale_infra(
    manifest: &RunManifest,
    executor: &dyn CommandExecutor,
) -> Result<(), RunnerError> {
    if manifest.infra != InfraMode::Compose {
        return Ok(());
    }
    let Some(project_name) = manifest.compose_project.as_deref() else {
        return Err(RunnerError::UnsafeCleanupRoot(format!(
            "cannot prove runner ownership for compose infrastructure in run {}: missing compose_project",
            manifest.run_id
        )));
    };
    let control = ComposeServiceControl::for_project(project_name.to_string())?;
    control.teardown(executor)?;
    Ok(())
}

fn stale_infra_preflight_detail(
    root: &Path,
    current_sandbox: &Path,
) -> Result<Option<String>, RunnerError> {
    if !root.exists() {
        return Ok(None);
    }
    let root = canonicalize_cleanup_root(root)?;
    let current_sandbox = current_sandbox.canonicalize().ok();
    let mut stale = Vec::new();
    for entry_result in fs::read_dir(&root)? {
        let entry = match entry_result {
            Ok(entry) => entry,
            Err(err) => {
                stale.push(format!("<entry-error: {err}>"));
                continue;
            }
        };
        let path = entry.path();
        let is_dir = match entry.file_type() {
            Ok(file_type) => file_type.is_dir(),
            Err(err) => {
                stale.push(format!("{} (ambiguous: {err})", path.display()));
                continue;
            }
        };
        if !is_dir {
            continue;
        }
        if current_sandbox.as_ref().is_some_and(|current| {
            path.canonicalize()
                .is_ok_and(|candidate| candidate == *current)
        }) {
            continue;
        }
        let cleanup_passed = match cleanup_report_passed(&path) {
            Ok(passed) => passed,
            Err(err) => {
                stale.push(format!("{} (report-error: {err})", path.display()));
                false
            }
        };
        if cleanup_passed {
            continue;
        }
        let manifest = match read_runner_owned_manifest(&path) {
            Ok(Some(manifest)) => manifest,
            Ok(None) => continue,
            Err(err) => {
                stale.push(format!(
                    "{} (ambiguous: {err})",
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("<non-utf8>")
                ));
                continue;
            }
        };
        if manifest.infra == InfraMode::Compose {
            let project = manifest
                .compose_project
                .as_deref()
                .unwrap_or("<missing-project>");
            stale.push(format!("{} ({project})", manifest.run_id));
        }
    }
    if stale.is_empty() {
        return Ok(None);
    }
    stale.sort();
    Ok(Some(format!(
        "found stale runner-owned compose infrastructure: {}; run `studiofs-e2e clean --root {}` before starting a full E2E sweep",
        stale.join(", "),
        root.display()
    )))
}

fn cleanup_report_passed(path: &Path) -> Result<bool, RunnerError> {
    let cleanup_path = path.join("cleanup.json");
    if !cleanup_path.exists() {
        return Ok(false);
    }
    let cleanup: CleanupReport = read_json_file(&cleanup_path)?;
    Ok(cleanup.status == RunStatus::Passed)
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

fn e2e_progress_fields(fields: &[(&str, String)]) {
    let mut line = String::new();
    for (index, (key, value)) in fields.iter().enumerate() {
        if index > 0 {
            line.push(' ');
        }
        line.push_str(key);
        line.push('=');
        line.push_str(&format_progress_value(value));
    }
    e2e_progress(&line);
}

fn e2e_stage_step_progress(run_id: &str, stage: &str, step: &str, state: &str) {
    let state_key = if matches!(state, "started" | "running") {
        "phase"
    } else {
        "status"
    };
    e2e_progress_fields(&[
        ("run", run_id.to_string()),
        ("stage", stage.to_string()),
        ("step", step.to_string()),
        (state_key, state.to_string()),
    ]);
}

fn format_progress_value(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || matches!(ch, '"' | '\\'))
    {
        serde_json::to_string(value).unwrap_or_else(|_| "\"<invalid>\"".to_string())
    } else {
        value.to_string()
    }
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
    use crate::infra::{CommandExecutor, CommandOutput, InfraMode, ServicePort};
    use std::cell::{Cell, RefCell};

    const DEFINITELY_DEAD_TEST_PID: u32 = 4_294_967_294;

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

        let outcome =
            run_scenario_lifecycle(&events_path, "run-1", &scenario, &context, 1, 1).unwrap();

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
    fn studiofs_bin_preflight_reports_fingerprint() {
        let temp = tempfile::tempdir().unwrap();
        let bin = temp.path().join("studiofs.exe");
        fs::write(&bin, b"studiofs-test-binary").unwrap();

        let (fingerprint, check) = studiofs_bin_preflight_check(Some(&bin));

        let fingerprint = fingerprint.expect("fingerprint should be recorded");
        assert_eq!(fingerprint.path, bin.display().to_string());
        assert_eq!(fingerprint.size_bytes, 20);
        assert_eq!(
            fingerprint.sha256,
            "babf491ea85eb483aacbae06a67e8e3c8cd8fc7da470eca166701d2964276205"
        );
        assert!(Path::new(&fingerprint.canonical_path).is_absolute());
        let check = check.expect("preflight check should be recorded");
        assert_eq!(check.name, "studiofs binary fingerprint");
        assert_eq!(check.status, RunStatus::Passed);
        assert!(check.detail.contains("size_bytes=20"));
        assert!(check.detail.contains(&fingerprint.sha256));
    }

    #[test]
    fn studiofs_bin_preflight_fails_when_binary_is_unreadable() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing-studiofs.exe");

        let (fingerprint, check) = studiofs_bin_preflight_check(Some(&missing));

        assert!(fingerprint.is_none());
        let check = check.expect("preflight check should be recorded");
        assert_eq!(check.name, "studiofs binary fingerprint");
        assert_eq!(check.status, RunStatus::PreflightFailed);
        assert!(check.detail.contains("missing-studiofs.exe"));
    }

    #[test]
    fn run_records_studiofs_bin_fingerprint_in_artifacts() {
        let temp = tempfile::tempdir().unwrap();
        let bin = temp.path().join("studiofs.exe");
        fs::write(&bin, b"studiofs-test-binary").unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("fingerprint-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 373,
                studiofs_bin: Some(bin.clone()),
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
            &temp.path().join("mount-locks"),
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Passed);
        let manifest = read_json_file::<RunManifest>(&outcome.sandbox.join("run.json")).unwrap();
        let manifest_fingerprint = manifest
            .studiofs_bin_fingerprint
            .expect("manifest should record fingerprint");
        assert_eq!(manifest_fingerprint.path, bin.display().to_string());
        assert_eq!(
            manifest_fingerprint.sha256,
            "babf491ea85eb483aacbae06a67e8e3c8cd8fc7da470eca166701d2964276205"
        );

        let preflight =
            read_json_file::<PreflightReport>(&outcome.sandbox.join("preflight.json")).unwrap();
        let fingerprint_check = preflight
            .checks
            .iter()
            .find(|check| check.name == "studiofs binary fingerprint")
            .expect("preflight should include fingerprint check");
        assert_eq!(fingerprint_check.status, RunStatus::Passed);
        assert!(
            fingerprint_check
                .detail
                .contains(&manifest_fingerprint.sha256)
        );

        let report = read_json_file::<RunReport>(&outcome.report_path).unwrap();
        assert_eq!(
            report
                .studiofs_bin
                .expect("report should record fingerprint")
                .sha256,
            manifest_fingerprint.sha256
        );
    }

    #[test]
    fn run_one_compose_config_uses_scenario_scoped_client_sandbox() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let run_id = "shared-infra-run";
        let sandbox = root.join(run_id);
        let scenario_name = scenario::BASIC_MULTI_CLIENT_SMOKE;
        let manifest = build_manifest(ManifestInput {
            run_id,
            seed: 360,
            sandbox: &sandbox,
            scenarios: &[scenario_name.to_string()],
            retention: RetentionPolicy::Always,
            selected_platform: "windows-winfsp",
            infra: InfraMode::Compose,
            compose_project: Some("studiofs-e2e-shared-infra-run"),
            allocated_ports: vec![
                ServicePort {
                    service: "postgres".to_string(),
                    host: "127.0.0.1".to_string(),
                    host_port: 25432,
                    container_port: 5432,
                    env_var: "STUDIOFS_E2E_POSTGRES_PORT".to_string(),
                },
                ServicePort {
                    service: "rustfs".to_string(),
                    host: "127.0.0.1".to_string(),
                    host_port: 29000,
                    container_port: 9000,
                    env_var: "STUDIOFS_E2E_RUSTFS_PORT".to_string(),
                },
            ],
            studiofs_bin: Some(Path::new("target/debug/studiofs.exe")),
            studiofs_bin_fingerprint: None,
            port_range: Some("29000-29100"),
            timeout: Some("30m"),
            created_at: "2026-05-08T00:00:00Z",
        });
        let request = RunOneRequest {
            root,
            run_id: run_id.to_string(),
            scenario: scenario_name.to_string(),
            seed: 360,
            studiofs_bin: Some(PathBuf::from("target/debug/studiofs.exe")),
            infra: InfraMode::Compose,
        };

        let config = run_one_client_controller_config(&request, &manifest, &sandbox, scenario_name)
            .unwrap()
            .expect("compose config");

        assert_eq!(
            config.sandbox,
            scenario_client_runtime_sandbox(&sandbox, scenario_name)
        );
        assert_ne!(config.sandbox, sandbox);
        assert!(config.sandbox.ends_with(Path::new(
            "artifacts/scenarios/basic-multi-client-smoke/client-runtime"
        )));
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

    #[test]
    fn hook_progress_heartbeat_reports_active_hook_context() {
        let mut output = Vec::new();
        let progress = HookProgressSnapshot {
            run_id: "run-1",
            scenario_name: "slow-scenario",
            scenario_index: 13,
            scenario_total: 13,
            hook: "run",
            phase: "running",
            elapsed: Duration::from_secs(90),
            last_event: "run.started",
            sandbox: Some(Path::new("target/e2e-runs/run-1")),
        };

        write_hook_progress_line(&mut output, &progress).unwrap();

        let line = String::from_utf8(output).unwrap();
        assert!(line.contains("run=run-1"));
        assert!(line.contains("scenario=13/13"));
        assert!(line.contains("scenario_name=slow-scenario"));
        assert!(line.contains("hook=run"));
        assert!(line.contains("phase=running"));
        assert!(line.contains("elapsed=90s"));
        assert!(line.contains("last_event=run.started"));
        assert!(line.contains("sandbox=target/e2e-runs/run-1"));
        assert!(line.ends_with('\n'));
    }

    #[test]
    fn heartbeat_last_event_uses_latest_operation_record() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("operations.ndjson");
        let mut writer = OperationLogWriter::open_append(&log).unwrap();
        let record = writer
            .record(
                "slow-scenario",
                ScenarioOperation::Write {
                    path: "file.bin".to_string(),
                    offset: Some(0),
                    bytes: 42,
                    checksum_sha256: None,
                },
            )
            .with_client("client-a");
        writer.append_record(&record).unwrap();

        let event = latest_operation_event(&log).unwrap();

        assert_eq!(event, "seq=1,op=write,client=client-a");
    }

    #[test]
    fn heartbeat_last_event_reads_only_last_nonempty_operation_line() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("operations.ndjson");
        let mut writer = OperationLogWriter::open_append(&log).unwrap();
        for sequence in 0..600 {
            let record = writer.record(
                "slow-scenario",
                ScenarioOperation::CreateDirectory {
                    path: format!("dir-{sequence}"),
                },
            );
            writer.append_record(&record).unwrap();
        }
        std::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(b"\n\n")
            .unwrap();

        let event = latest_operation_event(&log).unwrap();

        assert_eq!(event, "seq=600,op=create_directory");
    }

    #[test]
    fn progress_values_with_spaces_are_quoted() {
        let mut output = Vec::new();
        let progress = HookProgressSnapshot {
            run_id: "run with spaces",
            scenario_name: "slow scenario",
            scenario_index: 1,
            scenario_total: 1,
            hook: "run",
            phase: "running",
            elapsed: Duration::from_secs(30),
            last_event: "seq=2,op=write",
            sandbox: Some(Path::new("target/e2e runs/run 1")),
        };

        write_hook_progress_line(&mut output, &progress).unwrap();

        let line = String::from_utf8(output).unwrap();
        assert!(line.contains("run=\"run with spaces\""));
        assert!(line.contains("scenario_name=\"slow scenario\""));
        assert!(line.contains("last_event=seq=2,op=write"));
        assert!(line.contains("sandbox=\"target/e2e runs/run 1\""));
    }

    #[test]
    fn hook_heartbeat_does_not_deadlock_after_slow_hook() {
        let temp = tempfile::tempdir().unwrap();
        let events_path = temp.path().join("events.ndjson");
        let operation_log = temp.path().join("operations.ndjson");
        let (tx, rx) = std::sync::mpsc::channel();
        let worker_events_path = events_path.clone();
        let worker_operation_log = operation_log.clone();

        std::thread::spawn(move || {
            let mut operation_writer =
                OperationLogWriter::open_append(&worker_operation_log).unwrap();
            let result = run_hook_with_heartbeat_interval(
                &worker_events_path,
                &mut operation_writer,
                "heartbeat-run",
                HookContext {
                    scenario: "__test-heartbeat",
                    hook: "run",
                    scenario_index: 1,
                    scenario_total: 1,
                    operation_log: &worker_operation_log,
                },
                Duration::from_millis(10),
                || {
                    std::thread::sleep(Duration::from_millis(60));
                    Ok(())
                },
            );
            tx.send(result.map(|_| ())).unwrap();
        });

        let result = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("hook heartbeat should not deadlock after a slow hook");
        assert!(result.is_ok(), "{result:?}");
        let events = std::fs::read_to_string(events_path).unwrap();
        assert!(events.contains("\"phase\":\"started\""));
        assert!(events.contains("\"phase\":\"finished\""));
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
    fn compose_failure_run_writes_report_before_compose_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let run_id = "compose-failed-run";
        let report_path = root.join(run_id).join("report.json");
        let executor = RecordingExecutor::default();
        executor
            .require_report_before_compose_cleanup
            .replace(Some(report_path.clone()));

        let outcome = run_with_executor(
            RunRequest {
                root,
                run_id: Some(run_id.to_string()),
                scenarios: vec![scenario::TEST_FAILING_VALIDATE.to_string()],
                seed: 383,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        assert_eq!(outcome.exit_code, 1);
        assert!(report_path.exists());
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
        assert_eq!(report["status"], "failed");
        assert!(
            report["scenarios"][0]["error"]
                .as_str()
                .unwrap()
                .contains("validation failed")
        );
        assert_eq!(report["cleanup"]["status"], "passed");
        assert!(
            report["cleanup"]["actions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|action| action.as_str() == Some("docker compose logs"))
        );
    }

    #[test]
    fn windows_mount_lock_is_released_before_compose_cleanup_after_mounted_failure() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let lock_dir = temp.path().join("mount-locks");
        let lock_path = lock_dir.join("default-windows-mount-points.lock");
        let run_id = "mounted-failed-run";
        let report_path = root.join(run_id).join("report.json");
        let executor = RecordingExecutor::default();
        executor.fail_next_run_one_without_report.set(true);
        executor
            .require_report_before_compose_cleanup
            .replace(Some(report_path.clone()));
        executor
            .require_missing_lock_before_compose_cleanup
            .replace(Some(lock_path.clone()));
        let studiofs_bin = temp.path().join("studiofs.exe");
        fs::write(&studiofs_bin, b"test studiofs binary").unwrap();

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root,
                run_id: Some(run_id.to_string()),
                scenarios: vec![
                    scenario::BASIC_MULTI_CLIENT_SMOKE.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 384,
                studiofs_bin: Some(studiofs_bin),
                platform: PlatformSelection::WindowsWinfsp,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33340-33350".to_string()),
                timeout: Some("30s".to_string()),
                infra: InfraMode::Compose,
            },
            &executor,
            &lock_dir,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        assert!(report_path.exists());
        assert!(!lock_path.exists());
        let report: RunReport = read_json_file(&report_path).unwrap();
        assert_eq!(report.scenarios[0].status, ScenarioStatus::Failed);
        assert_eq!(report.cleanup.status, RunStatus::Passed);
    }

    #[test]
    fn run_writes_report_and_cleans_up_when_child_supervisor_errors() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let run_id = "child-supervisor-error";
        let report_path = root.join(run_id).join("report.json");
        let executor = RecordingExecutor::default();
        executor.io_error_next_run_one.set(true);

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root,
                run_id: Some(run_id.to_string()),
                scenarios: vec![
                    scenario::TEST_PASSING.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 385,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33360-33370".to_string()),
                timeout: Some("30s".to_string()),
                infra: InfraMode::Compose,
            },
            &executor,
            &temp.path().join("mount-locks"),
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        assert!(report_path.exists());
        let report: RunReport = read_json_file(&report_path).unwrap();
        assert_eq!(report.scenarios[0].status, ScenarioStatus::Failed);
        assert!(
            report.scenarios[0]
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("scenario supervisor failed before report was written")
        );
        assert_eq!(report.cleanup.status, RunStatus::Passed);
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
        assert_eq!(calls[logs_index].timeout, Some(Duration::from_secs(120)));
        assert_eq!(calls[down_index].timeout, Some(Duration::from_secs(120)));
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
    fn clean_recovers_manifest_owned_compose_infra_before_removing_sandbox() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let sandbox = root.join("interrupted-compose-run");
        fs::create_dir_all(sandbox.join("artifacts")).unwrap();
        write_pretty_json(
            &sandbox.join(RUNNER_MARKER_FILE),
            &serde_json::json!({
                "runner_crate": RUNNER_CRATE,
                "run_id": "interrupted-compose-run",
            }),
        )
        .unwrap();
        write_pretty_json(
            &sandbox.join("run.json"),
            &build_manifest(ManifestInput {
                run_id: "interrupted-compose-run",
                seed: 373,
                sandbox: &sandbox,
                scenarios: &[scenario::TEST_PASSING.to_string()],
                retention: RetentionPolicy::Always,
                selected_platform: "windows-winfsp",
                infra: InfraMode::Compose,
                compose_project: Some("studiofs-e2e-interrupted-compose-run"),
                allocated_ports: Vec::new(),
                studiofs_bin: None,
                studiofs_bin_fingerprint: None,
                port_range: None,
                timeout: None,
                created_at: &timestamp(),
            }),
        )
        .unwrap();

        let ambiguous = root.join("ambiguous-compose-run");
        fs::create_dir_all(ambiguous.join("artifacts")).unwrap();
        write_pretty_json(
            &ambiguous.join(RUNNER_MARKER_FILE),
            &serde_json::json!({
                "runner_crate": RUNNER_CRATE,
                "run_id": "ambiguous-compose-run",
            }),
        )
        .unwrap();
        fs::write(
            ambiguous.join("run.json"),
            r#"{"runner_crate":"studiofs-e2e","run_id":"ambiguous-compose-run","infra":"compose","compose_project":"studiofs-e2e-ambiguous-compose-run"}"#,
        )
        .unwrap();

        let executor = RecordingExecutor::default();
        let outcome = clean_with_executor(CleanRequest { root }, &executor).unwrap();

        assert_eq!(
            outcome.removed,
            vec![PathBuf::from("interrupted-compose-run")]
        );
        assert_eq!(outcome.skipped.len(), 1);
        assert!(outcome.skipped[0].path.ends_with("ambiguous-compose-run"));
        assert!(
            outcome.skipped[0]
                .error
                .contains("cannot prove runner ownership")
        );
        assert!(!sandbox.exists());
        assert!(ambiguous.exists());
        let calls = executor.calls.borrow();
        let down_call = calls
            .iter()
            .find(|call| call.args.contains(&"down".to_string()))
            .expect("compose recovery should tear down manifest-owned project");
        assert!(down_call.args.windows(2).any(|args| args
            == [
                "--project-name".to_string(),
                "studiofs-e2e-interrupted-compose-run".to_string()
            ]));
        assert!(down_call.args.contains(&"--volumes".to_string()));
        assert!(down_call.args.contains(&"--remove-orphans".to_string()));
    }

    #[test]
    fn compose_preflight_reports_stale_owned_infra_before_starting_new_stack() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let stale = root.join("stale-compose-run");
        fs::create_dir_all(stale.join("artifacts")).unwrap();
        write_pretty_json(
            &stale.join(RUNNER_MARKER_FILE),
            &serde_json::json!({
                "runner_crate": RUNNER_CRATE,
                "run_id": "stale-compose-run",
            }),
        )
        .unwrap();
        write_pretty_json(
            &stale.join("run.json"),
            &build_manifest(ManifestInput {
                run_id: "stale-compose-run",
                seed: 373,
                sandbox: &stale,
                scenarios: &[scenario::TEST_PASSING.to_string()],
                retention: RetentionPolicy::Always,
                selected_platform: "windows-winfsp",
                infra: InfraMode::Compose,
                compose_project: Some("studiofs-e2e-stale-compose-run"),
                allocated_ports: Vec::new(),
                studiofs_bin: None,
                studiofs_bin_fingerprint: None,
                port_range: None,
                timeout: None,
                created_at: &timestamp(),
            }),
        )
        .unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: root.clone(),
                run_id: Some("next-compose-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 373,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33400-33410".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
            &temp.path().join("mount-locks"),
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::PreflightFailed);
        let report = read_json_file::<RunReport>(&outcome.report_path).unwrap();
        let stale_check = report
            .preflight
            .checks
            .iter()
            .find(|check| check.name == "stale runner-owned E2E infrastructure")
            .expect("preflight should include stale infra diagnostic");
        assert_eq!(stale_check.status, RunStatus::PreflightFailed);
        assert!(stale_check.detail.contains("stale-compose-run"));
        assert!(stale_check.detail.contains("studiofs-e2e clean --root"));
        assert!(
            !executor
                .calls
                .borrow()
                .iter()
                .any(|call| call.args.contains(&"up".to_string())),
            "preflight must not start a new compose stack when stale owned infra is known"
        );
    }

    #[test]
    fn compose_preflight_ignores_retained_runs_that_completed_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let retained = root.join("retained-compose-run");
        fs::create_dir_all(retained.join("artifacts")).unwrap();
        write_pretty_json(
            &retained.join(RUNNER_MARKER_FILE),
            &serde_json::json!({
                "runner_crate": RUNNER_CRATE,
                "run_id": "retained-compose-run",
            }),
        )
        .unwrap();
        write_pretty_json(
            &retained.join("run.json"),
            &build_manifest(ManifestInput {
                run_id: "retained-compose-run",
                seed: 373,
                sandbox: &retained,
                scenarios: &[scenario::TEST_PASSING.to_string()],
                retention: RetentionPolicy::Always,
                selected_platform: "windows-winfsp",
                infra: InfraMode::Compose,
                compose_project: Some("studiofs-e2e-retained-compose-run"),
                allocated_ports: Vec::new(),
                studiofs_bin: None,
                studiofs_bin_fingerprint: None,
                port_range: None,
                timeout: None,
                created_at: &timestamp(),
            }),
        )
        .unwrap();
        write_pretty_json(
            &retained.join("cleanup.json"),
            &CleanupReport {
                status: RunStatus::Passed,
                actions: vec!["docker compose down --volumes --remove-orphans".to_string()],
                errors: Vec::new(),
            },
        )
        .unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: root.clone(),
                run_id: Some("next-compose-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 373,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33420-33430".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
            &temp.path().join("mount-locks"),
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Passed);
        assert!(
            executor
                .calls
                .borrow()
                .iter()
                .any(|call| call.args.contains(&"up".to_string())),
            "completed retained runs must not block a new compose stack"
        );
    }

    #[test]
    fn compose_preflight_reports_malformed_cleanup_json_and_continues_scanning() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let malformed = root.join("malformed-cleanup-run");
        fs::create_dir_all(malformed.join("artifacts")).unwrap();
        write_pretty_json(
            &malformed.join(RUNNER_MARKER_FILE),
            &serde_json::json!({
                "runner_crate": RUNNER_CRATE,
                "run_id": "malformed-cleanup-run",
            }),
        )
        .unwrap();
        write_pretty_json(
            &malformed.join("run.json"),
            &build_manifest(ManifestInput {
                run_id: "malformed-cleanup-run",
                seed: 373,
                sandbox: &malformed,
                scenarios: &[scenario::TEST_PASSING.to_string()],
                retention: RetentionPolicy::Always,
                selected_platform: "windows-winfsp",
                infra: InfraMode::Compose,
                compose_project: Some("studiofs-e2e-malformed-cleanup-run"),
                allocated_ports: Vec::new(),
                studiofs_bin: None,
                studiofs_bin_fingerprint: None,
                port_range: None,
                timeout: None,
                created_at: &timestamp(),
            }),
        )
        .unwrap();
        fs::write(malformed.join("cleanup.json"), "{").unwrap();

        let stale = root.join("stale-compose-run");
        fs::create_dir_all(stale.join("artifacts")).unwrap();
        write_pretty_json(
            &stale.join(RUNNER_MARKER_FILE),
            &serde_json::json!({
                "runner_crate": RUNNER_CRATE,
                "run_id": "stale-compose-run",
            }),
        )
        .unwrap();
        write_pretty_json(
            &stale.join("run.json"),
            &build_manifest(ManifestInput {
                run_id: "stale-compose-run",
                seed: 373,
                sandbox: &stale,
                scenarios: &[scenario::TEST_PASSING.to_string()],
                retention: RetentionPolicy::Always,
                selected_platform: "windows-winfsp",
                infra: InfraMode::Compose,
                compose_project: Some("studiofs-e2e-stale-compose-run"),
                allocated_ports: Vec::new(),
                studiofs_bin: None,
                studiofs_bin_fingerprint: None,
                port_range: None,
                timeout: None,
                created_at: &timestamp(),
            }),
        )
        .unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: root.clone(),
                run_id: Some("next-compose-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 373,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33440-33450".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
            &temp.path().join("mount-locks"),
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::PreflightFailed);
        let report = read_json_file::<RunReport>(&outcome.report_path).unwrap();
        let stale_check = report
            .preflight
            .checks
            .iter()
            .find(|check| check.name == "stale runner-owned E2E infrastructure")
            .expect("preflight should include stale infra diagnostic");
        assert!(stale_check.detail.contains("malformed-cleanup-run"));
        assert!(stale_check.detail.contains("report-error"));
        assert!(stale_check.detail.contains("stale-compose-run"));
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
                "pid": DEFINITELY_DEAD_TEST_PID,
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
    fn clean_reclaims_stale_default_mount_lock_when_holder_pid_is_not_running() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let lock_dir = temp.path().join("locks");
        std::fs::create_dir_all(&lock_dir).unwrap();
        let lock_path = lock_dir.join("default-windows-mount-points.lock");
        std::fs::write(
            &lock_path,
            serde_json::json!({
                "runner": RUNNER_CRATE,
                "scope": "default-windows-mount-points",
                "run_id": "stale-run",
                "pid": DEFINITELY_DEAD_TEST_PID,
                "created_at": timestamp(),
            })
            .to_string(),
        )
        .unwrap();

        let outcome = clean_with_executor_and_mount_lock_dir(
            CleanRequest { root },
            &RecordingExecutor::default(),
            &lock_dir,
            |_| ProcessStatus::NotRunning,
        )
        .unwrap();

        assert!(outcome.removed.is_empty());
        assert!(outcome.skipped.is_empty());
        assert!(!lock_path.exists());
    }

    #[test]
    fn clean_reclaims_legacy_default_mount_lock_without_run_id_when_holder_pid_is_not_running() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let lock_dir = temp.path().join("locks");
        std::fs::create_dir_all(&lock_dir).unwrap();
        let lock_path = lock_dir.join("default-windows-mount-points.lock");
        std::fs::write(
            &lock_path,
            serde_json::json!({
                "runner": RUNNER_CRATE,
                "scope": "default-windows-mount-points",
                "pid": DEFINITELY_DEAD_TEST_PID,
                "created_at": timestamp(),
            })
            .to_string(),
        )
        .unwrap();

        let outcome = clean_with_executor_and_mount_lock_dir(
            CleanRequest { root },
            &RecordingExecutor::default(),
            &lock_dir,
            |_| ProcessStatus::NotRunning,
        )
        .unwrap();

        assert!(outcome.removed.is_empty());
        assert!(outcome.skipped.is_empty());
        assert!(!lock_path.exists());
    }

    #[test]
    fn clean_keeps_dead_pid_default_mount_lock_when_recorded_run_still_exists() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        std::fs::create_dir_all(root.join("active-run")).unwrap();
        let lock_dir = temp.path().join("locks");
        std::fs::create_dir_all(&lock_dir).unwrap();
        let lock_path = lock_dir.join("default-windows-mount-points.lock");
        std::fs::write(
            &lock_path,
            serde_json::json!({
                "runner": RUNNER_CRATE,
                "scope": "default-windows-mount-points",
                "run_id": "active-run",
                "pid": DEFINITELY_DEAD_TEST_PID,
                "created_at": timestamp(),
            })
            .to_string(),
        )
        .unwrap();

        let outcome = clean_with_executor_and_mount_lock_dir(
            CleanRequest { root },
            &RecordingExecutor::default(),
            &lock_dir,
            |_| ProcessStatus::NotRunning,
        )
        .unwrap();

        assert!(outcome.removed.is_empty());
        assert!(outcome.skipped.is_empty());
        assert!(lock_path.exists());
    }

    #[test]
    fn clean_keeps_dead_pid_default_mount_lock_when_recorded_run_id_is_unsafe() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("e2e-runs");
        let lock_dir = temp.path().join("locks");
        std::fs::create_dir_all(&lock_dir).unwrap();
        let lock_path = lock_dir.join("default-windows-mount-points.lock");
        std::fs::write(
            &lock_path,
            serde_json::json!({
                "runner": RUNNER_CRATE,
                "scope": "default-windows-mount-points",
                "run_id": "../outside-run",
                "pid": DEFINITELY_DEAD_TEST_PID,
                "created_at": timestamp(),
            })
            .to_string(),
        )
        .unwrap();

        let outcome = clean_with_executor_and_mount_lock_dir(
            CleanRequest { root },
            &RecordingExecutor::default(),
            &lock_dir,
            |_| ProcessStatus::NotRunning,
        )
        .unwrap();

        assert!(outcome.removed.is_empty());
        assert!(outcome.skipped.is_empty());
        assert!(lock_path.exists());
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
        let check = report["preflight"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "default Windows mounted E2E run lock")
            .expect("mount lock preflight check");
        assert_eq!(check["name"], "default Windows mounted E2E run lock");
        assert_eq!(check["status"], "preflight_failed");
        assert!(check["detail"].as_str().unwrap().contains("first-run"));
        assert!(check["detail"].as_str().unwrap().contains("second-run"));
        assert!(outcome.sandbox.join("events.ndjson").exists());
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

    #[test]
    fn multi_scenario_run_supervises_each_scenario_in_child_process() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("supervised-run".to_string()),
                scenarios: vec![
                    scenario::TEST_PASSING.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 371,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
            &temp.path().join("locks"),
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Passed);
        let calls = executor.calls.borrow();
        let child_runs = calls
            .iter()
            .filter(|call| call.args.iter().any(|arg| arg == "__run-one"))
            .collect::<Vec<_>>();
        assert_eq!(child_runs.len(), 2);
        assert!(child_runs.iter().all(|call| {
            call.timeout
                .is_some_and(|timeout| timeout <= Duration::from_secs(30))
        }));
        assert!(child_runs.iter().all(|call| {
            call.stdout_path
                .as_ref()
                .is_some_and(|path| path.ends_with("child.stdout.log"))
                && call
                    .stderr_path
                    .as_ref()
                    .is_some_and(|path| path.ends_with("child.stderr.log"))
        }));
        assert_eq!(executor.run_with_heartbeat_calls.get(), 2);
    }

    #[test]
    fn timed_out_supervised_child_marks_run_timed_out_and_skips_later_scenarios() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor::default();
        executor.timeout_next_run_one.set(true);

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("supervised-timeout-run".to_string()),
                scenarios: vec![
                    scenario::TEST_HANGING_RUN.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 371,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
            &temp.path().join("locks"),
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::TimedOut);
        assert_eq!(outcome.exit_code, 1);
        assert_eq!(outcome.scenarios.len(), 1);
        assert_eq!(outcome.scenarios[0].status, ScenarioStatus::TimedOut);
        let report = read_json_file::<RunReport>(&outcome.report_path).unwrap();
        assert_eq!(report.status, RunStatus::TimedOut);
        assert_eq!(report.scenarios[0].status, ScenarioStatus::TimedOut);
        let child_runs = executor
            .calls
            .borrow()
            .iter()
            .filter(|call| call.args.iter().any(|arg| arg == "__run-one"))
            .count();
        assert_eq!(child_runs, 1);
    }

    #[test]
    fn elapsed_deadline_times_out_before_spawning_supervised_child() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor::default();
        let run_id = "elapsed-deadline-run";
        let root = temp.path().join("e2e-runs");
        let sandbox = root.join(run_id);
        fs::create_dir_all(&sandbox).unwrap();
        write_pretty_json(
            &sandbox.join("run.json"),
            &build_manifest(ManifestInput {
                run_id,
                seed: 371,
                sandbox: &sandbox,
                scenarios: &[scenario::TEST_HANGING_RUN.to_string()],
                retention: RetentionPolicy::Always,
                selected_platform: "windows-winfsp",
                infra: InfraMode::Disabled,
                compose_project: None,
                allocated_ports: Vec::new(),
                studiofs_bin: None,
                studiofs_bin_fingerprint: None,
                port_range: None,
                timeout: Some("1ms"),
                created_at: &timestamp(),
            }),
        )
        .unwrap();
        let definition = scenario::resolve_scenario(scenario::TEST_HANGING_RUN)
            .unwrap()
            .definition();
        let request = RunRequest {
            root,
            run_id: Some(run_id.to_string()),
            scenarios: vec![scenario::TEST_HANGING_RUN.to_string()],
            seed: 371,
            studiofs_bin: None,
            platform: PlatformSelection::Auto,
            retention: RetentionPolicy::Always,
            report: None,
            port_range: None,
            timeout: Some("1ms".to_string()),
            infra: InfraMode::Disabled,
        };
        let deadline = RunDeadline::new(
            Duration::from_millis(1),
            Instant::now()
                .checked_sub(Duration::from_secs(1))
                .expect("test instant before now"),
        );

        let events_path = sandbox.join("events.ndjson");
        let lifecycle = run_scenario_child_with_timeout(ScenarioChildRun {
            sandbox: &sandbox,
            events_path: &events_path,
            run_id,
            definition,
            scenario_index: 1,
            scenario_total: 1,
            executor: &executor,
            request: &request,
            deadline: Some(deadline),
        })
        .unwrap();

        assert_eq!(lifecycle.outcome.status, ScenarioStatus::TimedOut);
        assert!(executor.calls.borrow().is_empty());
        assert!(
            lifecycle
                .report
                .error
                .as_deref()
                .is_some_and(|error| error.contains("already elapsed"))
        );
    }

    #[test]
    fn abnormal_supervised_child_without_report_is_marked_failed() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor::default();
        executor.fail_next_run_one_without_report.set(true);

        let outcome = run_with_executor_and_mount_lock_dir(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("supervised-abnormal-run".to_string()),
                scenarios: vec![
                    scenario::TEST_FAILING_VALIDATE.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 371,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
            &temp.path().join("locks"),
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        assert_eq!(outcome.scenarios.len(), 2);
        let report = read_json_file::<RunReport>(&outcome.report_path).unwrap();
        assert_eq!(report.scenarios[0].status, ScenarioStatus::Failed);
        assert!(
            report.scenarios[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("terminated abnormally"))
        );
    }

    #[test]
    fn successful_supervised_child_without_report_returns_structured_error() {
        let temp = tempfile::tempdir().unwrap();
        let executor = RecordingExecutor::default();
        executor.skip_next_run_one_report.set(true);
        let run_id = "missing-report-run";
        let root = temp.path().join("e2e-runs");
        let sandbox = root.join(run_id);
        fs::create_dir_all(&sandbox).unwrap();
        write_pretty_json(
            &sandbox.join("run.json"),
            &build_manifest(ManifestInput {
                run_id,
                seed: 371,
                sandbox: &sandbox,
                scenarios: &[scenario::TEST_PASSING.to_string()],
                retention: RetentionPolicy::Always,
                selected_platform: "windows-winfsp",
                infra: InfraMode::Disabled,
                compose_project: None,
                allocated_ports: Vec::new(),
                studiofs_bin: None,
                studiofs_bin_fingerprint: None,
                port_range: None,
                timeout: Some("30s"),
                created_at: &timestamp(),
            }),
        )
        .unwrap();
        let definition = scenario::resolve_scenario(scenario::TEST_PASSING)
            .unwrap()
            .definition();
        let request = RunRequest {
            root,
            run_id: Some(run_id.to_string()),
            scenarios: vec![scenario::TEST_PASSING.to_string()],
            seed: 371,
            studiofs_bin: None,
            platform: PlatformSelection::Auto,
            retention: RetentionPolicy::Always,
            report: None,
            port_range: None,
            timeout: Some("30s".to_string()),
            infra: InfraMode::Disabled,
        };

        let events_path = sandbox.join("events.ndjson");
        let err = run_scenario_child_with_timeout(ScenarioChildRun {
            sandbox: &sandbox,
            events_path: &events_path,
            run_id,
            definition,
            scenario_index: 1,
            scenario_total: 1,
            executor: &executor,
            request: &request,
            deadline: Some(RunDeadline::new(Duration::from_secs(30), Instant::now())),
        })
        .unwrap_err();

        assert!(matches!(
            err,
            RunnerError::MissingScenarioReport { path }
                if path.ends_with("scenario-report.json")
        ));
    }

    #[derive(Default)]
    struct RecordingExecutor {
        calls: RefCell<Vec<crate::infra::CommandSpec>>,
        run_with_heartbeat_calls: Cell<usize>,
        poison_events_path_after_up: RefCell<Option<PathBuf>>,
        require_report_before_compose_cleanup: RefCell<Option<PathBuf>>,
        require_missing_lock_before_compose_cleanup: RefCell<Option<PathBuf>>,
        timeout_next_run_one: Cell<bool>,
        io_error_next_run_one: Cell<bool>,
        fail_next_run_one_without_report: Cell<bool>,
        skip_next_run_one_report: Cell<bool>,
    }

    impl CommandExecutor for RecordingExecutor {
        fn run_with_heartbeat(
            &self,
            command: &crate::infra::CommandSpec,
            heartbeat_interval: Duration,
            heartbeat: &mut dyn FnMut(),
        ) -> std::io::Result<CommandOutput> {
            let _ = heartbeat_interval;
            let _ = heartbeat;
            if command.args.iter().any(|arg| arg == "__run-one") {
                self.run_with_heartbeat_calls
                    .set(self.run_with_heartbeat_calls.get() + 1);
            }
            self.run(command)
        }

        fn run(&self, command: &crate::infra::CommandSpec) -> std::io::Result<CommandOutput> {
            self.calls.borrow_mut().push(command.clone());
            if command.args.iter().any(|arg| arg == "__run-one") {
                if self.io_error_next_run_one.replace(false) {
                    return Err(std::io::Error::other("run-one spawn failed"));
                }
                if self.timeout_next_run_one.replace(false) {
                    return Ok(CommandOutput {
                        status: -1,
                        stdout: String::new(),
                        stderr: "command timed out after 30000ms".to_string(),
                    });
                }
                if self.fail_next_run_one_without_report.replace(false) {
                    return Ok(CommandOutput {
                        status: 101,
                        stdout: String::new(),
                        stderr: String::new(),
                    });
                }
                if self.skip_next_run_one_report.replace(false) {
                    return Ok(CommandOutput {
                        status: 0,
                        stdout: String::new(),
                        stderr: String::new(),
                    });
                }
                let scenario_name = command
                    .args
                    .iter()
                    .position(|arg| arg == "__run-one")
                    .and_then(|index| command.args.get(index + 1))
                    .expect("run-one scenario argument")
                    .clone();
                let root = command_arg_value(command, "--root");
                let run_id = command_arg_value(command, "--run-id");
                let seed = command_arg_value(command, "--seed").parse::<u64>().unwrap();
                let scenario_dir = PathBuf::from(root)
                    .join(&run_id)
                    .join("artifacts")
                    .join("scenarios")
                    .join(&scenario_name);
                std::fs::create_dir_all(&scenario_dir)?;
                write_scenario_report(
                    &scenario_dir,
                    &ScenarioReport {
                        name: scenario_name,
                        seed,
                        status: ScenarioStatus::Passed,
                        started_at: timestamp(),
                        finished_at: timestamp(),
                        duration_ms: 0,
                        artifacts: vec![
                            "artifacts/scenarios/__test-passing/operations.ndjson".to_string(),
                        ],
                        error: None,
                    },
                )
                .map_err(std::io::Error::other)?;
            }
            if command
                .args
                .iter()
                .any(|arg| arg == "logs" || arg == "down")
            {
                if let Some(report_path) =
                    self.require_report_before_compose_cleanup.borrow().as_ref()
                {
                    assert!(
                        report_path.exists(),
                        "report.json must exist before compose cleanup starts"
                    );
                }
                if let Some(lock_path) = self
                    .require_missing_lock_before_compose_cleanup
                    .borrow()
                    .as_ref()
                {
                    assert!(
                        !lock_path.exists(),
                        "default mount lock must be released before compose cleanup starts"
                    );
                }
            }
            if command.args.contains(&"up".to_string()) {
                if let Some(events_path) = self.poison_events_path_after_up.borrow_mut().take() {
                    if events_path.is_file() {
                        std::fs::remove_file(&events_path)?;
                    }
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

    fn command_arg_value(command: &crate::infra::CommandSpec, flag: &str) -> String {
        command
            .args
            .iter()
            .position(|arg| arg == flag)
            .and_then(|index| command.args.get(index + 1))
            .unwrap_or_else(|| panic!("missing {flag}"))
            .clone()
    }
}

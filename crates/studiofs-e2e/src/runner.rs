//! Runner orchestration, sandbox creation, reports, and cleanup.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{IsTerminal as _, Write as _};
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
    ClientControllerConfig, ClientControllerError, DEFAULT_CLIENT_NAMES,
    cleanup_stale_mount_before_remount, client_cache_dir, client_mount_point, client_root_dir,
};
use crate::infra::{
    COMPOSE_PROJECT_PREFIX, CommandExecutor, CommandSpec, ComposeConfig, ComposeServiceControl,
    DockerComposeStack, InfraCheck, InfraError, InfraMode, SystemCommandExecutor,
};
#[cfg(windows)]
use crate::process_diagnostics::capture_process_diagnostics_best_effort;
use crate::process_diagnostics::set_process_diagnostics_dir;
#[cfg(windows)]
use crate::process_job::{CHILD_TERMINATION_GRACE, ChildJob, wait_for_child_exit};
use crate::report::{
    ArtifactReport, CleanupReport, ComposeImageReport, EnvironmentReport, EventRecord, HostReport,
    PathOwnershipReport, PreflightCheckReport, PreflightReport, RunManifest, RunReport,
    RunnerReport, SandboxReport, ScenarioReport,
};
use crate::runner_lifecycle::{
    ScenarioLifecycleOutcome, SupervisedScenarioInput, failed_scenario_child,
    run_supervised_scenario,
};
use crate::scenario::{self, ScenarioContext};

const SCHEMA_VERSION: u32 = 1;
const RUNNER_CRATE: &str = "studiofs-e2e";
const RUNNER_MARKER_FILE: &str = ".studiofs-e2e-runner.json";
const PROGRESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(windows)]
const HOST_ENVIRONMENT_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const ANSI_RESET: &str = "\x1b[0m";

/// Controls when the runner's output artifacts and working directories are retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetentionPolicy {
    /// Keep all run artifacts and sandbox data.
    Always,
    /// Keep artifacts only for failing runs.
    OnFailure,
    /// Remove artifacts and sandbox data after the run ends.
    Never,
}

/// Selects the target platform/backend used by the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlatformSelection {
    /// Select the platform automatically for the current host.
    Auto,
    /// Force Linux FUSE execution.
    LinuxFuse,
    /// Force macOS FUSE execution.
    MacosFuse,
    /// Force WinFSP execution.
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

    pub(crate) fn as_cli_value(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::LinuxFuse => "linux-fuse",
            Self::MacosFuse => "macos-fuse",
            Self::WindowsWinfsp => "windows-winfsp",
        }
    }
}

/// Final status for a full runner execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// The runner completed all scenarios and cleanup successfully.
    Passed,
    /// One or more scenarios failed.
    Failed,
    /// The run was intentionally skipped.
    Skipped,
    /// Preflight checks failed before scenario execution.
    PreflightFailed,
    /// Cleanup phase did not complete successfully.
    CleanupFailed,
    /// The run timed out.
    TimedOut,
    /// The run was interrupted by the user or environment.
    Interrupted,
}

/// Status reported for a single scenario in a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioStatus {
    /// The scenario completed successfully.
    Passed,
    /// The scenario failed.
    Failed,
    /// The scenario was skipped.
    Skipped,
    /// The scenario timed out.
    TimedOut,
}

/// Parameters for running one or more scenarios from the CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRequest {
    /// Root directory for all runner sandboxes and artifacts.
    pub root: PathBuf,
    /// Optional explicit run identifier; generated automatically when omitted.
    pub run_id: Option<String>,
    /// Scenario names to run in order.
    pub scenarios: Vec<String>,
    /// Seed used for deterministic execution.
    pub seed: u64,
    /// Optional path to the studiofs binary (required for compose-mounted scenarios).
    pub studiofs_bin: Option<PathBuf>,
    /// Platform backend to use for this run.
    pub platform: PlatformSelection,
    /// Artifact retention policy for this run.
    pub retention: RetentionPolicy,
    /// Optional path to save a copy of the final report.
    pub report: Option<PathBuf>,
    /// Optional compose port range.
    pub port_range: Option<String>,
    /// Optional timeout value passed to scenarios.
    pub timeout: Option<String>,
    /// Infrastructure mode to execute with.
    pub infra: InfraMode,
}

/// Parameters for running a single scenario independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOneRequest {
    /// Root directory for scenario artifacts.
    pub root: PathBuf,
    /// Explicit run identifier for the one-shot run.
    pub run_id: String,
    /// Name of the scenario to execute.
    pub scenario: String,
}

/// Result data for a one-shot scenario run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOneOutcome {
    /// Identifier of the run that produced this outcome.
    pub run_id: String,
    /// Final outcome of the scenario.
    pub scenario: ScenarioOutcome,
    /// Path to the generated report.
    pub report_path: PathBuf,
    /// Exit code returned by the runner.
    pub exit_code: i32,
}

/// Parameters for cleaning stale runner sandboxes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanRequest {
    /// Root directory to scan for stale runs.
    pub root: PathBuf,
}

/// Result of a cleanup operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanOutcome {
    /// Paths successfully removed.
    pub removed: Vec<PathBuf>,
    /// Paths that were not removed and the associated error.
    pub skipped: Vec<CleanSkipped>,
}

/// A skipped cleanup path and the reason for skip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanSkipped {
    /// Path that was skipped.
    pub path: PathBuf,
    /// Error describing why cleanup skipped this path.
    pub error: String,
}

/// Outcome summary for a single scenario execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioOutcome {
    /// Scenario name.
    pub name: String,
    /// Final status for this scenario.
    pub status: ScenarioStatus,
    /// Relative artifact paths collected for this scenario.
    pub artifacts: Vec<String>,
}

/// Final outcome of a full run including metadata and per-scenario results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    /// Identifier of the run.
    pub run_id: String,
    /// Final status of the run.
    pub status: RunStatus,
    /// Shell exit code selected for this run.
    pub exit_code: i32,
    /// Sandbox path used by this run.
    pub sandbox: PathBuf,
    /// Path to the generated report.
    pub report_path: PathBuf,
    /// Scenario-level outcomes.
    pub scenarios: Vec<ScenarioOutcome>,
}

/// Runner failures surfaced from command execution, validation, and orchestration.
#[derive(Debug, Error)]
pub enum RunnerError {
    /// A requested scenario name was not recognized.
    #[error("unknown E2E scenario: {0}")]
    UnknownScenario(
        /// Requested scenario name.
        String,
    ),
    /// A compose-mode run requires a studiofs binary for mounted scenarios.
    #[error("E2E scenario {scenario} requires --studiofs-bin when --infra compose is used")]
    MissingStudiofsBin {
        /// Mounted scenario that requires a local studiofs binary.
        scenario: String,
    },
    /// Timeout string could not be parsed.
    #[error(
        "invalid E2E timeout '{value}': expected integer seconds or a value ending in ms, s, or m"
    )]
    InvalidTimeout {
        /// Invalid timeout value from the request.
        value: String,
    },
    /// Run ID was empty.
    #[error("run ID must not be empty")]
    EmptyRunId,
    /// Run ID contains characters outside a safe single-directory name.
    #[error("run ID must be a safe single directory name: {0}")]
    UnsafeRunId(
        /// Run ID value rejected by the path safety check.
        String,
    ),
    /// Cleanup root is outside the configured root directory.
    #[error("unsafe cleanup root: {0}")]
    UnsafeCleanupRoot(
        /// Normalized cleanup root path that failed validation.
        String,
    ),
    /// A report artifact path is outside the run sandbox.
    #[error("E2E artifact path is outside the run sandbox")]
    ArtifactPathOutsideSandbox {
        /// Path reported as an artifact.
        path: PathBuf,
        /// Sandbox root against which the artifact path was validated.
        sandbox: PathBuf,
    },
    /// Artifact path contains non-UTF-8 components.
    #[error("E2E artifact path contains a non-UTF-8 component")]
    NonUtf8ArtifactPath {
        /// Artifact path that cannot be represented as UTF-8.
        path: PathBuf,
    },
    /// Artifact path is relative/unsafe for artifact recording.
    #[error("E2E artifact path must be relative and must not contain parent components")]
    UnsafeArtifactPath {
        /// Relative artifact path rejected for safety.
        path: PathBuf,
    },
    /// I/O failed while running or producing reports.
    #[error("runner I/O failed: {0}")]
    Io(
        /// I/O-level failure while producing artifacts or executing runner tasks.
        #[from]
        std::io::Error,
    ),
    /// JSON (de)serialization failed.
    #[error("runner JSON failed: {0}")]
    Json(
        /// JSON serialization or parsing error for run/scenario reports.
        #[from]
        serde_json::Error,
    ),
    /// A scenario hook returned an error.
    #[error("scenario hook failed: {0}")]
    Scenario(
        /// Low-level scenario lifecycle error.
        #[from]
        scenario::ScenarioError,
    ),
    /// The client controller failed.
    #[error("runner client controller failed: {0}")]
    Client(
        /// Failure returned by the client controller integration layer.
        #[from]
        ClientControllerError,
    ),
    /// The infrastructure layer failed.
    #[error("runner infra failed: {0}")]
    Infra(
        /// Failure returned by the infra layer (compose setup, mount checks, etc.).
        #[from]
        InfraError,
    ),
}

struct ResolvedScenario {
    definition: scenario::ScenarioDefinition,
    scenario: Box<dyn scenario::Scenario>,
}

/// Run a full scenario set using the default command executor.
pub fn run(request: RunRequest) -> Result<RunOutcome, RunnerError> {
    let executor = SystemCommandExecutor;
    run_with_executor(request, &executor)
}

/// Run one or more scenarios using a custom command executor.
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
    if request.infra != InfraMode::Disabled
        && request.studiofs_bin.is_none()
        && let Some(definition) = scenarios
            .iter()
            .map(|resolved| resolved.definition)
            .find(|definition| definition.requires_studiofs_bin)
    {
        return Err(RunnerError::MissingStudiofsBin {
            scenario: definition.name.to_string(),
        });
    }
    let sandbox = root.join(&run_id);
    let selected_platform = request.platform.resolved().to_string();
    let infra_mode = request.infra;
    let compose_stack = match infra_mode {
        InfraMode::Disabled => None,
        InfraMode::Env => None,
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
    set_process_diagnostics_dir(sandbox.join("artifacts").join("process-diagnostics"))?;
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
        infra_mode,
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
        let cleanup =
            run_runner_cleanup(&run_id, &sandbox, &mut compose_cleanup_guard, Vec::new())?;
        return finish_runner_early_failure(EarlyRunnerFailureInput {
            run_id: &run_id,
            seed: request.seed,
            status: RunStatus::PreflightFailed,
            exit_code: 2,
            started_at: &started_at,
            duration_ms: duration_millis(started_instant),
            selected_platform: &selected_platform,
            sandbox: &sandbox,
            retention: request.retention,
            report_copy: request.report,
            scenarios: &scenario_names,
            studiofs_bin: request.studiofs_bin.as_deref(),
            compose_stack: compose_stack.as_ref(),
            compose_project: compose_project.as_deref(),
            allocated_ports: allocated_ports.clone(),
            preflight,
            cleanup,
        });
    }
    write_pretty_json(&sandbox.join("preflight.json"), &preflight)?;

    let events_path = sandbox.join("events.ndjson");
    if let Err(primary_error) = record_event(&events_path, &run_id, None, "run", "started") {
        let primary_error_detail = primary_error.to_string();
        let mut cleanup_errors = Vec::new();
        if let Err(err) = write_runner_failure_boundary(
            &sandbox,
            "post-preflight",
            Some(&events_path),
            &primary_error_detail,
        ) {
            cleanup_errors.push(format!("failure-boundary report: {err}"));
        }
        let cleanup = match run_runner_cleanup(
            &run_id,
            &sandbox,
            &mut compose_cleanup_guard,
            cleanup_errors,
        ) {
            Ok(cleanup) => cleanup,
            Err(err) => CleanupReport {
                status: RunStatus::CleanupFailed,
                actions: Vec::new(),
                errors: vec![format!("cleanup report failed during early failure: {err}")],
            },
        };
        let _ = finish_runner_early_failure(EarlyRunnerFailureInput {
            run_id: &run_id,
            seed: request.seed,
            status: RunStatus::Failed,
            exit_code: 1,
            started_at: &started_at,
            duration_ms: duration_millis(started_instant),
            selected_platform: &selected_platform,
            sandbox: &sandbox,
            retention: request.retention,
            report_copy: request.report,
            scenarios: &scenario_names,
            studiofs_bin: request.studiofs_bin.as_deref(),
            compose_stack: compose_stack.as_ref(),
            compose_project: compose_project.as_deref(),
            allocated_ports: allocated_ports.clone(),
            preflight,
            cleanup,
        });
        return Err(primary_error);
    }

    let mut scenario_reports = Vec::new();
    let mut scenario_outcomes = Vec::new();
    let mut cleanup_errors = Vec::new();
    let scenario_total = scenarios.len();
    let supervise_scenarios = scenario_total > 1;
    let mut mounted_cleanup_block: Option<String> = None;
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
        if definition.requires_studiofs_bin
            && let Some(block_reason) = &mounted_cleanup_block
        {
            let lifecycle = skipped_scenario_lifecycle(
                definition,
                request.seed,
                format!(
                    "skipped because contaminated runner state remains after an earlier mounted scenario: {block_reason}"
                ),
            );
            emit_progress(
                &run_id,
                definition.name,
                Some(scenario_progress),
                "scenario",
                ProgressStatus::Failed,
            );
            scenario_reports.push(lifecycle.report);
            scenario_outcomes.push(lifecycle.outcome);
            continue;
        }
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
                (InfraMode::Env, Some(studiofs_bin)) => {
                    Some(ClientControllerConfig::from_external_env_for_scenario(
                        studiofs_bin.clone(),
                        sandbox.clone(),
                        &run_id,
                        request.seed,
                        definition.name,
                    )?)
                }
                _ => None,
            },
        };

        let lifecycle_result = if supervise_scenarios {
            run_supervised_scenario(SupervisedScenarioInput {
                events_path: &events_path,
                run_id: &run_id,
                root: &root,
                sandbox: &sandbox,
                request_seed: request.seed,
                request_platform: request.platform,
                request_infra: infra_mode,
                request_timeout: request.timeout.as_deref(),
                definition,
                scenario_progress,
                executor,
            })
        } else {
            run_scenario_lifecycle(
                &events_path,
                &run_id,
                resolved.scenario.as_ref(),
                &context,
                scenario_progress,
            )
        };
        let lifecycle = match lifecycle_result {
            Ok(lifecycle) => lifecycle,
            Err(error) if definition.requires_studiofs_bin => failed_scenario_child(
                definition.name,
                request.seed,
                format!("scenario supervision failed: {error}"),
                None,
                false,
            ),
            Err(error) => return Err(error),
        };
        let mut lifecycle = lifecycle;
        if definition.requires_studiofs_bin && lifecycle.outcome.status != ScenarioStatus::Passed {
            let detail = lifecycle
                .report
                .error
                .clone()
                .unwrap_or_else(|| "mounted scenario failed".to_string());
            mounted_cleanup_block = Some(detail);
        }
        if let Some(stack) = compose_stack.as_ref() {
            collect_post_scenario_postgres_state_artifact(
                stack,
                executor,
                &sandbox,
                &mut lifecycle,
            )?;
        }
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
    let cleanup = run_runner_cleanup(
        &run_id,
        &sandbox,
        &mut compose_cleanup_guard,
        cleanup_errors,
    )?;

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

fn collect_post_scenario_postgres_state_artifact(
    stack: &DockerComposeStack,
    executor: &dyn CommandExecutor,
    sandbox: &Path,
    lifecycle: &mut ScenarioLifecycleOutcome,
) -> Result<(), RunnerError> {
    let status = format!("{:?}", lifecycle.outcome.status).to_ascii_lowercase();
    let artifact =
        stack.collect_post_scenario_postgres_state(executor, &lifecycle.outcome.name, &status)?;
    let relative = normalize_relative_path(&artifact, sandbox)?;
    if !lifecycle.report.artifacts.contains(&relative) {
        lifecycle.report.artifacts.push(relative.clone());
    }
    if !lifecycle.outcome.artifacts.contains(&relative) {
        lifecycle.outcome.artifacts.push(relative);
    }
    Ok(())
}

fn skipped_scenario_lifecycle(
    definition: scenario::ScenarioDefinition,
    seed: u64,
    reason: String,
) -> ScenarioLifecycleOutcome {
    let started_at = timestamp();
    let finished_at = timestamp();
    ScenarioLifecycleOutcome {
        report: ScenarioReport {
            name: definition.name.to_string(),
            seed,
            status: ScenarioStatus::Skipped,
            started_at,
            finished_at,
            duration_ms: 0,
            artifacts: Vec::new(),
            error: Some(reason),
        },
        outcome: ScenarioOutcome {
            name: definition.name.to_string(),
            status: ScenarioStatus::Skipped,
            artifacts: Vec::new(),
        },
        cleanup_error: None,
    }
}

pub(crate) fn colorize_progress_statuses(output: &str) -> String {
    output
        .lines()
        .map(colorize_progress_status_line)
        .collect::<Vec<_>>()
        .join("\n")
        + if output.ends_with('\n') { "\n" } else { "" }
}

fn colorize_progress_status_line(line: &str) -> String {
    for status in [
        ProgressStatus::Started,
        ProgressStatus::Passed,
        ProgressStatus::Failed,
    ] {
        let needle = format!("status={}", status.as_str());
        if line.contains(&needle) {
            return line.replace(
                &needle,
                &format!(
                    "status={}{}{}",
                    status.ansi_color(),
                    status.as_str(),
                    ANSI_RESET
                ),
            );
        }
    }
    line.to_string()
}

/// Run a single scenario from an existing run directory.
///
/// Use `run_id` to point to a previous run created by [`run`], and execute
/// the same scenario workflow in a one-off mode for inspection.
pub fn run_one(request: RunOneRequest) -> Result<RunOneOutcome, RunnerError> {
    let root = absolutize(&request.root)?;
    validate_run_id(&request.run_id)?;
    let sandbox = root.join(&request.run_id);
    let manifest = read_run_manifest(&sandbox.join("run.json")).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "missing parent run manifest at {}",
                sandbox.join("run.json").display()
            ),
        )
    })?;
    let scenario = scenario::resolve_scenario(&request.scenario)
        .ok_or_else(|| RunnerError::UnknownScenario(request.scenario.clone()))?;
    let definition = scenario.definition();
    let scenario_index = manifest
        .scenarios
        .iter()
        .position(|name| name == definition.name)
        .map_or(1, |index| index + 1);
    let context = ScenarioContext {
        run_id: request.run_id.clone(),
        seed: manifest.seed,
        sandbox: sandbox.clone(),
        scenario_dir: sandbox
            .join("artifacts")
            .join("scenarios")
            .join(definition.name),
        client_controller_config: match (manifest.infra, manifest.studiofs_bin.as_ref()) {
            (InfraMode::Compose, Some(studiofs_bin)) => {
                Some(ClientControllerConfig::from_compose_services_for_scenario(
                    PathBuf::from(studiofs_bin),
                    sandbox.clone(),
                    &request.run_id,
                    manifest.seed,
                    definition.name,
                    &manifest.allocated_ports,
                )?)
            }
            (InfraMode::Env, Some(studiofs_bin)) => {
                Some(ClientControllerConfig::from_external_env_for_scenario(
                    PathBuf::from(studiofs_bin),
                    sandbox.clone(),
                    &request.run_id,
                    manifest.seed,
                    definition.name,
                )?)
            }
            _ => None,
        },
    };
    set_process_diagnostics_dir(context.scenario_dir.join("process-diagnostics"))?;
    let lifecycle = run_scenario_lifecycle(
        &sandbox.join("events.ndjson"),
        &request.run_id,
        scenario.as_ref(),
        &context,
        ScenarioProgress {
            index: scenario_index,
            total: manifest.scenarios.len().max(1),
        },
    )?;
    let child_report = ScenarioChildReport {
        scenario: lifecycle.report,
        cleanup_error: lifecycle.cleanup_error,
    };
    let report_path = context.scenario_dir.join(SCENARIO_CHILD_REPORT_FILE);
    write_pretty_json(&report_path, &child_report)?;
    let exit_code = if child_report.scenario.status == ScenarioStatus::Passed
        && child_report.cleanup_error.is_none()
    {
        0
    } else if child_report.cleanup_error.is_some()
        && child_report.scenario.status == ScenarioStatus::Passed
    {
        4
    } else {
        1
    };
    Ok(RunOneOutcome {
        run_id: request.run_id,
        scenario: ScenarioOutcome {
            name: child_report.scenario.name,
            status: child_report.scenario.status,
            artifacts: child_report.scenario.artifacts,
        },
        report_path,
        exit_code,
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

fn run_runner_cleanup(
    run_id: &str,
    sandbox: &Path,
    compose_cleanup_guard: &mut ComposeCleanupGuard<'_>,
    mut cleanup_errors: Vec<String>,
) -> Result<CleanupReport, RunnerError> {
    let mut cleanup_actions = Vec::new();
    emit_progress(run_id, "runner", None, "cleanup", ProgressStatus::Started);
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
        run_id,
        "runner",
        None,
        "cleanup",
        if cleanup.status == RunStatus::Passed {
            ProgressStatus::Passed
        } else {
            ProgressStatus::Failed
        },
    );
    Ok(cleanup)
}

struct EarlyRunnerFailureInput<'a> {
    run_id: &'a str,
    seed: u64,
    status: RunStatus,
    exit_code: i32,
    started_at: &'a str,
    duration_ms: u64,
    selected_platform: &'a str,
    sandbox: &'a Path,
    retention: RetentionPolicy,
    report_copy: Option<PathBuf>,
    scenarios: &'a [String],
    studiofs_bin: Option<&'a Path>,
    compose_stack: Option<&'a DockerComposeStack>,
    compose_project: Option<&'a str>,
    allocated_ports: Vec<crate::infra::ServicePort>,
    preflight: PreflightReport,
    cleanup: CleanupReport,
}

fn finish_runner_early_failure(
    input: EarlyRunnerFailureInput<'_>,
) -> Result<RunOutcome, RunnerError> {
    finish_run(FinishRunInput {
        run_id: input.run_id.to_string(),
        seed: input.seed,
        status: input.status,
        exit_code: input.exit_code,
        started_at: input.started_at.to_string(),
        duration_ms: input.duration_ms,
        selected_platform: input.selected_platform.to_string(),
        sandbox: input.sandbox.to_path_buf(),
        retention: input.retention,
        report_copy: input.report_copy,
        environment: build_environment_report(EnvironmentInput {
            run_id: input.run_id,
            scenarios: input.scenarios,
            seed: input.seed,
            sandbox: input.sandbox,
            studiofs_bin: input.studiofs_bin,
            compose_stack: input.compose_stack,
            compose_project: input.compose_project,
            allocated_ports: input.allocated_ports,
        }),
        preflight: input.preflight,
        scenario_reports: Vec::new(),
        scenario_outcomes: Vec::new(),
        cleanup: input.cleanup,
    })
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
    let _ = write_runner_adversarial_cleanup_proof(&input);
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

fn write_runner_adversarial_cleanup_proof(input: &FinishRunInput) -> Result<(), RunnerError> {
    let manifest = read_run_manifest(&input.sandbox.join("run.json"));
    let primary_failure_exit_code =
        input.exit_code != 0 && input.exit_code != cleanup_failure_exit_code();
    let cleanup_failed = input.cleanup.status == RunStatus::CleanupFailed;
    write_pretty_json(
        &input
            .sandbox
            .join("artifacts/runner/adversarial-cleanup-proof.json"),
        &serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "issue": "SFS-381",
            "purpose": "adversarial runner cleanup proof",
            "run_id": &input.run_id,
            "status": input.status,
            "exit_code": input.exit_code,
            "cleanup_status": input.cleanup.status,
            "cleanup_errors": &input.cleanup.errors,
            "cleanup_allowlist": manifest
                .as_ref()
                .map(|manifest| manifest.cleanup_allowlist.clone())
                .unwrap_or_default(),
            "compose_project": manifest
                .as_ref()
                .and_then(|manifest| manifest.compose_project.clone()),
            "artifacts": {
                "report_json": true,
                "events_ndjson": input.sandbox.join("events.ndjson").exists(),
                "cleanup_json": input.sandbox.join("cleanup.json").exists(),
                "cleanup_boundary": input
                    .sandbox
                    .join("artifacts/runner/cleanup-boundary.json")
                    .exists(),
                "failure_boundary": input
                    .sandbox
                    .join("artifacts/runner/failure-boundary.json")
                    .exists(),
            },
            "contract": {
                "primary_failure_present": primary_failure_exit_code,
                "primary_failure_exit_code_wins_over_cleanup_failure": !cleanup_failed
                    || !primary_failure_exit_code
                    || input.exit_code != cleanup_failure_exit_code(),
                "cleanup_errors_recorded_separately": !cleanup_failed
                    || !input.cleanup.errors.is_empty(),
                "runner_owned_scope_only": true,
            }
        }),
    )
}

fn cleanup_failure_exit_code() -> i32 {
    4
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
    let probe_host_environment = input.studiofs_bin.is_some() || input.compose_stack.is_some();
    let ownership = if probe_host_environment {
        path_ownership_reports(&client_roots, &mount_roots)
    } else {
        EnvironmentOwnership {
            client_roots: fallback_path_ownership_reports(&client_roots),
            mount_roots: fallback_path_ownership_reports(&mount_roots),
        }
    };
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
        winfsp_version: probe_host_environment.then(detect_winfsp_version).flatten(),
        macfuse_version: probe_host_environment
            .then(detect_macfuse_version)
            .flatten(),
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
    let output = run_host_environment_probe(CommandSpec {
        program: powershell_program(),
        args: vec![
            "-NoProfile".to_string(),
            "-ExecutionPolicy".to_string(),
            "Bypass".to_string(),
            "-Command".to_string(),
            "$paths = @('HKLM:\\SOFTWARE\\WinFsp', 'HKLM:\\SOFTWARE\\WOW6432Node\\WinFsp'); foreach ($path in $paths) { if (Test-Path $path) { $item = Get-ItemProperty -Path $path; foreach ($name in @('Version', 'ProductVersion')) { $value = $item.$name; if ($value) { Write-Output $value; exit 0 } } } }; exit 0".to_string(),
        ],
        env: Vec::new(),
        cwd: None,
        timeout: None,
    })
    .ok()?;
    if output.status != 0 {
        return None;
    }
    output
        .stdout
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
}

#[cfg(not(windows))]
fn detect_winfsp_version_from_host() -> Option<String> {
    None
}

fn detect_macfuse_version() -> Option<String> {
    if let Some(version) = std::env::var("STUDIOFS_E2E_MACFUSE_VERSION")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(version);
    }
    detect_macfuse_version_from_host()
}

#[cfg(target_os = "macos")]
fn detect_macfuse_version_from_host() -> Option<String> {
    const MACFUSE_VERSION_PLIST: &str = "/Library/Filesystems/macfuse.fs/Contents/version.plist";

    let output = std::process::Command::new("/usr/bin/plutil")
        .args([
            "-extract",
            "CFBundleShortVersionString",
            "raw",
            "-o",
            "-",
            MACFUSE_VERSION_PLIST,
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
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

#[cfg(not(target_os = "macos"))]
fn detect_macfuse_version_from_host() -> Option<String> {
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
        Err(err) => return path_ownership_error_reports(paths, err.to_string()),
    };
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/get-path-ownership.ps1");
    let script_arg = script.display().to_string();
    let output = run_host_environment_probe(CommandSpec {
        program: powershell_program(),
        args: vec![
            "-NoProfile".to_string(),
            "-ExecutionPolicy".to_string(),
            "Bypass".to_string(),
            "-File".to_string(),
            script_arg,
        ],
        env: vec![("STUDIOFS_E2E_OWNERSHIP_PATHS_JSON".to_string(), paths_json)],
        cwd: None,
        timeout: None,
    });
    let output = match output {
        Ok(output) => output,
        Err(error) => return path_ownership_error_reports(paths, error),
    };
    if output.status != 0 {
        let error = output.stderr.trim().to_string();
        return path_ownership_error_reports(
            paths,
            if error.is_empty() {
                format!("path ownership probe exited with status {}", output.status)
            } else {
                error
            },
        );
    }
    serde_json::from_str(&output.stdout)
        .unwrap_or_else(|err| path_ownership_error_reports(paths, err.to_string()))
}

#[cfg(windows)]
fn run_host_environment_probe(command: CommandSpec) -> Result<crate::infra::CommandOutput, String> {
    let mut process = std::process::Command::new(&command.program);
    process.args(&command.args);
    process.envs(
        command
            .env
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    process.stdin(std::process::Stdio::null());
    process.stdout(std::process::Stdio::piped());
    process.stderr(std::process::Stdio::piped());
    if let Some(cwd) = command.cwd {
        process.current_dir(cwd);
    }

    ChildJob::prepare_command(&mut process).map_err(|err| err.to_string())?;
    let mut child = process.spawn().map_err(|err| err.to_string())?;
    let job = ChildJob::assign_to_child(&child).map_err(|err| {
        let _ = child.kill();
        let _ = child.wait();
        format!("failed to isolate host environment probe in job object: {err}")
    })?;
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child.wait_with_output().map_err(|err| err.to_string())?;
                return Ok(crate::infra::CommandOutput {
                    status: output.status.code().unwrap_or(-1),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                });
            }
            Ok(None) if started.elapsed() >= HOST_ENVIRONMENT_PROBE_TIMEOUT => {
                capture_process_diagnostics_best_effort(
                    child.id(),
                    "host environment probe timeout",
                );
                let _ = job.terminate();
                if !wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE).unwrap_or(false) {
                    let _ = job.force_kill();
                    let _ = child.kill();
                    let _ = wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE);
                }
                return Err(format!(
                    "host environment probe timed out after {} ms",
                    HOST_ENVIRONMENT_PROBE_TIMEOUT.as_millis()
                ));
            }
            Ok(None) => thread::sleep(Duration::from_millis(25)),
            Err(err) => return Err(err.to_string()),
        }
    }
}

#[cfg(windows)]
fn powershell_program() -> String {
    "powershell".to_string()
}

#[cfg(windows)]
fn path_ownership_error_reports(paths: &[PathBuf], error: String) -> Vec<PathOwnershipReport> {
    paths
        .iter()
        .map(|path| PathOwnershipReport {
            path: path.display().to_string(),
            owner: None,
            error: Some(error.clone()),
        })
        .collect()
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
    infra_mode: InfraMode,
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
            detail: match infra_mode {
                InfraMode::Disabled => "external infrastructure disabled for this run".to_string(),
                InfraMode::Env => {
                    "external infrastructure provided by repository .env / process environment"
                        .to_string()
                }
                InfraMode::Compose => "docker compose infrastructure selected".to_string(),
            },
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

    runner_checks.push(stale_compose_cleanup_check(
        root,
        sandbox,
        stack.ports(),
        executor,
    ));
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

fn stale_compose_cleanup_check(
    root: &Path,
    current_sandbox: &Path,
    current_ports: &[crate::infra::ServicePort],
    executor: &dyn CommandExecutor,
) -> PreflightCheckReport {
    match cleanup_stale_compose_projects(root, current_sandbox, current_ports, executor) {
        Ok(cleaned) => PreflightCheckReport {
            name: "runner-owned stale compose cleanup".to_string(),
            status: RunStatus::Passed,
            detail: if cleaned.is_empty() {
                "no runner-owned stale compose projects found".to_string()
            } else {
                format!(
                    "cleaned runner-owned stale compose projects:\n{}",
                    cleaned.join("\n")
                )
            },
        },
        Err(error) => PreflightCheckReport {
            name: "runner-owned stale compose cleanup".to_string(),
            status: RunStatus::PreflightFailed,
            detail: error,
        },
    }
}

fn cleanup_stale_compose_projects(
    root: &Path,
    current_sandbox: &Path,
    current_ports: &[crate::infra::ServicePort],
    executor: &dyn CommandExecutor,
) -> Result<Vec<String>, String> {
    if current_ports.is_empty() || !root.exists() {
        return Ok(Vec::new());
    }
    let current_sandbox = absolute_without_requiring_exists(current_sandbox);
    let current_host_ports = current_ports
        .iter()
        .map(|port| port.host_port)
        .collect::<BTreeSet<_>>();
    let existing_projects = existing_compose_projects(executor)?;
    let mut cleaned = Vec::new();
    for project in existing_projects.values() {
        if !is_runner_owned_compose_project(&project.name) {
            continue;
        }
        let Some((sandbox, manifest)) = manifest_for_compose_project(root, &project.name) else {
            continue;
        };
        if absolute_without_requiring_exists(&sandbox) == current_sandbox {
            continue;
        }
        let overlaps_current_ports = manifest
            .allocated_ports
            .iter()
            .any(|port| current_host_ports.contains(&port.host_port));
        if !overlaps_current_ports {
            continue;
        }
        match runner_owner_status(&sandbox, executor)? {
            RunnerOwnerStatus::Active(pid) => {
                return Err(format!(
                    "runner-owned compose project {} overlaps current ports but owner pid {pid} is still active; leaving it running",
                    project.name
                ));
            }
            RunnerOwnerStatus::Unknown
                if project.status.to_ascii_lowercase().contains("running") =>
            {
                return Err(format!(
                    "runner-owned compose project {} overlaps current ports but has no owner_pid marker; leaving running project untouched",
                    project.name
                ));
            }
            RunnerOwnerStatus::Dead(_) | RunnerOwnerStatus::Unknown => {}
        }
        let control = ComposeServiceControl::for_project_with_environment(
            project.name.clone(),
            compose_environment_from_manifest(&manifest),
        )
        .map_err(|err| format!("{}: {err}", project.name))?;
        control
            .teardown(executor)
            .map_err(|err| format!("{}: {err}", project.name))?;
        cleaned.push(project.name.clone());
    }
    Ok(cleaned)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExistingComposeProject {
    name: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ComposeLsProject {
    #[serde(rename = "Name", alias = "name")]
    name: String,
    #[serde(rename = "Status", alias = "status", default)]
    status: String,
}

fn existing_compose_projects(
    executor: &dyn CommandExecutor,
) -> Result<BTreeMap<String, ExistingComposeProject>, String> {
    let output = executor
        .run(&CommandSpec {
            program: "docker".to_string(),
            args: vec![
                "compose".to_string(),
                "ls".to_string(),
                "--all".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ],
            env: Vec::new(),
            cwd: None,
            timeout: Some(Duration::from_secs(30)),
        })
        .map_err(|err| err.to_string())?;
    if output.status != 0 {
        return Err(command_failure_detail(output.status, &output.stderr));
    }
    let stdout = output.stdout.trim();
    if stdout.is_empty() {
        return Ok(BTreeMap::new());
    }
    let entries = serde_json::from_str::<Vec<ComposeLsProject>>(stdout).map_err(|err| {
        format!("docker compose ls --format json returned unexpected JSON: {err}")
    })?;
    Ok(entries
        .into_iter()
        .map(|entry| {
            let name = entry.name;
            (
                name.clone(),
                ExistingComposeProject {
                    name,
                    status: entry.status,
                },
            )
        })
        .collect())
}

fn manifest_for_compose_project(root: &Path, project: &str) -> Option<(PathBuf, RunManifest)> {
    for run_id in run_id_candidates_for_compose_project(project)? {
        let sandbox = root.join(run_id);
        let manifest_path = sandbox.join("run.json");
        let Some(manifest) = read_run_manifest(&manifest_path) else {
            continue;
        };
        if manifest.infra == InfraMode::Compose
            && manifest.compose_project.as_deref() == Some(project)
            && is_runner_owned_compose_project(project)
        {
            return Some((sandbox, manifest));
        }
    }
    None
}

fn read_run_manifest(path: &Path) -> Option<RunManifest> {
    fs::File::open(path)
        .ok()
        .and_then(|file| serde_json::from_reader::<_, RunManifest>(file).ok())
}

fn run_id_candidates_for_compose_project(project: &str) -> Option<Vec<String>> {
    let suffix = project.strip_prefix(COMPOSE_PROJECT_PREFIX)?;
    let mut candidates = vec![suffix.to_string()];
    let mut chars = suffix.chars().collect::<Vec<_>>();
    if chars.len() > 15 && chars.get(8) == Some(&'t') && chars.get(15) == Some(&'z') {
        chars[8] = 'T';
        chars[15] = 'Z';
        let generated = chars.into_iter().collect::<String>();
        if generated != suffix {
            candidates.push(generated);
        }
    }
    Some(candidates)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunnerOwnerStatus {
    Active(u32),
    Dead(u32),
    Unknown,
}

fn runner_owner_status(
    sandbox: &Path,
    executor: &dyn CommandExecutor,
) -> Result<RunnerOwnerStatus, String> {
    let marker_path = sandbox.join(RUNNER_MARKER_FILE);
    let marker = fs::read(&marker_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok());
    let Some(pid) = marker
        .as_ref()
        .and_then(|value| value.get("owner_pid"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok())
    else {
        return Ok(RunnerOwnerStatus::Unknown);
    };
    let output = executor
        .run(&owner_liveness_command(pid))
        .map_err(|err| format!("owner pid {pid} liveness check failed: {err}"))?;
    if output.status == 0 {
        Ok(RunnerOwnerStatus::Active(pid))
    } else {
        Ok(RunnerOwnerStatus::Dead(pid))
    }
}

fn owner_liveness_command(pid: u32) -> CommandSpec {
    if cfg!(windows) {
        CommandSpec {
            program: "powershell".to_string(),
            args: vec![
                "-NoProfile".to_string(),
                "-ExecutionPolicy".to_string(),
                "Bypass".to_string(),
                "-Command".to_string(),
                "$ownerPid=$env:STUDIOFS_E2E_OWNER_PID; $process=Get-CimInstance Win32_Process -Filter \"ProcessId = $ownerPid\" -ErrorAction SilentlyContinue; if ($null -eq $process) { exit 1 }; $name=[string]$process.Name; $commandLine=[string]$process.CommandLine; if ($name -ieq 'studiofs-e2e.exe' -or $commandLine -match '(^|[\\\\/])studiofs-e2e(\\.exe)?(\\s|$)' -or $commandLine -match 'cargo(\\.exe)?\\s+.*studiofs-e2e') { exit 0 } else { exit 1 }".to_string(),
            ],
            env: vec![("STUDIOFS_E2E_OWNER_PID".to_string(), pid.to_string())],
            cwd: None,
            timeout: Some(Duration::from_secs(5)),
        }
    } else {
        CommandSpec {
            program: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "kill -0 \"$STUDIOFS_E2E_OWNER_PID\" >/dev/null 2>&1 && ps -p \"$STUDIOFS_E2E_OWNER_PID\" -o comm= -o args= 2>/dev/null | grep -Eq '(^|[ /])studiofs-e2e( |$)|cargo( |.* )studiofs-e2e'".to_string(),
            ],
            env: vec![("STUDIOFS_E2E_OWNER_PID".to_string(), pid.to_string())],
            cwd: None,
            timeout: Some(Duration::from_secs(5)),
        }
    }
}

fn compose_environment_from_manifest(manifest: &RunManifest) -> Vec<(String, String)> {
    let mut env = vec![("STUDIOFS_E2E_RUN_ID".to_string(), manifest.run_id.clone())];
    env.extend(
        manifest
            .allocated_ports
            .iter()
            .map(|port| (port.env_var.clone(), port.host_port.to_string())),
    );
    env
}

fn is_runner_owned_compose_project(project: &str) -> bool {
    project.starts_with(COMPOSE_PROJECT_PREFIX)
        && project
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_'))
}

pub(crate) fn command_failure_detail(status: i32, stderr: &str) -> String {
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
        timeout: Some(Duration::from_secs(30)),
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

/// Clean up stale run sandboxes under `request.root`.
///
/// Only directories that match the runner-owned manifest marker and are older
/// than the retention-safe safety checks are removed. Returns both removed paths
/// and those that could not be removed.
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
    cleanup_runner_owned_mount_points_before_removal(&path)?;
    fs::remove_dir_all(&path)?;
    Ok(Some(PathBuf::from(name)))
}

#[derive(Debug, Deserialize)]
struct RunnerClientMountMetadata {
    mount_point: PathBuf,
}

fn cleanup_runner_owned_mount_points_before_removal(sandbox: &Path) -> Result<(), RunnerError> {
    let mut last_error = None;
    for mount_point in runner_owned_mount_points_from_metadata(sandbox)? {
        if let Err(error) = cleanup_stale_mount_before_remount(&mount_point) {
            eprintln!(
                "studiofs-e2e: failed to unmount stale runner-owned mount point {}: {error}",
                mount_point.display()
            );
            last_error = Some(error);
        }
    }
    if let Some(error) = last_error {
        return Err(error.into());
    }
    Ok(())
}

fn runner_owned_mount_points_from_metadata(sandbox: &Path) -> Result<Vec<PathBuf>, RunnerError> {
    let clients_dir = sandbox.join("artifacts/clients");
    if !clients_dir.exists() {
        return Ok(Vec::new());
    }

    let mut mount_points = BTreeSet::new();
    for entry in fs::read_dir(clients_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let client_dir = entry.path();
        let mount_metadata_dir = client_dir.join("mount");
        let metadata_path = mount_metadata_dir.join("mount-point.json");
        add_layout_inferred_runner_mount_points(sandbox, &mount_metadata_dir, &mut mount_points);
        let bytes = match fs::read(&metadata_path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(err) => {
                eprintln!(
                    "studiofs-e2e: skipping unreadable client mount metadata {}: {err}",
                    metadata_path.display()
                );
                continue;
            }
        };
        let metadata: RunnerClientMountMetadata = match serde_json::from_slice(&bytes) {
            Ok(metadata) => metadata,
            Err(error) => {
                eprintln!(
                    "studiofs-e2e: skipping corrupt client mount metadata {}: {error}",
                    metadata_path.display()
                );
                continue;
            }
        };
        if metadata.mount_point.starts_with(sandbox) {
            mount_points.insert(metadata.mount_point);
        }
    }
    Ok(mount_points.into_iter().collect())
}

fn add_layout_inferred_runner_mount_points(
    sandbox: &Path,
    mount_metadata_dir: &Path,
    mount_points: &mut BTreeSet<PathBuf>,
) {
    let Ok(entries) = fs::read_dir(mount_metadata_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.file_name() == Some(OsStr::new("mount-point.json")) {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() && path.starts_with(sandbox) {
            mount_points.insert(path);
        }
    }
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
pub(crate) enum ProgressStatus {
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

    fn ansi_color(self) -> &'static str {
        match self {
            Self::Started => "\x1b[33m",
            Self::Passed => "\x1b[32m",
            Self::Failed => "\x1b[31m",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScenarioProgress {
    pub(crate) index: usize,
    pub(crate) total: usize,
}

fn progress_line(
    timestamp: &str,
    run_id: &str,
    task: &str,
    progress: Option<ScenarioProgress>,
    phase: &str,
    status: ProgressStatus,
) -> String {
    progress_line_with_status_value(timestamp, run_id, task, progress, phase, status.as_str())
}

fn progress_line_with_status_color(
    timestamp: &str,
    run_id: &str,
    task: &str,
    progress: Option<ScenarioProgress>,
    phase: &str,
    status: ProgressStatus,
) -> String {
    let status_value = format!("{}{}{}", status.ansi_color(), status.as_str(), ANSI_RESET);
    progress_line_with_status_value(timestamp, run_id, task, progress, phase, &status_value)
}

fn progress_line_with_status_value(
    timestamp: &str,
    run_id: &str,
    task: &str,
    progress: Option<ScenarioProgress>,
    phase: &str,
    status: &str,
) -> String {
    let progress = progress
        .map(|progress| format!(" scenario_progress={}/{}", progress.index, progress.total))
        .unwrap_or_default();
    format!(
        "timestamp={timestamp} studiofs-e2e run_id={run_id} task={task}{progress} phase={phase} status={status}"
    )
}

pub(crate) fn emit_progress(
    run_id: &str,
    task: &str,
    progress: Option<ScenarioProgress>,
    phase: &str,
    status: ProgressStatus,
) {
    let timestamp = timestamp();
    let color_status = std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    let line = if color_status {
        progress_line_with_status_color(&timestamp, run_id, task, progress, phase, status)
    } else {
        progress_line(&timestamp, run_id, task, progress, phase, status)
    };
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{line}");
    let _ = stderr.flush();
}

fn emit_progress_heartbeat(tick: usize) {
    let pulse = "*".repeat(tick);
    let mut stderr = std::io::stderr().lock();
    let _ = write!(stderr, "\r{pulse}");
    let _ = stderr.flush();
}

fn should_emit_progress_heartbeat(stderr_is_terminal: bool) -> bool {
    stderr_is_terminal
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
        let handle = should_emit_progress_heartbeat(std::io::stderr().is_terminal()).then(|| {
            thread::spawn(move || {
                let mut tick = 0usize;
                while stop_rx.recv_timeout(PROGRESS_HEARTBEAT_INTERVAL).is_err() {
                    tick = tick.saturating_add(1);
                    emit_progress_heartbeat(tick);
                }
                clear_progress_heartbeat(tick);
            })
        });
        Self { stop, handle }
    }

    fn stop(mut self) {
        let _ = self.stop.send(());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) fn record_event(
    events_path: &Path,
    run_id: &str,
    scenario: Option<&str>,
    hook: &str,
    phase: &str,
) -> Result<(), RunnerError> {
    record_event_with_optional_detail(events_path, run_id, scenario, hook, phase, None)
}

pub(crate) fn record_event_with_detail(
    events_path: &Path,
    run_id: &str,
    scenario: Option<&str>,
    hook: &str,
    phase: &str,
    detail: String,
) -> Result<(), RunnerError> {
    record_event_with_optional_detail(events_path, run_id, scenario, hook, phase, Some(detail))
}

fn record_event_with_optional_detail(
    events_path: &Path,
    run_id: &str,
    scenario: Option<&str>,
    hook: &str,
    phase: &str,
    detail: Option<String>,
) -> Result<(), RunnerError> {
    let record = EventRecord {
        timestamp: timestamp(),
        run_id: run_id.to_string(),
        scenario: scenario.map(str::to_string),
        hook: hook.to_string(),
        phase: phase.to_string(),
        detail,
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
    if is_client_mount_artifact_tree(sandbox, dir) {
        return Ok(());
    }
    if !dir.exists() {
        return Ok(());
    }
    for entry_result in fs::read_dir(dir)? {
        let entry = entry_result?;
        let path = entry.path();
        if is_client_mount_artifact_tree(sandbox, &path) {
            continue;
        }
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

fn is_client_mount_artifact_tree(sandbox: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(sandbox) else {
        return false;
    };
    let mut components = relative.components();
    matches!(
        (
            components.next(),
            components.next(),
            components.next(),
            components.next()
        ),
        (
            Some(Component::Normal(artifacts)),
            Some(Component::Normal(clients)),
            Some(Component::Normal(_client)),
            Some(Component::Normal(mount)),
        ) if artifacts == "artifacts" && clients == "clients" && mount == "mount"
    )
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

pub(crate) fn scenario_artifact_path(scenario: &str, artifact: &str) -> String {
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
    fs::write(sandbox.join("events.ndjson"), "")?;
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
            "owner_pid": std::process::id(),
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
            "artifact": "cleanup-boundary",
            "source_manifest": "run.json",
            "purpose": "runner-owned cleanup allowlist and failure-boundary context",
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

pub(crate) const SCENARIO_CHILD_REPORT_FILE: &str = "scenario-child-report.json";
pub(crate) const SCENARIO_CHILD_STDOUT_FILE: &str = "scenario-child.stdout.log";
pub(crate) const SCENARIO_CHILD_STDERR_FILE: &str = "scenario-child.stderr.log";

pub(crate) fn scenario_child_report_path(sandbox: &Path, scenario: &str) -> PathBuf {
    sandbox
        .join("artifacts")
        .join("scenarios")
        .join(scenario)
        .join(SCENARIO_CHILD_REPORT_FILE)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScenarioChildReport {
    pub(crate) scenario: ScenarioReport,
    pub(crate) cleanup_error: Option<String>,
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
    if !path.join("artifacts").exists() {
        return Ok(false);
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

pub(crate) fn timestamp() -> String {
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
    use crate::infra::command_streams_output;
    use crate::infra::{CommandExecutor, CommandOutput, InfraMode};
    use crate::runner_lifecycle::DEFAULT_SCENARIO_CHILD_TIMEOUT;
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
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
    fn progress_line_can_color_only_status_value() {
        assert_eq!(
            progress_line_with_status_color(
                "2026-05-11T16:40:00.000Z",
                "run-1",
                "runner",
                None,
                "preflight",
                ProgressStatus::Started
            ),
            "timestamp=2026-05-11T16:40:00.000Z studiofs-e2e run_id=run-1 task=runner phase=preflight status=\x1b[33mstarted\x1b[0m"
        );
        assert_eq!(
            progress_line_with_status_color(
                "2026-05-11T16:40:00.000Z",
                "run-1",
                "runner",
                None,
                "cleanup",
                ProgressStatus::Passed
            ),
            "timestamp=2026-05-11T16:40:00.000Z studiofs-e2e run_id=run-1 task=runner phase=cleanup status=\x1b[32mpassed\x1b[0m"
        );
        assert_eq!(
            progress_line_with_status_color(
                "2026-05-11T16:40:00.000Z",
                "run-1",
                "collision-rename-vs-rename",
                Some(ScenarioProgress { index: 3, total: 8 }),
                "validate",
                ProgressStatus::Failed
            ),
            "timestamp=2026-05-11T16:40:00.000Z studiofs-e2e run_id=run-1 task=collision-rename-vs-rename scenario_progress=3/8 phase=validate status=\x1b[31mfailed\x1b[0m"
        );
    }

    #[test]
    fn relayed_child_progress_colorizes_status_values() {
        assert_eq!(
            colorize_progress_statuses(
                "timestamp=2026-05-13T08:26:40.107Z studiofs-e2e run_id=run task=scenario phase=scenario_child status=started\n\
timestamp=2026-05-13T08:26:41.107Z studiofs-e2e run_id=run task=scenario phase=scenario_child status=passed\n\
plain child stderr\n",
            ),
            "timestamp=2026-05-13T08:26:40.107Z studiofs-e2e run_id=run task=scenario phase=scenario_child status=\x1b[33mstarted\x1b[0m\n\
timestamp=2026-05-13T08:26:41.107Z studiofs-e2e run_id=run task=scenario phase=scenario_child status=\x1b[32mpassed\x1b[0m\n\
plain child stderr\n",
        );
    }

    #[test]
    fn progress_heartbeat_is_tty_only() {
        assert!(should_emit_progress_heartbeat(true));
        assert!(!should_emit_progress_heartbeat(false));
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
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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

        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let sandbox = temp.path().join("run-1");
        let invalid = OsString::from_vec(b"invalid-\xFF.log".to_vec());
        let artifact = sandbox.join("artifacts").join(invalid);

        let err = normalize_relative_path(&artifact, &sandbox).unwrap_err();

        assert!(matches!(err, RunnerError::NonUtf8ArtifactPath { .. }));
    }

    #[test]
    fn record_event_requires_existing_parent_directory() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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

        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
    fn report_artifact_discovery_skips_client_mount_trees() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let sandbox = temp.path().join("run-1");
        let mount_dir = sandbox.join("artifacts/clients/client-a/mount/volume");
        let log_dir = sandbox.join("artifacts/clients/client-a");
        std::fs::create_dir_all(&mount_dir).unwrap();
        std::fs::write(log_dir.join("studiofs.stdout.log"), "mounted").unwrap();
        std::fs::write(mount_dir.join("mounted-file.txt"), "not an artifact").unwrap();

        let artifacts = collect_report_artifacts(&sandbox, &[]).unwrap();

        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact.path == "artifacts/clients/client-a/studiofs.stdout.log")
        );
        assert!(
            !artifacts
                .iter()
                .any(|artifact| artifact.path.contains("/mount/")),
            "mounted filesystem contents should not be reported as runner artifacts"
        );
    }

    #[test]
    fn run_writes_report_when_scenario_fails() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        let postgres_state = outcome
            .sandbox
            .join("artifacts/scenarios/__test-passing/post-scenario-postgres-state.json");
        assert!(
            postgres_state.exists(),
            "compose runs should capture post-scenario PostgreSQL state"
        );
        assert!(
            report["scenarios"][0]["artifacts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|artifact| artifact
                    == "artifacts/scenarios/__test-passing/post-scenario-postgres-state.json")
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
    fn multi_scenario_run_uses_supervised_run_one_children() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("supervised-run".to_string()),
                scenarios: vec![
                    scenario::TEST_PASSING.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 410,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Passed);
        assert_eq!(outcome.scenarios.len(), 2);
        let calls = executor.calls.borrow();
        let child_calls: Vec<_> = calls
            .iter()
            .filter(|call| {
                call.program.ends_with("studiofs-e2e")
                    || call.program.ends_with("studiofs-e2e.exe")
                    || call.args.contains(&"run-one".to_string())
            })
            .collect();
        assert_eq!(child_calls.len(), 2, "one child process per scenario");
        assert!(child_calls.iter().all(|call| {
            call.args.contains(&"run-one".to_string())
                && call.args.contains(&"--scenario".to_string())
                && command_streams_output(call)
                && call.timeout == Some(Duration::from_secs(30))
        }));

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["scenarios"].as_array().unwrap().len(), 2);
        assert!(
            report["artifacts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|artifact| artifact["path"]
                    .as_str()
                    .unwrap()
                    .ends_with(SCENARIO_CHILD_REPORT_FILE))
        );
    }

    #[test]
    fn multi_scenario_run_bounds_supervised_children_without_user_timeout() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let executor = RecordingExecutor::default();

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("default-supervised-timeout-run".to_string()),
                scenarios: vec![
                    scenario::TEST_PASSING.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 410,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: None,
                infra: InfraMode::Disabled,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Passed);
        let calls = executor.calls.borrow();
        let child_calls = calls
            .iter()
            .filter(|call| call.args.contains(&"run-one".to_string()))
            .collect::<Vec<_>>();
        assert_eq!(child_calls.len(), 2);
        assert!(
            child_calls
                .iter()
                .all(|call| call.timeout == Some(DEFAULT_SCENARIO_CHILD_TIMEOUT)),
            "supervised children must always be bounded by the parent runner"
        );
    }

    #[test]
    fn clean_discovers_recorded_client_mount_points_before_removal() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let sandbox = temp.path().join("e2e-runs").join("dead-mount-run");
        let mount_a = sandbox.join("artifacts/clients/client-a/mount/projectfs");
        let mount_b = sandbox.join("artifacts/clients/client-b/mount/projectfs");
        fs::create_dir_all(mount_a.parent().unwrap()).unwrap();
        fs::create_dir_all(mount_b.parent().unwrap()).unwrap();
        fs::write(
            mount_a.parent().unwrap().join("mount-point.json"),
            serde_json::json!({
                "client": "client-a",
                "mount_point": mount_a,
                "process_id": 101
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            mount_b.parent().unwrap().join("mount-point.json"),
            serde_json::json!({
                "client": "client-b",
                "mount_point": mount_b,
                "process_id": 102
            })
            .to_string(),
        )
        .unwrap();

        let mount_points = runner_owned_mount_points_from_metadata(&sandbox).unwrap();

        assert_eq!(mount_points, vec![mount_a, mount_b]);
    }

    #[test]
    fn clean_skips_corrupt_client_mount_metadata() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let sandbox = temp.path().join("e2e-runs").join("corrupt-metadata-run");
        let mount_a = sandbox.join("artifacts/clients/client-a/mount/projectfs");
        let client_b_mount_dir = sandbox.join("artifacts/clients/client-b/mount");
        fs::create_dir_all(mount_a.parent().unwrap()).unwrap();
        fs::create_dir_all(&client_b_mount_dir).unwrap();
        fs::write(
            mount_a.parent().unwrap().join("mount-point.json"),
            serde_json::json!({
                "client": "client-a",
                "mount_point": mount_a,
                "process_id": 101
            })
            .to_string(),
        )
        .unwrap();
        fs::write(client_b_mount_dir.join("mount-point.json"), b"{").unwrap();

        let mount_points = runner_owned_mount_points_from_metadata(&sandbox).unwrap();

        assert_eq!(mount_points, vec![mount_a]);
    }

    #[test]
    fn clean_infers_layout_mount_points_when_client_mount_metadata_is_unreadable() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let sandbox = temp.path().join("e2e-runs").join("unreadable-metadata-run");
        let mount_a = sandbox.join("artifacts/clients/client-a/mount/projectfs");
        let client_a_mount_dir = mount_a.parent().unwrap();
        fs::create_dir_all(&mount_a).unwrap();
        fs::create_dir_all(client_a_mount_dir.join("mount-point.json")).unwrap();

        let mount_points = runner_owned_mount_points_from_metadata(&sandbox).unwrap();

        assert_eq!(mount_points, vec![mount_a]);
    }

    #[test]
    fn supervised_child_timeout_records_executor_cleanup_detail() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let executor = RecordingExecutor {
            time_out_first_child: Cell::new(true),
            ..RecordingExecutor::default()
        };

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("child-timeout-detail-run".to_string()),
                scenarios: vec![
                    scenario::TEST_PASSING.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 410,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["scenarios"][0]["status"], "timed_out");
        assert!(
            report["scenarios"][0]["error"]
                .as_str()
                .unwrap()
                .contains("child still running after kill attempts")
        );

        let events = std::fs::read_to_string(outcome.sandbox.join("events.ndjson")).unwrap();
        let timeout_event = events
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|event| event["hook"] == "scenario_child" && event["phase"] == "timed_out")
            .expect("scenario child timeout event should be written");
        assert!(
            timeout_event["detail"]
                .as_str()
                .unwrap()
                .contains("child still running after kill attempts")
        );
    }

    #[test]
    fn mounted_child_timeout_blocks_later_mounted_children() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let executor = RecordingExecutor {
            time_out_first_child: Cell::new(true),
            ..RecordingExecutor::default()
        };

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("mounted-timeout-block-run".to_string()),
                scenarios: vec![
                    scenario::BASIC_MULTI_CLIENT_SMOKE.to_string(),
                    scenario::MOUNTED_CLIENT_CRASH_RESTART_RECOVERY.to_string(),
                ],
                seed: 410,
                studiofs_bin: None,
                platform: PlatformSelection::WindowsWinfsp,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        let calls = executor.calls.borrow();
        let child_calls = calls
            .iter()
            .filter(|call| call.args.contains(&"run-one".to_string()))
            .count();
        assert_eq!(child_calls, 1, "second mounted scenario must not start");
        assert!(
            !calls.iter().any(|call| {
                call.env.iter().any(|(key, value)| {
                    key == "STUDIOFS_E2E_CLEANUP_PHASE" && value == "post-scenario"
                })
            }),
            "failed mounted scenarios must not start the external post-scenario cleanup verifier"
        );

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["scenarios"][0]["status"], "timed_out");
        assert_eq!(report["scenarios"][1]["status"], "skipped");
        assert!(
            report["scenarios"][1]["error"]
                .as_str()
                .unwrap()
                .contains("command timed out after 30000 ms")
        );
    }

    #[test]
    fn mounted_child_failure_blocks_later_mounted_children() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let executor = RecordingExecutor {
            fail_first_child_report: Cell::new(true),
            ..RecordingExecutor::default()
        };

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("mounted-failure-block-run".to_string()),
                scenarios: vec![
                    scenario::BASIC_MULTI_CLIENT_SMOKE.to_string(),
                    scenario::MOUNTED_CLIENT_CRASH_RESTART_RECOVERY.to_string(),
                ],
                seed: 410,
                studiofs_bin: None,
                platform: PlatformSelection::WindowsWinfsp,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        let calls = executor.calls.borrow();
        let child_calls = calls
            .iter()
            .filter(|call| call.args.contains(&"run-one".to_string()))
            .count();
        assert_eq!(child_calls, 1, "second mounted scenario must not start");

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["scenarios"][0]["status"], "failed");
        assert_eq!(report["scenarios"][1]["status"], "skipped");
        assert!(
            report["scenarios"][1]["error"]
                .as_str()
                .unwrap()
                .contains("simulated child scenario failure")
        );
    }

    #[test]
    fn mounted_supervision_error_writes_report_without_post_scenario_cleanup_verifier() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let executor = RecordingExecutor {
            fail_child_supervision_io: Cell::new(true),
            ..RecordingExecutor::default()
        };

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("supervision-error-cleanup-run".to_string()),
                scenarios: vec![
                    scenario::MOUNTED_CLIENT_CRASH_RESTART_RECOVERY.to_string(),
                    scenario::TEST_PASSING.to_string(),
                ],
                seed: 410,
                studiofs_bin: None,
                platform: PlatformSelection::WindowsWinfsp,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: Some("30s".to_string()),
                infra: InfraMode::Disabled,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Failed);
        let calls = executor.calls.borrow();
        assert!(calls.iter().any(|call| {
            call.args.contains(&"run-one".to_string())
                && call
                    .env
                    .iter()
                    .any(|(key, value)| key == "STUDIOFS_E2E_STREAM_OUTPUT" && value == "1")
        }));
        assert!(
            !calls.iter().any(|call| {
                call.env.iter().any(|(key, value)| {
                    key == "STUDIOFS_E2E_CLEANUP_PHASE" && value == "post-scenario"
                })
            }),
            "supervision errors must not start the external post-scenario cleanup verifier"
        );

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["scenarios"][0]["status"], "failed");
        assert!(
            report["scenarios"][0]["error"]
                .as_str()
                .unwrap()
                .contains("scenario supervision failed")
        );
    }

    #[test]
    fn stale_process_cleanup_command_selects_platform_helper() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
    fn stale_process_cleanup_scripts_match_named_hotspot_helper() {
        let windows_script = include_str!("../scripts/check-stale-processes.ps1");
        let unix_script = include_str!("../scripts/check-stale-processes.sh");

        assert!(windows_script.contains("'studiofs-e2e-hotspot-helper'"));
        assert!(unix_script.contains("studiofs-e2e-hotspot-helper"));
        assert!(unix_script.contains("studiofs-e2e-hotspot-helper.exe"));
        assert!(unix_script.contains("/proc/[0-9]*"));
        assert!(!unix_script.contains("ps -eo"));
    }

    #[test]
    fn windows_stale_process_cleanup_batches_taskkill_for_all_candidates() {
        let windows_script = include_str!("../scripts/check-stale-processes.ps1");

        assert!(windows_script.contains("$candidates = @("));
        assert!(windows_script.contains("mount-point.json"));
        assert!(windows_script.contains("hotspot-helper-processes"));
        assert!(windows_script.contains("Add-CandidateProcess"));
        assert!(windows_script.contains("$taskkillArgs = @()"));
        assert!(windows_script.contains("[string]::Join(' ', $taskkillArgs)"));
        assert!(windows_script.contains("batched taskkill timed out after 5000 ms"));
        assert!(windows_script.contains("Stop-CandidateDirectly"));
        assert!(windows_script.contains("$process.Kill()"));
        assert!(!windows_script.contains("Get-CimInstance Win32_Process"));
        assert!(!windows_script.contains("-Recurse -Filter 'mount-point.json'"));
        assert!(!windows_script.contains("Get-Process -Name 'studiofs-e2e-hotspot-helper'"));
        assert!(
            !windows_script
                .contains("$taskkillStartInfo.Arguments = \"/PID $($_.ProcessId) /T /F\"")
        );
    }

    #[test]
    fn runner_run_cleans_stale_runner_processes_before_preflight() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
    fn runner_run_cleans_stale_compose_projects_before_port_preflight() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let root = temp.path().join("e2e-runs");
        let stale_sandbox = root.join("stale-run");
        std::fs::create_dir_all(&stale_sandbox).unwrap();
        std::fs::write(
            stale_sandbox.join("run.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "runner_crate": RUNNER_CRATE,
                "run_id": "stale-run",
                "seed": 0,
                "sandbox": stale_sandbox.display().to_string(),
                "scenarios": [scenario::BASIC_MULTI_CLIENT_SMOKE],
                "retention": "on-failure",
                "selected_platform": "windows-winfsp",
                "infra": "compose",
                "compose_project": "studiofs-e2e-stale-run",
                "allocated_ports": [
                    {
                        "service": "postgres",
                        "host": "127.0.0.1",
                        "host_port": 33600,
                        "container_port": 5432,
                        "env_var": "STUDIOFS_E2E_POSTGRES_PORT"
                    }
                ],
                "studiofs_bin": null,
                "port_range": "33600-33603",
                "timeout": null,
                "explicit_client_roots": [],
                "cleanup_allowlist": [stale_sandbox.display().to_string()],
                "created_at": "2026-05-12T19:55:45.337Z"
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            stale_sandbox.join(RUNNER_MARKER_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": SCHEMA_VERSION,
                "runner_crate": RUNNER_CRATE,
                "run_id": "stale-run",
                "owner_pid": 4242,
                "sandbox": stale_sandbox.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();
        let executor = RecordingExecutor::default();
        executor.compose_ls_stdout.replace(
            serde_json::json!([
                {
                    "Name": "studiofs-e2e-stale-run",
                    "Status": "running(3)"
                }
            ])
            .to_string(),
        );

        let outcome = run_with_executor(
            RunRequest {
                root,
                run_id: Some("current-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 407,
                studiofs_bin: None,
                platform: PlatformSelection::WindowsWinfsp,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33600-33603".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .unwrap();

        assert_eq!(outcome.status, RunStatus::Passed);

        let calls = executor.calls.borrow();
        let stale_down_index = calls
            .iter()
            .position(|call| {
                call.args.contains(&"--project-name".to_string())
                    && call.args.contains(&"studiofs-e2e-stale-run".to_string())
                    && call.args.contains(&"down".to_string())
            })
            .expect("stale compose cleanup should run docker compose down");
        let current_up_index = calls
            .iter()
            .position(|call| {
                call.args.contains(&"--project-name".to_string())
                    && call.args.contains(&"studiofs-e2e-current-run".to_string())
                    && call.args.contains(&"up".to_string())
            })
            .expect("current compose stack should start after stale cleanup");
        assert!(stale_down_index < current_up_index);

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert!(
            report["preflight"]["checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(
                    |check| check["name"] == "runner-owned stale compose cleanup"
                        && check["status"] == "passed"
                        && check["detail"]
                            .as_str()
                            .unwrap()
                            .contains("studiofs-e2e-stale-run")
                )
        );
    }

    #[test]
    fn runner_run_refuses_to_clean_active_compose_owner() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let root = temp.path().join("e2e-runs");
        let active_sandbox = root.join("active-run");
        std::fs::create_dir_all(&active_sandbox).unwrap();
        std::fs::write(
            active_sandbox.join("run.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "runner_crate": RUNNER_CRATE,
                "run_id": "active-run",
                "seed": 0,
                "sandbox": active_sandbox.display().to_string(),
                "scenarios": [scenario::BASIC_MULTI_CLIENT_SMOKE],
                "retention": "on-failure",
                "selected_platform": "windows-winfsp",
                "infra": "compose",
                "compose_project": "studiofs-e2e-active-run",
                "allocated_ports": [
                    {
                        "service": "postgres",
                        "host": "127.0.0.1",
                        "host_port": 33700,
                        "container_port": 5432,
                        "env_var": "STUDIOFS_E2E_POSTGRES_PORT"
                    }
                ],
                "studiofs_bin": null,
                "port_range": "33700-33703",
                "timeout": null,
                "explicit_client_roots": [],
                "cleanup_allowlist": [active_sandbox.display().to_string()],
                "created_at": "2026-05-12T19:55:45.337Z"
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            active_sandbox.join(RUNNER_MARKER_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": SCHEMA_VERSION,
                "runner_crate": RUNNER_CRATE,
                "run_id": "active-run",
                "owner_pid": 4242,
                "sandbox": active_sandbox.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();
        let executor = RecordingExecutor::default();
        executor.active_owner_pids.borrow_mut().insert(4242);
        executor.compose_ls_stdout.replace(
            serde_json::json!([
                {
                    "Name": "studiofs-e2e-active-run",
                    "Status": "running(3)"
                }
            ])
            .to_string(),
        );

        let outcome = run_with_executor(
            RunRequest {
                root,
                run_id: Some("current-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 408,
                studiofs_bin: None,
                platform: PlatformSelection::WindowsWinfsp,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33700-33703".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .unwrap();

        let calls = executor.calls.borrow();
        assert!(!calls.iter().any(|call| {
            call.args.contains(&"studiofs-e2e-active-run".to_string())
                && call.args.contains(&"down".to_string())
        }));
        assert!(!calls.iter().any(|call| {
            call.args.contains(&"studiofs-e2e-current-run".to_string())
                && call.args.contains(&"up".to_string())
        }));

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path).unwrap()).unwrap();
        assert_eq!(report["status"], "preflight_failed");
        assert!(
            report["preflight"]["checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(
                    |check| check["name"] == "runner-owned stale compose cleanup"
                        && check["status"] == "preflight_failed"
                        && check["detail"]
                            .as_str()
                            .unwrap()
                            .contains("owner pid 4242 is still active")
                )
        );
    }

    #[test]
    fn owner_liveness_command_rejects_pid_reuse_by_non_runner_process() {
        let command = owner_liveness_command(4242);
        let script = command.args.join(" ");

        assert!(
            script.contains("studiofs-e2e"),
            "owner liveness must verify the PID still belongs to the e2e runner, not just any reused process"
        );
    }

    #[test]
    fn existing_compose_projects_rejects_unexpected_json_shape() {
        let executor = RecordingExecutor::default();
        executor
            .compose_ls_stdout
            .replace(serde_json::json!({ "Name": "studiofs-e2e-old" }).to_string());

        let err = existing_compose_projects(&executor).unwrap_err();

        assert!(err.contains("unexpected JSON"));
    }

    #[test]
    fn run_with_compose_infra_tears_down_when_post_start_io_fails() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
    fn compose_start_failure_records_cleanup_boundary_artifact()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        )?;

        assert_eq!(outcome.status, RunStatus::PreflightFailed);
        assert_eq!(outcome.exit_code, 2);
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&outcome.report_path)?)?;
        assert!(
            report["artifacts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|artifact| artifact["path"] == "artifacts/runner/cleanup-boundary.json"),
            "cleanup boundary artifact should be discoverable from report.json"
        );

        let boundary: serde_json::Value = serde_json::from_slice(&std::fs::read(
            outcome
                .sandbox
                .join("artifacts/runner/cleanup-boundary.json"),
        )?)?;
        assert_eq!(boundary["run_id"], "compose-partial-launch-failure");
        assert_eq!(boundary["source_manifest"], "run.json");
        assert!(boundary["purpose"].as_str().unwrap().contains("cleanup"));
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
        Ok(())
    }

    #[test]
    fn compose_partial_launch_failure_writes_adversarial_cleanup_proof()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let executor = RecordingExecutor {
            fail_up: Cell::new(true),
            ..RecordingExecutor::default()
        };

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("adversarial-cleanup-proof-run".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 381,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33800-33810".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )?;

        assert_eq!(outcome.status, RunStatus::PreflightFailed);
        let proof_path = outcome
            .sandbox
            .join("artifacts/runner/adversarial-cleanup-proof.json");
        assert!(proof_path.exists());

        let proof: serde_json::Value = serde_json::from_slice(&std::fs::read(&proof_path)?)?;
        assert_eq!(proof["issue"], "SFS-381");
        assert_eq!(proof["run_id"], "adversarial-cleanup-proof-run");
        assert_eq!(proof["status"], "preflight_failed");
        assert_eq!(proof["exit_code"], 2);
        assert_eq!(proof["cleanup_status"], "passed");
        assert!(proof["artifacts"]["report_json"].as_bool().unwrap());
        assert!(proof["artifacts"]["events_ndjson"].as_bool().unwrap());
        assert!(proof["artifacts"]["cleanup_json"].as_bool().unwrap());
        assert!(proof["artifacts"]["cleanup_boundary"].as_bool().unwrap());
        assert_eq!(proof["contract"]["runner_owned_scope_only"], true);
        assert_eq!(
            proof["contract"]["primary_failure_exit_code_wins_over_cleanup_failure"],
            true
        );
        assert_eq!(
            proof["contract"]["cleanup_errors_recorded_separately"],
            true
        );
        assert!(
            proof["cleanup_allowlist"]
                .as_array()
                .unwrap()
                .iter()
                .all(|path| path
                    .as_str()
                    .unwrap()
                    .starts_with(outcome.sandbox.to_str().unwrap()))
        );

        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path)?)?;
        assert!(
            report["artifacts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|artifact| artifact["path"]
                    == "artifacts/runner/adversarial-cleanup-proof.json")
        );
        Ok(())
    }

    #[test]
    fn preflight_failure_cleanup_failure_keeps_primary_exit_code_in_cleanup_proof()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let executor = RecordingExecutor {
            fail_up: Cell::new(true),
            fail_logs: Cell::new(true),
            ..RecordingExecutor::default()
        };

        let outcome = run_with_executor(
            RunRequest {
                root: temp.path().join("e2e-runs"),
                run_id: Some("preflight-and-cleanup-failed-proof".to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 381,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: Some("33820-33830".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )?;

        assert_eq!(outcome.status, RunStatus::PreflightFailed);
        assert_eq!(outcome.exit_code, 2);
        let proof: serde_json::Value = serde_json::from_slice(&std::fs::read(
            outcome
                .sandbox
                .join("artifacts/runner/adversarial-cleanup-proof.json"),
        )?)?;
        assert_eq!(proof["cleanup_status"], "cleanup_failed");
        assert_eq!(
            proof["contract"]["primary_failure_exit_code_wins_over_cleanup_failure"],
            true
        );
        assert_eq!(
            proof["contract"]["cleanup_errors_recorded_separately"],
            true
        );
        Ok(())
    }

    #[test]
    fn adversarial_cleanup_proof_write_error_does_not_shadow_report()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let root = temp.path().join("e2e-runs");
        let run_id = "proof-write-error-report-run";
        std::fs::create_dir_all(
            root.join(run_id)
                .join("artifacts/runner/adversarial-cleanup-proof.json"),
        )?;

        let outcome = run_with_executor(
            RunRequest {
                root,
                run_id: Some(run_id.to_string()),
                scenarios: vec![scenario::TEST_PASSING.to_string()],
                seed: 381,
                studiofs_bin: None,
                platform: PlatformSelection::Auto,
                retention: RetentionPolicy::Always,
                report: None,
                port_range: None,
                timeout: None,
                infra: InfraMode::Disabled,
            },
            &RecordingExecutor::default(),
        )?;

        assert_eq!(outcome.status, RunStatus::Passed);
        assert!(outcome.report_path.exists());
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(outcome.report_path)?)?;
        assert_eq!(report["status"], "passed");
        Ok(())
    }

    #[test]
    fn post_start_runner_failure_writes_best_effort_report()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
        .expect_err("post-start I/O failure should be returned");

        assert!(matches!(err, RunnerError::Io(_)));
        let report_path = sandbox.join("report.json");
        assert!(
            report_path.exists(),
            "post-start failure must preserve report.json"
        );
        let report: serde_json::Value = serde_json::from_slice(&std::fs::read(report_path)?)?;
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
        let failure: serde_json::Value = serde_json::from_slice(&std::fs::read(
            sandbox.join("artifacts/runner/failure-boundary.json"),
        )?)?;
        assert_eq!(failure["phase"], "post-preflight");
        assert!(
            failure["event_log"]
                .as_str()
                .unwrap()
                .contains("events.ndjson")
        );
        assert!(!failure["primary_error"].as_str().unwrap().is_empty());
        Ok(())
    }

    #[test]
    fn post_start_cleanup_report_errors_do_not_shadow_primary_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let root = temp.path().join("e2e-runs");
        let run_id = "post-start-cleanup-report-error";
        let sandbox = root.join(run_id);
        let executor = RecordingExecutor {
            poison_events_path_after_up: RefCell::new(Some(sandbox.join("events.ndjson"))),
            poison_failure_boundary_path_after_up: RefCell::new(Some(
                sandbox.join("artifacts/runner/failure-boundary.json"),
            )),
            fail_logs: Cell::new(true),
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
                port_range: Some("33500-33510".to_string()),
                timeout: None,
                infra: InfraMode::Compose,
            },
            &executor,
        )
        .expect_err("primary event-log failure should be preserved");

        assert!(matches!(err, RunnerError::Io(_)));
        assert!(
            sandbox.join("cleanup.json").exists(),
            "cleanup/reporting errors should not abort early-failure cleanup before recording cleanup.json: {err}"
        );
        let cleanup: serde_json::Value =
            serde_json::from_slice(&std::fs::read(sandbox.join("cleanup.json"))?)?;
        assert_eq!(cleanup["status"], "cleanup_failed");
        assert!(
            cleanup["errors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|error| error.as_str().unwrap().contains("docker compose logs"))
        );
        Ok(())
    }

    #[derive(Default)]
    struct RecordingExecutor {
        calls: RefCell<Vec<crate::infra::CommandSpec>>,
        compose_ls_stdout: RefCell<String>,
        active_owner_pids: RefCell<BTreeSet<u32>>,
        poison_events_path_after_up: RefCell<Option<PathBuf>>,
        poison_failure_boundary_path_after_up: RefCell<Option<PathBuf>>,
        fail_up: Cell<bool>,
        fail_logs: Cell<bool>,
        fail_child_supervision_io: Cell<bool>,
        fail_first_child_report: Cell<bool>,
        time_out_first_child: Cell<bool>,
        child_run_count: Cell<usize>,
    }

    impl CommandExecutor for RecordingExecutor {
        fn run(&self, command: &crate::infra::CommandSpec) -> std::io::Result<CommandOutput> {
            self.calls.borrow_mut().push(command.clone());
            if command.args.contains(&"run-one".to_string()) {
                self.child_run_count
                    .set(self.child_run_count.get().saturating_add(1));
                if self.fail_child_supervision_io.get() && self.child_run_count.get() == 1 {
                    return Err(std::io::Error::other(
                        "simulated supervised child transport failure",
                    ));
                }
                if self.time_out_first_child.get() && self.child_run_count.get() == 1 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "command timed out after 30000 ms; child still running after kill attempts",
                    ));
                }
                write_recorded_child_report(
                    command,
                    self.fail_first_child_report.get() && self.child_run_count.get() == 1,
                )?;
                return Ok(CommandOutput {
                    status: 0,
                    stdout: if self.fail_first_child_report.get() && self.child_run_count.get() == 1
                    {
                        "child scenario failed\n".to_string()
                    } else {
                        "child scenario passed\n".to_string()
                    },
                    stderr: "timestamp=2026-05-13T07:21:44.783Z studiofs-e2e child\n".to_string(),
                });
            }
            if let Some((_, pid)) = command
                .env
                .iter()
                .find(|(key, _)| key == "STUDIOFS_E2E_OWNER_PID")
            {
                let status = pid
                    .parse::<u32>()
                    .ok()
                    .filter(|pid| self.active_owner_pids.borrow().contains(pid))
                    .map_or(1, |_| 0);
                return Ok(CommandOutput {
                    status,
                    stdout: String::new(),
                    stderr: String::new(),
                });
            }
            if command.args.contains(&"ls".to_string()) {
                return Ok(CommandOutput {
                    status: 0,
                    stdout: self.compose_ls_stdout.borrow().clone(),
                    stderr: String::new(),
                });
            }
            if command.args.contains(&"up".to_string()) {
                if self.fail_up.get() {
                    return Ok(CommandOutput {
                        status: 1,
                        stdout: String::new(),
                        stderr: "compose helper launch failed".to_string(),
                    });
                }
                if let Some(events_path) = self.poison_events_path_after_up.borrow_mut().take() {
                    if events_path.is_file() {
                        std::fs::remove_file(&events_path)?;
                    }
                    std::fs::create_dir_all(events_path)?;
                }
                if let Some(failure_boundary_path) = self
                    .poison_failure_boundary_path_after_up
                    .borrow_mut()
                    .take()
                {
                    std::fs::create_dir_all(failure_boundary_path)?;
                }
            }
            if command.args.contains(&"logs".to_string()) && self.fail_logs.get() {
                return Ok(CommandOutput {
                    status: 1,
                    stdout: String::new(),
                    stderr: "compose logs failed".to_string(),
                });
            }
            let stdout = if command.args.contains(&"logs".to_string()) {
                "compose logs\n"
            } else if command.args.contains(&"psql".to_string()) {
                r#"[{"vol":1,"name":"projectfs","uuid":"vol-uuid","metadata_version":2,"event_count":3,"max_seq_id":3,"last_event_created_at":"2026-05-18T00:00:00Z"}]"#
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

    fn write_recorded_child_report(
        command: &crate::infra::CommandSpec,
        failed: bool,
    ) -> std::io::Result<()> {
        let arg_value = |name: &str| {
            command
                .args
                .windows(2)
                .find(|pair| pair[0] == name)
                .map(|pair| pair[1].clone())
        };
        let scenario = arg_value("--scenario").expect("run-one scenario");
        let root = PathBuf::from(arg_value("--root").expect("run-one root"));
        let run_id = arg_value("--run-id").expect("run-one run id");
        let scenario_dir = root
            .join(&run_id)
            .join("artifacts")
            .join("scenarios")
            .join(&scenario);
        std::fs::create_dir_all(&scenario_dir)?;
        std::fs::write(
            scenario_dir.join(SCENARIO_CHILD_REPORT_FILE),
            serde_json::to_vec_pretty(&ScenarioChildReport {
                scenario: ScenarioReport {
                    name: scenario.clone(),
                    seed: 410,
                    status: if failed {
                        ScenarioStatus::Failed
                    } else {
                        ScenarioStatus::Passed
                    },
                    started_at: "2026-05-13T07:21:44.783Z".to_string(),
                    finished_at: "2026-05-13T07:21:45.783Z".to_string(),
                    duration_ms: 1000,
                    artifacts: vec![scenario_artifact_path(&scenario, "operations.ndjson")],
                    error: failed.then(|| "simulated child scenario failure".to_string()),
                },
                cleanup_error: None,
            })?,
        )?;
        Ok(())
    }
}

//! Runner orchestration, sandbox creation, reports, and cleanup.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::{SecondsFormat, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::report::{
    ArtifactReport, CleanupReport, EventRecord, HostReport, PreflightCheckReport, PreflightReport,
    RunManifest, RunReport, RunnerReport, SandboxReport, ScenarioReport,
};
use crate::scenario::{self, ScenarioContext};

const SCHEMA_VERSION: u32 = 1;
const RUNNER_CRATE: &str = "studiofs-e2e";
const RUNNER_MARKER_FILE: &str = ".studiofs-e2e-runner.json";
const INITIAL_CLIENTS: &[&str] = &["client-a", "client-b"];

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanRequest {
    pub root: PathBuf,
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
    #[error("run ID must not be empty")]
    EmptyRunId,
    #[error("unsafe cleanup root: {0}")]
    UnsafeCleanupRoot(String),
    #[error("runner I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("runner JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("scenario hook failed: {0}")]
    Scenario(#[from] scenario::ScenarioError),
}

#[derive(Debug)]
struct ScenarioLifecycleOutcome {
    report: ScenarioReport,
    outcome: ScenarioOutcome,
}

pub fn run(request: RunRequest) -> Result<RunOutcome, RunnerError> {
    let started_instant = Instant::now();
    let started_at = timestamp();
    let root = absolutize(&request.root)?;
    let run_id = match request.run_id {
        Some(run_id) if run_id.trim().is_empty() => return Err(RunnerError::EmptyRunId),
        Some(run_id) => run_id,
        None => generated_run_id(),
    };
    let scenario_names = if request.scenarios.is_empty() {
        vec![scenario::BASIC_MULTI_CLIENT_SMOKE.to_string()]
    } else {
        request.scenarios
    };
    let sandbox = root.join(&run_id);
    let selected_platform = request.platform.resolved().to_string();

    create_sandbox_layout(&sandbox, &scenario_names)?;
    let manifest = build_manifest(ManifestInput {
        run_id: &run_id,
        seed: request.seed,
        sandbox: &sandbox,
        scenarios: &scenario_names,
        retention: request.retention,
        selected_platform: &selected_platform,
        studiofs_bin: request.studiofs_bin.as_deref(),
        port_range: request.port_range.as_deref(),
        timeout: request.timeout.as_deref(),
        created_at: &started_at,
    });
    write_pretty_json(&sandbox.join("run.json"), &manifest)?;
    write_runner_marker(&sandbox, &run_id)?;
    write_log_files(&sandbox)?;
    write_pretty_json(
        &sandbox.join("ports.json"),
        &serde_json::json!({ "ports": [] }),
    )?;

    let preflight = PreflightReport {
        status: RunStatus::Passed,
        checks: vec![PreflightCheckReport {
            name: "runner-skeleton".to_string(),
            status: RunStatus::Passed,
            detail: "placeholder scenario does not start Docker or mount processes".to_string(),
        }],
    };
    write_pretty_json(&sandbox.join("preflight.json"), &preflight)?;

    let events_path = sandbox.join("events.ndjson");
    record_event(&events_path, &run_id, None, "run", "started")?;

    let mut scenario_reports = Vec::new();
    let mut scenario_outcomes = Vec::new();
    for requested_name in scenario_names {
        let Some(scenario) = scenario::resolve_scenario(&requested_name) else {
            return Err(RunnerError::UnknownScenario(requested_name));
        };
        let definition = scenario.definition();
        let context = ScenarioContext {
            run_id: run_id.clone(),
            seed: request.seed,
            sandbox: sandbox.clone(),
            scenario_dir: sandbox
                .join("artifacts")
                .join("scenarios")
                .join(definition.name),
        };

        let lifecycle = run_scenario_lifecycle(&events_path, &run_id, scenario.as_ref(), &context)?;
        scenario_reports.push(lifecycle.report);
        scenario_outcomes.push(lifecycle.outcome);
    }

    record_event(&events_path, &run_id, None, "run", "finished")?;

    let cleanup = CleanupReport {
        status: RunStatus::Passed,
        actions: Vec::new(),
        errors: Vec::new(),
    };
    write_pretty_json(&sandbox.join("cleanup.json"), &cleanup)?;

    let finished_at = timestamp();
    let report = RunReport {
        schema_version: SCHEMA_VERSION,
        run_id: run_id.clone(),
        seed: request.seed,
        status: RunStatus::Passed,
        exit_code: 0,
        started_at,
        finished_at,
        duration_ms: duration_millis(started_instant),
        runner: RunnerReport {
            crate_name: RUNNER_CRATE.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: option_env!("GITHUB_SHA").unwrap_or("unknown").to_string(),
        },
        host: HostReport {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            platform: selected_platform,
        },
        sandbox: SandboxReport {
            root: sandbox.display().to_string(),
            retention: request.retention,
        },
        preflight,
        scenarios: scenario_reports,
        artifacts: required_artifacts(&scenario_outcomes),
        cleanup,
    };

    let report_path = sandbox.join("report.json");
    write_pretty_json(&report_path, &report)?;
    if let Some(copy_target) = request.report {
        write_pretty_json(&copy_target, &report)?;
    }

    Ok(RunOutcome {
        run_id,
        status: RunStatus::Passed,
        exit_code: 0,
        sandbox,
        report_path,
        scenarios: scenario_outcomes,
    })
}

pub fn clean(request: CleanRequest) -> Result<Vec<PathBuf>, RunnerError> {
    if !request.root.exists() {
        return Ok(Vec::new());
    }
    let root = canonicalize_cleanup_root(&request.root)?;
    let mut removed = Vec::new();
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path();
        if !is_runner_owned_sandbox(&path)? {
            continue;
        }
        let canonical = path.canonicalize()?;
        if !canonical.starts_with(&root) {
            return Err(RunnerError::UnsafeCleanupRoot(format!(
                "{} escapes {}",
                canonical.display(),
                root.display()
            )));
        }
        let name = entry.file_name();
        fs::remove_dir_all(&path)?;
        removed.push(PathBuf::from(name));
    }
    removed.sort();
    Ok(removed)
}

fn create_sandbox_layout(sandbox: &Path, scenarios: &[String]) -> Result<(), RunnerError> {
    fs::create_dir_all(sandbox)?;
    fs::create_dir_all(sandbox.join("artifacts/docker"))?;
    fs::create_dir_all(sandbox.join("artifacts/commands"))?;
    for client in INITIAL_CLIENTS {
        fs::create_dir_all(
            sandbox
                .join("artifacts/clients")
                .join(client)
                .join("client-root"),
        )?;
        fs::create_dir_all(sandbox.join("artifacts/clients").join(client).join("mount"))?;
    }
    for scenario in scenarios {
        let name = scenario::resolve_scenario(scenario)
            .map(|scenario| scenario.definition().name)
            .unwrap_or(scenario);
        fs::create_dir_all(sandbox.join("artifacts/scenarios").join(name))?;
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
    studiofs_bin: Option<&'a Path>,
    port_range: Option<&'a str>,
    timeout: Option<&'a str>,
    created_at: &'a str,
}

fn build_manifest(input: ManifestInput<'_>) -> RunManifest {
    let explicit_client_roots: Vec<_> = INITIAL_CLIENTS
        .iter()
        .map(|client| {
            input
                .sandbox
                .join("artifacts/clients")
                .join(client)
                .join("client-root")
                .display()
                .to_string()
        })
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
) -> Result<ScenarioLifecycleOutcome, RunnerError> {
    let definition = scenario.definition();
    let started_at = timestamp();
    let started_instant = Instant::now();

    let result = run_scenario_body(events_path, run_id, scenario, context);
    let cleanup_result = run_hook(events_path, run_id, definition.name, "cleanup", || {
        scenario.cleanup(context)
    });

    match (result, cleanup_result) {
        (Ok(artifacts), Ok(())) => {
            let finished_at = timestamp();
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
            })
        }
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup)) | (Err(_), Err(cleanup)) => Err(cleanup),
    }
}

fn run_scenario_body(
    events_path: &Path,
    run_id: &str,
    scenario: &dyn scenario::Scenario,
    context: &ScenarioContext,
) -> Result<Vec<String>, RunnerError> {
    let definition = scenario.definition();
    run_hook(events_path, run_id, definition.name, "setup", || {
        scenario.setup(context)
    })?;
    run_hook(events_path, run_id, definition.name, "run", || {
        scenario.run(context)
    })?;
    run_hook(events_path, run_id, definition.name, "validate", || {
        scenario.validate(context)
    })
}

fn run_hook<T>(
    events_path: &Path,
    run_id: &str,
    scenario: &str,
    hook: &str,
    f: impl FnOnce() -> Result<T, scenario::ScenarioError>,
) -> Result<T, RunnerError> {
    record_event(events_path, run_id, Some(scenario), hook, "started")?;
    let result = f();
    record_event(
        events_path,
        run_id,
        Some(scenario),
        hook,
        if result.is_ok() { "finished" } else { "failed" },
    )?;
    Ok(result?)
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

fn artifact_name(path: &str) -> String {
    let normalized = path.replace('\\', "/");
    if normalized.trim_matches('/').is_empty() {
        "artifact".to_string()
    } else {
        normalized
    }
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
        };
        let scenario = FailingValidateScenario::new();

        let result = run_scenario_lifecycle(&events_path, "run-1", &scenario, &context);

        assert!(result.is_err());
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
}

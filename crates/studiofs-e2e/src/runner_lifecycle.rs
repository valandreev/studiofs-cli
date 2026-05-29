//! Process-lifecycle supervision for mounted E2E scenario children.

use std::fs::{self, OpenOptions};
use std::io::{IsTerminal as _, Write as _};
use std::path::Path;
use std::time::Duration;

use crate::infra::{CommandExecutor, CommandSpec, InfraMode, command_streams_output};
use crate::report::ScenarioReport;
use crate::runner::{
    PlatformSelection, ProgressStatus, RunnerError, SCENARIO_CHILD_STDERR_FILE,
    SCENARIO_CHILD_STDOUT_FILE, ScenarioOutcome, ScenarioProgress, ScenarioStatus,
    colorize_progress_statuses, command_failure_detail, emit_progress, record_event,
    record_event_with_detail, scenario_artifact_path, scenario_child_report_path, timestamp,
};
use crate::scenario;

pub(crate) const DEFAULT_SCENARIO_CHILD_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
pub(crate) struct ScenarioLifecycleOutcome {
    pub(crate) report: ScenarioReport,
    pub(crate) outcome: ScenarioOutcome,
    pub(crate) cleanup_error: Option<String>,
}

pub(crate) struct SupervisedScenarioInput<'a> {
    pub(crate) events_path: &'a Path,
    pub(crate) run_id: &'a str,
    pub(crate) root: &'a Path,
    pub(crate) sandbox: &'a Path,
    pub(crate) request_seed: u64,
    pub(crate) request_platform: PlatformSelection,
    pub(crate) request_infra: InfraMode,
    pub(crate) request_timeout: Option<&'a str>,
    pub(crate) definition: scenario::ScenarioDefinition,
    pub(crate) scenario_progress: ScenarioProgress,
    pub(crate) executor: &'a dyn CommandExecutor,
}

pub(crate) fn run_supervised_scenario(
    input: SupervisedScenarioInput<'_>,
) -> Result<ScenarioLifecycleOutcome, RunnerError> {
    record_event(
        input.events_path,
        input.run_id,
        Some(input.definition.name),
        "scenario_child",
        "started",
    )?;
    emit_progress(
        input.run_id,
        input.definition.name,
        Some(input.scenario_progress),
        "scenario_child",
        ProgressStatus::Started,
    );
    let command = scenario_child_command(&input)?;
    let child_report_path = scenario_child_report_path(input.sandbox, input.definition.name);
    match input.executor.run(&command) {
        Ok(output) => {
            if !command_streams_output(&command) {
                relay_child_output(&output);
            }
            append_child_output(input.sandbox, input.definition.name, &output)?;
            let mut lifecycle = match read_scenario_child_report(&child_report_path) {
                Ok(lifecycle) => lifecycle,
                Err(err) if output.status != 0 => failed_scenario_child(
                    input.definition.name,
                    input.request_seed,
                    command_failure_detail(output.status, &output.stderr),
                    Some(err.to_string()),
                    true,
                ),
                Err(err) => return Err(err),
            };
            if output.status != 0 && lifecycle.report.status == ScenarioStatus::Passed {
                lifecycle.report.status = ScenarioStatus::Failed;
                lifecycle.outcome.status = ScenarioStatus::Failed;
                lifecycle.report.error =
                    Some(command_failure_detail(output.status, &output.stderr));
            }
            record_event(
                input.events_path,
                input.run_id,
                Some(input.definition.name),
                "scenario_child",
                if lifecycle.outcome.status == ScenarioStatus::Passed {
                    "finished"
                } else {
                    "failed"
                },
            )?;
            emit_progress(
                input.run_id,
                input.definition.name,
                Some(input.scenario_progress),
                "scenario_child",
                if lifecycle.outcome.status == ScenarioStatus::Passed {
                    ProgressStatus::Passed
                } else {
                    ProgressStatus::Failed
                },
            );
            Ok(lifecycle)
        }
        Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {
            let lifecycle =
                timed_out_scenario_child(input.definition.name, input.request_seed, &err);
            record_event_with_detail(
                input.events_path,
                input.run_id,
                Some(input.definition.name),
                "scenario_child",
                "timed_out",
                err.to_string(),
            )?;
            emit_progress(
                input.run_id,
                input.definition.name,
                Some(input.scenario_progress),
                "scenario_child",
                ProgressStatus::Failed,
            );
            Ok(lifecycle)
        }
        Err(err) => Err(RunnerError::Io(err)),
    }
}

fn scenario_child_command(input: &SupervisedScenarioInput<'_>) -> Result<CommandSpec, RunnerError> {
    Ok(CommandSpec {
        program: std::env::current_exe()?.display().to_string(),
        args: vec![
            "run-one".to_string(),
            "--scenario".to_string(),
            input.definition.name.to_string(),
            "--root".to_string(),
            input.root.display().to_string(),
            "--run-id".to_string(),
            input.run_id.to_string(),
            "--seed".to_string(),
            input.request_seed.to_string(),
            "--platform".to_string(),
            input.request_platform.as_cli_value().to_string(),
            "--infra".to_string(),
            infra_cli_value(input.request_infra).to_string(),
        ],
        env: vec![
            ("STUDIOFS_E2E_SCENARIO_CHILD".to_string(), "1".to_string()),
            ("STUDIOFS_E2E_STREAM_OUTPUT".to_string(), "1".to_string()),
        ],
        cwd: None,
        timeout: Some(scenario_child_timeout(input.request_timeout)?),
    })
}

fn scenario_child_timeout(request_timeout: Option<&str>) -> Result<Duration, RunnerError> {
    let Some(request_timeout) = request_timeout else {
        return Ok(DEFAULT_SCENARIO_CHILD_TIMEOUT);
    };
    parse_timeout_duration(request_timeout).ok_or_else(|| RunnerError::InvalidTimeout {
        value: request_timeout.to_string(),
    })
}

fn infra_cli_value(infra: InfraMode) -> &'static str {
    match infra {
        InfraMode::Disabled => "disabled",
        InfraMode::Compose => "compose",
        InfraMode::Env => "env",
    }
}

fn parse_timeout_duration(value: &str) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Keep "ms" before "s": milliseconds also end with the seconds suffix.
    if let Some(ms) = trimmed.strip_suffix("ms") {
        return ms.trim().parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(seconds) = trimmed.strip_suffix('s') {
        return seconds.trim().parse::<u64>().ok().map(Duration::from_secs);
    }
    if let Some(minutes) = trimmed.strip_suffix('m') {
        return minutes
            .trim()
            .parse::<u64>()
            .ok()
            .and_then(|minutes| minutes.checked_mul(60))
            .map(Duration::from_secs);
    }
    trimmed.parse::<u64>().ok().map(Duration::from_secs)
}

fn read_scenario_child_report(path: &Path) -> Result<ScenarioLifecycleOutcome, RunnerError> {
    let child: crate::runner::ScenarioChildReport = serde_json::from_slice(&fs::read(path)?)?;
    Ok(ScenarioLifecycleOutcome {
        outcome: ScenarioOutcome {
            name: child.scenario.name.clone(),
            status: child.scenario.status,
            artifacts: child.scenario.artifacts.clone(),
        },
        report: child.scenario,
        cleanup_error: child.cleanup_error,
    })
}

fn timed_out_scenario_child(
    scenario: &str,
    seed: u64,
    err: &std::io::Error,
) -> ScenarioLifecycleOutcome {
    failed_scenario_child(scenario, seed, err.to_string(), None, false)
        .with_status(ScenarioStatus::TimedOut)
}

pub(crate) fn failed_scenario_child(
    scenario: &str,
    seed: u64,
    error: String,
    report_error: Option<String>,
    logs_written: bool,
) -> ScenarioLifecycleOutcome {
    let started_at = timestamp();
    let finished_at = timestamp();
    let mut artifacts = Vec::new();
    if logs_written {
        artifacts.push(scenario_artifact_path(scenario, SCENARIO_CHILD_STDERR_FILE));
    }
    if logs_written && report_error.is_some() {
        artifacts.push(scenario_artifact_path(scenario, SCENARIO_CHILD_STDOUT_FILE));
    }
    let error = match report_error {
        Some(report_err) => format!("{error}; child report unavailable: {report_err}"),
        None => error,
    };
    ScenarioLifecycleOutcome {
        report: ScenarioReport {
            name: scenario.to_string(),
            seed,
            status: ScenarioStatus::Failed,
            started_at,
            finished_at,
            duration_ms: 0,
            artifacts: artifacts.clone(),
            error: Some(error.clone()),
        },
        outcome: ScenarioOutcome {
            name: scenario.to_string(),
            status: ScenarioStatus::Failed,
            artifacts,
        },
        cleanup_error: Some(error),
    }
}

impl ScenarioLifecycleOutcome {
    fn with_status(mut self, status: ScenarioStatus) -> Self {
        self.report.status = status;
        self.outcome.status = status;
        self
    }
}

fn relay_child_output(output: &crate::infra::CommandOutput) {
    if !output.stdout.is_empty() {
        print!("{}", output.stdout);
        let _ = std::io::stdout().flush();
    }
    if !output.stderr.is_empty() {
        let stderr = if std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none() {
            colorize_progress_statuses(&output.stderr)
        } else {
            output.stderr.clone()
        };
        eprint!("{stderr}");
        let _ = std::io::stderr().flush();
    }
}

fn append_child_output(
    sandbox: &Path,
    scenario: &str,
    output: &crate::infra::CommandOutput,
) -> Result<(), RunnerError> {
    let scenario_dir = sandbox.join("artifacts").join("scenarios").join(scenario);
    fs::create_dir_all(&scenario_dir)?;
    append_text(
        &scenario_dir.join(SCENARIO_CHILD_STDOUT_FILE),
        &output.stdout,
    )?;
    append_text(
        &scenario_dir.join(SCENARIO_CHILD_STDERR_FILE),
        &output.stderr,
    )?;
    append_text(&sandbox.join("stdout.log"), &output.stdout)?;
    append_text(&sandbox.join("stderr.log"), &output.stderr)?;
    Ok(())
}

fn append_text(path: &Path, text: &str) -> Result<(), RunnerError> {
    if text.is_empty() {
        return Ok(());
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(text.as_bytes())?;
    file.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parse_timeout_duration_accepts_supported_units_in_suffix_order() {
        assert_eq!(
            parse_timeout_duration("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_timeout_duration("10s"), Some(Duration::from_secs(10)));
        assert_eq!(parse_timeout_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_timeout_duration("7"), Some(Duration::from_secs(7)));
    }

    #[test]
    fn scenario_child_command_rejects_invalid_timeout_format() {
        let definition = scenario::resolve_scenario(scenario::TEST_PASSING)
            .unwrap()
            .definition();
        let temp = tempfile::tempdir().unwrap();
        let events_path = temp.path().join("events.ndjson");
        let executor = crate::infra::SystemCommandExecutor;

        let err = scenario_child_command(&SupervisedScenarioInput {
            events_path: &events_path,
            run_id: "invalid-timeout-run",
            root: temp.path(),
            sandbox: temp.path(),
            request_seed: 438,
            request_platform: PlatformSelection::Auto,
            request_infra: InfraMode::Disabled,
            request_timeout: Some("30x"),
            definition,
            scenario_progress: ScenarioProgress { index: 1, total: 1 },
            executor: &executor,
        })
        .unwrap_err();

        assert!(matches!(err, RunnerError::InvalidTimeout { .. }));
        assert!(err.to_string().contains("30x"));
    }

    #[test]
    fn timed_out_scenario_child_does_not_advertise_unwritten_logs() {
        let err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");

        let lifecycle = timed_out_scenario_child(scenario::TEST_PASSING, 438, &err);

        assert_eq!(lifecycle.report.status, ScenarioStatus::TimedOut);
        assert!(lifecycle.report.artifacts.is_empty());
        assert!(lifecycle.outcome.artifacts.is_empty());
    }
}

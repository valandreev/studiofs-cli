//! Local First baseline gate orchestration for the accepted mounted scenario set.

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};

use crate::infra::{CommandExecutor, InfraMode, SystemCommandExecutor};
use crate::runner::{self, PlatformSelection, RetentionPolicy, RunOutcome, RunRequest, RunStatus};
use crate::scenario;

/// Baseline scenarios selected for the accepted mounted Local First gate run.
///
/// The order is intentionally fixed and spans the eight accepted scenario
/// entry points in `studiofs-scenario`.
pub const SCENARIOS_1_THROUGH_8: [&str; 8] = [
    scenario::BASIC_MULTI_CLIENT_SMOKE,
    scenario::POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
    scenario::RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
    scenario::MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
    scenario::MOUNTED_CRASH_BEFORE_WRITE_ACCEPTANCE,
    scenario::MOUNTED_CRASH_AFTER_PARTIAL_WRITE_ACCEPTANCE,
    scenario::MOUNTED_CRASH_AFTER_FULL_WRITE_ACCEPTANCE,
    scenario::COLLISION_RENAME_VS_RENAME,
];

/// Baseline gate input parameters shared across all executed iterations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineGateRequest {
    /// Root directory under which run artifacts and scenario state are created.
    pub root: PathBuf,
    /// Prefix used for generated run identifiers and report naming.
    pub run_id_prefix: String,
    /// Number of baseline iterations to execute.
    pub iterations: usize,
    /// Seed value shared across all iterations and scenarios.
    pub seed: u64,
    /// Path to the `studiofs` binary used for runs.
    pub studiofs_bin: PathBuf,
    /// Target platform selection used for scenario execution.
    pub platform: PlatformSelection,
    /// Retention policy applied to artifacts and generated reports.
    pub retention: RetentionPolicy,
    /// Optional port range for ephemeral infra services.
    pub port_range: Option<String>,
    /// Optional timeout override forwarded to scenario runs.
    pub timeout: Option<String>,
}

/// Aggregated outcomes of all baseline gate iterations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineGateOutcome {
    /// Outcomes for each executed baseline iteration.
    pub runs: Vec<RunOutcome>,
}

impl BaselineGateOutcome {
    /// Aggregates run status; returns `RunStatus::Passed` only when all runs pass.
    pub fn status(&self) -> RunStatus {
        if self.runs.iter().all(|run| run.status == RunStatus::Passed) {
            RunStatus::Passed
        } else {
            RunStatus::Failed
        }
    }

    /// Returns a conventional process exit code for the aggregate outcome.
    ///
    /// `0` indicates all runs passed; `1` indicates any failure.
    pub fn exit_code(&self) -> i32 {
        if self.status() == RunStatus::Passed {
            0
        } else {
            1
        }
    }
}

/// Runs the baseline gate using the default system command executor.
///
/// This is a convenience entry point for the common case where external
/// scenario and infra execution is delegated to `SystemCommandExecutor`.
pub fn run(request: BaselineGateRequest) -> Result<BaselineGateOutcome> {
    let executor = SystemCommandExecutor;
    run_with_executor(request, &executor)
}

/// Runs the baseline gate with an explicit command executor.
///
/// This entry point is useful in tests and alternate transport layers.
pub fn run_with_executor(
    request: BaselineGateRequest,
    executor: &dyn CommandExecutor,
) -> Result<BaselineGateOutcome> {
    if request.iterations == 0 {
        bail!("baseline gate iterations must be greater than zero");
    }

    let mut runs = Vec::with_capacity(request.iterations);
    for index in 0..request.iterations {
        let run_id = if request.iterations == 1 {
            request.run_id_prefix.clone()
        } else {
            format!("{}-{:02}", request.run_id_prefix, index + 1)
        };
        let outcome = runner::run_with_executor(
            RunRequest {
                root: request.root.clone(),
                run_id: Some(run_id),
                scenarios: SCENARIOS_1_THROUGH_8
                    .iter()
                    .map(|scenario| (*scenario).to_string())
                    .collect(),
                seed: request.seed,
                studiofs_bin: Some(request.studiofs_bin.clone()),
                platform: request.platform,
                retention: request.retention,
                report: None,
                port_range: request.port_range.clone(),
                timeout: request.timeout.clone(),
                infra: InfraMode::Compose,
            },
            executor,
        )
        .context("failed to run Local First baseline gate iteration")?;
        runs.push(outcome);
    }

    Ok(BaselineGateOutcome { runs })
}

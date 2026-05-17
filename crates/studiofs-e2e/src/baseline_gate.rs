//! Local First baseline gate orchestration for the accepted mounted scenario set.

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};

use crate::infra::{CommandExecutor, InfraMode, SystemCommandExecutor};
use crate::runner::{self, PlatformSelection, RetentionPolicy, RunOutcome, RunRequest, RunStatus};
use crate::scenario;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineGateRequest {
    pub root: PathBuf,
    pub run_id_prefix: String,
    pub iterations: usize,
    pub seed: u64,
    pub studiofs_bin: PathBuf,
    pub platform: PlatformSelection,
    pub retention: RetentionPolicy,
    pub port_range: Option<String>,
    pub timeout: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaselineGateOutcome {
    pub runs: Vec<RunOutcome>,
}

impl BaselineGateOutcome {
    pub fn status(&self) -> RunStatus {
        if self.runs.iter().all(|run| run.status == RunStatus::Passed) {
            RunStatus::Passed
        } else {
            RunStatus::Failed
        }
    }

    pub fn exit_code(&self) -> i32 {
        if self.status() == RunStatus::Passed {
            0
        } else {
            1
        }
    }
}

pub fn run(request: BaselineGateRequest) -> Result<BaselineGateOutcome> {
    let executor = SystemCommandExecutor;
    run_with_executor(request, &executor)
}

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

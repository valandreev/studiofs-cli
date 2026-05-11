//! Local First baseline gate orchestration for the accepted mounted scenario set.

use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};

use crate::infra::{CommandExecutor, CommandSpec, InfraMode, SystemCommandExecutor};
use crate::runner::{self, PlatformSelection, RetentionPolicy, RunOutcome, RunRequest, RunStatus};
use crate::scenario;

pub const SCENARIOS_1_THROUGH_8: [&str; 8] = [
    scenario::BASIC_MULTI_CLIENT_SMOKE,
    scenario::RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
    scenario::COLLISION_RENAME_VS_RENAME,
    scenario::POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
    scenario::MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
    scenario::COLLISION_RENAME_VS_UNLINK,
    scenario::COLLISION_UNLINK_VS_APPEND,
    scenario::COLLISION_OVERWRITE_VS_READ,
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

    check_windows_process_cleanliness(request.platform, &request.root, executor)?;

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

fn check_windows_process_cleanliness(
    platform: PlatformSelection,
    root: &Path,
    executor: &dyn CommandExecutor,
) -> Result<()> {
    if platform != PlatformSelection::WindowsWinfsp {
        return Ok(());
    }

    let output = executor.run(&windows_stale_process_command(root))?;

    if output.status != 0 {
        bail!(
            "failed to check stale StudioFS processes before baseline gate: {}",
            output.stderr.trim()
        );
    }
    let stale_processes = output.stdout.trim();
    if !stale_processes.is_empty() {
        bail!(
            "stale runner-owned StudioFS processes are still running before baseline gate:\n{}",
            stale_processes
        );
    }
    Ok(())
}

fn windows_stale_process_command(root: &Path) -> CommandSpec {
    let root = absolute_without_requiring_exists(root);
    CommandSpec {
        program: "powershell".to_string(),
        args: vec![
            "-NoProfile".to_string(),
            "-Command".to_string(),
            "$names=@('studiofs.exe','studiofs-e2e.exe'); \
             $selfPid=[int]$env:STUDIOFS_E2E_BASELINE_SELF_PID; \
             $rootPattern=[regex]::Escape($env:STUDIOFS_E2E_BASELINE_ROOT); \
             Get-CimInstance Win32_Process | \
             Where-Object { \
               $cmd=$_.CommandLine -as [string]; \
               $names -contains $_.Name -and $_.ProcessId -ne $selfPid -and \
               ($cmd -match $rootPattern -or $cmd -match '__mounted-chaos-op|\\smount\\s') \
             } | \
             ForEach-Object { \"$($_.ProcessId) $($_.Name) $($_.CommandLine)\" }"
                .to_string(),
        ],
        env: vec![
            (
                "STUDIOFS_E2E_BASELINE_ROOT".to_string(),
                root.display().to_string(),
            ),
            (
                "STUDIOFS_E2E_BASELINE_SELF_PID".to_string(),
                std::process::id().to_string(),
            ),
        ],
        cwd: None,
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::windows_stale_process_command;

    #[test]
    fn windows_process_check_uses_requested_root_instead_of_default_root() {
        let command = windows_stale_process_command(Path::new("D:\\custom-e2e-root"));

        assert!(command.env.contains(&(
            "STUDIOFS_E2E_BASELINE_ROOT".to_string(),
            "D:\\custom-e2e-root".to_string()
        )));
        assert!(!command.args.join(" ").contains("target\\\\e2e-runs"));
    }
}

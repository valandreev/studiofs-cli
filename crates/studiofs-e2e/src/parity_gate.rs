//! macFUSE JuiceFS parity benchmark gate orchestration.

use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};

use crate::infra::{CommandExecutor, InfraMode, SystemCommandExecutor};
use crate::runner::{self, PlatformSelection, RetentionPolicy, RunOutcome, RunRequest, RunStatus};
use crate::scenario;

/// Mounted scenarios selected for the SFS-495 macFUSE/JuiceFS parity gate.
pub const MACFUSE_JUICEFS_PARITY_SCENARIOS: [&str; 2] = [
    scenario::MOUNTED_FUSE_WRITE_BASELINE,
    scenario::MOUNTED_PERFORMANCE_PROBE,
];

/// Parity gate input parameters shared across all executed iterations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacfuseJuicefsParityGateRequest {
    /// Root directory under which run artifacts and scenario state are created.
    pub root: PathBuf,
    /// Prefix used for generated run identifiers and report naming.
    pub run_id_prefix: String,
    /// Number of parity iterations to execute.
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

/// Aggregated outcomes of all macFUSE parity gate iterations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacfuseJuicefsParityGateOutcome {
    /// Outcomes for each executed parity iteration.
    pub runs: Vec<RunOutcome>,
}

impl MacfuseJuicefsParityGateOutcome {
    /// Aggregates run status; returns `RunStatus::Passed` only when all runs pass.
    pub fn status(&self) -> RunStatus {
        if self.runs.iter().all(|run| run.status == RunStatus::Passed) {
            RunStatus::Passed
        } else {
            RunStatus::Failed
        }
    }

    /// Returns a conventional process exit code for the aggregate outcome.
    pub fn exit_code(&self) -> i32 {
        self.runs
            .iter()
            .find(|run| run.status != RunStatus::Passed)
            .map_or(0, |run| run.exit_code)
    }
}

/// Runs the parity gate using the default system command executor.
pub fn run(request: MacfuseJuicefsParityGateRequest) -> Result<MacfuseJuicefsParityGateOutcome> {
    let executor = SystemCommandExecutor;
    run_with_executor(request, &executor)
}

/// Runs the parity gate with an explicit command executor.
pub fn run_with_executor(
    request: MacfuseJuicefsParityGateRequest,
    executor: &dyn CommandExecutor,
) -> Result<MacfuseJuicefsParityGateOutcome> {
    if request.iterations == 0 {
        bail!("macFUSE JuiceFS parity gate iterations must be greater than zero");
    }
    if !platform_resolves_to_macos_fuse(request.platform) {
        bail!("macFUSE JuiceFS parity gate requires the macos-fuse platform");
    }

    let scenarios = MACFUSE_JUICEFS_PARITY_SCENARIOS
        .iter()
        .map(|scenario| (*scenario).to_string())
        .collect::<Vec<_>>();

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
                scenarios: scenarios.clone(),
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
        .context("failed to run macFUSE JuiceFS parity gate iteration")?;
        runs.push(outcome);
    }

    Ok(MacfuseJuicefsParityGateOutcome { runs })
}

fn platform_resolves_to_macos_fuse(platform: PlatformSelection) -> bool {
    match platform {
        PlatformSelection::MacosFuse => true,
        PlatformSelection::Auto => cfg!(target_os = "macos"),
        PlatformSelection::LinuxFuse | PlatformSelection::WindowsWinfsp => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::{CommandOutput, CommandSpec};

    struct PanicExecutor;

    impl CommandExecutor for PanicExecutor {
        fn run(&self, _command: &CommandSpec) -> std::io::Result<CommandOutput> {
            panic!("invalid parity gate platform should fail before command execution");
        }
    }

    #[test]
    fn parity_gate_rejects_non_macos_platform_before_running() {
        let error = run_with_executor(
            request_with_platform(PlatformSelection::LinuxFuse),
            &PanicExecutor,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("macFUSE JuiceFS parity gate requires the macos-fuse platform")
        );
    }

    #[test]
    fn parity_gate_auto_platform_only_resolves_on_macos() {
        assert_eq!(
            platform_resolves_to_macos_fuse(PlatformSelection::Auto),
            cfg!(target_os = "macos")
        );
    }

    #[test]
    fn parity_gate_exit_code_preserves_first_failing_runner_code() {
        let outcome = MacfuseJuicefsParityGateOutcome {
            runs: vec![
                run_outcome("preflight-failed", RunStatus::PreflightFailed, 2),
                run_outcome("cleanup-failed", RunStatus::CleanupFailed, 4),
            ],
        };

        assert_eq!(outcome.exit_code(), 2);
    }

    fn request_with_platform(platform: PlatformSelection) -> MacfuseJuicefsParityGateRequest {
        MacfuseJuicefsParityGateRequest {
            root: PathBuf::from("target/e2e-runs"),
            run_id_prefix: "sfs495-test".to_string(),
            iterations: 1,
            seed: 495,
            studiofs_bin: PathBuf::from("target/debug/studiofs"),
            platform,
            retention: RetentionPolicy::OnFailure,
            port_range: None,
            timeout: None,
        }
    }

    fn run_outcome(run_id: &str, status: RunStatus, exit_code: i32) -> RunOutcome {
        RunOutcome {
            run_id: run_id.to_string(),
            status,
            exit_code,
            sandbox: PathBuf::from("target/e2e-runs").join(run_id),
            report_path: PathBuf::from("target/e2e-runs")
                .join(run_id)
                .join("report.json"),
            scenarios: Vec::new(),
        }
    }
}

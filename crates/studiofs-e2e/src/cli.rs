//! Command-line surface for the StudioFS real E2E runner.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use crate::infra::InfraMode;
use crate::runner::{
    self, CleanRequest, PlatformSelection, RetentionPolicy, RunOneRequest, RunRequest,
};
use crate::scenario;

#[derive(Parser, Debug)]
#[command(name = "studiofs-e2e")]
#[command(version)]
#[command(about = "Run StudioFS real mounted E2E scenarios")]
/// Struct $name.
pub struct Cli {
    #[command(subcommand)]
    /// Field $name.
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
/// Enum $name.
pub enum Commands {
    /// List available E2E scenarios.
    List,
    /// Run one or more E2E scenarios.
    Run(RunArgs),
    /// Remove runner-owned sandboxes from the run root.
    Clean(CleanArgs),
    /// Internal helper for one mounted chaos filesystem operation.
    #[command(name = "__mounted-chaos-op", hide = true)]
    MountedChaosOp(MountedChaosOpArgs),
    /// Internal supervised single-scenario runner.
    #[command(name = "__run-one", hide = true)]
    RunOne(RunOneArgs),
}

#[derive(Args, Debug)]
/// Struct $name.
pub struct RunArgs {
    /// Run every registered E2E scenario in one supervised run.
    #[arg(long)]
    pub all: bool,

    /// Scenario name, for example basic-multi-client-smoke.
    pub scenario: Option<String>,

    /// Additional scenario name. Repeat to select multiple scenarios.
    #[arg(long = "scenario")]
    pub scenarios: Vec<String>,

    /// Parent directory for per-run sandboxes.
    #[arg(long, default_value = "target/e2e-runs")]
    pub root: PathBuf,

    /// Stable run ID. If omitted, the runner generates one.
    #[arg(long)]
    pub run_id: Option<String>,

    /// Deterministic seed recorded in the run manifest and report.
    #[arg(long, default_value_t = 0)]
    pub seed: u64,

    /// Path to the StudioFS product binary under test.
    #[arg(long)]
    pub studiofs_bin: Option<PathBuf>,

    /// Mount backend selection for real mounted runs.
    #[arg(long, value_enum, default_value = "auto")]
    pub platform: PlatformSelection,

    /// Sandbox retention policy.
    #[arg(long, value_enum, default_value = "on-failure")]
    pub retain: RetentionPolicy,

    /// Optional copy target for the final JSON report.
    #[arg(long)]
    pub report: Option<PathBuf>,

    /// Optional deterministic host port pool, for example 29000-29100.
    #[arg(long)]
    pub port_range: Option<String>,

    /// Optional whole-run timeout, for example 500ms, 30s, 10m, or 1h.
    #[arg(long)]
    pub timeout: Option<String>,

    /// External infrastructure mode for the run.
    #[arg(long, value_enum, default_value = "compose")]
    pub infra: InfraMode,
}

#[derive(Args, Debug)]
/// Struct $name.
pub struct CleanArgs {
    /// Parent directory containing per-run sandboxes.
    #[arg(long, default_value = "target/e2e-runs")]
    pub root: PathBuf,
}

#[derive(Args, Debug)]
/// Struct $name.
pub struct MountedChaosOpArgs {
    /// Serialized mounted chaos task JSON.
    #[arg(long)]
    pub task: PathBuf,

    /// File created by the parent runner to release concurrent helpers.
    #[arg(long)]
    pub start_file: PathBuf,
}

#[derive(Args, Debug)]
/// Struct $name.
pub struct RunOneArgs {
    /// Scenario name to execute in this child process.
    pub scenario: String,

    /// Parent directory for per-run sandboxes.
    #[arg(long)]
    pub root: PathBuf,

    /// Existing run ID owned by the parent supervisor.
    #[arg(long)]
    pub run_id: String,

    /// Deterministic seed recorded in the scenario report.
    #[arg(long)]
    pub seed: u64,

    /// Path to the StudioFS product binary under test.
    #[arg(long)]
    pub studiofs_bin: Option<PathBuf>,

    /// External infrastructure mode for the run.
    #[arg(long, value_enum)]
    pub infra: InfraMode,
}

impl RunArgs {
    /// Function $name.
    pub fn scenario_names(&self) -> Vec<String> {
        if self.all {
            return scenario::available_scenarios()
                .into_iter()
                .map(|scenario| scenario.name.to_string())
                .collect();
        }

        let mut names = Vec::new();
        if let Some(scenario) = &self.scenario {
            names.push(scenario.clone());
        }
        names.extend(self.scenarios.iter().cloned());
        if names.is_empty() {
            names.push(scenario::BASIC_MULTI_CLIENT_SMOKE.to_string());
        }
        names
    }

    /// Function $name.
    pub fn into_request(self) -> RunRequest {
        let scenarios = self.scenario_names();
        RunRequest {
            root: self.root,
            run_id: self.run_id,
            scenarios,
            seed: self.seed,
            studiofs_bin: self.studiofs_bin,
            platform: self.platform,
            retention: self.retain,
            report: self.report,
            port_range: self.port_range,
            timeout: self.timeout,
            infra: self.infra,
        }
    }
}

/// Function $name.
pub fn dispatch(cli: Cli) -> Result<i32> {
    match cli.command {
        Commands::List => {
            for scenario in scenario::available_scenarios() {
                println!("{}\t{}", scenario.name, scenario.description);
            }
            Ok(0)
        }
        Commands::Run(args) => {
            let outcome = runner::run(args.into_request())?;
            println!(
                "run_id={} status={:?} report={}",
                outcome.run_id,
                outcome.status,
                outcome.report_path.display()
            );
            Ok(outcome.exit_code)
        }
        Commands::Clean(args) => {
            let outcome = runner::clean(CleanRequest { root: args.root })?;
            for path in outcome.removed {
                println!("{}", path.display());
            }
            for skipped in outcome.skipped {
                eprintln!("skipped {}: {}", skipped.path.display(), skipped.error);
            }
            Ok(0)
        }
        Commands::MountedChaosOp(args) => {
            scenario::run_mounted_chaos_operation_helper(&args.task, &args.start_file)?;
            Ok(0)
        }
        Commands::RunOne(args) => Ok(runner::run_one(RunOneRequest {
            root: args.root,
            run_id: args.run_id,
            scenario: args.scenario,
            seed: args.seed,
            studiofs_bin: args.studiofs_bin,
            infra: args.infra,
        })?),
    }
}

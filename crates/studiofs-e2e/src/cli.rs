//! Command-line surface for the StudioFS real E2E runner.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};

use crate::runner::{self, CleanRequest, PlatformSelection, RetentionPolicy, RunRequest};
use crate::scenario;

#[derive(Parser, Debug)]
#[command(name = "studiofs-e2e")]
#[command(version)]
#[command(about = "Run StudioFS real mounted E2E scenarios")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// List available E2E scenarios.
    List,
    /// Run one or more E2E scenarios.
    Run(RunArgs),
    /// Remove runner-owned sandboxes from the run root.
    Clean(CleanArgs),
}

#[derive(Args, Debug)]
pub struct RunArgs {
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

    /// Optional whole-run timeout string reserved for future hook enforcement.
    #[arg(long)]
    pub timeout: Option<String>,
}

#[derive(Args, Debug)]
pub struct CleanArgs {
    /// Parent directory containing per-run sandboxes.
    #[arg(long, default_value = "target/e2e-runs")]
    pub root: PathBuf,
}

impl RunArgs {
    pub fn scenario_names(&self) -> Vec<String> {
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
        }
    }
}

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
            let removed = runner::clean(CleanRequest { root: args.root })?;
            for path in removed {
                println!("{}", path.display());
            }
            Ok(0)
        }
    }
}

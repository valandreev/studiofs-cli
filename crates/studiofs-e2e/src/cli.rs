//! Command-line surface for the StudioFS real E2E runner.

use std::path::PathBuf;

use anyhow::Result;
use chrono::{SecondsFormat, Utc};
use clap::{Args, Parser, Subcommand};

use crate::baseline_gate::{self, BaselineGateRequest};
use crate::infra::InfraMode;
use crate::runner::{
    self, CleanRequest, PlatformSelection, RetentionPolicy, RunOneRequest, RunRequest,
};
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
    /// Run exactly one scenario inside an existing parent runner sandbox.
    #[command(hide = true)]
    RunOne(RunOneArgs),
    /// Run the SFS-405 Local First baseline gate for mounted scenarios 1-8.
    LocalFirstBaselineGate(BaselineGateArgs),
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

    /// External infrastructure mode for the run.
    #[arg(long, value_enum, default_value = "compose")]
    pub infra: InfraMode,
}

#[derive(Args, Debug)]
pub struct RunOneArgs {
    /// Scenario name to run inside the parent sandbox.
    #[arg(long)]
    pub scenario: String,

    /// Parent directory for per-run sandboxes.
    #[arg(long, default_value = "target/e2e-runs")]
    pub root: PathBuf,

    /// Existing parent run ID.
    #[arg(long)]
    pub run_id: String,

    /// Deterministic seed accepted for CLI symmetry; run.json remains authoritative.
    #[arg(long, default_value_t = 0)]
    pub seed: u64,

    /// Mount backend accepted for CLI symmetry; run.json remains authoritative.
    #[arg(long, value_enum, default_value = "auto")]
    pub platform: PlatformSelection,

    /// External infrastructure mode accepted for CLI symmetry; run.json remains authoritative.
    #[arg(long, value_enum, default_value = "compose")]
    pub infra: InfraMode,
}

#[derive(Args, Debug)]
pub struct CleanArgs {
    /// Parent directory containing per-run sandboxes.
    #[arg(long, default_value = "target/e2e-runs")]
    pub root: PathBuf,
}

#[derive(Args, Debug)]
pub struct BaselineGateArgs {
    /// Path to the StudioFS product binary under test.
    #[arg(long)]
    pub studiofs_bin: PathBuf,

    /// Parent directory for per-run sandboxes.
    #[arg(long, default_value = "target/e2e-runs")]
    pub root: PathBuf,

    /// Stable run ID prefix. Multiple iterations append -01, -02, ...
    #[arg(long, default_value = "local-first-baseline")]
    pub run_id_prefix: String,

    /// Number of repeated scenario 1-8 passes.
    #[arg(long, default_value_t = 1)]
    pub iterations: usize,

    /// Deterministic seed recorded in every run manifest and report.
    #[arg(long, default_value_t = 405)]
    pub seed: u64,

    /// Mount backend selection for the baseline gate.
    #[arg(long, value_enum, default_value = "windows-winfsp")]
    pub platform: PlatformSelection,

    /// Sandbox retention policy.
    #[arg(long, value_enum, default_value = "on-failure")]
    pub retain: RetentionPolicy,

    /// Optional deterministic host port pool, for example 29700-29800.
    #[arg(long)]
    pub port_range: Option<String>,

    /// Optional whole-run timeout string forwarded to each runner pass.
    #[arg(long)]
    pub timeout: Option<String>,
}

impl RunArgs {
    pub fn scenario_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        if let Some(scenario) = &self.scenario {
            names.extend(expand_scenario_selector(scenario));
        }
        names.extend(
            self.scenarios
                .iter()
                .flat_map(|scenario| expand_scenario_selector(scenario)),
        );
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
            infra: self.infra,
        }
    }
}

fn expand_scenario_selector(selector: &str) -> Vec<String> {
    let scenarios = scenario::available_scenarios();
    selector
        .split(',')
        .flat_map(|part| expand_scenario_selector_part(part.trim(), &scenarios))
        .collect()
}

fn expand_scenario_selector_part(
    part: &str,
    scenarios: &[scenario::ScenarioDefinition],
) -> Vec<String> {
    if part.is_empty() {
        return Vec::new();
    }
    if let Some((start, end)) = parse_scenario_range(part) {
        return (start..=end)
            .map(|index| scenario_name_by_one_based_index(index, scenarios))
            .collect();
    }
    if let Ok(index) = part.parse::<usize>() {
        return vec![scenario_name_by_one_based_index(index, scenarios)];
    }
    vec![part.to_string()]
}

fn parse_scenario_range(part: &str) -> Option<(usize, usize)> {
    let (start, end) = part.split_once('-')?;
    let start = start.trim().parse::<usize>().ok()?;
    let end = end.trim().parse::<usize>().ok()?;
    (start <= end).then_some((start, end))
}

fn scenario_name_by_one_based_index(
    index: usize,
    scenarios: &[scenario::ScenarioDefinition],
) -> String {
    scenarios
        .get(index.saturating_sub(1))
        .map(|scenario| scenario.name.to_string())
        .unwrap_or_else(|| index.to_string())
}

pub fn format_scenario_list_line(index: usize, scenario: &scenario::ScenarioDefinition) -> String {
    format!("{}\t{}", index + 1, scenario.description)
}

impl BaselineGateArgs {
    pub fn into_request(self) -> BaselineGateRequest {
        BaselineGateRequest {
            root: self.root,
            run_id_prefix: self.run_id_prefix,
            iterations: self.iterations,
            seed: self.seed,
            studiofs_bin: self.studiofs_bin,
            platform: self.platform,
            retention: self.retain,
            port_range: self.port_range,
            timeout: self.timeout,
        }
    }
}

pub fn dispatch(cli: Cli) -> Result<i32> {
    match cli.command {
        Commands::List => {
            for (index, scenario) in scenario::available_scenarios().iter().enumerate() {
                println!("{}", format_scenario_list_line(index, scenario));
            }
            Ok(0)
        }
        Commands::Run(args) => {
            let outcome = runner::run(args.into_request())?;
            println!(
                "timestamp={} run_id={} status={:?} report={}",
                timestamp(),
                outcome.run_id,
                outcome.status,
                outcome.report_path.display()
            );
            Ok(outcome.exit_code)
        }
        Commands::RunOne(args) => {
            let outcome = runner::run_one(RunOneRequest {
                root: args.root,
                run_id: args.run_id,
                scenario: args.scenario,
            })?;
            if should_print_run_one_summary(outcome.exit_code) {
                println!(
                    "timestamp={} run_id={} scenario={} status={:?} report={}",
                    timestamp(),
                    outcome.run_id,
                    outcome.scenario.name,
                    outcome.scenario.status,
                    outcome.report_path.display()
                );
            }
            Ok(outcome.exit_code)
        }
        Commands::LocalFirstBaselineGate(args) => {
            let outcome = baseline_gate::run(args.into_request())?;
            for run in &outcome.runs {
                println!(
                    "timestamp={} run_id={} status={:?} report={}",
                    timestamp(),
                    run.run_id,
                    run.status,
                    run.report_path.display()
                );
            }
            Ok(outcome.exit_code())
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
    }
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn should_print_run_one_summary(exit_code: i32) -> bool {
    exit_code != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hidden_run_one_subcommand_parses() {
        let cli = Cli::try_parse_from([
            "studiofs-e2e",
            "run-one",
            "--scenario",
            "basic-multi-client-smoke",
            "--root",
            "target/e2e-runs",
            "--run-id",
            "run-one-test",
            "--seed",
            "410",
            "--platform",
            "windows-winfsp",
            "--infra",
            "disabled",
        ])
        .expect("hidden run-one command should parse");

        let Commands::RunOne(args) = cli.command else {
            panic!("expected hidden run-one command");
        };
        assert_eq!(args.scenario, "basic-multi-client-smoke");
        assert_eq!(args.run_id, "run-one-test");
        assert_eq!(args.seed, 410);
        assert_eq!(args.infra, InfraMode::Disabled);
    }

    #[test]
    fn run_one_summary_is_suppressed_for_successful_child() {
        assert!(!should_print_run_one_summary(0));
        assert!(should_print_run_one_summary(1));
        assert!(should_print_run_one_summary(4));
    }
}

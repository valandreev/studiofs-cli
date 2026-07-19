//! Command-line surface for the StudioFS real E2E runner.

use std::path::PathBuf;

use anyhow::Result;
use chrono::{SecondsFormat, Utc};
use clap::{Args, Parser, Subcommand};

use crate::baseline_gate::{self, BaselineGateRequest};
use crate::infra::InfraMode;
use crate::process_diagnostics;
use crate::runner::{
    self, CleanRequest, PlatformSelection, RetentionPolicy, RunOneRequest, RunRequest,
};
use crate::scenario;

#[derive(Parser, Debug)]
#[command(name = "studiofs-e2e")]
#[command(version)]
#[command(about = "Run StudioFS real mounted E2E scenarios")]
/// Parsed CLI arguments for the `studiofs-e2e` binary.
pub struct Cli {
    #[command(subcommand)]
    /// Top-level command selected from the CLI.
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
/// High-level command modes supported by the CLI.
pub enum Commands {
    /// List available E2E scenarios.
    List,
    /// Run one or more E2E scenarios.
    Run(RunArgs),
    /// Run exactly one scenario inside an existing parent runner sandbox.
    #[command(hide = true)]
    RunOne(RunOneArgs),
    /// Execute one bounded mounted hotspot filesystem operation.
    #[command(hide = true)]
    HotspotOp(HotspotOpArgs),
    /// Execute one bounded mounted sparse filesystem operation.
    #[command(hide = true)]
    SparseOp(SparseOpArgs),
    /// Execute the mounted open-handle mutation sequence in a bounded helper.
    #[command(hide = true)]
    OpenHandleOp(OpenHandleOpArgs),
    /// Capture best-effort diagnostics for a process before forced termination.
    #[command(hide = true)]
    ProcessDump(ProcessDumpArgs),
    /// Run the SFS-405 Local First baseline gate for mounted scenarios 1-8.
    LocalFirstBaselineGate(BaselineGateArgs),
    /// Remove runner-owned sandboxes from the run root.
    Clean(CleanArgs),
}

#[derive(Args, Debug)]
/// Options used to run one or many E2E scenarios.
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
/// Options used to run a single scenario in a pre-existing parent sandbox.
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
/// Hidden helper arguments for one mounted hotspot filesystem operation.
pub struct HotspotOpArgs {
    /// Operation name: mkdir, write, append, rename, or readdir.
    #[arg(long)]
    pub operation: String,

    /// Mounted root for the client that executes the operation.
    #[arg(long)]
    pub mount_point: PathBuf,

    /// Relative path under the mount point.
    #[arg(long)]
    pub path: String,

    /// Relative destination path for rename operations.
    #[arg(long)]
    pub target: Option<String>,

    /// Payload file for write and append operations.
    #[arg(long)]
    pub payload: Option<PathBuf>,

    /// Parent sandbox path used only so stale-process cleanup can identify helpers.
    #[arg(long, hide = true)]
    pub sandbox: Option<PathBuf>,

    /// Per-operation progress JSONL path used to diagnose bounded helper timeouts.
    #[arg(long, hide = true)]
    pub progress: Option<PathBuf>,

    /// Internal operation timeout so the helper can cancel mounted I/O before parent kill.
    #[arg(long, hide = true)]
    pub timeout_ms: Option<u64>,

    /// Keep the helper process alive after a successful operation.
    #[arg(long, hide = true)]
    pub hold_after_complete: bool,

    /// Marker file whose removal releases a held helper process.
    #[arg(long, hide = true)]
    pub hold_release_marker: Option<PathBuf>,

    /// Parent runner PID whose exit releases a held helper process.
    #[arg(long, hide = true)]
    pub hold_parent_pid: Option<u32>,
}

#[derive(Args, Debug)]
/// Hidden helper arguments for one mounted sparse filesystem operation.
pub struct SparseOpArgs {
    /// Operation name: write-range, truncate, read-range-exact, read-range-zero, or read-all-exact.
    #[arg(long)]
    pub operation: String,

    /// Mounted root for the client that executes the operation.
    #[arg(long)]
    pub mount_point: PathBuf,

    /// Relative path under the mount point.
    #[arg(long)]
    pub path: String,

    /// Byte offset for ranged sparse operations.
    #[arg(long)]
    pub offset: Option<u64>,

    /// Length for truncate or zero-range validation operations.
    #[arg(long)]
    pub len: Option<u64>,

    /// Payload file for write-range, read-range-exact, and read-all-exact.
    #[arg(long)]
    pub payload: Option<PathBuf>,

    /// Parent sandbox path used only so stale-process cleanup can identify helpers.
    #[arg(long, hide = true)]
    pub sandbox: Option<PathBuf>,

    /// Per-operation progress JSONL path used to diagnose bounded helper timeouts.
    #[arg(long, hide = true)]
    pub progress: Option<PathBuf>,

    /// Internal operation timeout accepted for symmetry with hotspot-op.
    #[arg(long, hide = true)]
    pub timeout_ms: Option<u64>,
}

#[derive(Args, Debug)]
/// Hidden helper arguments for the mounted open-handle mutation sequence.
pub struct OpenHandleOpArgs {
    /// Mounted root for the client that keeps the old handle open.
    #[arg(long)]
    pub client_a_mount: PathBuf,

    /// Mounted root for the client that mutates namespace entries.
    #[arg(long)]
    pub client_b_mount: PathBuf,

    /// Relative unlink target path under both mounts.
    #[arg(long)]
    pub unlink_path: String,

    /// Relative rename source path under client-b's mount.
    #[arg(long)]
    pub rename_source: String,

    /// Relative rename destination path under client-b's mount.
    #[arg(long)]
    pub rename_dest: String,

    /// Payload file appended through the old client-a handle.
    #[arg(long)]
    pub append_payload: PathBuf,

    /// Parent sandbox path used only so stale-process cleanup can identify helpers.
    #[arg(long, hide = true)]
    pub sandbox: Option<PathBuf>,

    /// Per-sequence progress JSONL path used to diagnose bounded helper timeouts.
    #[arg(long, hide = true)]
    pub progress: Option<PathBuf>,

    /// Marker file whose removal releases the helper after client unmount.
    #[arg(long, hide = true)]
    pub hold_release_marker: Option<PathBuf>,

    /// Parent runner PID whose exit releases a held helper process.
    #[arg(long, hide = true)]
    pub hold_parent_pid: Option<u32>,
}

#[derive(Args, Debug)]
/// Hidden helper arguments for process diagnostics capture.
pub struct ProcessDumpArgs {
    /// Process ID to inspect.
    #[arg(long)]
    pub pid: u32,

    /// Human-readable termination context.
    #[arg(long)]
    pub label: String,

    /// Directory where diagnostics artifacts should be written.
    #[arg(long)]
    pub output_dir: PathBuf,

    /// Exact artifact stem selected by the supervising process.
    #[arg(long, hide = true)]
    pub artifact_stem: Option<String>,
}

#[derive(Args, Debug)]
/// Options used to remove stale scenario sandboxes from the run root.
pub struct CleanArgs {
    /// Parent directory containing per-run sandboxes.
    #[arg(long, default_value = "target/e2e-runs")]
    pub root: PathBuf,
}

#[derive(Args, Debug)]
/// Options for the Local First baseline gate execution loop.
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
    /// Expands positional or repeated selectors into a concrete list of scenario names.
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

    /// Builds a `RunRequest` consumed by the runner backend.
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
        .flat_map(|part| expand_scenario_selector_part(part.trim(), scenarios))
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

/// Formats one indexed scenario row for `List` command output.
pub fn format_scenario_list_line(index: usize, scenario: &scenario::ScenarioDefinition) -> String {
    format!("{}\t{}", index + 1, scenario.description)
}

impl BaselineGateArgs {
    /// Builds a `BaselineGateRequest` consumed by the baseline gate runner.
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

/// Executes the CLI command and returns the process exit code.
pub fn dispatch(cli: Cli) -> Result<i32> {
    crate::process_job::install_cleanup_handler();
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
        Commands::HotspotOp(args) => {
            arm_held_helper_parent_watchdog(args.hold_parent_pid)?;
            let result = scenario::run_hotspot_op_helper_with_timeout(
                &args.operation,
                args.mount_point,
                &args.path,
                args.target.as_deref(),
                args.payload.as_deref(),
                args.progress.as_deref(),
                args.timeout_ms.map(std::time::Duration::from_millis),
            );
            // This helper exists only to isolate one potentially blocking mounted
            // filesystem call.  Once the call has returned successfully, exit
            // without running any remaining process teardown that can wedge while
            // WinFSP is under churn.
            match result {
                Ok(()) if args.hold_after_complete => {
                    wait_for_held_helper_release(
                        args.hold_release_marker.as_deref(),
                        args.hold_parent_pid,
                    );
                    std::process::exit(0);
                }
                Ok(()) => std::process::exit(0),
                Err(error) => {
                    eprintln!("{error:#}");
                    std::process::exit(1);
                }
            }
        }
        Commands::SparseOp(args) => {
            let result =
                scenario::run_sparse_op_helper_with_timeout(scenario::SparseOpHelperRequest {
                    operation: &args.operation,
                    mount_point: &args.mount_point,
                    path: &args.path,
                    offset: args.offset,
                    len: args.len,
                    payload: args.payload.as_deref(),
                    progress: args.progress.as_deref(),
                    timeout: args.timeout_ms.map(std::time::Duration::from_millis),
                });
            match result {
                Ok(()) => std::process::exit(0),
                Err(error) => {
                    eprintln!("{error:#}");
                    std::process::exit(1);
                }
            }
        }
        Commands::OpenHandleOp(args) => {
            arm_held_helper_parent_watchdog(args.hold_parent_pid)?;
            scenario::run_open_handle_op_helper(
                args.client_a_mount,
                args.client_b_mount,
                &args.unlink_path,
                &args.rename_source,
                &args.rename_dest,
                &args.append_payload,
                args.progress.as_deref(),
            )?;
            wait_for_held_helper_release(args.hold_release_marker.as_deref(), args.hold_parent_pid);
            // The parent removes the marker only after unmount, so process
            // teardown cannot wedge on a live macFUSE channel here.
            std::process::exit(0);
        }
        Commands::ProcessDump(args) => {
            if let Some(artifact_stem) = args.artifact_stem {
                process_diagnostics::write_process_dump_with_stem(
                    args.pid,
                    &args.label,
                    &args.output_dir,
                    &artifact_stem,
                )?;
            } else {
                process_diagnostics::write_process_dump(args.pid, &args.label, &args.output_dir)?;
            }
            std::process::exit(0);
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

const HELD_HELPER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);
const HELD_HELPER_WATCHDOG: std::time::Duration = std::time::Duration::from_secs(600);

fn wait_for_held_helper_release(marker: Option<&std::path::Path>, parent_pid: Option<u32>) {
    let Some(marker) = marker else {
        return;
    };
    let started = std::time::Instant::now();
    loop {
        let marker_exists = marker.exists();
        let parent_alive = parent_pid.map(held_helper_parent_is_alive).unwrap_or(true);
        let watchdog_expired = started.elapsed() >= HELD_HELPER_WATCHDOG;
        if !held_helper_should_wait(marker_exists, parent_alive, watchdog_expired) {
            return;
        }
        std::thread::sleep(HELD_HELPER_POLL_INTERVAL);
    }
}

fn held_helper_should_wait(
    marker_exists: bool,
    parent_alive: bool,
    watchdog_expired: bool,
) -> bool {
    marker_exists && parent_alive && !watchdog_expired
}

#[cfg(unix)]
fn held_helper_parent_is_alive(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 only probes whether the supplied process exists.
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(any(test, target_os = "macos"))]
fn held_helper_parent_is_valid(
    observed_parent_pid: u32,
    expected_parent_pid: u32,
    parent_alive: bool,
) -> bool {
    observed_parent_pid == expected_parent_pid && parent_alive
}

#[cfg(target_os = "macos")]
fn arm_held_helper_parent_watchdog(parent_pid: Option<u32>) -> std::io::Result<()> {
    let Some(expected_parent_pid) = parent_pid else {
        return Ok(());
    };
    std::thread::Builder::new()
        .name("studiofs-e2e-held-helper-parent-watchdog".to_string())
        .spawn(move || {
            loop {
                // SAFETY: getppid only reads the calling process's parent PID.
                let observed_parent_pid = unsafe { libc::getppid() };
                let parent_is_valid =
                    u32::try_from(observed_parent_pid)
                        .ok()
                        .is_some_and(|observed_parent_pid| {
                            held_helper_parent_is_valid(
                                observed_parent_pid,
                                expected_parent_pid,
                                held_helper_parent_is_alive(expected_parent_pid),
                            )
                        });
                if !parent_is_valid {
                    // A macOS helper may be detached after its mounted operation
                    // completes. Exiting here avoids retaining a live FUSE file
                    // descriptor when its scenario parent disappears mid-operation.
                    std::process::exit(0);
                }
                std::thread::sleep(HELD_HELPER_POLL_INTERVAL);
            }
        })?;
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn arm_held_helper_parent_watchdog(_parent_pid: Option<u32>) -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn held_helper_parent_is_alive(_pid: u32) -> bool {
    // Windows helpers are never detached, so the watchdog is sufficient for
    // this defensive fallback.
    true
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
    fn hidden_hotspot_op_subcommand_parses() {
        let cli = Cli::try_parse_from([
            "studiofs-e2e",
            "hotspot-op",
            "--operation",
            "rename",
            "--sandbox",
            "target/e2e-runs/run-1",
            "--mount-point",
            "Z:\\",
            "--path",
            "hotspot/slot-1.txt",
            "--target",
            "hotspot/slot-1-renamed.txt",
            "--timeout-ms",
            "13000",
            "--hold-parent-pid",
            "4242",
        ])
        .expect("hidden hotspot-op command should parse");

        let Commands::HotspotOp(args) = cli.command else {
            panic!("expected hidden hotspot-op command");
        };
        assert_eq!(args.operation, "rename");
        assert_eq!(args.sandbox, Some(PathBuf::from("target/e2e-runs/run-1")));
        assert_eq!(args.mount_point, PathBuf::from("Z:\\"));
        assert_eq!(args.path, "hotspot/slot-1.txt");
        assert_eq!(args.target.as_deref(), Some("hotspot/slot-1-renamed.txt"));
        assert_eq!(args.timeout_ms, Some(13_000));
        assert_eq!(args.hold_parent_pid, Some(4242));
        assert_eq!(args.progress, None);
    }

    #[test]
    fn hidden_open_handle_op_subcommand_parses() {
        let cli = Cli::try_parse_from([
            "studiofs-e2e",
            "open-handle-op",
            "--sandbox",
            "target/e2e-runs/run-1",
            "--client-a-mount",
            "Z:\\",
            "--client-b-mount",
            "V:\\",
            "--unlink-path",
            "open-handles/unlink-target.txt",
            "--rename-source",
            "open-handles/rename-source.txt",
            "--rename-dest",
            "open-handles/renamed-destination.txt",
            "--append-payload",
            "target/e2e-runs/run-1/open-handle.payload",
            "--hold-parent-pid",
            "4242",
        ])
        .expect("hidden open-handle-op command should parse");

        let Commands::OpenHandleOp(args) = cli.command else {
            panic!("expected hidden open-handle-op command");
        };
        assert_eq!(args.sandbox, Some(PathBuf::from("target/e2e-runs/run-1")));
        assert_eq!(args.client_a_mount, PathBuf::from("Z:\\"));
        assert_eq!(args.client_b_mount, PathBuf::from("V:\\"));
        assert_eq!(args.unlink_path, "open-handles/unlink-target.txt");
        assert_eq!(args.rename_source, "open-handles/rename-source.txt");
        assert_eq!(args.rename_dest, "open-handles/renamed-destination.txt");
        assert_eq!(
            args.append_payload,
            PathBuf::from("target/e2e-runs/run-1/open-handle.payload")
        );
        assert_eq!(args.progress, None);
        assert_eq!(args.hold_parent_pid, Some(4242));
    }

    #[test]
    fn held_helper_stops_waiting_after_parent_exit_or_watchdog() {
        assert!(held_helper_should_wait(true, true, false));
        assert!(!held_helper_should_wait(false, true, false));
        assert!(!held_helper_should_wait(true, false, false));
        assert!(!held_helper_should_wait(true, true, true));
    }

    #[test]
    fn held_helper_parent_watchdog_rejects_dead_or_reparented_parent() {
        assert!(held_helper_parent_is_valid(4242, 4242, true));
        assert!(!held_helper_parent_is_valid(4242, 4242, false));
        assert!(!held_helper_parent_is_valid(1, 4242, true));
    }

    #[test]
    fn hidden_process_dump_subcommand_parses() {
        let cli = Cli::try_parse_from([
            "studiofs-e2e",
            "process-dump",
            "--pid",
            "4242",
            "--label",
            "timeout before kill",
            "--output-dir",
            "target/e2e-runs/run-1/artifacts/process-diagnostics",
            "--artifact-stem",
            "000042-pid-4242-timeout-before-kill",
        ])
        .expect("hidden process-dump command should parse");

        let Commands::ProcessDump(args) = cli.command else {
            panic!("expected hidden process-dump command");
        };
        assert_eq!(args.pid, 4242);
        assert_eq!(args.label, "timeout before kill");
        assert_eq!(
            args.output_dir,
            PathBuf::from("target/e2e-runs/run-1/artifacts/process-diagnostics")
        );
        assert_eq!(
            args.artifact_stem,
            Some("000042-pid-4242-timeout-before-kill".to_string())
        );
    }

    #[test]
    fn run_one_summary_is_suppressed_for_successful_child() {
        assert!(!should_print_run_one_summary(0));
        assert!(should_print_run_one_summary(1));
        assert!(should_print_run_one_summary(4));
    }
}

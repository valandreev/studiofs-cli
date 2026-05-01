use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser as _;
use serde_json::Value;
use studiofs_e2e::cli::{CleanArgs, Cli, Commands, RunArgs};
use studiofs_e2e::runner::{
    CleanRequest, PlatformSelection, RetentionPolicy, RunRequest, RunStatus, ScenarioStatus,
};
use tempfile::tempdir;

#[test]
fn cli_parses_required_sfs318_entrypoints() {
    let list = Cli::try_parse_from(["studiofs-e2e", "list"]).unwrap();
    assert!(matches!(list.command, Commands::List));

    let run = Cli::try_parse_from([
        "studiofs-e2e",
        "run",
        "basic-multi-client-smoke",
        "--run-id",
        "sfs318-test",
        "--seed",
        "42",
    ])
    .unwrap();
    match run.command {
        Commands::Run(RunArgs {
            scenario,
            run_id,
            seed,
            platform,
            ..
        }) => {
            assert_eq!(scenario, Some("basic-multi-client-smoke".to_string()));
            assert_eq!(run_id, Some("sfs318-test".to_string()));
            assert_eq!(seed, 42);
            assert_eq!(platform, PlatformSelection::Auto);
        }
        other => panic!("expected run command, got {other:?}"),
    }

    let clean = Cli::try_parse_from(["studiofs-e2e", "clean"]).unwrap();
    assert!(matches!(clean.command, Commands::Clean(CleanArgs { .. })));
}

#[test]
fn list_scenarios_exposes_initial_basic_smoke_scenario() {
    let scenarios = studiofs_e2e::scenario::available_scenarios();

    assert!(
        scenarios
            .iter()
            .any(|scenario| scenario.name == "basic-multi-client-smoke")
    );
}

#[test]
fn run_creates_per_run_sandbox_manifest_report_and_validation_artifact() {
    let temp = tempdir().unwrap();
    let request = RunRequest {
        root: temp.path().join("e2e-runs"),
        run_id: Some("sfs318-test-run".to_string()),
        scenarios: vec!["basic-multi-client-smoke".to_string()],
        seed: 42,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
    };

    let outcome = studiofs_e2e::runner::run(request).unwrap();

    assert_eq!(outcome.run_id, "sfs318-test-run");
    assert_eq!(outcome.status, RunStatus::Passed);
    assert_eq!(outcome.scenarios[0].name, "basic-multi-client-smoke");
    assert_eq!(outcome.scenarios[0].status, ScenarioStatus::Passed);
    assert!(
        outcome.scenarios[0]
            .artifacts
            .contains(&"artifacts/scenarios/basic-multi-client-smoke/validation.json".to_string())
    );

    let sandbox = temp.path().join("e2e-runs").join("sfs318-test-run");
    assert_required_sandbox_files(&sandbox);

    let run_manifest = read_json(sandbox.join("run.json"));
    assert_eq!(run_manifest["run_id"], "sfs318-test-run");
    assert_eq!(run_manifest["seed"], 42);
    assert_eq!(run_manifest["scenarios"][0], "basic-multi-client-smoke");

    let report = read_json(sandbox.join("report.json"));
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["run_id"], "sfs318-test-run");
    assert_eq!(report["seed"], 42);
    assert_eq!(report["status"], "passed");
    assert_eq!(report["scenarios"][0]["name"], "basic-multi-client-smoke");
    assert_eq!(report["scenarios"][0]["seed"], 42);
    assert_eq!(report["scenarios"][0]["status"], "passed");
    assert!(report["scenarios"][0]["duration_ms"].is_number());
    assert_eq!(
        report["scenarios"][0]["artifacts"][0],
        "artifacts/scenarios/basic-multi-client-smoke/validation.json"
    );

    let validation =
        read_json(sandbox.join("artifacts/scenarios/basic-multi-client-smoke/validation.json"));
    assert_eq!(validation["scenario"], "basic-multi-client-smoke");
    assert_eq!(validation["seed"], 42);
}

#[test]
fn explicit_report_path_receives_copy_while_canonical_report_stays_in_sandbox() {
    let temp = tempdir().unwrap();
    let report_copy = temp.path().join("reports").join("latest.json");
    let request = RunRequest {
        root: temp.path().join("e2e-runs"),
        run_id: Some("copy-report-run".to_string()),
        scenarios: vec!["basic-multi-client-smoke".to_string()],
        seed: 7,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: Some(report_copy.clone()),
        port_range: None,
        timeout: None,
    };

    studiofs_e2e::runner::run(request).unwrap();

    let canonical = temp
        .path()
        .join("e2e-runs")
        .join("copy-report-run")
        .join("report.json");
    assert!(canonical.exists());
    assert!(report_copy.exists());
    assert_eq!(read_json(canonical), read_json(report_copy));
}

#[test]
fn clean_removes_only_runner_owned_sandboxes() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("e2e-runs");
    let request = RunRequest {
        root: root.clone(),
        run_id: Some("owned-run".to_string()),
        scenarios: vec!["basic-multi-client-smoke".to_string()],
        seed: 9,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
    };
    studiofs_e2e::runner::run(request).unwrap();

    let unrelated = root.join("manual-notes");
    fs::create_dir_all(&unrelated).unwrap();
    fs::write(unrelated.join("notes.txt"), "operator-owned").unwrap();

    let removed = studiofs_e2e::runner::clean(CleanRequest { root }).unwrap();

    assert_eq!(removed, vec![PathBuf::from("owned-run")]);
    assert!(!temp.path().join("e2e-runs").join("owned-run").exists());
    assert!(unrelated.join("notes.txt").exists());
}

fn assert_required_sandbox_files(sandbox: &Path) {
    for relative in [
        "run.json",
        "report.json",
        "events.ndjson",
        "stdout.log",
        "stderr.log",
        "preflight.json",
        "ports.json",
        "cleanup.json",
    ] {
        assert!(
            sandbox.join(relative).exists(),
            "expected sandbox file {relative}"
        );
    }
}

fn read_json(path: impl AsRef<Path>) -> Value {
    let content = fs::read_to_string(path).unwrap();
    serde_json::from_str(&content).unwrap()
}

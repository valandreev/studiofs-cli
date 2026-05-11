use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser as _;
use serde_json::Value;
use studiofs_e2e::baseline_gate;
use studiofs_e2e::cli::{BaselineGateArgs, CleanArgs, Cli, Commands, RunArgs};
use studiofs_e2e::infra::InfraMode;
use studiofs_e2e::runner::{
    CleanRequest, PlatformSelection, RetentionPolicy, RunRequest, RunStatus, RunnerError,
    ScenarioStatus,
};
use tempfile::tempdir;

#[test]
fn cli_parses_required_runner_entrypoints() {
    let list = Cli::try_parse_from(["studiofs-e2e", "list"]).unwrap();
    assert!(matches!(list.command, Commands::List));

    let run = Cli::try_parse_from([
        "studiofs-e2e",
        "run",
        "basic-multi-client-smoke",
        "--run-id",
        "runner-cli-test",
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
            assert_eq!(run_id, Some("runner-cli-test".to_string()));
            assert_eq!(seed, 42);
            assert_eq!(platform, PlatformSelection::Auto);
        }
        other => panic!("expected run command, got {other:?}"),
    }

    let clean = Cli::try_parse_from(["studiofs-e2e", "clean"]).unwrap();
    assert!(matches!(clean.command, Commands::Clean(CleanArgs { .. })));
}

#[test]
fn cli_parses_local_first_baseline_gate() {
    let gate = Cli::try_parse_from([
        "studiofs-e2e",
        "local-first-baseline-gate",
        "--studiofs-bin",
        "target/debug/studiofs.exe",
        "--run-id-prefix",
        "sfs405-baseline",
        "--iterations",
        "2",
        "--seed",
        "405",
        "--port-range",
        "29700-29800",
    ])
    .unwrap();

    match gate.command {
        Commands::LocalFirstBaselineGate(BaselineGateArgs {
            studiofs_bin,
            run_id_prefix,
            iterations,
            seed,
            platform,
            port_range,
            ..
        }) => {
            assert_eq!(studiofs_bin, PathBuf::from("target/debug/studiofs.exe"));
            assert_eq!(run_id_prefix, "sfs405-baseline");
            assert_eq!(iterations, 2);
            assert_eq!(seed, 405);
            assert_eq!(platform, PlatformSelection::WindowsWinfsp);
            assert_eq!(port_range.as_deref(), Some("29700-29800"));
        }
        other => panic!("expected local-first-baseline-gate command, got {other:?}"),
    }
}

#[test]
fn run_scenario_selection_expands_numeric_ranges() {
    let cli = Cli::try_parse_from(["studiofs-e2e", "run", "--scenario", "1-3"]).unwrap();
    let Commands::Run(args) = cli.command else {
        panic!("expected run command");
    };

    assert_eq!(args.scenario_names(), scenario_names_by_index(&[1, 2, 3]));
}

#[test]
fn run_scenario_selection_expands_comma_separated_numbers() {
    let cli = Cli::try_parse_from(["studiofs-e2e", "run", "--scenario", "2,5,10"]).unwrap();
    let Commands::Run(args) = cli.command else {
        panic!("expected run command");
    };

    assert_eq!(args.scenario_names(), scenario_names_by_index(&[2, 5, 10]));
}

#[test]
fn local_first_baseline_gate_uses_scenarios_1_through_8() {
    assert_eq!(
        baseline_gate::SCENARIOS_1_THROUGH_8,
        [
            "basic-multi-client-smoke",
            "rustfs-outage-multi-client-recovery",
            "collision-rename-vs-rename",
            "postgres-outage-multi-client-recovery",
            "mounted-client-crash-restart-recovery",
            "collision-rename-vs-unlink",
            "collision-unlink-vs-append",
            "collision-overwrite-vs-read",
        ]
    );
}

#[test]
fn scenario_list_order_matches_local_first_baseline_numbering() {
    let scenarios = studiofs_e2e::scenario::available_scenarios();
    let first_eight = scenarios
        .iter()
        .take(8)
        .map(|scenario| scenario.name)
        .collect::<Vec<_>>();

    assert_eq!(first_eight, baseline_gate::SCENARIOS_1_THROUGH_8);
}

#[test]
fn e2e_compose_pins_rustfs_baseline_image() {
    let compose_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("docker/compose.e2e.yml");
    let compose = fs::read_to_string(compose_path).unwrap();

    assert!(
        compose.contains("image: rustfs/rustfs:1.0.0-alpha.94"),
        "SFS-405 baseline gate must reject accidental RustFS latest usage"
    );
    assert!(!compose.contains("image: rustfs/rustfs:latest"));
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
fn list_scenarios_exposes_namespace_collision_suite() {
    let scenarios = studiofs_e2e::scenario::available_scenarios();
    let names = scenarios
        .iter()
        .map(|scenario| scenario.name)
        .collect::<Vec<_>>();

    for expected in [
        "collision-rename-vs-rename",
        "collision-rename-vs-unlink",
        "collision-unlink-vs-append",
        "collision-overwrite-vs-read",
        "collision-mkdir-rmdir-vs-create",
        "collision-readdir-stat-during-mutation",
    ] {
        assert!(
            names.contains(&expected),
            "expected namespace collision scenario {expected}"
        );
    }
}

#[test]
fn namespace_collision_scenario_disabled_run_writes_reproducible_artifacts() {
    let temp = tempdir().unwrap();
    let request = RunRequest {
        root: temp.path().join("e2e-runs"),
        run_id: Some("collision-contract-run".to_string()),
        scenarios: vec!["collision-rename-vs-unlink".to_string()],
        seed: 328,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
        infra: InfraMode::Disabled,
    };

    let outcome = studiofs_e2e::runner::run(request).unwrap();

    assert_eq!(outcome.status, RunStatus::Passed);
    assert_eq!(outcome.scenarios[0].name, "collision-rename-vs-unlink");
    assert!(
        outcome.scenarios[0].artifacts.contains(
            &"artifacts/scenarios/collision-rename-vs-unlink/namespace-collision-summary.json"
                .to_string()
        )
    );
    assert!(
        outcome.scenarios[0].artifacts.contains(
            &"artifacts/scenarios/collision-rename-vs-unlink/validation.json".to_string()
        )
    );

    let sandbox = temp.path().join("e2e-runs").join("collision-contract-run");
    let summary =
        read_json(sandbox.join(
            "artifacts/scenarios/collision-rename-vs-unlink/namespace-collision-summary.json",
        ));
    assert_eq!(summary["scenario"], "collision-rename-vs-unlink");
    assert_eq!(summary["clients"], 3);
    assert!(
        summary["operation_log"]
            .as_str()
            .unwrap()
            .ends_with("operations.ndjson")
    );

    let operations = fs::read_to_string(
        sandbox.join("artifacts/scenarios/collision-rename-vs-unlink/operations.ndjson"),
    )
    .unwrap();
    assert!(operations.contains("\"client\""));
    assert!(operations.contains("\"operation\""));
    assert!(operations.contains("\"order\""));
    assert!(operations.contains("\"result\""));
}

#[test]
fn run_creates_per_run_sandbox_manifest_report_and_validation_artifact() {
    let temp = tempdir().unwrap();
    let request = RunRequest {
        root: temp.path().join("e2e-runs"),
        run_id: Some("runner-contract-run".to_string()),
        scenarios: vec!["basic-multi-client-smoke".to_string()],
        seed: 42,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
        infra: InfraMode::Disabled,
    };

    let outcome = studiofs_e2e::runner::run(request).unwrap();

    assert_eq!(outcome.run_id, "runner-contract-run");
    assert_eq!(outcome.status, RunStatus::Passed);
    assert_eq!(outcome.scenarios[0].name, "basic-multi-client-smoke");
    assert_eq!(outcome.scenarios[0].status, ScenarioStatus::Passed);
    assert!(
        outcome.scenarios[0]
            .artifacts
            .contains(&"artifacts/scenarios/basic-multi-client-smoke/validation.json".to_string())
    );

    let sandbox = temp.path().join("e2e-runs").join("runner-contract-run");
    assert_required_sandbox_files(&sandbox);
    assert!(sandbox.join(".studiofs-e2e-runner.json").exists());

    let run_manifest = read_json(sandbox.join("run.json"));
    assert_eq!(run_manifest["run_id"], "runner-contract-run");
    assert_eq!(run_manifest["seed"], 42);
    assert_eq!(run_manifest["scenarios"][0], "basic-multi-client-smoke");

    let report = read_json(sandbox.join("report.json"));
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["run_id"], "runner-contract-run");
    assert_eq!(report["seed"], 42);
    assert_eq!(report["status"], "passed");
    assert_eq!(report["scenarios"][0]["name"], "basic-multi-client-smoke");
    assert_eq!(report["scenarios"][0]["seed"], 42);
    assert_eq!(report["scenarios"][0]["status"], "passed");
    assert!(report["scenarios"][0]["duration_ms"].is_number());
    let scenario_artifacts = report["scenarios"][0]["artifacts"].as_array().unwrap();
    assert!(scenario_artifacts.iter().any(
        |artifact| artifact == "artifacts/scenarios/basic-multi-client-smoke/operations.ndjson"
    ));
    assert!(
        scenario_artifacts
            .iter()
            .any(|artifact| artifact
                == "artifacts/scenarios/basic-multi-client-smoke/validation.json")
    );
    assert!(
        report["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|artifact| artifact["path"] == ".studiofs-e2e-runner.json")
    );
    assert_eq!(report["environment"]["run_id"], "runner-contract-run");
    assert!(report["environment"]["studiofs_bin"].is_null());
    assert!(report["environment"]["compose_project"].is_null());
    assert_eq!(
        report["environment"]["client_roots"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        report["environment"]["mount_roots"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert!(
        report["environment"]["compose_images"]
            .as_array()
            .unwrap()
            .iter()
            .any(|image| image["service"] == "postgres" && image["image"] == "postgres:18-alpine")
    );
    assert!(
        report["environment"]["compose_images"]
            .as_array()
            .unwrap()
            .iter()
            .any(|image| image["service"] == "rustfs"
                && image["image"] == "rustfs/rustfs:1.0.0-alpha.94")
    );

    let validation =
        read_json(sandbox.join("artifacts/scenarios/basic-multi-client-smoke/validation.json"));
    assert_eq!(validation["scenario"], "basic-multi-client-smoke");
    assert_eq!(validation["seed"], 42);
}

#[test]
fn report_links_runner_scenario_and_client_diagnostic_artifacts() {
    let temp = tempdir().unwrap();
    let request = RunRequest {
        root: temp.path().join("e2e-runs"),
        run_id: Some("diagnostic-artifacts-run".to_string()),
        scenarios: vec!["basic-multi-client-smoke".to_string()],
        seed: 43,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
        infra: InfraMode::Disabled,
    };

    let outcome = studiofs_e2e::runner::run(request).unwrap();
    let report = read_json(outcome.report_path);
    let artifacts = report["artifacts"].as_array().unwrap();

    for expected in [
        "report.json",
        "stdout.log",
        "stderr.log",
        "ports.json",
        "artifacts/scenarios/basic-multi-client-smoke/operations.ndjson",
        "artifacts/scenarios/basic-multi-client-smoke/validation.json",
        "artifacts/clients/client-a/studiofs.stdout.log",
        "artifacts/clients/client-a/studiofs.stderr.log",
        "artifacts/clients/client-a/studiofs-tracing.log",
    ] {
        assert!(
            artifacts
                .iter()
                .any(|artifact| artifact["path"] == expected),
            "expected report artifact {expected}"
        );
    }
    assert_eq!(
        report["scenarios"][0]["artifacts"][0],
        "artifacts/scenarios/basic-multi-client-smoke/operations.ndjson"
    );
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
        infra: InfraMode::Disabled,
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
        infra: InfraMode::Disabled,
    };
    studiofs_e2e::runner::run(request).unwrap();

    let unrelated = root.join("manual-notes");
    fs::create_dir_all(&unrelated).unwrap();
    fs::write(unrelated.join("notes.txt"), "operator-owned").unwrap();

    let removed = studiofs_e2e::runner::clean(CleanRequest { root })
        .unwrap()
        .removed;

    assert_eq!(removed, vec![PathBuf::from("owned-run")]);
    assert!(!temp.path().join("e2e-runs").join("owned-run").exists());
    assert!(unrelated.join("notes.txt").exists());
}

#[test]
fn clean_ignores_manifest_only_directories_without_runner_marker_and_layout() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("e2e-runs");
    let fake = root.join("fake-run");
    fs::create_dir_all(&fake).unwrap();
    fs::write(
        fake.join("run.json"),
        r#"{"runner_crate":"studiofs-e2e","run_id":"fake-run"}"#,
    )
    .unwrap();

    let removed = studiofs_e2e::runner::clean(CleanRequest { root })
        .unwrap()
        .removed;

    assert!(removed.is_empty());
    assert!(fake.join("run.json").exists());
}

#[test]
fn run_rejects_run_id_path_traversal_before_creating_sandbox() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("e2e-runs");
    let request = RunRequest {
        root,
        run_id: Some("../escape".to_string()),
        scenarios: vec!["basic-multi-client-smoke".to_string()],
        seed: 1,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
        infra: InfraMode::Disabled,
    };

    let err = studiofs_e2e::runner::run(request).unwrap_err();

    assert!(matches!(err, RunnerError::UnsafeRunId(_)));
    assert!(!temp.path().join("escape").exists());
}

#[test]
fn run_rejects_unknown_scenario_before_creating_sandbox() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("e2e-runs");
    let request = RunRequest {
        root,
        run_id: Some("unknown-scenario-run".to_string()),
        scenarios: vec!["missing-scenario".to_string()],
        seed: 1,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
        infra: InfraMode::Disabled,
    };

    let err = studiofs_e2e::runner::run(request).unwrap_err();

    assert!(matches!(err, RunnerError::UnknownScenario(_)));
    assert!(
        !temp
            .path()
            .join("e2e-runs")
            .join("unknown-scenario-run")
            .exists()
    );
}

#[test]
fn clean_continues_after_malformed_owned_candidate() {
    let temp = tempdir().unwrap();
    let root = temp.path().join("e2e-runs");
    let valid = RunRequest {
        root: root.clone(),
        run_id: Some("valid-run".to_string()),
        scenarios: vec!["basic-multi-client-smoke".to_string()],
        seed: 11,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
        infra: InfraMode::Disabled,
    };
    studiofs_e2e::runner::run(valid).unwrap();

    let malformed = root.join("malformed-run");
    fs::create_dir_all(&malformed).unwrap();
    fs::write(malformed.join(".studiofs-e2e-runner.json"), "{").unwrap();
    fs::write(
        malformed.join("run.json"),
        r#"{"runner_crate":"studiofs-e2e","run_id":"malformed-run"}"#,
    )
    .unwrap();

    let outcome = studiofs_e2e::runner::clean(CleanRequest { root }).unwrap();

    assert_eq!(outcome.removed, vec![PathBuf::from("valid-run")]);
    assert_eq!(outcome.skipped.len(), 1);
    assert!(malformed.join("run.json").exists());
    assert!(!temp.path().join("e2e-runs").join("valid-run").exists());
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
        "environment.json",
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

fn scenario_names_by_index(indexes: &[usize]) -> Vec<String> {
    let scenarios = studiofs_e2e::scenario::available_scenarios();
    indexes
        .iter()
        .map(|index| scenarios[index - 1].name.to_string())
        .collect()
}

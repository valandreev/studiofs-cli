use studiofs_scenario::seed_corpus::{
    SIMULATION_COUNTEREXAMPLE_SCHEMA_VERSION, SimulationCorpusError, SimulationCorpusKind,
    SimulationCorpusOptions, SimulationScenarioGroup, corpus_plan, read_simulation_counterexample,
    replay_simulation_counterexample, run_simulation_corpus,
};

#[test]
fn fast_and_deep_corpus_are_stable_named_seed_sets() {
    let fast = corpus_plan(SimulationCorpusKind::Fast);
    let deep = corpus_plan(SimulationCorpusKind::Deep);

    assert_eq!(
        fast.groups
            .iter()
            .map(|group| group.group)
            .collect::<Vec<_>>(),
        vec![
            SimulationScenarioGroup::NamespaceCollisions,
            SimulationScenarioGroup::CrashRestartReplay,
            SimulationScenarioGroup::MetadataOutage,
            SimulationScenarioGroup::ObjectStoreOutage,
        ]
    );
    assert_eq!(fast.seeds, vec![1, 2, 3]);
    assert_eq!(deep.seeds, (1..=16).collect::<Vec<_>>());
    assert!(
        fast.groups
            .iter()
            .all(|group| !group.replay_command.is_empty())
    );
}

#[test]
fn corpus_runner_filters_one_group_and_one_seed() {
    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: SimulationCorpusKind::Deep,
        group: Some(SimulationScenarioGroup::MetadataOutage),
        seed: Some(7),
        artifact_root: None,
        inject_failure: false,
    })
    .expect("corpus runs");

    assert!(report.passed, "{:#?}", report.failures);
    assert_eq!(report.total_runs, 1);
    assert_eq!(report.group_reports.len(), 1);
    assert_eq!(
        report.group_reports[0].group,
        SimulationScenarioGroup::MetadataOutage
    );
    assert_eq!(report.group_reports[0].seed_reports[0].seed, 7);
}

#[test]
fn corpus_failures_include_group_seed_invariant_and_artifact_path() {
    let artifact_root = unique_temp_dir("sfs340-corpus-counterexamples");
    std::fs::create_dir_all(&artifact_root).expect("create artifact root");

    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: SimulationCorpusKind::Fast,
        group: Some(SimulationScenarioGroup::CrashRestartReplay),
        seed: Some(13),
        artifact_root: Some(artifact_root.clone()),
        inject_failure: true,
    })
    .expect("corpus runs and writes artifacts");

    assert!(!report.passed);
    let failure = report.failures.first().expect("failure is reported");
    assert_eq!(failure.group, SimulationScenarioGroup::CrashRestartReplay);
    assert_eq!(failure.seed, 13);
    assert_eq!(failure.violated_invariant, "injected_counterexample");
    let artifact_path = failure
        .counterexample_artifact_path
        .as_ref()
        .expect("counterexample artifact path is reported");
    assert!(artifact_path.contains("sfs340-simulation-corpus"));
    assert!(std::path::Path::new(artifact_path).exists());
}

#[test]
fn all_corpus_group_failures_write_counterexample_artifacts() {
    for group in [
        SimulationScenarioGroup::NamespaceCollisions,
        SimulationScenarioGroup::CrashRestartReplay,
        SimulationScenarioGroup::MetadataOutage,
        SimulationScenarioGroup::ObjectStoreOutage,
    ] {
        let artifact_root = unique_temp_dir(&format!("sfs340-{group:?}-counterexamples"));
        std::fs::create_dir_all(&artifact_root).expect("create artifact root");

        let report = run_simulation_corpus(SimulationCorpusOptions {
            kind: SimulationCorpusKind::Fast,
            group: Some(group),
            seed: Some(13),
            artifact_root: Some(artifact_root),
            inject_failure: true,
        })
        .expect("corpus writes failure artifacts");

        assert!(!report.passed, "{group:?} should report injected failure");
        let failure = report.failures.first().expect("failure is reported");
        assert_eq!(failure.group, group);
        assert_eq!(failure.seed, 13);
        let artifact_path = failure
            .counterexample_artifact_path
            .as_ref()
            .expect("failure records an artifact path");
        assert!(
            std::path::Path::new(artifact_path).exists(),
            "artifact path exists for {group:?}: {artifact_path}"
        );
    }
}

#[test]
fn namespace_corpus_counterexample_replays_exactly_from_artifact_path() {
    let artifact_root = unique_temp_dir("sfs359-namespace-counterexample-replay");
    std::fs::create_dir_all(&artifact_root).expect("create artifact root");

    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: SimulationCorpusKind::Fast,
        group: Some(SimulationScenarioGroup::NamespaceCollisions),
        seed: Some(13),
        artifact_root: Some(artifact_root),
        inject_failure: true,
    })
    .expect("corpus writes failure artifacts");
    let artifact_path = report.failures[0]
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records an artifact path");

    let artifact =
        read_simulation_counterexample(artifact_path).expect("counterexample artifact can be read");
    assert_eq!(
        artifact.schema_version,
        SIMULATION_COUNTEREXAMPLE_SCHEMA_VERSION
    );
    assert_eq!(artifact.group, SimulationScenarioGroup::NamespaceCollisions);
    assert_eq!(
        artifact.sub_group.as_deref(),
        Some("injected_invariant_failure")
    );
    assert_eq!(artifact.seed, 13);
    assert!(!artifact.scheduler_trace.is_empty());
    assert!(!artifact.operation_log.is_empty());
    assert!(!artifact.event_trace.is_empty());
    assert!(artifact.final_snapshot.is_some());
    assert!(artifact.replay_command.contains("replay-counterexample"));

    let replay =
        replay_simulation_counterexample(artifact_path).expect("counterexample artifact replays");

    assert!(replay.reproduced);
    assert_eq!(replay.group, SimulationScenarioGroup::NamespaceCollisions);
    assert_eq!(replay.seed, 13);
    assert_eq!(replay.violated_invariant, "injected_counterexample");
}

#[test]
fn legacy_v1_simulation_counterexample_artifact_is_rejected_by_schema_before_full_parse() {
    let artifact_root = unique_temp_dir("sfs359-v1-artifact-rejection");
    std::fs::create_dir_all(&artifact_root).expect("create artifact root");
    let artifact_path = artifact_root.join("legacy-v1.json");
    std::fs::write(
        &artifact_path,
        serde_json::json!({
            "schema_version": 1,
            "scenario": "sfs340-simulation-corpus",
            "corpus": "fast",
            "group": "namespace_collisions",
            "seed": 13,
            "failure": {
                "violated_invariant": "injected_counterexample",
                "message": "legacy seed-only artifact"
            },
            "replay_command": "cargo run -p studiofs-scenario --bin studiofs-scenario -- run-corpus --corpus fast --scenario-group namespace-collisions --seed 13"
        })
        .to_string(),
    )
    .expect("write legacy artifact");

    let error =
        read_simulation_counterexample(&artifact_path).expect_err("legacy v1 is unsupported");

    assert!(matches!(
        error,
        SimulationCorpusError::UnsupportedSchemaVersion(1)
    ));
}

#[test]
fn simulation_counterexample_replay_rejects_trace_drift() {
    let artifact_root = unique_temp_dir("sfs359-namespace-counterexample-drift");
    std::fs::create_dir_all(&artifact_root).expect("create artifact root");

    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: SimulationCorpusKind::Fast,
        group: Some(SimulationScenarioGroup::NamespaceCollisions),
        seed: Some(13),
        artifact_root: Some(artifact_root),
        inject_failure: true,
    })
    .expect("corpus writes failure artifacts");
    let artifact_path = report.failures[0]
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records an artifact path");

    let mut artifact =
        read_simulation_counterexample(artifact_path).expect("counterexample artifact can be read");
    artifact.event_trace[0] = serde_json::json!({"drifted": true});
    std::fs::write(
        artifact_path,
        serde_json::to_vec_pretty(&artifact).expect("artifact serializes"),
    )
    .expect("rewrite drifted artifact");

    let error =
        replay_simulation_counterexample(artifact_path).expect_err("trace drift is rejected");

    let error = error.to_string();
    assert!(error.contains("event trace differed from artifact at index 0"));
    assert!(error.contains("saved="));
    assert!(error.contains("replayed="));
}

#[test]
fn object_store_counterexample_artifact_includes_snapshot() {
    let artifact_root = unique_temp_dir("sfs359-object-store-counterexample-snapshot");
    std::fs::create_dir_all(&artifact_root).expect("create artifact root");

    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: SimulationCorpusKind::Fast,
        group: Some(SimulationScenarioGroup::ObjectStoreOutage),
        seed: Some(13),
        artifact_root: Some(artifact_root),
        inject_failure: true,
    })
    .expect("corpus writes failure artifacts");
    let artifact_path = report.failures[0]
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records an artifact path");

    let artifact =
        read_simulation_counterexample(artifact_path).expect("counterexample artifact can be read");

    assert_eq!(artifact.group, SimulationScenarioGroup::ObjectStoreOutage);
    assert!(artifact.final_snapshot.is_some());
}

#[test]
fn cli_runs_one_seed_and_reports_group_seed_and_artifact_root() {
    let artifact_root = unique_temp_dir("sfs340-cli-artifacts");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_studiofs-scenario"))
        .args([
            "run-corpus",
            "--corpus",
            "fast",
            "--scenario-group",
            "metadata-outage",
            "--seed",
            "2",
            "--artifact-root",
        ])
        .arg(&artifact_root)
        .output()
        .expect("run studiofs-scenario CLI");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf-8");
    assert!(stdout.contains("scenario=sfs340-simulation-corpus"));
    assert!(stdout.contains("group=metadata-outage seed=2 status=passed"));
    assert!(stdout.contains("artifact_root="));
}

#[test]
fn cli_replays_saved_simulation_counterexample_artifact() {
    let artifact_root = unique_temp_dir("sfs359-cli-replay-artifacts");
    std::fs::create_dir_all(&artifact_root).expect("create artifact root");

    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: SimulationCorpusKind::Fast,
        group: Some(SimulationScenarioGroup::NamespaceCollisions),
        seed: Some(13),
        artifact_root: Some(artifact_root),
        inject_failure: true,
    })
    .expect("corpus writes failure artifacts");
    let artifact_path = report.failures[0]
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records an artifact path");

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_studiofs-scenario"))
        .arg("replay-counterexample")
        .arg(artifact_path)
        .output()
        .expect("run studiofs-scenario CLI");

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf-8");
    assert!(stdout.contains("reproduced sfs340-simulation-corpus"));
    assert!(stdout.contains("group namespace-collisions seed 13"));
}

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after unix epoch")
            .as_nanos()
    ))
}

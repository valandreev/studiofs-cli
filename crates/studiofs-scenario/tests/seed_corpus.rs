use studiofs_scenario::seed_corpus::{
    SimulationCorpusKind, SimulationCorpusOptions, SimulationScenarioGroup, corpus_plan,
    run_simulation_corpus,
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
    assert!(stdout.contains("group=metadata_outage seed=2 status=passed"));
    assert!(stdout.contains("artifact_root="));
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

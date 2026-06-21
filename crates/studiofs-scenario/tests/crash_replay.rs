//! Crate documentation for lint completion.
mod common;

use common::seed_order_signatures;
use studiofs_scenario::ScenarioOperation;
use studiofs_scenario::crash_replay::{
    CrashReplayArtifactError, CrashReplayFamily, CrashReplaySuiteConfig,
    read_crash_replay_counterexample, replay_crash_replay_counterexample, run_crash_replay_suite,
    run_crash_replay_suite_with_artifacts,
};

#[test]
fn crash_replay_suite_covers_required_restart_interleavings_across_seeds() {
    let report = run_crash_replay_suite(CrashReplaySuiteConfig {
        scenario: "sfs338-crash-replay-suite".to_string(),
        seeds: vec![1, 2, 3],
        families: vec![
            CrashReplayFamily::CrashAfterDurabilityBeforeUploadCompletion,
            CrashReplayFamily::CrashAfterUploadBeforeMetadataPublication,
            CrashReplayFamily::CrashDuringPublishAttempt,
            CrashReplayFamily::CrashDuringForeignApply,
            CrashReplayFamily::RestartWithPendingLocalUploadState,
            CrashReplayFamily::RepeatedRestartReplayLoops,
            CrashReplayFamily::StalePendingUploadEventsRaceNewerEpochs,
        ],
    });

    assert!(report.passed, "{:#?}", report.failures);
    assert_eq!(report.family_reports.len(), 7);
    assert_eq!(report.total_runs(), 21);
    assert!(
        report
            .family_reports
            .iter()
            .all(|family| family.seed_reports.len() == 3)
    );
    assert!(
        report
            .family_reports
            .iter()
            .flat_map(|family| &family.seed_reports)
            .all(|seed| seed.replay_idempotent
                && seed.peer_converged
                && seed.invariant_checkpoints >= 2
                && !seed.trace.is_empty())
    );
}

#[test]
fn stale_pending_upload_or_publish_events_never_override_newer_epochs() {
    let report = run_crash_replay_suite(CrashReplaySuiteConfig {
        scenario: "sfs338-stale-publish-race".to_string(),
        seeds: (1..=8).collect(),
        families: vec![CrashReplayFamily::StalePendingUploadEventsRaceNewerEpochs],
    });

    assert!(report.passed, "{:#?}", report.failures);
    let seed_reports = &report.family_reports[0].seed_reports;
    assert!(seed_reports.iter().all(|seed| {
        seed.trace
            .iter()
            .any(|record| record.kind == "stale_publish_rejected")
    }));
    assert!(seed_reports.iter().all(|seed| {
        seed.trace
            .iter()
            .any(|record| record.kind == "peer_observed_newer_epoch")
    }));
}

#[test]
fn deep_crash_replay_seeds_cover_more_than_parity_order_signatures() {
    let report = run_crash_replay_suite(CrashReplaySuiteConfig {
        scenario: "sfs358-crash-replay-seed-coverage".to_string(),
        seeds: (1..=16).collect(),
        families: vec![
            CrashReplayFamily::CrashAfterDurabilityBeforeUploadCompletion,
            CrashReplayFamily::CrashAfterUploadBeforeMetadataPublication,
            CrashReplayFamily::CrashDuringPublishAttempt,
            CrashReplayFamily::CrashDuringForeignApply,
            CrashReplayFamily::RestartWithPendingLocalUploadState,
            CrashReplayFamily::RepeatedRestartReplayLoops,
            CrashReplayFamily::StalePendingUploadEventsRaceNewerEpochs,
        ],
    });

    assert!(report.passed, "{:#?}", report.failures);
    let signatures = seed_order_signatures(
        &report
            .family_reports
            .iter()
            .flat_map(|family| &family.seed_reports)
            .map(|seed| {
                (
                    seed.seed,
                    seed.trace
                        .iter()
                        .filter(|record| record.kind == "scheduler_selected")
                        .filter_map(|record| record.path.clone())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
    );

    assert!(
        signatures.len() > 2,
        "deep crash/restart seeds collapsed to parity-only signatures: {signatures:#?}"
    );
}

#[test]
fn crash_replay_failures_include_replayable_counterexample_trace() {
    let report = run_crash_replay_suite(CrashReplaySuiteConfig {
        scenario: "sfs338-counterexample".to_string(),
        seeds: vec![13],
        families: vec![CrashReplayFamily::InjectedInvariantFailure],
    });

    assert!(!report.passed);
    let failure = report.failures.first().expect("failure is reported");
    assert_eq!(failure.scenario, "sfs338-counterexample");
    assert_eq!(failure.family, CrashReplayFamily::InjectedInvariantFailure);
    assert_eq!(failure.seed, 13);
    assert_eq!(failure.violated_invariant, "injected_counterexample");
    assert!(!failure.trace.is_empty());
    assert!(
        failure
            .trace
            .iter()
            .any(|record| record.kind == "counterexample_replay_ready")
    );
}

#[test]
fn crash_replay_failure_writes_replayable_counterexample_artifact() {
    let artifact_dir = unique_temp_dir("sfs339-counterexample-artifacts");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");

    let report = run_crash_replay_suite_with_artifacts(
        CrashReplaySuiteConfig {
            scenario: "sfs339-counterexample-artifact".to_string(),
            seeds: vec![13],
            families: vec![CrashReplayFamily::InjectedInvariantFailure],
        },
        &artifact_dir,
    )
    .expect("counterexample artifacts are written");

    assert!(!report.passed);
    let failure = report.failures.first().expect("failure is reported");
    let artifact_path = failure
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records its counterexample artifact path");

    let artifact = read_crash_replay_counterexample(artifact_path)
        .expect("counterexample artifact can be read");
    assert_eq!(artifact.scenario, "sfs339-counterexample-artifact");
    assert_eq!(artifact.seed, 13);
    assert_eq!(artifact.family, CrashReplayFamily::InjectedInvariantFailure);
    assert_eq!(
        artifact.failure.violated_invariant,
        "injected_counterexample"
    );
    assert!(!artifact.scheduler_trace.is_empty());
    assert!(!artifact.operation_log.is_empty());
    assert!(artifact.replay_command.contains("\""));
    assert!(artifact.operation_log.iter().any(|record| {
        matches!(
            &record.operation,
            ScenarioOperation::CreateFile { path, .. } if path == "replay.txt"
        )
    }));
    assert!(artifact.operation_log.iter().any(|record| {
        matches!(
            &record.operation,
            ScenarioOperation::Write {
                path,
                bytes: 1,
                checksum_sha256: Some(checksum_sha256),
                ..
            } if path == "replay.txt" && checksum_sha256 == "checksum-counterexample"
        )
    }));
    assert!(artifact.final_snapshot.is_some());
    assert!(artifact.replay_command.contains("replay-counterexample"));

    let replay =
        replay_crash_replay_counterexample(artifact_path).expect("counterexample artifact replays");
    assert!(replay.reproduced, "{replay:#?}");
    assert_eq!(replay.violated_invariant, "injected_counterexample");
}

#[test]
fn crash_replay_rejects_unknown_planned_trace_operations() {
    let artifact_dir = unique_temp_dir("sfs501-crash-replay-unknown-op");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");

    let report = run_crash_replay_suite_with_artifacts(
        CrashReplaySuiteConfig {
            scenario: "sfs501-crash-replay-unknown-op".to_string(),
            seeds: vec![13],
            families: vec![CrashReplayFamily::InjectedInvariantFailure],
        },
        &artifact_dir,
    )
    .expect("counterexample artifacts are written");

    let artifact_path = report.failures[0]
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records its counterexample artifact path");
    let mut artifact = read_crash_replay_counterexample(artifact_path)
        .expect("counterexample artifact can be read");
    let selected = artifact
        .client_trace
        .iter_mut()
        .find(|record| record.kind == "scheduler_selected")
        .expect("counterexample has scheduler selection");
    selected.path = Some("future_crash_operation".to_string());
    std::fs::write(
        artifact_path,
        serde_json::to_vec_pretty(&artifact).expect("serialize mutated artifact"),
    )
    .expect("rewrite mutated artifact");

    let error = replay_crash_replay_counterexample(artifact_path)
        .expect_err("unknown planned operation returns a scenario error");

    match error {
        CrashReplayArtifactError::DecodeTraceOperation(error) => {
            assert_eq!(error.kind(), "future_crash_operation");
            assert!(error.context().contains("sfs501-crash-replay-unknown-op"));
            assert!(error.context().contains("step 4"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn crash_replay_rejects_missing_planned_trace_operations() {
    let artifact_dir = unique_temp_dir("sfs501-crash-replay-missing-op");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");

    let report = run_crash_replay_suite_with_artifacts(
        CrashReplaySuiteConfig {
            scenario: "sfs501-crash-replay-missing-op".to_string(),
            seeds: vec![13],
            families: vec![CrashReplayFamily::InjectedInvariantFailure],
        },
        &artifact_dir,
    )
    .expect("counterexample artifacts are written");

    let artifact_path = report.failures[0]
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records its counterexample artifact path");
    let mut artifact = read_crash_replay_counterexample(artifact_path)
        .expect("counterexample artifact can be read");
    let selected = artifact
        .client_trace
        .iter_mut()
        .find(|record| record.kind == "scheduler_selected")
        .expect("counterexample has scheduler selection");
    selected.path = None;
    std::fs::write(
        artifact_path,
        serde_json::to_vec_pretty(&artifact).expect("serialize mutated artifact"),
    )
    .expect("rewrite mutated artifact");

    let error = replay_crash_replay_counterexample(artifact_path)
        .expect_err("missing planned operation returns a scenario error");

    match error {
        CrashReplayArtifactError::DecodeTraceOperation(error) => {
            assert_eq!(error.kind(), "missing planned operation kind");
            assert!(error.context().contains("sfs501-crash-replay-missing-op"));
            assert!(error.context().contains("step 4"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn crash_replay_reports_artifact_operation_log_inconsistency() {
    let artifact_dir = unique_temp_dir("sfs501-crash-replay-log-inconsistency");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");

    let report = run_crash_replay_suite_with_artifacts(
        CrashReplaySuiteConfig {
            scenario: "sfs501-crash-replay-log-inconsistency".to_string(),
            seeds: vec![13],
            families: vec![CrashReplayFamily::InjectedInvariantFailure],
        },
        &artifact_dir,
    )
    .expect("counterexample artifacts are written");

    let artifact_path = report.failures[0]
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records its counterexample artifact path");
    let mut artifact = read_crash_replay_counterexample(artifact_path)
        .expect("counterexample artifact can be read");
    artifact.operation_log.clear();
    std::fs::write(
        artifact_path,
        serde_json::to_vec_pretty(&artifact).expect("serialize mutated artifact"),
    )
    .expect("rewrite mutated artifact");

    let error = replay_crash_replay_counterexample(artifact_path)
        .expect_err("inconsistent artifact operation log is rejected");

    match error {
        CrashReplayArtifactError::ReplayMismatch(message) => assert_eq!(
            message,
            "artifact client trace decodes to a different operation log than stored in the artifact"
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn counterexample_artifact_path_uses_unknown_for_empty_sanitized_scenarios() {
    let artifact_dir = unique_temp_dir("sfs339-empty-scenario-artifact");
    std::fs::create_dir_all(&artifact_dir).expect("create artifact dir");

    let report = run_crash_replay_suite_with_artifacts(
        CrashReplaySuiteConfig {
            scenario: "///".to_string(),
            seeds: vec![13],
            families: vec![CrashReplayFamily::InjectedInvariantFailure],
        },
        &artifact_dir,
    )
    .expect("counterexample artifacts are written");

    let failure = report.failures.first().expect("failure is reported");
    let artifact_path = failure
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records its counterexample artifact path");
    assert!(artifact_path.replace('\\', "/").contains("/unknown/"));
}

#[test]
fn artifact_write_failures_are_collected_across_all_failing_seeds() {
    let artifact_root_file = unique_temp_dir("sfs339-artifact-root-file");
    std::fs::write(&artifact_root_file, b"not a directory").expect("create artifact root file");

    let error = run_crash_replay_suite_with_artifacts(
        CrashReplaySuiteConfig {
            scenario: "sfs339-write-failure-collection".to_string(),
            seeds: vec![13, 14],
            families: vec![CrashReplayFamily::InjectedInvariantFailure],
        },
        &artifact_root_file,
    )
    .expect_err("artifact writes fail after all failing seeds are attempted");

    match error {
        CrashReplayArtifactError::ArtifactWriteFailures { errors } => {
            assert_eq!(errors.len(), 2);
            assert_eq!(
                errors.iter().map(|error| error.seed).collect::<Vec<_>>(),
                vec![13, 14]
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn replay_idempotence_does_not_treat_memory_only_dirty_loss_as_failure() {
    let report = run_crash_replay_suite(CrashReplaySuiteConfig {
        scenario: "sfs338-memory-only-dirty-loss".to_string(),
        seeds: vec![5],
        families: vec![CrashReplayFamily::MemoryOnlyDirtyCrash],
    });

    assert!(report.passed, "{:#?}", report.failures);
    let seed = &report.family_reports[0].seed_reports[0];
    assert!(seed.replay_idempotent);
    assert!(
        seed.trace
            .iter()
            .any(|record| record.kind == "dirty_bytes_lost_after_crash")
    );
}

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    studiofs_test_support::workspace_target_temp_path(&format!("{name}-"))
}

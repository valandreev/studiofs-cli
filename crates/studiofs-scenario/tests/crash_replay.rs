use studiofs_scenario::crash_replay::{
    CrashReplayFamily, CrashReplaySuiteConfig, run_crash_replay_suite,
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

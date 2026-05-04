use studiofs_scenario::collision::{CollisionFamily, CollisionSuiteConfig, run_collision_suite};

#[test]
fn collision_suite_runs_required_families_across_seeded_interleavings() {
    let report = run_collision_suite(CollisionSuiteConfig {
        scenario: "sfs337-collision-suite".to_string(),
        seeds: vec![1, 2, 3],
        families: vec![
            CollisionFamily::RenameVsRename,
            CollisionFamily::RenameVsUnlink,
            CollisionFamily::UnlinkVsAppend,
            CollisionFamily::OverwriteWhileStaleReader,
            CollisionFamily::MkdirRmdirVsCreate,
        ],
    });

    assert!(report.passed, "{:#?}", report.failures);
    assert_eq!(report.family_reports.len(), 5);
    assert_eq!(report.total_runs(), 15);
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
            .all(|seed| seed.invariant_checkpoints >= 2 && !seed.trace.is_empty())
    );
}

#[test]
fn scheduler_selects_real_conflicting_operations_in_both_orders() {
    let report = run_collision_suite(CollisionSuiteConfig {
        scenario: "sfs337-real-interleavings".to_string(),
        seeds: (1..=8).collect(),
        families: vec![CollisionFamily::UnlinkVsAppend],
    });

    assert!(report.passed, "{:#?}", report.failures);
    let traces = report
        .family_reports
        .first()
        .unwrap()
        .seed_reports
        .iter()
        .map(|seed| {
            seed.trace
                .iter()
                .map(|record| record.kind.as_str())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert!(
        traces
            .iter()
            .any(|trace| ordered_pair(trace, "alpha_unlink", "beta_append"))
    );
    assert!(
        traces
            .iter()
            .any(|trace| ordered_pair(trace, "beta_append", "alpha_unlink"))
    );
    assert!(
        traces
            .iter()
            .all(|trace| trace.iter().any(|kind| *kind == "scheduler_selected"))
    );
}

#[test]
fn collision_failure_reports_replayable_counterexample_trace() {
    let report = run_collision_suite(CollisionSuiteConfig {
        scenario: "sfs337-counterexample".to_string(),
        seeds: vec![1],
        families: vec![CollisionFamily::InjectedInvariantFailure],
    });

    assert!(!report.passed);
    let failure = report.failures.first().expect("failure is reported");
    assert_eq!(failure.scenario, "sfs337-counterexample");
    assert_eq!(failure.family, CollisionFamily::InjectedInvariantFailure);
    assert_eq!(failure.seed, 1);
    assert_eq!(failure.violated_invariant, "injected_counterexample");
    assert!(!failure.trace.is_empty());
    assert!(
        failure
            .trace
            .iter()
            .any(|record| record.kind == "invariant_failed")
    );
}

fn ordered_pair(trace: &[&str], first: &str, second: &str) -> bool {
    let Some(first_index) = trace.iter().position(|kind| *kind == first) else {
        return false;
    };
    trace
        .iter()
        .skip(first_index + 1)
        .any(|kind| *kind == second)
}

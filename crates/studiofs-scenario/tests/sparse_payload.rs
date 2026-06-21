//! Crate documentation for lint completion.
use studiofs_scenario::ScenarioOperation;
use studiofs_scenario::client::SimulatedRuntime;
use studiofs_scenario::invariant::{LogicalByteRangeKind, validate_logical_invariants};
use studiofs_scenario::seed_corpus::{
    SimulationCorpusKind, SimulationCorpusOptions, SimulationScenarioGroup,
    read_simulation_counterexample, replay_simulation_counterexample, run_simulation_corpus,
};

#[test]
fn sparse_writes_truncate_and_read_ranges_preserve_holes() {
    let mut runtime = SimulatedRuntime::new(396);
    runtime.add_client("writer");

    runtime.create_file("writer", "sparse.bin").unwrap();
    runtime
        .write_sparse_range("writer", "sparse.bin", 4, b"abcd")
        .unwrap();
    runtime
        .write_sparse_range("writer", "sparse.bin", 12, b"tail")
        .unwrap();

    let spanning = runtime
        .read_file_range("writer", "sparse.bin", 0, 16)
        .unwrap();
    assert_eq!(&spanning.bytes[0..4], &[0, 0, 0, 0]);
    assert_eq!(&spanning.bytes[4..8], b"abcd");
    assert_eq!(&spanning.bytes[8..12], &[0, 0, 0, 0]);
    assert_eq!(&spanning.bytes[12..16], b"tail");
    assert_eq!(
        spanning.holes,
        vec![(0, 4).into(), (8, 4).into()],
        "read diagnostics should expose holes inside the requested range"
    );
    assert_eq!(
        runtime
            .stat("writer", "sparse.bin")
            .unwrap()
            .checksum_sha256
            .as_deref(),
        Some(spanning.checksum_sha256.as_str())
    );

    runtime
        .truncate_sparse_file("writer", "sparse.bin", 6)
        .unwrap();
    let shrunk = runtime
        .read_file_range("writer", "sparse.bin", 0, 8)
        .unwrap();
    assert_eq!(shrunk.bytes, b"\0\0\0\0ab".to_vec());
    assert_eq!(shrunk.holes, vec![(0, 4).into()]);

    runtime
        .truncate_sparse_file("writer", "sparse.bin", 10)
        .unwrap();
    let extended = runtime
        .read_file_range("writer", "sparse.bin", 0, 10)
        .unwrap();
    assert_eq!(extended.bytes, b"\0\0\0\0ab\0\0\0\0".to_vec());
    assert_eq!(extended.holes, vec![(0, 4).into(), (6, 4).into()]);
}

#[test]
fn sparse_ranges_converge_through_upload_publish_and_metadata_delivery() {
    let mut runtime = SimulatedRuntime::new(397);
    runtime.add_client("writer");
    runtime.add_client("reader");

    runtime.create_file("writer", "sparse.bin").unwrap();
    runtime
        .write_sparse_range("writer", "sparse.bin", 2, b"left")
        .unwrap();
    runtime
        .write_sparse_range("writer", "sparse.bin", 10, b"right")
        .unwrap();
    runtime.flush("writer", "sparse.bin").unwrap();

    let upload = runtime.schedule_upload("writer", "sparse.bin").unwrap();
    runtime.complete_upload(upload.upload_id).unwrap();
    runtime.publish_file("writer", "sparse.bin").unwrap();
    runtime.deliver_next_metadata("reader").unwrap();

    let reader = runtime
        .read_file_range("reader", "sparse.bin", 0, 15)
        .unwrap();
    assert_eq!(&reader.bytes[2..6], b"left");
    assert_eq!(&reader.bytes[10..15], b"right");
    assert_eq!(reader.holes, vec![(0, 2).into(), (6, 4).into()]);

    let snapshot = runtime.logical_snapshot();
    let report = validate_logical_invariants(&snapshot);
    assert!(report.passed, "{:#?}", report.violations);

    let reader_entry = snapshot
        .clients
        .iter()
        .find(|client| client.client == "reader")
        .unwrap()
        .entries
        .iter()
        .find(|entry| entry.path == "sparse.bin")
        .unwrap();
    assert_eq!(reader_entry.byte_ranges.len(), 4);
    assert_eq!(reader_entry.byte_ranges[0].kind, LogicalByteRangeKind::Hole);
    assert_eq!(reader_entry.byte_ranges[1].kind, LogicalByteRangeKind::Data);
}

#[test]
fn high_offset_sparse_write_does_not_materialize_full_file_for_checksum() {
    let mut runtime = SimulatedRuntime::new(398);
    runtime.add_client("writer");

    runtime.create_file("writer", "large-sparse.bin").unwrap();
    runtime
        .write_sparse_range("writer", "large-sparse.bin", 8 * 1024 * 1024 * 1024, b"x")
        .unwrap();

    let stat = runtime.stat("writer", "large-sparse.bin").unwrap();
    assert_eq!(stat.size, Some(8 * 1024 * 1024 * 1024 + 1));
    let tail = runtime
        .read_file_range("writer", "large-sparse.bin", 8 * 1024 * 1024 * 1024, 1)
        .unwrap();
    assert_eq!(tail.bytes, b"x");
}

#[test]
fn sparse_object_ids_use_logical_identity_after_rename() {
    let mut runtime = SimulatedRuntime::new(399);
    runtime.add_client("writer");

    runtime.create_file("writer", "dir/sparse.bin").unwrap();
    runtime
        .write_sparse_range("writer", "dir/sparse.bin", 4, b"left")
        .unwrap();
    runtime
        .rename("writer", "dir/sparse.bin", "dir/renamed.bin")
        .unwrap();
    runtime
        .write_sparse_range("writer", "dir/renamed.bin", 12, b"right")
        .unwrap();

    let snapshot = runtime.logical_snapshot();
    let entry = snapshot
        .clients
        .iter()
        .find(|client| client.client == "writer")
        .unwrap()
        .entries
        .iter()
        .find(|entry| entry.path == "dir/renamed.bin")
        .unwrap();
    let object_id = &entry.object.as_ref().unwrap().object_id;
    assert!(!object_id.contains("dir/sparse.bin"));
    assert!(!object_id.contains("dir/renamed.bin"));
    assert!(object_id.contains(entry.logical_id.as_deref().unwrap()));
}

#[test]
fn sparse_payload_corpus_runs_multiple_orderings() {
    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: SimulationCorpusKind::Deep,
        group: Some(SimulationScenarioGroup::SparsePayloadChaos),
        seed: None,
        artifact_root: None,
        inject_failure: false,
    })
    .expect("sparse payload corpus runs");

    assert!(report.passed, "{:#?}", report.failures);
    assert_eq!(report.group_reports.len(), 1);
    assert_eq!(
        report.group_reports[0].group,
        SimulationScenarioGroup::SparsePayloadChaos
    );
    let distinct_trace_shapes = report.group_reports[0]
        .seed_reports
        .iter()
        .map(|seed| seed.trace_records)
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        distinct_trace_shapes.len() > 1,
        "sparse payload seeds should exercise multiple operation orderings: {:#?}",
        report.group_reports[0].seed_reports
    );
}

#[test]
fn sparse_payload_counterexample_artifact_replays_with_range_operations() {
    let artifact_root = unique_temp_dir("sfs396-sparse-payload-counterexample");
    std::fs::create_dir_all(&artifact_root).expect("create artifact root");

    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: SimulationCorpusKind::Fast,
        group: Some(SimulationScenarioGroup::SparsePayloadChaos),
        seed: Some(2),
        artifact_root: Some(artifact_root),
        inject_failure: true,
    })
    .expect("sparse payload corpus writes counterexample");

    assert!(!report.passed);
    let artifact_path = report.failures[0]
        .counterexample_artifact_path
        .as_ref()
        .expect("failure records an artifact path");
    let artifact =
        read_simulation_counterexample(artifact_path).expect("counterexample artifact can be read");
    assert_eq!(artifact.group, SimulationScenarioGroup::SparsePayloadChaos);
    assert!(artifact.final_snapshot.is_some());
    assert!(artifact.operation_log.iter().any(|record| matches!(
        record.operation,
        ScenarioOperation::Write {
            offset: Some(10),
            ..
        }
    )));
    assert!(artifact.operation_log.iter().any(|record| matches!(
        record.operation,
        ScenarioOperation::Truncate { size: 13, .. }
    )));
    assert!(artifact.operation_log.iter().any(|record| matches!(
        record.operation,
        ScenarioOperation::Read { .. }
    )
        && record.metadata.contains_key("offset")));

    let replay =
        replay_simulation_counterexample(artifact_path).expect("counterexample artifact replays");
    assert!(replay.reproduced);
    assert_eq!(replay.group, SimulationScenarioGroup::SparsePayloadChaos);
    assert_eq!(replay.violated_invariant, "injected_counterexample");
}

fn unique_temp_dir(name: &str) -> std::path::PathBuf {
    studiofs_test_support::workspace_target_temp_path(&format!("{name}-"))
}

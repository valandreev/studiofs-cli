use std::fs;

use studiofs_e2e::infra::InfraMode;
use studiofs_e2e::runner::{PlatformSelection, RetentionPolicy, RunRequest, RunStatus};
use studiofs_scenario::{
    OperationLogWriter, OperationOutcome, ScenarioOperation, ScenarioOperationRecord,
    SimulationSupport, read_operation_log, simulation_supported_records, write_operation_log,
};
use tempfile::tempdir;

#[test]
fn shared_operation_model_round_trips_stable_json_for_e2e_and_simulation() {
    let records = vec![
        ScenarioOperationRecord::new(
            "collision-rename-vs-unlink",
            1,
            ScenarioOperation::CreateFile {
                path: "shared/file.txt".to_string(),
                bytes: Some(12),
            },
        )
        .with_client("client-a")
        .with_timestamp_ms(10)
        .with_outcome(OperationOutcome::Accepted),
        ScenarioOperationRecord::new(
            "collision-rename-vs-unlink",
            2,
            ScenarioOperation::Rename {
                from: "shared/file.txt".to_string(),
                to: "shared/renamed.txt".to_string(),
            },
        )
        .with_client("client-a")
        .with_timestamp_ms(12)
        .with_outcome(OperationOutcome::Accepted),
        ScenarioOperationRecord::new(
            "mounted-client-crash-restart-recovery",
            3,
            ScenarioOperation::Fsync {
                path: "studiofs-e2e-crash-after-local-durability.txt".to_string(),
            },
        )
        .with_client("client-a")
        .with_timestamp_ms(18)
        .with_outcome(OperationOutcome::Accepted),
        ScenarioOperationRecord::new(
            "mounted-client-crash-restart-recovery",
            4,
            ScenarioOperation::ClientCrash,
        )
        .with_client("client-a")
        .with_timestamp_ms(19)
        .with_outcome(OperationOutcome::Accepted),
        ScenarioOperationRecord::new(
            "postgres-outage-multi-client-recovery",
            5,
            ScenarioOperation::PostgresOutage,
        )
        .with_timestamp_ms(20)
        .with_outcome(OperationOutcome::Accepted),
        ScenarioOperationRecord::new(
            "rustfs-outage-multi-client-recovery",
            6,
            ScenarioOperation::ObjectStoreRecovery,
        )
        .with_timestamp_ms(21)
        .with_outcome(OperationOutcome::Accepted),
        ScenarioOperationRecord::new(
            "collision-rename-vs-unlink",
            7,
            ScenarioOperation::Barrier {
                name: "conflicting-operations-start".to_string(),
            },
        )
        .with_timestamp_ms(22)
        .with_outcome(OperationOutcome::Planned),
        ScenarioOperationRecord::new(
            "mounted-client-crash-restart-recovery",
            8,
            ScenarioOperation::Release {
                path: Some("studiofs-e2e-crash-after-local-durability.txt".to_string()),
            },
        )
        .with_client("client-a")
        .with_timestamp_ms(23)
        .with_outcome(OperationOutcome::Accepted)
        .with_simulation_support(SimulationSupport::Unsupported {
            reason: "mounted release callback boundary is not replayed by the core simulator"
                .to_string(),
        }),
    ];
    let temp = tempdir().unwrap();
    let log_path = temp.path().join("operations.ndjson");

    write_operation_log(&log_path, &records).unwrap();

    let log = fs::read_to_string(&log_path).unwrap();
    assert!(log.contains(r#""schema_version":1"#));
    assert!(log.contains(r#""type":"create_file""#));
    assert!(log.contains(r#""type":"rename""#));
    assert!(log.contains(r#""type":"fsync""#));
    assert!(log.contains(r#""type":"client_crash""#));
    assert!(log.contains(r#""type":"postgres_outage""#));
    assert!(log.contains(r#""type":"object_store_recovery""#));
    assert!(log.contains(r#""type":"barrier""#));
    assert!(log.contains(r#""simulation_support":{"status":"unsupported""#));

    let decoded = read_operation_log(&log_path).unwrap();
    assert_eq!(decoded, records);

    let replayable = simulation_supported_records(&decoded).collect::<Vec<_>>();
    assert_eq!(replayable.len(), records.len() - 1);
    assert!(
        replayable
            .iter()
            .all(|record| record.is_simulation_supported())
    );
}

#[test]
fn mounted_only_operation_defaults_to_simulation_unsupported() {
    let record = ScenarioOperationRecord::new(
        "platform-probe",
        1,
        ScenarioOperation::MountedOnly {
            name: "winfsp_drive_letter_probe".to_string(),
            reason: "requires a real WinFSP mount".to_string(),
        },
    );

    assert!(!record.is_simulation_supported());
    assert!(matches!(
        record.simulation_support,
        SimulationSupport::Unsupported { ref reason }
            if reason == "requires a real WinFSP mount"
    ));
}

#[test]
fn operation_log_writer_allocates_sequence_without_rescanning_each_append() {
    let temp = tempdir().unwrap();
    let log_path = temp.path().join("operations.ndjson");
    write_operation_log(
        &log_path,
        &[ScenarioOperationRecord::new(
            "existing-scenario",
            41,
            ScenarioOperation::Wait {
                checkpoint: "already-written".to_string(),
            },
        )],
    )
    .unwrap();

    let mut writer = OperationLogWriter::open_append(&log_path).unwrap();
    let first = writer
        .record(
            "existing-scenario",
            ScenarioOperation::SchedulerCheckpoint {
                name: "run.started".to_string(),
            },
        )
        .with_outcome(OperationOutcome::Observed);
    assert_eq!(first.sequence, 42);
    writer.append_record(&first).unwrap();
    let second = writer.record(
        "existing-scenario",
        ScenarioOperation::SchedulerCheckpoint {
            name: "run.finished".to_string(),
        },
    );
    assert_eq!(second.sequence, 43);
    writer.append_record(&second).unwrap();

    let records = read_operation_log(&log_path).unwrap();
    assert_eq!(records[1].sequence, 42);
    assert_eq!(records[2].sequence, 43);
}

#[test]
fn e2e_runner_operations_log_uses_shared_operation_records() {
    let temp = tempdir().unwrap();
    let request = RunRequest {
        root: temp.path().join("e2e-runs"),
        run_id: Some("shared-operation-run".to_string()),
        scenarios: vec!["basic-multi-client-smoke".to_string()],
        seed: 331,
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
    let operation_log = outcome
        .sandbox
        .join("artifacts/scenarios/basic-multi-client-smoke/operations.ndjson");
    let records = read_operation_log(operation_log).unwrap();
    assert!(records.iter().all(|record| {
        record.schema_version == 1
            && record.scenario == "basic-multi-client-smoke"
            && record.is_simulation_supported()
    }));
    assert!(records.iter().any(|record| matches!(
        &record.operation,
        ScenarioOperation::SchedulerCheckpoint { name } if name == "setup.started"
    )));
    assert!(records.iter().any(|record| matches!(
        &record.operation,
        ScenarioOperation::SchedulerCheckpoint { name } if name == "cleanup.finished"
    )));
}

#[test]
fn namespace_collision_operation_log_uses_shared_logical_operations() {
    let temp = tempdir().unwrap();
    let request = RunRequest {
        root: temp.path().join("e2e-runs"),
        run_id: Some("shared-collision-run".to_string()),
        scenarios: vec!["collision-rename-vs-unlink".to_string()],
        seed: 331,
        studiofs_bin: None,
        platform: PlatformSelection::Auto,
        retention: RetentionPolicy::Always,
        report: None,
        port_range: None,
        timeout: None,
        infra: InfraMode::Disabled,
    };

    let outcome = studiofs_e2e::runner::run(request).unwrap();
    let records = read_operation_log(
        outcome
            .sandbox
            .join("artifacts/scenarios/collision-rename-vs-unlink/operations.ndjson"),
    )
    .unwrap();

    assert!(records.iter().all(|record| {
        record.scenario == "collision-rename-vs-unlink" && record.is_simulation_supported()
    }));
    assert!(records.iter().any(|record| matches!(
        &record.operation,
        ScenarioOperation::Rename { from, to }
            if from == "shared/file.txt" && to == "shared/renamed.txt"
    )));
    assert!(records.iter().any(|record| matches!(
        &record.operation,
        ScenarioOperation::Unlink { path } if path == "shared/file.txt"
    )));
}

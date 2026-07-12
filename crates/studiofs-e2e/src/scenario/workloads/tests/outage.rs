use super::*;

#[test]
fn postgres_outage_records_local_operations_and_recovery_metrics() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients).publish_writer_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.bootstrap.init_client, "client-a");
    assert_eq!(
        summary.outage_file,
        "studiofs-e2e-postgres-outage-writer.txt"
    );
    assert_eq!(summary.writer_recovery_client, "client-a");
    assert_eq!(
        summary.writer_recovery_status,
        PostgresOutageWriterRecoveryStatus::Recovered
    );
    assert_eq!(
        summary.writer_recovery_bytes,
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len()
    );
    assert_eq!(summary.operations_accepted_during_outage, 1);
    assert!(summary.outage_duration_ms > 0);
    assert!(summary.writer_recovery_time_ms > 0);
    assert!(summary.peer_observation_time_ms > 0);
    assert!(summary.final_validator.converged);
    assert_eq!(
        driver.actions.borrow().as_slice(),
        [
            "bootstrap",
            "mount_all",
            "after_initial_write",
            "stop_postgres",
            "force_kill_writer",
            "start_postgres",
            "remount_writer",
            "unmount_all"
        ]
    );

    let summary_json: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            context
                .scenario_dir
                .join("postgres-outage-recovery-summary.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(summary_json["operations_accepted_during_outage"], 1);
    assert_eq!(summary_json["writer_recovery_client"], "client-a");
    assert_eq!(summary_json["writer_recovery_status"], "recovered");
    assert_eq!(
        summary_json["writer_recovery_bytes"],
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len()
    );
    assert!(summary_json["expected_writer_recovery_checksum_sha256"].is_string());
    assert!(summary_json["writer_recovery_checksum_sha256"].is_string());
    assert!(summary_json["peer_observation_time_ms"].is_number());
    assert!(
        context
            .scenario_dir
            .join("outage-operations.ndjson")
            .exists()
    );
    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("\"accepted\":true"));
    assert!(operations_log.contains("\"client\":\"client-a\""));
    assert!(!operations_log.contains("\"client\":\"client-b\""));
}

#[test]
fn postgres_outage_allows_missing_writer_recovery_after_unclean_shutdown() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients).lose_writer_payload_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 403,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(
        summary.writer_recovery_status,
        PostgresOutageWriterRecoveryStatus::LostAfterUncleanShutdown
    );
    assert_eq!(summary.writer_recovery_bytes, 0);
    assert_eq!(summary.writer_recovery_checksum_sha256, bytes_sha256(&[]));
    assert!(summary.final_validator.converged);
    assert_eq!(
        summary.final_validator.compared_clients,
        POSTGRES_OUTAGE_CLIENT_NAMES
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert!(summary.peer_observation_time_ms > 0);
}

#[test]
fn postgres_outage_empty_writer_placeholder_fails_final_convergence() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients)
        .lose_writer_payload_on_remount()
        .keep_lost_writer_payload_local_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 404,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    let ScenarioError::PostgresOutagePeerConvergenceFailed { detail } = err else {
        panic!("expected peer convergence failure, got {err:?}");
    };
    assert!(
        detail.contains("did not converge"),
        "lost writer placeholder should fail final convergence: {detail}"
    );
}

#[test]
fn postgres_outage_writer_recovery_reports_equal_length_corruption() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients)
        .publish_writer_on_remount()
        .corrupt_writer_payload_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 403,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    let ScenarioError::PostgresOutageWriterRecoveryFailed { detail, .. } = err else {
        panic!("expected writer recovery error");
    };
    assert!(detail.contains("expected checksum"));
    assert!(detail.contains("recovered checksum"));
}

#[test]
fn postgres_outage_duration_includes_postgres_restart_wait() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = POSTGRES_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingOutageDriver::new(clients)
        .publish_writer_on_remount()
        .with_start_postgres_delay(Duration::from_millis(40));
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 403,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_postgres_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(
        summary.outage_duration_ms >= 30,
        "outage duration should include PostgreSQL restart wait, got {}ms",
        summary.outage_duration_ms
    );
}

#[test]
fn rustfs_outage_keeps_local_writes_private_until_recovery_and_records_checksums() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = RUSTFS_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingRustfsOutageDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 326,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_rustfs_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.bootstrap.init_client, "client-a");
    assert_eq!(summary.outage_file, "studiofs-e2e-rustfs-outage.txt");
    assert_eq!(
        summary.local_read_checksum_sha256,
        summary.final_checksum_sha256
    );
    let initial_checksum = bytes_sha256(RUSTFS_OUTAGE_INITIAL_PAYLOAD);
    assert_eq!(summary.peer_visibility_during_outage.len(), 3);
    assert!(summary.peer_visibility_during_outage.iter().all(|probe| {
        probe.visible_checksum_sha256.as_deref() == Some(initial_checksum.as_str())
    }));
    assert_eq!(
        summary.peer_visible_during_outage.as_deref(),
        Some(initial_checksum.as_str())
    );
    assert_eq!(summary.final_client_checksums.len(), 4);
    assert!(
        summary
            .final_client_checksums
            .iter()
            .all(|entry| entry.checksum_sha256 == summary.local_read_checksum_sha256)
    );
    assert!(summary.final_validator.converged);
    assert_eq!(
        driver.actions.borrow().as_slice(),
        [
            "bootstrap",
            "mount_all",
            "after_initial_write",
            "stop_rustfs",
            "assert_peer_not_visible",
            "assert_peer_not_visible",
            "assert_peer_not_visible",
            "start_rustfs",
            "unmount_all"
        ]
    );

    let summary_json: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            context
                .scenario_dir
                .join("rustfs-outage-recovery-summary.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(summary_json["upload_recovery_time_ms"].is_number());
    assert_eq!(
        summary_json["local_read_checksum_sha256"],
        summary_json["final_checksum_sha256"]
    );
    assert_eq!(
        summary_json["peer_visibility_during_outage"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        summary_json["final_client_checksums"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    assert!(
        context
            .scenario_dir
            .join("rustfs-outage-operations.ndjson")
            .exists()
    );
    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("\"scenario\":\"rustfs-outage-multi-client-recovery\""));
}

#[test]
fn rustfs_outage_records_all_peer_visibility_and_final_checksums() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-four-client");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = ["client-a", "client-b", "client-c", "client-d"]
        .into_iter()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingRustfsOutageDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-four-client".to_string(),
        seed: 326,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_rustfs_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.peer_visibility_during_outage.len(), 3);
    assert_eq!(summary.final_client_checksums.len(), 4);
    assert!(
        summary
            .final_client_checksums
            .iter()
            .all(|entry| entry.checksum_sha256 == summary.local_read_checksum_sha256)
    );
}

#[test]
fn rustfs_outage_duration_excludes_peer_visibility_probe_latency() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY);
    let clients = RUSTFS_OUTAGE_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingRustfsOutageDriver::new(clients).with_peer_probe_delay(Duration::from_millis(200));
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 326,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_rustfs_outage_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(
        summary.outage_duration_ms < 100,
        "outage duration should exclude the peer visibility probe latency, got {}ms",
        summary.outage_duration_ms
    );
}

#[test]
fn rustfs_peer_visibility_probe_propagates_unexpected_io_errors() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let mount = temp.path().join("mounts").join("client-b");
    std::fs::create_dir_all(mount.join(RUSTFS_OUTAGE_FILE_NAME)).unwrap();
    let peer = client_spec_for(&sandbox, "projectfs", "client-b", mount);

    let err = assert_peer_not_visible(&peer, RUSTFS_OUTAGE_FILE_NAME, "forbidden").unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
}

#[test]
fn compose_control_reports_missing_run_manifest_path() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox: temp.path().join("run-1"),
        scenario_dir: temp.path().join("scenario"),
        client_controller_config: None,
    };

    let err = compose_control_from_manifest(&context).unwrap_err();

    assert!(matches!(err, ScenarioError::MissingRunManifest { .. }));
    assert!(err.to_string().contains("run.json"));
}

#[test]
fn outage_operations_log_rejected_writer_write_before_failing_expected_operation() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY);
    let rejected_mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(rejected_mount.parent().unwrap()).unwrap();
    std::fs::write(&rejected_mount, "not a directory").unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", rejected_mount);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    std::fs::create_dir_all(&context.scenario_dir).unwrap();

    let err = perform_postgres_writer_outage_operation(
        &context,
        &client,
        POSTGRES_OUTAGE_FILE_NAME,
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::RejectedOutageOperations {
            rejected: 1,
            total: 1
        }
    ));
    let log =
        std::fs::read_to_string(context.scenario_dir.join("outage-operations.ndjson")).unwrap();
    assert!(log.contains("\"accepted\":false"));
    assert!(log.contains("\"client\":\"client-a\""));
}

#[test]
fn outage_operations_require_existing_scenario_dir() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox: temp.path().join("run-1"),
        scenario_dir: temp.path().join("missing-scenario"),
        client_controller_config: None,
    };
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&context.sandbox, "projectfs", "client-a", mount);

    let err = perform_postgres_writer_outage_operation(
        &context,
        &client,
        POSTGRES_OUTAGE_FILE_NAME,
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD,
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(!context.scenario_dir.exists());
}

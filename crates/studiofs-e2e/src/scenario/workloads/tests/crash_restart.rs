use super::*;

#[test]
fn crash_restart_recovery_runs_both_modes_reuses_client_root_and_converges() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let original_client_root = clients[0].client_root.clone();
    let mut driver =
        RecordingCrashRestartDriver::new(clients).publish_current_file_before_convergence();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.bootstrap.init_client, "client-a");
    assert_eq!(summary.modes.len(), 2);
    assert_eq!(
        summary.modes[0].mode,
        CrashRestartMode::AfterLocalDurabilityBoundary
    );
    assert_eq!(
        summary.modes[1].mode,
        CrashRestartMode::BeforeRemotePublicationCompletes
    );
    assert!(summary.modes.iter().all(|mode| {
        mode.restart_client_root == original_client_root.display().to_string()
            && mode.local_read_checksum_sha256 == mode.final_checksum_sha256
            && mode.final_validator.converged
    }));
    assert!(summary.modes[0].peer_visible_before_convergence.is_none());
    assert!(summary.modes[1].peer_visible_before_convergence.is_none());
    assert_eq!(
        driver.actions.borrow().as_slice(),
        [
            "bootstrap",
            "mount_all",
            "force_kill_client-a",
            "remount_client-a",
            "force_kill_client-a",
            "remount_client-a",
            "unmount_all",
        ]
    );

    let summary_json: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            context
                .scenario_dir
                .join("crash-restart-recovery-summary.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(summary_json["modes"].as_array().unwrap().len(), 2);
    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("after_local_durability_boundary"));
    assert!(operations_log.contains("before_remote_publication_completes"));
}

#[test]
fn crash_restart_recovery_accepts_peer_visible_before_convergence() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-peer-visible");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients).publish_current_file_on_remount();
    let context = ScenarioContext {
        run_id: "run-peer-visible".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(summary.modes[0].peer_visible_before_convergence.is_none());
    assert_eq!(
        summary.modes[1].peer_visible_before_convergence.as_deref(),
        summary.modes[1].local_read_checksum_sha256.as_deref()
    );
    assert!(
        summary
            .modes
            .iter()
            .all(|mode| mode.final_validator.converged)
    );
}

#[test]
fn crash_restart_before_convergence_publish_reports_missing_source() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-missing-source");
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingCrashRestartDriver::new(clients).publish_current_file_before_convergence();

    let err = driver
        .before_peer_convergence_wait(CRASH_RESTART_CLIENT_NAMES[0], "missing.txt")
        .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::Io(error) if error.kind() == std::io::ErrorKind::NotFound
    ));
}

#[test]
fn crash_restart_recovery_uses_recovered_writer_checksum_as_oracle() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-recovered-writer");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients)
        .corrupt_local_on_remount()
        .publish_current_file_on_remount();
    let context = ScenarioContext {
        run_id: "run-recovered-writer".to_string(),
        seed: 404,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    let recovered_checksum = bytes_sha256(b"corrupted payload after restart\n");
    assert_eq!(
        summary.modes[0].local_read_checksum_sha256,
        Some(recovered_checksum.clone())
    );
    assert_eq!(
        summary.modes[0].final_checksum_sha256,
        Some(recovered_checksum)
    );
    assert!(summary.modes[0].final_validator.converged);
}

#[test]
fn crash_restart_before_publication_requires_final_peer_convergence() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-before-publication-writer-only");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingCrashRestartDriver::new(clients.clone()).publish_current_file_before_convergence();
    let context = ScenarioContext {
        run_id: "run-before-publication-writer-only".to_string(),
        seed: 405,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_mode(
        &context,
        &mut driver,
        &clients,
        CrashRestartMode::BeforeRemotePublicationCompletes,
        CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
        b"writer-only before publication\n",
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(
        summary.final_validator.compared_clients,
        CRASH_RESTART_CLIENT_NAMES
            .iter()
            .copied()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        summary.local_read_checksum_sha256,
        summary.final_checksum_sha256
    );
}

#[test]
fn crash_restart_before_publication_fails_when_recovered_state_never_converges() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-before-publication-missing-peer");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients.clone());
    let context = ScenarioContext {
        run_id: "run-before-publication-missing-peer".to_string(),
        seed: 405,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_crash_restart_mode(
        &context,
        &mut driver,
        &clients,
        CrashRestartMode::BeforeRemotePublicationCompletes,
        CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
        b"writer-only before publication\n",
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::CrashRestartPeerConvergenceFailed { .. }
    ));
}

#[test]
fn crash_restart_reports_peer_convergence_failure_when_recovered_state_is_not_published() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients).corrupt_local_on_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::CrashRestartPeerConvergenceFailed { .. }
    ));
}

#[test]
fn crash_restart_reports_unrecoverable_loss_when_writer_cannot_read_after_restart() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-unrecoverable");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingCrashRestartDriver::new(clients).remove_local_on_remount();
    let context = ScenarioContext {
        run_id: "run-unrecoverable".to_string(),
        seed: 404,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(summary.modes.iter().all(|mode| {
        mode.recovery_outcome == CrashRestartRecoveryOutcome::UnrecoverableLoss
            && mode.local_read_checksum_sha256.is_none()
            && mode.final_checksum_sha256.is_none()
            && mode.final_validator.converged
    }));
}

#[test]
fn crash_restart_reports_corrupt_advertised_file_when_final_peer_differs_from_recovered_writer() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let clients = CRASH_RESTART_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingCrashRestartDriver::new(clients).corrupt_converged_payload_after_remount();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::CrashRestartCorruptAdvertisedFile { .. }
    ));
}

#[test]
fn crash_restart_convergence_includes_every_available_client() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CLIENT_CRASH_RESTART_RECOVERY);
    let client_names = ["client-a", "client-b", "client-c"];
    let clients = client_names
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver =
        RecordingCrashRestartDriver::new(clients).publish_current_file_before_convergence();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 327,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_crash_restart_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(
        summary.modes[0].final_validator.compared_clients,
        client_names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        summary.modes[1].final_validator.compared_clients,
        client_names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>()
    );
}

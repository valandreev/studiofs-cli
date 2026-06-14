use super::*;

#[test]
fn namespace_collision_records_operations_and_validates_three_client_convergence() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_RENAME_VS_UNLINK);
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingNamespaceCollisionDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 328,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_namespace_collision_with_driver(
        &context,
        NamespaceCollisionKind::RenameVsUnlink,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.scenario, COLLISION_RENAME_VS_UNLINK);
    assert_eq!(summary.clients, 3);
    assert_eq!(summary.operations.len(), 2);
    assert!(summary.final_validator.converged);
    assert!(summary.namespace_violations.is_empty());
    assert_eq!(
        driver.actions.borrow().as_slice(),
        [
            "bootstrap",
            "mount_all",
            "replicate",
            "replicate",
            "unmount_all"
        ]
    );
    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("\"operation\":\"rename\""));
    assert!(operations_log.contains("\"operation\":\"unlink\""));
    assert!(
        context
            .scenario_dir
            .join("namespace-collision-summary.json")
            .exists()
    );
}

#[test]
fn namespace_collision_preserves_primary_error_when_unmount_also_fails() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_RENAME_VS_UNLINK);
    let mut driver =
        RecordingNamespaceCollisionDriver::new(Vec::new()).with_unmount_error("umount failed");
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 328,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_namespace_collision_with_driver(
        &context,
        NamespaceCollisionKind::RenameVsUnlink,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    let message = err.to_string();
    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionUnmountAfterFailure { .. }
    ));
    assert!(message.contains("requires at least 3 clients"));
    assert!(message.contains("umount failed"));
}

#[test]
fn namespace_collision_recording_replication_collects_errors_after_all_clients() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let source_mount = temp.path().join("mounts").join("client-a");
    let bad_mount = temp.path().join("mounts").join("client-b");
    let copied_mount = temp.path().join("mounts").join("client-c");
    std::fs::create_dir_all(&source_mount).unwrap();
    std::fs::write(source_mount.join("copied.txt"), b"payload").unwrap();
    std::fs::create_dir_all(bad_mount.parent().unwrap()).unwrap();
    std::fs::write(&bad_mount, b"not a directory").unwrap();
    std::fs::create_dir_all(&copied_mount).unwrap();
    let driver = RecordingNamespaceCollisionDriver::new(vec![
        client_spec_for(&sandbox, "projectfs", "client-a", source_mount),
        client_spec_for(&sandbox, "projectfs", "client-b", bad_mount),
        client_spec_for(&sandbox, "projectfs", "client-c", copied_mount.clone()),
    ]);

    let err = driver.replicate_from_first_client().unwrap_err();

    assert!(matches!(err, ScenarioError::ReplicationFailure { .. }));
    assert_eq!(
        std::fs::read(copied_mount.join("copied.txt")).unwrap(),
        b"payload"
    );
}

#[test]
fn namespace_collision_operation_timestamps_are_portable_u64_values() {
    fn accepts_u64(_value: u64) {}

    let operations = planned_namespace_collision_operations(
        NamespaceCollisionKind::RenameVsUnlink,
        Instant::now(),
    );

    accepts_u64(operations[0].timestamp_ms);
}

#[test]
fn namespace_collision_operation_timeout_reports_pending_without_waiting_forever() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-collision-timeout");
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
    let started = Instant::now();

    let err = run_concurrent_namespace_operations(
        started,
        vec![NamespaceCollisionTask::new(
            1,
            client,
            "rename",
            "shared/file.txt -> shared/renamed-a.txt",
            |_client| {
                thread::sleep(Duration::from_millis(200));
                Ok(())
            },
        )],
        Duration::from_millis(20),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert!(
        err.to_string()
            .contains("client-a rename shared/file.txt -> shared/renamed-a.txt")
    );
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "timeout must not wait for the hung namespace worker to finish"
    );
}

#[test]
fn namespace_collision_timeout_does_not_wait_for_blocked_mount_force_kill() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-collision-timeout-cleanup");
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingNamespaceCollisionDriver::new(clients);
    let primary = ScenarioError::NamespaceCollisionOperationTimeout {
        timeout_ms: 30_000,
        pending: vec!["client-a rename shared/file.txt".to_string()],
    };
    let started = Instant::now();

    let err = handle_namespace_collision_operation_timeout::<NamespaceCollisionSummary>(
        primary,
        &mut driver,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "timeout cleanup should return to the parent process boundary instead of waiting for blocked mount force-kill"
    );
    assert_eq!(
        driver.actions.borrow().as_slice(),
        ["emergency_kill_all_no_wait"],
        "operation timeout should delegate cleanup to the parent process boundary"
    );
}

#[test]
fn namespace_collision_mounted_io_timeout_uses_emergency_cleanup() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-collision-mounted-io-timeout");
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingNamespaceCollisionDriver::new(clients);
    let primary = ScenarioError::MountedIoTimeout {
        label: "namespace collision initial tree: write shared/file.txt".to_string(),
        timeout_ms: 5_000,
        cancel_detail: "thread cancellation is not available on this platform".to_string(),
    };

    let err =
        handle_namespace_collision_timeout_error::<NamespaceCollisionSummary>(primary, &mut driver)
            .unwrap_err();

    assert!(matches!(err, ScenarioError::MountedIoTimeout { .. }));
    assert_eq!(
        driver.actions.borrow().as_slice(),
        ["emergency_kill_all_no_wait"],
        "mounted I/O timeout should not attempt graceful unmount while a FUSE callback may be blocked"
    );
}

#[test]
fn namespace_collision_mounted_tree_snapshot_timeout_uses_emergency_cleanup() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-collision-mounted-tree-timeout");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_OVERWRITE_VS_READ);
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingNamespaceCollisionDriver::new(clients);
    let primary = ScenarioError::MountedTree(MountedTreeError::SnapshotUnavailableTimedOut {
        attempts: 2,
        elapsed_ms: 5_258,
        artifact_dir: scenario_dir.join("validation"),
        source: Box::new(MountedTreeError::SnapshotAttemptTimedOut { timeout_ms: 5_000 }),
    });

    let err =
        handle_namespace_collision_timeout_error::<NamespaceCollisionSummary>(primary, &mut driver)
            .unwrap_err();

    assert!(matches!(err, ScenarioError::MountedTree(_)));
    assert_eq!(
        driver.actions.borrow().as_slice(),
        ["emergency_kill_all_no_wait"],
        "mounted tree snapshot timeout should not attempt graceful unmount while a mount callback may be blocked"
    );
}

#[test]
fn namespace_collision_collect_timeout_allows_helper_cleanup_grace() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-collision-helper-grace");
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
    let started = Instant::now();

    let operations = run_concurrent_namespace_operations(
        started,
        vec![
            NamespaceCollisionTask::new(1, client, "rename", "shared/file.txt", |_client| {
                thread::sleep(Duration::from_millis(80));
                Ok(())
            })
            .with_completion_grace(Duration::from_millis(100)),
        ],
        Duration::from_millis(20),
    )
    .unwrap();

    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].result, "accepted");
    assert!(
        started.elapsed() >= Duration::from_millis(80),
        "helper-backed namespace operations must be allowed to finish bounded cleanup after the operation timeout"
    );
}

#[test]
fn namespace_collision_initial_tree_writes_directly() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let sandbox = temp.path().join("run-collision-seed-direct");
    let scenario_dir = sandbox.join("artifacts").join("collision-seed-direct");
    let context = ScenarioContext {
        run_id: "run-collision-seed-direct".to_string(),
        seed: 0,
        sandbox: sandbox.clone(),
        scenario_dir,
        client_controller_config: None,
    };
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);

    establish_collision_initial_tree(&context, NamespaceCollisionKind::RenameVsUnlink, &client)
        .unwrap();

    let payload = std::fs::read(client.mount_point.join("shared/file.txt")).unwrap();
    assert_eq!(payload, b"initial namespace payload\n");
}

#[test]
fn open_handle_sync_retry_accepts_transient_not_found() {
    let mut attempts = 0;

    let retries = sync_open_handle_with_retry(
        || {
            attempts += 1;
            if attempts < 3 {
                Err(io::Error::new(io::ErrorKind::NotFound, "transient"))
            } else {
                Ok(())
            }
        },
        Duration::from_millis(100),
        Duration::from_millis(1),
    )
    .unwrap();

    assert_eq!(retries, 2);
    assert_eq!(attempts, 3);
}

#[test]
fn open_handle_sync_retry_preserves_non_not_found_error() {
    let err = sync_open_handle_with_retry(
        || Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied")),
        Duration::from_millis(100),
        Duration::from_millis(1),
    )
    .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn open_handle_helper_records_progress_for_old_handle_sequence() {
    let temp = tempdir().unwrap();
    let client_a_mount = temp.path().join("mounts").join("client-a");
    let client_b_mount = temp.path().join("mounts").join("client-b");
    let unlink_path = "open-handles/unlink-target.txt";
    let rename_source = "open-handles/rename-source.txt";
    let rename_dest = "open-handles/renamed-destination.txt";
    let append_payload_path = temp.path().join("append.payload");
    let progress = temp.path().join("progress.jsonl");

    std::fs::create_dir_all(client_a_mount.join("open-handles")).unwrap();
    std::fs::create_dir_all(client_b_mount.join("open-handles")).unwrap();
    std::fs::write(client_a_mount.join(unlink_path), b"initial\n").unwrap();
    std::fs::write(client_b_mount.join(unlink_path), b"initial\n").unwrap();
    std::fs::write(client_b_mount.join(rename_source), b"rename source\n").unwrap();
    std::fs::write(&append_payload_path, b"append through old handle\n").unwrap();

    run_open_handle_op_helper(
        client_a_mount.clone(),
        client_b_mount.clone(),
        unlink_path,
        rename_source,
        rename_dest,
        &append_payload_path,
        Some(&progress),
    )
    .unwrap();

    assert!(!client_b_mount.join(unlink_path).exists());
    assert!(client_b_mount.join(rename_dest).exists());
    assert_eq!(
        std::fs::read(client_a_mount.join(unlink_path)).unwrap(),
        b"initial\nappend through old handle\n"
    );
    let progress_log = std::fs::read_to_string(progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_open_old_handle\""));
    assert!(progress_log.contains("\"phase\":\"after_remove_file\""));
    assert!(progress_log.contains("\"phase\":\"after_rename\""));
    assert!(progress_log.contains("\"phase\":\"after_sync_all\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_churn_operation_timeout_reports_pending_without_waiting_forever() {
    let active_workers = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let err = run_timed_hotspot_io_with_worker_limit(
        Duration::from_millis(20),
        "client-a mkdir hotspot".to_string(),
        move || {
            release_rx
                .recv()
                .map_err(|_| io::Error::other("hotspot worker release channel closed"))?;
            Ok(())
        },
        Arc::clone(&active_workers),
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert!(err.to_string().contains("client-a mkdir hotspot"));
    assert_eq!(
        active_workers.load(Ordering::SeqCst),
        1,
        "hotspot timeout must return while the mounted operation worker is still pending"
    );

    release_tx.send(()).unwrap();
    for _ in 0..50 {
        if active_workers.load(Ordering::SeqCst) == 0 {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(active_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn hotspot_rename_source_barrier_accepts_visible_source() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-hotspot-source-visible");
    let mount = temp.path().join("mounts").join("client-a");
    let source = mount.join("hotspot").join("slot-0.txt");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    std::fs::write(&source, b"payload").unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);

    wait_for_client_path_visible_until(
        &client,
        "hotspot/slot-0.txt",
        Instant::now() + Duration::from_secs(1),
        Duration::from_millis(5),
        "hotspot rename source",
    )
    .unwrap();
}

#[test]
fn hotspot_rename_source_barrier_rejects_missing_source_before_rename() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-hotspot-source-missing");
    let mount = temp.path().join("mounts").join("client-a");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);

    let err = wait_for_client_path_visible_until(
        &client,
        "hotspot/slot-0.txt",
        Instant::now() + Duration::from_millis(20),
        Duration::from_millis(5),
        "hotspot rename source",
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(err.to_string().contains("hotspot rename source"));
    assert!(err.to_string().contains("client-a:hotspot/slot-0.txt"));
}

#[test]
fn hotspot_churn_operation_timeout_bounds_pending_workers() {
    let active_workers = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let err = run_timed_hotspot_io_with_worker_limit(
        Duration::from_millis(20),
        "client-a mkdir hotspot".to_string(),
        move || {
            release_rx
                .recv()
                .map_err(|_| io::Error::other("hotspot worker release channel closed"))?;
            Ok(())
        },
        Arc::clone(&active_workers),
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert_eq!(active_workers.load(Ordering::SeqCst), 1);

    let started = Instant::now();
    let err = run_timed_hotspot_io_with_worker_limit(
        Duration::from_secs(1),
        "client-b append hotspot/slot-1.txt".to_string(),
        || Ok(()),
        Arc::clone(&active_workers),
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
    ));
    assert!(err.to_string().contains("hotspot worker limit reached"));
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "saturated hotspot worker limit must fail without spawning another worker"
    );

    release_tx.send(()).unwrap();
    for _ in 0..50 {
        if active_workers.load(Ordering::SeqCst) == 0 {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(active_workers.load(Ordering::SeqCst), 0);
}

#[test]
fn hotspot_churn_rename_targets_are_unique_per_round() {
    let isolation = test_isolation("hotspot");
    let mut targets = std::collections::BTreeSet::new();

    for round in 0..10 {
        let target = hotspot_churn_rename_target(&isolation, round);
        assert!(
            targets.insert(target.clone()),
            "hotspot churn must not reuse rename target {target}"
        );
    }
}

#[test]
fn hotspot_churn_completed_operations_release_worker_before_next_operation() {
    let active_workers = Arc::new(AtomicUsize::new(0));

    for index in 0..256 {
        run_timed_hotspot_io_with_worker_limit(
            Duration::from_secs(1),
            format!("completed hotspot operation {index}"),
            || Ok(()),
            Arc::clone(&active_workers),
            HOTSPOT_IO_MAX_ACTIVE_WORKERS,
        )
        .unwrap();

        assert_eq!(
            active_workers.load(Ordering::SeqCst),
            0,
            "completed hotspot operation {index} returned before releasing its worker permit"
        );
    }
}

#[test]
fn hotspot_helper_internal_timeout_leaves_parent_kill_reserve() {
    assert_eq!(
        hotspot_helper_internal_timeout(Duration::from_secs(15)),
        Duration::from_millis(12_250)
    );
    assert_eq!(
        hotspot_helper_internal_timeout(Duration::from_secs(1)),
        Duration::from_secs(1),
        "short operation timeouts should not underflow"
    );
}

#[test]
fn timed_out_hotspot_helper_child_wait_policy_matches_platform() {
    #[cfg(windows)]
    assert!(
        should_wait_for_timed_out_hotspot_helper_child(),
        "Windows job cleanup can wait for the helper process after force-kill"
    );
    #[cfg(unix)]
    assert!(
        !should_wait_for_timed_out_hotspot_helper_child(),
        "Unix cleanup must not waitpid a FUSE helper that may be stuck in D-state"
    );
}

#[test]
fn hotspot_helper_io_failure_is_recorded_as_rejected_operation() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-helper-reject");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let mount = temp.path().join("mount");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount);
    let context = ScenarioContext {
        run_id: "run-helper-reject".to_string(),
        seed: 415,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let record = timed_hotspot_helper_fs_op(
        &context,
        Instant::now(),
        1,
        &client,
        "rename",
        "missing.txt",
        Duration::from_secs(5),
        None,
        Some("renamed.txt"),
    )
    .unwrap();

    assert_eq!(record.result, "rejected");
    assert!(record.error.unwrap().contains("hotspot helper failed"));
}

#[test]
fn hotspot_unmount_after_timeout_is_fatal_not_rejected_operation() {
    let error = ScenarioError::NamespaceCollisionUnmountAfterFailure {
        primary: namespace_collision_operation_timeout(
            Duration::from_millis(20),
            vec![(1, "client-a rename hotspot/slot-0.txt".to_string())],
        )
        .to_string(),
        cleanup: "taskkill failed".to_string(),
    };

    assert!(hotspot_helper_error_is_fatal(&error));
}

#[test]
fn required_hotspot_churn_operation_is_logged_before_failing() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-required-helper-reject");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let context = ScenarioContext {
        run_id: "run-required-helper-reject".to_string(),
        seed: 415,
        sandbox,
        scenario_dir: scenario_dir.clone(),
        client_controller_config: None,
    };
    let operation = NamespaceCollisionOperation {
        order: 1,
        client: "client-a".to_string(),
        operation: "rename".to_string(),
        path: "hotspot/slot-0.txt".to_string(),
        result: "rejected".to_string(),
        timestamp_ms: 0,
        error: Some("source disappeared".to_string()),
    };
    let mut operations = Vec::new();

    let err = record_required_sparse_operation(&context, &mut operations, operation)
        .expect_err("rejected hotspot churn operations must fail the scenario");

    assert!(err.to_string().contains("source disappeared"));
    assert_eq!(operations.len(), 1);
    let log = std::fs::read_to_string(scenario_dir.join("operations.ndjson")).unwrap();
    assert!(log.contains("\"result\":\"rejected\""));
    assert!(log.contains("source disappeared"));
}

#[test]
fn hotspot_overwrite_can_use_barrier_helper_while_recording_overwrite() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-helper-barrier");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let mount = temp.path().join("mount");
    std::fs::create_dir_all(&mount).unwrap();
    let client = client_spec_for(&sandbox, "projectfs", "client-a", mount.clone());
    let context = ScenarioContext {
        run_id: "run-helper-barrier".to_string(),
        seed: 415,
        sandbox,
        scenario_dir: scenario_dir.clone(),
        client_controller_config: None,
    };

    let record = timed_hotspot_helper_fs_op_with_helper(
        &context,
        Instant::now(),
        1,
        &client,
        "overwrite",
        "overwrite-barrier",
        "hotspot/slot-1.txt",
        Duration::from_secs(5),
        Some(b"barrier payload"),
        None,
    )
    .unwrap();

    assert_eq!(record.operation, "overwrite");
    assert_eq!(record.result, "accepted");
    assert_eq!(
        std::fs::read(mount.join("hotspot").join("slot-1.txt")).unwrap(),
        b"barrier payload"
    );

    let progress_dir = scenario_dir.join("hotspot-helper-progress");
    let progress_file = std::fs::read_dir(progress_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.to_string_lossy().contains("overwrite-barrier"))
        .expect("barrier helper progress file");
    let progress_log = std::fs::read_to_string(progress_file).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_open_barrier_write\""));
    assert!(progress_log.contains("\"phase\":\"after_readback\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_helper_stat_records_exact_metadata_probe() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mount");
    let progress = temp.path().join("progress.jsonl");
    std::fs::create_dir_all(mount.join("hotspot")).unwrap();
    std::fs::write(mount.join("hotspot").join("slot-1.txt"), b"payload").unwrap();

    run_hotspot_op_helper(
        "stat",
        mount,
        "hotspot/slot-1.txt",
        None,
        None,
        Some(&progress),
    )
    .unwrap();

    let progress_log = std::fs::read_to_string(progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_metadata\""));
    assert!(progress_log.contains("\"phase\":\"after_metadata\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[cfg(unix)]
#[test]
fn hotspot_helper_timeout_is_enforced_before_parent_kill_on_unix() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mount");
    let payload = temp.path().join("blocked-payload.fifo");
    let progress = temp.path().join("progress.jsonl");
    std::fs::create_dir_all(&mount).unwrap();
    let status = std::process::Command::new("mkfifo")
        .arg(&payload)
        .status()
        .unwrap();
    assert!(status.success());

    let err = run_hotspot_op_helper_with_timeout(
        "create",
        mount.clone(),
        "blocked.txt",
        None,
        Some(&payload),
        Some(&progress),
        Some(Duration::from_millis(20)),
    )
    .unwrap_err();

    assert!(
        err.to_string()
            .contains(HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER),
        "helper should convert blocked mounted I/O into its internal timeout marker before the parent process timeout"
    );
    let progress_log = std::fs::read_to_string(&progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"timeout_cancel_requested\""));

    std::fs::write(&payload, b"payload").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while HOTSPOT_HELPER_TIMED_IO_ACTIVE_WORKERS.load(Ordering::Acquire) != 0
        && std::time::Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        HOTSPOT_HELPER_TIMED_IO_ACTIVE_WORKERS.load(Ordering::Acquire),
        0
    );
}

#[test]
fn hotspot_helper_unlink_removes_file_and_records_progress() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mount");
    let progress = temp.path().join("progress.jsonl");
    let target = mount.join("hotspot").join("unlink-target.txt");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, b"payload").unwrap();

    run_hotspot_op_helper(
        "unlink",
        mount,
        "hotspot/unlink-target.txt",
        None,
        None,
        Some(&progress),
    )
    .unwrap();

    assert!(!target.exists());
    let progress_log = std::fs::read_to_string(progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_remove_file\""));
    assert!(progress_log.contains("\"phase\":\"after_remove_file\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_helper_records_progress_for_sync_barrier_phases() {
    let temp = tempdir().unwrap();
    let mount = temp.path().join("mount");
    let payload = temp.path().join("payload.bin");
    let progress = temp.path().join("progress.jsonl");
    std::fs::create_dir_all(&mount).unwrap();
    std::fs::write(&payload, b"hotspot payload").unwrap();

    run_hotspot_op_helper(
        "overwrite",
        mount,
        "hotspot/slot-1.txt",
        None,
        Some(&payload),
        Some(&progress),
    )
    .unwrap();

    let progress_log = std::fs::read_to_string(progress).unwrap();
    assert!(progress_log.contains("\"phase\":\"before_write\""));
    assert!(progress_log.contains("\"phase\":\"after_write\""));
    assert!(progress_log.contains("\"phase\":\"before_sync_all\""));
    assert!(progress_log.contains("\"phase\":\"after_sync_all\""));
    assert!(progress_log.contains("\"phase\":\"completed\""));
}

#[test]
fn hotspot_helper_progress_records_complete_ndjson_lines() {
    let temp = tempdir().unwrap();
    let progress = temp.path().join("progress.jsonl");
    let path = temp.path().join("mount").join("hotspot").join("slot.txt");

    record_hotspot_helper_progress(
        Some(&progress),
        "before_write",
        "overwrite",
        &path,
        Some("target.txt"),
        Some(42),
    )
    .unwrap();
    record_hotspot_helper_progress(
        Some(&progress),
        "after_write",
        "overwrite",
        &path,
        None,
        None,
    )
    .unwrap();

    let progress_log = std::fs::read_to_string(progress).unwrap();
    let lines = progress_log.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    for line in lines {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(value["operation"], "overwrite");
        assert!(value["timestamp_ms"].is_u64());
    }
    assert!(
        progress_log.ends_with('\n'),
        "progress log should end each NDJSON record with a newline"
    );
}

#[test]
fn overwrite_vs_read_collision_records_repeated_read_loop() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_OVERWRITE_VS_READ);
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingNamespaceCollisionDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 328,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = run_namespace_collision_with_driver(
        &context,
        NamespaceCollisionKind::OverwriteVsRead,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    let read_loop = summary
        .operations
        .iter()
        .find(|operation| operation.operation == "read_loop")
        .expect("overwrite/read scenario should record repeated reads");
    assert_eq!(read_loop.result, "accepted");
}

#[test]
fn readdir_stat_collision_uses_scoped_final_validation() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-readdir-stat-final");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_READDIR_STAT_DURING_MUTATION);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(mount.join("scan")).unwrap();
            std::fs::write(mount.join("scan").join("renamed-alpha.data"), b"a\n").unwrap();
            std::fs::write(mount.join("scan").join("source-b.txt"), b"b\n").unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-readdir-stat-final".to_string(),
        seed: 328,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let summary = finish_namespace_collision(
        &context,
        NamespaceCollisionKind::ReaddirStatDuringMutation,
        None,
        clients,
        planned_namespace_collision_operations(
            NamespaceCollisionKind::ReaddirStatDuringMutation,
            Instant::now(),
        ),
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap();

    assert!(summary.final_validator.converged);
    assert!(summary.namespace_violations.is_empty());
    assert_eq!(summary.operations.len(), 3);
}

#[test]
fn namespace_collision_initial_validation_skips_seed_writer_payload_probe() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-initial-skip");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(COLLISION_OVERWRITE_VS_READ);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let clients = COLLISION_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-initial-skip".to_string(),
        seed: 328,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    for client in clients.iter().skip(1) {
        let path = client.mount_point.join("shared/data.txt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, COLLISION_OLD_COMMITTED_PAYLOAD).unwrap();
    }

    let missing = namespace_collision_initial_state_missing(
        &context,
        NamespaceCollisionKind::OverwriteVsRead,
        &clients,
    )
    .unwrap();

    assert!(
        missing.is_empty(),
        "seed writer was already read back by the seed helper and should not be probed again: {missing:?}"
    );
}

#[test]
fn mounted_smoke_reports_expected_and_actual_client_count() {
    let temp = tempdir().unwrap();
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(BASIC_MULTI_CLIENT_SMOKE);
    let clients = SMOKE_CLIENT_NAMES[..2]
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let mut driver = RecordingSmokeDriver::new(clients);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 324,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let err = run_mounted_smoke_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ScenarioError::InsufficientMountedClients {
            expected: 3,
            actual: 2
        }
    ));
}

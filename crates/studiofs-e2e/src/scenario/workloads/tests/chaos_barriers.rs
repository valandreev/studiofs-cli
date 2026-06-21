use super::*;

#[test]
fn mounted_chaos_initial_barrier_rejects_unreplicated_seed_tree() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CHAOS_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("chaos-test");
    seed_mounted_chaos_initial_tree(&clients[0], &isolation).unwrap();

    let err = wait_for_mounted_chaos_initial_tree(
        &context,
        &clients,
        &isolation,
        Duration::from_millis(25),
        Duration::from_millis(5),
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(
        err.to_string()
            .contains("mounted chaos initial tree did not become visible")
    );
}

#[test]
fn mounted_chaos_initial_seed_rejects_stale_zero_byte_file() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let writer = client_spec_for(
        &sandbox,
        "projectfs",
        "client-a",
        temp.path().join("mounts").join("client-a"),
    );
    let isolation = test_isolation("chaos-test");
    let stale_path = writer.mount_point.join(isolation.path("rename-source.txt"));
    std::fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
    std::fs::write(&stale_path, []).unwrap();

    let err = seed_mounted_chaos_initial_tree(&writer, &isolation).unwrap_err();

    let err_text = err.to_string().to_ascii_lowercase();
    assert!(
        err_text.contains("mounted chaos initial tree") && err_text.contains("file exists"),
        "expected stale warmup file to be rejected before convergence validation, got {err:?}"
    );
    assert_eq!(
        std::fs::read(&stale_path).unwrap(),
        Vec::<u8>::new(),
        "stale warmup files must not be overwritten in place"
    );
}

#[test]
fn stale_remount_warm_history_rejects_stale_zero_byte_nested_file() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = temp.path().join("scenario");
    let shared_mount = temp.path().join("mounts").join("shared");
    std::fs::create_dir_all(&scenario_dir).unwrap();
    std::fs::create_dir_all(&shared_mount).unwrap();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox: sandbox.clone(),
        scenario_dir,
        client_controller_config: None,
    };
    let clients = STALE_REMOUNT_WARM_WRITER_NAMES
        .iter()
        .copied()
        .map(|name| client_spec_for(&sandbox, "projectfs", name, shared_mount.clone()))
        .collect::<Vec<_>>();
    let isolation = test_isolation("stale-remount-test");
    let nested_path = shared_mount.join(isolation.path(COLD_OBSERVER_NESTED_FILE));
    std::fs::create_dir_all(nested_path.parent().unwrap()).unwrap();
    std::fs::write(&nested_path, []).unwrap();

    let err =
        run_stale_remount_warm_history(&context, Instant::now(), &clients, &isolation, 3, &[])
            .unwrap_err();

    assert!(
        err.to_string()
            .contains(&isolation.path(COLD_OBSERVER_NESTED_FILE)),
        "expected stale nested file rejection, got {err:?}"
    );
    assert_eq!(
        std::fs::read(&nested_path).unwrap(),
        Vec::<u8>::new(),
        "stale warm-history files must not be overwritten in place"
    );
}

#[test]
fn stale_remount_warm_history_uses_bounded_helper_for_rename() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = temp.path().join("scenario");
    let shared_mount = temp.path().join("mounts").join("shared");
    std::fs::create_dir_all(&scenario_dir).unwrap();
    std::fs::create_dir_all(&shared_mount).unwrap();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 325,
        sandbox: sandbox.clone(),
        scenario_dir: scenario_dir.clone(),
        client_controller_config: None,
    };
    let clients = STALE_REMOUNT_WARM_WRITER_NAMES
        .iter()
        .copied()
        .map(|name| client_spec_for(&sandbox, "projectfs", name, shared_mount.clone()))
        .collect::<Vec<_>>();
    let isolation = test_isolation("stale-remount-helper-test");

    let (operations, _summary) =
        run_stale_remount_warm_history(&context, Instant::now(), &clients, &isolation, 3, &[])
            .unwrap();

    assert!(
        operations
            .iter()
            .any(|operation| { operation.operation == "rename" && operation.result == "accepted" })
    );
    let progress_dir = scenario_dir.join("hotspot-helper-progress");
    let rename_progress_log = std::fs::read_dir(progress_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .find(|log| log.contains("\"operation\":\"rename\""))
        .expect("stale-remount rename should run through bounded helper");
    assert!(rename_progress_log.contains("\"phase\":\"before_rename\""));
    assert!(rename_progress_log.contains("\"phase\":\"after_rename\""));
}

#[test]
fn mounted_initial_payload_barrier_rejects_wrong_peer_payload() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    write_mounted_initial_file(
        clients[0].mount_point.join("seed/file.txt"),
        b"expected payload\n",
        "test initial tree",
    )
    .unwrap();
    write_mounted_initial_file(
        clients[1].mount_point.join("seed/file.txt"),
        b"wrong payload\n",
        "test initial tree",
    )
    .unwrap();

    let missing = clients
        .iter()
        .filter_map(|client| {
            match mounted_initial_payload_probe(
                client.clone(),
                "seed/file.txt".to_string(),
                b"expected payload\n".to_vec(),
            ) {
                MountedPayloadProbeStatus::Present => None,
                MountedPayloadProbeStatus::Missing(path) => Some(path),
            }
        })
        .collect::<Vec<_>>();

    assert!(
        missing.iter().any(|entry| {
            entry.contains("client-b:seed/file.txt")
                && entry.contains("expected 17 bytes sha256=")
                && entry.contains("read 14 bytes sha256=")
        }),
        "wrong peer payload must be reported in missing initial payloads, got {missing:?}"
    );
}

#[test]
fn mounted_initial_payload_probe_timeout_reports_blocked_client_without_waiting_forever() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let clients = ["client-a", "client-b"]
        .into_iter()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let required_payloads = [("seed/file.txt", b"expected payload\n".as_slice())];
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    let started = Instant::now();

    let missing = missing_mounted_initial_payloads_with_probe(
        &clients,
        &required_payloads,
        &probe_limiter,
        Duration::from_millis(20),
        |client, _relative_path, _expected_payload| {
            if client.name == "client-b" {
                thread::sleep(Duration::from_secs(5));
            }
            MountedPayloadProbeStatus::Present
        },
    );

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "blocked mounted probe must not consume the scenario-child timeout"
    );
    assert_eq!(
        missing,
        vec!["client-b:seed/file.txt (probe timed out after 20 ms)"]
    );
}

#[test]
fn mounted_probe_timeout_poison_skips_remaining_validation_probes() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let clients = ["client-a", "client-b"]
        .into_iter()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let required_payloads = [
        ("seed/file-a.txt", b"expected payload\n".as_slice()),
        ("seed/file-b.txt", b"expected payload\n".as_slice()),
    ];
    let probe_limiter = MountedProbeLimiter::new(MOUNTED_VALIDATION_MAX_ACTIVE_PROBES);
    let started = Instant::now();

    let missing = missing_mounted_initial_payloads_with_probe(
        &clients,
        &required_payloads,
        &probe_limiter,
        Duration::from_millis(20),
        |client, _relative_path, _expected_payload| {
            if client.name == "client-a" {
                thread::sleep(Duration::from_secs(5));
            }
            MountedPayloadProbeStatus::Present
        },
    );

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "remaining mounted probes must fail fast after the first blocked probe timeout"
    );
    assert_eq!(
            missing,
            vec![
                "client-a:seed/file-a.txt (probe timed out after 20 ms)".to_string(),
                "client-a:seed/file-b.txt (probe skipped because a previous mounted probe timed out; active probe workers 1/1)".to_string(),
                "client-b:seed/file-a.txt (probe skipped because a previous mounted probe timed out; active probe workers 1/1)".to_string(),
                "client-b:seed/file-b.txt (probe skipped because a previous mounted probe timed out; active probe workers 1/1)".to_string(),
            ]
        );
}

#[test]
fn open_handle_initial_seed_rejects_stale_zero_byte_file() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let writer = client_spec_for(
        &sandbox,
        "projectfs",
        "client-a",
        temp.path().join("mounts").join("client-a"),
    );
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_OPEN_HANDLE_CONVERGENCE);
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("open-handles-test");
    let stale_path = writer.mount_point.join(open_handle_isolated_path(
        &isolation,
        OPEN_HANDLE_RENAME_SOURCE,
    ));
    std::fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
    std::fs::write(&stale_path, []).unwrap();

    let err =
        seed_open_handle_initial_tree(&context, &[writer], &isolation, Duration::from_secs(1))
            .unwrap_err();

    let err_text = err.to_string().to_ascii_lowercase();
    assert!(
        err_text.contains("open handle seed") && err_text.contains("file exists"),
        "expected stale open-handle seed file to be rejected, got {err:?}"
    );
}

#[test]
fn mounted_chaos_initial_readback_waits_for_delayed_visibility() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let file_path = temp.path().join("chaos").join("delayed.txt");
    std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
    std::fs::write(&file_path, b"").unwrap();
    let payload = b"delayed mounted readback\n".to_vec();
    let writer_path = file_path.clone();
    let writer_payload = payload.clone();

    let writer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(40));
        std::fs::write(writer_path, writer_payload).unwrap();
    });

    let readback = wait_for_mounted_payload_readback(
        &file_path,
        &payload,
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .unwrap();
    writer.join().unwrap();

    assert_eq!(readback, payload);
}

#[test]
fn mounted_payload_readback_distinguishes_missing_path_from_empty_file() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let missing_path = temp.path().join("missing-empty.txt");

    let pool = BoundedMountedIoPool::new(1);
    let err = wait_for_mounted_payload_readback_with_pool(
        &pool,
        &missing_path,
        b"",
        Duration::from_secs(1),
        Duration::from_millis(1),
        Duration::from_secs(1),
    )
    .unwrap_err();

    assert!(
        matches!(&err, ScenarioError::Io(error) if error.kind() == io::ErrorKind::NotFound),
        "missing path with empty expected payload must not pass readback, got {err:?}"
    );
}

#[test]
fn mounted_payload_readback_accepts_existing_empty_file() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let empty_path = temp.path().join("empty.txt");
    std::fs::write(&empty_path, []).unwrap();

    let pool = BoundedMountedIoPool::new(1);
    let readback = wait_for_mounted_payload_readback_with_pool(
        &pool,
        &empty_path,
        b"",
        Duration::from_secs(1),
        Duration::from_millis(1),
        Duration::from_secs(1),
    )
    .unwrap();

    assert!(readback.is_empty());
}

#[test]
fn mounted_payload_readback_does_not_start_zero_millisecond_probe() {
    assert_eq!(
        mounted_payload_readback_probe_timeout(Duration::from_micros(500)),
        None,
        "sub-millisecond deadline remainder would report timeout_ms=0"
    );
    assert_eq!(
        mounted_payload_readback_probe_timeout(Duration::from_millis(1)),
        Some(Duration::from_millis(1))
    );
    assert_eq!(
        mounted_payload_readback_probe_timeout(MOUNTED_WRITE_READBACK_TIMEOUT * 2),
        Some(MOUNTED_WRITE_READBACK_TIMEOUT)
    );
}

#[test]
fn bounded_mounted_io_times_out_blocked_operation() {
    let pool = BoundedMountedIoPool::new(1);

    let started = Instant::now();
    let err = pool
        .run(
            "blocked read",
            Duration::from_millis(20),
            || -> io::Result<Vec<u8>> {
                thread::sleep(Duration::from_millis(200));
                Ok(Vec::new())
            },
        )
        .unwrap_err();

    assert!(
        started.elapsed() < Duration::from_millis(150),
        "bounded mounted I/O should return on timeout instead of waiting for the worker"
    );
    assert!(
        matches!(err, ScenarioError::MountedIoTimeout { .. }),
        "expected mounted I/O timeout, got {err:?}"
    );
}

#[test]
fn bounded_mounted_io_fails_fast_when_previous_worker_is_stuck() {
    let pool = BoundedMountedIoPool::new(1);

    let first = pool.run(
        "blocked write",
        Duration::from_millis(20),
        || -> io::Result<()> {
            thread::sleep(Duration::from_millis(200));
            Ok(())
        },
    );
    assert!(matches!(first, Err(ScenarioError::MountedIoTimeout { .. })));

    let started = Instant::now();
    let second = pool
        .run(
            "later read",
            Duration::from_secs(1),
            || -> io::Result<()> { Ok(()) },
        )
        .unwrap_err();

    assert!(
        started.elapsed() < Duration::from_millis(100),
        "second mounted I/O should fail fast while the previous worker is still active"
    );
    assert!(
        matches!(second, ScenarioError::MountedIoWorkerLimit { .. }),
        "expected mounted I/O worker-limit diagnostic, got {second:?}"
    );
}

#[test]
fn mounted_chaos_initial_barrier_accepts_replicated_seed_tree() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_CHAOS_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("chaos-test");
    seed_mounted_chaos_initial_tree(&clients[0], &isolation).unwrap();
    for peer in clients.iter().skip(1) {
        replace_dir_contents(&clients[0].mount_point, &peer.mount_point).unwrap();
    }

    let convergence = wait_for_mounted_chaos_initial_tree(
        &context,
        &clients,
        &isolation,
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .unwrap();

    assert!(convergence.comparison.converged);
    assert_eq!(convergence.comparison.differences, Vec::new());
}

#[test]
fn hotspot_churn_initial_barrier_rejects_unreplicated_hotspot_dir() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("hotspot-test");
    std::fs::create_dir_all(clients[0].mount_point.join(&isolation.root)).unwrap();

    let err = wait_for_hotspot_initial_tree(
        &context,
        &clients,
        &isolation.root,
        Duration::from_millis(25),
        Duration::from_millis(5),
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(
        err.to_string()
            .contains("hotspot initial tree did not become visible")
    );
}

#[test]
fn hotspot_churn_initial_barrier_accepts_replicated_hotspot_dir() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_HOTSPOT_CHURN_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(mount.join("hotspot-test")).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let convergence = wait_for_hotspot_initial_tree(
        &context,
        &clients,
        "hotspot-test",
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .unwrap();

    assert!(convergence.comparison.converged);
    assert_eq!(convergence.comparison.differences, Vec::new());
}

#[test]
fn advanced_mounted_validate_rejects_failed_long_history_oracle() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-long-history-validation");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let summary = AdvancedMountedSummary {
        scenario: MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE.to_string(),
        bootstrap: None,
        clients: 4,
        operations: Vec::new(),
        operation_log: format!(
            "artifacts/scenarios/{MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE}/operations.ndjson"
        ),
        final_validator: TreeComparison {
            converged: true,
            reference_client: Some("client-a".to_string()),
            compared_clients: vec![
                "client-a".to_string(),
                "client-b".to_string(),
                "client-c".to_string(),
                "client-d".to_string(),
            ],
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
        lww_oracle: None,
        sparse_ranges: None,
        cold_observer: None,
        long_history: Some(LongHistorySummary {
            bounded_operation_count: 11,
            expected_present_paths: vec!["missing.txt".to_string()],
            expected_absent_paths: Vec::new(),
            final_checksum_sha256: "bad-checksum".to_string(),
        }),
        long_history_validation: Some(LongHistoryValidation {
            matched: false,
            missing_present_paths: vec!["client-a:missing.txt".to_string()],
            unexpected_present_paths: Vec::new(),
            checksum_mismatches: Vec::new(),
        }),
        catchup: Some(planned_catchup_summary()),
        invalidation_dropout: None,
        symlink_attributes: None,
    };
    std::fs::write(
        scenario_dir.join("stale-remount-cold-discover-summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    let context = ScenarioContext {
        run_id: "run-long-history-validation".to_string(),
        seed: 418,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let scenario =
        AdvancedMountedScenario::new(AdvancedMountedKind::StaleRemountColdDiscoverCatchup);

    let result = scenario.validate(&context);
    assert!(result.is_err());
    let validation: serde_json::Value = serde_json::from_slice(
        &std::fs::read(context.scenario_dir.join("validation.json")).unwrap(),
    )
    .unwrap();

    assert_eq!(validation["status"], "failed");
}

#[test]
fn open_handle_initial_barrier_rejects_unreplicated_initial_tree() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_OPEN_HANDLE_CONVERGENCE);
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let isolation = test_isolation("open-handles-test");
    seed_open_handle_initial_tree(&context, &clients, &isolation, Duration::from_secs(1)).unwrap();

    let err = wait_for_open_handle_initial_tree(
        &context,
        &clients,
        &isolation,
        Duration::from_millis(25),
        Duration::from_millis(5),
    )
    .unwrap_err();

    assert!(matches!(err, ScenarioError::Io(_)));
    assert!(
        err.to_string()
            .contains("open handle initial tree did not become visible")
    );
}

#[test]
fn open_handle_initial_barrier_accepts_replicated_initial_tree() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_OPEN_HANDLE_CONVERGENCE);
    let isolation = test_isolation("open-handles-test");
    let clients = ADVANCED_MOUNTED_CLIENT_NAMES
        .iter()
        .copied()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            let unlink_target = open_handle_isolated_path(&isolation, OPEN_HANDLE_UNLINK_TARGET);
            let rename_source = open_handle_isolated_path(&isolation, OPEN_HANDLE_RENAME_SOURCE);
            write_parented(
                mount.join(&unlink_target),
                &isolation.payload(
                    "initial_unlink_target",
                    &unlink_target,
                    &String::from_utf8_lossy(OPEN_HANDLE_UNLINK_TARGET_PAYLOAD),
                ),
            )
            .unwrap();
            write_parented(
                mount.join(&rename_source),
                &isolation.payload(
                    "initial_rename_source",
                    &rename_source,
                    &String::from_utf8_lossy(OPEN_HANDLE_RENAME_SOURCE_PAYLOAD),
                ),
            )
            .unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };

    let convergence = wait_for_open_handle_initial_tree(
        &context,
        &clients,
        &isolation,
        Duration::from_secs(1),
        Duration::from_millis(5),
    )
    .unwrap();

    assert!(convergence.comparison.converged);
    assert_eq!(convergence.comparison.differences, Vec::new());
    let helper_progress = read_helper_progress(&context.scenario_dir);
    assert!(
        helper_progress.contains("\"operation\":\"read-exact\""),
        "open handle initial barrier must use helper-isolated read-exact probes"
    );
}

#[test]
fn mounted_smoke_reports_unmount_error_when_body_also_fails() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(BASIC_MULTI_CLIENT_SMOKE);
    let mut driver = RecordingSmokeDriver::new(Vec::new()).with_unmount_error("umount failed");
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

    let message = err.to_string();
    assert!(matches!(
        err,
        ScenarioError::MountedSmokeUnmountAfterFailure { .. }
    ));
    assert!(message.contains("requires at least 3 clients"));
    assert!(message.contains("umount failed"));
}

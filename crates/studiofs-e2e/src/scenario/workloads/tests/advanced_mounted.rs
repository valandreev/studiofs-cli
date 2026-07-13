use super::*;

#[test]
fn advanced_mounted_isolation_uses_volume_specific_roots_and_payloads() {
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox: PathBuf::from("sandbox"),
        scenario_dir: PathBuf::from("scenario"),
        client_controller_config: None,
    };

    let first = ScenarioIsolation::for_advanced(
        &context,
        AdvancedMountedKind::HotspotChurn,
        "projectfs-volume-a",
    );
    let second = ScenarioIsolation::for_advanced(
        &context,
        AdvancedMountedKind::HotspotChurn,
        "projectfs-volume-b",
    );

    assert_ne!(first.root, "hotspot");
    assert_ne!(first.root, second.root);
    assert!(first.path("slot-0.txt").starts_with("hotspot-"));
    assert_ne!(
        first.payload("overwrite", "slot-0.txt", "body"),
        second.payload("overwrite", "slot-0.txt", "body"),
        "payloads must vary with the mounted volume identity"
    );
}

#[test]
fn open_handle_timeout_cleanup_releases_completed_helper_hold() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let context = ScenarioContext {
        run_id: "open-handle-timeout-cleanup".to_string(),
        seed: 0,
        sandbox: temp.path().join("sandbox"),
        scenario_dir: temp.path().join("scenario"),
        client_controller_config: None,
    };
    let marker = create_completed_hotspot_helper_hold_marker(
        &context,
        &context.scenario_dir.join("open-handle.jsonl"),
    )
    .expect("create held helper marker");

    release_completed_hotspot_helpers_for_advanced_kind(&context, AdvancedMountedKind::OpenHandle)
        .expect("release held open-handle helper");

    assert!(
        !marker.exists(),
        "timeout cleanup must release a detached completed open-handle helper"
    );
}

#[test]
fn mounted_chaos_read_target_payload_is_direct_io_aligned() {
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox: PathBuf::from("sandbox"),
        scenario_dir: PathBuf::from("scenario"),
        client_controller_config: None,
    };
    let isolation =
        ScenarioIsolation::for_advanced(&context, AdvancedMountedKind::MountedChaos, "volume-a");
    let read_target = isolation.path("read-target.txt");

    let payload = mounted_chaos_payload(&isolation, "overwrite", &read_target, "new payload\n");

    assert_eq!(payload.len(), MOUNTED_CHAOS_DIRECT_IO_PAYLOAD_LEN);
    assert!(payload.len().is_multiple_of(4096));
    assert!(payload.starts_with(isolation.payload_marker.as_bytes()));
}

#[test]
fn mounted_chaos_non_read_target_payload_stays_compact() {
    let context = ScenarioContext {
        run_id: "run-1".to_string(),
        seed: 410,
        sandbox: PathBuf::from("sandbox"),
        scenario_dir: PathBuf::from("scenario"),
        client_controller_config: None,
    };
    let isolation =
        ScenarioIsolation::for_advanced(&context, AdvancedMountedKind::MountedChaos, "volume-a");
    let append_target = isolation.path("append-target.txt");

    let payload = mounted_chaos_payload(&isolation, "append", &append_target, "append\n");

    assert!(payload.len() < MOUNTED_CHAOS_DIRECT_IO_PAYLOAD_LEN);
}

#[test]
fn isolated_sparse_ranges_fit_final_logical_size() {
    let context = ScenarioContext {
        run_id: "focused-13-17-20260515".to_string(),
        seed: 377,
        sandbox: PathBuf::from("sandbox"),
        scenario_dir: PathBuf::from("scenario"),
        client_controller_config: None,
    };
    let isolation = ScenarioIsolation::for_advanced(
        &context,
        AdvancedMountedKind::SparseRange,
        "projectfs-volume-sparse-range-contract",
    );
    let sparse_path = isolation.path(
        SPARSE_FILE_PATH
            .strip_prefix("sparse/")
            .unwrap_or(SPARSE_FILE_PATH),
    );
    let ranges = sparse_expected_range_specs(Some((&isolation, &sparse_path)));
    let final_logical_size = sparse_final_logical_size(&ranges);

    assert!(
        final_logical_size > SPARSE_FINAL_LOGICAL_SIZE,
        "realistic isolated payloads should extend beyond the legacy fixed EOF"
    );

    for range in ranges {
        let end = range.offset + range.payload.len() as u64;
        assert!(
            end <= final_logical_size,
            "{} range ends at {end}, beyond sparse logical size {final_logical_size}",
            range.label
        );
    }
}

#[test]
fn isolated_sparse_ranges_fit_each_phase_logical_size() {
    let context = ScenarioContext {
        run_id: "repro-s17-sparse-after-contract-fix".to_string(),
        seed: 0,
        sandbox: PathBuf::from("sandbox"),
        scenario_dir: PathBuf::from("scenario"),
        client_controller_config: None,
    };
    let isolation = ScenarioIsolation::for_advanced(
        &context,
        AdvancedMountedKind::SparseRange,
        "sfs-e2e-repro-s17-sparse-after-contract-fix-0-28afb97ba53ab496",
    );
    let sparse_path = isolation.path(
        SPARSE_FILE_PATH
            .strip_prefix("sparse/")
            .unwrap_or(SPARSE_FILE_PATH),
    );
    let ranges = sparse_expected_range_specs(Some((&isolation, &sparse_path)));

    for (phase, logical_size, expected_ranges) in [
        (
            "initial sparse ranges",
            sparse_logical_size(SPARSE_INITIAL_LOGICAL_SIZE, &ranges[..2]),
            &ranges[..2],
        ),
        (
            "shrunk sparse ranges",
            sparse_logical_size(SPARSE_SHRUNK_LOGICAL_SIZE, &ranges[..2]),
            &ranges[..2],
        ),
        (
            "final sparse ranges",
            sparse_final_logical_size(&ranges),
            &ranges[..],
        ),
    ] {
        for range in expected_ranges {
            let end = range.offset + range.payload.len() as u64;
            assert!(
                end <= logical_size,
                "{phase}: {} range ends at {end}, beyond sparse logical size {logical_size}",
                range.label
            );
        }
    }
}

#[test]
fn sparse_helper_ops_cover_range_write_truncate_and_probes() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let mount = temp.path().join("mount");
    std::fs::create_dir_all(&mount).unwrap();
    let progress = temp.path().join("sparse-progress.jsonl");
    let payload_path = temp.path().join("payload.bin");
    std::fs::write(&payload_path, b"boundary").unwrap();

    run_sparse_op_helper(
        "write-range",
        &mount,
        "sparse/shared.bin",
        Some(1024),
        None,
        Some(&payload_path),
        Some(&progress),
    )
    .unwrap();
    run_sparse_op_helper(
        "truncate",
        &mount,
        "sparse/shared.bin",
        None,
        Some(4096),
        None,
        Some(&progress),
    )
    .unwrap();
    run_sparse_op_helper(
        "read-range-zero",
        &mount,
        "sparse/shared.bin",
        Some(0),
        Some(512),
        None,
        Some(&progress),
    )
    .unwrap();
    run_sparse_op_helper(
        "read-range-exact",
        &mount,
        "sparse/shared.bin",
        Some(1024),
        None,
        Some(&payload_path),
        Some(&progress),
    )
    .unwrap();
    let bytes = std::fs::read(temp.path().join("mount/sparse/shared.bin")).unwrap();
    assert_eq!(bytes.len(), 4096);
    assert_eq!(&bytes[1024..1032], b"boundary");
    let progress = std::fs::read_to_string(progress).unwrap();
    assert!(progress.contains("\"operation\":\"write-range\""));
    assert!(progress.contains("\"operation\":\"truncate\""));
    assert!(progress.contains("\"operation\":\"read-range-zero\""));
    assert!(progress.contains("\"operation\":\"read-range-exact\""));
}

#[test]
fn sparse_write_timeout_allows_winfsp_cleanup_purge_slack() {
    assert!(
        MOUNTED_SPARSE_WRITE_TIMEOUT >= MOUNTED_WRITE_READBACK_TIMEOUT,
        "sparse range writes and truncates must not have less time than generic mounted readback"
    );
    #[cfg(not(target_os = "macos"))]
    assert!(
        MOUNTED_SPARSE_WRITE_TIMEOUT > MOUNTED_WRITE_READBACK_TIMEOUT,
        "WinFSP sparse range writes need slack beyond the generic mounted readback timeout"
    );
    assert!(
        MOUNTED_SPARSE_WRITE_TIMEOUT >= HOTSPOT_CHURN_OPERATION_TIMEOUT,
        "sparse range writes should use the mounted helper operation timeout budget"
    );
}

#[test]
fn sparse_helper_payload_mismatch_detail_reports_checksums_and_first_diff() {
    let detail = payload_mismatch_detail(b"abcdef", b"abcxef");

    assert!(detail.contains("expected_sha256="), "{detail}");
    assert!(detail.contains("actual_sha256="), "{detail}");
    assert!(detail.contains("first_diff=3"), "{detail}");
    assert!(detail.contains("expected_byte=Some(100)"), "{detail}");
    assert!(detail.contains("actual_byte=Some(120)"), "{detail}");
}

#[test]
fn sparse_observation_uses_range_probes_instead_of_full_file_reads() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let mount = temp.path().join("mount");
    std::fs::create_dir_all(mount.join("sparse")).unwrap();
    let sparse_path = mount.join("sparse/shared.bin");
    let payload = b"range payload";
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&sparse_path)
        .unwrap();
    file.set_len(4096).unwrap();
    file.seek(SeekFrom::Start(1024)).unwrap();
    file.write_all(payload).unwrap();
    drop(file);

    let clients = vec![client_spec_for(&sandbox, "projectfs", "client-a", mount)];
    let context = ScenarioContext {
        run_id: "run-sparse-range-probes".to_string(),
        seed: 377,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let ranges = [SparseRangeSpec {
        label: "payload",
        offset: 1024,
        payload: payload.to_vec(),
    }];
    let holes = [SparseHoleSpec {
        offset: 2048,
        len: 128,
    }];

    let observation = observe_sparse_ranges(
        &context,
        &clients,
        "sparse/shared.bin",
        4096,
        &ranges,
        &holes,
    )
    .unwrap();

    assert!(observation.missing.is_empty(), "{:?}", observation.missing);
    let progress = std::fs::read_dir(context.scenario_dir.join("hotspot-helper-progress"))
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let body = std::fs::read_to_string(&path).unwrap();
            format!("{}\n{}", name, body)
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        progress.contains("read-range-exact") && progress.contains("read-range-zero"),
        "{progress}"
    );
    assert!(
        !progress.contains("read-all-exact"),
        "sparse validation probes must not use full-file reads: {progress}"
    );
}

#[test]
fn hotspot_helper_ops_cover_metadata_attribute_and_symlink_ops() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let mount = temp.path().join("mount");
    std::fs::create_dir_all(&mount).unwrap();
    let progress = temp.path().join("hotspot-progress.jsonl");
    let payload_path = temp.path().join("payload.txt");
    std::fs::write(&payload_path, b"target body").unwrap();

    run_hotspot_op_helper(
        "create",
        mount.clone(),
        "target.txt",
        None,
        Some(&payload_path),
        Some(&progress),
    )
    .unwrap();
    run_hotspot_op_helper(
        "set-readonly",
        mount.clone(),
        "target.txt",
        None,
        None,
        Some(&progress),
    )
    .unwrap();

    assert!(
        std::fs::metadata(mount.join("target.txt"))
            .unwrap()
            .permissions()
            .readonly()
    );

    #[cfg(unix)]
    {
        run_hotspot_op_helper(
            "symlink-create",
            mount.clone(),
            "link.txt",
            Some("target.txt"),
            None,
            Some(&progress),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_link(mount.join("link.txt")).unwrap(),
            PathBuf::from("target.txt")
        );
    }

    let progress = std::fs::read_to_string(progress).unwrap();
    assert!(progress.contains("\"operation\":\"set-readonly\""));
    #[cfg(unix)]
    assert!(progress.contains("\"operation\":\"symlink-create\""));
}

#[test]
fn symlink_helper_skip_accepts_windows_error_gen_failure() {
    let error = ScenarioError::Io(io::Error::other(
        "hotspot helper failed for client-a symlink create: status=exit code: 1; stderr=A device attached to the system is not functioning. (os error 31)",
    ));

    assert!(
        hotspot_helper_error_is_supported_symlink_skip(&error),
        "WinFSP may report unsupported symlink creation as ERROR_GEN_FAILURE"
    );
}

#[test]
fn readonly_helper_skip_does_not_accept_windows_error_gen_failure() {
    let error = ScenarioError::Io(io::Error::other(
        "hotspot helper failed for client-c readonly attribute set: status=exit code: 1; stderr=A device attached to the system is not functioning. (os error 31)",
    ));

    assert!(
        !hotspot_helper_error_is_supported_readonly_skip(&error),
        "ERROR_GEN_FAILURE is only a supported symlink-create skip, not a generic metadata-attribute skip"
    );
}

#[test]
fn sparse_wait_timeout_writes_diagnostics_artifacts() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE);
    std::fs::create_dir_all(&scenario_dir).unwrap();
    let clients = ["client-a", "client-b"]
        .into_iter()
        .map(|name| {
            let mount = temp.path().join("mounts").join(name);
            std::fs::create_dir_all(&mount).unwrap();
            client_spec_for(&sandbox, "projectfs", name, mount)
        })
        .collect::<Vec<_>>();
    let context = ScenarioContext {
        run_id: "run-sparse-diagnostics".to_string(),
        seed: 377,
        sandbox,
        scenario_dir,
        client_controller_config: None,
    };
    let expected_ranges = [SparseRangeSpec {
        label: "prefix",
        offset: 0,
        payload: b"expected".to_vec(),
    }];
    let hole_specs: [SparseHoleSpec; 0] = [];

    let error = wait_for_sparse_expected_until(SparseExpectedWait {
        context: &context,
        clients: &clients,
        sparse_path: "sparse/shared.bin",
        logical_size: 64,
        expected_ranges: &expected_ranges,
        hole_specs: &hole_specs,
        deadline: Instant::now(),
        poll_interval: Duration::from_millis(1),
        label: "initial sparse ranges",
    })
    .unwrap_err();

    assert!(
            error.to_string().contains(
                "diagnostics: artifacts/scenarios/mounted-sparse-file-range-convergence/sparse-initial-sparse-ranges-timeout-diagnostics.json"
            ),
            "{error}"
        );
    let diagnostics_path = context
        .scenario_dir
        .join("sparse-initial-sparse-ranges-timeout-diagnostics.json");
    let diagnostics: serde_json::Value =
        serde_json::from_slice(&std::fs::read(diagnostics_path).unwrap()).unwrap();
    assert_eq!(diagnostics["schema_version"], 1);
    assert_eq!(diagnostics["run_id"], "run-sparse-diagnostics");
    assert_eq!(diagnostics["label"], "initial sparse ranges");
    assert_eq!(diagnostics["clients"].as_array().unwrap().len(), 2);
    assert_eq!(diagnostics["clients"][0]["parent_visible"], false);
    assert_eq!(
        diagnostics["clients"][0]["metadata"]["error"]["kind"],
        "Other"
    );
    assert!(
        diagnostics["clients"][0]["metadata"]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("read-range-exact")
    );

    let operations_log =
        std::fs::read_to_string(context.scenario_dir.join("operations.ndjson")).unwrap();
    assert!(operations_log.contains("sparse_timeout_diagnostics"));
    assert!(operations_log.contains("sparse-initial-sparse-ranges-timeout-diagnostics.json"));
}

#[test]
fn mounted_smoke_writes_file_converges_three_mounts_and_unmounts() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let sandbox = temp.path().join("run-1");
    let scenario_dir = sandbox
        .join("artifacts")
        .join("scenarios")
        .join(BASIC_MULTI_CLIENT_SMOKE);
    let clients = SMOKE_CLIENT_NAMES
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

    let summary = run_mounted_smoke_with_driver(
        &context,
        &mut driver,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .unwrap();

    assert_eq!(summary.peer_bootstrap.volumes.len(), 3);
    assert_eq!(summary.volume_validations.len(), 3);
    assert_eq!(
        summary.volume_validations[0].convergence.compared_clients,
        SMOKE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>()
    );
    assert!(
        summary
            .volume_validations
            .iter()
            .all(|validation| validation.convergence.converged)
    );
    assert!(driver.mounted.get());
    assert!(driver.unmounted.get());

    for client in driver.clients {
        for validation in &summary.volume_validations {
            let file_name = PathBuf::from(&validation.written_file)
                .file_name()
                .unwrap()
                .to_owned();
            let payload = std::fs::read(client.mount_point.join(file_name)).unwrap();
            assert_eq!(payload, b"studiofs e2e mounted multi-client smoke\n");
        }
    }
    assert!(context.scenario_dir.join("smoke-summary.json").exists());
}

#[test]
fn mounted_smoke_validation_reads_legacy_summary_shape() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let summary_path = temp.path().join("smoke-summary.json");
    let legacy = LegacyMountedSmokeRunSummary {
        bootstrap: VolumeBootstrapSummary {
            volume_name: "projectfs".to_string(),
            init_client: "client-a".to_string(),
            discovered_clients: vec!["client-b".to_string(), "client-c".to_string()],
            clients: Vec::new(),
        },
        written_file: "Z:\\studiofs-e2e-smoke.txt".to_string(),
        convergence: TreeComparison {
            converged: true,
            reference_client: Some("client-a".to_string()),
            compared_clients: vec!["client-a".to_string()],
            differences: Vec::new(),
        },
        attempts: 2,
        elapsed_ms: 42,
    };
    std::fs::write(&summary_path, serde_json::to_vec(&legacy).unwrap()).unwrap();

    let validations = read_mounted_smoke_volume_validations(&summary_path).unwrap();

    assert_eq!(validations.len(), 1);
    assert_eq!(validations[0].volume_name, "projectfs");
    assert_eq!(validations[0].written_file, "Z:\\studiofs-e2e-smoke.txt");
    assert_eq!(validations[0].attempts, 2);
    assert!(validations[0].convergence.converged);
}

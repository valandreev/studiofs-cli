//! Integration tests for the `studiofs_e2e` client controller.
//!
//! These tests exercise `MultiClientController` orchestration (bootstrap, discovery, mount/unmount,
//! restart, and force-kill flows) using in-process test doubles.
//! `RecordingExecutor` and `RecordingProcess` capture command construction and lifecycle behavior
//! without spawning real `studiofs` processes.

use std::cell::{Cell, RefCell};
use std::io;
use std::path::Path;
use std::rc::Rc;
use std::time::Duration;

use studiofs_e2e::client::{
    ClientCommand, ClientCommandOutput, ClientControllerConfig, ClientControllerError,
    ClientLifecycleState, ClientProcess, ClientProcessExecutor, MultiClientController,
    PeerVolumeBootstrapOptions, S3Config, VolumeBootstrapOptions,
};
use studiofs_e2e::infra::ServicePort;
use tempfile::tempdir;

#[test]
fn controller_bootstraps_mounts_restarts_and_kills_isolated_clients() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let calls = Rc::clone(&executor.calls);
    let killed = Rc::clone(&executor.killed);
    let config = test_config(temp.path());
    let mut controller = MultiClientController::new(config, executor);

    controller
        .create_clients(["client-a", "client-b", "client-c"])
        .unwrap();
    controller.init_first_client().unwrap();
    controller.discover_additional_clients().unwrap();
    controller.mount_all().unwrap();
    controller.restart("client-b").unwrap();
    controller.force_kill_all().unwrap();

    let clients = controller.clients();
    assert_eq!(clients.len(), 3);
    assert!(clients.iter().all(|client| client.client_root.exists()));
    assert_ne!(clients[0].client_root, clients[1].client_root);
    assert_ne!(clients[1].cache_dir, clients[2].cache_dir);
    assert_eq!(
        clients[0].local_metadata_db,
        clients[0]
            .client_root
            .join("meta")
            .join("projectfs")
            .join("projectfs.db")
    );
    assert_eq!(clients[0].volume_name, "projectfs");
    assert!(clients[0].stdout_log.ends_with("studiofs.stdout.log"));
    assert!(clients[0].stderr_log.ends_with("studiofs.stderr.log"));
    assert!(clients[0].tracing_log.ends_with("studiofs-tracing.log"));
    assert!(clients[0].mount_metadata_dir.ends_with("mount"));
    assert!(
        clients
            .iter()
            .all(|client| client.state == ClientLifecycleState::Killed)
    );

    let calls = calls.borrow();
    assert_eq!(calls[0].args[0], "init");
    assert_eq!(calls[1].args[0], "discover");
    assert_eq!(calls[2].args[0], "discover");
    assert_eq!(
        calls.iter().filter(|call| call.action == "mount").count(),
        4
    );
    assert!(calls.iter().all(|call| {
        call.env
            .iter()
            .any(|(key, _)| key == "STUDIOFS_CLIENT_ROOT")
    }));
    assert!(calls.iter().all(|call| {
        call.env
            .iter()
            .any(|(key, _)| key == "STUDIOFS_DAEMON_LOG_DIR")
    }));
    assert!(calls.iter().all(|call| {
        call.env.iter().any(|(key, value)| {
            key == "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS" && value == "250"
        })
    }));
    assert!(
        calls
            .iter()
            .all(|call| call.stdout_log.ends_with("studiofs.stdout.log"))
    );
    assert!(
        calls
            .iter()
            .all(|call| call.stderr_log.ends_with("studiofs.stderr.log"))
    );
    assert_eq!(killed.get(), 3);
}

#[test]
fn controller_can_discover_and_mount_late_observer_after_warm_clients() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let calls = Rc::clone(&executor.calls);
    let config = test_config(temp.path());
    let mut controller = MultiClientController::new(config, executor);

    controller
        .create_clients(["client-a", "client-b", "client-d"])
        .unwrap();
    controller.init_first_client().unwrap();
    controller.discover("client-b").unwrap();
    controller.mount("client-a").unwrap();
    controller.mount("client-b").unwrap();
    controller.discover("client-d").unwrap();
    controller.mount("client-d").unwrap();

    let actions = calls
        .borrow()
        .iter()
        .map(|command| format!("{}:{}", command.client, command.action))
        .collect::<Vec<_>>();
    assert_eq!(
        actions,
        [
            "client-a:init",
            "client-b:discover",
            "client-a:mount",
            "client-b:mount",
            "client-d:discover",
            "client-d:mount",
        ]
    );
}

#[test]
fn mount_env_overrides_are_scoped_to_the_named_client() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let calls = Rc::clone(&executor.calls);
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);

    controller.create_clients(["client-a", "client-b"]).unwrap();
    controller.init_first_client().unwrap();
    controller.discover_additional_clients().unwrap();
    controller
        .set_client_env_overrides(
            "client-b",
            [
                (
                    "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS".to_string(),
                    "1500".to_string(),
                ),
                (
                    "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL".to_string(),
                    "1".to_string(),
                ),
            ],
        )
        .unwrap();
    controller.mount_all().unwrap();

    let calls = calls.borrow();
    let mount_a = calls
        .iter()
        .find(|call| call.action == "mount" && call.client == "client-a")
        .unwrap();
    let mount_b = calls
        .iter()
        .find(|call| call.action == "mount" && call.client == "client-b")
        .unwrap();

    assert!(mount_a.env.iter().any(|(key, value)| {
        key == "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS" && value == "250"
    }));
    assert!(
        !mount_a
            .env
            .iter()
            .any(|(key, _)| key == "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL")
    );
    assert!(mount_b.env.iter().any(|(key, value)| {
        key == "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS" && value == "1500"
    }));
    assert!(mount_b.env.iter().any(|(key, value)| {
        key == "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL" && value == "1"
    }));
}

#[test]
fn mount_arg_overrides_are_scoped_to_the_named_client() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let calls = Rc::clone(&executor.calls);
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);

    controller.create_clients(["client-a", "client-b"]).unwrap();
    controller.init_first_client().unwrap();
    controller.discover_additional_clients().unwrap();
    controller
        .set_client_mount_arg_overrides("client-b", ["--read-only"])
        .unwrap();
    controller.mount_all().unwrap();

    let calls = calls.borrow();
    let mount_a = calls
        .iter()
        .find(|call| call.action == "mount" && call.client == "client-a")
        .unwrap();
    let mount_b = calls
        .iter()
        .find(|call| call.action == "mount" && call.client == "client-b")
        .unwrap();

    assert!(!mount_a.args.iter().any(|arg| arg == "--read-only"));
    assert!(mount_b.args.iter().any(|arg| arg == "--read-only"));
}

#[test]
fn mount_metadata_records_process_id_after_successful_mount() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);

    controller.create_clients(["client-a"]).unwrap();
    controller.mount_all().unwrap();

    let clients = controller.clients();
    assert_eq!(clients[0].process_id, Some(100));
    let mount_metadata: serde_json::Value = serde_json::from_slice(
        &std::fs::read(clients[0].mount_metadata_dir.join("mount-point.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(mount_metadata["process_id"], 100);
}

#[test]
fn bootstrap_workflow_initializes_one_volume_and_discovers_isolated_clients() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        create_local_metadata_db: true,
        fail_if_local_metadata_dir_preexists: true,
        ..RecordingExecutor::new()
    };
    let calls = Rc::clone(&executor.calls);
    let config = test_config(temp.path());
    let mut controller = MultiClientController::new(config, executor);

    let summary = controller
        .bootstrap_volume(VolumeBootstrapOptions {
            client_names: vec![
                "client-a".to_string(),
                "client-b".to_string(),
                "client-c".to_string(),
            ],
            clean_existing_local_metadata: false,
        })
        .unwrap();

    assert_eq!(summary.volume_name, "projectfs");
    assert_eq!(summary.init_client, "client-a");
    assert_eq!(summary.discovered_clients, ["client-b", "client-c"]);
    assert_eq!(summary.clients.len(), 3);
    assert!(summary.clients.iter().all(|client| {
        client.local_metadata_db.exists()
            && client.local_metadata_db.ends_with("projectfs/projectfs.db")
    }));
    assert_ne!(
        summary.clients[0].local_metadata_db,
        summary.clients[1].local_metadata_db
    );
    assert_ne!(
        summary.clients[1].local_metadata_db,
        summary.clients[2].local_metadata_db
    );

    let calls = calls.borrow();
    assert_eq!(calls[0].action, "init");
    assert_eq!(calls[1].action, "discover");
    assert_eq!(calls[2].action, "discover");
    assert_eq!(calls[0].args[0], "init");
    assert_eq!(calls[1].args[0], "discover");
    assert_eq!(calls[2].args[0], "discover");
}

#[test]
fn peer_bootstrap_initializes_each_client_volume_and_cross_discovers_neighbors() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        create_local_metadata_db: true,
        fail_if_local_metadata_dir_preexists: true,
        ..RecordingExecutor::new()
    };
    let calls = Rc::clone(&executor.calls);
    let config = test_config(temp.path());
    let mut controller = MultiClientController::new(config, executor);

    let summary = controller
        .bootstrap_peer_volumes(PeerVolumeBootstrapOptions {
            client_names: vec![
                "client-a".to_string(),
                "client-b".to_string(),
                "client-c".to_string(),
            ],
            clean_existing_local_metadata: false,
        })
        .unwrap();

    assert_eq!(summary.volumes.len(), 3);
    assert_eq!(summary.volumes[0].init_client, "client-a");
    assert_eq!(summary.volumes[1].init_client, "client-b");
    assert_eq!(summary.volumes[2].init_client, "client-c");
    assert!(summary.volumes.iter().all(|volume| {
        volume.discovered_clients.len() == 2
            && !volume.discovered_clients.contains(&volume.init_client)
            && volume.clients.iter().all(|client| {
                client.local_metadata_db.exists()
                    && client
                        .local_metadata_db
                        .ends_with(format!("{}/{}.db", volume.volume_name, volume.volume_name))
            })
    }));

    let calls = calls.borrow();
    let init_calls = calls
        .iter()
        .filter(|call| call.action == "init")
        .collect::<Vec<_>>();
    let discover_calls = calls
        .iter()
        .filter(|call| call.action == "discover")
        .collect::<Vec<_>>();
    assert_eq!(init_calls.len(), 3);
    assert_eq!(discover_calls.len(), 6);
    for init in &init_calls {
        assert_eq!(init.args[0], "init");
        assert!(init.args[1].starts_with("projectfs-"));
        assert!(init.args[1].contains(&init.client));
    }
    for discover in &discover_calls {
        assert_eq!(discover.args[0], "discover");
        assert_ne!(
            discover.args[1],
            init_calls
                .iter()
                .find(|init| init.client == discover.client)
                .unwrap()
                .args[1]
        );
    }
}

#[test]
fn controller_can_select_peer_volume_for_sequential_mount_validation() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        create_local_metadata_db: true,
        fail_if_local_metadata_dir_preexists: true,
        ..RecordingExecutor::new()
    };
    let calls = Rc::clone(&executor.calls);
    let config = test_config(temp.path());
    let mut controller = MultiClientController::new(config, executor);

    let summary = controller
        .bootstrap_peer_volumes(PeerVolumeBootstrapOptions {
            client_names: vec![
                "client-a".to_string(),
                "client-b".to_string(),
                "client-c".to_string(),
            ],
            clean_existing_local_metadata: false,
        })
        .unwrap();
    let volume_name = summary.volumes[1].volume_name.clone();

    controller.select_volume_for_clients(&volume_name).unwrap();
    controller.mount_all().unwrap();

    let clients = controller.clients();
    assert!(clients.iter().all(|client| {
        client.volume_name == volume_name
            && client
                .local_metadata_db
                .ends_with(format!("{volume_name}/{volume_name}.db"))
    }));
    let calls = calls.borrow();
    let mount_calls = calls
        .iter()
        .filter(|call| call.action == "mount")
        .collect::<Vec<_>>();
    assert_eq!(mount_calls.len(), 3);
    assert!(mount_calls.iter().all(|call| call.args[1] == volume_name));
}

#[test]
fn bootstrap_workflow_can_clean_stale_local_metadata_before_cli_commands() {
    let temp = tempdir().unwrap();
    let stale_db = temp
        .path()
        .join("run-1")
        .join("artifacts")
        .join("clients")
        .join("client-a")
        .join("client-root")
        .join("meta")
        .join("projectfs")
        .join("projectfs.db");
    std::fs::create_dir_all(stale_db.parent().unwrap()).unwrap();
    std::fs::write(&stale_db, "stale").unwrap();
    let executor = RecordingExecutor {
        create_local_metadata_db: true,
        fail_if_local_metadata_dir_preexists: true,
        ..RecordingExecutor::new()
    };
    let config = test_config(temp.path());
    let mut controller = MultiClientController::new(config, executor);

    let summary = controller
        .bootstrap_volume(VolumeBootstrapOptions {
            client_names: vec!["client-a".to_string()],
            clean_existing_local_metadata: true,
        })
        .unwrap();

    assert_eq!(summary.clients.len(), 1);
    assert_eq!(
        std::fs::read_to_string(&summary.clients[0].local_metadata_db).unwrap(),
        "init"
    );
}

#[test]
fn bootstrap_workflow_reports_local_metadata_cleanup_failures_with_client_path() {
    let temp = tempdir().unwrap();
    let metadata_path = temp
        .path()
        .join("run-1")
        .join("artifacts")
        .join("clients")
        .join("client-a")
        .join("client-root")
        .join("meta")
        .join("projectfs");
    std::fs::create_dir_all(metadata_path.parent().unwrap()).unwrap();
    std::fs::write(&metadata_path, "not a directory").unwrap();
    let mut controller =
        MultiClientController::new(test_config(temp.path()), RecordingExecutor::new());

    let err = controller
        .bootstrap_volume(VolumeBootstrapOptions {
            client_names: vec!["client-a".to_string()],
            clean_existing_local_metadata: true,
        })
        .unwrap_err();

    let message = err.to_string();
    assert!(message.contains("client-a"));
    assert!(message.contains(&metadata_path.display().to_string()));
}

#[test]
fn compose_service_config_derives_unique_volume_and_docker_endpoints() {
    let temp = tempdir().unwrap();
    let config = ClientControllerConfig::from_compose_services(
        Path::new("studiofs-test-bin").to_path_buf(),
        temp.path().join("run-1"),
        "Run_1.20260502",
        321,
        &[
            service_port("postgres", 35432),
            service_port("rustfs", 39000),
            service_port("rustfs-console", 39001),
        ],
    )
    .unwrap();

    assert_eq!(config.volume_name, "sfs-e2e-run-1-20260502-321");
    assert_eq!(
        config.meta_dsn,
        "postgres://studiofs:studiofs@127.0.0.1:35432"
    );
    assert_eq!(config.s3.endpoint, "http://127.0.0.1:39000");
    assert_eq!(config.s3.bucket, "studiofs-e2e-run-1-20260502");
    assert_eq!(config.s3.access_key, "rustfsadmin");
    assert_eq!(config.s3.secret_key, "rustfsadmin");
}

#[test]
fn compose_service_config_can_scope_volume_to_scenario() {
    let temp = tempdir().unwrap();
    let config = ClientControllerConfig::from_compose_services_for_scenario(
        Path::new("studiofs-test-bin").to_path_buf(),
        temp.path().join("run-1"),
        "Run_1.20260502",
        321,
        "rustfs-outage-multi-client-recovery",
        &[
            service_port("postgres", 35432),
            service_port("rustfs", 39000),
        ],
    )
    .unwrap();

    assert_eq!(
        config.volume_name,
        "sfs-e2e-run-1-20260502-321-rustfs-outage-multi-client-recovery"
    );
    assert_eq!(config.s3.bucket, "studiofs-e2e-run-1-20260502");
}

#[test]
fn compose_service_config_keeps_bucket_name_within_s3_length_limit() {
    let temp = tempdir().unwrap();
    let config = ClientControllerConfig::from_compose_services(
        Path::new("studiofs-test-bin").to_path_buf(),
        temp.path().join("run-1"),
        "Run_1.20260502.with.an.extremely.long.unique.identifier.for.parallel-ci-workers",
        321,
        &[
            service_port("postgres", 35432),
            service_port("rustfs", 39000),
        ],
    )
    .unwrap();

    assert!(config.s3.bucket.len() <= 63, "{}", config.s3.bucket);
    assert!(
        config
            .s3
            .bucket
            .starts_with(|ch: char| ch.is_ascii_alphanumeric())
    );
    assert!(
        config
            .s3
            .bucket
            .ends_with(|ch: char| ch.is_ascii_alphanumeric())
    );
    assert!(config.volume_name.len() <= 63, "{}", config.volume_name);
}

#[test]
fn create_clients_rejects_path_traversal_client_names() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);

    for name in [
        "../escape",
        "nested/client",
        r"nested\client",
        ".hidden",
        "",
    ] {
        let err = controller.create_clients([name]).unwrap_err();
        assert!(matches!(err, ClientControllerError::UnsafeClientName(_)));
    }

    assert!(!temp.path().join("escape").exists());
}

#[test]
fn configured_mount_points_override_client_artifact_metadata_dirs() {
    let temp = tempdir().unwrap();
    let mut config = test_config(temp.path());
    let mount_a = temp.path().join("mount-a");
    let mount_b = temp.path().join("mount-b");
    let expected_mount_a = mount_a.join("projectfs");
    config.mount_points = vec![mount_a.clone(), mount_b];
    let executor = RecordingExecutor::new();
    let mut controller = MultiClientController::new(config, executor);

    controller.create_clients(["client-a", "client-b"]).unwrap();

    let clients = controller.clients();
    assert_eq!(clients[0].mount_point, expected_mount_a);
    assert_ne!(clients[0].mount_point, clients[0].mount_metadata_dir);
    assert!(clients[0].mount_metadata_dir.exists());
    let mount_metadata: serde_json::Value = serde_json::from_slice(
        &std::fs::read(clients[0].mount_metadata_dir.join("mount-point.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        mount_metadata["mount_point"],
        expected_mount_a.display().to_string()
    );
}

#[cfg(windows)]
#[test]
fn default_windows_mount_points_are_drive_roots_not_metadata_dirs() {
    let temp = tempdir().unwrap();
    let mut controller =
        MultiClientController::new(test_config(temp.path()), RecordingExecutor::new());

    controller.create_clients(["client-a", "client-b"]).unwrap();

    let clients = controller.clients();
    assert_eq!(clients.len(), 2);
    assert!(clients.iter().all(|client| client.mount_point.has_root()));
    assert!(clients.iter().all(|client| {
        client
            .mount_point
            .as_os_str()
            .to_string_lossy()
            .ends_with(":\\")
    }));
    assert_ne!(clients[0].mount_point, clients[1].mount_point);
    assert!(clients[0].mount_point.join("smoke.txt").has_root());
    assert!(
        clients
            .iter()
            .all(|client| client.mount_point != client.mount_metadata_dir)
    );
}

#[test]
fn mount_rejects_already_active_mount_point_before_spawning() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        mount_ready_before_spawn: true,
        ..RecordingExecutor::new()
    };
    let calls = Rc::clone(&executor.calls);
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller.create_clients(["client-a"]).unwrap();
    controller.init_first_client().unwrap();

    let err = controller.mount_all().unwrap_err();

    assert!(matches!(
        err,
        ClientControllerError::MountPointAlreadyActive { client, .. } if client == "client-a"
    ));
    assert!(!calls.borrow().iter().any(|call| call.action == "mount"));
}

#[test]
fn system_readiness_does_not_accept_precreated_mount_directory() {
    let temp = tempdir().unwrap();
    let executor = studiofs_e2e::client::SystemClientProcessExecutor;
    let command = client_command_for_mount_point(temp.path().join("mount"));
    std::fs::create_dir_all(&command.mount_point).unwrap();

    assert!(!executor.is_mount_ready(&command).unwrap());
}

#[test]
fn client_index_out_of_range_is_distinct_from_no_clients() {
    let temp = tempdir().unwrap();
    let mut controller =
        MultiClientController::new(test_config(temp.path()), RecordingExecutor::new());
    controller.create_clients(["client-a"]).unwrap();

    let err = controller.client_at(9).unwrap_err();

    assert!(matches!(
        err,
        ClientControllerError::ClientIndexOutOfRange { index: 9, len: 1 }
    ));
}

#[test]
fn create_clients_refuses_to_drop_running_process_handles() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let killed = Rc::clone(&executor.killed);
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller.create_clients(["client-a"]).unwrap();
    controller.init_first_client().unwrap();
    controller.mount_all().unwrap();

    let err = controller.create_clients(["client-b"]).unwrap_err();

    assert!(matches!(err, ClientControllerError::ClientsStillMounted));
    assert_eq!(killed.get(), 0);
    assert_eq!(controller.clients()[0].name, "client-a");
}

#[test]
fn unmount_waits_for_mount_process_to_exit() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let process_wait_calls = Rc::clone(&executor.process_wait_calls);
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller.create_clients(["client-a"]).unwrap();
    controller.init_first_client().unwrap();
    controller.mount_all().unwrap();

    controller.unmount_all().unwrap();

    assert!(process_wait_calls.get() >= 2);
    assert_eq!(
        controller.clients()[0].state,
        ClientLifecycleState::Unmounted
    );
}

#[test]
fn force_kill_then_remount_reuses_same_client_root_without_graceful_umount() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let calls = Rc::clone(&executor.calls);
    let killed = Rc::clone(&executor.killed);
    let stale_remount_cleanups = Rc::clone(&executor.stale_remount_cleanups);
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller.create_clients(["client-a", "client-b"]).unwrap();
    controller.init_first_client().unwrap();
    controller.discover_additional_clients().unwrap();
    controller.mount_all().unwrap();
    let original_root = controller.clients()[0].client_root.clone();

    controller.force_kill("client-a").unwrap();
    assert_eq!(controller.clients()[0].state, ClientLifecycleState::Killed);
    controller.remount("client-a").unwrap();

    let clients = controller.clients();
    assert_eq!(clients[0].client_root, original_root);
    assert_eq!(clients[0].state, ClientLifecycleState::Mounted);
    assert_eq!(killed.get(), 1);
    assert_eq!(stale_remount_cleanups.get(), 1);
    let calls = calls.borrow();
    let mount_calls = calls
        .iter()
        .filter(|call| call.client == "client-a" && call.action == "mount")
        .collect::<Vec<_>>();
    assert_eq!(mount_calls.len(), 2);
    assert!(
        !calls
            .iter()
            .any(|call| call.client == "client-a" && call.action == "umount"),
        "abrupt crash recovery must not model a clean unmount"
    );
}

#[test]
fn force_kill_all_attempts_every_client_and_clears_state_after_kill_error() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        kill_error_after: Cell::new(Some(1)),
        ..RecordingExecutor::new()
    };
    let killed = Rc::clone(&executor.killed);
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller
        .create_clients(["client-a", "client-b", "client-c"])
        .unwrap();
    controller.init_first_client().unwrap();
    controller.discover_additional_clients().unwrap();
    controller.mount_all().unwrap();

    let err = controller.force_kill_all().unwrap_err();

    assert!(err.to_string().contains("injected kill failure"));
    assert_eq!(killed.get(), 3);
    assert!(controller.clients().iter().all(|client| {
        client.state == ClientLifecycleState::Killed && client.process_id.is_none()
    }));
}

#[test]
fn force_kill_all_waits_for_mount_processes_to_exit_after_termination() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor::new();
    let wait_calls = Rc::clone(&executor.process_wait_calls);
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller.create_clients(["client-a", "client-b"]).unwrap();
    controller.init_first_client().unwrap();
    controller.discover_additional_clients().unwrap();
    controller.mount_all().unwrap();
    wait_calls.set(0);

    controller.force_kill_all().unwrap();

    assert!(
        wait_calls.get() >= 2,
        "force-kill cleanup must wait for every terminated mount process to exit"
    );
}

#[test]
fn unmount_all_force_kills_mount_process_that_survives_graceful_umount() {
    let temp = tempdir().unwrap();
    let mut config = test_config(temp.path());
    config.mount_exit_timeout = Duration::from_millis(20);
    config.poll_interval = Duration::from_millis(1);
    let executor = RecordingExecutor {
        exit_requires_kill: true,
        ..RecordingExecutor::new()
    };
    let killed = Rc::clone(&executor.killed);
    let mut controller = MultiClientController::new(config, executor);
    controller.create_clients(["client-a"]).unwrap();
    controller.init_first_client().unwrap();
    controller.mount_all().unwrap();

    let err = controller.unmount_all().unwrap_err();

    assert!(
        err.to_string()
            .contains("mount process did not exit after graceful unmount")
    );
    assert_eq!(
        killed.get(),
        1,
        "graceful unmount timeout must force-kill the lingering mount process"
    );
    assert_eq!(controller.clients()[0].state, ClientLifecycleState::Killed);
    assert_eq!(controller.clients()[0].process_id, None);
}

#[test]
fn mount_reports_process_exit_while_waiting_for_readiness() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        mount_ready: Cell::new(false),
        process_exit_status: Cell::new(42),
        ..RecordingExecutor::new()
    };
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller.create_clients(["client-a"]).unwrap();
    controller.init_first_client().unwrap();

    let err = controller.mount_all().unwrap_err();

    assert!(matches!(
        err,
        ClientControllerError::CommandFailed {
            action,
            status: 42,
            ..
        } if action == "mount"
    ));
    assert_eq!(controller.clients()[0].state, ClientLifecycleState::Exited);
}

#[test]
fn command_failure_includes_stdout_and_stderr() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        run_status: Cell::new(17),
        run_stdout: "stdout diagnostics".to_string(),
        run_stderr: "stderr diagnostics".to_string(),
        ..RecordingExecutor::new()
    };
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller.create_clients(["client-a"]).unwrap();

    let err = controller.init_first_client().unwrap_err();
    let message = err.to_string();

    assert!(message.contains("stdout diagnostics"));
    assert!(message.contains("stderr diagnostics"));
}

#[test]
fn command_failure_truncates_large_stdout_and_stderr() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        run_status: Cell::new(17),
        run_stdout: "o".repeat(10_000),
        run_stderr: "e".repeat(10_000),
        ..RecordingExecutor::new()
    };
    let mut controller = MultiClientController::new(test_config(temp.path()), executor);
    controller.create_clients(["client-a"]).unwrap();

    let message = controller.init_first_client().unwrap_err().to_string();

    assert!(message.len() < 9_000);
    assert!(message.contains("truncated"));
}

#[test]
fn unmount_all_continues_after_stuck_client_and_clears_failed_handle_state() {
    let temp = tempdir().unwrap();
    let executor = RecordingExecutor {
        exit_requires_kill: true,
        kill_error_after: Cell::new(Some(1)),
        ..RecordingExecutor::new()
    };
    let killed = Rc::clone(&executor.killed);
    let mut config = test_config(temp.path());
    config.mount_exit_timeout = Duration::from_millis(1);
    config.poll_interval = Duration::from_millis(1);
    let mut controller = MultiClientController::new(config, executor);

    controller.create_clients(["client-a", "client-b"]).unwrap();
    controller.mount_all().unwrap();

    let err = controller.unmount_all().unwrap_err();
    let clients = controller.clients();

    assert!(err.to_string().contains("graceful unmount"));
    assert_eq!(killed.get(), 2);
    assert_eq!(clients[0].state, ClientLifecycleState::Killed);
    assert_eq!(clients[0].process_id, None);
    assert_eq!(clients[1].state, ClientLifecycleState::Killed);
    assert_eq!(clients[1].process_id, None);
}

#[derive(Clone)]
/// Test double for [`ClientProcessExecutor`] that records all commands and can inject failures.
///
/// Used to validate controller command construction (args, env, derived paths) and to simulate
/// readiness/progress/exit behavior deterministically.
struct RecordingExecutor {
    calls: Rc<RefCell<Vec<ClientCommand>>>,
    killed: Rc<Cell<usize>>,
    stale_remount_cleanups: Rc<Cell<usize>>,
    readiness_calls: Rc<Cell<usize>>,
    process_wait_calls: Rc<Cell<usize>>,
    spawned_mount_point: Rc<RefCell<Option<std::path::PathBuf>>>,
    next_pid: Cell<u32>,
    mount_ready: Cell<bool>,
    mount_ready_before_spawn: bool,
    process_exit_status: Cell<i32>,
    run_status: Cell<i32>,
    run_stdout: String,
    run_stderr: String,
    create_local_metadata_db: bool,
    fail_if_local_metadata_dir_preexists: bool,
    kill_error_after: Cell<Option<usize>>,
    exit_requires_kill: bool,
}

impl RecordingExecutor {
    fn new() -> Self {
        Self {
            calls: Rc::new(RefCell::new(Vec::new())),
            killed: Rc::new(Cell::new(0)),
            stale_remount_cleanups: Rc::new(Cell::new(0)),
            readiness_calls: Rc::new(Cell::new(0)),
            process_wait_calls: Rc::new(Cell::new(0)),
            spawned_mount_point: Rc::new(RefCell::new(None)),
            next_pid: Cell::new(100),
            mount_ready: Cell::new(true),
            mount_ready_before_spawn: false,
            process_exit_status: Cell::new(0),
            run_status: Cell::new(0),
            run_stdout: String::new(),
            run_stderr: String::new(),
            create_local_metadata_db: false,
            fail_if_local_metadata_dir_preexists: false,
            kill_error_after: Cell::new(None),
            exit_requires_kill: false,
        }
    }
}

impl ClientProcessExecutor for RecordingExecutor {
    fn run(&self, command: &ClientCommand) -> io::Result<ClientCommandOutput> {
        let volume_name = command
            .args
            .get(1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing volume name"))?;
        let local_metadata_dir = command.client_root.join("meta").join(volume_name);
        if self.fail_if_local_metadata_dir_preexists && local_metadata_dir.exists() {
            return Err(io::Error::other(format!(
                "{} local metadata dir existed before command: {}",
                command.client,
                local_metadata_dir.display()
            )));
        }
        self.calls.borrow_mut().push(command.clone());
        if self.create_local_metadata_db {
            std::fs::create_dir_all(&local_metadata_dir)?;
            std::fs::write(
                local_metadata_dir.join(format!("{volume_name}.db")),
                &command.action,
            )?;
        }
        Ok(ClientCommandOutput {
            status: self.run_status.get(),
            stdout: self.run_stdout.clone(),
            stderr: self.run_stderr.clone(),
        })
    }

    fn spawn(&self, command: &ClientCommand) -> io::Result<Box<dyn ClientProcess>> {
        self.calls.borrow_mut().push(command.clone());
        self.spawned_mount_point
            .borrow_mut()
            .replace(command.mount_point.clone());
        let pid = self.next_pid.get();
        self.next_pid.set(pid + 1);
        Ok(Box::new(RecordingProcess {
            pid,
            killed: Rc::clone(&self.killed),
            kill_error_after: Cell::new(self.kill_error_after.get()),
            wait_calls: Rc::clone(&self.process_wait_calls),
            try_wait_calls: Cell::new(0),
            exit_status: self.process_exit_status.get(),
            exit_requires_kill: self.exit_requires_kill,
            was_killed: Cell::new(false),
        }))
    }

    fn is_mount_ready(&self, command: &ClientCommand) -> io::Result<bool> {
        self.readiness_calls.set(self.readiness_calls.get() + 1);
        Ok(self.mount_ready.get()
            && (self.mount_ready_before_spawn
                || self
                    .spawned_mount_point
                    .borrow()
                    .as_ref()
                    .is_some_and(|mount_point| mount_point == &command.mount_point)))
    }

    fn cleanup_stale_mount_before_remount(&self, _command: &ClientCommand) -> io::Result<()> {
        self.stale_remount_cleanups
            .set(self.stale_remount_cleanups.get() + 1);
        Ok(())
    }
}

/// Test double for [`ClientProcess`] returned by [`RecordingExecutor`].
///
/// Records kill/wait calls and can simulate a mount process that only exits after being killed.
struct RecordingProcess {
    pid: u32,
    killed: Rc<Cell<usize>>,
    kill_error_after: Cell<Option<usize>>,
    wait_calls: Rc<Cell<usize>>,
    try_wait_calls: Cell<usize>,
    exit_status: i32,
    exit_requires_kill: bool,
    was_killed: Cell<bool>,
}

impl ClientProcess for RecordingProcess {
    fn id(&self) -> u32 {
        self.pid
    }

    fn try_wait(&mut self) -> io::Result<Option<i32>> {
        self.wait_calls.set(self.wait_calls.get() + 1);
        let calls = self.try_wait_calls.get() + 1;
        self.try_wait_calls.set(calls);
        if self.exit_requires_kill && !self.was_killed.get() {
            return Ok(None);
        }
        Ok((calls > 1).then_some(self.exit_status))
    }

    fn kill(&mut self) -> io::Result<()> {
        let next = self.killed.get() + 1;
        self.killed.set(next);
        if self.kill_error_after.get() == Some(next) {
            return Err(io::Error::other("injected kill failure"));
        }
        self.was_killed.set(true);
        Ok(())
    }
}

/// Builds a minimal [`ClientControllerConfig`] rooted under the supplied sandbox directory.
///
/// Values are chosen for controller testing (short timeouts, loopback endpoints, stable defaults).
fn test_config(root: &Path) -> ClientControllerConfig {
    ClientControllerConfig {
        studiofs_bin: Path::new("studiofs-test-bin").to_path_buf(),
        sandbox: root.join("run-1"),
        volume_name: "projectfs".to_string(),
        meta_dsn: "postgres://studiofs:studiofs@127.0.0.1:25432".to_string(),
        s3: S3Config {
            endpoint: "http://127.0.0.1:29000".to_string(),
            bucket: "studiofs-e2e".to_string(),
            access_key: "studiofs".to_string(),
            secret_key: "studiofs123".to_string(),
            region: "us-east-1".to_string(),
        },
        mount_ready_timeout: Duration::from_millis(1),
        mount_exit_timeout: Duration::from_secs(15),
        poll_interval: Duration::from_millis(20),
        mount_points: Vec::new(),
        client_env_overrides: std::collections::BTreeMap::new(),
        mount_arg_overrides: std::collections::BTreeMap::new(),
    }
}

/// Builds a [`ServicePort`] descriptor matching the e2e compose harness conventions.
fn service_port(service: &str, host_port: u16) -> ServicePort {
    ServicePort {
        service: service.to_string(),
        host: "127.0.0.1".to_string(),
        host_port,
        container_port: 0,
        env_var: format!("TEST_{}", service.to_ascii_uppercase()),
    }
}

/// Constructs a baseline `mount` [`ClientCommand`] for controller validation tests.
fn client_command_for_mount_point(mount_point: std::path::PathBuf) -> ClientCommand {
    ClientCommand {
        client: "client-a".to_string(),
        action: "mount".to_string(),
        program: "studiofs-test-bin".to_string(),
        args: Vec::new(),
        env: Vec::new(),
        client_root: mount_point.join("client-root"),
        cache_dir: mount_point.join("cache"),
        mount_point,
        stdout_log: Path::new("stdout.log").to_path_buf(),
        stderr_log: Path::new("stderr.log").to_path_buf(),
        tracing_log: Path::new("tracing.log").to_path_buf(),
    }
}

//! Locally durable unpublished trash crash replay and fresh remote bootstrap.

use super::recoverable_trash::{checked_command, expect_payload, wait_absent};
use super::*;

pub(in crate::scenario) enum RecoverableTrashDurability {
    OfflineCrashReplay,
    S3OutageCrashReplay,
    ColdBootstrap,
}
const LIMIT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);
const BODY: &[u8] = b"offline durable retained file\0\xff\n";
const CHILD: &[u8] = b"offline durable retained directory child\0\n";

impl Scenario for RecoverableTrashDurability {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: match self {
                Self::OfflineCrashReplay => "mounted-recoverable-trash-durability",
                Self::S3OutageCrashReplay => "mounted-recoverable-trash-s3-crash-replay",
                Self::ColdBootstrap => "mounted-recoverable-trash-cold-bootstrap",
            },
            description: match self {
                Self::S3OutageCrashReplay => {
                    "unpublished native trash survives hard kill with S3 offline and PostgreSQL available for graph repair"
                }
                Self::OfflineCrashReplay => {
                    "unpublished native trash survives hard kill offline, publishes after recovery and bootstraps a fresh client"
                }
                Self::ColdBootstrap => {
                    "published native trash and restored file/directory bootstrap into entirely fresh metadata/cache with observable S3 reads"
                }
            },
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if !cfg!(target_os = "linux") {
            return Err(
                io::Error::new(io::ErrorKind::Unsupported, "Linux GIO/FUSE required").into(),
            );
        }
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;
        let mut cold_config = config.clone();
        cold_config.sandbox = context.scenario_dir.join("fresh-client-runtime");
        cold_config.mount_points.clear();
        let mut warm = MultiClientController::new(config.clone(), SystemClientProcessExecutor);
        let mut cold = MultiClientController::new(cold_config, SystemClientProcessExecutor);
        let compose = compose_control_from_manifest(context)?;
        let mut services_paused = false;
        let result = probe(
            context,
            &config,
            &mut warm,
            &mut cold,
            &compose,
            &mut services_paused,
            self,
        );
        let mut cleanup_errors = Vec::new();
        if services_paused {
            for service in [POSTGRES_SERVICE_NAME, RUSTFS_SERVICE_NAME] {
                if let Err(error) = compose.start_service(service, &SystemCommandExecutor) {
                    cleanup_errors.push(format!("restart {service}: {error}"));
                }
            }
        }
        for controller in [&mut cold, &mut warm] {
            let cleanup = if matches!(
                result,
                Err(ScenarioError::MountedIoTimeout { .. }
                    | ScenarioError::MountedIoWorkerLimit { .. })
            ) {
                controller.emergency_kill_all_no_wait("trash durability timeout")
            } else {
                controller.unmount_all()
            };
            if let Err(error) = cleanup {
                cleanup_errors.push(error.to_string());
            }
        }
        match (result, cleanup_errors.is_empty()) {
            (Ok(()), true) => Ok(()),
            (Err(primary), true) => Err(primary),
            (result, false) => Err(io::Error::other(format!(
                "{}; cleanup: {}",
                result
                    .err()
                    .map_or_else(|| "probe passed".into(), |error| error.to_string()),
                cleanup_errors.join("; ")
            ))
            .into()),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value = serde_json::from_slice(&fs::read(
            context.scenario_dir.join("durability-summary.json"),
        )?)?;
        if (!matches!(self, Self::ColdBootstrap) && summary["offline_crash_replay"] != true)
            || summary["fresh_remote_bootstrap"] != true
        {
            return Err(io::Error::other("trash durability acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{}/durability-summary.json",
            self.definition().name
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn save(context: &ScenarioContext, name: &str, value: &serde_json::Value) -> io::Result<()> {
    fs::write(
        context.scenario_dir.join(name),
        serde_json::to_vec_pretty(value)?,
    )
}

fn authority_head(config: &crate::client::ClientControllerConfig) -> Result<i64, ScenarioError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (client, connection) = tokio::time::timeout(Duration::from_secs(15), tokio_postgres::connect(&config.meta_dsn, tokio_postgres::NoTls))
            .await.map_err(|_| io::Error::other("authority connection timed out"))?
            .map_err(|_| io::Error::other("authority connection failed"))?;
        let connection = tokio::spawn(async move { let _ = connection.await; });
        let result = tokio::time::timeout(Duration::from_secs(15), client.query_one("SELECT ms.version FROM metadata_state ms JOIN volumes v ON v.vol=ms.vol WHERE v.name=$1", &[&config.volume_name])).await;
        drop(client);
        connection.abort();
        let _ = connection.await;
        result.map_err(|_| io::Error::other("authority query timed out"))?
            .map(|row| row.get(0)).map_err(|_| io::Error::other("authority query failed").into())
    })
}

// This fresh fixture creates no application xattrs. Zero shared SetXattr events
// is stronger than absence of its neutral retained-receipt xattrs, without
// coupling the harness to the metadata event wire decoder.
fn shared_xattr_event_count(
    config: &crate::client::ClientControllerConfig,
) -> Result<i64, ScenarioError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (client, connection) = tokio::time::timeout(Duration::from_secs(15), tokio_postgres::connect(&config.meta_dsn, tokio_postgres::NoTls))
            .await.map_err(|_| io::Error::other("receipt authority connection timed out"))?
            .map_err(|_| io::Error::other("receipt authority connection failed"))?;
        let connection = tokio::spawn(async move { let _ = connection.await; });
        let row = tokio::time::timeout(Duration::from_secs(15), client.query_one("SELECT count(*) FROM event_log ev JOIN volumes v ON v.vol=ev.vol WHERE v.name=$1 AND ev.event_type='set_xattr'", &[&config.volume_name])).await;
        drop(client);
        connection.abort();
        let _ = connection.await;
        row.map_err(|_| io::Error::other("receipt authority query timed out"))?
            .map(|row| row.get(0)).map_err(|_| io::Error::other("receipt authority query failed").into())
    })
}
fn wait_pending(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    baseline: u64,
) -> Result<serde_json::Value, ScenarioError> {
    let deadline = Instant::now() + LIMIT;
    loop {
        let snapshot = controller.runtime_snapshot("client-a")?;
        let state = &snapshot["synchronization"]["state"];
        let pending = state["pending_metadata_ops"].as_u64().unwrap_or(0)
            + state["inflight_metadata_ops"].as_u64().unwrap_or(0);
        if pending > 0
            && snapshot["sync_safety"]["generation"]
                .as_u64()
                .is_some_and(|generation| generation > baseline)
        {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "unpublished journal work not observed: {}",
                snapshot["sync_safety"]
            ))
            .into());
        }
        thread::sleep(POLL);
    }
}

fn write_durable(root: PathBuf) -> Result<(), ScenarioError> {
    run_bounded_mounted_io(
        "offline durable file and directory payloads",
        LIMIT,
        move || {
            fs::create_dir(root.join("album"))?;
            for (path, bytes) in [
                ("offline.bin", BODY),
                ("keep.bin", BODY),
                ("album/child.bin", CHILD),
            ] {
                let mut file = fs::File::create(root.join(path))?;
                file.write_all(bytes)?;
                file.sync_all()?;
            }
            Ok(())
        },
    )
}

#[cfg(unix)]
fn attrs(path: &Path) -> io::Result<serde_json::Value> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = fs::symlink_metadata(path)?;
    Ok(
        json!({"size": metadata.len(), "mode": metadata.mode() & 0o777, "uid": metadata.uid(), "gid": metadata.gid(), "nlink": metadata.nlink(), "is_dir": metadata.is_dir()}),
    )
}

#[cfg(not(unix))]
fn attrs(_path: &Path) -> io::Result<serde_json::Value> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Unix attributes required",
    ))
}

fn remote_gets(snapshot: &serde_json::Value) -> Result<u64, ScenarioError> {
    snapshot["write_path"]["fetch"]["s3_get_calls"]
        .as_u64()
        .ok_or_else(|| io::Error::other("S3 GET telemetry missing").into())
}

fn probe(
    context: &ScenarioContext,
    config: &crate::client::ClientControllerConfig,
    warm: &mut MultiClientController<SystemClientProcessExecutor>,
    cold: &mut MultiClientController<SystemClientProcessExecutor>,
    compose: &ComposeServiceControl,
    paused: &mut bool,
    mode: &RecoverableTrashDurability,
) -> Result<(), ScenarioError> {
    let offline_crash_replay = !matches!(mode, RecoverableTrashDurability::ColdBootstrap);
    let postgres_offline = matches!(mode, RecoverableTrashDurability::OfflineCrashReplay);
    warm.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec!["client-a".into(), "client-b".into()],
        clean_existing_local_metadata: true,
    })?;
    let a = warm.client_at(0)?.clone();
    let b = warm.client_at(1)?.clone();
    let mut retention = std::process::Command::new("timeout");
    retention
        .arg("45s")
        .arg(&config.studiofs_bin)
        .args(["trash-days", &a.volume_name, "7"])
        .env("STUDIOFS_CLIENT_ROOT", &a.client_root);
    checked_command(retention, "enable retention")?;
    warm.mount_all()?;
    let uid = std::process::Command::new("id").arg("-u").output()?;
    if !uid.status.success() {
        return Err(io::Error::other("mount uid unavailable").into());
    }
    let trash = format!(".Trash-{}", String::from_utf8_lossy(&uid.stdout).trim());
    // Capture shared authority before stopping its owned PostgreSQL process.
    // With both services stopped, no subsequently accepted namespace/payload
    // can be published before the hard kill or offline recovery.
    let shared_before = authority_head(config)?;
    let baseline = warm.runtime_snapshot("client-a")?["sync_safety"]["generation"]
        .as_u64()
        .ok_or_else(|| io::Error::other("local journal generation missing"))?;
    if offline_crash_replay {
        *paused = true;
        compose.stop_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)?;
        if postgres_offline {
            compose.stop_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
        }
        eprintln!(
            "trash durability: S3 stopped before payload creation; PostgreSQL offline={postgres_offline}"
        );
    }
    write_durable(a.mount_point.clone())?;
    let xdg = context.scenario_dir.join("isolated-xdg");
    fs::create_dir_all(xdg.join("data"))?;
    let mount = a.mount_point.clone();
    run_bounded_mounted_io(
        "offline GIO receipt commits and local fsync barrier",
        LIMIT,
        move || {
            for name in ["offline.bin", "keep.bin", "album"] {
                let mut gio = std::process::Command::new("timeout");
                gio.args(["25s", "gio", "trash"])
                    .arg(mount.join(name))
                    .env("HOME", &xdg)
                    .env("XDG_DATA_HOME", xdg.join("data"))
                    .env("XDG_CONFIG_HOME", xdg.join("config"))
                    .env("XDG_CACHE_HOME", xdg.join("cache"));
                checked_command(gio, "offline GIO trash")?;
            }
            // Strict fsync drains deferred namespace work and synchronizes redb,
            // including preceding receipt transactions, without remote publication.
            let mut barrier = fs::File::create(mount.join("local-barrier.bin"))?;
            barrier.write_all(b"receipt durability barrier\n")?;
            barrier.sync_all()
        },
    )?;
    let pending_before = if offline_crash_replay {
        let snapshot = wait_pending(warm, baseline)?;
        save(context, "before-kill-runtime.json", &snapshot)?;
        Some(snapshot)
    } else {
        None
    };
    for (path, bytes) in [
        ("offline.bin", BODY),
        ("keep.bin", BODY),
        ("album/child.bin", CHILD),
    ] {
        expect_payload(&a.mount_point.join(&trash).join("files").join(path), bytes)?;
    }
    for name in ["offline.bin", "keep.bin", "album"] {
        let sidecar = bounded_mounted_read(
            a.mount_point
                .join(&trash)
                .join("info")
                .join(format!("{name}.trashinfo")),
            "durable local receipt sidecar",
            LIMIT,
        )?;
        if !String::from_utf8_lossy(&sidecar).contains(&format!("Path={name}\n")) {
            return Err(io::Error::other("local receipt original path mismatch").into());
        }
        if offline_crash_replay {
            wait_absent(b.mount_point.join(name))?;
            wait_absent(
                b.mount_point
                    .join(&trash)
                    .join("info")
                    .join(format!("{name}.trashinfo")),
            )?;
        }
    }
    if let Some(pending_before) = pending_before {
        let shared_receipts_before_kill = if postgres_offline {
            json!({"authority_offline_since_before_creates": true})
        } else {
            let records = shared_xattr_event_count(config)?;
            if records != 0 {
                return Err(io::Error::other(format!(
                    "receipt already reached shared authority before hard kill: {records:?}"
                ))
                .into());
            }
            json!({"shared_set_xattr_events": records})
        };
        save(
            context,
            "shared-receipts-before-kill.json",
            &shared_receipts_before_kill,
        )?;
        save(
            context,
            "unpublished-proof.json",
            &json!({"shared_head_before_outage": shared_before, "owned_s3_stopped_before_creates": true, "owned_postgres_stopped_before_creates": postgres_offline, "shared_receipts_before_kill": shared_receipts_before_kill, "peer_originals_and_receipts_absent": true, "local_fsync_barrier_completed": true, "local_pending_journal_generation": pending_before["sync_safety"]["generation"]}),
        )?;
        warm.force_kill("client-a")?;
        fs::copy(
            &a.local_metadata_db,
            context.scenario_dir.join("writer-killed-before-reopen.db"),
        )?;
        eprintln!("trash durability: writer hard killed with unpublished durable receipts");
        warm.remount("client-a")?;
    }
    for (path, bytes) in [
        ("offline.bin", BODY),
        ("keep.bin", BODY),
        ("album/child.bin", CHILD),
    ] {
        expect_payload(&a.mount_point.join(&trash).join("files").join(path), bytes)?;
    }
    for name in ["offline.bin", "keep.bin", "album"] {
        let sidecar = bounded_mounted_read(
            a.mount_point
                .join(&trash)
                .join("info")
                .join(format!("{name}.trashinfo")),
            "recovered offline receipt",
            LIMIT,
        )?;
        if !String::from_utf8_lossy(&sidecar).contains(&format!("Path={name}\n")) {
            return Err(io::Error::other("recovered receipt mismatch").into());
        }
    }
    save(
        context,
        "before-restore-runtime.json",
        &warm.runtime_snapshot("client-a")?,
    )?;
    let mount = a.mount_point.clone();
    let trash_dir = trash.clone();
    run_bounded_mounted_io("restore retained file and directory", LIMIT, move || {
        fs::rename(
            mount.join(&trash_dir).join("files/offline.bin"),
            mount.join("restored.bin"),
        )
        .map_err(|error| io::Error::new(error.kind(), format!("restore offline.bin: {error}")))?;
        fs::rename(
            mount.join(&trash_dir).join("files/album"),
            mount.join("restored-album"),
        )
        .map_err(|error| io::Error::new(error.kind(), format!("restore album: {error}")))?;
        fs::OpenOptions::new()
            .write(true)
            .open(mount.join("local-barrier.bin"))
            .map_err(|error| {
                io::Error::new(error.kind(), format!("reopen local-barrier.bin: {error}"))
            })?
            .sync_all()
            .map_err(|error| {
                io::Error::new(error.kind(), format!("fsync restore barrier: {error}"))
            })
    })?;
    expect_payload(&a.mount_point.join("restored.bin"), BODY)?;
    expect_payload(&a.mount_point.join("restored-album/child.bin"), CHILD)?;
    if offline_crash_replay {
        eprintln!(
            "trash durability: S3-offline reopen retained exact bytes and restored file/directory"
        );
        compose.start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
        compose.start_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)?;
        *paused = false;
    }
    for (path, bytes) in [
        ("restored.bin".to_string(), BODY),
        ("restored-album/child.bin".to_string(), CHILD),
        (format!("{trash}/files/keep.bin"), BODY),
    ] {
        expect_payload(&b.mount_point.join(path), bytes)?;
    }
    let shared_after = authority_head(config)?;
    if shared_after <= shared_before {
        return Err(io::Error::other("shared authority did not advance after recovery").into());
    }
    let checked_paths = [
        "restored.bin".to_string(),
        "restored-album".to_string(),
        "restored-album/child.bin".to_string(),
        format!("{trash}/files/keep.bin"),
        format!("{trash}/info/keep.bin.trashinfo"),
    ];
    let writer_mount = a.mount_point.clone();
    let expected_paths = checked_paths.clone();
    let expected_attrs =
        run_bounded_mounted_io("capture published attributes", LIMIT, move || {
            expected_paths
                .iter()
                .map(|path| attrs(&writer_mount.join(path)))
                .collect::<io::Result<Vec<_>>>()
        })?;
    warm.unmount_all()?;
    // A new controller under a new sandbox owns entirely fresh metadata/cache.
    // Neither the writer cache nor the warm peer cache is copied or injected.
    cold.create_clients(["cold-client"])?;
    let c = cold.client_at(0)?.clone();
    if c.local_metadata_db.exists()
        || (c.cache_dir.exists() && fs::read_dir(&c.cache_dir)?.next().is_some())
    {
        return Err(io::Error::other("cold client reused metadata or cache").into());
    }
    save(
        context,
        "cold-client-before-discover.json",
        &json!({"client": c, "metadata_db_absent": true, "cache_empty": true}),
    )?;
    cold.discover("cold-client")?;
    cold.mount("cold-client")?;
    let cold_before = cold.runtime_snapshot("cold-client")?;
    let gets_before = remote_gets(&cold_before)?;
    for (path, bytes) in [
        ("restored.bin".to_string(), BODY),
        ("restored-album/child.bin".to_string(), CHILD),
        (format!("{trash}/files/keep.bin"), BODY),
    ] {
        expect_payload(&c.mount_point.join(path), bytes)?;
    }
    let reader_mount = c.mount_point.clone();
    let actual_paths = checked_paths.clone();
    let actual_attrs = run_bounded_mounted_io("cold bootstrapped attributes", LIMIT, move || {
        actual_paths
            .iter()
            .map(|path| attrs(&reader_mount.join(path)))
            .collect::<io::Result<Vec<_>>>()
    })?;
    if actual_attrs != expected_attrs {
        return Err(io::Error::other(format!(
            "cold attributes mismatch: expected {expected_attrs:?}, actual {actual_attrs:?}"
        ))
        .into());
    }
    let sidecar = bounded_mounted_read(
        c.mount_point.join(&trash).join("info/keep.bin.trashinfo"),
        "cold receipt original path",
        LIMIT,
    )?;
    if !String::from_utf8_lossy(&sidecar).contains("Path=keep.bin\n") {
        return Err(io::Error::other("cold receipt mismatch").into());
    }
    for name in ["offline.bin", "album"] {
        wait_absent(c.mount_point.join(name))?;
        wait_absent(
            c.mount_point
                .join(&trash)
                .join("info")
                .join(format!("{name}.trashinfo")),
        )?;
    }
    let cold_after = cold.runtime_snapshot("cold-client")?;
    let gets_after = remote_gets(&cold_after)?;
    if gets_after <= gets_before {
        return Err(
            io::Error::other("cold payload reads did not perform observable S3 GETs").into(),
        );
    }
    save(context, "cold-runtime-before.json", &cold_before)?;
    save(context, "cold-runtime-after.json", &cold_after)?;
    save(
        context,
        "durability-summary.json",
        &json!({"offline_crash_replay": offline_crash_replay, "hard_kill": offline_crash_replay, "same_writer_metadata_cache_reopened": offline_crash_replay, "file_and_directory_restored_while_s3_stopped": offline_crash_replay, "postgres_offline_during_reopen": postgres_offline, "shared_head_before": shared_before, "shared_head_after": shared_after, "peer_exact_bytes_after_publication": true, "fresh_remote_bootstrap": true, "cold_metadata_absent_before_discover": true, "cold_cache_empty_before_discover": true, "warm_clients_unmounted_before_cold_discover": true, "cold_s3_gets_before": gets_before, "cold_s3_gets_after": gets_after, "checked_paths": checked_paths, "expected_attrs": expected_attrs, "actual_attrs": actual_attrs, "file_sha256": bytes_sha256(BODY), "child_sha256": bytes_sha256(CHILD)}),
    )?;
    eprintln!(
        "trash durability: fresh client exact remote bytes/attributes/receipt and S3 GETs verified"
    );
    Ok(())
}

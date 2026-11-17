//! Mounted logical quota enforcement and recovery.

use super::*;

pub(in crate::scenario) struct LogicalQuota;
const NAME: &str = "mounted-logical-quota";
const LIMIT: Duration = Duration::from_secs(45);
const QUOTA_BYTES: u64 = 64 * 1024 * 1024;

impl Scenario for LogicalQuota {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: NAME,
            description: "Mounted logical quota rejects growth above the cap and permits shrink and later growth",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;
        let binary = config.studiofs_bin.clone();
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let result = probe(context, &binary, &mut controller);
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("logical quota timeout")
        } else {
            controller.unmount_all()
        };
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(cleanup.into()),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: NAME.into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup.into()),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join("capacity.json"))?)?;
        if report["total_bytes"] != QUOTA_BYTES
            || report["free_at_limit"] != 0
            || report["converged_used_bytes"] != QUOTA_BYTES / 2 * 3
        {
            return Err(io::Error::other("unexpected mounted quota capacity").into());
        }
        Ok(vec![format!("artifacts/scenarios/{NAME}/capacity.json")])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn probe(
    context: &ScenarioContext,
    binary: &Path,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<(), ScenarioError> {
    let compose = compose_control_from_manifest(context)?;
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec!["client-a".into(), "client-b".into()],
        clean_existing_local_metadata: true,
    })?;
    let a = controller.client_at(0)?.clone();
    let b = controller.client_at(1)?.clone();
    use crate::infra::{CommandExecutor, CommandSpec, SystemCommandExecutor};
    let quota = CommandSpec {
        program: binary.to_string_lossy().into_owned(),
        args: vec!["quota".into(), a.volume_name.clone(), "64M".into()],
        env: vec![(
            "STUDIOFS_CLIENT_ROOT".into(),
            a.client_root.to_string_lossy().into_owned(),
        )],
        cwd: None,
        timeout: Some(LIMIT),
    };
    if SystemCommandExecutor.run(&quota)?.status != 0 {
        return Err(io::Error::other("setting the scenario volume quota failed").into());
    }
    controller.mount_all()?;
    let wait_usage = |client: &str, expected: u64| -> Result<(), ScenarioError> {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let snapshot = controller.runtime_snapshot(client)?;
            let quota = &snapshot["logical_quota"];
            if quota["used_bytes"] == expected && quota["limit_bytes"] == QUOTA_BYTES {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other(format!(
                    "{client} usage failed to reach {expected}: {quota}"
                ))
                .into());
            }
            thread::sleep(Duration::from_millis(200));
        }
    };
    for client in ["client-a", "client-b"] {
        wait_usage(client, 0)?;
    }
    let path = a.mount_point.join("quota.bin");
    let (total, free) =
        run_bounded_mounted_io("logical quota enforcement and recovery", LIMIT, move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            let total = loop {
                let total = fs2::total_space(&a.mount_point)?;
                if total == QUOTA_BYTES {
                    break total;
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::other(format!(
                        "expected {QUOTA_BYTES} capacity, observed {total}"
                    )));
                }
                thread::sleep(Duration::from_millis(100));
            };
            let file = fs::File::create(&path)?;
            match file.set_len(QUOTA_BYTES + 1) {
                Err(error) if error.kind() == io::ErrorKind::StorageFull => {}
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!("over-quota set_len returned {:?}: {error}", error.kind()),
                    ));
                }
                Ok(()) => {
                    return Err(io::Error::other(
                        "over-quota set_len unexpectedly succeeded",
                    ));
                }
            }
            file.set_len(QUOTA_BYTES)
                .map_err(|e| io::Error::other(format!("growth to configured cap: {e}")))?;
            file.set_len(QUOTA_BYTES / 2)?;
            file.set_len(QUOTA_BYTES)
                .map_err(|e| io::Error::other(format!("growth to configured cap: {e}")))?;
            file.sync_all()?;
            let free = fs2::available_space(&a.mount_point)?;
            file.set_len(0)?;
            file.sync_all()?;
            Ok((total, free))
        })?;
    for client in ["client-a", "client-b"] {
        wait_usage(client, 0)?;
    }
    let a = controller.client_at(0)?.mount_point.clone();
    let b = b.mount_point;
    for (local, peer, name) in [(&a, &b, "left"), (&b, &a, "right")] {
        let path = local.join(name);
        run_bounded_mounted_io("seed quota peers", LIMIT, move || {
            let mut file = fs::File::create(path)?;
            file.write_all(b"x")
                .map_err(|e| io::Error::other(format!("seed {name}: {e}")))?;
            file.sync_all()
        })?;
        super::recoverable_trash::expect_payload(&peer.join(name), b"x")?;
    }
    for client in ["client-a", "client-b"] {
        wait_usage(client, 2)?;
    }
    const EOF: u64 = QUOTA_BYTES / 4 * 3;
    let writers = [a.join("left"), b.join("right")];
    compose.stop_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
    let writes = run_bounded_mounted_io(
        "admit growth while PostgreSQL is stopped",
        LIMIT,
        move || {
            for path in writers {
                let mut file = fs::OpenOptions::new().write(true).open(path)?;
                file.seek(SeekFrom::Start(EOF - 1))?;
                file.write_all(b"z")
                    .map_err(|e| io::Error::other(format!("offline growth: {e}")))?;
                file.sync_all()?;
            }
            Ok(())
        },
    );
    let restart = compose.start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor);
    writes?;
    restart?;
    for client in ["client-a", "client-b"] {
        wait_usage(client, 2 * EOF)?;
    }
    for path in [a.join("right"), b.join("left")] {
        run_bounded_mounted_io("read converged peer data above quota", LIMIT, move || {
            let mut file = fs::File::open(path)?;
            file.seek(SeekFrom::Start(EOF - 1))?;
            let mut byte = [0];
            file.read_exact(&mut byte)?;
            if byte != *b"z" {
                return Err(io::Error::other("peer byte mismatch"));
            }
            Ok(())
        })?;
    }
    fs::write(
        context.scenario_dir.join("capacity.json"),
        serde_json::to_vec(
            &serde_json::json!({"total_bytes":total,"free_at_limit":free,"converged_used_bytes":2*EOF}),
        )?,
    )?;
    Ok(())
}

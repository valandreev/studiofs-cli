//! Mounted acceptance for cache scrub repair and dirty-payload isolation.

use super::*;
use studiofs_vfs::VfsCacheScrubSnapshot;

const SUMMARY: &str = "cache-scrub.json";
const WAIT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(250);
const CLIENT: &str = "client-a";

pub(in crate::scenario) struct MountedCacheScrub;

impl Scenario for MountedCacheScrub {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: "mounted-cache-scrub",
            description: "repair three corrupt cache objects and preserve an unpublished payload",
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
        let compose = compose_control_from_manifest(context)?;
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let mut s3_stopped = false;
        let result = exercise(context, &mut controller, &compose, &mut s3_stopped);

        let recovery = if s3_stopped {
            compose
                .start_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)
                .map_err(ScenarioError::from)
        } else {
            Ok(())
        };
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("cache scrub mounted I/O timeout")
        } else {
            controller.unmount_all()
        }
        .map_err(ScenarioError::from);
        match (result, recovery.and(cleanup)) {
            (result, Ok(())) | (Ok(()), result) => result,
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: "mounted cache scrub".into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY))?)?;
        if summary["dirty_payload_preserved"] != true || summary["repaired_objects"] != 3 {
            return Err(io::Error::other("cache scrub acceptance evidence is incomplete").into());
        }
        Ok(Vec::new())
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn exercise(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    compose: &ComposeServiceControl,
    s3_stopped: &mut bool,
) -> Result<(), ScenarioError> {
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec![CLIENT.into()],
        clean_existing_local_metadata: true,
    })?;
    let client = controller.clients()[0].clone();
    let uuid = studiofs_meta::open_meta_engine(&client.local_metadata_db)
        .and_then(|meta| meta.get_format_info())
        .map_err(io::Error::other)?
        .uuid;
    let objects = client.cache_dir.join(uuid).join("payload").join("objects");
    controller.set_client_mount_arg_overrides(
        CLIENT,
        ["--cache-scrub", "true", "--cache-scrub-interval-secs", "2"],
    )?;
    controller.mount_all()?;

    let clean = [
        ("checksum.bin", vec![0x19; 8192]),
        ("missing.bin", vec![0x2a; 8192]),
        ("size.bin", vec![0x3b; 8192]),
    ];
    for (name, bytes) in &clean {
        write_local(&client.mount_point, name, bytes)?;
    }
    wait_snapshot(
        controller,
        "initial writes did not upload",
        |runtime, scrub| {
            Ok(runtime["sync_safety"]["state"] == "safe" && scrub.cache_scrub_checked >= 3)
        },
    )?;
    let paths = clean
        .iter()
        .map(|(_, bytes)| {
            find_payload(&objects, bytes)?
                .ok_or_else(|| io::Error::other("could not find clean cache payload"))
        })
        .collect::<io::Result<Vec<_>>>()?;

    // Keep the local payload unpublished for an entire pass by stopping S3.
    compose.stop_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)?;
    *s3_stopped = true;
    let dirty_bytes = vec![0x4c; 8192];
    write_local(&client.mount_point, "dirty.bin", &dirty_bytes)?;
    let mut dirty = None;
    let dirty_start = wait_snapshot(
        controller,
        "unpublished payload did not materialize in the cache",
        |runtime, _| {
            dirty = find_payload(&objects, &dirty_bytes)?;
            Ok(dirty.is_some()
                && runtime["upload"]["pending_uploads"]
                    .as_u64()
                    .is_some_and(|n| n > 0))
        },
    )?;
    let dirty = dirty.ok_or_else(|| io::Error::other("missing unpublished cache payload"))?;
    // Let any pass already in flight finish before waiting for a complete offline pass.
    let offline_baseline = wait_snapshot(
        controller,
        "no pass completed after the dirty write",
        |_, scrub| {
            Ok(
                scrub.cache_scrub_pass_completed_at > dirty_start.cache_scrub_pass_completed_at
                    && scrub.progress.is_none(),
            )
        },
    )?;
    let dirty_end = wait_snapshot(
        controller,
        "no full pass completed with a pending upload",
        |runtime, scrub| {
            Ok(runtime["upload"]["pending_uploads"]
                .as_u64()
                .is_some_and(|n| n > 0)
                && scrub.cache_scrub_pass_completed_at
                    > offline_baseline.cache_scrub_pass_completed_at
                && scrub.cache_scrub_checked >= offline_baseline.cache_scrub_checked + 3
                && scrub.cache_scrub_corrupt == dirty_start.cache_scrub_corrupt)
        },
    )?;
    if fs::read(&dirty)? != dirty_bytes
        || bounded_mounted_read(
            client.mount_point.join("dirty.bin"),
            "dirty scrub readback",
            WAIT,
        )? != dirty_bytes
    {
        return Err(io::Error::other("scrub changed the unpublished payload").into());
    }
    compose.start_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)?;
    *s3_stopped = false;
    let before = wait_snapshot(
        controller,
        "dirty payload did not upload after S3 recovery",
        |runtime, scrub| Ok(runtime["sync_safety"]["state"] == "safe" && scrub.progress.is_none()),
    )?;

    let mut file = OpenOptions::new().write(true).open(&paths[0])?;
    file.write_all(&[clean[0].1[0] ^ 1])?;
    drop(file);
    fs::remove_file(&paths[1])?;
    OpenOptions::new().write(true).open(&paths[2])?.set_len(1)?;
    let repaired = wait_snapshot(
        controller,
        "scrub did not repair all three corrupt objects",
        |_, scrub| {
            Ok(
                scrub.cache_scrub_refetched >= before.cache_scrub_refetched + 3
                    && scrub.cache_scrub_pass_completed_at > before.cache_scrub_pass_completed_at,
            )
        },
    )?;
    if repaired.cache_scrub_corrupt.checksum <= before.cache_scrub_corrupt.checksum
        || repaired.cache_scrub_corrupt.missing <= before.cache_scrub_corrupt.missing
        || repaired.cache_scrub_corrupt.size <= before.cache_scrub_corrupt.size
        || repaired.cache_scrub_corrupt.io != before.cache_scrub_corrupt.io
        || repaired.cache_scrub_checked < before.cache_scrub_checked + 3
        || repaired.cache_scrub_refetch_failed != before.cache_scrub_refetch_failed
    {
        return Err(io::Error::other(format!(
            "unexpected scrub counters: {before:?} -> {repaired:?}"
        ))
        .into());
    }
    for ((name, bytes), path) in clean.iter().zip(paths) {
        // Read the cache first to distinguish scrub repair from a foreground refill.
        if fs::read(&path)? != *bytes
            || bounded_mounted_read(
                client.mount_point.join(name),
                "repaired scrub readback",
                WAIT,
            )? != *bytes
        {
            return Err(io::Error::other(format!("incorrect payload after scrub: {name}")).into());
        }
    }
    fs::write(
        context.scenario_dir.join(SUMMARY),
        serde_json::to_vec_pretty(&json!({
            "dirty_payload_preserved": true,
            "repaired_objects": 3,
            "dirty_phase": dirty_end,
            "before": before,
            "repaired": repaired,
        }))?,
    )?;
    Ok(())
}

fn write_local(mount: &Path, name: &str, bytes: &[u8]) -> Result<(), ScenarioError> {
    let path = mount.join(name);
    let bytes = bytes.to_vec();
    run_bounded_mounted_io("write scrub fixture", WAIT, move || {
        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()
    })
}

fn find_payload(dir: &Path, bytes: &[u8]) -> io::Result<Option<PathBuf>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        if kind.is_dir() {
            if let Some(found) = find_payload(&path, bytes)? {
                return Ok(Some(found));
            }
        } else if kind.is_file()
            && path.extension().is_some_and(|ext| ext == "object")
            && entry.metadata()?.len() == bytes.len() as u64
            && fs::read(&path)? == bytes
        {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

fn read_snapshot(
    controller: &MultiClientController<SystemClientProcessExecutor>,
) -> Result<(serde_json::Value, VfsCacheScrubSnapshot), ScenarioError> {
    let runtime = controller.runtime_snapshot(CLIENT)?;
    let scrub = serde_json::from_value(runtime["cache_scrub"].clone())?;
    Ok((runtime, scrub))
}

fn wait_snapshot(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    message: &str,
    mut ready: impl FnMut(&serde_json::Value, &VfsCacheScrubSnapshot) -> io::Result<bool>,
) -> Result<VfsCacheScrubSnapshot, ScenarioError> {
    let deadline = Instant::now() + WAIT;
    loop {
        let (runtime, scrub) = read_snapshot(controller)?;
        if ready(&runtime, &scrub)? {
            return Ok(scrub);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::other(format!("{message}: {scrub:?}")).into());
        }
        thread::sleep(POLL.min(remaining));
    }
}

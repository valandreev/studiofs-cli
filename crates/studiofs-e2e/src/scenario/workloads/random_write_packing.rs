//! Mounted random-write packing and upload-progress acceptance.

use super::*;
use studiofs_vfs::RuntimeStatsSnapshot;

const SUMMARY: &str = "random-write-packing.json";
const FILE_BYTES: usize = 256 * 1024 * 1024;
const WRITE_BYTES: usize = 4096;
const PACK_BYTES: usize = 4 * 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(120);

pub(in crate::scenario) struct RandomWritePacking;

impl Scenario for RandomWritePacking {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: "mounted-random-write-packing",
            description: "random 4 KiB writes pack payloads and upload before fsync",
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
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let result = measure(context, &mut controller);
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("random-write packing timeout")
        } else {
            controller.unmount_all()
        };
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(cleanup.into()),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: "random-write packing".into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup.into()),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY))?)?;
        let writes = summary["writes"].as_u64().unwrap_or(0);
        let objects = summary["payload_objects"].as_u64().unwrap_or(u64::MAX);
        // Each drain can add a partial pack; 32 exceeds the number of aged drains in 15 seconds.
        let bound = writes
            .saturating_mul(WRITE_BYTES as u64)
            .div_ceil(PACK_BYTES as u64)
            + 32;
        if writes == 0
            || objects == 0
            || objects > bound
            || summary["upload_during_writes"].as_bool() != Some(true)
            || summary["readback_matches"].as_bool() != Some(true)
        {
            return Err(io::Error::other(format!(
                "random-write packing acceptance failed (object bound {bound}): {summary}"
            ))
            .into());
        }
        Ok(Vec::new())
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn measure(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<(), ScenarioError> {
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec!["client-a".into()],
        clean_existing_local_metadata: true,
    })?;
    controller.mount_all()?;
    let client = controller.clients()[0].clone();
    let mount = client.mount_point.clone();
    let mut summary = run_bounded_mounted_io(
        "random-write packing workload",
        IO_TIMEOUT,
        move || {
            let mut file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(mount.join("random.bin"))?;
            file.set_len(FILE_BYTES as u64)?;
            let mut expected = vec![0; FILE_BYTES];
            let mut state = 900_u64;
            let mut writes = 0_u64;
            let mut upload_during_writes = false;
            let mut samples = Vec::new();
            let started = Instant::now();
            while started.elapsed() < Duration::from_secs(15) {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let offset = ((state >> 32) as usize % (FILE_BYTES / WRITE_BYTES)) * WRITE_BYTES;
                let payload = [state as u8; WRITE_BYTES];
                file.seek(SeekFrom::Start(offset as u64))?;
                file.write_all(&payload)?;
                expected[offset..offset + WRITE_BYTES].copy_from_slice(&payload);
                writes += 1;
                if writes.is_multiple_of(256) {
                    let text = fs::read_to_string(mount.join(".studiofs-runtime-stats"))?;
                    let stats =
                        RuntimeStatsSnapshot::parse_text(&text).map_err(io::Error::other)?;
                    let pending = stats
                        .upload_queue
                        .gauges
                        .get("pending_depth")
                        .map_or(0, |metric| metric.value);
                    let active = stats
                        .upload_queue
                        .gauges
                        .get("active_uploads")
                        .map_or(0, |metric| metric.value);
                    upload_during_writes |= pending > 0 && active > 0;
                    samples.push(json!({"elapsed_ms": started.elapsed().as_millis(), "pending": pending, "active": active}));
                }
            }
            let before_fsync = fs::read_to_string(mount.join(".studiofs-runtime-stats"))?;
            let fsync_started = Instant::now();
            file.sync_all()?;
            let fsync_ms = fsync_started.elapsed().as_secs_f64() * 1000.0;
            let after_fsync = fs::read_to_string(mount.join(".studiofs-runtime-stats"))?;
            file.seek(SeekFrom::Start(0))?;
            let mut actual = Vec::new();
            file.read_to_end(&mut actual)?;
            Ok(
                json!({"writes": writes, "file_bytes": FILE_BYTES, "write_bytes": WRITE_BYTES,
            "fsync_ms": fsync_ms, "upload_during_writes": upload_during_writes,
            "readback_matches": actual == expected, "upload_samples": samples, "before_fsync": before_fsync, "after_fsync": after_fsync}),
            )
        },
    )?;
    summary["payload_objects"] = json!(count_payload_objects(&client.cache_dir)?);
    fs::write(
        context.scenario_dir.join(SUMMARY),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(())
}

fn count_payload_objects(root: &Path) -> io::Result<u64> {
    let mut count = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            count += count_payload_objects(&entry.path())?;
        } else if entry.path().extension().is_some_and(|ext| ext == "object") {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_only_payload_objects_in_nested_directories() {
        let tmp = studiofs_test_support::workspace_target_tempdir("packing-count-");
        fs::create_dir(tmp.path().join("ab")).unwrap();
        fs::write(tmp.path().join("ab/1.object"), b"payload").unwrap();
        fs::write(tmp.path().join("ab/2.stage"), b"stage").unwrap();
        assert_eq!(count_payload_objects(tmp.path()).unwrap(), 1);
    }
}

//! Mounted random-write packing and upload-progress acceptance.

use super::*;
use std::collections::BTreeSet;
use std::ffi::OsString;
use studiofs_vfs::{RUNTIME_STATS_FILE_REPORTED_SIZE, RuntimeStatsSnapshot};

const SUMMARY: &str = "random-write-packing.json";
const FILE_BYTES: usize = 256 * 1024 * 1024;
const WRITE_BYTES: usize = 4096;
const PACK_BYTES: usize = 4 * 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(120);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
// Aged writer-stack bytes drain after about 1.2 s; allow commit and upload admission on top.
const FIRST_UPLOAD_DEADLINE_MS: u64 = 5_000;
// Only a stuck fsync fails: loaded hosts spend seconds sealing the writer tail and committing.
const FSYNC_MAX_MS: f64 = 60_000.0;
// WinFSP may bound reads by the reported size, so live snapshots must stay well below it.
const RUNTIME_STATS_MAX_BYTES: usize = RUNTIME_STATS_FILE_REPORTED_SIZE as usize / 2;

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
        let first_upload_ms = summary["first_upload_ms"].as_u64().unwrap_or(u64::MAX);
        let fsync_ms = summary["fsync_ms"].as_f64().unwrap_or(f64::INFINITY);
        let stats_bytes = summary["after_fsync"].as_str().map_or(usize::MAX, str::len);
        let requested_bytes = writes.saturating_mul(WRITE_BYTES as u64);
        let accepted_bytes = accepted_write_bytes(summary["after_fsync"].as_str().unwrap_or(""))?;
        let bound = payload_object_bound(accepted_bytes);
        if writes == 0
            || accepted_bytes < requested_bytes
            || objects == 0
            || objects > bound
            || summary["upload_during_writes"].as_bool() != Some(true)
            || first_upload_ms > FIRST_UPLOAD_DEADLINE_MS
            || fsync_ms > FSYNC_MAX_MS
            || summary["readback_matches"].as_bool() != Some(true)
            || stats_bytes > RUNTIME_STATS_MAX_BYTES
        {
            return Err(io::Error::other(format!(
                "random-write packing acceptance failed (object bound {bound} for \
                 {accepted_bytes} accepted bytes, {requested_bytes} requested, \
                 first upload deadline {FIRST_UPLOAD_DEADLINE_MS} ms, fsync bound {FSYNC_MAX_MS} ms, \
                 runtime stats {stats_bytes}/{RUNTIME_STATS_MAX_BYTES} bytes): {summary}"
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
    let preexisting_caches = cache_entries(&crate::client::client_cache_dir(
        &context.sandbox,
        "client-a",
    ))?;
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
            let mut first_upload_ms = None;
            let mut samples = Vec::new();
            let started = Instant::now();
            let mut last_sample = started;
            while started.elapsed() < Duration::from_secs(15) {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let offset = ((state >> 32) as usize % (FILE_BYTES / WRITE_BYTES)) * WRITE_BYTES;
                let payload = [state as u8; WRITE_BYTES];
                file.seek(SeekFrom::Start(offset as u64))?;
                file.write_all(&payload)?;
                expected[offset..offset + WRITE_BYTES].copy_from_slice(&payload);
                writes += 1;
                if last_sample.elapsed() >= SAMPLE_INTERVAL {
                    last_sample = Instant::now();
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
                    let elapsed_ms = started.elapsed().as_millis() as u64;
                    if pending > 0 && active > 0 {
                        upload_during_writes = true;
                        first_upload_ms.get_or_insert(elapsed_ms);
                    }
                    samples.push(
                        json!({"elapsed_ms": elapsed_ms, "pending": pending, "active": active}),
                    );
                }
            }
            let before_fsync = fs::read_to_string(mount.join(".studiofs-runtime-stats"))?;
            let fsync_started = Instant::now();
            file.sync_all()?;
            let fsync_ms = fsync_started.elapsed().as_secs_f64() * 1000.0;
            let after_fsync = fs::read_to_string(mount.join(".studiofs-runtime-stats"))?;
            let fsync_commit_ms =
                metadata_commit_ms(&after_fsync)? - metadata_commit_ms(&before_fsync)?;
            file.seek(SeekFrom::Start(0))?;
            let mut actual = Vec::new();
            file.read_to_end(&mut actual)?;
            Ok(
                json!({"writes": writes, "file_bytes": FILE_BYTES, "write_bytes": WRITE_BYTES,
            "fsync_ms": fsync_ms, "fsync_commit_ms": fsync_commit_ms,
            "upload_during_writes": upload_during_writes, "first_upload_ms": first_upload_ms,
            "readback_matches": actual == expected, "upload_samples": samples, "before_fsync": before_fsync, "after_fsync": after_fsync}),
            )
        },
    )?;
    summary["payload_objects"] = json!(count_new_volume_payload_objects(
        &client.cache_dir,
        &preexisting_caches
    )?);
    fs::write(
        context.scenario_dir.join(SUMMARY),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(())
}

/// Cumulative local metadata commit time; the delta across fsync covers its final overlay commit.
fn metadata_commit_ms(stats_text: &str) -> io::Result<f64> {
    let stats = RuntimeStatsSnapshot::parse_text(stats_text).map_err(io::Error::other)?;
    let histogram = stats
        .metadata
        .latencies
        .get("commit")
        .ok_or_else(|| io::Error::other("runtime stats lack the metadata.commit histogram"))?;
    Ok(histogram.sum as f64 / 1_000_000.0)
}

/// Bytes the VFS accepted from write callbacks; macFUSE rounds UBC writes up to whole pages.
fn accepted_write_bytes(stats_text: &str) -> io::Result<u64> {
    let stats = RuntimeStatsSnapshot::parse_text(stats_text).map_err(io::Error::other)?;
    stats
        .vfs_write
        .counters
        .get("accepted_bytes")
        .map(|metric| metric.value)
        .ok_or_else(|| io::Error::other("runtime stats lack the vfs_write.accepted_bytes counter"))
}

/// Each drain can add a partial pack; 32 exceeds the number of aged drains in 15 seconds.
fn payload_object_bound(accepted_bytes: u64) -> u64 {
    accepted_bytes.div_ceil(PACK_BYTES as u64) + 32
}

fn cache_entries(cache_dir: &Path) -> io::Result<BTreeSet<OsString>> {
    fs::read_dir(cache_dir)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect()
}

/// A full run shares the client cache dir across scenarios; earlier volumes stay retained there.
fn count_new_volume_payload_objects(
    cache_dir: &Path,
    preexisting: &BTreeSet<OsString>,
) -> io::Result<u64> {
    let mut count = 0;
    for entry in fs::read_dir(cache_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && !preexisting.contains(&entry.file_name()) {
            count += count_payload_objects(&entry.path())?;
        }
    }
    Ok(count)
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

    #[test]
    fn counts_only_volume_caches_created_after_the_snapshot() {
        let tmp = studiofs_test_support::workspace_target_tempdir("packing-count-");
        fs::create_dir_all(tmp.path().join("earlier-volume/ab")).unwrap();
        fs::write(tmp.path().join("earlier-volume/ab/1.object"), b"old").unwrap();
        let preexisting = cache_entries(tmp.path()).unwrap();
        fs::create_dir_all(tmp.path().join("scenario-volume/cd")).unwrap();
        fs::write(tmp.path().join("scenario-volume/cd/2.object"), b"new").unwrap();
        fs::write(tmp.path().join("scenario-volume/cd/3.object"), b"new").unwrap();
        assert_eq!(
            count_new_volume_payload_objects(tmp.path(), &preexisting).unwrap(),
            2
        );
    }

    #[test]
    fn object_bound_follows_bytes_accepted_after_kernel_page_rounding() {
        // A retained macOS run: 50,682 random 4 KiB writes reached the VFS as 16 KiB pages and
        // packed into 170 objects, which is about 4 MiB per object.
        let text = "# studiofs_runtime_stats schema_version=1\n\
                    counter vfs_write.accepted_bytes bytes 830373888\n";
        assert_eq!(
            payload_object_bound(accepted_write_bytes(text).unwrap()),
            230
        );
        assert_eq!(payload_object_bound(50_682 * WRITE_BYTES as u64), 82);
        assert!(
            accepted_write_bytes("# studiofs_runtime_stats schema_version=1\n").is_err(),
            "a renamed counter must not turn into a zero object bound"
        );
    }

    #[test]
    fn reads_cumulative_metadata_commit_time_in_milliseconds() {
        let text = "# studiofs_runtime_stats schema_version=1\n\
                    histogram_count metadata.commit ns 3\n\
                    histogram_sum metadata.commit ns 2490462652\n";
        assert_eq!(metadata_commit_ms(text).unwrap(), 2_490.462_652);
        assert!(
            metadata_commit_ms("# studiofs_runtime_stats schema_version=1\n").is_err(),
            "a renamed commit metric must not turn into a zero fsync bound"
        );
    }
}

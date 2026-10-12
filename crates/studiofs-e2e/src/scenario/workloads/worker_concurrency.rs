//! Opt-in mounted cold-read worker-policy measurements.

use super::*;

const FILE_COUNT: usize = 128;
const _: () = assert!(FILE_COUNT <= u8::MAX as usize + 1);
const FILE_BYTES: usize = 256 * 1024;
const READERS: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(30);

pub(in crate::scenario) struct WorkerConcurrency;

impl Scenario for WorkerConcurrency {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: "mounted-worker-concurrency",
            description: "opt-in mounted cold/warm concurrent read worker-policy measurement",
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
            .ok_or_else(|| io::Error::other("worker measurement requires real mounted clients"))?;
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let result = measure(context, &mut controller);
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("worker measurement timeout")
        } else {
            controller.unmount_all()
        };
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(cleanup.into()),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: "worker measurement".into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup.into()),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value = serde_json::from_slice(&fs::read(
            context.scenario_dir.join("worker-measurement.json"),
        )?)?;
        let expected = (FILE_COUNT * FILE_BYTES) as u64;
        if summary["cold_bytes"].as_u64() != Some(expected)
            || summary["warm_bytes"].as_u64() != Some(expected)
        {
            return Err(io::Error::other(format!(
                "worker measurement byte count mismatch: expected {expected}, got cold={}, warm={}",
                summary["cold_bytes"], summary["warm_bytes"]
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
        client_names: vec!["client-a".into(), "client-b".into()],
        clean_existing_local_metadata: true,
    })?;
    controller.mount_all()?;
    let clients: Vec<_> = controller.clients().into_iter().cloned().collect();
    let writer = clients[0].mount_point.join("worker-measurement");
    let observer = clients[1].mount_point.join("worker-measurement");
    let writer_dir = writer.clone();
    run_bounded_mounted_io("worker measurement mkdir", IO_TIMEOUT, move || {
        fs::create_dir(&writer_dir)
    })?;
    for index in 0..FILE_COUNT {
        bounded_mounted_write(
            writer.join(index.to_string()),
            vec![u8::try_from(index).expect("FILE_COUNT fits unique byte markers"); FILE_BYTES],
            format!("worker measurement write {index}"),
            IO_TIMEOUT,
        )?;
    }

    wait_for_publication(&observer, Duration::from_secs(90))?;
    let cold_started = Instant::now();
    let cold_bytes = read_batch(&observer, FILE_COUNT, FILE_BYTES)?;
    let cold_seconds = cold_started.elapsed().as_secs_f64();
    let warm_started = Instant::now();
    let warm_bytes = read_batch(&observer, FILE_COUNT, FILE_BYTES)?;
    let warm_seconds = warm_started.elapsed().as_secs_f64();
    fs::write(
        context.scenario_dir.join("worker-measurement.json"),
        serde_json::to_vec_pretty(&json!({
            "files": FILE_COUNT, "file_bytes": FILE_BYTES, "readers": READERS,
            "cold_bytes": cold_bytes, "cold_seconds": cold_seconds,
            "warm_bytes": warm_bytes, "warm_seconds": warm_seconds,
            "writer_mount": writer, "observer_mount": observer,
            "observer_cache": clients[1].cache_dir,
        }))?,
    )?;
    Ok(())
}

fn wait_for_publication(observer: &Path, timeout: Duration) -> Result<(), ScenarioError> {
    // Peer-visible nonempty files require published payloads; stat does not warm their bytes.
    let deadline = Instant::now() + timeout;
    for index in 0..FILE_COUNT {
        let mut observed_size = None;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, format!(
                    "payload publication timed out waiting for file {index} ({index}/{FILE_COUNT} published, last observed size {observed_size:?}, expected {FILE_BYTES} bytes)"
                )).into());
            }
            match bounded_mounted_metadata(
                observer.join(index.to_string()),
                format!("worker measurement publication {index}"),
                remaining.min(IO_TIMEOUT),
            ) {
                Ok(metadata) if metadata.len() == FILE_BYTES as u64 => break,
                Ok(metadata) if metadata.len() > FILE_BYTES as u64 => {
                    return Err(io::Error::other(format!(
                        "file {index} size exceeded expected {FILE_BYTES} bytes: got {}",
                        metadata.len()
                    ))
                    .into());
                }
                Ok(metadata) => observed_size = Some(metadata.len()),
                Err(ScenarioError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    observed_size = None
                }
                Err(error) => return Err(error),
            }
            thread::sleep(remaining.min(Duration::from_millis(50)));
        }
    }
    Ok(())
}

fn read_batch(root: &Path, files: usize, file_bytes: usize) -> Result<u64, ScenarioError> {
    if files > usize::from(u8::MAX) + 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker measurement requires at most 256 unique byte markers",
        )
        .into());
    }
    let pool = BoundedMountedIoPool::new(READERS);
    let cancelled = AtomicBool::new(false);
    thread::scope(|scope| {
        let workers: Vec<_> = (0..files.min(READERS))
            .map(|reader| {
                let pool = pool.clone();
                let cancelled = &cancelled;
                thread::Builder::new().spawn_scoped(scope, move || {
                    read_worker(
                        root,
                        (reader..files).step_by(READERS),
                        file_bytes,
                        &pool,
                        cancelled,
                    )
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut total = Ok(0);
        // Join every worker even after an error so scope cannot rethrow an unhandled panic.
        for worker in workers {
            let result = worker
                .join()
                .map_err(|_| {
                    ScenarioError::Io(io::Error::other("worker measurement reader panicked"))
                })
                .and_then(|result| result);
            total = total.and_then(|total| result.map(|bytes| total + bytes));
        }
        total
    })
}

fn read_worker(
    root: &Path,
    indices: impl Iterator<Item = usize>,
    file_bytes: usize,
    pool: &BoundedMountedIoPool,
    cancelled: &AtomicBool,
) -> Result<u64, ScenarioError> {
    let mut bytes = 0;
    for index in indices {
        if cancelled.load(Ordering::Relaxed) {
            break;
        }
        let path = root.join(index.to_string());
        bytes += pool
            .run(
                format!("worker measurement read {index}"),
                IO_TIMEOUT,
                move || {
                    let expected_byte = u8::try_from(index).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "worker measurement file index exceeds byte range",
                        )
                    })?;
                    let data = fs::read(path)?;
                    if data.len() != file_bytes || data.iter().any(|byte| *byte != expected_byte) {
                        return Err(io::Error::other("worker measurement payload mismatch"));
                    }
                    Ok(data.len() as u64)
                },
            )
            .inspect_err(|_| cancelled.store(true, Ordering::Relaxed))?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    #[test]
    fn concurrent_read_batch_validates_every_file() {
        let root = studiofs_test_support::workspace_target_tempdir("worker-read-batch");
        for index in 0..128 {
            std::fs::write(
                root.path().join(index.to_string()),
                vec![u8::try_from(index).unwrap(); 1024],
            )
            .unwrap();
        }
        assert_eq!(super::read_batch(root.path(), 128, 1024).unwrap(), 131072);
        std::fs::write(root.path().join("2"), vec![0; 1024]).unwrap();
        assert!(super::read_batch(root.path(), 128, 1024).is_err());
    }

    #[test]
    fn failed_reader_cancels_other_readers_before_more_io() {
        let root = studiofs_test_support::workspace_target_tempdir("worker-read-cancel");
        let pool = super::BoundedMountedIoPool::new(1);
        let cancelled = super::AtomicBool::new(false);
        let error = super::read_worker(root.path(), 0..1, 1024, &pool, &cancelled).unwrap_err();
        assert!(
            matches!(error, super::ScenarioError::Io(error) if error.kind() == std::io::ErrorKind::NotFound)
        );
        assert!(
            cancelled.load(super::Ordering::Relaxed),
            "failed reader must signal cancellation"
        );
        assert_eq!(
            super::read_worker(root.path(), 1..2, 1024, &pool, &cancelled).unwrap(),
            0,
            "cancelled reader must skip even a missing file"
        );
    }

    #[test]
    fn publication_rejects_oversized_file_without_retrying() {
        let root = studiofs_test_support::workspace_target_tempdir("worker-publication-size");
        std::fs::File::create(root.path().join("0"))
            .unwrap()
            .set_len(262145)
            .unwrap();
        let error = super::wait_for_publication(root.path(), std::time::Duration::from_secs(1))
            .unwrap_err();
        assert!(
            matches!(&error, super::ScenarioError::Io(error) if error.kind() != std::io::ErrorKind::TimedOut),
            "oversized file must fail instead of polling until timeout: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("file 0") && message.contains("262145") && message.contains("262144"),
            "{message}"
        );
    }

    #[test]
    fn publication_timeout_reports_the_pending_file() {
        let root = studiofs_test_support::workspace_target_tempdir("worker-publication-timeout");
        std::fs::File::create(root.path().join("0"))
            .unwrap()
            .set_len(262144)
            .unwrap();
        std::fs::File::create(root.path().join("1"))
            .unwrap()
            .set_len(123)
            .unwrap();
        let error = super::wait_for_publication(root.path(), std::time::Duration::from_secs(1))
            .unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("file 1") && message.contains("1/128") && message.contains("123"),
            "{message}"
        );
    }

    #[test]
    fn validation_reports_actual_and_expected_byte_counts() {
        use super::Scenario;
        let root = studiofs_test_support::workspace_target_tempdir("worker-byte-counts");
        std::fs::write(
            root.path().join("worker-measurement.json"),
            br#"{"cold_bytes":1,"warm_bytes":2}"#,
        )
        .unwrap();
        let context = super::ScenarioContext {
            run_id: "test".into(),
            seed: 789,
            sandbox: root.path().into(),
            scenario_dir: root.path().into(),
            client_controller_config: None,
        };
        let message = super::WorkerConcurrency
            .validate(&context)
            .unwrap_err()
            .to_string();
        assert!(
            message.contains("33554432")
                && message.contains("cold=1")
                && message.contains("warm=2"),
            "{message}"
        );
    }

    #[test]
    fn read_batch_rejects_file_counts_that_repeat_payload_bytes() {
        let root = studiofs_test_support::workspace_target_tempdir("worker-payload-range");
        let error = super::read_batch(root.path(), 257, 1024).unwrap_err();
        assert!(
            matches!(error, super::ScenarioError::Io(error) if error.kind() == std::io::ErrorKind::InvalidInput)
        );
    }
}

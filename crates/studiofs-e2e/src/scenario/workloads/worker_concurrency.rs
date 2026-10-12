//! Opt-in mounted cold-read worker-policy measurements.

use super::*;

const FILE_COUNT: usize = 128;
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
            return Err(io::Error::other("worker measurement did not validate all bytes").into());
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
            vec![index as u8; FILE_BYTES],
            format!("worker measurement write {index}"),
            IO_TIMEOUT,
        )?;
    }

    // Peer-visible nonempty files require published payloads; stat does not warm their bytes.
    let deadline = Instant::now() + Duration::from_secs(90);
    for index in 0..FILE_COUNT {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "payload publication").into());
            }
            match bounded_mounted_metadata(
                observer.join(index.to_string()),
                format!("worker measurement publication {index}"),
                remaining.min(IO_TIMEOUT),
            ) {
                Ok(metadata) if metadata.len() == FILE_BYTES as u64 => break,
                Ok(_) => {}
                Err(ScenarioError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            thread::sleep(remaining.min(Duration::from_millis(50)));
        }
    }
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

fn read_batch(root: &Path, files: usize, file_bytes: usize) -> Result<u64, ScenarioError> {
    let pool = BoundedMountedIoPool::new(READERS);
    thread::scope(|scope| {
        let workers: Vec<_> = (0..files.min(READERS))
            .map(|reader| {
                let pool = pool.clone();
                thread::Builder::new().spawn_scoped(scope, move || {
                    let mut bytes = 0;
                    for index in (reader..files).step_by(READERS) {
                        let path = root.join(index.to_string());
                        bytes += pool.run(
                            format!("worker measurement read {index}"),
                            IO_TIMEOUT,
                            move || {
                                let data = fs::read(path)?;
                                if data.len() != file_bytes
                                    || data.iter().any(|byte| *byte != index as u8)
                                {
                                    return Err(io::Error::other(
                                        "worker measurement payload mismatch",
                                    ));
                                }
                                Ok(data.len() as u64)
                            },
                        )?;
                    }
                    Ok(bytes)
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

#[cfg(test)]
mod tests {
    #[test]
    fn concurrent_read_batch_validates_every_file() {
        let root = studiofs_test_support::workspace_target_tempdir("worker-read-batch");
        for index in 0..4 {
            std::fs::write(root.path().join(index.to_string()), vec![index as u8; 1024]).unwrap();
        }
        assert_eq!(super::read_batch(root.path(), 4, 1024).unwrap(), 4096);
        std::fs::write(root.path().join("2"), vec![0; 1024]).unwrap();
        assert!(super::read_batch(root.path(), 4, 1024).is_err());
    }
}

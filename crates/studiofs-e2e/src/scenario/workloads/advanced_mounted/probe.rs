//! Split from advanced mounted workload implementation.

use super::*;

pub(in crate::scenario) fn scenario_error_to_io(error: ScenarioError) -> io::Error {
    match error {
        ScenarioError::Io(error) => error,
        error => io::Error::other(error),
    }
}

pub(in crate::scenario) fn wait_for_mounted_payload_readback(
    path: &Path,
    payload: &[u8],
    timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<u8>, ScenarioError> {
    wait_for_mounted_payload_readback_with_pool(
        default_bounded_mounted_io_pool(),
        path,
        payload,
        timeout,
        poll_interval,
        MOUNTED_WRITE_READBACK_TIMEOUT,
    )
}

pub(in crate::scenario) fn wait_for_mounted_payload_readback_with_pool(
    pool: &BoundedMountedIoPool,
    path: &Path,
    payload: &[u8],
    timeout: Duration,
    poll_interval: Duration,
    max_probe_timeout: Duration,
) -> Result<Vec<u8>, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let mut last_readback = None;
    let mut last_not_found = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining <= poll_interval {
            if let Some(readback) = last_readback {
                return Ok(readback);
            }
            if let Some(error) = last_not_found {
                return Err(ScenarioError::Io(error));
            }
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("mounted payload readback did not find {}", path.display()),
            )));
        }
        let Some(probe_timeout) =
            mounted_payload_readback_probe_timeout_with_max(remaining, max_probe_timeout)
        else {
            if let Some(readback) = last_readback {
                return Ok(readback);
            }
            if let Some(error) = last_not_found {
                return Err(ScenarioError::Io(error));
            }
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("mounted payload readback did not find {}", path.display()),
            )));
        };
        match pool.run(format!("readback {}", path.display()), probe_timeout, {
            let path = path.to_path_buf();
            move || fs::read(path)
        }) {
            Ok(readback) if readback == payload && !payload.is_empty() => return Ok(readback),
            Ok(readback) if readback == payload => {
                match pool.run(
                    format!("readback metadata {}", path.display()),
                    probe_timeout,
                    {
                        let path = path.to_path_buf();
                        move || fs::metadata(path)
                    },
                ) {
                    Ok(metadata) if metadata.is_file() => return Ok(readback),
                    Ok(readback_metadata) => {
                        return Err(ScenarioError::Io(io::Error::other(format!(
                            "mounted payload readback expected file at {}, got metadata {:?}",
                            path.display(),
                            readback_metadata.file_type()
                        ))));
                    }
                    Err(ScenarioError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
            Ok(readback) => last_readback = Some(readback),
            Err(ScenarioError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                last_not_found = Some(error);
            }
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            if let Some(readback) = last_readback {
                return Ok(readback);
            }
            if let Some(error) = last_not_found {
                return Err(ScenarioError::Io(error));
            }
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("mounted payload readback did not find {}", path.display()),
            )));
        }
        thread::sleep(poll_interval.max(Duration::from_millis(1)));
    }
}

#[cfg(test)]
pub(in crate::scenario) fn mounted_payload_readback_probe_timeout(
    remaining: Duration,
) -> Option<Duration> {
    mounted_payload_readback_probe_timeout_with_max(remaining, MOUNTED_WRITE_READBACK_TIMEOUT)
}

fn mounted_payload_readback_probe_timeout_with_max(
    remaining: Duration,
    max_probe_timeout: Duration,
) -> Option<Duration> {
    if remaining < Duration::from_millis(1) {
        return None;
    }
    Some(remaining.min(max_probe_timeout))
}

pub(in crate::scenario) fn mounted_write_barrier_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
        options.custom_flags(FILE_FLAG_WRITE_THROUGH);
    }
    options
}

pub(in crate::scenario) fn wait_for_mounted_chaos_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let required_paths = MOUNTED_CHAOS_INITIAL_TREE
        .iter()
        .map(|(path, payload)| {
            let logical_path = path.strip_prefix("chaos/").unwrap_or(path);
            let mounted_path = isolation.path(logical_path);
            let payload = mounted_chaos_payload(
                isolation,
                "initial",
                &mounted_path,
                &String::from_utf8_lossy(payload),
            );
            (mounted_path, payload)
        })
        .collect::<Vec<_>>();
    let required_paths = required_paths
        .iter()
        .map(|(path, payload)| (path.as_str(), payload.as_slice()))
        .collect::<Vec<_>>();
    wait_for_mounted_initial_payloads_until(
        clients,
        &required_paths,
        deadline,
        poll_interval,
        "mounted chaos initial tree",
    )?;
    let remaining = remaining_initial_validation_timeout(deadline, "mounted chaos initial tree")?;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation-initial-chaos"),
        remaining,
        poll_interval,
    )
    .map_err(ScenarioError::from)
}

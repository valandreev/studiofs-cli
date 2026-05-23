//! Validation and assertion helpers for advanced mounted scenarios.

use super::*;

pub(in crate::scenario) fn wait_for_symlink_target_until(
    clients: &[ClientSpec],
    relative_path: &str,
    expected_target: &str,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    loop {
        let mut mismatches = Vec::new();
        for client in clients {
            match fs::read_link(client.mount_point.join(relative_path)) {
                Ok(target) if target == Path::new(expected_target) => {}
                Ok(target) => mismatches.push(format!(
                    "{}:{} expected {}, got {}",
                    client.name,
                    relative_path,
                    expected_target,
                    target.display()
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    mismatches.push(format!("{}:{relative_path} missing", client.name));
                }
                Err(error) => {
                    mismatches.push(format!("{}:{relative_path} ({error})", client.name));
                }
            }
        }
        if mismatches.is_empty() {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "symlink target did not converge for {relative_path}: {mismatches:?}"
            ))));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn wait_for_readonly_attribute_until(
    clients: &[ClientSpec],
    relative_path: &str,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<Vec<String>, ScenarioError> {
    loop {
        let mut observed = Vec::new();
        let mut pending = Vec::new();
        for client in clients {
            match fs::metadata(client.mount_point.join(relative_path)) {
                Ok(metadata) if metadata.permissions().readonly() => {
                    observed.push(client.name.clone());
                }
                Ok(_) => pending.push(format!("{}:{relative_path} not readonly", client.name)),
                Err(error) => pending.push(format!("{}:{relative_path} ({error})", client.name)),
            }
        }
        if observed.len() == clients.len() {
            return Ok(observed);
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "readonly attribute did not converge for {relative_path}: {pending:?}"
            ))));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn wait_for_invalidation_fallback_resume(
    clients: &[ClientSpec],
    target_path: &str,
    final_payload: &[u8],
    delete_path: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    wait_for_mounted_initial_payloads_until(
        clients,
        &[(target_path, final_payload)],
        deadline,
        poll_interval,
        "invalidation dropout fallback payload sync",
    )?;
    wait_for_mounted_paths_absent_until(
        clients,
        &[delete_path],
        deadline,
        poll_interval,
        "invalidation dropout fallback delete sync",
    )
}

pub(in crate::scenario) fn assert_observer_stays_stale_for_window(
    observer: &ClientSpec,
    relative_path: &str,
    final_payload: &[u8],
    stale_window: Duration,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + stale_window;
    loop {
        if observer_sees_payload(observer, relative_path, final_payload)? {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "{} observed final payload for {relative_path} before the {} ms stale window elapsed",
                observer.name,
                stale_window.as_millis()
            ))));
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn assert_observer_keeps_payload_for_window(
    observer: &ClientSpec,
    relative_path: &str,
    expected_payload: &[u8],
    stale_window: Duration,
    poll_interval: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + stale_window;
    loop {
        if !observer_sees_payload(observer, relative_path, expected_payload)? {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "{} stopped seeing stale payload for {relative_path} before the {} ms stale window elapsed",
                observer.name,
                stale_window.as_millis()
            ))));
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn observer_sees_payload(
    observer: &ClientSpec,
    relative_path: &str,
    expected_payload: &[u8],
) -> Result<bool, ScenarioError> {
    let _ = refresh_relative_path_parent_contains_child(&observer.mount_point, relative_path);
    match fs::read(observer.mount_point.join(relative_path)) {
        Ok(payload) => Ok(payload == expected_payload),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

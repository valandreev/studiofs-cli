//! Split from advanced mounted workload implementation.

use super::*;

pub(in crate::scenario) fn run_hotspot_churn_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    run_hotspot_churn_operations_with_timeout(
        context,
        clients,
        isolation,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        initial_convergence_timeout,
        poll_interval,
    )
}

pub(in crate::scenario) fn run_hotspot_churn_operations_with_timeout(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    operation_timeout: Duration,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    if clients.len() < 3 {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: 3,
            actual: clients.len(),
        });
    }

    let started = Instant::now();
    let mut operations = Vec::new();
    let mkdir_client = clients[0].clone();
    run_hotspot_helper_process(
        context,
        operation_timeout,
        &mkdir_client,
        "mkdir",
        &isolation.root,
        None,
        None,
        format!("{} mkdir {}", mkdir_client.name, isolation.root),
    )?;
    wait_for_hotspot_initial_tree(
        context,
        clients,
        &isolation.root,
        initial_convergence_timeout,
        poll_interval,
    )?;
    for round in 0..10 {
        let slot = isolation.path(&format!("slot-{}.txt", round % 6));
        let renamed = hotspot_churn_rename_target(isolation, round);
        let write_path = slot.clone();
        let write_payload = isolation.payload(
            "overwrite",
            &write_path,
            &format!("hotspot round {round}\n"),
        );
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[0],
            "overwrite",
            &slot,
            operation_timeout,
            Some(&write_payload),
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        let append_path = slot.clone();
        let append_payload_bytes =
            isolation.payload("append", &append_path, &format!("append round {round}\n"));
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[1],
            "append",
            &slot,
            operation_timeout,
            Some(&append_payload_bytes),
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        wait_for_client_path_visible_until(
            &clients[0],
            &slot,
            Instant::now() + operation_timeout,
            poll_interval,
            "hotspot rename source",
        )?;
        let rename_target = renamed.clone();
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[0],
            "rename",
            &slot,
            operation_timeout,
            None,
            Some(&rename_target),
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
        let order = operations.len() + 1;
        let operation = timed_hotspot_helper_fs_op(
            context,
            started,
            order,
            &clients[2],
            "readdir",
            &isolation.root,
            operation_timeout,
            None,
            None,
        )?;
        record_sparse_operation(context, &mut operations, operation)?;
    }
    Ok(operations)
}

pub(in crate::scenario) fn hotspot_churn_rename_target(
    isolation: &ScenarioIsolation,
    round: usize,
) -> String {
    isolation.path(&format!("slot-{}-round-{round}-renamed.txt", round % 6))
}

pub(in crate::scenario) fn wait_for_hotspot_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    root: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    wait_for_required_paths_visible_until(
        clients,
        &[root],
        deadline,
        poll_interval,
        "hotspot initial tree",
    )?;
    let remaining = remaining_initial_validation_timeout(deadline, "hotspot initial tree")?;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation-initial-hotspot"),
        remaining,
        poll_interval,
    )
    .map_err(ScenarioError::from)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn timed_hotspot_helper_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    timed_hotspot_helper_fs_op_with_helper(
        context, started, order, client, operation, operation, path, timeout, payload, target,
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn timed_hotspot_helper_fs_op_with_helper(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    helper_operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let label = format!("{} {} {}", client.name, operation, path);
    match run_hotspot_helper_process(
        context,
        timeout,
        client,
        helper_operation,
        path,
        target,
        payload,
        label,
    ) {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error) if hotspot_helper_error_is_fatal(&error) => Err(error),
        Err(error) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Err(error.to_string()),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn timed_hotspot_mount_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
    active_workers: Arc<AtomicUsize>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let _ = active_workers;
    match run_hotspot_helper_process(
        context,
        timeout,
        client,
        operation,
        path,
        target,
        payload,
        format!("{} {} {}", client.name, operation, path),
    ) {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error) if hotspot_helper_error_is_fatal(&error) => Err(error),
        Err(error) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Err(error.to_string()),
        )),
    }
}

pub(in crate::scenario) fn hotspot_helper_error_is_fatal(error: &ScenarioError) -> bool {
    matches!(
        error,
        ScenarioError::NamespaceCollisionOperationTimeout { .. }
            | ScenarioError::NamespaceCollisionUnmountAfterFailure { .. }
    )
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_hotspot_mount_operation_with_timeout(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
    active_workers: Arc<AtomicUsize>,
) -> Result<(), ScenarioError> {
    let payload_path = write_helper_payload(context, client, operation, payload)?;
    let progress_path = helper_progress_path(context, client, operation, path)?;
    let mount_point = client.mount_point.clone();
    let path_owned = path.to_string();
    let target_owned = target.map(str::to_string);
    let operation_owned = operation.to_string();
    let pending_label = format!("{} {} {}", client.name, operation, path);
    let result = run_timed_hotspot_io_with_worker_limit(
        timeout,
        pending_label.clone(),
        move || {
            run_hotspot_op_helper(
                &operation_owned,
                mount_point,
                &path_owned,
                target_owned.as_deref(),
                payload_path.as_deref(),
                Some(&progress_path),
            )
            .map_err(io::Error::other)
        },
        active_workers,
        HOTSPOT_IO_MAX_ACTIVE_WORKERS,
    );
    match result {
        Ok(()) => Ok(()),
        Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) => {
            #[cfg(not(test))]
            let _ = terminate_blocked_hotspot_client_process(client, &pending_label);
            Err(error)
        }
        Err(error) => Err(ScenarioError::Io(io::Error::other(format!(
            "{pending_label}: {error}"
        )))),
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_hotspot_helper_process(
    context: &ScenarioContext,
    timeout: Duration,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    target: Option<&str>,
    payload: Option<&[u8]>,
    pending_label: String,
) -> Result<(), ScenarioError> {
    let payload_path = write_helper_payload(context, client, operation, payload)?;
    let progress_path = helper_progress_path(context, client, operation, path)?;

    #[cfg(test)]
    {
        let _ = timeout;
        let _ = pending_label;
        let result = run_hotspot_op_helper(
            operation,
            client.mount_point.clone(),
            path,
            target,
            payload_path.as_deref(),
            Some(&progress_path),
        );
        result.map_err(|error| {
            ScenarioError::Io(io::Error::other(format!(
                "hotspot helper failed for {pending_label}: {error}"
            )))
        })
    }

    #[cfg(not(test))]
    {
        let exe = prepare_hotspot_helper_exe(context)?;
        let mut command = Command::new(exe);
        command
            .arg("hotspot-op")
            .arg("--operation")
            .arg(operation)
            .arg("--sandbox")
            .arg(&context.sandbox)
            .arg("--mount-point")
            .arg(&client.mount_point)
            .arg("--path")
            .arg(path)
            .arg("--progress")
            .arg(&progress_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(target) = target {
            command.arg("--target").arg(target);
        }
        if let Some(payload_path) = &payload_path {
            command.arg("--payload").arg(payload_path);
        }
        command
            .arg("--timeout-ms")
            .arg(duration_millis_u64(hotspot_helper_internal_timeout(timeout)).to_string());

        ChildJob::prepare_command(&mut command)?;
        let mut child = command.spawn()?;
        let mut stderr_reader = drain_child_stderr(&mut child);
        if let Err(error) = write_hotspot_helper_process_metadata(
            context,
            child.id(),
            client,
            operation,
            path,
            target,
        ) {
            let _ = child.kill();
            let _ = child.wait();
            let _ = collect_child_stderr(&mut stderr_reader);
            return Err(error);
        }
        let job = ChildJob::assign_to_child(&child).map_err(|error| {
            let _ = child.kill();
            let _ = child.wait();
            let _ = collect_child_stderr(&mut stderr_reader);
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!("failed to isolate hotspot helper process: {error}"),
            ))
        })?;

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                if status.success() {
                    let _ = collect_child_stderr(&mut stderr_reader);
                    return Ok(());
                }
                let stderr = collect_child_stderr(&mut stderr_reader);
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                let progress_suffix = progress
                    .as_ref()
                    .map(|progress| format!("; progress={progress}"))
                    .unwrap_or_default();
                if stderr.contains(HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER) {
                    return Err(namespace_collision_operation_timeout(
                        timeout,
                        vec![(1, format!("{pending_label}{progress_suffix}"))],
                    ));
                }
                return Err(ScenarioError::Io(io::Error::other(format!(
                    "hotspot helper failed for {pending_label}: status={status}; stderr={}; progress={}",
                    stderr.trim(),
                    progress.as_deref().unwrap_or("<none>")
                ))));
            }
            if Instant::now() >= deadline {
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                let progress_suffix = progress
                    .as_ref()
                    .map(|progress| format!("; progress={progress}"))
                    .unwrap_or_default();
                eprintln!(
                    "studiofs-e2e: mounted helper operation timed out after {} ms; terminating helper process: {}{}",
                    timeout.as_millis(),
                    pending_label,
                    progress_suffix
                );
                let mut cleanup_error =
                    terminate_blocked_hotspot_client_process(client, &pending_label)
                        .err()
                        .map(|error| error.to_string());
                let _ = job.terminate();
                if should_wait_for_timed_out_hotspot_helper_child() {
                    if !wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE).unwrap_or(false) {
                        let _ = job.force_kill();
                        if let Err(error) =
                            terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                        {
                            cleanup_error.get_or_insert_with(|| error.to_string());
                        }
                        let _ = child.kill();
                        let _ = wait_for_child_exit(&mut child, CHILD_TERMINATION_GRACE);
                    }
                } else {
                    let _ = job.force_kill();
                    if let Err(error) =
                        terminate_blocked_hotspot_helper_process(child.id(), &pending_label)
                    {
                        cleanup_error.get_or_insert_with(|| error.to_string());
                    }
                    let _ = child.kill();
                }
                if let Err(error) = terminate_blocked_hotspot_client_process(client, &pending_label)
                {
                    cleanup_error.get_or_insert_with(|| error.to_string());
                }
                let primary = namespace_collision_operation_timeout(
                    timeout,
                    vec![(1, format!("{pending_label}{progress_suffix}"))],
                );
                if let Some(cleanup) = cleanup_error {
                    return Err(ScenarioError::NamespaceCollisionUnmountAfterFailure {
                        primary: primary.to_string(),
                        cleanup,
                    });
                }
                return Err(primary);
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

pub(in crate::scenario) fn should_wait_for_timed_out_hotspot_helper_child() -> bool {
    cfg!(windows)
}

pub(in crate::scenario) fn hotspot_helper_internal_timeout(parent_timeout: Duration) -> Duration {
    let reserve = CHILD_TERMINATION_GRACE + HOTSPOT_HELPER_CANCEL_GRACE;
    parent_timeout
        .checked_sub(reserve)
        .unwrap_or(parent_timeout)
}

#[cfg(all(windows, not(test)))]
pub(in crate::scenario) fn terminate_blocked_hotspot_helper_process(
    pid: u32,
    pending_label: &str,
) -> io::Result<()> {
    eprintln!(
        "studiofs-e2e: force terminating hotspot helper process {pid} to release blocked I/O: {pending_label}"
    );
    match terminate_windows_pid_tree(pid, "blocked hotspot helper process", pending_label) {
        Ok(()) => Ok(()),
        Err(error) => {
            eprintln!(
                "studiofs-e2e: failed to terminate blocked hotspot helper process {pid}: {error}"
            );
            Err(error)
        }
    }
}

#[cfg(all(not(windows), not(test)))]
pub(in crate::scenario) fn terminate_blocked_hotspot_helper_process(
    _pid: u32,
    _pending_label: &str,
) -> io::Result<()> {
    Ok(())
}

#[cfg(all(windows, not(test)))]
pub(in crate::scenario) fn terminate_blocked_hotspot_client_process(
    client: &ClientSpec,
    pending_label: &str,
) -> io::Result<()> {
    let Some(pid) = client.process_id else {
        return Ok(());
    };
    eprintln!(
        "studiofs-e2e: terminating mounted client process {pid} to release blocked hotspot worker I/O: {pending_label}"
    );
    match terminate_windows_pid_tree(pid, "blocked mounted client process", pending_label) {
        Ok(()) => Ok(()),
        Err(error) => {
            eprintln!(
                "studiofs-e2e: failed to terminate blocked mounted client process {pid}: {error}"
            );
            Err(error)
        }
    }
}

#[cfg(all(not(windows), not(test)))]
pub(in crate::scenario) fn terminate_blocked_hotspot_client_process(
    _client: &ClientSpec,
    _pending_label: &str,
) -> io::Result<()> {
    Ok(())
}

#[cfg(all(windows, not(test)))]
pub(in crate::scenario) fn terminate_windows_pid_tree(
    pid: u32,
    label: &str,
    pending_label: &str,
) -> io::Result<()> {
    capture_process_diagnostics_best_effort(pid, &format!("{label}: {pending_label}"));
    match run_taskkill_with_timeout(pid, Duration::from_secs(5)) {
        Ok(output) if windows_taskkill_status_is_success(output.status.code()) => Ok(()),
        Ok(output) => {
            eprintln!(
                "studiofs-e2e: taskkill for {label} {pid} exited with {}; stdout={}; stderr={}",
                output.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&output.stdout).trim(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
            Err(io::Error::other(format!(
                "taskkill for {label} {pid} exited with {}; {pending_label}",
                output.status.code().unwrap_or(-1)
            )))
        }
        Err(error) => {
            eprintln!("studiofs-e2e: taskkill for {label} {pid} failed: {error}");
            Err(error)
        }
    }
}

#[cfg(not(test))]
pub(in crate::scenario) fn prepare_hotspot_helper_exe(
    context: &ScenarioContext,
) -> io::Result<PathBuf> {
    let source = std::env::current_exe()?;
    let helper_dir = context.scenario_dir.join("hotspot-helper-bin");
    fs::create_dir_all(&helper_dir)?;
    let extension = source
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("exe");
    let helper = helper_dir.join(format!("studiofs-e2e-hotspot-helper.{extension}"));
    if !helper.exists() {
        fs::copy(source, &helper)?;
    }
    Ok(helper)
}

pub(in crate::scenario) fn write_helper_payload(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    payload: Option<&[u8]>,
) -> Result<Option<PathBuf>, ScenarioError> {
    match payload {
        Some(payload) => {
            let payload_dir = context.scenario_dir.join("hotspot-op-payloads");
            fs::create_dir_all(&payload_dir)?;
            let id = HOTSPOT_HELPER_PAYLOAD_ID.fetch_add(1, Ordering::SeqCst);
            let payload_path =
                payload_dir.join(format!("{}-{}-{}.payload", id, client.name, operation));
            fs::write(&payload_path, payload)?;
            Ok(Some(payload_path))
        }
        None => Ok(None),
    }
}

pub(in crate::scenario) fn helper_progress_path(
    context: &ScenarioContext,
    client: &ClientSpec,
    operation: &str,
    path: &str,
) -> Result<PathBuf, ScenarioError> {
    let progress_dir = context.scenario_dir.join("hotspot-helper-progress");
    fs::create_dir_all(&progress_dir)?;
    let id = HOTSPOT_HELPER_PAYLOAD_ID.fetch_add(1, Ordering::SeqCst);
    Ok(progress_dir.join(format!(
        "{id:06}-{}-{}-{}.jsonl",
        client.name,
        sanitize_hotspot_progress_component(operation),
        sanitize_hotspot_progress_component(path)
    )))
}

#[cfg(not(test))]
pub(in crate::scenario) fn write_hotspot_helper_process_metadata(
    context: &ScenarioContext,
    pid: u32,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    target: Option<&str>,
) -> io::Result<()> {
    let process_dir = context.scenario_dir.join("hotspot-helper-processes");
    fs::create_dir_all(&process_dir)?;
    let metadata = json!({
        "process_id": pid,
        "process_name": "studiofs-e2e-hotspot-helper",
        "client": client.name,
        "operation": operation,
        "path": path,
        "target": target,
    });
    fs::write(
        process_dir.join(format!("{pid}.json")),
        serde_json::to_vec_pretty(&metadata).map_err(io::Error::other)?,
    )
}

#[cfg(not(test))]
pub(in crate::scenario) fn read_hotspot_helper_progress_tail(progress: &Path) -> Option<String> {
    let contents = fs::read_to_string(progress).ok()?;
    contents
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .map(str::to_string)
}

pub(in crate::scenario) fn run_timed_hotspot_io_with_worker_limit<T: Send + 'static>(
    timeout: Duration,
    pending_label: String,
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
    active_workers: Arc<AtomicUsize>,
    max_active_workers: usize,
) -> Result<T, ScenarioError> {
    let Some(permit) = HotspotIoWorkerPermit::try_acquire(active_workers, max_active_workers)
    else {
        let pending_label = format!(
            "hotspot worker limit reached; previous timed operation may still be blocked before {pending_label}"
        );
        eprintln!(
            "studiofs-e2e: {pending_label}; refusing to spawn another mounted hotspot worker"
        );
        return Err(namespace_collision_operation_timeout(
            timeout,
            vec![(1, pending_label)],
        ));
    };
    let (tx, rx) = mpsc::channel();
    thread::Builder::new()
        .name("studiofs-e2e-hotspot-io".to_string())
        .spawn(move || {
            let result = f();
            drop(permit);
            let _ = tx.send(result);
        })?;
    match rx.recv_timeout(timeout) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.into()),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            eprintln!(
                "studiofs-e2e: mounted hotspot operation timed out after {} ms; worker thread may remain blocked until process exit: {}",
                timeout.as_millis(),
                pending_label
            );
            Err(namespace_collision_operation_timeout(
                timeout,
                vec![(1, pending_label)],
            ))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(ScenarioError::Io(io::Error::other(
            "hotspot operation worker channel closed before completion",
        ))),
    }
}

pub(in crate::scenario) struct HotspotIoWorkerPermit {
    pub(in crate::scenario) active_workers: Arc<AtomicUsize>,
}

impl HotspotIoWorkerPermit {
    fn try_acquire(active_workers: Arc<AtomicUsize>, max_active_workers: usize) -> Option<Self> {
        let mut current = active_workers.load(Ordering::Acquire);
        loop {
            if current >= max_active_workers {
                return None;
            }
            match active_workers.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Self { active_workers }),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for HotspotIoWorkerPermit {
    fn drop(&mut self) {
        self.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

//! Split from advanced mounted workload implementation.

use super::*;

pub(in crate::scenario) fn run_lww_reconnect_operations(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<(Vec<NamespaceCollisionOperation>, LwwReconnectOracle), ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    let path = isolation.path("winner.txt");
    let client_a_payload = isolation.payload("write_client_a", &path, "version from client-a\n");
    operations.push(required_record(fs_op(
        started,
        1,
        &clients[0],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[0].mount_point.join(&path),
                &client_a_payload,
                "lww reconnect client-a write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        2,
        &clients[2],
        "stale_writer_offline",
        clients[2].name.as_str(),
        controller
            .force_kill(&clients[2].name)
            .map_err(ScenarioError::from),
    )?);
    let client_b_payload = isolation.payload("write_client_b", &path, "version from client-b\n");
    operations.push(required_record(fs_op(
        started,
        3,
        &clients[1],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[1].mount_point.join(&path),
                &client_b_payload,
                "lww reconnect client-b write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    let client_d_payload = isolation.payload("write_client_d", &path, "version from client-d\n");
    operations.push(required_record(fs_op(
        started,
        4,
        &clients[3],
        "write",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[3].mount_point.join(&path),
                &client_d_payload,
                "lww reconnect client-d write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        5,
        &clients[2],
        "stale_writer_reconnect",
        clients[2].name.as_str(),
        controller
            .remount(&clients[2].name)
            .map_err(ScenarioError::from),
    )?);
    let winner = isolation.payload(
        "write_lww_winner",
        &path,
        "explicit lww winner from reconnected client-c\n",
    );
    operations.push(required_record(fs_op(
        started,
        6,
        &clients[2],
        "write_lww_winner",
        &path,
        || {
            write_mounted_file_with_readback(
                clients[2].mount_point.join(&path),
                &winner,
                "lww reconnect winner write",
                false,
            )
            .map_err(scenario_error_to_io)
        },
    ))?);
    operations.push(required_op(
        started,
        7,
        &clients[2],
        "winner_crash_restart",
        clients[2].name.as_str(),
        controller
            .force_kill(&clients[2].name)
            .and_then(|()| controller.remount(&clients[2].name))
            .map_err(ScenarioError::from),
    )?);
    let actual = wait_for_mounted_payload_readback(
        &clients[2].mount_point.join(&path),
        &winner,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
    .map_err(scenario_error_to_io)?;
    let oracle = LwwReconnectOracle {
        expected_payload_sha256: bytes_sha256(&winner),
        actual_payload_sha256: bytes_sha256(&actual),
        matched: actual == winner,
    };
    write_lww_oracle(context, &oracle)?;
    Ok((operations, oracle))
}

pub(in crate::scenario) fn run_open_handle_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    seed_open_handle_initial_tree(context, clients, isolation, OPEN_HANDLE_OPERATION_TIMEOUT)?;
    wait_for_open_handle_initial_tree(
        context,
        clients,
        isolation,
        initial_convergence_timeout,
        poll_interval,
    )?;
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    let rename_dest = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_DEST);
    let append_payload = isolation.payload(
        "append_old_handle",
        &unlink_target,
        "append through old handle\n",
    );
    run_open_handle_helper_process(
        context,
        OPEN_HANDLE_OPERATION_TIMEOUT,
        &clients[0],
        &clients[1],
        &unlink_target,
        &rename_source,
        &rename_dest,
        &append_payload,
    )?;
    operations.push(record_advanced_operation(
        started,
        1,
        &clients[0],
        "open",
        &unlink_target,
        Ok(()),
    ));
    operations.push(record_advanced_operation(
        started,
        2,
        &clients[1],
        "unlink",
        &unlink_target,
        Ok(()),
    ));
    operations.push(record_advanced_operation(
        started,
        3,
        &clients[1],
        "rename",
        &rename_source,
        Ok(()),
    ));
    operations.push(record_advanced_operation(
        started,
        4,
        &clients[0],
        "append_old_handle",
        &unlink_target,
        Ok(()),
    ));
    Ok(operations)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_open_handle_helper_process(
    context: &ScenarioContext,
    timeout: Duration,
    client_a: &ClientSpec,
    client_b: &ClientSpec,
    unlink_path: &str,
    rename_source: &str,
    rename_dest: &str,
    append_payload: &[u8],
) -> Result<(), ScenarioError> {
    let payload_path =
        write_helper_payload(context, client_a, "open-handle", Some(append_payload))?
            .ok_or_else(|| ScenarioError::Io(io::Error::other("missing open handle payload")))?;
    let progress_path = helper_progress_path(context, client_a, "open-handle", unlink_path)?;
    let pending_label = format!("{} open-handle {}", client_a.name, unlink_path);

    #[cfg(test)]
    {
        let _ = timeout;
        run_open_handle_op_helper(
            client_a.mount_point.clone(),
            client_b.mount_point.clone(),
            unlink_path,
            rename_source,
            rename_dest,
            &payload_path,
            Some(&progress_path),
        )
        .map_err(|error| {
            ScenarioError::Io(io::Error::other(format!(
                "open handle helper failed for {pending_label}: {error}"
            )))
        })
    }

    #[cfg(not(test))]
    {
        let exe = prepare_hotspot_helper_exe(context)?;
        let mut command = Command::new(exe);
        command
            .arg("open-handle-op")
            .arg("--sandbox")
            .arg(&context.sandbox)
            .arg("--client-a-mount")
            .arg(&client_a.mount_point)
            .arg("--client-b-mount")
            .arg(&client_b.mount_point)
            .arg("--unlink-path")
            .arg(unlink_path)
            .arg("--rename-source")
            .arg(rename_source)
            .arg("--rename-dest")
            .arg(rename_dest)
            .arg("--append-payload")
            .arg(&payload_path)
            .arg("--progress")
            .arg(&progress_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());

        ChildJob::prepare_command(&mut command)?;
        let mut child = command.spawn()?;
        write_hotspot_helper_process_metadata(
            context,
            child.id(),
            client_a,
            "open-handle",
            unlink_path,
            Some(rename_dest),
        )?;
        let job = ChildJob::assign_to_child(&child).map_err(|error| {
            let _ = child.kill();
            let _ = child.wait();
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!("failed to isolate open handle helper process: {error}"),
            ))
        })?;

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                let stderr = child
                    .stderr
                    .take()
                    .and_then(|mut stderr| {
                        let mut output = String::new();
                        stderr.read_to_string(&mut output).ok().map(|_| output)
                    })
                    .unwrap_or_default();
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                return Err(ScenarioError::Io(io::Error::other(format!(
                    "open handle helper failed for {pending_label}: status={status}; stderr={}; progress={}",
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
                    "studiofs-e2e: open handle helper timed out after {} ms; terminating helper process: {}{}",
                    timeout.as_millis(),
                    pending_label,
                    progress_suffix
                );
                let mut cleanup_error =
                    terminate_blocked_hotspot_client_process(client_a, &pending_label)
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
                if let Err(error) =
                    terminate_blocked_hotspot_client_process(client_a, &pending_label)
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

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_sparse_helper_process(
    context: &ScenarioContext,
    timeout: Duration,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    offset: Option<u64>,
    len: Option<u64>,
    payload: Option<&[u8]>,
) -> Result<(), ScenarioError> {
    let payload_path = write_helper_payload(context, client, operation, payload)?;
    let progress_path = helper_progress_path(context, client, operation, path)?;
    let pending_label = format!("{} {} {}", client.name, operation, path);

    #[cfg(test)]
    {
        let _ = timeout;
        run_sparse_op_helper(
            operation,
            &client.mount_point,
            path,
            offset,
            len,
            payload_path.as_deref(),
            Some(&progress_path),
        )
        .map_err(|error| {
            ScenarioError::Io(io::Error::other(format!(
                "sparse helper failed for {pending_label}: {error}"
            )))
        })
    }

    #[cfg(not(test))]
    {
        let exe = prepare_hotspot_helper_exe(context)?;
        let mut command = Command::new(exe);
        command
            .arg("sparse-op")
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
        if let Some(offset) = offset {
            command.arg("--offset").arg(offset.to_string());
        }
        if let Some(len) = len {
            command.arg("--len").arg(len.to_string());
        }
        if let Some(payload_path) = &payload_path {
            command.arg("--payload").arg(payload_path);
        }
        command
            .arg("--timeout-ms")
            .arg(duration_millis_u64(hotspot_helper_internal_timeout(timeout)).to_string());

        ChildJob::prepare_command(&mut command)?;
        let mut child = command.spawn()?;
        write_hotspot_helper_process_metadata(context, child.id(), client, operation, path, None)?;
        let job = ChildJob::assign_to_child(&child).map_err(|error| {
            let _ = child.kill();
            let _ = child.wait();
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!("failed to isolate sparse helper process: {error}"),
            ))
        })?;

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                let stderr = child
                    .stderr
                    .take()
                    .and_then(|mut stderr| {
                        let mut output = String::new();
                        stderr.read_to_string(&mut output).ok().map(|_| output)
                    })
                    .unwrap_or_default();
                let progress = read_hotspot_helper_progress_tail(&progress_path);
                return Err(ScenarioError::Io(io::Error::other(format!(
                    "sparse helper failed for {pending_label}: status={status}; stderr={}; progress={}",
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
                    "studiofs-e2e: sparse helper timed out after {} ms; terminating helper process: {}{}",
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

pub(in crate::scenario) fn seed_open_handle_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    operation_timeout: Duration,
) -> Result<(), ScenarioError> {
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    let unlink_payload = isolation.payload(
        "initial_unlink_target",
        &unlink_target,
        &String::from_utf8_lossy(OPEN_HANDLE_UNLINK_TARGET_PAYLOAD),
    );
    let rename_payload = isolation.payload(
        "initial_rename_source",
        &rename_source,
        &String::from_utf8_lossy(OPEN_HANDLE_RENAME_SOURCE_PAYLOAD),
    );
    let active_workers = Arc::new(AtomicUsize::new(0));
    run_hotspot_mount_operation_with_timeout(
        context,
        &clients[0],
        "create",
        &unlink_target,
        operation_timeout,
        Some(&unlink_payload),
        None,
        Arc::clone(&active_workers),
    )
    .map_err(|error| ScenarioError::Io(io::Error::other(format!("open handle seed: {error}"))))?;
    run_hotspot_mount_operation_with_timeout(
        context,
        &clients[0],
        "create",
        &rename_source,
        operation_timeout,
        Some(&rename_payload),
        None,
        Arc::clone(&active_workers),
    )
    .map_err(|error| ScenarioError::Io(io::Error::other(format!("open handle seed: {error}"))))?;
    Ok(())
}

pub(in crate::scenario) fn wait_for_open_handle_initial_tree(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, ScenarioError> {
    let deadline = Instant::now() + timeout;
    let started = Instant::now();
    let mut attempts = 0;
    let required_payloads = open_handle_initial_payloads(isolation);
    loop {
        attempts += 1;
        let missing = open_handle_initial_state_missing(context, clients, &required_payloads)?;
        if missing.is_empty() {
            return Ok(open_handle_initial_tree_outcome(clients, started, attempts));
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "open handle initial tree did not become visible before timeout; missing: {}",
                    missing.join(", ")
                ),
            )));
        }
        thread::sleep(poll_interval.min(deadline.saturating_duration_since(now)));
    }
}

pub(in crate::scenario) fn open_handle_initial_payloads(
    isolation: &ScenarioIsolation,
) -> Vec<(String, Vec<u8>)> {
    let unlink_target = open_handle_isolated_path(isolation, OPEN_HANDLE_UNLINK_TARGET);
    let rename_source = open_handle_isolated_path(isolation, OPEN_HANDLE_RENAME_SOURCE);
    vec![
        (
            unlink_target.clone(),
            isolation.payload(
                "initial_unlink_target",
                &unlink_target,
                &String::from_utf8_lossy(OPEN_HANDLE_UNLINK_TARGET_PAYLOAD),
            ),
        ),
        (
            rename_source.clone(),
            isolation.payload(
                "initial_rename_source",
                &rename_source,
                &String::from_utf8_lossy(OPEN_HANDLE_RENAME_SOURCE_PAYLOAD),
            ),
        ),
    ]
}

pub(in crate::scenario) fn open_handle_initial_state_missing(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    required_payloads: &[(String, Vec<u8>)],
) -> Result<Vec<String>, ScenarioError> {
    let mut missing = Vec::new();
    for client in clients {
        for (relative_path, payload) in required_payloads {
            match run_hotspot_helper_process(
                context,
                MOUNTED_VALIDATION_PROBE_TIMEOUT,
                client,
                "read-exact",
                relative_path,
                None,
                Some(payload),
                format!(
                    "{} validate open handle initial {}",
                    client.name, relative_path
                ),
            ) {
                Ok(()) => {}
                Err(error) if hotspot_helper_error_is_fatal(&error) => return Err(error),
                Err(error) => {
                    missing.push(format!("{}:{} ({})", client.name, relative_path, error))
                }
            }
        }
    }
    Ok(missing)
}

pub(in crate::scenario) fn open_handle_initial_tree_outcome(
    clients: &[ClientSpec],
    started: Instant,
    attempts: usize,
) -> ConvergenceOutcome {
    let compared_clients = clients
        .iter()
        .map(|client| client.name.clone())
        .collect::<Vec<_>>();
    ConvergenceOutcome {
        attempts,
        elapsed_ms: started.elapsed().as_millis(),
        snapshots: Vec::new(),
        comparison: TreeComparison {
            converged: true,
            reference_client: compared_clients.first().cloned(),
            compared_clients,
            differences: Vec::new(),
        },
    }
}

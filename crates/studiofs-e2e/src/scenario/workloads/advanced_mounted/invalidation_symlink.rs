//! Split from advanced mounted workload implementation.

use super::*;

pub(in crate::scenario) fn invalidation_dropout_observer_env_overrides() -> Vec<(String, String)> {
    vec![
        (
            "STUDIOFS_E2E_DISABLE_METADATA_BACKGROUND_POLL".to_string(),
            "1".to_string(),
        ),
        (
            "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS".to_string(),
            INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS.to_string(),
        ),
    ]
}

pub(in crate::scenario) fn run_invalidation_dropout_fallback_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, InvalidationDropoutSummary), ScenarioError> {
    let started = Instant::now();
    let writer_a = advanced_client_by_name(clients, "client-a")?;
    let writer_b = advanced_client_by_name(clients, "client-b")?;
    let observer = advanced_client_by_name(clients, INVALIDATION_DROPOUT_OBSERVER_CLIENT)?;
    let writer_clients = clients
        .iter()
        .filter(|client| INVALIDATION_DROPOUT_WRITER_CLIENTS.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut operations = Vec::new();

    let baseline_path = isolation.path(INVALIDATION_DROPOUT_BASELINE_FILE);
    let target_path = isolation.path(INVALIDATION_DROPOUT_TARGET_FILE);
    let delete_path = isolation.path(INVALIDATION_DROPOUT_DELETE_TARGET);
    let baseline_payload = isolation.payload("baseline", &baseline_path, "baseline visible");
    let create_payload = isolation.payload("writer_create", &target_path, "created before dropout");
    let final_payload = isolation.payload("writer_overwrite", &target_path, "fallback final");
    let delete_payload = isolation.payload("writer_delete", &delete_path, "delete before fallback");

    run_hotspot_helper_process(
        context,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        writer_a,
        "create",
        &baseline_path,
        None,
        Some(&baseline_payload),
        format!(
            "{} invalidation dropout baseline {}",
            writer_a.name, baseline_path
        ),
    )?;
    run_hotspot_helper_process(
        context,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        writer_a,
        "create",
        &delete_path,
        None,
        Some(&delete_payload),
        format!(
            "{} invalidation dropout delete baseline {}",
            writer_a.name, delete_path
        ),
    )?;
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&baseline_path, baseline_payload.as_slice()),
            (&delete_path, delete_payload.as_slice()),
        ],
        Instant::now() + timeout,
        poll_interval,
        "invalidation dropout baseline",
    )?;

    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_dropout_start",
        INVALIDATION_DROPOUT_OBSERVER_CLIENT,
        Ok(()),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_a,
        "writer_create",
        &target_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer_a,
            "create",
            &target_path,
            None,
            Some(&create_payload),
            format!(
                "{} invalidation dropout writer create {}",
                writer_a.name, target_path
            ),
        ),
    )?);
    wait_for_mounted_initial_payloads_until(
        &writer_clients,
        &[(&target_path, create_payload.as_slice())],
        Instant::now() + timeout,
        poll_interval,
        "invalidation dropout writer create",
    )?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_b,
        "writer_overwrite",
        &target_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer_b,
            "overwrite-barrier-sync",
            &target_path,
            None,
            Some(&final_payload),
            format!(
                "{} invalidation dropout writer overwrite {}",
                writer_b.name, target_path
            ),
        ),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        writer_a,
        "writer_delete",
        &delete_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer_a,
            "unlink",
            &delete_path,
            None,
            None,
            format!(
                "{} invalidation dropout writer delete {}",
                writer_a.name, delete_path
            ),
        ),
    )?);

    let stale_window_observed = assert_observer_stays_stale_for_window(
        observer,
        &target_path,
        &final_payload,
        Duration::from_millis(INVALIDATION_DROPOUT_STALE_WINDOW_MS),
        poll_interval,
    )
    .is_ok();
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_stale_window",
        &target_path,
        stale_window_observed.then_some(()).ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "observer saw final payload before stale window elapsed",
            ))
        }),
    )?);
    let delete_stale_window_observed = assert_observer_keeps_payload_for_window(
        observer,
        &delete_path,
        &delete_payload,
        Duration::from_millis(INVALIDATION_DROPOUT_STALE_WINDOW_MS),
        poll_interval,
    )
    .is_ok();
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "observer_delete_stale_window",
        &delete_path,
        delete_stale_window_observed.then_some(()).ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "observer lost deleted path before stale window elapsed",
            ))
        }),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        observer,
        "fallback_sync_resume",
        &target_path,
        wait_for_invalidation_fallback_resume(
            clients,
            &target_path,
            final_payload.as_slice(),
            &delete_path,
            timeout,
            poll_interval,
        ),
    )?);

    let summary = InvalidationDropoutSummary {
        observer_client: observer.name.clone(),
        writer_clients: INVALIDATION_DROPOUT_WRITER_CLIENTS
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        notify_suppressed: true,
        fallback_interval_ms: INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS,
        stale_window_ms: INVALIDATION_DROPOUT_STALE_WINDOW_MS,
        stale_window_observed,
        delete_stale_window_observed,
        fallback_resumed: true,
        final_observer_path: target_path,
        final_observer_checksum_sha256: bytes_sha256(&final_payload),
        final_validation_result: "mounted_tree_convergence".to_string(),
    };
    write_invalidation_dropout_diagnostics(context, &summary)?;
    Ok((operations, summary))
}

pub(in crate::scenario) fn advanced_client_by_name<'a>(
    clients: &'a [ClientSpec],
    name: &str,
) -> Result<&'a ClientSpec, ScenarioError> {
    clients
        .iter()
        .find(|client| client.name == name)
        .ok_or_else(|| ScenarioError::Io(io::Error::other(format!("missing client {name}"))))
}

pub(in crate::scenario) fn run_symlink_attribute_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(Vec<NamespaceCollisionOperation>, SymlinkAttributeSummary), ScenarioError> {
    if clients.len() < 3 {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: 3,
            actual: clients.len(),
        });
    }

    let started = Instant::now();
    let writer = &clients[0];
    let renamer = &clients[1];
    let attr_writer = &clients[2];
    let mut operations = Vec::new();

    let target_v1 = isolation.path(SYMLINK_ATTRIBUTE_TARGET_V1);
    let target_v2 = isolation.path(SYMLINK_ATTRIBUTE_TARGET_V2);
    let link = isolation.path(SYMLINK_ATTRIBUTE_LINK);
    let renamed_link = isolation.path(SYMLINK_ATTRIBUTE_RENAMED_LINK);
    let readonly_path = isolation.path(SYMLINK_ATTRIBUTE_READONLY_FILE);
    let nearby_path = isolation.path(SYMLINK_ATTRIBUTE_NEARBY_FILE);

    let target_v1_payload = isolation.payload("target_v1", &target_v1, "symlink target v1");
    let target_v2_payload = isolation.payload("target_v2", &target_v2, "symlink target v2");
    let readonly_payload = isolation.payload("readonly", &readonly_path, "readonly metadata attr");
    let nearby_payload = isolation.payload("nearby", &nearby_path, "write near symlink path");

    operations.push(required_op(
        started,
        operations.len() + 1,
        writer,
        "symlink_target_write",
        &target_v1,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer,
            "create",
            &target_v1,
            None,
            Some(&target_v1_payload),
            format!("{} symlink target v1 {}", writer.name, target_v1),
        ),
    )?);
    wait_for_required_paths_visible_until(
        clients,
        &[&target_v1],
        Instant::now() + timeout,
        poll_interval,
        "symlink attribute initial target",
    )?;

    let link_result = run_hotspot_helper_process(
        context,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        writer,
        "symlink-create",
        &link,
        Some(SYMLINK_ATTRIBUTE_TARGET_V1),
        None,
        format!(
            "{} symlink create {} -> {}",
            writer.name, link, SYMLINK_ATTRIBUTE_TARGET_V1
        ),
    );
    let (symlink_supported, symlink_skip_reason) = match link_result {
        Ok(()) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                writer,
                "symlink_create",
                &format!("{link} -> {}", SYMLINK_ATTRIBUTE_TARGET_V1),
                Ok(()),
            )?);
            wait_for_symlink_target_until(
                clients,
                &link,
                SYMLINK_ATTRIBUTE_TARGET_V1,
                Instant::now() + timeout,
                poll_interval,
            )?;
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_rename",
                &format!("{link} -> {renamed_link}"),
                run_hotspot_helper_process(
                    context,
                    MOUNTED_WRITE_READBACK_TIMEOUT,
                    renamer,
                    "rename",
                    &link,
                    Some(&renamed_link),
                    None,
                    format!(
                        "{} symlink rename {} -> {}",
                        renamer.name, link, renamed_link
                    ),
                ),
            )?);
            wait_for_symlink_target_until(
                clients,
                &renamed_link,
                SYMLINK_ATTRIBUTE_TARGET_V1,
                Instant::now() + timeout,
                poll_interval,
            )?;
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_target_replacement_write",
                &target_v2,
                run_hotspot_helper_process(
                    context,
                    MOUNTED_WRITE_READBACK_TIMEOUT,
                    renamer,
                    "create",
                    &target_v2,
                    None,
                    Some(&target_v2_payload),
                    format!("{} symlink target v2 {}", renamer.name, target_v2),
                ),
            )?);
            operations.push(required_op(
                started,
                operations.len() + 1,
                renamer,
                "symlink_target_update",
                &format!("{renamed_link} -> {}", SYMLINK_ATTRIBUTE_TARGET_V2),
                run_hotspot_helper_process(
                    context,
                    MOUNTED_WRITE_READBACK_TIMEOUT,
                    renamer,
                    "symlink-replace",
                    &renamed_link,
                    Some(SYMLINK_ATTRIBUTE_TARGET_V2),
                    None,
                    format!(
                        "{} symlink target update {} -> {}",
                        renamer.name, renamed_link, SYMLINK_ATTRIBUTE_TARGET_V2
                    ),
                ),
            )?);
            wait_for_symlink_target_until(
                clients,
                &renamed_link,
                SYMLINK_ATTRIBUTE_TARGET_V2,
                Instant::now() + timeout,
                poll_interval,
            )?;
            (true, None)
        }
        Err(error) if hotspot_helper_error_is_supported_symlink_skip(&error) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                writer,
                "symlink_create_skip",
                &link,
                Ok(()),
            )?);
            (false, Some(error.to_string()))
        }
        Err(error) => return Err(error),
    };

    operations.push(required_op(
        started,
        operations.len() + 1,
        attr_writer,
        "readonly_attribute_target_write",
        &readonly_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            attr_writer,
            "create",
            &readonly_path,
            None,
            Some(&readonly_payload),
            format!(
                "{} readonly attribute target {}",
                attr_writer.name, readonly_path
            ),
        ),
    )?);
    let readonly_result = run_hotspot_helper_process(
        context,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        attr_writer,
        "set-readonly",
        &readonly_path,
        None,
        None,
        format!(
            "{} readonly attribute set {}",
            attr_writer.name, readonly_path
        ),
    );
    let (readonly_attribute_supported, readonly_attribute_skip_reason) = match readonly_result {
        Ok(()) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                attr_writer,
                "readonly_attribute_set",
                &readonly_path,
                Ok(()),
            )?);
            (true, None)
        }
        Err(error) if hotspot_helper_error_is_supported_readonly_skip(&error) => {
            operations.push(required_op(
                started,
                operations.len() + 1,
                attr_writer,
                "readonly_attribute_skip",
                &readonly_path,
                Ok(()),
            )?);
            (false, Some(error.to_string()))
        }
        Err(error) => return Err(error),
    };

    operations.push(required_op(
        started,
        operations.len() + 1,
        writer,
        "near_symlink_write",
        &nearby_path,
        run_hotspot_helper_process(
            context,
            MOUNTED_WRITE_READBACK_TIMEOUT,
            writer,
            "create",
            &nearby_path,
            None,
            Some(&nearby_payload),
            format!("{} near symlink write {}", writer.name, nearby_path),
        ),
    )?);

    let readonly_observed_clients = if readonly_attribute_supported {
        wait_for_readonly_attribute_until(
            clients,
            &readonly_path,
            Instant::now() + timeout,
            poll_interval,
        )?
    } else {
        Vec::new()
    };

    Ok((
        operations,
        SymlinkAttributeSummary {
            symlink_supported,
            symlink_skip_reason,
            readonly_attribute_supported,
            readonly_attribute_skip_reason,
            final_symlink_path: renamed_link,
            final_symlink_target: SYMLINK_ATTRIBUTE_TARGET_V2.to_string(),
            readonly_path,
            readonly_observed_clients,
            final_validation_result: "mounted_tree_convergence".to_string(),
        },
    ))
}

pub(in crate::scenario) fn hotspot_helper_error_is_supported_symlink_skip(
    error: &ScenarioError,
) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    hotspot_helper_error_is_supported_readonly_skip(error)
        || message.contains("os error 31")
        || message.contains("device attached to the system is not functioning")
}

pub(in crate::scenario) fn hotspot_helper_error_is_supported_readonly_skip(
    error: &ScenarioError,
) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("permission denied")
        || message.contains("unsupported")
        || message.contains("privilege")
        || message.contains("operation not permitted")
}

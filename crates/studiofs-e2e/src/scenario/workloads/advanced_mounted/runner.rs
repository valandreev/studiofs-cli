//! Split from advanced mounted workload implementation.

use super::*;

const EXPECT_ACTION_PAYLOAD_MSG: &str =
    "create and overwrite operations must have an action payload";

pub(in crate::scenario) fn run_advanced_mounted_with_controller(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    let result = run_advanced_mounted_body(context, kind, controller);
    if let Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) = result {
        if let Err(cleanup) = controller.emergency_kill_all_no_wait("advanced mounted timeout") {
            return Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
                scenario: kind.name().to_string(),
                primary: error.to_string(),
                cleanup: cleanup.to_string(),
            });
        }
        return Err(error);
    }

    let unmount_result = controller.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
            scenario: kind.name().to_string(),
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

pub(in crate::scenario) fn run_advanced_mounted_body(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    if kind == AdvancedMountedKind::ColdObserverLongHistory {
        return run_cold_observer_long_history_body(context, controller);
    }
    if kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup {
        return run_stale_remount_cold_discover_body(context, controller);
    }

    let bootstrap = controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: kind
            .client_names()
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;
    if kind == AdvancedMountedKind::InvalidationDropoutFallback {
        controller.set_client_env_overrides(
            INVALIDATION_DROPOUT_OBSERVER_CLIENT,
            invalidation_dropout_observer_env_overrides(),
        )?;
    }
    controller.mount_all()?;
    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    if clients.len() < kind.client_names().len() {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: kind.client_names().len(),
            actual: clients.len(),
        });
    }

    let isolation = ScenarioIsolation::for_advanced(context, kind, &bootstrap.volume_name);
    let execution =
        execute_advanced_mounted_operations(context, kind, controller, &clients, &isolation)?;
    if kind != AdvancedMountedKind::SparseRange {
        write_namespace_collision_operations(context, &execution.operations)?;
    }
    let summary = finish_advanced_mounted(
        context,
        kind,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations: execution.operations,
            lww_oracle: execution.lww_oracle,
            sparse_ranges: execution.sparse_ranges,
            cold_observer: execution.cold_observer,
            long_history: execution.long_history,
            catchup: execution.catchup,
            invalidation_dropout: execution.invalidation_dropout,
            symlink_attributes: execution.symlink_attributes,
        },
    )?;
    Ok(summary)
}

pub(in crate::scenario) fn write_advanced_mounted_plan_artifacts(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    let started = Instant::now();
    let operations = planned_advanced_mounted_operations(kind, started);
    write_namespace_collision_operations(context, &operations)?;
    let lww_oracle = (kind == AdvancedMountedKind::LwwReconnectChaos).then(|| {
        let checksum = bytes_sha256(b"planned lww reconnect winner\n");
        LwwReconnectOracle {
            expected_payload_sha256: checksum.clone(),
            actual_payload_sha256: checksum,
            matched: true,
        }
    });
    let sparse_ranges =
        (kind == AdvancedMountedKind::SparseRange).then(planned_sparse_range_summary);
    if let Some(oracle) = &lww_oracle {
        write_lww_oracle(context, oracle)?;
    }
    if kind == AdvancedMountedKind::ColdObserverLongHistory {
        write_planned_cold_observer_diagnostics(context)?;
    }
    if kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup {
        write_planned_catchup_diagnostics(context)?;
    }
    if kind == AdvancedMountedKind::InvalidationDropoutFallback {
        write_planned_invalidation_dropout_diagnostics(
            context,
            &planned_invalidation_dropout_summary(),
        )?;
    }
    let compared_clients = kind
        .client_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let summary = AdvancedMountedSummary {
        scenario: kind.name().to_string(),
        bootstrap: None,
        clients: kind.client_names().len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: TreeComparison {
            converged: true,
            reference_client: compared_clients.first().cloned(),
            compared_clients,
            differences: Vec::new(),
        },
        namespace_violations: Vec::new(),
        lww_oracle,
        sparse_ranges,
        cold_observer: (kind == AdvancedMountedKind::ColdObserverLongHistory)
            .then(planned_cold_observer_summary),
        long_history: match kind {
            AdvancedMountedKind::ColdObserverLongHistory => Some(planned_long_history_summary()),
            AdvancedMountedKind::StaleRemountColdDiscoverCatchup => {
                Some(planned_stale_remount_long_history_summary())
            }
            _ => None,
        },
        long_history_validation: match kind {
            AdvancedMountedKind::ColdObserverLongHistory
            | AdvancedMountedKind::StaleRemountColdDiscoverCatchup => {
                Some(LongHistoryValidation::matched())
            }
            _ => None,
        },
        catchup: (kind == AdvancedMountedKind::StaleRemountColdDiscoverCatchup)
            .then(planned_catchup_summary),
        invalidation_dropout: (kind == AdvancedMountedKind::InvalidationDropoutFallback)
            .then(planned_invalidation_dropout_summary),
        symlink_attributes: (kind == AdvancedMountedKind::SymlinkAttribute)
            .then(planned_symlink_attribute_summary),
    };
    write_advanced_mounted_summary(context, kind, &summary)?;
    Ok(summary)
}

pub(in crate::scenario) fn execute_advanced_mounted_operations(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<AdvancedMountedExecution, ScenarioError> {
    match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance
        | AdvancedMountedKind::CrashAfterPartialWriteAcceptance
        | AdvancedMountedKind::CrashAfterFullWriteAcceptance => Ok(AdvancedMountedExecution {
            operations: run_write_acceptance_crash_operations(
                kind, controller, clients, isolation,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::MountedChaos => Ok(AdvancedMountedExecution {
            operations: run_mounted_chaos_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::HotspotChurn => Ok(AdvancedMountedExecution {
            operations: run_hotspot_churn_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::FaultChaos => Ok(AdvancedMountedExecution {
            operations: run_fault_chaos_operations(context, controller, clients, isolation)?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::LwwReconnectChaos => {
            let (operations, oracle) =
                run_lww_reconnect_operations(context, controller, clients, isolation)?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: Some(oracle),
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::OpenHandle => Ok(AdvancedMountedExecution {
            operations: run_open_handle_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        }),
        AdvancedMountedKind::SparseRange => {
            let (operations, summary) = run_sparse_range_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: Some(summary),
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::InvalidationDropoutFallback => {
            let (operations, summary) = run_invalidation_dropout_fallback_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: Some(summary),
                symlink_attributes: None,
            })
        }
        AdvancedMountedKind::SymlinkAttribute => {
            let (operations, summary) = run_symlink_attribute_operations(
                context,
                clients,
                isolation,
                ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
            )?;
            Ok(AdvancedMountedExecution {
                operations,
                lww_oracle: None,
                sparse_ranges: None,
                cold_observer: None,
                long_history: None,
                catchup: None,
                invalidation_dropout: None,
                symlink_attributes: Some(summary),
            })
        }
        AdvancedMountedKind::ColdObserverLongHistory => unreachable!(
            "cold-observer long-history uses delayed discovery and is handled before common bootstrap"
        ),
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup => unreachable!(
            "stale-remount cold-discover catch-up uses delayed remount/discovery and is handled before common bootstrap"
        ),
    }
}

pub(in crate::scenario) fn run_cold_observer_long_history_body(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    controller.create_clients(COLD_OBSERVER_CLIENT_NAMES.iter().copied())?;
    controller.init_first_client()?;
    for client in &COLD_OBSERVER_WARM_CLIENT_NAMES[1..] {
        controller.discover(client)?;
    }
    for client in COLD_OBSERVER_WARM_CLIENT_NAMES {
        controller.mount(client)?;
    }

    let warm_clients = controller
        .clients()
        .into_iter()
        .filter(|client| COLD_OBSERVER_WARM_CLIENT_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let bootstrap = VolumeBootstrapSummary {
        volume_name: warm_clients
            .first()
            .map(|client| client.volume_name.clone())
            .unwrap_or_else(|| "projectfs".to_string()),
        init_client: COLD_OBSERVER_WARM_CLIENT_NAMES[0].to_string(),
        discovered_clients: COLD_OBSERVER_WARM_CLIENT_NAMES[1..]
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        clients: warm_clients
            .iter()
            .map(|client| crate::client::ClientBootstrapRecord {
                name: client.name.clone(),
                local_metadata_db: client.local_metadata_db.clone(),
                state: client.state,
            })
            .collect(),
    };
    write_bootstrap_summary(context, &bootstrap)?;

    let isolation = ScenarioIsolation::for_advanced(
        context,
        AdvancedMountedKind::ColdObserverLongHistory,
        &bootstrap.volume_name,
    );
    let started = Instant::now();
    let (mut operations, mut long_history) =
        run_cold_observer_warm_history(started, context, &warm_clients, &isolation)?;

    controller.discover("client-d")?;
    let cold_client = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-d")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold observer client-d")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_client,
        "cold_discover_after_history",
        "client-d",
        Ok(()),
    )?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_client,
        "cold_mount_after_history",
        "client-d",
        controller.mount("client-d").map_err(ScenarioError::from),
    )?);
    long_history.bounded_operation_count = operations.len();
    write_namespace_collision_operations(context, &operations)?;
    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let cold = clients
        .iter()
        .find(|client| client.name == "client-d")
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold observer client-d")))?;
    let cold_observer = ColdObserverSummary {
        client: cold.name.clone(),
        local_metadata_db: cold.local_metadata_db.clone(),
        cache_dir: cold.cache_dir.clone(),
        discovered_after_history: true,
    };

    finish_advanced_mounted(
        context,
        AdvancedMountedKind::ColdObserverLongHistory,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: Some(cold_observer),
            long_history: Some(long_history),
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
        },
    )
}

pub(in crate::scenario) fn run_stale_remount_cold_discover_body(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    controller.create_clients(COLD_OBSERVER_CLIENT_NAMES.iter().copied())?;
    controller.init_first_client()?;
    for client in &STALE_REMOUNT_INITIAL_CLIENT_NAMES[1..] {
        controller.discover(client)?;
    }
    for client in STALE_REMOUNT_INITIAL_CLIENT_NAMES {
        controller.mount(client)?;
    }

    let initial_clients = controller
        .clients()
        .into_iter()
        .filter(|client| STALE_REMOUNT_INITIAL_CLIENT_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let bootstrap = VolumeBootstrapSummary {
        volume_name: initial_clients
            .first()
            .map(|client| client.volume_name.clone())
            .unwrap_or_else(|| "projectfs".to_string()),
        init_client: "client-a".to_string(),
        discovered_clients: vec!["client-b".to_string(), "client-c".to_string()],
        clients: initial_clients
            .iter()
            .map(|client| crate::client::ClientBootstrapRecord {
                name: client.name.clone(),
                local_metadata_db: client.local_metadata_db.clone(),
                state: client.state,
            })
            .collect(),
    };
    write_bootstrap_summary(context, &bootstrap)?;

    let isolation = ScenarioIsolation::for_advanced(
        context,
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
        &bootstrap.volume_name,
    );
    let started = Instant::now();
    let mut operations = Vec::new();
    let stale_client = initial_clients
        .iter()
        .find(|client| client.name == "client-c")
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing stale remount client-c")))?;
    let baseline_path = isolation.path("baseline.txt");
    let baseline_payload = isolation.payload("initial_baseline", &baseline_path, "baseline");
    operations.push(timed_cold_observer_helper_fs_op(
        context,
        started,
        1,
        stale_client,
        "initial_baseline",
        "create",
        &baseline_path,
        &baseline_path,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&baseline_payload),
        None,
    )?);
    write_namespace_collision_operations(context, &operations)?;
    wait_for_mounted_initial_payloads_until(
        &initial_clients,
        &[(&baseline_path, baseline_payload.as_slice())],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "stale remount initial baseline",
    )?;
    operations.push(required_op(
        started,
        2,
        stale_client,
        "stale_client_unmount",
        "client-c",
        controller.unmount("client-c").map_err(ScenarioError::from),
    )?);
    write_namespace_collision_operations(context, &operations)?;

    let warm_clients = controller
        .clients()
        .into_iter()
        .filter(|client| STALE_REMOUNT_WARM_WRITER_NAMES.contains(&client.name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let (mut warm_history, long_history) = run_stale_remount_warm_history(
        context,
        started,
        &warm_clients,
        &isolation,
        operations.len() + 1,
        &operations,
    )?;
    operations.append(&mut warm_history);
    write_namespace_collision_operations(context, &operations)?;

    let stale_after_history = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-c")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing stale remount client-c")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &stale_after_history,
        "stale_remount_after_history",
        "client-c",
        controller.remount("client-c").map_err(ScenarioError::from),
    )?);
    controller.discover("client-d")?;
    let cold_after_discover = controller
        .clients()
        .into_iter()
        .find(|client| client.name == "client-d")
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing cold discover client-d")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_after_discover,
        "cold_discover_after_history",
        "client-d",
        controller.mount("client-d").map_err(ScenarioError::from),
    )?);
    write_namespace_collision_operations(context, &operations)?;

    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let catchup = CatchupSummary {
        warm_writers: STALE_REMOUNT_WARM_WRITER_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        stale_remount_client: "client-c".to_string(),
        cold_discover_client: "client-d".to_string(),
        stale_remount_after_history: true,
        cold_discover_after_history: true,
        final_validation_result: "mounted_tree_convergence".to_string(),
    };
    write_catchup_diagnostics(context, &catchup)?;
    finish_advanced_mounted(
        context,
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: Some(long_history),
            catchup: Some(catchup),
            invalidation_dropout: None,
            symlink_attributes: None,
        },
    )
}

pub(in crate::scenario) fn run_stale_remount_warm_history(
    context: &ScenarioContext,
    started: Instant,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    first_order: usize,
    previously_recorded: &[NamespaceCollisionOperation],
) -> Result<(Vec<NamespaceCollisionOperation>, LongHistorySummary), ScenarioError> {
    let mut operations = Vec::new();
    let mut persisted_operations = previously_recorded.to_vec();
    let delete_target = isolation.path(COLD_OBSERVER_DELETE_TARGET);
    let rename_source = isolation.path(COLD_OBSERVER_RENAME_SOURCE);
    let rename_dest = isolation.path(COLD_OBSERVER_RENAME_DEST);
    let overwrite_target = isolation.path(COLD_OBSERVER_OVERWRITE_TARGET);
    let append_target = isolation.path(COLD_OBSERVER_APPEND_TARGET);
    let nested_file = isolation.path(COLD_OBSERVER_NESTED_FILE);
    let payload = |label: &str, path: &str| isolation.payload(label, path, label);
    let rename_payload = payload("rename_source", &rename_source);
    let append_payload_bytes = payload("append", &append_target);
    let nested_payload = payload("nested_child", &nested_file);

    for (offset, (client_index, operation, path, log_path, action)) in [
        (
            0,
            "create",
            delete_target.clone(),
            delete_target.clone(),
            Some(payload("delete_target", &delete_target)),
        ),
        (
            0,
            "delete",
            delete_target.clone(),
            delete_target.clone(),
            None,
        ),
        (
            0,
            "create",
            rename_source.clone(),
            rename_source.clone(),
            Some(rename_payload.clone()),
        ),
        (
            1,
            "rename",
            rename_dest.clone(),
            format!("{rename_source} -> {rename_dest}"),
            None,
        ),
        (
            0,
            "overwrite",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v2", &overwrite_target)),
        ),
        (
            1,
            "append",
            append_target.clone(),
            append_target.clone(),
            Some(append_payload_bytes.clone()),
        ),
        (
            0,
            "create",
            nested_file.clone(),
            nested_file.clone(),
            Some(nested_payload.clone()),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let client = &clients[client_index];
        let operation_path = if operation == "rename" {
            rename_source.as_str()
        } else {
            path.as_str()
        };
        let target = (operation == "rename").then_some(rename_dest.as_str());
        let helper_operation = match operation {
            "create" => "create-barrier-sync",
            "overwrite" => "overwrite-barrier-sync",
            _ => operation,
        };
        let recorded = timed_cold_observer_helper_fs_op(
            context,
            started,
            first_order + offset,
            client,
            operation,
            helper_operation,
            operation_path,
            &log_path,
            HOTSPOT_CHURN_OPERATION_TIMEOUT,
            action.as_deref(),
            target,
        )?;
        record_sparse_operation(context, &mut persisted_operations, recorded.clone())?;
        operations.push(recorded);
        if matches!(operation, "create" | "overwrite") && path != delete_target {
            wait_for_mounted_initial_payloads_until(
                clients,
                &[(&path, action.as_deref().expect(EXPECT_ACTION_PAYLOAD_MSG))],
                Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                "stale remount warm history",
            )?;
        }
    }

    let final_payload = payload("overwrite_v2", &overwrite_target);
    let bounded_operation_count = first_order + operations.len() - 1;
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&rename_dest, rename_payload.as_slice()),
            (&overwrite_target, final_payload.as_slice()),
            (&append_target, append_payload_bytes.as_slice()),
            (&nested_file, nested_payload.as_slice()),
        ],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "stale remount warm history",
    )?;
    Ok((
        operations,
        LongHistorySummary {
            bounded_operation_count,
            expected_present_paths: vec![
                rename_dest,
                overwrite_target.clone(),
                append_target,
                nested_file,
            ],
            expected_absent_paths: vec![delete_target, rename_source],
            final_checksum_sha256: bytes_sha256(&final_payload),
        },
    ))
}

pub(in crate::scenario) fn run_cold_observer_warm_history(
    started: Instant,
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<(Vec<NamespaceCollisionOperation>, LongHistorySummary), ScenarioError> {
    let mut operations = Vec::new();
    let nested_dir = isolation.path("nested");
    let delete_target = isolation.path(COLD_OBSERVER_DELETE_TARGET);
    let rename_source = isolation.path(COLD_OBSERVER_RENAME_SOURCE);
    let rename_dest = isolation.path(COLD_OBSERVER_RENAME_DEST);
    let overwrite_target = isolation.path(COLD_OBSERVER_OVERWRITE_TARGET);
    let append_target = isolation.path(COLD_OBSERVER_APPEND_TARGET);
    let nested_file = isolation.path(COLD_OBSERVER_NESTED_FILE);

    let payload = |label: &str, path: &str| isolation.payload(label, path, label);
    let nested_payload = payload("nested_child", &nested_file);
    for (order, (client_index, operation, path, log_path, action)) in (1..).zip([
        (0, "mkdir", nested_dir.clone(), nested_dir.clone(), None),
        (
            0,
            "create",
            delete_target.clone(),
            delete_target.clone(),
            Some(payload("delete_target", &delete_target)),
        ),
        (
            1,
            "delete",
            delete_target.clone(),
            delete_target.clone(),
            None,
        ),
        (
            0,
            "create",
            rename_source.clone(),
            rename_source.clone(),
            Some(payload("rename_source", &rename_source)),
        ),
        (
            1,
            "rename",
            rename_dest.clone(),
            format!("{rename_source} -> {rename_dest}"),
            None,
        ),
        (
            2,
            "create",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v1", &overwrite_target)),
        ),
        (
            0,
            "overwrite",
            overwrite_target.clone(),
            overwrite_target.clone(),
            Some(payload("overwrite_v2", &overwrite_target)),
        ),
        (
            1,
            "append",
            append_target.clone(),
            append_target.clone(),
            Some(payload("append", &append_target)),
        ),
        (
            2,
            "create",
            nested_file.clone(),
            nested_file.clone(),
            Some(nested_payload.clone()),
        ),
    ]) {
        let client = &clients[client_index];
        let operation_path = if operation == "rename" {
            rename_source.as_str()
        } else {
            path.as_str()
        };
        let target = (operation == "rename").then_some(rename_dest.as_str());
        let helper_operation = match operation {
            "create" => "create-barrier-sync",
            "overwrite" => "overwrite-barrier-sync",
            _ => operation,
        };
        let operation_record = timed_cold_observer_helper_fs_op(
            context,
            started,
            order,
            client,
            operation,
            helper_operation,
            operation_path,
            &log_path,
            HOTSPOT_CHURN_OPERATION_TIMEOUT,
            action.as_deref(),
            target,
        )?;
        operations.push(operation_record);
        match operation {
            "create" if path == delete_target || path == rename_source => {
                wait_for_required_paths_visible_until(
                    clients,
                    &[&path],
                    Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                    ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                    "cold observer warm history",
                )?;
            }
            "create" | "overwrite" if path == overwrite_target => {
                wait_for_mounted_initial_payloads_until(
                    clients,
                    &[(&path, action.as_deref().expect(EXPECT_ACTION_PAYLOAD_MSG))],
                    Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
                    ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
                    "cold observer warm history",
                )?;
            }
            _ => {}
        }
    }

    let final_payload = payload("overwrite_v2", &overwrite_target);
    wait_for_mounted_initial_payloads_until(
        clients,
        &[
            (&overwrite_target, final_payload.as_slice()),
            (&nested_file, nested_payload.as_slice()),
        ],
        Instant::now() + ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT,
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
        "cold observer warm history",
    )?;
    fs::write(
        context.scenario_dir.join("cold-observer-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "cold_observer": "client-d",
            "warm_clients": COLD_OBSERVER_WARM_CLIENT_NAMES,
            "discovered_after_history": true,
        }))?,
    )?;

    let bounded_operation_count = operations.len();
    Ok((
        operations,
        LongHistorySummary {
            bounded_operation_count,
            expected_present_paths: vec![
                rename_dest,
                overwrite_target.clone(),
                append_target,
                nested_file,
            ],
            expected_absent_paths: vec![delete_target, rename_source],
            final_checksum_sha256: bytes_sha256(&final_payload),
        },
    ))
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn timed_cold_observer_helper_fs_op(
    context: &ScenarioContext,
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    helper_operation: &str,
    path: &str,
    log_path: &str,
    timeout: Duration,
    payload: Option<&[u8]>,
    target: Option<&str>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    let label = format!("{} {} {}", client.name, operation, log_path);
    run_hotspot_helper_process(
        context,
        timeout,
        client,
        helper_operation,
        path,
        target,
        payload,
        label,
    )?;
    required_op(started, order, client, operation, log_path, Ok(()))
}

pub(in crate::scenario) fn run_write_acceptance_crash_operations(
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let writer = &clients[0];
    let mut operations = Vec::new();
    let phase = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => "attempt_create_before_crash",
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => "partial_write",
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => "full_write",
        _ => unreachable!("write acceptance crash scenario expected"),
    };
    let file_name = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => "before-write.txt",
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => "partial-write.txt",
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => "full-write.txt",
        _ => unreachable!("write acceptance crash scenario expected"),
    };
    let path = isolation.path(file_name);
    let payload = isolation.payload(
        phase,
        &path,
        &format!("write acceptance crash phase {phase}\n"),
    );
    let expected = expected_write_acceptance_payload(kind, &payload);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        phase,
        &path,
        match expected.as_deref() {
            Some(accepted_payload) => {
                write_acceptance_payload(writer.mount_point.join(&path), accepted_payload)
                    .map_err(|error| error.to_string())
            }
            None => Ok(()),
        },
    ))?);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "force_kill",
        &path,
        controller
            .force_kill(&writer.name)
            .map_err(|error| error.to_string()),
    ))?);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "remount",
        &path,
        controller
            .remount(&writer.name)
            .map_err(|error| error.to_string()),
    ))?);
    let recovered_path = writer.mount_point.join(&path);
    let recovered = bounded_mounted_read(
        recovered_path.clone(),
        format!(
            "write acceptance recovered read {}",
            recovered_path.display()
        ),
        MOUNTED_WRITE_READBACK_TIMEOUT,
    )
    .map_err(scenario_error_to_io);
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        "readback",
        &path,
        match expected.as_deref() {
            Some(expected) => recovered
                .map_err(|error| error.to_string())
                .and_then(|bytes| {
                    if bytes == expected {
                        Ok(())
                    } else {
                        Err(format!(
                            "readback mismatch for {path}: expected checksum {}, got {}",
                            bytes_sha256(expected),
                            bytes_sha256(&bytes)
                        ))
                    }
                }),
            None => match recovered {
                Ok(bytes) => Err(format!(
                    "readback unexpectedly found {path}: {} bytes, checksum {}",
                    bytes.len(),
                    bytes_sha256(&bytes)
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.to_string()),
            },
        },
    ))?);
    operations.sort_by_key(|operation| operation.order);
    Ok(operations)
}

pub(in crate::scenario) fn expected_write_acceptance_payload(
    kind: AdvancedMountedKind,
    payload: &[u8],
) -> Option<Vec<u8>> {
    match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => None,
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => {
            Some(payload[..payload.len() / 2].to_vec())
        }
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => Some(payload.to_vec()),
        _ => unreachable!("write acceptance crash scenario expected"),
    }
}

pub(in crate::scenario) fn write_acceptance_payload(
    path: PathBuf,
    accepted_payload: &[u8],
) -> io::Result<()> {
    let payload = accepted_payload.to_vec();
    run_bounded_mounted_io(
        format!("write acceptance payload {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        move || {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut options = OpenOptions::new();
            options.create(true).truncate(true).write(true);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;

                const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
                options.custom_flags(FILE_FLAG_WRITE_THROUGH);
            }
            let mut file = options.open(path)?;
            if !payload.is_empty() {
                file.write_all(&payload)?;
            }
            file.sync_all()
        },
    )
    .map_err(scenario_error_to_io)
}

pub(in crate::scenario) fn run_mounted_chaos_operations(
    context: &ScenarioContext,
    clients: &[ClientSpec],
    isolation: &ScenarioIsolation,
    initial_convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<Vec<NamespaceCollisionOperation>, ScenarioError> {
    let started = Instant::now();
    let mut operations = Vec::new();
    let active_workers = Arc::new(AtomicUsize::new(0));
    let writer = &clients[0];
    seed_mounted_chaos_initial_tree(writer, isolation)?;
    wait_for_mounted_chaos_initial_tree(
        context,
        clients,
        isolation,
        initial_convergence_timeout,
        poll_interval,
    )?;
    for &(path, _) in MOUNTED_CHAOS_INITIAL_TREE {
        let path = isolation.path(path.strip_prefix("chaos/").unwrap_or(path));
        wait_for_hotspot_helper_stat_until(
            context,
            writer,
            &path,
            Instant::now() + HOTSPOT_CHURN_OPERATION_TIMEOUT,
            poll_interval,
            "mounted chaos initial path before conflicting operations",
            Arc::clone(&active_workers),
        )?;
    }
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        1,
        &clients[0],
        "rename",
        &isolation.path("rename-source.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        Some(&isolation.path("renamed-by-a.txt")),
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        2,
        &clients[1],
        "unlink",
        &isolation.path("unlink-target.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let append_payload = isolation.payload("append", "append-target.txt", "appended by chaos\n");
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        3,
        &clients[2],
        "append",
        &isolation.path("append-target.txt"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&append_payload),
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let overwrite_path = isolation.path("read-target.txt");
    let overwrite_payload = mounted_chaos_payload(
        isolation,
        "overwrite",
        &overwrite_path,
        "new committed payload\n",
    );
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        4,
        &clients[0],
        "overwrite",
        &overwrite_path,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        Some(&overwrite_payload),
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        5,
        &clients[1],
        "mkdir",
        &isolation.path("dir"),
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    let operation = timed_hotspot_mount_fs_op(
        context,
        started,
        6,
        &clients[2],
        "readdir",
        &isolation.root,
        HOTSPOT_CHURN_OPERATION_TIMEOUT,
        None,
        None,
        Arc::clone(&active_workers),
    )?;
    record_sparse_operation(context, &mut operations, operation)?;
    Ok(operations)
}

pub(in crate::scenario) fn seed_mounted_chaos_initial_tree(
    writer: &ClientSpec,
    isolation: &ScenarioIsolation,
) -> Result<(), ScenarioError> {
    for &(path, payload) in MOUNTED_CHAOS_INITIAL_TREE {
        let logical_path = path.strip_prefix("chaos/").unwrap_or(path);
        let mounted_path = isolation.path(logical_path);
        let payload = mounted_chaos_payload(
            isolation,
            "initial",
            &mounted_path,
            &String::from_utf8_lossy(payload),
        );
        write_mounted_initial_file_sync_only(
            writer.mount_point.join(mounted_path),
            &payload,
            "mounted chaos initial tree",
        )?;
    }
    Ok(())
}

pub(in crate::scenario) fn write_mounted_initial_file(
    path: PathBuf,
    payload: &[u8],
    label: &str,
) -> Result<(), ScenarioError> {
    write_mounted_initial_file_sync_only(path.clone(), payload, label)?;
    let readback = wait_for_mounted_payload_readback(
        &path,
        payload,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )?;
    if readback != payload {
        return Err(ScenarioError::Io(io::Error::other(format!(
            "{label} write readback mismatch for {}: expected {} bytes, read {} bytes",
            path.display(),
            payload.len(),
            readback.len()
        ))));
    }
    Ok(())
}

pub(in crate::scenario) fn write_mounted_initial_file_sync_only(
    path: PathBuf,
    payload: &[u8],
    label: &str,
) -> Result<(), ScenarioError> {
    let write_path = path.clone();
    let write_payload = payload.to_vec();
    let write_label = label.to_string();
    run_bounded_mounted_io(
        format!("{label}: write {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
        move || {
            if let Some(parent) = write_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut options = mounted_write_barrier_open_options();
            let mut file = options
                .create_new(true)
                .open(&write_path)
                .map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("{write_label}: open {}: {error}", write_path.display()),
                    )
                })?;
            file.write_all(&write_payload)?;
            file.sync_all()
        },
    )
}

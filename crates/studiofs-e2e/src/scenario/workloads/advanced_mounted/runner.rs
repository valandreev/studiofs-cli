//! Split from advanced mounted workload implementation.

use super::*;
use std::collections::HashSet;

const EXPECT_ACTION_PAYLOAD_MSG: &str =
    "create and overwrite operations must have an action payload";

pub(in crate::scenario) fn run_advanced_mounted_with_controller(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    let result = run_advanced_mounted_body(context, kind, controller);
    if let Err(error @ ScenarioError::NamespaceCollisionOperationTimeout { .. }) = result {
        return match controller.emergency_kill_all_no_wait("advanced mounted timeout") {
            Ok(()) => Err(error),
            Err(cleanup) => Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
                scenario: kind.name().to_string(),
                primary: error.to_string(),
                cleanup: cleanup.to_string(),
            }),
        };
    }

    let cleanup_result = cleanup_advanced_mounted_after_run(context, kind, controller);
    match (result, cleanup_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
            scenario: kind.name().to_string(),
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn cleanup_advanced_mounted_after_run(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<(), ScenarioError> {
    match controller.unmount_all() {
        Ok(()) => release_completed_hotspot_helpers_for_advanced_kind(context, kind),
        Err(unmount) => match controller.emergency_kill_all_no_wait("advanced mounted unmount") {
            Ok(()) => Err(unmount.into()),
            Err(emergency) => Err(ScenarioError::AdvancedMountedUnmountAfterFailure {
                scenario: kind.name().to_string(),
                primary: unmount.to_string(),
                cleanup: format!("emergency teardown failed: {emergency}"),
            }),
        },
    }
}

fn large_history_positive_env_override(
    name: &str,
    fallback: &str,
) -> Result<String, ScenarioError> {
    let value = std::env::var(name).unwrap_or_else(|_| fallback.to_string());
    if value.parse::<u64>().ok().is_none_or(|value| value == 0) {
        return Err(ScenarioError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be a positive integer"),
        )));
    }
    Ok(value)
}

fn large_history_catchup_overrides() -> Result<Vec<(String, String)>, ScenarioError> {
    let page_batches = large_history_positive_env_override(
        LARGE_HISTORY_PAGE_BATCHES_ENV,
        LARGE_HISTORY_CATCHUP_BATCHES_PER_PAGE,
    )?;
    let mut overrides = vec![
        (
            "STUDIOFS_PG_SYNC_MAX_BATCHES_PER_PAGE".to_string(),
            page_batches,
        ),
        (
            "STUDIOFS_PG_SYNC_POLL_SECS".to_string(),
            LARGE_HISTORY_CATCHUP_POLL_SECS.to_string(),
        ),
    ];
    if std::env::var(LARGE_HISTORY_PAGE_BYTES_ENV).is_ok() {
        let page_bytes = large_history_positive_env_override(LARGE_HISTORY_PAGE_BYTES_ENV, "1")?;
        overrides.push((
            "STUDIOFS_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE".to_string(),
            page_bytes,
        ));
    }
    Ok(overrides)
}

pub(in crate::scenario) fn release_completed_hotspot_helpers_for_advanced_kind(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
) -> Result<(), ScenarioError> {
    if kind == AdvancedMountedKind::OpenHandle {
        release_completed_hotspot_helpers(context).map_err(ScenarioError::Io)
    } else {
        Ok(())
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
    if kind == AdvancedMountedKind::LargeHistoryColdBootstrap {
        return run_large_history_cold_bootstrap_body(context, controller);
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
            large_history: None,
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
    if kind == AdvancedMountedKind::LargeHistoryColdBootstrap {
        write_planned_large_history_diagnostics(context)?;
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
        large_history: (kind == AdvancedMountedKind::LargeHistoryColdBootstrap)
            .then(planned_large_history_summary),
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
        AdvancedMountedKind::LargeHistoryColdBootstrap => unreachable!(
            "large-history cold-bootstrap uses delayed discovery and is handled before common bootstrap"
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
            large_history: None,
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
            large_history: None,
        },
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LargeHistoryPageSample {
    first_batch_version: Option<u64>,
    last_batch_version: Option<u64>,
    batch_count: u64,
    event_count: u64,
    response_bytes: u64,
    has_more: bool,
    query_duration_us: Option<u64>,
    apply_duration_us: Option<u64>,
    total_duration_us: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LargeHistoryStatusSample {
    lifecycle: String,
    local_applied_version: Option<u64>,
    startup_target_version: Option<u64>,
    remote_head_version: Option<u64>,
    checkpoint_version: Option<u64>,
    source_connected: bool,
    checkpoint_payload_bytes: Option<u64>,
    checkpoint_build_duration_us: Option<u64>,
    checkpoint_publish_duration_us: Option<u64>,
    checkpoint_total_duration_us: Option<u64>,
    current_page: Option<LargeHistoryPageSample>,
    last_page: Option<LargeHistoryPageSample>,
}

fn parse_status_version(value: &str) -> Option<u64> {
    let value = value.trim();
    (!value.is_empty() && value != "unknown").then(|| value.parse().ok())?
}

fn parse_status_bytes(value: &str) -> Option<u64> {
    let value = value.trim();
    let unit_start = value
        .find(|character: char| character.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let (amount, unit) = value.split_at(unit_start);
    let multiplier = match unit.trim() {
        "B" => 1.0,
        "KiB" => 1024.0,
        "MiB" => 1024.0 * 1024.0,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((amount.trim().parse::<f64>().ok()? * multiplier).round() as u64)
}

fn parse_status_duration_us(value: &str) -> Option<u64> {
    let value = value.trim().strip_suffix("ms")?.trim();
    Some((value.parse::<f64>().ok()? * 1_000.0).round() as u64)
}

fn parse_status_page(value: &str) -> Result<Option<LargeHistoryPageSample>, ScenarioError> {
    let value = value.trim();
    if value == "none" {
        return Ok(None);
    }
    let (range, details) = value.split_once(" (").ok_or_else(|| {
        ScenarioError::Io(io::Error::other(format!("invalid status page: {value}")))
    })?;
    let details = details.strip_suffix(')').ok_or_else(|| {
        ScenarioError::Io(io::Error::other(format!(
            "invalid status page details: {value}"
        )))
    })?;
    let (first, last) = range.split_once("..").ok_or_else(|| {
        ScenarioError::Io(io::Error::other(format!(
            "invalid status page range: {value}"
        )))
    })?;
    let mut batch_count = None;
    let mut event_count = None;
    let mut response_bytes = None;
    let mut has_more = None;
    let mut query_duration_us = None;
    let mut apply_duration_us = None;
    let mut total_duration_us = None;
    for detail in details.split(", ") {
        if detail.ends_with("batch") || detail.ends_with("batches") {
            batch_count = detail
                .split_whitespace()
                .next()
                .and_then(parse_status_version);
        } else if detail.ends_with("event") || detail.ends_with("events") {
            event_count = detail
                .split_whitespace()
                .next()
                .and_then(parse_status_version);
        } else if let Some(value) = detail.strip_prefix("more=") {
            has_more = Some(value == "true");
        } else if let Some(value) = detail.strip_prefix("query=") {
            query_duration_us = parse_status_duration_us(value);
        } else if let Some(value) = detail.strip_prefix("apply=") {
            apply_duration_us = parse_status_duration_us(value);
        } else if let Some(value) = detail.strip_prefix("total=") {
            total_duration_us = parse_status_duration_us(value);
        } else if detail != "oversized" {
            response_bytes = parse_status_bytes(detail);
        }
    }
    Ok(Some(LargeHistoryPageSample {
        first_batch_version: parse_status_version(first),
        last_batch_version: parse_status_version(last),
        batch_count: batch_count.ok_or_else(|| {
            ScenarioError::Io(io::Error::other(format!(
                "missing status page batch count: {value}"
            )))
        })?,
        event_count: event_count.ok_or_else(|| {
            ScenarioError::Io(io::Error::other(format!(
                "missing status page event count: {value}"
            )))
        })?,
        response_bytes: response_bytes.ok_or_else(|| {
            ScenarioError::Io(io::Error::other(format!(
                "missing status page byte count: {value}"
            )))
        })?,
        has_more: has_more.ok_or_else(|| {
            ScenarioError::Io(io::Error::other(format!(
                "missing status page more flag: {value}"
            )))
        })?,
        query_duration_us,
        apply_duration_us,
        total_duration_us,
    }))
}

fn parse_large_history_status(output: &str) -> Result<LargeHistoryStatusSample, ScenarioError> {
    let mut lifecycle = None;
    let mut local_applied_version = None;
    let mut startup_target_version = None;
    let mut remote_head_version = None;
    let mut checkpoint_version = None;
    let mut source_connected = false;
    let mut checkpoint_payload_bytes = None;
    let mut checkpoint_build_duration_us = None;
    let mut checkpoint_publish_duration_us = None;
    let mut checkpoint_total_duration_us = None;
    let mut current_page = None;
    let mut last_page = None;
    for line in output.lines().map(str::trim) {
        if let Some(value) = line.strip_prefix("Lifecycle:") {
            lifecycle = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("Local applied/target:") {
            let (applied, target) = value.trim().split_once('/').ok_or_else(|| {
                ScenarioError::Io(io::Error::other(format!(
                    "invalid status versions: {value}"
                )))
            })?;
            local_applied_version = parse_status_version(applied);
            startup_target_version = parse_status_version(target);
        } else if let Some(value) = line.strip_prefix("Local applied version:") {
            local_applied_version = parse_status_version(value);
        } else if let Some(value) = line.strip_prefix("Remote head:") {
            remote_head_version = parse_status_version(value);
        } else if let Some(value) = line.strip_prefix("Checkpoint version:") {
            checkpoint_version = parse_status_version(value);
        } else if let Some(value) = line.strip_prefix("Source connected:") {
            source_connected = value.trim() == "yes";
        } else if let Some(value) = line.strip_prefix("Checkpoint payload:") {
            checkpoint_payload_bytes = parse_status_bytes(value);
        } else if let Some(value) = line.strip_prefix("Checkpoint timings:") {
            for detail in value.trim().split(", ") {
                if let Some(value) = detail.strip_prefix("build=") {
                    checkpoint_build_duration_us = parse_status_duration_us(value);
                } else if let Some(value) = detail.strip_prefix("publish=") {
                    checkpoint_publish_duration_us = parse_status_duration_us(value);
                } else if let Some(value) = detail.strip_prefix("total=") {
                    checkpoint_total_duration_us = parse_status_duration_us(value);
                }
            }
        } else if let Some(value) = line.strip_prefix("Current page:") {
            current_page = parse_status_page(value)?;
        } else if let Some(value) = line.strip_prefix("Last page:") {
            last_page = parse_status_page(value)?;
        }
    }
    let lifecycle = lifecycle.ok_or_else(|| {
        ScenarioError::Io(io::Error::other("status output did not contain lifecycle"))
    })?;
    Ok(LargeHistoryStatusSample {
        lifecycle,
        local_applied_version,
        startup_target_version,
        remote_head_version,
        checkpoint_version,
        source_connected,
        checkpoint_payload_bytes,
        checkpoint_build_duration_us,
        checkpoint_publish_duration_us,
        checkpoint_total_duration_us,
        current_page,
        last_page,
    })
}

fn update_cold_checkpoint_version(
    checkpoint_version: &mut Option<u64>,
    sample: &LargeHistoryStatusSample,
) {
    *checkpoint_version = (*checkpoint_version).or(sample.checkpoint_version);
}

fn update_checkpoint_performance(
    payload_bytes: &mut Option<u64>,
    build_duration_us: &mut Option<u64>,
    publish_duration_us: &mut Option<u64>,
    total_duration_us: &mut Option<u64>,
    sample: &LargeHistoryStatusSample,
) {
    *payload_bytes = (*payload_bytes).or(sample.checkpoint_payload_bytes);
    *build_duration_us = (*build_duration_us).or(sample.checkpoint_build_duration_us);
    *publish_duration_us = (*publish_duration_us).or(sample.checkpoint_publish_duration_us);
    *total_duration_us = (*total_duration_us).or(sample.checkpoint_total_duration_us);
}

fn append_large_history_status(
    path: &Path,
    client: &str,
    sample: &LargeHistoryStatusSample,
) -> Result<(), ScenarioError> {
    let mut line = serde_json::to_vec(&json!({
        "client": client,
        "sample": sample,
    }))?;
    line.push(b'\n');
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&line)?;
    Ok(())
}

fn update_large_history_page_totals(
    pages: &mut Vec<LargeHistoryPageSample>,
    seen_pages: &mut HashSet<(Option<u64>, Option<u64>)>,
    sample: &LargeHistoryStatusSample,
) {
    for page in [&sample.current_page, &sample.last_page]
        .into_iter()
        .flatten()
    {
        let key = (page.first_batch_version, page.last_batch_version);
        if seen_pages.insert(key) {
            pages.push(page.clone());
        } else if let Some(existing) = pages
            .iter_mut()
            .find(|existing| (existing.first_batch_version, existing.last_batch_version) == key)
            && existing.total_duration_us.unwrap_or_default() == 0
            && page.total_duration_us.unwrap_or_default() > 0
        {
            *existing = page.clone();
        }
    }
}

fn large_history_remote_head_moved(before: Option<u64>, after: Option<u64>) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => after > before,
        (None, Some(_)) => true,
        _ => false,
    }
}

fn large_history_remote_head_caught_up(
    before: Option<u64>,
    previous_remote_head: Option<u64>,
    remote_head: Option<u64>,
    local_applied: Option<u64>,
) -> bool {
    large_history_remote_head_moved(before, remote_head)
        && previous_remote_head == remote_head
        && remote_head
            .zip(local_applied)
            .is_some_and(|(remote_head, local_applied)| local_applied >= remote_head)
}

pub(in crate::scenario) fn run_large_history_cold_bootstrap_body(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    controller.create_clients(LARGE_HISTORY_CLIENT_NAMES.iter().copied())?;
    controller.init_first_client()?;
    let config = context.client_controller_config.as_ref().ok_or_else(|| {
        ScenarioError::Io(io::Error::other(
            "large-history PG fixture requires client controller configuration",
        ))
    })?;
    let volume_name = config.volume_name.clone();
    let isolation = ScenarioIsolation::for_advanced(
        context,
        AdvancedMountedKind::LargeHistoryColdBootstrap,
        &volume_name,
    );
    let started = Instant::now();
    let _fixture =
        seed_large_history_fixture(config, &volume_name, &isolation.root, &context.scenario_dir)?;
    let catchup_overrides = large_history_catchup_overrides()?;
    if std::env::var_os(LARGE_HISTORY_PAGE_BATCHES_ENV).is_some()
        || std::env::var_os(LARGE_HISTORY_PAGE_BYTES_ENV).is_some()
    {
        controller.set_client_env_overrides("client-a", catchup_overrides.clone())?;
    }
    controller.set_client_env_overrides(LARGE_HISTORY_COLD_CLIENT, catchup_overrides)?;
    controller.set_mount_ready_timeout(LARGE_HISTORY_MOUNT_READY_TIMEOUT);
    controller.mount("client-a")?;
    let warm_clients = controller
        .clients()
        .into_iter()
        .filter(|client| client.name == "client-a")
        .cloned()
        .collect::<Vec<_>>();
    let warm_writer = warm_clients
        .first()
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing large-history client-a")))?;
    let bootstrap = VolumeBootstrapSummary {
        volume_name: volume_name.clone(),
        init_client: "client-a".to_string(),
        discovered_clients: Vec::new(),
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

    let mut operations = Vec::new();
    let status_path = context.scenario_dir.join("large-history-status.ndjson");
    fs::write(&status_path, [])?;
    let mut pages = Vec::new();
    let mut seen_pages = HashSet::new();
    let warm_deadline = Instant::now() + LARGE_HISTORY_CONVERGENCE_TIMEOUT;
    let initial_warm_status = loop {
        let sample = parse_large_history_status(&controller.status("client-a")?.stdout)?;
        append_large_history_status(&status_path, "client-a", &sample)?;
        update_large_history_page_totals(&mut pages, &mut seen_pages, &sample);
        let caught_up = sample
            .remote_head_version
            .zip(sample.local_applied_version)
            .is_some_and(|(remote_head, local_applied)| local_applied >= remote_head);
        if sample.lifecycle == "Live" && caught_up {
            break sample;
        }
        if Instant::now() >= warm_deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "large-history warm client did not apply the PostgreSQL fixture",
            )));
        }
        thread::sleep(LARGE_HISTORY_POLL_INTERVAL);
    };
    let initial_remote_head = initial_warm_status.remote_head_version;
    let after_initial_status = initial_warm_status.clone();
    append_large_history_status(&status_path, "client-a", &after_initial_status)?;
    operations.push(record_advanced_operation(
        started,
        1,
        &warm_writer,
        "large_history_pg_fixture_seed",
        &isolation.root,
        Ok(()),
    ));
    let cold_client = controller
        .clients()
        .into_iter()
        .find(|client| client.name == LARGE_HISTORY_COLD_CLIENT)
        .cloned()
        .ok_or_else(|| ScenarioError::Io(io::Error::other("missing large-history client-d")))?;
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_client,
        "cold_discover_after_large_history",
        LARGE_HISTORY_COLD_CLIENT,
        controller
            .discover(LARGE_HISTORY_COLD_CLIENT)
            .map_err(ScenarioError::from),
    )?);
    let mut replay_gate = Some(LargeHistoryReplayGate::acquire(&config.meta_dsn)?);
    operations.push(required_op(
        started,
        operations.len() + 1,
        &cold_client,
        "cold_mount_after_large_history",
        LARGE_HISTORY_COLD_CLIENT,
        controller
            .mount_until_lifecycle_ready(LARGE_HISTORY_COLD_CLIENT)
            .map_err(ScenarioError::from),
    )?);

    let cold_catchup_deadline = Instant::now() + LARGE_HISTORY_CONVERGENCE_TIMEOUT;
    let mut applied_version_progression = Vec::new();
    let mut checkpoint_version = None;
    let mut checkpoint_payload_bytes = None;
    let mut checkpoint_build_duration_us = None;
    let mut checkpoint_publish_duration_us = None;
    let mut checkpoint_total_duration_us = None;
    update_checkpoint_performance(
        &mut checkpoint_payload_bytes,
        &mut checkpoint_build_duration_us,
        &mut checkpoint_publish_duration_us,
        &mut checkpoint_total_duration_us,
        &after_initial_status,
    );
    let mut startup_target_version = None;
    let mut applied_version_restart_index = None;
    let mut restart_cursor_durable = false;
    let mut saw_catching_up = false;
    let mut restarted_during_catchup = false;
    let mut moving_head_observed = false;
    let mut moving_head_fixture_appended = false;
    let clients = controller
        .clients()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let cold_client_cursor_sync_path = isolation.path(&format!(
        "{}/shard-00/file-00000.bin",
        super::large_history_fixture::FIXTURE_FILES_DIRECTORY_NAME
    ));
    let cold_client_metadata_db_parent =
        cold_client.local_metadata_db.parent().ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "cold client metadata database has no parent",
            ))
        })?;
    let cold_client_metadata_db_name = cold_client
        .local_metadata_db
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "cold client metadata database name is not valid UTF-8",
            ))
        })?;
    let mut cold_client_metadata_db_sync_client = cold_client.clone();
    cold_client_metadata_db_sync_client.mount_point = cold_client_metadata_db_parent.to_path_buf();
    let (cursor_sync_client, cursor_sync_operation, cursor_sync_path) = if cfg!(target_os = "macos")
    {
        (
            &cold_client,
            "sync-mounted-existing",
            cold_client_cursor_sync_path,
        )
    } else {
        (
            &cold_client_metadata_db_sync_client,
            "sync-existing",
            cold_client_metadata_db_name.to_string(),
        )
    };
    let cold_client_writer_marker_path = isolation.path("control/cold-client-writer-marker.txt");
    let cold_client_writer_marker_payload = isolation.payload(
        "cold_client_writer",
        &cold_client_writer_marker_path,
        "large-history cold-client allocator write proof",
    );
    let peer_clients = clients
        .iter()
        .filter(|client| client.name != LARGE_HISTORY_COLD_CLIENT)
        .cloned()
        .collect::<Vec<_>>();
    if peer_clients.is_empty() {
        return Err(ScenarioError::Io(io::Error::other(
            "large-history cold writer has no peer mounts to validate replication",
        )));
    }
    let monitor_result = (|| {
        loop {
            let sample =
                parse_large_history_status(&controller.status(LARGE_HISTORY_COLD_CLIENT)?.stdout)?;
            append_large_history_status(&status_path, LARGE_HISTORY_COLD_CLIENT, &sample)?;
            update_cold_checkpoint_version(&mut checkpoint_version, &sample);
            update_checkpoint_performance(
                &mut checkpoint_payload_bytes,
                &mut checkpoint_build_duration_us,
                &mut checkpoint_publish_duration_us,
                &mut checkpoint_total_duration_us,
                &sample,
            );
            startup_target_version = startup_target_version.or(sample.startup_target_version);
            let is_first_post_restart_sample = restarted_during_catchup
                && applied_version_restart_index == Some(applied_version_progression.len());
            if let Some(applied) = sample.local_applied_version {
                applied_version_progression.push(applied);
            }
            if is_first_post_restart_sample
                && (sample.lifecycle != "CatchingUp"
                    || super::large_history_partial_replay_version(
                        checkpoint_version,
                        sample.local_applied_version,
                        startup_target_version,
                    )
                    .is_none())
            {
                return Err(ScenarioError::Io(io::Error::other(
                    "large-history cold client did not resume from a partial cursor",
                )));
            }
            update_large_history_page_totals(&mut pages, &mut seen_pages, &sample);
            saw_catching_up |= sample.lifecycle == "CatchingUp";

            if saw_catching_up && !moving_head_fixture_appended {
                replay_gate
                    .take()
                    .expect("large-history replay gate must be held until catch-up starts")
                    .release()?;
                append_large_history_moving_head_fixture(config, &volume_name)?;
                moving_head_fixture_appended = true;
                operations.push(record_advanced_operation(
                    started,
                    operations.len() + 1,
                    &warm_writer,
                    "large_history_pg_moving_head_append",
                    &isolation.root,
                    Ok(()),
                ));
            }

            if moving_head_fixture_appended
                && !restarted_during_catchup
                && super::large_history_partial_replay_version(
                    checkpoint_version,
                    sample.local_applied_version,
                    startup_target_version,
                )
                .is_some()
            {
                // Stop the next replay page before recording the cursor to sync.
                replay_gate = Some(LargeHistoryReplayGate::acquire(&config.meta_dsn)?);
                let paused_sample = parse_large_history_status(
                    &controller.status(LARGE_HISTORY_COLD_CLIENT)?.stdout,
                )?;
                append_large_history_status(
                    &status_path,
                    LARGE_HISTORY_COLD_CLIENT,
                    &paused_sample,
                )?;
                update_cold_checkpoint_version(&mut checkpoint_version, &paused_sample);
                update_checkpoint_performance(
                    &mut checkpoint_payload_bytes,
                    &mut checkpoint_build_duration_us,
                    &mut checkpoint_publish_duration_us,
                    &mut checkpoint_total_duration_us,
                    &paused_sample,
                );
                startup_target_version =
                    startup_target_version.or(paused_sample.startup_target_version);
                update_large_history_page_totals(&mut pages, &mut seen_pages, &paused_sample);
                if paused_sample.lifecycle != "CatchingUp" {
                    return Err(ScenarioError::Io(io::Error::other(
                        "large-history cold client left CatchingUp before the durable restart",
                    )));
                }
                let paused_cursor = super::large_history_partial_replay_version(
                    checkpoint_version,
                    paused_sample.local_applied_version,
                    startup_target_version,
                )
                .ok_or_else(|| {
                    ScenarioError::Io(io::Error::other(
                        "large-history cold client lost its partial replay cursor before restart",
                    ))
                })?;

                // MacFUSE fsync enters the engine's immediate metadata barrier.
                let cursor_sync = timed_hotspot_helper_fs_op_with_helper(
                    context,
                    started,
                    operations.len() + 1,
                    cursor_sync_client,
                    "cold_client_cursor_sync_barrier",
                    cursor_sync_operation,
                    &cursor_sync_path,
                    LARGE_HISTORY_CONVERGENCE_TIMEOUT,
                    None,
                    None,
                )?;
                let cursor_sync_accepted = cursor_sync.result == "accepted";
                operations.push(cursor_sync);
                if !cursor_sync_accepted {
                    write_namespace_collision_operations(context, &operations)?;
                    return Err(ScenarioError::Io(io::Error::other(
                        "large-history cold-client cursor sync barrier was rejected",
                    )));
                }

                let durable_sample = parse_large_history_status(
                    &controller.status(LARGE_HISTORY_COLD_CLIENT)?.stdout,
                )?;
                append_large_history_status(
                    &status_path,
                    LARGE_HISTORY_COLD_CLIENT,
                    &durable_sample,
                )?;
                update_cold_checkpoint_version(&mut checkpoint_version, &durable_sample);
                update_checkpoint_performance(
                    &mut checkpoint_payload_bytes,
                    &mut checkpoint_build_duration_us,
                    &mut checkpoint_publish_duration_us,
                    &mut checkpoint_total_duration_us,
                    &durable_sample,
                );
                startup_target_version =
                    startup_target_version.or(durable_sample.startup_target_version);
                update_large_history_page_totals(&mut pages, &mut seen_pages, &durable_sample);
                if durable_sample.lifecycle != "CatchingUp" {
                    return Err(ScenarioError::Io(io::Error::other(
                        "large-history cold client left CatchingUp before the durable restart",
                    )));
                }
                let durable_cursor = super::large_history_partial_replay_version(
                    checkpoint_version,
                    durable_sample.local_applied_version,
                    startup_target_version,
                )
                .ok_or_else(|| {
                    ScenarioError::Io(io::Error::other(
                        "large-history cold client lost its partial replay cursor before restart",
                    ))
                })?;
                if durable_cursor != paused_cursor {
                    return Err(ScenarioError::Io(io::Error::other(
                        "large-history cold-client replay cursor changed while the replay gate was held",
                    )));
                }
                applied_version_progression.push(durable_cursor);
                restart_cursor_durable = true;
                // The index points at the first recorded sample after restart.
                applied_version_restart_index = Some(applied_version_progression.len());
                // Let the active PostgreSQL request finish before graceful
                // shutdown drains and closes the remote metadata connection.
                replay_gate
                    .take()
                    .expect("replay gate is held until the durable cursor is recorded")
                    .release()?;
                controller.restart_until_lifecycle_ready(LARGE_HISTORY_COLD_CLIENT)?;
                restarted_during_catchup = true;
                operations.push(record_advanced_operation(
                    started,
                    operations.len() + 1,
                    &cold_client,
                    "cold_restart_during_catchup",
                    LARGE_HISTORY_COLD_CLIENT,
                    Ok(()),
                ));
                continue;
            }

            if sample.lifecycle == "Live" {
                if !restarted_during_catchup {
                    return Err(ScenarioError::Io(io::Error::other(format!(
                        "large-history cold client reached Live before a partial replay restart (checkpoint={checkpoint_version:?}, applied={:?}, target={startup_target_version:?})",
                        sample.local_applied_version,
                    ))));
                }
                break;
            }
            if Instant::now() >= cold_catchup_deadline {
                return Err(ScenarioError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "large-history cold-bootstrap client did not reach Live",
                )));
            }
            thread::sleep(LARGE_HISTORY_POLL_INTERVAL);
        }
        Ok::<(), ScenarioError>(())
    })();
    let replay_gate_result = replay_gate
        .take()
        .map(LargeHistoryReplayGate::release)
        .transpose();
    monitor_result?;
    replay_gate_result?;
    let reached_live = true;
    let moving_writer_completed = moving_head_fixture_appended;
    let moving_head_mutation_count = LARGE_HISTORY_MOVING_HEAD_MUTATION_COUNT;
    let mutation_count = LARGE_HISTORY_INITIAL_MUTATION_COUNT + moving_head_mutation_count + 1;
    operations.push(timed_hotspot_helper_fs_op_with_helper(
        context,
        started,
        operations.len() + 1,
        &cold_client,
        "cold_client_writer_marker",
        "create-barrier-sync",
        &cold_client_writer_marker_path,
        LARGE_HISTORY_CONVERGENCE_TIMEOUT,
        Some(&cold_client_writer_marker_payload),
        None,
    )?);
    wait_for_mounted_initial_payloads_until_with_probe_timeout(
        &peer_clients,
        &[(
            &cold_client_writer_marker_path,
            cold_client_writer_marker_payload.as_slice(),
        )],
        Instant::now() + LARGE_HISTORY_CONVERGENCE_TIMEOUT,
        LARGE_HISTORY_POLL_INTERVAL,
        "large-history cold-client writer marker convergence",
        LARGE_HISTORY_SNAPSHOT_ATTEMPT_TIMEOUT,
    )?;
    let cold_client_write_converged = true;

    let final_marker_path = isolation.path("control/final-read-write-marker.txt");
    let final_marker_payload = isolation.payload(
        "final_read_write",
        &final_marker_path,
        "large-history final read-write convergence",
    );
    // This mounted marker is written after the PG moving-head fixture. Its visibility
    // on the cold client is the ordered publication barrier for the preceding workload.
    operations.push(timed_hotspot_helper_fs_op_with_helper(
        context,
        started,
        operations.len() + 1,
        &clients[0],
        "final_read_write_marker_create",
        "write",
        &final_marker_path,
        LARGE_HISTORY_CONVERGENCE_TIMEOUT,
        Some(&final_marker_payload),
        None,
    )?);
    let final_deadline = Instant::now() + LARGE_HISTORY_CONVERGENCE_TIMEOUT;
    wait_for_mounted_initial_payloads_until_with_probe_timeout(
        &clients,
        &[(&final_marker_path, final_marker_payload.as_slice())],
        final_deadline,
        LARGE_HISTORY_POLL_INTERVAL,
        "large-history final read-write convergence",
        LARGE_HISTORY_SNAPSHOT_ATTEMPT_TIMEOUT,
    )?;
    let final_read_write_converged = true;
    if moving_writer_completed {
        let moving_head_baseline = after_initial_status
            .remote_head_version
            .or(initial_remote_head);
        let mut previous_remote_head = None;
        while Instant::now() < final_deadline {
            let warm_status = parse_large_history_status(&controller.status("client-a")?.stdout)?;
            append_large_history_status(&status_path, "client-a", &warm_status)?;
            let cold_status =
                parse_large_history_status(&controller.status(LARGE_HISTORY_COLD_CLIENT)?.stdout)?;
            append_large_history_status(&status_path, LARGE_HISTORY_COLD_CLIENT, &cold_status)?;
            update_cold_checkpoint_version(&mut checkpoint_version, &cold_status);
            update_checkpoint_performance(
                &mut checkpoint_payload_bytes,
                &mut checkpoint_build_duration_us,
                &mut checkpoint_publish_duration_us,
                &mut checkpoint_total_duration_us,
                &warm_status,
            );
            update_checkpoint_performance(
                &mut checkpoint_payload_bytes,
                &mut checkpoint_build_duration_us,
                &mut checkpoint_publish_duration_us,
                &mut checkpoint_total_duration_us,
                &cold_status,
            );
            if let Some(applied) = cold_status.local_applied_version {
                applied_version_progression.push(applied);
            }
            update_large_history_page_totals(&mut pages, &mut seen_pages, &cold_status);
            moving_head_observed = large_history_remote_head_caught_up(
                moving_head_baseline,
                previous_remote_head,
                warm_status.remote_head_version,
                cold_status.local_applied_version,
            );
            previous_remote_head = warm_status.remote_head_version;
            if moving_head_observed {
                break;
            }
            thread::sleep(LARGE_HISTORY_POLL_INTERVAL);
        }
        if !moving_head_observed {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "large-history cold-bootstrap client did not catch up to moving remote head",
            )));
        }
    }

    write_namespace_collision_operations(context, &operations)?;
    let large_history = LargeHistorySummary {
        mutation_count,
        // The post-writer marker barrier plus stable-head catch-up confirms the expected
        // workload is remotely visible; status does not expose an exact operation counter.
        remote_visible_mutations: if moving_head_observed {
            mutation_count
        } else {
            0
        },
        initial_mutation_count: LARGE_HISTORY_INITIAL_MUTATION_COUNT,
        moving_head_mutation_count,
        cold_client: LARGE_HISTORY_COLD_CLIENT.to_string(),
        checkpoint_version,
        startup_target_version,
        applied_version_progression,
        applied_version_restart_index,
        restart_cursor_durable,
        observed_page_count: pages.len(),
        observed_event_rows: pages.iter().map(|page| page.event_count).sum(),
        observed_event_bytes: pages.iter().map(|page| page.response_bytes).sum(),
        observed_page_query_duration_us: pages
            .iter()
            .filter_map(|page| page.query_duration_us)
            .sum(),
        observed_page_apply_duration_us: pages
            .iter()
            .filter_map(|page| page.apply_duration_us)
            .sum(),
        observed_page_total_duration_us: pages
            .iter()
            .filter_map(|page| page.total_duration_us)
            .sum(),
        checkpoint_payload_bytes,
        checkpoint_build_duration_us,
        checkpoint_publish_duration_us,
        checkpoint_total_duration_us,
        saw_catching_up,
        restarted_during_catchup,
        moving_head_observed,
        reached_live,
        final_read_write_converged,
        cold_client_write_converged,
        final_marker_path,
        wall_duration_ms: elapsed_millis_u64(started),
    };
    finish_advanced_mounted(
        context,
        AdvancedMountedKind::LargeHistoryColdBootstrap,
        Some(&isolation),
        &clients,
        AdvancedMountedFinishInput {
            bootstrap: Some(bootstrap),
            operations,
            lww_oracle: None,
            sparse_ranges: None,
            cold_observer: None,
            long_history: None,
            catchup: None,
            invalidation_dropout: None,
            symlink_attributes: None,
            large_history: Some(large_history),
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
    let accepted_payload = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => None,
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => {
            Some(&payload[..payload.len() / 2])
        }
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => Some(payload.as_slice()),
        _ => unreachable!("write acceptance crash scenario expected"),
    };
    operations.push(required_record(record_advanced_operation(
        started,
        operations.len() + 1,
        writer,
        phase,
        &path,
        match accepted_payload {
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
    if requires_post_crash_readback(kind) {
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
            match recovered {
                Ok(bytes) => Err(format!(
                    "readback unexpectedly found {path}: {} bytes, checksum {}",
                    bytes.len(),
                    bytes_sha256(&bytes)
                )),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.to_string()),
            },
        ))?);
    }
    operations.sort_by_key(|operation| operation.order);
    Ok(operations)
}

pub(in crate::scenario) fn requires_post_crash_readback(kind: AdvancedMountedKind) -> bool {
    kind == AdvancedMountedKind::CrashBeforeWriteAcceptance
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
    let write_payload = payload.to_vec();
    let write_label = label.to_string();
    let io_label = format!("{label}: write {}", path.display());
    run_bounded_mounted_io(io_label, MOUNTED_WRITE_READBACK_TIMEOUT, move || {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut options = mounted_write_barrier_open_options();
        let mut file = options.create_new(true).open(&path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("{write_label}: open {}: {error}", path.display()),
            )
        })?;
        file.write_all(&write_payload)?;
        file.sync_all()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_large_history_status_page_and_versions() {
        let sample = parse_large_history_status(
            "  Lifecycle:              CatchingUp\n\
             Local applied/target:   123/456\n\
             Remote head:            789\n\
             Checkpoint version:     100\n\
             Checkpoint payload:     1.0MiB\n\
             Checkpoint timings:     build=12.345ms, publish=6.789ms, total=19.134ms\n\
             Current page:           123..154 (32 batches, 512 events, 64 KiB, more=true, query=1.250ms, apply=2.500ms, total=3.750ms)\n\
             Last page:              91..122 (32 batches, 480 events, 60 KiB, more=true, query=1.000ms, apply=2.000ms, total=3.000ms)\n\
             Source connected:       yes\n",
        )
        .unwrap();

        assert_eq!(sample.lifecycle, "CatchingUp");
        assert_eq!(sample.local_applied_version, Some(123));
        assert_eq!(sample.startup_target_version, Some(456));
        assert_eq!(sample.remote_head_version, Some(789));
        assert_eq!(sample.checkpoint_version, Some(100));
        assert_eq!(sample.checkpoint_payload_bytes, Some(1024 * 1024));
        assert_eq!(sample.checkpoint_build_duration_us, Some(12_345));
        assert_eq!(sample.checkpoint_publish_duration_us, Some(6_789));
        assert_eq!(sample.checkpoint_total_duration_us, Some(19_134));
        assert!(sample.source_connected);
        let current_page = sample.current_page.unwrap();
        assert_eq!(current_page.event_count, 512);
        assert_eq!(current_page.query_duration_us, Some(1_250));
        assert_eq!(current_page.apply_duration_us, Some(2_500));
        assert_eq!(current_page.total_duration_us, Some(3_750));
        assert_eq!(sample.last_page.unwrap().response_bytes, 60 * 1024);
    }

    #[test]
    fn parses_compact_status_page_byte_units() {
        let page = parse_status_page("52..52 (1 batch, 872 events, 75.8KiB, more=false)")
            .unwrap()
            .unwrap();

        assert_eq!(page.response_bytes, 77_619);
    }

    #[test]
    fn parses_oversized_status_page_without_losing_byte_count() {
        let page =
            parse_status_page("52..52 (1 batch, 872 events, 75.8 KiB, more=false, oversized)")
                .unwrap()
                .unwrap();

        assert_eq!(page.response_bytes, 77_619);
    }

    #[test]
    fn rounds_fractional_status_page_byte_units() {
        let page = parse_status_page("52..52 (1 batch, 872 events, 1.9KiB, more=false)")
            .unwrap()
            .unwrap();

        assert_eq!(page.response_bytes, 1_946);
    }

    #[test]
    fn page_totals_deduplicate_repeated_page_ranges() {
        let sample = parse_large_history_status(
            "Lifecycle: CatchingUp\n\
             Current page: 52..52 (1 batch, 1 event, 1 KiB, more=false, query=0.000ms, apply=0.000ms, total=0.000ms)\n\
             Last page: 52..52 (1 batch, 1 event, 1 KiB, more=false, query=1.000ms, apply=2.000ms, total=3.000ms)\n",
        )
        .unwrap();
        let mut pages = Vec::new();
        let mut seen_pages = std::collections::HashSet::new();

        update_large_history_page_totals(&mut pages, &mut seen_pages, &sample);

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].query_duration_us, Some(1_000));
        assert_eq!(pages[0].apply_duration_us, Some(2_000));
        assert_eq!(pages[0].total_duration_us, Some(3_000));
    }

    #[test]
    fn moving_head_requires_cold_client_to_reach_observed_head() {
        assert!(!large_history_remote_head_caught_up(
            Some(10),
            Some(20),
            Some(20),
            Some(19),
        ));
    }

    #[test]
    fn moving_head_requires_a_stable_final_remote_head() {
        assert!(!large_history_remote_head_caught_up(
            Some(10),
            None,
            Some(20),
            Some(20)
        ));
        assert!(!large_history_remote_head_caught_up(
            Some(10),
            Some(20),
            Some(21),
            Some(21),
        ));
        assert!(large_history_remote_head_caught_up(
            Some(10),
            Some(20),
            Some(20),
            Some(20),
        ));
    }

    #[test]
    fn checkpoint_evidence_comes_only_from_cold_samples() {
        let sample = parse_large_history_status("Lifecycle: Live\n").unwrap();
        let mut checkpoint_version = None;

        update_cold_checkpoint_version(&mut checkpoint_version, &sample);

        assert_eq!(checkpoint_version, None);
    }
}

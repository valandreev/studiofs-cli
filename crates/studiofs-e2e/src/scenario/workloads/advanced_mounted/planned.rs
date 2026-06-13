//! Split from advanced mounted workload implementation.

use super::*;

pub(in crate::scenario) fn planned_cold_observer_summary() -> ColdObserverSummary {
    ColdObserverSummary {
        client: "client-d".to_string(),
        local_metadata_db: PathBuf::from(
            "artifacts/clients/client-d/client-root/meta/projectfs/projectfs.db",
        ),
        cache_dir: PathBuf::from("artifacts/clients/client-d/cache"),
        discovered_after_history: true,
    }
}

pub(in crate::scenario) fn planned_catchup_summary() -> CatchupSummary {
    CatchupSummary {
        warm_writers: STALE_REMOUNT_WARM_WRITER_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        stale_remount_client: "client-c".to_string(),
        cold_discover_client: "client-d".to_string(),
        stale_remount_after_history: true,
        cold_discover_after_history: true,
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

pub(in crate::scenario) fn planned_invalidation_dropout_summary() -> InvalidationDropoutSummary {
    let root = AdvancedMountedKind::InvalidationDropoutFallback.path_root();
    let target_path = format!("{root}/{INVALIDATION_DROPOUT_TARGET_FILE}");
    let final_payload =
        format!("planned invalidation dropout\npath={target_path}\nbody=fallback final\n")
            .into_bytes();
    InvalidationDropoutSummary {
        observer_client: INVALIDATION_DROPOUT_OBSERVER_CLIENT.to_string(),
        writer_clients: INVALIDATION_DROPOUT_WRITER_CLIENTS
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        notify_suppressed: true,
        fallback_interval_ms: INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS,
        stale_window_ms: INVALIDATION_DROPOUT_STALE_WINDOW_MS,
        stale_window_observed: true,
        delete_stale_window_observed: true,
        fallback_resumed: true,
        final_observer_path: target_path,
        final_observer_checksum_sha256: bytes_sha256(&final_payload),
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

pub(in crate::scenario) fn write_planned_cold_observer_diagnostics(
    context: &ScenarioContext,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("cold-observer-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "cold_observer": "client-d",
            "warm_clients": COLD_OBSERVER_WARM_CLIENT_NAMES,
            "discovered_after_history": true,
            "mode": "planned",
        }))?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn write_planned_catchup_diagnostics(
    context: &ScenarioContext,
) -> Result<(), ScenarioError> {
    write_catchup_diagnostics(context, &planned_catchup_summary())
}

pub(in crate::scenario) fn write_planned_invalidation_dropout_diagnostics(
    context: &ScenarioContext,
    summary: &InvalidationDropoutSummary,
) -> Result<(), ScenarioError> {
    write_invalidation_dropout_diagnostics(context, summary)
}

pub(in crate::scenario) fn write_catchup_diagnostics(
    context: &ScenarioContext,
    catchup: &CatchupSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("catchup-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "warm_writers": catchup.warm_writers,
            "stale_remount_client": catchup.stale_remount_client,
            "cold_discover_client": catchup.cold_discover_client,
            "stale_remount_after_history": catchup.stale_remount_after_history,
            "cold_discover_after_history": catchup.cold_discover_after_history,
            "final_validation_result": catchup.final_validation_result,
        }))?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn write_invalidation_dropout_diagnostics(
    context: &ScenarioContext,
    summary: &InvalidationDropoutSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context
            .scenario_dir
            .join("invalidation-fallback-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "observer_client": summary.observer_client,
            "writer_clients": summary.writer_clients,
            "notify_suppressed": summary.notify_suppressed,
            "fallback_interval_ms": summary.fallback_interval_ms,
            "stale_window_ms": summary.stale_window_ms,
            "stale_window_observed": summary.stale_window_observed,
            "delete_stale_window_observed": summary.delete_stale_window_observed,
            "fallback_resumed": summary.fallback_resumed,
            "final_observer_path": summary.final_observer_path,
            "final_observer_checksum_sha256": summary.final_observer_checksum_sha256,
            "final_validation_result": summary.final_validation_result,
        }))?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn planned_long_history_summary() -> LongHistorySummary {
    let overwrite_target = format!(
        "{}/{}",
        AdvancedMountedKind::ColdObserverLongHistory.path_root(),
        COLD_OBSERVER_OVERWRITE_TARGET
    );
    let final_payload = cold_observer_planned_payload("overwrite_v2", &overwrite_target);
    LongHistorySummary {
        bounded_operation_count: planned_advanced_mounted_operations(
            AdvancedMountedKind::ColdObserverLongHistory,
            Instant::now(),
        )
        .len(),
        expected_present_paths: vec![
            "cold-observer/renamed-final.txt".to_string(),
            "cold-observer/overwrite-target.txt".to_string(),
            "cold-observer/append-target.txt".to_string(),
            "cold-observer/nested/child.txt".to_string(),
        ],
        expected_absent_paths: vec![
            "cold-observer/delete-target.txt".to_string(),
            "cold-observer/rename-source.txt".to_string(),
        ],
        final_checksum_sha256: bytes_sha256(&final_payload),
    }
}

pub(in crate::scenario) fn planned_stale_remount_long_history_summary() -> LongHistorySummary {
    let root = AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root();
    let overwrite_target = format!("{root}/{COLD_OBSERVER_OVERWRITE_TARGET}");
    let final_payload = cold_observer_planned_payload("overwrite_v2", &overwrite_target);
    LongHistorySummary {
        bounded_operation_count: planned_advanced_mounted_operations(
            AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
            Instant::now(),
        )
        .len(),
        expected_present_paths: vec![
            format!("{root}/{COLD_OBSERVER_RENAME_DEST}"),
            format!("{root}/{COLD_OBSERVER_OVERWRITE_TARGET}"),
            format!("{root}/{COLD_OBSERVER_APPEND_TARGET}"),
            format!("{root}/{COLD_OBSERVER_NESTED_FILE}"),
        ],
        expected_absent_paths: vec![
            format!("{root}/{COLD_OBSERVER_DELETE_TARGET}"),
            format!("{root}/{COLD_OBSERVER_RENAME_SOURCE}"),
        ],
        final_checksum_sha256: bytes_sha256(&final_payload),
    }
}

pub(in crate::scenario) fn cold_observer_planned_payload(
    label: &str,
    logical_path: &str,
) -> Vec<u8> {
    format!("planned\nlabel={label}\npath={logical_path}\n{label}").into_bytes()
}

pub(in crate::scenario) fn sparse_ranges_match(
    kind: AdvancedMountedKind,
    summary: Option<&SparseRangeSummary>,
) -> bool {
    match (kind, summary) {
        (AdvancedMountedKind::SparseRange, Some(summary)) => {
            let final_logical_size = summary
                .expected_byte_ranges
                .iter()
                .fold(SPARSE_FINAL_LOGICAL_SIZE, |size, range| {
                    size.max(range.offset + range.len as u64)
                });
            summary.logical_size == final_logical_size
                && summary.read_probes.iter().all(|probe| probe.matched)
                && summary.zero_filled_holes.iter().all(|hole| hole.all_zero)
        }
        (AdvancedMountedKind::SparseRange, None) => false,
        (_, _) => true,
    }
}

pub(in crate::scenario) fn fs_op(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    f: impl FnOnce() -> io::Result<()>,
) -> NamespaceCollisionOperation {
    record_advanced_operation(
        started,
        order,
        client,
        operation,
        path,
        f().map_err(|error| error.to_string()),
    )
}

pub(in crate::scenario) fn required_op(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    result: Result<(), ScenarioError>,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    match result {
        Ok(()) => Ok(record_advanced_operation(
            started,
            order,
            client,
            operation,
            path,
            Ok(()),
        )),
        Err(error) => {
            let operation_record = record_advanced_operation(
                started,
                order,
                client,
                operation,
                path,
                Err(error.to_string()),
            );
            Err(ScenarioError::Io(io::Error::other(format!(
                "{} failed for {}: {:?}",
                operation, path, operation_record.error
            ))))
        }
    }
}

pub(in crate::scenario) fn required_record(
    operation: NamespaceCollisionOperation,
) -> Result<NamespaceCollisionOperation, ScenarioError> {
    if let Some(error) = &operation.error {
        Err(ScenarioError::Io(io::Error::other(format!(
            "{} failed for {}: {}",
            operation.operation, operation.path, error
        ))))
    } else {
        Ok(operation)
    }
}

pub(in crate::scenario) fn record_advanced_operation(
    started: Instant,
    order: usize,
    client: &ClientSpec,
    operation: &str,
    path: &str,
    result: Result<(), String>,
) -> NamespaceCollisionOperation {
    match result {
        Ok(()) => NamespaceCollisionOperation {
            order,
            client: client.name.clone(),
            operation: operation.to_string(),
            path: path.to_string(),
            result: "accepted".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: None,
        },
        Err(error) => NamespaceCollisionOperation {
            order,
            client: client.name.clone(),
            operation: operation.to_string(),
            path: path.to_string(),
            result: "rejected".to_string(),
            timestamp_ms: elapsed_millis_u64(started),
            error: Some(error),
        },
    }
}

pub(in crate::scenario) fn write_advanced_mounted_summary(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    summary: &AdvancedMountedSummary,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join(kind.summary_file()),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn write_lww_oracle(
    context: &ScenarioContext,
    oracle: &LwwReconnectOracle,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join("lww-oracle.json"),
        serde_json::to_vec_pretty(oracle)?,
    )?;
    Ok(())
}

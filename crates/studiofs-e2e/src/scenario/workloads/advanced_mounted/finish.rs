//! Split from advanced mounted workload implementation.

use super::*;

pub(in crate::scenario) struct AdvancedMountedFinishInput {
    pub(in crate::scenario) bootstrap: Option<VolumeBootstrapSummary>,
    pub(in crate::scenario) operations: Vec<NamespaceCollisionOperation>,
    pub(in crate::scenario) lww_oracle: Option<LwwReconnectOracle>,
    pub(in crate::scenario) sparse_ranges: Option<SparseRangeSummary>,
    pub(in crate::scenario) cold_observer: Option<ColdObserverSummary>,
    pub(in crate::scenario) long_history: Option<LongHistorySummary>,
    pub(in crate::scenario) catchup: Option<CatchupSummary>,
    pub(in crate::scenario) invalidation_dropout: Option<InvalidationDropoutSummary>,
    pub(in crate::scenario) symlink_attributes: Option<SymlinkAttributeSummary>,
    pub(in crate::scenario) large_history: Option<LargeHistorySummary>,
}

pub(in crate::scenario) fn finish_advanced_mounted(
    context: &ScenarioContext,
    kind: AdvancedMountedKind,
    isolation: Option<&ScenarioIsolation>,
    clients: &[ClientSpec],
    input: AdvancedMountedFinishInput,
) -> Result<AdvancedMountedSummary, ScenarioError> {
    let AdvancedMountedFinishInput {
        bootstrap,
        operations,
        lww_oracle,
        sparse_ranges,
        cold_observer,
        long_history,
        catchup,
        invalidation_dropout,
        symlink_attributes,
        large_history,
    } = input;
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        kind.convergence_timeout(),
        ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL,
    )?;
    let expected_absent_paths = expected_absent_paths_for(kind, isolation);
    let expected_absent_paths = expected_absent_paths
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut namespace_violations = Vec::new();
    for snapshot in &convergence.snapshots {
        namespace_violations.extend(validate_namespace_invariants(
            snapshot,
            &expected_absent_paths,
        ));
    }
    if !namespace_violations.is_empty() {
        return Err(ScenarioError::NamespaceInvariantViolations {
            violations: namespace_violations,
        });
    }
    let summary = AdvancedMountedSummary {
        scenario: kind.name().to_string(),
        bootstrap,
        clients: clients.len(),
        operations,
        operation_log: format!("artifacts/scenarios/{}/operations.ndjson", kind.name()),
        final_validator: convergence.comparison,
        namespace_violations,
        lww_oracle,
        sparse_ranges,
        cold_observer,
        long_history_validation: long_history
            .as_ref()
            .map(|history| validate_long_history_snapshots(&convergence.snapshots, history)),
        long_history,
        catchup,
        invalidation_dropout,
        symlink_attributes,
        large_history,
    };
    write_advanced_mounted_summary(context, kind, &summary)?;
    Ok(summary)
}

pub(in crate::scenario) fn expected_absent_paths_for(
    kind: AdvancedMountedKind,
    isolation: Option<&ScenarioIsolation>,
) -> Vec<String> {
    match (kind, isolation) {
        (AdvancedMountedKind::OpenHandle, Some(isolation)) => {
            vec![open_handle_isolated_path(
                isolation,
                OPEN_HANDLE_UNLINK_TARGET,
            )]
        }
        (AdvancedMountedKind::OpenHandle, None) => vec![OPEN_HANDLE_UNLINK_TARGET.to_string()],
        (AdvancedMountedKind::ColdObserverLongHistory, Some(isolation)) => vec![
            isolation.path(COLD_OBSERVER_DELETE_TARGET),
            isolation.path(COLD_OBSERVER_RENAME_SOURCE),
        ],
        (AdvancedMountedKind::ColdObserverLongHistory, None) => vec![
            format!(
                "{}/{}",
                AdvancedMountedKind::ColdObserverLongHistory.path_root(),
                COLD_OBSERVER_DELETE_TARGET
            ),
            format!(
                "{}/{}",
                AdvancedMountedKind::ColdObserverLongHistory.path_root(),
                COLD_OBSERVER_RENAME_SOURCE
            ),
        ],
        (AdvancedMountedKind::StaleRemountColdDiscoverCatchup, Some(isolation)) => vec![
            isolation.path(COLD_OBSERVER_DELETE_TARGET),
            isolation.path(COLD_OBSERVER_RENAME_SOURCE),
        ],
        (AdvancedMountedKind::StaleRemountColdDiscoverCatchup, None) => vec![
            format!(
                "{}/{}",
                AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root(),
                COLD_OBSERVER_DELETE_TARGET
            ),
            format!(
                "{}/{}",
                AdvancedMountedKind::StaleRemountColdDiscoverCatchup.path_root(),
                COLD_OBSERVER_RENAME_SOURCE
            ),
        ],
        (AdvancedMountedKind::InvalidationDropoutFallback, Some(isolation)) => {
            vec![isolation.path(INVALIDATION_DROPOUT_DELETE_TARGET)]
        }
        (AdvancedMountedKind::InvalidationDropoutFallback, None) => vec![format!(
            "{}/{}",
            AdvancedMountedKind::InvalidationDropoutFallback.path_root(),
            INVALIDATION_DROPOUT_DELETE_TARGET
        )],
        _ => Vec::new(),
    }
}

pub(in crate::scenario) fn validate_long_history_snapshots(
    snapshots: &[crate::mounted_tree::MountedTreeSnapshot],
    history: &LongHistorySummary,
) -> LongHistoryValidation {
    let mut missing_present_paths = Vec::new();
    let mut unexpected_present_paths = Vec::new();
    let mut checksum_mismatches = Vec::new();
    let checksum_path = long_history_checksum_path(history);

    for snapshot in snapshots {
        let entries_by_path: std::collections::HashMap<_, _> = snapshot
            .entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect();
        for expected_path in &history.expected_present_paths {
            let Some(entry) = entries_by_path.get(expected_path.as_str()) else {
                missing_present_paths.push(format!("{}:{expected_path}", snapshot.client));
                continue;
            };
            if checksum_path.as_deref() == Some(expected_path.as_str())
                && entry.checksum_sha256.as_deref() != Some(history.final_checksum_sha256.as_str())
            {
                checksum_mismatches.push(format!(
                    "{}:{expected_path}: expected {}, got {}",
                    snapshot.client,
                    history.final_checksum_sha256,
                    entry.checksum_sha256.as_deref().unwrap_or("<missing>")
                ));
            }
        }
        for absent_path in &history.expected_absent_paths {
            if entries_by_path.contains_key(absent_path.as_str()) {
                unexpected_present_paths.push(format!("{}:{absent_path}", snapshot.client));
            }
        }
    }

    LongHistoryValidation {
        matched: missing_present_paths.is_empty()
            && unexpected_present_paths.is_empty()
            && checksum_mismatches.is_empty(),
        missing_present_paths,
        unexpected_present_paths,
        checksum_mismatches,
    }
}

pub(in crate::scenario) fn long_history_checksum_path(
    history: &LongHistorySummary,
) -> Option<String> {
    history
        .expected_present_paths
        .iter()
        .find(|path| path.ends_with(COLD_OBSERVER_OVERWRITE_TARGET))
        .cloned()
        .or_else(|| history.expected_present_paths.first().cloned())
}

pub(in crate::scenario) fn planned_advanced_mounted_operations(
    kind: AdvancedMountedKind,
    started: Instant,
) -> Vec<NamespaceCollisionOperation> {
    let spec: &[(&str, &str, &str)] = match kind {
        AdvancedMountedKind::CrashBeforeWriteAcceptance => &[(
            "client-a",
            "before_write",
            "crash-before-write-acceptance/before-write.txt",
        )],
        AdvancedMountedKind::CrashAfterPartialWriteAcceptance => &[(
            "client-a",
            "partial_write",
            "crash-after-partial-write-acceptance/partial-write.txt",
        )],
        AdvancedMountedKind::CrashAfterFullWriteAcceptance => &[(
            "client-a",
            "full_write",
            "crash-after-full-write-acceptance/full-write.txt",
        )],
        AdvancedMountedKind::MountedChaos => &[
            ("client-a", "rename", "chaos/rename-source.txt"),
            ("client-b", "unlink", "chaos/unlink-target.txt"),
            ("client-c", "append", "chaos/append-target.txt"),
        ],
        AdvancedMountedKind::HotspotChurn => &[
            ("client-a", "overwrite", "hotspot/slot-0.txt"),
            ("client-b", "append", "hotspot/slot-0.txt"),
            ("client-c", "readdir", "hotspot"),
        ],
        AdvancedMountedKind::FaultChaos => &[
            ("client-a", "stop_postgres", "compose/postgres"),
            (
                "client-a",
                "local_write_during_pg_outage",
                "fault-chaos/postgres-outage.txt",
            ),
            ("client-b", "stop_rustfs", "compose/rustfs"),
            ("client-d", "cold_observer_readdir", "fault-chaos"),
        ],
        AdvancedMountedKind::LwwReconnectChaos => &[
            ("client-c", "stale_writer_offline", "client-c"),
            ("client-b", "write", "lww-chaos/winner.txt"),
            ("client-c", "write_lww_winner", "lww-chaos/winner.txt"),
            ("client-e", "late_observer_read", "lww-chaos/winner.txt"),
        ],
        AdvancedMountedKind::OpenHandle => &[
            ("client-a", "open", "open-handles/unlink-target.txt"),
            ("client-b", "unlink", "open-handles/unlink-target.txt"),
            ("client-b", "rename", "open-handles/rename-source.txt"),
            (
                "client-a",
                "append_old_handle",
                "open-handles/unlink-target.txt",
            ),
        ],
        AdvancedMountedKind::SparseRange => &[
            ("client-a", "write_range", "sparse/shared.bin@0"),
            ("client-a", "write_range", "sparse/shared.bin@1048569"),
            ("client-b", "read_probe", "sparse/shared.bin@65536"),
            ("client-b", "truncate_shrink", "sparse/shared.bin@1048833"),
            ("client-c", "truncate_extend", "sparse/shared.bin@3146061+"),
            ("client-c", "write_range", "sparse/shared.bin@2097147"),
            ("client-a", "read_probe", "sparse/shared.bin@3145900"),
        ],
        AdvancedMountedKind::ColdObserverLongHistory => &[
            ("client-a", "mkdir", "cold-observer/nested"),
            ("client-a", "create", "cold-observer/delete-target.txt"),
            ("client-b", "delete", "cold-observer/delete-target.txt"),
            ("client-a", "create", "cold-observer/rename-source.txt"),
            (
                "client-b",
                "rename",
                "cold-observer/rename-source.txt -> cold-observer/renamed-final.txt",
            ),
            ("client-c", "create", "cold-observer/overwrite-target.txt"),
            (
                "client-a",
                "overwrite",
                "cold-observer/overwrite-target.txt",
            ),
            ("client-b", "append", "cold-observer/append-target.txt"),
            ("client-c", "create", "cold-observer/nested/child.txt"),
            ("client-d", "cold_discover_after_history", "client-d"),
            ("client-d", "cold_mount_after_history", "client-d"),
        ],
        AdvancedMountedKind::StaleRemountColdDiscoverCatchup => &[
            (
                "client-c",
                "initial_baseline",
                "stale-remount-cold-discover/baseline.txt",
            ),
            ("client-c", "stale_client_unmount", "client-c"),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/delete-target.txt",
            ),
            (
                "client-a",
                "delete",
                "stale-remount-cold-discover/delete-target.txt",
            ),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/rename-source.txt",
            ),
            (
                "client-b",
                "rename",
                "stale-remount-cold-discover/rename-source.txt -> stale-remount-cold-discover/renamed-final.txt",
            ),
            (
                "client-a",
                "overwrite",
                "stale-remount-cold-discover/overwrite-target.txt",
            ),
            (
                "client-b",
                "append",
                "stale-remount-cold-discover/append-target.txt",
            ),
            (
                "client-a",
                "create",
                "stale-remount-cold-discover/nested/child.txt",
            ),
            ("client-c", "stale_remount_after_history", "client-c"),
            ("client-d", "cold_discover_after_history", "client-d"),
        ],
        AdvancedMountedKind::InvalidationDropoutFallback => &[
            ("client-c", "observer_dropout_start", "client-c"),
            (
                "client-a",
                "writer_create",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-b",
                "writer_overwrite",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-a",
                "writer_delete",
                "invalidation-dropout/delete-target.txt",
            ),
            (
                "client-c",
                "observer_stale_window",
                "invalidation-dropout/fallback-target.txt",
            ),
            (
                "client-c",
                "observer_delete_stale_window",
                "invalidation-dropout/delete-target.txt",
            ),
            (
                "client-c",
                "fallback_sync_resume",
                "invalidation-dropout/fallback-target.txt",
            ),
        ],
        AdvancedMountedKind::SymlinkAttribute => &[
            (
                "client-a",
                "symlink_target_write",
                "symlink-attribute/target-v1.txt",
            ),
            (
                "client-a",
                "symlink_create",
                "symlink-attribute/link-to-target -> target-v1.txt",
            ),
            (
                "client-b",
                "symlink_rename",
                "symlink-attribute/link-to-target -> symlink-attribute/renamed-link",
            ),
            (
                "client-b",
                "symlink_target_update",
                "symlink-attribute/renamed-link -> target-v2.txt",
            ),
            (
                "client-c",
                "readonly_attribute_set",
                "symlink-attribute/readonly-attribute.txt",
            ),
            (
                "client-a",
                "near_symlink_write",
                "symlink-attribute/near-symlink-write.txt",
            ),
        ],
        AdvancedMountedKind::LargeHistoryColdBootstrap => &[
            (
                "client-a",
                "large_history_initial_batch",
                "large-history-cold-bootstrap/bulk-create-delete:45000",
            ),
            ("client-d", "cold_discover_after_history", "client-d"),
            ("client-d", "cold_restart_during_catchup", "client-d"),
            (
                "client-a",
                "large_history_moving_head_batch",
                "large-history-cold-bootstrap/bulk-create-delete:5000",
            ),
        ],
    };
    spec.iter()
        .enumerate()
        .map(
            |(index, &(client, operation, path))| NamespaceCollisionOperation {
                order: index + 1,
                client: client.to_string(),
                operation: operation.to_string(),
                path: path.to_string(),
                result: "planned".to_string(),
                timestamp_ms: elapsed_millis_u64(started),
                error: None,
            },
        )
        .collect()
}

pub(in crate::scenario) fn planned_symlink_attribute_summary() -> SymlinkAttributeSummary {
    SymlinkAttributeSummary {
        symlink_supported: true,
        symlink_skip_reason: None,
        readonly_attribute_supported: true,
        readonly_attribute_skip_reason: None,
        final_symlink_path: format!(
            "{}/{}",
            AdvancedMountedKind::SymlinkAttribute.path_root(),
            SYMLINK_ATTRIBUTE_RENAMED_LINK
        ),
        final_symlink_target: SYMLINK_ATTRIBUTE_TARGET_V2.to_string(),
        readonly_path: format!(
            "{}/{}",
            AdvancedMountedKind::SymlinkAttribute.path_root(),
            SYMLINK_ATTRIBUTE_READONLY_FILE
        ),
        readonly_observed_clients: SYMLINK_ATTRIBUTE_CLIENT_NAMES
            .iter()
            .map(|client| (*client).to_string())
            .collect(),
        final_validation_result: "mounted_tree_convergence".to_string(),
    }
}

pub(in crate::scenario) struct SparseRangeSpec {
    pub(in crate::scenario) label: &'static str,
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) payload: Vec<u8>,
}

pub(in crate::scenario) struct SparseHoleSpec {
    pub(in crate::scenario) offset: u64,
    pub(in crate::scenario) len: usize,
}

//! Scenario registration and stable-name ownership.

use std::sync::OnceLock;

use super::types::{Scenario, ScenarioDefinition};
use super::workloads::*;

/// Returns the full registry of available scenario definitions.
pub fn available_scenarios() -> &'static [ScenarioDefinition] {
    static SCENARIOS: OnceLock<Vec<ScenarioDefinition>> = OnceLock::new();

    SCENARIOS.get_or_init(|| {
        vec![
            BasicMultiClientSmoke.definition(),
            PostgresOutageMultiClientRecovery.definition(),
            RustfsOutageMultiClientRecovery.definition(),
            MountedClientCrashRestartRecovery.definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::CrashBeforeWriteAcceptance)
                .definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::CrashAfterPartialWriteAcceptance)
                .definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::CrashAfterFullWriteAcceptance)
                .definition(),
            NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsRename).definition(),
            NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsUnlink).definition(),
            NamespaceCollisionScenario::new(NamespaceCollisionKind::UnlinkVsAppend).definition(),
            NamespaceCollisionScenario::new(NamespaceCollisionKind::OverwriteVsRead).definition(),
            NamespaceCollisionScenario::new(NamespaceCollisionKind::MkdirRmdirVsCreate)
                .definition(),
            NamespaceCollisionScenario::new(NamespaceCollisionKind::ReaddirStatDuringMutation)
                .definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::MountedChaos).definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::HotspotChurn).definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::FaultChaos).definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::LwwReconnectChaos).definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::OpenHandle).definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::SparseRange).definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::ColdObserverLongHistory).definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::StaleRemountColdDiscoverCatchup)
                .definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::InvalidationDropoutFallback)
                .definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::SymlinkAttribute).definition(),
            AdvancedMountedScenario::new(AdvancedMountedKind::LargeHistoryColdBootstrap)
                .definition(),
            SnapshotV2Scenario::new(SnapshotV2Kind::ManualRecovery).definition(),
            SnapshotV2Scenario::new(SnapshotV2Kind::MaterializerTakeover).definition(),
        ]
    })
}

/// Resolves a scenario name to a concrete scenario implementation.
pub(crate) fn resolve_scenario(name: &str) -> Option<Box<dyn Scenario>> {
    match name {
        BASIC_MULTI_CLIENT_SMOKE | "smoke" => Some(Box::new(BasicMultiClientSmoke)),
        POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY | "postgres-outage" => {
            Some(Box::new(PostgresOutageMultiClientRecovery))
        }
        RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY | "rustfs-outage" | "s3-outage" => {
            Some(Box::new(RustfsOutageMultiClientRecovery))
        }
        MOUNTED_CLIENT_CRASH_RESTART_RECOVERY | "client-crash-restart" | "crash-restart" => {
            Some(Box::new(MountedClientCrashRestartRecovery))
        }
        MOUNTED_CRASH_BEFORE_WRITE_ACCEPTANCE
        | "crash-before-write-acceptance"
        | "publication-boundary-before-write" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::CrashBeforeWriteAcceptance,
        ))),
        MOUNTED_CRASH_AFTER_PARTIAL_WRITE_ACCEPTANCE
        | "crash-after-partial-write-acceptance"
        | "publication-boundary-partial-write" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::CrashAfterPartialWriteAcceptance,
        ))),
        MOUNTED_CRASH_AFTER_FULL_WRITE_ACCEPTANCE
        | "crash-after-full-write-acceptance"
        | "publication-boundary"
        | "publication-boundary-crash" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::CrashAfterFullWriteAcceptance,
        ))),
        COLLISION_RENAME_VS_RENAME | "rename-vs-rename" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsRename),
        )),
        COLLISION_RENAME_VS_UNLINK | "rename-vs-unlink" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::RenameVsUnlink),
        )),
        COLLISION_UNLINK_VS_APPEND | "unlink-vs-append" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::UnlinkVsAppend),
        )),
        COLLISION_OVERWRITE_VS_READ | "overwrite-vs-read" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::OverwriteVsRead),
        )),
        COLLISION_MKDIR_RMDIR_VS_CREATE | "mkdir-rmdir-vs-create" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::MkdirRmdirVsCreate),
        )),
        COLLISION_READDIR_STAT_DURING_MUTATION | "readdir-stat-during-mutation" => Some(Box::new(
            NamespaceCollisionScenario::new(NamespaceCollisionKind::ReaddirStatDuringMutation),
        )),
        MOUNTED_CHAOS_CONVERGENCE | "mounted-chaos" | "chaos" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::MountedChaos),
        )),
        MOUNTED_HOTSPOT_CHURN_CONVERGENCE | "hotspot-churn" | "hot-dir-churn" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::HotspotChurn),
        )),
        MOUNTED_FAULT_CHAOS_CONVERGENCE | "fault-chaos" | "chaos-faults" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::FaultChaos),
        )),
        MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE | "lww-chaos" | "reconnect-lww" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::LwwReconnectChaos),
        )),
        MOUNTED_OPEN_HANDLE_CONVERGENCE | "open-handle" | "open-unlink-rename" => Some(Box::new(
            AdvancedMountedScenario::new(AdvancedMountedKind::OpenHandle),
        )),
        MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE | "sparse-range" | "sparse-file-range" => {
            Some(Box::new(AdvancedMountedScenario::new(
                AdvancedMountedKind::SparseRange,
            )))
        }
        MOUNTED_COLD_OBSERVER_LONG_HISTORY_CONVERGENCE
        | "cold-observer"
        | "cold-observer-long-history" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::ColdObserverLongHistory,
        ))),
        MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE
        | "stale-remount"
        | "cold-discover-catchup" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::StaleRemountColdDiscoverCatchup,
        ))),
        MOUNTED_INVALIDATION_DROPOUT_FALLBACK_CONVERGENCE
        | "invalidation-dropout"
        | "fallback-sync" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::InvalidationDropoutFallback,
        ))),
        MOUNTED_SYMLINK_ATTRIBUTE_CONVERGENCE | "symlink-attribute" | "symlink-attrs" => {
            Some(Box::new(AdvancedMountedScenario::new(
                AdvancedMountedKind::SymlinkAttribute,
            )))
        }
        MOUNTED_LARGE_HISTORY_COLD_BOOTSTRAP_CONVERGENCE
        | "large-history-cold-bootstrap"
        | "large-history-catchup" => Some(Box::new(AdvancedMountedScenario::new(
            AdvancedMountedKind::LargeHistoryColdBootstrap,
        ))),
        SNAPSHOT_MANUAL_RECOVERY => Some(Box::new(SnapshotV2Scenario::new(
            SnapshotV2Kind::ManualRecovery,
        ))),
        SNAPSHOT_MATERIALIZER_TAKEOVER => Some(Box::new(SnapshotV2Scenario::new(
            SnapshotV2Kind::MaterializerTakeover,
        ))),
        #[cfg(test)]
        TEST_FAILING_VALIDATE => Some(Box::new(TestFailingScenario {
            cleanup_fails: false,
        })),
        #[cfg(test)]
        TEST_FAILING_VALIDATE_AND_CLEANUP => Some(Box::new(TestFailingScenario {
            cleanup_fails: true,
        })),
        #[cfg(test)]
        TEST_PASSING => Some(Box::new(TestPassingScenario)),
        _ => None,
    }
}

//! Scenario registration and stable-name ownership.

use std::sync::OnceLock;

use super::types::{Scenario, ScenarioDefinition};
use super::workloads::cache_scrub::MountedCacheScrub;
use super::workloads::compression::MountedCompression;
use super::workloads::forward_compat_matrix::{
    FORWARD_COMPAT_MATRIX_ALIAS, MOUNTED_FORWARD_COMPAT_MATRIX, MountedForwardCompatMatrix,
};
use super::workloads::janitor_cli::{JanitorLiveBenchmark, JanitorPostgresRestart};
use super::workloads::macos_mmap_exec::MacosMmapExec;
use super::workloads::macos_recoverable_trash::{MACOS_RECOVERABLE_TRASH, MacosRecoverableTrash};
use super::workloads::published_volume_capture::PublishedVolumeFixtureCapture;
use super::workloads::scratch_auto_promote::{SCRATCH_AUTO_PROMOTE, ScratchAutoPromote};
use super::workloads::scratch_demote::{SCRATCH_DEMOTE, ScratchDemote};
use super::workloads::scratch_e2e::{
    SCRATCH_APPLEDOUBLE, SCRATCH_ATOMIC_SAVE, SCRATCH_CONFLICT, SCRATCH_DURABILITY,
    SCRATCH_ISOLATION, SCRATCH_ORPHAN_RESTORE, ScratchAppleDouble, ScratchAtomicSave,
    ScratchConflict, ScratchDurability, ScratchIsolation, ScratchOrphanRestore,
};
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
            WorkerConcurrency.definition(),
            ConcurrentMkdirMerge.definition(),
            // Keep this acceptance outside the long-standing numbered scenario range.
            MountedSyncSafetyTwoClient.definition(),
            RandomWritePacking.definition(),
            MacosMmapExec.definition(),
            PosixAclXattrRejection.definition(),
            RecoverableTrash.definition(),
            RecoverableTrashDurability::OfflineCrashReplay.definition(),
            RecoverableTrashDurability::S3OutageCrashReplay.definition(),
            RecoverableTrashDurability::ColdBootstrap.definition(),
            RecoverableTrashJanitor.definition(),
            LogicalQuota.definition(),
            MountedCacheScrub.definition(),
            ScratchIsolation.definition(),
            ScratchAtomicSave.definition(),
            ScratchDurability.definition(),
            ScratchAutoPromote.definition(),
            ScratchConflict.definition(),
            ScratchOrphanRestore.definition(),
            ScratchDemote.definition(),
            ScratchAppleDouble.definition(),
            MountedCompression.definition(),
            JanitorPostgresRestart.definition(),
            JanitorLiveBenchmark.definition(),
            PublishedVolumeFixtureCapture.definition(),
            MacosRecoverableTrash.definition(),
            MountedForwardCompatMatrix.definition(),
        ]
    })
}

/// Selector that expands to every registered scenario of the release E2E gate.
pub const RELEASE_GATE_SELECTOR: &str = "release-gate";

/// Registered scenarios the `release-gate` selector leaves out. The fixture
/// capture and the Janitor benchmark are opt-in tools. The forward-compat matrix
/// migrates the shared PostgreSQL irreversibly, so `tools/release/e2e_gate.py`
/// runs it in its own sandbox.
const RELEASE_GATE_EXCLUDED: [&str; 3] = [
    "published-volume-fixture-capture",
    "janitor-live-benchmark",
    MOUNTED_FORWARD_COMPAT_MATRIX,
];

/// Returns the registered scenario names the `release-gate` selector runs, in
/// registry order.
pub fn release_gate_scenarios() -> Vec<&'static str> {
    available_scenarios()
        .iter()
        .map(|scenario| scenario.name)
        .filter(|name| !RELEASE_GATE_EXCLUDED.contains(name))
        .collect()
}

/// Resolves a scenario name to a concrete scenario implementation.
pub(crate) fn resolve_scenario(name: &str) -> Option<Box<dyn Scenario>> {
    match name {
        "mounted-compression" => Some(Box::new(MountedCompression)),
        MOUNTED_FORWARD_COMPAT_MATRIX | FORWARD_COMPAT_MATRIX_ALIAS => {
            Some(Box::new(MountedForwardCompatMatrix))
        }
        "janitor-postgres-restart-recovery" => Some(Box::new(JanitorPostgresRestart)),
        "janitor-live-benchmark" => Some(Box::new(JanitorLiveBenchmark)),
        "published-volume-fixture-capture" => Some(Box::new(PublishedVolumeFixtureCapture)),
        "mounted-recoverable-trash-two-client" => Some(Box::new(RecoverableTrash)),
        "mounted-logical-quota" => Some(Box::new(LogicalQuota)),
        "mounted-cache-scrub" => Some(Box::new(MountedCacheScrub)),
        SCRATCH_ISOLATION => Some(Box::new(ScratchIsolation)),
        SCRATCH_ATOMIC_SAVE => Some(Box::new(ScratchAtomicSave)),
        SCRATCH_DURABILITY => Some(Box::new(ScratchDurability)),
        SCRATCH_AUTO_PROMOTE => Some(Box::new(ScratchAutoPromote)),
        SCRATCH_CONFLICT => Some(Box::new(ScratchConflict)),
        SCRATCH_ORPHAN_RESTORE => Some(Box::new(ScratchOrphanRestore)),
        SCRATCH_DEMOTE => Some(Box::new(ScratchDemote)),
        SCRATCH_APPLEDOUBLE => Some(Box::new(ScratchAppleDouble)),
        "mounted-recoverable-trash-durability" => {
            Some(Box::new(RecoverableTrashDurability::OfflineCrashReplay))
        }
        "mounted-recoverable-trash-s3-crash-replay" => {
            Some(Box::new(RecoverableTrashDurability::S3OutageCrashReplay))
        }
        "mounted-recoverable-trash-cold-bootstrap" => {
            Some(Box::new(RecoverableTrashDurability::ColdBootstrap))
        }
        "mounted-recoverable-trash-janitor-gc" => Some(Box::new(RecoverableTrashJanitor)),
        "mounted-random-write-packing" => Some(Box::new(RandomWritePacking)),
        "mounted-macos-mmap-exec" => Some(Box::new(MacosMmapExec)),
        MACOS_RECOVERABLE_TRASH => Some(Box::new(MacosRecoverableTrash)),
        "mounted-posix-acl-xattr-rejection" => Some(Box::new(PosixAclXattrRejection)),
        "mounted-worker-concurrency" => Some(Box::new(WorkerConcurrency)),
        "mounted-concurrent-mkdir-merge-convergence" | "concurrent-mkdir" => {
            Some(Box::new(ConcurrentMkdirMerge))
        }
        BASIC_MULTI_CLIENT_SMOKE | "smoke" => Some(Box::new(BasicMultiClientSmoke)),
        POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY | "postgres-outage" => {
            Some(Box::new(PostgresOutageMultiClientRecovery))
        }
        RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY | "rustfs-outage" | "s3-outage" => {
            Some(Box::new(RustfsOutageMultiClientRecovery))
        }
        MOUNTED_SYNC_SAFETY_TWO_CLIENT => Some(Box::new(MountedSyncSafetyTwoClient)),
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

#[cfg(test)]
mod tests {
    use super::{available_scenarios, resolve_scenario};

    #[test]
    fn scratch_scenarios_are_registered_and_resolvable() {
        for name in [
            super::SCRATCH_ISOLATION,
            super::SCRATCH_ATOMIC_SAVE,
            super::SCRATCH_DURABILITY,
            super::SCRATCH_AUTO_PROMOTE,
            super::SCRATCH_CONFLICT,
            super::SCRATCH_ORPHAN_RESTORE,
            super::SCRATCH_DEMOTE,
            super::SCRATCH_APPLEDOUBLE,
        ] {
            assert!(
                available_scenarios()
                    .iter()
                    .any(|scenario| scenario.name == name),
                "missing registered scratch scenario: {name}"
            );
            assert!(
                resolve_scenario(name).is_some(),
                "could not resolve scratch scenario: {name}"
            );
        }
    }

    #[test]
    fn logical_quota_is_registered_and_resolvable() {
        assert!(
            available_scenarios()
                .iter()
                .any(|scenario| scenario.name == "mounted-logical-quota")
        );
        assert!(resolve_scenario("mounted-logical-quota").is_some());
    }

    #[test]
    fn forward_compat_matrix_is_registered_and_resolvable() {
        assert!(
            available_scenarios()
                .iter()
                .any(|scenario| scenario.name == super::MOUNTED_FORWARD_COMPAT_MATRIX)
        );
        assert!(resolve_scenario(super::MOUNTED_FORWARD_COMPAT_MATRIX).is_some());
        assert!(resolve_scenario(super::FORWARD_COMPAT_MATRIX_ALIAS).is_some());
    }

    #[test]
    fn release_gate_runs_every_registered_scenario_except_excluded_ones() {
        let gate = super::release_gate_scenarios();
        for excluded in super::RELEASE_GATE_EXCLUDED {
            assert!(
                available_scenarios().iter().any(|s| s.name == excluded),
                "excluded scenario is not registered: {excluded}"
            );
            assert!(!gate.contains(&excluded), "gate must skip {excluded}");
        }
        assert_eq!(
            gate.len() + super::RELEASE_GATE_EXCLUDED.len(),
            available_scenarios().len()
        );
        assert!(gate.iter().all(|name| resolve_scenario(name).is_some()));
    }
}

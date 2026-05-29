//! Scenario registry and lifecycle hooks for mounted E2E runs.

use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
#[cfg(not(test))]
use std::process::{Command, Stdio};
use std::sync::{
    Arc, Barrier, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};

#[cfg(test)]
use crate::client::ClientControllerConfig;
use crate::client::{
    ClientControllerError, ClientSpec, MultiClientController, PeerVolumeBootstrapOptions,
    PeerVolumeBootstrapSummary, SystemClientProcessExecutor, VolumeBootstrapOptions,
    VolumeBootstrapSummary,
};
#[cfg(all(windows, not(test)))]
use crate::client::{run_taskkill_with_timeout, windows_taskkill_status_is_success};
use crate::infra::{
    CommandExecutor, ComposeServiceControl, POSTGRES_SERVICE_NAME, RUSTFS_SERVICE_NAME,
    SystemCommandExecutor,
};
use crate::mounted_tree::{
    ClientMount, ConvergenceOutcome, MountedTreeError, NamespaceInvariantViolation, TreeComparison,
    validate_namespace_invariants, wait_for_convergence,
};
#[cfg(all(windows, not(test)))]
use crate::process_diagnostics::capture_process_diagnostics_best_effort;
use crate::process_job::CHILD_TERMINATION_GRACE;
#[cfg(not(test))]
use crate::process_job::{ChildJob, wait_for_child_exit};
use crate::report::RunManifest;

/// Basic multi-client smoke scenario identifier.
pub const BASIC_MULTI_CLIENT_SMOKE: &str = "basic-multi-client-smoke";
/// Multi-client recovery scenario for PostgreSQL outages.
pub const POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY: &str = "postgres-outage-multi-client-recovery";
/// Multi-client recovery scenario for RustFS/S3 outages.
pub const RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY: &str = "rustfs-outage-multi-client-recovery";
/// Multi-client recovery scenario for mounted client crash and restart.
pub const MOUNTED_CLIENT_CRASH_RESTART_RECOVERY: &str = "mounted-client-crash-restart-recovery";
/// Publication-boundary scenario before a write reaches full remote acceptance.
pub const MOUNTED_CRASH_BEFORE_WRITE_ACCEPTANCE: &str = "mounted-crash-before-write-acceptance";
/// Publication-boundary scenario with partial remote write acceptance before crash.
pub const MOUNTED_CRASH_AFTER_PARTIAL_WRITE_ACCEPTANCE: &str =
    "mounted-crash-after-partial-write-acceptance";
/// Publication-boundary scenario with full remote write acceptance before crash.
pub const MOUNTED_CRASH_AFTER_FULL_WRITE_ACCEPTANCE: &str =
    "mounted-crash-after-full-write-acceptance";
/// Namespace-collision scenario for rename-vs-rename races.
pub const COLLISION_RENAME_VS_RENAME: &str = "collision-rename-vs-rename";
/// Namespace-collision scenario for rename-vs-unlink races.
pub const COLLISION_RENAME_VS_UNLINK: &str = "collision-rename-vs-unlink";
/// Namespace-collision scenario for unlink-vs-append races.
pub const COLLISION_UNLINK_VS_APPEND: &str = "collision-unlink-vs-append";
/// Namespace-collision scenario for overwrite-vs-read races.
pub const COLLISION_OVERWRITE_VS_READ: &str = "collision-overwrite-vs-read";
/// Namespace-collision scenario for mkdir/rmdir-vs-create races.
pub const COLLISION_MKDIR_RMDIR_VS_CREATE: &str = "collision-mkdir-rmdir-vs-create";
/// Namespace-collision scenario for readdir/stat races during mutation.
pub const COLLISION_READDIR_STAT_DURING_MUTATION: &str = "collision-readdir-stat-during-mutation";
/// Chaos convergence scenario for mounted clients.
pub const MOUNTED_CHAOS_CONVERGENCE: &str = "mounted-chaos-convergence";
/// Chaos convergence scenario for hotspot churn workloads.
pub const MOUNTED_HOTSPOT_CHURN_CONVERGENCE: &str = "mounted-hotspot-churn-convergence";
/// Chaos convergence scenario for injected fault handling.
pub const MOUNTED_FAULT_CHAOS_CONVERGENCE: &str = "mounted-fault-chaos-convergence";
/// Chaos convergence scenario for LWW reconnect behavior.
pub const MOUNTED_LWW_RECONNECT_CHAOS_CONVERGENCE: &str = "mounted-lww-reconnect-chaos-convergence";
/// Open-handle convergence scenario under concurrent mutations.
pub const MOUNTED_OPEN_HANDLE_CONVERGENCE: &str = "mounted-open-handle-convergence";
/// Sparse-range convergence scenario.
pub const MOUNTED_SPARSE_FILE_RANGE_CONVERGENCE: &str = "mounted-sparse-file-range-convergence";
/// Long-history cold-observer convergence scenario.
pub const MOUNTED_COLD_OBSERVER_LONG_HISTORY_CONVERGENCE: &str =
    "mounted-cold-observer-long-history-convergence";
/// Stale-remount cold-discover catchup convergence scenario.
pub const MOUNTED_STALE_REMOUNT_COLD_DISCOVER_CATCHUP_CONVERGENCE: &str =
    "mounted-stale-remount-cold-discover-catchup-convergence";
/// Invalidation-dropout fallback convergence scenario.
pub const MOUNTED_INVALIDATION_DROPOUT_FALLBACK_CONVERGENCE: &str =
    "mounted-invalidation-dropout-fallback-convergence";
/// Symlink attribute convergence scenario.
pub const MOUNTED_SYMLINK_ATTRIBUTE_CONVERGENCE: &str = "mounted-symlink-attribute-convergence";
const SMOKE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const COLLISION_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const ADVANCED_MOUNTED_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const FAULT_CHAOS_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
const LWW_RECONNECT_CLIENT_NAMES: &[&str] =
    &["client-a", "client-b", "client-c", "client-d", "client-e"];
const COLD_OBSERVER_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
const COLD_OBSERVER_WARM_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const STALE_REMOUNT_WARM_WRITER_NAMES: &[&str] = &["client-a", "client-b"];
const STALE_REMOUNT_INITIAL_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const INVALIDATION_DROPOUT_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const INVALIDATION_DROPOUT_WRITER_CLIENTS: &[&str] = &["client-a", "client-b"];
const INVALIDATION_DROPOUT_OBSERVER_CLIENT: &str = "client-c";
const SYMLINK_ATTRIBUTE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const POSTGRES_OUTAGE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c"];
const RUSTFS_OUTAGE_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "client-c", "client-d"];
const CRASH_RESTART_CLIENT_NAMES: &[&str] = &["client-a", "client-b"];
const SMOKE_FILE_NAME: &str = "studiofs-e2e-smoke.txt";
const SMOKE_FILE_PAYLOAD: &[u8] = b"studiofs e2e mounted multi-client smoke\n";
const SMOKE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const SMOKE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OUTAGE_INITIAL_FILE_NAME: &str = "studiofs-e2e-outage-initial.txt";
const POSTGRES_OUTAGE_FILE_NAME: &str = "studiofs-e2e-postgres-outage-writer.txt";
const POSTGRES_OUTAGE_RECOVERED_PAYLOAD: &[u8] =
    b"accepted writer-local PostgreSQL outage payload\n";
const OUTAGE_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const OUTAGE_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const RUSTFS_OUTAGE_FILE_NAME: &str = "studiofs-e2e-rustfs-outage.txt";
const RUSTFS_OUTAGE_INITIAL_PAYLOAD: &[u8] = b"initial healthy RustFS payload\n";
const RUSTFS_OUTAGE_RECOVERED_PAYLOAD: &[u8] =
    b"initial healthy RustFS payload\naccepted local payload while RustFS is down\n";
const CRASH_RESTART_LOCAL_DURABLE_FILE_NAME: &str = "studiofs-e2e-crash-after-local-durability.txt";
const CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME: &str =
    "studiofs-e2e-crash-before-remote-publication.txt";
const CRASH_RESTART_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const CRASH_RESTART_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const COLLISION_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
const COLLISION_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const COLLISION_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const COLLISION_OLD_COMMITTED_PAYLOAD: &[u8] = b"old committed payload\n";
const COLLISION_NEW_COMMITTED_PAYLOAD: &[u8] = b"new committed payload\n";
const OVERWRITE_READ_ATTEMPTS: u32 = 8;
const ADVANCED_MOUNTED_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(90);
const ADVANCED_MOUNTED_CONVERGENCE_POLL_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(target_os = "macos")]
const MOUNTED_WRITE_READBACK_TIMEOUT: Duration = Duration::from_secs(45);
#[cfg(not(target_os = "macos"))]
const MOUNTED_WRITE_READBACK_TIMEOUT: Duration = Duration::from_secs(5);
const MOUNTED_WRITE_READBACK_POLL_INTERVAL: Duration = Duration::from_millis(25);
#[cfg(not(test))]
const BOUNDED_MOUNTED_IO_MAX_WORKERS: usize = 1;
#[cfg(test)]
const BOUNDED_MOUNTED_IO_MAX_WORKERS: usize = 64;
#[cfg(target_os = "macos")]
const MOUNTED_VALIDATION_PROBE_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(not(target_os = "macos"))]
const MOUNTED_VALIDATION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const MOUNTED_VALIDATION_MAX_ACTIVE_PROBES: usize = 1;
#[cfg(target_os = "macos")]
const HOTSPOT_CHURN_OPERATION_TIMEOUT: Duration = Duration::from_secs(45);
#[cfg(not(target_os = "macos"))]
const HOTSPOT_CHURN_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const HOTSPOT_HELPER_CANCEL_GRACE: Duration = Duration::from_millis(750);
#[allow(dead_code)]
const HOTSPOT_HELPER_INTERNAL_TIMEOUT_MARKER: &str = "hotspot helper internal timeout";
const OPEN_HANDLE_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const OPEN_HANDLE_SYNC_RETRY_TIMEOUT: Duration = Duration::from_secs(2);
const OPEN_HANDLE_SYNC_RETRY_POLL_INTERVAL: Duration = Duration::from_millis(25);
const HOTSPOT_IO_MAX_ACTIVE_WORKERS: usize = 1;
static HOTSPOT_HELPER_PAYLOAD_ID: AtomicUsize = AtomicUsize::new(0);
const LWW_RECONNECT_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
const FAULT_CHAOS_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
const MOUNTED_CHAOS_INITIAL_TREE: &[(&str, &[u8])] = &[
    ("chaos/rename-source.txt", b"rename source\n"),
    ("chaos/unlink-target.txt", b"unlink target\n"),
    ("chaos/append-target.txt", b"append target\n"),
    ("chaos/read-target.txt", COLLISION_OLD_COMMITTED_PAYLOAD),
];
const OPEN_HANDLE_UNLINK_TARGET: &str = "open-handles/unlink-target.txt";
const OPEN_HANDLE_RENAME_SOURCE: &str = "open-handles/rename-source.txt";
const OPEN_HANDLE_RENAME_DEST: &str = "open-handles/renamed-destination.txt";
const OPEN_HANDLE_UNLINK_TARGET_PAYLOAD: &[u8] = b"open handle initial\n";
const OPEN_HANDLE_RENAME_SOURCE_PAYLOAD: &[u8] = b"rename source\n";
const SPARSE_FILE_PATH: &str = "sparse/shared.bin";
const SPARSE_INITIAL_LOGICAL_SIZE: u64 = 2_097_275;
const SPARSE_SHRUNK_LOGICAL_SIZE: u64 = 1_048_833;
const SPARSE_FINAL_LOGICAL_SIZE: u64 = 3_146_061;
const SPARSE_FIRST_BOUNDARY_OFFSET: u64 = 1_048_569;
const SPARSE_SECOND_BOUNDARY_OFFSET: u64 = 2_097_147;
const SPARSE_FINAL_TAIL_OFFSET: u64 = 3_145_900;
const SPARSE_PREFIX_PAYLOAD: &[u8] = b"sparse-prefix-sfs-377\n";
const SPARSE_FIRST_BOUNDARY_PAYLOAD: &[u8] = b"crosses-first-chunk-boundary-sfs-377\n";
const SPARSE_SECOND_BOUNDARY_PAYLOAD: &[u8] = b"crosses-second-chunk-boundary-sfs-377\n";
const SPARSE_FINAL_TAIL_PAYLOAD: &[u8] = b"sparse-final-tail-sfs-377\n";
const COLD_OBSERVER_DELETE_TARGET: &str = "delete-target.txt";
const COLD_OBSERVER_RENAME_SOURCE: &str = "rename-source.txt";
const COLD_OBSERVER_RENAME_DEST: &str = "renamed-final.txt";
const COLD_OBSERVER_OVERWRITE_TARGET: &str = "overwrite-target.txt";
const COLD_OBSERVER_APPEND_TARGET: &str = "append-target.txt";
const COLD_OBSERVER_NESTED_FILE: &str = "nested/child.txt";
const SYMLINK_ATTRIBUTE_TARGET_V1: &str = "target-v1.txt";
const SYMLINK_ATTRIBUTE_TARGET_V2: &str = "target-v2.txt";
const SYMLINK_ATTRIBUTE_LINK: &str = "link-to-target";
const SYMLINK_ATTRIBUTE_RENAMED_LINK: &str = "renamed-link";
const SYMLINK_ATTRIBUTE_READONLY_FILE: &str = "readonly-attribute.txt";
const SYMLINK_ATTRIBUTE_NEARBY_FILE: &str = "near-symlink-write.txt";
const INVALIDATION_DROPOUT_FALLBACK_INTERVAL_MS: u64 = 3_000;
const INVALIDATION_DROPOUT_STALE_WINDOW_MS: u64 = 500;
const INVALIDATION_DROPOUT_BASELINE_FILE: &str = "baseline.txt";
const INVALIDATION_DROPOUT_TARGET_FILE: &str = "fallback-target.txt";
const INVALIDATION_DROPOUT_DELETE_TARGET: &str = "delete-target.txt";

#[cfg(test)]
/// Scenario identifier for test helper that expects validation failures.
pub(crate) const TEST_FAILING_VALIDATE: &str = "__test-failing-validate";
#[cfg(test)]
/// Scenario identifier for test helper that expects validation failures with cleanup failures.
pub(crate) const TEST_FAILING_VALIDATE_AND_CLEANUP: &str = "__test-failing-validate-and-cleanup";
#[cfg(test)]
/// Scenario identifier for test helper that expects passing validation.
pub(crate) const TEST_PASSING: &str = "__test-passing";

use super::types::{Scenario, ScenarioContext, ScenarioDefinition, ScenarioError};

mod actions;
mod advanced_mounted;
mod assertions;
mod faults;
mod namespace_collision;
mod performance_probe;
mod smoke_outage;
#[cfg(test)]
mod test_fixtures;

pub(super) use actions::*;
pub use actions::{
    SparseOpHelperRequest, run_hotspot_op_helper_with_timeout, run_open_handle_op_helper,
    run_sparse_op_helper_with_timeout,
};
pub(super) use advanced_mounted::*;
pub(super) use assertions::*;
pub(super) use faults::*;
pub(super) use namespace_collision::*;
pub use performance_probe::MOUNTED_PERFORMANCE_PROBE;
pub(super) use performance_probe::MountedPerformanceProbe;
pub(super) use smoke_outage::*;
#[cfg(test)]
pub(super) use test_fixtures::*;

#[cfg(test)]
mod tests;

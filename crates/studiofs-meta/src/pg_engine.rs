#[cfg(test)]
use crate::engine::{
    MetaEngineCore, MetaFsOps, MetaGc, MetaNamespace, MetaPayloadObjects, MetaSymlink, MetaXattr,
};
use crate::engine::{
    MetaError, MetaFormat, MetaInodes, MetaReplication, MetaResult, PgSyncLifecycle,
};
#[cfg(test)]
use crate::events::MetaEvent;
use crate::pg_transport::connect_studiofs_postgres;
use crate::redb_engine::RedbMetaEngine;
use crate::types::*;
use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU32, AtomicU64};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Default maximum number of PostgreSQL command connections per client.
pub const DEFAULT_PG_POOL_SIZE: usize = 4;

const PG_POOL_SIZE_ENV_VAR: &str = "STUDIOFS_PG_POOL_SIZE";

#[cfg(test)]
thread_local! {
    static TEST_ENV_OVERRIDES: std::cell::RefCell<HashMap<&'static str, Option<String>>> =
        std::cell::RefCell::new(HashMap::new());
}

fn pg_env_var(key: &str) -> Option<String> {
    #[cfg(test)]
    if let Some(value) = TEST_ENV_OVERRIDES.with(|overrides| overrides.borrow().get(key).cloned()) {
        return value;
    }
    std::env::var(key).ok()
}

#[cfg(test)]
struct TestEnvGuard {
    key: &'static str,
    previous: Option<Option<String>>,
}

#[cfg(test)]
impl Drop for TestEnvGuard {
    fn drop(&mut self) {
        TEST_ENV_OVERRIDES.with(|overrides| {
            let mut overrides = overrides.borrow_mut();
            match self.previous.take() {
                Some(value) => {
                    overrides.insert(self.key, value);
                }
                None => {
                    overrides.remove(self.key);
                }
            }
        });
    }
}

#[cfg(test)]
fn set_test_env(key: &'static str, value: &str) -> TestEnvGuard {
    set_test_env_value(key, Some(value))
}

#[cfg(test)]
fn set_test_env_value(key: &'static str, value: Option<&str>) -> TestEnvGuard {
    let previous = TEST_ENV_OVERRIDES.with(|overrides| {
        overrides
            .borrow_mut()
            .insert(key, value.map(str::to_string))
    });
    TestEnvGuard { key, previous }
}

fn resolve_pg_pool_size(configured: usize) -> MetaResult<usize> {
    if let Some(value) = pg_env_var(PG_POOL_SIZE_ENV_VAR) {
        let pool_size = value.parse::<usize>().map_err(|_| {
            MetaError::Storage(format!("{PG_POOL_SIZE_ENV_VAR} must be a positive integer"))
        })?;
        if pool_size == 0 {
            return Err(MetaError::Storage(format!(
                "{PG_POOL_SIZE_ENV_VAR} must be a positive integer"
            )));
        }
        return Ok(pool_size);
    }

    if configured == 0 {
        return Err(MetaError::Storage(
            "postgres command pool size must be greater than zero".to_string(),
        ));
    }
    Ok(configured)
}

const LOCAL_PG_VERSION_COUNTER: &str = "pgLastAppliedBatchVersion";
const LOCAL_PG_NAMESPACE_VERSION_COUNTER: &str = "pgLastAppliedNamespaceBatchVersion";
const LOCAL_PG_CLIENT_SLOT_COUNTER: &str = "pgClientSlotPlusOne";
const LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER: &str = "pgLastFlushedLocalEventSeq";
pub(crate) const LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER: &str =
    "pgRedbRepairLocalGraphRecoveryPending";
pub(crate) const LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER: &str = "pgFreshClientPending";
const LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER: &str = "pgInodeActiveRangeStart";
const LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER: &str = "pgInodeActiveRangeEnd";
const LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER: &str = "pgInodeReservedRangeEnd";
const LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER: &str = "pgInodeCurrentWindowStart";
const LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER: &str = "pgInodeCurrentWindowEnd";
const LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER: &str = "pgInodePrefetchedWindowStart";
const LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER: &str = "pgInodePrefetchedWindowNext";
const LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER: &str = "pgInodePrefetchedWindowEnd";
const PG_NEXT_CLIENT_SLOT_COUNTER: &str = "next_client_slot";
const PG_RETIRED_INODE_COUNTER: &str = "next_inode";
const PG_NEXT_ALLOCATOR_NAMESPACE_COUNTER: &str = "next_allocator_namespace";
const DEFAULT_PG_FLUSH_INTERVAL_SECS: u64 = 60;
const DEFAULT_PG_FLUSH_OPS_THRESHOLD: u32 = 500;
const PG_EVENT_INSERT_CHUNK_SIZE: usize = 512;
const DEFAULT_PG_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PG_COMMAND_TIMEOUT_MS: u64 = 30_000;
const PG_JANITOR_LOCK_SALT: i64 = 0x5f53_4653_5f4a_414e;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StartupSnapshotImportMode {
    ReplaceLocalState,
    ReplaceLocalStatePreservingEventLog,
    PreserveLocalState,
}

pub(crate) fn is_local_pg_snapshot_state_counter(name: &str) -> bool {
    matches!(
        name,
        LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER
            | LOCAL_PG_CLIENT_SLOT_COUNTER
            | LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER
            | LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER
            | LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER
            | LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER
            | LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER
            | LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER
            | LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER
            | LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER
            | LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER
            | LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER
    )
}

/// Owns PostgreSQL worker cancellation state and handles.
struct PgBackgroundWorkers {
    poll_shutdown: Arc<AtomicBool>,
    poll_wakeup: Arc<(Mutex<bool>, Condvar)>,
    lock_notify_cancel: CancellationToken,
    metadata_notify_cancel: CancellationToken,
    state: Mutex<PgBackgroundWorkerState>,
    state_changed: Condvar,
}

struct PgBackgroundWorkerState {
    poll_handle: Option<std::thread::JoinHandle<()>>,
    metadata_notify_handle: Option<std::thread::JoinHandle<()>>,
    lock_notify_handle: Option<std::thread::JoinHandle<()>>,
    lock_renew_handle: Option<std::thread::JoinHandle<()>>,
    shutdown_complete: bool,
    shutdown_in_progress: bool,
    shutdown_error: Option<MetaError>,
}

#[derive(Clone, Copy)]
enum PgBackgroundWorker {
    Poll,
    MetadataNotify,
    LockNotify,
    LockRenew,
}

/// PostgreSQL-backed metadata engine.
///
/// Current implementation strategy:
/// - local `redb` remains the foreground serving store
/// - PostgreSQL `event_log` is the runtime delta stream
/// - PostgreSQL `metadata_state` stores bootstrap/checkpoint snapshots
/// - foreign namespace/attr state is applied into local `base_*`
/// - local serving remains `overlay over base`
/// - foreign batches are pulled in `batch_version` order
pub struct PgMetaEngine {
    local: Arc<RedbMetaEngine>,
    postgres_url: String,
    volume_id: i64,
    client_id: String,
    client_slot_plus_one: AtomicU64,
    local_version: AtomicI64,
    namespace_version: AtomicI64,
    flushed_event_seq: AtomicU64,
    local_position_lock: Mutex<()>,
    replication_lock: Mutex<()>,
    poll_interval: Duration,
    flush_interval: Duration,
    flush_idle_grace: Duration,
    flush_ops_threshold: u32,
    connect_timeout: Duration,
    command_timeout: Duration,
    command_pool: Arc<runtime::PgConnectionPool>,
    inflight_replication_ops: AtomicU32,
    pending_state: Mutex<PendingReplicationState>,
    remote_runtime: runtime::PgRemoteRuntime,
    allocator_runtime: Mutex<inode_allocator::AllocatorRuntime>,
    allocator_refill_lock: Mutex<()>,
    inode_id_headroom_available: AtomicBool,
    object_id_headroom_available: AtomicBool,
    pg_sync_lifecycle: AtomicU8,
    janitor_lock: Mutex<Option<PgJanitorLock>>,
    file_lock_cache: Mutex<HashMap<Ino, FileLockLease>>,
    held_file_locks: Mutex<HashMap<Ino, HeldFileLockLease>>,
    active_open_inodes: Mutex<HashMap<Ino, u32>>,
    self_weak: OnceLock<Weak<Self>>,
    lock_workers_enabled: bool,
    lock_worker_start_lock: Mutex<()>,
    background_workers: PgBackgroundWorkers,
    /// Wakes VFS metadata invalidation after PG metadata NOTIFY syncs local state.
    metadata_invalidation_notify: Arc<tokio::sync::Notify>,
    metadata_notify_received: AtomicU64,
    metadata_notify_reconnects: AtomicU64,
    metadata_fallback_poll_discoveries: AtomicU64,
    metadata_sync_errors: AtomicU64,
    metadata_last_successful_sync_unix_ms: AtomicI64,
    /// Last remote metadata head observed by a bounded command query.
    metadata_remote_head_version: AtomicI64,
    /// Whether the most recent metadata synchronization command succeeded.
    metadata_source_connected: AtomicBool,
    /// Whether PostgreSQL's sync format was validated successfully for this
    /// runtime. A local-first startup may leave this pending while PostgreSQL
    /// is unavailable; no remote pull or publication may pass that boundary.
    remote_format_validated: AtomicBool,
    remote_format_validation_lock: Mutex<()>,
}

/// PostgreSQL-only snapshot capabilities for callers that must not open the
/// volume's live redb database a second time.
pub struct PgSnapshotBackend {
    postgres_url: String,
    volume_id: i64,
    command_timeout: Duration,
    command_pool: Arc<runtime::PgConnectionPool>,
}

trait SnapshotBackendAccess {
    fn snapshot_postgres_url(&self) -> &str;
    fn snapshot_volume_id(&self) -> i64;
    fn snapshot_command_timeout(&self) -> Duration;
    fn snapshot_command_pool(&self) -> Arc<runtime::PgConnectionPool>;
}

impl SnapshotBackendAccess for PgSnapshotBackend {
    fn snapshot_postgres_url(&self) -> &str {
        &self.postgres_url
    }

    fn snapshot_volume_id(&self) -> i64 {
        self.volume_id
    }

    fn snapshot_command_timeout(&self) -> Duration {
        self.command_timeout
    }

    fn snapshot_command_pool(&self) -> Arc<runtime::PgConnectionPool> {
        self.command_pool.clone()
    }
}

impl SnapshotBackendAccess for PgMetaEngine {
    fn snapshot_postgres_url(&self) -> &str {
        &self.postgres_url
    }

    fn snapshot_volume_id(&self) -> i64 {
        self.volume_id
    }

    fn snapshot_command_timeout(&self) -> Duration {
        self.command_timeout
    }

    fn snapshot_command_pool(&self) -> Arc<runtime::PgConnectionPool> {
        self.command_pool.clone()
    }
}

impl PgSnapshotBackend {
    pub(crate) fn from_engine(engine: &PgMetaEngine) -> Arc<Self> {
        Arc::new(Self {
            postgres_url: engine.postgres_url.clone(),
            volume_id: engine.volume_id,
            command_timeout: engine.command_timeout,
            command_pool: engine.command_pool.clone(),
        })
    }

    /// Opens snapshot capabilities without opening any local metadata file.
    pub fn open(postgres_url: &str, volume_id: i64) -> MetaResult<Arc<Self>> {
        if postgres_url.trim().is_empty() {
            return Err(MetaError::Storage(
                "metadata.postgres.url must not be empty".to_string(),
            ));
        }
        let connect_timeout = PgMetaEngine::default_connect_timeout();
        let pool_size = resolve_pg_pool_size(DEFAULT_PG_POOL_SIZE)?;
        Ok(Arc::new(Self {
            postgres_url: postgres_url.to_string(),
            volume_id,
            command_timeout: PgMetaEngine::default_command_timeout(),
            command_pool: Arc::new(runtime::PgConnectionPool::new(
                postgres_url.to_string(),
                connect_timeout,
                connect_timeout,
                pool_size,
            )?),
        }))
    }
}

#[derive(Debug, Clone, Copy)]
/// Options that control how a [`PgMetaEngine`] instance is opened.
pub struct PgOpenOptions {
    /// Whether to start background polling and lock-worker threads immediately.
    pub background_poll: bool,
    /// Whether this open belongs to the checkpoint-first bootstrap of a newly
    /// discovered client.
    pub fresh_client: bool,
    /// Maximum number of PostgreSQL command connections for this client.
    /// Values above the runtime semaphore capacity are rejected.
    /// `STUDIOFS_PG_POOL_SIZE` overrides this value when set.
    pub pg_pool_size: usize,
}

impl Default for PgOpenOptions {
    fn default() -> Self {
        Self {
            background_poll: true,
            fresh_client: false,
            pg_pool_size: DEFAULT_PG_POOL_SIZE,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct PendingReplicationState {
    ops: u32,
    started_at: Option<Instant>,
    last_mutation_at: Option<Instant>,
    force_flush: bool,
    event_start: Option<u64>,
    event_end: u64,
}

#[derive(Clone, Copy)]
struct ClaimedPendingBatch {
    ops: u32,
    started_at: Option<Instant>,
    last_mutation_at: Option<Instant>,
    force_flush: bool,
    event_start: u64,
    event_end: u64,
}

#[path = "pg_engine/engine_core.rs"]
mod engine_core;
#[path = "pg_engine/inode_allocator.rs"]
mod inode_allocator;
#[path = "pg_engine/janitor.rs"]
mod janitor;
#[path = "pg_engine/lifecycle.rs"]
mod lifecycle;
#[path = "pg_engine/local_state.rs"]
mod local_state;
#[path = "pg_engine/local_traits.rs"]
mod local_traits;
#[path = "pg_engine/lock_leases.rs"]
mod lock_leases;
#[path = "pg_engine/lock_workers.rs"]
mod lock_workers;
#[path = "pg_engine/metadata_io.rs"]
mod metadata_io;
#[path = "pg_engine/metadata_notify.rs"]
mod metadata_notify;
#[path = "pg_engine/metadata_sync.rs"]
mod metadata_sync;
#[path = "pg_engine/remote_apply.rs"]
mod remote_apply;
#[path = "pg_engine/replication_api.rs"]
mod replication_api;
#[path = "pg_engine/replication_flush.rs"]
mod replication_flush;
#[path = "pg_engine/runtime.rs"]
mod runtime;
/// Canonical PostgreSQL schema definition and provisioning helper.
pub mod schema;
#[path = "pg_engine/snapshot_cleanup.rs"]
mod snapshot_cleanup;
/// PostgreSQL Snapshot v2 capture and control-plane queries.
pub(crate) mod snapshot_control;
#[path = "pg_engine/snapshot_jobs.rs"]
mod snapshot_jobs;
#[path = "pg_engine/snapshot_mount_leases.rs"]
mod snapshot_mount_leases;
#[path = "pg_engine/snapshot_policy.rs"]
mod snapshot_policy;
#[path = "pg_engine/snapshot_reachability.rs"]
pub(crate) mod snapshot_reachability;
#[path = "pg_engine/sql.rs"]
mod sql;
use janitor::PgJanitorLock;
#[cfg(test)]
use remote_apply::DecodedReplayBatch;
use remote_apply::{
    OverlayConflictSets, RecentFlush, RemoteApplyCoordinator, RemoteReplayApplyContext,
    RemoteReplayApplyOutcome,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataSyncSource {
    Manual,
    Notify,
    FallbackPoll,
}

#[derive(Clone, Copy)]
pub(super) enum PgTimeoutContext<'a> {
    Connection(&'a str),
    Command(&'a str),
}

enum StartupSchemaValidation {
    Available,
    Fallback(MetaError),
}

#[derive(Debug, Clone)]
struct HeldFileLockLease {
    lease_ttl: Duration,
    next_renew_at: Instant,
}

impl PgMetaEngine {
    const LOCK_NOTIFY_IDLE_POLL_INTERVAL_MS: u64 = 250;
    const LOCK_NOTIFY_SAFETY_REFRESH_INTERVAL_MS: u64 = 30_000;
    const METADATA_NOTIFY_IDLE_POLL_INTERVAL_MS: u64 = 250;
    const METADATA_NOTIFY_RECONNECT_MIN_DELAY_MS: u64 = 250;
    const METADATA_NOTIFY_RECONNECT_MAX_DELAY_MS: u64 = 5_000;

    fn pg_sync_lifecycle_from_raw(value: u8) -> PgSyncLifecycle {
        match value {
            value if value == PgSyncLifecycle::LocalReady as u8 => PgSyncLifecycle::LocalReady,
            value if value == PgSyncLifecycle::CatchingUp as u8 => PgSyncLifecycle::CatchingUp,
            value if value == PgSyncLifecycle::TargetReached as u8 => {
                PgSyncLifecycle::TargetReached
            }
            value if value == PgSyncLifecycle::AllocatorPending as u8 => {
                PgSyncLifecycle::AllocatorPending
            }
            value if value == PgSyncLifecycle::Live as u8 => PgSyncLifecycle::Live,
            value if value == PgSyncLifecycle::Degraded as u8 => PgSyncLifecycle::Degraded,
            _ => PgSyncLifecycle::Fatal,
        }
    }

    fn current_pg_sync_lifecycle(&self) -> PgSyncLifecycle {
        Self::pg_sync_lifecycle_from_raw(self.pg_sync_lifecycle.load(Ordering::Acquire))
    }

    pub(super) fn transition_pg_sync_lifecycle(&self, next: PgSyncLifecycle) {
        loop {
            let current_raw = self.pg_sync_lifecycle.load(Ordering::Acquire);
            let current = Self::pg_sync_lifecycle_from_raw(current_raw);
            if current == next {
                return;
            }
            if !current.can_transition_to(next) {
                warn!(
                    from = ?current,
                    to = ?next,
                    volume_id = self.volume_id,
                    "invalid pg sync lifecycle transition"
                );
                return;
            }
            if self
                .pg_sync_lifecycle
                .compare_exchange(current_raw, next as u8, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    pub(super) fn refresh_pg_sync_lifecycle_for_allocator(&self) {
        let current = self.current_pg_sync_lifecycle();
        let authority_available = self.metadata_authority_status().allows_new_mutations();
        let next = match (current, authority_available) {
            (PgSyncLifecycle::LocalReady, true)
            | (PgSyncLifecycle::TargetReached, true)
            | (PgSyncLifecycle::AllocatorPending, true) => PgSyncLifecycle::Live,
            (PgSyncLifecycle::LocalReady, false)
            | (PgSyncLifecycle::TargetReached, false)
            | (PgSyncLifecycle::Live, false) => PgSyncLifecycle::AllocatorPending,
            (lifecycle, _) => lifecycle,
        };
        self.transition_pg_sync_lifecycle(next);
    }

    fn env_duration_ms(key: &str, default_ms: u64) -> Duration {
        Duration::from_millis(
            pg_env_var(key)
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default_ms),
        )
    }

    fn default_connect_timeout() -> Duration {
        Self::env_duration_ms(
            "STUDIOFS_PG_CONNECT_TIMEOUT_MS",
            DEFAULT_PG_CONNECT_TIMEOUT_MS,
        )
    }

    fn default_command_timeout() -> Duration {
        Self::env_duration_ms(
            "STUDIOFS_PG_COMMAND_TIMEOUT_MS",
            DEFAULT_PG_COMMAND_TIMEOUT_MS,
        )
    }

    fn default_lock_notify_safety_refresh_interval() -> Duration {
        Self::env_duration_ms(
            "STUDIOFS_PG_LOCK_NOTIFY_SAFETY_REFRESH_MS",
            Self::LOCK_NOTIFY_SAFETY_REFRESH_INTERVAL_MS,
        )
    }

    fn validate_postgres_schema(
        postgres_url: &str,
        volume_id: i64,
        connect_timeout: Duration,
        command_timeout: Duration,
    ) -> MetaResult<StartupSchemaValidation> {
        let postgres_url = postgres_url.to_string();
        Self::run_async_once(async move {
            let (client, connection) = match tokio::time::timeout(
                connect_timeout,
                connect_studiofs_postgres(&postgres_url),
            )
            .await
            {
                Ok(Ok(connection)) => connection,
                Ok(Err(error))
                    if Self::is_postgres_unavailable(&error)
                        || matches!(&error, MetaError::Timeout(_)) =>
                {
                    return Ok(StartupSchemaValidation::Fallback(error));
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Ok(StartupSchemaValidation::Fallback(Self::pg_timeout_error(
                        PgTimeoutContext::Connection("validate postgres schema connect"),
                        connect_timeout,
                    )));
                }
            };
            let connection_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let result = tokio::time::timeout(
                command_timeout,
                async move {
                    let rows = client
                        .query(
                            r#"
                            SELECT required.name
                            FROM (
                                VALUES
                                    ('volumes'::TEXT),
                                    ('counters'::TEXT),
                                    ('event_log'::TEXT),
                                    ('metadata_state'::TEXT),
                                    ('acl_rules'::TEXT),
                                    ('file_lock_leases'::TEXT),
                                    ('snapshot_catalog'::TEXT),
                                    ('snapshot_creation_jobs'::TEXT),
                                    ('snapshot_creation_leases'::TEXT),
                                    ('snapshot_cleanup_leases'::TEXT),
                                    ('snapshot_mount_leases'::TEXT),
                                    ('snapshot_policies'::TEXT),
                                    ('snapshot_source_checkpoints'::TEXT),
                                    ('snapshot_current_references'::TEXT),
                                    ('snapshot_object_reachability'::TEXT),
                                    ('snapshot_reachability_batches'::TEXT),
                                    ('snapshot_reachability_bootstrap'::TEXT),
                                    ('snapshot_payload_upload_objects'::TEXT),
                                    ('snapshot_payload_upload_fragments'::TEXT),
                                    ('snapshot_payload_reconcile_cursors'::TEXT),
                                    ('snapshot_artifacts'::TEXT),
                                    ('snapshot_artifact_payload_proofs'::TEXT),
                                    ('snapshot_artifact_attempts'::TEXT)
                            ) AS required(name)
                            LEFT JOIN information_schema.tables AS table_info
                                ON table_info.table_schema = current_schema()
                                AND table_info.table_name = required.name
                            WHERE table_info.table_name IS NULL
                            ORDER BY required.name
                            "#,
                            &[],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error("postgres schema validation failed", error)
                        })?;
                    if !rows.is_empty() {
                        let missing = rows
                            .into_iter()
                            .map(|row| row.get::<_, String>(0))
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Err(MetaError::Storage(format!(
                            "postgres schema is missing required table(s): {missing}; recreate the PostgreSQL volume with `studiofs destroy` followed by `studiofs init`, then run `studiofs discover` for a fresh client"
                        )));
                    }

                    let rows = client
                        .query(
                            r#"
                            SELECT required.table_name || '.' || required.column_name
                            FROM (
                                VALUES
                                    ('metadata_state'::TEXT, 'metadata_format_version'::TEXT),
                                    ('metadata_state'::TEXT, 'checkpoint_sha256'::TEXT),
                                    ('metadata_state'::TEXT, 'checkpoint_publication_state'::TEXT),
                                    ('metadata_state'::TEXT, 'checkpoint_generation'::TEXT)
                            ) AS required(table_name, column_name)
                            LEFT JOIN information_schema.columns AS column_info
                                ON column_info.table_schema = current_schema()
                                AND column_info.table_name = required.table_name
                                AND column_info.column_name = required.column_name
                            WHERE column_info.column_name IS NULL
                            ORDER BY required.table_name, required.column_name
                            "#,
                            &[],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error("postgres schema column validation failed", error)
                        })?;
                    if !rows.is_empty() {
                        let missing = rows
                            .into_iter()
                            .map(|row| row.get::<_, String>(0))
                            .collect::<Vec<_>>()
                            .join(", ");
                        return Err(MetaError::Storage(format!(
                            "postgres schema is missing required column(s): {missing}; recreate the PostgreSQL volume with `studiofs destroy` followed by `studiofs init`, then run `studiofs discover` for a fresh client"
                        )));
                    }

                    let row = client
                        .query_opt(
                            "SELECT metadata_format_version FROM metadata_state WHERE vol = $1",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|error| {
                            Self::classify_pg_error("postgres metadata format query failed", error)
                        })?;
                    let found = row.and_then(|row| row.get::<_, Option<i16>>(0));
                    crate::ensure_supported_pg_sync_format_version(found)?;
                    Ok(())
                },
            )
            .await
            .map_err(|_| {
                Self::pg_timeout_error(
                    PgTimeoutContext::Command("validate postgres schema"),
                    command_timeout,
                )
            });
            connection_task.abort();
            let _ = connection_task.await;
            match result {
                Ok(Ok(())) => Ok(StartupSchemaValidation::Available),
                Ok(Err(error)) if Self::is_postgres_unavailable(&error) => {
                    Ok(StartupSchemaValidation::Fallback(error))
                }
                Ok(Err(error)) if matches!(&error, MetaError::Timeout(_)) => {
                    Ok(StartupSchemaValidation::Fallback(error))
                }
                Ok(Err(error)) => Err(error),
                Err(error) => Ok(StartupSchemaValidation::Fallback(error)),
            }
        })
    }

    fn is_postgres_unavailable(error: &MetaError) -> bool {
        matches!(error, MetaError::RemoteUnavailable(_))
    }

    fn ensure_remote_format_validated(&self) -> MetaResult<()> {
        let _guard = self
            .remote_format_validation_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg format validation lock poisoned".to_string()))?;
        if self.remote_format_validated.load(Ordering::Acquire) {
            return Ok(());
        }

        self.validate_remote_format_version()?;
        self.remote_format_validated.store(true, Ordering::Release);
        Ok(())
    }

    fn validate_remote_format_version(&self) -> MetaResult<()> {
        let command_pool = Arc::clone(&self.command_pool);
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "validate postgres format",
                move |client| {
                    Box::pin(async move {
                        let row = client
                            .query_opt(sql::SELECT_METADATA_FORMAT_VERSION, &[&volume_id])
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres metadata format query failed",
                                    error,
                                )
                            })?;
                        let found = row.and_then(|row| row.get::<_, Option<i16>>(0));
                        crate::ensure_supported_pg_sync_format_version(found)
                    })
                },
            )
            .await
        })
    }

    fn invalidate_remote_format_validation(&self) {
        self.remote_format_validated.store(false, Ordering::Release);
    }

    fn record_remote_operation_result<T>(&self, result: MetaResult<T>) -> MetaResult<T> {
        if result.is_err() {
            self.invalidate_remote_format_validation();
        }
        result
    }

    /// Open the PostgreSQL-backed metadata engine using default startup options.
    pub fn open(path: impl AsRef<Path>) -> MetaResult<Arc<Self>> {
        Self::open_with_options(path, PgOpenOptions::default())
    }

    /// Open the PostgreSQL-backed metadata engine with explicit startup options.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: PgOpenOptions,
    ) -> MetaResult<Arc<Self>> {
        let local = Arc::new(RedbMetaEngine::open(path)?);
        let repair_local_graph_recovery_pending =
            local.get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)? != 0;
        let info = local.get_format_info()?;
        if info.metadata.backend != MetadataBackend::Postgres {
            return Err(MetaError::Storage(
                "PgMetaEngine open requested for non-Postgres volume".to_string(),
            ));
        }
        let metadata_sync_succeeded;
        if repair_local_graph_recovery_pending {
            // A crash can persist the durable repair marker before the caller
            // resets these cursors. Reset them here as well, before any remote
            // snapshot/catch-up decision can trust the stale positions.
            local.set_counter(LOCAL_PG_VERSION_COUNTER, 0)?;
            local.set_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER, 0)?;
        }
        local.set_remote_namespace_visibility_filter(true)?;

        let pg = info.metadata.postgres.ok_or_else(|| {
            MetaError::Storage("missing metadata.postgres config for Postgres backend".to_string())
        })?;
        if pg.url.trim().is_empty() {
            return Err(MetaError::Storage(
                "metadata.postgres.url must not be empty".to_string(),
            ));
        }
        if options.fresh_client {
            local.set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1)?;
        }
        let fresh_client_pending = local.get_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)? != 0;
        let pool_size = resolve_pg_pool_size(options.pg_pool_size)?;

        let poll_secs = pg_env_var("STUDIOFS_PG_SYNC_POLL_SECS")
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2);
        let flush_idle_grace = Duration::from_millis(
            pg_env_var("STUDIOFS_PG_FLUSH_IDLE_MS")
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(5_000),
        );
        let flush_interval = Duration::from_secs(
            pg_env_var("STUDIOFS_PG_FLUSH_INTERVAL_SECS")
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_PG_FLUSH_INTERVAL_SECS),
        );
        let flush_ops_threshold = pg_env_var("STUDIOFS_PG_FLUSH_OPS_THRESHOLD")
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_PG_FLUSH_OPS_THRESHOLD);
        let connect_timeout = Self::default_connect_timeout();
        let command_timeout = Self::default_command_timeout();
        let remote_format_validated = match Self::validate_postgres_schema(
            &pg.url,
            pg.volume_id,
            connect_timeout,
            command_timeout,
        ) {
            Ok(StartupSchemaValidation::Available) => true,
            Ok(StartupSchemaValidation::Fallback(error)) => {
                warn!(
                    error = %error,
                    "postgres schema validation unavailable; continuing with local-first startup"
                );
                false
            }
            Err(error) => return Err(error),
        };
        let persisted_client_slot = Self::load_persisted_client_slot(&local)?;

        let client_slot = match persisted_client_slot {
            Some(slot) => slot,
            None => Self::ensure_client_slot(&local, &pg.url, pg.volume_id)?,
        };
        let restored_local_version = Self::load_persisted_local_version(&local)?;
        let restored_namespace_version = Self::load_persisted_namespace_version(&local)?;
        let restored_flushed_event_seq = Self::load_persisted_flushed_event_seq(&local)?;
        let restored_flushed_event_seq =
            if restored_flushed_event_seq == 0 && !Self::has_any_local_overlay_mutation(&local)? {
                local.get_counter("nextEventSeq")?
            } else {
                restored_flushed_event_seq
            };
        let startup_snapshot_import_mode = if repair_local_graph_recovery_pending {
            None
        } else {
            Self::startup_snapshot_import_mode(&local, restored_local_version)?
        };
        let metadata_invalidation_notify = Arc::clone(&local.invalidation_notify);
        let engine = Arc::new(Self {
            local,
            postgres_url: pg.url.clone(),
            volume_id: pg.volume_id,
            client_id: format!("client-slot:{client_slot}"),
            client_slot_plus_one: AtomicU64::new(client_slot.saturating_add(1)),
            local_version: AtomicI64::new(restored_local_version),
            namespace_version: AtomicI64::new(restored_namespace_version),
            flushed_event_seq: AtomicU64::new(restored_flushed_event_seq),
            local_position_lock: Mutex::new(()),
            replication_lock: Mutex::new(()),
            poll_interval: Duration::from_secs(poll_secs),
            flush_interval,
            flush_idle_grace,
            flush_ops_threshold,
            connect_timeout,
            command_timeout,
            command_pool: Arc::new(runtime::PgConnectionPool::new(
                pg.url.clone(),
                connect_timeout,
                connect_timeout,
                pool_size,
            )?),
            inflight_replication_ops: AtomicU32::new(0),
            pending_state: Mutex::new(PendingReplicationState::default()),
            remote_runtime: runtime::PgRemoteRuntime::new("studiofs-pg-remote-runtime")?,
            allocator_runtime: Mutex::new(inode_allocator::AllocatorRuntime::default()),
            allocator_refill_lock: Mutex::new(()),
            inode_id_headroom_available: AtomicBool::new(false),
            object_id_headroom_available: AtomicBool::new(false),
            pg_sync_lifecycle: AtomicU8::new(PgSyncLifecycle::LocalReady as u8),
            janitor_lock: Mutex::new(None),
            file_lock_cache: Mutex::new(HashMap::new()),
            held_file_locks: Mutex::new(HashMap::new()),
            active_open_inodes: Mutex::new(HashMap::new()),
            self_weak: OnceLock::new(),
            lock_workers_enabled: options.background_poll,
            lock_worker_start_lock: Mutex::new(()),
            background_workers: PgBackgroundWorkers::new(),
            metadata_invalidation_notify,
            metadata_notify_received: AtomicU64::new(0),
            metadata_notify_reconnects: AtomicU64::new(0),
            metadata_fallback_poll_discoveries: AtomicU64::new(0),
            metadata_sync_errors: AtomicU64::new(0),
            metadata_last_successful_sync_unix_ms: AtomicI64::new(0),
            metadata_remote_head_version: AtomicI64::new(-1),
            metadata_source_connected: AtomicBool::new(false),
            remote_format_validated: AtomicBool::new(remote_format_validated),
            remote_format_validation_lock: Mutex::new(()),
        });
        let _ = engine.self_weak.set(Arc::downgrade(&engine));

        if repair_local_graph_recovery_pending || fresh_client_pending {
            engine.transition_pg_sync_lifecycle(PgSyncLifecycle::CatchingUp);
        }

        // CONTEXT(SFS-698): Transitional startup path. SFS-695 defines local
        // serving readiness before remote replay; SFS-698 will move this
        // bootstrap/catch-up work behind that boundary.
        if repair_local_graph_recovery_pending {
            engine.pull_snapshot_locked(
                StartupSnapshotImportMode::ReplaceLocalStatePreservingEventLog,
            )?;
            engine.sync_metadata_to_current_head()?;
            metadata_sync_succeeded = true;
            engine.restore_pending_state_from_local_log()?;
            engine.restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair()?;
            engine
                .local
                .set_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 0)?;
        } else {
            // CONTEXT(SFS-698): Preserve unflushed local overlays while the
            // transitional startup path fast-forwards from the latest remote
            // checkpoint. The canonical lifecycle is documented in
            // `docs/pg-sync-contract.md`.
            if let Some(import_mode) = startup_snapshot_import_mode
                && let Err(error) = engine.pull_snapshot_locked(import_mode)
            {
                warn!(
                    error = %error,
                    "postgres metadata startup snapshot pull failed; continuing from local state"
                );
            }
            metadata_sync_succeeded = if let Err(error) = engine.sync_metadata_to_current_head() {
                warn!(
                    error = %error,
                    "postgres metadata startup catch-up did not reach the current remote head"
                );
                false
            } else {
                true
            };
            engine.restore_pending_state_from_local_log()?;
        }
        if repair_local_graph_recovery_pending && metadata_sync_succeeded {
            engine.transition_pg_sync_lifecycle(PgSyncLifecycle::TargetReached);
        }
        if fresh_client_pending {
            if metadata_sync_succeeded {
                engine.transition_pg_sync_lifecycle(PgSyncLifecycle::TargetReached);
                engine
                    .local
                    .set_counter(LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 0)?;
            } else {
                engine.transition_pg_sync_lifecycle(PgSyncLifecycle::Degraded);
            }
        }
        if metadata_sync_succeeded && let Err(error) = engine.backfill_current_reference_ledger() {
            warn!(
                error = %error,
                "postgres snapshot reachability bootstrap did not complete; janitor will remain conservative"
            );
        }
        engine.cache_client_slot(client_slot);
        engine.bootstrap_allocator_state();
        if options.background_poll {
            engine.ensure_metadata_notify_listener_started()?;
            let handle = Self::start_background_poll(
                Arc::downgrade(&engine),
                engine.poll_interval,
                engine.background_workers.poll_shutdown(),
                engine.background_workers.poll_wakeup(),
            )?;
            engine
                .background_workers
                .install_handle(PgBackgroundWorker::Poll, handle)?;
        }
        Ok(engine)
    }

    /// Return the configured PostgreSQL DSN for this engine instance.
    pub fn postgres_url(&self) -> &str {
        &self.postgres_url
    }

    /// Opens snapshot capabilities without opening the volume's local redb
    /// metadata database.
    pub fn open_snapshot_backend(
        postgres_url: &str,
        volume_id: i64,
    ) -> MetaResult<Arc<PgSnapshotBackend>> {
        PgSnapshotBackend::open(postgres_url, volume_id)
    }

    pub(crate) fn snapshot_backend(&self) -> Arc<PgSnapshotBackend> {
        PgSnapshotBackend::from_engine(self)
    }

    fn fetch_remote_uuid(&self, volume_id: i64) -> MetaResult<String> {
        let command_pool = self.command_pool.clone();
        let command_timeout = self.command_timeout;
        self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "fetch volume uuid",
                move |client| {
                    Box::pin(async move {
                        let row = client
                            .query_opt("SELECT uuid FROM volumes WHERE vol = $1", &[&volume_id])
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error("postgres volume uuid query failed", e)
                            })?;
                        let row = row.ok_or_else(|| {
                            MetaError::Storage(format!(
                                "volume id {} not found in postgres",
                                volume_id
                            ))
                        })?;
                        Ok(row.get::<_, String>(0))
                    })
                },
            )
            .await
        })
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    fn mutate<T, F>(&self, f: F) -> MetaResult<T>
    where
        F: FnOnce(&RedbMetaEngine) -> MetaResult<T>,
    {
        // Serving-state replay reads a local event prefix and writes it back to
        // redb. Keep the local mutation outside that read/apply window so the
        // replay cannot overwrite a mutation it did not observe.
        let pending_guard = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        // `f` is expected to execute one local redb operation that is already
        // atomic at the engine layer (single redb write txn where applicable).
        // This wrapper only tracks local event-seq growth for replication.
        let before_seq = self.local.get_counter("nextEventSeq")?;
        let result = f(&self.local)?;
        let after_seq = self.local.get_counter("nextEventSeq")?;
        drop(pending_guard);
        if self.mark_pending_after_mutation(before_seq, after_seq)? {
            self.signal_poll_wakeup();
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::engine::{
    MetaEngineCore, MetaFsOps, MetaGc, MetaNamespace, MetaPayloadObjects, MetaReplication,
    MetaSnapshots, MetaSymlink, MetaXattr,
};
use crate::engine::{MetaError, MetaFormat, MetaInodes, MetaResult};
#[cfg(test)]
use crate::events::MetaEvent;
use crate::redb_engine::RedbMetaEngine;
#[cfg(test)]
use crate::redb_engine::RedbSnapshot;
use crate::types::*;
use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::warn;

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
    let previous = TEST_ENV_OVERRIDES
        .with(|overrides| overrides.borrow_mut().insert(key, Some(value.to_string())));
    TestEnvGuard { key, previous }
}

const LOCAL_PG_VERSION_COUNTER: &str = "pgLastAppliedBatchVersion";
const LOCAL_PG_NAMESPACE_VERSION_COUNTER: &str = "pgLastAppliedNamespaceBatchVersion";
const LOCAL_PG_CLIENT_SLOT_COUNTER: &str = "pgClientSlotPlusOne";
const LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER: &str = "pgLastFlushedLocalEventSeq";
const LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER: &str = "pgInodeActiveRangeStart";
const LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER: &str = "pgInodeActiveRangeEnd";
const LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER: &str = "pgInodeReservedRangeEnd";
const LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER: &str = "pgInodeCurrentWindowStart";
const LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER: &str = "pgInodeCurrentWindowEnd";
const LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER: &str = "pgInodePrefetchedWindowStart";
const LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER: &str = "pgInodePrefetchedWindowNext";
const LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER: &str = "pgInodePrefetchedWindowEnd";
const PG_NEXT_CLIENT_SLOT_COUNTER: &str = "next_client_slot";
const PG_NEXT_INODE_COUNTER: &str = "next_inode";
const DEFAULT_PG_FLUSH_INTERVAL_SECS: u64 = 60;
const DEFAULT_PG_FLUSH_OPS_THRESHOLD: u32 = 500;
const PG_EVENT_INSERT_CHUNK_SIZE: usize = 512;
const DEFAULT_PG_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PG_COMMAND_TIMEOUT_MS: u64 = 30_000;
const PG_JANITOR_LOCK_SALT: i64 = 0x5f53_4653_5f4a_414e;
const LEGACY_PG_INODE_ID_STRIDE: u64 = 1_000_000_000;
#[cfg(test)]
const LEGACY_PG_INODE_RANGE_WINDOW: u64 = 1_000_000;
const PG_INODE_WINDOW_SIZE: u64 = 250_000;
const PG_INODE_PREFETCH_LOW_WATERMARK: u64 = 50_000;

pub(crate) fn is_local_pg_snapshot_state_counter(name: &str) -> bool {
    matches!(
        name,
        LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER
            | LOCAL_PG_CLIENT_SLOT_COUNTER
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
    inode_allocator_lock: Mutex<()>,
    inode_window_reservation_lock: Mutex<()>,
    inode_id_headroom_available: AtomicBool,
    janitor_lock: Mutex<Option<PgJanitorLock>>,
    file_lock_cache: Mutex<HashMap<Ino, FileLockLease>>,
    held_file_locks: Mutex<HashMap<Ino, HeldFileLockLease>>,
    active_open_inodes: Mutex<HashMap<Ino, u32>>,
    self_weak: OnceLock<Weak<Self>>,
    lock_workers_enabled: bool,
    lock_worker_start_lock: Mutex<()>,
    /// Signals the background poll thread to exit.
    poll_shutdown: Arc<AtomicBool>,
    /// Wakes the background poll thread before the regular polling interval.
    poll_wakeup: Arc<(Mutex<bool>, Condvar)>,
    /// Cancels the lock-notify listener async future immediately, even mid-await.
    lock_notify_cancel: CancellationToken,
    /// Wakes VFS metadata invalidation after PG metadata NOTIFY syncs local state.
    metadata_invalidation_notify: Arc<tokio::sync::Notify>,
    /// Cancels the metadata-notify listener async future immediately.
    metadata_notify_cancel: CancellationToken,
    metadata_notify_received: AtomicU64,
    metadata_notify_reconnects: AtomicU64,
    metadata_fallback_poll_discoveries: AtomicU64,
    metadata_sync_errors: AtomicU64,
    metadata_last_successful_sync_unix_ms: AtomicI64,
    /// Handle to the background poll thread, used by `prepare_for_close()`
    /// to join the thread so `Database::drop()` runs before process exit.
    poll_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    metadata_notify_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    lock_notify_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    lock_renew_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy)]
/// Options that control how a [`PgMetaEngine`] instance is opened.
pub struct PgOpenOptions {
    /// Whether to start background polling and lock-worker threads immediately.
    pub background_poll: bool,
}

impl Default for PgOpenOptions {
    fn default() -> Self {
        Self {
            background_poll: true,
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

#[path = "pg_engine/codec_keys.rs"]
mod codec_keys;
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
#[path = "pg_engine/sql.rs"]
mod sql;
use inode_allocator::LocalIdRangeSpec;
#[cfg(test)]
use inode_allocator::{InodeAllocatorState, InodeWindow, LocalIdRangeState};
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

const LEGACY_INODE_RANGE_SPEC: LocalIdRangeSpec = LocalIdRangeSpec {
    next_counter: counters::NEXT_INODE,
    active_start_counter: LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER,
    active_end_counter: LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER,
    reserved_end_counter: LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER,
    min_value: ROOT_INO + 1,
    slot_stride: LEGACY_PG_INODE_ID_STRIDE,
};

#[derive(Debug, Clone)]
struct HeldFileLockLease {
    lease_ttl: Duration,
    next_renew_at: Instant,
}

impl PgMetaEngine {
    const FILE_LOCK_LEASES_SCHEMA_SQL: &str = r#"
        CREATE TABLE IF NOT EXISTS counters (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            name TEXT NOT NULL,
            value BIGINT NOT NULL,
            PRIMARY KEY (vol, name)
        );

        CREATE TABLE IF NOT EXISTS file_lock_leases (
            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
            ino BIGINT NOT NULL,
            kind SMALLINT NOT NULL,
            exclusive_owner TEXT,
            shared_owners TEXT[] NOT NULL DEFAULT '{}',
            lease_expires_at_unix_ms BIGINT NOT NULL,
            generation BIGINT NOT NULL DEFAULT 1,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (vol, ino)
        );
    "#;
    const LOCK_NOTIFY_IDLE_POLL_INTERVAL_MS: u64 = 250;
    const LOCK_NOTIFY_SAFETY_REFRESH_INTERVAL_MS: u64 = 30_000;
    const METADATA_NOTIFY_IDLE_POLL_INTERVAL_MS: u64 = 250;
    const METADATA_NOTIFY_RECONNECT_MIN_DELAY_MS: u64 = 250;
    const METADATA_NOTIFY_RECONNECT_MAX_DELAY_MS: u64 = 5_000;

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

    fn ensure_file_lock_lease_schema(
        postgres_url: &str,
        connect_timeout: Duration,
        command_timeout: Duration,
    ) -> MetaResult<()> {
        #[cfg(test)]
        if let Some(message) = tests::schema_ensure_test_override() {
            return Err(MetaError::Storage(message));
        }

        let postgres_url = postgres_url.to_string();
        Self::run_async_once(async move {
            Self::with_postgres_client_timeout_once(
                postgres_url,
                connect_timeout,
                command_timeout,
                "ensure file lock lease schema",
                move |client| async move {
                    client
                        .batch_execute("SET client_min_messages TO WARNING")
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock lease schema notice suppression failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    client
                        .batch_execute(Self::FILE_LOCK_LEASES_SCHEMA_SQL)
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock lease schema ensure failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    Ok(())
                },
            )
            .await
        })
    }

    fn is_transient_pg_schema_unavailability(error: &MetaError) -> bool {
        match error {
            MetaError::Storage(message) => {
                !message.contains("sqlstate=")
                    && (message.contains("postgres connect failed:")
                        || message.contains("postgres ensure file lock lease schema timed out")
                        || message.contains("postgres file lock lease schema")
                            && message.contains("timed out"))
            }
            _ => false,
        }
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
        let info = local.get_format_info()?;
        if info.metadata.backend != MetadataBackend::Postgres {
            return Err(MetaError::Storage(
                "PgMetaEngine open requested for non-Postgres volume".to_string(),
            ));
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
        let persisted_client_slot = Self::load_persisted_client_slot(&local)?;
        if let Err(error) =
            Self::ensure_file_lock_lease_schema(&pg.url, connect_timeout, command_timeout)
        {
            let can_reopen_offline =
                Self::has_persisted_inode_allocator_state(&local, persisted_client_slot)?
                    && Self::is_transient_pg_schema_unavailability(&error);
            if can_reopen_offline {
                warn!(
                    error = %error,
                    "postgres lock schema unavailable during reopen; continuing from persisted local allocator state"
                );
            } else {
                return Err(error);
            }
        }

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
        let poll_shutdown = Arc::new(AtomicBool::new(false));
        let poll_wakeup = Arc::new((Mutex::new(false), Condvar::new()));
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
            )),
            inflight_replication_ops: AtomicU32::new(0),
            pending_state: Mutex::new(PendingReplicationState::default()),
            remote_runtime: runtime::PgRemoteRuntime::new("studiofs-pg-remote-runtime")?,
            inode_allocator_lock: Mutex::new(()),
            inode_window_reservation_lock: Mutex::new(()),
            // Optimistic: assume headroom is available until proven exhausted.
            // This flag is degraded to `false` only when the persisted local
            // inode/object window state is genuinely exhausted. Starting at
            // `false` caused `allows_new_mutations()` to block every write if
            // `prime_local_id_ranges` failed silently (via `let _ = ...`).
            inode_id_headroom_available: AtomicBool::new(true),
            janitor_lock: Mutex::new(None),
            file_lock_cache: Mutex::new(HashMap::new()),
            held_file_locks: Mutex::new(HashMap::new()),
            active_open_inodes: Mutex::new(HashMap::new()),
            self_weak: OnceLock::new(),
            lock_workers_enabled: options.background_poll,
            lock_worker_start_lock: Mutex::new(()),
            poll_shutdown: poll_shutdown.clone(),
            poll_wakeup: poll_wakeup.clone(),
            lock_notify_cancel: CancellationToken::new(),
            metadata_invalidation_notify: Arc::new(tokio::sync::Notify::new()),
            metadata_notify_cancel: CancellationToken::new(),
            metadata_notify_received: AtomicU64::new(0),
            metadata_notify_reconnects: AtomicU64::new(0),
            metadata_fallback_poll_discoveries: AtomicU64::new(0),
            metadata_sync_errors: AtomicU64::new(0),
            metadata_last_successful_sync_unix_ms: AtomicI64::new(0),
            poll_handle: Mutex::new(None),
            metadata_notify_handle: Mutex::new(None),
            lock_notify_handle: Mutex::new(None),
            lock_renew_handle: Mutex::new(None),
        });
        let _ = engine.self_weak.set(Arc::downgrade(&engine));

        // Bootstrap from the last checkpoint if present, then catch up from
        // append-only event deltas.
        if Self::should_pull_startup_snapshot(&engine.local, restored_local_version)? {
            let _ = engine.pull_snapshot_locked();
        }
        if let Err(error) = engine.sync_metadata_to_current_head() {
            warn!(
                error = %error,
                "postgres metadata startup catch-up did not reach the current remote head"
            );
        }
        let _ = engine.restore_pending_state_from_local_log();
        let _ = engine.prime_local_id_ranges(client_slot);
        if options.background_poll {
            engine.ensure_metadata_notify_listener_started()?;
            let handle = Self::start_background_poll(
                Arc::downgrade(&engine),
                engine.poll_interval,
                poll_shutdown,
                poll_wakeup,
            )?;
            *engine.poll_handle.lock().unwrap_or_else(|p| p.into_inner()) = Some(handle);
        }
        Ok(engine)
    }

    /// Return the configured PostgreSQL DSN for this engine instance.
    pub fn postgres_url(&self) -> &str {
        &self.postgres_url
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
                                MetaError::Storage(format!(
                                    "postgres volume uuid query failed: {}",
                                    Self::format_pg_error(e)
                                ))
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
        // `f` is expected to execute one local redb operation that is already
        // atomic at the engine layer (single redb write txn where applicable).
        // This wrapper only tracks local event-seq growth for replication.
        let before_seq = self.local.get_counter("nextEventSeq")?;
        let result = f(&self.local)?;
        let after_seq = self.local.get_counter("nextEventSeq")?;
        if self.mark_pending_after_mutation(before_seq, after_seq)? {
            self.signal_poll_wakeup();
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests;

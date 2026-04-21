use crate::engine::{
    BatchFlushEntry, BatchFlushResult, DeferredDeleteRecord, MetaAcl, MetaBlocks, MetaEngineCore,
    MetaError, MetaFormat, MetaFsOps, MetaGc, MetaInodes, MetaNamespace, MetaRecovery,
    MetaReplication, MetaResult, MetaSnapshots, MetaSymlink, MetaXattr, MetadataAuthorityStatus,
    PendingCreateRecord, UndoLogKind, UndoLogRecord,
};
use crate::events::MetaEvent;
use crate::redb_engine::{RedbMetaEngine, RedbSnapshot};
use crate::types::*;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::RuntimeFlavor;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const LOCAL_PG_VERSION_COUNTER: &str = "pgLastAppliedBatchVersion";
const LOCAL_PG_NAMESPACE_VERSION_COUNTER: &str = "pgLastAppliedNamespaceBatchVersion";
const LOCAL_PG_CLIENT_SLOT_COUNTER: &str = "pgClientSlotPlusOne";
const LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER: &str = "pgLastFlushedLocalEventSeq";
const LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER: &str = "pgInodeActiveRangeStart";
const LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER: &str = "pgInodeActiveRangeEnd";
const LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER: &str = "pgInodeReservedRangeEnd";
const LOCAL_PG_BLOCK_ACTIVE_RANGE_START_COUNTER: &str = "pgBlockActiveRangeStart";
const LOCAL_PG_BLOCK_ACTIVE_RANGE_END_COUNTER: &str = "pgBlockActiveRangeEnd";
const LOCAL_PG_BLOCK_RESERVED_RANGE_END_COUNTER: &str = "pgBlockReservedRangeEnd";
const PG_NEXT_CLIENT_SLOT_COUNTER: &str = "next_client_slot";
const DEFAULT_PG_FLUSH_INTERVAL_SECS: u64 = 60;
const DEFAULT_PG_FLUSH_OPS_THRESHOLD: u32 = 500;
const PG_EVENT_INSERT_CHUNK_SIZE: usize = 512;
const DEFAULT_PG_CONNECT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_PG_COMMAND_TIMEOUT_MS: u64 = 30_000;
const PG_JANITOR_LOCK_SALT: i64 = 0x5f53_4653_5f4a_414e;
const PG_INODE_ID_STRIDE: u64 = 1_000_000_000;
const PG_BLOCK_ID_STRIDE: u64 = 10_000_000_000;
const PG_INODE_RANGE_WINDOW: u64 = 1_000_000;
const PG_BLOCK_RANGE_WINDOW: u64 = 10_000_000;
const LOCAL_ID_LEASE_WINDOWS: u64 = 1;

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
    replication_lock: Mutex<()>,
    poll_interval: Duration,
    flush_interval: Duration,
    flush_idle_grace: Duration,
    flush_ops_threshold: u32,
    connect_timeout: Duration,
    command_timeout: Duration,
    inflight_replication_ops: AtomicU32,
    pending_state: Mutex<PendingReplicationState>,
    inode_id_cache: Mutex<LocalIdLeaseCache>,
    block_id_cache: Mutex<LocalIdLeaseCache>,
    inode_id_headroom_available: AtomicBool,
    block_id_headroom_available: AtomicBool,
    janitor_lock: Mutex<Option<PgJanitorLock>>,
    file_lock_cache: Mutex<HashMap<Ino, FileLockLease>>,
    held_file_locks: Mutex<HashMap<Ino, HeldFileLockLease>>,
    self_weak: OnceLock<Weak<Self>>,
    lock_workers_enabled: bool,
    lock_worker_start_lock: Mutex<()>,
    /// Signals the background poll thread to exit.
    poll_shutdown: Arc<AtomicBool>,
    /// Cancels the lock-notify listener async future immediately, even mid-await.
    lock_notify_cancel: CancellationToken,
    /// Handle to the background poll thread, used by `prepare_for_close()`
    /// to join the thread so `Database::drop()` runs before process exit.
    poll_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    lock_notify_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    lock_renew_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct PgOpenOptions {
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

#[derive(Clone, Default)]
struct OverlayConflictSets {
    block_inodes: HashSet<Ino>,
    attr_inodes: HashSet<Ino>,
    dentry_keys: HashSet<Vec<u8>>,
    symlink_inodes: HashSet<Ino>,
    xattr_inodes: HashSet<Ino>,
    xattr_keys: HashSet<Vec<u8>>,
    acl_keys: HashSet<Vec<u8>>,
}

struct RecentFlush {
    batch_version: i64,
    overlay: OverlayConflictSets,
}

#[derive(Clone, Copy)]
struct LocalIdRangeSpec {
    kind_label: &'static str,
    next_counter: &'static str,
    active_start_counter: &'static str,
    active_end_counter: &'static str,
    reserved_end_counter: &'static str,
    min_value: u64,
    slot_stride: u64,
    window_size: u64,
}

#[derive(Debug, Clone, Copy)]
struct LocalIdRangeState {
    next: u64,
    active_start: u64,
    active_end: u64,
    reserved_end: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LocalIdLeaseCache {
    next: u64,
    leased_end: u64,
    reserved_end: u64,
}

impl LocalIdLeaseCache {
    const fn from_persisted(state: LocalIdRangeState) -> Self {
        Self {
            next: state.next,
            leased_end: state.next,
            reserved_end: state.reserved_end,
        }
    }

    const fn has_headroom(self) -> bool {
        self.next < self.reserved_end
    }
}

const INODE_ID_RANGE_SPEC: LocalIdRangeSpec = LocalIdRangeSpec {
    kind_label: "inode",
    next_counter: counters::NEXT_INODE,
    active_start_counter: LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER,
    active_end_counter: LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER,
    reserved_end_counter: LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER,
    min_value: ROOT_INO + 1,
    slot_stride: PG_INODE_ID_STRIDE,
    window_size: PG_INODE_RANGE_WINDOW,
};

const BLOCK_ID_RANGE_SPEC: LocalIdRangeSpec = LocalIdRangeSpec {
    kind_label: "block",
    next_counter: counters::NEXT_BLOCK_ID,
    active_start_counter: LOCAL_PG_BLOCK_ACTIVE_RANGE_START_COUNTER,
    active_end_counter: LOCAL_PG_BLOCK_ACTIVE_RANGE_END_COUNTER,
    reserved_end_counter: LOCAL_PG_BLOCK_RESERVED_RANGE_END_COUNTER,
    min_value: 1,
    slot_stride: PG_BLOCK_ID_STRIDE,
    window_size: PG_BLOCK_RANGE_WINDOW,
};

struct PgJanitorLock {
    _runtime: tokio::runtime::Runtime,
    _client: tokio_postgres::Client,
    connection_task: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone)]
struct HeldFileLockLease {
    lease_ttl: Duration,
    next_renew_at: Instant,
}

impl PgMetaEngine {
    const FILE_LOCK_LEASES_SCHEMA_SQL: &str = r#"
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

    fn env_duration_ms(key: &str, default_ms: u64) -> Duration {
        Duration::from_millis(
            std::env::var(key)
                .ok()
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

    fn default_prepare_for_close_join_timeout() -> Duration {
        Self::env_duration_ms("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", 2_000)
    }

    fn join_background_thread_with_timeout(
        handle: std::thread::JoinHandle<()>,
        timeout: Duration,
        worker: &'static str,
    ) {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let _ = std::thread::Builder::new()
            .name(format!("pg-close-join-{worker}"))
            .spawn(move || {
                let _ = tx.send(handle.join());
            });

        match rx.recv_timeout(timeout) {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                warn!(worker, "pg background thread panicked during shutdown join");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                warn!(
                    worker,
                    timeout_ms = timeout.as_millis() as u64,
                    "pg background thread did not stop before shutdown join timeout"
                );
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                warn!(
                    worker,
                    "pg background thread join helper disconnected unexpectedly"
                );
            }
        }
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

        Self::run_async_once(async move {
            Self::with_postgres_client_timeout(
                postgres_url.to_string(),
                connect_timeout,
                command_timeout,
                "ensure file lock lease schema",
                move |client| async move {
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

    fn format_pg_error(e: tokio_postgres::Error) -> String {
        if let Some(db) = e.as_db_error() {
            format!("{} (sqlstate={})", db.message(), db.code().code())
        } else {
            e.to_string()
        }
    }

    fn pg_timeout_error(context: &str, timeout: Duration) -> MetaError {
        MetaError::Storage(format!(
            "postgres {context} timed out after {} ms",
            timeout.as_millis()
        ))
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

    fn load_persisted_local_version(local: &RedbMetaEngine) -> MetaResult<i64> {
        let version = local.get_counter(LOCAL_PG_VERSION_COUNTER)?;
        Ok(version.min(i64::MAX as u64) as i64)
    }

    fn load_persisted_namespace_version(local: &RedbMetaEngine) -> MetaResult<i64> {
        let version = local.get_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER)?;
        Ok(version.min(i64::MAX as u64) as i64)
    }

    fn persist_local_version(&self, version: i64) -> MetaResult<()> {
        if version < 0 {
            return Ok(());
        }
        self.local
            .set_counter(LOCAL_PG_VERSION_COUNTER, version as u64)?;
        self.local_version.store(version, Ordering::SeqCst);
        Ok(())
    }

    fn persist_namespace_version(&self, version: i64) -> MetaResult<()> {
        if version < 0 {
            return Ok(());
        }
        self.local
            .set_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER, version as u64)?;
        self.namespace_version.store(version, Ordering::SeqCst);
        Ok(())
    }

    fn load_persisted_flushed_event_seq(local: &RedbMetaEngine) -> MetaResult<u64> {
        local.get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER)
    }

    fn persist_flushed_event_seq(&self, seq: u64) -> MetaResult<()> {
        self.local
            .set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, seq)?;
        self.flushed_event_seq.store(seq, Ordering::SeqCst);
        Ok(())
    }

    fn load_persisted_client_slot(local: &RedbMetaEngine) -> MetaResult<Option<u64>> {
        let stored = local.get_counter(LOCAL_PG_CLIENT_SLOT_COUNTER)?;
        if stored == 0 {
            Ok(None)
        } else {
            Ok(Some(stored - 1))
        }
    }

    fn persist_client_slot(local: &RedbMetaEngine, slot: u64) -> MetaResult<()> {
        local.set_counter(
            LOCAL_PG_CLIENT_SLOT_COUNTER,
            slot.checked_add(1)
                .ok_or_else(|| MetaError::Storage("pg client slot overflow".to_string()))?,
        )
    }

    fn cache_client_slot(&self, slot: u64) {
        self.client_slot_plus_one
            .store(slot.saturating_add(1), Ordering::Release);
    }

    fn client_slot(&self) -> MetaResult<u64> {
        let cached = self.client_slot_plus_one.load(Ordering::Acquire);
        if cached != 0 {
            return Ok(cached - 1);
        }
        let slot = Self::load_persisted_client_slot(&self.local)?
            .ok_or_else(|| MetaError::Storage("missing persisted pg client slot".to_string()))?;
        self.cache_client_slot(slot);
        Ok(slot)
    }

    fn has_any_local_overlay_mutation(local: &RedbMetaEngine) -> MetaResult<bool> {
        Ok(!local.local_overlay_inodes()?.is_empty()
            || !local.local_overlay_attr_inodes()?.is_empty()
            || !local.local_overlay_dentries()?.is_empty()
            || !local.local_overlay_symlink_inodes()?.is_empty()
            || !local.local_overlay_xattr_keys()?.is_empty())
    }

    fn should_pull_startup_snapshot(
        local: &RedbMetaEngine,
        persisted_version: i64,
    ) -> MetaResult<bool> {
        if persisted_version > 0 {
            return Ok(false);
        }
        if Self::has_any_local_overlay_mutation(local)? {
            return Ok(false);
        }
        Ok(true)
    }

    fn reserve_counter_range_base(slot: u64, stride: u64, min_value: u64) -> u64 {
        let base = slot.saturating_mul(stride);
        base.max(min_value)
    }

    fn counter_range_limit(slot: u64, spec: LocalIdRangeSpec) -> u64 {
        Self::reserve_counter_range_base(slot.saturating_add(1), spec.slot_stride, spec.min_value)
    }

    fn extend_reserved_range_end(slot: u64, spec: LocalIdRangeSpec, current_end: u64) -> u64 {
        let limit = Self::counter_range_limit(slot, spec);
        current_end.saturating_add(spec.window_size).min(limit)
    }

    fn initial_local_id_range_state(
        slot: u64,
        spec: LocalIdRangeSpec,
        next_hint: u64,
    ) -> LocalIdRangeState {
        let base = Self::reserve_counter_range_base(slot, spec.slot_stride, spec.min_value);
        let limit = Self::counter_range_limit(slot, spec);
        let next = next_hint.clamp(base, limit);
        let offset = next.saturating_sub(base);
        let active_start = base
            .saturating_add((offset / spec.window_size).saturating_mul(spec.window_size))
            .min(limit);
        let active_end = active_start.saturating_add(spec.window_size).min(limit);
        let reserved_end = active_end.saturating_add(spec.window_size).min(limit);
        LocalIdRangeState {
            next,
            active_start,
            active_end,
            reserved_end,
        }
    }

    fn load_persisted_local_id_range_state(
        local: &RedbMetaEngine,
        spec: LocalIdRangeSpec,
    ) -> MetaResult<Option<LocalIdRangeState>> {
        let active_start = local.get_counter(spec.active_start_counter)?;
        let active_end = local.get_counter(spec.active_end_counter)?;
        let reserved_end = local.get_counter(spec.reserved_end_counter)?;
        if active_start == 0 || active_end == 0 || reserved_end == 0 {
            return Ok(None);
        }
        Ok(Some(LocalIdRangeState {
            next: local.get_counter(spec.next_counter)?,
            active_start,
            active_end,
            reserved_end,
        }))
    }

    fn load_valid_persisted_local_id_range_state(
        local: &RedbMetaEngine,
        slot: u64,
        spec: LocalIdRangeSpec,
    ) -> MetaResult<Option<LocalIdRangeState>> {
        Ok(Self::load_persisted_local_id_range_state(local, spec)?
            .filter(|state| Self::local_id_range_state_is_valid(slot, spec, *state)))
    }

    fn local_id_range_state_is_valid(
        slot: u64,
        spec: LocalIdRangeSpec,
        state: LocalIdRangeState,
    ) -> bool {
        let base = Self::reserve_counter_range_base(slot, spec.slot_stride, spec.min_value);
        let limit = Self::counter_range_limit(slot, spec);
        state.active_start >= base
            && state.active_start <= state.active_end
            && state.active_end <= state.reserved_end
            && state.reserved_end <= limit
            && state.next >= state.active_start
            && state.next <= state.reserved_end
    }

    fn normalize_local_id_range_state(
        slot: u64,
        spec: LocalIdRangeSpec,
        mut state: LocalIdRangeState,
    ) -> LocalIdRangeState {
        let limit = Self::counter_range_limit(slot, spec);
        if state.active_start < limit {
            state.active_end = state
                .active_end
                .max(state.active_start.saturating_add(1).min(limit))
                .min(limit);
        } else {
            state.active_end = limit;
        }
        state.reserved_end = state.reserved_end.max(state.active_end).min(limit);
        if state.next < state.active_start {
            state.next = state.active_start;
        }
        state
    }

    fn persist_local_id_range_state_locked(
        counters: &crate::redb_engine::CounterWriteTransaction<'_>,
        spec: LocalIdRangeSpec,
        state: LocalIdRangeState,
    ) -> MetaResult<()> {
        counters.set(spec.next_counter, state.next)?;
        counters.set(spec.active_start_counter, state.active_start)?;
        counters.set(spec.active_end_counter, state.active_end)?;
        counters.set(spec.reserved_end_counter, state.reserved_end)?;
        Ok(())
    }

    fn read_or_initialize_local_id_range_locked(
        counters: &crate::redb_engine::CounterWriteTransaction<'_>,
        slot: u64,
        spec: LocalIdRangeSpec,
    ) -> MetaResult<LocalIdRangeState> {
        let persisted = LocalIdRangeState {
            next: counters.get(spec.next_counter)?,
            active_start: counters.get(spec.active_start_counter)?,
            active_end: counters.get(spec.active_end_counter)?,
            reserved_end: counters.get(spec.reserved_end_counter)?,
        };
        let mut state = if persisted.active_start == 0
            || persisted.active_end == 0
            || persisted.reserved_end == 0
            || !Self::local_id_range_state_is_valid(slot, spec, persisted)
        {
            Self::initial_local_id_range_state(slot, spec, counters.get(spec.next_counter)?)
        } else {
            persisted
        };
        state = Self::normalize_local_id_range_state(slot, spec, state);
        Self::persist_local_id_range_state_locked(counters, spec, state)?;
        Ok(state)
    }

    fn advance_active_range(
        slot: u64,
        spec: LocalIdRangeSpec,
        mut state: LocalIdRangeState,
    ) -> LocalIdRangeState {
        if state.reserved_end <= state.active_end {
            return state;
        }
        state.active_start = state.active_end;
        state.active_end = state.reserved_end;
        Self::normalize_local_id_range_state(slot, spec, state)
    }

    fn has_persisted_local_id_headroom(local: &RedbMetaEngine, slot: u64) -> MetaResult<bool> {
        Ok(
            Self::load_valid_persisted_local_id_range_state(local, slot, INODE_ID_RANGE_SPEC)?
                .is_some()
                && Self::load_valid_persisted_local_id_range_state(
                    local,
                    slot,
                    BLOCK_ID_RANGE_SPEC,
                )?
                .is_some(),
        )
    }

    fn load_or_initialize_local_id_range_state(
        local: &RedbMetaEngine,
        slot: u64,
        spec: LocalIdRangeSpec,
    ) -> MetaResult<LocalIdRangeState> {
        local.write_counters(|counters| {
            Self::read_or_initialize_local_id_range_locked(counters, slot, spec)
        })
    }

    fn sync_local_id_lease_cache(
        &self,
        slot: u64,
        spec: LocalIdRangeSpec,
        cache: &Mutex<LocalIdLeaseCache>,
        headroom: &AtomicBool,
    ) -> MetaResult<()> {
        let state = Self::load_or_initialize_local_id_range_state(&self.local, slot, spec)?;
        let snapshot = LocalIdLeaseCache::from_persisted(state);
        let mut guard = cache
            .lock()
            .map_err(|_| MetaError::Storage("local id cache poisoned".to_string()))?;
        *guard = snapshot;
        headroom.store(snapshot.has_headroom(), Ordering::Release);
        Ok(())
    }

    fn ensure_local_id_headroom(&self, slot: u64) -> MetaResult<()> {
        self.local.write_counters(|counters| {
            let _ = Self::read_or_initialize_local_id_range_locked(
                counters,
                slot,
                INODE_ID_RANGE_SPEC,
            )?;
            let _ = Self::read_or_initialize_local_id_range_locked(
                counters,
                slot,
                BLOCK_ID_RANGE_SPEC,
            )?;
            Ok(())
        })?;
        self.sync_local_id_lease_cache(
            slot,
            INODE_ID_RANGE_SPEC,
            &self.inode_id_cache,
            &self.inode_id_headroom_available,
        )?;
        self.sync_local_id_lease_cache(
            slot,
            BLOCK_ID_RANGE_SPEC,
            &self.block_id_cache,
            &self.block_id_headroom_available,
        )
    }

    fn can_refresh_local_id_headroom(&self) -> MetaResult<bool> {
        #[cfg(test)]
        if let Some(value) = tests::headroom_refresh_test_override() {
            return Ok(value);
        }

        let postgres_url = self.postgres_url.clone();
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        Self::run_async_once(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "refresh local id headroom",
                move |client| async move {
                    client.simple_query("SELECT 1").await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres local id headroom refresh check failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    Ok(true)
                },
            )
            .await
            .or(Ok(false))
        })
    }

    fn reserve_local_id_lease(
        &self,
        slot: u64,
        spec: LocalIdRangeSpec,
        additional_ids: u64,
        pg_available: bool,
    ) -> MetaResult<LocalIdLeaseCache> {
        self.local.write_counters(|counters| {
            let mut state = Self::read_or_initialize_local_id_range_locked(counters, slot, spec)?;
            let lease_start = state.next;
            let minimum_end = lease_start.checked_add(additional_ids).ok_or_else(|| {
                MetaError::Storage(format!("pg {} id allocation overflow", spec.kind_label))
            })?;
            let limit = Self::counter_range_limit(slot, spec);
            if pg_available && state.next >= state.active_end && state.reserved_end < limit {
                state.reserved_end =
                    Self::extend_reserved_range_end(slot, spec, state.reserved_end);
            }
            while pg_available && minimum_end > state.reserved_end && state.reserved_end < limit {
                state.reserved_end =
                    Self::extend_reserved_range_end(slot, spec, state.reserved_end);
            }
            if minimum_end > state.reserved_end {
                return Err(MetaError::Storage(format!(
                    "pg local {} id headroom exhausted",
                    spec.kind_label
                )));
            }

            let lease_span =
                additional_ids.max(spec.window_size.saturating_mul(LOCAL_ID_LEASE_WINDOWS));
            let lease_end = lease_start
                .saturating_add(lease_span)
                .min(state.reserved_end);
            state.next = lease_end;
            while state.next >= state.active_end && state.active_end < state.reserved_end {
                state = Self::advance_active_range(slot, spec, state);
            }
            Self::persist_local_id_range_state_locked(counters, spec, state)?;
            Ok(LocalIdLeaseCache {
                next: lease_start,
                leased_end: lease_end,
                reserved_end: state.reserved_end,
            })
        })
    }

    fn allocate_local_ids(
        &self,
        slot: u64,
        spec: LocalIdRangeSpec,
        cache: &Mutex<LocalIdLeaseCache>,
        headroom: &AtomicBool,
        count: u64,
    ) -> MetaResult<u64> {
        let mut lease = cache
            .lock()
            .map_err(|_| MetaError::Storage("local id cache poisoned".to_string()))?;
        if lease.reserved_end == 0 && lease.leased_end == 0 {
            *lease = LocalIdLeaseCache::from_persisted(
                Self::load_or_initialize_local_id_range_state(&self.local, slot, spec)?,
            );
        }
        let end = lease.next.checked_add(count).ok_or_else(|| {
            MetaError::Storage(format!("pg {} id allocation overflow", spec.kind_label))
        })?;
        if end > lease.leased_end {
            let refreshed = self.reserve_local_id_lease(
                slot,
                spec,
                count,
                self.can_refresh_local_id_headroom()?,
            )?;
            *lease = refreshed;
        }
        let end = lease.next.checked_add(count).ok_or_else(|| {
            MetaError::Storage(format!("pg {} id allocation overflow", spec.kind_label))
        })?;
        if end > lease.reserved_end {
            return Err(MetaError::Storage(format!(
                "pg local {} id headroom exhausted",
                spec.kind_label
            )));
        }
        let first = lease.next;
        lease.next = end;
        headroom.store(lease.has_headroom(), Ordering::Release);
        Ok(first)
    }

    fn run_async_once<T, F>(fut: F) -> MetaResult<T>
    where
        T: Send,
        F: std::future::Future<Output = MetaResult<T>> + Send,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
                }
                RuntimeFlavor::CurrentThread => std::thread::scope(|scope| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(1);
                    scope.spawn(move || {
                        let result = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                MetaError::Storage(format!("failed to build tokio runtime: {e}"))
                            })
                            .and_then(|rt| rt.block_on(fut));
                        let _ = tx.send(result);
                    });
                    rx.recv().map_err(|e| {
                        MetaError::Storage(format!(
                            "failed to receive result from fallback runtime: {e}"
                        ))
                    })?
                }),
                _ => std::thread::scope(|scope| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(1);
                    scope.spawn(move || {
                        let result = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                MetaError::Storage(format!("failed to build tokio runtime: {e}"))
                            })
                            .and_then(|rt| rt.block_on(fut));
                        let _ = tx.send(result);
                    });
                    rx.recv().map_err(|e| {
                        MetaError::Storage(format!(
                            "failed to receive result from fallback runtime: {e}"
                        ))
                    })?
                }),
            };
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        rt.block_on(fut)
    }

    async fn with_postgres_client_timeout<T, F, Fut>(
        postgres_url: String,
        connect_timeout: Duration,
        command_timeout: Duration,
        context: &'static str,
        f: F,
    ) -> MetaResult<T>
    where
        F: FnOnce(tokio_postgres::Client) -> Fut,
        Fut: Future<Output = MetaResult<T>>,
    {
        let connect = tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls);
        let (client, connection) = tokio::time::timeout(connect_timeout, connect)
            .await
            .map_err(|_| Self::pg_timeout_error(&format!("{context} connect"), connect_timeout))?
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres connect failed: {}",
                    Self::format_pg_error(e)
                ))
            })?;

        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        let result = tokio::time::timeout(command_timeout, f(client))
            .await
            .map_err(|_| Self::pg_timeout_error(context, command_timeout));

        conn_task.abort();
        match result {
            Ok(result) => result,
            Err(err) => Err(err),
        }
    }

    fn janitor_lock_key(&self) -> i64 {
        self.volume_id.wrapping_mul(1_000_003) ^ PG_JANITOR_LOCK_SALT
    }

    fn try_acquire_pg_janitor_lock(&self) -> MetaResult<bool> {
        let mut state = self
            .janitor_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg janitor lock state poisoned".to_string()))?;
        if state.is_some() {
            return Ok(false);
        }

        let lock_key = self.janitor_lock_key();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        let connect = tokio_postgres::connect(&self.postgres_url, tokio_postgres::NoTls);
        let (client, connection) = runtime
            .block_on(async { tokio::time::timeout(self.connect_timeout, connect).await })
            .map_err(|_| Self::pg_timeout_error("janitor lock connect", self.connect_timeout))?
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres connect failed: {}",
                    Self::format_pg_error(e)
                ))
            })?;

        let connection_task = runtime.spawn(async move {
            let _ = connection.await;
        });
        let acquired = runtime
            .block_on(async {
                tokio::time::timeout(
                    self.command_timeout,
                    client.query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key]),
                )
                .await
            })
            .map_err(|_| Self::pg_timeout_error("janitor lock acquire", self.command_timeout))?
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres janitor lock acquire failed: {}",
                    Self::format_pg_error(e)
                ))
            })?
            .get::<usize, bool>(0);

        if !acquired {
            connection_task.abort();
            return Ok(false);
        }

        *state = Some(PgJanitorLock {
            _runtime: runtime,
            _client: client,
            connection_task,
        });
        Ok(true)
    }

    fn release_pg_janitor_lock(&self) -> MetaResult<()> {
        let mut state = self
            .janitor_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg janitor lock state poisoned".to_string()))?;
        let Some(lock) = state.take() else {
            return Ok(());
        };
        lock.connection_task.abort();
        Ok(())
    }

    fn lock_notify_channel(volume_id: i64) -> String {
        format!("studiofs_lock_vol_{volume_id}")
    }

    fn encode_file_lock_notify_payload(ino: Ino, generation: u64) -> String {
        format!("{ino}:{generation}")
    }

    fn parse_file_lock_notify_payload(payload: &str) -> Option<Ino> {
        let ino = payload.split(':').next()?.trim().parse::<u64>().ok()?;
        Some(ino)
    }

    fn decode_hard_lock_kind(raw: i16) -> MetaResult<HardLockKind> {
        match raw {
            1 => Ok(HardLockKind::Shared),
            2 => Ok(HardLockKind::Exclusive),
            other => Err(MetaError::Storage(format!(
                "invalid hard lock kind in postgres row: {other}"
            ))),
        }
    }

    fn cache_file_lock_lease(&self, snapshot: Option<FileLockLease>) {
        if let Ok(mut cache) = self.file_lock_cache.lock() {
            if let Some(snapshot) = snapshot {
                cache.insert(snapshot.ino, snapshot);
            }
        }
    }

    fn remove_cached_file_lock_lease(&self, ino: Ino) {
        if let Ok(mut cache) = self.file_lock_cache.lock() {
            cache.remove(&ino);
        }
    }

    fn upsert_cached_file_lock_lease(&self, snapshot: Option<FileLockLease>, ino: Ino) {
        if let Some(snapshot) = snapshot {
            self.cache_file_lock_lease(Some(snapshot));
        } else {
            self.remove_cached_file_lock_lease(ino);
        }
    }

    fn remember_held_file_lock(&self, ino: Ino, lease_ttl: Duration) {
        if let Ok(mut held) = self.held_file_locks.lock() {
            held.insert(
                ino,
                HeldFileLockLease {
                    lease_ttl,
                    next_renew_at: Instant::now() + Self::half_duration(lease_ttl),
                },
            );
        }
    }

    fn forget_held_file_lock(&self, ino: Ino) {
        if let Ok(mut held) = self.held_file_locks.lock() {
            held.remove(&ino);
        }
    }

    fn current_file_lock_lease_from_row(
        row: tokio_postgres::Row,
        now_ms: i64,
    ) -> MetaResult<Option<FileLockLease>> {
        let ino_raw: i64 = row.get(0);
        let kind_raw: i16 = row.get(1);
        let exclusive_owner: Option<String> = row.get(2);
        let shared_owners: Vec<String> = row.get(3);
        let lease_expires_at_unix_ms: i64 = row.get(4);
        let generation_raw: i64 = row.get(5);
        if lease_expires_at_unix_ms <= now_ms {
            return Ok(None);
        }
        let ino = u64::try_from(ino_raw).map_err(|_| {
            MetaError::Storage(format!(
                "invalid negative inode in lock lease row: {ino_raw}"
            ))
        })?;
        let generation = u64::try_from(generation_raw).map_err(|_| {
            MetaError::Storage(format!(
                "invalid negative generation in lock lease row: {generation_raw}"
            ))
        })?;
        Ok(Some(FileLockLease {
            ino,
            kind: Self::decode_hard_lock_kind(kind_raw)?,
            generation,
            lease_expires_at_unix_ms,
            exclusive_owner,
            shared_owners: shared_owners.into_iter().collect(),
        }))
    }

    fn half_duration(duration: Duration) -> Duration {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Duration::from_millis((millis / 2).max(1))
    }

    async fn current_file_lock_lease_with_client<C>(
        client: &C,
        volume_id: i64,
        ino: Ino,
    ) -> MetaResult<Option<FileLockLease>>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        let now_ms = Self::now_millis();
        let row = client
            .query_opt(
                r#"
                SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
                FROM file_lock_leases
                WHERE vol = $1 AND ino = $2
                "#,
                &[&volume_id, &(ino as i64)],
            )
            .await
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres file lock read failed: {}",
                    Self::format_pg_error(e)
                ))
            })?;
        match row {
            Some(row) => Self::current_file_lock_lease_from_row(row, now_ms),
            None => Ok(None),
        }
    }

    async fn refresh_known_file_lock_lease_with_client<C>(
        &self,
        client: &C,
        ino: Ino,
    ) -> MetaResult<Option<FileLockLease>>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        let snapshot =
            Self::current_file_lock_lease_with_client(client, self.volume_id, ino).await?;
        self.upsert_cached_file_lock_lease(snapshot.clone(), ino);
        Ok(snapshot)
    }

    fn ensure_lock_notify_listener_started(&self) -> MetaResult<()> {
        if !self.lock_workers_enabled {
            return Ok(());
        }

        if self
            .lock_notify_handle
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }

        let _start_guard = self
            .lock_worker_start_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg lock worker start state poisoned".to_string()))?;
        let engine = self.self_weak.get().cloned().ok_or_else(|| {
            MetaError::Storage("pg lock worker bootstrap missing self weak ref".to_string())
        })?;

        let mut notify_guard = self
            .lock_notify_handle
            .lock()
            .map_err(|_| MetaError::Storage("pg lock notify handle state poisoned".to_string()))?;
        if notify_guard.is_none() {
            *notify_guard = Some(Self::start_lock_notify_listener(
                engine.clone(),
                self.poll_shutdown.clone(),
                self.lock_notify_cancel.clone(),
            )?);
        }
        drop(notify_guard);

        Ok(())
    }

    fn ensure_lock_renew_worker_started(&self) -> MetaResult<()> {
        if !self.lock_workers_enabled {
            return Ok(());
        }

        if self
            .lock_renew_handle
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }

        let _start_guard = self
            .lock_worker_start_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg lock worker start state poisoned".to_string()))?;
        let engine = self.self_weak.get().cloned().ok_or_else(|| {
            MetaError::Storage("pg lock worker bootstrap missing self weak ref".to_string())
        })?;

        let mut renew_guard = self
            .lock_renew_handle
            .lock()
            .map_err(|_| MetaError::Storage("pg lock renew handle state poisoned".to_string()))?;
        if renew_guard.is_none() {
            *renew_guard = Some(Self::start_lock_renew_worker(
                engine,
                self.poll_shutdown.clone(),
            )?);
        }

        Ok(())
    }

    fn ensure_lock_workers_started(&self) -> MetaResult<()> {
        self.ensure_lock_notify_listener_started()?;
        self.ensure_lock_renew_worker_started()
    }

    fn start_lock_notify_listener(
        engine: Weak<Self>,
        shutdown: Arc<AtomicBool>,
        cancel: CancellationToken,
    ) -> MetaResult<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("pg-lock-notify".into())
            .spawn(move || {
                while !shutdown.load(Ordering::Acquire) {
                    // Upgrade briefly to snapshot the minimal runtime configuration
                    // we need for connecting, then drop the Arc so connect doesn't
                    // hold the engine alive. This avoids keeping Redb/engine Arcs
                    // alive across potentially long network connect attempts.
                    let (postgres_url, volume_id, connect_timeout, safety_refresh_interval) =
                        match engine.upgrade() {
                            Some(engine_arc) => {
                                (
                                    engine_arc.postgres_url.clone(),
                                    engine_arc.volume_id,
                                    engine_arc.connect_timeout,
                                    Self::default_lock_notify_safety_refresh_interval(),
                                )
                            }
                            None => break,
                        };

                    let channel = Self::lock_notify_channel(volume_id);

                    // Run the connect + listen + poll loop inside an async runtime.
                    // All awaitable operations are raced against `cancel` so that
                    // `prepare_for_close` can interrupt them immediately — including
                    // in-flight `refresh_known_file_lock_lease_with_client` calls
                    // that are triggered by file-write notifications.
                    let weak_engine = engine.clone();
                    let shutdown_for_task = shutdown.clone();
                    let cancel_for_task = cancel.clone();
                    let connect_result = (|| -> MetaResult<()> {
                        let fut = async move {
                            // Race connect against cancellation so we don't block
                            // `prepare_for_close` for up to connect_timeout.
                            let connect_result = tokio::select! {
                                biased;
                                _ = cancel_for_task.cancelled() => return Ok(()),
                                r = tokio::time::timeout(
                                    connect_timeout,
                                    tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls),
                                ) => r,
                            };
                            let (client, mut connection) = connect_result
                                .map_err(|_| Self::pg_timeout_error("lock notify connect", connect_timeout))?
                                .map_err(|e| MetaError::Storage(format!(
                                    "postgres lock notify connect failed: {}",
                                    Self::format_pg_error(e)
                                )))?;

                            // After successful connect, try to upgrade the weak ref.
                            let Some(engine_arc) = weak_engine.upgrade() else {
                                // Engine gone: drop client and exit cleanly.
                                return Ok(());
                            };

                            // Race LISTEN setup against cancellation.
                            let listen_cmd = format!("LISTEN {channel}");
                            tokio::select! {
                                biased;
                                _ = cancel_for_task.cancelled() => return Ok(()),
                                r = client.batch_execute(&listen_cmd) => {
                                    r.map_err(|e| MetaError::Storage(format!(
                                        "postgres LISTEN {channel} failed: {}",
                                        Self::format_pg_error(e)
                                    )))?;
                                }
                            }

                            let mut next_safety_refresh = Instant::now() + safety_refresh_interval;
                            while !shutdown_for_task.load(Ordering::Acquire) {
                                // Race the postgres poll against cancellation so that
                                // `prepare_for_close` can exit immediately even while
                                // the thread is blocked waiting for notifications.
                                let message = tokio::select! {
                                    biased;
                                    _ = cancel_for_task.cancelled() => break,
                                    msg = tokio::time::timeout(
                                        Duration::from_millis(Self::LOCK_NOTIFY_IDLE_POLL_INTERVAL_MS),
                                        std::future::poll_fn(|cx| connection.poll_message(cx)),
                                    ) => msg,
                                };
                                match message {
                                    Ok(Some(Ok(tokio_postgres::AsyncMessage::Notification(notification)))) => {
                                        if let Some(ino) = Self::parse_file_lock_notify_payload(notification.payload()) {
                                            // This is the critical path: a file write has been
                                            // notified. Race the postgres refresh against
                                            // cancellation so unmount is not blocked.
                                            tokio::select! {
                                                biased;
                                                _ = cancel_for_task.cancelled() => break,
                                                r = engine_arc.refresh_known_file_lock_lease_with_client(&client, ino) => { r?; }
                                            }
                                        }
                                    }
                                    Ok(Some(Ok(tokio_postgres::AsyncMessage::Notice(_)))) => {}
                                    Ok(Some(Ok(_))) => {}
                                    Ok(Some(Err(err))) => {
                                        return Err(MetaError::Storage(format!(
                                            "postgres lock notify poll failed: {}",
                                            Self::format_pg_error(err)
                                        )));
                                    }
                                    Ok(None) => return Ok(()),
                                    Err(_) => {
                                        if Instant::now() >= next_safety_refresh {
                                            let cached_inos: Vec<Ino> = engine_arc
                                                .file_lock_cache
                                                .lock()
                                                .map(|cache| cache.keys().copied().collect())
                                                .unwrap_or_default();
                                            for ino in cached_inos {
                                                // Also race the safety-refresh calls so they
                                                // can be cancelled cleanly at shutdown.
                                                tokio::select! {
                                                    biased;
                                                    _ = cancel_for_task.cancelled() => return Ok(()),
                                                    r = engine_arc.refresh_known_file_lock_lease_with_client(&client, ino) => { r?; }
                                                }
                                            }
                                            next_safety_refresh = Instant::now() + safety_refresh_interval;
                                        }
                                    }
                                }
                            }
                            Ok(())
                        };

                        // Build a small current-thread runtime to run the future.
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
                        rt.block_on(fut)
                    })();

                    if shutdown.load(Ordering::Acquire) || cancel.is_cancelled() {
                        break;
                    }
                    if connect_result.is_err() {
                        std::thread::sleep(Duration::from_millis(250));
                    }
                }
            })
            .map_err(|e| MetaError::Storage(format!("failed to spawn pg-lock-notify thread: {e}")))
    }

    fn start_lock_renew_worker(
        engine: Weak<Self>,
        shutdown: Arc<AtomicBool>,
    ) -> MetaResult<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("pg-lock-renew".into())
            .spawn(move || {
                while !shutdown.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(50));
                    let Some(engine_arc) = engine.upgrade() else {
                        break;
                    };
                    let now = Instant::now();
                    let due: Vec<(Ino, Duration)> = match engine_arc.held_file_locks.lock() {
                        Ok(held) => held
                            .iter()
                            .filter_map(|(&ino, state)| {
                                (state.next_renew_at <= now).then_some((ino, state.lease_ttl))
                            })
                            .collect(),
                        Err(_) => Vec::new(),
                    };

                    for (ino, lease_ttl) in due {
                        if shutdown.load(Ordering::Acquire) {
                            break;
                        }
                        let result = engine_arc.renew_file_lock_lease(ino, lease_ttl);
                        match result {
                            Ok(FileLockLeaseRenewOutcome::Renewed(_)) => {
                                if let Ok(mut held) = engine_arc.held_file_locks.lock()
                                    && let Some(state) = held.get_mut(&ino)
                                {
                                    state.next_renew_at =
                                        Instant::now() + Self::half_duration(lease_ttl);
                                }
                            }
                            Ok(FileLockLeaseRenewOutcome::Lost(_)) => {
                                engine_arc.forget_held_file_lock(ino);
                            }
                            Err(_) => {
                                if let Ok(mut held) = engine_arc.held_file_locks.lock()
                                    && let Some(state) = held.get_mut(&ino)
                                {
                                    state.next_renew_at = Instant::now()
                                        + Self::half_duration(
                                            lease_ttl.min(Duration::from_secs(1)),
                                        );
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|e| MetaError::Storage(format!("failed to spawn pg-lock-renew thread: {e}")))
    }

    fn load_pg_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "janitor snapshot pin query",
                move |client| async move {
                    client
                        .query(
                            "SELECT pinned_object_ids FROM snapshots WHERE vol = $1",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres janitor snapshot pin query failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })
                },
            )
            .await
        })?;

        let mut pins = HashSet::new();
        for row in rows {
            let raw_ids: Vec<i64> = row.get(0);
            for object_id in raw_ids {
                let object_id = u64::try_from(object_id).map_err(|_| {
                    MetaError::Storage(format!(
                        "snapshot pinned object id must be non-negative: {object_id}"
                    ))
                })?;
                pins.insert(object_id);
            }
        }
        Ok(pins)
    }

    fn reserve_global_client_slot(postgres_url: &str, volume_id: i64) -> MetaResult<u64> {
        let postgres_url = postgres_url.to_string();
        let connect_timeout = Self::default_connect_timeout();
        let command_timeout = Self::default_command_timeout();
        Self::run_async_once(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "reserve client slot",
                move |client| async move {
                    let mut client = client;
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres tx failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    tx.batch_execute(
                        r#"
                        CREATE TABLE IF NOT EXISTS counters (
                            vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                            name TEXT NOT NULL,
                            value BIGINT NOT NULL,
                            PRIMARY KEY (vol, name)
                        )
                        "#,
                    )
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres counters ensure failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    tx.execute(
                        r#"
                        INSERT INTO counters (vol, name, value)
                        VALUES ($1, $2, 0)
                        ON CONFLICT (vol, name) DO NOTHING
                        "#,
                        &[&volume_id, &PG_NEXT_CLIENT_SLOT_COUNTER],
                    )
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres slot seed failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let row = tx
                        .query_one(
                            r#"
                            UPDATE counters
                            SET value = value + 1
                            WHERE vol = $1 AND name = $2
                            RETURNING value - 1
                            "#,
                            &[&volume_id, &PG_NEXT_CLIENT_SLOT_COUNTER],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres slot reserve failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    tx.commit().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres slot commit failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;

                    let slot: i64 = row.get(0);
                    u64::try_from(slot).map_err(|_| {
                        MetaError::Storage(format!("invalid negative pg client slot: {slot}"))
                    })
                },
            )
            .await
        })
    }

    fn ensure_client_slot(
        local: &RedbMetaEngine,
        postgres_url: &str,
        volume_id: i64,
    ) -> MetaResult<u64> {
        if let Some(slot) = Self::load_persisted_client_slot(local)? {
            return Ok(slot);
        }
        let slot = Self::reserve_global_client_slot(postgres_url, volume_id)?;
        Self::persist_client_slot(local, slot)?;
        Ok(slot)
    }

    fn prime_local_id_ranges(&self, slot: u64) -> MetaResult<()> {
        self.cache_client_slot(slot);
        self.ensure_local_id_headroom(slot)?;
        // If persisted state already has no headroom (next >= reserved_end from a
        // previous session), extend it now so that `allows_new_mutations()` does not
        // block every write on startup.  Without this, the engine stays read-only
        // even when PG is healthy and the slot is not globally exhausted.
        let pg_available = self.can_refresh_local_id_headroom().unwrap_or(false);
        if pg_available {
            if !self.inode_id_headroom_available.load(Ordering::Acquire) {
                if let Ok(new_lease) =
                    self.reserve_local_id_lease(slot, INODE_ID_RANGE_SPEC, 0, true)
                {
                    *self
                        .inode_id_cache
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = new_lease;
                    self.inode_id_headroom_available
                        .store(new_lease.has_headroom(), Ordering::Release);
                }
            }
            if !self.block_id_headroom_available.load(Ordering::Acquire) {
                if let Ok(new_lease) =
                    self.reserve_local_id_lease(slot, BLOCK_ID_RANGE_SPEC, 0, true)
                {
                    *self
                        .block_id_cache
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = new_lease;
                    self.block_id_headroom_available
                        .store(new_lease.has_headroom(), Ordering::Release);
                }
            }
        }
        Ok(())
    }

    pub fn open(path: impl AsRef<Path>) -> MetaResult<Arc<Self>> {
        Self::open_with_options(path, PgOpenOptions::default())
    }

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

        let poll_secs = std::env::var("STUDIOFS_PG_SYNC_POLL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2);
        let flush_idle_grace = Duration::from_millis(
            std::env::var("STUDIOFS_PG_FLUSH_IDLE_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(5_000),
        );
        let flush_interval = Duration::from_secs(
            std::env::var("STUDIOFS_PG_FLUSH_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_PG_FLUSH_INTERVAL_SECS),
        );
        let flush_ops_threshold = std::env::var("STUDIOFS_PG_FLUSH_OPS_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_PG_FLUSH_OPS_THRESHOLD);
        let connect_timeout = Self::default_connect_timeout();
        let command_timeout = Self::default_command_timeout();
        let persisted_client_slot = Self::load_persisted_client_slot(&local)?;
        if let Err(error) =
            Self::ensure_file_lock_lease_schema(&pg.url, connect_timeout, command_timeout)
        {
            let can_reopen_offline = persisted_client_slot
                .map(|slot| Self::has_persisted_local_id_headroom(&local, slot))
                .transpose()?
                .unwrap_or(false)
                && Self::is_transient_pg_schema_unavailability(&error);
            if can_reopen_offline {
                warn!(
                    error = %error,
                    "postgres lock schema unavailable during reopen; continuing from persisted client slot"
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
        let engine = Arc::new(Self {
            local,
            postgres_url: pg.url,
            volume_id: pg.volume_id,
            client_id: format!("client-slot:{client_slot}"),
            client_slot_plus_one: AtomicU64::new(client_slot.saturating_add(1)),
            local_version: AtomicI64::new(restored_local_version),
            namespace_version: AtomicI64::new(restored_namespace_version),
            flushed_event_seq: AtomicU64::new(restored_flushed_event_seq),
            replication_lock: Mutex::new(()),
            poll_interval: Duration::from_secs(poll_secs),
            flush_interval,
            flush_idle_grace,
            flush_ops_threshold,
            connect_timeout,
            command_timeout,
            inflight_replication_ops: AtomicU32::new(0),
            pending_state: Mutex::new(PendingReplicationState::default()),
            inode_id_cache: Mutex::new(LocalIdLeaseCache::default()),
            block_id_cache: Mutex::new(LocalIdLeaseCache::default()),
            // Optimistic: assume headroom is available until proven exhausted.
            // These flags are degraded to `false` by `allocate_local_ids` only
            // when the reserved range is genuinely exhausted.  Starting at
            // `false` caused `allows_new_mutations()` to block every write if
            // `prime_local_id_ranges` failed silently (via `let _ = ...`).
            inode_id_headroom_available: AtomicBool::new(true),
            block_id_headroom_available: AtomicBool::new(true),
            janitor_lock: Mutex::new(None),
            file_lock_cache: Mutex::new(HashMap::new()),
            held_file_locks: Mutex::new(HashMap::new()),
            self_weak: OnceLock::new(),
            lock_workers_enabled: options.background_poll,
            lock_worker_start_lock: Mutex::new(()),
            poll_shutdown: poll_shutdown.clone(),
            lock_notify_cancel: CancellationToken::new(),
            poll_handle: Mutex::new(None),
            lock_notify_handle: Mutex::new(None),
            lock_renew_handle: Mutex::new(None),
        });
        let _ = engine.self_weak.set(Arc::downgrade(&engine));

        // Bootstrap from the last checkpoint if present, then catch up from
        // append-only event deltas.
        if Self::should_pull_startup_snapshot(&engine.local, restored_local_version)? {
            let _ = engine.pull_snapshot_locked();
        }
        let _ = engine.sync_metadata();
        let _ = engine.restore_pending_state_from_local_log();
        let _ = engine.prime_local_id_ranges(client_slot);
        if options.background_poll {
            let handle = Self::start_background_poll(
                Arc::downgrade(&engine),
                engine.poll_interval,
                poll_shutdown,
            );
            *engine.poll_handle.lock().unwrap_or_else(|p| p.into_inner()) = Some(handle);
        }
        Ok(engine)
    }

    pub fn postgres_url(&self) -> &str {
        &self.postgres_url
    }

    fn fetch_remote_uuid(&self, volume_id: i64) -> MetaResult<String> {
        let postgres_url = self.postgres_url.clone();
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "fetch volume uuid",
                move |client| async move {
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
                        MetaError::Storage(format!("volume id {} not found in postgres", volume_id))
                    })?;
                    Ok(row.get::<_, String>(0))
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

    fn start_background_poll(
        engine: std::sync::Weak<Self>,
        interval: Duration,
        shutdown: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("pg-meta-poll".into())
            .spawn(move || {
                loop {
                    if shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    // Sleep in short increments so we notice the shutdown flag
                    // promptly instead of blocking for the full poll interval.
                    let step = Duration::from_millis(250);
                    let mut remaining = interval;
                    while remaining > Duration::ZERO {
                        let nap = remaining.min(step);
                        std::thread::sleep(nap);
                        remaining = remaining.saturating_sub(nap);
                        if shutdown.load(Ordering::Acquire) {
                            return;
                        }
                    }
                    let Some(engine) = engine.upgrade() else {
                        break;
                    };
                    let _ = engine.flush_due_pending();
                    let _ = engine.sync_metadata();
                }
            })
            .expect("spawn pg-meta-poll thread")
    }

    fn run_async<T, F>(&self, fut: F) -> MetaResult<T>
    where
        T: Send,
        F: std::future::Future<Output = MetaResult<T>> + Send,
    {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return match handle.runtime_flavor() {
                RuntimeFlavor::MultiThread => {
                    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
                }
                RuntimeFlavor::CurrentThread => std::thread::scope(|scope| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(1);
                    scope.spawn(move || {
                        let result = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                MetaError::Storage(format!("failed to build tokio runtime: {e}"))
                            })
                            .and_then(|rt| rt.block_on(fut));
                        let _ = tx.send(result);
                    });
                    rx.recv().map_err(|e| {
                        MetaError::Storage(format!(
                            "failed to receive result from fallback runtime: {e}"
                        ))
                    })?
                }),
                _ => std::thread::scope(|scope| {
                    let (tx, rx) = std::sync::mpsc::sync_channel(1);
                    scope.spawn(move || {
                        let result = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| {
                                MetaError::Storage(format!("failed to build tokio runtime: {e}"))
                            })
                            .and_then(|rt| rt.block_on(fut));
                        let _ = tx.send(result);
                    });
                    rx.recv().map_err(|e| {
                        MetaError::Storage(format!(
                            "failed to receive result from fallback runtime: {e}"
                        ))
                    })?
                }),
            };
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        rt.block_on(fut)
    }

    fn decode_snapshot(bytes: &[u8]) -> MetaResult<RedbSnapshot> {
        serde_json::from_slice(bytes)
            .map_err(|e| MetaError::Serialization(format!("snapshot decode failed: {e}")))
    }

    fn encode_postcard<T: serde::Serialize>(val: &T) -> MetaResult<Vec<u8>> {
        postcard::to_stdvec(val)
            .map_err(|e| MetaError::Serialization(format!("postcard encode failed: {e}")))
    }

    fn project_remote_events(events: &[MetaEvent]) -> Vec<MetaEvent> {
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            match event {
                MetaEvent::SetFileBlock { .. }
                | MetaEvent::RemoveFileBlock { .. }
                | MetaEvent::RemoveFileBlocks { .. }
                | MetaEvent::SetFileBlocksBatch { .. }
                | MetaEvent::CommitFileBlock { .. } => {}
                MetaEvent::AddPendingDeletion { .. } | MetaEvent::RemovePendingDeletion { .. } => {}
                MetaEvent::CreateSnapshot { snapshot } => {
                    let mut synced = snapshot.clone();
                    synced.state = SnapshotState::Synced;
                    out.push(MetaEvent::CreateSnapshot { snapshot: synced });
                }
                MetaEvent::SetAttr { attr } if attr.kind == FileType::RegularFile => {
                    let mut remote_attr = attr.clone();
                    remote_attr.size = remote_attr.committed_size;
                    out.push(MetaEvent::SetAttr { attr: remote_attr });
                }
                _ => out.push(event.clone()),
            }
        }
        out
    }

    fn aux_event_conflicts_with_overlay(
        &self,
        event: &MetaEvent,
        overlay: &OverlayConflictSets,
        removed_inodes: &HashSet<Ino>,
    ) -> MetaResult<bool> {
        let conflict = match event {
            MetaEvent::RemoveAttr { ino } => {
                if removed_inodes.contains(ino) {
                    return Ok(false);
                }
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.symlink_inodes.contains(ino)
                    || overlay.xattr_inodes.contains(ino)
            }
            MetaEvent::SetFileBlock { .. } | MetaEvent::SetRegularFileOverlaySlices { .. } => false,
            MetaEvent::RemoveFileBlock { ino, .. } | MetaEvent::RemoveFileBlocks { ino } => {
                if removed_inodes.contains(ino) {
                    return Ok(false);
                }
                overlay.block_inodes.contains(ino)
            }
            MetaEvent::CommitFileBlock { ino, .. } | MetaEvent::PublishFileManifest { ino, .. } => {
                overlay.block_inodes.contains(ino)
            }
            MetaEvent::SetSymlinkTarget { ino, .. } => {
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.symlink_inodes.contains(ino)
            }
            MetaEvent::SetXattr { ino, name, .. } | MetaEvent::RemoveXattr { ino, name } => {
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.xattr_keys.contains(&Self::xattr_key(*ino, name))
            }
            MetaEvent::SetAclRule { ino, principal, .. }
            | MetaEvent::RemoveAclRule { ino, principal } => {
                overlay.attr_inodes.contains(ino)
                    || overlay.acl_keys.contains(&Self::acl_key(*ino, principal))
            }
            MetaEvent::SetFileBlocksBatch { blocks } => blocks
                .iter()
                .any(|(ino, _, _)| overlay.block_inodes.contains(ino)),
            MetaEvent::SetAttr { .. }
            | MetaEvent::AddEntry { .. }
            | MetaEvent::RemoveEntry { .. }
            | MetaEvent::RenameEntry { .. }
            | MetaEvent::AddPendingDeletion { .. }
            | MetaEvent::RemovePendingDeletion { .. }
            | MetaEvent::CreateSnapshot { .. }
            | MetaEvent::UpdateSnapshotState { .. } => false,
        };
        Ok(conflict)
    }

    fn event_has_aux_apply_effect(event: &MetaEvent) -> bool {
        match event {
            MetaEvent::RemoveAttr { .. }
            | MetaEvent::RemoveFileBlock { .. }
            | MetaEvent::RemoveFileBlocks { .. }
            | MetaEvent::CommitFileBlock { .. }
            | MetaEvent::PublishFileManifest { .. }
            | MetaEvent::SetSymlinkTarget { .. }
            | MetaEvent::SetXattr { .. }
            | MetaEvent::RemoveXattr { .. }
            | MetaEvent::SetAclRule { .. }
            | MetaEvent::RemoveAclRule { .. }
            | MetaEvent::AddPendingDeletion { .. }
            | MetaEvent::RemovePendingDeletion { .. }
            | MetaEvent::CreateSnapshot { .. }
            | MetaEvent::UpdateSnapshotState { .. } => true,
            MetaEvent::SetAttr { .. }
            | MetaEvent::SetFileBlock { .. }
            | MetaEvent::SetRegularFileOverlaySlices { .. }
            | MetaEvent::SetFileBlocksBatch { .. }
            | MetaEvent::AddEntry { .. }
            | MetaEvent::RemoveEntry { .. }
            | MetaEvent::RenameEntry { .. } => false,
        }
    }

    fn xattr_key(ino: Ino, name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(8 + name.len());
        key.extend_from_slice(&ino.to_le_bytes());
        key.extend_from_slice(name);
        key
    }

    fn acl_key(ino: Ino, principal: &str) -> Vec<u8> {
        let principal = principal.as_bytes();
        let mut key = Vec::with_capacity(8 + principal.len());
        key.extend_from_slice(&ino.to_le_bytes());
        key.extend_from_slice(principal);
        key
    }

    fn dentry_key(parent: Ino, name: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(8 + name.len());
        key.extend_from_slice(&parent.to_le_bytes());
        key.extend_from_slice(name);
        key
    }

    fn removed_inodes_for_batch(&self, batch_events: &[MetaEvent]) -> HashSet<Ino> {
        let mut removed = HashSet::new();
        for event in batch_events {
            if let MetaEvent::RemoveEntry { parent, name } = event
                && let Ok(entry) = self.local.lookup(*parent, name)
            {
                removed.insert(entry.ino);
            }
        }
        removed
    }

    fn overlay_conflicts_for_events(events: &[MetaEvent]) -> OverlayConflictSets {
        let mut overlay = OverlayConflictSets::default();
        for event in events {
            match event {
                MetaEvent::SetAttr { attr } => {
                    overlay.attr_inodes.insert(attr.ino);
                }
                MetaEvent::RemoveAttr { ino } => {
                    overlay.attr_inodes.insert(*ino);
                    overlay.block_inodes.insert(*ino);
                    overlay.symlink_inodes.insert(*ino);
                    overlay.xattr_inodes.insert(*ino);
                }
                MetaEvent::AddEntry { parent, name, .. }
                | MetaEvent::RemoveEntry { parent, name } => {
                    overlay.dentry_keys.insert(Self::dentry_key(*parent, name));
                }
                MetaEvent::RenameEntry {
                    parent,
                    name,
                    new_parent,
                    new_name,
                    entry,
                    displaced_set_attr,
                    displaced_remove_attr_ino,
                    moved_dir_attr,
                    old_parent_attr,
                    new_parent_attr,
                } => {
                    overlay.dentry_keys.insert(Self::dentry_key(*parent, name));
                    overlay
                        .dentry_keys
                        .insert(Self::dentry_key(*new_parent, new_name));
                    overlay.attr_inodes.insert(entry.ino);
                    if let Some(attr) = displaced_set_attr.as_ref() {
                        overlay.attr_inodes.insert(attr.ino);
                    }
                    if let Some(ino) = displaced_remove_attr_ino {
                        overlay.attr_inodes.insert(*ino);
                    }
                    if let Some(attr) = moved_dir_attr.as_ref() {
                        overlay.attr_inodes.insert(attr.ino);
                    }
                    if let Some(attr) = old_parent_attr.as_ref() {
                        overlay.attr_inodes.insert(attr.ino);
                    }
                    if let Some(attr) = new_parent_attr.as_ref() {
                        overlay.attr_inodes.insert(attr.ino);
                    }
                }
                MetaEvent::SetFileBlock { ino, .. }
                | MetaEvent::SetRegularFileOverlaySlices { ino, .. }
                | MetaEvent::RemoveFileBlock { ino, .. }
                | MetaEvent::RemoveFileBlocks { ino }
                | MetaEvent::CommitFileBlock { ino, .. }
                | MetaEvent::PublishFileManifest { ino, .. } => {
                    overlay.block_inodes.insert(*ino);
                }
                MetaEvent::SetFileBlocksBatch { blocks } => {
                    for (ino, _, _) in blocks {
                        overlay.block_inodes.insert(*ino);
                    }
                }
                MetaEvent::SetSymlinkTarget { ino, .. } => {
                    overlay.symlink_inodes.insert(*ino);
                    overlay.attr_inodes.insert(*ino);
                }
                MetaEvent::SetXattr { ino, name, .. } | MetaEvent::RemoveXattr { ino, name } => {
                    overlay.xattr_inodes.insert(*ino);
                    overlay.xattr_keys.insert(Self::xattr_key(*ino, name));
                }
                MetaEvent::SetAclRule { ino, principal, .. }
                | MetaEvent::RemoveAclRule { ino, principal } => {
                    overlay.attr_inodes.insert(*ino);
                    overlay.acl_keys.insert(Self::acl_key(*ino, principal));
                }
                MetaEvent::AddPendingDeletion { .. }
                | MetaEvent::RemovePendingDeletion { .. }
                | MetaEvent::CreateSnapshot { .. }
                | MetaEvent::UpdateSnapshotState { .. } => {}
            }
        }
        overlay
    }

    fn merge_overlay_conflicts(
        base: &OverlayConflictSets,
        extra: &OverlayConflictSets,
    ) -> OverlayConflictSets {
        let mut merged = base.clone();
        merged
            .block_inodes
            .extend(extra.block_inodes.iter().copied());
        merged.attr_inodes.extend(extra.attr_inodes.iter().copied());
        merged.dentry_keys.extend(extra.dentry_keys.iter().cloned());
        merged
            .symlink_inodes
            .extend(extra.symlink_inodes.iter().copied());
        merged
            .xattr_inodes
            .extend(extra.xattr_inodes.iter().copied());
        merged.xattr_keys.extend(extra.xattr_keys.iter().cloned());
        merged.acl_keys.extend(extra.acl_keys.iter().cloned());
        merged
    }

    fn local_overlay_conflicts(&self) -> MetaResult<OverlayConflictSets> {
        let start = self.flushed_event_seq.load(Ordering::SeqCst);
        let end = self.local.get_counter("nextEventSeq")?;
        if end <= start {
            return Ok(OverlayConflictSets::default());
        }

        let events = self.local.read_events_range(start, end)?;
        Ok(Self::overlay_conflicts_for_events(&events))
    }

    fn serving_event_conflicts_with_overlay(
        event: &MetaEvent,
        overlay: &OverlayConflictSets,
        removed_inodes: &HashSet<Ino>,
    ) -> bool {
        match event {
            MetaEvent::SetAttr { attr } => {
                overlay.block_inodes.contains(&attr.ino) || overlay.attr_inodes.contains(&attr.ino)
            }
            MetaEvent::RemoveAttr { ino } => {
                if removed_inodes.contains(ino) {
                    return false;
                }
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.symlink_inodes.contains(ino)
                    || overlay.xattr_inodes.contains(ino)
            }
            MetaEvent::AddEntry { parent, name, .. } | MetaEvent::RemoveEntry { parent, name } => {
                overlay
                    .dentry_keys
                    .contains(&Self::dentry_key(*parent, name))
            }
            MetaEvent::RenameEntry { .. } => false,
            MetaEvent::SetSymlinkTarget { ino, .. } => {
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.symlink_inodes.contains(ino)
            }
            MetaEvent::SetXattr { ino, name, .. } | MetaEvent::RemoveXattr { ino, name } => {
                overlay.block_inodes.contains(ino)
                    || overlay.attr_inodes.contains(ino)
                    || overlay.xattr_keys.contains(&Self::xattr_key(*ino, name))
            }
            MetaEvent::SetAclRule { ino, principal, .. }
            | MetaEvent::RemoveAclRule { ino, principal } => {
                overlay.attr_inodes.contains(ino)
                    || overlay.acl_keys.contains(&Self::acl_key(*ino, principal))
            }
            MetaEvent::SetFileBlock { .. }
            | MetaEvent::SetRegularFileOverlaySlices { .. }
            | MetaEvent::RemoveFileBlock { .. }
            | MetaEvent::RemoveFileBlocks { .. }
            | MetaEvent::SetFileBlocksBatch { .. }
            | MetaEvent::CommitFileBlock { .. }
            | MetaEvent::PublishFileManifest { .. }
            | MetaEvent::AddPendingDeletion { .. }
            | MetaEvent::RemovePendingDeletion { .. }
            | MetaEvent::CreateSnapshot { .. }
            | MetaEvent::UpdateSnapshotState { .. } => false,
        }
    }

    fn decode_foreign_batch(
        rows: &[tokio_postgres::Row],
        start_idx: usize,
        client_id: &str,
    ) -> MetaResult<(i64, usize, Vec<MetaEvent>, bool)> {
        let batch_version: i64 = rows[start_idx].get(0);
        let batch_client: Option<String> = rows[start_idx].get(3);
        let is_self_batch = batch_client.as_deref() == Some(client_id);
        let mut idx = start_idx;
        let mut batch_events = Vec::new();

        while idx < rows.len() {
            let row_batch_version: i64 = rows[idx].get(0);
            let row_client: Option<String> = rows[idx].get(3);
            if row_batch_version != batch_version || row_client != batch_client {
                break;
            }
            let payload: Vec<u8> = rows[idx].get(2);
            let event: MetaEvent = postcard::from_bytes(&payload).map_err(|e| {
                MetaError::Serialization(format!("postgres event decode failed: {e}"))
            })?;
            batch_events.push(event);
            idx += 1;
        }

        Ok((batch_version, idx, batch_events, is_self_batch))
    }

    fn apply_foreign_batch(
        &self,
        batch_events: &[MetaEvent],
        overlay: &OverlayConflictSets,
    ) -> MetaResult<bool> {
        let removed_inodes = self.removed_inodes_for_batch(batch_events);
        self.local.apply_replica_events_to_base(batch_events)?;
        let serving_events: Vec<MetaEvent> = batch_events
            .iter()
            .filter(|event| {
                !Self::serving_event_conflicts_with_overlay(event, overlay, &removed_inodes)
            })
            .cloned()
            .collect();
        self.local.apply_replica_serving_state(&serving_events)?;
        let mut aux_events = Vec::new();
        for event in batch_events {
            if !Self::event_has_aux_apply_effect(event) {
                continue;
            }
            if !self.aux_event_conflicts_with_overlay(event, overlay, &removed_inodes)? {
                aux_events.push(event.clone());
            }
        }

        if aux_events.is_empty() {
            return Ok(false);
        }

        self.local.apply_replica_aux_state(&aux_events)?;
        Ok(true)
    }

    fn apply_visible_self_batch(&self, batch_events: &[MetaEvent]) -> MetaResult<bool> {
        if batch_events.is_empty() {
            return Ok(false);
        }

        self.local.apply_replica_events_to_base(batch_events)?;
        for event in batch_events {
            if let MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                entry,
                ..
            } = event
            {
                self.local.reconcile_serving_rename_from_base(
                    *parent,
                    name,
                    *new_parent,
                    new_name,
                    entry,
                )?;
            }
        }
        let aux_events: Vec<MetaEvent> = batch_events
            .iter()
            .filter(|event| Self::event_has_aux_apply_effect(event))
            .cloned()
            .collect();
        if aux_events.is_empty() {
            return Ok(false);
        }

        self.local.apply_replica_aux_state(&aux_events)?;
        Ok(true)
    }

    fn acknowledge_local_prefix_from_self_batch(
        &self,
        batch_events: &[MetaEvent],
    ) -> MetaResult<()> {
        if batch_events.is_empty() {
            return Ok(());
        }

        let start = self.flushed_event_seq.load(Ordering::SeqCst);
        let end = self.local.get_counter("nextEventSeq")?;
        if end <= start {
            return Ok(());
        }

        let local_events = self.local.read_events_range(start, end)?;
        if local_events.len() < batch_events.len() {
            return Ok(());
        }

        let matches_prefix = local_events
            .iter()
            .zip(batch_events.iter())
            .take(batch_events.len())
            .all(|(local, remote)| {
                Self::encode_postcard(local).ok() == Self::encode_postcard(remote).ok()
            });
        if matches_prefix {
            let next_seq = start.max(1).saturating_add(batch_events.len() as u64);
            self.persist_flushed_event_seq(next_seq)?;
        }
        Ok(())
    }

    fn pull_snapshot_locked(&self) -> MetaResult<bool> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let local_version = self.local_version.load(Ordering::SeqCst);
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;

        let row = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "pull snapshot",
                move |client| async move {
                    let version_row = client
                        .query_opt(
                            "SELECT version FROM metadata_state WHERE vol=$1",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres snapshot version select failed: {e}"
                            ))
                        })?;

                    let Some(version_row) = version_row else {
                        return Ok(None);
                    };
                    let version: i64 = version_row.get(0);
                    if version <= local_version {
                        return Ok(None);
                    }

                    let row = client
                        .query_one(
                            "SELECT version, snapshot FROM metadata_state WHERE vol=$1",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres snapshot payload select failed: {e}"
                            ))
                        })?;

                    Ok(Some(row))
                },
            )
            .await
        })?;

        let Some(row) = row else {
            return Ok(false);
        };

        let version: i64 = row.get(0);
        let bytes: Vec<u8> = row.get(1);

        let snapshot = Self::decode_snapshot(&bytes)?;
        self.local.import_remote_snapshot(&snapshot)?;
        self.persist_local_version(version)?;
        Ok(true)
    }

    fn push_event_log_batch_locked(&self, events: &[MetaEvent]) -> MetaResult<i64> {
        let remote_events = Self::project_remote_events(events);
        if remote_events.is_empty() {
            return Ok(self.local_version.load(Ordering::SeqCst));
        }

        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let now_ms = Self::now_millis();
        let events_vec = remote_events;
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "push event log batch",
                move |client| async move {
                    let mut client = client;
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!("postgres begin tx failed: {e}"))
                    })?;

                    let batch_version: i64 = tx
                        .query_one(
                            r#"
                            UPDATE metadata_state
                            SET version = version + 1,
                                updated_at = now()
                            WHERE vol = $1
                            RETURNING version
                            "#,
                            &[&volume_id],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!("postgres metadata_state bump failed: {e}"))
                        })?
                        .get(0);

                    for chunk in events_vec.chunks(PG_EVENT_INSERT_CHUNK_SIZE) {
                        let event_types: Vec<&str> =
                            chunk.iter().map(|event| event.kind()).collect();
                        let payloads: Vec<Vec<u8>> = chunk
                            .iter()
                            .map(Self::encode_postcard)
                            .collect::<MetaResult<Vec<_>>>()?;
                        tx.execute(
                            r#"
                            INSERT INTO event_log (
                                vol,
                                batch_version,
                                event_type,
                                event_payload,
                                client_id,
                                client_timestamp
                            )
                            SELECT
                                $1,
                                $2,
                                batch.event_type,
                                batch.event_payload,
                                $3,
                                $4
                            FROM UNNEST($5::text[], $6::bytea[]) AS batch(event_type, event_payload)
                            "#,
                            &[
                                &volume_id,
                                &batch_version,
                                &client_id,
                                &now_ms,
                                &event_types,
                                &payloads,
                            ],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres event_log batch insert failed: {e}"
                            ))
                        })?;
                    }

                    for event in &events_vec {
                        match event {
                            MetaEvent::SetAclRule {
                                ino,
                                principal,
                                access_level,
                            } => {
                                tx.execute(
                                    r#"
                                    INSERT INTO acl_rules (vol, ino, principal, access_level, updated_at)
                                    VALUES ($1, $2, $3, $4, now())
                                    ON CONFLICT (vol, ino, principal) DO UPDATE
                                    SET access_level = EXCLUDED.access_level,
                                        updated_at = now()
                                    "#,
                                    &[&volume_id, &(*ino as i64), principal, &(*access_level as i16)],
                                )
                                .await
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres acl_rules upsert failed: {e}"
                                    ))
                                })?;
                            }
                            MetaEvent::RemoveAclRule { ino, principal } => {
                                tx.execute(
                                    "DELETE FROM acl_rules WHERE vol=$1 AND ino=$2 AND principal=$3",
                                    &[&volume_id, &(*ino as i64), principal],
                                )
                                .await
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres acl_rules delete failed: {e}"
                                    ))
                                })?;
                            }
                            MetaEvent::CreateSnapshot { snapshot } => {
                                let pinned: Vec<i64> = snapshot
                                    .pinned_object_ids
                                    .iter()
                                    .map(|id| {
                                        i64::try_from(*id).map_err(|_| {
                                            MetaError::Storage(format!(
                                                "snapshot pinned object id overflow: {id}"
                                            ))
                                        })
                                    })
                                    .collect::<MetaResult<Vec<_>>>()?;
                                tx.execute(
                                    r#"
                                    INSERT INTO snapshots (
                                        vol,
                                        id,
                                        root_ino,
                                        created_at,
                                        state,
                                        pinned_object_ids,
                                        snapshot_payload,
                                        updated_at
                                    )
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, now())
                                    ON CONFLICT (vol, id) DO UPDATE
                                    SET root_ino = EXCLUDED.root_ino,
                                        created_at = EXCLUDED.created_at,
                                        state = EXCLUDED.state,
                                        pinned_object_ids = EXCLUDED.pinned_object_ids,
                                        snapshot_payload = EXCLUDED.snapshot_payload,
                                        updated_at = now()
                                    "#,
                                    &[
                                        &volume_id,
                                        &snapshot.id,
                                        &(snapshot.root_ino as i64),
                                        &snapshot.created_at,
                                        &(snapshot.state as i16),
                                        &pinned,
                                        &snapshot.snapshot_payload,
                                    ],
                                )
                                .await
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres snapshots upsert failed: {e}"
                                    ))
                                })?;
                            }
                            MetaEvent::UpdateSnapshotState { id, state } => {
                                tx.execute(
                                    r#"
                                    UPDATE snapshots
                                    SET state = $3,
                                        updated_at = now()
                                    WHERE vol = $1 AND id = $2
                                    "#,
                                    &[&volume_id, id, &(*state as i16)],
                                )
                                .await
                                .map_err(|e| {
                                    MetaError::Storage(format!(
                                        "postgres snapshots state update failed: {e}"
                                    ))
                                })?;
                            }
                            _ => {}
                        }
                    }

                    let channel = format!("studiofs_vol_{volume_id}");
                    let _ = tx
                        .execute(
                            "SELECT pg_notify($1, $2)",
                            &[&channel, &batch_version.to_string()],
                        )
                        .await;

                    tx.commit()
                        .await
                        .map_err(|e| MetaError::Storage(format!("postgres commit failed: {e}")))?;
                    Ok(batch_version)
                },
            )
            .await
        })
    }

    fn write_checkpoint_locked(&self) -> MetaResult<()> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let snapshot = self.local.export_remote_snapshot()?;
        let payload = serde_json::to_vec(&snapshot)
            .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))?;
        let version = self.local_version.load(Ordering::SeqCst);
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;

        self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "write checkpoint",
                move |client| async move {
                    client
                        .execute(
                            r#"
                            INSERT INTO metadata_state (vol, version, snapshot)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (vol) DO UPDATE
                            SET version = EXCLUDED.version,
                                snapshot = EXCLUDED.snapshot,
                                updated_at = now()
                            "#,
                            &[&volume_id, &version, &payload],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!("postgres checkpoint upsert failed: {e}"))
                        })?;

                    Ok(())
                },
            )
            .await
        })
    }

    fn pull_foreign_events_locked(&self, recent_flush: Option<&RecentFlush>) -> MetaResult<bool> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let last_seq = self.local_version.load(Ordering::SeqCst);
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;

        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "pull foreign events",
                move |client| async move {
                    let rows = client
                        .query(
                            r#"
                            SELECT batch_version, seq_id, event_payload, client_id
                            FROM event_log
                            WHERE vol=$1 AND batch_version > $2
                            ORDER BY batch_version ASC, seq_id ASC
                            "#,
                            &[&volume_id, &last_seq],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!("postgres event_log select failed: {e}"))
                        })?;
                    Ok::<_, MetaError>(rows)
                },
            )
            .await
        })?;

        if rows.is_empty() {
            return Ok(false);
        }

        let mut overlay = self.local_overlay_conflicts()?;
        let mut applied_any = false;
        let mut max_version = last_seq;
        let mut max_namespace_version = self.namespace_version.load(Ordering::SeqCst);
        let mut idx = 0;
        while idx < rows.len() {
            let (batch_version, next_idx, batch_events, is_self_batch) =
                Self::decode_foreign_batch(&rows, idx, client_id.as_str())?;
            idx = next_idx;

            if is_self_batch {
                applied_any |= self.apply_visible_self_batch(&batch_events)?;
                self.acknowledge_local_prefix_from_self_batch(&batch_events)?;
            } else {
                let effective_overlay = if let Some(recent_flush) = recent_flush {
                    if batch_version < recent_flush.batch_version {
                        Self::merge_overlay_conflicts(&overlay, &recent_flush.overlay)
                    } else {
                        overlay.clone()
                    }
                } else {
                    overlay.clone()
                };
                applied_any |= self.apply_foreign_batch(&batch_events, &effective_overlay)?;
                if batch_events.iter().any(MetaEvent::is_namespace_mutation) {
                    max_namespace_version = max_namespace_version.max(batch_version);
                }
            }
            max_version = batch_version;
            overlay = self.local_overlay_conflicts()?;
        }
        self.persist_local_version(max_version)?;
        if max_namespace_version > self.namespace_version.load(Ordering::SeqCst) {
            self.persist_namespace_version(max_namespace_version)?;
        }
        Ok(applied_any)
    }

    fn clear_pending_state(state: &mut PendingReplicationState) {
        *state = PendingReplicationState::default();
    }

    fn finish_claimed_batch(&self, batch: ClaimedPendingBatch) {
        self.inflight_replication_ops
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_sub(batch.ops))
            })
            .ok();
    }

    fn restore_pending_state_from_local_log(&self) -> MetaResult<()> {
        if !Self::has_any_local_overlay_mutation(&self.local)? {
            let mut state = self
                .pending_state
                .lock()
                .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
            Self::clear_pending_state(&mut state);
            return Ok(());
        }

        let event_start = self.flushed_event_seq.load(Ordering::SeqCst);
        let event_end = self.local.get_counter("nextEventSeq")?;
        let mut state = self
            .pending_state
            .lock()
            .map_err(|_| MetaError::Storage("pg pending state lock poisoned".to_string()))?;
        if event_end <= event_start {
            Self::clear_pending_state(&mut state);
            return Ok(());
        }

        let now = Instant::now();
        let pending_events = event_end.saturating_sub(event_start);
        state.ops = u32::try_from(pending_events).unwrap_or(u32::MAX);
        state.force_flush = true;
        state.event_start = Some(event_start);
        state.event_end = event_end;
        state.started_at = Some(now);
        state.last_mutation_at = Some(now);
        Ok(())
    }

    fn mark_pending_after_mutation(&self, before_seq: u64, after_seq: u64) {
        let now = Instant::now();
        if let Ok(mut state) = self.pending_state.lock() {
            if state.ops == 0 {
                state.started_at = Some(now);
            }
            state.ops = state.ops.saturating_add(1);
            state.last_mutation_at = Some(now);
            if after_seq > before_seq {
                if state.event_start.is_none() {
                    state.event_start = Some(before_seq);
                }
                state.event_end = state.event_end.max(after_seq);
            }
            if state.ops >= self.flush_ops_threshold {
                state.force_flush = true;
            }
        }
    }

    fn request_pending_flush(&self) {
        if let Ok(mut state) = self.pending_state.lock() {
            state.force_flush = true;
        }
    }

    fn should_flush_due_interval(
        state: &PendingReplicationState,
        idle_grace: Duration,
        interval: Duration,
    ) -> bool {
        if state.ops == 0 {
            return false;
        }
        if let Some(t0) = state.last_mutation_at {
            return t0.elapsed() >= idle_grace;
        }
        if let Some(t0) = state.started_at {
            return t0.elapsed() >= interval;
        }
        true
    }

    fn claim_pending_batch(&self, force: bool) -> Option<ClaimedPendingBatch> {
        let mut state = self.pending_state.lock().ok()?;
        if state.ops == 0 {
            return None;
        }
        let should_flush = force
            || state.force_flush
            || Self::should_flush_due_interval(&state, self.flush_idle_grace, self.flush_interval);
        if !should_flush {
            return None;
        }
        let event_start = state.event_start?;
        let batch = ClaimedPendingBatch {
            ops: state.ops,
            started_at: state.started_at,
            last_mutation_at: state.last_mutation_at,
            force_flush: state.force_flush,
            event_start,
            event_end: state.event_end,
        };
        Self::clear_pending_state(&mut state);
        self.inflight_replication_ops
            .fetch_add(batch.ops, Ordering::SeqCst);
        Some(batch)
    }

    fn restore_pending_batch(&self, batch: ClaimedPendingBatch) {
        self.finish_claimed_batch(batch);
        if let Ok(mut state) = self.pending_state.lock() {
            state.ops = state.ops.saturating_add(batch.ops);
            state.force_flush |= batch.force_flush;
            state.event_start = Some(
                state
                    .event_start
                    .map(|current| current.min(batch.event_start))
                    .unwrap_or(batch.event_start),
            );
            state.event_end = state.event_end.max(batch.event_end);
            state.started_at = match (state.started_at, batch.started_at) {
                (Some(current), Some(claimed)) => Some(current.min(claimed)),
                (Some(current), None) => Some(current),
                (None, Some(claimed)) => Some(claimed),
                (None, None) => None,
            };
            state.last_mutation_at = match (state.last_mutation_at, batch.last_mutation_at) {
                (Some(current), Some(claimed)) => Some(current.max(claimed)),
                (Some(current), None) => Some(current),
                (None, Some(claimed)) => Some(claimed),
                (None, None) => None,
            };
        }
    }

    fn flush_pending_claimed_batch(
        &self,
        batch: ClaimedPendingBatch,
    ) -> MetaResult<Option<RecentFlush>> {
        let events = self
            .local
            .read_events_range(batch.event_start, batch.event_end)?;
        if events.is_empty() {
            return Ok(None);
        }
        let batch_version = self.push_event_log_batch_locked(&events)?;
        // Do not advance `local_version` here. The writer may have published a
        // newer batch_version while still missing earlier foreign batches.
        // `pull_foreign_events_locked()` must remain able to read the full gap.
        self.apply_visible_self_batch(&events)?;
        self.persist_flushed_event_seq(batch.event_end)?;
        Ok(Some(RecentFlush {
            batch_version,
            overlay: Self::overlay_conflicts_for_events(&events),
        }))
    }

    fn flush_pending_claimed_batch_with_restore(
        &self,
        batch: ClaimedPendingBatch,
    ) -> MetaResult<Option<RecentFlush>> {
        match self.flush_pending_claimed_batch(batch) {
            Ok(recent_flush) => {
                self.finish_claimed_batch(batch);
                Ok(recent_flush)
            }
            Err(err) => {
                let recovered_from_remote = self
                    .pull_foreign_events_locked(None)
                    .map(|_| self.flushed_event_seq.load(Ordering::SeqCst) >= batch.event_end)
                    .unwrap_or(false);
                if recovered_from_remote {
                    self.finish_claimed_batch(batch);
                    return Ok(None);
                }
                self.restore_pending_batch(batch);
                Err(err)
            }
        }
    }

    fn flush_due_pending(&self) -> MetaResult<()> {
        let Some(batch) = self.claim_pending_batch(false) else {
            return Ok(());
        };
        let _guard = self
            .replication_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
        let recent_flush = self.flush_pending_claimed_batch_with_restore(batch)?;
        let _ = self.pull_foreign_events_locked(recent_flush.as_ref())?;
        Ok(())
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
        self.mark_pending_after_mutation(before_seq, after_seq);
        Ok(result)
    }
}

impl MetaEngineCore for PgMetaEngine {
    fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool> {
        self.try_acquire_pg_janitor_lock()
    }

    fn release_global_janitor_lock(&self) -> MetaResult<()> {
        self.release_pg_janitor_lock()
    }

    fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        self.load_pg_global_snapshot_pins()
    }

    fn acquire_file_lock_lease(
        &self,
        ino: Ino,
        kind: HardLockKind,
        lease_ttl: Duration,
    ) -> MetaResult<FileLockLeaseAcquireOutcome> {
        self.ensure_lock_workers_started()?;
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        let outcome = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "acquire file lock lease",
                move |client| async move {
                    let mut client = client;
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock acquire begin tx failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let now_ms = Self::now_millis();
                    let lease_expires_at_unix_ms = now_ms
                        .checked_add(i64::try_from(lease_ttl.as_millis()).unwrap_or(i64::MAX))
                        .unwrap_or(i64::MAX);
                    tx.execute(
                        r#"
                        INSERT INTO file_lock_leases (
                            vol,
                            ino,
                            kind,
                            exclusive_owner,
                            shared_owners,
                            lease_expires_at_unix_ms,
                            generation,
                            updated_at
                        )
                        VALUES ($1, $2, $3, NULL, '{}', 0, 0, now())
                        ON CONFLICT (vol, ino) DO NOTHING
                        "#,
                        &[&volume_id, &(ino as i64), &(HardLockKind::Shared as i16)],
                    )
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock acquire ensure row failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let row = tx
                        .query_one(
                            r#"
                            SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
                            FROM file_lock_leases
                            WHERE vol = $1 AND ino = $2
                            FOR UPDATE
                            "#,
                            &[&volume_id, &(ino as i64)],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock acquire select failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    let current = Self::current_file_lock_lease_from_row(row, now_ms)?;

                    let maybe_next = match current.as_ref() {
                        None => Some(FileLockLease {
                            ino,
                            kind,
                            generation: 1,
                            lease_expires_at_unix_ms,
                            exclusive_owner: (kind == HardLockKind::Exclusive)
                                .then(|| client_id.clone()),
                            shared_owners: if kind == HardLockKind::Shared {
                                [client_id.clone()].into_iter().collect()
                            } else {
                                Default::default()
                            },
                        }),
                        Some(current) => {
                            let is_self_exclusive =
                                current.exclusive_owner.as_deref() == Some(client_id.as_str());
                            let is_self_shared = current.shared_owners.contains(client_id.as_str());
                            match kind {
                                HardLockKind::Shared => {
                                    if current.exclusive_owner.is_none() || is_self_exclusive {
                                        let mut next = current.clone();
                                        next.kind = HardLockKind::Shared;
                                        next.generation = next.generation.saturating_add(1);
                                        next.lease_expires_at_unix_ms = lease_expires_at_unix_ms;
                                        next.exclusive_owner = None;
                                        next.shared_owners.insert(client_id.clone());
                                        Some(next)
                                    } else {
                                        None
                                    }
                                }
                                HardLockKind::Exclusive => {
                                    let shared_conflict = !(current.shared_owners.is_empty()
                                        || (current.shared_owners.len() == 1 && is_self_shared));
                                    if (current.exclusive_owner.is_none() || is_self_exclusive)
                                        && !shared_conflict
                                    {
                                        Some(FileLockLease {
                                            ino,
                                            kind: HardLockKind::Exclusive,
                                            generation: current.generation.saturating_add(1),
                                            lease_expires_at_unix_ms,
                                            exclusive_owner: Some(client_id.clone()),
                                            shared_owners: Default::default(),
                                        })
                                    } else {
                                        None
                                    }
                                }
                            }
                        }
                    };

                    let outcome = if let Some(next) = maybe_next {
                        let shared_owners: Vec<String> =
                            next.shared_owners.iter().cloned().collect();
                        tx.execute(
                            r#"
                            UPDATE file_lock_leases
                            SET kind = $3,
                                exclusive_owner = $4,
                                shared_owners = $5,
                                lease_expires_at_unix_ms = $6,
                                generation = $7,
                                updated_at = now()
                            WHERE vol = $1 AND ino = $2
                            "#,
                            &[
                                &volume_id,
                                &(ino as i64),
                                &(next.kind as i16),
                                &next.exclusive_owner,
                                &shared_owners,
                                &next.lease_expires_at_unix_ms,
                                &(i64::try_from(next.generation).unwrap_or(i64::MAX)),
                            ],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock acquire update failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        let channel = Self::lock_notify_channel(volume_id);
                        let payload =
                            Self::encode_file_lock_notify_payload(ino, next.generation);
                        let _ = tx
                            .execute("SELECT pg_notify($1, $2)", &[&channel, &payload])
                            .await;
                        tx.commit().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock acquire commit failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        FileLockLeaseAcquireOutcome::Acquired(next)
                    } else {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock acquire rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        FileLockLeaseAcquireOutcome::Blocked(current.ok_or_else(|| {
                            MetaError::Storage(
                                "file lock acquire was blocked without a live conflicting lease"
                                    .to_string(),
                            )
                        })?)
                    };
                    Ok(outcome)
                },
            )
            .await
        })?;
        match &outcome {
            FileLockLeaseAcquireOutcome::Acquired(snapshot) => {
                self.remember_held_file_lock(snapshot.ino, lease_ttl);
                self.cache_file_lock_lease(Some(snapshot.clone()));
            }
            FileLockLeaseAcquireOutcome::Blocked(snapshot) => {
                self.cache_file_lock_lease(Some(snapshot.clone()));
            }
        }
        Ok(outcome)
    }

    fn release_file_lock_lease(&self, ino: Ino) -> MetaResult<FileLockLeaseReleaseOutcome> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        let outcome = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "release file lock lease",
                move |client| async move {
                    let mut client = client;
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock release begin tx failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let now_ms = Self::now_millis();
                    let row = tx
                        .query_opt(
                            r#"
                            SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
                            FROM file_lock_leases
                            WHERE vol = $1 AND ino = $2
                            FOR UPDATE
                            "#,
                            &[&volume_id, &(ino as i64)],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release select failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    let current = match row {
                        Some(row) => Self::current_file_lock_lease_from_row(row, now_ms)?,
                        None => None,
                    };
                    let Some(current) = current else {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        return Ok(FileLockLeaseReleaseOutcome::NotHeld(None));
                    };

                    let is_self_exclusive =
                        current.exclusive_owner.as_deref() == Some(client_id.as_str());
                    let is_self_shared = current.shared_owners.contains(client_id.as_str());
                    if !is_self_exclusive && !is_self_shared {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        return Ok(FileLockLeaseReleaseOutcome::NotHeld(Some(current)));
                    }

                    let next = if is_self_exclusive {
                        None
                    } else {
                        let mut next = current.clone();
                        next.shared_owners.remove(client_id.as_str());
                        if next.shared_owners.is_empty() {
                            None
                        } else {
                            next.generation = next.generation.saturating_add(1);
                            Some(next)
                        }
                    };

                    if let Some(next) = next.clone() {
                        let shared_owners: Vec<String> =
                            next.shared_owners.iter().cloned().collect();
                        tx.execute(
                            r#"
                            UPDATE file_lock_leases
                            SET kind = $3,
                                exclusive_owner = $4,
                                shared_owners = $5,
                                lease_expires_at_unix_ms = $6,
                                generation = $7,
                                updated_at = now()
                            WHERE vol = $1 AND ino = $2
                            "#,
                            &[
                                &volume_id,
                                &(ino as i64),
                                &(next.kind as i16),
                                &next.exclusive_owner,
                                &shared_owners,
                                &next.lease_expires_at_unix_ms,
                                &(i64::try_from(next.generation).unwrap_or(i64::MAX)),
                            ],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release update failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    } else {
                        tx.execute(
                            "DELETE FROM file_lock_leases WHERE vol = $1 AND ino = $2",
                            &[&volume_id, &(ino as i64)],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release delete failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    }
                    let generation = next.as_ref().map(|lease| lease.generation).unwrap_or_else(|| {
                        current.generation.saturating_add(1)
                    });
                    let channel = Self::lock_notify_channel(volume_id);
                    let payload = Self::encode_file_lock_notify_payload(ino, generation);
                    let _ = tx
                        .execute("SELECT pg_notify($1, $2)", &[&channel, &payload])
                        .await;
                    tx.commit().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock release commit failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    Ok(FileLockLeaseReleaseOutcome::Released(next))
                },
            )
            .await
        })?;
        self.forget_held_file_lock(ino);
        match &outcome {
            FileLockLeaseReleaseOutcome::Released(snapshot)
            | FileLockLeaseReleaseOutcome::NotHeld(snapshot) => {
                self.upsert_cached_file_lock_lease(snapshot.clone(), ino);
            }
        }
        Ok(outcome)
    }

    fn renew_file_lock_lease(
        &self,
        ino: Ino,
        lease_ttl: Duration,
    ) -> MetaResult<FileLockLeaseRenewOutcome> {
        self.ensure_lock_workers_started()?;
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        let outcome = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "renew file lock lease",
                move |client| async move {
                    let mut client = client;
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock renew begin tx failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let now_ms = Self::now_millis();
                    let lease_expires_at_unix_ms = now_ms
                        .checked_add(i64::try_from(lease_ttl.as_millis()).unwrap_or(i64::MAX))
                        .unwrap_or(i64::MAX);
                    let row = tx
                        .query_opt(
                            r#"
                            SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
                            FROM file_lock_leases
                            WHERE vol = $1 AND ino = $2
                            FOR UPDATE
                            "#,
                            &[&volume_id, &(ino as i64)],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock renew select failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    let current = match row {
                        Some(row) => Self::current_file_lock_lease_from_row(row, now_ms)?,
                        None => None,
                    };
                    let Some(mut current) = current else {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock renew rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        return Ok(FileLockLeaseRenewOutcome::Lost(None));
                    };

                    let is_self_exclusive =
                        current.exclusive_owner.as_deref() == Some(client_id.as_str());
                    let is_self_shared = current.shared_owners.contains(client_id.as_str());
                    if !is_self_exclusive && !is_self_shared {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock renew rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        return Ok(FileLockLeaseRenewOutcome::Lost(Some(current)));
                    }

                    current.generation = current.generation.saturating_add(1);
                    current.lease_expires_at_unix_ms = lease_expires_at_unix_ms;
                    let shared_owners: Vec<String> =
                        current.shared_owners.iter().cloned().collect();
                    tx.execute(
                        r#"
                        UPDATE file_lock_leases
                        SET kind = $3,
                            exclusive_owner = $4,
                            shared_owners = $5,
                            lease_expires_at_unix_ms = $6,
                            generation = $7,
                            updated_at = now()
                        WHERE vol = $1 AND ino = $2
                        "#,
                        &[
                            &volume_id,
                            &(ino as i64),
                            &(current.kind as i16),
                            &current.exclusive_owner,
                            &shared_owners,
                            &current.lease_expires_at_unix_ms,
                            &(i64::try_from(current.generation).unwrap_or(i64::MAX)),
                        ],
                    )
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock renew update failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let channel = Self::lock_notify_channel(volume_id);
                    let payload =
                        Self::encode_file_lock_notify_payload(ino, current.generation);
                    let _ = tx
                        .execute("SELECT pg_notify($1, $2)", &[&channel, &payload])
                        .await;
                    tx.commit().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock renew commit failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    Ok(FileLockLeaseRenewOutcome::Renewed(current))
                },
            )
            .await
        })?;
        match &outcome {
            FileLockLeaseRenewOutcome::Renewed(snapshot) => {
                self.remember_held_file_lock(snapshot.ino, lease_ttl);
                self.cache_file_lock_lease(Some(snapshot.clone()));
            }
            FileLockLeaseRenewOutcome::Lost(snapshot) => {
                self.forget_held_file_lock(ino);
                self.upsert_cached_file_lock_lease(snapshot.clone(), ino);
            }
        }
        Ok(outcome)
    }

    fn current_file_lock_lease(&self, ino: Ino) -> MetaResult<Option<FileLockLease>> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        let snapshot = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "read current file lock lease",
                move |client| async move {
                    Self::current_file_lock_lease_with_client(&client, volume_id, ino).await
                },
            )
            .await
        })?;
        self.upsert_cached_file_lock_lease(snapshot.clone(), ino);
        Ok(snapshot)
    }

    fn ensure_known_file_lock_monitoring(&self) -> MetaResult<()> {
        self.ensure_lock_notify_listener_started()
    }

    fn known_file_lock_lease(&self, ino: Ino) -> Option<FileLockLease> {
        let now_ms = Self::now_millis();
        let mut cache = self.file_lock_cache.lock().ok()?;
        let expired = cache
            .get(&ino)
            .map(|lease| lease.lease_expires_at_unix_ms <= now_ms)
            .unwrap_or(false);
        if expired {
            cache.remove(&ino);
            return None;
        }
        cache.get(&ino).cloned()
    }
}

impl MetaFormat for PgMetaEngine {
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()> {
        self.local.set_format_info(info)
    }

    fn get_format_info(&self) -> MetaResult<FormatInfo> {
        self.local.get_format_info()
    }
}
impl MetaInodes for PgMetaEngine {
    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        self.local.get_counter(name)
    }

    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        self.mutate(|local| local.incr_counter(name, delta))
    }

    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        self.mutate(|local| local.decr_counter(name, delta))
    }

    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()> {
        self.mutate(|local| local.set_counter(name, value))
    }

    fn next_inode(&self) -> MetaResult<Ino> {
        let slot = self.client_slot()?;
        self.allocate_local_ids(
            slot,
            INODE_ID_RANGE_SPEC,
            &self.inode_id_cache,
            &self.inode_id_headroom_available,
            1,
        )
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        self.local.get_attr(ino)
    }

    fn get_inode_lifecycle(&self, ino: Ino) -> MetaResult<Option<InodeLifecycleRecord>> {
        self.local.get_inode_lifecycle(ino)
    }

    fn list_inode_lifecycles(&self) -> MetaResult<Vec<InodeLifecycleRecord>> {
        self.local.list_inode_lifecycles()
    }

    fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>> {
        self.local.get_attrs_batch(inos)
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        self.mutate(|local| local.set_attr(attr))
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_attr(ino))
    }

    fn list_inode_attrs(&self) -> MetaResult<Vec<InodeAttr>> {
        self.local.list_inode_attrs()
    }
}
impl MetaNamespace for PgMetaEngine {
    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.local.lookup(parent, name)
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        self.local.readdir(parent)
    }

    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        self.mutate(|local| local.add_entry(parent, name, entry))
    }

    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.mutate(|local| local.remove_entry(parent, name))
    }
}
impl MetaFsOps for PgMetaEngine {
    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.mutate(|local| local.create_file_with_ino(ino, parent, name, uid, gid, mode))
    }

    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.mutate(|local| local.create_dir_with_ino(ino, parent, name, uid, gid, mode))
    }

    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.unlink(parent, name))
    }

    fn unlink_open_handle(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.unlink_open_handle(parent, name))
    }

    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.rmdir(parent, name))
    }

    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<RenameAtomicResult> {
        self.mutate(|local| local.rename_atomic(parent, name, new_parent, new_name))
    }

    fn rename(&self, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.rename(parent, name, new_parent, new_name))
    }

    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.link_atomic(ino, new_parent, new_name))
    }

    fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        self.mutate(|local| local.link(ino, new_parent, new_name))
    }

    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        self.mutate(|local| local.create_symlink_with_ino(ino, parent, name, target, uid, gid))
    }
}
impl MetaBlocks for PgMetaEngine {
    fn get_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>> {
        self.local.get_file_block(ino, block_index)
    }

    fn set_file_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>> {
        self.mutate(|local| local.set_file_block(ino, block_index, block))
    }

    fn get_file_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>> {
        self.local.get_file_blocks(ino)
    }

    fn lookup_regular_file_chunk_slices(
        &self,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_range: SliceRange,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        self.local
            .lookup_regular_file_chunk_slices(ino, chunk_id, chunk_range)
    }

    fn lookup_regular_file_slices(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        self.local.lookup_regular_file_slices(ino, offset, length)
    }

    fn remove_file_blocks(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        self.mutate(|local| local.remove_file_blocks(ino))
    }

    fn remove_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<u64>> {
        self.mutate(|local| local.remove_file_block(ino, block_index))
    }

    fn next_block_id(&self) -> MetaResult<u64> {
        self.next_block_ids(1)?
            .into_iter()
            .next()
            .ok_or_else(|| MetaError::Storage("missing allocated block id".to_string()))
    }

    fn next_block_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let slot = self.client_slot()?;
        let first = self.allocate_local_ids(
            slot,
            BLOCK_ID_RANGE_SPEC,
            &self.block_id_cache,
            &self.block_id_headroom_available,
            count as u64,
        )?;
        Ok((first..first + count as u64).collect())
    }

    fn flush_blocks_batch(&self, entries: &[BatchFlushEntry]) -> MetaResult<Vec<BatchFlushResult>> {
        self.mutate(|local| local.flush_blocks_batch(entries))
    }

    fn flush_regular_file_overlay_slices(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
    ) -> MetaResult<()> {
        self.mutate(|local| local.flush_regular_file_overlay_slices(ino, chunk_slices))
    }

    fn has_local_regular_file_overlay(&self, ino: Ino) -> MetaResult<bool> {
        self.local.has_local_regular_file_overlay(ino)
    }

    fn get_regular_file_overlay_object_ids(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        self.local.get_regular_file_overlay_object_ids(ino)
    }

    fn mark_regular_file_overlay_object_uploaded_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool> {
        let result = self.mutate(|local| {
            local.mark_regular_file_overlay_object_uploaded_if_matches(
                ino,
                expected_epoch,
                object_id,
            )
        });
        if matches!(result, Ok(true)) {
            self.request_pending_flush();
        }
        result
    }

    fn commit_uploaded_block(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool> {
        let result = self
            .mutate(|local| local.commit_uploaded_block(ino, block_index, expected_epoch, block));
        if matches!(result, Ok(true)) {
            self.request_pending_flush();
        }
        result
    }

    fn mark_file_block_uploaded_if_matches(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool> {
        let result = self.mutate(|local| {
            local.mark_file_block_uploaded_if_matches(ino, block_index, expected_epoch, block)
        });
        if matches!(result, Ok(true)) {
            self.request_pending_flush();
        }
        result
    }

    fn publish_file_manifest_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        expected_manifest: &[(u32, BlockRef)],
    ) -> MetaResult<bool> {
        let result = self.mutate(|local| {
            local.publish_file_manifest_if_matches(ino, expected_epoch, expected_manifest)
        });
        if matches!(result, Ok(true)) {
            self.request_pending_flush();
        }
        result
    }

    fn publish_regular_file_overlay_to_base_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
    ) -> MetaResult<bool> {
        let result = self.mutate(|local| {
            local.publish_regular_file_overlay_to_base_if_matches(ino, expected_epoch)
        });
        if matches!(result, Ok(true)) {
            self.request_pending_flush();
        }
        result
    }

    fn compact_fragmented_regular_file_chunks(&self, max_chunks: usize) -> MetaResult<usize> {
        let result = self.mutate(|local| local.compact_fragmented_regular_file_chunks(max_chunks));
        if matches!(result, Ok(count) if count > 0) {
            self.request_pending_flush();
        }
        result
    }
}
impl MetaGc for PgMetaEngine {
    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()> {
        self.mutate(|local| local.add_pending_deletion(object_id, timestamp))
    }

    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()> {
        self.mutate(|local| local.remove_pending_deletion(object_id))
    }

    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        self.local.get_pending_deletions()
    }

    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        self.local.get_referenced_object_ids()
    }
}
impl MetaRecovery for PgMetaEngine {
    fn add_deferred_delete(&self, record: &DeferredDeleteRecord) -> MetaResult<()> {
        self.mutate(|local| local.add_deferred_delete(record))
    }

    fn remove_deferred_delete(&self, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_deferred_delete(ino))
    }

    fn list_deferred_deletes(&self) -> MetaResult<Vec<DeferredDeleteRecord>> {
        self.local.list_deferred_deletes()
    }

    fn recover_pending_creates(&self) -> MetaResult<Vec<Ino>> {
        self.mutate(|local| local.recover_pending_creates())
    }

    fn add_pending_create(&self, record: &PendingCreateRecord) -> MetaResult<()> {
        self.mutate(|local| local.add_pending_create(record))
    }

    fn remove_pending_create(&self, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_pending_create(ino))
    }

    fn list_pending_creates(&self) -> MetaResult<Vec<PendingCreateRecord>> {
        self.local.list_pending_creates()
    }

    fn recover_undo_logs(&self) -> MetaResult<Vec<Ino>> {
        self.mutate(|local| local.recover_undo_logs())
    }

    fn append_undo_log(&self, record: &UndoLogRecord) -> MetaResult<()> {
        self.mutate(|local| local.append_undo_log(record))
    }

    fn remove_undo_logs_for(&self, kind: UndoLogKind, ino: Ino) -> MetaResult<()> {
        self.mutate(|local| local.remove_undo_logs_for(kind, ino))
    }

    fn list_undo_logs(&self) -> MetaResult<Vec<UndoLogRecord>> {
        self.local.list_undo_logs()
    }
}
impl MetaSymlink for PgMetaEngine {
    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.set_symlink_target(ino, target))
    }

    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>> {
        self.local.get_symlink_target(ino)
    }
}
impl MetaXattr for PgMetaEngine {
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        self.local.get_xattr(ino, name)
    }

    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()> {
        self.mutate(|local| local.set_xattr(ino, name, value, flags))
    }

    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()> {
        self.mutate(|local| local.remove_xattr(ino, name))
    }

    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        self.local.list_xattrs(ino)
    }
}
impl MetaAcl for PgMetaEngine {
    fn set_acl_rule(&self, ino: Ino, principal: &str, access_level: AccessLevel) -> MetaResult<()> {
        self.mutate(|local| local.set_acl_rule(ino, principal, access_level))
    }

    fn remove_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<()> {
        self.mutate(|local| local.remove_acl_rule(ino, principal))
    }

    fn get_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<Option<AccessLevel>> {
        self.local.get_acl_rule(ino, principal)
    }
}
impl MetaSnapshots for PgMetaEngine {
    fn create_snapshot(&self, root_ino: Ino) -> MetaResult<MetadataSnapshot> {
        self.mutate(|local| local.create_snapshot(root_ino))
    }

    fn list_snapshots(&self) -> MetaResult<Vec<MetadataSnapshot>> {
        let _ = self.sync_metadata();
        self.local.list_snapshots()
    }

    fn get_snapshot(&self, snapshot_id: &str) -> MetaResult<Option<MetadataSnapshot>> {
        let _ = self.sync_metadata();
        self.local.get_snapshot(snapshot_id)
    }
}
impl MetaReplication for PgMetaEngine {
    fn flush_metadata(&self) -> MetaResult<()> {
        let _guard = self
            .replication_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
        let recent_flush = if let Some(batch) = self.claim_pending_batch(true) {
            self.flush_pending_claimed_batch_with_restore(batch)?
        } else {
            None
        };
        let _ = self.pull_foreign_events_locked(recent_flush.as_ref())?;
        self.write_checkpoint_locked()?;
        Ok(())
    }

    fn request_eager_flush(&self) {
        self.request_pending_flush();
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        let _guard = self
            .replication_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg replication lock poisoned".to_string()))?;
        let _ = self.pull_foreign_events_locked(None)?;
        Ok(())
    }

    fn invalidation_sender_id(&self) -> Option<String> {
        Some(self.client_id.clone())
    }

    fn metadata_version_token(&self) -> Option<String> {
        Some(self.local_version.load(Ordering::SeqCst).to_string())
    }

    fn namespace_version_token(&self) -> Option<String> {
        Some(self.namespace_version.load(Ordering::SeqCst).to_string())
    }

    fn replication_backlog(&self) -> crate::engine::MetadataReplicationBacklog {
        let pending_ops = self
            .pending_state
            .lock()
            .map(|state| state.ops)
            .unwrap_or(0);
        let inflight_ops = self.inflight_replication_ops.load(Ordering::SeqCst);
        crate::engine::MetadataReplicationBacklog {
            pending_ops,
            inflight_ops,
        }
    }

    fn metadata_authority_status(&self) -> MetadataAuthorityStatus {
        MetadataAuthorityStatus {
            inode_id_headroom: self.inode_id_headroom_available.load(Ordering::Acquire),
            block_id_headroom: self.block_id_headroom_available.load(Ordering::Acquire),
        }
    }

    fn check_consistency(&self) -> MetaResult<()> {
        let local_info = self.local.get_format_info()?;
        let remote_uuid = self.fetch_remote_uuid(self.volume_id)?;
        if local_info.uuid != remote_uuid {
            return Err(MetaError::UuidMismatch {
                local: local_info.uuid,
                remote: remote_uuid,
            });
        }
        Ok(())
    }

    fn remote_metadata_size(&self) -> MetaResult<Option<u64>> {
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        let size = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "fetch remote metadata size",
                move |client| async move {
                    let row = client
                        .query_one(
                            r#"
                            SELECT 
                                (SELECT COALESCE(SUM(pg_column_size(snapshot)), 0) FROM metadata_state WHERE vol = $1) +
                                (SELECT COALESCE(SUM(pg_column_size(event_payload)), 0) FROM event_log WHERE vol = $1) +
                                (SELECT COALESCE(SUM(pg_column_size(snapshot_payload)), 0) FROM snapshots WHERE vol = $1)
                            "#,
                            &[&volume_id],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres metadata size query failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    let bytes: i64 = row.get(0);
                    Ok(bytes.max(0) as u64)
                },
            )
            .await
        })?;
        Ok(Some(size))
    }

    fn prepare_for_close(&self) {
        // Signal the background poll thread to exit and wait for it.
        // This ensures the thread's `Arc<PgMetaEngine>` (from Weak::upgrade)
        // is dropped before the caller proceeds with process exit, so that
        // `RedbMetaEngine` / `redb::Database` can run their Drop impls
        // cleanly — preventing "Database repair aborted" on next mount.
        //
        // Cancel the notify listener token first so any in-flight postgres
        // awaits (e.g. `refresh_known_file_lock_lease_with_client`) are
        // interrupted immediately rather than waiting up to command_timeout.
        self.lock_notify_cancel.cancel();
        self.poll_shutdown.store(true, Ordering::Release);
        let join_timeout = Self::default_prepare_for_close_join_timeout();
        if let Ok(mut guard) = self.poll_handle.lock()
            && let Some(handle) = guard.take()
        {
            Self::join_background_thread_with_timeout(handle, join_timeout, "poll");
        }
        if let Ok(mut guard) = self.lock_notify_handle.lock()
            && let Some(handle) = guard.take()
        {
            Self::join_background_thread_with_timeout(handle, join_timeout, "notify");
        }
        if let Ok(mut guard) = self.lock_renew_handle.lock()
            && let Some(handle) = guard.take()
        {
            Self::join_background_thread_with_timeout(handle, join_timeout, "renew");
        }
    }
}
impl crate::engine::MetaEngine for PgMetaEngine {
    fn flush_blocks_batch_with_attr(
        &self,
        entries: &[BatchFlushEntry],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Vec<BatchFlushResult>> {
        self.mutate(|local| local.flush_blocks_batch_with_attr(entries, attr))
    }

    fn flush_regular_file_overlay_slices_with_attr(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        self.mutate(|local| {
            local.flush_regular_file_overlay_slices_with_attr(ino, chunk_slices, attr)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_dsn::ensure_studiofs_database;
    use std::cell::{Cell, RefCell};
    use tempfile::tempdir;
    use uuid::Uuid;

    fn pg_err(e: tokio_postgres::Error) -> String {
        if let Some(db) = e.as_db_error() {
            format!("{} (sqlstate={})", db.message(), db.code().code())
        } else {
            e.to_string()
        }
    }

    fn workspace_env_path() -> std::path::PathBuf {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(|path| path.parent())
            .map(std::path::Path::to_path_buf)
            .unwrap_or(manifest_dir)
            .join(".env")
    }

    fn pg_dsn() -> Option<String> {
        let env_path = workspace_env_path();
        if env_path.is_file() {
            let _ = dotenvy::from_path(&env_path);
        } else {
            let _ = dotenvy::dotenv();
        }
        let raw = std::env::var("STUDIOFS_TEST_PG_DSN")
            .ok()
            .filter(|v| !v.trim().is_empty())?;
        run_async(async { ensure_studiofs_database(&raw).await })
            .ok()
            .map(|v| v.metadata_dsn)
    }

    fn run_async<T, F>(fut: F) -> MetaResult<T>
    where
        F: std::future::Future<Output = MetaResult<T>>,
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        rt.block_on(fut)
    }

    fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        cond()
    }

    fn wait_visible(engine: &PgMetaEngine, parent: Ino, name: &[u8], timeout: Duration) -> bool {
        wait_until(timeout, || {
            let _ = engine.sync_metadata();
            engine.lookup(parent, name).is_ok()
        })
    }

    fn wait_hidden(engine: &PgMetaEngine, parent: Ino, name: &[u8], timeout: Duration) -> bool {
        wait_until(timeout, || {
            let _ = engine.sync_metadata();
            matches!(
                engine.lookup(parent, name),
                Err(MetaError::EntryNotFound { .. })
            )
        })
    }

    fn drop_file_lock_lease_table(dsn: &str) {
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres connect failed: {}", pg_err(e)))
                })?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .execute("DROP TABLE IF EXISTS file_lock_leases", &[])
                .await
                .map_err(|e| {
                    MetaError::Storage(format!(
                        "postgres drop file_lock_leases failed: {}",
                        pg_err(e)
                    ))
                })?;
            conn_task.abort();
            Ok::<(), MetaError>(())
        })
        .expect("drop file_lock_leases");
    }

    fn reopen_pg_engine_with_retry(path: &std::path::Path) -> Arc<PgMetaEngine> {
        let mut reopened = None;
        assert!(wait_until(Duration::from_secs(3), || {
            match PgMetaEngine::open(path) {
                Ok(engine) => {
                    reopened = Some(engine);
                    true
                }
                Err(_) => false,
            }
        }));
        reopened.expect("reopen pg engine")
    }

    struct PgTestVolume {
        dsn: String,
        name: String,
        uuid: String,
        volume_id: i64,
    }

    impl PgTestVolume {
        fn new(dsn: &str, prefix: &str) -> Self {
            let name = format!(
                "{prefix}-{}-{}",
                std::process::id(),
                Uuid::new_v4().simple()
            );
            let uuid = Uuid::new_v4().to_string();
            let volume_id =
                setup_pg_volume(dsn, &name, &uuid, DEFAULT_BLOCK_SIZE).expect("setup pg");
            Self {
                dsn: dsn.to_string(),
                name,
                uuid,
                volume_id,
            }
        }

        fn format_info(&self) -> FormatInfo {
            format_info(&self.name, &self.uuid, &self.dsn, self.volume_id)
        }

        fn update_uuid(&self, new_uuid: &str) {
            let _ = run_async(async {
                let (client, connection) =
                    tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls)
                        .await
                        .expect("connect");
                let conn_task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                client
                    .execute(
                        "UPDATE volumes SET uuid = $1 WHERE vol = $2",
                        &[&new_uuid, &self.volume_id],
                    )
                    .await
                    .expect("update uuid");
                conn_task.abort();
                Ok::<(), MetaError>(())
            });
        }
    }

    impl Drop for PgTestVolume {
        fn drop(&mut self) {
            let _ = run_async(async {
                let (client, connection) =
                    tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls)
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!("postgres connect failed: {}", pg_err(e)))
                        })?;
                let conn_task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                client
                    .execute("DELETE FROM volumes WHERE vol = $1", &[&self.volume_id])
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!("postgres cleanup failed: {}", pg_err(e)))
                    })?;
                conn_task.abort();
                Ok::<(), MetaError>(())
            });
        }
    }

    fn seed_local(path: &std::path::Path, info: &FormatInfo) {
        let seed = RedbMetaEngine::open(path).expect("seed open");
        seed.set_format_info(info).expect("set format");
        seed.init_root(0, 0).expect("init root");
        let next_event_seq = seed.get_counter("nextEventSeq").expect("nextEventSeq");
        seed.set_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, next_event_seq)
            .expect("seed flushed event seq");
    }

    thread_local! {
        static TEST_SCHEMA_ENSURE_OVERRIDE: RefCell<Option<String>> = const { RefCell::new(None) };
        static TEST_HEADROOM_REFRESH_OVERRIDE: Cell<Option<bool>> = const { Cell::new(None) };
    }

    struct TestSchemaEnsureOverrideGuard {
        previous: Option<String>,
    }

    impl Drop for TestSchemaEnsureOverrideGuard {
        fn drop(&mut self) {
            TEST_SCHEMA_ENSURE_OVERRIDE.with(|override_state| {
                *override_state.borrow_mut() = self.previous.take();
            });
        }
    }

    fn set_schema_ensure_test_override(message: &str) -> TestSchemaEnsureOverrideGuard {
        let previous = TEST_SCHEMA_ENSURE_OVERRIDE
            .with(|override_state| override_state.borrow_mut().replace(message.to_string()));
        TestSchemaEnsureOverrideGuard { previous }
    }

    pub(super) fn schema_ensure_test_override() -> Option<String> {
        TEST_SCHEMA_ENSURE_OVERRIDE.with(|override_state| override_state.borrow().clone())
    }

    struct TestHeadroomRefreshOverrideGuard {
        previous: Option<bool>,
    }

    impl Drop for TestHeadroomRefreshOverrideGuard {
        fn drop(&mut self) {
            TEST_HEADROOM_REFRESH_OVERRIDE.with(|override_state| {
                override_state.set(self.previous.take());
            });
        }
    }

    fn set_headroom_refresh_test_override(value: bool) -> TestHeadroomRefreshOverrideGuard {
        let previous = TEST_HEADROOM_REFRESH_OVERRIDE.with(|override_state| {
            let previous = override_state.get();
            override_state.set(Some(value));
            previous
        });
        TestHeadroomRefreshOverrideGuard { previous }
    }

    pub(super) fn headroom_refresh_test_override() -> Option<bool> {
        TEST_HEADROOM_REFRESH_OVERRIDE.with(Cell::get)
    }

    struct TestEnvGuard {
        key: &'static str,
        previous: Option<String>,
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                unsafe {
                    std::env::set_var(self.key, previous);
                }
            } else {
                unsafe {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    fn set_test_env(key: &'static str, value: &str) -> TestEnvGuard {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        TestEnvGuard { key, previous }
    }

    fn set_file_state(engine: &PgMetaEngine, ino: Ino, size: u64, content_epoch: u64) {
        let mut attr = engine.get_attr(ino).expect("get file attr");
        attr.size = size;
        attr.content_epoch = content_epoch;
        attr.mtime = InodeAttr::now_nanos();
        attr.ctime = attr.mtime;
        engine.set_attr(&attr).expect("set file attr");
    }

    fn create_published_small_file(
        engine: &PgMetaEngine,
        parent: Ino,
        name: &[u8],
        object_id: u64,
    ) -> InodeAttr {
        let file = engine
            .create_file(parent, name, 0, 0, 0o644)
            .expect("create published file");
        set_file_state(engine, file.ino, 64, 1);
        engine
            .set_file_block(file.ino, 0, &BlockRef::new_partial(object_id, 64))
            .expect("set published file block");
        assert!(
            engine
                .publish_file_manifest(file.ino, 1)
                .expect("publish small file"),
            "small file publish must succeed"
        );
        file
    }

    fn temp_pg_engine_for_unit() -> (tempfile::TempDir, PgMetaEngine) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("meta.redb");
        let local = Arc::new(RedbMetaEngine::open(&path).expect("open local redb"));
        local.init_root(0, 0).expect("init root");
        local
            .set_format_info(&format_info(
                "unit-local",
                "unit-local-uuid",
                "postgres://127.0.0.1:1/studiofs",
                1,
            ))
            .expect("set local format info");
        local
            .set_remote_namespace_visibility_filter(true)
            .expect("enable remote namespace visibility filter");
        let flushed_event_seq = local.get_counter("nextEventSeq").expect("nextEventSeq");

        let engine = PgMetaEngine {
            local,
            postgres_url: "postgres://127.0.0.1:1/studiofs".to_string(),
            volume_id: 1,
            client_id: "unit-test".to_string(),
            client_slot_plus_one: AtomicU64::new(0),
            local_version: AtomicI64::new(0),
            namespace_version: AtomicI64::new(0),
            flushed_event_seq: AtomicU64::new(flushed_event_seq),
            replication_lock: Mutex::new(()),
            poll_interval: Duration::from_secs(60),
            flush_interval: Duration::from_secs(60),
            flush_idle_grace: Duration::from_secs(60),
            flush_ops_threshold: 8,
            connect_timeout: Duration::from_millis(50),
            command_timeout: Duration::from_millis(50),
            inflight_replication_ops: AtomicU32::new(0),
            pending_state: Mutex::new(PendingReplicationState::default()),
            inode_id_cache: Mutex::new(LocalIdLeaseCache::default()),
            block_id_cache: Mutex::new(LocalIdLeaseCache::default()),
            inode_id_headroom_available: AtomicBool::new(false),
            block_id_headroom_available: AtomicBool::new(false),
            janitor_lock: Mutex::new(None),
            file_lock_cache: Mutex::new(HashMap::new()),
            held_file_locks: Mutex::new(HashMap::new()),
            self_weak: OnceLock::new(),
            lock_workers_enabled: false,
            lock_worker_start_lock: Mutex::new(()),
            poll_shutdown: Arc::new(AtomicBool::new(false)),
            lock_notify_cancel: CancellationToken::new(),
            poll_handle: Mutex::new(None),
            lock_notify_handle: Mutex::new(None),
            lock_renew_handle: Mutex::new(None),
        };

        PgMetaEngine::persist_client_slot(&engine.local, 0).expect("persist unit test client slot");
        engine
            .prime_local_id_ranges(0)
            .expect("prime unit test local id ranges");

        (dir, engine)
    }

    #[test]
    fn prepare_for_close_does_not_block_forever_on_slow_background_thread() {
        let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
        let (_dir, engine) = temp_pg_engine_for_unit();
        *engine.poll_handle.lock().expect("poll_handle lock") = Some(
            std::thread::Builder::new()
                .name("pg-test-slow-close".into())
                .spawn(|| {
                    std::thread::sleep(Duration::from_secs(5));
                })
                .expect("spawn slow background thread"),
        );

        let started = Instant::now();
        engine.prepare_for_close();

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "prepare_for_close should respect the bounded join timeout for slow threads"
        );
    }

    fn apply_projected_remote_range(
        writer: &PgMetaEngine,
        remote: &PgMetaEngine,
        start: u64,
        end: u64,
    ) {
        let events = writer
            .local
            .read_events_range(start, end)
            .expect("read local events");
        let projected = PgMetaEngine::project_remote_events(&events);
        remote
            .apply_foreign_batch(&projected, &OverlayConflictSets::default())
            .expect("apply projected remote batch");
    }

    fn setup_pg_volume(dsn: &str, name: &str, uuid: &str, block_size: u32) -> MetaResult<i64> {
        run_async(async move {
            let (mut client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres connect failed: {}", pg_err(e)))
                })?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            let tx = client.transaction().await.map_err(|e| {
                MetaError::Storage(format!("postgres begin tx failed: {}", pg_err(e)))
            })?;

            tx.execute("SELECT pg_advisory_xact_lock($1)", &[&902_202_603_i64])
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres advisory lock failed: {}", pg_err(e)))
                })?;

            tx.batch_execute(
                r#"
                CREATE TABLE IF NOT EXISTS volumes (
                    vol BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    uuid TEXT NOT NULL UNIQUE,
                    block_size INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS event_log (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    seq_id BIGSERIAL PRIMARY KEY,
                    batch_version BIGINT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_payload BYTEA NOT NULL,
                    client_id TEXT,
                    client_timestamp BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS metadata_state (
                    vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
                    version BIGINT NOT NULL,
                    snapshot BYTEA NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS acl_rules (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    ino BIGINT NOT NULL,
                    principal TEXT NOT NULL,
                    access_level SMALLINT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (vol, ino, principal)
                );
                CREATE TABLE IF NOT EXISTS snapshots (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    id TEXT NOT NULL,
                    root_ino BIGINT NOT NULL,
                    created_at BIGINT NOT NULL,
                    state SMALLINT NOT NULL,
                    pinned_object_ids BIGINT[] NOT NULL DEFAULT '{}',
                    snapshot_payload BYTEA NOT NULL DEFAULT ''::bytea,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (vol, id)
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
                ALTER TABLE event_log
                    ADD COLUMN IF NOT EXISTS batch_version BIGINT;
                UPDATE event_log
                    SET batch_version = 0
                    WHERE batch_version IS NULL;
                ALTER TABLE event_log
                    ALTER COLUMN batch_version SET NOT NULL;
                "#,
            )
            .await
            .map_err(|e| {
                MetaError::Storage(format!("postgres schema ensure failed: {}", pg_err(e)))
            })?;

            let row = tx
                .query_one(
                    r#"
                    INSERT INTO volumes (name, uuid, block_size)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (uuid) DO UPDATE
                    SET name = EXCLUDED.name,
                        block_size = EXCLUDED.block_size
                    RETURNING vol
                    "#,
                    &[&name, &uuid, &(block_size as i32)],
                )
                .await
                .map_err(|e| {
                    MetaError::Storage(format!("postgres volume upsert failed: {}", pg_err(e)))
                })?;
            let volume_id: i64 = row.get(0);

            let empty_snapshot = serde_json::to_vec(&RedbSnapshot::default())
                .map_err(|e| MetaError::Serialization(format!("snapshot encode failed: {e}")))?;
            tx.execute(
                r#"
                INSERT INTO metadata_state (vol, version, snapshot)
                VALUES ($1, 0, $2)
                ON CONFLICT (vol) DO NOTHING
                "#,
                &[&volume_id, &empty_snapshot],
            )
            .await
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres metadata_state seed failed: {}",
                    pg_err(e)
                ))
            })?;

            tx.commit().await.map_err(|e| {
                MetaError::Storage(format!("postgres commit failed: {}", pg_err(e)))
            })?;
            conn_task.abort();
            Ok(volume_id)
        })
    }

    fn format_info(name: &str, uuid: &str, dsn: &str, volume_id: i64) -> FormatInfo {
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: name.to_string(),
            uuid: uuid.to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "studiofs-test".to_string(),
            access_key: "test".to_string(),
            secret_key: "test".to_string(),
            region: "us-east-1".to_string(),
            data_prefix: format!("{name}/objects/"),
            metadata: MetadataConfig {
                backend: MetadataBackend::Postgres,
                remote: None,
                postgres: Some(PostgresConfig {
                    url: dsn.to_string(),
                    volume_id,
                }),
            },
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }

    #[test]
    fn test_local_overlay_conflicts_reserve_pending_xattr_and_symlink() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        let file = pg
            .local
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .expect("create file");
        let sym = pg
            .local
            .create_symlink(ROOT_INO, b"link.txt", b"target-a", 0, 0)
            .expect("create symlink");

        let baseline = pg
            .local
            .get_counter("nextEventSeq")
            .expect("read baseline seq");
        {
            let mut state = pg.pending_state.lock().expect("pending_state");
            *state = PendingReplicationState::default();
        }

        pg.set_xattr(file.ino, b"user.color", b"red", 0)
            .expect("set local xattr");
        pg.set_symlink_target(sym.ino, b"target-b")
            .expect("set local symlink");

        let overlay = pg.local_overlay_conflicts().expect("overlay conflicts");
        assert!(
            !overlay.block_inodes.contains(&file.ino),
            "xattr reservation should not masquerade as block overlay"
        );
        assert!(overlay.symlink_inodes.contains(&sym.ino));
        assert!(overlay.xattr_inodes.contains(&file.ino));
        assert!(
            overlay
                .xattr_keys
                .contains(&PgMetaEngine::xattr_key(file.ino, b"user.color"))
        );
        assert!(
            !overlay
                .xattr_keys
                .contains(&PgMetaEngine::xattr_key(file.ino, b"user.other"))
        );
        assert!(
            pg.aux_event_conflicts_with_overlay(
                &MetaEvent::SetXattr {
                    ino: file.ino,
                    name: b"user.color".to_vec(),
                    value: b"blue".to_vec(),
                    flags: 0,
                },
                &overlay,
                &HashSet::new(),
            )
            .expect("xattr conflict")
        );
        assert!(
            pg.aux_event_conflicts_with_overlay(
                &MetaEvent::SetSymlinkTarget {
                    ino: sym.ino,
                    target: b"target-c".to_vec(),
                },
                &overlay,
                &HashSet::new(),
            )
            .expect("symlink conflict")
        );

        let state = pg.pending_state.lock().expect("pending_state");
        let start = state.event_start.expect("pending start set");
        let end = state.event_end;
        assert_eq!(start, baseline);
        assert!(end > start);
    }

    #[test]
    fn startup_snapshot_is_skipped_when_local_overlay_exists() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        pg.local
            .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
            .expect("create local draft");

        assert!(
            PgMetaEngine::has_any_local_overlay_mutation(&pg.local)
                .expect("local overlay mutation"),
            "local draft namespace should register as a local overlay mutation"
        );
        assert!(
            !PgMetaEngine::should_pull_startup_snapshot(&pg.local, 0)
                .expect("startup snapshot decision"),
            "startup snapshot must not clobber a local overlay after crash/restart"
        );
    }

    #[test]
    fn persisted_pg_version_roundtrips_in_local_counter() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        pg.persist_local_version(42).expect("persist local version");

        assert_eq!(pg.local_version.load(Ordering::SeqCst), 42);
        assert_eq!(
            PgMetaEngine::load_persisted_local_version(&pg.local).expect("reload local version"),
            42
        );
        assert!(
            !PgMetaEngine::should_pull_startup_snapshot(&pg.local, 42)
                .expect("startup snapshot decision"),
            "persisted PG version should suppress checkpoint bootstrap overwrite"
        );
    }

    #[test]
    fn persisted_pg_client_slot_roundtrips_in_local_counter() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("meta.redb");
        let local = RedbMetaEngine::open(&path).expect("open local redb");
        local.init_root(0, 0).expect("init root");

        assert_eq!(
            PgMetaEngine::load_persisted_client_slot(&local).expect("initial client slot"),
            None
        );
        PgMetaEngine::persist_client_slot(&local, 7).expect("persist client slot");
        assert_eq!(
            PgMetaEngine::load_persisted_client_slot(&local).expect("reload client slot"),
            Some(7)
        );
    }

    #[test]
    fn prime_local_id_ranges_persists_reserved_headroom_boundaries() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        PgMetaEngine::persist_client_slot(&pg.local, 3).expect("persist client slot");
        pg.prime_local_id_ranges(3)
            .expect("prime local id headroom ranges");

        let next_inode = pg
            .local
            .get_counter(counters::NEXT_INODE)
            .expect("next inode counter");
        let next_block = pg
            .local
            .get_counter(counters::NEXT_BLOCK_ID)
            .expect("next block counter");
        let inode_active_end = pg
            .local
            .get_counter("pgInodeActiveRangeEnd")
            .expect("inode active range end");
        let inode_reserved_end = pg
            .local
            .get_counter("pgInodeReservedRangeEnd")
            .expect("inode reserved range end");
        let block_active_end = pg
            .local
            .get_counter("pgBlockActiveRangeEnd")
            .expect("block active range end");
        let block_reserved_end = pg
            .local
            .get_counter("pgBlockReservedRangeEnd")
            .expect("block reserved range end");

        assert!(
            inode_active_end > next_inode,
            "inode active range must extend beyond the current cursor"
        );
        assert!(
            inode_reserved_end > inode_active_end,
            "inode reserved headroom should include at least one future window"
        );
        assert!(
            block_active_end > next_block,
            "block active range must extend beyond the current cursor"
        );
        assert!(
            block_reserved_end > block_active_end,
            "block reserved headroom should include at least one future window"
        );
    }

    #[test]
    fn open_with_persisted_slot_and_ranges_survives_pg_unavailable() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("meta.redb");
        let local = RedbMetaEngine::open(&path).expect("open local redb");
        local.init_root(0, 0).expect("init root");
        local
            .set_format_info(&format_info(
                "offline-reopen",
                "offline-reopen-uuid",
                "postgres://127.0.0.1:1/studiofs",
                1,
            ))
            .expect("set format info");

        PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
        let inode_base =
            PgMetaEngine::reserve_counter_range_base(5, PG_INODE_ID_STRIDE, ROOT_INO + 1);
        let block_base = PgMetaEngine::reserve_counter_range_base(5, PG_BLOCK_ID_STRIDE, 1);
        local
            .set_counter(counters::NEXT_INODE, inode_base + 2)
            .expect("persist next inode");
        local
            .set_counter(counters::NEXT_BLOCK_ID, block_base + 2)
            .expect("persist next block");
        local
            .set_counter("pgInodeActiveRangeStart", inode_base)
            .expect("persist inode active start");
        local
            .set_counter("pgInodeActiveRangeEnd", inode_base + 8)
            .expect("persist inode active end");
        local
            .set_counter("pgInodeReservedRangeEnd", inode_base + 16)
            .expect("persist inode reserved end");
        local
            .set_counter("pgBlockActiveRangeStart", block_base)
            .expect("persist block active start");
        local
            .set_counter("pgBlockActiveRangeEnd", block_base + 8)
            .expect("persist block active end");
        local
            .set_counter("pgBlockReservedRangeEnd", block_base + 16)
            .expect("persist block reserved end");
        drop(local);

        let reopened = PgMetaEngine::open_with_options(
            &path,
            PgOpenOptions {
                background_poll: false,
            },
        );
        let reopen_error = reopened.err().map(|err| err.to_string());
        assert!(
            reopen_error.is_none(),
            "engine should reopen from persisted client slot + headroom even while PG is unavailable: {reopen_error:?}"
        );
    }

    #[test]
    fn open_with_persisted_slot_without_ranges_still_requires_pg() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("meta.redb");
        let local = RedbMetaEngine::open(&path).expect("open local redb");
        local.init_root(0, 0).expect("init root");
        local
            .set_format_info(&format_info(
                "offline-reopen-no-ranges",
                "offline-reopen-no-ranges-uuid",
                "postgres://127.0.0.1:1/studiofs",
                1,
            ))
            .expect("set format info");
        PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
        drop(local);

        let reopened = PgMetaEngine::open_with_options(
            &path,
            PgOpenOptions {
                background_poll: false,
            },
        );
        assert!(
            reopened.is_err(),
            "offline reopen must not mint fresh headroom from a legacy client slot without persisted ranges"
        );
    }

    #[test]
    fn open_with_persisted_ranges_does_not_mask_non_transient_schema_errors() {
        let _schema_error = set_schema_ensure_test_override(
            "postgres file lock lease schema ensure failed: permission denied (sqlstate=42501)",
        );
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("meta.redb");
        let local = RedbMetaEngine::open(&path).expect("open local redb");
        local.init_root(0, 0).expect("init root");
        local
            .set_format_info(&format_info(
                "offline-reopen-sqlstate",
                "offline-reopen-sqlstate-uuid",
                "postgres://127.0.0.1:1/studiofs",
                1,
            ))
            .expect("set format info");

        PgMetaEngine::persist_client_slot(&local, 5).expect("persist client slot");
        let inode_base =
            PgMetaEngine::reserve_counter_range_base(5, PG_INODE_ID_STRIDE, ROOT_INO + 1);
        let block_base = PgMetaEngine::reserve_counter_range_base(5, PG_BLOCK_ID_STRIDE, 1);
        local
            .set_counter(counters::NEXT_INODE, inode_base + 2)
            .expect("persist next inode");
        local
            .set_counter(counters::NEXT_BLOCK_ID, block_base + 2)
            .expect("persist next block");
        local
            .set_counter("pgInodeActiveRangeStart", inode_base)
            .expect("persist inode active start");
        local
            .set_counter("pgInodeActiveRangeEnd", inode_base + 8)
            .expect("persist inode active end");
        local
            .set_counter("pgInodeReservedRangeEnd", inode_base + 16)
            .expect("persist inode reserved end");
        local
            .set_counter("pgBlockActiveRangeStart", block_base)
            .expect("persist block active start");
        local
            .set_counter("pgBlockActiveRangeEnd", block_base + 8)
            .expect("persist block active end");
        local
            .set_counter("pgBlockReservedRangeEnd", block_base + 16)
            .expect("persist block reserved end");
        drop(local);

        let reopened = PgMetaEngine::open_with_options(
            &path,
            PgOpenOptions {
                background_poll: false,
            },
        );
        assert!(
            reopened.is_err(),
            "non-transient schema/bootstrap failures must not be silently treated as offline-mode reopens"
        );
    }

    #[test]
    fn next_inode_advances_persisted_active_range_when_window_boundary_is_crossed() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        PgMetaEngine::persist_client_slot(&pg.local, 0).expect("persist client slot");
        let base = PgMetaEngine::reserve_counter_range_base(0, PG_INODE_ID_STRIDE, ROOT_INO + 1);
        pg.local
            .set_counter(counters::NEXT_INODE, base + 1)
            .expect("persist next inode");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, base)
            .expect("persist inode active start");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, base + 2)
            .expect("persist inode active end");
        pg.local
            .set_counter(LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, base + 4)
            .expect("persist inode reserved end");

        let allocated = pg.next_inode().expect("allocate boundary inode");
        assert_eq!(allocated, base + 1);
        assert_eq!(
            pg.local
                .get_counter(LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER)
                .expect("inode active start"),
            base + 2
        );
        assert_eq!(
            pg.local
                .get_counter(LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER)
                .expect("inode active end"),
            base + 4
        );
        assert_eq!(
            pg.local
                .get_counter(LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER)
                .expect("inode reserved end"),
            base + 4,
            "advancing into the already-reserved window must not fabricate a new future reservation"
        );
    }

    #[test]
    fn next_inode_stops_once_reserved_window_is_consumed() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        PgMetaEngine::persist_client_slot(&pg.local, 0).expect("persist client slot");
        let base = PgMetaEngine::reserve_counter_range_base(0, PG_INODE_ID_STRIDE, ROOT_INO + 1);
        pg.local
            .set_counter(counters::NEXT_INODE, base + 3)
            .expect("persist next inode");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, base + 2)
            .expect("persist inode active start");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, base + 4)
            .expect("persist inode active end");
        pg.local
            .set_counter(LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, base + 4)
            .expect("persist inode reserved end");

        let last = pg.next_inode().expect("final inode in reserved window");
        assert_eq!(last, base + 3);

        let authority = MetaReplication::metadata_authority_status(&pg);
        assert!(
            !authority.allows_new_mutations(),
            "consuming the final reserved inode should degrade the engine to read-only"
        );
        let error = pg
            .next_inode()
            .expect_err("allocator must stop once the reserved window is exhausted");
        assert!(
            matches!(error, MetaError::Storage(ref message) if message.contains("headroom exhausted")),
            "unexpected exhaustion error: {error}"
        );
    }

    #[test]
    fn next_inode_refreshes_reserved_headroom_when_pg_is_available() {
        let _pg_refresh = set_headroom_refresh_test_override(true);
        let (_dir, pg) = temp_pg_engine_for_unit();

        PgMetaEngine::persist_client_slot(&pg.local, 0).expect("persist client slot");
        let base = PgMetaEngine::reserve_counter_range_base(0, PG_INODE_ID_STRIDE, ROOT_INO + 1);
        pg.local
            .set_counter(counters::NEXT_INODE, base + 3)
            .expect("persist next inode");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, base + 2)
            .expect("persist inode active start");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, base + 4)
            .expect("persist inode active end");
        pg.local
            .set_counter(LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, base + 4)
            .expect("persist inode reserved end");

        let first = pg
            .next_inode()
            .expect("allocate final inode before refresh");
        let second = pg
            .next_inode()
            .expect("pg-backed refresh should reserve more headroom before going read-only");
        assert_eq!(first, base + 3);
        assert_eq!(second, base + 4);
        assert!(
            pg.local
                .get_counter(LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER)
                .expect("inode reserved end")
                > base + 4,
            "online refresh should extend the persisted reserved headroom"
        );
    }

    #[test]
    fn create_file_respects_inode_headroom_exhaustion() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        PgMetaEngine::persist_client_slot(&pg.local, 0).expect("persist client slot");
        let limit = PgMetaEngine::counter_range_limit(0, INODE_ID_RANGE_SPEC);
        pg.local
            .set_counter(counters::NEXT_INODE, limit)
            .expect("persist next inode");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, limit - 1)
            .expect("persist inode active start");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, limit)
            .expect("persist inode active end");
        pg.local
            .set_counter(LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, limit)
            .expect("persist inode reserved end");

        let result = pg.create_file(ROOT_INO, b"exhausted.txt", 0, 0, 0o644);
        assert!(
            matches!(result, Err(MetaError::Storage(ref message)) if message.contains("headroom exhausted")),
            "create_file must honor inode headroom exhaustion instead of bypassing the PG allocator: {result:?}"
        );
    }

    #[test]
    fn next_inode_reports_read_only_headroom_once_slot_limit_is_exhausted() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        PgMetaEngine::persist_client_slot(&pg.local, 0).expect("persist client slot");
        let limit = PgMetaEngine::counter_range_limit(0, INODE_ID_RANGE_SPEC);
        pg.local
            .set_counter(counters::NEXT_INODE, limit - 1)
            .expect("persist next inode");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, limit - 1)
            .expect("persist inode active start");
        pg.local
            .set_counter(LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, limit)
            .expect("persist inode active end");
        pg.local
            .set_counter(LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, limit)
            .expect("persist inode reserved end");
        let first = pg.next_inode().expect("final inode within local headroom");
        assert_eq!(first, limit - 1);

        let error = pg.next_inode().expect_err(
            "headroom should hard-stop new inode allocation once the slot is exhausted",
        );
        assert!(
            matches!(error, MetaError::Storage(ref message) if message.contains("headroom exhausted")),
            "unexpected exhaustion error: {error}"
        );

        let authority = MetaReplication::metadata_authority_status(&pg);
        assert!(
            !authority.allows_new_mutations(),
            "slot exhaustion should surface a read-only metadata authority status"
        );
    }

    #[test]
    fn persisted_pg_flushed_event_seq_roundtrips_in_local_counter() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        assert_eq!(
            PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
                .expect("initial flushed event seq"),
            0
        );
        pg.persist_flushed_event_seq(19)
            .expect("persist flushed event seq");
        assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 19);
        assert_eq!(
            PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
                .expect("reload flushed event seq"),
            19
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_file_lock_lease_blocks_peer_until_release() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-file-lock-blocks-peer");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open_with_options(
            &db_a,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open a");
        let b = PgMetaEngine::open_with_options(
            &db_b,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open b");
        let file = create_published_small_file(&a, ROOT_INO, b"locked.txt", 50_201);
        a.flush_metadata().expect("flush file create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"locked.txt",
            Duration::from_secs(10)
        ));

        let ttl = Duration::from_secs(2);
        let acquired = a
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("a acquire exclusive lease");
        assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

        let blocked = b
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("b conflicting acquire result");
        assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));

        let released = a
            .release_file_lock_lease(file.ino)
            .expect("a release lease");
        assert!(matches!(
            released,
            FileLockLeaseReleaseOutcome::Released(None)
        ));

        let acquired_after_release = b
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("b acquire after release");
        assert!(matches!(
            acquired_after_release,
            FileLockLeaseAcquireOutcome::Acquired(_)
        ));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_file_lock_first_acquire_serializes_concurrent_racers() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-file-lock-first-acquire-race");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open_with_options(
            &db_a,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open a");
        let b = PgMetaEngine::open_with_options(
            &db_b,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open b");
        let file = create_published_small_file(&a, ROOT_INO, b"first-acquire-race.txt", 50_202);
        a.flush_metadata().expect("flush file create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"first-acquire-race.txt",
            Duration::from_secs(10)
        ));

        let ttl = Duration::from_secs(2);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);
        let a2 = Arc::clone(&a);
        let b2 = Arc::clone(&b);

        let acquire_a = std::thread::spawn(move || {
            barrier_a.wait();
            a2.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        });
        let acquire_b = std::thread::spawn(move || {
            barrier_b.wait();
            b2.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
        });

        let outcome_a = acquire_a
            .join()
            .expect("join acquire a")
            .expect("acquire a");
        let outcome_b = acquire_b
            .join()
            .expect("join acquire b")
            .expect("acquire b");
        let acquired_count = [outcome_a, outcome_b]
            .into_iter()
            .filter(|outcome| matches!(outcome, FileLockLeaseAcquireOutcome::Acquired(_)))
            .count();
        assert_eq!(
            acquired_count, 1,
            "exactly one concurrent first-acquire attempt should win the lease row"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_open_recreates_missing_file_lock_lease_table() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-open-recreates-file-lock-lease-table");
        let info = volume.format_info();
        seed_local(&db, &info);
        drop_file_lock_lease_table(&dsn);

        let engine = PgMetaEngine::open_with_options(
            &db,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open pg engine after dropping file_lock_leases");
        let file = engine
            .create_file(ROOT_INO, b"recreated-lock-table.txt", 0, 0, 0o644)
            .expect("create file");

        let acquired = engine
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, Duration::from_secs(2))
            .expect("acquire lease after open-time schema ensure");
        assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_file_lock_lease_expires_for_peer() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-file-lock-expiry");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open_with_options(
            &db_a,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open a");
        let b = PgMetaEngine::open_with_options(
            &db_b,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open b");
        let file = create_published_small_file(&a, ROOT_INO, b"expiring.txt", 50_203);
        a.flush_metadata().expect("flush file create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"expiring.txt",
            Duration::from_secs(10)
        ));

        let ttl = Duration::from_millis(150);
        let acquired = a
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("a acquire lease");
        assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

        std::thread::sleep(Duration::from_millis(350));

        let peer_acquired = b
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("b acquire after expiry");
        assert!(matches!(
            peer_acquired,
            FileLockLeaseAcquireOutcome::Acquired(_)
        ));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_file_lock_lease_renew_keeps_peer_blocked() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-file-lock-renew");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open_with_options(
            &db_a,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open a");
        let b = PgMetaEngine::open_with_options(
            &db_b,
            PgOpenOptions {
                background_poll: false,
            },
        )
        .expect("open b");
        let file = create_published_small_file(&a, ROOT_INO, b"renew.txt", 50_204);
        a.flush_metadata().expect("flush file create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"renew.txt",
            Duration::from_secs(10)
        ));

        let ttl = Duration::from_secs(3);
        let acquired = a
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("a acquire lease");
        assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));

        std::thread::sleep(Duration::from_millis(500));

        let renewed = a
            .renew_file_lock_lease(file.ino, ttl)
            .expect("a renew lease");
        assert!(
            matches!(renewed, FileLockLeaseRenewOutcome::Renewed(_)),
            "expected renewed lease, got {renewed:?}"
        );

        std::thread::sleep(Duration::from_secs(1));

        let blocked = b
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("b conflicting acquire after renew");
        assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));

        std::thread::sleep(Duration::from_millis(3500));

        let acquired_after_renewed_expiry = b
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("b acquire after renewed expiry");
        assert!(matches!(
            acquired_after_renewed_expiry,
            FileLockLeaseAcquireOutcome::Acquired(_)
        ));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_file_lock_known_state_refreshes_from_notify() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-file-lock-notify-refresh");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");
        let file = create_published_small_file(&a, ROOT_INO, b"notify.txt", 50_205);
        a.flush_metadata().expect("flush file create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"notify.txt",
            Duration::from_secs(10)
        ));

        let ttl = Duration::from_secs(2);
        a.acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("a acquire lease");

        let blocked = b
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, ttl)
            .expect("b conflicting acquire result");
        assert!(matches!(blocked, FileLockLeaseAcquireOutcome::Blocked(_)));
        assert!(
            b.known_file_lock_lease(file.ino).is_some(),
            "blocked peer should cache the known conflicting lease"
        );

        a.release_file_lock_lease(file.ino)
            .expect("a release lease");

        assert!(
            wait_until(Duration::from_secs(10), || b
                .known_file_lock_lease(file.ino)
                .is_none()),
            "peer known lock cache should clear after NOTIFY-driven refresh"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_passive_lock_monitoring_starts_notify_listener_but_defers_renew_worker() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-lock-workers-lazy-start");
        let info = volume.format_info();
        seed_local(&db, &info);

        let engine = PgMetaEngine::open(&db).expect("open engine");
        assert!(
            engine
                .lock_notify_handle
                .lock()
                .expect("lock notify handle")
                .is_none(),
            "open alone should not eagerly start the pg lock notify listener"
        );
        assert!(
            engine
                .lock_renew_handle
                .lock()
                .expect("lock renew handle")
                .is_none(),
            "open should still defer the pg lock renew worker until a lock is held"
        );

        engine
            .ensure_known_file_lock_monitoring()
            .expect("start passive lock monitoring");
        assert!(
            engine
                .lock_notify_handle
                .lock()
                .expect("lock notify handle after passive monitoring")
                .is_some(),
            "passive lock monitoring should start the pg lock notify listener"
        );
        assert!(
            engine
                .lock_renew_handle
                .lock()
                .expect("lock renew handle after passive monitoring")
                .is_none(),
            "passive lock monitoring should not eagerly start the pg lock renew worker"
        );

        let file = engine
            .create_file(ROOT_INO, b"lazy-lock-workers.txt", 0, 0, 0o644)
            .expect("create file");
        let acquired = engine
            .acquire_file_lock_lease(file.ino, HardLockKind::Exclusive, Duration::from_secs(2))
            .expect("acquire lease");
        assert!(matches!(acquired, FileLockLeaseAcquireOutcome::Acquired(_)));
        assert!(
            engine
                .lock_notify_handle
                .lock()
                .expect("lock notify handle after acquire")
                .is_some(),
            "first lock usage should start the pg lock notify listener"
        );
        assert!(
            engine
                .lock_renew_handle
                .lock()
                .expect("lock renew handle after acquire")
                .is_some(),
            "first lock usage should start the pg lock renew worker"
        );
    }

    #[test]
    fn replication_backlog_reports_pending_and_inflight_ops() {
        let (_dir, pg) = temp_pg_engine_for_unit();

        {
            let mut state = pg.pending_state.lock().expect("pending_state");
            state.ops = 7;
        }
        pg.inflight_replication_ops.store(3, Ordering::SeqCst);

        let backlog = pg.replication_backlog();
        assert_eq!(backlog.pending_ops, 7);
        assert_eq!(backlog.inflight_ops, 3);
        assert_eq!(backlog.total_ops(), 10);
    }

    #[test]
    fn unit_remote_projection_preserves_nested_manifest_publish() {
        let (_writer_dir, writer) = temp_pg_engine_for_unit();
        let (_remote_dir, remote) = temp_pg_engine_for_unit();

        let start = writer.local.get_counter("nextEventSeq").expect("start seq");
        let dir = writer
            .create_dir(ROOT_INO, b"nested", 0, 0, 0o755)
            .expect("create dir");
        let file = writer
            .create_file(dir.ino, b"leaf.bin", 0, 0, 0o644)
            .expect("create file");
        set_file_state(&writer, file.ino, 4096, 1);
        let block = BlockRef::new_partial(701, 4096);
        writer
            .set_file_block(file.ino, 0, &block)
            .expect("set block");
        assert!(
            writer
                .publish_file_manifest(file.ino, 1)
                .expect("publish manifest"),
            "publish must succeed"
        );
        let end = writer.local.get_counter("nextEventSeq").expect("end seq");

        apply_projected_remote_range(&writer, &remote, start, end);

        let remote_dir = remote.lookup(ROOT_INO, b"nested").expect("lookup dir");
        let remote_file = remote
            .lookup(remote_dir.ino, b"leaf.bin")
            .expect("lookup file");
        assert_eq!(remote_file.ino, file.ino);
        let remote_attr = remote.get_attr(remote_file.ino).expect("get remote attr");
        assert_eq!(remote_attr.size, 4096);
        assert_eq!(remote_attr.committed_size, 4096);
        assert_eq!(remote_attr.committed_content_epoch, 1);
        assert_eq!(remote_attr.upload_state, UploadState::Ready);
        let remote_blocks = remote
            .get_file_blocks(remote_file.ino)
            .expect("remote blocks");
        assert_eq!(remote_blocks, vec![(0, block)]);
    }

    #[test]
    fn unit_remote_projection_updates_nested_file_after_temp_rename_then_publish() {
        let (_writer_dir, writer) = temp_pg_engine_for_unit();
        let (_remote_dir, remote) = temp_pg_engine_for_unit();
        let block_size = DEFAULT_BLOCK_SIZE as u64;

        let batch1_start = writer
            .local
            .get_counter("nextEventSeq")
            .expect("batch1 start");
        let dir = writer
            .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
            .expect("create dir");
        let file = writer
            .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
            .expect("create temp file");
        writer
            .rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
            .expect("rename temp to final");
        let batch1_end = writer
            .local
            .get_counter("nextEventSeq")
            .expect("batch1 end");

        apply_projected_remote_range(&writer, &remote, batch1_start, batch1_end);

        let remote_dir = remote
            .lookup(ROOT_INO, b"drafts")
            .expect("lookup dir after rename");
        assert!(matches!(
            remote.lookup(remote_dir.ino, b"final.bin"),
            Err(MetaError::EntryNotFound { .. })
        ));

        let batch2_start = batch1_end;
        set_file_state(&writer, file.ino, block_size + 4096, 1);
        let block0 = BlockRef::new_full(801, DEFAULT_BLOCK_SIZE);
        let block1 = BlockRef::new_partial(802, 4096);
        writer
            .set_file_block(file.ino, 0, &block0)
            .expect("set block 0");
        writer
            .set_file_block(file.ino, 1, &block1)
            .expect("set block 1");
        assert!(
            writer
                .publish_file_manifest(file.ino, 1)
                .expect("publish renamed manifest"),
            "publish after rename must succeed"
        );
        let batch2_end = writer
            .local
            .get_counter("nextEventSeq")
            .expect("batch2 end");

        apply_projected_remote_range(&writer, &remote, batch2_start, batch2_end);

        let remote_file_after = remote
            .lookup(remote_dir.ino, b"final.bin")
            .expect("lookup final after publish");
        assert_eq!(remote_file_after.ino, file.ino);
        let remote_attr_after = remote
            .get_attr(remote_file_after.ino)
            .expect("attr after publish");
        assert_eq!(remote_attr_after.size, block_size + 4096);
        assert_eq!(remote_attr_after.committed_size, block_size + 4096);
        assert_eq!(remote_attr_after.upload_state, UploadState::Ready);
        let remote_blocks = remote
            .get_file_blocks(remote_file_after.ino)
            .expect("remote blocks after publish");
        assert_eq!(remote_blocks, vec![(0, block0), (1, block1)]);
    }

    #[test]
    fn unit_remote_projection_converges_nested_rename_overwrite_to_source_inode() {
        let (_writer_dir, writer) = temp_pg_engine_for_unit();
        let (_remote_dir, remote) = temp_pg_engine_for_unit();
        let block_size = DEFAULT_BLOCK_SIZE as u64;

        let seed_start = writer
            .local
            .get_counter("nextEventSeq")
            .expect("seed start");
        let dir = writer
            .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
            .expect("create dir");
        let old = writer
            .create_file(dir.ino, b"final.bin", 0, 0, 0o644)
            .expect("create old final");
        set_file_state(&writer, old.ino, 4096, 1);
        let old_block = BlockRef::new_partial(901, 4096);
        writer
            .set_file_block(old.ino, 0, &old_block)
            .expect("set old block");
        assert!(
            writer
                .publish_file_manifest(old.ino, 1)
                .expect("publish old manifest"),
            "old manifest publish must succeed"
        );
        let seed_end = writer.local.get_counter("nextEventSeq").expect("seed end");

        apply_projected_remote_range(&writer, &remote, seed_start, seed_end);

        let remote_dir = remote.lookup(ROOT_INO, b"shared").expect("lookup dir");
        let remote_old = remote
            .lookup(remote_dir.ino, b"final.bin")
            .expect("lookup seeded final");
        assert_eq!(remote_old.ino, old.ino);

        let replace_start = seed_end;
        let temp = writer
            .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
            .expect("create replacement temp");
        set_file_state(&writer, temp.ino, block_size + 4096, 2);
        let new_block0 = BlockRef::new_full(902, DEFAULT_BLOCK_SIZE);
        let new_block1 = BlockRef::new_partial(903, 4096);
        writer
            .set_file_block(temp.ino, 0, &new_block0)
            .expect("set replacement block 0");
        writer
            .set_file_block(temp.ino, 1, &new_block1)
            .expect("set replacement block 1");
        assert!(
            writer
                .publish_file_manifest(temp.ino, 2)
                .expect("publish replacement temp"),
            "replacement temp publish must succeed"
        );
        writer
            .rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
            .expect("rename replacement over final");
        let replace_end = writer
            .local
            .get_counter("nextEventSeq")
            .expect("replace end");

        apply_projected_remote_range(&writer, &remote, replace_start, replace_end);

        let remote_final = remote
            .lookup(remote_dir.ino, b"final.bin")
            .expect("lookup replaced final");
        assert_eq!(remote_final.ino, temp.ino);
        assert!(
            remote.get_attr(old.ino).is_err(),
            "old inode must be removed"
        );
        let remote_attr = remote
            .get_attr(remote_final.ino)
            .expect("get replaced attr");
        assert_eq!(remote_attr.size, block_size + 4096);
        assert_eq!(remote_attr.committed_size, block_size + 4096);
        assert_eq!(remote_attr.committed_content_epoch, 2);
        assert_eq!(remote_attr.upload_state, UploadState::Ready);
        let remote_blocks = remote
            .get_file_blocks(remote_final.ino)
            .expect("replacement blocks");
        assert_eq!(remote_blocks, vec![(0, new_block0), (1, new_block1)]);
    }

    #[test]
    fn unit_remote_projection_keeps_published_file_visible_when_unpublished_epoch_is_deleted() {
        let (_writer_dir, writer) = temp_pg_engine_for_unit();
        let (_remote_dir, remote) = temp_pg_engine_for_unit();

        let seed_start = writer
            .local
            .get_counter("nextEventSeq")
            .expect("seed start");
        let dir = writer
            .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
            .expect("create dir");
        let file = writer
            .create_file(dir.ino, b"final.bin", 0, 0, 0o644)
            .expect("create final");
        set_file_state(&writer, file.ino, 4096, 1);
        let published = BlockRef::new_partial(904, 4096);
        writer
            .set_file_block(file.ino, 0, &published)
            .expect("set published block");
        assert!(
            writer
                .publish_file_manifest(file.ino, 1)
                .expect("publish seed manifest"),
            "seed publish must succeed"
        );
        let seed_end = writer.local.get_counter("nextEventSeq").expect("seed end");

        apply_projected_remote_range(&writer, &remote, seed_start, seed_end);

        let remote_dir = remote.lookup(ROOT_INO, b"shared").expect("lookup dir");
        let remote_file = remote
            .lookup(remote_dir.ino, b"final.bin")
            .expect("lookup published file");
        assert_eq!(remote_file.ino, file.ino);

        let upload_start = seed_end;
        set_file_state(&writer, file.ino, 4096, 2);
        let uploaded_overlay = BlockRef::new_partial(905, 4096);
        writer
            .set_file_block(file.ino, 0, &uploaded_overlay)
            .expect("set unpublished overlay");
        assert!(
            writer
                .mark_file_block_uploaded_if_matches(file.ino, 0, 2, &uploaded_overlay)
                .expect("mark overlay uploaded"),
            "overlay upload marker must succeed"
        );
        let upload_end = writer
            .local
            .get_counter("nextEventSeq")
            .expect("upload end");

        apply_projected_remote_range(&writer, &remote, upload_start, upload_end);

        writer
            .unlink(dir.ino, b"final.bin")
            .expect("unlink unpublished replacement");
        let delete_end = writer
            .local
            .get_counter("nextEventSeq")
            .expect("delete end");

        apply_projected_remote_range(&writer, &remote, upload_end, delete_end);

        let remote_after = remote
            .lookup(remote_dir.ino, b"final.bin")
            .expect("remote must retain last published entry");
        assert_eq!(remote_after.ino, file.ino);
        let remote_attr = remote.get_attr(file.ino).expect("remote attr");
        assert_eq!(remote_attr.size, 4096);
        assert_eq!(remote_attr.committed_size, 4096);
        assert_eq!(remote_attr.content_epoch, 1);
        assert_eq!(remote_attr.committed_content_epoch, 1);
        let remote_blocks = remote.get_file_blocks(file.ino).expect("remote blocks");
        assert_eq!(remote_blocks, vec![(0, published)]);

        assert!(
            writer.get_attr(file.ino).is_err(),
            "local unlink removes inode attr"
        );
        let lifecycle = writer
            .get_inode_lifecycle(file.ino)
            .expect("local lifecycle")
            .expect("deleted-before-publish lifecycle");
        assert_eq!(lifecycle.state, InodeLifecycleState::DeletedBeforePublish);
        assert!(lifecycle.has_published_base);
        assert_eq!(lifecycle.deferred_cleanup_object_ids, vec![905]);
        let pending_deletions = writer.get_pending_deletions().expect("pending deletions");
        assert!(
            pending_deletions
                .iter()
                .any(|(object_id, _timestamp)| *object_id == 905),
            "uploaded unpublished object should be queued for GC"
        );
        assert!(
            pending_deletions
                .iter()
                .all(|(object_id, _timestamp)| *object_id != 904),
            "last published object must remain referenced"
        );
    }

    #[test]
    fn unit_remote_projection_preserves_overlay_publish_path() {
        let (_writer_dir, writer) = temp_pg_engine_for_unit();
        let (_remote_dir, remote) = temp_pg_engine_for_unit();

        let start = writer.local.get_counter("nextEventSeq").expect("start seq");
        let file = writer
            .create_file(ROOT_INO, b"overlay.bin", 0, 0, 0o644)
            .expect("create overlay file");
        set_file_state(&writer, file.ino, 4096, 1);
        let block = BlockRef::new_partial(906, 4096);
        writer
            .set_file_block(file.ino, 0, &block)
            .expect("set overlay block");
        assert!(
            writer
                .mark_file_block_uploaded_if_matches(file.ino, 0, 1, &block)
                .expect("mark overlay uploaded"),
            "uploaded marker must succeed"
        );
        assert!(
            writer
                .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
                .expect("publish overlay to base"),
            "overlay publish must succeed"
        );
        let end = writer.local.get_counter("nextEventSeq").expect("end seq");

        apply_projected_remote_range(&writer, &remote, start, end);

        let remote_file = remote
            .lookup(ROOT_INO, b"overlay.bin")
            .expect("lookup overlay file");
        let remote_attr = remote.get_attr(remote_file.ino).expect("remote attr");
        assert_eq!(remote_attr.size, 4096);
        assert_eq!(remote_attr.committed_size, 4096);
        assert_eq!(remote_attr.committed_content_epoch, 1);
        assert_eq!(remote_attr.upload_state, UploadState::Ready);
        let remote_blocks = remote
            .get_file_blocks(remote_file.ino)
            .expect("remote overlay blocks");
        assert_eq!(remote_blocks, vec![(0, block)]);
    }

    #[test]
    fn claim_and_restore_pending_batch_updates_replication_backlog() {
        let (_dir, pg) = temp_pg_engine_for_unit();
        {
            let mut state = pg.pending_state.lock().expect("pending_state");
            state.ops = 4;
            state.force_flush = true;
            state.event_start = Some(10);
            state.event_end = 14;
            state.started_at = Some(Instant::now());
            state.last_mutation_at = Some(Instant::now());
        }

        let batch = pg.claim_pending_batch(true).expect("claimed batch");
        let claimed = pg.replication_backlog();
        assert_eq!(claimed.pending_ops, 0);
        assert_eq!(claimed.inflight_ops, 4);

        pg.restore_pending_batch(batch);
        let restored = pg.replication_backlog();
        assert_eq!(restored.pending_ops, 4);
        assert_eq!(restored.inflight_ops, 0);
    }

    #[test]
    fn restore_pending_state_from_local_log_rebuilds_unpublished_range() {
        let (_dir, pg) = temp_pg_engine_for_unit();
        let start = pg.local.get_counter("nextEventSeq").expect("baseline seq");

        pg.local
            .create_dir(ROOT_INO, b"draft", 0, 0, 0o755)
            .expect("create draft dir");
        pg.local
            .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
            .expect("create draft file");

        {
            let mut state = pg.pending_state.lock().expect("pending_state");
            *state = PendingReplicationState::default();
        }

        pg.restore_pending_state_from_local_log()
            .expect("restore pending state");

        let end = pg.local.get_counter("nextEventSeq").expect("end seq");
        let backlog = pg.replication_backlog();
        assert_eq!(backlog.pending_ops, (end - start) as u32);
        let state = pg.pending_state.lock().expect("pending_state");
        assert!(state.force_flush);
        assert_eq!(state.event_start, Some(start));
        assert_eq!(state.event_end, end);
    }

    #[test]
    fn pg_event_insert_chunking_covers_all_events() {
        let events = vec![MetaEvent::RemoveAttr { ino: 1 }; 1_025];
        let chunk_sizes: Vec<usize> = events
            .chunks(PG_EVENT_INSERT_CHUNK_SIZE)
            .map(|chunk| chunk.len())
            .collect();

        assert_eq!(chunk_sizes, vec![512, 512, 1]);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_flush_advances_checkpoint_version() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-test");
        seed_local(&db, &volume.format_info());

        let pg = PgMetaEngine::open(&db).expect("open pg");
        pg.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644)
            .expect("create file");
        pg.flush_metadata().expect("flush metadata");
        let ok = wait_until(Duration::from_secs(10), || {
            run_async(async {
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let conn_task = tokio::spawn(async move {
                    let _ = connection.await;
                });
                let row = client
                    .query_opt(
                        "SELECT version FROM metadata_state WHERE vol=$1",
                        &[&volume.volume_id],
                    )
                    .await
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                conn_task.abort();
                Ok::<bool, MetaError>(row.map(|r| r.get::<_, i64>(0) >= 1).unwrap_or(false))
            })
            .unwrap_or(false)
        });
        assert!(ok, "expected metadata_state version >= 1");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_clients_reserve_disjoint_local_id_ranges() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-client-slots");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let a_slot = PgMetaEngine::load_persisted_client_slot(&a.local)
            .expect("read a slot")
            .expect("a slot");
        let b_slot = PgMetaEngine::load_persisted_client_slot(&b.local)
            .expect("read b slot")
            .expect("b slot");
        assert_ne!(
            a_slot, b_slot,
            "distinct clients must reserve distinct slots"
        );

        let a_next_inode = a
            .local
            .get_counter(counters::NEXT_INODE)
            .expect("a next inode");
        let b_next_inode = b
            .local
            .get_counter(counters::NEXT_INODE)
            .expect("b next inode");
        let a_next_block = a
            .local
            .get_counter(counters::NEXT_BLOCK_ID)
            .expect("a next block");
        let b_next_block = b
            .local
            .get_counter(counters::NEXT_BLOCK_ID)
            .expect("b next block");

        assert_ne!(a_next_inode, b_next_inode);
        assert_ne!(a_next_block, b_next_block);

        let a_file = a
            .create_file(ROOT_INO, b"a.txt", 0, 0, 0o644)
            .expect("a create file");
        let b_file = b
            .create_file(ROOT_INO, b"b.txt", 0, 0, 0o644)
            .expect("b create file");
        assert_ne!(
            a_file.ino, b_file.ino,
            "new inode allocations must not collide"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_sync_metadata_between_clients() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-sync");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        a.create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
            .expect("create dir on a");
        a.flush_metadata().expect("flush a");
        assert!(
            wait_visible(&b, ROOT_INO, b"shared", Duration::from_secs(10)),
            "shared dir should become visible on b"
        );
        let entry = b.lookup(ROOT_INO, b"shared").expect("lookup on b");
        assert!(entry.ino > 0);
        assert_eq!(entry.kind, FileType::Directory);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-committed-block-read");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = create_published_small_file(&a, ROOT_INO, b"remote.bin", 50_301);
        set_file_state(&a, file.ino, 4096, 2);
        let block = BlockRef::new_partial(77, 4096);
        a.set_file_block(file.ino, 0, &block)
            .expect("set staged block");
        assert!(
            a.publish_file_manifest(file.ino, 2)
                .expect("publish committed manifest")
        );
        a.flush_metadata().expect("flush a");

        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"remote.bin",
            Duration::from_secs(10)
        ));
        let remote = b.lookup(ROOT_INO, b"remote.bin").expect("lookup remote");
        let remote_block = b
            .get_file_block(remote.ino, 0)
            .expect("get remote block")
            .expect("remote committed block");
        assert_eq!(remote_block.object_id, 77);

        let remote_blocks = b.get_file_blocks(remote.ino).expect("get remote blocks");
        assert_eq!(remote_blocks.len(), 1);
        assert_eq!(remote_blocks[0].0, 0);
        assert_eq!(remote_blocks[0].1.object_id, 77);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_remote_uploading_marker_replication() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-remote-uploading-marker");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"uploading.bin", 0, 0, 0o644)
            .expect("create file");
        set_file_state(&a, file.ino, 4096, 1);
        a.set_file_block(file.ino, 0, &BlockRef::new_partial(501, 4096))
            .expect("set staged block");
        a.flush_metadata().expect("flush uploading marker");

        assert!(wait_until(Duration::from_secs(10), || {
            let _ = b.sync_metadata();
            matches!(
                b.lookup(ROOT_INO, b"uploading.bin"),
                Err(MetaError::EntryNotFound { .. })
            )
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_nested_manifest_publish_visible_on_remote_peer() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-nested-manifest-publish");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let dir = a
            .create_dir(ROOT_INO, b"nested", 0, 0, 0o755)
            .expect("create nested dir");
        a.flush_metadata().expect("flush nested dir");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"nested",
            Duration::from_secs(10)
        ));

        let file = a
            .create_file(dir.ino, b"leaf.bin", 0, 0, 0o644)
            .expect("create nested file");
        set_file_state(&a, file.ino, 4096, 1);
        let block = BlockRef::new_partial(511, 4096);
        a.set_file_block(file.ino, 0, &block)
            .expect("set nested block");
        assert!(
            a.publish_file_manifest(file.ino, 1)
                .expect("publish nested manifest")
        );
        a.flush_metadata().expect("flush nested publish");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = b.sync_metadata();
            match b.lookup(ROOT_INO, b"nested") {
                Ok(remote_dir) => match b.lookup(remote_dir.ino, b"leaf.bin") {
                    Ok(remote_file) => b
                        .get_attr(remote_file.ino)
                        .map(|attr| {
                            attr.size == 4096
                                && attr.committed_size == 4096
                                && attr.upload_state == UploadState::Ready
                        })
                        .unwrap_or(false),
                    Err(_) => false,
                },
                Err(_) => false,
            }
        }));

        let remote_dir = b.lookup(ROOT_INO, b"nested").expect("lookup remote dir");
        let remote_file = b
            .lookup(remote_dir.ino, b"leaf.bin")
            .expect("lookup remote nested file");
        let remote_blocks = b
            .get_file_blocks(remote_file.ino)
            .expect("get remote nested blocks");
        assert_eq!(remote_blocks, vec![(0, block)]);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_nested_temp_rename_then_publish_visible_on_remote_peer() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-nested-temp-rename-publish");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let dir = a
            .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
            .expect("create drafts dir");
        a.flush_metadata().expect("flush drafts dir");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"drafts",
            Duration::from_secs(10)
        ));

        let file = a
            .create_file(dir.ino, b"temp.bin", 0, 0, 0o644)
            .expect("create temp file");
        a.rename(dir.ino, b"temp.bin", dir.ino, b"final.bin")
            .expect("rename temp to final");
        a.flush_metadata().expect("flush rename");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = b.sync_metadata();
            match b.lookup(ROOT_INO, b"drafts") {
                Ok(remote_dir) => matches!(
                    b.lookup(remote_dir.ino, b"final.bin"),
                    Err(MetaError::EntryNotFound { .. })
                ),
                Err(_) => false,
            }
        }));

        let block_size = DEFAULT_BLOCK_SIZE as u64;
        set_file_state(&a, file.ino, block_size + 4096, 1);
        let block0 = BlockRef::new_full(611, DEFAULT_BLOCK_SIZE);
        let block1 = BlockRef::new_partial(612, 4096);
        a.set_file_block(file.ino, 0, &block0).expect("set block 0");
        a.set_file_block(file.ino, 1, &block1).expect("set block 1");
        assert!(
            a.publish_file_manifest(file.ino, 1)
                .expect("publish renamed nested manifest")
        );
        a.flush_metadata().expect("flush renamed nested manifest");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = b.sync_metadata();
            match b.lookup(ROOT_INO, b"drafts") {
                Ok(remote_dir) => match b.lookup(remote_dir.ino, b"final.bin") {
                    Ok(remote_file) => {
                        let attr_ready = b
                            .get_attr(remote_file.ino)
                            .map(|attr| {
                                attr.size == block_size + 4096
                                    && attr.committed_size == block_size + 4096
                                    && attr.upload_state == UploadState::Ready
                            })
                            .unwrap_or(false);
                        let blocks_ready = b
                            .get_file_blocks(remote_file.ino)
                            .map(|blocks| blocks == vec![(0, block0.clone()), (1, block1.clone())])
                            .unwrap_or(false);
                        attr_ready && blocks_ready
                    }
                    Err(_) => false,
                },
                Err(_) => false,
            }
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_remote_update_keeps_old_committed_version_until_switch() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-old-committed-until-switch");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"committed.bin", 0, 0, 0o644)
            .expect("create file");
        set_file_state(&a, file.ino, 4096, 1);
        let baseline = BlockRef::new_partial(701, 4096);
        a.set_file_block(file.ino, 0, &baseline)
            .expect("set baseline block");
        assert!(
            a.publish_file_manifest(file.ino, 1)
                .expect("publish baseline manifest")
        );
        a.flush_metadata().expect("flush baseline");

        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"committed.bin",
            Duration::from_secs(10)
        ));
        let remote = b.lookup(ROOT_INO, b"committed.bin").expect("lookup remote");
        assert_eq!(
            b.get_file_block(remote.ino, 0)
                .expect("get baseline remote block")
                .expect("baseline remote block")
                .object_id,
            701
        );

        set_file_state(&a, file.ino, 4096, 2);
        let replacement = BlockRef::new_partial(702, 4096);
        a.set_file_block(file.ino, 0, &replacement)
            .expect("set replacement block");
        a.flush_metadata().expect("flush updating marker");

        assert!(wait_until(Duration::from_secs(10), || {
            let _ = b.sync_metadata();
            b.get_attr(remote.ino)
                .map(|attr| attr.upload_state == UploadState::Updating)
                .unwrap_or(false)
        }));

        let remote_attr = b
            .get_attr(remote.ino)
            .expect("get remote attr after update");
        assert_eq!(remote_attr.upload_state, UploadState::Updating);
        let remote_block = b
            .get_file_block(remote.ino, 0)
            .expect("get remote block after update")
            .expect("remote block after update");
        assert_eq!(
            remote_block.object_id, 701,
            "remote peer must keep serving the previous committed version until publish"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_publish_switch_is_atomic_across_peers() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-atomic-publish-switch");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"atomic.bin", 0, 0, 0o644)
            .expect("create file");
        let block_size = DEFAULT_BLOCK_SIZE as u64;
        set_file_state(&a, file.ino, block_size + 4096, 1);
        let baseline0 = BlockRef::new_full(801, DEFAULT_BLOCK_SIZE);
        let baseline1 = BlockRef::new_partial(802, 4096);
        a.set_file_block(file.ino, 0, &baseline0)
            .expect("set baseline block 0");
        a.set_file_block(file.ino, 1, &baseline1)
            .expect("set baseline block 1");
        assert!(
            a.publish_file_manifest(file.ino, 1)
                .expect("publish baseline manifest")
        );
        a.flush_metadata().expect("flush baseline manifest");

        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"atomic.bin",
            Duration::from_secs(10)
        ));
        let remote = b.lookup(ROOT_INO, b"atomic.bin").expect("lookup remote");

        set_file_state(&a, file.ino, block_size + 4096, 2);
        let replacement0 = BlockRef::new_full(901, DEFAULT_BLOCK_SIZE);
        let replacement1 = BlockRef::new_partial(902, 4096);
        a.set_file_block(file.ino, 0, &replacement0)
            .expect("set replacement block 0");
        a.set_file_block(file.ino, 1, &replacement1)
            .expect("set replacement block 1");
        a.flush_metadata().expect("flush updating state");

        assert!(wait_until(Duration::from_secs(10), || {
            let _ = b.sync_metadata();
            let blocks = b.get_file_blocks(remote.ino).unwrap_or_default();
            blocks.len() == 2
                && blocks[0].1.object_id == 801
                && blocks[1].1.object_id == 802
                && b.get_attr(remote.ino)
                    .map(|attr| attr.upload_state == UploadState::Updating)
                    .unwrap_or(false)
        }));

        assert!(
            a.publish_file_manifest(file.ino, 2)
                .expect("publish replacement manifest")
        );
        a.flush_metadata().expect("flush replacement manifest");

        assert!(wait_until(Duration::from_secs(10), || {
            let _ = b.sync_metadata();
            let blocks = b.get_file_blocks(remote.ino).unwrap_or_default();
            blocks.len() == 2
                && blocks[0].1.object_id == 901
                && blocks[1].1.object_id == 902
                && b.get_attr(remote.ino)
                    .map(|attr| attr.upload_state == UploadState::Ready)
                    .unwrap_or(false)
        }));

        let remote_blocks = b.get_file_blocks(remote.ino).expect("get remote blocks");
        assert_eq!(remote_blocks.len(), 2);
        assert_eq!(remote_blocks[0].1.object_id, 901);
        assert_eq!(remote_blocks[1].1.object_id, 902);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_foreign_events_do_not_block_local_accepted_reads() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let dir_c = tempdir().expect("tempdir c");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let db_c = dir_c.path().join("meta-c.redb");
        let volume = PgTestVolume::new(&dsn, "pg-foreign-does-not-flush-local");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);
        seed_local(&db_c, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");
        let c = PgMetaEngine::open(&db_c).expect("open c");

        let draft = b
            .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
            .expect("create local draft");
        set_file_state(&b, draft.ino, 4096, 1);
        b.set_file_block(draft.ino, 0, &BlockRef::new_partial(1001, 4096))
            .expect("set local draft block");

        a.create_dir(ROOT_INO, b"foreign", 0, 0, 0o755)
            .expect("create foreign dir");
        a.flush_metadata().expect("flush foreign dir");

        assert!(wait_until(Duration::from_secs(10), || {
            if let Ok(mut state) = b.pending_state.lock() {
                state.force_flush = false;
                state.last_mutation_at = Some(Instant::now());
            }
            let _ = b.sync_metadata();
            b.lookup(ROOT_INO, b"foreign").is_ok()
        }));

        let local_attr = b.get_attr(draft.ino).expect("get local draft attr");
        assert_eq!(local_attr.size, 4096);
        assert_eq!(local_attr.committed_size, 0);
        assert_eq!(local_attr.upload_state, UploadState::Uploading);
        let local_block = b
            .get_file_block(draft.ino, 0)
            .expect("get local draft block")
            .expect("local draft block");
        assert_eq!(local_block.object_id, 1001);

        if let Ok(mut state) = b.pending_state.lock() {
            state.force_flush = false;
            state.last_mutation_at = Some(Instant::now());
        }
        let _ = c.sync_metadata();
        assert!(
            c.lookup(ROOT_INO, b"draft.bin").is_err(),
            "syncing foreign events on b must not flush its local accepted draft into PG"
        );
        assert!(
            c.lookup(ROOT_INO, b"foreign").is_ok(),
            "foreign namespace convergence should still work while b has local-only accepted state"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_overlay_allows_unrelated_remote_batches_to_apply() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-overlay-prefix");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let draft = a
            .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
            .expect("create draft");
        let mut draft_attr = a.get_attr(draft.ino).expect("get draft attr");
        draft_attr.size = 4096;
        a.set_attr(&draft_attr).expect("set draft size");
        a.set_file_block(draft.ino, 0, &BlockRef::new_partial(501, 4096))
            .expect("set draft staged block");
        a.flush_metadata().expect("flush draft namespace");

        b.create_dir(ROOT_INO, b"foreign.txt", 0, 0, 0o755)
            .expect("create foreign");
        b.flush_metadata().expect("flush foreign");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.lookup(ROOT_INO, b"foreign.txt").is_ok()
        }));

        let draft_after = a.get_attr(draft.ino).expect("get local draft");
        assert_eq!(draft_after.size, 4096);
        assert_eq!(draft_after.committed_size, 0);
        let local_block = a
            .get_file_block(draft.ino, 0)
            .expect("get staged draft block")
            .expect("staged draft block exists");
        assert_eq!(local_block.object_id, 501);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_committed_block_applies_despite_local_attr_overlay() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-committed-block-vs-attr-overlay");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"remote.bin", 0, 0, 0o644)
            .expect("create file");
        set_file_state(&a, file.ino, 4096, 1);
        a.set_file_block(file.ino, 0, &BlockRef::new_partial(87, 4096))
            .expect("set baseline remote block");
        assert!(
            a.publish_file_manifest(file.ino, 1)
                .expect("publish baseline manifest")
        );
        a.flush_metadata().expect("flush create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"remote.bin",
            Duration::from_secs(10)
        ));

        let b_file = b.lookup(ROOT_INO, b"remote.bin").expect("lookup on b");
        let mut local_attr = b.get_attr(b_file.ino).expect("get local attr");
        local_attr.size = 8192;
        b.set_attr(&local_attr).expect("set local attr overlay");

        set_file_state(&a, file.ino, 4096, 2);
        let block = BlockRef::new_partial(88, 4096);
        a.set_file_block(file.ino, 0, &block)
            .expect("set staged remote block");
        assert!(
            a.publish_file_manifest(file.ino, 2)
                .expect("publish remote block manifest")
        );
        a.flush_metadata().expect("flush remote block");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = b.sync_metadata();
            b.get_file_block(b_file.ino, 0)
                .map(|block| block.map(|value| value.object_id == 88).unwrap_or(false))
                .unwrap_or(false)
        }));

        let committed = b
            .get_file_block(b_file.ino, 0)
            .expect("get committed block on b")
            .expect("committed block visible on b");
        assert_eq!(committed.object_id, 88);

        let attr_after = b.get_attr(b_file.ino).expect("get local attr after sync");
        assert_eq!(
            attr_after.size, 8192,
            "local attr overlay should still mask remote size"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_conflicting_batch_does_not_block_later_foreign_batch() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-conflict-gap");
        let info = volume.format_info();
        seed_local(&db, &info);

        let pg = PgMetaEngine::open(&db).expect("open pg");
        let draft = pg
            .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
            .expect("create draft");
        let mut draft_attr = pg.get_attr(draft.ino).expect("get draft attr");
        draft_attr.size = 4096;
        pg.set_attr(&draft_attr).expect("set staged size");
        pg.set_file_block(draft.ino, 0, &BlockRef::new_partial(601, 4096))
            .expect("set draft block");

        let mut conflicted_attr = draft_attr.clone();
        conflicted_attr.size = 8192;

        let remote_ino = 1_234_567_890u64;
        let remote_attr = InodeAttr::new_dir(remote_ino, ROOT_INO, 0, 0, 0o755);

        let conflict_event = MetaEvent::SetAttr {
            attr: conflicted_attr,
        };
        let add_event = MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        };
        let dentry_event = MetaEvent::AddEntry {
            parent: ROOT_INO,
            name: b"unrelated.txt".to_vec(),
            entry: DirEntryValue {
                ino: remote_ino,
                kind: FileType::Directory,
            },
        };

        let conflict_payload = postcard::to_stdvec(&conflict_event).expect("encode conflict event");
        let add_payload = postcard::to_stdvec(&add_event).expect("encode add attr event");
        let dentry_payload =
            postcard::to_stdvec(&dentry_event).expect("encode unrelated dentry event");

        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });

            client
                .execute(
                    "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 1, $2, $3, 'manual-foreign')",
                    &[&volume.volume_id, &conflict_event.kind(), &conflict_payload],
                )
                .await
                .map_err(|e| MetaError::Storage(format!("insert conflict event failed: {e}")))?;

            client
                .execute(
                    "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 2, $2, $3, 'manual-foreign')",
                    &[&volume.volume_id, &add_event.kind(), &add_payload],
                )
                .await
                .map_err(|e| MetaError::Storage(format!("insert remote attr event failed: {e}")))?;
            client
                .execute(
                    "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
                    VALUES ($1, 2, $2, $3, 'manual-foreign')",
                    &[&volume.volume_id, &dentry_event.kind(), &dentry_payload],
                )
                .await
                .map_err(|e| MetaError::Storage(format!("insert remote dentry event failed: {e}")))?;
            conn_task.abort();
            Ok(())
        })
        .expect("write synthetic remote batches");

        pg.sync_metadata().expect("sync remote batches");
        let remote = pg
            .lookup(ROOT_INO, b"unrelated.txt")
            .expect("lookup unrelated entry");
        assert_eq!(remote.ino, remote_ino);

        let local = pg
            .get_attr(draft.ino)
            .expect("lookup local draft attr after sync");
        assert_eq!(
            local.size, 4096,
            "conflicting foreign batch should not clobber local overlay"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_checkpoint_excludes_uncommitted_local_blocks() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-checkpoint-projection");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let file = a
            .create_file(ROOT_INO, b"draft.bin", 0, 0, 0o644)
            .expect("create draft");

        let mut attr = a.get_attr(file.ino).expect("get attr");
        attr.size = 4096;
        attr.mtime = InodeAttr::now_nanos();
        attr.ctime = attr.mtime;
        a.set_attr(&attr).expect("set staged size");
        a.set_file_block(file.ino, 0, &BlockRef::new_partial(42, 4096))
            .expect("set staged block");
        a.flush_metadata().expect("flush a");

        let b = PgMetaEngine::open(&db_b).expect("open b");
        assert!(
            wait_hidden(&b, ROOT_INO, b"draft.bin", Duration::from_secs(10)),
            "checkpoint must keep a brand-new unpublished file hidden from peers"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_remote_unlink_removes_file_from_namespace_immediately() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-remote-unlink");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        create_published_small_file(&a, ROOT_INO, b"remote-delete.txt", 50_302);
        a.flush_metadata().expect("flush create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"remote-delete.txt",
            Duration::from_secs(10)
        ));

        a.unlink(ROOT_INO, b"remote-delete.txt")
            .expect("unlink on a");
        a.flush_metadata().expect("flush unlink");

        assert!(wait_until(Duration::from_secs(10), || {
            let _ = b.sync_metadata();
            b.lookup(ROOT_INO, b"remote-delete.txt").is_err()
        }));
        assert!(b.lookup(ROOT_INO, b"remote-delete.txt").is_err());
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_peer_can_unlink_foreign_visible_uploading_entry() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-peer-unlink-uploading");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"uploading.bin", 0, 0, 0o644)
            .expect("create uploading file");
        set_file_state(&a, file.ino, 4096, 1);
        a.set_file_block(file.ino, 0, &BlockRef::new_partial(776, 4096))
            .expect("set baseline uploading block");
        assert!(
            a.publish_file_manifest(file.ino, 1)
                .expect("publish baseline manifest")
        );
        a.flush_metadata().expect("flush baseline");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"uploading.bin",
            Duration::from_secs(10)
        ));

        set_file_state(&a, file.ino, 4096, 2);
        a.set_file_block(file.ino, 0, &BlockRef::new_partial(777, 4096))
            .expect("set local uploading block");
        a.flush_metadata().expect("flush uploading namespace");

        assert!(wait_until(Duration::from_secs(10), || {
            let _ = b.sync_metadata();
            b.lookup(ROOT_INO, b"uploading.bin")
                .map(|entry| {
                    b.get_attr(entry.ino)
                        .map(|attr| attr.upload_state == UploadState::Updating)
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        }));

        b.unlink(ROOT_INO, b"uploading.bin")
            .expect("unlink uploading file on peer");
        b.flush_metadata().expect("flush peer unlink");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            let _ = b.sync_metadata();
            a.lookup(ROOT_INO, b"uploading.bin").is_ok()
                && b.lookup(ROOT_INO, b"uploading.bin").is_err()
        }));
        let local_attr = a
            .get_attr(file.ino)
            .expect("get local attr after peer unlink");
        assert_eq!(local_attr.upload_state, UploadState::Ready);
        let local_block = a
            .get_file_block(file.ino, 0)
            .expect("get local block after peer unlink")
            .expect("writer keeps local pending block after peer unlink");
        assert_eq!(local_block.object_id, 777);

        drop(a);
        let reopened = reopen_pg_engine_with_retry(&db_a);
        reopened.sync_metadata().expect("sync reopened a");
        assert!(reopened.lookup(ROOT_INO, b"uploading.bin").is_ok());
        assert_eq!(
            reopened
                .get_file_block(file.ino, 0)
                .expect("get reopened local block")
                .expect("reopened local block")
                .object_id,
            777
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_remote_rename_overwrite_converges_to_source_inode() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-rename-overwrite");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let src = create_published_small_file(&a, ROOT_INO, b"src.txt", 50_304);
        let dst = create_published_small_file(&a, ROOT_INO, b"dst.txt", 50_305);
        a.flush_metadata().expect("flush seed files");

        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"src.txt",
            Duration::from_secs(10)
        ));
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"dst.txt",
            Duration::from_secs(10)
        ));

        a.rename(ROOT_INO, b"src.txt", ROOT_INO, b"dst.txt")
            .expect("rename overwrite on a");
        a.flush_metadata().expect("flush rename overwrite");

        assert!(wait_until(Duration::from_secs(10), || {
            let _ = b.sync_metadata();
            b.lookup(ROOT_INO, b"src.txt").is_err()
                && b.lookup(ROOT_INO, b"dst.txt")
                    .map(|entry| entry.ino == src.ino)
                    .unwrap_or(false)
        }));

        let on_b = b.lookup(ROOT_INO, b"dst.txt").expect("lookup dst on b");
        assert_eq!(on_b.ino, src.ino);
        assert_ne!(on_b.ino, dst.ino);
        assert!(b.lookup(ROOT_INO, b"src.txt").is_err());
        assert!(
            b.get_attr(dst.ino).is_err(),
            "overwritten inode should be removed"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_peer_can_rename_foreign_visible_base_entry() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-peer-rename-base");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_306);
        a.flush_metadata().expect("flush create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"remote.txt",
            Duration::from_secs(10)
        ));

        b.rename(ROOT_INO, b"remote.txt", ROOT_INO, b"renamed-by-b.txt")
            .expect("rename on b");
        b.flush_metadata().expect("flush rename on b");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.lookup(ROOT_INO, b"remote.txt").is_err()
                && a.lookup(ROOT_INO, b"renamed-by-b.txt")
                    .map(|entry| entry.ino == file.ino)
                    .unwrap_or(false)
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_peer_can_unlink_foreign_visible_base_entry() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-peer-unlink-base");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_307);
        a.flush_metadata().expect("flush create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"remote.txt",
            Duration::from_secs(10)
        ));

        b.unlink(ROOT_INO, b"remote.txt").expect("unlink on b");
        b.flush_metadata().expect("flush unlink on b");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.lookup(ROOT_INO, b"remote.txt").is_err()
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_peer_unlink_base_recovers_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-reopen-peer-unlink-base");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = create_published_small_file(&a, ROOT_INO, b"remote.txt", 50_308);
        a.flush_metadata().expect("flush create");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"remote.txt",
            Duration::from_secs(10)
        ));

        b.unlink(ROOT_INO, b"remote.txt").expect("unlink on b");
        b.flush_metadata().expect("flush unlink on b");
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.lookup(ROOT_INO, b"remote.txt").is_err()
        }));

        drop(a);
        let reopened_a = reopen_pg_engine_with_retry(&db_a);
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = reopened_a.sync_metadata();
            reopened_a.lookup(ROOT_INO, b"remote.txt").is_err()
                && reopened_a.get_attr(file.ino).is_err()
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_own_flush_does_not_skip_earlier_foreign_batch() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-own-flush-gap");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        a.create_dir(ROOT_INO, b"from_a.txt", 0, 0, 0o755)
            .expect("create on a");
        a.flush_metadata().expect("flush a");

        b.create_dir(ROOT_INO, b"from_b.txt", 0, 0, 0o755)
            .expect("create on b");
        b.flush_metadata().expect("flush b");

        assert!(
            wait_visible(&b, ROOT_INO, b"from_a.txt", Duration::from_secs(12)),
            "client B must not skip an earlier foreign batch when it publishes its own batch"
        );
        assert!(
            wait_visible(&a, ROOT_INO, b"from_b.txt", Duration::from_secs(12)),
            "client A should observe client B's batch too"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_earlier_foreign_batch_gap_recovers_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-reopen-gap");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        a.create_dir(ROOT_INO, b"from_a.txt", 0, 0, 0o755)
            .expect("create on a");
        a.flush_metadata().expect("flush a");

        b.create_dir(ROOT_INO, b"from_b.txt", 0, 0, 0o755)
            .expect("create on b");
        b.flush_metadata().expect("flush b");

        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"from_a.txt",
            Duration::from_secs(12)
        ));
        drop(b);

        let reopened_b = reopen_pg_engine_with_retry(&db_b);
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = reopened_b.sync_metadata();
            reopened_b.lookup(ROOT_INO, b"from_a.txt").is_ok()
                && reopened_b.lookup(ROOT_INO, b"from_b.txt").is_ok()
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_two_clients_different_dirs_no_loss() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-dirs");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let dir_a_attr = a
            .create_dir(ROOT_INO, b"client_a", 0, 0, 0o755)
            .expect("a create dir");
        let dir_b_attr = b
            .create_dir(ROOT_INO, b"client_b", 0, 0, 0o755)
            .expect("b create dir");

        for i in 0..20 {
            let name = format!("a_{i:02}.txt");
            a.create_dir(dir_a_attr.ino, name.as_bytes(), 0, 0, 0o755)
                .expect("a create dir");
        }
        for i in 0..20 {
            let name = format!("b_{i:02}.txt");
            b.create_dir(dir_b_attr.ino, name.as_bytes(), 0, 0, 0o755)
                .expect("b create dir");
        }

        a.flush_metadata().expect("flush a");
        b.flush_metadata().expect("flush b");
        let converged = wait_until(Duration::from_secs(20), || {
            let _ = a.flush_metadata();
            let _ = b.flush_metadata();
            let _ = a.sync_metadata();
            let _ = b.sync_metadata();
            a.lookup(ROOT_INO, b"client_a").is_ok()
                && a.lookup(ROOT_INO, b"client_b").is_ok()
                && b.lookup(ROOT_INO, b"client_a").is_ok()
                && b.lookup(ROOT_INO, b"client_b").is_ok()
        });
        assert!(converged);

        let a_client_a = a.lookup(ROOT_INO, b"client_a").expect("a lookup client_a");
        let a_client_b = a.lookup(ROOT_INO, b"client_b").expect("a lookup client_b");
        let b_client_a = b.lookup(ROOT_INO, b"client_a").expect("b lookup client_a");
        let b_client_b = b.lookup(ROOT_INO, b"client_b").expect("b lookup client_b");

        let a_files_in_a = a.readdir(a_client_a.ino).expect("a readdir client_a");
        let a_files_in_b = a.readdir(a_client_b.ino).expect("a readdir client_b");
        let b_files_in_a = b.readdir(b_client_a.ino).expect("b readdir client_a");
        let b_files_in_b = b.readdir(b_client_b.ino).expect("b readdir client_b");

        assert_eq!(
            a_files_in_a.len(),
            20,
            "client A should see all files from A"
        );
        assert_eq!(
            a_files_in_b.len(),
            20,
            "client A should see all files from B"
        );
        assert_eq!(
            b_files_in_a.len(),
            20,
            "client B should see all files from A"
        );
        assert_eq!(
            b_files_in_b.len(),
            20,
            "client B should see all files from B"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_same_dir_concurrent_writes_recovers_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-reopen-samedir");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let shared = a
            .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
            .expect("create shared dir");
        a.flush_metadata().expect("flush shared");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"shared",
            Duration::from_secs(10)
        ));

        for i in 0..20 {
            let name = format!("a_{i:02}.dat");
            a.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
                .expect("a create dir");
        }
        for i in 0..20 {
            let name = format!("b_{i:02}.dat");
            b.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
                .expect("b create dir");
        }

        a.flush_metadata().expect("flush a");
        b.flush_metadata().expect("flush b");
        assert!(wait_until(Duration::from_secs(20), || {
            let _ = a.flush_metadata();
            let _ = b.flush_metadata();
            let _ = a.sync_metadata();
            let _ = b.sync_metadata();
            a.readdir(shared.ino)
                .map(|v| v.len() == 40)
                .unwrap_or(false)
                && b.readdir(shared.ino)
                    .map(|v| v.len() == 40)
                    .unwrap_or(false)
        }));

        drop(a);
        drop(b);

        let reopened_a = reopen_pg_engine_with_retry(&db_a);
        let reopened_b = reopen_pg_engine_with_retry(&db_b);
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = reopened_a.sync_metadata();
            let _ = reopened_b.sync_metadata();
            reopened_a
                .lookup(ROOT_INO, b"shared")
                .ok()
                .and_then(|dir| reopened_a.readdir(dir.ino).ok())
                .map(|v| v.len() == 40)
                .unwrap_or(false)
                && reopened_b
                    .lookup(ROOT_INO, b"shared")
                    .ok()
                    .and_then(|dir| reopened_b.readdir(dir.ino).ok())
                    .map(|v| v.len() == 40)
                    .unwrap_or(false)
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_same_dir_different_files_no_loss() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-samedir");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let shared = a
            .create_dir(ROOT_INO, b"shared", 0, 0, 0o755)
            .expect("create shared dir");
        a.flush_metadata().expect("flush shared");
        assert!(
            wait_visible(&b, ROOT_INO, b"shared", Duration::from_secs(10)),
            "shared dir should become visible on b"
        );

        for i in 0..20 {
            let name = format!("a_{i:02}.dat");
            a.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
                .expect("a create dir");
        }
        for i in 0..20 {
            let name = format!("b_{i:02}.dat");
            b.create_dir(shared.ino, name.as_bytes(), 0, 0, 0o755)
                .expect("b create dir");
        }

        a.flush_metadata().expect("flush a");
        b.flush_metadata().expect("flush b");
        let converged = wait_until(Duration::from_secs(20), || {
            let _ = a.flush_metadata();
            let _ = b.flush_metadata();
            let _ = a.sync_metadata();
            let _ = b.sync_metadata();
            let a_ok = a
                .readdir(shared.ino)
                .map(|v| v.len() >= 40)
                .unwrap_or(false);
            let b_ok = b
                .readdir(shared.ino)
                .map(|v| v.len() >= 40)
                .unwrap_or(false);
            a_ok && b_ok
        });
        assert!(converged);

        let a_entries = a.readdir(shared.ino).expect("a readdir");
        let b_entries = b.readdir(shared.ino).expect("b readdir");
        assert_eq!(a_entries.len(), 40, "A should see all files in shared dir");
        assert_eq!(b_entries.len(), 40, "B should see all files in shared dir");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_concurrent_create_same_name_single_winner() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-concurrent-create");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a2 = Arc::clone(&a);
        let b2 = Arc::clone(&b);
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);

        let t1 = std::thread::spawn(move || {
            barrier_a.wait();
            let out = a2.create_dir(ROOT_INO, b"race.txt", 0, 0, 0o755);
            if out.is_ok() {
                let _ = a2.flush_metadata();
            }
            out
        });
        let t2 = std::thread::spawn(move || {
            barrier_b.wait();
            let out = b2.create_dir(ROOT_INO, b"race.txt", 0, 0, 0o755);
            if out.is_ok() {
                let _ = b2.flush_metadata();
            }
            out
        });

        let r1 = t1.join().expect("thread a");
        let r2 = t2.join().expect("thread b");

        let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|v| *v).count();
        assert!(
            ok_count >= 1,
            "at least one client should create race.txt in async sync model"
        );

        a.flush_metadata().expect("flush a");
        b.flush_metadata().expect("flush b");
        assert!(wait_visible(
            &a,
            ROOT_INO,
            b"race.txt",
            Duration::from_secs(12)
        ));
        let entry = a.lookup(ROOT_INO, b"race.txt").expect("lookup race");
        assert!(entry.ino > 0);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_concurrent_file_vs_dir_same_name_single_winner() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-file-vs-dir-race");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a2 = Arc::clone(&a);
        let b2 = Arc::clone(&b);
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);

        let t1 = std::thread::spawn(move || {
            barrier_a.wait();
            let out = a2.create_file(ROOT_INO, b"victim", 0, 0, 0o644);
            if out.is_ok() {
                let _ = a2.flush_metadata();
            }
            out.map(|attr| (attr.ino, FileType::RegularFile))
        });
        let t2 = std::thread::spawn(move || {
            barrier_b.wait();
            let out = b2.create_dir(ROOT_INO, b"victim", 0, 0, 0o755);
            if out.is_ok() {
                let _ = b2.flush_metadata();
            }
            out.map(|attr| (attr.ino, FileType::Directory))
        });

        let r1 = t1.join().expect("thread a");
        let r2 = t2.join().expect("thread b");
        assert!(
            r1.is_ok() || r2.is_ok(),
            "at least one side must win the namespace race"
        );

        if let Ok(entry) = a.lookup(ROOT_INO, b"victim") {
            if a.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
                set_file_state(&a, entry.ino, 64, 1);
                a.set_file_block(entry.ino, 0, &BlockRef::new_partial(50_320, 64))
                    .expect("set winner block on a");
                assert!(
                    a.publish_file_manifest(entry.ino, 1)
                        .expect("publish winner on a")
                );
                a.flush_metadata().expect("flush winner on a");
            }
        } else if let Ok(entry) = b.lookup(ROOT_INO, b"victim") {
            if b.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
                set_file_state(&b, entry.ino, 64, 1);
                b.set_file_block(entry.ino, 0, &BlockRef::new_partial(50_321, 64))
                    .expect("set winner block on b");
                assert!(
                    b.publish_file_manifest(entry.ino, 1)
                        .expect("publish winner on b")
                );
                b.flush_metadata().expect("flush winner on b");
            }
        }

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.flush_metadata();
            let _ = b.flush_metadata();
            let _ = a.sync_metadata();
            let _ = b.sync_metadata();
            match (a.lookup(ROOT_INO, b"victim"), b.lookup(ROOT_INO, b"victim")) {
                (Ok(ae), Ok(be)) => match (a.get_attr(ae.ino), b.get_attr(be.ino)) {
                    (Ok(a_attr), Ok(b_attr)) => ae.ino == be.ino && a_attr.kind == b_attr.kind,
                    _ => false,
                },
                _ => false,
            }
        }));

        let a_entry = a.lookup(ROOT_INO, b"victim").expect("a lookup victim");
        let b_entry = b.lookup(ROOT_INO, b"victim").expect("b lookup victim");
        let a_attr = a.get_attr(a_entry.ino).expect("a attr victim");
        let b_attr = b.get_attr(b_entry.ino).expect("b attr victim");
        assert_eq!(a_entry.ino, b_entry.ino);
        assert_eq!(a_attr.kind, b_attr.kind);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_file_dir_race_recovers_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-reopen-file-dir-race");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a2 = Arc::clone(&a);
        let b2 = Arc::clone(&b);
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);

        let t1 = std::thread::spawn(move || {
            barrier_a.wait();
            let out = a2.create_file(ROOT_INO, b"victim", 0, 0, 0o644);
            if out.is_ok() {
                let _ = a2.flush_metadata();
            }
            out
        });
        let t2 = std::thread::spawn(move || {
            barrier_b.wait();
            let out = b2.create_dir(ROOT_INO, b"victim", 0, 0, 0o755);
            if out.is_ok() {
                let _ = b2.flush_metadata();
            }
            out
        });

        let _ = t1.join().expect("thread a");
        let _ = t2.join().expect("thread b");

        if let Ok(entry) = a.lookup(ROOT_INO, b"victim") {
            if a.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
                set_file_state(&a, entry.ino, 64, 1);
                a.set_file_block(entry.ino, 0, &BlockRef::new_partial(50_322, 64))
                    .expect("set winner block on a");
                assert!(
                    a.publish_file_manifest(entry.ino, 1)
                        .expect("publish winner on a")
                );
                a.flush_metadata().expect("flush winner on a");
            }
        } else if let Ok(entry) = b.lookup(ROOT_INO, b"victim") {
            if b.get_attr(entry.ino).map(|attr| attr.kind).ok() == Some(FileType::RegularFile) {
                set_file_state(&b, entry.ino, 64, 1);
                b.set_file_block(entry.ino, 0, &BlockRef::new_partial(50_323, 64))
                    .expect("set winner block on b");
                assert!(
                    b.publish_file_manifest(entry.ino, 1)
                        .expect("publish winner on b")
                );
                b.flush_metadata().expect("flush winner on b");
            }
        }

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.flush_metadata();
            let _ = b.flush_metadata();
            let _ = a.sync_metadata();
            let _ = b.sync_metadata();
            match (a.lookup(ROOT_INO, b"victim"), b.lookup(ROOT_INO, b"victim")) {
                (Ok(ae), Ok(be)) => match (a.get_attr(ae.ino), b.get_attr(be.ino)) {
                    (Ok(a_attr), Ok(b_attr)) => ae.ino == be.ino && a_attr.kind == b_attr.kind,
                    _ => false,
                },
                _ => false,
            }
        }));

        let expected = a.lookup(ROOT_INO, b"victim").expect("winner on a");
        let expected_attr = a.get_attr(expected.ino).expect("winner attr on a");

        drop(a);
        drop(b);

        let reopened_a = reopen_pg_engine_with_retry(&db_a);
        let reopened_b = reopen_pg_engine_with_retry(&db_b);

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = reopened_a.sync_metadata();
            let _ = reopened_b.sync_metadata();
            match (
                reopened_a.lookup(ROOT_INO, b"victim"),
                reopened_b.lookup(ROOT_INO, b"victim"),
            ) {
                (Ok(ae), Ok(be)) => {
                    match (reopened_a.get_attr(ae.ino), reopened_b.get_attr(be.ino)) {
                        (Ok(a_attr), Ok(b_attr)) => {
                            ae.ino == expected.ino
                                && be.ino == expected.ino
                                && a_attr.kind == expected_attr.kind
                                && b_attr.kind == expected_attr.kind
                        }
                        _ => false,
                    }
                }
                _ => false,
            }
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_concurrent_rename_single_winner() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-concurrent-rename");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");
        a.create_dir(ROOT_INO, b"source.txt", 0, 0, 0o755)
            .expect("seed source");
        a.flush_metadata().expect("flush source");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"source.txt",
            Duration::from_secs(10)
        ));

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a2 = Arc::clone(&a);
        let b2 = Arc::clone(&b);
        let barrier_a = Arc::clone(&barrier);
        let barrier_b = Arc::clone(&barrier);

        let t1 = std::thread::spawn(move || {
            barrier_a.wait();
            let out = a2.rename(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_a.txt");
            if out.is_ok() {
                let _ = a2.flush_metadata();
            }
            out
        });
        let t2 = std::thread::spawn(move || {
            barrier_b.wait();
            let out = b2.rename(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_b.txt");
            if out.is_ok() {
                let _ = b2.flush_metadata();
            }
            out
        });

        let r1 = t1.join().expect("thread a");
        let r2 = t2.join().expect("thread b");
        let ok_count = [r1.is_ok(), r2.is_ok()].into_iter().filter(|v| *v).count();
        assert!(ok_count >= 1, "at least one rename should apply");
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.lookup(ROOT_INO, b"source.txt").is_err()
        }));
        let a_exists = a.lookup(ROOT_INO, b"moved_by_a.txt").is_ok();
        let b_exists = a.lookup(ROOT_INO, b"moved_by_b.txt").is_ok();
        assert_ne!(a_exists, b_exists, "only one destination name should exist");
        assert!(
            a.lookup(ROOT_INO, b"source.txt").is_err(),
            "source must be gone"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_remote_rename_overwrite_recovers_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-reopen-rename-overwrite");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let src = create_published_small_file(&a, ROOT_INO, b"src.txt", 50_310);
        let dst = create_published_small_file(&a, ROOT_INO, b"dst.txt", 50_311);
        a.flush_metadata().expect("flush seed files");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"src.txt",
            Duration::from_secs(10)
        ));
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"dst.txt",
            Duration::from_secs(10)
        ));

        a.rename(ROOT_INO, b"src.txt", ROOT_INO, b"dst.txt")
            .expect("rename overwrite on a");
        a.flush_metadata().expect("flush rename overwrite");
        assert!(wait_until(Duration::from_secs(10), || {
            let _ = b.sync_metadata();
            b.lookup(ROOT_INO, b"src.txt").is_err()
                && b.lookup(ROOT_INO, b"dst.txt")
                    .map(|entry| entry.ino == src.ino)
                    .unwrap_or(false)
        }));

        drop(b);
        let reopened_b = reopen_pg_engine_with_retry(&db_b);
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = reopened_b.sync_metadata();
            reopened_b.lookup(ROOT_INO, b"src.txt").is_err()
                && reopened_b
                    .lookup(ROOT_INO, b"dst.txt")
                    .map(|entry| entry.ino == src.ino)
                    .unwrap_or(false)
                && reopened_b.get_attr(dst.ino).is_err()
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_publish_before_local_persist_recovers_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };
        let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-crash-after-publish");
        let info = volume.format_info();
        seed_local(&db, &info);

        let a = PgMetaEngine::open(&db).expect("open a");
        a.create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
            .expect("create local file");

        let start = a.flushed_event_seq.load(Ordering::SeqCst);
        let end = a.local.get_counter("nextEventSeq").expect("nextEventSeq");
        let events = a.local.read_events_range(start, end).expect("local events");
        let _version = a
            .push_event_log_batch_locked(&events)
            .expect("push batch without local persist");

        drop(a);

        let reopened = reopen_pg_engine_with_retry(&db);
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = reopened.sync_metadata();
            reopened.lookup(ROOT_INO, b"draft.txt").is_ok()
                && reopened.flushed_event_seq.load(Ordering::SeqCst)
                    == reopened.local.get_counter("nextEventSeq").unwrap_or(0)
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_publish_then_remote_unlink_recovers_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };
        let _poll_guard = set_test_env("STUDIOFS_PG_SYNC_POLL_SECS", "3600");

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-crash-publish-then-unlink");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_dir(ROOT_INO, b"remote.txt", 0, 0, 0o755)
            .expect("create local entry");
        let start = a.flushed_event_seq.load(Ordering::SeqCst);
        let end = a.local.get_counter("nextEventSeq").expect("nextEventSeq");
        let events = a.local.read_events_range(start, end).expect("local events");
        let _version = a
            .push_event_log_batch_locked(&events)
            .expect("push batch without local persist");

        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"remote.txt",
            Duration::from_secs(12)
        ));
        b.rmdir(ROOT_INO, b"remote.txt").expect("peer remove");
        b.flush_metadata().expect("flush peer remove");

        drop(a);

        let reopened_a = reopen_pg_engine_with_retry(&db_a);
        assert!(wait_until(Duration::from_secs(12), || {
            let _ = reopened_a.sync_metadata();
            reopened_a.lookup(ROOT_INO, b"remote.txt").is_err()
                && reopened_a.get_attr(file.ino).is_err()
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_unpublished_local_create_flushes_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-reopen-unpublished-create");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        a.create_dir(ROOT_INO, b"draft.txt", 0, 0, 0o755)
            .expect("create local draft");
        drop(a);

        let reopened_a = reopen_pg_engine_with_retry(&db_a);
        assert!(
            reopened_a.replication_backlog().pending_ops > 0,
            "reopen must rebuild pending state for unpublished local events"
        );

        reopened_a
            .flush_metadata()
            .expect("flush restored unpublished events");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"draft.txt",
            Duration::from_secs(12)
        ));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_claimed_batch_crash_flushes_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-reopen-claimed-batch-crash");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        a.create_dir(ROOT_INO, b"draft.txt", 0, 0, 0o755)
            .expect("create local draft");
        let claimed = a.claim_pending_batch(true).expect("claim pending batch");
        assert!(claimed.event_end > claimed.event_start);
        drop(a);

        let reopened_a = reopen_pg_engine_with_retry(&db_a);
        assert!(
            reopened_a.replication_backlog().pending_ops > 0,
            "reopen must recover a claimed-but-unpublished batch after crash"
        );

        reopened_a.flush_metadata().expect("flush recovered batch");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"draft.txt",
            Duration::from_secs(12)
        ));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_reopen_after_claimed_multi_event_batch_preserves_sequence_without_discover() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-reopen-claimed-multievent");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let dir = a
            .create_dir(ROOT_INO, b"drafts", 0, 0, 0o755)
            .expect("create drafts dir");
        let file = a
            .create_dir(dir.ino, b"temp.txt", 0, 0, 0o755)
            .expect("create temp dir");
        a.rename(dir.ino, b"temp.txt", dir.ino, b"final.txt")
            .expect("rename temp to final");
        let claimed = a.claim_pending_batch(true).expect("claim pending batch");
        assert!(claimed.event_end > claimed.event_start);
        drop(a);

        let reopened_a = reopen_pg_engine_with_retry(&db_a);
        reopened_a
            .flush_metadata()
            .expect("flush recovered multi-event batch");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = b.sync_metadata();
            match b.lookup(ROOT_INO, b"drafts") {
                Ok(dir_entry) => {
                    b.lookup(dir_entry.ino, b"temp.txt").is_err()
                        && b.lookup(dir_entry.ino, b"final.txt")
                            .map(|entry| entry.ino == file.ino)
                            .unwrap_or(false)
                }
                Err(_) => false,
            }
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_delete_and_recreate() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-delete-recreate");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let first = a
            .create_dir(ROOT_INO, b"recreate.txt", 0, 0, 0o755)
            .expect("create first");
        a.rmdir(ROOT_INO, b"recreate.txt").expect("remove first");
        let second = a
            .create_dir(ROOT_INO, b"recreate.txt", 0, 0, 0o755)
            .expect("create second");
        assert_ne!(
            first.ino, second.ino,
            "recreated entry should have a new inode in current model"
        );

        a.flush_metadata().expect("flush a");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"recreate.txt",
            Duration::from_secs(10)
        ));
        let on_b = b.lookup(ROOT_INO, b"recreate.txt").expect("lookup on b");
        assert_eq!(on_b.ino, second.ino);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_rapid_create_delete() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-rapid-cd");
        seed_local(&db, &volume.format_info());

        let pg = PgMetaEngine::open(&db).expect("open pg");
        for i in 0..200 {
            let name = format!("rapid_{i:03}.tmp");
            pg.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
                .expect("create");
            pg.unlink(ROOT_INO, name.as_bytes()).expect("unlink");
        }

        let entries = pg.readdir(ROOT_INO).expect("readdir root");
        assert!(
            entries.is_empty(),
            "rapid create/delete should leave root empty"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_hardlinks_cross_client() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-hardlink");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = create_published_small_file(&a, ROOT_INO, b"orig.txt", 50_312);
        a.link(file.ino, ROOT_INO, b"link.txt").expect("hardlink");

        a.flush_metadata().expect("flush a");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"orig.txt",
            Duration::from_secs(10)
        ));
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"link.txt",
            Duration::from_secs(10)
        ));
        let orig = b.lookup(ROOT_INO, b"orig.txt").expect("lookup orig");
        let link = b.lookup(ROOT_INO, b"link.txt").expect("lookup link");
        assert_eq!(orig.ino, link.ino, "hardlink should point to same inode");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_symlinks_cross_client() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-symlink");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let sym = a
            .create_symlink(ROOT_INO, b"my-link", b"target/path.txt", 0, 0)
            .expect("create symlink");
        a.flush_metadata().expect("flush a");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"my-link",
            Duration::from_secs(10)
        ));
        let link = b.lookup(ROOT_INO, b"my-link").expect("lookup symlink");
        assert_eq!(link.ino, sym.ino);
        let target = b.get_symlink_target(link.ino).expect("readlink");
        assert_eq!(target, b"target/path.txt");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_peer_can_update_foreign_visible_symlink_target() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-peer-symlink-update");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let sym = a
            .create_symlink(ROOT_INO, b"shared-link", b"target/a", 0, 0)
            .expect("create symlink on a");
        a.flush_metadata().expect("flush create on a");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"shared-link",
            Duration::from_secs(10)
        ));
        assert_eq!(b.get_symlink_target(sym.ino).unwrap(), b"target/a");

        b.set_symlink_target(sym.ino, b"target/b")
            .expect("update symlink on b");
        b.flush_metadata().expect("flush update on b");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.get_symlink_target(sym.ino)
                .map(|target| target == b"target/b")
                .unwrap_or(false)
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_local_xattr_overlay_masks_foreign_update_until_flush() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-xattr-overlay-precedence");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = create_published_small_file(&a, ROOT_INO, b"shared.txt", 50_313);
        a.set_xattr(file.ino, b"user.color", b"red", 0)
            .expect("set red xattr on a");
        a.flush_metadata().expect("flush red on a");
        assert!(wait_visible(
            &b,
            ROOT_INO,
            b"shared.txt",
            Duration::from_secs(10)
        ));
        assert_eq!(
            b.get_xattr(file.ino, b"user.color").unwrap(),
            Some(b"red".to_vec())
        );

        b.set_xattr(file.ino, b"user.color", b"blue", 0)
            .expect("set blue xattr on b");

        a.set_xattr(file.ino, b"user.color", b"green", 0)
            .expect("set green xattr on a");
        a.flush_metadata().expect("flush green on a");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = b.sync_metadata();
            b.get_xattr(file.ino, b"user.color")
                .map(|value| value == Some(b"blue".to_vec()))
                .unwrap_or(false)
        }));
        assert_eq!(
            b.get_xattr(file.ino, b"user.color").unwrap(),
            Some(b"blue".to_vec()),
            "local xattr overlay should mask foreign base update"
        );

        b.flush_metadata().expect("flush blue on b");

        assert!(wait_until(Duration::from_secs(12), || {
            let _ = a.sync_metadata();
            a.get_xattr(file.ino, b"user.color")
                .map(|value| value == Some(b"blue".to_vec()))
                .unwrap_or(false)
        }));
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_basic_crud() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-basic-crud");
        seed_local(&db, &volume.format_info());

        let pg = PgMetaEngine::open(&db).expect("open pg");
        let file = pg
            .create_file(ROOT_INO, b"hello.txt", 0, 0, 0o644)
            .expect("create");
        let looked = pg.lookup(ROOT_INO, b"hello.txt").expect("lookup");
        assert_eq!(file.ino, looked.ino);

        pg.rename(ROOT_INO, b"hello.txt", ROOT_INO, b"renamed.txt")
            .expect("rename");
        assert!(pg.lookup(ROOT_INO, b"hello.txt").is_err());
        assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_ok());

        pg.unlink(ROOT_INO, b"renamed.txt").expect("unlink");
        assert!(pg.lookup(ROOT_INO, b"renamed.txt").is_err());

        pg.create_dir(ROOT_INO, b"subdir", 0, 0, 0o755)
            .expect("mkdir");
        pg.rmdir(ROOT_INO, b"subdir").expect("rmdir");
        assert!(pg.lookup(ROOT_INO, b"subdir").is_err());
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_deep_path() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-deep-path");
        seed_local(&db, &volume.format_info());

        let pg = PgMetaEngine::open(&db).expect("open pg");
        let mut parent = ROOT_INO;
        for n in [b"a", b"b", b"c", b"d", b"e"] {
            let dir = pg.create_dir(parent, n, 0, 0, 0o755).expect("mkdir");
            parent = dir.ino;
        }
        let leaf = pg
            .create_file(parent, b"leaf.txt", 0, 0, 0o644)
            .expect("create leaf");
        let looked = pg.lookup(parent, b"leaf.txt").expect("lookup leaf");
        assert_eq!(leaf.ino, looked.ino);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_large_directory() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-large-dir");
        seed_local(&db, &volume.format_info());

        let pg = PgMetaEngine::open(&db).expect("open pg");
        for i in 0..1000 {
            let name = format!("file_{i:04}.dat");
            pg.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644)
                .expect("create");
        }
        let entries = pg.readdir(ROOT_INO).expect("readdir");
        assert_eq!(entries.len(), 1000);
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_special_filenames() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-special-names");
        seed_local(&db, &volume.format_info());

        let pg = PgMetaEngine::open(&db).expect("open pg");
        let names = vec![
            b"file with spaces.txt".to_vec(),
            "файл_тест_日本語.txt".as_bytes().to_vec(),
            format!("{}.txt", "a".repeat(200)).as_bytes().to_vec(),
            b"file...multiple...dots.txt".to_vec(),
            b"-dashstart.txt".to_vec(),
        ];
        for n in &names {
            pg.create_file(ROOT_INO, n, 0, 0, 0o644).expect("create");
        }
        for n in &names {
            assert!(
                pg.lookup(ROOT_INO, n).is_ok(),
                "name should resolve: {:?}",
                n
            );
        }
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_readdir_during_modifications() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-readdir-race");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let writer = std::thread::spawn({
            let a = Arc::clone(&a);
            move || {
                for i in 0..120 {
                    let name = format!("w_{i:03}.dat");
                    let _ = a.create_file(ROOT_INO, name.as_bytes(), 0, 0, 0o644);
                    if i % 3 == 0 {
                        let _ = a.unlink(ROOT_INO, name.as_bytes());
                    }
                }
            }
        });

        for _ in 0..80 {
            let _ = b.sync_metadata();
            let _ = b.readdir(ROOT_INO).expect("readdir should not fail");
            std::thread::sleep(Duration::from_millis(10));
        }
        writer.join().expect("writer join");
        b.sync_metadata().expect("final sync");
        let _ = b.readdir(ROOT_INO).expect("final readdir");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_visibility_timing() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-vis-timing");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let mut delays = Vec::new();
        for i in 0..5 {
            let name = format!("vis_{i:02}.dat");
            a.create_dir(ROOT_INO, name.as_bytes(), 0, 0, 0o755)
                .expect("create");
            a.flush_metadata().expect("flush a");
            let start = std::time::Instant::now();
            let mut seen = false;
            for _ in 0..50 {
                b.sync_metadata().expect("sync b");
                if b.lookup(ROOT_INO, name.as_bytes()).is_ok() {
                    seen = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            assert!(seen, "entry should become visible on peer");
            delays.push(start.elapsed().as_millis() as u64);
        }

        let avg = delays.iter().sum::<u64>() / delays.len() as u64;
        assert!(avg < 5000, "avg visibility delay should stay bounded");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_snapshot_eventually_visible_and_mountable_on_peer() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-snapshot-visibility");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"snap-peer.txt", 0, 0, 0o644)
            .expect("create file on a");
        set_file_state(&a, file.ino, 64, 1);
        a.set_file_block(file.ino, 0, &BlockRef::new_partial(44_001, 64))
            .expect("set block");

        let created = a.create_snapshot(ROOT_INO).expect("create snapshot");
        a.flush_metadata().expect("flush snapshot");

        let visible = wait_until(Duration::from_secs(12), || {
            let _ = b.sync_metadata();
            b.get_snapshot(&created.id)
                .ok()
                .flatten()
                .map(|s| s.state == SnapshotState::Synced)
                .unwrap_or(false)
        });
        assert!(visible, "snapshot should become visible on peer");

        let peer_snapshot = b
            .get_snapshot(&created.id)
            .expect("snapshot query")
            .expect("snapshot exists on peer");
        assert!(peer_snapshot.pinned_object_ids.contains(&44_001));
        assert!(!peer_snapshot.snapshot_payload.is_empty());

        let mount_dir = tempdir().expect("mount tempdir");
        let mount_db = mount_dir.path().join("snapshot-mount.redb");
        let mount_engine = RedbMetaEngine::open(&mount_db).expect("open mount redb");
        let mut mount_info = info.clone();
        mount_info.metadata.backend = MetadataBackend::RedbLocal;
        mount_info.metadata.postgres = None;
        mount_engine
            .set_format_info(&mount_info)
            .expect("set mount format");
        let snap_state = RedbMetaEngine::decode_snapshot_payload(&peer_snapshot.snapshot_payload)
            .expect("decode payload");
        mount_engine
            .import_snapshot(&snap_state)
            .expect("import payload");
        assert!(mount_engine.lookup(ROOT_INO, b"snap-peer.txt").is_ok());
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_janitor_lock_is_exclusive() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-janitor-lock");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        assert!(
            a.try_acquire_global_janitor_lock()
                .expect("janitor lock acquire on a"),
            "first janitor should acquire the PG lock"
        );
        assert!(
            !b.try_acquire_global_janitor_lock()
                .expect("janitor lock probe on b while a holds it"),
            "janitor should skip when another client already holds the PG lock"
        );
        a.release_global_janitor_lock()
            .expect("janitor lock release on a");

        assert!(
            b.try_acquire_global_janitor_lock()
                .expect("janitor lock acquire on b after release"),
            "janitor should proceed once the PG lock is free again"
        );
        b.release_global_janitor_lock()
            .expect("janitor lock release on b");
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_janitor_reads_snapshot_pins_from_postgres() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir_a = tempdir().expect("tempdir a");
        let dir_b = tempdir().expect("tempdir b");
        let db_a = dir_a.path().join("meta-a.redb");
        let db_b = dir_b.path().join("meta-b.redb");
        let volume = PgTestVolume::new(&dsn, "pg-janitor-snapshot-pins");
        let info = volume.format_info();
        seed_local(&db_a, &info);
        seed_local(&db_b, &info);

        let a = PgMetaEngine::open(&db_a).expect("open a");
        let b = PgMetaEngine::open(&db_b).expect("open b");

        let file = a
            .create_file(ROOT_INO, b"snap-gc.txt", 0, 0, 0o644)
            .expect("create file");
        set_file_state(&a, file.ino, 64, 1);
        a.set_file_block(file.ino, 0, &BlockRef::new_partial(55_001, 64))
            .expect("set block");
        let _snapshot = a.create_snapshot(ROOT_INO).expect("create snapshot");
        a.flush_metadata().expect("flush snapshot");

        let pins = b
            .get_global_snapshot_pins()
            .expect("query snapshot pins from postgres");
        assert!(
            pins.contains(&55_001),
            "janitor must see cross-client snapshot pins from postgres"
        );
    }

    #[test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    fn test_pg_uuid_mismatch_blocks_consistency_check() {
        let Some(dsn) = pg_dsn() else {
            return;
        };

        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("meta.redb");
        let volume = PgTestVolume::new(&dsn, "pg-uuid-mismatch");
        let info = volume.format_info();
        seed_local(&db_path, &info);

        let engine = PgMetaEngine::open(&db_path).expect("open engine");

        // Initial check should pass
        engine
            .check_consistency()
            .expect("initial UUID check must pass");

        // Simulate disk recreation: change UUID in PG for this volume_id
        volume.update_uuid("recreated-uuid-12345");

        // Next check should fail
        let result = engine.check_consistency();
        assert!(result.is_err());
        match result.unwrap_err() {
            MetaError::UuidMismatch { local, remote } => {
                assert_eq!(local, info.uuid);
                assert_eq!(remote, "recreated-uuid-12345");
            }
            e => panic!("expected UuidMismatch, got {:?}", e),
        }
    }
}

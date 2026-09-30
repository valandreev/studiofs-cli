//! redb implementation of MetaEngine.
//!
//! Uses redb as an embedded ACID KV store for all filesystem metadata.

use crate::coherence::{InvalidationIntent, InvalidationJournal, PendingInvalidations};
use crate::engine::*;
use crate::snapshot_ports::{UploadedFragmentLedger, UploadedFragmentLedgerEntry};
use crate::types::*;
use redb::{
    Database, Durability, ReadOnlyDatabase, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, WriteTransaction,
};
use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::ErrorKind;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, Once, RwLock, RwLockReadGuard, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

pub(crate) mod layout;
pub use self::layout::RedbSnapshot;
use self::layout::*;

mod fs_ops;

impl From<redb::StorageError> for MetaError {
    fn from(error: redb::StorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

static VOLATILE_DURABILITY_WARN: Once = Once::new();
const REDB_VOLATILE_DURABILITY_BARRIER_INTERVAL_MS: u64 = 1_000;
// ponytail: page scans still block new writes; cache for five seconds and move sampling off-read-path if measured scans remain costly.
const LOCAL_STORAGE_PAGE_STATS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default)]
struct LocalStorageCompactionStats {
    attempts: u64,
    compacted: u64,
    no_work: u64,
    skipped_busy: u64,
    errors: u64,
    last_result: Option<LocalStorageCompactionResult>,
    last_error: bool,
    last_skip_reason: Option<LocalStorageCompactionSkipReason>,
    last_duration_ns: Option<u64>,
    last_success_bytes_before: Option<u64>,
    last_success_bytes_after: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct LocalStoragePageStats {
    allocated_pages: u64,
    leaf_pages: u64,
    branch_pages: u64,
    tree_height: u32,
    stored_bytes: u64,
    metadata_bytes: u64,
    fragmented_bytes: u64,
    page_size_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct CachedLocalStoragePageStats {
    sampled_at: Instant,
    stats: LocalStoragePageStats,
}

#[derive(Debug, Clone, Copy, Default)]
struct LocalEventLogPruneStats {
    rows_pruned: u64,
    payload_bytes_pruned: u64,
    skipped_busy: u64,
    last_rows: Option<u64>,
    last_first_seq: Option<u64>,
    last_last_seq: Option<u64>,
    last_payload_bytes: Option<u64>,
    last_duration_ns: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LocalEventLogPruneBatch {
    pub(crate) rows: usize,
    pub(crate) first_seq: Option<u64>,
    pub(crate) last_seq: Option<u64>,
    pub(crate) payload_bytes: u64,
    pub(crate) floor_updated: bool,
    pub(crate) skipped_busy: bool,
}

/// Metadata engine backed by redb.
pub struct RedbMetaEngine {
    db: Option<Arc<RwLock<Database>>>,
    read_only_db: Option<Arc<ReadOnlyDatabase>>,
    metadata_path: PathBuf,
    local_storage_compaction: Mutex<LocalStorageCompactionStats>,
    local_event_log_prune: Mutex<LocalEventLogPruneStats>,
    local_storage_page_stats: Mutex<Option<CachedLocalStoragePageStats>>,
    local_storage_page_stats_refresh: Mutex<()>,
    write_metrics: Arc<MetadataWriteMetrics>,
    relaxed_durability: Option<Arc<RelaxedDurabilityRuntime>>,
    write_txn_lock: Arc<Mutex<()>>,
    invalidation_journal: Mutex<InvalidationJournal>,
    pub(crate) invalidation_notify: Arc<tokio::sync::Notify>,
    #[cfg(test)]
    allocator_state_write_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

/// A pinned redb read view for a consistent serving-checkpoint export.
///
/// Reads and exports use one MVCC snapshot while foreground writes continue.
/// Drop it after the checkpoint data is built so redb can reclaim older pages.
pub(crate) struct ServingCheckpointReadSnapshot {
    transaction: ReadTransaction,
}

impl ServingCheckpointReadSnapshot {
    /// Read a counter from this snapshot's transaction.
    pub(crate) fn get_counter(&self, name: &str) -> MetaResult<u64> {
        RedbMetaEngine::read_counter_from_read_txn(&self.transaction, name)
    }

    /// Read format metadata from this snapshot's transaction.
    pub(crate) fn format_info(&self) -> MetaResult<crate::schema::FormatInfo> {
        RedbMetaEngine::read_format_info_from_read_txn(&self.transaction)
    }

    /// Serialize the serving checkpoint visible in this snapshot.
    pub(crate) fn export_serving_checkpoint_bytes(&self) -> MetaResult<Vec<u8>> {
        let checkpoint = self.export_serving_checkpoint()?;
        postcard::to_stdvec(&checkpoint).map_err(|error| {
            MetaError::Serialization(format!("serving checkpoint encode failed: {error}"))
        })
    }

    /// Serialize this checkpoint while periodically checking for cancellation.
    pub(crate) fn export_serving_checkpoint_bytes_with_check(
        &self,
        check: &impl Fn() -> MetaResult<()>,
    ) -> MetaResult<Vec<u8>> {
        let checkpoint = self.export_serving_checkpoint_with_check(check)?;
        serialize_with_check(&checkpoint, check, "serving checkpoint")
    }

    /// Export the serving checkpoint visible in this snapshot.
    pub(crate) fn export_serving_checkpoint(
        &self,
    ) -> MetaResult<crate::snapshot::ServingCheckpoint> {
        RedbMetaEngine::export_serving_checkpoint_from_read_txn(&self.transaction)
    }

    pub(crate) fn export_serving_checkpoint_with_check(
        &self,
        check: &impl Fn() -> MetaResult<()>,
    ) -> MetaResult<crate::snapshot::ServingCheckpoint> {
        RedbMetaEngine::export_serving_checkpoint_from_read_txn_with_check(&self.transaction, check)
    }
}

struct CheckedCheckpointWriter<'a, F> {
    bytes: &'a mut Vec<u8>,
    check: &'a F,
    error: Option<MetaError>,
    next_check_at: usize,
}

pub(super) fn serialize_with_check<T: serde::Serialize>(
    value: &T,
    check: &impl Fn() -> MetaResult<()>,
    context: &str,
) -> MetaResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = CheckedCheckpointWriter {
        bytes: &mut bytes,
        check,
        error: None,
        next_check_at: 0,
    };
    let serialization_error = postcard::to_io(value, &mut writer)
        .err()
        .map(|error| format!("{context} encode failed: {error}"));
    if let Some(error) = writer.error.take() {
        return Err(error);
    }
    if let Some(error) = serialization_error {
        return Err(MetaError::Serialization(error));
    }
    check()?;
    Ok(bytes)
}

impl<F> std::io::Write for CheckedCheckpointWriter<'_, F>
where
    F: Fn() -> MetaResult<()>,
{
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        const CHECK_INTERVAL: usize = 64 * 1024;

        for chunk in buffer.chunks(CHECK_INTERVAL) {
            // Check before the next chunk would exceed the byte budget.
            if self.bytes.len() >= self.next_check_at
                || chunk.len() > self.next_check_at - self.bytes.len()
            {
                if let Err(error) = (self.check)() {
                    self.error = Some(error);
                    return Err(std::io::Error::other("serving checkpoint export cancelled"));
                }
                self.next_check_at = self.bytes.len().saturating_add(CHECK_INTERVAL);
            }
            self.bytes.extend_from_slice(chunk);
        }
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Err(error) = (self.check)() {
            self.error = Some(error);
            return Err(std::io::Error::other("serving checkpoint export cancelled"));
        }
        Ok(())
    }
}

struct RelaxedDurabilityState {
    started: Instant,
    interval_ms: u64,
    next_barrier_ms: AtomicU64,
    pending: AtomicBool,
    force_immediate: AtomicBool,
    stopping: AtomicBool,
    wake: (Mutex<()>, Condvar),
    immediate_barriers: AtomicU64,
    #[cfg(test)]
    fail_next_barrier: AtomicBool,
    #[cfg(test)]
    after_barrier_commit: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
    #[cfg(test)]
    wait_count: AtomicU64,
    #[cfg(test)]
    last_wait_timeout_ms: AtomicU64,
}

struct RelaxedDurabilityRuntime {
    state: Arc<RelaxedDurabilityState>,
    worker: Option<JoinHandle<()>>,
}

impl Drop for RelaxedDurabilityRuntime {
    fn drop(&mut self) {
        {
            let _wake_guard = self
                .state
                .wake
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.state.stopping.store(true, Ordering::Release);
            self.state.wake.1.notify_all();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for RedbMetaEngine {
    fn drop(&mut self) {
        // Redb writes its allocator-state checkpoint from Database::Drop.
        // Stop and join the relaxed-durability worker first so it cannot still
        // hold a database Arc or start a final barrier while that checkpoint
        // is being written.
        drop(self.relaxed_durability.take());
        drop(self.read_only_db.take());
        drop(self.db.take());
    }
}

/// Lock-free counters for one metadata write-transaction stage.
#[derive(Default)]
struct MetadataWriteStageMetrics {
    calls: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
}

/// Lock-free timing counters for redb write transaction stages.
#[derive(Default)]
struct MetadataWriteMetrics {
    begin_write: MetadataWriteStageMetrics,
    mutation: MetadataWriteStageMetrics,
    commit: MetadataWriteStageMetrics,
    batch: MetadataBatchWriteMetrics,
}

struct MetadataBatchWriteMetrics {
    batches: AtomicU64,
    requested_ops: AtomicU64,
    actual_ops: AtomicU64,
    estimated_slices: AtomicU64,
    estimated_payload_bytes: AtomicU64,
    failures: AtomicU64,
    rollback_index: AtomicU64,
    total_ns: AtomicU64,
}

const NO_ROLLBACK_INDEX: u64 = u64::MAX;

#[derive(Clone, Copy)]
struct EstimatedBatchMetrics {
    slices: u64,
    payload_bytes: u64,
}

impl Default for MetadataBatchWriteMetrics {
    fn default() -> Self {
        Self {
            batches: AtomicU64::new(0),
            requested_ops: AtomicU64::new(0),
            actual_ops: AtomicU64::new(0),
            estimated_slices: AtomicU64::new(0),
            estimated_payload_bytes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            rollback_index: AtomicU64::new(NO_ROLLBACK_INDEX),
            total_ns: AtomicU64::new(0),
        }
    }
}

impl MetadataWriteMetrics {
    fn observe_batch(
        &self,
        requested_ops: u64,
        actual_ops: u64,
        estimated: EstimatedBatchMetrics,
        failure_index: Option<usize>,
        failed: bool,
        elapsed: Duration,
    ) {
        self.batch.batches.fetch_add(1, Ordering::Relaxed);
        self.batch
            .requested_ops
            .fetch_add(requested_ops, Ordering::Relaxed);
        self.batch
            .actual_ops
            .fetch_add(actual_ops, Ordering::Relaxed);
        self.batch
            .estimated_slices
            .fetch_add(estimated.slices, Ordering::Relaxed);
        self.batch
            .estimated_payload_bytes
            .fetch_add(estimated.payload_bytes, Ordering::Relaxed);
        self.batch.total_ns.fetch_add(
            elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        if failed {
            self.batch.failures.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(index) = failure_index {
            self.batch
                .rollback_index
                .store(index as u64, Ordering::Relaxed);
        }
    }

    fn observe(stage: &MetadataWriteStageMetrics, elapsed: Duration) {
        let elapsed_ns = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        // Snapshot readers may observe the three relaxed counters between updates;
        // this deliberate tearing keeps the hot write path lock-free.
        stage.calls.fetch_add(1, Ordering::Relaxed);
        stage.total_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        stage.max_ns.fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn snapshot_stage(stage: &MetadataWriteStageMetrics) -> MetadataWriteStageTiming {
        MetadataWriteStageTiming {
            calls: stage.calls.load(Ordering::Relaxed),
            total_ns: stage.total_ns.load(Ordering::Relaxed),
            max_ns: stage.max_ns.load(Ordering::Relaxed),
        }
    }

    fn snapshot(&self) -> MetadataWriteObservability {
        MetadataWriteObservability {
            begin_write: Self::snapshot_stage(&self.begin_write),
            mutation: Self::snapshot_stage(&self.mutation),
            commit: Self::snapshot_stage(&self.commit),
            batch: MetadataBatchWriteObservability {
                batches: self.batch.batches.load(Ordering::Relaxed),
                requested_ops: self.batch.requested_ops.load(Ordering::Relaxed),
                actual_ops: self.batch.actual_ops.load(Ordering::Relaxed),
                estimated_slices: self.batch.estimated_slices.load(Ordering::Relaxed),
                estimated_payload_bytes: self.batch.estimated_payload_bytes.load(Ordering::Relaxed),
                failures: self.batch.failures.load(Ordering::Relaxed),
                rollback_index: match self.batch.rollback_index.load(Ordering::Relaxed) {
                    NO_ROLLBACK_INDEX => None,
                    index => Some(index),
                },
                total_ns: self.batch.total_ns.load(Ordering::Relaxed),
            },
        }
    }
}

#[cfg(test)]
pub(crate) struct CounterWriteTransaction<'a> {
    txn: &'a WriteTransaction,
}

pub(crate) struct RedbWriteTransaction<'a> {
    txn: WriteTransaction,
    _db: RwLockReadGuard<'a, Database>,
    _write_txn_guard: MutexGuard<'a, ()>,
    write_metrics: &'a MetadataWriteMetrics,
    mutation_started: Instant,
    relaxed_durability: Option<&'a RelaxedDurabilityRuntime>,
    invalidation_journal: &'a Mutex<InvalidationJournal>,
    invalidation_notify: &'a tokio::sync::Notify,
    invalidations: RefCell<PendingInvalidations>,
}

fn begin_redb_write_txn(
    db: &Database,
    relaxed_durability: Option<&RelaxedDurabilityRuntime>,
) -> MetaResult<WriteTransaction> {
    let mut txn = db
        .begin_write()
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    if let Some(runtime) = relaxed_durability {
        txn.set_durability(if runtime.state.force_immediate.load(Ordering::Acquire) {
            Durability::Immediate
        } else {
            Durability::None
        })
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    }
    Ok(txn)
}

fn begin_immediate_redb_write_txn(db: &Database) -> MetaResult<WriteTransaction> {
    let mut txn = db
        .begin_write()
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    txn.set_durability(Durability::Immediate)
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    Ok(txn)
}

fn immediate_metadata_barrier(
    db: &RwLock<Database>,
    write_txn_lock: &Mutex<()>,
    relaxed_durability: Option<&RelaxedDurabilityState>,
) -> MetaResult<()> {
    let pending_was_cleared = relaxed_durability
        .map(|state| state.pending.swap(false, Ordering::AcqRel))
        .unwrap_or(false);
    let result: MetaResult<()> = (|| {
        #[cfg(test)]
        if relaxed_durability
            .is_some_and(|state| state.fail_next_barrier.swap(false, Ordering::AcqRel))
        {
            return Err(MetaError::Storage(
                "simulated metadata barrier failure".to_string(),
            ));
        }

        let _write_txn_guard = write_txn_lock
            .lock()
            .map_err(|_| MetaError::Storage("redb write lock poisoned".to_string()))?;
        let db = db
            .read()
            .map_err(|_| MetaError::Storage("redb lock poisoned".to_string()))?;
        let mut txn = db
            .begin_write()
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        txn.set_durability(Durability::Immediate)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))?;

        #[cfg(test)]
        if let Some(barrier) = relaxed_durability.and_then(|state| {
            state
                .after_barrier_commit
                .lock()
                .ok()
                .and_then(|mut hook| hook.take())
        }) {
            let _ = barrier.0.send(());
            let _ = barrier.1.recv_timeout(Duration::from_secs(10));
        }

        Ok(())
    })();
    if result.is_err()
        && pending_was_cleared
        && let Some(state) = relaxed_durability
    {
        state.pending.store(true, Ordering::Release);
    }
    result?;
    if let Some(state) = relaxed_durability {
        state.immediate_barriers.fetch_add(1, Ordering::Relaxed);
        let elapsed_ms = state
            .started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        state.next_barrier_ms.store(
            elapsed_ms.saturating_add(state.interval_ms),
            Ordering::Release,
        );
    }
    Ok(())
}

fn relaxed_barrier_due(state: &RelaxedDurabilityState) -> bool {
    let elapsed_ms = state
        .started
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    elapsed_ms >= state.next_barrier_ms.load(Ordering::Acquire)
}

fn relaxed_barrier_wait_timeout(state: &RelaxedDurabilityState) -> Duration {
    let elapsed_ms = state
        .started
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let next_barrier_ms = state.next_barrier_ms.load(Ordering::Acquire);
    Duration::from_millis(
        next_barrier_ms
            .checked_sub(elapsed_ms)
            .unwrap_or(state.interval_ms),
    )
}

fn start_relaxed_durability_runtime_with_interval(
    db: &Arc<RwLock<Database>>,
    write_txn_lock: Arc<Mutex<()>>,
    interval: Duration,
) -> MetaResult<Arc<RelaxedDurabilityRuntime>> {
    let interval_ms = interval.as_millis().min(u128::from(u64::MAX)) as u64;
    let state = Arc::new(RelaxedDurabilityState {
        started: Instant::now(),
        interval_ms,
        next_barrier_ms: AtomicU64::new(interval_ms),
        pending: AtomicBool::new(false),
        force_immediate: AtomicBool::new(false),
        stopping: AtomicBool::new(false),
        wake: (Mutex::new(()), Condvar::new()),
        immediate_barriers: AtomicU64::new(0),
        #[cfg(test)]
        fail_next_barrier: AtomicBool::new(false),
        #[cfg(test)]
        after_barrier_commit: Mutex::new(None),
        #[cfg(test)]
        wait_count: AtomicU64::new(0),
        #[cfg(test)]
        last_wait_timeout_ms: AtomicU64::new(0),
    });
    let worker_state = Arc::clone(&state);
    let worker_write_txn_lock = Arc::clone(&write_txn_lock);
    let weak_db: Weak<RwLock<Database>> = Arc::downgrade(db);
    let worker = std::thread::Builder::new()
        .name("studiofs-redb-durability".to_string())
        .spawn(move || {
            loop {
                let guard = worker_state
                    .wake
                    .0
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if worker_state.stopping.load(Ordering::Acquire) {
                    break;
                }
                let wait_timeout = relaxed_barrier_wait_timeout(&worker_state);
                #[cfg(test)]
                {
                    worker_state.last_wait_timeout_ms.store(
                        wait_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                        Ordering::Release,
                    );
                    worker_state.wait_count.fetch_add(1, Ordering::Release);
                }
                let (guard, _) = worker_state
                    .wake
                    .1
                    .wait_timeout(guard, wait_timeout)
                    .unwrap_or_else(|e| e.into_inner());
                drop(guard);
                if worker_state.stopping.load(Ordering::Acquire) {
                    break;
                }
                if !worker_state.pending.load(Ordering::Acquire)
                    || !relaxed_barrier_due(&worker_state)
                {
                    continue;
                }
                let Some(db) = weak_db.upgrade() else {
                    break;
                };
                if let Err(error) =
                    immediate_metadata_barrier(&db, &worker_write_txn_lock, Some(&worker_state))
                {
                    warn!(%error, "periodic redb metadata durability barrier failed");
                }
            }
        })
        .map_err(|error| {
            MetaError::Storage(format!("failed to spawn redb durability worker: {error}"))
        })?;
    Ok(Arc::new(RelaxedDurabilityRuntime {
        state,
        worker: Some(worker),
    }))
}

fn begin_redb_read_txn(db: &dyn ReadableDatabase) -> MetaResult<ReadTransaction> {
    db.begin_read()
        .map_err(|e| MetaError::Storage(e.to_string()))
}

#[cfg(test)]
impl CounterWriteTransaction<'_> {
    pub(crate) fn set(&self, name: &str, value: u64) -> MetaResult<()> {
        let mut table = self
            .txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .insert(name, value)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }
}

impl Deref for RedbWriteTransaction<'_> {
    type Target = WriteTransaction;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl RedbWriteTransaction<'_> {
    pub(crate) fn invalidate(&self, intent: InvalidationIntent) {
        self.invalidations.borrow_mut().push(intent);
    }

    pub(crate) fn commit(self) -> Result<(), redb::CommitError> {
        let Self {
            txn,
            _db,
            write_metrics,
            mutation_started,
            relaxed_durability,
            _write_txn_guard,
            invalidation_journal,
            invalidation_notify,
            invalidations,
        } = self;
        let commit_started = Instant::now();
        MetadataWriteMetrics::observe(&write_metrics.mutation, commit_started - mutation_started);
        let result = txn.commit();
        if result.is_ok()
            && let Some(runtime) = relaxed_durability
            && !runtime.state.force_immediate.load(Ordering::Acquire)
        {
            runtime.state.pending.store(true, Ordering::Release);
        }
        MetadataWriteMetrics::observe(&write_metrics.commit, commit_started.elapsed());
        // Retain the writer guard through publication so generations have the
        // same order as successful commits, including concurrent replay callers.
        if result.is_ok()
            && invalidation_journal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .publish(invalidations.into_inner())
        {
            invalidation_notify.notify_one();
        }
        drop(_write_txn_guard);
        result
    }

    pub(crate) fn abort(self) -> Result<(), redb::StorageError> {
        let Self {
            txn,
            _db,
            write_metrics,
            mutation_started,
            ..
        } = self;
        // Abort latency itself is intentionally excluded; mutation ends when abort starts,
        // matching commit accounting before the durable commit call.
        MetadataWriteMetrics::observe(&write_metrics.mutation, mutation_started.elapsed());
        txn.abort()
    }
}

impl RedbMetaEngine {
    pub(crate) fn update_counters_in_txn(
        txn: &WriteTransaction,
        positions: &[(&str, Option<u64>)],
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for &(name, value) in positions {
            let Some(value) = value else {
                continue;
            };
            let stored = table
                .get(name)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let stored_value = stored.map(|stored| stored.value());
            let should_insert = stored_value.is_none_or(|stored| value > stored);
            if should_insert {
                table
                    .insert(name, value)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        Ok(())
    }

    pub(crate) fn seal_relaxed_durability_for_close(&self) {
        if let Some(runtime) = &self.relaxed_durability {
            runtime.state.force_immediate.store(true, Ordering::Release);
        }
    }

    pub(crate) fn apply_redb_repair_recovery(
        &self,
        events: &[crate::events::MetaEvent],
        obsolete_recovery_inodes: &[crate::types::Ino],
    ) -> MetaResult<()> {
        if events.is_empty() && obsolete_recovery_inodes.is_empty() {
            return Ok(());
        }

        let txn = self.begin_immediate_write_txn()?;
        if !events.is_empty() {
            Self::apply_replica_serving_state_in_txn(&txn, events, false)?;
            Self::rebuild_inode_lifecycle_table_in_txn(&txn)?;
        }
        for ino in obsolete_recovery_inodes {
            Self::remove_pending_regular_file_recovery_in_txn(&txn, *ino)?;
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

    pub(crate) fn begin_read_txn(&self) -> MetaResult<ReadTransaction> {
        if let Some(db) = self.db.as_ref() {
            let db = db
                .read()
                .map_err(|_| MetaError::Storage("redb lock poisoned".to_string()))?;
            return begin_redb_read_txn(&*db);
        }

        self.read_only_db
            .as_ref()
            .ok_or_else(|| MetaError::Storage("redb database handle is unavailable".to_string()))?
            .begin_read()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

    pub(crate) fn begin_serving_checkpoint_snapshot(
        &self,
    ) -> MetaResult<ServingCheckpointReadSnapshot> {
        Ok(ServingCheckpointReadSnapshot {
            transaction: self.begin_read_txn()?,
        })
    }

    pub(crate) fn begin_write_txn(&self) -> MetaResult<RedbWriteTransaction<'_>> {
        let write_txn_guard = self
            .write_txn_lock
            .lock()
            .map_err(|_| MetaError::Storage("redb write lock poisoned".to_string()))?;
        self.begin_write_txn_with_guard(write_txn_guard)
    }

    fn try_begin_write_txn(&self) -> MetaResult<Option<RedbWriteTransaction<'_>>> {
        let write_txn_guard = match self.write_txn_lock.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(MetaError::Storage("redb write lock poisoned".to_string()));
            }
        };
        self.begin_write_txn_with_guard(write_txn_guard).map(Some)
    }

    fn begin_write_txn_with_guard<'a>(
        &'a self,
        write_txn_guard: MutexGuard<'a, ()>,
    ) -> MetaResult<RedbWriteTransaction<'a>> {
        let db = self.db.as_ref().ok_or_else(|| MetaError::AccessDenied {
            ino: ROOT_INO,
            principal: "snapshot-read-only".to_string(),
            required: AccessLevel::ReadWrite,
            actual: AccessLevel::ReadOnly,
        })?;
        let db = db
            .read()
            .map_err(|_| MetaError::Storage("redb lock poisoned".to_string()))?;
        let started = Instant::now();
        let result = begin_redb_write_txn(&db, self.relaxed_durability.as_deref());
        MetadataWriteMetrics::observe(&self.write_metrics.begin_write, started.elapsed());
        let txn = result?;
        Ok(RedbWriteTransaction {
            txn,
            _db: db,
            _write_txn_guard: write_txn_guard,
            write_metrics: &self.write_metrics,
            mutation_started: Instant::now(),
            relaxed_durability: self.relaxed_durability.as_deref(),
            invalidation_journal: &self.invalidation_journal,
            invalidation_notify: &self.invalidation_notify,
            invalidations: RefCell::default(),
        })
    }

    pub(crate) fn begin_immediate_write_txn(&self) -> MetaResult<RedbWriteTransaction<'_>> {
        let write_txn_guard = self
            .write_txn_lock
            .lock()
            .map_err(|_| MetaError::Storage("redb write lock poisoned".to_string()))?;
        let db = self.db.as_ref().ok_or_else(|| MetaError::AccessDenied {
            ino: ROOT_INO,
            principal: "snapshot-read-only".to_string(),
            required: AccessLevel::ReadWrite,
            actual: AccessLevel::ReadOnly,
        })?;
        let db = db
            .read()
            .map_err(|_| MetaError::Storage("redb lock poisoned".to_string()))?;
        let started = Instant::now();
        let result = begin_immediate_redb_write_txn(&db);
        MetadataWriteMetrics::observe(&self.write_metrics.begin_write, started.elapsed());
        let txn = result?;
        Ok(RedbWriteTransaction {
            txn,
            _db: db,
            _write_txn_guard: write_txn_guard,
            write_metrics: &self.write_metrics,
            mutation_started: Instant::now(),
            relaxed_durability: None,
            invalidation_journal: &self.invalidation_journal,
            invalidation_notify: &self.invalidation_notify,
            invalidations: RefCell::default(),
        })
    }

    /// Open or create a metadata database at the given path.
    pub fn open(path: impl AsRef<Path>) -> MetaResult<Self> {
        Self::open_with_relaxed_interval(
            path,
            Duration::from_millis(REDB_VOLATILE_DURABILITY_BARRIER_INTERVAL_MS),
        )
    }

    fn open_with_relaxed_interval(
        path: impl AsRef<Path>,
        durability_interval: Duration,
    ) -> MetaResult<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                MetaError::Storage(format!("failed to create directory for redb: {e}"))
            })?;
        }

        let db = Database::create(path.as_ref())
            .map_err(|e| MetaError::Storage(format!("failed to open redb: {e}")))?;

        let persisted_format = {
            let txn = db
                .begin_read()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            match Self::read_format_info_from_read_txn(&txn) {
                Ok(format_info) => Some(format_info),
                Err(MetaError::NotFormatted) => None,
                Err(error) => return Err(error),
            }
        };

        {
            let txn = begin_redb_write_txn(&db, None)?;
            let _ = txn.open_table(SETTINGS_TABLE);
            let _ = txn.open_table(COUNTERS_TABLE);
            let _ = txn.open_table(LOCAL_ALLOCATOR_STATE_TABLE);
            let _ = txn.open_table(INODES_TABLE);
            let _ = txn.open_table(BASE_INODES_TABLE);
            let _ = txn.open_table(INODE_LIFECYCLE_TABLE);
            let _ = txn.open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE);
            let _ = txn.open_table(INODE_TOMBSTONES_TABLE);
            let _ = txn.open_table(DENTRIES_TABLE);
            let _ = txn.open_table(DENTRIES_BY_INODE_TABLE);
            let _ = txn.open_table(BASE_DENTRIES_TABLE);
            let _ = txn.open_table(BASE_DENTRIES_BY_INODE_TABLE);
            let _ = txn.open_table(DENTRY_TOMBSTONES_TABLE);
            let _ = txn.open_table(REGULAR_FILE_OVERLAY_TABLE);
            let _ = txn.open_table(REGULAR_FILE_BASE_TABLE);
            let _ = txn.open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE);
            let _ = txn.open_table(PENDING_DELETIONS_TABLE);
            let _ = txn.open_table(PENDING_CREATES_TABLE);
            let _ = txn.open_table(DEFERRED_DELETES_TABLE);
            let _ = txn.open_table(UNDO_LOG_TABLE);
            let _ = txn.open_table(SYMLINKS_TABLE);
            let _ = txn.open_table(BASE_SYMLINKS_TABLE);
            let _ = txn.open_table(SYMLINK_TOMBSTONES_TABLE);
            let _ = txn.open_table(XATTRS_TABLE);
            let _ = txn.open_table(BASE_XATTRS_TABLE);
            let _ = txn.open_table(XATTR_TOMBSTONES_TABLE);
            let _ = txn.open_table(ACL_RULES_TABLE);
            let _ = txn.open_table(BASE_ACL_RULES_TABLE);
            let _ = txn.open_table(ACL_RULE_TOMBSTONES_TABLE);
            let _ = txn.open_table(EVENT_LOG_TABLE);
            let _ = txn.open_table(LOCAL_UPLOAD_LEDGER_TABLE);
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        debug!("opened metadata database at {:?}", path.as_ref());
        let db = Arc::new(RwLock::new(db));
        let write_txn_lock = Arc::new(Mutex::new(()));
        let relaxed_durability = Some(start_relaxed_durability_runtime_with_interval(
            &db,
            Arc::clone(&write_txn_lock),
            durability_interval,
        )?);
        VOLATILE_DURABILITY_WARN.call_once(|| {
            debug!(
                interval_ms = durability_interval.as_millis() as u64,
                "volatile redb metadata durability is enabled with bounded periodic and explicit barriers"
            );
        });
        let engine = Self {
            db: Some(db),
            read_only_db: None,
            metadata_path: path.as_ref().to_path_buf(),
            local_storage_compaction: Mutex::default(),
            local_event_log_prune: Mutex::default(),
            local_storage_page_stats: Mutex::default(),
            local_storage_page_stats_refresh: Mutex::default(),
            write_metrics: Arc::new(MetadataWriteMetrics::default()),
            relaxed_durability,
            write_txn_lock,
            invalidation_journal: Mutex::default(),
            invalidation_notify: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            allocator_state_write_hook: Mutex::new(None),
        };
        engine.ensure_dentry_by_inode_indexes()?;
        let _ = engine.recover_undo_logs()?;

        match persisted_format {
            Some(format_info) => {
                engine.ensure_inode_lifecycle_table()?;
                debug!(
                    "format version {} is up to date",
                    format_info.format_version
                );
            }
            None => {
                debug!("new database (not yet formatted) — skipping migration check");
            }
        }

        Ok(engine)
    }

    /// Open an existing metadata database through redb's read-only backend.
    ///
    /// Unlike [`Self::open`], this path never creates tables, repairs rows, or
    /// starts a durability worker. It is intended for immutable snapshot
    /// artifacts only.
    pub fn open_read_only(path: impl AsRef<Path>) -> MetaResult<Self> {
        let db = ReadOnlyDatabase::open(path.as_ref()).map_err(|error| {
            MetaError::Storage(format!("failed to open redb read-only: {error}"))
        })?;
        Ok(Self {
            db: None,
            read_only_db: Some(Arc::new(db)),
            metadata_path: path.as_ref().to_path_buf(),
            local_storage_compaction: Mutex::default(),
            local_event_log_prune: Mutex::default(),
            local_storage_page_stats: Mutex::default(),
            local_storage_page_stats_refresh: Mutex::default(),
            write_metrics: Arc::new(MetadataWriteMetrics::default()),
            relaxed_durability: None,
            write_txn_lock: Arc::new(Mutex::new(())),
            invalidation_journal: Mutex::default(),
            invalidation_notify: Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            allocator_state_write_hook: Mutex::new(None),
        })
    }

    /// Initialize a fresh filesystem with root inode.
    /// `uid` and `gid` are used as the owner of the root directory.
    pub fn init_root(&self, uid: u32, gid: u32) -> MetaResult<()> {
        if self.get_attr(ROOT_INO).is_ok() {
            debug!("root inode already exists, skipping init");
            return Ok(());
        }

        self.set_counter(counters::NEXT_INODE, ROOT_INO + 1)?;
        self.set_counter(counters::TOTAL_INODES, 1)?;
        self.set_counter(counters::USED_SPACE, 0)?;

        let root = InodeAttr::new_dir(ROOT_INO, ROOT_INO, uid, gid, 0o755);
        self.set_attr(&root)?;
        debug!("initialized root inode with uid={}, gid={}", uid, gid);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn write_counters<T, F>(&self, f: F) -> MetaResult<T>
    where
        F: FnOnce(&CounterWriteTransaction<'_>) -> MetaResult<T>,
    {
        let txn = self.begin_write_txn()?;
        let counters = CounterWriteTransaction { txn: &txn };
        let out = f(&counters)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(out)
    }

    pub(crate) fn flush_local_metadata(&self) -> MetaResult<()> {
        let Some(runtime) = self.relaxed_durability.as_deref() else {
            return Ok(());
        };
        if runtime.state.pending.load(Ordering::Acquire)
            && relaxed_barrier_due(&runtime.state)
            && let Some(db) = self.db.as_ref()
        {
            immediate_metadata_barrier(db, &self.write_txn_lock, Some(&runtime.state))?;
        }
        Ok(())
    }

    pub(crate) fn sync_local_metadata(&self) -> MetaResult<()> {
        let Some(db) = self.db.as_ref() else {
            return Ok(());
        };
        immediate_metadata_barrier(
            db,
            &self.write_txn_lock,
            self.relaxed_durability
                .as_deref()
                .map(|runtime| runtime.state.as_ref()),
        )
    }

    #[cfg(test)]
    fn open_relaxed_for_test(path: impl AsRef<Path>) -> Self {
        Self::open_relaxed_with_interval_for_test(
            path,
            Duration::from_millis(REDB_VOLATILE_DURABILITY_BARRIER_INTERVAL_MS),
        )
    }

    #[cfg(test)]
    fn open_relaxed_with_interval_for_test(path: impl AsRef<Path>, interval: Duration) -> Self {
        Self::open_with_relaxed_interval(path, interval).expect("open relaxed redb test engine")
    }

    #[cfg(test)]
    fn relaxed_pending_for_test(&self) -> bool {
        self.relaxed_durability
            .as_ref()
            .is_some_and(|runtime| runtime.state.pending.load(Ordering::Acquire))
    }

    #[cfg(test)]
    fn immediate_barriers_for_test(&self) -> u64 {
        self.relaxed_durability.as_ref().map_or(0, |runtime| {
            runtime.state.immediate_barriers.load(Ordering::Relaxed)
        })
    }

    #[cfg(test)]
    fn make_relaxed_barrier_due_for_test(&self) {
        if let Some(runtime) = &self.relaxed_durability {
            runtime.state.next_barrier_ms.store(0, Ordering::Release);
        }
    }

    #[cfg(test)]
    fn fail_next_barrier_for_test(&self) {
        if let Some(runtime) = &self.relaxed_durability {
            runtime
                .state
                .fail_next_barrier
                .store(true, Ordering::Release);
        }
    }

    #[cfg(test)]
    fn pause_after_barrier_commit_for_test(
        &self,
        reached: std::sync::mpsc::SyncSender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) {
        if let Some(runtime) = &self.relaxed_durability {
            *runtime
                .state
                .after_barrier_commit
                .lock()
                .expect("barrier hook lock") = Some((reached, resume));
        }
    }
}

mod acl;
mod allocator_state;
mod chunk_state;
mod format;
mod gc;
mod inodes;
pub(crate) use inodes::PendingRecoveryDentry;
mod namespace;
mod payload_objects;
mod recovery;
mod replication;
mod snapshots;
mod symlink;
mod txn;
mod upload_ledger;
mod xattr;

impl MetaEngineCore for RedbMetaEngine {
    fn metadata_write_observability(&self) -> MetadataWriteObservability {
        self.write_metrics.snapshot()
    }

    fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool> {
        Ok(true)
    }

    fn release_global_janitor_lock(&self) -> MetaResult<()> {
        Ok(())
    }

    fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        Ok(HashSet::new())
    }

    fn acquire_file_lock_lease(
        &self,
        _ino: Ino,
        _kind: HardLockKind,
        _lease_ttl: std::time::Duration,
    ) -> MetaResult<FileLockLeaseAcquireOutcome> {
        Err(MetaError::Storage(
            "file lock leases require postgres metadata backend".to_string(),
        ))
    }

    fn release_file_lock_lease(&self, _ino: Ino) -> MetaResult<FileLockLeaseReleaseOutcome> {
        Err(MetaError::Storage(
            "file lock leases require postgres metadata backend".to_string(),
        ))
    }

    fn renew_file_lock_lease(
        &self,
        _ino: Ino,
        _lease_ttl: std::time::Duration,
    ) -> MetaResult<FileLockLeaseRenewOutcome> {
        Err(MetaError::Storage(
            "file lock leases require postgres metadata backend".to_string(),
        ))
    }

    fn current_file_lock_lease(&self, _ino: Ino) -> MetaResult<Option<FileLockLease>> {
        Ok(None)
    }

    fn known_file_lock_lease(&self, _ino: Ino) -> Option<FileLockLease> {
        None
    }
}

impl UploadedFragmentLedger for RedbMetaEngine {
    fn fragment_ledger_volume_id(&self) -> MetaResult<crate::snapshot::VolumeId> {
        // redb-only mounts have no shared PostgreSQL volume identity.
        Ok(0)
    }

    fn claim_uploaded_fragment_reconciliation(
        &self,
        _owner: &str,
    ) -> MetaResult<Vec<crate::snapshot_ports::UploadedFragmentReconciliationClaim>> {
        Ok(Vec::new())
    }

    fn finish_uploaded_fragment_reconciliation(
        &self,
        _owner: &str,
        _claim: &crate::snapshot_ports::UploadedFragmentReconciliationClaim,
        _outcome: crate::snapshot_ports::UploadedFragmentReconciliationOutcome,
    ) -> MetaResult<()> {
        Ok(())
    }

    fn claim_uploaded_object_deletion(
        &self,
        _object_id: u64,
        _owner: &str,
    ) -> MetaResult<crate::snapshot_ports::UploadedObjectDeletionClaim> {
        Ok(crate::snapshot_ports::UploadedObjectDeletionClaim::Untracked)
    }

    fn finish_uploaded_object_deletion(
        &self,
        _object_id: u64,
        _owner: &str,
        _fencing_token: i64,
        _outcome: crate::snapshot_ports::UploadedObjectDeletionOutcome,
    ) -> MetaResult<()> {
        Ok(())
    }

    fn begin_fragment_upload(&self, _entry: &UploadedFragmentLedgerEntry) -> MetaResult<()> {
        // CONTEXT: redb-only mounts have no shared PostgreSQL authority; the
        // existing local upload lifecycle remains their source of truth.
        Ok(())
    }

    fn confirm_fragment_uploaded(&self, _entry: &UploadedFragmentLedgerEntry) -> MetaResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

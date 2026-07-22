//! redb implementation of MetaEngine.
//!
//! Uses redb as an embedded ACID KV store for all filesystem metadata.

use crate::engine::*;
use crate::types::*;
use redb::{
    Database, Durability, ReadOnlyDatabase, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, WriteTransaction,
};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::ErrorKind;
use std::ops::Deref;
use std::path::Path;
use std::sync::{
    Arc, Condvar, Mutex, Once, RwLock, RwLockReadGuard, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

pub(crate) mod layout;
pub use self::layout::RedbSnapshot;
use self::layout::*;

mod fs_ops;

static RELAXED_DURABILITY_WARN: Once = Once::new();
const REDB_RELAXED_DURABILITY_BARRIER_INTERVAL_MS: u64 = 5_000;

/// Metadata engine backed by redb.
pub struct RedbMetaEngine {
    db: Arc<RwLock<Database>>,
    write_metrics: Arc<MetadataWriteMetrics>,
    relaxed_durability: Option<Arc<RelaxedDurabilityRuntime>>,
}

struct RelaxedDurabilityState {
    started: Instant,
    interval_ms: u64,
    next_barrier_ms: AtomicU64,
    pending: AtomicBool,
    stopping: AtomicBool,
    wake: (Mutex<()>, Condvar),
    immediate_barriers: AtomicU64,
    #[cfg(test)]
    fail_next_barrier: AtomicBool,
}

struct RelaxedDurabilityRuntime {
    state: Arc<RelaxedDurabilityState>,
    worker: Option<JoinHandle<()>>,
}

impl Drop for RelaxedDurabilityRuntime {
    fn drop(&mut self) {
        self.state.stopping.store(true, Ordering::Release);
        self.state.wake.1.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
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
}

impl MetadataWriteMetrics {
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
        }
    }
}

pub(crate) struct CounterWriteTransaction<'a> {
    txn: &'a WriteTransaction,
}

pub(crate) struct RedbWriteTransaction<'a> {
    txn: WriteTransaction,
    _db: RwLockReadGuard<'a, Database>,
    write_metrics: Arc<MetadataWriteMetrics>,
    mutation_started: Instant,
    relaxed_durability: Option<Arc<RelaxedDurabilityRuntime>>,
}

fn redb_relaxed_durability_enabled() -> bool {
    std::env::var("STUDIOFS_REDB_RELAXED_DURABILITY")
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

fn begin_redb_write_txn(
    db: &Database,
    relaxed_durability: Option<&Arc<RelaxedDurabilityRuntime>>,
) -> MetaResult<WriteTransaction> {
    let mut txn = db
        .begin_write()
        .map_err(|e| MetaError::Storage(e.to_string()))?;
    if relaxed_durability.is_some() {
        txn.set_durability(Durability::None)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
    }
    Ok(txn)
}

fn immediate_metadata_barrier(
    db: &RwLock<Database>,
    relaxed_durability: Option<&RelaxedDurabilityState>,
) -> MetaResult<()> {
    #[cfg(test)]
    if relaxed_durability.is_some_and(|state| state.fail_next_barrier.swap(false, Ordering::AcqRel))
    {
        return Err(MetaError::Storage(
            "simulated metadata barrier failure".to_string(),
        ));
    }
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
    if let Some(state) = relaxed_durability {
        state.pending.store(false, Ordering::Release);
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

fn start_relaxed_durability_runtime_with_interval(
    db: &Arc<RwLock<Database>>,
    interval: Duration,
) -> Arc<RelaxedDurabilityRuntime> {
    let interval_ms = interval.as_millis().min(u128::from(u64::MAX)) as u64;
    let state = Arc::new(RelaxedDurabilityState {
        started: Instant::now(),
        interval_ms,
        next_barrier_ms: AtomicU64::new(interval_ms),
        pending: AtomicBool::new(false),
        stopping: AtomicBool::new(false),
        wake: (Mutex::new(()), Condvar::new()),
        immediate_barriers: AtomicU64::new(0),
        #[cfg(test)]
        fail_next_barrier: AtomicBool::new(false),
    });
    let worker_state = Arc::clone(&state);
    let weak_db: Weak<RwLock<Database>> = Arc::downgrade(db);
    let worker = std::thread::Builder::new()
        .name("studiofs-redb-durability".to_string())
        .spawn(move || {
            while !worker_state.stopping.load(Ordering::Acquire) {
                let guard = worker_state
                    .wake
                    .0
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let _ = worker_state.wake.1.wait_timeout(guard, interval);
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
                if let Err(error) = immediate_metadata_barrier(&db, Some(&worker_state)) {
                    warn!(%error, "periodic redb metadata durability barrier failed");
                }
            }
        })
        .expect("failed to spawn redb durability worker");
    Arc::new(RelaxedDurabilityRuntime {
        state,
        worker: Some(worker),
    })
}

fn begin_redb_read_txn(db: &Database) -> MetaResult<ReadTransaction> {
    db.begin_read()
        .map_err(|e| MetaError::Storage(e.to_string()))
}

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
    pub(crate) fn commit(self) -> Result<(), redb::CommitError> {
        let Self {
            txn,
            _db,
            write_metrics,
            mutation_started,
            relaxed_durability,
        } = self;
        let commit_started = Instant::now();
        MetadataWriteMetrics::observe(&write_metrics.mutation, commit_started - mutation_started);
        let result = txn.commit();
        if result.is_ok()
            && let Some(runtime) = relaxed_durability
        {
            runtime.state.pending.store(true, Ordering::Release);
        }
        MetadataWriteMetrics::observe(&write_metrics.commit, commit_started.elapsed());
        result
    }

    pub(crate) fn abort(self) -> Result<(), redb::StorageError> {
        let Self {
            txn,
            _db,
            write_metrics,
            mutation_started,
            relaxed_durability: _,
        } = self;
        // Abort latency itself is intentionally excluded; mutation ends when abort starts,
        // matching commit accounting before the durable commit call.
        MetadataWriteMetrics::observe(&write_metrics.mutation, mutation_started.elapsed());
        txn.abort()
    }
}

impl RedbMetaEngine {
    pub(crate) fn read_db(&self) -> MetaResult<RwLockReadGuard<'_, Database>> {
        self.db
            .read()
            .map_err(|_| MetaError::Storage("redb lock poisoned".to_string()))
    }

    pub(crate) fn begin_read_txn(&self) -> MetaResult<ReadTransaction> {
        let db = self.read_db()?;
        begin_redb_read_txn(&db)
    }

    pub(crate) fn begin_write_txn(&self) -> MetaResult<RedbWriteTransaction<'_>> {
        let db = self.read_db()?;
        let started = Instant::now();
        let result = begin_redb_write_txn(&db, self.relaxed_durability.as_ref());
        MetadataWriteMetrics::observe(&self.write_metrics.begin_write, started.elapsed());
        let txn = result?;
        Ok(RedbWriteTransaction {
            txn,
            _db: db,
            write_metrics: Arc::clone(&self.write_metrics),
            mutation_started: Instant::now(),
            relaxed_durability: self.relaxed_durability.clone(),
        })
    }

    /// Open or create a metadata database at the given path.
    pub fn open(path: impl AsRef<Path>) -> MetaResult<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                MetaError::Storage(format!("failed to create directory for redb: {e}"))
            })?;
        }

        let db = Database::create(path.as_ref())
            .map_err(|e| MetaError::Storage(format!("failed to open redb: {e}")))?;

        {
            let txn = begin_redb_write_txn(&db, None)?;
            let _ = txn.open_table(SETTINGS_TABLE);
            let _ = txn.open_table(COUNTERS_TABLE);
            let _ = txn.open_table(INODES_TABLE);
            let _ = txn.open_table(BASE_INODES_TABLE);
            let _ = txn.open_table(INODE_LIFECYCLE_TABLE);
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
            let _ = txn.open_table(SNAPSHOTS_TABLE);
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        debug!("opened metadata database at {:?}", path.as_ref());
        let db = Arc::new(RwLock::new(db));
        let relaxed_durability = redb_relaxed_durability_enabled().then(|| {
            start_relaxed_durability_runtime_with_interval(
                &db,
                Duration::from_millis(REDB_RELAXED_DURABILITY_BARRIER_INTERVAL_MS),
            )
        });
        if relaxed_durability.is_some() {
            RELAXED_DURABILITY_WARN.call_once(|| {
                warn!(
                    interval_ms = REDB_RELAXED_DURABILITY_BARRIER_INTERVAL_MS,
                    "relaxed redb metadata durability is enabled with bounded periodic and explicit barriers"
                );
            });
        }
        let engine = Self {
            db,
            write_metrics: Arc::new(MetadataWriteMetrics::default()),
            relaxed_durability,
        };
        engine.ensure_dentry_by_inode_indexes()?;
        let _ = engine.recover_undo_logs()?;

        match engine.get_format_info() {
            Ok(format_info) if format_info.format_version == CURRENT_FORMAT_VERSION => {
                engine.ensure_inode_lifecycle_table()?;
                debug!(
                    "format version {} is up to date",
                    format_info.format_version
                );
            }
            Ok(info) => {
                return Err(MetaError::Storage(format!(
                    "unsupported format version: {}",
                    info.format_version
                )));
            }
            Err(MetaError::NotFormatted) => {
                debug!("new database (not yet formatted) — skipping migration check");
            }
            Err(e) => return Err(e),
        }

        Ok(engine)
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
        if runtime.state.pending.load(Ordering::Acquire) && relaxed_barrier_due(&runtime.state) {
            immediate_metadata_barrier(&self.db, Some(&runtime.state))?;
        }
        Ok(())
    }

    pub(crate) fn sync_local_metadata(&self) -> MetaResult<()> {
        immediate_metadata_barrier(
            &self.db,
            self.relaxed_durability
                .as_deref()
                .map(|runtime| runtime.state.as_ref()),
        )
    }

    #[cfg(test)]
    fn open_relaxed_for_test(path: impl AsRef<Path>) -> Self {
        Self::open_relaxed_with_interval_for_test(
            path,
            Duration::from_millis(REDB_RELAXED_DURABILITY_BARRIER_INTERVAL_MS),
        )
    }

    #[cfg(test)]
    fn open_relaxed_with_interval_for_test(path: impl AsRef<Path>, interval: Duration) -> Self {
        let mut engine = Self::open(path).expect("open relaxed redb test engine");
        if engine.relaxed_durability.is_none() {
            engine.relaxed_durability = Some(start_relaxed_durability_runtime_with_interval(
                &engine.db, interval,
            ));
        }
        engine
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
}

mod acl;
mod chunk_state;
mod format;
mod gc;
mod inodes;
mod namespace;
mod payload_objects;
mod recovery;
mod replication;
mod snapshots;
mod symlink;
mod txn;
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

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

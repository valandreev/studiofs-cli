//! Current persistent local payload object runtime.
//!
//! This module defines:
//! - the non-mutating detection contract for the current layout state;
//! - the active local payload backend backed by a file-per-object
//!   payload store plus redb lifecycle index.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use tracing::{debug, warn};

use crate::local::{
    LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState, LocalBlockObjectStats,
    LocalBlockOrigin, LocalBlockPut, LocalBlockStorageStats, LocalBlockUploadState,
};

#[path = "block_index.rs"]
pub mod cache_index;
#[path = "block_eviction.rs"]
pub mod eviction;
#[path = "block_store.rs"]
pub mod file_payload_store;
#[path = "block_layout.rs"]
pub mod layout;
#[path = "block_platform.rs"]
pub mod platform;
#[path = "startup_recovery.rs"]
pub mod reconcile;

pub use cache_index::{
    CacheIndex, CacheIndexEntry, CacheIndexError, CacheIndexStats, CacheIndexUpsert,
    DirtyReplayEntry, EvictionCandidateEntry,
};
use eviction::{CacheBudgetGovernor, EVICTION_CANDIDATE_PAGE_SIZE, EVICTION_DELETE_BATCH_SIZE};
pub use file_payload_store::{FilePayloadStore, FilePayloadStoreError, PayloadBatchEntry};
pub use layout::{LOCAL_BLOCK_PAYLOAD_SHARD_COUNT, LOCAL_BLOCK_REDB_INDEX_FILE, LocalBlockLayout};
pub use reconcile::{LocalBlockStartupReconcileError, LocalBlockStartupReconcileReport};
use reconcile::{LocalBlockStartupReconcileMode, reconcile_startup_state};

const ACCESS_TOUCH_MIN_INTERVAL: Duration = Duration::from_secs(60);
const ACCESS_TOUCH_BATCH_SIZE: usize = 128;
#[cfg(not(test))]
const ACCESS_TOUCH_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(test)]
const ACCESS_TOUCH_FLUSH_INTERVAL: Duration = Duration::ZERO;
const ACCESS_TOUCH_RETRY_BASE_BACKOFF_MS: u64 = 10;
const ACCESS_TOUCH_RETRY_MAX_EXPONENT: u8 = 8;
const DIRTY_ORPHAN_PRUNE_MIN_INTERVAL: Duration = Duration::from_secs(30);
const LOW_SPACE_UPLOAD_DRAIN_WAIT: Duration = Duration::from_millis(25);
const LOW_SPACE_UPLOAD_DRAIN_RETRIES: usize = 4;

/// Backend kind available on the runtime path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalBlockBackendKind {
    Current,
}

impl LocalBlockBackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "local-block-runtime",
        }
    }
}

impl fmt::Display for LocalBlockBackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Detection snapshot for a per-volume cache root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalBlockRuntimeDetection {
    pub layout: LocalBlockLayout,
    pub index_exists: bool,
    pub payload_objects_dir_exists: bool,
    pub upload_objects_dir_exists: bool,
    pub tmp_dir_exists: bool,
    pub index_dir_exists: bool,
}

impl LocalBlockRuntimeDetection {
    pub fn has_runtime_layout(&self) -> bool {
        self.index_exists
            || self.payload_objects_dir_exists
            || self.upload_objects_dir_exists
            || self.tmp_dir_exists
            || self.index_dir_exists
    }

    pub const fn has_complete_runtime_layout(&self) -> bool {
        !self.needs_runtime_provisioning()
    }

    pub const fn needs_runtime_provisioning(&self) -> bool {
        !self.payload_objects_dir_exists
            || !self.upload_objects_dir_exists
            || !self.tmp_dir_exists
            || !self.index_dir_exists
    }

    pub fn runtime_backend(&self) -> Option<LocalBlockBackendKind> {
        if !self.has_complete_runtime_layout() {
            None
        } else {
            Some(LocalBlockBackendKind::Current)
        }
    }
}

/// Detect backend contracts for a per-volume cache root.
pub fn detect_cache_backends(cache_root: &Path) -> io::Result<LocalBlockRuntimeDetection> {
    let layout = LocalBlockLayout::from_cache_root(cache_root);
    let index_exists = layout.redb_index_path.try_exists()?;
    let payload_objects_dir_exists = layout.payload_objects_dir.try_exists()?;
    let upload_objects_dir_exists = layout.upload_stage_objects_dir.try_exists()?;
    let tmp_dir_exists = layout.tmp_dir.try_exists()?;
    let index_dir_exists = layout.index_dir.try_exists()?;
    Ok(LocalBlockRuntimeDetection {
        layout,
        index_exists,
        payload_objects_dir_exists,
        upload_objects_dir_exists,
        tmp_dir_exists,
        index_dir_exists,
    })
}

/// Configuration for opening the current local payload object backend.
#[derive(Debug, Clone)]
pub struct LocalBlockBackendConfig {
    pub cache_root: PathBuf,
    pub volume_uuid: [u8; 16],
}

/// Payload-store sync cadence policy for the current local payload object backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LocalBlockPayloadSyncMode {
    /// Keep per-write file+directory barriers at payload commit time.
    #[default]
    Durable,
    /// Defer payload-store barriers to grouped `sync_data()` calls.
    Fast,
}

/// Errors returned while opening the current local payload object backend.
#[derive(Debug, thiserror::Error)]
pub enum LocalBlockBackendOpenError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("local payload object runtime layout under '{cache_root}' is incomplete")]
    IncompleteRuntimeLayout { cache_root: PathBuf },
    #[error("failed to open local block index at '{path}': {source}")]
    OpenCacheIndex {
        path: PathBuf,
        #[source]
        source: CacheIndexError,
    },
    #[error("failed to open local block payload store at '{cache_root}': {source}")]
    OpenPayloadStore {
        cache_root: PathBuf,
        #[source]
        source: FilePayloadStoreError,
    },
    #[error("failed to reconcile local block startup state under '{cache_root}': {source}")]
    StartupReconcile {
        cache_root: PathBuf,
        #[source]
        source: LocalBlockStartupReconcileError,
    },
}

impl LocalBlockBackendOpenError {
    pub fn is_transient_open_lock_conflict(&self) -> bool {
        match self {
            Self::OpenCacheIndex { source, .. } => source.is_database_lock_conflict(),
            _ => false,
        }
    }
}

/// Current local backend backed by file-per-object payload files and redb index.
#[derive(Debug)]
pub struct LocalBlockBackendRuntime {
    layout: LocalBlockLayout,
    volume_uuid: [u8; 16],
    index: CacheIndex,
    payload_store: FilePayloadStore,
    budget_governor: CacheBudgetGovernor,
    eviction_pass_lock: Mutex<()>,
    runtime_pinned: Mutex<HashMap<u64, usize>>,
    eviction_scan_skipped_non_evictable: AtomicU64,
    pending_access_touches: Mutex<PendingAccessTouches>,
    last_dirty_orphan_prune_at: Mutex<Instant>,
    dirty_validation_in_progress: AtomicBool,
    storage_state_epoch: Mutex<u64>,
    storage_state_changed: Condvar,
}

struct DirtyValidationGuard<'a> {
    backend: &'a LocalBlockBackendRuntime,
}

impl Drop for DirtyValidationGuard<'_> {
    fn drop(&mut self) {
        self.backend
            .dirty_validation_in_progress
            .store(false, Ordering::Release);
    }
}

impl Drop for LocalBlockBackendRuntime {
    fn drop(&mut self) {
        self.flush_access_touches(true, "drop");
    }
}

impl LocalBlockBackendRuntime {
    pub fn open(config: LocalBlockBackendConfig) -> Result<Self, LocalBlockBackendOpenError> {
        Self::open_with_budget_and_sync_mode(config, None, LocalBlockPayloadSyncMode::Durable)
    }

    pub fn open_with_budget(
        config: LocalBlockBackendConfig,
        cache_budget_bytes: Option<u64>,
    ) -> Result<Self, LocalBlockBackendOpenError> {
        Self::open_with_budget_and_sync_mode(
            config,
            cache_budget_bytes,
            LocalBlockPayloadSyncMode::Durable,
        )
    }

    pub fn open_with_budget_and_sync_mode(
        config: LocalBlockBackendConfig,
        cache_budget_bytes: Option<u64>,
        payload_sync_mode: LocalBlockPayloadSyncMode,
    ) -> Result<Self, LocalBlockBackendOpenError> {
        let layout = LocalBlockLayout::from_cache_root(&config.cache_root);
        if !layout.payload_objects_dir.try_exists()?
            || !layout.upload_stage_objects_dir.try_exists()?
            || !layout.tmp_dir.try_exists()?
            || !layout.index_dir.try_exists()?
        {
            return Err(LocalBlockBackendOpenError::IncompleteRuntimeLayout {
                cache_root: layout.runtime_root.clone(),
            });
        }

        let index = CacheIndex::open(&layout.redb_index_path).map_err(|source| {
            LocalBlockBackendOpenError::OpenCacheIndex {
                path: layout.redb_index_path.clone(),
                source,
            }
        })?;

        let payload_store =
            FilePayloadStore::open_with_sync_mode(layout.clone(), payload_sync_mode).map_err(
                |source| LocalBlockBackendOpenError::OpenPayloadStore {
                    cache_root: layout.runtime_root.clone(),
                    source,
                },
            )?;

        let startup_reconcile_mode = LocalBlockStartupReconcileMode::from_env();
        let startup_reconcile_report =
            reconcile_startup_state(&layout, &index, startup_reconcile_mode).map_err(|source| {
                LocalBlockBackendOpenError::StartupReconcile {
                    cache_root: layout.runtime_root.clone(),
                    source,
                }
            })?;
        if !startup_reconcile_report.is_clean() {
            warn!(
                mode = ?startup_reconcile_mode,
                temp_files_removed = startup_reconcile_report.temp_files_removed,
                stale_index_entries_removed = startup_reconcile_report.stale_index_entries_removed,
                orphan_payload_files_removed =
                    startup_reconcile_report.orphan_payload_files_removed,
                "local block runtime startup reconciliation repaired cache artifacts"
            );
        }

        Ok(Self {
            layout,
            volume_uuid: config.volume_uuid,
            index,
            payload_store,
            budget_governor: cache_budget_bytes
                .map(CacheBudgetGovernor::bounded)
                .unwrap_or_else(CacheBudgetGovernor::unbounded),
            eviction_pass_lock: Mutex::new(()),
            runtime_pinned: Mutex::new(HashMap::new()),
            eviction_scan_skipped_non_evictable: AtomicU64::new(0),
            pending_access_touches: Mutex::new(PendingAccessTouches::new()),
            last_dirty_orphan_prune_at: Mutex::new(
                Instant::now()
                    .checked_sub(DIRTY_ORPHAN_PRUNE_MIN_INTERVAL)
                    .unwrap_or_else(Instant::now),
            ),
            dirty_validation_in_progress: AtomicBool::new(false),
            storage_state_epoch: Mutex::new(0),
            storage_state_changed: Condvar::new(),
        })
    }

    fn has_index_entry(&self, object_id: u64) -> bool {
        match self.index.get_entry(object_id) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "local block runtime index lookup failed while checking payload presence"
                );
                false
            }
        }
    }

    fn touch_access_if_read(&self, object_id: u64, read_len: usize, op: &'static str) {
        if read_len == 0 {
            return;
        }
        let pending_ids = {
            let mut pending = self.pending_access_touches.lock();
            pending.enqueue(object_id);
            pending.take_ready_for_flush(false, Instant::now())
        };
        if let Some(pending_ids) = pending_ids {
            self.flush_access_touch_ids(pending_ids, op);
        }
    }

    fn remove_orphaned_index_entry(&self, object_id: u64, operation: &'static str) {
        warn!(
            object_id,
            operation,
            "local block runtime detected index entry without payload; pruning stale index entry"
        );
        if let Err(error) = self.index.remove(object_id) {
            warn!(
                object_id,
                operation,
                error = ?error,
                "local block runtime failed to prune stale index entry"
            );
        }
    }

    fn payload_missing(&self, object_id: u64, operation: &'static str) -> bool {
        match self.payload_store.payload_exists(object_id) {
            Ok(exists) => !exists,
            Err(error) => {
                warn!(
                    object_id,
                    operation,
                    error = ?error,
                    "local block runtime payload existence probe failed"
                );
                false
            }
        }
    }

    fn begin_dirty_validation(&self) -> Option<DirtyValidationGuard<'_>> {
        if self
            .dirty_validation_in_progress
            .swap(true, Ordering::AcqRel)
        {
            return None;
        }
        let now = Instant::now();
        let mut last_prune = self.last_dirty_orphan_prune_at.lock();
        if now.duration_since(*last_prune) < DIRTY_ORPHAN_PRUNE_MIN_INTERVAL {
            self.dirty_validation_in_progress
                .store(false, Ordering::Release);
            return None;
        }
        *last_prune = now;
        Some(DirtyValidationGuard { backend: self })
    }

    fn remove_orphaned_index_entries(&self, object_ids: &[u64], operation: &'static str) {
        if object_ids.is_empty() {
            return;
        }
        warn!(
            operation,
            orphan_count = object_ids.len(),
            "local block runtime pruning stale index entries that no longer have payload files"
        );
        if let Err(error) = self.index.remove_batch(object_ids) {
            warn!(
                operation,
                orphan_count = object_ids.len(),
                error = ?error,
                "local block runtime batch prune of stale index entries failed"
            );
        } else {
            self.forget_access_touches(object_ids);
        }
    }

    fn remove_batch_internal(&self, object_ids: &[u64]) -> Result<(), CacheIndexError> {
        self.index.remove_batch(object_ids)?;
        self.forget_access_touches(object_ids);
        self.payload_store.remove_batch(object_ids);
        self.note_storage_state_change();
        Ok(())
    }

    fn note_storage_state_change(&self) {
        let mut epoch = self.storage_state_epoch.lock();
        *epoch = (*epoch).wrapping_add(1);
        self.storage_state_changed.notify_all();
    }

    fn wait_for_storage_state_change(&self, observed_epoch: u64) {
        let mut epoch = self.storage_state_epoch.lock();
        if *epoch != observed_epoch {
            return;
        }
        self.storage_state_changed
            .wait_for(&mut epoch, LOW_SPACE_UPLOAD_DRAIN_WAIT);
    }

    fn retry_non_evictable_put_after_upload_drain(
        &self,
        target_bytes: u64,
        payload_len: u64,
        mut current_after_evict: u64,
    ) -> Result<u64, LocalBlockBackendError> {
        for attempt in 0..LOW_SPACE_UPLOAD_DRAIN_RETRIES {
            if !self
                .budget_governor
                .exceeds_budget_after_insert(current_after_evict, payload_len)
            {
                break;
            }

            let dirty_object_count = self
                .index
                .dirty_object_count()
                .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;
            if dirty_object_count == 0 {
                current_after_evict =
                    self.evict_to_target_bytes(target_bytes, "put_low_space_retry")?;
                break;
            }

            let observed_epoch = *self.storage_state_epoch.lock();
            debug!(
                attempt = attempt + 1,
                current_bytes = current_after_evict,
                target_bytes,
                payload_len,
                dirty_object_count,
                wait_ms = LOW_SPACE_UPLOAD_DRAIN_WAIT.as_millis(),
                "local block runtime low-space put waiting briefly for pending upload drain"
            );
            self.wait_for_storage_state_change(observed_epoch);
            current_after_evict =
                self.evict_to_target_bytes(target_bytes, "put_low_space_retry")?;
        }
        Ok(current_after_evict)
    }

    fn flush_access_touches(&self, force: bool, operation: &'static str) {
        let pending_ids = {
            let mut pending = self.pending_access_touches.lock();
            let Some(pending_ids) = pending.take_ready_for_flush(force, Instant::now()) else {
                return;
            };
            pending_ids
        };
        self.flush_access_touch_ids(pending_ids, operation);
    }

    fn flush_access_touch_ids(&self, pending_ids: Vec<u64>, operation: &'static str) {
        if let Err(error) = self
            .index
            .touch_access_many_unique(&pending_ids, ACCESS_TOUCH_MIN_INTERVAL)
        {
            warn!(
                operation,
                count = pending_ids.len(),
                error = ?error,
                "local block runtime batched access touch failed"
            );
            let mut pending = self.pending_access_touches.lock();
            pending.requeue_after_failure(pending_ids);
        } else {
            self.pending_access_touches.lock().record_flush_success();
        }
    }

    fn forget_access_touches(&self, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        let mut pending = self.pending_access_touches.lock();
        pending.forget_many(object_ids);
    }

    fn evict_to_target_bytes_impl(
        &self,
        target_bytes: u64,
        operation: &'static str,
    ) -> Result<u64, LocalBlockBackendError> {
        self.flush_access_touches(true, operation);
        let stats = self
            .index
            .stats()
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;
        if stats.current_bytes <= target_bytes {
            return Ok(stats.current_bytes);
        }

        let mut cursor = None;
        let mut batch = Vec::with_capacity(EVICTION_DELETE_BATCH_SIZE);
        let mut estimated_current = stats.current_bytes;
        loop {
            if estimated_current <= target_bytes {
                break;
            }

            let page = self
                .index
                .eviction_candidates_page_stats(cursor, EVICTION_CANDIDATE_PAGE_SIZE)
                .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;
            if page.entries.is_empty() {
                break;
            }
            cursor = page.entries.last().map(|entry| entry.access_order);
            let mut skipped_non_evictable = page.skipped_non_evictable;
            for candidate in page.entries {
                if estimated_current <= target_bytes {
                    break;
                }
                if self.is_runtime_pinned(candidate.object_id) {
                    skipped_non_evictable = skipped_non_evictable.saturating_add(1);
                    continue;
                }
                batch.push(candidate.object_id);
                estimated_current = estimated_current.saturating_sub(candidate.payload_len);
                if batch.len() >= EVICTION_DELETE_BATCH_SIZE {
                    self.remove_batch_internal(&batch)
                        .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;
                    batch.clear();
                }
            }
            if skipped_non_evictable > 0 {
                let skipped_total = self
                    .eviction_scan_skipped_non_evictable
                    .fetch_add(skipped_non_evictable, Ordering::Relaxed)
                    .saturating_add(skipped_non_evictable);
                debug!(
                    operation,
                    scanned_rows = page.scanned_rows,
                    skipped_non_evictable,
                    skipped_non_evictable_total = skipped_total,
                    "local block runtime eviction candidate scan skipped non-evictable rows"
                );
            }
            if !batch.is_empty() {
                self.remove_batch_internal(&batch)
                    .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;
                batch.clear();
            }
        }

        let final_current = self
            .index
            .stats()
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?
            .current_bytes;
        if final_current > target_bytes {
            warn!(
                operation,
                current_bytes = final_current,
                target_bytes,
                "local block runtime budget pressure remained above target after deterministic eviction pass"
            );
        }
        Ok(final_current)
    }

    fn evict_to_target_bytes(
        &self,
        target_bytes: u64,
        operation: &'static str,
    ) -> Result<u64, LocalBlockBackendError> {
        let _eviction_guard = self.eviction_pass_lock.lock();
        self.evict_to_target_bytes_impl(target_bytes, operation)
    }

    fn try_evict_to_target_bytes(
        &self,
        target_bytes: u64,
        operation: &'static str,
    ) -> Result<Option<u64>, LocalBlockBackendError> {
        let Some(_eviction_guard) = self.eviction_pass_lock.try_lock() else {
            return Ok(None);
        };
        self.evict_to_target_bytes_impl(target_bytes, operation)
            .map(Some)
    }

    fn enforce_budget_before_put(
        &self,
        object_id: u64,
        payload_len: u64,
        evictable: bool,
    ) -> Result<bool, LocalBlockBackendError> {
        let stats = self
            .index
            .stats()
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;
        let Some(target_bytes) = self
            .budget_governor
            .target_before_insert(stats.current_bytes, payload_len)
        else {
            return Ok(false);
        };

        let mut current_after_evict = self.evict_to_target_bytes(target_bytes, "put")?;
        if !evictable {
            current_after_evict = self.retry_non_evictable_put_after_upload_drain(
                target_bytes,
                payload_len,
                current_after_evict,
            )?;
        }
        if !self
            .budget_governor
            .exceeds_budget_after_insert(current_after_evict, payload_len)
        {
            return Ok(false);
        }

        if evictable {
            warn!(
                object_id,
                payload_len,
                current_bytes = current_after_evict,
                budget_bytes = ?self.budget_governor.budget_bytes(),
                "local block runtime skipped evictable put because budget remained under hard pressure"
            );
            return Ok(true);
        }

        Err(LocalBlockBackendError::CapacityExhausted {
            needed: payload_len,
            free: self.budget_governor.free_bytes(current_after_evict),
        })
    }

    fn is_runtime_pinned(&self, object_id: u64) -> bool {
        self.runtime_pinned.lock().contains_key(&object_id)
    }

    fn runtime_pinned_evictable_bytes(&self) -> u64 {
        let object_ids = self
            .runtime_pinned
            .lock()
            .keys()
            .copied()
            .collect::<Vec<_>>();
        object_ids
            .into_iter()
            .filter_map(|object_id| self.index.get_entry(object_id).ok().flatten())
            .filter(|entry| {
                entry.lifecycle == LocalBlockLifecycleState::Uploaded && entry.evictable
            })
            .fold(0_u64, |total, entry| {
                total.saturating_add(entry.payload_len)
            })
    }

    fn forget_runtime_pins(&self, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        let mut runtime_pinned = self.runtime_pinned.lock();
        for object_id in object_ids {
            runtime_pinned.remove(object_id);
        }
    }
}

impl LocalBlockBackend for LocalBlockBackendRuntime {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get(&self, object_id: u64) -> Option<Bytes> {
        if !self.has_index_entry(object_id) {
            return None;
        }
        let payload = self.payload_store.get(object_id);
        if let Some(payload) = payload {
            self.touch_access_if_read(object_id, 1, "get");
            return Some(payload);
        }
        if self.payload_missing(object_id, "get") {
            self.remove_orphaned_index_entry(object_id, "get");
        }
        None
    }

    fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        if !self.has_index_entry(object_id) {
            return None;
        }
        let read = self.payload_store.read_into(object_id, data_offset, out);
        if let Some(bytes) = read {
            self.touch_access_if_read(object_id, bytes, "read_into");
            return Some(bytes);
        }
        if self.payload_missing(object_id, "read_into") {
            self.remove_orphaned_index_entry(object_id, "read_into");
        }
        None
    }

    fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
        match self.index.get_entry(object_id) {
            Ok(Some(_)) => {}
            Ok(None) => return None,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "local block runtime index get_for_upload lookup failed"
                );
                return None;
            }
        }
        let payload = self.payload_store.get(object_id);
        if payload.is_none() && self.payload_missing(object_id, "get_for_upload") {
            self.remove_orphaned_index_entry(object_id, "get_for_upload");
        }
        payload
    }

    fn contains(&self, object_id: u64) -> bool {
        self.has_index_entry(object_id) && self.payload_store.contains(object_id)
    }

    fn advise_will_need(&self, _object_id: u64) {}

    fn put(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), LocalBlockBackendError> {
        let payload_len = u64::try_from(data.len()).map_err(|_| {
            LocalBlockBackendError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "payload length exceeds u64",
            ))
        })?;
        if self.enforce_budget_before_put(object_id, payload_len, evictable)? {
            return Ok(());
        }

        self.payload_store
            .put(object_id, data)
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;

        let lifecycle = if evictable {
            LocalBlockLifecycleState::Uploaded
        } else {
            LocalBlockLifecycleState::Dirty
        };
        let payload_key = self
            .layout
            .payload_object_relative_path(object_id)
            .to_string_lossy()
            .into_owned()
            .into_bytes();
        if let Err(error) = self.index.upsert(CacheIndexUpsert {
            object_id,
            payload_key,
            payload_len,
            lifecycle,
            origin: if evictable {
                LocalBlockOrigin::Downloaded
            } else {
                LocalBlockOrigin::Local
            },
            upload_state: if evictable {
                LocalBlockUploadState::None
            } else {
                LocalBlockUploadState::Pending
            },
            evictable,
        }) {
            self.payload_store.remove(object_id);
            warn!(
                object_id,
                volume_uuid = ?self.volume_uuid,
                error = ?error,
                "local block runtime index upsert failed after payload write; removed payload for rollback"
            );
            return Err(LocalBlockBackendError::Other(Box::new(error)));
        }

        Ok(())
    }

    fn put_batch(&self, entries: &[LocalBlockPut]) -> Result<(), LocalBlockBackendError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut updates = Vec::with_capacity(entries.len());
        let mut persisted_entries = Vec::with_capacity(entries.len());
        let mut estimated_current = self
            .index
            .stats()
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?
            .current_bytes;

        for entry in entries {
            let payload_len = u64::try_from(entry.data.len()).map_err(|_| {
                LocalBlockBackendError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "payload length exceeds u64",
                ))
            })?;

            if let Some(target_bytes) = self
                .budget_governor
                .target_before_insert(estimated_current, payload_len)
            {
                let mut current_after_evict =
                    self.evict_to_target_bytes(target_bytes, "put_batch")?;
                if !entry.evictable {
                    current_after_evict = self.retry_non_evictable_put_after_upload_drain(
                        target_bytes,
                        payload_len,
                        current_after_evict,
                    )?;
                }
                if self
                    .budget_governor
                    .exceeds_budget_after_insert(current_after_evict, payload_len)
                {
                    if entry.evictable {
                        warn!(
                            object_id = entry.object_id,
                            payload_len,
                            current_bytes = current_after_evict,
                            budget_bytes = ?self.budget_governor.budget_bytes(),
                            "local block runtime skipped evictable batch put because budget remained under hard pressure"
                        );
                        estimated_current = current_after_evict;
                        continue;
                    }
                    return Err(LocalBlockBackendError::CapacityExhausted {
                        needed: payload_len,
                        free: self.budget_governor.free_bytes(current_after_evict),
                    });
                }
                estimated_current = current_after_evict;
            }
            estimated_current = estimated_current.saturating_add(payload_len);

            let lifecycle = if entry.evictable {
                LocalBlockLifecycleState::Uploaded
            } else {
                LocalBlockLifecycleState::Dirty
            };
            let payload_key = self
                .layout
                .payload_object_relative_path(entry.object_id)
                .to_string_lossy()
                .into_owned()
                .into_bytes();
            updates.push(CacheIndexUpsert {
                object_id: entry.object_id,
                payload_key,
                payload_len,
                lifecycle,
                origin: if entry.evictable {
                    LocalBlockOrigin::Downloaded
                } else {
                    LocalBlockOrigin::Local
                },
                upload_state: if entry.evictable {
                    LocalBlockUploadState::None
                } else {
                    LocalBlockUploadState::Pending
                },
                evictable: entry.evictable,
            });
            persisted_entries.push(PayloadBatchEntry {
                object_id: entry.object_id,
                data: entry.data.as_ref(),
            });
        }

        if updates.is_empty() {
            return Ok(());
        }

        if let Err(error) = self.payload_store.put_batch(&persisted_entries) {
            return Err(LocalBlockBackendError::Other(Box::new(error)));
        }

        if let Err(error) = self.index.upsert_many(&updates) {
            let written_object_ids: Vec<u64> = persisted_entries
                .iter()
                .map(|entry| entry.object_id)
                .collect();
            self.payload_store.remove_batch(&written_object_ids);
            warn!(
                object_count = persisted_entries.len(),
                error = ?error,
                "local block runtime batch index upsert failed after payload writes; removed payloads for rollback"
            );
            return Err(LocalBlockBackendError::Other(Box::new(error)));
        }

        Ok(())
    }

    fn sync_data(&self) -> Result<(), LocalBlockBackendError> {
        self.flush_access_touches(true, "sync_data");
        self.payload_store
            .sync_data()
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))
    }

    fn run_pressure_maintenance(&self) -> Result<(), LocalBlockBackendError> {
        self.flush_access_touches(true, "run_pressure_maintenance");
        let stats = self
            .index
            .stats()
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;
        if let Some(target_bytes) = self.budget_governor.background_target(stats.current_bytes) {
            match self.try_evict_to_target_bytes(target_bytes, "run_pressure_maintenance")? {
                Some(current_bytes) => {
                    debug!(
                        current_bytes,
                        target_bytes,
                        "local block runtime background pressure maintenance eviction pass completed"
                    );
                }
                None => {
                    debug!(
                        target_bytes,
                        "local block runtime background pressure maintenance skipped because another eviction pass is in progress"
                    );
                }
            }
        }
        Ok(())
    }

    fn mark_uploading(&self, object_id: u64) {
        if let Err(error) = self.index.mark_uploading(object_id) {
            warn!(object_id, error = ?error, "local block runtime index mark_uploading failed");
        }
    }

    fn mark_upload_failed(&self, object_id: u64) {
        if let Err(error) = self.index.mark_upload_failed(object_id) {
            warn!(
                object_id,
                error = ?error,
                "local block runtime index mark_upload_failed failed"
            );
        }
    }

    fn mark_uploaded(&self, object_id: u64) {
        if let Err(error) = self.index.mark_uploaded(object_id) {
            warn!(object_id, error = ?error, "local block runtime index mark_uploaded failed");
        } else {
            self.note_storage_state_change();
        }
    }

    fn pin(&self, object_id: u64) {
        let mut runtime_pinned = self.runtime_pinned.lock();
        let count = runtime_pinned.entry(object_id).or_default();
        *count = count.saturating_add(1);
    }

    fn unpin(&self, object_id: u64) {
        let mut runtime_pinned = self.runtime_pinned.lock();
        let std::collections::hash_map::Entry::Occupied(mut entry) =
            runtime_pinned.entry(object_id)
        else {
            return;
        };
        let count = entry.get_mut();
        *count = count.saturating_sub(1);
        if *count == 0 {
            entry.remove();
        }
    }

    fn pin_batch(&self, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        let mut runtime_pinned = self.runtime_pinned.lock();
        for object_id in object_ids {
            let count = runtime_pinned.entry(*object_id).or_default();
            *count = count.saturating_add(1);
        }
    }

    fn unpin_batch(&self, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        let mut runtime_pinned = self.runtime_pinned.lock();
        for object_id in object_ids {
            let std::collections::hash_map::Entry::Occupied(mut entry) =
                runtime_pinned.entry(*object_id)
            else {
                continue;
            };
            let count = entry.get_mut();
            *count = count.saturating_sub(1);
            if *count == 0 {
                entry.remove();
            }
        }
    }

    fn remove(&self, object_id: u64) {
        if let Err(error) = self.index.remove(object_id) {
            warn!(object_id, error = ?error, "local block runtime index remove failed");
            return;
        }
        self.forget_runtime_pins(&[object_id]);
        self.forget_access_touches(&[object_id]);
        self.payload_store.remove(object_id);
        self.note_storage_state_change();
    }

    fn remove_batch(&self, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        if let Err(error) = self.remove_batch_internal(object_ids) {
            warn!(
                count = object_ids.len(),
                error = ?error,
                "local block runtime index remove_batch failed"
            );
            return;
        }
        self.forget_runtime_pins(object_ids);
    }

    fn dirty_object_ids(&self) -> Vec<u64> {
        match self.index.dirty_uploading_entries() {
            Ok(entries) => {
                let _validation_guard = match self.begin_dirty_validation() {
                    Some(guard) => guard,
                    None => return entries.into_iter().map(|entry| entry.object_id).collect(),
                };
                if entries.is_empty() {
                    return entries.into_iter().map(|entry| entry.object_id).collect();
                }
                let mut object_ids = Vec::with_capacity(entries.len());
                let mut orphaned_ids = Vec::new();
                for entry in entries {
                    if self.payload_missing(entry.object_id, "dirty_object_ids") {
                        orphaned_ids.push(entry.object_id);
                    } else {
                        object_ids.push(entry.object_id);
                    }
                }
                self.remove_orphaned_index_entries(&orphaned_ids, "dirty_object_ids");
                object_ids
            }
            Err(error) => {
                warn!(
                    error = ?error,
                    "local block runtime index dirty snapshot failed"
                );
                Vec::new()
            }
        }
    }

    fn dirty_object_count(&self) -> usize {
        match self.index.dirty_object_count() {
            Ok(count) => count,
            Err(error) => {
                warn!(error = ?error, "local block runtime index dirty count lookup failed");
                0
            }
        }
    }

    fn lifecycle_state(&self, object_id: u64) -> LocalBlockLifecycleState {
        match self.index.get_entry(object_id) {
            Ok(Some(entry)) => {
                if self.payload_missing(object_id, "lifecycle_state") {
                    self.remove_orphaned_index_entry(object_id, "lifecycle_state");
                    LocalBlockLifecycleState::Missing
                } else {
                    entry.lifecycle
                }
            }
            Ok(None) => LocalBlockLifecycleState::Missing,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "local block runtime index lifecycle lookup failed"
                );
                LocalBlockLifecycleState::Missing
            }
        }
    }

    fn is_evictable(&self, object_id: u64) -> bool {
        match self.index.get_entry(object_id) {
            Ok(Some(entry)) => {
                if self.payload_missing(object_id, "is_evictable") {
                    self.remove_orphaned_index_entry(object_id, "is_evictable");
                    false
                } else {
                    entry.evictable && !self.is_runtime_pinned(object_id)
                }
            }
            Ok(None) => false,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "local block runtime index evictable lookup failed"
                );
                false
            }
        }
    }

    fn storage_stats(&self) -> LocalBlockStorageStats {
        match self.index.stats() {
            Ok(stats) => {
                let runtime_pinned_evictable_bytes = self.runtime_pinned_evictable_bytes();
                LocalBlockStorageStats {
                    current_bytes: stats.current_bytes,
                    dirty_bytes: stats.dirty_bytes,
                    evictable_bytes: stats
                        .evictable_bytes
                        .saturating_sub(runtime_pinned_evictable_bytes),
                }
            }
            Err(error) => {
                warn!(error = ?error, "local block runtime index storage stats lookup failed");
                LocalBlockStorageStats::default()
            }
        }
    }

    fn object_stats(&self, object_id: u64) -> Option<LocalBlockObjectStats> {
        match self.index.get_entry(object_id) {
            Ok(Some(entry)) => Some(LocalBlockObjectStats {
                payload_len: entry.payload_len,
                lifecycle: entry.lifecycle,
                origin: entry.origin,
                upload_state: entry.upload_state,
                last_access_unix_millis: entry.last_access_unix_millis,
                evictable: entry.evictable && !self.is_runtime_pinned(object_id),
            }),
            Ok(None) => None,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "local block runtime index object stats lookup failed"
                );
                None
            }
        }
    }

    fn object_ids(&self) -> Vec<u64> {
        match self.index.access_order_object_ids() {
            Ok(object_ids) => object_ids,
            Err(error) => {
                warn!(error = ?error, "local block runtime index object id snapshot failed");
                Vec::new()
            }
        }
    }
}

#[derive(Debug)]
struct PendingAccessTouches {
    object_ids: Vec<u64>,
    object_set: HashSet<u64>,
    last_flush_at: Instant,
    failed_flushes: u8,
    next_retry_at: Option<Instant>,
}

impl PendingAccessTouches {
    fn new() -> Self {
        let pending = Self {
            object_ids: Vec::new(),
            object_set: HashSet::new(),
            last_flush_at: Instant::now(),
            failed_flushes: 0,
            next_retry_at: None,
        };
        pending.debug_assert_consistent();
        pending
    }

    fn enqueue(&mut self, object_id: u64) {
        if self.object_set.insert(object_id) {
            self.object_ids.push(object_id);
        }
        self.debug_assert_consistent();
    }

    fn forget_many(&mut self, object_ids: &[u64]) {
        let mut removed_any = false;
        for object_id in object_ids {
            removed_any |= self.object_set.remove(object_id);
        }
        if !removed_any {
            return;
        }
        self.object_ids
            .retain(|candidate| self.object_set.contains(candidate));
        self.debug_assert_consistent();
    }

    fn take_all(&mut self) -> Vec<u64> {
        self.object_set.clear();
        let drained = std::mem::take(&mut self.object_ids);
        self.debug_assert_consistent();
        drained
    }

    fn take_ready_for_flush(&mut self, force: bool, now: Instant) -> Option<Vec<u64>> {
        if self.object_ids.is_empty() {
            return None;
        }
        if !force {
            if let Some(next_retry) = self.next_retry_at
                && now < next_retry
            {
                return None;
            }
            let should_flush = self.object_ids.len() >= ACCESS_TOUCH_BATCH_SIZE
                || now.duration_since(self.last_flush_at) >= ACCESS_TOUCH_FLUSH_INTERVAL;
            if !should_flush {
                return None;
            }
        }
        self.last_flush_at = now;
        Some(self.take_all())
    }

    fn requeue_after_failure(&mut self, object_ids: Vec<u64>) {
        for object_id in object_ids {
            self.enqueue(object_id);
        }
        self.failed_flushes = self.failed_flushes.saturating_add(1);
        let exp = self
            .failed_flushes
            .saturating_sub(1)
            .min(ACCESS_TOUCH_RETRY_MAX_EXPONENT);
        let delay_ms = ACCESS_TOUCH_RETRY_BASE_BACKOFF_MS.saturating_mul(1_u64 << exp);
        self.next_retry_at = Some(Instant::now() + Duration::from_millis(delay_ms));
        self.last_flush_at = Instant::now();
    }

    fn record_flush_success(&mut self) {
        self.failed_flushes = 0;
        self.next_retry_at = None;
    }

    fn debug_assert_consistent(&self) {
        debug_assert_eq!(
            self.object_ids.len(),
            self.object_set.len(),
            "pending touch queue lost set/vector parity"
        );
        debug_assert!(
            self.object_ids
                .iter()
                .all(|object_id| self.object_set.contains(object_id)),
            "pending touch queue contains object ids missing from the dedupe set"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::contract_tests::assert_local_block_backend_contract;
    use crate::local::{LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState};
    use crate::local::{LocalBlockOrigin, LocalBlockUploadState};

    fn create_v3_marker(layout: &LocalBlockLayout) {
        layout.provision().unwrap();
    }

    #[test]
    fn empty_root_has_no_runtime_backend() {
        let temp = tempfile::tempdir().unwrap();
        let detection = detect_cache_backends(temp.path()).unwrap();

        assert!(!detection.has_runtime_layout());
        assert!(!detection.index_exists);
        assert!(!detection.payload_objects_dir_exists);
        assert!(!detection.upload_objects_dir_exists);
        assert!(!detection.tmp_dir_exists);
        assert!(!detection.index_dir_exists);
        assert!(detection.needs_runtime_provisioning());
        assert_eq!(detection.runtime_backend(), None);
    }

    #[test]
    fn detects_current_runtime_layout() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let detection = detect_cache_backends(temp.path()).unwrap();
        assert!(detection.has_runtime_layout());
        assert!(!detection.index_exists);
        assert!(detection.payload_objects_dir_exists);
        assert!(detection.upload_objects_dir_exists);
        assert!(detection.tmp_dir_exists);
        assert!(detection.index_dir_exists);
        assert!(!detection.needs_runtime_provisioning());
        assert_eq!(
            detection.runtime_backend(),
            Some(LocalBlockBackendKind::Current)
        );
    }

    #[test]
    fn partial_runtime_layout_is_not_reported_as_current_backend() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        std::fs::create_dir_all(&layout.index_dir).unwrap();

        let detection = detect_cache_backends(temp.path()).unwrap();
        assert!(detection.has_runtime_layout());
        assert!(!detection.has_complete_runtime_layout());
        assert!(detection.needs_runtime_provisioning());
        assert_eq!(detection.runtime_backend(), None);
    }

    #[test]
    fn local_block_backend_requires_runtime_dirs() {
        let temp = tempfile::tempdir().unwrap();
        let err = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0xAB; 16],
        })
        .unwrap_err();
        assert!(matches!(
            err,
            LocalBlockBackendOpenError::IncompleteRuntimeLayout { .. }
        ));
    }

    #[test]
    fn local_block_backend_satisfies_local_block_backend_contract() {
        assert_local_block_backend_contract(|| {
            let temp = tempfile::tempdir().unwrap();
            let layout = LocalBlockLayout::from_cache_root(temp.path());
            create_v3_marker(&layout);
            let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0xCA; 16],
            })
            .unwrap();
            (backend, temp)
        });
    }

    #[test]
    fn local_block_backend_roundtrips_payload_and_index_state() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0xCD; 16],
        })
        .unwrap();
        backend.put(42, b"payload", false).unwrap();
        backend.sync_data().unwrap();
        assert_eq!(backend.get_for_upload(42).unwrap().as_ref(), b"payload");
        assert_eq!(backend.lifecycle_state(42), LocalBlockLifecycleState::Dirty);
        assert_eq!(backend.dirty_object_ids(), vec![42]);
    }

    #[test]
    fn local_block_fast_sync_mode_defers_payload_store_barriers_until_sync_data() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open_with_budget_and_sync_mode(
            LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0xCE; 16],
            },
            None,
            LocalBlockPayloadSyncMode::Fast,
        )
        .unwrap();
        backend.put(101, b"one", false).unwrap();
        backend.put(102, b"two", false).unwrap();

        let before_sync = backend.payload_store.sync_barrier_snapshot();
        assert_eq!(before_sync.per_put_file_sync_calls, 0);
        assert_eq!(before_sync.per_put_dir_sync_calls, 0);
        assert!(before_sync.pending_grouped_file_syncs >= 2);
        assert!(before_sync.pending_grouped_dir_syncs >= 1);

        backend.sync_data().unwrap();
        let after_sync = backend.payload_store.sync_barrier_snapshot();
        assert_eq!(after_sync.pending_grouped_file_syncs, 0);
        assert_eq!(after_sync.pending_grouped_dir_syncs, 0);
        assert!(after_sync.grouped_file_sync_calls >= 2);
        assert!(after_sync.grouped_dir_sync_calls >= 1);
    }

    #[test]
    fn local_backend_uses_index_state_after_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x11; 16],
        })
        .unwrap();
        backend.put(1, b"one", false).unwrap();
        backend.put(2, b"two", false).unwrap();
        backend.mark_uploading(2);
        backend.sync_data().unwrap();
        drop(backend);

        // Simulate source-of-truth recovery decisions in the redb index.
        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        index.mark_uploaded(1).unwrap();
        drop(index);

        let reopened = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x11; 16],
        })
        .unwrap();
        assert_eq!(
            reopened.lifecycle_state(1),
            LocalBlockLifecycleState::Uploaded
        );
        assert_eq!(
            reopened.lifecycle_state(2),
            LocalBlockLifecycleState::Uploading
        );
        assert_eq!(reopened.dirty_object_ids(), vec![2]);
        let uploaded_stats = reopened.object_stats(1).unwrap();
        assert_eq!(uploaded_stats.origin, LocalBlockOrigin::Local);
        assert_eq!(uploaded_stats.upload_state, LocalBlockUploadState::None);
        let pending_stats = reopened.object_stats(2).unwrap();
        assert_eq!(pending_stats.origin, LocalBlockOrigin::Local);
        assert_eq!(pending_stats.upload_state, LocalBlockUploadState::Uploading);
    }

    #[test]
    fn local_block_backend_reopens_existing_index_state() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let existing_backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x12; 16],
        })
        .unwrap();
        existing_backend.put(7, b"legacy-pending", false).unwrap();
        existing_backend.mark_uploading(7);
        existing_backend.sync_data().unwrap();
        drop(existing_backend);

        let reopened = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x12; 16],
        })
        .unwrap();
        assert_eq!(
            reopened.lifecycle_state(7),
            LocalBlockLifecycleState::Uploading
        );
        let stats = reopened.object_stats(7).unwrap();
        assert_eq!(stats.origin, LocalBlockOrigin::Local);
        assert_eq!(stats.upload_state, LocalBlockUploadState::Uploading);
        assert_eq!(reopened.dirty_object_ids(), vec![7]);
        assert_eq!(reopened.get(7).unwrap().as_ref(), b"legacy-pending");
    }

    #[test]
    fn local_backend_object_stats_distinguish_local_pending_and_downloaded_cache() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x21; 16],
        })
        .unwrap();
        backend.put(10, b"local-pending", false).unwrap();
        backend.put(11, b"downloaded-cache", true).unwrap();

        let pending = backend.object_stats(10).unwrap();
        assert_eq!(pending.origin, LocalBlockOrigin::Local);
        assert_eq!(pending.upload_state, LocalBlockUploadState::Pending);
        assert!(pending.last_access_unix_millis > 0);

        let cached = backend.object_stats(11).unwrap();
        assert_eq!(cached.origin, LocalBlockOrigin::Downloaded);
        assert_eq!(cached.upload_state, LocalBlockUploadState::None);
        assert!(cached.last_access_unix_millis > 0);
    }

    #[test]
    fn local_backend_get_refreshes_access_order() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x22; 16],
        })
        .unwrap();
        backend.put(10, b"ten", true).unwrap();
        backend.put(11, b"eleven", true).unwrap();
        assert_eq!(backend.get(10).unwrap().as_ref(), b"ten");
        drop(backend);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        assert_eq!(index.access_order_object_ids().unwrap(), vec![11, 10]);
    }

    #[test]
    fn local_backend_read_into_refreshes_access_order() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x23; 16],
        })
        .unwrap();
        backend.put(10, b"ten", true).unwrap();
        backend.put(11, b"eleven", true).unwrap();
        let mut out = [0_u8; 2];
        assert_eq!(backend.read_into(10, 0, &mut out), Some(2));
        assert_eq!(&out, b"te");
        drop(backend);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        assert_eq!(index.access_order_object_ids().unwrap(), vec![11, 10]);
    }

    #[test]
    fn local_block_backend_persists_payload_objects_in_file_per_object_layout() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x33; 16],
        })
        .unwrap();
        backend.put(0xABCD, b"payload", false).unwrap();

        assert!(
            layout.payload_object_path(0xABCD).exists(),
            "local runtime must persist each payload object as an individual payload file"
        );
        let entry = backend.index.get_entry(0xABCD).unwrap().unwrap();
        assert_eq!(entry.origin, LocalBlockOrigin::Local);
        assert_eq!(entry.upload_state, LocalBlockUploadState::Pending);
        backend.sync_data().unwrap();
    }

    #[test]
    fn local_backend_reopens_with_existing_payload_file() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x44; 16],
        })
        .unwrap();
        backend.put(77, b"reopen-payload", false).unwrap();
        backend.sync_data().unwrap();
        drop(backend);

        let reopened = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x44; 16],
        })
        .unwrap();
        assert_eq!(reopened.get(77).unwrap().as_ref(), b"reopen-payload");
    }

    #[test]
    fn local_backend_open_reconciles_stale_index_entries() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x45; 16],
        })
        .unwrap();
        backend.put(501, b"stale-index", false).unwrap();
        backend.sync_data().unwrap();
        std::fs::remove_file(layout.payload_object_path(501)).unwrap();
        drop(backend);

        let index_before = CacheIndex::open(&layout.redb_index_path).unwrap();
        assert!(index_before.get_entry(501).unwrap().is_some());
        drop(index_before);

        let reopened = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x45; 16],
        })
        .unwrap();
        drop(reopened);

        let index_after = CacheIndex::open(&layout.redb_index_path).unwrap();
        assert!(index_after.get_entry(501).unwrap().is_none());
    }

    #[test]
    fn local_backend_open_removes_orphan_temp_files() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let staged_tmp_dir_file = layout.tmp_dir.join("pending").join("write.tmp");
        std::fs::create_dir_all(staged_tmp_dir_file.parent().unwrap()).unwrap();
        std::fs::write(&staged_tmp_dir_file, b"staged").unwrap();

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x46; 16],
        })
        .unwrap();
        drop(backend);

        assert!(!staged_tmp_dir_file.exists());
    }

    #[test]
    fn local_backend_failed_upload_requeues_without_losing_payload_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x55; 16],
        })
        .unwrap();
        backend.put(1, b"one", false).unwrap();
        backend.put(2, b"two", false).unwrap();

        assert_eq!(backend.dirty_object_ids(), vec![1, 2]);
        backend.mark_uploading(1);
        assert_eq!(
            backend.lifecycle_state(1),
            LocalBlockLifecycleState::Uploading
        );
        backend.mark_upload_failed(1);

        assert_eq!(backend.lifecycle_state(1), LocalBlockLifecycleState::Dirty);
        assert_eq!(
            backend.get_for_upload(1).unwrap().as_ref(),
            b"one",
            "failed upload must keep payload bytes for replay"
        );
        assert_eq!(
            backend.dirty_object_ids(),
            vec![2, 1],
            "failed uploads should be requeued to the back of dirty order"
        );
    }

    #[test]
    fn local_backend_prunes_missing_payload_entries_from_dirty_replay() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x66; 16],
        })
        .unwrap();
        backend.put(10, b"ten", false).unwrap();
        backend.put(11, b"eleven", false).unwrap();
        std::fs::remove_file(layout.payload_object_path(10)).unwrap();

        assert_eq!(
            backend.dirty_object_ids(),
            vec![11],
            "dirty replay should skip and prune index entries whose payload vanished"
        );
        assert_eq!(
            backend.lifecycle_state(10),
            LocalBlockLifecycleState::Missing
        );
        assert!(!backend.contains(10));
    }

    #[test]
    fn local_backend_object_stats_do_not_prune_missing_payload_entries() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x67; 16],
        })
        .unwrap();
        backend.put(10, b"ten", false).unwrap();
        backend.sync_data().unwrap();
        std::fs::remove_file(layout.payload_object_path(10)).unwrap();

        assert!(
            backend.object_stats(10).is_some(),
            "observational object_stats should use index state without probing payload files"
        );
        drop(backend);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        assert!(
            index.get_entry(10).unwrap().is_some(),
            "object_stats must not prune stale index state during diagnostics reads"
        );
    }

    #[test]
    fn local_backend_enforces_budget_with_deterministic_eviction_order() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open_with_budget(
            LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0x71; 16],
            },
            Some(64),
        )
        .unwrap();
        backend.put(1, &[1_u8; 32], true).unwrap();
        backend.put(2, &[2_u8; 32], true).unwrap();
        backend.put(3, &[3_u8; 32], true).unwrap();

        let stats = backend.index.stats().unwrap();
        assert_eq!(stats.current_bytes, 64);
        assert!(
            !backend.contains(1),
            "oldest uploaded block should be reclaimed first"
        );
        assert!(backend.contains(2));
        assert!(backend.contains(3));
    }

    #[test]
    fn local_backend_returns_capacity_exhausted_for_non_evictable_pressure() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open_with_budget(
            LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0x72; 16],
            },
            Some(32),
        )
        .unwrap();
        backend.put(10, &[0xAA; 32], false).unwrap();
        let err = backend.put(11, &[0xBB; 16], false).unwrap_err();
        assert!(matches!(
            err,
            LocalBlockBackendError::CapacityExhausted { .. }
        ));
        assert_eq!(
            backend.lifecycle_state(11),
            LocalBlockLifecycleState::Missing
        );
    }

    #[test]
    fn local_backend_waits_for_pending_upload_drain_before_capacity_exhausted() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = std::sync::Arc::new(
            LocalBlockBackendRuntime::open_with_budget(
                LocalBlockBackendConfig {
                    cache_root: temp.path().to_path_buf(),
                    volume_uuid: [0x7B; 16],
                },
                Some(32),
            )
            .unwrap(),
        );
        backend.put(10, &[0xAA; 32], false).unwrap();
        backend.mark_uploading(10);

        let background = std::sync::Arc::clone(&backend);
        let join = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            background.mark_uploaded(10);
        });

        backend.put(11, &[0xBB; 16], false).unwrap();

        join.join().unwrap();
        assert!(
            !backend.contains(10),
            "once the older pending block finishes upload, low-space admission should reclaim it before rejecting a new local block"
        );
        assert!(
            backend.contains(11),
            "the new block should be admitted after reclaiming the older uploaded block"
        );
        assert_eq!(
            backend.lifecycle_state(11),
            LocalBlockLifecycleState::Dirty,
            "the newly admitted local block remains dirty until a later upload transition marks it uploaded"
        );
    }

    #[test]
    fn local_backend_retry_low_space_reclaims_when_pending_upload_already_finished() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open_with_budget(
            LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0x7C; 16],
            },
            Some(32),
        )
        .unwrap();
        backend.put(10, &[0xAA; 32], false).unwrap();
        backend.mark_uploading(10);

        let current_after_evict = backend.index.stats().unwrap().current_bytes;
        let target_bytes = backend
            .budget_governor
            .target_before_insert(current_after_evict, 16)
            .expect("budget pressure should require eviction before insert");

        backend.mark_uploaded(10);

        let current_after_retry = backend
            .retry_non_evictable_put_after_upload_drain(target_bytes, 16, current_after_evict)
            .unwrap();

        assert!(
            current_after_retry <= target_bytes,
            "retry should run one more eviction pass after the pending upload becomes evictable"
        );
        assert!(
            !backend.contains(10),
            "the uploaded block should be reclaimed once it becomes evictable"
        );
    }

    #[test]
    fn local_backend_background_pressure_reclaims_to_low_watermark() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open_with_budget(
            LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0x73; 16],
            },
            Some(100),
        )
        .unwrap();
        backend.put(20, &[0x01; 30], true).unwrap();
        backend.put(21, &[0x02; 30], true).unwrap();
        backend.put(22, &[0x03; 30], true).unwrap();
        backend.put(23, &[0x04; 5], true).unwrap();
        assert_eq!(backend.index.stats().unwrap().current_bytes, 95);

        backend.run_pressure_maintenance().unwrap();

        let stats = backend.index.stats().unwrap();
        assert!(
            stats.current_bytes <= 80,
            "maintenance should reclaim to low watermark when above high watermark"
        );
        assert!(
            !backend.contains(20),
            "oldest uploaded block should be reclaimed during pressure maintenance"
        );
        assert!(backend.contains(21));
    }

    #[test]
    fn local_runtime_pins_do_not_persist_across_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let config = LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x7A; 16],
        };
        let backend =
            LocalBlockBackendRuntime::open_with_budget(config.clone(), Some(100)).unwrap();
        backend.put(20, &[0x01; 30], true).unwrap();
        backend.pin(20);
        assert!(
            !backend.is_evictable(20),
            "runtime pin must make an uploaded block non-evictable in-process"
        );
        assert_eq!(backend.storage_stats().evictable_bytes, 0);
        assert!(!backend.object_stats(20).unwrap().evictable);
        drop(backend);

        let reopened = LocalBlockBackendRuntime::open_with_budget(config, Some(100)).unwrap();
        assert!(
            reopened.is_evictable(20),
            "runtime pin state must be cleared on reopen instead of persisting in redb"
        );
        assert_eq!(reopened.storage_stats().evictable_bytes, 30);
        assert!(reopened.object_stats(20).unwrap().evictable);
    }

    #[test]
    fn local_backend_eviction_tracks_skipped_non_evictable_scan_rows() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open_with_budget(
            LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0x74; 16],
            },
            Some(96),
        )
        .unwrap();
        backend.put(30, &[0xAA; 32], false).unwrap();
        backend.put(31, &[0xBB; 32], true).unwrap();
        backend.put(32, &[0xCC; 32], true).unwrap();

        backend.run_pressure_maintenance().unwrap();

        assert!(
            backend.contains(30),
            "pressure maintenance must keep local pending blocks while reclaiming disposable cache"
        );
        assert!(
            !backend.contains(31),
            "the oldest disposable downloaded/uploaded block should be reclaimed first"
        );
        assert!(backend.contains(32));
        assert!(
            backend
                .eviction_scan_skipped_non_evictable
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 1
        );
    }

    #[test]
    fn dirty_validation_gate_allows_single_runner_per_interval() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = LocalBlockBackendRuntime::open(LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x77; 16],
        })
        .unwrap();

        let first = backend.begin_dirty_validation();
        assert!(first.is_some());
        assert!(
            backend.begin_dirty_validation().is_none(),
            "concurrent callers should not start another validation pass"
        );

        drop(first);
        assert!(
            !backend
                .dirty_validation_in_progress
                .load(std::sync::atomic::Ordering::Acquire),
            "validation guard drop should clear in-progress marker"
        );
        assert!(
            backend.begin_dirty_validation().is_none(),
            "interval throttle should prevent immediate re-validation"
        );
    }

    #[test]
    fn pending_access_touches_forget_many_prunes_in_single_pass() {
        let mut pending = PendingAccessTouches::new();
        pending.enqueue(1);
        pending.enqueue(2);
        pending.enqueue(3);
        pending.enqueue(4);

        pending.forget_many(&[2, 4, 99]);
        let drained = pending.take_all();
        assert_eq!(drained, vec![1, 3]);
    }

    #[test]
    fn pending_access_touches_failure_backoff_delays_retry() {
        let mut pending = PendingAccessTouches::new();
        pending.enqueue(42);
        let now = Instant::now();
        let drained = pending.take_ready_for_flush(true, now).unwrap();
        pending.requeue_after_failure(drained);

        let retry_at = pending.next_retry_at.expect("backoff is set");
        assert!(
            pending.take_ready_for_flush(false, now).is_none(),
            "retry should be deferred until backoff elapses"
        );
        assert!(
            pending.take_ready_for_flush(false, retry_at).is_some(),
            "retry should resume once backoff elapses"
        );
    }

    #[test]
    fn pending_access_touches_dedupes_duplicate_enqueue() {
        let mut pending = PendingAccessTouches::new();
        pending.enqueue(7);
        pending.enqueue(7);
        pending.enqueue(8);

        assert_eq!(pending.take_all(), vec![7, 8]);
    }
}

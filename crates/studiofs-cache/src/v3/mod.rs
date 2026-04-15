//! Cache 3.0 contract and local backend.
//!
//! This module defines:
//! - the non-mutating detection contract for Cache 3.0 layout state;
//! - the active Cache 3.0 local block backend backed by a file-per-block
//!   payload store plus redb lifecycle index.

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tracing::{debug, warn};

use crate::local::{
    LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState, LocalBlockObjectStats,
    LocalBlockStorageStats,
};

pub mod cache_index;
pub mod eviction;
pub mod file_payload_store;
pub mod layout;
pub mod platform;
pub mod reconcile;

pub use cache_index::{
    CacheIndex, CacheIndexEntry, CacheIndexError, CacheIndexStats, CacheIndexUpsert,
    DirtyReplayEntry, EvictionCandidateEntry,
};
use eviction::{CacheBudgetGovernor, EVICTION_CANDIDATE_PAGE_SIZE, EVICTION_DELETE_BATCH_SIZE};
pub use file_payload_store::{FilePayloadStore, FilePayloadStoreError};
pub use layout::{
    CACHE_V3_LAYOUT_MARKER_FILE, CACHE_V3_LAYOUT_VERSION, CACHE_V3_PAYLOAD_SHARD_COUNT,
    CACHE_V3_REDB_INDEX_FILE, CACHE_V3_SUBTREE, CacheV3Layout,
};
pub use reconcile::{CacheV3StartupReconcileError, CacheV3StartupReconcileReport};
use reconcile::{CacheV3StartupReconcileMode, reconcile_startup_state};

const ACCESS_TOUCH_MIN_INTERVAL: Duration = Duration::from_secs(60);
const ACCESS_TOUCH_BATCH_SIZE: usize = 128;
#[cfg(not(test))]
const ACCESS_TOUCH_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
#[cfg(test)]
const ACCESS_TOUCH_FLUSH_INTERVAL: Duration = Duration::ZERO;
const ACCESS_TOUCH_RETRY_BASE_BACKOFF_MS: u64 = 10;
const ACCESS_TOUCH_RETRY_MAX_EXPONENT: u8 = 8;
const DIRTY_ORPHAN_PRUNE_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// Backend kind available on the runtime path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheBackendKind {
    CacheV3,
}

impl CacheBackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CacheV3 => "cache-v3",
        }
    }
}

impl fmt::Display for CacheBackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Detection snapshot for a per-volume cache root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheBackendDetection {
    pub layout: CacheV3Layout,
    pub v3_layout_marker_exists: bool,
    pub v3_index_exists: bool,
    pub v3_payload_blocks_dir_exists: bool,
    pub v3_tmp_dir_exists: bool,
    pub v3_index_dir_exists: bool,
}

impl CacheBackendDetection {
    pub fn has_cache_v3_contract(&self) -> bool {
        self.v3_layout_marker_exists
    }

    pub const fn needs_runtime_provisioning(&self) -> bool {
        !self.v3_layout_marker_exists
            || !self.v3_payload_blocks_dir_exists
            || !self.v3_tmp_dir_exists
            || !self.v3_index_dir_exists
    }

    pub fn runtime_backend(&self) -> Option<CacheBackendKind> {
        if !self.has_cache_v3_contract() {
            None
        } else {
            Some(CacheBackendKind::CacheV3)
        }
    }
}

/// Detect backend contracts for a per-volume cache root.
pub fn detect_cache_backends(cache_root: &Path) -> io::Result<CacheBackendDetection> {
    let layout = CacheV3Layout::from_cache_root(cache_root);
    let v3_layout_marker_exists = layout.layout_marker.try_exists()?;
    let v3_index_exists = layout.redb_index_path.try_exists()?;
    let v3_payload_blocks_dir_exists = layout.payload_blocks_dir.try_exists()?;
    let v3_tmp_dir_exists = layout.tmp_dir.try_exists()?;
    let v3_index_dir_exists = layout.index_dir.try_exists()?;
    Ok(CacheBackendDetection {
        layout,
        v3_layout_marker_exists,
        v3_index_exists,
        v3_payload_blocks_dir_exists,
        v3_tmp_dir_exists,
        v3_index_dir_exists,
    })
}

/// Configuration for opening the Cache 3.0 local backend.
#[derive(Debug, Clone)]
pub struct CacheV3LocalBlockBackendConfig {
    pub cache_root: PathBuf,
    pub volume_uuid: [u8; 16],
}

/// Payload-store sync cadence policy for Cache 3.0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheV3PayloadSyncMode {
    /// Keep per-write file+directory barriers at payload commit time.
    #[default]
    Durable,
    /// Defer payload-store barriers to grouped `sync_data()` calls.
    Fast,
}

/// Errors returned while opening the Cache 3.0 local backend.
#[derive(Debug, thiserror::Error)]
pub enum CacheV3LocalBlockBackendError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("cache-v3 layout marker is missing at '{path}'")]
    MissingLayoutMarker { path: PathBuf },
    #[error("failed to open cache-v3 index at '{path}': {source}")]
    OpenCacheIndex {
        path: PathBuf,
        #[source]
        source: CacheIndexError,
    },
    #[error("failed to open cache-v3 payload store at '{cache_root}': {source}")]
    OpenPayloadStore {
        cache_root: PathBuf,
        #[source]
        source: FilePayloadStoreError,
    },
    #[error("failed to reconcile cache-v3 startup state under '{cache_root}': {source}")]
    StartupReconcile {
        cache_root: PathBuf,
        #[source]
        source: CacheV3StartupReconcileError,
    },
}

impl CacheV3LocalBlockBackendError {
    pub fn is_transient_open_lock_conflict(&self) -> bool {
        match self {
            Self::OpenCacheIndex { source, .. } => source.is_database_lock_conflict(),
            _ => false,
        }
    }
}

/// Cache 3.0 local backend backed by file-per-block payload files and redb index.
#[derive(Debug)]
pub struct CacheV3LocalBlockBackend {
    layout: CacheV3Layout,
    volume_uuid: [u8; 16],
    index: CacheIndex,
    payload_store: FilePayloadStore,
    budget_governor: CacheBudgetGovernor,
    eviction_pass_lock: Mutex<()>,
    eviction_scan_skipped_non_evictable: AtomicU64,
    pending_access_touches: Mutex<PendingAccessTouches>,
    last_dirty_orphan_prune_at: Mutex<Instant>,
    dirty_validation_in_progress: AtomicBool,
}

struct DirtyValidationGuard<'a> {
    backend: &'a CacheV3LocalBlockBackend,
}

impl Drop for DirtyValidationGuard<'_> {
    fn drop(&mut self) {
        self.backend
            .dirty_validation_in_progress
            .store(false, Ordering::Release);
    }
}

impl Drop for CacheV3LocalBlockBackend {
    fn drop(&mut self) {
        self.flush_access_touches(true, "drop");
    }
}

impl CacheV3LocalBlockBackend {
    pub fn open(
        config: CacheV3LocalBlockBackendConfig,
    ) -> Result<Self, CacheV3LocalBlockBackendError> {
        Self::open_with_budget_and_sync_mode(config, None, CacheV3PayloadSyncMode::Durable)
    }

    pub fn open_with_budget(
        config: CacheV3LocalBlockBackendConfig,
        cache_budget_bytes: Option<u64>,
    ) -> Result<Self, CacheV3LocalBlockBackendError> {
        Self::open_with_budget_and_sync_mode(
            config,
            cache_budget_bytes,
            CacheV3PayloadSyncMode::Durable,
        )
    }

    pub fn open_with_budget_and_sync_mode(
        config: CacheV3LocalBlockBackendConfig,
        cache_budget_bytes: Option<u64>,
        payload_sync_mode: CacheV3PayloadSyncMode,
    ) -> Result<Self, CacheV3LocalBlockBackendError> {
        let layout = CacheV3Layout::from_cache_root(&config.cache_root);
        if !layout.layout_marker.try_exists()? {
            return Err(CacheV3LocalBlockBackendError::MissingLayoutMarker {
                path: layout.layout_marker.clone(),
            });
        }

        let index = CacheIndex::open(&layout.redb_index_path).map_err(|source| {
            CacheV3LocalBlockBackendError::OpenCacheIndex {
                path: layout.redb_index_path.clone(),
                source,
            }
        })?;

        let payload_store =
            FilePayloadStore::open_with_sync_mode(layout.clone(), payload_sync_mode).map_err(
                |source| CacheV3LocalBlockBackendError::OpenPayloadStore {
                    cache_root: layout.v3_root.clone(),
                    source,
                },
            )?;

        let startup_reconcile_mode = CacheV3StartupReconcileMode::from_env();
        let startup_reconcile_report =
            reconcile_startup_state(&layout, &index, startup_reconcile_mode).map_err(|source| {
                CacheV3LocalBlockBackendError::StartupReconcile {
                    cache_root: layout.v3_root.clone(),
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
                "cache-v3 startup reconciliation repaired cache artifacts"
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
            eviction_scan_skipped_non_evictable: AtomicU64::new(0),
            pending_access_touches: Mutex::new(PendingAccessTouches::new()),
            last_dirty_orphan_prune_at: Mutex::new(
                Instant::now()
                    .checked_sub(DIRTY_ORPHAN_PRUNE_MIN_INTERVAL)
                    .unwrap_or_else(Instant::now),
            ),
            dirty_validation_in_progress: AtomicBool::new(false),
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
                    "cache-v3 index lookup failed while checking payload presence"
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
            operation, "cache-v3 detected index entry without payload; pruning stale index entry"
        );
        if let Err(error) = self.index.remove(object_id) {
            warn!(
                object_id,
                operation,
                error = ?error,
                "cache-v3 failed to prune stale index entry"
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
                    "cache-v3 payload existence probe failed"
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
            "cache-v3 pruning stale index entries that no longer have payload files"
        );
        if let Err(error) = self.index.remove_batch(object_ids) {
            warn!(
                operation,
                orphan_count = object_ids.len(),
                error = ?error,
                "cache-v3 batch prune of stale index entries failed"
            );
        } else {
            self.forget_access_touches(object_ids);
        }
    }

    fn remove_batch_internal(&self, object_ids: &[u64]) -> Result<(), CacheIndexError> {
        self.index.remove_batch(object_ids)?;
        self.forget_access_touches(object_ids);
        self.payload_store.remove_batch(object_ids);
        Ok(())
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
                "cache-v3 batched access touch failed"
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
            if page.skipped_non_evictable > 0 {
                let skipped_total = self
                    .eviction_scan_skipped_non_evictable
                    .fetch_add(page.skipped_non_evictable, Ordering::Relaxed)
                    .saturating_add(page.skipped_non_evictable);
                debug!(
                    operation,
                    scanned_rows = page.scanned_rows,
                    skipped_non_evictable = page.skipped_non_evictable,
                    skipped_non_evictable_total = skipped_total,
                    "cache-v3 eviction candidate scan skipped non-evictable rows"
                );
            }

            for candidate in page.entries {
                if estimated_current <= target_bytes {
                    break;
                }
                batch.push(candidate.object_id);
                estimated_current = estimated_current.saturating_sub(candidate.payload_len);
                if batch.len() >= EVICTION_DELETE_BATCH_SIZE {
                    self.remove_batch_internal(&batch)
                        .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;
                    batch.clear();
                }
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
                "cache-v3 budget pressure remained above target after deterministic eviction pass"
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

        let current_after_evict = self.evict_to_target_bytes(target_bytes, "put")?;
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
                "cache-v3 skipped evictable put because budget remained under hard pressure"
            );
            return Ok(true);
        }

        Err(LocalBlockBackendError::CapacityExhausted {
            needed: payload_len,
            free: self.budget_governor.free_bytes(current_after_evict),
        })
    }
}

impl LocalBlockBackend for CacheV3LocalBlockBackend {
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
                    "cache-v3 index get_for_upload lookup failed"
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
            .payload_block_relative_path(object_id)
            .to_string_lossy()
            .into_owned()
            .into_bytes();
        if let Err(error) = self.index.upsert(CacheIndexUpsert {
            object_id,
            payload_key,
            payload_len,
            lifecycle,
            evictable,
        }) {
            self.payload_store.remove(object_id);
            warn!(
                object_id,
                volume_uuid = ?self.volume_uuid,
                error = ?error,
                "cache-v3 index upsert failed after payload write; removed payload for rollback"
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
                        "cache-v3 background pressure maintenance eviction pass completed"
                    );
                }
                None => {
                    debug!(
                        target_bytes,
                        "cache-v3 background pressure maintenance skipped because another eviction pass is in progress"
                    );
                }
            }
        }
        Ok(())
    }

    fn mark_uploading(&self, object_id: u64) {
        if let Err(error) = self.index.mark_uploading(object_id) {
            warn!(object_id, error = ?error, "cache-v3 index mark_uploading failed");
        }
    }

    fn mark_upload_failed(&self, object_id: u64) {
        if let Err(error) = self.index.mark_upload_failed(object_id) {
            warn!(
                object_id,
                error = ?error,
                "cache-v3 index mark_upload_failed failed"
            );
        }
    }

    fn mark_uploaded(&self, object_id: u64) {
        if let Err(error) = self.index.mark_uploaded(object_id) {
            warn!(object_id, error = ?error, "cache-v3 index mark_uploaded failed");
        }
    }

    fn remove(&self, object_id: u64) {
        if let Err(error) = self.index.remove(object_id) {
            warn!(object_id, error = ?error, "cache-v3 index remove failed");
            return;
        }
        self.forget_access_touches(&[object_id]);
        self.payload_store.remove(object_id);
    }

    fn remove_batch(&self, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        if let Err(error) = self.remove_batch_internal(object_ids) {
            warn!(
                count = object_ids.len(),
                error = ?error,
                "cache-v3 index remove_batch failed"
            );
        }
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
                    "cache-v3 index dirty snapshot failed"
                );
                Vec::new()
            }
        }
    }

    fn dirty_object_count(&self) -> usize {
        match self.index.dirty_object_count() {
            Ok(count) => count,
            Err(error) => {
                warn!(error = ?error, "cache-v3 index dirty count lookup failed");
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
                    "cache-v3 index lifecycle lookup failed"
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
                    entry.evictable
                }
            }
            Ok(None) => false,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "cache-v3 index evictable lookup failed"
                );
                false
            }
        }
    }

    fn storage_stats(&self) -> LocalBlockStorageStats {
        match self.index.stats() {
            Ok(stats) => LocalBlockStorageStats {
                current_bytes: stats.current_bytes,
                dirty_bytes: stats.dirty_bytes,
                evictable_bytes: stats.evictable_bytes,
            },
            Err(error) => {
                warn!(error = ?error, "cache-v3 index storage stats lookup failed");
                LocalBlockStorageStats::default()
            }
        }
    }

    fn object_stats(&self, object_id: u64) -> Option<LocalBlockObjectStats> {
        match self.index.get_entry(object_id) {
            Ok(Some(entry)) => Some(LocalBlockObjectStats {
                payload_len: entry.payload_len,
                lifecycle: entry.lifecycle,
                evictable: entry.evictable,
            }),
            Ok(None) => None,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "cache-v3 index object stats lookup failed"
                );
                None
            }
        }
    }

    fn object_ids(&self) -> Vec<u64> {
        match self.index.access_order_object_ids() {
            Ok(object_ids) => object_ids,
            Err(error) => {
                warn!(error = ?error, "cache-v3 index object id snapshot failed");
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
        Self {
            object_ids: Vec::new(),
            object_set: HashSet::new(),
            last_flush_at: Instant::now(),
            failed_flushes: 0,
            next_retry_at: None,
        }
    }

    fn enqueue(&mut self, object_id: u64) {
        if self.object_set.insert(object_id) {
            self.object_ids.push(object_id);
        }
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
    }

    fn take_all(&mut self) -> Vec<u64> {
        self.object_set.clear();
        std::mem::take(&mut self.object_ids)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::contract_tests::assert_local_block_backend_contract;
    use crate::local::{LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState};

    fn create_v3_marker(layout: &CacheV3Layout) {
        std::fs::create_dir_all(&layout.v3_root).unwrap();
        std::fs::write(&layout.layout_marker, b"v1").unwrap();
    }

    #[test]
    fn empty_root_has_no_runtime_backend() {
        let temp = tempfile::tempdir().unwrap();
        let detection = detect_cache_backends(temp.path()).unwrap();

        assert!(!detection.has_cache_v3_contract());
        assert!(!detection.v3_index_exists);
        assert!(!detection.v3_payload_blocks_dir_exists);
        assert!(!detection.v3_tmp_dir_exists);
        assert!(!detection.v3_index_dir_exists);
        assert!(detection.needs_runtime_provisioning());
        assert_eq!(detection.runtime_backend(), None);
    }

    #[test]
    fn detects_v3_only_layout() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let detection = detect_cache_backends(temp.path()).unwrap();
        assert!(detection.has_cache_v3_contract());
        assert!(!detection.v3_index_exists);
        assert!(!detection.v3_payload_blocks_dir_exists);
        assert!(!detection.v3_tmp_dir_exists);
        assert!(!detection.v3_index_dir_exists);
        assert!(detection.needs_runtime_provisioning());
        assert_eq!(detection.runtime_backend(), Some(CacheBackendKind::CacheV3));
    }

    #[test]
    fn cache_v3_backend_requires_layout_marker() {
        let temp = tempfile::tempdir().unwrap();
        let err = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0xAB; 16],
        })
        .unwrap_err();
        assert!(matches!(
            err,
            CacheV3LocalBlockBackendError::MissingLayoutMarker { .. }
        ));
    }

    #[test]
    fn cache_v3_backend_satisfies_local_block_backend_contract() {
        assert_local_block_backend_contract(|| {
            let temp = tempfile::tempdir().unwrap();
            let layout = CacheV3Layout::from_cache_root(temp.path());
            create_v3_marker(&layout);
            let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0xCA; 16],
            })
            .unwrap();
            (backend, temp)
        });
    }

    #[test]
    fn cache_v3_backend_roundtrips_payload_and_index_state() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
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
    fn cache_v3_fast_sync_mode_defers_payload_store_barriers_until_sync_data() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open_with_budget_and_sync_mode(
            CacheV3LocalBlockBackendConfig {
                cache_root: temp.path().to_path_buf(),
                volume_uuid: [0xCE; 16],
            },
            None,
            CacheV3PayloadSyncMode::Fast,
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
    fn cache_v3_backend_uses_index_state_after_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
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

        let reopened = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
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
    }

    #[test]
    fn cache_v3_backend_get_refreshes_access_order() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
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
    fn cache_v3_backend_read_into_refreshes_access_order() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
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
    fn cache_v3_backend_persists_blocks_in_file_per_block_payload_layout() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x33; 16],
        })
        .unwrap();
        backend.put(0xABCD, b"payload", false).unwrap();
        backend.sync_data().unwrap();

        assert!(
            layout.payload_block_path(0xABCD).exists(),
            "cache-v3 must persist each block as an individual payload file"
        );
    }

    #[test]
    fn cache_v3_backend_reopens_with_existing_payload_file() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x44; 16],
        })
        .unwrap();
        backend.put(77, b"reopen-payload", false).unwrap();
        backend.sync_data().unwrap();
        drop(backend);

        let reopened = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x44; 16],
        })
        .unwrap();
        assert_eq!(reopened.get(77).unwrap().as_ref(), b"reopen-payload");
    }

    #[test]
    fn cache_v3_backend_open_reconciles_stale_index_entries() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x45; 16],
        })
        .unwrap();
        backend.put(501, b"stale-index", false).unwrap();
        backend.sync_data().unwrap();
        std::fs::remove_file(layout.payload_block_path(501)).unwrap();
        drop(backend);

        let index_before = CacheIndex::open(&layout.redb_index_path).unwrap();
        assert!(index_before.get_entry(501).unwrap().is_some());
        drop(index_before);

        let reopened = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x45; 16],
        })
        .unwrap();
        drop(reopened);

        let index_after = CacheIndex::open(&layout.redb_index_path).unwrap();
        assert!(index_after.get_entry(501).unwrap().is_none());
    }

    #[test]
    fn cache_v3_backend_open_removes_orphan_temp_files() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let staged_tmp_dir_file = layout.tmp_dir.join("pending").join("write.tmp");
        std::fs::create_dir_all(staged_tmp_dir_file.parent().unwrap()).unwrap();
        std::fs::write(&staged_tmp_dir_file, b"staged").unwrap();

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x46; 16],
        })
        .unwrap();
        drop(backend);

        assert!(!staged_tmp_dir_file.exists());
    }

    #[test]
    fn cache_v3_backend_failed_upload_requeues_without_losing_payload_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
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
    fn cache_v3_backend_prunes_missing_payload_entries_from_dirty_replay() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x66; 16],
        })
        .unwrap();
        backend.put(10, b"ten", false).unwrap();
        backend.put(11, b"eleven", false).unwrap();
        std::fs::remove_file(layout.payload_block_path(10)).unwrap();

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
    fn cache_v3_backend_object_stats_do_not_prune_missing_payload_entries() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x67; 16],
        })
        .unwrap();
        backend.put(10, b"ten", false).unwrap();
        backend.sync_data().unwrap();
        std::fs::remove_file(layout.payload_block_path(10)).unwrap();

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
    fn cache_v3_backend_enforces_budget_with_deterministic_eviction_order() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open_with_budget(
            CacheV3LocalBlockBackendConfig {
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
    fn cache_v3_backend_returns_capacity_exhausted_for_non_evictable_pressure() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open_with_budget(
            CacheV3LocalBlockBackendConfig {
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
    fn cache_v3_background_pressure_reclaims_to_low_watermark() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open_with_budget(
            CacheV3LocalBlockBackendConfig {
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
    fn cache_v3_eviction_tracks_skipped_non_evictable_scan_rows() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open_with_budget(
            CacheV3LocalBlockBackendConfig {
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
            backend
                .eviction_scan_skipped_non_evictable
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 1
        );
    }

    #[test]
    fn dirty_validation_gate_allows_single_runner_per_interval() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
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
}

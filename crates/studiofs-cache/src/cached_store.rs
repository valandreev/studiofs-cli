//! Cached-store runtime for the production Local-First data path.
//!
//! This module owns the active local cache backend, pending writeback queue,
//! object pinning, and cache-pipeline diagnostics used by the mount runtime.

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::warn;

use crate::local::{
    LocalPayloadBackend, LocalPayloadLifecycleState, LocalPayloadObjectStats, LocalPayloadPut,
    LocalPayloadStorageStats,
};
use crate::metrics::{CachePipelineMetrics, CachePipelineMetricsSnapshot};
use crate::runtime::LocalPayloadLayout;
use crate::upload_queue::{PendingUpload, UploadQueue};
use crate::upload_stage::{UploadStageStore, UploadStageStoreError};

/// Stable identifier for cached payload objects.
pub type ObjectId = u64;
const DEFAULT_WRITER_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;
const MIN_WRITER_PAGE_ALLOCATION: usize = 64 * 1024;

#[derive(Debug, Clone)]
/// Configuration for opening a [`CachedStore`].
pub struct CachedStoreConfig {
    /// Root directory for on-disk cache state.
    pub cache_dir: PathBuf,
    /// In-memory budget (bytes) for the hot-upload buffer. `0` disables it.
    pub upload_hot_bytes: usize,
}

#[derive(Debug, Clone)]
/// Parameters for constructing a [`CachedStoreWriterSlice`].
pub struct CachedStoreWriterConfig {
    /// Inode number associated with the write stream.
    pub ino: u64,
    /// Absolute offset (bytes) of the first byte represented by this slice.
    pub base_offset: u64,
    /// Initial object id to assign to the first staged block.
    pub first_object_id: ObjectId,
    /// Content epoch used to route/validate uploads for this write stream.
    pub content_epoch: u64,
    /// Block size (bytes) to stage at a time.
    pub block_size: usize,
    /// Writer page size (bytes) used for internal buffering/alignment.
    pub page_size: usize,
    /// Optional dirty-byte threshold for throttling signals.
    pub dirty_byte_limit: Option<usize>,
    /// Optional cap on the writer's buffered bytes.
    pub max_buffer_bytes: Option<usize>,
    /// Whether to enqueue uploads as blocks are staged.
    pub submit_upload: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Summary of the effects of a single write call.
pub struct WriterWriteResult {
    /// Bytes accepted from the caller.
    pub accepted_bytes: usize,
    /// Total dirty bytes retained in the writer after the write.
    pub dirty_bytes: usize,
    /// Whether the writer considers itself over the configured dirty limit.
    pub throttled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Metadata describing a staged block persisted to the local cache.
pub struct StagedSlice {
    /// Inode number associated with the staged block.
    pub ino: u64,
    /// Absolute start offset (bytes) for this block.
    pub offset: u64,
    /// Object id assigned to the staged payload.
    pub object_id: ObjectId,
    /// Payload length (bytes).
    pub len: usize,
    /// Content epoch associated with this staged write.
    pub content_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A staged block along with its detached payload bytes.
pub struct DetachedStagedSlice {
    /// Staged block metadata.
    pub staged: StagedSlice,
    /// Payload bytes for the staged block.
    pub payload: Bytes,
    /// Whether the staged block should be submitted to the upload queue.
    pub submit_upload: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Snapshot of writer counters maintained by the cached-store pipeline.
pub struct CachedStoreWriterCountersSnapshot {
    /// Writer-owned bytes retained by the cached-store slice writer, including
    /// zero-filled sparse gaps that will be staged locally.
    pub dirty_bytes: u64,
    /// Total bytes staged into the local cache backend.
    pub staged_bytes: u64,
    /// Bytes currently attributed to pending uploads.
    pub pending_upload_bytes: u64,
    /// Total number of slices staged.
    pub staged_slices: u64,
    /// Total number of staging latency observations recorded.
    pub staging_latency_observations: u64,
    /// Total number of throttle events recorded.
    pub throttle_events: u64,
}

#[derive(Debug, Default)]
struct CachedStoreWriterCounters {
    dirty_bytes: AtomicU64,
    staged_bytes: AtomicU64,
    pending_upload_bytes: AtomicU64,
    staged_slices: AtomicU64,
    staging_latency_observations: AtomicU64,
    throttle_events: AtomicU64,
}

impl CachedStoreWriterCounters {
    fn snapshot(&self) -> CachedStoreWriterCountersSnapshot {
        CachedStoreWriterCountersSnapshot {
            dirty_bytes: self.dirty_bytes.load(Ordering::Relaxed),
            staged_bytes: self.staged_bytes.load(Ordering::Relaxed),
            pending_upload_bytes: self.pending_upload_bytes.load(Ordering::Relaxed),
            staged_slices: self.staged_slices.load(Ordering::Relaxed),
            staging_latency_observations: self.staging_latency_observations.load(Ordering::Relaxed),
            throttle_events: self.throttle_events.load(Ordering::Relaxed),
        }
    }

    fn add_dirty_bytes(&self, bytes: usize) {
        self.dirty_bytes
            .fetch_add(saturating_u64_from_usize(bytes), Ordering::Relaxed);
    }

    fn subtract_dirty_bytes(&self, bytes: usize) {
        subtract_saturating(&self.dirty_bytes, saturating_u64_from_usize(bytes));
    }

    fn record_staged(&self, bytes: usize) {
        let bytes = saturating_u64_from_usize(bytes);
        self.staged_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.staged_slices.fetch_add(1, Ordering::Relaxed);
        self.staging_latency_observations
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_pending_upload(&self, bytes: usize) {
        self.pending_upload_bytes
            .fetch_add(saturating_u64_from_usize(bytes), Ordering::Relaxed);
    }

    fn record_upload_terminal(&self, bytes: u64) {
        subtract_saturating(&self.pending_upload_bytes, bytes);
    }

    fn record_throttle(&self) {
        self.throttle_events.fetch_add(1, Ordering::Relaxed);
    }
}

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn subtract_saturating(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

#[derive(Debug, thiserror::Error)]
/// Errors produced by [`CachedStoreWriterSlice`].
pub enum CachedStoreWriterError {
    /// Invalid block/page geometry (must be greater than zero).
    #[error("writer page size and block size must be greater than zero")]
    InvalidPageGeometry,
    /// The requested write offset targets bytes already frozen.
    #[error("write offset {offset} targets bytes already frozen before {frozen_before}")]
    WriteBeforeFrozen {
        /// File offset (in bytes) where the write would begin.
        offset: u64,
        /// Smallest byte offset that is still writable.
        frozen_before: u64,
    },
    /// The requested offset could not be represented on this platform.
    #[error("writer offset is too large for this platform")]
    OffsetTooLarge,
    /// The writer's buffered bytes would exceed its configured limit.
    #[error("writer buffer limit exceeded: requested {requested} bytes, limit {limit} bytes")]
    BufferLimitExceeded {
        /// Requested buffer size in bytes for the current write operation.
        requested: usize,
        /// Configured hard limit in bytes for the writer buffer.
        limit: usize,
    },
    /// The local payload backend failed while staging an object.
    #[error("local payload backend failed while staging object {object_id}: {source}")]
    LocalPayload {
        /// Object id that failed to stage.
        object_id: ObjectId,
        /// Underlying local payload error.
        #[source]
        source: crate::local::LocalPayloadBackendError,
    },
    /// The upload stage store failed while staging an object.
    #[error("upload stage failed while staging object {object_id}: {source}")]
    UploadStage {
        /// Object id that failed to stage.
        object_id: ObjectId,
        /// Underlying upload-stage store error.
        #[source]
        source: UploadStageStoreError,
    },
}

/// In-memory writer for staging blocks into a [`CachedStore`].
pub struct CachedStoreWriterSlice {
    /// Cached-store backing instance for this writer.
    store: Arc<CachedStore>,
    /// Static writer configuration used for block sizing and buffering rules.
    config: CachedStoreWriterConfig,
    /// Offset of the first byte covered by currently active buffered pages.
    active_start: u64,
    /// Next object id that should be assigned to a staged block.
    next_object_id: ObjectId,
    /// Timestamp when this writer slice was created.
    created_at: Instant,
    /// Buffered write pages pending staging or flushing.
    pages: VecDeque<WriterPage>,
    /// Current number of bytes buffered as dirty in this writer.
    dirty_len: usize,
}

enum WriterPage {
    Mutable(BytesMut),
    Owned(Bytes),
}

impl WriterPage {
    fn len(&self) -> usize {
        match self {
            Self::Mutable(page) => page.len(),
            Self::Owned(page) => page.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        match self {
            Self::Mutable(page) => page.capacity(),
            Self::Owned(page) => page.len(),
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mutable(page) => page.as_ref(),
            Self::Owned(page) => page.as_ref(),
        }
    }

    fn make_mut(&mut self, capacity: usize) -> &mut BytesMut {
        if let Self::Owned(page) = self {
            let mut mutable = BytesMut::with_capacity(capacity.max(page.len()));
            mutable.extend_from_slice(page.as_ref());
            *self = Self::Mutable(mutable);
        }
        match self {
            Self::Mutable(page) => page,
            Self::Owned(_) => unreachable!("owned page converted to mutable"),
        }
    }

    fn split_to_bytes(&mut self, len: usize) -> Bytes {
        match self {
            Self::Mutable(page) => page.split_to(len).freeze(),
            Self::Owned(page) => page.split_to(len),
        }
    }

    fn into_bytes(self) -> Bytes {
        match self {
            Self::Mutable(page) => page.freeze(),
            Self::Owned(page) => page,
        }
    }
}

impl CachedStoreConfig {
    /// Creates a new config rooted at `cache_dir` with hot-upload buffering disabled.
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            upload_hot_bytes: 0,
        }
    }

    /// Sets the hot-upload in-memory buffer budget (bytes).
    pub fn with_upload_hot_bytes(mut self, upload_hot_bytes: usize) -> Self {
        self.upload_hot_bytes = upload_hot_bytes;
        self
    }
}

#[derive(Debug, Default)]
struct PinnedState {
    objects: HashMap<ObjectId, usize>,
}

impl PinnedState {
    fn insert(&mut self, object_id: ObjectId) -> bool {
        let count = self.objects.entry(object_id).or_default();
        let first_pin = *count == 0;
        *count = count.saturating_add(1);
        first_pin
    }

    fn insert_many(&mut self, object_ids: impl IntoIterator<Item = ObjectId>) -> Vec<ObjectId> {
        let mut first_pins = Vec::new();
        for object_id in object_ids {
            if self.insert(object_id) {
                first_pins.push(object_id);
            }
        }
        first_pins
    }

    fn contains(&self, object_id: ObjectId) -> bool {
        self.objects.contains_key(&object_id)
    }

    fn remove(&mut self, object_id: ObjectId) -> bool {
        let std::collections::hash_map::Entry::Occupied(mut entry) = self.objects.entry(object_id)
        else {
            return false;
        };
        let count = entry.get_mut();
        *count = count.saturating_sub(1);
        if *count == 0 {
            entry.remove();
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
struct UploadHotEntry {
    data: Bytes,
    generation: u64,
}

#[derive(Debug, Default)]
struct UploadHotState {
    budget_bytes: usize,
    total_bytes: usize,
    next_generation: u64,
    // Queue entries made stale by remove/replace and not yet skipped or compacted.
    stale_order_entries: usize,
    order: VecDeque<(ObjectId, u64)>,
    entries: HashMap<ObjectId, UploadHotEntry>,
}

impl UploadHotState {
    const MIN_STALE_COMPACT_ENTRIES: usize = 64;

    fn new(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            ..Self::default()
        }
    }

    fn insert(&mut self, object_id: ObjectId, data: Bytes) {
        if self.budget_bytes == 0 || data.is_empty() {
            return;
        }
        let len = data.len();
        self.next_generation = self.next_generation.saturating_add(1);
        let generation = self.next_generation;
        if let Some(previous) = self
            .entries
            .insert(object_id, UploadHotEntry { data, generation })
        {
            self.total_bytes = self.total_bytes.saturating_sub(previous.data.len());
            self.stale_order_entries = self.stale_order_entries.saturating_add(1);
        }
        self.total_bytes = self.total_bytes.saturating_add(len);
        self.order.push_back((object_id, generation));
        self.evict_over_budget();
        self.compact_stale_order_if_needed();
    }

    fn get(&self, object_id: ObjectId) -> Option<Bytes> {
        self.entries.get(&object_id).map(|entry| entry.data.clone())
    }

    fn remove(&mut self, object_id: ObjectId) {
        if let Some(previous) = self.entries.remove(&object_id) {
            self.total_bytes = self.total_bytes.saturating_sub(previous.data.len());
            self.stale_order_entries = self.stale_order_entries.saturating_add(1);
            self.compact_stale_order_if_needed();
        }
    }

    fn evict_over_budget(&mut self) {
        while self.total_bytes > self.budget_bytes {
            let Some((object_id, generation)) = self.order.pop_front() else {
                break;
            };
            let should_evict = self
                .entries
                .get(&object_id)
                .is_some_and(|entry| entry.generation == generation);
            if should_evict {
                if let Some(previous) = self.entries.remove(&object_id) {
                    self.total_bytes = self.total_bytes.saturating_sub(previous.data.len());
                }
            } else {
                self.stale_order_entries = self.stale_order_entries.saturating_sub(1);
            }
        }
    }

    fn compact_stale_order_if_needed(&mut self) {
        let compact_threshold = self
            .entries
            .len()
            .saturating_mul(2)
            .max(Self::MIN_STALE_COMPACT_ENTRIES);
        if self.stale_order_entries <= compact_threshold {
            return;
        }

        // Rare compaction trades a bounded latency spike for O(1) ordinary removes.
        self.order.retain(|(object_id, generation)| {
            self.entries
                .get(object_id)
                .is_some_and(|entry| entry.generation == *generation)
        });
        self.stale_order_entries = 0;
    }
}

/// Cached payload store backing the local-first mount runtime.
pub struct CachedStore {
    /// Cache configuration used to build this runtime instance.
    config: CachedStoreConfig,
    /// Active local payload backend for cached object storage.
    local_payload_backend: Arc<dyn LocalPayloadBackend>,
    /// Upload staging layer for payloads awaiting remote publication.
    upload_stage_store: UploadStageStore,
    /// In-memory hot-upload cache state.
    upload_hot: Mutex<UploadHotState>,
    /// Pin accounting for currently referenced cached objects.
    pinned: Mutex<PinnedState>,
    /// Queue that schedules staged uploads for worker dispatch.
    upload_queue: Arc<UploadQueue>,
    /// Bytes currently pending upload per object id.
    pending_upload_bytes_by_object: Mutex<HashMap<ObjectId, u64>>,
    /// Atomic counters for writer staging and upload metrics.
    writer_counters: CachedStoreWriterCounters,
    /// Pipeline metrics exposed for diagnostics and observability.
    metrics: CachePipelineMetrics,
}

impl CachedStore {
    /// Opens the cached store and returns an upload receiver for new pending uploads.
    pub fn open(
        config: CachedStoreConfig,
        local_payload_backend: Arc<dyn LocalPayloadBackend>,
    ) -> io::Result<(Arc<Self>, mpsc::Receiver<()>)> {
        std::fs::create_dir_all(&config.cache_dir)?;
        let upload_hot_bytes = config.upload_hot_bytes;
        let upload_stage_store =
            UploadStageStore::open(LocalPayloadLayout::from_cache_root(&config.cache_dir))
                .map_err(io::Error::other)?;
        let (upload_queue, upload_rx) = UploadQueue::new(&config.cache_dir);
        let cached_store = Arc::new(Self {
            config,
            local_payload_backend,
            upload_stage_store,
            upload_hot: Mutex::new(UploadHotState::new(upload_hot_bytes)),
            pinned: Mutex::new(PinnedState::default()),
            upload_queue,
            pending_upload_bytes_by_object: Mutex::new(HashMap::new()),
            writer_counters: CachedStoreWriterCounters::default(),
            metrics: CachePipelineMetrics::default(),
        });
        Ok((cached_store, upload_rx))
    }

    /// Creates a new writer slice for staging writes into this cache.
    pub fn open_writer_slice(
        self: &Arc<Self>,
        config: CachedStoreWriterConfig,
    ) -> CachedStoreWriterSlice {
        CachedStoreWriterSlice {
            active_start: config.base_offset,
            next_object_id: config.first_object_id,
            created_at: Instant::now(),
            store: Arc::clone(self),
            config,
            pages: VecDeque::new(),
            dirty_len: 0,
        }
    }

    /// Returns a point-in-time snapshot of writer counters.
    pub fn snapshot_writer_counters(&self) -> CachedStoreWriterCountersSnapshot {
        self.writer_counters.snapshot()
    }

    /// Returns the active local payload backend used by this cached store.
    pub fn local_payload_backend(&self) -> &Arc<dyn LocalPayloadBackend> {
        &self.local_payload_backend
    }

    /// Loads an object payload from the local payload backend.
    pub fn get(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.local_payload_backend.get(object_id)
    }

    /// Reads a subrange of an object payload into `out`.
    pub fn read_into(
        &self,
        object_id: ObjectId,
        data_offset: usize,
        out: &mut [u8],
    ) -> Option<usize> {
        self.local_payload_backend
            .read_into(object_id, data_offset, out)
    }

    /// Loads an object payload that is eligible for upload.
    pub fn get_for_upload(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.local_payload_backend.get_for_upload(object_id)
    }

    /// Loads an object payload from the upload stage store.
    pub fn get_staged_for_upload(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.upload_stage_store.get(object_id)
    }

    /// Returns whether an object is present in the upload stage store.
    pub fn has_staged_upload(&self, object_id: ObjectId) -> bool {
        self.upload_stage_store.contains(object_id)
    }

    /// Returns a cached copy of the object payload from the in-memory hot-upload buffer.
    pub fn get_hot_upload(&self, object_id: ObjectId) -> Option<Bytes> {
        self.upload_hot.lock().get(object_id)
    }

    /// Returns whether the local payload backend currently contains this object.
    pub fn contains(&self, object_id: ObjectId) -> bool {
        self.local_payload_backend.contains(object_id)
    }

    /// Advises the local payload backend that the object will be needed soon.
    pub fn advise_will_need(&self, object_id: ObjectId) {
        self.local_payload_backend.advise_will_need(object_id);
    }

    /// Stores an object payload in the local payload backend.
    pub fn put(
        &self,
        object_id: ObjectId,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.put(object_id, data, evictable)
    }

    /// Stores an owned payload object in the local payload backend.
    pub fn put_owned(
        &self,
        put: LocalPayloadPut,
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.put_owned(put)
    }

    /// Stores a batch of owned payload objects in the local payload backend.
    pub fn put_batch(
        &self,
        entries: &[LocalPayloadPut],
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.put_batch(entries)
    }

    /// Flushes data durability for the local payload backend.
    pub fn sync_data(&self) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.sync_data()
    }

    /// Performs pressure/eviction maintenance on the local payload backend.
    pub fn run_pressure_maintenance(&self) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.run_pressure_maintenance()
    }

    /// Marks an object as currently uploading in the local payload backend.
    pub fn mark_uploading(
        &self,
        object_id: ObjectId,
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.mark_uploading(object_id)
    }

    /// Marks an object upload as failed in the local payload backend.
    pub fn mark_upload_failed(
        &self,
        object_id: ObjectId,
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.mark_upload_failed(object_id)
    }

    /// Marks an object as fully uploaded in the local payload backend.
    pub fn mark_uploaded(
        &self,
        object_id: ObjectId,
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.mark_uploaded(object_id)
    }

    /// Removes an object from the local payload backend.
    pub fn remove(
        &self,
        object_id: ObjectId,
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.remove(object_id)
    }

    /// Removes a batch of objects from the local payload backend.
    pub fn remove_batch(
        &self,
        object_ids: &[ObjectId],
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.remove_batch(object_ids)
    }

    /// Links a batch of staged payloads into the upload stage store.
    pub fn link_upload_stage_batch(
        &self,
        object_ids: &[ObjectId],
    ) -> Result<(), UploadStageStoreError> {
        self.upload_stage_store.link_payload_batch(object_ids)
    }

    /// Populates the in-memory hot-upload buffer with payload bytes from `entries`.
    pub fn cache_hot_upload_batch(&self, entries: &[LocalPayloadPut]) {
        if entries.is_empty() {
            return;
        }
        let mut upload_hot = self.upload_hot.lock();
        if upload_hot.budget_bytes == 0 {
            return;
        }
        for entry in entries {
            upload_hot.insert(entry.object_id, entry.data.clone());
        }
    }

    /// Persists detached staged slices to the local cache backend and upload stage store.
    pub fn persist_detached_staged_slices(
        &self,
        entries: Vec<DetachedStagedSlice>,
    ) -> Result<(), CachedStoreWriterError> {
        let Some(first_entry) = entries.first() else {
            return Ok(());
        };

        let started_at = Instant::now();
        let object_ids = entries
            .iter()
            .map(|entry| entry.staged.object_id)
            .collect::<Vec<_>>();
        let first_object_id = first_entry.staged.object_id;
        let puts = entries
            .iter()
            .map(|entry| LocalPayloadPut {
                object_id: entry.staged.object_id,
                data: entry.payload.clone(),
                evictable: false,
            })
            .collect::<Vec<_>>();
        if let Err(source) = self.local_payload_backend.put_batch(&puts) {
            if let Err(cleanup_error) = self.local_payload_backend.remove_batch(&object_ids) {
                warn!(
                    object_count = object_ids.len(),
                    error = ?cleanup_error,
                    "failed to clean up local payloads after staging failure"
                );
            }
            return Err(CachedStoreWriterError::LocalPayload {
                object_id: first_object_id,
                source,
            });
        }
        if let Err(source) = self.upload_stage_store.link_payload_batch(&object_ids) {
            if let Err(cleanup_error) = self.upload_stage_store.remove_batch(&object_ids) {
                warn!(
                    object_count = object_ids.len(),
                    error = ?cleanup_error,
                    "failed to clean up upload stages after linking failure"
                );
            }
            if let Err(cleanup_error) = self.local_payload_backend.remove_batch(&object_ids) {
                warn!(
                    object_count = object_ids.len(),
                    error = ?cleanup_error,
                    "failed to clean up local payloads after linking failure"
                );
            }
            return Err(CachedStoreWriterError::UploadStage {
                object_id: first_object_id,
                source,
            });
        }
        self.cache_hot_upload_batch(&puts);
        let mut total_bytes = 0u64;
        for entry in &entries {
            total_bytes = total_bytes.saturating_add(saturating_u64_from_usize(entry.staged.len));
            if entry.submit_upload {
                self.submit_staged_upload(
                    entry.staged.ino,
                    entry.staged.offset,
                    entry.staged.object_id,
                    entry.staged.content_epoch,
                    entry.staged.len,
                );
            } else {
                self.writer_counters.record_staged(entry.staged.len);
            }
        }
        self.metrics.materialize_batches.record_batch(
            saturating_u64_from_usize(entries.len()),
            total_bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    /// Removes a staged upload payload from the upload stage store.
    pub fn remove_staged_upload(&self, object_id: ObjectId) -> Result<(), UploadStageStoreError> {
        self.upload_stage_store.remove(object_id)
    }

    /// Removes a batch of staged upload payloads from the upload stage store.
    pub fn remove_staged_upload_batch(
        &self,
        object_ids: &[ObjectId],
    ) -> Result<(), UploadStageStoreError> {
        self.upload_stage_store.remove_batch(object_ids)
    }

    /// Removes an entry from the in-memory hot-upload buffer.
    pub fn remove_hot_upload(&self, object_id: ObjectId) {
        self.upload_hot.lock().remove(object_id);
    }

    /// Removes a batch of entries from the in-memory hot-upload buffer.
    pub fn remove_hot_upload_batch(&self, object_ids: &[ObjectId]) {
        let mut upload_hot = self.upload_hot.lock();
        for &object_id in object_ids {
            upload_hot.remove(object_id);
        }
    }

    /// Returns object ids that are considered dirty by the local payload backend.
    pub fn dirty_object_ids(&self) -> Vec<ObjectId> {
        self.local_payload_backend.dirty_object_ids()
    }

    /// Returns the number of objects that are considered dirty by the local payload backend.
    pub fn dirty_object_count(&self) -> usize {
        self.local_payload_backend.dirty_object_count()
    }

    /// Returns the lifecycle state for an object in the local payload backend.
    pub fn lifecycle_state(&self, object_id: ObjectId) -> LocalPayloadLifecycleState {
        self.local_payload_backend.lifecycle_state(object_id)
    }

    /// Returns whether the object is evictable (and not currently pinned).
    pub fn is_evictable(&self, object_id: ObjectId) -> bool {
        self.local_payload_backend.is_evictable(object_id) && !self.is_pinned(object_id)
    }

    /// Returns storage accounting stats for the local payload backend.
    pub fn storage_stats(&self) -> LocalPayloadStorageStats {
        self.local_payload_backend.storage_stats()
    }

    /// Returns per-object stats for the given object id.
    pub fn object_stats(&self, object_id: ObjectId) -> Option<LocalPayloadObjectStats> {
        self.local_payload_backend.object_stats(object_id)
    }

    /// Returns the set of object ids currently known to the local payload backend.
    pub fn object_ids(&self) -> Vec<ObjectId> {
        self.local_payload_backend.object_ids()
    }

    /// Pins an object so it is not evicted while in use or pending upload.
    pub fn pin(&self, object_id: ObjectId) {
        let first_pin = self.pinned.lock().insert(object_id);
        if first_pin {
            self.local_payload_backend.pin(object_id);
        }
    }

    /// Returns whether the object is currently pinned.
    pub fn is_pinned(&self, object_id: ObjectId) -> bool {
        self.pinned.lock().contains(object_id)
    }

    /// Releases a pin on an object, allowing eviction once the final pin is dropped.
    pub fn unpin(&self, object_id: ObjectId) {
        let last_pin = self.pinned.lock().remove(object_id);
        if last_pin {
            self.local_payload_backend.unpin(object_id);
        }
    }

    /// Submits a single upload op to the upload queue and pins the object until terminal.
    pub fn submit_upload(&self, ino: u64, offset: u64, object_id: ObjectId, content_epoch: u64) {
        let payload_len = self
            .local_payload_backend
            .object_stats(object_id)
            .map(|stats| stats.payload_len)
            .unwrap_or(0);
        self.submit_upload_with_payload(ino, offset, offset, object_id, content_epoch, payload_len);
    }

    /// Submits a single upload op with explicit payload geometry.
    pub fn submit_upload_with_payload(
        &self,
        ino: u64,
        offset: u64,
        object_file_offset: u64,
        object_id: ObjectId,
        content_epoch: u64,
        payload_len: u64,
    ) {
        self.pin(object_id);
        self.upload_queue.submit_with_payload(
            ino,
            offset,
            object_file_offset,
            object_id,
            content_epoch,
            payload_len,
        );
        self.metrics
            .upload_queue
            .record_submit(self.upload_queue.pending_count());
    }

    /// Submits a batch of upload ops to the upload queue and pins all referenced objects.
    pub fn submit_uploads_batch(&self, ops: &[(u64, u64, u64, ObjectId, u64, u64)]) {
        let first_pins = {
            let mut pinned = self.pinned.lock();
            pinned.insert_many(ops.iter().map(|(_, _, _, object_id, _, _)| *object_id))
        };
        self.local_payload_backend.pin_batch(&first_pins);
        let object_ids = ops
            .iter()
            .map(|(_, _, _, object_id, _, _)| *object_id)
            .collect::<Vec<_>>();
        if let Err(error) = self.upload_stage_store.link_payload_batch(&object_ids) {
            warn!(
                object_count = object_ids.len(),
                error = %error,
                "failed to link upload-stage payloads before batch upload submit"
            );
        }
        for (ino, offset, object_file_offset, object_id, content_epoch, payload_len) in ops {
            self.upload_queue.submit_with_payload(
                *ino,
                *offset,
                *object_file_offset,
                *object_id,
                *content_epoch,
                *payload_len,
            );
            self.metrics
                .upload_queue
                .record_submit(self.upload_queue.pending_count());
        }
    }

    /// Records completion for an upload op, unpinning the object if it reached terminal.
    pub fn complete_upload(&self, op: &PendingUpload) {
        if self.upload_queue.complete(op) {
            let mut bytes = self.record_upload_terminal(op.object_id);
            if bytes == 0 {
                bytes = self
                    .local_payload_backend
                    .object_stats(op.object_id)
                    .map(|stats| stats.payload_len)
                    .unwrap_or(0);
            }
            self.unpin(op.object_id);
            self.metrics.upload_queue.record_complete(bytes);
        }
    }

    /// Requeues an upload op back into the upload queue.
    pub fn requeue_upload(&self, op: PendingUpload) -> bool {
        let queued = self.upload_queue.requeue(op);
        if queued {
            self.metrics.upload_queue.record_requeue();
        }
        queued
    }

    /// Requeues an upload op after rebasing it onto newer metadata.
    pub fn requeue_rebased_upload(&self, current: &PendingUpload, rebased: PendingUpload) -> bool {
        let queued = self.upload_queue.requeue_rebased(current, rebased);
        if queued {
            self.metrics.upload_queue.record_requeue();
        }
        queued
    }

    /// Cancels an upload op and unpins the object if the cancellation succeeded.
    pub fn cancel_upload(&self, op: &PendingUpload) {
        if self.upload_queue.cancel(op) {
            self.record_upload_terminal(op.object_id);
            self.unpin(op.object_id);
            self.metrics.upload_queue.record_cancel();
        }
    }

    /// Returns the number of currently pending uploads.
    pub fn pending_upload_count(&self) -> usize {
        self.upload_queue.pending_count()
    }

    /// Claims current pending uploads after the upload worker consumes a wakeup.
    pub fn claim_pending_uploads(&self) -> Vec<PendingUpload> {
        self.upload_queue.claim_pending()
    }

    /// Returns whether `op` still matches the authoritative pending upload.
    pub fn is_pending_upload_current(&self, op: &PendingUpload) -> bool {
        self.upload_queue.is_current(op)
    }

    /// Waits until the upload queue becomes idle or `timeout` elapses.
    pub async fn wait_for_uploads_idle(&self, timeout: Duration) -> bool {
        self.upload_queue.wait_for_idle(timeout).await
    }

    /// Returns a snapshot of currently pending uploads.
    pub fn snapshot_pending_uploads(&self) -> Vec<PendingUpload> {
        self.metrics.upload_queue.record_snapshot_pending();
        self.upload_queue.snapshot_pending()
    }

    /// Returns a snapshot of pending uploads for `ino`.
    pub fn snapshot_pending_uploads_for_ino(&self, ino: u64) -> Vec<PendingUpload> {
        self.metrics.upload_queue.record_snapshot_pending();
        self.upload_queue.snapshot_pending_for_ino(ino)
    }

    /// Returns whether the upload queue has any pending uploads for `ino`.
    pub fn has_pending_uploads_for_ino(&self, ino: u64) -> bool {
        self.upload_queue.has_pending_for_ino(ino)
    }

    /// Returns whether the upload queue has a newer pending upload for `ino`.
    pub fn has_pending_uploads_for_ino_newer_than(&self, ino: u64, content_epoch: u64) -> bool {
        self.upload_queue
            .has_pending_for_ino_newer_than(ino, content_epoch)
    }

    /// Flushes the upload queue journal to disk.
    pub async fn flush_upload_journal(&self) {
        self.upload_queue.flush_journal().await;
    }

    /// Returns the configured cache directory.
    pub fn cache_dir(&self) -> &Path {
        &self.config.cache_dir
    }

    /// Returns a snapshot of cache pipeline metrics.
    pub fn cache_pipeline_metrics(&self) -> CachePipelineMetricsSnapshot {
        let mut snapshot = self.metrics.snapshot(self.upload_queue.pending_count());
        let ingress = self.upload_queue.ingress_snapshot();
        snapshot.upload_queue.ingress_depth = ingress.depth;
        snapshot.upload_queue.ingress_capacity = ingress.capacity;
        snapshot.upload_queue.ingress_coalesced = ingress.coalesced;
        snapshot
    }

    /// Resets cache pipeline diagnostics metrics.
    pub fn reset_diagnostics_metrics(&self) {
        self.metrics.reset();
        self.upload_queue.reset_ingress_metrics();
    }

    /// Requests upload queue shutdown and flushes any remaining journal state.
    pub async fn shutdown(&self) {
        self.upload_queue.shutdown();
        self.flush_upload_journal().await;
    }

    fn submit_staged_upload(
        &self,
        ino: u64,
        offset: u64,
        object_id: ObjectId,
        content_epoch: u64,
        payload_len: usize,
    ) {
        self.pending_upload_bytes_by_object
            .lock()
            .insert(object_id, saturating_u64_from_usize(payload_len));
        self.writer_counters.record_staged(payload_len);
        self.writer_counters.record_pending_upload(payload_len);
        self.submit_upload_with_payload(
            ino,
            offset,
            offset,
            object_id,
            content_epoch,
            saturating_u64_from_usize(payload_len),
        );
    }

    fn record_upload_terminal(&self, object_id: ObjectId) -> u64 {
        if let Some(bytes) = self
            .pending_upload_bytes_by_object
            .lock()
            .remove(&object_id)
        {
            self.writer_counters.record_upload_terminal(bytes);
            bytes
        } else {
            0
        }
    }
}

impl CachedStoreWriterSlice {
    /// Writes `data` at absolute `offset`, extending the dirty range as needed.
    pub fn write_at(
        &mut self,
        offset: u64,
        data: &[u8],
    ) -> Result<WriterWriteResult, CachedStoreWriterError> {
        self.write_at_inner(offset, data.len(), |writer, relative, old_len| {
            if relative == old_len {
                writer.append_to_dirty(data);
            } else {
                writer.ensure_len(relative + data.len());
                writer.copy_into_dirty(relative, data);
            }
        })
    }

    /// Writes owned `data` at absolute `offset`, retaining append pages without copying.
    pub fn write_owned_at(
        &mut self,
        offset: u64,
        data: Bytes,
    ) -> Result<WriterWriteResult, CachedStoreWriterError> {
        self.write_at_inner(offset, data.len(), |writer, relative, old_len| {
            if relative >= old_len {
                writer.ensure_len(relative);
                writer.append_owned_to_dirty(data);
            } else {
                writer.ensure_len(relative + data.len());
                writer.copy_into_dirty(relative, data.as_ref());
            }
        })
    }

    fn write_at_inner(
        &mut self,
        offset: u64,
        data_len: usize,
        write: impl FnOnce(&mut Self, usize, usize),
    ) -> Result<WriterWriteResult, CachedStoreWriterError> {
        self.validate_geometry()?;
        if offset < self.active_start {
            return Err(CachedStoreWriterError::WriteBeforeFrozen {
                offset,
                frozen_before: self.active_start,
            });
        }

        let relative = usize::try_from(offset - self.active_start)
            .map_err(|_| CachedStoreWriterError::OffsetTooLarge)?;
        let old_len = self.dirty_bytes();
        let end = relative
            .checked_add(data_len)
            .ok_or(CachedStoreWriterError::OffsetTooLarge)?;
        let absolute_end = end;
        let target_capacity = round_up_to_page(absolute_end, self.config.page_size);
        self.validate_buffer_limit(target_capacity)?;
        write(self, relative, old_len);

        let dirty_delta = end.saturating_sub(old_len);
        if dirty_delta > 0 {
            self.store.writer_counters.add_dirty_bytes(dirty_delta);
        }
        let dirty_bytes = self.dirty_bytes();
        let throttled = self
            .config
            .dirty_byte_limit
            .is_some_and(|limit| dirty_bytes > limit);
        if throttled {
            self.store.writer_counters.record_throttle();
        }

        Ok(WriterWriteResult {
            accepted_bytes: data_len,
            dirty_bytes,
            throttled,
        })
    }

    /// Stages completed blocks up to `target_offset` and returns their metadata.
    pub fn flush_to(
        &mut self,
        target_offset: u64,
    ) -> Result<Vec<StagedSlice>, CachedStoreWriterError> {
        self.validate_geometry()?;
        let mut staged = Vec::new();
        while self.dirty_bytes() >= self.config.block_size
            && self
                .active_start
                .saturating_add(self.config.block_size as u64)
                <= target_offset
        {
            staged.push(self.stage_prefix(self.config.block_size)?);
        }
        Ok(staged)
    }

    /// Returns the number of whole blocks currently available to stage.
    pub fn completed_block_count(&self) -> usize {
        if self.config.block_size == 0 {
            return 0;
        }
        self.dirty_bytes() / self.config.block_size
    }

    /// Stages up to `max_blocks` completed blocks and returns their metadata.
    pub fn flush_completed_blocks(
        &mut self,
        max_blocks: usize,
    ) -> Result<Vec<StagedSlice>, CachedStoreWriterError> {
        self.validate_geometry()?;
        let blocks = self.completed_block_count().min(max_blocks);
        if blocks == 0 {
            return Ok(Vec::new());
        }
        let bytes = blocks.saturating_mul(self.config.block_size);
        let target_offset = self.active_start.saturating_add(bytes as u64);
        self.flush_to(target_offset)
    }

    /// Detaches up to `max_blocks` completed blocks, returning payload bytes without persisting them.
    pub fn detach_completed_blocks(
        &mut self,
        max_blocks: usize,
    ) -> Result<Vec<DetachedStagedSlice>, CachedStoreWriterError> {
        self.validate_geometry()?;
        let blocks = self.completed_block_count().min(max_blocks);
        if blocks == 0 {
            return Ok(Vec::new());
        }
        let bytes = blocks.saturating_mul(self.config.block_size);
        let target_offset = self.active_start.saturating_add(bytes as u64);
        self.detach_to(target_offset)
    }

    /// Stages all remaining dirty bytes (including a final partial block) and returns their metadata.
    pub fn finish(&mut self) -> Result<Vec<StagedSlice>, CachedStoreWriterError> {
        self.validate_geometry()?;
        let mut staged = Vec::new();
        while self.dirty_bytes() > 0 {
            let len = self.dirty_bytes().min(self.config.block_size);
            staged.push(self.stage_prefix(len)?);
        }
        Ok(staged)
    }

    /// Detaches all remaining dirty bytes (including a final partial block) without persisting them.
    pub fn detach_finish(&mut self) -> Result<Vec<DetachedStagedSlice>, CachedStoreWriterError> {
        self.validate_geometry()?;
        let mut staged = Vec::new();
        while self.dirty_bytes() > 0 {
            let len = self.dirty_bytes().min(self.config.block_size);
            staged.push(self.detach_prefix(len)?);
        }
        Ok(staged)
    }

    /// Freezes (stages) pending dirty bytes if the writer is older than `max_age`.
    pub fn freeze_if_older_than(
        &mut self,
        max_age: Duration,
    ) -> Result<Vec<StagedSlice>, CachedStoreWriterError> {
        if self.dirty_bytes() == 0 || self.created_at.elapsed() < max_age {
            return Ok(Vec::new());
        }
        self.finish()
    }

    /// Reads accepted dirty bytes starting at absolute `offset` into `out`.
    pub fn read_accepted(&self, offset: u64, out: &mut [u8]) -> usize {
        if offset < self.active_start {
            return 0;
        }
        let Ok(relative) = usize::try_from(offset - self.active_start) else {
            return 0;
        };
        let dirty_bytes = self.dirty_bytes();
        if relative >= dirty_bytes {
            return 0;
        }
        let len = out.len().min(dirty_bytes - relative);
        self.copy_dirty_into(relative, &mut out[..len]);
        len
    }

    /// Returns the absolute `[start, end)` range currently retained as dirty bytes.
    pub fn accepted_range(&self) -> Option<(u64, u64)> {
        let dirty_bytes = self.dirty_bytes();
        if dirty_bytes == 0 {
            return None;
        }
        Some((
            self.active_start,
            self.active_start.saturating_add(dirty_bytes as u64),
        ))
    }

    /// Returns the current number of dirty bytes retained in memory.
    pub fn dirty_bytes(&self) -> usize {
        self.dirty_len
    }

    /// Overrides the next object id that will be assigned to a staged block.
    pub fn set_next_object_id(&mut self, next_object_id: ObjectId) {
        self.next_object_id = next_object_id;
    }

    fn stage_prefix(&mut self, len: usize) -> Result<StagedSlice, CachedStoreWriterError> {
        let started_at = Instant::now();
        let payload = self.take_prefix(len);
        let can_reuse_hot_payload = self.dirty_bytes() == 0;
        let object_id = self.next_object_id;

        if let Err(source) = self.store.local_payload_backend.put_owned(LocalPayloadPut {
            object_id,
            data: payload.clone(),
            evictable: false,
        }) {
            self.restore_staged_prefix(payload);
            return Err(CachedStoreWriterError::LocalPayload { object_id, source });
        }
        if let Err(source) = self.store.upload_stage_store.link_payload(object_id) {
            self.restore_staged_prefix(payload);
            return Err(CachedStoreWriterError::UploadStage { object_id, source });
        }
        self.cache_hot_upload_if_enabled(object_id, payload, can_reuse_hot_payload);
        self.next_object_id = self.next_object_id.saturating_add(1);
        self.store.metrics.materialize_batches.record_batch(
            1,
            saturating_u64_from_usize(len),
            started_at.elapsed(),
        );
        self.store.writer_counters.subtract_dirty_bytes(len);
        if self.config.submit_upload {
            self.store.submit_staged_upload(
                self.config.ino,
                self.active_start,
                object_id,
                self.config.content_epoch,
                len,
            );
        }

        let staged = StagedSlice {
            ino: self.config.ino,
            offset: self.active_start,
            object_id,
            len,
            content_epoch: self.config.content_epoch,
        };
        self.active_start = self.active_start.saturating_add(len as u64);
        Ok(staged)
    }

    fn detach_to(
        &mut self,
        target_offset: u64,
    ) -> Result<Vec<DetachedStagedSlice>, CachedStoreWriterError> {
        self.validate_geometry()?;
        let mut staged = Vec::new();
        while self.dirty_bytes() >= self.config.block_size
            && self
                .active_start
                .saturating_add(self.config.block_size as u64)
                <= target_offset
        {
            staged.push(self.detach_prefix(self.config.block_size)?);
        }
        Ok(staged)
    }

    fn detach_prefix(&mut self, len: usize) -> Result<DetachedStagedSlice, CachedStoreWriterError> {
        self.validate_geometry()?;
        let payload = self.take_prefix(len);
        let object_id = self.next_object_id;
        let staged = StagedSlice {
            ino: self.config.ino,
            offset: self.active_start,
            object_id,
            len,
            content_epoch: self.config.content_epoch,
        };
        self.next_object_id = self.next_object_id.saturating_add(1);
        self.store.writer_counters.subtract_dirty_bytes(len);
        self.active_start = self.active_start.saturating_add(len as u64);
        Ok(DetachedStagedSlice {
            staged,
            payload,
            submit_upload: self.config.submit_upload,
        })
    }

    fn restore_staged_prefix(&mut self, payload: Bytes) {
        if payload.is_empty() {
            return;
        }
        let mut restored = VecDeque::new();
        let mut copied = 0usize;
        while copied < payload.len() {
            let take = (payload.len() - copied).min(self.config.page_size);
            restored.push_back(WriterPage::Owned(payload.slice(copied..copied + take)));
            copied += take;
        }
        restored.append(&mut self.pages);
        self.pages = restored;
        self.dirty_len = self.dirty_len.saturating_add(payload.len());
    }

    fn cache_hot_upload_if_enabled(
        &self,
        object_id: ObjectId,
        payload: Bytes,
        can_reuse_payload: bool,
    ) {
        let mut upload_hot = self.store.upload_hot.lock();
        if upload_hot.budget_bytes > 0 {
            let payload = if can_reuse_payload {
                payload
            } else {
                Bytes::copy_from_slice(payload.as_ref())
            };
            upload_hot.insert(object_id, payload);
        }
    }

    fn ensure_len(&mut self, target_len: usize) {
        while self.dirty_len < target_len {
            if self
                .pages
                .back()
                .is_none_or(|page| page.len() >= self.config.page_size)
            {
                let remaining = target_len - self.dirty_len;
                self.pages
                    .push_back(WriterPage::Mutable(BytesMut::with_capacity(
                        remaining.min(self.config.page_size),
                    )));
            }
            // INVARIANT: the branch above creates a page whenever the deque is
            // empty or the previous tail cannot accept more bytes.
            #[expect(
                clippy::expect_used,
                reason = "page deque is populated by the preceding branch"
            )]
            let page = self
                .pages
                .back_mut()
                .expect("page exists after push")
                .make_mut(self.config.page_size);
            let remaining = target_len - self.dirty_len;
            let append = remaining.min(self.config.page_size - page.len());
            page.resize(page.len() + append, 0);
            self.dirty_len += append;
        }
    }

    fn append_to_dirty(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            if self
                .pages
                .back()
                .is_none_or(|page| page.len() >= self.config.page_size)
            {
                let capacity = self
                    .config
                    .page_size
                    .min(data.len().max(MIN_WRITER_PAGE_ALLOCATION));
                self.pages
                    .push_back(WriterPage::Mutable(BytesMut::with_capacity(capacity)));
            }
            #[expect(
                clippy::expect_used,
                reason = "page deque is populated by the preceding branch"
            )]
            let page = self
                .pages
                .back_mut()
                .expect("page exists after push")
                .make_mut(self.config.page_size);
            let take = data.len().min(self.config.page_size - page.len());
            page.extend_from_slice(&data[..take]);
            self.dirty_len += take;
            data = &data[take..];
        }
    }

    fn append_owned_to_dirty(&mut self, mut data: Bytes) {
        while !data.is_empty() {
            match self.pages.back_mut() {
                Some(page) if page.len() < self.config.page_size => {
                    let page = page.make_mut(self.config.page_size);
                    let take = data.len().min(self.config.page_size - page.len());
                    let prefix = data.split_to(take);
                    page.extend_from_slice(prefix.as_ref());
                    self.dirty_len += take;
                    continue;
                }
                _ => {}
            }
            if data.len() <= self.config.page_size {
                self.dirty_len += data.len();
                self.pages.push_back(WriterPage::Owned(data));
                break;
            }
            // Keep page boundaries stable while Bytes::split_to shares the
            // original allocation instead of copying the payload.
            let take = data.len().min(self.config.page_size);
            let page = data.split_to(take);
            self.dirty_len += page.len();
            self.pages.push_back(WriterPage::Owned(page));
        }
    }

    fn copy_into_dirty(&mut self, relative: usize, data: &[u8]) {
        let mut write_pos = relative;
        let mut copied = 0usize;
        let mut page_base = 0usize;
        for page in &mut self.pages {
            let page_end = page_base + page.len();
            if write_pos >= page_end {
                page_base = page_end;
                continue;
            }
            let start = write_pos.saturating_sub(page_base);
            let page = page.make_mut(self.config.page_size);
            let take = (page.len() - start).min(data.len() - copied);
            page[start..start + take].copy_from_slice(&data[copied..copied + take]);
            copied += take;
            write_pos += take;
            if copied == data.len() {
                return;
            }
            page_base = page_end;
        }
        debug_assert_eq!(copied, data.len());
    }

    fn copy_dirty_into(&self, relative: usize, out: &mut [u8]) {
        let mut read_pos = relative;
        let mut copied = 0usize;
        let mut page_base = 0usize;
        for page in &self.pages {
            let page_end = page_base + page.len();
            if read_pos >= page_end {
                page_base = page_end;
                continue;
            }
            let start = read_pos.saturating_sub(page_base);
            let take = (page.len() - start).min(out.len() - copied);
            out[copied..copied + take].copy_from_slice(&page.as_slice()[start..start + take]);
            copied += take;
            read_pos += take;
            if copied == out.len() {
                return;
            }
            page_base = page_end;
        }
        debug_assert_eq!(copied, out.len());
    }

    fn copy_prefix_to_bytes(&self, len: usize) -> Bytes {
        let mut payload = BytesMut::with_capacity(len);
        let mut remaining = len;
        for page in &self.pages {
            let take = remaining.min(page.len());
            payload.extend_from_slice(&page.as_slice()[..take]);
            remaining -= take;
            if remaining == 0 {
                break;
            }
        }
        debug_assert_eq!(remaining, 0);
        payload.freeze()
    }

    fn drop_prefix(&mut self, mut len: usize) {
        while len > 0 {
            let Some(front) = self.pages.front_mut() else {
                break;
            };
            let front_len = front.len();
            if len < front_len {
                let _ = front.split_to_bytes(len);
                break;
            }
            self.pages.pop_front();
            len -= front_len;
        }
        while self.pages.front().is_some_and(WriterPage::is_empty) {
            self.pages.pop_front();
        }
    }

    fn take_prefix(&mut self, len: usize) -> Bytes {
        debug_assert!(len <= self.dirty_len);
        if len == 0 {
            return Bytes::new();
        }
        let payload = match self.pages.front().map(WriterPage::len) {
            Some(front_len) if len == front_len => {
                // INVARIANT: front_len came from the current front page.
                #[expect(
                    clippy::expect_used,
                    reason = "front length was read from the current front page"
                )]
                self.pages
                    .pop_front()
                    .expect("front page exists")
                    .into_bytes()
            }
            Some(front_len) if len < front_len => {
                // INVARIANT: front_len came from the current front page.
                #[expect(
                    clippy::expect_used,
                    reason = "front length was read from the current front page"
                )]
                self.pages
                    .front_mut()
                    .expect("front page exists")
                    .split_to_bytes(len)
            }
            _ => {
                let payload = self.copy_prefix_to_bytes(len);
                self.drop_prefix(len);
                payload
            }
        };
        while self.pages.front().is_some_and(WriterPage::is_empty) {
            self.pages.pop_front();
        }
        self.dirty_len = self.dirty_len.saturating_sub(len);
        payload
    }

    fn validate_buffer_limit(&self, requested: usize) -> Result<(), CachedStoreWriterError> {
        let limit = self
            .config
            .max_buffer_bytes
            .unwrap_or(DEFAULT_WRITER_MAX_BUFFER_BYTES);
        if requested > limit {
            return Err(CachedStoreWriterError::BufferLimitExceeded { requested, limit });
        }
        Ok(())
    }

    fn validate_geometry(&self) -> Result<(), CachedStoreWriterError> {
        if self.config.block_size == 0 || self.config.page_size == 0 {
            Err(CachedStoreWriterError::InvalidPageGeometry)
        } else {
            Ok(())
        }
    }
}

impl Drop for CachedStoreWriterSlice {
    fn drop(&mut self) {
        let remaining_dirty = self.dirty_bytes();
        if remaining_dirty > 0 {
            self.store
                .writer_counters
                .subtract_dirty_bytes(remaining_dirty);
        }
    }
}

fn round_up_to_page(len: usize, page_size: usize) -> usize {
    if len == 0 {
        return 0;
    }
    len.div_ceil(page_size) * page_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{open_local_payload_backend, open_local_payload_backend_with_budget};

    fn temp_cached_store(dir: &tempfile::TempDir) -> (Arc<CachedStore>, mpsc::Receiver<()>) {
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open cached store")
    }

    fn temp_cached_store_with_budget(
        dir: &tempfile::TempDir,
        budget_bytes: u64,
    ) -> (Arc<CachedStore>, mpsc::Receiver<()>) {
        let backend = open_local_payload_backend_with_budget(
            dir.path(),
            "00000000-0000-0000-0000-000000000001",
            budget_bytes,
        )
        .expect("open budgeted backend")
        .backend;
        CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open cached store")
    }

    async fn recv_one(store: &CachedStore, rx: &mut mpsc::Receiver<()>) -> PendingUpload {
        rx.recv().await.expect("upload wakeup");
        store
            .claim_pending_uploads()
            .into_iter()
            .next()
            .expect("claimed upload")
    }

    fn try_recv_one(store: &CachedStore, rx: &mut mpsc::Receiver<()>) -> PendingUpload {
        rx.try_recv().expect("upload wakeup");
        store
            .claim_pending_uploads()
            .into_iter()
            .next()
            .expect("claimed upload")
    }

    fn writer_config() -> CachedStoreWriterConfig {
        CachedStoreWriterConfig {
            ino: 7,
            base_offset: 0,
            first_object_id: 10_000,
            content_epoch: 3,
            block_size: 8,
            page_size: 4,
            dirty_byte_limit: None,
            max_buffer_bytes: None,
            submit_upload: true,
        }
    }

    #[derive(Clone)]
    struct RecordedPayloadPut {
        object_id: ObjectId,
        data: Bytes,
        used_owned_entry_point: bool,
    }

    #[derive(Clone)]
    struct RecordingOwnedPayloadBackend {
        inner: Arc<dyn LocalPayloadBackend>,
        puts: Arc<Mutex<Vec<RecordedPayloadPut>>>,
        fail_next_owned_put: Arc<std::sync::atomic::AtomicBool>,
    }

    impl RecordingOwnedPayloadBackend {
        fn new(inner: Arc<dyn LocalPayloadBackend>) -> Self {
            Self {
                inner,
                puts: Arc::new(Mutex::new(Vec::new())),
                fail_next_owned_put: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            }
        }

        fn failing_next_owned_put(inner: Arc<dyn LocalPayloadBackend>) -> Self {
            Self {
                inner,
                puts: Arc::new(Mutex::new(Vec::new())),
                fail_next_owned_put: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }
        }

        fn take_owned_puts(&self) -> Vec<RecordedPayloadPut> {
            std::mem::take(&mut *self.puts.lock())
        }
    }

    impl LocalPayloadBackend for RecordingOwnedPayloadBackend {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn get(&self, object_id: u64) -> Option<Bytes> {
            self.inner.get(object_id)
        }

        fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
            self.inner.read_into(object_id, data_offset, out)
        }

        fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
            self.inner.get_for_upload(object_id)
        }

        fn contains(&self, object_id: u64) -> bool {
            self.inner.contains(object_id)
        }

        fn advise_will_need(&self, object_id: u64) {
            self.inner.advise_will_need(object_id);
        }

        fn put(
            &self,
            object_id: u64,
            data: &[u8],
            evictable: bool,
        ) -> Result<(), crate::local::LocalPayloadBackendError> {
            let owned = Bytes::copy_from_slice(data);
            self.puts.lock().push(RecordedPayloadPut {
                object_id,
                data: owned,
                used_owned_entry_point: false,
            });
            self.inner.put(object_id, data, evictable)
        }

        fn put_owned(
            &self,
            put: LocalPayloadPut,
        ) -> Result<(), crate::local::LocalPayloadBackendError> {
            if self
                .fail_next_owned_put
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                return Err(crate::local::LocalPayloadBackendError::Other(Box::new(
                    std::io::Error::other("injected owned put failure"),
                )));
            }
            self.puts.lock().push(RecordedPayloadPut {
                object_id: put.object_id,
                data: put.data.clone(),
                used_owned_entry_point: true,
            });
            self.inner.put_owned(put)
        }

        fn sync_data(&self) -> Result<(), crate::local::LocalPayloadBackendError> {
            self.inner.sync_data()
        }

        fn run_pressure_maintenance(&self) -> Result<(), crate::local::LocalPayloadBackendError> {
            self.inner.run_pressure_maintenance()
        }

        fn mark_uploading(
            &self,
            object_id: u64,
        ) -> Result<(), crate::local::LocalPayloadBackendError> {
            self.inner.mark_uploading(object_id)
        }

        fn mark_upload_failed(
            &self,
            object_id: u64,
        ) -> Result<(), crate::local::LocalPayloadBackendError> {
            self.inner.mark_upload_failed(object_id)
        }

        fn mark_uploaded(
            &self,
            object_id: u64,
        ) -> Result<(), crate::local::LocalPayloadBackendError> {
            self.inner.mark_uploaded(object_id)
        }

        fn pin(&self, object_id: u64) {
            self.inner.pin(object_id);
        }

        fn unpin(&self, object_id: u64) {
            self.inner.unpin(object_id);
        }

        fn remove(&self, object_id: u64) -> Result<(), crate::local::LocalPayloadBackendError> {
            self.inner.remove(object_id)
        }

        fn remove_batch(
            &self,
            object_ids: &[u64],
        ) -> Result<(), crate::local::LocalPayloadBackendError> {
            self.inner.remove_batch(object_ids)
        }

        fn dirty_object_ids(&self) -> Vec<u64> {
            self.inner.dirty_object_ids()
        }

        fn lifecycle_state(&self, object_id: u64) -> LocalPayloadLifecycleState {
            self.inner.lifecycle_state(object_id)
        }

        fn is_evictable(&self, object_id: u64) -> bool {
            self.inner.is_evictable(object_id)
        }

        fn storage_stats(&self) -> LocalPayloadStorageStats {
            self.inner.storage_stats()
        }

        fn object_stats(&self, object_id: u64) -> Option<LocalPayloadObjectStats> {
            self.inner.object_stats(object_id)
        }

        fn object_ids(&self) -> Vec<u64> {
            self.inner.object_ids()
        }
    }

    #[test]
    fn writer_slice_accepts_in_order_writes_into_pages() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"abcd").expect("write first page");
        writer.write_at(4, b"ef").expect("write tail");

        let mut out = [0; 6];
        assert_eq!(writer.read_accepted(0, &mut out), 6);
        assert_eq!(&out, b"abcdef");
        assert_eq!(writer.dirty_bytes(), 6);
        assert!(cached_store.snapshot_writer_counters().dirty_bytes >= 6);
    }

    #[test]
    fn writer_slice_owned_append_detaches_without_copying_single_page() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());
        let payload = Bytes::from(Vec::from(&b"page"[..]));
        let payload_ptr = payload.as_ptr();

        writer
            .write_owned_at(0, payload)
            .expect("write owned payload");
        let detached = writer.detach_finish().expect("detach payload");

        assert_eq!(detached[0].payload.as_ptr(), payload_ptr);
    }

    #[test]
    fn writer_slice_consecutive_owned_appends_merge_tail_page() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer
            .write_owned_at(0, Bytes::from(Vec::from(&b"ab"[..])))
            .expect("first owned append");
        writer
            .write_owned_at(2, Bytes::from(Vec::from(&b"cd"[..])))
            .expect("second owned append");

        assert_eq!(writer.pages.len(), 1);
        assert!(matches!(writer.pages.front(), Some(WriterPage::Mutable(_))));
        let mut out = [0; 4];
        assert_eq!(writer.read_accepted(0, &mut out), 4);
        assert_eq!(&out, b"abcd");
    }

    #[test]
    fn writer_slice_zero_fills_sparse_gap_before_offset() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(4, b"tail").expect("sparse write");

        let mut out = [0xff; 8];
        assert_eq!(writer.read_accepted(0, &mut out), 8);
        assert_eq!(&out[..4], &[0, 0, 0, 0]);
        assert_eq!(&out[4..], b"tail");
    }

    #[test]
    fn writer_slice_owned_sparse_append_keeps_payload_page() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());
        let payload = Bytes::from(Vec::from(&b"tail"[..]));
        let payload_ptr = payload.as_ptr();

        writer
            .write_owned_at(8, payload)
            .expect("owned sparse write");

        let mut out = [0xff; 12];
        assert_eq!(writer.read_accepted(0, &mut out), 12);
        assert_eq!(&out[..8], &[0; 8]);
        assert_eq!(&out[8..], b"tail");
        let Some(WriterPage::Owned(page)) = writer.pages.back() else {
            panic!("sparse owned append should keep an owned tail page");
        };
        assert_eq!(page.as_ptr(), payload_ptr);
    }

    #[tokio::test]
    async fn writer_slice_full_block_flush_stages_and_registers_upload() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, mut rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"abcdefgh").expect("write full block");
        let staged = writer.flush_to(8).expect("flush full block");

        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].offset, 0);
        assert_eq!(staged[0].len, 8);
        assert_eq!(staged[0].object_id, 10_000);
        assert_eq!(
            cached_store
                .get_staged_for_upload(staged[0].object_id)
                .expect("staged payload")
                .as_ref(),
            b"abcdefgh"
        );
        let op = recv_one(&cached_store, &mut rx).await;
        assert_eq!(op.object_id, staged[0].object_id);
        assert_eq!(op.ino, 7);
        assert_eq!(op.offset, 0);
    }

    #[test]
    fn detached_persist_without_upload_does_not_report_pending_upload_bytes() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, mut rx) = temp_cached_store(&dir);
        let mut config = writer_config();
        config.submit_upload = false;
        let mut writer = cached_store.open_writer_slice(config);

        writer.write_at(0, b"abcdefgh").expect("write full block");
        let detached = writer
            .detach_completed_blocks(1)
            .expect("detach completed block");
        cached_store
            .persist_detached_staged_slices(detached)
            .expect("persist detached block");

        let counters = cached_store.snapshot_writer_counters();
        assert_eq!(counters.staged_bytes, 8);
        assert_eq!(counters.staged_slices, 1);
        assert_eq!(counters.pending_upload_bytes, 0);
        assert_eq!(cached_store.pending_upload_count(), 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn detached_persist_with_upload_counts_pending_bytes_once() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, mut rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"abcdefgh").expect("write full block");
        let detached = writer
            .detach_completed_blocks(1)
            .expect("detach completed block");
        cached_store
            .persist_detached_staged_slices(detached)
            .expect("persist detached block");

        let counters = cached_store.snapshot_writer_counters();
        assert_eq!(counters.staged_bytes, 8);
        assert_eq!(counters.staged_slices, 1);
        assert_eq!(counters.pending_upload_bytes, 8);
        let op = try_recv_one(&cached_store, &mut rx);
        assert_eq!(op.object_id, 10_000);
    }

    #[test]
    fn detached_persist_reuses_payload_for_hot_upload_cache() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let inner_backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        let backend = Arc::new(RecordingOwnedPayloadBackend::new(inner_backend));
        let backend_for_store: Arc<dyn LocalPayloadBackend> = backend.clone();
        let (cached_store, _rx) = CachedStore::open(
            CachedStoreConfig::new(dir.path()).with_upload_hot_bytes(64),
            backend_for_store,
        )
        .expect("open cached store");
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"abcdefgh").expect("write full block");
        let detached = writer
            .detach_completed_blocks(1)
            .expect("detach completed block");
        cached_store
            .persist_detached_staged_slices(detached)
            .expect("persist detached block");

        let recorded = backend.take_owned_puts();
        assert_eq!(recorded.len(), 1);
        let hot = cached_store
            .get_hot_upload(recorded[0].object_id)
            .expect("hot upload payload");
        assert_eq!(recorded[0].data.as_ptr(), hot.as_ptr());
    }

    #[test]
    fn writer_slice_full_block_flush_uses_compact_hot_upload_payload() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let inner_backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        let backend = Arc::new(RecordingOwnedPayloadBackend::new(inner_backend));
        let backend_for_store = Arc::clone(&backend);
        let (cached_store, _rx) = CachedStore::open(
            CachedStoreConfig::new(dir.path()).with_upload_hot_bytes(64),
            backend_for_store,
        )
        .expect("open cached store");
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"abcdefgh").expect("write full block");
        let staged = writer.flush_to(8).expect("flush full block");

        assert_eq!(staged.len(), 1);
        let recorded = backend.take_owned_puts();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].object_id, staged[0].object_id);
        assert_eq!(recorded[0].data.as_ref(), b"abcdefgh");
        assert!(
            recorded[0].used_owned_entry_point,
            "writer staging should hand frozen payload ownership to the backend"
        );

        let hot = cached_store
            .get_hot_upload(staged[0].object_id)
            .expect("hot upload payload");
        assert_eq!(hot.as_ref(), b"abcdefgh");
        assert_eq!(
            recorded[0].data.as_ptr(),
            hot.as_ptr(),
            "hot upload cache should reuse an already detached staged slice"
        );
    }

    #[test]
    fn writer_slice_hot_upload_copies_payload_when_dirty_tail_remains() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let inner_backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        let backend = Arc::new(RecordingOwnedPayloadBackend::new(inner_backend));
        let backend_for_store = Arc::clone(&backend);
        let (cached_store, _rx) = CachedStore::open(
            CachedStoreConfig::new(dir.path()).with_upload_hot_bytes(64),
            backend_for_store,
        )
        .expect("open cached store");
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer
            .write_at(0, b"abcdefghijkl")
            .expect("write block and tail");
        let staged = writer.flush_to(8).expect("flush full block");

        assert_eq!(staged.len(), 1);
        assert_eq!(writer.dirty_bytes(), 4);
        let recorded = backend.take_owned_puts();
        assert_eq!(recorded.len(), 1);
        let hot = cached_store
            .get_hot_upload(staged[0].object_id)
            .expect("hot upload payload");

        assert_eq!(recorded[0].data.as_ref(), b"abcdefgh");
        assert_eq!(hot.as_ref(), b"abcdefgh");
        assert_ne!(
            recorded[0].data.as_ptr(),
            hot.as_ptr(),
            "hot cache should not retain the writer allocation when dirty tail remains"
        );
    }

    #[test]
    fn writer_slice_keeps_dirty_prefix_retryable_after_owned_put_failure() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let inner_backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        let backend = Arc::new(RecordingOwnedPayloadBackend::failing_next_owned_put(
            inner_backend,
        ));
        let (cached_store, _rx) =
            CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open store");
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"abcdefgh").expect("write full block");
        let err = writer.flush_to(8).expect_err("injected put failure");

        assert!(matches!(err, CachedStoreWriterError::LocalPayload { .. }));
        assert_eq!(writer.dirty_bytes(), 8);
        assert_eq!(cached_store.snapshot_writer_counters().dirty_bytes, 8);

        let mut accepted = [0_u8; 8];
        assert_eq!(writer.read_accepted(0, &mut accepted), 8);
        assert_eq!(&accepted, b"abcdefgh");

        let staged = writer.flush_to(8).expect("retry flush");
        assert_eq!(staged.len(), 1);
        assert_eq!(
            cached_store
                .get_staged_for_upload(staged[0].object_id)
                .expect("retry staged payload")
                .as_ref(),
            b"abcdefgh"
        );
        assert_eq!(writer.dirty_bytes(), 0);
    }

    #[test]
    fn writer_slice_keeps_dirty_prefix_retryable_after_upload_stage_failure() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());
        let layout = LocalPayloadLayout::from_cache_root(dir.path());
        let stage_path = layout.upload_stage_object_path(10_000);
        let stage_parent = stage_path.parent().expect("stage parent");
        std::fs::create_dir_all(stage_parent.parent().expect("stage grandparent"))
            .expect("stage grandparent dir");
        std::fs::File::create(stage_parent).expect("stage parent obstruction");

        writer.write_at(0, b"abcdefgh").expect("write full block");
        let err = writer.flush_to(8).expect_err("injected stage failure");

        assert!(matches!(err, CachedStoreWriterError::UploadStage { .. }));
        assert_eq!(writer.dirty_bytes(), 8);
        assert_eq!(cached_store.snapshot_writer_counters().dirty_bytes, 8);

        std::fs::remove_file(stage_parent).expect("remove stage obstruction");
        let staged = writer.flush_to(8).expect("retry flush");

        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].object_id, 10_000);
        assert_eq!(
            cached_store
                .get_staged_for_upload(staged[0].object_id)
                .expect("retry staged payload")
                .as_ref(),
            b"abcdefgh"
        );
        assert_eq!(writer.dirty_bytes(), 0);
    }

    #[test]
    fn writer_slice_flush_completed_blocks_leaves_partial_tail_buffered() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer
            .write_at(0, b"abcdefghijkl")
            .expect("write one full block and one partial tail");
        let staged = writer
            .flush_completed_blocks(usize::MAX)
            .expect("flush completed blocks");

        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].offset, 0);
        assert_eq!(staged[0].len, 8);
        assert_eq!(writer.dirty_bytes(), 4);
        let mut tail = [0_u8; 4];
        assert_eq!(writer.read_accepted(8, &mut tail), 4);
        assert_eq!(&tail, b"ijkl");
    }

    #[test]
    fn writer_slice_sequential_append_preserves_overwrite_path() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"abcd").expect("append first page");
        writer.write_at(4, b"efgh").expect("append second page");
        writer.write_at(8, b"ijkl").expect("append third page");
        writer
            .write_at(4, b"EF")
            .expect("overwrite inside dirty pages");

        let mut out = [0_u8; 12];
        assert_eq!(writer.read_accepted(0, &mut out), out.len());
        assert_eq!(&out, b"abcdEFghijkl");
    }

    #[test]
    fn writer_slice_small_append_preallocates_minimum_page_capacity() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut config = writer_config();
        config.block_size = 1024 * 1024;
        config.page_size = 1024 * 1024;
        let mut writer = cached_store.open_writer_slice(config);

        writer.write_at(0, b"x").expect("small append");

        assert!(writer.pages[0].capacity() >= MIN_WRITER_PAGE_ALLOCATION);
    }

    #[test]
    fn writer_slice_finish_stages_partial_tail() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"abc").expect("write partial tail");
        let staged = writer.finish().expect("finish partial");

        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].len, 3);
        assert_eq!(
            cached_store
                .get_staged_for_upload(staged[0].object_id)
                .expect("staged tail")
                .as_ref(),
            b"abc"
        );
        assert_eq!(writer.dirty_bytes(), 0);
    }

    #[test]
    fn writer_slice_age_threshold_freezes_dirty_tail() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(0, b"aged").expect("write dirty tail");
        let staged = writer
            .freeze_if_older_than(Duration::ZERO)
            .expect("age threshold freeze");

        assert_eq!(staged.len(), 1);
        assert_eq!(
            cached_store
                .get_staged_for_upload(staged[0].object_id)
                .expect("staged aged tail")
                .as_ref(),
            b"aged"
        );
    }

    #[test]
    fn writer_slice_staged_payload_survives_reopen_under_same_cache_root() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let object_id = {
            let (cached_store, _rx) = temp_cached_store(&dir);
            let mut writer = cached_store.open_writer_slice(writer_config());
            writer.write_at(0, b"recover").expect("write payload");
            writer.finish().expect("finish payload")[0].object_id
        };

        let (reopened, _rx) = temp_cached_store(&dir);

        assert_eq!(
            reopened
                .get_staged_for_upload(object_id)
                .expect("recovered staged payload")
                .as_ref(),
            b"recover"
        );
    }

    #[test]
    fn writer_slice_pressure_accounting_reports_throttle_events() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut config = writer_config();
        config.dirty_byte_limit = Some(4);
        let mut writer = cached_store.open_writer_slice(config);

        let result = writer.write_at(0, b"abcde").expect("write over limit");

        assert!(result.throttled);
        let counters = cached_store.snapshot_writer_counters();
        assert_eq!(counters.throttle_events, 1);
        assert!(counters.dirty_bytes >= 5);
    }

    #[test]
    fn writer_slice_rejects_sparse_write_before_allocating_past_buffer_limit() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut config = writer_config();
        config.max_buffer_bytes = Some(8);
        let mut writer = cached_store.open_writer_slice(config);

        let err = writer.write_at(1024, b"x").expect_err("buffer limit");

        assert!(matches!(
            err,
            CachedStoreWriterError::BufferLimitExceeded {
                requested: 1028,
                limit: 8
            }
        ));
        assert_eq!(writer.dirty_bytes(), 0);
        assert_eq!(cached_store.snapshot_writer_counters().dirty_bytes, 0);
    }

    #[test]
    fn writer_slice_default_buffer_limit_rejects_unbounded_sparse_gap() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        let offset = u64::try_from(DEFAULT_WRITER_MAX_BUFFER_BYTES).unwrap() + 1;
        let err = writer
            .write_at(offset, b"x")
            .expect_err("default buffer limit");

        assert!(matches!(
            err,
            CachedStoreWriterError::BufferLimitExceeded { limit, .. }
                if limit == DEFAULT_WRITER_MAX_BUFFER_BYTES
        ));
        assert_eq!(writer.dirty_bytes(), 0);
        assert_eq!(cached_store.snapshot_writer_counters().dirty_bytes, 0);
    }

    #[test]
    fn writer_slice_rejects_write_when_rounded_capacity_exceeds_buffer_limit() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut config = writer_config();
        config.page_size = 16;
        config.max_buffer_bytes = Some(8);
        let mut writer = cached_store.open_writer_slice(config);

        let err = writer
            .write_at(0, b"abcdefgh")
            .expect_err("rounded capacity exceeds limit");

        assert!(matches!(
            err,
            CachedStoreWriterError::BufferLimitExceeded {
                requested: 16,
                limit: 8
            }
        ));
        assert_eq!(writer.dirty_bytes(), 0);
        assert_eq!(cached_store.snapshot_writer_counters().dirty_bytes, 0);
    }

    #[test]
    fn writer_slice_drop_releases_unstaged_dirty_byte_accounting() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);

        {
            let mut writer = cached_store.open_writer_slice(writer_config());
            writer.write_at(0, b"dirty").expect("write dirty tail");
            assert_eq!(cached_store.snapshot_writer_counters().dirty_bytes, 5);
        }

        assert_eq!(cached_store.snapshot_writer_counters().dirty_bytes, 0);
    }

    #[test]
    fn writer_slice_flushes_multiple_blocks_and_keeps_tail_readable() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer
            .write_at(0, b"abcdefghijklmnopqr")
            .expect("write two blocks and tail");
        let staged = writer.flush_to(16).expect("flush full blocks");

        assert_eq!(staged.len(), 2);
        assert_eq!(staged[0].offset, 0);
        assert_eq!(staged[1].offset, 8);
        let mut tail = [0; 2];
        assert_eq!(writer.read_accepted(16, &mut tail), 2);
        assert_eq!(&tail, b"qr");
        assert_eq!(writer.dirty_bytes(), 2);
    }

    #[tokio::test]
    async fn submit_and_complete_tracks_pending_and_pin_state() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, mut rx) = temp_cached_store(&dir);

        cached_store.submit_upload(1, 0, 101, 1);
        let op = recv_one(&cached_store, &mut rx).await;

        assert!(cached_store.is_pinned(101));
        assert_eq!(cached_store.pending_upload_count(), 1);

        cached_store.complete_upload(&op);

        assert!(!cached_store.is_pinned(101));
        assert_eq!(cached_store.pending_upload_count(), 0);
    }

    #[tokio::test]
    async fn cancel_clears_pending_and_pin_state() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, mut rx) = temp_cached_store(&dir);

        cached_store.submit_upload(7, 4096, 202, 3);
        let op = recv_one(&cached_store, &mut rx).await;
        cached_store.cancel_upload(&op);

        assert!(!cached_store.is_pinned(202));
        assert_eq!(cached_store.pending_upload_count(), 0);
        assert!(cached_store.snapshot_pending_uploads().is_empty());
    }

    #[tokio::test]
    async fn requeue_preserves_pending_depth() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, mut rx) = temp_cached_store(&dir);

        cached_store.submit_upload(9, 8192, 303, 4);
        let op = recv_one(&cached_store, &mut rx).await;
        assert!(cached_store.requeue_upload(op.clone()));
        let requeued = recv_one(&cached_store, &mut rx).await;

        assert_eq!(requeued.object_id, 303);
        assert_eq!(cached_store.pending_upload_count(), 1);
        assert!(cached_store.is_pinned(303));
    }

    #[tokio::test]
    async fn submit_uploads_batch_links_payloads_into_upload_stage() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, mut rx) = temp_cached_store(&dir);
        let put = LocalPayloadPut {
            object_id: 404,
            data: bytes::Bytes::from_static(b"batch-staged-payload"),
            evictable: false,
        };
        cached_store
            .put_batch(std::slice::from_ref(&put))
            .expect("persist payload batch");

        cached_store.submit_uploads_batch(&[(
            7,
            1024,
            1024,
            put.object_id,
            3,
            put.data.len() as u64,
        )]);
        let op = recv_one(&cached_store, &mut rx).await;

        assert_eq!(op.object_id, put.object_id);
        assert_eq!(
            cached_store.get_staged_for_upload(put.object_id).as_deref(),
            Some(&b"batch-staged-payload"[..])
        );
    }

    #[test]
    fn upload_stage_roundtrip_through_cached_store_api() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let put = LocalPayloadPut {
            object_id: 404,
            data: bytes::Bytes::from_static(b"staged-payload"),
            evictable: false,
        };
        cached_store
            .put_batch(std::slice::from_ref(&put))
            .expect("persist payload batch");

        cached_store
            .link_upload_stage_batch(&[put.object_id])
            .expect("link upload stage batch");

        assert_eq!(
            cached_store.get_staged_for_upload(put.object_id).as_deref(),
            Some(&b"staged-payload"[..])
        );

        cached_store
            .remove_staged_upload(put.object_id)
            .expect("remove upload stage");

        assert!(cached_store.get_staged_for_upload(put.object_id).is_none());
    }

    #[test]
    fn hot_upload_buffer_evicts_oldest_entries_over_budget() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        let (cached_store, _rx) = CachedStore::open(
            CachedStoreConfig::new(dir.path()).with_upload_hot_bytes(8),
            backend,
        )
        .expect("open cached store");

        cached_store.cache_hot_upload_batch(&[
            LocalPayloadPut {
                object_id: 1,
                data: Bytes::from_static(b"aaaa"),
                evictable: false,
            },
            LocalPayloadPut {
                object_id: 2,
                data: Bytes::from_static(b"bbbb"),
                evictable: false,
            },
            LocalPayloadPut {
                object_id: 3,
                data: Bytes::from_static(b"cccc"),
                evictable: false,
            },
        ]);

        assert!(cached_store.get_hot_upload(1).is_none());
        assert_eq!(
            cached_store.get_hot_upload(2).as_deref(),
            Some(&b"bbbb"[..])
        );
        assert_eq!(
            cached_store.get_hot_upload(3).as_deref(),
            Some(&b"cccc"[..])
        );
    }

    #[test]
    fn hot_upload_batch_skips_work_when_disabled() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        let (cached_store, _rx) = CachedStore::open(
            CachedStoreConfig::new(dir.path()).with_upload_hot_bytes(0),
            backend,
        )
        .expect("open cached store");

        cached_store.cache_hot_upload_batch(&[LocalPayloadPut {
            object_id: 1,
            data: Bytes::from_static(b"aaaa"),
            evictable: false,
        }]);

        assert!(cached_store.get_hot_upload(1).is_none());
    }

    #[test]
    fn hot_upload_buffer_remove_leaves_lru_entry_stale_without_affecting_accounting() {
        let mut state = UploadHotState::new(64);

        state.insert(1, Bytes::from_static(b"aaaa"));
        state.insert(1, Bytes::from_static(b"bbbb"));
        state.insert(2, Bytes::from_static(b"cccc"));
        assert_eq!(state.order.len(), 3);

        state.remove(1);

        assert!(state.get(1).is_none());
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.total_bytes, 4);
        assert_eq!(state.order.len(), 3);
    }

    #[test]
    fn hot_upload_buffer_repeated_remove_leaves_stale_lru_entries_but_evicts_correctly() {
        let mut state = UploadHotState::new(8);

        for object_id in 1..=3 {
            state.insert(object_id, Bytes::from_static(b"aaaa"));
            state.remove(object_id);
        }

        assert_eq!(state.entries.len(), 0);
        assert_eq!(state.total_bytes, 0);
        assert_eq!(state.order.len(), 3);

        state.insert(4, Bytes::from_static(b"bbbb"));
        state.insert(5, Bytes::from_static(b"cccc"));
        state.insert(6, Bytes::from_static(b"dddd"));

        assert!(state.get(4).is_none());
        assert_eq!(state.get(5).as_deref(), Some(&b"cccc"[..]));
        assert_eq!(state.get(6).as_deref(), Some(&b"dddd"[..]));
        assert_eq!(state.total_bytes, 8);
    }

    #[test]
    fn hot_upload_buffer_repeated_remove_bounds_stale_lru_entries() {
        let mut state = UploadHotState::new(1024);

        for object_id in 1..=128 {
            state.insert(object_id, Bytes::from_static(b"aaaa"));
            state.remove(object_id);
        }

        assert_eq!(state.entries.len(), 0);
        assert_eq!(state.total_bytes, 0);
        assert!(state.order.len() <= UploadHotState::MIN_STALE_COMPACT_ENTRIES);
    }

    #[test]
    fn pressure_maintenance_does_not_evict_pinned_uploaded_blocks() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store_with_budget(&dir, 100);

        cached_store.put(20, &[0x01; 30], true).expect("put 20");
        cached_store.put(21, &[0x02; 30], true).expect("put 21");
        cached_store.put(22, &[0x03; 30], true).expect("put 22");
        cached_store.put(23, &[0x04; 5], true).expect("put 23");
        cached_store.pin(20);

        cached_store
            .run_pressure_maintenance()
            .expect("run pressure maintenance");

        assert!(
            cached_store.contains(20),
            "pinned uploaded payload object must survive pressure maintenance"
        );
    }

    #[test]
    fn double_pin_requires_double_unpin_before_eviction_can_resume() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let (cached_store, _rx) = temp_cached_store_with_budget(&dir, 100);

        cached_store.put(20, &[0x01; 30], true).expect("put 20");
        cached_store.put(21, &[0x02; 30], true).expect("put 21");
        cached_store.put(22, &[0x03; 30], true).expect("put 22");
        cached_store.put(23, &[0x04; 5], true).expect("put 23");

        cached_store.pin(20);
        cached_store.pin(20);
        cached_store.unpin(20);

        cached_store
            .run_pressure_maintenance()
            .expect("run pressure maintenance");

        assert!(
            cached_store.contains(20),
            "single unpin must not release an object that still has another pin"
        );

        cached_store.unpin(20);
        assert!(
            cached_store.is_evictable(20),
            "object should become evictable again after the final unpin"
        );
        cached_store.put(24, &[0x05; 30], true).expect("put 24");
        cached_store
            .run_pressure_maintenance()
            .expect("run pressure maintenance after final unpin");

        assert!(
            !cached_store.contains(20),
            "object becomes evictable again after the final unpin"
        );
    }
}

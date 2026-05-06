//! Cached-store runtime for the production Local-First data path.
//!
//! This module owns the active local cache backend, pending writeback queue,
//! object pinning, and cache-pipeline diagnostics used by the mount runtime.
//! Benchmark-only cache helpers live in `benchmark_cache`.

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

use crate::local::{
    LocalPayloadBackend, LocalPayloadLifecycleState, LocalPayloadObjectStats, LocalPayloadPut,
    LocalPayloadStorageStats,
};
use crate::metrics::{CachePipelineMetrics, CachePipelineMetricsSnapshot};
use crate::runtime::LocalPayloadLayout;
use crate::upload_queue::{PendingUpload, UploadQueue};
use crate::upload_stage::{UploadStageStore, UploadStageStoreError};

/// Type alias $name.
pub type ObjectId = u64;
const DEFAULT_WRITER_MAX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
/// Struct $name.
pub struct CachedStoreConfig {
    /// Field $name.
    pub cache_dir: PathBuf,
    /// Field $name.
    pub upload_hot_bytes: usize,
}

#[derive(Debug, Clone)]
/// Struct $name.
pub struct CachedStoreWriterConfig {
    /// Field $name.
    pub ino: u64,
    /// Field $name.
    pub base_offset: u64,
    /// Field $name.
    pub first_object_id: ObjectId,
    /// Field $name.
    pub content_epoch: u64,
    /// Field $name.
    pub block_size: usize,
    /// Field $name.
    pub page_size: usize,
    /// Field $name.
    pub dirty_byte_limit: Option<usize>,
    /// Field $name.
    pub max_buffer_bytes: Option<usize>,
    /// Field $name.
    pub submit_upload: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Struct $name.
pub struct WriterWriteResult {
    /// Field $name.
    pub accepted_bytes: usize,
    /// Field $name.
    pub dirty_bytes: usize,
    /// Field $name.
    pub throttled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct StagedSlice {
    /// Field $name.
    pub ino: u64,
    /// Field $name.
    pub offset: u64,
    /// Field $name.
    pub object_id: ObjectId,
    /// Field $name.
    pub len: usize,
    /// Field $name.
    pub content_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct DetachedStagedSlice {
    /// Field $name.
    pub staged: StagedSlice,
    /// Field $name.
    pub payload: Bytes,
    /// Field $name.
    pub submit_upload: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CachedStoreWriterCountersSnapshot {
    /// Writer-owned bytes retained by the cached-store slice writer, including
    /// zero-filled sparse gaps that will be staged locally.
    pub dirty_bytes: u64,
    /// Field $name.
    pub staged_bytes: u64,
    /// Field $name.
    pub pending_upload_bytes: u64,
    /// Field $name.
    pub staged_slices: u64,
    /// Field $name.
    pub staging_latency_observations: u64,
    /// Field $name.
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
/// Enum $name.
pub enum CachedStoreWriterError {
    #[error("writer page size and block size must be greater than zero")]
    /// Variant $name.
    InvalidPageGeometry,
    #[error("write offset {offset} targets bytes already frozen before {frozen_before}")]
    /// Field $name.
    WriteBeforeFrozen {
        /// The offset value.
        offset: u64,
        /// The frozen_before value.
        frozen_before: u64,
    },
    #[error("writer offset is too large for this platform")]
    /// Variant $name.
    OffsetTooLarge,
    #[error("writer buffer limit exceeded: requested {requested} bytes, limit {limit} bytes")]
    /// Field $name.
    BufferLimitExceeded {
        /// The requested value.
        requested: usize,
        /// The limit value.
        limit: usize,
    },
    #[error("local payload backend failed while staging object {object_id}: {source}")]
    /// Variant $name.
    LocalPayload {
        /// Field $name.
        object_id: ObjectId,
        #[source]
        /// Field $name.
        source: crate::local::LocalPayloadBackendError,
    },
    #[error("upload stage failed while staging object {object_id}: {source}")]
    /// Variant $name.
    UploadStage {
        /// Field $name.
        object_id: ObjectId,
        #[source]
        /// Field $name.
        source: UploadStageStoreError,
    },
}

/// Struct $name.
pub struct CachedStoreWriterSlice {
    store: Arc<CachedStore>,
    config: CachedStoreWriterConfig,
    active_start: u64,
    next_object_id: ObjectId,
    created_at: Instant,
    pages: VecDeque<BytesMut>,
    dirty_len: usize,
}

impl CachedStoreConfig {
    /// Function $name.
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            upload_hot_bytes: 0,
        }
    }

    /// Function $name.
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

/// Struct $name.
pub struct CachedStore {
    config: CachedStoreConfig,
    local_payload_backend: Arc<dyn LocalPayloadBackend>,
    upload_stage_store: UploadStageStore,
    upload_hot: Mutex<UploadHotState>,
    pinned: Mutex<PinnedState>,
    upload_queue: Arc<UploadQueue>,
    pending_upload_bytes_by_object: Mutex<HashMap<ObjectId, u64>>,
    writer_counters: CachedStoreWriterCounters,
    metrics: CachePipelineMetrics,
}

impl CachedStore {
    /// Function $name.
    pub fn open(
        config: CachedStoreConfig,
        local_payload_backend: Arc<dyn LocalPayloadBackend>,
    ) -> io::Result<(Arc<Self>, mpsc::UnboundedReceiver<PendingUpload>)> {
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

    /// Function $name.
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

    /// Function $name.
    pub fn snapshot_writer_counters(&self) -> CachedStoreWriterCountersSnapshot {
        self.writer_counters.snapshot()
    }

    /// Function $name.
    pub fn local_payload_backend(&self) -> &Arc<dyn LocalPayloadBackend> {
        &self.local_payload_backend
    }

    /// Function $name.
    pub fn get(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.local_payload_backend.get(object_id)
    }

    /// Function $name.
    pub fn read_into(
        &self,
        object_id: ObjectId,
        data_offset: usize,
        out: &mut [u8],
    ) -> Option<usize> {
        self.local_payload_backend
            .read_into(object_id, data_offset, out)
    }

    /// Function $name.
    pub fn get_for_upload(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.local_payload_backend.get_for_upload(object_id)
    }

    /// Function $name.
    pub fn get_staged_for_upload(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.upload_stage_store.get(object_id)
    }

    /// Function $name.
    pub fn has_staged_upload(&self, object_id: ObjectId) -> bool {
        self.upload_stage_store.contains(object_id)
    }

    /// Function $name.
    pub fn get_hot_upload(&self, object_id: ObjectId) -> Option<Bytes> {
        self.upload_hot.lock().get(object_id)
    }

    /// Function $name.
    pub fn contains(&self, object_id: ObjectId) -> bool {
        self.local_payload_backend.contains(object_id)
    }

    /// Function $name.
    pub fn advise_will_need(&self, object_id: ObjectId) {
        self.local_payload_backend.advise_will_need(object_id);
    }

    /// Function $name.
    pub fn put(
        &self,
        object_id: ObjectId,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.put(object_id, data, evictable)
    }

    /// Function $name.
    pub fn put_owned(
        &self,
        put: LocalPayloadPut,
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.put_owned(put)
    }

    /// Function $name.
    pub fn put_batch(
        &self,
        entries: &[LocalPayloadPut],
    ) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.put_batch(entries)
    }

    /// Function $name.
    pub fn sync_data(&self) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.sync_data()
    }

    /// Function $name.
    pub fn run_pressure_maintenance(&self) -> Result<(), crate::local::LocalPayloadBackendError> {
        self.local_payload_backend.run_pressure_maintenance()
    }

    /// Function $name.
    pub fn mark_uploading(&self, object_id: ObjectId) {
        self.local_payload_backend.mark_uploading(object_id);
    }

    /// Function $name.
    pub fn mark_upload_failed(&self, object_id: ObjectId) {
        self.local_payload_backend.mark_upload_failed(object_id);
    }

    /// Function $name.
    pub fn mark_uploaded(&self, object_id: ObjectId) {
        self.local_payload_backend.mark_uploaded(object_id);
    }

    /// Function $name.
    pub fn remove(&self, object_id: ObjectId) {
        self.local_payload_backend.remove(object_id);
    }

    /// Function $name.
    pub fn remove_batch(&self, object_ids: &[ObjectId]) {
        self.local_payload_backend.remove_batch(object_ids);
    }

    /// Function $name.
    pub fn link_upload_stage_batch(
        &self,
        object_ids: &[ObjectId],
    ) -> Result<(), UploadStageStoreError> {
        self.upload_stage_store.link_payload_batch(object_ids)
    }

    /// Function $name.
    pub fn cache_hot_upload_batch(&self, entries: &[LocalPayloadPut]) {
        let mut upload_hot = self.upload_hot.lock();
        for entry in entries {
            upload_hot.insert(entry.object_id, entry.data.clone());
        }
    }

    /// Function $name.
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
            self.local_payload_backend.remove_batch(&object_ids);
            return Err(CachedStoreWriterError::LocalPayload {
                object_id: first_object_id,
                source,
            });
        }
        if let Err(source) = self.upload_stage_store.link_payload_batch(&object_ids) {
            self.upload_stage_store.remove_batch(&object_ids);
            self.local_payload_backend.remove_batch(&object_ids);
            return Err(CachedStoreWriterError::UploadStage {
                object_id: first_object_id,
                source,
            });
        }
        {
            let mut upload_hot = self.upload_hot.lock();
            for put in &puts {
                upload_hot.insert(put.object_id, Bytes::copy_from_slice(put.data.as_ref()));
            }
        }
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

    /// Function $name.
    pub fn remove_staged_upload(&self, object_id: ObjectId) {
        self.upload_stage_store.remove(object_id);
    }

    /// Function $name.
    pub fn remove_staged_upload_batch(&self, object_ids: &[ObjectId]) {
        self.upload_stage_store.remove_batch(object_ids);
    }

    /// Function $name.
    pub fn remove_hot_upload(&self, object_id: ObjectId) {
        self.upload_hot.lock().remove(object_id);
    }

    /// Function $name.
    pub fn remove_hot_upload_batch(&self, object_ids: &[ObjectId]) {
        let mut upload_hot = self.upload_hot.lock();
        for &object_id in object_ids {
            upload_hot.remove(object_id);
        }
    }

    /// Function $name.
    pub fn dirty_object_ids(&self) -> Vec<ObjectId> {
        self.local_payload_backend.dirty_object_ids()
    }

    /// Function $name.
    pub fn dirty_object_count(&self) -> usize {
        self.local_payload_backend.dirty_object_count()
    }

    /// Function $name.
    pub fn lifecycle_state(&self, object_id: ObjectId) -> LocalPayloadLifecycleState {
        self.local_payload_backend.lifecycle_state(object_id)
    }

    /// Function $name.
    pub fn is_evictable(&self, object_id: ObjectId) -> bool {
        self.local_payload_backend.is_evictable(object_id) && !self.is_pinned(object_id)
    }

    /// Function $name.
    pub fn storage_stats(&self) -> LocalPayloadStorageStats {
        self.local_payload_backend.storage_stats()
    }

    /// Function $name.
    pub fn object_stats(&self, object_id: ObjectId) -> Option<LocalPayloadObjectStats> {
        self.local_payload_backend.object_stats(object_id)
    }

    /// Function $name.
    pub fn object_ids(&self) -> Vec<ObjectId> {
        self.local_payload_backend.object_ids()
    }

    /// Function $name.
    pub fn pin(&self, object_id: ObjectId) {
        let first_pin = self.pinned.lock().insert(object_id);
        if first_pin {
            self.local_payload_backend.pin(object_id);
        }
    }

    /// Function $name.
    pub fn is_pinned(&self, object_id: ObjectId) -> bool {
        self.pinned.lock().contains(object_id)
    }

    /// Function $name.
    pub fn unpin(&self, object_id: ObjectId) {
        let last_pin = self.pinned.lock().remove(object_id);
        if last_pin {
            self.local_payload_backend.unpin(object_id);
        }
    }

    /// Function $name.
    pub fn submit_upload(&self, ino: u64, offset: u64, object_id: ObjectId, content_epoch: u64) {
        self.pin(object_id);
        self.upload_queue
            .submit(ino, offset, object_id, content_epoch);
        self.metrics
            .upload_queue
            .record_submit(self.upload_queue.pending_count());
    }

    /// Function $name.
    pub fn submit_uploads_batch(&self, ops: &[(u64, u64, ObjectId, u64)]) {
        let first_pins = {
            let mut pinned = self.pinned.lock();
            pinned.insert_many(ops.iter().map(|(_, _, object_id, _)| *object_id))
        };
        self.local_payload_backend.pin_batch(&first_pins);
        for (ino, offset, object_id, content_epoch) in ops {
            self.upload_queue
                .submit(*ino, *offset, *object_id, *content_epoch);
            self.metrics
                .upload_queue
                .record_submit(self.upload_queue.pending_count());
        }
    }

    /// Function $name.
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

    /// Function $name.
    pub fn requeue_upload(&self, op: PendingUpload) -> bool {
        let queued = self.upload_queue.requeue(op);
        if queued {
            self.metrics.upload_queue.record_requeue();
        }
        queued
    }

    /// Function $name.
    pub fn cancel_upload(&self, op: &PendingUpload) {
        if self.upload_queue.cancel(op) {
            self.record_upload_terminal(op.object_id);
            self.unpin(op.object_id);
            self.metrics.upload_queue.record_cancel();
        }
    }

    /// Function $name.
    pub fn pending_upload_count(&self) -> usize {
        self.upload_queue.pending_count()
    }

    /// Function $name.
    pub async fn wait_for_uploads_idle(&self, timeout: Duration) -> bool {
        self.upload_queue.wait_for_idle(timeout).await
    }

    /// Function $name.
    pub fn snapshot_pending_uploads(&self) -> Vec<PendingUpload> {
        self.upload_queue.snapshot_pending()
    }

    /// Function $name.
    pub fn has_pending_uploads_for_ino(&self, ino: u64) -> bool {
        self.upload_queue.has_pending_for_ino(ino)
    }

    /// Function $name.
    pub async fn flush_upload_journal(&self) {
        self.upload_queue.flush_journal().await;
    }

    /// Function $name.
    pub fn cache_dir(&self) -> &Path {
        &self.config.cache_dir
    }

    /// Function $name.
    pub fn cache_pipeline_metrics(&self) -> CachePipelineMetricsSnapshot {
        self.metrics.snapshot(self.upload_queue.pending_count())
    }

    /// Function $name.
    pub fn reset_diagnostics_metrics(&self) {
        self.metrics.reset();
    }

    /// Function $name.
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
        self.submit_upload(ino, offset, object_id, content_epoch);
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
    /// Function $name.
    pub fn write_at(
        &mut self,
        offset: u64,
        data: &[u8],
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
            .checked_add(data.len())
            .ok_or(CachedStoreWriterError::OffsetTooLarge)?;
        let absolute_end = end;
        let target_capacity = round_up_to_page(absolute_end, self.config.page_size);
        self.validate_buffer_limit(target_capacity)?;
        self.ensure_len(absolute_end);
        let write_start = relative;
        self.copy_into_dirty(write_start, data);

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
            accepted_bytes: data.len(),
            dirty_bytes,
            throttled,
        })
    }

    /// Function $name.
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

    /// Function $name.
    pub fn completed_block_count(&self) -> usize {
        if self.config.block_size == 0 {
            return 0;
        }
        self.dirty_bytes() / self.config.block_size
    }

    /// Function $name.
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

    /// Function $name.
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

    /// Function $name.
    pub fn finish(&mut self) -> Result<Vec<StagedSlice>, CachedStoreWriterError> {
        self.validate_geometry()?;
        let mut staged = Vec::new();
        while self.dirty_bytes() > 0 {
            let len = self.dirty_bytes().min(self.config.block_size);
            staged.push(self.stage_prefix(len)?);
        }
        Ok(staged)
    }

    /// Function $name.
    pub fn detach_finish(&mut self) -> Result<Vec<DetachedStagedSlice>, CachedStoreWriterError> {
        self.validate_geometry()?;
        let mut staged = Vec::new();
        while self.dirty_bytes() > 0 {
            let len = self.dirty_bytes().min(self.config.block_size);
            staged.push(self.detach_prefix(len)?);
        }
        Ok(staged)
    }

    /// Function $name.
    pub fn freeze_if_older_than(
        &mut self,
        max_age: Duration,
    ) -> Result<Vec<StagedSlice>, CachedStoreWriterError> {
        if self.dirty_bytes() == 0 || self.created_at.elapsed() < max_age {
            return Ok(Vec::new());
        }
        self.finish()
    }

    /// Function $name.
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

    /// Function $name.
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

    /// Function $name.
    pub fn dirty_bytes(&self) -> usize {
        self.dirty_len
    }

    /// Function $name.
    pub fn set_next_object_id(&mut self, next_object_id: ObjectId) {
        self.next_object_id = next_object_id;
    }

    fn stage_prefix(&mut self, len: usize) -> Result<StagedSlice, CachedStoreWriterError> {
        let started_at = Instant::now();
        let payload = self.take_prefix(len);
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
        self.cache_hot_upload_if_enabled(object_id, &payload);
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
            restored.push_back(BytesMut::from(&payload[copied..copied + take]));
            copied += take;
        }
        restored.append(&mut self.pages);
        self.pages = restored;
        self.dirty_len = self.dirty_len.saturating_add(payload.len());
    }

    fn cache_hot_upload_if_enabled(&self, object_id: ObjectId, payload: &Bytes) {
        let mut upload_hot = self.store.upload_hot.lock();
        if upload_hot.budget_bytes > 0 {
            let payload = Bytes::copy_from_slice(payload.as_ref());
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
                self.pages.push_back(BytesMut::with_capacity(
                    remaining.min(self.config.page_size),
                ));
            }
            let page = self.pages.back_mut().expect("page exists after push");
            let remaining = target_len - self.dirty_len;
            let append = remaining.min(self.config.page_size - page.len());
            page.resize(page.len() + append, 0);
            self.dirty_len += append;
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
            out[copied..copied + take].copy_from_slice(&page[start..start + take]);
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
            payload.extend_from_slice(&page[..take]);
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
            let Some(front_len) = self.pages.front().map(BytesMut::len) else {
                break;
            };
            if len < front_len {
                let _ = self
                    .pages
                    .front_mut()
                    .expect("front page exists")
                    .split_to(len);
                break;
            }
            self.pages.pop_front();
            len -= front_len;
        }
        while self.pages.front().is_some_and(BytesMut::is_empty) {
            self.pages.pop_front();
        }
    }

    fn take_prefix(&mut self, len: usize) -> Bytes {
        debug_assert!(len <= self.dirty_len);
        if len == 0 {
            return Bytes::new();
        }
        let payload = match self.pages.front().map(BytesMut::len) {
            Some(front_len) if len == front_len => {
                self.pages.pop_front().expect("front page exists").freeze()
            }
            Some(front_len) if len < front_len => self
                .pages
                .front_mut()
                .expect("front page exists")
                .split_to(len)
                .freeze(),
            _ => {
                let payload = self.copy_prefix_to_bytes(len);
                self.drop_prefix(len);
                payload
            }
        };
        while self.pages.front().is_some_and(BytesMut::is_empty) {
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

    fn temp_cached_store(
        dir: &tempfile::TempDir,
    ) -> (Arc<CachedStore>, mpsc::UnboundedReceiver<PendingUpload>) {
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open cached store")
    }

    fn temp_cached_store_with_budget(
        dir: &tempfile::TempDir,
        budget_bytes: u64,
    ) -> (Arc<CachedStore>, mpsc::UnboundedReceiver<PendingUpload>) {
        let backend = open_local_payload_backend_with_budget(
            dir.path(),
            "00000000-0000-0000-0000-000000000001",
            budget_bytes,
        )
        .expect("open budgeted backend")
        .backend;
        CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open cached store")
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

        fn mark_uploading(&self, object_id: u64) {
            self.inner.mark_uploading(object_id);
        }

        fn mark_upload_failed(&self, object_id: u64) {
            self.inner.mark_upload_failed(object_id);
        }

        fn mark_uploaded(&self, object_id: u64) {
            self.inner.mark_uploaded(object_id);
        }

        fn pin(&self, object_id: u64) {
            self.inner.pin(object_id);
        }

        fn unpin(&self, object_id: u64) {
            self.inner.unpin(object_id);
        }

        fn remove(&self, object_id: u64) {
            self.inner.remove(object_id);
        }

        fn remove_batch(&self, object_ids: &[u64]) {
            self.inner.remove_batch(object_ids);
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
        let dir = tempfile::tempdir().expect("tempdir");
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
    fn writer_slice_zero_fills_sparse_gap_before_offset() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cached_store, _rx) = temp_cached_store(&dir);
        let mut writer = cached_store.open_writer_slice(writer_config());

        writer.write_at(4, b"tail").expect("sparse write");

        let mut out = [0xff; 8];
        assert_eq!(writer.read_accepted(0, &mut out), 8);
        assert_eq!(&out[..4], &[0, 0, 0, 0]);
        assert_eq!(&out[4..], b"tail");
    }

    #[tokio::test]
    async fn writer_slice_full_block_flush_stages_and_registers_upload() {
        let dir = tempfile::tempdir().expect("tempdir");
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
        let op = rx.recv().await.expect("pending upload");
        assert_eq!(op.object_id, staged[0].object_id);
        assert_eq!(op.ino, 7);
        assert_eq!(op.offset, 0);
    }

    #[test]
    fn detached_persist_without_upload_does_not_report_pending_upload_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let op = rx.try_recv().expect("pending upload");
        assert_eq!(op.object_id, 10_000);
    }

    #[test]
    fn writer_slice_full_block_flush_uses_compact_hot_upload_payload() {
        let dir = tempfile::tempdir().expect("tempdir");
        let inner_backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        let backend = Arc::new(RecordingOwnedPayloadBackend::new(inner_backend));
        let (cached_store, _rx) = CachedStore::open(
            CachedStoreConfig::new(dir.path()).with_upload_hot_bytes(64),
            backend.clone(),
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
        assert_ne!(
            recorded[0].data.as_ptr(),
            hot.as_ptr(),
            "hot upload cache should keep a compact payload instead of retaining writer backing memory"
        );
    }

    #[test]
    fn writer_slice_hot_upload_copies_payload_when_dirty_tail_remains() {
        let dir = tempfile::tempdir().expect("tempdir");
        let inner_backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
                .expect("open backend")
                .backend;
        let backend = Arc::new(RecordingOwnedPayloadBackend::new(inner_backend));
        let (cached_store, _rx) = CachedStore::open(
            CachedStoreConfig::new(dir.path()).with_upload_hot_bytes(64),
            backend.clone(),
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
    fn writer_slice_finish_stages_partial_tail() {
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
        let (cached_store, mut rx) = temp_cached_store(&dir);

        cached_store.submit_upload(1, 0, 101, 1);
        let op = rx.recv().await.expect("queued upload");

        assert!(cached_store.is_pinned(101));
        assert_eq!(cached_store.pending_upload_count(), 1);

        cached_store.complete_upload(&op);

        assert!(!cached_store.is_pinned(101));
        assert_eq!(cached_store.pending_upload_count(), 0);
    }

    #[tokio::test]
    async fn cancel_clears_pending_and_pin_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cached_store, mut rx) = temp_cached_store(&dir);

        cached_store.submit_upload(7, 4096, 202, 3);
        let op = rx.recv().await.expect("queued upload");
        cached_store.cancel_upload(&op);

        assert!(!cached_store.is_pinned(202));
        assert_eq!(cached_store.pending_upload_count(), 0);
        assert!(cached_store.snapshot_pending_uploads().is_empty());
    }

    #[tokio::test]
    async fn requeue_preserves_pending_depth() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (cached_store, mut rx) = temp_cached_store(&dir);

        cached_store.submit_upload(9, 8192, 303, 4);
        let op = rx.recv().await.expect("queued upload");
        assert!(cached_store.requeue_upload(op.clone()));
        let requeued = rx.recv().await.expect("requeued upload");

        assert_eq!(requeued.object_id, 303);
        assert_eq!(cached_store.pending_upload_count(), 1);
        assert!(cached_store.is_pinned(303));
    }

    #[test]
    fn upload_stage_roundtrip_through_cached_store_api() {
        let dir = tempfile::tempdir().expect("tempdir");
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

        cached_store.remove_staged_upload(put.object_id);

        assert!(cached_store.get_staged_for_upload(put.object_id).is_none());
    }

    #[test]
    fn hot_upload_buffer_evicts_oldest_entries_over_budget() {
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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
        let dir = tempfile::tempdir().expect("tempdir");
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

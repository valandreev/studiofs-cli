//! Cached-store runtime for the production Local-First data path.
//!
//! This module owns the active local cache backend, pending writeback queue,
//! object pinning, and cache-pipeline diagnostics used by the mount runtime.
//! Benchmark-only cache helpers live in `benchmark_cache`.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::local::{
    LocalBlockBackend, LocalBlockLifecycleState, LocalBlockObjectStats, LocalBlockPut,
    LocalBlockStorageStats,
};
use crate::metrics::{CachePipelineMetrics, CachePipelineMetricsSnapshot};
use crate::runtime::LocalBlockLayout;
use crate::upload_queue::{PendingUpload, UploadQueue};
use crate::upload_stage::{UploadStageStore, UploadStageStoreError};

pub type ObjectId = u64;

#[derive(Debug, Clone)]
pub struct CachedStoreConfig {
    pub cache_dir: PathBuf,
    pub upload_hot_bytes: usize,
}

impl CachedStoreConfig {
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            upload_hot_bytes: 0,
        }
    }

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
    order: VecDeque<(ObjectId, u64)>,
    entries: HashMap<ObjectId, UploadHotEntry>,
}

impl UploadHotState {
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
        self.next_generation = self.next_generation.saturating_add(1);
        let generation = self.next_generation;
        if let Some(previous) = self
            .entries
            .insert(object_id, UploadHotEntry { data, generation })
        {
            self.total_bytes = self.total_bytes.saturating_sub(previous.data.len());
        }
        self.total_bytes = self
            .total_bytes
            .saturating_add(self.entries[&object_id].data.len());
        self.order.push_back((object_id, generation));
        self.evict_over_budget();
    }

    fn get(&self, object_id: ObjectId) -> Option<Bytes> {
        self.entries.get(&object_id).map(|entry| entry.data.clone())
    }

    fn remove(&mut self, object_id: ObjectId) {
        if let Some(previous) = self.entries.remove(&object_id) {
            self.total_bytes = self.total_bytes.saturating_sub(previous.data.len());
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
                self.remove(object_id);
            }
        }
    }
}

pub struct CachedStore {
    config: CachedStoreConfig,
    local_block_backend: Arc<dyn LocalBlockBackend>,
    upload_stage_store: UploadStageStore,
    upload_hot: Mutex<UploadHotState>,
    pinned: Mutex<PinnedState>,
    upload_queue: Arc<UploadQueue>,
    metrics: CachePipelineMetrics,
}

impl CachedStore {
    pub fn open(
        config: CachedStoreConfig,
        local_block_backend: Arc<dyn LocalBlockBackend>,
    ) -> io::Result<(Arc<Self>, mpsc::UnboundedReceiver<PendingUpload>)> {
        std::fs::create_dir_all(&config.cache_dir)?;
        let upload_hot_bytes = config.upload_hot_bytes;
        let upload_stage_store =
            UploadStageStore::open(LocalBlockLayout::from_cache_root(&config.cache_dir))
                .map_err(io::Error::other)?;
        let (upload_queue, upload_rx) = UploadQueue::new(&config.cache_dir);
        let cached_store = Arc::new(Self {
            config,
            local_block_backend,
            upload_stage_store,
            upload_hot: Mutex::new(UploadHotState::new(upload_hot_bytes)),
            pinned: Mutex::new(PinnedState::default()),
            upload_queue,
            metrics: CachePipelineMetrics::default(),
        });
        Ok((cached_store, upload_rx))
    }

    pub fn local_block_backend(&self) -> &Arc<dyn LocalBlockBackend> {
        &self.local_block_backend
    }

    pub fn get(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.local_block_backend.get(object_id)
    }

    pub fn read_into(
        &self,
        object_id: ObjectId,
        data_offset: usize,
        out: &mut [u8],
    ) -> Option<usize> {
        self.local_block_backend
            .read_into(object_id, data_offset, out)
    }

    pub fn get_for_upload(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.local_block_backend.get_for_upload(object_id)
    }

    pub fn get_staged_for_upload(&self, object_id: ObjectId) -> Option<bytes::Bytes> {
        self.upload_stage_store.get(object_id)
    }

    pub fn has_staged_upload(&self, object_id: ObjectId) -> bool {
        self.upload_stage_store.contains(object_id)
    }

    pub fn get_hot_upload(&self, object_id: ObjectId) -> Option<Bytes> {
        self.upload_hot.lock().get(object_id)
    }

    pub fn contains(&self, object_id: ObjectId) -> bool {
        self.local_block_backend.contains(object_id)
    }

    pub fn advise_will_need(&self, object_id: ObjectId) {
        self.local_block_backend.advise_will_need(object_id);
    }

    pub fn put(
        &self,
        object_id: ObjectId,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), crate::local::LocalBlockBackendError> {
        self.local_block_backend.put(object_id, data, evictable)
    }

    pub fn put_batch(
        &self,
        entries: &[LocalBlockPut],
    ) -> Result<(), crate::local::LocalBlockBackendError> {
        self.local_block_backend.put_batch(entries)
    }

    pub fn sync_data(&self) -> Result<(), crate::local::LocalBlockBackendError> {
        self.local_block_backend.sync_data()
    }

    pub fn run_pressure_maintenance(&self) -> Result<(), crate::local::LocalBlockBackendError> {
        self.local_block_backend.run_pressure_maintenance()
    }

    pub fn mark_uploading(&self, object_id: ObjectId) {
        self.local_block_backend.mark_uploading(object_id);
    }

    pub fn mark_upload_failed(&self, object_id: ObjectId) {
        self.local_block_backend.mark_upload_failed(object_id);
    }

    pub fn mark_uploaded(&self, object_id: ObjectId) {
        self.local_block_backend.mark_uploaded(object_id);
    }

    pub fn remove(&self, object_id: ObjectId) {
        self.local_block_backend.remove(object_id);
    }

    pub fn remove_batch(&self, object_ids: &[ObjectId]) {
        self.local_block_backend.remove_batch(object_ids);
    }

    pub fn link_upload_stage_batch(
        &self,
        object_ids: &[ObjectId],
    ) -> Result<(), UploadStageStoreError> {
        self.upload_stage_store.link_payload_batch(object_ids)
    }

    pub fn cache_hot_upload_batch(&self, entries: &[LocalBlockPut]) {
        let mut upload_hot = self.upload_hot.lock();
        for entry in entries {
            upload_hot.insert(entry.object_id, entry.data.clone());
        }
    }

    pub fn remove_staged_upload(&self, object_id: ObjectId) {
        self.upload_stage_store.remove(object_id);
    }

    pub fn remove_staged_upload_batch(&self, object_ids: &[ObjectId]) {
        self.upload_stage_store.remove_batch(object_ids);
    }

    pub fn remove_hot_upload(&self, object_id: ObjectId) {
        self.upload_hot.lock().remove(object_id);
    }

    pub fn remove_hot_upload_batch(&self, object_ids: &[ObjectId]) {
        let mut upload_hot = self.upload_hot.lock();
        for &object_id in object_ids {
            upload_hot.remove(object_id);
        }
    }

    pub fn dirty_object_ids(&self) -> Vec<ObjectId> {
        self.local_block_backend.dirty_object_ids()
    }

    pub fn dirty_object_count(&self) -> usize {
        self.local_block_backend.dirty_object_count()
    }

    pub fn lifecycle_state(&self, object_id: ObjectId) -> LocalBlockLifecycleState {
        self.local_block_backend.lifecycle_state(object_id)
    }

    pub fn is_evictable(&self, object_id: ObjectId) -> bool {
        self.local_block_backend.is_evictable(object_id) && !self.is_pinned(object_id)
    }

    pub fn storage_stats(&self) -> LocalBlockStorageStats {
        self.local_block_backend.storage_stats()
    }

    pub fn object_stats(&self, object_id: ObjectId) -> Option<LocalBlockObjectStats> {
        self.local_block_backend.object_stats(object_id)
    }

    pub fn object_ids(&self) -> Vec<ObjectId> {
        self.local_block_backend.object_ids()
    }

    pub fn pin(&self, object_id: ObjectId) {
        let first_pin = self.pinned.lock().insert(object_id);
        if first_pin {
            self.local_block_backend.pin(object_id);
        }
    }

    pub fn is_pinned(&self, object_id: ObjectId) -> bool {
        self.pinned.lock().contains(object_id)
    }

    pub fn unpin(&self, object_id: ObjectId) {
        let last_pin = self.pinned.lock().remove(object_id);
        if last_pin {
            self.local_block_backend.unpin(object_id);
        }
    }

    pub fn submit_upload(&self, ino: u64, offset: u64, object_id: ObjectId, content_epoch: u64) {
        self.pin(object_id);
        self.upload_queue
            .submit(ino, offset, object_id, content_epoch);
        self.metrics
            .upload_queue
            .record_submit(self.upload_queue.pending_count());
    }

    pub fn submit_uploads_batch(&self, ops: &[(u64, u64, ObjectId, u64)]) {
        let first_pins = {
            let mut pinned = self.pinned.lock();
            pinned.insert_many(ops.iter().map(|(_, _, object_id, _)| *object_id))
        };
        self.local_block_backend.pin_batch(&first_pins);
        for (ino, offset, object_id, content_epoch) in ops {
            self.upload_queue
                .submit(*ino, *offset, *object_id, *content_epoch);
            self.metrics
                .upload_queue
                .record_submit(self.upload_queue.pending_count());
        }
    }

    pub fn complete_upload(&self, op: &PendingUpload) {
        self.upload_queue.complete(op);
        self.unpin(op.object_id);
        self.metrics.upload_queue.record_complete();
    }

    pub fn requeue_upload(&self, op: PendingUpload) -> bool {
        let queued = self.upload_queue.requeue(op);
        if queued {
            self.metrics.upload_queue.record_requeue();
        }
        queued
    }

    pub fn cancel_upload(&self, op: &PendingUpload) {
        self.upload_queue.cancel(op);
        self.unpin(op.object_id);
        self.metrics.upload_queue.record_cancel();
    }

    pub fn pending_upload_count(&self) -> usize {
        self.upload_queue.pending_count()
    }

    pub async fn wait_for_uploads_idle(&self, timeout: Duration) -> bool {
        self.upload_queue.wait_for_idle(timeout).await
    }

    pub fn snapshot_pending_uploads(&self) -> Vec<PendingUpload> {
        self.upload_queue.snapshot_pending()
    }

    pub fn has_pending_uploads_for_ino(&self, ino: u64) -> bool {
        self.upload_queue.has_pending_for_ino(ino)
    }

    pub async fn flush_upload_journal(&self) {
        self.upload_queue.flush_journal().await;
    }

    pub fn cache_dir(&self) -> &Path {
        &self.config.cache_dir
    }

    pub fn cache_pipeline_metrics(&self) -> CachePipelineMetricsSnapshot {
        self.metrics.snapshot(self.upload_queue.pending_count())
    }

    pub fn reset_diagnostics_metrics(&self) {
        self.metrics.reset();
    }

    pub async fn shutdown(&self) {
        self.upload_queue.shutdown();
        self.flush_upload_journal().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{open_local_block_backend, open_local_block_backend_with_budget};

    fn temp_cached_store(
        dir: &tempfile::TempDir,
    ) -> (Arc<CachedStore>, mpsc::UnboundedReceiver<PendingUpload>) {
        let backend = open_local_block_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
            .expect("open backend")
            .backend;
        CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open cached store")
    }

    fn temp_cached_store_with_budget(
        dir: &tempfile::TempDir,
        budget_bytes: u64,
    ) -> (Arc<CachedStore>, mpsc::UnboundedReceiver<PendingUpload>) {
        let backend = open_local_block_backend_with_budget(
            dir.path(),
            "00000000-0000-0000-0000-000000000001",
            budget_bytes,
        )
        .expect("open budgeted backend")
        .backend;
        CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open cached store")
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
        let put = LocalBlockPut {
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
        let backend = open_local_block_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
            .expect("open backend")
            .backend;
        let (cached_store, _rx) = CachedStore::open(
            CachedStoreConfig::new(dir.path()).with_upload_hot_bytes(8),
            backend,
        )
        .expect("open cached store");

        cached_store.cache_hot_upload_batch(&[
            LocalBlockPut {
                object_id: 1,
                data: Bytes::from_static(b"aaaa"),
                evictable: false,
            },
            LocalBlockPut {
                object_id: 2,
                data: Bytes::from_static(b"bbbb"),
                evictable: false,
            },
            LocalBlockPut {
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
            "pinned uploaded block must survive pressure maintenance"
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

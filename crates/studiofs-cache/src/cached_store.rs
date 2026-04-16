//! Cached-store runtime for the production Local-First data path.
//!
//! This module owns the active local cache backend, pending writeback queue,
//! object pinning, and cache-pipeline diagnostics used by the mount runtime.
//! Benchmark-only cache helpers live in `benchmark_cache`.

use parking_lot::Mutex;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::local::{
    LocalBlockBackend, LocalBlockLifecycleState, LocalBlockObjectStats, LocalBlockStorageStats,
};
use crate::metrics::{CachePipelineMetrics, CachePipelineMetricsSnapshot};
use crate::upload_queue::{PendingUpload, UploadQueue};

pub type ObjectId = u64;

#[derive(Debug, Clone)]
pub struct CachedStoreConfig {
    pub cache_dir: PathBuf,
}

impl CachedStoreConfig {
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
        }
    }
}

#[derive(Debug, Default)]
struct PinnedState {
    objects: HashSet<ObjectId>,
}

impl PinnedState {
    fn insert(&mut self, object_id: ObjectId) {
        self.objects.insert(object_id);
    }

    fn contains(&self, object_id: ObjectId) -> bool {
        self.objects.contains(&object_id)
    }

    fn remove(&mut self, object_id: ObjectId) {
        self.objects.remove(&object_id);
    }
}

pub struct CachedStore {
    config: CachedStoreConfig,
    local_block_backend: Arc<dyn LocalBlockBackend>,
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
        let (upload_queue, upload_rx) = UploadQueue::new(&config.cache_dir);
        let cached_store = Arc::new(Self {
            config,
            local_block_backend,
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
        self.local_block_backend.is_evictable(object_id)
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
        self.pinned.lock().insert(object_id);
    }

    pub fn is_pinned(&self, object_id: ObjectId) -> bool {
        self.pinned.lock().contains(object_id) || self.upload_queue.has_pending_object(object_id)
    }

    pub fn unpin(&self, object_id: ObjectId) {
        self.pinned.lock().remove(object_id);
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
        for (ino, offset, object_id, content_epoch) in ops {
            self.submit_upload(*ino, *offset, *object_id, *content_epoch);
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
    use crate::open_local_block_backend;

    fn temp_cached_store(
        dir: &tempfile::TempDir,
    ) -> (Arc<CachedStore>, mpsc::UnboundedReceiver<PendingUpload>) {
        let backend = open_local_block_backend(dir.path(), "00000000-0000-0000-0000-000000000001")
            .expect("open backend")
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
}

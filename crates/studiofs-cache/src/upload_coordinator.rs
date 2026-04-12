//! Upload-orchestration layer for the production write path.
//!
//! This module owns pending-upload pinning, in-memory queue orchestration, and
//! upload-pipeline metrics. It intentionally does not implement a data cache.

use parking_lot::Mutex;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};

use crate::metrics::{CachePipelineMetrics, CachePipelineMetricsSnapshot};
use crate::upload_queue::{PendingUpload, UploadQueue};

pub type ObjectId = u64;

#[derive(Debug, Clone)]
pub struct UploadCoordinatorConfig {
    pub cache_dir: PathBuf,
}

impl UploadCoordinatorConfig {
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CancelUploadMode {
    KeepBlock,
    RemoveBlock,
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

pub struct UploadCoordinator {
    config: UploadCoordinatorConfig,
    pinned: Mutex<PinnedState>,
    upload_queue: Arc<UploadQueue>,
    metrics: CachePipelineMetrics,
    background_tasks_started: AtomicBool,
    shutdown_notify: Notify,
}

impl UploadCoordinator {
    pub fn open(
        config: UploadCoordinatorConfig,
    ) -> io::Result<(Arc<Self>, mpsc::UnboundedReceiver<PendingUpload>)> {
        std::fs::create_dir_all(&config.cache_dir)?;
        let (upload_queue, upload_rx) = UploadQueue::new(&config.cache_dir);
        let coordinator = Arc::new(Self {
            pinned: Mutex::new(PinnedState::default()),
            upload_queue,
            metrics: CachePipelineMetrics::default(),
            background_tasks_started: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
            config,
        });
        Ok((coordinator, upload_rx))
    }

    pub fn spawn_background_tasks(self: &Arc<Self>) {
        let _ = self.background_tasks_started.compare_exchange(
            false,
            true,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
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

    pub fn cancel_upload(&self, op: &PendingUpload, _mode: CancelUploadMode) {
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

    pub async fn shutdown(&self) {
        self.upload_queue.shutdown();
        self.flush_upload_journal().await;
        self.shutdown_notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn submit_and_complete_tracks_pending_and_pin_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, mut rx) =
            UploadCoordinator::open(UploadCoordinatorConfig::new(dir.path())).expect("open");

        coordinator.submit_upload(1, 0, 101, 1);
        let op = rx.recv().await.expect("queued upload");

        assert!(coordinator.is_pinned(101));
        assert_eq!(coordinator.pending_upload_count(), 1);

        coordinator.complete_upload(&op);

        assert!(!coordinator.is_pinned(101));
        assert_eq!(coordinator.pending_upload_count(), 0);
    }

    #[tokio::test]
    async fn cancel_clears_pending_and_pin_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, mut rx) =
            UploadCoordinator::open(UploadCoordinatorConfig::new(dir.path())).expect("open");

        coordinator.submit_upload(7, 4096, 202, 3);
        let op = rx.recv().await.expect("queued upload");
        coordinator.cancel_upload(&op, CancelUploadMode::RemoveBlock);

        assert!(!coordinator.is_pinned(202));
        assert_eq!(coordinator.pending_upload_count(), 0);
        assert!(coordinator.snapshot_pending_uploads().is_empty());
    }

    #[tokio::test]
    async fn requeue_preserves_pending_depth() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, mut rx) =
            UploadCoordinator::open(UploadCoordinatorConfig::new(dir.path())).expect("open");

        coordinator.submit_upload(9, 8192, 303, 4);
        let op = rx.recv().await.expect("queued upload");
        assert!(coordinator.requeue_upload(op.clone()));
        let requeued = rx.recv().await.expect("requeued upload");

        assert_eq!(requeued.object_id, 303);
        assert_eq!(coordinator.pending_upload_count(), 1);
        assert!(coordinator.is_pinned(303));
    }
}

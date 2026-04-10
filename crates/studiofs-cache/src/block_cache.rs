//! Upload-orchestration layer for `BlockCache` call sites.
//!
//! The old two-tier cache stack has been retired. This module now focuses on
//! pending-upload pinning, queue orchestration, and the small in-memory helper
//! surface still used by benchmark-oriented call sites. The L1 state here is
//! intentionally retained for `test-cache` performance benchmarks; the VFS main
//! data path uses the local block backend directly.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};

use crate::metrics::{CachePipelineMetrics, CachePipelineMetricsSnapshot};
use crate::upload_queue::{PendingUpload, UploadQueue};

pub type ObjectId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2IoPriority {
    Foreground,
    Background,
}

/// Default L1 (RAM) size: 2 GiB.
pub const DEFAULT_L1_SIZE: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct BlockCacheConfig {
    pub l1_size: u64,
    pub cache_dir: PathBuf,
}

impl BlockCacheConfig {
    pub fn new(cache_dir: impl Into<PathBuf>, l1_size: u64) -> Self {
        Self {
            l1_size,
            cache_dir: cache_dir.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheReadLevel {
    L1,
    Miss,
}

#[derive(Debug, Clone)]
pub struct CacheReadTrace {
    pub level: CacheReadLevel,
    pub l1_lookup_latency: Duration,
    pub shim_lookup_latency: Option<Duration>,
}

#[derive(Debug, Clone, Default)]
pub struct BlockCacheStats {
    pub l1_bytes: u64,
}

#[derive(Debug, Default)]
struct L1State {
    by_object: HashMap<ObjectId, Bytes>,
    insertion_order: VecDeque<ObjectId>,
    total_bytes: u64,
}

impl L1State {
    fn get_cloned(&self, object_id: ObjectId) -> Option<Bytes> {
        self.by_object.get(&object_id).cloned()
    }

    fn contains(&self, object_id: ObjectId) -> bool {
        self.by_object.contains_key(&object_id)
    }

    fn insert(&mut self, object_id: ObjectId, data: Bytes, max_bytes: u64) -> u64 {
        let is_new = !self.by_object.contains_key(&object_id);
        if let Some(previous) = self.by_object.remove(&object_id) {
            self.total_bytes = self.total_bytes.saturating_sub(previous.len() as u64);
        }
        self.total_bytes = self.total_bytes.saturating_add(data.len() as u64);
        self.by_object.insert(object_id, data);
        if is_new {
            self.insertion_order.push_back(object_id);
        }
        self.evict_to_limit(max_bytes)
    }

    fn remove(&mut self, object_id: ObjectId) -> bool {
        let Some(removed) = self.by_object.remove(&object_id) else {
            return false;
        };
        self.total_bytes = self.total_bytes.saturating_sub(removed.len() as u64);
        true
    }

    fn evict_to_limit(&mut self, max_bytes: u64) -> u64 {
        let mut evicted = 0u64;
        while self.total_bytes > max_bytes {
            let Some(candidate) = self.insertion_order.pop_front() else {
                break;
            };
            if let Some(removed) = self.by_object.remove(&candidate) {
                self.total_bytes = self.total_bytes.saturating_sub(removed.len() as u64);
                evicted = evicted.saturating_add(1);
            }
        }
        evicted
    }
}

/// Compatibility `BlockCache` wrapper.
///
/// Backed by:
/// - pending-upload pinning state
/// - in-memory `UploadQueue`
/// - compatibility metrics counters
///
/// The L1 map is kept as a benchmark/compatibility-only surface and is not part
/// of the VFS primary data path.
pub struct BlockCache {
    config: BlockCacheConfig,
    l1: Mutex<L1State>,
    l1_max_bytes: AtomicU64,
    pinned: Mutex<HashSet<ObjectId>>,
    upload_queue: Arc<UploadQueue>,
    metrics: CachePipelineMetrics,
    background_tasks_started: AtomicBool,
    shutdown_notify: Notify,
}

impl BlockCache {
    pub fn open(
        config: BlockCacheConfig,
    ) -> io::Result<(Arc<Self>, mpsc::UnboundedReceiver<PendingUpload>)> {
        std::fs::create_dir_all(&config.cache_dir)?;
        let (upload_queue, upload_rx) = UploadQueue::new(&config.cache_dir);
        let cache = Arc::new(Self {
            l1: Mutex::new(L1State::default()),
            l1_max_bytes: AtomicU64::new(config.l1_size.max(1)),
            pinned: Mutex::new(HashSet::new()),
            upload_queue,
            metrics: CachePipelineMetrics::default(),
            background_tasks_started: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
            config,
        });
        Ok((cache, upload_rx))
    }

    pub fn spawn_background_tasks(self: &Arc<Self>) {
        let _ = self.background_tasks_started.compare_exchange(
            false,
            true,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub async fn get(&self, object_id: ObjectId) -> Option<Bytes> {
        self.get_with_trace(object_id).await.0
    }

    pub async fn get_with_trace(&self, object_id: ObjectId) -> (Option<Bytes>, CacheReadTrace) {
        self.get_with_trace_and_priority(object_id, L2IoPriority::Foreground)
            .await
    }

    pub async fn get_with_trace_and_priority(
        &self,
        object_id: ObjectId,
        _priority: L2IoPriority,
    ) -> (Option<Bytes>, CacheReadTrace) {
        let started = Instant::now();
        let l1_hit = self.l1.lock().get_cloned(object_id);
        let l1_latency = started.elapsed();
        match l1_hit {
            Some(data) => (
                Some(data),
                CacheReadTrace {
                    level: CacheReadLevel::L1,
                    l1_lookup_latency: l1_latency,
                    shim_lookup_latency: None,
                },
            ),
            None => (
                None,
                CacheReadTrace {
                    level: CacheReadLevel::Miss,
                    l1_lookup_latency: l1_latency,
                    shim_lookup_latency: None,
                },
            ),
        }
    }

    pub async fn put(&self, object_id: ObjectId, data: Bytes) -> io::Result<()> {
        self.stage_l1(object_id, data.clone());
        Ok(())
    }

    pub async fn remove(&self, object_id: ObjectId) {
        self.remove_with_priority(object_id, L2IoPriority::Foreground)
            .await;
    }

    pub async fn remove_with_priority(&self, object_id: ObjectId, _priority: L2IoPriority) {
        let _ = self.l1.lock().remove(object_id);
    }

    pub async fn remove_batch_with_priority(&self, ids: &[ObjectId], _priority: L2IoPriority) {
        let mut l1 = self.l1.lock();
        for object_id in ids {
            let _ = l1.remove(*object_id);
        }
    }

    pub fn remove_sync(&self, object_id: ObjectId) {
        let _ = self.l1.lock().remove(object_id);
    }

    pub fn contains(&self, object_id: ObjectId) -> bool {
        self.l1.lock().contains(object_id)
    }

    pub fn pin(&self, object_id: ObjectId) {
        self.pinned.lock().insert(object_id);
    }

    pub fn is_pinned(&self, object_id: ObjectId) -> bool {
        self.pinned.lock().contains(&object_id) || self.upload_queue.has_pending_object(object_id)
    }

    pub fn unpin(&self, object_id: ObjectId) {
        self.pinned.lock().remove(&object_id);
    }

    pub fn stage_l1(&self, object_id: ObjectId, data: Bytes) {
        let max_bytes = self.l1_max_bytes.load(Ordering::Acquire).max(1);
        let _ = self.l1.lock().insert(object_id, data, max_bytes);
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

    pub fn cancel_upload(&self, op: &PendingUpload, remove_block: bool) {
        self.upload_queue.cancel(op);
        self.unpin(op.object_id);
        self.metrics.upload_queue.record_cancel();
        if remove_block {
            self.remove_sync(op.object_id);
        }
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

    pub fn stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            l1_bytes: self.l1_bytes(),
        }
    }

    pub fn l1_bytes(&self) -> u64 {
        self.l1.lock().total_bytes
    }

    pub fn l1_max_bytes(&self) -> u64 {
        self.l1_max_bytes.load(Ordering::Acquire)
    }

    pub fn cache_dir(&self) -> &Path {
        &self.config.cache_dir
    }

    pub fn cache_pipeline_metrics(&self) -> CachePipelineMetricsSnapshot {
        self.metrics.snapshot(self.upload_queue.pending_count())
    }

    pub async fn shutdown(&self) {
        self.flush_upload_journal().await;
        self.shutdown_notify.notify_waiters();
    }
}

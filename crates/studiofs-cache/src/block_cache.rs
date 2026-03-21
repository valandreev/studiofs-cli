//! Unified Block Cache — two-level (RAM + NVMe) with integrated upload queue.
//!
//! ```text
//! ┌─────────────┐    ┌──────────────┐    ┌─────────┐
//! │  L1 Memory  │ ←→ │  L2 NVMe     │ ←→ │  S3     │
//! │  (2-4 GB)   │    │  (elastic)   │    │ (cold)  │
//! └─────────────┘    └──────────────┘    └─────────┘
//!        ↕                  ↕
//!    ┌───────────────────────────┐
//!    │    Unified LRU Tracker    │
//!    └───────────────────────────┘
//! ```
//!
//! See `docs/cache-v3-design.md` for full design rationale.

use bytes::Bytes;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, mpsc};
use tracing::{debug, info, warn};

use crate::l1_mem::L1MemCache;
use crate::l2_disk::{self, L2DiskCache};
use crate::lru_tracker::{BlockLocation, CheckpointEntry, LruStats, LruTracker, ObjectId};
use crate::metrics::{CachePipelineMetrics, CachePipelineMetricsSnapshot};
use crate::upload_queue::{PendingUpload, UploadQueue};

// =============================================================================
// Configuration
// =============================================================================

/// Cache checkpoint format.
mod checkpoint {
    pub const MAGIC: &[u8; 6] = b"STFSCM";
    pub const VERSION: u16 = 1;
    /// Per entry: object_id(8) + size(4) + location(1) + pinned(1) + pad(4) = 18 bytes.
    pub const ENTRY_SIZE: usize = 18;
}

type CheckpointSignature = (u32, u32);
type CheckpointWrite = (Vec<u8>, CheckpointSignature);

/// Default L1 (RAM) size: 2 GiB.
pub const DEFAULT_L1_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Default L2 (NVMe) size: 1 TiB.
pub const DEFAULT_L2_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

/// Default free-disk-space ratio threshold for emergency eviction.
pub const DEFAULT_DISK_FREE_RATIO: f64 = 0.10;

/// Fixed worker count for bounded background L2 commits.
pub const DEFAULT_L2_COMMIT_WORKERS: usize = 32;

/// Bounded queue depth for background L2 commit scheduling.
pub const DEFAULT_L2_COMMIT_QUEUE_CAPACITY: usize = 4096;

/// Default timeout for L2 read operations.
pub const DEFAULT_L2_READ_TIMEOUT_MS: u64 = 5_000;

/// Default timeout for L2 write operations.
pub const DEFAULT_L2_WRITE_TIMEOUT_MS: u64 = 5_000;

/// Consecutive L2 read timeouts required to open the read circuit breaker.
pub const DEFAULT_L2_READ_BREAKER_TIMEOUT_THRESHOLD: u32 = 3;

/// L2 read circuit-breaker cooldown after repeated read timeouts.
pub const DEFAULT_L2_READ_BREAKER_COOLDOWN_MS: u64 = 5_000;

/// Maximum attempts per L2 commit job (initial try + retries).
const L2_COMMIT_MAX_ATTEMPTS: u32 = 3;

/// Base retry backoff for transient L2 write failures.
const L2_COMMIT_RETRY_BASE_MS: u64 = 10;

/// Checkpoint interval (seconds).
const CHECKPOINT_INTERVAL_SECS: u64 = 60;

/// Eviction check interval (seconds).
const EVICTION_INTERVAL_SECS: u64 = 10;

/// Configuration for the unified block cache.
#[derive(Debug, Clone)]
pub struct BlockCacheConfig {
    /// L1 RAM budget in bytes.
    pub l1_size: u64,
    /// L2 NVMe budget in bytes.
    pub l2_size: u64,
    /// Root cache directory on NVMe.
    pub cache_dir: PathBuf,
    /// Target free disk space ratio (trigger lazy eviction below this).
    pub disk_free_ratio: f64,
    /// Number of background workers that commit staged blocks to L2.
    pub l2_commit_workers: usize,
    /// Queue capacity for pending L2 commits (backpressure when full).
    pub l2_commit_queue_capacity: usize,
    /// Max time to wait for an L2 read before treating it as a cache miss.
    pub l2_read_timeout: Duration,
    /// Max time to wait for an L2 write before failing the operation.
    pub l2_write_timeout: Duration,
    /// Number of consecutive L2 read timeouts before bypassing L2 temporarily.
    pub l2_read_breaker_timeout_threshold: u32,
    /// How long L2 reads are bypassed once the breaker opens.
    pub l2_read_breaker_cooldown: Duration,
}

impl BlockCacheConfig {
    pub fn new(cache_dir: impl Into<PathBuf>, l1_size: u64, l2_size: u64) -> Self {
        Self {
            l1_size,
            l2_size,
            cache_dir: cache_dir.into(),
            disk_free_ratio: DEFAULT_DISK_FREE_RATIO,
            l2_commit_workers: DEFAULT_L2_COMMIT_WORKERS,
            l2_commit_queue_capacity: DEFAULT_L2_COMMIT_QUEUE_CAPACITY,
            l2_read_timeout: Duration::from_millis(DEFAULT_L2_READ_TIMEOUT_MS),
            l2_write_timeout: Duration::from_millis(DEFAULT_L2_WRITE_TIMEOUT_MS),
            l2_read_breaker_timeout_threshold: DEFAULT_L2_READ_BREAKER_TIMEOUT_THRESHOLD,
            l2_read_breaker_cooldown: Duration::from_millis(DEFAULT_L2_READ_BREAKER_COOLDOWN_MS),
        }
    }
}

#[derive(Debug)]
struct L2CommitJob {
    object_id: ObjectId,
    data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheReadLevel {
    L1,
    L2,
    Miss,
}

#[derive(Debug, Clone, Copy)]
pub struct CacheReadTrace {
    pub level: CacheReadLevel,
    pub l1_lookup_latency: Duration,
    pub l2_lookup_latency: Option<Duration>,
}

#[derive(Debug)]
struct L2CommitScheduler {
    tx: mpsc::Sender<L2CommitJob>,
    rx: Arc<Mutex<mpsc::Receiver<L2CommitJob>>>,
    workers: usize,
    workers_started: AtomicBool,
}

impl L2CommitScheduler {
    fn new(workers: usize, queue_capacity: usize) -> Self {
        let workers = workers.max(1);
        let queue_capacity = queue_capacity.max(1);
        let (tx, rx) = mpsc::channel(queue_capacity);
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
            workers,
            workers_started: AtomicBool::new(false),
        }
    }

    fn queue_capacity(&self) -> usize {
        self.tx.max_capacity()
    }

    fn workers(&self) -> usize {
        self.workers
    }

    fn receiver(&self) -> Arc<Mutex<mpsc::Receiver<L2CommitJob>>> {
        Arc::clone(&self.rx)
    }

    fn mark_started(&self) -> bool {
        self.workers_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

#[derive(Debug)]
struct LoadedCheckpoint {
    entries: Vec<CheckpointEntry>,
    signature: (u32, u32),
}

// =============================================================================
// BlockCache
// =============================================================================

/// Unified two-level block cache with integrated upload queue.
pub struct BlockCache {
    /// L1: in-memory sharded HashMap.
    l1: L1MemCache,
    /// L2: file-per-block on NVMe.
    l2: L2DiskCache,
    /// Unified LRU tracker.
    lru: LruTracker,
    /// Upload queue (writeback journal).
    upload_queue: Arc<UploadQueue>,
    /// Configuration.
    config: BlockCacheConfig,
    /// Signal to wake the background evictor.
    evict_notify: Arc<Notify>,
    /// Signal to stop background tasks.
    shutdown_notify: Arc<Notify>,
    /// Bounded scheduler for asynchronous L2 commits.
    l2_commit_scheduler: L2CommitScheduler,
    /// Cache-pipeline observability for throughput/latency/queue depth.
    metrics: CachePipelineMetrics,
    /// Last persisted checkpoint signature: `(entry_count, crc32(entries))`.
    /// Used to skip redundant tmp+rename cycles when metadata is unchanged.
    last_checkpoint_signature: std::sync::Mutex<Option<(u32, u32)>>,
    /// Monotonic deadline until which L2 reads are bypassed.
    l2_read_breaker_open_until: std::sync::Mutex<Option<Instant>>,
    /// Number of consecutive L2 read timeout events.
    l2_read_timeout_streak: AtomicU32,
}

impl BlockCache {
    /// Open the cache.  Scans existing block files and loads the LRU checkpoint.
    ///
    /// Returns `(cache, upload_rx)` where `upload_rx` yields pending uploads
    /// for the VFS upload worker.
    pub fn open(
        config: BlockCacheConfig,
    ) -> io::Result<(Arc<Self>, mpsc::UnboundedReceiver<PendingUpload>)> {
        let l1 = L1MemCache::new(config.l1_size);
        let l2 = L2DiskCache::open(&config.cache_dir, config.l2_size)?;
        let lru = LruTracker::new();
        let evict_notify = Arc::new(Notify::new());
        let shutdown_notify = Arc::new(Notify::new());
        let l2_commit_scheduler =
            L2CommitScheduler::new(config.l2_commit_workers, config.l2_commit_queue_capacity);

        // Try loading checkpoint, then reconcile with actual files on disk.
        let checkpoint_path = config.cache_dir.join("cache.meta");
        let loaded_checkpoint = load_checkpoint(&checkpoint_path).ok();
        let (checkpoint_entries, initial_checkpoint_signature) = match loaded_checkpoint {
            Some(loaded) => (loaded.entries, Some(loaded.signature)),
            None => (Vec::new(), None),
        };
        let had_checkpoint = !checkpoint_entries.is_empty();

        // Scan disk to find actual block files.
        let on_disk = l2.scan()?;

        if had_checkpoint {
            // Load checkpoint ordering, but only for blocks that actually exist on disk.
            let disk_set: std::collections::HashSet<u64> =
                on_disk.iter().map(|&(id, _)| id).collect();
            let valid: Vec<CheckpointEntry> = checkpoint_entries
                .into_iter()
                .filter(|e| disk_set.contains(&e.object_id))
                .collect();
            lru.load_checkpoint(valid);
            // Add any blocks found on disk that weren't in the checkpoint.
            for &(id, size) in &on_disk {
                if !lru.contains(id) {
                    lru.insert(id, size, BlockLocation::DiskOnly, false);
                }
            }
            debug!(
                checkpoint_valid = lru.len(),
                disk_blocks = on_disk.len(),
                "loaded LRU checkpoint"
            );
        } else {
            // No checkpoint — insert all disk blocks with uniform ordering.
            for &(id, size) in &on_disk {
                lru.insert(id, size, BlockLocation::DiskOnly, false);
            }
            if !on_disk.is_empty() {
                debug!(
                    blocks = on_disk.len(),
                    "rebuilt LRU from disk scan (no checkpoint)"
                );
            }
        }

        // Create upload queue (scans upload/ for crash recovery).
        let (upload_queue, upload_rx) = UploadQueue::new(&config.cache_dir);

        // Pin blocks that have pending uploads.
        for pending in upload_queue.snapshot_pending() {
            lru.pin(pending.object_id);
        }

        let metrics = CachePipelineMetrics::default();
        metrics.l2_commit_scheduler.configure(
            l2_commit_scheduler.workers(),
            l2_commit_scheduler.queue_capacity(),
        );

        let cache = Arc::new(Self {
            l1,
            l2,
            lru,
            upload_queue,
            config,
            evict_notify,
            shutdown_notify,
            l2_commit_scheduler,
            metrics,
            last_checkpoint_signature: std::sync::Mutex::new(initial_checkpoint_signature),
            l2_read_breaker_open_until: std::sync::Mutex::new(None),
            l2_read_timeout_streak: AtomicU32::new(0),
        });

        Ok((cache, upload_rx))
    }

    /// Spawn background tasks (evictor, checkpoint writer).
    ///
    /// Must be called after `open()` from within a tokio runtime.
    pub fn spawn_background_tasks(self: &Arc<Self>) {
        if self.l2_commit_scheduler.mark_started() {
            for worker_id in 0..self.l2_commit_scheduler.workers() {
                let cache = self.clone();
                tokio::spawn(async move {
                    cache.background_l2_commit_worker(worker_id).await;
                });
            }
        }

        let cache = self.clone();
        tokio::spawn(async move {
            cache.background_evictor().await;
        });

        let cache = self.clone();
        tokio::spawn(async move {
            cache.background_checkpoint().await;
        });
    }

    // =========================================================================
    // Core API
    // =========================================================================

    /// Read a block.  Returns `None` on cache miss.
    ///
    /// Lookup: L1 (RAM) → L2 (NVMe file) → None.
    pub async fn get(&self, object_id: ObjectId) -> Option<Bytes> {
        self.get_with_trace(object_id).await.0
    }

    /// Read a block and return per-level lookup trace for observability.
    ///
    /// This does not change cache behavior; it only exposes lookup timing and
    /// the level that served the payload.
    pub async fn get_with_trace(&self, object_id: ObjectId) -> (Option<Bytes>, CacheReadTrace) {
        let l1_started = Instant::now();
        if let Some(data) = self.l1.get(object_id) {
            let trace = CacheReadTrace {
                level: CacheReadLevel::L1,
                l1_lookup_latency: l1_started.elapsed(),
                l2_lookup_latency: None,
            };
            self.lru.touch(object_id);
            return (Some(data), trace);
        }
        let l1_lookup_latency = l1_started.elapsed();

        let l2_started = Instant::now();
        if self.is_l2_read_breaker_open() {
            return (
                None,
                CacheReadTrace {
                    level: CacheReadLevel::Miss,
                    l1_lookup_latency,
                    l2_lookup_latency: Some(l2_started.elapsed()),
                },
            );
        }

        let expected_size = self.lru.size(object_id);
        let l2_result = tokio::time::timeout(
            self.config.l2_read_timeout,
            self.l2.get_with_size_hint(object_id, expected_size),
        )
        .await;
        let data_opt = match l2_result {
            Ok(data) => {
                self.record_l2_read_completion();
                data
            }
            Err(_) => {
                self.record_l2_read_timeout(object_id);
                warn!(
                    object_id,
                    timeout_ms = self.config.l2_read_timeout.as_millis(),
                    "L2 read timed out; falling back to cache miss"
                );
                None
            }
        };
        let Some(data) = data_opt else {
            return (
                None,
                CacheReadTrace {
                    level: CacheReadLevel::Miss,
                    l1_lookup_latency,
                    l2_lookup_latency: Some(l2_started.elapsed()),
                },
            );
        };
        let l2_lookup_latency = l2_started.elapsed();
        self.metrics
            .l2_read
            .record(data.len() as u64, l2_lookup_latency);

        // Promote to L1 if budget allows (or near budget).
        self.promote_to_l1(object_id, data.clone());
        self.lru.touch(object_id);

        (
            Some(data),
            CacheReadTrace {
                level: CacheReadLevel::L2,
                l1_lookup_latency,
                l2_lookup_latency: Some(l2_lookup_latency),
            },
        )
    }

    fn is_l2_read_breaker_open(&self) -> bool {
        let now = Instant::now();
        let mut guard = match self.l2_read_breaker_open_until.lock() {
            Ok(guard) => guard,
            Err(_) => return false,
        };
        match *guard {
            Some(until) if now < until => true,
            Some(_) => {
                *guard = None;
                false
            }
            None => false,
        }
    }

    fn record_l2_read_completion(&self) {
        self.l2_read_timeout_streak.store(0, Ordering::Relaxed);
    }

    fn record_l2_read_timeout(&self, object_id: ObjectId) {
        let streak = self.l2_read_timeout_streak.fetch_add(1, Ordering::Relaxed) + 1;
        let threshold = self.config.l2_read_breaker_timeout_threshold.max(1);
        if streak < threshold {
            return;
        }
        self.l2_read_timeout_streak.store(0, Ordering::Relaxed);
        let cooldown_ms = self
            .config
            .l2_read_breaker_cooldown
            .as_millis()
            .clamp(1, u64::MAX as u128) as u64;
        let open_until = Instant::now() + Duration::from_millis(cooldown_ms);
        if let Ok(mut guard) = self.l2_read_breaker_open_until.lock() {
            *guard = Some(open_until);
        }
        warn!(
            object_id,
            threshold, cooldown_ms, "L2 read circuit breaker opened after repeated read timeouts"
        );
    }

    /// Read a block synchronously (for upload workers that need re-read).
    pub fn get_sync(&self, object_id: ObjectId) -> Option<Bytes> {
        if let Some(data) = self.l1.get(object_id) {
            return Some(data);
        }
        self.l2.get_sync(object_id)
    }

    /// Read a block synchronously from L2 only (never from L1).
    pub fn get_sync_l2_only(&self, object_id: ObjectId) -> Option<Bytes> {
        self.l2.get_sync(object_id)
    }

    /// Write a block to cache (L1 + L2).
    ///
    /// The block is written to the NVMe file and kept in RAM.
    pub async fn put(&self, object_id: ObjectId, data: Bytes) -> io::Result<()> {
        let size = data.len() as u32;

        // Write to L2 (NVMe file) — async, non-blocking.
        let started = Instant::now();
        match tokio::time::timeout(
            self.config.l2_write_timeout,
            self.l2.put_bytes(object_id, data.clone()),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "L2 write timed out after {}ms",
                        self.config.l2_write_timeout.as_millis()
                    ),
                ));
            }
        }
        self.metrics
            .l2_write
            .record(data.len() as u64, started.elapsed());

        // Insert into L1 (RAM).
        self.l1.insert(object_id, data);

        // Track in LRU.
        self.lru
            .insert(object_id, size, BlockLocation::MemoryAndDisk, false);

        // Check if eviction is needed.
        if self.l1.over_budget() || self.l2.total_bytes() > self.l2.max_bytes() {
            self.evict_notify.notify_one();
        }

        Ok(())
    }

    /// Remove a block from all cache levels.
    pub async fn remove(&self, object_id: ObjectId) {
        self.l1.remove(object_id);
        self.l2.remove(object_id).await;
        self.lru.remove(object_id);
    }

    /// Remove a block synchronously (for use in sync eviction paths).
    pub fn remove_sync(&self, object_id: ObjectId) {
        self.l1.remove(object_id);
        self.l2.remove_sync(object_id);
        self.lru.remove(object_id);
    }

    /// Check if a block exists in any cache level.
    pub fn contains(&self, object_id: ObjectId) -> bool {
        self.lru.contains(object_id)
    }

    /// Pin a block (prevent eviction until upload completes).
    pub fn pin(&self, object_id: ObjectId) {
        self.lru.pin(object_id);
    }

    /// Check if a block is pinned (has a pending upload).
    pub fn is_pinned(&self, object_id: ObjectId) -> bool {
        self.lru.is_pinned(object_id)
    }

    /// Unpin a block (allow eviction).
    pub fn unpin(&self, object_id: ObjectId) {
        self.lru.unpin(object_id);
    }

    // =========================================================================
    // Two-phase staging (Plan A)
    // =========================================================================

    /// **Phase 1 — instant.** Insert a block into L1 RAM only, track it in
    /// the LRU as [`MemoryOnly`](BlockLocation::MemoryOnly), and pin it so
    /// the evictor cannot discard it before L2 is written and the upload
    /// completes.
    ///
    /// Called from `drain_dirty_mem` to make blocks immediately readable
    /// without waiting for NVMe I/O.
    pub fn stage_l1(&self, object_id: ObjectId, data: Bytes) {
        let size = data.len() as u32;
        self.l1.insert(object_id, data);
        self.lru
            .insert(object_id, size, BlockLocation::MemoryOnly, true);
    }

    /// **Phase 2 — background.** Write a block to L2 (NVMe) and promote its
    /// LRU location from `MemoryOnly` to `MemoryAndDisk`.
    ///
    /// The write is routed through the bounded L2 scheduler; this method
    /// applies backpressure when the scheduler queue is saturated.
    /// Does NOT unpin — the block stays pinned until the S3 upload completes.
    pub async fn commit_l2(&self, object_id: ObjectId, data: &[u8]) -> io::Result<()> {
        self.enqueue_l2_commit(object_id, Bytes::copy_from_slice(data))
            .await
    }

    /// **Phase 2 — background batch.** Enqueue multiple L2 commits through the
    /// bounded scheduler.
    ///
    /// Backpressure is applied when the queue is full and this call awaits
    /// worker progress.
    pub async fn commit_l2_batch(&self, blocks: &[(ObjectId, Bytes)]) -> io::Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        self.metrics.materialize_batches.enter_inflight();
        let batch_started = Instant::now();
        let total_bytes = blocks
            .iter()
            .map(|(_, data)| data.len() as u64)
            .sum::<u64>();

        let enqueue_result = async {
            for (object_id, data) in blocks {
                self.enqueue_l2_commit(*object_id, data.clone()).await?;
            }
            Ok::<(), io::Error>(())
        }
        .await;

        self.metrics.materialize_batches.record_batch(
            blocks.len() as u64,
            total_bytes,
            batch_started.elapsed(),
        );
        self.metrics.materialize_batches.leave_inflight();

        enqueue_result
    }

    async fn enqueue_l2_commit(&self, object_id: ObjectId, data: Bytes) -> io::Result<()> {
        let job = L2CommitJob { object_id, data };
        match self.l2_commit_scheduler.tx.try_reserve() {
            Ok(permit) => {
                self.metrics.l2_commit_scheduler.record_enqueue();
                permit.send(job);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.l2_commit_scheduler.record_backpressure_event();
                let backpressure_started = Instant::now();
                let permit = self.l2_commit_scheduler.tx.reserve().await.map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "l2 commit queue closed")
                })?;
                self.metrics
                    .l2_commit_scheduler
                    .record_backpressure_wait(backpressure_started.elapsed());
                self.metrics.l2_commit_scheduler.record_enqueue();
                permit.send(job);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "l2 commit queue closed",
            )),
        }
    }

    // =========================================================================
    // Upload queue delegation
    // =========================================================================

    /// Submit a block for async S3 upload.  Pins it in the LRU.
    pub fn submit_upload(&self, ino: u64, offset: u64, object_id: ObjectId, content_epoch: u64) {
        self.lru.pin(object_id);
        self.upload_queue
            .submit(ino, offset, object_id, content_epoch);
        self.metrics
            .upload_queue
            .record_submit(self.upload_queue.pending_count());
    }

    /// Submit multiple blocks for async S3 upload in a batch.
    ///
    /// Each block is pinned in the LRU and a journal entry is written.
    /// More efficient than calling `submit_upload` in a loop because it
    /// avoids re-acquiring the LRU shard lock repeatedly.
    pub fn submit_uploads_batch(&self, ops: &[(u64, u64, ObjectId, u64)]) {
        for &(ino, offset, object_id, content_epoch) in ops {
            self.lru.pin(object_id);
            self.upload_queue
                .submit(ino, offset, object_id, content_epoch);
            self.metrics
                .upload_queue
                .record_submit(self.upload_queue.pending_count());
        }
    }

    /// Mark an upload as complete.  Unpins the block.  Block stays cached.
    pub fn complete_upload(&self, op: &PendingUpload) {
        self.lru.unpin(op.object_id);
        self.upload_queue.complete(op);
        self.metrics.upload_queue.record_complete();
    }

    /// Requeue a failed upload for retry.
    pub fn requeue_upload(&self, op: PendingUpload) -> bool {
        let queued = self.upload_queue.requeue(op);
        if queued {
            self.metrics.upload_queue.record_requeue();
        }
        queued
    }

    /// Cancel a pending upload: unpin the block, remove the journal entry,
    /// and decrement the pending count.  Optionally removes the block data
    /// from cache entirely (used when the owning inode was deleted).
    pub fn cancel_upload(&self, op: &PendingUpload, remove_block: bool) {
        self.lru.unpin(op.object_id);
        self.upload_queue.cancel(op);
        self.metrics.upload_queue.record_cancel();
        if remove_block {
            self.remove_sync(op.object_id);
        }
    }

    /// Number of pending uploads.
    pub fn pending_upload_count(&self) -> usize {
        self.upload_queue.pending_count()
    }

    /// Wait until all pending uploads drain or timeout.
    pub async fn wait_for_uploads_idle(&self, timeout: Duration) -> bool {
        self.upload_queue.wait_for_idle(timeout).await
    }

    /// Snapshot pending uploads from the live in-memory index.
    pub fn snapshot_pending_uploads(&self) -> Vec<PendingUpload> {
        self.upload_queue.snapshot_pending()
    }

    pub async fn flush_upload_journal(&self) {
        self.upload_queue.flush_journal().await;
    }

    // =========================================================================
    // Resize
    // =========================================================================

    /// Resize the L2 disk budget.  Triggers eviction if shrunk.
    pub fn resize_l2(&self, new_max: u64) {
        self.l2.set_max_bytes(new_max);
        if self.l2.total_bytes() > new_max {
            self.evict_notify.notify_one();
        }
        info!(
            new_max_gb = new_max / (1024 * 1024 * 1024),
            "L2 cache resized"
        );
    }

    // =========================================================================
    // Stats
    // =========================================================================

    /// Get LRU statistics.
    pub fn stats(&self) -> LruStats {
        self.lru.stats()
    }

    /// Total bytes in L1 (RAM).
    pub fn l1_bytes(&self) -> u64 {
        self.l1.total_bytes()
    }

    /// Total bytes in L2 (NVMe).
    pub fn l2_bytes(&self) -> u64 {
        self.l2.total_bytes()
    }

    /// Cache directory.
    pub fn cache_dir(&self) -> &Path {
        self.l2.cache_dir()
    }

    /// Snapshot cache-pipeline metrics for debugging/benchmark reporting.
    pub fn cache_pipeline_metrics(&self) -> CachePipelineMetricsSnapshot {
        self.metrics.snapshot(self.upload_queue.pending_count())
    }

    // =========================================================================
    // Shutdown
    // =========================================================================

    /// Graceful shutdown: persist LRU checkpoint.
    pub async fn shutdown(&self) {
        self.shutdown_notify.notify_waiters();
        self.upload_queue.flush_journal().await;

        // Persist final checkpoint.
        let checkpoint_path = self.config.cache_dir.join("cache.meta");
        match self.prepare_checkpoint_write() {
            Ok(Some((buf, signature))) => {
                let path = checkpoint_path.clone();
                match crate::runtime::spawn_blocking_profiled(
                    "cache_checkpoint_shutdown_save",
                    move || Self::persist_checkpoint_file(&path, &buf),
                )
                .await
                {
                    Ok(Ok(())) => {
                        if let Err(e) = self.record_checkpoint_signature(signature) {
                            warn!(error = %e, "failed to record final cache checkpoint signature");
                        } else {
                            debug!("saved final cache checkpoint");
                        }
                    }
                    Ok(Err(e)) => warn!(error = %e, "failed to save final cache checkpoint"),
                    Err(e) => warn!(error = %e, "final checkpoint save task failed"),
                }
            }
            Ok(None) => {
                debug!("final cache checkpoint unchanged; skipping write");
            }
            Err(e) => {
                warn!(error = %e, "failed to prepare final cache checkpoint");
            }
        }
    }

    // =========================================================================
    // Internal: promotion
    // =========================================================================

    fn promote_to_l1(&self, object_id: ObjectId, data: Bytes) {
        self.l1.insert(object_id, data);
        self.lru
            .set_location(object_id, BlockLocation::MemoryAndDisk);

        if self.l1.over_budget() {
            self.evict_notify.notify_one();
        }
    }

    // =========================================================================
    // Internal: eviction
    // =========================================================================

    /// Run L1 eviction: demote oldest entries from RAM to disk-only.
    fn evict_l1_bytes(&self, mut to_free: u64) {
        while to_free > 0 {
            let Some((id, size)) = self.lru.evict_l1() else {
                break;
            };
            // Remove from L1 RAM (data stays on L2 disk).
            self.l1.remove(id);
            if size as u64 >= to_free {
                break;
            }
            to_free -= size as u64;
        }
    }

    /// Run L2 eviction: fully remove oldest disk-only entries.
    fn evict_l2_bytes(&self, mut to_free: u64) {
        while to_free > 0 {
            let Some((id, size)) = self.lru.evict_l2() else {
                break;
            };
            // Also remove from L1 just in case.
            self.l1.remove(id);
            // Remove file from disk (sync — we're in a background task).
            self.l2.remove_sync(id);
            if size as u64 >= to_free {
                break;
            }
            to_free -= size as u64;
        }
    }

    // =========================================================================
    // Background tasks
    // =========================================================================

    async fn background_l2_commit_worker(self: Arc<Self>, worker_id: usize) {
        let rx = self.l2_commit_scheduler.receiver();
        loop {
            let maybe_job = tokio::select! {
                biased;
                _ = self.shutdown_notify.notified() => break,
                job = async {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                } => job,
            };
            let Some(job) = maybe_job else {
                break;
            };

            self.metrics.l2_commit_scheduler.record_dequeue();
            let _worker_guard = self.metrics.l2_commit_scheduler.enter_worker();

            let started = Instant::now();
            let mut attempts = 0u32;
            let mut completed = false;
            while attempts < L2_COMMIT_MAX_ATTEMPTS {
                attempts += 1;
                let write_result: io::Result<()> = match tokio::time::timeout(
                    self.config.l2_write_timeout,
                    self.l2.put_bytes(job.object_id, job.data.clone()),
                )
                .await
                {
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "L2 commit timed out after {}ms",
                            self.config.l2_write_timeout.as_millis()
                        ),
                    )),
                    Ok(result) => result,
                };

                match write_result {
                    Ok(()) => {
                        self.metrics
                            .l2_write
                            .record(job.data.len() as u64, started.elapsed());
                        self.lru
                            .set_location(job.object_id, BlockLocation::MemoryAndDisk);
                        self.metrics.l2_commit_scheduler.record_complete();
                        completed = true;
                        break;
                    }
                    Err(e) => {
                        if e.kind() == io::ErrorKind::TimedOut {
                            self.metrics.l2_commit_scheduler.record_failed();
                            warn!(
                                worker_id,
                                object_id = job.object_id,
                                attempts,
                                error = %e,
                                "L2 commit timed out; dropping job without retry"
                            );
                            break;
                        }
                        if attempts >= L2_COMMIT_MAX_ATTEMPTS {
                            self.metrics.l2_commit_scheduler.record_failed();
                            warn!(
                                worker_id,
                                object_id = job.object_id,
                                attempts,
                                error = %e,
                                "L2 commit worker exhausted retries"
                            );
                            break;
                        }
                        let backoff_ms = L2_COMMIT_RETRY_BASE_MS << (attempts - 1);
                        warn!(
                            worker_id,
                            object_id = job.object_id,
                            attempts,
                            backoff_ms,
                            error = %e,
                            "L2 commit worker retrying after transient failure"
                        );
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    }
                }
            }

            if !completed && tracing::enabled!(tracing::Level::DEBUG) {
                debug!(
                    worker_id,
                    object_id = job.object_id,
                    "L2 commit worker dropped job after retries (block remains in L1)"
                );
            }

            if self.l1.over_budget() || self.l2.total_bytes() > self.l2.max_bytes() {
                self.evict_notify.notify_one();
            }
        }

        debug!(worker_id, "background L2 commit worker stopped");
    }

    async fn background_evictor(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(EVICTION_INTERVAL_SECS));
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_notify.notified() => break,
                _ = self.evict_notify.notified() => {},
                _ = interval.tick() => {},
            }

            // Phase 1: L1 overflow → demote to disk-only.
            let l1_used = self.l1.total_bytes();
            let l1_max = self.l1.max_bytes();
            if l1_used > l1_max {
                let excess = l1_used - l1_max;
                self.evict_l1_bytes(excess);
                debug!(freed_mb = excess / (1024 * 1024), "L1 eviction complete");
            }

            // Phase 2: L2 overflow → full eviction.
            let l2_used = self.l2.total_bytes();
            let l2_max = self.l2.max_bytes();
            if l2_used > l2_max {
                let excess = l2_used - l2_max;
                self.evict_l2_bytes(excess);
                debug!(freed_mb = excess / (1024 * 1024), "L2 eviction complete");
            }

            // Phase 3: Disk space guard.
            if let Some((free, total)) = l2_disk::get_free_disk_space(&self.config.cache_dir) {
                let ratio = free as f64 / total as f64;
                if ratio < self.config.disk_free_ratio {
                    let target_free = (total as f64 * self.config.disk_free_ratio) as u64;
                    let need = target_free.saturating_sub(free);
                    if need > 0 {
                        warn!(
                            free_pct = format!("{:.1}", ratio * 100.0),
                            need_mb = need / (1024 * 1024),
                            "low disk space — emergency eviction"
                        );
                        // First demote L1 entries, then evict L2.
                        self.evict_l1_bytes(need);
                        self.evict_l2_bytes(need);
                    }
                }
            }
        }
        debug!("background evictor stopped");
    }

    async fn background_checkpoint(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_secs(CHECKPOINT_INTERVAL_SECS));
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_notify.notified() => break,
                _ = interval.tick() => {},
            }

            let path = self.config.cache_dir.join("cache.meta");
            let cache = Arc::clone(&self);
            match crate::runtime::spawn_blocking_profiled("cache_checkpoint_save", move || {
                cache.save_checkpoint(&path)
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(error = %e, "failed to save cache checkpoint");
                }
                Err(e) => {
                    warn!(error = %e, "checkpoint save task failed");
                }
            }
        }
        debug!("background checkpoint stopped");
    }

    // =========================================================================
    // Checkpoint persistence
    // =========================================================================

    fn prepare_checkpoint_write(&self) -> io::Result<Option<CheckpointWrite>> {
        let (buf, signature) = self.build_checkpoint_payload();
        let should_skip = {
            let guard = self
                .last_checkpoint_signature
                .lock()
                .map_err(|_| io::Error::other("checkpoint signature lock poisoned"))?;
            matches!(*guard, Some(last) if last == signature)
        };
        if should_skip {
            Ok(None)
        } else {
            Ok(Some((buf, signature)))
        }
    }

    fn build_checkpoint_payload(&self) -> CheckpointWrite {
        let entries = self.lru.snapshot_for_checkpoint();
        let count = entries.len() as u32;

        let mut buf = Vec::with_capacity(
            checkpoint::MAGIC.len() + 2 + 4 + 4 + entries.len() * checkpoint::ENTRY_SIZE,
        );

        // Header.
        buf.extend_from_slice(checkpoint::MAGIC);
        buf.extend_from_slice(&checkpoint::VERSION.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        // Placeholder for CRC32 (filled in below).
        let crc_offset = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes());

        // Entries.
        for e in &entries {
            buf.extend_from_slice(&e.object_id.to_le_bytes());
            buf.extend_from_slice(&e.size.to_le_bytes());
            buf.push(match e.location {
                BlockLocation::DiskOnly => 0,
                BlockLocation::MemoryAndDisk => 1,
                BlockLocation::MemoryOnly => 2,
            });
            buf.push(if e.pinned { 1 } else { 0 });
            buf.extend_from_slice(&[0u8; 4]); // padding
        }

        // CRC32 over entries.
        let entry_data = &buf[crc_offset + 4..];
        let crc = crc32fast::hash(entry_data);
        buf[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_le_bytes());
        (buf, (count, crc))
    }

    fn persist_checkpoint_file(path: &Path, buf: &[u8]) -> io::Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, buf)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    fn record_checkpoint_signature(&self, signature: CheckpointSignature) -> io::Result<()> {
        let mut guard = self
            .last_checkpoint_signature
            .lock()
            .map_err(|_| io::Error::other("checkpoint signature lock poisoned"))?;
        *guard = Some(signature);
        Ok(())
    }

    fn save_checkpoint(&self, path: &Path) -> io::Result<()> {
        let Some((buf, signature)) = self.prepare_checkpoint_write()? else {
            return Ok(());
        };

        Self::persist_checkpoint_file(path, &buf)?;
        self.record_checkpoint_signature(signature)?;

        Ok(())
    }
}

/// Load and validate a checkpoint file.
fn load_checkpoint(path: &Path) -> io::Result<LoadedCheckpoint> {
    let data = std::fs::read(path)?;
    let header_size = checkpoint::MAGIC.len() + 2 + 4 + 4; // magic(6) + version(2) + count(4) + crc(4)
    if data.len() < header_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "checkpoint too short",
        ));
    }
    if &data[0..6] != checkpoint::MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad checkpoint magic",
        ));
    }
    let version = u16::from_le_bytes([data[6], data[7]]);
    if version != checkpoint::VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported checkpoint version: {version}"),
        ));
    }
    let count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    let stored_crc = u32::from_le_bytes(data[12..16].try_into().unwrap());

    let entry_data = &data[16..];
    if entry_data.len() < count * checkpoint::ENTRY_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated checkpoint",
        ));
    }

    // Verify CRC.
    let computed_crc = crc32fast::hash(&entry_data[..count * checkpoint::ENTRY_SIZE]);
    if computed_crc != stored_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("checkpoint CRC mismatch: stored={stored_crc:#x}, computed={computed_crc:#x}"),
        ));
    }

    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let base = i * checkpoint::ENTRY_SIZE;
        let object_id = u64::from_le_bytes(entry_data[base..base + 8].try_into().unwrap());
        let size = u32::from_le_bytes(entry_data[base + 8..base + 12].try_into().unwrap());
        let location = match entry_data[base + 12] {
            0 => BlockLocation::DiskOnly,
            2 => BlockLocation::MemoryOnly,
            _ => BlockLocation::MemoryAndDisk,
        };
        let pinned = entry_data[base + 13] != 0;

        entries.push(CheckpointEntry {
            object_id,
            size,
            location,
            pinned,
        });
    }

    Ok(LoadedCheckpoint {
        entries,
        signature: (count as u32, stored_crc),
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(dir: &Path) -> BlockCacheConfig {
        BlockCacheConfig {
            l1_size: 1024 * 1024,      // 1 MiB
            l2_size: 10 * 1024 * 1024, // 10 MiB
            cache_dir: dir.to_path_buf(),
            disk_free_ratio: 0.01,
            l2_commit_workers: DEFAULT_L2_COMMIT_WORKERS,
            l2_commit_queue_capacity: DEFAULT_L2_COMMIT_QUEUE_CAPACITY,
            l2_read_timeout: Duration::from_millis(DEFAULT_L2_READ_TIMEOUT_MS),
            l2_write_timeout: Duration::from_millis(DEFAULT_L2_WRITE_TIMEOUT_MS),
            l2_read_breaker_timeout_threshold: DEFAULT_L2_READ_BREAKER_TIMEOUT_THRESHOLD,
            l2_read_breaker_cooldown: Duration::from_millis(DEFAULT_L2_READ_BREAKER_COOLDOWN_MS),
        }
    }

    #[tokio::test]
    async fn put_get_remove() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, _rx) = BlockCache::open(test_config(dir.path())).unwrap();

        let data = Bytes::from_static(b"hello cache v3");
        cache.put(1, data.clone()).await.unwrap();

        // L1 hit.
        let got = cache.get(1).await.unwrap();
        assert_eq!(got, data);

        // Remove and verify miss.
        cache.remove(1).await;
        assert!(cache.get(1).await.is_none());
    }

    #[tokio::test]
    async fn l2_promotion() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, _rx) = BlockCache::open(test_config(dir.path())).unwrap();

        let data = Bytes::from(vec![42u8; 512]);
        cache.put(10, data.clone()).await.unwrap();

        // Force eviction from L1.
        cache.l1.remove(10);
        cache.lru.set_location(10, BlockLocation::DiskOnly);

        // get() should promote from L2 back to L1.
        let got = cache.get(10).await.unwrap();
        assert_eq!(got, data);
        assert!(cache.l1.contains(10));
    }

    #[tokio::test]
    async fn checkpoint_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        {
            let (cache, _rx) = BlockCache::open(config.clone()).unwrap();
            cache.put(1, Bytes::from_static(b"aaa")).await.unwrap();
            cache.put(2, Bytes::from_static(b"bbb")).await.unwrap();
            cache.put(3, Bytes::from_static(b"ccc")).await.unwrap();
            cache.shutdown().await;
        }

        // Reopen — should recover LRU from checkpoint.
        {
            let (cache, _rx) = BlockCache::open(config).unwrap();
            assert_eq!(cache.lru.len(), 3);
            assert!(cache.contains(1));
            assert!(cache.contains(2));
            assert!(cache.contains(3));

            // L2 should have the data.
            let got = cache.get(2).await.unwrap();
            assert_eq!(got, Bytes::from_static(b"bbb"));
        }
    }

    #[tokio::test]
    async fn l2_read_breaker_opens_after_threshold_timeouts() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.l2_read_breaker_timeout_threshold = 2;
        config.l2_read_breaker_cooldown = Duration::from_secs(2);

        let (cache, _rx) = BlockCache::open(config).unwrap();
        assert!(!cache.is_l2_read_breaker_open());

        cache.record_l2_read_timeout(7);
        assert!(!cache.is_l2_read_breaker_open());

        cache.record_l2_read_timeout(7);
        assert!(cache.is_l2_read_breaker_open());
    }

    #[tokio::test]
    async fn reopen_initializes_checkpoint_signature() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());

        {
            let (cache, _rx) = BlockCache::open(config.clone()).unwrap();
            cache.put(11, Bytes::from_static(b"seed")).await.unwrap();
            cache.shutdown().await;
        }

        let (cache, _rx) = BlockCache::open(config).unwrap();
        let signature = cache
            .last_checkpoint_signature
            .lock()
            .unwrap()
            .as_ref()
            .copied();
        assert!(signature.is_some());
    }

    #[tokio::test]
    async fn upload_submit_complete() {
        let dir = tempfile::tempdir().unwrap();
        let (cache, mut rx) = BlockCache::open(test_config(dir.path())).unwrap();

        let data = Bytes::from_static(b"upload me");
        cache.put(100, data).await.unwrap();
        cache.submit_upload(1, 0, 100, 0);

        let op = rx.recv().await.unwrap();
        assert_eq!(op.object_id, 100);

        // Block should still be readable.
        assert!(cache.get(100).await.is_some());

        cache.complete_upload(&op);
        // Block stays in cache after upload (R5).
        assert!(cache.contains(100));
    }

    #[tokio::test]
    async fn l1_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.l1_size = 100; // tiny L1 budget

        let (cache, _rx) = BlockCache::open(config).unwrap();

        // Insert data that exceeds L1 budget.
        cache.put(1, Bytes::from(vec![0u8; 60])).await.unwrap();
        cache.put(2, Bytes::from(vec![1u8; 60])).await.unwrap();

        // Manually trigger L1 eviction.
        let excess = cache.l1.total_bytes().saturating_sub(cache.l1.max_bytes());
        if excess > 0 {
            cache.evict_l1_bytes(excess);
        }

        // At least one entry should be demoted from L1 but still on L2.
        let still_in_l1 = cache.l1.len();
        assert!(
            still_in_l1 < 2,
            "expected L1 eviction to demote at least one entry"
        );

        // Both should still be readable (from L2).
        assert!(cache.get(1).await.is_some());
        assert!(cache.get(2).await.is_some());
    }
}

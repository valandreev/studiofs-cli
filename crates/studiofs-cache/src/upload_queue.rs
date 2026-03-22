//! Upload Queue — writeback journal for pending S3 uploads.
//!
//! Each pending upload is tracked by a small journal file:
//!
//! ```text
//! {cache_dir}/upload/{object_id:016x}.pending
//! ```
//!
//! Written atomically via tmp + rename.  On startup the `upload/` directory is
//! scanned to recover pending uploads after a crash.
//!
//! The upload queue does **not** own the upload worker itself — it provides
//! a channel of [`PendingUpload`] items that the VFS upload worker consumes.
//! This keeps cache-layer responsibilities separate from network I/O.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, info, warn};

// =============================================================================
// Journal format constants
// =============================================================================

/// Magic bytes: "STFSUP" (6 bytes).
const JOURNAL_MAGIC: &[u8; 6] = b"STFSUP";
/// Journal format version.
const JOURNAL_VERSION: u16 = 2;
/// Total v1 entry size: 6 + 2 + 8 + 8 + 8 + 1 + 8 + 1 = 42 bytes.
const JOURNAL_ENTRY_SIZE_V1: usize = 42;
/// Total v2 entry size: 6 + 2 + 8 + 8 + 8 + 8 + 8 = 48 bytes.
const JOURNAL_ENTRY_SIZE_V2: usize = 48;

// =============================================================================
// Types
// =============================================================================

/// Inode number (re-exported convenience).
pub type Ino = u64;

/// A pending upload operation.
#[derive(Debug, Clone)]
pub struct PendingUpload {
    /// Inode that owns this block.
    pub ino: Ino,
    /// File offset this block corresponds to.
    pub offset: u64,
    /// Object ID (block ID).
    pub object_id: u64,
    /// Monotonic content generation captured when the upload was queued.
    pub content_epoch: u64,
    /// Path to the journal file tracking this op.
    pub journal_path: PathBuf,
    /// Enqueue timestamp in unix milliseconds (from journal).
    pub created_unix_ms: u64,
    /// In-memory retry attempt counter for cooldown requeue logic.
    pub retry_count: u32,
}

// =============================================================================
// Upload Queue
// =============================================================================

/// Manages persistent journal of pending S3 uploads.
pub struct UploadQueue {
    upload_dir: PathBuf,
    sender: mpsc::UnboundedSender<PendingUpload>,
    journal_tx: mpsc::UnboundedSender<JournalOp>,
    pending_count: AtomicUsize,
    pending_state: Mutex<PendingState>,
    idle_notify: Arc<Notify>,
}

/// Atomically-consistent index of in-flight uploads.
///
/// Both maps are guarded by the same mutex so that `has_pending_for_ino`
/// never observes an inconsistent state where a block exists in the
/// object-id index but the per-inode counter has not yet been bumped.
struct PendingState {
    by_object: HashMap<u64, PendingUpload>,
    ino_counts: HashMap<Ino, usize>,
}

enum JournalOp {
    Write {
        ino: Ino,
        offset: u64,
        object_id: u64,
        content_epoch: u64,
        final_path: PathBuf,
        created_unix_nanos: i64,
    },
    Complete {
        path: PathBuf,
    },
    Barrier {
        ack: std::sync::mpsc::Sender<()>,
    },
}

impl UploadQueue {
    /// Create a new upload queue.
    ///
    /// Returns `(queue, receiver)`.  The receiver yields [`PendingUpload`]
    /// items for the VFS upload worker.
    ///
    /// Any existing `.pending` files are scanned and re-queued (crash recovery).
    pub fn new(cache_dir: impl AsRef<Path>) -> (Arc<Self>, mpsc::UnboundedReceiver<PendingUpload>) {
        let upload_dir = cache_dir.as_ref().join("upload");
        fs::create_dir_all(&upload_dir).expect("failed to create upload dir");

        let (tx, rx) = mpsc::unbounded_channel();
        let (journal_tx, journal_rx) = mpsc::unbounded_channel();
        let idle_notify = Arc::new(Notify::new());

        let queue = Arc::new(Self {
            upload_dir: upload_dir.clone(),
            sender: tx,
            journal_tx,
            pending_count: AtomicUsize::new(0),
            pending_state: Mutex::new(PendingState {
                by_object: HashMap::new(),
                ino_counts: HashMap::new(),
            }),
            idle_notify,
        });

        tokio::spawn(journal_writer_task(journal_rx));
        queue.scan_and_queue();

        (queue, rx)
    }

    /// Submit a new pending upload (hot path — non-blocking).
    ///
    /// 1. Writes a journal file atomically (tmp + rename).
    /// 2. Sends the upload op to the channel.
    ///
    /// The caller is responsible for pinning the block in the LRU tracker
    /// before calling this.
    pub fn submit(&self, ino: Ino, offset: u64, object_id: u64, content_epoch: u64) {
        let journal_path = self.journal_path(object_id);
        let created_unix_nanos = now_unix_nanos_i64();
        let _ = self.journal_tx.send(JournalOp::Write {
            ino,
            offset,
            object_id,
            content_epoch,
            final_path: journal_path.clone(),
            created_unix_nanos,
        });

        self.pending_count.fetch_add(1, Ordering::Relaxed);

        let op = PendingUpload {
            ino,
            offset,
            object_id,
            content_epoch,
            journal_path,
            created_unix_ms: unix_nanos_to_ms(created_unix_nanos),
            retry_count: 0,
        };

        self.record_pending(op.clone());
        if self.sender.send(op).is_err() {
            error!("upload channel closed — cannot queue upload");
            self.remove_pending(object_id);
            self.mark_done();
        }
    }

    /// Mark an upload as complete.
    ///
    /// Removes the journal file.  The caller is responsible for unpinning the
    /// block in the LRU tracker.
    pub fn complete(&self, op: &PendingUpload) {
        self.remove_pending(op.object_id);
        if !op.journal_path.as_os_str().is_empty() {
            let _ = self.journal_tx.send(JournalOp::Complete {
                path: op.journal_path.clone(),
            });
        }
        self.mark_done();
    }

    /// Requeue a failed upload for retry (does not increment pending count).
    pub fn requeue(&self, op: PendingUpload) -> bool {
        self.sender.send(op).is_ok()
    }

    /// Cancel a pending upload: remove the journal file and decrement the
    /// pending count.  The caller is responsible for unpinning + removing the
    /// block from cache.
    pub fn cancel(&self, op: &PendingUpload) {
        self.remove_pending(op.object_id);
        if !op.journal_path.as_os_str().is_empty() {
            let _ = self.journal_tx.send(JournalOp::Complete {
                path: op.journal_path.clone(),
            });
        }
        self.mark_done();
    }

    /// Wait until queued journal operations before this call are applied.
    pub async fn flush_journal(&self) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        if self
            .journal_tx
            .send(JournalOp::Barrier { ack: tx })
            .is_err()
        {
            return;
        }
        let _ = crate::runtime::spawn_blocking_profiled("upload_flush_barrier_wait", move || {
            rx.recv_timeout(Duration::from_secs(5))
        })
        .await;
    }

    /// Wait until all currently pending uploads complete or `timeout` elapses.
    pub async fn wait_for_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            // Register interest BEFORE checking the count.  This prevents a
            // TOCTOU race where mark_done() fires notify_waiters() between our
            // load() and .notified().await — losing the notification and
            // causing a spurious full-timeout wait.
            let notified = self.idle_notify.notified();

            if self.pending_count.load(Ordering::Acquire) == 0 {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, notified).await.is_err() {
                // Final check after timeout — count might have reached 0 just
                // before the deadline.
                return self.pending_count.load(Ordering::Acquire) == 0;
            }
        }
    }

    /// Current number of pending uploads.
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Snapshot all currently pending upload ops from the live in-memory index.
    pub fn snapshot_pending(&self) -> Vec<PendingUpload> {
        let state = self.pending_state.lock();
        let mut ops: Vec<_> = state.by_object.values().cloned().collect();
        ops.sort_by_key(|op| (op.created_unix_ms, op.object_id));
        ops
    }

    /// Upload directory path.
    pub fn upload_dir(&self) -> &Path {
        &self.upload_dir
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    fn mark_done(&self) {
        // Saturating decrement — guards against double-complete (reconcile + uploader race).
        let result = self
            .pending_count
            .fetch_update(Ordering::Release, Ordering::Acquire, |v| {
                if v > 0 { Some(v - 1) } else { None }
            });
        if result == Ok(1) {
            // Was 1, now 0 — notify idle waiters.
            self.idle_notify.notify_waiters();
        }
    }

    fn journal_path(&self, object_id: u64) -> PathBuf {
        self.upload_dir.join(format!("{:016x}.pending", object_id))
    }

    fn record_pending(&self, op: PendingUpload) {
        let mut state = self.pending_state.lock();
        let ino = op.ino;
        let old = state.by_object.insert(op.object_id, op);
        // Only bump the ino counter when inserting a genuinely new object_id.
        if old.is_none() {
            *state.ino_counts.entry(ino).or_insert(0) += 1;
        }
    }

    fn remove_pending(&self, object_id: u64) {
        let mut state = self.pending_state.lock();
        if let Some(removed) = state.by_object.remove(&object_id) {
            if let std::collections::hash_map::Entry::Occupied(mut e) =
                state.ino_counts.entry(removed.ino)
            {
                *e.get_mut() -= 1;
                if *e.get() == 0 {
                    e.remove_entry();
                }
            }
        }
    }

    /// O(1) check whether any pending uploads reference the given inode.
    pub fn has_pending_for_ino(&self, ino: Ino) -> bool {
        self.pending_state.lock().ino_counts.contains_key(&ino)
    }

    fn parse_journal(&self, path: &Path) -> io::Result<PendingUpload> {
        let mut f = File::open(path)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;

        if data.len() < JOURNAL_ENTRY_SIZE_V1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "journal too short",
            ));
        }
        if &data[0..6] != JOURNAL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad journal magic",
            ));
        }
        let version = u16::from_le_bytes([data[6], data[7]]);
        let object_id = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let offset = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let ino = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let (content_epoch, ts_nanos) = match version {
            1 => (0, i64::from_le_bytes(data[33..41].try_into().unwrap())),
            2 => {
                if data.len() < JOURNAL_ENTRY_SIZE_V2 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "journal v2 too short",
                    ));
                }
                let content_epoch = u64::from_le_bytes(data[32..40].try_into().unwrap());
                let ts_nanos = i64::from_le_bytes(data[40..48].try_into().unwrap());
                (content_epoch, ts_nanos)
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported journal version {other}"),
                ));
            }
        };

        Ok(PendingUpload {
            ino,
            offset,
            object_id,
            content_epoch,
            journal_path: path.to_path_buf(),
            created_unix_ms: unix_nanos_to_ms(ts_nanos),
            retry_count: 0,
        })
    }

    fn load_pending_entry(&self, path: &Path) -> io::Result<Option<PendingUpload>> {
        match self.parse_journal(path) {
            Ok(op) => Ok(Some(op)),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
                ) =>
            {
                debug!(
                    path = %path.display(),
                    error = %e,
                    "upload journal disappeared during scan, skipping"
                );
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn scan_and_queue(&self) {
        let read_dir = match fs::read_dir(&self.upload_dir) {
            Ok(rd) => rd,
            Err(e) => {
                warn!(error = %e, "failed to scan upload dir");
                return;
            }
        };

        let mut count = 0u32;
        let mut corrupted = 0u32;

        for entry in read_dir {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();

            // Clean up stale tmp files.
            if path.extension().is_some_and(|ext| ext == "tmp") {
                let _ = fs::remove_file(&path);
                continue;
            }

            if path.is_file() && path.extension().is_some_and(|ext| ext == "pending") {
                match self.load_pending_entry(&path) {
                    Ok(Some(op)) => {
                        self.pending_count.fetch_add(1, Ordering::Relaxed);
                        if self.sender.send(op.clone()).is_ok() {
                            self.record_pending(op);
                            count += 1;
                        } else {
                            self.pending_count.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "corrupt upload journal, removing");
                        corrupted += 1;
                        let _ = fs::remove_file(&path);
                    }
                }
            }
        }

        if count > 0 {
            info!(count, "recovered pending uploads from journal");
        }
        if corrupted > 0 {
            warn!(corrupted, "removed corrupted upload journals");
        }
    }
}

fn now_unix_nanos_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn unix_nanos_to_ms(ts_nanos: i64) -> u64 {
    if ts_nanos <= 0 {
        0
    } else {
        (ts_nanos as u64) / 1_000_000
    }
}

async fn journal_writer_task(mut rx: mpsc::UnboundedReceiver<JournalOp>) {
    while let Some(op) = rx.recv().await {
        match op {
            JournalOp::Write {
                ino,
                offset,
                object_id,
                content_epoch,
                final_path,
                created_unix_nanos,
            } => {
                let tmp = final_path.with_extension("tmp");
                let mut buf = Vec::with_capacity(JOURNAL_ENTRY_SIZE_V2);
                buf.extend_from_slice(JOURNAL_MAGIC);
                buf.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
                buf.extend_from_slice(&object_id.to_le_bytes());
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&ino.to_le_bytes());
                buf.extend_from_slice(&content_epoch.to_le_bytes());
                buf.extend_from_slice(&created_unix_nanos.to_le_bytes());

                if let Err(e) = tokio::fs::write(&tmp, &buf).await {
                    error!(object_id, error = %e, "upload journal write failed");
                    continue;
                }
                if let Err(e) = tokio::fs::rename(&tmp, &final_path).await {
                    error!(object_id, error = %e, "upload journal rename failed");
                }
            }
            JournalOp::Complete { path } => match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "failed to remove upload journal")
                }
            },
            JournalOp::Barrier { ack } => {
                let _ = ack.send(());
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn submit_complete_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, mut rx) = UploadQueue::new(dir.path());

        queue.submit(100, 0, 42, 7);
        queue.flush_journal().await;
        assert_eq!(queue.pending_count(), 1);

        let op = rx.recv().await.unwrap();
        assert_eq!(op.object_id, 42);
        assert_eq!(op.ino, 100);
        assert_eq!(op.content_epoch, 7);
        assert!(op.journal_path.exists());

        queue.complete(&op);
        queue.flush_journal().await;
        assert_eq!(queue.pending_count(), 0);
        assert!(!op.journal_path.exists());
    }

    #[tokio::test]
    async fn startup_rebuilds_pending_upload_index_from_journal() {
        let dir = tempfile::tempdir().unwrap();

        // Simulate a prior session that left journal files.
        {
            let (queue, _rx) = UploadQueue::new(dir.path());
            queue.submit(1, 0, 10, 3);
            queue.submit(2, 1048576, 20, 4);
            queue.flush_journal().await;
            // Drop without completing — journals remain on disk.
        }

        // New session: should recover both.
        let (queue, mut rx) = UploadQueue::new(dir.path());
        assert_eq!(queue.pending_count(), 2);

        let op1 = rx.recv().await.unwrap();
        let op2 = rx.recv().await.unwrap();

        let mut ids: Vec<u64> = vec![op1.object_id, op2.object_id];
        ids.sort();
        assert_eq!(ids, vec![10, 20]);
        assert_eq!(queue.snapshot_pending().len(), 2);
    }

    #[tokio::test]
    async fn snapshot_pending() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, _rx) = UploadQueue::new(dir.path());

        queue.submit(1, 0, 100, 1);
        queue.submit(2, 0, 200, 2);
        queue.flush_journal().await;

        let snap = queue.snapshot_pending();
        assert_eq!(snap.len(), 2);
    }

    #[tokio::test]
    async fn snapshot_pending_uses_live_index_without_directory_scan() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, _rx) = UploadQueue::new(dir.path());

        queue.submit(1, 0, 777, 3);
        queue.flush_journal().await;

        let upload_dir = dir.path().join("upload");
        let hidden_dir = dir.path().join("upload-hidden");
        std::fs::rename(&upload_dir, &hidden_dir).unwrap();

        let snap = queue.snapshot_pending();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].object_id, 777);
    }

    #[tokio::test]
    async fn pending_upload_snapshot_ignores_concurrent_completion_enoent() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, _rx) = UploadQueue::new(dir.path());

        queue.submit(1, 0, 888, 9);
        queue.flush_journal().await;

        let path = queue.journal_path(888);
        std::fs::remove_file(&path).unwrap();

        let recovered = queue
            .load_pending_entry(&path)
            .expect("missing journal should be treated as benign");
        assert!(recovered.is_none());
    }

    #[tokio::test]
    async fn wait_for_idle() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, _rx) = UploadQueue::new(dir.path());

        // No pending ops — should return immediately.
        assert!(queue.wait_for_idle(Duration::from_millis(10)).await);
    }

    #[tokio::test]
    async fn has_pending_for_ino_tracks_inserts_and_removals() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, mut rx) = UploadQueue::new(dir.path());

        assert!(!queue.has_pending_for_ino(1));
        assert!(!queue.has_pending_for_ino(2));

        // Submit two blocks for ino 1 and one for ino 2.
        queue.submit(1, 0, 100, 1);
        queue.submit(1, 4096, 101, 1);
        queue.submit(2, 0, 200, 1);
        queue.flush_journal().await;

        assert!(queue.has_pending_for_ino(1));
        assert!(queue.has_pending_for_ino(2));
        assert!(!queue.has_pending_for_ino(3));

        // Complete one of the two blocks for ino 1.
        let op = rx.recv().await.unwrap();
        assert_eq!(op.ino, 1);
        queue.complete(&op);

        // ino 1 still has the second block pending.
        assert!(queue.has_pending_for_ino(1));

        // Complete the second block for ino 1.
        let op = rx.recv().await.unwrap();
        assert_eq!(op.ino, 1);
        queue.complete(&op);
        assert!(!queue.has_pending_for_ino(1));

        // ino 2 still pending.
        assert!(queue.has_pending_for_ino(2));

        // Complete ino 2.
        let op = rx.recv().await.unwrap();
        queue.complete(&op);
        assert!(!queue.has_pending_for_ino(2));
    }
}

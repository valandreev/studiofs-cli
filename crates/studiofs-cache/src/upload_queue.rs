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

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc};
use tracing::{error, info, warn};

// =============================================================================
// Journal format constants
// =============================================================================

/// Magic bytes: "STFSUP" (6 bytes).
const JOURNAL_MAGIC: &[u8; 6] = b"STFSUP";
/// Journal format version.
const JOURNAL_VERSION: u16 = 1;
/// Total entry size: 6 + 2 + 8 + 8 + 8 + 1 + 8 + 1 = 42 bytes.
const JOURNAL_ENTRY_SIZE: usize = 42;

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
    /// Path to the journal file tracking this op.
    pub journal_path: PathBuf,
}

// =============================================================================
// Upload Queue
// =============================================================================

/// Manages persistent journal of pending S3 uploads.
pub struct UploadQueue {
    upload_dir: PathBuf,
    sender: mpsc::UnboundedSender<PendingUpload>,
    pending_count: AtomicUsize,
    idle_notify: Arc<Notify>,
}

impl UploadQueue {
    /// Create a new upload queue.
    ///
    /// Returns `(queue, receiver)`.  The receiver yields [`PendingUpload`]
    /// items for the VFS upload worker.
    ///
    /// Any existing `.pending` files are scanned and re-queued (crash recovery).
    pub fn new(
        cache_dir: impl AsRef<Path>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<PendingUpload>) {
        let upload_dir = cache_dir.as_ref().join("upload");
        fs::create_dir_all(&upload_dir).expect("failed to create upload dir");

        let (tx, rx) = mpsc::unbounded_channel();
        let idle_notify = Arc::new(Notify::new());

        let queue = Arc::new(Self {
            upload_dir,
            sender: tx,
            pending_count: AtomicUsize::new(0),
            idle_notify,
        });

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
    pub fn submit(&self, ino: Ino, offset: u64, object_id: u64) {
        // Write journal file.
        let journal_path = self.journal_path(object_id);
        if let Err(e) = self.write_journal(ino, offset, object_id, &journal_path) {
            error!(object_id, error = %e, "failed to write upload journal");
            // Continue anyway — the upload itself is more critical than the journal.
        }

        self.pending_count.fetch_add(1, Ordering::Relaxed);

        let op = PendingUpload {
            ino,
            offset,
            object_id,
            journal_path,
        };

        if self.sender.send(op).is_err() {
            error!("upload channel closed — cannot queue upload");
            self.mark_done();
        }
    }

    /// Mark an upload as complete.
    ///
    /// Removes the journal file.  The caller is responsible for unpinning the
    /// block in the LRU tracker.
    pub fn complete(&self, op: &PendingUpload) {
        if !op.journal_path.as_os_str().is_empty() {
            if let Err(e) = fs::remove_file(&op.journal_path) {
                if e.kind() != io::ErrorKind::NotFound {
                    warn!(
                        object_id = op.object_id,
                        error = %e,
                        "failed to remove upload journal"
                    );
                }
            }
        }
        self.mark_done();
    }

    /// Requeue a failed upload for retry (does not increment pending count).
    pub fn requeue(&self, op: PendingUpload) -> bool {
        self.sender.send(op).is_ok()
    }

    /// Wait until all currently pending uploads complete or `timeout` elapses.
    pub async fn wait_for_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.pending_count.load(Ordering::Relaxed) == 0 {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, self.idle_notify.notified())
                .await
                .is_err()
            {
                return false;
            }
        }
    }

    /// Current number of pending uploads.
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Snapshot all currently pending upload ops from disk.
    pub fn snapshot_pending(&self) -> Vec<PendingUpload> {
        let mut ops = Vec::new();
        let read_dir = match fs::read_dir(&self.upload_dir) {
            Ok(rd) => rd,
            Err(e) => {
                warn!(error = %e, "failed to read upload dir for snapshot");
                return ops;
            }
        };
        for entry in read_dir {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "pending") {
                match self.parse_journal(&path) {
                    Ok(op) => ops.push(op),
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "corrupt upload journal");
                    }
                }
            }
        }
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
        let result = self.pending_count.fetch_update(
            Ordering::Release,
            Ordering::Acquire,
            |v| if v > 0 { Some(v - 1) } else { None },
        );
        if result == Ok(1) {
            // Was 1, now 0 — notify idle waiters.
            self.idle_notify.notify_waiters();
        }
    }

    fn journal_path(&self, object_id: u64) -> PathBuf {
        self.upload_dir
            .join(format!("{:016x}.pending", object_id))
    }

    fn write_journal(
        &self,
        ino: Ino,
        offset: u64,
        object_id: u64,
        final_path: &Path,
    ) -> io::Result<()> {
        let tmp = final_path.with_extension("tmp");

        let mut buf = Vec::with_capacity(JOURNAL_ENTRY_SIZE);
        buf.extend_from_slice(JOURNAL_MAGIC);
        buf.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
        buf.extend_from_slice(&object_id.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&ino.to_le_bytes());
        buf.push(0u8); // status = pending
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        buf.extend_from_slice(&ts.to_le_bytes());
        buf.push(0u8); // padding

        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        // No fsync — journal is best-effort.  The disk cache + metadata DB
        // are the authoritative source.
        drop(f);

        fs::rename(&tmp, final_path)?;
        Ok(())
    }

    fn parse_journal(&self, path: &Path) -> io::Result<PendingUpload> {
        let mut f = File::open(path)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;

        if data.len() < JOURNAL_ENTRY_SIZE {
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
        let _version = u16::from_le_bytes([data[6], data[7]]);
        let object_id = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let offset = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let ino = u64::from_le_bytes(data[24..32].try_into().unwrap());

        Ok(PendingUpload {
            ino,
            offset,
            object_id,
            journal_path: path.to_path_buf(),
        })
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
                match self.parse_journal(&path) {
                    Ok(op) => {
                        self.pending_count.fetch_add(1, Ordering::Relaxed);
                        if self.sender.send(op).is_ok() {
                            count += 1;
                        } else {
                            self.pending_count.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
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

        queue.submit(100, 0, 42);
        assert_eq!(queue.pending_count(), 1);

        let op = rx.recv().await.unwrap();
        assert_eq!(op.object_id, 42);
        assert_eq!(op.ino, 100);
        assert!(op.journal_path.exists());

        queue.complete(&op);
        assert_eq!(queue.pending_count(), 0);
        assert!(!op.journal_path.exists());
    }

    #[tokio::test]
    async fn crash_recovery() {
        let dir = tempfile::tempdir().unwrap();

        // Simulate a prior session that left journal files.
        {
            let (queue, _rx) = UploadQueue::new(dir.path());
            queue.submit(1, 0, 10);
            queue.submit(2, 1048576, 20);
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
    }

    #[tokio::test]
    async fn snapshot_pending() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, _rx) = UploadQueue::new(dir.path());

        queue.submit(1, 0, 100);
        queue.submit(2, 0, 200);

        let snap = queue.snapshot_pending();
        assert_eq!(snap.len(), 2);
    }

    #[tokio::test]
    async fn wait_for_idle() {
        let dir = tempfile::tempdir().unwrap();
        let (queue, _rx) = UploadQueue::new(dir.path());

        // No pending ops — should return immediately.
        assert!(queue.wait_for_idle(Duration::from_millis(10)).await);
    }
}

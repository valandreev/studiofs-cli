//! Writeback cache — tracks dirty objects and persists pending operations.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use studiofs_meta::Ino;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use crate::disk::DiskCache;

/// A pending writeback operation.
#[derive(Debug, Clone)]
pub struct PendingOp {
    pub ino: Ino,
    pub offset: u64,
    pub object_id: u64,
    /// Path to the journal file tracking this op.
    pub journal_path: PathBuf,
}

/// Manages persistent state of pending uploads.
pub struct WritebackManager {
    pending_dir: PathBuf,
    sender: mpsc::UnboundedSender<PendingOp>,
    disk_cache: Arc<DiskCache>,
}

impl WritebackManager {
    /// Create a new writeback manager.
    /// Scans the `pending_dir` and re-queues any existing operations.
    pub fn new(cache_dir: impl AsRef<Path>, disk_cache: Arc<DiskCache>) -> (Arc<Self>, mpsc::UnboundedReceiver<PendingOp>) {
        let pending_dir = cache_dir.as_ref().join("pending");
        fs::create_dir_all(&pending_dir).expect("failed to create pending dir");

        let (tx, rx) = mpsc::unbounded_channel();
        let manager = Arc::new(Self {
            pending_dir,
            sender: tx,
            disk_cache,
        });

        // Scan existing on startup
        manager.scan_and_queue();

        (manager, rx)
    }

    fn scan_and_queue(&self) {
        let read_dir = match fs::read_dir(&self.pending_dir) {
            Ok(rd) => rd,
            Err(e) => {
                error!("failed to read pending dir: {}", e);
                return;
            }
        };

        let mut count = 0;
        for entry in read_dir {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            if path.is_file() {
                if let Some(op) = self.parse_op(&path) {
                    // Pin object so it doesn't get evicted before upload
                    self.disk_cache.pin(op.object_id);
                    
                    if self.sender.send(op).is_ok() {
                        count += 1;
                    }
                }
            }
        }
        if count > 0 {
            info!("recovered {} pending uploads from crash journal", count);
        }
    }

    /// Submit a new pending operation.
    /// Persists it to disk journal, then queues it in memory.
    pub async fn submit(&self, ino: Ino, offset: u64, object_id: u64) {
        // Pin first
        self.disk_cache.pin(object_id);

        let filename = format!("{}_{}_{:016x}", ino, offset, object_id);
        let path = self.pending_dir.join(&filename);

        // Create empty journal file
        // Content doesn't matter, filename has all info.
        let path_clone = path.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || {
            fs::File::create(&path_clone)
        }).await.expect("Task Join Error") {
            error!("failed to create journal file {:?}: {}", path, e);
            // If checking journal fail, maybe unpin?
            // But we still attempt upload via memory.
        }

        let op = PendingOp {
            ino,
            offset,
            object_id,
            journal_path: path,
        };

        if let Err(_) = self.sender.send(op) {
            error!("failed to queue pending op (channel closed)");
             // If channel closed, we are shutting down?
             // But journal is written. Next start will recover.
             // If we unpin, we might lose it? 
             // Keep pinned.
        }
    }

    /// Mark an operation as complete (removes journal file).
    pub fn complete(&self, op: &PendingOp) {
        if op.journal_path.exists() {
            if let Err(e) = fs::remove_file(&op.journal_path) {
                warn!("failed to remove journal file {:?}: {}", op.journal_path, e);
            }
        }
        self.disk_cache.unpin(op.object_id);
    }

    fn parse_op(&self, path: &Path) -> Option<PendingOp> {
        let name = path.file_name()?.to_str()?;
        let parts: Vec<&str> = name.split('_').collect();
        if parts.len() != 3 {
             return None;
        }

        let ino: Ino = parts[0].parse().ok()?;
        let offset: u64 = parts[1].parse().ok()?;
        let object_id: u64 = u64::from_str_radix(parts[2], 16).ok()?;

        Some(PendingOp {
            ino,
            offset,
            object_id,
            journal_path: path.to_path_buf(),
        })
    }
}

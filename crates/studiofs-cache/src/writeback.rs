//! Writeback cache - tracks dirty objects and persists pending operations.
//!
//! Architecture Key Changes:
//! - Enhanced journal format with status tracking
//! - Binary journal format for efficiency
//! - Recovery support for crash resilience
//!
//! Journal file format:
//! - Header: 16 bytes (magic + version)
//! - Object ID: 8 bytes
//! - File Offset: 8 bytes
//! - Inode Number: 8 bytes
//! - Status: 1 byte (0=pending, 1=uploading, 2=complete)
//! - Timestamp: 8 bytes

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use studiofs_meta::Ino;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use crate::disk::DiskCache;

// =============================================================================
// Journal Format Constants
// =============================================================================

/// Magic bytes for journal files: "STFSJOUR"
const JOURNAL_MAGIC: &[u8; 8] = b"STFSJOUR";

/// Journal format version
const JOURNAL_VERSION: u16 = 2;

/// Total journal entry size: 8 + 2 + 8 + 8 + 8 + 1 + 8 = 43 bytes
const JOURNAL_ENTRY_SIZE: usize = 43;

// =============================================================================
// Journal Status
// =============================================================================

/// Status of a pending upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UploadStatus {
    /// Upload is pending (not yet started).
    Pending = 0,
    /// Upload is in progress.
    Uploading = 1,
    /// Upload completed successfully.
    Complete = 2,
}

impl TryFrom<u8> for UploadStatus {
    type Error = std::io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Uploading),
            2 => Ok(Self::Complete),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid upload status: {}", value),
            )),
        }
    }
}

// =============================================================================
// Journal Entry
// =============================================================================

/// A journal entry representing a pending upload.
#[derive(Debug, Clone)]
pub struct JournalEntry {
    /// Object ID (sequential block ID).
    pub object_id: u64,
    /// File offset (for backward compatibility, not used in the new architecture).
    pub offset: u64,
    /// Inode number.
    pub ino: Ino,
    /// Upload status.
    pub status: UploadStatus,
    /// Creation timestamp (unix timestamp in nanos).
    pub timestamp: i64,
}

impl JournalEntry {
    /// Create a new pending journal entry.
    pub fn new(ino: Ino, offset: u64, object_id: u64) -> Self {
        Self {
            object_id,
            offset,
            ino,
            status: UploadStatus::Pending,
            timestamp: current_timestamp_nanos(),
        }
    }

    /// Encode the journal entry to binary format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(JOURNAL_ENTRY_SIZE);

        // Magic (8 bytes)
        buf.extend_from_slice(JOURNAL_MAGIC);
        // Version (2 bytes)
        buf.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
        // Object ID (8 bytes)
        buf.extend_from_slice(&self.object_id.to_le_bytes());
        // Offset (8 bytes)
        buf.extend_from_slice(&self.offset.to_le_bytes());
        // Inode (8 bytes)
        buf.extend_from_slice(&self.ino.to_le_bytes());
        // Status (1 byte)
        buf.push(self.status as u8);
        // Timestamp (8 bytes)
        buf.extend_from_slice(&self.timestamp.to_le_bytes());

        buf
    }

    /// Decode a journal entry from binary format.
    pub fn decode(data: &[u8]) -> std::io::Result<Self> {
        if data.len() < JOURNAL_ENTRY_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Journal entry too short: {} < {}", data.len(), JOURNAL_ENTRY_SIZE),
            ));
        }

        // Verify magic
        if &data[0..8] != JOURNAL_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid journal magic bytes",
            ));
        }

        // Verify version
        let version = u16::from_le_bytes([data[8], data[9]]);
        if version != JOURNAL_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported journal version: {}", version),
            ));
        }

        let object_id = u64::from_le_bytes(data[10..18].try_into().unwrap());
        let offset = u64::from_le_bytes(data[18..26].try_into().unwrap());
        let ino = u64::from_le_bytes(data[26..34].try_into().unwrap());
        let status = UploadStatus::try_from(data[34])?;
        let timestamp = i64::from_le_bytes(data[35..43].try_into().unwrap());

        Ok(Self {
            object_id,
            offset,
            ino,
            status,
            timestamp,
        })
    }
}

// =============================================================================
// Pending Operation
// =============================================================================

/// A pending writeback operation.
#[derive(Debug, Clone)]
pub struct PendingOp {
    pub ino: Ino,
    pub offset: u64,
    pub object_id: u64,
    /// Path to the journal file tracking this op.
    pub journal_path: PathBuf,
    /// Upload status.
    pub status: UploadStatus,
}

// =============================================================================
// Journal Operation (for background async writes)
// =============================================================================

/// Asynchronous journal operation sent to the background journal-writer task.
enum JournalOp {
    /// Write a new journal entry (no fsync — best-effort crash durability).
    Write { ino: Ino, offset: u64, object_id: u64 },
    /// Remove a completed journal file.
    Complete { path: PathBuf },
}

// =============================================================================
// Writeback Manager
// =============================================================================

/// Manages persistent state of pending uploads.
///
/// Journal writes are decoupled from the hot write path: `submit()` is
/// synchronous and pushes to two channels immediately:
///  1. `sender`     → the upload worker that talks to S3
///  2. `journal_tx` → a background task that writes journal files with
///     no `sync_all()` (avoids one fsync per block)
pub struct WritebackManager {
    pending_dir: PathBuf,
    sender: mpsc::UnboundedSender<PendingOp>,
    journal_tx: mpsc::UnboundedSender<JournalOp>,
    disk_cache: Arc<DiskCache>,
}

impl WritebackManager {
    /// Create a new writeback manager.
    /// Scans the `pending_dir` and re-queues any existing operations.
    pub fn new(cache_dir: impl AsRef<Path>, disk_cache: Arc<DiskCache>) -> (Arc<Self>, mpsc::UnboundedReceiver<PendingOp>) {
        let pending_dir = cache_dir.as_ref().join("pending");
        fs::create_dir_all(&pending_dir).expect("failed to create pending dir");

        let (tx, rx) = mpsc::unbounded_channel();
        let (journal_tx, journal_rx) = mpsc::unbounded_channel::<JournalOp>();

        // Clone for journal_writer_task — pending_dir is moved into the struct.
        let journal_dir = pending_dir.clone();

        let manager = Arc::new(Self {
            pending_dir,
            sender: tx,
            journal_tx,
            disk_cache,
        });

        // Spawn background journal writer (no fsync in hot path).
        tokio::spawn(journal_writer_task(journal_dir, journal_rx));

        // Scan existing on startup (crash recovery).
        manager.scan_and_queue();

        (manager, rx)
    }

    /// Scan existing journal files and queue pending operations.
    fn scan_and_queue(&self) {
        let read_dir = match fs::read_dir(&self.pending_dir) {
            Ok(rd) => rd,
            Err(e) => {
                error!("failed to read pending dir: {}", e);
                return;
            }
        };

        let mut count = 0;
        let mut corrupted = 0;

        for entry in read_dir {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();

            if path.is_file() && path.extension().is_some_and(|ext| ext == "journal") {
                match self.parse_journal_file(&path) {
                    Ok(op) => {
                        // Only queue pending uploads
                        if op.status == UploadStatus::Pending {
                            // Pin object so it doesn't get evicted before upload
                            self.disk_cache.pin(op.object_id);

                            if self.sender.send(op).is_ok() {
                                count += 1;
                            }
                        } else {
                            // Upload was in progress or complete - clean up
                            debug!("cleaning up journal with status {:?}: {:?}", op.status, path);
                            if let Err(e) = fs::remove_file(&path) {
                                warn!("failed to remove completed journal {:?}: {}", path, e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("corrupt journal file {:?}: {}", path, e);
                        corrupted += 1;
                        // Remove corrupt journal
                        if let Err(e) = fs::remove_file(&path) {
                            warn!("failed to remove corrupt journal {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        if count > 0 {
            info!("recovered {} pending uploads from crash journal", count);
        }
        if corrupted > 0 {
            warn!("removed {} corrupted journal files", corrupted);
        }
    }

    /// Submit a new pending operation.
    ///
    /// **Hot-path** — returns immediately with no disk I/O or blocking.
    /// The journal file is written by a background task (no `sync_all()`), and
    /// the upload op is queued for the S3 uploader immediately.
    pub fn submit(&self, ino: Ino, offset: u64, object_id: u64) {
        // Pin to prevent eviction before upload completes.
        self.disk_cache.pin(object_id);

        let filename = format!("{:016x}_{:016x}_{:016x}.journal", ino, offset, object_id);
        let path = self.pending_dir.join(&filename);

        // Enqueue journal write (background, no fsync).
        let _ = self.journal_tx.send(JournalOp::Write { ino, offset, object_id });

        // Enqueue for S3 upload immediately.
        let op = PendingOp {
            ino,
            offset,
            object_id,
            journal_path: path,
            status: UploadStatus::Pending,
        };
        if self.sender.send(op).is_err() {
            error!("failed to queue pending op (channel closed)");
        }
    }

    /// Mark an operation as complete.
    /// Journal file removal is dispatched to the background writer (non-blocking).
    pub fn complete(&self, op: &PendingOp) {
        self.disk_cache.unpin(op.object_id);
        if !op.journal_path.as_os_str().is_empty() {
            let _ = self.journal_tx.send(JournalOp::Complete { path: op.journal_path.clone() });
        }
    }

    /// Update the status of a pending operation.
    pub fn update_status(&self, op: &PendingOp, status: UploadStatus) -> std::io::Result<()> {
        let entry = JournalEntry {
            object_id: op.object_id,
            offset: op.offset,
            ino: op.ino,
            status,
            timestamp: current_timestamp_nanos(),
        };

        let data = entry.encode();
        let tmp_path = op.journal_path.with_extension("tmp");

        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&data)?;
            // No sync_all() — journal files are best-effort crash durability.
            // The slab index and metadata DB are the authoritative persistence.
        }

        fs::rename(&tmp_path, &op.journal_path)?;
        Ok(())
    }

    /// Parse a journal file.
    fn parse_journal_file(&self, path: &Path) -> std::io::Result<PendingOp> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let entry = JournalEntry::decode(&data)?;

        Ok(PendingOp {
            ino: entry.ino,
            offset: entry.offset,
            object_id: entry.object_id,
            journal_path: path.to_path_buf(),
            status: entry.status,
        })
    }

    /// Get the pending directory path.
    pub fn pending_dir(&self) -> &Path {
        &self.pending_dir
    }
}

// =============================================================================
// Background Journal Writer
// =============================================================================

/// Async task that writes / removes journal files without blocking the hot path.
///
/// Files are written without `sync_all()` (no per-block fsync). On a process
/// crash some journal files may be missing; the startup scan handles recovery
/// by re-queuing any pending journals that survived.
async fn journal_writer_task(
    pending_dir: PathBuf,
    mut rx: mpsc::UnboundedReceiver<JournalOp>,
) {
    while let Some(op) = rx.recv().await {
        match op {
            JournalOp::Write { ino, offset, object_id } => {
                let filename =
                    format!("{:016x}_{:016x}_{:016x}.journal", ino, offset, object_id);
                let path = pending_dir.join(&filename);
                let tmp = path.with_extension("tmp");
                let entry = JournalEntry::new(ino, offset, object_id);
                let data = entry.encode();
                // Write then rename — atomic on most filesystems, no fsync needed.
                if let Err(e) = tokio::fs::write(&tmp, &data).await {
                    warn!("journal_writer: write {:?} failed: {}", tmp, e);
                    continue;
                }
                if let Err(e) = tokio::fs::rename(&tmp, &path).await {
                    warn!("journal_writer: rename {:?} failed: {}", tmp, e);
                }
            }
            JournalOp::Complete { path } => {
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => warn!("journal_writer: remove {:?} failed: {}", path, e),
                }
            }
        }
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Get current timestamp in nanoseconds since Unix epoch.
fn current_timestamp_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_entry_roundtrip() {
        let entry = JournalEntry::new(123, 456, 789);
        let encoded = entry.encode();
        assert_eq!(encoded.len(), JOURNAL_ENTRY_SIZE);

        let decoded = JournalEntry::decode(&encoded).unwrap();
        assert_eq!(decoded.object_id, 789);
        assert_eq!(decoded.offset, 456);
        assert_eq!(decoded.ino, 123);
        assert_eq!(decoded.status, UploadStatus::Pending);
    }

    #[test]
    fn test_journal_status_update() {
        let mut entry = JournalEntry::new(1, 0, 42);
        entry.status = UploadStatus::Uploading;

        let encoded = entry.encode();
        let decoded = JournalEntry::decode(&encoded).unwrap();
        assert_eq!(decoded.status, UploadStatus::Uploading);
    }

    #[test]
    fn test_invalid_magic() {
        let data = vec![0u8; JOURNAL_ENTRY_SIZE];
        // No magic at start
        assert!(JournalEntry::decode(&data).is_err());
    }

    #[test]
    fn test_invalid_version() {
        let mut data = vec![0u8; JOURNAL_ENTRY_SIZE];
        data[0..8].copy_from_slice(JOURNAL_MAGIC);
        data[8..10].copy_from_slice(&99u16.to_le_bytes()); // Invalid version
        assert!(JournalEntry::decode(&data).is_err());
    }
}

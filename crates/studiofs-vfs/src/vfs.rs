//! Core VFS implementation — Block-based architecture.
//!
//! Key design:
//! - Files are split into fixed-size blocks (default 8MB)
//! - Each block has a sequential ID (not content hash)
//! - Blocks are immutable - never modified, only created/deleted
//! - No deduplication = no reference counting overhead
//! - All metadata in redb, S3 stores only raw data blocks
//!
//! Write Path:
//! 1. Split data into blocks
//! 2. Package block into STFS object format
//! 3. Allocate sequential block ID
//! 4. Store in cache with journal
//! 5. Queue for async upload
//! 6. Update metadata with BlockRef
//!
//! Read Path:
//! 1. Look up block references from metadata
//! 2. Fetch from cache/S3
//! 3. Validate object payload + checksum
//! 4. Assemble response

#[path = "commit.rs"]
mod commit;
#[path = "materialize.rs"]
mod materialize;

use crate::incarnation::{
    CommittedIncarnation, InodeIncarnationState, PendingIncarnation, VfsUploadState,
};
use crate::qos::{FairInodeScheduler, QosController};
use crate::readahead::{DirAccessTracker, ReadAheadTracker};

use crate::runtime::{run_blocking, run_meta};
use bytes::{Bytes, BytesMut};
use opendal::options::WriteOptions;
use opendal::{EntryMode, ErrorKind, Operator};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use studiofs_cache::block_cache::{BlockCache, BlockCacheConfig, DEFAULT_L1_SIZE, DEFAULT_L2_SIZE};
use studiofs_cache::upload_queue::PendingUpload;
use studiofs_data::object::{create_object, format_object_key, parse_object_id_from_key};
use studiofs_data::store::{DataStore, DataStoreConfig, IoProfile};
use studiofs_meta::types::{BlockFlags, block_index_from_offset, offset_within_block};
use studiofs_meta::*;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, error, info, warn};

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("metadata error: {0}")]
    Meta(#[from] studiofs_meta::MetaError),

    #[error("data error: {0}")]
    Data(#[from] studiofs_data::store::DataError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("object error: {0}")]
    Object(#[from] studiofs_data::object::ObjectError),

    #[error("block not found: inode {0}, block {1}")]
    BlockNotFound(Ino, u32),

    #[error("lock conflict (EAGAIN)")]
    WouldBlock,

    #[error("remote content is still uploading (EAGAIN)")]
    RemoteContentPending,

    #[error("invalid filename: {0}")]
    InvalidName(String),

    #[error("{0}")]
    Other(String),
}

/// Filesystem statistics returned by `statfs()`.
#[derive(Debug, Clone)]
pub struct StatFs {
    /// Total number of blocks (virtual).
    pub blocks: u64,
    /// Free blocks.
    pub bfree: u64,
    /// Available blocks (same as bfree for single-user).
    pub bavail: u64,
    /// Number of inodes (files + dirs).
    pub files: u64,
    /// Free inode slots.
    pub ffree: u64,
    /// Block size.
    pub bsize: u32,
    /// Maximum filename length.
    pub namelen: u32,
    /// Fragment size.
    pub frsize: u32,
}

/// Snapshot of local accepted-ingress state used for observability and
/// backpressure diagnostics.
#[derive(Debug, Clone, Default)]
pub struct VfsIngressStats {
    /// Accepted bytes currently buffered in RAM (`dirty_mem`).
    pub dirty_mem_bytes: usize,
    /// Accepted block count currently buffered in RAM (`dirty_mem`).
    pub dirty_mem_blocks: usize,
    /// Accepted bytes currently spilled to local NVMe (`ingress_spill`).
    pub spill_bytes: usize,
    /// Accepted spill records currently present in `ingress_spill`.
    pub spill_records: usize,
    /// Age of the oldest spill record in milliseconds.
    pub oldest_spill_age_ms: u64,
    /// Cumulative milliseconds spent waiting on local backpressure in the
    /// foreground path.
    pub write_backpressure_wait_ms: u64,
    /// Number of blocks that were forced to spill because RAM acceptance was
    /// over budget.
    pub forced_spill_blocks: u64,
    /// Total blocks drained from RAM accepted state into metadata/cache.
    pub drained_mem_blocks: u64,
    /// Total bytes drained from RAM accepted state into metadata/cache.
    pub drained_mem_bytes: u64,
    /// Total blocks drained from spill accepted state into metadata/cache.
    pub drained_spill_blocks: u64,
    /// Total bytes drained from spill accepted state into metadata/cache.
    pub drained_spill_bytes: u64,
    /// Successful ingress checkpoint persists since startup.
    pub checkpoint_persists: u64,
    /// Failed ingress checkpoint persist attempts since startup.
    pub checkpoint_failures: u64,
    /// Age of the last successful ingress checkpoint in milliseconds.
    pub last_checkpoint_age_ms: u64,
    /// Covered segment id of the last successful ingress checkpoint.
    pub last_checkpoint_segment_id: u64,
    /// Covered offset within the covered segment of the last successful checkpoint.
    pub last_checkpoint_offset: u64,
    /// Uncheckpointed ingress record bytes since the last successful checkpoint.
    pub checkpoint_pending_bytes: u64,
    /// Uncheckpointed ingress record mutations since the last successful checkpoint.
    pub checkpoint_pending_mutations: u64,
    /// Active-segment lag relative to the last successful checkpoint.
    pub checkpoint_active_segment_lag: u64,
    /// Number of distinct segment ids still referenced by the live ingress index.
    pub checkpoint_live_segments: u64,
}

/// Immediate local-ingress acceptance decision for a prospective write batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VfsIngressLane {
    DirtyMem,
    Spill,
    Backpressure,
}

/// Snapshot of the current ingress pressure surface for a given payload size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VfsIngressPressure {
    pub next_bytes: usize,
    pub dirty_mem_bytes: usize,
    pub dirty_mem_limit: usize,
    pub spill_bytes: usize,
    pub spill_limit: usize,
    pub lane: VfsIngressLane,
}

#[derive(Debug, Clone, Default)]
pub struct VfsUploadStats {
    pub pending_uploads: usize,
    pub upload_requeues: u64,
    pub canceled_deleted: u64,
    pub canceled_stale: u64,
}

pub type VfsResult<T> = Result<T, VfsError>;

// =============================================================================
// Kernel Cache Invalidation
// =============================================================================

/// Callback trait for notifying the platform filesystem driver (FUSE / WinFSP)
/// to drop cached metadata so that userspace immediately sees remote changes.
///
/// Implemented by platform bridges and registered on the VFS via
/// [`StudioVfs::set_kernel_invalidator`] after mount.
pub trait KernelInvalidator: Send + Sync {
    /// Invalidate cached attributes and directory listing for a single inode.
    /// For directories this forces the kernel to re-issue `readdir`.
    fn inval_inode(&self, ino: u64);
}

// =============================================================================
// Filename Validation
// =============================================================================

/// Validate a filename (directory entry name).
///
/// Rejects:
/// - Empty names
/// - Names longer than 255 bytes
/// - Names containing '/' (path separator)
/// - Dots-only names (`.`, `..`)
/// - Names containing Windows-invalid characters: < > : " \ | ? *
fn validate_name(name: &[u8]) -> VfsResult<()> {
    if name.is_empty() {
        return Err(VfsError::InvalidName("name is empty".into()));
    }
    if name.len() > 255 {
        return Err(VfsError::InvalidName(format!(
            "name too long ({} bytes, max 255)",
            name.len()
        )));
    }

    // Check for Unix path separator and Windows-invalid characters
    let invalid_chars = b"/<>:\"|\\?*";
    if let Some(pos) = name.iter().position(|&b| invalid_chars.contains(&b)) {
        let char_display = name[pos] as char;
        return Err(VfsError::InvalidName(format!(
            "name contains invalid character '{}'",
            char_display
        )));
    }

    if name == b"." || name == b".." {
        return Err(VfsError::InvalidName("name is '.' or '..'".into()));
    }
    Ok(())
}

// =============================================================================
// Constants & Configuration
// =============================================================================

/// Attribute cache TTL — read-through attributes are valid for 1 second.
const ATTR_CACHE_TTL: Duration = Duration::from_secs(1);
const DEFAULT_JANITOR_INTERVAL: Duration = Duration::from_secs(30 * 60);
const DEFAULT_JANITOR_SKIP_NEWER_THAN: Duration = Duration::from_secs(15 * 60);
const DEFAULT_JANITOR_MAX_DELETES_PER_RUN: u64 = 500;
const DEFAULT_JANITOR_LEASE_TTL: Duration = Duration::from_secs(45 * 60);
// JuiceFS-like high-level read tuning default:
// - buffer-size: 2048M
const DEFAULT_READ_BUFFER_MB: u64 = 2_048;
// Block-level derived defaults (used as fallback if conversion is unavailable).
const DEFAULT_PREFETCH_CONCURRENCY: usize = 128;
const DEFAULT_READAHEAD_WIDTH: u32 = 128;
const DEFAULT_READAHEAD_MIN_PREFETCH: u32 = 16;
const DEFAULT_READAHEAD_SEQ_STEP: u8 = 4;
const DEFAULT_READAHEAD_RANDOM_PENALTY: u8 = 2;
const DEFAULT_PREFETCH_FOREGROUND_RESERVE: usize = 32;
const DEFAULT_PREFETCH_MAX_PER_CYCLE: usize = 96;
const DEFAULT_PREFETCH_PAUSE_ON_NETERR_MS: u64 = 250;
const DEFAULT_DIR_PREFETCH_MAX_BATCH: usize = 0;
const DEFAULT_UPLOAD_DELAY_MS: u64 = 2000;
const DEFAULT_UPLOAD_REQUEUE_BASE_MS: u64 = 5000;
const DEFAULT_UPLOAD_REQUEUE_MAX_MS: u64 = 120_000;
const DEFAULT_UPLOAD_STATS_INTERVAL_SECS: u64 = 10;
const DEFAULT_UPLOAD_CACHE_IO_SLOTS: usize = 32;
const DEFAULT_UPLOAD_CACHE_IO_USER_PENALTY: u32 = 4;
const DEFAULT_UPLOAD_USER_IDLE_GRACE_MS: u64 = 250;
const INITIAL_INGRESS_SEGMENT_ID: u64 = 1;
const INGRESS_SEGMENT_MAGIC: [u8; 4] = *b"ISG1";
const INGRESS_RECORD_PUT: u8 = 1;
const INGRESS_RECORD_INVALIDATE: u8 = 2;
const INGRESS_RECORD_HEADER_LEN: usize = 37;
const DEFAULT_INGRESS_SEGMENT_MAX_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_INGRESS_CHECKPOINT_INTERVAL_SECS: u64 = 30;
const DEFAULT_INGRESS_CHECKPOINT_MIN_APPEND_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_INGRESS_CHECKPOINT_MIN_MUTATIONS: u64 = 128;
const DEFAULT_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG: u64 = 2;
const DEFAULT_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS: u64 = 4;
const DEFAULT_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS: u64 = 10;
const DEFAULT_METADATA_INVALIDATION_SYNC_INTERVAL_MS: u64 = 500;
const DEFAULT_LOCK_LEASE_TTL: Duration = Duration::from_secs(120);
const DEFAULT_LOCK_RETRY_BASE_MS: u64 = 25;
const DEFAULT_LOCK_RETRY_MAX_MS: u64 = 500;
#[derive(Clone)]
struct DistributedFlock {
    op: Operator,
    prefix: String,
    client_id: String,
    lease_ttl: Duration,
    retry_base_ms: u64,
    retry_max_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct RemoteLockRecord {
    lease_until_unix: i64,
    exclusive_owner: Option<String>,
    shared_owners: BTreeSet<String>,
}

// =============================================================================
// VFS Configuration
// =============================================================================

/// VFS configuration.
#[derive(Debug, Clone)]
pub struct VfsConfig {
    /// Path to metadata database.
    pub meta_path: String,
    /// Path to disk cache directory.
    pub cache_dir: String,
    /// Maximum disk cache size in bytes (L2 NVMe budget).
    pub cache_size: u64,
    /// L1 RAM cache budget in bytes.
    pub memory_limit: u64,
    /// Block size in bytes (default 8MB).
    pub block_size: u32,
    /// Maximum concurrent uploads.
    pub max_concurrent_uploads: usize,
    /// Number of blocks to prefetch ahead when a sequential access pattern is
    /// detected.  0 disables read-ahead entirely.  Default: 128 (= 1024 MB with
    /// 8 MB blocks).
    pub readahead_width: u32,
    /// Maximum bytes to hold in the dirty-write memory buffer before applying
    /// backpressure.  This buffer stores blocks that `flush()` has accepted
    /// but the background flusher has not yet written to disk.
    /// Default: 12 GiB.  Larger buffer absorbs write bursts better and lets
    /// the parallel background flusher saturate NVMe.
    pub max_dirty_bytes: usize,
}

impl Default for VfsConfig {
    fn default() -> Self {
        Self {
            meta_path: String::new(),
            cache_dir: String::new(),
            cache_size: DEFAULT_L2_SIZE,
            memory_limit: DEFAULT_L1_SIZE,
            block_size: studiofs_meta::types::DEFAULT_BLOCK_SIZE,
            max_concurrent_uploads: 4,
            readahead_width: DEFAULT_READAHEAD_WIDTH,
            max_dirty_bytes: 12 * 1024 * 1024 * 1024,
        }
    }
}

fn parse_size_bytes_env(raw: &str) -> Option<usize> {
    let s = raw.trim().to_ascii_uppercase();
    if s.is_empty() {
        return None;
    }
    let (num_part, multiplier) = if let Some(v) = s.strip_suffix("TB") {
        (v, 1024u128 * 1024 * 1024 * 1024)
    } else if let Some(v) = s.strip_suffix("GB") {
        (v, 1024u128 * 1024 * 1024)
    } else if let Some(v) = s.strip_suffix("MB") {
        (v, 1024u128 * 1024)
    } else if let Some(v) = s.strip_suffix("KB") {
        (v, 1024u128)
    } else if let Some(v) = s.strip_suffix('B') {
        (v, 1u128)
    } else {
        (s.as_str(), 1u128)
    };
    let num = num_part.trim().parse::<u128>().ok()?;
    let bytes = num.checked_mul(multiplier)?;
    usize::try_from(bytes).ok()
}

// =============================================================================
// Open File Handle Tracking
// =============================================================================

/// Flags associated with an open file descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFlags(u32);

impl OpenFlags {
    pub const RDONLY: Self = Self(0x01);
    pub const WRONLY: Self = Self(0x02);
    pub const RDWR: Self = Self(0x04);
    pub const APPEND: Self = Self(0x08);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// Information about a single open file descriptor.
#[derive(Clone, Debug)]
pub struct OpenFile {
    pub ino: Ino,
    pub flags: OpenFlags,
}

// =============================================================================
// File Locking (flock)
// =============================================================================

/// File-level lock kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    /// Shared (read) lock.
    Shared,
    /// Exclusive (write) lock.
    Exclusive,
    /// Unlock.
    Unlock,
}

/// Internal state for a file-level lock.
#[derive(Debug)]
struct FileLock {
    kind: LockKind,
    owner: u64,
    /// Number of shared locks held (only meaningful when `kind == Shared`).
    shared_count: u32,
}

/// Per-VFS handle table tracking all open file descriptors.
struct HandleTable {
    /// Monotonically increasing file handle counter.
    next_fh: AtomicU64,
    /// All open handles: fh → OpenFile.
    handles: parking_lot::RwLock<HashMap<u64, OpenFile>>,
    /// Inode → number of currently open file descriptors.
    open_counts: Mutex<HashMap<Ino, u32>>,
    /// Inodes that were unlinked while still open (deferred deletion).
    orphans: Mutex<std::collections::HashSet<Ino>>,
}

impl HandleTable {
    fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
            handles: parking_lot::RwLock::new(HashMap::new()),
            open_counts: Mutex::new(HashMap::new()),
            orphans: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Allocate a new file handle and register it.
    fn open(&self, ino: Ino, flags: OpenFlags) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.write().insert(fh, OpenFile { ino, flags });
        *self.open_counts.lock().entry(ino).or_insert(0) += 1;
        fh
    }

    /// Look up an open file handle.
    fn get(&self, fh: u64) -> Option<OpenFile> {
        self.handles.read().get(&fh).cloned()
    }

    /// Close a file handle.  Returns `(ino, remaining_open_count)`.
    fn close(&self, fh: u64) -> Option<(Ino, u32)> {
        let open_file = self.handles.write().remove(&fh)?;
        let mut counts = self.open_counts.lock();
        let count = counts.get_mut(&open_file.ino)?;
        *count = count.saturating_sub(1);
        let remaining = *count;
        if remaining == 0 {
            counts.remove(&open_file.ino);
        }
        Some((open_file.ino, remaining))
    }

    /// Get the number of open handles for an inode.
    fn open_count(&self, ino: Ino) -> u32 {
        self.open_counts.lock().get(&ino).copied().unwrap_or(0)
    }

    /// Mark an inode as orphan (unlinked while open).
    fn mark_orphan(&self, ino: Ino) {
        self.orphans.lock().insert(ino);
    }

    /// Check if inode is an orphan and remove the mark. Returns true if it was.
    fn take_orphan(&self, ino: Ino) -> bool {
        self.orphans.lock().remove(&ino)
    }
}

// =============================================================================
// Pending Block (Write Buffer)
// =============================================================================

/// A block that is being written but not yet flushed.
#[derive(Debug, Clone)]
struct PendingBlock {
    /// Block data (uncompressed).
    data: BytesMut,
    /// Has this block been modified?
    dirty: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ExtentRecordId {
    object_id: u64,
    content_epoch: u64,
}

#[derive(Debug, Clone)]
struct SpillEntry {
    seq: u64,
    path: PathBuf,
    offset: u64,
    len: usize,
    created_unix_ms: u64,
}

#[derive(Debug, Default)]
struct IngressLiveIndex {
    entries: HashMap<(Ino, u32), SpillEntry>,
}

impl IngressLiveIndex {
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn get_cloned(&self, key: &(Ino, u32)) -> Option<SpillEntry> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: (Ino, u32), entry: SpillEntry) -> Option<SpillEntry> {
        self.entries.insert(key, entry)
    }

    fn remove(&mut self, key: &(Ino, u32)) -> Option<SpillEntry> {
        self.entries.remove(key)
    }

    fn keys(&self) -> impl Iterator<Item = &(Ino, u32)> {
        self.entries.keys()
    }

    fn iter(&self) -> impl Iterator<Item = (&(Ino, u32), &SpillEntry)> {
        self.entries.iter()
    }

    fn live_segment_count(&self) -> u64 {
        let mut ids = std::collections::HashSet::new();
        for entry in self.entries.values() {
            if let Some(segment_id) = parse_ingress_segment_id(&entry.path) {
                ids.insert(segment_id);
            }
        }
        ids.len() as u64
    }
}

#[derive(Debug, Clone)]
struct RecoveredIngressRecord {
    seq: u64,
    path: PathBuf,
    offset: u64,
    len: usize,
    created_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngressCheckpointEntry {
    ino: Ino,
    block_index: u32,
    seq: u64,
    segment_id: u64,
    offset: u64,
    len: usize,
    created_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngressCheckpointFile {
    version: u32,
    covered_segment_id: u64,
    covered_offset: u64,
    next_seq: u64,
    entries: Vec<IngressCheckpointEntry>,
}

#[derive(Debug, Clone, Copy)]
struct IngressCheckpointMeta {
    covered_segment_id: u64,
    covered_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngressStateFile {
    version: u32,
    active_segment_id: u64,
    next_seq: u64,
}

fn oldest_spill_age_ms(
    now_ms: u64,
    spill: &std::collections::HashMap<(Ino, u32), SpillEntry>,
) -> u64 {
    spill
        .values()
        .map(|entry| now_ms.saturating_sub(entry.created_unix_ms))
        .max()
        .unwrap_or(0)
}

fn ingress_checkpoint_due_with_policy(
    pending_bytes: u64,
    pending_mutations: u64,
    min_append_bytes: u64,
    min_mutations: u64,
    active_segment_lag: u64,
    max_segment_lag: u64,
    live_segment_count: u64,
    max_live_segments: u64,
    timer_fired: bool,
) -> bool {
    if pending_bytes == 0 && pending_mutations == 0 {
        return false;
    }
    timer_fired
        || pending_bytes >= min_append_bytes
        || pending_mutations >= min_mutations
        || active_segment_lag >= max_segment_lag
        || live_segment_count >= max_live_segments
}

fn read_ingress_entry_bytes(entry: &SpillEntry) -> VfsResult<Bytes> {
    let mut file = std::fs::File::open(&entry.path)?;
    file.seek(SeekFrom::Start(entry.offset))?;
    let mut buf = vec![0u8; entry.len];
    file.read_exact(&mut buf)?;
    Ok(Bytes::from(buf))
}

fn ingress_segment_path(dir: &std::path::Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{segment_id:016x}.seg"))
}

fn list_ingress_segment_files(segment_dir: &std::path::Path) -> VfsResult<Vec<(u64, PathBuf)>> {
    let mut files: Vec<(u64, PathBuf)> = match std::fs::read_dir(segment_dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() || path.extension().is_none_or(|ext| ext != "seg") {
                    return None;
                }
                let id = parse_ingress_segment_id(&path)?;
                Some((id, path))
            })
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(VfsError::Io(e)),
    };
    files.sort_unstable_by_key(|(id, _)| *id);
    Ok(files)
}

fn current_active_ingress_segment_id(segment_dir: &std::path::Path) -> VfsResult<u64> {
    let files = list_ingress_segment_files(segment_dir)?;
    Ok(files
        .last()
        .map(|(id, _)| *id)
        .unwrap_or(INITIAL_INGRESS_SEGMENT_ID))
}

fn active_ingress_segment_path(dir: &std::path::Path, active_segment_id: u64) -> PathBuf {
    ingress_segment_path(dir, active_segment_id)
}

fn latest_ingress_checkpoint_path(dir: &std::path::Path) -> PathBuf {
    dir.join("latest.json")
}

fn shutdown_metadata_flush_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("STUDIOFS_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS),
    )
}

fn run_sync_with_timeout<T: Send + 'static>(
    timeout: Duration,
    operation: impl FnOnce() -> VfsResult<T> + Send + 'static,
) -> VfsResult<Option<T>> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let result = operation();
        let _ = tx.send(result);
    });

    match rx.recv_timeout(timeout) {
        Ok(result) => result.map(Some),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Ok(None),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(VfsError::Other(
            "timed operation thread disconnected".to_string(),
        )),
    }
}

fn ingress_state_path(spill_dir: &std::path::Path) -> PathBuf {
    spill_dir.join("state.json")
}

fn parse_ingress_segment_id(path: &std::path::Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    u64::from_str_radix(stem, 16).ok()
}

fn load_latest_ingress_checkpoint(
    checkpoint_dir: &std::path::Path,
    segment_dir: &std::path::Path,
) -> VfsResult<Option<IngressCheckpointFile>> {
    let path = latest_ingress_checkpoint_path(checkpoint_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(VfsError::Io(e)),
    };
    let checkpoint: IngressCheckpointFile = serde_json::from_slice(&bytes)
        .map_err(|e| VfsError::Other(format!("invalid ingress checkpoint: {e}")))?;
    if checkpoint.version != 1 {
        return Err(VfsError::Other(format!(
            "unsupported ingress checkpoint version {}",
            checkpoint.version
        )));
    }
    for entry in &checkpoint.entries {
        let path = ingress_segment_path(segment_dir, entry.segment_id);
        if !path.exists() {
            return Err(VfsError::Other(format!(
                "missing ingress segment {:016x} referenced by checkpoint",
                entry.segment_id
            )));
        }
    }
    Ok(Some(checkpoint))
}

fn load_ingress_state(spill_dir: &std::path::Path) -> VfsResult<Option<IngressStateFile>> {
    let path = ingress_state_path(spill_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(VfsError::Io(e)),
    };
    let state: IngressStateFile = serde_json::from_slice(&bytes)
        .map_err(|e| VfsError::Other(format!("invalid ingress state: {e}")))?;
    if state.version != 1 {
        return Err(VfsError::Other(format!(
            "unsupported ingress state version {}",
            state.version
        )));
    }
    Ok(Some(state))
}

fn write_ingress_state(
    spill_dir: &std::path::Path,
    active_segment_id: u64,
    next_seq: u64,
) -> VfsResult<()> {
    std::fs::create_dir_all(spill_dir)?;
    let state = IngressStateFile {
        version: 1,
        active_segment_id,
        next_seq,
    };
    let tmp = spill_dir.join("state.json.tmp");
    let path = ingress_state_path(spill_dir);
    let bytes = serde_json::to_vec_pretty(&state)
        .map_err(|e| VfsError::Other(format!("encode ingress state failed: {e}")))?;
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn write_ingress_checkpoint(
    checkpoint_dir: &std::path::Path,
    ingress_live: &IngressLiveIndex,
    segment_dir: &std::path::Path,
    active_segment_id: u64,
    next_seq: u64,
) -> VfsResult<IngressCheckpointMeta> {
    std::fs::create_dir_all(checkpoint_dir)?;
    let active_segment_path = active_ingress_segment_path(segment_dir, active_segment_id);
    let covered_offset = std::fs::metadata(&active_segment_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    let mut entries = Vec::with_capacity(ingress_live.len());
    for (&(ino, block_index), entry) in ingress_live.iter() {
        let Some(segment_id) = parse_ingress_segment_id(&entry.path) else {
            continue;
        };
        entries.push(IngressCheckpointEntry {
            ino,
            block_index,
            seq: entry.seq,
            segment_id,
            offset: entry.offset,
            len: entry.len,
            created_unix_ms: entry.created_unix_ms,
        });
    }
    let checkpoint = IngressCheckpointFile {
        version: 1,
        covered_segment_id: active_segment_id,
        covered_offset,
        next_seq,
        entries,
    };
    let tmp = checkpoint_dir.join("latest.json.tmp");
    let path = latest_ingress_checkpoint_path(checkpoint_dir);
    let bytes = serde_json::to_vec_pretty(&checkpoint)
        .map_err(|e| VfsError::Other(format!("encode ingress checkpoint failed: {e}")))?;
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)?;
    let live_segment_ids: std::collections::HashSet<u64> = checkpoint
        .entries
        .iter()
        .map(|entry| entry.segment_id)
        .collect();
    for (segment_id, path) in list_ingress_segment_files(segment_dir)? {
        if segment_id < checkpoint.covered_segment_id && !live_segment_ids.contains(&segment_id) {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(IngressCheckpointMeta {
        covered_segment_id: checkpoint.covered_segment_id,
        covered_offset: checkpoint.covered_offset,
    })
}

fn append_ingress_record(
    segment_dir: &std::path::Path,
    segment_id: u64,
    key: (Ino, u32),
    seq: u64,
    created_unix_ms: u64,
    kind: u8,
    payload: &[u8],
) -> VfsResult<u64> {
    std::fs::create_dir_all(segment_dir)?;
    let path = ingress_segment_path(segment_dir, segment_id);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    let start = file.seek(SeekFrom::End(0))?;
    file.write_all(&INGRESS_SEGMENT_MAGIC)?;
    file.write_all(&[kind])?;
    file.write_all(&key.0.to_le_bytes())?;
    file.write_all(&key.1.to_le_bytes())?;
    file.write_all(&seq.to_le_bytes())?;
    file.write_all(&created_unix_ms.to_le_bytes())?;
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    if !payload.is_empty() {
        file.write_all(payload)?;
    }
    Ok(start + INGRESS_RECORD_HEADER_LEN as u64)
}

fn replay_ingress_segment_records(
    segment_dir: &std::path::Path,
    recovered: &mut HashMap<(Ino, u32), RecoveredIngressRecord>,
    next_seq: &mut u64,
    resume_after: Option<(u64, u64)>,
) -> VfsResult<()> {
    for (id, path) in list_ingress_segment_files(segment_dir)? {
        if let Some((covered_segment_id, covered_offset)) = resume_after {
            if id < covered_segment_id {
                continue;
            }
            if id == covered_segment_id && covered_offset > 0 {
                let file_len = std::fs::metadata(&path)?.len();
                if covered_offset >= file_len {
                    continue;
                }
            }
        }
        let mut file = std::fs::File::open(&path)?;
        if let Some((covered_segment_id, covered_offset)) = resume_after
            && id == covered_segment_id
            && covered_offset > 0
        {
            file.seek(SeekFrom::Start(covered_offset))?;
        }
        loop {
            let record_start = file.stream_position()?;
            let mut header = [0u8; INGRESS_RECORD_HEADER_LEN];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(VfsError::Io(e)),
            }
            if header[0..4] != INGRESS_SEGMENT_MAGIC {
                return Err(VfsError::Other("invalid ingress segment magic".into()));
            }
            let kind = header[4];
            let ino = u64::from_le_bytes(header[5..13].try_into().unwrap());
            let block_index = u32::from_le_bytes(header[13..17].try_into().unwrap());
            let seq = u64::from_le_bytes(header[17..25].try_into().unwrap());
            let created_unix_ms = u64::from_le_bytes(header[25..33].try_into().unwrap());
            let payload_len = u32::from_le_bytes(header[33..37].try_into().unwrap()) as usize;
            *next_seq = (*next_seq).max(seq.saturating_add(1));
            let payload_offset = record_start + INGRESS_RECORD_HEADER_LEN as u64;
            if payload_len > 0 {
                file.seek(SeekFrom::Current(payload_len as i64))?;
            }
            let key = (ino, block_index);
            match kind {
                INGRESS_RECORD_PUT => {
                    let should_replace = recovered
                        .get(&key)
                        .map(|current| seq >= current.seq)
                        .unwrap_or(true);
                    if should_replace {
                        recovered.insert(
                            key,
                            RecoveredIngressRecord {
                                seq,
                                path: path.clone(),
                                offset: payload_offset,
                                len: payload_len,
                                created_unix_ms,
                            },
                        );
                    }
                }
                INGRESS_RECORD_INVALIDATE => {
                    let should_replace = recovered
                        .get(&key)
                        .map(|current| seq >= current.seq)
                        .unwrap_or(true);
                    if should_replace {
                        recovered.insert(
                            key,
                            RecoveredIngressRecord {
                                seq,
                                path: path.clone(),
                                offset: payload_offset,
                                len: 0,
                                created_unix_ms,
                            },
                        );
                    }
                }
                _ => {
                    return Err(VfsError::Other(
                        "unknown ingress segment record kind".into(),
                    ));
                }
            }
        }
    }

    recovered.retain(|_, record| record.len > 0);
    Ok(())
}

#[cfg(test)]
fn recover_ingress_segment_records(
    segment_dir: &std::path::Path,
) -> VfsResult<(HashMap<(Ino, u32), RecoveredIngressRecord>, u64)> {
    let mut recovered = HashMap::<(Ino, u32), RecoveredIngressRecord>::new();
    let mut next_seq = 1u64;
    replay_ingress_segment_records(segment_dir, &mut recovered, &mut next_seq, None)?;
    Ok((recovered, next_seq))
}

fn recover_spill_dir(
    spill_dir: &std::path::Path,
) -> VfsResult<(
    std::collections::HashMap<(Ino, u32), SpillEntry>,
    usize,
    u64,
    u64,
)> {
    let segment_dir = spill_dir.join("segments");
    let checkpoint_dir = spill_dir.join("checkpoints");
    let mut recovered = HashMap::<(Ino, u32), RecoveredIngressRecord>::new();
    let mut next_seq = 1u64;
    let mut active_segment_id = INITIAL_INGRESS_SEGMENT_ID;
    let mut resume_after = None;

    if let Some(state) = load_ingress_state(spill_dir)? {
        next_seq = next_seq.max(state.next_seq);
        active_segment_id = active_segment_id.max(state.active_segment_id);
    }

    if let Some(checkpoint) = load_latest_ingress_checkpoint(&checkpoint_dir, &segment_dir)? {
        next_seq = next_seq.max(checkpoint.next_seq);
        resume_after = Some((checkpoint.covered_segment_id, checkpoint.covered_offset));
        for entry in checkpoint.entries {
            recovered.insert(
                (entry.ino, entry.block_index),
                RecoveredIngressRecord {
                    seq: entry.seq,
                    path: ingress_segment_path(&segment_dir, entry.segment_id),
                    offset: entry.offset,
                    len: entry.len,
                    created_unix_ms: entry.created_unix_ms,
                },
            );
        }
    }
    replay_ingress_segment_records(&segment_dir, &mut recovered, &mut next_seq, resume_after)?;

    let mut total_bytes = 0usize;
    let mut spill_entries = std::collections::HashMap::new();
    for (key, record) in recovered {
        let spill = SpillEntry {
            seq: record.seq,
            path: record.path,
            offset: record.offset,
            len: record.len,
            created_unix_ms: record.created_unix_ms,
        };
        total_bytes = total_bytes.saturating_add(spill.len);
        spill_entries.insert(key, spill);
    }

    active_segment_id = active_segment_id.max(current_active_ingress_segment_id(&segment_dir)?);
    Ok((spill_entries, total_bytes, next_seq, active_segment_id))
}

// =============================================================================
// Internal State (for Clone support)
// =============================================================================

/// Internal state that can be cloned and shared.
struct VfsState {
    /// Pending blocks: (inode, block_index) -> PendingBlock
    pending_blocks: Mutex<std::collections::HashMap<(Ino, u32), PendingBlock>>,
    /// Explicit per-inode pending/committed incarnation model for local serving.
    inode_incarnations: Mutex<std::collections::HashMap<Ino, InodeIncarnationState>>,
    /// Live extent identity per inode/block used to invalidate superseded uploads.
    live_extent_records: Mutex<std::collections::HashMap<(Ino, u32), ExtentRecordId>>,
    /// Successfully uploaded live extents waiting for atomic file-level publish.
    uploaded_extent_records: Mutex<std::collections::HashMap<(Ino, u32), ExtentRecordId>>,
    /// Dirty attributes cache (from writes not yet synced to redb).
    dirty_attrs: Mutex<std::collections::HashMap<Ino, InodeAttr>>,
    /// Read-through attribute cache with TTL: inode -> (attr, timestamp)
    attr_read_cache: Mutex<std::collections::HashMap<Ino, (InodeAttr, Instant)>>,
    /// Dirty blocks accepted by `flush()` that have not yet been written to
    /// disk by the background flusher.  Reads must consult this buffer
    /// BEFORE falling through to the block cache or S3.
    dirty_mem: Mutex<std::collections::HashMap<(Ino, u32), Bytes>>,
    /// Live accepted ingress records keyed by `(ino, block_index)`.
    ingress_live: Mutex<IngressLiveIndex>,
    /// Serialize append/rotation decisions for ingress segment writes.
    ingress_segment_write_lock: Mutex<()>,
    /// Approximate total bytes currently held in `dirty_mem` (for backpressure).
    dirty_bytes: AtomicUsize,
    /// Approximate total bytes currently held in the local ingress live index.
    dirty_spill_bytes: AtomicUsize,
    /// Number of blocks forced into spill because RAM acceptance was full.
    forced_spill_blocks: AtomicU64,
    /// Total blocks drained from RAM accepted state.
    drained_mem_blocks: AtomicU64,
    /// Total bytes drained from RAM accepted state.
    drained_mem_bytes: AtomicU64,
    /// Total blocks drained from spill accepted state.
    drained_spill_blocks: AtomicU64,
    /// Total bytes drained from spill accepted state.
    drained_spill_bytes: AtomicU64,
    /// Total time foreground writers spent waiting on local backpressure.
    write_backpressure_wait_ms: AtomicU64,
    /// Notification fired by the background flusher each time it drains blocks
    /// from `dirty_mem`, used for low-latency backpressure wakeup in `write()`
    /// and `flush()` instead of polling with `sleep(5ms)`.
    dirty_drained_notify: tokio::sync::Notify,
    /// Serialize dirty buffer drainers so a foreground writer can assist the
    /// background flusher without duplicating metadata work for the same keys.
    dirty_drain_lock: tokio::sync::Mutex<()>,
    /// Sequence used to generate unique spill file names.
    spill_seq: AtomicU64,
    /// Current active append-only ingress segment id.
    active_ingress_segment_id: AtomicU64,
    /// Inodes that have been unlinked.  The background flusher checks this
    /// set before calling `flush_block()` to avoid re-inserting evicted blocks
    /// into the cache (TOCTOU race between `drain_dirty_mem` and `unlink`).
    deleted_inodes: Mutex<std::collections::HashSet<Ino>>,
    /// Object IDs that should be removed from the block cache after their
    /// pending upload completes (or is cancelled).  Populated by
    /// `evict_inode_from_caches()` when it encounters a pinned block.
    pending_evictions: Mutex<std::collections::HashSet<u64>>,
    /// Number of upload operations requeued after process failures.
    upload_requeues: AtomicU64,
    /// Upload operations canceled because inode was deleted/missing.
    upload_canceled_deleted: AtomicU64,
    /// Upload operations canceled because the block was superseded by a newer one.
    upload_canceled_stale: AtomicU64,
    /// Number of foreground reads currently in progress.
    foreground_reads_inflight: AtomicUsize,
    /// Last time a foreground read was observed (unix millis).
    last_foreground_read_ms: AtomicU64,
    /// Flag set at shutdown to stop the background slab flusher so it cannot
    /// race with the shutdown drain.
    slab_flusher_stopped: std::sync::atomic::AtomicBool,
    /// Flag set at shutdown to stop periodic ingress checkpoint persistence.
    ingress_checkpoint_stopped: std::sync::atomic::AtomicBool,
    /// Successful ingress checkpoint persists since startup.
    ingress_checkpoint_persists: AtomicU64,
    /// Failed ingress checkpoint persists since startup.
    ingress_checkpoint_failures: AtomicU64,
    /// Timestamp of the last successful ingress checkpoint.
    ingress_last_checkpoint_unix_ms: AtomicU64,
    /// Covered segment id of the last successful ingress checkpoint.
    ingress_last_checkpoint_segment_id: AtomicU64,
    /// Covered offset of the last successful ingress checkpoint.
    ingress_last_checkpoint_offset: AtomicU64,
    /// Bytes appended to ingress records since the last successful checkpoint.
    ingress_checkpoint_pending_bytes: AtomicU64,
    /// Record mutations appended since the last successful checkpoint.
    ingress_checkpoint_pending_mutations: AtomicU64,
    /// Wake periodic checkpoint worker when ingress churn crosses policy thresholds.
    ingress_checkpoint_notify: tokio::sync::Notify,
    /// File-level locks (flock semantics).
    file_locks: Mutex<std::collections::HashMap<Ino, FileLock>>,
    /// Notification for waiters blocked on flock.
    lock_notify: tokio::sync::Notify,
    /// Global shutdown gate for non-uploader background loops.
    shutdown_requested: std::sync::atomic::AtomicBool,
    /// Global shutdown gate for upload dispatch/worker tasks.
    uploader_shutdown_requested: std::sync::atomic::AtomicBool,
    /// Marks that `StudioVfs::shutdown()` has finished once successfully.
    shutdown_completed: std::sync::atomic::AtomicBool,
    /// Wake non-uploader background loops so shutdown does not wait for long sleeps.
    shutdown_notify: tokio::sync::Notify,
    /// Wake uploader dispatch/worker loops so shutdown can stop queue consumption.
    uploader_shutdown_notify: tokio::sync::Notify,
    /// Join handles for top-level non-uploader background tasks.
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    /// Top-level uploader task; it owns and joins its worker pool internally.
    uploader_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
struct JanitorLease {
    op: Operator,
    lock_path: String,
    owner_id: String,
}

// =============================================================================
// StudioVfs Implementation
// =============================================================================

/// The core VFS - satisfies filesystem operations using metadata + data + cache.
pub struct StudioVfs {
    /// Metadata engine.
    meta: Arc<dyn MetaEngine>,
    /// Data store for writes (uploads, deletes, GC).
    data_write: Arc<DataStore>,
    /// Unified two-level block cache (L1 RAM + L2 NVMe) with upload queue.
    block_cache: Arc<BlockCache>,
    /// Block size in bytes.
    block_size: u32,
    /// Volume data prefix for S3 object keys.
    data_prefix: String,
    /// Internal state (mutable).
    state: Arc<VfsState>,
    // --- Upload Control ---
    /// Adaptive upload scheduler (AIMD + retries).
    upload_scheduler: Arc<crate::upload::UploadScheduler>,
    upload_delay: Duration,
    upload_requeue_base: Duration,
    upload_requeue_max: Duration,
    upload_stats_interval: Duration,
    slab_flush_interval: Duration,
    ingress_checkpoint_interval: Duration,
    ingress_checkpoint_min_append_bytes: u64,
    ingress_checkpoint_min_mutations: u64,
    ingress_checkpoint_max_segment_lag: u64,
    ingress_checkpoint_max_live_segments: u64,
    /// Shared local-I/O budget for upload path reads from cache.
    upload_cache_io_slots: Arc<Semaphore>,
    qos: Arc<QosController>,
    /// Extra cost per upload op when foreground reads are active.
    upload_cache_io_user_penalty: u32,
    /// Grace window after the last foreground read to keep user priority.
    upload_user_idle_grace: Duration,
    /// Sequential read-ahead tracker.
    readahead: Arc<ReadAheadTracker>,
    /// Directory-level prefetch tracker.
    dir_prefetch: Arc<DirAccessTracker>,
    /// Channel sender for scheduling background prefetch tasks.
    /// The receiver lives in a spawned task that has an Arc<Self>.
    prefetch_tx: tokio::sync::mpsc::UnboundedSender<(Ino, u32)>,
    /// Signal sender to wake the background flusher when new dirty blocks
    /// are buffered.
    dirty_flush_tx: tokio::sync::mpsc::UnboundedSender<()>,
    /// Maximum total bytes allowed in the dirty-write memory buffer before
    /// `flush()` / `write()` apply backpressure.
    max_dirty_bytes: usize,
    /// Maximum total bytes allowed in local NVMe spill before foreground
    /// writes block waiting for the pipeline to catch up.
    max_spill_bytes: usize,
    max_ingress_segment_bytes: u64,
    ingress_spill_dir: PathBuf,
    /// Append-only ingress segment directory for the next storage phase.
    ingress_segment_dir: PathBuf,
    /// Checkpoint directory for bounded ingress replay recovery.
    ingress_checkpoint_dir: PathBuf,
    /// Handle table: tracks all open file descriptors.
    handle_table: HandleTable,
    /// Optional distributed lease for janitor leader election.
    janitor_lease: Option<JanitorLease>,
    janitor_interval: Duration,
    janitor_skip_newer_than: Duration,
    janitor_max_deletes_per_run: u64,
    janitor_lease_ttl: Duration,
    prefetch_concurrency: usize,
    dir_prefetch_max_batch: usize,
    prefetch_foreground_reserve: usize,
    prefetch_max_per_cycle: usize,
    prefetch_pause_on_neterr: Duration,
    /// Adaptive fetch scheduler (AIMD + singleflight).
    /// Replaces the old `FetchThrottle`, `inflight_fetches`, and VFS-retry.
    fetch_scheduler: Arc<crate::fetch::FetchScheduler>,
    /// Optional serverless distributed lock manager for bimodal consistency.
    distributed_flock: Option<DistributedFlock>,
    /// Optional kernel cache invalidator (set by platform bridge after mount).
    kernel_invalidator: std::sync::RwLock<Option<Arc<dyn KernelInvalidator>>>,
}

impl StudioVfs {
    fn restore_live_extents_from_pending_uploads(&self) {
        let pending = self.block_cache.snapshot_pending_uploads();
        if pending.is_empty() {
            return;
        }

        let mut restored = 0usize;
        let mut attrs = HashMap::<Ino, Option<InodeAttr>>::new();
        for op in pending {
            let block_index = block_index_from_offset(op.offset, self.block_size);
            let attr = attrs
                .entry(op.ino)
                .or_insert_with(|| self.meta.get_attr(op.ino).ok());
            let Some(attr) = attr.as_ref() else {
                continue;
            };
            if attr.content_epoch != op.content_epoch {
                continue;
            }

            let current = match self.meta.get_file_block(op.ino, block_index) {
                Ok(Some(block_ref)) => block_ref,
                _ => continue,
            };
            if current.object_id != op.object_id {
                continue;
            }

            self.record_live_extent(op.ino, block_index, op.object_id, op.content_epoch);
            restored += 1;
        }

        if restored > 0 {
            info!(
                restored_live_extents = restored,
                "restored pending live extents from upload journal"
            );
        }
    }

    /// Create a new VFS instance.
    pub fn new(
        meta: Arc<dyn MetaEngine>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        memory_limit: u64,
    ) -> VfsResult<Arc<Self>> {
        {
            // Get volume info
            let format_info = meta.get_format_info()?;
            let data_prefix = if format_info.data_prefix.trim().is_empty() {
                format!("{}/objects/", format_info.name)
            } else if format_info.data_prefix.ends_with('/') {
                format_info.data_prefix.clone()
            } else {
                format!("{}/", format_info.data_prefix)
            };
            let block_size = format_info.block_size;
            let janitor_interval = Duration::from_secs(
                std::env::var("STUDIOFS_JANITOR_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_JANITOR_INTERVAL.as_secs()),
            );
            let janitor_skip_newer_than = Duration::from_secs(
                std::env::var("STUDIOFS_JANITOR_SKIP_NEWER_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_JANITOR_SKIP_NEWER_THAN.as_secs()),
            );
            let janitor_max_deletes_per_run = std::env::var("STUDIOFS_JANITOR_MAX_DELETES")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_JANITOR_MAX_DELETES_PER_RUN);
            let janitor_lease_ttl = Duration::from_secs(
                std::env::var("STUDIOFS_JANITOR_LEASE_TTL_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_JANITOR_LEASE_TTL.as_secs()),
            );
            let metadata_invalidation_sync_interval = Duration::from_millis(
                std::env::var("STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_METADATA_INVALIDATION_SYNC_INTERVAL_MS),
            );
            let read_buffer_mb = std::env::var("STUDIOFS_READ_BUFFER_MB")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_READ_BUFFER_MB);
            let block_size_nonzero = block_size.max(1) as u128;
            let mb_to_blocks = |mb: u64| -> u32 {
                let bytes = (mb as u128) * 1024 * 1024;
                let blocks = (bytes / block_size_nonzero).max(1).min(u32::MAX as u128);
                blocks as u32
            };
            let buffer_blocks_default = mb_to_blocks(read_buffer_mb).max(32);
            let prefetch_concurrency_default = DEFAULT_PREFETCH_CONCURRENCY;

            let prefetch_concurrency = std::env::var("STUDIOFS_PREFETCH_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(prefetch_concurrency_default.max(16));
            let readahead_width = std::env::var("STUDIOFS_READAHEAD_WIDTH")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_READAHEAD_WIDTH);
            let min_readahead_prefetch_floor = DEFAULT_READAHEAD_MIN_PREFETCH.min(16);
            let readahead_min_prefetch = std::env::var("STUDIOFS_READAHEAD_MIN_PREFETCH")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_READAHEAD_MIN_PREFETCH.max(min_readahead_prefetch_floor));
            let readahead_seq_step = std::env::var("STUDIOFS_READAHEAD_SEQ_STEP")
                .ok()
                .and_then(|v| v.parse::<u8>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_READAHEAD_SEQ_STEP);
            let readahead_random_penalty = std::env::var("STUDIOFS_READAHEAD_RANDOM_PENALTY")
                .ok()
                .and_then(|v| v.parse::<u8>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_READAHEAD_RANDOM_PENALTY);
            let dir_prefetch_max_batch = std::env::var("STUDIOFS_DIR_PREFETCH_MAX_BATCH")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(DEFAULT_DIR_PREFETCH_MAX_BATCH);
            let prefetch_foreground_reserve_default =
                (prefetch_concurrency / 2).clamp(16, (buffer_blocks_default as usize).max(16));
            let prefetch_foreground_reserve = std::env::var("STUDIOFS_PREFETCH_FOREGROUND_RESERVE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(
                    prefetch_foreground_reserve_default.max(DEFAULT_PREFETCH_FOREGROUND_RESERVE),
                );
            let prefetch_max_per_cycle_default = DEFAULT_PREFETCH_MAX_PER_CYCLE;
            let min_prefetch_cycle_floor = DEFAULT_PREFETCH_MAX_PER_CYCLE.min(32);
            let prefetch_max_per_cycle = std::env::var("STUDIOFS_PREFETCH_MAX_PER_CYCLE")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(prefetch_max_per_cycle_default.max(min_prefetch_cycle_floor));
            let prefetch_pause_on_neterr = Duration::from_millis(
                std::env::var("STUDIOFS_PREFETCH_PAUSE_ON_NETERR_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_PREFETCH_PAUSE_ON_NETERR_MS),
            );
            let _lock_lease_ttl = Duration::from_secs(
                std::env::var("STUDIOFS_LOCK_TTL_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_LOCK_LEASE_TTL.as_secs()),
            );
            let lock_retry_base_ms = std::env::var("STUDIOFS_LOCK_RETRY_BASE_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_LOCK_RETRY_BASE_MS);
            let _lock_retry_max_ms = std::env::var("STUDIOFS_LOCK_RETRY_MAX_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v >= lock_retry_base_ms)
                .unwrap_or(DEFAULT_LOCK_RETRY_MAX_MS.max(lock_retry_base_ms));

            let janitor_lease = None;
            let distributed_flock = None;
            let max_dirty_bytes = std::env::var("STUDIOFS_MAX_DIRTY_BYTES")
                .ok()
                .and_then(|v| parse_size_bytes_env(&v))
                .filter(|v| *v > 0)
                .unwrap_or(12 * 1024 * 1024 * 1024);
            let max_spill_bytes = std::env::var("STUDIOFS_MAX_SPILL_BYTES")
                .ok()
                .and_then(|v| parse_size_bytes_env(&v))
                .filter(|v| *v > 0)
                .unwrap_or(64 * 1024 * 1024 * 1024usize);
            let max_ingress_segment_bytes = std::env::var("STUDIOFS_INGRESS_SEGMENT_MAX_BYTES")
                .ok()
                .and_then(|v| parse_size_bytes_env(&v))
                .filter(|v| *v > 0)
                .map(|v| v as u64)
                .unwrap_or(DEFAULT_INGRESS_SEGMENT_MAX_BYTES);
            let spill_dir = PathBuf::from(cache_dir).join("ingress_spill");
            std::fs::create_dir_all(&spill_dir).map_err(VfsError::Io)?;
            let ingress_segment_dir = spill_dir.join("segments");
            std::fs::create_dir_all(&ingress_segment_dir).map_err(VfsError::Io)?;
            let ingress_checkpoint_dir = spill_dir.join("checkpoints");
            std::fs::create_dir_all(&ingress_checkpoint_dir).map_err(VfsError::Io)?;
            let existing_checkpoint =
                load_latest_ingress_checkpoint(&ingress_checkpoint_dir, &ingress_segment_dir)?;
            let (
                recovered_spill,
                recovered_spill_bytes,
                recovered_spill_seq,
                active_ingress_segment_id,
            ) = recover_spill_dir(&spill_dir)?;
            let initial_checkpoint_meta = existing_checkpoint.map(|checkpoint| {
                let checkpoint_path = latest_ingress_checkpoint_path(&ingress_checkpoint_dir);
                let checkpoint_unix_ms = std::fs::metadata(checkpoint_path)
                    .ok()
                    .and_then(|meta| meta.modified().ok())
                    .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                (
                    checkpoint_unix_ms,
                    checkpoint.covered_segment_id,
                    checkpoint.covered_offset,
                )
            });

            // Init unified block cache (L1 RAM + L2 NVMe + upload queue)
            let cache_config = BlockCacheConfig::new(cache_dir, memory_limit, cache_size);
            let (block_cache, upload_rx) =
                BlockCache::open(cache_config).map_err(|e| VfsError::Io(e))?;
            block_cache.spawn_background_tasks();

            // Init ReadAhead tracker + prefetch channel
            let readahead = Arc::new(ReadAheadTracker::new_with_tuning(
                readahead_width,
                readahead_min_prefetch,
                readahead_seq_step,
                readahead_random_penalty,
            ));
            let dir_prefetch = Arc::new(DirAccessTracker::new(2));
            let (prefetch_tx, mut prefetch_rx) =
                tokio::sync::mpsc::unbounded_channel::<(Ino, u32)>();

            // Dirty-write memory buffer + background flusher channel.
            let (dirty_flush_tx, dirty_flush_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

            // Create a read-optimised DataStore with dedicated connection pool and
            // tighter timeouts so that background uploads can never starve reads.
            let data_read = Arc::new(
                DataStore::with_profile(data_config, IoProfile::Read).map_err(|e| {
                    VfsError::Other(format!("failed to create read data store: {e}"))
                })?,
            );

            // Create a write-optimised DataStore for uploads/deletes.
            let data_write = Arc::new(
                DataStore::with_profile(data_config, IoProfile::Write).map_err(|e| {
                    VfsError::Other(format!("failed to create write data store: {e}"))
                })?,
            );

            // Build AIMD config for the fetch scheduler from env or defaults.
            let fetch_aimd_config = {
                let fetch_max_window_default = (buffer_blocks_default as f64).clamp(64.0, 4096.0);
                let fetch_initial_window_default =
                    (fetch_max_window_default / 8.0).clamp(16.0, 256.0);
                let fetch_increase_default = if fetch_max_window_default >= 1024.0 {
                    4.0
                } else {
                    2.0
                };
                let initial = std::env::var("STUDIOFS_FETCH_INITIAL_WINDOW")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(fetch_initial_window_default);
                let min = std::env::var("STUDIOFS_FETCH_MIN_WINDOW")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(1.0);
                let max = std::env::var("STUDIOFS_FETCH_MAX_WINDOW")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(fetch_max_window_default);
                let increase = std::env::var("STUDIOFS_FETCH_INCREASE_STEP")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(fetch_increase_default);
                let decrease = std::env::var("STUDIOFS_FETCH_DECREASE_FACTOR")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.85);
                let cooldown_secs = std::env::var("STUDIOFS_FETCH_DECREASE_COOLDOWN_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(3);
                crate::aimd::AimdConfig {
                    initial_window: initial,
                    min_window: min,
                    max_window: max,
                    increase_step: increase,
                    decrease_factor: decrease,
                    decrease_cooldown: Duration::from_secs(cooldown_secs),
                }
            };

            let qos = Arc::new(QosController::from_env());
            let fetch_scheduler = Arc::new(crate::fetch::FetchScheduler::new(
                data_read,
                block_cache.clone(),
                fetch_aimd_config,
                prefetch_foreground_reserve,
                qos.clone(),
            ));

            // Build AIMD config for upload scheduling from env or defaults.
            let upload_aimd_config = {
                let initial = std::env::var("STUDIOFS_UPLOAD_INITIAL_WINDOW")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(16.0);
                let min = std::env::var("STUDIOFS_UPLOAD_MIN_WINDOW")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(1.0);
                let max = std::env::var("STUDIOFS_UPLOAD_MAX_WINDOW")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(512.0);
                let increase = std::env::var("STUDIOFS_UPLOAD_INCREASE_STEP")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(2.0);
                let decrease = std::env::var("STUDIOFS_UPLOAD_DECREASE_FACTOR")
                    .ok()
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.85);
                let cooldown_secs = std::env::var("STUDIOFS_UPLOAD_DECREASE_COOLDOWN_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(3);
                crate::aimd::AimdConfig {
                    initial_window: initial,
                    min_window: min,
                    max_window: max,
                    increase_step: increase,
                    decrease_factor: decrease,
                    decrease_cooldown: Duration::from_secs(cooldown_secs),
                }
            };
            let upload_scheduler = Arc::new(crate::upload::UploadScheduler::new(
                data_write.clone(),
                upload_aimd_config,
            ));
            let upload_delay = Duration::from_millis(
                std::env::var("STUDIOFS_UPLOAD_DELAY_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_UPLOAD_DELAY_MS),
            );
            let upload_requeue_base = Duration::from_millis(
                std::env::var("STUDIOFS_UPLOAD_REQUEUE_BASE_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_UPLOAD_REQUEUE_BASE_MS),
            );
            let upload_requeue_max = Duration::from_millis(
                std::env::var("STUDIOFS_UPLOAD_REQUEUE_MAX_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v >= upload_requeue_base.as_millis() as u64)
                    .unwrap_or(
                        DEFAULT_UPLOAD_REQUEUE_MAX_MS.max(upload_requeue_base.as_millis() as u64),
                    ),
            );
            let upload_stats_interval = Duration::from_secs(
                std::env::var("STUDIOFS_UPLOAD_STATS_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_UPLOAD_STATS_INTERVAL_SECS),
            );
            let slab_flush_interval = Duration::from_millis(
                std::env::var("STUDIOFS_SLAB_FLUSH_INTERVAL_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(10),
            );
            let disable_background_slab_flush =
                std::env::var("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH")
                    .ok()
                    .is_some_and(|v| {
                        let v = v.trim().to_ascii_lowercase();
                        v == "1" || v == "true" || v == "yes" || v == "on"
                    });
            let ingress_checkpoint_interval = Duration::from_secs(
                std::env::var("STUDIOFS_INGRESS_CHECKPOINT_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_INGRESS_CHECKPOINT_INTERVAL_SECS),
            );
            let ingress_checkpoint_min_append_bytes =
                std::env::var("STUDIOFS_INGRESS_CHECKPOINT_MIN_APPEND_BYTES")
                    .ok()
                    .and_then(|v| parse_size_bytes_env(&v))
                    .filter(|v| *v > 0)
                    .map(|v| v as u64)
                    .unwrap_or(DEFAULT_INGRESS_CHECKPOINT_MIN_APPEND_BYTES);
            let ingress_checkpoint_min_mutations =
                std::env::var("STUDIOFS_INGRESS_CHECKPOINT_MIN_MUTATIONS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_INGRESS_CHECKPOINT_MIN_MUTATIONS);
            let ingress_checkpoint_max_segment_lag =
                std::env::var("STUDIOFS_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG);
            let ingress_checkpoint_max_live_segments =
                std::env::var("STUDIOFS_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS);
            let upload_cache_io_slots = std::env::var("STUDIOFS_UPLOAD_CACHE_IO_SLOTS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_UPLOAD_CACHE_IO_SLOTS);
            let upload_cache_io_user_penalty =
                std::env::var("STUDIOFS_UPLOAD_CACHE_IO_USER_PENALTY")
                    .ok()
                    .and_then(|v| v.parse::<u32>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_UPLOAD_CACHE_IO_USER_PENALTY)
                    .min(upload_cache_io_slots as u32);
            let upload_user_idle_grace = Duration::from_millis(
                std::env::var("STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_UPLOAD_USER_IDLE_GRACE_MS),
            );
            info!(
                writeback = true,
                upload_delay_ms = upload_delay.as_millis() as u64,
                requeue_base_ms = upload_requeue_base.as_millis() as u64,
                requeue_max_ms = upload_requeue_max.as_millis() as u64,
                upload_stats_interval_secs = upload_stats_interval.as_secs(),
                slab_flush_interval_ms = slab_flush_interval.as_millis() as u64,
                ingress_checkpoint_interval_secs = ingress_checkpoint_interval.as_secs(),
                ingress_checkpoint_min_append_bytes,
                ingress_checkpoint_min_mutations,
                ingress_checkpoint_max_segment_lag,
                ingress_checkpoint_max_live_segments,
                upload_cache_io_slots,
                upload_cache_io_user_penalty,
                upload_user_idle_grace_ms = upload_user_idle_grace.as_millis() as u64,
                max_dirty_bytes,
                max_spill_bytes,
                "upload pipeline v2 enabled"
            );

            let vfs = Arc::new(Self {
                meta,
                data_write,
                block_cache,
                block_size,
                data_prefix,
                state: Arc::new(VfsState {
                    pending_blocks: Mutex::new(std::collections::HashMap::new()),
                    inode_incarnations: Mutex::new(std::collections::HashMap::new()),
                    live_extent_records: Mutex::new(std::collections::HashMap::new()),
                    uploaded_extent_records: Mutex::new(std::collections::HashMap::new()),
                    dirty_attrs: Mutex::new(std::collections::HashMap::new()),
                    attr_read_cache: Mutex::new(std::collections::HashMap::new()),
                    dirty_mem: Mutex::new(std::collections::HashMap::new()),
                    ingress_live: Mutex::new(IngressLiveIndex {
                        entries: recovered_spill,
                    }),
                    ingress_segment_write_lock: Mutex::new(()),
                    dirty_bytes: AtomicUsize::new(0),
                    dirty_spill_bytes: AtomicUsize::new(recovered_spill_bytes),
                    forced_spill_blocks: AtomicU64::new(0),
                    drained_mem_blocks: AtomicU64::new(0),
                    drained_mem_bytes: AtomicU64::new(0),
                    drained_spill_blocks: AtomicU64::new(0),
                    drained_spill_bytes: AtomicU64::new(0),
                    write_backpressure_wait_ms: AtomicU64::new(0),
                    dirty_drained_notify: tokio::sync::Notify::new(),
                    dirty_drain_lock: tokio::sync::Mutex::new(()),
                    spill_seq: AtomicU64::new(recovered_spill_seq),
                    active_ingress_segment_id: AtomicU64::new(active_ingress_segment_id),
                    deleted_inodes: Mutex::new(std::collections::HashSet::new()),
                    pending_evictions: Mutex::new(std::collections::HashSet::new()),
                    upload_requeues: AtomicU64::new(0),
                    upload_canceled_deleted: AtomicU64::new(0),
                    upload_canceled_stale: AtomicU64::new(0),
                    foreground_reads_inflight: AtomicUsize::new(0),
                    last_foreground_read_ms: AtomicU64::new(0),
                    slab_flusher_stopped: std::sync::atomic::AtomicBool::new(false),
                    ingress_checkpoint_stopped: std::sync::atomic::AtomicBool::new(false),
                    ingress_checkpoint_persists: AtomicU64::new(0),
                    ingress_checkpoint_failures: AtomicU64::new(0),
                    ingress_last_checkpoint_unix_ms: AtomicU64::new(
                        initial_checkpoint_meta
                            .map(|(unix_ms, _, _)| unix_ms)
                            .unwrap_or(0),
                    ),
                    ingress_last_checkpoint_segment_id: AtomicU64::new(
                        initial_checkpoint_meta
                            .map(|(_, segment_id, _)| segment_id)
                            .unwrap_or(0),
                    ),
                    ingress_last_checkpoint_offset: AtomicU64::new(
                        initial_checkpoint_meta
                            .map(|(_, _, offset)| offset)
                            .unwrap_or(0),
                    ),
                    ingress_checkpoint_pending_bytes: AtomicU64::new(0),
                    ingress_checkpoint_pending_mutations: AtomicU64::new(0),
                    ingress_checkpoint_notify: tokio::sync::Notify::new(),
                    file_locks: Mutex::new(std::collections::HashMap::new()),
                    lock_notify: tokio::sync::Notify::new(),
                    shutdown_requested: std::sync::atomic::AtomicBool::new(false),
                    uploader_shutdown_requested: std::sync::atomic::AtomicBool::new(false),
                    shutdown_completed: std::sync::atomic::AtomicBool::new(false),
                    shutdown_notify: tokio::sync::Notify::new(),
                    uploader_shutdown_notify: tokio::sync::Notify::new(),
                    background_tasks: Mutex::new(Vec::new()),
                    uploader_task: Mutex::new(None),
                }),
                upload_scheduler,
                upload_delay,
                upload_requeue_base,
                upload_requeue_max,
                upload_stats_interval,
                slab_flush_interval,
                ingress_checkpoint_interval,
                ingress_checkpoint_min_append_bytes,
                ingress_checkpoint_min_mutations,
                ingress_checkpoint_max_segment_lag,
                ingress_checkpoint_max_live_segments,
                upload_cache_io_slots: Arc::new(Semaphore::new(upload_cache_io_slots)),
                qos,
                upload_cache_io_user_penalty,
                upload_user_idle_grace,
                readahead: readahead.clone(),
                dir_prefetch,
                prefetch_tx,
                dirty_flush_tx,
                max_dirty_bytes,
                max_spill_bytes,
                max_ingress_segment_bytes,
                ingress_spill_dir: spill_dir,
                ingress_segment_dir,
                ingress_checkpoint_dir,
                handle_table: HandleTable::new(),
                janitor_lease,
                janitor_interval,
                janitor_skip_newer_than,
                janitor_max_deletes_per_run,
                janitor_lease_ttl,
                prefetch_concurrency,
                dir_prefetch_max_batch,
                prefetch_foreground_reserve,
                prefetch_max_per_cycle,
                prefetch_pause_on_neterr,
                fetch_scheduler: fetch_scheduler.clone(),
                distributed_flock,
                kernel_invalidator: std::sync::RwLock::new(None),
            });

            vfs.restore_live_extents_from_pending_uploads();

            // Spawn prefetch worker — holds Arc<Self> so it can call read_block()
            let vfs_for_prefetch = vfs.clone();
            let prefetch_limit = vfs.prefetch_concurrency.max(1);
            vfs.track_background_task(tokio::spawn(async move {
                let prefetch_sem = Arc::new(Semaphore::new(prefetch_limit));
                let mut inflight = JoinSet::new();
                loop {
                    tokio::select! {
                        _ = vfs_for_prefetch.state.shutdown_notify.notified() => {
                            inflight.abort_all();
                            while inflight.join_next().await.is_some() {}
                            break;
                        }
                        Some((ino, block_idx)) = prefetch_rx.recv() => {
                            if vfs_for_prefetch.shutdown_requested() {
                                continue;
                            }
                            let permit = match prefetch_sem.clone().acquire_owned().await {
                                Ok(permit) => permit,
                                Err(_) => break,
                            };
                            let v = vfs_for_prefetch.clone();
                            inflight.spawn(async move {
                                let _permit = permit;
                                let _ = v.read_block_for_prefetch(ino, block_idx).await;
                            });
                        }
                        joined = inflight.join_next(), if !inflight.is_empty() => {
                            if joined.is_none() {
                                break;
                            }
                        }
                    }
                }
            }));

            // Spawn Upload Worker
            let vfs_clone = vfs.clone();
            vfs.set_uploader_task(tokio::spawn(async move {
                vfs_clone.background_uploader(upload_rx).await;
            }));

            // Spawn upload stats logger.
            let vfs_clone = vfs.clone();
            vfs.track_background_task(tokio::spawn(async move {
                vfs_clone.background_upload_stats().await;
            }));

            // Spawn periodic ingress checkpoint worker.
            let vfs_clone = vfs.clone();
            vfs.track_background_task(tokio::spawn(async move {
                vfs_clone.background_ingress_checkpoint().await;
            }));

            // Warm up the HTTP GET connection pool asynchronously.
            // Avoid blocking here: `Handle::block_on` from another thread can
            // deadlock when `StudioVfs::new` is called under a current-thread runtime
            // (which is exactly how several integration tests execute).
            {
                let fs = fetch_scheduler.clone();
                let timeout = Duration::from_secs(3);
                vfs.track_background_task(tokio::spawn(async move {
                    let _ = fs.warmup(timeout).await;
                }));
            }

            // Spawn GC Worker
            let vfs_clone = vfs.clone();
            vfs.track_background_task(tokio::spawn(async move {
                vfs_clone.background_gc().await;
            }));

            // Spawn periodic metadata sync watcher that turns remote metadata
            // version changes into kernel inode invalidations for the mounted FS.
            let vfs_clone = vfs.clone();
            vfs.track_background_task(tokio::spawn(async move {
                vfs_clone
                    .background_metadata_invalidation(metadata_invalidation_sync_interval)
                    .await;
            }));

            // Spawn Janitor Worker (automatic orphan scan/cleanup)
            let vfs_clone = vfs.clone();
            vfs.track_background_task(tokio::spawn(async move {
                vfs_clone.background_janitor().await;
            }));

            // Spawn Background Slab Flusher
            // Drains dirty_mem → slab → upload queue asynchronously after flush() returns.
            if disable_background_slab_flush {
                drop(dirty_flush_rx);
            } else {
                let vfs_clone = vfs.clone();
                vfs.track_background_task(tokio::spawn(async move {
                    vfs_clone.background_slab_flusher(dirty_flush_rx).await;
                }));
            }

            Ok(vfs)
        }
    }

    fn track_background_task(&self, handle: JoinHandle<()>) {
        self.state.background_tasks.lock().push(handle);
    }

    fn set_uploader_task(&self, handle: JoinHandle<()>) {
        *self.state.uploader_task.lock() = Some(handle);
    }

    fn kernel_invalidator(&self) -> Option<Arc<dyn KernelInvalidator>> {
        self.kernel_invalidator.read().unwrap().clone()
    }

    fn collect_metadata_inodes(engine: &dyn MetaEngine) -> MetaResult<HashSet<Ino>> {
        let mut seen = HashSet::from([ROOT_INO]);
        let mut stack = vec![ROOT_INO];

        while let Some(ino) = stack.pop() {
            let attr = match engine.get_attr(ino) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            if attr.kind != FileType::Directory {
                continue;
            }
            let entries = match engine.readdir(ino) {
                Ok(entries) => entries,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            for entry in entries {
                if seen.insert(entry.ino) {
                    stack.push(entry.ino);
                }
            }
        }

        Ok(seen)
    }

    fn invalidate_kernel_inodes(&self, inodes: impl IntoIterator<Item = Ino>) {
        let Some(invalidator) = self.kernel_invalidator() else {
            return;
        };
        let mut unique = HashSet::new();
        for ino in inodes {
            if unique.insert(ino) {
                invalidator.inval_inode(ino);
            }
        }
    }

    async fn background_metadata_invalidation(self: Arc<Self>, interval: Duration) {
        let mut last_token = self.meta.metadata_version_token();
        let mut last_snapshot: HashSet<Ino> = HashSet::new();

        if last_token.is_some() {
            let meta = self.meta.clone();
            if let Ok(snapshot) = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                Self::collect_metadata_inodes(engine.as_ref())
            })
            .await
            {
                last_snapshot = snapshot;
            }
        }

        loop {
            tokio::select! {
                _ = self.state.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if self.kernel_invalidator().is_none() {
                continue;
            }

            let meta = self.meta.clone();
            let sync = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                let before = engine.metadata_version_token();
                engine.sync_metadata()?;
                let after = engine.metadata_version_token();
                Ok((before, after))
            })
            .await;
            let Ok((_before, after_token)) = sync else {
                continue;
            };

            let changed = matches!(
                (&last_token, &after_token),
                (Some(previous), Some(current)) if previous != current
            );
            last_token = after_token.clone();

            if !changed {
                continue;
            }

            let meta = self.meta.clone();
            let Ok(current_snapshot) = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                Self::collect_metadata_inodes(engine.as_ref())
            })
            .await
            else {
                continue;
            };

            let mut to_invalidate = last_snapshot.clone();
            to_invalidate.extend(current_snapshot.iter().copied());
            self.invalidate_kernel_inodes(to_invalidate);
            last_snapshot = current_snapshot;
        }
    }

    fn shutdown_requested(&self) -> bool {
        self.state.shutdown_requested.load(Ordering::Acquire)
    }

    fn uploader_shutdown_requested(&self) -> bool {
        self.state
            .uploader_shutdown_requested
            .load(Ordering::Acquire)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_requested()
            || self.uploader_shutdown_requested()
            || self.state.shutdown_completed.load(Ordering::Acquire)
    }

    fn request_uploader_shutdown(&self) {
        if !self
            .state
            .uploader_shutdown_requested
            .swap(true, Ordering::AcqRel)
        {
            self.state.uploader_shutdown_notify.notify_waiters();
        }
    }

    async fn join_background_tasks(&self, timeout: Duration) {
        let handles = {
            let mut handles = self.state.background_tasks.lock();
            std::mem::take(&mut *handles)
        };
        for mut handle in handles {
            if !handle.is_finished() {
                match tokio::time::timeout(timeout, &mut handle).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) if e.is_cancelled() => {}
                    Ok(Err(e)) => {
                        warn!(error = %e, "background task join failed");
                    }
                    Err(_) => {
                        handle.abort();
                        let _ = handle.await;
                        warn!(
                            timeout_ms = timeout.as_millis() as u64,
                            "background task did not stop in time, aborted"
                        );
                    }
                }
            } else if let Err(e) = handle.await
                && !e.is_cancelled()
            {
                warn!(error = %e, "background task join failed");
            }
        }
    }

    async fn join_uploader_task(&self, timeout: Duration) {
        let handle = self.state.uploader_task.lock().take();
        let Some(mut handle) = handle else {
            return;
        };
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.is_cancelled() => {}
            Ok(Err(e)) => {
                warn!(error = %e, "uploader task join failed");
            }
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "uploader task did not stop in time, aborted"
                );
            }
        }
    }

    fn now_unix_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn now_unix_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn pending_age_stats_ms(&self) -> (u64, u64) {
        let now_ms = Self::now_unix_millis();
        let mut ages: Vec<u64> = self
            .block_cache
            .snapshot_pending_uploads()
            .into_iter()
            .map(|op| now_ms.saturating_sub(op.created_unix_ms))
            .collect();
        if ages.is_empty() {
            return (0, 0);
        }
        ages.sort_unstable();
        let p95_idx = ((ages.len() - 1) * 95) / 100;
        let p95 = ages[p95_idx];
        let max = *ages.last().unwrap_or(&0);
        (p95, max)
    }

    fn spill_age_stats_ms(&self) -> (usize, u64) {
        let ingress_live = self.state.ingress_live.lock();
        let count = ingress_live.len();
        let spill: HashMap<(Ino, u32), SpillEntry> =
            ingress_live.iter().map(|(k, v)| (*k, v.clone())).collect();
        let oldest_age_ms = oldest_spill_age_ms(Self::now_unix_millis(), &spill);
        (count, oldest_age_ms)
    }

    fn upload_pipeline_is_idle(&self, pending_uploads: usize, ingress: &VfsIngressStats) -> bool {
        crate::upload::upload_log_is_idle(pending_uploads, self.upload_scheduler.in_flight())
            && ingress.dirty_mem_blocks == 0
            && ingress.dirty_mem_bytes == 0
            && ingress.spill_records == 0
            && ingress.spill_bytes == 0
            && ingress.checkpoint_pending_bytes == 0
            && ingress.checkpoint_pending_mutations == 0
    }

    fn ingress_checkpoint_due(&self, timer_fired: bool) -> bool {
        let pending_bytes = self
            .state
            .ingress_checkpoint_pending_bytes
            .load(Ordering::Relaxed);
        let pending_mutations = self
            .state
            .ingress_checkpoint_pending_mutations
            .load(Ordering::Relaxed);
        let active_segment_lag = self
            .state
            .active_ingress_segment_id
            .load(Ordering::Relaxed)
            .saturating_sub(
                self.state
                    .ingress_last_checkpoint_segment_id
                    .load(Ordering::Relaxed),
            );
        let live_segment_count = self.state.ingress_live.lock().live_segment_count();
        ingress_checkpoint_due_with_policy(
            pending_bytes,
            pending_mutations,
            self.ingress_checkpoint_min_append_bytes,
            self.ingress_checkpoint_min_mutations,
            active_segment_lag,
            self.ingress_checkpoint_max_segment_lag,
            live_segment_count,
            self.ingress_checkpoint_max_live_segments,
            timer_fired,
        )
    }

    fn persist_ingress_state(&self) -> VfsResult<()> {
        write_ingress_state(
            &self.ingress_spill_dir,
            self.state.active_ingress_segment_id.load(Ordering::Relaxed),
            self.state.spill_seq.load(Ordering::Relaxed),
        )
    }

    fn persist_ingress_recovery_state_owned(
        state: Arc<VfsState>,
        ingress_checkpoint_dir: PathBuf,
        ingress_segment_dir: PathBuf,
        ingress_spill_dir: PathBuf,
    ) -> VfsResult<()> {
        let _writer_guard = state.ingress_segment_write_lock.lock();
        let ingress_live = state.ingress_live.lock();
        let meta = write_ingress_checkpoint(
            &ingress_checkpoint_dir,
            &ingress_live,
            &ingress_segment_dir,
            state.active_ingress_segment_id.load(Ordering::Relaxed),
            state.spill_seq.load(Ordering::Relaxed),
        )?;
        state
            .ingress_checkpoint_persists
            .fetch_add(1, Ordering::Relaxed);
        state
            .ingress_last_checkpoint_unix_ms
            .store(Self::now_unix_millis(), Ordering::Relaxed);
        state
            .ingress_last_checkpoint_segment_id
            .store(meta.covered_segment_id, Ordering::Relaxed);
        state
            .ingress_last_checkpoint_offset
            .store(meta.covered_offset, Ordering::Relaxed);
        state
            .ingress_checkpoint_pending_bytes
            .store(0, Ordering::Relaxed);
        state
            .ingress_checkpoint_pending_mutations
            .store(0, Ordering::Relaxed);
        write_ingress_state(
            &ingress_spill_dir,
            state.active_ingress_segment_id.load(Ordering::Relaxed),
            state.spill_seq.load(Ordering::Relaxed),
        )
    }

    fn note_ingress_checkpoint_churn(&self, record_bytes: u64) {
        let pending_bytes = self
            .state
            .ingress_checkpoint_pending_bytes
            .fetch_add(record_bytes, Ordering::Relaxed)
            .saturating_add(record_bytes);
        let pending_mutations = self
            .state
            .ingress_checkpoint_pending_mutations
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if pending_bytes >= self.ingress_checkpoint_min_append_bytes
            || pending_mutations >= self.ingress_checkpoint_min_mutations
        {
            self.state.ingress_checkpoint_notify.notify_one();
        }
    }

    fn append_ingress_record_live(
        &self,
        key: (Ino, u32),
        seq: u64,
        created_unix_ms: u64,
        kind: u8,
        payload: &[u8],
    ) -> VfsResult<(u64, PathBuf)> {
        let _guard = self.state.ingress_segment_write_lock.lock();
        let record_len = INGRESS_RECORD_HEADER_LEN as u64 + payload.len() as u64;
        let mut active_segment_id = self.state.active_ingress_segment_id.load(Ordering::Relaxed);
        let active_path = active_ingress_segment_path(&self.ingress_segment_dir, active_segment_id);
        let active_len = std::fs::metadata(&active_path)
            .map(|meta| meta.len())
            .unwrap_or(0);
        if active_len > 0 && active_len.saturating_add(record_len) > self.max_ingress_segment_bytes
        {
            active_segment_id = active_segment_id.saturating_add(1);
            self.state
                .active_ingress_segment_id
                .store(active_segment_id, Ordering::Relaxed);
        }
        let offset = append_ingress_record(
            &self.ingress_segment_dir,
            active_segment_id,
            key,
            seq,
            created_unix_ms,
            kind,
            payload,
        )?;
        self.note_ingress_checkpoint_churn(record_len);
        if let Err(e) = self.persist_ingress_state() {
            warn!("failed to persist ingress state after append: {}", e);
        }
        Ok((
            offset,
            ingress_segment_path(&self.ingress_segment_dir, active_segment_id),
        ))
    }

    fn upload_requeue_delay(&self, op: &PendingUpload) -> Duration {
        let exp = op.retry_count.min(8);
        let base = self
            .upload_requeue_base
            .saturating_mul(2u32.saturating_pow(exp));
        let capped = base.min(self.upload_requeue_max);
        let jitter_max = (capped.as_millis() as u64 / 5).max(1);
        let jitter_seed = op.object_id ^ Self::now_unix_millis();
        let jitter_ms = jitter_seed % jitter_max;
        capped.saturating_add(Duration::from_millis(jitter_ms))
    }

    fn remove_spill_entry(&self, key: (Ino, u32)) -> Option<SpillEntry> {
        let removed = self.state.ingress_live.lock().remove(&key);
        if let Some(ref entry) = removed {
            let seq = self.state.spill_seq.fetch_add(1, Ordering::Relaxed);
            let _ = self.append_ingress_record_live(
                key,
                seq,
                Self::now_unix_millis(),
                INGRESS_RECORD_INVALIDATE,
                &[],
            );
            self.state
                .dirty_spill_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(entry.len))
                })
                .ok();
            if entry.path.extension().is_some_and(|ext| ext == "spill") {
                let _ = std::fs::remove_file(&entry.path);
            }
            self.state.dirty_drained_notify.notify_waiters();
        }
        removed
    }

    fn load_spill_block(&self, key: (Ino, u32)) -> VfsResult<Option<Bytes>> {
        let entry = self.state.ingress_live.lock().get_cloned(&key);
        let Some(entry) = entry else {
            return Ok(None);
        };
        Ok(Some(read_ingress_entry_bytes(&entry)?))
    }

    fn spill_block(&self, key: (Ino, u32), data: Bytes) -> VfsResult<()> {
        let seq = self.state.spill_seq.fetch_add(1, Ordering::Relaxed);
        let created_unix_ms = Self::now_unix_millis();
        let (segment_offset, segment_path) =
            self.append_ingress_record_live(key, seq, created_unix_ms, INGRESS_RECORD_PUT, &data)?;

        if let Some(old) = self.state.ingress_live.lock().insert(
            key,
            SpillEntry {
                seq,
                path: segment_path,
                offset: segment_offset,
                len: data.len(),
                created_unix_ms,
            },
        ) {
            self.state
                .dirty_spill_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(old.len))
                })
                .ok();
            if old.path.extension().is_some_and(|ext| ext == "spill") {
                let _ = std::fs::remove_file(old.path);
            }
        }
        self.state
            .dirty_spill_bytes
            .fetch_add(data.len(), Ordering::Relaxed);
        Ok(())
    }

    async fn current_content_epoch_async(&self, ino: Ino) -> VfsResult<u64> {
        if let Some(attr) = self.state.dirty_attrs.lock().get(&ino) {
            return Ok(attr.content_epoch);
        }
        let meta = self.meta.clone();
        run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            Ok(engine.get_attr(ino)?.content_epoch)
        })
        .await
    }

    fn pending_content_epoch_for_inode(&self, ino: Ino) -> Option<u64> {
        self.state
            .inode_incarnations
            .lock()
            .get(&ino)
            .and_then(|state| {
                state
                    .pending_incarnation
                    .as_ref()
                    .map(|pending| pending.content_epoch)
            })
    }

    fn pending_live_extent_record(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
    ) -> Option<ExtentRecordId> {
        let pending_content_epoch = pending_content_epoch?;
        self.state
            .live_extent_records
            .lock()
            .get(&(ino, block_index))
            .copied()
            .filter(|extent| extent.content_epoch == pending_content_epoch)
    }

    fn read_pending_overlay_block_sync(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
    ) -> VfsResult<Option<Bytes>> {
        if let Some(pending_block) = self.state.pending_blocks.lock().get(&(ino, block_index)) {
            return Ok(Some(pending_block.data.clone().freeze()));
        }
        if let Some(data) = self.state.dirty_mem.lock().get(&(ino, block_index)) {
            return Ok(Some(data.clone()));
        }
        if let Some(extent) =
            self.pending_live_extent_record(ino, block_index, pending_content_epoch)
            && let Some(data) = self.block_cache.get_sync(extent.object_id)
        {
            return Ok(Some(data));
        }
        if let Some(data) = self.load_spill_block((ino, block_index))? {
            return Ok(Some(data));
        }
        Ok(None)
    }

    async fn read_pending_overlay_block_async(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
    ) -> VfsResult<Option<Bytes>> {
        if let Some(pending_block) = self.state.pending_blocks.lock().get(&(ino, block_index)) {
            return Ok(Some(pending_block.data.clone().freeze()));
        }
        if let Some(data) = self.state.dirty_mem.lock().get(&(ino, block_index)) {
            return Ok(Some(data.clone()));
        }
        if let Some(extent) =
            self.pending_live_extent_record(ino, block_index, pending_content_epoch)
            && let Some(data) = self.block_cache.get(extent.object_id).await
        {
            return Ok(Some(data));
        }
        if let Some(data) = self.load_spill_block_async((ino, block_index)).await? {
            return Ok(Some(data));
        }
        Ok(None)
    }

    fn inode_has_local_overlay(&self, ino: Ino) -> bool {
        if self
            .state
            .pending_blocks
            .lock()
            .keys()
            .any(|(candidate, _)| *candidate == ino)
        {
            return true;
        }
        if self
            .state
            .dirty_mem
            .lock()
            .keys()
            .any(|(candidate, _)| *candidate == ino)
        {
            return true;
        }
        self.state
            .ingress_live
            .lock()
            .keys()
            .any(|(candidate, _)| *candidate == ino)
    }

    fn inode_has_pending_uploads(&self, ino: Ino) -> bool {
        self.block_cache
            .snapshot_pending_uploads()
            .iter()
            .any(|op| op.ino == ino)
    }

    fn derive_committed_incarnation_from_attr(
        &self,
        attr: &InodeAttr,
    ) -> Option<CommittedIncarnation> {
        if attr.kind != FileType::RegularFile {
            return None;
        }
        Some(CommittedIncarnation {
            content_epoch: attr.committed_content_epoch,
            size: attr.committed_size,
        })
    }

    fn update_pending_incarnation_size(&self, ino: Ino, size: u64) {
        if let Some(state) = self.state.inode_incarnations.lock().get_mut(&ino) {
            let base_size = state
                .committed_incarnation
                .as_ref()
                .map(|committed| committed.size)
                .unwrap_or(0);
            if let Some(pending) = state.pending_incarnation.as_mut() {
                pending.size = size;
                pending.base_size = base_size;
            }
        }
    }

    fn begin_pending_incarnation(&self, ino: Ino, attr: &mut InodeAttr) {
        if attr.kind != FileType::RegularFile {
            return;
        }

        let mut states = self.state.inode_incarnations.lock();
        let state = states.entry(ino).or_insert_with(|| InodeIncarnationState {
            committed_incarnation: self.derive_committed_incarnation_from_attr(attr),
            pending_incarnation: None,
            upload_state: VfsUploadState::Idle,
        });

        if state.pending_incarnation.is_none() {
            let committed = state.committed_incarnation.clone().or_else(|| {
                if attr.committed_size == 0 {
                    None
                } else {
                    Some(CommittedIncarnation {
                        content_epoch: attr.content_epoch,
                        size: attr.committed_size,
                    })
                }
            });
            let next_epoch = attr.content_epoch.saturating_add(1);
            state.committed_incarnation = committed.clone();
            state.pending_incarnation = Some(PendingIncarnation {
                content_epoch: next_epoch,
                size: attr.size,
                base_size: committed.as_ref().map(|base| base.size).unwrap_or(0),
            });
            state.upload_state = if committed.is_some() {
                VfsUploadState::Updating
            } else {
                VfsUploadState::Uploading
            };
            attr.content_epoch = next_epoch;
        } else {
            attr.content_epoch = state
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch)
                .unwrap_or(attr.content_epoch);
        }

        let base_size = state
            .committed_incarnation
            .as_ref()
            .map(|committed| committed.size)
            .unwrap_or(0);
        if let Some(pending) = state.pending_incarnation.as_mut() {
            pending.size = attr.size;
            pending.base_size = base_size;
        }
    }

    fn refresh_inode_incarnation_state_from_attr(
        &self,
        ino: Ino,
        attr: &InodeAttr,
    ) -> InodeIncarnationState {
        let has_local_overlay = self.inode_has_local_overlay(ino);
        let has_pending_uploads = self.inode_has_pending_uploads(ino);

        let mut states = self.state.inode_incarnations.lock();
        let previous = states
            .get(&ino)
            .cloned()
            .unwrap_or_else(|| InodeIncarnationState {
                committed_incarnation: self.derive_committed_incarnation_from_attr(attr),
                pending_incarnation: None,
                upload_state: VfsUploadState::Idle,
            });

        let needs_pending = previous.pending_incarnation.is_some()
            || has_local_overlay
            || has_pending_uploads
            || attr.size != attr.committed_size;

        let next_state = if needs_pending {
            let committed = if previous.pending_incarnation.is_some() {
                previous.committed_incarnation.clone()
            } else if previous.committed_incarnation.is_some()
                && previous.upload_state != VfsUploadState::Idle
            {
                previous.committed_incarnation.clone()
            } else if attr.committed_size == 0
                && previous.committed_incarnation.is_none()
                && previous.upload_state == VfsUploadState::Uploading
            {
                None
            } else {
                self.derive_committed_incarnation_from_attr(attr)
                    .map(|mut committed| {
                        if attr.size != attr.committed_size
                            && committed.content_epoch == attr.content_epoch
                        {
                            committed.content_epoch = committed.content_epoch.saturating_sub(1);
                        }
                        committed
                    })
            };

            let pending_epoch = previous
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch)
                .unwrap_or(attr.content_epoch);

            InodeIncarnationState {
                committed_incarnation: committed.clone(),
                pending_incarnation: Some(PendingIncarnation {
                    content_epoch: pending_epoch,
                    size: attr.size,
                    base_size: committed.as_ref().map(|base| base.size).unwrap_or(0),
                }),
                upload_state: if committed.is_some() {
                    VfsUploadState::Updating
                } else {
                    VfsUploadState::Uploading
                },
            }
        } else {
            InodeIncarnationState {
                committed_incarnation: self.derive_committed_incarnation_from_attr(attr),
                pending_incarnation: None,
                upload_state: VfsUploadState::Idle,
            }
        };

        states.insert(ino, next_state.clone());
        next_state
    }

    fn clear_inode_incarnation(&self, ino: Ino) {
        self.state.inode_incarnations.lock().remove(&ino);
    }

    pub async fn inode_incarnation_state(&self, ino: Ino) -> VfsResult<InodeIncarnationState> {
        let attr = self.getattr_async(ino).await?;
        Ok(self.refresh_inode_incarnation_state_from_attr(ino, &attr))
    }

    fn record_live_extent(&self, ino: Ino, block_index: u32, object_id: u64, content_epoch: u64) {
        self.state.live_extent_records.lock().insert(
            (ino, block_index),
            ExtentRecordId {
                object_id,
                content_epoch,
            },
        );
        self.state
            .uploaded_extent_records
            .lock()
            .remove(&(ino, block_index));
    }

    fn remove_live_extent(&self, ino: Ino, block_index: u32) {
        self.state
            .live_extent_records
            .lock()
            .remove(&(ino, block_index));
        self.state
            .uploaded_extent_records
            .lock()
            .remove(&(ino, block_index));
    }

    fn remove_live_extents_for_inode(&self, ino: Ino) {
        self.state
            .live_extent_records
            .lock()
            .retain(|(candidate, _), _| *candidate != ino);
        self.state
            .uploaded_extent_records
            .lock()
            .retain(|(candidate, _), _| *candidate != ino);
    }

    fn remove_live_extents_for_inode_epoch(&self, ino: Ino, content_epoch: u64) {
        self.state
            .live_extent_records
            .lock()
            .retain(|(candidate, _), extent| {
                *candidate != ino || extent.content_epoch != content_epoch
            });
        self.state
            .uploaded_extent_records
            .lock()
            .retain(|(candidate, _), extent| {
                *candidate != ino || extent.content_epoch != content_epoch
            });
    }

    fn record_uploaded_extent(
        &self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
        content_epoch: u64,
    ) {
        self.state.uploaded_extent_records.lock().insert(
            (ino, block_index),
            ExtentRecordId {
                object_id,
                content_epoch,
            },
        );
    }

    fn uploaded_extent_records_for_inode_epoch(
        &self,
        ino: Ino,
        content_epoch: u64,
    ) -> Vec<(u32, ExtentRecordId)> {
        self.state
            .uploaded_extent_records
            .lock()
            .iter()
            .filter_map(|((candidate_ino, block_index), extent)| {
                (*candidate_ino == ino && extent.content_epoch == content_epoch)
                    .then_some((*block_index, *extent))
            })
            .collect()
    }

    fn remote_upload_placeholder_visible(&self, ino: Ino, attr: &InodeAttr) -> bool {
        attr.kind == FileType::RegularFile
            && attr.upload_state == UploadState::Uploading
            && attr.committed_size == 0
            && !self.inode_has_local_overlay(ino)
            && !self.inode_has_pending_uploads(ino)
    }

    pub fn live_extent_records_for_inode(&self, ino: Ino) -> Vec<(u32, u64, u64)> {
        let mut out: Vec<_> = self
            .state
            .live_extent_records
            .lock()
            .iter()
            .filter(|((candidate, _), _)| *candidate == ino)
            .map(|((_, block_index), extent)| {
                (*block_index, extent.object_id, extent.content_epoch)
            })
            .collect();
        out.sort_by_key(|(block_index, _, _)| *block_index);
        out
    }

    pub fn pending_uploads_for_inode(&self, ino: Ino) -> Vec<(u64, u64, u64)> {
        let mut out: Vec<_> = self
            .block_cache
            .snapshot_pending_uploads()
            .into_iter()
            .filter(|op| op.ino == ino)
            .map(|op| (op.offset, op.object_id, op.content_epoch))
            .collect();
        out.sort_by_key(|(offset, _, _)| *offset);
        out
    }

    pub fn upload_stats(&self) -> VfsUploadStats {
        VfsUploadStats {
            pending_uploads: self.block_cache.pending_upload_count(),
            upload_requeues: self.state.upload_requeues.load(Ordering::Relaxed),
            canceled_deleted: self.state.upload_canceled_deleted.load(Ordering::Relaxed),
            canceled_stale: self.state.upload_canceled_stale.load(Ordering::Relaxed),
        }
    }

    fn logical_block_len(&self, file_size: u64, block_index: u32) -> usize {
        let block_start = block_index as u64 * self.block_size as u64;
        if block_start >= file_size {
            return 0;
        }
        file_size
            .saturating_sub(block_start)
            .min(self.block_size as u64) as usize
    }

    fn clear_pending_block(&self, ino: Ino, block_index: u32) {
        let _ = self.state.pending_blocks.lock().remove(&(ino, block_index));
    }

    fn trim_local_block_staging(
        &self,
        ino: Ino,
        block_index: u32,
        new_len: usize,
    ) -> VfsResult<bool> {
        let mut changed = false;

        {
            let mut pending = self.state.pending_blocks.lock();
            if let Some(block) = pending.get_mut(&(ino, block_index))
                && block.data.len() > new_len
            {
                block.data.truncate(new_len);
                block.dirty = true;
                changed = true;
            }
        }

        {
            let mut dirty = self.state.dirty_mem.lock();
            if let Some(current) = dirty.get(&(ino, block_index)).cloned()
                && current.len() > new_len
            {
                self.state
                    .dirty_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(current.len()))
                    })
                    .ok();
                if new_len == 0 {
                    let _ = dirty.remove(&(ino, block_index));
                } else {
                    dirty.insert((ino, block_index), current.slice(0..new_len));
                    self.state.dirty_bytes.fetch_add(new_len, Ordering::Relaxed);
                }
                self.state.dirty_drained_notify.notify_waiters();
                changed = true;
            }
        }

        if let Some(current) = self.load_spill_block((ino, block_index))?
            && current.len() > new_len
        {
            let _ = self.remove_spill_entry((ino, block_index));
            if new_len > 0 {
                self.spill_block((ino, block_index), current.slice(0..new_len))?;
            }
            changed = true;
        }

        Ok(changed)
    }

    fn load_local_block_bytes_sync(
        &self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
    ) -> VfsResult<Option<Bytes>> {
        let pending_content_epoch = self.pending_content_epoch_for_inode(ino);
        if let Some(data) =
            self.read_pending_overlay_block_sync(ino, block_index, pending_content_epoch)?
        {
            return Ok(Some(data));
        }
        Ok(self.block_cache.get_sync(object_id))
    }

    fn install_truncated_pending_block(
        &self,
        ino: Ino,
        block_index: u32,
        data: Bytes,
        new_len: usize,
    ) {
        self.clear_pending_block(ino, block_index);
        let mut pending = self.state.pending_blocks.lock();
        pending.insert(
            (ino, block_index),
            PendingBlock {
                data: BytesMut::from(&data[..new_len]),
                dirty: true,
            },
        );
    }

    async fn enqueue_dirty_blocks(&self, ino: Ino, blocks: Vec<(u32, Bytes)>) -> VfsResult<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let _accept_qos = self.qos.try_acquire_accept();
        let total: usize = blocks.iter().map(|(_, d)| d.len()).sum();
        let block_count = blocks.len() as u64;

        loop {
            // Register the waiter before capacity checks so a concurrent drain
            // cannot free space and notify in the race window between "no room"
            // and `notified().await`.
            let notified = self.state.dirty_drained_notify.notified();
            let dirty_bytes = self.state.dirty_bytes.load(Ordering::Relaxed);
            if dirty_bytes.saturating_add(total) <= self.max_dirty_bytes {
                for (block_index, data) in &blocks {
                    self.spill_block_async((ino, *block_index), data.clone())
                        .await?;
                }
                {
                    let mut dirty = self.state.dirty_mem.lock();
                    for (block_index, data) in &blocks {
                        if let Some(old) = dirty.insert((ino, *block_index), data.clone()) {
                            self.state
                                .dirty_bytes
                                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                                    Some(v.saturating_sub(old.len()))
                                })
                                .ok();
                        }
                    }
                }
                self.state.dirty_bytes.fetch_add(total, Ordering::Relaxed);
                let _ = self.dirty_flush_tx.send(());
                return Ok(());
            }

            let spill_bytes = self.state.dirty_spill_bytes.load(Ordering::Relaxed);
            if spill_bytes.saturating_add(total) <= self.max_spill_bytes {
                for (block_index, data) in blocks {
                    {
                        let mut dirty = self.state.dirty_mem.lock();
                        if let Some(old) = dirty.remove(&(ino, block_index)) {
                            self.state
                                .dirty_bytes
                                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                                    Some(v.saturating_sub(old.len()))
                                })
                                .ok();
                        }
                    }
                    self.spill_block_async((ino, block_index), data).await?;
                }
                self.state
                    .forced_spill_blocks
                    .fetch_add(block_count, Ordering::Relaxed);
                self.state.dirty_drained_notify.notify_waiters();
                let _ = self.dirty_flush_tx.send(());
                return Ok(());
            }

            let wait_started = Instant::now();
            notified.await;
            self.state
                .write_backpressure_wait_ms
                .fetch_add(wait_started.elapsed().as_millis() as u64, Ordering::Relaxed);
        }
    }

    async fn spill_block_async(&self, key: (Ino, u32), data: Bytes) -> VfsResult<()> {
        let state = self.state.clone();
        let ingress_segment_dir = self.ingress_segment_dir.clone();
        let spill_state_dir = self.ingress_spill_dir.clone();
        let max_ingress_segment_bytes = self.max_ingress_segment_bytes;
        let data = data.as_ref().to_vec();
        run_blocking(move || {
            let (seq, created_unix_ms, segment_id, offset) = {
                let _guard = state.ingress_segment_write_lock.lock();
                let seq = state.spill_seq.fetch_add(1, Ordering::Relaxed);
                let created_unix_ms = StudioVfs::now_unix_millis();

                let mut segment_id = state.active_ingress_segment_id.load(Ordering::Relaxed);
                let active_path = active_ingress_segment_path(&ingress_segment_dir, segment_id);
                let active_len = std::fs::metadata(&active_path)
                    .map(|meta| meta.len())
                    .unwrap_or(0);
                if active_len > 0
                    && active_len
                        .saturating_add(INGRESS_RECORD_HEADER_LEN as u64 + data.len() as u64)
                        > max_ingress_segment_bytes
                {
                    segment_id = segment_id.saturating_add(1);
                    state
                        .active_ingress_segment_id
                        .store(segment_id, Ordering::Relaxed);
                }

                let offset = append_ingress_record(
                    &ingress_segment_dir,
                    segment_id,
                    key,
                    seq,
                    created_unix_ms,
                    INGRESS_RECORD_PUT,
                    &data,
                )?;
                let _ = write_ingress_state(
                    &spill_state_dir,
                    segment_id,
                    state.spill_seq.load(Ordering::Relaxed),
                );
                (seq, created_unix_ms, segment_id, offset)
            };

            if let Some(old) = state.ingress_live.lock().insert(
                key,
                SpillEntry {
                    seq,
                    path: ingress_segment_path(&ingress_segment_dir, segment_id),
                    offset,
                    len: data.len(),
                    created_unix_ms,
                },
            ) {
                state
                    .dirty_spill_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(old.len))
                    })
                    .ok();
                if old.path.extension().is_some_and(|ext| ext == "spill") {
                    let _ = std::fs::remove_file(old.path);
                }
            }
            state
                .dirty_spill_bytes
                .fetch_add(data.len(), Ordering::Relaxed);
            Ok(())
        })
        .await
    }

    #[inline]
    fn mark_foreground_read(&self) {
        self.state
            .foreground_reads_inflight
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .last_foreground_read_ms
            .store(Self::now_unix_millis(), Ordering::Relaxed);
    }

    #[inline]
    fn unmark_foreground_read(&self) {
        self.state
            .foreground_reads_inflight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
        self.state
            .last_foreground_read_ms
            .store(Self::now_unix_millis(), Ordering::Relaxed);
    }

    #[inline]
    fn foreground_read_active(&self) -> bool {
        if self.state.foreground_reads_inflight.load(Ordering::Relaxed) > 0 {
            return true;
        }
        let last = self.state.last_foreground_read_ms.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        Self::now_unix_millis().saturating_sub(last)
            <= self.upload_user_idle_grace.as_millis() as u64
    }

    fn encode_lease_record(owner_id: &str, lease_until_unix: i64, fencing_token: u64) -> Vec<u8> {
        format!("{owner_id}\n{lease_until_unix}\n{fencing_token}\n").into_bytes()
    }

    fn decode_lease_record(bytes: &[u8]) -> Option<(String, i64, u64)> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        let owner = lines.next()?.to_string();
        let lease_until = lines.next()?.parse::<i64>().ok()?;
        let fencing = lines.next()?.parse::<u64>().ok()?;
        Some((owner, lease_until, fencing))
    }

    async fn acquire_janitor_lease(&self, lease: &JanitorLease) -> bool {
        let now = Self::now_unix_secs();
        let lease_until = now.saturating_add(self.janitor_lease_ttl.as_secs() as i64);

        let create_payload = Self::encode_lease_record(&lease.owner_id, lease_until, 1);
        let mut create_opts = WriteOptions::default();
        create_opts.if_not_exists = true;
        match lease
            .op
            .write_options(&lease.lock_path, create_payload, create_opts)
            .await
        {
            Ok(_) => {
                debug!(
                    lock_path = lease.lock_path,
                    owner_id = lease.owner_id,
                    "janitor lease acquired via create"
                );
                return true;
            }
            Err(err) if err.kind() == ErrorKind::ConditionNotMatch => {}
            Err(err) => {
                warn!("janitor lease create failed: {}", err);
                return false;
            }
        }

        let stat = match lease.op.stat(&lease.lock_path).await {
            Ok(v) => v,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // Lock disappeared between create attempt and stat; next cycle will retry.
                return false;
            }
            Err(err) => {
                warn!("janitor lease stat failed: {}", err);
                return false;
            }
        };
        let etag = stat.etag().map(ToString::to_string);
        let current_raw = match lease.op.read(&lease.lock_path).await {
            Ok(v) => v,
            Err(err) => {
                warn!("janitor lease read failed: {}", err);
                return false;
            }
        };
        let (owner, current_until, fencing) = match Self::decode_lease_record(&current_raw.to_vec())
        {
            Some(v) => v,
            None => ("".to_string(), 0, 0),
        };

        if current_until > now && owner != lease.owner_id {
            debug!(
                lock_path = lease.lock_path,
                lock_owner = owner,
                lock_until = current_until,
                now,
                "janitor lease is held by another client"
            );
            return false;
        }

        let mut update_opts = WriteOptions::default();
        if let Some(v) = etag {
            update_opts.if_match = Some(v);
        }
        let update_payload =
            Self::encode_lease_record(&lease.owner_id, lease_until, fencing.saturating_add(1));
        match lease
            .op
            .write_options(&lease.lock_path, update_payload, update_opts)
            .await
        {
            Ok(_) => {
                debug!(
                    lock_path = lease.lock_path,
                    owner_id = lease.owner_id,
                    "janitor lease acquired/renewed via CAS"
                );
                true
            }
            Err(err) if err.kind() == ErrorKind::ConditionNotMatch => false,
            Err(err) => {
                warn!("janitor lease CAS update failed: {}", err);
                false
            }
        }
    }

    fn lock_record_path(cfg: &DistributedFlock, ino: Ino) -> String {
        format!("{}{}.lock", cfg.prefix, ino)
    }

    fn encode_remote_lock_record(state: &RemoteLockRecord) -> Vec<u8> {
        let exclusive = state.exclusive_owner.as_deref().unwrap_or("-");
        let shared = if state.shared_owners.is_empty() {
            "-".to_string()
        } else {
            state
                .shared_owners
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        };
        format!("{}\n{}\n{}\n", state.lease_until_unix, exclusive, shared).into_bytes()
    }

    fn decode_remote_lock_record(bytes: &[u8]) -> Option<RemoteLockRecord> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        let lease_until_unix = lines.next()?.trim().parse::<i64>().ok()?;
        let exclusive_raw = lines.next().unwrap_or("-").trim();
        let shared_raw = lines.next().unwrap_or("-").trim();
        let exclusive_owner = if exclusive_raw.is_empty() || exclusive_raw == "-" {
            None
        } else {
            Some(exclusive_raw.to_string())
        };
        let mut shared_owners = BTreeSet::new();
        if !shared_raw.is_empty() && shared_raw != "-" {
            for item in shared_raw.split(',') {
                let v = item.trim();
                if !v.is_empty() {
                    shared_owners.insert(v.to_string());
                }
            }
        }
        Some(RemoteLockRecord {
            lease_until_unix,
            exclusive_owner,
            shared_owners,
        })
    }

    fn normalize_remote_lock_state(state: &mut RemoteLockRecord, now: i64) {
        if state.lease_until_unix <= now {
            state.lease_until_unix = 0;
            state.exclusive_owner = None;
            state.shared_owners.clear();
        }
    }

    async fn acquire_distributed_flock(
        &self,
        cfg: &DistributedFlock,
        ino: Ino,
        op: LockKind,
    ) -> VfsResult<bool> {
        let path = Self::lock_record_path(cfg, ino);
        let now = Self::now_unix_secs();
        let lease_until = now.saturating_add(cfg.lease_ttl.as_secs() as i64);

        let mut retries = 0u32;
        loop {
            let mut retry_later = false;
            let (mut state, etag_opt) = match cfg.op.stat(&path).await {
                Ok(meta) => {
                    let etag = meta.etag().map(ToString::to_string);
                    match cfg.op.read(&path).await {
                        Ok(raw) => {
                            let parsed =
                                Self::decode_remote_lock_record(&raw.to_vec()).unwrap_or_default();
                            (parsed, etag)
                        }
                        Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                            retry_later = true;
                            (RemoteLockRecord::default(), None)
                        }
                        Err(err) => {
                            return Err(VfsError::Other(format!("lock read failed: {err}")));
                        }
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    (RemoteLockRecord::default(), None)
                }
                Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                    retry_later = true;
                    (RemoteLockRecord::default(), None)
                }
                Err(err) => return Err(VfsError::Other(format!("lock stat failed: {err}"))),
            };
            if retry_later {
                retries = retries.saturating_add(1);
                if retries >= 12 {
                    return Ok(false);
                }
                let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                    .min(cfg.retry_max_ms);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }

            Self::normalize_remote_lock_state(&mut state, now);
            let mut next_state = state.clone();
            let client = cfg.client_id.clone();

            let allowed = match op {
                LockKind::Shared => {
                    if let Some(excl) = &state.exclusive_owner {
                        excl == &client
                    } else {
                        true
                    }
                }
                LockKind::Exclusive => {
                    let excl_ok = state
                        .exclusive_owner
                        .as_ref()
                        .map(|v| v == &client)
                        .unwrap_or(true);
                    let shared_ok = state.shared_owners.is_empty()
                        || (state.shared_owners.len() == 1
                            && state.shared_owners.contains(&client));
                    excl_ok && shared_ok
                }
                LockKind::Unlock => true,
            };

            if !allowed {
                return Ok(false);
            }

            match op {
                LockKind::Shared => {
                    next_state.exclusive_owner = None;
                    next_state.shared_owners.insert(client);
                }
                LockKind::Exclusive => {
                    next_state.exclusive_owner = Some(client);
                    next_state.shared_owners.clear();
                }
                LockKind::Unlock => {
                    if next_state.exclusive_owner.as_deref() == Some(cfg.client_id.as_str()) {
                        next_state.exclusive_owner = None;
                    }
                    next_state.shared_owners.remove(cfg.client_id.as_str());
                }
            }
            next_state.lease_until_unix = lease_until;

            let mut opts = WriteOptions::default();
            if let Some(etag) = etag_opt {
                opts.if_match = Some(etag);
            } else {
                opts.if_not_exists = true;
            }

            let payload = Self::encode_remote_lock_record(&next_state);
            match cfg.op.write_options(&path, payload, opts).await {
                Ok(_) => return Ok(true),
                Err(err) if err.kind() == ErrorKind::ConditionNotMatch => {
                    retries = retries.saturating_add(1);
                    if retries >= 12 {
                        return Ok(false);
                    }
                    let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                        .min(cfg.retry_max_ms);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                    retries = retries.saturating_add(1);
                    if retries >= 12 {
                        return Ok(false);
                    }
                    let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                        .min(cfg.retry_max_ms);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(err) => return Err(VfsError::Other(format!("lock write failed: {err}"))),
            }
        }
    }

    /// Register a kernel cache invalidator (called by the platform bridge
    /// after mount so metadata refreshes can invalidate kernel dentry/attribute cache).
    pub fn set_kernel_invalidator(&self, inv: Arc<dyn KernelInvalidator>) {
        *self.kernel_invalidator.write().unwrap() = Some(inv);
    }

    /// Get metadata engine reference.
    pub fn meta(&self) -> &dyn MetaEngine {
        self.meta.as_ref()
    }

    /// Get volume name.
    pub fn volume_name(&self) -> VfsResult<String> {
        Ok(self.meta.get_format_info()?.name)
    }

    /// Get volume UUID.
    pub fn uuid(&self) -> VfsResult<String> {
        Ok(self.meta.get_format_info()?.uuid)
    }

    /// Get block size.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Wait until all pending S3 uploads complete or `timeout` elapses.
    ///
    /// Returns `true` if the upload queue drained within the timeout.
    /// Useful in tests to ensure block data is available on S3 before
    /// cross-client reads.
    pub async fn wait_for_uploads_idle(&self, timeout: Duration) -> bool {
        self.block_cache.wait_for_uploads_idle(timeout).await
    }

    /// Get filesystem statistics.
    ///
    /// Returns real inode count and used space from redb counters.
    /// Total capacity is virtual (1 PB) since the backing store is cloud.
    pub fn statfs(&self) -> VfsResult<StatFs> {
        use studiofs_meta::types::counters;
        let used_inodes = self.meta.get_counter(counters::TOTAL_INODES)?;
        let used_bytes = self.meta.get_counter(counters::USED_SPACE)?;
        let bsize = self.block_size as u64;

        // Virtual capacity: 1 PB
        let total_bytes: u64 = 1024 * 1024 * 1024 * 1024 * 1024; // 1 PB
        let total_blocks = total_bytes / bsize;
        let used_blocks = used_bytes / bsize;
        let free_blocks = total_blocks.saturating_sub(used_blocks);

        Ok(StatFs {
            blocks: total_blocks,
            bfree: free_blocks,
            bavail: free_blocks,
            files: used_inodes,
            ffree: u64::MAX - used_inodes,
            bsize: self.block_size,
            namelen: 255,
            frsize: self.block_size,
        })
    }

    /// Snapshot accepted local-ingress state for diagnostics and backpressure
    /// observability.
    pub fn ingress_stats(&self) -> VfsIngressStats {
        let dirty_mem = self.state.dirty_mem.lock();
        let dirty_mem_blocks = dirty_mem.len();
        let dirty_mem_bytes = dirty_mem.values().map(|data| data.len()).sum();
        drop(dirty_mem);

        let (spill_records, oldest_spill_age_ms) = self.spill_age_stats_ms();
        let checkpoint_live_segments = self.state.ingress_live.lock().live_segment_count();
        let last_checkpoint_unix_ms = self
            .state
            .ingress_last_checkpoint_unix_ms
            .load(Ordering::Relaxed);
        let last_checkpoint_age_ms = if last_checkpoint_unix_ms == 0 {
            0
        } else {
            Self::now_unix_millis().saturating_sub(last_checkpoint_unix_ms)
        };

        VfsIngressStats {
            dirty_mem_bytes,
            dirty_mem_blocks,
            spill_bytes: self.state.dirty_spill_bytes.load(Ordering::Relaxed),
            spill_records,
            oldest_spill_age_ms,
            write_backpressure_wait_ms: self
                .state
                .write_backpressure_wait_ms
                .load(Ordering::Relaxed),
            forced_spill_blocks: self.state.forced_spill_blocks.load(Ordering::Relaxed),
            drained_mem_blocks: self.state.drained_mem_blocks.load(Ordering::Relaxed),
            drained_mem_bytes: self.state.drained_mem_bytes.load(Ordering::Relaxed),
            drained_spill_blocks: self.state.drained_spill_blocks.load(Ordering::Relaxed),
            drained_spill_bytes: self.state.drained_spill_bytes.load(Ordering::Relaxed),
            checkpoint_persists: self
                .state
                .ingress_checkpoint_persists
                .load(Ordering::Relaxed),
            checkpoint_failures: self
                .state
                .ingress_checkpoint_failures
                .load(Ordering::Relaxed),
            last_checkpoint_age_ms,
            last_checkpoint_segment_id: self
                .state
                .ingress_last_checkpoint_segment_id
                .load(Ordering::Relaxed),
            last_checkpoint_offset: self
                .state
                .ingress_last_checkpoint_offset
                .load(Ordering::Relaxed),
            checkpoint_pending_bytes: self
                .state
                .ingress_checkpoint_pending_bytes
                .load(Ordering::Relaxed),
            checkpoint_pending_mutations: self
                .state
                .ingress_checkpoint_pending_mutations
                .load(Ordering::Relaxed),
            checkpoint_active_segment_lag: self
                .state
                .active_ingress_segment_id
                .load(Ordering::Relaxed)
                .saturating_sub(
                    self.state
                        .ingress_last_checkpoint_segment_id
                        .load(Ordering::Relaxed),
                ),
            checkpoint_live_segments,
        }
    }

    /// Describe where a new locally materialized payload would be accepted
    /// right now: RAM, spill, or local backpressure.
    pub fn ingress_pressure_for(&self, next_bytes: usize) -> VfsIngressPressure {
        let dirty_mem_bytes = self.state.dirty_bytes.load(Ordering::Relaxed);
        let spill_bytes = self.state.dirty_spill_bytes.load(Ordering::Relaxed);
        // Prefer spill once spill accounting is already in use, even for tiny writes,
        // so we do not oscillate between spill and DirtyMem after RAM capacity was
        // exhausted once in the current process lifetime.
        let prefer_dirty_mem =
            spill_bytes == 0 && dirty_mem_bytes.saturating_add(next_bytes) <= self.max_dirty_bytes;
        let lane = if prefer_dirty_mem {
            VfsIngressLane::DirtyMem
        } else if spill_bytes.saturating_add(next_bytes) <= self.max_spill_bytes {
            VfsIngressLane::Spill
        } else {
            VfsIngressLane::Backpressure
        };

        VfsIngressPressure {
            next_bytes,
            dirty_mem_bytes,
            dirty_mem_limit: self.max_dirty_bytes,
            spill_bytes,
            spill_limit: self.max_spill_bytes,
            lane,
        }
    }

    // =========================================================================
    // Attribute Operations
    // =========================================================================

    /// Get file attributes.
    pub fn getattr(&self, ino: Ino) -> VfsResult<InodeAttr> {
        // 1. Check dirty attrs cache first (write path data takes precedence)
        if let Some(attr) = self.state.dirty_attrs.lock().get(&ino) {
            return Ok(attr.clone());
        }

        // 2. Check read-through cache with TTL
        {
            let cache = self.state.attr_read_cache.lock();
            if let Some((attr, timestamp)) = cache.get(&ino)
                && timestamp.elapsed() < ATTR_CACHE_TTL
            {
                return Ok(attr.clone());
            }
            // Drop lock before potentially slow redb read
        }

        // 3. Fetch from redb and populate read-through cache
        let attr = self.meta.get_attr(ino)?;
        self.state
            .attr_read_cache
            .lock()
            .insert(ino, (attr.clone(), Instant::now()));
        Ok(attr)
    }

    async fn getattr_async(&self, ino: Ino) -> VfsResult<InodeAttr> {
        if let Some(attr) = self.state.dirty_attrs.lock().get(&ino) {
            return Ok(attr.clone());
        }

        if let Some((attr, timestamp)) = self.state.attr_read_cache.lock().get(&ino).cloned()
            && timestamp.elapsed() < ATTR_CACHE_TTL
        {
            return Ok(attr);
        }

        let meta = self.meta.clone();
        let attr = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(ino)
        })
        .await?;
        self.state
            .attr_read_cache
            .lock()
            .insert(ino, (attr.clone(), Instant::now()));
        Ok(attr)
    }

    /// Set file attributes (partial update).
    ///
    /// Accepts `SetattrArgs` — any field set to `None`/`Omit` is left unchanged.
    pub fn setattr(&self, ino: Ino, args: SetattrArgs) -> VfsResult<InodeAttr> {
        let mut attr = self.getattr(ino)?;
        let mut changed = false;

        if let Some(new_size) = args.size
            && new_size != attr.size
        {
            self.begin_pending_incarnation(ino, &mut attr);
            if new_size < attr.size {
                // Truncate - may need to remove blocks
                self.truncate_blocks(ino, new_size, attr.size)?;
                attr.committed_size = attr.committed_size.min(new_size);
            }
            attr.size = new_size;
            self.update_pending_incarnation_size(ino, attr.size);
            changed = true;
        }

        if let Some(mode) = args.mode {
            attr.mode = mode & 0o7777;
            changed = true;
        }

        if let Some(uid) = args.uid {
            attr.uid = uid;
            changed = true;
        }

        if let Some(gid) = args.gid {
            attr.gid = gid;
            changed = true;
        }

        if let Some(ns) = args.atime.resolve() {
            attr.atime = ns;
            changed = true;
        }

        if let Some(ns) = args.mtime.resolve() {
            attr.mtime = ns;
            changed = true;
        }

        if let Some(ns) = args.crtime.resolve() {
            attr.crtime = ns;
            changed = true;
        }

        if let Some(wa) = args.win_attrs {
            attr.win_attrs = wa;
            changed = true;
        }

        if changed {
            // Always update ctime when any attribute changes
            attr.touch_ctime();

            // Store in dirty cache
            self.state.dirty_attrs.lock().insert(ino, attr.clone());

            // Invalidate read-through cache since we've changed the attribute
            self.state.attr_read_cache.lock().remove(&ino);

            // Commit to metadata
            self.meta.set_attr(&attr)?;
            self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        }

        Ok(attr)
    }

    /// Truncate blocks beyond new size.
    fn truncate_blocks(&self, ino: Ino, new_size: u64, old_size: u64) -> VfsResult<()> {
        let old_last_block = if old_size == 0 {
            0
        } else {
            block_index_from_offset(old_size - 1, self.block_size)
        };

        // Calculate the first block index to remove.
        // When new_size == 0, remove ALL blocks starting from index 0.
        // Otherwise, keep blocks 0..=new_last_block and remove the rest.
        let first_block_to_remove = if new_size == 0 {
            0
        } else {
            block_index_from_offset(new_size - 1, self.block_size) + 1
        };

        // Remove blocks beyond new size (or all blocks when new_size == 0).
        for block_idx in first_block_to_remove..=old_last_block {
            self.clear_pending_block(ino, block_idx);
            self.remove_live_extent(ino, block_idx);
            // Remove the block and get its ID for GC
            if let Some(object_id) = self.meta.remove_file_block(ino, block_idx)? {
                // Mark old block for deletion (S3 cleanup via background GC)
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                self.meta.add_pending_deletion(object_id, timestamp)?;
                debug!("marking block {} for deletion (truncate)", object_id);

                // Immediately evict this block from all cache tiers.
                self.evict_block_from_caches(ino, block_idx, object_id);
            } else {
                let _ = self.remove_spill_entry((ino, block_idx));
                let mut dirty = self.state.dirty_mem.lock();
                if let Some(data) = dirty.remove(&(ino, block_idx)) {
                    self.state
                        .dirty_bytes
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                            Some(v.saturating_sub(data.len()))
                        })
                        .ok();
                    self.state.dirty_drained_notify.notify_waiters();
                }
            }
        }

        // Update last block length if partial
        if new_size > 0 {
            let new_last_block = block_index_from_offset(new_size - 1, self.block_size);
            let last_block_offset = offset_within_block(new_size - 1, self.block_size);
            let new_last_len = last_block_offset as usize + 1;
            let trimmed_local = self.trim_local_block_staging(ino, new_last_block, new_last_len)?;
            if let Some(mut block_ref) = self.meta.get_file_block(ino, new_last_block)? {
                if !trimmed_local
                    && let Some(bytes) =
                        self.load_local_block_bytes_sync(ino, new_last_block, block_ref.object_id)?
                    && bytes.len() > new_last_len
                {
                    self.install_truncated_pending_block(ino, new_last_block, bytes, new_last_len);
                }
                block_ref.length = last_block_offset + 1;
                if block_ref.length < self.block_size {
                    block_ref.flags |= BlockFlags::PARTIAL_BLOCK;
                } else {
                    block_ref.flags.remove(BlockFlags::PARTIAL_BLOCK);
                }
                self.meta.set_file_block(ino, new_last_block, &block_ref)?;
            }
        }

        Ok(())
    }

    // =========================================================================
    // Directory Operations
    // =========================================================================

    /// Lookup an entry in a directory.
    pub fn lookup(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        let entry = self.meta.lookup(parent, name)?;
        Ok(self.meta.get_attr(entry.ino)?)
    }

    /// Read directory entries.
    pub fn readdir(&self, ino: Ino) -> VfsResult<Vec<DirEntry>> {
        let entries = self.meta.readdir(ino)?;

        // Feed the directory prefetch tracker with regular-file children.
        let file_children: Vec<Ino> = entries
            .iter()
            .filter(|e| e.kind == FileType::RegularFile)
            .map(|e| e.ino)
            .collect();
        if !file_children.is_empty() {
            self.dir_prefetch.on_readdir(ino, file_children);
        }

        Ok(entries)
    }

    // =========================================================================
    // File Handle Operations
    // =========================================================================

    /// Open a file and allocate a file handle.
    ///
    /// Called by FUSE `open()` / WinFSP `open()`.
    /// If `O_TRUNC` is requested (`trunc == true`), the file is truncated to 0.
    pub fn open_file(&self, ino: Ino, flags: OpenFlags, trunc: bool) -> VfsResult<u64> {
        // Verify the inode exists
        let attr = self.getattr(ino)?;
        if self.remote_upload_placeholder_visible(ino, &attr) {
            return Err(VfsError::RemoteContentPending);
        }

        // If O_TRUNC requested, truncate to 0
        if trunc {
            self.setattr(ino, SetattrArgs::size_only(0))?;
        }

        let fh = self.handle_table.open(ino, flags);
        debug!("open_file ino={} fh={} flags={:#x}", ino, fh, flags.bits());

        // Eager prefetch: when opening a regular file for reading, schedule
        // background prefetch of the first block to hide initial S3 latency.
        if attr.kind == studiofs_meta::types::FileType::RegularFile
            && attr.size > 0
            && (flags.contains(OpenFlags::RDONLY) || flags.contains(OpenFlags::RDWR))
        {
            if self.readahead.start_prefetch(ino, 0) {
                let _ = self.prefetch_tx.send((ino, 0));
            }
        }

        Ok(fh)
    }

    /// Release (close) a file handle.
    ///
    /// If the inode was unlinked while this handle was open (orphan) and this
    /// was the last open handle, perform the deferred deletion now.
    pub async fn release_fh(&self, fh: u64) -> VfsResult<()> {
        let Some((ino, remaining)) = self.handle_table.close(fh) else {
            debug!("release_fh: unknown fh={}", fh);
            return Ok(());
        };
        debug!("release_fh fh={} ino={} remaining={}", fh, ino, remaining);

        if remaining == 0 && self.handle_table.take_orphan(ino) {
            // Last handle closed on an unlinked file — final cleanup
            debug!("orphan cleanup: ino={} (last fd closed)", ino);

            self.state.deleted_inodes.lock().insert(ino);

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let meta = self.meta.clone();
            let (removed_ids, total_bytes) = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                let blocks = engine.get_file_blocks(ino)?;
                let total_bytes: u64 = blocks.iter().map(|(_, b)| b.length as u64).sum();

                let removed_ids = engine.remove_file_blocks(ino)?;
                for &object_id in &removed_ids {
                    engine.add_pending_deletion(object_id, timestamp)?;
                }

                if total_bytes > 0 {
                    engine.decr_counter(studiofs_meta::types::counters::USED_SPACE, total_bytes)?;
                }

                engine.remove_attr(ino)?;
                Ok((removed_ids, total_bytes))
            })
            .await?;

            if !removed_ids.is_empty() {
                self.state
                    .pending_blocks
                    .lock()
                    .retain(|(i, _), _| *i != ino);
                self.evict_inode_from_caches(ino, &removed_ids);
            }

            debug!(
                "orphan cleanup complete: ino={} removed_blocks={} freed_bytes={}",
                ino,
                removed_ids.len(),
                total_bytes
            );
            self.clear_inode_incarnation(ino);
            self.state.dirty_attrs.lock().remove(&ino);
            self.state.attr_read_cache.lock().remove(&ino);
        }
        Ok(())
    }

    /// Flush data for a file handle (per-fd close in POSIX).
    ///
    /// Called by FUSE `flush()`.  Flushes pending writes for the inode.
    pub async fn flush_fh(&self, fh: u64) -> VfsResult<()> {
        if let Some(open_file) = self.handle_table.get(fh) {
            self.flush(open_file.ino).await?;
        }
        Ok(())
    }

    /// fsync — flush data (and optionally metadata) to durable storage.
    ///
    /// 1. Flushes pending write buffer to the dirty-mem / slab pipeline.
    /// 2. If `!datasync`, also commits dirty attrs to redb.
    ///
    /// S3 upload remains async — same as LucidLink behaviour.
    pub async fn fsync(&self, ino: Ino, datasync: bool) -> VfsResult<()> {
        // Flush pending write buffer
        self.flush(ino).await?;

        // If full fsync (not datasync), also persist dirty attributes to redb
        if !datasync && let Some(attr) = self.state.dirty_attrs.lock().remove(&ino) {
            let meta = self.meta.clone();
            run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.set_attr(&attr)
            })
            .await?;
            self.state.attr_read_cache.lock().remove(&ino);
        }
        Ok(())
    }

    /// Get information about an open file handle.
    pub fn get_open_file(&self, fh: u64) -> Option<OpenFile> {
        self.handle_table.get(fh)
    }

    /// Get the number of open handles for an inode.
    pub fn open_count(&self, ino: Ino) -> u32 {
        self.handle_table.open_count(ino)
    }

    // =========================================================================
    // File Locking (flock)
    // =========================================================================

    /// Acquire or release a file-level lock (flock semantics).
    ///
    /// - `Shared`: multiple readers allowed, blocks if exclusive lock held by another owner.
    /// - `Exclusive`: single writer, blocks if any lock held by another owner.
    /// - `Unlock`: release the lock.
    ///
    /// If `blocking` is false and the lock cannot be acquired, returns `VfsError::WouldBlock`.
    pub async fn flock(&self, ino: Ino, op: LockKind, owner: u64, blocking: bool) -> VfsResult<()> {
        loop {
            if op == LockKind::Unlock {
                let had_local_lock = {
                    let mut locks = self.state.file_locks.lock();
                    let mut had = false;
                    if let Some(entry) = locks.get_mut(&ino) {
                        match entry.kind {
                            LockKind::Shared => {
                                had = true;
                                entry.shared_count = entry.shared_count.saturating_sub(1);
                                if entry.shared_count == 0 {
                                    locks.remove(&ino);
                                }
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                had = true;
                                locks.remove(&ino);
                            }
                            _ => {}
                        }
                    }
                    had
                };

                if had_local_lock {
                    if let Some(cfg) = &self.distributed_flock {
                        // Bimodal strong mode guarantee:
                        // flush local data + metadata before releasing distributed lock.
                        self.flush(ino).await?;
                        let meta = self.meta.clone();
                        run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                            engine.flush_metadata()?;
                            Ok(())
                        })
                        .await?;
                        let _ = self
                            .acquire_distributed_flock(cfg, ino, LockKind::Unlock)
                            .await?;
                    }
                    self.state.lock_notify.notify_waiters();
                }
                return Ok(());
            }

            let local_allowed = {
                let locks = self.state.file_locks.lock();
                match locks.get(&ino) {
                    None => true,
                    Some(entry) => match op {
                        LockKind::Shared => {
                            entry.kind == LockKind::Shared
                                || (entry.kind == LockKind::Exclusive && entry.owner == owner)
                        }
                        LockKind::Exclusive => {
                            entry.kind == LockKind::Unlock
                                || (entry.kind == LockKind::Exclusive && entry.owner == owner)
                        }
                        LockKind::Unlock => true,
                    },
                }
            };
            if !local_allowed {
                if !blocking {
                    return Err(VfsError::WouldBlock);
                }
                self.state.lock_notify.notified().await;
                continue;
            }

            if let Some(cfg) = &self.distributed_flock {
                let acquired = self.acquire_distributed_flock(cfg, ino, op).await?;
                if !acquired {
                    if !blocking {
                        return Err(VfsError::WouldBlock);
                    }
                    tokio::time::sleep(Duration::from_millis(cfg.retry_base_ms)).await;
                    continue;
                }
                // Bimodal strong mode guarantee:
                // refresh local metadata view before lock is considered granted.
                let meta = self.meta.clone();
                run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.sync_metadata()?;
                    Ok(())
                })
                .await?;
            }

            let committed = {
                let mut locks = self.state.file_locks.lock();
                match op {
                    LockKind::Shared => {
                        let entry = locks.entry(ino).or_insert(FileLock {
                            kind: LockKind::Unlock,
                            owner: 0,
                            shared_count: 0,
                        });
                        match entry.kind {
                            LockKind::Unlock | LockKind::Shared => {
                                entry.kind = LockKind::Shared;
                                entry.shared_count = entry.shared_count.saturating_add(1);
                                true
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                entry.kind = LockKind::Shared;
                                entry.shared_count = 1;
                                true
                            }
                            _ => false,
                        }
                    }
                    LockKind::Exclusive => {
                        let entry = locks.entry(ino).or_insert(FileLock {
                            kind: LockKind::Unlock,
                            owner: 0,
                            shared_count: 0,
                        });
                        match entry.kind {
                            LockKind::Unlock => {
                                *entry = FileLock {
                                    kind: LockKind::Exclusive,
                                    owner,
                                    shared_count: 0,
                                };
                                true
                            }
                            LockKind::Exclusive if entry.owner == owner => true,
                            _ => false,
                        }
                    }
                    LockKind::Unlock => true,
                }
            };
            if committed {
                return Ok(());
            }

            // Local conflict appeared after remote acquisition attempt.
            // Best-effort release remote lock and retry.
            if let Some(cfg) = &self.distributed_flock {
                let _ = self
                    .acquire_distributed_flock(cfg, ino, LockKind::Unlock)
                    .await;
            }
            if !blocking {
                return Err(VfsError::WouldBlock);
            }
            self.state.lock_notify.notified().await;
        }
    }

    // =========================================================================
    // Hardlinks
    // =========================================================================

    /// Create a hard link: add a new directory entry pointing to an existing inode.
    pub fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> VfsResult<InodeAttr> {
        validate_name(new_name)?;
        let attr = self.meta.link(ino, new_parent, new_name)?;
        // Invalidate attr caches (nlink changed)
        self.state.attr_read_cache.lock().remove(&ino);
        self.state.dirty_attrs.lock().remove(&ino);
        self.meta.request_eager_flush();
        Ok(attr)
    }

    /// Create a regular file.
    pub fn create(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> VfsResult<InodeAttr> {
        validate_name(name)?;
        let attr = self
            .meta
            .create_file(parent, name, uid, gid, mode & 0o7777)?;
        self.state.inode_incarnations.lock().insert(
            attr.ino,
            InodeIncarnationState {
                committed_incarnation: None,
                pending_incarnation: Some(PendingIncarnation {
                    content_epoch: attr.content_epoch,
                    size: attr.size,
                    base_size: 0,
                }),
                upload_state: VfsUploadState::Uploading,
            },
        );
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.attr_read_cache.lock().remove(&parent);
        self.meta.request_eager_flush();
        Ok(attr)
    }

    /// Create a directory.
    pub fn mkdir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> VfsResult<InodeAttr> {
        validate_name(name)?;
        let attr = self
            .meta
            .create_dir(parent, name, uid, gid, mode & 0o7777)?;
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.attr_read_cache.lock().remove(&parent);
        self.meta.request_eager_flush();
        Ok(attr)
    }

    /// Create a symbolic link.
    pub fn symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> VfsResult<InodeAttr> {
        validate_name(name)?;
        let attr = self.meta.create_symlink(parent, name, target, uid, gid)?;
        self.state.attr_read_cache.lock().remove(&parent);
        self.meta.request_eager_flush();
        Ok(attr)
    }

    /// Read the target of a symbolic link.
    pub fn readlink(&self, ino: Ino) -> VfsResult<Vec<u8>> {
        Ok(self.meta.get_symlink_target(ino)?)
    }

    // =========================================================================
    // Extended Attributes (xattr)
    // =========================================================================

    /// Get an extended attribute value.
    pub fn get_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<Option<Vec<u8>>> {
        Ok(self.meta.get_xattr(ino, name)?)
    }

    /// Set an extended attribute.
    pub fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> VfsResult<()> {
        self.meta.set_xattr(ino, name, value, flags)?;
        Ok(())
    }

    /// Remove an extended attribute.
    pub fn remove_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<()> {
        self.meta.remove_xattr(ino, name)?;
        Ok(())
    }

    /// List all extended attribute names for an inode.
    pub fn list_xattrs(&self, ino: Ino) -> VfsResult<Vec<Vec<u8>>> {
        Ok(self.meta.list_xattrs(ino)?)
    }

    /// Remove a file.
    ///
    /// If the file has open handles, the dentry is removed and nlink
    /// decremented, but the inode and blocks are kept alive (orphan).
    /// Actual cleanup happens in `release_fh()` when the last handle closes.
    pub fn unlink(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        // First, look up the entry to get the ino so we can check open handles.
        let entry = self.meta.lookup(parent, name)?;
        let ino = entry.ino;

        let open_handles = self.handle_table.open_count(ino);

        if open_handles > 0 {
            // Open-unlink: remove dentry + decrement nlink, but keep inode alive.
            let ev = self.meta.remove_entry(parent, name)?;
            if ev.kind == FileType::Directory {
                return Err(VfsError::Meta(MetaError::IsADirectory(ino)));
            }

            let mut attr = self.meta.get_attr(ino)?;
            attr.nlink = attr.nlink.saturating_sub(1);
            self.meta.set_attr(&attr)?;

            // Update parent mtime
            if let Ok(mut parent_attr) = self.meta.get_attr(parent) {
                parent_attr.touch_mtime();
                let _ = self.meta.set_attr(&parent_attr);
            }

            // Mark as orphan — release_fh will do the real cleanup
            self.handle_table.mark_orphan(ino);
            debug!(
                "unlink ino={}: {} open handles, deferred cleanup (orphan)",
                ino, open_handles
            );

            // Invalidate read-through cache (name no longer resolves)
            self.state.attr_read_cache.lock().remove(&ino);
        } else {
            // No open handles — immediate full cleanup via meta.unlink()
            let attr = self.meta.unlink(parent, name)?;

            if attr.nlink == 0 {
                // Inode fully removed — clean up blocks, caches, pending writes.

                // Mark this inode as deleted FIRST — the background slab flusher checks
                // this set to avoid re-inserting evicted blocks (race condition fix).
                self.state.deleted_inodes.lock().insert(attr.ino);

                // Remove all blocks and mark for GC
                let removed_ids = self.meta.remove_file_blocks(attr.ino)?;
                let l2_before = self.block_cache.l2_bytes();
                info!(
                    "unlink ino={}: removed {} block refs, object_ids={:?}, l2_before={}",
                    attr.ino,
                    removed_ids.len(),
                    removed_ids,
                    l2_before,
                );

                // Add to pending deletions (for eventual S3 deletion via background GC)
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);

                for object_id in &removed_ids {
                    self.meta.add_pending_deletion(*object_id, timestamp)?;
                    debug!("marking object {} for deletion (unlink)", object_id);
                }

                // Decrement USED_SPACE by the file's logical size
                if attr.size > 0 {
                    self.meta
                        .decr_counter(studiofs_meta::types::counters::USED_SPACE, attr.size)?;
                }

                // Clear pending blocks buffer
                self.state
                    .pending_blocks
                    .lock()
                    .retain(|(ino, _), _| *ino != attr.ino);

                // Immediately evict all cached blocks for this inode — no need to wait for GC.
                self.evict_inode_from_caches(attr.ino, &removed_ids);

                let l2_after = self.block_cache.l2_bytes();
                info!(
                    "unlink ino={}: l2_after={}, freed={} bytes",
                    attr.ino,
                    l2_after,
                    l2_before.saturating_sub(l2_after),
                );

                // Clear dirty attrs and read-through cache
                self.clear_inode_incarnation(attr.ino);
                self.state.dirty_attrs.lock().remove(&attr.ino);
            } else {
                info!(
                    "unlink ino={}: nlink={}, keeping inode alive (hardlink)",
                    attr.ino, attr.nlink
                );
            }

            self.state.attr_read_cache.lock().remove(&attr.ino);
        }

        self.meta.request_eager_flush();
        Ok(())
    }

    /// Remove a directory.
    pub fn rmdir(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        self.meta.rmdir(parent, name)?;
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.attr_read_cache.lock().remove(&parent);
        self.meta.request_eager_flush();
        Ok(())
    }

    /// Rename a file or directory.
    ///
    /// Uses `rename_atomic()` for a single-transaction metadata update.
    /// If the rename displaces an existing file (overwrite), its blocks
    /// are marked for GC and evicted from all cache tiers.
    pub fn rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> VfsResult<()> {
        if parent == new_parent && name == new_name {
            return Ok(());
        }
        validate_name(new_name)?;
        let displaced_ino = self
            .meta
            .rename_atomic(parent, name, new_parent, new_name)?;

        // If an existing entry was overwritten, schedule GC + evict caches
        if let Some(ino) = displaced_ino {
            self.state.deleted_inodes.lock().insert(ino);

            // Compute total bytes for USED_SPACE before removing blocks
            let blocks = self.meta.get_file_blocks(ino)?;
            let total_bytes: u64 = blocks.iter().map(|(_, b)| b.length as u64).sum();

            let removed_ids = self.meta.remove_file_blocks(ino)?;
            if !removed_ids.is_empty() {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                for &object_id in &removed_ids {
                    self.meta.add_pending_deletion(object_id, timestamp)?;
                }
                self.state
                    .pending_blocks
                    .lock()
                    .retain(|(i, _), _| *i != ino);
                self.evict_inode_from_caches(ino, &removed_ids);
            }

            // Decrement USED_SPACE for removed blocks
            if total_bytes > 0 {
                self.meta
                    .decr_counter(studiofs_meta::types::counters::USED_SPACE, total_bytes)?;
            }

            self.state.dirty_attrs.lock().remove(&ino);
            self.state.attr_read_cache.lock().remove(&ino);
            self.clear_inode_incarnation(ino);
        }

        // Invalidate source and dest parent directory caches
        self.state.attr_read_cache.lock().remove(&parent);
        if parent != new_parent {
            self.state.attr_read_cache.lock().remove(&new_parent);
        }
        self.meta.request_eager_flush();
        Ok(())
    }

    // =========================================================================
    // Cache Eviction Helpers
    // =========================================================================

    /// Immediately evict **all** blocks for `ino` from every cache tier.
    ///
    /// Called by `unlink()` so that deleted file data is no longer occupying
    /// disk / RAM space without waiting for the next background GC run.
    fn evict_inode_from_caches(&self, ino: Ino, object_ids: &[u64]) {
        self.remove_live_extents_for_inode(ino);

        // L1.5 — dirty memory buffer: drain blocks buffered post-flush.
        {
            let mut dirty = self.state.dirty_mem.lock();
            let removed_bytes: usize = dirty
                .iter()
                .filter(|((i, _), _)| *i == ino)
                .map(|(_, d)| d.len())
                .sum();
            dirty.retain(|(i, _), _| *i != ino);
            if removed_bytes > 0 {
                self.state
                    .dirty_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(removed_bytes))
                    })
                    .ok();
                self.state.dirty_drained_notify.notify_waiters();
                debug!(
                    "evict_inode L1.5: ino={} freed {} bytes from dirty_mem",
                    ino, removed_bytes
                );
            }
        }
        {
            let spill_keys: Vec<(Ino, u32)> = self
                .state
                .ingress_live
                .lock()
                .keys()
                .filter(|(i, _)| *i == ino)
                .cloned()
                .collect();
            for key in spill_keys {
                let _ = self.remove_spill_entry(key);
            }
        }

        // L1 + L2 — unified block cache: remove each object.
        // Blocks with a pending upload are pinned — schedule deferred eviction
        // so they are cleaned up once the upload completes (or is cancelled by
        // the uploader when it detects the inode is deleted).
        let mut evicted = 0;
        let mut deferred_pinned = 0;
        for &object_id in object_ids {
            if self.block_cache.is_pinned(object_id) {
                // Record for post-upload cleanup instead of silently skipping.
                self.state.pending_evictions.lock().insert(object_id);
                deferred_pinned += 1;
                continue;
            }
            self.block_cache.remove_sync(object_id);
            evicted += 1;
        }
        if evicted > 0 || deferred_pinned > 0 {
            debug!(
                "evict_inode: ino={} evicted {} blocks, deferred pinned={}",
                ino, evicted, deferred_pinned
            );
        }

        // Invalidate readahead state for this inode.
        self.readahead.evict(ino);
    }

    /// Immediately evict a single block from every cache tier.
    ///
    /// Called by `truncate_blocks()` for each block that is removed by a
    /// truncation.
    fn evict_block_from_caches(&self, ino: Ino, block_index: u32, object_id: u64) {
        self.clear_pending_block(ino, block_index);
        self.remove_live_extent(ino, block_index);

        // L1.5 — dirty memory buffer.
        {
            let mut dirty = self.state.dirty_mem.lock();
            if let Some(data) = dirty.remove(&(ino, block_index)) {
                self.state
                    .dirty_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(data.len()))
                    })
                    .ok();
                self.state.dirty_drained_notify.notify_waiters();
            }
        }
        let _ = self.remove_spill_entry((ino, block_index));

        // L1 + L2 — unified block cache (defer if pinned for pending upload).
        if self.block_cache.is_pinned(object_id) {
            self.state.pending_evictions.lock().insert(object_id);
            debug!(
                "evict_block: deferred pinned object_id={} ino={} block={}",
                object_id, ino, block_index
            );
        } else {
            self.block_cache.remove_sync(object_id);
        }
    }

    // =========================================================================
    // Read Path
    // =========================================================================

    /// Read file data at the given offset.
    /// Delegates to `read_into()` to avoid code duplication.
    pub async fn read(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        let attr = self.getattr_async(ino).await?;
        if self.remote_upload_placeholder_visible(ino, &attr) {
            return Err(VfsError::RemoteContentPending);
        }
        self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        let end = (offset + size as u64).min(attr.size);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let effective_size = (end - offset) as usize;
        let mut buf = vec![0u8; effective_size];
        let written = self.read_into(ino, offset, &mut buf).await?;
        buf.truncate(written as usize);
        Ok(Bytes::from(buf))
    }

    /// Read file data directly into a provided buffer (P1.3 — avoid intermediate Bytes allocation).
    ///
    /// Same as `read()` but writes directly to the output buffer, avoiding one memcpy.
    /// Returns the number of bytes written.
    pub async fn read_into(&self, ino: Ino, offset: u64, buffer: &mut [u8]) -> VfsResult<u32> {
        self.mark_foreground_read();
        struct ForegroundReadGuard<'a>(&'a StudioVfs);
        impl Drop for ForegroundReadGuard<'_> {
            fn drop(&mut self) {
                self.0.unmark_foreground_read();
            }
        }
        let _guard = ForegroundReadGuard(self);
        let attr = self.getattr_async(ino).await?;
        if self.remote_upload_placeholder_visible(ino, &attr) {
            return Err(VfsError::RemoteContentPending);
        }
        let incarnation_state = self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        let pending_content_epoch = incarnation_state
            .pending_incarnation
            .as_ref()
            .map(|pending| pending.content_epoch);

        // Clamp read to file size
        let end = (offset + buffer.len() as u64).min(attr.size);
        if offset >= end {
            return Ok(0);
        }
        let _effective_size = (end - offset) as usize;

        // Pre-compute all blocks needed
        let mut blocks_needed: Vec<(u32, usize, usize)> = Vec::new(); // (block_index, offset_in_block, to_copy)
        let mut cur = offset;
        while cur < end {
            let idx = block_index_from_offset(cur, self.block_size);
            let off_in = offset_within_block(cur, self.block_size) as usize;
            let space = self.block_size as usize - off_in;
            let to_copy = (end - cur).min(space as u64) as usize;
            blocks_needed.push((idx, off_in, to_copy));
            cur += to_copy as u64;
        }

        // If only one block, read sequentially to avoid overhead
        if blocks_needed.len() == 1 {
            let (block_idx, off_in, to_copy) = blocks_needed[0];
            return match self
                .read_block_or_pending(ino, block_idx, pending_content_epoch)
                .await
            {
                Ok(block_data) => {
                    let copy_len = if to_copy == 0 || off_in >= block_data.len() {
                        0
                    } else {
                        to_copy.min(block_data.len() - off_in)
                    };
                    if copy_len > 0 {
                        buffer[..copy_len].copy_from_slice(&block_data[off_in..off_in + copy_len]);
                    }
                    self.maybe_prefetch(ino, &blocks_needed);
                    Ok(copy_len as u32)
                }
                Err(VfsError::BlockNotFound(_, _)) => {
                    buffer[..to_copy].fill(0u8);
                    self.maybe_prefetch(ino, &blocks_needed);
                    Ok(to_copy as u32)
                }
                Err(e) => Err(e),
            };
        }

        // Parallel fetch — concurrency is governed by the AIMD controller
        // inside FetchScheduler, so no external semaphore is needed here.
        let fetch_futures: Vec<_> = blocks_needed
            .iter()
            .map(|(idx, _, _)| {
                let block_idx = *idx;
                async move {
                    self.read_block_or_pending(ino, block_idx, pending_content_epoch)
                        .await
                }
            })
            .collect();

        // Execute all fetches in parallel
        let fetched = futures::future::join_all(fetch_futures).await;

        // Write results directly to output buffer
        let mut written = 0usize;
        for (i, block_result) in fetched.into_iter().enumerate() {
            let (_block_idx, off_in, to_copy) = blocks_needed[i];
            match block_result {
                Ok(block_data) => {
                    let copy_len = if to_copy == 0 || off_in >= block_data.len() {
                        0
                    } else {
                        to_copy.min(block_data.len() - off_in)
                    };
                    if copy_len > 0 && written + copy_len <= buffer.len() {
                        buffer[written..written + copy_len]
                            .copy_from_slice(&block_data[off_in..off_in + copy_len]);
                        written += copy_len;
                    }
                }
                Err(VfsError::BlockNotFound(_, _)) => {
                    let zeros_len = to_copy.min(buffer.len().saturating_sub(written));
                    buffer[written..written + zeros_len].fill(0u8);
                    written += zeros_len;
                }
                Err(e) => return Err(e),
            }
        }

        self.maybe_prefetch(ino, &blocks_needed);
        Ok(written as u32)
    }

    /// Trigger read-ahead prefetches if a sequential access pattern is detected.
    /// Called after every successful `read_into()` to amortise scheduling overhead.
    #[inline]
    fn maybe_prefetch(&self, ino: Ino, blocks_needed: &[(u32, usize, usize)]) {
        if self.shutdown_requested() || blocks_needed.is_empty() || self.readahead.width == 0 {
            return;
        }
        // After transient transport failures, pause best-effort prefetch briefly
        // so foreground demand reads can recover the pipeline first.
        if self
            .fetch_scheduler
            .recent_send_error(self.prefetch_pause_on_neterr)
        {
            return;
        }
        // Keep headroom for foreground reads: cap best-effort prefetch by
        // current fetch window and in-flight load.
        let window = self.fetch_scheduler.window().floor().max(1.0) as usize;
        let in_flight = self.fetch_scheduler.in_flight();
        let available = window.saturating_sub(in_flight);
        let mut budget = available
            .saturating_sub(self.prefetch_foreground_reserve)
            .min(self.prefetch_max_per_cycle);
        if budget == 0 {
            return;
        }

        let first_block = blocks_needed[0].0;
        let last_block = blocks_needed.last().unwrap().0;
        let to_prefetch = self.readahead.record_read(ino, first_block, last_block);
        for block_idx in to_prefetch {
            if budget == 0 {
                break;
            }
            if self.readahead.start_prefetch(ino, block_idx) {
                // Best-effort; ignore send errors (worker may have exited).
                let _ = self.prefetch_tx.send((ino, block_idx));
                budget = budget.saturating_sub(1);
            }
        }

        // Directory-level prefetch: if this child is in a tracked directory
        // and the threshold is met, prefetch block 0 of sibling files.
        let siblings = self.dir_prefetch.on_child_read(ino);
        for sibling_ino in siblings.into_iter().take(self.dir_prefetch_max_batch) {
            if budget == 0 {
                break;
            }
            let _ = self.prefetch_tx.send((sibling_ino, 0));
            budget = budget.saturating_sub(1);
        }
    }

    /// Read a block from the local pending incarnation overlay first, then
    /// fall back to the committed/current base.
    async fn read_block_or_pending(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
    ) -> VfsResult<Bytes> {
        if let Some(data) = self
            .read_pending_overlay_block_async(ino, block_index, pending_content_epoch)
            .await?
        {
            return Ok(data);
        }

        self.read_block(ino, block_index).await
    }

    async fn get_file_block_async(
        &self,
        ino: Ino,
        block_index: u32,
    ) -> VfsResult<Option<BlockRef>> {
        let meta = self.meta.clone();
        run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            engine.get_file_block(ino, block_index)
        })
        .await
    }

    async fn load_spill_block_async(&self, key: (Ino, u32)) -> VfsResult<Option<Bytes>> {
        let state = self.state.clone();
        run_blocking(move || {
            let entry = state.ingress_live.lock().get_cloned(&key);
            let Some(entry) = entry else {
                return Ok(None);
            };
            Ok(Some(read_ingress_entry_bytes(&entry)?))
        })
        .await
    }

    /// Prefetch a block into cache (called from spawned prefetch tasks).
    ///
    /// Like `read_block()` but silently ignores errors and always marks the
    /// readahead inflight state as done when finished.
    async fn read_block_for_prefetch(&self, ino: Ino, block_index: u32) {
        if self.shutdown_requested() {
            self.readahead.finish_prefetch(ino, block_index);
            return;
        }
        if !self.readahead.is_prefetch_relevant(ino, block_index) {
            self.readahead.finish_prefetch(ino, block_index);
            return;
        }

        let block_ref = match self.get_file_block_async(ino, block_index).await {
            Ok(Some(block_ref)) => block_ref,
            _ => {
                self.readahead.finish_prefetch(ino, block_index);
                return;
            }
        };

        // Skip if block is already cached (L1 or L2) — avoid any S3 I/O.
        if self.block_cache.contains(block_ref.object_id) {
            self.readahead.finish_prefetch(ino, block_index);
            return;
        }
        if self.shutdown_requested() {
            self.readahead.finish_prefetch(ino, block_index);
            return;
        }

        // Best-effort prefetch: keep reserved AIMD headroom for foreground reads.
        let key = format_object_key(&self.data_prefix, block_ref.object_id);
        let _ = self
            .fetch_scheduler
            .fetch_block_prefetch(block_ref.object_id, &key)
            .await;
        self.readahead.finish_prefetch(ino, block_index);
    }

    /// Read a single block from S3 via the adaptive FetchScheduler.
    ///
    /// The scheduler handles:
    /// - L1/L2 cache checks (fast path)
    /// - Singleflight request coalescing (only one S3 GET per object_id)
    /// - AIMD-governed concurrency (TCP-cwnd–like adaptive window)
    /// - Retry with exponential backoff on transient errors
    /// - Cache population on success
    async fn read_block(&self, ino: Ino, block_index: u32) -> VfsResult<Bytes> {
        // Get block reference from metadata
        let block_ref = self
            .get_file_block_async(ino, block_index)
            .await?
            .ok_or(VfsError::BlockNotFound(ino, block_index))?;

        let key = format_object_key(&self.data_prefix, block_ref.object_id);
        self.fetch_scheduler
            .fetch_block(block_ref.object_id, &key)
            .await
            .map_err(|e| match e {
                crate::fetch::FetchError::NotFound { .. } => {
                    VfsError::BlockNotFound(ino, block_index)
                }
                other => VfsError::Other(other.to_string()),
            })
    }

    // =========================================================================
    // Write Path
    // =========================================================================

    /// Write data to a file at the given offset.
    ///
    /// Full blocks are flushed eagerly (package + cache + upload submit) so that
    /// the final `flush()` only handles the last partial block, eliminating the
    /// end-of-write stall where all blocks were previously processed at once.
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        if data.is_empty() {
            return Ok(0);
        }

        let initial_attr = self.getattr_async(ino).await?;
        let current_size = initial_attr.size;
        self.refresh_inode_incarnation_state_from_attr(ino, &initial_attr);
        let mut written = 0u64;
        let mut remaining = data.len();
        let mut current_offset = offset;
        let mut data_offset = 0;
        let mut full_blocks: Vec<(u32, Bytes)> = Vec::new();

        while remaining > 0 {
            let block_index = block_index_from_offset(current_offset, self.block_size);
            let offset_in_block = offset_within_block(current_offset, self.block_size) as usize;
            let space_in_block = self.block_size as usize - offset_in_block;
            let to_write = remaining.min(space_in_block);

            // Load existing block data if needed (outside lock to avoid holding
            // parking_lot::Mutex across .await)
            let need_load = !self
                .state
                .pending_blocks
                .lock()
                .contains_key(&(ino, block_index));
            if need_load {
                let existing_data = match self.read_block_if_exists(ino, block_index).await {
                    Ok(Some(d)) => d,
                    _ => Bytes::new(),
                };
                let logical_len = self.logical_block_len(current_size, block_index);
                let existing_data = if existing_data.len() > logical_len {
                    existing_data.slice(0..logical_len)
                } else {
                    existing_data
                };
                let mut pending = self.state.pending_blocks.lock();
                // Double-check: another write call might have inserted while we loaded
                pending
                    .entry((ino, block_index))
                    .or_insert_with(|| PendingBlock {
                        data: BytesMut::from(existing_data.as_ref()),
                        dirty: false,
                    });
            }

            // Write data into the pending block (brief lock)
            {
                let mut pending = self.state.pending_blocks.lock();
                let pending_block = pending.get_mut(&(ino, block_index)).unwrap();
                self.remove_live_extent(ino, block_index);

                // Ensure buffer is large enough
                let end_pos = offset_in_block + to_write;
                if pending_block.data.len() < end_pos {
                    pending_block.data.resize(end_pos, 0);
                }

                // Copy data
                pending_block.data[offset_in_block..end_pos]
                    .copy_from_slice(&data[data_offset..data_offset + to_write]);
                pending_block.dirty = true;

                // If block is now full → extract for eager flush
                if pending_block.data.len() >= self.block_size as usize {
                    let pb = pending.remove(&(ino, block_index)).unwrap();
                    full_blocks.push((block_index, pb.data.freeze()));
                }
            }

            written += to_write as u64;
            remaining -= to_write;
            current_offset += to_write as u64;
            data_offset += to_write;
        }

        // Enter or reuse a single pending incarnation for this inode and keep
        // all subsequent writes in the same local version until upload/commit catches up.
        let mut base_attr = self.getattr_async(ino).await?;
        self.begin_pending_incarnation(ino, &mut base_attr);
        {
            let mut dirty = self.state.dirty_attrs.lock();
            let attr = dirty.entry(ino).or_insert_with(|| base_attr.clone());
            attr.content_epoch = base_attr.content_epoch;
            if current_offset > attr.size {
                attr.size = current_offset;
                attr.touch_mtime();
            }
            self.update_pending_incarnation_size(ino, attr.size);
        }

        // Eagerly move full blocks into the dirty memory buffer so write() returns
        // immediately without waiting for object packaging or slab I/O.
        // The background slab flusher will process them asynchronously.
        self.enqueue_dirty_blocks(ino, full_blocks).await?;

        Ok(written as u32)
    }

    /// Read a block if it exists, returns None if block not found.
    async fn read_block_if_exists(&self, ino: Ino, block_index: u32) -> VfsResult<Option<Bytes>> {
        let pending_content_epoch = self.pending_content_epoch_for_inode(ino);
        if let Some(data) = self
            .read_pending_overlay_block_async(ino, block_index, pending_content_epoch)
            .await?
        {
            return Ok(Some(data));
        }

        // Get block reference from metadata
        let block_ref = match self.get_file_block_async(ino, block_index).await? {
            Some(br) => br,
            None => return Ok(None),
        };

        // Try unified cache (L1 + L2)
        if let Some(cached) = self.block_cache.get(block_ref.object_id).await {
            return Ok(Some(cached));
        }

        // Fetch from S3 via FetchScheduler (handles retry, AIMD, singleflight)
        let key = format_object_key(&self.data_prefix, block_ref.object_id);
        let data = self
            .fetch_scheduler
            .fetch_block(block_ref.object_id, &key)
            .await
            .map_err(|e| VfsError::Other(e.to_string()))?;

        Ok(Some(data))
    }

    /// Flush all buffered blocks for a file.
    ///
    /// With eager flushing in `write()`, most full blocks are already flushed.
    /// This handles only the remaining dirty blocks (typically just the last partial block).
    /// Remaining blocks are flushed in parallel for best performance.
    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        // Move all remaining dirty pending blocks into the dirty memory buffer.
        // The background slab flusher handles package + slab write + upload queue.
        // This function returns immediately after the in-memory move — no disk I/O.
        let blocks_to_buffer: Vec<(u32, Bytes)> = {
            let mut pending = self.state.pending_blocks.lock();
            let keys: Vec<_> = pending.keys().filter(|(i, _)| *i == ino).cloned().collect();

            let mut blocks = Vec::new();
            for key in keys {
                if let Some(pb) = pending.remove(&key)
                    && pb.dirty
                {
                    blocks.push((key.1, pb.data.freeze()));
                }
            }
            blocks
        };

        self.enqueue_dirty_blocks(ino, blocks_to_buffer).await?;

        // Keep flush non-blocking for interactive UX: move data to dirty buffer
        // and commit attrs; background flusher materializes block refs asynchronously.
        let dirty_attr = { self.state.dirty_attrs.lock().remove(&ino) };
        if let Some(attr) = dirty_attr {
            let attr_for_meta = attr.clone();
            let meta = self.meta.clone();
            run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.set_attr(&attr_for_meta)
            })
            .await?;
            self.state.attr_read_cache.lock().remove(&ino);
            self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        }

        Ok(())
    }

    // =========================================================================
    // Background Slab Flusher
    // =========================================================================

    /// Woken by `dirty_flush_tx` signals and a periodic timer.
    /// Drains accepted ingress records into cache, metadata, and upload intents.
    async fn background_slab_flusher(
        self: Arc<Self>,
        rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        materialize::Materializer::background_loop(self, rx).await;
    }

    async fn drain_dirty_mem(&self) {
        materialize::Materializer::drain(self).await;
    }

    // =========================================================================
    // Background Tasks
    // =========================================================================

    /// Background task to process pending uploads.
    async fn background_uploader(
        self: Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>,
    ) {
        info!("background uploader started");
        let worker_count = std::env::var("STUDIOFS_UPLOAD_WORKER_COUNT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or_else(|| {
                std::env::var("STUDIOFS_UPLOAD_MAX_SPAWNED")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(4)
            });

        let dispatch_capacity = worker_count.max(1) * 2;
        let (worker_tx, worker_rx) = tokio::sync::mpsc::channel::<PendingUpload>(dispatch_capacity);
        let worker_rx = std::sync::Arc::new(tokio::sync::Mutex::new(worker_rx));
        let mut workers = JoinSet::new();
        info!(
            upload_worker_count = worker_count,
            dispatch_capacity, "upload worker pool started"
        );

        for _ in 0..worker_count {
            let rx = worker_rx.clone();
            let vfs = self.clone();
            workers.spawn(async move {
                loop {
                    let op = tokio::select! {
                        _ = vfs.state.uploader_shutdown_notify.notified() => None,
                        op = async {
                            let mut rx = rx.lock().await;
                            rx.recv().await
                        } => op,
                    };
                    let Some(op) = op else {
                        break;
                    };
                    if vfs.uploader_shutdown_requested() {
                        break;
                    }

                    if let Err(e) = vfs.process_upload(op.clone()).await {
                        if vfs.uploader_shutdown_requested() {
                            break;
                        }
                        // If the inode was deleted, don't retry — cancel the upload.
                        if vfs.state.deleted_inodes.lock().contains(&op.ino) {
                            info!(
                                object_id = op.object_id,
                                ino = op.ino,
                                "upload failed for deleted inode, cancelling: {}",
                                e
                            );
                            vfs.block_cache.cancel_upload(&op, true);
                            vfs.state.pending_evictions.lock().remove(&op.object_id);
                        } else {
                            let mut retry_op = op;
                            retry_op.retry_count = retry_op.retry_count.saturating_add(1);
                            let delay = vfs.upload_requeue_delay(&retry_op);
                            vfs.state.upload_requeues.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                object_id = retry_op.object_id,
                                ino = retry_op.ino,
                                retry_count = retry_op.retry_count,
                                delay_ms = delay.as_millis() as u64,
                                "upload processing error, will requeue: {}",
                                e
                            );
                            let should_requeue = tokio::select! {
                                _ = vfs.state.uploader_shutdown_notify.notified() => false,
                                _ = tokio::time::sleep(delay) => !vfs.uploader_shutdown_requested(),
                            };
                            if should_requeue && !vfs.block_cache.requeue_upload(retry_op) {
                                error!("failed to requeue upload op after error");
                            }
                        }
                    }
                }
            });
        }

        let mut fair = FairInodeScheduler::default();
        let mut inbound_closed = false;
        loop {
            if self.uploader_shutdown_requested() || (inbound_closed && fair.is_empty()) {
                break;
            }

            tokio::select! {
                biased;
                _ = self.state.uploader_shutdown_notify.notified() => break,
                permit = worker_tx.reserve(), if !fair.is_empty() => {
                    match permit {
                        Ok(permit) => {
                            if let Some(op) = fair.pop() {
                                permit.send(op);
                            }
                        }
                        Err(_) => break,
                    }
                }
                inbound = rx.recv(), if !inbound_closed => {
                    match inbound {
                        Some(op) => fair.push(op.ino, op),
                        None => inbound_closed = true,
                    }
                }
            }
        }

        drop(worker_tx);
        let join_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if workers.is_empty() {
                break;
            }
            let remaining = join_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                break;
            }
            match tokio::time::timeout(remaining, workers.join_next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(e))) if e.is_cancelled() => {}
                Ok(Some(Err(e))) => {
                    warn!(error = %e, "uploader worker join failed");
                }
                Ok(None) => break,
                Err(_) => {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    break;
                }
            }
        }
    }

    /// Process a single upload operation.
    ///
    /// The cache stores raw block payloads; we package them into the STFS object
    /// format (header + payload) before uploading to S3. The upload itself is
    /// handled by `UploadScheduler` (AIMD concurrency + timeout/retry policy).
    ///
    /// If the owning inode has been deleted, the upload is cancelled immediately
    /// (no S3 write) and the block is evicted from cache.
    async fn process_upload(&self, op: PendingUpload) -> VfsResult<()> {
        if self.uploader_shutdown_requested() {
            return Ok(());
        }
        if self.upload_delay > Duration::from_millis(0) {
            let age_ms = Self::now_unix_millis().saturating_sub(op.created_unix_ms);
            let delay_ms = self.upload_delay.as_millis() as u64;
            if age_ms < delay_ms {
                tokio::select! {
                    _ = self.state.uploader_shutdown_notify.notified() => return Ok(()),
                    _ = tokio::time::sleep(Duration::from_millis(delay_ms - age_ms)) => {}
                }
            }
        }
        if self.uploader_shutdown_requested() {
            return Ok(());
        }

        // ── Fast-path: inode deleted → cancel upload, skip S3 write ──────
        let inode_deleted = self.state.deleted_inodes.lock().contains(&op.ino);
        // Also check metadata: after a remount, `deleted_inodes` is empty but
        // the inode may have been deleted in a prior session.  If the inode no
        // longer exists in the metadata store, the upload is stale.
        let meta = self.meta.clone();
        let attr = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(op.ino)
        })
        .await
        .ok();
        let inode_gone = inode_deleted || attr.is_none();
        if inode_gone {
            self.state
                .upload_canceled_deleted
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                "upload cancelled: inode deleted or missing"
            );
            self.block_cache.cancel_upload(&op, true);
            self.state.pending_evictions.lock().remove(&op.object_id);
            return Ok(());
        }

        // Coalescing: if this file block has already been replaced in metadata
        // with a newer object_id, skip uploading this stale one.
        let block_index = block_index_from_offset(op.offset, self.block_size);
        let expected_extent = ExtentRecordId {
            object_id: op.object_id,
            content_epoch: op.content_epoch,
        };
        if let Some(current_extent) = self
            .state
            .live_extent_records
            .lock()
            .get(&(op.ino, block_index))
            .copied()
            && current_extent != expected_extent
        {
            self.state
                .upload_canceled_stale
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                block_index,
                queued_epoch = op.content_epoch,
                current_object_id = current_extent.object_id,
                current_epoch = current_extent.content_epoch,
                "upload cancelled: stale extent record superseded"
            );
            self.block_cache.cancel_upload(&op, true);
            self.state.pending_evictions.lock().remove(&op.object_id);
            return Ok(());
        }
        if let Some(attr) = attr.as_ref()
            && attr.content_epoch != op.content_epoch
        {
            self.state
                .upload_canceled_stale
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                block_index,
                queued_epoch = op.content_epoch,
                current_epoch = attr.content_epoch,
                "upload cancelled: stale content epoch"
            );
            self.block_cache.cancel_upload(&op, true);
            self.state.pending_evictions.lock().remove(&op.object_id);
            return Ok(());
        }
        if let Ok(Some(current)) = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_file_block(op.ino, block_index)
        })
        .await
            && current.object_id != op.object_id
        {
            self.state
                .upload_canceled_stale
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                current_object_id = current.object_id,
                ino = op.ino,
                block_index,
                "upload cancelled: stale block superseded by newer object"
            );
            self.block_cache.cancel_upload(&op, true);
            self.state.pending_evictions.lock().remove(&op.object_id);
            return Ok(());
        }

        let t0 = Instant::now();
        let _upload_qos = self
            .qos
            .acquire_upload()
            .await
            .map_err(|e| VfsError::Other(format!("upload qos permit failed: {e}")))?;
        // Upload QoS: when user reads are active, each upload consumes multiple
        // local I/O slots, effectively shrinking upload pressure and preserving
        // read latency/throughput for foreground operations.
        let upload_io_cost = if self.foreground_read_active() {
            self.upload_cache_io_user_penalty.max(1)
        } else {
            1
        };
        let _upload_io_permit = self
            .upload_cache_io_slots
            .clone()
            .acquire_many_owned(upload_io_cost)
            .await
            .map_err(|e| VfsError::Other(format!("upload io qos permit failed: {e}")))?;
        // Read raw block payload from cache, then package for S3.
        let raw_payload = match self.block_cache.get(op.object_id).await {
            Some(data) => data,
            None => {
                // Block data is missing from cache.  If the inode was deleted
                // between our check above and now (race), cancel cleanly.
                // Otherwise this is a genuine error — let the caller requeue.
                if self.state.deleted_inodes.lock().contains(&op.ino) {
                    self.block_cache.cancel_upload(&op, false);
                    self.state.pending_evictions.lock().remove(&op.object_id);
                    return Ok(());
                }
                return Err(VfsError::Other(format!(
                    "object {} missing from cache",
                    op.object_id
                )));
            }
        };
        let object_bytes = Bytes::from(create_object(&raw_payload)?);
        drop(_upload_io_permit);

        let key = format_object_key(&self.data_prefix, op.object_id);
        self.upload_scheduler
            .put(&key, object_bytes)
            .await
            .map_err(|e| VfsError::Other(e.to_string()))?;

        let current = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_file_block(op.ino, block_index)
        })
        .await?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        match current {
            Some(ref block_ref)
                if block_ref.object_id == op.object_id
                    && run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                        engine
                            .get_attr(op.ino)
                            .map(|attr| attr.content_epoch == op.content_epoch)
                    })
                    .await
                    .unwrap_or(false) =>
            {
                commit::CommitCoordinator::finish_extent_upload(self, &op, block_index).await?;
                self.block_cache.complete_upload(&op);
            }
            _ => {
                run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                    engine.add_pending_deletion(op.object_id, timestamp)
                })
                .await?;
                debug!(
                    object_id = op.object_id,
                    ino = op.ino,
                    block_index,
                    "upload completed for stale block, marked for deletion"
                );
                self.block_cache.complete_upload(&op);
            }
        }

        if let Ok(attr) = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(op.ino)
        })
        .await
        {
            self.refresh_inode_incarnation_state_from_attr(op.ino, &attr);
        } else {
            self.clear_inode_incarnation(op.ino);
        }

        // Post-upload cleanup: if the block was marked for deferred eviction
        // (inode was deleted while the upload was in flight), remove it now.
        if self.state.pending_evictions.lock().remove(&op.object_id) {
            self.block_cache.remove_sync(op.object_id);
            debug!(
                object_id = op.object_id,
                "post-upload eviction: block removed (inode deleted)"
            );
        } else if let Ok(Some(current)) =
            run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                engine.get_file_block(op.ino, block_index)
            })
            .await
            && current.object_id != op.object_id
        {
            self.block_cache.remove_sync(op.object_id);
        }

        debug!(
            object_id = op.object_id,
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "background upload complete"
        );
        Ok(())
    }

    async fn background_upload_stats(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = self.state.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.upload_stats_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            let pending = self.block_cache.pending_upload_count();
            let metadata_backlog = self.meta.replication_backlog();
            let (p95_ms, max_ms) = self.pending_age_stats_ms();
            let ingress = self.ingress_stats();
            info!(
                pending_uploads = pending,
                pending_metadata_ops = metadata_backlog.total_ops(),
                "upload pipeline"
            );
            if !self.upload_pipeline_is_idle(pending, &ingress) {
                debug!(
                    requeues = self.state.upload_requeues.load(Ordering::Relaxed),
                    canceled_deleted = self.state.upload_canceled_deleted.load(Ordering::Relaxed),
                    canceled_stale = self.state.upload_canceled_stale.load(Ordering::Relaxed),
                    foreground_reads_inflight =
                        self.state.foreground_reads_inflight.load(Ordering::Relaxed),
                    dirty_mem_blocks = ingress.dirty_mem_blocks,
                    dirty_mem_bytes = ingress.dirty_mem_bytes,
                    spill_records = ingress.spill_records,
                    spill_bytes = ingress.spill_bytes,
                    oldest_spill_age_ms = ingress.oldest_spill_age_ms,
                    forced_spill_blocks = ingress.forced_spill_blocks,
                    drained_mem_blocks = ingress.drained_mem_blocks,
                    drained_mem_bytes = ingress.drained_mem_bytes,
                    drained_spill_blocks = ingress.drained_spill_blocks,
                    drained_spill_bytes = ingress.drained_spill_bytes,
                    checkpoint_persists = ingress.checkpoint_persists,
                    checkpoint_failures = ingress.checkpoint_failures,
                    last_checkpoint_age_ms = ingress.last_checkpoint_age_ms,
                    last_checkpoint_segment_id = ingress.last_checkpoint_segment_id,
                    last_checkpoint_offset = ingress.last_checkpoint_offset,
                    checkpoint_pending_bytes = ingress.checkpoint_pending_bytes,
                    checkpoint_pending_mutations = ingress.checkpoint_pending_mutations,
                    checkpoint_active_segment_lag = ingress.checkpoint_active_segment_lag,
                    checkpoint_live_segments = ingress.checkpoint_live_segments,
                    write_backpressure_wait_ms = ingress.write_backpressure_wait_ms,
                    upload_io_slots_available = self.upload_cache_io_slots.available_permits(),
                    qos_accept_available = self.qos.accept_available(),
                    qos_materialize_available = self.qos.materialize_available(),
                    qos_upload_available = self.qos.upload_available(),
                    qos_commit_available = self.qos.commit_available(),
                    qos_fetch_available = self.qos.fetch_available(),
                    pending_metadata_ops = metadata_backlog.pending_ops,
                    inflight_metadata_ops = metadata_backlog.inflight_ops,
                    "upload pipeline counters"
                );
            }
            self.upload_scheduler.log_stats(pending, p95_ms, max_ms);
        }
    }

    async fn background_ingress_checkpoint(self: Arc<Self>) {
        loop {
            let timer = tokio::time::sleep(self.ingress_checkpoint_interval);
            tokio::pin!(timer);
            let timer_fired = tokio::select! {
                _ = self.state.shutdown_notify.notified() => return,
                _ = &mut timer => true,
                _ = self.state.ingress_checkpoint_notify.notified() => false,
            };
            if self
                .state
                .ingress_checkpoint_stopped
                .load(Ordering::Acquire)
                || self.shutdown_requested()
            {
                return;
            }
            if self.state.ingress_live.lock().is_empty() {
                continue;
            }
            if !self.ingress_checkpoint_due(timer_fired) {
                continue;
            }
            let state = self.state.clone();
            let ingress_checkpoint_dir = self.ingress_checkpoint_dir.clone();
            let ingress_segment_dir = self.ingress_segment_dir.clone();
            let ingress_spill_dir = self.ingress_spill_dir.clone();
            if let Err(e) = run_blocking(move || {
                Self::persist_ingress_recovery_state_owned(
                    state,
                    ingress_checkpoint_dir,
                    ingress_segment_dir,
                    ingress_spill_dir,
                )
            })
            .await
            {
                self.state
                    .ingress_checkpoint_failures
                    .fetch_add(1, Ordering::Relaxed);
                warn!("background ingress checkpoint persist failed: {}", e);
            }
        }
    }

    /// Background task for garbage collection.
    async fn background_gc(self: Arc<Self>) {
        info!("background GC started");

        let interval = tokio::time::Duration::from_secs(600); // 10 minutes
        let grace_period = 3600i64; // 1 hour grace period

        loop {
            tokio::select! {
                _ = self.state.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }

            // Get current timestamp
            let now = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            // Get pending deletions
            let meta = self.meta.clone();
            match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.get_pending_deletions()
            })
            .await
            {
                Ok(pending) => {
                    let mut deleted_count = 0;
                    for (object_id, timestamp) in pending {
                        // Check if grace period has passed
                        if now - timestamp < grace_period {
                            debug!("skipping object {} - still in grace period", object_id);
                            continue;
                        }

                        // Delete from S3
                        let key = format_object_key(&self.data_prefix, object_id);
                        match self.data_write.delete_by_key(&key).await {
                            Ok(_) => {
                                // Remove from pending deletions
                                let meta = self.meta.clone();
                                if let Err(e) =
                                    run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                                        engine.remove_pending_deletion(object_id)?;
                                        Ok(())
                                    })
                                    .await
                                {
                                    warn!(
                                        "failed to remove pending deletion for {}: {}",
                                        object_id, e
                                    );
                                }
                                deleted_count += 1;
                                debug!("GC deleted object {}", object_id);
                            }
                            Err(e) => {
                                warn!("failed to delete object {} from S3: {}", object_id, e);
                            }
                        }
                    }
                    if deleted_count > 0 {
                        info!("GC deleted {} objects", deleted_count);
                    }
                }
                Err(e) => {
                    error!("failed to get pending deletions: {}", e);
                }
            }
        }
    }

    /// Background janitor for orphaned objects:
    /// - scans all objects under data_prefix
    /// - compares with currently referenced block object_ids in metadata
    /// - deletes unreferenced objects older than grace window
    async fn background_janitor(self: Arc<Self>) {
        info!(
            interval_secs = self.janitor_interval.as_secs(),
            grace_secs = self.janitor_skip_newer_than.as_secs(),
            max_deletes = self.janitor_max_deletes_per_run,
            "background janitor started"
        );
        let mut cycle: u64 = 0;

        loop {
            cycle = cycle.saturating_add(1);
            tokio::select! {
                _ = self.state.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.janitor_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }

            if let Some(lease) = &self.janitor_lease
                && !self.acquire_janitor_lease(lease).await
            {
                debug!(
                    cycle,
                    lock_path = lease.lock_path,
                    "janitor cycle skipped: lease not acquired"
                );
                continue;
            }

            let meta = self.meta.clone();
            let referenced = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.get_referenced_object_ids()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor cycle failed: get_referenced_object_ids");
                    continue;
                }
            };
            let meta = self.meta.clone();
            let pending_map: HashMap<u64, i64> =
                match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.get_pending_deletions()
                })
                .await
                {
                    Ok(v) => v.into_iter().collect(),
                    Err(e) => {
                        warn!(cycle, error = %e, "janitor: failed to read pending deletions");
                        HashMap::new()
                    }
                };

            let entries = match self
                .data_write
                .operator()
                .list_with(&self.data_prefix)
                .recursive(true)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(
                        cycle,
                        prefix = self.data_prefix,
                        error = %e,
                        "janitor cycle failed: list data prefix"
                    );
                    continue;
                }
            };

            let now = SystemTime::now();
            let now_unix = now
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let mut scanned: u64 = 0;
            let mut orphan_candidates: u64 = 0;
            let mut deleted: u64 = 0;
            let mut skipped_young: u64 = 0;
            let mut skipped_unknown_age: u64 = 0;
            let mut skipped_cap: u64 = 0;
            let mut skipped_non_object_key: u64 = 0;
            let mut delete_errors: u64 = 0;

            for entry in entries {
                if entry.metadata().mode() != EntryMode::FILE {
                    continue;
                }
                scanned = scanned.saturating_add(1);
                let path = entry.path();
                let object_id = match parse_object_id_from_key(path) {
                    Ok(id) => id,
                    Err(_) => {
                        skipped_non_object_key = skipped_non_object_key.saturating_add(1);
                        continue;
                    }
                };
                if referenced.contains(&object_id) {
                    continue;
                }
                orphan_candidates = orphan_candidates.saturating_add(1);
                if deleted >= self.janitor_max_deletes_per_run {
                    skipped_cap = skipped_cap.saturating_add(1);
                    continue;
                }

                let mut is_old_enough = false;
                if let Some(ts) = pending_map.get(&object_id) {
                    let age = now_unix.saturating_sub(*ts) as u64;
                    if age >= self.janitor_skip_newer_than.as_secs() {
                        is_old_enough = true;
                    }
                } else if let Some(last_modified) = entry.metadata().last_modified() {
                    let last_modified_st: SystemTime = last_modified.into();
                    let age = now
                        .duration_since(last_modified_st)
                        .unwrap_or_else(|_| Duration::from_secs(0));
                    if age >= self.janitor_skip_newer_than {
                        is_old_enough = true;
                    }
                } else {
                    skipped_unknown_age = skipped_unknown_age.saturating_add(1);
                }

                if !is_old_enough {
                    skipped_young = skipped_young.saturating_add(1);
                    continue;
                }

                if let Err(e) = self.data_write.operator().delete(path).await {
                    delete_errors = delete_errors.saturating_add(1);
                    warn!(cycle, path, error = %e, "janitor: delete failed");
                    continue;
                }
                deleted = deleted.saturating_add(1);
            }

            debug!(
                cycle,
                scanned,
                referenced = referenced.len(),
                orphan_candidates,
                deleted,
                skipped_young,
                skipped_unknown_age,
                skipped_cap,
                skipped_non_object_key,
                delete_errors,
                "janitor cycle complete"
            );
        }
    }

    /// Persist the cache checkpoint to disk.
    ///
    /// Called at shutdown so the LRU state survives across remounts.
    pub async fn persist_cache(&self) {
        self.block_cache.shutdown().await;
        tracing::info!("block cache checkpoint persisted");
    }

    async fn flush_pending_blocks_for_shutdown(&self) {
        let mut inos: BTreeSet<Ino> = {
            let pending = self.state.pending_blocks.lock();
            pending.keys().map(|(ino, _)| *ino).collect()
        };
        {
            let dirty_attrs = self.state.dirty_attrs.lock();
            inos.extend(dirty_attrs.keys().copied());
        }
        if inos.is_empty() {
            return;
        }

        info!(
            inode_count = inos.len(),
            "VFS shutdown: flushing buffered inodes"
        );
        for ino in inos {
            if let Err(e) = self.flush(ino).await {
                warn!(ino, error = %e, "VFS shutdown: flush inode failed");
            }
        }
    }

    async fn reconcile_remote_completed_pending(&self) -> usize {
        let pending_ops = self.block_cache.snapshot_pending_uploads();
        if pending_ops.is_empty() {
            return 0;
        }

        let mut reconciled = 0usize;
        for op in &pending_ops {
            let key = format_object_key(&self.data_prefix, op.object_id);
            match tokio::time::timeout(
                Duration::from_secs(5),
                self.fetch_scheduler.data_store().exists(&key),
            )
            .await
            {
                Ok(Ok(true)) => {
                    let block_index = block_index_from_offset(op.offset, self.block_size);
                    if let Err(e) =
                        commit::CommitCoordinator::finish_extent_upload(self, op, block_index).await
                    {
                        debug!(
                            object_id = op.object_id,
                            ino = op.ino,
                            error = %e,
                            "VFS shutdown: publish reconcile after remote exists() failed"
                        );
                    } else {
                        self.block_cache.complete_upload(op);
                    }
                    reconciled += 1;
                }
                Ok(Ok(false)) => {}
                Ok(Err(e)) => {
                    debug!(
                        object_id = op.object_id,
                        error = %e,
                        "VFS shutdown: remote reconcile exists() failed"
                    );
                }
                Err(_) => {
                    debug!(
                        object_id = op.object_id,
                        "VFS shutdown: remote reconcile exists() timed out, skipping"
                    );
                }
            }
        }
        if reconciled > 0 {
            info!(
                reconciled,
                "VFS shutdown: reconciled pending uploads already present on remote"
            );
        }
        reconciled
    }

    /// Best-effort graceful shutdown: drain dirty buffers, wait for pending
    /// uploads to complete, then persist cache checkpoint.
    pub async fn shutdown(&self, upload_drain_timeout: Duration) {
        if self.state.shutdown_completed.load(Ordering::Acquire) {
            return;
        }
        if self.state.shutdown_requested.swap(true, Ordering::AcqRel) {
            return;
        }
        let t0 = Instant::now();
        let background_join_timeout = Duration::from_secs(2);

        // Phase 0: quiesce non-uploader background work so shutdown can finish
        // the remaining accepted ingress without concurrent producers racing it.
        self.state
            .slab_flusher_stopped
            .store(true, Ordering::Release);
        self.state
            .ingress_checkpoint_stopped
            .store(true, Ordering::Release);
        self.state.shutdown_notify.notify_waiters();
        self.state.ingress_checkpoint_notify.notify_waiters();
        let _ = self.dirty_flush_tx.send(());
        self.join_background_tasks(background_join_timeout).await;

        // Phase 1: flush pending write buffer → dirty_mem.
        let dirty_before = self.state.dirty_bytes.load(Ordering::Relaxed);
        info!(
            dirty_bytes = dirty_before,
            pending_uploads = self.block_cache.pending_upload_count(),
            "VFS shutdown: flushing pending write buffer"
        );
        self.flush_pending_blocks_for_shutdown().await;

        // Phase 2: drain remaining accepted ingress into cache + upload queue.
        // Loop until the materializer consumes every live accepted record.
        let dirty_after_flush = self.state.dirty_bytes.load(Ordering::Relaxed);
        info!(
            dirty_bytes = dirty_after_flush,
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: draining dirty memory buffer"
        );
        loop {
            let before = self.state.dirty_bytes.load(Ordering::Relaxed);
            if before == 0
                && self.state.dirty_mem.lock().is_empty()
                && self.state.ingress_live.lock().is_empty()
            {
                break;
            }
            self.drain_dirty_mem().await;
        }
        info!(
            pending_uploads = self.block_cache.pending_upload_count(),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: drain complete"
        );
        let state = self.state.clone();
        let ingress_checkpoint_dir = self.ingress_checkpoint_dir.clone();
        let ingress_segment_dir = self.ingress_segment_dir.clone();
        let ingress_spill_dir = self.ingress_spill_dir.clone();
        if let Err(e) = run_blocking(move || {
            Self::persist_ingress_recovery_state_owned(
                state,
                ingress_checkpoint_dir,
                ingress_segment_dir,
                ingress_spill_dir,
            )
        })
        .await
        {
            self.state
                .ingress_checkpoint_failures
                .fetch_add(1, Ordering::Relaxed);
            warn!("VFS shutdown: ingress recovery state persist failed: {}", e);
        }

        // Phase 3: wait for uploads to drain, reconcile already-uploaded blocks.
        let pending = self.block_cache.pending_upload_count();
        if pending > 0 {
            if upload_drain_timeout.is_zero() {
                info!(
                    pending,
                    "VFS shutdown: skipping upload drain (resume from journal on next start)"
                );
            } else {
                info!(pending, "VFS shutdown: waiting for uploads to drain");
                let mut drained = self
                    .block_cache
                    .wait_for_uploads_idle(upload_drain_timeout)
                    .await;
                if !drained {
                    let reconciled = self.reconcile_remote_completed_pending().await;
                    if reconciled > 0 {
                        info!(
                            reconciled,
                            "VFS shutdown: reconciled already-uploaded blocks"
                        );
                        drained = self
                            .block_cache
                            .wait_for_uploads_idle(Duration::from_millis(500))
                            .await;
                    }
                }
                if !drained {
                    let pending_ops = self.block_cache.snapshot_pending_uploads();
                    let sample_object_ids: Vec<u64> =
                        pending_ops.iter().take(8).map(|op| op.object_id).collect();
                    warn!(
                        pending = self.block_cache.pending_upload_count(),
                        pending_journal_files = pending_ops.len(),
                        pending_object_ids_sample = ?sample_object_ids,
                        timeout_ms = upload_drain_timeout.as_millis() as u64,
                        "VFS shutdown: upload queue did not fully drain before timeout"
                    );
                }
            }
        }
        self.request_uploader_shutdown();
        self.join_uploader_task(background_join_timeout).await;

        // Phase 4: persist metadata + cache checkpoint.
        info!(
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: persisting metadata"
        );
        let meta = self.meta.clone();
        let metadata_flush_timeout = shutdown_metadata_flush_timeout();
        match run_sync_with_timeout(metadata_flush_timeout, move || {
            meta.flush_metadata().map_err(VfsError::Meta)
        }) {
            Ok(Some(())) => {}
            Ok(None) => {
                warn!(
                    timeout_ms = metadata_flush_timeout.as_millis() as u64,
                    "VFS shutdown: metadata flush timed out, leaving remote sync for next session"
                );
            }
            Err(e) => {
                warn!("VFS shutdown: metadata flush failed: {}", e);
            }
        }
        self.persist_cache().await;
        self.state.shutdown_completed.store(true, Ordering::Release);
        info!(
            total_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: complete"
        );
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    #[test]
    fn test_block_index_calculation() {
        let block_size = 4 * 1024 * 1024; // 4MB

        assert_eq!(block_index_from_offset(0, block_size), 0);
        assert_eq!(block_index_from_offset(1, block_size), 0);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024, block_size), 1);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024 + 1, block_size), 1);
    }

    #[test]
    fn test_run_sync_with_timeout_returns_result_before_deadline() {
        let result = run_sync_with_timeout(Duration::from_millis(100), || Ok::<_, VfsError>(7))
            .expect("operation should succeed");
        assert_eq!(result, Some(7));
    }

    #[test]
    fn test_run_sync_with_timeout_returns_none_on_timeout() {
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();
        let result = run_sync_with_timeout(Duration::from_millis(10), move || {
            std::thread::sleep(Duration::from_millis(100));
            finished_clone.store(true, AtomicOrdering::Release);
            Ok::<_, VfsError>(())
        })
        .expect("timeout helper should not fail");

        assert_eq!(result, None);
        assert!(
            !finished.load(AtomicOrdering::Acquire),
            "timed-out helper should return before the slow operation finishes"
        );
    }

    #[test]
    fn test_offset_within_block() {
        let block_size = 4 * 1024 * 1024; // 4MB

        assert_eq!(offset_within_block(0, block_size), 0);
        assert_eq!(offset_within_block(100, block_size), 100);
        assert_eq!(offset_within_block(4 * 1024 * 1024, block_size), 0);
        assert_eq!(offset_within_block(4 * 1024 * 1024 + 100, block_size), 100);
    }

    // =========================================================================
    // HandleTable Unit Tests
    // =========================================================================

    #[test]
    fn test_handle_table_open_close() {
        let ht = HandleTable::new();
        let fh1 = ht.open(10, OpenFlags::RDONLY);
        let fh2 = ht.open(10, OpenFlags::RDWR);
        let fh3 = ht.open(20, OpenFlags::WRONLY);

        assert_ne!(fh1, fh2);
        assert_ne!(fh2, fh3);

        // Open count
        assert_eq!(ht.open_count(10), 2);
        assert_eq!(ht.open_count(20), 1);
        assert_eq!(ht.open_count(99), 0);

        // Get
        let of1 = ht.get(fh1).unwrap();
        assert_eq!(of1.ino, 10);
        assert!(of1.flags.contains(OpenFlags::RDONLY));

        let of3 = ht.get(fh3).unwrap();
        assert_eq!(of3.ino, 20);

        assert!(ht.get(99999).is_none());

        // Close fh1 — ino 10 still has 1 open
        let (ino, remaining) = ht.close(fh1).unwrap();
        assert_eq!(ino, 10);
        assert_eq!(remaining, 1);
        assert_eq!(ht.open_count(10), 1);

        // Close fh2 — ino 10 now at 0
        let (ino, remaining) = ht.close(fh2).unwrap();
        assert_eq!(ino, 10);
        assert_eq!(remaining, 0);
        assert_eq!(ht.open_count(10), 0);

        // Double-close returns None
        assert!(ht.close(fh1).is_none());
    }

    #[test]
    fn test_handle_table_orphans() {
        let ht = HandleTable::new();
        let _fh = ht.open(42, OpenFlags::RDWR);

        // Not an orphan yet
        assert!(!ht.take_orphan(42));

        // Mark as orphan
        ht.mark_orphan(42);

        // take_orphan returns true and clears it
        assert!(ht.take_orphan(42));
        assert!(!ht.take_orphan(42)); // second call returns false
    }

    #[test]
    fn test_open_flags_bitwise() {
        let f = OpenFlags::RDWR | OpenFlags::APPEND;
        assert!(f.contains(OpenFlags::RDWR));
        assert!(f.contains(OpenFlags::APPEND));
        assert!(!f.contains(OpenFlags::WRONLY));

        let f2 = f.union(OpenFlags::WRONLY);
        assert!(f2.contains(OpenFlags::WRONLY));
    }

    #[test]
    fn test_validate_name_valid() {
        assert!(validate_name(b"hello.txt").is_ok());
        assert!(validate_name(b"a").is_ok());
        assert!(validate_name(b"...").is_ok());
        assert!(validate_name(&vec![b'x'; 255]).is_ok());
    }

    #[test]
    fn test_validate_name_empty() {
        assert!(matches!(validate_name(b""), Err(VfsError::InvalidName(_))));
    }

    #[test]
    fn test_validate_name_too_long() {
        let long = vec![b'a'; 256];
        assert!(matches!(
            validate_name(&long),
            Err(VfsError::InvalidName(_))
        ));
    }

    #[test]
    fn test_validate_name_slash() {
        assert!(matches!(
            validate_name(b"a/b"),
            Err(VfsError::InvalidName(_))
        ));
    }

    #[test]
    fn test_validate_name_dot() {
        assert!(matches!(validate_name(b"."), Err(VfsError::InvalidName(_))));
        assert!(matches!(
            validate_name(b".."),
            Err(VfsError::InvalidName(_))
        ));
    }

    #[test]
    fn test_validate_name_windows_special_chars() {
        // Test each Windows-invalid character: < > : " \ | ? *
        assert!(matches!(
            validate_name(b"file<name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file>name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file:name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file\"name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file\\name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file|name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file?name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file*name"),
            Err(VfsError::InvalidName(_))
        ));
    }

    #[test]
    fn test_recover_spill_dir_reads_segment_backed_entries() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            INGRESS_RECORD_PUT,
            b"newer-data",
        )
        .unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 8),
            3,
            300,
            INGRESS_RECORD_PUT,
            b"other",
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();

        assert_eq!(recovered.len(), 2);
        let first = recovered.get(&(42, 7)).unwrap();
        let second = recovered.get(&(42, 8)).unwrap();
        let mut file = std::fs::File::open(&first.path).unwrap();
        file.seek(SeekFrom::Start(first.offset)).unwrap();
        let mut buf = vec![0u8; first.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"newer-data");
        let mut file = std::fs::File::open(&second.path).unwrap();
        file.seek(SeekFrom::Start(second.offset)).unwrap();
        let mut buf = vec![0u8; second.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"other");
        assert_eq!(total_bytes, "newer-data".len() + "other".len());
        assert_eq!(next_seq, 4);
        assert_eq!(active_segment_id, 1);
        assert!(recovered.get(&(42, 7)).unwrap().created_unix_ms > 0);
    }

    #[test]
    fn test_oldest_spill_age_ms_uses_oldest_record() {
        let mut spill = std::collections::HashMap::new();
        spill.insert(
            (1, 0),
            SpillEntry {
                seq: 1,
                path: PathBuf::from("a.spill"),
                offset: 0,
                len: 4,
                created_unix_ms: 1_000,
            },
        );
        spill.insert(
            (1, 1),
            SpillEntry {
                seq: 2,
                path: PathBuf::from("b.spill"),
                offset: 0,
                len: 4,
                created_unix_ms: 4_000,
            },
        );

        assert_eq!(oldest_spill_age_ms(10_000, &spill), 9_000);
    }

    #[test]
    fn test_read_ingress_entry_bytes_reads_segment_slice() {
        let dir = tempfile::tempdir().unwrap();
        let segment_dir = dir.path().join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            INGRESS_RECORD_PUT,
            b"first",
        )
        .unwrap();
        let second_offset = append_ingress_record(
            &segment_dir,
            1,
            (42, 8),
            2,
            200,
            INGRESS_RECORD_PUT,
            b"second-payload",
        )
        .unwrap();
        let segment_path = active_ingress_segment_path(&segment_dir, 1);

        let bytes = read_ingress_entry_bytes(&SpillEntry {
            seq: 2,
            path: segment_path,
            offset: second_offset,
            len: "second-payload".len(),
            created_unix_ms: 200,
        })
        .unwrap();

        assert_eq!(bytes.as_ref(), b"second-payload");
    }

    #[test]
    fn test_recover_ingress_segment_records_keeps_latest_put() {
        let dir = tempfile::tempdir().unwrap();
        let segment_dir = dir.path().join("segments");
        append_ingress_record(&segment_dir, 1, (42, 7), 1, 100, INGRESS_RECORD_PUT, b"old")
            .unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            2,
            200,
            INGRESS_RECORD_PUT,
            b"newer",
        )
        .unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 8),
            3,
            300,
            INGRESS_RECORD_PUT,
            b"other",
        )
        .unwrap();

        let (recovered, next_seq) = recover_ingress_segment_records(&segment_dir).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(next_seq, 4);
        let newer = recovered.get(&(42, 7)).unwrap();
        let other = recovered.get(&(42, 8)).unwrap();
        let mut file = std::fs::File::open(&newer.path).unwrap();
        file.seek(SeekFrom::Start(newer.offset)).unwrap();
        let mut buf = vec![0u8; newer.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"newer");
        let mut file = std::fs::File::open(&other.path).unwrap();
        file.seek(SeekFrom::Start(other.offset)).unwrap();
        let mut buf = vec![0u8; other.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"other");
    }

    #[test]
    fn test_recover_ingress_segment_records_applies_invalidate() {
        let dir = tempfile::tempdir().unwrap();
        let segment_dir = dir.path().join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            INGRESS_RECORD_PUT,
            b"payload",
        )
        .unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            2,
            200,
            INGRESS_RECORD_INVALIDATE,
            &[],
        )
        .unwrap();

        let (recovered, next_seq) = recover_ingress_segment_records(&segment_dir).unwrap();
        assert_eq!(next_seq, 3);
        assert!(
            !recovered.contains_key(&(42, 7)),
            "invalidate should remove the latest accepted record from replay view"
        );
    }

    #[test]
    fn test_recover_spill_dir_rebuilds_from_segments_without_spill_files() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            5,
            123,
            INGRESS_RECORD_PUT,
            b"segment-data",
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(total_bytes, "segment-data".len());
        assert_eq!(next_seq, 6);
        assert_eq!(active_segment_id, 1);
        let entry = recovered.get(&(42, 7)).unwrap();
        let mut file = std::fs::File::open(&entry.path).unwrap();
        file.seek(SeekFrom::Start(entry.offset)).unwrap();
        let mut buf = vec![0u8; entry.len];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"segment-data");
    }

    #[test]
    fn test_recover_spill_dir_replays_only_segment_tail_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        let checkpoint_dir = spill_dir.join("checkpoints");
        let first_offset = append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            1,
            100,
            INGRESS_RECORD_PUT,
            b"first",
        )
        .unwrap();
        let mut ingress_live = IngressLiveIndex::default();
        ingress_live.insert(
            (42, 7),
            SpillEntry {
                seq: 1,
                path: active_ingress_segment_path(&segment_dir, 1),
                offset: first_offset,
                len: "first".len(),
                created_unix_ms: 100,
            },
        );
        write_ingress_checkpoint(&checkpoint_dir, &ingress_live, &segment_dir, 1, 2).unwrap();

        append_ingress_record(
            &segment_dir,
            1,
            (42, 8),
            2,
            200,
            INGRESS_RECORD_PUT,
            b"second",
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(total_bytes, "first".len() + "second".len());
        assert_eq!(next_seq, 3);
        assert_eq!(active_segment_id, 1);
        assert_eq!(
            read_ingress_entry_bytes(recovered.get(&(42, 7)).unwrap()).unwrap(),
            "first"
        );
        assert_eq!(
            read_ingress_entry_bytes(recovered.get(&(42, 8)).unwrap()).unwrap(),
            "second"
        );
    }

    #[test]
    fn test_recover_spill_dir_applies_tail_invalidate_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        let checkpoint_dir = spill_dir.join("checkpoints");
        let first_offset = append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            7,
            100,
            INGRESS_RECORD_PUT,
            b"payload",
        )
        .unwrap();
        let mut ingress_live = IngressLiveIndex::default();
        ingress_live.insert(
            (42, 7),
            SpillEntry {
                seq: 7,
                path: active_ingress_segment_path(&segment_dir, 1),
                offset: first_offset,
                len: "payload".len(),
                created_unix_ms: 100,
            },
        );
        write_ingress_checkpoint(&checkpoint_dir, &ingress_live, &segment_dir, 1, 8).unwrap();
        append_ingress_record(
            &segment_dir,
            1,
            (42, 7),
            8,
            200,
            INGRESS_RECORD_INVALIDATE,
            &[],
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();
        assert!(recovered.is_empty());
        assert_eq!(total_bytes, 0);
        assert_eq!(next_seq, 9);
        assert_eq!(active_segment_id, 1);
    }

    #[test]
    fn test_write_ingress_checkpoint_prunes_fully_covered_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        let checkpoint_dir = spill_dir.join("checkpoints");
        append_ingress_record(&segment_dir, 1, (1, 0), 1, 100, INGRESS_RECORD_PUT, b"old").unwrap();
        let new_offset =
            append_ingress_record(&segment_dir, 2, (1, 1), 2, 200, INGRESS_RECORD_PUT, b"new")
                .unwrap();
        let mut ingress_live = IngressLiveIndex::default();
        ingress_live.insert(
            (1, 1),
            SpillEntry {
                seq: 2,
                path: ingress_segment_path(&segment_dir, 2),
                offset: new_offset,
                len: 3,
                created_unix_ms: 200,
            },
        );

        write_ingress_checkpoint(&checkpoint_dir, &ingress_live, &segment_dir, 2, 3).unwrap();

        assert!(!ingress_segment_path(&segment_dir, 1).exists());
        assert!(ingress_segment_path(&segment_dir, 2).exists());
    }

    #[test]
    fn test_recover_spill_dir_uses_state_file_for_writer_head() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        write_ingress_state(spill_dir, 5, 11).unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();

        assert!(recovered.is_empty());
        assert_eq!(total_bytes, 0);
        assert_eq!(next_seq, 11);
        assert_eq!(active_segment_id, 5);
    }

    #[test]
    fn test_recover_spill_dir_prefers_replayed_seq_over_stale_state_file() {
        let dir = tempfile::tempdir().unwrap();
        let spill_dir = dir.path();
        let segment_dir = spill_dir.join("segments");
        write_ingress_state(spill_dir, 1, 2).unwrap();
        append_ingress_record(
            &segment_dir,
            3,
            (42, 7),
            9,
            100,
            INGRESS_RECORD_PUT,
            b"payload",
        )
        .unwrap();

        let (recovered, total_bytes, next_seq, active_segment_id) =
            recover_spill_dir(spill_dir).unwrap();

        assert_eq!(recovered.len(), 1);
        assert_eq!(total_bytes, "payload".len());
        assert_eq!(next_seq, 10);
        assert_eq!(active_segment_id, 3);
    }

    #[test]
    fn test_ingress_checkpoint_due_on_timer_with_pending_churn() {
        assert!(ingress_checkpoint_due_with_policy(
            0, 1, 1024, 8, 0, 2, 1, 4, true
        ));
        assert!(!ingress_checkpoint_due_with_policy(
            0, 1, 1024, 8, 0, 2, 1, 4, false
        ));
    }

    #[test]
    fn test_ingress_checkpoint_due_on_threshold_notify() {
        assert!(ingress_checkpoint_due_with_policy(
            4096, 0, 4096, 8, 0, 2, 1, 4, false
        ));
        assert!(ingress_checkpoint_due_with_policy(
            0, 8, 4096, 8, 0, 2, 1, 4, false
        ));
        assert!(!ingress_checkpoint_due_with_policy(
            1024, 1, 4096, 8, 0, 2, 1, 4, false
        ));
    }

    #[test]
    fn test_ingress_checkpoint_due_on_segment_lag_or_live_segment_spread() {
        assert!(ingress_checkpoint_due_with_policy(
            1024, 1, 4096, 8, 2, 2, 1, 4, false
        ));
        assert!(ingress_checkpoint_due_with_policy(
            1024, 1, 4096, 8, 0, 2, 4, 4, false
        ));
    }
}

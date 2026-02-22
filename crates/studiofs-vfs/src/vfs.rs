//! Core VFS implementation — Block-based architecture.
//!
//! Key design:
//! - Files are split into fixed-size blocks (default 4MB)
//! - Each block has a sequential ID (not content hash)
//! - Blocks are immutable - never modified, only created/deleted
//! - No deduplication = no reference counting overhead
//! - All metadata in redb, S3 stores only raw data blocks
//!
//! Write Path:
//! 1. Split data into blocks
//! 2. Compress with LZ4
//! 3. Allocate sequential block ID
//! 4. Store in cache with journal
//! 5. Queue for async upload
//! 6. Update metadata with BlockRef
//!
//! Read Path:
//! 1. Look up block references from metadata
//! 2. Fetch from cache/S3
//! 3. Decompress
//! 4. Assemble response

use crate::readahead::{DirAccessTracker, ReadAheadTracker};
use async_nats::Client as NatsClient;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use opendal::options::WriteOptions;
use opendal::{EntryMode, ErrorKind, Operator, services::S3};
use parking_lot::Mutex;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use studiofs_cache::disk::{DEFAULT_DISK_CACHE_SIZE, DiskCache};
use studiofs_cache::memory::{CacheKey, MemoryCache};
use studiofs_cache::writeback::{PendingOp, WritebackManager};
use studiofs_data::object::{
    create_object, format_object_key, parse_object_id_from_key, read_object,
};
use studiofs_data::store::DataStore;
use studiofs_meta::types::{BlockFlags, BlockRef, block_index_from_offset, offset_within_block};
use studiofs_meta::*;
use thiserror::Error;
use tokio::sync::{Semaphore, broadcast};
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

    #[error("disk cache error: {0}")]
    DiskCache(#[from] studiofs_cache::disk::DiskCacheError),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("object error: {0}")]
    Object(#[from] studiofs_data::object::ObjectError),

    #[error("block not found: inode {0}, block {1}")]
    BlockNotFound(Ino, u32),

    #[error("lock conflict (EAGAIN)")]
    WouldBlock,

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

pub type VfsResult<T> = Result<T, VfsError>;

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
const DEFAULT_REMOTE_READ_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_REMOTE_READ_RETRIES: u32 = 3;
const DEFAULT_REMOTE_UPLOAD_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_READ_CONCURRENCY: usize = 16;
const DEFAULT_PREFETCH_CONCURRENCY: usize = 8;
const DEFAULT_DIR_PREFETCH_MAX_BATCH: usize = 0;
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
    /// Maximum disk cache size in bytes.
    pub cache_size: u64,
    /// Block size in bytes (default 4MB).
    pub block_size: u32,
    /// Maximum concurrent uploads.
    pub max_concurrent_uploads: usize,
    /// Upload retry count.
    pub upload_retries: u32,
    /// Store decompressed blocks in disk cache for faster reads.
    /// On cache hit, no decompression is needed. Uses slightly more disk space
    /// but is a net win for low-compressibility content (VFX, compressed media).
    pub store_decompressed: bool,
    /// Number of blocks to prefetch ahead when a sequential access pattern is
    /// detected.  0 disables read-ahead entirely.  Default: 32 (= 128 MB with
    /// 4 MB blocks).
    pub readahead_width: u32,
    /// Maximum bytes to hold in the dirty-write memory buffer before applying
    /// backpressure.  This buffer stores blocks that `flush()` has accepted
    /// but the background slab flusher has not yet written to disk.
    /// Default: 2 GiB.
    pub max_dirty_bytes: usize,
}

impl Default for VfsConfig {
    fn default() -> Self {
        Self {
            meta_path: String::new(),
            cache_dir: String::new(),
            cache_size: DEFAULT_DISK_CACHE_SIZE,
            block_size: studiofs_meta::types::DEFAULT_BLOCK_SIZE,
            max_concurrent_uploads: 32,
            upload_retries: 3,
            store_decompressed: true,
            readahead_width: 32,
            max_dirty_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
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

// =============================================================================
// Internal State (for Clone support)
// =============================================================================

/// Internal state that can be cloned and shared.
struct VfsState {
    /// Pending blocks: (inode, block_index) -> PendingBlock
    pending_blocks: Mutex<std::collections::HashMap<(Ino, u32), PendingBlock>>,
    /// Dirty attributes cache (from writes not yet synced to redb).
    dirty_attrs: Mutex<std::collections::HashMap<Ino, InodeAttr>>,
    /// Read-through attribute cache with TTL: inode -> (attr, timestamp)
    attr_read_cache: Mutex<std::collections::HashMap<Ino, (InodeAttr, Instant)>>,
    /// Compressed object data waiting for S3 upload (used when store_decompressed=true).
    /// Maps object_id -> compressed STFS bytes. Removed once the upload completes.
    pending_upload_data: Mutex<std::collections::HashMap<u64, bytes::Bytes>>,
    /// In-flight S3 block fetches for request coalescing.
    /// When multiple readers request the same block concurrently, only the first
    /// issues an S3 GET; the rest wait for a broadcast notification and then
    /// read from the newly-populated cache.
    inflight_fetches: Mutex<std::collections::HashMap<(Ino, u32), broadcast::Sender<()>>>,
    /// Dirty blocks accepted by `flush()` that have not yet been written to the
    /// slab by the background slab flusher.  Reads must consult this buffer
    /// BEFORE falling through to the disk cache or S3.
    dirty_mem: Mutex<std::collections::HashMap<(Ino, u32), Bytes>>,
    /// Approximate total bytes currently held in `dirty_mem` (for backpressure).
    dirty_bytes: AtomicUsize,
    /// Inodes that have been unlinked.  The background slab flusher checks this
    /// set before calling `flush_block()` to avoid re-inserting evicted blocks
    /// into the cache (TOCTOU race between `drain_dirty_mem` and `unlink`).
    deleted_inodes: Mutex<std::collections::HashSet<Ino>>,
    /// File-level locks (flock semantics).
    file_locks: Mutex<std::collections::HashMap<Ino, FileLock>>,
    /// Notification for waiters blocked on flock.
    lock_notify: tokio::sync::Notify,
}

#[derive(Clone)]
struct JanitorLease {
    op: Operator,
    lock_path: String,
    owner_id: String,
}

#[derive(Clone)]
struct InvalidationBus {
    nats_url: String,
    subject_base: String,
    sender_id: String,
}

// =============================================================================
// StudioVfs Implementation
// =============================================================================

/// The core VFS - satisfies filesystem operations using metadata + data + cache.
pub struct StudioVfs {
    /// Metadata engine.
    meta: Arc<dyn MetaEngine>,
    /// Data store (S3).
    data: Arc<DataStore>,
    /// Disk cache.
    disk_cache: Arc<DiskCache>,
    /// Writeback manager.
    writeback: Arc<WritebackManager>,
    /// Optional L1 in-memory block cache.
    mem_cache: Option<Arc<MemoryCache>>,
    /// Block size in bytes.
    block_size: u32,
    /// Volume data prefix for S3 object keys.
    data_prefix: String,
    /// Internal state (mutable).
    state: Arc<VfsState>,
    // --- Upload Control ---
    /// Semaphore for limiting concurrent uploads.
    upload_semaphore: Arc<Semaphore>,
    /// Upload retry count.
    upload_retries: u32,
    /// Whether disk cache stores decompressed block data.
    /// When true: cache reads skip decompression; uploads re-use pre-compressed data
    /// from pending_upload_data instead of compressing again.
    store_decompressed: bool,
    /// Sequential read-ahead tracker.
    readahead: Arc<ReadAheadTracker>,
    /// Directory-level prefetch tracker.
    dir_prefetch: Arc<DirAccessTracker>,
    /// Channel sender for scheduling background prefetch tasks.
    /// The receiver lives in a spawned task that has an Arc<Self>.
    prefetch_tx: tokio::sync::mpsc::UnboundedSender<(Ino, u32)>,
    /// Signal sender to wake the background slab flusher when new dirty blocks
    /// are buffered.
    dirty_flush_tx: tokio::sync::mpsc::UnboundedSender<()>,
    /// Maximum total bytes allowed in the dirty-write memory buffer before
    /// `flush()` / `write()` apply backpressure.
    max_dirty_bytes: usize,
    /// Handle table: tracks all open file descriptors.
    handle_table: HandleTable,
    /// Optional distributed lease for janitor leader election.
    janitor_lease: Option<JanitorLease>,
    janitor_interval: Duration,
    janitor_skip_newer_than: Duration,
    janitor_max_deletes_per_run: u64,
    janitor_lease_ttl: Duration,
    remote_read_timeout: Duration,
    remote_read_retries: u32,
    remote_upload_timeout: Duration,
    read_concurrency: usize,
    prefetch_concurrency: usize,
    dir_prefetch_max_batch: usize,
    /// Optional serverless distributed lock manager for bimodal consistency.
    distributed_flock: Option<DistributedFlock>,
    /// Optional NATS invalidation bus listener config.
    invalidation_bus: Option<InvalidationBus>,
}

impl StudioVfs {
    /// Create a new VFS instance.
    pub fn new(
        meta: Arc<dyn MetaEngine>,
        data: Arc<DataStore>,
        mem_cache: Option<Arc<MemoryCache>>,
        cache_dir: &str,
        cache_size: u64,
    ) -> VfsResult<Arc<Self>> {
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
        let invalidation_bus = std::env::var("STUDIOFS_NATS_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(|url| InvalidationBus {
                nats_url: url,
                subject_base: format!("fs.invalidate.{}", format_info.name),
                sender_id: format!("pid:{}", std::process::id()),
            });
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
        let remote_read_timeout = Duration::from_secs(
            std::env::var("STUDIOFS_REMOTE_READ_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_REMOTE_READ_TIMEOUT.as_secs()),
        );
        let remote_read_retries = std::env::var("STUDIOFS_REMOTE_READ_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_REMOTE_READ_RETRIES);
        let remote_upload_timeout = Duration::from_secs(
            std::env::var("STUDIOFS_REMOTE_UPLOAD_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_REMOTE_UPLOAD_TIMEOUT.as_secs()),
        );
        let read_concurrency = std::env::var("STUDIOFS_READ_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_READ_CONCURRENCY);
        let prefetch_concurrency = std::env::var("STUDIOFS_PREFETCH_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_PREFETCH_CONCURRENCY);
        let dir_prefetch_max_batch = std::env::var("STUDIOFS_DIR_PREFETCH_MAX_BATCH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_DIR_PREFETCH_MAX_BATCH);
        let lock_lease_ttl = Duration::from_secs(
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
        let lock_retry_max_ms = std::env::var("STUDIOFS_LOCK_RETRY_MAX_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v >= lock_retry_base_ms)
            .unwrap_or(DEFAULT_LOCK_RETRY_MAX_MS.max(lock_retry_base_ms));

        let janitor_lease = if format_info.metadata.backend == MetadataBackend::Tonbo {
            if let Some(remote) = &format_info.metadata.remote {
                let mut builder = S3::default()
                    .endpoint(&remote.endpoint)
                    .bucket(&remote.bucket)
                    .access_key_id(&remote.access_key)
                    .secret_access_key(&remote.secret_key)
                    .disable_config_load()
                    .disable_ec2_metadata();
                if !remote.region.is_empty() {
                    builder = builder.region(&remote.region);
                } else {
                    builder = builder.region("us-east-1");
                }
                let op = Operator::new(builder)
                    .map_err(|e| {
                        VfsError::Other(format!("failed to init janitor lease operator: {e}"))
                    })?
                    .finish();
                let mut prefix = if remote.prefix.trim().is_empty() {
                    format!("{}/metadata/", format_info.name)
                } else {
                    remote.prefix.clone()
                };
                if !prefix.ends_with('/') {
                    prefix.push('/');
                }
                let now_nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                Some(JanitorLease {
                    op,
                    lock_path: format!("{}janitor.lock", prefix),
                    owner_id: format!("{}:{}:{}", format_info.name, std::process::id(), now_nanos),
                })
            } else {
                None
            }
        } else {
            None
        };

        let distributed_flock = if format_info.metadata.backend == MetadataBackend::Tonbo {
            if let Some(remote) = &format_info.metadata.remote {
                let mut builder = S3::default()
                    .endpoint(&remote.endpoint)
                    .bucket(&remote.bucket)
                    .access_key_id(&remote.access_key)
                    .secret_access_key(&remote.secret_key)
                    .disable_config_load()
                    .disable_ec2_metadata();
                if !remote.region.is_empty() {
                    builder = builder.region(&remote.region);
                } else {
                    builder = builder.region("us-east-1");
                }
                let op = Operator::new(builder)
                    .map_err(|e| VfsError::Other(format!("failed to init lock operator: {e}")))?
                    .finish();
                let mut prefix = if remote.prefix.trim().is_empty() {
                    format!("{}/metadata/", format_info.name)
                } else {
                    remote.prefix.clone()
                };
                if !prefix.ends_with('/') {
                    prefix.push('/');
                }
                let now_nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                Some(DistributedFlock {
                    op,
                    prefix: format!("{prefix}locks/"),
                    client_id: format!("{}:{}:{}", format_info.name, std::process::id(), now_nanos),
                    lease_ttl: lock_lease_ttl,
                    retry_base_ms: lock_retry_base_ms,
                    retry_max_ms: lock_retry_max_ms,
                })
            } else {
                None
            }
        } else {
            None
        };

        // Init Disk Cache
        let disk_cache = DiskCache::new(cache_dir, cache_size)?;

        // Init Writeback Manager
        let (writeback, upload_rx) = WritebackManager::new(cache_dir, disk_cache.clone());

        // Init ReadAhead tracker + prefetch channel
        let readahead = Arc::new(ReadAheadTracker::new(32));
        let dir_prefetch = Arc::new(DirAccessTracker::new(2));
        let (prefetch_tx, mut prefetch_rx) = tokio::sync::mpsc::unbounded_channel::<(Ino, u32)>();

        // Dirty-write memory buffer + background slab flusher channel.
        let (dirty_flush_tx, dirty_flush_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let vfs = Arc::new(Self {
            meta,
            data,
            disk_cache,
            writeback,
            mem_cache,
            block_size,
            data_prefix,
            state: Arc::new(VfsState {
                pending_blocks: Mutex::new(std::collections::HashMap::new()),
                dirty_attrs: Mutex::new(std::collections::HashMap::new()),
                attr_read_cache: Mutex::new(std::collections::HashMap::new()),
                pending_upload_data: Mutex::new(std::collections::HashMap::new()),
                inflight_fetches: Mutex::new(std::collections::HashMap::new()),
                dirty_mem: Mutex::new(std::collections::HashMap::new()),
                dirty_bytes: AtomicUsize::new(0),
                deleted_inodes: Mutex::new(std::collections::HashSet::new()),
                file_locks: Mutex::new(std::collections::HashMap::new()),
                lock_notify: tokio::sync::Notify::new(),
            }),
            upload_semaphore: Arc::new(Semaphore::new(32)),
            upload_retries: 3,
            store_decompressed: true,
            readahead: readahead.clone(),
            dir_prefetch,
            prefetch_tx,
            dirty_flush_tx,
            max_dirty_bytes: 2 * 1024 * 1024 * 1024,
            handle_table: HandleTable::new(),
            janitor_lease,
            janitor_interval,
            janitor_skip_newer_than,
            janitor_max_deletes_per_run,
            janitor_lease_ttl,
            remote_read_timeout,
            remote_read_retries,
            remote_upload_timeout,
            read_concurrency,
            prefetch_concurrency,
            dir_prefetch_max_batch,
            distributed_flock,
            invalidation_bus,
        });

        // Spawn prefetch worker — holds Arc<Self> so it can call read_block()
        let vfs_for_prefetch = vfs.clone();
        let prefetch_limit = vfs.prefetch_concurrency.max(1);
        tokio::spawn(async move {
            let prefetch_sem = Arc::new(Semaphore::new(prefetch_limit));
            while let Some((ino, block_idx)) = prefetch_rx.recv().await {
                let permit = prefetch_sem.clone().acquire_owned().await.unwrap();
                let v = vfs_for_prefetch.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = v.read_block_for_prefetch(ino, block_idx).await;
                });
            }
        });

        // Spawn Upload Worker
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_uploader(upload_rx).await;
        });

        // Warm up remote data path (DNS/TCP/TLS/session) so first cold-cache read
        // does not pay full connection setup latency on the user request path.
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_remote_warmup().await;
        });

        // Spawn GC Worker
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_gc().await;
        });

        // Spawn Janitor Worker (automatic orphan scan/cleanup)
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_janitor().await;
        });

        // Spawn Invalidation Bus Listener (optional)
        if let Some(bus) = vfs.invalidation_bus.clone() {
            let vfs_clone = vfs.clone();
            tokio::spawn(async move {
                vfs_clone.background_invalidation_listener(bus).await;
            });
        }

        // Spawn Background Slab Flusher
        // Drains dirty_mem → slab → upload queue asynchronously after flush() returns.
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_slab_flusher(dirty_flush_rx).await;
        });

        Ok(vfs)
    }

    fn now_unix_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
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
        create_opts.if_none_match = Some("*".to_string());
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

    /// Set file attributes (partial update).
    ///
    /// Accepts `SetattrArgs` — any field set to `None`/`Omit` is left unchanged.
    pub fn setattr(&self, ino: Ino, args: SetattrArgs) -> VfsResult<InodeAttr> {
        let mut attr = self.getattr(ino)?;
        let mut changed = false;

        if let Some(new_size) = args.size
            && new_size != attr.size
        {
            if new_size < attr.size {
                // Truncate - may need to remove blocks
                self.truncate_blocks(ino, new_size, attr.size)?;
            }
            attr.size = new_size;
            changed = true;
        }

        if let Some(mode) = args.mode {
            attr.mode = mode;
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

        if changed {
            // Always update ctime when any attribute changes
            attr.touch_ctime();

            // Store in dirty cache
            self.state.dirty_attrs.lock().insert(ino, attr.clone());

            // Invalidate read-through cache since we've changed the attribute
            self.state.attr_read_cache.lock().remove(&ino);

            // Commit to metadata
            self.meta.set_attr(&attr)?;
        }

        Ok(attr)
    }

    /// Truncate blocks beyond new size.
    fn truncate_blocks(&self, ino: Ino, new_size: u64, old_size: u64) -> VfsResult<()> {
        let _block_size = self.block_size as u64;

        // Calculate which blocks to remove
        let new_last_block = if new_size == 0 {
            0
        } else {
            block_index_from_offset(new_size - 1, self.block_size)
        };

        let old_last_block = if old_size == 0 {
            0
        } else {
            block_index_from_offset(old_size - 1, self.block_size)
        };

        // Remove blocks beyond new size
        if new_last_block < old_last_block {
            for block_idx in (new_last_block + 1)..=old_last_block {
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
                }
            }
        }

        // Update last block length if partial
        if new_size > 0 {
            let last_block_offset = offset_within_block(new_size - 1, self.block_size);
            if let Some(mut block_ref) = self.meta.get_file_block(ino, new_last_block)? {
                block_ref.length = last_block_offset + 1;
                block_ref.flags |= BlockFlags::PARTIAL_BLOCK;
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

        // If O_TRUNC requested, truncate to 0
        if trunc {
            self.setattr(ino, SetattrArgs::size_only(0))?;
        }

        let fh = self.handle_table.open(ino, flags);
        debug!("open_file ino={} fh={} flags={:#x}", ino, fh, flags.bits());

        // Eager prefetch: when opening a regular file for reading, schedule
        // background prefetch of the first N blocks.  This hides S3 latency
        // for the common open→sequential-read pattern so the first read()
        // finds data already in cache.
        if attr.kind == studiofs_meta::types::FileType::RegularFile
            && attr.size > 0
            && (flags.contains(OpenFlags::RDONLY) || flags.contains(OpenFlags::RDWR))
        {
            let total_blocks =
                ((attr.size + self.block_size as u64 - 1) / self.block_size as u64) as u32;
            let prefetch_count = total_blocks.min(16); // first 16 blocks = 64 MB
            for block_idx in 0..prefetch_count {
                if self.readahead.start_prefetch(ino, block_idx) {
                    let _ = self.prefetch_tx.send((ino, block_idx));
                }
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
            info!("orphan cleanup: ino={} (last fd closed)", ino);

            self.state.deleted_inodes.lock().insert(ino);

            // Get block sizes before removing for USED_SPACE tracking
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
                self.disk_cache.cleanup_empty_slabs();
            }

            // Decrement USED_SPACE for orphan cleanup
            if total_bytes > 0 {
                self.meta
                    .decr_counter(studiofs_meta::types::counters::USED_SPACE, total_bytes)?;
            }

            self.meta.remove_attr(ino)?;
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
            self.meta.set_attr(&attr)?;
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
                        self.meta.flush_metadata()?;
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
                self.meta.sync_metadata()?;
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
        let attr = self.meta.create_file(parent, name, uid, gid, mode)?;
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.attr_read_cache.lock().remove(&parent);
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
        let attr = self.meta.create_dir(parent, name, uid, gid, mode)?;
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.attr_read_cache.lock().remove(&parent);
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
            info!(
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
                let cache_size_before = self.disk_cache.current_size();
                info!(
                    "unlink ino={}: removed {} block refs, object_ids={:?}, cache_size_before={}",
                    attr.ino,
                    removed_ids.len(),
                    removed_ids,
                    cache_size_before,
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

                // Delete empty slab files so physical disk space is reclaimed.
                let slabs_deleted = self.disk_cache.cleanup_empty_slabs();

                let cache_size_after = self.disk_cache.current_size();
                info!(
                    "unlink ino={}: cache_size_after={}, freed={} bytes, slabs_deleted={}",
                    attr.ino,
                    cache_size_after,
                    cache_size_before.saturating_sub(cache_size_after),
                    slabs_deleted,
                );

                // Clear dirty attrs and read-through cache
                self.state.dirty_attrs.lock().remove(&attr.ino);
            } else {
                info!(
                    "unlink ino={}: nlink={}, keeping inode alive (hardlink)",
                    attr.ino, attr.nlink
                );
            }

            self.state.attr_read_cache.lock().remove(&attr.ino);
        }

        Ok(())
    }

    /// Remove a directory.
    pub fn rmdir(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        self.meta.rmdir(parent, name)?;
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.attr_read_cache.lock().remove(&parent);
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
                self.disk_cache.cleanup_empty_slabs();
            }

            // Decrement USED_SPACE for removed blocks
            if total_bytes > 0 {
                self.meta
                    .decr_counter(studiofs_meta::types::counters::USED_SPACE, total_bytes)?;
            }

            self.state.dirty_attrs.lock().remove(&ino);
            self.state.attr_read_cache.lock().remove(&ino);
        }

        // Invalidate source and dest parent directory caches
        self.state.attr_read_cache.lock().remove(&parent);
        if parent != new_parent {
            self.state.attr_read_cache.lock().remove(&new_parent);
        }
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
        // L1 — in-memory LRU: remove all blocks for this inode in one pass.
        if let Some(mc) = &self.mem_cache {
            mc.invalidate_inode(ino);
        }

        // L1.5 — dirty memory buffer (Phase 5): drain blocks buffered post-flush.
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
                debug!(
                    "evict_inode L1.5: ino={} freed {} bytes from dirty_mem",
                    ino, removed_bytes
                );
            }
        }

        // L2 — slab disk cache: remove by object_id (O(1) HashMap lookup each).
        let mut l2_evicted = 0;
        let mut l2_skipped_pinned = 0;
        for &object_id in object_ids {
            if self.disk_cache.is_pinned(object_id) {
                l2_skipped_pinned += 1;
                continue;
            }
            if self.disk_cache.exists(object_id) {
                self.disk_cache.remove_object_sync(object_id);
                l2_evicted += 1;
            }
        }
        if l2_evicted > 0 || l2_skipped_pinned > 0 {
            debug!(
                "evict_inode L2: ino={} evicted {} objects from slab cache, skipped pinned={}",
                ino, l2_evicted, l2_skipped_pinned
            );
        }

        // Clean up pre-compressed upload data stashed for these objects.
        {
            let mut upload_data = self.state.pending_upload_data.lock();
            for &object_id in object_ids {
                if !self.disk_cache.is_pinned(object_id) {
                    upload_data.remove(&object_id);
                }
            }
        }

        // Cancel any coalesced in-flight S3 fetches for this inode.
        self.state
            .inflight_fetches
            .lock()
            .retain(|(i, _), _| *i != ino);

        // Invalidate readahead state for this inode.
        self.readahead.evict(ino);
    }

    /// Immediately evict a single block from every cache tier.
    ///
    /// Called by `truncate_blocks()` for each block that is removed by a
    /// truncation.
    fn evict_block_from_caches(&self, ino: Ino, block_index: u32, object_id: u64) {
        // L1 — in-memory LRU.
        if let Some(mc) = &self.mem_cache {
            let mem_key = CacheKey::new(ino, block_index as u64 * self.block_size as u64);
            mc.remove(&mem_key);
        }

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
            }
        }

        // L2 — slab disk cache.
        if self.disk_cache.is_pinned(object_id) {
            debug!(
                "evict_block L2: skip pinned object_id={} ino={} block={}",
                object_id, ino, block_index
            );
        } else {
            self.disk_cache.remove_object_sync(object_id);
        }

        // Pending upload payload must stay until upload completion if pinned.
        if !self.disk_cache.is_pinned(object_id) {
            self.state.pending_upload_data.lock().remove(&object_id);
        }

        // Cancel in-flight S3 fetch for this specific block.
        self.state
            .inflight_fetches
            .lock()
            .remove(&(ino, block_index));
    }

    // =========================================================================
    // Read Path
    // =========================================================================

    /// Read file data at the given offset.
    /// Delegates to `read_into()` to avoid code duplication.
    pub async fn read(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        let attr = self.getattr(ino)?;
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
        let attr = self.getattr(ino)?;

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
            return match self.read_block_or_pending(ino, block_idx).await {
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

        // Parallel fetch with concurrency limit.
        let semaphore = Arc::new(Semaphore::new(self.read_concurrency.max(1)));

        let fetch_futures: Vec<_> = blocks_needed
            .iter()
            .map(|(idx, _, _)| {
                let sem = semaphore.clone();
                let block_idx = *idx;
                async move {
                    let _permit = sem.acquire().await.unwrap();
                    self.read_block_or_pending(ino, block_idx).await
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
        if blocks_needed.is_empty() || self.readahead.width == 0 {
            return;
        }
        let first_block = blocks_needed[0].0;
        let last_block = blocks_needed.last().unwrap().0;
        let to_prefetch = self.readahead.record_read(ino, first_block, last_block);
        for block_idx in to_prefetch {
            if self.readahead.start_prefetch(ino, block_idx) {
                // Best-effort; ignore send errors (worker may have exited).
                let _ = self.prefetch_tx.send((ino, block_idx));
            }
        }

        // Directory-level prefetch: if this child is in a tracked directory
        // and the threshold is met, prefetch block 0 of sibling files.
        let siblings = self.dir_prefetch.on_child_read(ino);
        for sibling_ino in siblings.into_iter().take(self.dir_prefetch_max_batch) {
            let _ = self.prefetch_tx.send((sibling_ino, 0));
        }
    }

    /// Read a block from storage or pending writes. Helper for parallel reads.
    async fn read_block_or_pending(&self, ino: Ino, block_index: u32) -> VfsResult<Bytes> {
        // 1. In-progress write buffer (not yet flushed to any tier)
        {
            let pending = self.state.pending_blocks.lock();
            if let Some(pending_block) = pending.get(&(ino, block_index)) {
                return Ok(pending_block.data.clone().freeze());
            }
        }

        // 2. Dirty memory buffer (accepted by flush(), not yet on slab)
        {
            let dirty = self.state.dirty_mem.lock();
            if let Some(data) = dirty.get(&(ino, block_index)) {
                return Ok(data.clone());
            }
        }

        // 3. Slab cache / S3
        self.read_block(ino, block_index).await
    }

    /// Prefetch a block into cache (called from spawned prefetch tasks).
    ///
    /// Like `read_block()` but silently ignores errors and always marks the
    /// readahead inflight state as done when finished.
    async fn read_block_for_prefetch(&self, ino: Ino, block_index: u32) {
        // Skip if already in memory cache (hot L1 check before any I/O)
        let mem_key = CacheKey::new(ino, block_index as u64 * self.block_size as u64);
        if let Some(mc) = &self.mem_cache
            && mc.get(&mem_key).is_some()
        {
            self.readahead.finish_prefetch(ino, block_index);
            return;
        }

        // Fetch from disk cache or S3 (read_block handles all cache levels)
        let _ = self.read_block(ino, block_index).await;
        self.readahead.finish_prefetch(ino, block_index);
    }

    /// Read a single block from storage, with in-flight request coalescing.
    ///
    /// When multiple concurrent reads target the same block (common with OS readahead),
    /// only the first request fetches from S3. Subsequent requests wait for the first
    /// to complete and then read from the now-populated cache.
    async fn read_block(&self, ino: Ino, block_index: u32) -> VfsResult<Bytes> {
        // Get block reference from metadata
        let block_ref = self
            .meta
            .get_file_block(ino, block_index)?
            .ok_or(VfsError::BlockNotFound(ino, block_index))?;

        let mem_key = CacheKey::new(ino, block_index as u64 * self.block_size as u64);

        // L1: Try memory cache first
        if let Some(mc) = &self.mem_cache
            && let Some(cached) = mc.get(&mem_key)
        {
            return Ok(cached);
        }

        // L2: Try disk cache
        if let Some(cached) = self.disk_cache.get_object(block_ref.object_id).await? {
            let result = if self.store_decompressed {
                cached
            } else {
                Bytes::from(read_object(&cached)?)
            };
            if let Some(mc) = &self.mem_cache {
                mc.put(mem_key, result.clone());
            }
            return Ok(result);
        }

        // L3: Fetch from S3 with in-flight coalescing
        loop {
            // Check if another task is already fetching this block
            let maybe_rx = {
                let inflight = self.state.inflight_fetches.lock();
                inflight.get(&(ino, block_index)).map(|tx| tx.subscribe())
            };

            if let Some(mut rx) = maybe_rx {
                // Another task is fetching this block — wait for completion
                let _ = rx.recv().await;

                // Re-check caches (should now be populated)
                if let Some(mc) = &self.mem_cache
                    && let Some(cached) = mc.get(&mem_key)
                {
                    return Ok(cached);
                }
                if let Some(cached) = self.disk_cache.get_object(block_ref.object_id).await? {
                    let result = if self.store_decompressed {
                        cached
                    } else {
                        Bytes::from(read_object(&cached)?)
                    };
                    if let Some(mc) = &self.mem_cache {
                        mc.put(mem_key, result.clone());
                    }
                    return Ok(result);
                }
                // Original fetch failed — loop and try to become the fetcher ourselves
                continue;
            }

            // No in-flight fetch — register ourselves as the fetcher
            let (tx, _) = broadcast::channel::<()>(1);
            self.state
                .inflight_fetches
                .lock()
                .insert((ino, block_index), tx.clone());

            // Cold prefetch: when we hit S3 for the first time, eagerly schedule
            // prefetches for the next N blocks so that subsequent reads don't
            // stall.  This fires before the actual fetch so prefetches run in
            // parallel with this request.
            {
                let cold_ahead = (self.readahead.width as u32).min(16);
                for i in 1..=cold_ahead {
                    if let Some(next_block) = block_index.checked_add(i) {
                        if self.readahead.start_prefetch(ino, next_block) {
                            let _ = self.prefetch_tx.send((ino, next_block));
                        }
                    }
                }
            }

            // Fetch from S3
            let result = async {
                let key = format_object_key(&self.data_prefix, block_ref.object_id);
                let compressed = self.get_remote_object_with_retry(&key).await?;
                let decompressed = Bytes::from(read_object(&compressed)?);

                if self.store_decompressed {
                    self.disk_cache
                        .put_object(block_ref.object_id, &decompressed)
                        .await?;
                } else {
                    self.disk_cache
                        .put_object(block_ref.object_id, &compressed)
                        .await?;
                }

                if let Some(mc) = &self.mem_cache {
                    mc.put(mem_key, decompressed.clone());
                }

                Ok::<Bytes, VfsError>(decompressed)
            }
            .await;

            // Clean up and notify waiters (regardless of success/failure)
            self.state
                .inflight_fetches
                .lock()
                .remove(&(ino, block_index));
            let _ = tx.send(());

            return result;
        }
    }

    async fn get_remote_object_with_retry(&self, key: &str) -> VfsResult<Bytes> {
        let mut attempt: u32 = 0;
        loop {
            match tokio::time::timeout(self.remote_read_timeout, self.data.get_by_key(key)).await {
                Ok(Ok(bytes)) => return Ok(bytes),
                Ok(Err(e)) => {
                    if attempt >= self.remote_read_retries {
                        return Err(VfsError::Data(e));
                    }
                    warn!(
                        attempt = attempt + 1,
                        retries = self.remote_read_retries,
                        key,
                        error = %e,
                        "remote read failed, retrying"
                    );
                }
                Err(_) => {
                    if attempt >= self.remote_read_retries {
                        return Err(VfsError::Other(format!(
                            "timeout while reading remote object key={} after {}ms",
                            key,
                            self.remote_read_timeout.as_millis()
                        )));
                    }
                    warn!(
                        attempt = attempt + 1,
                        retries = self.remote_read_retries,
                        key,
                        timeout_ms = self.remote_read_timeout.as_millis() as u64,
                        "remote read timed out, retrying"
                    );
                }
            }

            attempt = attempt.saturating_add(1);
            let backoff_ms = 150u64.saturating_mul(1u64 << attempt.min(5));
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    // =========================================================================
    // Write Path
    // =========================================================================

    /// Write data to a file at the given offset.
    ///
    /// Full blocks are flushed eagerly (compress + cache + writeback submit) so that
    /// the final `flush()` only handles the last partial block, eliminating the
    /// end-of-write stall where all blocks were previously processed at once.
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        if data.is_empty() {
            return Ok(0);
        }

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

        // Update file size in dirty attrs
        {
            let mut dirty = self.state.dirty_attrs.lock();
            let attr = dirty
                .entry(ino)
                .or_insert_with(|| self.meta.get_attr(ino).unwrap());
            if current_offset > attr.size {
                attr.size = current_offset;
                attr.touch_mtime();
            }
        }

        // Eagerly move full blocks into the dirty memory buffer so write() returns
        // immediately without waiting for compression or slab I/O.
        // The background slab flusher will process them asynchronously.
        if !full_blocks.is_empty() {
            let total: usize = full_blocks.iter().map(|(_, d)| d.len()).sum();
            // Backpressure: stall briefly if the flusher is falling behind.
            while self
                .state
                .dirty_bytes
                .load(Ordering::Relaxed)
                .saturating_add(total)
                > self.max_dirty_bytes
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            {
                let mut dirty = self.state.dirty_mem.lock();
                for (block_index, data) in full_blocks {
                    dirty.insert((ino, block_index), data);
                }
            }
            self.state.dirty_bytes.fetch_add(total, Ordering::Relaxed);
            let _ = self.dirty_flush_tx.send(());
        }

        Ok(written as u32)
    }

    /// Read a block if it exists, returns None if block not found.
    async fn read_block_if_exists(&self, ino: Ino, block_index: u32) -> VfsResult<Option<Bytes>> {
        // Check dirty memory buffer first: blocks live here between flush() and
        // the background slab write, so metadata may not yet list them.
        {
            let dirty = self.state.dirty_mem.lock();
            if let Some(data) = dirty.get(&(ino, block_index)) {
                return Ok(Some(data.clone()));
            }
        }

        // Get block reference from metadata
        let block_ref = match self.meta.get_file_block(ino, block_index)? {
            Some(br) => br,
            None => return Ok(None),
        };

        // Try disk cache first
        if let Some(cached) = self.disk_cache.get_object(block_ref.object_id).await? {
            if self.store_decompressed {
                return Ok(Some(cached));
            } else {
                let decompressed = read_object(&cached)?;
                return Ok(Some(Bytes::from(decompressed)));
            }
        }

        // Fetch from S3
        let key = format_object_key(&self.data_prefix, block_ref.object_id);
        let compressed = self.get_remote_object_with_retry(&key).await?;
        let decompressed = Bytes::from(read_object(&compressed)?);

        if self.store_decompressed {
            self.disk_cache
                .put_object(block_ref.object_id, &decompressed)
                .await?;
        } else {
            self.disk_cache
                .put_object(block_ref.object_id, &compressed)
                .await?;
        }

        Ok(Some(decompressed))
    }

    /// Flush all buffered blocks for a file.
    ///
    /// With eager flushing in `write()`, most full blocks are already flushed.
    /// This handles only the remaining dirty blocks (typically just the last partial block).
    /// Remaining blocks are flushed in parallel for best performance.
    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        // Move all remaining dirty pending blocks into the dirty memory buffer.
        // The background slab flusher handles compress + slab write + upload queue.
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

        if !blocks_to_buffer.is_empty() {
            let total: usize = blocks_to_buffer.iter().map(|(_, d)| d.len()).sum();
            // Backpressure: wait if dirty buffer is saturated.
            while self
                .state
                .dirty_bytes
                .load(Ordering::Relaxed)
                .saturating_add(total)
                > self.max_dirty_bytes
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            {
                let mut dirty = self.state.dirty_mem.lock();
                for (block_index, data) in blocks_to_buffer {
                    dirty.insert((ino, block_index), data);
                }
            }
            self.state.dirty_bytes.fetch_add(total, Ordering::Relaxed);
            let _ = self.dirty_flush_tx.send(());
        }

        // Keep flush non-blocking for interactive UX: move data to dirty buffer
        // and commit attrs; background flusher materializes block refs asynchronously.
        if let Some(attr) = self.state.dirty_attrs.lock().remove(&ino) {
            self.meta.set_attr(&attr)?;
            self.state.attr_read_cache.lock().remove(&ino);
        }

        Ok(())
    }

    /// Flush a single block to storage.
    async fn flush_block(&self, ino: Ino, block_index: u32, data: Bytes) -> VfsResult<()> {
        let length = data.len() as u32;
        let is_partial = length < self.block_size;

        // Allocate block ID
        let object_id = self.meta.next_block_id()?;

        // Create compressed STFS object (needed for S3 upload in all cases)
        let compressed = Bytes::from(create_object(&data)?);

        if self.store_decompressed {
            // Cache the decompressed block — no decompress needed on future cache hits.
            // The compressed version is stashed in pending_upload_data until upload completes.
            self.disk_cache.put_object(object_id, &data).await?;
            self.state
                .pending_upload_data
                .lock()
                .insert(object_id, compressed);
            if let Some(mc) = &self.mem_cache {
                mc.put(
                    CacheKey::new(ino, block_index as u64 * self.block_size as u64),
                    data.clone(),
                );
            }
        } else {
            // Cache the compressed STFS object (traditional path)
            self.disk_cache.put_object(object_id, &compressed).await?;
            if let Some(mc) = &self.mem_cache {
                mc.put(
                    CacheKey::new(ino, block_index as u64 * self.block_size as u64),
                    data.clone(),
                );
            }
        }

        // Submit to writeback queue (synchronous — no disk I/O; see WritebackManager).
        let file_offset = block_index as u64 * self.block_size as u64;
        self.writeback.submit(ino, file_offset, object_id);

        // Create block reference
        let flags = if is_partial {
            BlockFlags::COMPRESSED_LZ4 | BlockFlags::PARTIAL_BLOCK
        } else {
            BlockFlags::COMPRESSED_LZ4
        };

        let block_ref = BlockRef {
            object_id,
            offset: 0,
            length,
            flags,
        };

        // Update metadata (returns old block for GC)
        if let Some(old_block) = self.meta.set_file_block(ino, block_index, &block_ref)? {
            // Mark old block for deletion
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            self.meta
                .add_pending_deletion(old_block.object_id, timestamp)?;
            debug!(
                "marking old block {} for deletion (overwrite)",
                old_block.object_id
            );

            // Update USED_SPACE: subtract old block size, add new block size
            self.meta.decr_counter(
                studiofs_meta::types::counters::USED_SPACE,
                old_block.length as u64,
            )?;
        }
        // Track new block in USED_SPACE counter
        self.meta
            .incr_counter(studiofs_meta::types::counters::USED_SPACE, length as u64)?;

        debug!(
            "flushed block {} for inode {} (object_id={})",
            block_index, ino, object_id
        );
        Ok(())
    }

    // =========================================================================
    // Background Slab Flusher
    // =========================================================================

    /// Woken by `dirty_flush_tx` signals and a periodic timer.
    /// Drains the dirty memory buffer → slab → upload queue.
    async fn background_slab_flusher(
        self: Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        info!("background slab flusher started");
        let mut timer = tokio::time::interval(Duration::from_millis(50));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                msg = rx.recv() => {
                    if msg.is_none() {
                        break; // channel closed
                    }
                }
                _ = timer.tick() => {}
            }
            self.drain_dirty_mem().await;
        }
    }

    /// Process all blocks currently in `dirty_mem`:
    /// compress → slab write → metadata update → upload queue.
    ///
    /// Blocks are kept in `dirty_mem` until the slab write succeeds so that
    /// concurrent reads always find the data in at least one tier.
    async fn drain_dirty_mem(&self) {
        // Snapshot keys without removing — reads remain consistent during processing.
        let keys: Vec<(Ino, u32)> = self.state.dirty_mem.lock().keys().cloned().collect();
        if keys.is_empty() {
            return;
        }

        for (ino, block_index) in keys {
            // ── Race-condition guard ──────────────────────────────────────
            // If unlink() deleted this inode between snapshot and now, skip
            // the block — otherwise flush_block would re-insert it into the
            // slab cache and metadata, undoing the eviction.
            if self.state.deleted_inodes.lock().contains(&ino) {
                let removed = self.state.dirty_mem.lock().remove(&(ino, block_index));
                if let Some(data) = removed {
                    self.state
                        .dirty_bytes
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                            Some(v.saturating_sub(data.len()))
                        })
                        .ok();
                }
                debug!(
                    "slab flusher: skipping deleted inode {} block {}",
                    ino, block_index
                );
                continue;
            }

            // Clone the data while holding a brief lock.
            let data = {
                let dirty = self.state.dirty_mem.lock();
                match dirty.get(&(ino, block_index)) {
                    Some(d) => d.clone(),
                    None => continue, // already processed by a concurrent drain
                }
            };
            let len = data.len();

            // compress → slab → metadata → upload queue
            match self.flush_block(ino, block_index, data).await {
                Ok(()) => {
                    // Success: remove from dirty_mem and release byte count.
                    self.state.dirty_mem.lock().remove(&(ino, block_index));
                    self.state
                        .dirty_bytes
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                            Some(v.saturating_sub(len))
                        })
                        .ok();
                }
                Err(e) => {
                    // Leave in dirty_mem for retry on the next drain cycle.
                    error!(
                        "slab flusher: flush_block failed ino={} block={}: {}",
                        ino, block_index, e
                    );
                }
            }
        }
    }

    // =========================================================================
    // Background Tasks
    // =========================================================================

    /// Background task to process pending uploads.
    async fn background_uploader(
        self: Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<PendingOp>,
    ) {
        info!("background uploader started");

        while let Some(op) = rx.recv().await {
            let vfs = self.clone();
            let retry_op = op.clone();
            tokio::spawn(async move {
                if let Err(e) = vfs.process_upload(op).await {
                    warn!("upload processing error, will requeue: {}", e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    if !vfs.writeback.requeue(retry_op) {
                        error!("failed to requeue upload op after error");
                    }
                }
            });
        }
    }

    async fn background_remote_warmup(self: Arc<Self>) {
        let timeout = self
            .remote_read_timeout
            .saturating_add(Duration::from_secs(2));
        match tokio::time::timeout(timeout, self.data.check()).await {
            Ok(Ok(())) => {
                debug!("remote data path warmup completed");
            }
            Ok(Err(e)) => {
                debug!("remote data path warmup failed: {}", e);
            }
            Err(_) => {
                debug!(
                    timeout_ms = timeout.as_millis() as u64,
                    "remote data path warmup timed out"
                );
            }
        }
    }

    /// Process a single upload operation.
    async fn process_upload(&self, op: PendingOp) -> VfsResult<()> {
        let _permit = self.upload_semaphore.acquire().await.unwrap();

        // Get the compressed STFS object to upload.
        // When store_decompressed=true the disk cache holds raw bytes, so we use
        // the pre-compressed data stashed in pending_upload_data instead.
        // Fallback: re-compress from disk cache (handles restarts / edge-cases).
        let data: Bytes = if self.store_decompressed {
            // First try the pre-compressed stash (zero extra work)
            let stashed = self.state.pending_upload_data.lock().remove(&op.object_id);
            if let Some(compressed) = stashed {
                compressed
            } else {
                // Fallback: the stash was lost (e.g. process restart) — re-compress
                let decompressed =
                    self.disk_cache
                        .get_object(op.object_id)
                        .await?
                        .ok_or_else(|| {
                            VfsError::Other(format!(
                                "object {} missing from disk cache",
                                op.object_id
                            ))
                        })?;
                Bytes::from(create_object(&decompressed)?)
            }
        } else {
            // Disk cache holds compressed STFS object directly
            self.disk_cache
                .get_object(op.object_id)
                .await?
                .ok_or_else(|| {
                    VfsError::Other(format!("object {} missing from disk cache", op.object_id))
                })?
        };

        // Format S3 key
        let key = format_object_key(&self.data_prefix, op.object_id);

        // Check if already exists (optimistic)
        let needs_upload =
            match tokio::time::timeout(self.remote_upload_timeout, self.data.exists(&key)).await {
                Ok(Ok(exists)) => !exists,
                Ok(Err(e)) => {
                    warn!(
                        object_id = op.object_id,
                        key,
                        error = %e,
                        "upload preflight exists() failed, proceeding with upload"
                    );
                    true
                }
                Err(_) => {
                    warn!(
                        object_id = op.object_id,
                        key,
                        timeout_ms = self.remote_upload_timeout.as_millis() as u64,
                        "upload preflight exists() timed out, proceeding with upload"
                    );
                    true
                }
            };

        if needs_upload {
            // Retry loop
            let mut retries = 0;
            loop {
                match tokio::time::timeout(
                    self.remote_upload_timeout,
                    self.data.put_by_key(&key, data.clone()),
                )
                .await
                {
                    Ok(Ok(_)) => break,
                    Ok(Err(e)) => {
                        retries += 1;
                        if retries > self.upload_retries {
                            return Err(VfsError::Data(e));
                        }
                        warn!(
                            "upload retry {} for {} ({}): {}",
                            retries, op.object_id, key, e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(1 << retries)).await;
                    }
                    Err(_) => {
                        retries += 1;
                        if retries > self.upload_retries {
                            return Err(VfsError::Other(format!(
                                "timeout while uploading object {} key={} after {}ms",
                                op.object_id,
                                key,
                                self.remote_upload_timeout.as_millis()
                            )));
                        }
                        warn!(
                            retries,
                            object_id = op.object_id,
                            key,
                            timeout_ms = self.remote_upload_timeout.as_millis() as u64,
                            "upload timed out, retrying"
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(1 << retries)).await;
                    }
                }
            }
        }

        // Mark complete
        self.writeback.complete(&op);
        debug!("background upload complete: {}", op.object_id);
        Ok(())
    }

    /// Background task for garbage collection.
    async fn background_gc(self: Arc<Self>) {
        info!("background GC started");

        let interval = tokio::time::Duration::from_secs(600); // 10 minutes
        let grace_period = 3600i64; // 1 hour grace period

        loop {
            tokio::time::sleep(interval).await;

            // Get current timestamp
            let now = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            // Get pending deletions
            match self.meta.get_pending_deletions() {
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
                        match self.data.delete_by_key(&key).await {
                            Ok(_) => {
                                // Remove from pending deletions
                                if let Err(e) = self.meta.remove_pending_deletion(object_id) {
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
            tokio::time::sleep(self.janitor_interval).await;

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

            let referenced = match self.meta.get_referenced_object_ids() {
                Ok(v) => v,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor cycle failed: get_referenced_object_ids");
                    continue;
                }
            };
            let pending_map: HashMap<u64, i64> = match self.meta.get_pending_deletions() {
                Ok(v) => v.into_iter().collect(),
                Err(e) => {
                    warn!(cycle, error = %e, "janitor: failed to read pending deletions");
                    HashMap::new()
                }
            };

            let entries = match self
                .data
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

                if let Err(e) = self.data.operator().delete(path).await {
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

    fn invalidate_inode_read_caches(&self, ino: Ino) {
        self.state.attr_read_cache.lock().remove(&ino);
        if let Some(mc) = &self.mem_cache {
            mc.invalidate_inode(ino);
        }
    }

    fn invalidate_all_read_caches(&self) {
        self.state.attr_read_cache.lock().clear();
        if let Some(mc) = &self.mem_cache {
            mc.clear();
        }
    }

    async fn background_invalidation_listener(self: Arc<Self>, bus: InvalidationBus) {
        let client: NatsClient = match async_nats::connect(&bus.nats_url).await {
            Ok(c) => c,
            Err(err) => {
                warn!(
                    "invalidation bus connect failed ({}): {}",
                    bus.nats_url, err
                );
                return;
            }
        };

        let subject_pattern = format!("{}.>", bus.subject_base);
        let mut sub = match client.subscribe(subject_pattern.clone()).await {
            Ok(s) => s,
            Err(err) => {
                warn!(
                    "invalidation bus subscribe failed ({}): {}",
                    subject_pattern, err
                );
                return;
            }
        };

        info!(subject_pattern, "invalidation bus listener started");
        while let Some(msg) = sub.next().await {
            let source_id = std::str::from_utf8(&msg.payload)
                .ok()
                .unwrap_or_default()
                .to_string();
            if source_id == bus.sender_id {
                continue;
            }

            let subject = msg.subject.to_string();
            let target = subject.rsplit('.').next().unwrap_or("all");
            if target == "all" {
                self.invalidate_all_read_caches();
                debug!(subject, source_id, "applied global invalidation");
            } else if let Ok(ino) = target.parse::<u64>() {
                self.invalidate_inode_read_caches(ino);
                debug!(subject, source_id, ino, "applied inode invalidation");
            } else {
                self.invalidate_all_read_caches();
                debug!(subject, source_id, "applied fallback global invalidation");
            }
        }
    }

    /// Persist the disk cache index to disk.
    ///
    /// Should be called before unmounting so the slab index survives across
    /// remounts. The `Drop` impl on `SlabStore` also persists on exit, but
    /// calling this explicitly gives a clean, logged persist at a known point.
    pub fn persist_cache(&self) {
        if let Err(e) = self.disk_cache.persist() {
            error!("failed to persist disk cache on shutdown: {}", e);
        } else {
            tracing::info!("disk cache index persisted");
        }
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
        let pending_ops = self.writeback.snapshot_pending_ops();
        if pending_ops.is_empty() {
            return 0;
        }

        let mut reconciled = 0usize;
        for op in pending_ops {
            let key = format_object_key(&self.data_prefix, op.object_id);
            match self.data.exists(&key).await {
                Ok(true) => {
                    self.writeback.complete(&op);
                    reconciled += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    debug!(
                        object_id = op.object_id,
                        error = %e,
                        "VFS shutdown: remote reconcile exists() failed"
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

    /// Best-effort graceful shutdown: drain dirty buffers, flush journal writes,
    /// wait for uploader queue to quiesce, then persist cache metadata.
    pub async fn shutdown(&self, upload_drain_timeout: Duration) {
        info!("VFS shutdown: flushing pending write buffer");
        self.flush_pending_blocks_for_shutdown().await;

        info!("VFS shutdown: draining dirty memory buffer");
        self.drain_dirty_mem().await;

        info!("VFS shutdown: flushing journal writer queue");
        self.writeback.flush_journal().await;

        if self.writeback.pending_count() > 0 {
            let reconciled = self.reconcile_remote_completed_pending().await;
            if reconciled > 0 {
                self.writeback.flush_journal().await;
            }
        }

        let mut drained = self.writeback.wait_for_idle(upload_drain_timeout).await;
        if !drained {
            let reconciled = self.reconcile_remote_completed_pending().await;
            if reconciled > 0 {
                self.writeback.flush_journal().await;
                drained = self
                    .writeback
                    .wait_for_idle(Duration::from_millis(250))
                    .await;
            }
        }

        if !drained {
            let pending_ops = self.writeback.snapshot_pending_ops();
            let sample_object_ids: Vec<u64> =
                pending_ops.iter().take(8).map(|op| op.object_id).collect();
            warn!(
                pending = self.writeback.pending_count(),
                pending_journal_files = pending_ops.len(),
                pending_object_ids_sample = ?sample_object_ids,
                timeout_ms = upload_drain_timeout.as_millis() as u64,
                "VFS shutdown: upload queue did not fully drain before timeout"
            );
        }

        // Flush completion journal removals enqueued during the final upload drain.
        self.writeback.flush_journal().await;
        if let Err(e) = self.meta.flush_metadata() {
            warn!("VFS shutdown: metadata flush failed: {}", e);
        }
        self.persist_cache();
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_index_calculation() {
        let block_size = 4 * 1024 * 1024; // 4MB

        assert_eq!(block_index_from_offset(0, block_size), 0);
        assert_eq!(block_index_from_offset(1, block_size), 0);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024, block_size), 1);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024 + 1, block_size), 1);
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
}

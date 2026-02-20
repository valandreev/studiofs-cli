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

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use studiofs_cache::disk::{DiskCache, DEFAULT_DISK_CACHE_SIZE};
use studiofs_cache::memory::{MemoryCache, CacheKey};
use studiofs_cache::writeback::{WritebackManager, PendingOp};
use studiofs_data::object::{create_object, read_object, format_object_key};
use studiofs_data::store::DataStore;
use studiofs_meta::*;
use studiofs_meta::types::{BlockRef, BlockFlags, block_index_from_offset, offset_within_block};
use thiserror::Error;
use tracing::{debug, info, error, warn};
use parking_lot::Mutex;
use tokio::sync::{broadcast, Semaphore};
use crate::readahead::{DirAccessTracker, ReadAheadTracker};

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
        return Err(VfsError::InvalidName(
            "name is '.' or '..'".into(),
        ));
    }
    Ok(())
}

// =============================================================================
// Constants & Configuration
// =============================================================================

/// Attribute cache TTL — read-through attributes are valid for 1 second.
const ATTR_CACHE_TTL: Duration = Duration::from_secs(1);

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
    /// detected.  0 disables read-ahead entirely.  Default: 8 (= 32 MB with
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
            readahead_width: 8,
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
    pub const RDONLY: Self  = Self(0x01);
    pub const WRONLY: Self  = Self(0x02);
    pub const RDWR: Self    = Self(0x04);
    pub const APPEND: Self  = Self(0x08);

    pub const fn empty() -> Self { Self(0) }

    pub const fn bits(self) -> u32 { self.0 }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self { Self(self.0 | rhs.0) }
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

// =============================================================================
// StudioVfs Implementation
// =============================================================================

/// The core VFS - satisfies filesystem operations using metadata + data + cache.
pub struct StudioVfs {
    /// Metadata engine.
    meta: Arc<RedbMetaEngine>,
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
    /// Volume UUID for S3 key prefix.
    volume_uuid: String,
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
}

impl StudioVfs {
    /// Create a new VFS instance.
    pub fn new(
        meta: Arc<RedbMetaEngine>,
        data: Arc<DataStore>,
        mem_cache: Option<Arc<MemoryCache>>,
        cache_dir: &str,
        cache_size: u64,
    ) -> VfsResult<Arc<Self>> {
        // Get volume info
        let format_info = meta.get_format_info()?;
        let volume_uuid = format_info.uuid.clone();
        let block_size = format_info.block_size;

        // Init Disk Cache
        let disk_cache = DiskCache::new(cache_dir, cache_size)?;

        // Init Writeback Manager
        let (writeback, upload_rx) = WritebackManager::new(cache_dir, disk_cache.clone());

        // Init ReadAhead tracker + prefetch channel
        let readahead = Arc::new(ReadAheadTracker::new(8));
        let dir_prefetch = Arc::new(DirAccessTracker::new(2));
        let (prefetch_tx, mut prefetch_rx) =
            tokio::sync::mpsc::unbounded_channel::<(Ino, u32)>();

        // Dirty-write memory buffer + background slab flusher channel.
        let (dirty_flush_tx, dirty_flush_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let vfs = Arc::new(Self {
            meta,
            data,
            disk_cache,
            writeback,
            mem_cache,
            block_size,
            volume_uuid,
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
        });

        // Spawn prefetch worker — holds Arc<Self> so it can call read_block()
        let vfs_for_prefetch = vfs.clone();
        tokio::spawn(async move {
            while let Some((ino, block_idx)) = prefetch_rx.recv().await {
                let v = vfs_for_prefetch.clone();
                tokio::spawn(async move {
                    let _ = v.read_block_for_prefetch(ino, block_idx).await;
                });
            }
        });

        // Spawn Upload Worker
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_uploader(upload_rx).await;
        });

        // Spawn GC Worker
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_gc().await;
        });

        // Spawn Background Slab Flusher
        // Drains dirty_mem → slab → upload queue asynchronously after flush() returns.
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_slab_flusher(dirty_flush_rx).await;
        });

        Ok(vfs)
    }

    /// Get metadata engine reference.
    pub fn meta(&self) -> &RedbMetaEngine {
        &self.meta
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
                && timestamp.elapsed() < ATTR_CACHE_TTL {
                    return Ok(attr.clone());
                }
            // Drop lock before potentially slow redb read
        }

        // 3. Fetch from redb and populate read-through cache
        let attr = self.meta.get_attr(ino)?;
        self.state.attr_read_cache.lock().insert(ino, (attr.clone(), Instant::now()));
        Ok(attr)
    }

    /// Set file attributes (partial update).
    ///
    /// Accepts `SetattrArgs` — any field set to `None`/`Omit` is left unchanged.
    pub fn setattr(&self, ino: Ino, args: SetattrArgs) -> VfsResult<InodeAttr> {
        let mut attr = self.getattr(ino)?;
        let mut changed = false;

        if let Some(new_size) = args.size
            && new_size != attr.size {
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
        let _attr = self.getattr(ino)?;

        // If O_TRUNC requested, truncate to 0
        if trunc {
            self.setattr(ino, SetattrArgs::size_only(0))?;
        }

        let fh = self.handle_table.open(ino, flags);
        debug!("open_file ino={} fh={} flags={:#x}", ino, fh, flags.bits());
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
                self.state.pending_blocks.lock().retain(|(i, _), _| *i != ino);
                self.evict_inode_from_caches(ino, &removed_ids);
                self.disk_cache.cleanup_empty_slabs();
            }

            // Decrement USED_SPACE for orphan cleanup
            if total_bytes > 0 {
                self.meta.decr_counter(
                    studiofs_meta::types::counters::USED_SPACE,
                    total_bytes,
                )?;
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
        if !datasync
            && let Some(attr) = self.state.dirty_attrs.lock().remove(&ino) {
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
            {
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
                                entry.shared_count += 1;
                                return Ok(());
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                // Downgrade exclusive → shared
                                entry.kind = LockKind::Shared;
                                entry.shared_count = 1;
                                return Ok(());
                            }
                            LockKind::Exclusive => {
                                // Blocked by another owner's exclusive lock
                            }
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
                                return Ok(());
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                // Already held by us — re-entrant
                                return Ok(());
                            }
                            _ => {
                                // Blocked by another owner
                            }
                        }
                    }
                    LockKind::Unlock => {
                        if let Some(entry) = locks.get_mut(&ino) {
                            match entry.kind {
                                LockKind::Shared => {
                                    entry.shared_count = entry.shared_count.saturating_sub(1);
                                    if entry.shared_count == 0 {
                                        locks.remove(&ino);
                                    }
                                }
                                LockKind::Exclusive if entry.owner == owner => {
                                    locks.remove(&ino);
                                }
                                _ => {}
                            }
                        }
                        self.state.lock_notify.notify_waiters();
                        return Ok(());
                    }
                }
            }
            // If we get here, the lock could not be acquired
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
                    self.meta.decr_counter(
                        studiofs_meta::types::counters::USED_SPACE,
                        attr.size,
                    )?;
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
        let displaced_ino = self.meta.rename_atomic(parent, name, new_parent, new_name)?;

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
                self.state.pending_blocks.lock().retain(|(i, _), _| *i != ino);
                self.evict_inode_from_caches(ino, &removed_ids);
                self.disk_cache.cleanup_empty_slabs();
            }

            // Decrement USED_SPACE for removed blocks
            if total_bytes > 0 {
                self.meta.decr_counter(
                    studiofs_meta::types::counters::USED_SPACE,
                    total_bytes,
                )?;
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
                debug!("evict_inode L1.5: ino={} freed {} bytes from dirty_mem", ino, removed_bytes);
            }
        }

        // L2 — slab disk cache: remove by object_id (O(1) HashMap lookup each).
        let mut l2_evicted = 0;
        for &object_id in object_ids {
            if self.disk_cache.exists(object_id) {
                self.disk_cache.remove_object_sync(object_id);
                l2_evicted += 1;
            }
        }
        if l2_evicted > 0 {
            debug!("evict_inode L2: ino={} evicted {} objects from slab cache", ino, l2_evicted);
        }

        // Clean up pre-compressed upload data stashed for these objects.
        {
            let mut upload_data = self.state.pending_upload_data.lock();
            for &object_id in object_ids {
                upload_data.remove(&object_id);
            }
        }

        // Cancel any coalesced in-flight S3 fetches for this inode.
        self.state.inflight_fetches.lock().retain(|(i, _), _| *i != ino);

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
        self.disk_cache.remove_object_sync(object_id);

        // Cancel in-flight S3 fetch for this specific block.
        self.state.inflight_fetches.lock().remove(&(ino, block_index));
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
            let block_data = self.read_block_or_pending(ino, block_idx).await?;
            let copy_len = if to_copy == 0 || off_in >= block_data.len() {
                0
            } else {
                to_copy.min(block_data.len() - off_in)
            };
            if copy_len > 0 {
                buffer[..copy_len].copy_from_slice(&block_data[off_in..off_in + copy_len]);
            }
            self.maybe_prefetch(ino, &blocks_needed);
            return Ok(copy_len as u32);
        }

        // Parallel fetch with concurrency limit (16 concurrent reads)
        const CONCURRENT_LIMIT: usize = 16;
        let semaphore = Arc::new(Semaphore::new(CONCURRENT_LIMIT));

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
        for (i, block_result) in fetched.iter().enumerate() {
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
                Err(_) => {
                    // Missing block - fill with zeros
                    let zeros_len = to_copy.min(buffer.len() - written);
                    buffer[written..written + zeros_len].fill(0u8);
                    written += zeros_len;
                }
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
        for sibling_ino in siblings {
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
            && mc.get(&mem_key).is_some() {
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
        let block_ref = self.meta.get_file_block(ino, block_index)?
            .ok_or(VfsError::BlockNotFound(ino, block_index))?;

        let mem_key = CacheKey::new(ino, block_index as u64 * self.block_size as u64);

        // L1: Try memory cache first
        if let Some(mc) = &self.mem_cache
            && let Some(cached) = mc.get(&mem_key) {
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
                    && let Some(cached) = mc.get(&mem_key) {
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
            self.state.inflight_fetches.lock().insert((ino, block_index), tx.clone());

            // Fetch from S3
            let result = async {
                let key = format_object_key(&self.volume_uuid, block_ref.object_id);
                let compressed = self.data.get_object(&key).await?;
                let decompressed = Bytes::from(read_object(&compressed)?);

                if self.store_decompressed {
                    self.disk_cache.put_object(block_ref.object_id, &decompressed).await?;
                } else {
                    self.disk_cache.put_object(block_ref.object_id, &compressed).await?;
                }

                if let Some(mc) = &self.mem_cache {
                    mc.put(mem_key, decompressed.clone());
                }

                Ok::<Bytes, VfsError>(decompressed)
            }
            .await;

            // Clean up and notify waiters (regardless of success/failure)
            self.state.inflight_fetches.lock().remove(&(ino, block_index));
            let _ = tx.send(());

            return result;
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
            let need_load = !self.state.pending_blocks.lock().contains_key(&(ino, block_index));
            if need_load {
                let existing_data = match self.read_block_if_exists(ino, block_index).await {
                    Ok(Some(d)) => d,
                    _ => Bytes::new(),
                };
                let mut pending = self.state.pending_blocks.lock();
                // Double-check: another write call might have inserted while we loaded
                pending.entry((ino, block_index)).or_insert_with(|| PendingBlock {
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
                pending_block.data[offset_in_block..end_pos].copy_from_slice(
                    &data[data_offset..data_offset + to_write]
                );
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
            let attr = dirty.entry(ino).or_insert_with(|| self.meta.get_attr(ino).unwrap());
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
            while self.state.dirty_bytes.load(Ordering::Relaxed).saturating_add(total)
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
        let key = format_object_key(&self.volume_uuid, block_ref.object_id);
        let compressed = self.data.get_object(&key).await?;
        let decompressed = Bytes::from(read_object(&compressed)?);

        if self.store_decompressed {
            self.disk_cache.put_object(block_ref.object_id, &decompressed).await?;
        } else {
            self.disk_cache.put_object(block_ref.object_id, &compressed).await?;
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
            let keys: Vec<_> = pending.keys()
                .filter(|(i, _)| *i == ino)
                .cloned()
                .collect();

            let mut blocks = Vec::new();
            for key in keys {
                if let Some(pb) = pending.remove(&key)
                    && pb.dirty {
                        blocks.push((key.1, pb.data.freeze()));
                    }
            }
            blocks
        };

        if !blocks_to_buffer.is_empty() {
            let total: usize = blocks_to_buffer.iter().map(|(_, d)| d.len()).sum();
            // Backpressure: wait if dirty buffer is saturated.
            while self.state.dirty_bytes.load(Ordering::Relaxed).saturating_add(total)
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

        // Commit dirty attributes synchronously — file size and mtime must be
        // visible immediately after flush() returns.
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
            self.state.pending_upload_data.lock().insert(object_id, compressed);
        } else {
            // Cache the compressed STFS object (traditional path)
            self.disk_cache.put_object(object_id, &compressed).await?;
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
            self.meta.add_pending_deletion(old_block.object_id, timestamp)?;
            debug!("marking old block {} for deletion (overwrite)", old_block.object_id);

            // Update USED_SPACE: subtract old block size, add new block size
            self.meta.decr_counter(studiofs_meta::types::counters::USED_SPACE, old_block.length as u64)?;
        }
        // Track new block in USED_SPACE counter
        self.meta.incr_counter(studiofs_meta::types::counters::USED_SPACE, length as u64)?;

        debug!("flushed block {} for inode {} (object_id={})", block_index, ino, object_id);
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
    async fn background_uploader(self: Arc<Self>, mut rx: tokio::sync::mpsc::UnboundedReceiver<PendingOp>) {
        info!("background uploader started");

        while let Some(op) = rx.recv().await {
            let vfs = self.clone();
            tokio::spawn(async move {
                if let Err(e) = vfs.process_upload(op).await {
                    error!("upload processing error: {}", e);
                }
            });
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
                let decompressed = self.disk_cache.get_object(op.object_id).await?
                    .ok_or_else(|| VfsError::Other(format!("object {} missing from disk cache", op.object_id)))?;
                Bytes::from(create_object(&decompressed)?)
            }
        } else {
            // Disk cache holds compressed STFS object directly
            self.disk_cache.get_object(op.object_id).await?
                .ok_or_else(|| VfsError::Other(format!("object {} missing from disk cache", op.object_id)))?
        };

        // Format S3 key
        let key = format_object_key(&self.volume_uuid, op.object_id);

        // Check if already exists (optimistic)
        let needs_upload = match self.data.exists(&key).await {
            Ok(exists) => !exists,
            Err(_) => true,
        };

        if needs_upload {
            // Retry loop
            let mut retries = 0;
            loop {
                match self.data.put_object(&key, data.clone()).await {
                    Ok(_) => break,
                    Err(e) => {
                        retries += 1;
                        if retries > self.upload_retries {
                            return Err(VfsError::Data(e));
                        }
                        warn!("upload retry {} for {}: {}", retries, op.object_id, e);
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
                        let key = format_object_key(&self.volume_uuid, object_id);
                        match self.data.delete_object(&key).await {
                            Ok(_) => {
                                // Remove from pending deletions
                                if let Err(e) = self.meta.remove_pending_deletion(object_id) {
                                    warn!("failed to remove pending deletion for {}: {}", object_id, e);
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
        assert!(matches!(
            validate_name(b""),
            Err(VfsError::InvalidName(_))
        ));
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
        assert!(matches!(
            validate_name(b"."),
            Err(VfsError::InvalidName(_))
        ));
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
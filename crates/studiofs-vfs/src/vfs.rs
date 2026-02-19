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
use std::sync::Arc;
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

    #[error("{0}")]
    Other(String),
}

pub type VfsResult<T> = Result<T, VfsError>;

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
        }
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
            }),
            upload_semaphore: Arc::new(Semaphore::new(32)),
            upload_retries: 3,
            store_decompressed: true,
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
            if let Some((attr, timestamp)) = cache.get(&ino) {
                if timestamp.elapsed() < ATTR_CACHE_TTL {
                    return Ok(attr.clone());
                }
            }
            // Drop lock before potentially slow redb read
        }

        // 3. Fetch from redb and populate read-through cache
        let attr = self.meta.get_attr(ino)?;
        self.state.attr_read_cache.lock().insert(ino, (attr.clone(), Instant::now()));
        Ok(attr)
    }

    /// Set file attributes (partial update).
    pub fn setattr(&self, ino: Ino, size: Option<u64>) -> VfsResult<InodeAttr> {
        let mut attr = self.getattr(ino)?;

        if let Some(new_size) = size {
            // Truncate or extend
            if new_size < attr.size {
                // Truncate - may need to remove blocks
                self.truncate_blocks(ino, new_size, attr.size)?;
            }
            attr.size = new_size;
        }

        attr.touch_mtime();

        // Store in dirty cache
        self.state.dirty_attrs.lock().insert(ino, attr.clone());

        // Invalidate read-through cache since we've changed the attribute
        self.state.attr_read_cache.lock().remove(&ino);

        // Commit to metadata
        self.meta.set_attr(&attr)?;
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
                    // Mark old block for deletion
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    self.meta.add_pending_deletion(object_id, timestamp)?;
                    debug!("marking block {} for deletion (truncate)", object_id);
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
    #[allow(deprecated)]
    pub fn readdir(&self, ino: Ino) -> VfsResult<Vec<DirEntry>> {
        Ok(self.meta.readdir(ino)?)
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
        let attr = self.meta.create_dir(parent, name, uid, gid, mode)?;
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.attr_read_cache.lock().remove(&parent);
        Ok(attr)
    }

    /// Remove a file.
    pub fn unlink(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        let attr = self.meta.unlink(parent, name)?;

        // Remove all blocks and mark for GC
        let removed_ids = self.meta.remove_file_blocks(attr.ino)?;

        // Add to pending deletions
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        for object_id in removed_ids {
            self.meta.add_pending_deletion(object_id, timestamp)?;
            debug!("marking object {} for deletion (unlink)", object_id);
        }

        // Clear pending blocks buffer
        self.state.pending_blocks.lock().retain(|(ino, _), _| *ino != attr.ino);

        // Clear dirty attrs and read-through cache
        self.state.dirty_attrs.lock().remove(&attr.ino);
        self.state.attr_read_cache.lock().remove(&attr.ino);

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
    pub fn rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> VfsResult<()> {
        self.meta.rename(parent, name, new_parent, new_name)?;
        // Invalidate both source and dest parent directory caches (entries changed)
        self.state.attr_read_cache.lock().remove(&parent);
        if parent != new_parent {
            self.state.attr_read_cache.lock().remove(&new_parent);
        }
        Ok(())
    }

    // =========================================================================
    // Read Path
    // =========================================================================

    /// Read file data at the given offset.
    /// Uses parallel block fetching (up to 16 concurrent blocks) for improved throughput.
    pub async fn read(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        let attr = self.getattr(ino)?;

        // Clamp read to file size
        let end = (offset + size as u64).min(attr.size);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let effective_size = (end - offset) as usize;

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
            let slice = if to_copy == 0 {
                vec![0u8; to_copy]
            } else {
                block_data[off_in..off_in + to_copy].to_vec()
            };
            return Ok(Bytes::from(slice));
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

        // Assemble result
        let mut result = BytesMut::with_capacity(effective_size);
        for (i, block_result) in fetched.iter().enumerate() {
            let (_block_idx, off_in, to_copy) = blocks_needed[i];
            match block_result {
                Ok(block_data) => {
                    let slice = if to_copy == 0 || off_in >= block_data.len() {
                        vec![0u8; to_copy]
                    } else {
                        let actual_copy = to_copy.min(block_data.len() - off_in);
                        block_data[off_in..off_in + actual_copy].to_vec()
                    };
                    result.extend_from_slice(&slice);
                }
                Err(_) => {
                    // Missing block - fill with zeros
                    result.extend(std::iter::repeat(0u8).take(to_copy));
                }
            }
        }

        Ok(result.freeze())
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

        Ok(written as u32)
    }

    /// Read a block from storage or pending writes. Helper for parallel reads.
    async fn read_block_or_pending(&self, ino: Ino, block_index: u32) -> VfsResult<Bytes> {
        // Check pending blocks first (writes not yet flushed)
        {
            let pending = self.state.pending_blocks.lock();
            if let Some(pending_block) = pending.get(&(ino, block_index)) {
                return Ok(pending_block.data.clone().freeze());
            }
        }

        // Read from storage
        self.read_block(ino, block_index).await
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
        if let Some(mc) = &self.mem_cache {
            if let Some(cached) = mc.get(&mem_key) {
                return Ok(cached);
            }
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
                if let Some(mc) = &self.mem_cache {
                    if let Some(cached) = mc.get(&mem_key) {
                        return Ok(cached);
                    }
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
                if !pending.contains_key(&(ino, block_index)) {
                    pending.insert((ino, block_index), PendingBlock {
                        data: BytesMut::from(existing_data.as_ref()),
                        dirty: false,
                    });
                }
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

        // Eagerly flush full blocks in parallel (compress + cache + submit to writeback).
        // This spreads the work across the entire write duration instead of
        // doing it all at once in the final flush() call.
        if !full_blocks.is_empty() {
            let flush_futs: Vec<_> = full_blocks
                .into_iter()
                .map(|(block_index, block_data)| self.flush_block(ino, block_index, block_data))
                .collect();
            let results = futures::future::join_all(flush_futs).await;
            for r in results {
                r?;
            }
        }

        Ok(written as u32)
    }
    
    /// Read a block if it exists, returns None if block not found.
    async fn read_block_if_exists(&self, ino: Ino, block_index: u32) -> VfsResult<Option<Bytes>> {
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
        // Extract all pending blocks for this inode
        let blocks_to_flush: Vec<(u32, Bytes)> = {
            let mut pending = self.state.pending_blocks.lock();
            let keys: Vec<_> = pending.keys()
                .filter(|(i, _)| *i == ino)
                .cloned()
                .collect();

            let mut blocks = Vec::new();
            for key in keys {
                if let Some(pb) = pending.remove(&key) {
                    if pb.dirty {
                        blocks.push((key.1, pb.data.freeze()));
                    }
                }
            }
            blocks
        };

        // Flush remaining blocks in parallel
        if !blocks_to_flush.is_empty() {
            let flush_futs: Vec<_> = blocks_to_flush
                .into_iter()
                .map(|(block_index, data)| self.flush_block(ino, block_index, data))
                .collect();
            let results = futures::future::join_all(flush_futs).await;
            for r in results {
                r?;
            }
        }

        // Commit dirty attributes
        if let Some(attr) = self.state.dirty_attrs.lock().remove(&ino) {
            self.meta.set_attr(&attr)?;
            // Invalidate read-through cache since attributes changed (mtime updated)
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

        // Submit to writeback queue
        let file_offset = block_index as u64 * self.block_size as u64;
        self.writeback.submit(ino, file_offset, object_id).await;

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
        }

        debug!("flushed block {} for inode {} (object_id={})", block_index, ino, object_id);
        Ok(())
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
}
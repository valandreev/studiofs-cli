//! Core VFS implementation v2 - Block-based architecture.
//!
//! Architecture v2 Key Changes:
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
use studiofs_cache::disk::{DiskCache, DEFAULT_DISK_CACHE_SIZE};
use studiofs_cache::writeback::{WritebackManager, PendingOp};
use studiofs_data::object::{create_object, read_object, format_object_key};
use studiofs_data::store::DataStore;
use studiofs_meta::*;
use studiofs_meta::types::{BlockRef, BlockFlags, block_index_from_offset, offset_within_block};
use thiserror::Error;
use tracing::{debug, info, error, warn};
use parking_lot::Mutex;
use tokio::sync::Semaphore;

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
    /// Dirty attributes cache.
    dirty_attrs: Mutex<std::collections::HashMap<Ino, InodeAttr>>,
}

// =============================================================================
// StudioVfs V2 Implementation
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
}

impl StudioVfs {
    /// Create a new VFS instance.
    pub fn new(
        meta: Arc<RedbMetaEngine>,
        data: Arc<DataStore>,
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
            block_size,
            volume_uuid,
            state: Arc::new(VfsState {
                pending_blocks: Mutex::new(std::collections::HashMap::new()),
                dirty_attrs: Mutex::new(std::collections::HashMap::new()),
            }),
            upload_semaphore: Arc::new(Semaphore::new(32)),
            upload_retries: 3,
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
        // Check dirty attrs cache first
        if let Some(attr) = self.state.dirty_attrs.lock().get(&ino) {
            return Ok(attr.clone());
        }
        Ok(self.meta.get_attr(ino)?)
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
        Ok(self.meta.create_file(parent, name, uid, gid, mode)?)
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
        Ok(self.meta.create_dir(parent, name, uid, gid, mode)?)
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

        // Clear dirty attrs
        self.state.dirty_attrs.lock().remove(&attr.ino);

        Ok(())
    }

    /// Remove a directory.
    pub fn rmdir(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        Ok(self.meta.rmdir(parent, name)?)
    }

    /// Rename a file or directory.
    pub fn rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> VfsResult<()> {
        Ok(self.meta.rename(parent, name, new_parent, new_name)?)
    }

    // =========================================================================
    // Read Path
    // =========================================================================

    /// Read file data at the given offset.
    pub async fn read(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        let attr = self.getattr(ino)?;

        // Clamp read to file size
        let end = (offset + size as u64).min(attr.size);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let effective_size = (end - offset) as usize;

        let mut result = BytesMut::with_capacity(effective_size);
        let mut current_offset = offset;
        let mut remaining = effective_size;

        while remaining > 0 {
            let block_index = block_index_from_offset(current_offset, self.block_size);
            let offset_in_block = offset_within_block(current_offset, self.block_size) as usize;

            // Check pending blocks first (writes not yet flushed)
            let block_data = {
                let pending = self.state.pending_blocks.lock();
                if let Some(pending_block) = pending.get(&(ino, block_index)) {
                    // Use pending data
                    pending_block.data.clone().freeze()
                } else {
                    // Read from storage
                    drop(pending);
                    self.read_block(ino, block_index).await?
                }
            };

            // Copy slice
            let to_copy = remaining.min(block_data.len().saturating_sub(offset_in_block));
            if to_copy == 0 {
                // Hole or missing block - fill with zeros
                let zeros = remaining.min(self.block_size as usize - offset_in_block);
                result.extend(std::iter::repeat(0u8).take(zeros));
                current_offset += zeros as u64;
                remaining -= zeros;
            } else {
                result.extend_from_slice(&block_data[offset_in_block..offset_in_block + to_copy]);
                current_offset += to_copy as u64;
                remaining -= to_copy;
            }
        }

        Ok(result.freeze())
    }

    /// Read a single block from storage.
    async fn read_block(&self, ino: Ino, block_index: u32) -> VfsResult<Bytes> {
        // Get block reference from metadata
        let block_ref = self.meta.get_file_block(ino, block_index)?
            .ok_or(VfsError::BlockNotFound(ino, block_index))?;

        // Try disk cache first
        if let Some(data) = self.disk_cache.get_object(block_ref.object_id).await? {
            // Decompress using read_object
            let decompressed = read_object(&data)?;
            return Ok(Bytes::from(decompressed));
        }

        // Fetch from S3
        let key = format_object_key(&self.volume_uuid, block_ref.object_id);
        let data = self.data.get_object(&key).await?;

        // Store in disk cache
        self.disk_cache.put_object(block_ref.object_id, &data).await?;

        // Decompress using read_object
        let decompressed = read_object(&data)?;
        Ok(Bytes::from(decompressed))
    }

    // =========================================================================
    // Write Path
    // =========================================================================

    /// Write data to a file at the given offset.
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        if data.is_empty() {
            return Ok(0);
        }

        let mut written = 0u64;
        let mut remaining = data.len();
        let mut current_offset = offset;
        let mut data_offset = 0;

        while remaining > 0 {
            let block_index = block_index_from_offset(current_offset, self.block_size);
            let offset_in_block = offset_within_block(current_offset, self.block_size) as usize;
            let space_in_block = self.block_size as usize - offset_in_block;
            let to_write = remaining.min(space_in_block);

            // Get or create pending block
            {
                let mut pending = self.state.pending_blocks.lock();
                
                // Check if block already in pending cache
                if !pending.contains_key(&(ino, block_index)) {
                    // Try to read existing block data from storage
                    let existing_data = match self.read_block_if_exists(ino, block_index).await {
                        Ok(Some(data)) => data,
                        _ => Bytes::new(),
                    };
                    
                    pending.insert((ino, block_index), PendingBlock {
                        data: BytesMut::from(existing_data.as_ref()),
                        dirty: false,
                    });
                }
                
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
        if let Some(data) = self.disk_cache.get_object(block_ref.object_id).await? {
            // Decompress using read_object
            let decompressed = read_object(&data)?;
            return Ok(Some(Bytes::from(decompressed)));
        }

        // Fetch from S3
        let key = format_object_key(&self.volume_uuid, block_ref.object_id);
        let data = self.data.get_object(&key).await?;

        // Store in disk cache
        self.disk_cache.put_object(block_ref.object_id, &data).await?;

        // Decompress using read_object
        let decompressed = read_object(&data)?;
        Ok(Some(Bytes::from(decompressed)))
    }

    /// Flush all buffered blocks for a file.
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

        // Flush each block
        for (block_index, data) in blocks_to_flush {
            self.flush_block(ino, block_index, data).await?;
        }

        // Commit dirty attributes
        if let Some(attr) = self.state.dirty_attrs.lock().remove(&ino) {
            self.meta.set_attr(&attr)?;
        }

        Ok(())
    }

    /// Flush a single block to storage.
    async fn flush_block(&self, ino: Ino, block_index: u32, data: Bytes) -> VfsResult<()> {
        let length = data.len() as u32;
        let is_partial = length < self.block_size;

        // Allocate block ID
        let object_id = self.meta.next_block_id()?;

        // Create object with header (includes compression)
        let object_data = create_object(&data)?;

        // Write to disk cache
        self.disk_cache.put_object(object_id, &object_data).await?;

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
        info!("background uploader started (v2)");

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

        // Read from disk cache
        let data = self.disk_cache.get_object(op.object_id).await?
            .ok_or_else(|| VfsError::Other(format!("object {} missing from disk cache", op.object_id)))?;

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
        info!("background GC started (v2)");

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
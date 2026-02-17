//! Core VFS implementation — wires together metadata, data, and cache layers.

use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;
use studiofs_cache::memory::{CacheKey, MemoryCache};
use studiofs_cache::disk::DiskCache;
use studiofs_data::store::DataStore;
use studiofs_data::object::{
    self, create_object, read_object, hash_object, ObjectPayload, ObjectKind, Objectlink,
};
use studiofs_meta::*;
use thiserror::Error;
use tracing::{debug, info};

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

    #[error("{0}")]
    Other(String),
}

pub type VfsResult<T> = Result<T, VfsError>;

/// VFS configuration.
#[derive(Debug, Clone)]
pub struct VfsConfig {
    /// Path to metadata database.
    pub meta_path: String,
    /// Path to disk cache directory.
    pub cache_dir: String,
    /// Maximum disk cache size in bytes.
    pub cache_size: u64,
    /// Maximum in-memory cache size in bytes.
    pub memory_limit: usize,
}

impl Default for VfsConfig {
    fn default() -> Self {
        Self {
            meta_path: String::new(),
            cache_dir: String::new(),
            cache_size: 100 * 1024 * 1024 * 1024, // 100 GB
            memory_limit: 1024 * 1024 * 1024,      // 1 GB
        }
    }
}

/// The core VFS — satisfies filesystem operations using metadata + data + cache.
pub struct StudioVfs {
    meta: Arc<RedbMetaEngine>,
    data: Arc<DataStore>,
    mem_cache: Arc<MemoryCache>,
    disk_cache: Arc<DiskCache>,
    // Pending writes buffer: (ino, chunk_idx, block_idx) -> (start_offset_in_block, data)
    pending_writes: Arc<tokio::sync::Mutex<std::collections::HashMap<(Ino, u32, u32), (u32, BytesMut)>>>,
    // Notify background uploader when new slices are pending
    upload_notify: Arc<tokio::sync::Notify>,
}

impl StudioVfs {
    /// Create a new VFS instance.
    pub fn new(
        meta: Arc<RedbMetaEngine>,
        data: Arc<DataStore>,
        mem_cache: Arc<MemoryCache>,
        disk_cache: Arc<DiskCache>,
    ) -> Self {
        Self {
            meta,
            data,
            mem_cache,
            disk_cache,
            pending_writes: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            upload_notify: Arc::new(tokio::sync::Notify::new()),
        }
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

    // ─── Attribute Operations ────────────────────────────────────────────

    /// Get file attributes.
    pub fn getattr(&self, ino: Ino) -> VfsResult<InodeAttr> {
        Ok(self.meta.get_attr(ino)?)
    }

    /// Set file attributes (partial update).
    pub fn setattr(&self, ino: Ino, size: Option<u64>) -> VfsResult<InodeAttr> {
        let mut attr = self.meta.get_attr(ino)?;

        if let Some(new_size) = size {
            attr.size = new_size;
            // TODO: truncate chunks beyond new size
        }

        attr.touch_mtime();
        self.meta.set_attr(&attr)?;
        Ok(attr)
    }

    // ─── Directory Operations ────────────────────────────────────────────

    /// Lookup an entry in a directory.
    pub fn lookup(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        let entry = self.meta.lookup(parent, name)?;
        Ok(self.meta.get_attr(entry.ino)?)
    }

    /// Read directory entries.
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
        // Invalidate caches
        self.mem_cache.invalidate_inode(attr.ino);
        // Disk cache invalidation by inode is no longer supported/needed
        // self.disk_cache.invalidate_inode(attr.ino)?;
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

    // ─── Read Path ───────────────────────────────────────────────────────

    /// Read file data at the given offset.
    ///
    /// Read path: memory cache → disk cache (TODO) → S3
    pub async fn read(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        let attr = self.meta.get_attr(ino)?;

        // Clamp read to file size
        let end = (offset + size as u64).min(attr.size);
        if offset >= end {
            return Ok(Bytes::new());
        }
        let effective_size = (end - offset) as usize;

        // Try memory cache first
        let cache_key = CacheKey::new(ino, offset);
        if let Some(data) = self.mem_cache.get(&cache_key) {
            if data.len() >= effective_size {
                debug!(ino, offset, size = effective_size, "memory cache hit");
                return Ok(data.slice(..effective_size));
            }
        }

        // Fetch from S3 via chunk/slice resolution
        // This will now handle disk cache internally per-block
        let data = self.read_from_storage(ino, offset, effective_size, &attr).await?;

        // Store in memory cache
        self.mem_cache.put(cache_key, data.clone());
        
        // Disk cache population moved to read_from_storage -> fetch_slice_range

        Ok(data)
    }

    /// Resolve chunk/slice/block data from S3 storage.
    async fn read_from_storage(
        &self,
        ino: Ino,
        offset: u64,
        size: usize,
        _attr: &InodeAttr,
    ) -> VfsResult<Bytes> {
        // 1. Get Data Root
        let root_id = match self.meta.get_data_root(ino)? {
            Some(id) => id,
            None => return Ok(BytesMut::zeroed(size).freeze()),
        };

        // 2. Fetch & Parse Root Object
        let root_bytes = self.data.get_object(&root_id).await
            .map_err(|e| VfsError::Data(e))?;
        
        let root_payload_bytes = read_object(&root_bytes)
            .map_err(|e| VfsError::Other(e.to_string()))?;
        
        let root_payload = ObjectPayload::from_bytes(&root_payload_bytes)
             .map_err(|e| VfsError::Other(e.to_string()))?;

        // 3. Resolve Data
        let mut result = BytesMut::with_capacity(size);
        let mut remaining = size;
        let mut current_offset = offset;

        match root_payload.kind {
            ObjectKind::Data => {
                // Single object file. Assume it starts at 0.
                if current_offset < root_payload.data.len() as u64 {
                    let end = (current_offset + remaining as u64).min(root_payload.data.len() as u64);
                    let len = (end - current_offset) as usize;
                    result.extend_from_slice(&root_payload.data[current_offset as usize..end as usize]);
                    remaining -= len;
                }
                // Fill rest with zeroes if any
                if remaining > 0 {
                    result.extend_from_slice(&vec![0u8; remaining]);
                }
            }
            ObjectKind::Meta => {
                // Tree traversal
                while remaining > 0 {
                     if let Some(link) = root_payload.find_child(current_offset) {
                         // Fetch child
                         // Optimization: cache child objects?
                         let child_bytes = self.data.get_object(&link.object_id).await?;
                         let child_payload_bytes = read_object(&child_bytes)
                             .map_err(|e| VfsError::Other(e.to_string()))?;
                         let child_payload = ObjectPayload::from_bytes(&child_payload_bytes)
                             .map_err(|e| VfsError::Other(e.to_string()))?;
                         
                         match child_payload.kind {
                             ObjectKind::Data => {
                                 // Calculate relative offset
                                 let off_in_child = current_offset - link.offset;
                                 let available = (link.length as u64).saturating_sub(off_in_child);
                                 let to_read = (remaining as u64).min(available) as usize;
                                 
                                 if to_read > 0 {
                                     result.extend_from_slice(&child_payload.data[off_in_child as usize..off_in_child as usize + to_read]);
                                     remaining -= to_read;
                                     current_offset += to_read as u64;
                                 } else {
                                     // Should not happen if find_child is correct
                                     break; 
                                 }
                             }
                             ObjectKind::Meta => {
                                 // Nested tree? For now, we only support 1 level of nesting (Root -> Data).
                                 // Recursing would need a separate function.
                                 // Let's error or skip. 
                                 return Err(VfsError::Other("Nested meta objects not yet supported".into()));
                             }
                         }
                     } else {
                         // Hole
                         // Find next child to determine hole size, or just 1 byte?
                         // Optim: skip to next child start or end of read
                         // For now, read 4KB zeroes?
                         let hole_size = remaining.min(4096);
                         result.extend_from_slice(&vec![0u8; hole_size]);
                         remaining -= hole_size;
                         current_offset += hole_size as u64;
                     }
                }
            }
        }

        Ok(result.freeze())
    }

    /// Fetch a range of data from a slice by reading the appropriate blocks.
    async fn fetch_slice_range(
        &self,
        slice_id: u64,
        slice_size: u32,
        off_in_slice: u32,
        len: u32,
    ) -> VfsResult<Bytes> {
        let bs = BLOCK_SIZE as u64;
        let mut result = BytesMut::with_capacity(len as usize);
        let mut remaining = len as u64;
        let mut pos = off_in_slice as u64;

        while remaining > 0 {
            let block_idx = (pos / bs) as u32;
            let offset_in_block = pos % bs;
            let block_actual_size = if (block_idx + 1) as u64 * bs > slice_size as u64 {
                slice_size as u64 - block_idx as u64 * bs
            } else {
                bs
            };

            let to_read = remaining.min(block_actual_size - offset_in_block);

            // Try disk cache first
            let block_actual_u32 = block_actual_size as u32;
            let cached_block = self.disk_cache.get(slice_id, block_idx, block_actual_u32)?;

            let block_data = if let Some(data) = cached_block {
                if offset_in_block == 0 && to_read == block_actual_size {
                    data
                } else {
                     data.slice(offset_in_block as usize..(offset_in_block + to_read) as usize)
                }
            } else {
                // Disk cache miss — fetch from S3
                let fetched_data = if offset_in_block == 0 && to_read == block_actual_size {
                    // Read full block
                    self.data
                        .get_block(slice_id, block_idx, block_actual_u32)
                        .await?
                } else {
                    // Read partial block (range request)
                    // NOTE: use get_block_range only if we don't want to cache the full block.
                    // But for L2 cache, we usually want to cache the full block?
                    // If we only read a range, we can't cache the full block unless we download it all.
                    // For performance, if we have a disk cache, maybe we SHOULD download the full block?
                    // For now, let's stick to range read if it's partial, but we WON'T cache it to disk
                    // unless we fetched the whole thing.
                    
                    // Optimization: If cache is enabled, maybe always fetch full block?
                    // Let's keep it simple: only cache if we fetched full block or if we decide to fetch full block.
                    // Since specific requirement wasn't to change read behavior aggressively,
                    // let's stick to existing logic but only put to cache if we have full block.
                    
                    self.data
                        .get_block_range(
                            slice_id,
                            block_idx,
                            block_actual_u32,
                            offset_in_block,
                            to_read,
                        )
                        .await?
                };

                // Populate disk cache if we fetched a full block (and it matches expected size)
                if fetched_data.len() as u64 == block_actual_size {
                     self.disk_cache.put(slice_id, block_idx, &fetched_data)?;
                }
                
                fetched_data
            };

            result.extend_from_slice(&block_data);
            pos += to_read;
            remaining -= to_read;
        }

        Ok(result.freeze())
    }

    // ─── Write Path ──────────────────────────────────────────────────────

    /// Write data to a file at the given offset.
    ///
    /// Buffers writes into 4MiB blocks. Uploads only when a block is full.
    /// Partial blocks are kept in memory until `flush` is called (or next write fills them).
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        let mut written = 0;
        let mut remaining = data.len();
        let mut current_offset = offset;
        let mut data_offset = 0;

        let bs = BLOCK_SIZE as u64; // 4MiB

        // We need to iterate block by block to buffer correctly
        while remaining > 0 {
            let chunk_key = ChunkKey::from_offset(ino, current_offset);
            let offset_in_chunk = current_offset % CHUNK_SIZE;
            let block_idx_in_chunk = (offset_in_chunk / bs) as u32;
            let offset_in_block = (offset_in_chunk % bs) as usize;

            let space_in_block = BLOCK_SIZE as usize - offset_in_block;
            let to_write = remaining.min(space_in_block);

            // Append to pending buffer
            let mut pending = self.pending_writes.lock().await;
            let key = (ino, chunk_key.index, block_idx_in_chunk);
            let (start_off, buffer) = pending.entry(key).or_insert_with(|| (offset_in_block as u32, BytesMut::with_capacity(BLOCK_SIZE as usize)));
            
            // If we are writing outside the current buffered range, flush and restart
            let buf_end = *start_off + buffer.len() as u32;
            if (offset_in_block as u32) < *start_off || (offset_in_block as u32) > buf_end {
                // Gap or jump! Flush current part.
                let data = buffer.clone();
                let old_start = *start_off;
                self.flush_block(ino, chunk_key.index, block_idx_in_chunk, old_start, &data).await?;
                
                // Reset buffer to new start
                *start_off = offset_in_block as u32;
                buffer.clear();
            }

            // Relative offset in current buffer
            let rel_off = (offset_in_block as u32 - *start_off) as usize;

            if rel_off < buffer.len() {
                 // Overwrite/Append mix
                 for i in 0..to_write {
                     let p = rel_off + i;
                     if p < buffer.len() {
                         buffer[p] = data[data_offset + i];
                     } else {
                         buffer.put_u8(data[data_offset + i]);
                     }
                 }
            } else {
                // Clean append to buffer
                buffer.extend_from_slice(&data[data_offset..data_offset + to_write]);
            }

            // If block is full (4MiB), flush it immediately
            if buffer.len() >= BLOCK_SIZE as usize {
                let data = buffer.clone();
                let off = *start_off;
                self.flush_block(ino, chunk_key.index, block_idx_in_chunk, off, &data).await?;
                pending.remove(&key);
            }

            written += to_write;
            remaining -= to_write;
            current_offset += to_write as u64;
            data_offset += to_write;
        }

        // Update file size in metadata immediately
        let mut attr = self.meta.get_attr(ino)?;
        if current_offset > attr.size {
            attr.size = current_offset;
            attr.touch_mtime();
            self.meta.set_attr(&attr)?;
        }
        
        Ok(written as u32)
    }

    /// Flush all buffered blocks for a file.
    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        let mut pending = self.pending_writes.lock().await;
        
        let keys: Vec<_> = pending.keys()
             .filter(|(i, _, _)| *i == ino)
             .cloned()
             .collect();

        for key in keys {
            if let Some((start_off, data)) = pending.remove(&key) {
                let (_, chunk_idx, block_idx) = key;
                self.flush_block(ino, chunk_idx, block_idx, start_off, &data).await?;
            }
        }
        Ok(())
    }

    /// Helper to upload a block (full or partial) and update metadata
    async fn flush_block(&self, ino: Ino, chunk_idx: u32, block_idx: u32, start_off: u32, data: &[u8]) -> VfsResult<()> {
        if data.is_empty() { return Ok(()); }
        
        // 1. Create & Upload Data Object
        let payload = ObjectPayload::new_data(data.to_vec());
        let payload_bytes = payload.to_bytes().map_err(|e| VfsError::Other(e.to_string()))?;
        let object_bytes = create_object(&payload_bytes);
        let object_id = hash_object(&object_bytes);
        
        // Check if exists? (Content addressable)
        if !self.data.exists(&self.data.object_key(&object_id)).await? {
             self.data.put_object(&object_id, Bytes::from(object_bytes)).await?;
        }

        // 2. Update Root (Meta Object)
        // TODO: This uses a Global Lock implicitly effectively via Redb transaction inside MetaEngine? 
        // No, we need to be careful about race conditions if multiple writers.
        // For now, simple read-update-write loop.
        
        let mut children = Vec::new();
        if let Some(root_id) = self.meta.get_data_root(ino)? {
            // Fetch existing root
            let root_bytes = self.data.get_object(&root_id).await?;
            let root_payload_bytes = read_object(&root_bytes).map_err(|e| VfsError::Other(e.to_string()))?;
            let root_payload = ObjectPayload::from_bytes(&root_payload_bytes).map_err(|e| VfsError::Other(e.to_string()))?;
            
            match root_payload.kind {
                 ObjectKind::Meta => children = root_payload.children,
                 ObjectKind::Data => {
                     // Upgrade single-object file to Meta
                     // The old object covered 0..len
                     // We don't have its length explicitly stored in payload... 
                     // payload.data.len() is the length!
                     let len = root_payload.data.len() as u32;
                     // We need the ID of the old object to link to it. 
                     // Wait, we only have the payload bytes. 
                     // We need to re-hash or store the ID? 
                     // We just read `root_id`, that IS the ID.
                     children.push(Objectlink {
                         offset: 0,
                         length: len,
                         object_id: root_id,
                     });
                 }
            }
        }

        // 3. Update Children List (Remove overlaps, insert new)
        let bs = BLOCK_SIZE as u32; // 4MB
        // Usually chunk_idx is part of offset logic.
        // offset = chunk_idx * CHUNK_SIZE + block_idx * BLOCK_SIZE + start_off
        // CHUNK_SIZE = 64MB, BLOCK_SIZE = 4MB
        let offset = (chunk_idx as u64 * CHUNK_SIZE) + (block_idx as u64 * bs as u64) + start_off as u64;
        let length = data.len() as u32;
        let end = offset + length as u64;

        // Simple filter: remove any child that is fully covered or overlaps?
        // To be safe and simple: remove anything overlapping and re-insert?
        // Or better: keep it simple append-only log? 
        // The read path needs to handle overlaps (last win).
        // My `find_child` implementation assumes non-overlapping or first-match?
        // `find_child` does linear search. If I always append, later entries are at end.
        // `find` returns first match. `rev().find()` would return last match.
        // Let's use `rev()` in `read_from_storage`? Or ensure no overlaps here.
        // Let's Filter out overlaps here.
        
        children.retain(|child| {
            let child_end = child.offset + child.length as u64;
            // Keep if NO overlap
            // Overlap if: start < child_end && end > child_start
            ! (offset < child_end && end > child.offset)
        });

        children.push(Objectlink {
            offset,
            length,
            object_id,
        });
        
        // Sort by offset for cleaner tree
        children.sort_by_key(|c| c.offset);

        // 4. Create New Root
        let new_root = ObjectPayload::new_meta(children);
        let new_root_bytes = create_object(&new_root.to_bytes().map_err(|e| VfsError::Other(e.to_string()))?);
        let new_root_id = hash_object(&new_root_bytes);
        
        self.data.put_object(&new_root_id, Bytes::from(new_root_bytes)).await?;
        self.meta.set_data_root(ino, &new_root_id)?;
        
        debug!("flushed block: ino={} offset={} len={} new_root={}", ino, offset, length, new_root_id);
        Ok(())
    }

    // ─── Background Tasks ────────────────────────────────────────────────────────

    /// Spawn the background uploader task.
    ///
    /// This should be called once after creating the VFS.
    pub fn spawn_background_uploader(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                // Wait for notification
                self.upload_notify.notified().await;

                // Process all pending slices
                // In a real system, we'd want a more sophisticated queue.
                match self.meta.get_pending_slices() {
                    Ok(slices) => {
                        for (slice_id, length) in slices {
                            if let Err(e) = self.process_pending_slice(slice_id, length).await {
                                tracing::error!("failed to upload slice {}: {}", slice_id, e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("failed to get pending slices: {}", e);
                    }
                }
            }
        });
    }

    /// Process a single pending slice: read from disk -> upload to S3 -> remove pending.
    async fn process_pending_slice(&self, slice_id: u64, length: u32) -> VfsResult<()> {
        // Read from disk cache (expect full block)
        // We use block_idx 0 because we treat each slice as a single block for now (upto 4MB).
        // Larger slices would need iteration, but our write path enforces 4MB blocks.
        let data = self.disk_cache.get(slice_id, 0, length)?;
        
        if let Some(bytes) = data {
            // Upload to S3
            self.data.put_block(slice_id, 0, Bytes::copy_from_slice(&bytes)).await?;
            // Mark as uploaded
            self.meta.remove_pending_slice(slice_id)?;
            debug!("background upload complete: slice {}", slice_id);
        } else {
             // Slice not found in disk cache?
             // This is bad. It means we lost data or it wasn't written correctly.
             // Or maybe it was evicted? (Should not evict dirty slices!)
             // For now, log error. Recovery is hard if data is gone from disk.
             tracing::error!("integrity error: pending slice {} not found in disk cache", slice_id);
             // Should we remove it from pending checking S3?
             // If it's not on disk, maybe it IS on S3 already?
             if self.data.get_block(slice_id, 0, length).await.is_ok() {
                 self.meta.remove_pending_slice(slice_id)?;
                 tracing::warn!("slice {} was missing from disk but found on S3, recovered", slice_id);
             }
        }
        Ok(())
    }

    /// Spawn a background task to handle garbage collection.
    pub fn spawn_background_gc(self: Arc<Self>) {
        tokio::spawn(async move {
            // GC interval: 10 minutes
            let interval = tokio::time::Duration::from_secs(600);
            
            loop {
                tokio::time::sleep(interval).await;

                // Process deleted slices
                match self.meta.get_deleted_slices() {
                    Ok(slices) => {
                        if !slices.is_empty() {
                            info!("starting GC: {} slices to delete", slices.len());
                        }
                        for (slice_id, length) in slices {
                            // Delete from S3 (best effort)
                            if let Err(e) = self.data.delete_block(slice_id, 0, length).await {
                                tracing::warn!("failed to delete slice {} from S3: {}", slice_id, e);
                            }

                            // Delete from Disk Cache
                            if let Err(e) = self.disk_cache.remove(slice_id, 0, length) {
                                tracing::warn!("failed to delete slice {} from disk cache: {}", slice_id, e);
                            }

                            // Remove from metadata (commit deletion)
                            if let Err(e) = self.meta.remove_deleted_slice(slice_id) {
                                tracing::error!("failed to remove deleted slice {}: {}", slice_id, e);
                            } else {
                                debug!("GC complete: slice {}", slice_id);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("failed to get deleted slices: {}", e);
                    }
                }
            }
        });
    }
}

// ─── Slice Resolution ────────────────────────────────────────────────────────

/// A resolved data segment after flattening overlapping slices.
#[derive(Debug)]
struct ResolvedSegment {
    /// Slice ID (0 = hole).
    slice_id: u64,
    /// Total size of the original slice (needed for block calculations).
    slice_size: u32,
    /// Offset within the slice.
    off: u32,
    /// Length of data in this segment.
    len: u32,
}

/// Resolve overlapping slices for a given range within a chunk.
///
/// Later slices in the list take precedence (same as JuiceFS).
/// Returns a list of non-overlapping segments covering the requested range.
fn resolve_slices(slices: &[Slice], offset: u32, size: u32) -> Vec<ResolvedSegment> {
    if slices.is_empty() || size == 0 {
        return vec![ResolvedSegment {
            slice_id: 0,
            slice_size: size,
            off: 0,
            len: size,
        }];
    }

    let end = offset + size;

    // Build a coverage map: for each byte position, which slice covers it?
    // We use a simpler approach: iterate slices in order (later wins) and
    // build intervals.

    // Create an interval list: [(start, end, slice_idx)]
    // where later entries override earlier ones
    let mut coverage: Vec<(u32, u32, Option<usize>)> = vec![(offset, end, None)];

    for (idx, slice) in slices.iter().enumerate() {
        let s_start = slice.pos + slice.off;
        let s_end = slice.pos + slice.off + slice.len;

        // Clip to our range of interest
        let clip_start = s_start.max(offset);
        let clip_end = s_end.min(end);
        if clip_start >= clip_end {
            continue;
        }

        // Split existing coverage intervals
        let mut new_coverage = Vec::new();
        for &(c_start, c_end, c_slice) in &coverage {
            if clip_start <= c_start && clip_end >= c_end {
                // Fully covered by new slice
                new_coverage.push((c_start, c_end, Some(idx)));
            } else if clip_start > c_start && clip_end < c_end {
                // New slice splits this interval into 3 parts
                new_coverage.push((c_start, clip_start, c_slice));
                new_coverage.push((clip_start, clip_end, Some(idx)));
                new_coverage.push((clip_end, c_end, c_slice));
            } else if clip_start <= c_start && clip_end > c_start && clip_end < c_end {
                // Overlaps beginning
                new_coverage.push((c_start, clip_end, Some(idx)));
                new_coverage.push((clip_end, c_end, c_slice));
            } else if clip_start > c_start && clip_start < c_end && clip_end >= c_end {
                // Overlaps end
                new_coverage.push((c_start, clip_start, c_slice));
                new_coverage.push((clip_start, c_end, Some(idx)));
            } else {
                // No overlap
                new_coverage.push((c_start, c_end, c_slice));
            }
        }
        coverage = new_coverage;
    }

    // Convert coverage to resolved segments
    coverage
        .iter()
        .filter(|(s, e, _)| s < e)
        .map(|&(start, end, slice_idx)| match slice_idx {
            Some(idx) => {
                let slice = &slices[idx];
                let off_in_slice = (start - slice.pos) + slice.off;
                ResolvedSegment {
                    slice_id: slice.id,
                    slice_size: slice.size,
                    off: off_in_slice,
                    len: end - start,
                }
            }
            None => ResolvedSegment {
                slice_id: 0,
                slice_size: 0,
                off: 0,
                len: end - start,
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_empty_slices() {
        let segments = resolve_slices(&[], 0, 1024);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].slice_id, 0); // hole
        assert_eq!(segments[0].len, 1024);
    }

    #[test]
    fn test_resolve_single_slice() {
        let slices = vec![Slice {
            pos: 0,
            id: 1,
            size: 4096,
            off: 0,
            len: 4096,
        }];
        let segments = resolve_slices(&slices, 0, 4096);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].slice_id, 1);
        assert_eq!(segments[0].len, 4096);
    }

    #[test]
    fn test_resolve_overlapping_slices() {
        // Slice 1 covers 0..4096, Slice 2 covers 1024..3072
        // Result: [0..1024 from S1, 1024..3072 from S2, 3072..4096 from S1]
        let slices = vec![
            Slice {
                pos: 0,
                id: 1,
                size: 4096,
                off: 0,
                len: 4096,
            },
            Slice {
                pos: 1024,
                id: 2,
                size: 2048,
                off: 0,
                len: 2048,
            },
        ];
        let segments = resolve_slices(&slices, 0, 4096);
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].slice_id, 1);
        assert_eq!(segments[0].len, 1024);
        assert_eq!(segments[1].slice_id, 2);
        assert_eq!(segments[1].len, 2048);
        assert_eq!(segments[2].slice_id, 1);
        assert_eq!(segments[2].len, 1024);
    }
}

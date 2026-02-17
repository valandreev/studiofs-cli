//! Core VFS implementation — wires together metadata, data, and cache layers.

use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;
use studiofs_cache::memory::MemoryCache;
use studiofs_cache::disk::DiskCache;
use studiofs_data::store::DataStore;
use studiofs_data::object::{
    create_object, read_object, hash_object, ObjectPayload, ObjectKind, Objectlink,
};
use studiofs_meta::*;
use thiserror::Error;
use tracing::{debug, info, error};
use studiofs_cache::writeback::{WritebackManager, PendingOp};
use parking_lot::Mutex;


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
#[derive(Clone)]
pub struct StudioVfs {
    meta: Arc<RedbMetaEngine>,
    data: Arc<DataStore>,
    mem_cache: Arc<MemoryCache>,
    disk_cache: Arc<DiskCache>,
    
    // RAM buffer for current blocks: (ino, chunk_idx, block_idx) -> (start_offset_in_block, data)
    pending_writes: Arc<Mutex<std::collections::HashMap<(Ino, u32, u32), (u32, BytesMut)>>>,
    
    // Memory cache for Attributes (size, mtime) to avoid Redb hits on every write
    dirty_attrs: Arc<Mutex<std::collections::HashMap<Ino, InodeAttr>>>,
    
    // Pending links (completed blocks not yet committed to metadata): ino -> list of links
    pending_links: Arc<Mutex<std::collections::HashMap<Ino, Vec<Objectlink>>>>,
    
    // Track active disk flush tasks
    active_flushes: Arc<Mutex<std::collections::HashMap<Ino, Vec<tokio::task::JoinHandle<VfsResult<()>>>>>>,
    
    // Limit global concurrent disk flushes (e.g. 32)
    flush_semaphore: Arc<tokio::sync::Semaphore>,
    
    // Writeback Manager for persistent queuing
    writeback: Arc<WritebackManager>,
}

impl StudioVfs {
    /// Create a new VFS instance.
    pub fn new(
        meta: Arc<RedbMetaEngine>,
        data: Arc<DataStore>,
        mem_cache: Arc<MemoryCache>,
        
        // Configs
        cache_dir: &str,
        cache_size: u64,
    ) -> VfsResult<Arc<Self>> {
        // Init Disk Cache
        let disk_cache = DiskCache::new(cache_dir, cache_size)?; // Use provided size
        
        // Init Writeback Manager
        let (writeback, upload_rx) = WritebackManager::new(cache_dir, disk_cache.clone());

        let vfs = Arc::new(Self {
            meta,
            data,
            mem_cache,
            disk_cache,
            pending_writes: Arc::new(Mutex::new(std::collections::HashMap::new())),
            dirty_attrs: Arc::new(Mutex::new(std::collections::HashMap::new())),
            pending_links: Arc::new(Mutex::new(std::collections::HashMap::new())),
            active_flushes: Arc::new(Mutex::new(std::collections::HashMap::new())),
            flush_semaphore: Arc::new(tokio::sync::Semaphore::new(32)),
            writeback,
        });

        // Spawn Upload Worker
        let vfs_clone = vfs.clone();
        tokio::spawn(async move {
            vfs_clone.background_uploader(upload_rx).await;
        });

        Ok(vfs)
    }
    
    /// Background task to process pending uploads.
    /// Background task to process pending uploads.
    async fn background_uploader(self: Arc<Self>, rx: tokio::sync::mpsc::UnboundedReceiver<PendingOp>) {
        info!("background uploader started");
        
        use futures::stream::StreamExt;
        
        let self_ref = &self;
        // Use a simpler approach: spawn a task for each upload, but limit concurrency via a semaphore or just buffer_unordered on a stream of futures.
        // Since rx is a receiver, we can turn it into a stream.
        
        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        
        const CONCURRENCY: usize = 32; // Parallel uploads

        stream.for_each_concurrent(CONCURRENCY, |op| async move {
            // 1. Read from DiskCache (async)
            let data_opt = match self_ref.disk_cache.get_object(op.object_id).await {
                Ok(d) => d,
                Err(e) => {
                    error!("disk cache error for {}: {}", op.object_id, e);
                    return;
                }
            };
            
            if let Some(data) = data_opt {
                // 2. Upload to S3 (if not exists)
                let id_str = format!("{:016x}", op.object_id);
                let key = self_ref.data.object_key(&id_str);
                
                // Optimistic check
                let needs_upload = match self_ref.data.exists(&key).await {
                     Ok(exists) => !exists,
                     Err(_) => true, 
                };
                
                if needs_upload {
                     if let Err(e) = self_ref.data.put_object(&id_str, data).await {
                         error!("failed to upload object {}: {}", op.object_id, e);
                         // Retry logic would go here, maybe re-queue?
                         return; 
                     }
                }
                
                // 3. Mark Complete
                self_ref.writeback.complete(&op);
                debug!("background upload complete: {}", op.object_id);

            } else {
                error!("pending upload object {} missing from disk cache", op.object_id);
            }
        }).await;
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
        // 1. Check dirty attrs cache (RAM speed)
        if let Some(attr) = self.dirty_attrs.lock().get(&ino) {
            return Ok(attr.clone());
        }
        
        // 2. Fetch from Redb
        Ok(self.meta.get_attr(ino)?)
    }

    /// Set file attributes (partial update).
    pub fn setattr(&self, ino: Ino, size: Option<u64>) -> VfsResult<InodeAttr> {
        let mut attr = self.getattr(ino)?;

        if let Some(new_size) = size {
            attr.size = new_size;
            // TODO: truncate chunks beyond new size
        }

        attr.touch_mtime();
        
        // Store in dirty cache IMMEDIATELY
        self.dirty_attrs.lock().insert(ino, attr.clone());
        
        // Commit asynchronously or on flush? 
        // For setattr, let's commit immediately to Redb to be safe, but we've already updated the cache.
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

        // Read loop (block aligned)
        let mut result = BytesMut::with_capacity(effective_size);
        let mut current_offset = offset;
        let mut remaining = effective_size;

        while remaining > 0 {
            let block_start = (current_offset / BLOCK_SIZE as u64) * BLOCK_SIZE as u64;
            let offset_in_block = (current_offset - block_start) as usize;

            
            // Should we clamp available to BLOCK_SIZE? Yes.
            let block_len = if block_start + BLOCK_SIZE as u64 > attr.size {
                (attr.size - block_start) as usize
            } else {
                BLOCK_SIZE as usize
            };
            
            // Try cache
            let cache_key = studiofs_cache::memory::CacheKey::new(ino, block_start);
            let block_data: Bytes;
            
            if let Some(cached) = self.mem_cache.get(&cache_key) {
                 block_data = cached;
            } else {
                 // Fetch full block (or up to EOF)
                 block_data = self.read_from_storage(ino, block_start, block_len, &attr).await?;
                 
                 // Cache it
                 self.mem_cache.put(cache_key, block_data.clone());
            }

            // Copy slice
            let to_copy = remaining.min(block_data.len().saturating_sub(offset_in_block));
            if to_copy == 0 {
                 break; // EOF or error
            }

            result.extend_from_slice(&block_data[offset_in_block..offset_in_block + to_copy]);

            current_offset += to_copy as u64;
            remaining -= to_copy;
        }

        Ok(result.freeze())

    }

    /// Resolve chunk/slice/block data from S3 storage.
    /// Fetch object data from Disk Cache or S3.
    async fn get_object_with_cache(&self, object_id: &str) -> VfsResult<Bytes> {
        let oid_u64 = u64::from_str_radix(object_id, 16).map_err(|e| VfsError::Other(e.to_string()))?;

        // 1. Try Disk Cache
        if let Some(data) = self.disk_cache.get_object(oid_u64).await? {
            // debug!("disk cache hit: {}", object_id);
            return Ok(data);
        }

        debug!("disk cache miss: {}, fetching from S3", object_id);

        // 2. Fetch from S3
        let data = self.data.get_object(object_id).await
            .map_err(|e| VfsError::Data(e))?;

        // 3. Populate Disk Cache
        self.disk_cache.put_object(oid_u64, &data).await?;

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
        let root_bytes = match self.get_object_with_cache(&root_id).await {
            Ok(b) => b,
            Err(e) => {
                error!("failed to read root object {}: {}", root_id, e);
                return Err(e);
            }
        };
        
        let root_payload_bytes = read_object(&root_bytes)
            .map_err(|e| VfsError::Other(e.to_string()))?;
        
        let root_payload = ObjectPayload::from_bytes(&root_payload_bytes)
             .map_err(|e| VfsError::Other(e.to_string()))?;

        // 3. Resolve Data
        let mut result = BytesMut::zeroed(size);
        let current_offset = offset;

        match root_payload.kind {
            ObjectKind::Data => {
                // Single object file. Assume it starts at 0.
                if current_offset < root_payload.data.len() as u64 {
                    let end = (current_offset + size as u64).min(root_payload.data.len() as u64);
                    let len = (end - current_offset) as usize;
                    result[0..len].copy_from_slice(&root_payload.data[current_offset as usize..end as usize]);
                }
            }
            ObjectKind::Meta => {
                // Identify all children overlapping the requested range
                let end_offset = offset + size as u64;
                let children_to_fetch: Vec<Objectlink> = root_payload.children.iter()
                    .filter(|child| {
                        let child_end = child.offset + child.length as u64;
                        // Overlap check
                        !(end_offset <= child.offset || offset >= child_end)
                    })
                    .cloned()
                    .collect();

                if children_to_fetch.is_empty() {
                    return Ok(result.freeze());
                }

                // Parallel fetch
                use futures::stream::{self, StreamExt};
                
                // Concurrency limit
                const CONCURRENCY: usize = 16; 

                let fetch_stream = stream::iter(children_to_fetch)
                    .map(|link| {
                        let self_ref = &self;
                        async move {
                            let child_bytes = self_ref.get_object_with_cache(&link.object_id).await?;
                            let child_payload_bytes = read_object(&child_bytes)
                                .map_err(|e| VfsError::Other(e.to_string()))?;
                            let child_payload = ObjectPayload::from_bytes(&child_payload_bytes)
                                .map_err(|e| VfsError::Other(e.to_string()))?;
                            
                            Ok::<_, VfsError>((link, child_payload))
                        }
                    })
                    .buffer_unordered(CONCURRENCY);

                let mut stream = fetch_stream;
                
                while let Some(fetch_result) = stream.next().await {
                    match fetch_result {
                        Ok((link, payload)) => {
                            if let ObjectKind::Data = payload.kind {
                                // Calculate overlap with request
                                // Request: [offset, offset + size)
                                // Child: [link.offset, link.offset + link.length)
                                
                                let start = offset.max(link.offset);
                                let end = (offset + size as u64).min(link.offset + link.length as u64);
                                
                                if start < end {
                                    let len = (end - start) as usize;
                                    let dest_off = (start - offset) as usize;
                                    let src_off = (start - link.offset) as usize;
                                    
                                    result[dest_off..dest_off + len]
                                        .copy_from_slice(&payload.data[src_off..src_off + len]);
                                }
                            }
                        },
                        Err(e) => {
                            tracing::error!("failed to fetch child object: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok(result.freeze())
    }



    // ─── Write Path ──────────────────────────────────────────────────────

    /// Write data to a file at the given offset.
    ///
    /// Buffers writes into 4MiB blocks. Uploads only when a block is full.
    /// Partial blocks are kept in memory until `flush` is called (or next write fills them).
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        let mut written = 0u64;
        let mut remaining = data.len();
        let mut current_offset = offset;
        let mut data_offset = 0;

        let bs = BLOCK_SIZE as u64; // 4MiB

        while remaining > 0 {
            let chunk_key = ChunkKey::from_offset(ino, current_offset);
            let offset_in_chunk = current_offset % CHUNK_SIZE;
            let block_idx_in_chunk = (offset_in_chunk / bs) as u32;
            let offset_in_block = (offset_in_chunk % bs) as usize;

            let space_in_block = BLOCK_SIZE as usize - offset_in_block;
            let to_write = remaining.min(space_in_block);
            
            // LOCK: Rapid sync access to buffers
            let mut flush_gap_data = None;
            let mut flush_full_data = None;
            {
                let mut pending = self.pending_writes.lock();
                let key = (ino, chunk_key.index, block_idx_in_chunk);
                let (start_off, buffer) = pending.entry(key).or_insert_with(|| (offset_in_block as u32, BytesMut::with_capacity(BLOCK_SIZE as usize)));
                
                // If we are writing outside the current buffered range, flush and restart
                let buf_end = *start_off + buffer.len() as u32;
                if (offset_in_block as u32) < *start_off || (offset_in_block as u32) > buf_end {
                    flush_gap_data = Some((buffer.clone().freeze(), *start_off));
                    *start_off = offset_in_block as u32;
                    buffer.clear();
                }

                // Append to buffer
                let rel_off = (offset_in_block as u32 - *start_off) as usize;
                if rel_off < buffer.len() {
                     for i in 0..to_write {
                         let p = rel_off + i;
                         if p < buffer.len() { buffer[p] = data[data_offset + i]; } else { buffer.put_u8(data[data_offset + i]); }
                     }
                } else {
                    buffer.extend_from_slice(&data[data_offset..data_offset + to_write]);
                }

                if buffer.len() >= BLOCK_SIZE as usize {
                    flush_full_data = Some((buffer.clone().freeze(), *start_off));
                    pending.remove(&key);
                }
            } // UNLOCK

            // ASYNC FLUSH tasks
            if let Some((d, off)) = flush_gap_data { self.flush_later(ino, chunk_key.index, block_idx_in_chunk, off, d); }
            if let Some((d, off)) = flush_full_data { self.flush_later(ino, chunk_key.index, block_idx_in_chunk, off, d); }

            written += to_write as u64;
            remaining -= to_write;
            current_offset += to_write as u64;
            data_offset += to_write;
        }

        // Update file size in RAM CACHE only (very fast)
        {
            let mut dirty = self.dirty_attrs.lock();
            let attr = dirty.entry(ino).or_insert_with(|| self.meta.get_attr(ino).expect("attr exists"));
            if current_offset > attr.size {
                attr.size = current_offset;
                attr.touch_mtime();
            }
        }
        
        Ok(written as u32)
    }

    /// Internal helper to spawn a background flush task.
    fn flush_later(&self, ino: Ino, chunk_idx: u32, block_idx: u32, start_off: u32, data: Bytes) {
        let vfs = self.clone();
        let handle = tokio::spawn(async move {
            let _permit = vfs.flush_semaphore.acquire().await.map_err(|e| VfsError::Other(e.to_string()))?;

            if start_off == 0 && data.len() == BLOCK_SIZE as usize {
                let key_offset = (chunk_idx as u64 * CHUNK_SIZE) + (block_idx as u64 * BLOCK_SIZE as u64);
                let cache_key = studiofs_cache::memory::CacheKey::new(ino, key_offset);
                vfs.mem_cache.put(cache_key, data.clone());
            }

            if let Some(link) = vfs.flush_block_internal(ino, chunk_idx, block_idx, start_off, data).await? {
                vfs.pending_links.lock().entry(ino).or_default().push(link);
            }
            Ok(())
        });

        let mut flushes = self.active_flushes.lock();
        let vec = flushes.entry(ino).or_default();
        vec.push(handle);
        if vec.len() > 256 { vec.retain(|h| !h.is_finished()); }
    }

    /// Flush all buffered blocks for a file.
    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        // 1. Extract all remaining partial buffers
        let mut entries_to_flush = Vec::new();
        {
            let mut pending = self.pending_writes.lock();
            let keys: Vec<_> = pending.keys().filter(|(i, _, _)| *i == ino).cloned().collect();
            for key in keys {
                if let Some((start_off, data)) = pending.remove(&key) {
                    let (_, chunk_idx, block_idx) = key;
                    entries_to_flush.push((chunk_idx, block_idx, start_off, data.freeze()));
                }
            }
        }
        
        // 2. Start flushes for remaining buffers
        for (chunk_idx, block_idx, start_off, data) in entries_to_flush {
            self.flush_later(ino, chunk_idx, block_idx, start_off, data);
        }

        // 3. Await ALL active flushes for this inode
        let handles = self.active_flushes.lock().remove(&ino).unwrap_or_default();
        for h in handles { h.await.map_err(|e| VfsError::Join(e))??; }
        
        // 4. Collect and clear pending links
        let pending_links_vec = self.pending_links.lock().remove(&ino).unwrap_or_default();

        // 5. Commit to metadata
        if !pending_links_vec.is_empty() {
            self.commit_uploads(ino, pending_links_vec).await?;
        }
        
        // 6. Finalize dirty attributes to Redb
        if let Some(attr) = self.dirty_attrs.lock().remove(&ino) {
            self.meta.set_attr(&attr)?;
        }

        Ok(())
    }

    /// Helper to upload a block (full or partial) to Disk Cache & Writeback Queue.
    async fn flush_block_internal(&self, ino: Ino, chunk_idx: u32, block_idx: u32, start_off: u32, data: Bytes) -> VfsResult<Option<Objectlink>> {
        if data.is_empty() { return Ok(None); }
        
        let data_vec = data.to_vec();
        
        // 1. Offload CPU-intensive compression and hashing
        let (object_id, object_bytes, length) = tokio::task::spawn_blocking(move || {
             let payload = ObjectPayload::new_data(data_vec);
             let length = payload.data.len() as u32;
             let payload_bytes = payload.to_bytes().map_err(|e| VfsError::Other(e.to_string()))?;
             let object_bytes = create_object(&payload_bytes);
             let object_id = hash_object(&object_bytes);
             Ok::<_, VfsError>((object_id, object_bytes, length))
        }).await??;

        // 2. Write to Disk Cache & Writeback Queue
        let oid_u64 = u64::from_str_radix(&object_id, 16).map_err(|e| VfsError::Other(e.to_string()))?;
        self.disk_cache.put_object(oid_u64, &object_bytes).await?;
        
        let bs = BLOCK_SIZE as u64;
        let offset = (chunk_idx as u64 * CHUNK_SIZE) + (block_idx as u64 * bs) + start_off as u64;
        
        self.writeback.submit(ino, offset, oid_u64).await;

        Ok(Some(Objectlink {
            offset,
            length,
            object_id,
        }))
    }



    /// Commit all pending uploads for an inode to the metadata.
    async fn commit_uploads(&self, ino: Ino, new_links: Vec<Objectlink>) -> VfsResult<()> {
        if new_links.is_empty() { return Ok(()); }
        
        // 1. Fetch current root
        let mut children = Vec::new();
        if let Some(root_id) = self.meta.get_data_root(ino)? {
            // Fetch existing root (use cache!)
            if let Ok(root_bytes) = self.get_object_with_cache(&root_id).await {
                if let Ok(root_payload_bytes) = read_object(&root_bytes) {
                     if let Ok(root_payload) = ObjectPayload::from_bytes(&root_payload_bytes) {
                         match root_payload.kind {
                             ObjectKind::Meta => children = root_payload.children,
                             ObjectKind::Data => {
                                 let len = root_payload.data.len() as u32;
                                 children.push(Objectlink {
                                     offset: 0,
                                     length: len,
                                     object_id: root_id,
                                 });
                             }
                         }
                     }
                }
            }
        }

        // 2. Merge new links
        for link in new_links {
             // Remove overlaps
             let link_end = link.offset + link.length as u64;
             children.retain(|child| {
                 let child_end = child.offset + child.length as u64;
                 ! (link.offset < child_end && link_end > child.offset)
             });
             children.push(link);
        }
        
        children.sort_by_key(|c| c.offset);

        // 3. Create New Root
        let (new_root_id, new_root_bytes) = tokio::task::spawn_blocking(move || {
            let new_root = ObjectPayload::new_meta(children);
            let new_root_bytes = create_object(&new_root.to_bytes().map_err(|e| VfsError::Other(e.to_string()))?);
            let new_root_id = hash_object(&new_root_bytes);
            Ok::<_, VfsError>((new_root_id, new_root_bytes))
        }).await??;
        
        // Write to Disk Cache & Queue
        let root_oid_u64 = u64::from_str_radix(&new_root_id, 16).map_err(|e| VfsError::Other(e.to_string()))?;
        self.disk_cache.put_object(root_oid_u64, &new_root_bytes).await?;
        self.writeback.submit(ino, 0, root_oid_u64).await;
        
        // Update Redb Transactionally (Locally)
        self.meta.set_data_root(ino, &new_root_id)?;
        
        debug!("consumed uploads and updated root: ino={} new_root={}", ino, new_root_id);
        Ok(())
    }

    // ─── Background Tasks ────────────────────────────────────────────────────────



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
                            if let Err(e) = self.disk_cache.remove_object(slice_id).await {
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

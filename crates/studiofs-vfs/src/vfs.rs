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
use tracing::{debug, info, error, warn};
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
    
    // RAM buffer for currently being written blocks
    pending_writes: Arc<Mutex<std::collections::HashMap<(Ino, u32, u32), (u32, BytesMut)>>>,
    
    // RAM buffer for blocks currently in-flight to disk/S3
    flushing_blocks: Arc<Mutex<std::collections::HashMap<(Ino, u32, u32), (u32, Bytes)>>>,
    
    // Memory cache for Attributes (size, mtime) to avoid Redb hits on every write
    dirty_attrs: Arc<Mutex<std::collections::HashMap<Ino, InodeAttr>>>,
    
    // Pending links (completed blocks not yet committed to metadata): ino -> list of links
    pending_links: Arc<Mutex<std::collections::HashMap<Ino, Vec<Objectlink>>>>,
    
    // Track active disk flush tasks
    active_flushes: Arc<Mutex<std::collections::HashMap<Ino, Vec<tokio::task::JoinHandle<VfsResult<()>>>>>>,
    
    // Limit global concurrent disk flushes (e.g. 32)
    flush_semaphore: Arc<tokio::sync::Semaphore>,
    
    // Sequential prefetch tracker: Ino -> last_requested_offset
    last_reads: Arc<Mutex<std::collections::HashMap<Ino, u64>>>,
    
    // Writeback Manager for persistent queuing
    writeback: Arc<WritebackManager>,
    
    // Sharded locks for commit_uploads serialization (Ino % 32)
    commit_locks: Vec<Arc<tokio::sync::Mutex<()>>>,
    
    // Links currently being committed (visible to Read during commit)
    committing_links: Arc<Mutex<std::collections::HashMap<Ino, Vec<Objectlink>>>>,
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
            flushing_blocks: Arc::new(Mutex::new(std::collections::HashMap::new())),
            dirty_attrs: Arc::new(Mutex::new(std::collections::HashMap::new())),
            pending_links: Arc::new(Mutex::new(std::collections::HashMap::new())),
            active_flushes: Arc::new(Mutex::new(std::collections::HashMap::new())),
            flush_semaphore: Arc::new(tokio::sync::Semaphore::new(32)),
            last_reads: Arc::new(Mutex::new(std::collections::HashMap::new())),
            writeback,
            
            // Initialize 32 commit locks
            commit_locks: (0..32).map(|_| Arc::new(tokio::sync::Mutex::new(()))).collect(),
            committing_links: Arc::new(Mutex::new(std::collections::HashMap::new())),
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
        let attr = self.getattr(ino)?;

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

        // SEQUENTIAL PREFETCH LOGIC
        {
            let mut last_reads = self.last_reads.lock();
            let last_off = last_reads.get(&ino).cloned().unwrap_or(0);
            
            // If this read starts where the last one ended (or within 4KB gap), it's sequential
            if offset == last_off || (offset > last_off && offset < last_off + 4096) {
                // Trigger prefetch for the next N blocks (e.g. 4 blocks = 16MB)
                let next_block_start = ((offset + size as u64 + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64) * BLOCK_SIZE as u64;
                for i in 0..4 {
                    let prefetch_off = next_block_start + (i as u64 * BLOCK_SIZE as u64);
                    if prefetch_off < attr.size {
                        self.prefetch_block(ino, prefetch_off, &attr);
                    }
                }
            }
            last_reads.insert(ino, offset + size as u64);
        }

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
            let mut block_data_mut: BytesMut;
            
            if let Some(cached) = self.mem_cache.get(&cache_key) {
                 block_data_mut = cached.into();
            } else {
                 let block_data = self.read_from_storage(ino, block_start, block_len).await?;
                 self.mem_cache.put(cache_key, block_data.clone());
                 block_data_mut = block_data.into();
            }

            let bkey = (ino, (block_start / CHUNK_SIZE) as u32, ((block_start % CHUNK_SIZE) / BLOCK_SIZE as u64) as u32);

            // 1. OVERLAY flushing_blocks (In-flight to disk)
            {
                if let Some((start_off, buffer)) = self.flushing_blocks.lock().get(&bkey) {
                    let start = *start_off as usize;
                    let len = buffer.len();
                    if start < block_data_mut.len() {
                        let to_copy = len.min(block_data_mut.len() - start);
                        block_data_mut[start..start + to_copy].copy_from_slice(&buffer[..to_copy]);
                    }
                }
            }

            // 2. OVERLAY pending_writes (RAM buffers) - most recent data wins
            {
                let pending = self.pending_writes.lock();
                if let Some((start_off, buffer)) = pending.get(&bkey) {
                    let start = *start_off as usize;
                    let len = buffer.len();
                    if start < block_data_mut.len() {
                        let to_copy = len.min(block_data_mut.len() - start);
                        block_data_mut[start..start + to_copy].copy_from_slice(&buffer[..to_copy]);
                    }
                }
            }
            
            let block_data = block_data_mut.freeze();

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

    /// Fetch a block asynchronously into the MemoryCache.
    fn prefetch_block(&self, ino: Ino, offset: u64, attr: &InodeAttr) {
        let cache_key = studiofs_cache::memory::CacheKey::new(ino, offset);
        if self.mem_cache.get(&cache_key).is_some() {
             return; // Already cached
        }
        
        let vfs = self.clone();
        let attr_clone = attr.clone();
        tokio::spawn(async move {
            let block_len = if offset + BLOCK_SIZE as u64 > attr_clone.size {
                 (attr_clone.size - offset) as usize
            } else {
                 BLOCK_SIZE as usize
            };
            if let Ok(data) = vfs.read_from_storage(ino, offset, block_len).await {
                 vfs.mem_cache.put(cache_key, data);
            }
        });
    }

    /// Resolve chunk/slice/block data from S3 storage, merging pending links.
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

    /// Resolve chunk/slice/block data from S3 storage, merging pending links using "Paint Layers".
    async fn read_from_storage(
        &self,
        ino: Ino,
        offset: u64,
        size: usize,
    ) -> VfsResult<Bytes> {
        // 1. Get Data Root from Meta
        let meta_root_id = self.meta.get_data_root(ino)?;

        // 2. Collect ALL links in LAYERS (Oldest -> Newest)
        // Layer 0: Metadata (Base)
        let mut meta_links = Vec::new();
        if let Some(root_id) = meta_root_id {
            if let Ok(root_bytes) = self.get_object_with_cache(&root_id).await {
                if let Ok(root_payload_bytes) = read_object(&root_bytes) {
                     if let Ok(root_payload) = ObjectPayload::from_bytes(&root_payload_bytes) {
                         match root_payload.kind {
                             ObjectKind::Meta => meta_links = root_payload.children,
                             ObjectKind::Data => {
                                 meta_links.push(Objectlink {
                                     offset: 0,
                                     length: root_payload.data.len() as u32,
                                     object_id: root_id,
                                 });
                             }
                         }
                     }
                }
            }
        }
        
        // Layer 1: Committing Links (Middle)
        // These are links that are currently being committed to metadata (race window)
        let committing_links_snapshot = {
            self.committing_links.lock().get(&ino).cloned().unwrap_or_default()
        };

        // Layer 2: Pending Links (Top)
        // These are links that have been flushed to disk but not yet committed
        let pending_links_snapshot = {
            self.pending_links.lock().get(&ino).cloned().unwrap_or_default()
        };

        // Combine into ordered layers
        // We want to process them in order: Meta -> Committing -> Pending.
        // But for efficient reading, we should gather ALL relevant links and fetch them.
        // The core idea of "Paint Layers":
        // Initialize buffer with 0.
        // Iterate layers from bottom up.
        // For each link in layer: copy data to buffer (overwriting previous).

        let end_offset = offset + size as u64;

        // Collect fetch tasks: (LayerIndex, LinkIndex, ObjectLink)
        // LayerIndex 0=Meta, 1=Committing, 2=Pending
        let mut fetch_tasks = Vec::new();
        
        let layers = vec![meta_links, committing_links_snapshot, pending_links_snapshot];
        
        for (layer_idx, layer) in layers.iter().enumerate() {
            for link in layer {
                // Check intersection
                let link_end = link.offset + link.length as u64;
                if !(end_offset <= link.offset || offset >= link_end) {
                    fetch_tasks.push((layer_idx, link.clone()));
                }
            }
        }

        if fetch_tasks.is_empty() {
             return Ok(BytesMut::zeroed(size).freeze());
        }

        // Parallel Fetch
        use futures::stream::{self, StreamExt};
        const CONCURRENCY: usize = 16;

        let fetch_stream = stream::iter(fetch_tasks)
            .map(|(layer_idx, link)| {
                let self_ref = &self;
                async move {
                    let child_bytes = self_ref.get_object_with_cache(&link.object_id).await?;
                    let child_payload_bytes = read_object(&child_bytes)
                        .map_err(|e| VfsError::Other(e.to_string()))?;
                    let child_payload = ObjectPayload::from_bytes(&child_payload_bytes)
                        .map_err(|e| VfsError::Other(e.to_string()))?;
                    Ok::<_, VfsError>((layer_idx, link, child_payload))
                }
            })
            .buffer_unordered(CONCURRENCY);

        // Collect results into a buffer to sort by LayerIndex
        let mut results = Vec::new();
        let mut stream = fetch_stream;
        while let Some(res) = stream.next().await {
            match res {
                Ok(val) => results.push(val),
                Err(e) => {
                    // Log error but continue? Or fail? 
                    // Fail prevents partial data returns which is safer.
                    return Err(e);
                }
            }
        }
        
        // Sort by LayerIndex (asc) then Offset (asc)
        // Stable sort to keep relative order if needed?
        // LayerIndex is crucial.
        results.sort_by_key(|(layer_idx, link, _)| (*layer_idx, link.offset));

        // PAINT
        let mut result = BytesMut::zeroed(size);
        for (_, link, payload) in results {
            if let ObjectKind::Data = payload.kind {
                let start = offset.max(link.offset);
                let end = (offset + size as u64).min(link.offset + link.length as u64);
                
                if start < end {
                    let len = (end - start) as usize;
                    let dest_off = (start - offset) as usize;
                    let src_off = (start - link.offset) as usize;
                    
                    if src_off + len <= payload.data.len() {
                        result[dest_off..dest_off + len].copy_from_slice(&payload.data[src_off..src_off + len]);
                    } else {
                        // Truncated payload?
                        warn!("payload data shorter than expected: {} < {}", payload.data.len(), src_off + len);
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
                    // Gap or jump! Flush current part.
                    let data = buffer.clone().freeze();
                    flush_gap_data = Some((data, *start_off));
                    
                    // Reset buffer to new start
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

                // Invalidate MemoryCache to ensure subsequent reads see new data
                let key_offset = (chunk_key.index as u64 * CHUNK_SIZE) + (block_idx_in_chunk as u64 * BLOCK_SIZE as u64);
                let cache_key = studiofs_cache::memory::CacheKey::new(ino, key_offset);
                self.mem_cache.remove(&cache_key);

                if buffer.len() >= BLOCK_SIZE as usize {
                    // ATOMIC TRANSFER to in-flight state
                    let data_bytes = buffer.clone().freeze();
                    self.flushing_blocks.lock().insert(key, (*start_off, data_bytes.clone()));
                    
                    flush_full_data = Some((data_bytes, *start_off));
                    pending.remove(&key);
                }
            } // UNLOCK

            // ASYNC FLUSH tasks
            if let Some((d, off)) = flush_gap_data {
                self.flushing_blocks.lock().insert((ino, chunk_key.index, block_idx_in_chunk), (off, d.clone()));
                self.flush_later(ino, chunk_key.index, block_idx_in_chunk, off, d);
            }
            if let Some((d, off)) = flush_full_data {
                // Already inserted into flushing_blocks inside the lock above
                self.flush_later(ino, chunk_key.index, block_idx_in_chunk, off, d);
            }

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
            
            // Remove from in-flight tracker
            vfs.flushing_blocks.lock().remove(&(ino, chunk_idx, block_idx));
            
            Ok::<(), VfsError>(())
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
            let mut flushing = self.flushing_blocks.lock();
            
            let keys: Vec<_> = pending.keys().filter(|(i, _, _)| *i == ino).cloned().collect();
            for key in keys {
                if let Some((start_off, data)) = pending.remove(&key) {
                    let (_, chunk_idx, block_idx) = key;
                    let data_frozen = data.freeze();
                    flushing.insert(key, (start_off, data_frozen.clone()));
                    entries_to_flush.push((chunk_idx, block_idx, start_off, data_frozen));
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
        
        // 4. Collect pending links
        // CRITICAL: Move to committing_links BEFORE removing from pending_links to avoid stale reads.
        let pending_links_vec = {
            let mut pending = self.pending_links.lock();
            let mut committing = self.committing_links.lock();
            
            if let Some(links) = pending.remove(&ino) {
                // Insert into committing_links so read() sees them during commit
                committing.insert(ino, links.clone());
                links
            } else {
                Vec::new()
            }
        };

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

        // 0. Acquire Commit Lock (Serialize metadata updates for this inode)
        let lock_idx = (ino % 32) as usize;
        let _commit_guard = self.commit_locks[lock_idx].lock().await;

        // 0b. Store in committing_links (Make visible to Read)
        {
            let mut committing = self.committing_links.lock();
            committing.insert(ino, new_links.clone());
        }
        
        // Ensure cleanup even on error (using a scope guard or defer would be better, but manual limits scope)
        // We will clear it at the end.

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

        // 2. Merge new links with OVERLAP HANDLING
        // We must process new links one by one.
        for new_link in new_links {
             let mut i = 0;
             while i < children.len() {
                 let child = &children[i];
                 let child_end = child.offset + child.length as u64;
                 let link_end = new_link.offset + new_link.length as u64;
                 
                 // Check overlap
                 if new_link.offset < child_end && link_end > child.offset {
                     // Overlap detected! We must SPLINTER the existing child.
                     // 1. Remove child
                     let child_to_splinter = children.remove(i);
                     
                     // 2. splinter it
                     // This is async, so we might need to be careful about iteration index?
                     // We are inside async fn, so await is fine.
                     let splinters = self.splinter_object(ino, child_to_splinter, &new_link).await?;
                     
                     // 3. Insert splinters
                     for s in splinters {
                         children.insert(i, s);
                         i += 1;
                     }
                     // Don't increment i here, effectively checking the *next* child in the list
                 } else {
                     i += 1;
                 }
             }
             children.push(new_link);
             // Sort to keep order for next pass
             children.sort_by_key(|c| c.offset);
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

        // 4. Clear committing_links
        {
            self.committing_links.lock().remove(&ino);
        }
        
        debug!("consumed uploads and updated root: ino={} new_root={}", ino, new_root_id);
        Ok(())
    }



    /// Split an existing object into parts that do NOT overlap with the `overwrite` link.
    /// Returns 0, 1, or 2 new links (Head and/or Tail).
    async fn splinter_object(&self, ino: Ino, original: Objectlink, overwrite: &Objectlink) -> VfsResult<Vec<Objectlink>> {
        let mut result = Vec::new();
        let orig_end = original.offset + original.length as u64;
        let over_end = overwrite.offset + overwrite.length as u64;

        // Check/Calculate Head (part of original BEFORE overwrite)
        let head_len = if overwrite.offset > original.offset {
            (overwrite.offset - original.offset) as u32
        } else { 0 };

        // Check/Calculate Tail (part of original AFTER overwrite)
        let tail_len = if over_end < orig_end {
            (orig_end - over_end) as u32
        } else { 0 };

        if head_len == 0 && tail_len == 0 {
            return Ok(result); // Completely covered, nothing to keep
        }

        // We need the data.
        let data_bytes = self.get_object_with_cache(&original.object_id).await?;
        let full_data = read_object(&data_bytes).map_err(|e| VfsError::Other(e.to_string()))?;
        
        // Sanity check
        if full_data.len() != original.length as usize {
            warn!("splinter: original object length mismatch expected {} got {}", original.length, full_data.len());
        }

        // Create HEAD
        if head_len > 0 {
            let len = head_len as usize;
            if len <= full_data.len() {
                let head_data = &full_data[0..len];
                let (hid, hbytes) = self.create_object_blob(head_data).await?;
                
                // Persist
                let hoid = u64::from_str_radix(&hid, 16).unwrap_or(0);
                self.disk_cache.put_object(hoid, &hbytes).await?;
                
                // Use correct file offset for journaling
                self.writeback.submit(ino, original.offset, hoid).await;
                             
                result.push(Objectlink {
                    offset: original.offset,
                    length: head_len,
                    object_id: hid,
                });
            }
        }

        // Create TAIL
        if tail_len > 0 {
            let start_in_obj = (original.length - tail_len) as usize;
            if start_in_obj < full_data.len() {
                let tail_data = &full_data[start_in_obj..];
                let (tid, tbytes) = self.create_object_blob(tail_data).await?;
                
                let toid = u64::from_str_radix(&tid, 16).unwrap_or(0);
                self.disk_cache.put_object(toid, &tbytes).await?;
                
                // Use correct file offset for journaling
                self.writeback.submit(ino, over_end, toid).await; 
                
                result.push(Objectlink {
                    offset: over_end,
                    length: tail_len,
                    object_id: tid,
                });
            }
        }

        Ok(result)
    }

    /// Helper to create object bytes (compressed/hashed) off-thread
    async fn create_object_blob(&self, data: &[u8]) -> VfsResult<(String, Vec<u8>)> {
        let data_vec = data.to_vec();
        tokio::task::spawn_blocking(move || {
             let payload = ObjectPayload::new_data(data_vec);
             let payload_bytes = payload.to_bytes().map_err(|e| VfsError::Other(e.to_string()))?;
             let object_bytes = create_object(&payload_bytes);
             let object_id = hash_object(&object_bytes);
             Ok((object_id, object_bytes))
        }).await?
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

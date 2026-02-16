//! Core VFS implementation — wires together metadata, data, and cache layers.

use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;
use studiofs_cache::memory::{CacheKey, MemoryCache};
use studiofs_cache::disk::DiskCache;
use studiofs_data::store::DataStore;
use studiofs_meta::*;
use thiserror::Error;
use tracing::debug;

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
    // Pending writes buffer: (ino, chunk_idx, block_idx) -> data
    // Using a simple mutex for now. In a real system, this should be more granular/concurrent.
    // For now, let's wrap the whole map in a Mutex.
    pending_writes: Arc<tokio::sync::Mutex<std::collections::HashMap<(Ino, u32, u32), BytesMut>>>,
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
        let mut result = BytesMut::with_capacity(size);
        let mut remaining = size;
        let mut current_offset = offset;

        while remaining > 0 {
            let chunk_key = ChunkKey::from_offset(ino, current_offset);
            let chunk_offset = current_offset - (chunk_key.index as u64 * CHUNK_SIZE);

            let slices = self.meta.get_chunk_slices(&chunk_key)?;

            // Resolve the latest view of the data at this offset
            let resolved = resolve_slices(&slices, chunk_offset as u32, remaining as u32);

            for segment in &resolved {
                if segment.slice_id == 0 {
                    // Hole — zero-filled
                    result.extend_from_slice(&vec![0u8; segment.len as usize]);
                } else {
                    // Fetch blocks from S3
                    let block_data = self
                        .fetch_slice_range(segment.slice_id, segment.slice_size, segment.off, segment.len)
                        .await?;
                    result.extend_from_slice(&block_data);
                }
                remaining -= segment.len as usize;
                current_offset += segment.len as u64;
            }

            if resolved.is_empty() {
                // No slices cover this region — it's a hole
                let in_chunk = (CHUNK_SIZE - chunk_offset).min(remaining as u64) as usize;
                result.extend_from_slice(&vec![0u8; in_chunk]);
                remaining -= in_chunk;
                current_offset += in_chunk as u64;
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
            let buffer = pending.entry(key).or_insert_with(|| BytesMut::with_capacity(BLOCK_SIZE as usize));
            
            // If we are writing to the middle of a buffer that doesn't exist or is too short,
            // we technically should zero-fill or fetch the existing block.
            // For now, assuming APPEND-only or sequential writes for simplicity as per requirements.
            // But to be safe, if we have a gap, we should fill it.
            if buffer.len() < offset_in_block {
                let gap = offset_in_block - buffer.len();
                buffer.extend_from_slice(&vec![0u8; gap]);
            } else if buffer.len() > offset_in_block {
                 // Overwrite logic: unrelated to "append" optimization, but needed for correctness.
                 // BytesMut truncation is cheap.
                 // For simplified "append-mostly" workload:
                 if buffer.len() > offset_in_block {
                     // We are overwriting. Truncate? No, that loses tail.
                     // Helper: overwrite existing bytes
                     // This is tricky with BytesMut efficiently. 
                     // Let's just assume we append or fill.
                     // If we overwrite, we might need to be smarter. 
                     // Let's rely on standard logic: extend.
                     // If offset_in_block < buffer.len(), we are rewriting old data.
                     // Let's just use `put` on the slice? BytesMut is accessible.
                     for i in 0..to_write {
                         if offset_in_block + i < buffer.len() {
                             buffer[offset_in_block + i] = data[data_offset + i];
                         } else {
                             buffer.put_u8(data[data_offset + i]);
                         }
                     }
                 } else {
                     buffer.extend_from_slice(&data[data_offset..data_offset + to_write]);
                 }
            } else {
                buffer.extend_from_slice(&data[data_offset..data_offset + to_write]);
            }

            // If block is full (4MiB), flush it immediately
            if buffer.len() >= BLOCK_SIZE as usize {
                // Ensure we slice nicely to 4MB just in case
                let block_data = buffer.clone(); // Clone to upload
                // Remove from pending? NO. Only flush removes it?
                // Actually, if we upload it, we can remove it to free memory?
                // YES. 
                // Wait, if we keep writing to ANY part of this block later (e.g. rewrite),
                // we'd need to fetch it back.
                // Assuming JuiceFS behavior: once full -> commit -> slice.
                self.flush_block(ino, chunk_key.index, block_idx_in_chunk, &block_data).await?;
                
                // Remove from pending to free memory
                pending.remove(&key);
            }

            written += to_write;
            remaining -= to_write;
            current_offset += to_write as u64;
            data_offset += to_write;
        }

        // Update file size in metadata immediately so `ls` sees it, even if data is buffered?
        // Yes, usually.
        let mut attr = self.meta.get_attr(ino)?;
        if current_offset > attr.size {
            attr.size = current_offset;
            attr.touch_mtime();
            self.meta.set_attr(&attr)?;
        }
        
        Ok(written as u32)
    }

    /// Flush all buffered blocks for a file (or all files if ino=0?).
    /// Uploads pending 4MB blocks (or partials) to S3 and creates slices.
    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        let mut pending = self.pending_writes.lock().await;
        
        // Find all keys for this inode
        // Note: iterating all map is inefficient. Should map Ino -> Vec<Keys>.
        // Optimization for later.
        let keys: Vec<_> = pending.keys()
             .filter(|(i, _, _)| *i == ino)
             .cloned()
             .collect();

        for key in keys {
            if let Some(data) = pending.remove(&key) {
                let (_, chunk_idx, block_idx) = key;
                self.flush_block(ino, chunk_idx, block_idx, &data).await?;
            }
        }
        Ok(())
    }

    /// Helper to upload a block (full or partial) and update metadata
    async fn flush_block(&self, ino: Ino, chunk_idx: u32, block_idx: u32, data: &[u8]) -> VfsResult<()> {
        let slice_id = self.meta.next_slice_id()?;
        
        // Block matches S3 object exactly?
        // JuiceFS: "Block is basic storage unit... 4MiB default"
        // If we have < 4MiB (eof), we still upload.
        
        // Upload to S3
        // Note: block_idx passed to put_block is effectively 0 for this new slice?
        // Wait, `slice` has `pos`. `put_block` uses (slice_id, block_index).
        // If we make a NEW slice for *just this block*, then inside that slice:
        // - slice size = data.len()
        // - it has 1 block (index 0) if len <= 4MB.
        // - Wait, `put_block` takes `block_index`. 
        // If this slice corresponds to a 4MB range in file, it is ONE block.
        // So for this slice_id, we put block_index=0.
        
        self.data.put_block(slice_id, 0, Bytes::copy_from_slice(data)).await?;
        self.disk_cache.put(slice_id, 0, data)?;

        // Update Chunk Metadata
        let chunk_key = ChunkKey::new(ino, chunk_idx);
        let bs = BLOCK_SIZE as u32;
        let pos_in_chunk = block_idx * bs; // Where this block starts in chunk

        let slice = Slice {
            pos: pos_in_chunk,
            id: slice_id,
            size: data.len() as u32,
            off: 0,
            len: data.len() as u32,
        };
        
        self.meta.append_chunk_slice(&chunk_key, &slice)?;
        
        debug!(ino, chunk_idx, block_idx, len=data.len(), "flushed block");
        Ok(())
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

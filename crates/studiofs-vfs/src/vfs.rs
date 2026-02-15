//! Core VFS implementation — wires together metadata, data, and cache layers.

use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use studiofs_cache::memory::{CacheKey, MemoryCache};
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
}

impl StudioVfs {
    /// Create a new VFS instance.
    pub fn new(
        meta: Arc<RedbMetaEngine>,
        data: Arc<DataStore>,
        mem_cache: Arc<MemoryCache>,
    ) -> Self {
        Self {
            meta,
            data,
            mem_cache,
        }
    }

    /// Get metadata engine reference.
    pub fn meta(&self) -> &RedbMetaEngine {
        &self.meta
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
        let data = self.read_from_storage(ino, offset, effective_size, &attr).await?;

        // Store in memory cache
        self.mem_cache.put(cache_key, data.clone());

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

            let block_data = if offset_in_block == 0 && to_read == block_actual_size {
                // Read full block
                self.data
                    .get_block(slice_id, block_idx, block_actual_size as u32)
                    .await?
            } else {
                // Read partial block (range request)
                self.data
                    .get_block_range(
                        slice_id,
                        block_idx,
                        block_actual_size as u32,
                        offset_in_block,
                        to_read,
                    )
                    .await?
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
    /// Creates a new slice for the written data and uploads blocks to S3.
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        let written = data.len() as u32;

        // Allocate a new slice ID
        let slice_id = self.meta.next_slice_id()?;

        // Split data into blocks and upload
        let blocks = studiofs_data::block::split_into_blocks(data, 0);
        for (block_idx, block_data) in blocks {
            self.data
                .put_block(slice_id, block_idx, block_data)
                .await?;
        }

        // Update metadata: append slice to appropriate chunk(s)
        let chunk_key = ChunkKey::from_offset(ino, offset);
        let pos_in_chunk = (offset % CHUNK_SIZE) as u32;

        let slice = Slice {
            pos: pos_in_chunk,
            id: slice_id,
            size: written,
            off: 0,
            len: written,
        };
        self.meta.append_chunk_slice(&chunk_key, &slice)?;

        // Update file size
        let mut attr = self.meta.get_attr(ino)?;
        let new_end = offset + written as u64;
        if new_end > attr.size {
            attr.size = new_end;
        }
        attr.touch_mtime();
        self.meta.set_attr(&attr)?;

        // Invalidate memory cache for affected range
        self.mem_cache.invalidate_inode(ino);

        debug!(ino, offset, written, slice_id, "wrote data");
        Ok(written)
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

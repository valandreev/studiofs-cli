//! Slab Store — persistent block storage using large mmap'd slab files.
//!
//! Instead of one file per cached block (thousands of files, 4+ syscalls per read),
//! blocks are packed into large "slab" files (default 2 GB each) that remain
//! permanently memory-mapped for the lifetime of the process.
//!
//! **Read path (hot):** HashMap lookup → pointer arithmetic → `Bytes::copy_from_slice`
//! - 0 syscalls per cache hit
//! - 1 memcpy per read (vs 3 in the old file-per-block design)
//! - No `spawn_blocking` needed
//!
//! **Write path:** find free region in a slab → memcpy into mmap → update index
//!
//! **Eviction:** LRU tracking per entry, free regions returned to slab free-list.

use bytes::Bytes;
use memmap2::MmapMut;
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

// =============================================================================
// Constants
// =============================================================================

/// Default slab file size: 2 GB.
const DEFAULT_SLAB_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Alignment for allocations within a slab (4 KB = page-aligned).
const ALLOC_ALIGN: u32 = 4096;

/// Index file magic bytes.
const INDEX_MAGIC: &[u8; 4] = b"SLBI";

/// Index file version.
const INDEX_VERSION: u32 = 1;

// =============================================================================
// Types
// =============================================================================

/// Object ID (sequential block ID).
pub type ObjectId = u64;

/// A reference to data stored within a slab.
#[derive(Debug, Clone, Copy)]
pub struct SlabEntry {
    /// Which slab file this block lives in.
    pub slab_id: u16,
    /// Byte offset within the slab.
    pub offset: u32,
    /// Length of the stored data.
    pub length: u32,
    /// Last access time for LRU eviction.
    pub last_access: Instant,
}

/// A free region within a slab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FreeRegion {
    offset: u32,
    length: u32,
}

/// A single slab file: a large memory-mapped file holding many cached blocks.
pub struct Slab {
    /// Raw pointer to the mmap'd region (stable for the lifetime of the Slab).
    /// We use a raw pointer + len so that `write_at` can work through `&self`
    /// (interior mutability). Safety: the free-list ensures non-overlapping writes.
    mmap_ptr: *mut u8,
    mmap_len: usize,
    /// Keep the MmapMut alive (dropped last).
    _mmap: MmapMut,
    /// Path to the slab file.
    #[allow(dead_code)]
    path: PathBuf,
    /// Slab capacity in bytes.
    capacity: u64,
    /// Free regions within this slab, ordered by offset.
    free_list: Mutex<Vec<FreeRegion>>,
    /// Total free bytes in this slab.
    free_bytes: AtomicU64,
}

// SAFETY: The free-list Mutex ensures non-overlapping writes. Reads only happen
// on regions that have been fully written and published to the index.
unsafe impl Send for Slab {}
unsafe impl Sync for Slab {}

impl Slab {
    /// Create or open a slab file of the given size.
    fn open_or_create(path: &Path, size: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let metadata = file.metadata()?;
        if metadata.len() < size {
            file.set_len(size)?;
        }

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let mmap_ptr = mmap.as_mut_ptr();
        let mmap_len = mmap.len();

        Ok(Self {
            mmap_ptr,
            mmap_len,
            _mmap: mmap,
            path: path.to_path_buf(),
            capacity: size,
            free_list: Mutex::new(vec![FreeRegion {
                offset: 0,
                length: size as u32,
            }]),
            free_bytes: AtomicU64::new(size),
        })
    }

    /// Open an existing slab file (for recovery).
    fn open_existing(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let size = metadata.len();
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let mmap_ptr = mmap.as_mut_ptr();
        let mmap_len = mmap.len();

        Ok(Self {
            mmap_ptr,
            mmap_len,
            _mmap: mmap,
            path: path.to_path_buf(),
            capacity: size,
            // Free list will be rebuilt from index
            free_list: Mutex::new(Vec::new()),
            free_bytes: AtomicU64::new(0),
        })
    }

    /// Allocate a region within the slab using first-fit.
    /// Returns (offset, aligned_length) or None if no space.
    fn allocate(&self, size: u32) -> Option<(u32, u32)> {
        let aligned_size = align_up(size, ALLOC_ALIGN);
        let mut free_list = self.free_list.lock();

        for i in 0..free_list.len() {
            if free_list[i].length >= aligned_size {
                let offset = free_list[i].offset;

                if free_list[i].length == aligned_size {
                    free_list.remove(i);
                } else {
                    free_list[i].offset += aligned_size;
                    free_list[i].length -= aligned_size;
                }

                self.free_bytes.fetch_sub(aligned_size as u64, Ordering::Relaxed);
                return Some((offset, aligned_size));
            }
        }

        None
    }

    /// Free a region within the slab, merging with adjacent free regions.
    fn free(&self, offset: u32, length: u32) {
        let aligned_length = align_up(length, ALLOC_ALIGN);
        let mut free_list = self.free_list.lock();

        let new_region = FreeRegion {
            offset,
            length: aligned_length,
        };

        // Find insertion point (keep sorted by offset)
        let pos = free_list
            .binary_search_by_key(&offset, |r| r.offset)
            .unwrap_or_else(|p| p);

        free_list.insert(pos, new_region);

        // Merge with next region
        if pos + 1 < free_list.len() {
            let cur_end = free_list[pos].offset + free_list[pos].length;
            if cur_end == free_list[pos + 1].offset {
                free_list[pos].length += free_list[pos + 1].length;
                free_list.remove(pos + 1);
            }
        }

        // Merge with previous region
        if pos > 0 {
            let prev_end = free_list[pos - 1].offset + free_list[pos - 1].length;
            if prev_end == free_list[pos].offset {
                free_list[pos - 1].length += free_list[pos].length;
                free_list.remove(pos);
            }
        }

        self.free_bytes
            .fetch_add(aligned_length as u64, Ordering::Relaxed);
    }

    /// Write data to a specific offset in the slab (memcpy into mmap).
    ///
    /// SAFETY: Caller must ensure `offset..offset+data.len()` is within an
    /// allocated (non-free) region. The free-list mutex guarantees this.
    fn write_at(&self, offset: u32, data: &[u8]) {
        let start = offset as usize;
        let end = start + data.len();
        assert!(end <= self.mmap_len, "write_at out of bounds");
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.mmap_ptr.add(start),
                data.len(),
            );
        }
    }

    /// Read data from a specific offset in the slab (zero-copy pointer).
    fn read_slice(&self, offset: u32, length: u32) -> &[u8] {
        let start = offset as usize;
        let end = start + length as usize;
        assert!(end <= self.mmap_len, "read_slice out of bounds");
        unsafe { std::slice::from_raw_parts(self.mmap_ptr.add(start), length as usize) }
    }

    /// Free bytes available in this slab.
    #[allow(dead_code)]
    fn free_bytes(&self) -> u64 {
        self.free_bytes.load(Ordering::Relaxed)
    }
}

// =============================================================================
// SlabStore
// =============================================================================

/// Persistent slab-based block store with LRU eviction.
///
/// Replaces the file-per-block DiskCache with large mmap'd slab files.
/// All reads are synchronous (no spawn_blocking needed).
pub struct SlabStore {
    /// Slab directory.
    slab_dir: PathBuf,
    /// All open slabs (permanently mmap'd).
    slabs: RwLock<Vec<Arc<Slab>>>,
    /// Object index: object_id → slab entry.
    index: RwLock<HashMap<ObjectId, SlabEntry>>,
    /// LRU ordering for eviction: (last_access, object_id).
    /// BTreeMap ensures oldest entries are first.
    lru: Mutex<BTreeMap<(Instant, ObjectId), ()>>,
    /// Set of pinned objects (prevent eviction).
    pinned: Mutex<HashSet<ObjectId>>,
    /// Current total data size (excluding alignment padding).
    total_data_bytes: AtomicU64,
    /// Maximum total size.
    max_size: u64,
    /// Minimum total size (don't evict below this).
    min_size: u64,
    /// Default slab file size.
    slab_file_size: u64,
}

impl SlabStore {
    /// Create or open a slab store at the given directory.
    pub fn open(slab_dir: &Path, max_size: u64, min_size: u64) -> io::Result<Self> {
        fs::create_dir_all(slab_dir)?;

        let store = Self {
            slab_dir: slab_dir.to_path_buf(),
            slabs: RwLock::new(Vec::new()),
            index: RwLock::new(HashMap::new()),
            lru: Mutex::new(BTreeMap::new()),
            pinned: Mutex::new(HashSet::new()),
            total_data_bytes: AtomicU64::new(0),
            max_size,
            min_size,
            slab_file_size: DEFAULT_SLAB_SIZE.min(max_size), // Don't create slabs bigger than max
        };

        // Load existing index and slabs
        store.recover()?;

        Ok(store)
    }

    /// Read an object from the store. Returns None if not found.
    ///
    /// This is the **hot path** — no syscalls, no spawn_blocking.
    /// Just a HashMap lookup + memcpy from the persistent mmap.
    pub fn get(&self, id: ObjectId) -> Option<Bytes> {
        // Look up in index
        let entry = {
            let idx = self.index.read();
            idx.get(&id).copied()?
        };

        // Read from slab — just pointer arithmetic on the persistent mmap
        let slabs = self.slabs.read();
        if entry.slab_id as usize >= slabs.len() {
            warn!("slab_id {} out of range for object {:016x}", entry.slab_id, id);
            return None;
        }
        let slab = &slabs[entry.slab_id as usize];
        let data = slab.read_slice(entry.offset, entry.length);
        let bytes = Bytes::copy_from_slice(data);

        // Update LRU
        self.touch_lru(id, entry.last_access);

        Some(bytes)
    }

    /// Write an object to the store.
    /// Evicts LRU entries if needed to make room.
    pub fn put(&self, id: ObjectId, data: &[u8]) -> io::Result<()> {
        let data_len = data.len() as u32;

        // If already exists, remove old entry first
        if self.index.read().contains_key(&id) {
            self.remove(id);
        }

        // Evict until we have space
        self.evict_for(data_len as u64)?;

        // Try to allocate in existing slabs
        let (slab_id, offset, aligned_len) = self.allocate_in_slab(data_len)?;

        // Write data to slab
        {
            let slabs = self.slabs.read();
            slabs[slab_id as usize].write_at(offset, data);
        }

        // Register in index
        let now = Instant::now();
        let entry = SlabEntry {
            slab_id,
            offset,
            length: data_len,
            last_access: now,
        };

        {
            let mut idx = self.index.write();
            idx.insert(id, entry);
        }

        // Update LRU
        self.lru.lock().insert((now, id), ());

        // Update total size
        self.total_data_bytes
            .fetch_add(aligned_len as u64, Ordering::Relaxed);

        debug!(
            "[SLAB-PUT] id={:016x} slab={} offset={} len={}",
            id, slab_id, offset, data_len
        );

        Ok(())
    }

    /// Remove an object from the store.
    pub fn remove(&self, id: ObjectId) {
        let entry = {
            let mut idx = self.index.write();
            idx.remove(&id)
        };

        if let Some(entry) = entry {
            // Free the region in the slab
            let slabs = self.slabs.read();
            if (entry.slab_id as usize) < slabs.len() {
                let aligned = align_up(entry.length, ALLOC_ALIGN);
                slabs[entry.slab_id as usize].free(entry.offset, entry.length);
                self.total_data_bytes
                    .fetch_sub(aligned as u64, Ordering::Relaxed);
            }

            // Remove from LRU
            self.lru.lock().remove(&(entry.last_access, id));

            debug!("[SLAB-REMOVE] id={:016x} freed {} bytes", id, entry.length);
        }
    }

    /// Check if an object exists.
    pub fn contains(&self, id: ObjectId) -> bool {
        self.index.read().contains_key(&id)
    }

    /// Pin an object to prevent eviction.
    pub fn pin(&self, id: ObjectId) {
        self.pinned.lock().insert(id);
    }

    /// Unpin an object.
    pub fn unpin(&self, id: ObjectId) {
        self.pinned.lock().remove(&id);
    }

    /// Check if an object is pinned.
    pub fn is_pinned(&self, id: ObjectId) -> bool {
        self.pinned.lock().contains(&id)
    }

    /// Current total data bytes (excluding alignment padding counted slightly differently).
    pub fn total_bytes(&self) -> u64 {
        self.total_data_bytes.load(Ordering::Relaxed)
    }

    /// Total number of entries.
    pub fn entry_count(&self) -> usize {
        self.index.read().len()
    }

    // =========================================================================
    // Internal: Allocation
    // =========================================================================

    /// Try to allocate space in existing slabs, creating a new slab if needed.
    fn allocate_in_slab(&self, size: u32) -> io::Result<(u16, u32, u32)> {
        // Try existing slabs first
        {
            let slabs = self.slabs.read();
            for (i, slab) in slabs.iter().enumerate() {
                if let Some((offset, aligned)) = slab.allocate(size) {
                    return Ok((i as u16, offset, aligned));
                }
            }
        }

        // No space — create a new slab
        let slab_id = self.create_slab()?;

        let slabs = self.slabs.read();
        if let Some((offset, aligned)) = slabs[slab_id as usize].allocate(size) {
            Ok((slab_id, offset, aligned))
        } else {
            Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("failed to allocate {} bytes in new slab", size),
            ))
        }
    }

    /// Create a new slab file.
    fn create_slab(&self) -> io::Result<u16> {
        let mut slabs = self.slabs.write();
        let slab_id = slabs.len() as u16;
        let path = self.slab_dir.join(format!("slab_{:04}.dat", slab_id));

        info!(
            "[SLAB-CREATE] slab_{:04}.dat size={}MB",
            slab_id,
            self.slab_file_size / (1024 * 1024)
        );

        let slab = Slab::open_or_create(&path, self.slab_file_size)?;
        slabs.push(Arc::new(slab));

        Ok(slab_id)
    }

    // =========================================================================
    // Internal: LRU / Eviction
    // =========================================================================

    /// Update LRU timestamp for an object.
    fn touch_lru(&self, id: ObjectId, old_instant: Instant) {
        let now = Instant::now();
        let mut lru = self.lru.lock();
        lru.remove(&(old_instant, id));
        lru.insert((now, id), ());
        drop(lru);

        // Update entry's last_access
        if let Some(entry) = self.index.write().get_mut(&id) {
            entry.last_access = now;
        }
    }

    /// Evict LRU entries until we have room for `needed` bytes.
    pub fn evict_for(&self, needed: u64) -> io::Result<()> {
        let mut evicted_total = 0u64;

        loop {
            let current = self.total_data_bytes.load(Ordering::Relaxed);
            if current + needed <= self.max_size {
                break;
            }

            // Don't evict below minimum
            if current <= self.min_size {
                warn!(
                    "slab store at minimum size ({}MB), cannot evict",
                    self.min_size / (1024 * 1024)
                );
                break;
            }

            // Find oldest non-pinned entry
            let victim = {
                let lru = self.lru.lock();
                let pinned = self.pinned.lock();
                let mut found = None;
                for ((instant, id), _) in lru.iter() {
                    if !pinned.contains(id) {
                        found = Some((*instant, *id));
                        break;
                    }
                }
                found
            };

            match victim {
                Some((_instant, id)) => {
                    let size_before = self.total_data_bytes.load(Ordering::Relaxed);
                    self.remove(id);
                    let freed = size_before - self.total_data_bytes.load(Ordering::Relaxed);
                    evicted_total += freed;
                }
                None => break, // No evictable entries
            }
        }

        if evicted_total > 0 {
            info!(
                "[SLAB-EVICT] evicted {} bytes ({} entries remain)",
                evicted_total,
                self.entry_count()
            );
        }

        Ok(())
    }

    // =========================================================================
    // Internal: Persistence / Recovery
    // =========================================================================

    /// Recover state from existing slab files and index.
    fn recover(&self) -> io::Result<()> {
        let index_path = self.slab_dir.join("index.bin");
        if !index_path.exists() {
            debug!("[SLAB-RECOVER] no index.bin found, starting fresh");
            return Ok(());
        }

        // Read index
        let index_data = fs::read(&index_path)?;
        if index_data.len() < 8 {
            warn!("[SLAB-RECOVER] index.bin too small, starting fresh");
            return Ok(());
        }

        // Validate magic + version
        if &index_data[0..4] != INDEX_MAGIC {
            warn!("[SLAB-RECOVER] invalid index magic, starting fresh");
            return Ok(());
        }
        let version = u32::from_le_bytes(index_data[4..8].try_into().unwrap());
        if version != INDEX_VERSION {
            warn!(
                "[SLAB-RECOVER] unsupported index version {}, starting fresh",
                version
            );
            return Ok(());
        }

        // Read entry count
        if index_data.len() < 12 {
            return Ok(());
        }
        let entry_count = u32::from_le_bytes(index_data[8..12].try_into().unwrap()) as usize;

        // Read slab count
        if index_data.len() < 16 {
            return Ok(());
        }
        let slab_count = u32::from_le_bytes(index_data[12..16].try_into().unwrap()) as usize;

        // Open slab files
        let mut slabs = self.slabs.write();
        for i in 0..slab_count {
            let path = self.slab_dir.join(format!("slab_{:04}.dat", i));
            if !path.exists() {
                warn!("[SLAB-RECOVER] slab_{:04}.dat missing, stopping recovery", i);
                break;
            }
            let slab = Slab::open_existing(&path)?;
            slabs.push(Arc::new(slab));
        }

        // Read index entries
        // Each entry: object_id(8) + slab_id(2) + offset(4) + length(4) = 18 bytes
        const ENTRY_SIZE: usize = 18;
        let entries_start = 16;
        let entries_end = entries_start + entry_count * ENTRY_SIZE;
        if index_data.len() < entries_end {
            warn!("[SLAB-RECOVER] index truncated, loaded partial");
        }

        let mut idx = self.index.write();
        let mut lru = self.lru.lock();
        let mut total_data = 0u64;
        let now = Instant::now();

        let actual_entries = (index_data.len() - entries_start) / ENTRY_SIZE;
        let load_count = actual_entries.min(entry_count);

        for i in 0..load_count {
            let base = entries_start + i * ENTRY_SIZE;
            let object_id =
                u64::from_le_bytes(index_data[base..base + 8].try_into().unwrap());
            let slab_id =
                u16::from_le_bytes(index_data[base + 8..base + 10].try_into().unwrap());
            let offset =
                u32::from_le_bytes(index_data[base + 10..base + 14].try_into().unwrap());
            let length =
                u32::from_le_bytes(index_data[base + 14..base + 18].try_into().unwrap());

            if (slab_id as usize) >= slabs.len() {
                continue; // Skip entries referencing missing slabs
            }

            let entry = SlabEntry {
                slab_id,
                offset,
                length,
                last_access: now,
            };

            idx.insert(object_id, entry);
            lru.insert((now, object_id), ());
            total_data += align_up(length, ALLOC_ALIGN) as u64;
        }

        self.total_data_bytes.store(total_data, Ordering::Relaxed);

        // Rebuild free lists for each slab from the allocated regions
        self.rebuild_free_lists(&slabs, &idx);

        drop(lru);
        drop(idx);
        drop(slabs);

        info!(
            "[SLAB-RECOVER] loaded {} entries across {} slabs, total={} MB",
            load_count,
            slab_count,
            total_data / (1024 * 1024)
        );

        Ok(())
    }

    /// Rebuild slab free lists from the index (called during recovery).
    fn rebuild_free_lists(
        &self,
        slabs: &[Arc<Slab>],
        index: &HashMap<ObjectId, SlabEntry>,
    ) {
        // Collect allocated regions per slab
        let mut slab_allocated: Vec<Vec<(u32, u32)>> = vec![Vec::new(); slabs.len()];
        for entry in index.values() {
            if (entry.slab_id as usize) < slabs.len() {
                let aligned = align_up(entry.length, ALLOC_ALIGN);
                slab_allocated[entry.slab_id as usize].push((entry.offset, aligned));
            }
        }

        for (i, slab) in slabs.iter().enumerate() {
            let mut regions = slab_allocated[i].clone();
            regions.sort_by_key(|(offset, _)| *offset);

            let mut free_list = Vec::new();
            let mut cursor = 0u32;
            let cap = slab.capacity as u32;

            for (offset, length) in &regions {
                if *offset > cursor {
                    free_list.push(FreeRegion {
                        offset: cursor,
                        length: offset - cursor,
                    });
                }
                cursor = offset + length;
            }

            // Remaining space after last allocation
            if cursor < cap {
                free_list.push(FreeRegion {
                    offset: cursor,
                    length: cap - cursor,
                });
            }

            let total_free: u64 = free_list.iter().map(|r| r.length as u64).sum();
            *slab.free_list.lock() = free_list;
            slab.free_bytes.store(total_free, Ordering::Relaxed);
        }
    }

    /// Persist the index to disk (called periodically and on shutdown).
    pub fn persist_index(&self) -> io::Result<()> {
        let idx = self.index.read();
        let slabs = self.slabs.read();
        let slab_count = slabs.len();
        drop(slabs);

        let entry_count = idx.len();
        const ENTRY_SIZE: usize = 18;
        let total_size = 16 + entry_count * ENTRY_SIZE;

        let mut buf = Vec::with_capacity(total_size);

        // Header
        buf.extend_from_slice(INDEX_MAGIC);
        buf.extend_from_slice(&INDEX_VERSION.to_le_bytes());
        buf.extend_from_slice(&(entry_count as u32).to_le_bytes());
        buf.extend_from_slice(&(slab_count as u32).to_le_bytes());

        // Entries
        for (object_id, entry) in idx.iter() {
            buf.extend_from_slice(&object_id.to_le_bytes());
            buf.extend_from_slice(&entry.slab_id.to_le_bytes());
            buf.extend_from_slice(&entry.offset.to_le_bytes());
            buf.extend_from_slice(&entry.length.to_le_bytes());
        }

        drop(idx);

        // Atomic write
        let index_path = self.slab_dir.join("index.bin");
        let tmp_path = self.slab_dir.join("index.bin.tmp");
        fs::write(&tmp_path, &buf)?;
        fs::rename(&tmp_path, &index_path)?;

        debug!(
            "[SLAB-PERSIST] saved {} entries, {} slabs",
            entry_count, slab_count
        );

        Ok(())
    }
}

// =============================================================================
// Utility
// =============================================================================

/// Align a value up to the given alignment.
#[inline]
fn align_up(value: u32, align: u32) -> u32 {
    (value + align - 1) & !(align - 1)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
        assert_eq!(align_up(8000, 4096), 8192);
    }

    #[test]
    fn test_free_region_merge() {
        let dir = tempfile::tempdir().unwrap();
        let slab = Slab::open_or_create(&dir.path().join("test.dat"), 1024 * 1024).unwrap();

        // Allocate three regions
        let (off1, len1) = slab.allocate(4096).unwrap();
        let (off2, len2) = slab.allocate(4096).unwrap();
        let (off3, _len3) = slab.allocate(4096).unwrap();

        assert_eq!(off1, 0);
        assert_eq!(off2, 4096);
        assert_eq!(off3, 8192);

        // Free middle region
        slab.free(off2, 4096);
        assert_eq!(slab.free_list.lock().len(), 2); // gap + tail

        // Free first region — should merge with gap
        slab.free(off1, 4096);
        // After merging: [0..8192) free, then [12288..) free => 2 regions
        let fl = slab.free_list.lock();
        assert_eq!(fl.len(), 2);
        assert_eq!(fl[0].offset, 0);
        assert_eq!(fl[0].length, len1 + len2); // Merged
    }

    #[test]
    fn test_slab_store_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = SlabStore::open(
            &dir.path().join("slabs"),
            100 * 1024 * 1024, // 100 MB max
            1024 * 1024,       // 1 MB min
        )
        .unwrap();

        // Put and get
        let data = vec![42u8; 8192];
        store.put(1, &data).unwrap();
        let got = store.get(1).unwrap();
        assert_eq!(got.as_ref(), &data[..]);

        // Overwrite
        let data2 = vec![99u8; 4096];
        store.put(1, &data2).unwrap();
        let got2 = store.get(1).unwrap();
        assert_eq!(got2.as_ref(), &data2[..]);

        // Remove
        store.remove(1);
        assert!(store.get(1).is_none());
    }

    #[test]
    fn test_slab_store_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let slab_dir = dir.path().join("slabs");

        // Write data
        {
            let store = SlabStore::open(&slab_dir, 100 * 1024 * 1024, 1024 * 1024).unwrap();
            store.put(100, &[1u8; 4096]).unwrap();
            store.put(200, &[2u8; 8192]).unwrap();
            store.put(300, &[3u8; 16384]).unwrap();
            store.persist_index().unwrap();
        }

        // Re-open and verify
        {
            let store = SlabStore::open(&slab_dir, 100 * 1024 * 1024, 1024 * 1024).unwrap();
            assert_eq!(store.entry_count(), 3);

            let d1 = store.get(100).unwrap();
            assert_eq!(d1.len(), 4096);
            assert!(d1.iter().all(|&b| b == 1));

            let d2 = store.get(200).unwrap();
            assert_eq!(d2.len(), 8192);
            assert!(d2.iter().all(|&b| b == 2));

            let d3 = store.get(300).unwrap();
            assert_eq!(d3.len(), 16384);
            assert!(d3.iter().all(|&b| b == 3));
        }
    }

    #[test]
    fn test_slab_store_eviction() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny max_size to force eviction
        let store = SlabStore::open(
            &dir.path().join("slabs"),
            32 * 4096, // ~128 KB max
            0,         // No minimum
        )
        .unwrap();

        // Fill cache
        for i in 0..32 {
            store.put(i, &[i as u8; 4096]).unwrap();
        }

        // Adding more should evict old entries
        store.put(100, &[0xAA; 4096]).unwrap();
        assert!(store.get(100).is_some());
        // Some early entries should have been evicted
        assert!(store.entry_count() <= 32);
    }

    #[test]
    fn test_slab_store_pinning() {
        let dir = tempfile::tempdir().unwrap();
        let store = SlabStore::open(
            &dir.path().join("slabs"),
            8 * 4096, // Tiny
            0,
        )
        .unwrap();

        // Fill and pin first entry
        store.put(1, &[1u8; 4096]).unwrap();
        store.pin(1);

        // Fill rest
        for i in 2..=8 {
            store.put(i, &[i as u8; 4096]).unwrap();
        }

        // Add one more — should NOT evict pinned entry 1
        store.put(99, &[99u8; 4096]).unwrap();

        // Entry 1 should still exist (pinned)
        assert!(store.get(1).is_some());
    }

    #[test]
    fn test_slab_large_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = SlabStore::open(
            &dir.path().join("slabs"),
            100 * 1024 * 1024,
            0,
        )
        .unwrap();

        // Write a 4MB block (typical block size)
        let data = vec![0xBBu8; 4 * 1024 * 1024];
        store.put(42, &data).unwrap();

        let got = store.get(42).unwrap();
        assert_eq!(got.len(), 4 * 1024 * 1024);
        assert!(got.iter().all(|&b| b == 0xBB));
    }
}

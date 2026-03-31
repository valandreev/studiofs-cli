//! Single-file block cache with a native free-space allocator.
//!
//! `BlockStore` is the Cache v2 foundation: one `blocks.data` file, a
//! [`FreeList`] allocator, and an in-memory entry map.  This first version
//! uses pread/pwrite for all I/O; the mmap read path comes in a later issue.
//!
//! Thread-safe: concurrent readers via `RwLock`, writes serialised through
//! the allocator `Mutex`.

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tracing::warn;

use crate::v2::allocator::{AllocError, FreeList};
use crate::v2::format::{
    self, BLOCK_HEADER_SIZE, BlockHeader, FORMAT_VERSION, SUPERBLOCK_SIZE, Superblock,
    alloc_size_for,
};
use crate::v2::platform;

// ===========================================================================
// Configuration
// ===========================================================================

/// Configuration for opening a [`BlockStore`].
#[derive(Debug, Clone)]
pub struct BlockStoreConfig {
    /// Directory for cache files (`blocks.data` will be created here).
    pub cache_dir: PathBuf,
    /// Maximum size of the data file in bytes.
    pub max_size: u64,
    /// Volume UUID — validated on open to prevent cross-volume cache reuse.
    pub volume_uuid: [u8; 16],
}

// ===========================================================================
// Errors
// ===========================================================================

/// Errors returned by [`BlockStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum BlockStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error(
        "cache full: need {needed} bytes, limit {max_size} \
         (allocated {allocated}, free {free})"
    )]
    CacheFull {
        needed: u64,
        max_size: u64,
        allocated: u64,
        free: u64,
    },
    #[error("corrupt superblock: {0}")]
    CorruptSuperblock(String),
    #[error("format version mismatch: expected {expected}, found {found}")]
    VersionMismatch { expected: u32, found: u32 },
    #[error("volume UUID mismatch")]
    VolumeUuidMismatch,
    #[error(
        "max_size shrunk: superblock records {superblock_max} but config specifies {config_max}"
    )]
    MaxSizeShrunk {
        superblock_max: u64,
        config_max: u64,
    },
}

// ===========================================================================
// Internal types
// ===========================================================================

/// In-memory metadata for a single cached block.
#[derive(Debug, Clone, Copy)]
struct BlockEntry {
    /// Byte offset of this block's header in the data file.
    offset: u64,
    /// Actual payload size (bytes).
    data_size: u32,
    /// Total allocated size on disk (page-aligned, includes header + padding).
    alloc_size: u32,
    /// CRC-32 of the payload (for read-time verification).
    checksum: u32,
    /// Flags stored in the on-disk header (DIRTY, PINNED, …).
    flags: u8,
}

// ===========================================================================
// BlockStore
// ===========================================================================

/// Single-file block cache with a built-in allocator.
///
/// One instance per mount.  Thread-safe for concurrent get/put/remove.
pub struct BlockStore {
    file: File,
    config: BlockStoreConfig,
    allocator: Mutex<FreeList>,
    entries: RwLock<HashMap<u64, BlockEntry>>,
    dirty_count: AtomicU32,
    block_count: AtomicU64,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
}

impl BlockStore {
    /// Create or open a block store in `config.cache_dir`.
    ///
    /// * **New file** — writes a fresh superblock and starts empty.
    /// * **Existing file** — validates the superblock (magic, version, UUID).
    ///   Because index persistence is not yet implemented (see SFS-136), the
    ///   in-memory index starts empty and any pre-existing data regions are
    ///   treated as reclaimable free space.
    pub fn open(config: BlockStoreConfig) -> Result<Self, BlockStoreError> {
        std::fs::create_dir_all(&config.cache_dir)?;
        let data_path = config.cache_dir.join("blocks.data");

        let (file, file_len) = if data_path.exists() {
            let file = OpenOptions::new().read(true).write(true).open(&data_path)?;
            let len = file.metadata()?.len();

            if len < SUPERBLOCK_SIZE {
                return Err(BlockStoreError::CorruptSuperblock(
                    "file shorter than superblock".into(),
                ));
            }

            // Validate superblock.
            let mut sb_buf = [0u8; 4096];
            platform::pread(&file, &mut sb_buf, 0)?;
            let sb = Superblock::from_bytes(&sb_buf)
                .map_err(|e| BlockStoreError::CorruptSuperblock(e.to_string()))?;

            if sb.version != FORMAT_VERSION {
                return Err(BlockStoreError::VersionMismatch {
                    expected: FORMAT_VERSION,
                    found: sb.version,
                });
            }
            if sb.volume_uuid != config.volume_uuid {
                return Err(BlockStoreError::VolumeUuidMismatch);
            }
            if config.max_size < sb.max_size {
                return Err(BlockStoreError::MaxSizeShrunk {
                    superblock_max: sb.max_size,
                    config_max: config.max_size,
                });
            }

            (file, len)
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&data_path)?;

            // Write superblock.
            let sb = Superblock::new(config.max_size, config.volume_uuid);
            platform::pwrite(&file, &sb.to_bytes(), 0)?;
            file.set_len(SUPERBLOCK_SIZE)?;
            platform::fdatasync(&file)?;

            (file, SUPERBLOCK_SIZE)
        };

        // No index persistence yet — treat all data-region bytes as free.
        let file_end = std::cmp::max(file_len, SUPERBLOCK_SIZE);
        let allocator = if file_end > SUPERBLOCK_SIZE {
            warn!(
                data_bytes = file_end - SUPERBLOCK_SIZE,
                "opening non-empty blocks.data without index persistence; \
                 all existing data regions will be treated as free space \
                 (index persistence coming in SFS-136)",
            );
            FreeList::recover(config.max_size, file_end, &[])
        } else {
            FreeList::new(config.max_size)
        };

        Ok(Self {
            file,
            config,
            allocator: Mutex::new(allocator),
            entries: RwLock::new(HashMap::new()),
            dirty_count: AtomicU32::new(0),
            block_count: AtomicU64::new(0),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        })
    }

    /// Read a block from the cache.
    ///
    /// Returns `None` on cache miss or data-integrity failure.
    /// Atomically bumps the hit/miss counter.
    pub fn get(&self, object_id: u64) -> Option<Bytes> {
        let entry = {
            let entries = self.entries.read();
            *entries.get(&object_id)?
        };

        let mut buf = vec![0u8; entry.data_size as usize];
        if let Err(e) = platform::pread(
            &self.file,
            &mut buf,
            entry.offset + BLOCK_HEADER_SIZE as u64,
        ) {
            warn!(object_id, offset = entry.offset, %e, "block pread failed");
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Verify checksum.
        let actual_crc = crc32fast::hash(&buf);
        if actual_crc != entry.checksum {
            warn!(
                object_id,
                offset = entry.offset,
                expected = entry.checksum,
                actual = actual_crc,
                "block checksum mismatch",
            );
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        self.hit_count.fetch_add(1, Ordering::Relaxed);
        Some(Bytes::from(buf))
    }

    /// Check whether a block exists without reading its data.
    pub fn contains(&self, object_id: u64) -> bool {
        self.entries.read().contains_key(&object_id)
    }

    /// Write a block to the cache.
    ///
    /// * `evictable = false` marks the block as *dirty* (not eligible for
    ///   eviction until the caller calls [`mark_evictable`]).
    /// * If the block already exists it is replaced (remove + re-put).
    /// * Returns [`BlockStoreError::CacheFull`] when there is no room and
    ///   nothing can be reclaimed.
    pub fn put(&self, object_id: u64, data: &[u8], evictable: bool) -> Result<(), BlockStoreError> {
        let data_size = data.len() as u32;
        let alloc = alloc_size_for(data_size);

        // Remove if already present (remove is a no-op for missing keys,
        // saving one RwLock read compared to contains()+remove()).
        self.remove(object_id);

        // Allocate space + ensure the file is large enough (under lock).
        let offset = {
            let mut alloc_guard = self.allocator.lock();
            let offset = alloc_guard.allocate(alloc).map_err(|e| match e {
                AllocError::CacheFull {
                    needed,
                    max_size,
                    allocated,
                    free,
                } => BlockStoreError::CacheFull {
                    needed,
                    max_size,
                    allocated,
                    free,
                },
            })?;
            let needed_len = alloc_guard.file_end();
            drop(alloc_guard);

            let current_len = self.file.metadata()?.len();
            if needed_len > current_len {
                self.file.set_len(needed_len)?;
            }
            offset
        };

        // Write header + payload via pwrite (concurrent-safe, no seek).
        let checksum = crc32fast::hash(data);
        let flags = if evictable { 0 } else { format::FLAG_DIRTY };
        let header = BlockHeader::new(object_id, data_size, checksum, flags);
        platform::pwrite(&self.file, &header.to_bytes(), offset)?;
        if !data.is_empty() {
            platform::pwrite(&self.file, data, offset + BLOCK_HEADER_SIZE as u64)?;
        }

        // Record in-memory entry.
        {
            let mut entries = self.entries.write();
            entries.insert(
                object_id,
                BlockEntry {
                    offset,
                    data_size,
                    alloc_size: alloc as u32,
                    checksum,
                    flags,
                },
            );
        }
        self.block_count.fetch_add(1, Ordering::Relaxed);
        if !evictable {
            self.dirty_count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Mark a dirty block as evictable (e.g. after a successful S3 upload).
    pub fn mark_evictable(&self, object_id: u64) {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.get_mut(&object_id) {
            if entry.flags & format::FLAG_DIRTY != 0 {
                entry.flags &= !format::FLAG_DIRTY;
                self.dirty_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Remove a single block from the cache.
    pub fn remove(&self, object_id: u64) {
        let entry = {
            let mut entries = self.entries.write();
            entries.remove(&object_id)
        };
        if let Some(entry) = entry {
            self.allocator
                .lock()
                .free(entry.offset, entry.alloc_size as u64);
            self.block_count.fetch_sub(1, Ordering::Relaxed);
            if entry.flags & format::FLAG_DIRTY != 0 {
                self.dirty_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Remove multiple blocks in one call.
    pub fn remove_batch(&self, object_ids: &[u64]) {
        let removed: Vec<BlockEntry> = {
            let mut entries = self.entries.write();
            object_ids
                .iter()
                .filter_map(|id| entries.remove(id))
                .collect()
        };
        if removed.is_empty() {
            return;
        }

        let mut alloc = self.allocator.lock();
        let mut dirty_removed = 0u32;
        for entry in &removed {
            alloc.free(entry.offset, entry.alloc_size as u64);
            if entry.flags & format::FLAG_DIRTY != 0 {
                dirty_removed += 1;
            }
        }
        drop(alloc);

        self.block_count
            .fetch_sub(removed.len() as u64, Ordering::Relaxed);
        if dirty_removed > 0 {
            self.dirty_count.fetch_sub(dirty_removed, Ordering::Relaxed);
        }
    }

    /// Flush the data file to stable storage (`fdatasync`).
    pub fn sync_data(&self) -> Result<(), BlockStoreError> {
        platform::fdatasync(&self.file)?;
        Ok(())
    }

    /// Graceful shutdown: sync data and drop the file handle.
    pub fn close(self) -> Result<(), BlockStoreError> {
        platform::fdatasync(&self.file)?;
        Ok(())
    }

    // -- Metrics --

    /// Bytes currently allocated to live blocks (including headers + padding).
    pub fn allocated_bytes(&self) -> u64 {
        self.allocator.lock().allocated_bytes()
    }

    /// Number of blocks currently in the cache.
    pub fn block_count(&self) -> u64 {
        self.block_count.load(Ordering::Relaxed)
    }

    /// Number of dirty (non-evictable) blocks.
    pub fn dirty_count(&self) -> u32 {
        self.dirty_count.load(Ordering::Relaxed)
    }

    /// Total cache hits since open.
    pub fn hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Total cache misses since open.
    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    /// Path to the data file.
    pub fn data_path(&self) -> PathBuf {
        self.config.cache_dir.join("blocks.data")
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> BlockStoreConfig {
        BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: 64 * 1024 * 1024, // 64 MiB
            volume_uuid: [0xAA; 16],
        }
    }

    #[test]
    fn create_new_store() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();
        assert_eq!(store.block_count(), 0);
        assert_eq!(store.allocated_bytes(), 0);
        assert!(dir.path().join("blocks.data").exists());
        store.close().unwrap();
    }

    #[test]
    fn put_and_get() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        let data = b"hello, cache v2!";
        store.put(1, data, true).unwrap();
        assert_eq!(store.block_count(), 1);

        let got = store.get(1).expect("should hit");
        assert_eq!(&got[..], data);
        assert_eq!(store.hit_count(), 1);
        assert_eq!(store.miss_count(), 0);
    }

    #[test]
    fn get_miss_returns_none() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        assert!(store.get(999).is_none());
        assert_eq!(store.miss_count(), 0); // miss counter only bumps on pread/checksum failures
    }

    #[test]
    fn contains_check() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        assert!(!store.contains(1));
        store.put(1, b"x", true).unwrap();
        assert!(store.contains(1));
    }

    #[test]
    fn remove_single() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"data", true).unwrap();
        assert!(store.contains(1));

        store.remove(1);
        assert!(!store.contains(1));
        assert_eq!(store.block_count(), 0);
    }

    #[test]
    fn remove_batch_multiple() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"a", true).unwrap();
        store.put(2, b"b", true).unwrap();
        store.put(3, b"c", true).unwrap();

        store.remove_batch(&[1, 3, 999]); // 999 doesn't exist — harmless
        assert!(!store.contains(1));
        assert!(store.contains(2));
        assert!(!store.contains(3));
        assert_eq!(store.block_count(), 1);
    }

    #[test]
    fn put_overwrites_existing() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"first", true).unwrap();
        store.put(1, b"second", true).unwrap();
        assert_eq!(store.block_count(), 1);

        let got = store.get(1).unwrap();
        assert_eq!(&got[..], b"second");
    }

    #[test]
    fn variable_block_sizes() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        let small = vec![0xAAu8; 100];
        let medium = vec![0xBBu8; 8_000];
        let large = vec![0xCCu8; 100_000];

        store.put(1, &small, true).unwrap();
        store.put(2, &medium, true).unwrap();
        store.put(3, &large, true).unwrap();

        assert_eq!(&store.get(1).unwrap()[..], &small[..]);
        assert_eq!(&store.get(2).unwrap()[..], &medium[..]);
        assert_eq!(&store.get(3).unwrap()[..], &large[..]);
    }

    #[test]
    fn empty_payload() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"", true).unwrap();
        let got = store.get(1).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn dirty_flag_tracking() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"dirty", false).unwrap();
        assert_eq!(store.dirty_count(), 1);

        store.put(2, b"clean", true).unwrap();
        assert_eq!(store.dirty_count(), 1);

        store.mark_evictable(1);
        assert_eq!(store.dirty_count(), 0);
    }

    #[test]
    fn dirty_remove_decrements() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"dirty", false).unwrap();
        assert_eq!(store.dirty_count(), 1);

        store.remove(1);
        assert_eq!(store.dirty_count(), 0);
    }

    #[test]
    fn capacity_limit_returns_error() {
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 3 * format::PAGE_SIZE, // room for 3 small blocks
            volume_uuid: [0xBB; 16],
        };
        let store = BlockStore::open(config).unwrap();

        // Fill up (each 1-byte block takes 1 page = 4096).
        store.put(1, b"a", true).unwrap();
        store.put(2, b"b", true).unwrap();
        store.put(3, b"c", true).unwrap();

        // Fourth block should fail.
        let res = store.put(4, b"d", true);
        assert!(res.is_err());
        match res.unwrap_err() {
            BlockStoreError::CacheFull { .. } => {}
            other => panic!("expected CacheFull, got {other}"),
        }

        // Remove one, then the put should succeed.
        store.remove(2);
        store.put(4, b"d", true).unwrap();
        assert_eq!(&store.get(4).unwrap()[..], b"d");
    }

    #[test]
    fn allocator_reuse_after_remove() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"first", true).unwrap();
        let alloc_before = store.allocated_bytes();

        store.remove(1);
        assert_eq!(store.allocated_bytes(), 0);

        store.put(2, b"second", true).unwrap();
        let alloc_after = store.allocated_bytes();
        assert_eq!(alloc_before, alloc_after);
    }

    #[test]
    fn sync_data_does_not_error() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();
        store.put(1, b"data", true).unwrap();
        store.sync_data().unwrap();
    }

    #[test]
    fn close_and_reopen() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);

        // First session.
        let store = BlockStore::open(config.clone()).unwrap();
        store.put(1, b"persist?", true).unwrap();
        store.close().unwrap();

        // Second session — no index persistence yet so the cache starts empty.
        let store2 = BlockStore::open(config).unwrap();
        assert_eq!(store2.block_count(), 0);
        assert!(store2.get(1).is_none());
    }

    #[test]
    fn reopen_validates_uuid() {
        let dir = TempDir::new().unwrap();
        let config1 = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: 64 * 1024 * 1024,
            volume_uuid: [0x11; 16],
        };
        BlockStore::open(config1).unwrap().close().unwrap();

        let config2 = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: 64 * 1024 * 1024,
            volume_uuid: [0x22; 16], // different UUID
        };
        let res = BlockStore::open(config2);
        assert!(matches!(res, Err(BlockStoreError::VolumeUuidMismatch)));
    }

    #[test]
    fn hit_miss_counters() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"data", true).unwrap();
        store.get(1); // hit
        store.get(1); // hit
        assert_eq!(store.hit_count(), 2);
        assert_eq!(store.miss_count(), 0);
    }

    #[test]
    fn large_block_16mb() {
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: 256 * 1024 * 1024,
            volume_uuid: [0xCC; 16],
        };
        let store = BlockStore::open(config).unwrap();

        let big = vec![0x42u8; 16 * 1024 * 1024];
        store.put(1, &big, true).unwrap();

        let got = store.get(1).unwrap();
        assert_eq!(got.len(), big.len());
        assert_eq!(got[0], 0x42);
        assert_eq!(got[got.len() - 1], 0x42);
    }

    #[test]
    fn many_blocks_cycle() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        // Insert 100 blocks.
        for i in 0u64..100 {
            let data = format!("block-{i}");
            store.put(i, data.as_bytes(), true).unwrap();
        }
        assert_eq!(store.block_count(), 100);

        // Verify all.
        for i in 0u64..100 {
            let expected = format!("block-{i}");
            let got = store.get(i).unwrap();
            assert_eq!(&got[..], expected.as_bytes());
        }

        // Remove odd IDs.
        let odds: Vec<u64> = (0..100).filter(|i| i % 2 == 1).collect();
        store.remove_batch(&odds);
        assert_eq!(store.block_count(), 50);

        // Even IDs still readable.
        for i in (0u64..100).step_by(2) {
            assert!(store.contains(i));
        }
    }

    #[test]
    fn fragmentation_and_coalesce() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        // Insert blocks of varying sizes: 100B, 4000B, 8000B, 500B, 16000B
        let sizes: &[usize] = &[
            100, 4_000, 8_000, 500, 16_000, 200, 6_000, 1_000, 12_000, 50,
        ];
        for (i, &sz) in sizes.iter().enumerate() {
            let data = vec![(i & 0xFF) as u8; sz];
            store.put(i as u64, &data, true).unwrap();
        }
        assert_eq!(store.block_count(), sizes.len() as u64);
        let alloc_before = store.allocated_bytes();

        // Remove every other block to create fragmentation.
        for i in (0..sizes.len()).step_by(2) {
            store.remove(i as u64);
        }
        assert_eq!(store.block_count(), 5);

        // Remaining blocks should still be readable.
        for i in (1..sizes.len()).step_by(2) {
            let expected = vec![(i & 0xFF) as u8; sizes[i]];
            let got = store.get(i as u64).unwrap();
            assert_eq!(&got[..], &expected[..]);
        }

        // Re-insert into the holes — allocator should reuse free space.
        for i in (0..sizes.len()).step_by(2) {
            let data = vec![0xFFu8; sizes[i]];
            store.put(i as u64, &data, true).unwrap();
        }
        assert_eq!(store.block_count(), sizes.len() as u64);

        // Total allocation should be the same (space was reused, not grown).
        assert_eq!(store.allocated_bytes(), alloc_before);
    }

    #[test]
    fn concurrent_put_get() {
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let store = Arc::new(BlockStore::open(test_config(&dir)).unwrap());

        // Phase 1: concurrent puts from 4 threads, 50 blocks each (disjoint ID ranges).
        let mut handles = Vec::new();
        for t in 0u64..4 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let base = t * 50;
                for i in 0..50 {
                    let id = base + i;
                    let data = format!("t{t}-block-{i}");
                    s.put(id, data.as_bytes(), true).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(store.block_count(), 200);

        // Phase 2: concurrent reads from 4 threads.
        let mut handles = Vec::new();
        for t in 0u64..4 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let base = t * 50;
                for i in 0..50 {
                    let id = base + i;
                    let expected = format!("t{t}-block-{i}");
                    let got = s.get(id).expect("should hit");
                    assert_eq!(&got[..], expected.as_bytes());
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(store.hit_count(), 200);

        // Phase 3: concurrent mixed put + remove.
        let mut handles = Vec::new();
        // Thread removing odd IDs
        {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for id in (1..200u64).step_by(2) {
                    s.remove(id);
                }
            }));
        }
        // Thread putting new blocks into a higher ID range
        {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for id in 200..300u64 {
                    s.put(id, b"new-block", true).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Even IDs from original range should still be present.
        for id in (0..200u64).step_by(2) {
            assert!(store.contains(id), "even id {id} should survive");
        }
    }

    #[test]
    fn reopen_rejects_shrunk_max_size() {
        let dir = TempDir::new().unwrap();
        let config1 = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: 128 * 1024 * 1024,
            volume_uuid: [0x11; 16],
        };
        BlockStore::open(config1).unwrap().close().unwrap();

        let config2 = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: 64 * 1024 * 1024, // smaller than superblock
            volume_uuid: [0x11; 16],
        };
        let res = BlockStore::open(config2);
        assert!(matches!(res, Err(BlockStoreError::MaxSizeShrunk { .. })));
    }
}

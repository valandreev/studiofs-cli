//! Single-file block cache with a native free-space allocator and LRU eviction.
//!
//! `BlockStore` is the Cache v2 core: one `blocks.data` file, a [`FreeList`]
//! best-fit allocator, a [`BlockIndex`] for O(1) lookup, and an intrusive LRU
//! list for eviction ordering.
//!
//! ## Thread-safety model
//!
//! | State | Lock |
//! |-------|------|
//! | [`BlockIndex`] (map + LRU + counters) | `Mutex<BlockIndex>` |
//! | [`FreeList`] (allocator) | `Mutex<FreeList>` |
//! | Hit / miss counters | `AtomicU64` |
//!
//! Locks are **never held simultaneously** — the index lock is always
//! released before the allocator lock is acquired, and vice-versa.  This
//! strict ordering prevents deadlocks.
//!
//! ## Eviction
//!
//! When [`put`] cannot obtain enough space, it repeatedly calls
//! `BlockIndex::evict_one` to pop the oldest *evictable* LRU entry (blocks
//! with `DIRTY`, `PINNED`, or `UPLOADING` are skipped), frees the recovered
//! allocation, and retries.  If no evictable blocks remain it returns
//! [`BlockStoreError::CacheFull`].

use bytes::Bytes;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::warn;

use crate::v2::allocator::FreeList;
use crate::v2::format::{
    self, BLOCK_HEADER_SIZE, BlockHeader, FORMAT_VERSION, SUPERBLOCK_SIZE, Superblock,
    alloc_size_for,
};
use crate::v2::index::{BlockFlags, BlockIndex};
use crate::v2::platform;

// ===========================================================================
// Configuration
// ===========================================================================

/// Configuration for opening a [`BlockStore`].
#[derive(Debug, Clone)]
pub struct BlockStoreConfig {
    /// Directory for cache files (`blocks.data` will be created here).
    pub cache_dir: PathBuf,
    /// Maximum total size of the data file in bytes.
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
// BlockStore
// ===========================================================================

/// Single-file block cache with built-in allocator and LRU eviction.
///
/// One instance per mount.  Thread-safe for concurrent `get` / `put` /
/// `remove` calls from multiple threads.
pub struct BlockStore {
    file: File,
    config: BlockStoreConfig,
    /// Combined block map + LRU list + dirty/block counters.
    index: Mutex<BlockIndex>,
    /// Best-fit free-space allocator for `blocks.data`.
    allocator: Mutex<FreeList>,
    /// Total cache hits since open.
    hit_count: AtomicU64,
    /// Total cache misses since open.
    miss_count: AtomicU64,
    /// Set by `close()` so [`Drop`] can skip the redundant fdatasync.
    synced: AtomicBool,
}

/// Frees an allocated region unless the caller explicitly commits it.
///
/// `BlockStore::put` allocates space before writing block bytes and before the
/// in-memory index entry is published. This guard closes that gap so an I/O
/// error or panic cannot leave allocator state permanently ahead of the index.
struct PendingAllocation<'a> {
    store: &'a BlockStore,
    offset: u64,
    alloc_size: u64,
    committed: bool,
}

impl<'a> PendingAllocation<'a> {
    fn new(store: &'a BlockStore, offset: u64, alloc_size: u64) -> Self {
        Self {
            store,
            offset,
            alloc_size,
            committed: false,
        }
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for PendingAllocation<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.store
            .allocator
            .lock()
            .free(self.offset, self.alloc_size);
    }
}

impl BlockStore {
    /// Create or open a block store in `config.cache_dir`.
    ///
    /// * **New file** — writes a fresh superblock, starts empty.
    /// * **Existing file** — validates the superblock (magic, version, UUID).
    ///   Index persistence is implemented in SFS-137; until then the in-memory
    ///   index starts empty and all pre-existing data-region bytes are treated
    ///   as reclaimable free space.
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

            if sb.page_size != format::PAGE_SIZE as u32 {
                return Err(BlockStoreError::CorruptSuperblock(format!(
                    "page_size mismatch: expected {}, found {}",
                    format::PAGE_SIZE,
                    sb.page_size,
                )));
            }
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
            if config.max_size > sb.max_size {
                // Persist the larger limit so a future reopen reflects the
                // current upper bound rather than the original smaller value.
                let updated_sb = Superblock::new(config.max_size, config.volume_uuid);
                platform::pwrite(&file, &updated_sb.to_bytes(), 0)?;
                platform::fdatasync(&file)?;
            }

            (file, len)
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&data_path)?;

            let sb = Superblock::new(config.max_size, config.volume_uuid);
            platform::pwrite(&file, &sb.to_bytes(), 0)?;
            file.set_len(SUPERBLOCK_SIZE)?;
            platform::fdatasync(&file)?;

            (file, SUPERBLOCK_SIZE)
        };

        // No index persistence yet — treat all data-region bytes as free
        // (index persistence is implemented in SFS-137).
        let file_end = std::cmp::max(file_len, SUPERBLOCK_SIZE);
        let allocator = if file_end > SUPERBLOCK_SIZE {
            warn!(
                data_bytes = file_end - SUPERBLOCK_SIZE,
                "opening non-empty blocks.data without index persistence; \
                 all existing data regions will be treated as free space \
                 (snapshot+WAL persistence coming in SFS-137)",
            );
            FreeList::recover(config.max_size, file_end, &[])
        } else {
            FreeList::new(config.max_size)
        };

        Ok(Self {
            file,
            config,
            index: Mutex::new(BlockIndex::new()),
            allocator: Mutex::new(allocator),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            synced: AtomicBool::new(false),
        })
    }

    // -----------------------------------------------------------------------
    // Core read operations
    // -----------------------------------------------------------------------

    /// Read a block from the cache.
    ///
    /// Returns `None` on a cache miss or a data-integrity failure (short read
    /// or checksum mismatch).  Bumps the hit/miss counter.
    ///
    /// On a hit, moves the block to the MRU position in the LRU list.
    pub fn get(&self, object_id: u64) -> Option<Bytes> {
        // Phase 1: copy entry metadata under the index lock (no I/O inside lock).
        let (offset, data_size, checksum) = {
            let idx = self.index.lock();
            match idx.get(object_id) {
                Some(e) => (e.offset, e.data_size, e.checksum),
                None => {
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
        };

        // Phase 2: pread outside the lock (concurrent-safe, no seek).
        let mut buf = vec![0u8; data_size as usize];
        if let Err(e) = platform::pread(&self.file, &mut buf, offset + BLOCK_HEADER_SIZE as u64) {
            warn!(object_id, offset, %e, "block pread failed");
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Verify checksum.
        let actual_crc = crc32fast::hash(&buf);
        if actual_crc != checksum {
            warn!(
                object_id,
                offset,
                expected = checksum,
                actual = actual_crc,
                "block checksum mismatch",
            );
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Phase 3: revalidate metadata and touch LRU under the index lock.
        //
        // This closes an ABA window where the on-disk region at `offset` could
        // have been freed and reallocated to a different object between the
        // initial metadata read and the `pread` above. If the current index
        // entry no longer matches the metadata used for the read, treat this
        // as a miss and discard the payload.
        {
            let mut idx = self.index.lock();
            let metadata_matches = matches!(
                idx.get(object_id),
                Some(e)
                    if e.offset == offset
                        && e.data_size == data_size
                        && e.checksum == checksum
            );

            if !metadata_matches {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            idx.touch(object_id);
        }

        self.hit_count.fetch_add(1, Ordering::Relaxed);
        Some(Bytes::from(buf))
    }

    /// Check whether a block exists in the cache without reading data or
    /// updating the LRU order.
    pub fn contains(&self, object_id: u64) -> bool {
        self.index.lock().contains(object_id)
    }

    // -----------------------------------------------------------------------
    // Core write operations
    // -----------------------------------------------------------------------

    /// Write a block to the cache.
    ///
    /// * `evictable = false` marks the block as *dirty* (sets the `DIRTY`
    ///   flag); the block will not be chosen for LRU eviction until
    ///   [`mark_evictable`] is called.
    /// * If the block already exists it is replaced in the cache.
    /// * When the cache is full, eligible LRU blocks are evicted automatically
    ///   to make room.  Returns [`BlockStoreError::CacheFull`] only when all
    ///   remaining blocks are non-evictable.
    pub fn put(&self, object_id: u64, data: &[u8], evictable: bool) -> Result<(), BlockStoreError> {
        self.put_inner(object_id, data, evictable, || {})
    }

    fn put_inner<F>(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
        after_write: F,
    ) -> Result<(), BlockStoreError>
    where
        F: FnOnce(),
    {
        let data_size = u32::try_from(data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "block too large for cache format (size exceeds u32::MAX)",
            )
        })?;
        let alloc = alloc_size_for(data_size);

        // Remove any existing entry for this object_id and free its space so
        // the allocator can reuse it.  This is done before the new allocation
        // so the freed bytes count toward capacity for the incoming block.
        if let Some(old) = self.index.lock().remove(object_id) {
            self.allocator.lock().free(old.offset, old.alloc_size);
        }

        // Allocate space, evicting LRU blocks when needed.
        let offset = self.allocate_with_eviction(alloc)?;
        let mut pending_allocation = PendingAllocation::new(self, offset, alloc);

        // Write header + payload via pwrite.  On I/O failure the allocated
        // region is released by `pending_allocation` so the allocator does not
        // leak space even if we return early or unwind before publishing the
        // new index entry.
        let checksum = crc32fast::hash(data);
        let flags = if evictable {
            BlockFlags::empty()
        } else {
            BlockFlags::DIRTY
        };
        // Only DIRTY and PINNED are persisted in the on-disk header;
        // UPLOADING is a transient in-memory flag.
        let disk_flags = flags.bits() & (format::FLAG_DIRTY | format::FLAG_PINNED);
        let header = BlockHeader::new(object_id, data_size, checksum, disk_flags);

        let write_result =
            platform::pwrite(&self.file, &header.to_bytes(), offset).and_then(|()| {
                if !data.is_empty() {
                    platform::pwrite(&self.file, data, offset + BLOCK_HEADER_SIZE as u64)
                } else {
                    Ok(())
                }
            });

        if let Err(e) = write_result {
            return Err(BlockStoreError::Io(e));
        }

        after_write();

        // Register the block in the index (push to LRU tail). If another
        // concurrent put inserted the same object_id after our initial remove,
        // `insert` returns the replaced allocation and we free it afterwards
        // without holding the index and allocator locks simultaneously.
        let replaced = self
            .index
            .lock()
            .insert(object_id, offset, data_size, alloc, checksum, flags);
        pending_allocation.commit();

        if let Some(old) = replaced {
            self.allocator.lock().free(old.offset, old.alloc_size);
        }

        Ok(())
    }

    #[cfg(test)]
    fn put_with_after_write_hook<F>(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
        after_write: F,
    ) -> Result<(), BlockStoreError>
    where
        F: FnOnce(),
    {
        self.put_inner(object_id, data, evictable, after_write)
    }

    /// Mark a dirty block as evictable — typically called after a successful
    /// S3 upload.
    ///
    /// Clears both `DIRTY` and `UPLOADING` flags.  Persists the cleared flags
    /// to the on-disk block header so the crash-recovery scanner (SFS-137)
    /// sees the correct state.  On pwrite failure the block retains its current
    /// flags and a warning is logged.
    ///
    /// # Concurrency
    ///
    /// This method intentionally holds the index lock across the header
    /// `pwrite`. The header write is small and fixed-size, while dropping the
    /// lock would open an ABA window where the block could be removed, its
    /// disk offset reused for another object, and then overwritten by the stale
    /// header write.
    pub fn mark_evictable(&self, object_id: u64) {
        // Keep the index lock for the entire operation so the entry cannot be
        // removed or replaced while we are writing its on-disk header.
        let mut idx = self.index.lock();
        let (offset, data_size, checksum, pinned) = match idx.get(object_id) {
            Some(e)
                if e.flags
                    .intersects(BlockFlags::DIRTY | BlockFlags::UPLOADING) =>
            {
                (
                    e.offset,
                    e.data_size,
                    e.checksum,
                    e.flags.contains(BlockFlags::PINNED),
                )
            }
            _ => return, // already evictable or not present
        };

        // Write the on-disk header with DIRTY cleared; preserve PINNED.
        let disk_flags = if pinned { format::FLAG_PINNED } else { 0 };
        let header = BlockHeader::new(object_id, data_size, checksum, disk_flags);

        match platform::pwrite(&self.file, &header.to_bytes(), offset) {
            Ok(()) => {
                idx.modify_flags(
                    object_id,
                    BlockFlags::DIRTY | BlockFlags::UPLOADING,
                    BlockFlags::empty(),
                );
            }
            Err(e) => {
                warn!(
                    error = ?e,
                    object_id,
                    offset,
                    "mark_evictable: pwrite failed; block retains dirty/uploading flags",
                );
            }
        }
    }

    /// Mark a block as *currently uploading* to S3.
    ///
    /// Sets the `UPLOADING` flag so the block is excluded from LRU eviction
    /// while the transfer is in flight.  No-op if the block does not exist.
    /// The `UPLOADING` flag is transient and never persisted to disk.
    pub fn mark_uploading(&self, object_id: u64) {
        self.index
            .lock()
            .modify_flags(object_id, BlockFlags::empty(), BlockFlags::UPLOADING);
    }

    /// Mark that an S3 upload has failed, clearing the `UPLOADING` flag so
    /// the block can be retried.  The `DIRTY` flag is preserved.
    ///
    /// No-op if the block does not exist.
    pub fn mark_upload_failed(&self, object_id: u64) {
        self.index
            .lock()
            .modify_flags(object_id, BlockFlags::UPLOADING, BlockFlags::empty());
    }

    /// Remove a single block from the cache.
    ///
    /// No-op if the block does not exist.
    pub fn remove(&self, object_id: u64) {
        if let Some(removed) = self.index.lock().remove(object_id) {
            self.allocator
                .lock()
                .free(removed.offset, removed.alloc_size);
        }
    }

    /// Remove multiple blocks in a single call.
    ///
    /// Unknown IDs are silently skipped.  More efficient than calling
    /// [`remove`] in a loop because the allocator lock is acquired only once.
    pub fn remove_batch(&self, object_ids: &[u64]) {
        let removed = self.index.lock().remove_batch(object_ids);
        if removed.is_empty() {
            return;
        }
        let mut alloc = self.allocator.lock();
        for entry in &removed {
            alloc.free(entry.offset, entry.alloc_size);
        }
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    /// Flush the data file to stable storage (`fdatasync`).
    pub fn sync_data(&self) -> Result<(), BlockStoreError> {
        platform::fdatasync(&self.file)?;
        Ok(())
    }

    /// Graceful shutdown: flush data and release the file handle.
    ///
    /// Prefer explicit `close()` over relying on [`Drop`].
    pub fn close(self) -> Result<(), BlockStoreError> {
        platform::fdatasync(&self.file)?;
        self.synced.store(true, Ordering::Relaxed);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    /// Bytes currently allocated to live blocks (header + data + padding).
    pub fn allocated_bytes(&self) -> u64 {
        self.allocator.lock().allocated_bytes()
    }

    /// Number of blocks currently in the cache.
    pub fn block_count(&self) -> u64 {
        self.index.lock().block_count()
    }

    /// Number of blocks with the `DIRTY` flag set (not yet uploaded to S3).
    pub fn dirty_count(&self) -> u32 {
        self.index.lock().dirty_count()
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

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Allocate `alloc` bytes, evicting LRU blocks as needed.
    ///
    /// 1. Try a direct allocation.
    /// 2. On failure, pop the oldest evictable block from the index, free its
    ///    space, and retry.  Repeat until allocation succeeds or no more
    ///    evictable blocks remain.
    ///
    /// The file is extended to accommodate the new allocation before returning.
    fn allocate_with_eviction(&self, alloc: u64) -> Result<u64, BlockStoreError> {
        // First attempt without eviction.
        if let Some(offset) = self.try_allocate_and_extend(alloc)? {
            return Ok(offset);
        }

        // Eviction loop: free one block at a time until the allocation fits.
        loop {
            let evicted = self.index.lock().evict_one();
            match evicted {
                Some(entry) => {
                    self.allocator.lock().free(entry.offset, entry.alloc_size);
                }
                None => {
                    // All remaining blocks are non-evictable.
                    let a = self.allocator.lock();
                    return Err(BlockStoreError::CacheFull {
                        needed: alloc,
                        max_size: self.config.max_size,
                        allocated: a.allocated_bytes(),
                        free: a.free_bytes(),
                    });
                }
            }

            if let Some(offset) = self.try_allocate_and_extend(alloc)? {
                return Ok(offset);
            }
            // If we evicted but still cannot allocate (e.g. the freed block
            // was too small to satisfy the request on its own), evict more.
        }
    }

    /// Try a single allocation from the free list and extend the physical file
    /// if the logical `file_end` grew.
    ///
    /// Returns `Ok(Some(offset))` on success, `Ok(None)` when the free list
    /// cannot satisfy the request without eviction.
    fn try_allocate_and_extend(&self, alloc: u64) -> Result<Option<u64>, BlockStoreError> {
        let mut a = self.allocator.lock();
        let offset = match a.allocate(alloc) {
            Ok(off) => off,
            Err(_) => return Ok(None),
        };
        // Extend the physical file while still holding the allocator lock so
        // that a concurrent free+allocate in another thread cannot race with
        // set_len.
        let needed_len = a.file_end();
        let current_len = self.file.metadata()?.len();
        if needed_len > current_len {
            self.file.set_len(needed_len)?;
        }
        Ok(Some(offset))
    }
}

impl Drop for BlockStore {
    fn drop(&mut self) {
        if self.synced.load(Ordering::Relaxed) {
            return;
        }
        if let Err(e) = platform::fdatasync(&self.file) {
            warn!(%e, "BlockStore drop: fdatasync failed");
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> BlockStoreConfig {
        BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: 64 * 1024 * 1024, // 64 MiB
            volume_uuid: [0xAA; 16],
        }
    }

    // -----------------------------------------------------------------------
    // Baseline correctness
    // -----------------------------------------------------------------------

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
        assert_eq!(store.miss_count(), 1);
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

        store.remove_batch(&[1, 3, 999]); // 999 is a no-op
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
        assert!(store.get(1).unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // Flag tracking
    // -----------------------------------------------------------------------

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
    fn mark_uploading_and_evictable() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"data", false).unwrap(); // DIRTY
        assert_eq!(store.dirty_count(), 1);

        store.mark_uploading(1); // DIRTY | UPLOADING

        store.mark_evictable(1); // clears both
        assert_eq!(store.dirty_count(), 0);
    }

    #[test]
    fn mark_upload_failed_preserves_dirty() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"data", false).unwrap(); // DIRTY
        store.mark_uploading(1);
        store.mark_upload_failed(1); // clears UPLOADING, keeps DIRTY

        assert_eq!(store.dirty_count(), 1);
        // Block should still be non-evictable (DIRTY set):
        // fill cache with clean blocks and verify block 1 survives.
        let second_dir = TempDir::new().unwrap();
        let small_cfg = BlockStoreConfig {
            cache_dir: second_dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 3 * format::PAGE_SIZE,
            volume_uuid: [0xBB; 16],
        };
        let s2 = BlockStore::open(small_cfg).unwrap();
        s2.put(1, b"data", false).unwrap();
        s2.mark_uploading(1);
        s2.mark_upload_failed(1);
        // Fill with 2 clean blocks then attempt a 3rd — eviction should NOT
        // pick block 1 because it's still dirty.
        s2.put(2, b"clean", true).unwrap();
        s2.put(3, b"clean", true).unwrap();
        // Cache is full (3 pages used); put(4) must evict 2 or 3, not 1.
        s2.put(4, b"clean", true).unwrap();
        assert!(s2.contains(1), "dirty block should not be evicted");
    }

    // -----------------------------------------------------------------------
    // Eviction behaviour
    // -----------------------------------------------------------------------

    #[test]
    fn auto_evict_lru_when_full() {
        // Cache holds exactly 3 single-page blocks.
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 3 * format::PAGE_SIZE,
            volume_uuid: [0xDD; 16],
        };
        let store = BlockStore::open(config).unwrap();

        // Insert 3 evictable blocks in order 1, 2, 3.
        store.put(1, b"a", true).unwrap();
        store.put(2, b"b", true).unwrap();
        store.put(3, b"c", true).unwrap();
        assert_eq!(store.block_count(), 3);

        // Insert a 4th block — should auto-evict block 1 (oldest LRU).
        store.put(4, b"d", true).unwrap();
        assert_eq!(store.block_count(), 3);
        assert!(!store.contains(1), "oldest block evicted");
        assert!(store.contains(2));
        assert!(store.contains(3));
        assert!(store.contains(4));
    }

    #[test]
    fn evict_skips_dirty_blocks() {
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 3 * format::PAGE_SIZE,
            volume_uuid: [0xEE; 16],
        };
        let store = BlockStore::open(config).unwrap();

        // 1 = dirty (oldest), 2 = clean, 3 = clean.
        store.put(1, b"x", false).unwrap(); // DIRTY
        store.put(2, b"y", true).unwrap();
        store.put(3, b"z", true).unwrap();

        // Insert 4th — must evict block 2 (oldest evictable), not block 1.
        store.put(4, b"w", true).unwrap();
        assert!(store.contains(1), "dirty block must not be evicted");
        assert!(!store.contains(2), "oldest evictable was evicted");
    }

    #[test]
    fn all_dirty_returns_cache_full() {
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 3 * format::PAGE_SIZE,
            volume_uuid: [0xFF; 16],
        };
        let store = BlockStore::open(config).unwrap();

        // Fill with dirty (non-evictable) blocks.
        store.put(1, b"d", false).unwrap();
        store.put(2, b"d", false).unwrap();
        store.put(3, b"d", false).unwrap();

        // 4th put cannot evict anyone → CacheFull.
        let res = store.put(4, b"d", false);
        assert!(matches!(res, Err(BlockStoreError::CacheFull { .. })));
    }

    #[test]
    fn eviction_touch_updates_lru_order() {
        // 3-page cache: insert 1, 2, 3; touch 1 (making 2 the oldest); then
        // inserting 4 should evict 2.
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 3 * format::PAGE_SIZE,
            volume_uuid: [0x10; 16],
        };
        let store = BlockStore::open(config).unwrap();

        store.put(1, b"a", true).unwrap();
        store.put(2, b"b", true).unwrap();
        store.put(3, b"c", true).unwrap();

        // Touch block 1 via get() — it moves to MRU; 2 is now the oldest.
        store.get(1);

        store.put(4, b"d", true).unwrap();
        assert!(store.contains(1), "recently touched; should not be evicted");
        assert!(!store.contains(2), "oldest LRU; evicted");
    }

    #[test]
    fn uploading_flag_prevents_eviction() {
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 3 * format::PAGE_SIZE,
            volume_uuid: [0x20; 16],
        };
        let store = BlockStore::open(config).unwrap();

        store.put(1, b"u", true).unwrap(); // evictable but UPLOADING
        store.mark_uploading(1);
        store.put(2, b"e", true).unwrap();
        store.put(3, b"e", true).unwrap();

        // 4th block: 1 is UPLOADING, 2 is oldest evictable.
        store.put(4, b"e", true).unwrap();
        assert!(store.contains(1), "uploading block not evicted");
        assert!(!store.contains(2), "oldest plain-evictable evicted");
    }

    // -----------------------------------------------------------------------
    // Allocator / capacity
    // -----------------------------------------------------------------------

    #[test]
    fn allocator_reuse_after_remove() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"first", true).unwrap();
        let alloc_before = store.allocated_bytes();
        store.remove(1);
        assert_eq!(store.allocated_bytes(), 0);
        store.put(2, b"second", true).unwrap();
        assert_eq!(store.allocated_bytes(), alloc_before);
    }

    #[test]
    fn panic_after_write_releases_pending_allocation() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = store.put_with_after_write_hook(1, b"first", true, || {
                panic!("simulated panic after write");
            });
        }));

        assert!(panic_result.is_err());
        assert_eq!(store.block_count(), 0);
        assert_eq!(store.allocated_bytes(), 0);
        assert!(!store.contains(1));

        store.put(2, b"second", true).unwrap();
        assert_eq!(&store.get(2).unwrap()[..], b"second");
    }

    #[test]
    fn fragmentation_and_coalesce() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        let sizes: &[usize] = &[
            100, 4_000, 8_000, 500, 16_000, 200, 6_000, 1_000, 12_000, 50,
        ];
        for (i, &sz) in sizes.iter().enumerate() {
            let data = vec![(i & 0xFF) as u8; sz];
            store.put(i as u64, &data, true).unwrap();
        }
        let alloc_before = store.allocated_bytes();

        // Create fragmentation.
        for i in (0..sizes.len()).step_by(2) {
            store.remove(i as u64);
        }
        assert_eq!(store.block_count(), 5);

        for i in (1..sizes.len()).step_by(2) {
            let expected = vec![(i & 0xFF) as u8; sizes[i]];
            assert_eq!(&store.get(i as u64).unwrap()[..], &expected[..]);
        }

        // Re-insert; allocator should reuse freed space.
        for i in (0..sizes.len()).step_by(2) {
            let data = vec![0xFFu8; sizes[i]];
            store.put(i as u64, &data, true).unwrap();
        }
        assert_eq!(store.allocated_bytes(), alloc_before);
    }

    // -----------------------------------------------------------------------
    // Persistence / reopen
    // -----------------------------------------------------------------------

    #[test]
    fn sync_data_does_not_error() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();
        store.put(1, b"data", true).unwrap();
        store.sync_data().unwrap();
    }

    #[test]
    fn close_and_reopen_empty_index() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(1, b"persist?", true).unwrap();
        store.close().unwrap();

        // Index persistence is SFS-137; the cache starts empty on reopen.
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
            volume_uuid: [0x22; 16],
        };
        assert!(matches!(
            BlockStore::open(config2),
            Err(BlockStoreError::VolumeUuidMismatch)
        ));
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
            max_size: 64 * 1024 * 1024,
            volume_uuid: [0x11; 16],
        };
        assert!(matches!(
            BlockStore::open(config2),
            Err(BlockStoreError::MaxSizeShrunk { .. })
        ));
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    #[test]
    fn hit_miss_counters() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"data", true).unwrap();
        store.get(1);
        store.get(1);
        assert_eq!(store.hit_count(), 2);
        assert_eq!(store.miss_count(), 0);
        store.get(99);
        assert_eq!(store.miss_count(), 1);
    }

    // -----------------------------------------------------------------------
    // Large blocks
    // -----------------------------------------------------------------------

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
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    #[test]
    fn concurrent_put_get() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(BlockStore::open(test_config(&dir)).unwrap());

        // Phase 1: concurrent puts from 4 threads (disjoint ID ranges).
        let mut handles = Vec::new();
        for t in 0u64..4 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0u64..50 {
                    let id = t * 50 + i;
                    let data = format!("t{t}-block-{i}");
                    s.put(id, data.as_bytes(), true).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(store.block_count(), 200);

        // Phase 2: concurrent reads.
        let mut handles = Vec::new();
        for t in 0u64..4 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0u64..50 {
                    let id = t * 50 + i;
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

        // Phase 3: concurrent removes and new puts.
        let mut handles = Vec::new();
        {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for id in (1u64..200).step_by(2) {
                    s.remove(id);
                }
            }));
        }
        {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for id in 200u64..300 {
                    s.put(id, b"new-block", true).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        for id in (0u64..200).step_by(2) {
            assert!(store.contains(id), "even id {id} should survive");
        }
    }

    #[test]
    fn concurrent_touch_stress() {
        // Many threads repeatedly get the same block to stress the LRU touch path.
        let dir = TempDir::new().unwrap();
        let store = Arc::new(BlockStore::open(test_config(&dir)).unwrap());

        // Seed several blocks.
        for id in 0u64..20 {
            store.put(id, b"shared", true).unwrap();
        }

        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for _ in 0..200 {
                    for id in 0u64..20 {
                        let _ = s.get(id);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // All blocks must still be present and readable after the touch storm.
        assert_eq!(store.block_count(), 20);
        for id in 0u64..20 {
            assert!(store.contains(id));
        }
    }

    #[test]
    fn many_blocks_cycle() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        for i in 0u64..100 {
            let data = format!("block-{i}");
            store.put(i, data.as_bytes(), true).unwrap();
        }
        assert_eq!(store.block_count(), 100);

        for i in 0u64..100 {
            let expected = format!("block-{i}");
            let got = store.get(i).unwrap();
            assert_eq!(&got[..], expected.as_bytes());
        }

        let odds: Vec<u64> = (0..100).filter(|i| i % 2 == 1).collect();
        store.remove_batch(&odds);
        assert_eq!(store.block_count(), 50);
        for i in (0u64..100).step_by(2) {
            assert!(store.contains(i));
        }
    }
}

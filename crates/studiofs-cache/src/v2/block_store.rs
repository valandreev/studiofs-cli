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
//! | Metadata WAL appender | `Mutex<WalAppender>` |
//! | [`FreeList`] (allocator) | `Mutex<FreeList>` |
//! | Physical file extension (`set_len`) | `Mutex<()>` (`file_growth`) |
//! | Hit / miss counters | `AtomicU64` |
//!
//! The allocator lock is never held at the same time as the index lock.
//! Some metadata mutations briefly hold the index and WAL locks together,
//! but there is no reverse `wal -> index -> allocator` path.  That lock
//! ordering prevents deadlocks while keeping WAL appends serialized.
//!
//! The `file_growth` lock is always acquired **after** the allocator lock has
//! been released.  It serialises `set_len` calls so that a logical `file_end`
//! advance from one thread's allocation is made physically visible before any
//! allocated offset in the new range is returned to a caller.  Holding the
//! allocator lock across the (potentially slow) `set_len` syscall is therefore
//! no longer necessary, which eliminates the Windows zero-fill stall that
//! previously blocked all concurrent allocations for up to hundreds of
//! milliseconds per 256 MiB growth event.
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
use std::time::Instant;
use tracing::warn;

use crate::v2::allocator::FreeList;
use crate::v2::format::{
    self, BLOCK_HEADER_SIZE, BlockHeader, FORMAT_VERSION, SUPERBLOCK_SIZE, Superblock,
    alloc_size_for,
};
use crate::v2::index::{BlockFlags, BlockIndex, PersistedIndexEntry, RemovedEntry};
use crate::v2::mmap_reader::MmapReader;
use crate::v2::persistence::{self, WalAppender};
use crate::v2::platform;

const BACKGROUND_EVICT_HIGH_WATERMARK_PERCENT: u64 = 90;
const BACKGROUND_EVICT_LOW_WATERMARK_PERCENT: u64 = 80;
const EMERGENCY_DISK_FREE_RATIO_NUMERATOR: u64 = 5;
const EMERGENCY_DISK_FREE_RATIO_DENOMINATOR: u64 = 100;
const EMERGENCY_DISK_FREE_FLOOR_ENV: &str = "STUDIOFS_CACHE_EMERGENCY_DISK_FREE_FLOOR_BYTES";
const DEFAULT_EMERGENCY_DISK_FREE_FLOOR_BYTES: u64 = 1024 * 1024 * 1024;
const PRESSURE_EVICTION_BATCH_TRIGGER_BYTES: u64 = 64 * 1024 * 1024;
const PRESSURE_EVICTION_BATCH_TARGET_BYTES: u64 = 16 * 1024 * 1024;
const PRESSURE_EVICTION_BATCH_MAX_BLOCKS: usize = 128;
const DISK_HEADROOM_REFRESH_INTERVAL_NS: u64 = 250_000_000;

#[derive(Debug, Clone, Copy, Default)]
pub struct PressureMaintenanceReport {
    pub background_evicted_blocks: u64,
    pub background_evicted_bytes: u64,
    pub emergency_evicted_blocks: u64,
    pub emergency_evicted_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
enum EvictionReason {
    Allocation,
    BackgroundWatermark,
    EmergencyDiskPressure,
}

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
    mmap_reader: MmapReader,
    /// Cached physical file length, updated only after successful growth.
    file_len: AtomicU64,
    /// Combined block map + LRU list + dirty/block counters.
    index: Mutex<BlockIndex>,
    /// Append-only metadata journal for mutations since the last snapshot.
    wal: Mutex<WalAppender>,
    /// Best-fit free-space allocator for `blocks.data`.
    allocator: Mutex<FreeList>,
    /// Serialises physical file extension (`set_len`) so that a logical
    /// `file_end` advance is physically visible (i.e. the file is extended)
    /// before any offset in the newly-grown region is returned to a caller.
    /// Always acquired *after* the allocator lock has been released; never
    /// acquired while the allocator lock is held.
    file_growth: Mutex<()>,
    /// Total cache hits since open.
    hit_count: AtomicU64,
    /// Total cache misses since open.
    miss_count: AtomicU64,
    /// Total evicted blocks.
    eviction_count: AtomicU64,
    /// Total bytes reclaimed by evictions.
    eviction_bytes: AtomicU64,
    /// Blocks evicted by background high-watermark maintenance.
    background_eviction_count: AtomicU64,
    /// Bytes reclaimed by background high-watermark maintenance.
    background_eviction_bytes: AtomicU64,
    /// Blocks evicted under emergency low-free-space pressure.
    emergency_eviction_count: AtomicU64,
    /// Bytes reclaimed under emergency low-free-space pressure.
    emergency_eviction_bytes: AtomicU64,
    /// Number of blocks dropped due to corruption/integrity failures.
    corruption_count: AtomicU64,
    /// Read operation count (hits + misses).
    read_ops: AtomicU64,
    /// Bytes returned to readers.
    read_bytes: AtomicU64,
    /// Total read latency in nanoseconds.
    read_latency_total_ns: AtomicU64,
    /// Max observed read latency in nanoseconds.
    read_latency_max_ns: AtomicU64,
    /// Successful write operation count.
    write_ops: AtomicU64,
    /// Bytes accepted on successful writes.
    write_bytes: AtomicU64,
    /// Total successful write latency in nanoseconds.
    write_latency_total_ns: AtomicU64,
    /// Max observed successful write latency in nanoseconds.
    write_latency_max_ns: AtomicU64,
    /// Best-effort post-read cache-drop hints issued by upload rereads.
    advisory_dont_need_count: AtomicU64,
    /// Minimum free-space target used by emergency disk-pressure eviction.
    emergency_disk_free_floor_bytes: u64,
    /// Cached filesystem free-space state; refreshed periodically instead of on every put.
    disk_free_bytes: AtomicU64,
    disk_total_bytes: AtomicU64,
    disk_headroom_last_refresh_elapsed_ns: AtomicU64,
    disk_headroom_clock_start: Instant,
    disk_headroom_refresh_in_progress: AtomicBool,
    disk_headroom_refresh_attempt_count: AtomicU64,
    disk_headroom_refresh_success_count: AtomicU64,
    disk_headroom_refresh_error_count: AtomicU64,
    disk_headroom_refresh_skip_interval_count: AtomicU64,
    disk_headroom_refresh_skip_inflight_count: AtomicU64,
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
        self.store.release_allocation(self.offset, self.alloc_size);
    }
}

impl BlockStore {
    /// Create or open a block store in `config.cache_dir`.
    ///
    /// * **New file** — writes a fresh superblock, starts empty.
    /// * **Existing file** — validates the superblock (magic, version, UUID).
    /// * **Metadata fast path** — loads `blocks.idx` and replays `blocks.wal`.
    /// * **Fallback** — scans `blocks.data` block headers when metadata is
    ///   missing or corrupt.
    pub fn open(config: BlockStoreConfig) -> Result<Self, BlockStoreError> {
        std::fs::create_dir_all(&config.cache_dir)?;
        let data_path = config.cache_dir.join("blocks.data");

        let (file, file_len) = if data_path.exists() {
            let file = OpenOptions::new().read(true).write(true).open(&data_path)?;
            // Re-apply the sparse attribute each open; NTFS preserves it, but
            // calling it again is idempotent and ensures existing non-sparse
            // files benefit from the optimisation after an upgrade.
            platform::make_sparse(&file)?;
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

            // Enable the sparse-file attribute before the first set_len so
            // that future file-extension calls are not stalled by synchronous
            // NTFS zero-fill (Windows only; no-op on other platforms).
            platform::make_sparse(&file)?;

            let sb = Superblock::new(config.max_size, config.volume_uuid);
            platform::pwrite(&file, &sb.to_bytes(), 0)?;
            file.set_len(SUPERBLOCK_SIZE)?;
            platform::fdatasync(&file)?;

            (file, SUPERBLOCK_SIZE)
        };

        let file_end = std::cmp::max(file_len, SUPERBLOCK_SIZE);
        let (recovered_entries, reset_wal) = match persistence::load_entries(
            &config.cache_dir,
            &file,
        ) {
            Ok(Some(entries)) => (entries, false),
            Ok(None) => {
                if file_end > SUPERBLOCK_SIZE {
                    warn!(
                        data_bytes = file_end - SUPERBLOCK_SIZE,
                        "cache metadata missing; rebuilding BlockStore index by scanning blocks.data",
                    );
                }
                (persistence::scan_data_file(&file)?, true)
            }
            Err(error) => {
                warn!(
                    error = ?error,
                    "cache metadata load failed; rebuilding BlockStore index by scanning blocks.data",
                );
                (persistence::scan_data_file(&file)?, true)
            }
        };

        let occupied: Vec<(u64, u64)> = recovered_entries
            .iter()
            .map(|entry| (entry.offset, entry.alloc_size))
            .collect();
        let allocator = if occupied.is_empty() && file_end == SUPERBLOCK_SIZE {
            FreeList::new(config.max_size)
        } else {
            FreeList::recover(config.max_size, file_end, &occupied)
        };

        let cache_dir = config.cache_dir.clone();
        let mmap_reader = MmapReader::new(&file, file_end)?;
        let emergency_disk_free_floor_bytes = Self::read_emergency_disk_free_floor_bytes();

        Ok(Self {
            file,
            config,
            mmap_reader,
            file_len: AtomicU64::new(file_end),
            index: Mutex::new(BlockIndex::from_persisted_entries(&recovered_entries)),
            wal: Mutex::new(WalAppender::open(&cache_dir, reset_wal)?),
            allocator: Mutex::new(allocator),
            file_growth: Mutex::new(()),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            eviction_bytes: AtomicU64::new(0),
            background_eviction_count: AtomicU64::new(0),
            background_eviction_bytes: AtomicU64::new(0),
            emergency_eviction_count: AtomicU64::new(0),
            emergency_eviction_bytes: AtomicU64::new(0),
            corruption_count: AtomicU64::new(0),
            read_ops: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            read_latency_total_ns: AtomicU64::new(0),
            read_latency_max_ns: AtomicU64::new(0),
            write_ops: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            write_latency_total_ns: AtomicU64::new(0),
            write_latency_max_ns: AtomicU64::new(0),
            advisory_dont_need_count: AtomicU64::new(0),
            emergency_disk_free_floor_bytes,
            disk_free_bytes: AtomicU64::new(0),
            disk_total_bytes: AtomicU64::new(0),
            disk_headroom_last_refresh_elapsed_ns: AtomicU64::new(0),
            disk_headroom_clock_start: Instant::now(),
            disk_headroom_refresh_in_progress: AtomicBool::new(false),
            disk_headroom_refresh_attempt_count: AtomicU64::new(0),
            disk_headroom_refresh_success_count: AtomicU64::new(0),
            disk_headroom_refresh_error_count: AtomicU64::new(0),
            disk_headroom_refresh_skip_interval_count: AtomicU64::new(0),
            disk_headroom_refresh_skip_inflight_count: AtomicU64::new(0),
            synced: AtomicBool::new(false),
        })
    }

    fn parse_positive_u64(raw: &str) -> Option<u64> {
        raw.trim().parse::<u64>().ok().filter(|value| *value > 0)
    }

    fn read_emergency_disk_free_floor_bytes() -> u64 {
        match std::env::var(EMERGENCY_DISK_FREE_FLOOR_ENV) {
            Ok(raw) => match Self::parse_positive_u64(&raw) {
                Some(value) => value,
                None => {
                    warn!(
                        env = EMERGENCY_DISK_FREE_FLOOR_ENV,
                        value = %raw,
                        default_bytes = DEFAULT_EMERGENCY_DISK_FREE_FLOOR_BYTES,
                        "invalid emergency free-space floor override; using default"
                    );
                    DEFAULT_EMERGENCY_DISK_FREE_FLOOR_BYTES
                }
            },
            Err(_) => DEFAULT_EMERGENCY_DISK_FREE_FLOOR_BYTES,
        }
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
        let started = Instant::now();
        // Phase 1: copy entry metadata under the index lock (no I/O inside lock).
        let (offset, data_size, checksum) = {
            let idx = self.index.lock();
            match idx.get(object_id) {
                Some(e) => (e.offset, e.data_size, e.checksum),
                None => {
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    self.record_read_metrics(0, started);
                    return None;
                }
            }
        };

        // Phase 2: mmap fast path with pread fallback outside the lock.
        let data = match self
            .mmap_reader
            .read(offset + BLOCK_HEADER_SIZE as u64, data_size as usize)
        {
            Ok(data) => data,
            Err(e) => {
                warn!(object_id, offset, %e, "block read failed");
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                self.drop_corrupt_entry_if_still_current(object_id, offset, data_size, checksum);
                self.record_read_metrics(0, started);
                return None;
            }
        };

        // Verify checksum.
        let actual_crc = crc32fast::hash(&data);
        if actual_crc != checksum {
            warn!(
                object_id,
                offset,
                expected = checksum,
                actual = actual_crc,
                "block checksum mismatch",
            );
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            self.drop_corrupt_entry_if_still_current(object_id, offset, data_size, checksum);
            self.record_read_metrics(0, started);
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
                self.record_read_metrics(0, started);
                return None;
            }

            idx.touch(object_id);
        }

        self.hit_count.fetch_add(1, Ordering::Relaxed);
        self.record_read_metrics(data.len() as u64, started);
        Some(data)
    }

    /// Read a byte range from a cached block directly into `out`.
    ///
    /// Returns the number of bytes copied, or `None` on a cache miss or
    /// integrity failure. The full payload checksum is verified before the
    /// access is reported as a hit.
    pub fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        let started = Instant::now();
        let (offset, data_size, checksum) = {
            let idx = self.index.lock();
            match idx.get(object_id) {
                Some(e) => (e.offset, e.data_size, e.checksum),
                None => {
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    self.record_read_metrics(0, started);
                    return None;
                }
            }
        };

        let (bytes_copied, actual_crc) = match self.mmap_reader.read_into(
            offset + BLOCK_HEADER_SIZE as u64,
            data_size as usize,
            data_offset,
            out,
        ) {
            Ok(result) => result,
            Err(e) => {
                warn!(object_id, offset, %e, "block ranged read failed");
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                self.drop_corrupt_entry_if_still_current(object_id, offset, data_size, checksum);
                self.record_read_metrics(0, started);
                return None;
            }
        };

        if actual_crc != checksum {
            warn!(
                object_id,
                offset,
                expected = checksum,
                actual = actual_crc,
                "block checksum mismatch",
            );
            self.miss_count.fetch_add(1, Ordering::Relaxed);
            self.drop_corrupt_entry_if_still_current(object_id, offset, data_size, checksum);
            self.record_read_metrics(0, started);
            return None;
        }

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
                self.record_read_metrics(0, started);
                return None;
            }

            idx.touch(object_id);
        }

        self.hit_count.fetch_add(1, Ordering::Relaxed);
        self.record_read_metrics(bytes_copied as u64, started);
        Some(bytes_copied)
    }

    /// Read a block for upload fallback and hint the OS that the pages can be
    /// dropped from cache afterwards where supported.
    pub fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
        let data = self.get(object_id)?;
        self.advise_dont_need(object_id);
        Some(data)
    }

    /// Check whether a block exists in the cache without reading data or
    /// updating the LRU order.
    pub fn contains(&self, object_id: u64) -> bool {
        self.index.lock().contains(object_id)
    }

    /// Submit a best-effort read-ahead hint for a cached block.
    pub fn advise_will_need(&self, object_id: u64) {
        let metadata = {
            let idx = self.index.lock();
            idx.get(object_id)
                .map(|entry| (entry.offset + BLOCK_HEADER_SIZE as u64, entry.data_size))
        };

        let (offset, data_size) = match metadata {
            Some(metadata) => metadata,
            None => return,
        };

        if let Err(error) = platform::advise_will_need(&self.file, offset, u64::from(data_size)) {
            warn!(object_id, offset, error = ?error, "explicit read-ahead hint failed");
        }
    }

    /// Submit a best-effort "drop pages" hint for a cached block.
    pub fn advise_dont_need(&self, object_id: u64) {
        let metadata = {
            let idx = self.index.lock();
            idx.get(object_id)
                .map(|entry| Self::upload_dont_need_range(entry.offset, entry.data_size))
        };

        let (offset, len) = match metadata {
            Some(metadata) => metadata,
            None => return,
        };

        if let Err(error) = platform::advise_dont_need(&self.file, offset, len) {
            warn!(
                object_id,
                offset,
                len,
                error = ?error,
                "explicit dont-need hint failed"
            );
            return;
        }

        self.advisory_dont_need_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn upload_dont_need_range(entry_offset: u64, data_size: u32) -> (u64, u64) {
        (
            entry_offset,
            u64::from(data_size).saturating_add(BLOCK_HEADER_SIZE as u64),
        )
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
        let write_started = Instant::now();
        let data_size = u32::try_from(data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "block too large for cache format (size exceeds u32::MAX)",
            )
        })?;
        let alloc = alloc_size_for(data_size);

        // Remove any existing entry for this object_id and free its space so
        // the allocator can reuse it. The old header is invalidated before the
        // lock is released so full data-file scans never see both versions as
        // live at once.
        if let Some(old) = self.remove_entry_for_overwrite(object_id)? {
            self.release_allocation(old.offset, old.alloc_size);
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
        let persisted_entry = PersistedIndexEntry {
            object_id,
            offset,
            data_size,
            alloc_size: alloc,
            checksum,
            flags,
        };
        let replaced = {
            let mut idx = self.index.lock();
            self.wal
                .lock()
                .append_put(persisted_entry)
                .map_err(BlockStoreError::Io)?;
            let replaced = idx.insert(object_id, offset, data_size, alloc, checksum, flags);
            if let Some(ref old) = replaced
                && let Err(error) = self.invalidate_block_header(old.offset)
            {
                warn!(
                    error = ?error,
                    object_id = old.object_id,
                    offset = old.offset,
                    "put: failed to invalidate superseded block header",
                );
            }
            replaced
        };
        pending_allocation.commit();

        if let Some(old) = replaced {
            self.release_allocation(old.offset, old.alloc_size);
        }

        if let Err(error) = self.mmap_reader.remap_if_needed(offset + alloc) {
            warn!(
                object_id,
                offset,
                error = ?error,
                "mmap remap after growth failed; reads will use pread fallback until the next remap",
            );
        }

        self.record_write_metrics(data.len() as u64, write_started);
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
        let new_flags = if pinned {
            BlockFlags::PINNED
        } else {
            BlockFlags::empty()
        };
        let disk_flags = persistence::persisted_flag_bits(new_flags);
        let header = BlockHeader::new(object_id, data_size, checksum, disk_flags);

        match platform::pwrite(&self.file, &header.to_bytes(), offset) {
            Ok(()) => {
                idx.modify_flags(
                    object_id,
                    BlockFlags::DIRTY | BlockFlags::UPLOADING,
                    BlockFlags::empty(),
                );
                if let Err(error) = self.wal.lock().append_update_flags(object_id, new_flags) {
                    warn!(
                        error = ?error,
                        object_id,
                        "mark_evictable: failed to append WAL update",
                    );
                }
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
        let removed = {
            let mut idx = self.index.lock();
            let mut wal = self.wal.lock();
            let removed = idx.remove(object_id);
            if let Some(ref entry) = removed {
                if let Err(error) = self.invalidate_block_header(entry.offset) {
                    warn!(
                        error = ?error,
                        object_id = entry.object_id,
                        offset = entry.offset,
                        "remove: failed to invalidate block header",
                    );
                }
                if let Err(error) = wal.append_remove(entry.object_id) {
                    warn!(
                        error = ?error,
                        object_id = entry.object_id,
                        "remove: failed to append WAL remove",
                    );
                }
            }
            removed
        };

        if let Some(removed) = removed {
            self.release_allocation(removed.offset, removed.alloc_size);
        }
    }

    /// Remove multiple blocks in a single call.
    ///
    /// Unknown IDs are silently skipped.  More efficient than calling
    /// [`remove`] in a loop because the allocator lock is acquired only once.
    pub fn remove_batch(&self, object_ids: &[u64]) {
        let removed = {
            let mut idx = self.index.lock();
            let mut wal = self.wal.lock();
            let removed = idx.remove_batch(object_ids);
            for entry in &removed {
                if let Err(error) = self.invalidate_block_header(entry.offset) {
                    warn!(
                        error = ?error,
                        object_id = entry.object_id,
                        offset = entry.offset,
                        "remove_batch: failed to invalidate block header",
                    );
                }
                if let Err(error) = wal.append_remove(entry.object_id) {
                    warn!(
                        error = ?error,
                        object_id = entry.object_id,
                        "remove_batch: failed to append WAL remove",
                    );
                }
            }
            removed
        };
        if removed.is_empty() {
            return;
        }
        self.release_allocations_batch(&removed);
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
        self.write_snapshot_and_clear_wal()?;
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

    /// Snapshot dirty object ids in deterministic oldest-first order.
    pub fn dirty_object_ids(&self) -> Vec<u64> {
        self.index.lock().dirty_object_ids()
    }

    /// Total cache hits since open.
    pub fn hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Total cache misses since open.
    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    /// Total number of blocks evicted from cache.
    pub fn eviction_count(&self) -> u64 {
        self.eviction_count.load(Ordering::Relaxed)
    }

    /// Total bytes reclaimed by eviction.
    pub fn eviction_bytes(&self) -> u64 {
        self.eviction_bytes.load(Ordering::Relaxed)
    }

    /// Number of blocks evicted by background watermark maintenance.
    pub fn background_eviction_count(&self) -> u64 {
        self.background_eviction_count.load(Ordering::Relaxed)
    }

    /// Bytes reclaimed by background watermark maintenance.
    pub fn background_eviction_bytes(&self) -> u64 {
        self.background_eviction_bytes.load(Ordering::Relaxed)
    }

    /// Number of blocks evicted by emergency disk-pressure handling.
    pub fn emergency_eviction_count(&self) -> u64 {
        self.emergency_eviction_count.load(Ordering::Relaxed)
    }

    /// Bytes reclaimed by emergency disk-pressure handling.
    pub fn emergency_eviction_bytes(&self) -> u64 {
        self.emergency_eviction_bytes.load(Ordering::Relaxed)
    }

    /// Number of entries dropped due to detected corruption/integrity failures.
    pub fn corruption_count(&self) -> u64 {
        self.corruption_count.load(Ordering::Relaxed)
    }

    /// Total read operations observed by the store.
    pub fn read_ops_count(&self) -> u64 {
        self.read_ops.load(Ordering::Relaxed)
    }

    /// Total bytes returned from successful reads.
    pub fn read_bytes(&self) -> u64 {
        self.read_bytes.load(Ordering::Relaxed)
    }

    /// Total accumulated read latency in nanoseconds.
    pub fn read_latency_total_ns(&self) -> u64 {
        self.read_latency_total_ns.load(Ordering::Relaxed)
    }

    /// Max observed read latency in nanoseconds.
    pub fn read_latency_max_ns(&self) -> u64 {
        self.read_latency_max_ns.load(Ordering::Relaxed)
    }

    /// Total successful writes.
    pub fn write_ops_count(&self) -> u64 {
        self.write_ops.load(Ordering::Relaxed)
    }

    /// Total bytes accepted on successful writes.
    pub fn write_bytes(&self) -> u64 {
        self.write_bytes.load(Ordering::Relaxed)
    }

    /// Total accumulated successful write latency in nanoseconds.
    pub fn write_latency_total_ns(&self) -> u64 {
        self.write_latency_total_ns.load(Ordering::Relaxed)
    }

    /// Max observed successful write latency in nanoseconds.
    pub fn write_latency_max_ns(&self) -> u64 {
        self.write_latency_max_ns.load(Ordering::Relaxed)
    }

    /// Number of attempted post-read dont-need hints issued (best-effort).
    pub fn advisory_dont_need_count(&self) -> u64 {
        self.advisory_dont_need_count.load(Ordering::Relaxed)
    }

    /// Number of attempted disk-headroom refresh syscalls.
    pub fn disk_headroom_refresh_attempt_count(&self) -> u64 {
        self.disk_headroom_refresh_attempt_count
            .load(Ordering::Relaxed)
    }

    /// Number of successful disk-headroom refresh syscalls.
    pub fn disk_headroom_refresh_success_count(&self) -> u64 {
        self.disk_headroom_refresh_success_count
            .load(Ordering::Relaxed)
    }

    /// Number of failed disk-headroom refresh syscalls.
    pub fn disk_headroom_refresh_error_count(&self) -> u64 {
        self.disk_headroom_refresh_error_count
            .load(Ordering::Relaxed)
    }

    /// Number of refresh requests skipped because refresh interval has not elapsed.
    pub fn disk_headroom_refresh_skip_interval_count(&self) -> u64 {
        self.disk_headroom_refresh_skip_interval_count
            .load(Ordering::Relaxed)
    }

    /// Number of refresh requests skipped because another thread is refreshing now.
    pub fn disk_headroom_refresh_skip_inflight_count(&self) -> u64 {
        self.disk_headroom_refresh_skip_inflight_count
            .load(Ordering::Relaxed)
    }

    /// Approximate number of avoided disk-headroom syscalls.
    pub fn disk_headroom_refresh_saved_syscall_count(&self) -> u64 {
        self.disk_headroom_refresh_skip_interval_count()
            .saturating_add(self.disk_headroom_refresh_skip_inflight_count())
    }

    /// Hit ratio in range `[0.0, 1.0]`.
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.hit_count();
        let misses = self.miss_count();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Miss ratio in range `[0.0, 1.0]`.
    pub fn miss_ratio(&self) -> f64 {
        let hits = self.hit_count();
        let misses = self.miss_count();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            misses as f64 / total as f64
        }
    }

    /// Effective read throughput derived from successful reads and observed
    /// read latency since the `BlockStore` was opened.
    pub fn read_throughput_bytes_per_sec(&self) -> f64 {
        let total_ns = self.read_latency_total_ns();
        if total_ns == 0 {
            0.0
        } else {
            self.read_bytes() as f64 / (total_ns as f64 / 1_000_000_000.0)
        }
    }

    /// Effective write throughput derived from successful writes and observed
    /// write latency since the `BlockStore` was opened.
    pub fn write_throughput_bytes_per_sec(&self) -> f64 {
        let total_ns = self.write_latency_total_ns();
        if total_ns == 0 {
            0.0
        } else {
            self.write_bytes() as f64 / (total_ns as f64 / 1_000_000_000.0)
        }
    }

    /// Path to the data file.
    pub fn data_path(&self) -> PathBuf {
        self.config.cache_dir.join("blocks.data")
    }

    /// Run best-effort cache pressure maintenance:
    /// 1. background watermark eviction when occupancy is high;
    /// 2. emergency eviction under low filesystem free-space pressure.
    pub fn run_pressure_maintenance(&self) -> PressureMaintenanceReport {
        let mut report = PressureMaintenanceReport::default();

        let (background_blocks, background_bytes) = self.evict_down_to_low_watermark();
        report.background_evicted_blocks = background_blocks;
        report.background_evicted_bytes = background_bytes;

        let (emergency_blocks, emergency_bytes) = self.evict_for_low_disk_headroom_if_needed(0);
        report.emergency_evicted_blocks = emergency_blocks;
        report.emergency_evicted_bytes = emergency_bytes;

        report
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
        let _ = self.evict_for_low_disk_headroom_if_needed(alloc);

        // First attempt without eviction.
        if let Some(offset) = self.try_allocate_and_extend(alloc)? {
            return Ok(offset);
        }

        // Eviction loop: free one block at a time until the allocation fits.
        loop {
            let evicted = self.evict_one_for_allocation()?;
            match evicted {
                Some(entry) => {
                    self.record_eviction(entry.alloc_size, EvictionReason::Allocation);
                    self.release_allocation(entry.offset, entry.alloc_size);
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
        // Phase 1 — logical allocation (fast, in-memory only).
        //
        // Acquire the allocator lock only long enough to advance the
        // in-memory free list and record the new logical `file_end`.  We
        // deliberately release the lock *before* calling `set_len` so that
        // concurrent allocations from existing free space can proceed while
        // one thread performs the (potentially slow) physical file extension.
        let (offset, needed_len) = {
            let mut a = self.allocator.lock();
            let offset = match a.allocate(alloc) {
                Ok(off) => off,
                Err(_) => return Ok(None),
            };
            let needed_len = a.file_end();
            (offset, needed_len)
        }; // allocator lock released

        // Phase 2 — physical file extension (may be slow on Windows without
        // sparse support due to synchronous NTFS zero-fill).
        //
        // The `file_growth` lock serialises concurrent extension attempts so
        // that the physical file always covers the logical `file_end` before
        // any offset in the newly-grown region is returned to a caller.  Only
        // one thread performs the actual `set_len`; the rest re-check after
        // acquiring the lock and skip if the file already reaches `needed_len`.
        let current_len = self.file_len.load(Ordering::Acquire);
        if needed_len > current_len {
            let _growth_guard = self.file_growth.lock();
            // Re-check: another thread may have extended the file while we
            // were waiting for the growth lock.
            let current_len = self.file_len.load(Ordering::Acquire);
            if needed_len > current_len {
                if let Err(error) =
                    platform::preallocate(&self.file, current_len, needed_len - current_len)
                {
                    warn!(
                        current_len,
                        needed_len,
                        error = ?error,
                        "best-effort data-file preallocation failed",
                    );
                }
                if let Err(error) = self.file.set_len(needed_len) {
                    // Growth failed: free the allocation we recorded in Phase 1
                    // so the allocator stays consistent with the physical file.
                    drop(_growth_guard);
                    let mut a = self.allocator.lock();
                    a.free(offset, alloc);
                    return Err(error.into());
                }
                // Publish the new file length only after the physical growth
                // has completed so Acquire readers never observe a length that
                // still lacks backing storage.
                self.file_len.store(needed_len, Ordering::Release);
            }
        }
        Ok(Some(offset))
    }

    fn record_read_metrics(&self, bytes: u64, started: Instant) {
        let elapsed_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.read_latency_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.read_latency_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn record_write_metrics(&self, bytes: u64, started: Instant) {
        let elapsed_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.write_latency_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.write_latency_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn record_eviction(&self, reclaimed_bytes: u64, reason: EvictionReason) {
        self.eviction_count.fetch_add(1, Ordering::Relaxed);
        self.eviction_bytes
            .fetch_add(reclaimed_bytes, Ordering::Relaxed);
        match reason {
            EvictionReason::Allocation => {}
            EvictionReason::BackgroundWatermark => {
                self.background_eviction_count
                    .fetch_add(1, Ordering::Relaxed);
                self.background_eviction_bytes
                    .fetch_add(reclaimed_bytes, Ordering::Relaxed);
            }
            EvictionReason::EmergencyDiskPressure => {
                self.emergency_eviction_count
                    .fetch_add(1, Ordering::Relaxed);
                self.emergency_eviction_bytes
                    .fetch_add(reclaimed_bytes, Ordering::Relaxed);
            }
        }
    }

    fn evict_bytes_for_reason(&self, target_bytes: u64, reason: EvictionReason) -> (u64, u64) {
        if target_bytes == 0 {
            return (0, 0);
        }

        let batchable_reason = matches!(
            reason,
            EvictionReason::BackgroundWatermark | EvictionReason::EmergencyDiskPressure
        );
        let mut evicted_blocks = 0u64;
        let mut evicted_bytes = 0u64;
        while evicted_bytes < target_bytes {
            let remaining = target_bytes.saturating_sub(evicted_bytes);
            let use_batch = batchable_reason && remaining >= PRESSURE_EVICTION_BATCH_TRIGGER_BYTES;

            if use_batch {
                let (blocks, bytes) = match self.evict_batch_for_maintenance(remaining, reason) {
                    Ok(result) => result,
                    Err(error) => {
                        warn!(
                            error = ?error,
                            target_bytes,
                            evicted_bytes,
                            "eviction pass aborted due to storage error"
                        );
                        break;
                    }
                };
                if blocks == 0 {
                    break;
                }
                evicted_blocks = evicted_blocks.saturating_add(blocks);
                evicted_bytes = evicted_bytes.saturating_add(bytes);
                continue;
            }

            let entry = match self.evict_one_for_allocation() {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(error) => {
                    warn!(
                        error = ?error,
                        target_bytes,
                        evicted_bytes,
                        "eviction pass aborted due to storage error"
                    );
                    break;
                }
            };

            evicted_blocks += 1;
            evicted_bytes = evicted_bytes.saturating_add(entry.alloc_size);
            self.record_eviction(entry.alloc_size, reason);
            self.release_allocation(entry.offset, entry.alloc_size);
        }

        (evicted_blocks, evicted_bytes)
    }

    fn evict_batch_for_maintenance(
        &self,
        target_bytes: u64,
        reason: EvictionReason,
    ) -> Result<(u64, u64), BlockStoreError> {
        let batch_target = target_bytes.min(PRESSURE_EVICTION_BATCH_TARGET_BYTES);
        let removed = {
            let mut idx = self.index.lock();
            let mut wal = self.wal.lock();
            let mut removed = Vec::with_capacity(PRESSURE_EVICTION_BATCH_MAX_BLOCKS);
            let mut reclaimed = 0u64;

            while reclaimed < batch_target && removed.len() < PRESSURE_EVICTION_BATCH_MAX_BLOCKS {
                let Some((object_id, offset)) = idx.peek_oldest_evictable() else {
                    break;
                };

                // Same publication ordering as single-entry eviction:
                // invalidate header before dropping index entry.
                self.invalidate_block_header(offset)?;
                let entry = idx
                    .remove(object_id)
                    .expect("peeked eviction candidate must still exist while index lock is held");

                if let Err(error) = wal.append_remove(entry.object_id) {
                    warn!(
                        error = ?error,
                        object_id = entry.object_id,
                        offset = entry.offset,
                        "evict_batch_for_maintenance: failed to append WAL remove",
                    );
                }
                reclaimed = reclaimed.saturating_add(entry.alloc_size);
                removed.push(entry);
            }

            removed
        };

        let mut evicted_blocks = 0u64;
        let mut evicted_bytes = 0u64;
        for entry in &removed {
            evicted_blocks += 1;
            evicted_bytes = evicted_bytes.saturating_add(entry.alloc_size);
            self.record_eviction(entry.alloc_size, reason);
        }
        if !removed.is_empty() {
            self.release_allocations_batch(&removed);
        }

        Ok((evicted_blocks, evicted_bytes))
    }

    fn evict_down_to_low_watermark(&self) -> (u64, u64) {
        let allocated = self.allocated_bytes();
        let high = self
            .config
            .max_size
            .saturating_mul(BACKGROUND_EVICT_HIGH_WATERMARK_PERCENT)
            / 100;
        if allocated <= high {
            return (0, 0);
        }

        let low = self
            .config
            .max_size
            .saturating_mul(BACKGROUND_EVICT_LOW_WATERMARK_PERCENT)
            / 100;
        let target = allocated.saturating_sub(low);
        self.evict_bytes_for_reason(target, EvictionReason::BackgroundWatermark)
    }

    fn evict_for_low_disk_headroom_if_needed(&self, required_growth_bytes: u64) -> (u64, u64) {
        let Some((free, total)) = self.refresh_disk_headroom_if_needed() else {
            return (0, 0);
        };
        if total == 0 {
            return (0, 0);
        }

        let ratio_target = total.saturating_mul(EMERGENCY_DISK_FREE_RATIO_NUMERATOR)
            / EMERGENCY_DISK_FREE_RATIO_DENOMINATOR;
        let mut target_free = ratio_target.max(
            self.emergency_disk_free_floor_bytes
                .saturating_add(required_growth_bytes),
        );
        target_free = target_free.min(total);
        if free >= target_free {
            return (0, 0);
        }

        let need = target_free.saturating_sub(free);
        let (blocks, bytes) =
            self.evict_bytes_for_reason(need, EvictionReason::EmergencyDiskPressure);
        if blocks > 0 {
            warn!(
                free_bytes = free,
                total_bytes = total,
                target_free_bytes = target_free,
                need_bytes = need,
                evicted_blocks = blocks,
                evicted_bytes = bytes,
                "low disk headroom: emergency cache eviction pass completed"
            );
            self.refresh_disk_headroom_snapshot();
            if let Some((free_after, _)) = self.cached_disk_headroom()
                && free_after <= free
            {
                warn!(
                    free_bytes_before = free,
                    free_bytes_after = free_after,
                    evicted_blocks = blocks,
                    evicted_bytes = bytes,
                    "low disk headroom persists after emergency eviction; underlying filesystem may ignore hole-punch/dont-need hints"
                );
            }
        }
        (blocks, bytes)
    }

    fn disk_headroom_elapsed_ns(&self) -> u64 {
        self.disk_headroom_clock_start
            .elapsed()
            .as_nanos()
            .min(u64::MAX as u128) as u64
    }

    fn cached_disk_headroom(&self) -> Option<(u64, u64)> {
        let total = self.disk_total_bytes.load(Ordering::Acquire);
        if total == 0 {
            return None;
        }
        Some((self.disk_free_bytes.load(Ordering::Acquire), total))
    }

    fn refresh_disk_headroom_snapshot(&self) -> Option<(u64, u64)> {
        self.disk_headroom_refresh_attempt_count
            .fetch_add(1, Ordering::Relaxed);
        match crate::v2::platform::get_free_disk_space(&self.config.cache_dir) {
            Some((free, total)) => {
                self.disk_free_bytes.store(free, Ordering::Release);
                self.disk_total_bytes.store(total, Ordering::Release);
                self.disk_headroom_last_refresh_elapsed_ns
                    .store(self.disk_headroom_elapsed_ns(), Ordering::Release);
                self.disk_headroom_refresh_success_count
                    .fetch_add(1, Ordering::Relaxed);
                Some((free, total))
            }
            None => {
                self.disk_headroom_refresh_error_count
                    .fetch_add(1, Ordering::Relaxed);
                self.disk_headroom_last_refresh_elapsed_ns
                    .store(self.disk_headroom_elapsed_ns(), Ordering::Release);
                warn!(
                    cache_dir = %self.config.cache_dir.display(),
                    "failed to refresh disk headroom; falling back to cached value"
                );
                self.cached_disk_headroom()
            }
        }
    }

    fn refresh_disk_headroom_if_needed(&self) -> Option<(u64, u64)> {
        if self
            .disk_headroom_refresh_in_progress
            .load(Ordering::Acquire)
        {
            self.disk_headroom_refresh_skip_inflight_count
                .fetch_add(1, Ordering::Relaxed);
            return self.cached_disk_headroom();
        }

        let has_cached_snapshot = self.disk_total_bytes.load(Ordering::Acquire) != 0;
        let now_elapsed_ns = self.disk_headroom_elapsed_ns();
        let last_elapsed_ns = self
            .disk_headroom_last_refresh_elapsed_ns
            .load(Ordering::Acquire);
        if has_cached_snapshot
            && now_elapsed_ns.saturating_sub(last_elapsed_ns) < DISK_HEADROOM_REFRESH_INTERVAL_NS
        {
            self.disk_headroom_refresh_skip_interval_count
                .fetch_add(1, Ordering::Relaxed);
            return self.cached_disk_headroom();
        }
        if self
            .disk_headroom_refresh_in_progress
            .swap(true, Ordering::AcqRel)
        {
            self.disk_headroom_refresh_skip_inflight_count
                .fetch_add(1, Ordering::Relaxed);
            return self.cached_disk_headroom();
        }
        let refreshed = self.refresh_disk_headroom_snapshot();
        self.disk_headroom_refresh_in_progress
            .store(false, Ordering::Release);
        refreshed
    }

    fn drop_corrupt_entry_if_still_current(
        &self,
        object_id: u64,
        offset: u64,
        data_size: u32,
        checksum: u32,
    ) {
        let removed = {
            let mut idx = self.index.lock();
            let matches_snapshot = matches!(
                idx.get(object_id),
                Some(entry)
                    if entry.offset == offset
                        && entry.data_size == data_size
                        && entry.checksum == checksum
            );
            if !matches_snapshot {
                return;
            }

            if let Err(error) = self.invalidate_block_header(offset) {
                warn!(
                    object_id,
                    offset,
                    error = ?error,
                    "failed to invalidate corrupt block header before purge"
                );
            }

            let removed = idx.remove(object_id);
            if let Some(entry) = removed.as_ref()
                && let Err(error) = self.wal.lock().append_remove(entry.object_id)
            {
                warn!(
                    error = ?error,
                    object_id = entry.object_id,
                    offset = entry.offset,
                    "failed to append WAL remove for corrupt block purge",
                );
            }
            removed
        };

        if let Some(entry) = removed {
            self.corruption_count.fetch_add(1, Ordering::Relaxed);
            self.release_allocation(entry.offset, entry.alloc_size);
        }
    }

    fn remove_entry_for_overwrite(
        &self,
        object_id: u64,
    ) -> Result<Option<RemovedEntry>, BlockStoreError> {
        let mut idx = self.index.lock();
        let Some(offset) = idx.get(object_id).map(|entry| entry.offset) else {
            return Ok(None);
        };

        // Keep the index lock held across tombstoning + removal so a
        // concurrent get() cannot observe a still-indexed block whose header
        // has already been invalidated on disk.
        self.invalidate_block_header(offset)?;
        Ok(idx.remove(object_id))
    }

    fn evict_one_for_allocation(&self) -> Result<Option<RemovedEntry>, BlockStoreError> {
        let mut idx = self.index.lock();
        let Some((object_id, offset)) = idx.peek_oldest_evictable() else {
            return Ok(None);
        };

        // Same rationale as overwrite removal: tombstone publication must stay
        // ordered with index removal to avoid readers racing with a half-
        // published eviction.
        self.invalidate_block_header(offset)?;
        let removed = idx
            .remove(object_id)
            .expect("peeked eviction candidate must still exist while index lock is held");

        if let Err(error) = self.wal.lock().append_remove(removed.object_id) {
            warn!(
                error = ?error,
                object_id = removed.object_id,
                offset = removed.offset,
                "evict_one_for_allocation: failed to append WAL remove",
            );
        }

        Ok(Some(removed))
    }

    fn invalidate_block_header(&self, offset: u64) -> io::Result<()> {
        let tombstone = [0u8; BLOCK_HEADER_SIZE as usize];
        platform::pwrite(&self.file, &tombstone, offset)
    }

    fn release_allocation(&self, offset: u64, alloc_size: u64) {
        self.issue_allocation_release_hints(offset, alloc_size, None);
        self.allocator.lock().free(offset, alloc_size);
    }

    fn release_allocations_batch(&self, removed: &[RemovedEntry]) {
        for entry in removed {
            self.issue_allocation_release_hints(
                entry.offset,
                entry.alloc_size,
                Some(entry.object_id),
            );
        }

        let mut allocator = self.allocator.lock();
        for entry in removed {
            allocator.free(entry.offset, entry.alloc_size);
        }
    }

    fn issue_allocation_release_hints(&self, offset: u64, alloc_size: u64, object_id: Option<u64>) {
        if let Err(error) = platform::punch_hole(&self.file, offset, alloc_size) {
            warn!(
                object_id,
                offset,
                alloc_size,
                error = ?error,
                "hole punch failed for freed cache region",
            );
        }
        if let Err(error) = platform::advise_dont_need(&self.file, offset, alloc_size) {
            warn!(
                object_id,
                offset,
                alloc_size,
                error = ?error,
                "dont-need hint failed for freed cache region",
            );
        }
    }

    fn write_snapshot_and_clear_wal(&self) -> Result<(), BlockStoreError> {
        let entries = self.index.lock().snapshot_for_persistence();
        persistence::write_snapshot(&self.config.cache_dir, &entries)?;
        self.wal.lock().clear()?;
        Ok(())
    }

    #[cfg(test)]
    fn force_mapped_len_for_test(&self, mapped_len: u64) {
        self.mmap_reader
            .force_mapped_len_for_test(mapped_len)
            .unwrap();
    }

    #[cfg(test)]
    fn mapped_len_for_test(&self) -> u64 {
        self.mmap_reader.mapped_len_for_test()
    }

    #[cfg(test)]
    fn cached_file_len_for_test(&self) -> u64 {
        self.file_len.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn set_cached_disk_headroom_for_test(&self, free: u64, total: u64) {
        self.disk_free_bytes.store(free, Ordering::Release);
        self.disk_total_bytes.store(total, Ordering::Release);
    }

    #[cfg(test)]
    fn set_disk_headroom_last_refresh_to_now_for_test(&self) {
        self.disk_headroom_last_refresh_elapsed_ns
            .store(self.disk_headroom_elapsed_ns(), Ordering::Release);
    }

    #[cfg(test)]
    fn set_disk_headroom_refresh_in_progress_for_test(&self, in_progress: bool) {
        self.disk_headroom_refresh_in_progress
            .store(in_progress, Ordering::Release);
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
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use std::thread;

    use super::*;
    use crate::v2::format::{PAGE_SIZE, SUPERBLOCK_SIZE};
    use crate::v2::persistence::{WAL_HEADER_SIZE, WAL_RECORD_SIZE};
    use tempfile::TempDir;

    fn test_config(dir: &TempDir) -> BlockStoreConfig {
        BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: 64 * 1024 * 1024, // 64 MiB
            volume_uuid: [0xAA; 16],
        }
    }

    fn reopen_store_with_read_only_data_file(store: &mut BlockStore) {
        let data_path = store.data_path();
        store.file = OpenOptions::new().read(true).open(&data_path).unwrap();
        let file_len = store.file.metadata().unwrap().len();
        store.mmap_reader = MmapReader::new(&store.file, file_len).unwrap();
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
    fn file_len_cache_tracks_growth() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        let initial_len = store.cached_file_len_for_test();
        store.put(1, &vec![0xAB; PAGE_SIZE as usize], true).unwrap();

        let cached_len = store.cached_file_len_for_test();
        let actual_len = std::fs::metadata(store.data_path()).unwrap().len();

        assert!(cached_len >= initial_len);
        assert_eq!(cached_len, actual_len);
    }

    #[test]
    fn try_allocate_and_extend_rolls_back_on_set_len_failure() {
        let dir = TempDir::new().unwrap();
        let mut store = BlockStore::open(test_config(&dir)).unwrap();
        reopen_store_with_read_only_data_file(&mut store);

        let file_len_before = store.file_len.load(Ordering::Acquire);
        let file_end_before = store.allocator.lock().file_end();
        let allocated_before = store.allocator.lock().allocated_bytes();

        let result = store.try_allocate_and_extend(PAGE_SIZE);

        assert!(matches!(result, Err(BlockStoreError::Io(_))));
        assert_eq!(store.file_len.load(Ordering::Acquire), file_len_before);

        let allocator = store.allocator.lock();
        assert_eq!(allocator.file_end(), file_end_before);
        assert_eq!(allocator.allocated_bytes(), allocated_before);
    }

    #[test]
    fn disk_headroom_refresh_interval_uses_cached_value() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();
        store.set_cached_disk_headroom_for_test(123, 456);
        store.set_disk_headroom_last_refresh_to_now_for_test();

        let attempts_before = store.disk_headroom_refresh_attempt_count();
        let skipped_before = store.disk_headroom_refresh_skip_interval_count();
        let saved_before = store.disk_headroom_refresh_saved_syscall_count();

        let got = store.refresh_disk_headroom_if_needed();
        assert_eq!(got, Some((123, 456)));
        assert_eq!(store.disk_headroom_refresh_attempt_count(), attempts_before);
        assert_eq!(
            store.disk_headroom_refresh_skip_interval_count(),
            skipped_before + 1
        );
        assert_eq!(
            store.disk_headroom_refresh_saved_syscall_count(),
            saved_before + 1
        );
    }

    #[test]
    fn disk_headroom_refresh_inflight_uses_cached_value() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();
        store.set_cached_disk_headroom_for_test(111, 999);
        // Backdate last refresh marker so this call would refresh unless
        // blocked by the in-progress guard.
        let now = store.disk_headroom_elapsed_ns();
        let backdated =
            now.saturating_sub(DISK_HEADROOM_REFRESH_INTERVAL_NS.saturating_add(1_000_000));
        store
            .disk_headroom_last_refresh_elapsed_ns
            .store(backdated, Ordering::Release);
        store.set_disk_headroom_refresh_in_progress_for_test(true);

        let attempts_before = store.disk_headroom_refresh_attempt_count();
        let skipped_before = store.disk_headroom_refresh_skip_inflight_count();
        let saved_before = store.disk_headroom_refresh_saved_syscall_count();

        let got = store.refresh_disk_headroom_if_needed();
        assert_eq!(got, Some((111, 999)));
        assert_eq!(store.disk_headroom_refresh_attempt_count(), attempts_before);
        assert_eq!(
            store.disk_headroom_refresh_skip_inflight_count(),
            skipped_before + 1
        );
        assert_eq!(
            store.disk_headroom_refresh_saved_syscall_count(),
            saved_before + 1
        );

        // Cleanup marker so this fixture stays isolated for future calls.
        store.set_disk_headroom_refresh_in_progress_for_test(false);
    }

    #[test]
    fn disk_headroom_refresh_without_cache_bypasses_interval_skip() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();
        store.set_disk_headroom_last_refresh_to_now_for_test();
        store.set_cached_disk_headroom_for_test(0, 0);

        let attempts_before = store.disk_headroom_refresh_attempt_count();
        let skipped_before = store.disk_headroom_refresh_skip_interval_count();

        let _ = store.refresh_disk_headroom_if_needed();

        assert_eq!(
            store.disk_headroom_refresh_attempt_count(),
            attempts_before + 1
        );
        assert_eq!(
            store.disk_headroom_refresh_skip_interval_count(),
            skipped_before
        );
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

    #[test]
    fn read_into_copies_requested_range() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(7, b"abcdefgh", true).unwrap();

        let mut out = [0u8; 3];
        let read = store.read_into(7, 2, &mut out).unwrap();
        assert_eq!(read, 3);
        assert_eq!(&out, b"cde");
    }

    #[test]
    fn checksum_mismatch_purges_corrupt_entry() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"healthy", true).unwrap();

        let file = OpenOptions::new()
            .write(true)
            .open(store.data_path())
            .unwrap();
        platform::pwrite(&file, b"X", SUPERBLOCK_SIZE + BLOCK_HEADER_SIZE as u64).unwrap();

        assert!(
            store.get(1).is_none(),
            "corrupt read should surface as miss"
        );
        assert!(!store.contains(1), "corrupt entry should be purged");
        assert_eq!(store.block_count(), 0);
        assert_eq!(store.corruption_count(), 1);
    }

    #[test]
    fn get_for_upload_issues_dont_need_hint_counter() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(3, b"payload", true).unwrap();
        let before = store.advisory_dont_need_count();
        let got = store
            .get_for_upload(3)
            .expect("upload reread should still return payload");
        assert_eq!(&got[..], b"payload");
        assert_eq!(store.advisory_dont_need_count(), before + 1);
    }

    #[test]
    fn run_pressure_maintenance_evicts_down_to_watermark() {
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 10 * format::PAGE_SIZE,
            volume_uuid: [0xB0; 16],
        };
        let store = BlockStore::open(config).unwrap();

        for id in 0..10_u64 {
            store.put(id, b"x", true).unwrap();
        }
        let before = store.block_count();
        let report = store.run_pressure_maintenance();
        let after = store.block_count();

        assert!(report.background_evicted_blocks > 0);
        assert!(after < before);
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
    fn close_and_reopen_restores_blocks_and_flags() {
        let dir = TempDir::new().unwrap();
        let config = BlockStoreConfig {
            cache_dir: dir.path().to_path_buf(),
            max_size: SUPERBLOCK_SIZE + 3 * format::PAGE_SIZE,
            volume_uuid: [0x44; 16],
        };

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(1, b"dirty", false).unwrap();
        store.put(2, b"clean", true).unwrap();
        store.close().unwrap();

        let store2 = BlockStore::open(config).unwrap();
        assert_eq!(store2.block_count(), 2);
        assert_eq!(store2.dirty_count(), 1);
        assert_eq!(&store2.get(1).unwrap()[..], b"dirty");
        assert_eq!(&store2.get(2).unwrap()[..], b"clean");

        store2.put(3, b"new-clean", true).unwrap();
        store2.put(4, b"evict", true).unwrap();
        assert!(
            store2.contains(1),
            "dirty block should remain non-evictable after reopen"
        );
        assert!(
            !store2.contains(2),
            "oldest clean block should be evicted after reopen"
        );
    }

    #[test]
    fn clean_shutdown_overwrites_existing_snapshot() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(1, b"first", true).unwrap();
        store.close().unwrap();

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(2, b"second", true).unwrap();
        store.close().unwrap();

        let reopened = BlockStore::open(config).unwrap();
        assert_eq!(reopened.block_count(), 2);
        assert_eq!(&reopened.get(1).unwrap()[..], b"first");
        assert_eq!(&reopened.get(2).unwrap()[..], b"second");
    }

    #[test]
    fn reopen_after_abrupt_drop_replays_snapshot_and_wal() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(1, b"snap", true).unwrap();
        store.close().unwrap();

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(2, b"wal-clean", true).unwrap();
        store.put(3, b"wal-dirty", false).unwrap();
        drop(store);

        let reopened = BlockStore::open(config).unwrap();
        assert_eq!(reopened.block_count(), 3);
        assert_eq!(reopened.dirty_count(), 1);
        assert_eq!(&reopened.get(1).unwrap()[..], b"snap");
        assert_eq!(&reopened.get(2).unwrap()[..], b"wal-clean");
        assert_eq!(&reopened.get(3).unwrap()[..], b"wal-dirty");
    }

    #[test]
    fn reopen_ignores_partial_wal_tail() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(1, b"snap", true).unwrap();
        store.close().unwrap();

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(2, b"keep", true).unwrap();
        store.put(3, b"drop", true).unwrap();
        drop(store);

        let wal_path = dir.path().join("blocks.wal");
        let wal_file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        wal_file
            .set_len(WAL_HEADER_SIZE + WAL_RECORD_SIZE + WAL_RECORD_SIZE / 2)
            .unwrap();

        let reopened = BlockStore::open(config).unwrap();
        assert!(reopened.contains(1));
        assert!(reopened.contains(2));
        assert!(!reopened.contains(3));
    }

    #[test]
    fn corrupt_snapshot_falls_back_to_data_scan() {
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);

        let store = BlockStore::open(config.clone()).unwrap();
        store.put(1, b"alpha", true).unwrap();
        store.put(2, b"beta", false).unwrap();
        store.close().unwrap();

        let snapshot_path = dir.path().join("blocks.idx");
        let snapshot_file = OpenOptions::new().write(true).open(&snapshot_path).unwrap();
        platform::pwrite(&snapshot_file, b"BROKEN!!", 0).unwrap();

        let reopened = BlockStore::open(config).unwrap();
        assert_eq!(reopened.block_count(), 2);
        assert_eq!(reopened.dirty_count(), 1);
        assert_eq!(&reopened.get(1).unwrap()[..], b"alpha");
        assert_eq!(&reopened.get(2).unwrap()[..], b"beta");
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

    #[test]
    fn parse_positive_u64_accepts_only_positive_numbers() {
        assert_eq!(BlockStore::parse_positive_u64("1"), Some(1));
        assert_eq!(BlockStore::parse_positive_u64(" 42 "), Some(42));
        assert_eq!(BlockStore::parse_positive_u64("0"), None);
        assert_eq!(BlockStore::parse_positive_u64("-7"), None);
        assert_eq!(BlockStore::parse_positive_u64("abc"), None);
    }

    #[test]
    fn upload_dont_need_range_is_page_aligned_to_block_start() {
        let entry_offset = SUPERBLOCK_SIZE + PAGE_SIZE;
        let data_size = 8192u32;
        let (offset, len) = BlockStore::upload_dont_need_range(entry_offset, data_size);

        assert_eq!(offset, entry_offset);
        assert_eq!(offset % PAGE_SIZE, 0);
        assert_eq!(len, u64::from(data_size) + BLOCK_HEADER_SIZE as u64);
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

    #[test]
    fn get_falls_back_when_mapping_lags() {
        let dir = TempDir::new().unwrap();
        let store = BlockStore::open(test_config(&dir)).unwrap();

        store.put(1, b"alpha", true).unwrap();
        store.put(2, b"beta", true).unwrap();

        store.force_mapped_len_for_test(SUPERBLOCK_SIZE + PAGE_SIZE);
        assert_eq!(store.mapped_len_for_test(), SUPERBLOCK_SIZE + PAGE_SIZE);

        let got = store
            .get(2)
            .expect("stale mappings should fall back to pread");
        assert_eq!(&got[..], b"beta");
        assert!(store.mapped_len_for_test() > SUPERBLOCK_SIZE + PAGE_SIZE);
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
    fn concurrent_reads_survive_remap_pressure() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(BlockStore::open(test_config(&dir)).unwrap());

        let payloads: Vec<Vec<u8>> = (0u64..32)
            .map(|id| format!("payload-{id}").into_bytes())
            .collect();
        for (id, payload) in payloads.iter().enumerate() {
            store.put(id as u64, payload, true).unwrap();
        }

        let full_len = std::fs::metadata(store.data_path()).unwrap().len();
        let narrow_len = SUPERBLOCK_SIZE + PAGE_SIZE;
        store.force_mapped_len_for_test(narrow_len);

        let mut handles = Vec::new();
        for _ in 0..4 {
            let store = Arc::clone(&store);
            let payloads = payloads.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..40 {
                    for (id, payload) in payloads.iter().enumerate() {
                        let got = store
                            .get(id as u64)
                            .expect("all blocks should stay readable");
                        assert_eq!(&got[..], &payload[..]);
                    }
                }
            }));
        }

        for _ in 0..50 {
            store.force_mapped_len_for_test(narrow_len);
            store.force_mapped_len_for_test(full_len);
        }

        for handle in handles {
            handle.join().unwrap();
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

//! DiskCache - persistent object cache with LRU eviction and disk space monitoring.
//!
//! Architecture v2 Key Changes:
//! - Default disk cache size: 50 GB (was 100 GB)
//! - Minimum disk cache size: 1 GB (don't evict below this)
//! - Target free disk space: 10% (was 15%)
//! - Objects keyed by sequential block ID (not content hash)
//!
//! Objects are stored as individual files in a cache directory, keyed by their 64-bit ID.

use bytes::Bytes;
use memmap2::MmapOptions;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// =============================================================================
// Configuration Constants (v2 Architecture)
// =============================================================================

/// Default disk cache size: 50 GB
pub const DEFAULT_DISK_CACHE_SIZE: u64 = 50 * 1024 * 1024 * 1024;

/// Minimum disk cache size: 1 GB (don't evict below this)
pub const MIN_DISK_CACHE_SIZE: u64 = 1 * 1024 * 1024 * 1024;

/// Target free disk space ratio: 10%
pub const DEFAULT_FREE_RATIO: f64 = 0.10;

/// GC interval: 60 seconds
pub const GC_INTERVAL_SECS: u64 = 60;

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
pub enum DiskCacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("cache directory not found: {0}")]
    NoCacheDir(String),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("other: {0}")]
    Other(String),
}

pub type DiskCacheResult<T> = Result<T, DiskCacheError>;

// =============================================================================
// Cache Configuration
// =============================================================================

/// Configuration for disk cache behavior.
#[derive(Debug, Clone)]
pub struct DiskCacheConfig {
    /// Maximum disk cache size in bytes.
    pub max_size: u64,
    /// Minimum disk cache size in bytes (don't evict below this).
    pub min_size: u64,
    /// Target free disk space ratio (0.0-1.0).
    pub target_free_ratio: f64,
    /// Cache directory path.
    pub cache_dir: PathBuf,
    /// Store decompressed block data instead of compressed STFS objects.
    /// Faster reads (no decompression on cache hit), uses more disk space.
    /// Should be true for low-compressibility workloads (VFX, already-compressed media).
    pub store_decompressed: bool,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_DISK_CACHE_SIZE,
            min_size: MIN_DISK_CACHE_SIZE,
            target_free_ratio: DEFAULT_FREE_RATIO,
            cache_dir: PathBuf::from(".cache"),
            store_decompressed: true,
        }
    }
}

// =============================================================================
// Internal Types
// =============================================================================

/// Entry in the LRU tracking.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Size of cached object in bytes.
    size: u64,
    /// Last access time (for LRU).
    last_access: Instant,
}

/// Key for a disk cache entry (Object ID / Block ID).
type ObjectId = u64;

/// Number of shards for the LRU entries map.
/// 256 shards means O(1) lock contention at 16+ concurrent readers.
const ENTRIES_SHARD_COUNT: usize = 256;

/// Map an ObjectId to a shard index.
#[inline]
fn entries_shard(id: ObjectId) -> usize {
    (id as usize) % ENTRIES_SHARD_COUNT
}

// =============================================================================
// DiskCache Implementation
// =============================================================================

/// Persistent disk cache with size-based and free-space-based eviction.
///
/// Key features:
/// - LRU eviction when cache exceeds max_size
/// - Disk space monitoring - evict if free space < target_free_ratio
/// - Minimum size guarantee - never evict below min_size
/// - Pinned objects - prevent eviction of pending uploads
/// - Sharded entry map (256 shards) for reduced lock contention
pub struct DiskCache {
    /// Cache directory for objects.
    cache_dir: PathBuf,
    /// Configuration.
    config: DiskCacheConfig,
    /// Sharded LRU entry tracking (256 shards by id % 256).
    entries: Vec<Mutex<BTreeMap<ObjectId, CacheEntry>>>,
    /// Set of pinned objects (prevent eviction). Pinning is rare, single mutex is fine.
    pinned: Mutex<std::collections::HashSet<ObjectId>>,
    /// Current total cache size (lock-free).
    current_size: AtomicU64,
}

impl DiskCache {
    /// Create a new disk cache with default configuration.
    pub fn new(cache_dir: impl AsRef<Path>, max_size_bytes: u64) -> DiskCacheResult<Arc<Self>> {
        let config = DiskCacheConfig {
            max_size: max_size_bytes,
            min_size: MIN_DISK_CACHE_SIZE,
            target_free_ratio: DEFAULT_FREE_RATIO,
            cache_dir: cache_dir.as_ref().to_path_buf(),
            store_decompressed: true,
        };
        Self::with_config(config)
    }

    /// Create a new disk cache with custom configuration.
    pub fn with_config(config: DiskCacheConfig) -> DiskCacheResult<Arc<Self>> {
        let objects_dir = config.cache_dir.join("objects");
        fs::create_dir_all(&objects_dir)?;

        let cache = Self {
            cache_dir: objects_dir,
            config,
            entries: (0..ENTRIES_SHARD_COUNT)
                .map(|_| Mutex::new(BTreeMap::new()))
                .collect(),
            pinned: Mutex::new(std::collections::HashSet::new()),
            current_size: AtomicU64::new(0),
        };

        // Scan existing cache on startup
        cache.scan_existing()?;

        info!(
            "disk cache initialized: dir={:?}, max={}GB, min={}GB",
            cache.cache_dir,
            cache.config.max_size / (1024 * 1024 * 1024),
            cache.config.min_size / (1024 * 1024 * 1024)
        );

        // Wrap in Arc for background monitoring
        let cache = Arc::new(cache);

        // Spawn background GC monitor
        let monitor_cache = cache.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(GC_INTERVAL_SECS));
            loop {
                interval.tick().await;
                if let Err(e) = monitor_cache.enforce_limits().await {
                    error!("disk cache background GC failed: {}", e);
                }
            }
        });

        Ok(cache)
    }

    /// Scan existing cache files and populate LRU tracking.
    fn scan_existing(&self) -> DiskCacheResult<()> {
        let mut total_size = 0u64;

        if !self.cache_dir.exists() {
            return Ok(());
        }

        // Collect all (id, size) pairs first, then distribute to shards.
        let mut found: Vec<(ObjectId, u64)> = Vec::new();

        let mut stack = vec![self.cache_dir.clone()];
        while let Some(dir) = stack.pop() {
            let read_dir = match fs::read_dir(&dir) {
                Ok(rd) => rd,
                Err(_) => continue,
            };

            for entry in read_dir {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.is_file() {
                    if let Some(id) = self.parse_filename(&path) {
                        if let Ok(meta) = entry.metadata() {
                            found.push((id, meta.len()));
                            total_size += meta.len();
                        }
                    }
                }
            }
        }

        // Distribute into shards
        for (id, size) in found {
            self.entries[entries_shard(id)].lock().insert(
                id,
                CacheEntry { size, last_access: Instant::now() },
            );
        }
        self.current_size.store(total_size, Ordering::Relaxed);

        debug!(
            "scanned existing cache entries: total_size={total_size} bytes"
        );
        Ok(())
    }

    /// Get the file path for an object.
    fn object_path(&self, id: ObjectId) -> PathBuf {
        let hash_str = format!("{:016x}", id); // 16 chars hex
        let prefix = &hash_str[0..2];
        let sub_prefix = &hash_str[2..4];

        self.cache_dir
            .join(prefix)
            .join(sub_prefix)
            .join(&hash_str)
    }

    /// Parse a filename back to ObjectId.
    fn parse_filename(&self, path: &Path) -> Option<ObjectId> {
        let name = path.file_name()?.to_str()?;
        u64::from_str_radix(name, 16).ok()
    }

    /// Read an object from disk cache.
    pub async fn get_object(&self, id: ObjectId) -> DiskCacheResult<Option<Bytes>> {
        let path = self.object_path(id);

        let data = tokio::task::spawn_blocking(move || -> std::io::Result<Option<Bytes>> {
            // Direct open to avoid TOCTOU race (Time-Of-Check-Time-Of-Use)
            let file = match fs::File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug!("[DISK-CACHE-MISS] id={:016x} path={:?}", id, path);
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };

            // Use mmap for efficient reads: OS page cache manages prefetching,
            // and the map is immediately copied and dropped so the file is
            // not held open after this function returns (safe for LRU eviction).
            let mmap = unsafe { MmapOptions::new().map(&file)? };
            let bytes = Bytes::copy_from_slice(&mmap);
            // mmap and file are dropped here — file is closed before eviction can run

            debug!("[DISK-CACHE-READ] id={:016x} size={} path={:?}", id, bytes.len(), path);

            Ok(Some(bytes))
        }).await??;

        if data.is_some() {
            // Update last access time in the correct shard.
            let mut shard = self.entries[entries_shard(id)].lock();
            if let Some(entry) = shard.get_mut(&id) {
                entry.last_access = Instant::now();
            } else if let Some(d) = &data {
                // Inconsistency (e.g. written externally) — re-register.
                shard.insert(id, CacheEntry {
                    size: d.len() as u64,
                    last_access: Instant::now(),
                });
            }
        }

        Ok(data)
    }

    /// Write an object to disk cache.
    pub async fn put_object(&self, id: ObjectId, data: &[u8]) -> DiskCacheResult<()> {
        let size = data.len() as u64;

        debug!("[DISK-CACHE-WRITE] id={:016x} size={}", id, size);

        // Evict if needed
        self.reserve_space(size).await?;

        let path = self.object_path(id);
        let data_vec = data.to_vec();

        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            // Ensure parent dirs exist
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }

            // Atomic write: write to temp then rename
            // Use UUID to avoid collisions with concurrent writers for the same object
            let tmp_path = path.with_extension(format!("tmp.{}", Uuid::new_v4()));

            {
                let mut file = fs::File::create(&tmp_path)?;
                file.write_all(&data_vec)?;
                // NOTE: sync_all() omitted for disk cache writes.
                // Disk cache contains data that is always re-fetchable from S3,
                // so fsync is not required for correctness (trade-off: can survive crashes at cost of re-fetching).
                // Keep fsync ONLY for writeback journal (critical metadata).
            }

            fs::rename(&tmp_path, &path)?;

            // Verify file was written correctly
            if let Ok(metadata) = fs::metadata(&path) {
                debug!("[DISK-CACHE-WRITE-OK] path={:?} written_size={}", path, metadata.len());
            } else {
                warn!("[DISK-CACHE-WRITE-FAIL] path={:?} - file not found after rename!", path);
            }

            Ok(())
        }).await??;

        let mut shard = self.entries[entries_shard(id)].lock();
        if let Some(old) = shard.insert(
            id,
            CacheEntry { size, last_access: Instant::now() },
        ) {
            self.current_size.fetch_sub(old.size, Ordering::Relaxed);
        }
        self.current_size.fetch_add(size, Ordering::Relaxed);

        Ok(())
    }

    /// Delete an object from cache.
    pub async fn remove_object(&self, id: ObjectId) -> DiskCacheResult<()> {
        let path = self.object_path(id);

        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            if path.exists() {
                fs::remove_file(&path)?;
            }
            Ok(())
        }).await??;

        let mut shard = self.entries[entries_shard(id)].lock();
        if let Some(entry) = shard.remove(&id) {
            self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Check if object exists.
    pub fn exists(&self, id: ObjectId) -> bool {
        self.entries[entries_shard(id)].lock().contains_key(&id)
    }

    /// Reserve space for `needed` bytes by evicting LRU.
    /// Also checks free disk space.
    pub async fn reserve_space(&self, needed: u64) -> DiskCacheResult<()> {
        self.evict_for(needed).await
    }

    /// Enforce size and disk space limits (Background GC).
    pub async fn enforce_limits(&self) -> DiskCacheResult<()> {
        self.evict_for(0).await
    }

    /// Pin an object to prevent it from being evicted.
    pub fn pin(&self, id: ObjectId) {
        self.pinned.lock().insert(id);
    }

    /// Unpin an object, allowing it to be evicted.
    pub fn unpin(&self, id: ObjectId) {
        self.pinned.lock().remove(&id);
    }

    /// Check if an object is pinned.
    pub fn is_pinned(&self, id: ObjectId) -> bool {
        self.pinned.lock().contains(&id)
    }

    /// Evict blocks to make room for new data.
    ///
    /// Eviction policy (v2):
    /// 1. Check if cache + needed > max_size -> need eviction
    /// 2. Check if disk space < target_free_ratio -> need eviction
    /// 3. Don't evict below min_size
    /// 4. Skip pinned blocks (pending upload)
    /// 5. Evict LRU blocks
    async fn evict_for(&self, needed_bytes: u64) -> DiskCacheResult<()> {
        let mut evicted = 0u64;
        let min_size = self.config.min_size;

        loop {
            // Check if we have enough space
            let current = self.current_size.load(Ordering::Relaxed);
            let size_ok = current + needed_bytes <= self.config.max_size;

            // Check disk space
            let disk_ok = !self.is_disk_space_low().await;

            if size_ok && disk_ok {
                break;
            }

            // Don't evict below minimum
            if current <= min_size {
                warn!(
                    "cache at minimum size ({}GB), cannot evict further",
                    min_size / (1024 * 1024 * 1024)
                );
                break;
            }

            // Find LRU candidate
            let candidate = self.find_lru_candidate()?;

            match candidate {
                Some(object_id) => {
                    // Skip pinned blocks
                    if self.is_pinned(object_id) {
                        continue;
                    }

                    // Delete the file
                    self.delete_object(object_id).await?;

                    // Update tracking via shard
                    let evicted_size = {
                        let mut shard = self.entries[entries_shard(object_id)].lock();
                        if let Some(entry) = shard.remove(&object_id) {
                            self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
                            Some(entry.size)
                        } else {
                            None
                        }
                    };

                    if let Some(size) = evicted_size {
                        evicted += size;
                        debug!(
                            id = %format!("{:016x}", object_id),
                            evicted_size = size,
                            total_evicted = evicted,
                            "evicted from disk cache"
                        );
                    }
                }
                None => break, // No evictable blocks
            }
        }

        if evicted > 0 {
            info!("evicted {} bytes from disk cache", evicted);
        }

        Ok(())
    }

    /// Find the LRU candidate for eviction (approximate global LRU across all shards).
    fn find_lru_candidate(&self) -> DiskCacheResult<Option<ObjectId>> {
        let pinned = self.pinned.lock();
        let mut best: Option<(ObjectId, Instant)> = None;

        for shard in &self.entries {
            let s = shard.lock();
            for (k, v) in s.iter() {
                if pinned.contains(k) {
                    continue;
                }
                let is_older = best.map_or(true, |(_, t)| v.last_access < t);
                if is_older {
                    best = Some((*k, v.last_access));
                }
            }
        }

        Ok(best.map(|(id, _)| id))
    }

    /// Delete an object file from disk.
    async fn delete_object(&self, id: ObjectId) -> DiskCacheResult<()> {
        let path = self.object_path(id);

        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            if path.exists() {
                fs::remove_file(&path)?;
            }
            Ok(())
        }).await??;

        Ok(())
    }

    /// Check if disk space is below the target free ratio.
    async fn is_disk_space_low(&self) -> bool {
        let path = self.cache_dir.clone();
        let target_ratio = self.config.target_free_ratio;

        tokio::task::spawn_blocking(move || {
            match get_free_disk_space(&path) {
                Some((free, total)) => {
                    if total == 0 { return false; }
                    let ratio = free as f64 / total as f64;
                    if ratio < target_ratio {
                        warn!(
                            "disk space low: {:.1}% free ({} / {} bytes), target: {:.1}%",
                            ratio * 100.0,
                            free,
                            total,
                            target_ratio * 100.0
                        );
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        }).await.unwrap_or(false)
    }

    /// Current cache size in bytes (lock-free).
    pub fn current_size(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Get cache configuration.
    pub fn config(&self) -> &DiskCacheConfig {
        &self.config
    }
}

// =============================================================================
// Platform-specific Disk Space Functions
// =============================================================================

/// Get free and total disk space for the partition containing the given path.
#[cfg(windows)]
fn get_free_disk_space(path: &Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;

    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }

    let wide_path: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    let mut free_available: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut free_bytes: u64 = 0;

    unsafe {
        let ret = GetDiskFreeSpaceExW(
            wide_path.as_ptr(),
            &mut free_available,
            &mut total_bytes,
            &mut free_bytes,
        );
        if ret != 0 {
            Some((free_bytes, total_bytes))
        } else {
            None
        }
    }
}

#[cfg(unix)]
fn get_free_disk_space(path: &Path) -> Option<(u64, u64)> {
    use std::ffi::CString;

    let c_path = CString::new(path.to_str()?).ok()?;
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            let free = stat.f_bfree as u64 * stat.f_bsize as u64;
            let total = stat.f_blocks as u64 * stat.f_bsize as u64;
            Some((free, total))
        } else {
            None
        }
    }
}

#[cfg(not(any(windows, unix)))]
fn get_free_disk_space(_path: &Path) -> Option<(u64, u64)> {
    None
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DiskCacheConfig::default();
        assert_eq!(config.max_size, DEFAULT_DISK_CACHE_SIZE);
        assert_eq!(config.min_size, MIN_DISK_CACHE_SIZE);
        assert!((config.target_free_ratio - DEFAULT_FREE_RATIO).abs() < 0.001);
    }

    #[test]
    fn test_object_path_format() {
        let config = DiskCacheConfig {
            cache_dir: PathBuf::from("/tmp/cache"),
            ..Default::default()
        };
        let cache = DiskCache {
            cache_dir: config.cache_dir.join("objects"),
            config,
            entries: (0..ENTRIES_SHARD_COUNT)
                .map(|_| Mutex::new(BTreeMap::new()))
                .collect(),
            pinned: Mutex::new(std::collections::HashSet::new()),
            current_size: AtomicU64::new(0),
        };

        // Test path generation
        let path = cache.object_path(0x123456789abcdef0);
        assert!(path.ends_with("12/34/123456789abcdef0"));
    }
}

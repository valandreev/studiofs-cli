//! DiskCache - persistent object cache with LRU eviction and disk space monitoring.
//!
//! Architecture Key Changes (v2 — slab-backed):
//! - Default disk cache size: 50 GB (was 100 GB)
//! - Minimum disk cache size: 1 GB (don't evict below this)
//! - Target free disk space: 10% (was 15%)
//! - Objects keyed by sequential block ID (not content hash)
//!
//! Blocks are packed into large slab files (~2 GB each) that stay permanently
//! memory-mapped.  Reads are a simple HashMap lookup + memcpy from the mmap —
//! zero syscalls on cache hit.  Old file-per-block scheme is replaced entirely.

use bytes::Bytes;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tracing::{error, info, warn};

use crate::slab::SlabStore;

// =============================================================================
// Configuration Constants
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

/// Key for a disk cache entry (Object ID / Block ID).
type ObjectId = u64;

// =============================================================================
// DiskCache Implementation
// =============================================================================

/// Persistent disk cache with size-based and free-space-based eviction.
///
/// v2: backed by large slab files (~2 GB each) instead of one file per block.
/// Reads are always synchronous (HashMap lookup + memcpy from persistent mmap).
///
/// Key features:
/// - LRU eviction when cache exceeds max_size
/// - Disk space monitoring — evict if free space < target_free_ratio
/// - Minimum size guarantee — never evict below min_size
/// - Pinned objects — prevent eviction of pending uploads
pub struct DiskCache {
    /// Configuration.
    config: DiskCacheConfig,
    /// Slab-backed storage.
    store: SlabStore,
    /// GC interval counter — persists index every N GC runs.
    gc_counter: AtomicU64,
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
        let slab_dir = config.cache_dir.join("slabs");
        fs::create_dir_all(&slab_dir)?;

        let store = SlabStore::open(&slab_dir, config.max_size, config.min_size)?;

        info!(
            "disk cache initialized (slab-backed): dir={:?}, max={}GB, min={}GB, entries={}",
            slab_dir,
            config.max_size / (1024 * 1024 * 1024),
            config.min_size / (1024 * 1024 * 1024),
            store.entry_count(),
        );

        let cache = Arc::new(Self {
            config,
            store,
            gc_counter: AtomicU64::new(0),
        });

        // Spawn background GC monitor
        let monitor_cache = cache.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(GC_INTERVAL_SECS));
            loop {
                interval.tick().await;
                if let Err(e) = monitor_cache.enforce_limits().await {
                    error!("disk cache background GC failed: {}", e);
                }

                // Persist index every 5 GC runs (~5 min)
                let count = monitor_cache.gc_counter.fetch_add(1, Ordering::Relaxed);
                if count % 5 == 0 {
                    if let Err(e) = monitor_cache.store.persist_index() {
                        error!("failed to persist slab index: {}", e);
                    }
                }
            }
        });

        Ok(cache)
    }

    /// Read an object from disk cache.
    ///
    /// **Hot path** — synchronous slab lookup + memcpy from persistent mmap.
    /// No `spawn_blocking`, no syscalls.
    pub async fn get_object(&self, id: ObjectId) -> DiskCacheResult<Option<Bytes>> {
        Ok(self.store.get(id))
    }

    /// Write an object to disk cache.
    pub async fn put_object(&self, id: ObjectId, data: &[u8]) -> DiskCacheResult<()> {
        self.store.put(id, data)?;
        Ok(())
    }

    /// Delete an object from cache.
    pub async fn remove_object(&self, id: ObjectId) -> DiskCacheResult<()> {
        self.store.remove(id);
        Ok(())
    }

    /// Check if object exists.
    pub fn exists(&self, id: ObjectId) -> bool {
        self.store.contains(id)
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
        self.store.pin(id);
    }

    /// Unpin an object, allowing it to be evicted.
    pub fn unpin(&self, id: ObjectId) {
        self.store.unpin(id);
    }

    /// Check if an object is pinned.
    pub fn is_pinned(&self, id: ObjectId) -> bool {
        self.store.is_pinned(id)
    }

    /// Evict blocks to make room for new data.
    ///
    /// Eviction policy:
    /// 1. Check if cache + needed > max_size -> need eviction
    /// 2. Check if disk space < target_free_ratio -> need eviction
    /// 3. Don't evict below min_size
    /// 4. Skip pinned blocks (pending upload)
    /// 5. Evict via SlabStore LRU (delegated)
    async fn evict_for(&self, needed_bytes: u64) -> DiskCacheResult<()> {
        // SlabStore already handles LRU eviction + pinning in put().
        // Here we additionally check disk space and force eviction.
        let current = self.store.total_bytes();
        let size_ok = current + needed_bytes <= self.config.max_size;
        let disk_ok = !self.is_disk_space_low().await;

        if size_ok && disk_ok {
            return Ok(());
        }

        // Trigger SlabStore eviction (already done in put, but force it)
        self.store.evict_for(needed_bytes)?;

        Ok(())
    }

    /// Check if disk space is below the target free ratio.
    async fn is_disk_space_low(&self) -> bool {
        let path = self.config.cache_dir.clone();
        let target_ratio = self.config.target_free_ratio;

        tokio::task::spawn_blocking(move || match get_free_disk_space(&path) {
            Some((free, total)) => {
                if total == 0 {
                    return false;
                }
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
        })
        .await
        .unwrap_or(false)
    }

    /// Current cache size in bytes (lock-free).
    pub fn current_size(&self) -> u64 {
        self.store.total_bytes()
    }

    /// Get cache configuration.
    pub fn config(&self) -> &DiskCacheConfig {
        &self.config
    }

    /// Persist slab index to disk (call on shutdown).
    pub fn persist(&self) -> DiskCacheResult<()> {
        self.store.persist_index()?;
        Ok(())
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

    #[tokio::test]
    async fn test_slab_diskcache_basic() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(dir.path(), 100 * 1024 * 1024).unwrap();

        // Put
        let data = vec![0xAAu8; 4096];
        cache.put_object(1, &data).await.unwrap();

        // Get
        let got = cache.get_object(1).await.unwrap().unwrap();
        assert_eq!(got.as_ref(), &data[..]);

        // Exists
        assert!(cache.exists(1));
        assert!(!cache.exists(9999));

        // Remove
        cache.remove_object(1).await.unwrap();
        assert!(!cache.exists(1));
        let gone = cache.get_object(1).await.unwrap();
        assert!(gone.is_none());
    }

    #[tokio::test]
    async fn test_slab_diskcache_large_block() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(dir.path(), 100 * 1024 * 1024).unwrap();

        // 4MB block (typical)
        let data = vec![0xBBu8; 4 * 1024 * 1024];
        cache.put_object(42, &data).await.unwrap();

        let got = cache.get_object(42).await.unwrap().unwrap();
        assert_eq!(got.len(), 4 * 1024 * 1024);
        assert!(got.iter().all(|&b| b == 0xBB));
    }

    #[tokio::test]
    async fn test_slab_diskcache_pin_unpin() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(dir.path(), 100 * 1024 * 1024).unwrap();

        cache.put_object(1, &[1u8; 4096]).await.unwrap();
        assert!(!cache.is_pinned(1));

        cache.pin(1);
        assert!(cache.is_pinned(1));

        cache.unpin(1);
        assert!(!cache.is_pinned(1));
    }

    #[tokio::test]
    async fn test_slab_diskcache_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(dir.path(), 100 * 1024 * 1024).unwrap();

        cache.put_object(1, &[0xAAu8; 4096]).await.unwrap();
        cache.put_object(1, &[0xBBu8; 8192]).await.unwrap();

        let got = cache.get_object(1).await.unwrap().unwrap();
        assert_eq!(got.len(), 8192);
        assert!(got.iter().all(|&b| b == 0xBB));
    }

    #[tokio::test]
    async fn test_slab_diskcache_current_size() {
        let dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::new(dir.path(), 100 * 1024 * 1024).unwrap();

        assert_eq!(cache.current_size(), 0);
        cache.put_object(1, &[0u8; 4096]).await.unwrap();
        assert!(cache.current_size() > 0);
    }
}

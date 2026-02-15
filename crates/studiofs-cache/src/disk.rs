//! Disk cache — persistent block cache with LRU eviction and disk space monitoring.
//!
//! Blocks are stored as individual files in a cache directory.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use studiofs_meta::Ino;
use thiserror::Error;
use tracing::{debug, info, warn};

#[derive(Debug, Error)]
pub enum DiskCacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("cache directory not found: {0}")]
    NoCacheDir(String),
}

pub type DiskCacheResult<T> = Result<T, DiskCacheError>;

/// Entry in the LRU tracking.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Size of cached data in bytes.
    size: u64,
    /// Last access time (for LRU).
    last_access: Instant,
}

/// Key for a disk cache entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct DiskCacheKey {
    ino: Ino,
    offset: u64,
}

/// Persistent disk cache with size-based and free-space-based eviction.
pub struct DiskCache {
    cache_dir: PathBuf,
    /// Maximum cache size in bytes (default 100 GB).
    max_size: u64,
    /// Minimum free disk space ratio (0.0-1.0, default 0.15 = 15%).
    min_free_ratio: f64,
    /// Track cached entries for LRU eviction.
    entries: Mutex<BTreeMap<DiskCacheKey, CacheEntry>>,
    /// Current total cache size.
    current_size: Mutex<u64>,
}

impl DiskCache {
    /// Create a new disk cache.
    pub fn new(cache_dir: impl AsRef<Path>, max_size_bytes: u64) -> DiskCacheResult<Self> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        fs::create_dir_all(&cache_dir)?;

        let cache = Self {
            cache_dir,
            max_size: max_size_bytes,
            min_free_ratio: 0.15,
            entries: Mutex::new(BTreeMap::new()),
            current_size: Mutex::new(0),
        };

        // Scan existing cache on startup
        cache.scan_existing()?;

        info!(
            "disk cache initialized: dir={:?}, max={}GB",
            cache.cache_dir,
            cache.max_size / (1024 * 1024 * 1024)
        );
        Ok(cache)
    }

    /// Scan existing cache files and populate LRU tracking.
    fn scan_existing(&self) -> DiskCacheResult<()> {
        let mut entries = self.entries.lock();
        let mut total_size = 0u64;

        if !self.cache_dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&self.cache_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(key) = self.parse_cache_filename(&path) {
                    let meta = entry.metadata()?;
                    let size = meta.len();
                    entries.insert(
                        key,
                        CacheEntry {
                            size,
                            last_access: Instant::now(),
                        },
                    );
                    total_size += size;
                }
            }
        }

        *self.current_size.lock() = total_size;
        debug!("scanned {} existing cache entries ({total_size} bytes)", entries.len());
        Ok(())
    }

    /// Get the file path for a cache entry.
    fn cache_path(&self, ino: Ino, offset: u64) -> PathBuf {
        self.cache_dir.join(format!("{ino}_{offset}"))
    }

    /// Parse a cache filename back to a key.
    fn parse_cache_filename(&self, path: &Path) -> Option<DiskCacheKey> {
        let name = path.file_name()?.to_str()?;
        let mut parts = name.split('_');
        let ino: Ino = parts.next()?.parse().ok()?;
        let offset: u64 = parts.next()?.parse().ok()?;
        Some(DiskCacheKey { ino, offset })
    }

    /// Read a block from disk cache.
    pub fn get(&self, ino: Ino, offset: u64) -> DiskCacheResult<Option<Bytes>> {
        let path = self.cache_path(ino, offset);
        if !path.exists() {
            return Ok(None);
        }

        let mut file = fs::File::open(&path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        // Update last access time
        let key = DiskCacheKey { ino, offset };
        if let Some(entry) = self.entries.lock().get_mut(&key) {
            entry.last_access = Instant::now();
        }

        Ok(Some(Bytes::from(data)))
    }

    /// Write a block to disk cache.
    pub fn put(&self, ino: Ino, offset: u64, data: &[u8]) -> DiskCacheResult<()> {
        let size = data.len() as u64;

        // Evict if needed
        self.evict_to_fit(size)?;

        let path = self.cache_path(ino, offset);
        let mut file = fs::File::create(&path)?;
        file.write_all(data)?;

        let key = DiskCacheKey { ino, offset };
        let mut entries = self.entries.lock();
        if let Some(old) = entries.insert(
            key,
            CacheEntry {
                size,
                last_access: Instant::now(),
            },
        ) {
            *self.current_size.lock() -= old.size;
        }
        *self.current_size.lock() += size;

        Ok(())
    }

    /// Remove all cache entries for an inode.
    pub fn invalidate_inode(&self, ino: Ino) -> DiskCacheResult<()> {
        let mut entries = self.entries.lock();
        let mut current = self.current_size.lock();

        let keys_to_remove: Vec<DiskCacheKey> = entries
            .keys()
            .filter(|k| k.ino == ino)
            .cloned()
            .collect();

        for key in keys_to_remove {
            if let Some(entry) = entries.remove(&key) {
                *current = current.saturating_sub(entry.size);
                let path = self.cache_path(key.ino, key.offset);
                let _ = fs::remove_file(&path);
            }
        }

        Ok(())
    }

    /// Evict LRU entries until we have space for `needed` bytes.
    fn evict_to_fit(&self, needed: u64) -> DiskCacheResult<()> {
        let mut entries = self.entries.lock();
        let mut current = self.current_size.lock();

        // Check disk space
        let needs_space_eviction = self.is_disk_space_low();

        while *current + needed > self.max_size || needs_space_eviction {
            // Find LRU entry
            let lru_key = entries
                .iter()
                .min_by_key(|(_, v)| v.last_access)
                .map(|(k, _)| k.clone());

            if let Some(key) = lru_key {
                if let Some(entry) = entries.remove(&key) {
                    let path = self.cache_path(key.ino, key.offset);
                    if let Err(e) = fs::remove_file(&path) {
                        warn!("failed to remove cache file {:?}: {e}", path);
                    }
                    *current = current.saturating_sub(entry.size);
                    debug!(
                        ino = key.ino,
                        offset = key.offset,
                        "evicted from disk cache"
                    );
                }
            } else {
                break;
            }

            // Re-check after eviction
            if *current + needed <= self.max_size && !self.is_disk_space_low() {
                break;
            }
        }

        Ok(())
    }

    /// Check if disk space is below the minimum free ratio.
    fn is_disk_space_low(&self) -> bool {
        match get_free_disk_space(&self.cache_dir) {
            Some((free, total)) => {
                let ratio = free as f64 / total as f64;
                if ratio < self.min_free_ratio {
                    warn!(
                        "disk space low: {:.1}% free ({} / {} bytes)",
                        ratio * 100.0,
                        free,
                        total
                    );
                    true
                } else {
                    false
                }
            }
            None => false, // can't check, assume OK
        }
    }

    /// Current cache size in bytes.
    pub fn current_size(&self) -> u64 {
        *self.current_size.lock()
    }
}

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

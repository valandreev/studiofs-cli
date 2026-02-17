//! DiskCache — persistent object cache with LRU eviction and disk space monitoring.
//!
//! Objects are stored as individual files in a cache directory, keyed by their 64-bit hash.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info, warn, error};

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

/// Entry in the LRU tracking.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Size of cached object in bytes.
    size: u64,
    /// Last access time (for LRU).
    last_access: Instant,
}

/// Key for a disk cache entry (Object ID / Hash).
type ObjectId = u64;

/// Persistent disk cache with size-based and free-space-based eviction.
pub struct DiskCache {
    cache_dir: PathBuf,
    /// Maximum cache size in bytes (default 100 GB).
    max_size: u64,
    /// Minimum free disk space ratio (0.0-1.0, default 0.15 = 15%).
    min_free_ratio: f64,
    /// Track cached entries for LRU eviction.
    entries: Mutex<BTreeMap<ObjectId, CacheEntry>>,
    /// Set of pinned objects (prevent eviction).
    pinned: Mutex<std::collections::HashSet<ObjectId>>,
    /// Current total cache size.
    current_size: Mutex<u64>,
}

impl DiskCache {
    /// Create a new disk cache.
    pub fn new(cache_dir: impl AsRef<Path>, max_size_bytes: u64) -> DiskCacheResult<Arc<Self>> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        let objects_dir = cache_dir.join("objects");
        fs::create_dir_all(&objects_dir)?;

        let cache = Self {
            cache_dir: objects_dir, // We primarily work in the objects subdir
            max_size: max_size_bytes,
            min_free_ratio: 0.15,
            entries: Mutex::new(BTreeMap::new()),
            pinned: Mutex::new(std::collections::HashSet::new()),
            current_size: Mutex::new(0),
        };

        // Scan existing cache on startup
        cache.scan_existing()?;

        info!(
            "disk cache initialized: dir={:?}, max={}GB",
            cache.cache_dir,
            cache.max_size / (1024 * 1024 * 1024)
        );
        
        // Wrap in Arc for background monitoring
        let cache = Arc::new(cache);
        
        // Spawn background GC monitor
        let monitor_cache = cache.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60)); // Check every minute
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
        let mut entries = self.entries.lock();
        let mut total_size = 0u64;

        if !self.cache_dir.exists() {
            return Ok(());
        }

        // Recursive scan
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
                            let size = meta.len();
                            entries.insert(
                                id,
                                CacheEntry {
                                    size,
                                    last_access: Instant::now(), // Assume recent? Or file mtime? Mtime better but costly.
                                },
                            );
                            total_size += size;
                        }
                    }
                }
            }
        }

        *self.current_size.lock() = total_size;
        debug!("scanned {} existing cache entries ({total_size} bytes)", entries.len());
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
            if !path.exists() {
                return Ok(None);
            }
            let mut file = fs::File::open(&path)?;
            let mut data = Vec::new(); // We don't know size easily without lookup
            file.read_to_end(&mut data)?;
            Ok(Some(Bytes::from(data)))
        }).await??;

        if data.is_some() {
            // Update last access time
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.get_mut(&id) {
                entry.last_access = Instant::now();
            } else {
                 // Inconsistency? Re-add
                 if let Some(d) = &data {
                     entries.insert(id, CacheEntry {
                         size: d.len() as u64,
                         last_access: Instant::now(),
                     });
                 }
            }
        }

        Ok(data)
    }

    /// Write an object to disk cache.
    pub async fn put_object(&self, id: ObjectId, data: &[u8]) -> DiskCacheResult<()> {
        let size = data.len() as u64;

        // Evict if needed
        self.reserve_space(size).await?;

        let path = self.object_path(id);
        let data_vec = data.to_vec(); // Clone data for the thread
        
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            // Ensure parent dirs exist
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            
            // Atomic write: write to temp then rename
            // BUT: temp file needs to be on same volume.
            // We use a `.tmp` suffix on the final path.
            let tmp_path = path.with_extension("tmp");
            
            {
                let mut file = fs::File::create(&tmp_path)?;
                file.write_all(&data_vec)?;
                // file.sync_all()?; // OMITTED FOR PERFORMANCE
            }
            
            fs::rename(&tmp_path, &path)?;
            Ok(())
        }).await??;

        let mut entries = self.entries.lock();
        if let Some(old) = entries.insert(
            id,
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

    /// Delete an object from cache.
    pub async fn remove_object(&self, id: ObjectId) -> DiskCacheResult<()> {
        let path = self.object_path(id);
        
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            if path.exists() {
                fs::remove_file(&path)?;
            }
            Ok(())
        }).await??;

        let mut entries = self.entries.lock();
        if let Some(entry) = entries.remove(&id) {
            *self.current_size.lock() -= entry.size;
        }

        Ok(())
    }

    /// Check if object exists.
    pub fn exists(&self, id: ObjectId) -> bool {
        self.entries.lock().contains_key(&id)
    }

    /// Reserve space for `needed` bytes by evicting LRU.
    /// Also checks free disk space.
    pub async fn reserve_space(&self, needed: u64) -> DiskCacheResult<()> {
        self.evict_lru(needed).await
    }

    /// Enforce size and disk space limits (Background GC).
    pub async fn enforce_limits(&self) -> DiskCacheResult<()> {
        self.evict_lru(0).await
    }

    /// Pin an object to prevent it from being evicted.
    pub fn pin(&self, id: ObjectId) {
        self.pinned.lock().insert(id);
    }

    /// Unpin an object, allowing it to be evicted.
    pub fn unpin(&self, id: ObjectId) {
        self.pinned.lock().remove(&id);
    }

    /// Evict LRU entries until we fit `incoming_size` AND satisfy disk space requirements.
    async fn evict_lru(&self, incoming_size: u64) -> DiskCacheResult<()> {
        loop {
            // 1. Check if we satisfy constraints
            let needs_eviction = {
                let current = *self.current_size.lock();
                if current + incoming_size > self.max_size {
                    true
                } else {
                    // Size is OK, check disk space (async)
                    false
                }
            };

            let needs_eviction = if needs_eviction {
                true
            } else {
                // Only check disk space if size is fine
                self.is_disk_space_low().await
            };

            if !needs_eviction {
                break;
            }

            // 2. Need eviction, find candidate
            let (to_evict, path_to_delete) = {
                let mut entries = self.entries.lock();
                let pinned = self.pinned.lock();

                let lru_id = entries
                    .iter()
                    .filter(|(k, _)| !pinned.contains(k)) // Skip pinned
                    .min_by_key(|(_, v)| v.last_access)
                    .map(|(k, _)| *k);

                match lru_id {
                    Some(id) => {
                         let entry = entries.remove(&id).unwrap(); // safe query
                         *self.current_size.lock() -= entry.size;
                         debug!(id = %format!("{:x}", id), "evicted from disk cache");
                         (Some(id), self.object_path(id))
                    }
                    None => (None, PathBuf::new())
                }
            }; 

            if let Some(_id) = to_evict {
                // Perform deletion outside lock
                let path = path_to_delete;
                let path_clone = path.clone();
                if let Err(e) = tokio::task::spawn_blocking(move || fs::remove_file(&path_clone)).await? {
                     warn!("failed to remove cache file {:?}: {}", path, e);
                }
            } else {
                // No more candidates
                break;
            }
        }

        Ok(())
    }

    /// Check if disk space is below the minimum free ratio.
    /// Made async-friendly (blocking call wrapped).
    async fn is_disk_space_low(&self) -> bool {
        let path = self.cache_dir.clone();
        let min_ratio = self.min_free_ratio;
        
        tokio::task::spawn_blocking(move || {
            match get_free_disk_space(&path) {
                Some((free, total)) => {
                    if total == 0 { return false; }
                    let ratio = free as f64 / total as f64;
                    if ratio < min_ratio {
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
                None => false,
            }
        }).await.unwrap_or(false)
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

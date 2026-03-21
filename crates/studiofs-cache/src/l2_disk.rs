//! L2 Disk Cache — file-per-block storage on NVMe.
//!
//! Each cached block is a single file in a sharded directory structure:
//!
//! ```text
//! {cache_dir}/blocks/{shard_hex}/{object_id:016x}.block
//! ```
//!
//! Files contain **decompressed** block data (no headers, no framing).
//! Blocks are written atomically via tmp-file + rename.
//!
//! This design is elastic: the cache starts at 0 bytes and grows on demand.
//! Eviction = `unlink` — no internal fragmentation, no free-list bookkeeping.
//!
//! ## Performance optimisations
//!
//! - **Pre-computed shard paths:** The 256 shard directory paths are built
//!   once at open() and stored in an array, eliminating `format!` + 3×`join()`
//!   on every hot-path call.
//! - **Single `spawn_blocking` per put:** write + rename are combined into one
//!   blocking call instead of two.
//! - **Batch put/get:** `put_batch` and `get_batch` amortise thread-pool
//!   scheduling overhead by processing multiple blocks per `spawn_blocking`.

use bytes::Bytes;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

use crate::platform_direct_io;

// =============================================================================
// L2DiskCache
// =============================================================================

/// Upper bound for active L2 blocking-I/O tasks.
const DEFAULT_L2_BLOCKING_IO_MAX_IN_FLIGHT: usize = 128;

/// File-per-block disk cache on NVMe.
pub struct L2DiskCache {
    /// Root cache directory (contains `blocks/` subdirectory).
    cache_dir: PathBuf,
    /// Pre-computed shard directory paths: `shard_dirs[byte] = "{cache_dir}/blocks/{byte:02x}"`.
    /// Avoids `format!` + `PathBuf::join` on every hot-path call.
    shard_dirs: Arc<[PathBuf; 256]>,
    /// Total cached bytes on disk (approximate — updated on put/remove).
    total_bytes: AtomicU64,
    /// Maximum allowed bytes.
    max_bytes: AtomicU64,
    /// Limits active blocking L2 I/O to avoid exhausting Tokio's blocking pool.
    io_semaphore: Arc<Semaphore>,
}

impl L2DiskCache {
    fn unique_tmp_path(path: &Path) -> PathBuf {
        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id() as u64;
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("block");
        let tmp_name = format!("{file_name}.tmp-{pid:08x}-{now_nanos:016x}-{seq:016x}");
        path.with_file_name(tmp_name)
    }

    fn write_block_file(path: &Path, tmp_path: &Path, data: &[u8]) -> io::Result<()> {
        match platform_direct_io::write_file(tmp_path, data) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                platform_direct_io::write_file(tmp_path, data)?;
            }
            Err(e) => return Err(e),
        }

        match std::fs::rename(tmp_path, path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                platform_direct_io::write_file(tmp_path, data)?;
                std::fs::rename(tmp_path, path)
            }
            Err(e) => Err(e),
        }
    }

    /// Open (or create) the disk cache at `cache_dir`.
    ///
    /// If the directory already exists, it is reused.  All 256 shard
    /// subdirectories are pre-created so that `put()` never needs to
    /// call `create_dir_all` on the hot path.
    pub fn open(cache_dir: impl AsRef<Path>, max_bytes: u64) -> io::Result<Self> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        let blocks_dir = cache_dir.join("blocks");
        std::fs::create_dir_all(&blocks_dir)?;

        // Pre-create all 256 shard directories (00..ff) and cache their paths
        // so hot-path calls never need `format!` + `PathBuf::join`.
        let shard_dirs: [PathBuf; 256] = std::array::from_fn(|i| {
            let dir = blocks_dir.join(format!("{:02x}", i));
            // Ignore errors for pre-existing dirs.
            let _ = std::fs::create_dir_all(&dir);
            dir
        });

        let this = Self {
            cache_dir,
            shard_dirs: Arc::new(shard_dirs),
            total_bytes: AtomicU64::new(0),
            max_bytes: AtomicU64::new(max_bytes),
            io_semaphore: Arc::new(Semaphore::new(DEFAULT_L2_BLOCKING_IO_MAX_IN_FLIGHT)),
        };

        Ok(this)
    }

    /// Scan existing block files and rebuild `total_bytes`.
    ///
    /// Call this once at startup.  Returns the list of `(object_id, size)` pairs
    /// found on disk, useful for reloading LRU state.
    pub fn scan(&self) -> io::Result<Vec<(u64, u32)>> {
        let blocks_dir = self.cache_dir.join("blocks");
        let mut found = Vec::new();
        let mut total: u64 = 0;

        if !blocks_dir.exists() {
            return Ok(found);
        }

        for shard_entry in std::fs::read_dir(&blocks_dir)? {
            let shard_entry = shard_entry?;
            if !shard_entry.file_type()?.is_dir() {
                continue;
            }
            for block_entry in std::fs::read_dir(shard_entry.path())? {
                let block_entry = block_entry?;
                let path = block_entry.path();
                let Some(ext) = path.extension() else {
                    continue;
                };
                if ext != "block" {
                    // Clean up stale tmp files from interrupted writes.
                    let is_stale_tmp = ext == "tmp"
                        || path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .map(|name| name.contains(".tmp-"))
                            .unwrap_or(false);
                    if is_stale_tmp {
                        let _ = std::fs::remove_file(&path);
                    }
                    continue;
                }
                let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let Ok(object_id) = u64::from_str_radix(stem, 16) else {
                    continue;
                };
                let size = block_entry.metadata()?.len() as u32;
                total += size as u64;
                found.push((object_id, size));
            }
        }

        self.total_bytes.store(total, Ordering::Relaxed);
        debug!(
            blocks = found.len(),
            total_mb = total / (1024 * 1024),
            "L2DiskCache scan complete"
        );
        Ok(found)
    }

    async fn read_block_buffered(path: PathBuf, semaphore: Arc<Semaphore>) -> io::Result<Bytes> {
        let permit = semaphore
            .acquire_owned()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "L2 I/O semaphore closed"))?;

        crate::runtime::spawn_blocking_profiled("l2_read_buffered", move || -> io::Result<Bytes> {
            let _permit = permit;
            std::fs::read(path).map(Bytes::from)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn acquire_io_permit(&self) -> io::Result<OwnedSemaphorePermit> {
        self.io_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "L2 I/O semaphore closed"))
    }

    async fn read_block_bytes(
        &self,
        path: PathBuf,
        expected_len_hint: Option<usize>,
    ) -> io::Result<Bytes> {
        {
            let permit = self.acquire_io_permit().await?;
            if let Some(bytes) =
                platform_direct_io::read_block_direct(path.clone(), expected_len_hint, permit)
                    .await?
            {
                return Ok(bytes);
            }
        }

        Self::read_block_buffered(path, self.io_semaphore.clone()).await
    }

    /// Read a block from disk. Returns `None` if not cached.
    ///
    /// Direct-I/O probing is delegated to `platform_direct_io`.
    /// If a platform/filesystem rejects direct mode for this block, read falls
    /// back to async buffered I/O.
    pub async fn get(&self, object_id: u64) -> Option<Bytes> {
        self.get_with_size_hint(object_id, None).await
    }

    /// Read a block using a known payload size hint from cache metadata.
    ///
    /// When `size_hint` is present, platform direct-I/O can skip per-read
    /// metadata/stat calls on the hot path.
    pub async fn get_with_size_hint(
        &self,
        object_id: u64,
        size_hint: Option<u32>,
    ) -> Option<Bytes> {
        let path = self.block_path(object_id);
        match self
            .read_block_bytes(path, size_hint.map(|v| v as usize))
            .await
        {
            Ok(data) => Some(data),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => {
                warn!(object_id, error = %e, "L2DiskCache read error");
                None
            }
        }
    }

    /// Read multiple blocks from disk in parallel.
    ///
    /// Returns a `Vec` of `(object_id, Option<Bytes>)` in the same order as
    /// the input.
    pub async fn get_batch(&self, ids: &[u64]) -> Vec<(u64, Option<Bytes>)> {
        let mut handles = Vec::with_capacity(ids.len());
        for &id in ids {
            let path = self.block_path(id);
            let semaphore = self.io_semaphore.clone();
            handles.push((
                id,
                tokio::spawn(async move {
                    let permit = semaphore.clone().acquire_owned().await.map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "L2 I/O semaphore closed")
                    })?;
                    if let Some(bytes) =
                        platform_direct_io::read_block_direct(path.clone(), None, permit).await?
                    {
                        Ok(bytes)
                    } else {
                        Self::read_block_buffered(path, semaphore.clone()).await
                    }
                }),
            ));
        }

        let mut results = Vec::with_capacity(handles.len());
        for (id, handle) in handles {
            let data = match handle.await {
                Ok(Ok(data)) => Some(data),
                Ok(Err(e)) if e.kind() == io::ErrorKind::NotFound => None,
                Ok(Err(e)) => {
                    warn!(id, error = %e, "L2DiskCache batch read error");
                    None
                }
                Err(e) => {
                    warn!(id, error = %e, "L2DiskCache batch join error");
                    None
                }
            };
            results.push((id, data));
        }
        results
    }

    /// Read a block synchronously (for non-async contexts like upload workers).
    pub fn get_sync(&self, object_id: u64) -> Option<Bytes> {
        let path = self.block_path(object_id);
        match std::fs::read(&path) {
            Ok(data) => Some(Bytes::from(data)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => {
                warn!(object_id, error = %e, "L2DiskCache sync read error");
                None
            }
        }
    }

    /// Write a block to disk (decompressed data) from an owned `Bytes` buffer.
    ///
    /// Atomic: writes to a `.tmp` file then renames — combined in a single
    /// `spawn_blocking` call to halve the thread-pool scheduling overhead.
    ///
    /// Shard directories are pre-created by `open()`, so no `create_dir_all`
    /// call on the hot path.
    pub async fn put_bytes(&self, object_id: u64, data: Bytes) -> io::Result<()> {
        let path = self.block_path(object_id);
        let tmp_path = Self::unique_tmp_path(&path);
        let len = data.len() as u64;
        let permit = self.acquire_io_permit().await?;

        crate::runtime::spawn_blocking_profiled("l2_put", move || -> io::Result<()> {
            let _permit = permit;
            Self::write_block_file(&path, &tmp_path, &data)
        })
        .await
        .map_err(io::Error::other)??;

        self.total_bytes.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    /// Write a block to disk from a byte slice.
    ///
    /// This compatibility helper copies into an owned `Bytes` payload; prefer
    /// [`put_bytes`] on hot paths that already own a `Bytes` buffer.
    pub async fn put(&self, object_id: u64, data: &[u8]) -> io::Result<()> {
        self.put_bytes(object_id, Bytes::copy_from_slice(data))
            .await
    }

    /// Write multiple blocks to disk in parallel.
    ///
    /// Each block is written in its own `spawn_blocking` task so NVMe can
    /// service them concurrently through multiple queues.  Returns errors
    /// for individual blocks as `(object_id, Error)`.
    pub async fn put_batch(&self, blocks: &[(u64, Bytes)]) -> Vec<(u64, io::Result<()>, u64)> {
        let mut handles = Vec::with_capacity(blocks.len());
        for (id, data) in blocks {
            let path = self.block_path(*id);
            let tmp_path = Self::unique_tmp_path(&path);
            let owned = data.clone();
            let object_id = *id;
            let semaphore = self.io_semaphore.clone();
            handles.push((
                object_id,
                data.len() as u64,
                tokio::spawn(async move {
                    let permit = semaphore.acquire_owned().await.map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "L2 I/O semaphore closed")
                    })?;
                    crate::runtime::spawn_blocking_profiled(
                        "l2_put_batch",
                        move || -> (io::Result<()>, u64) {
                            // Intentionally measure on-worker disk write latency only.
                            // Queueing delay before worker pickup is excluded.
                            let _permit = permit;
                            let started = std::time::Instant::now();
                            let result = Self::write_block_file(&path, &tmp_path, &owned);
                            let elapsed_ns =
                                started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                            (result, elapsed_ns)
                        },
                    )
                    .await
                    .map_err(io::Error::other)
                }),
            ));
        }
        let mut results = Vec::with_capacity(handles.len());
        for (id, len, handle) in handles {
            let (res, elapsed_ns) = match handle.await {
                Ok(Ok((Ok(()), elapsed_ns))) => {
                    self.total_bytes.fetch_add(len, Ordering::Relaxed);
                    (Ok(()), elapsed_ns)
                }
                Ok(Ok((Err(e), elapsed_ns))) => (Err(e), elapsed_ns),
                Ok(Err(e)) => (Err(e), 0),
                Err(e) => (Err(io::Error::other(e)), 0),
            };
            results.push((id, res, elapsed_ns));
        }
        results
    }

    /// Write a block synchronously.
    ///
    /// Shard directories are pre-created by `open()`.
    pub fn put_sync(&self, object_id: u64, data: &[u8]) -> io::Result<()> {
        let path = self.block_path(object_id);
        let tmp_path = Self::unique_tmp_path(&path);

        Self::write_block_file(&path, &tmp_path, data)?;

        self.total_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a block from disk.
    pub async fn remove(&self, object_id: u64) {
        let path = self.block_path(object_id);
        let permit = match self.acquire_io_permit().await {
            Ok(permit) => permit,
            Err(e) => {
                warn!(object_id, error = %e, "L2DiskCache remove permit error");
                return;
            }
        };
        // Single spawn_blocking for metadata + remove.
        let removed_size = crate::runtime::spawn_blocking_profiled("l2_remove", move || -> u64 {
            let _permit = permit;
            match std::fs::metadata(&path) {
                Ok(meta) => {
                    let size = meta.len();
                    match std::fs::remove_file(&path) {
                        Ok(()) => size,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
                        Err(e) => {
                            warn!(object_id, error = %e, "L2DiskCache remove error");
                            0
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
                Err(e) => {
                    warn!(object_id, error = %e, "L2DiskCache remove stat error");
                    0
                }
            }
        })
        .await
        .unwrap_or(0);
        if removed_size > 0 {
            self.total_bytes.fetch_sub(removed_size, Ordering::Relaxed);
        }
    }

    /// Remove a block synchronously.
    pub fn remove_sync(&self, object_id: u64) {
        let path = self.block_path(object_id);
        match std::fs::metadata(&path) {
            Ok(meta) => {
                let size = meta.len();
                if let Err(e) = std::fs::remove_file(&path) {
                    if e.kind() != io::ErrorKind::NotFound {
                        warn!(object_id, error = %e, "L2DiskCache sync remove error");
                    }
                } else {
                    self.total_bytes.fetch_sub(size, Ordering::Relaxed);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                warn!(object_id, error = %e, "L2DiskCache sync remove stat error");
            }
        }
    }

    /// Check whether a block file exists on disk.
    pub fn contains(&self, object_id: u64) -> bool {
        self.block_path(object_id).exists()
    }

    /// Current approximate total bytes on disk.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Maximum allowed bytes on disk.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes.load(Ordering::Relaxed)
    }

    /// Resize the disk budget.
    pub fn set_max_bytes(&self, max: u64) {
        self.max_bytes.store(max, Ordering::Relaxed);
    }

    /// Compute the path for a given block.
    ///
    /// Uses pre-computed shard directory paths — only one `format!` and one
    /// `PathBuf::join` instead of three.
    ///
    /// `{shard_dirs[high_byte]}/{object_id:016x}.block`
    #[inline]
    fn block_path(&self, object_id: u64) -> PathBuf {
        let shard_byte = (object_id >> 56) as u8; // top 8 bits = first 2 hex chars
        self.shard_dirs[shard_byte as usize].join(format!("{:016x}.block", object_id))
    }

    /// Root cache directory.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }
}

// =============================================================================
// Disk space helpers
// =============================================================================

/// Returns `(free_bytes, total_bytes)` for the filesystem containing `path`.
#[cfg(target_os = "windows")]
pub fn get_free_disk_space(path: &Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;

    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut free_caller: u64 = 0;
    let mut total: u64 = 0;
    let mut free_total: u64 = 0;
    unsafe {
        let ret = GetDiskFreeSpaceExW(wide.as_ptr(), &mut free_caller, &mut total, &mut free_total);
        if ret != 0 {
            Some((free_caller, total))
        } else {
            None
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub fn get_free_disk_space(path: &Path) -> Option<(u64, u64)> {
    use std::ffi::CString;
    let c_path = CString::new(path.to_str()?).ok()?;
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            let total = stat.f_blocks as u64 * stat.f_frsize as u64;
            let free = stat.f_bavail as u64 * stat.f_frsize as u64;
            Some((free, total))
        } else {
            None
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_get_remove() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2DiskCache::open(dir.path(), 1024 * 1024 * 1024).unwrap();

        let data = b"hello world block data";
        cache.put(42, data).await.unwrap();

        let got = cache.get(42).await.unwrap();
        assert_eq!(&got[..], data);
        assert!(cache.contains(42));
        assert_eq!(cache.total_bytes(), data.len() as u64);

        cache.remove(42).await;
        assert!(!cache.contains(42));
        assert_eq!(cache.total_bytes(), 0);
        assert!(cache.get(42).await.is_none());
    }

    #[tokio::test]
    async fn put_recreates_missing_shard_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2DiskCache::open(dir.path(), 1024 * 1024 * 1024).unwrap();
        let object_id = 0xab00000000000001;
        let shard_dir = dir.path().join("blocks").join("ab");

        std::fs::remove_dir_all(&shard_dir).unwrap();
        cache.put(object_id, b"recreate shard").await.unwrap();

        let got = cache.get(object_id).await.unwrap();
        assert_eq!(&got[..], b"recreate shard");
    }

    #[tokio::test]
    async fn scan_finds_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2DiskCache::open(dir.path(), 1024 * 1024 * 1024).unwrap();

        cache.put(1, b"block1").await.unwrap();
        cache.put(2, b"block22").await.unwrap();
        cache.put(0xff00, b"block_ff").await.unwrap();

        // Reset counter and rescan.
        cache.total_bytes.store(0, Ordering::Relaxed);
        let found = cache.scan().unwrap();
        assert_eq!(found.len(), 3);
        assert_eq!(
            cache.total_bytes(),
            b"block1".len() as u64 + b"block22".len() as u64 + b"block_ff".len() as u64
        );
    }

    #[test]
    fn block_path_sharding() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2DiskCache::open(dir.path(), 0).unwrap();
        // object_id = 0x00000000000000ff → shard "00" (first 2 hex chars)
        let path = cache.block_path(0xff);
        assert!(path.to_str().unwrap().contains("00"));
        assert!(path.to_str().unwrap().ends_with("00000000000000ff.block"));

        // object_id = 0xab00000000000000 → shard "ab"
        let path = cache.block_path(0xab00000000000000);
        assert!(path.to_str().unwrap().contains("ab"));
    }

    #[test]
    fn sync_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2DiskCache::open(dir.path(), 1024 * 1024 * 1024).unwrap();
        cache.put_sync(99, b"sync data").unwrap();
        let got = cache.get_sync(99).unwrap();
        assert_eq!(&got[..], b"sync data");
    }
}

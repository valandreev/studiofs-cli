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

use bytes::{Bytes, BytesMut};
use lru::LruCache;
use parking_lot::Mutex;
use std::fs::File;
use std::io;
use std::io::Read as _;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

use crate::platform_direct_io;

#[cfg(unix)]
use std::os::unix::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::FileExt as _;
#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt as _;

// =============================================================================
// L2DiskCache
// =============================================================================

/// Upper bound for active L2 blocking write/remove I/O tasks.
const DEFAULT_L2_FOREGROUND_BLOCKING_IO_MAX_IN_FLIGHT: usize = 128;
/// Upper bound for background upload I/O tasks.
const DEFAULT_L2_BACKGROUND_BLOCKING_IO_MAX_IN_FLIGHT: usize = 32;
/// Max number of blocking L2 read tasks scheduled concurrently by one batch read call.
const DEFAULT_L2_READ_BATCH_MAX_IN_FLIGHT: usize = 256;
/// Number of hot block files to keep as open descriptors for sync read fast-path.
const DEFAULT_L2_SYNC_FD_CACHE_CAPACITY: usize = 16_384;
const DEFAULT_BUFFERED_READ_CAPACITY: usize = 16 * 1024 * 1024;
const L2_SYNC_FD_CACHE_CAPACITY_ENV: &str = "STUDIOFS_L2_SYNC_FD_CACHE_CAPACITY";
const L2_SYNC_DIRECT_FD_CACHE_CAPACITY_ENV: &str = "STUDIOFS_L2_SYNC_DIRECT_FD_CACHE_CAPACITY";
const L2_READ_BATCH_MAX_IN_FLIGHT_ENV: &str = "STUDIOFS_L2_READ_BATCH_MAX_IN_FLIGHT";
#[cfg(windows)]
const WINDOWS_FILE_SHARE_ALL: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
#[cfg(windows)]
const WINDOWS_ERROR_INVALID_PARAMETER: i32 = 87;
type PutBatchWorkerOutput = (usize, u64, u64, io::Result<(io::Result<()>, u64)>);
type RemoveBatchWorkerOutput = (usize, u64, io::Result<u64>);

/// Priority class for L2 I/O permits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2IoPriority {
    Foreground,
    Background,
}

fn aligned_bytes_mut_uninit(len: usize, alignment: usize) -> io::Result<BytesMut> {
    if alignment == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "alignment must be non-zero",
        ));
    }
    if !alignment.is_power_of_two() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "alignment must be a power of two",
        ));
    }
    if len == 0 {
        return Ok(BytesMut::new());
    }

    let total = len
        .checked_add(alignment)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "buffer size overflow"))?;
    let mut raw = BytesMut::with_capacity(total);
    let base = raw.as_ptr() as usize;
    let misalignment = base % alignment;
    let prefix = if misalignment == 0 {
        0
    } else {
        alignment - misalignment
    };
    let target_len = prefix
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "buffer size overflow"))?;
    if raw.capacity() < target_len {
        return Err(io::Error::other(
            "allocator returned insufficient capacity for aligned buffer",
        ));
    }

    // SAFETY: `raw` has capacity >= `target_len` by construction. This
    // helper is only used for direct I/O read targets that are fully written
    // by OS read calls before any initialized bytes are observed.
    unsafe {
        raw.set_len(target_len);
    }
    if prefix != 0 {
        let _ = raw.split_to(prefix);
    }
    debug_assert_eq!(raw.len(), len);
    debug_assert!((raw.as_ptr() as usize).is_multiple_of(alignment));
    Ok(raw)
}

fn try_read_direct_block(
    path: &Path,
    expected_len_hint: Option<usize>,
    priority: L2IoPriority,
) -> io::Result<Option<Bytes>> {
    let Some(std_file) = platform_direct_io::open_block_direct_read(path)? else {
        return Ok(None);
    };

    let expected_len = match expected_len_hint {
        Some(len) => len,
        None => std_file.metadata()?.len() as usize,
    };
    let mut direct_buf =
        aligned_bytes_mut_uninit(expected_len, platform_direct_io::DIRECT_IO_ALIGNMENT)?;
    if let Some(read_len) =
        platform_direct_io::read_block_direct_from_file(std_file, &mut direct_buf, priority)?
    {
        if read_len > expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "direct read returned more bytes than requested",
            ));
        }
        if expected_len_hint.is_some() && read_len != expected_len {
            debug!(
                expected_len,
                read_len, "direct read size mismatch for hinted block; falling back"
            );
            return Ok(None);
        }
        direct_buf.truncate(read_len);
        Ok(Some(direct_buf.freeze()))
    } else {
        Ok(None)
    }
}

fn read_file_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(windows)]
    {
        return file.seek_read(buf, offset);
    }
    #[cfg(unix)]
    {
        return file.read_at(buf, offset);
    }
    #[allow(unreachable_code)]
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "pread-style reads are unsupported on this platform",
    ))
}

fn read_into_slice_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    if buf.is_empty() {
        return Ok(0);
    }
    // One positional read attempt is enough for the sync cache-hit path.
    // Short reads are propagated to caller and treated as tail zero-fill there.
    read_file_at(file, buf, offset)
}

fn sync_fd_cache_capacity() -> usize {
    std::env::var(L2_SYNC_FD_CACHE_CAPACITY_ENV)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_L2_SYNC_FD_CACHE_CAPACITY)
}

fn direct_sync_fd_cache_capacity(total_capacity: usize) -> usize {
    std::env::var(L2_SYNC_DIRECT_FD_CACHE_CAPACITY_ENV)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(total_capacity / 2)
        .clamp(1, total_capacity.saturating_sub(1))
}

fn read_batch_max_in_flight() -> usize {
    std::env::var(L2_READ_BATCH_MAX_IN_FLIGHT_ENV)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_L2_READ_BATCH_MAX_IN_FLIGHT)
}

#[cfg(unix)]
fn current_nofile_soft_limit() -> Option<u64> {
    // SAFETY: `getrlimit` initializes the provided struct on success.
    let mut lim = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, lim.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `rc == 0` means `lim` was initialized by `getrlimit`.
    let lim = unsafe { lim.assume_init() };
    Some(lim.rlim_cur)
}

#[cfg(not(unix))]
fn current_nofile_soft_limit() -> Option<u64> {
    None
}

fn open_sync_read_file(path: &Path) -> io::Result<File> {
    #[cfg(windows)]
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true).share_mode(WINDOWS_FILE_SHARE_ALL);
        let file = opts.open(path)?;
        apply_l2_read_open_hints(&file);
        Ok(file)
    }
    #[cfg(not(windows))]
    {
        let file = File::open(path)?;
        apply_l2_read_open_hints(&file);
        Ok(file)
    }
}

fn apply_l2_read_open_hints(file: &File) {
    #[cfg(target_os = "macos")]
    {
        let _ = file;
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = file;
    }
}

fn apply_l2_read_close_hints(file: &File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;

        let fd = file.as_raw_fd();
        let _ = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
    }
}

fn read_block_buffered(
    path: &Path,
    expected_len_hint: Option<usize>,
    priority: L2IoPriority,
) -> io::Result<Bytes> {
    let mut file = open_sync_read_file(path)?;

    let capacity = match expected_len_hint {
        Some(len) => len,
        None => match usize::try_from(file.metadata()?.len()) {
            Ok(len) => len,
            Err(_) => DEFAULT_BUFFERED_READ_CAPACITY,
        },
    };
    let mut out = Vec::with_capacity(capacity);

    if priority == L2IoPriority::Foreground {
        file.read_to_end(&mut out)?;
    } else {
        // Keep 1MB chunking for background work so foreground reads can interleave.
        let chunk_size = platform_direct_io::IO_CHUNK_SIZE_BACKGROUND as u64;
        loop {
            let n = (&mut file).take(chunk_size).read_to_end(&mut out)?;
            if n == 0 {
                break;
            }
        }
    }

    apply_l2_read_close_hints(&file);
    Ok(Bytes::from(out))
}

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
    /// Limits active foreground L2 I/O to avoid exhausting Tokio's blocking pool.
    foreground_io_semaphore: Arc<Semaphore>,
    /// Limits active background L2 I/O for upload/flush work.
    background_io_semaphore: Arc<Semaphore>,
    /// Max number of blocking L2 reads spawned concurrently by one batch call.
    read_batch_max_in_flight: usize,
    /// Small LRU of open file descriptors used by synchronous read fast-path.
    sync_read_fd_cache: Mutex<LruCache<u64, Arc<File>>>,
    /// Small LRU of open direct-I/O descriptors used by unbuffered sync reads.
    sync_direct_read_fd_cache: Mutex<LruCache<u64, Arc<File>>>,
}

impl Drop for L2DiskCache {
    fn drop(&mut self) {
        // Clear FD caches to ensure all file descriptors are closed before shutdown.
        // This prevents FD leaks in long-running processes and ensures clean shutdown.
        let mut cache = self.sync_read_fd_cache.lock();
        cache.clear();
        drop(cache);

        let mut direct_cache = self.sync_direct_read_fd_cache.lock();
        direct_cache.clear();
    }
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

        // Keep total open-FD budget bounded even with separate buffered/direct
        // sync caches by splitting one configured capacity across both.
        let total_fd_cache_capacity = sync_fd_cache_capacity().max(2);
        if let Some(nofile_soft_limit) = current_nofile_soft_limit()
            && total_fd_cache_capacity as u64 >= nofile_soft_limit
        {
            warn!(
                fd_cache_capacity = total_fd_cache_capacity,
                nofile_soft_limit,
                env = L2_SYNC_FD_CACHE_CAPACITY_ENV,
                "L2 sync FD cache capacity is near/above process nofile soft limit; consider lowering cache capacity or increasing ulimit -n"
            );
        }
        let direct_fd_cache_capacity = direct_sync_fd_cache_capacity(total_fd_cache_capacity);
        let buffered_fd_cache_capacity =
            total_fd_cache_capacity.saturating_sub(direct_fd_cache_capacity);

        let this = Self {
            cache_dir,
            shard_dirs: Arc::new(shard_dirs),
            total_bytes: AtomicU64::new(0),
            max_bytes: AtomicU64::new(max_bytes),
            foreground_io_semaphore: Arc::new(Semaphore::new(
                DEFAULT_L2_FOREGROUND_BLOCKING_IO_MAX_IN_FLIGHT,
            )),
            background_io_semaphore: Arc::new(Semaphore::new(
                DEFAULT_L2_BACKGROUND_BLOCKING_IO_MAX_IN_FLIGHT,
            )),
            read_batch_max_in_flight: read_batch_max_in_flight().max(1),
            sync_read_fd_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(buffered_fd_cache_capacity)
                    .expect("sync fd cache capacity must be non-zero"),
            )),
            sync_direct_read_fd_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(direct_fd_cache_capacity)
                    .expect("sync direct fd cache capacity must be non-zero"),
            )),
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

    fn read_block_sync(
        path: &Path,
        expected_len_hint: Option<usize>,
        priority: L2IoPriority,
    ) -> io::Result<Bytes> {
        if let Some(bytes) = try_read_direct_block(path, expected_len_hint, priority)? {
            return Ok(bytes);
        }
        read_block_buffered(path, expected_len_hint, priority)
    }

    async fn acquire_io_permit(&self, priority: L2IoPriority) -> io::Result<OwnedSemaphorePermit> {
        Arc::clone(self.io_semaphore(priority))
            .acquire_owned()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "L2 I/O semaphore closed"))
    }

    #[inline]
    fn io_semaphore(&self, priority: L2IoPriority) -> &Arc<Semaphore> {
        match priority {
            L2IoPriority::Foreground => &self.foreground_io_semaphore,
            L2IoPriority::Background => &self.background_io_semaphore,
        }
    }

    async fn read_block_bytes(
        &self,
        path: PathBuf,
        expected_len_hint: Option<usize>,
        priority: L2IoPriority,
    ) -> io::Result<Bytes> {
        let permit = self.acquire_io_permit(priority).await?;
        crate::runtime::spawn_blocking_profiled("l2_read", move || {
            let _permit = permit;
            Self::read_block_sync(&path, expected_len_hint, priority)
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Read a block from disk. Returns `None` if not cached.
    ///
    /// Direct-I/O probing is delegated to `platform_direct_io`.
    /// If a platform/filesystem rejects direct mode for this block, read falls
    /// back to async buffered I/O.
    pub async fn get(&self, object_id: u64) -> Option<Bytes> {
        self.get_with_priority(object_id, L2IoPriority::Foreground)
            .await
    }

    /// Read a block from disk with an explicit QoS priority.
    pub async fn get_with_priority(&self, object_id: u64, priority: L2IoPriority) -> Option<Bytes> {
        self.get_with_size_hint(object_id, None, priority).await
    }

    /// Read a block using a known payload size hint from cache metadata.
    ///
    /// When `size_hint` is present, platform direct-I/O can skip per-read
    /// metadata/stat calls on the hot path.
    pub async fn get_with_size_hint(
        &self,
        object_id: u64,
        size_hint: Option<u32>,
        priority: L2IoPriority,
    ) -> Option<Bytes> {
        let path = self.block_path(object_id);
        match self
            .read_block_bytes(path, size_hint.map(|v| v as usize), priority)
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
        self.get_batch_with_priority(ids, L2IoPriority::Foreground)
            .await
    }

    /// Read multiple blocks from disk in parallel with a QoS priority.
    pub async fn get_batch_with_priority(
        &self,
        ids: &[u64],
        priority: L2IoPriority,
    ) -> Vec<(u64, Option<Bytes>)> {
        let sized_ids: Vec<(u64, Option<u32>)> = ids.iter().copied().map(|id| (id, None)).collect();
        self.get_batch_with_size_hints(&sized_ids, priority).await
    }

    /// Read multiple blocks from disk in parallel with optional size hints.
    ///
    /// If a size hint is provided for an item, path metadata reads can be skipped.
    pub async fn get_batch_with_size_hints(
        &self,
        ids: &[(u64, Option<u32>)],
        priority: L2IoPriority,
    ) -> Vec<(u64, Option<Bytes>)> {
        let mut results: Vec<(u64, Option<Bytes>)> =
            ids.iter().map(|(id, _)| (*id, None)).collect();
        let max_in_flight = self.read_batch_max_in_flight.max(1);
        let mut join_set = tokio::task::JoinSet::new();
        let mut next_idx = 0usize;
        let io_semaphore = self.io_semaphore(priority);

        let spawn_one =
            |idx: usize, join_set: &mut tokio::task::JoinSet<(usize, io::Result<Bytes>)>| {
                let (id, size_hint) = ids[idx];
                let path = self.block_path(id);
                let semaphore = Arc::clone(io_semaphore);
                join_set.spawn(async move {
                    let permit = match semaphore.acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            return (
                                idx,
                                Err(io::Error::new(
                                    io::ErrorKind::BrokenPipe,
                                    "L2 I/O semaphore closed",
                                )),
                            );
                        }
                    };
                    let read_result =
                        match crate::runtime::spawn_blocking_profiled("l2_read_batch", move || {
                            let _permit = permit;
                            Self::read_block_sync(
                                &path,
                                size_hint.map(|len| len as usize),
                                priority,
                            )
                        })
                        .await
                        {
                            Ok(result) => result,
                            Err(e) => Err(io::Error::other(e)),
                        };
                    (idx, read_result)
                });
            };

        while next_idx < ids.len() && join_set.len() < max_in_flight {
            spawn_one(next_idx, &mut join_set);
            next_idx += 1;
        }

        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok((idx, Ok(data))) => {
                    results[idx].1 = Some(data);
                }
                Ok((idx, Err(e))) if e.kind() == io::ErrorKind::NotFound => {
                    results[idx].1 = None;
                }
                Ok((idx, Err(e))) => {
                    let id = results[idx].0;
                    warn!(id, error = %e, "L2DiskCache batch read error");
                }
                Err(e) => {
                    warn!(error = %e, "L2DiskCache batch join error");
                }
            }

            if next_idx < ids.len() {
                spawn_one(next_idx, &mut join_set);
                next_idx += 1;
            }
        }

        results
    }

    /// Read a block synchronously (for non-async contexts like upload workers).
    pub fn get_sync(&self, object_id: u64) -> Option<Bytes> {
        let path = self.block_path(object_id);
        match Self::read_block_sync(&path, None, L2IoPriority::Foreground) {
            Ok(data) => Some(data),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => {
                warn!(object_id, error = %e, "L2DiskCache sync read error");
                None
            }
        }
    }

    /// Read a block synchronously into a provided output slice at an offset.
    ///
    /// Returns `Ok(None)` when the block file is not present.
    pub fn read_into_sync(
        &self,
        object_id: u64,
        offset: u64,
        out: &mut [u8],
    ) -> io::Result<Option<usize>> {
        let file = match self.fd_cache_get(object_id) {
            Some(file) => file,
            None => {
                let path = self.block_path(object_id);
                let opened = match open_sync_read_file(&path) {
                    Ok(file) => Arc::new(file),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(e),
                };
                self.fd_cache_insert(object_id, Arc::clone(&opened));
                opened
            }
        };
        let read = read_into_slice_at(file.as_ref(), offset, out)?;
        Ok(Some(read))
    }

    /// Read a block synchronously into `out` using platform direct-I/O.
    ///
    /// Returns `Ok(None)` when direct-I/O is unavailable for this block/path
    /// (including alignment constraints and platform/filesystem limitations).
    pub fn read_into_sync_direct(
        &self,
        object_id: u64,
        offset: u64,
        out: &mut [u8],
    ) -> io::Result<Option<usize>> {
        #[cfg(windows)]
        if !offset.is_multiple_of(platform_direct_io::DIRECT_IO_ALIGNMENT as u64) {
            return Ok(None);
        }
        let file = match self.direct_fd_cache_get(object_id) {
            Some(file) => file,
            None => {
                let path = self.block_path(object_id);
                let opened = match platform_direct_io::open_block_direct_read(&path)? {
                    Some(file) => Arc::new(file),
                    None => return Ok(None),
                };
                self.direct_fd_cache_insert(object_id, Arc::clone(&opened));
                opened
            }
        };
        match read_into_slice_at(file.as_ref(), offset, out) {
            Ok(read) => Ok(Some(read)),
            Err(e) if e.kind() == io::ErrorKind::InvalidInput => Ok(None),
            #[cfg(windows)]
            Err(e) if matches!(e.raw_os_error(), Some(WINDOWS_ERROR_INVALID_PARAMETER)) => Ok(None),
            Err(e) => Err(e),
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
        self.put_bytes_with_priority(object_id, data, L2IoPriority::Foreground)
            .await
    }

    /// Write a block to disk with an explicit QoS priority.
    pub async fn put_bytes_with_priority(
        &self,
        object_id: u64,
        data: Bytes,
        priority: L2IoPriority,
    ) -> io::Result<()> {
        let path = self.block_path(object_id);
        let tmp_path = Self::unique_tmp_path(&path);
        let len = data.len() as u64;
        let permit = self.acquire_io_permit(priority).await?;

        crate::runtime::spawn_blocking_profiled("l2_put", move || -> io::Result<()> {
            let _permit = permit;
            Self::write_block_file(&path, &tmp_path, &data)
        })
        .await
        .map_err(io::Error::other)??;

        // Invalidate after successful write+rename so future sync reads reopen
        // the new inode instead of a cached old descriptor.
        self.fd_cache_invalidate(object_id);
        self.total_bytes.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    /// Write a block to disk from a byte slice.
    ///
    /// This compatibility helper copies into an owned `Bytes` payload; prefer
    /// [`put_bytes`] on hot paths that already own a `Bytes` buffer.
    pub async fn put(&self, object_id: u64, data: &[u8]) -> io::Result<()> {
        self.put_bytes_with_priority(
            object_id,
            Bytes::copy_from_slice(data),
            L2IoPriority::Foreground,
        )
        .await
    }

    /// Write a block to disk from a byte slice with explicit QoS priority.
    pub async fn put_with_priority(
        &self,
        object_id: u64,
        data: &[u8],
        priority: L2IoPriority,
    ) -> io::Result<()> {
        self.put_bytes_with_priority(object_id, Bytes::copy_from_slice(data), priority)
            .await
    }

    /// Write multiple blocks to disk in parallel.
    ///
    /// Each block is written in its own `spawn_blocking` task so NVMe can
    /// service them concurrently through multiple queues.  Returns errors
    /// for individual blocks as `(object_id, Error)`.
    pub async fn put_batch(&self, blocks: &[(u64, Bytes)]) -> Vec<(u64, io::Result<()>, u64)> {
        self.put_batch_with_priority(blocks, L2IoPriority::Foreground)
            .await
    }

    /// Write multiple blocks to disk in parallel with a QoS priority.
    pub async fn put_batch_with_priority(
        &self,
        blocks: &[(u64, Bytes)],
        priority: L2IoPriority,
    ) -> Vec<(u64, io::Result<()>, u64)> {
        let max_in_flight = match priority {
            L2IoPriority::Foreground => DEFAULT_L2_FOREGROUND_BLOCKING_IO_MAX_IN_FLIGHT,
            L2IoPriority::Background => DEFAULT_L2_BACKGROUND_BLOCKING_IO_MAX_IN_FLIGHT,
        }
        .max(1);
        let mut join_set = tokio::task::JoinSet::new();
        let mut next_idx = 0usize;
        let mut results: Vec<(u64, io::Result<()>, u64)> = blocks
            .iter()
            .map(|(id, _)| {
                (
                    *id,
                    Err(io::Error::other("L2 put batch task did not complete")),
                    0,
                )
            })
            .collect();
        let io_semaphore = self.io_semaphore(priority);

        let spawn_one = |idx: usize, join_set: &mut tokio::task::JoinSet<PutBatchWorkerOutput>| {
            let (id, data) = &blocks[idx];
            let path = self.block_path(*id);
            let tmp_path = Self::unique_tmp_path(&path);
            let owned = data.clone();
            let object_id = *id;
            let len = data.len() as u64;
            let semaphore = Arc::clone(io_semaphore);
            join_set.spawn(async move {
                let permit = match semaphore.acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        return (
                            idx,
                            object_id,
                            len,
                            Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "L2 I/O semaphore closed",
                            )),
                        );
                    }
                };

                let job = crate::runtime::spawn_blocking_profiled(
                    "l2_put_batch",
                    move || -> (io::Result<()>, u64) {
                        // Intentionally measure on-worker disk write latency only.
                        // Queueing delay before worker pickup is excluded.
                        let _permit = permit;
                        let started = std::time::Instant::now();
                        let result = Self::write_block_file(&path, &tmp_path, &owned);
                        let elapsed_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                        (result, elapsed_ns)
                    },
                )
                .await
                .map_err(io::Error::other);

                (idx, object_id, len, job)
            });
        };

        while next_idx < blocks.len() && join_set.len() < max_in_flight {
            spawn_one(next_idx, &mut join_set);
            next_idx += 1;
        }

        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok((idx, id, len, Ok((Ok(()), elapsed_ns)))) => {
                    self.fd_cache_invalidate(id);
                    self.total_bytes.fetch_add(len, Ordering::Relaxed);
                    results[idx] = (id, Ok(()), elapsed_ns);
                }
                Ok((idx, id, _len, Ok((Err(e), elapsed_ns)))) => {
                    results[idx] = (id, Err(e), elapsed_ns);
                }
                Ok((idx, id, _len, Err(e))) => {
                    results[idx] = (id, Err(e), 0);
                }
                Err(e) => {
                    warn!(error = %e, "L2DiskCache put_batch join error");
                }
            }

            if next_idx < blocks.len() {
                spawn_one(next_idx, &mut join_set);
                next_idx += 1;
            }
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

        // Invalidate after successful write+rename so future sync reads reopen
        // the new inode instead of a cached old descriptor.
        self.fd_cache_invalidate(object_id);
        self.total_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a block from disk.
    pub async fn remove(&self, object_id: u64) {
        self.remove_with_priority(object_id, L2IoPriority::Foreground)
            .await;
    }

    /// Remove a block from disk with an explicit QoS priority.
    pub async fn remove_with_priority(&self, object_id: u64, priority: L2IoPriority) {
        self.fd_cache_invalidate(object_id);
        let path = self.block_path(object_id);
        let permit = match self.acquire_io_permit(priority).await {
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

    /// Remove multiple blocks from disk in parallel.
    pub async fn remove_batch(&self, object_ids: &[u64]) -> Vec<(u64, io::Result<()>)> {
        self.remove_batch_with_priority(object_ids, L2IoPriority::Foreground)
            .await
    }

    /// Remove multiple blocks from disk in parallel with a QoS priority.
    pub async fn remove_batch_with_priority(
        &self,
        object_ids: &[u64],
        priority: L2IoPriority,
    ) -> Vec<(u64, io::Result<()>)> {
        let max_in_flight = match priority {
            L2IoPriority::Foreground => DEFAULT_L2_FOREGROUND_BLOCKING_IO_MAX_IN_FLIGHT,
            L2IoPriority::Background => DEFAULT_L2_BACKGROUND_BLOCKING_IO_MAX_IN_FLIGHT,
        }
        .max(1);
        let mut join_set = tokio::task::JoinSet::new();
        let mut next_idx = 0usize;
        let mut results: Vec<(u64, io::Result<()>)> = object_ids
            .iter()
            .copied()
            .map(|id| {
                (
                    id,
                    Err(io::Error::other("L2 remove batch task did not complete")),
                )
            })
            .collect();
        let io_semaphore = self.io_semaphore(priority);

        let spawn_one =
            |idx: usize, join_set: &mut tokio::task::JoinSet<RemoveBatchWorkerOutput>| {
                let object_id = object_ids[idx];
                self.fd_cache_invalidate(object_id);
                let path = self.block_path(object_id);
                let semaphore = Arc::clone(io_semaphore);
                join_set.spawn(async move {
                    let permit = match semaphore.acquire_owned().await {
                        Ok(permit) => permit,
                        Err(_) => {
                            return (
                                idx,
                                object_id,
                                Err(io::Error::new(
                                    io::ErrorKind::BrokenPipe,
                                    "L2 I/O semaphore closed",
                                )),
                            );
                        }
                    };

                    let job =
                        crate::runtime::spawn_blocking_profiled("l2_remove_batch", move || {
                            let _permit = permit;
                            match std::fs::metadata(&path) {
                                Ok(meta) => {
                                    let size = meta.len();
                                    match std::fs::remove_file(&path) {
                                        Ok(()) => Ok(size),
                                        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
                                        Err(e) => Err(e),
                                    }
                                }
                                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
                                Err(e) => Err(e),
                            }
                        })
                        .await
                        .map_err(io::Error::other)
                        .and_then(|result| result);

                    (idx, object_id, job)
                });
            };

        while next_idx < object_ids.len() && join_set.len() < max_in_flight {
            spawn_one(next_idx, &mut join_set);
            next_idx += 1;
        }

        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok((idx, id, Ok(removed_size))) => {
                    if removed_size > 0 {
                        self.total_bytes.fetch_sub(removed_size, Ordering::Relaxed);
                    }
                    results[idx] = (id, Ok(()));
                }
                Ok((idx, id, Err(e))) if e.kind() == io::ErrorKind::NotFound => {
                    results[idx] = (id, Ok(()));
                }
                Ok((idx, id, Err(e))) => {
                    results[idx] = (id, Err(e));
                }
                Err(e) => {
                    warn!(error = %e, "L2DiskCache remove_batch join error");
                }
            }

            if next_idx < object_ids.len() {
                spawn_one(next_idx, &mut join_set);
                next_idx += 1;
            }
        }

        results
    }

    /// Remove a block synchronously.
    pub fn remove_sync(&self, object_id: u64) {
        self.fd_cache_invalidate(object_id);
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

    fn fd_cache_get(&self, object_id: u64) -> Option<Arc<File>> {
        self.sync_read_fd_cache.lock().get(&object_id).cloned()
    }

    fn fd_cache_insert(&self, object_id: u64, file: Arc<File>) {
        self.sync_read_fd_cache.lock().put(object_id, file);
    }

    fn fd_cache_invalidate(&self, object_id: u64) {
        let _ = self.sync_read_fd_cache.lock().pop(&object_id);
        let _ = self.sync_direct_read_fd_cache.lock().pop(&object_id);
    }

    fn direct_fd_cache_get(&self, object_id: u64) -> Option<Arc<File>> {
        self.sync_direct_read_fd_cache
            .lock()
            .get(&object_id)
            .cloned()
    }

    fn direct_fd_cache_insert(&self, object_id: u64, file: Arc<File>) {
        self.sync_direct_read_fd_cache.lock().put(object_id, file);
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
    async fn foreground_and_background_permits_are_independent() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = L2DiskCache::open(dir.path(), 1024 * 1024 * 1024).unwrap();
        cache.foreground_io_semaphore = Arc::new(Semaphore::new(1));
        cache.background_io_semaphore = Arc::new(Semaphore::new(1));

        let _foreground = cache
            .acquire_io_permit(L2IoPriority::Foreground)
            .await
            .unwrap();
        let background = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            cache.acquire_io_permit(L2IoPriority::Background),
        )
        .await;
        assert!(
            background.is_ok(),
            "background permit should not wait on foreground"
        );
    }

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

    #[test]
    fn sync_fd_budget_is_split_across_buffered_and_direct_caches() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2DiskCache::open(dir.path(), 1024 * 1024 * 1024).unwrap();

        let buffered = cache.sync_read_fd_cache.lock().cap().get();
        let direct = cache.sync_direct_read_fd_cache.lock().cap().get();
        let total = sync_fd_cache_capacity().max(2);

        assert_eq!(buffered + direct, total);
        assert!(buffered > 0);
        assert!(direct > 0);
    }

    #[test]
    fn fd_cache_invalidate_clears_both_buffered_and_direct_entries() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2DiskCache::open(dir.path(), 1024 * 1024 * 1024).unwrap();
        let object_id = 0x1200_0000_0000_0001;
        let path = cache.block_path(object_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, b"fd-cache-test").unwrap();

        let file = Arc::new(File::open(&path).unwrap());
        cache.fd_cache_insert(object_id, Arc::clone(&file));
        cache.direct_fd_cache_insert(object_id, file);

        assert!(cache.fd_cache_get(object_id).is_some());
        assert!(cache.direct_fd_cache_get(object_id).is_some());

        cache.fd_cache_invalidate(object_id);

        assert!(cache.fd_cache_get(object_id).is_none());
        assert!(cache.direct_fd_cache_get(object_id).is_none());
    }

    #[cfg(windows)]
    #[test]
    fn read_into_sync_direct_rejects_unaligned_offset() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2DiskCache::open(dir.path(), 1024 * 1024 * 1024).unwrap();
        let mut out = [0u8; 4096];
        let result = cache.read_into_sync_direct(1, 1, &mut out).unwrap();
        assert!(result.is_none());
    }
}

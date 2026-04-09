//! Cache 3.0 file-per-block payload storage.

use std::array;
use std::fs::{File, OpenOptions};
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use tracing::warn;

use super::layout::CacheV3Layout;
use super::platform;

const TEMP_FILE_CREATE_ATTEMPTS: usize = 32;
const TEMP_FILE_SUFFIX: &str = ".tmp";
#[cfg(not(test))]
const READ_FD_CACHE_CAPACITY: usize = 2_048;
#[cfg(test)]
const READ_FD_CACHE_CAPACITY: usize = 16;
const READ_FD_CACHE_SHARDS: usize = 16;
const READ_HINT_MIN_LEN: usize = 512 * 1024;

static TEMP_FILE_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum FilePayloadStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("failed to create a unique temp payload file for object_id={object_id} in '{dir}'")]
    TempFileCreateExhausted { object_id: u64, dir: PathBuf },
}

/// File-per-block payload store rooted under Cache v3 layout.
#[derive(Debug, Clone)]
pub struct FilePayloadStore {
    layout: CacheV3Layout,
    read_handles: Arc<ReadHandleCache>,
}

impl FilePayloadStore {
    pub fn open(layout: CacheV3Layout) -> Result<Self, FilePayloadStoreError> {
        std::fs::create_dir_all(&layout.payload_blocks_dir)?;
        std::fs::create_dir_all(&layout.tmp_dir)?;
        Ok(Self {
            layout,
            read_handles: Arc::new(ReadHandleCache {
                shards: array::from_fn(|_| {
                    Mutex::new(LruCache::new(read_fd_cache_shard_capacity()))
                }),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
            }),
        })
    }

    pub fn get(&self, object_id: u64) -> Option<Bytes> {
        let path = self.layout.payload_block_path(object_id);
        let file = self.cached_read_handle(object_id, &path).ok()?;
        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                warn!(
                    object_id,
                    path = %path.display(),
                    error = ?error,
                    "cache-v3 payload get metadata failed"
                );
                return None;
            }
        };
        let len = match usize::try_from(metadata.len()) {
            Ok(len) => len,
            Err(_) => {
                warn!(
                    object_id,
                    path = %path.display(),
                    payload_len = metadata.len(),
                    "cache-v3 payload get length exceeds addressable usize"
                );
                return None;
            }
        };
        let mut payload = vec![0_u8; len];
        if len > 0 {
            if let Err(error) = platform::pread_exact(file.as_ref(), &mut payload, 0) {
                warn!(
                    object_id,
                    path = %path.display(),
                    error = ?error,
                    "cache-v3 payload get failed"
                );
                return None;
            }
        }
        Some(Bytes::from(payload))
    }

    pub fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        if out.is_empty() {
            return Some(0);
        }
        let start = u64::try_from(data_offset).ok()?;

        let path = self.layout.payload_block_path(object_id);
        let file = self.cached_read_handle(object_id, &path).ok()?;
        if out.len() >= READ_HINT_MIN_LEN {
            let hint_len = u64::try_from(out.len()).unwrap_or(u64::MAX);
            if let Err(error) = platform::advise_will_need(file.as_ref(), start, hint_len) {
                warn!(
                    object_id,
                    data_offset,
                    path = %path.display(),
                    error = ?error,
                    "cache-v3 payload read_into advise_will_need failed"
                );
            }
        }

        match platform::pread(file.as_ref(), out, start) {
            Ok(read) => Some(read),
            Err(error) => {
                warn!(
                    object_id,
                    data_offset,
                    path = %path.display(),
                    error = ?error,
                    "cache-v3 payload read_into failed"
                );
                None
            }
        }
    }

    pub fn contains(&self, object_id: u64) -> bool {
        match self.payload_exists(object_id) {
            Ok(exists) => exists,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "cache-v3 payload contains check failed"
                );
                false
            }
        }
    }

    pub fn payload_exists(&self, object_id: u64) -> io::Result<bool> {
        self.layout.payload_block_path(object_id).try_exists()
    }

    pub fn put(&self, object_id: u64, data: &[u8]) -> Result<(), FilePayloadStoreError> {
        let payload_path = self.layout.payload_block_path(object_id);
        let payload_dir = payload_path.parent().ok_or_else(|| {
            io::Error::other(format!(
                "payload path has no parent directory: {}",
                payload_path.display()
            ))
        })?;
        std::fs::create_dir_all(payload_dir)?;

        let temp_path = self.write_temp_payload_file(&self.layout.tmp_dir, object_id, data)?;
        match platform::rename_atomic(&temp_path, &payload_path) {
            Ok(()) => {}
            Err(error) if platform::is_cross_device_link_error(&error) => {
                let _ = std::fs::remove_file(&temp_path);
                // Keep rename atomic even with unusual split-mount layouts by
                // staging in the destination directory.
                let payload_local_temp =
                    self.write_temp_payload_file(payload_dir, object_id, data)?;
                if let Err(rename_error) =
                    platform::rename_atomic(&payload_local_temp, &payload_path)
                {
                    let _ = std::fs::remove_file(&payload_local_temp);
                    return Err(FilePayloadStoreError::Io(rename_error));
                }
            }
            Err(error) => {
                let _ = std::fs::remove_file(&temp_path);
                return Err(FilePayloadStoreError::Io(error));
            }
        }
        self.invalidate_cached_read_handle(object_id);
        platform::sync_directory(&self.layout.tmp_dir)?;
        platform::sync_directory(payload_dir)?;
        Ok(())
    }

    pub fn remove(&self, object_id: u64) {
        self.invalidate_cached_read_handle(object_id);
        let payload_path = self.layout.payload_block_path(object_id);
        if let Err(error) = std::fs::remove_file(&payload_path)
            && error.kind() != io::ErrorKind::NotFound
        {
            warn!(
                object_id,
                path = %payload_path.display(),
                error = ?error,
                "cache-v3 payload remove failed"
            );
        }
    }

    pub fn remove_batch(&self, object_ids: &[u64]) {
        for object_id in object_ids {
            self.remove(*object_id);
        }
    }

    pub fn sync_data(&self) -> Result<(), FilePayloadStoreError> {
        for dir in [
            self.layout.payload_blocks_dir.as_path(),
            self.layout.payload_root.as_path(),
            self.layout.v3_root.as_path(),
        ] {
            if let Err(error) = platform::sync_directory(dir) {
                if error.kind() == io::ErrorKind::NotFound {
                    continue;
                }
                return Err(error.into());
            }
        }
        Ok(())
    }

    fn write_temp_payload_file(
        &self,
        temp_dir: &Path,
        object_id: u64,
        data: &[u8],
    ) -> Result<PathBuf, FilePayloadStoreError> {
        let (mut temp_file, temp_path) = self.create_temp_file(temp_dir, object_id)?;
        platform::write_all(&mut temp_file, data)?;
        platform::sync_data(&temp_file)?;
        drop(temp_file);
        Ok(temp_path)
    }

    fn create_temp_file(
        &self,
        temp_dir: &Path,
        object_id: u64,
    ) -> Result<(File, PathBuf), FilePayloadStoreError> {
        let hex = format!("{object_id:016x}");
        for _ in 0..TEMP_FILE_CREATE_ATTEMPTS {
            let candidate =
                temp_dir.join(format!("{hex}-{}{}", next_temp_token(), TEMP_FILE_SUFFIX));
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(&candidate)
            {
                Ok(file) => return Ok((file, candidate)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }

        Err(FilePayloadStoreError::TempFileCreateExhausted {
            object_id,
            dir: temp_dir.to_path_buf(),
        })
    }

    fn open_for_read(path: &Path) -> io::Result<File> {
        match platform::open_read_only(path) {
            Ok(file) => Ok(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(error),
            Err(error) => Err(error),
        }
    }

    fn cached_read_handle(&self, object_id: u64, path: &Path) -> io::Result<Arc<File>> {
        let shard = read_fd_shard(object_id);
        if let Some(file) = self.read_handles.shards[shard]
            .lock()
            .get(&object_id)
            .cloned()
        {
            self.read_handles.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(file);
        }

        self.read_handles.misses.fetch_add(1, Ordering::Relaxed);
        let file = Arc::new(Self::open_for_read(path)?);
        if let Err(error) = platform::advise_random(file.as_ref()) {
            warn!(
                object_id,
                path = %path.display(),
                error = ?error,
                "cache-v3 payload open advise_random failed"
            );
        }

        let mut handles = self.read_handles.shards[shard].lock();
        if let Some(existing) = handles.get(&object_id).cloned() {
            self.read_handles.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(existing);
        }
        let evicted_lru = handles.len() == handles.cap().get() && !handles.contains(&object_id);
        let _ = handles.put(object_id, Arc::clone(&file));
        if evicted_lru {
            self.read_handles.evictions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(file)
    }

    fn invalidate_cached_read_handle(&self, object_id: u64) {
        self.read_handles.shards[read_fd_shard(object_id)]
            .lock()
            .pop(&object_id);
    }

    #[cfg(test)]
    fn read_handle_stats(&self) -> ReadHandleStats {
        ReadHandleStats {
            hits: self.read_handles.hits.load(Ordering::Relaxed),
            misses: self.read_handles.misses.load(Ordering::Relaxed),
            evictions: self.read_handles.evictions.load(Ordering::Relaxed),
            cached: self
                .read_handles
                .shards
                .iter()
                .map(|shard| shard.lock().len())
                .sum(),
        }
    }
}

#[derive(Debug)]
struct ReadHandleCache {
    shards: [Mutex<LruCache<u64, Arc<File>>>; READ_FD_CACHE_SHARDS],
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadHandleStats {
    hits: u64,
    misses: u64,
    evictions: u64,
    cached: usize,
}

fn read_fd_cache_shard_capacity() -> NonZeroUsize {
    let capacity = READ_FD_CACHE_CAPACITY.div_ceil(READ_FD_CACHE_SHARDS);
    NonZeroUsize::new(capacity).expect("READ_FD_CACHE_CAPACITY must be non-zero")
}

fn read_fd_shard(object_id: u64) -> usize {
    (object_id as usize) & (READ_FD_CACHE_SHARDS - 1)
}

fn next_temp_token() -> String {
    let seq = TEMP_FILE_SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:08x}-{:016x}-{:032x}", std::process::id(), seq, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_cold_reopen_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout.clone()).unwrap();
        store.put(42, b"payload").unwrap();
        drop(store);

        let reopened = FilePayloadStore::open(layout).unwrap();
        assert_eq!(reopened.get(42).unwrap().as_ref(), b"payload");
    }

    #[test]
    fn read_into_supports_partial_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        store.put(7, b"abcdef").unwrap();

        let mut out = [0_u8; 3];
        assert_eq!(store.read_into(7, 2, &mut out), Some(3));
        assert_eq!(&out, b"cde");
        assert_eq!(store.read_into(7, 99, &mut out), Some(0));
    }

    #[test]
    fn staged_temp_file_does_not_override_committed_payload_without_rename() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout.clone()).unwrap();
        store.put(9001, b"committed").unwrap();

        // Simulate a crash after temp-file fsync but before atomic rename.
        let (mut temp_file, temp_path) = store.create_temp_file(&layout.tmp_dir, 9001).unwrap();
        platform::write_all(&mut temp_file, b"staged-new-value").unwrap();
        platform::sync_data(&temp_file).unwrap();
        drop(temp_file);
        drop(store);

        let reopened = FilePayloadStore::open(layout).unwrap();
        assert_eq!(reopened.get(9001).unwrap().as_ref(), b"committed");
        assert!(temp_path.exists());
    }

    #[test]
    fn hot_read_into_reuses_cached_descriptors() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        store.put(700, b"abcdef").unwrap();

        let mut out = [0_u8; 3];
        assert_eq!(store.read_into(700, 0, &mut out), Some(3));
        assert_eq!(store.read_into(700, 1, &mut out), Some(3));
        assert_eq!(store.read_into(700, 2, &mut out), Some(3));

        let stats = store.read_handle_stats();
        assert_eq!(stats.misses, 1);
        assert!(
            stats.hits >= 2,
            "expected repeated reads to hit descriptor cache"
        );
        assert_eq!(stats.cached, 1);
    }

    #[test]
    fn overwrite_invalidates_cached_descriptor() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();
        store.put(1337, b"old").unwrap();

        let mut out = [0_u8; 3];
        assert_eq!(store.read_into(1337, 0, &mut out), Some(3));
        assert_eq!(&out, b"old");

        store.put(1337, b"new").unwrap();
        assert_eq!(store.read_into(1337, 0, &mut out), Some(3));
        assert_eq!(&out, b"new");
    }

    #[test]
    fn descriptor_cache_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        let store = FilePayloadStore::open(layout).unwrap();

        for object_id in 0..(READ_FD_CACHE_CAPACITY + 4) as u64 {
            store.put(object_id, b"x").unwrap();
            let mut out = [0_u8; 1];
            assert_eq!(store.read_into(object_id, 0, &mut out), Some(1));
        }

        let stats = store.read_handle_stats();
        assert_eq!(stats.cached, READ_FD_CACHE_CAPACITY);
        assert!(stats.evictions >= 1);
    }
}

//! Cache 3.0 file-per-block payload storage.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tracing::warn;

use super::layout::CacheV3Layout;
use super::platform;

const TEMP_FILE_CREATE_ATTEMPTS: usize = 32;
const TEMP_FILE_SUFFIX: &str = ".tmp";

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
}

impl FilePayloadStore {
    pub fn open(layout: CacheV3Layout) -> Result<Self, FilePayloadStoreError> {
        std::fs::create_dir_all(&layout.payload_blocks_dir)?;
        std::fs::create_dir_all(&layout.tmp_dir)?;
        Ok(Self { layout })
    }

    pub fn get(&self, object_id: u64) -> Option<Bytes> {
        let path = self.layout.payload_block_path(object_id);
        let file = Self::open_for_read(&path).ok()?;
        let len = usize::try_from(file.metadata().ok()?.len()).ok()?;
        let mut payload = vec![0_u8; len];
        if len > 0 {
            if let Err(error) = platform::pread_exact(&file, &mut payload, 0) {
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

        let path = self.layout.payload_block_path(object_id);
        let file = Self::open_for_read(&path).ok()?;
        let file_len = file.metadata().ok()?.len();
        let start = u64::try_from(data_offset).ok()?;
        if start >= file_len {
            return Some(0);
        }

        let available = usize::try_from(file_len - start).unwrap_or(usize::MAX);
        let to_read = out.len().min(available);
        if to_read == 0 {
            return Some(0);
        }

        match platform::pread(&file, &mut out[..to_read], start) {
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
        self.layout
            .payload_block_path(object_id)
            .try_exists()
            .unwrap_or(false)
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

        let (mut temp_file, temp_path) = self.create_temp_file(object_id)?;
        platform::write_all(&mut temp_file, data)?;
        platform::sync_data(&temp_file)?;
        drop(temp_file);

        if let Err(error) = platform::rename_atomic(&temp_path, &payload_path) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(FilePayloadStoreError::Io(error));
        }
        platform::sync_directory(payload_dir)?;
        Ok(())
    }

    pub fn remove(&self, object_id: u64) {
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

    pub fn sync_data(&self) -> Result<(), FilePayloadStoreError> {
        Ok(())
    }

    fn create_temp_file(&self, object_id: u64) -> Result<(File, PathBuf), FilePayloadStoreError> {
        let payload_path = self.layout.payload_block_path(object_id);
        let payload_dir = payload_path.parent().ok_or_else(|| {
            io::Error::other(format!(
                "payload path has no parent directory: {}",
                payload_path.display()
            ))
        })?;

        let hex = format!("{object_id:016x}");
        for _ in 0..TEMP_FILE_CREATE_ATTEMPTS {
            let candidate =
                payload_dir.join(format!("{hex}-{}{}", next_temp_token(), TEMP_FILE_SUFFIX));
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
            dir: payload_dir.to_path_buf(),
        })
    }

    fn open_for_read(path: &Path) -> io::Result<File> {
        match OpenOptions::new().read(true).open(path) {
            Ok(file) => Ok(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(error),
            Err(error) => Err(error),
        }
    }
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
        let (mut temp_file, temp_path) = store.create_temp_file(9001).unwrap();
        platform::write_all(&mut temp_file, b"staged-new-value").unwrap();
        platform::sync_data(&temp_file).unwrap();
        drop(temp_file);
        drop(store);

        let reopened = FilePayloadStore::open(layout).unwrap();
        assert_eq!(reopened.get(9001).unwrap().as_ref(), b"committed");
        assert!(temp_path.exists());
    }
}

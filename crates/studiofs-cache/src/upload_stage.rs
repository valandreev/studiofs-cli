use crate::runtime::LocalPayloadLayout;
use std::fs;
use std::io;
use tracing::warn;

#[derive(Debug, thiserror::Error)]
/// Errors returned by [`UploadStageStore`] operations.
pub enum UploadStageStoreError {
    #[error("I/O error: {0}")]
    /// An underlying filesystem I/O error.
    Io(#[from] io::Error),
}

#[derive(Debug, Clone)]
/// A small local store for upload-staged payload objects.
///
/// The upload stage is backed by files in `layout.upload_stage_objects_dir` and is populated by
/// creating hardlinks that point at durable payload objects in the main payload store.
pub struct UploadStageStore {
    layout: LocalPayloadLayout,
}

impl UploadStageStore {
    /// Opens (and creates if needed) the upload stage directory for the given payload layout.
    pub fn open(layout: LocalPayloadLayout) -> Result<Self, UploadStageStoreError> {
        fs::create_dir_all(&layout.upload_stage_objects_dir)?;
        Ok(Self { layout })
    }

    /// Reads the staged payload bytes for `object_id` if present.
    ///
    /// Returns `None` if the staged file does not exist or if the read fails (a warning is logged
    /// in the latter case).
    pub fn get(&self, object_id: u64) -> Option<bytes::Bytes> {
        let path = self.layout.upload_stage_object_path(object_id);
        match fs::read(&path) {
            Ok(bytes) => Some(bytes::Bytes::from(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => {
                warn!(
                    object_id,
                    path = %path.display(),
                    error = ?error,
                    "upload stage read failed"
                );
                None
            }
        }
    }

    /// Returns `true` if a staged payload file for `object_id` exists.
    pub fn contains(&self, object_id: u64) -> bool {
        self.layout
            .upload_stage_object_path(object_id)
            .try_exists()
            .unwrap_or(false)
    }

    /// Ensures the payload for `object_id` is present in the upload stage.
    ///
    /// This creates a hardlink from the durable payload object into the upload stage directory.
    /// The operation is intended to be idempotent: if the staged file already exists, it is a
    /// no-op.
    pub fn link_payload(&self, object_id: u64) -> Result<(), UploadStageStoreError> {
        let payload_path = self.layout.payload_object_path(object_id);
        let stage_path = self.layout.upload_stage_object_path(object_id);
        let stage_dir = stage_path.parent().ok_or_else(|| {
            io::Error::other(format!(
                "upload stage path has no parent directory: {}",
                stage_path.display()
            ))
        })?;
        fs::create_dir_all(stage_dir)?;

        match fs::hard_link(&payload_path, &stage_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if stage_path.try_exists().unwrap_or(false) {
                    return Ok(());
                }
                let _ = fs::remove_file(&stage_path);
                match fs::hard_link(&payload_path, &stage_path) {
                    Ok(()) => Ok(()),
                    Err(retry_error) if retry_error.kind() == io::ErrorKind::AlreadyExists => {
                        Ok(())
                    }
                    Err(retry_error) => Err(UploadStageStoreError::Io(retry_error)),
                }
            }
            Err(error) => Err(UploadStageStoreError::Io(error)),
        }
    }

    /// Calls [`Self::link_payload`] for each object id in `object_ids`.
    pub fn link_payload_batch(&self, object_ids: &[u64]) -> Result<(), UploadStageStoreError> {
        for &object_id in object_ids {
            self.link_payload(object_id)?;
        }
        Ok(())
    }

    /// Removes the staged payload file for `object_id` if it exists.
    ///
    /// Non-`NotFound` errors are logged as warnings and otherwise ignored.
    pub fn remove(&self, object_id: u64) {
        let path = self.layout.upload_stage_object_path(object_id);
        if let Err(error) = fs::remove_file(&path)
            && error.kind() != io::ErrorKind::NotFound
        {
            warn!(
                object_id,
                path = %path.display(),
                error = ?error,
                "upload stage remove failed"
            );
        }
    }

    /// Calls [`Self::remove`] for each object id in `object_ids`.
    pub fn remove_batch(&self, object_ids: &[u64]) {
        for &object_id in object_ids {
            self.remove(object_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::FilePayloadStore;

    #[test]
    fn upload_stage_roundtrip() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(tmp.path());
        layout.provision().expect("provision");
        let payload_store = FilePayloadStore::open(layout.clone()).expect("payload store");
        let store = UploadStageStore::open(layout.clone()).expect("open");

        payload_store.put(0x42, b"payload").expect("put payload");
        store.link_payload_batch(&[0x42]).expect("link payload");

        assert!(store.contains(0x42));
        assert_eq!(store.get(0x42).unwrap().as_ref(), b"payload");

        store.remove(0x42);
        assert!(!store.contains(0x42));
        assert!(store.get(0x42).is_none());
        assert!(!layout.upload_stage_object_path(0x42).exists());
        assert_eq!(
            payload_store.get(0x42).unwrap().as_ref(),
            b"payload",
            "removing the upload hardlink must keep the durable payload file intact"
        );
    }

    #[test]
    fn upload_stage_link_payload_is_idempotent() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(tmp.path());
        layout.provision().expect("provision");
        let payload_store = FilePayloadStore::open(layout.clone()).expect("payload store");
        let store = UploadStageStore::open(layout).expect("open");

        payload_store.put(0x99, b"payload").expect("put payload");
        store.link_payload(0x99).expect("first link");
        store
            .link_payload(0x99)
            .expect("second link should be a no-op");

        assert!(store.contains(0x99));
        assert_eq!(store.get(0x99).unwrap().as_ref(), b"payload");
    }
}

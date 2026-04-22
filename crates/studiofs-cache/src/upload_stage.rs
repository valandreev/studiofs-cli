use crate::runtime::LocalBlockLayout;
use std::fs;
use std::io;
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum UploadStageStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Clone)]
pub struct UploadStageStore {
    layout: LocalBlockLayout,
}

impl UploadStageStore {
    pub fn open(layout: LocalBlockLayout) -> Result<Self, UploadStageStoreError> {
        fs::create_dir_all(&layout.upload_stage_blocks_dir)?;
        Ok(Self { layout })
    }

    pub fn get(&self, object_id: u64) -> Option<bytes::Bytes> {
        let path = self.layout.upload_stage_block_path(object_id);
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

    pub fn contains(&self, object_id: u64) -> bool {
        self.layout
            .upload_stage_block_path(object_id)
            .try_exists()
            .unwrap_or(false)
    }

    pub fn link_payload(&self, object_id: u64) -> Result<(), UploadStageStoreError> {
        let payload_path = self.layout.payload_block_path(object_id);
        let stage_path = self.layout.upload_stage_block_path(object_id);
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
                let _ = fs::remove_file(&stage_path);
                fs::hard_link(&payload_path, &stage_path).map_err(UploadStageStoreError::Io)
            }
            Err(error) => Err(UploadStageStoreError::Io(error)),
        }
    }

    pub fn link_payload_batch(&self, object_ids: &[u64]) -> Result<(), UploadStageStoreError> {
        for &object_id in object_ids {
            self.link_payload(object_id)?;
        }
        Ok(())
    }

    pub fn remove(&self, object_id: u64) {
        let path = self.layout.upload_stage_block_path(object_id);
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
        let tmp = tempfile::tempdir().expect("tempdir");
        let layout = LocalBlockLayout::from_cache_root(tmp.path());
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
        assert!(!layout.upload_stage_block_path(0x42).exists());
        assert_eq!(
            payload_store.get(0x42).unwrap().as_ref(),
            b"payload",
            "removing the upload hardlink must keep the durable payload file intact"
        );
    }
}

//! Active local payload backend contract used by the current local-cache data path.
//!
//! The goal is to keep VFS/data-path code independent of backend internals
//! while still exposing the payload lifecycle states required for upload replay
//! and transition handling.

use bytes::Bytes;
use std::any::Any;
use uuid::Uuid;

/// Lifecycle state of a single locally persisted payload object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPayloadLifecycleState {
    /// Payload object is not present in local storage.
    Missing,
    /// Payload object is present and dirty (needs upload).
    Dirty,
    /// Payload object is present and currently in-flight for upload.
    Uploading,
    /// Payload object is present and uploaded (not dirty/uploading).
    ///
    /// Note: this does not imply the payload object is evictable in every backend.
    /// Backend-specific constraints (for example pinning) may still prevent
    /// eviction.
    Uploaded,
}

/// Where a persisted local payload originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPayloadOrigin {
    /// Materialized locally by the write path and may need upload/publish work.
    Local,
    /// Downloaded from the published remote view for read-cache reuse.
    Downloaded,
}

/// Upload state tracked per persisted local payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPayloadUploadState {
    /// No upload work is pending for this payload object.
    None,
    /// The payload object is durably accepted locally and queued for background upload.
    Pending,
    /// The payload object is currently being uploaded by the background pipeline.
    Uploading,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocalPayloadStorageStats {
    pub current_bytes: u64,
    pub dirty_bytes: u64,
    pub evictable_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalPayloadObjectStats {
    pub payload_len: u64,
    pub lifecycle: LocalPayloadLifecycleState,
    pub origin: LocalPayloadOrigin,
    pub upload_state: LocalPayloadUploadState,
    pub last_access_unix_millis: u64,
    pub evictable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPayloadPut {
    pub object_id: u64,
    pub data: Bytes,
    pub evictable: bool,
}

/// Backend-agnostic error surface for local payload operations.
#[derive(Debug, thiserror::Error)]
pub enum LocalPayloadBackendError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("local backend full: need {needed} bytes, free {free} bytes")]
    CapacityExhausted { needed: u64, free: u64 },
    #[error("local backend operation failed: {0}")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Minimal local payload API required by read/write/upload paths.
pub trait LocalPayloadBackend: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn get(&self, object_id: u64) -> Option<Bytes>;
    fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize>;
    fn get_for_upload(&self, object_id: u64) -> Option<Bytes>;
    fn contains(&self, object_id: u64) -> bool;
    fn advise_will_need(&self, object_id: u64);

    fn put(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), LocalPayloadBackendError>;
    fn put_batch(&self, entries: &[LocalPayloadPut]) -> Result<(), LocalPayloadBackendError> {
        for entry in entries {
            self.put(entry.object_id, entry.data.as_ref(), entry.evictable)?;
        }
        Ok(())
    }
    fn sync_data(&self) -> Result<(), LocalPayloadBackendError>;
    fn run_pressure_maintenance(&self) -> Result<(), LocalPayloadBackendError>;

    fn mark_uploading(&self, object_id: u64);
    fn mark_upload_failed(&self, object_id: u64);
    fn mark_uploaded(&self, object_id: u64);
    fn pin(&self, object_id: u64);
    fn unpin(&self, object_id: u64);
    fn pin_batch(&self, object_ids: &[u64]) {
        for &object_id in object_ids {
            self.pin(object_id);
        }
    }
    fn unpin_batch(&self, object_ids: &[u64]) {
        for &object_id in object_ids {
            self.unpin(object_id);
        }
    }

    fn remove(&self, object_id: u64);
    fn remove_batch(&self, object_ids: &[u64]);

    fn dirty_object_ids(&self) -> Vec<u64>;
    fn dirty_object_count(&self) -> usize {
        self.dirty_object_ids().len()
    }
    fn lifecycle_state(&self, object_id: u64) -> LocalPayloadLifecycleState;
    fn is_evictable(&self, object_id: u64) -> bool;
    fn storage_stats(&self) -> LocalPayloadStorageStats {
        LocalPayloadStorageStats::default()
    }
    fn object_stats(&self, object_id: u64) -> Option<LocalPayloadObjectStats> {
        let _ = object_id;
        None
    }
    fn object_ids(&self) -> Vec<u64> {
        Vec::new()
    }
}

/// Derive a stable 16-byte local backend volume UUID from a format UUID string.
///
/// Accepts plain UUID strings and falls back to deterministic UUIDv5 over the
/// provided bytes so legacy/non-UUID values still map consistently.
pub fn derive_backend_volume_uuid(volume_uuid: &str) -> [u8; 16] {
    Uuid::parse_str(volume_uuid)
        .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_URL, volume_uuid.as_bytes()))
        .into_bytes()
}

#[cfg(test)]
pub(crate) mod contract_tests {
    use super::{LocalPayloadBackend, LocalPayloadLifecycleState};

    pub(crate) fn assert_local_payload_backend_contract<B, G, F>(factory: F)
    where
        B: LocalPayloadBackend,
        F: Fn() -> (B, G),
    {
        dirty_upload_lifecycle_roundtrip(&factory);
        dirty_snapshot_ordering(&factory);
        remove_operations_clear_visibility(&factory);
    }

    fn dirty_upload_lifecycle_roundtrip<B, G, F>(factory: &F)
    where
        B: LocalPayloadBackend,
        F: Fn() -> (B, G),
    {
        let (backend, _guard) = factory();
        let object_id = 101;

        assert_eq!(
            backend.lifecycle_state(object_id),
            LocalPayloadLifecycleState::Missing
        );

        backend.put(object_id, b"dirty-block", false).unwrap();
        backend.sync_data().unwrap();
        assert!(backend.contains(object_id));
        assert_eq!(
            backend.get(object_id).unwrap().as_ref(),
            b"dirty-block",
            "read path must return persisted bytes"
        );
        assert_eq!(
            backend.lifecycle_state(object_id),
            LocalPayloadLifecycleState::Dirty
        );
        assert_eq!(backend.dirty_object_ids(), vec![object_id]);

        backend.mark_uploading(object_id);
        assert_eq!(
            backend.lifecycle_state(object_id),
            LocalPayloadLifecycleState::Uploading
        );
        assert_eq!(backend.dirty_object_ids(), vec![object_id]);
        assert_eq!(
            backend.get_for_upload(object_id).unwrap().as_ref(),
            b"dirty-block",
            "upload reread must return persisted bytes"
        );

        backend.mark_upload_failed(object_id);
        assert_eq!(
            backend.lifecycle_state(object_id),
            LocalPayloadLifecycleState::Dirty
        );
        assert_eq!(backend.dirty_object_ids(), vec![object_id]);

        backend.mark_uploading(object_id);
        backend.mark_uploaded(object_id);
        assert_eq!(
            backend.lifecycle_state(object_id),
            LocalPayloadLifecycleState::Uploaded
        );
        assert!(
            backend.dirty_object_ids().is_empty(),
            "uploaded payload object must no longer be dirty"
        );
    }

    fn dirty_snapshot_ordering<B, G, F>(factory: &F)
    where
        B: LocalPayloadBackend,
        F: Fn() -> (B, G),
    {
        let (backend, _guard) = factory();

        backend.put(1, b"a", false).unwrap();
        backend.put(2, b"b", true).unwrap();
        backend.put(3, b"c", false).unwrap();
        backend.sync_data().unwrap();

        assert_eq!(
            backend.dirty_object_ids(),
            vec![1, 3],
            "dirty snapshot must remain deterministic oldest-first"
        );
        assert_eq!(
            backend.lifecycle_state(2),
            LocalPayloadLifecycleState::Uploaded
        );
    }

    fn remove_operations_clear_visibility<B, G, F>(factory: &F)
    where
        B: LocalPayloadBackend,
        F: Fn() -> (B, G),
    {
        let (backend, _guard) = factory();

        backend.put(10, b"x", false).unwrap();
        backend.put(11, b"y", false).unwrap();
        backend.sync_data().unwrap();

        backend.remove(10);
        assert_eq!(
            backend.lifecycle_state(10),
            LocalPayloadLifecycleState::Missing
        );
        assert!(!backend.contains(10));

        backend.remove_batch(&[11, 12]);
        assert_eq!(
            backend.lifecycle_state(11),
            LocalPayloadLifecycleState::Missing
        );
        assert!(
            backend.dirty_object_ids().is_empty(),
            "removed blocks must no longer appear as dirty"
        );
    }
}

//! Active local block backend contract used by the Cache 3.0 data path.
//!
//! The goal is to keep VFS/data-path code independent of backend internals
//! while still exposing the block lifecycle states required for upload replay
//! and transition handling.

use bytes::Bytes;
use std::any::Any;
use uuid::Uuid;

/// Lifecycle state of a single locally persisted block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalBlockLifecycleState {
    /// Block is not present in local storage.
    Missing,
    /// Block is present and dirty (needs upload).
    Dirty,
    /// Block is present and currently in-flight for upload.
    Uploading,
    /// Block is present and uploaded (not dirty/uploading).
    ///
    /// Note: this does not imply the block is evictable in every backend.
    /// Backend-specific constraints (for example pinning) may still prevent
    /// eviction.
    Uploaded,
}

/// Backend-agnostic error surface for local block operations.
#[derive(Debug, thiserror::Error)]
pub enum LocalBlockBackendError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("local backend full: need {needed} bytes, free {free} bytes")]
    CapacityExhausted { needed: u64, free: u64 },
    #[error("local backend operation failed: {0}")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Minimal local block API required by read/write/upload paths.
pub trait LocalBlockBackend: Send + Sync {
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
    ) -> Result<(), LocalBlockBackendError>;
    fn sync_data(&self) -> Result<(), LocalBlockBackendError>;
    fn run_pressure_maintenance(&self) -> Result<(), LocalBlockBackendError>;

    fn mark_uploading(&self, object_id: u64);
    fn mark_upload_failed(&self, object_id: u64);
    fn mark_uploaded(&self, object_id: u64);

    fn remove(&self, object_id: u64);
    fn remove_batch(&self, object_ids: &[u64]);

    fn dirty_object_ids(&self) -> Vec<u64>;
    fn lifecycle_state(&self, object_id: u64) -> LocalBlockLifecycleState;
    fn is_evictable(&self, object_id: u64) -> bool;
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
    use super::{LocalBlockBackend, LocalBlockLifecycleState};

    pub(crate) fn assert_local_block_backend_contract<B, G, F>(factory: F)
    where
        B: LocalBlockBackend,
        F: Fn() -> (B, G),
    {
        dirty_upload_lifecycle_roundtrip(&factory);
        dirty_snapshot_ordering(&factory);
        remove_operations_clear_visibility(&factory);
    }

    fn dirty_upload_lifecycle_roundtrip<B, G, F>(factory: &F)
    where
        B: LocalBlockBackend,
        F: Fn() -> (B, G),
    {
        let (backend, _guard) = factory();
        let object_id = 101;

        assert_eq!(
            backend.lifecycle_state(object_id),
            LocalBlockLifecycleState::Missing
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
            LocalBlockLifecycleState::Dirty
        );
        assert_eq!(backend.dirty_object_ids(), vec![object_id]);

        backend.mark_uploading(object_id);
        assert_eq!(
            backend.lifecycle_state(object_id),
            LocalBlockLifecycleState::Uploading
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
            LocalBlockLifecycleState::Dirty
        );
        assert_eq!(backend.dirty_object_ids(), vec![object_id]);

        backend.mark_uploading(object_id);
        backend.mark_uploaded(object_id);
        assert_eq!(
            backend.lifecycle_state(object_id),
            LocalBlockLifecycleState::Uploaded
        );
        assert!(
            backend.dirty_object_ids().is_empty(),
            "uploaded block must no longer be dirty"
        );
    }

    fn dirty_snapshot_ordering<B, G, F>(factory: &F)
    where
        B: LocalBlockBackend,
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
            LocalBlockLifecycleState::Uploaded
        );
    }

    fn remove_operations_clear_visibility<B, G, F>(factory: &F)
    where
        B: LocalBlockBackend,
        F: Fn() -> (B, G),
    {
        let (backend, _guard) = factory();

        backend.put(10, b"x", false).unwrap();
        backend.put(11, b"y", false).unwrap();
        backend.sync_data().unwrap();

        backend.remove(10);
        assert_eq!(
            backend.lifecycle_state(10),
            LocalBlockLifecycleState::Missing
        );
        assert!(!backend.contains(10));

        backend.remove_batch(&[11, 12]);
        assert_eq!(
            backend.lifecycle_state(11),
            LocalBlockLifecycleState::Missing
        );
        assert!(
            backend.dirty_object_ids().is_empty(),
            "removed blocks must no longer appear as dirty"
        );
    }
}

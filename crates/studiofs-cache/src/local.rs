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
/// Aggregate storage usage counters for a local payload backend.
pub struct LocalPayloadStorageStats {
    /// Total bytes currently stored by the backend.
    pub current_bytes: u64,
    /// Total bytes considered dirty (pending upload).
    pub dirty_bytes: u64,
    /// Total bytes that are eligible for eviction (subject to backend constraints).
    pub evictable_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Per-object statistics returned by [`LocalPayloadBackend::object_stats`].
pub struct LocalPayloadObjectStats {
    /// Payload length in bytes.
    pub payload_len: u64,
    /// Current lifecycle state.
    pub lifecycle: LocalPayloadLifecycleState,
    /// Where the payload originated.
    pub origin: LocalPayloadOrigin,
    /// Upload state associated with this payload.
    pub upload_state: LocalPayloadUploadState,
    /// Last-access timestamp in Unix milliseconds.
    pub last_access_unix_millis: u64,
    /// Whether the backend currently considers this payload evictable.
    pub evictable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Owned put request used by batch APIs.
pub struct LocalPayloadPut {
    /// Backend object id.
    pub object_id: u64,
    /// Payload bytes to persist.
    pub data: Bytes,
    /// Whether the backend may evict this payload under pressure.
    pub evictable: bool,
}

/// Backend-agnostic error surface for local payload operations.
#[derive(Debug, thiserror::Error)]
pub enum LocalPayloadBackendError {
    /// An underlying OS I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The backend cannot accept the write because it would exceed local capacity.
    #[error("local backend full: need {needed} bytes, free {free} bytes")]
    CapacityExhausted {
        /// How many bytes would be needed to perform the operation.
        needed: u64,
        /// How many bytes are currently free in the backend.
        free: u64,
    },
    /// A backend-specific error not otherwise classified by this contract.
    #[error("local backend operation failed: {0}")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Minimal local payload API required by read/write/upload paths.
pub trait LocalPayloadBackend: Send + Sync {
    /// Returns `self` as `dyn Any` for downcasting to a concrete backend type.
    fn as_any(&self) -> &dyn Any;

    /// Returns the full payload bytes for `object_id`, if present.
    fn get(&self, object_id: u64) -> Option<Bytes>;

    /// Reads a byte range from the payload into `out`.
    ///
    /// Returns the number of bytes written on success, or `None` if the payload
    /// is not present.
    fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize>;

    /// Returns bytes suitable for upload for `object_id`, if present.
    fn get_for_upload(&self, object_id: u64) -> Option<Bytes>;

    /// Returns `true` if `object_id` exists in the backend storage.
    fn contains(&self, object_id: u64) -> bool;

    /// Provides a read-ahead hint to the backend that `object_id` will be used soon.
    fn advise_will_need(&self, object_id: u64);

    /// Persists payload data for `object_id`.
    ///
    /// `evictable` expresses whether the backend may evict the payload under
    /// storage pressure (subject to backend-specific constraints).
    fn put(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), LocalPayloadBackendError>;

    /// Convenience wrapper around [`Self::put`] taking an owned entry.
    fn put_owned(&self, put: LocalPayloadPut) -> Result<(), LocalPayloadBackendError> {
        self.put(put.object_id, put.data.as_ref(), put.evictable)
    }

    /// Persists multiple payload entries as a best-effort batch operation.
    fn put_batch(&self, entries: &[LocalPayloadPut]) -> Result<(), LocalPayloadBackendError> {
        for entry in entries {
            self.put_owned(entry.clone())?;
        }
        Ok(())
    }

    /// Flushes any buffered payload data and makes it durable.
    fn sync_data(&self) -> Result<(), LocalPayloadBackendError>;

    /// Runs backend-specific maintenance to relieve local storage pressure.
    fn run_pressure_maintenance(&self) -> Result<(), LocalPayloadBackendError>;

    /// Marks `object_id` as currently being uploaded.
    fn mark_uploading(&self, object_id: u64);
    /// Marks the upload attempt for `object_id` as failed (payload remains dirty).
    fn mark_upload_failed(&self, object_id: u64);
    /// Marks `object_id` as uploaded (no longer dirty).
    fn mark_uploaded(&self, object_id: u64);

    /// Pins `object_id` to prevent eviction.
    fn pin(&self, object_id: u64);
    /// Unpins `object_id`, allowing eviction if otherwise eligible.
    fn unpin(&self, object_id: u64);

    /// Pins all payload objects listed in `object_ids`.
    fn pin_batch(&self, object_ids: &[u64]) {
        for &object_id in object_ids {
            self.pin(object_id);
        }
    }

    /// Unpins all payload objects listed in `object_ids`.
    fn unpin_batch(&self, object_ids: &[u64]) {
        for &object_id in object_ids {
            self.unpin(object_id);
        }
    }

    /// Removes `object_id` from the backend.
    fn remove(&self, object_id: u64);
    /// Removes all payload objects listed in `object_ids` from the backend.
    fn remove_batch(&self, object_ids: &[u64]);

    /// Returns the ordered snapshot of dirty payload object ids.
    fn dirty_object_ids(&self) -> Vec<u64>;

    /// Returns the number of dirty payload objects in the current snapshot.
    fn dirty_object_count(&self) -> usize {
        self.dirty_object_ids().len()
    }

    /// Returns the lifecycle state for `object_id`.
    fn lifecycle_state(&self, object_id: u64) -> LocalPayloadLifecycleState;

    /// Returns `true` if `object_id` is currently considered evictable.
    fn is_evictable(&self, object_id: u64) -> bool;

    /// Returns aggregate storage statistics for the backend.
    fn storage_stats(&self) -> LocalPayloadStorageStats {
        LocalPayloadStorageStats::default()
    }

    /// Returns per-object statistics for `object_id`, if available.
    fn object_stats(&self, object_id: u64) -> Option<LocalPayloadObjectStats> {
        let _ = object_id;
        None
    }

    /// Returns all payload object ids known to the backend.
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

        backend.put(object_id, b"dirty-payload", false).unwrap();
        backend.sync_data().unwrap();
        assert!(backend.contains(object_id));
        assert_eq!(
            backend.get(object_id).unwrap().as_ref(),
            b"dirty-payload",
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
            b"dirty-payload",
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
            "removed payload objects must no longer appear as dirty"
        );
    }
}

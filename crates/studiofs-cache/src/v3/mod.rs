//! Cache 3.0 contract and rollout guardrails.
//!
//! This module defines:
//! - the non-mutating detection contract for legacy vs v3 cache layouts;
//! - rollout guardrails that block making Cache 3.0 the default backend
//!   until final validation is complete.
//! - the active Cache 3.0 local block backend backed by a file-per-block
//!   payload store plus redb lifecycle index.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use tracing::warn;

use crate::v2::{LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState};

pub mod cache_index;
pub mod file_payload_store;
pub mod layout;
pub mod platform;

pub use cache_index::{
    CacheIndex, CacheIndexEntry, CacheIndexError, CacheIndexStats, CacheIndexUpsert,
    DirtyReplayEntry,
};
pub use file_payload_store::{FilePayloadStore, FilePayloadStoreError};
pub use layout::{
    CACHE_V3_LAYOUT_MARKER_FILE, CACHE_V3_LAYOUT_VERSION, CACHE_V3_PAYLOAD_SHARD_COUNT,
    CACHE_V3_REDB_INDEX_FILE, CACHE_V3_SUBTREE, CacheV3Layout,
};

const LEGACY_RAW_DATA_FILE_NAME: &str = "blocks.raw";
const ACCESS_TOUCH_MIN_INTERVAL: Duration = Duration::from_secs(60);

/// Final validation issue that must pass before Cache 3.0 is allowed as default.
///
/// This must be set before flipping `CACHE_V3_DEFAULT_ALLOWED` to `true`.
pub const CACHE_V3_FINAL_VALIDATION_ISSUE: Option<&str> = None;
/// Explicit rollout gate. Remains false until final validation passes.
pub const CACHE_V3_DEFAULT_ALLOWED: bool = false;
/// Human-readable rollout rule surfaced by status and mount errors.
pub const CACHE_V3_DEFAULT_GUARDRAIL: &str = "Cache 3.0 cannot become the default backend until the final validation \
     issue closes and this gate is explicitly switched to true.";

/// Backend kind selected by the current on-disk contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheBackendKind {
    LegacyRawV2,
    CacheV3,
}

impl CacheBackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyRawV2 => "legacy-raw-v2",
            Self::CacheV3 => "cache-v3",
        }
    }
}

impl fmt::Display for CacheBackendKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Detection snapshot for a per-volume cache root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheBackendDetection {
    pub layout: CacheV3Layout,
    pub legacy_raw_data_path: PathBuf,
    pub legacy_raw_data_exists: bool,
    pub v3_layout_marker_exists: bool,
}

impl CacheBackendDetection {
    pub fn has_legacy_raw_backend(&self) -> bool {
        self.legacy_raw_data_exists
    }

    pub fn has_cache_v3_contract(&self) -> bool {
        self.v3_layout_marker_exists
    }

    pub fn has_dual_layout(&self) -> bool {
        self.has_legacy_raw_backend() && self.has_cache_v3_contract()
    }

    /// Mount backend contract selection for the current rollout phase.
    ///
    /// During rollout we keep legacy raw as the selected backend whenever it
    /// exists, even if a v3 subtree is present. A v3-only root selects Cache 3.0.
    pub fn selected_mount_backend(&self) -> CacheBackendKind {
        if self.has_cache_v3_contract() && !self.has_legacy_raw_backend() {
            CacheBackendKind::CacheV3
        } else {
            CacheBackendKind::LegacyRawV2
        }
    }
}

/// Detect backend contracts for a per-volume cache root using read-only probes.
///
/// This function never creates, removes, or mutates filesystem entries.
pub fn detect_cache_backends(cache_root: &Path) -> io::Result<CacheBackendDetection> {
    let layout = CacheV3Layout::from_cache_root(cache_root);
    let legacy_raw_data_path = cache_root.join(LEGACY_RAW_DATA_FILE_NAME);
    let v3_layout_marker_exists = layout.layout_marker.try_exists()?;
    Ok(CacheBackendDetection {
        layout,
        legacy_raw_data_exists: legacy_raw_data_path.try_exists()?,
        v3_layout_marker_exists,
        legacy_raw_data_path,
    })
}

/// Configuration for opening the Cache 3.0 local backend.
#[derive(Debug, Clone)]
pub struct CacheV3LocalBlockBackendConfig {
    pub cache_root: PathBuf,
    pub volume_uuid: [u8; 16],
}

/// Errors returned while opening the Cache 3.0 local backend.
#[derive(Debug, thiserror::Error)]
pub enum CacheV3LocalBlockBackendError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("cache-v3 layout marker is missing at '{path}'")]
    MissingLayoutMarker { path: PathBuf },
    #[error("failed to open cache-v3 index at '{path}': {source}")]
    OpenCacheIndex {
        path: PathBuf,
        #[source]
        source: CacheIndexError,
    },
    #[error("failed to open cache-v3 payload store at '{cache_root}': {source}")]
    OpenPayloadStore {
        cache_root: PathBuf,
        #[source]
        source: FilePayloadStoreError,
    },
}

/// Cache 3.0 local backend backed by file-per-block payload files and redb index.
#[derive(Debug)]
pub struct CacheV3LocalBlockBackend {
    layout: CacheV3Layout,
    volume_uuid: [u8; 16],
    index: CacheIndex,
    payload_store: FilePayloadStore,
}

impl CacheV3LocalBlockBackend {
    pub fn open(
        config: CacheV3LocalBlockBackendConfig,
    ) -> Result<Self, CacheV3LocalBlockBackendError> {
        let layout = CacheV3Layout::from_cache_root(&config.cache_root);
        if !layout.layout_marker.try_exists()? {
            return Err(CacheV3LocalBlockBackendError::MissingLayoutMarker {
                path: layout.layout_marker.clone(),
            });
        }

        let index = CacheIndex::open(&layout.redb_index_path).map_err(|source| {
            CacheV3LocalBlockBackendError::OpenCacheIndex {
                path: layout.redb_index_path.clone(),
                source,
            }
        })?;

        let payload_store = FilePayloadStore::open(layout.clone()).map_err(|source| {
            CacheV3LocalBlockBackendError::OpenPayloadStore {
                cache_root: layout.v3_root.clone(),
                source,
            }
        })?;

        Ok(Self {
            layout,
            volume_uuid: config.volume_uuid,
            index,
            payload_store,
        })
    }

    fn has_index_entry(&self, object_id: u64) -> bool {
        match self.index.get_entry(object_id) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "cache-v3 index lookup failed while checking payload presence"
                );
                false
            }
        }
    }

    fn touch_access_if_read(&self, object_id: u64, read_len: usize, op: &'static str) {
        if read_len == 0 {
            return;
        }
        if let Err(error) = self
            .index
            .touch_access_if_stale(object_id, ACCESS_TOUCH_MIN_INTERVAL)
        {
            warn!(
                object_id,
                operation = op,
                error = ?error,
                "cache-v3 index access touch failed"
            );
        }
    }
}

impl LocalBlockBackend for CacheV3LocalBlockBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get(&self, object_id: u64) -> Option<Bytes> {
        if !self.has_index_entry(object_id) {
            return None;
        }
        let payload = self.payload_store.get(object_id);
        if payload.is_some() {
            self.touch_access_if_read(object_id, 1, "get");
        }
        payload
    }

    fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        if !self.has_index_entry(object_id) {
            return None;
        }
        let read = self.payload_store.read_into(object_id, data_offset, out);
        if let Some(bytes) = read {
            self.touch_access_if_read(object_id, bytes, "read_into");
        }
        read
    }

    fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
        if !self.has_index_entry(object_id) {
            return None;
        }
        self.payload_store.get(object_id)
    }

    fn contains(&self, object_id: u64) -> bool {
        self.has_index_entry(object_id) && self.payload_store.contains(object_id)
    }

    fn advise_will_need(&self, _object_id: u64) {}

    fn put(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), LocalBlockBackendError> {
        self.payload_store
            .put(object_id, data)
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;

        let lifecycle = if evictable {
            LocalBlockLifecycleState::Uploaded
        } else {
            LocalBlockLifecycleState::Dirty
        };
        let payload_len = u64::try_from(data.len()).map_err(|_| {
            LocalBlockBackendError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "payload length exceeds u64",
            ))
        })?;
        let payload_key = self
            .layout
            .payload_block_relative_path(object_id)
            .to_string_lossy()
            .into_owned()
            .into_bytes();
        if let Err(error) = self.index.upsert(CacheIndexUpsert {
            object_id,
            payload_key,
            payload_len,
            lifecycle,
            evictable,
        }) {
            self.payload_store.remove(object_id);
            warn!(
                object_id,
                volume_uuid = ?self.volume_uuid,
                error = ?error,
                "cache-v3 index upsert failed after payload write; removed payload for rollback"
            );
            return Err(LocalBlockBackendError::Other(Box::new(error)));
        }

        Ok(())
    }

    fn sync_data(&self) -> Result<(), LocalBlockBackendError> {
        self.payload_store
            .sync_data()
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))
    }

    fn run_pressure_maintenance(&self) -> Result<(), LocalBlockBackendError> {
        Ok(())
    }

    fn mark_uploading(&self, object_id: u64) {
        if let Err(error) = self.index.mark_uploading(object_id) {
            warn!(object_id, error = ?error, "cache-v3 index mark_uploading failed");
        }
    }

    fn mark_upload_failed(&self, object_id: u64) {
        if let Err(error) = self.index.mark_upload_failed(object_id) {
            warn!(
                object_id,
                error = ?error,
                "cache-v3 index mark_upload_failed failed"
            );
        }
    }

    fn mark_uploaded(&self, object_id: u64) {
        if let Err(error) = self.index.mark_uploaded(object_id) {
            warn!(object_id, error = ?error, "cache-v3 index mark_uploaded failed");
        }
    }

    fn remove(&self, object_id: u64) {
        self.payload_store.remove(object_id);
        if let Err(error) = self.index.remove(object_id) {
            warn!(object_id, error = ?error, "cache-v3 index remove failed");
        }
    }

    fn remove_batch(&self, object_ids: &[u64]) {
        for object_id in object_ids {
            self.payload_store.remove(*object_id);
        }
        if let Err(error) = self.index.remove_batch(object_ids) {
            warn!(
                count = object_ids.len(),
                error = ?error,
                "cache-v3 index remove_batch failed"
            );
        }
    }

    fn dirty_object_ids(&self) -> Vec<u64> {
        match self.index.dirty_uploading_entries() {
            Ok(entries) => entries.into_iter().map(|entry| entry.object_id).collect(),
            Err(error) => {
                warn!(
                    error = ?error,
                    "cache-v3 index dirty snapshot failed"
                );
                Vec::new()
            }
        }
    }

    fn lifecycle_state(&self, object_id: u64) -> LocalBlockLifecycleState {
        match self.index.get_entry(object_id) {
            Ok(Some(entry)) => entry.lifecycle,
            Ok(None) => LocalBlockLifecycleState::Missing,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "cache-v3 index lifecycle lookup failed"
                );
                LocalBlockLifecycleState::Missing
            }
        }
    }

    fn is_evictable(&self, object_id: u64) -> bool {
        match self.index.get_entry(object_id) {
            Ok(Some(entry)) => entry.evictable,
            Ok(None) => false,
            Err(error) => {
                warn!(
                    object_id,
                    error = ?error,
                    "cache-v3 index evictable lookup failed"
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::{LocalBlockBackend, LocalBlockLifecycleState};

    fn create_v3_marker(layout: &CacheV3Layout) {
        std::fs::create_dir_all(&layout.v3_root).unwrap();
        std::fs::write(&layout.layout_marker, b"v1").unwrap();
    }

    #[test]
    fn empty_root_defaults_to_legacy_contract() {
        let temp = tempfile::tempdir().unwrap();
        let detection = detect_cache_backends(temp.path()).unwrap();

        assert!(!detection.has_legacy_raw_backend());
        assert!(!detection.has_cache_v3_contract());
        assert_eq!(
            detection.selected_mount_backend(),
            CacheBackendKind::LegacyRawV2
        );
    }

    #[test]
    fn detects_legacy_raw_layout() {
        let temp = tempfile::tempdir().unwrap();
        let raw_path = temp.path().join(LEGACY_RAW_DATA_FILE_NAME);
        std::fs::write(&raw_path, b"raw").unwrap();

        let detection = detect_cache_backends(temp.path()).unwrap();
        assert!(detection.has_legacy_raw_backend());
        assert!(!detection.has_cache_v3_contract());
        assert_eq!(
            detection.selected_mount_backend(),
            CacheBackendKind::LegacyRawV2
        );
    }

    #[test]
    fn detects_v3_only_layout() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let detection = detect_cache_backends(temp.path()).unwrap();
        assert!(!detection.has_legacy_raw_backend());
        assert!(detection.has_cache_v3_contract());
        assert_eq!(
            detection.selected_mount_backend(),
            CacheBackendKind::CacheV3
        );
    }

    #[test]
    fn detects_dual_layout_and_prefers_legacy_contract_during_rollout() {
        let temp = tempfile::tempdir().unwrap();
        let raw_path = temp.path().join(LEGACY_RAW_DATA_FILE_NAME);
        std::fs::write(&raw_path, b"raw").unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let detection = detect_cache_backends(temp.path()).unwrap();
        assert!(detection.has_dual_layout());
        assert_eq!(
            detection.selected_mount_backend(),
            CacheBackendKind::LegacyRawV2
        );
    }

    #[test]
    fn cache_v3_backend_requires_layout_marker() {
        let temp = tempfile::tempdir().unwrap();
        let err = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0xAB; 16],
        })
        .unwrap_err();
        assert!(matches!(
            err,
            CacheV3LocalBlockBackendError::MissingLayoutMarker { .. }
        ));
    }

    #[test]
    fn cache_v3_backend_roundtrips_payload_and_index_state() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0xCD; 16],
        })
        .unwrap();
        backend.put(42, b"payload", false).unwrap();
        backend.sync_data().unwrap();
        assert_eq!(backend.get_for_upload(42).unwrap().as_ref(), b"payload");
        assert_eq!(backend.lifecycle_state(42), LocalBlockLifecycleState::Dirty);
        assert_eq!(backend.dirty_object_ids(), vec![42]);
    }

    #[test]
    fn cache_v3_backend_uses_index_state_after_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x11; 16],
        })
        .unwrap();
        backend.put(1, b"one", false).unwrap();
        backend.put(2, b"two", false).unwrap();
        backend.mark_uploading(2);
        backend.sync_data().unwrap();
        drop(backend);

        // Simulate source-of-truth recovery decisions in the redb index.
        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        index.mark_uploaded(1).unwrap();
        drop(index);

        let reopened = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x11; 16],
        })
        .unwrap();
        assert_eq!(
            reopened.lifecycle_state(1),
            LocalBlockLifecycleState::Uploaded
        );
        assert_eq!(
            reopened.lifecycle_state(2),
            LocalBlockLifecycleState::Uploading
        );
        assert_eq!(reopened.dirty_object_ids(), vec![2]);
    }

    #[test]
    fn cache_v3_backend_get_refreshes_access_order() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x22; 16],
        })
        .unwrap();
        backend.put(10, b"ten", true).unwrap();
        backend.put(11, b"eleven", true).unwrap();
        assert_eq!(backend.get(10).unwrap().as_ref(), b"ten");
        drop(backend);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        assert_eq!(index.access_order_object_ids().unwrap(), vec![11, 10]);
    }

    #[test]
    fn cache_v3_backend_persists_blocks_in_file_per_block_payload_layout() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x33; 16],
        })
        .unwrap();
        backend.put(0xABCD, b"payload", false).unwrap();
        backend.sync_data().unwrap();

        assert!(
            layout.payload_block_path(0xABCD).exists(),
            "cache-v3 must persist each block as an individual payload file"
        );
        assert!(
            !layout.v3_root.join(LEGACY_RAW_DATA_FILE_NAME).exists(),
            "cache-v3 data path must not rely on legacy single-file blocks.raw storage"
        );
    }

    #[test]
    fn cache_v3_backend_reopens_without_legacy_raw_payload_file() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let backend = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x44; 16],
        })
        .unwrap();
        backend.put(77, b"reopen-payload", false).unwrap();
        backend.sync_data().unwrap();
        drop(backend);

        let legacy_raw = layout.v3_root.join(LEGACY_RAW_DATA_FILE_NAME);
        if legacy_raw.exists() {
            std::fs::remove_file(&legacy_raw).unwrap();
        }

        let reopened = CacheV3LocalBlockBackend::open(CacheV3LocalBlockBackendConfig {
            cache_root: temp.path().to_path_buf(),
            volume_uuid: [0x44; 16],
        })
        .unwrap();
        assert_eq!(reopened.get(77).unwrap().as_ref(), b"reopen-payload");
    }
}

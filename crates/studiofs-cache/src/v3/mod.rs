//! Cache 3.0 contract and rollout guardrails.
//!
//! This module defines:
//! - the non-mutating detection contract for legacy vs v3 cache layouts;
//! - rollout guardrails that block making Cache 3.0 the default backend
//!   until final validation is complete.
//! - a bootstrap-only `LocalBlockBackend` placeholder to wire mount/status
//!   selection while the full data path is still in migration.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::v2::{
    LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState, RawBlockFileBackend,
    RawBlockFileBackendConfig, RawBlockFileBackendError,
};

pub mod layout;

pub use layout::{
    CACHE_V3_LAYOUT_MARKER_FILE, CACHE_V3_LAYOUT_VERSION, CACHE_V3_PAYLOAD_SHARD_COUNT,
    CACHE_V3_REDB_INDEX_FILE, CACHE_V3_SUBTREE, CacheV3Layout,
};

const LEGACY_RAW_DATA_FILE_NAME: &str = "blocks.raw";

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

/// Configuration for opening the Cache 3.0 local backend placeholder.
#[derive(Debug, Clone)]
pub struct CacheV3LocalBlockBackendConfig {
    pub cache_root: PathBuf,
    pub volume_uuid: [u8; 16],
}

/// Errors returned while opening the Cache 3.0 local backend placeholder.
#[derive(Debug, thiserror::Error)]
pub enum CacheV3LocalBlockBackendError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("cache-v3 layout marker is missing at '{path}'")]
    MissingLayoutMarker { path: PathBuf },
    #[error("failed to open cache-v3 compatibility raw backend at '{cache_root}': {source}")]
    OpenCompatibilityRawBackend {
        cache_root: PathBuf,
        #[source]
        source: RawBlockFileBackendError,
    },
}

/// Cache 3.0 local backend placeholder.
///
/// This struct intentionally wires mount/status bootstrap to a backend choice
/// while the full Cache 3.0 data path is implemented in follow-up issues.
#[derive(Debug)]
pub struct CacheV3LocalBlockBackend {
    _layout: CacheV3Layout,
    inner: RawBlockFileBackend,
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

        let inner = RawBlockFileBackend::open(RawBlockFileBackendConfig {
            cache_dir: layout.v3_root.clone(),
            volume_uuid: config.volume_uuid,
        })
        .map_err(
            |source| CacheV3LocalBlockBackendError::OpenCompatibilityRawBackend {
                cache_root: layout.v3_root.clone(),
                source,
            },
        )?;

        Ok(Self {
            _layout: layout,
            inner,
        })
    }
}

impl LocalBlockBackend for CacheV3LocalBlockBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get(&self, object_id: u64) -> Option<Bytes> {
        self.inner.get(object_id)
    }

    fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        self.inner.read_into(object_id, data_offset, out)
    }

    fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
        self.inner.get_for_upload(object_id)
    }

    fn contains(&self, object_id: u64) -> bool {
        self.inner.contains(object_id)
    }

    fn advise_will_need(&self, object_id: u64) {
        self.inner.advise_will_need(object_id);
    }

    fn put(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), LocalBlockBackendError> {
        self.inner.put(object_id, data, evictable)
    }

    fn sync_data(&self) -> Result<(), LocalBlockBackendError> {
        self.inner.sync_data()
    }

    fn run_pressure_maintenance(&self) -> Result<(), LocalBlockBackendError> {
        self.inner.run_pressure_maintenance()
    }

    fn mark_uploading(&self, object_id: u64) {
        self.inner.mark_uploading(object_id);
    }

    fn mark_upload_failed(&self, object_id: u64) {
        self.inner.mark_upload_failed(object_id);
    }

    fn mark_uploaded(&self, object_id: u64) {
        self.inner.mark_uploaded(object_id);
    }

    fn remove(&self, object_id: u64) {
        self.inner.remove(object_id);
    }

    fn remove_batch(&self, object_ids: &[u64]) {
        self.inner.remove_batch(object_ids);
    }

    fn dirty_object_ids(&self) -> Vec<u64> {
        self.inner.dirty_object_ids()
    }

    fn lifecycle_state(&self, object_id: u64) -> LocalBlockLifecycleState {
        self.inner.lifecycle_state(object_id)
    }

    fn is_evictable(&self, object_id: u64) -> bool {
        self.inner.is_evictable(object_id)
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
    fn cache_v3_backend_bootstraps_via_compatibility_raw_backend() {
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
}

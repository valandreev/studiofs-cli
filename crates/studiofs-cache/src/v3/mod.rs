//! Cache 3.0 contract and rollout guardrails.
//!
//! This module defines:
//! - the non-mutating detection contract for legacy vs v3 cache layouts;
//! - rollout guardrails that block making Cache 3.0 the default backend
//!   until final validation is complete.

use std::fmt;
use std::path::{Path, PathBuf};

pub mod layout;

pub use layout::{
    CACHE_V3_LAYOUT_MARKER_FILE, CACHE_V3_LAYOUT_VERSION, CACHE_V3_PAYLOAD_SHARD_COUNT,
    CACHE_V3_REDB_INDEX_FILE, CACHE_V3_SUBTREE, CacheV3Layout,
};

const LEGACY_RAW_DATA_FILE_NAME: &str = "blocks.raw";

/// Final validation issue that must pass before Cache 3.0 is allowed as default.
pub const CACHE_V3_FINAL_VALIDATION_ISSUE: &str = "SFS-TBD";
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
pub fn detect_cache_backends(cache_root: &Path) -> CacheBackendDetection {
    let layout = CacheV3Layout::from_cache_root(cache_root);
    let legacy_raw_data_path = cache_root.join(LEGACY_RAW_DATA_FILE_NAME);
    let v3_layout_marker_exists = layout.layout_marker.exists();
    CacheBackendDetection {
        layout,
        legacy_raw_data_exists: legacy_raw_data_path.exists(),
        v3_layout_marker_exists,
        legacy_raw_data_path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_v3_marker(layout: &CacheV3Layout) {
        std::fs::create_dir_all(&layout.v3_root).unwrap();
        std::fs::write(&layout.layout_marker, b"v1").unwrap();
    }

    #[test]
    fn empty_root_defaults_to_legacy_contract() {
        let temp = tempfile::tempdir().unwrap();
        let detection = detect_cache_backends(temp.path());

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

        let detection = detect_cache_backends(temp.path());
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

        let detection = detect_cache_backends(temp.path());
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

        let detection = detect_cache_backends(temp.path());
        assert!(detection.has_dual_layout());
        assert_eq!(
            detection.selected_mount_backend(),
            CacheBackendKind::LegacyRawV2
        );
    }
}

//! StudioFS Cache Layer.
//!
//! `v2::RawBlockFileBackend` is the active local block backend.
//! `block_cache` remains as a compatibility shim for upload-queue and
//! pending-upload tracking at the write-path boundary.
//! `v3` provides contract detection plus the file-per-block local backend.

use std::path::Path;
use std::sync::Arc;

pub mod block_cache;
pub mod metrics;
pub mod upload_queue;

// --- Cache v2: single-file local block backend ---
pub mod v2;
// --- Cache v3: contract detection + file-per-block backend ---
pub mod v3;

// --- Re-exports ---
pub use block_cache::{BlockCache, BlockCacheConfig, L2IoPriority};
pub use upload_queue::PendingUpload;

/// Local backend selection mode for mount-time contract resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalBackendSelectionMode {
    /// Resolve backend from on-disk contract detection.
    Auto,
    /// Force legacy raw backend even when cache-v3 contract is present.
    ForceLegacyRawV2,
    /// Force cache-v3 backend even when legacy raw data is present.
    ForceCacheV3,
}

/// Open result for the selected local backend implementation.
pub struct OpenedLocalBlockBackend {
    pub detection: v3::CacheBackendDetection,
    pub backend_kind: v3::CacheBackendKind,
    pub backend: Arc<dyn v2::LocalBlockBackend>,
}

/// Errors surfaced while selecting and opening the local backend.
#[derive(Debug, thiserror::Error)]
pub enum OpenLocalBlockBackendError {
    #[error("failed to inspect cache backend contract at '{cache_root}': {source}")]
    DetectCacheBackend {
        cache_root: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open legacy raw backend at '{cache_root}': {source}")]
    OpenLegacyRawBackend {
        cache_root: String,
        #[source]
        source: v2::RawBlockFileBackendError,
    },
    #[error("failed to open cache-v3 backend at '{cache_root}': {source}")]
    OpenCacheV3Backend {
        cache_root: String,
        #[source]
        source: v3::CacheV3LocalBlockBackendError,
    },
}

fn open_legacy_backend(
    cache_root: &Path,
    cache_root_display: &str,
    volume_uuid: &str,
) -> Result<Arc<dyn v2::LocalBlockBackend>, OpenLocalBlockBackendError> {
    v2::open_legacy_local_block_backend(cache_root, volume_uuid)
        .map(|backend| Arc::new(backend) as Arc<dyn v2::LocalBlockBackend>)
        .map_err(|source| OpenLocalBlockBackendError::OpenLegacyRawBackend {
            cache_root: cache_root_display.to_owned(),
            source,
        })
}

fn open_cache_v3_backend(
    cache_root: &Path,
    cache_root_display: &str,
    volume_uuid: &str,
) -> Result<Arc<dyn v2::LocalBlockBackend>, OpenLocalBlockBackendError> {
    open_cache_v3_backend_with_budget(cache_root, cache_root_display, volume_uuid, None)
}

fn open_cache_v3_backend_with_budget(
    cache_root: &Path,
    cache_root_display: &str,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
) -> Result<Arc<dyn v2::LocalBlockBackend>, OpenLocalBlockBackendError> {
    v3::CacheV3LocalBlockBackend::open_with_budget(
        v3::CacheV3LocalBlockBackendConfig {
            cache_root: cache_root.to_path_buf(),
            volume_uuid: v2::derive_local_backend_volume_uuid(volume_uuid),
        },
        cache_budget_bytes,
    )
    .map(|backend| Arc::new(backend) as Arc<dyn v2::LocalBlockBackend>)
    .map_err(|source| OpenLocalBlockBackendError::OpenCacheV3Backend {
        cache_root: cache_root_display.to_owned(),
        source,
    })
}

/// Open the currently selected local backend for a cache root.
///
/// Selection is derived from on-disk contract detection:
/// - legacy raw remains selected during rollout when both layouts exist;
/// - cache-v3 is selected only when provisioned in isolation.
pub fn open_local_block_backend(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    open_local_block_backend_with_mode(cache_root, volume_uuid, LocalBackendSelectionMode::Auto)
}

/// Open the currently selected local backend using an explicit selection mode.
pub fn open_local_block_backend_with_mode(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    selection_mode: LocalBackendSelectionMode,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    let cache_root = cache_root.as_ref();
    let cache_root_display = cache_root.display().to_string();

    let detection = v3::detect_cache_backends(cache_root).map_err(|source| {
        OpenLocalBlockBackendError::DetectCacheBackend {
            cache_root: cache_root_display.clone(),
            source,
        }
    })?;
    let backend_kind = match selection_mode {
        LocalBackendSelectionMode::Auto => detection.selected_mount_backend(),
        LocalBackendSelectionMode::ForceLegacyRawV2 => v3::CacheBackendKind::LegacyRawV2,
        LocalBackendSelectionMode::ForceCacheV3 => v3::CacheBackendKind::CacheV3,
    };
    let backend: Arc<dyn v2::LocalBlockBackend> = match backend_kind {
        v3::CacheBackendKind::LegacyRawV2 => {
            open_legacy_backend(cache_root, &cache_root_display, volume_uuid)?
        }
        v3::CacheBackendKind::CacheV3 => {
            open_cache_v3_backend(cache_root, &cache_root_display, volume_uuid)?
        }
    };

    Ok(OpenedLocalBlockBackend {
        detection,
        backend_kind,
        backend,
    })
}

/// Open the selected local backend with an explicit Cache 3.0 budget.
///
/// The budget is applied only when Cache 3.0 is selected by on-disk contract.
pub fn open_local_block_backend_with_budget(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    open_local_block_backend_with_budget_and_mode(
        cache_root,
        volume_uuid,
        cache_budget_bytes,
        LocalBackendSelectionMode::Auto,
    )
}

/// Open the selected local backend with an explicit Cache 3.0 budget and
/// explicit backend selection mode.
pub fn open_local_block_backend_with_budget_and_mode(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    selection_mode: LocalBackendSelectionMode,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    let cache_root = cache_root.as_ref();
    let cache_root_display = cache_root.display().to_string();

    let detection = v3::detect_cache_backends(cache_root).map_err(|source| {
        OpenLocalBlockBackendError::DetectCacheBackend {
            cache_root: cache_root_display.clone(),
            source,
        }
    })?;
    let backend_kind = match selection_mode {
        LocalBackendSelectionMode::Auto => detection.selected_mount_backend(),
        LocalBackendSelectionMode::ForceLegacyRawV2 => v3::CacheBackendKind::LegacyRawV2,
        LocalBackendSelectionMode::ForceCacheV3 => v3::CacheBackendKind::CacheV3,
    };
    let backend: Arc<dyn v2::LocalBlockBackend> = match backend_kind {
        v3::CacheBackendKind::LegacyRawV2 => {
            open_legacy_backend(cache_root, &cache_root_display, volume_uuid)?
        }
        v3::CacheBackendKind::CacheV3 => open_cache_v3_backend_with_budget(
            cache_root,
            &cache_root_display,
            volume_uuid,
            Some(cache_budget_bytes),
        )?,
    };

    Ok(OpenedLocalBlockBackend {
        detection,
        backend_kind,
        backend,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v3::{CacheBackendKind, CacheV3Layout};

    #[test]
    fn force_cache3_selects_v3_backend_on_dual_layout() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cache_root = temp.path();
        std::fs::write(cache_root.join("blocks.raw"), b"legacy").expect("write legacy raw");
        let layout = CacheV3Layout::from_cache_root(cache_root);
        std::fs::create_dir_all(&layout.v3_root).expect("create v3 root");
        std::fs::write(&layout.layout_marker, b"v1").expect("create marker");

        let opened = open_local_block_backend_with_mode(
            cache_root,
            "00000000-0000-0000-0000-000000000001",
            LocalBackendSelectionMode::ForceCacheV3,
        )
        .expect("open forced cache-v3 backend");

        assert_eq!(opened.backend_kind, CacheBackendKind::CacheV3);
    }

    #[test]
    fn force_legacy_selects_raw_backend_on_v3_only_layout() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cache_root = temp.path();
        let layout = CacheV3Layout::from_cache_root(cache_root);
        std::fs::create_dir_all(&layout.v3_root).expect("create v3 root");
        std::fs::write(&layout.layout_marker, b"v1").expect("create marker");

        let opened = open_local_block_backend_with_mode(
            cache_root,
            "00000000-0000-0000-0000-000000000001",
            LocalBackendSelectionMode::ForceLegacyRawV2,
        )
        .expect("open forced legacy backend");

        assert_eq!(opened.backend_kind, CacheBackendKind::LegacyRawV2);
    }
}

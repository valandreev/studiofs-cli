//! StudioFS Cache Layer.
//!
//! `v2::RawBlockFileBackend` is the active local block backend.
//! `block_cache` remains as a compatibility shim for upload-queue and
//! pending-upload tracking at the write-path boundary.
//! `v3` provides contract detection plus a bootstrap-only placeholder backend.

use std::path::Path;
use std::sync::Arc;

pub mod block_cache;
pub mod metrics;
pub mod upload_queue;

// --- Cache v2: single-file local block backend ---
pub mod v2;
// --- Cache v3: contract detection + bootstrap placeholder backend ---
pub mod v3;

// --- Re-exports ---
pub use block_cache::{BlockCache, BlockCacheConfig, L2IoPriority};
pub use upload_queue::PendingUpload;

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
    v3::CacheV3LocalBlockBackend::open(v3::CacheV3LocalBlockBackendConfig {
        cache_root: cache_root.to_path_buf(),
        volume_uuid: v2::derive_local_backend_volume_uuid(volume_uuid),
    })
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
    let cache_root = cache_root.as_ref();
    let cache_root_display = cache_root.display().to_string();

    let detection = v3::detect_cache_backends(cache_root).map_err(|source| {
        OpenLocalBlockBackendError::DetectCacheBackend {
            cache_root: cache_root_display.clone(),
            source,
        }
    })?;
    let backend_kind = detection.selected_mount_backend();
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

//! StudioFS Cache Layer.
//!
//! `block_cache` provides upload-queue and pending-upload tracking at the
//! write-path boundary.
//! `v3` provides contract detection plus the file-per-block local backend.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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

const CACHE_V3_OPEN_LOCK_RETRY_ATTEMPTS: usize = 40;
const CACHE_V3_OPEN_LOCK_RETRY_DELAY: Duration = Duration::from_millis(25);

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
    #[error("failed to provision cache-v3 layout under '{cache_root}': {source}")]
    ProvisionCacheV3Layout {
        cache_root: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open cache-v3 backend at '{cache_root}': {source}")]
    OpenCacheV3Backend {
        cache_root: String,
        #[source]
        source: Box<v3::CacheV3LocalBlockBackendError>,
    },
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
    let config = v3::CacheV3LocalBlockBackendConfig {
        cache_root: cache_root.to_path_buf(),
        volume_uuid: v2::derive_local_backend_volume_uuid(volume_uuid),
    };

    for attempt in 0..=CACHE_V3_OPEN_LOCK_RETRY_ATTEMPTS {
        match v3::CacheV3LocalBlockBackend::open_with_budget(config.clone(), cache_budget_bytes) {
            Ok(backend) => return Ok(Arc::new(backend) as Arc<dyn v2::LocalBlockBackend>),
            Err(source)
                if source.is_transient_open_lock_conflict()
                    && attempt < CACHE_V3_OPEN_LOCK_RETRY_ATTEMPTS =>
            {
                std::thread::sleep(CACHE_V3_OPEN_LOCK_RETRY_DELAY);
            }
            Err(source) => {
                return Err(OpenLocalBlockBackendError::OpenCacheV3Backend {
                    cache_root: cache_root_display.to_owned(),
                    source: Box::new(source),
                });
            }
        }
    }

    unreachable!("retry loop must return or error before exhausting attempts")
}

fn provision_cache_v3_layout(layout: &v3::CacheV3Layout) -> std::io::Result<()> {
    std::fs::create_dir_all(&layout.tmp_dir)?;
    std::fs::create_dir_all(&layout.payload_blocks_dir)?;
    std::fs::create_dir_all(&layout.index_dir)?;
    if !layout.layout_marker.exists() {
        std::fs::write(&layout.layout_marker, b"v1")?;
    }
    Ok(())
}

fn cache_v3_layout_needs_provisioning(
    detection: &v3::CacheBackendDetection,
) -> Result<bool, std::io::Error> {
    Ok(!detection.v3_layout_marker_exists
        || !detection.v3_payload_blocks_dir_exists
        || !detection.layout.tmp_dir.try_exists()?
        || !detection.layout.index_dir.try_exists()?)
}

/// Open the Cache 3.0 local backend for a cache root.
pub fn open_local_block_backend(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    open_local_block_backend_with_budget_impl(cache_root.as_ref(), volume_uuid, None)
}

/// Open the selected local backend with an explicit Cache 3.0 budget.
pub fn open_local_block_backend_with_budget(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    open_local_block_backend_with_budget_impl(
        cache_root.as_ref(),
        volume_uuid,
        Some(cache_budget_bytes),
    )
}

fn open_local_block_backend_with_budget_impl(
    cache_root: &Path,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    let cache_root_display = cache_root.display().to_string();

    let mut detection = v3::detect_cache_backends(cache_root).map_err(|source| {
        OpenLocalBlockBackendError::DetectCacheBackend {
            cache_root: cache_root_display.clone(),
            source,
        }
    })?;
    if cache_v3_layout_needs_provisioning(&detection).map_err(|source| {
        OpenLocalBlockBackendError::DetectCacheBackend {
            cache_root: cache_root_display.clone(),
            source,
        }
    })? {
        provision_cache_v3_layout(&detection.layout).map_err(|source| {
            OpenLocalBlockBackendError::ProvisionCacheV3Layout {
                cache_root: detection.layout.v3_root.display().to_string(),
                source,
            }
        })?;
        detection = v3::detect_cache_backends(cache_root).map_err(|source| {
            OpenLocalBlockBackendError::DetectCacheBackend {
                cache_root: cache_root_display.clone(),
                source,
            }
        })?;
    }
    let backend_kind = v3::CacheBackendKind::CacheV3;
    let backend = match cache_budget_bytes {
        Some(cache_budget_bytes) => open_cache_v3_backend_with_budget(
            cache_root,
            &cache_root_display,
            volume_uuid,
            Some(cache_budget_bytes),
        )?,
        None => open_cache_v3_backend(cache_root, &cache_root_display, volume_uuid)?,
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
    fn cache_v3_root_opens_cache_v3_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cache_root = temp.path();
        let layout = CacheV3Layout::from_cache_root(cache_root);
        std::fs::create_dir_all(&layout.v3_root).expect("create v3 root");
        std::fs::write(&layout.layout_marker, b"v1").expect("create marker");

        let opened = open_local_block_backend(cache_root, "00000000-0000-0000-0000-000000000001")
            .expect("open cache-v3 backend");

        assert_eq!(opened.backend_kind, CacheBackendKind::CacheV3);
    }
}

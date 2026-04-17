//! StudioFS Cache Layer.
//!
//! `cached_store` provides the unified local cache + staged-writeback runtime
//! used by the production mount path.
//! `benchmark_cache` provides the lightweight in-memory benchmark surface used
//! by `studiofs test-cache`.
//! `v3` provides contract detection plus the file-per-block local backend.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub mod benchmark_cache;
pub mod cached_store;
pub mod local;
pub mod metrics;
pub mod upload_queue;
// --- Cache v3: contract detection + file-per-block backend ---
pub mod v3;

// --- Re-exports ---
pub use benchmark_cache::{
    BenchmarkCache, BenchmarkCacheConfig, BenchmarkCacheStats, CacheReadLevel, CacheReadTrace,
    DEFAULT_MEMORY_CACHE_SIZE,
};
pub use cached_store::{CachedStore, CachedStoreConfig};
pub use upload_queue::PendingUpload;

const CACHE_V3_OPEN_LOCK_RETRY_ATTEMPTS: usize = 40;
const CACHE_V3_OPEN_LOCK_RETRY_DELAY: Duration = Duration::from_millis(25);

/// Open result for the selected local backend implementation.
pub struct OpenedLocalBlockBackend {
    pub detection: v3::CacheBackendDetection,
    pub backend_kind: v3::CacheBackendKind,
    pub backend: Arc<dyn local::LocalBlockBackend>,
}

/// Prepared cache-v3 runtime state for a per-volume cache root.
#[derive(Debug, Clone)]
pub struct PreparedCacheV3Runtime {
    pub layout: v3::CacheV3Layout,
    pub detection: v3::CacheBackendDetection,
}

/// Errors surfaced while provisioning cache-v3 runtime state.
#[derive(Debug, thiserror::Error)]
pub enum PrepareCacheV3RuntimeError {
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

/// Ensure the cache-v3 runtime contract exists for a per-volume cache root.
pub fn prepare_cache_v3_runtime(
    cache_root: impl AsRef<Path>,
) -> Result<PreparedCacheV3Runtime, PrepareCacheV3RuntimeError> {
    let cache_root = cache_root.as_ref();
    let cache_root_display = cache_root.display().to_string();

    let mut detection = v3::detect_cache_backends(cache_root).map_err(|source| {
        PrepareCacheV3RuntimeError::DetectCacheBackend {
            cache_root: cache_root_display.clone(),
            source,
        }
    })?;
    if detection.needs_runtime_provisioning() {
        detection.layout.provision().map_err(|source| {
            PrepareCacheV3RuntimeError::ProvisionCacheV3Layout {
                cache_root: cache_root_display.clone(),
                source,
            }
        })?;
        detection = v3::detect_cache_backends(cache_root).map_err(|source| {
            PrepareCacheV3RuntimeError::DetectCacheBackend {
                cache_root: cache_root_display.clone(),
                source,
            }
        })?;
    }

    Ok(PreparedCacheV3Runtime {
        layout: detection.layout.clone(),
        detection,
    })
}

fn open_detected_local_block_backend_budgeted(
    detection: v3::CacheBackendDetection,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
    payload_sync_mode: v3::CacheV3PayloadSyncMode,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    let cache_root = detection.layout.cache_root.as_path();
    let cache_root_display = cache_root.display().to_string();
    let backend_kind = v3::CacheBackendKind::CacheV3;
    let backend = open_cache_v3_backend_budgeted(
        cache_root,
        &cache_root_display,
        volume_uuid,
        cache_budget_bytes,
        payload_sync_mode,
    )?;

    Ok(OpenedLocalBlockBackend {
        detection,
        backend_kind,
        backend,
    })
}

fn open_cache_v3_backend(
    cache_root: &Path,
    cache_root_display: &str,
    volume_uuid: &str,
) -> Result<Arc<dyn local::LocalBlockBackend>, OpenLocalBlockBackendError> {
    open_cache_v3_backend_budgeted(
        cache_root,
        cache_root_display,
        volume_uuid,
        None,
        v3::CacheV3PayloadSyncMode::Durable,
    )
}

fn open_cache_v3_backend_budgeted(
    cache_root: &Path,
    cache_root_display: &str,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
    payload_sync_mode: v3::CacheV3PayloadSyncMode,
) -> Result<Arc<dyn local::LocalBlockBackend>, OpenLocalBlockBackendError> {
    let volume_uuid = local::derive_backend_volume_uuid(volume_uuid);
    let mut last_conflict: Option<Box<v3::CacheV3LocalBlockBackendError>> = None;

    for attempt in 0..=CACHE_V3_OPEN_LOCK_RETRY_ATTEMPTS {
        let config = v3::CacheV3LocalBlockBackendConfig {
            cache_root: cache_root.to_path_buf(),
            volume_uuid,
        };
        match v3::CacheV3LocalBlockBackend::open_with_budget_and_sync_mode(
            config,
            cache_budget_bytes,
            payload_sync_mode,
        ) {
            Ok(backend) => return Ok(Arc::new(backend) as Arc<dyn local::LocalBlockBackend>),
            Err(source)
                if source.is_transient_open_lock_conflict()
                    && attempt < CACHE_V3_OPEN_LOCK_RETRY_ATTEMPTS =>
            {
                last_conflict = Some(Box::new(source));
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

    let source =
        last_conflict.expect("cache-v3 open loop must capture the final transient conflict");
    Err(OpenLocalBlockBackendError::OpenCacheV3Backend {
        cache_root: cache_root_display.to_owned(),
        source,
    })
}

/// Open the current local cache backend for a cache root.
pub fn open_local_block_backend(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    open_local_block_backend_budgeted(
        cache_root.as_ref(),
        volume_uuid,
        None,
        v3::CacheV3PayloadSyncMode::Durable,
    )
}

/// Open the selected local backend with an explicit local-cache budget.
pub fn open_local_block_backend_with_budget(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    open_local_block_backend_with_budget_and_sync_mode(
        cache_root,
        volume_uuid,
        cache_budget_bytes,
        v3::CacheV3PayloadSyncMode::Durable,
    )
}

/// Open the selected local backend with an explicit local-cache budget and
/// payload-store sync policy.
pub fn open_local_block_backend_with_budget_and_sync_mode(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    payload_sync_mode: v3::CacheV3PayloadSyncMode,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    open_local_block_backend_budgeted(
        cache_root.as_ref(),
        volume_uuid,
        Some(cache_budget_bytes),
        payload_sync_mode,
    )
}

/// Open the selected local backend using an already prepared local-cache runtime.
pub fn open_prepared_local_block_backend_with_budget_and_sync_mode(
    prepared: PreparedCacheV3Runtime,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    payload_sync_mode: v3::CacheV3PayloadSyncMode,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    open_detected_local_block_backend_budgeted(
        prepared.detection,
        volume_uuid,
        Some(cache_budget_bytes),
        payload_sync_mode,
    )
}

fn open_local_block_backend_budgeted(
    cache_root: &Path,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
    payload_sync_mode: v3::CacheV3PayloadSyncMode,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    let cache_root_display = cache_root.display().to_string();

    let detection = prepare_cache_v3_runtime(cache_root)
        .map_err(|source| match source {
            PrepareCacheV3RuntimeError::DetectCacheBackend { cache_root, source } => {
                OpenLocalBlockBackendError::DetectCacheBackend { cache_root, source }
            }
            PrepareCacheV3RuntimeError::ProvisionCacheV3Layout { cache_root, source } => {
                OpenLocalBlockBackendError::ProvisionCacheV3Layout { cache_root, source }
            }
        })?
        .detection;
    match cache_budget_bytes {
        Some(cache_budget_bytes) => open_detected_local_block_backend_budgeted(
            detection,
            volume_uuid,
            Some(cache_budget_bytes),
            payload_sync_mode,
        ),
        None => {
            let backend = open_cache_v3_backend(cache_root, &cache_root_display, volume_uuid)?;
            Ok(OpenedLocalBlockBackend {
                detection,
                backend_kind: v3::CacheBackendKind::CacheV3,
                backend,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v3::{CacheBackendKind, CacheV3Layout};

    #[test]
    fn prepare_cache_v3_runtime_creates_contract_paths() {
        let temp = tempfile::tempdir().expect("tempdir");

        let prepared = prepare_cache_v3_runtime(temp.path()).expect("prepare cache-v3 runtime");

        assert_eq!(prepared.layout, CacheV3Layout::from_cache_root(temp.path()));
        assert!(prepared.detection.has_cache_v3_contract());
        assert!(prepared.layout.layout_marker.exists());
        assert!(prepared.layout.tmp_dir.exists());
        assert!(prepared.layout.payload_blocks_dir.exists());
        assert!(prepared.layout.index_dir.exists());
    }

    #[test]
    fn prepare_cache_v3_runtime_is_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");

        let first = prepare_cache_v3_runtime(temp.path()).expect("first prepare");
        let second = prepare_cache_v3_runtime(temp.path()).expect("second prepare");

        assert_eq!(first.layout, second.layout);
        assert_eq!(first.detection, second.detection);
        assert!(second.detection.has_cache_v3_contract());
    }

    #[test]
    fn prepared_cache_v3_runtime_opens_cache_v3_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let prepared = prepare_cache_v3_runtime(temp.path()).expect("prepare cache-v3 runtime");

        let opened = open_local_block_backend(
            &prepared.layout.cache_root,
            "00000000-0000-0000-0000-000000000001",
        )
        .expect("open cache-v3 backend");

        assert_eq!(opened.backend_kind, CacheBackendKind::CacheV3);
        assert!(opened.detection.has_cache_v3_contract());
    }

    #[test]
    fn prepared_cache_v3_runtime_reuses_detection_to_open_budgeted_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let prepared = prepare_cache_v3_runtime(temp.path()).expect("prepare cache-v3 runtime");

        let opened = open_prepared_local_block_backend_with_budget_and_sync_mode(
            prepared.clone(),
            "00000000-0000-0000-0000-000000000001",
            64 * 1024 * 1024,
            v3::CacheV3PayloadSyncMode::Durable,
        )
        .expect("open prepared cache-v3 backend");

        assert_eq!(opened.backend_kind, CacheBackendKind::CacheV3);
        assert_eq!(opened.detection, prepared.detection);
    }

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

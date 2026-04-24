//! StudioFS Cache Layer.
//!
//! `cached_store` provides the unified local cache + staged-writeback runtime
//! used by the production mount path.
//! `benchmark_cache` provides the lightweight in-memory benchmark surface used
//! by `studiofs test-cache`.
//! `runtime` provides the production file-per-block local backend.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub mod benchmark_cache;
pub mod cached_store;
pub mod local;
pub mod metrics;
pub mod runtime;
pub mod upload_queue;
pub mod upload_stage;

// --- Re-exports ---
pub use benchmark_cache::{
    BenchmarkCache, BenchmarkCacheConfig, BenchmarkCacheStats, CacheReadLevel, CacheReadTrace,
    DEFAULT_MEMORY_CACHE_SIZE,
};
pub use cached_store::{CachedStore, CachedStoreConfig};
pub use runtime::{
    LocalBlockBackendConfig as PersistentLocalBlockBackendConfig, LocalBlockBackendKind,
    LocalBlockBackendOpenError as PersistentLocalBlockBackendError,
    LocalBlockBackendRuntime as PersistentLocalBlockBackend, LocalBlockLayout,
    LocalBlockPayloadSyncMode, LocalBlockRuntimeDetection,
};
pub use upload_queue::PendingUpload;

const LOCAL_BLOCK_OPEN_LOCK_RETRY_ATTEMPTS: usize = 40;
const LOCAL_BLOCK_OPEN_LOCK_RETRY_DELAY: Duration = Duration::from_millis(25);
const RETIRED_LOCAL_BLOCK_SUBTREE: &str = "v3";
const RETIRED_LOCAL_BLOCK_LEGACY_UPLOAD_DIR: &str = "upload-stage";

fn is_recognized_retired_runtime_root(path: &Path, metadata: &std::fs::Metadata) -> bool {
    let file_type = metadata.file_type();
    if !file_type.is_dir() || file_type.is_symlink() {
        return false;
    }

    let layout = runtime::LocalBlockLayout::from_cache_root(path);
    let legacy_upload_dir = path.join(RETIRED_LOCAL_BLOCK_LEGACY_UPLOAD_DIR);
    let recognized_entries = [
        layout.tmp_dir.as_path(),
        layout.payload_root.as_path(),
        layout.upload_stage_root.as_path(),
        legacy_upload_dir.as_path(),
        layout.index_dir.as_path(),
    ];
    let recognized_count = recognized_entries
        .into_iter()
        .filter(|entry| entry.exists())
        .count();

    recognized_count >= 2 || layout.redb_index_path.exists()
}

/// Open result for the selected local backend implementation.
pub struct OpenedLocalBlockBackend {
    pub detection: runtime::LocalBlockRuntimeDetection,
    pub backend_kind: runtime::LocalBlockBackendKind,
    pub backend: Arc<dyn local::LocalBlockBackend>,
}

/// Prepared local block runtime state for a per-volume cache root.
#[derive(Debug, Clone)]
pub struct PreparedLocalBlockRuntime {
    pub layout: runtime::LocalBlockLayout,
    pub detection: runtime::LocalBlockRuntimeDetection,
}

/// Errors surfaced while provisioning the local block runtime state.
#[derive(Debug, thiserror::Error)]
pub enum PrepareLocalBlockRuntimeError {
    #[error("failed to inspect cache backend contract at '{cache_root}': {source}")]
    DetectCacheBackend {
        cache_root: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to provision local block runtime layout under '{cache_root}': {source}")]
    ProvisionLocalBlockLayout {
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
    #[error("failed to provision local block runtime layout under '{cache_root}': {source}")]
    ProvisionLocalBlockLayout {
        cache_root: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to open local block backend at '{cache_root}': {source}")]
    OpenLocalBlockBackend {
        cache_root: String,
        #[source]
        source: Box<runtime::LocalBlockBackendOpenError>,
    },
}

/// Ensure the local block runtime contract exists for a per-volume cache root.
pub fn prepare_local_block_runtime(
    cache_root: impl AsRef<Path>,
) -> Result<PreparedLocalBlockRuntime, PrepareLocalBlockRuntimeError> {
    let cache_root = cache_root.as_ref();
    let cache_root_display = cache_root.display().to_string();
    let retired_runtime_root = cache_root.join(RETIRED_LOCAL_BLOCK_SUBTREE);

    if let Ok(metadata) = std::fs::symlink_metadata(&retired_runtime_root) {
        if is_recognized_retired_runtime_root(&retired_runtime_root, &metadata) {
            std::fs::remove_dir_all(&retired_runtime_root).map_err(|source| {
                PrepareLocalBlockRuntimeError::ProvisionLocalBlockLayout {
                    cache_root: cache_root_display.clone(),
                    source,
                }
            })?;
        }
    }

    let mut detection = runtime::detect_cache_backends(cache_root).map_err(|source| {
        PrepareLocalBlockRuntimeError::DetectCacheBackend {
            cache_root: cache_root_display.clone(),
            source,
        }
    })?;
    if detection.needs_runtime_provisioning() {
        detection.layout.provision().map_err(|source| {
            PrepareLocalBlockRuntimeError::ProvisionLocalBlockLayout {
                cache_root: cache_root_display.clone(),
                source,
            }
        })?;
        detection = runtime::detect_cache_backends(cache_root).map_err(|source| {
            PrepareLocalBlockRuntimeError::DetectCacheBackend {
                cache_root: cache_root_display.clone(),
                source,
            }
        })?;
    }

    Ok(PreparedLocalBlockRuntime {
        layout: detection.layout.clone(),
        detection,
    })
}

fn open_detected_local_block_backend_budgeted(
    detection: runtime::LocalBlockRuntimeDetection,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
    payload_sync_mode: runtime::LocalBlockPayloadSyncMode,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    let cache_root = detection.layout.cache_root.as_path();
    let cache_root_display = cache_root.display().to_string();
    let backend_kind = runtime::LocalBlockBackendKind::Current;
    let backend = open_current_local_block_backend_budgeted(
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

fn open_current_local_block_backend(
    cache_root: &Path,
    cache_root_display: &str,
    volume_uuid: &str,
) -> Result<Arc<dyn local::LocalBlockBackend>, OpenLocalBlockBackendError> {
    open_current_local_block_backend_budgeted(
        cache_root,
        cache_root_display,
        volume_uuid,
        None,
        runtime::LocalBlockPayloadSyncMode::Durable,
    )
}

fn open_current_local_block_backend_budgeted(
    cache_root: &Path,
    cache_root_display: &str,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
    payload_sync_mode: runtime::LocalBlockPayloadSyncMode,
) -> Result<Arc<dyn local::LocalBlockBackend>, OpenLocalBlockBackendError> {
    let volume_uuid = local::derive_backend_volume_uuid(volume_uuid);
    let mut last_conflict: Option<Box<runtime::LocalBlockBackendOpenError>> = None;

    for attempt in 0..=LOCAL_BLOCK_OPEN_LOCK_RETRY_ATTEMPTS {
        let config = runtime::LocalBlockBackendConfig {
            cache_root: cache_root.to_path_buf(),
            volume_uuid,
        };
        match runtime::LocalBlockBackendRuntime::open_with_budget_and_sync_mode(
            config,
            cache_budget_bytes,
            payload_sync_mode,
        ) {
            Ok(backend) => return Ok(Arc::new(backend) as Arc<dyn local::LocalBlockBackend>),
            Err(source)
                if source.is_transient_open_lock_conflict()
                    && attempt < LOCAL_BLOCK_OPEN_LOCK_RETRY_ATTEMPTS =>
            {
                last_conflict = Some(Box::new(source));
                std::thread::sleep(LOCAL_BLOCK_OPEN_LOCK_RETRY_DELAY);
            }
            Err(source) => {
                return Err(OpenLocalBlockBackendError::OpenLocalBlockBackend {
                    cache_root: cache_root_display.to_owned(),
                    source: Box::new(source),
                });
            }
        }
    }

    let source =
        last_conflict.expect("local block open loop must capture the final transient conflict");
    Err(OpenLocalBlockBackendError::OpenLocalBlockBackend {
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
        runtime::LocalBlockPayloadSyncMode::Durable,
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
        runtime::LocalBlockPayloadSyncMode::Durable,
    )
}

/// Open the selected local backend with an explicit local-cache budget and
/// payload-store sync policy.
pub fn open_local_block_backend_with_budget_and_sync_mode(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    payload_sync_mode: runtime::LocalBlockPayloadSyncMode,
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
    prepared: PreparedLocalBlockRuntime,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    payload_sync_mode: runtime::LocalBlockPayloadSyncMode,
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
    payload_sync_mode: runtime::LocalBlockPayloadSyncMode,
) -> Result<OpenedLocalBlockBackend, OpenLocalBlockBackendError> {
    let cache_root_display = cache_root.display().to_string();

    let detection = prepare_local_block_runtime(cache_root)
        .map_err(|source| match source {
            PrepareLocalBlockRuntimeError::DetectCacheBackend { cache_root, source } => {
                OpenLocalBlockBackendError::DetectCacheBackend { cache_root, source }
            }
            PrepareLocalBlockRuntimeError::ProvisionLocalBlockLayout { cache_root, source } => {
                OpenLocalBlockBackendError::ProvisionLocalBlockLayout { cache_root, source }
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
            let backend =
                open_current_local_block_backend(cache_root, &cache_root_display, volume_uuid)?;
            Ok(OpenedLocalBlockBackend {
                detection,
                backend_kind: runtime::LocalBlockBackendKind::Current,
                backend,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{LocalBlockBackendKind, LocalBlockLayout, LocalBlockPayloadSyncMode};

    #[test]
    fn prepare_local_block_runtime_creates_contract_paths() {
        let temp = tempfile::tempdir().expect("tempdir");

        let prepared =
            prepare_local_block_runtime(temp.path()).expect("prepare local block runtime");

        assert_eq!(
            prepared.layout,
            LocalBlockLayout::from_cache_root(temp.path())
        );
        assert!(prepared.detection.has_runtime_layout());
        assert!(prepared.layout.tmp_dir.exists());
        assert!(prepared.layout.payload_objects_dir.exists());
        assert!(prepared.layout.upload_stage_objects_dir.exists());
        assert!(prepared.layout.index_dir.exists());
    }

    #[test]
    fn prepare_local_block_runtime_is_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");

        let first = prepare_local_block_runtime(temp.path()).expect("first prepare");
        let second = prepare_local_block_runtime(temp.path()).expect("second prepare");

        assert_eq!(first.layout, second.layout);
        assert_eq!(first.detection, second.detection);
        assert!(second.detection.has_runtime_layout());
    }

    #[test]
    fn prepare_local_block_runtime_removes_retired_v3_subtree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let retired_root = temp.path().join(RETIRED_LOCAL_BLOCK_SUBTREE);
        std::fs::create_dir_all(retired_root.join("payload")).expect("create retired payload");
        std::fs::create_dir_all(retired_root.join("index")).expect("create retired index");
        std::fs::write(retired_root.join("stale.txt"), b"stale").expect("write stale file");

        let prepared =
            prepare_local_block_runtime(temp.path()).expect("prepare local block runtime");

        assert!(prepared.detection.has_runtime_layout());
        assert!(!retired_root.exists());
        assert!(prepared.layout.upload_stage_objects_dir.exists());
    }

    #[test]
    fn prepare_local_block_runtime_keeps_unrecognized_v3_entry() {
        let temp = tempfile::tempdir().expect("tempdir");
        let retired_root = temp.path().join(RETIRED_LOCAL_BLOCK_SUBTREE);
        std::fs::create_dir_all(&retired_root).expect("create unrelated v3 dir");
        std::fs::write(retired_root.join("notes.txt"), b"user data").expect("write unrelated file");

        let prepared =
            prepare_local_block_runtime(temp.path()).expect("prepare local block runtime");

        assert!(prepared.detection.has_runtime_layout());
        assert!(
            retired_root.exists(),
            "non-runtime v3 entries should be preserved"
        );
        assert!(retired_root.join("notes.txt").exists());
    }

    #[test]
    fn prepared_local_block_runtime_opens_local_block_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let prepared =
            prepare_local_block_runtime(temp.path()).expect("prepare local block runtime");

        let opened = open_local_block_backend(
            &prepared.layout.cache_root,
            "00000000-0000-0000-0000-000000000001",
        )
        .expect("open local block backend");

        assert_eq!(opened.backend_kind, LocalBlockBackendKind::Current);
        assert!(opened.detection.has_runtime_layout());
    }

    #[test]
    fn prepared_local_block_runtime_reuses_detection_to_open_budgeted_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let prepared =
            prepare_local_block_runtime(temp.path()).expect("prepare local block runtime");

        let opened = open_prepared_local_block_backend_with_budget_and_sync_mode(
            prepared.clone(),
            "00000000-0000-0000-0000-000000000001",
            64 * 1024 * 1024,
            LocalBlockPayloadSyncMode::Durable,
        )
        .expect("open prepared local block backend");

        assert_eq!(opened.backend_kind, LocalBlockBackendKind::Current);
        assert_eq!(opened.detection, prepared.detection);
    }

    #[test]
    fn local_block_runtime_root_opens_local_block_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cache_root = temp.path();
        let layout = LocalBlockLayout::from_cache_root(cache_root);
        layout.provision().expect("provision runtime layout");

        let opened = open_local_block_backend(cache_root, "00000000-0000-0000-0000-000000000001")
            .expect("open local block backend");

        assert_eq!(opened.backend_kind, LocalBlockBackendKind::Current);
    }
}

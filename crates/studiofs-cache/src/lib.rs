//! StudioFS Cache Layer.
//!
//! `cached_store` provides the unified local cache + staged-writeback runtime
//! used by the production mount path.
//! `benchmark_cache` provides the lightweight in-memory benchmark surface used
//! by `studiofs test-cache`.
//! `runtime` provides the production local payload object backend.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Benchmark cache facade used only by local benchmark/test tooling.
pub mod benchmark_cache;
/// Production cache runtime backing VFS mount paths with staged writeback and local payload caching.
pub mod cached_store;
/// Local payload backends and object-store layout helpers.
pub mod local;
/// Runtime statistics and cache pipeline metric snapshots.
pub mod metrics;
/// Local payload runtime detection/provisioning and backend runtime configuration.
pub mod runtime;
/// Upload queue staging APIs and pending upload surface.
pub mod upload_queue;
/// Upload-stage cache layer and local upload backend staging types.
pub mod upload_stage;

// --- Re-exports ---
pub use benchmark_cache::{
    BenchmarkCache, BenchmarkCacheConfig, BenchmarkCacheStats, CacheReadLevel, CacheReadTrace,
    DEFAULT_MEMORY_CACHE_SIZE,
};
pub use cached_store::{
    CachedStore, CachedStoreConfig, CachedStoreWriterConfig, CachedStoreWriterCountersSnapshot,
    CachedStoreWriterError, CachedStoreWriterSlice, DetachedStagedSlice, StagedSlice,
    WriterWriteResult,
};
pub use runtime::{
    LocalPayloadBackendConfig as PersistentLocalPayloadBackendConfig, LocalPayloadBackendKind,
    LocalPayloadBackendOpenError as PersistentLocalPayloadBackendError,
    LocalPayloadBackendRuntime as PersistentLocalPayloadBackend, LocalPayloadLayout,
    LocalPayloadRuntimeDetection, LocalPayloadSyncMode, LocalPayloadWriteMode,
};
pub use upload_queue::PendingUpload;

const LOCAL_PAYLOAD_OPEN_LOCK_RETRY_ATTEMPTS: usize = 40;
const LOCAL_PAYLOAD_OPEN_LOCK_RETRY_DELAY: Duration = Duration::from_millis(25);
const RETIRED_LOCAL_PAYLOAD_SUBTREE: &str = "v3";
const RETIRED_LOCAL_PAYLOAD_LEGACY_UPLOAD_DIR: &str = "upload-stage";

fn is_recognized_retired_runtime_root(path: &Path, metadata: &std::fs::Metadata) -> bool {
    let file_type = metadata.file_type();
    if !file_type.is_dir() || file_type.is_symlink() {
        return false;
    }

    let layout = runtime::LocalPayloadLayout::from_cache_root(path);
    let legacy_upload_dir = path.join(RETIRED_LOCAL_PAYLOAD_LEGACY_UPLOAD_DIR);
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
pub struct OpenedLocalPayloadBackend {
    /// Backend runtime detection result used to re-open or inspect current layout.
    pub detection: runtime::LocalPayloadRuntimeDetection,
    /// Concrete backend family chosen for this cache root.
    pub backend_kind: runtime::LocalPayloadBackendKind,
    /// Trait object for the opened local payload backend.
    pub backend: Arc<dyn local::LocalPayloadBackend>,
}

/// Prepared local payload object runtime state for a per-volume cache root.
#[derive(Debug, Clone)]
pub struct PreparedLocalPayloadRuntime {
    /// Stable cache layout coordinates for metadata, payload and staging paths.
    pub layout: runtime::LocalPayloadLayout,
    /// Runtime detection snapshot used when re-opening backend instances.
    pub detection: runtime::LocalPayloadRuntimeDetection,
}

/// Errors surfaced while provisioning the local payload object runtime state.
#[derive(Debug, thiserror::Error)]
pub enum PrepareLocalPayloadRuntimeError {
    /// Runtime detection failed while inspecting an existing cache root.
    #[error("failed to inspect cache backend contract at '{cache_root}': {source}")]
    DetectCacheBackend {
        /// Cache root path being inspected.
        cache_root: String,
        /// Original I/O error from cache inspection.
        #[source]
        source: std::io::Error,
    },
    /// Cache layout creation failed while provisioning required payload directories.
    #[error(
        "failed to provision local payload object runtime layout under '{cache_root}': {source}"
    )]
    ProvisionLocalPayloadLayout {
        /// Cache root path where provisioning was attempted.
        cache_root: String,
        /// Original I/O error from layout provisioning.
        #[source]
        source: std::io::Error,
    },
}

/// Errors surfaced while selecting and opening the local backend.
#[derive(Debug, thiserror::Error)]
pub enum OpenLocalPayloadBackendError {
    /// Runtime detection failed while selecting backend contract.
    #[error("failed to inspect cache backend contract at '{cache_root}': {source}")]
    DetectCacheBackend {
        /// Cache root path being inspected.
        cache_root: String,
        /// Original I/O error from backend detection.
        #[source]
        source: std::io::Error,
    },
    /// Failed to provision local runtime layout before opening.
    #[error(
        "failed to provision local payload object runtime layout under '{cache_root}': {source}"
    )]
    ProvisionLocalPayloadLayout {
        /// Cache root path where provisioning was attempted.
        cache_root: String,
        /// Original I/O error from layout provisioning.
        #[source]
        source: std::io::Error,
    },
    /// Backend open failed; includes the underlying backend-specific cause.
    #[error("failed to open local payload object backend at '{cache_root}': {source}")]
    OpenLocalPayloadBackend {
        /// Cache root path whose backend could not be opened.
        cache_root: String,
        /// Backend-opening failure from local payload runtime.
        #[source]
        source: Box<runtime::LocalPayloadBackendOpenError>,
    },
}

/// Ensure the local payload object runtime contract exists for a per-volume cache root.
pub fn prepare_local_payload_runtime(
    cache_root: impl AsRef<Path>,
) -> Result<PreparedLocalPayloadRuntime, PrepareLocalPayloadRuntimeError> {
    let cache_root = cache_root.as_ref();
    let cache_root_display = cache_root.display().to_string();
    let retired_runtime_root = cache_root.join(RETIRED_LOCAL_PAYLOAD_SUBTREE);

    if let Ok(metadata) = std::fs::symlink_metadata(&retired_runtime_root)
        && is_recognized_retired_runtime_root(&retired_runtime_root, &metadata)
    {
        std::fs::remove_dir_all(&retired_runtime_root).map_err(|source| {
            PrepareLocalPayloadRuntimeError::ProvisionLocalPayloadLayout {
                cache_root: cache_root_display.clone(),
                source,
            }
        })?;
    }

    let mut detection = runtime::detect_cache_backends(cache_root).map_err(|source| {
        PrepareLocalPayloadRuntimeError::DetectCacheBackend {
            cache_root: cache_root_display.clone(),
            source,
        }
    })?;
    if detection.needs_runtime_provisioning() {
        detection.layout.provision().map_err(|source| {
            PrepareLocalPayloadRuntimeError::ProvisionLocalPayloadLayout {
                cache_root: cache_root_display.clone(),
                source,
            }
        })?;
        detection = runtime::detect_cache_backends(cache_root).map_err(|source| {
            PrepareLocalPayloadRuntimeError::DetectCacheBackend {
                cache_root: cache_root_display.clone(),
                source,
            }
        })?;
    }

    Ok(PreparedLocalPayloadRuntime {
        layout: detection.layout.clone(),
        detection,
    })
}

fn open_detected_local_payload_backend_budgeted(
    detection: runtime::LocalPayloadRuntimeDetection,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
    payload_sync_mode: runtime::LocalPayloadSyncMode,
    payload_write_mode: runtime::LocalPayloadWriteMode,
) -> Result<OpenedLocalPayloadBackend, OpenLocalPayloadBackendError> {
    let cache_root = detection.layout.cache_root.as_path();
    let cache_root_display = cache_root.display().to_string();
    let backend_kind = runtime::LocalPayloadBackendKind::Current;
    let backend = open_current_local_payload_backend_budgeted(
        cache_root,
        &cache_root_display,
        volume_uuid,
        cache_budget_bytes,
        payload_sync_mode,
        payload_write_mode,
    )?;

    Ok(OpenedLocalPayloadBackend {
        detection,
        backend_kind,
        backend,
    })
}

fn open_current_local_payload_backend_budgeted(
    cache_root: &Path,
    cache_root_display: &str,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
    payload_sync_mode: runtime::LocalPayloadSyncMode,
    payload_write_mode: runtime::LocalPayloadWriteMode,
) -> Result<Arc<dyn local::LocalPayloadBackend>, OpenLocalPayloadBackendError> {
    let volume_uuid = local::derive_backend_volume_uuid(volume_uuid);

    for attempt in 0..=LOCAL_PAYLOAD_OPEN_LOCK_RETRY_ATTEMPTS {
        let config = runtime::LocalPayloadBackendConfig {
            cache_root: cache_root.to_path_buf(),
            volume_uuid,
        };
        match runtime::LocalPayloadBackendRuntime::open_with_budget_sync_and_write_mode(
            config,
            cache_budget_bytes,
            payload_sync_mode,
            payload_write_mode,
        ) {
            Ok(backend) => return Ok(Arc::new(backend) as Arc<dyn local::LocalPayloadBackend>),
            Err(source)
                if source.is_transient_open_lock_conflict()
                    && attempt < LOCAL_PAYLOAD_OPEN_LOCK_RETRY_ATTEMPTS =>
            {
                std::thread::sleep(LOCAL_PAYLOAD_OPEN_LOCK_RETRY_DELAY);
            }
            Err(source) => {
                return Err(OpenLocalPayloadBackendError::OpenLocalPayloadBackend {
                    cache_root: cache_root_display.to_owned(),
                    source: Box::new(source),
                });
            }
        }
    }

    unreachable!("local payload open loop returns on success or final failure")
}

/// Open the current local cache backend for a cache root.
pub fn open_local_payload_backend(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
) -> Result<OpenedLocalPayloadBackend, OpenLocalPayloadBackendError> {
    open_local_payload_backend_budgeted(
        cache_root.as_ref(),
        volume_uuid,
        None,
        runtime::LocalPayloadSyncMode::Durable,
        runtime::LocalPayloadWriteMode::Buffered,
    )
}

/// Open the selected local backend with an explicit local-cache budget.
pub fn open_local_payload_backend_with_budget(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
) -> Result<OpenedLocalPayloadBackend, OpenLocalPayloadBackendError> {
    open_local_payload_backend_with_budget_and_sync_mode(
        cache_root,
        volume_uuid,
        cache_budget_bytes,
        runtime::LocalPayloadSyncMode::Durable,
    )
}

/// Open the selected local backend with an explicit local-cache budget and
/// payload-store sync policy.
pub fn open_local_payload_backend_with_budget_and_sync_mode(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    payload_sync_mode: runtime::LocalPayloadSyncMode,
) -> Result<OpenedLocalPayloadBackend, OpenLocalPayloadBackendError> {
    open_local_payload_backend_with_budget_sync_and_write_mode(
        cache_root,
        volume_uuid,
        cache_budget_bytes,
        payload_sync_mode,
        runtime::LocalPayloadWriteMode::Buffered,
    )
}

/// Open the selected local backend with explicit budget, sync policy, and write policy.
pub fn open_local_payload_backend_with_budget_sync_and_write_mode(
    cache_root: impl AsRef<Path>,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    payload_sync_mode: runtime::LocalPayloadSyncMode,
    payload_write_mode: runtime::LocalPayloadWriteMode,
) -> Result<OpenedLocalPayloadBackend, OpenLocalPayloadBackendError> {
    open_local_payload_backend_budgeted(
        cache_root.as_ref(),
        volume_uuid,
        Some(cache_budget_bytes),
        payload_sync_mode,
        payload_write_mode,
    )
}

/// Open the selected local backend using an already prepared local-cache runtime.
pub fn open_prepared_local_payload_backend_with_budget_and_sync_mode(
    prepared: PreparedLocalPayloadRuntime,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    payload_sync_mode: runtime::LocalPayloadSyncMode,
) -> Result<OpenedLocalPayloadBackend, OpenLocalPayloadBackendError> {
    open_prepared_local_payload_backend_with_budget_sync_and_write_mode(
        prepared,
        volume_uuid,
        cache_budget_bytes,
        payload_sync_mode,
        runtime::LocalPayloadWriteMode::Buffered,
    )
}

/// Open the selected local backend using an already prepared local-cache runtime
/// with explicit budget, sync policy, and write policy.
pub fn open_prepared_local_payload_backend_with_budget_sync_and_write_mode(
    prepared: PreparedLocalPayloadRuntime,
    volume_uuid: &str,
    cache_budget_bytes: u64,
    payload_sync_mode: runtime::LocalPayloadSyncMode,
    payload_write_mode: runtime::LocalPayloadWriteMode,
) -> Result<OpenedLocalPayloadBackend, OpenLocalPayloadBackendError> {
    open_detected_local_payload_backend_budgeted(
        prepared.detection,
        volume_uuid,
        Some(cache_budget_bytes),
        payload_sync_mode,
        payload_write_mode,
    )
}

fn open_local_payload_backend_budgeted(
    cache_root: &Path,
    volume_uuid: &str,
    cache_budget_bytes: Option<u64>,
    payload_sync_mode: runtime::LocalPayloadSyncMode,
    payload_write_mode: runtime::LocalPayloadWriteMode,
) -> Result<OpenedLocalPayloadBackend, OpenLocalPayloadBackendError> {
    let cache_root_display = cache_root.display().to_string();

    let detection = prepare_local_payload_runtime(cache_root)
        .map_err(|source| match source {
            PrepareLocalPayloadRuntimeError::DetectCacheBackend { cache_root, source } => {
                OpenLocalPayloadBackendError::DetectCacheBackend { cache_root, source }
            }
            PrepareLocalPayloadRuntimeError::ProvisionLocalPayloadLayout { cache_root, source } => {
                OpenLocalPayloadBackendError::ProvisionLocalPayloadLayout { cache_root, source }
            }
        })?
        .detection;
    match cache_budget_bytes {
        Some(cache_budget_bytes) => open_detected_local_payload_backend_budgeted(
            detection,
            volume_uuid,
            Some(cache_budget_bytes),
            payload_sync_mode,
            payload_write_mode,
        ),
        None => {
            let backend = open_current_local_payload_backend_budgeted(
                cache_root,
                &cache_root_display,
                volume_uuid,
                None,
                payload_sync_mode,
                payload_write_mode,
            )?;
            Ok(OpenedLocalPayloadBackend {
                detection,
                backend_kind: runtime::LocalPayloadBackendKind::Current,
                backend,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{LocalPayloadBackendKind, LocalPayloadLayout, LocalPayloadSyncMode};

    #[test]
    fn prepare_local_payload_runtime_creates_contract_paths() {
        let temp = tempfile::tempdir().expect("tempdir");

        let prepared =
            prepare_local_payload_runtime(temp.path()).expect("prepare local payload runtime");

        assert_eq!(
            prepared.layout,
            LocalPayloadLayout::from_cache_root(temp.path())
        );
        assert!(prepared.detection.has_runtime_layout());
        assert!(prepared.layout.tmp_dir.exists());
        assert!(prepared.layout.payload_objects_dir.exists());
        assert!(prepared.layout.upload_stage_objects_dir.exists());
        assert!(prepared.layout.index_dir.exists());
    }

    #[test]
    fn prepare_local_payload_runtime_is_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");

        let first = prepare_local_payload_runtime(temp.path()).expect("first prepare");
        let second = prepare_local_payload_runtime(temp.path()).expect("second prepare");

        assert_eq!(first.layout, second.layout);
        assert_eq!(first.detection, second.detection);
        assert!(second.detection.has_runtime_layout());
    }

    #[test]
    fn prepare_local_payload_runtime_removes_retired_v3_subtree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let retired_root = temp.path().join(RETIRED_LOCAL_PAYLOAD_SUBTREE);
        std::fs::create_dir_all(retired_root.join("payload")).expect("create retired payload");
        std::fs::create_dir_all(retired_root.join("index")).expect("create retired index");
        std::fs::write(retired_root.join("stale.txt"), b"stale").expect("write stale file");

        let prepared =
            prepare_local_payload_runtime(temp.path()).expect("prepare local payload runtime");

        assert!(prepared.detection.has_runtime_layout());
        assert!(!retired_root.exists());
        assert!(prepared.layout.upload_stage_objects_dir.exists());
    }

    #[test]
    fn prepare_local_payload_runtime_keeps_unrecognized_v3_entry() {
        let temp = tempfile::tempdir().expect("tempdir");
        let retired_root = temp.path().join(RETIRED_LOCAL_PAYLOAD_SUBTREE);
        std::fs::create_dir_all(&retired_root).expect("create unrelated v3 dir");
        std::fs::write(retired_root.join("notes.txt"), b"user data").expect("write unrelated file");

        let prepared =
            prepare_local_payload_runtime(temp.path()).expect("prepare local payload runtime");

        assert!(prepared.detection.has_runtime_layout());
        assert!(
            retired_root.exists(),
            "non-runtime v3 entries should be preserved"
        );
        assert!(retired_root.join("notes.txt").exists());
    }

    #[test]
    fn prepared_local_payload_runtime_opens_local_payload_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let prepared =
            prepare_local_payload_runtime(temp.path()).expect("prepare local payload runtime");

        let opened = open_local_payload_backend(
            &prepared.layout.cache_root,
            "00000000-0000-0000-0000-000000000001",
        )
        .expect("open local payload backend");

        assert_eq!(opened.backend_kind, LocalPayloadBackendKind::Current);
        assert!(opened.detection.has_runtime_layout());
    }

    #[test]
    fn prepared_local_payload_runtime_reuses_detection_to_open_budgeted_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let prepared =
            prepare_local_payload_runtime(temp.path()).expect("prepare local payload runtime");

        let opened = open_prepared_local_payload_backend_with_budget_and_sync_mode(
            prepared.clone(),
            "00000000-0000-0000-0000-000000000001",
            64 * 1024 * 1024,
            LocalPayloadSyncMode::Durable,
        )
        .expect("open prepared local payload backend");

        assert_eq!(opened.backend_kind, LocalPayloadBackendKind::Current);
        assert_eq!(opened.detection, prepared.detection);
    }

    #[test]
    fn local_payload_runtime_root_opens_local_payload_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let cache_root = temp.path();
        let layout = LocalPayloadLayout::from_cache_root(cache_root);
        layout.provision().expect("provision runtime layout");

        let opened = open_local_payload_backend(cache_root, "00000000-0000-0000-0000-000000000001")
            .expect("open local payload backend");

        assert_eq!(opened.backend_kind, LocalPayloadBackendKind::Current);
    }
}

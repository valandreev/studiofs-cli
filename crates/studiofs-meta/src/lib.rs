//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem using redb as the embedded KV store.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// Async adapters for invoking synchronous metadata capabilities from Tokio.
pub mod async_engine;
/// Fast Contract/Core fixtures and invariant checks for local tests.
pub mod contract_harness;
/// Backend-agnostic metadata contracts shared by all metadata engines.
pub mod engine;
/// Internal metadata event-log payloads used for replication and recovery.
pub mod events;
/// PostgreSQL DSN helpers for StudioFS metadata databases.
pub mod pg_dsn;
/// PostgreSQL-backed metadata engine implementation.
pub mod pg_engine;
/// TLS-aware PostgreSQL transport helpers.
pub mod pg_transport;
/// redb-backed metadata engine implementation.
pub mod redb_engine;
/// Internal on-disk metadata schema and replication model types.
pub mod schema;
/// Public metadata model types shared across engines and callers.
pub mod types;

use std::path::Path;
use std::sync::Arc;

pub use async_engine::{SyncMetaEngineAdapter, execute_sync_meta};
pub use engine::{
    LocalStorageCompactionResult, MetaAcl, MetaEngine, MetaEngineCore, MetaError, MetaFormat,
    MetaFsOps, MetaGc, MetaInodes, MetaNamespace, MetaPayloadObjects, MetaRecovery,
    MetaReplication, MetaResult, MetaSnapshots, MetaSymlink, MetaXattr,
};
pub use events::MetaEvent;
pub use pg_dsn::{
    STUDIOFS_METADATA_DB, StudioPgDsns, ensure_studiofs_database, resolve_studiofs_pg_dsns,
};
pub use pg_engine::{PgMetaEngine, PgOpenOptions};
pub use pg_transport::connect_studiofs_postgres;
pub use redb_engine::RedbMetaEngine;
pub use types::*;

#[derive(Debug, Clone, Copy)]
/// Runtime knobs that influence how a metadata engine instance is opened.
pub struct MetaOpenOptions {
    /// Whether remote-backed engines may start background polling workers.
    pub background_poll: bool,
}

impl Default for MetaOpenOptions {
    fn default() -> Self {
        Self {
            background_poll: true,
        }
    }
}

fn ensure_supported_format_version(info: &FormatInfo) -> MetaResult<()> {
    if info.format_version != CURRENT_FORMAT_VERSION {
        return Err(MetaError::UnsupportedFormatVersion {
            found: info.format_version,
            expected: CURRENT_FORMAT_VERSION,
        });
    }
    info.validate_format_contract()?;
    Ok(())
}

/// Open metadata engine based on format settings.
pub fn open_meta_engine(path: impl AsRef<Path>) -> MetaResult<Arc<dyn MetaEngine>> {
    open_meta_engine_with_options(path, MetaOpenOptions::default())
}

/// Open metadata engine with runtime behavior overrides.
pub fn open_meta_engine_with_options(
    path: impl AsRef<Path>,
    options: MetaOpenOptions,
) -> MetaResult<Arc<dyn MetaEngine>> {
    let path = path.as_ref();
    match RedbMetaEngine::read_format_info_readonly(path) {
        Ok(info) if info.metadata.backend == MetadataBackend::Postgres => {
            ensure_supported_format_version(&info)?;
            let pg = PgMetaEngine::open_with_options(
                path,
                PgOpenOptions {
                    background_poll: options.background_poll,
                },
            )?;
            let pg_info = pg.get_format_info()?;
            ensure_supported_format_version(&pg_info)?;
            if pg_info.metadata.backend != MetadataBackend::Postgres {
                return Err(MetaError::Storage(
                    "metadata backend switched during PgMetaEngine open".to_string(),
                ));
            }
            Ok(pg)
        }
        Ok(info) => {
            ensure_supported_format_version(&info)?;
            let redb = RedbMetaEngine::open(path)?;
            let redb_info = redb.get_format_info()?;
            ensure_supported_format_version(&redb_info)?;
            if redb_info.metadata.backend != info.metadata.backend {
                return Err(MetaError::Storage(
                    "metadata backend switched during RedbMetaEngine open".to_string(),
                ));
            }
            Ok(Arc::new(redb))
        }
        Err(MetaError::NotFormatted) => {
            let redb = RedbMetaEngine::open(path)?;
            Ok(Arc::new(redb))
        }
        Err(ref err) if err.is_redb_repair_needed() => {
            // ReadOnlyDatabase cannot repair an unclean shutdown.  Fall through
            // to Database::create() which automatically recovers from crashes,
            // power loss, and other unclean shutdowns (redb 3.x).
            tracing::warn!(
                "redb needs repair (read-only probe failed: {err}); \
                 attempting writable open for auto-recovery"
            );
            // Open writable to trigger redb auto-repair, read format info,
            // then drop before re-opening through the proper engine path.
            let info = {
                let redb = RedbMetaEngine::open(path)?;
                let info = redb.get_format_info()?;
                ensure_supported_format_version(&info)?;
                if info.metadata.backend == MetadataBackend::Postgres {
                    redb.set_counter(pg_engine::LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 1)?;
                    redb.set_counter("pgLastAppliedBatchVersion", 0)?;
                    redb.set_counter("pgLastAppliedNamespaceBatchVersion", 0)?;
                }
                info
                // redb dropped here — releases file lock
            };
            if info.metadata.backend == MetadataBackend::Postgres {
                let pg = PgMetaEngine::open_with_options(
                    path,
                    PgOpenOptions {
                        background_poll: options.background_poll,
                    },
                )?;
                Ok(pg)
            } else {
                let redb = RedbMetaEngine::open(path)?;
                Ok(Arc::new(redb))
            }
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_meta_engine_missing_file_creates_redb() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let path = dir.path().join("missing-meta.db");
        assert!(!path.exists());

        let engine = open_meta_engine_with_options(
            &path,
            MetaOpenOptions {
                background_poll: false,
            },
        )
        .expect("open missing metadata path");

        let info = engine.get_format_info();
        assert!(matches!(info, Err(MetaError::NotFormatted)));
    }

    #[test]
    fn pg_repair_reset_preserves_client_slot_and_local_flush_position() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let path = dir.path().join("pg-repair-reset.db");
        let redb = RedbMetaEngine::open(&path).expect("open redb");

        redb.set_counter("pgLastAppliedBatchVersion", 42)
            .expect("seed pg replay position");
        redb.set_counter("pgLastAppliedNamespaceBatchVersion", 41)
            .expect("seed pg namespace position");
        redb.set_counter("pgLastFlushedLocalEventSeq", 7)
            .expect("seed local flush position");
        redb.set_counter("pgClientSlotPlusOne", 3)
            .expect("seed client slot");

        redb.set_counter("pgLastAppliedBatchVersion", 0)
            .expect("reset pg replay position");
        redb.set_counter("pgLastAppliedNamespaceBatchVersion", 0)
            .expect("reset pg namespace position");

        assert_eq!(
            redb.get_counter("pgLastAppliedBatchVersion")
                .expect("pg replay position"),
            0
        );
        assert_eq!(
            redb.get_counter("pgLastAppliedNamespaceBatchVersion")
                .expect("pg namespace position"),
            0
        );
        assert_eq!(
            redb.get_counter("pgLastFlushedLocalEventSeq")
                .expect("local flush position"),
            7,
            "repair reset must not make already-flushed local events publish again"
        );
        assert_eq!(
            redb.get_counter("pgClientSlotPlusOne")
                .expect("client slot"),
            3,
            "repair reset must keep the stable client identity for self-batch detection"
        );
    }
}

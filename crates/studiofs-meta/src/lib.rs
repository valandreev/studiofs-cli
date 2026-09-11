//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem using redb as the embedded KV store.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub(crate) mod allocator_state;

/// Async adapters for invoking synchronous metadata capabilities from Tokio.
pub mod async_engine;
/// Platform-neutral invalidation intents for committed local metadata views.
pub mod coherence;
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
/// Read-only metadata adapter used by immutable snapshot mounts.
pub mod read_only;
/// redb-backed metadata engine implementation.
pub mod redb_engine;
/// Internal on-disk metadata schema and replication model types.
pub mod schema;
/// PostgreSQL-authoritative Snapshot v2 domain contracts.
pub mod snapshot;
/// Backend-agnostic Snapshot v2 application ports.
pub mod snapshot_ports;
/// Public metadata model types shared across engines and callers.
pub mod types;

use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

pub use async_engine::{SyncMetaEngineAdapter, execute_sync_meta};
pub use engine::{
    CheckpointDescriptorFailure, LocalStorageCompactionResult, MetaAcl, MetaEngine, MetaEngineCore,
    MetaError, MetaFormat, MetaFsOps, MetaGc, MetaInodes, MetaNamespace, MetaPayloadObjects,
    MetaRecovery, MetaReplication, MetaResult, MetaSymlink, MetaXattr,
    MetadataBatchWriteObservability, MetadataCoherenceProgress, MetadataWriteObservability,
    MetadataWriteStageTiming, NamespaceMutation, RegularFileOverlayMutation,
};
pub use events::MetaEvent;
pub use pg_dsn::{
    STUDIOFS_METADATA_DB, StudioPgDsns, ensure_studiofs_database, resolve_studiofs_pg_dsns,
};
pub use pg_engine::{DEFAULT_PG_POOL_SIZE, PgMetaEngine, PgOpenOptions, PgSnapshotBackend};
pub use pg_transport::connect_studiofs_postgres;
pub use read_only::ReadOnlyMetaEngine;
pub use redb_engine::RedbMetaEngine;
pub use types::*;

#[derive(serde::Deserialize)]
struct FormatVersionOnly {
    format_version: u16,
}

/// Decode and validate persisted [`FormatInfo`] while checking its version first.
///
/// The version-only pass preserves the actionable incompatibility error even when
/// an older format no longer satisfies the current JSON shape.
pub fn decode_format_info(bytes: &[u8]) -> MetaResult<FormatInfo> {
    let version: FormatVersionOnly =
        serde_json::from_slice(bytes).map_err(|e| MetaError::Serialization(e.to_string()))?;
    if version.format_version != CURRENT_FORMAT_VERSION {
        return Err(MetaError::UnsupportedFormatVersion {
            found: version.format_version,
            expected: CURRENT_FORMAT_VERSION,
        });
    }
    let info: FormatInfo =
        serde_json::from_slice(bytes).map_err(|e| MetaError::Serialization(e.to_string()))?;
    info.validate_format_contract()?;
    Ok(info)
}

/// Validate the atomic PostgreSQL serving-checkpoint descriptor.
pub fn validate_checkpoint_descriptor(
    snapshot_bytes: &[u8],
    sha256: Option<&[u8]>,
    publication_state: Option<&str>,
    generation: Option<i64>,
) -> MetaResult<()> {
    if publication_state != Some("published") {
        return Err(MetaError::InvalidCheckpointDescriptor {
            reason: CheckpointDescriptorFailure::PublicationState,
        });
    }
    if generation.unwrap_or_default() <= 0 {
        return Err(MetaError::InvalidCheckpointDescriptor {
            reason: CheckpointDescriptorFailure::Generation,
        });
    }
    let Some(sha256) = sha256 else {
        return Err(MetaError::InvalidCheckpointDescriptor {
            reason: CheckpointDescriptorFailure::Checksum,
        });
    };
    if sha256.len() != 32 || sha256 != Sha256::digest(snapshot_bytes).as_slice() {
        return Err(MetaError::InvalidCheckpointDescriptor {
            reason: CheckpointDescriptorFailure::Checksum,
        });
    }
    Ok(())
}

/// Opens an immutable snapshot metadata artifact behind the read-only metadata
/// adapter used by snapshot mounts.
pub fn open_read_only_snapshot_meta(
    path: impl AsRef<Path>,
) -> MetaResult<Arc<ReadOnlyMetaEngine<RedbMetaEngine>>> {
    let path = path.as_ref();
    let format_info = RedbMetaEngine::read_format_info_readonly(path)?;
    ensure_supported_format_version(&format_info)?;
    let redb = Arc::new(RedbMetaEngine::open_read_only(path)?);
    let opened_format_info = redb.get_format_info()?;
    ensure_supported_format_version(&opened_format_info)?;
    if opened_format_info.format_version != format_info.format_version
        || opened_format_info.name != format_info.name
        || opened_format_info.uuid != format_info.uuid
        || opened_format_info.data_prefix != format_info.data_prefix
        || opened_format_info.block_size != format_info.block_size
    {
        return Err(MetaError::Storage(
            "snapshot metadata changed while opening".to_string(),
        ));
    }
    Ok(Arc::new(ReadOnlyMetaEngine::new(redb)))
}

#[derive(Debug, Clone, Copy)]
/// Runtime knobs that influence how a metadata engine instance is opened.
pub struct MetaOpenOptions {
    /// Whether remote-backed engines may start background polling workers.
    pub background_poll: bool,
    /// Maximum number of PostgreSQL command connections for this client.
    /// Values above the runtime semaphore capacity are rejected.
    /// `STUDIOFS_PG_POOL_SIZE` overrides this value when set.
    pub pg_pool_size: usize,
}

impl Default for MetaOpenOptions {
    fn default() -> Self {
        Self {
            background_poll: true,
            pg_pool_size: DEFAULT_PG_POOL_SIZE,
        }
    }
}

/// Validate the local/remote metadata format before opening or importing it.
pub fn ensure_supported_format_version(info: &FormatInfo) -> MetaResult<()> {
    if info.format_version != CURRENT_FORMAT_VERSION {
        return Err(MetaError::UnsupportedFormatVersion {
            found: info.format_version,
            expected: CURRENT_FORMAT_VERSION,
        });
    }
    info.validate_format_contract()?;
    Ok(())
}

/// Validate the PostgreSQL scalar sync-format version stored with a checkpoint.
///
/// PostgreSQL stores this value as `SMALLINT`, so missing, negative, or
/// otherwise unrepresentable values are converted to the unsupported sentinel
/// instead of being accepted as a current format.
pub fn ensure_supported_pg_sync_format_version(found: Option<i16>) -> MetaResult<()> {
    let found = found
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or_default();
    if found != CURRENT_FORMAT_VERSION {
        return Err(MetaError::UnsupportedFormatVersion {
            found,
            expected: CURRENT_FORMAT_VERSION,
        });
    }
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
                    pg_pool_size: options.pg_pool_size,
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
                        pg_pool_size: options.pg_pool_size,
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
    fn pg_sync_format_validator_rejects_missing_or_legacy_values() {
        assert!(
            ensure_supported_pg_sync_format_version(Some(CURRENT_FORMAT_VERSION as i16,)).is_ok()
        );

        for found in [None, Some(4), Some(-1)] {
            let err = ensure_supported_pg_sync_format_version(found).unwrap_err();
            assert!(matches!(
                err,
                MetaError::UnsupportedFormatVersion {
                    expected: CURRENT_FORMAT_VERSION,
                    ..
                }
            ));
            let message = err.to_string();
            assert!(message.contains("recreate"));
            assert!(message.contains("discover"));
        }
    }

    #[test]
    fn format_info_decode_reports_malformed_payloads() {
        for bytes in [
            b"".as_slice(),
            b"{".as_slice(),
            br#"{"name":"missing-version"}"#.as_slice(),
        ] {
            assert!(matches!(
                decode_format_info(bytes),
                Err(MetaError::Serialization(_))
            ));
        }
    }

    #[test]
    fn checkpoint_descriptor_validator_reports_each_invalid_field() {
        let bytes = b"checkpoint";
        let digest = Sha256::digest(bytes);

        let cases = [
            (
                Some(1),
                Some(digest.as_slice()),
                Some("pending"),
                CheckpointDescriptorFailure::PublicationState,
            ),
            (
                Some(0),
                Some(digest.as_slice()),
                Some("published"),
                CheckpointDescriptorFailure::Generation,
            ),
            (
                Some(1),
                Some(&[0; 32][..]),
                Some("published"),
                CheckpointDescriptorFailure::Checksum,
            ),
        ];

        for (generation, checksum, publication_state, reason) in cases {
            let error =
                validate_checkpoint_descriptor(bytes, checksum, publication_state, generation)
                    .unwrap_err();
            assert!(matches!(
                error,
                MetaError::InvalidCheckpointDescriptor { reason: actual } if actual == reason
            ));
        }
    }

    #[test]
    fn checkpoint_descriptor_validator_rejects_missing_or_malformed_checksum() {
        let bytes = b"checkpoint";
        for checksum in [None, Some(&[0; 31][..])] {
            let error = validate_checkpoint_descriptor(bytes, checksum, Some("published"), Some(1))
                .unwrap_err();
            assert!(matches!(
                error,
                MetaError::InvalidCheckpointDescriptor {
                    reason: CheckpointDescriptorFailure::Checksum
                }
            ));
        }

        let digest = Sha256::digest(bytes);
        let error = validate_checkpoint_descriptor(
            bytes,
            Some(digest.as_slice()),
            Some("published"),
            Some(-1),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            MetaError::InvalidCheckpointDescriptor {
                reason: CheckpointDescriptorFailure::Generation
            }
        ));
    }

    #[test]
    fn newer_format_error_includes_upgrade_guidance() {
        let error = MetaError::UnsupportedFormatVersion {
            found: CURRENT_FORMAT_VERSION + 1,
            expected: CURRENT_FORMAT_VERSION,
        };

        let message = error.to_string();
        assert!(message.contains("matching StudioFS binary"));
        assert!(message.contains("If the stored state is older"));
    }

    #[test]
    fn test_open_meta_engine_missing_file_creates_redb() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let path = dir.path().join("missing-meta.db");
        assert!(!path.exists());

        let engine = open_meta_engine_with_options(
            &path,
            MetaOpenOptions {
                background_poll: false,
                ..Default::default()
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

    #[test]
    fn storage_repair_text_is_not_treated_as_a_typed_repair_error() {
        assert!(!MetaError::Storage("Database repair aborted".to_string()).is_redb_repair_needed());
        assert!(
            MetaError::RepairRequired("redb database requires repair".to_string())
                .is_redb_repair_needed()
        );
    }
}

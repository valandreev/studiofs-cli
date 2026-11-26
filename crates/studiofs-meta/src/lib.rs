//! StudioFS Metadata Engine
//!
//! Provides metadata storage for the virtual filesystem. Mounted foreground
//! operations use the local `redb` view. For PostgreSQL-backed volumes,
//! `PgMetaEngine` bootstraps and replicates shared state into that view;
//! PostgreSQL is not a foreground metadata read backend.
//! All filesystem structure (inodes, directory entries, chunk/slice mappings) is managed here.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub(crate) mod allocator_state;

/// Async adapters for invoking synchronous metadata capabilities from Tokio.
pub mod async_engine;
/// Backend-neutral client session data.
pub mod client_sessions;
/// Platform-neutral invalidation intents for committed local metadata views.
pub mod coherence;
/// Pure capability evaluator deciding per-operation volume compatibility.
pub mod compat;
/// Fast Contract/Core fixtures and invariant checks for local tests.
pub mod contract_harness;
/// Backend-agnostic metadata contracts shared by all metadata engines.
pub mod engine;
/// Internal metadata event-log payloads used for replication and recovery.
pub mod events;
/// Metadata protection snapshots for standalone object cleanup.
pub mod janitor;
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
/// Shared recoverable-trash retention policy.
pub mod trash;
/// Public metadata model types shared across engines and callers.
pub mod types;
/// Typed version identities of the persisted boundaries.
pub mod version;
/// Per-volume protocol policy and feature activation state (SFS-1007).
pub mod volume_protocol;

/// Test-only successor change registry (SFS-1131).
#[cfg(feature = "test-successor")]
pub mod test_successor;

use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

pub use async_engine::{SyncMetaEngineAdapter, execute_sync_meta};
pub use engine::{
    BootstrapCheckpointDescriptor, CheckpointDescriptorFailure, CheckpointEncoding,
    LocalOutboundAckSnapshot, LocalStorageCompactionResult, LocalStorageCompactionSkipReason,
    LocalStorageDiagnostics, MetaAcl, MetaEngine, MetaEngineCore, MetaError, MetaFormat, MetaFsOps,
    MetaGc, MetaInodes, MetaNamespace, MetaPayloadObjects, MetaRecovery, MetaReplication,
    MetaResult, MetaSymlink, MetaXattr, MetadataBatchWriteObservability,
    MetadataCheckpointFreshnessDiagnostics, MetadataCheckpointRefreshReason,
    MetadataCoherenceProgress, MetadataWriteObservability, MetadataWriteStageTiming,
    NamespaceMutation, OrphanInodeFinalization, PgSyncDiagnostics, PgSyncFailure, PgSyncLifecycle,
    PgSyncPageDiagnostics, PgSyncStatus, PgSyncWriteAuthorityReason, RegularFileOverlayMutation,
    ScratchAutoPromoteResult, ScratchAuxTable, ScratchConflictRecord,
    ScratchConsistencyRepairSummary, ScratchConsistencyReport, ScratchDanglingDentry,
    ScratchOrphanAuxRow, ScratchOrphanEntry, ScratchOrphanInode, ScratchOrphanRecord,
    ScratchPayloadObjectRef, ScratchRenameResult, UpgradeRequiredReason,
};
pub use events::MetaEvent;
pub use janitor::{
    JANITOR_DELETION_CLAIM_LEASE_SECS, JanitorDeletionSettlement, JanitorEligibility,
    JanitorMetadataError, JanitorMetadataSession, JanitorProtection, JanitorSweepCheckpoint,
    JanitorSweepScope, MAX_JANITOR_ELIGIBILITY_BATCH,
};
pub use pg_dsn::{
    STUDIOFS_METADATA_DB, StudioPgDsns, ensure_studiofs_database, resolve_studiofs_pg_dsns,
};
pub use pg_engine::{
    DEFAULT_PG_POOL_SIZE, LOCAL_PG_RETAINED_RESTORE_OWNER_COUNTER, PgMetaEngine, PgOpenOptions,
    PgSnapshotBackend,
};
pub use pg_transport::connect_studiofs_postgres;
pub use read_only::ReadOnlyMetaEngine;
pub use redb_engine::{RedbMetaEngine, layout::fold_dentry_name};
pub use types::*;

#[derive(serde::Deserialize)]
struct FormatVersionOnly {
    format_version: u16,
}

/// Decode and validate shared [`FormatInfo`] while checking its version first.
///
/// The version-only pass preserves the actionable incompatibility error even when
/// an older format no longer satisfies the current JSON shape. An unknown field
/// or enum variant is a newer writer's addition and returns
/// [`MetaError::UpgradeRequired`] naming it.
pub fn decode_format_info(bytes: &[u8]) -> MetaResult<FormatInfo> {
    decode_format_info_as(bytes, version::SHARED_VOLUME_FORMAT)
}

fn decode_local_format_info(bytes: &[u8]) -> MetaResult<FormatInfo> {
    decode_format_info_as(bytes, version::LOCAL_METADATA_LAYOUT)
}

/// [`decode_format_info`] with the version judged by `identity`, so the local
/// redb header copy and shared `FormatInfo` answer to separate identities.
fn decode_format_info_as(
    bytes: &[u8],
    identity: version::VersionIdentity,
) -> MetaResult<FormatInfo> {
    let version: FormatVersionOnly =
        serde_json::from_slice(bytes).map_err(|error| invalid_format_info_json(&error))?;
    identity.check(version.format_version)?;
    let info: FormatInfo = serde_json::from_slice(bytes).map_err(format_info_decode_error)?;
    info.validate_format_contract()?;
    Ok(info)
}

/// Maps serde's unknown field/variant errors to [`MetaError::UpgradeRequired`].
///
/// `serde_json::Error` exposes a data category for both cases. The pinned
/// decoder regression tests guard the Serde diagnostic syntax used here.
fn format_info_decode_error(error: serde_json::Error) -> MetaError {
    if !error.is_data() {
        return invalid_format_info_json(&error);
    }
    let message = error.to_string();
    let quoted = |prefix: &str| {
        let rest = message.strip_prefix(prefix)?;
        let (name, _) = rest.rsplit_once("`, expected ")?;
        let safe_name = if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            "<redacted>"
        } else {
            name
        };
        Some(safe_name.to_owned())
    };
    let reason = if let Some(field) = quoted("unknown field `") {
        UpgradeRequiredReason::UnknownFormatInfoField { field }
    } else if let Some(variant) = quoted("unknown variant `") {
        UpgradeRequiredReason::UnknownFormatInfoVariant { variant }
    } else {
        return invalid_format_info_json(&error);
    };
    MetaError::UpgradeRequired { reason }
}

fn invalid_format_info_json(error: &serde_json::Error) -> MetaError {
    // Serde's data errors can contain the original value of a malformed field.
    MetaError::Serialization(format!(
        "invalid FormatInfo JSON at line {}, column {}",
        error.line(),
        error.column(),
    ))
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
    /// Whether this open is the checkpoint-first bootstrap of a newly
    /// discovered client.
    pub fresh_client: bool,
    /// Maximum number of PostgreSQL command connections for this client.
    /// Values above the runtime semaphore capacity are rejected.
    /// `STUDIOFS_PG_POOL_SIZE` overrides this value when set.
    pub pg_pool_size: usize,
    /// Whether to defer synchronous remote startup probes until replication runs.
    pub defer_remote_startup_probes: bool,
}

impl Default for MetaOpenOptions {
    fn default() -> Self {
        Self {
            background_poll: true,
            fresh_client: false,
            pg_pool_size: DEFAULT_PG_POOL_SIZE,
            defer_remote_startup_probes: false,
        }
    }
}

/// Validate shared [`FormatInfo`] against the shared volume format before
/// opening or importing it.
pub fn ensure_supported_format_version(info: &FormatInfo) -> MetaResult<()> {
    version::SHARED_VOLUME_FORMAT.check(info.format_version)?;
    info.validate_format_contract()?;
    Ok(())
}

/// Validate a local redb header against the local metadata layout identity.
fn ensure_supported_local_format_version(info: &FormatInfo) -> MetaResult<()> {
    version::LOCAL_METADATA_LAYOUT.check(info.format_version)?;
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
    version::SHARED_VOLUME_FORMAT.check(found)
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
            ensure_supported_local_format_version(&info)?;
            let pg = PgMetaEngine::open_with_options(
                path,
                PgOpenOptions {
                    background_poll: options.background_poll,
                    fresh_client: options.fresh_client,
                    pg_pool_size: options.pg_pool_size,
                    defer_remote_startup_probes: options.defer_remote_startup_probes,
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
            ensure_supported_local_format_version(&info)?;
            let redb = RedbMetaEngine::open(path)?;
            let redb_info = redb.get_format_info()?;
            ensure_supported_local_format_version(&redb_info)?;
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
                ensure_supported_local_format_version(&info)?;
                if info.metadata.backend == MetadataBackend::Postgres {
                    mark_unclean_pg_local_graph(&redb)?;
                }
                info
                // redb dropped here — releases file lock
            };
            if info.metadata.backend == MetadataBackend::Postgres {
                let pg = PgMetaEngine::open_with_options(
                    path,
                    PgOpenOptions {
                        background_poll: options.background_poll,
                        fresh_client: options.fresh_client,
                        pg_pool_size: options.pg_pool_size,
                        defer_remote_startup_probes: options.defer_remote_startup_probes,
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

/// Decide whether an unclean redb image needs the PostgreSQL graph repair.
///
/// redb recovers the last durable commit, a consistent prefix of local history.
/// That prefix stays authoritative when the publication fence was active and no
/// graph repair was in progress: every event this client published was durable
/// before publication, so the image is not older than this client's shared
/// history and serves (and later flushes) its unpublished journal like a clean
/// reopen. Otherwise the image may predate published local events, and repair
/// rebuilds it from PostgreSQL before any local authority is trusted.
fn mark_unclean_pg_local_graph(redb: &RedbMetaEngine) -> MetaResult<()> {
    if pg_engine::local_graph_survives_unclean_shutdown(redb)? {
        tracing::info!("redb recovered a fenced local graph; keeping local authority");
        return Ok(());
    }
    redb.set_counter(pg_engine::LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 1)?;
    redb.set_counter(pg_engine::LOCAL_PG_VERSION_COUNTER, 0)?;
    redb.set_counter(pg_engine::LOCAL_PG_NAMESPACE_VERSION_COUNTER, 0)?;
    Ok(())
}

#[cfg(test)]
mod baseline_fixtures;

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

    fn baseline_format_info_json() -> serde_json::Value {
        serde_json::to_value(baseline_fixtures::fixture_format_info()).unwrap()
    }

    fn assert_upgrade_required(bytes: &[u8], expected: &UpgradeRequiredReason) {
        let error = decode_format_info(bytes).unwrap_err();
        assert!(
            matches!(&error, MetaError::UpgradeRequired { reason } if reason == expected),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(message.contains("upgrade required"), "{message}");
        assert!(!message.contains("sfs-fixture-secret"), "{message}");
        assert!(!message.contains("postgres://"), "{message}");
    }

    #[test]
    fn format_info_decode_reports_unknown_field_as_upgrade_required() {
        let mut baseline = baseline_format_info_json();
        baseline["metadata"]["remote"] = serde_json::json!({
            "endpoint": "http://127.0.0.1:9000",
            "bucket": "metadata",
            "access_key": "fixture-access",
            "secret_key": "sfs-fixture-secret",
            "region": "us-east-1",
        });
        for pointer in [
            "",
            "/metadata",
            "/metadata/remote",
            "/metadata/postgres",
            "/encryption",
        ] {
            let mut json = baseline.clone();
            json.pointer_mut(pointer)
                .and_then(serde_json::Value::as_object_mut)
                .unwrap_or_else(|| panic!("object at {pointer:?}"))
                .insert("successor_field".to_string(), serde_json::json!(true));
            assert_upgrade_required(
                &serde_json::to_vec(&json).unwrap(),
                &UpgradeRequiredReason::UnknownFormatInfoField {
                    field: "successor_field".to_string(),
                },
            );
        }
    }

    #[test]
    fn format_info_decode_reports_unknown_backend_as_upgrade_required() {
        let mut json = baseline_format_info_json();
        json["metadata"]["backend"] = serde_json::json!("redis");
        assert_upgrade_required(
            &serde_json::to_vec(&json).unwrap(),
            &UpgradeRequiredReason::UnknownFormatInfoVariant {
                variant: "redis".to_string(),
            },
        );
    }

    #[test]
    fn format_info_decode_redacts_non_identifier_backend_names() {
        let mut json = baseline_format_info_json();
        json["metadata"]["backend"] = serde_json::json!("future\nbackend");
        assert_upgrade_required(
            &serde_json::to_vec(&json).unwrap(),
            &UpgradeRequiredReason::UnknownFormatInfoVariant {
                variant: "<redacted>".to_string(),
            },
        );
    }

    #[test]
    fn format_info_decode_redacts_malformed_field_values() {
        let mut json = baseline_format_info_json();
        json["metadata"]["postgres"]["volume_id"] = serde_json::json!("fixture-private-value");
        let error = decode_format_info(&serde_json::to_vec(&json).unwrap()).unwrap_err();
        assert!(matches!(error, MetaError::Serialization(_)));
        assert!(!format!("{error}").contains("fixture-private-value"));
        assert!(!format!("{error:?}").contains("fixture-private-value"));
    }

    #[test]
    fn format_info_version_is_checked_before_unknown_fields() {
        let mut json = baseline_format_info_json();
        json["format_version"] = serde_json::json!(5);
        json["successor_field"] = serde_json::json!(true);
        assert!(matches!(
            decode_format_info(&serde_json::to_vec(&json).unwrap()),
            Err(MetaError::UnsupportedFormatVersion {
                found: 5,
                expected: 6
            })
        ));
    }

    #[test]
    fn persisted_version_gates_distinguish_volume_and_checkpoint_fields() {
        use crate::snapshot::ServingCheckpoint;

        // A newer serving checkpoint format is still outside the serving schema
        // range, and a shared volume's version is outside both checkpoint ranges.
        let checkpoint_format = ServingCheckpoint::FORMAT_VERSION;
        let checkpoint_schema = ServingCheckpoint::SCHEMA_VERSION;
        let info = baseline_fixtures::fixture_format_info();
        let volume_format = i16::try_from(info.format_version).unwrap();
        assert!(ServingCheckpoint::supports_format_version(
            checkpoint_format
        ));
        assert!(ServingCheckpoint::supports_schema_version(
            checkpoint_schema
        ));
        assert!(!ServingCheckpoint::supports_schema_version(
            checkpoint_format
        ));
        assert!(!ServingCheckpoint::supports_format_version(volume_format));
        assert!(!ServingCheckpoint::supports_schema_version(volume_format));

        let dir = studiofs_test_support::workspace_target_tempdir("version-gates-");
        let path = dir.path().join("metadata.redb");
        let redb = RedbMetaEngine::open(&path).unwrap();
        redb.set_format_info(&info).unwrap();
        drop(redb);
        let local = RedbMetaEngine::read_format_info_readonly(&path).unwrap();
        assert_eq!(local.format_version, info.format_version);
        ensure_supported_pg_sync_format_version(Some(volume_format)).unwrap();
        assert!(matches!(
            ensure_supported_pg_sync_format_version(Some(checkpoint_format)),
            Err(MetaError::UnsupportedFormatVersion { .. })
        ));
    }

    #[test]
    fn bootstrap_descriptor_keeps_format_info_upgrade_required() {
        let mut json = baseline_format_info_json();
        json["successor_field"] = serde_json::json!(true);
        let checkpoint_bytes = b"checkpoint".to_vec();
        let descriptor = BootstrapCheckpointDescriptor {
            volume_id: 1,
            volume_uuid: baseline_fixtures::fixture_format_info().uuid,
            format_info_bytes: serde_json::to_vec(&json).unwrap(),
            metadata_format_version: version::SHARED_VOLUME_FORMAT.current,
            remote_head_version: 1,
            checkpoint_version: 1,
            checkpoint_encoding: None,
            checkpoint_sha256: Sha256::digest(&checkpoint_bytes).to_vec(),
            checkpoint_bytes,
            publication_state: "published".to_string(),
            publication_generation: 1,
        };
        let error = descriptor.validate_and_decode().unwrap_err();
        assert!(
            matches!(
                error,
                MetaError::UpgradeRequired {
                    reason: UpgradeRequiredReason::UnknownFormatInfoField { .. }
                }
            ),
            "{error:?}"
        );
        assert_eq!(
            PgSyncFailure::from_error(&error),
            PgSyncFailure::UpgradeRequired
        );
    }

    #[test]
    fn format_info_decode_rejects_zero_block_size() {
        let mut info = FormatInfo::new(
            "volume".to_string(),
            "volume-uuid".to_string(),
            FormatInfoDataStore {
                endpoint: String::new(),
                bucket: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                region: String::new(),
            },
            1,
        )
        .expect("construct format info");
        info.block_size = 0;
        let bytes = serde_json::to_vec(&info).expect("serialize format info");

        assert!(matches!(
            decode_format_info(&bytes),
            Err(MetaError::InvalidFormatConfig(
                FormatInfoError::ZeroBlockSize
            ))
        ));
    }

    #[test]
    fn format_info_decode_rejects_tiny_nonzero_block_size() {
        let mut info = FormatInfo::new(
            "volume".to_string(),
            "volume-uuid".to_string(),
            FormatInfoDataStore {
                endpoint: String::new(),
                bucket: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                region: String::new(),
            },
            1,
        )
        .expect("construct format info");
        info.block_size = 1;
        let bytes = serde_json::to_vec(&info).expect("serialize format info");

        assert!(matches!(
            decode_format_info(&bytes),
            Err(MetaError::InvalidFormatConfig(
                FormatInfoError::BlockSizeBelowMinimum {
                    block_size_bytes: 1,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn format_info_decode_rejects_block_size_above_supported_maximum() {
        let mut info = FormatInfo::new(
            "volume".to_string(),
            "volume-uuid".to_string(),
            FormatInfoDataStore {
                endpoint: String::new(),
                bucket: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                region: String::new(),
            },
            4,
        )
        .expect("construct format info at maximum supported block size");
        info.block_size = 5 * 1024 * 1024;
        let bytes = serde_json::to_vec(&info).expect("serialize format info");

        assert!(matches!(
            decode_format_info(&bytes),
            Err(MetaError::InvalidFormatConfig(
                FormatInfoError::BlockSizeAboveMaximum {
                    block_size_bytes,
                    maximum_bytes,
                }
            )) if block_size_bytes == 5 * BYTES_PER_MIB
                && maximum_bytes == 4 * BYTES_PER_MIB
        ));
    }

    #[test]
    fn encryption_contract_errors_still_convert_to_meta_error() {
        let error = MetaError::from(schema::EncryptionContractError::Disabled);

        assert!(matches!(
            error,
            MetaError::InvalidFormatConfig(schema::FormatInfoError::Encryption(
                schema::EncryptionContractError::Disabled
            ))
        ));
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

        redb.set_counter(crate::pg_engine::LOCAL_PG_VERSION_COUNTER, 42)
            .expect("seed pg replay position");
        redb.set_counter(crate::pg_engine::LOCAL_PG_NAMESPACE_VERSION_COUNTER, 41)
            .expect("seed pg namespace position");
        redb.set_counter(crate::schema::counters::LOCAL_PG_FLUSHED_EVENT_SEQ, 7)
            .expect("seed local flush position");
        redb.set_counter("pgClientSlotPlusOne", 3)
            .expect("seed client slot");

        redb.set_counter(crate::pg_engine::LOCAL_PG_VERSION_COUNTER, 0)
            .expect("reset pg replay position");
        redb.set_counter(crate::pg_engine::LOCAL_PG_NAMESPACE_VERSION_COUNTER, 0)
            .expect("reset pg namespace position");

        assert_eq!(
            redb.get_counter(crate::pg_engine::LOCAL_PG_VERSION_COUNTER)
                .expect("pg replay position"),
            0
        );
        assert_eq!(
            redb.get_counter(crate::pg_engine::LOCAL_PG_NAMESPACE_VERSION_COUNTER)
                .expect("pg namespace position"),
            0
        );
        assert_eq!(
            redb.get_counter(crate::schema::counters::LOCAL_PG_FLUSHED_EVENT_SEQ)
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

    fn unclean_pg_image(name: &str, fence: u64, repair: u64) -> (impl Sized, RedbMetaEngine) {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let redb = RedbMetaEngine::open(dir.path().join(name)).expect("open redb");
        redb.set_counter(crate::pg_engine::LOCAL_PG_PUBLICATION_FENCE_COUNTER, fence)
            .expect("seed publication fence");
        redb.set_counter(
            crate::pg_engine::LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER,
            repair,
        )
        .expect("seed repair marker");
        redb.set_counter(crate::pg_engine::LOCAL_PG_VERSION_COUNTER, 42)
            .expect("seed pg replay position");
        redb.set_counter(crate::pg_engine::LOCAL_PG_NAMESPACE_VERSION_COUNTER, 41)
            .expect("seed pg namespace position");
        (dir, redb)
    }

    fn repair_state(redb: &RedbMetaEngine) -> (u64, u64, u64) {
        (
            redb.get_counter(crate::pg_engine::LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
                .unwrap(),
            redb.get_counter(crate::pg_engine::LOCAL_PG_VERSION_COUNTER)
                .unwrap(),
            redb.get_counter(crate::pg_engine::LOCAL_PG_NAMESPACE_VERSION_COUNTER)
                .unwrap(),
        )
    }

    #[test]
    fn fenced_unclean_pg_image_keeps_local_graph_without_postgres_repair() {
        let (_dir, redb) = unclean_pg_image("fenced.db", 1, 0);
        mark_unclean_pg_local_graph(&redb).expect("classify unclean image");
        assert_eq!(
            repair_state(&redb),
            (0, 42, 41),
            "a fenced image must reopen from its own cursors, not wait for PostgreSQL"
        );
    }

    #[test]
    fn unfenced_or_repairing_unclean_pg_image_still_requires_repair() {
        for (name, fence, repair) in [("legacy.db", 0, 0), ("repairing.db", 1, 1)] {
            let (_dir, redb) = unclean_pg_image(name, fence, repair);
            mark_unclean_pg_local_graph(&redb).expect("classify unclean image");
            assert_eq!(repair_state(&redb), (1, 0, 0), "{name}");
        }
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

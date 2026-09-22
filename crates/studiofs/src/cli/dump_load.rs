//! `studiofs dump` / `studiofs load` — PostgreSQL metadata manifest backup/restore.

use clap::Args;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use studiofs_meta::pg_engine::schema::ensure_studiofs_pg_schema;
use studiofs_meta::{
    MetaError, connect_studiofs_postgres, decode_format_info, ensure_studiofs_database,
    ensure_supported_pg_sync_format_version, open_meta_engine, validate_checkpoint_descriptor,
};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpVolume {
    vol: i64,
    name: String,
    uuid: String,
    block_size: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpCounter {
    name: String,
    value: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DumpEvent {
    batch_version: i64,
    event_type: String,
    event_payload: Vec<u8>,
    client_id: Option<String>,
    client_timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataDump {
    dump_version: u16,
    volume: DumpVolume,
    metadata_state_format_version: i16,
    metadata_state_version: i64,
    metadata_state_checkpoint_version: i64,
    metadata_state_snapshot: Vec<u8>,
    metadata_state_checkpoint_sha256: Vec<u8>,
    metadata_state_format_info: Vec<u8>,
    metadata_state_publication_state: String,
    metadata_state_generation: i64,
    counters: Vec<DumpCounter>,
    event_log: Vec<DumpEvent>,
}

#[derive(Debug, Deserialize)]
struct DumpHeader {
    dump_version: u16,
}

/// Typed failures found while validating a metadata dump.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum DumpValidationError {
    #[error(
        "metadata state versions cannot be negative (checkpoint: {checkpoint_version}, head: {head_version})"
    )]
    NegativeMetadataVersions {
        checkpoint_version: i64,
        head_version: i64,
    },
    #[error("checkpoint version {checkpoint_version} is ahead of metadata head {head_version}")]
    CheckpointAheadOfHead {
        checkpoint_version: i64,
        head_version: i64,
    },
    #[error("event log contains a negative batch version {batch_version}")]
    NegativeEventBatchVersion { batch_version: i64 },
    #[error(
        "event log batches are not ordered by version (previous: {previous}, current: {current})"
    )]
    UnorderedEventBatches { previous: i64, current: i64 },
    #[error("event batch {batch_version} is ahead of metadata head {head_version}")]
    BatchAheadOfHead {
        batch_version: i64,
        head_version: i64,
    },
    #[error("event log is missing tail batch {expected_version} before batch {batch_version}")]
    MissingTailBatch {
        expected_version: i64,
        batch_version: i64,
    },
    #[error(
        "event log is missing tail batch {expected_version} to reach metadata head {head_version}"
    )]
    MissingHeadTailBatch {
        expected_version: i64,
        head_version: i64,
    },
}

#[derive(Debug, Error)]
pub enum DumpLoadError {
    #[error("input/output error: {0}")]
    Io(#[from] std::io::Error),
    #[error("postgres operation '{operation}' failed: {source}")]
    PostgresOperation {
        operation: String,
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("metadata operation failed: {0}")]
    Meta(#[from] MetaError),
    #[error("json serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Validation(#[from] DumpValidationError),
    #[error("{0}")]
    Message(String),
}

type DumpLoadResult<T> = std::result::Result<T, DumpLoadError>;

fn postgres_operation(
    operation: impl Into<String>,
    source: tokio_postgres::Error,
) -> DumpLoadError {
    DumpLoadError::PostgresOperation {
        operation: operation.into(),
        source,
    }
}

#[derive(Debug, Args)]
#[command(about = "Dump metadata manifest from PostgreSQL (source of truth)")]
pub(crate) struct DumpArgs {
    /// Disk/volume name
    name: String,

    /// Output dump path (JSON)
    #[arg(long = "path-to-dump")]
    path_to_dump: PathBuf,
}

impl DumpArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let meta_dir = super::utils::get_default_meta_dir(&self.name)?;
        dump(&self.name, &meta_dir, &self.path_to_dump)?;
        Ok(())
    }
}

#[derive(Debug, Args)]
#[command(about = "Load metadata manifest into PostgreSQL")]
pub(crate) struct LoadArgs {
    /// Input dump path (JSON)
    #[arg(long = "path-to-dump")]
    path_to_dump: PathBuf,

    /// PostgreSQL address/DSN
    #[arg(long = "postgresql-address", alias = "postgresql-adress")]
    postgresql_address: String,
}

impl LoadArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        load(&self.path_to_dump, &self.postgresql_address)?;
        Ok(())
    }
}

fn load_remote_dump(meta_dsn: &str, volume_id: i64) -> DumpLoadResult<MetadataDump> {
    crate::runtime_bridge::block_on(async move {
        let metadata_dsn = ensure_studiofs_database(meta_dsn).await?.metadata_dsn;
        let (mut client, connection) = connect_studiofs_postgres(&metadata_dsn).await?;
        let connection_task = tokio::spawn(connection);

        let dump = async {
            // Keep checkpoint, counters, and event rows on one MVCC snapshot.
            let transaction = client
                .build_transaction()
                .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await
                .map_err(|source| postgres_operation("start dump snapshot transaction", source))?;

            let row = transaction
                .query_opt(
                    r#"
            SELECT
                v.vol,
                v.name,
                v.uuid,
                v.block_size,
                ms.metadata_format_version,
                ms.version,
                ms.checkpoint_version,
                ms.snapshot,
                ms.checkpoint_sha256,
                ms.format_info,
                ms.checkpoint_publication_state,
                ms.checkpoint_generation
            FROM volumes v
            JOIN metadata_state ms ON ms.vol = v.vol
            WHERE v.vol = $1
                    "#,
                    &[&volume_id],
                )
                .await
                .map_err(|source| postgres_operation("query metadata_state", source))?;

            let row = row.ok_or_else(|| {
                DumpLoadError::Message(format!("volume {volume_id} not found in postgres"))
            })?;

            let decode_state_row = |source| postgres_operation("decode metadata_state row", source);
            let volume = DumpVolume {
                vol: row.try_get(0).map_err(decode_state_row)?,
                name: row.try_get(1).map_err(decode_state_row)?,
                uuid: row.try_get(2).map_err(decode_state_row)?,
                block_size: row.try_get(3).map_err(decode_state_row)?,
            };
            let state_format_version: i16 = row.try_get(4).map_err(decode_state_row)?;
            let state_version: i64 = row.try_get(5).map_err(decode_state_row)?;
            let state_checkpoint_version: i64 = row.try_get(6).map_err(decode_state_row)?;
            let state_snapshot: Vec<u8> = row.try_get(7).map_err(decode_state_row)?;
            let state_checkpoint_sha256: Vec<u8> = row.try_get(8).map_err(decode_state_row)?;
            let state_format_info: Vec<u8> = row.try_get(9).map_err(decode_state_row)?;
            let state_publication_state: String = row.try_get(10).map_err(decode_state_row)?;
            let state_generation: i64 = row.try_get(11).map_err(decode_state_row)?;

            let counter_rows = transaction
                .query(
                    "SELECT name, value FROM counters WHERE vol = $1 ORDER BY name",
                    &[&volume.vol],
                )
                .await
                .map_err(|source| postgres_operation("query counters", source))?;
            let counters = counter_rows
                .into_iter()
                .map(|row| {
                    Ok(DumpCounter {
                        name: row.try_get(0)?,
                        value: row.try_get(1)?,
                    })
                })
                .collect::<Result<Vec<_>, tokio_postgres::Error>>()
                .map_err(|source| postgres_operation("decode counter rows", source))?;

            let event_rows = transaction
                .query(
                    r#"
            SELECT batch_version, event_type, event_payload, client_id, client_timestamp
            FROM event_log
            WHERE vol = $1 AND batch_version > $2
            ORDER BY seq_id
                    "#,
                    &[&volume.vol, &state_checkpoint_version],
                )
                .await
                .map_err(|source| postgres_operation("query event_log", source))?;
            let event_log = event_rows
                .into_iter()
                .map(|row| {
                    Ok(DumpEvent {
                        batch_version: row.try_get(0)?,
                        event_type: row.try_get(1)?,
                        event_payload: row.try_get(2)?,
                        client_id: row.try_get(3)?,
                        client_timestamp: row.try_get(4)?,
                    })
                })
                .collect::<Result<Vec<_>, tokio_postgres::Error>>()
                .map_err(|source| postgres_operation("decode event_log rows", source))?;

            let dump = MetadataDump {
                dump_version: 4,
                volume,
                metadata_state_format_version: state_format_version,
                metadata_state_version: state_version,
                metadata_state_checkpoint_version: state_checkpoint_version,
                metadata_state_snapshot: state_snapshot,
                metadata_state_checkpoint_sha256: state_checkpoint_sha256,
                metadata_state_format_info: state_format_info,
                metadata_state_publication_state: state_publication_state,
                metadata_state_generation: state_generation,
                counters,
                event_log,
            };
            transaction
                .commit()
                .await
                .map_err(|source| postgres_operation("commit dump snapshot transaction", source))?;
            Ok(dump)
        }
        .await;

        drop(client);
        connection_task.abort();
        let _ = connection_task.await;
        dump
    })
}

fn rebind_postgres_volume_id(format_info_bytes: &[u8], volume_id: i64) -> DumpLoadResult<Vec<u8>> {
    let mut format_info = decode_format_info(format_info_bytes)?;
    let postgres =
        format_info.metadata.postgres.as_mut().ok_or_else(|| {
            DumpLoadError::Message("dump format is not postgres-backed".to_string())
        })?;
    postgres.volume_id = volume_id;
    Ok(serde_json::to_vec(&format_info)?)
}

pub fn dump(name: &str, meta_dir: &Path, output: &Path) -> DumpLoadResult<()> {
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        return Err(DumpLoadError::Message(format!(
            "local metadata DB not found: {}",
            db_path.display()
        )));
    }

    // Ensure local pending mutations are flushed to PG before dumping from PG.
    let meta = open_meta_engine(&db_path)?;
    meta.flush_metadata()?;
    meta.sync_metadata()?;

    let format_info = meta.get_format_info()?;
    let pg = format_info
        .metadata
        .postgres
        .as_ref()
        .ok_or_else(|| DumpLoadError::Message("volume is not postgres-backed".to_string()))?;

    let dump = load_remote_dump(&pg.url, pg.volume_id)?;
    validate_dump(&dump)?;

    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(output, serde_json::to_vec_pretty(&dump)?)?;

    println!("✓ Dump created from PostgreSQL");
    println!("  Volume: {}", dump.volume.name);
    println!("  Output: {}", output.display());
    Ok(())
}

fn apply_dump(postgres_address: &str, dump: &MetadataDump) -> DumpLoadResult<i64> {
    crate::runtime_bridge::block_on(async move {
        let metadata_dsn = ensure_studiofs_database(postgres_address)
            .await?
            .metadata_dsn;
        let (mut client, connection) = connect_studiofs_postgres(&metadata_dsn).await?;
        let connection_task = tokio::spawn(connection);

        let result = async {
            ensure_studiofs_pg_schema(&client).await?;
            let tx = client
                .transaction()
                .await
                .map_err(|source| postgres_operation("start load transaction", source))?;
            tx.execute("SELECT pg_advisory_xact_lock($1)", &[&902_202_603_i64])
                .await
                .map_err(|source| postgres_operation("acquire load advisory lock", source))?;

            let row = tx
                .query_one(
                    r#"
            INSERT INTO volumes (name, uuid, block_size)
            VALUES ($1, $2, $3)
            ON CONFLICT (uuid) DO UPDATE
            SET name = EXCLUDED.name,
                block_size = EXCLUDED.block_size
            RETURNING vol
            "#,
                    &[&dump.volume.name, &dump.volume.uuid, &dump.volume.block_size],
                )
                .await
                .map_err(|source| postgres_operation("upsert volume", source))?;
            let target_vol: i64 = row
                .try_get(0)
                .map_err(|source| postgres_operation("decode upserted volume row", source))?;
            let format_info_bytes =
                rebind_postgres_volume_id(&dump.metadata_state_format_info, target_vol)?;

            let has_snapshot_v2_state: bool = tx
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM snapshot_catalog WHERE vol = $1) OR EXISTS (SELECT 1 FROM snapshot_policies WHERE vol = $1)",
                    &[&target_vol],
                )
                .await
                .map_err(|source| postgres_operation("check snapshot v2 state", source))?
                .try_get(0)
                .map_err(|source| postgres_operation("decode snapshot v2 state row", source))?;
            if has_snapshot_v2_state {
                return Err(DumpLoadError::Message(
                    "cannot load metadata over existing Snapshot v2 state; remove snapshots and policy first"
                        .to_string(),
                ));
            }

            tx.execute("DELETE FROM snapshot_policies WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|source| postgres_operation("clear snapshot policies", source))?;
            tx.execute("DELETE FROM snapshot_catalog WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|source| postgres_operation("clear snapshot catalog", source))?;
            tx.execute("DELETE FROM counters WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|source| postgres_operation("clear counters", source))?;
            tx.execute("DELETE FROM event_log WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|source| postgres_operation("clear event log", source))?;
            tx.execute("DELETE FROM metadata_state WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|source| postgres_operation("clear metadata_state", source))?;
            tx.execute(
                "INSERT INTO metadata_state (vol, metadata_format_version, version, checkpoint_version, snapshot, checkpoint_sha256, format_info, checkpoint_publication_state, checkpoint_generation) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                &[
                    &target_vol,
                    &dump.metadata_state_format_version,
                    &dump.metadata_state_version,
                    &dump.metadata_state_checkpoint_version,
                    &dump.metadata_state_snapshot,
                    &dump.metadata_state_checkpoint_sha256,
                    &format_info_bytes,
                    &dump.metadata_state_publication_state,
                    &dump.metadata_state_generation,
                ],
            )
            .await
            .map_err(|source| postgres_operation("insert metadata_state", source))?;

            for counter in &dump.counters {
                tx.execute(
                    "INSERT INTO counters (vol, name, value) VALUES ($1, $2, $3)",
                    &[&target_vol, &counter.name, &counter.value],
                )
                .await
                .map_err(|source| {
                    postgres_operation(format!("insert counter '{}'", counter.name), source)
                })?;
            }

            for event in &dump.event_log {
                tx.execute(
                    r#"
            INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id, client_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
                    &[
                        &target_vol,
                        &event.batch_version,
                        &event.event_type,
                        &event.event_payload,
                        &event.client_id,
                        &event.client_timestamp,
                    ],
                )
                .await
                .map_err(|source| {
                    postgres_operation(
                        format!(
                            "insert event_log row for vol={target_vol} batch_version={} event_type={}",
                            event.batch_version, event.event_type
                        ),
                        source,
                    )
                })?;
            }

            tx.commit()
                .await
                .map_err(|source| postgres_operation("commit load transaction", source))?;

            Ok::<i64, DumpLoadError>(target_vol)
        }
        .await;

        drop(client);
        connection_task.abort();
        let _ = connection_task.await;
        result
    })
}

fn validate_dump(dump: &MetadataDump) -> DumpLoadResult<()> {
    ensure_supported_pg_sync_format_version(Some(dump.metadata_state_format_version))?;
    decode_format_info(&dump.metadata_state_format_info)?;
    validate_checkpoint_descriptor(
        &dump.metadata_state_snapshot,
        Some(&dump.metadata_state_checkpoint_sha256),
        Some(&dump.metadata_state_publication_state),
        Some(dump.metadata_state_generation),
    )?;

    let checkpoint_version = dump.metadata_state_checkpoint_version;
    let head_version = dump.metadata_state_version;
    if checkpoint_version < 0 || head_version < 0 {
        return Err(DumpValidationError::NegativeMetadataVersions {
            checkpoint_version,
            head_version,
        }
        .into());
    }
    if checkpoint_version > head_version {
        return Err(DumpValidationError::CheckpointAheadOfHead {
            checkpoint_version,
            head_version,
        }
        .into());
    }

    let mut last_event_version = None;
    let mut contiguous_through = checkpoint_version;
    for event in &dump.event_log {
        let batch_version = event.batch_version;
        if batch_version < 0 {
            return Err(DumpValidationError::NegativeEventBatchVersion { batch_version }.into());
        }
        if let Some(previous) = last_event_version
            && batch_version < previous
        {
            return Err(DumpValidationError::UnorderedEventBatches {
                previous,
                current: batch_version,
            }
            .into());
        }
        last_event_version = Some(batch_version);

        if batch_version <= checkpoint_version {
            continue;
        }
        if batch_version > head_version {
            return Err(DumpValidationError::BatchAheadOfHead {
                batch_version,
                head_version,
            }
            .into());
        }
        if batch_version == contiguous_through {
            continue;
        }

        let expected_version = contiguous_through + 1;
        if batch_version != expected_version {
            return Err(DumpValidationError::MissingTailBatch {
                expected_version,
                batch_version,
            }
            .into());
        }
        contiguous_through = batch_version;
    }
    if contiguous_through < head_version {
        return Err(DumpValidationError::MissingHeadTailBatch {
            expected_version: contiguous_through + 1,
            head_version,
        }
        .into());
    }
    Ok(())
}

pub fn load(input: &Path, postgres_address: &str) -> DumpLoadResult<()> {
    let bytes = std::fs::read(input).map_err(|e| {
        DumpLoadError::Message(format!("failed to read dump {}: {e}", input.display()))
    })?;
    let header: DumpHeader = serde_json::from_slice(&bytes).map_err(|e| {
        DumpLoadError::Message(format!("failed to parse dump {}: {e}", input.display()))
    })?;

    if header.dump_version != 4 {
        return Err(DumpLoadError::Message(format!(
            "unsupported dump version: {}",
            header.dump_version
        )));
    }

    let dump: MetadataDump = serde_json::from_slice(&bytes).map_err(|e| {
        DumpLoadError::Message(format!("failed to parse dump {}: {e}", input.display()))
    })?;
    let volume_name = dump.volume.name.as_str();

    validate_dump(&dump)?;

    apply_dump(postgres_address, &dump)?;

    println!("✓ Dump loaded into PostgreSQL");
    println!("  Volume: {}", volume_name);
    println!("  Source: {}", input.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn dump_parses_name_and_output_path() {
        let cli =
            crate::cli::parse_for_test(&["dump", "vol", "--path-to-dump", "reports/metadata.json"])
                .unwrap();

        match cli.command {
            crate::cli::Commands::Dump(DumpArgs { name, path_to_dump }) => {
                assert_eq!(name, "vol");
                assert_eq!(path_to_dump, PathBuf::from("reports/metadata.json"));
            }
            other => panic!("expected Dump, got {other:?}"),
        }
    }

    #[test]
    fn load_parses_canonical_and_legacy_postgres_flags() {
        let canonical = crate::cli::parse_for_test(&[
            "load",
            "--path-to-dump",
            "reports/metadata.json",
            "--postgresql-address",
            "postgres://user:pass@127.0.0.1:5432",
        ])
        .unwrap();
        match canonical.command {
            crate::cli::Commands::Load(LoadArgs {
                path_to_dump,
                postgresql_address,
            }) => {
                assert_eq!(path_to_dump, PathBuf::from("reports/metadata.json"));
                assert_eq!(postgresql_address, "postgres://user:pass@127.0.0.1:5432");
            }
            other => panic!("expected Load, got {other:?}"),
        }

        let legacy = crate::cli::parse_for_test(&[
            "load",
            "--path-to-dump",
            "reports/metadata.json",
            "--postgresql-adress",
            "postgres://user:pass@127.0.0.1:5432",
        ])
        .unwrap();
        match legacy.command {
            crate::cli::Commands::Load(LoadArgs {
                postgresql_address, ..
            }) => assert_eq!(postgresql_address, "postgres://user:pass@127.0.0.1:5432"),
            other => panic!("expected Load, got {other:?}"),
        }
    }

    #[test]
    fn load_rejects_legacy_dump_before_full_deserialization() {
        let input = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(input.path(), br#"{"dump_version":3}"#).unwrap();

        let err = load(input.path(), "postgres://unused").unwrap_err();
        assert!(matches!(
            err,
            DumpLoadError::Message(message) if message.contains("unsupported dump version: 3")
        ));
    }

    fn test_dump() -> MetadataDump {
        let snapshot = vec![1, 2, 3];
        MetadataDump {
            dump_version: 4,
            volume: DumpVolume {
                vol: 1,
                name: "test".to_string(),
                uuid: "test".to_string(),
                block_size: 4096,
            },
            metadata_state_format_version: 5,
            metadata_state_version: 1,
            metadata_state_checkpoint_version: 1,
            metadata_state_snapshot: snapshot.clone(),
            metadata_state_checkpoint_sha256: Sha256::digest(&snapshot).to_vec(),
            metadata_state_format_info: serde_json::to_vec(
                &studiofs_meta::FormatInfo::new(
                    "test".to_string(),
                    "test-uuid".to_string(),
                    studiofs_meta::FormatInfoDataStore {
                        endpoint: "http://localhost:9000".to_string(),
                        bucket: "test-bucket".to_string(),
                        access_key: "test".to_string(),
                        secret_key: "test".to_string(),
                        region: "us-east-1".to_string(),
                    },
                    4,
                )
                .unwrap(),
            )
            .unwrap(),
            metadata_state_publication_state: "published".to_string(),
            metadata_state_generation: 1,
            counters: Vec::new(),
            event_log: Vec::new(),
        }
    }

    fn test_event(batch_version: i64) -> DumpEvent {
        DumpEvent {
            batch_version,
            event_type: "test".to_string(),
            event_payload: vec![1, 2, 3],
            client_id: None,
            client_timestamp: None,
        }
    }

    #[test]
    fn dump_checkpoint_validation_rejects_checksum_mismatch() {
        let mut dump = test_dump();
        dump.metadata_state_checkpoint_sha256[0] ^= 1;

        let err = validate_dump(&dump).unwrap_err();
        assert!(matches!(
            err,
            DumpLoadError::Meta(MetaError::InvalidCheckpointDescriptor {
                reason: studiofs_meta::CheckpointDescriptorFailure::Checksum
            })
        ));
    }

    #[test]
    fn dump_checkpoint_validation_rejects_legacy_metadata_format() {
        let mut dump = test_dump();
        dump.metadata_state_format_version = 4;

        let err = validate_dump(&dump).unwrap_err();
        assert!(matches!(
            err,
            DumpLoadError::Meta(MetaError::UnsupportedFormatVersion {
                found: 4,
                expected: 5
            })
        ));
    }

    #[test]
    fn dump_checkpoint_validation_rejects_incomplete_publication() {
        let mut dump = test_dump();
        dump.metadata_state_publication_state = "pending".to_string();

        let err = validate_dump(&dump).unwrap_err();
        assert!(matches!(
            err,
            DumpLoadError::Meta(MetaError::InvalidCheckpointDescriptor {
                reason: studiofs_meta::CheckpointDescriptorFailure::PublicationState
            })
        ));
    }

    #[test]
    fn load_rebinds_persisted_postgres_volume_id() {
        let mut format_info = studiofs_meta::FormatInfo::new(
            "test".to_string(),
            "test-uuid".to_string(),
            studiofs_meta::FormatInfoDataStore {
                endpoint: "http://localhost:9000".to_string(),
                bucket: "test-bucket".to_string(),
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                region: "us-east-1".to_string(),
            },
            4,
        )
        .unwrap();
        format_info.metadata.backend = studiofs_meta::MetadataBackend::Postgres;
        format_info.metadata.postgres = Some(studiofs_meta::PostgresConfig {
            url: "postgres://localhost".to_string(),
            volume_id: 7,
        });
        let bytes = serde_json::to_vec(&format_info).unwrap();

        let rebound = rebind_postgres_volume_id(&bytes, 42).unwrap();
        let rebound = studiofs_meta::decode_format_info(&rebound).unwrap();

        assert_eq!(rebound.metadata.postgres.unwrap().volume_id, 42);
    }

    #[test]
    fn valid_dump_passes_manifest_validation() {
        validate_dump(&test_dump()).expect("valid dump should pass manifest validation");
    }

    #[test]
    fn dump_validation_rejects_missing_event_log_tail() {
        let mut dump = test_dump();
        dump.metadata_state_checkpoint_version = 100;
        dump.metadata_state_version = 101;

        let err = validate_dump(&dump).unwrap_err();
        assert_eq!(
            err.to_string(),
            "event log is missing tail batch 101 to reach metadata head 101"
        );

        assert!(matches!(
            err,
            DumpLoadError::Validation(DumpValidationError::MissingHeadTailBatch {
                expected_version: 101,
                head_version: 101,
            })
        ));
    }

    #[test]
    fn dump_validation_rejects_gap_between_event_log_batches() {
        let mut dump = test_dump();
        dump.metadata_state_checkpoint_version = 100;
        dump.metadata_state_version = 103;
        dump.event_log = vec![test_event(101), test_event(103)];

        let err = validate_dump(&dump).unwrap_err();

        assert!(matches!(
            err,
            DumpLoadError::Validation(DumpValidationError::MissingTailBatch {
                expected_version: 102,
                batch_version: 103,
            })
        ));
    }

    #[test]
    fn dump_validation_accepts_contiguous_event_log_batches() {
        let mut dump = test_dump();
        dump.metadata_state_checkpoint_version = 1;
        dump.metadata_state_version = 3;
        dump.event_log = vec![test_event(1), test_event(2), test_event(2), test_event(3)];

        validate_dump(&dump).expect("complete event batches should pass validation");
    }

    #[test]
    fn dump_validation_rejects_negative_metadata_versions() {
        for (checkpoint_version, head_version) in [(-1, 0), (0, -1)] {
            let mut dump = test_dump();
            dump.metadata_state_checkpoint_version = checkpoint_version;
            dump.metadata_state_version = head_version;

            let err = validate_dump(&dump).unwrap_err();

            assert!(matches!(
                err,
                DumpLoadError::Validation(DumpValidationError::NegativeMetadataVersions {
                    checkpoint_version: actual_checkpoint,
                    head_version: actual_head,
                }) if actual_checkpoint == checkpoint_version && actual_head == head_version
            ));
        }
    }

    #[test]
    fn dump_validation_rejects_checkpoint_ahead_of_metadata_head() {
        let mut dump = test_dump();
        dump.metadata_state_checkpoint_version = 105;
        dump.metadata_state_version = 100;

        let err = validate_dump(&dump).unwrap_err();

        assert!(matches!(
            err,
            DumpLoadError::Validation(DumpValidationError::CheckpointAheadOfHead {
                checkpoint_version: 105,
                head_version: 100,
            })
        ));
    }

    #[test]
    fn dump_validation_rejects_negative_event_batch_version() {
        let mut dump = test_dump();
        dump.metadata_state_checkpoint_version = 0;
        dump.metadata_state_version = 1;
        dump.event_log = vec![test_event(-1)];

        let err = validate_dump(&dump).unwrap_err();

        assert!(matches!(
            err,
            DumpLoadError::Validation(DumpValidationError::NegativeEventBatchVersion {
                batch_version: -1,
            })
        ));
    }

    #[test]
    fn dump_validation_rejects_unordered_event_batches() {
        let mut dump = test_dump();
        dump.metadata_state_checkpoint_version = 100;
        dump.metadata_state_version = 102;
        dump.event_log = vec![test_event(101), test_event(102), test_event(101)];

        let err = validate_dump(&dump).unwrap_err();

        assert!(matches!(
            err,
            DumpLoadError::Validation(DumpValidationError::UnorderedEventBatches {
                previous: 102,
                current: 101,
            })
        ));
    }

    #[test]
    fn dump_validation_rejects_event_batch_ahead_of_metadata_head() {
        let mut dump = test_dump();
        dump.metadata_state_checkpoint_version = 100;
        dump.metadata_state_version = 101;
        dump.event_log = vec![test_event(101), test_event(102)];

        let err = validate_dump(&dump).unwrap_err();

        assert!(matches!(
            err,
            DumpLoadError::Validation(DumpValidationError::BatchAheadOfHead {
                batch_version: 102,
                head_version: 101,
            })
        ));
    }

    #[test]
    fn dump_validation_accepts_max_version_when_tail_is_complete() {
        let mut complete_dump = test_dump();
        complete_dump.metadata_state_checkpoint_version = i64::MAX - 1;
        complete_dump.metadata_state_version = i64::MAX;
        complete_dump.event_log = vec![test_event(i64::MAX)];
        validate_dump(&complete_dump).expect("maximum batch version should validate");
    }

    #[test]
    fn dump_validation_rejects_missing_tail_at_max_version() {
        let mut incomplete_dump = test_dump();
        incomplete_dump.metadata_state_checkpoint_version = i64::MAX - 1;
        incomplete_dump.metadata_state_version = i64::MAX;
        let err = validate_dump(&incomplete_dump).unwrap_err();
        assert!(matches!(
            err,
            DumpLoadError::Validation(DumpValidationError::MissingHeadTailBatch {
                expected_version,
                head_version,
            }) if expected_version == i64::MAX && head_version == i64::MAX
        ));
    }

    fn test_pg_dsn() -> Option<String> {
        if let Ok(dsn) = std::env::var("STUDIOFS_TEST_PG_DSN")
            && !dsn.trim().is_empty()
        {
            return Some(dsn);
        }

        dotenvy::from_path_iter(studiofs_test_support::workspace_root().join(".env"))
            .ok()?
            .filter_map(Result::ok)
            .find_map(|(key, value)| (key == "STUDIOFS_TEST_PG_DSN").then_some(value))
    }

    // Keep this guard in the synchronous test scope; Drop calls `Runtime::block_on`.
    struct PgTestVolumeCleanup<'a> {
        runtime: &'a tokio::runtime::Runtime,
        metadata_dsn: String,
        volume_id: i64,
        connection_tasks:
            Vec<tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>>,
        loader: Option<std::thread::JoinHandle<DumpLoadResult<MetadataDump>>>,
    }

    impl Drop for PgTestVolumeCleanup<'_> {
        fn drop(&mut self) {
            let connection_tasks = std::mem::take(&mut self.connection_tasks);
            if !connection_tasks.is_empty() {
                self.runtime.block_on(async move {
                    for task in &connection_tasks {
                        task.abort();
                    }
                    for task in connection_tasks {
                        let _ = task.await;
                    }
                });
            }
            if let Some(loader) = self.loader.take() {
                let _ = loader.join();
            }
            self.runtime.block_on(async {
                let Ok((client, connection)) = connect_studiofs_postgres(&self.metadata_dsn).await
                else {
                    return;
                };
                let connection_task = tokio::spawn(connection);
                let _ = client
                    .execute("DELETE FROM volumes WHERE vol = $1", &[&self.volume_id])
                    .await;
                connection_task.abort();
                let _ = connection_task.await;
            });
        }
    }

    #[test]
    #[ignore = "requires the PostgreSQL test database"]
    fn dump_snapshot_preserves_tail_pruned_after_metadata_read() {
        use std::time::Duration;

        let runtime = tokio::runtime::Runtime::new().expect("build PostgreSQL test runtime");

        let raw_dsn = test_pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
        let metadata_dsn = runtime
            .block_on(ensure_studiofs_database(&raw_dsn))
            .expect("resolve PostgreSQL test database")
            .metadata_dsn;

        let id = uuid::Uuid::new_v4().to_string();
        let mut dump = test_dump();
        dump.volume.name = format!("sfs-723-{id}");
        dump.volume.uuid = id;
        dump.metadata_state_checkpoint_version = 100;
        dump.metadata_state_version = 101;
        dump.counters.push(DumpCounter {
            name: "sfs723-test-counter".to_string(),
            value: 11,
        });
        dump.event_log = vec![test_event(99), test_event(101)];
        let mut format_info =
            decode_format_info(&dump.metadata_state_format_info).expect("test format info");
        format_info.metadata.backend = studiofs_meta::MetadataBackend::Postgres;
        format_info.metadata.postgres = Some(studiofs_meta::PostgresConfig {
            url: "test-postgres-dsn".to_string(),
            volume_id: 1,
        });
        dump.metadata_state_format_info =
            serde_json::to_vec(&format_info).expect("serialize test format info");
        let volume_id = apply_dump(&metadata_dsn, &dump).expect("seed PostgreSQL test volume");
        let mut cleanup = PgTestVolumeCleanup {
            runtime: &runtime,
            metadata_dsn: metadata_dsn.clone(),
            volume_id,
            connection_tasks: Vec::new(),
            loader: None,
        };

        runtime.block_on(async {
            let (mut client, connection) = connect_studiofs_postgres(&metadata_dsn)
                .await
                .expect("connect to PostgreSQL test database");
            cleanup.connection_tasks.push(tokio::spawn(connection));

            let (mut lock_client, lock_connection) = connect_studiofs_postgres(&metadata_dsn)
                .await
                .expect("connect to PostgreSQL lock session");
            cleanup
                .connection_tasks
                .push(tokio::spawn(lock_connection));
            let lock_tx = lock_client
                .transaction()
                .await
                .expect("begin PostgreSQL lock transaction");
            lock_tx
                .batch_execute("LOCK TABLE counters IN ACCESS EXCLUSIVE MODE")
                .await
                .expect("lock counters for deterministic dump pause");

            let loader_dsn = metadata_dsn.clone();
            cleanup.loader = Some(std::thread::spawn(move || {
                load_remote_dump(&loader_dsn, volume_id)
            }));
            let wait_for_counter_query = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let waiting: bool = client
                        .query_one(
                            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'counters'::regclass AND mode = 'AccessShareLock' AND NOT granted)",
                            &[],
                        )
                        .await
                        .expect("inspect PostgreSQL dump query state")
                        .try_get(0)
                        .expect("decode PostgreSQL dump query state");
                    if waiting {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
            wait_for_counter_query.expect("dump did not reach its counters query");

            let newer_snapshot = vec![4, 5, 6];
            let newer_checksum = Sha256::digest(&newer_snapshot).to_vec();
            let prune_tx = client
                .transaction()
                .await
                .expect("begin checkpoint publication and prune transaction");
            prune_tx
                .execute(
                    "UPDATE metadata_state SET checkpoint_version = 101, snapshot = $2, checkpoint_sha256 = $3, checkpoint_generation = 2, event_log_prune_floor = 101 WHERE vol = $1",
                    &[&volume_id, &newer_snapshot, &newer_checksum],
                )
                .await
                .expect("publish test checkpoint");
            let pruned = prune_tx
                .execute(
                    "DELETE FROM event_log WHERE vol = $1 AND batch_version <= 101",
                    &[&volume_id],
                )
                .await
                .expect("prune test event tail");
            prune_tx
                .commit()
                .await
                .expect("commit checkpoint publication and prune");
            assert_eq!(pruned, 2);
            lock_tx.commit().await.expect("release counters table lock");

            let captured = cleanup
                .loader
                .take()
                .expect("dump loader thread should be running")
                .join()
                .expect("dump loader thread panicked")
                .expect("load remote dump");
            validate_dump(&captured).expect("captured dump should be valid");
            assert_eq!(captured.metadata_state_checkpoint_version, 100);
            assert_eq!(captured.metadata_state_version, 101);
            assert_eq!(captured.event_log.len(), 1);
            assert_eq!(captured.event_log[0].batch_version, 101);
        });
    }
}

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
    #[error(
        "cannot load metadata over existing Snapshot v2 state ({}); remove snapshots and policy first",
        .tables.join(", ")
    )]
    TargetHasSnapshotV2State { tables: Vec<&'static str> },
    #[error(
        "cannot load metadata over existing reachability or payload upload state ({}); stop every client and the old metadata server, then load into a new, empty metadata database",
        .tables.join(", ")
    )]
    TargetHasAccountingState { tables: Vec<&'static str> },
    #[error(
        "cannot load metadata over existing trash authority state ({}); a replacement would drop terminal receipts that delayed clients rely on, so load into a new, empty metadata database",
        .tables.join(", ")
    )]
    TargetHasTrashAuthorityState { tables: Vec<&'static str> },
    #[error("write dump output '{}': {source:#}", .path.display())]
    WriteOutput {
        path: PathBuf,
        #[source]
        source: anyhow::Error,
    },
    #[error("{0}")]
    Message(String),
}

/// Snapshot v2 tables whose rows describe snapshots of the load target.
const SNAPSHOT_V2_TABLES: [&str; 2] = ["snapshot_catalog", "snapshot_policies"];

/// Version-keyed reachability accounting, the payload upload ledger and the
/// payload GC queue and sweep checkpoint. A dump
/// carries none of them, so rows retained for the target would describe a
/// different history than the restored metadata and could let GC delete live
/// payloads.
const ACCOUNTING_TABLES: [&str; 8] = [
    "snapshot_current_references",
    "snapshot_object_reachability",
    "snapshot_reachability_batches",
    "snapshot_reachability_bootstrap",
    "snapshot_payload_upload_objects",
    "snapshot_payload_upload_watermarks",
    "snapshot_payload_gc_candidates",
    "snapshot_payload_sweep_checkpoints",
];

/// Shared receipt authority and exact-retry receipts. Load rebuilds the
/// authority from the dump; stale rows could contradict it or turn a new
/// publication into a skipped "retry".
const TRASH_AUTHORITY_TABLES: [&str; 2] = ["trash_entry_authority", "trash_publication_receipts"];

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

    write_dump_output(output, &dump)?;

    println!("✓ Dump created from PostgreSQL");
    println!("  Volume: {}", dump.volume.name);
    println!("  Output: {}", output.display());
    Ok(())
}

fn write_dump_output(output: &Path, dump: &MetadataDump) -> DumpLoadResult<()> {
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    super::utils::write_json_file_atomically(output, dump).map_err(|source| {
        DumpLoadError::WriteOutput {
            path: output.to_path_buf(),
            source,
        }
    })
}

fn apply_dump(postgres_address: &str, dump: &MetadataDump) -> DumpLoadResult<i64> {
    crate::runtime_bridge::block_on(async move {
        let metadata_dsn = ensure_studiofs_database(postgres_address)
            .await?
            .metadata_dsn;
        let (mut client, connection) = connect_studiofs_postgres(&metadata_dsn).await?;
        let connection_task = tokio::spawn(connection);

        let result = async {
            // Short schema transaction: DDL table locks must not span the load.
            let schema_tx = client
                .transaction()
                .await
                .map_err(|source| postgres_operation("start schema transaction", source))?;
            ensure_studiofs_pg_schema(&schema_tx).await?;
            schema_tx
                .commit()
                .await
                .map_err(|source| postgres_operation("commit schema transaction", source))?;
            let tx = client
                .transaction()
                .await
                .map_err(|source| postgres_operation("start load transaction", source))?;
            tx.execute(
                "SELECT pg_advisory_xact_lock($1)",
                &[&super::init::INIT_SCHEMA_ADVISORY_LOCK],
            )
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
            // Fence new FK-backed accounting rows until the replacement commits.
            tx.query_one("SELECT vol FROM volumes WHERE vol = $1 FOR UPDATE", &[&target_vol])
                .await
                .map_err(|source| postgres_operation("lock load target volume", source))?;
            let previous_state = tx
                .query_opt(
                    "SELECT write_requirement, metadata_format_version FROM metadata_state WHERE vol = $1",
                    &[&target_vol],
                )
                .await
                .map_err(|source| postgres_operation("read load target format", source))?;
            if let Some(row) = &previous_state {
                ensure_supported_pg_sync_format_version(row.get(1))?;
            }
            let format_info_bytes =
                rebind_postgres_volume_id(&dump.metadata_state_format_info, target_vol)?;

            let snapshot_v2_tables =
                tables_with_volume_rows(&tx, target_vol, &SNAPSHOT_V2_TABLES).await?;
            if !snapshot_v2_tables.is_empty() {
                return Err(DumpLoadError::TargetHasSnapshotV2State {
                    tables: snapshot_v2_tables,
                });
            }
            let accounting_tables =
                tables_with_volume_rows(&tx, target_vol, &ACCOUNTING_TABLES).await?;
            if !accounting_tables.is_empty() {
                return Err(DumpLoadError::TargetHasAccountingState {
                    tables: accounting_tables,
                });
            }

            let trash_tables =
                tables_with_volume_rows(&tx, target_vol, &TRASH_AUTHORITY_TABLES).await?;
            if !trash_tables.is_empty() {
                return Err(DumpLoadError::TargetHasTrashAuthorityState {
                    tables: trash_tables,
                });
            }

            // Preserve the existing policy and write requirement.
            if previous_state.is_none() {
                studiofs_meta::pg_engine::insert_initial_volume_protocol(&tx, target_vol, "studiofs load")
                    .await?;
            }
            let write_requirement: i16 = previous_state.map_or(1, |row| row.get(0));
            let session =
                studiofs_meta::pg_engine::PublicationSession::begin(&tx, target_vol).await?;
            // The volume row lock blocks new FK-backed rows, so the guarded
            // snapshot and accounting tables stay empty until commit.
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
                "INSERT INTO metadata_state (vol, metadata_format_version, version, checkpoint_version, snapshot, checkpoint_sha256, format_info, checkpoint_publication_state, checkpoint_generation, write_requirement) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
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
                    &write_requirement,
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

            let events = dump
                .event_log
                .iter()
                .map(|event| {
                    (
                        event.batch_version,
                        event.event_type.as_str(),
                        event.event_payload.as_slice(),
                    )
                })
                .collect::<Vec<_>>();
            studiofs_meta::pg_engine::seed_retained_authority(
                &tx,
                target_vol,
                &dump.metadata_state_snapshot,
                dump.metadata_state_checkpoint_version,
                &events,
            )
            .await?;

            session.finish(&tx, target_vol).await?;
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

/// Returns the tables that hold at least one row for `vol`, in input order.
async fn tables_with_volume_rows(
    tx: &tokio_postgres::Transaction<'_>,
    vol: i64,
    tables: &[&'static str],
) -> DumpLoadResult<Vec<&'static str>> {
    // Table names are compile-time constants, never user input.
    let probes = tables
        .iter()
        .map(|table| format!("EXISTS (SELECT 1 FROM {table} WHERE vol = $1)"))
        .collect::<Vec<_>>()
        .join(", ");
    let row = tx
        .query_one(&format!("SELECT {probes}"), &[&vol])
        .await
        .map_err(|source| {
            postgres_operation(
                format!("check load target rows in {}", tables.join(", ")),
                source,
            )
        })?;
    let mut populated = Vec::new();
    for (index, table) in tables.iter().enumerate() {
        let has_rows: bool = row.try_get(index).map_err(|source| {
            postgres_operation(format!("decode load target row check for {table}"), source)
        })?;
        if has_rows {
            populated.push(*table);
        }
    }
    Ok(populated)
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
    let dump = parse_dump(input, &bytes)?;
    let volume_name = dump.volume.name.as_str();

    validate_dump(&dump)?;

    apply_dump(postgres_address, &dump)?;

    println!("✓ Dump loaded into PostgreSQL");
    println!("  Volume: {}", volume_name);
    println!("  Source: {}", input.display());
    Ok(())
}

fn parse_dump(input: &Path, bytes: &[u8]) -> DumpLoadResult<MetadataDump> {
    let header: DumpHeader = serde_json::from_slice(bytes).map_err(|e| {
        DumpLoadError::Message(format!("failed to parse dump {}: {e}", input.display()))
    })?;

    if header.dump_version != 4 {
        return Err(DumpLoadError::Message(format!(
            "unsupported dump version: {}",
            header.dump_version
        )));
    }

    serde_json::from_slice(bytes).map_err(|e| {
        DumpLoadError::Message(format!("failed to parse dump {}: {e}", input.display()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    /// SFS-1002 baseline: a version-4 dump built from the pinned studiofs-meta
    /// 0.2.5 fixtures, with a published checkpoint at version 4 and a replay
    /// tail of batches 5..=7.
    fn baseline_dump_fixture() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dump-v4-0.2.5.json")
    }

    fn meta_baseline_fixture(name: &str) -> Vec<u8> {
        std::fs::read(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../studiofs-meta/tests/fixtures/baseline-0.2.5")
                .join(name),
        )
        .unwrap()
    }

    #[test]
    #[ignore = "rewrites pinned fixtures; set STUDIOFS_REGENERATE_BASELINE_FIXTURES=1"]
    fn regenerate_baseline_dump_fixture() {
        if std::env::var_os("STUDIOFS_REGENERATE_BASELINE_FIXTURES").is_none() {
            return;
        }
        let format_info = meta_baseline_fixture("format-info.json");
        let info = decode_format_info(&format_info).unwrap();
        let checkpoint = meta_baseline_fixture("serving-checkpoint.bin");
        let event = |batch_version, event_type: &str, file: &str| DumpEvent {
            batch_version,
            event_type: event_type.to_string(),
            event_payload: meta_baseline_fixture(&format!("events/{file}")),
            client_id: Some("baseline-fixture-client".to_string()),
            client_timestamp: Some(1_764_000_000_000),
        };
        let dump = MetadataDump {
            dump_version: 4,
            volume: DumpVolume {
                vol: 1,
                name: info.name.clone(),
                uuid: info.uuid.clone(),
                block_size: i32::try_from(info.block_size).unwrap(),
            },
            metadata_state_format_version: studiofs_meta::schema::CURRENT_FORMAT_VERSION as i16,
            metadata_state_version: 7,
            metadata_state_checkpoint_version: 4,
            metadata_state_checkpoint_sha256: Sha256::digest(&checkpoint).to_vec(),
            metadata_state_snapshot: checkpoint,
            metadata_state_format_info: format_info,
            metadata_state_publication_state: "published".to_string(),
            metadata_state_generation: 2,
            counters: vec![DumpCounter {
                name: "nextInode".to_string(),
                value: 12,
            }],
            event_log: vec![
                event(
                    5,
                    "set_volume_settings",
                    "tag-21-set-volume-settings-v3.bin",
                ),
                event(6, "set_xattr", "tag-10-set-xattr.bin"),
                event(7, "add_entry", "tag-03-add-entry.bin"),
            ],
        };
        write_dump_output(&baseline_dump_fixture(), &dump).unwrap();
    }

    #[test]
    fn baseline_dump_fixture_decodes_with_current_binary() {
        let path = baseline_dump_fixture();
        let mut dump = parse_dump(&path, &std::fs::read(&path).unwrap()).unwrap();
        // The pinned dump predates adoption: load refuses its format-6 volume.
        assert!(matches!(
            validate_dump(&dump),
            Err(DumpLoadError::Meta(MetaError::NeedsMigration { found: 6 }))
        ));
        let current = studiofs_meta::schema::CURRENT_FORMAT_VERSION;
        let mut header: studiofs_meta::schema::FormatInfo =
            serde_json::from_slice(&dump.metadata_state_format_info).unwrap();
        header.format_version = current;
        dump.metadata_state_format_info = serde_json::to_vec(&header).unwrap();
        dump.metadata_state_format_version = i16::try_from(current).unwrap();
        validate_dump(&dump).unwrap();
        assert_eq!(dump.dump_version, 4);
        assert_eq!(
            (
                dump.metadata_state_checkpoint_version,
                dump.metadata_state_version
            ),
            (4, 7)
        );
        assert_eq!(
            dump.event_log
                .iter()
                .map(|event| event.batch_version)
                .collect::<Vec<_>>(),
            vec![5, 6, 7]
        );
        assert_eq!(
            dump.metadata_state_snapshot,
            meta_baseline_fixture("serving-checkpoint.bin")
        );
    }

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

    #[test]
    fn accounting_rejection_names_blocking_tables_and_migration_target() {
        let err = DumpLoadError::TargetHasAccountingState {
            tables: vec![
                "snapshot_reachability_batches",
                "snapshot_payload_upload_objects",
            ],
        };

        assert_eq!(
            err.to_string(),
            "cannot load metadata over existing reachability or payload upload state \
             (snapshot_reachability_batches, snapshot_payload_upload_objects); stop every \
             client and the old metadata server, then load into a new, empty metadata database"
        );
    }

    fn test_dump() -> MetadataDump {
        // An empty serving checkpoint: six empty postcard sequences.
        let snapshot = vec![0; 6];
        MetadataDump {
            dump_version: 4,
            volume: DumpVolume {
                vol: 1,
                name: "test".to_string(),
                uuid: "test".to_string(),
                block_size: 4096,
            },
            metadata_state_format_version: studiofs_meta::schema::CURRENT_FORMAT_VERSION as i16,
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

    #[test]
    #[ignore = "requires the PostgreSQL test database"]
    fn load_refuses_to_adopt_an_existing_format6_volume() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let raw_dsn = test_pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
        let metadata_dsn = runtime
            .block_on(ensure_studiofs_database(&raw_dsn))
            .unwrap()
            .metadata_dsn;
        let schema = crate::cli::pg_test_schema::PgTestSchema::new(&metadata_dsn).unwrap();
        let mut dump = test_dump();
        let mut header = decode_format_info(&dump.metadata_state_format_info).unwrap();
        header.metadata.backend = studiofs_meta::MetadataBackend::Postgres;
        header.metadata.postgres = Some(studiofs_meta::PostgresConfig {
            url: "test-postgres-dsn".into(),
            volume_id: 1,
        });
        dump.metadata_state_format_info = serde_json::to_vec(&header).unwrap();
        let volume = apply_dump(schema.dsn(), &dump).unwrap();
        let (client, connection) = runtime
            .block_on(studiofs_meta::connect_studiofs_postgres(schema.dsn()))
            .unwrap();
        let task = runtime.spawn(connection);
        // A 0.2.4 volume has no policy row; the downgrade fence admits it.
        for sql in [
            "DELETE FROM volume_protocol WHERE vol = $1",
            "UPDATE metadata_state SET metadata_format_version = 6 WHERE vol = $1",
        ] {
            runtime.block_on(client.execute(sql, &[&volume])).unwrap();
        }
        assert!(matches!(
            apply_dump(schema.dsn(), &dump),
            Err(DumpLoadError::Meta(MetaError::NeedsMigration { found: 6 }))
        ));
        let row = runtime
            .block_on(client.query_one(
                "SELECT metadata_format_version FROM metadata_state WHERE vol = $1",
                &[&volume],
            ))
            .unwrap();
        assert_eq!(row.get::<_, i16>(0), 6);
        task.abort();
    }

    #[test]
    fn dump_output_replaces_previous_backup_with_valid_dump() {
        let directory = studiofs_test_support::workspace_target_tempdir("sfs-870-");
        let output = directory.path().join("dump.json");
        let mut previous = test_dump();
        previous.volume.name = "previous".to_string();
        std::fs::write(&output, serde_json::to_vec_pretty(&previous).unwrap()).unwrap();
        let dump = test_dump();

        write_dump_output(&output, &dump).unwrap();

        let bytes = std::fs::read(&output).unwrap();
        let restored: MetadataDump = serde_json::from_slice(&bytes).unwrap();
        validate_dump(&restored).unwrap();
        assert_eq!(bytes, serde_json::to_vec_pretty(&dump).unwrap());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn dump_output_serialization_failure_preserves_previous_backup() {
        struct FailingSerialize;
        impl Serialize for FailingSerialize {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("injected serialization failure"))
            }
        }
        let directory = studiofs_test_support::workspace_target_tempdir("sfs-870-");
        let output = directory.path().join("dump.json");
        let previous = serde_json::to_vec_pretty(&test_dump()).unwrap();
        std::fs::write(&output, &previous).unwrap();

        let error = super::super::utils::write_json_file_atomically(&output, &FailingSerialize)
            .unwrap_err();

        let message = format!("{error:#}");
        assert!(
            message.contains("serialize temporary file for"),
            "{message}"
        );
        assert!(
            message.contains("injected serialization failure"),
            "{message}"
        );
        assert_eq!(std::fs::read(&output).unwrap(), previous);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn dump_output_replace_failure_preserves_destination_and_reports_path() {
        let directory = studiofs_test_support::workspace_target_tempdir("sfs-870-");
        // A non-empty directory at the destination makes the final rename fail
        // after the temporary file was fully written and synced.
        let output = directory.path().join("dump.json");
        std::fs::create_dir(&output).unwrap();
        let previous = serde_json::to_vec_pretty(&test_dump()).unwrap();
        std::fs::write(output.join("previous.json"), &previous).unwrap();

        let error = write_dump_output(&output, &test_dump()).unwrap_err();

        assert!(
            matches!(&error, DumpLoadError::WriteOutput { path, .. } if path == &output),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(message.contains("replace file"), "{message}");
        assert!(message.contains(&output.display().to_string()), "{message}");
        assert_eq!(
            std::fs::read(output.join("previous.json")).unwrap(),
            previous
        );
        assert_eq!(std::fs::read_dir(&output).unwrap().count(), 1);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    fn test_event(batch_version: i64) -> DumpEvent {
        DumpEvent {
            batch_version,
            event_type: "set_xattr".to_string(),
            event_payload: studiofs_meta::events::encode_event(
                &studiofs_meta::MetaEvent::SetXattr {
                    ino: 1,
                    name: b"user.test".to_vec(),
                    value: vec![1, 2, 3],
                    flags: 0,
                },
            )
            .unwrap(),
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
                expected: studiofs_meta::schema::CURRENT_FORMAT_VERSION
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
    struct PgDumpTasks<'a> {
        runtime: &'a tokio::runtime::Runtime,
        connection_tasks:
            Vec<tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>>,
        loader: Option<std::thread::JoinHandle<DumpLoadResult<MetadataDump>>>,
    }

    impl Drop for PgDumpTasks<'_> {
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
        let schema = crate::cli::pg_test_schema::PgTestSchema::new(&metadata_dsn)
            .expect("create isolated PostgreSQL schema");
        let metadata_dsn = schema.dsn().to_owned();

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
        let mut cleanup = PgDumpTasks {
            runtime: &runtime,
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

    #[test]
    #[ignore = "requires the PostgreSQL test database"]
    fn load_seeds_trash_authority_and_rejects_existing_trash_rows() {
        use studiofs_meta::schema::ROOT_INO;
        use studiofs_meta::trash::{MetaVolumeSettings, RetainedEntry, VolumeSettings};
        use studiofs_meta::{MetaFormat, MetaFsOps, MetaInodes, RedbMetaEngine};

        let runtime = tokio::runtime::Runtime::new().expect("build PostgreSQL test runtime");
        let raw_dsn = test_pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required");
        let metadata_dsn = runtime
            .block_on(ensure_studiofs_database(&raw_dsn))
            .expect("resolve PostgreSQL test database")
            .metadata_dsn;
        let schema = crate::cli::pg_test_schema::PgTestSchema::new(&metadata_dsn)
            .expect("create isolated PostgreSQL schema");
        let metadata_dsn = schema.dsn();

        // Source state: a still-retained receipt and a restored (terminal) one
        // in the checkpoint, plus a receipt admitted in the post-checkpoint tail.
        let temp = studiofs_test_support::workspace_target_tempdir("load-trash-");
        let source = RedbMetaEngine::open(temp.path().join("source")).unwrap();
        source.init_root(0, 0).unwrap();
        source
            .set_format_info(&decode_format_info(&test_dump().metadata_state_format_info).unwrap())
            .unwrap();
        // Settle the TTL into base so the serving checkpoint carries it.
        let checkpoint_settings = source.compare_and_set_trash_days(0, 7).unwrap().unwrap();
        source
            .apply_replica_events_to_base(&[studiofs_meta::MetaEvent::SetVolumeSettings {
                settings: checkpoint_settings,
            }])
            .unwrap();
        let store = source
            .create_dir(ROOT_INO, b"private", 0, 0, 0o700)
            .unwrap();
        let mut receipts = Vec::new();
        for name in [&b"kept"[..], b"done", b"tail"] {
            source.create_dir(ROOT_INO, name, 0, 0, 0o700).unwrap();
        }
        for name in [&b"kept"[..], b"done"] {
            let receipt = source.next_inode().unwrap();
            source
                .retain_namespace_entry(ROOT_INO, name, store.ino, receipt)
                .unwrap();
            receipts.push(receipt);
        }
        source
            .create_dir(receipts[1], b"stage", 0, 0, 0o700)
            .unwrap();
        let data = studiofs_meta::MetaNamespace::lookup(&source, receipts[1], b"data").unwrap();
        let data_attr = source.get_attr(data.ino).unwrap();
        let fence = studiofs_meta::trash::RetainedSourceFence {
            ino: data.ino,
            ctime: data_attr.ctime,
            size: data_attr.size,
            content_epoch: data_attr.content_epoch,
        };
        source
            .restore_retained_entry(receipts[1], ROOT_INO, b"restored", b"stage", &[fence])
            .unwrap();
        let snapshot = source.export_serving_checkpoint_bytes().unwrap();
        let tail_start = source
            .get_counter(studiofs_meta::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        let tail_receipt = source.next_inode().unwrap();
        source
            .retain_namespace_entry(ROOT_INO, b"tail", store.ino, tail_receipt)
            .unwrap();
        receipts.push(tail_receipt);
        let tail = source.read_events_range(tail_start, u64::MAX).unwrap();
        assert!(!tail.is_empty());

        let id = uuid::Uuid::new_v4().to_string();
        let mut dump = test_dump();
        dump.volume.name = format!("sfs-1054-load-{id}");
        dump.volume.uuid = id;
        let mut format_info = decode_format_info(&dump.metadata_state_format_info).unwrap();
        format_info.metadata.backend = studiofs_meta::MetadataBackend::Postgres;
        format_info.metadata.postgres = Some(studiofs_meta::PostgresConfig {
            url: "test-postgres-dsn".to_string(),
            volume_id: 1,
        });
        dump.metadata_state_format_info = serde_json::to_vec(&format_info).unwrap();
        dump.metadata_state_checkpoint_version = 1;
        dump.metadata_state_version = 2;
        dump.metadata_state_checkpoint_sha256 = Sha256::digest(&snapshot).to_vec();
        dump.metadata_state_snapshot = snapshot;
        dump.event_log = tail
            .iter()
            .map(|event| DumpEvent {
                batch_version: 2,
                event_type: event.kind().to_string(),
                event_payload: studiofs_meta::events::encode_event(event).unwrap(),
                client_id: None,
                client_timestamp: None,
            })
            .collect();
        validate_dump(&dump).expect("synthetic dump is valid");

        let volume_id = apply_dump(metadata_dsn, &dump).expect("load dump");
        let stored_settings = |volume_id: i64| {
            runtime.block_on(async {
                let (client, connection) = connect_studiofs_postgres(metadata_dsn).await.unwrap();
                let task = tokio::spawn(connection);
                let bytes: Option<Vec<u8>> = client
                    .query_one(
                        "SELECT volume_settings FROM metadata_state WHERE vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .unwrap()
                    .get(0);
                task.abort();
                bytes.map(|bytes| serde_json::from_slice::<VolumeSettings>(&bytes).unwrap())
            })
        };
        assert_eq!(
            stored_settings(volume_id),
            Some(checkpoint_settings),
            "load must seed the trash-days CAS row from the checkpoint"
        );
        let seeded = runtime.block_on(async {
            let (client, connection) = connect_studiofs_postgres(metadata_dsn).await.unwrap();
            let task = tokio::spawn(connection);
            let rows = client
                .query(
                    "SELECT receipt, record FROM trash_entry_authority WHERE vol = $1 ORDER BY receipt",
                    &[&volume_id],
                )
                .await
                .unwrap();
            task.abort();
            rows.iter()
                .map(|row| {
                    let record: RetainedEntry =
                        serde_json::from_slice(&row.get::<_, Vec<u8>>(1)).unwrap();
                    (row.get::<_, i64>(0) as u64, record)
                })
                .collect::<Vec<_>>()
        });
        let expected = receipts
            .iter()
            .map(|receipt| (*receipt, source.retained_entry(*receipt).unwrap().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(seeded, expected, "load must seed the receipt authority");

        // Reloading would replace the projection and its terminal tombstones.
        let error = apply_dump(metadata_dsn, &dump).unwrap_err();
        assert!(
            matches!(
                &error,
                DumpLoadError::TargetHasTrashAuthorityState { tables }
                    if tables == &vec!["trash_entry_authority"]
            ),
            "{error}"
        );

        // A tag-19 tail disabling trash supersedes the checkpoint and keeps
        // the last positive TTL; the shared CAS continues from its revision.
        let disabled = source
            .compare_and_set_trash_days(checkpoint_settings.revision, 0)
            .unwrap()
            .unwrap();
        let disable_event = studiofs_meta::MetaEvent::SetVolumeSettings { settings: disabled };
        let id = uuid::Uuid::new_v4().to_string();
        dump.volume.name = format!("sfs-1054-load-{id}");
        dump.volume.uuid = id;
        dump.metadata_state_version = 3;
        dump.event_log.push(DumpEvent {
            batch_version: 3,
            event_type: disable_event.kind().to_string(),
            event_payload: studiofs_meta::events::encode_event(&disable_event).unwrap(),
            client_id: None,
            client_timestamp: None,
        });
        validate_dump(&dump).expect("synthetic dump with settings tail is valid");
        let tail_volume_id = apply_dump(metadata_dsn, &dump).expect("load dump with settings");
        assert_eq!(stored_settings(tail_volume_id), Some(disabled));
        assert_eq!(disabled.trash.existing_entry_days(), 7);
        let next = runtime.block_on(async {
            let (mut client, connection) = connect_studiofs_postgres(metadata_dsn).await.unwrap();
            let task = tokio::spawn(connection);
            let next = studiofs_meta::pg_engine::set_shared_trash_days(
                &mut client,
                tail_volume_id,
                Some(disabled.revision),
                3,
                "load-test",
                0,
            )
            .await
            .unwrap();
            task.abort();
            next
        });
        let next = next.expect("CAS from the loaded revision is not stale");
        assert_eq!(next.revision, disabled.revision + 1);
        assert_eq!(next.trash.days(), 3);
    }
}

//! `studiofs dump` / `studiofs load` — PostgreSQL metadata manifest backup/restore.

use clap::Args;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use studiofs_meta::pg_engine::schema::ensure_studiofs_pg_schema;
use studiofs_meta::{
    MetaError, connect_studiofs_postgres, ensure_studiofs_database,
    ensure_supported_format_version, ensure_supported_pg_sync_format_version, open_meta_engine,
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

#[derive(Debug, Error)]
pub enum DumpLoadError {
    #[error("input/output error: {0}")]
    Io(#[from] std::io::Error),
    #[error("postgres operation failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("metadata operation failed: {0}")]
    Meta(#[from] MetaError),
    #[error("json serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Message(String),
}

type DumpLoadResult<T> = std::result::Result<T, DumpLoadError>;

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
        let metadata_dsn = ensure_studiofs_database(meta_dsn)
            .await
            .map_err(|e| DumpLoadError::Message(e.to_string()))?
            .metadata_dsn;
        let (client, connection) = connect_studiofs_postgres(&metadata_dsn)
            .await
            .map_err(|e| DumpLoadError::Message(e.to_string()))?;
        let connection_task = tokio::spawn(connection);

        let dump = async {
            let row = client
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
                .map_err(|e| {
                    DumpLoadError::Message(format!("failed to query metadata_state: {e}"))
                })?;

            let row = row.ok_or_else(|| {
                DumpLoadError::Message(format!("volume {volume_id} not found in postgres"))
            })?;

            let volume = DumpVolume {
                vol: row.get(0),
                name: row.get(1),
                uuid: row.get(2),
                block_size: row.get(3),
            };
            let state_format_version: i16 = row.get(4);
            let state_version: i64 = row.get(5);
            let state_checkpoint_version: i64 = row.get(6);
            let state_snapshot: Vec<u8> = row.get(7);
            let state_checkpoint_sha256: Vec<u8> = row.get(8);
            let state_format_info: Vec<u8> = row.get(9);
            let state_publication_state: String = row.get(10);
            let state_generation: i64 = row.get(11);

            let counter_rows = client
                .query(
                    "SELECT name, value FROM counters WHERE vol = $1 ORDER BY name",
                    &[&volume.vol],
                )
                .await
                .map_err(|e| DumpLoadError::Message(format!("failed to query counters: {e}")))?;
            let counters = counter_rows
                .into_iter()
                .map(|r| DumpCounter {
                    name: r.get(0),
                    value: r.get(1),
                })
                .collect();

            let event_rows = client
                .query(
                    r#"
            SELECT batch_version, event_type, event_payload, client_id, client_timestamp
            FROM event_log
            WHERE vol = $1
            ORDER BY seq_id
            "#,
                    &[&volume.vol],
                )
                .await
                .map_err(|e| DumpLoadError::Message(format!("failed to query event_log: {e}")))?;
            let event_log = event_rows
                .into_iter()
                .map(|r| DumpEvent {
                    batch_version: r.get(0),
                    event_type: r.get(1),
                    event_payload: r.get(2),
                    client_id: r.get(3),
                    client_timestamp: r.get(4),
                })
                .collect();

            Ok(MetadataDump {
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
            })
        }
        .await;

        drop(client);
        connection_task.abort();
        let _ = connection_task.await;
        dump
    })
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

fn apply_dump(postgres_address: &str, dump: MetadataDump) -> DumpLoadResult<i64> {
    crate::runtime_bridge::block_on(async move {
        let metadata_dsn = ensure_studiofs_database(postgres_address)
            .await
            .map_err(|e| DumpLoadError::Message(e.to_string()))?
            .metadata_dsn;
        let (mut client, connection) = connect_studiofs_postgres(&metadata_dsn)
            .await
            .map_err(|e| DumpLoadError::Message(e.to_string()))?;
        let connection_task = tokio::spawn(connection);

        let result = async {
            ensure_studiofs_pg_schema(&client).await?;
            let tx = client
                .transaction()
                .await
                .map_err(|e| DumpLoadError::Message(format!("postgres transaction failed: {e}")))?;
            tx.execute("SELECT pg_advisory_xact_lock($1)", &[&902_202_603_i64])
                .await
                .map_err(|e| DumpLoadError::Message(format!("postgres advisory lock failed: {e}")))?;

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
                .map_err(|e| DumpLoadError::Message(format!("upsert volume failed: {e}")))?;
            let target_vol: i64 = row.get(0);

            let has_snapshot_v2_state: bool = tx
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM snapshot_catalog WHERE vol = $1) OR EXISTS (SELECT 1 FROM snapshot_policies WHERE vol = $1)",
                    &[&target_vol],
                )
                .await
                .map_err(|e| {
                    DumpLoadError::Message(format!("check snapshot v2 state failed: {e}"))
                })?
                .get(0);
            if has_snapshot_v2_state {
                return Err(DumpLoadError::Message(
                    "cannot load metadata over existing Snapshot v2 state; remove snapshots and policy first"
                        .to_string(),
                ));
            }

            tx.execute("DELETE FROM snapshot_policies WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear snapshot policies failed: {e}")))?;
            tx.execute("DELETE FROM snapshot_catalog WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear snapshot catalog failed: {e}")))?;
            tx.execute("DELETE FROM counters WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear counters failed: {e}")))?;
            tx.execute("DELETE FROM event_log WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear event log failed: {e}")))?;
            tx.execute("DELETE FROM metadata_state WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear metadata_state failed: {e}")))?;
            tx.execute(
                "INSERT INTO metadata_state (vol, metadata_format_version, version, checkpoint_version, snapshot, checkpoint_sha256, format_info, checkpoint_publication_state, checkpoint_generation) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                &[
                    &target_vol,
                    &dump.metadata_state_format_version,
                    &dump.metadata_state_version,
                    &dump.metadata_state_checkpoint_version,
                    &dump.metadata_state_snapshot,
                    &dump.metadata_state_checkpoint_sha256,
                    &dump.metadata_state_format_info,
                    &dump.metadata_state_publication_state,
                    &dump.metadata_state_generation,
                ],
            )
            .await
            .map_err(|e| DumpLoadError::Message(format!("insert metadata_state failed: {e}")))?;

            for counter in &dump.counters {
                tx.execute(
                    "INSERT INTO counters (vol, name, value) VALUES ($1, $2, $3)",
                    &[&target_vol, &counter.name, &counter.value],
                )
                .await
                .map_err(|e| {
                    DumpLoadError::Message(format!("insert counter '{}' failed: {e}", counter.name))
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
                .map_err(|e| {
                    DumpLoadError::Message(format!(
                        "insert event_log row failed for vol={} batch_version={} event_type={}: {e}",
                        target_vol, event.batch_version, event.event_type
                    ))
                })?;
            }

            tx.commit()
                .await
                .map_err(|e| DumpLoadError::Message(format!("postgres commit failed: {e}")))?;

            Ok::<i64, DumpLoadError>(target_vol)
        }
        .await;

        drop(client);
        connection_task.abort();
        let _ = connection_task.await;
        result
    })
}

pub fn load(input: &Path, postgres_address: &str) -> DumpLoadResult<()> {
    let bytes = std::fs::read(input).map_err(|e| {
        DumpLoadError::Message(format!("failed to read dump {}: {e}", input.display()))
    })?;
    let dump: MetadataDump = serde_json::from_slice(&bytes).map_err(|e| {
        DumpLoadError::Message(format!("failed to parse dump {}: {e}", input.display()))
    })?;
    let volume_name = dump.volume.name.clone();

    if dump.dump_version != 4 {
        return Err(DumpLoadError::Message(format!(
            "unsupported dump version: {}",
            dump.dump_version
        )));
    }

    ensure_supported_pg_sync_format_version(Some(dump.metadata_state_format_version))?;
    let format_info: studiofs_meta::FormatInfo =
        serde_json::from_slice(&dump.metadata_state_format_info)?;
    ensure_supported_format_version(&format_info)?;
    if dump.metadata_state_checkpoint_sha256.as_slice()
        != Sha256::digest(&dump.metadata_state_snapshot).as_slice()
    {
        return Err(DumpLoadError::Message(
            "dump checkpoint checksum does not match checkpoint bytes".to_string(),
        ));
    }
    if dump.metadata_state_publication_state != "published" || dump.metadata_state_generation <= 0 {
        return Err(DumpLoadError::Message(
            "dump checkpoint descriptor is not a completed publication".to_string(),
        ));
    }

    apply_dump(postgres_address, dump)?;

    println!("✓ Dump loaded into PostgreSQL");
    println!("  Volume: {}", volume_name);
    println!("  Source: {}", input.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

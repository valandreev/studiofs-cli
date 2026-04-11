//! `studiofs dump` / `studiofs load` — PostgreSQL metadata manifest backup/restore.

use std::path::Path;

use serde::{Deserialize, Serialize};
use studiofs_meta::{MetaError, ensure_studiofs_database, open_meta_engine};
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
struct DumpSnapshot {
    id: String,
    root_ino: i64,
    created_at: i64,
    state: i16,
    pinned_object_ids: Vec<i64>,
    snapshot_payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataDump {
    dump_version: u16,
    volume: DumpVolume,
    metadata_state_version: i64,
    metadata_state_snapshot: Vec<u8>,
    counters: Vec<DumpCounter>,
    event_log: Vec<DumpEvent>,
    #[serde(default)]
    snapshots: Vec<DumpSnapshot>,
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

async fn ensure_pg_schema(client: &tokio_postgres::Client) -> DumpLoadResult<()> {
    client
        .batch_execute(
            r#"
                CREATE TABLE IF NOT EXISTS volumes (
                    vol BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    uuid TEXT NOT NULL UNIQUE,
                    block_size INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS counters (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    value BIGINT NOT NULL,
                    PRIMARY KEY (vol, name)
                );

                CREATE TABLE IF NOT EXISTS event_log (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    seq_id BIGSERIAL PRIMARY KEY,
                    batch_version BIGINT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_payload BYTEA NOT NULL,
                    client_id TEXT,
                    client_timestamp BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS metadata_state (
                    vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
                    version BIGINT NOT NULL,
                    snapshot BYTEA NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS acl_rules (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    ino BIGINT NOT NULL,
                    principal TEXT NOT NULL,
                    access_level SMALLINT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (vol, ino, principal)
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
                    id TEXT NOT NULL,
                    root_ino BIGINT NOT NULL,
                    created_at BIGINT NOT NULL,
                    state SMALLINT NOT NULL,
                    pinned_object_ids BIGINT[] NOT NULL DEFAULT '{}',
                    snapshot_payload BYTEA NOT NULL DEFAULT ''::bytea,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (vol, id)
                );

                ALTER TABLE event_log
                    ADD COLUMN IF NOT EXISTS batch_version BIGINT;
                UPDATE event_log
                    SET batch_version = 0
                    WHERE batch_version IS NULL;
                ALTER TABLE event_log
                    ALTER COLUMN batch_version SET NOT NULL;

                CREATE INDEX IF NOT EXISTS idx_event_log_vol_seq
                    ON event_log(vol, seq_id);
                CREATE INDEX IF NOT EXISTS idx_event_log_vol_batch_seq
                    ON event_log(vol, batch_version, seq_id);
        "#,
        )
        .await?;
    Ok(())
}

fn load_remote_dump(meta_dsn: &str, volume_id: i64) -> DumpLoadResult<MetadataDump> {
    crate::runtime_bridge::block_on(async move {
        let metadata_dsn = ensure_studiofs_database(meta_dsn)
            .await
            .map_err(|e| DumpLoadError::Message(e.to_string()))?
            .metadata_dsn;
        let (client, connection) = tokio_postgres::connect(&metadata_dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| DumpLoadError::Message(format!("cannot connect to postgres: {e}")))?;
        let connection_task = tokio::spawn(connection);

        let dump = async {
            let row = client
                .query_opt(
                    r#"
            SELECT v.vol, v.name, v.uuid, v.block_size, ms.version, ms.snapshot
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
            let state_version: i64 = row.get(4);
            let state_snapshot: Vec<u8> = row.get(5);

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

            let snapshot_rows = client
                .query(
                    r#"
            SELECT id, root_ino, created_at, state, pinned_object_ids, snapshot_payload
            FROM snapshots
            WHERE vol = $1
            ORDER BY created_at DESC, id ASC
            "#,
                    &[&volume.vol],
                )
                .await
                .map_err(|e| DumpLoadError::Message(format!("failed to query snapshots: {e}")))?;
            let snapshots = snapshot_rows
                .into_iter()
                .map(|r| DumpSnapshot {
                    id: r.get(0),
                    root_ino: r.get(1),
                    created_at: r.get(2),
                    state: r.get(3),
                    pinned_object_ids: r.get(4),
                    snapshot_payload: r.get(5),
                })
                .collect();

            Ok(MetadataDump {
                dump_version: 1,
                volume,
                metadata_state_version: state_version,
                metadata_state_snapshot: state_snapshot,
                counters,
                event_log,
                snapshots,
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
        let (mut client, connection) =
            tokio_postgres::connect(&metadata_dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| DumpLoadError::Message(format!("cannot connect to postgres: {e}")))?;
        let connection_task = tokio::spawn(connection);

        let result = async {
            ensure_pg_schema(&client).await?;
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

            tx.execute("DELETE FROM counters WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear counters failed: {e}")))?;
            tx.execute("DELETE FROM event_log WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear event log failed: {e}")))?;
            tx.execute("DELETE FROM metadata_state WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear metadata_state failed: {e}")))?;
            tx.execute("DELETE FROM snapshots WHERE vol = $1", &[&target_vol])
                .await
                .map_err(|e| DumpLoadError::Message(format!("clear snapshots failed: {e}")))?;

            tx.execute(
                "INSERT INTO metadata_state (vol, version, snapshot) VALUES ($1, $2, $3)",
                &[&target_vol, &dump.metadata_state_version, &dump.metadata_state_snapshot],
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

            for snapshot in &dump.snapshots {
                tx.execute(
                    r#"
            INSERT INTO snapshots (
                vol,
                id,
                root_ino,
                created_at,
                state,
                pinned_object_ids,
                snapshot_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
                    &[
                        &target_vol,
                        &snapshot.id,
                        &snapshot.root_ino,
                        &snapshot.created_at,
                        &snapshot.state,
                        &snapshot.pinned_object_ids,
                        &snapshot.snapshot_payload,
                    ],
                )
                .await
                .map_err(|e| {
                    DumpLoadError::Message(format!(
                        "insert snapshots row failed for vol={} snapshot_id={}: {e}",
                        target_vol, snapshot.id
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

    if dump.dump_version != 1 {
        return Err(DumpLoadError::Message(format!(
            "unsupported dump version: {}",
            dump.dump_version
        )));
    }

    apply_dump(postgres_address, dump)?;

    println!("✓ Dump loaded into PostgreSQL");
    println!("  Volume: {}", volume_name);
    println!("  Source: {}", input.display());
    Ok(())
}

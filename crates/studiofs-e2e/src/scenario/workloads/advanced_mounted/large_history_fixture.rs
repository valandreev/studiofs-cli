//! PostgreSQL-backed history fixture for the large-history mounted scenario.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use studiofs_meta::MetaEvent;
use studiofs_meta::events::encode_event;
use studiofs_meta::redb_engine::{RedbMetaEngine, RedbSnapshot};
use studiofs_meta::schema::{DirEntryValue, FileType, InodeAttr, ROOT_INO};
use tokio_postgres::NoTls;

use super::*;
use crate::client::ClientControllerConfig;

const FIXTURE_DIRECTORY_INO: u64 = 2;
const FIXTURE_FILES_DIRECTORY_INO: u64 = 3;
const FIXTURE_FIRST_SHARD_INO: u64 = 4;
pub(in crate::scenario) const FIXTURE_SHARD_COUNT: usize = 64;
const FIXTURE_FILES_PER_SHARD: usize = LARGE_HISTORY_FIXTURE_FILE_COUNT / FIXTURE_SHARD_COUNT;
const FIXTURE_FIRST_FILE_INO: u64 = FIXTURE_FIRST_SHARD_INO + FIXTURE_SHARD_COUNT as u64;
pub(in crate::scenario) const FIXTURE_FILES_DIRECTORY_NAME: &str = "fixture-files";
const FIXTURE_UID: u32 = 1000;
const FIXTURE_GID: u32 = 1000;
const FIXTURE_DIRECTORY_MODE: u16 = 0o755;
const FIXTURE_FILE_MODE: u16 = 0o644;
const FIXTURE_FILE_SIZE: u64 = 1;
const FIXTURE_TIME_NANOS: i64 = 1_700_000_000_000_000_000;
const FIXTURE_CLIENT_ID: &str = "studiofs-e2e-pg-fixture";
const FIXTURE_CHECKPOINT_VERSION: i64 = 8;
const FIXTURE_REPLAY_BATCH_COUNT: usize = 32;
#[cfg(test)]
const FIXTURE_REPLAY_EVENT_COUNT: usize =
    LARGE_HISTORY_FIXTURE_EVENTS_PER_BATCH * FIXTURE_REPLAY_BATCH_COUNT;
const REPLAY_GATE_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

const INSERT_FIXTURE_EVENTS: &str = r#"
    INSERT INTO event_log (
        vol,
        batch_version,
        event_type,
        event_payload,
        client_id,
        client_timestamp
    )
    SELECT
        $1,
        fixture.batch_version,
        fixture.event_type,
        fixture.event_payload,
        $2,
        NULL
    FROM UNNEST($3::bigint[], $4::text[], $5::bytea[])
        AS fixture(batch_version, event_type, event_payload)
    "#;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct LargeHistoryFixtureSummary {
    pub(in crate::scenario) fixture_root: String,
    pub(in crate::scenario) file_count: usize,
    pub(in crate::scenario) checkpoint_event_count: usize,
    pub(in crate::scenario) event_count: usize,
    pub(in crate::scenario) total_event_count: usize,
    #[serde(default)]
    pub(in crate::scenario) replay_batch_count: usize,
    pub(in crate::scenario) checkpoint_version: u64,
    pub(in crate::scenario) batch_count: u64,
    pub(in crate::scenario) remote_head_version: u64,
    pub(in crate::scenario) checkpoint_payload_bytes: u64,
    pub(in crate::scenario) event_payload_bytes: u64,
    pub(in crate::scenario) elapsed_ms: u64,
}

#[derive(Debug)]
struct FixtureEvent {
    batch_version: i64,
    event_type: String,
    event_payload: Vec<u8>,
}

struct ConnectionGuard {
    task: tokio::task::JoinHandle<()>,
}

pub(in crate::scenario) struct LargeHistoryReplayGate {
    release: Option<mpsc::Sender<()>>,
    thread: Option<thread::JoinHandle<Result<(), io::Error>>>,
}

impl LargeHistoryReplayGate {
    pub(in crate::scenario) fn acquire(dsn: &str) -> Result<Self, ScenarioError> {
        let (acquired_tx, acquired_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let dsn = dsn.to_string();
        let thread = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| io::Error::other("large-history replay gate runtime failed"))?;
            runtime.block_on(async move {
                let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls)
                    .await
                    .map_err(|_| io::Error::other("large-history replay gate connection failed"))?;
                let _connection_guard = ConnectionGuard {
                    task: tokio::spawn(async move {
                        let _ = connection.await;
                    }),
                };
                let transaction = client.transaction().await.map_err(|_| {
                    io::Error::other("large-history replay gate transaction failed")
                })?;
                transaction
                    .batch_execute("LOCK TABLE event_log IN ACCESS EXCLUSIVE MODE")
                    .await
                    .map_err(|_| io::Error::other("large-history replay gate lock failed"))?;
                acquired_tx.send(()).map_err(|_| {
                    io::Error::other("large-history replay gate acquire signal failed")
                })?;
                release_rx.recv().map_err(|_| {
                    io::Error::other("large-history replay gate release signal failed")
                })?;
                transaction
                    .commit()
                    .await
                    .map_err(|_| io::Error::other("large-history replay gate release failed"))
            })
        });

        acquired_rx
            .recv_timeout(REPLAY_GATE_ACQUIRE_TIMEOUT)
            .map_err(|_| {
                ScenarioError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "large-history replay gate was not acquired",
                ))
            })?;
        Ok(Self {
            release: Some(release_tx),
            thread: Some(thread),
        })
    }

    pub(in crate::scenario) fn release(mut self) -> Result<(), ScenarioError> {
        self.release_gate()?;
        self.join_thread()
    }

    fn release_gate(&mut self) -> Result<(), ScenarioError> {
        if let Some(release) = self.release.take() {
            release.send(()).map_err(|_| {
                ScenarioError::Io(io::Error::other(
                    "large-history replay gate release signal failed",
                ))
            })?;
        }
        Ok(())
    }

    fn join_thread(&mut self) -> Result<(), ScenarioError> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        thread
            .join()
            .map_err(|_| {
                ScenarioError::Io(io::Error::other(
                    "large-history replay gate thread panicked",
                ))
            })?
            .map_err(ScenarioError::Io)
    }
}

impl Drop for LargeHistoryReplayGate {
    fn drop(&mut self) {
        let _ = self.release_gate();
        let _ = self.join_thread();
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(in crate::scenario) fn seed_large_history_fixture(
    config: &ClientControllerConfig,
    volume_name: &str,
    fixture_root: &str,
    scenario_dir: &Path,
) -> Result<LargeHistoryFixtureSummary, ScenarioError> {
    let started = Instant::now();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(ScenarioError::Io)?;
    let summary = runtime.block_on(seed_large_history_fixture_async(
        &config.meta_dsn,
        volume_name,
        fixture_root,
        scenario_dir,
    ))?;
    fs::write(
        scenario_dir.join("large-history-fixture.json"),
        serde_json::to_vec_pretty(&LargeHistoryFixtureSummary {
            elapsed_ms: elapsed_millis_u64(started),
            ..summary.clone()
        })?,
    )?;
    Ok(LargeHistoryFixtureSummary {
        elapsed_ms: elapsed_millis_u64(started),
        ..summary
    })
}

pub(in crate::scenario) fn append_large_history_moving_head_fixture(
    config: &ClientControllerConfig,
    volume_name: &str,
) -> Result<u64, ScenarioError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(ScenarioError::Io)?;
    runtime.block_on(append_large_history_moving_head_fixture_async(
        &config.meta_dsn,
        volume_name,
    ))
}

async fn append_large_history_moving_head_fixture_async(
    dsn: &str,
    volume_name: &str,
) -> Result<u64, ScenarioError> {
    let (mut client, connection) = tokio_postgres::connect(dsn, NoTls).await.map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history moving-head fixture connection failed",
        ))
    })?;
    let _connection_guard = ConnectionGuard {
        task: tokio::spawn(async move {
            let _ = connection.await;
        }),
    };
    let transaction = client.transaction().await.map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history moving-head fixture transaction failed",
        ))
    })?;
    let volume_row = transaction
        .query_opt("SELECT vol FROM volumes WHERE name = $1", &[&volume_name])
        .await
        .map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history moving-head fixture volume lookup failed",
            ))
        })?
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history moving-head fixture volume was not found",
            ))
        })?;
    let volume_id: i64 = volume_row.get(0);
    let current_version: i64 = transaction
        .query_one(
            "SELECT version FROM metadata_state WHERE vol = $1 FOR UPDATE",
            &[&volume_id],
        )
        .await
        .map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history moving-head fixture state lookup failed",
            ))
        })?
        .get(0);
    let first_batch_version = current_version.checked_add(1).ok_or_else(|| {
        ScenarioError::Io(io::Error::other(
            "large-history moving-head fixture version overflow",
        ))
    })?;
    let events = build_moving_head_events(first_batch_version)?;
    let event_payload_bytes = events.iter().try_fold(0_i64, |total, event| {
        total
            .checked_add(i64::try_from(event.event_payload.len()).map_err(|_| {
                ScenarioError::Io(io::Error::other(
                    "large-history moving-head fixture payload overflow",
                ))
            })?)
            .ok_or_else(|| {
                ScenarioError::Io(io::Error::other(
                    "large-history moving-head fixture payload counter overflow",
                ))
            })
    })?;
    insert_fixture_events(&transaction, volume_id, &events).await?;
    let remote_head_version = events
        .last()
        .map(|event| event.batch_version)
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history moving-head fixture generated no events",
            ))
        })?;
    let updated = transaction
        .execute(
            "UPDATE metadata_state
             SET version = $2,
                 total_event_payload_bytes = total_event_payload_bytes + $3,
                 updated_at = now()
             WHERE vol = $1 AND version = $4",
            &[
                &volume_id,
                &remote_head_version,
                &event_payload_bytes,
                &current_version,
            ],
        )
        .await
        .map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history moving-head fixture state update failed",
            ))
        })?;
    if updated != 1 {
        return Err(ScenarioError::Io(io::Error::other(
            "large-history moving-head fixture state changed during append",
        )));
    }
    transaction.commit().await.map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history moving-head fixture commit failed",
        ))
    })?;
    u64::try_from(remote_head_version).map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history moving-head fixture produced a negative version",
        ))
    })
}

async fn insert_fixture_events(
    transaction: &tokio_postgres::Transaction<'_>,
    volume_id: i64,
    events: &[FixtureEvent],
) -> Result<(), ScenarioError> {
    for chunk in events.chunks(LARGE_HISTORY_FIXTURE_INSERT_CHUNK_ROWS) {
        let batch_versions = chunk
            .iter()
            .map(|event| event.batch_version)
            .collect::<Vec<_>>();
        let event_types = chunk
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>();
        let event_payloads = chunk
            .iter()
            .map(|event| event.event_payload.clone())
            .collect::<Vec<_>>();
        transaction
            .execute(
                INSERT_FIXTURE_EVENTS,
                &[
                    &volume_id,
                    &FIXTURE_CLIENT_ID,
                    &batch_versions,
                    &event_types,
                    &event_payloads,
                ],
            )
            .await
            .map_err(|_| {
                ScenarioError::Io(io::Error::other(
                    "large-history PostgreSQL fixture event-log insert failed",
                ))
            })?;
    }
    Ok(())
}

async fn seed_large_history_fixture_async(
    dsn: &str,
    volume_name: &str,
    fixture_root: &str,
    scenario_dir: &Path,
) -> Result<LargeHistoryFixtureSummary, ScenarioError> {
    let (mut client, connection) = tokio_postgres::connect(dsn, NoTls).await.map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history PostgreSQL fixture connection failed",
        ))
    })?;
    let _connection_guard = ConnectionGuard {
        task: tokio::spawn(async move {
            let _ = connection.await;
        }),
    };
    let transaction = client.transaction().await.map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history PostgreSQL fixture transaction failed",
        ))
    })?;
    let volume_row = transaction
        .query_opt("SELECT vol FROM volumes WHERE name = $1", &[&volume_name])
        .await
        .map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture volume lookup failed",
            ))
        })?
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture volume was not found",
            ))
        })?;
    let volume_id: i64 = volume_row.get(0);
    let state_row = transaction
        .query_one(
            "SELECT version, checkpoint_version FROM metadata_state WHERE vol = $1 FOR UPDATE",
            &[&volume_id],
        )
        .await
        .map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture metadata state lookup failed",
            ))
        })?;
    let metadata_version: i64 = state_row.get(0);
    let checkpoint_version: i64 = state_row.get(1);
    if metadata_version != 0 || checkpoint_version != 0 {
        return Err(ScenarioError::Io(io::Error::other(
            "large-history PostgreSQL fixture requires an empty metadata history",
        )));
    }
    let existing_event_count: i64 = transaction
        .query_one(
            "SELECT COUNT(*) FROM event_log WHERE vol = $1",
            &[&volume_id],
        )
        .await
        .map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture event-log lookup failed",
            ))
        })?
        .get(0);
    if existing_event_count != 0 {
        return Err(ScenarioError::Io(io::Error::other(
            "large-history PostgreSQL fixture requires an empty event log",
        )));
    }

    let (checkpoint_events, events, file_count) = build_fixture_events(fixture_root)?;
    let checkpoint_bytes = build_serving_checkpoint(&checkpoint_events, scenario_dir)?;
    let checkpoint_payload_bytes = encoded_payload_bytes(&checkpoint_events)?;
    let event_payload_bytes = events.iter().try_fold(0_u64, |total, event| {
        total
            .checked_add(event.event_payload.len() as u64)
            .ok_or_else(|| {
                ScenarioError::Io(io::Error::other(
                    "large-history PostgreSQL fixture payload size overflow",
                ))
            })
    })?;
    let remote_head_version = events
        .last()
        .map(|event| event.batch_version)
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture generated no events",
            ))
        })?;

    insert_fixture_events(&transaction, volume_id, &events).await?;
    let total_payload_bytes = checkpoint_payload_bytes
        .checked_add(event_payload_bytes)
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture total payload size overflow",
            ))
        })?;
    let total_payload_bytes = i64::try_from(total_payload_bytes).map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history PostgreSQL fixture payload counter overflow",
        ))
    })?;
    let checkpoint_payload_bytes_i64 = i64::try_from(checkpoint_payload_bytes).map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history PostgreSQL fixture checkpoint payload counter overflow",
        ))
    })?;
    let updated = transaction
        .execute(
            "UPDATE metadata_state
             SET version = $2,
                 checkpoint_version = $3,
                 total_event_payload_bytes = $4,
                 checkpoint_total_event_bytes = $5,
                 snapshot = $6,
                 checkpoint_sha256 = $7,
                 checkpoint_publication_state = 'published',
                 checkpoint_generation = checkpoint_generation + 1,
                 checkpoint_updated_at = now(),
                 updated_at = now()
             WHERE vol = $1 AND version = 0 AND checkpoint_version = 0",
            &[
                &volume_id,
                &remote_head_version,
                &FIXTURE_CHECKPOINT_VERSION,
                &total_payload_bytes,
                &checkpoint_payload_bytes_i64,
                &checkpoint_bytes,
                &sha256(&checkpoint_bytes),
            ],
        )
        .await
        .map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture metadata state update failed",
            ))
        })?;
    if updated != 1 {
        return Err(ScenarioError::Io(io::Error::other(
            "large-history PostgreSQL fixture metadata state changed during seed",
        )));
    }
    transaction.commit().await.map_err(|_| {
        ScenarioError::Io(io::Error::other(
            "large-history PostgreSQL fixture commit failed",
        ))
    })?;

    Ok(LargeHistoryFixtureSummary {
        fixture_root: fixture_root.to_string(),
        file_count,
        checkpoint_event_count: checkpoint_events.len(),
        event_count: events.len(),
        total_event_count: checkpoint_events.len().saturating_add(events.len()),
        replay_batch_count: events.len() / LARGE_HISTORY_FIXTURE_EVENTS_PER_BATCH,
        checkpoint_version: u64::try_from(FIXTURE_CHECKPOINT_VERSION).map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture checkpoint version was negative",
            ))
        })?,
        batch_count: u64::try_from(remote_head_version).map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture produced a negative batch count",
            ))
        })?,
        remote_head_version: u64::try_from(remote_head_version).map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history PostgreSQL fixture produced a negative remote head version",
            ))
        })?,
        checkpoint_payload_bytes,
        event_payload_bytes,
        elapsed_ms: 0,
    })
}

fn configured_replay_batch_count() -> Result<usize, ScenarioError> {
    let value = std::env::var(LARGE_HISTORY_REPLAY_BATCHES_ENV)
        .unwrap_or_else(|_| FIXTURE_REPLAY_BATCH_COUNT.to_string());
    let replay_batch_count = value.parse::<usize>().map_err(|_| {
        ScenarioError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{LARGE_HISTORY_REPLAY_BATCHES_ENV} must be a positive integer"),
        ))
    })?;
    if replay_batch_count == 0 {
        return Err(ScenarioError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{LARGE_HISTORY_REPLAY_BATCHES_ENV} must be a positive integer"),
        )));
    }
    let replay_event_count = LARGE_HISTORY_FIXTURE_EVENTS_PER_BATCH
        .checked_mul(replay_batch_count)
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture replay event count overflow",
            ))
        })?;
    if replay_event_count > LARGE_HISTORY_INITIAL_MUTATION_COUNT {
        return Err(ScenarioError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{LARGE_HISTORY_REPLAY_BATCHES_ENV} exceeds the initial mutation budget"),
        )));
    }
    Ok(replay_batch_count)
}

fn build_fixture_events(
    fixture_root: &str,
) -> Result<(Vec<MetaEvent>, Vec<FixtureEvent>, usize), ScenarioError> {
    let file_count = LARGE_HISTORY_FIXTURE_FILE_COUNT;
    let replay_batch_count = configured_replay_batch_count()?;
    let replay_event_count = LARGE_HISTORY_FIXTURE_EVENTS_PER_BATCH
        .checked_mul(replay_batch_count)
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture replay event count overflow",
            ))
        })?;
    let checkpoint_event_count = LARGE_HISTORY_INITIAL_MUTATION_COUNT
        .checked_sub(replay_event_count)
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture checkpoint history underflow",
            ))
        })?;
    let mut checkpoint_events = Vec::with_capacity(checkpoint_event_count);
    let mut replay_events = Vec::with_capacity(replay_event_count);
    let mut root_attr = InodeAttr::new_dir(
        ROOT_INO,
        0,
        FIXTURE_UID,
        FIXTURE_GID,
        FIXTURE_DIRECTORY_MODE,
    );
    set_fixture_times(&mut root_attr, FIXTURE_TIME_NANOS);
    checkpoint_events.push(MetaEvent::SetAttr { attr: root_attr });
    let mut directory_attr = InodeAttr::new_dir(
        FIXTURE_DIRECTORY_INO,
        ROOT_INO,
        FIXTURE_UID,
        FIXTURE_GID,
        FIXTURE_DIRECTORY_MODE,
    );
    set_fixture_times(&mut directory_attr, FIXTURE_TIME_NANOS);
    checkpoint_events.push(MetaEvent::SetAttr {
        attr: directory_attr,
    });
    checkpoint_events.push(MetaEvent::AddEntry {
        parent: ROOT_INO,
        name: fixture_root.as_bytes().to_vec(),
        entry: DirEntryValue {
            ino: FIXTURE_DIRECTORY_INO,
            kind: FileType::Directory,
        },
    });

    let mut files_directory_attr = InodeAttr::new_dir(
        FIXTURE_FILES_DIRECTORY_INO,
        FIXTURE_DIRECTORY_INO,
        FIXTURE_UID,
        FIXTURE_GID,
        FIXTURE_DIRECTORY_MODE,
    );
    set_fixture_times(&mut files_directory_attr, FIXTURE_TIME_NANOS);
    checkpoint_events.push(MetaEvent::SetAttr {
        attr: files_directory_attr,
    });
    checkpoint_events.push(MetaEvent::AddEntry {
        parent: FIXTURE_DIRECTORY_INO,
        name: FIXTURE_FILES_DIRECTORY_NAME.as_bytes().to_vec(),
        entry: DirEntryValue {
            ino: FIXTURE_FILES_DIRECTORY_INO,
            kind: FileType::Directory,
        },
    });

    for shard_index in 0..FIXTURE_SHARD_COUNT {
        let ino = FIXTURE_FIRST_SHARD_INO + shard_index as u64;
        let mut attr = InodeAttr::new_dir(
            ino,
            FIXTURE_FILES_DIRECTORY_INO,
            FIXTURE_UID,
            FIXTURE_GID,
            FIXTURE_DIRECTORY_MODE,
        );
        set_fixture_times(&mut attr, FIXTURE_TIME_NANOS);
        checkpoint_events.push(MetaEvent::SetAttr { attr });
        checkpoint_events.push(MetaEvent::AddEntry {
            parent: FIXTURE_FILES_DIRECTORY_INO,
            name: format!("shard-{shard_index:02}").into_bytes(),
            entry: DirEntryValue {
                ino,
                kind: FileType::Directory,
            },
        });
    }

    let mut file_attrs = Vec::with_capacity(file_count);
    for index in 0..file_count {
        let ino = FIXTURE_FIRST_FILE_INO
            .checked_add(index as u64)
            .ok_or_else(|| {
                ScenarioError::Io(io::Error::other(
                    "large-history fixture inode number overflow",
                ))
            })?;
        let shard_index = index / FIXTURE_FILES_PER_SHARD;
        let shard_ino = FIXTURE_FIRST_SHARD_INO + shard_index as u64;
        let mut attr =
            InodeAttr::new_file(ino, shard_ino, FIXTURE_UID, FIXTURE_GID, FIXTURE_FILE_MODE);
        attr.size = FIXTURE_FILE_SIZE;
        attr.committed_size = FIXTURE_FILE_SIZE;
        set_fixture_times(&mut attr, FIXTURE_TIME_NANOS);
        checkpoint_events.push(MetaEvent::SetAttr { attr: attr.clone() });
        checkpoint_events.push(MetaEvent::AddEntry {
            parent: shard_ino,
            name: format!("file-{index:05}.bin").into_bytes(),
            entry: DirEntryValue {
                ino,
                kind: FileType::RegularFile,
            },
        });
        file_attrs.push(attr);
    }

    let checkpoint_filler_count = checkpoint_event_count
        .checked_sub(checkpoint_events.len())
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture state exceeds logical checkpoint history",
            ))
        })?;
    for filler_index in 0..checkpoint_filler_count {
        let file_index = filler_index % file_attrs.len();
        let mut attr = file_attrs[file_index].clone();
        let delta = i64::try_from(filler_index.saturating_add(1)).map_err(|_| {
            ScenarioError::Io(io::Error::other("large-history fixture timestamp overflow"))
        })?;
        set_fixture_times(&mut attr, FIXTURE_TIME_NANOS.saturating_add(delta));
        checkpoint_events.push(MetaEvent::SetAttr { attr });
    }

    for filler_index in 0..replay_event_count {
        let file_index = filler_index % file_attrs.len();
        let mut attr = file_attrs[file_index].clone();
        let delta = i64::try_from(
            checkpoint_filler_count
                .saturating_add(filler_index)
                .saturating_add(1),
        )
        .map_err(|_| {
            ScenarioError::Io(io::Error::other("large-history fixture timestamp overflow"))
        })?;
        set_fixture_times(&mut attr, FIXTURE_TIME_NANOS.saturating_add(delta));
        push_fixture_event(
            &mut replay_events,
            MetaEvent::SetAttr { attr },
            FIXTURE_CHECKPOINT_VERSION + 1,
        )?;
    }

    Ok((checkpoint_events, replay_events, file_count))
}

fn build_moving_head_events(first_batch_version: i64) -> Result<Vec<FixtureEvent>, ScenarioError> {
    let mut events = Vec::with_capacity(LARGE_HISTORY_MOVING_HEAD_MUTATION_COUNT);
    for event_index in 0..LARGE_HISTORY_MOVING_HEAD_MUTATION_COUNT {
        let file_index = event_index % LARGE_HISTORY_FIXTURE_FILE_COUNT;
        let ino = FIXTURE_FIRST_FILE_INO
            .checked_add(file_index as u64)
            .ok_or_else(|| {
                ScenarioError::Io(io::Error::other(
                    "large-history moving-head fixture inode overflow",
                ))
            })?;
        let shard_index = file_index / FIXTURE_FILES_PER_SHARD;
        let shard_ino = FIXTURE_FIRST_SHARD_INO + shard_index as u64;
        let mut attr =
            InodeAttr::new_file(ino, shard_ino, FIXTURE_UID, FIXTURE_GID, FIXTURE_FILE_MODE);
        attr.size = FIXTURE_FILE_SIZE;
        attr.committed_size = FIXTURE_FILE_SIZE;
        let delta = i64::try_from(
            LARGE_HISTORY_INITIAL_MUTATION_COUNT
                .saturating_add(event_index)
                .saturating_add(1),
        )
        .map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history moving-head fixture timestamp overflow",
            ))
        })?;
        set_fixture_times(&mut attr, FIXTURE_TIME_NANOS.saturating_add(delta));
        push_fixture_event(
            &mut events,
            MetaEvent::SetAttr { attr },
            first_batch_version,
        )?;
    }
    Ok(events)
}

fn build_serving_checkpoint(
    checkpoint_events: &[MetaEvent],
    scenario_dir: &Path,
) -> Result<Vec<u8>, ScenarioError> {
    let snapshot = serving_snapshot_from_events(checkpoint_events)?;
    let path = scenario_dir.join("large-history-fixture-checkpoint.redb");
    let result = (|| {
        let engine = RedbMetaEngine::open(&path).map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture checkpoint database open failed",
            ))
        })?;
        engine.import_remote_snapshot(&snapshot).map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture checkpoint database seed failed",
            ))
        })?;
        engine.export_serving_checkpoint_bytes().map_err(|_| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture checkpoint export failed",
            ))
        })
    })();
    let _ = fs::remove_file(&path);
    result
}

fn serving_snapshot_from_events(events: &[MetaEvent]) -> Result<RedbSnapshot, ScenarioError> {
    let mut inodes = BTreeMap::new();
    let mut dentries = BTreeMap::new();
    for event in events {
        match event {
            MetaEvent::SetAttr { attr } => {
                let bytes = postcard::to_stdvec(attr).map_err(|_| {
                    ScenarioError::Io(io::Error::other(
                        "large-history fixture checkpoint attr encoding failed",
                    ))
                })?;
                inodes.insert(attr.ino, bytes);
            }
            MetaEvent::AddEntry {
                parent,
                name,
                entry,
            } => {
                let mut key = parent.to_le_bytes().to_vec();
                key.extend_from_slice(name);
                let value = postcard::to_stdvec(entry).map_err(|_| {
                    ScenarioError::Io(io::Error::other(
                        "large-history fixture checkpoint dentry encoding failed",
                    ))
                })?;
                dentries.insert(key, value);
            }
            _ => {
                return Err(ScenarioError::Io(io::Error::other(
                    "large-history fixture checkpoint contains unsupported event",
                )));
            }
        }
    }
    Ok(RedbSnapshot {
        inodes: inodes.into_iter().collect(),
        dentries: dentries.into_iter().collect(),
        ..RedbSnapshot::default()
    })
}

fn encoded_payload_bytes(events: &[MetaEvent]) -> Result<u64, ScenarioError> {
    events.iter().try_fold(0_u64, |total, event| {
        total
            .checked_add(
                encode_event(event)
                    .map_err(|_| {
                        ScenarioError::Io(io::Error::other(
                            "large-history fixture checkpoint event encoding failed",
                        ))
                    })?
                    .len() as u64,
            )
            .ok_or_else(|| {
                ScenarioError::Io(io::Error::other(
                    "large-history fixture checkpoint payload size overflow",
                ))
            })
    })
}

fn sha256(bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest as _, Sha256};

    Sha256::digest(bytes).to_vec()
}

fn set_fixture_times(attr: &mut InodeAttr, timestamp: i64) {
    attr.mtime = timestamp;
    attr.ctime = timestamp;
    attr.atime = timestamp;
    attr.crtime = timestamp;
}

fn push_fixture_event(
    events: &mut Vec<FixtureEvent>,
    event: MetaEvent,
    first_batch_version: i64,
) -> Result<(), ScenarioError> {
    let event_type = event.kind().to_string();
    let event_payload = encode_event(&event).map_err(|error| {
        ScenarioError::Io(io::Error::other(format!(
            "large-history fixture event encoding failed: {error}"
        )))
    })?;
    let batch_offset = events
        .len()
        .checked_div(LARGE_HISTORY_FIXTURE_EVENTS_PER_BATCH)
        .and_then(|batch| i64::try_from(batch).ok())
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture batch version overflow",
            ))
        })?;
    let batch_version = first_batch_version
        .checked_add(batch_offset)
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "large-history fixture batch version overflow",
            ))
        })?;
    events.push(FixtureEvent {
        batch_version,
        event_type,
        event_payload,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_has_many_visible_files_and_multiple_batches() {
        let (checkpoint_events, replay_events, file_count) =
            build_fixture_events("fixture-root").expect("fixture events");

        assert_eq!(file_count, LARGE_HISTORY_FIXTURE_FILE_COUNT);
        assert_eq!(
            checkpoint_events.len() + replay_events.len(),
            LARGE_HISTORY_INITIAL_MUTATION_COUNT
        );
        assert_eq!(replay_events.len(), FIXTURE_REPLAY_EVENT_COUNT);
        assert!(checkpoint_events.len() > file_count);
        assert_eq!(replay_events.last().expect("last event").batch_version, 40);
        assert_eq!(replay_events[0].event_type, "set_attr");
    }

    #[test]
    fn fixture_files_are_sharded_below_a_dedicated_directory() {
        let (checkpoint_events, _, _) =
            build_fixture_events("fixture-root").expect("fixture events");

        let fixture_root = child_ino(&checkpoint_events, ROOT_INO, "fixture-root");
        let files_root = child_ino(&checkpoint_events, fixture_root, "fixture-files");
        let first_shard = child_ino(&checkpoint_events, files_root, "shard-00");
        let last_shard = child_ino(&checkpoint_events, files_root, "shard-63");

        assert_eq!(
            child_ino(&checkpoint_events, first_shard, "file-00000.bin"),
            FIXTURE_FIRST_FILE_INO
        );
        assert_eq!(
            child_ino(&checkpoint_events, last_shard, "file-04095.bin"),
            FIXTURE_FIRST_FILE_INO + 4095
        );
    }

    #[test]
    fn fixture_batches_are_contiguous_and_bounded() {
        let (_, events, _) = build_fixture_events("fixture-root").expect("fixture events");

        for (index, event) in events.iter().enumerate() {
            let expected_batch = FIXTURE_CHECKPOINT_VERSION
                + (index / LARGE_HISTORY_FIXTURE_EVENTS_PER_BATCH) as i64
                + 1;
            assert_eq!(event.batch_version, expected_batch);
        }
    }

    #[test]
    fn moving_head_fixture_has_exact_mutation_count_and_bounded_batches() {
        let events = build_moving_head_events(42).expect("moving-head events");

        assert_eq!(events.len(), LARGE_HISTORY_MOVING_HEAD_MUTATION_COUNT);
        assert_eq!(events.first().expect("first event").batch_version, 42);
        let expected_last_batch = 42
            + ((LARGE_HISTORY_MOVING_HEAD_MUTATION_COUNT - 1)
                / LARGE_HISTORY_FIXTURE_EVENTS_PER_BATCH) as i64;
        assert_eq!(
            events.last().expect("last event").batch_version,
            expected_last_batch
        );
        assert!(events.iter().all(|event| event.event_type == "set_attr"));
    }

    fn child_ino(events: &[MetaEvent], parent: u64, name: &str) -> u64 {
        events
            .iter()
            .find_map(|event| match event {
                MetaEvent::AddEntry {
                    parent: event_parent,
                    name: event_name,
                    entry,
                } if *event_parent == parent && event_name == name.as_bytes() => Some(entry.ino),
                _ => None,
            })
            .unwrap_or_else(|| panic!("missing fixture entry {parent}:{name}"))
    }
}

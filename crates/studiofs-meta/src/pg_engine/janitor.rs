use super::{PgMetaEngine, PgTimeoutContext};
use crate::engine::{MetaError, MetaResult};
use crate::janitor::JanitorMetadataError;
use crate::pg_transport::connect_studiofs_postgres;
use crate::schema::FormatInfo;
use std::collections::HashSet;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

const PG_JANITOR_LOCK_SALT: i64 = 0x5f53_4653_5f4a_414e;

/// Objects retained by live or leased snapshots; shared by mounted and CLI janitors.
const SNAPSHOT_PIN_QUERY: &str = "SELECT DISTINCT reachability.object_id
    FROM snapshot_object_reachability AS reachability
    JOIN snapshot_catalog AS catalog
      ON catalog.vol = reachability.vol
     AND catalog.cut_version >= reachability.start_version
     AND (reachability.end_version IS NULL
          OR catalog.cut_version < reachability.end_version)
    WHERE reachability.vol = $1
      AND (
          catalog.lifecycle IN ('creating', 'ready')
          OR (
              catalog.lifecycle = 'deleting'
              AND EXISTS (
                  SELECT 1
                  FROM snapshot_mount_leases AS mount_lease
                  WHERE mount_lease.vol = catalog.vol
                    AND mount_lease.snapshot_id = catalog.id
                    AND mount_lease.released_at IS NULL
                    AND mount_lease.lease_expires_at > clock_timestamp()
              )
          )
      )";

pub(crate) struct PgJanitorSession {
    postgres_url: String,
    volume_id: i64,
    connect_timeout: Duration,
    command_timeout: Duration,
    runtime: Option<Runtime>,
    client: Option<Client>,
    connection_task: Option<JoinHandle<()>>,
}

pub(crate) struct PgJanitorProtection {
    pub(crate) referenced: HashSet<u64>,
    pub(crate) snapshot_pins: HashSet<u64>,
}

impl PgJanitorSession {
    pub(crate) fn new(postgres_url: String, volume_id: i64) -> Self {
        Self {
            postgres_url,
            volume_id,
            connect_timeout: PgMetaEngine::default_connect_timeout(),
            command_timeout: PgMetaEngine::default_command_timeout(),
            runtime: None,
            client: None,
            connection_task: None,
        }
    }

    pub(crate) fn lock_is_held(&self) -> bool {
        self.client.is_some()
    }

    pub(crate) fn try_acquire_global_janitor_lock(&mut self) -> MetaResult<bool> {
        if self.client.is_some() {
            return Ok(false);
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|error| {
                MetaError::Storage(format!("failed to build janitor runtime: {error}"))
            })?;
        let connect = connect_studiofs_postgres(&self.postgres_url);
        let (client, connection) = runtime
            .block_on(async { tokio::time::timeout(self.connect_timeout, connect).await })
            .map_err(|_| {
                PgMetaEngine::pg_timeout_error(
                    PgTimeoutContext::Connection("janitor lock connect"),
                    self.connect_timeout,
                )
            })??;
        let connection_task = runtime.spawn(async move {
            let _ = connection.await;
        });
        self.runtime = Some(runtime);
        self.client = Some(client);
        self.connection_task = Some(connection_task);

        let lock_key = pg_janitor_lock_key(self.volume_id);
        let result = match (self.runtime.as_ref(), self.client.as_ref()) {
            (Some(runtime), Some(client)) => runtime.block_on(async {
                tokio::time::timeout(
                    self.command_timeout,
                    client.query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key]),
                )
                .await
            }),
            _ => {
                self.close_connection();
                return Err(MetaError::Storage(
                    "janitor PostgreSQL session was not initialized".to_owned(),
                ));
            }
        };
        match result {
            Ok(Ok(row)) => {
                let acquired = row.get::<usize, bool>(0);
                if !acquired {
                    self.close_connection();
                }
                Ok(acquired)
            }
            Ok(Err(error)) => {
                self.close_connection();
                Err(PgMetaEngine::classify_pg_error(
                    "postgres janitor lock acquire failed",
                    error,
                ))
            }
            Err(_) => {
                self.close_connection();
                Err(PgMetaEngine::pg_timeout_error(
                    PgTimeoutContext::Command("janitor lock acquire"),
                    self.command_timeout,
                ))
            }
        }
    }

    pub(crate) fn release_global_janitor_lock(&mut self) -> MetaResult<()> {
        if self.client.is_none() {
            return Ok(());
        }
        let lock_key = pg_janitor_lock_key(self.volume_id);
        let result = match (self.runtime.as_ref(), self.client.as_ref()) {
            (Some(runtime), Some(client)) => runtime.block_on(async {
                tokio::time::timeout(
                    self.command_timeout,
                    client.query_one("SELECT pg_advisory_unlock($1)", &[&lock_key]),
                )
                .await
            }),
            _ => {
                self.close_connection();
                return Err(MetaError::Storage(
                    "janitor PostgreSQL session was not initialized".to_owned(),
                ));
            }
        };
        self.close_connection();
        match result {
            Ok(Ok(row)) if row.get::<usize, bool>(0) => Ok(()),
            Ok(Ok(_)) => Err(MetaError::Storage(
                "postgres janitor lock was not held during release".to_owned(),
            )),
            Ok(Err(error)) => Err(PgMetaEngine::classify_pg_error(
                "postgres janitor lock release failed",
                error,
            )),
            Err(_) => Err(PgMetaEngine::pg_timeout_error(
                PgTimeoutContext::Command("janitor lock release"),
                self.command_timeout,
            )),
        }
    }

    fn close_connection(&mut self) {
        drop(self.client.take());
        if let Some(task) = self.connection_task.take() {
            task.abort();
            if let Some(runtime) = self.runtime.as_ref() {
                let _ = runtime.block_on(task);
            }
        }
        self.runtime.take();
    }

    pub(crate) fn read_protection(
        &mut self,
        local_info: &FormatInfo,
        local_version: u64,
    ) -> Result<PgJanitorProtection, JanitorMetadataError> {
        let (Some(client), Some(runtime)) = (self.client.as_mut(), self.runtime.as_ref()) else {
            return Err(JanitorMetadataError::UnsafeToDelete(
                "PostgreSQL janitor lock is not held",
            ));
        };
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let local_info = local_info.clone();
        let result = runtime.block_on(async {
            tokio::time::timeout(command_timeout, async {
                let transaction = client.transaction().await.map_err(|error| {
                    janitor_pg_error("janitor metadata transaction failed", error)
                })?;
                transaction
                    .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor read-only transaction failed", error)
                    })?;

                let row = transaction
                    .query_opt(
                        "SELECT volumes.uuid, metadata_state.version, metadata_state.metadata_format_version, metadata_state.format_info, metadata_state.checkpoint_publication_state, metadata_state.checkpoint_generation FROM volumes INNER JOIN metadata_state ON metadata_state.vol = volumes.vol WHERE volumes.vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor volume identity query failed", error)
                    })?
                    .ok_or(JanitorMetadataError::UnsafeToDelete(
                        "PostgreSQL volume binding is missing",
                    ))?;

                let remote_uuid: String = row.get(0);
                let remote_head: i64 = row.get(1);
                let metadata_format_version: Option<i16> = row.get(2);
                let remote_format_bytes: Vec<u8> = row.get(3);
                let publication_state: String = row.get(4);
                let generation: i64 = row.get(5);
                if remote_head < 0
                    || publication_state != "published"
                    || generation <= 0
                    || remote_format_bytes.is_empty()
                {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "PostgreSQL storage binding is incomplete",
                    ));
                }
                crate::ensure_supported_pg_sync_format_version(metadata_format_version)
                    .map_err(|_| {
                        JanitorMetadataError::UnsafeToDelete("PostgreSQL format is unsupported")
                    })?;
                let remote_info = crate::decode_format_info(&remote_format_bytes).map_err(|_| {
                    JanitorMetadataError::UnsafeToDelete("PostgreSQL storage binding is invalid")
                })?;
                if remote_uuid != local_info.uuid
                    || remote_info.name != local_info.name
                    || remote_info.uuid != local_info.uuid
                    || remote_info.endpoint != local_info.endpoint
                    || remote_info.bucket != local_info.bucket
                    || remote_info.data_prefix != local_info.data_prefix
                    || remote_info.metadata.backend != crate::schema::MetadataBackend::Postgres
                    || remote_info
                        .metadata
                        .postgres
                        .as_ref()
                        .is_none_or(|config| config.volume_id != volume_id)
                {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "PostgreSQL volume identity or storage binding does not match",
                    ));
                }
                if i64::try_from(local_version).ok() != Some(remote_head) {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "local metadata is not synchronized with PostgreSQL",
                    ));
                }

                let bootstrap = transaction
                    .query_opt(
                        "SELECT initialized_version FROM snapshot_reachability_bootstrap WHERE vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor snapshot bootstrap query failed", error)
                    })?;
                let Some(bootstrap) = bootstrap else {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "snapshot reachability bootstrap is incomplete",
                    ));
                };
                let initialized_version: i64 = bootstrap.get(0);
                if !(0..=remote_head).contains(&initialized_version) {
                    return Err(JanitorMetadataError::UnsafeToDelete(
                        "snapshot reachability bootstrap is incomplete",
                    ));
                }
                let reference_rows = transaction
                    .query(
                        "SELECT DISTINCT object_id FROM snapshot_current_references WHERE vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor current-reference query failed", error)
                    })?;
                let pin_rows = transaction
                    .query(
                        SNAPSHOT_PIN_QUERY,
                        &[&volume_id],
                    )
                    .await
                    .map_err(|error| {
                        janitor_pg_error("janitor snapshot pin query failed", error)
                    })?;

                let referenced = decode_janitor_object_ids(reference_rows)
                    .map_err(|error| janitor_pg_error("janitor current references are invalid", error))?;
                let snapshot_pins = decode_janitor_object_ids(pin_rows)
                    .map_err(|error| janitor_pg_error("janitor snapshot pins are invalid", error))?;
                transaction.commit().await.map_err(|error| {
                    janitor_pg_error("janitor metadata read commit failed", error)
                })?;
                Ok(PgJanitorProtection {
                    referenced,
                    snapshot_pins,
                })
            })
            .await
        });
        match result {
            Ok(result) => result,
            Err(_) => Err(JanitorMetadataError::BackendUnavailable(
                "PostgreSQL janitor metadata query timed out".to_owned(),
            )),
        }
    }
}

/// Drops the source error so PostgreSQL details (possibly the DSN) never reach CLI output.
fn janitor_pg_error(context: &'static str, _error: impl std::fmt::Display) -> JanitorMetadataError {
    JanitorMetadataError::BackendUnavailable(context.to_owned())
}

impl Drop for PgJanitorSession {
    fn drop(&mut self) {
        self.close_connection();
    }
}

fn decode_janitor_object_ids(rows: Vec<tokio_postgres::Row>) -> MetaResult<HashSet<u64>> {
    rows.into_iter()
        .map(|row| {
            u64::try_from(row.get::<_, i64>(0))
                .map_err(|_| MetaError::Storage("janitor object id is invalid".to_owned()))
        })
        .collect()
}

pub(super) struct PgJanitorLock {
    pub(super) _runtime: tokio::runtime::Runtime,
    pub(super) _client: tokio_postgres::Client,
    pub(super) connection_task: tokio::task::JoinHandle<()>,
}

impl PgMetaEngine {
    fn janitor_lock_key(&self) -> i64 {
        pg_janitor_lock_key(self.volume_id)
    }

    pub(super) fn try_acquire_pg_janitor_lock(&self) -> MetaResult<bool> {
        let mut state = self
            .janitor_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg janitor lock state poisoned".to_string()))?;
        if state.is_some() {
            return Ok(false);
        }

        let lock_key = self.janitor_lock_key();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| MetaError::Storage(format!("failed to build tokio runtime: {e}")))?;
        let connect = connect_studiofs_postgres(&self.postgres_url);
        let (client, connection) = runtime
            .block_on(async { tokio::time::timeout(self.connect_timeout, connect).await })
            .map_err(|_| {
                Self::pg_timeout_error(
                    PgTimeoutContext::Connection("janitor lock connect"),
                    self.connect_timeout,
                )
            })??;

        let connection_task = runtime.spawn(async move {
            let _ = connection.await;
        });
        let acquired = runtime
            .block_on(async {
                tokio::time::timeout(
                    self.command_timeout,
                    client.query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key]),
                )
                .await
            })
            .map_err(|_| {
                Self::pg_timeout_error(
                    PgTimeoutContext::Command("janitor lock acquire"),
                    self.command_timeout,
                )
            })?
            .map_err(|e| Self::classify_pg_error("postgres janitor lock acquire failed", e))?
            .get::<usize, bool>(0);

        if !acquired {
            connection_task.abort();
            return Ok(false);
        }

        *state = Some(PgJanitorLock {
            _runtime: runtime,
            _client: client,
            connection_task,
        });
        Ok(true)
    }

    pub(super) fn release_pg_janitor_lock(&self) -> MetaResult<()> {
        let mut state = self
            .janitor_lock
            .lock()
            .map_err(|_| MetaError::Storage("pg janitor lock state poisoned".to_string()))?;
        let Some(lock) = state.take() else {
            return Ok(());
        };
        lock.connection_task.abort();
        Ok(())
    }

    pub(super) fn load_pg_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "janitor snapshot pin query",
                move |client| {
                    Box::pin(async move {
                        let bootstrap_ready = client
                            .query_opt(
                                "SELECT initialized_version FROM snapshot_reachability_bootstrap WHERE vol = $1",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres janitor snapshot bootstrap query failed",
                                    e,
                                )
                            })?;
                        if bootstrap_ready.is_none() {
                            return Err(MetaError::Storage(
                                "snapshot reachability bootstrap incomplete".to_owned(),
                            ));
                        }
                        client
                            .query(
                                SNAPSHOT_PIN_QUERY,
                                &[&volume_id],
                            )
                            .await
                            .map_err(|e| {
                                Self::classify_pg_error(
                                    "postgres janitor snapshot pin query failed",
                                    e,
                                )
                            })
                    })
                },
            )
            .await
        })?;

        let mut pins = HashSet::new();
        for row in rows {
            let object_id: i64 = row.get(0);
            let object_id = u64::try_from(object_id).map_err(|_| {
                MetaError::Storage(format!(
                    "snapshot artifact object id must be non-negative: {object_id}"
                ))
            })?;
            pins.insert(object_id);
        }
        Ok(pins)
    }
}

pub(super) fn pg_janitor_lock_key(volume_id: i64) -> i64 {
    volume_id.wrapping_mul(1_000_003) ^ PG_JANITOR_LOCK_SALT
}

const PG_EVENT_LOG_PRUNE_LOCK_SALT: i64 = 0x5f53_4653_5f50_5255;

pub(super) fn pg_event_log_prune_lock_key(volume_id: i64) -> i64 {
    volume_id.wrapping_mul(1_000_003) ^ PG_EVENT_LOG_PRUNE_LOCK_SALT
}

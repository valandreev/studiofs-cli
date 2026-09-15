use super::{PG_JANITOR_LOCK_SALT, PgMetaEngine, PgTimeoutContext};
use crate::engine::{MetaError, MetaResult};
use crate::pg_transport::connect_studiofs_postgres;
use std::collections::HashSet;

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
                                "SELECT DISTINCT reachability.object_id
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
                                   )",
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

use super::{PG_JANITOR_LOCK_SALT, PgMetaEngine};
use crate::engine::{MetaError, MetaResult};
use std::collections::HashSet;

pub(super) struct PgJanitorLock {
    pub(super) _runtime: tokio::runtime::Runtime,
    pub(super) _client: tokio_postgres::Client,
    pub(super) connection_task: tokio::task::JoinHandle<()>,
}

impl PgMetaEngine {
    fn janitor_lock_key(&self) -> i64 {
        self.volume_id.wrapping_mul(1_000_003) ^ PG_JANITOR_LOCK_SALT
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
        let connect = tokio_postgres::connect(&self.postgres_url, tokio_postgres::NoTls);
        let (client, connection) = runtime
            .block_on(async { tokio::time::timeout(self.connect_timeout, connect).await })
            .map_err(|_| Self::pg_timeout_error("janitor lock connect", self.connect_timeout))?
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres connect failed: {}",
                    Self::format_pg_error(e)
                ))
            })?;

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
            .map_err(|_| Self::pg_timeout_error("janitor lock acquire", self.command_timeout))?
            .map_err(|e| {
                MetaError::Storage(format!(
                    "postgres janitor lock acquire failed: {}",
                    Self::format_pg_error(e)
                ))
            })?
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
        let postgres_url = self.postgres_url.clone();
        let volume_id = self.volume_id;
        let connect_timeout = self.connect_timeout;
        let command_timeout = self.command_timeout;
        let rows = self.run_async(async move {
            Self::with_postgres_client_timeout(
                postgres_url,
                connect_timeout,
                command_timeout,
                "janitor snapshot pin query",
                move |client| async move {
                    client
                        .query(
                            "SELECT pinned_object_ids FROM snapshots WHERE vol = $1",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres janitor snapshot pin query failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })
                },
            )
            .await
        })?;

        let mut pins = HashSet::new();
        for row in rows {
            let raw_ids: Vec<i64> = row.get(0);
            for object_id in raw_ids {
                let object_id = u64::try_from(object_id).map_err(|_| {
                    MetaError::Storage(format!(
                        "snapshot pinned object id must be non-negative: {object_id}"
                    ))
                })?;
                pins.insert(object_id);
            }
        }
        Ok(pins)
    }
}

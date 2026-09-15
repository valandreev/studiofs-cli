use super::{PgMetaEngine, janitor::pg_event_log_prune_lock_key, sql};
use crate::engine::{MetaError, MetaResult};
use std::sync::Arc;
use tracing::debug;

/// Diagnostics returned after one bounded event-log prune transaction.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct EventLogPruneResult {
    /// Candidate floor selected from the published checkpoint and retained sources.
    pub(super) safe_floor: i64,
    /// Highest complete batch version removed or confirmed durable after this run.
    pub(super) durable_floor: i64,
    /// Number of event rows removed by this transaction.
    pub(super) pruned_rows: i64,
    /// Cumulative number of rows removed for the volume.
    pub(super) total_pruned_rows: i64,
}

fn nonnegative(value: i64, name: &str) -> MetaResult<i64> {
    if value < 0 {
        return Err(MetaError::Storage(format!(
            "postgres {name} must be nonnegative"
        )));
    }
    Ok(value)
}

/// Reports whether the published checkpoint has no event-log history left to prune.
pub(super) fn should_skip_event_log_prune(progress: Option<(i64, i64)>) -> bool {
    progress.is_none_or(|(checkpoint_version, current_floor)| current_floor >= checkpoint_version)
}

pub(super) fn calculate_safe_prune_floor(
    checkpoint_version: i64,
    source_floor: Option<i64>,
    current_floor: i64,
) -> MetaResult<i64> {
    let checkpoint_version = nonnegative(checkpoint_version, "checkpoint version")?;
    let current_floor = nonnegative(current_floor, "event-log prune floor")?;
    let source_floor = source_floor
        .map(|floor| nonnegative(floor, "snapshot source prune floor"))
        .transpose()?;
    let safe_floor = checkpoint_version.min(source_floor.unwrap_or(checkpoint_version));
    if safe_floor < current_floor {
        return Err(MetaError::Storage(
            "postgres event-log prune floor would move backwards".to_string(),
        ));
    }
    Ok(safe_floor)
}

impl PgMetaEngine {
    pub(super) fn validate_event_log_prune_batch_limit(batch_limit: i64) -> MetaResult<()> {
        if batch_limit <= 0 {
            return Err(MetaError::Storage(
                "postgres event-log prune batch limit must be positive".to_string(),
            ));
        }
        Ok(())
    }

    /// Runs one bounded, lock-fenced event-log retention transaction.
    ///
    /// Returns `None` when pruning is not currently eligible or another
    /// maintenance attempt owns the per-volume advisory lock.
    pub(super) fn run_event_log_prune_once(&self) -> MetaResult<Option<EventLogPruneResult>> {
        Self::validate_event_log_prune_batch_limit(self.event_log_prune_batches)?;

        let command_pool = Arc::clone(&self.command_pool);
        let command_timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let lock_key = pg_event_log_prune_lock_key(volume_id);
        let batch_limit = self.event_log_prune_batches;
        let result = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "prune postgres event log",
                move |client| {
                    Box::pin(async move {
                        let progress = if let Some(state) = client
                            .query_opt(sql::SELECT_EVENT_LOG_PRUNE_PROGRESS, &[&volume_id])
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log prune progress query failed",
                                    error,
                                )
                            })? {
                            let checkpoint_version =
                                state.try_get::<_, i64>(0).map_err(|error| {
                                    Self::classify_pg_error(
                                        "postgres event-log checkpoint version parse failed",
                                        error,
                                    )
                                })?;
                            let current_floor = state.try_get::<_, i64>(1).map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log prune floor parse failed",
                                    error,
                                )
                            })?;
                            Some((checkpoint_version, current_floor))
                        } else {
                            None
                        };
                        if should_skip_event_log_prune(progress) {
                            return Ok(None);
                        }

                        let tx = client.transaction().await.map_err(|error| {
                            Self::classify_pg_error(
                                "postgres event-log prune transaction failed",
                                error,
                            )
                        })?;
                        let lock_acquired = tx
                            .query_one(sql::TRY_EVENT_LOG_PRUNE_ADVISORY_LOCK, &[&lock_key])
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log prune advisory lock failed",
                                    error,
                                )
                            })?
                            .try_get::<_, bool>(0)
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log prune advisory lock result parse failed",
                                    error,
                                )
                            })?;
                        if !lock_acquired {
                            return Ok(None);
                        }

                        let Some(state) = tx
                            .query_opt(sql::SELECT_EVENT_LOG_PRUNE_STATE, &[&volume_id])
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log prune state query failed",
                                    error,
                                )
                            })?
                        else {
                            return Ok(None);
                        };

                        let checkpoint_version = nonnegative(
                            state.try_get(0).map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log checkpoint version parse failed",
                                    error,
                                )
                            })?,
                            "checkpoint version",
                        )?;
                        let current_floor = nonnegative(
                            state.try_get(1).map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log prune floor parse failed",
                                    error,
                                )
                            })?,
                            "event-log prune floor",
                        )?;
                        let source_floor = tx
                            .query_one(sql::SELECT_EVENT_LOG_SOURCE_PRUNE_FLOOR, &[&volume_id])
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log snapshot retention query failed",
                                    error,
                                )
                            })?
                            .try_get::<_, Option<i64>>(0)
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log snapshot retention parse failed",
                                    error,
                                )
                            })?
                            .map(|floor| nonnegative(floor, "snapshot source prune floor"))
                            .transpose()?;
                        let safe_floor = calculate_safe_prune_floor(
                            checkpoint_version,
                            source_floor,
                            current_floor,
                        )?;

                        if current_floor >= safe_floor {
                            return Ok(None);
                        }

                        let deleted = tx
                            .query_one(
                                sql::DELETE_EVENT_LOG_PRUNE_BATCH,
                                &[&volume_id, &safe_floor, &batch_limit],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log prune delete failed",
                                    error,
                                )
                            })?;
                        let pruned_rows = nonnegative(
                            deleted.try_get(0).map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log pruned-row count parse failed",
                                    error,
                                )
                            })?,
                            "pruned rows",
                        )?;
                        let max_deleted_batch = deleted
                            .try_get::<_, Option<i64>>(1)
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log deleted batch version parse failed",
                                    error,
                                )
                            })?
                            .map(|batch_version| {
                                nonnegative(batch_version, "deleted batch version")
                            })
                            .transpose()?;

                        let has_remaining = tx
                            .query_opt(
                                sql::SELECT_EVENT_LOG_REMAINING_BELOW_FLOOR,
                                &[&volume_id, &safe_floor],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log remaining-row query failed",
                                    error,
                                )
                            })?
                            .is_some();
                        let durable_floor = if has_remaining {
                            current_floor.max(max_deleted_batch.unwrap_or(current_floor))
                        } else {
                            safe_floor
                        };
                        let state = tx
                            .query_one(
                                sql::UPDATE_EVENT_LOG_PRUNE_STATE,
                                &[&volume_id, &durable_floor, &pruned_rows],
                            )
                            .await
                            .map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres event-log prune state update failed",
                                    error,
                                )
                            })?;
                        let durable_floor = nonnegative(
                            state.try_get(0).map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres durable event-log prune floor parse failed",
                                    error,
                                )
                            })?,
                            "durable event-log prune floor",
                        )?;
                        let total_pruned_rows = nonnegative(
                            state.try_get(1).map_err(|error| {
                                Self::classify_pg_error(
                                    "postgres total event-log pruned-row counter parse failed",
                                    error,
                                )
                            })?,
                            "total event-log pruned rows",
                        )?;

                        tx.commit().await.map_err(|error| {
                            Self::classify_pg_error("postgres event-log prune commit failed", error)
                        })?;
                        Ok(Some(EventLogPruneResult {
                            safe_floor,
                            durable_floor,
                            pruned_rows,
                            total_pruned_rows,
                        }))
                    })
                },
            )
            .await
        });
        if let Ok(Some(prune)) = &result {
            debug!(
                volume_id = self.volume_id,
                safe_prune_floor = prune.safe_floor,
                durable_prune_floor = prune.durable_floor,
                pruned_rows = prune.pruned_rows,
                total_pruned_rows = prune.total_pruned_rows,
                "postgres event-log prune maintenance completed"
            );
        }
        result
    }
}

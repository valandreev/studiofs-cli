use super::PgMetaEngine;
use crate::engine::{MetaError, MetaResult};
use crate::snapshot::{SnapshotDailyPolicy, SnapshotPolicyRecord};
use crate::snapshot_ports::{SnapshotPolicyStore, SnapshotPortError, SnapshotPortFuture};
use chrono::NaiveTime;
use std::sync::Arc;
use std::time::Duration;

const STALE_POLICY: &str = "snapshot policy generation fenced";

fn map_error(operation: &'static str, error: MetaError) -> SnapshotPortError {
    if matches!(&error, MetaError::Storage(message) if message == STALE_POLICY) {
        SnapshotPortError::permanent(
            "stale_policy_generation",
            "snapshot policy generation is no longer current",
        )
    } else {
        SnapshotPortError::from_backend(operation, &error.to_string())
    }
}

fn pg_failure(operation: &'static str) -> MetaError {
    MetaError::RemoteUnavailable(format!("snapshot PostgreSQL operation failed: {operation}"))
}

fn retention_seconds(policy: &SnapshotDailyPolicy) -> Result<i64, SnapshotPortError> {
    policy
        .retention()
        .validate_for_origin(crate::snapshot::SnapshotOrigin::Automatic)
        .map_err(|_| {
            SnapshotPortError::permanent("invalid_policy", "policy retention is invalid")
        })?;
    i64::try_from(policy.retention().ttl().map_or(0, |ttl| ttl.as_secs())).map_err(|_| {
        SnapshotPortError::permanent("invalid_policy", "policy retention is too large")
    })
}

fn parse_policy_row(
    generation: i64,
    daily_at: NaiveTime,
    timezone: String,
    retention_seconds: i64,
) -> MetaResult<SnapshotPolicyRecord> {
    if generation <= 0 || retention_seconds <= 0 {
        return Err(MetaError::Storage(
            "invalid snapshot policy row".to_string(),
        ));
    }
    let policy = SnapshotDailyPolicy::new(
        daily_at,
        timezone,
        Duration::from_secs(
            u64::try_from(retention_seconds)
                .map_err(|_| MetaError::Storage("invalid snapshot policy retention".to_string()))?,
        ),
    )
    .map_err(|_| MetaError::Storage("invalid snapshot policy row".to_string()))?;
    Ok(SnapshotPolicyRecord { generation, policy })
}

impl SnapshotPolicyStore for PgMetaEngine {
    fn set_policy<'a>(
        &'a self,
        policy: &'a SnapshotDailyPolicy,
        expected_generation: Option<i64>,
    ) -> SnapshotPortFuture<'a, SnapshotPolicyRecord> {
        let policy = policy.clone();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let retention_seconds = retention_seconds(&policy)?;
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "set snapshot policy",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin policy set"))?;
                        tx.query_opt(
                            "SELECT vol FROM volumes WHERE vol = $1 FOR UPDATE",
                            &[&volume_id],
                        )
                        .await
                        .map_err(|_| pg_failure("lock policy volume"))?
                        .ok_or_else(|| pg_failure("policy volume is missing"))?;
                        let current = tx
                            .query_opt(
                                "SELECT generation FROM snapshot_policies WHERE vol = $1 FOR UPDATE",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|_| pg_failure("read policy generation"))?
                            .map(|row| row.get::<_, i64>(0));
                        if let Some(expected_generation) = expected_generation
                            && current != Some(expected_generation)
                        {
                            return Err(MetaError::Storage(STALE_POLICY.to_string()));
                        }
                        let generation = current
                            .unwrap_or(0)
                            .checked_add(1)
                            .ok_or_else(|| MetaError::Storage("policy generation overflow".to_string()))?;
                        let daily_at = policy.daily_at();
                        let timezone = policy.timezone().to_string();
                        tx.execute(
                            "INSERT INTO snapshot_policies (vol, generation, daily_at, timezone, retention_seconds, enabled) VALUES ($1, $2, $3, $4, $5, TRUE) ON CONFLICT (vol) DO UPDATE SET generation = EXCLUDED.generation, daily_at = EXCLUDED.daily_at, timezone = EXCLUDED.timezone, retention_seconds = EXCLUDED.retention_seconds, enabled = TRUE, updated_at = clock_timestamp()",
                            &[
                                &volume_id,
                                &generation,
                                &daily_at,
                                &timezone,
                                &retention_seconds,
                            ],
                        )
                        .await
                        .map_err(|_| pg_failure("store snapshot policy"))?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit policy set"))?;
                        Ok(SnapshotPolicyRecord { generation, policy })
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("set snapshot policy", error))
        })
    }

    fn load_policy<'a>(&'a self) -> SnapshotPortFuture<'a, Option<SnapshotPolicyRecord>> {
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "load snapshot policy",
                |client| {
                    Box::pin(async move {
                        let row = client
                            .query_opt(
                                "SELECT generation, daily_at, timezone, retention_seconds FROM snapshot_policies WHERE vol = $1 AND enabled = TRUE",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|_| pg_failure("load snapshot policy"))?;
                        row.map(|row| {
                            parse_policy_row(
                                row.get(0),
                                row.get(1),
                                row.get(2),
                                row.get(3),
                            )
                        })
                        .transpose()
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("load snapshot policy", error))
        })
    }

    fn cancel_policy<'a>(&'a self, expected_generation: i64) -> SnapshotPortFuture<'a, ()> {
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "cancel snapshot policy",
                |client| {
                    Box::pin(async move {
                        let updated = client
                            .execute(
                                "UPDATE snapshot_policies SET enabled = FALSE, updated_at = clock_timestamp() WHERE vol = $1 AND enabled = TRUE AND generation = $2",
                                &[&volume_id, &expected_generation],
                            )
                            .await
                            .map_err(|_| pg_failure("cancel snapshot policy"))?;
                        if updated != 1 {
                            return Err(MetaError::Storage(STALE_POLICY.to_string()));
                        }
                        Ok(())
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("cancel snapshot policy", error))
        })
    }
}

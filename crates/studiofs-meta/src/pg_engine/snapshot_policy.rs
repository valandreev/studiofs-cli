use super::{PgMetaEngine, snapshot_control};
use crate::engine::{MetaError, MetaResult};
use crate::snapshot::{
    SnapshotDailyPolicy, SnapshotOrigin, SnapshotPolicyEvaluation, SnapshotPolicyEvaluatorLease,
    SnapshotPolicyRecord, SnapshotPolicySlot,
};
use crate::snapshot_ports::{
    SnapshotPolicyEvaluatorId, SnapshotPolicyStore, SnapshotPortError, SnapshotPortFuture,
};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, NaiveTime, Utc};
use std::sync::Arc;
use std::time::Duration;

const STALE_POLICY: &str = "snapshot policy generation fenced";
const POLICY_VOLUME_MISSING: &str = "snapshot policy volume is missing";
const STALE_EVALUATOR_LEASE: &str = "snapshot policy evaluator lease fenced";

fn map_error(operation: &'static str, error: MetaError) -> SnapshotPortError {
    match &error {
        MetaError::Storage(message) if message == STALE_POLICY => SnapshotPortError::permanent(
            "stale_policy_generation",
            "snapshot policy generation is no longer current",
        ),
        MetaError::Storage(message) if message == POLICY_VOLUME_MISSING => {
            SnapshotPortError::permanent(
                "volume_not_found",
                "snapshot policy volume does not exist",
            )
        }
        MetaError::Storage(message) if message == STALE_EVALUATOR_LEASE => {
            SnapshotPortError::permanent(
                "stale_evaluator_lease",
                "snapshot policy evaluator lease is no longer current",
            )
        }
        _ => SnapshotPortError::from_backend(operation, &error.to_string()),
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

fn lease_millis(lease_for: Duration) -> Result<i64, SnapshotPortError> {
    let millis = i64::try_from(lease_for.as_millis()).map_err(|_| {
        SnapshotPortError::permanent("invalid_lease", "lease duration is too large")
    })?;
    if millis <= 0 {
        return Err(SnapshotPortError::permanent(
            "invalid_lease",
            "lease duration must be positive",
        ));
    }
    Ok(millis)
}

fn next_poll(now: DateTime<Utc>) -> DateTime<Utc> {
    now + ChronoDuration::seconds(30)
}

fn next_policy_wake(
    policy: &SnapshotDailyPolicy,
    local_date: NaiveDate,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    local_date
        .succ_opt()
        .and_then(|next_date| policy.resolve_local_slot(next_date).ok())
        .and_then(|slot| match slot {
            SnapshotPolicySlot::Due { due_at, .. } if due_at > now => Some(due_at),
            _ => None,
        })
        .unwrap_or_else(|| next_poll(now))
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
                        .ok_or_else(|| MetaError::Storage(POLICY_VOLUME_MISSING.to_string()))?;
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

    fn claim_evaluator_lease<'a>(
        &'a self,
        context: &'a crate::snapshot::SnapshotVolumeContext,
        owner: &'a SnapshotPolicyEvaluatorId,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotPolicyEvaluatorLease> {
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let owner = owner.as_str().to_string();
        let context_volume_id = context.volume_id;
        Box::pin(async move {
            if context_volume_id != volume_id {
                return Err(SnapshotPortError::permanent(
                    "volume_mismatch",
                    "snapshot policy volume context does not match engine",
                ));
            }
            let lease_ms = lease_millis(lease_for)?;
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "claim snapshot policy evaluator lease",
                |client| {
                    Box::pin(async move {
                        client
                            .query_opt(
                                "INSERT INTO snapshot_policy_evaluator_leases (vol, owner, lease_expires_at, fencing_token) VALUES ($1, $2, clock_timestamp() + ($3::bigint * interval '1 millisecond'), 1) ON CONFLICT (vol) DO UPDATE SET owner = EXCLUDED.owner, lease_expires_at = EXCLUDED.lease_expires_at, fencing_token = CASE WHEN snapshot_policy_evaluator_leases.owner = EXCLUDED.owner AND snapshot_policy_evaluator_leases.lease_expires_at > clock_timestamp() THEN snapshot_policy_evaluator_leases.fencing_token ELSE snapshot_policy_evaluator_leases.fencing_token + 1 END, acquired_at = CASE WHEN snapshot_policy_evaluator_leases.owner = EXCLUDED.owner AND snapshot_policy_evaluator_leases.lease_expires_at > clock_timestamp() THEN snapshot_policy_evaluator_leases.acquired_at ELSE clock_timestamp() END, renewed_at = clock_timestamp() WHERE snapshot_policy_evaluator_leases.owner = EXCLUDED.owner OR snapshot_policy_evaluator_leases.lease_expires_at <= clock_timestamp() RETURNING owner, lease_expires_at, fencing_token",
                                &[&volume_id, &owner, &lease_ms],
                            )
                            .await
                            .map_err(|_| pg_failure("claim snapshot policy evaluator lease"))?
                            .map(|row| SnapshotPolicyEvaluatorLease {
                                owner: row.get(0),
                                expires_at: row.get(1),
                                fencing_token: row.get(2),
                            })
                            .ok_or_else(|| MetaError::Storage(STALE_EVALUATOR_LEASE.to_string()))
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("claim snapshot policy evaluator lease", error))
        })
    }

    fn renew_evaluator_lease<'a>(
        &'a self,
        lease: &'a SnapshotPolicyEvaluatorLease,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotPolicyEvaluatorLease> {
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let lease = lease.clone();
        Box::pin(async move {
            let lease_ms = lease_millis(lease_for)?;
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "renew snapshot policy evaluator lease",
                |client| {
                    Box::pin(async move {
                        client
                            .query_opt(
                                "UPDATE snapshot_policy_evaluator_leases SET lease_expires_at = clock_timestamp() + ($3::bigint * interval '1 millisecond'), renewed_at = clock_timestamp() WHERE vol = $1 AND owner = $2 AND fencing_token = $4 AND lease_expires_at > clock_timestamp() RETURNING owner, lease_expires_at, fencing_token",
                                &[&volume_id, &lease.owner, &lease_ms, &lease.fencing_token],
                            )
                            .await
                            .map_err(|_| pg_failure("renew snapshot policy evaluator lease"))?
                            .map(|row| SnapshotPolicyEvaluatorLease {
                                owner: row.get(0),
                                expires_at: row.get(1),
                                fencing_token: row.get(2),
                            })
                            .ok_or_else(|| MetaError::Storage(STALE_EVALUATOR_LEASE.to_string()))
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("renew snapshot policy evaluator lease", error))
        })
    }

    fn evaluate_due_slot<'a>(
        &'a self,
        context: &'a crate::snapshot::SnapshotVolumeContext,
        lease: &'a SnapshotPolicyEvaluatorLease,
        now: DateTime<Utc>,
    ) -> SnapshotPortFuture<'a, SnapshotPolicyEvaluation> {
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let lease = lease.clone();
        let context_volume_id = context.volume_id;
        Box::pin(async move {
            if context_volume_id != volume_id {
                return Err(SnapshotPortError::permanent(
                    "volume_mismatch",
                    "snapshot policy volume context does not match engine",
                ));
            }
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "evaluate snapshot policy slot",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot policy evaluation"))?;
                        tx.query_opt(
                            "SELECT 1 FROM snapshot_policy_evaluator_leases WHERE vol = $1 AND owner = $2 AND fencing_token = $3 AND lease_expires_at > clock_timestamp() FOR UPDATE",
                            &[&volume_id, &lease.owner, &lease.fencing_token],
                        )
                        .await
                        .map_err(|_| pg_failure("lock snapshot policy evaluator lease"))?
                        .ok_or_else(|| MetaError::Storage(STALE_EVALUATOR_LEASE.to_string()))?;
                        let Some(row) = tx
                            .query_opt(
                                "SELECT generation, daily_at, timezone, retention_seconds, latest_completed_slot FROM snapshot_policies WHERE vol = $1 AND enabled = TRUE",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|_| pg_failure("load active snapshot policy"))?
                        else {
                            tx.commit()
                                .await
                                .map_err(|_| pg_failure("commit empty snapshot policy evaluation"))?;
                            return Ok(SnapshotPolicyEvaluation::NoPolicy {
                                next_wake_at: next_poll(now),
                            });
                        };
                        let generation: i64 = row.get(0);
                        let policy = parse_policy_row(
                            generation,
                            row.get(1),
                            row.get(2),
                            row.get(3),
                        )?;
                        let latest_completed: Option<DateTime<Utc>> = row.get(4);
                        let Some(slot) = policy.policy.latest_due_slot(now, latest_completed) else {
                            tx.commit()
                                .await
                                .map_err(|_| pg_failure("commit idle snapshot policy evaluation"))?;
                            return Ok(SnapshotPolicyEvaluation::NoPolicy {
                                next_wake_at: next_poll(now),
                            });
                        };
                        match slot {
                            SnapshotPolicySlot::SkippedDstGap { local_date } => {
                                let next_wake_at = next_policy_wake(&policy.policy, local_date, now);
                                let inserted = tx
                                    .execute(
                                        "INSERT INTO snapshot_policy_slots (vol, generation, local_date, daily_at, slot_kind) VALUES ($1, $2, $3, $4, 'skipped_dst_gap') ON CONFLICT (vol, generation, local_date, daily_at) DO NOTHING",
                                        &[&volume_id, &generation, &local_date, &policy.policy.daily_at()],
                                    )
                                    .await
                                    .map_err(|_| pg_failure("record skipped snapshot policy slot"))?;
                                if inserted == 0 {
                                    tx.commit()
                                        .await
                                        .map_err(|_| pg_failure("commit recorded snapshot policy slot"))?;
                                    return Ok(SnapshotPolicyEvaluation::NoPolicy {
                                        next_wake_at,
                                    });
                                }
                                tx.commit()
                                    .await
                                    .map_err(|_| pg_failure("commit skipped snapshot policy slot"))?;
                                Ok(SnapshotPolicyEvaluation::SkippedDstGap {
                                    next_wake_at,
                                })
                            }
                            SnapshotPolicySlot::Due { local_date, due_at } => {
                                let inserted = tx
                                    .execute(
                                        "INSERT INTO snapshot_policy_slots (vol, generation, local_date, daily_at, slot_kind, due_at) VALUES ($1, $2, $3, $4, 'due', $5) ON CONFLICT (vol, generation, local_date, daily_at) DO NOTHING",
                                        &[&volume_id, &generation, &local_date, &policy.policy.daily_at(), &due_at],
                                )
                                .await
                                .map_err(|_| pg_failure("claim snapshot policy slot"))?;
                                if inserted == 0 {
                                    let linked = tx
                                        .query_one(
                                            "SELECT snapshot_id IS NOT NULL FROM snapshot_policy_slots WHERE vol = $1 AND generation = $2 AND local_date = $3 AND daily_at = $4",
                                            &[&volume_id, &generation, &local_date, &policy.policy.daily_at()],
                                        )
                                        .await
                                        .map_err(|_| pg_failure("load claimed snapshot policy slot"))?
                                        .get::<_, bool>(0);
                                    if !linked {
                                        return Err(pg_failure(
                                            "claimed snapshot policy slot is incomplete",
                                        ));
                                    }
                                    tx.commit()
                                        .await
                                        .map_err(|_| pg_failure("commit followed snapshot policy slot"))?;
                                    return Ok(SnapshotPolicyEvaluation::Followed {
                                        next_wake_at: next_poll(now),
                                    });
                                }
                                let entry = snapshot_control::capture_snapshot_in_transaction(
                                    &tx,
                                    &snapshot_control::SnapshotCaptureRequest {
                                        volume_id,
                                        origin: SnapshotOrigin::Automatic,
                                        retention: policy.policy.retention(),
                                    },
                                )
                                .await?;
                                let snapshot_id = entry.snapshot_id.to_string();
                                tx.execute(
                                    "UPDATE snapshot_policy_slots SET snapshot_id = $5, updated_at = clock_timestamp() WHERE vol = $1 AND generation = $2 AND local_date = $3 AND daily_at = $4",
                                    &[&volume_id, &generation, &local_date, &policy.policy.daily_at(), &snapshot_id],
                                )
                                .await
                                .map_err(|_| pg_failure("link snapshot policy slot"))?;
                                tx.commit()
                                    .await
                                    .map_err(|_| pg_failure("commit created snapshot policy slot"))?;
                                Ok(SnapshotPolicyEvaluation::Created {
                                    next_wake_at: next_poll(now),
                                })
                            }
                        }
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("evaluate snapshot policy slot", error))
        })
    }

    fn release_evaluator_lease<'a>(
        &'a self,
        lease: &'a SnapshotPolicyEvaluatorLease,
    ) -> SnapshotPortFuture<'a, ()> {
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        let lease = lease.clone();
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "release snapshot policy evaluator lease",
                |client| {
                    Box::pin(async move {
                        let released = client
                            .execute(
                                "UPDATE snapshot_policy_evaluator_leases SET owner = '', lease_expires_at = clock_timestamp(), renewed_at = clock_timestamp() WHERE vol = $1 AND owner = $2 AND fencing_token = $3",
                                &[&volume_id, &lease.owner, &lease.fencing_token],
                            )
                            .await
                            .map_err(|_| pg_failure("release snapshot policy evaluator lease"))?;
                        if released != 1 {
                            return Err(MetaError::Storage(STALE_EVALUATOR_LEASE.to_string()));
                        }
                        Ok(())
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("release snapshot policy evaluator lease", error))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn missing_policy_volume_is_permanent() {
        let error = map_error(
            "set snapshot policy",
            MetaError::Storage("snapshot policy volume is missing".to_string()),
        );

        assert!(matches!(
            error,
            SnapshotPortError::Permanent { code, .. } if code.as_str() == "volume_not_found"
        ));
    }

    #[test]
    fn skipped_dst_gap_wakes_at_the_next_local_slot() {
        let policy = SnapshotDailyPolicy::new(
            NaiveTime::from_hms_opt(2, 30, 0).expect("valid policy time"),
            "America/New_York",
            Duration::from_secs(60),
        )
        .expect("valid policy");
        let now = Utc
            .with_ymd_and_hms(2026, 3, 8, 8, 0, 0)
            .single()
            .expect("valid UTC now");

        assert_eq!(
            next_policy_wake(
                &policy,
                NaiveDate::from_ymd_opt(2026, 3, 8).expect("valid local date"),
                now,
            ),
            Utc.with_ymd_and_hms(2026, 3, 9, 6, 30, 0)
                .single()
                .expect("valid next UTC slot")
        );
    }
}

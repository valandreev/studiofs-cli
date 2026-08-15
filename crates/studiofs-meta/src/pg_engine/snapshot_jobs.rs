use super::{PgMetaEngine, SnapshotBackendAccess};
use crate::engine::{MetaError, MetaResult};
use crate::snapshot::{
    SnapshotArtifactDescriptor, SnapshotCatalogEntry, SnapshotCreationJob, SnapshotCreationLease,
    SnapshotCreationState, SnapshotCut, SnapshotId, SnapshotLifecycle, SnapshotRetention,
    SnapshotVolumeContext,
};
use crate::snapshot_ports::{
    SnapshotClaimOrFollow, SnapshotFailure, SnapshotJobClaim, SnapshotJobStore,
    SnapshotPayloadProof, SnapshotPortError, SnapshotPortFuture, SnapshotRetry, SnapshotWorkerId,
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::{GenericClient, Row};

const STALE_LEASE: &str = "snapshot lease fenced";
const SNAPSHOT_NOT_FOUND: &str = "snapshot job not found";

fn lease_millis(lease_for: Duration) -> Result<f64, SnapshotPortError> {
    let millis = i64::try_from(lease_for.as_millis()).map_err(|_| {
        SnapshotPortError::permanent("invalid_lease", "lease duration is too large")
    })?;
    if millis <= 0 {
        return Err(SnapshotPortError::permanent(
            "invalid_lease",
            "lease duration must be positive",
        ));
    }
    Ok(millis as f64)
}

fn map_error(operation: &'static str, error: MetaError) -> SnapshotPortError {
    match &error {
        MetaError::Storage(message) if message == STALE_LEASE => {
            SnapshotPortError::permanent("stale_lease", "snapshot lease is no longer owned")
        }
        MetaError::Storage(message) if message == SNAPSHOT_NOT_FOUND => {
            SnapshotPortError::permanent("snapshot_not_found", "snapshot job does not exist")
        }
        _ => SnapshotPortError::from_backend(operation, &error.to_string()),
    }
}

fn pg_failure(operation: &'static str) -> MetaError {
    MetaError::RemoteUnavailable(format!("snapshot PostgreSQL operation failed: {operation}"))
}

fn parse_creation_state(value: &str) -> MetaResult<SnapshotCreationState> {
    match value {
        "queued" => Ok(SnapshotCreationState::Queued),
        "running" => Ok(SnapshotCreationState::Running),
        "retry_wait" => Ok(SnapshotCreationState::RetryWait),
        "completed" => Ok(SnapshotCreationState::Completed),
        "failed" => Ok(SnapshotCreationState::Failed),
        _ => Err(MetaError::Storage("invalid snapshot job state".to_string())),
    }
}

fn parse_lifecycle(value: &str) -> MetaResult<SnapshotLifecycle> {
    match value {
        "creating" => Ok(SnapshotLifecycle::Creating),
        "ready" => Ok(SnapshotLifecycle::Ready),
        "failed" => Ok(SnapshotLifecycle::Failed),
        "deleting" => Ok(SnapshotLifecycle::Deleting),
        "deleted" => Ok(SnapshotLifecycle::Deleted),
        _ => Err(MetaError::Storage("invalid snapshot lifecycle".to_string())),
    }
}

fn parse_origin(value: &str) -> MetaResult<crate::snapshot::SnapshotOrigin> {
    value
        .parse()
        .map_err(|_| MetaError::Storage("invalid snapshot origin".to_string()))
}

fn parse_retention(
    origin: crate::snapshot::SnapshotOrigin,
    seconds: Option<i64>,
) -> MetaResult<SnapshotRetention> {
    let retention = match seconds {
        Some(seconds) if seconds > 0 => SnapshotRetention::manual(Some(Duration::from_secs(
            u64::try_from(seconds)
                .map_err(|_| MetaError::Storage("invalid snapshot retention".to_string()))?,
        )))
        .map_err(|_| MetaError::Storage("invalid snapshot retention".to_string()))?,
        Some(_) => return Err(MetaError::Storage("invalid snapshot retention".to_string())),
        None => SnapshotRetention::manual(None)
            .map_err(|_| MetaError::Storage("invalid snapshot retention".to_string()))?,
    };
    retention
        .validate_for_origin(origin)
        .map_err(|_| MetaError::Storage("snapshot retention does not match origin".to_string()))?;
    Ok(retention)
}

async fn load_job<C>(
    client: &C,
    volume_id: i64,
    snapshot_id: &str,
) -> MetaResult<SnapshotCreationJob>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT j.state, j.retry_count, j.next_retry_at, j.failure_message, l.owner, l.lease_expires_at, l.fencing_token FROM snapshot_creation_jobs AS j LEFT JOIN snapshot_creation_leases AS l ON l.vol = j.vol AND l.snapshot_id = j.snapshot_id WHERE j.vol = $1 AND j.snapshot_id = $2",
            &[&volume_id, &snapshot_id],
        )
        .await
        .map_err(|_| pg_failure("load snapshot job"))?
        .ok_or_else(|| MetaError::Storage(SNAPSHOT_NOT_FOUND.to_string()))?;
    let snapshot_id = SnapshotId::parse(snapshot_id)
        .map_err(|_| MetaError::Storage("invalid snapshot ID in PostgreSQL".to_string()))?;
    let state: String = row.get(0);
    let retry_count: i32 = row.get(1);
    let retry_count = u32::try_from(retry_count)
        .map_err(|_| MetaError::Storage("invalid snapshot retry count".to_string()))?;
    let owner = row
        .get::<_, Option<String>>(4)
        .filter(|owner| !owner.is_empty());
    let lease_expires_at = owner.as_ref().and_then(|_| row.get(5));
    Ok(SnapshotCreationJob {
        snapshot_id,
        state: parse_creation_state(&state)?,
        retry_count,
        owner,
        lease_expires_at,
        fencing_token: row.get::<_, Option<i64>>(6).unwrap_or(0),
        next_retry_at: row.get(2),
        failure: row.get(3),
    })
}

fn catalog_query(filter: &str) -> String {
    format!(
        "SELECT c.id, v.name, v.uuid, c.origin, c.cut_version, c.captured_at, c.lifecycle, c.retention_seconds, c.ready_at, c.expires_at, c.failure_code, c.failure_message, a.object_key, a.format_version, a.schema_version, a.sha256, a.byte_length, a.verified_at, j.state, j.failure_message FROM snapshot_catalog AS c JOIN volumes AS v ON v.vol = c.vol LEFT JOIN snapshot_artifacts AS a ON a.vol = c.vol AND a.snapshot_id = c.id LEFT JOIN snapshot_creation_jobs AS j ON j.vol = c.vol AND j.snapshot_id = c.id {filter}"
    )
}

fn parse_catalog_row(row: &Row, volume_id: i64) -> MetaResult<SnapshotCatalogEntry> {
    let snapshot_id_text: &str = row.get(0);
    let snapshot_id = SnapshotId::parse(snapshot_id_text)
        .map_err(|_| MetaError::Storage("invalid snapshot ID in PostgreSQL".to_string()))?;
    let volume =
        SnapshotVolumeContext::new(volume_id, row.get::<_, String>(1), row.get::<_, String>(2))
            .map_err(|_| MetaError::Storage("invalid snapshot volume identity".to_string()))?;
    let origin = parse_origin(row.get(3))?;
    let captured_at: DateTime<Utc> = row.get(5);
    let cut = SnapshotCut::new(volume_id, row.get(4), captured_at)
        .map_err(|_| MetaError::Storage("invalid snapshot cut".to_string()))?;
    let lifecycle = parse_lifecycle(row.get(6))?;
    let retention = parse_retention(origin, row.get(7))?;
    let job_state: &str = row
        .get::<_, Option<&str>>(18)
        .ok_or_else(|| MetaError::Storage(SNAPSHOT_NOT_FOUND.to_string()))?;
    let creation_state = parse_creation_state(job_state)?;
    let job_failure: Option<String> = row.get(19);
    let artifact = match row.get::<_, Option<String>>(12) {
        None => None,
        Some(object_key) => {
            let format_version = u16::try_from(row.get::<_, Option<i16>>(13).ok_or_else(|| {
                MetaError::Storage("missing artifact format version".to_string())
            })?)
            .map_err(|_| MetaError::Storage("invalid artifact format version".to_string()))?;
            let schema_version = u16::try_from(row.get::<_, Option<i16>>(14).ok_or_else(|| {
                MetaError::Storage("missing artifact schema version".to_string())
            })?)
            .map_err(|_| MetaError::Storage("invalid artifact schema version".to_string()))?;
            let digest: &[u8] = row
                .get::<_, Option<&[u8]>>(15)
                .ok_or_else(|| MetaError::Storage("missing artifact checksum".to_string()))?;
            let sha256: [u8; 32] = digest
                .try_into()
                .map_err(|_| MetaError::Storage("invalid artifact checksum".to_string()))?;
            let byte_length = u64::try_from(
                row.get::<_, Option<i64>>(16)
                    .ok_or_else(|| MetaError::Storage("missing artifact length".to_string()))?,
            )
            .map_err(|_| MetaError::Storage("invalid artifact length".to_string()))?;
            let verified_at: DateTime<Utc> =
                row.get::<_, Option<DateTime<Utc>>>(17).ok_or_else(|| {
                    MetaError::Storage("missing artifact verification time".to_string())
                })?;
            Some(
                SnapshotArtifactDescriptor::new(
                    object_key,
                    format_version,
                    schema_version,
                    crate::snapshot::SnapshotSha256::from_bytes(sha256),
                    byte_length,
                    cut,
                    verified_at,
                )
                .map_err(|_| MetaError::Storage("invalid snapshot artifact".to_string()))?,
            )
        }
    };

    Ok(SnapshotCatalogEntry {
        snapshot_id,
        volume,
        origin,
        cut,
        lifecycle,
        creation_state,
        retention,
        ready_at: row.get(8),
        expires_at: row.get(9),
        artifact,
        failure: row
            .get::<_, Option<String>>(11)
            .or_else(|| row.get(10))
            .or(job_failure),
    })
}

pub(super) async fn load_catalog<C>(
    client: &C,
    volume_id: i64,
    snapshot_id: &str,
) -> MetaResult<SnapshotCatalogEntry>
where
    C: GenericClient + Sync,
{
    let query = catalog_query("WHERE c.vol = $1 AND c.id = $2");
    let row = client
        .query_opt(&query, &[&volume_id, &snapshot_id])
        .await
        .map_err(|_| pg_failure("load snapshot catalog"))?
        .ok_or_else(|| MetaError::Storage(SNAPSHOT_NOT_FOUND.to_string()))?;
    parse_catalog_row(&row, volume_id)
}

async fn claim_job<C>(
    client: &C,
    volume_id: i64,
    snapshot_id: &str,
    worker: &SnapshotWorkerId,
    lease_ms: f64,
) -> MetaResult<SnapshotJobClaim>
where
    C: GenericClient + Sync,
{
    let worker_text = worker.as_str().to_string();
    let lease = client
        .query_one(
            "INSERT INTO snapshot_creation_leases (vol, snapshot_id, owner, lease_expires_at, fencing_token, acquired_at, renewed_at) VALUES ($1, $2, $3, clock_timestamp() + ($4::double precision * interval '1 millisecond'), 1, clock_timestamp(), clock_timestamp()) ON CONFLICT (vol, snapshot_id) DO UPDATE SET owner = EXCLUDED.owner, lease_expires_at = EXCLUDED.lease_expires_at, fencing_token = snapshot_creation_leases.fencing_token + 1, acquired_at = clock_timestamp(), renewed_at = clock_timestamp() RETURNING lease_expires_at, fencing_token",
            &[&volume_id, &snapshot_id, &worker_text, &lease_ms],
        )
        .await
        .map_err(|_| pg_failure("claim snapshot lease"))?;
    let expires_at: DateTime<Utc> = lease.get(0);
    let fencing_token: i64 = lease.get(1);
    let updated = client
        .execute(
            "UPDATE snapshot_creation_jobs SET state = 'running', next_retry_at = NULL, updated_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2",
            &[&volume_id, &snapshot_id],
        )
        .await
        .map_err(|_| pg_failure("claim snapshot job"))?;
    if updated != 1 {
        return Err(MetaError::Storage(SNAPSHOT_NOT_FOUND.to_string()));
    }
    let job = load_job(client, volume_id, snapshot_id).await?;
    Ok(SnapshotJobClaim {
        job,
        lease: SnapshotCreationLease {
            owner: worker.as_str().to_string(),
            expires_at,
            fencing_token,
        },
    })
}

impl<T: SnapshotBackendAccess + Send + Sync> SnapshotJobStore for T {
    fn claim_next<'a>(
        &'a self,
        worker: &'a SnapshotWorkerId,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, Option<SnapshotJobClaim>> {
        let worker = worker.clone();
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let lease_ms = lease_millis(lease_for)?;
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "claim next snapshot job",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot claim"))?;
                        let row = tx
                            .query_opt(
                                "SELECT j.snapshot_id
                                 FROM snapshot_creation_jobs AS j
                                 JOIN snapshot_catalog AS c
                                   ON c.vol = j.vol
                                  AND c.id = j.snapshot_id
                                 LEFT JOIN snapshot_creation_leases AS l
                                   ON l.vol = j.vol
                                  AND l.snapshot_id = j.snapshot_id
                                 WHERE j.vol = $1
                                   AND j.state IN ('queued', 'retry_wait', 'running')
                                   AND c.lifecycle = 'creating'
                                   AND (j.state <> 'retry_wait'
                                        OR j.next_retry_at IS NULL
                                        OR j.next_retry_at <= clock_timestamp())
                                   AND (l.snapshot_id IS NULL
                                        OR l.lease_expires_at <= clock_timestamp())
                                 ORDER BY j.created_at, j.snapshot_id
                                 LIMIT 1
                                 FOR UPDATE OF j SKIP LOCKED",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|_| pg_failure("find next snapshot job"))?;
                        let Some(row) = row else {
                            tx.commit()
                                .await
                                .map_err(|_| pg_failure("commit empty snapshot claim"))?;
                            return Ok(None);
                        };
                        let snapshot_id: String = row.get(0);
                        let claim =
                            claim_job(&tx, volume_id, &snapshot_id, &worker, lease_ms).await?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot claim"))?;
                        Ok(Some(claim))
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("claim next snapshot job", error))
        })
    }

    fn claim_id_or_follow<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
        worker: &'a SnapshotWorkerId,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotClaimOrFollow> {
        let snapshot_id = snapshot_id.clone();
        let worker = worker.clone();
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let lease_ms = lease_millis(lease_for)?;
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "claim requested snapshot job",
                |client| {
                    Box::pin(async move {
                        let snapshot_id_text = snapshot_id.to_string();
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin requested snapshot claim"))?;
                        let row = tx
                            .query_opt(
                                "SELECT j.state, c.lifecycle, (j.next_retry_at IS NULL OR j.next_retry_at <= clock_timestamp()) AS retry_due, (l.snapshot_id IS NULL OR l.lease_expires_at <= clock_timestamp()) AS lease_available FROM snapshot_creation_jobs AS j JOIN snapshot_catalog AS c ON c.vol = j.vol AND c.id = j.snapshot_id LEFT JOIN snapshot_creation_leases AS l ON l.vol = j.vol AND l.snapshot_id = j.snapshot_id WHERE j.vol = $1 AND j.snapshot_id = $2 FOR UPDATE OF j",
                                &[&volume_id, &snapshot_id_text],
                            )
                            .await
                            .map_err(|_| pg_failure("find requested snapshot job"))?;
                        let Some(row) = row else {
                            return Err(MetaError::Storage(SNAPSHOT_NOT_FOUND.to_string()));
                        };
                        let state: String = row.get(0);
                        let lifecycle: String = row.get(1);
                        let retry_due: bool = row.get(2);
                        let lease_available: bool = row.get(3);
                        let claimable = lease_available
                            && lifecycle == "creating"
                            && (state == "queued"
                                || (state == "retry_wait" && retry_due)
                                || state == "running");
                        if claimable {
                            let claim = claim_job(
                                &tx,
                                volume_id,
                                snapshot_id.as_str(),
                                &worker,
                                lease_ms,
                            )
                            .await?;
                            tx.commit()
                                .await
                                .map_err(|_| pg_failure("commit requested snapshot claim"))?;
                            return Ok(SnapshotClaimOrFollow::Claimed(claim));
                        }
                        let entry = load_catalog(&tx, volume_id, snapshot_id.as_str()).await?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot follow"))?;
                        Ok(SnapshotClaimOrFollow::Follow(entry))
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("claim requested snapshot job", error))
        })
    }

    fn renew_lease<'a>(
        &'a self,
        claim: &'a SnapshotJobClaim,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotCreationLease> {
        let claim = claim.clone();
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let lease_ms = lease_millis(lease_for)?;
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "renew snapshot lease",
                |client| {
                    Box::pin(async move {
                        let row = client
                            .query_opt(
                                "UPDATE snapshot_creation_leases SET lease_expires_at = clock_timestamp() + ($5::double precision * interval '1 millisecond'), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4 AND lease_expires_at > clock_timestamp() RETURNING lease_expires_at, fencing_token",
                                &[
                                    &volume_id,
                                    &claim.job.snapshot_id.to_string(),
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                    &lease_ms,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("renew snapshot lease"))?;
                        let Some(row) = row else {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        };
                        Ok(SnapshotCreationLease {
                            owner: claim.lease.owner,
                            expires_at: row.get(0),
                            fencing_token: row.get(1),
                        })
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("renew snapshot lease", error))
        })
    }

    fn release_lease<'a>(&'a self, claim: &'a SnapshotJobClaim) -> SnapshotPortFuture<'a, ()> {
        let claim = claim.clone();
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "release snapshot lease",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot lease release"))?;
                        let requeued = tx
                            .execute(
                                "UPDATE snapshot_creation_jobs AS j SET state = 'retry_wait', next_retry_at = clock_timestamp(), updated_at = clock_timestamp() WHERE j.vol = $1 AND j.snapshot_id = $2 AND j.state = 'running' AND EXISTS (SELECT 1 FROM snapshot_creation_leases AS l WHERE l.vol = j.vol AND l.snapshot_id = j.snapshot_id AND l.owner = $3 AND l.fencing_token = $4)",
                                &[
                                    &volume_id,
                                    &claim.job.snapshot_id.to_string(),
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("requeue released snapshot job"))?;
                        if requeued != 1 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        let released = tx
                            .execute(
                                "UPDATE snapshot_creation_leases SET owner = '', lease_expires_at = clock_timestamp(), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4",
                                &[
                                    &volume_id,
                                    &claim.job.snapshot_id.to_string(),
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("release snapshot lease"))?;
                        if released != 1 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot lease release"))?;
                        Ok(())
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("release snapshot lease", error))
        })
    }

    fn mark_retry_wait<'a>(
        &'a self,
        claim: &'a SnapshotJobClaim,
        retry: SnapshotRetry,
    ) -> SnapshotPortFuture<'a, SnapshotCreationJob> {
        let claim = claim.clone();
        let message = retry.message.as_str().to_string();
        let retry_count = retry.retry_count;
        let next_retry_at = retry.next_retry_at;
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let retry_count = i32::try_from(retry_count).map_err(|_| {
                SnapshotPortError::permanent("invalid_retry", "retry count is too large")
            })?;
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "mark snapshot retry",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot retry"))?;
                        let updated = tx
                            .execute(
                                "UPDATE snapshot_creation_jobs AS j SET state = 'retry_wait', retry_count = $3, next_retry_at = $4, failure_message = $5, updated_at = clock_timestamp() WHERE j.vol = $1 AND j.snapshot_id = $2 AND j.state = 'running' AND EXISTS (SELECT 1 FROM snapshot_creation_leases AS l WHERE l.vol = j.vol AND l.snapshot_id = j.snapshot_id AND l.owner = $6 AND l.fencing_token = $7 AND l.lease_expires_at > clock_timestamp())",
                                &[
                                    &volume_id,
                                    &claim.job.snapshot_id.to_string(),
                                    &retry_count,
                                    &next_retry_at,
                                    &message,
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("mark snapshot retry"))?;
                        if updated != 1 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        tx.execute(
                            "UPDATE snapshot_creation_leases SET owner = '', lease_expires_at = clock_timestamp(), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4",
                            &[
                                &volume_id,
                                &claim.job.snapshot_id.to_string(),
                                &claim.lease.owner,
                                &claim.lease.fencing_token,
                            ],
                        )
                        .await
                        .map_err(|_| pg_failure("release snapshot retry lease"))?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot retry"))?;
                        Ok(SnapshotCreationJob {
                            snapshot_id: claim.job.snapshot_id,
                            state: SnapshotCreationState::RetryWait,
                            retry_count: u32::try_from(retry_count).unwrap_or(u32::MAX),
                            owner: None,
                            lease_expires_at: None,
                            fencing_token: claim.lease.fencing_token,
                            next_retry_at: Some(next_retry_at),
                            failure: Some(message),
                        })
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("mark snapshot retry", error))
        })
    }

    fn mark_failed<'a>(
        &'a self,
        claim: &'a SnapshotJobClaim,
        failure: SnapshotFailure,
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
        let claim = claim.clone();
        let code = crate::snapshot_ports::RedactedSnapshotMessage::new(failure.code)
            .as_str()
            .to_string();
        let message = failure.message.as_str().to_string();
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "mark snapshot failed",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot failure"))?;
                        let updated = tx
                            .execute(
                                "UPDATE snapshot_creation_jobs AS j SET state = 'failed', next_retry_at = NULL, failure_message = $3, updated_at = clock_timestamp() WHERE j.vol = $1 AND j.snapshot_id = $2 AND j.state = 'running' AND EXISTS (SELECT 1 FROM snapshot_creation_leases AS l WHERE l.vol = j.vol AND l.snapshot_id = j.snapshot_id AND l.owner = $4 AND l.fencing_token = $5 AND l.lease_expires_at > clock_timestamp())",
                                &[
                                    &volume_id,
                                    &claim.job.snapshot_id.to_string(),
                                    &message,
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("mark snapshot job failed"))?;
                        if updated != 1 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        tx.execute(
                            "UPDATE snapshot_catalog AS c SET lifecycle = 'failed', failure_code = $3, failure_message = $4, updated_at = clock_timestamp() WHERE c.vol = $1 AND c.id = $2 AND c.lifecycle = 'creating'",
                            &[&volume_id, &claim.job.snapshot_id.to_string(), &code, &message],
                        )
                        .await
                        .map_err(|_| pg_failure("mark snapshot catalog failed"))?;
                        tx.execute(
                            "UPDATE snapshot_creation_leases SET owner = '', lease_expires_at = clock_timestamp(), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4",
                            &[
                                &volume_id,
                                &claim.job.snapshot_id.to_string(),
                                &claim.lease.owner,
                                &claim.lease.fencing_token,
                            ],
                        )
                        .await
                        .map_err(|_| pg_failure("release failed snapshot lease"))?;
                        let entry = load_catalog(&tx, volume_id, claim.job.snapshot_id.as_str()).await?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot failure"))?;
                        Ok(entry)
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("mark snapshot failed", error))
        })
    }

    fn publish_ready<'a>(
        &'a self,
        claim: &'a SnapshotJobClaim,
        descriptor: &'a SnapshotArtifactDescriptor,
        payload_proofs: &'a [SnapshotPayloadProof],
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
        let claim = claim.clone();
        let descriptor = descriptor.clone();
        let payload_proofs = payload_proofs.to_vec();
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "publish snapshot ready",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot publication"))?;
                        let current = load_catalog(&tx, volume_id, claim.job.snapshot_id.as_str()).await?;
                        if current.lifecycle != SnapshotLifecycle::Creating
                            || current.creation_state != SnapshotCreationState::Running
                            || current.cut != descriptor.cut
                        {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        SnapshotArtifactDescriptor::new(
                            descriptor.object_key.clone(),
                            descriptor.format_version,
                            descriptor.schema_version,
                            descriptor.sha256,
                            descriptor.byte_length,
                            descriptor.cut,
                            descriptor.verified_at,
                        )
                        .map_err(|_| MetaError::Storage("invalid snapshot artifact".to_string()))?;
                        let format_version = i16::try_from(descriptor.format_version)
                            .map_err(|_| MetaError::Storage("invalid artifact format version".to_string()))?;
                        let schema_version = i16::try_from(descriptor.schema_version)
                            .map_err(|_| MetaError::Storage("invalid artifact schema version".to_string()))?;
                        let byte_length = i64::try_from(descriptor.byte_length)
                            .map_err(|_| MetaError::Storage("invalid artifact length".to_string()))?;
                        let mut object_ids = Vec::with_capacity(payload_proofs.len());
                        let mut offsets = Vec::with_capacity(payload_proofs.len());
                        let mut lengths = Vec::with_capacity(payload_proofs.len());
                        let mut hashes = Vec::with_capacity(payload_proofs.len());
                        for proof in &payload_proofs {
                            object_ids.push(
                                i64::try_from(proof.object_id).map_err(|_| {
                                    MetaError::Storage("invalid payload object ID".to_string())
                                })?,
                            );
                            offsets.push(i64::try_from(proof.offset).map_err(|_| {
                                MetaError::Storage("invalid payload offset".to_string())
                            })?);
                            lengths.push(i64::from(proof.length));
                            hashes.push(proof.sha256.as_bytes().to_vec());
                        }
                        tx.execute(
                            "INSERT INTO snapshot_artifacts (vol, snapshot_id, object_key, format_version, schema_version, sha256, byte_length, verified_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                            &[
                                &volume_id,
                                &claim.job.snapshot_id.to_string(),
                                &descriptor.object_key,
                                &format_version,
                                &schema_version,
                                &descriptor.sha256.as_bytes().as_slice(),
                                &byte_length,
                                &descriptor.verified_at,
                            ],
                        )
                        .await
                        .map_err(|_| pg_failure("store snapshot artifact"))?;
                        if !payload_proofs.is_empty() {
                            tx.execute(
                                "INSERT INTO snapshot_artifact_payload_proofs (vol, snapshot_id, object_id, byte_offset, byte_length, sha256, verified_at) SELECT $1, $2, proof.object_id, proof.byte_offset, proof.byte_length, proof.sha256, clock_timestamp() FROM UNNEST($3::bigint[], $4::bigint[], $5::bigint[], $6::bytea[]) AS proof(object_id, byte_offset, byte_length, sha256)",
                                &[
                                    &volume_id,
                                    &claim.job.snapshot_id.to_string(),
                                    &object_ids,
                                    &offsets,
                                    &lengths,
                                    &hashes,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("store snapshot payload proof"))?;
                        }
                        let updated = tx
                            .query_opt(
                                "WITH now AS (SELECT clock_timestamp() AS ts) UPDATE snapshot_catalog AS c SET lifecycle = 'ready', ready_at = now.ts, expires_at = CASE WHEN c.retention_seconds IS NULL THEN NULL ELSE now.ts + (c.retention_seconds::double precision * interval '1 second') END, updated_at = now.ts FROM snapshot_creation_jobs AS j JOIN snapshot_creation_leases AS l ON l.vol = j.vol AND l.snapshot_id = j.snapshot_id CROSS JOIN now WHERE c.vol = $1 AND c.id = $2 AND c.lifecycle = 'creating' AND j.state = 'running' AND l.owner = $3 AND l.fencing_token = $4 AND l.lease_expires_at > now.ts RETURNING c.ready_at",
                                &[
                                    &volume_id,
                                    &claim.job.snapshot_id.to_string(),
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("publish snapshot ready"))?;
                        if updated.is_none() {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        let completed = tx
                            .execute(
                                "UPDATE snapshot_creation_jobs AS j SET state = 'completed', next_retry_at = NULL, updated_at = clock_timestamp() WHERE j.vol = $1 AND j.snapshot_id = $2 AND j.state = 'running' AND EXISTS (SELECT 1 FROM snapshot_creation_leases AS l WHERE l.vol = j.vol AND l.snapshot_id = j.snapshot_id AND l.owner = $3 AND l.fencing_token = $4)",
                                &[
                                    &volume_id,
                                    &claim.job.snapshot_id.to_string(),
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("complete snapshot job"))?;
                        if completed != 1 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        tx.execute(
                            "UPDATE snapshot_policies AS p SET latest_completed_slot = GREATEST(COALESCE(p.latest_completed_slot, s.due_at), s.due_at), updated_at = clock_timestamp() FROM snapshot_policy_slots AS s WHERE p.vol = s.vol AND p.generation = s.generation AND s.vol = $1 AND s.snapshot_id = $2 AND s.slot_kind = 'due' AND s.due_at IS NOT NULL",
                            &[&volume_id, &claim.job.snapshot_id.to_string()],
                        )
                        .await
                        .map_err(|_| pg_failure("record completed snapshot policy slot"))?;
                        tx.execute(
                            "UPDATE snapshot_creation_leases SET owner = '', lease_expires_at = clock_timestamp(), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4",
                            &[
                                &volume_id,
                                &claim.job.snapshot_id.to_string(),
                                &claim.lease.owner,
                                &claim.lease.fencing_token,
                            ],
                        )
                        .await
                        .map_err(|_| pg_failure("release published snapshot lease"))?;
                        let entry = load_catalog(&tx, volume_id, claim.job.snapshot_id.as_str()).await?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot publication"))?;
                        Ok(entry)
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("publish snapshot ready", error))
        })
    }

    fn load_catalog<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
        let snapshot_id = snapshot_id.clone();
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "load snapshot catalog",
                move |client| {
                    let snapshot_id = snapshot_id.clone();
                    Box::pin(
                        async move { load_catalog(client, volume_id, snapshot_id.as_str()).await },
                    )
                },
            )
            .await;
            result.map_err(|error| map_error("load snapshot catalog", error))
        })
    }

    fn list_catalog<'a>(&'a self) -> SnapshotPortFuture<'a, Vec<SnapshotCatalogEntry>> {
        let pool = Arc::clone(&self.snapshot_command_pool());
        let timeout = self.snapshot_command_timeout();
        let volume_id = self.snapshot_volume_id();
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "list snapshot catalog",
                move |client| {
                    Box::pin(async move {
                        let query = catalog_query("WHERE c.vol = $1 ORDER BY c.captured_at, c.id");
                        let rows = client
                            .query(&query, &[&volume_id])
                            .await
                            .map_err(|_| pg_failure("list snapshot catalog"))?;
                        rows.iter()
                            .map(|row| parse_catalog_row(row, volume_id))
                            .collect()
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("list snapshot catalog", error))
        })
    }
}

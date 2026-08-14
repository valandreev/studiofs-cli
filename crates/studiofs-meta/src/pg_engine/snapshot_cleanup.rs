use super::{PgMetaEngine, snapshot_control, snapshot_jobs};
use crate::engine::MetaError;
use crate::snapshot::{
    SnapshotCatalogEntry, SnapshotCleanupClaim, SnapshotCleanupLease, SnapshotId,
};
use crate::snapshot_ports::{
    SnapshotCleanupStore, SnapshotPortError, SnapshotPortFuture, SnapshotWorkerId,
};
use std::sync::Arc;
use std::time::Duration;

const STALE_LEASE: &str = "snapshot cleanup lease fenced";
const SNAPSHOT_NOT_FOUND: &str = "snapshot job not found";
const SNAPSHOT_NOT_DELETING: &str = "snapshot is not deleting";
const ACTIVE_MOUNT_LEASE: &str = "snapshot cleanup blocked by active mount lease";
const ACTIVE_CREATION_LEASE: &str = "snapshot cleanup blocked by active creation lease";

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

fn map_error(operation: &'static str, error: MetaError) -> SnapshotPortError {
    match &error {
        MetaError::Storage(message) if message == STALE_LEASE => {
            SnapshotPortError::permanent("stale_lease", "snapshot cleanup lease is no longer owned")
        }
        MetaError::Storage(message) if message == SNAPSHOT_NOT_FOUND => {
            SnapshotPortError::permanent("snapshot_not_found", "snapshot does not exist")
        }
        MetaError::Storage(message) if message == SNAPSHOT_NOT_DELETING => {
            SnapshotPortError::permanent("snapshot_not_deleting", "snapshot is not deleting")
        }
        MetaError::Storage(message)
            if message == ACTIVE_MOUNT_LEASE || message == ACTIVE_CREATION_LEASE =>
        {
            SnapshotPortError::retryable(
                "snapshot cleanup",
                "snapshot cleanup is waiting for an active lease",
            )
        }
        _ => SnapshotPortError::from_backend(operation, &error.to_string()),
    }
}

fn pg_failure(operation: &'static str) -> MetaError {
    MetaError::RemoteUnavailable(format!("snapshot PostgreSQL operation failed: {operation}"))
}

impl SnapshotCleanupStore for PgMetaEngine {
    fn request_delete<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
        let snapshot_id = snapshot_id.clone();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "request snapshot deletion",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot deletion"))?;
                        let entry = snapshot_control::request_snapshot_delete_in_transaction(
                            &tx,
                            volume_id,
                            &snapshot_id,
                        )
                        .await?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot deletion"))?;
                        Ok(entry)
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("request snapshot deletion", error))
        })
    }

    fn claim_next<'a>(
        &'a self,
        worker: &'a SnapshotWorkerId,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, Option<SnapshotCleanupClaim>> {
        let worker = worker.clone();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let lease_ms = lease_millis(lease_for)?;
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "claim snapshot cleanup",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot cleanup claim"))?;
                        let expired_row = tx
                            .query_opt(
                                "SELECT c.id
                                 FROM snapshot_catalog AS c
                                 WHERE c.vol = $1
                                   AND c.lifecycle = 'ready'
                                   AND c.ready_at IS NOT NULL
                                   AND c.expires_at IS NOT NULL
                                   AND c.expires_at <= clock_timestamp()
                                 ORDER BY c.updated_at, c.id
                                 LIMIT 1
                                 FOR UPDATE OF c SKIP LOCKED",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|_| pg_failure("find expired snapshot cleanup"))?;
                        if let Some(row) = expired_row {
                            let snapshot_id_text: String = row.get(0);
                            let snapshot_id = SnapshotId::parse(&snapshot_id_text).map_err(|_| {
                                MetaError::Storage("invalid snapshot ID in PostgreSQL".to_string())
                            })?;
                            snapshot_control::request_snapshot_delete_in_transaction(
                                &tx,
                                volume_id,
                                &snapshot_id,
                            )
                            .await?;
                        }
                        let row = tx
                            .query_opt(
                                "SELECT c.id
                                 FROM snapshot_catalog AS c
                                 LEFT JOIN snapshot_cleanup_leases AS l
                                   ON l.vol = c.vol
                                  AND l.snapshot_id = c.id
                                 WHERE c.vol = $1
                                   AND c.lifecycle = 'deleting'
                                   AND NOT EXISTS (
                                       SELECT 1
                                       FROM snapshot_mount_leases AS mount_lease
                                       WHERE mount_lease.vol = c.vol
                                         AND mount_lease.snapshot_id = c.id
                                         AND mount_lease.released_at IS NULL
                                         AND mount_lease.lease_expires_at > clock_timestamp()
                                   )
                                   AND NOT EXISTS (
                                       SELECT 1
                                       FROM snapshot_creation_leases AS creation_lease
                                       WHERE creation_lease.vol = c.vol
                                         AND creation_lease.snapshot_id = c.id
                                         AND creation_lease.owner <> ''
                                         AND creation_lease.lease_expires_at > clock_timestamp()
                                   )
                                   AND (l.snapshot_id IS NULL
                                        OR l.lease_expires_at <= clock_timestamp())
                                 ORDER BY c.updated_at, c.id
                                 LIMIT 1
                                 FOR UPDATE OF c SKIP LOCKED",
                                &[&volume_id],
                            )
                            .await
                            .map_err(|_| pg_failure("find snapshot cleanup"))?;
                        let Some(row) = row else {
                            tx.commit()
                                .await
                                .map_err(|_| pg_failure("commit empty snapshot cleanup claim"))?;
                            return Ok(None);
                        };
                        let snapshot_id: String = row.get(0);
                        let worker_text = worker.as_str();
                        let lease = tx
                            .query_one(
                                "INSERT INTO snapshot_cleanup_leases (vol, snapshot_id, owner, lease_expires_at, fencing_token, acquired_at, renewed_at) VALUES ($1, $2, $3, clock_timestamp() + ($4::bigint * interval '1 millisecond'), 1, clock_timestamp(), clock_timestamp()) ON CONFLICT (vol, snapshot_id) DO UPDATE SET owner = EXCLUDED.owner, lease_expires_at = EXCLUDED.lease_expires_at, fencing_token = snapshot_cleanup_leases.fencing_token + 1, acquired_at = clock_timestamp(), renewed_at = clock_timestamp() RETURNING lease_expires_at, fencing_token",
                                &[&volume_id, &snapshot_id, &worker_text, &lease_ms],
                            )
                            .await
                            .map_err(|_| pg_failure("claim snapshot cleanup lease"))?;
                        let catalog = snapshot_jobs::load_catalog(&tx, volume_id, &snapshot_id).await?;
                        let claim = SnapshotCleanupClaim {
                            catalog,
                            lease: SnapshotCleanupLease {
                                owner: worker.as_str().to_string(),
                                expires_at: lease.get(0),
                                fencing_token: lease.get(1),
                            },
                        };
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot cleanup claim"))?;
                        Ok(Some(claim))
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("claim snapshot cleanup", error))
        })
    }

    fn renew_lease<'a>(
        &'a self,
        claim: &'a SnapshotCleanupClaim,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotCleanupClaim> {
        let claim = claim.clone();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let lease_ms = lease_millis(lease_for)?;
            let snapshot_id = claim.catalog.snapshot_id.to_string();
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "renew snapshot cleanup lease",
                |client| {
                    Box::pin(async move {
                        let row = client
                            .query_opt(
                                "UPDATE snapshot_cleanup_leases SET lease_expires_at = clock_timestamp() + ($5::bigint * interval '1 millisecond'), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4 AND lease_expires_at > clock_timestamp() RETURNING lease_expires_at, fencing_token",
                                &[
                                    &volume_id,
                                    &snapshot_id,
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                    &lease_ms,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("renew snapshot cleanup lease"))?
                            .ok_or_else(|| MetaError::Storage(STALE_LEASE.to_string()))?;
                        let mut renewed = claim;
                        renewed.lease.expires_at = row.get(0);
                        renewed.lease.fencing_token = row.get(1);
                        Ok(renewed)
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("renew snapshot cleanup lease", error))
        })
    }

    fn release_lease<'a>(&'a self, claim: &'a SnapshotCleanupClaim) -> SnapshotPortFuture<'a, ()> {
        let claim = claim.clone();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let snapshot_id = claim.catalog.snapshot_id.to_string();
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "release snapshot cleanup lease",
                |client| {
                    Box::pin(async move {
                        let released = client
                            .execute(
                                "UPDATE snapshot_cleanup_leases SET owner = '', lease_expires_at = clock_timestamp(), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4",
                                &[
                                    &volume_id,
                                    &snapshot_id,
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("release snapshot cleanup lease"))?;
                        if released != 1 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        Ok(())
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("release snapshot cleanup lease", error))
        })
    }

    fn finalize_deleted<'a>(
        &'a self,
        claim: &'a SnapshotCleanupClaim,
    ) -> SnapshotPortFuture<'a, SnapshotCatalogEntry> {
        let claim = claim.clone();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let snapshot_id = claim.catalog.snapshot_id.to_string();
            let result = PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "finalize snapshot deletion",
                |client| {
                    Box::pin(async move {
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin snapshot finalization"))?;
                        let lifecycle = tx
                            .query_opt(
                                "SELECT lifecycle FROM snapshot_catalog WHERE vol = $1 AND id = $2 FOR UPDATE",
                                &[&volume_id, &snapshot_id],
                            )
                            .await
                            .map_err(|_| pg_failure("load snapshot for finalization"))?
                            .ok_or_else(|| MetaError::Storage(SNAPSHOT_NOT_FOUND.to_string()))?;
                        let lifecycle: String = lifecycle.get(0);
                        if lifecycle == "deleted" {
                            tx.execute(
                                "UPDATE snapshot_cleanup_leases SET owner = '', lease_expires_at = clock_timestamp(), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4",
                                &[
                                    &volume_id,
                                    &snapshot_id,
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("release completed cleanup lease"))?;
                            let catalog = snapshot_jobs::load_catalog(&tx, volume_id, &snapshot_id).await?;
                            tx.commit()
                                .await
                                .map_err(|_| pg_failure("commit completed snapshot cleanup"))?;
                            return Ok(catalog);
                        }
                        if lifecycle != "deleting" {
                            return Err(MetaError::Storage(SNAPSHOT_NOT_DELETING.to_string()));
                        }
                        let active_mount: bool = tx
                            .query_one(
                                "SELECT EXISTS (SELECT 1 FROM snapshot_mount_leases WHERE vol = $1 AND snapshot_id = $2 AND released_at IS NULL AND lease_expires_at > clock_timestamp())",
                                &[&volume_id, &snapshot_id],
                            )
                            .await
                            .map_err(|_| pg_failure("check snapshot mount leases"))?
                            .get(0);
                        if active_mount {
                            return Err(MetaError::Storage(ACTIVE_MOUNT_LEASE.to_string()));
                        }
                        let active_creation: bool = tx
                            .query_one(
                                "SELECT EXISTS (SELECT 1 FROM snapshot_creation_leases WHERE vol = $1 AND snapshot_id = $2 AND owner <> '' AND lease_expires_at > clock_timestamp())",
                                &[&volume_id, &snapshot_id],
                            )
                            .await
                            .map_err(|_| pg_failure("check snapshot creation leases"))?
                            .get(0);
                        if active_creation {
                            return Err(MetaError::Storage(ACTIVE_CREATION_LEASE.to_string()));
                        }

                        for statement in [
                            "DELETE FROM snapshot_artifact_payload_proofs WHERE vol = $1 AND snapshot_id = $2",
                            "DELETE FROM snapshot_artifacts WHERE vol = $1 AND snapshot_id = $2",
                            "DELETE FROM snapshot_artifact_attempts WHERE vol = $1 AND snapshot_id = $2",
                            "DELETE FROM snapshot_source_checkpoints WHERE vol = $1 AND snapshot_id = $2",
                        ] {
                            tx.execute(statement, &[&volume_id, &snapshot_id])
                                .await
                                .map_err(|_| pg_failure("delete snapshot cleanup metadata"))?;
                        }
                        let updated = tx
                            .execute(
                                "UPDATE snapshot_catalog SET lifecycle = 'deleted', updated_at = clock_timestamp() WHERE vol = $1 AND id = $2 AND lifecycle = 'deleting'",
                                &[&volume_id, &snapshot_id],
                            )
                            .await
                            .map_err(|_| pg_failure("finalize deleted snapshot"))?;
                        if updated != 1 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        let released = tx
                            .execute(
                                "UPDATE snapshot_cleanup_leases SET owner = '', lease_expires_at = clock_timestamp(), renewed_at = clock_timestamp() WHERE vol = $1 AND snapshot_id = $2 AND owner = $3 AND fencing_token = $4",
                                &[
                                    &volume_id,
                                    &snapshot_id,
                                    &claim.lease.owner,
                                    &claim.lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("release finalized cleanup lease"))?;
                        if released != 1 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        let catalog = snapshot_jobs::load_catalog(&tx, volume_id, &snapshot_id).await?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit snapshot finalization"))?;
                        Ok(catalog)
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("finalize snapshot deletion", error))
        })
    }
}

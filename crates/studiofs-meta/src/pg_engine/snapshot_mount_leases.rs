use crate::engine::MetaError;
use crate::snapshot::{SnapshotId, SnapshotMountLease, SnapshotSha256};
use crate::snapshot_ports::{SnapshotMountStore, SnapshotPortError, SnapshotPortFuture};
use std::sync::Arc;
use std::time::Duration;

const STALE_LEASE: &str = "snapshot mount lease fenced";
const SNAPSHOT_NOT_FOUND: &str = "snapshot mount snapshot not found";
const SNAPSHOT_NOT_READY: &str = "snapshot mount snapshot not ready";

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
            SnapshotPortError::permanent("stale_lease", "snapshot mount lease is no longer owned")
        }
        MetaError::Storage(message) if message == SNAPSHOT_NOT_FOUND => {
            SnapshotPortError::permanent("snapshot_not_found", "snapshot does not exist")
        }
        MetaError::Storage(message) if message == SNAPSHOT_NOT_READY => {
            SnapshotPortError::permanent("snapshot_not_ready", "snapshot is not ready")
        }
        _ => SnapshotPortError::from_backend(operation, &error.to_string()),
    }
}

fn pg_failure(operation: &'static str) -> MetaError {
    MetaError::RemoteUnavailable(format!("snapshot PostgreSQL operation failed: {operation}"))
}

fn lease_from_row(
    row: &tokio_postgres::Row,
    snapshot_id: SnapshotId,
    owner: String,
    cache_volume_uuid: String,
    cache_sha256: SnapshotSha256,
) -> SnapshotMountLease {
    SnapshotMountLease {
        lease_id: row.get(0),
        snapshot_id,
        owner,
        cache_volume_uuid,
        cache_sha256,
        expires_at: row.get(1),
        heartbeat_at: row.get(2),
        fencing_token: row.get(3),
        released: false,
    }
}

impl SnapshotMountStore for super::PgMetaEngine {
    fn acquire<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
        owner: &'a str,
        cache_volume_uuid: &'a str,
        cache_sha256: &'a SnapshotSha256,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotMountLease> {
        let snapshot_id = snapshot_id.clone();
        let owner = owner.to_string();
        let cache_volume_uuid = cache_volume_uuid.to_string();
        let cache_sha256 = *cache_sha256;
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let lease_ms = lease_millis(lease_for)?;
            if owner.trim().is_empty() || cache_volume_uuid.trim().is_empty() {
                return Err(SnapshotPortError::permanent(
                    "invalid_mount_lease",
                    "mount lease identity is empty",
                ));
            }
            let result = super::PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "acquire snapshot mount lease",
                |client| {
                    Box::pin(async move {
                        let snapshot_id_text = snapshot_id.to_string();
                        let tx = client
                            .transaction()
                            .await
                            .map_err(|_| pg_failure("begin mount lease acquisition"))?;
                        let catalog = tx
                            .query_opt(
                                "SELECT lifecycle, (expires_at IS NULL OR expires_at > clock_timestamp())
                                 FROM snapshot_catalog
                                 WHERE vol = $1 AND id = $2
                                 FOR UPDATE",
                                &[&volume_id, &snapshot_id_text],
                            )
                            .await
                            .map_err(|_| pg_failure("load mount snapshot catalog"))?;
                        let Some(catalog) = catalog else {
                            return Err(MetaError::Storage(SNAPSHOT_NOT_FOUND.to_string()));
                        };
                        let lifecycle: String = catalog.get(0);
                        let unexpired: bool = catalog.get(1);
                        if lifecycle != "ready" || !unexpired {
                            return Err(MetaError::Storage(SNAPSHOT_NOT_READY.to_string()));
                        }

                        let inserted = tx
                            .query_opt(
                                "INSERT INTO snapshot_mount_leases
                                    (vol, snapshot_id, owner, cache_volume_uuid, cache_sha256,
                                     lease_expires_at, heartbeat_at)
                                 SELECT c.vol, c.id, $3, $4, $5,
                                        clock_timestamp() + ($6::double precision * interval '1 millisecond'),
                                        clock_timestamp()
                                 FROM snapshot_catalog AS c
                                 WHERE c.vol = $1
                                   AND c.id = $2
                                   AND c.lifecycle = 'ready'
                                   AND (c.expires_at IS NULL OR c.expires_at > clock_timestamp())
                                 RETURNING lease_id",
                                &[
                                    &volume_id,
                                    &snapshot_id_text,
                                    &owner,
                                    &cache_volume_uuid,
                                    &cache_sha256.as_bytes().as_slice(),
                                    &lease_ms,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("insert mount lease"))?
                            .ok_or_else(|| MetaError::Storage(SNAPSHOT_NOT_READY.to_string()))?;
                        let lease_id: i64 = inserted.get(0);
                        let row = tx
                            .query_one(
                                "UPDATE snapshot_mount_leases
                                 SET fencing_token = lease_id
                                 WHERE lease_id = $1
                                 RETURNING lease_id, lease_expires_at, heartbeat_at, fencing_token",
                                &[&lease_id],
                            )
                            .await
                            .map_err(|_| pg_failure("fence mount lease"))?;
                        tx.commit()
                            .await
                            .map_err(|_| pg_failure("commit mount lease acquisition"))?;
                        Ok(lease_from_row(
                            &row,
                            snapshot_id,
                            owner,
                            cache_volume_uuid,
                            cache_sha256,
                        ))
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("acquire snapshot mount lease", error))
        })
    }

    fn renew<'a>(
        &'a self,
        lease: &'a SnapshotMountLease,
        lease_for: Duration,
    ) -> SnapshotPortFuture<'a, SnapshotMountLease> {
        let lease = lease.clone();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let lease_ms = lease_millis(lease_for)?;
            let cache_sha256 = *lease.cache_sha256.as_bytes();
            let result = super::PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "renew snapshot mount lease",
                |client| {
                    Box::pin(async move {
                        let snapshot_id = lease.snapshot_id.as_str();
                        let updated = client
                            .query_opt(
                                "UPDATE snapshot_mount_leases
                                 SET lease_expires_at = clock_timestamp() + ($8::double precision * interval '1 millisecond'),
                                     heartbeat_at = clock_timestamp()
                                 WHERE lease_id = $1
                                   AND vol = $2
                                   AND snapshot_id = $3
                                   AND owner = $4
                                   AND cache_volume_uuid = $5
                                   AND cache_sha256 = $6
                                   AND fencing_token = $7
                                   AND released_at IS NULL
                                   AND lease_expires_at > clock_timestamp()
                                 RETURNING lease_id, lease_expires_at, heartbeat_at, fencing_token",
                                &[
                                    &lease.lease_id,
                                    &volume_id,
                                    &snapshot_id,
                                    &lease.owner,
                                    &lease.cache_volume_uuid,
                                    &&cache_sha256[..],
                                    &lease.fencing_token,
                                    &lease_ms,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("renew mount lease"))?
                            .ok_or_else(|| MetaError::Storage(STALE_LEASE.to_string()))?;
                        Ok(lease_from_row(
                            &updated,
                            lease.snapshot_id,
                            lease.owner,
                            lease.cache_volume_uuid,
                            lease.cache_sha256,
                        ))
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("renew snapshot mount lease", error))
        })
    }

    fn release<'a>(&'a self, lease: &'a SnapshotMountLease) -> SnapshotPortFuture<'a, ()> {
        let lease = lease.clone();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let cache_sha256 = *lease.cache_sha256.as_bytes();
            let result = super::PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "release snapshot mount lease",
                |client| {
                    Box::pin(async move {
                        let snapshot_id = lease.snapshot_id.as_str();
                        let updated = client
                            .execute(
                                "UPDATE snapshot_mount_leases
                                 SET released_at = clock_timestamp(),
                                     lease_expires_at = clock_timestamp(),
                                     heartbeat_at = clock_timestamp()
                                 WHERE lease_id = $1
                                   AND vol = $2
                                   AND snapshot_id = $3
                                   AND owner = $4
                                   AND cache_volume_uuid = $5
                                   AND cache_sha256 = $6
                                   AND fencing_token = $7",
                                &[
                                    &lease.lease_id,
                                    &volume_id,
                                    &snapshot_id,
                                    &lease.owner,
                                    &lease.cache_volume_uuid,
                                    &&cache_sha256[..],
                                    &lease.fencing_token,
                                ],
                            )
                            .await
                            .map_err(|_| pg_failure("release mount lease"))?;
                        if updated == 0 {
                            return Err(MetaError::Storage(STALE_LEASE.to_string()));
                        }
                        Ok(())
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("release snapshot mount lease", error))
        })
    }

    fn has_active_lease<'a>(
        &'a self,
        snapshot_id: &'a SnapshotId,
        cache_volume_uuid: &'a str,
        cache_sha256: &'a SnapshotSha256,
    ) -> SnapshotPortFuture<'a, bool> {
        let snapshot_id = snapshot_id.clone();
        let cache_volume_uuid = cache_volume_uuid.to_string();
        let cache_sha256 = *cache_sha256.as_bytes();
        let pool = Arc::clone(&self.command_pool);
        let timeout = self.command_timeout;
        let volume_id = self.volume_id;
        Box::pin(async move {
            let result = super::PgMetaEngine::with_postgres_client_timeout(
                pool,
                timeout,
                "check snapshot mount lease",
                |client| {
                    Box::pin(async move {
                        let snapshot_id = snapshot_id.as_str();
                        client
                            .query_one(
                                "SELECT EXISTS (
                                    SELECT 1
                                    FROM snapshot_mount_leases
                                    WHERE vol = $1
                                      AND snapshot_id = $2
                                      AND cache_volume_uuid = $3
                                      AND cache_sha256 = $4
                                      AND released_at IS NULL
                                      AND lease_expires_at > clock_timestamp()
                                )",
                                &[
                                    &volume_id,
                                    &snapshot_id,
                                    &cache_volume_uuid,
                                    &&cache_sha256[..],
                                ],
                            )
                            .await
                            .map(|row| row.get(0))
                            .map_err(|_| pg_failure("check mount lease"))
                    })
                },
            )
            .await;
            result.map_err(|error| map_error("check snapshot mount lease", error))
        })
    }
}

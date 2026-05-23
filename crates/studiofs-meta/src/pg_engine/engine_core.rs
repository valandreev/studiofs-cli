use super::PgMetaEngine;
use crate::engine::{MetaEngineCore, MetaError, MetaResult};
use crate::types::*;
use std::collections::HashSet;
use std::time::Duration;

impl MetaEngineCore for PgMetaEngine {
    fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool> {
        self.try_acquire_pg_janitor_lock()
    }

    fn release_global_janitor_lock(&self) -> MetaResult<()> {
        self.release_pg_janitor_lock()
    }

    fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        self.load_pg_global_snapshot_pins()
    }

    fn acquire_file_lock_lease(
        &self,
        ino: Ino,
        kind: HardLockKind,
        lease_ttl: Duration,
    ) -> MetaResult<FileLockLeaseAcquireOutcome> {
        self.ensure_lock_workers_started()?;
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let command_timeout = self.command_timeout;
        let outcome = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "acquire file lock lease",
                move |client| Box::pin(async move {
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock acquire begin tx failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let now_ms = Self::now_millis();
                    let lease_expires_at_unix_ms = now_ms
                        .checked_add(i64::try_from(lease_ttl.as_millis()).unwrap_or(i64::MAX))
                        .unwrap_or(i64::MAX);
                    tx.execute(
                        r#"
                        INSERT INTO file_lock_leases (
                            vol,
                            ino,
                            kind,
                            exclusive_owner,
                            shared_owners,
                            lease_expires_at_unix_ms,
                            generation,
                            updated_at
                        )
                        VALUES ($1, $2, $3, NULL, '{}', 0, 0, now())
                        ON CONFLICT (vol, ino) DO NOTHING
                        "#,
                        &[&volume_id, &(ino as i64), &(HardLockKind::Shared as i16)],
                    )
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock acquire ensure row failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let row = tx
                        .query_one(
                            r#"
                            SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
                            FROM file_lock_leases
                            WHERE vol = $1 AND ino = $2
                            FOR UPDATE
                            "#,
                            &[&volume_id, &(ino as i64)],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock acquire select failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    let current = Self::current_file_lock_lease_from_row(row, now_ms)?;

                    let maybe_next = match current.as_ref() {
                        None => Some(FileLockLease {
                            ino,
                            kind,
                            generation: 1,
                            lease_expires_at_unix_ms,
                            exclusive_owner: (kind == HardLockKind::Exclusive)
                                .then(|| client_id.clone()),
                            shared_owners: if kind == HardLockKind::Shared {
                                [client_id.clone()].into_iter().collect()
                            } else {
                                Default::default()
                            },
                        }),
                        Some(current) => {
                            let is_self_exclusive =
                                current.exclusive_owner.as_deref() == Some(client_id.as_str());
                            let is_self_shared = current.shared_owners.contains(client_id.as_str());
                            match kind {
                                HardLockKind::Shared => {
                                    if current.exclusive_owner.is_none() || is_self_exclusive {
                                        let mut next = current.clone();
                                        next.kind = HardLockKind::Shared;
                                        next.generation = next.generation.saturating_add(1);
                                        next.lease_expires_at_unix_ms = lease_expires_at_unix_ms;
                                        next.exclusive_owner = None;
                                        next.shared_owners.insert(client_id.clone());
                                        Some(next)
                                    } else {
                                        None
                                    }
                                }
                                HardLockKind::Exclusive => {
                                    let shared_conflict = !(current.shared_owners.is_empty()
                                        || (current.shared_owners.len() == 1 && is_self_shared));
                                    if (current.exclusive_owner.is_none() || is_self_exclusive)
                                        && !shared_conflict
                                    {
                                        Some(FileLockLease {
                                            ino,
                                            kind: HardLockKind::Exclusive,
                                            generation: current.generation.saturating_add(1),
                                            lease_expires_at_unix_ms,
                                            exclusive_owner: Some(client_id.clone()),
                                            shared_owners: Default::default(),
                                        })
                                    } else {
                                        None
                                    }
                                }
                            }
                        }
                    };

                    let outcome = if let Some(next) = maybe_next {
                        let shared_owners: Vec<String> =
                            next.shared_owners.iter().cloned().collect();
                        tx.execute(
                            r#"
                            UPDATE file_lock_leases
                            SET kind = $3,
                                exclusive_owner = $4,
                                shared_owners = $5,
                                lease_expires_at_unix_ms = $6,
                                generation = $7,
                                updated_at = now()
                            WHERE vol = $1 AND ino = $2
                            "#,
                            &[
                                &volume_id,
                                &(ino as i64),
                                &(next.kind as i16),
                                &next.exclusive_owner,
                                &shared_owners,
                                &next.lease_expires_at_unix_ms,
                                &(i64::try_from(next.generation).unwrap_or(i64::MAX)),
                            ],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock acquire update failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        let channel = Self::lock_notify_channel(volume_id);
                        let payload =
                            Self::encode_file_lock_notify_payload(ino, next.generation);
                        let _ = tx
                            .execute("SELECT pg_notify($1, $2)", &[&channel, &payload])
                            .await;
                        tx.commit().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock acquire commit failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        FileLockLeaseAcquireOutcome::Acquired(next)
                    } else {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock acquire rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        FileLockLeaseAcquireOutcome::Blocked(current.ok_or_else(|| {
                            MetaError::Storage(
                                "file lock acquire was blocked without a live conflicting lease"
                                    .to_string(),
                            )
                        })?)
                    };
                    Ok(outcome)
                }),
            )
            .await
        })?;
        match &outcome {
            FileLockLeaseAcquireOutcome::Acquired(snapshot) => {
                self.remember_held_file_lock(snapshot.ino, lease_ttl);
                self.cache_file_lock_lease(Some(snapshot.clone()));
            }
            FileLockLeaseAcquireOutcome::Blocked(snapshot) => {
                self.cache_file_lock_lease(Some(snapshot.clone()));
            }
        }
        Ok(outcome)
    }

    fn release_file_lock_lease(&self, ino: Ino) -> MetaResult<FileLockLeaseReleaseOutcome> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let command_timeout = self.command_timeout;
        let outcome = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "release file lock lease",
                move |client| Box::pin(async move {
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock release begin tx failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let now_ms = Self::now_millis();
                    let row = tx
                        .query_opt(
                            r#"
                            SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
                            FROM file_lock_leases
                            WHERE vol = $1 AND ino = $2
                            FOR UPDATE
                            "#,
                            &[&volume_id, &(ino as i64)],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release select failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    let current = match row {
                        Some(row) => Self::current_file_lock_lease_from_row(row, now_ms)?,
                        None => None,
                    };
                    let Some(current) = current else {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        return Ok(FileLockLeaseReleaseOutcome::NotHeld(None));
                    };

                    let is_self_exclusive =
                        current.exclusive_owner.as_deref() == Some(client_id.as_str());
                    let is_self_shared = current.shared_owners.contains(client_id.as_str());
                    if !is_self_exclusive && !is_self_shared {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        return Ok(FileLockLeaseReleaseOutcome::NotHeld(Some(current)));
                    }

                    let next = if is_self_exclusive {
                        None
                    } else {
                        let mut next = current.clone();
                        next.shared_owners.remove(client_id.as_str());
                        if next.shared_owners.is_empty() {
                            None
                        } else {
                            next.generation = next.generation.saturating_add(1);
                            Some(next)
                        }
                    };

                    if let Some(next) = next.clone() {
                        let shared_owners: Vec<String> =
                            next.shared_owners.iter().cloned().collect();
                        tx.execute(
                            r#"
                            UPDATE file_lock_leases
                            SET kind = $3,
                                exclusive_owner = $4,
                                shared_owners = $5,
                                lease_expires_at_unix_ms = $6,
                                generation = $7,
                                updated_at = now()
                            WHERE vol = $1 AND ino = $2
                            "#,
                            &[
                                &volume_id,
                                &(ino as i64),
                                &(next.kind as i16),
                                &next.exclusive_owner,
                                &shared_owners,
                                &next.lease_expires_at_unix_ms,
                                &(i64::try_from(next.generation).unwrap_or(i64::MAX)),
                            ],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release update failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    } else {
                        tx.execute(
                            "DELETE FROM file_lock_leases WHERE vol = $1 AND ino = $2",
                            &[&volume_id, &(ino as i64)],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock release delete failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    }
                    let generation = next.as_ref().map(|lease| lease.generation).unwrap_or_else(|| {
                        current.generation.saturating_add(1)
                    });
                    let channel = Self::lock_notify_channel(volume_id);
                    let payload = Self::encode_file_lock_notify_payload(ino, generation);
                    let _ = tx
                        .execute("SELECT pg_notify($1, $2)", &[&channel, &payload])
                        .await;
                    tx.commit().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock release commit failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    Ok(FileLockLeaseReleaseOutcome::Released(next))
                }),
            )
            .await
        })?;
        self.forget_held_file_lock(ino);
        match &outcome {
            FileLockLeaseReleaseOutcome::Released(snapshot)
            | FileLockLeaseReleaseOutcome::NotHeld(snapshot) => {
                self.upsert_cached_file_lock_lease(snapshot.clone(), ino);
            }
        }
        Ok(outcome)
    }

    fn renew_file_lock_lease(
        &self,
        ino: Ino,
        lease_ttl: Duration,
    ) -> MetaResult<FileLockLeaseRenewOutcome> {
        self.ensure_lock_workers_started()?;
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let client_id = self.client_id.clone();
        let command_timeout = self.command_timeout;
        let outcome = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "renew file lock lease",
                move |client| Box::pin(async move {
                    let tx = client.transaction().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock renew begin tx failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let now_ms = Self::now_millis();
                    let lease_expires_at_unix_ms = now_ms
                        .checked_add(i64::try_from(lease_ttl.as_millis()).unwrap_or(i64::MAX))
                        .unwrap_or(i64::MAX);
                    let row = tx
                        .query_opt(
                            r#"
                            SELECT ino, kind, exclusive_owner, shared_owners, lease_expires_at_unix_ms, generation
                            FROM file_lock_leases
                            WHERE vol = $1 AND ino = $2
                            FOR UPDATE
                            "#,
                            &[&volume_id, &(ino as i64)],
                        )
                        .await
                        .map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock renew select failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                    let current = match row {
                        Some(row) => Self::current_file_lock_lease_from_row(row, now_ms)?,
                        None => None,
                    };
                    let Some(mut current) = current else {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock renew rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        return Ok(FileLockLeaseRenewOutcome::Lost(None));
                    };

                    let is_self_exclusive =
                        current.exclusive_owner.as_deref() == Some(client_id.as_str());
                    let is_self_shared = current.shared_owners.contains(client_id.as_str());
                    if !is_self_exclusive && !is_self_shared {
                        tx.rollback().await.map_err(|e| {
                            MetaError::Storage(format!(
                                "postgres file lock renew rollback failed: {}",
                                Self::format_pg_error(e)
                            ))
                        })?;
                        return Ok(FileLockLeaseRenewOutcome::Lost(Some(current)));
                    }

                    current.generation = current.generation.saturating_add(1);
                    current.lease_expires_at_unix_ms = lease_expires_at_unix_ms;
                    let shared_owners: Vec<String> =
                        current.shared_owners.iter().cloned().collect();
                    tx.execute(
                        r#"
                        UPDATE file_lock_leases
                        SET kind = $3,
                            exclusive_owner = $4,
                            shared_owners = $5,
                            lease_expires_at_unix_ms = $6,
                            generation = $7,
                            updated_at = now()
                        WHERE vol = $1 AND ino = $2
                        "#,
                        &[
                            &volume_id,
                            &(ino as i64),
                            &(current.kind as i16),
                            &current.exclusive_owner,
                            &shared_owners,
                            &current.lease_expires_at_unix_ms,
                            &(i64::try_from(current.generation).unwrap_or(i64::MAX)),
                        ],
                    )
                    .await
                    .map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock renew update failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    let channel = Self::lock_notify_channel(volume_id);
                    let payload =
                        Self::encode_file_lock_notify_payload(ino, current.generation);
                    let _ = tx
                        .execute("SELECT pg_notify($1, $2)", &[&channel, &payload])
                        .await;
                    tx.commit().await.map_err(|e| {
                        MetaError::Storage(format!(
                            "postgres file lock renew commit failed: {}",
                            Self::format_pg_error(e)
                        ))
                    })?;
                    Ok(FileLockLeaseRenewOutcome::Renewed(current))
                }),
            )
            .await
        })?;
        match &outcome {
            FileLockLeaseRenewOutcome::Renewed(snapshot) => {
                self.remember_held_file_lock(snapshot.ino, lease_ttl);
                self.cache_file_lock_lease(Some(snapshot.clone()));
            }
            FileLockLeaseRenewOutcome::Lost(snapshot) => {
                self.forget_held_file_lock(ino);
                self.upsert_cached_file_lock_lease(snapshot.clone(), ino);
            }
        }
        Ok(outcome)
    }

    fn current_file_lock_lease(&self, ino: Ino) -> MetaResult<Option<FileLockLease>> {
        let command_pool = self.command_pool.clone();
        let volume_id = self.volume_id;
        let command_timeout = self.command_timeout;
        let snapshot = self.run_async(async move {
            Self::with_postgres_client_timeout(
                command_pool,
                command_timeout,
                "read current file lock lease",
                move |client| {
                    Box::pin(async move {
                        let client: &tokio_postgres::Client = client;
                        Self::current_file_lock_lease_with_client(client, volume_id, ino).await
                    })
                },
            )
            .await
        })?;
        self.upsert_cached_file_lock_lease(snapshot.clone(), ino);
        Ok(snapshot)
    }

    fn ensure_known_file_lock_monitoring(&self) -> MetaResult<()> {
        self.ensure_lock_notify_listener_started()
    }

    fn known_file_lock_lease(&self, ino: Ino) -> Option<FileLockLease> {
        let now_ms = Self::now_millis();
        let mut cache = self.file_lock_cache.lock().ok()?;
        let expired = cache
            .get(&ino)
            .map(|lease| lease.lease_expires_at_unix_ms <= now_ms)
            .unwrap_or(false);
        if expired {
            cache.remove(&ino);
            return None;
        }
        cache.get(&ino).cloned()
    }
}

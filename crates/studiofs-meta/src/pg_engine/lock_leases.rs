use super::{HeldFileLockLease, PgMetaEngine, sql};
use crate::engine::{MetaError, MetaResult};
use crate::types::*;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

impl PgMetaEngine {
    pub(super) fn lock_notify_channel(volume_id: i64) -> String {
        format!("studiofs_lock_vol_{volume_id}")
    }

    pub(super) fn metadata_notify_channel(volume_id: i64) -> String {
        format!("studiofs_meta_vol_{volume_id}")
    }

    pub(super) fn encode_file_lock_notify_payload(ino: Ino, generation: u64) -> String {
        format!("{ino}:{generation}")
    }

    pub(super) fn parse_file_lock_notify_payload(payload: &str) -> Option<Ino> {
        let ino = payload.split(':').next()?.trim().parse::<u64>().ok()?;
        Some(ino)
    }

    pub(super) fn decode_hard_lock_kind(raw: i16) -> MetaResult<HardLockKind> {
        match raw {
            1 => Ok(HardLockKind::Shared),
            2 => Ok(HardLockKind::Exclusive),
            other => Err(MetaError::Storage(format!(
                "invalid hard lock kind in postgres row: {other}"
            ))),
        }
    }

    pub(super) fn cache_file_lock_lease(&self, snapshot: Option<FileLockLease>) {
        if let Ok(mut cache) = self.file_lock_cache.lock()
            && let Some(snapshot) = snapshot
        {
            cache.insert(snapshot.ino, snapshot);
        }
    }

    pub(super) fn remove_cached_file_lock_lease(&self, ino: Ino) {
        if let Ok(mut cache) = self.file_lock_cache.lock() {
            cache.remove(&ino);
        }
    }

    pub(super) fn upsert_cached_file_lock_lease(&self, snapshot: Option<FileLockLease>, ino: Ino) {
        if let Some(snapshot) = snapshot {
            self.cache_file_lock_lease(Some(snapshot));
        } else {
            self.remove_cached_file_lock_lease(ino);
        }
    }

    pub(super) fn remember_held_file_lock(&self, ino: Ino, lease_ttl: Duration) {
        if let Ok(mut held) = self.held_file_locks.lock() {
            held.insert(
                ino,
                HeldFileLockLease {
                    lease_ttl,
                    next_renew_at: Instant::now() + Self::half_duration(lease_ttl),
                },
            );
        }
    }

    pub(super) fn forget_held_file_lock(&self, ino: Ino) {
        if let Ok(mut held) = self.held_file_locks.lock() {
            held.remove(&ino);
        }
    }

    pub(super) fn current_file_lock_lease_from_row(
        row: tokio_postgres::Row,
        now_ms: i64,
    ) -> MetaResult<Option<FileLockLease>> {
        let ino_raw: i64 = row.get(0);
        let kind_raw: i16 = row.get(1);
        let exclusive_owner: Option<String> = row.get(2);
        let shared_owners: Vec<String> = row.get(3);
        let lease_expires_at_unix_ms: i64 = row.get(4);
        let generation_raw: i64 = row.get(5);
        if lease_expires_at_unix_ms <= now_ms {
            return Ok(None);
        }
        let ino = u64::try_from(ino_raw).map_err(|_| {
            MetaError::Storage(format!(
                "invalid negative inode in lock lease row: {ino_raw}"
            ))
        })?;
        let generation = u64::try_from(generation_raw).map_err(|_| {
            MetaError::Storage(format!(
                "invalid negative generation in lock lease row: {generation_raw}"
            ))
        })?;
        Ok(Some(FileLockLease {
            ino,
            kind: Self::decode_hard_lock_kind(kind_raw)?,
            generation,
            lease_expires_at_unix_ms,
            exclusive_owner,
            shared_owners: shared_owners.into_iter().collect(),
        }))
    }

    pub(super) fn half_duration(duration: Duration) -> Duration {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Duration::from_millis((millis / 2).max(1))
    }

    pub(super) async fn current_file_lock_lease_with_client<C>(
        client: &C,
        volume_id: i64,
        ino: Ino,
    ) -> MetaResult<Option<FileLockLease>>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        let now_ms = Self::now_millis();
        let row = client
            .query_opt(sql::SELECT_FILE_LOCK_LEASE, &[&volume_id, &(ino as i64)])
            .await
            .map_err(|e| Self::classify_pg_error("postgres file lock read failed", e))?;
        match row {
            Some(row) => Self::current_file_lock_lease_from_row(row, now_ms),
            None => Ok(None),
        }
    }

    pub(super) async fn refresh_known_file_lock_lease_with_client<C>(
        &self,
        client: &C,
        ino: Ino,
    ) -> MetaResult<Option<FileLockLease>>
    where
        C: tokio_postgres::GenericClient + Sync,
    {
        if !self.remote_identity_validated.load(Ordering::Acquire) {
            return Err(MetaError::RemoteUnavailable(
                "remote identity is not validated".to_string(),
            ));
        }
        let snapshot =
            Self::current_file_lock_lease_with_client(client, self.volume_id, ino).await?;
        self.upsert_cached_file_lock_lease(snapshot.clone(), ino);
        Ok(snapshot)
    }
}

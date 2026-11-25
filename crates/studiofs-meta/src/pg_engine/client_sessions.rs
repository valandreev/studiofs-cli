use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

use super::PgMetaEngine;
use super::runtime::PgConnectionPool;
use crate::client_sessions::{ClientSession, SessionCapabilities};
use crate::engine::{MetaError, MetaResult};
use crate::version::{CLIENT_RELEASE, SHARED_VOLUME_FORMAT, SHARED_WRITE_REQUIREMENT};

pub(super) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const LEASE_SECONDS: i32 = 60;

pub(super) const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS client_sessions (
        vol BIGINT NOT NULL REFERENCES volumes(vol) ON DELETE CASCADE,
        session_id TEXT NOT NULL,
        owner TEXT NOT NULL,
        build TEXT NOT NULL,
        capabilities JSONB NOT NULL,
        policy_epoch BIGINT NOT NULL CHECK (policy_epoch >= 0),
        fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
        heartbeat_at TIMESTAMPTZ NOT NULL,
        lease_expires_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (vol, session_id)
    );";

const HEARTBEAT: &str = "
    INSERT INTO client_sessions AS s (
        vol, session_id, owner, build, capabilities, policy_epoch,
        fencing_token, heartbeat_at, lease_expires_at
    )
    VALUES (
        $1, $2, $3, $4, $5::TEXT::JSONB,
        COALESCE((SELECT policy_epoch FROM volume_protocol WHERE vol = $1), 0),
        1, clock_timestamp(), clock_timestamp() + ($6::INT * interval '1 second')
    )
    ON CONFLICT (vol, session_id) DO UPDATE
    SET fencing_token = s.fencing_token
            + CASE WHEN s.lease_expires_at <= clock_timestamp() THEN 1 ELSE 0 END,
        heartbeat_at = clock_timestamp(),
        lease_expires_at = clock_timestamp() + ($6::INT * interval '1 second'),
        policy_epoch = GREATEST(s.policy_epoch, EXCLUDED.policy_epoch)
    WHERE s.owner = EXCLUDED.owner AND ($7::BIGINT IS NULL OR s.fencing_token = $7)
    RETURNING fencing_token";

const RELEASE: &str = "
    UPDATE client_sessions SET lease_expires_at = clock_timestamp()
    WHERE vol = $1 AND session_id = $2 AND owner = $3 AND fencing_token = $4";

#[derive(Clone)]
pub(super) struct SessionOwner {
    id: String,
    owner: String,
}

impl SessionOwner {
    pub(super) fn new() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            owner: Uuid::new_v4().to_string(),
        }
    }

    pub(super) async fn heartbeat_on_pool(
        &self,
        pool: Arc<PgConnectionPool>,
        timeout: Duration,
        volume: i64,
        expected_token: Option<i64>,
    ) -> MetaResult<SessionLease> {
        let owner = self.clone();
        PgMetaEngine::with_postgres_client_timeout(
            pool,
            timeout,
            "refresh client session",
            move |client| {
                Box::pin(async move { owner.heartbeat(client, volume, expected_token).await })
            },
        )
        .await
    }

    pub(super) async fn heartbeat<C: GenericClient + Sync>(
        &self,
        client: &C,
        volume: i64,
        expected_token: Option<i64>,
    ) -> MetaResult<SessionLease> {
        let capabilities = crate::compat::ClientCapabilities::current();
        let capabilities = serde_json::to_string(&SessionCapabilities {
            volume_format_min: SHARED_VOLUME_FORMAT.min_supported,
            volume_format_max: SHARED_VOLUME_FORMAT.current,
            write_requirement_max: SHARED_WRITE_REQUIREMENT.current,
            read: capabilities.read.into_iter().map(str::to_owned).collect(),
            write: capabilities.write.into_iter().map(str::to_owned).collect(),
            maintain: capabilities
                .maintain
                .into_iter()
                .map(str::to_owned)
                .collect(),
        })
        .map_err(|error| MetaError::Serialization(error.to_string()))?;
        let row = client
            .query_opt(
                HEARTBEAT,
                &[
                    &volume,
                    &self.id,
                    &self.owner,
                    &CLIENT_RELEASE,
                    &capabilities,
                    &LEASE_SECONDS,
                    &expected_token,
                ],
            )
            .await
            .map_err(|error| PgMetaEngine::classify_pg_error("refresh client session", error))?
            .ok_or_else(|| MetaError::Storage("client session ownership changed".into()))?;
        Ok(SessionLease {
            owner: self.clone(),
            token: row.get(0),
        })
    }
}

#[derive(Clone)]
pub(super) struct SessionLease {
    owner: SessionOwner,
    token: i64,
}

impl SessionLease {
    pub(super) fn token(&self) -> i64 {
        self.token
    }

    pub(super) async fn release<C: GenericClient + Sync>(
        &self,
        client: &C,
        volume: i64,
    ) -> MetaResult<()> {
        client
            .execute(
                RELEASE,
                &[&volume, &self.owner.id, &self.owner.owner, &self.token],
            )
            .await
            .map_err(|error| PgMetaEngine::classify_pg_error("release client session", error))?;
        Ok(())
    }

    pub(super) async fn stamp(&self, tx: &Transaction<'_>, volume: i64) -> MetaResult<()> {
        // Check expiry after locking the lease row. PostgreSQL can otherwise
        // evaluate a volatile WHERE predicate before waiting for the row lock.
        tx.query_opt(
            "WITH locked AS MATERIALIZED (
                SELECT policy_epoch, lease_expires_at FROM client_sessions
                WHERE vol = $1 AND session_id = $2 AND owner = $3 AND fencing_token = $4
                FOR SHARE
             )
             SELECT set_config('studiofs.session_id', $2, true),
                    set_config('studiofs.policy_epoch', policy_epoch::TEXT, true),
                    set_config('studiofs.session_fencing_token', $4::BIGINT::TEXT, true)
             FROM locked WHERE lease_expires_at > clock_timestamp()",
            &[&volume, &self.owner.id, &self.owner.owner, &self.token],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("stamp publication session", error))?
        .ok_or_else(|| MetaError::Storage("publication client session is inactive".into()))?;
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct ClientSessionState {
    lease: Option<SessionLease>,
    next_attempt: Option<Instant>,
    healthy: bool,
}

/// A short-lived session for a command publishing within one transaction.
pub struct PublicationSession(SessionLease);

impl PublicationSession {
    /// Register and stamp a session for this transaction.
    ///
    /// # Errors
    ///
    /// Returns a classified PostgreSQL error if registration or stamping fails.
    pub async fn begin(tx: &Transaction<'_>, volume: i64) -> MetaResult<Self> {
        let lease = SessionOwner::new().heartbeat(tx, volume, None).await?;
        lease.stamp(tx, volume).await?;
        Ok(Self(lease))
    }

    /// Expire the session after its last write and before committing.
    ///
    /// # Errors
    ///
    /// Returns a classified PostgreSQL error if expiry fails.
    pub async fn finish(self, tx: &Transaction<'_>, volume: i64) -> MetaResult<()> {
        self.0.release(tx, volume).await
    }
}

impl PgMetaEngine {
    pub(super) fn heartbeat_client_session(&self) {
        let _ = self.refresh_client_session();
    }

    /// Refresh the session at most once per interval. A failed attempt leaves
    /// local serving and allocator authority untouched.
    pub(super) fn refresh_client_session(&self) -> MetaResult<SessionLease> {
        let mut state = self
            .client_session_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .background_workers
            .poll_shutdown_ref()
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(MetaError::WorkerShutdownCancelled);
        }
        let now = Instant::now();
        if state.next_attempt.is_some_and(|next| now < next) {
            return state
                .lease
                .clone()
                .filter(|_| state.healthy)
                .ok_or_else(|| MetaError::Storage("client session refresh is pending".into()));
        }
        let owner = self.client_session_owner.clone();
        // Retry an ambiguous response by re-reading the generation through the
        // owner-checked upsert. Normal renewals always carry the expected token.
        let token = state
            .lease
            .as_ref()
            .filter(|_| state.healthy)
            .map(|lease| lease.token);
        let pool = self.command_pool.clone();
        let volume = self.volume_id;
        let timeout = self.command_timeout;
        let result = self
            .run_async(async move { owner.heartbeat_on_pool(pool, timeout, volume, token).await });
        if let Err(error) = &result {
            tracing::debug!(%error, "client session heartbeat deferred");
        }
        state.next_attempt = Some(Instant::now() + HEARTBEAT_INTERVAL);
        state.healthy = result.is_ok();
        if let Ok(lease) = &result {
            state.lease = Some(lease.clone());
        }
        result
    }

    pub(super) fn release_client_session(&self, deadline: Instant) {
        let Ok(mut state) = self.client_session_state.try_lock() else {
            return;
        };
        let Some(lease) = state.lease.take() else {
            return;
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        let pool = self.command_pool.clone();
        let volume = self.volume_id;
        let result = self.run_async(async move {
            tokio::time::timeout(
                remaining,
                Self::with_postgres_client_timeout(
                    pool,
                    remaining,
                    "release client session",
                    move |client| Box::pin(async move { lease.release(client, volume).await }),
                ),
            )
            .await
            .map_err(|_| MetaError::Timeout("release client session".into()))?
        });
        if let Err(error) = result {
            tracing::debug!(%error, "client session will expire on the server clock");
        }
    }
}

/// Observe the active and inactive sessions for one volume in one MVCC
/// snapshot. All rows use the same PostgreSQL clock reading for expiry.
///
/// # Errors
///
/// Returns a classified PostgreSQL error or an error decoding stored session data.
pub async fn read_client_sessions<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
) -> MetaResult<Vec<ClientSession>> {
    client
        .query(
            "WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS as_of)
         SELECT session_id, build, capabilities::TEXT, policy_epoch, fencing_token,
                heartbeat_at, lease_expires_at, lease_expires_at > observed.as_of
         FROM client_sessions CROSS JOIN observed WHERE vol = $1 ORDER BY session_id",
            &[&volume],
        )
        .await
        .map_err(|error| PgMetaEngine::classify_pg_error("read client sessions", error))?
        .into_iter()
        .map(|row| {
            let session_id = Uuid::parse_str(row.get(0))
                .map_err(|_| MetaError::Storage("invalid client session identity".into()))?;
            let capabilities = serde_json::from_str(row.get(2))
                .map_err(|_| MetaError::Storage("invalid client session capabilities".into()))?;
            Ok(ClientSession {
                session_id,
                build: row.get(1),
                capabilities,
                policy_epoch: row.get(3),
                fencing_token: row.get(4),
                heartbeat_at: row.get(5),
                lease_expires_at: row.get(6),
                active: row.get(7),
            })
        })
        .collect()
}

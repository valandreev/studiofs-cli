use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

use super::PgMetaEngine;
use super::runtime::PgConnectionPool;
use crate::client_sessions::{ClientSession, SessionCapabilities};
use crate::compat::{ClientCapabilities, Operation, VolumeRequirements, evaluate};
use crate::engine::{MetaError, MetaResult, UpgradeRequiredReason};
use crate::version::{CLIENT_RELEASE, SHARED_VOLUME_FORMAT, SHARED_WRITE_REQUIREMENT};
use crate::volume_protocol::VolumeProtocol;

pub(super) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const FAILED_REFRESH_BACKOFF: Duration = Duration::from_millis(250);
const LEASE_SECONDS: i32 = 60;
pub(super) const INACTIVE_SESSION_RETENTION_SECONDS: i32 = 86_400;

/// Test hook run before refresh network I/O, used to force refresh interleavings.
#[cfg(test)]
pub(super) static REFRESH_IO_HOOK: std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>> =
    std::sync::Mutex::new(None);
const PRUNE_INACTIVE_BATCH_LIMIT: i64 = 128;

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
    );
    CREATE INDEX IF NOT EXISTS idx_client_sessions_vol_lease_expires_at
        ON client_sessions (vol, lease_expires_at);";

const SELECT_VOLUME_PROTOCOL_FOR_SESSION: &str = "
    SELECT p.policy_epoch,
           COALESCE(m.write_requirement, 1::SMALLINT),
           p.requirements::TEXT
    FROM volume_protocol p
    LEFT JOIN metadata_state m ON m.vol = p.vol
    WHERE p.vol = $1";

const REGISTER: &str = "
    WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now_ts)
    INSERT INTO client_sessions (
        vol, session_id, owner, build, capabilities, policy_epoch,
        fencing_token, heartbeat_at, lease_expires_at
    )
    SELECT $1, $2, $3, $4, $5::TEXT::JSONB, $6,
           1, observed.now_ts, observed.now_ts + ($7::INT * interval '1 second')
    FROM observed
    ON CONFLICT (vol, session_id) DO NOTHING
    RETURNING fencing_token, policy_epoch";

const RENEW: &str = "
    WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now_ts)
    UPDATE client_sessions AS s
    SET fencing_token = s.fencing_token
            + CASE WHEN s.lease_expires_at <= observed.now_ts THEN 1 ELSE 0 END,
        heartbeat_at = observed.now_ts,
        lease_expires_at = observed.now_ts + ($6::INT * interval '1 second'),
        policy_epoch = GREATEST(s.policy_epoch, $5)
    FROM observed
    WHERE s.vol = $1 AND s.session_id = $2 AND s.owner = $3 AND s.fencing_token = $4
    RETURNING s.fencing_token, s.policy_epoch";

#[cfg(test)]
const RELEASE_LEASE: &str = "
    WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now_ts)
    UPDATE client_sessions AS s
    SET lease_expires_at = observed.now_ts,
        fencing_token = s.fencing_token + 1,
        owner = ''
    FROM observed
    WHERE s.vol = $1 AND s.session_id = $2 AND s.owner = $3 AND s.fencing_token = $4";

const CLOSE_OWNER: &str = "
    WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now_ts)
    INSERT INTO client_sessions AS s (
        vol, session_id, owner, build, capabilities, policy_epoch,
        fencing_token, heartbeat_at, lease_expires_at
    )
    SELECT $1, $2, '', $4, $5::TEXT::JSONB, 0, 1, observed.now_ts, observed.now_ts
    FROM observed
    ON CONFLICT (vol, session_id) DO UPDATE
    SET lease_expires_at = EXCLUDED.lease_expires_at,
        fencing_token = s.fencing_token + 1,
        owner = ''
    WHERE s.owner = $3";

const DELETE_SHORT_LIVED: &str = "
    DELETE FROM client_sessions
    WHERE vol = $1 AND session_id = $2 AND owner = $3 AND fencing_token = $4";

const PRUNE_EXPIRED: &str = "
    WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now_ts),
    expired AS (
        SELECT s.vol, s.session_id
        FROM client_sessions s CROSS JOIN observed
        WHERE s.vol = $1
          AND s.lease_expires_at < observed.now_ts - ($2::INT * interval '1 second')
        ORDER BY s.lease_expires_at ASC
        LIMIT $3
    )
    DELETE FROM client_sessions s
    USING expired, observed
    WHERE s.vol = expired.vol AND s.session_id = expired.session_id
      -- Rechecked on the latest row version so a concurrent renewal survives.
      AND s.lease_expires_at < observed.now_ts - ($2::INT * interval '1 second')";

const STAMP: &str = "
    WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS now_ts),
    locked AS MATERIALIZED (
        SELECT policy_epoch, lease_expires_at FROM client_sessions
        WHERE vol = $1 AND session_id = $2 AND owner = $3 AND fencing_token = $4
        FOR SHARE
    ),
    current_policy AS MATERIALIZED (
        SELECT COALESCE((SELECT policy_epoch FROM volume_protocol WHERE vol = $1), 0) AS epoch
    )
    SELECT locked.lease_expires_at > observed.now_ts AS lease_active,
           locked.policy_epoch = current_policy.epoch AS policy_current,
           CASE WHEN locked.lease_expires_at > observed.now_ts
                     AND locked.policy_epoch = current_policy.epoch
                THEN set_config('studiofs.session_id', $2, true) END,
           CASE WHEN locked.lease_expires_at > observed.now_ts
                     AND locked.policy_epoch = current_policy.epoch
                THEN set_config('studiofs.policy_epoch', locked.policy_epoch::TEXT, true) END,
           CASE WHEN locked.lease_expires_at > observed.now_ts
                     AND locked.policy_epoch = current_policy.epoch
                THEN set_config('studiofs.session_fencing_token', $4::BIGINT::TEXT, true) END,
           locked.policy_epoch
    FROM locked CROSS JOIN observed CROSS JOIN current_policy";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SessionRole {
    Mount,
    Maintenance,
}

#[derive(Debug, Clone)]
pub(super) enum PolicyValidation {
    /// The validated epoch (0 without a policy row) and the policy publication
    /// negotiates event encodings against.
    Compatible(i64, Arc<VolumeProtocol>),
    Incompatible(MetaError),
}

#[derive(Debug)]
pub(super) enum HeartbeatError {
    Fenced,
    Other(MetaError),
}

impl HeartbeatError {
    pub(super) fn into_meta_error(self) -> MetaError {
        match self {
            Self::Fenced => MetaError::Storage("client session ownership changed".into()),
            Self::Other(error) => error,
        }
    }
}

#[derive(Debug)]
pub(super) enum StampError {
    SessionInactive,
    PolicyStale,
    Other(MetaError),
}

impl StampError {
    pub(super) fn into_meta_error(self) -> MetaError {
        match self {
            Self::SessionInactive => {
                MetaError::Storage("publication client session is inactive".into())
            }
            Self::PolicyStale => {
                MetaError::Storage("publication client session policy epoch is stale".into())
            }
            Self::Other(error) => error,
        }
    }
}

fn classify_session_pg_error(context: &str, error: tokio_postgres::Error) -> MetaError {
    if error.code().is_some_and(|code| code.code() == "42P01") {
        return MetaError::Storage(format!(
            "{context} failed: client_sessions schema is missing (SQLSTATE 42P01); stop mounts and janitors, then run `studiofs upgrade NAME` (add `--repair-schema` to restore dropped tables)"
        ));
    }
    PgMetaEngine::classify_pg_error(context, error)
}

fn encode_capabilities() -> MetaResult<String> {
    let capabilities = ClientCapabilities::current();
    serde_json::to_string(&SessionCapabilities {
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
    .map_err(|error| MetaError::Serialization(error.to_string()))
}

pub(super) fn evaluate_protocol_for_role(
    protocol: &VolumeProtocol,
    role: SessionRole,
) -> Result<(), MetaError> {
    if role == SessionRole::Mount && protocol.write_requirement > SHARED_WRITE_REQUIREMENT.current {
        return Err(MetaError::UpgradeRequired {
            reason: UpgradeRequiredReason::WriteRequirementNewer {
                found: protocol.write_requirement,
                supported: SHARED_WRITE_REQUIREMENT.current,
            },
        });
    }
    let client_caps = ClientCapabilities::current();
    let mut volume_reqs = VolumeRequirements::baseline();
    protocol.apply_to(&mut volume_reqs);
    let decision = evaluate(&client_caps, &volume_reqs, &BTreeMap::new());
    let required_ops: &[Operation] = match role {
        SessionRole::Mount => &[
            Operation::Bootstrap,
            Operation::Replay,
            Operation::Publication,
            Operation::Checkpoint,
        ],
        SessionRole::Maintenance => &[
            Operation::Bootstrap,
            Operation::Replay,
            Operation::Maintenance,
        ],
    };
    for &operation in required_ops {
        if let Some(refusal) = decision.refusal(operation) {
            return Err(MetaError::UpgradeRequired {
                reason: UpgradeRequiredReason::VolumeProtocolUnknown {
                    detail: format!("{} ({})", refusal.reason.as_str(), refusal.subject),
                },
            });
        }
    }
    Ok(())
}

pub(super) async fn validate_volume_protocol_for_role<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
    role: SessionRole,
) -> MetaResult<PolicyValidation> {
    let row = client
        .query_opt(SELECT_VOLUME_PROTOCOL_FOR_SESSION, &[&volume])
        .await
        .map_err(|error| classify_session_pg_error("read volume protocol for session", error))?;
    let Some(row) = row else {
        return Ok(PolicyValidation::Compatible(
            0,
            Arc::new(VolumeProtocol::baseline()),
        ));
    };
    let epoch: i64 = row.get(0);
    let write_requirement: i16 = row.get(1);
    let requirements: String = row.get(2);
    let protocol = match VolumeProtocol::decode(epoch, write_requirement, &requirements) {
        Ok(protocol) => protocol,
        Err(error) => return Ok(PolicyValidation::Incompatible(error)),
    };
    match evaluate_protocol_for_role(&protocol, role) {
        Ok(()) => Ok(PolicyValidation::Compatible(epoch, Arc::new(protocol))),
        Err(error) => Ok(PolicyValidation::Incompatible(error)),
    }
}

/// Prunes expired `client_sessions` rows older than `retention_seconds` in a
/// single bounded batch. Preserves recently expired sessions for inspection.
pub(super) async fn prune_expired_client_sessions<C: GenericClient + Sync>(
    client: &C,
    volume: i64,
    retention_seconds: i32,
) -> MetaResult<u64> {
    client
        .execute(
            PRUNE_EXPIRED,
            &[&volume, &retention_seconds, &PRUNE_INACTIVE_BATCH_LIMIT],
        )
        .await
        .map_err(|error| classify_session_pg_error("prune expired client sessions", error))
}

#[derive(Clone, PartialEq, Eq)]
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

    #[cfg(test)]
    pub(super) fn id(&self) -> &str {
        &self.id
    }

    pub(super) async fn heartbeat_on_pool(
        &self,
        pool: Arc<PgConnectionPool>,
        timeout: Duration,
        volume: i64,
        expected_token: Option<i64>,
        role: SessionRole,
    ) -> Result<(SessionLease, Option<MetaError>), HeartbeatError> {
        let owner = self.clone();
        let outcome = PgMetaEngine::with_postgres_client_timeout(
            pool,
            timeout,
            "refresh client session",
            move |client| {
                Box::pin(async move {
                    match owner.heartbeat(client, volume, expected_token, role).await {
                        Ok(ok) => Ok(Ok(ok)),
                        Err(HeartbeatError::Fenced) => Ok(Err(HeartbeatError::Fenced)),
                        Err(HeartbeatError::Other(error)) => Err(error),
                    }
                })
            },
        )
        .await;
        match outcome {
            Ok(inner) => inner,
            Err(error) => Err(HeartbeatError::Other(error)),
        }
    }

    /// Registers (`expected_token == None`, INSERT-only) or renews
    /// (`expected_token == Some(token)`, strictly fenced UPDATE-only) this
    /// session. Never stamps an unvalidated `policy_epoch`; if the volume's
    /// policy is incompatible, returns the compatibility error alongside the
    /// lease so background heartbeats stay alive at their last validated epoch
    /// while publication refuses with the typed error.
    pub(super) async fn heartbeat<C: GenericClient + Sync>(
        &self,
        client: &C,
        volume: i64,
        expected_token: Option<i64>,
        role: SessionRole,
    ) -> Result<(SessionLease, Option<MetaError>), HeartbeatError> {
        let (validated_epoch, protocol, policy_error) =
            match validate_volume_protocol_for_role(client, volume, role).await {
                Ok(PolicyValidation::Compatible(epoch, protocol)) => (epoch, protocol, None),
                // Publication refuses with `policy_error` before it uses this lease.
                Ok(PolicyValidation::Incompatible(error)) => {
                    (0, Arc::new(VolumeProtocol::baseline()), Some(error))
                }
                Err(error) => return Err(HeartbeatError::Other(error)),
            };
        let row = match expected_token {
            None => {
                let capabilities = encode_capabilities().map_err(HeartbeatError::Other)?;
                client
                    .query_opt(
                        REGISTER,
                        &[
                            &volume,
                            &self.id,
                            &self.owner,
                            &CLIENT_RELEASE,
                            &capabilities,
                            &validated_epoch,
                            &LEASE_SECONDS,
                        ],
                    )
                    .await
                    .map_err(|error| {
                        HeartbeatError::Other(classify_session_pg_error(
                            "register client session",
                            error,
                        ))
                    })?
                    .ok_or(HeartbeatError::Fenced)?
            }
            Some(token) => client
                .query_opt(
                    RENEW,
                    &[
                        &volume,
                        &self.id,
                        &self.owner,
                        &token,
                        &validated_epoch,
                        &LEASE_SECONDS,
                    ],
                )
                .await
                .map_err(|error| {
                    HeartbeatError::Other(classify_session_pg_error(
                        "refresh client session",
                        error,
                    ))
                })?
                .ok_or(HeartbeatError::Fenced)?,
        };
        if expected_token.is_none() {
            // Opportunistic bounded retention sweep on initial registration.
            let _ =
                prune_expired_client_sessions(client, volume, INACTIVE_SESSION_RETENTION_SECONDS)
                    .await;
        }
        Ok((
            SessionLease {
                owner: self.clone(),
                token: row.get(0),
                validated_epoch,
                protocol,
                #[cfg(test)]
                policy_epoch: row.get(1),
            },
            policy_error,
        ))
    }

    /// Permanently closes this owner so neither delayed initial registration
    /// nor delayed renewals can resurrect it.
    pub(super) async fn close<C: GenericClient + Sync>(
        &self,
        client: &C,
        volume: i64,
    ) -> MetaResult<()> {
        let capabilities = encode_capabilities()?;
        client
            .execute(
                CLOSE_OWNER,
                &[
                    &volume,
                    &self.id,
                    &self.owner,
                    &CLIENT_RELEASE,
                    &capabilities,
                ],
            )
            .await
            .map_err(|error| classify_session_pg_error("close client session", error))?;
        Ok(())
    }

    pub(super) async fn close_on_pool(
        &self,
        pool: Arc<PgConnectionPool>,
        timeout: Duration,
        volume: i64,
    ) -> MetaResult<()> {
        let owner = self.clone();
        PgMetaEngine::with_postgres_client_timeout(
            pool,
            timeout,
            "close client session",
            move |client| Box::pin(async move { owner.close(client, volume).await }),
        )
        .await
    }
}

#[derive(Clone)]
pub(super) struct SessionLease {
    owner: SessionOwner,
    token: i64,
    /// Epoch of `protocol`; a stamp under any other epoch is stale.
    validated_epoch: i64,
    protocol: Arc<VolumeProtocol>,
    #[cfg(test)]
    policy_epoch: i64,
}

impl SessionLease {
    pub(super) fn token(&self) -> i64 {
        self.token
    }

    /// Policy this lease validated. After a successful stamp it is the
    /// volume's current policy for the rest of the transaction's checks.
    pub(super) fn protocol(&self) -> &VolumeProtocol {
        &self.protocol
    }

    #[cfg(test)]
    pub(super) fn owner(&self) -> &SessionOwner {
        &self.owner
    }

    #[cfg(test)]
    pub(super) fn policy_epoch(&self) -> i64 {
        self.policy_epoch
    }

    /// Releases this exact generation; a stale token cannot release a newer one.
    #[cfg(test)]
    pub(super) async fn release<C: GenericClient + Sync>(
        &self,
        client: &C,
        volume: i64,
    ) -> MetaResult<()> {
        client
            .execute(
                RELEASE_LEASE,
                &[&volume, &self.owner.id, &self.owner.owner, &self.token],
            )
            .await
            .map_err(|error| classify_session_pg_error("release client session", error))?;
        Ok(())
    }

    async fn delete_short_lived<C: GenericClient + Sync>(
        &self,
        client: &C,
        volume: i64,
    ) -> MetaResult<()> {
        client
            .execute(
                DELETE_SHORT_LIVED,
                &[&volume, &self.owner.id, &self.owner.owner, &self.token],
            )
            .await
            .map_err(|error| classify_session_pg_error("remove publication session", error))?;
        Ok(())
    }

    pub(super) async fn stamp_checked(
        &self,
        tx: &Transaction<'_>,
        volume: i64,
    ) -> Result<(), StampError> {
        let row = tx
            .query_opt(
                STAMP,
                &[&volume, &self.owner.id, &self.owner.owner, &self.token],
            )
            .await
            .map_err(|error| {
                StampError::Other(classify_session_pg_error(
                    "stamp publication session",
                    error,
                ))
            })?
            .ok_or(StampError::SessionInactive)?;
        let lease_active: bool = row.get(0);
        if !lease_active {
            return Err(StampError::SessionInactive);
        }
        let policy_current: bool = row.get(1);
        // A concurrent renewal may have stamped a newer epoch than the policy
        // this lease carries; publication must negotiate against the stamped one.
        if !policy_current || row.get::<_, i64>(5) != self.validated_epoch {
            return Err(StampError::PolicyStale);
        }
        Ok(())
    }

    pub(super) async fn stamp(&self, tx: &Transaction<'_>, volume: i64) -> MetaResult<()> {
        self.stamp_checked(tx, volume)
            .await
            .map_err(StampError::into_meta_error)
    }
}

pub(super) struct ClientSessionState {
    owner: SessionOwner,
    lease: Option<SessionLease>,
    next_heartbeat: Option<Instant>,
    retry_after: Option<Instant>,
    registration_attempted: bool,
    closed: bool,
    last_error: Option<MetaError>,
    policy_error: Option<MetaError>,
}

impl Default for ClientSessionState {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientSessionState {
    pub(super) fn new() -> Self {
        Self {
            owner: SessionOwner::new(),
            lease: None,
            next_heartbeat: None,
            retry_after: None,
            registration_attempted: false,
            closed: false,
            last_error: None,
            policy_error: None,
        }
    }
}

#[derive(Clone, Copy)]
enum RefreshMode<'a> {
    Background,
    Publication,
    /// Refresh past `stale` unless a concurrent refresh already replaced it.
    ForcePublication(&'a SessionLease),
}

/// A short-lived session for a command publishing within one transaction.
pub struct PublicationSession(SessionLease);

impl PublicationSession {
    /// Validate the volume policy, register, and stamp a session for this
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns `UpgradeRequired` / `RepairRequired` if the volume's policy is
    /// incompatible with this binary, or a classified PostgreSQL error if
    /// registration or stamping fails.
    pub async fn begin(tx: &Transaction<'_>, volume: i64) -> MetaResult<Self> {
        let (validated_epoch, protocol) =
            match validate_volume_protocol_for_role(tx, volume, SessionRole::Mount).await? {
                PolicyValidation::Compatible(epoch, protocol) => (epoch, protocol),
                PolicyValidation::Incompatible(error) => return Err(error),
            };
        let owner = SessionOwner::new();
        let capabilities = encode_capabilities()?;
        let row = tx
            .query_opt(
                REGISTER,
                &[
                    &volume,
                    &owner.id,
                    &owner.owner,
                    &CLIENT_RELEASE,
                    &capabilities,
                    &validated_epoch,
                    &LEASE_SECONDS,
                ],
            )
            .await
            .map_err(|error| classify_session_pg_error("register publication session", error))?
            .ok_or_else(|| MetaError::Storage("publication session identity conflict".into()))?;
        let lease = SessionLease {
            owner,
            token: row.get(0),
            validated_epoch,
            protocol,
            #[cfg(test)]
            policy_epoch: row.get(1),
        };
        lease.stamp(tx, volume).await?;
        Ok(Self(lease))
    }

    /// Removes the short-lived transactional session row after its last write
    /// and before committing, avoiding unbounded session-table growth.
    ///
    /// # Errors
    ///
    /// Returns a classified PostgreSQL error if cleanup fails.
    pub async fn finish(self, tx: &Transaction<'_>, volume: i64) -> MetaResult<()> {
        self.0.delete_short_lived(tx, volume).await
    }

    /// Policy validated and stamped for this transaction.
    pub(super) fn protocol(&self) -> &VolumeProtocol {
        self.0.protocol()
    }
}

impl PgMetaEngine {
    pub(super) fn heartbeat_client_session(&self) {
        let _ = self.refresh_client_session_inner(RefreshMode::Background);
    }

    /// Refresh the session for a background heartbeat or test probe. A failed
    /// attempt leaves local serving and allocator authority untouched.
    #[cfg(test)]
    pub(super) fn refresh_client_session(&self) -> MetaResult<SessionLease> {
        self.refresh_client_session_inner(RefreshMode::Publication)
    }

    pub(super) fn acquire_publication_session(&self) -> MetaResult<SessionLease> {
        self.refresh_client_session_inner(RefreshMode::Publication)
    }

    pub(super) fn force_refresh_publication_session(
        &self,
        stale: &SessionLease,
    ) -> MetaResult<SessionLease> {
        self.refresh_client_session_inner(RefreshMode::ForcePublication(stale))
    }

    fn refresh_client_session_inner(&self, mode: RefreshMode<'_>) -> MetaResult<SessionLease> {
        // Single-flight: concurrent refreshes would fence each other and race
        // their state writes. Waiters re-check the fast path below and reuse
        // the lease the previous refresh produced. `release_client_session`
        // never takes this guard, so close is not blocked by in-flight I/O.
        let _refresh = self
            .client_session_refresh
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (initial_owner, expected_token) = {
            let mut state = self
                .client_session_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed
                || self
                    .background_workers
                    .poll_shutdown_ref()
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(MetaError::WorkerShutdownCancelled);
            }
            let now = Instant::now();
            match mode {
                RefreshMode::Background => {
                    if state.next_heartbeat.is_some_and(|next| now < next) {
                        if let Some(lease) = state.lease.clone() {
                            return Ok(lease);
                        }
                        return Err(state.last_error.clone().unwrap_or_else(|| {
                            MetaError::RemoteUnavailable(
                                "client session refresh is backing off".into(),
                            )
                        }));
                    }
                }
                RefreshMode::Publication => {
                    if state.next_heartbeat.is_some_and(|next| now < next)
                        && let Some(lease) = state.lease.clone()
                    {
                        if let Some(policy_error) = state.policy_error.clone() {
                            return Err(policy_error);
                        }
                        return Ok(lease);
                    }
                    if state.lease.is_none() && state.retry_after.is_some_and(|retry| now < retry) {
                        return Err(state.last_error.clone().unwrap_or_else(|| {
                            MetaError::RemoteUnavailable(
                                "client session refresh is backing off".into(),
                            )
                        }));
                    }
                }
                RefreshMode::ForcePublication(stale) => {
                    if state.next_heartbeat.is_some_and(|next| now < next)
                        && let Some(lease) = state.lease.clone()
                        && (lease.owner != stale.owner || lease.token != stale.token)
                    {
                        if let Some(policy_error) = state.policy_error.clone() {
                            return Err(policy_error);
                        }
                        return Ok(lease);
                    }
                }
            }
            state.registration_attempted = true;
            (
                state.owner.clone(),
                state.lease.as_ref().map(SessionLease::token),
            )
        };

        #[cfg(test)]
        if let Some(hook) = REFRESH_IO_HOOK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            hook();
        }

        // Never hold `client_session_state` across PostgreSQL network I/O.
        let pool = self.command_pool.clone();
        let volume = self.volume_id;
        let timeout = self.command_timeout;
        let network_result = self.run_async(async move {
            match initial_owner
                .heartbeat_on_pool(
                    pool.clone(),
                    timeout,
                    volume,
                    expected_token,
                    SessionRole::Mount,
                )
                .await
            {
                Ok(ok) => Ok((initial_owner, ok)),
                Err(HeartbeatError::Fenced) => {
                    // Generation diverged (e.g., ambiguous timeout bumped server token).
                    // Close the diverged owner and rotate to a fresh session identity.
                    let _ = initial_owner
                        .close_on_pool(pool.clone(), timeout, volume)
                        .await;
                    let rotated = SessionOwner::new();
                    let ok = rotated
                        .heartbeat_on_pool(pool, timeout, volume, None, SessionRole::Mount)
                        .await
                        .map_err(HeartbeatError::into_meta_error)?;
                    Ok((rotated, ok))
                }
                Err(HeartbeatError::Other(error)) => Err(error),
            }
        });

        let (close_orphan, result) = {
            let mut state = self
                .client_session_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let shutting_down = state.closed
                || self
                    .background_workers
                    .poll_shutdown_ref()
                    .load(std::sync::atomic::Ordering::Acquire);
            if shutting_down {
                let orphan = match &network_result {
                    Ok((used_owner, _)) => Some(used_owner.clone()),
                    Err(_) => None,
                };
                (orphan, Err(MetaError::WorkerShutdownCancelled))
            } else {
                let now = Instant::now();
                match network_result {
                    Ok((used_owner, (lease, policy_error))) => {
                        state.owner = used_owner;
                        state.lease = Some(lease.clone());
                        state.next_heartbeat = Some(now + HEARTBEAT_INTERVAL);
                        state.retry_after = None;
                        state.last_error = None;
                        state.policy_error = policy_error.clone();
                        let out = if !matches!(mode, RefreshMode::Background) {
                            if let Some(error) = policy_error {
                                Err(error)
                            } else {
                                Ok(lease)
                            }
                        } else {
                            Ok(lease)
                        };
                        (None, out)
                    }
                    Err(error) => {
                        tracing::debug!(%error, "client session heartbeat deferred");
                        state.next_heartbeat = Some(now + HEARTBEAT_INTERVAL);
                        state.retry_after = Some(now + FAILED_REFRESH_BACKOFF);
                        state.last_error = Some(error.clone());
                        (None, Err(error))
                    }
                }
            }
        };

        if let Some(orphan) = close_orphan {
            let pool = self.command_pool.clone();
            let volume = self.volume_id;
            let timeout = self.command_timeout.min(Duration::from_secs(2));
            let _ =
                self.run_async(async move { orphan.close_on_pool(pool, timeout, volume).await });
        }
        result
    }

    pub(super) fn release_client_session(&self, deadline: Instant) {
        let (owner, should_close) = {
            let mut state = self
                .client_session_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            let should_close = state.registration_attempted || state.lease.is_some();
            state.lease = None;
            (state.owner.clone(), should_close)
        };
        if !should_close {
            return;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        let pool = self.command_pool.clone();
        let volume = self.volume_id;
        let result = self.run_async(async move {
            tokio::time::timeout(remaining, owner.close_on_pool(pool, remaining, volume))
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
        .map_err(|error| classify_session_pg_error("read client sessions", error))?
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

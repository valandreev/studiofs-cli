//! Snapshot v2 domain contracts.

use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;

/// PostgreSQL's durable identifier for a StudioFS volume.
pub type VolumeId = i64;

/// Maximum finite retention accepted by Snapshot v2.
///
/// The bound keeps the PostgreSQL `TIMESTAMPTZ` expiry calculation within a
/// practical range and prevents a durable job from becoming a poison pill.
pub const MAX_RETENTION_SECS: u64 = u32::MAX as u64;

/// Errors raised while constructing a Snapshot v2 value object.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum SnapshotValidationError {
    /// A required text value was empty or contained an unsupported character.
    #[error("invalid snapshot text value")]
    InvalidText,
    /// A volume identifier was not positive.
    #[error("invalid snapshot volume id")]
    InvalidVolumeId,
    /// A cut version was negative.
    #[error("invalid snapshot cut version")]
    InvalidCutVersion,
    /// A volume UUID was not canonical.
    #[error("invalid snapshot volume uuid")]
    InvalidVolumeUuid,
    /// A retention duration was zero or could not be represented.
    #[error("invalid snapshot retention")]
    InvalidRetention,
    /// A retention duration could not be added to its Ready timestamp.
    #[error("invalid snapshot expiration")]
    InvalidExpiration,
    /// An artifact descriptor contained an invalid field.
    #[error("invalid snapshot artifact descriptor")]
    InvalidArtifact,
}

/// Whether a snapshot was requested manually or by the automatic policy.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotOrigin {
    /// A user explicitly requested the snapshot.
    Manual,
    /// The volume's daily policy requested the snapshot.
    Automatic,
}

impl SnapshotOrigin {
    /// Returns the stable PostgreSQL representation of this origin.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Automatic => "automatic",
        }
    }
}

impl Display for SnapshotOrigin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SnapshotOrigin {
    type Err = SnapshotValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "manual" => Ok(Self::Manual),
            "automatic" => Ok(Self::Automatic),
            _ => Err(SnapshotValidationError::InvalidText),
        }
    }
}

/// Error returned for a malformed allocated public snapshot identifier.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
pub enum SnapshotIdError {
    /// The identifier did not contain the required volume and timestamp.
    #[error("invalid snapshot id")]
    Invalid,
    /// The volume name portion was invalid.
    #[error("invalid snapshot volume name")]
    InvalidVolumeName,
    /// The collision suffix was not allocated by PostgreSQL's ordinal scheme.
    #[error("invalid snapshot collision suffix")]
    InvalidSuffix,
}

/// A validated, PostgreSQL-allocated public snapshot identifier.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub struct SnapshotId(String);

impl SnapshotId {
    /// Builds the unsuffixed identifier for a PostgreSQL cut-registration time.
    pub fn from_cut(
        volume_name: &str,
        captured_at: DateTime<Utc>,
    ) -> Result<Self, SnapshotIdError> {
        validate_volume_name(volume_name)?;
        Ok(Self(format!(
            "{volume_name}.{}",
            captured_at.format("%Y-%m-%dT%H-%M-%S%.3fZ")
        )))
    }

    /// Parses the only public identifier shape accepted by Snapshot v2.
    pub fn parse(value: &str) -> Result<Self, SnapshotIdError> {
        let (volume_name, timestamp_and_suffix) =
            value.split_once('.').ok_or(SnapshotIdError::Invalid)?;
        validate_volume_name(volume_name)?;

        const TIMESTAMP_LEN: usize = 24;
        let (timestamp, suffix) = if timestamp_and_suffix.len() == TIMESTAMP_LEN {
            (timestamp_and_suffix, None)
        } else {
            if timestamp_and_suffix.len() < TIMESTAMP_LEN {
                return Err(SnapshotIdError::Invalid);
            }
            let (timestamp, suffix) = timestamp_and_suffix.split_at(TIMESTAMP_LEN);
            (timestamp, Some(parse_collision_suffix(suffix)?))
        };
        if timestamp.len() != TIMESTAMP_LEN {
            return Err(SnapshotIdError::Invalid);
        }
        NaiveDateTime::parse_from_str(
            timestamp
                .strip_suffix('Z')
                .ok_or(SnapshotIdError::Invalid)?,
            "%Y-%m-%dT%H-%M-%S%.3f",
        )
        .map_err(|_| SnapshotIdError::Invalid)?;
        let canonical = match suffix {
            None => format!("{volume_name}.{timestamp}"),
            Some(suffix) => format!("{volume_name}.{timestamp}-{suffix}"),
        };
        if canonical != value {
            return Err(SnapshotIdError::Invalid);
        }
        Ok(Self(canonical))
    }

    /// Returns the canonical public identifier text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns a PostgreSQL collision allocation derived from this base ID.
    pub(crate) fn with_collision_suffix(
        volume_name: &str,
        captured_at: DateTime<Utc>,
        suffix: u64,
    ) -> Result<Self, SnapshotIdError> {
        if suffix < 2 {
            return Err(SnapshotIdError::InvalidSuffix);
        }
        let base = Self::from_cut(volume_name, captured_at)?;
        Ok(Self(format!("{}-{suffix}", base.0)))
    }
}

impl<'de> Deserialize<'de> for SnapshotId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

impl Display for SnapshotId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for SnapshotId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for SnapshotId {
    type Err = SnapshotIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// The single immutable metadata cut identified by a snapshot.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub struct SnapshotCut {
    volume_id: VolumeId,
    cut_version: i64,
    captured_at: DateTime<Utc>,
}

impl<'de> Deserialize<'de> for SnapshotCut {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SnapshotCutFields {
            volume_id: VolumeId,
            cut_version: i64,
            captured_at: DateTime<Utc>,
        }

        let fields = SnapshotCutFields::deserialize(deserializer)?;
        Self::new(fields.volume_id, fields.cut_version, fields.captured_at)
            .map_err(serde::de::Error::custom)
    }
}

impl SnapshotCut {
    /// Creates a cut with a positive volume ID and non-negative published version.
    pub fn new(
        volume_id: VolumeId,
        cut_version: i64,
        captured_at: DateTime<Utc>,
    ) -> Result<Self, SnapshotValidationError> {
        if volume_id <= 0 {
            return Err(SnapshotValidationError::InvalidVolumeId);
        }
        if cut_version < 0 {
            return Err(SnapshotValidationError::InvalidCutVersion);
        }
        Ok(Self {
            volume_id,
            cut_version,
            captured_at,
        })
    }

    /// Returns the authoritative PostgreSQL volume ID.
    pub const fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    /// Returns the published metadata high-water mark included in the cut.
    pub const fn cut_version(&self) -> i64 {
        self.cut_version
    }

    /// Returns the PostgreSQL cut-registration timestamp in UTC.
    pub const fn captured_at(&self) -> DateTime<Utc> {
        self.captured_at
    }
}

/// Retention attached to a snapshot. `None` means infinite manual retention.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub struct SnapshotRetention {
    ttl: Option<Duration>,
}

impl SnapshotRetention {
    /// Creates manual retention, where `None` means retain forever.
    pub fn manual(ttl: Option<Duration>) -> Result<Self, SnapshotValidationError> {
        validate_retention(ttl)?;
        Ok(Self { ttl })
    }

    /// Creates automatic retention, which must be finite and positive.
    pub fn automatic(retention: Duration) -> Result<Self, SnapshotValidationError> {
        validate_retention(Some(retention))?;
        Ok(Self {
            ttl: Some(retention),
        })
    }

    /// Rejects infinite retention for automatic snapshots.
    pub fn validate_for_origin(
        &self,
        origin: SnapshotOrigin,
    ) -> Result<(), SnapshotValidationError> {
        if matches!(origin, SnapshotOrigin::Automatic) && self.ttl.is_none() {
            return Err(SnapshotValidationError::InvalidRetention);
        }
        Ok(())
    }

    /// Returns the configured duration, or `None` for infinite retention.
    pub const fn ttl(&self) -> Option<Duration> {
        self.ttl
    }

    /// Computes expiry from the durable Ready timestamp.
    ///
    /// Returns `Ok(None)` only for infinite manual retention.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotValidationError::InvalidExpiration`] if the timestamp
    /// range cannot represent the computed expiry.
    pub fn expires_at(
        &self,
        ready_at: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, SnapshotValidationError> {
        let Some(ttl) = self.ttl else {
            return Ok(None);
        };
        let ttl = ChronoDuration::from_std(ttl)
            .map_err(|_| SnapshotValidationError::InvalidExpiration)?;
        ready_at
            .checked_add_signed(ttl)
            .map(Some)
            .ok_or(SnapshotValidationError::InvalidExpiration)
    }
}

impl<'de> Deserialize<'de> for SnapshotRetention {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SnapshotRetentionFields {
            ttl: Option<Duration>,
        }

        let fields = SnapshotRetentionFields::deserialize(deserializer)?;
        Self::manual(fields.ttl).map_err(serde::de::Error::custom)
    }
}

/// One automatic daily snapshot policy for a volume.
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct SnapshotDailyPolicy {
    /// Local wall-clock time at which the policy becomes due.
    daily_at: NaiveTime,
    /// Persisted IANA timezone name, with `UTC` as the default.
    timezone: String,
    /// Finite automatic retention beginning at Ready publication.
    retention: SnapshotRetention,
}

impl<'de> Deserialize<'de> for SnapshotDailyPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct SnapshotDailyPolicyFields {
            daily_at: NaiveTime,
            timezone: String,
            retention: SnapshotRetention,
        }

        let fields = SnapshotDailyPolicyFields::deserialize(deserializer)?;
        let retention = fields
            .retention
            .ttl()
            .ok_or_else(|| serde::de::Error::custom("automatic policy retention is infinite"))?;
        Self::new(fields.daily_at, fields.timezone, retention).map_err(serde::de::Error::custom)
    }
}

impl SnapshotDailyPolicy {
    /// Creates a policy value without claiming PostgreSQL authority.
    pub fn new(
        daily_at: NaiveTime,
        timezone: impl Into<String>,
        retention: Duration,
    ) -> Result<Self, SnapshotValidationError> {
        let timezone = timezone.into();
        if timezone.is_empty() || timezone.len() > 128 || !timezone.is_ascii() {
            return Err(SnapshotValidationError::InvalidText);
        }
        Ok(Self {
            daily_at,
            timezone,
            retention: SnapshotRetention::automatic(retention)?,
        })
    }

    /// Returns the local scheduled time.
    pub const fn daily_at(&self) -> NaiveTime {
        self.daily_at
    }

    /// Returns the persisted timezone name.
    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    /// Returns the finite automatic retention.
    pub const fn retention(&self) -> SnapshotRetention {
        self.retention
    }
}

/// PostgreSQL-assigned generation and policy projection.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotPolicyRecord {
    /// Monotonically increasing policy generation.
    pub generation: i64,
    /// Active automatic policy value.
    pub policy: SnapshotDailyPolicy,
}

fn validate_retention(ttl: Option<Duration>) -> Result<(), SnapshotValidationError> {
    if ttl.is_some_and(|duration| {
        duration.is_zero()
            || duration.as_secs() > MAX_RETENTION_SECS
            || i64::try_from(duration.as_secs()).is_err()
            || ChronoDuration::from_std(duration).is_err()
    }) {
        return Err(SnapshotValidationError::InvalidRetention);
    }
    Ok(())
}

/// Durable catalog lifecycle. This state controls mount and deletion admission.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotLifecycle {
    /// The cut is registered and its artifact is being materialized.
    Creating,
    /// The artifact and all payload proofs are ready to mount.
    Ready,
    /// The attempt cannot produce a usable artifact without repair or retry.
    Failed,
    /// New mounts are rejected while existing leases drain.
    Deleting,
    /// Catalog and artifact cleanup is complete.
    Deleted,
}

impl SnapshotLifecycle {
    /// Returns the stable PostgreSQL representation of this lifecycle.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
        }
    }
}

/// Durable creation-job execution state, distinct from [`SnapshotLifecycle`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotCreationState {
    /// The job is waiting for a worker claim.
    Queued,
    /// A fenced worker owns the job.
    Running,
    /// The worker recorded a retry deadline.
    RetryWait,
    /// Artifact publication completed.
    Completed,
    /// The worker recorded an unrecoverable creation failure.
    Failed,
}

impl SnapshotCreationState {
    /// Returns the stable PostgreSQL representation of this execution state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::RetryWait => "retry_wait",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

/// Error for an illegal persisted state transition.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Error)]
#[error("illegal snapshot state transition")]
pub struct SnapshotTransitionError;

/// Validates a catalog lifecycle compare-and-set transition.
pub fn validate_snapshot_lifecycle_transition(
    from: SnapshotLifecycle,
    to: SnapshotLifecycle,
) -> Result<(), SnapshotTransitionError> {
    let legal = from == to
        || matches!(
            (from, to),
            (SnapshotLifecycle::Creating, SnapshotLifecycle::Ready)
                | (SnapshotLifecycle::Creating, SnapshotLifecycle::Failed)
                | (SnapshotLifecycle::Creating, SnapshotLifecycle::Deleting)
                | (SnapshotLifecycle::Ready, SnapshotLifecycle::Deleting)
                | (SnapshotLifecycle::Failed, SnapshotLifecycle::Creating)
                | (SnapshotLifecycle::Failed, SnapshotLifecycle::Deleting)
                | (SnapshotLifecycle::Deleting, SnapshotLifecycle::Deleted)
        );
    legal.then_some(()).ok_or(SnapshotTransitionError)
}

/// Validates a creation-job execution compare-and-set transition.
pub fn validate_snapshot_creation_transition(
    from: SnapshotCreationState,
    to: SnapshotCreationState,
) -> Result<(), SnapshotTransitionError> {
    let legal = from == to
        || matches!(
            (from, to),
            (
                SnapshotCreationState::Queued,
                SnapshotCreationState::Running
            ) | (SnapshotCreationState::Queued, SnapshotCreationState::Failed)
                | (
                    SnapshotCreationState::Running,
                    SnapshotCreationState::RetryWait
                )
                | (
                    SnapshotCreationState::Running,
                    SnapshotCreationState::Completed
                )
                | (
                    SnapshotCreationState::Running,
                    SnapshotCreationState::Failed
                )
                | (
                    SnapshotCreationState::RetryWait,
                    SnapshotCreationState::Running
                )
                | (
                    SnapshotCreationState::RetryWait,
                    SnapshotCreationState::Failed
                )
                | (SnapshotCreationState::Failed, SnapshotCreationState::Queued)
        );
    legal.then_some(()).ok_or(SnapshotTransitionError)
}

/// A local identity hint that has not yet been checked against PostgreSQL.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotVolumeIdentityHint {
    /// Locally discovered numeric volume identifier.
    pub volume_id: VolumeId,
    /// Locally discovered human-readable volume name.
    pub volume_name: String,
    /// Locally discovered immutable volume UUID.
    pub volume_uuid: String,
}

impl SnapshotVolumeIdentityHint {
    /// Validates a local hint without treating it as authoritative.
    pub fn new(
        volume_id: VolumeId,
        volume_name: impl Into<String>,
        volume_uuid: impl Into<String>,
    ) -> Result<Self, SnapshotValidationError> {
        let hint = Self {
            volume_id,
            volume_name: volume_name.into(),
            volume_uuid: volume_uuid.into(),
        };
        hint.validate()?;
        Ok(hint)
    }

    /// Validates all identity fields.
    pub fn validate(&self) -> Result<(), SnapshotValidationError> {
        if self.volume_id <= 0 {
            return Err(SnapshotValidationError::InvalidVolumeId);
        }
        validate_volume_name(&self.volume_name)
            .map_err(|_| SnapshotValidationError::InvalidText)?;
        uuid::Uuid::parse_str(&self.volume_uuid)
            .map_err(|_| SnapshotValidationError::InvalidVolumeUuid)?;
        Ok(())
    }
}

/// PostgreSQL-verified identity required by every catalog operation.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotVolumeContext {
    /// Authoritative numeric volume identifier.
    pub volume_id: VolumeId,
    /// Authoritative volume name used in public IDs.
    pub volume_name: String,
    /// Authoritative immutable volume UUID used for artifact isolation.
    pub volume_uuid: String,
}

impl SnapshotVolumeContext {
    /// Creates a verified context from PostgreSQL row values.
    pub fn new(
        volume_id: VolumeId,
        volume_name: impl Into<String>,
        volume_uuid: impl Into<String>,
    ) -> Result<Self, SnapshotValidationError> {
        let hint = SnapshotVolumeIdentityHint::new(volume_id, volume_name, volume_uuid)?;
        Ok(Self {
            volume_id: hint.volume_id,
            volume_name: hint.volume_name,
            volume_uuid: hint.volume_uuid,
        })
    }
}

/// A validated SHA-256 checksum stored with an immutable artifact descriptor.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SnapshotSha256([u8; 32]);

impl SnapshotSha256 {
    /// Creates a checksum from its raw 32-byte digest.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw digest.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns lowercase hexadecimal checksum text.
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

/// Thin immutable artifact metadata stored in the PostgreSQL catalog.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotArtifactDescriptor {
    /// Final immutable object key, never an attempt key.
    pub object_key: String,
    /// Artifact envelope format version.
    pub format_version: u16,
    /// Read-only artifact schema version.
    pub schema_version: u16,
    /// Exact artifact checksum.
    pub sha256: SnapshotSha256,
    /// Artifact byte length.
    pub byte_length: u64,
    /// Cut represented by the artifact.
    pub cut: SnapshotCut,
    /// Time at which the final object was verified.
    pub verified_at: DateTime<Utc>,
}

impl SnapshotArtifactDescriptor {
    /// Validates and creates an immutable artifact descriptor.
    pub fn new(
        object_key: impl Into<String>,
        format_version: u16,
        schema_version: u16,
        sha256: SnapshotSha256,
        byte_length: u64,
        cut: SnapshotCut,
        verified_at: DateTime<Utc>,
    ) -> Result<Self, SnapshotValidationError> {
        let object_key = object_key.into();
        if object_key.is_empty() || object_key.contains("..") || object_key.contains('\0') {
            return Err(SnapshotValidationError::InvalidArtifact);
        }
        if format_version == 0 || schema_version == 0 {
            return Err(SnapshotValidationError::InvalidArtifact);
        }
        Ok(Self {
            object_key,
            format_version,
            schema_version,
            sha256,
            byte_length,
            cut,
            verified_at,
        })
    }
}

/// Catalog projection used by list, inspect, and worker readiness checks.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotCatalogEntry {
    /// Public PostgreSQL-allocated identifier.
    pub snapshot_id: SnapshotId,
    /// Verified volume identity.
    pub volume: SnapshotVolumeContext,
    /// Explicit manual or automatic origin.
    pub origin: SnapshotOrigin,
    /// Immutable metadata cut.
    pub cut: SnapshotCut,
    /// Catalog lifecycle.
    pub lifecycle: SnapshotLifecycle,
    /// Worker execution state.
    pub creation_state: SnapshotCreationState,
    /// Retention policy.
    pub retention: SnapshotRetention,
    /// Durable Ready timestamp, if published.
    pub ready_at: Option<DateTime<Utc>>,
    /// Durable expiry timestamp, if finite.
    pub expires_at: Option<DateTime<Utc>>,
    /// Final artifact descriptor, only after publication.
    pub artifact: Option<SnapshotArtifactDescriptor>,
    /// Redacted latest failure, if any.
    pub failure: Option<String>,
}

/// Durable snapshot creation job state.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotCreationJob {
    /// Snapshot whose artifact is being created.
    pub snapshot_id: SnapshotId,
    /// Current execution state.
    pub state: SnapshotCreationState,
    /// Number of retry attempts already recorded.
    pub retry_count: u32,
    /// Current worker owner, if claimed.
    pub owner: Option<String>,
    /// Current lease expiry.
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// Fencing token for conditional writes.
    pub fencing_token: i64,
    /// Earliest next retry time.
    pub next_retry_at: Option<DateTime<Utc>>,
    /// Redacted latest failure.
    pub failure: Option<String>,
}

/// Fenced creation lease returned by a conditional claim.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotCreationLease {
    /// Worker that owns the lease.
    pub owner: String,
    /// Lease expiry in UTC.
    pub expires_at: DateTime<Utc>,
    /// Monotonically increasing fencing token.
    pub fencing_token: i64,
}

/// Fenced mount lease for a verified Ready snapshot.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMountLease {
    /// Mounted snapshot identifier.
    pub snapshot_id: SnapshotId,
    /// Mount owner identity.
    pub owner: String,
    /// Lease expiry in UTC.
    pub expires_at: DateTime<Utc>,
    /// Last successful heartbeat.
    pub heartbeat_at: DateTime<Utc>,
    /// Monotonically increasing fencing token.
    pub fencing_token: i64,
    /// Whether release has been durably requested.
    pub released: bool,
}

fn validate_volume_name(value: &str) -> Result<(), SnapshotIdError> {
    if value.is_empty()
        || value.len() > 255
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(SnapshotIdError::InvalidVolumeName);
    }
    Ok(())
}

fn parse_collision_suffix(value: &str) -> Result<u64, SnapshotIdError> {
    let digits = value
        .strip_prefix('-')
        .ok_or(SnapshotIdError::InvalidSuffix)?;
    if digits.is_empty() || (digits.len() > 1 && digits.starts_with('0')) {
        return Err(SnapshotIdError::InvalidSuffix);
    }
    let suffix = digits
        .parse::<u64>()
        .map_err(|_| SnapshotIdError::InvalidSuffix)?;
    (suffix >= 2)
        .then_some(suffix)
        .ok_or(SnapshotIdError::InvalidSuffix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn origin_is_explicit_and_creation_state_is_not_lifecycle() {
        let manual = SnapshotOrigin::Manual;
        let automatic = SnapshotOrigin::Automatic;

        assert_ne!(manual, automatic);
        assert!(
            validate_snapshot_lifecycle_transition(
                SnapshotLifecycle::Creating,
                SnapshotLifecycle::Ready
            )
            .is_ok()
        );
        assert!(
            validate_snapshot_lifecycle_transition(
                SnapshotLifecycle::Ready,
                SnapshotLifecycle::Creating
            )
            .is_err()
        );
        assert!(
            validate_snapshot_creation_transition(
                SnapshotCreationState::Queued,
                SnapshotCreationState::Running
            )
            .is_ok()
        );
        assert!(
            validate_snapshot_creation_transition(
                SnapshotCreationState::Completed,
                SnapshotCreationState::Running
            )
            .is_err()
        );
    }

    #[test]
    fn cut_uses_only_cut_version_and_captured_at() {
        let captured_at = Utc
            .with_ymd_and_hms(2026, 8, 12, 14, 7, 9)
            .single()
            .expect("valid UTC timestamp");
        let cut = SnapshotCut::new(7, 42, captured_at).expect("valid snapshot cut");

        assert_eq!(cut.volume_id(), 7);
        assert_eq!(cut.cut_version(), 42);
        assert_eq!(cut.captured_at(), captured_at);
        assert!(SnapshotCut::new(7, -1, captured_at).is_err());
        assert!(SnapshotRetention::manual(None).is_ok());
        assert!(SnapshotRetention::manual(Some(Duration::ZERO)).is_err());
        assert!(SnapshotRetention::manual(Some(Duration::MAX)).is_err());
        assert!(SnapshotRetention::automatic(Duration::from_secs(60)).is_ok());
    }

    #[test]
    fn snapshot_id_uses_volume_and_millisecond_utc_cut_timestamp() {
        let captured_at = Utc
            .with_ymd_and_hms(2026, 8, 12, 14, 7, 9)
            .single()
            .expect("valid UTC timestamp");
        let id = SnapshotId::from_cut("projectfs", captured_at).expect("valid snapshot id");

        assert_eq!(id.as_str(), "projectfs.2026-08-12T14-07-09.000Z");
        assert_eq!(id.to_string(), "projectfs.2026-08-12T14-07-09.000Z");
        assert_eq!(SnapshotId::parse(id.as_str()).expect("parse id"), id);
        assert!(SnapshotId::parse("projectfs.2026-08-12-14-07-09").is_err());
        assert!(SnapshotId::parse("projectfs.2026-08-12T14-07-09.000Z-1").is_err());
        assert!(SnapshotId::parse("projectfs.2026-08-12T14-07-09.000Z-02").is_err());
        assert!(SnapshotId::parse("projectfs.2026-08-12T14-07-09.000Z-2").is_ok());
    }

    #[test]
    fn automatic_origin_rejects_infinite_retention() {
        let infinite = SnapshotRetention::manual(None).expect("manual infinite retention");
        assert!(infinite.validate_for_origin(SnapshotOrigin::Manual).is_ok());
        assert!(
            infinite
                .validate_for_origin(SnapshotOrigin::Automatic)
                .is_err()
        );
        assert!(
            SnapshotRetention::automatic(Duration::from_secs(60))
                .expect("automatic retention")
                .validate_for_origin(SnapshotOrigin::Automatic)
                .is_ok()
        );
    }

    #[test]
    fn retention_accepts_the_postgres_safe_upper_bound() {
        assert!(SnapshotRetention::automatic(Duration::from_secs(MAX_RETENTION_SECS)).is_ok());
    }

    #[test]
    fn retention_rejects_values_that_can_overflow_postgres_timestamptz() {
        assert!(SnapshotRetention::automatic(Duration::from_secs(MAX_RETENTION_SECS + 1)).is_err());
    }

    #[test]
    fn retention_expiration_reports_datetime_overflow() {
        let retention =
            SnapshotRetention::automatic(Duration::from_secs(60)).expect("finite retention");

        assert_eq!(
            retention.expires_at(DateTime::<Utc>::MAX_UTC),
            Err(SnapshotValidationError::InvalidExpiration)
        );
    }

    #[test]
    fn infinite_retention_has_no_expiration() {
        let retention = SnapshotRetention::manual(None).expect("infinite retention");

        assert_eq!(retention.expires_at(DateTime::<Utc>::MIN_UTC), Ok(None));
    }

    #[test]
    fn policy_value_requires_finite_retention_and_nonempty_timezone() {
        let daily_at = NaiveTime::from_hms_opt(2, 30, 0).expect("valid daily time");
        let policy = SnapshotDailyPolicy::new(daily_at, "UTC", Duration::from_secs(24 * 60 * 60))
            .expect("valid policy");
        assert_eq!(policy.timezone(), "UTC");
        assert!(SnapshotDailyPolicy::new(daily_at, "", Duration::from_secs(60)).is_err());
        assert!(SnapshotDailyPolicy::new(daily_at, "UTC", Duration::ZERO).is_err());
    }

    #[test]
    fn serde_deserialization_revalidates_snapshot_values() {
        let invalid_id = serde_json::from_str::<SnapshotId>("\"projectfs.2026-08-12-14-07-09\"");
        assert!(invalid_id.is_err());

        let invalid_cut = serde_json::from_str::<SnapshotCut>(
            "{\"volume_id\":0,\"cut_version\":1,\"captured_at\":\"2026-08-12T14:07:09Z\"}",
        );
        assert!(invalid_cut.is_err());

        let invalid_retention =
            serde_json::from_str::<SnapshotRetention>("{\"ttl\":{\"secs\":0,\"nanos\":0}}");
        assert!(invalid_retention.is_err());
    }

    #[test]
    fn snapshot_port_error_redacts_sentinel_credentials() {
        let error = crate::snapshot_ports::SnapshotPortError::from_backend(
            "capture snapshot",
            "postgres://snapshot-user-sentinel:snapshot-password-sentinel@db.example/
             ?access_key=snapshot-access-sentinel&secret_key=snapshot-secret-sentinel",
        );
        let rendered = format!("{error:?} {error}");

        assert!(!rendered.contains("snapshot-user-sentinel"));
        assert!(!rendered.contains("snapshot-password-sentinel"));
        assert!(!rendered.contains("snapshot-access-sentinel"));
        assert!(!rendered.contains("snapshot-secret-sentinel"));
        assert!(rendered.contains("capture snapshot"));
    }
}

//! Per-volume protocol policy (ADR 0018 "Per-volume policy and policy epoch",
//! SFS-1007).
//!
//! Each volume has one policy record: the three required capability sets, the
//! activation state of every feature, a policy epoch that increases on every
//! change, and the write requirement that publication checks atomically with
//! the version bump. The types are plain data with no PostgreSQL concepts, so
//! the 0.2.7 backend-neutral interface (SFS-1036) reuses them. The PostgreSQL
//! adapter lives in `pg_engine::volume_protocol`.
//!
//! Provisioning writes [`VolumeProtocol::baseline`](crate::volume_protocol::VolumeProtocol::baseline) and never activates a
//! feature. Every later change goes through
//! [`check_update`](crate::volume_protocol::check_update), which only lets a
//! feature move one lifecycle step at a time and never lets a binary require a
//! capability it does not itself declare.

use std::collections::{BTreeMap, BTreeSet};

use crate::compat::{
    BASELINE_WRITE_CAPABILITIES, CAP_EVENT_CODEC_V1, CAP_MAINTENANCE_PAYLOAD_GC_V2,
    CAP_PAYLOAD_SFEC_V1, CAP_PAYLOAD_STFS_V2, CAP_PAYLOAD_STFS_V3, CheckpointDescriptor,
    ClientCapabilities, VolumeRequirements,
};
use crate::engine::{MetaError, MetaResult, UpgradeRequiredReason};
use crate::version::SHARED_WRITE_REQUIREMENT;

/// Epoch of a freshly provisioned policy.
pub const INITIAL_POLICY_EPOCH: u64 = 1;

/// Lifecycle of a feature on one volume (Expand–Migrate–Activate–Contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureState {
    /// Expanded: shared objects exist, nothing on the volume uses the feature.
    Available,
    /// In use, but every required-set binary can still serve the volume.
    Compatible,
    /// Listed in at least one required set; binaries without it are fenced
    /// from that operation.
    Required,
    /// Contracted; no supported binary needs it. Terminal.
    Retired,
}

impl FeatureState {
    /// Whether a feature may move from `self` to `to` in one policy change.
    /// Unchanged states are always allowed.
    #[must_use]
    pub const fn may_become(self, to: Self) -> bool {
        use FeatureState::{Available, Compatible, Required, Retired};
        matches!(
            (self, to),
            (Available, Available | Compatible | Retired)
                | (Compatible, Available | Compatible | Required | Retired)
                // Deactivation (rollback before Contract) goes back to Compatible.
                | (Required, Compatible | Required)
                | (Retired, Retired)
        )
    }
}

/// Persisted part of the policy; the epoch and write requirement are stored
/// beside it so publication can check the requirement without decoding this.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolRequirements {
    /// A binary missing any of these must not mount or replay.
    pub read: BTreeSet<String>,
    /// A binary missing any of these must not publish.
    pub write: BTreeSet<String>,
    /// A binary missing any of these must not delete, prune or run GC.
    pub maintain: BTreeSet<String>,
    /// Serving checkpoint format and state schema selected for this volume.
    pub checkpoint: CheckpointDescriptor,
    /// Activation state of every feature the volume has ever known.
    pub features: BTreeMap<String, FeatureState>,
}

/// One volume's protocol policy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeProtocol {
    /// Increases by one on every change.
    pub epoch: u64,
    /// Publication succeeds only when this is at most the publisher's
    /// [`SHARED_WRITE_REQUIREMENT`]. Activate raises it to fence older writers.
    pub write_requirement: u16,
    /// Required sets, checkpoint identity and feature states.
    pub requirements: ProtocolRequirements,
}

/// Why a policy change or a stored policy was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolRejection {
    /// An operation requires a maintenance capability this binary lacks.
    #[error("maintenance_forbidden: {reason} ({capability})")]
    MaintenanceForbidden {
        /// Stable compatibility reason.
        reason: &'static str,
        /// Required capability or version identity.
        capability: String,
    },
    /// Previously admitted remote maintenance must drain before activation.
    #[error("maintenance is in progress; retry the policy change after it drains")]
    MaintenanceInProgress,
    /// The volume has no policy record.
    #[error("the volume has no protocol policy; run `studiofs upgrade NAME` to create it")]
    Missing,
    /// Another change committed first; reread the policy and retry.
    #[error("protocol policy epoch is {found}, expected {expected}; reread the policy and retry")]
    EpochConflict {
        /// Epoch the caller based its change on.
        expected: u64,
        /// Epoch currently stored.
        found: u64,
    },
    /// A capability is in a required set without being a `required` feature,
    /// or a `required` feature is in no required set.
    #[error("capability `{0}` is inconsistent between the required sets and its feature state")]
    Inconsistent(String),
    /// A feature skipped a lifecycle step, left `retired`, or was removed.
    #[error("feature `{name}` cannot move from {from:?} to {to:?}")]
    InvalidTransition {
        /// Feature name.
        name: String,
        /// Stored state; `None` for a feature not yet known.
        from: Option<FeatureState>,
        /// Requested state; `None` when the feature was removed.
        to: Option<FeatureState>,
    },
    /// The acting binary would require a capability it does not declare.
    #[error("this StudioFS binary does not declare capability `{0}` and cannot activate it")]
    UnknownCapability(String),
    /// The proposed checkpoint format or schema is unsupported by the actor.
    #[error("this StudioFS binary does not support {identity} {found}")]
    UnsupportedCheckpoint {
        /// Boundary of the unsupported version.
        identity: &'static str,
        /// Proposed version.
        found: u16,
    },
    /// The write requirement does not follow the write-set change.
    #[error("write requirement {found} is invalid: {rule}")]
    WriteRequirement {
        /// Requested value.
        found: u16,
        /// Violated rule.
        rule: &'static str,
    },
    /// Publication would emit an event encoding that some reader admitted by
    /// the policy may not decode (SFS-1010). The event stays unpublished
    /// local work until the capability is required for readers.
    #[error(
        "event `{kind}` needs reader capability `{capability}`, which the volume protocol does not require; it stays unpublished"
    )]
    EventNotRepresentable {
        /// [`MetaEvent::kind`](crate::events::MetaEvent::kind) of the refused event.
        kind: &'static str,
        /// Read capability its oldest faithful encoding needs.
        capability: &'static str,
    },
    /// An explicit feature cutover (SFS-1017) or Contract DDL step was refused.
    #[error("feature cutover refused: {0}")]
    CutoverRefused(String),
}

impl VolumeProtocol {
    /// The 0.2.5 baseline policy: `init`, `load` and the format 5/6 adoption
    /// (SFS-1132) all write exactly this. Every baseline capability is
    /// `required`; nothing else is known or active.
    #[must_use]
    pub fn baseline() -> Self {
        let base = VolumeRequirements::baseline();
        let features = [
            CAP_EVENT_CODEC_V1,
            CAP_PAYLOAD_STFS_V2,
            CAP_PAYLOAD_STFS_V3,
            CAP_PAYLOAD_SFEC_V1,
            CAP_MAINTENANCE_PAYLOAD_GC_V2,
        ]
        .into_iter()
        .map(|name| (name.to_owned(), FeatureState::Required))
        .collect();
        Self {
            epoch: INITIAL_POLICY_EPOCH,
            write_requirement: SHARED_WRITE_REQUIREMENT.min_supported,
            requirements: ProtocolRequirements {
                read: base.read,
                write: base.write,
                maintain: base.maintain,
                checkpoint: CheckpointDescriptor::BASELINE,
                features,
            },
        }
    }

    /// Checks that the required sets and feature states agree.
    ///
    /// # Errors
    ///
    /// [`ProtocolRejection::Inconsistent`] or
    /// [`ProtocolRejection::WriteRequirement`].
    pub fn validate(&self) -> Result<(), ProtocolRejection> {
        let sets = &self.requirements;
        let required = |name: &str| {
            sets.read.contains(name) || sets.write.contains(name) || sets.maintain.contains(name)
        };
        if let Some(name) = sets
            .read
            .iter()
            .chain(&sets.write)
            .chain(&sets.maintain)
            .find(|name| sets.features.get(name.as_str()) != Some(&FeatureState::Required))
        {
            return Err(ProtocolRejection::Inconsistent(name.clone()));
        }
        if let Some((name, _)) = sets
            .features
            .iter()
            .find(|(name, state)| **state == FeatureState::Required && !required(name))
        {
            return Err(ProtocolRejection::Inconsistent(name.clone()));
        }
        if self.write_requirement < SHARED_WRITE_REQUIREMENT.min_supported {
            return Err(ProtocolRejection::WriteRequirement {
                found: self.write_requirement,
                rule: "it must be at least 1",
            });
        }
        Ok(())
    }

    /// Copies the required sets, checkpoint identity and `compatible`
    /// (optional, in use) features into evaluator input.
    pub fn apply_to(&self, volume: &mut VolumeRequirements) {
        let sets = &self.requirements;
        volume.read.clone_from(&sets.read);
        volume.write.clone_from(&sets.write);
        volume.maintain.clone_from(&sets.maintain);
        volume.checkpoint = Some(sets.checkpoint);
        volume.optional = sets
            .features
            .iter()
            .filter(|(_, state)| **state == FeatureState::Compatible)
            .map(|(name, _)| name.clone())
            .collect();
    }

    /// Decides every operation for `volume` under this policy. The mount checks
    /// and the operator report both use this path.
    #[must_use]
    pub fn compatibility_decision(
        &self,
        client: &ClientCapabilities,
        mut volume: VolumeRequirements,
        actual_checkpoint: Option<CheckpointDescriptor>,
        local: &BTreeMap<String, u16>,
    ) -> crate::compat::CompatibilityDecision {
        self.apply_to(&mut volume);
        volume.checkpoint = actual_checkpoint;
        crate::compat::evaluate(client, &volume, local)
    }

    /// Returns a refusal for a read or write operation against this policy.
    pub fn check_operations(
        &self,
        volume: VolumeRequirements,
        actual_checkpoint: Option<CheckpointDescriptor>,
        local: &BTreeMap<String, u16>,
        operations: &[crate::compat::Operation],
    ) -> crate::MetaResult<()> {
        use crate::{MetaError, UpgradeRequiredReason};
        self.validate()?;
        for &operation in operations {
            if operation == crate::compat::Operation::Publication
                && self.write_requirement > SHARED_WRITE_REQUIREMENT.current
            {
                return Err(MetaError::UpgradeRequired {
                    reason: UpgradeRequiredReason::WriteRequirementNewer {
                        found: self.write_requirement,
                        supported: SHARED_WRITE_REQUIREMENT.current,
                    },
                });
            }
        }
        let decision = self.compatibility_decision(
            &ClientCapabilities::current(),
            volume,
            actual_checkpoint,
            local,
        );
        for &operation in operations {
            if let Some(refusal) = decision.refusal(operation) {
                return Err(MetaError::UpgradeRequired {
                    reason: UpgradeRequiredReason::VolumeProtocolUnknown {
                        detail: format!(
                            "{} ({}). {}",
                            refusal.reason.as_str(),
                            refusal.subject,
                            refusal.reason.action()
                        ),
                    },
                });
            }
        }
        Ok(())
    }

    /// Encodes the persisted requirements.
    ///
    /// # Errors
    ///
    /// `Serialization` if encoding fails.
    pub fn encode_requirements(&self) -> MetaResult<String> {
        serde_json::to_string(&self.requirements)
            .map_err(|error| MetaError::Serialization(error.to_string()))
    }

    /// Decodes a stored policy. An unknown field or feature state was written
    /// by a newer binary, so it fails closed as `UpgradeRequired`.
    ///
    /// # Errors
    ///
    /// `UpgradeRequired` for an undecodable or out-of-range record, and
    /// `RepairRequired` for a decodable but inconsistent one.
    pub fn decode(epoch: i64, write_requirement: i16, requirements: &str) -> MetaResult<Self> {
        let unknown = |detail: String| MetaError::UpgradeRequired {
            reason: UpgradeRequiredReason::VolumeProtocolUnknown { detail },
        };
        let requirements: ProtocolRequirements =
            serde_json::from_str(requirements).map_err(|error| unknown(error.to_string()))?;
        let protocol = Self {
            epoch: u64::try_from(epoch)
                .map_err(|_| unknown(format!("policy epoch {epoch} is negative")))?,
            write_requirement: u16::try_from(write_requirement).map_err(|_| {
                unknown(format!("write requirement {write_requirement} is negative"))
            })?,
            requirements,
        };
        protocol.validate().map_err(|error| {
            MetaError::RepairRequired(format!("stored volume protocol: {error}"))
        })?;
        Ok(protocol)
    }
}

/// Validates `proposed` as the next policy after `current` when made by a
/// binary that declares `actor`. Returns `false` when nothing changes, in
/// which case the epoch must not move.
///
/// Rules: every feature moves by [`FeatureState::may_become`] and is never
/// removed; a new feature starts `available`; a feature becoming `compatible`
/// or `required`, and every capability added to a required set, must be
/// declared by `actor` in the matching set; adding a write capability raises
/// the write requirement, an unchanged write set keeps it, and it never
/// exceeds what `actor` itself can publish. Lowering the requirement requires
/// the actor to declare every retained write capability. Returning to the
/// baseline requires baseline writers to support them as well. Changing the
/// checkpoint identity requires the actor to support the proposed format and
/// schema.
///
/// # Errors
///
/// The first violated rule as a [`ProtocolRejection`].
pub fn check_update(
    current: &VolumeProtocol,
    proposed: &VolumeProtocol,
    actor: &ClientCapabilities,
) -> Result<bool, ProtocolRejection> {
    proposed.validate()?;
    let (old, new) = (&current.requirements, &proposed.requirements);
    for (name, from) in &old.features {
        let to = new.features.get(name).copied();
        if !to.is_some_and(|to| from.may_become(to)) {
            return Err(ProtocolRejection::InvalidTransition {
                name: name.clone(),
                from: Some(*from),
                to,
            });
        }
    }
    let declared = |name: &str| {
        actor.read.contains(name) || actor.write.contains(name) || actor.maintain.contains(name)
    };
    for (name, to) in &new.features {
        let from = old.features.get(name).copied();
        if from.is_none() && *to != FeatureState::Available {
            return Err(ProtocolRejection::InvalidTransition {
                name: name.clone(),
                from,
                to: Some(*to),
            });
        }
        let activated = matches!(to, FeatureState::Compatible | FeatureState::Required);
        if activated && from != Some(*to) && !declared(name) {
            return Err(ProtocolRejection::UnknownCapability(name.clone()));
        }
    }
    for (old_set, new_set, known) in [
        (&old.read, &new.read, &actor.read),
        (&old.write, &new.write, &actor.write),
        (&old.maintain, &new.maintain, &actor.maintain),
    ] {
        if let Some(name) = new_set
            .difference(old_set)
            .find(|name| !known.contains(name.as_str()))
        {
            return Err(ProtocolRejection::UnknownCapability(name.clone()));
        }
    }
    let found = proposed.write_requirement;
    let rule = if found > SHARED_WRITE_REQUIREMENT.current {
        Some("it exceeds what this binary can publish")
    } else if new.write.difference(&old.write).next().is_some() {
        (found <= current.write_requirement).then_some("adding a write capability must raise it")
    } else if new.write == old.write {
        (found != current.write_requirement)
            .then_some("it may only change together with the write set")
    } else {
        (found > current.write_requirement)
            .then_some("removing write capabilities must not raise it")
    };
    if let Some(rule) = rule {
        return Err(ProtocolRejection::WriteRequirement { found, rule });
    }
    if found < current.write_requirement {
        if let Some(name) = new
            .write
            .iter()
            .find(|name| !actor.write.contains(name.as_str()))
        {
            return Err(ProtocolRejection::UnknownCapability(name.clone()));
        }
        if found == SHARED_WRITE_REQUIREMENT.min_supported
            && new
                .write
                .iter()
                .any(|name| !BASELINE_WRITE_CAPABILITIES.contains(&name.as_str()))
        {
            return Err(ProtocolRejection::WriteRequirement {
                found,
                rule: "baseline writers must support every retained write capability",
            });
        }
    }
    if new.checkpoint != old.checkpoint {
        for (identity, found) in [
            (actor.checkpoint_format, new.checkpoint.format),
            (actor.checkpoint_schema, new.checkpoint.schema),
        ] {
            if !identity.supports(found) {
                return Err(ProtocolRejection::UnsupportedCheckpoint {
                    identity: identity.name,
                    found,
                });
            }
        }
    }
    Ok(found != current.write_requirement || new != old)
}

/// Phase of an explicit Expand–Migrate–Activate–Contract feature cutover (SFS-1017).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CutoverPhase {
    /// Register the feature as `available`; all 0.2.5+ clients stay writable.
    Expand,
    /// Mark the feature `compatible` (in use, optional); all 0.2.5+ clients stay writable.
    Migrate,
    /// Require the feature for the selected roles (`required`), fencing clients that lack it.
    Activate,
    /// Pre-Contract rollback: move `required` back to `compatible`, or `compatible` back to `available`.
    Deactivate,
    /// Terminal retirement (`retired`) on this volume; allowed only from `available`
    /// (after `deactivate`). Runs no DDL; the database-wide check gates the Contract DDL step.
    Contract,
}

impl CutoverPhase {
    /// Stable lowercase name for diagnostics and CLI output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Expand => "expand",
            Self::Migrate => "migrate",
            Self::Activate => "activate",
            Self::Deactivate => "deactivate",
            Self::Contract => "contract",
        }
    }
}

/// Operation set a feature gates when activated as `required`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityRole {
    /// Required to mount, bootstrap, and replay.
    Read,
    /// Required to publish events or volume settings.
    Write,
    /// Required to run GC, janitor, or pruning.
    Maintain,
}

/// How expired sessions with unknown offline local work are handled before a breaking activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OfflineClientPolicy {
    /// Expired sessions lacking the feature block cutover because they may hold unpublished local work.
    #[default]
    BlockUnknownWork,
    /// Operator explicitly accepts that offline clients lacking the feature will be fenced on reconnect.
    AcceptFence,
}

/// Operator request for a per-volume feature cutover step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutoverRequest {
    /// Target capability name.
    pub feature: String,
    /// Target lifecycle phase.
    pub phase: CutoverPhase,
    /// Capability roles required when activating. `Deactivate` removes every role
    /// and rejects a non-empty set; other phases ignore it.
    pub roles: BTreeSet<CapabilityRole>,
    /// Optional explicit `write_requirement` override.
    pub write_requirement: Option<u16>,
    /// Policy for expired sessions that may hold unknown offline local work.
    pub offline_client_policy: OfflineClientPolicy,
}

/// Local cache observations fed into cutover readiness checks when available.
///
/// Event publication to PostgreSQL happens only after the corresponding payload
/// uploads have completed, so `unpublished_events` covers dirty/unpublished
/// payload work as well.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CutoverLocalState {
    /// Unpublished local metadata events in the inspected local cache.
    pub unpublished_events: u64,
    /// Local applied metadata version, if a local cache was inspected.
    pub local_version: Option<i64>,
}

/// Shared backend observations fed into cutover readiness checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutoverSharedState {
    /// Current remote metadata head version.
    pub remote_head_version: i64,
    /// Latest published baseline checkpoint version.
    pub checkpoint_version: i64,
    /// Whether the serving checkpoint is ready.
    pub checkpoint_ready: bool,
    /// Published checkpoint encodings available for bootstrap.
    pub available_checkpoints: Vec<CheckpointDescriptor>,
    /// Whether snapshot creation or GC/cleanup maintenance is currently active.
    pub snapshot_or_gc_active: bool,
    /// Description of active snapshot/GC work, if any.
    pub active_maintenance_detail: Option<String>,
}

/// A specific readiness blocker preventing a cutover transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CutoverBlocker {
    /// The requested feature state transition violates `check_update` or `FeatureState::may_become`.
    InvalidTransition(String),
    /// The acting binary does not declare a capability required by the proposed policy.
    UnsupportedByActor(String),
    /// An active client session lacks a required capability or write requirement.
    ActiveUnsupportedSession {
        /// Session identifier.
        session_id: uuid::Uuid,
        /// Client build string.
        build: String,
        /// Capabilities or requirements missing from the session.
        missing: Vec<String>,
    },
    /// An expired session lacks the capability and may still hold unpublished offline local work.
    ExpiredUnsupportedSession {
        /// Session identifier.
        session_id: uuid::Uuid,
        /// Client build string.
        build: String,
        /// Capabilities or requirements missing from the session.
        missing: Vec<String>,
    },
    /// Local cache has unpublished metadata events (or payloads awaiting publication).
    DirtyUnpublishedLocalWork {
        /// Unpublished local metadata event count.
        unpublished_events: u64,
    },
    /// Local cache has not caught up to the remote metadata head.
    ReplicationLag {
        /// Local applied metadata version.
        local_version: i64,
        /// Remote metadata head version.
        remote_head_version: i64,
    },
    /// The volume has metadata but no published serving checkpoint with the
    /// policy's checkpoint descriptor (cutover never changes that descriptor).
    MissingCompatibleCheckpoint {
        /// Required checkpoint descriptor.
        required: CheckpointDescriptor,
    },
    /// Snapshot creation or GC/cleanup maintenance is in progress.
    SnapshotOrGcInProgress {
        /// Detail of the active maintenance or snapshot job.
        detail: String,
    },
}

impl std::fmt::Display for CutoverBlocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTransition(msg) => write!(f, "invalid transition: {msg}"),
            Self::UnsupportedByActor(msg) => write!(f, "unsupported by acting binary: {msg}"),
            Self::ActiveUnsupportedSession {
                session_id,
                build,
                missing,
            } => write!(
                f,
                "active session {session_id} (build {build}) lacks: {}",
                missing.join(", ")
            ),
            Self::ExpiredUnsupportedSession {
                session_id,
                build,
                missing,
            } => write!(
                f,
                "expired session {session_id} (build {build}) lacks [{}] and may hold unknown offline local work (pass --accept-offline-clients to fence on reconnect)",
                missing.join(", ")
            ),
            Self::DirtyUnpublishedLocalWork { unpublished_events } => write!(
                f,
                "local cache has dirty unpublished work ({unpublished_events} unpublished events)"
            ),
            Self::ReplicationLag {
                local_version,
                remote_head_version,
            } => write!(
                f,
                "local replication lag: local version {local_version} < remote head {remote_head_version}"
            ),
            Self::MissingCompatibleCheckpoint { required } => write!(
                f,
                "no published serving checkpoint for format {} schema {}",
                required.format, required.schema
            ),
            Self::SnapshotOrGcInProgress { detail } => {
                write!(f, "snapshot or GC maintenance in progress: {detail}")
            }
        }
    }
}

/// Partitioned client sessions inspected for cutover readiness.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionInspection {
    /// Active sessions that support the proposed policy.
    pub eligible_active: Vec<crate::client_sessions::ClientSession>,
    /// Active sessions that lack required capabilities or write requirement.
    pub blocking_active: Vec<crate::client_sessions::ClientSession>,
    /// Expired sessions that lack required capabilities or write requirement.
    pub expired_unsupported: Vec<crate::client_sessions::ClientSession>,
    /// Active sessions running on an older policy epoch.
    pub stale_epoch_active: Vec<crate::client_sessions::ClientSession>,
}

/// Full dry-run or pre-execution report for an explicit feature cutover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutoverDryRunReport {
    /// Target feature capability name.
    pub feature: String,
    /// Requested cutover phase.
    pub phase: CutoverPhase,
    /// Current policy epoch.
    pub current_epoch: u64,
    /// Next policy epoch if applied (`current_epoch` when no-op).
    pub next_epoch: u64,
    /// Current stored policy.
    pub current: VolumeProtocol,
    /// Proposed policy if transition construction succeeded.
    pub proposed: Option<VolumeProtocol>,
    /// Whether this transition can be rolled back later.
    pub reversible: bool,
    /// Human-readable description of the rollback boundary.
    pub rollback_boundary: String,
    /// Inspected client sessions.
    pub sessions: SessionInspection,
    /// Blockers preventing execution.
    pub blockers: Vec<CutoverBlocker>,
}

impl CutoverDryRunReport {
    /// Returns `true` when there are no blockers.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.blockers.is_empty()
    }
}

/// Computes the missing capabilities/requirements for a `ClientSession` against `proposed`.
#[must_use]
pub fn missing_capabilities_for_session(
    session: &crate::client_sessions::ClientSession,
    proposed: &VolumeProtocol,
) -> Vec<String> {
    let mut missing = Vec::new();
    if session.capabilities.volume_format_max < crate::compat::BASELINE_VOLUME_FORMAT {
        missing.push(format!(
            "volume_format>={}",
            crate::compat::BASELINE_VOLUME_FORMAT
        ));
    }
    if session.capabilities.write_requirement_max < proposed.write_requirement {
        missing.push(format!("write_requirement>={}", proposed.write_requirement));
    }
    for cap in &proposed.requirements.read {
        if !session.capabilities.read.iter().any(|c| c == cap) {
            missing.push(format!("read:{cap}"));
        }
    }
    for cap in &proposed.requirements.write {
        if !session.capabilities.write.iter().any(|c| c == cap) {
            missing.push(format!("write:{cap}"));
        }
    }
    for cap in &proposed.requirements.maintain {
        if !session.capabilities.maintain.iter().any(|c| c == cap) {
            missing.push(format!("maintain:{cap}"));
        }
    }
    missing
}

/// Constructs the proposed [`VolumeProtocol`] for a cutover phase transition.
///
/// # Errors
///
/// Returns [`ProtocolRejection`] if the requested transition is invalid for the
/// current feature state or roles.
pub(crate) fn build_proposed_protocol(
    current: &VolumeProtocol,
    request: &CutoverRequest,
) -> Result<VolumeProtocol, ProtocolRejection> {
    let mut proposed = current.clone();
    let current_state = current.requirements.features.get(&request.feature).copied();
    match request.phase {
        CutoverPhase::Expand => {
            if let Some(state) = current_state
                && state != FeatureState::Available
            {
                return Err(ProtocolRejection::InvalidTransition {
                    name: request.feature.clone(),
                    from: Some(state),
                    to: Some(FeatureState::Available),
                });
            }
            proposed
                .requirements
                .features
                .insert(request.feature.clone(), FeatureState::Available);
        }
        CutoverPhase::Migrate => {
            if current_state == Some(FeatureState::Required) {
                return Err(ProtocolRejection::InvalidTransition {
                    name: request.feature.clone(),
                    from: Some(FeatureState::Required),
                    to: Some(FeatureState::Compatible),
                });
            }
            proposed
                .requirements
                .features
                .insert(request.feature.clone(), FeatureState::Compatible);
        }
        CutoverPhase::Activate => {
            if request.roles.is_empty() && current_state != Some(FeatureState::Required) {
                return Err(ProtocolRejection::Inconsistent(request.feature.clone()));
            }
            proposed
                .requirements
                .features
                .insert(request.feature.clone(), FeatureState::Required);
            for role in &request.roles {
                match role {
                    CapabilityRole::Read => {
                        proposed.requirements.read.insert(request.feature.clone());
                    }
                    CapabilityRole::Write => {
                        if proposed.requirements.write.insert(request.feature.clone()) {
                            proposed.write_requirement =
                                current.write_requirement.saturating_add(1);
                        }
                    }
                    CapabilityRole::Maintain => {
                        proposed
                            .requirements
                            .maintain
                            .insert(request.feature.clone());
                    }
                }
            }
            // An explicit value always wins; `check_update` validates it.
            if let Some(wr) = request.write_requirement {
                proposed.write_requirement = wr;
            }
        }
        CutoverPhase::Deactivate if !request.roles.is_empty() => {
            return Err(ProtocolRejection::CutoverRefused(format!(
                "deactivate removes every role of `{}`; do not pass roles",
                request.feature
            )));
        }
        CutoverPhase::Deactivate => match current_state {
            Some(FeatureState::Required) => {
                proposed
                    .requirements
                    .features
                    .insert(request.feature.clone(), FeatureState::Compatible);
                proposed.requirements.read.remove(&request.feature);
                let removed_write = proposed.requirements.write.remove(&request.feature);
                proposed.requirements.maintain.remove(&request.feature);
                if removed_write {
                    proposed.write_requirement = request
                        .write_requirement
                        .unwrap_or(SHARED_WRITE_REQUIREMENT.min_supported);
                }
            }
            Some(FeatureState::Compatible) => {
                proposed
                    .requirements
                    .features
                    .insert(request.feature.clone(), FeatureState::Available);
            }
            Some(FeatureState::Available) => {}
            other => {
                return Err(ProtocolRejection::InvalidTransition {
                    name: request.feature.clone(),
                    from: other,
                    to: Some(FeatureState::Compatible),
                });
            }
        },
        CutoverPhase::Contract => {
            if current_state != Some(FeatureState::Available) {
                return Err(ProtocolRejection::CutoverRefused(format!(
                    "feature `{}` is {current_state:?}; deactivate it to available before contract",
                    request.feature
                )));
            }
            proposed
                .requirements
                .features
                .insert(request.feature.clone(), FeatureState::Retired);
        }
    }
    Ok(proposed)
}

/// Evaluates whether `request` can safely transition `current` given `actor`,
/// `sessions`, `local`, and `shared` observations.
#[must_use]
pub fn evaluate_cutover_readiness(
    current: &VolumeProtocol,
    request: &CutoverRequest,
    actor: &ClientCapabilities,
    sessions: &[crate::client_sessions::ClientSession],
    local: &CutoverLocalState,
    shared: &CutoverSharedState,
) -> CutoverDryRunReport {
    let mut blockers = Vec::new();
    let mut inspection = SessionInspection::default();
    let (reversible, rollback_boundary) = match request.phase {
        CutoverPhase::Expand => (
            true,
            "Reversible: feature is only marked available; 0.2.5+ clients remain writable."
                .to_owned(),
        ),
        CutoverPhase::Migrate => (
            true,
            "Reversible before Activate: deactivate returns feature to available.".to_owned(),
        ),
        CutoverPhase::Activate => (
            true,
            "Reversible before Contract via `deactivate` (unless an irreversible format was written)."
                .to_owned(),
        ),
        CutoverPhase::Deactivate => (
            true,
            "Rollback step: returns feature to pre-activation compatibility.".to_owned(),
        ),
        CutoverPhase::Contract => (
            false,
            "Irreversible: `retired` is terminal and cannot be deactivated.".to_owned(),
        ),
    };

    let proposed = match build_proposed_protocol(current, request) {
        Ok(proposed) => match check_update(current, &proposed, actor) {
            Ok(_) => Some(proposed),
            Err(err @ ProtocolRejection::UnknownCapability(_))
            | Err(err @ ProtocolRejection::UnsupportedCheckpoint { .. }) => {
                blockers.push(CutoverBlocker::UnsupportedByActor(err.to_string()));
                None
            }
            Err(err) => {
                blockers.push(CutoverBlocker::InvalidTransition(err.to_string()));
                None
            }
        },
        Err(err) => {
            blockers.push(CutoverBlocker::InvalidTransition(err.to_string()));
            None
        }
    };

    let target_protocol = proposed.as_ref().unwrap_or(current);
    let mut changed = false;
    if let Some(prop) = &proposed {
        changed = prop.write_requirement != current.write_requirement
            || prop.requirements != current.requirements;
    }

    for session in sessions {
        let missing = missing_capabilities_for_session(session, target_protocol);
        if session.active {
            if u64::try_from(session.policy_epoch).map_or(true, |epoch| epoch < current.epoch) {
                inspection.stale_epoch_active.push(session.clone());
            }
            if missing.is_empty() {
                inspection.eligible_active.push(session.clone());
            } else {
                inspection.blocking_active.push(session.clone());
                if matches!(
                    request.phase,
                    CutoverPhase::Activate | CutoverPhase::Contract
                ) {
                    blockers.push(CutoverBlocker::ActiveUnsupportedSession {
                        session_id: session.session_id,
                        build: session.build.clone(),
                        missing,
                    });
                }
            }
        } else if !missing.is_empty() {
            inspection.expired_unsupported.push(session.clone());
            if request.phase == CutoverPhase::Activate
                && request.offline_client_policy == OfflineClientPolicy::BlockUnknownWork
            {
                blockers.push(CutoverBlocker::ExpiredUnsupportedSession {
                    session_id: session.session_id,
                    build: session.build.clone(),
                    missing,
                });
            }
        }
    }

    if matches!(
        request.phase,
        CutoverPhase::Activate | CutoverPhase::Contract
    ) {
        if local.unpublished_events > 0 {
            blockers.push(CutoverBlocker::DirtyUnpublishedLocalWork {
                unpublished_events: local.unpublished_events,
            });
        }
        if let Some(local_version) = local.local_version
            && local_version < shared.remote_head_version
        {
            blockers.push(CutoverBlocker::ReplicationLag {
                local_version,
                remote_head_version: shared.remote_head_version,
            });
        }
        if shared.snapshot_or_gc_active {
            blockers.push(CutoverBlocker::SnapshotOrGcInProgress {
                detail: shared
                    .active_maintenance_detail
                    .clone()
                    .unwrap_or_else(|| "active snapshot or GC operation".to_owned()),
            });
        }
    }

    if request.phase == CutoverPhase::Activate {
        let required_chk = target_protocol.requirements.checkpoint;
        let has_chk =
            shared.checkpoint_ready && shared.available_checkpoints.contains(&required_chk);
        if shared.remote_head_version > 0 && !has_chk {
            blockers.push(CutoverBlocker::MissingCompatibleCheckpoint {
                required: required_chk,
            });
        }
    }

    let next_epoch = if changed {
        current.epoch.saturating_add(1)
    } else {
        current.epoch
    };

    CutoverDryRunReport {
        feature: request.feature.clone(),
        phase: request.phase,
        current_epoch: current.epoch,
        next_epoch,
        current: current.clone(),
        proposed,
        reversible,
        rollback_boundary,
        sessions: inspection,
        blockers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::{LOCAL_PAYLOAD_INDEX_FORMAT, Operation, evaluate};
    use crate::version::LOCAL_METADATA_LAYOUT;

    const FUTURE: &str = "metadata.future.v1";

    fn successor_actor() -> ClientCapabilities {
        let mut actor = ClientCapabilities::current();
        actor.read.insert(FUTURE);
        actor.write.insert(FUTURE);
        actor
    }

    fn with(protocol: &VolumeProtocol, edit: impl FnOnce(&mut VolumeProtocol)) -> VolumeProtocol {
        let mut next = protocol.clone();
        edit(&mut next);
        next
    }

    fn set_state(state: FeatureState) -> impl FnOnce(&mut VolumeProtocol) {
        move |next| {
            next.requirements.features.insert(FUTURE.to_owned(), state);
        }
    }

    #[test]
    fn baseline_is_consistent_and_activates_nothing_beyond_the_baseline() {
        let baseline = VolumeProtocol::baseline();
        baseline.validate().unwrap();
        assert_eq!((baseline.epoch, baseline.write_requirement), (1, 1));
        assert!(
            baseline
                .requirements
                .features
                .values()
                .all(|state| *state == FeatureState::Required)
        );
        let mut volume = VolumeRequirements::baseline();
        baseline.apply_to(&mut volume);
        assert_eq!(volume, VolumeRequirements::baseline());
    }

    #[test]
    fn stored_policy_round_trips_and_unknown_shapes_fail_closed() {
        let baseline = VolumeProtocol::baseline();
        let json = baseline.encode_requirements().unwrap();
        assert_eq!(VolumeProtocol::decode(1, 1, &json).unwrap(), baseline);
        for stored in [
            json.replacen("{", r#"{"successor_field":1,"#, 1),
            json.replace(r#""required""#, r#""successor_state""#),
        ] {
            assert!(matches!(
                VolumeProtocol::decode(1, 1, &stored),
                Err(MetaError::UpgradeRequired {
                    reason: UpgradeRequiredReason::VolumeProtocolUnknown { .. }
                })
            ));
        }
        assert!(matches!(
            VolumeProtocol::decode(-1, 1, &json),
            Err(MetaError::UpgradeRequired { .. })
        ));
        let inconsistent = json.replace(
            r#""maintenance.payload_gc.v2":"required""#,
            r#""maintenance.payload_gc.v2":"compatible""#,
        );
        assert!(matches!(
            VolumeProtocol::decode(1, 1, &inconsistent),
            Err(MetaError::RepairRequired(_))
        ));
    }

    #[test]
    fn stored_checkpoint_requirements_drive_policy_validation_and_evaluation() {
        use crate::compat::{CheckpointDescriptor, ReasonCode};

        let baseline = VolumeProtocol::baseline();
        let actor = ClientCapabilities::current();
        let mut json: serde_json::Value =
            serde_json::from_str(&baseline.encode_requirements().unwrap()).unwrap();
        for (format, schema, supported) in [(3, 1, true), (5, 1, false), (4, 2, false)] {
            json["checkpoint"] = serde_json::json!({"format": format, "schema": schema});
            let proposal = VolumeProtocol::decode(1, 1, &json.to_string()).unwrap();
            let round_trip =
                VolumeProtocol::decode(1, 1, &proposal.encode_requirements().unwrap()).unwrap();
            assert_eq!(round_trip, proposal);
            let mut volume = VolumeRequirements::baseline();
            volume.checkpoint = None;
            round_trip.apply_to(&mut volume);
            assert_eq!(
                volume.checkpoint,
                Some(CheckpointDescriptor { format, schema })
            );
            let assessment = evaluate(&actor, &volume, &BTreeMap::new());
            if supported {
                assert_eq!(check_update(&baseline, &proposal, &actor), Ok(true));
                assert!(assessment.allows(Operation::Checkpoint));
            } else {
                let (identity, found) = if format > actor.checkpoint_format.current {
                    (actor.checkpoint_format.name, format)
                } else {
                    (actor.checkpoint_schema.name, schema)
                };
                assert_eq!(
                    check_update(&baseline, &proposal, &actor),
                    Err(ProtocolRejection::UnsupportedCheckpoint { identity, found })
                );
                assert_eq!(
                    assessment.refusal(Operation::Checkpoint).unwrap().reason,
                    ReasonCode::CheckpointFormatNewer
                );
            }
        }
    }

    #[test]
    fn features_move_one_lifecycle_step_and_retired_is_terminal() {
        let actor = successor_actor();
        let baseline = VolumeProtocol::baseline();
        assert!(matches!(
            check_update(
                &baseline,
                &with(&baseline, set_state(FeatureState::Compatible)),
                &actor
            ),
            Err(ProtocolRejection::InvalidTransition { from: None, .. })
        ));
        let available = with(&baseline, set_state(FeatureState::Available));
        assert!(check_update(&baseline, &available, &actor).unwrap());
        let compatible = with(&available, set_state(FeatureState::Compatible));
        assert!(check_update(&available, &compatible, &actor).unwrap());
        let retired = with(&compatible, set_state(FeatureState::Retired));
        assert!(check_update(&compatible, &retired, &actor).unwrap());
        for state in [FeatureState::Available, FeatureState::Compatible] {
            assert!(matches!(
                check_update(&retired, &with(&retired, set_state(state)), &actor),
                Err(ProtocolRejection::InvalidTransition { .. })
            ));
        }
        let removed = with(&retired, |next| {
            next.requirements.features.remove(FUTURE);
        });
        assert!(matches!(
            check_update(&retired, &removed, &actor),
            Err(ProtocolRejection::InvalidTransition { to: None, .. })
        ));
        // No change keeps the epoch.
        assert!(!check_update(&retired, &retired, &actor).unwrap());
    }

    #[test]
    fn activating_a_write_capability_raises_the_write_requirement_and_fences_old_writers() {
        let actor = successor_actor();
        let compatible = with(
            &with(
                &VolumeProtocol::baseline(),
                set_state(FeatureState::Available),
            ),
            set_state(FeatureState::Compatible),
        );
        let activate = |write_requirement| {
            with(&compatible, |next| {
                set_state(FeatureState::Required)(next);
                next.requirements.write.insert(FUTURE.to_owned());
                next.write_requirement = write_requirement;
            })
        };
        assert!(matches!(
            check_update(&compatible, &activate(1), &actor),
            Err(ProtocolRejection::WriteRequirement { found: 1, .. })
        ));
        // The baseline binary cannot publish above its own write requirement.
        assert!(matches!(
            check_update(&compatible, &activate(2), &actor),
            Err(ProtocolRejection::WriteRequirement { found: 2, .. })
        ));
        // A baseline actor does not declare the capability at all.
        let mut no_raise = activate(1);
        no_raise.write_requirement = 1;
        assert!(matches!(
            check_update(&compatible, &no_raise, &ClientCapabilities::current()),
            Err(ProtocolRejection::UnknownCapability(name)) if name == FUTURE
        ));
        // Changing the requirement without a write-set change is refused.
        let mut bumped = compatible.clone();
        bumped.write_requirement = 0;
        assert!(check_update(&compatible, &bumped, &actor).is_err());
    }

    #[test]
    fn lowering_the_write_requirement_checks_every_remaining_write_capability() {
        let actor = successor_actor();
        let remaining = "metadata.other_writer.v1";
        let current = with(&VolumeProtocol::baseline(), |next| {
            next.requirements
                .write
                .insert(CAP_PAYLOAD_STFS_V2.to_owned());
            for name in [FUTURE, remaining] {
                next.requirements.write.insert(name.to_owned());
                next.requirements
                    .features
                    .insert(name.to_owned(), FeatureState::Required);
            }
            next.write_requirement = 2;
        });
        let rollback = with(&current, |next| {
            next.requirements.write.remove(FUTURE);
            set_state(FeatureState::Compatible)(next);
            next.write_requirement = 1;
        });
        assert_eq!(
            check_update(&current, &rollback, &actor),
            Err(ProtocolRejection::UnknownCapability(remaining.to_owned()))
        );
        let mut capable_actor = actor;
        capable_actor.write.insert(remaining);
        assert!(matches!(
            check_update(&current, &rollback, &capable_actor),
            Err(ProtocolRejection::WriteRequirement { found: 1, .. })
        ));
        let full_rollback = with(&rollback, |next| {
            next.requirements.write.remove(remaining);
            next.requirements
                .features
                .insert(remaining.to_owned(), FeatureState::Compatible);
        });
        assert!(check_update(&current, &full_rollback, &capable_actor).unwrap());
    }

    #[test]
    fn unknown_required_capabilities_never_grant_write_authority() {
        let mut policy = VolumeProtocol::baseline();
        policy.requirements.write.insert(FUTURE.to_owned());
        policy
            .requirements
            .features
            .insert(FUTURE.to_owned(), FeatureState::Required);
        policy.validate().unwrap();
        let mut volume = VolumeRequirements::baseline();
        policy.apply_to(&mut volume);
        let local = BTreeMap::from([
            (
                LOCAL_METADATA_LAYOUT.name.to_owned(),
                LOCAL_METADATA_LAYOUT.current,
            ),
            (
                LOCAL_PAYLOAD_INDEX_FORMAT.name.to_owned(),
                LOCAL_PAYLOAD_INDEX_FORMAT.current,
            ),
        ]);
        let decision = evaluate(&ClientCapabilities::current(), &volume, &local);
        assert!(!decision.allows(Operation::Publication));
        assert!(decision.allows(Operation::Bootstrap));
    }

    fn make_session(
        active: bool,
        has_future: bool,
        write_requirement_max: u16,
    ) -> crate::client_sessions::ClientSession {
        use crate::client_sessions::{ClientSession, SessionCapabilities};
        let now = chrono::Utc::now();
        let mut read = vec![
            CAP_EVENT_CODEC_V1.to_owned(),
            CAP_PAYLOAD_STFS_V2.to_owned(),
            CAP_PAYLOAD_STFS_V3.to_owned(),
            CAP_PAYLOAD_SFEC_V1.to_owned(),
        ];
        if has_future {
            read.push(FUTURE.to_owned());
        }
        ClientSession {
            session_id: uuid::Uuid::new_v4(),
            build: "0.2.5".to_owned(),
            capabilities: SessionCapabilities {
                volume_format_min: 7,
                volume_format_max: 7,
                write_requirement_max,
                read,
                write: BASELINE_WRITE_CAPABILITIES
                    .iter()
                    .map(|s| (*s).to_owned())
                    .collect(),
                maintain: vec![CAP_MAINTENANCE_PAYLOAD_GC_V2.to_owned()],
            },
            policy_epoch: 1,
            fencing_token: 1,
            heartbeat_at: now,
            lease_expires_at: if active {
                now + chrono::Duration::seconds(60)
            } else {
                now - chrono::Duration::seconds(60)
            },
            active,
        }
    }

    fn ready_shared_state() -> CutoverSharedState {
        CutoverSharedState {
            remote_head_version: 10,
            checkpoint_version: 10,
            checkpoint_ready: true,
            available_checkpoints: vec![CheckpointDescriptor::BASELINE],
            snapshot_or_gc_active: false,
            active_maintenance_detail: None,
        }
    }

    #[test]
    fn dry_run_reports_active_and_expired_session_and_local_blockers() {
        let actor = successor_actor();
        let mut compatible = VolumeProtocol::baseline();
        compatible
            .requirements
            .features
            .insert(FUTURE.to_owned(), FeatureState::Compatible);

        let active_unsupported = make_session(true, false, 1);
        let active_supported = make_session(true, true, 2);
        let expired_unsupported = make_session(false, false, 1);

        let req = CutoverRequest {
            feature: FUTURE.to_owned(),
            phase: CutoverPhase::Activate,
            roles: BTreeSet::from([CapabilityRole::Read]),
            write_requirement: None,
            offline_client_policy: OfflineClientPolicy::BlockUnknownWork,
        };
        let local = CutoverLocalState {
            unpublished_events: 2,
            local_version: Some(8),
        };
        let mut shared = ready_shared_state();
        shared.snapshot_or_gc_active = true;
        shared.checkpoint_ready = false;

        let report = evaluate_cutover_readiness(
            &compatible,
            &req,
            &actor,
            &[
                active_unsupported.clone(),
                active_supported.clone(),
                expired_unsupported.clone(),
            ],
            &local,
            &shared,
        );
        assert!(!report.is_ready());
        assert_eq!(report.current_epoch, 1);
        assert_eq!(report.next_epoch, 2);
        assert_eq!(report.sessions.eligible_active.len(), 1);
        assert_eq!(report.sessions.blocking_active.len(), 1);
        assert_eq!(report.sessions.expired_unsupported.len(), 1);
        assert!(report.blockers.iter().any(|b| matches!(
            b,
            CutoverBlocker::ActiveUnsupportedSession { session_id, .. }
                if *session_id == active_unsupported.session_id
        )));
        assert!(report.blockers.iter().any(|b| matches!(
            b,
            CutoverBlocker::ExpiredUnsupportedSession { session_id, .. }
                if *session_id == expired_unsupported.session_id
        )));
        assert!(report.blockers.iter().any(|b| matches!(
            b,
            CutoverBlocker::DirtyUnpublishedLocalWork {
                unpublished_events: 2,
            }
        )));
        assert!(report.blockers.iter().any(|b| matches!(
            b,
            CutoverBlocker::ReplicationLag {
                local_version: 8,
                remote_head_version: 10
            }
        )));
        assert!(
            report
                .blockers
                .iter()
                .any(|b| matches!(b, CutoverBlocker::MissingCompatibleCheckpoint { .. }))
        );
        assert!(
            report
                .blockers
                .iter()
                .any(|b| matches!(b, CutoverBlocker::SnapshotOrGcInProgress { .. }))
        );

        // Accepting offline client fence clears only the expired-session blocker, not active ones.
        let mut accept_req = req;
        accept_req.offline_client_policy = OfflineClientPolicy::AcceptFence;
        let report_accepted = evaluate_cutover_readiness(
            &compatible,
            &accept_req,
            &actor,
            &[active_supported, expired_unsupported],
            &CutoverLocalState::default(),
            &ready_shared_state(),
        );
        assert!(report_accepted.is_ready(), "{:?}", report_accepted.blockers);
    }

    #[test]
    fn expand_and_migrate_keep_baseline_writable_and_deactivate_refused_after_retired() {
        let actor = successor_actor();
        let baseline = VolumeProtocol::baseline();
        let expand_req = CutoverRequest {
            feature: FUTURE.to_owned(),
            phase: CutoverPhase::Expand,
            roles: BTreeSet::new(),
            write_requirement: None,
            offline_client_policy: OfflineClientPolicy::BlockUnknownWork,
        };
        let expanded = build_proposed_protocol(&baseline, &expand_req).unwrap();
        assert_eq!(expanded.write_requirement, 1);
        assert!(check_update(&baseline, &expanded, &actor).unwrap());

        let migrate_req = CutoverRequest {
            phase: CutoverPhase::Migrate,
            ..expand_req.clone()
        };
        let migrated = build_proposed_protocol(&expanded, &migrate_req).unwrap();
        assert_eq!(migrated.write_requirement, 1);
        assert!(check_update(&expanded, &migrated, &actor).unwrap());

        // Baseline 0.2.5 client can still publish during Expand and Migrate.
        migrated
            .check_operations(
                VolumeRequirements::baseline(),
                Some(CheckpointDescriptor::BASELINE),
                &BTreeMap::new(),
                &[
                    Operation::Bootstrap,
                    Operation::Replay,
                    Operation::Publication,
                ],
            )
            .unwrap();

        let deactivate_req = CutoverRequest {
            phase: CutoverPhase::Deactivate,
            ..expand_req.clone()
        };
        let available = build_proposed_protocol(&migrated, &deactivate_req).unwrap();
        let contract_req = CutoverRequest {
            phase: CutoverPhase::Contract,
            ..expand_req
        };
        let retired = build_proposed_protocol(&available, &contract_req).unwrap();
        assert!(check_update(&available, &retired, &actor).unwrap());

        // Deactivation after Retired is refused.
        let report = evaluate_cutover_readiness(
            &retired,
            &deactivate_req,
            &actor,
            &[],
            &CutoverLocalState::default(),
            &ready_shared_state(),
        );
        assert!(!report.is_ready());
        assert!(
            report
                .blockers
                .iter()
                .any(|b| matches!(b, CutoverBlocker::InvalidTransition(_)))
        );
    }

    fn request(phase: CutoverPhase, roles: &[CapabilityRole]) -> CutoverRequest {
        CutoverRequest {
            feature: FUTURE.to_owned(),
            phase,
            roles: roles.iter().copied().collect(),
            write_requirement: None,
            offline_client_policy: OfflineClientPolicy::BlockUnknownWork,
        }
    }

    #[test]
    fn contract_requires_explicit_deactivation_to_available() {
        let contract = request(CutoverPhase::Contract, &[]);
        for state in [FeatureState::Compatible, FeatureState::Required] {
            let current = with(&VolumeProtocol::baseline(), set_state(state));
            let err = build_proposed_protocol(&current, &contract).unwrap_err();
            assert!(
                matches!(&err, ProtocolRejection::CutoverRefused(msg) if msg.contains("deactivate it to available")),
                "{err}"
            );
        }
        let available = with(
            &VolumeProtocol::baseline(),
            set_state(FeatureState::Available),
        );
        let retired = build_proposed_protocol(&available, &contract).unwrap();
        assert_eq!(
            retired.requirements.features.get(FUTURE),
            Some(&FeatureState::Retired)
        );
    }

    #[test]
    fn deactivate_rejects_roles() {
        let current = with(
            &VolumeProtocol::baseline(),
            set_state(FeatureState::Required),
        );
        let err = build_proposed_protocol(
            &current,
            &request(CutoverPhase::Deactivate, &[CapabilityRole::Read]),
        )
        .unwrap_err();
        assert!(matches!(err, ProtocolRejection::CutoverRefused(_)), "{err}");
    }

    #[test]
    fn activate_applies_explicit_write_requirement_when_write_already_required() {
        let actor = successor_actor();
        let mut req = request(CutoverPhase::Activate, &[CapabilityRole::Write]);
        let compatible = with(
            &VolumeProtocol::baseline(),
            set_state(FeatureState::Compatible),
        );
        let active = build_proposed_protocol(&compatible, &req).unwrap();
        assert_eq!(active.write_requirement, 2);
        req.write_requirement = Some(5);
        assert_eq!(
            build_proposed_protocol(&active, &req)
                .unwrap()
                .write_requirement,
            5
        );
        // A refused proposal yields no proposal, no epoch bump and no session
        // blockers measured against the refused policy.
        let report = evaluate_cutover_readiness(
            &compatible,
            &req,
            &actor,
            &[make_session(true, false, 1)],
            &CutoverLocalState::default(),
            &ready_shared_state(),
        );
        assert!(report.proposed.is_none());
        assert_eq!(report.next_epoch, report.current_epoch);
        assert!(
            report
                .blockers
                .iter()
                .all(|b| matches!(b, CutoverBlocker::InvalidTransition(_))),
            "{:?}",
            report.blockers
        );
    }
}

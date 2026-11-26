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

    /// Returns a refusal for a read or write operation against this policy.
    pub fn check_operations(
        &self,
        mut volume: VolumeRequirements,
        actual_checkpoint: Option<CheckpointDescriptor>,
        local: &BTreeMap<String, u16>,
        operations: &[crate::compat::Operation],
    ) -> crate::MetaResult<()> {
        use crate::{MetaError, UpgradeRequiredReason};
        self.validate()?;
        self.apply_to(&mut volume);
        volume.checkpoint = actual_checkpoint;
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
        let decision = crate::compat::evaluate(&ClientCapabilities::current(), &volume, local);
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
}

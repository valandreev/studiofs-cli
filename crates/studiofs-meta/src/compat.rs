//! Central capability evaluator (ADR 0018, SFS-1004).
//!
//! [`evaluate`](crate::compat::evaluate) is a pure function of the binary's
//! declared [`ClientCapabilities`](crate::compat::ClientCapabilities), the
//! volume's [`VolumeRequirements`](crate::compat::VolumeRequirements) and the
//! identities found in the local stores. It performs no I/O, so callers can
//! run it on a cached policy without a backend round trip. The inputs hold no
//! PostgreSQL types; the 0.2.7 Redis-compatible backend (SFS-1034) reuses them.
//!
//! The evaluator is not wired into mount, replay or publication yet: those
//! paths keep their existing exact-format checks (fail closed) until policy
//! enforcement (SFS-1015) lands. The volume policy (SFS-1007) feeds it through
//! [`VolumeProtocol::apply_to`](crate::volume_protocol::VolumeProtocol::apply_to).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;

use crate::version::{
    LOCAL_METADATA_LAYOUT, POSTGRES_PAYLOAD_GC_SCHEMA, SERVING_CHECKPOINT_FORMAT,
    SERVING_CHECKPOINT_SCHEMA, SHARED_VOLUME_FORMAT, VersionIdentity,
};

/// Shared volume format of the 0.2.5 baseline, written by `studiofs upgrade`
/// (SFS-1132). From this format on the number stays fixed; later changes move
/// capabilities and identities instead.
pub const BASELINE_VOLUME_FORMAT: u16 = 7;

const _: () = assert!(
    BASELINE_VOLUME_FORMAT == SHARED_VOLUME_FORMAT.current + 1,
    "SFS-1132: reconcile the evaluator baseline with SHARED_VOLUME_FORMAT"
);

/// Shared formats of 0.2.0–0.2.4 volumes that `studiofs upgrade` adopts.
pub const MIGRATABLE_VOLUME_FORMATS: RangeInclusive<u16> = 5..=6;

/// Event wire envelope `SFS-EVT` version 1 with the baseline tag set.
pub const CAP_EVENT_CODEC_V1: &str = "event.codec.v1";
/// STFS payload object version 2 (`studiofs_data::object::VERSION`).
pub const CAP_PAYLOAD_STFS_V2: &str = "payload.stfs.v2";
/// STFS payload object version 3 (`studiofs_data::object::VERSION_V3`).
pub const CAP_PAYLOAD_STFS_V3: &str = "payload.stfs.v3";
/// SFEC envelope version 1 (`studiofs_data::envelope::SFEC_VERSION`).
pub const CAP_PAYLOAD_SFEC_V1: &str = "payload.sfec.v1";
/// Payload GC and Janitor deletion protocol of `payload_gc` schema 2.
pub const CAP_MAINTENANCE_PAYLOAD_GC_V2: &str = "maintenance.payload_gc.v2";

/// Capability namespaces that are never ignorable: data in them changes what
/// a reader serves, so an unknown name is refused even when declared optional.
const SAFETY_CRITICAL_NAMESPACES: &[&str] = &["event.", "payload.", "checkpoint."];

/// Local payload index format (`studiofs-cache` `INDEX_FORMAT_VERSION`; format 1
/// is migrated in place on open). `studiofs-vfs` tests pin it to the cache
/// constants. Entry encodings are per record, not a store identity.
pub const LOCAL_PAYLOAD_INDEX_FORMAT: VersionIdentity = VersionIdentity {
    name: "local payload index format",
    current: 2,
    min_supported: 1,
};

/// What a binary can read, write and maintain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCapabilities {
    /// Shared volume formats this binary serves.
    pub volume_format: VersionIdentity,
    /// Capabilities this binary can read and replay.
    pub read: BTreeSet<&'static str>,
    /// Capabilities this binary can publish.
    pub write: BTreeSet<&'static str>,
    /// Capabilities this binary can delete or prune data for.
    pub maintain: BTreeSet<&'static str>,
    /// Shared backend schema components this binary needs, keyed by `name`.
    pub schema_components: Vec<VersionIdentity>,
    /// Serving checkpoint byte formats this binary decodes.
    pub checkpoint_format: VersionIdentity,
    /// Serving checkpoint state schemas this binary decodes.
    pub checkpoint_schema: VersionIdentity,
    /// Local store identities this binary opens, keyed by `name`.
    pub local_formats: Vec<VersionIdentity>,
}

impl ClientCapabilities {
    /// Capabilities of this binary once `studiofs upgrade` writes format
    /// [`BASELINE_VOLUME_FORMAT`] (SFS-1132). Until then the shipped binary
    /// still serves [`SHARED_VOLUME_FORMAT`], so this must not be wired into
    /// mount (SFS-1015) before SFS-1132 lands.
    ///
    /// Requiring [`POSTGRES_PAYLOAD_GC_SCHEMA`] is PostgreSQL-specific; SFS-1034
    /// parameterises the schema components by metadata backend.
    #[must_use]
    pub fn current() -> Self {
        Self {
            volume_format: VersionIdentity {
                name: SHARED_VOLUME_FORMAT.name,
                current: BASELINE_VOLUME_FORMAT,
                min_supported: BASELINE_VOLUME_FORMAT,
            },
            read: BTreeSet::from([
                CAP_EVENT_CODEC_V1,
                CAP_PAYLOAD_STFS_V2,
                CAP_PAYLOAD_STFS_V3,
                CAP_PAYLOAD_SFEC_V1,
            ]),
            write: BTreeSet::from([
                CAP_EVENT_CODEC_V1,
                CAP_PAYLOAD_STFS_V2,
                CAP_PAYLOAD_STFS_V3,
                CAP_PAYLOAD_SFEC_V1,
            ]),
            maintain: BTreeSet::from([CAP_MAINTENANCE_PAYLOAD_GC_V2]),
            schema_components: vec![POSTGRES_PAYLOAD_GC_SCHEMA],
            checkpoint_format: SERVING_CHECKPOINT_FORMAT,
            checkpoint_schema: SERVING_CHECKPOINT_SCHEMA,
            local_formats: vec![LOCAL_METADATA_LAYOUT, LOCAL_PAYLOAD_INDEX_FORMAT],
        }
    }
}

/// Latest serving checkpoint descriptor of a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointDescriptor {
    /// Serving checkpoint byte format.
    pub format: u16,
    /// Serving checkpoint state schema.
    pub schema: u16,
}

/// What a volume requires, read from its policy record and shared backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeRequirements {
    /// Shared volume format number.
    pub format: u16,
    /// A binary missing any of these must not mount or replay.
    pub read: BTreeSet<String>,
    /// A binary missing any of these must not publish.
    pub write: BTreeSet<String>,
    /// A binary missing any of these must not delete, prune or run GC.
    pub maintain: BTreeSet<String>,
    /// Capabilities in use that a reader may ignore. Unknown names in a
    /// safety-critical namespace (`event.`, `payload.`, `checkpoint.`) are
    /// still refused.
    pub optional: BTreeSet<String>,
    /// Schema components present in the shared backend, name to version.
    pub schema_components: BTreeMap<String, u16>,
    /// Latest serving checkpoint, if one exists.
    pub checkpoint: Option<CheckpointDescriptor>,
}

impl VolumeRequirements {
    /// Requirements of a volume adopted into the 0.2.5 baseline.
    #[must_use]
    pub fn baseline() -> Self {
        let set = |names: &[&str]| names.iter().map(|name| (*name).to_owned()).collect();
        Self {
            format: BASELINE_VOLUME_FORMAT,
            read: set(&[
                CAP_EVENT_CODEC_V1,
                CAP_PAYLOAD_STFS_V2,
                CAP_PAYLOAD_STFS_V3,
                CAP_PAYLOAD_SFEC_V1,
            ]),
            write: set(&[CAP_EVENT_CODEC_V1, CAP_PAYLOAD_STFS_V3, CAP_PAYLOAD_SFEC_V1]),
            maintain: set(&[CAP_MAINTENANCE_PAYLOAD_GC_V2]),
            optional: BTreeSet::new(),
            schema_components: BTreeMap::from([(
                POSTGRES_PAYLOAD_GC_SCHEMA.name.to_owned(),
                POSTGRES_PAYLOAD_GC_SCHEMA.current,
            )]),
            checkpoint: Some(CheckpointDescriptor {
                format: SERVING_CHECKPOINT_FORMAT.current,
                schema: SERVING_CHECKPOINT_SCHEMA.current,
            }),
        }
    }
}

/// Operation the evaluator decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Operation {
    /// Bootstrap from the shared backend and serve reads.
    Bootstrap,
    /// Replay shared events into the local view.
    Replay,
    /// Publish local mutations to the shared backend.
    Publication,
    /// Read or publish a serving checkpoint.
    Checkpoint,
    /// Delete or prune shared data (GC, Janitor, retention).
    Maintenance,
}

impl Operation {
    /// Every operation, in report order.
    pub const ALL: [Self; 5] = [
        Self::Bootstrap,
        Self::Replay,
        Self::Publication,
        Self::Checkpoint,
        Self::Maintenance,
    ];

    /// Stable machine-readable name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Replay => "replay",
            Self::Publication => "publication",
            Self::Checkpoint => "checkpoint",
            Self::Maintenance => "maintenance",
        }
    }
}

/// Overall compatibility, ordered from most to least permissive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Compatibility {
    /// Mount, replay and publish.
    CompatibleRw,
    /// Mount and replay; publication is refused.
    CompatibleRo,
    /// The volume or a local store must be migrated first.
    NeedsMigration,
    /// This binary is too old for the volume or a local store.
    UpgradeRequired,
    /// No supported path exists.
    Incompatible,
}

impl Compatibility {
    /// Stable machine-readable name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CompatibleRw => "compatible_rw",
            Self::CompatibleRo => "compatible_ro",
            Self::NeedsMigration => "needs_migration",
            Self::UpgradeRequired => "upgrade_required",
            Self::Incompatible => "incompatible",
        }
    }
}

/// Stable reason for a refusal. Each reason has one [`Compatibility`] class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReasonCode {
    /// 0.2.0–0.2.4 volume format.
    VolumeFormatNeedsMigration,
    /// Volume format newer than this binary.
    VolumeFormatNewer,
    /// Volume format older than any supported release.
    VolumeFormatUnsupported,
    /// Local store identity this binary does not know.
    LocalFormatUnknown,
    /// Local store written by a newer binary.
    LocalFormatNewer,
    /// Local store older than this binary opens.
    LocalFormatOutdated,
    /// Shared schema component this binary needs is absent.
    SchemaComponentMissing,
    /// Shared schema component older than this binary needs.
    SchemaComponentOutdated,
    /// Shared schema component newer than this binary maintains.
    SchemaComponentNewer,
    /// Shared schema component this binary does not know.
    SchemaComponentUnknown,
    /// Event kind or codec this binary cannot decode.
    EventKindUnknown,
    /// Payload object format this binary cannot read.
    PayloadFormatUnsupported,
    /// Other read capability this binary lacks.
    ReadCapabilityMissing,
    /// Serving checkpoint format or schema newer than this binary.
    CheckpointFormatNewer,
    /// Serving checkpoint format or schema older than any supported release.
    CheckpointFormatUnsupported,
    /// Write capability this binary lacks.
    WriteCapabilityMissing,
    /// Maintenance capability this binary lacks.
    MaintenanceCapabilityMissing,
}

impl ReasonCode {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VolumeFormatNeedsMigration => "volume_format_needs_migration",
            Self::VolumeFormatNewer => "volume_format_newer",
            Self::VolumeFormatUnsupported => "volume_format_unsupported",
            Self::LocalFormatUnknown => "local_format_unknown",
            Self::LocalFormatNewer => "local_format_newer",
            Self::LocalFormatOutdated => "local_format_outdated",
            Self::SchemaComponentMissing => "schema_component_missing",
            Self::SchemaComponentOutdated => "schema_component_outdated",
            Self::SchemaComponentNewer => "schema_component_newer",
            Self::SchemaComponentUnknown => "schema_component_unknown",
            Self::EventKindUnknown => "event_kind_unknown",
            Self::PayloadFormatUnsupported => "payload_format_unsupported",
            Self::ReadCapabilityMissing => "read_capability_missing",
            Self::CheckpointFormatNewer => "checkpoint_format_newer",
            Self::CheckpointFormatUnsupported => "checkpoint_format_unsupported",
            Self::WriteCapabilityMissing => "write_capability_missing",
            Self::MaintenanceCapabilityMissing => "maintenance_capability_missing",
        }
    }

    /// Compatibility class of this reason.
    #[must_use]
    pub const fn class(self) -> Compatibility {
        match self {
            Self::VolumeFormatNeedsMigration
            | Self::LocalFormatOutdated
            | Self::SchemaComponentMissing
            | Self::SchemaComponentOutdated => Compatibility::NeedsMigration,
            Self::VolumeFormatUnsupported | Self::CheckpointFormatUnsupported => {
                Compatibility::Incompatible
            }
            Self::VolumeFormatNewer
            | Self::LocalFormatUnknown
            | Self::LocalFormatNewer
            | Self::SchemaComponentNewer
            | Self::SchemaComponentUnknown
            | Self::EventKindUnknown
            | Self::PayloadFormatUnsupported
            | Self::ReadCapabilityMissing
            | Self::CheckpointFormatNewer
            | Self::WriteCapabilityMissing
            | Self::MaintenanceCapabilityMissing => Compatibility::UpgradeRequired,
        }
    }

    /// What the operator should do.
    #[must_use]
    pub const fn action(self) -> &'static str {
        match self {
            Self::VolumeFormatNeedsMigration => {
                "run `studiofs upgrade` to adopt the volume into the 0.2.5 baseline"
            }
            Self::SchemaComponentMissing | Self::SchemaComponentOutdated => {
                "run `studiofs upgrade` to migrate the shared metadata schema"
            }
            Self::LocalFormatOutdated => {
                "run `studiofs upgrade` to migrate the local store before mounting"
            }
            Self::VolumeFormatUnsupported => {
                "the volume predates StudioFS 0.2.0 and is not supported; restore it with the release that wrote it"
            }
            Self::CheckpointFormatUnsupported => {
                "the serving checkpoint predates any supported StudioFS release; restore it with the release that wrote it"
            }
            Self::VolumeFormatNewer
            | Self::LocalFormatUnknown
            | Self::LocalFormatNewer
            | Self::SchemaComponentNewer
            | Self::SchemaComponentUnknown
            | Self::EventKindUnknown
            | Self::PayloadFormatUnsupported
            | Self::ReadCapabilityMissing
            | Self::CheckpointFormatNewer
            | Self::WriteCapabilityMissing
            | Self::MaintenanceCapabilityMissing => {
                "upgrade StudioFS to a release that supports this volume"
            }
        }
    }
}

/// One typed refusal: a stable reason plus the capability or identity it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// Stable reason code.
    pub reason: ReasonCode,
    /// Capability name or `identity version` the reason is about.
    pub subject: String,
}

/// Decision for one operation; `refusal` is `None` when it is allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAssessment {
    /// Assessed operation.
    pub operation: Operation,
    /// Worst-class refusal, if any; within a class the first in check order.
    pub refusal: Option<Refusal>,
}

/// Result of [`evaluate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityDecision {
    /// Overall class, driven by bootstrap, replay and publication.
    pub compatibility: Compatibility,
    /// Refusal that set `compatibility`; `None` for [`Compatibility::CompatibleRw`].
    pub reason: Option<Refusal>,
    /// One assessment per [`Operation::ALL`] entry, in that order.
    pub operations: [OperationAssessment; 5],
}

impl CompatibilityDecision {
    /// Whether `operation` is allowed.
    #[must_use]
    pub fn allows(&self, operation: Operation) -> bool {
        self.refusal(operation).is_none()
    }

    /// Refusal of `operation`, if any.
    #[must_use]
    pub fn refusal(&self, operation: Operation) -> Option<&Refusal> {
        self.operations
            .iter()
            .find(|assessment| assessment.operation == operation)
            .and_then(|assessment| assessment.refusal.as_ref())
    }
}

/// Decides every operation for `client` on `volume`. `local` maps the local
/// store identities found on disk (by `VersionIdentity::name`) to their
/// versions; it is empty for a fresh cache.
#[must_use]
pub fn evaluate(
    client: &ClientCapabilities,
    volume: &VolumeRequirements,
    local: &BTreeMap<String, u16>,
) -> CompatibilityDecision {
    use Operation::{Bootstrap, Checkpoint, Maintenance, Publication};
    const ALL: &[Operation] = &Operation::ALL;

    let mut operations = Operation::ALL.map(|operation| OperationAssessment {
        operation,
        refusal: None,
    });
    // Checks run in a fixed order; each operation keeps its worst-class
    // refusal, and the first one on a tie.
    let mut refuse = |targets: &[Operation], reason: ReasonCode, subject: String| {
        for assessment in &mut operations {
            let worse = assessment
                .refusal
                .as_ref()
                .is_none_or(|refusal| reason.class() > refusal.reason.class());
            if worse && targets.contains(&assessment.operation) {
                assessment.refusal = Some(Refusal {
                    reason,
                    subject: subject.clone(),
                });
            }
        }
    };

    let format = volume.format;
    if !client.volume_format.supports(format) {
        let reason = if MIGRATABLE_VOLUME_FORMATS.contains(&format) {
            ReasonCode::VolumeFormatNeedsMigration
        } else if format > client.volume_format.current {
            ReasonCode::VolumeFormatNewer
        } else {
            ReasonCode::VolumeFormatUnsupported
        };
        refuse(
            ALL,
            reason,
            format!("{} {format}", client.volume_format.name),
        );
    }

    for (name, &found) in local {
        let reason = match client.local_formats.iter().find(|id| id.name == name) {
            None => Some(ReasonCode::LocalFormatUnknown),
            Some(id) if found > id.current => Some(ReasonCode::LocalFormatNewer),
            Some(id) if found < id.min_supported => Some(ReasonCode::LocalFormatOutdated),
            Some(_) => None,
        };
        if let Some(reason) = reason {
            refuse(ALL, reason, format!("{name} {found}"));
        }
    }

    for id in &client.schema_components {
        let found = volume.schema_components.get(id.name).copied();
        let (targets, reason): (&[Operation], _) = match found {
            None => (ALL, ReasonCode::SchemaComponentMissing),
            Some(found) if found < id.min_supported => (ALL, ReasonCode::SchemaComponentOutdated),
            // A newer component is additive for readers and writers, but this
            // binary must not delete under rules it does not know.
            Some(found) if found > id.current => (&[Maintenance], ReasonCode::SchemaComponentNewer),
            Some(_) => continue,
        };
        let subject = found.map_or_else(
            || id.name.to_owned(),
            |found| format!("{} {found}", id.name),
        );
        refuse(targets, reason, subject);
    }
    // This binary must not delete under a component's rules it does not know.
    for (name, found) in &volume.schema_components {
        if !client.schema_components.iter().any(|id| id.name == name) {
            refuse(
                &[Maintenance],
                ReasonCode::SchemaComponentUnknown,
                format!("{name} {found}"),
            );
        }
    }

    // Unknown safety-critical names are refused wherever the volume declares them.
    let safety_critical = |name: &&String| {
        SAFETY_CRITICAL_NAMESPACES
            .iter()
            .any(|namespace| name.starts_with(namespace))
    };
    let read_set: BTreeSet<&String> = volume
        .read
        .iter()
        .chain(
            volume
                .optional
                .iter()
                .chain(&volume.write)
                .chain(&volume.maintain)
                .filter(safety_critical),
        )
        .collect();
    for name in read_set {
        if client.read.contains(name.as_str()) {
            continue;
        }
        let reason = if name.starts_with("event.") {
            ReasonCode::EventKindUnknown
        } else if name.starts_with("payload.") {
            ReasonCode::PayloadFormatUnsupported
        } else {
            ReasonCode::ReadCapabilityMissing
        };
        refuse(ALL, reason, name.clone());
    }

    if let Some(checkpoint) = volume.checkpoint {
        for (id, found) in [
            (client.checkpoint_format, checkpoint.format),
            (client.checkpoint_schema, checkpoint.schema),
        ] {
            if !id.supports(found) {
                let reason = if found > id.current {
                    ReasonCode::CheckpointFormatNewer
                } else {
                    ReasonCode::CheckpointFormatUnsupported
                };
                refuse(
                    &[Bootstrap, Checkpoint],
                    reason,
                    format!("{} {found}", id.name),
                );
            }
        }
    }

    // Write authority comes only from declared write capabilities, never from
    // a version range.
    for name in &volume.write {
        if !client.write.contains(name.as_str()) {
            refuse(
                &[Publication, Checkpoint],
                ReasonCode::WriteCapabilityMissing,
                name.clone(),
            );
        }
    }
    for name in &volume.maintain {
        if !client.maintain.contains(name.as_str()) {
            refuse(
                &[Maintenance],
                ReasonCode::MaintenanceCapabilityMissing,
                name.clone(),
            );
        }
    }

    // Bootstrap and replay decide whether the volume mounts at all; the worst
    // of their refusals wins. Publication alone only downgrades to read-only.
    let refusal_of = |operation: Operation| {
        operations
            .iter()
            .find(|assessment| assessment.operation == operation)
            .and_then(|assessment| assessment.refusal.as_ref())
    };
    let read_refusal = [Operation::Bootstrap, Operation::Replay]
        .into_iter()
        .filter_map(refusal_of)
        .max_by_key(|refusal| (refusal.reason.class(), std::cmp::Reverse(refusal.reason)));
    let (compatibility, reason) = match (read_refusal, refusal_of(Publication)) {
        (Some(refusal), _) => (refusal.reason.class(), Some(refusal.clone())),
        (None, Some(refusal)) => (Compatibility::CompatibleRo, Some(refusal.clone())),
        (None, None) => (Compatibility::CompatibleRw, None),
    };
    CompatibilityDecision {
        compatibility,
        reason,
        operations,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline_local() -> BTreeMap<String, u16> {
        BTreeMap::from([
            (
                LOCAL_METADATA_LAYOUT.name.to_owned(),
                LOCAL_METADATA_LAYOUT.current,
            ),
            (LOCAL_PAYLOAD_INDEX_FORMAT.name.to_owned(), 2),
        ])
    }

    fn eval(volume: &VolumeRequirements) -> CompatibilityDecision {
        evaluate(&ClientCapabilities::current(), volume, &baseline_local())
    }

    fn reason(decision: &CompatibilityDecision, operation: Operation) -> Option<&'static str> {
        decision
            .refusal(operation)
            .map(|refusal| refusal.reason.as_str())
    }

    fn overall(decision: &CompatibilityDecision) -> (&'static str, Option<&'static str>) {
        (
            decision.compatibility.as_str(),
            decision
                .reason
                .as_ref()
                .map(|refusal| refusal.reason.as_str()),
        )
    }

    #[test]
    fn compat_baseline_volume_is_read_write_for_every_operation() {
        let decision = eval(&VolumeRequirements::baseline());
        assert_eq!(overall(&decision), ("compatible_rw", None));
        for operation in Operation::ALL {
            assert!(decision.allows(operation), "{}", operation.as_str());
        }
        // A fresh cache has no local identities yet.
        let fresh = evaluate(
            &ClientCapabilities::current(),
            &VolumeRequirements::baseline(),
            &BTreeMap::new(),
        );
        assert_eq!(fresh.compatibility, Compatibility::CompatibleRw);
    }

    #[test]
    fn compat_0_2_x_volume_formats_need_studiofs_upgrade() {
        for format in [5, 6] {
            let decision = eval(&VolumeRequirements {
                format,
                ..VolumeRequirements::baseline()
            });
            assert_eq!(
                overall(&decision),
                ("needs_migration", Some("volume_format_needs_migration"))
            );
            assert!(Operation::ALL.iter().all(|op| !decision.allows(*op)));
            let refusal = decision.reason.unwrap();
            assert!(refusal.reason.action().contains("studiofs upgrade"));
            assert_eq!(refusal.subject, format!("shared volume format {format}"));
        }
    }

    #[test]
    fn compat_volume_format_outside_supported_range_is_refused() {
        let newer = eval(&VolumeRequirements {
            format: 8,
            ..VolumeRequirements::baseline()
        });
        assert_eq!(
            overall(&newer),
            ("upgrade_required", Some("volume_format_newer"))
        );
        let ancient = eval(&VolumeRequirements {
            format: 4,
            ..VolumeRequirements::baseline()
        });
        assert_eq!(
            overall(&ancient),
            ("incompatible", Some("volume_format_unsupported"))
        );
    }

    #[test]
    fn compat_unknown_required_read_capability_blocks_every_operation() {
        let mut volume = VolumeRequirements::baseline();
        volume.read.insert("namespace.future_feature.v1".to_owned());
        let decision = eval(&volume);
        assert_eq!(
            overall(&decision),
            ("upgrade_required", Some("read_capability_missing"))
        );
        assert!(Operation::ALL.iter().all(|op| !decision.allows(*op)));
        assert_eq!(
            decision.reason.unwrap().subject,
            "namespace.future_feature.v1"
        );
    }

    #[test]
    fn compat_unknown_optional_capability_is_ignored_unless_safety_critical() {
        let mut volume = VolumeRequirements::baseline();
        volume
            .optional
            .insert("table.successor_stats.v1".to_owned());
        assert_eq!(eval(&volume).compatibility, Compatibility::CompatibleRw);

        // An unknown event is never ignorable, even when declared optional.
        volume
            .optional
            .insert("event.volume_settings.v4".to_owned());
        let decision = eval(&volume);
        assert_eq!(
            overall(&decision),
            ("upgrade_required", Some("event_kind_unknown"))
        );
        assert!(!decision.allows(Operation::Replay));
    }

    #[test]
    fn compat_unknown_event_codec_or_kind_is_refused_before_replay() {
        for name in ["event.codec.v2", "event.volume_settings.v4"] {
            let mut volume = VolumeRequirements::baseline();
            volume.read.insert(name.to_owned());
            let decision = eval(&volume);
            assert_eq!(
                reason(&decision, Operation::Replay),
                Some("event_kind_unknown")
            );
            assert_eq!(decision.compatibility, Compatibility::UpgradeRequired);
        }
    }

    #[test]
    fn compat_unsupported_payload_format_is_refused_before_reading() {
        let mut volume = VolumeRequirements::baseline();
        volume.read.insert("payload.stfs.v4".to_owned());
        let decision = eval(&volume);
        assert_eq!(
            overall(&decision),
            ("upgrade_required", Some("payload_format_unsupported"))
        );
        assert_eq!(
            reason(&decision, Operation::Bootstrap),
            Some("payload_format_unsupported")
        );
    }

    #[test]
    fn compat_missing_or_outdated_schema_component_needs_migration() {
        let mut volume = VolumeRequirements::baseline();
        volume.schema_components.clear();
        let decision = eval(&volume);
        assert_eq!(
            overall(&decision),
            ("needs_migration", Some("schema_component_missing"))
        );
        assert!(Operation::ALL.iter().all(|op| !decision.allows(*op)));

        volume
            .schema_components
            .insert(POSTGRES_PAYLOAD_GC_SCHEMA.name.to_owned(), 1);
        assert_eq!(
            overall(&eval(&volume)),
            ("needs_migration", Some("schema_component_outdated"))
        );
    }

    #[test]
    fn compat_missing_pg_column_maps_to_outdated_schema_component() {
        // SFS-1006 schema validation reports a component whose required
        // column is missing as below `min_supported`; the evaluator sees only
        // that version, never the column.
        let mut volume = VolumeRequirements::baseline();
        volume.schema_components.insert(
            POSTGRES_PAYLOAD_GC_SCHEMA.name.to_owned(),
            POSTGRES_PAYLOAD_GC_SCHEMA.min_supported - 1,
        );
        let decision = eval(&volume);
        assert_eq!(
            overall(&decision),
            ("needs_migration", Some("schema_component_outdated"))
        );
        assert!(Operation::ALL.iter().all(|op| !decision.allows(*op)));
        assert!(
            decision
                .reason
                .unwrap()
                .reason
                .action()
                .contains("studiofs upgrade")
        );
    }

    #[test]
    fn compat_newer_schema_component_only_forbids_maintenance() {
        let mut volume = VolumeRequirements::baseline();
        volume
            .schema_components
            .insert(POSTGRES_PAYLOAD_GC_SCHEMA.name.to_owned(), 3);
        let decision = eval(&volume);
        assert_eq!(decision.compatibility, Compatibility::CompatibleRw);
        assert_eq!(
            reason(&decision, Operation::Maintenance),
            Some("schema_component_newer")
        );
    }

    #[test]
    fn compat_extra_schema_component_only_forbids_maintenance() {
        let mut volume = VolumeRequirements::baseline();
        volume
            .schema_components
            .insert("successor_stats".to_owned(), 1);
        let decision = eval(&volume);
        assert_eq!(decision.compatibility, Compatibility::CompatibleRw);
        assert_eq!(
            reason(&decision, Operation::Maintenance),
            Some("schema_component_unknown")
        );
        assert_eq!(
            decision.refusal(Operation::Maintenance).unwrap().subject,
            "successor_stats 1"
        );
    }

    #[test]
    fn compat_worse_class_wins_over_earlier_migration_refusal() {
        let mut volume = VolumeRequirements::baseline();
        volume.read.insert("event.volume_settings.v4".to_owned());
        let expected = ("upgrade_required", Some("event_kind_unknown"));

        // Outdated local redb layout is checked before capabilities.
        let mut local = baseline_local();
        local.insert(LOCAL_METADATA_LAYOUT.name.to_owned(), 5);
        let decision = evaluate(&ClientCapabilities::current(), &volume, &local);
        assert_eq!(overall(&decision), expected);
        assert_eq!(
            reason(&decision, Operation::Replay),
            Some("event_kind_unknown")
        );

        // Missing schema component is checked before capabilities too.
        volume.schema_components.clear();
        let decision = eval(&volume);
        assert_eq!(overall(&decision), expected);
        assert_eq!(
            reason(&decision, Operation::Maintenance),
            Some("event_kind_unknown")
        );

        // Within one class the first refusal in check order still wins.
        volume.schema_components = VolumeRequirements::baseline().schema_components;
        volume.read.insert("aa.future.v1".to_owned());
        assert_eq!(
            reason(&eval(&volume), Operation::Replay),
            Some("read_capability_missing")
        );
    }

    #[test]
    fn compat_checkpoint_reader_mismatch_refuses_bootstrap_and_checkpoint_only() {
        let mut volume = VolumeRequirements::baseline();
        volume.checkpoint = Some(CheckpointDescriptor {
            format: 5,
            schema: 1,
        });
        let decision = eval(&volume);
        assert_eq!(
            overall(&decision),
            ("upgrade_required", Some("checkpoint_format_newer"))
        );
        assert_eq!(
            reason(&decision, Operation::Checkpoint),
            Some("checkpoint_format_newer")
        );
        assert!(decision.allows(Operation::Replay));
        assert!(decision.allows(Operation::Publication));

        volume.checkpoint = Some(CheckpointDescriptor {
            format: 4,
            schema: 2,
        });
        assert_eq!(
            reason(&eval(&volume), Operation::Checkpoint),
            Some("checkpoint_format_newer")
        );

        volume.checkpoint = Some(CheckpointDescriptor {
            format: 0,
            schema: 1,
        });
        assert_eq!(
            reason(&eval(&volume), Operation::Checkpoint),
            Some("checkpoint_format_unsupported")
        );

        volume.checkpoint = None;
        assert_eq!(eval(&volume).compatibility, Compatibility::CompatibleRw);
    }

    #[test]
    fn compat_unknown_write_capability_is_read_only() {
        let mut volume = VolumeRequirements::baseline();
        volume.write.insert("table.successor_stats.v1".to_owned());
        let decision = eval(&volume);
        assert_eq!(
            overall(&decision),
            ("compatible_ro", Some("write_capability_missing"))
        );
        assert!(decision.allows(Operation::Bootstrap));
        assert!(decision.allows(Operation::Replay));
        assert!(!decision.allows(Operation::Publication));
        assert!(!decision.allows(Operation::Checkpoint));

        // An unknown event in any declared set is never ignorable for a reader.
        volume.write.insert("event.volume_settings.v4".to_owned());
        assert_eq!(
            overall(&eval(&volume)),
            ("upgrade_required", Some("event_kind_unknown"))
        );
    }

    #[test]
    fn compat_read_capability_never_grants_write_authority() {
        // The binary reads the capability (its version is "in range"), but
        // never declared it can write it.
        let mut client = ClientCapabilities::current();
        client.read.insert("payload.stfs.v4");
        let mut volume = VolumeRequirements::baseline();
        volume.read.insert("payload.stfs.v4".to_owned());
        volume.write.insert("payload.stfs.v4".to_owned());
        let decision = evaluate(&client, &volume, &baseline_local());
        assert_eq!(
            overall(&decision),
            ("compatible_ro", Some("write_capability_missing"))
        );

        client.write.insert("payload.stfs.v4");
        let decision = evaluate(&client, &volume, &baseline_local());
        assert_eq!(decision.compatibility, Compatibility::CompatibleRw);
    }

    #[test]
    fn compat_unknown_maintenance_capability_forbids_only_maintenance() {
        let mut volume = VolumeRequirements::baseline();
        volume
            .maintain
            .insert("maintenance.payload_gc.v3".to_owned());
        let decision = eval(&volume);
        assert_eq!(decision.compatibility, Compatibility::CompatibleRw);
        assert_eq!(
            reason(&decision, Operation::Maintenance),
            Some("maintenance_capability_missing")
        );
    }

    #[test]
    fn compat_local_store_identities_are_range_checked() {
        let volume = VolumeRequirements::baseline();
        let client = ClientCapabilities::current();
        let mut local = baseline_local();
        local.insert(LOCAL_PAYLOAD_INDEX_FORMAT.name.to_owned(), 3);
        let decision = evaluate(&client, &volume, &local);
        assert_eq!(
            overall(&decision),
            ("upgrade_required", Some("local_format_newer"))
        );

        let mut local = baseline_local();
        local.insert(LOCAL_METADATA_LAYOUT.name.to_owned(), 5);
        assert_eq!(
            overall(&evaluate(&client, &volume, &local)),
            ("needs_migration", Some("local_format_outdated"))
        );

        let mut local = baseline_local();
        local.insert("local successor store".to_owned(), 1);
        assert_eq!(
            overall(&evaluate(&client, &volume, &local)),
            ("upgrade_required", Some("local_format_unknown"))
        );
    }

    #[test]
    fn compat_successor_client_reads_baseline_and_activated_successor_volume() {
        // Representative 0.2.7-like client: adds an event kind it reads and writes.
        let mut successor = ClientCapabilities::current();
        successor.read.insert("event.volume_settings.v4");
        successor.write.insert("event.volume_settings.v4");
        let baseline = VolumeRequirements::baseline();
        assert_eq!(
            evaluate(&successor, &baseline, &baseline_local()).compatibility,
            Compatibility::CompatibleRw
        );

        // Activated as read-compatible: the baseline binary stays read-only.
        let mut activated = baseline;
        successor.write.insert("table.successor_stats.v1");
        activated
            .write
            .insert("table.successor_stats.v1".to_owned());
        assert_eq!(
            evaluate(&successor, &activated, &baseline_local()).compatibility,
            Compatibility::CompatibleRw
        );
        assert_eq!(eval(&activated).compatibility, Compatibility::CompatibleRo);

        // Activated as read-incompatible: the baseline binary must upgrade.
        activated.read.insert("event.volume_settings.v4".to_owned());
        assert_eq!(
            eval(&activated).compatibility,
            Compatibility::UpgradeRequired
        );
    }

    #[test]
    fn compat_evaluation_is_deterministic_and_ordered() {
        let mut volume = VolumeRequirements::baseline();
        volume.read.insert("zz.future.v1".to_owned());
        volume.read.insert("aa.future.v1".to_owned());
        volume.format = 6;
        let first = eval(&volume);
        assert_eq!(first, eval(&volume));
        // A worse class outranks the earlier migration refusal; within a
        // class the first name in sorted order wins.
        assert_eq!(
            overall(&first),
            ("upgrade_required", Some("read_capability_missing"))
        );
        assert_eq!(first.reason.as_ref().unwrap().subject, "aa.future.v1");
        let ops: Vec<_> = first.operations.iter().map(|a| a.operation).collect();
        assert_eq!(ops, Operation::ALL);
    }

    #[test]
    fn compat_stable_strings_and_actions() {
        let expected = [
            (
                ReasonCode::VolumeFormatNeedsMigration,
                "volume_format_needs_migration",
                "needs_migration",
            ),
            (
                ReasonCode::VolumeFormatNewer,
                "volume_format_newer",
                "upgrade_required",
            ),
            (
                ReasonCode::VolumeFormatUnsupported,
                "volume_format_unsupported",
                "incompatible",
            ),
            (
                ReasonCode::LocalFormatUnknown,
                "local_format_unknown",
                "upgrade_required",
            ),
            (
                ReasonCode::LocalFormatNewer,
                "local_format_newer",
                "upgrade_required",
            ),
            (
                ReasonCode::LocalFormatOutdated,
                "local_format_outdated",
                "needs_migration",
            ),
            (
                ReasonCode::SchemaComponentMissing,
                "schema_component_missing",
                "needs_migration",
            ),
            (
                ReasonCode::SchemaComponentOutdated,
                "schema_component_outdated",
                "needs_migration",
            ),
            (
                ReasonCode::SchemaComponentNewer,
                "schema_component_newer",
                "upgrade_required",
            ),
            (
                ReasonCode::SchemaComponentUnknown,
                "schema_component_unknown",
                "upgrade_required",
            ),
            (
                ReasonCode::EventKindUnknown,
                "event_kind_unknown",
                "upgrade_required",
            ),
            (
                ReasonCode::PayloadFormatUnsupported,
                "payload_format_unsupported",
                "upgrade_required",
            ),
            (
                ReasonCode::ReadCapabilityMissing,
                "read_capability_missing",
                "upgrade_required",
            ),
            (
                ReasonCode::CheckpointFormatNewer,
                "checkpoint_format_newer",
                "upgrade_required",
            ),
            (
                ReasonCode::CheckpointFormatUnsupported,
                "checkpoint_format_unsupported",
                "incompatible",
            ),
            (
                ReasonCode::WriteCapabilityMissing,
                "write_capability_missing",
                "upgrade_required",
            ),
            (
                ReasonCode::MaintenanceCapabilityMissing,
                "maintenance_capability_missing",
                "upgrade_required",
            ),
        ];
        for (code, name, class) in expected {
            assert_eq!(code.as_str(), name);
            assert_eq!(code.class().as_str(), class);
            assert!(!code.action().is_empty());
        }
        let ops: Vec<_> = Operation::ALL.iter().map(|op| op.as_str()).collect();
        assert_eq!(
            ops,
            [
                "bootstrap",
                "replay",
                "publication",
                "checkpoint",
                "maintenance"
            ]
        );
        assert_eq!(Compatibility::CompatibleRo.as_str(), "compatible_ro");
    }
}

use crate::compat::CAP_EVENT_CODEC_V1;
use crate::engine::{MetaError, MetaResult, UpgradeRequiredReason};
use crate::types::{DirEntryValue, InodeAttr};
use crate::volume_protocol::{ProtocolRejection, VolumeProtocol};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Stable prefix for a one-byte wire version, one-byte tag, and postcard payload.
const META_EVENT_WIRE_MAGIC: [u8; 7] = *b"SFS-EVT";
/// Envelope versions readable by this binary. Legacy ordinals have no envelope.
pub const META_EVENT_READ_VERSIONS: &[u8] = &[1];
/// Envelope version emitted by the normal journal and publication encoders.
pub const META_EVENT_WRITE_VERSION: u8 = 1;
const TAG_SET_ATTR: u8 = 1;
const TAG_REMOVE_ATTR: u8 = 2;
const TAG_ADD_ENTRY: u8 = 3;
const TAG_REMOVE_ENTRY: u8 = 4;
const TAG_RENAME_ENTRY: u8 = 5;
const TAG_REMOVE_REGULAR_FILE_GRAPH: u8 = 6;
const TAG_SET_REGULAR_FILE_OVERLAY_SLICES: u8 = 7;
const TAG_PUBLISH_REGULAR_FILE_BASE_SLICES: u8 = 8;
const TAG_SET_SYMLINK_TARGET: u8 = 9;
const TAG_SET_XATTR: u8 = 10;
const TAG_REMOVE_XATTR: u8 = 11;
const TAG_SET_ACL_RULE: u8 = 12;
const TAG_REMOVE_ACL_RULE: u8 = 13;
const TAG_ADD_PENDING_DELETION: u8 = 14;
const TAG_REMOVE_PENDING_DELETION: u8 = 15;
// Tags 16 and 17 belonged to retired snapshot events and remain reserved.
const TAG_REMOVE_LOCAL_ENTRY: u8 = 18;
const TAG_SET_VOLUME_SETTINGS: u8 = 19;
const TAG_SET_VOLUME_SETTINGS_V2: u8 = 20;
const TAG_SET_VOLUME_SETTINGS_V3: u8 = 21;
#[cfg(feature = "test-successor")]
const TAG_SET_USER_XATTR: u8 = 22;

// Append only. Each tag fixes a postcard layout and its apply semantics.
// A change in layout or meaning needs a new tag and a required capability.
// Tags 19-21 show how to add fields without changing older layouts.
// Before enabling publication, define replay semantics, rollback behavior, and
// golden fixtures. A reader may normalize a new encoding to an existing mutation
// only when that mutation represents the full meaning of the new encoding.
// None marks a local-journal-only tag. The capability is the read capability
// every active reader must declare before publication may emit the tag.
const EVENT_CODECS: &[(u8, Option<&str>, &str)] = &[
    (TAG_SET_ATTR, Some("set_attr"), CAP_EVENT_CODEC_V1),
    (TAG_REMOVE_ATTR, Some("remove_attr"), CAP_EVENT_CODEC_V1),
    (TAG_ADD_ENTRY, Some("add_entry"), CAP_EVENT_CODEC_V1),
    (TAG_REMOVE_ENTRY, Some("remove_entry"), CAP_EVENT_CODEC_V1),
    (TAG_RENAME_ENTRY, Some("rename_entry"), CAP_EVENT_CODEC_V1),
    (
        TAG_REMOVE_REGULAR_FILE_GRAPH,
        Some("remove_regular_file_graph"),
        CAP_EVENT_CODEC_V1,
    ),
    (
        TAG_SET_REGULAR_FILE_OVERLAY_SLICES,
        Some("set_regular_file_overlay_slices"),
        CAP_EVENT_CODEC_V1,
    ),
    (
        TAG_PUBLISH_REGULAR_FILE_BASE_SLICES,
        Some("publish_regular_file_base_slices"),
        CAP_EVENT_CODEC_V1,
    ),
    (
        TAG_SET_SYMLINK_TARGET,
        Some("set_symlink_target"),
        CAP_EVENT_CODEC_V1,
    ),
    (TAG_SET_XATTR, Some("set_xattr"), CAP_EVENT_CODEC_V1),
    (TAG_REMOVE_XATTR, Some("remove_xattr"), CAP_EVENT_CODEC_V1),
    (TAG_SET_ACL_RULE, Some("set_acl_rule"), CAP_EVENT_CODEC_V1),
    (
        TAG_REMOVE_ACL_RULE,
        Some("remove_acl_rule"),
        CAP_EVENT_CODEC_V1,
    ),
    (
        TAG_ADD_PENDING_DELETION,
        Some("add_pending_deletion"),
        CAP_EVENT_CODEC_V1,
    ),
    (
        TAG_REMOVE_PENDING_DELETION,
        Some("remove_pending_deletion"),
        CAP_EVENT_CODEC_V1,
    ),
    (TAG_REMOVE_LOCAL_ENTRY, None, CAP_EVENT_CODEC_V1),
    (
        TAG_SET_VOLUME_SETTINGS,
        Some("set_volume_settings"),
        CAP_EVENT_CODEC_V1,
    ),
    (
        TAG_SET_VOLUME_SETTINGS_V2,
        Some("set_volume_settings"),
        CAP_EVENT_CODEC_V1,
    ),
    (
        TAG_SET_VOLUME_SETTINGS_V3,
        Some("set_volume_settings"),
        CAP_EVENT_CODEC_V1,
    ),
    #[cfg(feature = "test-successor")]
    (
        TAG_SET_USER_XATTR,
        Some("set_user_xattr"),
        crate::test_successor::SET_USER_XATTR_CAPABILITY,
    ),
];

const _: () = {
    let mut previous = 0;
    let mut index = 0;
    while index < EVENT_CODECS.len() {
        let tag = EVENT_CODECS[index].0;
        assert!(tag > previous && tag != 16 && tag != 17);
        previous = tag;
        index += 1;
    }
};

/// A well-framed event uses an identity this binary cannot interpret.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnsupportedEventReason {
    /// The tagged envelope uses a newer or otherwise unsupported wire version.
    #[error("unsupported MetaEvent wire version {0}")]
    WireVersion(u8),
    /// The tag is unassigned, reserved, or absent in this build.
    #[error("unknown MetaEvent wire kind tag {0}")]
    Tag(u8),
    /// The PostgreSQL kind is absent from the shared codec registry.
    #[error("unknown PostgreSQL MetaEvent type")]
    PostgresType,
}

impl UnsupportedEventReason {
    /// Compatibility evaluator category for an unsupported persisted event.
    pub fn reason_code(&self) -> crate::compat::ReasonCode {
        crate::compat::ReasonCode::EventKindUnknown
    }
}

impl From<UnsupportedEventReason> for MetaError {
    fn from(reason: UnsupportedEventReason) -> Self {
        Self::UpgradeRequired {
            reason: UpgradeRequiredReason::UnsupportedEvent(reason),
        }
    }
}

fn registered_codec(tag: u8) -> MetaResult<&'static (u8, Option<&'static str>, &'static str)> {
    EVENT_CODECS
        .binary_search_by_key(&tag, |&(registered, _, _)| registered)
        .map(|index| &EVENT_CODECS[index])
        .map_err(|_| UnsupportedEventReason::Tag(tag).into())
}

fn event_codec(tag: u8) -> MetaResult<Option<&'static str>> {
    registered_codec(tag).map(|codec| codec.1)
}

/// Chooses the wire tag publication emits for `event` under the volume's
/// active protocol (SFS-1010): the oldest tag that represents the whole
/// mutation, provided every reader the policy admits declares its codec.
/// The policy row decides; no event history is scanned. A mutation the
/// oldest faithful tag cannot carry for every reader is refused, never
/// re-encoded with fewer fields.
// ponytail: every emitted tag shares `event.codec.v1`, so the oldest faithful
// tag is the only candidate. A future tag with its own capability must try the
// faithful tags oldest-first and encode publication with the chosen one.
fn negotiate_wire_tag(event: &MetaEvent, protocol: &VolumeProtocol) -> MetaResult<u8> {
    let &(tag, _, capability) = registered_codec(event.wire_tag())?;
    if protocol.requirements.read.contains(capability) {
        Ok(tag)
    } else {
        Err(ProtocolRejection::EventNotRepresentable {
            kind: event.kind(),
            capability,
        }
        .into())
    }
}

/// Checks a publication batch against the volume's active protocol before
/// any event is written. A refusal leaves the batch, which the caller keeps
/// as unpublished local work; events authored before a policy change are
/// re-encoded here, so they publish only where the encoding is identical.
///
/// # Errors
///
/// `VolumeProtocolRejected(EventNotRepresentable)` for the first event the
/// protocol cannot carry.
pub(crate) fn ensure_publishable<'a>(
    events: impl IntoIterator<Item = &'a MetaEvent>,
    protocol: &VolumeProtocol,
) -> MetaResult<()> {
    events
        .into_iter()
        .try_for_each(|event| negotiate_wire_tag(event, protocol).map(drop))
}

/// Represents a single mutation to the metadata state.
/// These events are appended to a local journal and eventually pushed to S3
/// to achieve Event Sourcing and Eventual Consistency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaEvent {
    /// An ordered mutable-volume configuration revision.
    SetVolumeSettings {
        /// Complete policy accepted for this revision.
        settings: crate::trash::VolumeSettings,
    },
    // ─── Core Inode Operations ───────────────────────────────────────────────
    /// Create or update an inode.
    SetAttr {
        /// Full inode attributes that should become authoritative.
        attr: InodeAttr,
    },

    /// Remove an inode entirely.
    RemoveAttr {
        /// Inode to remove from metadata.
        ino: u64,
    },

    // ─── Directory Operations ────────────────────────────────────────────────
    /// Add a directory entry.
    AddEntry {
        /// Parent directory that should contain the entry.
        parent: u64,
        /// Entry name encoded as raw bytes.
        name: Vec<u8>,
        /// Dentry payload to store for the child.
        entry: DirEntryValue,
    },

    /// Remove a directory entry.
    RemoveEntry {
        /// Parent directory that currently contains the entry.
        parent: u64,
        /// Entry name encoded as raw bytes.
        name: Vec<u8>,
    },

    /// Remove a name only from this client's serving view, retaining peer state.
    RemoveLocalEntry {
        /// Parent directory containing the locally removed name.
        parent: u64,
        /// Entry name encoded as raw bytes.
        name: Vec<u8>,
    },

    /// Atomically rename a directory entry. Replicas apply this only if the
    /// source dentry still points at the expected inode.
    RenameEntry {
        /// Source parent directory.
        parent: u64,
        /// Source entry name encoded as raw bytes.
        name: Vec<u8>,
        /// Destination parent directory.
        new_parent: u64,
        /// Destination entry name encoded as raw bytes.
        new_name: Vec<u8>,
        /// Entry payload that should appear at the destination.
        entry: DirEntryValue,
        /// Replacement attrs for an inode displaced at the destination, if any.
        displaced_set_attr: Box<Option<InodeAttr>>,
        /// Displaced inode that should be removed outright, if any.
        displaced_remove_attr_ino: Option<u64>,
        /// Updated attrs for a moved directory inode after the rename.
        moved_dir_attr: Box<Option<InodeAttr>>,
        /// Updated attrs for the source parent after the rename.
        old_parent_attr: Box<Option<InodeAttr>>,
        /// Updated attrs for the destination parent after the rename.
        new_parent_attr: Box<Option<InodeAttr>>,
    },

    // ─── Regular File Graph Operations ──────────────────────────────────────
    /// Remove all regular-file graph rows for a file (e.g., unlink).
    RemoveRegularFileGraph {
        /// File inode whose slice graph should be removed.
        ino: u64,
    },

    /// Replace the local overlay slices for a regular file chunk batch.
    ///
    /// The payload carries the final row state for each chunk. An empty slice
    /// vector means the chunk row should be removed.
    SetRegularFileOverlaySlices {
        /// File inode whose overlay chunks are being rewritten.
        ino: u64,
        /// Final chunk rows as `(chunk_id, slices)` pairs.
        chunk_slices: Vec<(crate::schema::ChunkId, Vec<crate::schema::SliceRecord>)>,
    },

    /// Atomically publish the remote-visible regular-file base graph.
    ///
    /// The payload carries the full published base rows for the inode after the
    /// publish barrier completes. This is the slice-native publish event used
    /// by regular-file graphs that can no longer be expressed as a contiguous
    /// `(logical_index, payload_ref)` projection.
    PublishRegularFileBaseSlices {
        /// File inode whose published base chunks are being replaced.
        ino: u64,
        /// Final published base rows as `(chunk_id, slices)` pairs.
        chunk_slices: Vec<(crate::schema::ChunkId, Vec<crate::schema::SliceRecord>)>,
    },

    // ─── Target & Extended Attributes ────────────────────────────────────────
    /// Set a symlink target.
    SetSymlinkTarget {
        /// Symlink inode being updated.
        ino: u64,
        /// Raw target bytes to persist.
        target: Vec<u8>,
    },

    /// Set an extended attribute.
    SetXattr {
        /// Inode that owns the xattr.
        ino: u64,
        /// Raw xattr name bytes.
        name: Vec<u8>,
        /// Raw xattr value bytes.
        value: Vec<u8>,
        /// POSIX-style create/replace flags for the mutation.
        flags: u32, // XATTR_CREATE (1) or XATTR_REPLACE (2)
    },

    /// Remove an extended attribute.
    RemoveXattr {
        /// Inode that owns the xattr.
        ino: u64,
        /// Raw xattr name bytes.
        name: Vec<u8>,
    },

    /// Set ACL rule for `(ino, principal)`.
    SetAclRule {
        /// Inode protected by the ACL rule.
        ino: u64,
        /// Principal receiving the rule.
        principal: String,
        /// Effective access level to persist.
        access_level: crate::types::AccessLevel,
    },

    /// Remove ACL rule for `(ino, principal)`.
    RemoveAclRule {
        /// Inode protected by the ACL rule.
        ino: u64,
        /// Principal whose explicit rule should be removed.
        principal: String,
    },

    // ─── Garbage Collection ──────────────────────────────────────────────────
    /// Mark an object for garbage collection.
    AddPendingDeletion {
        /// Object id scheduled for deferred deletion.
        object_id: u64,
        /// Unix timestamp when the object became pending deletion.
        timestamp: i64,
    },

    /// Remove an object from pending deletions (GC complete).
    RemovePendingDeletion {
        /// Object id that finished deferred deletion.
        object_id: u64,
    },
}

impl MetaEvent {
    /// Return a stable lowercase identifier for this event kind.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SetVolumeSettings { .. } => "set_volume_settings",
            Self::SetAttr { .. } => "set_attr",
            Self::RemoveAttr { .. } => "remove_attr",
            Self::AddEntry { .. } => "add_entry",
            Self::RemoveEntry { .. } => "remove_entry",
            Self::RemoveLocalEntry { .. } => "remove_local_entry",
            Self::RenameEntry { .. } => "rename_entry",
            Self::RemoveRegularFileGraph { .. } => "remove_regular_file_graph",
            Self::SetRegularFileOverlaySlices { .. } => "set_regular_file_overlay_slices",
            Self::PublishRegularFileBaseSlices { .. } => "publish_regular_file_base_slices",
            Self::SetSymlinkTarget { .. } => "set_symlink_target",
            Self::SetXattr { .. } => "set_xattr",
            Self::RemoveXattr { .. } => "remove_xattr",
            Self::SetAclRule { .. } => "set_acl_rule",
            Self::RemoveAclRule { .. } => "remove_acl_rule",
            Self::AddPendingDeletion { .. } => "add_pending_deletion",
            Self::RemovePendingDeletion { .. } => "remove_pending_deletion",
        }
    }

    fn wire_tag(&self) -> u8 {
        match self {
            Self::SetVolumeSettings { settings } if settings.compression.is_enabled() => {
                TAG_SET_VOLUME_SETTINGS_V3
            }
            Self::SetVolumeSettings { settings } if settings.logical_quota_bytes.is_some() => {
                TAG_SET_VOLUME_SETTINGS_V2
            }
            Self::SetVolumeSettings { .. } => TAG_SET_VOLUME_SETTINGS,
            Self::SetAttr { .. } => TAG_SET_ATTR,
            Self::RemoveAttr { .. } => TAG_REMOVE_ATTR,
            Self::AddEntry { .. } => TAG_ADD_ENTRY,
            Self::RemoveEntry { .. } => TAG_REMOVE_ENTRY,
            Self::RemoveLocalEntry { .. } => TAG_REMOVE_LOCAL_ENTRY,
            Self::RenameEntry { .. } => TAG_RENAME_ENTRY,
            Self::RemoveRegularFileGraph { .. } => TAG_REMOVE_REGULAR_FILE_GRAPH,
            Self::SetRegularFileOverlaySlices { .. } => TAG_SET_REGULAR_FILE_OVERLAY_SLICES,
            Self::PublishRegularFileBaseSlices { .. } => TAG_PUBLISH_REGULAR_FILE_BASE_SLICES,
            Self::SetSymlinkTarget { .. } => TAG_SET_SYMLINK_TARGET,
            Self::SetXattr { .. } => TAG_SET_XATTR,
            Self::RemoveXattr { .. } => TAG_REMOVE_XATTR,
            Self::SetAclRule { .. } => TAG_SET_ACL_RULE,
            Self::RemoveAclRule { .. } => TAG_REMOVE_ACL_RULE,
            Self::AddPendingDeletion { .. } => TAG_ADD_PENDING_DELETION,
            Self::RemovePendingDeletion { .. } => TAG_REMOVE_PENDING_DELETION,
        }
    }

    /// Return `true` when this event mutates the directory namespace.
    pub fn is_namespace_mutation(&self) -> bool {
        matches!(
            self,
            Self::AddEntry { .. }
                | Self::RemoveEntry { .. }
                | Self::RemoveLocalEntry { .. }
                | Self::RenameEntry { .. }
        )
    }

    /// Returns the primary Inode associated with this event, if applicable.
    /// This is useful for partitioning or logical clocks.
    pub fn primary_ino(&self) -> Option<u64> {
        match self {
            Self::SetVolumeSettings { .. } => None,
            Self::SetAttr { attr } => Some(attr.ino),
            Self::RemoveAttr { ino } => Some(*ino),
            Self::AddEntry { parent, .. } => Some(*parent),
            Self::RemoveEntry { parent, .. } | Self::RemoveLocalEntry { parent, .. } => {
                Some(*parent)
            }
            Self::RenameEntry { parent, .. } => Some(*parent),
            Self::RemoveRegularFileGraph { ino } => Some(*ino),
            Self::SetRegularFileOverlaySlices { ino, .. } => Some(*ino),
            Self::PublishRegularFileBaseSlices { ino, .. } => Some(*ino),
            Self::SetSymlinkTarget { ino, .. } => Some(*ino),
            Self::SetXattr { ino, .. } => Some(*ino),
            Self::RemoveXattr { ino, .. } => Some(*ino),
            Self::SetAclRule { ino, .. } => Some(*ino),
            Self::RemoveAclRule { ino, .. } => Some(*ino),
            Self::AddPendingDeletion { .. } => None,
            Self::RemovePendingDeletion { .. } => None,
        }
    }
}

/// Encode a metadata event using the persisted PostgreSQL/local-journal wire format.
///
/// Integration fixtures and storage adapters use this function to create event
/// rows without duplicating the format's tag and envelope rules.
pub fn encode_event(event: &MetaEvent) -> MetaResult<Vec<u8>> {
    let tag = event.wire_tag();
    match event {
        MetaEvent::SetVolumeSettings { settings } if tag == TAG_SET_VOLUME_SETTINGS => {
            encode_event_envelope(tag, &(settings.revision, settings.trash))
        }
        MetaEvent::SetVolumeSettings { settings } if tag == TAG_SET_VOLUME_SETTINGS_V2 => {
            encode_event_envelope(
                tag,
                &(
                    settings.revision,
                    settings.trash,
                    settings.logical_quota_bytes,
                ),
            )
        }
        MetaEvent::SetVolumeSettings { settings } => encode_event_envelope(tag, settings),
        MetaEvent::SetAttr { attr } => encode_event_envelope(tag, attr),
        MetaEvent::RemoveAttr { ino } => encode_event_envelope(tag, ino),
        MetaEvent::AddEntry {
            parent,
            name,
            entry,
        } => encode_event_envelope(tag, &(*parent, name, entry)),
        MetaEvent::RemoveEntry { parent, name } | MetaEvent::RemoveLocalEntry { parent, name } => {
            encode_event_envelope(tag, &(*parent, name))
        }
        MetaEvent::RenameEntry {
            parent,
            name,
            new_parent,
            new_name,
            entry,
            displaced_set_attr,
            displaced_remove_attr_ino,
            moved_dir_attr,
            old_parent_attr,
            new_parent_attr,
        } => encode_event_envelope(
            tag,
            &(
                *parent,
                name,
                *new_parent,
                new_name,
                entry,
                displaced_set_attr,
                displaced_remove_attr_ino,
                moved_dir_attr,
                old_parent_attr,
                new_parent_attr,
            ),
        ),
        MetaEvent::RemoveRegularFileGraph { ino } => encode_event_envelope(tag, ino),
        MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices }
        | MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } => {
            encode_event_envelope(tag, &(*ino, chunk_slices))
        }
        MetaEvent::SetSymlinkTarget { ino, target } => encode_event_envelope(tag, &(*ino, target)),
        MetaEvent::SetXattr {
            ino,
            name,
            value,
            flags,
        } => encode_event_envelope(tag, &(*ino, name, value, *flags)),
        MetaEvent::RemoveXattr { ino, name } => encode_event_envelope(tag, &(*ino, name)),
        MetaEvent::SetAclRule {
            ino,
            principal,
            access_level,
        } => encode_event_envelope(tag, &(*ino, principal, *access_level)),
        MetaEvent::RemoveAclRule { ino, principal } => {
            encode_event_envelope(tag, &(*ino, principal))
        }
        MetaEvent::AddPendingDeletion {
            object_id,
            timestamp,
        } => encode_event_envelope(tag, &(*object_id, *timestamp)),
        MetaEvent::RemovePendingDeletion { object_id } => encode_event_envelope(tag, object_id),
    }
}

pub(crate) fn decode_event(bytes: &[u8]) -> MetaResult<MetaEvent> {
    let (tag, payload) = decode_event_wire_parts(bytes)?;
    decode_tagged_event(tag, payload)
}

pub(crate) fn decode_event_with_type(event_type: &str, bytes: &[u8]) -> MetaResult<MetaEvent> {
    let (tag, payload) = decode_event_wire_parts(bytes)?;
    let kind = event_codec(tag)?;
    if kind != Some(event_type) {
        if !EVENT_CODECS
            .iter()
            .any(|&(_, kind, _)| kind == Some(event_type))
        {
            return Err(UnsupportedEventReason::PostgresType.into());
        }
        return Err(MetaError::Serialization(format!(
            "PostgreSQL MetaEvent type does not match decoded wire tag {tag}"
        )));
    }
    decode_tagged_event(tag, payload)
}

fn decode_event_wire_parts(bytes: &[u8]) -> MetaResult<(u8, &[u8])> {
    if bytes.first() == META_EVENT_WIRE_MAGIC.first() {
        if !bytes.starts_with(&META_EVENT_WIRE_MAGIC) {
            return Err(MetaError::Serialization(
                "invalid MetaEvent wire magic".to_string(),
            ));
        }
        let header_len = META_EVENT_WIRE_MAGIC.len() + 2;
        if bytes.len() < header_len {
            return Err(MetaError::Serialization(
                "truncated MetaEvent wire envelope header".to_string(),
            ));
        }
        let version = bytes[META_EVENT_WIRE_MAGIC.len()];
        if !META_EVENT_READ_VERSIONS.contains(&version) {
            return Err(UnsupportedEventReason::WireVersion(version).into());
        }
        let tag = bytes[META_EVENT_WIRE_MAGIC.len() + 1];
        return Ok((tag, &bytes[header_len..]));
    }

    let (ordinal, payload) = postcard::take_from_bytes::<u32>(bytes).map_err(|error| {
        MetaError::Serialization(format!("legacy MetaEvent ordinal decode failed: {error}"))
    })?;
    let tag = match ordinal {
        0 => TAG_SET_ATTR,
        1 => TAG_REMOVE_ATTR,
        2 => TAG_ADD_ENTRY,
        3 => TAG_REMOVE_ENTRY,
        4 => TAG_RENAME_ENTRY,
        5 => TAG_REMOVE_REGULAR_FILE_GRAPH,
        6 => TAG_SET_REGULAR_FILE_OVERLAY_SLICES,
        7 => TAG_PUBLISH_REGULAR_FILE_BASE_SLICES,
        8 => TAG_SET_SYMLINK_TARGET,
        9 => TAG_SET_XATTR,
        10 => TAG_REMOVE_XATTR,
        11 => TAG_SET_ACL_RULE,
        12 => TAG_REMOVE_ACL_RULE,
        13 => TAG_ADD_PENDING_DELETION,
        14 => TAG_REMOVE_PENDING_DELETION,
        _ => {
            return Err(MetaError::Serialization(format!(
                "unknown legacy MetaEvent ordinal {ordinal}"
            )));
        }
    };
    Ok((tag, payload))
}

fn encode_event_envelope<T: Serialize>(tag: u8, payload: &T) -> MetaResult<Vec<u8>> {
    let envelope = (
        META_EVENT_WIRE_MAGIC,
        META_EVENT_WRITE_VERSION,
        tag,
        payload,
    );
    postcard::to_stdvec(&envelope).map_err(|error| {
        MetaError::Serialization(format!("MetaEvent envelope encode failed: {error}"))
    })
}

fn decode_event_payload<T: DeserializeOwned>(tag: u8, payload: &[u8]) -> MetaResult<T> {
    let (value, remainder) = postcard::take_from_bytes(payload).map_err(|error| {
        MetaError::Serialization(format!(
            "MetaEvent wire tag {tag} payload decode failed: {error}"
        ))
    })?;
    if !remainder.is_empty() {
        return Err(MetaError::Serialization(format!(
            "MetaEvent wire tag {tag} payload contains {} trailing bytes",
            remainder.len()
        )));
    }
    Ok(value)
}

fn decode_tagged_event(tag: u8, payload: &[u8]) -> MetaResult<MetaEvent> {
    match tag {
        TAG_SET_VOLUME_SETTINGS => {
            let (revision, trash) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::SetVolumeSettings {
                settings: crate::trash::VolumeSettings {
                    revision,
                    trash,
                    logical_quota_bytes: None,
                    compression: crate::trash::CompressionPolicy::off(),
                },
            })
        }
        TAG_SET_VOLUME_SETTINGS_V2 => {
            let (revision, trash, logical_quota_bytes) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::SetVolumeSettings {
                settings: crate::trash::VolumeSettings {
                    revision,
                    trash,
                    logical_quota_bytes,
                    compression: crate::trash::CompressionPolicy::off(),
                },
            })
        }
        TAG_SET_VOLUME_SETTINGS_V3 => Ok(MetaEvent::SetVolumeSettings {
            settings: decode_event_payload(tag, payload)?,
        }),
        TAG_SET_ATTR => Ok(MetaEvent::SetAttr {
            attr: decode_event_payload(tag, payload)?,
        }),
        TAG_REMOVE_ATTR => Ok(MetaEvent::RemoveAttr {
            ino: decode_event_payload(tag, payload)?,
        }),
        TAG_ADD_ENTRY => {
            let (parent, name, entry) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::AddEntry {
                parent,
                name,
                entry,
            })
        }
        TAG_REMOVE_ENTRY => {
            let (parent, name) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::RemoveEntry { parent, name })
        }
        TAG_REMOVE_LOCAL_ENTRY => {
            let (parent, name) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::RemoveLocalEntry { parent, name })
        }
        TAG_RENAME_ENTRY => {
            let (
                parent,
                name,
                new_parent,
                new_name,
                entry,
                displaced_set_attr,
                displaced_remove_attr_ino,
                moved_dir_attr,
                old_parent_attr,
                new_parent_attr,
            ) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::RenameEntry {
                parent,
                name,
                new_parent,
                new_name,
                entry,
                displaced_set_attr,
                displaced_remove_attr_ino,
                moved_dir_attr,
                old_parent_attr,
                new_parent_attr,
            })
        }
        TAG_REMOVE_REGULAR_FILE_GRAPH => Ok(MetaEvent::RemoveRegularFileGraph {
            ino: decode_event_payload(tag, payload)?,
        }),
        TAG_SET_REGULAR_FILE_OVERLAY_SLICES => {
            let (ino, chunk_slices) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices })
        }
        TAG_PUBLISH_REGULAR_FILE_BASE_SLICES => {
            let (ino, chunk_slices) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices })
        }
        TAG_SET_SYMLINK_TARGET => {
            let (ino, target) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::SetSymlinkTarget { ino, target })
        }
        TAG_SET_XATTR => {
            let (ino, name, value, flags) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::SetXattr {
                ino,
                name,
                value,
                flags,
            })
        }
        TAG_REMOVE_XATTR => {
            let (ino, name) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::RemoveXattr { ino, name })
        }
        TAG_SET_ACL_RULE => {
            let (ino, principal, access_level) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::SetAclRule {
                ino,
                principal,
                access_level,
            })
        }
        TAG_REMOVE_ACL_RULE => {
            let (ino, principal) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::RemoveAclRule { ino, principal })
        }
        TAG_ADD_PENDING_DELETION => {
            let (object_id, timestamp) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::AddPendingDeletion {
                object_id,
                timestamp,
            })
        }
        TAG_REMOVE_PENDING_DELETION => Ok(MetaEvent::RemovePendingDeletion {
            object_id: decode_event_payload(tag, payload)?,
        }),
        #[cfg(feature = "test-successor")]
        TAG_SET_USER_XATTR => {
            let (ino, suffix, value): (u64, Vec<u8>, Vec<u8>) = decode_event_payload(tag, payload)?;
            if suffix.is_empty() || suffix.len() > 250 || suffix.contains(&0) {
                return Err(MetaError::Serialization(
                    "invalid user xattr name in successor MetaEvent".into(),
                ));
            }
            let mut name = b"user.".to_vec();
            name.extend(suffix);
            Ok(MetaEvent::SetXattr {
                ino,
                name,
                value,
                flags: 0,
            })
        }
        _ => Err(UnsupportedEventReason::Tag(tag).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_settings_tag19_roundtrip_and_policy_validation() {
        let mut settings = crate::trash::VolumeSettings {
            revision: 1,
            ..Default::default()
        };
        settings.trash.set_days(7);
        let event = MetaEvent::SetVolumeSettings { settings };
        let encoded = encode_event(&event).unwrap();
        assert_eq!(encoded[8], 19);
        let legacy = encode_event_envelope(19, &(settings.revision, settings.trash)).unwrap();
        assert_eq!(
            decode_event_with_type("set_volume_settings", &legacy).unwrap(),
            event
        );
        assert_eq!(decode_event(&encoded).unwrap(), event);
        assert_eq!(
            decode_event_with_type("set_volume_settings", &encoded).unwrap(),
            event
        );
        let invalid =
            postcard::to_stdvec(&(*b"SFS-EVT", 1_u8, 19_u8, (1_u64, (7_u32, 1_u32)))).unwrap();
        assert!(matches!(
            decode_event(&invalid),
            Err(MetaError::Serialization(_))
        ));
    }
    #[test]
    fn quota_settings_reject_legacy_rewrites_and_roundtrip_tag20() {
        #[derive(serde::Deserialize)]
        struct Legacy {
            revision: u64,
            trash: crate::trash::TrashPolicy,
        }
        let mut settings = crate::trash::VolumeSettings::default();
        let legacy: Legacy =
            serde_json::from_slice(&serde_json::to_vec(&settings).unwrap()).unwrap();
        assert_eq!(
            (legacy.revision, legacy.trash),
            (settings.revision, settings.trash)
        );
        settings.logical_quota_bytes = Some(0);
        let json = serde_json::to_vec(&settings).unwrap();
        assert!(serde_json::from_slice::<Legacy>(&json).is_err());
        assert_eq!(
            serde_json::from_slice::<crate::trash::VolumeSettings>(&json).unwrap(),
            settings
        );
        let event = MetaEvent::SetVolumeSettings { settings };
        let encoded = encode_event(&event).unwrap();
        assert_eq!(encoded[8], 20);
        assert_eq!(
            decode_event_with_type("set_volume_settings", &encoded).unwrap(),
            event
        );
    }

    #[test]
    fn compression_settings_tag19_tag20_default_off_and_tag21_roundtrips() {
        use crate::trash::{CompressionPolicy, TrashPolicy, VolumeSettings};

        let mut trash = TrashPolicy::default();
        trash.set_days(7);

        // Tag 19 fixture (2-field layout) decodes with compression = off.
        let tag19_bytes = encode_event_envelope(TAG_SET_VOLUME_SETTINGS, &(1_u64, trash)).unwrap();
        assert_eq!(tag19_bytes[8], 19);
        assert_eq!(
            decode_event_with_type("set_volume_settings", &tag19_bytes).unwrap(),
            MetaEvent::SetVolumeSettings {
                settings: VolumeSettings {
                    revision: 1,
                    trash,
                    logical_quota_bytes: None,
                    compression: CompressionPolicy::off(),
                },
            }
        );

        // Tag 20 fixture (3-field layout) decodes with compression = off and
        // encode_event keeps the 3-field layout unchanged when compression is off.
        let tag20_bytes =
            encode_event_envelope(TAG_SET_VOLUME_SETTINGS_V2, &(2_u64, trash, Some(8192_u64)))
                .unwrap();
        assert_eq!(tag20_bytes[8], 20);
        let tag20_event = MetaEvent::SetVolumeSettings {
            settings: VolumeSettings {
                revision: 2,
                trash,
                logical_quota_bytes: Some(8192),
                compression: CompressionPolicy::off(),
            },
        };
        assert_eq!(encode_event(&tag20_event).unwrap(), tag20_bytes);
        assert_eq!(
            decode_event_with_type("set_volume_settings", &tag20_bytes).unwrap(),
            tag20_event
        );

        // Tag 21 event round-trips the compression policy.
        let tag21_event = MetaEvent::SetVolumeSettings {
            settings: VolumeSettings {
                revision: 3,
                trash,
                logical_quota_bytes: Some(8192),
                compression: CompressionPolicy::zstd(9).unwrap(),
            },
        };
        let tag21_bytes = encode_event(&tag21_event).unwrap();
        assert_eq!(tag21_bytes[8], 21);
        assert_eq!(decode_event(&tag21_bytes).unwrap(), tag21_event);
        assert_eq!(
            decode_event_with_type("set_volume_settings", &tag21_bytes).unwrap(),
            tag21_event
        );

        // Invalid compression level in tag 21 is rejected.
        for bad_level in [0_u8, 20_u8] {
            let invalid = postcard::to_stdvec(&(
                *b"SFS-EVT",
                1_u8,
                TAG_SET_VOLUME_SETTINGS_V3,
                (3_u64, trash, Some(8192_u64), (1_u8, bad_level)),
            ))
            .unwrap();
            assert!(matches!(
                decode_event(&invalid),
                Err(MetaError::Serialization(_))
            ));
        }
    }

    use crate::types::{
        AccessLevel, ChunkId, FileType, PayloadRef, SliceId, SliceOrder, SliceRange, SliceRecord,
        UploadState,
    };

    fn sample_attr(ino: u64, kind: FileType) -> InodeAttr {
        InodeAttr {
            ino,
            kind,
            mode: 0o644,
            uid: 10,
            gid: 20,
            size: 30,
            committed_size: 25,
            nlink: 1,
            mtime: 40,
            ctime: 50,
            atime: 60,
            parent: 2,
            crtime: 70,
            win_attrs: 80,
            content_epoch: 90,
            committed_content_epoch: 100,
            upload_state: UploadState::Ready,
        }
    }

    fn sample_slices() -> Vec<(ChunkId, Vec<SliceRecord>)> {
        vec![(
            ChunkId(3),
            vec![SliceRecord::new(
                SliceId(4),
                SliceOrder(5),
                SliceRange::new(6, 7).expect("sample range"),
                PayloadRef::new_partial(8, 9),
            )],
        )]
    }

    fn event_fixtures() -> Vec<(MetaEvent, &'static [u8], &'static [u8])> {
        let attr = sample_attr(11, FileType::RegularFile);
        let entry = DirEntryValue {
            ino: 12,
            kind: FileType::Directory,
        };
        vec![
            (
                MetaEvent::SetAttr { attr: attr.clone() },
                b"SFS-EVT\x01\x01\x0b\x00\xa4\x03\x0a\x14\x1e\x19\x01\x50\x64\x78\x02\x8c\x01\x50\x5a\x64\x02",
                b"\x00\x0b\x00\xa4\x03\x0a\x14\x1e\x19\x01\x50\x64\x78\x02\x8c\x01\x50\x5a\x64\x02",
            ),
            (
                MetaEvent::RemoveAttr { ino: 13 },
                b"SFS-EVT\x01\x02\x0d",
                b"\x01\x0d",
            ),
            (
                MetaEvent::AddEntry {
                    parent: 14,
                    name: b"child".to_vec(),
                    entry: entry.clone(),
                },
                b"SFS-EVT\x01\x03\x0e\x05child\x0c\x01",
                b"\x02\x0e\x05child\x0c\x01",
            ),
            (
                MetaEvent::RemoveEntry {
                    parent: 15,
                    name: b"old".to_vec(),
                },
                b"SFS-EVT\x01\x04\x0f\x03old",
                b"\x03\x0f\x03old",
            ),
            (
                MetaEvent::RenameEntry {
                    parent: 16,
                    name: b"from".to_vec(),
                    new_parent: 17,
                    new_name: b"to".to_vec(),
                    entry: entry.clone(),
                    displaced_set_attr: Box::new(Some(attr.clone())),
                    displaced_remove_attr_ino: Some(18),
                    moved_dir_attr: Box::new(Some(attr.clone())),
                    old_parent_attr: Box::new(Some(attr.clone())),
                    new_parent_attr: Box::new(Some(attr.clone())),
                },
                &[
                    83, 70, 83, 45, 69, 86, 84, 1, 5, 16, 4, 102, 114, 111, 109, 17, 2,
                    116, 111, 12, 1, 1, 11, 0, 164, 3, 10, 20, 30, 25, 1, 80, 100, 120, 2,
                    140, 1, 80, 90, 100, 2, 1, 18, 1, 11, 0, 164, 3, 10, 20, 30, 25, 1, 80,
                    100, 120, 2, 140, 1, 80, 90, 100, 2, 1, 11, 0, 164, 3, 10, 20, 30, 25, 1,
                    80, 100, 120, 2, 140, 1, 80, 90, 100, 2, 1, 11, 0, 164, 3, 10, 20, 30,
                    25, 1, 80, 100, 120, 2, 140, 1, 80, 90, 100, 2,
                ],
                &[
                    4, 16, 4, 102, 114, 111, 109, 17, 2, 116, 111, 12, 1, 1, 11, 0, 164, 3,
                    10, 20, 30, 25, 1, 80, 100, 120, 2, 140, 1, 80, 90, 100, 2, 1, 18, 1,
                    11, 0, 164, 3, 10, 20, 30, 25, 1, 80, 100, 120, 2, 140, 1, 80, 90, 100,
                    2, 1, 11, 0, 164, 3, 10, 20, 30, 25, 1, 80, 100, 120, 2, 140, 1, 80, 90,
                    100, 2, 1, 11, 0, 164, 3, 10, 20, 30, 25, 1, 80, 100, 120, 2, 140, 1,
                    80, 90, 100, 2,
                ],
            ),
            (
                MetaEvent::RemoveRegularFileGraph { ino: 19 },
                b"SFS-EVT\x01\x06\x13",
                b"\x05\x13",
            ),
            (
                MetaEvent::SetRegularFileOverlaySlices {
                    ino: 20,
                    chunk_slices: sample_slices(),
                },
                b"SFS-EVT\x01\x07\x14\x01\x03\x01\x04\x05\x06\x07\x01\x08\x00\x09\x02\x00",
                b"\x06\x14\x01\x03\x01\x04\x05\x06\x07\x01\x08\x00\x09\x02\x00",
            ),
            (
                MetaEvent::PublishRegularFileBaseSlices {
                    ino: 21,
                    chunk_slices: sample_slices(),
                },
                b"SFS-EVT\x01\x08\x15\x01\x03\x01\x04\x05\x06\x07\x01\x08\x00\x09\x02\x00",
                b"\x07\x15\x01\x03\x01\x04\x05\x06\x07\x01\x08\x00\x09\x02\x00",
            ),
            (
                MetaEvent::SetSymlinkTarget {
                    ino: 22,
                    target: b"/target".to_vec(),
                },
                b"SFS-EVT\x01\x09\x16\x07/target",
                b"\x08\x16\x07/target",
            ),
            (
                MetaEvent::SetXattr {
                    ino: 23,
                    name: b"user.key".to_vec(),
                    value: b"value".to_vec(),
                    flags: 1,
                },
                b"SFS-EVT\x01\x0a\x17\x08user.key\x05value\x01",
                b"\x09\x17\x08user.key\x05value\x01",
            ),
            (
                MetaEvent::RemoveXattr {
                    ino: 24,
                    name: b"user.key".to_vec(),
                },
                b"SFS-EVT\x01\x0b\x18\x08user.key",
                b"\x0a\x18\x08user.key",
            ),
            (
                MetaEvent::SetAclRule {
                    ino: 25,
                    principal: "alice".to_string(),
                    access_level: AccessLevel::ReadWrite,
                },
                b"SFS-EVT\x01\x0c\x19\x05alice\x02",
                b"\x0b\x19\x05alice\x02",
            ),
            (
                MetaEvent::RemoveAclRule {
                    ino: 26,
                    principal: "alice".to_string(),
                },
                b"SFS-EVT\x01\x0d\x1a\x05alice",
                b"\x0c\x1a\x05alice",
            ),
            (
                MetaEvent::AddPendingDeletion {
                    object_id: 27,
                    timestamp: 28,
                },
                b"SFS-EVT\x01\x0e\x1b\x38",
                b"\x0d\x1b\x38",
            ),
            (
                MetaEvent::RemovePendingDeletion { object_id: 29 },
                b"SFS-EVT\x01\x0f\x1d",
                b"\x0e\x1d",
            ),
        ]
    }

    #[test]
    fn local_entry_removal_has_a_stable_local_journal_tag() {
        let event = MetaEvent::RemoveLocalEntry {
            parent: 15,
            name: b"old".to_vec(),
        };
        let bytes = b"SFS-EVT\x01\x12\x0f\x03old";
        assert_eq!(encode_event(&event).unwrap(), bytes);
        assert_eq!(decode_event(bytes).unwrap(), event);
        assert!(event.is_namespace_mutation());
        assert_eq!(event.primary_ino(), Some(15));
    }

    #[test]
    fn legacy_snapshot_events_are_rejected() {
        let create_snapshot = b"SFS-EVT\x01\x10\x08snapshot\x1e\x3e\x01\x01\x20\x07payload";
        let update_snapshot = b"SFS-EVT\x01\x11\x08snapshot\x02";

        for (bytes, tag) in [
            (create_snapshot.as_slice(), 16),
            (update_snapshot.as_slice(), 17),
        ] {
            let error = decode_event(bytes).expect_err("legacy snapshot event");
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown MetaEvent wire kind tag {tag}")),
                "unexpected error: {error}"
            );
        }

        for (bytes, ordinal) in [(b"\x0f\x00".as_slice(), 15), (b"\x10\x00".as_slice(), 16)] {
            let error = decode_event(bytes).expect_err("legacy snapshot event ordinal");
            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown legacy MetaEvent ordinal {ordinal}")),
                "unexpected error: {error}"
            );
        }

        assert!(
            decode_event_with_type("create_snapshot", create_snapshot).is_err(),
            "v1 snapshot events must not enter the Snapshot v2 event path"
        );
    }

    #[test]
    fn every_event_kind_matches_its_versioned_wire_fixture() {
        for (event, expected_wire, legacy_wire) in event_fixtures() {
            let encoded = encode_event(&event).expect("encode event fixture");
            assert_eq!(encoded, expected_wire, "fixture drift for {}", event.kind());

            let decoded = decode_event(expected_wire).expect("decode event fixture");
            assert_eq!(decoded.kind(), event.kind());
            assert_eq!(format!("{decoded:?}"), format!("{event:?}"));

            let typed = decode_event_with_type(event.kind(), expected_wire)
                .expect("decode typed event fixture");
            assert_eq!(typed.kind(), event.kind());

            let typed_legacy = decode_event_with_type(event.kind(), legacy_wire)
                .expect("decode typed legacy event fixture");
            assert_eq!(typed_legacy.kind(), event.kind());
        }
    }

    #[test]
    fn every_legacy_event_fixture_decodes_with_its_original_ordinal() {
        for (event, _, legacy_wire) in event_fixtures() {
            let decoded = decode_event(legacy_wire).expect("decode legacy event fixture");

            assert_eq!(format!("{decoded:?}"), format!("{event:?}"));
        }
    }

    #[test]
    fn event_decoders_reject_invalid_slice_ranges_in_both_wire_formats() {
        for (start, end, valid) in [
            (3, 4, true),
            (0, 0, false),
            (2, 1, false),
            (0, crate::schema::DEFAULT_CHUNK_SIZE + 1, false),
        ] {
            for (is_base, legacy_ordinal) in [(false, 6_u32), (true, 7_u32)] {
                let chunk_slices = vec![(
                    ChunkId(3),
                    vec![SliceRecord::new(
                        SliceId(4),
                        SliceOrder(5),
                        SliceRange { start, end },
                        PayloadRef::new_partial(8, 1),
                    )],
                )];
                let event = if is_base {
                    MetaEvent::PublishRegularFileBaseSlices {
                        ino: 20,
                        chunk_slices,
                    }
                } else {
                    MetaEvent::SetRegularFileOverlaySlices {
                        ino: 20,
                        chunk_slices,
                    }
                };
                // Historical bytes bypass today's validating SliceRange serializer.
                let raw_chunks = vec![(
                    ChunkId(3),
                    vec![(
                        SliceId(4),
                        SliceOrder(5),
                        (start, end),
                        Some(PayloadRef::new_partial(8, 1)),
                        crate::schema::PayloadRefState::LocalOnly,
                    )],
                )];
                let tag = if is_base {
                    TAG_PUBLISH_REGULAR_FILE_BASE_SLICES
                } else {
                    TAG_SET_REGULAR_FILE_OVERLAY_SLICES
                };
                let versioned = encode_event_envelope(tag, &(20_u64, &raw_chunks))
                    .expect("historical envelope");
                let legacy = postcard::to_stdvec(&(legacy_ordinal, (20_u64, &raw_chunks)))
                    .expect("historical legacy event");
                if valid {
                    assert_eq!(encode_event(&event).unwrap(), versioned);
                } else {
                    assert!(matches!(
                        encode_event(&event),
                        Err(MetaError::Serialization(_))
                    ));
                }

                for bytes in [&versioned, &legacy] {
                    if valid {
                        let decoded = decode_event(bytes).expect("decode valid slice event");
                        assert_eq!(format!("{decoded:?}"), format!("{event:?}"));
                    } else {
                        assert!(
                            matches!(decode_event(bytes), Err(MetaError::Serialization(_))),
                            "invalid range [{start}, {end}) must fail as persisted corruption"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn legacy_remove_attr_fixture_still_decodes() {
        let event = decode_event(&[1_u8, 42_u8]).expect("decode legacy event");

        assert!(matches!(event, MetaEvent::RemoveAttr { ino: 42 }));
    }

    #[test]
    fn unknown_legacy_ordinal_is_rejected_with_actionable_error() {
        let error = decode_event(&[17_u8]).expect_err("unknown legacy ordinal");

        assert!(matches!(error, MetaError::Serialization(_)));
        assert!(
            error
                .to_string()
                .contains("unknown legacy MetaEvent ordinal 17"),
            "unexpected error: {error}"
        );
    }

    fn assert_unsupported(result: MetaResult<MetaEvent>, expected: UnsupportedEventReason) {
        assert!(matches!(
            result,
            Err(MetaError::UpgradeRequired {
                reason: UpgradeRequiredReason::UnsupportedEvent(reason),
            }) if reason == expected
        ));
    }

    #[test]
    fn unknown_wire_version_is_rejected() {
        let fixture = b"SFS-EVT\x02\x02\x2a";
        let reason = UnsupportedEventReason::WireVersion(2);
        assert_unsupported(decode_event(fixture), reason.clone());
        assert_unsupported(decode_event_with_type("remove_attr", fixture), reason);
    }

    #[test]
    fn unknown_wire_tag_is_rejected() {
        for tag in [16, 17, 255] {
            let mut fixture = *b"SFS-EVT\x01\xff\x00";
            fixture[8] = tag;
            let reason = UnsupportedEventReason::Tag(tag);
            assert_unsupported(decode_event(&fixture), reason.clone());
            assert_unsupported(decode_event_with_type("remove_attr", &fixture), reason);
        }
    }

    #[test]
    fn malformed_or_truncated_wire_is_serialization_error() {
        let fixture = b"SFS-EVT\x01\x02\x2a";
        for len in 0..fixture.len() {
            assert!(matches!(
                decode_event(&fixture[..len]),
                Err(MetaError::Serialization(_))
            ));
        }
        for corrupt in [b"SFS-BAD\x01\x02\x2a", b"XFS-EVT\x01\x02\x2a"] {
            assert!(matches!(
                decode_event(corrupt),
                Err(MetaError::Serialization(_))
            ));
        }
    }

    #[test]
    fn successor_user_xattr_golden_fixture() {
        let fixture = b"SFS-EVT\x01\x16\x2a\x04note\x02hi";
        let result = decode_event_with_type("set_user_xattr", fixture);
        #[cfg(not(feature = "test-successor"))]
        assert_unsupported(result, UnsupportedEventReason::Tag(22));
        #[cfg(feature = "test-successor")]
        {
            let expected = MetaEvent::SetXattr {
                ino: 42,
                name: b"user.note".to_vec(),
                value: b"hi".to_vec(),
                flags: 0,
            };
            assert_eq!(result.unwrap(), expected);
            assert_eq!(
                encode_event_envelope(
                    TAG_SET_USER_XATTR,
                    &(42_u64, b"note".as_slice(), b"hi".as_slice())
                )
                .unwrap(),
                fixture,
            );
            assert_eq!(
                encode_event(&expected).unwrap(),
                b"SFS-EVT\x01\x0a\x2a\x09user.note\x02hi\x00",
            );
            assert!(matches!(
                decode_event(&fixture[..fixture.len() - 1]),
                Err(MetaError::Serialization(_))
            ));
            assert!(matches!(
                decode_event(b"SFS-EVT\x01\x16\x2a\x00\x02hi"),
                Err(MetaError::Serialization(_))
            ));
        }
    }

    #[test]
    fn trailing_wire_payload_is_rejected() {
        let mut encoded = encode_event(&MetaEvent::RemoveAttr { ino: 42 }).expect("encode event");
        encoded.push(0);

        let error = decode_event(&encoded).expect_err("trailing payload");

        assert!(error.to_string().contains("trailing bytes"));
    }

    #[test]
    fn postgres_event_type_must_match_decoded_payload() {
        let encoded = encode_event(&MetaEvent::RemoveAttr { ino: 42 }).expect("encode event");

        assert_unsupported(
            decode_event_with_type("future_event", &encoded),
            UnsupportedEventReason::PostgresType,
        );

        let mismatch = decode_event_with_type("set_attr", &encoded)
            .expect_err("mismatched PostgreSQL event type");
        assert!(mismatch.to_string().contains("does not match decoded"));

        let legacy_mismatch = decode_event_with_type("set_attr", &[1_u8, 42_u8])
            .expect_err("mismatched legacy PostgreSQL event type");
        assert!(
            legacy_mismatch
                .to_string()
                .contains("does not match decoded")
        );
    }

    fn settings_event(quota: Option<u64>, compression: bool) -> MetaEvent {
        let mut settings = crate::trash::VolumeSettings {
            revision: 3,
            logical_quota_bytes: quota,
            ..Default::default()
        };
        if compression {
            settings.compression = crate::trash::CompressionPolicy::zstd_default();
        }
        MetaEvent::SetVolumeSettings { settings }
    }

    /// A policy whose readers are not all known to hold the v1 event codec.
    fn policy_without_event_codec() -> VolumeProtocol {
        let mut policy = VolumeProtocol::baseline();
        let sets = &mut policy.requirements;
        sets.read.remove(CAP_EVENT_CODEC_V1);
        sets.write.remove(CAP_EVENT_CODEC_V1);
        sets.features.insert(
            CAP_EVENT_CODEC_V1.to_owned(),
            crate::volume_protocol::FeatureState::Compatible,
        );
        policy.validate().unwrap();
        policy
    }

    #[test]
    fn publication_negotiates_the_oldest_settings_tag_that_keeps_the_policy() {
        let baseline = VolumeProtocol::baseline();
        for (event, tag) in [
            (settings_event(None, false), 19),
            (settings_event(Some(1 << 30), false), 20),
            (settings_event(None, true), 21),
            (settings_event(Some(1 << 30), true), 21),
        ] {
            assert_eq!(negotiate_wire_tag(&event, &baseline).unwrap(), tag);
            let encoded = encode_event(&event).unwrap();
            assert_eq!(encoded[8], tag);
            // The negotiated encoding carries every field: nothing is downgraded.
            assert_eq!(decode_event(&encoded).unwrap(), event);
        }
        ensure_publishable(
            &[settings_event(None, true), MetaEvent::RemoveAttr { ino: 7 }],
            &baseline,
        )
        .unwrap();
    }

    #[test]
    fn publication_refuses_an_event_the_active_protocol_cannot_represent() {
        let policy = policy_without_event_codec();
        for event in [
            settings_event(None, false),
            settings_event(None, true),
            MetaEvent::RemoveAttr { ino: 7 },
        ] {
            assert!(matches!(
                negotiate_wire_tag(&event, &policy),
                Err(MetaError::VolumeProtocolRejected(
                    ProtocolRejection::EventNotRepresentable {
                        kind,
                        capability: CAP_EVENT_CODEC_V1,
                    }
                )) if kind == event.kind()
            ));
        }
        // Pending events authored under the old policy are checked against the
        // current one at publication; the caller keeps them unpublished.
        let pending = vec![MetaEvent::RemoveAttr { ino: 7 }, settings_event(None, true)];
        let before = pending.clone();
        assert!(matches!(
            ensure_publishable(&pending, &policy),
            Err(MetaError::VolumeProtocolRejected(
                ProtocolRejection::EventNotRepresentable { .. }
            ))
        ));
        assert_eq!(pending, before);
        ensure_publishable(&pending, &VolumeProtocol::baseline()).unwrap();
    }

    #[test]
    fn every_registered_tag_names_a_declared_read_capability() {
        let current = crate::compat::ClientCapabilities::current();
        for &(tag, _, capability) in EVENT_CODECS {
            assert!(current.read.contains(capability), "tag {tag}: {capability}");
        }
    }
}

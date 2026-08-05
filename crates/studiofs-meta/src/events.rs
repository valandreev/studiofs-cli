use crate::engine::{MetaError, MetaResult};
use crate::types::{DirEntryValue, InodeAttr, MetadataSnapshot, SnapshotState};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const META_EVENT_WIRE_MAGIC: &[u8] = b"SFS-EVT";
const META_EVENT_WIRE_VERSION: u8 = 1;
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
const TAG_CREATE_SNAPSHOT: u8 = 16;
const TAG_UPDATE_SNAPSHOT_STATE: u8 = 17;

// These tag values are part of the persisted format: append new values, never
// reorder or reuse an existing value.

/// Represents a single mutation to the metadata state.
/// These events are appended to a local journal and eventually pushed to S3
/// to achieve Event Sourcing and Eventual Consistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaEvent {
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

    // ─── Snapshots ───────────────────────────────────────────────────────────
    /// Create or update snapshot definition.
    CreateSnapshot {
        /// Snapshot descriptor to persist.
        snapshot: MetadataSnapshot,
    },

    /// Update snapshot replication state.
    UpdateSnapshotState {
        /// Snapshot id to update.
        id: String,
        /// New replication state for the snapshot.
        state: SnapshotState,
    },
}

impl MetaEvent {
    /// Return a stable lowercase identifier for this event kind.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SetAttr { .. } => "set_attr",
            Self::RemoveAttr { .. } => "remove_attr",
            Self::AddEntry { .. } => "add_entry",
            Self::RemoveEntry { .. } => "remove_entry",
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
            Self::CreateSnapshot { .. } => "create_snapshot",
            Self::UpdateSnapshotState { .. } => "update_snapshot_state",
        }
    }

    fn wire_tag(&self) -> u8 {
        match self {
            Self::SetAttr { .. } => TAG_SET_ATTR,
            Self::RemoveAttr { .. } => TAG_REMOVE_ATTR,
            Self::AddEntry { .. } => TAG_ADD_ENTRY,
            Self::RemoveEntry { .. } => TAG_REMOVE_ENTRY,
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
            Self::CreateSnapshot { .. } => TAG_CREATE_SNAPSHOT,
            Self::UpdateSnapshotState { .. } => TAG_UPDATE_SNAPSHOT_STATE,
        }
    }

    /// Return `true` when this event mutates the directory namespace.
    pub fn is_namespace_mutation(&self) -> bool {
        matches!(
            self,
            Self::AddEntry { .. } | Self::RemoveEntry { .. } | Self::RenameEntry { .. }
        )
    }

    /// Returns the primary Inode associated with this event, if applicable.
    /// This is useful for partitioning or logical clocks.
    pub fn primary_ino(&self) -> Option<u64> {
        match self {
            Self::SetAttr { attr } => Some(attr.ino),
            Self::RemoveAttr { ino } => Some(*ino),
            Self::AddEntry { parent, .. } => Some(*parent),
            Self::RemoveEntry { parent, .. } => Some(*parent),
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
            Self::CreateSnapshot { snapshot } => Some(snapshot.root_ino),
            Self::UpdateSnapshotState { .. } => None,
        }
    }
}

pub(crate) fn encode_event(event: &MetaEvent) -> MetaResult<Vec<u8>> {
    let payload = match event {
        MetaEvent::SetAttr { attr } => encode_event_payload(attr)?,
        MetaEvent::RemoveAttr { ino } => encode_event_payload(ino)?,
        MetaEvent::AddEntry {
            parent,
            name,
            entry,
        } => encode_event_payload(&(*parent, name, entry))?,
        MetaEvent::RemoveEntry { parent, name } => encode_event_payload(&(*parent, name))?,
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
        } => encode_event_payload(&(
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
        ))?,
        MetaEvent::RemoveRegularFileGraph { ino } => encode_event_payload(ino)?,
        MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices }
        | MetaEvent::PublishRegularFileBaseSlices { ino, chunk_slices } => {
            encode_event_payload(&(*ino, chunk_slices))?
        }
        MetaEvent::SetSymlinkTarget { ino, target } => encode_event_payload(&(*ino, target))?,
        MetaEvent::SetXattr {
            ino,
            name,
            value,
            flags,
        } => encode_event_payload(&(*ino, name, value, *flags))?,
        MetaEvent::RemoveXattr { ino, name } => encode_event_payload(&(*ino, name))?,
        MetaEvent::SetAclRule {
            ino,
            principal,
            access_level,
        } => encode_event_payload(&(*ino, principal, *access_level))?,
        MetaEvent::RemoveAclRule { ino, principal } => encode_event_payload(&(*ino, principal))?,
        MetaEvent::AddPendingDeletion {
            object_id,
            timestamp,
        } => encode_event_payload(&(*object_id, *timestamp))?,
        MetaEvent::RemovePendingDeletion { object_id } => encode_event_payload(object_id)?,
        MetaEvent::CreateSnapshot { snapshot } => encode_event_payload(snapshot)?,
        MetaEvent::UpdateSnapshotState { id, state } => encode_event_payload(&(id, *state))?,
    };

    let mut encoded = Vec::with_capacity(META_EVENT_WIRE_MAGIC.len() + 2 + payload.len());
    encoded.extend_from_slice(META_EVENT_WIRE_MAGIC);
    encoded.push(META_EVENT_WIRE_VERSION);
    encoded.push(event.wire_tag());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub(crate) fn decode_event(bytes: &[u8]) -> MetaResult<MetaEvent> {
    if bytes.first() == META_EVENT_WIRE_MAGIC.first() {
        if !bytes.starts_with(META_EVENT_WIRE_MAGIC) {
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
        if version != META_EVENT_WIRE_VERSION {
            return Err(MetaError::Serialization(format!(
                "unsupported MetaEvent wire version {version}, expected {META_EVENT_WIRE_VERSION}"
            )));
        }
        let tag = bytes[META_EVENT_WIRE_MAGIC.len() + 1];
        let payload = &bytes[header_len..];
        return decode_tagged_event(tag, payload);
    }

    decode_complete(bytes, "legacy MetaEvent")
}

pub(crate) fn decode_event_with_type(event_type: &str, bytes: &[u8]) -> MetaResult<MetaEvent> {
    let event = decode_event(bytes)?;
    if event.kind() != event_type {
        return Err(MetaError::Serialization(format!(
            "PostgreSQL MetaEvent type `{event_type}` does not match decoded `{}`",
            event.kind()
        )));
    }
    Ok(event)
}

fn encode_event_payload<T: Serialize>(payload: &T) -> MetaResult<Vec<u8>> {
    postcard::to_stdvec(payload).map_err(|error| {
        MetaError::Serialization(format!("MetaEvent payload encode failed: {error}"))
    })
}

fn decode_event_payload<T: DeserializeOwned>(tag: u8, payload: &[u8]) -> MetaResult<T> {
    decode_complete(payload, &format!("MetaEvent wire tag {tag} payload"))
}

fn decode_complete<T: DeserializeOwned>(bytes: &[u8], context: &str) -> MetaResult<T> {
    let (value, remainder) = postcard::take_from_bytes(bytes)
        .map_err(|error| MetaError::Serialization(format!("{context} decode failed: {error}")))?;
    if !remainder.is_empty() {
        return Err(MetaError::Serialization(format!(
            "{context} contains {} trailing bytes",
            remainder.len()
        )));
    }
    Ok(value)
}

fn decode_tagged_event(tag: u8, payload: &[u8]) -> MetaResult<MetaEvent> {
    match tag {
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
        TAG_CREATE_SNAPSHOT => Ok(MetaEvent::CreateSnapshot {
            snapshot: decode_event_payload(tag, payload)?,
        }),
        TAG_UPDATE_SNAPSHOT_STATE => {
            let (id, state) = decode_event_payload(tag, payload)?;
            Ok(MetaEvent::UpdateSnapshotState { id, state })
        }
        _ => Err(MetaError::Serialization(format!(
            "unknown MetaEvent wire kind tag {tag}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn event_fixtures() -> Vec<(MetaEvent, u8)> {
        let attr = sample_attr(11, FileType::RegularFile);
        let entry = DirEntryValue {
            ino: 12,
            kind: FileType::Directory,
        };
        vec![
            (MetaEvent::SetAttr { attr: attr.clone() }, TAG_SET_ATTR),
            (MetaEvent::RemoveAttr { ino: 13 }, TAG_REMOVE_ATTR),
            (
                MetaEvent::AddEntry {
                    parent: 14,
                    name: b"child".to_vec(),
                    entry: entry.clone(),
                },
                TAG_ADD_ENTRY,
            ),
            (
                MetaEvent::RemoveEntry {
                    parent: 15,
                    name: b"old".to_vec(),
                },
                TAG_REMOVE_ENTRY,
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
                TAG_RENAME_ENTRY,
            ),
            (
                MetaEvent::RemoveRegularFileGraph { ino: 19 },
                TAG_REMOVE_REGULAR_FILE_GRAPH,
            ),
            (
                MetaEvent::SetRegularFileOverlaySlices {
                    ino: 20,
                    chunk_slices: sample_slices(),
                },
                TAG_SET_REGULAR_FILE_OVERLAY_SLICES,
            ),
            (
                MetaEvent::PublishRegularFileBaseSlices {
                    ino: 21,
                    chunk_slices: sample_slices(),
                },
                TAG_PUBLISH_REGULAR_FILE_BASE_SLICES,
            ),
            (
                MetaEvent::SetSymlinkTarget {
                    ino: 22,
                    target: b"/target".to_vec(),
                },
                TAG_SET_SYMLINK_TARGET,
            ),
            (
                MetaEvent::SetXattr {
                    ino: 23,
                    name: b"user.key".to_vec(),
                    value: b"value".to_vec(),
                    flags: 1,
                },
                TAG_SET_XATTR,
            ),
            (
                MetaEvent::RemoveXattr {
                    ino: 24,
                    name: b"user.key".to_vec(),
                },
                TAG_REMOVE_XATTR,
            ),
            (
                MetaEvent::SetAclRule {
                    ino: 25,
                    principal: "alice".to_string(),
                    access_level: AccessLevel::ReadWrite,
                },
                TAG_SET_ACL_RULE,
            ),
            (
                MetaEvent::RemoveAclRule {
                    ino: 26,
                    principal: "alice".to_string(),
                },
                TAG_REMOVE_ACL_RULE,
            ),
            (
                MetaEvent::AddPendingDeletion {
                    object_id: 27,
                    timestamp: 28,
                },
                TAG_ADD_PENDING_DELETION,
            ),
            (
                MetaEvent::RemovePendingDeletion { object_id: 29 },
                TAG_REMOVE_PENDING_DELETION,
            ),
            (
                MetaEvent::CreateSnapshot {
                    snapshot: MetadataSnapshot {
                        id: "snapshot".to_string(),
                        root_ino: 30,
                        created_at: 31,
                        state: SnapshotState::Synced,
                        pinned_object_ids: vec![32],
                        snapshot_payload: b"payload".to_vec(),
                    },
                },
                TAG_CREATE_SNAPSHOT,
            ),
            (
                MetaEvent::UpdateSnapshotState {
                    id: "snapshot".to_string(),
                    state: SnapshotState::Failed,
                },
                TAG_UPDATE_SNAPSHOT_STATE,
            ),
        ]
    }

    #[test]
    fn remove_attr_new_wire_fixture_is_independent_of_enum_ordinal() {
        let event = MetaEvent::RemoveAttr { ino: 42 };

        assert_eq!(
            encode_event(&event).expect("encode event"),
            b"SFS-EVT\x01\x02\x2a"
        );
    }

    #[test]
    fn every_event_kind_roundtrips_with_its_stable_wire_tag() {
        for (event, expected_tag) in event_fixtures() {
            let encoded = encode_event(&event).expect("encode event fixture");
            assert_eq!(
                encoded[..META_EVENT_WIRE_MAGIC.len()],
                *META_EVENT_WIRE_MAGIC
            );
            assert_eq!(
                encoded[META_EVENT_WIRE_MAGIC.len()],
                META_EVENT_WIRE_VERSION
            );
            assert_eq!(encoded[META_EVENT_WIRE_MAGIC.len() + 1], expected_tag);

            let decoded = decode_event(&encoded).expect("decode event fixture");
            assert_eq!(decoded.kind(), event.kind());
            assert_eq!(format!("{decoded:?}"), format!("{event:?}"));
        }
    }

    #[test]
    fn legacy_remove_attr_fixture_still_decodes() {
        let event = decode_event(&[1_u8, 42_u8]).expect("decode legacy event");

        assert!(matches!(event, MetaEvent::RemoveAttr { ino: 42 }));
    }

    #[test]
    fn unknown_wire_version_is_rejected() {
        let error = decode_event(b"SFS-EVT\x02\x02\x2a").expect_err("unknown version");

        assert!(
            error
                .to_string()
                .contains("unsupported MetaEvent wire version")
        );
    }

    #[test]
    fn unknown_wire_tag_is_rejected() {
        let error = decode_event(b"SFS-EVT\x01\xff\x00").expect_err("unknown tag");

        assert!(
            error
                .to_string()
                .contains("unknown MetaEvent wire kind tag")
        );
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

        let unknown = decode_event_with_type("future_event", &encoded)
            .expect_err("unknown PostgreSQL event type");
        assert!(unknown.to_string().contains("does not match decoded"));

        let mismatch = decode_event_with_type("set_attr", &encoded)
            .expect_err("mismatched PostgreSQL event type");
        assert!(mismatch.to_string().contains("does not match decoded"));
    }
}

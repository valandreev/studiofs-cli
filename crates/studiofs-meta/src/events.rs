use crate::engine::{MetaError, MetaResult};
use crate::types::{DirEntryValue, InodeAttr};
use serde::Serialize;
use serde::de::DeserializeOwned;

const META_EVENT_WIRE_MAGIC: [u8; 7] = *b"SFS-EVT";
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

// These tag values are part of the persisted format: append new values, never
// reorder or reuse an existing value.

/// Represents a single mutation to the metadata state.
/// These events are appended to a local journal and eventually pushed to S3
/// to achieve Event Sourcing and Eventual Consistency.
#[derive(Debug, Clone, PartialEq, Eq)]
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
        MetaEvent::SetAttr { attr } => encode_event_envelope(tag, attr),
        MetaEvent::RemoveAttr { ino } => encode_event_envelope(tag, ino),
        MetaEvent::AddEntry {
            parent,
            name,
            entry,
        } => encode_event_envelope(tag, &(*parent, name, entry)),
        MetaEvent::RemoveEntry { parent, name } => encode_event_envelope(tag, &(*parent, name)),
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
    let expected_tag = wire_tag_for_kind(event_type).ok_or_else(|| {
        MetaError::Serialization(format!("unknown PostgreSQL MetaEvent type `{event_type}`"))
    })?;
    let (tag, payload) = decode_event_wire_parts(bytes)?;
    if tag != expected_tag {
        return Err(MetaError::Serialization(format!(
            "PostgreSQL MetaEvent type `{event_type}` does not match decoded wire tag {tag}"
        )));
    }
    decode_tagged_event(tag, payload)
}

fn wire_tag_for_kind(kind: &str) -> Option<u8> {
    match kind {
        "set_attr" => Some(TAG_SET_ATTR),
        "remove_attr" => Some(TAG_REMOVE_ATTR),
        "add_entry" => Some(TAG_ADD_ENTRY),
        "remove_entry" => Some(TAG_REMOVE_ENTRY),
        "rename_entry" => Some(TAG_RENAME_ENTRY),
        "remove_regular_file_graph" => Some(TAG_REMOVE_REGULAR_FILE_GRAPH),
        "set_regular_file_overlay_slices" => Some(TAG_SET_REGULAR_FILE_OVERLAY_SLICES),
        "publish_regular_file_base_slices" => Some(TAG_PUBLISH_REGULAR_FILE_BASE_SLICES),
        "set_symlink_target" => Some(TAG_SET_SYMLINK_TARGET),
        "set_xattr" => Some(TAG_SET_XATTR),
        "remove_xattr" => Some(TAG_REMOVE_XATTR),
        "set_acl_rule" => Some(TAG_SET_ACL_RULE),
        "remove_acl_rule" => Some(TAG_REMOVE_ACL_RULE),
        "add_pending_deletion" => Some(TAG_ADD_PENDING_DELETION),
        "remove_pending_deletion" => Some(TAG_REMOVE_PENDING_DELETION),
        _ => None,
    }
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
        if version != META_EVENT_WIRE_VERSION {
            return Err(MetaError::Serialization(format!(
                "unsupported MetaEvent wire version {version}, expected {META_EVENT_WIRE_VERSION}"
            )));
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
    let envelope = (META_EVENT_WIRE_MAGIC, META_EVENT_WIRE_VERSION, tag, payload);
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

        assert!(
            error
                .to_string()
                .contains("unknown legacy MetaEvent ordinal 17"),
            "unexpected error: {error}"
        );
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
        assert!(
            unknown
                .to_string()
                .contains("unknown PostgreSQL MetaEvent type")
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
}

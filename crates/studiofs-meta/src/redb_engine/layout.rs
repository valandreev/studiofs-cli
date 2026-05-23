use super::*;

// ─── Table Definitions ──────────────────────────────────────────────────────

/// Format info: "format" -> JSON bytes
pub(super) const SETTINGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("settings");

/// Counters: counter_name -> u64 (as bytes)
pub(super) const COUNTERS_TABLE: TableDefinition<&str, u64> = TableDefinition::new("counters");

/// Inodes: ino (u64) -> InodeAttr (postcard)
pub(super) const INODES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("inodes");

/// Remote-visible base attrs imported from checkpoint + foreign PG apply.
pub(super) const BASE_INODES_TABLE: TableDefinition<u64, &[u8]> =
    TableDefinition::new("base_inodes");

/// Explicit inode lifecycle metadata for locally durable regular files.
pub(super) const INODE_LIFECYCLE_TABLE: TableDefinition<u64, &[u8]> =
    TableDefinition::new("inode_lifecycle");

/// Local tombstones hiding remote-visible base attrs.
pub(super) const INODE_TOMBSTONES_TABLE: TableDefinition<u64, u8> =
    TableDefinition::new("inode_tombstones");

/// Directory entries: (parent_ino, name) as bytes -> DirEntryValue (postcard)
pub(super) const DENTRIES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("dentries");

/// Reverse index for serving dentries: (ino, parent_ino, name) -> marker.
pub(super) const DENTRIES_BY_INODE_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("dentries_by_inode");

/// Remote-visible base dentries imported from checkpoint + foreign PG apply.
pub(super) const BASE_DENTRIES_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("base_dentries");

/// Reverse index for base dentries: (ino, parent_ino, name) -> marker.
pub(super) const BASE_DENTRIES_BY_INODE_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("base_dentries_by_inode");

/// Local tombstones hiding remote-visible base dentries.
pub(super) const DENTRY_TOMBSTONES_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("dentry_tombstones");

/// Local overlay regular-file chunk rows: (ino, chunk_id) -> RegularFileChunkState.
pub(super) const REGULAR_FILE_OVERLAY_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("regular_file_overlay");

/// Remote-visible base regular-file chunk rows: (ino, chunk_id) -> RegularFileChunkState.
pub(super) const REGULAR_FILE_BASE_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("regular_file_base");

/// Pending overlay chunk compaction work: (ino, chunk_id) -> marker.
pub(super) const PENDING_REGULAR_FILE_COMPACTION_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("pending_regular_file_compaction");

pub(super) type RegularFileChunkSignature = Vec<(SliceRange, Option<PayloadRef>)>;
pub(super) type RegularFileGraphSignature = BTreeMap<ChunkId, RegularFileChunkSignature>;

/// Pending Deletions (GC): object_id (u64) -> deletion_timestamp (i64)
pub(super) const PENDING_DELETIONS_TABLE: TableDefinition<u64, i64> =
    TableDefinition::new("pending_deletions");

/// Pending create intents: ino -> CreateIntent (postcard)
pub(super) const PENDING_CREATES_TABLE: TableDefinition<u64, &[u8]> =
    TableDefinition::new("pending_creates");

/// Deferred deletes (open-unlink orphans): ino -> DeferredDeleteRecord (postcard)
pub(super) const DEFERRED_DELETES_TABLE: TableDefinition<u64, &[u8]> =
    TableDefinition::new("deferred_deletes");

/// Generic undo-log records: op_id -> UndoLogRecord (postcard)
pub(super) const UNDO_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("undo_log");
pub(super) const NEXT_UNDO_OP_ID_COUNTER: &str = "next_undo_op_id";

/// Symlink targets: ino (u64) -> target path (&[u8])
pub(super) const SYMLINKS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("symlinks");

/// Remote-visible base symlink targets imported from checkpoint + foreign apply.
pub(super) const BASE_SYMLINKS_TABLE: TableDefinition<u64, &[u8]> =
    TableDefinition::new("base_symlinks");

/// Local tombstones hiding remote-visible base symlink targets.
pub(super) const SYMLINK_TOMBSTONES_TABLE: TableDefinition<u64, u8> =
    TableDefinition::new("symlink_tombstones");

/// Extended attributes: (ino(8) + name_bytes) -> value_bytes
pub(super) const XATTRS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("xattrs");

/// Remote-visible base xattrs imported from checkpoint + foreign apply.
pub(super) const BASE_XATTRS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("base_xattrs");

/// Local tombstones hiding remote-visible base xattrs.
pub(super) const XATTR_TOMBSTONES_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("xattr_tombstones");

/// ACL rules: (ino(8) + principal_bytes) -> access level (u8).
pub(super) const ACL_RULES_TABLE: TableDefinition<&[u8], u8> = TableDefinition::new("acl_rules");

/// Remote-visible base ACL rules imported from checkpoint + foreign apply.
pub(super) const BASE_ACL_RULES_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("base_acl_rules");

/// Local tombstones hiding remote-visible base ACL rules.
pub(super) const ACL_RULE_TOMBSTONES_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("acl_rule_tombstones");

/// Event Log: sequence_id (u64) -> MetaEvent (postcard)
pub(super) const EVENT_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("event_log");

/// Snapshot definitions: snapshot_id (uuid text) -> MetadataSnapshot (postcard)
pub(super) const SNAPSHOTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshots");
pub(super) const REMOTE_NAMESPACE_VISIBILITY_FILTER_SETTING: &str =
    "remote_namespace_visibility_filter";
pub(super) const DENTRY_BY_INODE_INDEX_VERSION_SETTING: &str = "dentry_by_inode_index_version";
pub(super) const CURRENT_DENTRY_BY_INODE_INDEX_VERSION: &[u8] = b"1";

/// Serializable full snapshot of redb metadata state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct RedbSnapshot {
    /// Settings table rows as `(key, raw_value)` pairs.
    pub settings: Vec<(String, Vec<u8>)>,
    /// Counter rows as `(name, value)` pairs.
    pub counters: Vec<(String, u64)>,
    /// Inode rows as `(ino, encoded_attr)` pairs.
    pub inodes: Vec<(u64, Vec<u8>)>,
    /// Dentry rows as `(encoded_key, encoded_value)` pairs.
    pub dentries: Vec<(Vec<u8>, Vec<u8>)>,
    #[serde(default)]
    /// Overlay regular-file chunk rows as `(ino, encoded_state)` pairs.
    pub regular_file_overlay: Vec<(u64, Vec<u8>)>,
    #[serde(default)]
    /// Base regular-file chunk rows as `(ino, encoded_state)` pairs.
    pub regular_file_base: Vec<(u64, Vec<u8>)>,
    /// Pending deletion rows as `(object_id, timestamp)` pairs.
    pub pending_deletions: Vec<(u64, i64)>,
    /// Symlink rows as `(ino, target_bytes)` pairs.
    pub symlinks: Vec<(u64, Vec<u8>)>,
    /// Xattr rows as `(encoded_key, decoded_value)` pairs.
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    /// ACL rows as `(encoded_key, access_level)` pairs.
    pub acl_rules: Vec<(Vec<u8>, u8)>,
    /// Local event-log rows as `(sequence, encoded_event)` pairs.
    pub event_log: Vec<(u64, Vec<u8>)>,
    /// Snapshot-definition rows as `(snapshot_id, encoded_snapshot)` pairs.
    pub snapshots: Vec<(String, Vec<u8>)>,
}

pub(crate) fn encode_dentry_key(parent: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + name.len());
    key.extend_from_slice(&parent.to_le_bytes());
    key.extend_from_slice(name);
    key
}

pub(super) fn encode_dentry_by_inode_key(ino: Ino, parent: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + name.len());
    key.extend_from_slice(&ino.to_le_bytes());
    key.extend_from_slice(&parent.to_le_bytes());
    key.extend_from_slice(name);
    key
}

fn invalid_redb_key_len(key_kind: &str, expected: &str, actual: usize) -> MetaError {
    MetaError::Storage(format!(
        "invalid redb {key_kind} key length: expected {expected}, got {actual}"
    ))
}

pub(super) fn decode_inode_prefix(key: &[u8], key_kind: &str) -> MetaResult<Ino> {
    let prefix = key
        .get(..8)
        .ok_or_else(|| invalid_redb_key_len(key_kind, "at least 8 bytes", key.len()))?;
    let mut ino_bytes = [0_u8; 8];
    ino_bytes.copy_from_slice(prefix);
    Ok(u64::from_le_bytes(ino_bytes))
}

/// Decode a dentry key back to (parent_ino, name).
pub(super) fn decode_dentry_key(key: &[u8]) -> MetaResult<(Ino, &[u8])> {
    let parent = decode_inode_prefix(key, "dentry")?;
    let name = key
        .get(8..)
        .ok_or_else(|| invalid_redb_key_len("dentry", "at least 8 bytes", key.len()))?;
    Ok((parent, name))
}

pub(super) fn decode_dentry_by_inode_key(key: &[u8]) -> MetaResult<(Ino, Ino, &[u8])> {
    let ino = decode_inode_prefix(key, "dentry-by-inode")?;
    let parent_bytes = key
        .get(8..16)
        .ok_or_else(|| invalid_redb_key_len("dentry-by-inode", "at least 16 bytes", key.len()))?;
    let mut parent = [0_u8; 8];
    parent.copy_from_slice(parent_bytes);
    let name = key
        .get(16..)
        .ok_or_else(|| invalid_redb_key_len("dentry-by-inode", "at least 16 bytes", key.len()))?;
    Ok((ino, u64::from_le_bytes(parent), name))
}

/// The prefix for scanning all dentries of a parent inode.
pub(super) fn dentry_prefix(parent: Ino) -> [u8; 8] {
    parent.to_le_bytes()
}

pub(super) fn dentry_by_inode_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

/// Encode a regular-file chunk key: ino (8 bytes LE) + chunk_id (8 bytes LE).
pub(super) fn encode_regular_file_chunk_key(ino: Ino, chunk_id: ChunkId) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&ino.to_le_bytes());
    buf[8..16].copy_from_slice(&chunk_id.0.to_le_bytes());
    buf
}

/// Decode a regular-file chunk key back to (ino, chunk_id).
pub(super) fn decode_regular_file_chunk_key(key: &[u8]) -> MetaResult<(Ino, ChunkId)> {
    if key.len() != 16 {
        return Err(invalid_redb_key_len(
            "regular-file chunk",
            "exactly 16 bytes",
            key.len(),
        ));
    }

    let ino = decode_inode_prefix(key, "regular-file chunk")?;
    let mut chunk_bytes = [0_u8; 8];
    chunk_bytes.copy_from_slice(&key[8..16]);
    Ok((ino, ChunkId(u64::from_le_bytes(chunk_bytes))))
}

/// The prefix for scanning all chunk rows of an inode.
pub(super) fn regular_file_chunk_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

/// Encode an xattr key: ino (8 bytes LE) + name bytes.
pub(crate) fn encode_xattr_key(ino: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + name.len());
    key.extend_from_slice(&ino.to_le_bytes());
    key.extend_from_slice(name);
    key
}

/// The prefix for scanning all xattrs of an inode.
pub(super) fn xattr_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

pub(super) fn xattr_name_from_key(key: &[u8]) -> &[u8] {
    &key[8..]
}

pub(super) const RESOURCE_FORK_XATTR_NAME: &[u8] = b"com.apple.ResourceFork";
pub(super) const RESOURCE_FORK_ENCODING_MAGIC: &[u8; 4] = b"SFRF";
pub(super) const RESOURCE_FORK_ENCODING_VERSION: u8 = 1;
pub(super) const RESOURCE_FORK_ENCODING_SCHEME_RLE: u8 = 1;
pub(super) const RESOURCE_FORK_REPEAT_RUN_THRESHOLD: usize = 4;

pub(super) fn resource_fork_checksum(bytes: &[u8]) -> u32 {
    // FNV-1a checksum for light framing validation.
    let mut hash = 0x811c_9dc5u32;
    for byte in bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

pub(super) fn rle_compress(input: &[u8]) -> Vec<u8> {
    if input.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(input.len());
    let mut i = 0usize;
    while i < input.len() {
        let run_byte = input[i];
        let mut run_len = 1usize;
        while i + run_len < input.len() && input[i + run_len] == run_byte && run_len < 128 {
            run_len += 1;
        }

        if run_len >= RESOURCE_FORK_REPEAT_RUN_THRESHOLD {
            out.push(0x80 | (run_len as u8 - 1));
            out.push(run_byte);
            i += run_len;
            continue;
        }

        let literal_start = i;
        let mut literal_len = run_len;
        i += run_len;
        while i < input.len() && literal_len < 128 {
            let probe_byte = input[i];
            let mut probe_run = 1usize;
            while i + probe_run < input.len()
                && input[i + probe_run] == probe_byte
                && probe_run < 128
            {
                probe_run += 1;
            }
            if probe_run >= RESOURCE_FORK_REPEAT_RUN_THRESHOLD {
                break;
            }
            let take = probe_run.min(128 - literal_len);
            literal_len += take;
            i += take;
        }
        out.push(literal_len as u8 - 1);
        out.extend_from_slice(&input[literal_start..literal_start + literal_len]);
    }

    out
}

pub(super) fn rle_decompress(input: &[u8], expected_len: usize) -> Option<Vec<u8>> {
    let initial_capacity = expected_len.min(64 * 1024);
    let mut out = Vec::with_capacity(initial_capacity);
    let mut i = 0usize;
    while i < input.len() {
        let token = input[i];
        i += 1;
        let len = ((token & 0x7f) as usize) + 1;
        if token & 0x80 != 0 {
            let value = *input.get(i)?;
            i += 1;
            out.extend(std::iter::repeat_n(value, len));
        } else {
            let end = i.checked_add(len)?;
            let literal = input.get(i..end)?;
            out.extend_from_slice(literal);
            i = end;
        }
        if out.len() > expected_len {
            return None;
        }
    }
    if out.len() == expected_len {
        Some(out)
    } else {
        None
    }
}

pub(super) fn encode_resource_fork_value_for_storage(value: &[u8]) -> Vec<u8> {
    let Ok(len_u32) = u32::try_from(value.len()) else {
        return value.to_vec();
    };
    let compressed = rle_compress(value);
    let header_len = 4 + 1 + 1 + 4 + 4;
    if compressed.len() + header_len >= value.len() {
        return value.to_vec();
    }

    let mut framed = Vec::with_capacity(header_len + compressed.len());
    framed.extend_from_slice(RESOURCE_FORK_ENCODING_MAGIC);
    framed.push(RESOURCE_FORK_ENCODING_VERSION);
    framed.push(RESOURCE_FORK_ENCODING_SCHEME_RLE);
    framed.extend_from_slice(&len_u32.to_le_bytes());
    framed.extend_from_slice(&resource_fork_checksum(value).to_le_bytes());
    framed.extend_from_slice(&compressed);
    framed
}

pub(super) fn decode_resource_fork_value_from_storage(stored: &[u8]) -> Vec<u8> {
    let header_len = 4 + 1 + 1 + 4 + 4;
    if stored.len() < header_len {
        return stored.to_vec();
    }
    if &stored[..4] != RESOURCE_FORK_ENCODING_MAGIC {
        return stored.to_vec();
    }
    if stored[4] != RESOURCE_FORK_ENCODING_VERSION || stored[5] != RESOURCE_FORK_ENCODING_SCHEME_RLE
    {
        return stored.to_vec();
    }
    let expected_len = u32::from_le_bytes([stored[6], stored[7], stored[8], stored[9]]) as usize;
    let expected_checksum = u32::from_le_bytes([stored[10], stored[11], stored[12], stored[13]]);
    let payload = &stored[header_len..];
    match rle_decompress(payload, expected_len) {
        Some(decoded) if resource_fork_checksum(&decoded) == expected_checksum => decoded,
        _ => stored.to_vec(),
    }
}

pub(super) fn encode_xattr_value_for_storage<'a>(name: &[u8], value: &'a [u8]) -> Cow<'a, [u8]> {
    if name == RESOURCE_FORK_XATTR_NAME {
        Cow::Owned(encode_resource_fork_value_for_storage(value))
    } else {
        Cow::Borrowed(value)
    }
}

pub(super) fn decode_xattr_value_from_storage(name: &[u8], stored: &[u8]) -> Vec<u8> {
    if name == RESOURCE_FORK_XATTR_NAME {
        decode_resource_fork_value_from_storage(stored)
    } else {
        stored.to_vec()
    }
}

pub(super) fn decode_xattr_value_from_storage_owned(name: &[u8], stored: Vec<u8>) -> Vec<u8> {
    if name == RESOURCE_FORK_XATTR_NAME {
        decode_resource_fork_value_from_storage(stored.as_slice())
    } else {
        stored
    }
}

pub(crate) fn encode_acl_rule_key(ino: Ino, principal: &str) -> Vec<u8> {
    let principal_bytes = principal.as_bytes();
    let mut key = Vec::with_capacity(8 + principal_bytes.len());
    key.extend_from_slice(&ino.to_le_bytes());
    key.extend_from_slice(principal_bytes);
    key
}

pub(super) fn acl_rule_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

// ─── Postcard helpers ────────────────────────────────────────────────────────

pub(super) fn encode<T: serde::Serialize>(val: &T) -> MetaResult<Vec<u8>> {
    postcard::to_stdvec(val).map_err(|e| MetaError::Serialization(e.to_string()))
}

pub(super) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> MetaResult<T> {
    postcard::from_bytes(bytes).map_err(|e| MetaError::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_dentry_key_rejects_short_key() {
        let err = decode_dentry_key(&[1, 2, 3, 4]).expect_err("short key must fail");
        assert!(matches!(
            err,
            MetaError::Storage(message)
                if message.contains("invalid redb dentry key length")
        ));
    }

    #[test]
    fn decode_regular_file_chunk_key_rejects_invalid_length() {
        let err = decode_regular_file_chunk_key(&[0_u8; 15]).expect_err("short key must fail");
        assert!(matches!(
            err,
            MetaError::Storage(message)
                if message.contains("invalid redb regular-file chunk key length")
        ));
    }
}

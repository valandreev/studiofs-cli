//! redb implementation of MetaEngine.
//!
//! Uses redb as an embedded ACID KV store for all filesystem metadata.

use crate::engine::*;
use crate::types::*;
use redb::{Database, ReadOnlyDatabase, ReadableDatabase, ReadableTable, TableDefinition};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info};

// ─── Table Definitions ──────────────────────────────────────────────────────

/// Format info: "format" -> JSON bytes
const SETTINGS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("settings");

/// Counters: counter_name -> u64 (as bytes)
const COUNTERS_TABLE: TableDefinition<&str, u64> = TableDefinition::new("counters");

/// Inodes: ino (u64) -> InodeAttr (postcard)
const INODES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("inodes");

/// Remote-visible base attrs imported from checkpoint + foreign PG apply.
const BASE_INODES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("base_inodes");

/// Local tombstones hiding remote-visible base attrs.
const INODE_TOMBSTONES_TABLE: TableDefinition<u64, u8> = TableDefinition::new("inode_tombstones");

/// Directory entries: (parent_ino, name) as bytes -> DirEntryValue (postcard)
const DENTRIES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("dentries");

/// Remote-visible base dentries imported from checkpoint + foreign PG apply.
const BASE_DENTRIES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("base_dentries");

/// Local tombstones hiding remote-visible base dentries.
const DENTRY_TOMBSTONES_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("dentry_tombstones");

/// File Blocks: (ino, block_index) -> BlockRef (postcard)
/// Key encoding: ino (8 bytes LE) + block_index (4 bytes LE)
const FILE_BLOCKS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_blocks");

/// Remote-visible committed file blocks: (ino, block_index) -> BlockRef.
const COMMITTED_FILE_BLOCKS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("committed_file_blocks");

/// Local overlay regular-file chunk rows: (ino, chunk_id) -> RegularFileChunkState.
const REGULAR_FILE_OVERLAY_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("regular_file_overlay");

/// Remote-visible base regular-file chunk rows: (ino, chunk_id) -> RegularFileChunkState.
const REGULAR_FILE_BASE_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("regular_file_base");

/// Pending overlay chunk compaction work: (ino, chunk_id) -> marker.
const PENDING_REGULAR_FILE_COMPACTION_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("pending_regular_file_compaction");

type RegularFileChunkSignature = Vec<(SliceRange, Option<BlockRef>)>;
type RegularFileGraphSignature = BTreeMap<ChunkId, RegularFileChunkSignature>;

/// Pending Deletions (GC): object_id (u64) -> deletion_timestamp (i64)
const PENDING_DELETIONS_TABLE: TableDefinition<u64, i64> =
    TableDefinition::new("pending_deletions");

/// Pending create intents: ino -> CreateIntent (postcard)
const PENDING_CREATES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("pending_creates");

/// Deferred deletes (open-unlink orphans): ino -> DeferredDeleteRecord (postcard)
const DEFERRED_DELETES_TABLE: TableDefinition<u64, &[u8]> =
    TableDefinition::new("deferred_deletes");

/// Generic undo-log records: op_id -> UndoLogRecord (postcard)
const UNDO_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("undo_log");
const NEXT_UNDO_OP_ID_COUNTER: &str = "next_undo_op_id";

/// Symlink targets: ino (u64) -> target path (&[u8])
const SYMLINKS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("symlinks");

/// Remote-visible base symlink targets imported from checkpoint + foreign apply.
const BASE_SYMLINKS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("base_symlinks");

/// Local tombstones hiding remote-visible base symlink targets.
const SYMLINK_TOMBSTONES_TABLE: TableDefinition<u64, u8> =
    TableDefinition::new("symlink_tombstones");

/// Extended attributes: (ino(8) + name_bytes) -> value_bytes
const XATTRS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("xattrs");

/// Remote-visible base xattrs imported from checkpoint + foreign apply.
const BASE_XATTRS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("base_xattrs");

/// Local tombstones hiding remote-visible base xattrs.
const XATTR_TOMBSTONES_TABLE: TableDefinition<&[u8], u8> = TableDefinition::new("xattr_tombstones");

/// ACL rules: (ino(8) + principal_bytes) -> access level (u8).
const ACL_RULES_TABLE: TableDefinition<&[u8], u8> = TableDefinition::new("acl_rules");

/// Remote-visible base ACL rules imported from checkpoint + foreign apply.
const BASE_ACL_RULES_TABLE: TableDefinition<&[u8], u8> = TableDefinition::new("base_acl_rules");

/// Local tombstones hiding remote-visible base ACL rules.
const ACL_RULE_TOMBSTONES_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("acl_rule_tombstones");

const REGULAR_FILE_MIGRATION_NAME: &str = "regular_files_v2_to_v3";
const MIGRATION_STATUS_STARTED: u8 = 1;
const MIGRATION_STATUS_COMPLETED: u8 = 2;

/// Event Log: sequence_id (u64) -> MetaEvent (postcard)
const EVENT_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("event_log");

/// Snapshot definitions: snapshot_id (uuid text) -> MetadataSnapshot (postcard)
const SNAPSHOTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshots");

/// Migration status: migration_name -> status (u8)
/// Used to track migration progress and detect interrupted migrations
const MIGRATION_STATUS_TABLE: TableDefinition<&str, u8> = TableDefinition::new("migration_status");

/// Serializable full snapshot of redb metadata state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct RedbSnapshot {
    pub settings: Vec<(String, Vec<u8>)>,
    pub counters: Vec<(String, u64)>,
    pub inodes: Vec<(u64, Vec<u8>)>,
    pub dentries: Vec<(Vec<u8>, Vec<u8>)>,
    pub file_blocks: Vec<(Vec<u8>, Vec<u8>)>,
    pub committed_file_blocks: Vec<(Vec<u8>, Vec<u8>)>,
    #[serde(default)]
    pub regular_file_overlay: Vec<(u64, Vec<u8>)>,
    #[serde(default)]
    pub regular_file_base: Vec<(u64, Vec<u8>)>,
    pub pending_deletions: Vec<(u64, i64)>,
    pub symlinks: Vec<(u64, Vec<u8>)>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    pub acl_rules: Vec<(Vec<u8>, u8)>,
    pub event_log: Vec<(u64, Vec<u8>)>,
    pub snapshots: Vec<(String, Vec<u8>)>,
}

// ─── Key Encoding Helpers ────────────────────────────────────────────────────

/// Encode a dentry key: parent_ino (8 bytes LE) + name bytes.
fn encode_dentry_key(parent: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + name.len());
    key.extend_from_slice(&parent.to_le_bytes());
    key.extend_from_slice(name);
    key
}

/// Decode a dentry key back to (parent_ino, name).
fn decode_dentry_key(key: &[u8]) -> (Ino, &[u8]) {
    let parent = u64::from_le_bytes(key[..8].try_into().unwrap());
    let name = &key[8..];
    (parent, name)
}

/// The prefix for scanning all dentries of a parent inode.
fn dentry_prefix(parent: Ino) -> [u8; 8] {
    parent.to_le_bytes()
}

/// Encode a file block key: ino (8 bytes LE) + block_index (4 bytes LE).
fn encode_file_block_key(ino: Ino, block_index: u32) -> [u8; 12] {
    let mut buf = [0u8; 12];
    buf[..8].copy_from_slice(&ino.to_le_bytes());
    buf[8..12].copy_from_slice(&block_index.to_le_bytes());
    buf
}

/// Decode a file block key back to (ino, block_index).
fn decode_file_block_key(key: &[u8]) -> (Ino, u32) {
    let ino = u64::from_le_bytes(key[..8].try_into().unwrap());
    let block_index = u32::from_le_bytes(key[8..12].try_into().unwrap());
    (ino, block_index)
}

/// The prefix for scanning all blocks of an inode.
fn file_block_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

/// Encode a regular-file chunk key: ino (8 bytes LE) + chunk_id (8 bytes LE).
fn encode_regular_file_chunk_key(ino: Ino, chunk_id: ChunkId) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&ino.to_le_bytes());
    buf[8..16].copy_from_slice(&chunk_id.0.to_le_bytes());
    buf
}

/// Decode a regular-file chunk key back to (ino, chunk_id).
fn decode_regular_file_chunk_key(key: &[u8]) -> (Ino, ChunkId) {
    let ino = u64::from_le_bytes(key[..8].try_into().unwrap());
    let chunk_id = u64::from_le_bytes(key[8..16].try_into().unwrap());
    (ino, ChunkId(chunk_id))
}

/// The prefix for scanning all chunk rows of an inode.
fn regular_file_chunk_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

/// Encode an xattr key: ino (8 bytes LE) + name bytes.
fn encode_xattr_key(ino: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + name.len());
    key.extend_from_slice(&ino.to_le_bytes());
    key.extend_from_slice(name);
    key
}

/// The prefix for scanning all xattrs of an inode.
fn xattr_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

fn xattr_name_from_key(key: &[u8]) -> &[u8] {
    &key[8..]
}

const RESOURCE_FORK_XATTR_NAME: &[u8] = b"com.apple.ResourceFork";
const RESOURCE_FORK_ENCODING_MAGIC: &[u8; 4] = b"SFRF";
const RESOURCE_FORK_ENCODING_VERSION: u8 = 1;
const RESOURCE_FORK_ENCODING_SCHEME_RLE: u8 = 1;
const RESOURCE_FORK_REPEAT_RUN_THRESHOLD: usize = 4;

fn resource_fork_checksum(bytes: &[u8]) -> u32 {
    // FNV-1a checksum for light framing validation.
    let mut hash = 0x811c_9dc5u32;
    for byte in bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

fn rle_compress(input: &[u8]) -> Vec<u8> {
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

fn rle_decompress(input: &[u8], expected_len: usize) -> Option<Vec<u8>> {
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

fn encode_resource_fork_value_for_storage(value: &[u8]) -> Vec<u8> {
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

fn decode_resource_fork_value_from_storage(stored: &[u8]) -> Vec<u8> {
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

fn encode_xattr_value_for_storage<'a>(name: &[u8], value: &'a [u8]) -> Cow<'a, [u8]> {
    if name == RESOURCE_FORK_XATTR_NAME {
        Cow::Owned(encode_resource_fork_value_for_storage(value))
    } else {
        Cow::Borrowed(value)
    }
}

fn decode_xattr_value_from_storage(name: &[u8], stored: &[u8]) -> Vec<u8> {
    if name == RESOURCE_FORK_XATTR_NAME {
        decode_resource_fork_value_from_storage(stored)
    } else {
        stored.to_vec()
    }
}

fn decode_xattr_value_from_storage_owned(name: &[u8], stored: Vec<u8>) -> Vec<u8> {
    if name == RESOURCE_FORK_XATTR_NAME {
        decode_resource_fork_value_from_storage(stored.as_slice())
    } else {
        stored
    }
}

fn encode_acl_rule_key(ino: Ino, principal: &str) -> Vec<u8> {
    let principal_bytes = principal.as_bytes();
    let mut key = Vec::with_capacity(8 + principal_bytes.len());
    key.extend_from_slice(&ino.to_le_bytes());
    key.extend_from_slice(principal_bytes);
    key
}

fn acl_rule_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

// ─── Postcard helpers ────────────────────────────────────────────────────────

fn encode<T: serde::Serialize>(val: &T) -> MetaResult<Vec<u8>> {
    postcard::to_stdvec(val).map_err(|e| MetaError::Serialization(e.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> MetaResult<T> {
    postcard::from_bytes(bytes).map_err(|e| MetaError::Serialization(e.to_string()))
}

// ─── RedbMetaEngine ──────────────────────────────────────────────────────────

/// Metadata engine backed by redb.
pub struct RedbMetaEngine {
    db: Arc<Database>,
}

impl RedbMetaEngine {
    /// Open or create a metadata database at the given path.
    pub fn open(path: impl AsRef<Path>) -> MetaResult<Self> {
        // Ensure the parent directory exists before opening the database
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                MetaError::Storage(format!("failed to create directory for redb: {e}"))
            })?;
        }

        let db = Database::create(path.as_ref())
            .map_err(|e| MetaError::Storage(format!("failed to open redb: {e}")))?;

        // Ensure all tables exist
        {
            let txn = db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = txn.open_table(SETTINGS_TABLE);
            let _ = txn.open_table(COUNTERS_TABLE);
            let _ = txn.open_table(INODES_TABLE);
            let _ = txn.open_table(BASE_INODES_TABLE);
            let _ = txn.open_table(INODE_TOMBSTONES_TABLE);
            let _ = txn.open_table(DENTRIES_TABLE);
            let _ = txn.open_table(BASE_DENTRIES_TABLE);
            let _ = txn.open_table(DENTRY_TOMBSTONES_TABLE);
            let _ = txn.open_table(FILE_BLOCKS_TABLE);
            let _ = txn.open_table(COMMITTED_FILE_BLOCKS_TABLE);
            let _ = txn.open_table(REGULAR_FILE_OVERLAY_TABLE);
            let _ = txn.open_table(REGULAR_FILE_BASE_TABLE);
            let _ = txn.open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE);
            let _ = txn.open_table(PENDING_DELETIONS_TABLE);
            let _ = txn.open_table(PENDING_CREATES_TABLE);
            let _ = txn.open_table(DEFERRED_DELETES_TABLE);
            let _ = txn.open_table(UNDO_LOG_TABLE);
            let _ = txn.open_table(SYMLINKS_TABLE);
            let _ = txn.open_table(BASE_SYMLINKS_TABLE);
            let _ = txn.open_table(SYMLINK_TOMBSTONES_TABLE);
            let _ = txn.open_table(XATTRS_TABLE);
            let _ = txn.open_table(BASE_XATTRS_TABLE);
            let _ = txn.open_table(XATTR_TOMBSTONES_TABLE);
            let _ = txn.open_table(ACL_RULES_TABLE);
            let _ = txn.open_table(BASE_ACL_RULES_TABLE);
            let _ = txn.open_table(ACL_RULE_TOMBSTONES_TABLE);
            let _ = txn.open_table(EVENT_LOG_TABLE);
            let _ = txn.open_table(SNAPSHOTS_TABLE);
            let _ = txn.open_table(MIGRATION_STATUS_TABLE);
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        debug!("opened metadata database at {:?}", path.as_ref());
        let engine = Self { db: Arc::new(db) };
        let _ = engine.recover_undo_logs()?;

        match engine.get_format_info() {
            Ok(format_info) if format_info.format_version == 2 => {
                info!("migrating metadata from format version 2 to 3");
                engine.migrate_regular_files_to_chunk_slice_model()?;

                let mut updated_format_info = format_info;
                updated_format_info.format_version = 3;
                engine.set_format_info(&updated_format_info)?;
                engine.validate_migration_completed()?;
                info!("metadata migration completed successfully");
            }
            Ok(format_info) if format_info.format_version >= 3 => {
                engine.validate_migration_completed()?;
                debug!(
                    "format version {} is up to date",
                    format_info.format_version
                );
            }
            Ok(info) => {
                return Err(MetaError::Storage(format!(
                    "unsupported format version: {}",
                    info.format_version
                )));
            }
            Err(MetaError::NotFormatted) => {
                debug!("new database (not yet formatted) — skipping migration check");
            }
            Err(e) => return Err(e),
        }

        Ok(engine)
    }

    /// Migrate regular files from the old FILE_BLOCKS flat model to the new chunk/slice/block model.
    ///
    /// This migration is performed when upgrading from format version 2 to 3.
    /// It reads all FILE_BLOCKS entries, converts them to chunk/slice format,
    /// and stores them in the new tables while keeping the legacy flat tables
    /// populated for compatibility during the transition.
    ///
    /// Key design decisions (addressing review findings):
    /// - FILE_BLOCKS (uncommitted/local) → LocalOnly in OVERLAY table
    /// - COMMITTED_FILE_BLOCKS → Published in BASE table
    /// - Batch processing (100 inodes per transaction) for efficiency
    /// - Streaming approach to minimize memory usage
    ///
    /// Rollback is not supported in-place; interrupted upgrades are detected by
    /// startup validation and can be resumed by rerunning the migration.
    fn migrate_regular_files_to_chunk_slice_model(&self) -> MetaResult<()> {
        const BATCH_SIZE: usize = 100;

        {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let migration_status = txn
                .open_table(MIGRATION_STATUS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if let Some(status) = migration_status
                .get(REGULAR_FILE_MIGRATION_NAME)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|v| v.value())
            {
                if status == MIGRATION_STATUS_COMPLETED {
                    debug!(
                        "migration {} already completed",
                        REGULAR_FILE_MIGRATION_NAME
                    );
                    return Ok(());
                }
            }
        }

        let format_info = self.get_format_info()?;
        let block_size = format_info.block_size;

        info!(
            "starting migration: regular files from v2 to v3 chunk/slice model (block_size={})",
            block_size
        );

        {
            let txn = self
                .db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            {
                let mut migration_status = txn
                    .open_table(MIGRATION_STATUS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                migration_status
                    .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_STARTED)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        let mut pending_inodes: Vec<Ino> = Vec::with_capacity(BATCH_SIZE);
        let mut pending_set: HashSet<Ino> = HashSet::with_capacity(BATCH_SIZE);
        let mut last_processed_key: Vec<u8> = Vec::new();

        for table_name in &[FILE_BLOCKS_TABLE, COMMITTED_FILE_BLOCKS_TABLE] {
            last_processed_key.clear();

            loop {
                pending_inodes.clear();
                pending_set.clear();

                {
                    let txn = self
                        .db
                        .begin_read()
                        .map_err(|e| MetaError::Storage(e.to_string()))?;

                    let table = txn
                        .open_table(*table_name)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;

                    let iter = if last_processed_key.is_empty() {
                        table
                            .iter()
                            .map_err(|e| MetaError::Storage(e.to_string()))?
                    } else {
                        table
                            .range(last_processed_key.as_slice()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?
                    };

                    for row in iter {
                        let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                        let key_bytes = key.value();

                        let (ino, _) = decode_file_block_key(key_bytes);

                        if pending_set.insert(ino) {
                            pending_inodes.push(ino);
                        }

                        if pending_inodes.len() >= BATCH_SIZE {
                            break;
                        }
                    }
                }

                if pending_inodes.is_empty() {
                    break;
                }

                self.migrate_inode_batch(pending_inodes.as_slice(), block_size)?;

                last_processed_key =
                    encode_file_block_key(*pending_inodes.last().unwrap(), u32::MAX).to_vec();
            }
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut migration_status = txn
                .open_table(MIGRATION_STATUS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            migration_status
                .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_COMPLETED)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        info!("migration completed: regular files v2 to v3 chunk/slice model");
        Ok(())
    }

    /// Migrate a batch of inodes in a single transaction.
    fn migrate_inode_batch(&self, inodes: &[Ino], block_size: u32) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut pending_compaction = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for &ino in inodes {
            if Self::has_regular_file_chunk_rows_in_write_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
            )? {
                continue;
            }

            let local_blocks =
                Self::get_file_blocks_from_txn_internal(&txn, FILE_BLOCKS_TABLE, ino)?;

            let committed_blocks =
                Self::get_file_blocks_from_txn_internal(&txn, COMMITTED_FILE_BLOCKS_TABLE, ino)?;

            if local_blocks.is_empty() && committed_blocks.is_empty() {
                continue;
            }

            let overlay_state = Self::convert_blocks_to_chunk_state(&local_blocks, block_size)?;
            let mut base_state =
                Self::convert_blocks_to_chunk_state(&committed_blocks, block_size)?;

            for chunk in base_state.chunks.values_mut() {
                for slice in chunk.slices.values_mut() {
                    slice.block_state = BlockRefState::Published;
                }
            }

            let mut overlay_table = txn
                .open_table(REGULAR_FILE_OVERLAY_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (chunk_id, chunk_state) in &overlay_state.chunks {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let encoded = encode(chunk_state)?;
                overlay_table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                if chunk_state.fragmentation.should_compact() {
                    pending_compaction
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                } else {
                    let _ = pending_compaction
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }

            let mut base_table = txn
                .open_table(REGULAR_FILE_BASE_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (chunk_id, chunk_state) in &base_state.chunks {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let encoded = encode(chunk_state)?;
                base_table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            // Keep the legacy block tables populated for now.
            //
            // Existing MetaBlocks methods (`get_file_block`, `get_file_blocks`,
            // `set_file_block`, `remove_file_block`) and VFS startup flows still
            // read from FILE_BLOCKS_TABLE / COMMITTED_FILE_BLOCKS_TABLE. Deleting
            // those rows here would make migrated volumes appear to have empty
            // manifests until all consumers are switched to the chunk/slice APIs.
            //
            // The migration therefore remains additive: it backfills the regular
            // file chunk tables while preserving the legacy source of truth for
            // compatibility.
        }

        drop(pending_compaction);
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        Ok(())
    }

    /// Get file blocks for an inode from a specific table within a transaction.
    fn get_file_blocks_from_txn_internal(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<(u32, BlockRef)>> {
        let prefix = file_block_prefix(ino);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut blocks = std::collections::BTreeMap::new();

        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;

        let iter = table
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for result in iter {
            let (key, value) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value();
            let (decoded_ino, block_index) = decode_file_block_key(key_bytes);
            if decoded_ino != ino {
                break;
            }
            let block: BlockRef = decode(value.value())?;
            blocks.insert(block_index, block);
        }

        Ok(blocks.into_iter().collect())
    }

    /// Convert flat block list to chunk/slice state using the provided block size.
    fn convert_blocks_to_chunk_state(
        blocks: &[(u32, BlockRef)],
        block_size: u32,
    ) -> MetaResult<RegularFileOverlayState> {
        use crate::schema::DEFAULT_CHUNK_SIZE;

        let mut state = RegularFileOverlayState::default();

        for (block_index, block_ref) in blocks {
            let block_start_offset = (*block_index as u64) * (block_size as u64);
            let chunk_id = ChunkId(block_start_offset / (DEFAULT_CHUNK_SIZE as u64));
            let chunk_start_offset = chunk_id.0 * (DEFAULT_CHUNK_SIZE as u64);
            let slice_start = (block_start_offset - chunk_start_offset) as u32;
            let slice_end = slice_start + block_ref.length;

            let slice_range = SliceRange::new(slice_start, slice_end).ok_or_else(|| {
                MetaError::Storage(format!(
                    "invalid slice range for block index {}: start={}, end={}",
                    block_index, slice_start, slice_end
                ))
            })?;

            let chunk_state = state.chunks.entry(chunk_id).or_default();
            let slice_id = chunk_state.alloc_slice_id();
            let slice_order = chunk_state.alloc_slice_order();
            let slice_record =
                SliceRecord::new(slice_id, slice_order, slice_range, block_ref.clone());
            chunk_state.insert_slice(slice_record);
        }

        for chunk_state in state.chunks.values_mut() {
            chunk_state.refresh_fragmentation_metrics();
        }

        Ok(state)
    }

    /// Validate that migration from v2 to v3 completed successfully.
    /// Useful for manual recovery scenarios and debugging.
    #[allow(dead_code)]
    fn validate_migration_completed(&self) -> MetaResult<()> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let migration_status = txn
            .open_table(MIGRATION_STATUS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        debug!(
            "checking migration status for {}",
            REGULAR_FILE_MIGRATION_NAME
        );

        let migration_entry = migration_status
            .get(REGULAR_FILE_MIGRATION_NAME)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let migration_completed = migration_entry
            .as_ref()
            .map(|v| {
                let status = v.value();
                debug!(
                    "migration status value: {} (expected: {})",
                    status, MIGRATION_STATUS_COMPLETED
                );
                status == MIGRATION_STATUS_COMPLETED
            })
            .unwrap_or(false);

        debug!("migration_completed: {}", migration_completed);

        if !migration_completed {
            let reason = migration_entry
                .as_ref()
                .map(|v| format!("status={}", v.value()))
                .unwrap_or_else(|| "not found".to_string());
            return Err(MetaError::Storage(format!(
                "migration from format v2 to v3 did not complete successfully ({})",
                reason
            )));
        }

        debug!("migration validation passed: completion marker is set");
        Ok(())
    }

    /// Probe format info from an existing redb database without opening it for writes.
    pub fn read_format_info_readonly(path: impl AsRef<Path>) -> MetaResult<FormatInfo> {
        let db = match ReadOnlyDatabase::open(path.as_ref()) {
            Ok(db) => db,
            Err(redb::DatabaseError::Storage(redb::StorageError::Io(err)))
                if err.kind() == ErrorKind::NotFound =>
            {
                return Err(MetaError::NotFormatted);
            }
            Err(e) => {
                return Err(MetaError::Storage(format!(
                    "failed to open redb read-only: {e}"
                )));
            }
        };
        let txn = db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let entry = table
            .get("format")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or(MetaError::NotFormatted)?;
        serde_json::from_slice(entry.value()).map_err(|e| MetaError::Serialization(e.to_string()))
    }

    /// Initialize a fresh filesystem with root inode.
    /// `uid` and `gid` are used as the owner of the root directory.
    pub fn init_root(&self, uid: u32, gid: u32) -> MetaResult<()> {
        // Check if root already exists
        if self.get_attr(ROOT_INO).is_ok() {
            debug!("root inode already exists, skipping init");
            return Ok(());
        }

        // Set initial counters
        self.set_counter(counters::NEXT_INODE, ROOT_INO + 1)?;
        self.set_counter(counters::NEXT_BLOCK_ID, 1)?;
        self.set_counter(counters::USED_SPACE, 0)?;
        self.set_counter(counters::TOTAL_INODES, 1)?;

        // Create root directory inode owned by the initializing user
        let root_attr = InodeAttr::new_dir(ROOT_INO, ROOT_INO, uid, gid, 0o755);
        self.set_attr(&root_attr)?;

        debug!("initialized root inode");
        Ok(())
    }

    /// Append a MetaEvent to the local event log table within an existing write transaction.
    fn append_event_in_txn(
        txn: &redb::WriteTransaction,
        event: &crate::events::MetaEvent,
    ) -> MetaResult<()> {
        let mut counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        // Get the next sequence ID for the event log
        let seq_id = match counters
            .get("nextEventSeq")
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => v.value(),
            None => 1,
        };
        counters
            .insert("nextEventSeq", seq_id + 1)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        // Encode and insert the event
        let mut events = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let data = encode(event)?;
        events
            .insert(seq_id, data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        Ok(())
    }

    fn remove_all_committed_blocks_in_txn(
        committed: &mut redb::Table<'_, &[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<u64>> {
        let prefix = file_block_prefix(ino);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;
        let rows: Vec<(Vec<u8>, u64)> = {
            let iter = committed
                .range(prefix.as_slice()..range_end.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            iter.filter_map(|r| r.ok())
                .filter_map(|(k, v)| {
                    let block: BlockRef = decode(v.value()).ok()?;
                    Some((k.value().to_vec(), block.object_id))
                })
                .collect()
        };
        for (key, _) in &rows {
            let _ = committed.remove(key.as_slice());
        }
        Ok(rows.into_iter().map(|(_, object_id)| object_id).collect())
    }

    fn remove_all_file_blocks_in_txn(
        blocks: &mut redb::Table<'_, &[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<u64>> {
        let prefix = file_block_prefix(ino);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;
        let rows: Vec<(Vec<u8>, u64)> = {
            let iter = blocks
                .range(prefix.as_slice()..range_end.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            iter.filter_map(|r| r.ok())
                .filter_map(|(k, v)| {
                    let block: BlockRef = decode(v.value()).ok()?;
                    Some((k.value().to_vec(), block.object_id))
                })
                .collect()
        };
        for (key, _) in &rows {
            let _ = blocks.remove(key.as_slice());
        }
        Ok(rows.into_iter().map(|(_, object_id)| object_id).collect())
    }

    fn get_attr_from_table(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<u64, &[u8]>,
        ino: Ino,
    ) -> MetaResult<Option<InodeAttr>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|entry| decode(entry.value()))
            .transpose()
    }

    fn get_attr_from_write_table(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<u64, &[u8]>,
        ino: Ino,
    ) -> MetaResult<Option<InodeAttr>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|entry| decode(entry.value()))
            .transpose()
    }

    fn local_overlay_inodes_from_txn(txn: &redb::ReadTransaction) -> MetaResult<HashSet<Ino>> {
        let mut overlay = HashSet::new();

        let file_blocks = txn
            .open_table(FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let committed = txn
            .open_table(COMMITTED_FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in file_blocks
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (ino, _block_index) = decode_file_block_key(key.value());
            let committed_value = committed
                .get(key.value())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            match committed_value {
                Some(committed_value) if committed_value.value() == value.value() => {}
                _ => {
                    overlay.insert(ino);
                }
            }
        }

        let regular_overlay =
            Self::regular_file_chunk_states_from_txn(txn, REGULAR_FILE_OVERLAY_TABLE)?;
        let regular_base = Self::regular_file_chunk_states_from_txn(txn, REGULAR_FILE_BASE_TABLE)?;
        for (ino, overlay_chunks) in regular_overlay {
            let overlay_signature = Self::regular_file_graph_signature(&overlay_chunks);
            if overlay_signature.is_empty() {
                continue;
            }
            let base_signature = regular_base
                .get(&ino)
                .map(Self::regular_file_graph_signature)
                .unwrap_or_default();
            if overlay_signature != base_signature {
                overlay.insert(ino);
            }
        }

        Ok(overlay)
    }

    fn regular_file_chunk_states_from_txn(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
    ) -> MetaResult<BTreeMap<Ino, BTreeMap<ChunkId, RegularFileChunkState>>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut states: BTreeMap<Ino, BTreeMap<ChunkId, RegularFileChunkState>> = BTreeMap::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (ino, chunk_id) = decode_regular_file_chunk_key(key.value());
            let chunk_state: RegularFileChunkState = decode(value.value())?;
            states.entry(ino).or_default().insert(chunk_id, chunk_state);
        }
        Ok(states)
    }

    fn regular_file_chunk_states_for_inode_in_write_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<BTreeMap<ChunkId, RegularFileChunkState>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut chunks = BTreeMap::new();
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            let (decoded_ino, chunk_id) = decode_regular_file_chunk_key(key.value());
            debug_assert_eq!(decoded_ino, ino);
            let chunk_state: RegularFileChunkState = decode(value.value())?;
            chunks.insert(chunk_id, chunk_state);
        }
        Ok(chunks)
    }

    fn has_regular_file_chunk_rows_in_write_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut iter = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match iter.next() {
            Some(row) => {
                let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                Ok(key.value().starts_with(prefix.as_ref()))
            }
            None => Ok(false),
        }
    }

    fn has_regular_file_chunk_rows_in_read_txn(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut iter = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match iter.next() {
            Some(row) => {
                let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                Ok(key.value().starts_with(prefix.as_ref()))
            }
            None => Ok(false),
        }
    }

    /// Build a data-only signature for a regular-file chunk graph.
    ///
    /// Internal counters and slice lifecycle fields are intentionally ignored so
    /// callers can detect real overlay data divergence without flagging pure
    /// bookkeeping changes.
    fn regular_file_graph_signature(
        chunks: &BTreeMap<ChunkId, RegularFileChunkState>,
    ) -> RegularFileGraphSignature {
        let mut signature = BTreeMap::new();
        for (chunk_id, chunk) in chunks {
            if chunk.slices.is_empty() {
                continue;
            }
            let mut chunk_signature = Vec::with_capacity(chunk.slices.len());
            for slice in chunk.slices.values() {
                chunk_signature.push((slice.range, slice.block.clone()));
            }
            signature.insert(*chunk_id, chunk_signature);
        }
        signature
    }

    fn regular_file_chunk_state_from_slices(
        chunk_slices: &[crate::schema::SliceRecord],
    ) -> MetaResult<RegularFileChunkState> {
        let mut state = RegularFileChunkState::default();
        let mut max_slice_id: Option<SliceId> = None;
        let mut max_slice_order: Option<SliceOrder> = None;

        for slice in chunk_slices.iter().cloned() {
            max_slice_id = Some(match max_slice_id {
                Some(current) => current.max(slice.slice_id),
                None => slice.slice_id,
            });
            max_slice_order = Some(match max_slice_order {
                Some(current) => current.max(slice.order),
                None => slice.order,
            });
            state.slices.insert(slice.slice_id, slice);
        }

        state.next_slice_id = match max_slice_id {
            Some(slice_id) => SliceId(
                slice_id
                    .0
                    .checked_add(1)
                    .ok_or_else(|| MetaError::Storage("slice id overflow".to_string()))?,
            ),
            None => SliceId(0),
        };
        state.next_slice_order = match max_slice_order {
            Some(slice_order) => SliceOrder(
                slice_order
                    .0
                    .checked_add(1)
                    .ok_or_else(|| MetaError::Storage("slice order overflow".to_string()))?,
            ),
            None => SliceOrder(0),
        };
        state.refresh_fragmentation_metrics();
        Ok(state)
    }

    fn set_regular_file_chunk_state_in_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        track_pending_compaction: bool,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_slices: &[crate::schema::SliceRecord],
    ) -> MetaResult<Option<RegularFileChunkState>> {
        let key = encode_regular_file_chunk_key(ino, chunk_id);
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        if chunk_slices.is_empty() {
            let _ = table
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if track_pending_compaction {
                let mut pending = txn
                    .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = pending
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
            return Ok(None);
        }

        let state = Self::regular_file_chunk_state_from_slices(chunk_slices)?;
        let encoded = encode(&state)?;
        table
            .insert(key.as_slice(), encoded.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        if track_pending_compaction {
            let mut pending = txn
                .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if state.fragmentation.should_compact() {
                pending
                    .insert(key.as_slice(), 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            } else {
                let _ = pending
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }

        Ok(Some(state))
    }

    /// Collect object IDs reachable from a regular-file chunk table.
    ///
    /// This streams the table row-by-row so reachability checks never need to
    /// materialize the whole chunk graph in memory.
    fn regular_file_graph_object_ids_from_table(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        refs: &mut HashSet<u64>,
    ) -> MetaResult<()> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let chunk: RegularFileChunkState = decode(value.value())?;
            for slice in chunk.slices.values() {
                if let Some(block) = &slice.block {
                    refs.insert(block.object_id);
                }
            }
        }
        Ok(())
    }

    fn clear_table_in_txn<K, V>(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<K, V>,
    ) -> MetaResult<()>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .retain(|_, _| false)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn regular_file_snapshot_rows_from_txn<TState, F>(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        wrap_state: F,
    ) -> MetaResult<Vec<(u64, Vec<u8>)>>
    where
        TState: serde::Serialize,
        F: Fn(BTreeMap<ChunkId, RegularFileChunkState>) -> TState,
    {
        let states = Self::regular_file_chunk_states_from_txn(txn, table_def)?;
        let mut snapshot_rows = Vec::with_capacity(states.len());
        for (ino, chunks) in states {
            let state = wrap_state(chunks);
            snapshot_rows.push((ino, encode(&state)?));
        }
        Ok(snapshot_rows)
    }

    fn clear_regular_file_chunk_rows_in_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut keys_to_remove = Vec::new();
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            keys_to_remove.push(key.value().to_vec());
        }
        for key in keys_to_remove {
            let _ = table
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn clear_regular_file_pending_compaction_rows_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut pending = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut keys_to_remove = Vec::new();
        for row in pending
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            keys_to_remove.push(key.value().to_vec());
        }
        for key in keys_to_remove {
            let _ = pending
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn clear_regular_file_graph_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<()> {
        Self::clear_regular_file_pending_compaction_rows_in_txn(txn, ino)?;
        Self::clear_regular_file_chunk_rows_in_txn(txn, REGULAR_FILE_OVERLAY_TABLE, ino)?;
        Self::clear_regular_file_chunk_rows_in_txn(txn, REGULAR_FILE_BASE_TABLE, ino)?;
        Ok(())
    }

    fn replace_regular_file_chunk_rows_from_blocks_in_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
        blocks: &[(u32, BlockRef)],
        block_size: u32,
        published: bool,
        track_pending_compaction: bool,
    ) -> MetaResult<()> {
        if track_pending_compaction {
            Self::clear_regular_file_pending_compaction_rows_in_txn(txn, ino)?;
        }
        Self::clear_regular_file_chunk_rows_in_txn(txn, table_def, ino)?;
        if blocks.is_empty() {
            return Ok(());
        }

        let graph = Self::convert_blocks_to_chunk_state(blocks, block_size)?;
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for (chunk_id, chunk_state) in graph.chunks {
            let chunk_state = if published {
                Self::chunk_for_published_base(&chunk_state)
            } else {
                chunk_state
            };
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let encoded = encode(&chunk_state)?;
            table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if track_pending_compaction && chunk_state.fragmentation.should_compact() {
                let mut pending = txn
                    .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                pending
                    .insert(key.as_slice(), 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        Ok(())
    }

    fn replace_regular_file_chunk_rows_from_snapshot_rows<F>(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        snapshot_rows: &[(u64, Vec<u8>)],
        parse_chunks: F,
        track_pending_compaction: bool,
    ) -> MetaResult<()>
    where
        F: Fn(&[u8]) -> MetaResult<BTreeMap<ChunkId, RegularFileChunkState>>,
    {
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .retain(|_, _| false)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for (ino, bytes) in snapshot_rows {
            let chunks = parse_chunks(bytes)?;
            for (chunk_id, chunk_state) in chunks {
                let key = encode_regular_file_chunk_key(*ino, chunk_id);
                let encoded = encode(&chunk_state)?;
                table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                if track_pending_compaction && chunk_state.fragmentation.should_compact() {
                    let mut pending = txn
                        .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    pending
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }
        }
        Ok(())
    }

    fn regular_file_committed_size_from_chunks(
        chunks: &BTreeMap<ChunkId, RegularFileChunkState>,
    ) -> u64 {
        let mut size = 0u64;
        for (chunk_id, chunk_state) in chunks {
            let chunk_start = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
            for slice in chunk_state.slices.values() {
                if slice.block.is_some() {
                    let absolute_end = chunk_start.saturating_add(slice.range.end as u64);
                    size = size.max(absolute_end);
                }
            }
        }
        size
    }

    fn legacy_committed_size_from_blocks(blocks: &[(u32, BlockRef)]) -> u64 {
        let mut size = 0u64;
        let mut expected = 0u32;
        for (block_index, block) in blocks {
            if *block_index != expected {
                break;
            }
            size = size.saturating_add(block.length as u64);
            expected = expected.saturating_add(1);
            if block.is_partial() {
                break;
            }
        }
        size
    }

    fn chunk_for_published_base(chunk: &RegularFileChunkState) -> RegularFileChunkState {
        let mut published = chunk.clone();
        for slice in published.slices.values_mut() {
            slice.block_state = BlockRefState::Published;
        }
        published
    }

    fn compact_regular_file_chunk_state(
        chunk_id: ChunkId,
        chunk: &RegularFileChunkState,
    ) -> MetaResult<Option<RegularFileChunkState>> {
        let full_range = SliceRange::new(0, DEFAULT_CHUNK_SIZE)
            .expect("full chunk range should always be valid");
        let resolved = resolve_chunk_slice_spans(chunk_id, full_range, Some(chunk), None);
        let mut compacted = RegularFileChunkState {
            next_slice_id: chunk.next_slice_id,
            next_slice_order: chunk.next_slice_order,
            slices: BTreeMap::new(),
            fragmentation: ChunkFragmentationCounters::default(),
        };
        let mut merged: Vec<(SliceRange, Option<BlockRef>, BlockRefState)> = Vec::new();

        for span in resolved {
            let Some(slice_id) = span.slice_id else {
                continue;
            };
            let source_state = chunk
                .slices
                .get(&slice_id)
                .map(|slice| slice.block_state)
                .ok_or_else(|| {
                    MetaError::Storage("compaction source slice is missing".to_string())
                })?;
            let block = span.block.clone();

            if let Some((last_range, last_block, last_state)) = merged.last_mut() {
                match (last_block.as_mut(), block.as_ref()) {
                    (Some(last_block), Some(block))
                        if *last_state == source_state
                            && last_block.object_id == block.object_id
                            && last_block.offset.saturating_add(last_block.length)
                                == block.offset
                            && last_block.flags == block.flags
                            && last_range.end == span.range.start =>
                    {
                        last_range.end = span.range.end;
                        last_block.length = last_block.length.saturating_add(block.length);
                        continue;
                    }
                    (None, None)
                        if *last_state == source_state && last_range.end == span.range.start =>
                    {
                        last_range.end = span.range.end;
                        continue;
                    }
                    _ => {}
                }
            }

            merged.push((span.range, block, source_state));
        }

        if merged.is_empty() {
            return Ok(None);
        }

        for (range, block, block_state) in merged {
            let slice_id = compacted
                .try_alloc_slice_id()
                .ok_or_else(|| MetaError::Storage("slice id overflow".to_string()))?;
            let slice_order = compacted
                .try_alloc_slice_order()
                .ok_or_else(|| MetaError::Storage("slice order overflow".to_string()))?;
            compacted.slices.insert(
                slice_id,
                SliceRecord {
                    slice_id,
                    order: slice_order,
                    range,
                    block,
                    block_state,
                },
            );
        }

        compacted.refresh_fragmentation_metrics();
        Ok(Some(compacted))
    }

    fn publish_regular_file_overlay_to_base_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<u64> {
        // Publication semantics are "overlay becomes the new remote-visible
        // base" for the inode, so base rows are reconciled to exactly match the
        // normalized overlay graph.
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        let existing_base = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )?;

        let mut desired_base = BTreeMap::new();
        for (chunk_id, chunk_state) in &overlay_chunks {
            desired_base.insert(*chunk_id, Self::chunk_for_published_base(chunk_state));
        }

        let mut stale_chunk_ids: BTreeSet<ChunkId> = existing_base.keys().copied().collect();
        let mut base = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for (chunk_id, desired_chunk) in &desired_base {
            stale_chunk_ids.remove(chunk_id);
            let unchanged = existing_base
                .get(chunk_id)
                .is_some_and(|existing_chunk| existing_chunk == desired_chunk);
            if unchanged {
                continue;
            }

            let key = encode_regular_file_chunk_key(ino, *chunk_id);
            let encoded = encode(desired_chunk)?;
            base.insert(key.as_slice(), encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        for chunk_id in stale_chunk_ids {
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let _ = base
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        // Overlay rows are local/staging state. Once promoted to committed base,
        // clear them to avoid unbounded growth and stale finalize gates.
        Self::clear_regular_file_chunk_rows_in_txn(txn, REGULAR_FILE_OVERLAY_TABLE, ino)?;
        Ok(Self::regular_file_committed_size_from_chunks(
            &overlay_chunks,
        ))
    }

    fn regular_file_graph_block_size(&self) -> MetaResult<u32> {
        match self.get_format_info() {
            Ok(info) => Ok(info.block_size),
            Err(MetaError::NotFormatted) => Ok(DEFAULT_BLOCK_SIZE),
            Err(err) => Err(err),
        }
    }

    fn synthesize_block_ref_from_resolved_spans(
        ino: Ino,
        spans: &[ResolvedSliceSpan],
        block_index: u32,
        block_size: u32,
    ) -> MetaResult<Option<BlockRef>> {
        #[derive(Clone, Copy)]
        struct Range {
            start: u64,
            end: u64,
        }

        fn claim_ranges(unresolved: Vec<Range>, candidate: Range) -> (Vec<Range>, Vec<Range>) {
            let mut next_unresolved = Vec::new();
            let mut claimed = Vec::new();
            for range in unresolved {
                let overlap_start = range.start.max(candidate.start);
                let overlap_end = range.end.min(candidate.end);
                if overlap_start >= overlap_end {
                    next_unresolved.push(range);
                    continue;
                }
                if range.start < overlap_start {
                    next_unresolved.push(Range {
                        start: range.start,
                        end: overlap_start,
                    });
                }
                claimed.push(Range {
                    start: overlap_start,
                    end: overlap_end,
                });
                if overlap_end < range.end {
                    next_unresolved.push(Range {
                        start: overlap_end,
                        end: range.end,
                    });
                }
            }
            (next_unresolved, claimed)
        }

        let block_start = (block_index as u64).saturating_mul(block_size as u64);
        let block_end = block_start.saturating_add(block_size as u64);
        let mut unresolved = vec![Range {
            start: block_start,
            end: block_end,
        }];
        let mut claimed_refs: Vec<(u64, BlockRef)> = Vec::new();

        for source in [ResolvedSliceSource::Overlay, ResolvedSliceSource::Base] {
            for span in spans.iter().filter(|span| span.source == source) {
                if unresolved.is_empty() {
                    break;
                }
                let Some(block) = span.block.as_ref() else {
                    continue;
                };
                let chunk_start = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
                let span_range = Range {
                    start: chunk_start.saturating_add(span.range.start as u64),
                    end: chunk_start.saturating_add(span.range.end as u64),
                };
                let (next_unresolved, claimed) = claim_ranges(unresolved, span_range);
                unresolved = next_unresolved;
                for overlap in claimed {
                    let delta = overlap.start.saturating_sub(span_range.start);
                    let Some(delta_u32) = u32::try_from(delta).ok() else {
                        continue;
                    };
                    let Some(block_offset) = block.offset.checked_add(delta_u32) else {
                        continue;
                    };
                    let take_len = overlap.end.saturating_sub(overlap.start);
                    let Some(take_len_u32) = u32::try_from(take_len).ok() else {
                        continue;
                    };
                    claimed_refs.push((
                        overlap.start,
                        BlockRef {
                            object_id: block.object_id,
                            offset: block_offset,
                            length: take_len_u32,
                            flags: block.flags,
                        },
                    ));
                }
            }
        }

        if claimed_refs.is_empty() {
            return Ok(None);
        }
        claimed_refs.sort_by_key(|(start, _)| *start);

        let mut chosen: Option<BlockRef> = None;
        for (_, part) in claimed_refs {
            if let Some(existing) = chosen.as_mut() {
                if existing.object_id != part.object_id
                    || existing.offset.saturating_add(existing.length) != part.offset
                {
                    return Err(MetaError::FragmentedBlock { ino, block_index });
                }
                existing.length = existing.length.saturating_add(part.length);
                existing.flags |= part.flags;
            } else {
                chosen = Some(part);
            }
        }

        Ok(chosen.filter(|block| block.length > 0))
    }

    fn lookup_regular_file_slices_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| MetaError::Storage("regular-file lookup range overflow".to_string()))?;
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        let base_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )?;
        let mut spans = Vec::new();
        let mut cursor = offset;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64);
            let query_end = end.min(chunk_end);
            let start_in_chunk = (cursor - chunk_start) as u32;
            let end_in_chunk = (query_end - chunk_start) as u32;
            let query = SliceRange::new(start_in_chunk, end_in_chunk).ok_or_else(|| {
                MetaError::Storage("invalid chunk-local lookup range".to_string())
            })?;
            let overlay = overlay_chunks.get(&chunk_id);
            let base = base_chunks.get(&chunk_id);
            spans.extend(resolve_chunk_slice_spans(chunk_id, query, overlay, base));
            cursor = query_end;
        }
        Ok(merge_adjacent_spans(spans))
    }

    fn local_overlay_attr_inodes_from_txn(txn: &redb::ReadTransaction) -> MetaResult<HashSet<Ino>> {
        let current = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, current_bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ino = ino.value();
            match base
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_bytes) if base_bytes.value() == current_bytes.value() => {}
                _ => {
                    overlay.insert(ino);
                }
            }
        }

        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(ino.value());
        }

        Ok(overlay)
    }

    fn resolve_attr_in_read_snapshot(
        txn: &redb::ReadTransaction,
        ino: Ino,
        overlay_block_inodes: &HashSet<Ino>,
        overlay_attr_inodes: &HashSet<Ino>,
    ) -> MetaResult<InodeAttr> {
        if Self::has_u64_marker(txn, INODE_TOMBSTONES_TABLE, ino)? {
            return Err(MetaError::InodeNotFound(ino));
        }

        if overlay_attr_inodes.contains(&ino) {
            return Self::get_attr_from_table(txn, INODES_TABLE, ino)?
                .ok_or(MetaError::InodeNotFound(ino));
        }

        if overlay_block_inodes.contains(&ino) {
            if let Some(attr) = Self::get_attr_from_table(txn, INODES_TABLE, ino)? {
                return Ok(attr);
            }
            if let Some(attr) = Self::get_attr_from_table(txn, BASE_INODES_TABLE, ino)? {
                return Ok(attr);
            }
            return Err(MetaError::InodeNotFound(ino));
        }

        if let Some(attr) = Self::get_attr_from_table(txn, BASE_INODES_TABLE, ino)? {
            return Ok(attr);
        }

        Self::get_attr_from_table(txn, INODES_TABLE, ino)?.ok_or(MetaError::InodeNotFound(ino))
    }

    fn lookup_in_table(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<Option<DirEntryValue>> {
        let key = encode_dentry_key(parent, name);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        if let Ok(Some(entry)) = table.get(key.as_slice()) {
            return decode(entry.value()).map(Some);
        }

        let name_lower = String::from_utf8_lossy(name).to_lowercase();
        let prefix = dentry_prefix(parent);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;

        let iter = table
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for result in iter {
            let (k, v) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            let (decoded_parent, stored_name) = decode_dentry_key(key_bytes);
            if decoded_parent != parent {
                break;
            }
            let stored_name_lower = String::from_utf8_lossy(stored_name).to_lowercase();
            if stored_name_lower == name_lower {
                return decode(v.value()).map(Some);
            }
        }

        Ok(None)
    }

    fn lookup_in_write_table(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<Option<DirEntryValue>> {
        let key = encode_dentry_key(parent, name);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        if let Ok(Some(entry)) = table.get(key.as_slice()) {
            return decode(entry.value()).map(Some);
        }

        let name_lower = String::from_utf8_lossy(name).to_lowercase();
        let prefix = dentry_prefix(parent);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;

        let iter = table
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for result in iter {
            let (k, v) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            let (decoded_parent, stored_name) = decode_dentry_key(key_bytes);
            if decoded_parent != parent {
                break;
            }
            let stored_name_lower = String::from_utf8_lossy(stored_name).to_lowercase();
            if stored_name_lower == name_lower {
                return decode(v.value()).map(Some);
            }
        }

        Ok(None)
    }

    fn has_slice_marker_in_write(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], u8>,
        key: &[u8],
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some())
    }

    fn has_u64_marker_in_write(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<u64, u8>,
        key: u64,
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some())
    }

    fn lookup_merged_in_write_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<Option<DirEntryValue>> {
        let key = encode_dentry_key(parent, name);
        if Self::has_slice_marker_in_write(txn, DENTRY_TOMBSTONES_TABLE, key.as_slice())? {
            return Ok(None);
        }

        if let Some(entry) = Self::lookup_in_write_table(txn, DENTRIES_TABLE, parent, name)? {
            return Ok(Some(entry));
        }
        if let Some(entry) = Self::lookup_in_write_table(txn, BASE_DENTRIES_TABLE, parent, name)? {
            return Ok(Some(entry));
        }

        Ok(None)
    }

    fn get_attr_merged_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<InodeAttr> {
        if Self::has_u64_marker_in_write(txn, INODE_TOMBSTONES_TABLE, ino)? {
            return Err(MetaError::InodeNotFound(ino));
        }
        if let Some(attr) = Self::get_attr_from_write_table(txn, INODES_TABLE, ino)? {
            return Ok(attr);
        }
        if let Some(attr) = Self::get_attr_from_write_table(txn, BASE_INODES_TABLE, ino)? {
            return Ok(attr);
        }
        Err(MetaError::InodeNotFound(ino))
    }

    fn readdir_from_write_table(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        parent: Ino,
    ) -> MetaResult<Vec<DirEntry>> {
        let prefix = dentry_prefix(parent);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut entries = Vec::new();
        let range_start = prefix.as_slice();
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;

        let iter = table
            .range(range_start..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for result in iter {
            let (key, value) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value();
            let (decoded_parent, name) = decode_dentry_key(key_bytes);
            if decoded_parent != parent {
                break;
            }
            let entry_val: DirEntryValue = decode(value.value())?;
            entries.push(DirEntry {
                name: name.to_vec(),
                ino: entry_val.ino,
                kind: entry_val.kind,
            });
        }

        Ok(entries)
    }

    fn local_overlay_dentries_in_write_txn(
        txn: &redb::WriteTransaction,
    ) -> MetaResult<HashSet<Vec<u8>>> {
        let current = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();

        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }

        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }

        Ok(overlay)
    }

    fn readdir_merged_in_write_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
    ) -> MetaResult<Vec<DirEntry>> {
        let overlay_dentries = Self::local_overlay_dentries_in_write_txn(txn)?;
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut merged = std::collections::BTreeMap::<Vec<u8>, DirEntry>::new();
        for entry in Self::readdir_from_write_table(txn, BASE_DENTRIES_TABLE, parent)? {
            merged.insert(entry.name.clone(), entry);
        }

        let prefix = dentry_prefix(parent);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;
        for row in tombstones
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (decoded_parent, name) = decode_dentry_key(key.value());
            if decoded_parent != parent {
                break;
            }
            merged.remove(name);
        }

        for entry in Self::readdir_from_write_table(txn, DENTRIES_TABLE, parent)? {
            let key = encode_dentry_key(parent, &entry.name);
            if overlay_dentries.contains(&key) {
                merged.insert(entry.name.clone(), entry);
            }
        }

        let base_names: std::collections::HashSet<Vec<u8>> =
            Self::readdir_from_write_table(txn, BASE_DENTRIES_TABLE, parent)?
                .into_iter()
                .map(|entry| entry.name)
                .collect();
        for key in overlay_dentries {
            let (overlay_parent, overlay_name) = decode_dentry_key(&key);
            if overlay_parent != parent {
                continue;
            }
            if !base_names.contains(overlay_name)
                && let Some(entry) =
                    Self::lookup_in_write_table(txn, DENTRIES_TABLE, parent, overlay_name)?
            {
                merged.insert(
                    overlay_name.to_vec(),
                    DirEntry {
                        name: overlay_name.to_vec(),
                        ino: entry.ino,
                        kind: entry.kind,
                    },
                );
            }
            if base_names.contains(overlay_name)
                && Self::lookup_in_write_table(txn, DENTRIES_TABLE, parent, overlay_name)?.is_none()
            {
                merged.remove(overlay_name);
            }
        }

        Ok(merged.into_values().collect())
    }

    fn readdir_from_table(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        parent: Ino,
    ) -> MetaResult<Vec<DirEntry>> {
        let prefix = dentry_prefix(parent);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut entries = Vec::new();
        let range_start = prefix.as_slice();
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;

        let iter = table
            .range(range_start..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for result in iter {
            let (key, value) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value();
            let (decoded_parent, name) = decode_dentry_key(key_bytes);
            if decoded_parent != parent {
                break;
            }
            let entry_val: DirEntryValue = decode(value.value())?;
            entries.push(DirEntry {
                name: name.to_vec(),
                ino: entry_val.ino,
                kind: entry_val.kind,
            });
        }

        Ok(entries)
    }

    fn get_bytes_from_table_u64(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<u64, &[u8]>,
        key: u64,
    ) -> MetaResult<Option<Vec<u8>>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|entry| entry.value().to_vec()))
    }

    fn get_bytes_from_table_slice(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        key: &[u8],
    ) -> MetaResult<Option<Vec<u8>>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|entry| entry.value().to_vec()))
    }

    fn has_u64_marker(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<u64, u8>,
        key: u64,
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some())
    }

    fn has_slice_marker(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], u8>,
        key: &[u8],
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some())
    }

    fn keys_for_inode_in_table(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<Vec<u8>>> {
        let prefix = xattr_prefix(ino);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let range = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut keys = Vec::new();
        for item in range {
            let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            keys.push(key_bytes.to_vec());
        }
        Ok(keys)
    }

    fn keys_for_inode_in_write_table(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<Vec<u8>>> {
        let prefix = xattr_prefix(ino);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let range = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut keys = Vec::new();
        for item in range {
            let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            keys.push(key_bytes.to_vec());
        }
        Ok(keys)
    }

    fn merged_symlink_target(
        &self,
        txn: &redb::ReadTransaction,
        ino: Ino,
    ) -> MetaResult<Option<Vec<u8>>> {
        if Self::has_u64_marker(txn, SYMLINK_TOMBSTONES_TABLE, ino)? {
            return Ok(None);
        }
        if let Some(target) = Self::get_bytes_from_table_u64(txn, SYMLINKS_TABLE, ino)? {
            return Ok(Some(target));
        }
        Self::get_bytes_from_table_u64(txn, BASE_SYMLINKS_TABLE, ino)
    }

    fn merged_xattr_value(
        &self,
        txn: &redb::ReadTransaction,
        ino: Ino,
        name: &[u8],
    ) -> MetaResult<Option<Vec<u8>>> {
        let key = encode_xattr_key(ino, name);
        if Self::has_slice_marker(txn, XATTR_TOMBSTONES_TABLE, key.as_slice())? {
            return Ok(None);
        }
        if let Some(value) = Self::get_bytes_from_table_slice(txn, XATTRS_TABLE, key.as_slice())? {
            return Ok(Some(decode_xattr_value_from_storage_owned(name, value)));
        }
        Ok(
            Self::get_bytes_from_table_slice(txn, BASE_XATTRS_TABLE, key.as_slice())?
                .map(|value| decode_xattr_value_from_storage_owned(name, value)),
        )
    }

    fn set_xattr_value_in_table_in_txn(
        txn: &redb::WriteTransaction,
        table: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
        name: &[u8],
        value: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let stored = encode_xattr_value_for_storage(name, value);
        let mut xattrs = txn
            .open_table(table)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        xattrs
            .insert(key.as_slice(), stored.as_ref())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn set_inode_overlay_in_txn(txn: &redb::WriteTransaction, attr: &InodeAttr) -> MetaResult<()> {
        let mut tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(attr.ino);
        drop(tombstones);

        let data = encode(attr)?;
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        inodes
            .insert(attr.ino, data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_inode_overlay_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<()> {
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = inodes.remove(ino);
        drop(inodes);

        let has_base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some();

        let mut tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if has_base {
            tombstones
                .insert(ino, 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = tombstones.remove(ino);
        }
        Ok(())
    }

    fn set_dentry_overlay_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        let mut tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        drop(tombstones);

        let data = encode(entry)?;
        let mut dentries = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        dentries
            .insert(key.as_slice(), data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_dentry_overlay_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        let mut dentries = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = dentries.remove(key.as_slice());
        drop(dentries);

        let has_base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some();

        let mut tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if has_base {
            tombstones
                .insert(key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = tombstones.remove(key.as_slice());
        }
        Ok(())
    }

    fn set_inode_mirror_in_txn(txn: &redb::WriteTransaction, attr: &InodeAttr) -> MetaResult<()> {
        let mut tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(attr.ino);
        drop(tombstones);

        let data = encode(attr)?;
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        inodes
            .insert(attr.ino, data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_inode_mirror_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<()> {
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = inodes.remove(ino);
        drop(inodes);

        let mut tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(ino);
        Ok(())
    }

    fn set_dentry_mirror_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        let mut tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        drop(tombstones);

        let data = encode(entry)?;
        let mut dentries = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        dentries
            .insert(key.as_slice(), data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_dentry_mirror_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        let mut dentries = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = dentries.remove(key.as_slice());
        drop(dentries);

        let mut tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        Ok(())
    }

    fn set_symlink_mirror_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        target: &[u8],
    ) -> MetaResult<()> {
        let mut tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(ino);
        drop(tombstones);

        let mut symlinks = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        symlinks
            .insert(ino, target)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_symlink_mirror_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<()> {
        let mut symlinks = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = symlinks.remove(ino);
        drop(symlinks);

        let mut tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(ino);
        Ok(())
    }

    fn set_xattr_mirror_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        name: &[u8],
        value: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let mut tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        drop(tombstones);

        Self::set_xattr_value_in_table_in_txn(txn, XATTRS_TABLE, ino, name, value)?;
        Ok(())
    }

    fn remove_xattr_mirror_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let mut xattrs = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = xattrs.remove(key.as_slice());
        drop(xattrs);

        let mut tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        Ok(())
    }

    fn set_symlink_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        target: &[u8],
    ) -> MetaResult<()> {
        let mut tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(ino);
        drop(tombstones);

        let mut symlinks = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        symlinks
            .insert(ino, target)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_symlink_overlay_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<()> {
        let mut symlinks = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = symlinks.remove(ino);
        drop(symlinks);

        let has_base = txn
            .open_table(BASE_SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some();

        let mut tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if has_base {
            tombstones
                .insert(ino, 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = tombstones.remove(ino);
        }
        Ok(())
    }

    fn set_xattr_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        name: &[u8],
        value: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let mut tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        drop(tombstones);

        Self::set_xattr_value_in_table_in_txn(txn, XATTRS_TABLE, ino, name, value)?;
        Ok(())
    }

    fn remove_xattr_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let mut xattrs = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = xattrs.remove(key.as_slice());
        drop(xattrs);

        let has_base = txn
            .open_table(BASE_XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some();

        let mut tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if has_base {
            tombstones
                .insert(key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = tombstones.remove(key.as_slice());
        }
        Ok(())
    }

    fn clear_inode_overlay_aux_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<()> {
        Self::remove_symlink_overlay_in_txn(txn, ino)?;

        let current_keys = {
            let txn_read = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = xattr_prefix(ino);
            let range = txn_read
                .range(prefix.as_ref()..)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut keys = Vec::new();
            for item in range {
                let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key_bytes = k.value();
                if !key_bytes.starts_with(&prefix) {
                    break;
                }
                keys.push(key_bytes.to_vec());
            }
            keys
        };
        for key in current_keys {
            let name = xattr_name_from_key(&key).to_vec();
            Self::remove_xattr_overlay_in_txn(txn, ino, &name)?;
        }

        Self::clear_regular_file_graph_in_txn(txn, ino)?;

        let base_keys = {
            let txn_read = txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = xattr_prefix(ino);
            let range = txn_read
                .range(prefix.as_ref()..)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut keys = Vec::new();
            for item in range {
                let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key_bytes = k.value();
                if !key_bytes.starts_with(&prefix) {
                    break;
                }
                keys.push(key_bytes.to_vec());
            }
            keys
        };
        for key in base_keys {
            let mut tombstones = txn
                .open_table(XATTR_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            tombstones
                .insert(key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        let acl_keys = {
            let table = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = acl_rule_prefix(ino);
            let range = table
                .range(prefix.as_ref()..)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut keys = Vec::new();
            for item in range {
                let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key_bytes = k.value();
                if !key_bytes.starts_with(&prefix) {
                    break;
                }
                keys.push(key_bytes.to_vec());
            }
            keys
        };
        if !acl_keys.is_empty() {
            let mut table = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for key in &acl_keys {
                let _ = table.remove(key.as_slice());
            }
            drop(table);
        }

        let base_acl_keys = {
            let table = txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = acl_rule_prefix(ino);
            let range = table
                .range(prefix.as_ref()..)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut keys = Vec::new();
            for item in range {
                let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key_bytes = k.value();
                if !key_bytes.starts_with(&prefix) {
                    break;
                }
                keys.push(key_bytes.to_vec());
            }
            keys
        };
        if !base_acl_keys.is_empty() {
            let mut tombstones = txn
                .open_table(ACL_RULE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for key in base_acl_keys {
                tombstones
                    .insert(key.as_slice(), 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn compute_committed_size_in_txn(
        txn: &redb::WriteTransaction,
        committed: &redb::Table<'_, &[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<u64> {
        if let Some(size) = Self::regular_file_committed_size_in_txn(txn, ino)? {
            return Ok(size);
        }

        let committed_blocks = Self::committed_manifest_in_txn(committed, ino)?;
        Ok(Self::legacy_committed_size_from_blocks(
            committed_blocks.as_slice(),
        ))
    }

    #[cfg(test)]
    fn regular_file_committed_size_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Option<u64>> {
        let table = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut size = 0u64;
        let mut saw_row = false;
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            let (decoded_ino, chunk_id) = decode_regular_file_chunk_key(key.value());
            debug_assert_eq!(decoded_ino, ino);
            saw_row = true;
            let chunk: RegularFileChunkState = decode(value.value())?;
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            for slice in chunk.slices.values() {
                if slice.block.is_some() {
                    let slice_end = chunk_start.saturating_add(slice.range.end as u64);
                    size = size.max(slice_end);
                }
            }
        }
        Ok(saw_row.then_some(size))
    }

    fn committed_manifest_in_txn(
        committed: &redb::Table<'_, &[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<(u32, BlockRef)>> {
        let prefix = file_block_prefix(ino);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;
        let mut out = Vec::new();
        for row in committed
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (decoded_ino, block_index) = decode_file_block_key(key.value());
            if decoded_ino != ino {
                break;
            }
            out.push((block_index, decode(value.value())?));
        }
        Ok(out)
    }

    fn current_manifest_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Vec<(u32, BlockRef)>> {
        let attr = Self::get_attr_from_write_table(txn, INODES_TABLE, ino)?
            .or_else(|| {
                Self::get_attr_from_write_table(txn, BASE_INODES_TABLE, ino)
                    .ok()
                    .flatten()
            })
            .ok_or(MetaError::InodeNotFound(ino))?;
        if attr.size == 0 {
            return Ok(Vec::new());
        }

        let spans = Self::lookup_regular_file_slices_in_write_txn(txn, ino, 0, attr.size)?;
        if !spans.is_empty() {
            let block_size = Self::regular_file_graph_block_size_from_write_txn(txn)?;
            let block_count = ((attr.size.saturating_sub(1)) / block_size as u64).saturating_add(1);
            let mut synthesized = Vec::new();
            for idx in 0..block_count {
                let block_index = idx as u32;
                if let Some(block) = Self::synthesize_block_ref_from_resolved_spans(
                    ino,
                    spans.as_slice(),
                    block_index,
                    block_size,
                )? {
                    synthesized.push((block_index, block));
                }
            }
            return Ok(synthesized);
        }

        let blocks = txn
            .open_table(FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let committed = txn
            .open_table(COMMITTED_FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = file_block_prefix(ino);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;
        let mut merged = std::collections::BTreeMap::new();
        for row in blocks
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (decoded_ino, block_index) = decode_file_block_key(key.value());
            if decoded_ino != ino {
                break;
            }
            merged.insert(block_index, decode(value.value())?);
        }
        for row in committed
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (decoded_ino, block_index) = decode_file_block_key(key.value());
            if decoded_ino != ino {
                break;
            }
            merged.entry(block_index).or_insert(decode(value.value())?);
        }
        Ok(merged.into_iter().collect())
    }

    fn format_info_from_write_txn(txn: &redb::WriteTransaction) -> MetaResult<FormatInfo> {
        let table = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let entry = table
            .get("format")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or(MetaError::NotFormatted)?;
        serde_json::from_slice(entry.value()).map_err(|e| MetaError::Serialization(e.to_string()))
    }

    fn regular_file_graph_block_size_from_write_txn(
        txn: &redb::WriteTransaction,
    ) -> MetaResult<u32> {
        Ok(Self::format_info_from_write_txn(txn)?.block_size)
    }

    fn transition_upload_state_for_local_mutation(
        attr: &mut InodeAttr,
        has_committed_manifest: bool,
    ) {
        if attr.kind != FileType::RegularFile {
            attr.upload_state = UploadState::Ready;
            return;
        }
        if attr.content_epoch == attr.committed_content_epoch && attr.size == attr.committed_size {
            attr.upload_state = UploadState::Ready;
        } else if has_committed_manifest
            || attr.committed_size > 0
            || attr.committed_content_epoch > 0
        {
            attr.upload_state = UploadState::Updating;
        } else {
            attr.upload_state = UploadState::Uploading;
        }
    }

    fn manifest_matches_committed_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<bool> {
        let current = Self::current_manifest_in_txn(txn, ino)?;
        let committed = txn
            .open_table(COMMITTED_FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(current == Self::committed_manifest_in_txn(&committed, ino)?)
    }

    /// Export full redb state as a serializable snapshot.
    pub fn export_snapshot(&self) -> MetaResult<RedbSnapshot> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut snapshot = RedbSnapshot::default();

        {
            let table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .settings
                    .push((k.value().to_string(), v.value().to_vec()));
            }
        }
        {
            let table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.counters.push((k.value().to_string(), v.value()));
            }
        }
        {
            let table = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.inodes.push((k.value(), v.value().to_vec()));
            }
        }
        {
            let table = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .dentries
                    .push((k.value().to_vec(), v.value().to_vec()));
            }
        }
        {
            let table = txn
                .open_table(FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .file_blocks
                    .push((k.value().to_vec(), v.value().to_vec()));
            }
        }
        {
            let table = txn
                .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .committed_file_blocks
                    .push((k.value().to_vec(), v.value().to_vec()));
            }
        }
        {
            snapshot.regular_file_overlay = Self::regular_file_snapshot_rows_from_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                |chunks| RegularFileOverlayState { chunks },
            )?;
        }
        {
            snapshot.regular_file_base = Self::regular_file_snapshot_rows_from_txn(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                |chunks| RegularFileBaseState { chunks },
            )?;
        }
        {
            let table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.pending_deletions.push((k.value(), v.value()));
            }
        }
        {
            let base = txn
                .open_table(BASE_SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let overlay = txn
                .open_table(SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let tombstones = txn
                .open_table(SYMLINK_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut merged = std::collections::BTreeMap::<u64, Vec<u8>>::new();
            for row in base.iter().map_err(|e| MetaError::Storage(e.to_string()))? {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.insert(k.value(), v.value().to_vec());
            }
            for row in tombstones
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, _v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.remove(&k.value());
            }
            for row in overlay
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.insert(k.value(), v.value().to_vec());
            }
            snapshot.symlinks.extend(merged);
        }
        {
            let base = txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let overlay = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let tombstones = txn
                .open_table(XATTR_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut merged = std::collections::BTreeMap::<Vec<u8>, Vec<u8>>::new();
            for row in base.iter().map_err(|e| MetaError::Storage(e.to_string()))? {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key = k.value().to_vec();
                let decoded = decode_xattr_value_from_storage(xattr_name_from_key(&key), v.value());
                merged.insert(key, decoded);
            }
            for row in tombstones
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, _v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.remove(k.value());
            }
            for row in overlay
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key = k.value().to_vec();
                let decoded = decode_xattr_value_from_storage(xattr_name_from_key(&key), v.value());
                merged.insert(key, decoded);
            }
            snapshot.xattrs.extend(merged);
        }
        {
            let base = txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let overlay = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let tombstones = txn
                .open_table(ACL_RULE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut merged = std::collections::BTreeMap::<Vec<u8>, u8>::new();
            for row in base.iter().map_err(|e| MetaError::Storage(e.to_string()))? {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.insert(k.value().to_vec(), v.value());
            }
            for row in tombstones
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, _v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.remove(k.value());
            }
            for row in overlay
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.insert(k.value().to_vec(), v.value());
            }
            snapshot.acl_rules.extend(merged);
        }
        {
            let table = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.event_log.push((k.value(), v.value().to_vec()));
            }
        }
        {
            let table = txn
                .open_table(SNAPSHOTS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .snapshots
                    .push((k.value().to_string(), v.value().to_vec()));
            }
        }

        Ok(snapshot)
    }

    /// Export a remote-visible snapshot suitable for PostgreSQL checkpoints.
    pub fn export_remote_snapshot(&self) -> MetaResult<RedbSnapshot> {
        let mut snapshot = self.export_snapshot()?;

        let mut projected_inodes = Vec::with_capacity(snapshot.inodes.len());
        for (ino, bytes) in snapshot.inodes.drain(..) {
            let mut attr: InodeAttr = decode(&bytes)?;
            if attr.kind == FileType::RegularFile {
                attr.size = attr.committed_size;
            }
            projected_inodes.push((ino, encode(&attr)?));
        }
        snapshot.inodes = projected_inodes;

        let mut referenced_objects = BTreeMap::new();
        for (_, bytes) in &snapshot.committed_file_blocks {
            let block: BlockRef = decode(bytes)?;
            referenced_objects
                .entry(block.object_id)
                .or_insert(block.length as u64);
        }
        for (_, bytes) in &snapshot.regular_file_base {
            let regular: RegularFileBaseState = decode(bytes)?;
            for chunk in regular.chunks.values() {
                for slice in chunk.slices.values() {
                    if let Some(block) = slice.block.as_ref() {
                        referenced_objects
                            .entry(block.object_id)
                            .or_insert(block.length as u64);
                    }
                }
            }
        }
        let committed_used_space = referenced_objects
            .values()
            .fold(0_u64, |acc, len| acc.saturating_add(*len));

        snapshot.file_blocks = snapshot.committed_file_blocks.clone();
        snapshot.regular_file_overlay = snapshot.regular_file_base.clone();
        snapshot.event_log.clear();
        snapshot
            .counters
            .retain(|(name, _)| name.as_str() != "nextEventSeq");
        if let Some((_, value)) = snapshot
            .counters
            .iter_mut()
            .find(|(name, _)| name.as_str() == counters::USED_SPACE)
        {
            *value = committed_used_space;
        } else {
            snapshot
                .counters
                .push((counters::USED_SPACE.to_string(), committed_used_space));
        }

        snapshot.snapshots.retain(|(_id, raw)| {
            decode::<MetadataSnapshot>(raw)
                .map(|snapshot| snapshot.state == SnapshotState::Synced)
                .unwrap_or(false)
        });

        Ok(snapshot)
    }

    /// Returns inodes that currently have local-only staged state.
    pub fn local_overlay_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::local_overlay_inodes_from_txn(&txn)
    }

    /// Returns inode attrs that diverged from the last imported/applied base view.
    pub fn local_overlay_attr_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::local_overlay_attr_inodes_from_txn(&txn)
    }

    /// Returns dentry keys `(parent, name)` that diverged from the base namespace.
    pub fn local_overlay_dentries(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();

        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }

        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }

        Ok(overlay)
    }

    pub fn local_overlay_symlink_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, current_bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ino = ino.value();
            match base
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_bytes) if base_bytes.value() == current_bytes.value() => {}
                _ => {
                    overlay.insert(ino);
                }
            }
        }
        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(ino.value());
        }
        Ok(overlay)
    }

    pub fn local_overlay_xattr_keys(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, current_value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == current_value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }
        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }

        Ok(overlay)
    }

    pub fn local_overlay_xattr_inodes(&self) -> MetaResult<HashSet<Ino>> {
        Ok(self
            .local_overlay_xattr_keys()?
            .into_iter()
            .map(|key| u64::from_le_bytes(key[..8].try_into().unwrap()))
            .collect())
    }

    pub fn local_overlay_acl_keys(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = txn
            .open_table(ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(ACL_RULE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut overlay = HashSet::new();
        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, current_value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == current_value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }
        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }
        Ok(overlay)
    }

    /// Returns true if local metadata still contains overlay state that is not
    /// yet remote-visible in the base view.
    pub fn has_local_overlay_state(&self) -> MetaResult<bool> {
        if !self.local_overlay_inodes()?.is_empty() {
            return Ok(true);
        }

        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let has_base_metadata = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .next()
            .is_some()
            || txn
                .open_table(BASE_DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(REGULAR_FILE_BASE_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some()
            || txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some();

        if !has_base_metadata {
            return Ok(false);
        }

        Ok(!self.local_overlay_attr_inodes()?.is_empty()
            || !self.local_overlay_dentries()?.is_empty()
            || !self.local_overlay_symlink_inodes()?.is_empty()
            || !self.local_overlay_xattr_keys()?.is_empty()
            || !self.local_overlay_acl_keys()?.is_empty())
    }

    /// Replace redb state with a provided snapshot.
    pub fn import_snapshot(&self, snapshot: &RedbSnapshot) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        Self::clear_table_in_txn(&txn, PENDING_REGULAR_FILE_COMPACTION_TABLE)?;

        {
            let mut table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.settings {
                table
                    .insert(k.as_str(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.counters {
                table
                    .insert(k.as_str(), *v)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.inodes {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(BASE_INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.inodes {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            Self::clear_table_in_txn(&txn, INODE_TOMBSTONES_TABLE)?;
        }
        {
            let mut table = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.dentries {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            Self::clear_table_in_txn(&txn, DENTRY_TOMBSTONES_TABLE)?;
        }
        {
            let mut table = txn
                .open_table(BASE_DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.dentries {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.file_blocks {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.committed_file_blocks {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            Self::replace_regular_file_chunk_rows_from_snapshot_rows(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                &snapshot.regular_file_overlay,
                |bytes| {
                    let state: RegularFileOverlayState = decode(bytes)?;
                    Ok(state.chunks)
                },
                true,
            )?;
        }
        {
            Self::replace_regular_file_chunk_rows_from_snapshot_rows(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                &snapshot.regular_file_base,
                |bytes| {
                    let state: RegularFileBaseState = decode(bytes)?;
                    Ok(state.chunks)
                },
                false,
            )?;
        }
        {
            let mut table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.pending_deletions {
                table
                    .insert(*k, *v)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            Self::clear_table_in_txn(&txn, SYMLINKS_TABLE)?;
        }
        {
            let mut table = txn
                .open_table(BASE_SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.symlinks {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            Self::clear_table_in_txn(&txn, SYMLINK_TOMBSTONES_TABLE)?;
        }
        {
            Self::clear_table_in_txn(&txn, XATTRS_TABLE)?;
        }
        {
            let mut table = txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.xattrs {
                let name = xattr_name_from_key(k.as_slice());
                let stored = encode_xattr_value_for_storage(name, v.as_slice());
                table
                    .insert(k.as_slice(), stored.as_ref())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            Self::clear_table_in_txn(&txn, XATTR_TOMBSTONES_TABLE)?;
        }
        {
            Self::clear_table_in_txn(&txn, ACL_RULES_TABLE)?;
        }
        {
            let mut table = txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.acl_rules {
                table
                    .insert(k.as_slice(), *v)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            Self::clear_table_in_txn(&txn, ACL_RULE_TOMBSTONES_TABLE)?;
        }
        {
            let mut table = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.event_log {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(SNAPSHOTS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.snapshots {
                table
                    .insert(k.as_str(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Read decoded events from local event log in [start_seq, end_seq).
    pub fn read_events_range(
        &self,
        start_seq: u64,
        end_seq: u64,
    ) -> MetaResult<Vec<crate::events::MetaEvent>> {
        if end_seq <= start_seq {
            return Ok(Vec::new());
        }
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .range(start_seq..end_seq)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_seq, bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ev: crate::events::MetaEvent = decode(bytes.value())?;
            out.push(ev);
        }
        Ok(out)
    }

    /// Apply only remote-visible auxiliary state from foreign replica events
    /// without re-appending it to the local event log. Namespace/attr state is
    /// kept in `base_*`.
    pub fn apply_replica_aux_state(&self, events: &[crate::events::MetaEvent]) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for event in events {
            match event {
                crate::events::MetaEvent::SetAttr { .. } => {}
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    let mut symlinks = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = symlinks.remove(*ino);
                    drop(symlinks);

                    let mut xattrs = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = xattr_prefix(*ino);
                    let keys_to_remove: Vec<Vec<u8>> = {
                        let range = xattrs
                            .range(prefix.as_ref()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        range
                            .take_while(|r| {
                                r.as_ref()
                                    .map(|(k, _)| k.value().starts_with(&prefix))
                                    .unwrap_or(false)
                            })
                            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                            .collect()
                    };
                    for key in keys_to_remove {
                        let _ = xattrs.remove(key.as_slice());
                    }

                    let mut blocks = txn
                        .open_table(FILE_BLOCKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = Self::remove_all_file_blocks_in_txn(&mut blocks, *ino)?;
                    drop(blocks);
                    let mut committed = txn
                        .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = Self::remove_all_committed_blocks_in_txn(&mut committed, *ino)?;
                }
                crate::events::MetaEvent::AddEntry { .. } => {}
                crate::events::MetaEvent::RemoveEntry { .. } => {}
                crate::events::MetaEvent::RenameEntry { .. } => {}
                crate::events::MetaEvent::SetFileBlock { .. } => {}
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    for (chunk_id, slices) in chunk_slices {
                        Self::set_regular_file_chunk_state_in_txn(
                            &txn,
                            REGULAR_FILE_BASE_TABLE,
                            false,
                            *ino,
                            *chunk_id,
                            slices,
                        )?;
                    }
                }

                crate::events::MetaEvent::RemoveFileBlock { ino, block_index } => {
                    let key = encode_file_block_key(*ino, *block_index);
                    let mut committed = txn
                        .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = committed.remove(key.as_slice());
                }
                crate::events::MetaEvent::RemoveFileBlocks { ino } => {
                    let mut blocks = txn
                        .open_table(FILE_BLOCKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = Self::remove_all_file_blocks_in_txn(&mut blocks, *ino)?;
                    drop(blocks);
                    let mut committed = txn
                        .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = Self::remove_all_committed_blocks_in_txn(&mut committed, *ino)?;
                }
                crate::events::MetaEvent::SetFileBlocksBatch { .. } => {}
                crate::events::MetaEvent::CommitFileBlock {
                    ino,
                    block_index,
                    block,
                } => {
                    let key = encode_file_block_key(*ino, *block_index);
                    let data = encode(block)?;

                    let mut committed = txn
                        .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    committed
                        .insert(key.as_slice(), data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::PublishFileManifest { ino, blocks } => {
                    let mut committed = txn
                        .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = Self::remove_all_committed_blocks_in_txn(&mut committed, *ino)?;
                    for (block_index, block) in blocks {
                        let key = encode_file_block_key(*ino, *block_index);
                        let data = encode(block)?;
                        committed
                            .insert(key.as_slice(), data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    let mut table = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*ino, target.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_value_in_table_in_txn(
                        &txn,
                        BASE_XATTRS_TABLE,
                        *ino,
                        name,
                        value,
                    )?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    table
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::AddPendingDeletion {
                    object_id,
                    timestamp,
                } => {
                    let mut table = txn
                        .open_table(PENDING_DELETIONS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*object_id, *timestamp)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemovePendingDeletion { object_id } => {
                    let mut table = txn
                        .open_table(PENDING_DELETIONS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = table.remove(*object_id);
                }
                crate::events::MetaEvent::CreateSnapshot { snapshot } => {
                    let mut table = txn
                        .open_table(SNAPSHOTS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let bytes = encode(snapshot)?;
                    table
                        .insert(snapshot.id.as_str(), bytes.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::UpdateSnapshotState { id, state } => {
                    let mut table = txn
                        .open_table(SNAPSHOTS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let existing_bytes = table
                        .get(id.as_str())
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|existing| existing.value().to_vec());
                    if let Some(existing_bytes) = existing_bytes {
                        let mut snapshot: MetadataSnapshot = decode(existing_bytes.as_slice())?;
                        snapshot.state = *state;
                        let bytes = encode(&snapshot)?;
                        table
                            .insert(id.as_str(), bytes.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                }
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Apply only namespace/attr portions of replica events into the remote-visible base view.
    pub fn apply_replica_events_to_base(
        &self,
        events: &[crate::events::MetaEvent],
    ) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for event in events {
            match event {
                crate::events::MetaEvent::SetAttr { attr } => {
                    let data = encode(attr)?;
                    let mut table = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(attr.ino, data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    let mut table = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = table.remove(*ino);

                    let mut symlinks = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = symlinks.remove(*ino);
                    drop(symlinks);

                    let mut xattrs = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = xattr_prefix(*ino);
                    let keys_to_remove: Vec<Vec<u8>> = {
                        let range = xattrs
                            .range(prefix.as_ref()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        range
                            .take_while(|r| {
                                r.as_ref()
                                    .map(|(k, _)| k.value().starts_with(&prefix))
                                    .unwrap_or(false)
                            })
                            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                            .collect()
                    };
                    for key in keys_to_remove {
                        let _ = xattrs.remove(key.as_slice());
                    }

                    let mut acl = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let prefix = acl_rule_prefix(*ino);
                    let keys_to_remove: Vec<Vec<u8>> = {
                        let range = acl
                            .range(prefix.as_ref()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        range
                            .take_while(|r| {
                                r.as_ref()
                                    .map(|(k, _)| k.value().starts_with(&prefix))
                                    .unwrap_or(false)
                            })
                            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                            .collect()
                    };
                    for key in keys_to_remove {
                        let _ = acl.remove(key.as_slice());
                    }
                }
                crate::events::MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    let key = encode_dentry_key(*parent, name);
                    let data = encode(entry)?;
                    let mut table = txn
                        .open_table(BASE_DENTRIES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(key.as_slice(), data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    let key = encode_dentry_key(*parent, name);
                    let mut table = txn
                        .open_table(BASE_DENTRIES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::RenameEntry {
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
                } => {
                    let src_key = encode_dentry_key(*parent, name);
                    let dst_key = encode_dentry_key(*new_parent, new_name);

                    let mut dentries = txn
                        .open_table(BASE_DENTRIES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let src_matches = dentries
                        .get(src_key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|value| decode::<DirEntryValue>(value.value()))
                        .transpose()?
                        .map(|current| current.ino == entry.ino && current.kind == entry.kind)
                        .unwrap_or(false);
                    if !src_matches {
                        continue;
                    }
                    let _ = dentries.remove(src_key.as_slice());
                    let _ = dentries.remove(dst_key.as_slice());
                    let data = encode(entry)?;
                    dentries
                        .insert(dst_key.as_slice(), data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    drop(dentries);

                    let mut inodes = txn
                        .open_table(BASE_INODES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    if let Some(attr) = displaced_set_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(ino) = displaced_remove_attr_ino {
                        let _ = inodes.remove(*ino);
                    }
                    if let Some(attr) = moved_dir_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(attr) = old_parent_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                    if let Some(attr) = new_parent_attr.as_ref() {
                        let data = encode(attr)?;
                        inodes
                            .insert(attr.ino, data.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    let mut table = txn
                        .open_table(BASE_SYMLINKS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    table
                        .insert(*ino, target.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_value_in_table_in_txn(
                        &txn,
                        BASE_XATTRS_TABLE,
                        *ino,
                        name,
                        value,
                    )?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    table
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut table = txn
                        .open_table(BASE_ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    for (chunk_id, slices) in chunk_slices {
                        Self::set_regular_file_chunk_state_in_txn(
                            &txn,
                            REGULAR_FILE_BASE_TABLE,
                            false,
                            *ino,
                            *chunk_id,
                            slices,
                        )?;
                    }
                }
                crate::events::MetaEvent::SetFileBlock { .. }
                | crate::events::MetaEvent::RemoveFileBlock { .. }
                | crate::events::MetaEvent::RemoveFileBlocks { .. }
                | crate::events::MetaEvent::SetFileBlocksBatch { .. }
                | crate::events::MetaEvent::CommitFileBlock { .. }
                | crate::events::MetaEvent::PublishFileManifest { .. }
                | crate::events::MetaEvent::AddPendingDeletion { .. }
                | crate::events::MetaEvent::RemovePendingDeletion { .. }
                | crate::events::MetaEvent::CreateSnapshot { .. }
                | crate::events::MetaEvent::UpdateSnapshotState { .. } => {}
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Sync the foreground serving tables to a remote-visible state for objects
    /// that do not currently have local overlay conflicts.
    pub fn apply_replica_serving_state(
        &self,
        events: &[crate::events::MetaEvent],
    ) -> MetaResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for event in events {
            match event {
                crate::events::MetaEvent::SetAttr { attr } => {
                    Self::set_inode_mirror_in_txn(&txn, attr)?;
                }
                crate::events::MetaEvent::RemoveAttr { ino } => {
                    Self::remove_inode_mirror_in_txn(&txn, *ino)?;
                    Self::remove_symlink_mirror_in_txn(&txn, *ino)?;

                    let current_keys =
                        Self::keys_for_inode_in_write_table(&txn, XATTRS_TABLE, *ino)?;
                    for key in current_keys {
                        let name = xattr_name_from_key(&key).to_vec();
                        Self::remove_xattr_mirror_in_txn(&txn, *ino, &name)?;
                    }
                    let base_keys =
                        Self::keys_for_inode_in_write_table(&txn, BASE_XATTRS_TABLE, *ino)?;
                    for key in base_keys {
                        let name = xattr_name_from_key(&key).to_vec();
                        Self::remove_xattr_mirror_in_txn(&txn, *ino, &name)?;
                    }

                    let prefix = acl_rule_prefix(*ino);
                    let mut acl_rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let acl_rule_keys: Vec<Vec<u8>> = {
                        let range = acl_rules
                            .range(prefix.as_ref()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        range
                            .take_while(|r| {
                                r.as_ref()
                                    .map(|(k, _)| k.value().starts_with(&prefix))
                                    .unwrap_or(false)
                            })
                            .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                            .collect()
                    };
                    for key in acl_rule_keys {
                        let _ = acl_rules.remove(key.as_slice());
                    }
                }
                crate::events::MetaEvent::AddEntry {
                    parent,
                    name,
                    entry,
                } => {
                    Self::set_dentry_mirror_in_txn(&txn, *parent, name, entry)?;
                }
                crate::events::MetaEvent::RemoveEntry { parent, name } => {
                    Self::remove_dentry_mirror_in_txn(&txn, *parent, name)?;
                }
                crate::events::MetaEvent::RenameEntry {
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
                } => {
                    let src_key = encode_dentry_key(*parent, name);
                    let src_matches = txn
                        .open_table(DENTRIES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .get(src_key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|value| decode::<DirEntryValue>(value.value()))
                        .transpose()?
                        .map(|current| current.ino == entry.ino && current.kind == entry.kind)
                        .unwrap_or(false);
                    if !src_matches {
                        continue;
                    }

                    Self::remove_dentry_mirror_in_txn(&txn, *parent, name)?;
                    Self::remove_dentry_mirror_in_txn(&txn, *new_parent, new_name)?;
                    Self::set_dentry_mirror_in_txn(&txn, *new_parent, new_name, entry)?;

                    if let Some(attr) = displaced_set_attr.as_ref() {
                        Self::set_inode_mirror_in_txn(&txn, attr)?;
                    }
                    if let Some(ino) = displaced_remove_attr_ino {
                        Self::remove_inode_mirror_in_txn(&txn, *ino)?;
                    }
                    if let Some(attr) = moved_dir_attr.as_ref() {
                        Self::set_inode_mirror_in_txn(&txn, attr)?;
                    }
                    if let Some(attr) = old_parent_attr.as_ref() {
                        Self::set_inode_mirror_in_txn(&txn, attr)?;
                    }
                    if let Some(attr) = new_parent_attr.as_ref() {
                        Self::set_inode_mirror_in_txn(&txn, attr)?;
                    }
                }
                crate::events::MetaEvent::SetSymlinkTarget { ino, target } => {
                    Self::set_symlink_mirror_in_txn(&txn, *ino, target)?;
                }
                crate::events::MetaEvent::SetXattr {
                    ino, name, value, ..
                } => {
                    Self::set_xattr_mirror_in_txn(&txn, *ino, name, value)?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    Self::remove_xattr_mirror_in_txn(&txn, *ino, name)?;
                }
                crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal,
                    access_level,
                } => {
                    let mut rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let mut tombstones = txn
                        .open_table(ACL_RULE_TOMBSTONES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    let _ = tombstones.remove(key.as_slice());
                    rules
                        .insert(key.as_slice(), *access_level as u8)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveAclRule { ino, principal } => {
                    let mut rules = txn
                        .open_table(ACL_RULES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let mut tombstones = txn
                        .open_table(ACL_RULE_TOMBSTONES_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_acl_rule_key(*ino, principal);
                    let _ = rules.remove(key.as_slice());
                    tombstones
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::SetRegularFileOverlaySlices { ino, chunk_slices } => {
                    for (chunk_id, slices) in chunk_slices {
                        Self::set_regular_file_chunk_state_in_txn(
                            &txn,
                            REGULAR_FILE_OVERLAY_TABLE,
                            true,
                            *ino,
                            *chunk_id,
                            slices,
                        )?;
                    }
                }
                crate::events::MetaEvent::SetFileBlock { .. }
                | crate::events::MetaEvent::RemoveFileBlock { .. }
                | crate::events::MetaEvent::RemoveFileBlocks { .. }
                | crate::events::MetaEvent::SetFileBlocksBatch { .. }
                | crate::events::MetaEvent::CommitFileBlock { .. }
                | crate::events::MetaEvent::PublishFileManifest { .. }
                | crate::events::MetaEvent::AddPendingDeletion { .. }
                | crate::events::MetaEvent::RemovePendingDeletion { .. }
                | crate::events::MetaEvent::CreateSnapshot { .. }
                | crate::events::MetaEvent::UpdateSnapshotState { .. } => {}
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn reconcile_serving_rename_from_base(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let src_in_base = Self::lookup_in_write_table(&txn, BASE_DENTRIES_TABLE, parent, name)?
            .map(|value| value.ino == entry.ino && value.kind == entry.kind)
            .unwrap_or(false);
        let dst_in_base =
            Self::lookup_in_write_table(&txn, BASE_DENTRIES_TABLE, new_parent, new_name)?
                .map(|value| value.ino == entry.ino && value.kind == entry.kind)
                .unwrap_or(false);

        if dst_in_base {
            Self::remove_dentry_mirror_in_txn(&txn, parent, name)?;
            Self::remove_dentry_mirror_in_txn(&txn, new_parent, new_name)?;
            Self::set_dentry_mirror_in_txn(&txn, new_parent, new_name, entry)?;
        } else if src_in_base {
            Self::remove_dentry_mirror_in_txn(&txn, new_parent, new_name)?;
            Self::set_dentry_mirror_in_txn(&txn, parent, name, entry)?;
        } else {
            Self::remove_dentry_mirror_in_txn(&txn, parent, name)?;
            Self::remove_dentry_mirror_in_txn(&txn, new_parent, new_name)?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn collect_snapshot_pinned_object_ids(&self, root_ino: Ino) -> MetaResult<Vec<u64>> {
        let mut pinned = HashSet::new();
        let mut stack = vec![root_ino];
        let mut visited = HashSet::new();

        while let Some(ino) = stack.pop() {
            if !visited.insert(ino) {
                continue;
            }
            let attr = self.get_attr(ino)?;
            match attr.kind {
                FileType::RegularFile => {
                    // Snapshots pin the visible file contents, so resolve the
                    // overlay/base view instead of reading legacy flat blocks.
                    for span in self.lookup_regular_file_slices(ino, 0, attr.size)? {
                        if let Some(block) = span.block {
                            pinned.insert(block.object_id);
                        }
                    }
                }
                FileType::Directory => {
                    for entry in self.readdir(ino)? {
                        stack.push(entry.ino);
                    }
                }
                FileType::Symlink => {}
            }
        }

        let mut out: Vec<u64> = pinned.into_iter().collect();
        out.sort_unstable();
        Ok(out)
    }
}

impl MetaFsOps for RedbMetaEngine {
    // --- Atomic Rename ---------------------------------------------------

    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<RenameAtomicResult> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let src_entry = Self::lookup_merged_in_write_txn(&txn, parent, name)?.ok_or_else(|| {
            MetaError::EntryNotFound {
                parent,
                name: String::from_utf8_lossy(name).to_string(),
            }
        })?;
        let dst_entry_opt = Self::lookup_merged_in_write_txn(&txn, new_parent, new_name)?;
        if let Some(dst_entry) = &dst_entry_opt
            && dst_entry.kind == FileType::Directory
            && !Self::readdir_merged_in_write_txn(&txn, dst_entry.ino)?.is_empty()
        {
            return Err(MetaError::DirNotEmpty(dst_entry.ino));
        }

        let moved_dir_seed = if src_entry.kind == FileType::Directory {
            Some(Self::get_attr_merged_in_write_txn(&txn, src_entry.ino)?)
        } else {
            None
        };
        let old_parent_seed = if src_entry.kind == FileType::Directory && parent != new_parent {
            Some(Self::get_attr_merged_in_write_txn(&txn, parent)?)
        } else {
            None
        };
        let new_parent_seed = if src_entry.kind == FileType::Directory && parent != new_parent {
            Some(Self::get_attr_merged_in_write_txn(&txn, new_parent)?)
        } else {
            None
        };
        let displaced_attr_seed = match &dst_entry_opt {
            Some(dst_entry) if dst_entry.kind != FileType::Directory => {
                Some(Self::get_attr_merged_in_write_txn(&txn, dst_entry.ino)?)
            }
            _ => None,
        };

        let displaced_ino: Option<Ino>;
        let mut displaced_attr_event: Option<crate::events::MetaEvent> = None;
        let mut moved_dir_attr = moved_dir_seed;
        let mut old_parent_attr = old_parent_seed;
        let mut new_parent_attr = new_parent_seed;
        {
            Self::remove_dentry_overlay_in_txn(&txn, parent, name)?;
            displaced_ino = if let Some(dst_entry) = dst_entry_opt.clone() {
                if dst_entry.kind == FileType::Directory {
                    Self::remove_inode_overlay_in_txn(&txn, dst_entry.ino)?;
                    Self::clear_inode_overlay_aux_in_txn(&txn, dst_entry.ino)?;
                } else {
                    if let Some(mut attr) = displaced_attr_seed.clone() {
                        attr.nlink = attr.nlink.saturating_sub(1);
                        if attr.nlink == 0 {
                            Self::remove_inode_overlay_in_txn(&txn, dst_entry.ino)?;
                            Self::clear_inode_overlay_aux_in_txn(&txn, dst_entry.ino)?;
                            displaced_attr_event =
                                Some(crate::events::MetaEvent::RemoveAttr { ino: dst_entry.ino });
                        } else {
                            Self::set_inode_overlay_in_txn(&txn, &attr)?;
                            displaced_attr_event =
                                Some(crate::events::MetaEvent::SetAttr { attr: attr.clone() });
                        }
                    }
                }
                Self::remove_dentry_overlay_in_txn(&txn, new_parent, new_name)?;
                Some(dst_entry.ino)
            } else {
                None
            };

            Self::set_dentry_overlay_in_txn(&txn, new_parent, new_name, &src_entry)?;

            if src_entry.kind == FileType::Directory && parent != new_parent {
                if let Some(mut attr) = moved_dir_attr.take() {
                    attr.parent = new_parent;
                    Self::set_inode_overlay_in_txn(&txn, &attr)?;
                    moved_dir_attr = Some(attr);
                }
                if let Some(mut attr) = old_parent_attr.take() {
                    attr.nlink = attr.nlink.saturating_sub(1);
                    attr.touch_mtime();
                    Self::set_inode_overlay_in_txn(&txn, &attr)?;
                    old_parent_attr = Some(attr);
                }
                if let Some(mut attr) = new_parent_attr.take() {
                    attr.nlink += 1;
                    attr.touch_mtime();
                    Self::set_inode_overlay_in_txn(&txn, &attr)?;
                    new_parent_attr = Some(attr);
                }
            }

            let displaced_set_attr = match &displaced_attr_event {
                Some(crate::events::MetaEvent::SetAttr { attr }) => Some(attr.clone()),
                _ => None,
            };
            let displaced_remove_attr_ino = match &displaced_attr_event {
                Some(crate::events::MetaEvent::RemoveAttr { ino }) => Some(*ino),
                _ => None,
            };
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RenameEntry {
                    parent,
                    name: name.to_vec(),
                    new_parent,
                    new_name: new_name.to_vec(),
                    entry: src_entry.clone(),
                    displaced_set_attr: Box::new(displaced_set_attr),
                    displaced_remove_attr_ino,
                    moved_dir_attr: Box::new(moved_dir_attr),
                    old_parent_attr: Box::new(old_parent_attr),
                    new_parent_attr: Box::new(new_parent_attr),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(RenameAtomicResult {
            moved_ino: src_entry.ino,
            displaced_ino,
        })
    }

    // --- Hardlinks -------------------------------------------------------

    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        let mut attr = self.get_attr(ino)?;
        let mut parent_attr = self.get_attr(new_parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(new_parent));
        }
        if self.lookup(new_parent, new_name).is_ok() {
            return Err(MetaError::EntryExists {
                parent: new_parent,
                name: String::from_utf8_lossy(new_name).to_string(),
            });
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let result;
        let parent_attr_event: Option<InodeAttr>;
        {
            attr.nlink += 1;
            attr.touch_ctime();
            Self::set_inode_overlay_in_txn(&txn, &attr)?;
            let entry = DirEntryValue {
                ino,
                kind: attr.kind,
            };
            Self::set_dentry_overlay_in_txn(&txn, new_parent, new_name, &entry)?;

            parent_attr.touch_mtime();
            Self::set_inode_overlay_in_txn(&txn, &parent_attr)?;
            parent_attr_event = Some(parent_attr.clone());

            result = attr;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr {
                    attr: result.clone(),
                },
            )?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::AddEntry {
                    parent: new_parent,
                    name: new_name.to_vec(),
                    entry: DirEntryValue {
                        ino,
                        kind: result.kind,
                    },
                },
            )?;
            if let Some(parent_attr) = &parent_attr_event {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr {
                        attr: parent_attr.clone(),
                    },
                )?;
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(result)
    }

    // --- High-Level Operations (ACID Atomicity) --------------------------

    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        let intent = PendingCreateRecord {
            ino,
            parent,
            name: name.to_vec(),
            kind: FileType::RegularFile,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        };
        self.add_pending_create(&intent)?;

        let create_result = (|| -> MetaResult<InodeAttr> {
            let txn = self
                .db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let result_attr;
            {
                let parent_attr = Self::get_attr_merged_in_write_txn(&txn, parent)?;
                if parent_attr.kind != FileType::Directory {
                    return Err(MetaError::NotADirectory(parent));
                }
                if Self::lookup_merged_in_write_txn(&txn, parent, name)?.is_some() {
                    return Err(MetaError::EntryExists {
                        parent,
                        name: String::from_utf8_lossy(name).to_string(),
                    });
                }

                let mut dentries = txn
                    .open_table(DENTRIES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut inodes = txn
                    .open_table(INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut counters = txn
                    .open_table(COUNTERS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                let dentry_key = encode_dentry_key(parent, name);

                let total_inodes = match counters
                    .get(counters::TOTAL_INODES)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    Some(v) => v.value(),
                    None => 0,
                };
                counters
                    .insert(counters::TOTAL_INODES, total_inodes + 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                let attr = InodeAttr::new_file(ino, parent, uid, gid, mode);
                let attr_data = encode(&attr)?;
                inodes
                    .insert(ino, attr_data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut inode_tombstones = txn
                    .open_table(INODE_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = inode_tombstones.remove(ino);
                drop(inode_tombstones);

                let entry_val = encode(&DirEntryValue {
                    ino,
                    kind: FileType::RegularFile,
                })?;
                dentries
                    .insert(dentry_key.as_slice(), entry_val.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut dentry_tombstones = txn
                    .open_table(DENTRY_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = dentry_tombstones.remove(dentry_key.as_slice());

                // redb does not allow opening the same table twice in one write txn
                // while an existing table handle is still alive.
                drop(counters);

                result_attr = attr.clone();

                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                )?;
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::AddEntry {
                        parent,
                        name: name.to_vec(),
                        entry: DirEntryValue {
                            ino,
                            kind: FileType::RegularFile,
                        },
                    },
                )?;
            }
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Ok(result_attr)
        })();

        match create_result {
            Ok(attr) => {
                self.remove_pending_create(ino)?;
                Ok(attr)
            }
            Err(err) => {
                let _ = self.remove_pending_create(ino);
                Err(err)
            }
        }
    }

    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        let intent = PendingCreateRecord {
            ino,
            parent,
            name: name.to_vec(),
            kind: FileType::Directory,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        };
        self.add_pending_create(&intent)?;

        let create_result = (|| -> MetaResult<InodeAttr> {
            let txn = self
                .db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let result_attr;
            {
                let mut parent_attr = Self::get_attr_merged_in_write_txn(&txn, parent)?;
                if parent_attr.kind != FileType::Directory {
                    return Err(MetaError::NotADirectory(parent));
                }
                if Self::lookup_merged_in_write_txn(&txn, parent, name)?.is_some() {
                    return Err(MetaError::EntryExists {
                        parent,
                        name: String::from_utf8_lossy(name).to_string(),
                    });
                }

                let mut dentries = txn
                    .open_table(DENTRIES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut inodes = txn
                    .open_table(INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut counters = txn
                    .open_table(COUNTERS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                let dentry_key = encode_dentry_key(parent, name);

                let total_inodes = match counters
                    .get(counters::TOTAL_INODES)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    Some(v) => v.value(),
                    None => 0,
                };
                counters
                    .insert(counters::TOTAL_INODES, total_inodes + 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                let attr = InodeAttr::new_dir(ino, parent, uid, gid, mode);
                let attr_data = encode(&attr)?;
                inodes
                    .insert(ino, attr_data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut inode_tombstones = txn
                    .open_table(INODE_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = inode_tombstones.remove(ino);
                let _ = inode_tombstones.remove(parent);
                drop(inode_tombstones);

                let entry_val = encode(&DirEntryValue {
                    ino,
                    kind: FileType::Directory,
                })?;
                dentries
                    .insert(dentry_key.as_slice(), entry_val.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut dentry_tombstones = txn
                    .open_table(DENTRY_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = dentry_tombstones.remove(dentry_key.as_slice());
                drop(dentry_tombstones);

                parent_attr.nlink += 1;
                parent_attr.touch_mtime();
                let p_data = encode(&parent_attr)?;
                inodes
                    .insert(parent, p_data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                // redb does not allow opening the same table twice in one write txn
                // while an existing table handle is still alive.
                drop(counters);

                result_attr = attr.clone();

                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                )?;
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr {
                        attr: parent_attr.clone(),
                    },
                )?;
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::AddEntry {
                        parent,
                        name: name.to_vec(),
                        entry: DirEntryValue {
                            ino,
                            kind: FileType::Directory,
                        },
                    },
                )?;
            }
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Ok(result_attr)
        })();

        match create_result {
            Ok(attr) => {
                self.remove_pending_create(ino)?;
                Ok(attr)
            }
            Err(err) => {
                let _ = self.remove_pending_create(ino);
                Err(err)
            }
        }
    }

    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr> {
        let ino = self.next_inode()?;
        let intent = PendingCreateRecord {
            ino,
            parent,
            name: name.to_vec(),
            kind: FileType::Symlink,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        };
        self.add_pending_create(&intent)?;

        let create_result = (|| -> MetaResult<InodeAttr> {
            let txn = self
                .db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let result_attr;
            {
                let parent_attr = Self::get_attr_merged_in_write_txn(&txn, parent)?;
                if parent_attr.kind != FileType::Directory {
                    return Err(MetaError::NotADirectory(parent));
                }
                if Self::lookup_merged_in_write_txn(&txn, parent, name)?.is_some() {
                    return Err(MetaError::EntryExists {
                        parent,
                        name: String::from_utf8_lossy(name).to_string(),
                    });
                }

                let mut dentries = txn
                    .open_table(DENTRIES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut inodes = txn
                    .open_table(INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut counters = txn
                    .open_table(COUNTERS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                let dentry_key = encode_dentry_key(parent, name);

                let total_inodes = match counters
                    .get(counters::TOTAL_INODES)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    Some(v) => v.value(),
                    None => 0,
                };
                counters
                    .insert(counters::TOTAL_INODES, total_inodes + 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                let attr = InodeAttr::new_symlink(ino, parent, uid, gid, target.len());
                let attr_data = encode(&attr)?;
                inodes
                    .insert(ino, attr_data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut inode_tombstones = txn
                    .open_table(INODE_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = inode_tombstones.remove(ino);
                drop(inode_tombstones);

                let entry_val = encode(&DirEntryValue {
                    ino,
                    kind: FileType::Symlink,
                })?;
                dentries
                    .insert(dentry_key.as_slice(), entry_val.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut dentry_tombstones = txn
                    .open_table(DENTRY_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = dentry_tombstones.remove(dentry_key.as_slice());
                drop(dentry_tombstones);

                Self::set_symlink_overlay_in_txn(&txn, ino, target)?;

                // redb does not allow opening the same table twice in one write txn
                // while an existing table handle is still alive.
                drop(counters);

                result_attr = attr.clone();

                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                )?;
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetSymlinkTarget {
                        ino,
                        target: target.to_vec(),
                    },
                )?;
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::AddEntry {
                        parent,
                        name: name.to_vec(),
                        entry: DirEntryValue {
                            ino,
                            kind: FileType::Symlink,
                        },
                    },
                )?;
            }
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Ok(result_attr)
        })();

        match create_result {
            Ok(attr) => {
                self.remove_pending_create(ino)?;
                Ok(attr)
            }
            Err(err) => {
                let _ = self.remove_pending_create(ino);
                Err(err)
            }
        }
    }

    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        let entry = self.lookup(parent, name)?;
        if entry.kind == FileType::Directory {
            return Err(MetaError::IsADirectory(entry.ino));
        }
        let mut attr = self.get_attr(entry.ino)?;
        let mut parent_attr = self.get_attr(parent)?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let result_attr;
        let parent_attr_event: Option<InodeAttr>;
        {
            Self::remove_dentry_overlay_in_txn(&txn, parent, name)?;

            attr.nlink = attr.nlink.saturating_sub(1);
            if attr.nlink == 0 {
                Self::remove_inode_overlay_in_txn(&txn, entry.ino)?;
                Self::clear_inode_overlay_aux_in_txn(&txn, entry.ino)?;

                let mut blocks_table = txn
                    .open_table(FILE_BLOCKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let block_prefix = file_block_prefix(entry.ino);
                let mut range_end = block_prefix.to_vec();
                *range_end.last_mut().unwrap() += 1;

                let keys_to_remove: Vec<Vec<u8>> = {
                    let iter = blocks_table
                        .range(block_prefix.as_slice()..range_end.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    iter.filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                        .collect()
                };
                for key in keys_to_remove {
                    let _ = blocks_table.remove(key.as_slice());
                }
            } else {
                Self::set_inode_overlay_in_txn(&txn, &attr)?;
            }

            parent_attr.touch_mtime();
            Self::set_inode_overlay_in_txn(&txn, &parent_attr)?;
            parent_attr_event = Some(parent_attr.clone());

            result_attr = attr;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveEntry {
                    parent,
                    name: name.to_vec(),
                },
            )?;
            if result_attr.nlink == 0 {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::RemoveFileBlocks {
                        ino: result_attr.ino,
                    },
                )?;
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::RemoveAttr {
                        ino: result_attr.ino,
                    },
                )?;
            } else {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr {
                        attr: result_attr.clone(),
                    },
                )?;
            }
            if let Some(parent_attr) = &parent_attr_event {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr {
                        attr: parent_attr.clone(),
                    },
                )?;
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(result_attr)
    }

    fn unlink_open_handle(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let result_attr;
        {
            let entry = Self::lookup_merged_in_write_txn(&txn, parent, name)?.ok_or(
                MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                },
            )?;
            if entry.kind == FileType::Directory {
                return Err(MetaError::IsADirectory(entry.ino));
            }

            let mut attr = Self::get_attr_merged_in_write_txn(&txn, entry.ino)?;
            attr.nlink = attr.nlink.saturating_sub(1);
            Self::set_inode_overlay_in_txn(&txn, &attr)?;

            let mut parent_attr = Self::get_attr_merged_in_write_txn(&txn, parent)?;
            parent_attr.touch_mtime();
            Self::set_inode_overlay_in_txn(&txn, &parent_attr)?;

            Self::remove_dentry_overlay_in_txn(&txn, parent, name)?;

            let created_at = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let record = DeferredDeleteRecord {
                ino: entry.ino,
                parent,
                name: name.to_vec(),
                created_at,
            };
            let mut deferred = txn
                .open_table(DEFERRED_DELETES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let encoded = encode(&record)?;
            deferred
                .insert(entry.ino, encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let undo = UndoLogRecord {
                op_id: Self::next_undo_op_id_in_txn(&txn)?,
                kind: UndoLogKind::DeferredDelete,
                phase: UndoLogPhase::Started,
                ino: entry.ino,
                parent,
                name: name.to_vec(),
                created_at,
            };
            Self::append_undo_log_in_txn(&txn, &undo)?;

            result_attr = attr.clone();

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveEntry {
                    parent,
                    name: name.to_vec(),
                },
            )?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr {
                    attr: result_attr.clone(),
                },
            )?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr {
                    attr: parent_attr.clone(),
                },
            )?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(result_attr)
    }

    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()> {
        let entry = self.lookup(parent, name)?;
        if entry.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(entry.ino));
        }
        if !self.readdir(entry.ino)?.is_empty() {
            return Err(MetaError::DirNotEmpty(entry.ino));
        }
        let mut parent_attr = self.get_attr(parent)?;

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_dentry_overlay_in_txn(&txn, parent, name)?;
            Self::remove_inode_overlay_in_txn(&txn, entry.ino)?;
            Self::clear_inode_overlay_aux_in_txn(&txn, entry.ino)?;

            parent_attr.nlink = parent_attr.nlink.saturating_sub(1);
            parent_attr.touch_mtime();
            Self::set_inode_overlay_in_txn(&txn, &parent_attr)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr {
                    attr: parent_attr.clone(),
                },
            )?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveAttr { ino: entry.ino },
            )?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveEntry {
                    parent,
                    name: name.to_vec(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }
}

impl RedbMetaEngine {
    fn next_undo_op_id_in_txn(txn: &redb::WriteTransaction) -> MetaResult<u64> {
        let mut counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = match counters
            .get(NEXT_UNDO_OP_ID_COUNTER)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => v.value(),
            None => 1,
        };
        let next = current.saturating_add(1);
        counters
            .insert(NEXT_UNDO_OP_ID_COUNTER, next)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(current)
    }

    fn append_undo_log_in_txn(
        txn: &redb::WriteTransaction,
        record: &UndoLogRecord,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(UNDO_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let encoded = encode(record)?;
        table
            .insert(record.op_id, encoded.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_undo_logs_for_in_txn(
        txn: &redb::WriteTransaction,
        kind: UndoLogKind,
        ino: Ino,
    ) -> MetaResult<()> {
        Self::set_undo_log_phase_for_in_txn(txn, kind, ino, UndoLogPhase::Completed)?;
        Ok(())
    }

    fn set_undo_log_phase_for(
        &self,
        kind: UndoLogKind,
        ino: Ino,
        phase: UndoLogPhase,
    ) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::set_undo_log_phase_for_in_txn(&txn, kind, ino, phase)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn set_undo_log_phase_for_in_txn(
        txn: &redb::WriteTransaction,
        kind: UndoLogKind,
        ino: Ino,
        phase: UndoLogPhase,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(UNDO_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut updates: Vec<(u64, UndoLogRecord)> = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut record: UndoLogRecord = decode(v.value())?;
            if record.kind == kind && record.ino == ino {
                record.phase = phase;
                updates.push((k.value(), record));
            }
        }
        for (key, record) in updates {
            let encoded = encode(&record)?;
            table
                .insert(key, encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    fn add_pending_create_intent(&self, intent: &PendingCreateRecord) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(PENDING_CREATES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let encoded = encode(intent)?;
            table
                .insert(intent.ino, encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let undo = UndoLogRecord {
                op_id: Self::next_undo_op_id_in_txn(&txn)?,
                kind: UndoLogKind::PendingCreate,
                phase: UndoLogPhase::Started,
                ino: intent.ino,
                parent: intent.parent,
                name: intent.name.clone(),
                created_at: intent.created_at,
            };
            Self::append_undo_log_in_txn(&txn, &undo)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_pending_create_intent(&self, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(PENDING_CREATES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = table
                .remove(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Self::remove_undo_logs_for_in_txn(&txn, UndoLogKind::PendingCreate, ino)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        self.cleanup_stale_undo_logs()?;
        Ok(())
    }

    fn list_pending_create_intents(&self) -> MetaResult<Vec<PendingCreateRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(PENDING_CREATES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            out.push(decode(v.value())?);
        }
        Ok(out)
    }

    fn recover_pending_create_intents(&self) -> MetaResult<Vec<Ino>> {
        let intents = self.list_pending_create_intents()?;
        if intents.is_empty() {
            return Ok(Vec::new());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut healed = 0u64;
        let mut touched_parents: HashSet<Ino> = HashSet::new();
        for intent in intents {
            Self::set_undo_log_phase_for_in_txn(
                &txn,
                UndoLogKind::PendingCreate,
                intent.ino,
                UndoLogPhase::Replaying,
            )?;
            let entry = Self::lookup_merged_in_write_txn(&txn, intent.parent, &intent.name)?;
            let inode_exists = Self::get_attr_merged_in_write_txn(&txn, intent.ino).is_ok();

            if let Some(entry) = entry
                && entry.ino == intent.ino
                && !inode_exists
            {
                Self::remove_dentry_overlay_in_txn(&txn, intent.parent, &intent.name)?;
                healed = healed.saturating_add(1);
                touched_parents.insert(intent.parent);
            }

            let mut table = txn
                .open_table(PENDING_CREATES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = table
                .remove(intent.ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Self::remove_undo_logs_for_in_txn(&txn, UndoLogKind::PendingCreate, intent.ino)?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if healed > 0 {
            debug!(healed, "recovered pending create intents");
        }
        Ok(touched_parents.into_iter().collect())
    }

    fn cleanup_stale_undo_logs(&self) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let undo_entries: Vec<(u64, UndoLogRecord)> = {
            let undo = txn
                .open_table(UNDO_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut entries = Vec::new();
            for row in undo.iter().map_err(|e| MetaError::Storage(e.to_string()))? {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let rec: UndoLogRecord = decode(v.value())?;
                entries.push((k.value(), rec));
            }
            entries
        };

        if undo_entries.is_empty() {
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            return Ok(());
        }

        let mut stale_op_ids = Vec::new();
        {
            let pending = txn
                .open_table(PENDING_CREATES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let deferred = txn
                .open_table(DEFERRED_DELETES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (op_id, rec) in &undo_entries {
                if rec.phase == UndoLogPhase::Completed {
                    stale_op_ids.push(*op_id);
                    continue;
                }
                let is_live = match rec.kind {
                    UndoLogKind::PendingCreate => pending
                        .get(rec.ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .is_some(),
                    UndoLogKind::DeferredDelete => deferred
                        .get(rec.ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .is_some(),
                };
                if !is_live {
                    stale_op_ids.push(*op_id);
                }
            }
        }

        if !stale_op_ids.is_empty() {
            let mut undo = txn
                .open_table(UNDO_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for op_id in stale_op_ids {
                let _ = undo
                    .remove(op_id)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }
}

impl MetaEngineCore for RedbMetaEngine {
    fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool> {
        Ok(true)
    }

    fn release_global_janitor_lock(&self) -> MetaResult<()> {
        Ok(())
    }

    fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        let mut pins = HashSet::new();
        for snapshot in self.list_snapshots()? {
            pins.extend(snapshot.pinned_object_ids);
        }
        Ok(pins)
    }
}

impl MetaFormat for RedbMetaEngine {
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()> {
        let json = serde_json::to_vec(info).map_err(|e| MetaError::Serialization(e.to_string()))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert("format", json.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            drop(table);
            if info.format_version == CURRENT_FORMAT_VERSION {
                let mut migration_status = txn
                    .open_table(MIGRATION_STATUS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                migration_status
                    .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_COMPLETED)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_format_info(&self) -> MetaResult<FormatInfo> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let entry = table
            .get("format")
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or(MetaError::NotFormatted)?;
        let info: FormatInfo = serde_json::from_slice(entry.value())
            .map_err(|e| MetaError::Serialization(e.to_string()))?;
        Ok(info)
    }
}
impl MetaInodes for RedbMetaEngine {
    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match table
            .get(name)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
    }

    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let new_val;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let current = match table
                .get(name)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 0,
            };
            new_val = current + delta;
            table
                .insert(name, new_val)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let new_val;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let current = match table
                .get(name)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 0,
            };
            new_val = current.saturating_sub(delta);
            table
                .insert(name, new_val)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(name, value)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn next_inode(&self) -> MetaResult<Ino> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let ino;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            ino = match table
                .get(counters::NEXT_INODE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => ROOT_INO + 1,
            };
            table
                .insert(counters::NEXT_INODE, ino + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(ino)
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;
        Self::resolve_attr_in_read_snapshot(&txn, ino, &overlay_block_inodes, &overlay_attr_inodes)
    }

    fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>> {
        if inos.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;
        let mut attrs = Vec::with_capacity(inos.len());
        for &ino in inos {
            match Self::resolve_attr_in_read_snapshot(
                &txn,
                ino,
                &overlay_block_inodes,
                &overlay_attr_inodes,
            ) {
                Ok(attr) => attrs.push(attr),
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(attrs)
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut normalized = attr.clone();
            if normalized.kind == FileType::RegularFile {
                let committed = txn
                    .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let has_committed_manifest =
                    !Self::committed_manifest_in_txn(&committed, normalized.ino)?.is_empty();
                Self::transition_upload_state_for_local_mutation(
                    &mut normalized,
                    has_committed_manifest,
                );
            }
            Self::set_inode_overlay_in_txn(&txn, &normalized)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr {
                    attr: normalized.clone(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_inode_overlay_in_txn(&txn, ino)?;

            Self::clear_inode_overlay_aux_in_txn(&txn, ino)?;

            Self::append_event_in_txn(&txn, &crate::events::MetaEvent::RemoveAttr { ino })?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list_inode_attrs(&self) -> MetaResult<Vec<InodeAttr>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;

        let mut inos = std::collections::BTreeSet::new();
        for table_def in [BASE_INODES_TABLE, INODES_TABLE] {
            let table = txn
                .open_table(table_def)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (ino, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                inos.insert(ino.value());
            }
        }

        let mut attrs = Vec::with_capacity(inos.len());
        for ino in inos {
            match Self::resolve_attr_in_read_snapshot(
                &txn,
                ino,
                &overlay_block_inodes,
                &overlay_attr_inodes,
            ) {
                Ok(attr) => attrs.push(attr),
                Err(MetaError::InodeNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(attrs)
    }
}
impl MetaNamespace for RedbMetaEngine {
    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_dentries = self.local_overlay_dentries()?;
        let key = encode_dentry_key(parent, name);
        if Self::has_slice_marker(&txn, DENTRY_TOMBSTONES_TABLE, key.as_slice())? {
            return Err(MetaError::EntryNotFound {
                parent,
                name: String::from_utf8_lossy(name).to_string(),
            });
        }

        if overlay_dentries.contains(&key) {
            return Self::lookup_in_table(&txn, DENTRIES_TABLE, parent, name)?.ok_or(
                MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                },
            );
        }

        if let Some(entry) = Self::lookup_in_table(&txn, BASE_DENTRIES_TABLE, parent, name)? {
            return Ok(entry);
        }

        Self::lookup_in_table(&txn, DENTRIES_TABLE, parent, name)?.ok_or(MetaError::EntryNotFound {
            parent,
            name: String::from_utf8_lossy(name).to_string(),
        })
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_dentries = self.local_overlay_dentries()?;
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut merged = std::collections::BTreeMap::<Vec<u8>, DirEntry>::new();
        for entry in Self::readdir_from_table(&txn, BASE_DENTRIES_TABLE, parent)? {
            merged.insert(entry.name.clone(), entry);
        }

        let prefix = dentry_prefix(parent);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;
        for row in tombstones
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (decoded_parent, name) = decode_dentry_key(key.value());
            if decoded_parent != parent {
                break;
            }
            merged.remove(name);
        }

        for entry in Self::readdir_from_table(&txn, DENTRIES_TABLE, parent)? {
            let key = encode_dentry_key(parent, &entry.name);
            if overlay_dentries.contains(&key) {
                merged.insert(entry.name.clone(), entry);
            }
        }

        let base_names: std::collections::HashSet<Vec<u8>> =
            Self::readdir_from_table(&txn, BASE_DENTRIES_TABLE, parent)?
                .into_iter()
                .map(|entry| entry.name)
                .collect();
        for key in overlay_dentries {
            let (overlay_parent, overlay_name) = decode_dentry_key(&key);
            if overlay_parent != parent {
                continue;
            }
            if !base_names.contains(overlay_name)
                && let Some(entry) =
                    Self::lookup_in_table(&txn, DENTRIES_TABLE, parent, overlay_name)?
            {
                merged.insert(
                    overlay_name.to_vec(),
                    DirEntry {
                        name: overlay_name.to_vec(),
                        ino: entry.ino,
                        kind: entry.kind,
                    },
                );
            }
            if base_names.contains(overlay_name)
                && Self::lookup_in_table(&txn, DENTRIES_TABLE, parent, overlay_name)?.is_none()
            {
                merged.remove(overlay_name);
            }
        }

        Ok(merged.into_values().collect())
    }

    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        if self.lookup(parent, name).is_ok() {
            return Err(MetaError::EntryExists {
                parent,
                name: String::from_utf8_lossy(name).to_string(),
            });
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::set_dentry_overlay_in_txn(&txn, parent, name, entry)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::AddEntry {
                    parent,
                    name: name.to_vec(),
                    entry: entry.clone(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        let entry_val = self.lookup(parent, name)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_dentry_overlay_in_txn(&txn, parent, name)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveEntry {
                    parent,
                    name: name.to_vec(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(entry_val)
    }
}
impl MetaBlocks for RedbMetaEngine {
    fn get_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>> {
        let block_size = self.regular_file_graph_block_size()?;
        let block_start = (block_index as u64).saturating_mul(block_size as u64);
        let spans = self.lookup_regular_file_slices(ino, block_start, block_size as u64)?;
        if !spans.is_empty() {
            return Self::synthesize_block_ref_from_resolved_spans(
                ino,
                spans.as_slice(),
                block_index,
                block_size,
            );
        }

        let key = encode_file_block_key(ino, block_index);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match table
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(entry) => {
                let block: BlockRef = decode(entry.value())?;
                Ok(Some(block))
            }
            None => {
                let committed = txn
                    .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                match committed
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    Some(entry) => {
                        let block: BlockRef = decode(entry.value())?;
                        Ok(Some(block))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    fn set_file_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>> {
        let key = encode_file_block_key(ino, block_index);
        let data = encode(block)?;
        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let old_block;
        {
            let mut table = txn
                .open_table(FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // Get old block if exists (for GC)
            old_block = if let Some(entry) = table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let old: BlockRef = decode(entry.value())?;
                Some(old)
            } else {
                None
            };

            table
                .insert(key.as_slice(), data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let mut inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| g.value().to_vec());
            if let Some(attr_bytes) = attr_bytes {
                let mut attr: InodeAttr = decode(&attr_bytes)?;
                let committed = txn
                    .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let has_committed_manifest =
                    !Self::committed_manifest_in_txn(&committed, ino)?.is_empty();
                Self::transition_upload_state_for_local_mutation(&mut attr, has_committed_manifest);
                let encoded = encode(&attr)?;
                inodes
                    .insert(ino, encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }

        let current_blocks = Self::get_file_blocks_from_txn_internal(&txn, FILE_BLOCKS_TABLE, ino)?;
        Self::replace_regular_file_chunk_rows_from_blocks_in_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
            current_blocks.as_slice(),
            block_size,
            false,
            true,
        )?;

        Self::append_event_in_txn(
            &txn,
            &crate::events::MetaEvent::SetFileBlock {
                ino,
                block_index,
                block: block.clone(),
            },
        )?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(old_block)
    }

    fn get_file_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>> {
        let block_size = self.regular_file_graph_block_size()?;
        let file_size = match self.get_attr(ino) {
            Ok(attr) => attr.size,
            Err(MetaError::InodeNotFound(_)) => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };

        if file_size > 0 {
            let spans = self.lookup_regular_file_slices(ino, 0, file_size)?;
            if !spans.is_empty() {
                let block_count =
                    ((file_size.saturating_sub(1)) / block_size as u64).saturating_add(1);
                let mut synthesized = Vec::new();
                for idx in 0..block_count {
                    let block_index = idx as u32;
                    if let Some(block) = Self::synthesize_block_ref_from_resolved_spans(
                        ino,
                        spans.as_slice(),
                        block_index,
                        block_size,
                    )? {
                        synthesized.push((block_index, block));
                    }
                }
                return Ok(synthesized);
            }
        }

        let prefix = file_block_prefix(ino);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let committed = txn
            .open_table(COMMITTED_FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut blocks = std::collections::BTreeMap::new();

        // Scan all keys starting with the inode prefix
        let mut range_end = prefix.to_vec();
        // Increment last byte to get exclusive upper bound
        *range_end.last_mut().unwrap() += 1;

        let iter = table
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for result in iter {
            let (key, value) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value();
            let (decoded_ino, block_index) = decode_file_block_key(key_bytes);
            if decoded_ino != ino {
                break;
            }
            let block: BlockRef = decode(value.value())?;
            blocks.insert(block_index, block);
        }

        let iter = committed
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for result in iter {
            let (key, value) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value();
            let (decoded_ino, block_index) = decode_file_block_key(key_bytes);
            if decoded_ino != ino {
                break;
            }
            blocks.entry(block_index).or_insert(decode(value.value())?);
        }
        Ok(blocks.into_iter().collect())
    }

    fn lookup_regular_file_chunk_slices(
        &self,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_range: SliceRange,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_table = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_table = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let key = encode_regular_file_chunk_key(ino, chunk_id);
        let overlay: Option<RegularFileChunkState> = overlay_table
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode(value.value()))
            .transpose()?;
        let base: Option<RegularFileChunkState> = base_table
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode(value.value()))
            .transpose()?;
        Ok(resolve_chunk_slice_spans(
            chunk_id,
            chunk_range,
            overlay.as_ref(),
            base.as_ref(),
        ))
    }

    fn lookup_regular_file_slices(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| MetaError::Storage("regular-file lookup range overflow".to_string()))?;
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_table = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_table = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut spans = Vec::new();
        let mut cursor = offset;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64);
            let query_end = end.min(chunk_end);
            let start_in_chunk = (cursor - chunk_start) as u32;
            let end_in_chunk = (query_end - chunk_start) as u32;
            let query = SliceRange::new(start_in_chunk, end_in_chunk).ok_or_else(|| {
                MetaError::Storage("invalid chunk-local lookup range".to_string())
            })?;
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let overlay: Option<RegularFileChunkState> = overlay_table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|value| decode(value.value()))
                .transpose()?;
            let base: Option<RegularFileChunkState> = base_table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|value| decode(value.value()))
                .transpose()?;
            spans.extend(resolve_chunk_slice_spans(
                chunk_id,
                query,
                overlay.as_ref(),
                base.as_ref(),
            ));
            cursor = query_end;
        }
        Ok(merge_adjacent_spans(spans))
    }

    fn remove_file_blocks(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        let prefix = file_block_prefix(ino);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let removed_ids;
        {
            let mut table = txn
                .open_table(FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut committed = txn
                .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // Collect keys to remove
            let mut range_end = prefix.to_vec();
            *range_end.last_mut().unwrap() += 1;

            // First pass: collect all block IDs
            let blocks_to_remove: Vec<(Vec<u8>, u64)> = {
                let iter = table
                    .range(prefix.as_slice()..range_end.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                iter.filter_map(|r| r.ok())
                    .filter_map(|(k, v)| {
                        let block: BlockRef = decode(v.value()).ok()?;
                        Some((k.value().to_vec(), block.object_id))
                    })
                    .collect()
            };

            // Extract just the IDs for return
            removed_ids = blocks_to_remove.iter().map(|(_, id)| *id).collect();

            // Remove all entries
            for (key, _) in &blocks_to_remove {
                table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = committed.remove(key.as_slice());
            }

            Self::clear_regular_file_graph_in_txn(&txn, ino)?;

            Self::append_event_in_txn(&txn, &crate::events::MetaEvent::RemoveFileBlocks { ino })?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(removed_ids)
    }

    fn remove_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<u64>> {
        let key = encode_file_block_key(ino, block_index);
        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let removed_id;
        let mut updated_attr: Option<InodeAttr> = None;
        let mut committed_manifest: Option<Vec<(u32, BlockRef)>> = None;
        {
            let mut table = txn
                .open_table(FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut committed = txn
                .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            if let Some(entry) = table
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let block: BlockRef = decode(entry.value())?;
                removed_id = Some(block.object_id);
                let _ = committed.remove(key.as_slice());

                let current_committed = Self::committed_manifest_in_txn(&committed, ino)?;
                let new_committed_size =
                    Self::legacy_committed_size_from_blocks(current_committed.as_slice());
                committed_manifest = Some(current_committed);
                let mut inodes = txn
                    .open_table(INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let attr_bytes = inodes
                    .get(ino)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .map(|g| g.value().to_vec());
                if let Some(attr_bytes) = attr_bytes {
                    let mut attr: InodeAttr = decode(&attr_bytes)?;
                    attr.committed_size = new_committed_size;
                    let has_committed_manifest = new_committed_size > 0;
                    Self::transition_upload_state_for_local_mutation(
                        &mut attr,
                        has_committed_manifest,
                    );
                    let data = encode(&attr)?;
                    inodes
                        .insert(ino, data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    updated_attr = Some(attr);
                }

                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::RemoveFileBlock { ino, block_index },
                )?;
                if let Some(attr) = &updated_attr {
                    Self::append_event_in_txn(
                        &txn,
                        &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                    )?;
                }
            } else {
                removed_id = None;
            }
        }

        if removed_id.is_some() {
            let current_blocks =
                Self::get_file_blocks_from_txn_internal(&txn, FILE_BLOCKS_TABLE, ino)?;
            Self::replace_regular_file_chunk_rows_from_blocks_in_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
                current_blocks.as_slice(),
                block_size,
                false,
                true,
            )?;

            if let Some(committed_manifest) = committed_manifest.as_ref() {
                Self::replace_regular_file_chunk_rows_from_blocks_in_txn(
                    &txn,
                    REGULAR_FILE_BASE_TABLE,
                    ino,
                    committed_manifest.as_slice(),
                    block_size,
                    true,
                    false,
                )?;
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(removed_id)
    }

    fn next_block_id(&self) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let id;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            id = match table
                .get(counters::NEXT_BLOCK_ID)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 1, // Start from 1
            };
            table
                .insert(counters::NEXT_BLOCK_ID, id + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(id)
    }

    /// Allocate `count` consecutive block IDs in a single redb transaction.
    fn next_block_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let first_id;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            first_id = match table
                .get(counters::NEXT_BLOCK_ID)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 1,
            };
            table
                .insert(counters::NEXT_BLOCK_ID, first_id + count as u64)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok((first_id..first_id + count as u64).collect())
    }

    /// Flush a batch of block writes in a **single** redb transaction.
    ///
    /// This replaces N individual transactions (each with its own fsync) with
    /// one transaction - reducing commit overhead by ~100-300x.
    fn flush_blocks_batch(&self, entries: &[BatchFlushEntry]) -> MetaResult<Vec<BatchFlushResult>> {
        self.flush_blocks_batch_with_attr_txn(entries, None)
    }

    fn flush_regular_file_overlay_slices(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
    ) -> MetaResult<()> {
        self.flush_regular_file_overlay_slices_with_attr_txn(ino, chunk_slices, None)
    }

    fn has_local_regular_file_overlay(&self, ino: Ino) -> MetaResult<bool> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::has_regular_file_chunk_rows_in_read_txn(&txn, REGULAR_FILE_OVERLAY_TABLE, ino)
    }

    fn get_regular_file_overlay_object_ids(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut ids = std::collections::BTreeSet::new();
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            let chunk: RegularFileChunkState = decode(value.value())?;
            for slice in chunk.slices.values() {
                if let Some(block) = &slice.block {
                    ids.insert(block.object_id);
                }
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn commit_uploaded_block(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool> {
        let key = encode_file_block_key(ino, block_index);
        let data = encode(block)?;
        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let committed_manifest = {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(false),
            };
            let mut attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(false);
            }
            drop(inodes);

            let blocks = txn
                .open_table(FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let current = blocks
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| decode::<BlockRef>(g.value()))
                .transpose()?;
            drop(blocks);
            let Some(current) = current else {
                return Ok(false);
            };
            if current != *block {
                return Ok(false);
            }

            let mut committed = txn
                .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let old_committed = committed
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| decode::<BlockRef>(g.value()))
                .transpose()?;

            committed
                .insert(key.as_slice(), data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let committed_manifest = Self::committed_manifest_in_txn(&committed, ino)?;
            let new_committed_size =
                Self::legacy_committed_size_from_blocks(committed_manifest.as_slice());
            drop(committed);

            let previous_committed_size = attr.committed_size;
            let previous_committed_epoch = attr.committed_content_epoch;
            let previous_upload_state = attr.upload_state;
            let manifest_matches_committed = Self::manifest_matches_committed_in_txn(&txn, ino)?;
            attr.committed_size = new_committed_size;
            if manifest_matches_committed {
                attr.committed_content_epoch = attr.content_epoch;
                attr.upload_state = UploadState::Ready;
            } else {
                Self::transition_upload_state_for_local_mutation(&mut attr, new_committed_size > 0);
            }
            let committed_changed = previous_committed_size != attr.committed_size
                || previous_committed_epoch != attr.committed_content_epoch
                || previous_upload_state != attr.upload_state;
            let attr_data = encode(&attr)?;
            let mut inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            inodes
                .insert(ino, attr_data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            drop(inodes);

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::CommitFileBlock {
                    ino,
                    block_index,
                    block: block.clone(),
                },
            )?;
            if committed_changed {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                )?;
            }

            if let Some(old) = old_committed
                && old.object_id != block.object_id
            {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                let mut pending_del_table = txn
                    .open_table(PENDING_DELETIONS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                pending_del_table
                    .insert(old.object_id, timestamp)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                drop(pending_del_table);
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::AddPendingDeletion {
                        object_id: old.object_id,
                        timestamp,
                    },
                )?;
            }
            committed_manifest
        };

        Self::replace_regular_file_chunk_rows_from_blocks_in_txn(
            &txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
            committed_manifest.as_slice(),
            block_size,
            true,
            false,
        )?;

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn publish_file_manifest_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        expected_manifest: &[(u32, BlockRef)],
    ) -> MetaResult<bool> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let published_blocks;
        {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(false),
            };
            let mut attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(false);
            }
            drop(inodes);

            published_blocks = Self::current_manifest_in_txn(&txn, ino)?;
            if published_blocks.as_slice() != expected_manifest {
                return Ok(false);
            }

            let publish_regular_graph = Self::has_regular_file_chunk_rows_in_write_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
            )?;
            let regular_graph_committed_size = if publish_regular_graph {
                Some(Self::publish_regular_file_overlay_to_base_in_txn(
                    &txn, ino,
                )?)
            } else {
                None
            };

            let mut committed = txn
                .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let old_committed = Self::remove_all_committed_blocks_in_txn(&mut committed, ino)?;
            let mut surviving = std::collections::HashSet::new();
            for (block_index, block) in &published_blocks {
                let key = encode_file_block_key(ino, *block_index);
                let data = encode(block)?;
                committed
                    .insert(key.as_slice(), data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                surviving.insert(block.object_id);
            }
            drop(committed);

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            for object_id in old_committed {
                if surviving.contains(&object_id) {
                    continue;
                }
                let mut pending_del_table = txn
                    .open_table(PENDING_DELETIONS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                pending_del_table
                    .insert(object_id, timestamp)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                drop(pending_del_table);
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::AddPendingDeletion {
                        object_id,
                        timestamp,
                    },
                )?;
            }

            attr.committed_size = regular_graph_committed_size
                .map(|size| size.min(attr.size))
                .unwrap_or(attr.size);
            attr.committed_content_epoch = attr.content_epoch;
            attr.upload_state = UploadState::Ready;
            let attr_data = encode(&attr)?;
            let mut inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            inodes
                .insert(ino, attr_data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::PublishFileManifest {
                    ino,
                    blocks: published_blocks.clone(),
                },
            )?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
            )?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn publish_regular_file_overlay_to_base_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
    ) -> MetaResult<bool> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        {
            let mut inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(false),
            };
            let mut attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(false);
            }

            let committed_size = Self::publish_regular_file_overlay_to_base_in_txn(&txn, ino)?;
            attr.committed_size = committed_size.min(attr.size);
            attr.committed_content_epoch = attr.content_epoch;
            attr.upload_state = UploadState::Ready;
            let encoded = encode(&attr)?;
            inodes
                .insert(ino, encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
            )?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn compact_fragmented_regular_file_chunks(&self, max_chunks: usize) -> MetaResult<usize> {
        if max_chunks == 0 {
            return Ok(0);
        }

        let mut compacted = 0usize;
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let pending_table = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut candidates: Vec<(Ino, ChunkId)> = Vec::new();

        for row in pending_table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (ino, chunk_id) = decode_regular_file_chunk_key(key.value());
            candidates.push((ino, chunk_id));
            if candidates.len() >= max_chunks {
                break;
            }
        }
        drop(pending_table);
        drop(txn);

        for (ino, chunk_id) in candidates {
            if compacted >= max_chunks {
                break;
            }

            let txn = self
                .db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut overlay_table = txn
                .open_table(REGULAR_FILE_OVERLAY_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut pending_table = txn
                .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let current = overlay_table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|value| decode::<RegularFileChunkState>(value.value()))
                .transpose()?;

            let Some(current) = current else {
                let _ = pending_table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                drop(pending_table);
                drop(overlay_table);
                txn.commit()
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                continue;
            };
            if !current.fragmentation_metrics().should_compact() {
                let _ = pending_table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                drop(pending_table);
                drop(overlay_table);
                txn.commit()
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                continue;
            }

            let Some(compacted_chunk) = Self::compact_regular_file_chunk_state(chunk_id, &current)?
            else {
                let _ = overlay_table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = pending_table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                        ino,
                        chunk_slices: vec![(chunk_id, Vec::new())],
                    },
                )?;
                drop(pending_table);
                drop(overlay_table);
                txn.commit()
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                compacted = compacted.saturating_add(1);
                continue;
            };

            if compacted_chunk == current {
                let _ = pending_table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                drop(pending_table);
                drop(overlay_table);
                txn.commit()
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                continue;
            }

            let encoded = encode(&compacted_chunk)?;
            overlay_table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let compacted_slices: Vec<crate::schema::SliceRecord> =
                compacted_chunk.slices.values().cloned().collect();
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                    ino,
                    chunk_slices: vec![(chunk_id, compacted_slices)],
                },
            )?;

            if compacted_chunk.fragmentation.should_compact() {
                let _ = pending_table
                    .insert(key.as_slice(), 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            } else {
                let _ = pending_table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            drop(pending_table);
            drop(overlay_table);
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            compacted = compacted.saturating_add(1);
        }

        Ok(compacted)
    }
}

impl RedbMetaEngine {
    fn flush_regular_file_overlay_slices_with_attr_txn(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let normalized_attr = if let Some(attr) = attr {
            let committed = txn
                .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut normalized = attr.clone();
            if normalized.kind == FileType::RegularFile {
                let has_committed_manifest =
                    !Self::committed_manifest_in_txn(&committed, normalized.ino)?.is_empty();
                Self::transition_upload_state_for_local_mutation(
                    &mut normalized,
                    has_committed_manifest,
                );
            }
            Some(normalized)
        } else {
            None
        };
        let mut final_chunk_slices: Vec<(ChunkId, Vec<crate::schema::SliceRecord>)> =
            Vec::with_capacity(chunk_slices.len());

        {
            let mut overlay_table = txn
                .open_table(REGULAR_FILE_OVERLAY_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut counters = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut pending_compaction = txn
                .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let mut space_delta: i64 = 0;

            for (chunk_id, new_slices) in chunk_slices {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let mut state = if let Some(entry) = overlay_table
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    decode::<crate::schema::RegularFileChunkState>(entry.value())?
                } else {
                    crate::schema::RegularFileChunkState::default()
                };

                // Pre-calculate which existing slices are fully covered by the incoming ones.
                // This bounds metadata growth and performs O(N*M) where N is incoming slices and M is existing in one chunk.
                let mut fully_covered = Vec::new();
                for (id, existing) in &state.slices {
                    for new_slice in new_slices.iter() {
                        if new_slice.range.start <= existing.range.start
                            && new_slice.range.end >= existing.range.end
                        {
                            fully_covered.push(*id);
                            break;
                        }
                    }
                }

                for id in fully_covered {
                    if let Some(removed) = state.slices.remove(&id) {
                        if let Some(block) = &removed.block {
                            space_delta -= block.length as i64;
                        }
                    }
                }

                for mut new_slice in new_slices.iter().cloned() {
                    let slice_id = state
                        .try_alloc_slice_id()
                        .ok_or_else(|| MetaError::Storage("slice id overflow".to_string()))?;
                    let slice_order = state
                        .try_alloc_slice_order()
                        .ok_or_else(|| MetaError::Storage("slice order overflow".to_string()))?;
                    new_slice.slice_id = slice_id;
                    new_slice.order = slice_order;

                    if let Some(block) = &new_slice.block {
                        space_delta += block.length as i64;
                    }

                    state.slices.insert(slice_id, new_slice);
                }

                state.refresh_fragmentation_metrics();

                if state.fragmentation.should_compact() {
                    pending_compaction
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                } else {
                    let _ = pending_compaction
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }

                let data = encode(&state)?;
                overlay_table
                    .insert(key.as_slice(), data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                final_chunk_slices.push((*chunk_id, state.slices.values().cloned().collect()));
            }

            if space_delta != 0 {
                let current_used = match counters
                    .get(counters::USED_SPACE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    Some(v) => v.value(),
                    None => 0,
                };
                counters
                    .insert(
                        counters::USED_SPACE,
                        current_used.saturating_add_signed(space_delta),
                    )
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            if let Some(attr) = normalized_attr.as_ref() {
                Self::set_inode_overlay_in_txn(&txn, attr)?;
            }
        }

        Self::append_event_in_txn(
            &txn,
            &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                ino,
                chunk_slices: final_chunk_slices,
            },
        )?;
        if let Some(attr) = normalized_attr.as_ref() {
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
            )?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn flush_blocks_batch_with_attr_txn(
        &self,
        entries: &[BatchFlushEntry],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Vec<BatchFlushResult>> {
        if entries.is_empty() && attr.is_none() {
            return Ok(Vec::new());
        }

        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut results = Vec::with_capacity(entries.len());
        let mut used_space_delta: i64 = 0;

        let normalized_attr = if let Some(attr) = attr {
            let committed = txn
                .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut normalized = attr.clone();
            if normalized.kind == FileType::RegularFile {
                let has_committed_manifest =
                    !Self::committed_manifest_in_txn(&committed, normalized.ino)?.is_empty();
                Self::transition_upload_state_for_local_mutation(
                    &mut normalized,
                    has_committed_manifest,
                );
            }
            Some(normalized)
        } else {
            None
        };

        {
            let mut touched_inodes = std::collections::BTreeSet::new();
            if !entries.is_empty() {
                let mut blocks_table = txn
                    .open_table(FILE_BLOCKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                for e in entries {
                    let key = encode_file_block_key(e.ino, e.block_index);
                    let block_ref = BlockRef {
                        object_id: e.object_id,
                        offset: 0,
                        length: e.length,
                        flags: e.flags,
                    };
                    let data = encode(&block_ref)?;

                    let old_block = if let Some(entry) = blocks_table
                        .get(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                    {
                        let old: BlockRef = decode(entry.value())?;
                        Some(old)
                    } else {
                        None
                    };

                    blocks_table
                        .insert(key.as_slice(), data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;

                    if let Some(ref old) = old_block {
                        used_space_delta -= old.length as i64;
                    }

                    used_space_delta += e.length as i64;
                    results.push(BatchFlushResult { old_block });
                    touched_inodes.insert(e.ino);
                }
            }

            if !touched_inodes.is_empty() {
                let committed = txn
                    .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut inodes = txn
                    .open_table(INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                for &ino in &touched_inodes {
                    if normalized_attr.as_ref().is_some_and(|attr| attr.ino == ino) {
                        continue;
                    }
                    let attr_bytes = inodes
                        .get(ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|g| g.value().to_vec());
                    if let Some(attr_bytes) = attr_bytes {
                        let mut current_attr: InodeAttr = decode(&attr_bytes)?;
                        let has_committed_manifest =
                            !Self::committed_manifest_in_txn(&committed, ino)?.is_empty();
                        Self::transition_upload_state_for_local_mutation(
                            &mut current_attr,
                            has_committed_manifest,
                        );
                        let encoded = encode(&current_attr)?;
                        inodes
                            .insert(ino, encoded.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                }
            }

            if let Some(attr) = normalized_attr.as_ref() {
                Self::set_inode_overlay_in_txn(&txn, attr)?;
            }

            if !entries.is_empty() {
                {
                    let mut counters_table = txn
                        .open_table(COUNTERS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let current_used = match counters_table
                        .get(counters::USED_SPACE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                    {
                        Some(v) => v.value(),
                        None => 0,
                    };
                    let new_used = if used_space_delta >= 0 {
                        current_used.saturating_add(used_space_delta as u64)
                    } else {
                        current_used.saturating_sub((-used_space_delta) as u64)
                    };
                    counters_table
                        .insert(counters::USED_SPACE, new_used)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }

                let batch_blocks: Vec<(Ino, u32, BlockRef)> = entries
                    .iter()
                    .map(|e| {
                        (
                            e.ino,
                            e.block_index,
                            BlockRef {
                                object_id: e.object_id,
                                offset: 0,
                                length: e.length,
                                flags: e.flags,
                            },
                        )
                    })
                    .collect();

                for &ino in &touched_inodes {
                    let current_blocks =
                        Self::get_file_blocks_from_txn_internal(&txn, FILE_BLOCKS_TABLE, ino)?;
                    Self::replace_regular_file_chunk_rows_from_blocks_in_txn(
                        &txn,
                        REGULAR_FILE_OVERLAY_TABLE,
                        ino,
                        current_blocks.as_slice(),
                        block_size,
                        false,
                        true,
                    )?;
                }
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetFileBlocksBatch {
                        blocks: batch_blocks,
                    },
                )?;
            }

            if let Some(attr) = normalized_attr.as_ref() {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                )?;
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(results)
    }
}

impl MetaGc for RedbMetaEngine {
    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(object_id, timestamp)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::AddPendingDeletion {
                    object_id,
                    timestamp,
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .remove(object_id)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemovePendingDeletion { object_id },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(PENDING_DELETIONS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let deletions: Vec<(u64, i64)> = table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.value(), v.value()))
            .collect();

        Ok(deletions)
    }

    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let committed = txn
            .open_table(COMMITTED_FILE_BLOCKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let snapshots = txn
            .open_table(SNAPSHOTS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut refs = HashSet::new();
        // Stream each table directly into the shared accumulator so we never
        // build a second temporary set or materialize the chunk graph.
        for item in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_k, v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let block: BlockRef = decode(v.value())?;
            refs.insert(block.object_id);
        }
        for item in committed
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_k, v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let block: BlockRef = decode(v.value())?;
            refs.insert(block.object_id);
        }
        Self::regular_file_graph_object_ids_from_table(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            &mut refs,
        )?;
        Self::regular_file_graph_object_ids_from_table(&txn, REGULAR_FILE_BASE_TABLE, &mut refs)?;
        for item in snapshots
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_k, v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let snapshot: MetadataSnapshot = decode(v.value())?;
            refs.extend(snapshot.pinned_object_ids);
        }
        Ok(refs)
    }
}
impl MetaRecovery for RedbMetaEngine {
    fn add_deferred_delete(&self, record: &DeferredDeleteRecord) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(DEFERRED_DELETES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let encoded = encode(record)?;
            table
                .insert(record.ino, encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_deferred_delete(&self, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(DEFERRED_DELETES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = table
                .remove(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Self::remove_undo_logs_for_in_txn(&txn, UndoLogKind::DeferredDelete, ino)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        self.cleanup_stale_undo_logs()?;
        Ok(())
    }

    fn list_deferred_deletes(&self) -> MetaResult<Vec<DeferredDeleteRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(DEFERRED_DELETES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_ino, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            out.push(decode(value.value())?);
        }
        Ok(out)
    }

    fn recover_pending_creates(&self) -> MetaResult<Vec<Ino>> {
        self.recover_pending_create_intents()
    }

    fn recover_undo_logs(&self) -> MetaResult<Vec<Ino>> {
        let mut touched_parents: HashSet<Ino> =
            self.recover_pending_create_intents()?.into_iter().collect();

        let deferred_by_ino: std::collections::HashMap<Ino, DeferredDeleteRecord> = self
            .list_deferred_deletes()?
            .into_iter()
            .map(|record| (record.ino, record))
            .collect();
        let undo_logs = self.list_undo_logs()?;
        if !undo_logs.is_empty() {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            for record in undo_logs {
                if record.kind != UndoLogKind::DeferredDelete {
                    continue;
                }
                if record.phase == UndoLogPhase::Completed {
                    continue;
                }
                let Some(deferred) = deferred_by_ino.get(&record.ino) else {
                    continue;
                };
                self.set_undo_log_phase_for(
                    record.kind.clone(),
                    record.ino,
                    UndoLogPhase::Replaying,
                )?;

                let blocks = match self.get_file_blocks(record.ino) {
                    Ok(blocks) => blocks,
                    Err(MetaError::InodeNotFound(_)) => Vec::new(),
                    Err(err) => return Err(err),
                };
                let total_bytes: u64 = blocks.iter().map(|(_, b)| b.length as u64).sum();
                let removed_ids = match self.remove_file_blocks(record.ino) {
                    Ok(ids) => ids,
                    Err(MetaError::InodeNotFound(_)) => Vec::new(),
                    Err(err) => return Err(err),
                };
                for object_id in removed_ids {
                    self.add_pending_deletion(object_id, timestamp)?;
                }
                if total_bytes > 0 {
                    let _ = self.decr_counter(counters::USED_SPACE, total_bytes)?;
                }
                match self.remove_attr(record.ino) {
                    Ok(()) => {}
                    Err(MetaError::InodeNotFound(_)) => {}
                    Err(err) => return Err(err),
                }
                self.remove_deferred_delete(record.ino)?;
                touched_parents.insert(deferred.parent);
            }
        }

        self.cleanup_stale_undo_logs()?;
        Ok(touched_parents.into_iter().collect())
    }

    fn add_pending_create(&self, record: &PendingCreateRecord) -> MetaResult<()> {
        self.add_pending_create_intent(record)
    }

    fn remove_pending_create(&self, ino: Ino) -> MetaResult<()> {
        self.remove_pending_create_intent(ino)?;
        self.cleanup_stale_undo_logs()?;
        Ok(())
    }

    fn list_pending_creates(&self) -> MetaResult<Vec<PendingCreateRecord>> {
        self.list_pending_create_intents()
    }

    fn append_undo_log(&self, record: &UndoLogRecord) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::append_undo_log_in_txn(&txn, record)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_undo_logs_for(&self, kind: UndoLogKind, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_undo_logs_for_in_txn(&txn, kind, ino)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list_undo_logs(&self) -> MetaResult<Vec<UndoLogRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(UNDO_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            out.push(decode(v.value())?);
        }
        Ok(out)
    }
}
impl MetaSymlink for RedbMetaEngine {
    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::set_symlink_overlay_in_txn(&txn, ino, target)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetSymlinkTarget {
                    ino,
                    target: target.to_vec(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        self.merged_symlink_target(&txn, ino)?
            .ok_or(MetaError::InodeNotFound(ino))
    }
}
impl MetaXattr for RedbMetaEngine {
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        self.merged_xattr_value(&txn, ino, name)
    }

    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()> {
        let current = self.get_xattr(ino, name)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            if flags & XATTR_CREATE_FLAG != 0 && current.is_some() {
                return Err(MetaError::EntryExists {
                    parent: ino,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }
            if flags & XATTR_REPLACE_FLAG != 0 && current.is_none() {
                return Err(MetaError::EntryNotFound {
                    parent: ino,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }

            Self::set_xattr_overlay_in_txn(&txn, ino, name, value)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetXattr {
                    ino,
                    name: name.to_vec(),
                    value: value.to_vec(),
                    flags,
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()> {
        if self.get_xattr(ino, name)?.is_none() {
            return Err(MetaError::EntryNotFound {
                parent: ino,
                name: String::from_utf8_lossy(name).to_string(),
            });
        }
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_xattr_overlay_in_txn(&txn, ino, name)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveXattr {
                    ino,
                    name: name.to_vec(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut names = std::collections::BTreeSet::<Vec<u8>>::new();

        for key in Self::keys_for_inode_in_table(&txn, BASE_XATTRS_TABLE, ino)? {
            if !Self::has_slice_marker(&txn, XATTR_TOMBSTONES_TABLE, key.as_slice())? {
                names.insert(xattr_name_from_key(&key).to_vec());
            }
        }
        for key in Self::keys_for_inode_in_table(&txn, XATTRS_TABLE, ino)? {
            names.insert(xattr_name_from_key(&key).to_vec());
        }

        Ok(names.into_iter().collect())
    }
}
impl MetaAcl for RedbMetaEngine {
    fn set_acl_rule(&self, ino: Ino, principal: &str, access_level: AccessLevel) -> MetaResult<()> {
        let _ = self.get_attr(ino)?;
        let key = encode_acl_rule_key(ino, principal);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut rules = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            rules
                .insert(key.as_slice(), access_level as u8)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut tombstones = txn
                .open_table(ACL_RULE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = tombstones.remove(key.as_slice());
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAclRule {
                    ino,
                    principal: principal.to_string(),
                    access_level,
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<()> {
        let key = encode_acl_rule_key(ino, principal);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut rules = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = rules.remove(key.as_slice());
            let mut tombstones = txn
                .open_table(ACL_RULE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            tombstones
                .insert(key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveAclRule {
                    ino,
                    principal: principal.to_string(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<Option<AccessLevel>> {
        let key = encode_acl_rule_key(ino, principal);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if Self::has_slice_marker(&txn, ACL_RULE_TOMBSTONES_TABLE, key.as_slice())? {
            return Ok(None);
        }
        let rules = txn
            .open_table(ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if let Some(value) = rules
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            return AccessLevel::from_u8(value.value())
                .ok_or_else(|| MetaError::Serialization("invalid ACL access_level".to_string()))
                .map(Some);
        }
        let base = txn
            .open_table(BASE_ACL_RULES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match base
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(value) => AccessLevel::from_u8(value.value())
                .ok_or_else(|| MetaError::Serialization("invalid ACL access_level".to_string()))
                .map(Some),
            None => Ok(None),
        }
    }
}
impl MetaSnapshots for RedbMetaEngine {
    fn create_snapshot(&self, root_ino: Ino) -> MetaResult<MetadataSnapshot> {
        let root_attr = self.get_attr(root_ino)?;
        if root_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(root_ino));
        }

        let pinned_object_ids = self.collect_snapshot_pinned_object_ids(root_ino)?;
        let snapshot_payload = serde_json::to_vec(&self.export_snapshot()?)
            .map_err(|e| MetaError::Serialization(e.to_string()))?;

        let snapshot = MetadataSnapshot {
            id: uuid::Uuid::new_v4().to_string(),
            root_ino,
            created_at: InodeAttr::now_nanos(),
            state: SnapshotState::Pending,
            pinned_object_ids,
            snapshot_payload,
        };

        let bytes = encode(&snapshot)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(SNAPSHOTS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(snapshot.id.as_str(), bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::CreateSnapshot {
                    snapshot: snapshot.clone(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(snapshot)
    }

    fn list_snapshots(&self) -> MetaResult<Vec<MetadataSnapshot>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(SNAPSHOTS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut snapshots = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_id, raw) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let snapshot: MetadataSnapshot = decode(raw.value())?;
            snapshots.push(snapshot);
        }
        snapshots.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(snapshots)
    }

    fn get_snapshot(&self, snapshot_id: &str) -> MetaResult<Option<MetadataSnapshot>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(SNAPSHOTS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let Some(raw) = table
            .get(snapshot_id)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };
        let snapshot: MetadataSnapshot = decode(raw.value())?;
        Ok(Some(snapshot))
    }
}
impl MetaReplication for RedbMetaEngine {
    fn check_consistency(&self) -> MetaResult<()> {
        Ok(())
    }

    fn flush_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn sync_metadata(&self) -> MetaResult<()> {
        Ok(())
    }

    fn invalidation_sender_id(&self) -> Option<String> {
        None
    }

    fn request_eager_flush(&self) {}

    fn metadata_version_token(&self) -> Option<String> {
        None
    }

    fn namespace_version_token(&self) -> Option<String> {
        self.metadata_version_token()
    }

    fn replication_backlog(&self) -> MetadataReplicationBacklog {
        MetadataReplicationBacklog::default()
    }

    fn remote_metadata_size(&self) -> MetaResult<Option<u64>> {
        Ok(None)
    }
}
impl crate::engine::MetaEngine for RedbMetaEngine {
    fn flush_blocks_batch_with_attr(
        &self,
        entries: &[BatchFlushEntry],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Vec<BatchFlushResult>> {
        self.flush_blocks_batch_with_attr_txn(entries, attr)
    }

    fn flush_regular_file_overlay_slices_with_attr(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<()> {
        self.flush_regular_file_overlay_slices_with_attr_txn(ino, chunk_slices, attr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::MetaEvent;

    fn temp_engine() -> RedbMetaEngine {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let engine = RedbMetaEngine::open(&path).unwrap();
        engine.init_root(1000, 1000).unwrap();
        // Leak dir so it isn't dropped
        std::mem::forget(dir);
        engine
    }

    fn set_regular_file_chunk_rows(
        engine: &RedbMetaEngine,
        ino: Ino,
        overlay_rows: &[(ChunkId, RegularFileChunkState)],
        base_rows: &[(ChunkId, RegularFileChunkState)],
    ) {
        let txn = engine.db.begin_write().unwrap();
        {
            RedbMetaEngine::clear_regular_file_chunk_rows_in_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
            )
            .unwrap();
            RedbMetaEngine::clear_regular_file_chunk_rows_in_txn(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                ino,
            )
            .unwrap();

            let mut overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
            for (chunk_id, chunk_state) in overlay_rows {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let bytes = encode(chunk_state).unwrap();
                overlay.insert(key.as_slice(), bytes.as_slice()).unwrap();
            }
            drop(overlay);

            let mut base = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
            for (chunk_id, chunk_state) in base_rows {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let bytes = encode(chunk_state).unwrap();
                base.insert(key.as_slice(), bytes.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    fn set_regular_file_pending_compaction_rows(
        engine: &RedbMetaEngine,
        ino: Ino,
        chunks: &[ChunkId],
    ) {
        let txn = engine.db.begin_write().unwrap();
        {
            let mut pending = txn
                .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .unwrap();
            for chunk_id in chunks {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                pending.insert(key.as_slice(), 1).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    #[test]
    fn test_regular_file_prefix_scan_is_exact_per_inode() {
        let engine = temp_engine();

        let mut chunk_1 = RegularFileChunkState::default();
        chunk_1.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("valid slice ino 1"),
            BlockRef::new_partial(3001, 4096),
        ));
        let mut chunk_10 = RegularFileChunkState::default();
        chunk_10.insert_slice(SliceRecord::new(
            SliceId(2),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("valid slice ino 10"),
            BlockRef::new_partial(3010, 4096),
        ));

        let txn = engine.db.begin_write().unwrap();
        {
            let mut overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
            let key_1 = encode_regular_file_chunk_key(1, ChunkId(0));
            let key_10 = encode_regular_file_chunk_key(10, ChunkId(0));
            let bytes_1 = encode(&chunk_1).unwrap();
            let bytes_10 = encode(&chunk_10).unwrap();
            overlay
                .insert(key_1.as_slice(), bytes_1.as_slice())
                .unwrap();
            overlay
                .insert(key_10.as_slice(), bytes_10.as_slice())
                .unwrap();
        }

        let only_1 = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            1,
        )
        .unwrap();
        let only_10 = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            10,
        )
        .unwrap();

        assert_eq!(only_1.len(), 1);
        assert_eq!(
            only_1
                .get(&ChunkId(0))
                .expect("chunk for ino 1")
                .slices
                .len(),
            1
        );
        assert_eq!(only_10.len(), 1);
        assert_eq!(
            only_10
                .get(&ChunkId(0))
                .expect("chunk for ino 10")
                .slices
                .len(),
            1
        );
    }

    #[test]
    fn test_clear_regular_file_graph_clears_pending_compaction_rows() {
        let engine = temp_engine();
        let ino = engine
            .create_file(ROOT_INO, b"clear-graph.bin", 1000, 1000, 0o644)
            .unwrap()
            .ino;

        set_regular_file_pending_compaction_rows(&engine, ino, &[ChunkId(3), ChunkId(7)]);

        let txn = engine.db.begin_write().unwrap();
        RedbMetaEngine::clear_regular_file_graph_in_txn(&txn, ino).unwrap();
        txn.commit().unwrap();

        let txn = engine.db.begin_read().unwrap();
        let pending = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .unwrap();
        assert!(
            pending
                .get(encode_regular_file_chunk_key(ino, ChunkId(3)).as_slice())
                .unwrap()
                .is_none()
        );
        assert!(
            pending
                .get(encode_regular_file_chunk_key(ino, ChunkId(7)).as_slice())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_legacy_block_replace_clears_pending_compaction_rows() {
        let engine = temp_engine();
        let ino = engine
            .create_file(ROOT_INO, b"legacy-replace.bin", 1000, 1000, 0o644)
            .unwrap()
            .ino;

        set_regular_file_pending_compaction_rows(&engine, ino, &[ChunkId(11)]);

        let txn = engine.db.begin_write().unwrap();
        RedbMetaEngine::replace_regular_file_chunk_rows_from_blocks_in_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
            &[(0, BlockRef::new_partial(1001, 4096))],
            DEFAULT_BLOCK_SIZE,
            false,
            true,
        )
        .unwrap();
        txn.commit().unwrap();

        let txn = engine.db.begin_read().unwrap();
        let pending = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .unwrap();
        assert!(
            pending
                .get(encode_regular_file_chunk_key(ino, ChunkId(11)).as_slice())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_format_info_roundtrip() {
        let engine = temp_engine();
        let info = FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: "test-vol".into(),
            uuid: "abc-123".into(),
            endpoint: "http://localhost:9000".into(),
            bucket: "testbucket".into(),
            access_key: "studiofs".into(),
            secret_key: "studiofs123".into(),
            region: "".into(),
            data_prefix: "test-volume/objects/".into(),
            metadata: MetadataConfig::default(),
            block_size: DEFAULT_BLOCK_SIZE,
        };
        engine.set_format_info(&info).unwrap();
        engine.validate_migration_completed().unwrap();
        let loaded = engine.get_format_info().unwrap();
        assert_eq!(loaded.name, "test-vol");
        assert_eq!(loaded.endpoint, "http://localhost:9000");
    }

    #[test]
    fn test_read_format_info_readonly_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("probe.db");

        let engine = RedbMetaEngine::open(&path).unwrap();
        engine.init_root(1000, 1000).unwrap();
        let info = FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: "probe-vol".into(),
            uuid: "probe-uuid".into(),
            endpoint: "http://localhost:9000".into(),
            bucket: "probe-bucket".into(),
            access_key: "studiofs".into(),
            secret_key: "studiofs123".into(),
            region: "".into(),
            data_prefix: "probe-volume/objects/".into(),
            metadata: MetadataConfig::default(),
            block_size: DEFAULT_BLOCK_SIZE,
        };
        engine.set_format_info(&info).unwrap();
        drop(engine);

        let loaded = RedbMetaEngine::read_format_info_readonly(&path).unwrap();
        assert_eq!(loaded.name, "probe-vol");
        assert_eq!(loaded.uuid, "probe-uuid");
    }

    #[test]
    fn test_read_format_info_readonly_returns_not_formatted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("probe-not-formatted.db");

        let engine = RedbMetaEngine::open(&path).unwrap();
        engine.init_root(1000, 1000).unwrap();
        drop(engine);

        let result = RedbMetaEngine::read_format_info_readonly(&path);
        assert!(matches!(result, Err(MetaError::NotFormatted)));
    }

    #[test]
    fn test_read_format_info_readonly_reports_serialization_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("probe-invalid-format.db");

        let engine = RedbMetaEngine::open(&path).unwrap();
        let txn = engine.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(SETTINGS_TABLE).unwrap();
            table.insert("format", b"{not-json".as_slice()).unwrap();
        }
        txn.commit().unwrap();
        drop(engine);

        let result = RedbMetaEngine::read_format_info_readonly(&path);
        assert!(matches!(result, Err(MetaError::Serialization(_))));
    }

    #[test]
    fn test_snapshot_pins_objects_and_is_listed() {
        let engine = temp_engine();
        let file = engine
            .create_file(ROOT_INO, b"snap.txt", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = DEFAULT_CHUNK_SIZE as u64;
        engine.set_attr(&attr).unwrap();

        let mut overlay_chunk = RegularFileChunkState::default();
        let mut overlay_slice = SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, DEFAULT_CHUNK_SIZE).expect("valid overlay slice"),
            BlockRef::new_partial(9_001, DEFAULT_CHUNK_SIZE),
        );
        overlay_slice.block_state = BlockRefState::Uploaded;
        overlay_chunk.insert_slice(overlay_slice);

        let mut base_chunk = RegularFileChunkState::default();
        let mut base_slice = SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(0, DEFAULT_CHUNK_SIZE).expect("valid base slice"),
            BlockRef::new_partial(9_002, DEFAULT_CHUNK_SIZE),
        );
        base_slice.block_state = BlockRefState::Published;
        base_chunk.insert_slice(base_slice);

        set_regular_file_chunk_rows(
            &engine,
            file.ino,
            &[(ChunkId(0), overlay_chunk)],
            &[(ChunkId(0), base_chunk)],
        );

        let refs_before = engine.get_referenced_object_ids().unwrap();
        assert!(refs_before.contains(&9_001));
        assert!(refs_before.contains(&9_002));

        let snapshot = engine.create_snapshot(ROOT_INO).unwrap();
        assert_eq!(snapshot.root_ino, ROOT_INO);
        assert_eq!(snapshot.state, SnapshotState::Pending);
        assert_eq!(snapshot.pinned_object_ids, vec![9_001]);
        assert!(!snapshot.snapshot_payload.is_empty());

        let listed = engine.list_snapshots().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, snapshot.id);
        assert_eq!(
            engine.get_snapshot(&snapshot.id).unwrap().unwrap().id,
            snapshot.id
        );

        engine.unlink(ROOT_INO, b"snap.txt").unwrap();
        let refs = engine.get_referenced_object_ids().unwrap();
        assert!(
            refs.contains(&9_001),
            "snapshot pins should keep object ids referenced"
        );
        assert!(
            !refs.contains(&9_002),
            "shadowed base object should not stay pinned after unlink"
        );
    }

    #[test]
    fn test_root_inode_exists() {
        let engine = temp_engine();
        let attr = engine.get_attr(ROOT_INO).unwrap();
        assert_eq!(attr.ino, ROOT_INO);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.mode, 0o755);
    }

    #[test]
    fn test_create_file_and_lookup() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"test.txt", 1000, 1000, 0o644)
            .unwrap();
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.mode, 0o644);

        let entry = engine.lookup(ROOT_INO, b"test.txt").unwrap();
        assert_eq!(entry.ino, attr.ino);
        assert_eq!(entry.kind, FileType::RegularFile);
    }

    #[test]
    fn test_create_dir_and_readdir() {
        let engine = temp_engine();
        let dir_attr = engine
            .create_dir(ROOT_INO, b"subdir", 1000, 1000, 0o755)
            .unwrap();
        assert_eq!(dir_attr.kind, FileType::Directory);

        // Parent nlink should be 3 (root=2, +1 for subdir)
        let root = engine.get_attr(ROOT_INO).unwrap();
        assert_eq!(root.nlink, 3);

        let entries = engine.readdir(ROOT_INO).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, b"subdir");
    }

    #[test]
    fn test_get_attrs_batch_returns_ordered_attrs() {
        let engine = temp_engine();
        let a = engine
            .create_file(ROOT_INO, b"a.txt", 1000, 1000, 0o644)
            .unwrap();
        let b = engine
            .create_file(ROOT_INO, b"b.txt", 1000, 1000, 0o644)
            .unwrap();

        let attrs = engine.get_attrs_batch(&[b.ino, a.ino]).unwrap();
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].ino, b.ino);
        assert_eq!(attrs[1].ino, a.ino);
    }

    #[test]
    fn test_get_attrs_batch_skips_missing_inodes() {
        let engine = temp_engine();
        let kept = engine
            .create_file(ROOT_INO, b"kept.txt", 1000, 1000, 0o644)
            .unwrap();
        let removed = engine
            .create_file(ROOT_INO, b"removed.txt", 1000, 1000, 0o644)
            .unwrap();
        engine.unlink(ROOT_INO, b"removed.txt").unwrap();

        let attrs = engine.get_attrs_batch(&[removed.ino, kept.ino]).unwrap();
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0].ino, kept.ino);
    }

    #[test]
    fn test_unlink_file() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"to_delete.txt", 1000, 1000, 0o644)
            .unwrap();

        engine.unlink(ROOT_INO, b"to_delete.txt").unwrap();

        assert!(engine.lookup(ROOT_INO, b"to_delete.txt").is_err());
        assert!(engine.get_attr(attr.ino).is_err());
    }

    #[test]
    fn test_unlink_open_handle_keeps_inode_and_decrements_nlink() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"open-unlink.txt", 1000, 1000, 0o644)
            .unwrap();

        let result = engine
            .unlink_open_handle(ROOT_INO, b"open-unlink.txt")
            .unwrap();

        assert_eq!(result.ino, attr.ino);
        assert_eq!(result.nlink, 0);
        assert!(engine.lookup(ROOT_INO, b"open-unlink.txt").is_err());
        let persisted = engine.get_attr(attr.ino).unwrap();
        assert_eq!(persisted.nlink, 0);
        let deferred = engine.list_deferred_deletes().unwrap();
        assert!(deferred.iter().any(|record| record.ino == attr.ino));
        engine.remove_deferred_delete(attr.ino).unwrap();
        let deferred_after = engine.list_deferred_deletes().unwrap();
        assert!(!deferred_after.iter().any(|record| record.ino == attr.ino));
    }

    #[test]
    fn test_recover_pending_create_intent_removes_dangling_dentry() {
        let engine = temp_engine();
        let ino = engine.next_inode().unwrap();
        let intent = PendingCreateRecord {
            ino,
            parent: ROOT_INO,
            name: b"ghost-create.txt".to_vec(),
            kind: FileType::RegularFile,
            created_at: 0,
        };
        engine.add_pending_create(&intent).unwrap();
        engine
            .add_entry(
                ROOT_INO,
                b"ghost-create.txt",
                &DirEntryValue {
                    ino,
                    kind: FileType::RegularFile,
                },
            )
            .unwrap();

        assert!(engine.lookup(ROOT_INO, b"ghost-create.txt").is_ok());
        assert!(engine.get_attr(ino).is_err());

        engine.recover_pending_create_intents().unwrap();

        assert!(engine.lookup(ROOT_INO, b"ghost-create.txt").is_err());
        assert!(engine.list_pending_creates().unwrap().is_empty());
    }

    #[test]
    fn test_recover_undo_logs_replays_deferred_delete() {
        let engine = temp_engine();
        let parent = ROOT_INO;
        let name = b"orphaned-open-unlink.txt";

        let attr = engine.create_file(parent, name, 1000, 1000, 0o644).unwrap();
        let unlinked = engine.unlink_open_handle(parent, name).unwrap();
        assert_eq!(unlinked.ino, attr.ino);

        let deferred_before = engine.list_deferred_deletes().unwrap();
        assert!(deferred_before.iter().any(|r| r.ino == attr.ino));
        let undo_before = engine.list_undo_logs().unwrap();
        assert!(
            undo_before
                .iter()
                .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == attr.ino)
        );
        assert!(engine.get_attr(attr.ino).is_ok());

        let touched = engine.recover_undo_logs().unwrap();
        assert!(touched.contains(&parent));

        assert!(matches!(
            engine.get_attr(attr.ino),
            Err(MetaError::InodeNotFound(_))
        ));
        let deferred_after = engine.list_deferred_deletes().unwrap();
        assert!(!deferred_after.iter().any(|r| r.ino == attr.ino));
        let undo_after = engine.list_undo_logs().unwrap();
        assert!(
            !undo_after
                .iter()
                .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == attr.ino)
        );
    }

    #[test]
    fn test_recover_undo_logs_cleans_stale_deferred_undo_record() {
        let engine = temp_engine();

        let stale = UndoLogRecord {
            op_id: 9_999_001,
            kind: UndoLogKind::DeferredDelete,
            phase: UndoLogPhase::Started,
            ino: 424_242,
            parent: ROOT_INO,
            name: b"stale-deferred-undo.txt".to_vec(),
            created_at: 0,
        };
        engine.append_undo_log(&stale).unwrap();
        assert!(
            engine
                .list_undo_logs()
                .unwrap()
                .iter()
                .any(|r| r.op_id == stale.op_id)
        );

        let touched = engine.recover_undo_logs().unwrap();
        assert!(touched.is_empty());
        assert!(
            !engine
                .list_undo_logs()
                .unwrap()
                .iter()
                .any(|r| r.op_id == stale.op_id)
        );
    }

    #[test]
    fn test_recover_undo_logs_replays_pending_and_deferred_mixed() {
        let engine = temp_engine();

        let pending_ino = engine.next_inode().unwrap();
        let pending_name = b"mixed-pending-create.txt";
        let intent = PendingCreateRecord {
            ino: pending_ino,
            parent: ROOT_INO,
            name: pending_name.to_vec(),
            kind: FileType::RegularFile,
            created_at: 0,
        };
        engine.add_pending_create(&intent).unwrap();
        engine
            .add_entry(
                ROOT_INO,
                pending_name,
                &DirEntryValue {
                    ino: pending_ino,
                    kind: FileType::RegularFile,
                },
            )
            .unwrap();
        assert!(engine.lookup(ROOT_INO, pending_name).is_ok());
        assert!(engine.get_attr(pending_ino).is_err());

        let deferred_name = b"mixed-deferred-delete.txt";
        let deferred_attr = engine
            .create_file(ROOT_INO, deferred_name, 1000, 1000, 0o644)
            .unwrap();
        engine.unlink_open_handle(ROOT_INO, deferred_name).unwrap();
        assert!(
            engine
                .list_deferred_deletes()
                .unwrap()
                .iter()
                .any(|r| r.ino == deferred_attr.ino)
        );

        let touched = engine.recover_undo_logs().unwrap();
        assert!(touched.contains(&ROOT_INO));

        assert!(engine.lookup(ROOT_INO, pending_name).is_err());
        assert!(engine.list_pending_creates().unwrap().is_empty());
        assert!(matches!(
            engine.get_attr(deferred_attr.ino),
            Err(MetaError::InodeNotFound(_))
        ));
        assert!(
            !engine
                .list_deferred_deletes()
                .unwrap()
                .iter()
                .any(|r| r.ino == deferred_attr.ino)
        );
        assert!(engine.list_undo_logs().unwrap().is_empty());
    }

    #[test]
    fn test_recover_undo_logs_replays_deferred_from_replaying_phase() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"replaying-phase.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;
        engine
            .unlink_open_handle(ROOT_INO, b"replaying-phase.txt")
            .unwrap();
        assert!(
            engine
                .list_deferred_deletes()
                .unwrap()
                .iter()
                .any(|r| r.ino == ino)
        );

        engine
            .remove_undo_logs_for(UndoLogKind::DeferredDelete, ino)
            .unwrap();
        let replaying = UndoLogRecord {
            op_id: 9_999_111,
            kind: UndoLogKind::DeferredDelete,
            phase: UndoLogPhase::Replaying,
            ino,
            parent: ROOT_INO,
            name: b"replaying-phase.txt".to_vec(),
            created_at: 0,
        };
        engine.append_undo_log(&replaying).unwrap();

        let touched = engine.recover_undo_logs().unwrap();
        assert!(touched.contains(&ROOT_INO));
        assert!(matches!(
            engine.get_attr(ino),
            Err(MetaError::InodeNotFound(_))
        ));
        assert!(
            !engine
                .list_deferred_deletes()
                .unwrap()
                .iter()
                .any(|r| r.ino == ino)
        );
        assert!(
            !engine
                .list_undo_logs()
                .unwrap()
                .iter()
                .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == ino)
        );
    }

    #[test]
    fn test_recover_undo_logs_replays_pending_from_replaying_phase() {
        let engine = temp_engine();

        let ino = engine.next_inode().unwrap();
        let name = b"pending-replaying-phase.txt";
        let intent = PendingCreateRecord {
            ino,
            parent: ROOT_INO,
            name: name.to_vec(),
            kind: FileType::RegularFile,
            created_at: 0,
        };
        engine.add_pending_create(&intent).unwrap();
        engine
            .add_entry(
                ROOT_INO,
                name,
                &DirEntryValue {
                    ino,
                    kind: FileType::RegularFile,
                },
            )
            .unwrap();
        assert!(engine.lookup(ROOT_INO, name).is_ok());
        assert!(engine.get_attr(ino).is_err());

        engine
            .remove_undo_logs_for(UndoLogKind::PendingCreate, ino)
            .unwrap();
        let replaying = UndoLogRecord {
            op_id: 9_999_121,
            kind: UndoLogKind::PendingCreate,
            phase: UndoLogPhase::Replaying,
            ino,
            parent: ROOT_INO,
            name: name.to_vec(),
            created_at: 0,
        };
        engine.append_undo_log(&replaying).unwrap();

        let touched = engine.recover_undo_logs().unwrap();
        assert!(touched.contains(&ROOT_INO));
        assert!(engine.lookup(ROOT_INO, name).is_err());
        assert!(engine.list_pending_creates().unwrap().is_empty());
        assert!(
            !engine
                .list_undo_logs()
                .unwrap()
                .iter()
                .any(|r| r.kind == UndoLogKind::PendingCreate && r.ino == ino)
        );
    }

    #[test]
    fn test_remove_undo_logs_for_marks_completed_until_cleanup() {
        let engine = temp_engine();

        let ino = engine.next_inode().unwrap();
        let record = UndoLogRecord {
            op_id: 9_999_131,
            kind: UndoLogKind::PendingCreate,
            phase: UndoLogPhase::Started,
            ino,
            parent: ROOT_INO,
            name: b"completed-lifecycle.txt".to_vec(),
            created_at: 0,
        };
        engine.append_undo_log(&record).unwrap();

        engine
            .remove_undo_logs_for(UndoLogKind::PendingCreate, ino)
            .unwrap();
        let logs_after_mark = engine.list_undo_logs().unwrap();
        let completed = logs_after_mark
            .iter()
            .find(|r| r.kind == UndoLogKind::PendingCreate && r.ino == ino)
            .expect("record should remain until cleanup");
        assert_eq!(completed.phase, UndoLogPhase::Completed);

        engine.cleanup_stale_undo_logs().unwrap();
        assert!(
            !engine
                .list_undo_logs()
                .unwrap()
                .iter()
                .any(|r| r.kind == UndoLogKind::PendingCreate && r.ino == ino)
        );
    }

    #[test]
    fn test_remove_deferred_delete_performs_post_cleanup_of_completed_undo() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"deferred-completed.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;
        engine
            .unlink_open_handle(ROOT_INO, b"deferred-completed.txt")
            .unwrap();

        engine.remove_deferred_delete(ino).unwrap();
        assert!(
            !engine
                .list_undo_logs()
                .unwrap()
                .iter()
                .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == ino)
        );
    }

    #[test]
    fn test_remove_pending_create_performs_post_cleanup_of_completed_undo() {
        let engine = temp_engine();

        let ino = engine.next_inode().unwrap();
        let intent = PendingCreateRecord {
            ino,
            parent: ROOT_INO,
            name: b"pending-completed.txt".to_vec(),
            kind: FileType::RegularFile,
            created_at: 0,
        };
        engine.add_pending_create(&intent).unwrap();

        engine.remove_pending_create(ino).unwrap();
        assert!(
            !engine
                .list_pending_creates()
                .unwrap()
                .iter()
                .any(|r| r.ino == ino)
        );
        assert!(
            !engine
                .list_undo_logs()
                .unwrap()
                .iter()
                .any(|r| r.kind == UndoLogKind::PendingCreate && r.ino == ino)
        );
    }

    #[test]
    fn test_cleanup_stale_undo_logs_removes_completed_even_if_live_rows_exist() {
        let engine = temp_engine();

        let pending_ino = engine.next_inode().unwrap();
        let pending_intent = PendingCreateRecord {
            ino: pending_ino,
            parent: ROOT_INO,
            name: b"live-pending-completed-undo.txt".to_vec(),
            kind: FileType::RegularFile,
            created_at: 0,
        };
        engine.add_pending_create(&pending_intent).unwrap();
        engine
            .remove_undo_logs_for(UndoLogKind::PendingCreate, pending_ino)
            .unwrap();

        let deferred_ino = engine.next_inode().unwrap();
        let deferred = DeferredDeleteRecord {
            ino: deferred_ino,
            parent: ROOT_INO,
            name: b"live-deferred-completed-undo.txt".to_vec(),
            created_at: 0,
        };
        engine.add_deferred_delete(&deferred).unwrap();
        engine
            .append_undo_log(&UndoLogRecord {
                op_id: 9_999_201,
                kind: UndoLogKind::DeferredDelete,
                phase: UndoLogPhase::Started,
                ino: deferred_ino,
                parent: ROOT_INO,
                name: deferred.name.clone(),
                created_at: 0,
            })
            .unwrap();
        engine
            .remove_undo_logs_for(UndoLogKind::DeferredDelete, deferred_ino)
            .unwrap();

        let before = engine.list_undo_logs().unwrap();
        assert!(before.iter().any(|r| {
            r.kind == UndoLogKind::PendingCreate
                && r.ino == pending_ino
                && r.phase == UndoLogPhase::Completed
        }));
        assert!(before.iter().any(|r| {
            r.kind == UndoLogKind::DeferredDelete
                && r.ino == deferred_ino
                && r.phase == UndoLogPhase::Completed
        }));
        assert!(
            engine
                .list_pending_creates()
                .unwrap()
                .iter()
                .any(|r| r.ino == pending_ino)
        );
        assert!(
            engine
                .list_deferred_deletes()
                .unwrap()
                .iter()
                .any(|r| r.ino == deferred_ino)
        );

        engine.cleanup_stale_undo_logs().unwrap();
        let after = engine.list_undo_logs().unwrap();
        assert!(
            !after
                .iter()
                .any(|r| r.kind == UndoLogKind::PendingCreate && r.ino == pending_ino)
        );
        assert!(
            !after
                .iter()
                .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == deferred_ino)
        );
    }

    #[test]
    fn test_repeated_pending_and_deferred_cycles_do_not_accumulate_undo_logs() {
        let engine = temp_engine();

        for i in 0..16u64 {
            let pending_ino = engine.next_inode().unwrap();
            let pending_intent = PendingCreateRecord {
                ino: pending_ino,
                parent: ROOT_INO,
                name: format!("loop-pending-{i}.txt").into_bytes(),
                kind: FileType::RegularFile,
                created_at: 0,
            };
            engine.add_pending_create(&pending_intent).unwrap();
            engine.remove_pending_create(pending_ino).unwrap();

            let deferred_name = format!("loop-deferred-{i}.txt");
            let attr = engine
                .create_file(ROOT_INO, deferred_name.as_bytes(), 1000, 1000, 0o644)
                .unwrap();
            engine
                .unlink_open_handle(ROOT_INO, deferred_name.as_bytes())
                .unwrap();
            engine.remove_deferred_delete(attr.ino).unwrap();
        }

        assert!(
            engine.list_undo_logs().unwrap().is_empty(),
            "undo log should remain empty after repeated completed-cleanup cycles"
        );
    }

    #[test]
    fn test_rmdir() {
        let engine = temp_engine();
        engine
            .create_dir(ROOT_INO, b"empty_dir", 1000, 1000, 0o755)
            .unwrap();
        engine.rmdir(ROOT_INO, b"empty_dir").unwrap();

        assert!(engine.lookup(ROOT_INO, b"empty_dir").is_err());
        let root = engine.get_attr(ROOT_INO).unwrap();
        assert_eq!(root.nlink, 2); // back to initial
    }

    #[test]
    fn test_rmdir_notempty() {
        let engine = temp_engine();
        let dir = engine
            .create_dir(ROOT_INO, b"notempty", 1000, 1000, 0o755)
            .unwrap();
        engine
            .create_file(dir.ino, b"file.txt", 1000, 1000, 0o644)
            .unwrap();

        let result = engine.rmdir(ROOT_INO, b"notempty");
        assert!(result.is_err());
    }

    #[test]
    fn test_rename() {
        let engine = temp_engine();
        let src = engine
            .create_file(ROOT_INO, b"old.txt", 1000, 1000, 0o644)
            .unwrap();
        let displaced = engine
            .rename_atomic(ROOT_INO, b"old.txt", ROOT_INO, b"new.txt")
            .unwrap();
        assert_eq!(displaced.moved_ino, src.ino);
        assert!(displaced.displaced_ino.is_none());

        assert!(engine.lookup(ROOT_INO, b"old.txt").is_err());
        assert!(engine.lookup(ROOT_INO, b"new.txt").is_ok());
    }

    #[test]
    fn test_rename_overwrite_file() {
        let engine = temp_engine();
        let src = engine
            .create_file(ROOT_INO, b"src.txt", 1000, 1000, 0o644)
            .unwrap();
        let dst = engine
            .create_file(ROOT_INO, b"dst.txt", 1000, 1000, 0o644)
            .unwrap();

        let displaced = engine
            .rename_atomic(ROOT_INO, b"src.txt", ROOT_INO, b"dst.txt")
            .unwrap();

        // Should return the displaced inode
        assert_eq!(displaced.moved_ino, src.ino);
        assert_eq!(displaced.displaced_ino, Some(dst.ino));
        // Source is gone, dest now points to src's inode
        assert!(engine.lookup(ROOT_INO, b"src.txt").is_err());
        let entry = engine.lookup(ROOT_INO, b"dst.txt").unwrap();
        assert_eq!(entry.ino, src.ino);
        // Displaced inode should be removed (nlink was 1 → 0)
        assert!(engine.get_attr(dst.ino).is_err());
    }

    #[test]
    fn test_rename_overwrite_empty_dir() {
        let engine = temp_engine();
        let file = engine
            .create_file(ROOT_INO, b"file.txt", 1000, 1000, 0o644)
            .unwrap();
        let dir = engine
            .create_dir(ROOT_INO, b"empty_dir", 1000, 1000, 0o755)
            .unwrap();

        let displaced = engine
            .rename_atomic(ROOT_INO, b"file.txt", ROOT_INO, b"empty_dir")
            .unwrap();

        assert_eq!(displaced.moved_ino, file.ino);
        assert_eq!(displaced.displaced_ino, Some(dir.ino));
        assert!(engine.get_attr(dir.ino).is_err());
        let entry = engine.lookup(ROOT_INO, b"empty_dir").unwrap();
        assert_eq!(entry.ino, file.ino);
    }

    #[test]
    fn test_rename_overwrite_nonempty_dir_fails() {
        let engine = temp_engine();
        engine
            .create_file(ROOT_INO, b"file.txt", 1000, 1000, 0o644)
            .unwrap();
        let dir = engine
            .create_dir(ROOT_INO, b"notempty", 1000, 1000, 0o755)
            .unwrap();
        engine
            .create_file(dir.ino, b"child.txt", 1000, 1000, 0o644)
            .unwrap();

        let result = engine.rename_atomic(ROOT_INO, b"file.txt", ROOT_INO, b"notempty");
        assert!(result.is_err());
        // Source should still exist (rollback in same txn)
        assert!(engine.lookup(ROOT_INO, b"file.txt").is_ok());
        assert!(engine.lookup(ROOT_INO, b"notempty").is_ok());
    }

    #[test]
    fn test_rename_move_dir_between_parents() {
        let engine = temp_engine();
        let dir_a = engine
            .create_dir(ROOT_INO, b"a", 1000, 1000, 0o755)
            .unwrap();
        let dir_b = engine
            .create_dir(ROOT_INO, b"b", 1000, 1000, 0o755)
            .unwrap();
        // root nlink = 2 + 2 subdirs = 4
        assert_eq!(engine.get_attr(ROOT_INO).unwrap().nlink, 4);

        // Create a subdir inside /a
        let sub = engine
            .create_dir(dir_a.ino, b"sub", 1000, 1000, 0o755)
            .unwrap();
        // /a nlink = 2 + 1 subdir = 3
        assert_eq!(engine.get_attr(dir_a.ino).unwrap().nlink, 3);
        assert_eq!(engine.get_attr(dir_b.ino).unwrap().nlink, 2);

        // Move /a/sub → /b/sub
        let displaced = engine
            .rename_atomic(dir_a.ino, b"sub", dir_b.ino, b"sub")
            .unwrap();
        assert_eq!(displaced.moved_ino, sub.ino);
        assert!(displaced.displaced_ino.is_none());

        // /a nlink = 2 (lost subdir)
        assert_eq!(engine.get_attr(dir_a.ino).unwrap().nlink, 2);
        // /b nlink = 3 (gained subdir)
        assert_eq!(engine.get_attr(dir_b.ino).unwrap().nlink, 3);
        // sub's parent should now be dir_b
        assert_eq!(engine.get_attr(sub.ino).unwrap().parent, dir_b.ino);
        // Dentry moved
        assert!(engine.lookup(dir_a.ino, b"sub").is_err());
        assert!(engine.lookup(dir_b.ino, b"sub").is_ok());
    }

    #[test]
    fn test_rename_nonexistent_source() {
        let engine = temp_engine();
        let result = engine.rename_atomic(ROOT_INO, b"nope.txt", ROOT_INO, b"dst.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_rename_concurrent_single_winner() {
        let engine = Arc::new(temp_engine());
        let src = engine
            .create_file(ROOT_INO, b"source.txt", 1000, 1000, 0o644)
            .unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(3));
        let a_engine = Arc::clone(&engine);
        let a_barrier = Arc::clone(&barrier);
        let t_a = std::thread::spawn(move || {
            a_barrier.wait();
            a_engine.rename_atomic(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_a.txt")
        });

        let b_engine = Arc::clone(&engine);
        let b_barrier = Arc::clone(&barrier);
        let t_b = std::thread::spawn(move || {
            b_barrier.wait();
            b_engine.rename_atomic(ROOT_INO, b"source.txt", ROOT_INO, b"moved_by_b.txt")
        });

        barrier.wait();

        let out_a = t_a.join().expect("rename thread a panicked");
        let out_b = t_b.join().expect("rename thread b panicked");
        let ok_count = [out_a.is_ok(), out_b.is_ok()]
            .into_iter()
            .filter(|ok| *ok)
            .count();
        assert_eq!(ok_count, 1, "exactly one concurrent rename should win");
        assert!(
            engine.lookup(ROOT_INO, b"source.txt").is_err(),
            "source entry must be removed after winning rename"
        );

        let a_entry = engine.lookup(ROOT_INO, b"moved_by_a.txt").ok();
        let b_entry = engine.lookup(ROOT_INO, b"moved_by_b.txt").ok();
        assert_ne!(
            a_entry.is_some(),
            b_entry.is_some(),
            "only one destination name should remain visible"
        );
        if let Some(entry) = a_entry {
            assert_eq!(entry.ino, src.ino);
        }
        if let Some(entry) = b_entry {
            assert_eq!(entry.ino, src.ino);
        }
    }

    #[test]
    fn test_next_inode_increments() {
        let engine = temp_engine();
        let ino1 = engine.next_inode().unwrap();
        let ino2 = engine.next_inode().unwrap();
        assert_eq!(ino2, ino1 + 1);
    }

    #[test]
    fn test_list_inode_attrs_ignores_sparse_next_inode_range() {
        let engine = temp_engine();
        let file = engine
            .create_file(ROOT_INO, b"sparse.txt", 1000, 1000, 0o644)
            .unwrap();
        engine.set_counter(counters::NEXT_INODE, 1_000_000).unwrap();

        let attrs = engine.list_inode_attrs().unwrap();
        let inos: std::collections::BTreeSet<_> = attrs.into_iter().map(|attr| attr.ino).collect();

        assert!(inos.contains(&ROOT_INO));
        assert!(inos.contains(&file.ino));
        assert_eq!(inos.len(), 2);
    }

    // === Architecture Tests ===

    #[test]
    fn test_file_block_operations() {
        let engine = temp_engine();

        // Create a file
        let attr = engine
            .create_file(ROOT_INO, b"test_blocks.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        // Initially no blocks
        assert!(engine.get_file_block(ino, 0).unwrap().is_none());
        assert!(engine.get_file_blocks(ino).unwrap().is_empty());

        // Add a block
        let block1 = BlockRef::new_full(1, 4 * 1024 * 1024);
        let old = engine.set_file_block(ino, 0, &block1).unwrap();
        assert!(old.is_none()); // No previous block

        let txn = engine.db.begin_write().unwrap();
        let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )
        .unwrap();
        txn.abort().unwrap();
        let overlay_chunk = overlay_chunks.get(&ChunkId(0)).unwrap();
        assert_eq!(overlay_chunk.slices.len(), 1);
        assert_eq!(
            overlay_chunk.slices.values().next().unwrap().block_state,
            BlockRefState::LocalOnly
        );

        // Retrieve the block
        let retrieved = engine.get_file_block(ino, 0).unwrap().unwrap();
        assert_eq!(retrieved.object_id, 1);
        assert_eq!(retrieved.length, 4 * 1024 * 1024);
        assert!(!retrieved.is_compressed());
        assert!(!retrieved.is_partial());

        // Add another block
        let block2 = BlockRef::new_partial(2, 1000);
        engine.set_file_block(ino, 1, &block2).unwrap();

        let txn = engine.db.begin_write().unwrap();
        let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )
        .unwrap();
        txn.abort().unwrap();

        // Get all blocks
        let all_blocks = engine.get_file_blocks(ino).unwrap();
        assert_eq!(all_blocks.len(), 2);
        assert_eq!(all_blocks[0].0, 0); // block index
        assert_eq!(all_blocks[0].1.object_id, 1);
        assert_eq!(all_blocks[1].0, 1); // block index
        assert_eq!(all_blocks[1].1.object_id, 2);
        assert!(all_blocks[1].1.is_partial());
        assert_eq!(overlay_chunks.get(&ChunkId(0)).unwrap().slices.len(), 2);
    }

    #[test]
    fn test_flush_blocks_batch_updates_regular_file_overlay() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"batch_blocks.txt", 1000, 1000, 0o644)
            .unwrap();

        let results = engine
            .flush_blocks_batch(&[
                BatchFlushEntry {
                    ino: attr.ino,
                    block_index: 0,
                    object_id: 11,
                    length: 4 * 1024 * 1024,
                    flags: BlockFlags::empty(),
                },
                BatchFlushEntry {
                    ino: attr.ino,
                    block_index: 1,
                    object_id: 12,
                    length: 1000,
                    flags: BlockFlags::PARTIAL_BLOCK,
                },
            ])
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(engine.get_file_blocks(attr.ino).unwrap().len(), 2);

        let txn = engine.db.begin_write().unwrap();
        let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            attr.ino,
        )
        .unwrap();
        txn.abort().unwrap();

        assert_eq!(overlay_chunks.len(), 1);
        assert_eq!(overlay_chunks.get(&ChunkId(0)).unwrap().slices.len(), 2);
    }

    #[test]
    fn test_file_block_overwrite_returns_old() {
        let engine = temp_engine();

        // Create a file
        let attr = engine
            .create_file(ROOT_INO, b"test_overwrite.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        // Add a block
        let block1 = BlockRef::new_full(1, 4 * 1024 * 1024);
        engine.set_file_block(ino, 0, &block1).unwrap();

        // Overwrite with new block
        let block2 = BlockRef::new_full(2, 4 * 1024 * 1024);
        let old = engine.set_file_block(ino, 0, &block2).unwrap();

        // Old block should be returned for GC
        assert!(old.is_some());
        let old_block = old.unwrap();
        assert_eq!(old_block.object_id, 1);

        // Current block should be the new one
        let current = engine.get_file_block(ino, 0).unwrap().unwrap();
        assert_eq!(current.object_id, 2);
    }

    #[test]
    fn test_get_file_block_falls_back_to_committed() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"committed_only.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        let block = BlockRef::new_partial(7, 4096);
        let mut remote_attr = engine.get_attr(ino).unwrap();
        remote_attr.size = 4096;
        remote_attr.committed_size = 4096;
        engine
            .apply_replica_events_to_base(&[crate::events::MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            }])
            .unwrap();
        engine
            .apply_replica_aux_state(&[crate::events::MetaEvent::CommitFileBlock {
                ino,
                block_index: 0,
                block: block.clone(),
            }])
            .unwrap();

        let current = engine.get_file_block(ino, 0).unwrap().unwrap();
        assert_eq!(current.object_id, 7);

        let all_blocks = engine.get_file_blocks(ino).unwrap();
        assert_eq!(all_blocks.len(), 1);
        assert_eq!(all_blocks[0].0, 0);
        assert_eq!(all_blocks[0].1.object_id, 7);
    }

    #[test]
    fn test_synthesize_block_ref_rejects_fragmented_block() {
        let spans = vec![
            ResolvedSliceSpan {
                chunk_id: ChunkId(0),
                range: SliceRange::new(0, 4096).unwrap(),
                source: ResolvedSliceSource::Overlay,
                slice_id: Some(SliceId(1)),
                slice_order: Some(SliceOrder(1)),
                block: Some(BlockRef::new_partial(7, 4096)),
            },
            ResolvedSliceSpan {
                chunk_id: ChunkId(0),
                range: SliceRange::new(4096, 8192).unwrap(),
                source: ResolvedSliceSource::Overlay,
                slice_id: Some(SliceId(2)),
                slice_order: Some(SliceOrder(2)),
                block: Some(BlockRef::new_partial(8, 4096)),
            },
        ];

        let err = RedbMetaEngine::synthesize_block_ref_from_resolved_spans(
            ROOT_INO,
            spans.as_slice(),
            0,
            8192,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            MetaError::FragmentedBlock { ino, block_index }
                if ino == ROOT_INO && block_index == 0
        ));
    }

    #[test]
    fn test_replica_apply_keeps_namespace_in_base_only() {
        let engine = temp_engine();

        let remote_ino = 555;
        let remote_attr = InodeAttr::new_file(remote_ino, ROOT_INO, 1000, 1000, 0o644);
        engine
            .apply_replica_events_to_base(&[
                crate::events::MetaEvent::SetAttr {
                    attr: remote_attr.clone(),
                },
                crate::events::MetaEvent::AddEntry {
                    parent: ROOT_INO,
                    name: b"remote.txt".to_vec(),
                    entry: DirEntryValue {
                        ino: remote_ino,
                        kind: FileType::RegularFile,
                    },
                },
            ])
            .unwrap();

        let before_overlay_attrs = engine.local_overlay_attr_inodes().unwrap();
        let before_overlay_dentries = engine.local_overlay_dentries().unwrap();
        engine
            .apply_replica_aux_state(&[
                crate::events::MetaEvent::SetAttr {
                    attr: remote_attr.clone(),
                },
                crate::events::MetaEvent::AddEntry {
                    parent: ROOT_INO,
                    name: b"remote.txt".to_vec(),
                    entry: DirEntryValue {
                        ino: remote_ino,
                        kind: FileType::RegularFile,
                    },
                },
            ])
            .unwrap();

        assert_eq!(engine.get_attr(remote_ino).unwrap().ino, remote_ino);
        assert!(engine.lookup(ROOT_INO, b"remote.txt").is_ok());
        assert_eq!(
            engine.local_overlay_attr_inodes().unwrap(),
            before_overlay_attrs
        );
        assert_eq!(
            engine.local_overlay_dentries().unwrap(),
            before_overlay_dentries
        );
    }

    #[test]
    fn test_replica_apply_keeps_symlink_and_xattr_in_base_only() {
        let engine = temp_engine();

        let remote_ino = 777;
        let remote_attr = InodeAttr::new_symlink(remote_ino, ROOT_INO, 0, 0, 7);
        engine
            .apply_replica_events_to_base(&[
                crate::events::MetaEvent::SetAttr {
                    attr: remote_attr.clone(),
                },
                crate::events::MetaEvent::SetSymlinkTarget {
                    ino: remote_ino,
                    target: b"/remote".to_vec(),
                },
                crate::events::MetaEvent::SetXattr {
                    ino: remote_ino,
                    name: b"user.color".to_vec(),
                    value: b"red".to_vec(),
                    flags: 0,
                },
            ])
            .unwrap();

        let before_symlink_overlay = engine.local_overlay_symlink_inodes().unwrap();
        let before_xattr_overlay = engine.local_overlay_xattr_keys().unwrap();

        engine
            .apply_replica_aux_state(&[
                crate::events::MetaEvent::SetSymlinkTarget {
                    ino: remote_ino,
                    target: b"/remote".to_vec(),
                },
                crate::events::MetaEvent::SetXattr {
                    ino: remote_ino,
                    name: b"user.color".to_vec(),
                    value: b"red".to_vec(),
                    flags: 0,
                },
            ])
            .unwrap();

        assert_eq!(engine.get_symlink_target(remote_ino).unwrap(), b"/remote");
        assert_eq!(
            engine.get_xattr(remote_ino, b"user.color").unwrap(),
            Some(b"red".to_vec())
        );
        assert_eq!(
            engine.local_overlay_symlink_inodes().unwrap(),
            before_symlink_overlay
        );
        assert_eq!(
            engine.local_overlay_xattr_keys().unwrap(),
            before_xattr_overlay
        );
    }

    #[test]
    fn test_local_staged_block_overrides_committed_block() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"overlay_precedence.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        let committed = BlockRef::new_partial(7, 4096);
        engine.set_file_block(ino, 0, &committed).unwrap();
        assert!(engine.commit_uploaded_block(ino, 0, 0, &committed).unwrap());

        let txn = engine.db.begin_write().unwrap();
        let base_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )
        .unwrap();
        txn.abort().unwrap();
        let base_chunk = base_chunks.get(&ChunkId(0)).unwrap();
        assert_eq!(base_chunk.slices.len(), 1);
        assert_eq!(
            base_chunk.slices.values().next().unwrap().block_state,
            BlockRefState::Published
        );

        let staged = BlockRef::new_partial(9, 4096);
        engine.set_file_block(ino, 0, &staged).unwrap();

        let current = engine.get_file_block(ino, 0).unwrap().unwrap();
        assert_eq!(current.object_id, 9);

        let all_blocks = engine.get_file_blocks(ino).unwrap();
        assert_eq!(all_blocks.len(), 1);
        assert_eq!(all_blocks[0].0, 0);
        assert_eq!(all_blocks[0].1.object_id, 9);
    }

    #[test]
    fn test_remove_file_blocks() {
        let engine = temp_engine();

        // Create a file
        let attr = engine
            .create_file(ROOT_INO, b"test_remove.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        // Add multiple blocks
        for i in 0..5 {
            let block = BlockRef::new_full(i as u64 + 1, 4 * 1024 * 1024);
            engine.set_file_block(ino, i, &block).unwrap();
        }

        // Verify blocks exist
        assert_eq!(engine.get_file_blocks(ino).unwrap().len(), 5);

        // Remove all blocks
        let removed_ids = engine.remove_file_blocks(ino).unwrap();
        assert_eq!(removed_ids.len(), 5);
        assert_eq!(removed_ids, vec![1, 2, 3, 4, 5]);

        // Verify no blocks remain
        assert!(engine.get_file_blocks(ino).unwrap().is_empty());
        assert!(engine.get_file_block(ino, 0).unwrap().is_none());

        let txn = engine.db.begin_write().unwrap();
        let overlay_empty = RedbMetaEngine::has_regular_file_chunk_rows_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )
        .unwrap();
        let base_empty = RedbMetaEngine::has_regular_file_chunk_rows_in_write_txn(
            &txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )
        .unwrap();
        txn.abort().unwrap();
        assert!(!overlay_empty);
        assert!(!base_empty);
    }

    #[test]
    fn test_remove_file_block_updates_regular_file_graph() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"remove-one.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        let committed = BlockRef::new_full(31, 4 * 1024 * 1024);
        engine.set_file_block(ino, 0, &committed).unwrap();
        assert!(engine.commit_uploaded_block(ino, 0, 0, &committed).unwrap());

        let staged = BlockRef::new_full(32, 4 * 1024 * 1024);
        engine.set_file_block(ino, 1, &staged).unwrap();

        let removed = engine.remove_file_block(ino, 0).unwrap();
        assert_eq!(removed, Some(31));

        let txn = engine.db.begin_write().unwrap();
        let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )
        .unwrap();
        let base_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )
        .unwrap();
        txn.abort().unwrap();

        assert_eq!(overlay_chunks.len(), 1);
        assert_eq!(overlay_chunks.get(&ChunkId(0)).unwrap().slices.len(), 1);
        assert!(base_chunks.is_empty());
        assert_eq!(engine.get_file_blocks(ino).unwrap().len(), 1);
        assert_eq!(
            engine.get_file_block(ino, 1).unwrap().unwrap().object_id,
            32
        );
    }

    #[test]
    fn test_detect_local_overlay_state() {
        let engine = temp_engine();

        let file = engine
            .create_file(ROOT_INO, b"draft.bin", 1000, 1000, 0o644)
            .unwrap();
        assert!(!engine.has_local_overlay_state().unwrap());

        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 4096;
        engine.set_attr(&attr).unwrap();
        engine
            .set_file_block(file.ino, 0, &BlockRef::new_partial(11, 4096))
            .unwrap();
        assert!(engine.has_local_overlay_state().unwrap());

        assert!(
            engine
                .commit_uploaded_block(file.ino, 0, 0, &BlockRef::new_partial(11, 4096))
                .unwrap()
        );
        assert!(engine.has_local_overlay_state().unwrap());
    }

    #[test]
    fn test_detect_local_overlay_state_for_attr_only_against_base() {
        let source = temp_engine();
        let base_file = source
            .create_file(ROOT_INO, b"base_attr.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        assert!(!engine.has_local_overlay_state().unwrap());

        let mut attr = engine.get_attr(base_file.ino).unwrap();
        attr.mode = 0o600;
        engine.set_attr(&attr).unwrap();

        assert!(engine.has_local_overlay_state().unwrap());
    }

    #[test]
    fn test_detect_local_overlay_state_for_dentry_only_against_base() {
        let source = temp_engine();
        source
            .create_file(ROOT_INO, b"base_name.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        assert!(!engine.has_local_overlay_state().unwrap());

        engine.remove_entry(ROOT_INO, b"base_name.txt").unwrap();

        assert!(engine.has_local_overlay_state().unwrap());
    }

    #[test]
    fn test_detect_overlay_attr_against_imported_base() {
        let source = temp_engine();
        let base_file = source
            .create_file(ROOT_INO, b"base.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        assert!(engine.local_overlay_attr_inodes().unwrap().is_empty());

        let mut attr = engine.get_attr(base_file.ino).unwrap();
        attr.mode = 0o600;
        engine.set_attr(&attr).unwrap();

        let overlay = engine.local_overlay_attr_inodes().unwrap();
        assert!(overlay.contains(&base_file.ino));
    }

    #[test]
    fn test_detect_overlay_dentries_against_imported_base() {
        let source = temp_engine();
        source
            .create_file(ROOT_INO, b"base.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        assert!(engine.local_overlay_dentries().unwrap().is_empty());

        engine
            .create_file(ROOT_INO, b"local-only.txt", 1000, 1000, 0o644)
            .unwrap();
        let local_key = encode_dentry_key(ROOT_INO, b"local-only.txt");
        let overlay = engine.local_overlay_dentries().unwrap();
        assert!(overlay.contains(&local_key));

        engine.unlink(ROOT_INO, b"base.txt").unwrap();
        let base_key = encode_dentry_key(ROOT_INO, b"base.txt");
        let overlay = engine.local_overlay_dentries().unwrap();
        assert!(overlay.contains(&base_key));
    }

    #[test]
    fn test_detect_overlay_symlink_against_imported_base() {
        let source = temp_engine();
        let link = source
            .create_symlink(ROOT_INO, b"base-link", b"/base/target", 0, 0)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        assert!(engine.local_overlay_symlink_inodes().unwrap().is_empty());

        engine
            .set_symlink_target(link.ino, b"/local/target")
            .unwrap();

        let overlay = engine.local_overlay_symlink_inodes().unwrap();
        assert!(overlay.contains(&link.ino));
    }

    #[test]
    fn test_detect_overlay_xattr_against_imported_base() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"base-xattr.txt", 0, 0, 0o644)
            .unwrap();
        source
            .set_xattr(file.ino, b"user.color", b"red", 0)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        assert!(engine.local_overlay_xattr_keys().unwrap().is_empty());

        engine
            .set_xattr(file.ino, b"user.color", b"blue", 0)
            .unwrap();

        let overlay = engine.local_overlay_xattr_keys().unwrap();
        assert!(overlay.contains(&encode_xattr_key(file.ino, b"user.color")));
    }

    #[test]
    fn test_getattr_falls_back_to_base_when_no_local_overlay() {
        let engine = temp_engine();

        let base_attr = InodeAttr::new_file(42, ROOT_INO, 1000, 1000, 0o644);
        engine
            .apply_replica_events_to_base(&[crate::events::MetaEvent::SetAttr {
                attr: base_attr.clone(),
            }])
            .unwrap();

        let loaded = engine.get_attr(42).unwrap();
        assert_eq!(loaded.ino, 42);
        assert_eq!(loaded.mode, base_attr.mode);
    }

    #[test]
    fn test_lookup_and_readdir_fall_back_to_base_when_no_local_overlay() {
        let engine = temp_engine();

        let base_attr = InodeAttr::new_file(77, ROOT_INO, 1000, 1000, 0o644);
        let base_entry = DirEntryValue {
            ino: 77,
            kind: FileType::RegularFile,
        };
        engine
            .apply_replica_events_to_base(&[
                crate::events::MetaEvent::SetAttr {
                    attr: base_attr.clone(),
                },
                crate::events::MetaEvent::AddEntry {
                    parent: ROOT_INO,
                    name: b"base-only.txt".to_vec(),
                    entry: base_entry.clone(),
                },
            ])
            .unwrap();

        let looked = engine.lookup(ROOT_INO, b"base-only.txt").unwrap();
        assert_eq!(looked.ino, 77);

        let entries = engine.readdir(ROOT_INO).unwrap();
        assert!(entries.iter().any(|entry| entry.name == b"base-only.txt"));
    }

    #[test]
    fn test_local_overlay_wins_over_base_read_path() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"shared.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();

        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.mode = 0o600;
        engine.set_attr(&attr).unwrap();
        engine
            .rename(ROOT_INO, b"shared.txt", ROOT_INO, b"local.txt")
            .unwrap();

        let loaded = engine.get_attr(file.ino).unwrap();
        assert_eq!(loaded.mode, 0o600);
        assert!(engine.lookup(ROOT_INO, b"shared.txt").is_err());
        let renamed = engine.lookup(ROOT_INO, b"local.txt").unwrap();
        assert_eq!(renamed.ino, file.ino);
    }

    #[test]
    fn test_unlink_base_only_entry_creates_local_tombstone() {
        let source = temp_engine();
        source
            .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        let base_key = encode_dentry_key(ROOT_INO, b"remote.txt");

        engine.unlink(ROOT_INO, b"remote.txt").unwrap();

        assert!(engine.lookup(ROOT_INO, b"remote.txt").is_err());
        assert!(engine.local_overlay_dentries().unwrap().contains(&base_key));
    }

    #[test]
    fn test_rename_base_only_entry_creates_local_overlay_mapping() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();

        engine
            .rename(ROOT_INO, b"remote.txt", ROOT_INO, b"local.txt")
            .unwrap();

        assert!(engine.lookup(ROOT_INO, b"remote.txt").is_err());
        let renamed = engine.lookup(ROOT_INO, b"local.txt").unwrap();
        assert_eq!(renamed.ino, file.ino);
    }

    #[test]
    fn test_link_base_only_entry_creates_local_overlay_mapping() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();

        engine.link(file.ino, ROOT_INO, b"local-link.txt").unwrap();

        let original = engine.lookup(ROOT_INO, b"remote.txt").unwrap();
        let link = engine.lookup(ROOT_INO, b"local-link.txt").unwrap();
        assert_eq!(original.ino, file.ino);
        assert_eq!(link.ino, file.ino);
    }

    #[test]
    fn test_add_entry_rejects_name_visible_from_base() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();

        let result = engine.add_entry(
            ROOT_INO,
            b"remote.txt",
            &DirEntryValue {
                ino: file.ino,
                kind: FileType::RegularFile,
            },
        );
        assert!(matches!(result, Err(MetaError::EntryExists { .. })));
    }

    #[test]
    fn test_remove_entry_base_only_creates_local_tombstone() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        let key = encode_dentry_key(ROOT_INO, b"remote.txt");

        let removed = engine.remove_entry(ROOT_INO, b"remote.txt").unwrap();
        assert_eq!(removed.ino, file.ino);
        assert!(engine.lookup(ROOT_INO, b"remote.txt").is_err());
        assert!(engine.local_overlay_dentries().unwrap().contains(&key));
    }

    #[test]
    fn test_referenced_object_ids_include_committed_blocks() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"refs.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        engine
            .set_file_block(ino, 0, &BlockRef::new_partial(21, 4096))
            .unwrap();
        engine
            .commit_uploaded_block(ino, 0, 0, &BlockRef::new_partial(21, 4096))
            .unwrap();
        engine
            .set_file_block(ino, 1, &BlockRef::new_partial(22, 4096))
            .unwrap();

        let refs = engine.get_referenced_object_ids().unwrap();
        assert!(refs.contains(&21));
        assert!(refs.contains(&22));
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn test_regular_file_graph_tables_roundtrip_and_project_remote_overlay() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"graph.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        let mut overlay_slices = BTreeMap::new();
        overlay_slices.insert(
            SliceId(1),
            SliceRecord::new(
                SliceId(1),
                SliceOrder(1),
                SliceRange::new(0, 4096).expect("valid overlay slice"),
                BlockRef::new_partial(301, 4096),
            ),
        );
        let overlay_chunk = RegularFileChunkState {
            next_slice_id: SliceId(2),
            next_slice_order: SliceOrder(2),
            slices: overlay_slices,
            fragmentation: ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 0,
                overlapping_slices: 0,
                fragmented_bytes: 4096,
            },
        };

        let mut overlay_state = RegularFileOverlayState::default();
        overlay_state
            .chunks
            .insert(ChunkId(11), overlay_chunk.clone());

        let mut base_slices = BTreeMap::new();
        base_slices.insert(
            SliceId(3),
            SliceRecord::new(
                SliceId(3),
                SliceOrder(3),
                SliceRange::new(0, 4096).expect("valid base slice"),
                BlockRef::new_partial(401, 4096),
            ),
        );
        let base_chunk = RegularFileChunkState {
            next_slice_id: SliceId(4),
            next_slice_order: SliceOrder(4),
            slices: base_slices,
            fragmentation: ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 0,
                overlapping_slices: 0,
                fragmented_bytes: 4096,
            },
        };

        let mut base_state = RegularFileBaseState::default();
        base_state.chunks.insert(ChunkId(11), base_chunk.clone());

        let legacy_block = BlockRef::new_partial(901, 1024);
        let legacy_bytes = encode(&legacy_block).expect("encode legacy block");
        let legacy_key = encode_file_block_key(ino, 0);
        let overlay_bytes = encode(&overlay_chunk).expect("encode overlay chunk");
        let base_bytes = encode(&base_chunk).expect("encode base chunk");
        let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(11));

        let txn = engine.db.begin_write().unwrap();
        {
            let mut committed_table = txn.open_table(COMMITTED_FILE_BLOCKS_TABLE).unwrap();
            committed_table
                .insert(legacy_key.as_slice(), legacy_bytes.as_slice())
                .unwrap();

            let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
            overlay_table
                .insert(chunk_key.as_slice(), overlay_bytes.as_slice())
                .unwrap();

            let mut base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
            base_table
                .insert(chunk_key.as_slice(), base_bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();

        assert!(engine.local_overlay_inodes().unwrap().contains(&ino));

        let snapshot = engine.export_snapshot().unwrap();
        assert_eq!(
            snapshot.regular_file_overlay,
            vec![(
                ino,
                encode(&overlay_state).expect("encode overlay snapshot")
            )]
        );
        assert_eq!(
            snapshot.regular_file_base,
            vec![(ino, encode(&base_state).expect("encode base snapshot"))]
        );
        let imported = temp_engine();
        imported.import_snapshot(&snapshot).unwrap();

        let txn = imported.db.begin_read().unwrap();
        let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        let overlay_row = overlay_table
            .get(chunk_key.as_slice())
            .unwrap()
            .expect("imported overlay row");
        let imported_overlay: RegularFileChunkState =
            decode(overlay_row.value()).expect("decode imported overlay");
        assert_eq!(imported_overlay, overlay_chunk);

        let base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
        let base_row = base_table
            .get(chunk_key.as_slice())
            .unwrap()
            .expect("imported base row");
        let imported_base: RegularFileChunkState =
            decode(base_row.value()).expect("decode imported base");
        assert_eq!(imported_base, base_chunk);

        let remote_snapshot = engine.export_remote_snapshot().unwrap();
        assert_eq!(
            remote_snapshot.regular_file_overlay,
            remote_snapshot.regular_file_base
        );
        assert_eq!(
            remote_snapshot.regular_file_base,
            snapshot.regular_file_base
        );
        let used_space = remote_snapshot
            .counters
            .iter()
            .find(|(name, _)| name.as_str() == counters::USED_SPACE)
            .map(|(_, value)| *value)
            .expect("used space counter");
        assert_eq!(used_space, 1024 + 4096);
    }

    #[test]
    fn test_import_snapshot_rebuilds_pending_compaction_rows() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"snapshot-pending.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = file.ino;

        let mut chunk_slices = BTreeMap::new();
        chunk_slices.insert(
            SliceId(1),
            SliceRecord::new(
                SliceId(1),
                SliceOrder(1),
                SliceRange::new(0, 4096).expect("first slice"),
                BlockRef::new_partial(1001, 4096),
            ),
        );
        chunk_slices.insert(
            SliceId(2),
            SliceRecord::new(
                SliceId(2),
                SliceOrder(2),
                SliceRange::new(0, 4096).expect("shadowed slice"),
                BlockRef::new_partial(1002, 4096),
            ),
        );
        let mut overlay_state = RegularFileOverlayState::default();
        overlay_state.chunks.insert(
            ChunkId(4),
            RegularFileChunkState {
                next_slice_id: SliceId(3),
                next_slice_order: SliceOrder(3),
                slices: chunk_slices,
                fragmentation: ChunkFragmentationCounters {
                    live_slices: 2,
                    tombstone_slices: 0,
                    overlapping_slices: 1,
                    fragmented_bytes: 4096,
                },
            },
        );

        let txn = source.db.begin_write().unwrap();
        {
            let mut overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
            let key = encode_regular_file_chunk_key(ino, ChunkId(4));
            let bytes = encode(&overlay_state.chunks[&ChunkId(4)]).unwrap();
            overlay.insert(key.as_slice(), bytes.as_slice()).unwrap();
        }
        txn.commit().unwrap();

        let snapshot = source.export_snapshot().unwrap();

        let target = temp_engine();
        set_regular_file_pending_compaction_rows(&target, ino, &[ChunkId(99)]);

        target.import_snapshot(&snapshot).unwrap();

        let txn = target.db.begin_read().unwrap();
        let pending = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .unwrap();
        assert!(
            pending
                .get(encode_regular_file_chunk_key(ino, ChunkId(99)).as_slice())
                .unwrap()
                .is_none()
        );
        assert!(
            pending
                .get(encode_regular_file_chunk_key(ino, ChunkId(4)).as_slice())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn test_regular_file_overlay_detection_ignores_counter_only_changes() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"counter-only.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        let mut overlay_slice = SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("valid overlay slice"),
            BlockRef::new_partial(501, 4096),
        );
        overlay_slice.block_state = BlockRefState::Uploading;
        let mut overlay_slices = BTreeMap::new();
        overlay_slices.insert(overlay_slice.slice_id, overlay_slice);
        let overlay_chunk = RegularFileChunkState {
            next_slice_id: SliceId(2),
            next_slice_order: SliceOrder(2),
            slices: overlay_slices,
            fragmentation: ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 0,
                overlapping_slices: 0,
                fragmented_bytes: 4096,
            },
        };

        let mut base_chunk = RegularFileChunkState {
            next_slice_id: SliceId(9),
            next_slice_order: SliceOrder(9),
            slices: overlay_chunk.slices.clone(),
            fragmentation: ChunkFragmentationCounters {
                live_slices: 0,
                tombstone_slices: 1,
                overlapping_slices: 2,
                fragmented_bytes: 8192,
            },
        };
        base_chunk
            .slices
            .get_mut(&SliceId(1))
            .expect("base slice")
            .block_state = BlockRefState::Published;

        let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(11));
        let overlay_bytes = encode(&overlay_chunk).expect("encode overlay chunk");
        let base_bytes = encode(&base_chunk).expect("encode base chunk");

        let txn = engine.db.begin_write().unwrap();
        {
            let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
            overlay_table
                .insert(chunk_key.as_slice(), overlay_bytes.as_slice())
                .unwrap();

            let mut base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
            base_table
                .insert(chunk_key.as_slice(), base_bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();

        assert!(!engine.local_overlay_inodes().unwrap().contains(&ino));
    }

    #[test]
    fn test_regular_file_graph_signature_ignores_slice_identity_changes() {
        let mut chunk_a = RegularFileChunkState::default();
        chunk_a.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("slice a"),
            BlockRef::new_partial(501, 4096),
        ));

        let mut chunk_b = RegularFileChunkState::default();
        chunk_b.insert_slice(SliceRecord::new(
            SliceId(9),
            SliceOrder(17),
            SliceRange::new(0, 4096).expect("slice b"),
            BlockRef::new_partial(501, 4096),
        ));

        let mut chunks_a = BTreeMap::new();
        chunks_a.insert(ChunkId(0), chunk_a);
        let mut chunks_b = BTreeMap::new();
        chunks_b.insert(ChunkId(0), chunk_b);

        assert_eq!(
            RedbMetaEngine::regular_file_graph_signature(&chunks_a),
            RedbMetaEngine::regular_file_graph_signature(&chunks_b)
        );
    }

    #[test]
    fn test_regular_file_overlay_flush_allocates_unique_slice_ids_and_orders() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"overlay-flush.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;
        let chunk_id = ChunkId(11);
        let chunk_key = encode_regular_file_chunk_key(ino, chunk_id);

        let mut existing_slices = BTreeMap::new();
        existing_slices.insert(
            SliceId(3),
            SliceRecord::new(
                SliceId(3),
                SliceOrder(8),
                SliceRange::new(0, 4096).expect("valid existing slice"),
                BlockRef::new_partial(601, 4096),
            ),
        );
        let existing_chunk = RegularFileChunkState {
            next_slice_id: SliceId(7),
            next_slice_order: SliceOrder(11),
            slices: existing_slices,
            fragmentation: ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 0,
                overlapping_slices: 0,
                fragmented_bytes: 4096,
            },
        };
        let existing_bytes = encode(&existing_chunk).expect("encode existing chunk");

        let txn = engine.db.begin_write().unwrap();
        {
            let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
            overlay_table
                .insert(chunk_key.as_slice(), existing_bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();

        let incoming = vec![
            SliceRecord {
                slice_id: SliceId::default(),
                order: SliceOrder::default(),
                range: SliceRange::new(0, 4096).expect("valid replacement slice"),
                block: Some(BlockRef::new_partial(701, 4096)),
                block_state: BlockRefState::LocalOnly,
            },
            SliceRecord {
                slice_id: SliceId::default(),
                order: SliceOrder::default(),
                range: SliceRange::new(4096, 8192).expect("valid appended slice"),
                block: Some(BlockRef::new_partial(702, 4096)),
                block_state: BlockRefState::LocalOnly,
            },
        ];

        engine
            .flush_regular_file_overlay_slices(ino, &[(chunk_id, incoming)])
            .unwrap();

        let txn = engine.db.begin_read().unwrap();
        let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        let row = overlay_table
            .get(chunk_key.as_slice())
            .unwrap()
            .expect("flushed overlay row");
        let chunk: RegularFileChunkState = decode(row.value()).expect("decode flushed chunk");

        assert_eq!(chunk.next_slice_id, SliceId(9));
        assert_eq!(chunk.next_slice_order, SliceOrder(13));
        assert_eq!(chunk.slices.len(), 2);

        let slices: Vec<_> = chunk.slices.values().collect();
        assert_eq!(
            slices
                .iter()
                .map(|slice| slice.slice_id)
                .collect::<Vec<_>>(),
            vec![SliceId(7), SliceId(8)]
        );
        assert_eq!(
            slices.iter().map(|slice| slice.order).collect::<Vec<_>>(),
            vec![SliceOrder(11), SliceOrder(12)]
        );
        assert_eq!(
            slices[0].range,
            SliceRange::new(0, 4096).expect("first flushed range")
        );
        assert_eq!(
            slices[1].range,
            SliceRange::new(4096, 8192).expect("second flushed range")
        );
    }

    #[test]
    fn test_regular_file_overlay_flush_emits_final_chunk_state() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"overlay-event.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;
        let chunk_id = ChunkId(9);
        let chunk_key = encode_regular_file_chunk_key(ino, chunk_id);

        let mut existing_slices = BTreeMap::new();
        existing_slices.insert(
            SliceId(3),
            SliceRecord::new(
                SliceId(3),
                SliceOrder(8),
                SliceRange::new(0, 4096).expect("valid existing slice"),
                BlockRef::new_partial(801, 4096),
            ),
        );
        let existing_chunk = RegularFileChunkState {
            next_slice_id: SliceId(7),
            next_slice_order: SliceOrder(11),
            slices: existing_slices,
            fragmentation: ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 0,
                overlapping_slices: 0,
                fragmented_bytes: 4096,
            },
        };
        let existing_bytes = encode(&existing_chunk).expect("encode existing chunk");

        let txn = engine.db.begin_write().unwrap();
        {
            let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
            overlay_table
                .insert(chunk_key.as_slice(), existing_bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();

        let incoming = vec![SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(4096, 8192).expect("incoming range"),
            block: Some(BlockRef::new_partial(802, 4096)),
            block_state: BlockRefState::LocalOnly,
        }];

        engine
            .flush_regular_file_overlay_slices(ino, &[(chunk_id, incoming)])
            .unwrap();

        let txn = engine.db.begin_read().unwrap();
        let counters = txn.open_table(COUNTERS_TABLE).unwrap();
        let next_event_seq = counters
            .get("nextEventSeq")
            .unwrap()
            .expect("next event seq")
            .value();
        let events = txn.open_table(EVENT_LOG_TABLE).unwrap();
        let row = events
            .get(next_event_seq - 1)
            .unwrap()
            .expect("flushed event");
        let event: MetaEvent = decode(row.value()).expect("decode flushed event");
        let payload = match event {
            MetaEvent::SetRegularFileOverlaySlices {
                ino: event_ino,
                chunk_slices,
            } => {
                assert_eq!(event_ino, ino);
                chunk_slices
            }
            other => panic!("unexpected event kind: {:?}", other),
        };

        let overlay_row = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .unwrap()
            .get(chunk_key.as_slice())
            .unwrap()
            .expect("flushed overlay row");
        let chunk: RegularFileChunkState = decode(overlay_row.value()).expect("decode chunk");

        let expected_slices: Vec<_> = chunk.slices.values().cloned().collect();
        assert_eq!(payload, vec![(chunk_id, expected_slices)]);
    }

    #[test]
    fn test_regular_file_overlay_slice_events_replace_rows_and_delete_empty_payload() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"overlay-replay.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;
        let chunk_id = ChunkId(17);
        let chunk_key = encode_regular_file_chunk_key(ino, chunk_id);

        let mut initial_slices = BTreeMap::new();
        initial_slices.insert(
            SliceId(1),
            SliceRecord::new(
                SliceId(1),
                SliceOrder(4),
                SliceRange::new(0, 8192).expect("initial slice"),
                BlockRef::new_partial(901, 8192),
            ),
        );
        let initial_chunk = RegularFileChunkState {
            next_slice_id: SliceId(2),
            next_slice_order: SliceOrder(5),
            slices: initial_slices,
            fragmentation: ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 0,
                overlapping_slices: 0,
                fragmented_bytes: 0,
            },
        };
        set_regular_file_chunk_rows(
            &engine,
            ino,
            &[(chunk_id, initial_chunk.clone())],
            &[(chunk_id, initial_chunk)],
        );

        let replacement_event = MetaEvent::SetRegularFileOverlaySlices {
            ino,
            chunk_slices: vec![(
                chunk_id,
                vec![
                    SliceRecord::new(
                        SliceId(9),
                        SliceOrder(9),
                        SliceRange::new(0, 4096).expect("replacement data"),
                        BlockRef::new_partial(902, 4096),
                    ),
                    SliceRecord::tombstone(
                        SliceId(10),
                        SliceOrder(10),
                        SliceRange::new(4096, 8192).expect("replacement tombstone"),
                    ),
                ],
            )],
        };

        engine
            .apply_replica_events_to_base(std::slice::from_ref(&replacement_event))
            .unwrap();
        engine
            .apply_replica_serving_state(std::slice::from_ref(&replacement_event))
            .unwrap();

        {
            let txn = engine.db.begin_read().unwrap();
            let base_row = txn
                .open_table(REGULAR_FILE_BASE_TABLE)
                .unwrap()
                .get(chunk_key.as_slice())
                .unwrap()
                .expect("replayed base row");
            let base_chunk: RegularFileChunkState = decode(base_row.value()).expect("decode base");
            let overlay_row = txn
                .open_table(REGULAR_FILE_OVERLAY_TABLE)
                .unwrap()
                .get(chunk_key.as_slice())
                .unwrap()
                .expect("replayed overlay row");
            let overlay_chunk: RegularFileChunkState =
                decode(overlay_row.value()).expect("decode overlay");

            let expected_chunk = RegularFileChunkState {
                next_slice_id: SliceId(11),
                next_slice_order: SliceOrder(11),
                slices: vec![
                    SliceRecord::new(
                        SliceId(9),
                        SliceOrder(9),
                        SliceRange::new(0, 4096).expect("expected replacement data"),
                        BlockRef::new_partial(902, 4096),
                    ),
                    SliceRecord::tombstone(
                        SliceId(10),
                        SliceOrder(10),
                        SliceRange::new(4096, 8192).expect("expected replacement tombstone"),
                    ),
                ]
                .into_iter()
                .map(|slice| (slice.slice_id, slice))
                .collect(),
                fragmentation: ChunkFragmentationCounters {
                    live_slices: 1,
                    tombstone_slices: 1,
                    overlapping_slices: 0,
                    fragmented_bytes: 0,
                },
            };

            assert_eq!(base_chunk, expected_chunk);
            assert_eq!(overlay_chunk, expected_chunk);
            assert!(
                txn.open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                    .unwrap()
                    .get(chunk_key.as_slice())
                    .unwrap()
                    .is_none()
            );
        }

        let delete_event = MetaEvent::SetRegularFileOverlaySlices {
            ino,
            chunk_slices: vec![(chunk_id, Vec::new())],
        };
        engine
            .apply_replica_events_to_base(std::slice::from_ref(&delete_event))
            .unwrap();
        engine
            .apply_replica_serving_state(std::slice::from_ref(&delete_event))
            .unwrap();

        let txn = engine.db.begin_read().unwrap();
        assert!(
            txn.open_table(REGULAR_FILE_BASE_TABLE)
                .unwrap()
                .get(chunk_key.as_slice())
                .unwrap()
                .is_none()
        );
        assert!(
            txn.open_table(REGULAR_FILE_OVERLAY_TABLE)
                .unwrap()
                .get(chunk_key.as_slice())
                .unwrap()
                .is_none()
        );
        assert!(
            txn.open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .unwrap()
                .get(chunk_key.as_slice())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_flush_regular_file_overlay_slices_with_attr_is_atomic() {
        let engine = temp_engine();
        let mut attr = engine
            .create_file(ROOT_INO, b"overlay-atomic.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;
        attr.content_epoch = 11;
        attr.size = 4096;
        engine.set_attr(&attr).unwrap();

        let incoming = vec![SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(0, 4096).expect("valid range"),
            block: Some(BlockRef::new_partial(200_001, 4096)),
            block_state: BlockRefState::LocalOnly,
        }];

        let mut new_attr = attr.clone();
        new_attr.size = 8192;
        new_attr.content_epoch = 12;
        engine
            .flush_regular_file_overlay_slices_with_attr(
                ino,
                &[(ChunkId(0), incoming)],
                Some(&new_attr),
            )
            .unwrap();

        let persisted_attr = engine.get_attr(ino).unwrap();
        assert_eq!(persisted_attr.size, 8192);
        assert_eq!(persisted_attr.content_epoch, 12);
        assert_eq!(persisted_attr.upload_state, UploadState::Uploading);

        let txn = engine.db.begin_read().unwrap();
        assert!(
            RedbMetaEngine::has_regular_file_chunk_rows_in_read_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino
            )
            .unwrap()
        );
    }

    #[test]
    fn test_lookup_regular_file_chunk_slices_overlay_precedence_with_tombstone() {
        let engine = temp_engine();
        let ino = engine
            .create_file(ROOT_INO, b"lookup-overlay.bin", 1000, 1000, 0o644)
            .unwrap()
            .ino;

        let mut base_chunk = RegularFileChunkState::default();
        base_chunk.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 8192).expect("valid base slice 0"),
            BlockRef::new_partial(901, 8192),
        ));
        base_chunk.insert_slice(SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(8192, 12288).expect("valid base slice 1"),
            BlockRef::new_partial(902, 4096),
        ));

        let mut overlay_chunk = RegularFileChunkState::default();
        overlay_chunk.insert_slice(SliceRecord::new(
            SliceId(10),
            SliceOrder(1),
            SliceRange::new(2048, 6144).expect("valid overlay data slice"),
            BlockRef::new_partial(801, 4096),
        ));
        overlay_chunk.insert_slice(SliceRecord::tombstone(
            SliceId(11),
            SliceOrder(2),
            SliceRange::new(7000, 9000).expect("valid tombstone slice"),
        ));

        set_regular_file_chunk_rows(
            &engine,
            ino,
            &[(ChunkId(0), overlay_chunk)],
            &[(ChunkId(0), base_chunk)],
        );

        let spans = engine
            .lookup_regular_file_chunk_slices(
                ino,
                ChunkId(0),
                SliceRange::new(0, 12288).expect("valid query"),
            )
            .unwrap();
        assert_eq!(spans.len(), 5);
        assert_eq!(
            spans[0].range,
            SliceRange {
                start: 0,
                end: 2048
            }
        );
        assert_eq!(spans[0].source, ResolvedSliceSource::Base);
        assert_eq!(spans[0].block.as_ref().unwrap().object_id, 901);

        assert_eq!(
            spans[1].range,
            SliceRange {
                start: 2048,
                end: 6144
            }
        );
        assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
        assert_eq!(spans[1].block.as_ref().unwrap().object_id, 801);

        assert_eq!(
            spans[2].range,
            SliceRange {
                start: 6144,
                end: 7000
            }
        );
        assert_eq!(spans[2].source, ResolvedSliceSource::Base);
        assert_eq!(spans[2].block.as_ref().unwrap().object_id, 901);

        assert_eq!(
            spans[3].range,
            SliceRange {
                start: 7000,
                end: 9000
            }
        );
        assert_eq!(spans[3].source, ResolvedSliceSource::OverlayTombstone);
        assert!(spans[3].block.is_none());

        assert_eq!(
            spans[4].range,
            SliceRange {
                start: 9000,
                end: 12288
            }
        );
        assert_eq!(spans[4].source, ResolvedSliceSource::Base);
        assert_eq!(spans[4].block.as_ref().unwrap().object_id, 902);
    }

    #[test]
    fn test_compact_fragmented_regular_file_chunks_preserves_tombstones() {
        let engine = temp_engine();
        let ino = engine
            .create_file(ROOT_INO, b"compact-overlay.bin", 1000, 1000, 0o644)
            .unwrap()
            .ino;

        let mut overlay_chunk = RegularFileChunkState::default();
        overlay_chunk.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("shadowed slice"),
            BlockRef::new_partial(1201, 4096),
        ));
        overlay_chunk.insert_slice(SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(0, 4096).expect("visible slice"),
            BlockRef::new_partial(1202, 4096),
        ));
        overlay_chunk.insert_slice(SliceRecord::tombstone(
            SliceId(3),
            SliceOrder(3),
            SliceRange::new(4096, 6144).expect("tombstone range"),
        ));

        set_regular_file_chunk_rows(&engine, ino, &[(ChunkId(0), overlay_chunk.clone())], &[]);

        let txn = engine.db.begin_write().unwrap();
        {
            let mut pending = txn
                .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .unwrap();
            let key = encode_regular_file_chunk_key(ino, ChunkId(0));
            pending.insert(key.as_slice(), 1).unwrap();
        }
        txn.commit().unwrap();

        let query = SliceRange::new(0, 6144).expect("lookup query");
        let before = engine
            .lookup_regular_file_chunk_slices(ino, ChunkId(0), query)
            .unwrap();
        assert_eq!(before.len(), 2);
        assert_eq!(before[0].source, ResolvedSliceSource::Overlay);
        assert_eq!(before[0].block.as_ref().unwrap().object_id, 1202);
        assert_eq!(before[1].source, ResolvedSliceSource::OverlayTombstone);
        assert!(before[1].block.is_none());

        let compacted = engine.compact_fragmented_regular_file_chunks(1).unwrap();
        assert_eq!(compacted, 1);

        let after = engine
            .lookup_regular_file_chunk_slices(ino, ChunkId(0), query)
            .unwrap();
        assert_eq!(after.len(), before.len());
        for (expected, actual) in before.iter().zip(after.iter()) {
            assert_eq!(actual.range, expected.range);
            assert_eq!(actual.source, expected.source);
            assert_eq!(actual.block, expected.block);
        }

        let txn = engine.db.begin_read().unwrap();
        let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        let key = encode_regular_file_chunk_key(ino, ChunkId(0));
        let row = overlay_table
            .get(key.as_slice())
            .unwrap()
            .expect("compacted overlay chunk");
        let compacted_chunk: RegularFileChunkState =
            decode(row.value()).expect("decode compacted chunk");
        assert_eq!(compacted_chunk.slices.len(), 2);
        assert_eq!(
            compacted_chunk.fragmentation,
            ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 1,
                overlapping_slices: 0,
                fragmented_bytes: 0,
            }
        );
        assert!(!compacted_chunk.fragmentation.should_compact());
    }

    #[test]
    fn test_lookup_regular_file_slices_cross_chunk_and_ordered_base_resolution() {
        let engine = temp_engine();
        let ino = engine
            .create_file(ROOT_INO, b"lookup-cross-chunk.bin", 1000, 1000, 0o644)
            .unwrap()
            .ino;

        let tail_start = DEFAULT_CHUNK_SIZE - 2048;

        let mut chunk0_base = RegularFileChunkState::default();
        chunk0_base.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(tail_start, DEFAULT_CHUNK_SIZE).expect("valid chunk0 base"),
            BlockRef::new_partial(1101, 2048),
        ));
        let mut chunk0_overlay = RegularFileChunkState::default();
        chunk0_overlay.insert_slice(SliceRecord::new(
            SliceId(2),
            SliceOrder(1),
            SliceRange::new(tail_start + 512, tail_start + 1536).expect("valid chunk0 overlay"),
            BlockRef::new_partial(1201, 1024),
        ));

        let mut chunk1_base = RegularFileChunkState::default();
        chunk1_base.insert_slice(SliceRecord::new(
            SliceId(3),
            SliceOrder(1),
            SliceRange::new(0, 1024).expect("valid chunk1 base low order"),
            BlockRef::new_partial(1301, 1024),
        ));
        chunk1_base.insert_slice(SliceRecord::new(
            SliceId(4),
            SliceOrder(2),
            SliceRange::new(512, 2048).expect("valid chunk1 base high order"),
            BlockRef::new_partial(1401, 1536),
        ));

        set_regular_file_chunk_rows(
            &engine,
            ino,
            &[(ChunkId(0), chunk0_overlay)],
            &[(ChunkId(0), chunk0_base), (ChunkId(1), chunk1_base)],
        );

        let spans = engine
            .lookup_regular_file_slices(ino, DEFAULT_CHUNK_SIZE as u64 - 2048, 4096)
            .unwrap();
        assert_eq!(spans.len(), 5);

        assert_eq!(spans[0].chunk_id, ChunkId(0));
        assert_eq!(
            spans[0].range,
            SliceRange {
                start: tail_start,
                end: tail_start + 512
            }
        );
        assert_eq!(spans[0].source, ResolvedSliceSource::Base);
        assert_eq!(spans[0].block.as_ref().unwrap().object_id, 1101);

        assert_eq!(spans[1].chunk_id, ChunkId(0));
        assert_eq!(
            spans[1].range,
            SliceRange {
                start: tail_start + 512,
                end: tail_start + 1536
            }
        );
        assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
        assert_eq!(spans[1].block.as_ref().unwrap().object_id, 1201);

        assert_eq!(spans[2].chunk_id, ChunkId(0));
        assert_eq!(
            spans[2].range,
            SliceRange {
                start: tail_start + 1536,
                end: DEFAULT_CHUNK_SIZE
            }
        );
        assert_eq!(spans[2].source, ResolvedSliceSource::Base);
        assert_eq!(spans[2].block.as_ref().unwrap().object_id, 1101);

        assert_eq!(spans[3].chunk_id, ChunkId(1));
        assert_eq!(spans[3].range, SliceRange { start: 0, end: 512 });
        assert_eq!(spans[3].source, ResolvedSliceSource::Base);
        assert_eq!(spans[3].block.as_ref().unwrap().object_id, 1301);

        assert_eq!(spans[4].chunk_id, ChunkId(1));
        assert_eq!(
            spans[4].range,
            SliceRange {
                start: 512,
                end: 2048
            }
        );
        assert_eq!(spans[4].source, ResolvedSliceSource::Base);
        assert_eq!(spans[4].block.as_ref().unwrap().object_id, 1401);
    }

    #[test]
    fn test_lookup_regular_file_slices_boundary_spanning_write_is_split_per_chunk() {
        let engine = temp_engine();
        let ino = engine
            .create_file(ROOT_INO, b"lookup-boundary-split.bin", 1000, 1000, 0o644)
            .unwrap()
            .ino;

        let tail_start = DEFAULT_CHUNK_SIZE - 1024;

        let mut chunk0_base = RegularFileChunkState::default();
        chunk0_base.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(tail_start, DEFAULT_CHUNK_SIZE).expect("valid chunk0 base"),
            BlockRef::new_partial(2101, 1024),
        ));
        let mut chunk1_base = RegularFileChunkState::default();
        chunk1_base.insert_slice(SliceRecord::new(
            SliceId(2),
            SliceOrder(1),
            SliceRange::new(0, 1024).expect("valid chunk1 base"),
            BlockRef::new_partial(2102, 1024),
        ));

        let mut chunk0_overlay = RegularFileChunkState::default();
        chunk0_overlay.insert_slice(SliceRecord::new(
            SliceId(10),
            SliceOrder(1),
            SliceRange::new(tail_start + 512, DEFAULT_CHUNK_SIZE).expect("valid chunk0 overlay"),
            BlockRef::new_partial(2201, 512),
        ));
        let mut chunk1_overlay = RegularFileChunkState::default();
        chunk1_overlay.insert_slice(SliceRecord::new(
            SliceId(11),
            SliceOrder(1),
            SliceRange::new(0, 512).expect("valid chunk1 overlay"),
            BlockRef::new_partial(2202, 512),
        ));

        set_regular_file_chunk_rows(
            &engine,
            ino,
            &[(ChunkId(0), chunk0_overlay), (ChunkId(1), chunk1_overlay)],
            &[(ChunkId(0), chunk0_base), (ChunkId(1), chunk1_base)],
        );

        let spans = engine
            .lookup_regular_file_slices(ino, DEFAULT_CHUNK_SIZE as u64 - 1024, 2048)
            .unwrap();
        assert_eq!(spans.len(), 4);

        assert_eq!(spans[0].chunk_id, ChunkId(0));
        assert_eq!(
            spans[0].range,
            SliceRange {
                start: tail_start,
                end: tail_start + 512
            }
        );
        assert_eq!(spans[0].source, ResolvedSliceSource::Base);
        assert_eq!(spans[0].block.as_ref().unwrap().object_id, 2101);

        assert_eq!(spans[1].chunk_id, ChunkId(0));
        assert_eq!(
            spans[1].range,
            SliceRange {
                start: tail_start + 512,
                end: DEFAULT_CHUNK_SIZE
            }
        );
        assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
        assert_eq!(spans[1].block.as_ref().unwrap().object_id, 2201);

        assert_eq!(spans[2].chunk_id, ChunkId(1));
        assert_eq!(spans[2].range, SliceRange { start: 0, end: 512 });
        assert_eq!(spans[2].source, ResolvedSliceSource::Overlay);
        assert_eq!(spans[2].block.as_ref().unwrap().object_id, 2202);

        assert_eq!(spans[3].chunk_id, ChunkId(1));
        assert_eq!(
            spans[3].range,
            SliceRange {
                start: 512,
                end: 1024
            }
        );
        assert_eq!(spans[3].source, ResolvedSliceSource::Base);
        assert_eq!(spans[3].block.as_ref().unwrap().object_id, 2102);
    }

    #[test]
    fn test_publish_regular_file_overlay_to_base_if_matches_promotes_base_rows() {
        let engine = temp_engine();
        let file = engine
            .create_file(ROOT_INO, b"publish-slice-base.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.content_epoch = 7;
        attr.size = chunk_start_offset(ChunkId(2), DEFAULT_CHUNK_SIZE) + 4096;
        engine.set_attr(&attr).unwrap();

        let mut overlay_chunk = RegularFileChunkState::default();
        overlay_chunk.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("valid overlay data"),
            BlockRef::new_partial(1601, 4096),
        ));
        overlay_chunk.insert_slice(SliceRecord::tombstone(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(4096, 8192).expect("valid overlay tombstone"),
        ));
        set_regular_file_chunk_rows(&engine, file.ino, &[(ChunkId(2), overlay_chunk)], &[]);

        assert!(
            !engine
                .publish_regular_file_overlay_to_base_if_matches(file.ino, 6)
                .unwrap()
        );
        assert!(
            engine
                .publish_regular_file_overlay_to_base_if_matches(file.ino, 7)
                .unwrap()
        );

        let txn = engine.db.begin_read().unwrap();
        let base = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
        let chunk_key = encode_regular_file_chunk_key(file.ino, ChunkId(2));
        let row = base
            .get(chunk_key.as_slice())
            .unwrap()
            .expect("published base chunk");
        let published: RegularFileChunkState = decode(row.value()).unwrap();
        assert_eq!(published.slices.len(), 2);
        assert_eq!(
            published.slices.get(&SliceId(1)).unwrap().block_state,
            BlockRefState::Published
        );
        assert_eq!(
            published.slices.get(&SliceId(2)).unwrap().block_state,
            BlockRefState::Published
        );
        let overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        assert!(
            overlay.get(chunk_key.as_slice()).unwrap().is_none(),
            "overlay row must be cleared after publish"
        );

        let published_attr = engine.get_attr(file.ino).unwrap();
        assert_eq!(
            published_attr.committed_size,
            chunk_start_offset(ChunkId(2), DEFAULT_CHUNK_SIZE) + 4096
        );
        assert_eq!(published_attr.committed_content_epoch, 7);
        assert_eq!(published_attr.upload_state, UploadState::Ready);
    }

    #[test]
    fn test_publish_regular_file_overlay_to_base_keeps_sparse_tail_committed_size_below_size() {
        let engine = temp_engine();
        let file = engine
            .create_file(ROOT_INO, b"publish-sparse-tail.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.content_epoch = 5;
        attr.size = 16 * 1024;
        engine.set_attr(&attr).unwrap();

        let mut overlay_chunk = RegularFileChunkState::default();
        overlay_chunk.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("valid sparse data slice"),
            BlockRef::new_partial(2601, 4096),
        ));
        set_regular_file_chunk_rows(&engine, file.ino, &[(ChunkId(0), overlay_chunk)], &[]);

        assert!(
            engine
                .publish_regular_file_overlay_to_base_if_matches(file.ino, 5)
                .unwrap()
        );

        let published_attr = engine.get_attr(file.ino).unwrap();
        assert_eq!(published_attr.size, 16 * 1024);
        assert_eq!(published_attr.committed_size, 4096);
        assert!(published_attr.committed_size < published_attr.size);
    }

    #[test]
    fn test_compute_committed_size_prefers_regular_file_base_rows() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"committed-size.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        let mut chunk = RegularFileChunkState::default();
        chunk.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("valid committed slice"),
            BlockRef::new_partial(601, 4096),
        ));

        let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(1));
        let chunk_bytes = encode(&chunk).expect("encode committed chunk");
        let legacy_key = encode_file_block_key(ino, 0);
        let legacy_block = BlockRef::new_partial(701, 1024);
        let legacy_bytes = encode(&legacy_block).expect("encode legacy block");

        let txn = engine.db.begin_write().unwrap();
        {
            {
                let mut base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
                base_table
                    .insert(chunk_key.as_slice(), chunk_bytes.as_slice())
                    .unwrap();
            }

            let mut committed_table = txn.open_table(COMMITTED_FILE_BLOCKS_TABLE).unwrap();
            committed_table
                .insert(legacy_key.as_slice(), legacy_bytes.as_slice())
                .unwrap();
            let committed_size =
                RedbMetaEngine::compute_committed_size_in_txn(&txn, &committed_table, ino)
                    .expect("compute committed size");
            assert_eq!(
                committed_size,
                chunk_start_offset(ChunkId(1), DEFAULT_CHUNK_SIZE) + 4096
            );
        }
    }

    #[test]
    fn test_redb_snapshot_defaults_missing_regular_file_fields() {
        let snapshot_json = r#"{
            "settings": [],
            "counters": [],
            "inodes": [],
            "dentries": [],
            "file_blocks": [],
            "committed_file_blocks": [],
            "pending_deletions": [],
            "symlinks": [],
            "xattrs": [],
            "acl_rules": [],
            "event_log": [],
            "snapshots": []
        }"#;
        let snapshot: RedbSnapshot = serde_json::from_str(snapshot_json).unwrap();
        assert!(snapshot.regular_file_overlay.is_empty());
        assert!(snapshot.regular_file_base.is_empty());
    }

    #[test]
    fn test_regular_file_graph_rows_removed_on_unlink() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"unlink-graph.bin", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;

        let mut chunk = RegularFileChunkState::default();
        chunk.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("valid slice"),
            BlockRef::new_partial(777, 4096),
        ));
        let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(0));
        let chunk_bytes = encode(&chunk).expect("encode chunk state");

        let txn = engine.db.begin_write().unwrap();
        {
            let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
            overlay_table
                .insert(chunk_key.as_slice(), chunk_bytes.as_slice())
                .unwrap();

            let mut base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
            base_table
                .insert(chunk_key.as_slice(), chunk_bytes.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();

        engine.unlink(ROOT_INO, b"unlink-graph.bin").unwrap();

        let txn = engine.db.begin_read().unwrap();
        let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        assert!(overlay_table.get(chunk_key.as_slice()).unwrap().is_none());
        let base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
        assert!(base_table.get(chunk_key.as_slice()).unwrap().is_none());
    }

    #[test]
    fn test_commit_uploaded_block_rejects_stale_epoch() {
        let engine = temp_engine();

        let file = engine
            .create_file(ROOT_INO, b"stale-epoch.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 4096;
        attr.content_epoch = 3;
        engine.set_attr(&attr).unwrap();

        let block = BlockRef::new_partial(30, 4096);
        engine.set_file_block(file.ino, 0, &block).unwrap();

        assert!(
            !engine
                .commit_uploaded_block(file.ino, 0, 2, &block)
                .unwrap()
        );
        assert!(engine.get_pending_deletions().unwrap().is_empty());
    }

    #[test]
    fn test_commit_uploaded_block_rejects_replaced_block() {
        let engine = temp_engine();

        let file = engine
            .create_file(ROOT_INO, b"stale-block.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 4096;
        engine.set_attr(&attr).unwrap();

        let old = BlockRef::new_partial(41, 4096);
        engine.set_file_block(file.ino, 0, &old).unwrap();
        let new = BlockRef::new_partial(42, 4096);
        engine.set_file_block(file.ino, 0, &new).unwrap();

        assert!(!engine.commit_uploaded_block(file.ino, 0, 0, &old).unwrap());
        assert!(engine.commit_uploaded_block(file.ino, 0, 0, &new).unwrap());
    }

    #[test]
    fn redb_manifest_publish_sets_committed_head() {
        let engine = temp_engine();

        let file = engine
            .create_file(ROOT_INO, b"publish-head.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 8192;
        attr.content_epoch = 1;
        engine.set_attr(&attr).unwrap();

        let block0 = BlockRef::new_partial(101, 4096);
        let block1 = BlockRef::new_partial(102, 4096);
        engine.set_file_block(file.ino, 0, &block0).unwrap();
        engine.set_file_block(file.ino, 1, &block1).unwrap();

        assert!(engine.publish_file_manifest(file.ino, 1).unwrap());

        let published_attr = engine.get_attr(file.ino).unwrap();
        assert_eq!(published_attr.committed_size, 8192);
        assert_eq!(published_attr.committed_content_epoch, 1);
        assert_eq!(published_attr.upload_state, UploadState::Ready);

        let snapshot = engine.export_remote_snapshot().unwrap();
        let remote = temp_engine();
        remote.import_snapshot(&snapshot).unwrap();
        let remote_attr = remote.get_attr(file.ino).unwrap();
        assert_eq!(remote_attr.size, 8192);
        assert_eq!(remote_attr.committed_size, 8192);
        assert_eq!(remote_attr.committed_content_epoch, 1);
        assert_eq!(remote_attr.upload_state, UploadState::Ready);

        let remote_blocks = remote.get_file_blocks(file.ino).unwrap();
        assert_eq!(remote_blocks, vec![(0, block0), (1, block1)]);
    }

    #[test]
    fn redb_upload_state_transitions_follow_inode_fields() {
        let engine = temp_engine();

        let file = engine
            .create_file(ROOT_INO, b"upload-state.bin", 1000, 1000, 0o644)
            .unwrap();
        assert_eq!(
            engine.get_attr(file.ino).unwrap().upload_state,
            UploadState::Ready
        );

        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 4096;
        attr.content_epoch = 1;
        engine.set_attr(&attr).unwrap();
        engine
            .set_file_block(file.ino, 0, &BlockRef::new_partial(201, 4096))
            .unwrap();
        let uploading = engine.get_attr(file.ino).unwrap();
        assert_eq!(uploading.upload_state, UploadState::Uploading);
        assert!(engine.publish_file_manifest(file.ino, 1).unwrap());
        let ready = engine.get_attr(file.ino).unwrap();
        assert_eq!(ready.upload_state, UploadState::Ready);
        assert_eq!(ready.committed_content_epoch, 1);

        let mut update_attr = ready.clone();
        update_attr.content_epoch = 2;
        engine.set_attr(&update_attr).unwrap();
        engine
            .set_file_block(file.ino, 0, &BlockRef::new_partial(202, 4096))
            .unwrap();
        let updating = engine.get_attr(file.ino).unwrap();
        assert_eq!(updating.upload_state, UploadState::Updating);
        assert_eq!(updating.committed_content_epoch, 1);
    }

    #[test]
    fn redb_partial_update_does_not_expose_new_blocks_before_publish() {
        let engine = temp_engine();

        let file = engine
            .create_file(ROOT_INO, b"partial-update.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 4096;
        attr.content_epoch = 1;
        engine.set_attr(&attr).unwrap();
        let committed = BlockRef::new_partial(301, 4096);
        engine.set_file_block(file.ino, 0, &committed).unwrap();
        assert!(engine.publish_file_manifest(file.ino, 1).unwrap());

        let mut update_attr = engine.get_attr(file.ino).unwrap();
        update_attr.content_epoch = 2;
        engine.set_attr(&update_attr).unwrap();
        let staged = BlockRef::new_partial(302, 4096);
        engine.set_file_block(file.ino, 0, &staged).unwrap();

        let local = engine.get_file_block(file.ino, 0).unwrap().unwrap();
        assert_eq!(local.object_id, 302);

        let snapshot = engine.export_remote_snapshot().unwrap();
        let remote = temp_engine();
        remote.import_snapshot(&snapshot).unwrap();

        let remote_attr = remote.get_attr(file.ino).unwrap();
        assert_eq!(remote_attr.size, 4096);
        assert_eq!(remote_attr.committed_content_epoch, 1);
        assert_eq!(remote_attr.upload_state, UploadState::Updating);
        let remote_block = remote.get_file_block(file.ino, 0).unwrap().unwrap();
        assert_eq!(remote_block.object_id, 301);
    }

    #[test]
    fn redb_publish_file_manifest_if_matches_rejects_manifest_changes() {
        let engine = temp_engine();

        let file = engine
            .create_file(ROOT_INO, b"publish-race.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 8192;
        attr.content_epoch = 1;
        engine.set_attr(&attr).unwrap();

        let block0 = BlockRef::new_partial(501, 4096);
        engine.set_file_block(file.ino, 0, &block0).unwrap();
        let verified_manifest = engine.get_file_blocks(file.ino).unwrap();

        let block1 = BlockRef::new_partial(502, 4096);
        engine.set_file_block(file.ino, 1, &block1).unwrap();

        assert!(
            !engine
                .publish_file_manifest_if_matches(file.ino, 1, &verified_manifest)
                .unwrap()
        );

        let attr_after = engine.get_attr(file.ino).unwrap();
        assert_eq!(attr_after.upload_state, UploadState::Uploading);
        assert_eq!(attr_after.committed_content_epoch, 0);
    }

    #[test]
    fn redb_rename_keeps_same_inode_and_incarnation_ids() {
        let engine = temp_engine();

        let file = engine
            .create_file(ROOT_INO, b"rename-inc.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 4096;
        attr.content_epoch = 7;
        engine.set_attr(&attr).unwrap();
        engine
            .set_file_block(file.ino, 0, &BlockRef::new_partial(401, 4096))
            .unwrap();
        assert!(engine.publish_file_manifest(file.ino, 7).unwrap());

        engine
            .rename_atomic(
                ROOT_INO,
                b"rename-inc.bin",
                ROOT_INO,
                b"rename-inc-next.bin",
            )
            .unwrap();

        let renamed = engine.lookup(ROOT_INO, b"rename-inc-next.bin").unwrap();
        assert_eq!(renamed.ino, file.ino);
        let renamed_attr = engine.get_attr(renamed.ino).unwrap();
        assert_eq!(renamed_attr.content_epoch, 7);
        assert_eq!(renamed_attr.committed_content_epoch, 7);
        assert_eq!(renamed_attr.upload_state, UploadState::Ready);
    }

    #[test]
    fn test_next_block_id() {
        let engine = temp_engine();

        let id1 = engine.next_block_id().unwrap();
        let id2 = engine.next_block_id().unwrap();
        let id3 = engine.next_block_id().unwrap();

        assert_eq!(id2, id1 + 1);
        assert_eq!(id3, id2 + 1);
    }

    // === Symlink Tests ===

    #[test]
    fn test_create_symlink() {
        let engine = temp_engine();

        let attr = engine
            .create_symlink(ROOT_INO, b"link1", b"/tmp/target", 1000, 1000)
            .unwrap();
        assert_eq!(attr.kind, FileType::Symlink);
        assert_eq!(attr.mode, 0o777);
        assert_eq!(attr.size, 11); // length of "/tmp/target"
        assert_eq!(attr.uid, 1000);
        assert_eq!(attr.gid, 1000);

        // Verify target is stored
        let target = engine.get_symlink_target(attr.ino).unwrap();
        assert_eq!(&target, b"/tmp/target");

        // Verify dentry exists
        let looked_up = engine.lookup(ROOT_INO, b"link1").unwrap();
        assert_eq!(looked_up.ino, attr.ino);
        assert_eq!(looked_up.kind, FileType::Symlink);
    }

    #[test]
    fn test_symlink_falls_back_to_base_when_no_local_overlay() {
        let source = temp_engine();
        let link = source
            .create_symlink(ROOT_INO, b"base-link", b"/base/target", 0, 0)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();

        let target = engine.get_symlink_target(link.ino).unwrap();
        assert_eq!(target, b"/base/target");
    }

    #[test]
    fn test_local_symlink_overlay_wins_over_base() {
        let source = temp_engine();
        let link = source
            .create_symlink(ROOT_INO, b"base-link", b"/base/target", 0, 0)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        engine
            .set_symlink_target(link.ino, b"/local/target")
            .unwrap();

        let target = engine.get_symlink_target(link.ino).unwrap();
        assert_eq!(target, b"/local/target");
    }

    #[test]
    fn test_remove_attr_base_symlink_creates_local_tombstone() {
        let source = temp_engine();
        let link = source
            .create_symlink(ROOT_INO, b"base-link", b"/base/target", 0, 0)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        engine.remove_attr(link.ino).unwrap();

        assert!(engine.get_symlink_target(link.ino).is_err());
        assert!(
            engine
                .local_overlay_symlink_inodes()
                .unwrap()
                .contains(&link.ino)
        );
    }

    #[test]
    fn test_symlink_target_stored_and_retrieved() {
        let engine = temp_engine();

        let attr = engine
            .create_symlink(ROOT_INO, b"mylink", b"relative/path/to/file", 0, 0)
            .unwrap();

        let target = engine.get_symlink_target(attr.ino).unwrap();
        assert_eq!(&target, b"relative/path/to/file");
    }

    #[test]
    fn test_symlink_remove_cascades() {
        let engine = temp_engine();

        let attr = engine
            .create_symlink(ROOT_INO, b"cascade_link", b"/target", 0, 0)
            .unwrap();
        let ino = attr.ino;

        // Verify symlink target exists
        assert!(engine.get_symlink_target(ino).is_ok());

        // Remove the dentry and attr
        engine.remove_entry(ROOT_INO, b"cascade_link").unwrap();
        engine.remove_attr(ino).unwrap();

        // Symlink target should be cleaned up by remove_attr
        assert!(engine.get_symlink_target(ino).is_err());
    }

    #[test]
    fn test_symlink_duplicate_name_fails() {
        let engine = temp_engine();

        engine
            .create_file(ROOT_INO, b"existing", 0, 0, 0o644)
            .unwrap();

        let result = engine.create_symlink(ROOT_INO, b"existing", b"/target", 0, 0);
        assert!(result.is_err());
    }

    // === Hardlink Tests ===

    #[test]
    fn test_link_basic() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"original.txt", 1000, 1000, 0o644)
            .unwrap();
        let ino = attr.ino;
        assert_eq!(attr.nlink, 1);

        // Create hardlink
        let linked = engine.link(ino, ROOT_INO, b"hardlink.txt").unwrap();
        assert_eq!(linked.ino, ino);
        assert_eq!(linked.nlink, 2);

        // Both names should resolve to the same inode
        let entry1 = engine.lookup(ROOT_INO, b"original.txt").unwrap();
        let entry2 = engine.lookup(ROOT_INO, b"hardlink.txt").unwrap();
        assert_eq!(entry1.ino, ino);
        assert_eq!(entry2.ino, ino);
    }

    #[test]
    fn test_link_directory_fails() {
        let engine = temp_engine();

        let dir = engine.create_dir(ROOT_INO, b"mydir", 0, 0, 0o755).unwrap();

        let result = engine.link(dir.ino, ROOT_INO, b"dir_link");
        assert!(result.is_err()); // EPERM: can't hardlink directories
    }

    #[test]
    fn test_link_duplicate_name_fails() {
        let engine = temp_engine();

        let attr = engine
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .unwrap();
        engine
            .create_file(ROOT_INO, b"taken.txt", 0, 0, 0o644)
            .unwrap();

        let result = engine.link(attr.ino, ROOT_INO, b"taken.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_link_unlink_nlink_tracking() {
        let engine = temp_engine();

        let attr = engine.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644).unwrap();
        let ino = attr.ino;

        // Create two hardlinks: nlink should be 3
        engine.link(ino, ROOT_INO, b"b.txt").unwrap();
        let linked = engine.link(ino, ROOT_INO, b"c.txt").unwrap();
        assert_eq!(linked.nlink, 3);

        // Unlink one: nlink=2, inode still exists
        let unlinked = engine.unlink(ROOT_INO, b"a.txt").unwrap();
        assert_eq!(unlinked.nlink, 2);
        assert!(engine.get_attr(ino).is_ok());

        // Unlink another: nlink=1, still exists
        let unlinked2 = engine.unlink(ROOT_INO, b"b.txt").unwrap();
        assert_eq!(unlinked2.nlink, 1);
        assert!(engine.get_attr(ino).is_ok());

        // Unlink last: nlink=0, inode removed
        let unlinked3 = engine.unlink(ROOT_INO, b"c.txt").unwrap();
        assert_eq!(unlinked3.nlink, 0);
        assert!(engine.get_attr(ino).is_err());
    }

    // === xattr Tests ===

    #[test]
    fn test_xattr_set_get() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .unwrap();

        // Set xattr
        engine
            .set_xattr(attr.ino, b"user.color", b"red", 0)
            .unwrap();

        // Get xattr
        let val = engine.get_xattr(attr.ino, b"user.color").unwrap();
        assert_eq!(val, Some(b"red".to_vec()));

        // Non-existent xattr returns None
        let none = engine.get_xattr(attr.ino, b"user.missing").unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn test_resource_fork_xattr_compresses_when_beneficial() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"resource-fork.bin", 0, 0, 0o644)
            .unwrap();

        let value = vec![0x41; 64 * 1024];
        engine
            .set_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME, value.as_slice(), 0)
            .unwrap();

        let txn = engine.db.begin_read().unwrap();
        let table = txn.open_table(XATTRS_TABLE).unwrap();
        let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
        let stored = table
            .get(key.as_slice())
            .unwrap()
            .expect("stored resource fork xattr")
            .value()
            .to_vec();

        assert!(
            stored.len() < value.len(),
            "expected compressed storage to reduce metadata bytes"
        );
        assert_eq!(
            engine
                .get_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME)
                .unwrap(),
            Some(value)
        );
    }

    #[test]
    fn test_resource_fork_xattr_keeps_plain_when_not_compressible() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"resource-fork-random.bin", 0, 0, 0o644)
            .unwrap();

        let value: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        engine
            .set_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME, value.as_slice(), 0)
            .unwrap();

        let txn = engine.db.begin_read().unwrap();
        let table = txn.open_table(XATTRS_TABLE).unwrap();
        let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
        let stored = table
            .get(key.as_slice())
            .unwrap()
            .expect("stored resource fork xattr")
            .value()
            .to_vec();

        assert_eq!(
            stored, value,
            "non-compressible resource fork payload should stay plain"
        );
    }

    #[test]
    fn test_resource_fork_legacy_plain_storage_is_compatible() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"legacy-rfork.bin", 0, 0, 0o644)
            .unwrap();
        let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
        let legacy_plain = b"legacy-plain-resource-fork".to_vec();

        let txn = engine.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(XATTRS_TABLE).unwrap();
            table
                .insert(key.as_slice(), legacy_plain.as_slice())
                .unwrap();
        }
        txn.commit().unwrap();

        assert_eq!(
            engine
                .get_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME)
                .unwrap(),
            Some(legacy_plain)
        );
        assert_eq!(
            engine.list_xattrs(attr.ino).unwrap(),
            vec![RESOURCE_FORK_XATTR_NAME.to_vec()]
        );
    }

    #[test]
    fn test_resource_fork_snapshot_exports_plain_value() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"snapshot-rfork.bin", 0, 0, 0o644)
            .unwrap();
        let value = vec![0x33; 8192];
        engine
            .set_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME, value.as_slice(), 0)
            .unwrap();

        let snapshot = engine.export_snapshot().unwrap();
        let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
        let snap_value = snapshot
            .xattrs
            .iter()
            .find_map(|(k, v)| if k == &key { Some(v.clone()) } else { None })
            .expect("snapshot xattr entry");

        assert_eq!(snap_value, value);
    }

    #[test]
    fn test_resource_fork_decode_handles_poisoned_declared_length() {
        let mut framed = Vec::new();
        framed.extend_from_slice(RESOURCE_FORK_ENCODING_MAGIC);
        framed.push(RESOURCE_FORK_ENCODING_VERSION);
        framed.push(RESOURCE_FORK_ENCODING_SCHEME_RLE);
        framed.extend_from_slice(&u32::MAX.to_le_bytes());
        framed.extend_from_slice(&0u32.to_le_bytes());
        framed.push(0); // 1-byte literal run marker, but payload is intentionally incomplete

        let decoded = decode_resource_fork_value_from_storage(framed.as_slice());
        assert_eq!(decoded, framed);
    }

    #[test]
    fn test_xattr_falls_back_to_base_when_no_local_overlay() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"base-xattr.txt", 0, 0, 0o644)
            .unwrap();
        source
            .set_xattr(file.ino, b"user.color", b"red", 0)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();

        assert_eq!(
            engine.get_xattr(file.ino, b"user.color").unwrap(),
            Some(b"red".to_vec())
        );
        assert_eq!(
            engine.list_xattrs(file.ino).unwrap(),
            vec![b"user.color".to_vec()]
        );
    }

    #[test]
    fn test_local_xattr_overlay_wins_over_base() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"base-xattr.txt", 0, 0, 0o644)
            .unwrap();
        source
            .set_xattr(file.ino, b"user.color", b"red", 0)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        engine
            .set_xattr(file.ino, b"user.color", b"blue", 0)
            .unwrap();

        assert_eq!(
            engine.get_xattr(file.ino, b"user.color").unwrap(),
            Some(b"blue".to_vec())
        );
    }

    #[test]
    fn test_remove_base_xattr_creates_local_tombstone() {
        let source = temp_engine();
        let file = source
            .create_file(ROOT_INO, b"base-xattr.txt", 0, 0, 0o644)
            .unwrap();
        source
            .set_xattr(file.ino, b"user.color", b"red", 0)
            .unwrap();
        let snapshot = source.export_snapshot().unwrap();

        let engine = temp_engine();
        engine.import_snapshot(&snapshot).unwrap();
        engine.remove_xattr(file.ino, b"user.color").unwrap();

        assert_eq!(engine.get_xattr(file.ino, b"user.color").unwrap(), None);
        assert!(engine.list_xattrs(file.ino).unwrap().is_empty());
        assert!(
            engine
                .local_overlay_xattr_keys()
                .unwrap()
                .contains(&encode_xattr_key(file.ino, b"user.color"))
        );
    }

    #[test]
    fn test_xattr_overwrite() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .unwrap();

        engine.set_xattr(attr.ino, b"user.tag", b"v1", 0).unwrap();
        engine.set_xattr(attr.ino, b"user.tag", b"v2", 0).unwrap();

        let val = engine.get_xattr(attr.ino, b"user.tag").unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_xattr_create_flag() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .unwrap();

        // XATTR_CREATE on new key succeeds
        engine.set_xattr(attr.ino, b"user.x", b"a", 1).unwrap();

        // XATTR_CREATE on existing key fails
        let result = engine.set_xattr(attr.ino, b"user.x", b"b", 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_xattr_replace_flag() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .unwrap();

        // XATTR_REPLACE on non-existent key fails
        let result = engine.set_xattr(attr.ino, b"user.y", b"a", 2);
        assert!(result.is_err());

        // Set it first, then REPLACE succeeds
        engine.set_xattr(attr.ino, b"user.y", b"a", 0).unwrap();
        engine.set_xattr(attr.ino, b"user.y", b"b", 2).unwrap();
        let val = engine.get_xattr(attr.ino, b"user.y").unwrap();
        assert_eq!(val, Some(b"b".to_vec()));
    }

    #[test]
    fn test_xattr_remove() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .unwrap();

        engine.set_xattr(attr.ino, b"user.rm", b"data", 0).unwrap();
        engine.remove_xattr(attr.ino, b"user.rm").unwrap();

        let val = engine.get_xattr(attr.ino, b"user.rm").unwrap();
        assert!(val.is_none());

        // Remove non-existent xattr fails
        let result = engine.remove_xattr(attr.ino, b"user.rm");
        assert!(result.is_err());
    }

    #[test]
    fn test_xattr_list() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .unwrap();

        engine.set_xattr(attr.ino, b"user.alpha", b"1", 0).unwrap();
        engine.set_xattr(attr.ino, b"user.beta", b"2", 0).unwrap();
        engine.set_xattr(attr.ino, b"user.gamma", b"3", 0).unwrap();

        let names = engine.list_xattrs(attr.ino).unwrap();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&b"user.alpha".to_vec()));
        assert!(names.contains(&b"user.beta".to_vec()));
        assert!(names.contains(&b"user.gamma".to_vec()));
    }

    #[test]
    fn test_xattr_cascade_on_remove_attr() {
        let engine = temp_engine();
        let attr = engine
            .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
            .unwrap();

        engine
            .set_xattr(attr.ino, b"user.keep", b"data", 0)
            .unwrap();

        // Remove the inode — xattrs should be cascade-deleted
        engine.unlink(ROOT_INO, b"file.txt").unwrap();

        // xattr should be gone
        let names = engine.list_xattrs(attr.ino).unwrap();
        assert!(names.is_empty());
    }

    #[test]
    fn test_xattr_isolation_between_inodes() {
        let engine = temp_engine();
        let f1 = engine.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644).unwrap();
        let f2 = engine.create_file(ROOT_INO, b"b.txt", 0, 0, 0o644).unwrap();

        engine
            .set_xattr(f1.ino, b"user.owner", b"alice", 0)
            .unwrap();
        engine.set_xattr(f2.ino, b"user.owner", b"bob", 0).unwrap();

        assert_eq!(
            engine.get_xattr(f1.ino, b"user.owner").unwrap(),
            Some(b"alice".to_vec())
        );
        assert_eq!(
            engine.get_xattr(f2.ino, b"user.owner").unwrap(),
            Some(b"bob".to_vec())
        );

        // list_xattrs should only show each inode's own xattrs
        assert_eq!(engine.list_xattrs(f1.ino).unwrap().len(), 1);
        assert_eq!(engine.list_xattrs(f2.ino).unwrap().len(), 1);
    }

    #[test]
    fn test_decr_counter() {
        let engine = temp_engine();
        engine.set_counter("test_space", 1000).unwrap();
        let v = engine.decr_counter("test_space", 300).unwrap();
        assert_eq!(v, 700);
        let v = engine.decr_counter("test_space", 700).unwrap();
        assert_eq!(v, 0);
        // Saturating subtraction — doesn't go below 0
        let v = engine.decr_counter("test_space", 1).unwrap();
        assert_eq!(v, 0);
    }

    #[test]
    fn test_decr_counter_unset() {
        let engine = temp_engine();
        // Decrementing an unset counter (defaults to 0) saturates at 0
        let v = engine.decr_counter("noexist", 42).unwrap();
        assert_eq!(v, 0);
    }

    #[test]
    fn test_acl_rule_set_get_remove_roundtrip() {
        let engine = temp_engine();
        let secret = engine.create_dir(ROOT_INO, b"secret", 0, 0, 0o755).unwrap();

        assert_eq!(engine.get_acl_rule(secret.ino, "alice").unwrap(), None);
        engine
            .set_acl_rule(secret.ino, "alice", AccessLevel::ReadOnly)
            .unwrap();
        assert_eq!(
            engine.get_acl_rule(secret.ino, "alice").unwrap(),
            Some(AccessLevel::ReadOnly)
        );

        engine
            .set_acl_rule(secret.ino, "alice", AccessLevel::NoAccess)
            .unwrap();
        assert_eq!(
            engine.get_acl_rule(secret.ino, "alice").unwrap(),
            Some(AccessLevel::NoAccess)
        );

        engine.remove_acl_rule(secret.ino, "alice").unwrap();
        assert_eq!(engine.get_acl_rule(secret.ino, "alice").unwrap(), None);
    }

    #[test]
    fn test_acl_rules_cascade_on_remove_attr() {
        let engine = temp_engine();
        let dir = engine.create_dir(ROOT_INO, b"secret", 0, 0, 0o755).unwrap();
        engine
            .set_acl_rule(dir.ino, "bob", AccessLevel::ReadWrite)
            .unwrap();
        assert_eq!(
            engine.get_acl_rule(dir.ino, "bob").unwrap(),
            Some(AccessLevel::ReadWrite)
        );

        engine.rmdir(ROOT_INO, b"secret").unwrap();
        assert_eq!(engine.get_acl_rule(dir.ino, "bob").unwrap(), None);
    }

    #[test]
    fn test_migrate_regular_files_from_v2_to_v3() {
        use crate::schema::DEFAULT_BLOCK_SIZE;

        let engine = temp_engine();

        // Set up v2 format info to simulate a pre-migration database
        let v2_format_info = FormatInfo {
            format_version: 2, // v2 format
            name: "test-vol".into(),
            uuid: "abc-123".into(),
            endpoint: "http://localhost:9000".into(),
            bucket: "testbucket".into(),
            access_key: "studiofs".into(),
            secret_key: "studiofs123".into(),
            region: "".into(),
            data_prefix: "test-volume/objects/".into(),
            metadata: MetadataConfig::default(),
            block_size: DEFAULT_BLOCK_SIZE,
        };
        engine.set_format_info(&v2_format_info).unwrap();

        // Create a file with some blocks in the old format
        let file_attr = engine
            .create_file(ROOT_INO, b"test_migrate.txt", 0, 0, 0o644)
            .unwrap();

        // Simulate old v2 block storage by directly setting FILE_BLOCKS entries
        let block1 = BlockRef::new_full(1001, DEFAULT_BLOCK_SIZE);
        let block2 = BlockRef::new_partial(1002, DEFAULT_BLOCK_SIZE / 2);

        engine.set_file_block(file_attr.ino, 0, &block1).unwrap();
        engine.set_file_block(file_attr.ino, 1, &block2).unwrap();

        // Verify old blocks are stored
        let old_blocks = engine.get_file_blocks(file_attr.ino).unwrap();
        assert_eq!(old_blocks.len(), 2);
        assert_eq!(old_blocks[0].1.object_id, 1001);
        assert_eq!(old_blocks[1].1.object_id, 1002);

        // Run migration
        engine.migrate_regular_files_to_chunk_slice_model().unwrap();

        // Verify legacy blocks are preserved while the chunk/slice tables are backfilled
        let old_blocks_after = engine.get_file_blocks(file_attr.ino).unwrap();
        assert_eq!(old_blocks_after, old_blocks);

        // Verify new chunk/slice data exists
        let txn = engine.db.begin_write().unwrap();
        let has_overlay = RedbMetaEngine::has_regular_file_chunk_rows_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            file_attr.ino,
        )
        .unwrap();
        assert!(has_overlay);

        let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            file_attr.ino,
        )
        .unwrap();

        // Should have one chunk (both blocks fit in DEFAULT_CHUNK_SIZE)
        assert_eq!(overlay_chunks.len(), 1);
        let chunk_state = overlay_chunks.get(&ChunkId(0)).unwrap();
        assert_eq!(chunk_state.slices.len(), 2);

        // Verify slice data
        let slice_ids: Vec<_> = chunk_state.slices.keys().collect();
        let slice1 = &chunk_state.slices[slice_ids[0]];
        let slice2 = &chunk_state.slices[slice_ids[1]];

        // Check that blocks are correctly mapped
        // Note: slice_ids order depends on BTreeMap ordering, not insertion order
        let block1_ref = slice1.block.as_ref().unwrap();
        let block2_ref = slice2.block.as_ref().unwrap();

        assert_eq!(block1_ref.object_id, 1001);
        assert_eq!(block2_ref.object_id, 1002);
        assert!(!block1_ref.is_partial()); // block1 was new_full
        assert!(block2_ref.is_partial()); // block2 was new_partial
    }

    #[test]
    fn test_migrate_multiple_files_with_different_chunks() {
        use crate::schema::DEFAULT_BLOCK_SIZE;

        let engine = temp_engine();

        // Set up v2 format info to simulate a pre-migration database
        let v2_format_info = FormatInfo {
            format_version: 2,
            name: "test-vol".into(),
            uuid: "abc-123".into(),
            endpoint: "http://localhost:9000".into(),
            bucket: "testbucket".into(),
            access_key: "studiofs".into(),
            secret_key: "studiofs123".into(),
            region: "".into(),
            data_prefix: "test-volume/objects/".into(),
            metadata: MetadataConfig::default(),
            block_size: DEFAULT_BLOCK_SIZE,
        };
        engine.set_format_info(&v2_format_info).unwrap();

        // Create two files
        let file1_attr = engine
            .create_file(ROOT_INO, b"file1.txt", 0, 0, 0o644)
            .unwrap();
        let file2_attr = engine
            .create_file(ROOT_INO, b"file2.txt", 0, 0, 0o644)
            .unwrap();

        // Add blocks to file1 (single chunk)
        let block1 = BlockRef::new_full(2001, DEFAULT_BLOCK_SIZE);
        engine.set_file_block(file1_attr.ino, 0, &block1).unwrap();

        // Add blocks to file2 (will span multiple chunks in new model)
        let block2 = BlockRef::new_full(2002, DEFAULT_BLOCK_SIZE);
        let block3 = BlockRef::new_full(2003, DEFAULT_BLOCK_SIZE);
        engine.set_file_block(file2_attr.ino, 0, &block2).unwrap();
        engine.set_file_block(file2_attr.ino, 1, &block3).unwrap();

        // Run migration
        engine.migrate_regular_files_to_chunk_slice_model().unwrap();

        // Verify both files migrated correctly
        let txn = engine.db.begin_write().unwrap();

        // File 1: single chunk
        let file1_overlay = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            file1_attr.ino,
        )
        .unwrap();
        assert_eq!(file1_overlay.len(), 1);

        // File 2: should also be single chunk since blocks are contiguous
        // Migration backfills the overlay rows without removing legacy blocks.
        let file2_overlay = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            file2_attr.ino,
        )
        .unwrap();
        assert_eq!(file2_overlay.len(), 1);
        let chunk_state = file2_overlay.get(&ChunkId(0)).unwrap();
        assert_eq!(chunk_state.slices.len(), 2);
    }

    #[test]
    fn test_migration_validation() {
        use crate::schema::DEFAULT_BLOCK_SIZE;

        let engine = temp_engine();

        // Set up v2 format info to simulate a pre-migration database
        let v2_format_info = FormatInfo {
            format_version: 2,
            name: "test-vol".into(),
            uuid: "abc-123".into(),
            endpoint: "http://localhost:9000".into(),
            bucket: "testbucket".into(),
            access_key: "studiofs".into(),
            secret_key: "studiofs123".into(),
            region: "".into(),
            data_prefix: "test-volume/objects/".into(),
            metadata: MetadataConfig::default(),
            block_size: DEFAULT_BLOCK_SIZE,
        };
        engine.set_format_info(&v2_format_info).unwrap();

        // Create a file with blocks
        let file_attr = engine
            .create_file(ROOT_INO, b"validate.txt", 0, 0, 0o644)
            .unwrap();
        let block = BlockRef::new_full(3001, 1024);
        engine.set_file_block(file_attr.ino, 0, &block).unwrap();

        // Run migration
        engine.migrate_regular_files_to_chunk_slice_model().unwrap();

        // Validation should pass
        engine.validate_migration_completed().unwrap();

        // Downgrade the upgrade marker to simulate an incomplete migration.
        let txn = engine.db.begin_write().unwrap();
        {
            let mut migration_status = txn.open_table(MIGRATION_STATUS_TABLE).unwrap();
            migration_status
                .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_STARTED)
                .unwrap();
        }
        txn.commit().unwrap();

        // Validation should now fail
        assert!(engine.validate_migration_completed().is_err());
    }

    #[test]
    fn test_migration_resume_after_interruption() {
        use crate::schema::DEFAULT_BLOCK_SIZE;

        let engine = temp_engine();

        let v2_format_info = FormatInfo {
            format_version: 2,
            name: "test-vol".into(),
            uuid: "abc-123".into(),
            endpoint: "http://localhost:9000".into(),
            bucket: "testbucket".into(),
            access_key: "studiofs".into(),
            secret_key: "studiofs123".into(),
            region: "".into(),
            data_prefix: "test-volume/objects/".into(),
            metadata: MetadataConfig::default(),
            block_size: DEFAULT_BLOCK_SIZE,
        };
        engine.set_format_info(&v2_format_info).unwrap();

        let file1_attr = engine
            .create_file(ROOT_INO, b"resume1.txt", 0, 0, 0o644)
            .unwrap();
        let file2_attr = engine
            .create_file(ROOT_INO, b"resume2.txt", 0, 0, 0o644)
            .unwrap();

        let block1 = BlockRef::new_full(4001, DEFAULT_BLOCK_SIZE);
        let block2 = BlockRef::new_full(4002, DEFAULT_BLOCK_SIZE);
        engine.set_file_block(file1_attr.ino, 0, &block1).unwrap();
        engine.set_file_block(file2_attr.ino, 0, &block2).unwrap();

        let txn = engine.db.begin_write().unwrap();
        {
            let mut migration_status = txn.open_table(MIGRATION_STATUS_TABLE).unwrap();
            migration_status
                .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_STARTED)
                .unwrap();
        }
        txn.commit().unwrap();

        engine.migrate_regular_files_to_chunk_slice_model().unwrap();

        let file1_blocks = engine.get_file_blocks(file1_attr.ino).unwrap();
        let file2_blocks = engine.get_file_blocks(file2_attr.ino).unwrap();
        assert_eq!(file1_blocks.len(), 1);
        assert_eq!(file2_blocks.len(), 1);
        assert_eq!(file1_blocks[0].1.object_id, 4001);
        assert_eq!(file2_blocks[0].1.object_id, 4002);

        engine.validate_migration_completed().unwrap();

        let txn = engine.db.begin_write().unwrap();
        let file1_overlay = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            file1_attr.ino,
        )
        .unwrap();
        let file2_overlay = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            file2_attr.ino,
        )
        .unwrap();
        txn.abort().unwrap();

        assert_eq!(file1_overlay.len(), 1);
        assert_eq!(file2_overlay.len(), 1);

        let file1_chunk = file1_overlay.get(&ChunkId(0)).unwrap();
        assert_eq!(file1_chunk.slices.len(), 1);
        let slice = file1_chunk.slices.iter().next().unwrap().1;
        assert_eq!(slice.block.as_ref().unwrap().object_id, 4001);

        let file2_chunk = file2_overlay.get(&ChunkId(0)).unwrap();
        assert_eq!(file2_chunk.slices.len(), 1);
        let slice = file2_chunk.slices.iter().next().unwrap().1;
        assert_eq!(slice.block.as_ref().unwrap().object_id, 4002);
    }
}

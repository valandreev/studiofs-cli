//! redb implementation of MetaEngine.
//!
//! Uses redb as an embedded ACID KV store for all filesystem metadata.

use crate::engine::*;
use crate::types::*;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tracing::debug;

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

/// Pending Deletions (GC): object_id (u64) -> deletion_timestamp (i64)
const PENDING_DELETIONS_TABLE: TableDefinition<u64, i64> =
    TableDefinition::new("pending_deletions");

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

/// Event Log: sequence_id (u64) -> MetaEvent (postcard)
const EVENT_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("event_log");

/// Serializable full snapshot of redb metadata state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct RedbSnapshot {
    pub settings: Vec<(String, Vec<u8>)>,
    pub counters: Vec<(String, u64)>,
    pub inodes: Vec<(u64, Vec<u8>)>,
    pub dentries: Vec<(Vec<u8>, Vec<u8>)>,
    pub file_blocks: Vec<(Vec<u8>, Vec<u8>)>,
    pub committed_file_blocks: Vec<(Vec<u8>, Vec<u8>)>,
    pub pending_deletions: Vec<(u64, i64)>,
    pub symlinks: Vec<(u64, Vec<u8>)>,
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    pub event_log: Vec<(u64, Vec<u8>)>,
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
            let _ = txn.open_table(PENDING_DELETIONS_TABLE);
            let _ = txn.open_table(SYMLINKS_TABLE);
            let _ = txn.open_table(BASE_SYMLINKS_TABLE);
            let _ = txn.open_table(SYMLINK_TOMBSTONES_TABLE);
            let _ = txn.open_table(XATTRS_TABLE);
            let _ = txn.open_table(BASE_XATTRS_TABLE);
            let _ = txn.open_table(XATTR_TOMBSTONES_TABLE);
            let _ = txn.open_table(EVENT_LOG_TABLE);
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        debug!("opened metadata database at {:?}", path.as_ref());
        Ok(Self { db: Arc::new(db) })
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

        Ok(overlay)
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
            return Ok(Some(value));
        }
        Self::get_bytes_from_table_slice(txn, BASE_XATTRS_TABLE, key.as_slice())
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

        let mut xattrs = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        xattrs
            .insert(key.as_slice(), value)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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

        let mut xattrs = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        xattrs
            .insert(key.as_slice(), value)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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

        Ok(())
    }

    fn compute_committed_size_in_txn(
        committed: &redb::Table<'_, &[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<u64> {
        let prefix = file_block_prefix(ino);
        let mut range_end = prefix.to_vec();
        *range_end.last_mut().unwrap() += 1;
        let mut size = 0u64;
        let mut expected = 0u32;
        for row in committed
            .range(prefix.as_slice()..range_end.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (decoded_ino, block_index) = decode_file_block_key(key.value());
            if decoded_ino != ino || block_index != expected {
                break;
            }
            let block: BlockRef = decode(value.value())?;
            size = size.saturating_add(block.length as u64);
            expected = expected.saturating_add(1);
            if block.is_partial() {
                break;
            }
        }
        Ok(size)
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
                merged.insert(k.value().to_vec(), v.value().to_vec());
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
                merged.insert(k.value().to_vec(), v.value().to_vec());
            }
            snapshot.xattrs.extend(merged);
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

        let committed_used_space =
            snapshot
                .committed_file_blocks
                .iter()
                .try_fold(0_u64, |acc, (_, bytes)| {
                    let block: BlockRef = decode(bytes)?;
                    Ok::<u64, MetaError>(acc.saturating_add(block.length as u64))
                })?;

        snapshot.file_blocks = snapshot.committed_file_blocks.clone();
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
                .is_some();

        if !has_base_metadata {
            return Ok(false);
        }

        Ok(!self.local_overlay_attr_inodes()?.is_empty()
            || !self.local_overlay_dentries()?.is_empty()
            || !self.local_overlay_symlink_inodes()?.is_empty()
            || !self.local_overlay_xattr_keys()?.is_empty())
    }

    /// Replace redb state with a provided snapshot.
    pub fn import_snapshot(&self, snapshot: &RedbSnapshot) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        {
            let mut table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<String> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_str());
            }
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
            let keys: Vec<String> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_str());
            }
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
            let keys: Vec<u64> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
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
            let keys: Vec<u64> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
            for (k, v) in &snapshot.inodes {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(INODE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<u64> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
        }
        {
            let mut table = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
            for (k, v) in &snapshot.dentries {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(DENTRY_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
        }
        {
            let mut table = txn
                .open_table(BASE_DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
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
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
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
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
            for (k, v) in &snapshot.committed_file_blocks {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<u64> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
            for (k, v) in &snapshot.pending_deletions {
                table
                    .insert(*k, *v)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<u64> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
        }
        {
            let mut table = txn
                .open_table(BASE_SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<u64> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
            for (k, v) in &snapshot.symlinks {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(SYMLINK_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<u64> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
        }
        {
            let mut table = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
        }
        {
            let mut table = txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
            for (k, v) in &snapshot.xattrs {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(XATTR_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<Vec<u8>> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                let _ = table.remove(key.as_slice());
            }
        }
        {
            let mut table = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let keys: Vec<u64> = table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                let _ = table.remove(key);
            }
            for (k, v) in &snapshot.event_log {
                table
                    .insert(*k, v.as_slice())
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
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    table
                        .insert(key.as_slice(), value.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
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
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    table
                        .insert(key.as_slice(), value.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                crate::events::MetaEvent::RemoveXattr { ino, name } => {
                    let mut table = txn
                        .open_table(BASE_XATTRS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let key = encode_xattr_key(*ino, name);
                    let _ = table.remove(key.as_slice());
                }
                crate::events::MetaEvent::SetFileBlock { .. }
                | crate::events::MetaEvent::RemoveFileBlock { .. }
                | crate::events::MetaEvent::RemoveFileBlocks { .. }
                | crate::events::MetaEvent::SetFileBlocksBatch { .. }
                | crate::events::MetaEvent::CommitFileBlock { .. }
                | crate::events::MetaEvent::PublishFileManifest { .. }
                | crate::events::MetaEvent::AddPendingDeletion { .. }
                | crate::events::MetaEvent::RemovePendingDeletion { .. } => {}
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
                crate::events::MetaEvent::SetFileBlock { .. }
                | crate::events::MetaEvent::RemoveFileBlock { .. }
                | crate::events::MetaEvent::RemoveFileBlocks { .. }
                | crate::events::MetaEvent::SetFileBlocksBatch { .. }
                | crate::events::MetaEvent::CommitFileBlock { .. }
                | crate::events::MetaEvent::PublishFileManifest { .. }
                | crate::events::MetaEvent::AddPendingDeletion { .. }
                | crate::events::MetaEvent::RemovePendingDeletion { .. } => {}
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
}

impl MetaEngine for RedbMetaEngine {
    // ─── Format / Settings ───────────────────────────────────────────────

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

    // ─── Counters ────────────────────────────────────────────────────────

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

    // ─── Inodes ──────────────────────────────────────────────────────────

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
            attrs.push(Self::resolve_attr_in_read_snapshot(
                &txn,
                ino,
                &overlay_block_inodes,
                &overlay_attr_inodes,
            )?);
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

    // ─── Directory Entries ───────────────────────────────────────────────

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
    // --- File Blocks -----------------------------------------------------

    fn get_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>> {
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

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetFileBlock {
                    ino,
                    block_index,
                    block: block.clone(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(old_block)
    }

    fn get_file_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>> {
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

            Self::append_event_in_txn(&txn, &crate::events::MetaEvent::RemoveFileBlocks { ino })?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(removed_ids)
    }

    fn remove_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<u64>> {
        let key = encode_file_block_key(ino, block_index);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let removed_id;
        let mut updated_attr: Option<InodeAttr> = None;
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

                let new_committed_size = Self::compute_committed_size_in_txn(&committed, ino)?;
                drop(committed);
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
    /// one transaction — reducing commit overhead by ~100-300×.
    fn flush_blocks_batch(&self, entries: &[BatchFlushEntry]) -> MetaResult<Vec<BatchFlushResult>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut results = Vec::with_capacity(entries.len());
        let mut used_space_delta: i64 = 0;

        {
            let mut blocks_table = txn
                .open_table(FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut counters_table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut touched_inodes = std::collections::BTreeSet::new();

            for e in entries {
                let key = encode_file_block_key(e.ino, e.block_index);
                let block_ref = BlockRef {
                    object_id: e.object_id,
                    offset: 0,
                    length: e.length,
                    flags: e.flags,
                };
                let data = encode(&block_ref)?;

                // Read old block ref (for GC).
                let old_block = if let Some(entry) = blocks_table
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    let old: BlockRef = decode(entry.value())?;
                    Some(old)
                } else {
                    None
                };

                // Write new block ref.
                blocks_table
                    .insert(key.as_slice(), data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                // Handle old block: add to pending deletions, adjust delta.
                if let Some(ref old) = old_block {
                    used_space_delta -= old.length as i64;
                }

                used_space_delta += e.length as i64;
                results.push(BatchFlushResult { old_block });
                touched_inodes.insert(e.ino);
            }

            if !touched_inodes.is_empty() {
                let committed = txn
                    .open_table(COMMITTED_FILE_BLOCKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut inodes = txn
                    .open_table(INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                for ino in touched_inodes {
                    let attr_bytes = inodes
                        .get(ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|g| g.value().to_vec());
                    if let Some(attr_bytes) = attr_bytes {
                        let mut attr: InodeAttr = decode(&attr_bytes)?;
                        let has_committed_manifest =
                            !Self::committed_manifest_in_txn(&committed, ino)?.is_empty();
                        Self::transition_upload_state_for_local_mutation(
                            &mut attr,
                            has_committed_manifest,
                        );
                        let encoded = encode(&attr)?;
                        inodes
                            .insert(ino, encoded.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                    }
                }
            }

            // Apply net USED_SPACE delta in one counter update.
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

            // Inline event-log append: we already hold COUNTERS_TABLE, so we
            // cannot call append_event_in_txn (which would try to open it again).
            let seq_id = match counters_table
                .get("nextEventSeq")
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 1,
            };
            counters_table
                .insert("nextEventSeq", seq_id + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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
            let event = crate::events::MetaEvent::SetFileBlocksBatch {
                blocks: batch_blocks,
            };
            let event_data = encode(&event)?;
            let mut events = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            events
                .insert(seq_id, event_data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(results)
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

            let new_committed_size = Self::compute_committed_size_in_txn(&committed, ino)?;
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
            inodes
                .insert(ino, attr_data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn publish_file_manifest(&self, ino: Ino, expected_epoch: u64) -> MetaResult<bool> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let published_blocks;
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

            published_blocks = Self::current_manifest_in_txn(&txn, ino)?;

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

            attr.committed_size = attr.size;
            attr.committed_content_epoch = attr.content_epoch;
            attr.upload_state = UploadState::Ready;
            let attr_data = encode(&attr)?;
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

    // --- Pending Deletions (GC) -----------------------------------------

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

        // Iterate all entries
        let deletions: Vec<(u64, i64)> = table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.value(), v.value()))
            .collect();

        Ok(deletions)
    }

    // --- Atomic Rename ---------------------------------------------------

    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<RenameAtomicResult> {
        let src_entry = self.lookup(parent, name)?;
        let dst_entry_opt = self.lookup(new_parent, new_name).ok();
        if let Some(dst_entry) = &dst_entry_opt
            && dst_entry.kind == FileType::Directory
            && !self.readdir(dst_entry.ino)?.is_empty()
        {
            return Err(MetaError::DirNotEmpty(dst_entry.ino));
        }

        let moved_dir_seed = if src_entry.kind == FileType::Directory {
            Some(self.get_attr(src_entry.ino)?)
        } else {
            None
        };
        let old_parent_seed = if src_entry.kind == FileType::Directory && parent != new_parent {
            Some(self.get_attr(parent)?)
        } else {
            None
        };
        let new_parent_seed = if src_entry.kind == FileType::Directory && parent != new_parent {
            Some(self.get_attr(new_parent)?)
        } else {
            None
        };
        let displaced_attr_seed = match &dst_entry_opt {
            Some(dst_entry) if dst_entry.kind != FileType::Directory => {
                Some(self.get_attr(dst_entry.ino)?)
            }
            _ => None,
        };

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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
                } else {
                    if let Some(mut attr) = displaced_attr_seed.clone() {
                        attr.nlink = attr.nlink.saturating_sub(1);
                        if attr.nlink == 0 {
                            Self::remove_inode_overlay_in_txn(&txn, dst_entry.ino)?;
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
        let mut refs = HashSet::new();
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
        Ok(refs)
    }

    // --- Symlinks --------------------------------------------------------

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

    // --- Extended Attributes (xattr) --------------------------------------

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
            // XATTR_CREATE (1): fail if exists
            if flags & 1 != 0 && current.is_some() {
                return Err(MetaError::EntryExists {
                    parent: ino,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }
            // XATTR_REPLACE (2): fail if not exists
            if flags & 2 != 0 && current.is_none() {
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

    // --- High-Level Operations (ACID Atomicity) --------------------------

    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let parent_attr = self.get_attr(parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(parent));
        }
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
        let result_attr;
        {
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

            let ino = match counters
                .get(counters::NEXT_INODE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 2,
            };
            counters
                .insert(counters::NEXT_INODE, ino + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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
    }

    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr> {
        let mut parent_attr = self.get_attr(parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(parent));
        }
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
        let result_attr;
        {
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

            let ino = match counters
                .get(counters::NEXT_INODE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 2,
            };
            counters
                .insert(counters::NEXT_INODE, ino + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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
    }

    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr> {
        let parent_attr = self.get_attr(parent)?;
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(parent));
        }
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
        let result_attr;
        {
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

            let ino = match counters
                .get(counters::NEXT_INODE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 2,
            };
            counters
                .insert(counters::NEXT_INODE, ino + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_engine() -> RedbMetaEngine {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let engine = RedbMetaEngine::open(&path).unwrap();
        engine.init_root(1000, 1000).unwrap();
        // Leak dir so it isn't dropped
        std::mem::forget(dir);
        engine
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
        let loaded = engine.get_format_info().unwrap();
        assert_eq!(loaded.name, "test-vol");
        assert_eq!(loaded.endpoint, "http://localhost:9000");
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
    fn test_next_inode_increments() {
        let engine = temp_engine();
        let ino1 = engine.next_inode().unwrap();
        let ino2 = engine.next_inode().unwrap();
        assert_eq!(ino2, ino1 + 1);
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

        // Retrieve the block
        let retrieved = engine.get_file_block(ino, 0).unwrap().unwrap();
        assert_eq!(retrieved.object_id, 1);
        assert_eq!(retrieved.length, 4 * 1024 * 1024);
        assert!(!retrieved.is_compressed());
        assert!(!retrieved.is_partial());

        // Add another block
        let block2 = BlockRef::new_partial(2, 1000);
        engine.set_file_block(ino, 1, &block2).unwrap();

        // Get all blocks
        let all_blocks = engine.get_file_blocks(ino).unwrap();
        assert_eq!(all_blocks.len(), 2);
        assert_eq!(all_blocks[0].0, 0); // block index
        assert_eq!(all_blocks[0].1.object_id, 1);
        assert_eq!(all_blocks[1].0, 1); // block index
        assert_eq!(all_blocks[1].1.object_id, 2);
        assert!(all_blocks[1].1.is_partial());
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
        assert!(!engine.has_local_overlay_state().unwrap());
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
}

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

/// Directory entries: (parent_ino, name) as bytes -> DirEntryValue (postcard)
const DENTRIES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("dentries");

/// File Blocks: (ino, block_index) -> BlockRef (postcard)
/// Key encoding: ino (8 bytes LE) + block_index (4 bytes LE)
const FILE_BLOCKS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("file_blocks");

/// Pending Deletions (GC): object_id (u64) -> deletion_timestamp (i64)
const PENDING_DELETIONS_TABLE: TableDefinition<u64, i64> =
    TableDefinition::new("pending_deletions");

/// Symlink targets: ino (u64) -> target path (&[u8])
const SYMLINKS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("symlinks");

/// Extended attributes: (ino(8) + name_bytes) -> value_bytes
const XATTRS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("xattrs");

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
            let _ = txn.open_table(DENTRIES_TABLE);
            let _ = txn.open_table(FILE_BLOCKS_TABLE);
            let _ = txn.open_table(PENDING_DELETIONS_TABLE);
            let _ = txn.open_table(SYMLINKS_TABLE);
            let _ = txn.open_table(XATTRS_TABLE);
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

    /// Adjust nlink of an inode within an existing write transaction.
    fn adjust_nlink_in_txn(
        inodes: &mut redb::Table<'_, u64, &[u8]>,
        ino: Ino,
        delta: i32,
    ) -> MetaResult<()> {
        let attr_bytes = inodes
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|g| g.value().to_vec());
        if let Some(bytes) = attr_bytes {
            let mut attr: InodeAttr = decode(&bytes)?;
            if delta > 0 {
                attr.nlink += delta as u32;
            } else {
                attr.nlink = attr.nlink.saturating_sub((-delta) as u32);
            }
            attr.touch_mtime();
            let data = encode(&attr)?;
            inodes
                .insert(ino, data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
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
            let table = txn
                .open_table(SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.symlinks.push((k.value(), v.value().to_vec()));
            }
        }
        {
            let table = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .xattrs
                    .push((k.value().to_vec(), v.value().to_vec()));
            }
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
            for (k, v) in &snapshot.symlinks {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
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
            for (k, v) in &snapshot.xattrs {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
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
        let table = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let entry = table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or(MetaError::InodeNotFound(ino))?;
        decode(entry.value())
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        let data = encode(attr)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(attr.ino, data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
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
            let mut table = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .remove(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // Cascade: remove symlink target if exists
            let mut symlinks = txn
                .open_table(SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let _ = symlinks.remove(ino);

            // Cascade: remove all xattrs for this inode
            let mut xattrs = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = xattr_prefix(ino);
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

            Self::append_event_in_txn(&txn, &crate::events::MetaEvent::RemoveAttr { ino })?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    // ─── Directory Entries ───────────────────────────────────────────────

    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        let key = encode_dentry_key(parent, name);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        // Fast path: try exact-case match first
        if let Ok(Some(entry)) = table.get(key.as_slice()) {
            return decode(entry.value());
        }

        // Slow path: case-insensitive search by scanning parent's entries
        // This is critical for Windows compatibility: files like "Image.jpg" and "image.jpg"
        // must resolve to the same inode (Windows is case-insensitive)
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
                return decode(v.value());
            }
        }

        Err(MetaError::EntryNotFound {
            parent,
            name: String::from_utf8_lossy(name).to_string(),
        })
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        let prefix = dentry_prefix(parent);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut entries = Vec::new();

        // Scan all keys starting with the parent prefix
        let range_start = prefix.as_slice();
        let mut range_end = prefix.to_vec();
        // Increment last byte to get exclusive upper bound
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

    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        let data = encode(entry)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // Check existence
            if table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_some()
            {
                return Err(MetaError::EntryExists {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }

            table
                .insert(key.as_slice(), data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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
        let key = encode_dentry_key(parent, name);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let entry_val;
        {
            let mut table = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let removed = table
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .ok_or_else(|| MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                })?;
            entry_val = decode(removed.value())?;

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
            None => Ok(None),
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

        let mut blocks = Vec::new();

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
            blocks.push((block_index, block));
        }

        // Sort by block index for sequential access
        blocks.sort_by_key(|(idx, _)| *idx);
        Ok(blocks)
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
        {
            let mut table = txn
                .open_table(FILE_BLOCKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            if let Some(entry) = table
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let block: BlockRef = decode(entry.value())?;
                removed_id = Some(block.object_id);

                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::RemoveFileBlock { ino, block_index },
                )?;
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

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

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
            let mut pending_del_table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut counters_table = txn
                .open_table(COUNTERS_TABLE)
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
                    pending_del_table
                        .insert(old.object_id, timestamp)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    used_space_delta -= old.length as i64;
                }

                used_space_delta += e.length as i64;
                results.push(BatchFlushResult { old_block });
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
    ) -> MetaResult<Option<Ino>> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let displaced_ino: Option<Ino>;
        {
            let mut dentries = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // 1. Fetch and remove the source entry — copy value out immediately
            //    so the AccessGuard is dropped before further table mutations.
            let src_key = encode_dentry_key(parent, name);
            let src_val_bytes = dentries
                .remove(src_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| g.value().to_vec())
                .ok_or_else(|| MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                })?;
            let src_entry: DirEntryValue = decode(&src_val_bytes)?;

            // 2. Check if destination already exists → handle overwrite
            let dst_key = encode_dentry_key(new_parent, new_name);
            let dst_entry_opt: Option<DirEntryValue> = dentries
                .get(dst_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| decode(g.value()))
                .transpose()?;

            displaced_ino = if let Some(dst_entry) = dst_entry_opt {
                if dst_entry.kind == FileType::Directory {
                    // Target is a directory — it must be empty
                    let prefix = dentry_prefix(dst_entry.ino);
                    let mut range_end = prefix.to_vec();
                    *range_end.last_mut().unwrap() += 1;
                    let has_children = dentries
                        .range(prefix.as_slice()..range_end.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .next()
                        .is_some();
                    if has_children {
                        // Re-insert source entry (rollback within same txn)
                        dentries
                            .insert(src_key.as_slice(), src_val_bytes.as_slice())
                            .map_err(|e| MetaError::Storage(e.to_string()))?;
                        return Err(MetaError::DirNotEmpty(dst_entry.ino));
                    }
                    // Remove the empty dir inode
                    inodes
                        .remove(dst_entry.ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                } else {
                    // Target is a file — decrement nlink, remove if zero
                    let attr_bytes = inodes
                        .get(dst_entry.ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|g| g.value().to_vec());
                    if let Some(bytes) = attr_bytes {
                        let mut attr: InodeAttr = decode(&bytes)?;
                        attr.nlink = attr.nlink.saturating_sub(1);
                        if attr.nlink == 0 {
                            inodes
                                .remove(dst_entry.ino)
                                .map_err(|e| MetaError::Storage(e.to_string()))?;
                        } else {
                            let data = encode(&attr)?;
                            inodes
                                .insert(dst_entry.ino, data.as_slice())
                                .map_err(|e| MetaError::Storage(e.to_string()))?;
                        }
                    }
                }
                // Remove old destination dentry
                dentries
                    .remove(dst_key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                Some(dst_entry.ino)
            } else {
                None
            };

            // 3. Insert destination entry (same value as source)
            dentries
                .insert(dst_key.as_slice(), src_val_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // 4. If moving a directory between parents, update parent pointer + nlinks
            if src_entry.kind == FileType::Directory && parent != new_parent {
                // Update the moved directory's parent pointer
                let dir_attr_bytes = inodes
                    .get(src_entry.ino)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .map(|g| g.value().to_vec());
                if let Some(bytes) = dir_attr_bytes {
                    let mut attr: InodeAttr = decode(&bytes)?;
                    attr.parent = new_parent;
                    let data = encode(&attr)?;
                    inodes
                        .insert(src_entry.ino, data.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;

                    Self::append_event_in_txn(
                        &txn,
                        &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                    )?;
                }
                // Decrement old parent nlink
                Self::adjust_nlink_in_txn(&mut inodes, parent, -1)?;
                // Increment new parent nlink
                Self::adjust_nlink_in_txn(&mut inodes, new_parent, 1)?;
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(displaced_ino)
    }

    // --- Hardlinks -------------------------------------------------------

    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let result;
        {
            let mut dentries = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // 1. Ensure new_name doesn't already exist
            let dst_key = encode_dentry_key(new_parent, new_name);
            let existing = dentries
                .get(dst_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_some();
            if existing {
                return Err(MetaError::EntryExists {
                    parent: new_parent,
                    name: String::from_utf8_lossy(new_name).to_string(),
                });
            }

            // 2. Increment nlink + update ctime on the target inode
            let attr_bytes = inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| g.value().to_vec())
                .ok_or(MetaError::InodeNotFound(ino))?;
            let mut attr: InodeAttr = decode(&attr_bytes)?;
            attr.nlink += 1;
            attr.touch_ctime();
            let data = encode(&attr)?;
            inodes
                .insert(ino, data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // 3. Add new dentry pointing to the existing inode
            let entry_val = encode(&DirEntryValue {
                ino,
                kind: attr.kind,
            })?;
            dentries
                .insert(dst_key.as_slice(), entry_val.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // 4. Touch parent mtime
            let parent_bytes = inodes
                .get(new_parent)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| g.value().to_vec());
            if let Some(bytes) = parent_bytes {
                let mut parent_attr: InodeAttr = decode(&bytes)?;
                parent_attr.touch_mtime();
                let pdata = encode(&parent_attr)?;
                inodes
                    .insert(new_parent, pdata.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            result = attr;
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
        let mut refs = HashSet::new();
        for item in table
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
            let mut table = txn
                .open_table(SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(ino, target)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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
        let table = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let raw = table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or(MetaError::InodeNotFound(ino))?;
        Ok(raw.value().to_vec())
    }

    // --- Extended Attributes (xattr) --------------------------------------

    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        let key = encode_xattr_key(ino, name);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match table
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }

    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let exists = table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_some();

            // XATTR_CREATE (1): fail if exists
            if flags & 1 != 0 && exists {
                return Err(MetaError::EntryExists {
                    parent: ino,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }
            // XATTR_REPLACE (2): fail if not exists
            if flags & 2 != 0 && !exists {
                return Err(MetaError::EntryNotFound {
                    parent: ino,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }

            table
                .insert(key.as_slice(), value)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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
        let key = encode_xattr_key(ino, name);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .ok_or(MetaError::EntryNotFound {
                    parent: ino,
                    name: String::from_utf8_lossy(name).to_string(),
                })?;

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
        let table = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = xattr_prefix(ino);
        let range = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut names = Vec::new();
        for item in range {
            let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            // Extract name: skip the 8-byte ino prefix
            names.push(key_bytes[8..].to_vec());
        }
        Ok(names)
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

            let parent_bytes = inodes
                .get(parent)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .ok_or(MetaError::InodeNotFound(parent))?
                .value()
                .to_vec();
            let parent_attr: InodeAttr = decode(&parent_bytes)?;
            if parent_attr.kind != FileType::Directory {
                return Err(MetaError::NotADirectory(parent));
            }

            let dentry_key = encode_dentry_key(parent, name);
            if dentries
                .get(dentry_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_some()
            {
                return Err(MetaError::EntryExists {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }

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

            let entry_val = encode(&DirEntryValue {
                ino,
                kind: FileType::RegularFile,
            })?;
            dentries
                .insert(dentry_key.as_slice(), entry_val.as_slice())
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

            let parent_bytes = inodes
                .get(parent)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .ok_or(MetaError::InodeNotFound(parent))?
                .value()
                .to_vec();
            let mut parent_attr: InodeAttr = decode(&parent_bytes)?;
            if parent_attr.kind != FileType::Directory {
                return Err(MetaError::NotADirectory(parent));
            }

            let dentry_key = encode_dentry_key(parent, name);
            if dentries
                .get(dentry_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_some()
            {
                return Err(MetaError::EntryExists {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }

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

            let entry_val = encode(&DirEntryValue {
                ino,
                kind: FileType::Directory,
            })?;
            dentries
                .insert(dentry_key.as_slice(), entry_val.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

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
            let mut symlinks = txn
                .open_table(SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let parent_bytes = inodes
                .get(parent)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .ok_or(MetaError::InodeNotFound(parent))?
                .value()
                .to_vec();
            let parent_attr: InodeAttr = decode(&parent_bytes)?;
            if parent_attr.kind != FileType::Directory {
                return Err(MetaError::NotADirectory(parent));
            }

            let dentry_key = encode_dentry_key(parent, name);
            if dentries
                .get(dentry_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_some()
            {
                return Err(MetaError::EntryExists {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }

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

            let entry_val = encode(&DirEntryValue {
                ino,
                kind: FileType::Symlink,
            })?;
            dentries
                .insert(dentry_key.as_slice(), entry_val.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            symlinks
                .insert(ino, target)
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

            let dentry_key = encode_dentry_key(parent, name);
            let dentry_val_bytes = dentries
                .remove(dentry_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .ok_or_else(|| MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                })?
                .value()
                .to_vec();
            let entry: DirEntryValue = decode(&dentry_val_bytes)?;

            if entry.kind == FileType::Directory {
                dentries
                    .insert(dentry_key.as_slice(), dentry_val_bytes.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                return Err(MetaError::IsADirectory(entry.ino));
            }

            let attr_bytes = inodes
                .get(entry.ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .ok_or(MetaError::InodeNotFound(entry.ino))?
                .value()
                .to_vec();
            let mut attr: InodeAttr = decode(&attr_bytes)?;

            attr.nlink = attr.nlink.saturating_sub(1);
            if attr.nlink == 0 {
                inodes
                    .remove(entry.ino)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                let mut symlinks = txn
                    .open_table(SYMLINKS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = symlinks.remove(entry.ino);

                let mut xattrs = txn
                    .open_table(XATTRS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let prefix = xattr_prefix(entry.ino);
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
                let attr_data = encode(&attr)?;
                inodes
                    .insert(entry.ino, attr_data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            let parent_bytes = inodes
                .get(parent)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| g.value().to_vec());
            if let Some(bytes) = parent_bytes {
                let mut parent_attr: InodeAttr = decode(&bytes)?;
                parent_attr.touch_mtime();
                let p_attr_data = encode(&parent_attr)?;
                inodes
                    .insert(parent, p_attr_data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            result_attr = attr;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(result_attr)
    }

    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut dentries = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let dentry_key = encode_dentry_key(parent, name);
            let dentry_val_bytes = dentries
                .get(dentry_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .ok_or_else(|| MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).to_string(),
                })?
                .value()
                .to_vec();
            let entry: DirEntryValue = decode(&dentry_val_bytes)?;

            if entry.kind != FileType::Directory {
                return Err(MetaError::NotADirectory(entry.ino));
            }

            let prefix = dentry_prefix(entry.ino);
            let mut range_end = prefix.to_vec();
            *range_end.last_mut().unwrap() += 1;
            let has_children = dentries
                .range(prefix.as_slice()..range_end.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .next()
                .is_some();
            if has_children {
                return Err(MetaError::DirNotEmpty(entry.ino));
            }

            dentries
                .remove(dentry_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            inodes
                .remove(entry.ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let mut xattrs = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let xattr_pfx = xattr_prefix(entry.ino);
            let keys_to_remove: Vec<Vec<u8>> = {
                let range = xattrs
                    .range(xattr_pfx.as_ref()..)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                range
                    .take_while(|r| {
                        r.as_ref()
                            .map(|(k, _)| k.value().starts_with(&xattr_pfx))
                            .unwrap_or(false)
                    })
                    .filter_map(|r| r.ok().map(|(k, _)| k.value().to_vec()))
                    .collect()
            };
            for key in keys_to_remove {
                let _ = xattrs.remove(key.as_slice());
            }

            let parent_bytes = inodes
                .get(parent)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| g.value().to_vec());
            if let Some(bytes) = parent_bytes {
                let mut parent_attr: InodeAttr = decode(&bytes)?;
                parent_attr.nlink = parent_attr.nlink.saturating_sub(1);
                parent_attr.touch_mtime();
                let p_attr_data = encode(&parent_attr)?;
                inodes
                    .insert(parent, p_attr_data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr {
                        attr: parent_attr.clone(),
                    },
                )?;
            }

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
            name: "test-vol".into(),
            uuid: "abc-123".into(),
            endpoint: "http://localhost:9000".into(),
            bucket: "testbucket".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            region: "".into(),
            data_prefix: "test-volume/objects/".into(),
            metadata: MetadataConfig::default(),
            version: String::new(),
            block_size: DEFAULT_BLOCK_SIZE,
            chunk_size: 0,
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
        engine
            .create_file(ROOT_INO, b"old.txt", 1000, 1000, 0o644)
            .unwrap();
        // rename_atomic returns None when no overwrite
        let displaced = engine
            .rename_atomic(ROOT_INO, b"old.txt", ROOT_INO, b"new.txt")
            .unwrap();
        assert!(displaced.is_none());

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
        assert_eq!(displaced, Some(dst.ino));
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

        assert_eq!(displaced, Some(dir.ino));
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
        assert!(displaced.is_none());

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

//! redb implementation of MetaEngine.
//!
//! Uses redb as an embedded ACID KV store for all filesystem metadata.

use crate::engine::*;
use crate::types::*;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
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
        let entry = table
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .ok_or_else(|| MetaError::EntryNotFound {
                parent,
                name: String::from_utf8_lossy(name).to_string(),
            })?;
        decode(entry.value())
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
        engine
            .rename(ROOT_INO, b"old.txt", ROOT_INO, b"new.txt")
            .unwrap();

        assert!(engine.lookup(ROOT_INO, b"old.txt").is_err());
        assert!(engine.lookup(ROOT_INO, b"new.txt").is_ok());
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
        assert!(retrieved.is_compressed());
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
}

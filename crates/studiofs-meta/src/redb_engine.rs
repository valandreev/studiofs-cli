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

/// Chunks: (ino, chunk_index) as bytes -> Vec<Slice> (postcard)
const CHUNKS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("chunks");

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

/// Encode a chunk key: ino (8 bytes LE) + index (4 bytes BE).
fn encode_chunk_key(key: &ChunkKey) -> [u8; 12] {
    let mut buf = [0u8; 12];
    buf[..8].copy_from_slice(&key.ino.to_le_bytes());
    buf[8..12].copy_from_slice(&key.index.to_be_bytes());
    buf
}

/// The prefix for scanning all chunks of an inode.
fn chunk_prefix(ino: Ino) -> [u8; 8] {
    ino.to_le_bytes()
}

/// The prefix for scanning all dentries of a parent inode.
fn dentry_prefix(parent: Ino) -> [u8; 8] {
    parent.to_le_bytes()
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
            let _ = txn.open_table(CHUNKS_TABLE);
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        debug!("opened metadata database at {:?}", path.as_ref());
        Ok(Self { db: Arc::new(db) })
    }

    /// Initialize a fresh filesystem with root inode.
    pub fn init_root(&self) -> MetaResult<()> {
        // Check if root already exists
        if self.get_attr(ROOT_INO).is_ok() {
            debug!("root inode already exists, skipping init");
            return Ok(());
        }

        // Set initial counters
        self.set_counter(counters::NEXT_INODE, ROOT_INO + 1)?;
        self.set_counter(counters::NEXT_SLICE_ID, 1)?;
        self.set_counter(counters::USED_SPACE, 0)?;
        self.set_counter(counters::TOTAL_INODES, 1)?;

        // Create root directory inode
        let root_attr = InodeAttr::new_dir(ROOT_INO, ROOT_INO, 0, 0, 0o755);
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

    // ─── Chunks / Slices ─────────────────────────────────────────────────

    fn get_chunk_slices(&self, key: &ChunkKey) -> MetaResult<Vec<Slice>> {
        let encoded_key = encode_chunk_key(key);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(CHUNKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match table
            .get(encoded_key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(entry) => decode(entry.value()),
            None => Ok(Vec::new()),
        }
    }

    fn append_chunk_slice(&self, key: &ChunkKey, slice: &Slice) -> MetaResult<()> {
        let encoded_key = encode_chunk_key(key);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(CHUNKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let mut slices: Vec<Slice> = match table
                .get(encoded_key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(entry) => decode(entry.value())?,
                None => Vec::new(),
            };
            slices.push(slice.clone());

            let data = encode(&slices)?;
            table
                .insert(encoded_key.as_slice(), data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn set_chunk_slices(&self, key: &ChunkKey, slices: &[Slice]) -> MetaResult<()> {
        let encoded_key = encode_chunk_key(key);
        let data = encode(&slices.to_vec())?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(CHUNKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(encoded_key.as_slice(), data.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_file_chunks(&self, ino: Ino) -> MetaResult<()> {
        let prefix = chunk_prefix(ino);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(CHUNKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            // Collect keys to remove
            let mut range_end = prefix.to_vec();
            *range_end.last_mut().unwrap() += 1;

            let keys_to_remove: Vec<Vec<u8>> = {
                let iter = table
                    .range(prefix.as_slice()..range_end.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                iter.filter_map(|r| r.ok())
                    .map(|(k, _)| k.value().to_vec())
                    .collect()
            };

            for key in &keys_to_remove {
                table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        if cfg!(debug_assertions) {
            debug!("removed all chunks for inode {ino}");
        }
        Ok(())
    }

    fn next_slice_id(&self) -> MetaResult<u64> {
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
                .get(counters::NEXT_SLICE_ID)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 1,
            };
            table
                .insert(counters::NEXT_SLICE_ID, id + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_engine() -> RedbMetaEngine {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let engine = RedbMetaEngine::open(&path).unwrap();
        engine.init_root().unwrap();
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
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
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
    fn test_chunk_slices() {
        let engine = temp_engine();
        let key = ChunkKey::new(2, 0);

        let slice1 = Slice {
            pos: 0,
            id: 1,
            size: BLOCK_SIZE,
            off: 0,
            len: BLOCK_SIZE,
        };
        engine.append_chunk_slice(&key, &slice1).unwrap();

        let slice2 = Slice {
            pos: BLOCK_SIZE,
            id: 2,
            size: BLOCK_SIZE,
            off: 0,
            len: BLOCK_SIZE,
        };
        engine.append_chunk_slice(&key, &slice2).unwrap();

        let slices = engine.get_chunk_slices(&key).unwrap();
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].id, 1);
        assert_eq!(slices[1].id, 2);
    }

    #[test]
    fn test_next_inode_increments() {
        let engine = temp_engine();
        let ino1 = engine.next_inode().unwrap();
        let ino2 = engine.next_inode().unwrap();
        assert_eq!(ino2, ino1 + 1);
    }
}

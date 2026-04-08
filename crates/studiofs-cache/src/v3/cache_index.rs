//! Cache 3.0 metadata index backed by redb.
//!
//! The index is the source of truth for block lifecycle state, replay ordering,
//! and space accounting. Accounting counters are updated atomically with row
//! mutations so budget checks never require payload scans.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::v2::LocalBlockLifecycleState;

const INDEX_FORMAT_VERSION: u64 = 1;
const ENTRY_ENCODING_VERSION: u8 = 1;
const NO_DIRTY_ORDER: u64 = u64::MAX;

const BLOCKS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("blocks");
const DIRTY_ORDER_TABLE: TableDefinition<u64, u64> = TableDefinition::new("dirty_order");
const ACCESS_ORDER_TABLE: TableDefinition<u64, u64> = TableDefinition::new("access_order");
const COUNTERS_TABLE: TableDefinition<&str, u64> = TableDefinition::new("counters");

const COUNTER_INDEX_FORMAT: &str = "index_format_version";
const COUNTER_CURRENT_BYTES: &str = "current_bytes";
const COUNTER_DIRTY_BYTES: &str = "dirty_bytes";
const COUNTER_EVICTABLE_BYTES: &str = "evictable_bytes";
const COUNTER_NEXT_DIRTY_SEQ: &str = "next_dirty_seq";
const COUNTER_NEXT_ACCESS_SEQ: &str = "next_access_seq";

#[derive(Debug, thiserror::Error)]
pub enum CacheIndexError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("index storage error: {0}")]
    Storage(String),
    #[error("index corruption: {0}")]
    Corruption(String),
    #[error("unsupported cache index format: expected {expected}, found {found}")]
    UnsupportedFormat { expected: u64, found: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheIndexStats {
    pub current_bytes: u64,
    pub dirty_bytes: u64,
    pub evictable_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheIndexEntry {
    pub object_id: u64,
    pub payload_key: Vec<u8>,
    pub payload_len: u64,
    pub lifecycle: LocalBlockLifecycleState,
    pub evictable: bool,
    pub dirty_order: Option<u64>,
    pub access_order: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheIndexUpsert {
    pub object_id: u64,
    pub payload_key: Vec<u8>,
    pub payload_len: u64,
    pub lifecycle: LocalBlockLifecycleState,
    pub evictable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyReplayEntry {
    pub object_id: u64,
    pub payload_key: Vec<u8>,
    pub payload_len: u64,
    pub lifecycle: LocalBlockLifecycleState,
    pub dirty_order: u64,
}

#[derive(Debug)]
pub struct CacheIndex {
    db_path: PathBuf,
    db: Arc<Database>,
}

impl CacheIndex {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CacheIndexError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(&path).map_err(storage_error)?;
        let index = Self {
            db_path: path,
            db: Arc::new(db),
        };
        index.initialize_schema()?;
        Ok(index)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn stats(&self) -> Result<CacheIndexStats, CacheIndexError> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;
        Ok(CacheIndexStats {
            current_bytes: required_counter(&counters, COUNTER_CURRENT_BYTES)?,
            dirty_bytes: required_counter(&counters, COUNTER_DIRTY_BYTES)?,
            evictable_bytes: required_counter(&counters, COUNTER_EVICTABLE_BYTES)?,
        })
    }

    pub fn get_entry(&self, object_id: u64) -> Result<Option<CacheIndexEntry>, CacheIndexError> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
        Ok(read_entry_from_blocks(&blocks, object_id)?.map(|entry| entry.into_public(object_id)))
    }

    pub fn upsert(&self, mut update: CacheIndexUpsert) -> Result<(), CacheIndexError> {
        if matches!(
            update.lifecycle,
            LocalBlockLifecycleState::Dirty | LocalBlockLifecycleState::Uploading
        ) {
            update.evictable = false;
        }

        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let old = read_entry_from_blocks_write(&blocks, update.object_id)?;
            let mut new = StoredEntry {
                payload_key: update.payload_key,
                payload_len: update.payload_len,
                lifecycle: update.lifecycle,
                evictable: update.evictable,
                dirty_order: None,
                access_order: allocate_sequence(&mut counters, COUNTER_NEXT_ACCESS_SEQ)?,
            };
            if is_dirty_like(new.lifecycle) {
                new.dirty_order = Some(allocate_sequence(&mut counters, COUNTER_NEXT_DIRTY_SEQ)?);
            }

            apply_entry_change(
                update.object_id,
                old.as_ref(),
                Some(&new),
                &mut blocks,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    pub fn touch_access(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_from_blocks_write(&blocks, object_id)? else {
                return Ok(());
            };
            let mut new = old.clone();
            new.access_order = allocate_sequence(&mut counters, COUNTER_NEXT_ACCESS_SEQ)?;

            apply_entry_change(
                object_id,
                Some(&old),
                Some(&new),
                &mut blocks,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    pub fn mark_uploading(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_from_blocks_write(&blocks, object_id)? else {
                return Ok(());
            };
            if old.lifecycle != LocalBlockLifecycleState::Dirty {
                return Ok(());
            }

            let mut new = old.clone();
            new.lifecycle = LocalBlockLifecycleState::Uploading;
            new.evictable = false;
            if new.dirty_order.is_none() {
                new.dirty_order = Some(allocate_sequence(&mut counters, COUNTER_NEXT_DIRTY_SEQ)?);
            }

            apply_entry_change(
                object_id,
                Some(&old),
                Some(&new),
                &mut blocks,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    pub fn mark_upload_failed(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_from_blocks_write(&blocks, object_id)? else {
                return Ok(());
            };
            if old.lifecycle != LocalBlockLifecycleState::Uploading {
                return Ok(());
            }

            let mut new = old.clone();
            new.lifecycle = LocalBlockLifecycleState::Dirty;
            new.evictable = false;
            new.dirty_order = Some(allocate_sequence(&mut counters, COUNTER_NEXT_DIRTY_SEQ)?);

            apply_entry_change(
                object_id,
                Some(&old),
                Some(&new),
                &mut blocks,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    pub fn mark_uploaded(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_from_blocks_write(&blocks, object_id)? else {
                return Ok(());
            };
            let mut new = old.clone();
            new.lifecycle = LocalBlockLifecycleState::Uploaded;
            new.evictable = true;
            new.dirty_order = None;

            apply_entry_change(
                object_id,
                Some(&old),
                Some(&new),
                &mut blocks,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    pub fn remove(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_from_blocks_write(&blocks, object_id)? else {
                return Ok(());
            };

            apply_entry_change(
                object_id,
                Some(&old),
                None,
                &mut blocks,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    pub fn dirty_uploading_entries(&self) -> Result<Vec<DirtyReplayEntry>, CacheIndexError> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
        let dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;

        let mut ordered = Vec::new();
        let iter = dirty_order.range::<u64>(..).map_err(storage_error)?;
        for pair in iter {
            let (order, object_id) = pair.map_err(storage_error)?;
            let order = order.value();
            let object_id = object_id.value();

            let Some(entry) = read_entry_from_blocks(&blocks, object_id)? else {
                return Err(CacheIndexError::Corruption(format!(
                    "dirty order references missing block object_id={object_id}"
                )));
            };
            if entry.dirty_order != Some(order) {
                return Err(CacheIndexError::Corruption(format!(
                    "dirty order mismatch for object_id={object_id}: table={order}, row={:?}",
                    entry.dirty_order
                )));
            }
            if !is_dirty_like(entry.lifecycle) {
                return Err(CacheIndexError::Corruption(format!(
                    "dirty order contains non-dirty lifecycle for object_id={object_id}: {:?}",
                    entry.lifecycle
                )));
            }

            ordered.push(DirtyReplayEntry {
                object_id,
                payload_key: entry.payload_key,
                payload_len: entry.payload_len,
                lifecycle: entry.lifecycle,
                dirty_order: order,
            });
        }

        Ok(ordered)
    }

    pub fn access_order_object_ids(&self) -> Result<Vec<u64>, CacheIndexError> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
        let access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;

        let mut ordered = Vec::new();
        let iter = access_order.range::<u64>(..).map_err(storage_error)?;
        for pair in iter {
            let (_, object_id) = pair.map_err(storage_error)?;
            let object_id = object_id.value();
            if read_entry_from_blocks(&blocks, object_id)?.is_some() {
                ordered.push(object_id);
            }
        }
        Ok(ordered)
    }

    fn initialize_schema(&self) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let blocks_exist = {
                let blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
                blocks.first().map_err(storage_error)?.is_some()
            };
            let _ = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let _ = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;

            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            if let Some(found) = counters
                .get(COUNTER_INDEX_FORMAT)
                .map_err(storage_error)?
                .map(|value| value.value())
            {
                if found != INDEX_FORMAT_VERSION {
                    return Err(CacheIndexError::UnsupportedFormat {
                        expected: INDEX_FORMAT_VERSION,
                        found,
                    });
                }
            } else if blocks_exist {
                return Err(CacheIndexError::Corruption(format!(
                    "missing '{COUNTER_INDEX_FORMAT}' counter in non-empty index"
                )));
            } else {
                counters
                    .insert(COUNTER_INDEX_FORMAT, &INDEX_FORMAT_VERSION)
                    .map_err(storage_error)?;
            }

            for (name, default) in [
                (COUNTER_CURRENT_BYTES, 0_u64),
                (COUNTER_DIRTY_BYTES, 0_u64),
                (COUNTER_EVICTABLE_BYTES, 0_u64),
                (COUNTER_NEXT_DIRTY_SEQ, 0_u64),
                (COUNTER_NEXT_ACCESS_SEQ, 0_u64),
            ] {
                if counters.get(name).map_err(storage_error)?.is_none() {
                    if blocks_exist {
                        return Err(CacheIndexError::Corruption(format!(
                            "missing '{name}' counter in non-empty index"
                        )));
                    }
                    counters.insert(name, &default).map_err(storage_error)?;
                }
            }
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredEntry {
    payload_key: Vec<u8>,
    payload_len: u64,
    lifecycle: LocalBlockLifecycleState,
    evictable: bool,
    dirty_order: Option<u64>,
    access_order: u64,
}

impl StoredEntry {
    fn into_public(self, object_id: u64) -> CacheIndexEntry {
        CacheIndexEntry {
            object_id,
            payload_key: self.payload_key,
            payload_len: self.payload_len,
            lifecycle: self.lifecycle,
            evictable: self.evictable,
            dirty_order: self.dirty_order,
            access_order: self.access_order,
        }
    }
}

fn read_entry_from_blocks(
    blocks: &redb::ReadOnlyTable<u64, &[u8]>,
    object_id: u64,
) -> Result<Option<StoredEntry>, CacheIndexError> {
    blocks
        .get(&object_id)
        .map_err(storage_error)?
        .map(|value| decode_entry(value.value(), object_id))
        .transpose()
}

fn read_entry_from_blocks_write(
    blocks: &redb::Table<'_, u64, &[u8]>,
    object_id: u64,
) -> Result<Option<StoredEntry>, CacheIndexError> {
    blocks
        .get(&object_id)
        .map_err(storage_error)?
        .map(|value| decode_entry(value.value(), object_id))
        .transpose()
}

fn apply_entry_change(
    object_id: u64,
    old: Option<&StoredEntry>,
    new: Option<&StoredEntry>,
    blocks: &mut redb::Table<'_, u64, &[u8]>,
    dirty_order: &mut redb::Table<'_, u64, u64>,
    access_order: &mut redb::Table<'_, u64, u64>,
    counters: &mut redb::Table<'_, &str, u64>,
) -> Result<(), CacheIndexError> {
    if let Some(old_entry) = old {
        remove_order_link(dirty_order, old_entry.dirty_order, object_id, "dirty")?;
        remove_order_link(
            access_order,
            Some(old_entry.access_order),
            object_id,
            "access",
        )?;
    }

    if let Some(new_entry) = new {
        if let Some(dirty_seq) = new_entry.dirty_order {
            dirty_order
                .insert(&dirty_seq, &object_id)
                .map_err(storage_error)?;
        }
        access_order
            .insert(&new_entry.access_order, &object_id)
            .map_err(storage_error)?;
    }

    apply_accounting_delta(counters, old, new)?;

    match new {
        Some(new_entry) => {
            let encoded = encode_entry(new_entry)?;
            blocks
                .insert(&object_id, encoded.as_slice())
                .map_err(storage_error)?;
        }
        None => {
            let _ = blocks.remove(&object_id).map_err(storage_error)?;
        }
    }

    Ok(())
}

fn remove_order_link(
    table: &mut redb::Table<'_, u64, u64>,
    sequence: Option<u64>,
    object_id: u64,
    order_name: &str,
) -> Result<(), CacheIndexError> {
    let Some(sequence) = sequence else {
        return Ok(());
    };
    let removed = table.remove(&sequence).map_err(storage_error)?;
    if let Some(value) = removed {
        let found = value.value();
        if found != object_id {
            return Err(CacheIndexError::Corruption(format!(
                "{order_name} order sequence={sequence} points to object_id={found}, expected {object_id}"
            )));
        }
    }
    Ok(())
}

fn apply_accounting_delta(
    counters: &mut redb::Table<'_, &str, u64>,
    old: Option<&StoredEntry>,
    new: Option<&StoredEntry>,
) -> Result<(), CacheIndexError> {
    let (old_current, old_dirty, old_evictable) = contribution(old);
    let (new_current, new_dirty, new_evictable) = contribution(new);

    adjust_counter(
        counters,
        COUNTER_CURRENT_BYTES,
        i128::from(new_current) - i128::from(old_current),
    )?;
    adjust_counter(
        counters,
        COUNTER_DIRTY_BYTES,
        i128::from(new_dirty) - i128::from(old_dirty),
    )?;
    adjust_counter(
        counters,
        COUNTER_EVICTABLE_BYTES,
        i128::from(new_evictable) - i128::from(old_evictable),
    )?;
    Ok(())
}

fn contribution(entry: Option<&StoredEntry>) -> (u64, u64, u64) {
    let Some(entry) = entry else {
        return (0, 0, 0);
    };
    let dirty = if is_dirty_like(entry.lifecycle) {
        entry.payload_len
    } else {
        0
    };
    let evictable = if entry.evictable {
        entry.payload_len
    } else {
        0
    };
    (entry.payload_len, dirty, evictable)
}

fn adjust_counter(
    counters: &mut redb::Table<'_, &str, u64>,
    name: &'static str,
    delta: i128,
) -> Result<(), CacheIndexError> {
    if delta == 0 {
        return Ok(());
    }

    let current = required_counter(counters, name)?;
    let next = if delta > 0 {
        current.checked_add(u64::try_from(delta).map_err(|_| {
            CacheIndexError::Corruption(format!(
                "counter '{name}' positive delta overflow: {delta}"
            ))
        })?)
    } else {
        current.checked_sub(u64::try_from(-delta).map_err(|_| {
            CacheIndexError::Corruption(format!(
                "counter '{name}' negative delta overflow: {delta}"
            ))
        })?)
    }
    .ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "counter '{name}' underflow/overflow: current={current}, delta={delta}"
        ))
    })?;

    counters.insert(name, &next).map_err(storage_error)?;
    Ok(())
}

fn required_counter<T>(table: &T, key: &'static str) -> Result<u64, CacheIndexError>
where
    T: ReadableTable<&'static str, u64>,
{
    table
        .get(key)
        .map_err(storage_error)?
        .map(|value| value.value())
        .ok_or_else(|| CacheIndexError::Corruption(format!("missing required counter '{key}'")))
}

fn allocate_sequence(
    counters: &mut redb::Table<'_, &str, u64>,
    counter_name: &'static str,
) -> Result<u64, CacheIndexError> {
    let current = required_counter(counters, counter_name)?;
    let next = current.checked_add(1).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "counter '{counter_name}' overflow while allocating sequence"
        ))
    })?;
    counters
        .insert(counter_name, &next)
        .map_err(storage_error)?;
    Ok(current)
}

fn is_dirty_like(lifecycle: LocalBlockLifecycleState) -> bool {
    matches!(
        lifecycle,
        LocalBlockLifecycleState::Dirty | LocalBlockLifecycleState::Uploading
    )
}

fn encode_entry(entry: &StoredEntry) -> Result<Vec<u8>, CacheIndexError> {
    let payload_key_len = u32::try_from(entry.payload_key.len())
        .map_err(|_| CacheIndexError::Corruption("payload key exceeds u32 length".to_string()))?;
    let mut out = Vec::with_capacity(32 + entry.payload_key.len());
    out.push(ENTRY_ENCODING_VERSION);
    out.push(encode_lifecycle(entry.lifecycle));
    out.push(u8::from(entry.evictable));
    out.push(0);
    out.extend_from_slice(&entry.payload_len.to_le_bytes());
    out.extend_from_slice(&entry.dirty_order.unwrap_or(NO_DIRTY_ORDER).to_le_bytes());
    out.extend_from_slice(&entry.access_order.to_le_bytes());
    out.extend_from_slice(&payload_key_len.to_le_bytes());
    out.extend_from_slice(&entry.payload_key);
    Ok(out)
}

fn decode_entry(raw: &[u8], object_id: u64) -> Result<StoredEntry, CacheIndexError> {
    const HEADER_LEN: usize = 32;
    if raw.len() < HEADER_LEN {
        return Err(CacheIndexError::Corruption(format!(
            "entry too short for object_id={object_id}: len={}",
            raw.len()
        )));
    }
    if raw[0] != ENTRY_ENCODING_VERSION {
        return Err(CacheIndexError::Corruption(format!(
            "unknown entry encoding version for object_id={object_id}: {}",
            raw[0]
        )));
    }

    let lifecycle = decode_lifecycle(raw[1]).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "invalid lifecycle value for object_id={object_id}: {}",
            raw[1]
        ))
    })?;
    let evictable = raw[2] != 0;
    let payload_len = u64::from_le_bytes(raw[4..12].try_into().unwrap());
    let dirty_order_raw = u64::from_le_bytes(raw[12..20].try_into().unwrap());
    let access_order = u64::from_le_bytes(raw[20..28].try_into().unwrap());
    let payload_key_len = u32::from_le_bytes(raw[28..32].try_into().unwrap()) as usize;

    if raw.len() != HEADER_LEN + payload_key_len {
        return Err(CacheIndexError::Corruption(format!(
            "payload key length mismatch for object_id={object_id}: encoded={payload_key_len}, total_len={}",
            raw.len()
        )));
    }
    let payload_key = raw[HEADER_LEN..].to_vec();

    let dirty_order = if dirty_order_raw == NO_DIRTY_ORDER {
        None
    } else {
        Some(dirty_order_raw)
    };

    Ok(StoredEntry {
        payload_key,
        payload_len,
        lifecycle,
        evictable,
        dirty_order,
        access_order,
    })
}

fn encode_lifecycle(lifecycle: LocalBlockLifecycleState) -> u8 {
    match lifecycle {
        LocalBlockLifecycleState::Missing => 0,
        LocalBlockLifecycleState::Dirty => 1,
        LocalBlockLifecycleState::Uploading => 2,
        LocalBlockLifecycleState::Uploaded => 3,
    }
}

fn decode_lifecycle(raw: u8) -> Option<LocalBlockLifecycleState> {
    match raw {
        0 => Some(LocalBlockLifecycleState::Missing),
        1 => Some(LocalBlockLifecycleState::Dirty),
        2 => Some(LocalBlockLifecycleState::Uploading),
        3 => Some(LocalBlockLifecycleState::Uploaded),
        _ => None,
    }
}

fn storage_error<E: std::fmt::Display>(error: E) -> CacheIndexError {
    CacheIndexError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_index(path: &Path) -> CacheIndex {
        CacheIndex::open(path).expect("open cache index")
    }

    fn upsert_dirty(index: &CacheIndex, object_id: u64, payload_len: u64) {
        index
            .upsert(CacheIndexUpsert {
                object_id,
                payload_key: format!("payload/{object_id}.block").into_bytes(),
                payload_len,
                lifecycle: LocalBlockLifecycleState::Dirty,
                evictable: false,
            })
            .expect("upsert dirty block");
    }

    fn upsert_uploaded(index: &CacheIndex, object_id: u64, payload_len: u64) {
        index
            .upsert(CacheIndexUpsert {
                object_id,
                payload_key: format!("payload/{object_id}.block").into_bytes(),
                payload_len,
                lifecycle: LocalBlockLifecycleState::Uploaded,
                evictable: true,
            })
            .expect("upsert uploaded block");
    }

    #[test]
    fn accounting_is_exact_across_lifecycle_transitions() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_dirty(&index, 11, 100);
        upsert_uploaded(&index, 12, 25);

        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 125);
        assert_eq!(stats.dirty_bytes, 100);
        assert_eq!(stats.evictable_bytes, 25);

        index.mark_uploading(11).unwrap();
        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 125);
        assert_eq!(stats.dirty_bytes, 100);
        assert_eq!(stats.evictable_bytes, 25);

        index.mark_uploaded(11).unwrap();
        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 125);
        assert_eq!(stats.dirty_bytes, 0);
        assert_eq!(stats.evictable_bytes, 125);

        index.remove(12).unwrap();
        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 100);
        assert_eq!(stats.dirty_bytes, 0);
        assert_eq!(stats.evictable_bytes, 100);
    }

    #[test]
    fn reopening_restores_deterministic_dirty_upload_order() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");

        {
            let index = open_index(&db_path);
            upsert_dirty(&index, 1, 10);
            upsert_dirty(&index, 2, 20);
            index.mark_uploading(1).unwrap();
            index.mark_upload_failed(1).unwrap();
            index.mark_uploading(2).unwrap();

            let before_restart = index.dirty_uploading_entries().unwrap();
            assert_eq!(before_restart.len(), 2);
            assert_eq!(before_restart[0].object_id, 2);
            assert_eq!(
                before_restart[0].lifecycle,
                LocalBlockLifecycleState::Uploading
            );
            assert_eq!(before_restart[1].object_id, 1);
            assert_eq!(before_restart[1].lifecycle, LocalBlockLifecycleState::Dirty);
        }

        let reopened = open_index(&db_path);
        let after_restart = reopened.dirty_uploading_entries().unwrap();
        assert_eq!(after_restart.len(), 2);
        assert_eq!(after_restart[0].object_id, 2);
        assert_eq!(
            after_restart[0].lifecycle,
            LocalBlockLifecycleState::Uploading
        );
        assert_eq!(after_restart[1].object_id, 1);
        assert_eq!(after_restart[1].lifecycle, LocalBlockLifecycleState::Dirty);

        let stats = reopened.stats().unwrap();
        assert_eq!(stats.current_bytes, 30);
        assert_eq!(stats.dirty_bytes, 30);
        assert_eq!(stats.evictable_bytes, 0);
    }

    #[test]
    fn touch_access_updates_lru_order_without_changing_accounting() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_dirty(&index, 20, 50);
        upsert_uploaded(&index, 21, 30);
        index.touch_access(20).unwrap();

        assert_eq!(index.access_order_object_ids().unwrap(), vec![21, 20]);
        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 80);
        assert_eq!(stats.dirty_bytes, 50);
        assert_eq!(stats.evictable_bytes, 30);
    }
}

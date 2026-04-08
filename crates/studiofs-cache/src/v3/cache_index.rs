//! Cache 3.0 metadata index backed by redb.
//!
//! The index is the source of truth for block lifecycle state, replay ordering,
//! and space accounting. Accounting counters are updated atomically with row
//! mutations so budget checks never require payload scans.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::Mutex;
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
const LIST_PAGE_SIZE: usize = 4_096;
#[cfg(not(test))]
const TOUCH_ACCESS_LRU_CAPACITY: usize = 200_000;
#[cfg(test)]
const TOUCH_ACCESS_LRU_CAPACITY: usize = 128;

const ENTRY_OFFSET_VERSION: usize = 0;
const ENTRY_OFFSET_LIFECYCLE: usize = 1;
const ENTRY_OFFSET_EVICTABLE: usize = 2;
const ENTRY_OFFSET_PAYLOAD_LEN: usize = 4;
const ENTRY_OFFSET_DIRTY_ORDER: usize = 12;
const ENTRY_OFFSET_ACCESS_ORDER: usize = 20;
const ENTRY_OFFSET_PAYLOAD_KEY_LEN: usize = 28;
const ENTRY_HEADER_LEN: usize = 32;

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
    last_touch_access: Mutex<LruCache<u64, Instant>>,
}

impl CacheIndex {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CacheIndexError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // redb::Database::create() creates a new DB for missing/empty files and
        // opens an existing valid redb file, so this is safe for reopen paths.
        let db = Database::create(&path).map_err(storage_error)?;
        let index = Self {
            db_path: path,
            db: Arc::new(db),
            last_touch_access: Mutex::new(LruCache::new(touch_access_lru_capacity())),
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
        self.last_touch_access.lock().pop(&object_id);
        Ok(())
    }

    /// Remove many objects in a single write transaction.
    pub fn remove_batch(&self, object_ids: &[u64]) -> Result<(), CacheIndexError> {
        if object_ids.is_empty() {
            return Ok(());
        }

        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;
            let mut batch_delta = AccountingDelta::default();

            for object_id in object_ids {
                let Some(old) = read_entry_from_blocks_write(&blocks, *object_id)? else {
                    continue;
                };
                remove_order_link(&mut dirty_order, old.dirty_order, *object_id, "dirty")?;
                remove_order_link(
                    &mut access_order,
                    Some(old.access_order),
                    *object_id,
                    "access",
                )?;
                let _ = blocks.remove(object_id).map_err(storage_error)?;
                batch_delta.subtract_entry(&old)?;
            }

            if !batch_delta.is_zero() {
                apply_accounting_delta_values(&mut counters, batch_delta)?;
            }
        }

        txn.commit().map_err(storage_error)?;
        let mut last_touch = self.last_touch_access.lock();
        for object_id in object_ids {
            last_touch.pop(object_id);
        }
        Ok(())
    }

    pub fn dirty_uploading_entries(&self) -> Result<Vec<DirtyReplayEntry>, CacheIndexError> {
        let mut cursor = None;
        let mut out = Vec::new();
        loop {
            let page = self.dirty_uploading_entries_page(cursor, LIST_PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|entry| entry.dirty_order);
            out.extend(page);
        }
        Ok(out)
    }

    /// Page dirty/uploading entries by dirty order sequence.
    pub fn dirty_uploading_entries_page(
        &self,
        start_after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<DirtyReplayEntry>, CacheIndexError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let txn = self.db.begin_read().map_err(storage_error)?;
        let blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
        let dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;

        let start = match start_after {
            Some(last_seen) => match last_seen.checked_add(1) {
                Some(next) => next,
                None => return Ok(Vec::new()),
            },
            None => 0,
        };

        let mut ordered = Vec::with_capacity(limit.min(256));
        let iter = dirty_order.range::<u64>(start..).map_err(storage_error)?;
        for pair in iter {
            if ordered.len() >= limit {
                break;
            }
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
        let mut cursor = None;
        let mut out = Vec::new();
        loop {
            let page = self.access_order_object_ids_page(cursor, LIST_PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|(_, order)| *order);
            out.extend(page.into_iter().map(|(object_id, _)| object_id));
        }
        Ok(out)
    }

    /// Page access-order ids by sequence; returns `(object_id, access_order_seq)`.
    pub fn access_order_object_ids_page(
        &self,
        start_after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<(u64, u64)>, CacheIndexError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let txn = self.db.begin_read().map_err(storage_error)?;
        let blocks = txn.open_table(BLOCKS_TABLE).map_err(storage_error)?;
        let access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;

        let start = match start_after {
            Some(last_seen) => match last_seen.checked_add(1) {
                Some(next) => next,
                None => return Ok(Vec::new()),
            },
            None => 0,
        };

        let mut ordered = Vec::with_capacity(limit.min(256));
        let iter = access_order.range::<u64>(start..).map_err(storage_error)?;
        for pair in iter {
            if ordered.len() >= limit {
                break;
            }
            let (order, object_id) = pair.map_err(storage_error)?;
            let order = order.value();
            let object_id = object_id.value();
            if read_entry_from_blocks(&blocks, object_id)?.is_some() {
                ordered.push((object_id, order));
            }
        }
        Ok(ordered)
    }

    /// Best-effort atime-like throttling helper for hot paths.
    ///
    /// Returns `true` when touch is persisted, `false` when skipped because
    /// `min_interval` has not elapsed yet for this object id.
    pub fn touch_access_if_stale(
        &self,
        object_id: u64,
        min_interval: Duration,
    ) -> Result<bool, CacheIndexError> {
        let now = Instant::now();
        {
            let mut gate = self.last_touch_access.lock();
            if let Some(last) = gate.get(&object_id) {
                if last.elapsed() < min_interval {
                    return Ok(false);
                }
            }
            gate.put(object_id, now);
        }
        if let Err(error) = self.touch_access(object_id) {
            self.last_touch_access.lock().pop(&object_id);
            return Err(error);
        }
        Ok(true)
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
    let old_dirty_order = old.and_then(|entry| entry.dirty_order);
    let new_dirty_order = new.and_then(|entry| entry.dirty_order);
    if old_dirty_order != new_dirty_order {
        if let Some(old_dirty) = old_dirty_order {
            remove_order_link(dirty_order, Some(old_dirty), object_id, "dirty")?;
        }
        if let Some(dirty_seq) = new_dirty_order {
            dirty_order
                .insert(&dirty_seq, &object_id)
                .map_err(storage_error)?;
        }
    }

    let old_access_order = old.map(|entry| entry.access_order);
    let new_access_order = new.map(|entry| entry.access_order);
    if old_access_order != new_access_order {
        if let Some(old_access) = old_access_order {
            remove_order_link(access_order, Some(old_access), object_id, "access")?;
        }
        if let Some(new_access) = new_access_order {
            access_order
                .insert(&new_access, &object_id)
                .map_err(storage_error)?;
        }
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
    apply_accounting_delta_values(
        counters,
        AccountingDelta {
            current_delta: i128::from(new_current) - i128::from(old_current),
            dirty_delta: i128::from(new_dirty) - i128::from(old_dirty),
            evictable_delta: i128::from(new_evictable) - i128::from(old_evictable),
        },
    )
}

fn apply_accounting_delta_values(
    counters: &mut redb::Table<'_, &str, u64>,
    delta: AccountingDelta,
) -> Result<(), CacheIndexError> {
    adjust_counter(counters, COUNTER_CURRENT_BYTES, delta.current_delta)?;
    adjust_counter(counters, COUNTER_DIRTY_BYTES, delta.dirty_delta)?;
    adjust_counter(counters, COUNTER_EVICTABLE_BYTES, delta.evictable_delta)?;
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

#[derive(Debug, Default, Clone, Copy)]
struct AccountingDelta {
    current_delta: i128,
    dirty_delta: i128,
    evictable_delta: i128,
}

impl AccountingDelta {
    fn is_zero(self) -> bool {
        self.current_delta == 0 && self.dirty_delta == 0 && self.evictable_delta == 0
    }

    fn subtract_entry(&mut self, entry: &StoredEntry) -> Result<(), CacheIndexError> {
        let payload = i128::from(entry.payload_len);
        self.current_delta = self
            .current_delta
            .checked_sub(payload)
            .ok_or_else(|| CacheIndexError::Corruption("current_delta underflow".to_string()))?;
        if is_dirty_like(entry.lifecycle) {
            self.dirty_delta = self
                .dirty_delta
                .checked_sub(payload)
                .ok_or_else(|| CacheIndexError::Corruption("dirty_delta underflow".to_string()))?;
        }
        if entry.evictable {
            self.evictable_delta = self.evictable_delta.checked_sub(payload).ok_or_else(|| {
                CacheIndexError::Corruption("evictable_delta underflow".to_string())
            })?;
        }
        Ok(())
    }
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
    let mut out = Vec::with_capacity(ENTRY_HEADER_LEN + entry.payload_key.len());
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
    if raw.len() < ENTRY_HEADER_LEN {
        return Err(CacheIndexError::Corruption(format!(
            "entry too short for object_id={object_id}: len={}",
            raw.len()
        )));
    }
    if raw[ENTRY_OFFSET_VERSION] != ENTRY_ENCODING_VERSION {
        return Err(CacheIndexError::Corruption(format!(
            "unknown entry encoding version for object_id={object_id}: {}",
            raw[ENTRY_OFFSET_VERSION]
        )));
    }

    let lifecycle = decode_lifecycle(raw[ENTRY_OFFSET_LIFECYCLE]).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "invalid lifecycle value for object_id={object_id}: {}",
            raw[ENTRY_OFFSET_LIFECYCLE]
        ))
    })?;
    let evictable = raw[ENTRY_OFFSET_EVICTABLE] != 0;
    let payload_len = read_u64_field(raw, ENTRY_OFFSET_PAYLOAD_LEN, object_id, "payload_len")?;
    let dirty_order_raw = read_u64_field(raw, ENTRY_OFFSET_DIRTY_ORDER, object_id, "dirty_order")?;
    let access_order = read_u64_field(raw, ENTRY_OFFSET_ACCESS_ORDER, object_id, "access_order")?;
    let payload_key_len = read_u32_field(
        raw,
        ENTRY_OFFSET_PAYLOAD_KEY_LEN,
        object_id,
        "payload_key_len",
    )? as usize;

    if raw.len() != ENTRY_HEADER_LEN + payload_key_len {
        return Err(CacheIndexError::Corruption(format!(
            "payload key length mismatch for object_id={object_id}: encoded={payload_key_len}, total_len={}",
            raw.len()
        )));
    }
    let payload_key = raw[ENTRY_HEADER_LEN..].to_vec();

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

fn read_u64_field(
    raw: &[u8],
    offset: usize,
    object_id: u64,
    field_name: &'static str,
) -> Result<u64, CacheIndexError> {
    let bytes = raw.get(offset..offset + 8).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "missing {field_name} bytes for object_id={object_id} at offset={offset}"
        ))
    })?;
    let array: [u8; 8] = bytes.try_into().map_err(|_| {
        CacheIndexError::Corruption(format!(
            "invalid {field_name} width for object_id={object_id}"
        ))
    })?;
    Ok(u64::from_le_bytes(array))
}

fn read_u32_field(
    raw: &[u8],
    offset: usize,
    object_id: u64,
    field_name: &'static str,
) -> Result<u32, CacheIndexError> {
    let bytes = raw.get(offset..offset + 4).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "missing {field_name} bytes for object_id={object_id} at offset={offset}"
        ))
    })?;
    let array: [u8; 4] = bytes.try_into().map_err(|_| {
        CacheIndexError::Corruption(format!(
            "invalid {field_name} width for object_id={object_id}"
        ))
    })?;
    Ok(u32::from_le_bytes(array))
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

fn touch_access_lru_capacity() -> NonZeroUsize {
    NonZeroUsize::new(TOUCH_ACCESS_LRU_CAPACITY)
        .expect("TOUCH_ACCESS_LRU_CAPACITY must be non-zero")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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

    #[test]
    fn remove_batch_updates_counters_in_single_pass() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_dirty(&index, 1, 10);
        upsert_dirty(&index, 2, 20);
        upsert_uploaded(&index, 3, 30);
        index.mark_uploading(2).unwrap();

        index.remove_batch(&[1, 3, 99]).unwrap();

        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 20);
        assert_eq!(stats.dirty_bytes, 20);
        assert_eq!(stats.evictable_bytes, 0);
        let dirty = index.dirty_uploading_entries().unwrap();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].object_id, 2);
        assert_eq!(dirty[0].payload_key, b"payload/2.block".to_vec());
        assert_eq!(dirty[0].payload_len, 20);
        assert_eq!(dirty[0].lifecycle, LocalBlockLifecycleState::Uploading);
    }

    #[test]
    fn failed_upload_requeues_to_dirty_tail_with_expected_state_accounting() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_dirty(&index, 1, 10);
        upsert_dirty(&index, 2, 20);

        index.mark_uploading(1).unwrap();
        index.mark_upload_failed(1).unwrap();
        let dirty_after_failure = index.dirty_uploading_entries().unwrap();
        assert_eq!(
            dirty_after_failure
                .iter()
                .map(|entry| (entry.object_id, entry.lifecycle))
                .collect::<Vec<_>>(),
            vec![
                (2, LocalBlockLifecycleState::Dirty),
                (1, LocalBlockLifecycleState::Dirty)
            ]
        );

        index.mark_uploading(2).unwrap();
        let dirty_with_inflight = index.dirty_uploading_entries().unwrap();
        assert_eq!(
            dirty_with_inflight
                .iter()
                .map(|entry| (entry.object_id, entry.lifecycle))
                .collect::<Vec<_>>(),
            vec![
                (2, LocalBlockLifecycleState::Uploading),
                (1, LocalBlockLifecycleState::Dirty)
            ]
        );

        index.mark_uploaded(2).unwrap();
        let dirty_after_upload = index.dirty_uploading_entries().unwrap();
        assert_eq!(
            dirty_after_upload
                .iter()
                .map(|entry| (entry.object_id, entry.lifecycle))
                .collect::<Vec<_>>(),
            vec![(1, LocalBlockLifecycleState::Dirty)]
        );

        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 30);
        assert_eq!(stats.dirty_bytes, 10);
        assert_eq!(stats.evictable_bytes, 20);
    }

    #[test]
    fn dirty_and_access_page_apis_allow_chunked_iteration() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        for id in 1..=5 {
            upsert_dirty(&index, id, 1);
        }

        let page1 = index.dirty_uploading_entries_page(None, 2).unwrap();
        let cursor = page1.last().map(|entry| entry.dirty_order);
        let page2 = index.dirty_uploading_entries_page(cursor, 2).unwrap();
        assert_eq!(
            page1
                .iter()
                .map(|entry| entry.object_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            page2
                .iter()
                .map(|entry| entry.object_id)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );

        let access1 = index.access_order_object_ids_page(None, 3).unwrap();
        let access_cursor = access1.last().map(|(_, seq)| *seq);
        let access2 = index
            .access_order_object_ids_page(access_cursor, 3)
            .unwrap();
        assert_eq!(
            access1.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            access2.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![4, 5]
        );
    }

    #[test]
    fn touch_access_if_stale_skips_hot_retouches() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_dirty(&index, 10, 5);
        upsert_uploaded(&index, 11, 5);
        assert_eq!(index.access_order_object_ids().unwrap(), vec![10, 11]);

        assert!(
            index
                .touch_access_if_stale(10, Duration::from_secs(60))
                .unwrap()
        );
        assert_eq!(index.access_order_object_ids().unwrap(), vec![11, 10]);

        assert!(
            !index
                .touch_access_if_stale(10, Duration::from_secs(60))
                .unwrap()
        );
        assert_eq!(index.access_order_object_ids().unwrap(), vec![11, 10]);
    }

    #[test]
    fn touch_tracking_lru_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        let count = TOUCH_ACCESS_LRU_CAPACITY + 32;
        for object_id in 0..count {
            assert!(
                index
                    .touch_access_if_stale(object_id as u64, Duration::ZERO)
                    .unwrap()
            );
        }

        assert_eq!(
            index.last_touch_access.lock().len(),
            TOUCH_ACCESS_LRU_CAPACITY
        );
    }
}

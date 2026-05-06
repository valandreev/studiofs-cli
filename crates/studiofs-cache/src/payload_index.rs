//! Metadata index for the current `v3` local cache backend, backed by redb.
//!
//! The index is the source of truth for payload-object lifecycle state, replay
//! ordering, and space accounting. Accounting counters are updated atomically
//! with row mutations so budget checks never require payload scans.

use std::array;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::Mutex;
use redb::{
    Database, Error as RedbError, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition,
};

use crate::local::{LocalPayloadLifecycleState, LocalPayloadOrigin, LocalPayloadUploadState};

const LEGACY_INDEX_FORMAT_VERSION: u64 = 1;
const INDEX_FORMAT_VERSION: u64 = 2;
const LEGACY_ENTRY_ENCODING_VERSION: u8 = 1;
const ENTRY_ENCODING_VERSION: u8 = 2;
const NO_DIRTY_ORDER: u64 = u64::MAX;

const PAYLOAD_OBJECTS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("payload_objects");
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
const TOUCH_ACCESS_LRU_SHARDS: usize = 16;

const ENTRY_OFFSET_VERSION: usize = 0;
const ENTRY_OFFSET_LIFECYCLE: usize = 1;
const ENTRY_OFFSET_EVICTABLE: usize = 2;
const ENTRY_OFFSET_ORIGIN: usize = 3;
const ENTRY_OFFSET_UPLOAD_STATE: usize = 4;
const ENTRY_OFFSET_PAYLOAD_LEN: usize = 8;
const ENTRY_OFFSET_DIRTY_ORDER: usize = 16;
const ENTRY_OFFSET_ACCESS_ORDER: usize = 24;
const ENTRY_OFFSET_LAST_ACCESS_UNIX_MILLIS: usize = 32;
const ENTRY_OFFSET_PAYLOAD_KEY_LEN: usize = 40;
const ENTRY_HEADER_LEN: usize = 44;
const LEGACY_ENTRY_OFFSET_LIFECYCLE: usize = 1;
const LEGACY_ENTRY_OFFSET_EVICTABLE: usize = 2;
const LEGACY_ENTRY_OFFSET_PAYLOAD_LEN: usize = 4;
const LEGACY_ENTRY_OFFSET_DIRTY_ORDER: usize = 12;
const LEGACY_ENTRY_OFFSET_ACCESS_ORDER: usize = 20;
const LEGACY_ENTRY_OFFSET_PAYLOAD_KEY_LEN: usize = 28;
const LEGACY_ENTRY_HEADER_LEN: usize = 32;

#[derive(Debug, thiserror::Error)]
/// Enum $name.
pub enum CacheIndexError {
    #[error("I/O error: {0}")]
    /// Variant $name.
    Io(#[from] std::io::Error),
    #[error("index storage error: {0}")]
    /// Variant $name.
    Storage(#[source] RedbError),
    #[error("index corruption: {0}")]
    /// Variant $name.
    Corruption(String),
    #[error("unsupported cache index format: expected {expected}, found {found}")]
    /// Field $name.
    UnsupportedFormat {
        /// The expected value.
        expected: u64,
        /// The found value.
        found: u64,
    },
}

impl CacheIndexError {
    pub(crate) fn is_database_lock_conflict(&self) -> bool {
        matches!(self, Self::Storage(RedbError::DatabaseAlreadyOpen))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Struct $name.
pub struct CacheIndexStats {
    /// Field $name.
    pub current_bytes: u64,
    /// Field $name.
    pub dirty_bytes: u64,
    /// Field $name.
    pub evictable_bytes: u64,
}

#[derive(Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CacheIndexEntry {
    /// Field $name.
    pub object_id: u64,
    /// Field $name.
    pub payload_key: Vec<u8>,
    /// Field $name.
    pub payload_len: u64,
    /// Field $name.
    pub lifecycle: LocalPayloadLifecycleState,
    /// Field $name.
    pub origin: LocalPayloadOrigin,
    /// Field $name.
    pub upload_state: LocalPayloadUploadState,
    /// Field $name.
    pub last_access_unix_millis: u64,
    /// Field $name.
    pub evictable: bool,
    /// Field $name.
    pub dirty_order: Option<u64>,
    /// Field $name.
    pub access_order: u64,
}

#[derive(Clone, PartialEq, Eq)]
/// Struct $name.
pub struct CacheIndexUpsert {
    /// Field $name.
    pub object_id: u64,
    /// Field $name.
    pub payload_key: Vec<u8>,
    /// Field $name.
    pub payload_len: u64,
    /// Field $name.
    pub lifecycle: LocalPayloadLifecycleState,
    /// Field $name.
    pub origin: LocalPayloadOrigin,
    /// Field $name.
    pub upload_state: LocalPayloadUploadState,
    /// Field $name.
    pub evictable: bool,
}

#[derive(Clone, PartialEq, Eq)]
/// Struct $name.
pub struct DirtyReplayEntry {
    /// Field $name.
    pub object_id: u64,
    /// Field $name.
    pub payload_key: Vec<u8>,
    /// Field $name.
    pub payload_len: u64,
    /// Field $name.
    pub lifecycle: LocalPayloadLifecycleState,
    /// Field $name.
    pub origin: LocalPayloadOrigin,
    /// Field $name.
    pub upload_state: LocalPayloadUploadState,
    /// Field $name.
    pub dirty_order: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct EvictionCandidateEntry {
    /// Field $name.
    pub object_id: u64,
    /// Field $name.
    pub payload_len: u64,
    /// Field $name.
    pub access_order: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// Struct $name.
pub struct EvictionCandidatePage {
    /// Field $name.
    pub entries: Vec<EvictionCandidateEntry>,
    /// Field $name.
    pub scanned_rows: u64,
    /// Field $name.
    pub skipped_non_evictable: u64,
}

#[derive(Debug)]
/// Struct $name.
pub struct CacheIndex {
    db_path: PathBuf,
    db: Arc<Database>,
    last_touch_access_shards: [Mutex<LruCache<u64, Instant>>; TOUCH_ACCESS_LRU_SHARDS],
}

fn normalize_upsert(mut update: CacheIndexUpsert) -> CacheIndexUpsert {
    if matches!(
        update.lifecycle,
        LocalPayloadLifecycleState::Dirty | LocalPayloadLifecycleState::Uploading
    ) {
        update.evictable = false;
    }
    if matches!(update.upload_state, LocalPayloadUploadState::Uploading) {
        update.lifecycle = LocalPayloadLifecycleState::Uploading;
    } else if matches!(update.upload_state, LocalPayloadUploadState::Pending) {
        update.lifecycle = LocalPayloadLifecycleState::Dirty;
    } else if update.origin == LocalPayloadOrigin::Downloaded {
        update.lifecycle = LocalPayloadLifecycleState::Uploaded;
    }
    update
}

impl CacheIndex {
    /// Function $name.
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
            last_touch_access_shards: array::from_fn(|_| {
                Mutex::new(LruCache::new(touch_access_lru_cap()))
            }),
        };
        index.initialize_schema()?;
        Ok(index)
    }

    /// Function $name.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Function $name.
    pub fn stats(&self) -> Result<CacheIndexStats, CacheIndexError> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;
        Ok(CacheIndexStats {
            current_bytes: required_counter(&counters, COUNTER_CURRENT_BYTES)?,
            dirty_bytes: required_counter(&counters, COUNTER_DIRTY_BYTES)?,
            evictable_bytes: required_counter(&counters, COUNTER_EVICTABLE_BYTES)?,
        })
    }

    /// Function $name.
    pub fn dirty_object_count(&self) -> Result<usize, CacheIndexError> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
        usize::try_from(dirty_order.len().map_err(storage_error)?).map_err(|_| {
            CacheIndexError::Corruption("dirty order length overflowed usize".to_string())
        })
    }

    /// Function $name.
    pub fn get_entry(&self, object_id: u64) -> Result<Option<CacheIndexEntry>, CacheIndexError> {
        let txn = self.db.begin_read().map_err(storage_error)?;
        let payloads = txn
            .open_table(PAYLOAD_OBJECTS_TABLE)
            .map_err(storage_error)?;
        Ok(read_entry_from_payloads(&payloads, object_id)?
            .map(|entry| entry.into_public(object_id)))
    }

    /// Function $name.
    pub fn upsert(&self, mut update: CacheIndexUpsert) -> Result<(), CacheIndexError> {
        update = normalize_upsert(update);

        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut payloads = txn
                .open_table(PAYLOAD_OBJECTS_TABLE)
                .map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let old = read_entry_write(&payloads, update.object_id)?;
            let mut new = StoredEntry {
                payload_key: update.payload_key,
                payload_len: update.payload_len,
                lifecycle: update.lifecycle,
                origin: update.origin,
                upload_state: update.upload_state,
                last_access_unix_millis: now_unix_millis(),
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
                &mut payloads,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    /// Function $name.
    pub fn upsert_many(&self, updates: &[CacheIndexUpsert]) -> Result<(), CacheIndexError> {
        if updates.is_empty() {
            return Ok(());
        }

        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut payloads = txn
                .open_table(PAYLOAD_OBJECTS_TABLE)
                .map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            for update in updates.iter().cloned().map(normalize_upsert) {
                let old = read_entry_write(&payloads, update.object_id)?;
                let mut new = StoredEntry {
                    payload_key: update.payload_key,
                    payload_len: update.payload_len,
                    lifecycle: update.lifecycle,
                    origin: update.origin,
                    upload_state: update.upload_state,
                    last_access_unix_millis: now_unix_millis(),
                    evictable: update.evictable,
                    dirty_order: None,
                    access_order: allocate_sequence(&mut counters, COUNTER_NEXT_ACCESS_SEQ)?,
                };
                if is_dirty_like(new.lifecycle) {
                    new.dirty_order =
                        Some(allocate_sequence(&mut counters, COUNTER_NEXT_DIRTY_SEQ)?);
                }

                apply_entry_change(
                    update.object_id,
                    old.as_ref(),
                    Some(&new),
                    &mut payloads,
                    &mut dirty_order,
                    &mut access_order,
                    &mut counters,
                )?;
            }
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    /// Function $name.
    pub fn touch_access(&self, object_id: u64) -> Result<(), CacheIndexError> {
        self.touch_access_batch(&[object_id])
    }

    /// Batch access-order updates in a single transaction.
    pub fn touch_access_many(
        &self,
        object_ids: &[u64],
        min_interval: Duration,
    ) -> Result<usize, CacheIndexError> {
        self.touch_access_many_impl(object_ids, min_interval, false)
    }

    /// Variant for callers that already guarantee uniqueness.
    pub fn touch_access_many_unique(
        &self,
        object_ids: &[u64],
        min_interval: Duration,
    ) -> Result<usize, CacheIndexError> {
        self.touch_access_many_impl(object_ids, min_interval, true)
    }

    fn touch_access_many_impl(
        &self,
        object_ids: &[u64],
        min_interval: Duration,
        assume_unique: bool,
    ) -> Result<usize, CacheIndexError> {
        if object_ids.is_empty() {
            return Ok(0);
        }

        let now = Instant::now();
        let mut due = Vec::with_capacity(object_ids.len().min(256));
        if assume_unique {
            for object_id in object_ids {
                if self.mark_touch_due(*object_id, now, min_interval) {
                    due.push(*object_id);
                }
            }
        } else {
            let mut seen = HashSet::with_capacity(object_ids.len().min(256));
            for object_id in object_ids {
                if !seen.insert(*object_id) {
                    continue;
                }
                if self.mark_touch_due(*object_id, now, min_interval) {
                    due.push(*object_id);
                }
            }
        }

        if due.is_empty() {
            return Ok(0);
        }
        if let Err(error) = self.touch_access_batch(&due) {
            self.rollback_touch_due(&due);
            return Err(error);
        }
        Ok(due.len())
    }

    /// Function $name.
    pub fn touch_access_if_stale(
        &self,
        object_id: u64,
        min_interval: Duration,
    ) -> Result<bool, CacheIndexError> {
        Ok(self.touch_access_many(&[object_id], min_interval)? == 1)
    }

    fn touch_access_batch(&self, object_ids: &[u64]) -> Result<(), CacheIndexError> {
        if object_ids.is_empty() {
            return Ok(());
        }

        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut payloads = txn
                .open_table(PAYLOAD_OBJECTS_TABLE)
                .map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            for object_id in object_ids {
                let Some(old) = read_entry_write(&payloads, *object_id)? else {
                    continue;
                };
                let mut new = old.clone();
                new.access_order = allocate_sequence(&mut counters, COUNTER_NEXT_ACCESS_SEQ)?;
                new.last_access_unix_millis = now_unix_millis();

                apply_entry_change(
                    *object_id,
                    Some(&old),
                    Some(&new),
                    &mut payloads,
                    &mut dirty_order,
                    &mut access_order,
                    &mut counters,
                )?;
            }
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    /// Function $name.
    pub fn mark_uploading(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut payloads = txn
                .open_table(PAYLOAD_OBJECTS_TABLE)
                .map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_write(&payloads, object_id)? else {
                return Ok(());
            };
            if old.lifecycle != LocalPayloadLifecycleState::Dirty {
                return Ok(());
            }

            let mut new = old.clone();
            new.lifecycle = LocalPayloadLifecycleState::Uploading;
            new.upload_state = LocalPayloadUploadState::Uploading;
            new.evictable = false;
            if new.dirty_order.is_none() {
                new.dirty_order = Some(allocate_sequence(&mut counters, COUNTER_NEXT_DIRTY_SEQ)?);
            }

            apply_entry_change(
                object_id,
                Some(&old),
                Some(&new),
                &mut payloads,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    /// Function $name.
    pub fn mark_upload_failed(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut payloads = txn
                .open_table(PAYLOAD_OBJECTS_TABLE)
                .map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_write(&payloads, object_id)? else {
                return Ok(());
            };
            if old.lifecycle != LocalPayloadLifecycleState::Uploading {
                return Ok(());
            }

            let mut new = old.clone();
            new.lifecycle = LocalPayloadLifecycleState::Dirty;
            new.upload_state = LocalPayloadUploadState::Pending;
            new.evictable = false;
            new.dirty_order = Some(allocate_sequence(&mut counters, COUNTER_NEXT_DIRTY_SEQ)?);

            apply_entry_change(
                object_id,
                Some(&old),
                Some(&new),
                &mut payloads,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    /// Function $name.
    pub fn mark_uploaded(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut payloads = txn
                .open_table(PAYLOAD_OBJECTS_TABLE)
                .map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_write(&payloads, object_id)? else {
                return Ok(());
            };
            let mut new = old.clone();
            new.lifecycle = LocalPayloadLifecycleState::Uploaded;
            new.upload_state = LocalPayloadUploadState::None;
            new.evictable = true;
            new.dirty_order = None;

            apply_entry_change(
                object_id,
                Some(&old),
                Some(&new),
                &mut payloads,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        Ok(())
    }

    /// Function $name.
    pub fn remove(&self, object_id: u64) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut payloads = txn
                .open_table(PAYLOAD_OBJECTS_TABLE)
                .map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let Some(old) = read_entry_write(&payloads, object_id)? else {
                return Ok(());
            };

            apply_entry_change(
                object_id,
                Some(&old),
                None,
                &mut payloads,
                &mut dirty_order,
                &mut access_order,
                &mut counters,
            )?;
        }

        txn.commit().map_err(storage_error)?;
        self.rollback_touch_due(&[object_id]);
        Ok(())
    }

    /// Remove many objects in a single write transaction.
    pub fn remove_batch(&self, object_ids: &[u64]) -> Result<(), CacheIndexError> {
        if object_ids.is_empty() {
            return Ok(());
        }

        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let mut payloads = txn
                .open_table(PAYLOAD_OBJECTS_TABLE)
                .map_err(storage_error)?;
            let mut dirty_order = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let mut access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;
            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;
            let mut batch_delta = AccountingDelta::default();

            for object_id in object_ids {
                let Some(old) = read_entry_write(&payloads, *object_id)? else {
                    continue;
                };
                remove_order_link(&mut dirty_order, old.dirty_order, *object_id, "dirty")?;
                remove_order_link(
                    &mut access_order,
                    Some(old.access_order),
                    *object_id,
                    "access",
                )?;
                let _ = payloads.remove(object_id).map_err(storage_error)?;
                batch_delta.subtract_entry(&old)?;
            }

            if !batch_delta.is_zero() {
                apply_accounting_delta_values(&mut counters, batch_delta)?;
            }
        }

        txn.commit().map_err(storage_error)?;
        self.rollback_touch_due(object_ids);
        Ok(())
    }

    /// Function $name.
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
        let payloads = txn
            .open_table(PAYLOAD_OBJECTS_TABLE)
            .map_err(storage_error)?;
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

            let Some(entry) = read_entry_from_payloads(&payloads, object_id)? else {
                return Err(CacheIndexError::Corruption(format!(
                    "dirty order references missing payload object_id={object_id}"
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
                origin: entry.origin,
                upload_state: entry.upload_state,
                dirty_order: order,
            });
        }

        Ok(ordered)
    }

    /// Function $name.
    pub fn access_order_object_ids(&self) -> Result<Vec<u64>, CacheIndexError> {
        let mut cursor = None;
        let mut out = Vec::new();
        loop {
            let page = self.access_order_page(cursor, LIST_PAGE_SIZE)?;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|(_, order)| *order);
            out.extend(page.into_iter().map(|(object_id, _)| object_id));
        }
        Ok(out)
    }

    /// Page access-order ids by sequence; returns `(object_id, access_order_seq)`.
    pub fn access_order_page(
        &self,
        start_after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<(u64, u64)>, CacheIndexError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let txn = self.db.begin_read().map_err(storage_error)?;
        let payloads = txn
            .open_table(PAYLOAD_OBJECTS_TABLE)
            .map_err(storage_error)?;
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
            if read_entry_from_payloads(&payloads, object_id)?.is_some() {
                ordered.push((object_id, order));
            }
        }
        Ok(ordered)
    }

    /// Function $name.
    pub fn access_order_object_ids_page(
        &self,
        start_after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<(u64, u64)>, CacheIndexError> {
        self.access_order_page(start_after, limit)
    }

    /// Page uploaded+evictable candidates in deterministic access order.
    pub fn eviction_candidates_page(
        &self,
        start_after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<EvictionCandidateEntry>, CacheIndexError> {
        Ok(self
            .eviction_candidates_page_stats(start_after, limit)?
            .entries)
    }

    /// Page uploaded+evictable candidates and include scan stats.
    pub fn eviction_candidates_page_stats(
        &self,
        start_after: Option<u64>,
        limit: usize,
    ) -> Result<EvictionCandidatePage, CacheIndexError> {
        if limit == 0 {
            return Ok(EvictionCandidatePage::default());
        }
        let txn = self.db.begin_read().map_err(storage_error)?;
        let payloads = txn
            .open_table(PAYLOAD_OBJECTS_TABLE)
            .map_err(storage_error)?;
        let access_order = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;

        let start = match start_after {
            Some(last_seen) => match last_seen.checked_add(1) {
                Some(next) => next,
                None => return Ok(EvictionCandidatePage::default()),
            },
            None => 0,
        };

        let mut ordered = Vec::with_capacity(limit);
        let mut scanned_rows = 0_u64;
        let mut skipped_non_evictable = 0_u64;
        let iter = access_order.range::<u64>(start..).map_err(storage_error)?;
        for pair in iter {
            if ordered.len() >= limit {
                break;
            }
            let (order, object_id) = pair.map_err(storage_error)?;
            let order = order.value();
            let object_id = object_id.value();
            scanned_rows = scanned_rows.saturating_add(1);

            let Some(entry) = read_entry_from_payloads(&payloads, object_id)? else {
                continue;
            };
            if entry.access_order != order {
                return Err(CacheIndexError::Corruption(format!(
                    "access order mismatch for object_id={object_id}: table={order}, row={}",
                    entry.access_order
                )));
            }
            if entry.lifecycle == LocalPayloadLifecycleState::Uploaded && entry.evictable {
                ordered.push(EvictionCandidateEntry {
                    object_id,
                    payload_len: entry.payload_len,
                    access_order: order,
                });
            } else {
                skipped_non_evictable = skipped_non_evictable.saturating_add(1);
            }
        }

        Ok(EvictionCandidatePage {
            entries: ordered,
            scanned_rows,
            skipped_non_evictable,
        })
    }

    fn initialize_schema(&self) -> Result<(), CacheIndexError> {
        let txn = self.db.begin_write().map_err(storage_error)?;
        {
            let payloads_exist = {
                let payloads = txn
                    .open_table(PAYLOAD_OBJECTS_TABLE)
                    .map_err(storage_error)?;
                payloads.first().map_err(storage_error)?.is_some()
            };
            let _ = txn.open_table(DIRTY_ORDER_TABLE).map_err(storage_error)?;
            let _ = txn.open_table(ACCESS_ORDER_TABLE).map_err(storage_error)?;

            let mut counters = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;

            let found_index_format = counters
                .get(COUNTER_INDEX_FORMAT)
                .map_err(storage_error)?
                .map(|value| value.value());
            if let Some(found) = found_index_format {
                if found == LEGACY_INDEX_FORMAT_VERSION {
                    counters
                        .insert(COUNTER_INDEX_FORMAT, &INDEX_FORMAT_VERSION)
                        .map_err(storage_error)?;
                } else if found != INDEX_FORMAT_VERSION {
                    return Err(CacheIndexError::UnsupportedFormat {
                        expected: INDEX_FORMAT_VERSION,
                        found,
                    });
                }
            } else if payloads_exist {
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
                    if payloads_exist {
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

impl CacheIndex {
    fn mark_touch_due(&self, object_id: u64, now: Instant, min_interval: Duration) -> bool {
        let mut gate = self.last_touch_access_shards[touch_access_lru_shard(object_id)].lock();
        if let Some(last) = gate.get(&object_id)
            && last.elapsed() < min_interval
        {
            return false;
        }
        gate.put(object_id, now);
        true
    }

    fn rollback_touch_due(&self, object_ids: &[u64]) {
        for object_id in object_ids {
            self.last_touch_access_shards[touch_access_lru_shard(*object_id)]
                .lock()
                .pop(object_id);
        }
    }

    #[cfg(test)]
    fn touch_lru_len(&self) -> usize {
        self.last_touch_access_shards
            .iter()
            .map(|shard| shard.lock().len())
            .sum()
    }
}

#[derive(Clone, PartialEq, Eq)]
struct StoredEntry {
    payload_key: Vec<u8>,
    payload_len: u64,
    lifecycle: LocalPayloadLifecycleState,
    origin: LocalPayloadOrigin,
    upload_state: LocalPayloadUploadState,
    last_access_unix_millis: u64,
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
            origin: self.origin,
            upload_state: self.upload_state,
            last_access_unix_millis: self.last_access_unix_millis,
            evictable: self.evictable,
            dirty_order: self.dirty_order,
            access_order: self.access_order,
        }
    }
}

fn read_entry_from_payloads(
    payloads: &redb::ReadOnlyTable<u64, &[u8]>,
    object_id: u64,
) -> Result<Option<StoredEntry>, CacheIndexError> {
    payloads
        .get(&object_id)
        .map_err(storage_error)?
        .map(|value| decode_entry(value.value(), object_id))
        .transpose()
}

fn read_entry_write(
    payloads: &redb::Table<'_, u64, &[u8]>,
    object_id: u64,
) -> Result<Option<StoredEntry>, CacheIndexError> {
    payloads
        .get(&object_id)
        .map_err(storage_error)?
        .map(|value| decode_entry(value.value(), object_id))
        .transpose()
}

fn apply_entry_change(
    object_id: u64,
    old: Option<&StoredEntry>,
    new: Option<&StoredEntry>,
    payloads: &mut redb::Table<'_, u64, &[u8]>,
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
            payloads
                .insert(&object_id, encoded.as_slice())
                .map_err(storage_error)?;
        }
        None => {
            let _ = payloads.remove(&object_id).map_err(storage_error)?;
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
    let current = counters
        .get(counter_name)
        .map_err(storage_error)?
        .map(|value| value.value())
        .unwrap_or(0);
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

fn is_dirty_like(lifecycle: LocalPayloadLifecycleState) -> bool {
    matches!(
        lifecycle,
        LocalPayloadLifecycleState::Dirty | LocalPayloadLifecycleState::Uploading
    )
}

fn encode_entry(entry: &StoredEntry) -> Result<Vec<u8>, CacheIndexError> {
    let payload_key_len = u32::try_from(entry.payload_key.len())
        .map_err(|_| CacheIndexError::Corruption("payload key exceeds u32 length".to_string()))?;
    let mut out = Vec::with_capacity(ENTRY_HEADER_LEN + entry.payload_key.len());
    out.push(ENTRY_ENCODING_VERSION);
    out.push(encode_lifecycle(entry.lifecycle));
    out.push(u8::from(entry.evictable));
    out.push(encode_origin(entry.origin));
    out.push(encode_upload_state(entry.upload_state));
    out.extend_from_slice(&[0_u8; 3]);
    out.extend_from_slice(&entry.payload_len.to_le_bytes());
    out.extend_from_slice(&entry.dirty_order.unwrap_or(NO_DIRTY_ORDER).to_le_bytes());
    out.extend_from_slice(&entry.access_order.to_le_bytes());
    out.extend_from_slice(&entry.last_access_unix_millis.to_le_bytes());
    out.extend_from_slice(&payload_key_len.to_le_bytes());
    out.extend_from_slice(&entry.payload_key);
    Ok(out)
}

fn decode_entry(raw: &[u8], object_id: u64) -> Result<StoredEntry, CacheIndexError> {
    let Some(&version) = raw.get(ENTRY_OFFSET_VERSION) else {
        return Err(CacheIndexError::Corruption(format!(
            "entry too short for object_id={object_id}: len={}",
            raw.len()
        )));
    };
    match version {
        LEGACY_ENTRY_ENCODING_VERSION => decode_legacy_entry(raw, object_id),
        ENTRY_ENCODING_VERSION => decode_current_entry(raw, object_id),
        _ => Err(CacheIndexError::Corruption(format!(
            "unknown entry encoding version for object_id={object_id}: {version}",
        ))),
    }
}

fn decode_current_entry(raw: &[u8], object_id: u64) -> Result<StoredEntry, CacheIndexError> {
    if raw.len() < ENTRY_HEADER_LEN {
        return Err(CacheIndexError::Corruption(format!(
            "entry too short for object_id={object_id}: len={}",
            raw.len()
        )));
    }

    let lifecycle = decode_lifecycle(raw[ENTRY_OFFSET_LIFECYCLE]).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "invalid lifecycle value for object_id={object_id}: {}",
            raw[ENTRY_OFFSET_LIFECYCLE]
        ))
    })?;
    let evictable = raw[ENTRY_OFFSET_EVICTABLE] != 0;
    let origin = decode_origin(raw[ENTRY_OFFSET_ORIGIN]).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "invalid origin value for object_id={object_id}: {}",
            raw[ENTRY_OFFSET_ORIGIN]
        ))
    })?;
    let upload_state = decode_upload_state(raw[ENTRY_OFFSET_UPLOAD_STATE]).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "invalid upload_state value for object_id={object_id}: {}",
            raw[ENTRY_OFFSET_UPLOAD_STATE]
        ))
    })?;
    let payload_len = read_u64_field(raw, ENTRY_OFFSET_PAYLOAD_LEN, object_id, "payload_len")?;
    let dirty_order_raw = read_u64_field(raw, ENTRY_OFFSET_DIRTY_ORDER, object_id, "dirty_order")?;
    let access_order = read_u64_field(raw, ENTRY_OFFSET_ACCESS_ORDER, object_id, "access_order")?;
    let last_access_unix_millis = read_u64_field(
        raw,
        ENTRY_OFFSET_LAST_ACCESS_UNIX_MILLIS,
        object_id,
        "last_access_unix_millis",
    )?;
    let payload_key_len = usize::try_from(read_u32_field(
        raw,
        ENTRY_OFFSET_PAYLOAD_KEY_LEN,
        object_id,
        "payload_key_len",
    )?)
    .map_err(|_| {
        CacheIndexError::Corruption(format!(
            "payload key length does not fit usize for object_id={object_id}"
        ))
    })?;

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
        origin,
        upload_state,
        last_access_unix_millis,
        evictable,
        dirty_order,
        access_order,
    })
}

fn decode_legacy_entry(raw: &[u8], object_id: u64) -> Result<StoredEntry, CacheIndexError> {
    if raw.len() < LEGACY_ENTRY_HEADER_LEN {
        return Err(CacheIndexError::Corruption(format!(
            "entry too short for object_id={object_id}: len={}",
            raw.len()
        )));
    }

    let lifecycle = decode_lifecycle(raw[LEGACY_ENTRY_OFFSET_LIFECYCLE]).ok_or_else(|| {
        CacheIndexError::Corruption(format!(
            "invalid lifecycle value for object_id={object_id}: {}",
            raw[LEGACY_ENTRY_OFFSET_LIFECYCLE]
        ))
    })?;
    let evictable = raw[LEGACY_ENTRY_OFFSET_EVICTABLE] != 0;
    let payload_len = read_u64_field(
        raw,
        LEGACY_ENTRY_OFFSET_PAYLOAD_LEN,
        object_id,
        "payload_len",
    )?;
    let dirty_order_raw = read_u64_field(
        raw,
        LEGACY_ENTRY_OFFSET_DIRTY_ORDER,
        object_id,
        "dirty_order",
    )?;
    let access_order = read_u64_field(
        raw,
        LEGACY_ENTRY_OFFSET_ACCESS_ORDER,
        object_id,
        "access_order",
    )?;
    let payload_key_len = usize::try_from(read_u32_field(
        raw,
        LEGACY_ENTRY_OFFSET_PAYLOAD_KEY_LEN,
        object_id,
        "payload_key_len",
    )?)
    .map_err(|_| {
        CacheIndexError::Corruption(format!(
            "payload key length does not fit usize for object_id={object_id}"
        ))
    })?;

    if raw.len() != LEGACY_ENTRY_HEADER_LEN + payload_key_len {
        return Err(CacheIndexError::Corruption(format!(
            "payload key length mismatch for object_id={object_id}: encoded={payload_key_len}, total_len={}",
            raw.len()
        )));
    }
    let payload_key = raw[LEGACY_ENTRY_HEADER_LEN..].to_vec();
    let dirty_order = if dirty_order_raw == NO_DIRTY_ORDER {
        None
    } else {
        Some(dirty_order_raw)
    };

    Ok(StoredEntry {
        payload_key,
        payload_len,
        lifecycle,
        origin: infer_legacy_origin(lifecycle, evictable),
        upload_state: infer_legacy_upload_state(lifecycle),
        last_access_unix_millis: 0,
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

fn encode_lifecycle(lifecycle: LocalPayloadLifecycleState) -> u8 {
    match lifecycle {
        LocalPayloadLifecycleState::Missing => 0,
        LocalPayloadLifecycleState::Dirty => 1,
        LocalPayloadLifecycleState::Uploading => 2,
        LocalPayloadLifecycleState::Uploaded => 3,
    }
}

fn decode_lifecycle(raw: u8) -> Option<LocalPayloadLifecycleState> {
    match raw {
        0 => Some(LocalPayloadLifecycleState::Missing),
        1 => Some(LocalPayloadLifecycleState::Dirty),
        2 => Some(LocalPayloadLifecycleState::Uploading),
        3 => Some(LocalPayloadLifecycleState::Uploaded),
        _ => None,
    }
}

fn encode_origin(origin: LocalPayloadOrigin) -> u8 {
    match origin {
        LocalPayloadOrigin::Local => 1,
        LocalPayloadOrigin::Downloaded => 2,
    }
}

fn decode_origin(raw: u8) -> Option<LocalPayloadOrigin> {
    match raw {
        1 => Some(LocalPayloadOrigin::Local),
        2 => Some(LocalPayloadOrigin::Downloaded),
        _ => None,
    }
}

fn encode_upload_state(upload_state: LocalPayloadUploadState) -> u8 {
    match upload_state {
        LocalPayloadUploadState::None => 0,
        LocalPayloadUploadState::Pending => 1,
        LocalPayloadUploadState::Uploading => 2,
    }
}

fn decode_upload_state(raw: u8) -> Option<LocalPayloadUploadState> {
    match raw {
        0 => Some(LocalPayloadUploadState::None),
        1 => Some(LocalPayloadUploadState::Pending),
        2 => Some(LocalPayloadUploadState::Uploading),
        _ => None,
    }
}

fn infer_legacy_origin(
    lifecycle: LocalPayloadLifecycleState,
    evictable: bool,
) -> LocalPayloadOrigin {
    if matches!(
        lifecycle,
        LocalPayloadLifecycleState::Dirty | LocalPayloadLifecycleState::Uploading
    ) || !evictable
    {
        LocalPayloadOrigin::Local
    } else {
        LocalPayloadOrigin::Downloaded
    }
}

fn infer_legacy_upload_state(lifecycle: LocalPayloadLifecycleState) -> LocalPayloadUploadState {
    match lifecycle {
        LocalPayloadLifecycleState::Missing => LocalPayloadUploadState::None,
        LocalPayloadLifecycleState::Dirty => LocalPayloadUploadState::Pending,
        LocalPayloadLifecycleState::Uploading => LocalPayloadUploadState::Uploading,
        LocalPayloadLifecycleState::Uploaded => LocalPayloadUploadState::None,
    }
}

fn now_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn storage_error<E: Into<RedbError>>(error: E) -> CacheIndexError {
    CacheIndexError::Storage(error.into())
}

fn touch_access_lru_cap() -> NonZeroUsize {
    let cap = TOUCH_ACCESS_LRU_CAPACITY
        .div_ceil(TOUCH_ACCESS_LRU_SHARDS)
        .max(1);
    // A capacity of zero is never valid for LruCache; we clamp to 1 so behavior
    // stays defined even under pathological tuning values.
    NonZeroUsize::new(cap).expect("touch access LRU capacity must be non-zero")
}

fn touch_access_lru_shard(object_id: u64) -> usize {
    let shard = object_id % u64::try_from(TOUCH_ACCESS_LRU_SHARDS).unwrap_or(u64::MAX);
    usize::try_from(shard).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::{LocalPayloadOrigin, LocalPayloadUploadState};
    use std::time::Duration;

    fn open_index(path: &Path) -> CacheIndex {
        CacheIndex::open(path).expect("open cache index")
    }

    #[test]
    fn storage_error_detects_redb_lock_conflict_variant() {
        let error = CacheIndexError::Storage(RedbError::DatabaseAlreadyOpen);
        assert!(error.is_database_lock_conflict());

        let other = CacheIndexError::Storage(RedbError::DatabaseClosed);
        assert!(!other.is_database_lock_conflict());
    }

    fn upsert_dirty(index: &CacheIndex, object_id: u64, payload_len: u64) {
        index
            .upsert(CacheIndexUpsert {
                object_id,
                payload_key: format!("payload/{object_id}.object").into_bytes(),
                payload_len,
                lifecycle: LocalPayloadLifecycleState::Dirty,
                origin: LocalPayloadOrigin::Local,
                upload_state: LocalPayloadUploadState::Pending,
                evictable: false,
            })
            .expect("upsert dirty payload object");
    }

    fn upsert_uploaded(index: &CacheIndex, object_id: u64, payload_len: u64) {
        index
            .upsert(CacheIndexUpsert {
                object_id,
                payload_key: format!("payload/{object_id}.object").into_bytes(),
                payload_len,
                lifecycle: LocalPayloadLifecycleState::Uploaded,
                origin: LocalPayloadOrigin::Downloaded,
                upload_state: LocalPayloadUploadState::None,
                evictable: true,
            })
            .expect("upsert uploaded payload object");
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
        assert_eq!(index.dirty_object_count().unwrap(), 1);

        index.mark_uploading(11).unwrap();
        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 125);
        assert_eq!(stats.dirty_bytes, 100);
        assert_eq!(stats.evictable_bytes, 25);
        assert_eq!(index.dirty_object_count().unwrap(), 1);

        index.mark_uploaded(11).unwrap();
        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 125);
        assert_eq!(stats.dirty_bytes, 0);
        assert_eq!(stats.evictable_bytes, 125);
        assert_eq!(index.dirty_object_count().unwrap(), 0);

        index.remove(12).unwrap();
        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 100);
        assert_eq!(stats.dirty_bytes, 0);
        assert_eq!(stats.evictable_bytes, 100);
        assert_eq!(index.dirty_object_count().unwrap(), 0);
    }

    #[test]
    fn upsert_many_batches_accounting_and_lifecycle_updates() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        index
            .upsert_many(&[
                CacheIndexUpsert {
                    object_id: 21,
                    payload_key: b"payload/21.object".to_vec(),
                    payload_len: 64,
                    lifecycle: LocalPayloadLifecycleState::Dirty,
                    origin: LocalPayloadOrigin::Local,
                    upload_state: LocalPayloadUploadState::Pending,
                    evictable: false,
                },
                CacheIndexUpsert {
                    object_id: 22,
                    payload_key: b"payload/22.object".to_vec(),
                    payload_len: 32,
                    lifecycle: LocalPayloadLifecycleState::Uploaded,
                    origin: LocalPayloadOrigin::Downloaded,
                    upload_state: LocalPayloadUploadState::None,
                    evictable: true,
                },
            ])
            .expect("batch upsert");

        let stats = index.stats().expect("index stats after batch upsert");
        assert_eq!(stats.current_bytes, 96);
        assert_eq!(stats.dirty_bytes, 64);
        assert_eq!(stats.evictable_bytes, 32);
        assert_eq!(index.dirty_object_count().unwrap(), 1);

        let dirty = index
            .dirty_uploading_entries()
            .expect("dirty/uploading entries after batch upsert");
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].object_id, 21);
        assert_eq!(dirty[0].payload_len, 64);
        assert_eq!(dirty[0].lifecycle, LocalPayloadLifecycleState::Dirty);
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
                LocalPayloadLifecycleState::Uploading
            );
            assert_eq!(before_restart[1].object_id, 1);
            assert_eq!(
                before_restart[1].lifecycle,
                LocalPayloadLifecycleState::Dirty
            );
        }

        let reopened = open_index(&db_path);
        let after_restart = reopened.dirty_uploading_entries().unwrap();
        assert_eq!(after_restart.len(), 2);
        assert_eq!(after_restart[0].object_id, 2);
        assert_eq!(
            after_restart[0].lifecycle,
            LocalPayloadLifecycleState::Uploading
        );
        assert_eq!(after_restart[1].object_id, 1);
        assert_eq!(
            after_restart[1].lifecycle,
            LocalPayloadLifecycleState::Dirty
        );

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
        assert_eq!(dirty[0].payload_key, b"payload/2.object".to_vec());
        assert_eq!(dirty[0].payload_len, 20);
        assert_eq!(dirty[0].lifecycle, LocalPayloadLifecycleState::Uploading);
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
                (2, LocalPayloadLifecycleState::Dirty),
                (1, LocalPayloadLifecycleState::Dirty)
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
                (2, LocalPayloadLifecycleState::Uploading),
                (1, LocalPayloadLifecycleState::Dirty)
            ]
        );

        index.mark_uploaded(2).unwrap();
        let dirty_after_upload = index.dirty_uploading_entries().unwrap();
        assert_eq!(
            dirty_after_upload
                .iter()
                .map(|entry| (entry.object_id, entry.lifecycle))
                .collect::<Vec<_>>(),
            vec![(1, LocalPayloadLifecycleState::Dirty)]
        );

        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 30);
        assert_eq!(stats.dirty_bytes, 10);
        assert_eq!(stats.evictable_bytes, 20);
    }

    #[test]
    fn remove_while_uploading_clears_entry_and_prevents_requeue() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_dirty(&index, 42, 16);
        index.mark_uploading(42).unwrap();
        index.remove(42).unwrap();

        // A stale upload-failed callback after removal must be a no-op.
        index.mark_upload_failed(42).unwrap();

        assert!(index.get_entry(42).unwrap().is_none());
        assert!(
            index.dirty_uploading_entries().unwrap().is_empty(),
            "removed uploading entry should not remain in replay queue"
        );
        let stats = index.stats().unwrap();
        assert_eq!(stats.current_bytes, 0);
        assert_eq!(stats.dirty_bytes, 0);
        assert_eq!(stats.evictable_bytes, 0);
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

        let access1 = index.access_order_page(None, 3).unwrap();
        let access_cursor = access1.last().map(|(_, seq)| *seq);
        let access2 = index.access_order_page(access_cursor, 3).unwrap();
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
    fn index_persists_origin_upload_state_and_last_access_across_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        index
            .upsert(CacheIndexUpsert {
                object_id: 99,
                payload_key: b"payload/99.object".to_vec(),
                payload_len: 9,
                lifecycle: LocalPayloadLifecycleState::Dirty,
                origin: LocalPayloadOrigin::Local,
                upload_state: LocalPayloadUploadState::Pending,
                evictable: false,
            })
            .unwrap();
        let before_touch = index.get_entry(99).unwrap().unwrap();
        assert_eq!(before_touch.origin, LocalPayloadOrigin::Local);
        assert_eq!(before_touch.upload_state, LocalPayloadUploadState::Pending);
        assert!(before_touch.last_access_unix_millis > 0);

        std::thread::sleep(Duration::from_millis(1));
        index.touch_access(99).unwrap();
        let after_touch = index.get_entry(99).unwrap().unwrap();
        assert!(after_touch.last_access_unix_millis >= before_touch.last_access_unix_millis);

        drop(index);

        let reopened = open_index(&db_path);
        let reopened_entry = reopened.get_entry(99).unwrap().unwrap();
        assert_eq!(reopened_entry.origin, LocalPayloadOrigin::Local);
        assert_eq!(
            reopened_entry.upload_state,
            LocalPayloadUploadState::Pending
        );
        assert_eq!(
            reopened_entry.last_access_unix_millis,
            after_touch.last_access_unix_millis
        );
    }

    #[test]
    fn allocate_sequence_recovers_missing_sequence_counter() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        let txn = index.db.begin_write().unwrap();
        {
            let mut counters = txn.open_table(COUNTERS_TABLE).unwrap();
            counters.remove(COUNTER_NEXT_ACCESS_SEQ).unwrap();
        }
        txn.commit().unwrap();

        upsert_uploaded(&index, 55, 5);
        let entry = index.get_entry(55).unwrap().unwrap();
        assert_eq!(entry.access_order, 0);
    }

    #[test]
    fn touch_access_if_stale_many_batches_unique_ids() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_uploaded(&index, 1, 5);
        upsert_uploaded(&index, 2, 5);
        upsert_uploaded(&index, 3, 5);
        assert_eq!(index.access_order_object_ids().unwrap(), vec![1, 2, 3]);

        assert_eq!(
            index.touch_access_many(&[3, 3, 1], Duration::ZERO).unwrap(),
            2
        );
        assert_eq!(index.access_order_object_ids().unwrap(), vec![2, 3, 1]);

        assert_eq!(
            index
                .touch_access_many(&[3, 1], Duration::from_secs(60))
                .unwrap(),
            0
        );
        assert_eq!(index.access_order_object_ids().unwrap(), vec![2, 3, 1]);
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

        assert_eq!(index.touch_lru_len(), TOUCH_ACCESS_LRU_CAPACITY);
    }

    #[test]
    fn eviction_candidates_filter_to_uploaded_evictable_rows() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_uploaded(&index, 1, 10);
        upsert_dirty(&index, 2, 20);
        upsert_uploaded(&index, 3, 30);
        index.mark_uploading(2).unwrap();

        let ids = index
            .eviction_candidates_page(None, 16)
            .unwrap()
            .into_iter()
            .map(|entry| entry.object_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 3]);

        index.touch_access(1).unwrap();
        let ids_after_touch = index
            .eviction_candidates_page(None, 16)
            .unwrap()
            .into_iter()
            .map(|entry| entry.object_id)
            .collect::<Vec<_>>();
        assert_eq!(ids_after_touch, vec![3, 1]);

        let page = index.eviction_candidates_page_stats(None, 16).unwrap();
        assert_eq!(page.skipped_non_evictable, 1);
        assert_eq!(page.scanned_rows, 3);
    }

    #[test]
    fn eviction_candidates_support_cursor_paging() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("cache.redb");
        let index = open_index(&db_path);

        upsert_uploaded(&index, 10, 1);
        upsert_uploaded(&index, 11, 1);
        upsert_uploaded(&index, 12, 1);

        let page1 = index.eviction_candidates_page(None, 2).unwrap();
        assert_eq!(page1.len(), 2);
        let cursor = page1.last().map(|entry| entry.access_order);
        let page2 = index.eviction_candidates_page(cursor, 2).unwrap();
        assert_eq!(page2.len(), 1);
        assert_eq!(
            page1
                .iter()
                .chain(page2.iter())
                .map(|entry| entry.object_id)
                .collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
    }
}

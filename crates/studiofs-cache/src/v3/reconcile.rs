//! Cache 3.0 startup reconciliation.
//!
//! Startup reconciliation is intentionally scoped to the Cache 3.0 subtree.
//! It never touches legacy `blocks.raw` data under the cache root.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

use super::{CacheIndex, CacheIndexError, CacheV3Layout, FilePayloadStore};

const LIST_PAGE_SIZE: usize = 4_096;
const INDEX_REMOVE_BATCH_SIZE: usize = 1_024;
const STARTUP_RECONCILE_MODE_ENV: &str = "STUDIOFS_CACHE_V3_STARTUP_RECONCILE_MODE";
const STARTUP_RECONCILE_MODE_FULL: &str = "full";

/// Startup reconciliation mode.
///
/// `Lightweight` is the default for mount latency and performs only the checks
/// needed to recover interrupted temp writes and dirty replay viability.
/// `Full` additionally runs expensive global index/payload mismatch sweeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheV3StartupReconcileMode {
    Lightweight,
    Full,
}

impl CacheV3StartupReconcileMode {
    pub fn from_env() -> Self {
        match std::env::var(STARTUP_RECONCILE_MODE_ENV) {
            Ok(value) if value.eq_ignore_ascii_case(STARTUP_RECONCILE_MODE_FULL) => Self::Full,
            _ => Self::Lightweight,
        }
    }
}

/// Result of startup reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheV3StartupReconcileReport {
    pub temp_files_removed: u64,
    pub stale_index_entries_removed: u64,
    pub orphan_payload_files_removed: u64,
}

impl CacheV3StartupReconcileReport {
    pub fn is_clean(self) -> bool {
        self.temp_files_removed == 0
            && self.stale_index_entries_removed == 0
            && self.orphan_payload_files_removed == 0
    }
}

/// Errors returned by startup reconciliation.
#[derive(Debug, thiserror::Error)]
pub enum CacheV3StartupReconcileError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("cache-v3 index error: {0}")]
    Index(#[from] CacheIndexError),
}

/// Reconcile startup state for Cache 3.0.
///
/// `Lightweight` mode:
/// - delete orphan temp files under `v3/tmp/**`;
/// - prune dirty/uploading index entries that reference missing payload files.
///
/// `Full` mode additionally:
/// - scan all index rows and remove stale rows with missing payload;
/// - scan payload files and remove payload orphans with no index entry.
pub fn reconcile_startup_state(
    layout: &CacheV3Layout,
    index: &CacheIndex,
    payload_store: &FilePayloadStore,
    mode: CacheV3StartupReconcileMode,
) -> Result<CacheV3StartupReconcileReport, CacheV3StartupReconcileError> {
    let temp_files_removed = remove_temp_files(layout)?;
    let stale_index_entries_removed = match mode {
        CacheV3StartupReconcileMode::Lightweight => {
            prune_missing_payload_for_dirty_entries(index, payload_store)?
        }
        CacheV3StartupReconcileMode::Full => {
            prune_missing_payload_for_all_entries(index, payload_store)?
        }
    };
    let orphan_payload_files_removed = match mode {
        CacheV3StartupReconcileMode::Lightweight => 0,
        CacheV3StartupReconcileMode::Full => remove_orphan_payload_files(layout, index)?,
    };

    Ok(CacheV3StartupReconcileReport {
        temp_files_removed,
        stale_index_entries_removed,
        orphan_payload_files_removed,
    })
}

fn prune_missing_payload_for_dirty_entries(
    index: &CacheIndex,
    payload_store: &FilePayloadStore,
) -> Result<u64, CacheV3StartupReconcileError> {
    let mut cursor = None;
    let mut removed_total = 0_u64;

    loop {
        let page = index.dirty_uploading_entries_page(cursor, LIST_PAGE_SIZE)?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|entry| entry.dirty_order);

        let mut stale_ids = Vec::new();
        for entry in page {
            if !payload_store.payload_exists(entry.object_id)? {
                stale_ids.push(entry.object_id);
            }
        }
        stale_ids.sort_unstable();
        stale_ids.dedup();

        for chunk in stale_ids.chunks(INDEX_REMOVE_BATCH_SIZE) {
            index.remove_batch(chunk)?;
        }
        removed_total = removed_total.saturating_add(stale_ids.len() as u64);
    }

    Ok(removed_total)
}

fn prune_missing_payload_for_all_entries(
    index: &CacheIndex,
    payload_store: &FilePayloadStore,
) -> Result<u64, CacheV3StartupReconcileError> {
    let index_object_ids = collect_index_object_ids(index)?;
    let mut stale_ids = Vec::new();
    for object_id in index_object_ids {
        if !payload_store.payload_exists(object_id)? {
            stale_ids.push(object_id);
        }
    }

    stale_ids.sort_unstable();
    stale_ids.dedup();
    for chunk in stale_ids.chunks(INDEX_REMOVE_BATCH_SIZE) {
        index.remove_batch(chunk)?;
    }
    Ok(stale_ids.len() as u64)
}

fn remove_temp_files(layout: &CacheV3Layout) -> Result<u64, io::Error> {
    let mut removed = 0_u64;
    for file in list_files_recursive(&layout.tmp_dir)? {
        if remove_file_if_present(&file)? {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

fn remove_orphan_payload_files(
    layout: &CacheV3Layout,
    index: &CacheIndex,
) -> Result<u64, CacheV3StartupReconcileError> {
    let index_object_ids = collect_index_object_ids(index)?;
    let mut removed = 0_u64;
    for file in list_files_recursive(&layout.payload_blocks_dir)? {
        let Some(file_name) = file.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(object_id) = CacheV3Layout::parse_payload_block_file_name(file_name) else {
            continue;
        };
        if !index_object_ids.contains(&object_id) && remove_file_if_present(&file)? {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

fn collect_index_object_ids(index: &CacheIndex) -> Result<HashSet<u64>, CacheIndexError> {
    let mut object_ids = HashSet::new();
    let mut cursor = None;
    loop {
        let page = index.access_order_object_ids_page(cursor, LIST_PAGE_SIZE)?;
        if page.is_empty() {
            break;
        }
        for (object_id, access_order) in page {
            object_ids.insert(object_id);
            cursor = Some(access_order);
        }
    }
    Ok(object_ids)
}

fn list_files_recursive(root: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut files = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for entry in entries {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_dir() {
                dirs.push(path);
            } else if file_type.is_file() {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn remove_file_if_present(path: &Path) -> Result<bool, io::Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::LocalBlockLifecycleState;

    fn create_v3_marker(layout: &CacheV3Layout) {
        std::fs::create_dir_all(&layout.v3_root).unwrap();
        std::fs::write(&layout.layout_marker, b"v1").unwrap();
    }

    fn payload_key(layout: &CacheV3Layout, object_id: u64) -> Vec<u8> {
        layout
            .payload_block_relative_path(object_id)
            .to_string_lossy()
            .into_owned()
            .into_bytes()
    }

    #[test]
    fn startup_reconcile_lightweight_repairs_temp_and_dirty_index_payload_mismatches() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let payload_store = FilePayloadStore::open(layout.clone()).unwrap();

        payload_store.put(1, b"kept").unwrap();
        index
            .upsert(super::super::CacheIndexUpsert {
                object_id: 1,
                payload_key: payload_key(&layout, 1),
                payload_len: 4,
                lifecycle: LocalBlockLifecycleState::Dirty,
                evictable: false,
            })
            .unwrap();
        index.mark_uploading(1).unwrap();

        payload_store.put(2, b"stale").unwrap();
        index
            .upsert(super::super::CacheIndexUpsert {
                object_id: 2,
                payload_key: payload_key(&layout, 2),
                payload_len: 5,
                lifecycle: LocalBlockLifecycleState::Dirty,
                evictable: false,
            })
            .unwrap();
        std::fs::remove_file(layout.payload_block_path(2)).unwrap();

        let tmp_in_tmp_dir = layout.tmp_dir.join("pending").join("upload.tmp");
        std::fs::create_dir_all(tmp_in_tmp_dir.parent().unwrap()).unwrap();
        std::fs::write(&tmp_in_tmp_dir, b"staged").unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            &payload_store,
            CacheV3StartupReconcileMode::Lightweight,
        )
        .unwrap();
        assert_eq!(report.temp_files_removed, 1);
        assert_eq!(report.stale_index_entries_removed, 1);
        assert_eq!(report.orphan_payload_files_removed, 0);

        assert!(index.get_entry(1).unwrap().is_some());
        assert!(index.get_entry(2).unwrap().is_none());
        assert!(!tmp_in_tmp_dir.exists());
    }

    #[test]
    fn startup_reconcile_full_removes_orphan_payload_files() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let payload_store = FilePayloadStore::open(layout.clone()).unwrap();

        payload_store.put(7, b"tracked").unwrap();
        index
            .upsert(super::super::CacheIndexUpsert {
                object_id: 7,
                payload_key: payload_key(&layout, 7),
                payload_len: 7,
                lifecycle: LocalBlockLifecycleState::Uploaded,
                evictable: true,
            })
            .unwrap();

        let orphan_payload_path = layout.payload_block_path(8);
        std::fs::create_dir_all(orphan_payload_path.parent().unwrap()).unwrap();
        std::fs::write(&orphan_payload_path, b"orphan").unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            &payload_store,
            CacheV3StartupReconcileMode::Full,
        )
        .unwrap();

        assert_eq!(report.orphan_payload_files_removed, 1);
        assert!(!payload_store.payload_exists(8).unwrap());
        assert!(payload_store.payload_exists(7).unwrap());
    }

    #[test]
    fn startup_reconcile_never_touches_legacy_raw_data() {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        create_v3_marker(&layout);
        std::fs::create_dir_all(&layout.payload_blocks_dir).unwrap();

        let legacy_raw_path = layout.cache_root.join("blocks.raw");
        std::fs::write(&legacy_raw_path, b"legacy").unwrap();

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let payload_store = FilePayloadStore::open(layout.clone()).unwrap();
        let report = reconcile_startup_state(
            &layout,
            &index,
            &payload_store,
            CacheV3StartupReconcileMode::Lightweight,
        )
        .unwrap();

        assert!(report.is_clean());
        assert_eq!(std::fs::read(&legacy_raw_path).unwrap(), b"legacy");
    }
}

//! Startup reconciliation for the current local block runtime.
//!
//! Startup reconciliation is intentionally scoped to the runtime's current
//! `v3/` subtree implementation detail. It never mutates unrelated files under
//! the cache root.

use std::io;
use std::path::Path;

use super::{CacheIndex, CacheIndexError, LocalBlockLayout};

const LIST_PAGE_SIZE: usize = 4_096;
const INDEX_REMOVE_BATCH_SIZE: usize = 1_024;
const STARTUP_RECONCILE_MODE_ENV: &str = "STUDIOFS_LOCAL_BLOCK_STARTUP_RECONCILE_MODE";
const LEGACY_STARTUP_RECONCILE_MODE_ENV: &str = "STUDIOFS_CACHE_V3_STARTUP_RECONCILE_MODE";
const STARTUP_RECONCILE_MODE_FULL: &str = "full";

/// Startup reconciliation mode.
///
/// `Lightweight` is the default for mount latency and performs only the checks
/// needed to recover interrupted temp writes and dirty replay viability.
/// `Full` additionally runs expensive global index/payload mismatch sweeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalBlockStartupReconcileMode {
    Lightweight,
    Full,
}

impl LocalBlockStartupReconcileMode {
    pub fn from_env() -> Self {
        let configured = std::env::var(STARTUP_RECONCILE_MODE_ENV)
            .ok()
            .or_else(|| std::env::var(LEGACY_STARTUP_RECONCILE_MODE_ENV).ok());
        match configured {
            Some(value) if value.eq_ignore_ascii_case(STARTUP_RECONCILE_MODE_FULL) => Self::Full,
            _ => Self::Lightweight,
        }
    }
}

/// Result of startup reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LocalBlockStartupReconcileReport {
    pub temp_files_removed: u64,
    pub stale_index_entries_removed: u64,
    pub orphan_payload_files_removed: u64,
}

impl LocalBlockStartupReconcileReport {
    pub fn is_clean(self) -> bool {
        self.temp_files_removed == 0
            && self.stale_index_entries_removed == 0
            && self.orphan_payload_files_removed == 0
    }
}

/// Errors returned by startup reconciliation.
#[derive(Debug, thiserror::Error)]
pub enum LocalBlockStartupReconcileError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("local block runtime index error: {0}")]
    Index(#[from] CacheIndexError),
}

/// Reconcile startup state for the current local block runtime.
///
/// `Lightweight` mode:
/// - delete orphan temp files under `v3/tmp/**`;
/// - prune dirty/uploading index entries that reference missing payload files.
///
/// `Full` mode additionally:
/// - scan all index rows and remove stale rows with missing payload;
/// - scan payload files and remove payload orphans with no index entry.
pub fn reconcile_startup_state(
    layout: &LocalBlockLayout,
    index: &CacheIndex,
    mode: LocalBlockStartupReconcileMode,
) -> Result<LocalBlockStartupReconcileReport, LocalBlockStartupReconcileError> {
    let temp_files_removed = remove_temp_files(layout)?;
    let stale_index_entries_removed = match mode {
        LocalBlockStartupReconcileMode::Lightweight => prune_dirty_payloads(layout, index)?,
        LocalBlockStartupReconcileMode::Full => prune_all_payloads(layout, index)?,
    };
    let (additional_temp_files_removed, orphan_payload_files_removed) = match mode {
        LocalBlockStartupReconcileMode::Lightweight => (0, 0),
        LocalBlockStartupReconcileMode::Full => {
            remove_orphan_payloads_and_legacy_temps(layout, index)?
        }
    };

    Ok(LocalBlockStartupReconcileReport {
        temp_files_removed: temp_files_removed.saturating_add(additional_temp_files_removed),
        stale_index_entries_removed,
        orphan_payload_files_removed,
    })
}

fn prune_dirty_payloads(
    layout: &LocalBlockLayout,
    index: &CacheIndex,
) -> Result<u64, LocalBlockStartupReconcileError> {
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
            if !payload_exists(layout, entry.object_id)? {
                stale_ids.push(entry.object_id);
            }
        }
        stale_ids.sort_unstable();
        stale_ids.dedup();

        for chunk in stale_ids.chunks(INDEX_REMOVE_BATCH_SIZE) {
            index.remove_batch(chunk)?;
        }
        removed_total = removed_total.saturating_add(usize_to_u64(stale_ids.len()));
    }

    Ok(removed_total)
}

fn prune_all_payloads(
    layout: &LocalBlockLayout,
    index: &CacheIndex,
) -> Result<u64, LocalBlockStartupReconcileError> {
    let mut cursor = None;
    let mut removed_total = 0_u64;
    loop {
        let page = index.access_order_object_ids_page(cursor, LIST_PAGE_SIZE)?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|(_, access_order)| *access_order);

        let mut stale_ids = Vec::new();
        for (object_id, _) in page {
            if !payload_exists(layout, object_id)? {
                stale_ids.push(object_id);
            }
        }

        stale_ids.sort_unstable();
        stale_ids.dedup();
        for chunk in stale_ids.chunks(INDEX_REMOVE_BATCH_SIZE) {
            index.remove_batch(chunk)?;
        }
        removed_total = removed_total.saturating_add(usize_to_u64(stale_ids.len()));
    }
    Ok(removed_total)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn remove_temp_files(layout: &LocalBlockLayout) -> Result<u64, io::Error> {
    let mut removed = 0_u64;
    visit_files_recursive(&layout.tmp_dir, &mut |file| -> io::Result<()> {
        if remove_file_if_present(file)? {
            removed = removed.saturating_add(1);
        }
        Ok(())
    })?;
    Ok(removed)
}

fn remove_orphan_payloads_and_legacy_temps(
    layout: &LocalBlockLayout,
    index: &CacheIndex,
) -> Result<(u64, u64), LocalBlockStartupReconcileError> {
    let mut temp_files_removed = 0_u64;
    let mut orphan_payload_files_removed = 0_u64;
    visit_files_recursive(&layout.payload_blocks_dir, &mut |file| -> Result<
        (),
        LocalBlockStartupReconcileError,
    > {
        let Some(file_name) = file.file_name().and_then(|name| name.to_str()) else {
            return Ok(());
        };
        if file_name.ends_with(".tmp") {
            if remove_file_if_present(file)? {
                temp_files_removed = temp_files_removed.saturating_add(1);
            }
            return Ok(());
        };
        let Some(object_id) = LocalBlockLayout::parse_payload_block_file_name(file_name) else {
            return Ok(());
        };
        if index.get_entry(object_id)?.is_none() && remove_file_if_present(file)? {
            orphan_payload_files_removed = orphan_payload_files_removed.saturating_add(1);
        }
        Ok(())
    })?;
    Ok((temp_files_removed, orphan_payload_files_removed))
}

fn visit_files_recursive<F, E>(root: &Path, visitor: &mut F) -> Result<(), E>
where
    F: FnMut(&Path) -> Result<(), E>,
    E: From<io::Error>,
{
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            if file_type.is_dir() {
                dirs.push(path);
            } else if file_type.is_file() {
                visitor(&path)?;
            }
        }
    }
    Ok(())
}

fn payload_exists(layout: &LocalBlockLayout, object_id: u64) -> io::Result<bool> {
    layout.payload_block_path(object_id).try_exists()
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
    use super::super::FilePayloadStore;
    use super::*;
    use crate::local::{LocalBlockLifecycleState, LocalBlockOrigin, LocalBlockUploadState};

    fn create_v3_marker(layout: &LocalBlockLayout) {
        std::fs::create_dir_all(&layout.runtime_root).unwrap();
        std::fs::write(&layout.layout_marker, b"v1").unwrap();
    }

    fn payload_key(layout: &LocalBlockLayout, object_id: u64) -> Vec<u8> {
        layout
            .payload_block_relative_path(object_id)
            .to_string_lossy()
            .into_owned()
            .into_bytes()
    }

    #[test]
    fn startup_reconcile_lightweight_repairs_temp_and_dirty_index_payload_mismatches() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
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
                origin: LocalBlockOrigin::Local,
                upload_state: LocalBlockUploadState::Pending,
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
                origin: LocalBlockOrigin::Local,
                upload_state: LocalBlockUploadState::Pending,
                evictable: false,
            })
            .unwrap();
        std::fs::remove_file(layout.payload_block_path(2)).unwrap();

        let tmp_in_tmp_dir = layout.tmp_dir.join("pending").join("upload.tmp");
        std::fs::create_dir_all(tmp_in_tmp_dir.parent().unwrap()).unwrap();
        std::fs::write(&tmp_in_tmp_dir, b"staged").unwrap();

        let report =
            reconcile_startup_state(&layout, &index, LocalBlockStartupReconcileMode::Lightweight)
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
        let layout = LocalBlockLayout::from_cache_root(temp.path());
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
                origin: LocalBlockOrigin::Downloaded,
                upload_state: LocalBlockUploadState::None,
                evictable: true,
            })
            .unwrap();

        let orphan_payload_path = layout.payload_block_path(8);
        std::fs::create_dir_all(orphan_payload_path.parent().unwrap()).unwrap();
        std::fs::write(&orphan_payload_path, b"orphan").unwrap();
        let legacy_tmp_path = layout
            .payload_block_path(9)
            .with_file_name("0000000000000009-legacy.tmp");
        std::fs::create_dir_all(legacy_tmp_path.parent().unwrap()).unwrap();
        std::fs::write(&legacy_tmp_path, b"legacy-tmp").unwrap();

        let report =
            reconcile_startup_state(&layout, &index, LocalBlockStartupReconcileMode::Full).unwrap();

        assert_eq!(report.temp_files_removed, 1);
        assert_eq!(report.orphan_payload_files_removed, 1);
        assert!(!payload_store.payload_exists(8).unwrap());
        assert!(!legacy_tmp_path.exists());
        assert!(payload_store.payload_exists(7).unwrap());
    }

    #[test]
    fn startup_reconcile_never_touches_unrelated_cache_root_files() {
        let temp = tempfile::tempdir().unwrap();
        let layout = LocalBlockLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);
        std::fs::create_dir_all(&layout.payload_blocks_dir).unwrap();

        let unrelated_path = layout.cache_root.join("unrelated.cache-artifact");
        std::fs::write(&unrelated_path, b"keep-me").unwrap();

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let report =
            reconcile_startup_state(&layout, &index, LocalBlockStartupReconcileMode::Lightweight)
                .unwrap();

        assert!(report.is_clean());
        assert_eq!(std::fs::read(&unrelated_path).unwrap(), b"keep-me");
    }
}

//! Startup reconciliation for the current local payload object runtime.
//!
//! Startup reconciliation is intentionally scoped to the runtime root. It never
//! mutates unrelated files under the cache root.

use std::io;
use std::path::{Path, PathBuf};

use super::{
    CacheIndex, CacheIndexError, LOCAL_PAYLOAD_BATCH_INTENT_DIR,
    LOCAL_PAYLOAD_BATCH_INTENT_FILE_EXTENSION, LocalPayloadLayout,
};
use tracing::warn;

const LIST_PAGE_SIZE: usize = 4_096;
const INDEX_REMOVE_BATCH_SIZE: usize = 1_024;
const STARTUP_RECONCILE_MODE_ENV: &str = "STUDIOFS_LOCAL_PAYLOAD_STARTUP_RECONCILE_MODE";
const LEGACY_STARTUP_RECONCILE_MODE_ENV: &str = "STUDIOFS_CACHE_V3_STARTUP_RECONCILE_MODE";
const STARTUP_RECONCILE_MODE_FULL: &str = "full";

/// Startup reconciliation mode.
///
/// `Lightweight` is the default for mount latency and performs only the checks
/// needed to recover interrupted temp writes and dirty replay viability.
/// `Full` additionally runs expensive global index/payload mismatch sweeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPayloadStartupReconcileMode {
    /// Recover only lightweight startup mismatches for fast mount availability.
    ///
    /// This mode repairs interrupted temporary writes and dirty replay state without
    /// running a full payload/index sweep.
    Lightweight,
    /// Run a full startup reconciliation including expensive global integrity sweeps.
    ///
    /// In addition to lightweight repairs, this mode performs full index validation and
    /// orphan payload cleanup to enforce cache consistency.
    Full,
}

impl LocalPayloadStartupReconcileMode {
    /// Read the startup reconciliation mode from environment variables.
    ///
    /// Prefers `STARTUP_RECONCILE_MODE_ENV`, falling back to
    /// `LEGACY_STARTUP_RECONCILE_MODE_ENV` for compatibility. Any value other
    /// than `"full"` (case-insensitive) selects [`LocalPayloadStartupReconcileMode::Lightweight`].
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
pub struct LocalPayloadStartupReconcileReport {
    /// Number of temporary files removed during reconciliation.
    pub temp_files_removed: u64,
    /// Number of stale index entries removed during reconciliation.
    pub stale_index_entries_removed: u64,
    /// Number of orphan payload files removed during reconciliation.
    pub orphan_payload_files_removed: u64,
    /// Number of batch intent files resolved during reconciliation.
    pub batch_intents_resolved: u64,
}

impl LocalPayloadStartupReconcileReport {
    /// Returns `true` when reconciliation did not perform any cleanup actions.
    pub fn is_clean(self) -> bool {
        self.temp_files_removed == 0
            && self.stale_index_entries_removed == 0
            && self.orphan_payload_files_removed == 0
            && self.batch_intents_resolved == 0
    }
}

/// Errors returned by startup reconciliation.
#[derive(Debug, thiserror::Error)]
pub enum LocalPayloadStartupReconcileError {
    /// The filesystem returned an I/O error while scanning or mutating the local cache runtime.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// The local payload index returned an error while validating or mutating index state.
    #[error("local payload object runtime index error: {0}")]
    Index(#[from] CacheIndexError),
}

/// Reconcile startup state for the current local payload object runtime.
///
/// `Lightweight` mode:
/// - delete orphan temp files under `tmp/**`;
/// - prune dirty/uploading index entries that reference missing payload files.
///
/// `Full` mode additionally:
/// - scan all index rows and remove stale rows with missing payload;
/// - scan payload files and remove payload orphans with no index entry.
pub fn reconcile_startup_state(
    layout: &LocalPayloadLayout,
    index: &CacheIndex,
    mode: LocalPayloadStartupReconcileMode,
) -> Result<LocalPayloadStartupReconcileReport, LocalPayloadStartupReconcileError> {
    let batch_recovery = recover_batch_payload_intents(layout, index)?;
    let temp_files_removed = remove_temp_files(layout)?;
    let shard_temp_files_removed = remove_payload_shard_temp_files(layout)?;
    let stale_index_entries_removed = match mode {
        LocalPayloadStartupReconcileMode::Lightweight => prune_dirty_payloads(layout, index)?,
        LocalPayloadStartupReconcileMode::Full => prune_all_payloads(layout, index)?,
    };
    let (additional_temp_files_removed, orphan_payload_files_removed) = match mode {
        LocalPayloadStartupReconcileMode::Lightweight => (0, 0),
        LocalPayloadStartupReconcileMode::Full => {
            remove_orphan_payloads_and_legacy_temps(layout, index)?
        }
    };

    Ok(LocalPayloadStartupReconcileReport {
        temp_files_removed: temp_files_removed
            .saturating_add(shard_temp_files_removed)
            .saturating_add(additional_temp_files_removed)
            .saturating_add(batch_recovery.temp_files_removed),
        stale_index_entries_removed,
        orphan_payload_files_removed,
        batch_intents_resolved: batch_recovery.intents_resolved,
    })
}

#[derive(Debug, Default)]
struct BatchIntentRecoveryReport {
    intents_resolved: u64,
    temp_files_removed: u64,
}

#[derive(Debug, Default)]
struct BatchPayloadIntent {
    phase: String,
    objects: Vec<BatchPayloadIntentObject>,
}

#[derive(Debug, Default)]
struct BatchPayloadIntentObject {
    object_id: u64,
    temp: Option<PathBuf>,
    final_path: Option<PathBuf>,
}

fn recover_batch_payload_intents(
    layout: &LocalPayloadLayout,
    index: &CacheIndex,
) -> Result<BatchIntentRecoveryReport, LocalPayloadStartupReconcileError> {
    let intent_dir = batch_intent_dir(layout);
    let mut report = BatchIntentRecoveryReport::default();

    visit_files_recursive(&intent_dir, &mut |intent_path| -> Result<
        (),
        LocalPayloadStartupReconcileError,
    > {
        let Some(file_name) = intent_path.file_name().and_then(|name| name.to_str()) else {
            return Ok(());
        };
        if !file_name.ends_with(&batch_intent_file_suffix()) {
            return Ok(());
        }

        let Some(intent) = read_batch_payload_intent(layout, intent_path)? else {
            return Ok(());
        };
        for object in &intent.objects {
            if index.get_entry(object.object_id)?.is_none()
                && let Some(final_path) = &object.final_path
            {
                let _ = remove_file_if_present(final_path)?;
            }
            if let Some(temp_path) = &object.temp
                && remove_file_if_present(temp_path)?
            {
                report.temp_files_removed = report.temp_files_removed.saturating_add(1);
            }
        }

        let _ = remove_file_if_present(intent_path)?;
        report.intents_resolved = report.intents_resolved.saturating_add(1);
        Ok(())
    })?;

    Ok(report)
}

fn read_batch_payload_intent(
    layout: &LocalPayloadLayout,
    path: &Path,
) -> Result<Option<BatchPayloadIntent>, LocalPayloadStartupReconcileError> {
    let content = std::fs::read_to_string(path)?;
    let mut intent = BatchPayloadIntent::default();
    let mut saw_version = false;
    for line in content.lines() {
        if let Some(version) = line.strip_prefix("version=") {
            if version != "1" {
                warn!(
                    path = %path.display(),
                    version,
                    "local payload runtime skipped unsupported batch payload intent version"
                );
                return Ok(None);
            }
            saw_version = true;
            continue;
        }
        if let Some(phase) = line.strip_prefix("phase=") {
            intent.phase = phase.to_string();
            continue;
        }
        if let Some(row) = line.strip_prefix("object=")
            && let Some(object) = parse_batch_payload_intent_object(layout, row)
        {
            intent.objects.push(object);
        }
    }
    if !saw_version {
        warn!(
            path = %path.display(),
            "local payload runtime skipped batch payload intent without version header"
        );
        return Ok(None);
    }
    Ok(Some(intent))
}

fn parse_batch_payload_intent_object(
    layout: &LocalPayloadLayout,
    row: &str,
) -> Option<BatchPayloadIntentObject> {
    let mut object_id = None;
    let mut temp = None;
    let mut final_path = None;
    for (idx, part) in row.split('|').enumerate() {
        if idx == 0 {
            object_id = part.parse::<u64>().ok();
        } else if let Some(path_hex) = part.strip_prefix("temp_hex=") {
            temp = decode_batch_intent_path_hex(path_hex)
                .and_then(|path| resolve_batch_intent_relative_path(layout, &path));
        } else if let Some(path_hex) = part.strip_prefix("final_hex=") {
            final_path = decode_batch_intent_path_hex(path_hex)
                .and_then(|path| resolve_batch_intent_relative_path(layout, &path));
        } else if let Some(path) = part.strip_prefix("temp=") {
            temp = resolve_batch_intent_relative_path(layout, path);
        } else if let Some(path) = part.strip_prefix("final=") {
            final_path = resolve_batch_intent_relative_path(layout, path);
        }
    }
    Some(BatchPayloadIntentObject {
        object_id: object_id?,
        temp,
        final_path,
    })
}

fn resolve_batch_intent_relative_path(layout: &LocalPayloadLayout, value: &str) -> Option<PathBuf> {
    if value.is_empty() || value.contains('\\') || value.contains(':') || value.contains("..") {
        return None;
    }
    let mut path = layout.runtime_root.clone();
    for component in value.split('/') {
        if component.is_empty() {
            return None;
        }
        path.push(component);
    }
    Some(path)
}

fn decode_batch_intent_path_hex(value: &str) -> Option<String> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for idx in (0..value.len()).step_by(2) {
        let byte = u8::from_str_radix(&value[idx..idx + 2], 16).ok()?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).ok()
}

fn batch_intent_dir(layout: &LocalPayloadLayout) -> PathBuf {
    layout.tmp_dir.join(LOCAL_PAYLOAD_BATCH_INTENT_DIR)
}

fn batch_intent_file_suffix() -> String {
    format!(".{LOCAL_PAYLOAD_BATCH_INTENT_FILE_EXTENSION}")
}

fn prune_dirty_payloads(
    layout: &LocalPayloadLayout,
    index: &CacheIndex,
) -> Result<u64, LocalPayloadStartupReconcileError> {
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
    layout: &LocalPayloadLayout,
    index: &CacheIndex,
) -> Result<u64, LocalPayloadStartupReconcileError> {
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

fn remove_temp_files(layout: &LocalPayloadLayout) -> Result<u64, io::Error> {
    let mut removed = 0_u64;
    let intent_dir = batch_intent_dir(layout);
    visit_files_recursive(&layout.tmp_dir, &mut |file| -> io::Result<()> {
        if file.starts_with(&intent_dir) {
            return Ok(());
        }
        if remove_file_if_present(file)? {
            removed = removed.saturating_add(1);
        }
        Ok(())
    })?;
    Ok(removed)
}

fn remove_payload_shard_temp_files(layout: &LocalPayloadLayout) -> Result<u64, io::Error> {
    let mut removed = 0_u64;
    visit_files_recursive(&layout.payload_objects_dir, &mut |file| -> io::Result<()> {
        let Some(file_name) = file.file_name().and_then(|name| name.to_str()) else {
            return Ok(());
        };
        if file_name.ends_with(".tmp") && remove_file_if_present(file)? {
            removed = removed.saturating_add(1);
        }
        Ok(())
    })?;
    Ok(removed)
}

fn remove_orphan_payloads_and_legacy_temps(
    layout: &LocalPayloadLayout,
    index: &CacheIndex,
) -> Result<(u64, u64), LocalPayloadStartupReconcileError> {
    let mut temp_files_removed = 0_u64;
    let mut orphan_payload_files_removed = 0_u64;
    visit_files_recursive(&layout.payload_objects_dir, &mut |file| -> Result<
        (),
        LocalPayloadStartupReconcileError,
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
        let Some(object_id) = LocalPayloadLayout::parse_payload_object_file_name(file_name) else {
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

fn payload_exists(layout: &LocalPayloadLayout, object_id: u64) -> io::Result<bool> {
    layout.payload_object_path(object_id).try_exists()
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
    use crate::local::{LocalPayloadLifecycleState, LocalPayloadOrigin, LocalPayloadUploadState};

    fn create_v3_marker(layout: &LocalPayloadLayout) {
        layout.provision().unwrap();
    }

    fn payload_key(layout: &LocalPayloadLayout, object_id: u64) -> Vec<u8> {
        layout
            .payload_object_relative_path(object_id)
            .to_string_lossy()
            .into_owned()
            .into_bytes()
    }

    #[test]
    fn startup_reconcile_lightweight_repairs_temp_and_dirty_index_payload_mismatches() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let payload_store = FilePayloadStore::open(layout.clone()).unwrap();

        payload_store.put(1, b"kept").unwrap();
        index
            .upsert(super::super::CacheIndexUpsert {
                object_id: 1,
                payload_key: payload_key(&layout, 1),
                payload_len: 4,
                lifecycle: LocalPayloadLifecycleState::Dirty,
                origin: LocalPayloadOrigin::Local,
                upload_state: LocalPayloadUploadState::Pending,
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
                lifecycle: LocalPayloadLifecycleState::Dirty,
                origin: LocalPayloadOrigin::Local,
                upload_state: LocalPayloadUploadState::Pending,
                evictable: false,
            })
            .unwrap();
        std::fs::remove_file(layout.payload_object_path(2)).unwrap();

        let tmp_in_tmp_dir = layout.tmp_dir.join("pending").join("upload.tmp");
        std::fs::create_dir_all(tmp_in_tmp_dir.parent().unwrap()).unwrap();
        std::fs::write(&tmp_in_tmp_dir, b"staged").unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            LocalPayloadStartupReconcileMode::Lightweight,
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
    fn startup_reconcile_lightweight_resolves_incomplete_batch_payload_intent() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();

        let committed_payload_path = layout.payload_object_path(10);
        std::fs::create_dir_all(committed_payload_path.parent().unwrap()).unwrap();
        std::fs::write(&committed_payload_path, b"committed-without-index").unwrap();

        let staged_temp_path = layout.tmp_dir.join("batch-staged-11.tmp");
        std::fs::create_dir_all(staged_temp_path.parent().unwrap()).unwrap();
        std::fs::write(&staged_temp_path, b"staged").unwrap();

        let intent_dir = layout.tmp_dir.join("batch-intents");
        std::fs::create_dir_all(&intent_dir).unwrap();
        let final_path_10 = layout
            .payload_object_relative_path(10)
            .to_string_lossy()
            .replace('\\', "/");
        let temp_path_11 = staged_temp_path
            .strip_prefix(&layout.runtime_root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        std::fs::write(
            intent_dir.join("batch-0000000000000001.intent"),
            format!(
                "version=1\nphase=committing\nobject=10|temp=|final={final_path_10}\nobject=11|temp={temp_path_11}|final=\n"
            ),
        )
        .unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            LocalPayloadStartupReconcileMode::Lightweight,
        )
        .unwrap();

        assert_eq!(report.batch_intents_resolved, 1);
        assert_eq!(report.temp_files_removed, 1);
        assert!(!committed_payload_path.exists());
        assert!(!staged_temp_path.exists());
        assert!(!intent_dir.join("batch-0000000000000001.intent").exists());
    }

    #[test]
    fn startup_reconcile_lightweight_removes_payload_shard_temp_files() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let shard_temp_path = layout
            .payload_object_path(11)
            .with_file_name("000000000000000b-staged.tmp");
        std::fs::create_dir_all(shard_temp_path.parent().unwrap()).unwrap();
        std::fs::write(&shard_temp_path, b"staged").unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            LocalPayloadStartupReconcileMode::Lightweight,
        )
        .unwrap();

        assert_eq!(report.temp_files_removed, 1);
        assert!(!shard_temp_path.exists());
    }

    #[test]
    fn startup_reconcile_batch_intent_preserves_payloads_that_reached_index() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let payload_store = FilePayloadStore::open(layout.clone()).unwrap();
        payload_store.put(12, b"indexed").unwrap();
        index
            .upsert(super::super::CacheIndexUpsert {
                object_id: 12,
                payload_key: payload_key(&layout, 12),
                payload_len: 7,
                lifecycle: LocalPayloadLifecycleState::Uploaded,
                origin: LocalPayloadOrigin::Downloaded,
                upload_state: LocalPayloadUploadState::None,
                evictable: true,
            })
            .unwrap();

        let final_path_12 = layout
            .payload_object_relative_path(12)
            .to_string_lossy()
            .replace('\\', "/");
        let intent_dir = layout.tmp_dir.join("batch-intents");
        std::fs::create_dir_all(&intent_dir).unwrap();
        std::fs::write(
            intent_dir.join("batch-0000000000000002.intent"),
            format!("version=1\nphase=committed\nobject=12|temp=|final={final_path_12}\n"),
        )
        .unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            LocalPayloadStartupReconcileMode::Lightweight,
        )
        .unwrap();

        assert_eq!(report.batch_intents_resolved, 1);
        assert_eq!(payload_store.get(12).unwrap().as_ref(), b"indexed");
        assert!(!intent_dir.join("batch-0000000000000002.intent").exists());
    }

    #[test]
    fn startup_reconcile_skips_unsupported_batch_intent_version() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let payload_path = layout.payload_object_path(14);
        std::fs::create_dir_all(payload_path.parent().unwrap()).unwrap();
        std::fs::write(&payload_path, b"future-format-payload").unwrap();

        let final_path_14 = layout
            .payload_object_relative_path(14)
            .to_string_lossy()
            .replace('\\', "/");
        let intent_dir = batch_intent_dir(&layout);
        let intent_path = intent_dir.join("batch-0000000000000004.intent");
        std::fs::create_dir_all(&intent_dir).unwrap();
        std::fs::write(
            &intent_path,
            format!("version=2\nphase=committing\nobject=14|temp=|final={final_path_14}\n"),
        )
        .unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            LocalPayloadStartupReconcileMode::Lightweight,
        )
        .unwrap();

        assert_eq!(report.batch_intents_resolved, 0);
        assert_eq!(
            std::fs::read(&payload_path).unwrap(),
            b"future-format-payload"
        );
        assert!(intent_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn startup_reconcile_batch_intent_accepts_hex_encoded_paths_with_delimiters() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let payload_path = layout
            .payload_objects_dir
            .join("aa")
            .join("bb")
            .join("contains|delimiter.object");
        std::fs::create_dir_all(payload_path.parent().unwrap()).unwrap();
        std::fs::write(&payload_path, b"payload").unwrap();
        let final_path = payload_path
            .strip_prefix(&layout.runtime_root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let final_path_hex = encode_test_path_hex(&final_path);

        let intent_dir = batch_intent_dir(&layout);
        std::fs::create_dir_all(&intent_dir).unwrap();
        std::fs::write(
            intent_dir.join("batch-0000000000000005.intent"),
            format!(
                "version=1\nphase=committing\nobject=15|temp_hex=|final_hex={final_path_hex}\n"
            ),
        )
        .unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            LocalPayloadStartupReconcileMode::Lightweight,
        )
        .unwrap();

        assert_eq!(report.batch_intents_resolved, 1);
        assert!(!payload_path.exists());
    }

    #[cfg(windows)]
    #[test]
    fn startup_reconcile_batch_intent_rejects_rooted_paths() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let outside = studiofs_test_support::workspace_target_tempdir("test-");
        let outside_path = outside.path().join("outside.object");
        std::fs::write(&outside_path, b"outside-runtime").unwrap();

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let intent_dir = layout.tmp_dir.join("batch-intents");
        std::fs::create_dir_all(&intent_dir).unwrap();
        std::fs::write(
            intent_dir.join("batch-0000000000000003.intent"),
            format!(
                "version=1\nphase=committing\nobject=13|temp=|final={}\n",
                outside_path.to_string_lossy()
            ),
        )
        .unwrap();

        let report = reconcile_startup_state(
            &layout,
            &index,
            LocalPayloadStartupReconcileMode::Lightweight,
        )
        .unwrap();

        assert_eq!(report.batch_intents_resolved, 1);
        assert_eq!(std::fs::read(&outside_path).unwrap(), b"outside-runtime");
    }

    #[cfg(unix)]
    fn encode_test_path_hex(value: &str) -> String {
        let mut encoded = String::with_capacity(value.len() * 2);
        for byte in value.as_bytes() {
            encoded.push_str(&format!("{byte:02x}"));
        }
        encoded
    }

    #[test]
    fn startup_reconcile_full_removes_orphan_payload_files() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let payload_store = FilePayloadStore::open(layout.clone()).unwrap();

        payload_store.put(7, b"tracked").unwrap();
        index
            .upsert(super::super::CacheIndexUpsert {
                object_id: 7,
                payload_key: payload_key(&layout, 7),
                payload_len: 7,
                lifecycle: LocalPayloadLifecycleState::Uploaded,
                origin: LocalPayloadOrigin::Downloaded,
                upload_state: LocalPayloadUploadState::None,
                evictable: true,
            })
            .unwrap();

        let orphan_payload_path = layout.payload_object_path(8);
        std::fs::create_dir_all(orphan_payload_path.parent().unwrap()).unwrap();
        std::fs::write(&orphan_payload_path, b"orphan").unwrap();
        let legacy_tmp_path = layout
            .payload_object_path(9)
            .with_file_name("0000000000000009-legacy.tmp");
        std::fs::create_dir_all(legacy_tmp_path.parent().unwrap()).unwrap();
        std::fs::write(&legacy_tmp_path, b"legacy-tmp").unwrap();

        let report =
            reconcile_startup_state(&layout, &index, LocalPayloadStartupReconcileMode::Full)
                .unwrap();

        assert_eq!(report.temp_files_removed, 1);
        assert_eq!(report.orphan_payload_files_removed, 1);
        assert!(!payload_store.payload_exists(8).unwrap());
        assert!(!legacy_tmp_path.exists());
        assert!(payload_store.payload_exists(7).unwrap());
    }

    #[test]
    fn startup_reconcile_never_touches_unrelated_cache_root_files() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        create_v3_marker(&layout);
        std::fs::create_dir_all(&layout.payload_objects_dir).unwrap();

        let unrelated_path = layout.cache_root.join("unrelated.cache-artifact");
        std::fs::write(&unrelated_path, b"keep-me").unwrap();

        let index = CacheIndex::open(&layout.redb_index_path).unwrap();
        let report = reconcile_startup_state(
            &layout,
            &index,
            LocalPayloadStartupReconcileMode::Lightweight,
        )
        .unwrap();

        assert!(report.is_clean());
        assert_eq!(std::fs::read(&unrelated_path).unwrap(), b"keep-me");
    }
}

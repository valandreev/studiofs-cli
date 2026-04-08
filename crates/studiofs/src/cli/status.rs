//! `studiofs status` — show filesystem status.

use crate::cli::utils::format_bytes;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use studiofs_cache::v2::{LocalBlockBackend, open_legacy_local_block_backend};
use studiofs_cache::v3::{
    CACHE_V3_DEFAULT_ALLOWED, CACHE_V3_DEFAULT_GUARDRAIL, CACHE_V3_FINAL_VALIDATION_ISSUE,
    CacheBackendDetection, CacheBackendKind, CacheIndex, detect_cache_backends,
};
use studiofs_cache::{OpenLocalBlockBackendError, open_local_block_backend};
use studiofs_meta::counters;
#[cfg(test)]
use uuid::Uuid;

const INGRESS_SEGMENT_MAGIC: [u8; 4] = *b"ISG1";

const INGRESS_RECORD_PUT: u8 = 1;
const INGRESS_RECORD_INVALIDATE: u8 = 2;
const INGRESS_RECORD_HEADER_LEN: usize = 37;
const RAW_FILE_MAGIC: [u8; 8] = *b"STFSRAWF";
const RAW_RECORD_MAGIC: [u8; 8] = *b"STFSRAWR";
const RAW_FILE_VERSION: u32 = 1;
const RAW_FILE_HEADER_LEN: u64 = 32;
const RAW_RECORD_HEADER_LEN: u64 = 32;
const RAW_RECORD_KIND_PUT: u8 = 1;
const RAW_RECORD_KIND_STATE: u8 = 2;
const RAW_RECORD_KIND_REMOVE: u8 = 3;
const RAW_STATE_DIRTY: u8 = 1;
const RAW_STATE_UPLOADING: u8 = 2;
const RAW_STATE_UPLOADED: u8 = 3;

#[derive(Debug, Default)]
struct SpillDirStats {
    records: usize,
    bytes: u64,
    oldest_age_secs: u64,
    checkpoint_age_secs: u64,
    checkpoint_segment_id: u64,
    checkpoint_offset: u64,
}

#[derive(Debug, Clone)]
struct SegmentRecordStat {
    seq: u64,
    len: usize,
    created_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct CacheUsageStats {
    usage_bytes: u64,
    dirty_bytes: u64,
    evictable_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
enum LegacyRawLifecycle {
    Dirty,
    Uploading,
    Uploaded,
}

#[derive(Debug, Clone, Copy)]
struct LegacyRawEntry {
    data_len: u64,
    lifecycle: LegacyRawLifecycle,
    evictable: bool,
}

fn local_cache_path(meta_dir: &Path, cache_dir: Option<&Path>, volume_uuid: &str) -> PathBuf {
    let base = cache_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| meta_dir.join("cache"));
    base.join(volume_uuid)
}

fn detect_cache_backends_with_fallback(
    cache_path: &Path,
    detector: impl FnOnce(&Path) -> std::io::Result<CacheBackendDetection>,
) -> Option<CacheBackendDetection> {
    match detector(cache_path) {
        Ok(detection) => Some(detection),
        Err(error) => {
            eprintln!(
                "warning: failed to inspect cache backend contract at '{}': {error}",
                cache_path.display()
            );
            None
        }
    }
}

fn detect_cache_backends_for_status(cache_path: &Path) -> Option<CacheBackendDetection> {
    detect_cache_backends_with_fallback(cache_path, detect_cache_backends)
}

fn map_status_open_local_backend_error(
    cache_dir: &Path,
    source: OpenLocalBlockBackendError,
) -> anyhow::Error {
    anyhow::anyhow!(
        "failed to open local cache backend at '{}': {source}",
        cache_dir.display()
    )
}

fn decode_legacy_raw_lifecycle(state: u8) -> Option<LegacyRawLifecycle> {
    match state {
        RAW_STATE_DIRTY => Some(LegacyRawLifecycle::Dirty),
        RAW_STATE_UPLOADING => Some(LegacyRawLifecycle::Uploading),
        RAW_STATE_UPLOADED => Some(LegacyRawLifecycle::Uploaded),
        _ => None,
    }
}

fn scan_cache_v3_usage(
    cache_backend_detection: &CacheBackendDetection,
) -> anyhow::Result<CacheUsageStats> {
    // Status is allowed to open the index directly to read aggregate counters.
    let index = CacheIndex::open(&cache_backend_detection.layout.redb_index_path)?;
    let stats = index.stats()?;
    Ok(CacheUsageStats {
        usage_bytes: stats.current_bytes,
        dirty_bytes: stats.dirty_bytes,
        evictable_bytes: stats.evictable_bytes,
    })
}

fn scan_legacy_raw_cache_usage(raw_data_path: &Path) -> anyhow::Result<CacheUsageStats> {
    if !raw_data_path.exists() {
        return Ok(CacheUsageStats::default());
    }

    let mut file = std::fs::File::open(raw_data_path)?;
    let file_len = file.metadata()?.len();
    if file_len < RAW_FILE_HEADER_LEN {
        anyhow::bail!(
            "legacy raw cache file '{}' is shorter than the header",
            raw_data_path.display()
        );
    }

    let mut header = [0_u8; RAW_FILE_HEADER_LEN as usize];
    file.read_exact(&mut header)?;
    if header[0..8] != RAW_FILE_MAGIC {
        anyhow::bail!(
            "legacy raw cache file '{}' has an invalid magic",
            raw_data_path.display()
        );
    }
    let version = u32::from_le_bytes(header[8..12].try_into().expect("version bytes"));
    if version != RAW_FILE_VERSION {
        anyhow::bail!(
            "legacy raw cache file '{}' has unsupported version {}",
            raw_data_path.display(),
            version
        );
    }

    let mut entries: HashMap<u64, LegacyRawEntry> = HashMap::new();
    let mut cursor = RAW_FILE_HEADER_LEN;

    while cursor + RAW_RECORD_HEADER_LEN <= file_len {
        file.seek(std::io::SeekFrom::Start(cursor))?;
        let mut record = [0_u8; RAW_RECORD_HEADER_LEN as usize];
        if let Err(error) = file.read_exact(&mut record) {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(error.into());
        }
        if record[0..8] != RAW_RECORD_MAGIC {
            break;
        }
        let record_version =
            u32::from_le_bytes(record[8..12].try_into().expect("record version bytes"));
        if record_version != RAW_FILE_VERSION {
            break;
        }

        let kind = record[12];
        let state = record[13];
        let evictable = record[14] != 0;
        let object_id = u64::from_le_bytes(record[16..24].try_into().expect("object id bytes"));
        let data_len = u64::from_le_bytes(record[24..32].try_into().expect("data len bytes"));

        match kind {
            RAW_RECORD_KIND_PUT => {
                let data_end = cursor
                    .saturating_add(RAW_RECORD_HEADER_LEN)
                    .saturating_add(data_len);
                if data_end > file_len {
                    break;
                }
                let lifecycle =
                    decode_legacy_raw_lifecycle(state).unwrap_or(LegacyRawLifecycle::Dirty);
                let evictable = matches!(lifecycle, LegacyRawLifecycle::Uploaded) && evictable;
                entries.insert(
                    object_id,
                    LegacyRawEntry {
                        data_len,
                        lifecycle,
                        evictable,
                    },
                );
                cursor = data_end;
            }
            RAW_RECORD_KIND_STATE => {
                if let Some(entry) = entries.get_mut(&object_id)
                    && let Some(lifecycle) = decode_legacy_raw_lifecycle(state)
                {
                    entry.lifecycle = lifecycle;
                    entry.evictable = matches!(lifecycle, LegacyRawLifecycle::Uploaded);
                }
                cursor = cursor.saturating_add(RAW_RECORD_HEADER_LEN);
            }
            RAW_RECORD_KIND_REMOVE => {
                entries.remove(&object_id);
                cursor = cursor.saturating_add(RAW_RECORD_HEADER_LEN);
            }
            _ => break,
        }
    }

    let mut stats = CacheUsageStats::default();
    for entry in entries.values() {
        stats.usage_bytes = stats.usage_bytes.saturating_add(entry.data_len);
        if matches!(
            entry.lifecycle,
            LegacyRawLifecycle::Dirty | LegacyRawLifecycle::Uploading
        ) {
            stats.dirty_bytes = stats.dirty_bytes.saturating_add(entry.data_len);
        }
        if matches!(entry.lifecycle, LegacyRawLifecycle::Uploaded) && entry.evictable {
            stats.evictable_bytes = stats.evictable_bytes.saturating_add(entry.data_len);
        }
    }

    Ok(stats)
}

fn scan_cache_usage(
    cache_path: &Path,
    cache_backend_detection: Option<&CacheBackendDetection>,
) -> anyhow::Result<CacheUsageStats> {
    let selected_backend = cache_backend_detection
        .map(CacheBackendDetection::selected_mount_backend)
        .unwrap_or(CacheBackendKind::LegacyRawV2);

    match selected_backend {
        CacheBackendKind::LegacyRawV2 => {
            scan_legacy_raw_cache_usage(&cache_path.join("blocks.raw"))
        }
        CacheBackendKind::CacheV3 => {
            let detection = cache_backend_detection.ok_or_else(|| {
                anyhow::anyhow!("cache-v3 backend selected but backend detection is unavailable")
            })?;
            scan_cache_v3_usage(detection)
        }
    }
}

fn scan_spill_dir(spill_dir: &Path) -> anyhow::Result<SpillDirStats> {
    let mut stats = SpillDirStats::default();
    let now = std::time::SystemTime::now();
    let mut live = std::collections::HashMap::<(u64, u32), SegmentRecordStat>::new();

    let segment_dir = spill_dir.join("segments");
    let segment_entries = match std::fs::read_dir(&segment_dir) {
        Ok(entries) => Some(entries),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    if let Some(entries) = segment_entries {
        let mut files: Vec<(u64, PathBuf)> = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() || path.extension().is_none_or(|ext| ext != "seg") {
                    return None;
                }
                let stem = path.file_stem()?.to_str()?;
                let id = u64::from_str_radix(stem, 16).ok()?;
                Some((id, path))
            })
            .collect();
        files.sort_unstable_by_key(|(id, _)| *id);

        for (_id, path) in files {
            let mut file = std::fs::File::open(path)?;
            loop {
                let mut header = [0u8; INGRESS_RECORD_HEADER_LEN];
                match std::io::Read::read_exact(&mut file, &mut header) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }
                if header[0..4] != INGRESS_SEGMENT_MAGIC {
                    anyhow::bail!("invalid ingress segment magic in status scan");
                }
                let kind = header[4];
                let ino = u64::from_le_bytes(header[5..13].try_into().unwrap());
                let block_index = u32::from_le_bytes(header[13..17].try_into().unwrap());
                let seq = u64::from_le_bytes(header[17..25].try_into().unwrap());
                let created_unix_ms = u64::from_le_bytes(header[25..33].try_into().unwrap());
                let payload_len = u32::from_le_bytes(header[33..37].try_into().unwrap()) as usize;
                if payload_len > 0 {
                    std::io::Seek::seek(&mut file, std::io::SeekFrom::Current(payload_len as i64))?;
                }
                let key = (ino, block_index);
                match kind {
                    INGRESS_RECORD_PUT => {
                        let should_replace = live
                            .get(&key)
                            .map(|current| seq >= current.seq)
                            .unwrap_or(true);
                        if should_replace {
                            live.insert(
                                key,
                                SegmentRecordStat {
                                    seq,
                                    len: payload_len,
                                    created_unix_ms,
                                },
                            );
                        }
                    }
                    INGRESS_RECORD_INVALIDATE => {
                        let should_replace = live
                            .get(&key)
                            .map(|current| seq >= current.seq)
                            .unwrap_or(true);
                        if should_replace {
                            live.remove(&key);
                        }
                    }
                    _ => anyhow::bail!("unknown ingress segment record kind in status scan"),
                }
            }
        }
    }

    for record in live.values() {
        stats.records = stats.records.saturating_add(1);
        stats.bytes = stats.bytes.saturating_add(record.len as u64);
        let modified = UNIX_EPOCH + std::time::Duration::from_millis(record.created_unix_ms);
        let age_secs = now
            .duration_since(modified)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        stats.oldest_age_secs = stats.oldest_age_secs.max(age_secs);
    }

    let checkpoint_path = spill_dir.join("checkpoints").join("latest.json");
    if let Ok(bytes) = std::fs::read(&checkpoint_path) {
        let checkpoint: Value = serde_json::from_slice(&bytes)?;
        stats.checkpoint_segment_id = checkpoint
            .get("covered_segment_id")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        stats.checkpoint_offset = checkpoint
            .get("covered_offset")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if let Ok(meta) = std::fs::metadata(&checkpoint_path)
            && let Ok(modified) = meta.modified()
        {
            stats.checkpoint_age_secs = now
                .duration_since(modified)
                .map(|d| d.as_secs())
                .unwrap_or(0);
        }
    }

    Ok(stats)
}

fn scan_pending_uploads(
    cache_dir: &Path,
    volume_uuid: &str,
    cache_backend_detection: Option<&CacheBackendDetection>,
) -> anyhow::Result<usize> {
    let data_path = cache_dir.join("blocks.raw");
    let selected_backend = cache_backend_detection
        .map(CacheBackendDetection::selected_mount_backend)
        .unwrap_or(CacheBackendKind::LegacyRawV2);

    if matches!(selected_backend, CacheBackendKind::LegacyRawV2) && !data_path.exists() {
        return Ok(0);
    };

    let store: Arc<dyn LocalBlockBackend> = match cache_backend_detection {
        Some(_) => {
            open_local_block_backend(cache_dir, volume_uuid)
                .map_err(|source| map_status_open_local_backend_error(cache_dir, source))?
                .backend
        }
        None => Arc::new(
            open_legacy_local_block_backend(cache_dir, volume_uuid).map_err(anyhow::Error::from)?,
        ),
    };
    Ok(store.dirty_object_ids().len())
}

pub fn run(name: &str, meta_dir: &Path, cache_dir: Option<&Path>) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));

    if !db_path.exists() {
        println!("✗ Volume '{}' not found at {:?}", name, db_path);
        println!("  Run `studiofs init {}` first.", name);
        return Ok(());
    }

    let engine = studiofs_meta::open_meta_engine(&db_path)?;
    let format_info = engine.get_format_info()?;

    let total_inodes = engine.get_counter(counters::TOTAL_INODES)?;
    let used_space = engine.get_counter(counters::USED_SPACE)?;
    let next_inode = engine.get_counter(counters::NEXT_INODE)?;
    let next_block = engine.get_counter(counters::NEXT_BLOCK_ID)?;

    let cache_path = local_cache_path(meta_dir, cache_dir, &format_info.uuid);
    let cache_backend_detection = detect_cache_backends_for_status(&cache_path);
    let spill_stats = scan_spill_dir(&cache_path.join("ingress_spill"))?;
    let pending_uploads = scan_pending_uploads(
        &cache_path,
        &format_info.uuid,
        cache_backend_detection.as_ref(),
    )?;
    let cache_usage = match scan_cache_usage(&cache_path, cache_backend_detection.as_ref()) {
        Ok(stats) => Some(stats),
        Err(error) => {
            eprintln!(
                "warning: failed to compute local cache usage stats at '{}': {error}",
                cache_path.display()
            );
            None
        }
    };

    let remote_size = engine.remote_metadata_size()?.unwrap_or(0);
    let local_db_size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

    println!("StudioFS Volume Status");
    println!("──────────────────────");
    println!("  Name:       {}", format_info.name);
    println!("  UUID:       {}", format_info.uuid);
    println!("  Endpoint:   {}", format_info.endpoint);
    println!("  Bucket:     {}", format_info.bucket);
    println!("  Data prefix: {}", format_info.data_prefix);
    println!(
        "  Block size: {} MiB",
        format_info.block_size / (1024 * 1024)
    );

    if let Some(pg) = &format_info.metadata.postgres {
        println!("\n  Meta backend: Postgres");
        println!("  Postgres URL:  {}", pg.url);
        println!("  Postgres vol:  {}", pg.volume_id);
        println!("  Postgres size: {}", format_bytes(remote_size));
    } else {
        println!("\n  Meta backend: {:?}", format_info.metadata.backend);
    }

    println!("\n  Inodes:     {}", total_inodes);
    println!("  Next inode: {}", next_inode);
    println!("  Next block: {}", next_block);

    println!("\n  Used space: {}", format_bytes(used_space));

    println!("\n  Metadata:   {:?}", db_path);
    println!("  Meta size:   {}", format_bytes(local_db_size));

    println!("\nLocal Cache");
    println!("───────────");
    println!("  Root:                {}", cache_path.display());
    let selected_backend = cache_backend_detection
        .as_ref()
        .map(CacheBackendDetection::selected_mount_backend);
    let backend_version = selected_backend
        .map(|backend| backend.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let contract_state = if let Some(detection) = cache_backend_detection.as_ref() {
        if detection.has_dual_layout() {
            "dual-layout (legacy + cache-v3)"
        } else if detection.has_cache_v3_contract() {
            "cache-v3 only"
        } else if detection.has_legacy_raw_backend() {
            "legacy raw only"
        } else {
            "no local cache layout detected"
        }
    } else {
        "unknown"
    };
    let migration_needed = if let Some(detection) = cache_backend_detection.as_ref() {
        detection.selected_mount_backend() == CacheBackendKind::LegacyRawV2
    } else {
        true
    };
    let reconcile_needed = pending_uploads > 0
        || spill_stats.records > 0
        || cache_usage
            .map(|stats| stats.dirty_bytes > 0)
            .unwrap_or(false);
    println!("  Backend version:     {}", backend_version);
    println!("  Contract state:      {}", contract_state);
    println!(
        "  Migration needed:    {}",
        if migration_needed { "yes" } else { "no" }
    );
    println!(
        "  Reconcile needed:    {}",
        if reconcile_needed { "yes" } else { "no" }
    );
    if let Some(stats) = cache_usage {
        println!("  Usage bytes:         {}", format_bytes(stats.usage_bytes));
        println!("  Dirty bytes:         {}", format_bytes(stats.dirty_bytes));
        println!(
            "  Evictable bytes:     {}",
            format_bytes(stats.evictable_bytes)
        );
    } else {
        println!("  Usage bytes:         unknown");
        println!("  Dirty bytes:         unknown");
        println!("  Evictable bytes:     unknown");
    }
    println!(
        "  Cache v3 default:    {}",
        if CACHE_V3_DEFAULT_ALLOWED {
            "enabled"
        } else {
            "blocked"
        }
    );
    let final_validation_issue = CACHE_V3_FINAL_VALIDATION_ISSUE.unwrap_or("UNASSIGNED");
    println!("  Final validation:    {}", final_validation_issue);
    println!("  Guardrail:           {}", CACHE_V3_DEFAULT_GUARDRAIL);

    println!("\nLocal Ingress");
    println!("─────────────");
    println!("  Spill records:      {}", spill_stats.records);
    println!("  Spill bytes:        {}", format_bytes(spill_stats.bytes));
    println!("  Oldest spill age:   {}s", spill_stats.oldest_age_secs);
    println!("  Checkpoint age:     {}s", spill_stats.checkpoint_age_secs);
    println!(
        "  Checkpoint cursor:  {:016x}@{}",
        spill_stats.checkpoint_segment_id, spill_stats.checkpoint_offset
    );
    println!("  Pending uploads:    {}", pending_uploads);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn scan_spill_dir_counts_records_bytes_and_oldest_age() {
        let tmp = tempfile::tempdir().unwrap();
        let spill_dir = tmp.path().join("ingress_spill");
        let segment_dir = spill_dir.join("segments");
        std::fs::create_dir_all(&segment_dir).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(segment_dir.join("0000000000000001.seg"))
            .unwrap();
        file.write_all(&INGRESS_SEGMENT_MAGIC).unwrap();
        file.write_all(&[INGRESS_RECORD_PUT]).unwrap();
        file.write_all(&42u64.to_le_bytes()).unwrap();
        file.write_all(&7u32.to_le_bytes()).unwrap();
        file.write_all(&1u64.to_le_bytes()).unwrap();
        file.write_all(&100u64.to_le_bytes()).unwrap();
        file.write_all(&(3u32).to_le_bytes()).unwrap();
        file.write_all(b"abc").unwrap();
        file.write_all(&INGRESS_SEGMENT_MAGIC).unwrap();
        file.write_all(&[INGRESS_RECORD_PUT]).unwrap();
        file.write_all(&42u64.to_le_bytes()).unwrap();
        file.write_all(&8u32.to_le_bytes()).unwrap();
        file.write_all(&2u64.to_le_bytes()).unwrap();
        file.write_all(&200u64.to_le_bytes()).unwrap();
        file.write_all(&(5u32).to_le_bytes()).unwrap();
        file.write_all(b"hello").unwrap();
        std::fs::write(spill_dir.join("ignore.tmp"), b"x").unwrap();

        let stats = scan_spill_dir(&spill_dir).unwrap();
        assert_eq!(stats.records, 2);
        assert_eq!(stats.bytes, 8);
        assert_eq!(stats.checkpoint_segment_id, 0);
    }

    #[test]
    fn scan_spill_dir_reads_checkpoint_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let spill_dir = tmp.path().join("ingress_spill");
        let checkpoint_dir = spill_dir.join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir).unwrap();
        std::fs::write(
            checkpoint_dir.join("latest.json"),
            br#"{"version":1,"covered_segment_id":3,"covered_offset":4096,"next_seq":7,"entries":[]}"#,
        )
        .unwrap();

        let stats = scan_spill_dir(&spill_dir).unwrap();
        assert_eq!(stats.checkpoint_segment_id, 3);
        assert_eq!(stats.checkpoint_offset, 4096);
    }

    #[test]
    fn scan_pending_uploads_uses_raw_backend_dirty_flags() {
        let tmp = tempfile::tempdir().unwrap();
        let volume_uuid = Uuid::new_v4();
        let store = open_legacy_local_block_backend(tmp.path(), &volume_uuid.to_string()).unwrap();
        store.put(1, b"dirty", false).unwrap();
        store.put(2, b"clean", true).unwrap();

        assert_eq!(
            scan_pending_uploads(tmp.path(), &volume_uuid.to_string(), None).unwrap(),
            1
        );
    }

    #[test]
    fn scan_pending_uploads_supports_cache_v3_selection() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = studiofs_cache::v3::CacheV3Layout::from_cache_root(tmp.path());
        std::fs::create_dir_all(&layout.v3_root).unwrap();
        std::fs::write(&layout.layout_marker, b"v1").unwrap();
        let volume_uuid = Uuid::new_v4();

        let detection = detect_cache_backends_for_status(tmp.path()).unwrap();
        assert_eq!(
            detection.selected_mount_backend(),
            CacheBackendKind::CacheV3
        );
        assert_eq!(
            scan_pending_uploads(tmp.path(), &volume_uuid.to_string(), Some(&detection)).unwrap(),
            0
        );
    }

    #[test]
    fn scan_legacy_raw_cache_usage_reports_usage_dirty_and_evictable_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let volume_uuid = Uuid::new_v4();
        let store = open_legacy_local_block_backend(tmp.path(), &volume_uuid.to_string()).unwrap();
        store.put(1, b"dirty", false).unwrap();
        store.put(2, b"uploaded", true).unwrap();

        let stats = scan_legacy_raw_cache_usage(&tmp.path().join("blocks.raw")).unwrap();
        assert_eq!(stats.usage_bytes, 13);
        assert_eq!(stats.dirty_bytes, 5);
        assert_eq!(stats.evictable_bytes, 8);
    }

    #[test]
    fn detect_cache_backends_with_fallback_returns_none_on_error() {
        let tmp = tempfile::tempdir().unwrap();
        let detection = detect_cache_backends_with_fallback(tmp.path(), |_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated permission error",
            ))
        });
        assert!(detection.is_none());
    }

    #[test]
    fn detect_cache_backends_for_status_returns_detection_when_probe_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let detection = detect_cache_backends_for_status(tmp.path());
        assert!(detection.is_some());
    }
}

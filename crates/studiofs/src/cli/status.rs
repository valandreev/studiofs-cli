//! `studiofs status` — show filesystem status.

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;
use studiofs_meta::{MetaEngine, RedbMetaEngine, counters};

const INGRESS_SEGMENT_MAGIC: [u8; 4] = *b"ISG1";
const INGRESS_RECORD_PUT: u8 = 1;
const INGRESS_RECORD_INVALIDATE: u8 = 2;
const INGRESS_RECORD_HEADER_LEN: usize = 37;

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

fn local_cache_path(meta_dir: &Path, cache_dir: Option<&Path>, volume_uuid: &str) -> PathBuf {
    let base = cache_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| meta_dir.join("cache"));
    base.join(volume_uuid)
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
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

fn scan_pending_uploads(upload_dir: &Path) -> anyhow::Result<usize> {
    let entries = match std::fs::read_dir(upload_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };

    Ok(entries
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "pending"))
        .count())
}

pub fn run(name: &str, meta_dir: &Path, cache_dir: Option<&Path>) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));

    if !db_path.exists() {
        println!("✗ Volume '{}' not found at {:?}", name, db_path);
        println!("  Run `studiofs init {}` first.", name);
        return Ok(());
    }

    let engine = RedbMetaEngine::open(&db_path)?;
    let format_info = engine.get_format_info()?;

    let total_inodes = engine.get_counter(counters::TOTAL_INODES)?;
    let used_space = engine.get_counter(counters::USED_SPACE)?;
    let next_inode = engine.get_counter(counters::NEXT_INODE)?;
    let next_block = engine.get_counter(counters::NEXT_BLOCK_ID)?;

    let cache_path = local_cache_path(meta_dir, cache_dir, &format_info.uuid);
    let spill_stats = scan_spill_dir(&cache_path.join("ingress_spill"))?;
    let pending_uploads = scan_pending_uploads(&cache_path.join("upload"))?;

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
    println!(
        "  Chunk size: {} MiB",
        format_info.chunk_size / (1024 * 1024)
    );
    println!("  Meta backend: {:?}", format_info.metadata.backend);
    if let Some(remote) = &format_info.metadata.remote {
        println!("  Meta endpoint: {}", remote.endpoint);
        println!("  Meta bucket:   {}", remote.bucket);
        println!("  Meta prefix:   {}", remote.prefix);
    }
    if let Some(pg) = &format_info.metadata.postgres {
        println!("  Postgres URL:  {}", pg.url);
        println!("  Postgres vol:  {}", pg.volume_id);
    }
    println!("  Meta cache TTL: {}s", format_info.metadata.cache_ttl_secs);
    println!(
        "  Meta flush interval: {}s",
        format_info.metadata.flush_interval_secs
    );
    println!(
        "  Meta flush ops threshold: {}",
        format_info.metadata.flush_ops_threshold
    );
    println!(
        "  Meta flush bytes threshold: {}",
        format_info.metadata.flush_bytes_threshold
    );
    println!();
    println!("  Inodes:     {}", total_inodes);
    println!("  Used space: {} bytes", used_space);
    println!("  Next inode: {}", next_inode);
    println!("  Next block: {}", next_block);
    println!("  Metadata:   {:?}", db_path);
    println!("  Cache:      {:?}", cache_path);
    println!();
    println!("Local Ingress");
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
    fn scan_pending_uploads_counts_pending_files_only() {
        let tmp = tempfile::tempdir().unwrap();
        let upload_dir = tmp.path().join("upload");
        std::fs::create_dir_all(&upload_dir).unwrap();
        std::fs::write(upload_dir.join("one.pending"), b"x").unwrap();
        std::fs::write(upload_dir.join("two.pending"), b"y").unwrap();
        std::fs::write(upload_dir.join("done.txt"), b"z").unwrap();

        assert_eq!(scan_pending_uploads(&upload_dir).unwrap(), 2);
    }
}

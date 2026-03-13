//! `studiofs janitor` — remove orphaned data objects not referenced by metadata.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opendal::services::S3;
use opendal::{EntryMode, Operator};
use studiofs_data::object::parse_object_id_from_key;
use studiofs_meta::types::{BlockRef, MetadataSnapshot};
use studiofs_meta::{MetaEngine, RedbMetaEngine};

#[derive(Debug, Default)]
struct JanitorStats {
    scanned_files: u64,
    referenced_files: u64,
    orphan_candidates: u64,
    skipped_young: u64,
    skipped_unknown_age: u64,
    skipped_cap: u64,
    deleted: u64,
    delete_errors: u64,
}

fn normalize_prefix(format_info: &studiofs_meta::FormatInfo) -> String {
    let raw = if format_info.data_prefix.trim().is_empty() {
        format!("{}/objects/", format_info.name)
    } else {
        format_info.data_prefix.clone()
    };
    if raw.ends_with('/') {
        raw
    } else {
        format!("{}/", raw)
    }
}

fn collect_referenced_objects(snapshot: &studiofs_meta::redb_engine::RedbSnapshot) -> HashSet<u64> {
    let mut refs = HashSet::new();
    for (_k, v) in &snapshot.file_blocks {
        if let Ok(br) = postcard::from_bytes::<BlockRef>(v) {
            refs.insert(br.object_id);
        }
    }
    for (_id, raw) in &snapshot.snapshots {
        if let Ok(meta_snapshot) = postcard::from_bytes::<MetadataSnapshot>(raw) {
            refs.extend(meta_snapshot.pinned_object_ids);
        }
    }
    refs
}

fn collect_pending_deletions(
    snapshot: &studiofs_meta::redb_engine::RedbSnapshot,
) -> HashMap<u64, i64> {
    let mut pending = HashMap::new();
    for (object_id, ts) in &snapshot.pending_deletions {
        pending.insert(*object_id, *ts);
    }
    pending
}

async fn run_one_cycle(
    op: &Operator,
    prefix: &str,
    referenced: &HashSet<u64>,
    pending: &HashMap<u64, i64>,
    dry_run: bool,
    skip_newer_than_secs: u64,
    max_deletes: u64,
) -> anyhow::Result<JanitorStats> {
    let mut stats = JanitorStats::default();
    let now = SystemTime::now();
    let now_unix = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64;

    let entries = op.list_with(prefix).recursive(true).await?;
    for entry in entries {
        if entry.metadata().mode() != EntryMode::FILE {
            continue;
        }

        stats.scanned_files = stats.scanned_files.saturating_add(1);
        let path = entry.path().to_string();

        let object_id = match parse_object_id_from_key(&path) {
            Ok(id) => id,
            Err(_) => continue,
        };

        if referenced.contains(&object_id) {
            stats.referenced_files = stats.referenced_files.saturating_add(1);
            continue;
        }
        stats.orphan_candidates = stats.orphan_candidates.saturating_add(1);

        if stats.deleted >= max_deletes {
            stats.skipped_cap = stats.skipped_cap.saturating_add(1);
            continue;
        }

        if let Some(pending_ts) = pending.get(&object_id) {
            let age_secs = now_unix.saturating_sub(*pending_ts) as u64;
            if age_secs < skip_newer_than_secs {
                stats.skipped_young = stats.skipped_young.saturating_add(1);
                continue;
            }
        } else {
            let meta = match op.stat(&path).await {
                Ok(m) => m,
                Err(_) => {
                    stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
                    continue;
                }
            };
            let Some(last_modified) = meta.last_modified() else {
                stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
                continue;
            };
            let last_modified_st: SystemTime = last_modified.into();
            let age_secs = now
                .duration_since(last_modified_st)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_secs();
            if age_secs < skip_newer_than_secs {
                stats.skipped_young = stats.skipped_young.saturating_add(1);
                continue;
            }
        }

        if dry_run {
            stats.deleted = stats.deleted.saturating_add(1);
            continue;
        }

        match op.delete(&path).await {
            Ok(_) => {
                stats.deleted = stats.deleted.saturating_add(1);
            }
            Err(_) => {
                stats.delete_errors = stats.delete_errors.saturating_add(1);
            }
        }
    }

    Ok(stats)
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    meta_dir: &Path,
    dry_run: bool,
    loop_mode: bool,
    interval_minutes: u64,
    skip_newer_minutes: u64,
    max_deletes: u64,
) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        return Err(anyhow::anyhow!(
            "Volume '{}' not found at {:?}",
            name,
            db_path
        ));
    }

    let redb = RedbMetaEngine::open(&db_path)?;
    let format_info = redb.get_format_info()?;
    let snapshot = redb.export_snapshot()?;

    let prefix = normalize_prefix(&format_info);
    let referenced = collect_referenced_objects(&snapshot);
    let pending = collect_pending_deletions(&snapshot);

    let mut builder = S3::default()
        .endpoint(&format_info.endpoint)
        .bucket(&format_info.bucket)
        .access_key_id(&format_info.access_key)
        .secret_access_key(&format_info.secret_key)
        .disable_config_load()
        .disable_ec2_metadata();
    if !format_info.region.is_empty() {
        builder = builder.region(&format_info.region);
    } else {
        builder = builder.region("us-east-1");
    }
    let op = Operator::new(builder)?.finish();

    let rt = tokio::runtime::Runtime::new()?;
    let skip_secs = skip_newer_minutes.saturating_mul(60);

    println!("Janitor started for '{}'", name);
    println!(
        "  Mode:      {}",
        if dry_run { "dry-run" } else { "delete" }
    );
    println!("  Prefix:    {}", prefix);
    println!("  Grace:     {} minutes", skip_newer_minutes);
    println!("  Max delete: {}", max_deletes);
    println!(
        "  Referenced objects in metadata snapshot: {}",
        referenced.len()
    );

    loop {
        let stats = rt.block_on(run_one_cycle(
            &op,
            &prefix,
            &referenced,
            &pending,
            dry_run,
            skip_secs,
            max_deletes.max(1),
        ))?;

        println!("Janitor cycle done:");
        println!("  scanned:              {}", stats.scanned_files);
        println!("  referenced:           {}", stats.referenced_files);
        println!("  orphan candidates:    {}", stats.orphan_candidates);
        println!("  skipped young:        {}", stats.skipped_young);
        println!("  skipped unknown age:  {}", stats.skipped_unknown_age);
        println!("  skipped by cap:       {}", stats.skipped_cap);
        println!(
            "  {}: {}",
            if dry_run { "would delete" } else { "deleted" },
            stats.deleted
        );
        println!("  delete errors:        {}", stats.delete_errors);

        if !loop_mode {
            break;
        }
        std::thread::sleep(Duration::from_secs(
            interval_minutes.max(1).saturating_mul(60),
        ));
    }

    Ok(())
}

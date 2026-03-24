//! `studiofs janitor` — remove orphaned data objects not referenced by metadata.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opendal::services::S3;
use opendal::{EntryMode, Operator};
use studiofs_data::object::{format_object_fragment_keys, parse_object_id_from_key};
use studiofs_meta::open_meta_engine;

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

struct JanitorCycleContext<'a> {
    op: &'a Operator,
    prefix: &'a str,
    referenced: &'a HashSet<u64>,
    global_snapshot_pins: &'a HashSet<u64>,
    pending: &'a HashMap<u64, i64>,
    dry_run: bool,
    skip_newer_than_secs: u64,
    max_deletes: u64,
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

async fn run_one_cycle(ctx: JanitorCycleContext<'_>) -> anyhow::Result<JanitorStats> {
    let mut stats = JanitorStats::default();
    let mut seen_objects = HashSet::new();
    let now = SystemTime::now();
    let now_unix = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64;

    let entries = ctx.op.list_with(ctx.prefix).recursive(true).await?;
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
        if !seen_objects.insert(object_id) {
            continue;
        }

        if ctx.referenced.contains(&object_id) {
            stats.referenced_files = stats.referenced_files.saturating_add(1);
            continue;
        }
        if ctx.global_snapshot_pins.contains(&object_id) {
            stats.referenced_files = stats.referenced_files.saturating_add(1);
            continue;
        }
        stats.orphan_candidates = stats.orphan_candidates.saturating_add(1);

        if stats.deleted >= ctx.max_deletes {
            stats.skipped_cap = stats.skipped_cap.saturating_add(1);
            continue;
        }

        if let Some(pending_ts) = ctx.pending.get(&object_id) {
            let age_secs = now_unix.saturating_sub(*pending_ts) as u64;
            if age_secs < ctx.skip_newer_than_secs {
                stats.skipped_young = stats.skipped_young.saturating_add(1);
                continue;
            }
        } else {
            let meta = match ctx.op.stat(&path).await {
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
            if age_secs < ctx.skip_newer_than_secs {
                stats.skipped_young = stats.skipped_young.saturating_add(1);
                continue;
            }
        }

        if ctx.dry_run {
            stats.deleted = stats.deleted.saturating_add(1);
            continue;
        }

        let mut delete_failed = false;
        for key in format_object_fragment_keys(ctx.prefix, object_id) {
            if let Err(err) = ctx.op.delete(&key).await {
                delete_failed = true;
                eprintln!(
                    "janitor: failed to delete fragment key={} object_id={}: {}",
                    key, object_id, err
                );
            }
        }
        if delete_failed {
            stats.delete_errors = stats.delete_errors.saturating_add(1);
            continue;
        }
        stats.deleted = stats.deleted.saturating_add(1);
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

    let engine = open_meta_engine(&db_path)?;
    let format_info = engine.get_format_info()?;

    let prefix = normalize_prefix(&format_info);

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

    loop {
        let lock_acquired = engine.try_acquire_global_janitor_lock()?;
        if !lock_acquired {
            println!("Janitor cycle skipped: PG janitor lock not acquired");
            if !loop_mode {
                break;
            }
            std::thread::sleep(Duration::from_secs(
                interval_minutes.max(1).saturating_mul(60),
            ));
            continue;
        }

        let cycle_result = (|| -> anyhow::Result<(JanitorStats, HashSet<u64>, HashSet<u64>)> {
            let global_snapshot_pins = engine.get_global_snapshot_pins()?;
            let referenced = engine.get_referenced_object_ids()?;
            let pending: HashMap<u64, i64> = engine.get_pending_deletions()?.into_iter().collect();

            let stats = rt.block_on(run_one_cycle(JanitorCycleContext {
                op: &op,
                prefix: &prefix,
                referenced: &referenced,
                global_snapshot_pins: &global_snapshot_pins,
                pending: &pending,
                dry_run,
                skip_newer_than_secs: skip_secs,
                max_deletes: max_deletes.max(1),
            }))?;

            Ok((stats, referenced, global_snapshot_pins))
        })();
        if let Err(e) = engine.release_global_janitor_lock() {
            eprintln!("Warning: failed to release PG janitor lock: {}", e);
        }
        let (stats, referenced, global_snapshot_pins) = cycle_result?;

        println!("Janitor cycle done:");
        println!("  scanned:              {}", stats.scanned_files);
        println!("  metadata refs:        {}", referenced.len());
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
        println!("  snapshot pins:        {}", global_snapshot_pins.len());

        if !loop_mode {
            break;
        }
        std::thread::sleep(Duration::from_secs(
            interval_minutes.max(1).saturating_mul(60),
        ));
    }

    Ok(())
}

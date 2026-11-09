//! Provider-neutral standalone janitor cycle.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use studiofs_data::janitor::JanitorEntry;
use studiofs_data::object::{format_object_fragment_keys, parse_object_id_from_key};

#[derive(Debug, Default)]
pub(super) struct JanitorStats {
    pub(super) scanned_files: u64,
    pub(super) referenced_files: u64,
    pub(super) orphan_candidates: u64,
    pub(super) skipped_young: u64,
    pub(super) skipped_unknown_age: u64,
    pub(super) skipped_cap: u64,
    pub(super) deleted: u64,
    pub(super) delete_errors: u64,
    pub(super) initial_listing_elapsed: Duration,
    pub(super) initial_listing_entries: u64,
    pub(super) peak_listing_entries: u64,
    pub(super) time_to_first_candidate: Option<Duration>,
    pub(super) cycle_elapsed: Duration,
    pub(super) head_requests: u64,
    pub(super) head_elapsed: Duration,
    pub(super) delete_requests: u64,
    pub(super) delete_elapsed: Duration,
}

pub(super) struct JanitorCycleContext<'a, S> {
    pub(super) store: &'a S,
    pub(super) prefix: &'a str,
    pub(super) referenced: &'a HashSet<u64>,
    pub(super) global_snapshot_pins: &'a HashSet<u64>,
    pub(super) pending: &'a HashMap<u64, i64>,
    pub(super) dry_run: bool,
    pub(super) skip_newer_than_secs: u64,
    pub(super) max_deletes: u64,
}

#[derive(Debug)]
pub(super) enum JanitorStoreError {
    NotFound,
    Other(String),
}

pub(super) trait JanitorObjectStore {
    async fn list(&self, prefix: &str) -> Result<Vec<JanitorEntry>, String>;
    async fn last_modified(&self, key: &str) -> Result<Option<SystemTime>, String>;
    async fn delete(&self, key: &str) -> Result<(), JanitorStoreError>;
}

fn discover_object(
    entry: JanitorEntry,
    seen_objects: &mut HashSet<u64>,
    stats: &mut JanitorStats,
) -> Option<(u64, String)> {
    if !entry.is_file {
        return None;
    }
    stats.scanned_files = stats.scanned_files.saturating_add(1);
    let object_id = parse_object_id_from_key(&entry.key).ok()?;
    seen_objects
        .insert(object_id)
        .then_some((object_id, entry.key))
}

fn is_protected(object_id: u64, referenced: &HashSet<u64>, snapshot_pins: &HashSet<u64>) -> bool {
    referenced.contains(&object_id) || snapshot_pins.contains(&object_id)
}

async fn process_orphan<S: JanitorObjectStore>(
    ctx: &JanitorCycleContext<'_, S>,
    object_id: u64,
    path: &str,
    now: SystemTime,
    now_unix: i64,
    stats: &mut JanitorStats,
) {
    if stats.deleted >= ctx.max_deletes {
        stats.skipped_cap = stats.skipped_cap.saturating_add(1);
        return;
    }

    if let Some(pending_ts) = ctx.pending.get(&object_id) {
        match u64::try_from(now_unix.saturating_sub(*pending_ts)) {
            Ok(age_secs) if age_secs >= ctx.skip_newer_than_secs => {}
            Ok(_) => {
                stats.skipped_young = stats.skipped_young.saturating_add(1);
                return;
            }
            Err(_) => {
                stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
                return;
            }
        }
    } else {
        if stats.head_requests == 0 {
            eprintln!("janitor phase=head started");
        }
        stats.head_requests = stats.head_requests.saturating_add(1);
        let head_started = Instant::now();
        let last_modified = ctx.store.last_modified(path).await;
        stats.head_elapsed += head_started.elapsed();
        let last_modified = match last_modified {
            Ok(Some(value)) => value,
            Ok(None) | Err(_) => {
                stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
                return;
            }
        };
        let age_secs = now
            .duration_since(last_modified)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        if age_secs < ctx.skip_newer_than_secs {
            stats.skipped_young = stats.skipped_young.saturating_add(1);
            return;
        }
    }

    if ctx.dry_run {
        stats.deleted = stats.deleted.saturating_add(1);
        return;
    }

    let mut delete_failed = false;
    for key in format_object_fragment_keys(ctx.prefix, object_id) {
        if stats.delete_requests == 0 {
            eprintln!("janitor phase=delete started");
        }
        stats.delete_requests = stats.delete_requests.saturating_add(1);
        let delete_started = Instant::now();
        let result = ctx.store.delete(&key).await;
        stats.delete_elapsed += delete_started.elapsed();
        if let Err(error) = result {
            match error {
                JanitorStoreError::NotFound => {}
                JanitorStoreError::Other(error) => {
                    delete_failed = true;
                    eprintln!(
                        "janitor: failed to delete fragment key={} object_id={}: {}",
                        key, object_id, error
                    );
                }
            }
        }
    }
    if delete_failed {
        stats.delete_errors = stats.delete_errors.saturating_add(1);
    } else {
        stats.deleted = stats.deleted.saturating_add(1);
    }
}

pub(super) async fn run_one_cycle<S: JanitorObjectStore>(
    ctx: JanitorCycleContext<'_, S>,
) -> anyhow::Result<JanitorStats> {
    let mut stats = JanitorStats::default();
    let cycle_started = Instant::now();
    let mut seen_objects = HashSet::new();
    let now = SystemTime::now();
    let now_unix = i64::try_from(
        now.duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs(),
    )
    .unwrap_or(i64::MAX);

    eprintln!("janitor phase=initial_listing started");
    let listing_started = Instant::now();
    let entries = ctx
        .store
        .list(ctx.prefix)
        .await
        .map_err(|error| anyhow::anyhow!("failed to list janitor objects: {error}"))?;
    stats.initial_listing_elapsed = listing_started.elapsed();
    stats.initial_listing_entries = u64::try_from(entries.len()).unwrap_or(u64::MAX);
    stats.peak_listing_entries = stats.initial_listing_entries;
    eprintln!(
        "janitor phase=initial_listing completed duration_ms={} entries={}",
        super::duration_ms(stats.initial_listing_elapsed),
        stats.initial_listing_entries
    );

    for entry in entries {
        let Some((object_id, path)) = discover_object(entry, &mut seen_objects, &mut stats) else {
            continue;
        };
        if is_protected(object_id, ctx.referenced, ctx.global_snapshot_pins) {
            stats.referenced_files = stats.referenced_files.saturating_add(1);
            continue;
        }
        stats.orphan_candidates = stats.orphan_candidates.saturating_add(1);
        if stats.time_to_first_candidate.is_none() {
            stats.time_to_first_candidate = Some(cycle_started.elapsed());
        }
        process_orphan(&ctx, object_id, &path, now, now_unix, &mut stats).await;
    }

    stats.cycle_elapsed = cycle_started.elapsed();
    eprintln!(
        "janitor phase=head completed calls={} duration_ms={}",
        stats.head_requests,
        super::duration_ms(stats.head_elapsed)
    );
    eprintln!(
        "janitor phase=delete completed calls={} duration_ms={}",
        stats.delete_requests,
        super::duration_ms(stats.delete_elapsed)
    );
    Ok(stats)
}

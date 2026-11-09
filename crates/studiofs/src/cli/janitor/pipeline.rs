//! Provider-neutral standalone janitor cycle.

use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use studiofs_data::janitor::{JanitorDeleteError, JanitorEntry};
use studiofs_data::object::canonical_object_delete_keys;
use studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome;
use studiofs_meta::{JanitorEligibility, MAX_JANITOR_ELIGIBILITY_BATCH};

/// Bound for one remote DELETE. All canonical keys of an object must finish
/// inside the 60-second deletion-claim lease, or the finish is fenced out.
const DELETE_KEY_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(20)
};
const _: () = assert!(
    (studiofs_data::object::CLOUD_FRAGMENT_COUNT as u64 + 1) * 20 < 60,
    "canonical key deletes must fit inside the deletion-claim lease"
);

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
    pub(super) skipped_non_canonical: u64,
    pub(super) claims: u64,
    pub(super) claim_protected: u64,
    pub(super) claim_finishes: u64,
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
    pub(super) dry_run: bool,
    pub(super) skip_newer_than_secs: u64,
    pub(super) max_deletes: u64,
    pub(super) metadata: &'a mut dyn super::JanitorMetadataSource,
}

pub(super) trait JanitorObjectStore {
    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<JanitorEntry>>;
    async fn last_modified(&self, key: &str) -> anyhow::Result<Option<SystemTime>>;
    async fn delete(&self, key: &str) -> Result<(), JanitorDeleteError>;
}

/// One discovered object: its ID, the listed key, and every canonical key to delete.
struct Candidate {
    object_id: u64,
    path: String,
    delete_keys: Vec<String>,
}

fn discover_object(
    entry: JanitorEntry,
    prefix: &str,
    seen_objects: &mut HashSet<u64>,
    stats: &mut JanitorStats,
) -> Option<Candidate> {
    if !entry.is_file {
        return None;
    }
    stats.scanned_files = stats.scanned_files.saturating_add(1);
    let Some((object_id, delete_keys)) = canonical_object_delete_keys(prefix, &entry.key) else {
        stats.skipped_non_canonical = stats.skipped_non_canonical.saturating_add(1);
        return None;
    };
    seen_objects.insert(object_id).then_some(Candidate {
        object_id,
        path: entry.key,
        delete_keys,
    })
}

async fn process_orphan<S: JanitorObjectStore>(
    ctx: &mut JanitorCycleContext<'_, S>,
    candidate: &Candidate,
    now: SystemTime,
    now_unix: i64,
    stats: &mut JanitorStats,
    pending_ts: Option<i64>,
) -> anyhow::Result<()> {
    let Candidate {
        object_id,
        path,
        delete_keys,
    } = candidate;
    let object_id = *object_id;
    if stats.deleted >= ctx.max_deletes {
        stats.skipped_cap = stats.skipped_cap.saturating_add(1);
        return Ok(());
    }

    if let Some(pending_ts) = pending_ts {
        match u64::try_from(now_unix.saturating_sub(pending_ts)) {
            Ok(age_secs) if age_secs >= ctx.skip_newer_than_secs => {}
            Ok(_) => {
                stats.skipped_young = stats.skipped_young.saturating_add(1);
                return Ok(());
            }
            Err(_) => {
                stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
                return Ok(());
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
                return Ok(());
            }
        };
        let age_secs = now
            .duration_since(last_modified)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        if age_secs < ctx.skip_newer_than_secs {
            stats.skipped_young = stats.skipped_young.saturating_add(1);
            return Ok(());
        }
    }

    if ctx.dry_run {
        stats.deleted = stats.deleted.saturating_add(1);
        return Ok(());
    }

    // A claim failure means safety cannot be proven; stop the cycle instead of guessing.
    let claim = tokio::task::block_in_place(|| ctx.metadata.claim_deletion(object_id))
        .with_context(|| format!("failed to claim janitor object {object_id} for deletion"))?;
    stats.claims = stats.claims.saturating_add(1);
    let Some(fencing_token) = claim else {
        stats.claim_protected = stats.claim_protected.saturating_add(1);
        return Ok(());
    };

    let mut delete_failed = false;
    for key in delete_keys {
        if stats.delete_requests == 0 {
            eprintln!("janitor phase=delete started");
        }
        stats.delete_requests = stats.delete_requests.saturating_add(1);
        let delete_started = Instant::now();
        let result = tokio::time::timeout(DELETE_KEY_TIMEOUT, ctx.store.delete(key))
            .await
            .unwrap_or_else(|_| Err(JanitorDeleteError::Other("delete timed out".to_owned())));
        stats.delete_elapsed += delete_started.elapsed();
        if let Err(error) = result {
            match error {
                JanitorDeleteError::NotFound => {}
                JanitorDeleteError::Other(error) => {
                    delete_failed = true;
                    eprintln!(
                        "janitor: failed to delete object key={} object_id={}: {}",
                        key, object_id, error
                    );
                }
            }
        }
    }
    let outcome = if delete_failed {
        UploadedObjectDeletionOutcome::Retryable
    } else {
        UploadedObjectDeletionOutcome::Deleted
    };
    // Success needs PostgreSQL confirmation; an unfinished claim expires and is retried.
    let finish = tokio::task::block_in_place(|| {
        ctx.metadata
            .finish_deletion(object_id, fencing_token, outcome)
    });
    match finish {
        Ok(()) => stats.claim_finishes = stats.claim_finishes.saturating_add(1),
        Err(error) => {
            delete_failed = true;
            eprintln!("janitor: failed to finish deletion claim object_id={object_id}: {error:#}");
        }
    }
    if delete_failed {
        stats.delete_errors = stats.delete_errors.saturating_add(1);
    } else {
        stats.deleted = stats.deleted.saturating_add(1);
    }
    Ok(())
}

pub(super) async fn run_one_cycle<S: JanitorObjectStore>(
    mut ctx: JanitorCycleContext<'_, S>,
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
        .context("failed to list janitor objects")?;
    stats.initial_listing_elapsed = listing_started.elapsed();
    stats.initial_listing_entries = u64::try_from(entries.len()).unwrap_or(u64::MAX);
    stats.peak_listing_entries = stats.initial_listing_entries;
    eprintln!(
        "janitor phase=initial_listing completed duration_ms={} entries={}",
        super::duration_ms(stats.initial_listing_elapsed),
        stats.initial_listing_entries
    );

    let mut candidates = Vec::with_capacity(MAX_JANITOR_ELIGIBILITY_BATCH);
    for entry in entries {
        let Some(candidate) = discover_object(entry, ctx.prefix, &mut seen_objects, &mut stats)
        else {
            continue;
        };
        candidates.push(candidate);
        if candidates.len() == MAX_JANITOR_ELIGIBILITY_BATCH {
            process_candidate_batch(
                &mut ctx,
                std::mem::take(&mut candidates),
                now,
                now_unix,
                cycle_started,
                &mut stats,
            )
            .await?;
        }
    }
    if !candidates.is_empty() {
        process_candidate_batch(
            &mut ctx,
            candidates,
            now,
            now_unix,
            cycle_started,
            &mut stats,
        )
        .await?;
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

async fn process_candidate_batch<S: JanitorObjectStore>(
    ctx: &mut JanitorCycleContext<'_, S>,
    candidates: Vec<Candidate>,
    now: SystemTime,
    now_unix: i64,
    cycle_started: Instant,
    stats: &mut JanitorStats,
) -> anyhow::Result<()> {
    let ids: Vec<u64> = candidates
        .iter()
        .map(|candidate| candidate.object_id)
        .collect();
    // The metadata session owns a synchronous runtime; leave this worker before its block_on.
    let protection = tokio::task::block_in_place(|| ctx.metadata.check_eligibility(&ids))?;
    // Resolve every decision before any delete so one unknown outcome retains the whole batch.
    // `Some(pending_deletion)` marks an eligible orphan; `None` marks a protected object.
    let decisions = candidates
        .into_iter()
        .map(|candidate| match protection.get(&candidate.object_id) {
            Some(JanitorEligibility::Protected) => Ok((candidate, None)),
            Some(&JanitorEligibility::Eligible { pending_deletion }) => {
                Ok((candidate, Some(pending_deletion)))
            }
            None | Some(JanitorEligibility::Unknown) => Err(anyhow::anyhow!(
                "janitor eligibility is unknown; refusing to delete candidate batch"
            )),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for (candidate, orphan) in decisions {
        match orphan {
            None => {
                stats.referenced_files = stats.referenced_files.saturating_add(1);
            }
            Some(pending_deletion) => {
                stats.orphan_candidates = stats.orphan_candidates.saturating_add(1);
                if stats.time_to_first_candidate.is_none() {
                    stats.time_to_first_candidate = Some(cycle_started.elapsed());
                }
                process_orphan(ctx, &candidate, now, now_unix, stats, pending_deletion).await?;
            }
        }
    }
    Ok(())
}

//! Provider-neutral standalone janitor cycle.

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
/// Bound for one HEAD age probe; a stalled probe leaves the object as unknown age,
/// so one request cannot hold the cycle past its wall-clock budget.
const HEAD_TIMEOUT: Duration = DELETE_KEY_TIMEOUT;
/// Bound for one listing step; a stalled page fails the cycle. Finished
/// deletions are already confirmed in PostgreSQL, so no completed work is lost.
const LIST_PAGE_TIMEOUT: Duration = if cfg!(test) {
    // Long enough for a loaded CI host to serve a loopback page.
    Duration::from_secs(2)
} else {
    Duration::from_secs(60)
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
    pub(super) deleted: u64,
    pub(super) delete_errors: u64,
    pub(super) skipped_non_canonical: u64,
    pub(super) claims: u64,
    pub(super) claim_protected: u64,
    pub(super) claim_finishes: u64,
    pub(super) initial_listing_elapsed: Duration,
    pub(super) initial_listing_entries: u64,
    pub(super) first_listing_entry: Option<Duration>,
    pub(super) peak_buffered_candidates: u64,
    pub(super) time_to_first_candidate: Option<Duration>,
    pub(super) cycle_elapsed: Duration,
    pub(super) head_requests: u64,
    pub(super) head_elapsed: Duration,
    pub(super) delete_requests: u64,
    pub(super) delete_elapsed: Duration,
    pub(super) outcome: CycleOutcome,
}

/// Why a cycle stopped admitting work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum CycleOutcome {
    /// The whole prefix was listed and every listed candidate was decided.
    #[default]
    Complete,
    /// `max_deletes` successful deletions were confirmed and admission stopped. Listing
    /// never looks ahead, so the cap met on the last entry also reports this.
    DeleteCapReached,
    /// The listed-entry budget was met before the listing was seen to end.
    ScanBudgetExhausted,
    /// The wall-clock budget ran out before all listed work was admitted.
    TimeBudgetExhausted,
}

impl CycleOutcome {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::DeleteCapReached => "capped (max deletes reached)",
            Self::ScanBudgetExhausted => "partial (scan budget exhausted)",
            Self::TimeBudgetExhausted => "partial (time budget exhausted)",
        }
    }
}

/// Per-cycle work limits. They are checked only where new work is admitted, so a
/// claimed deletion always runs through its PostgreSQL finish.
#[derive(Debug, Clone, Copy)]
pub(super) struct RunBudget {
    /// Confirmed successful (dry-run: would-be) deletions; protected and failed
    /// objects never consume it. Deletion is sequential, so it cannot overshoot.
    pub(super) max_deletes: u64,
    /// Listed entries after which no further entry is pulled from the listing.
    pub(super) max_scan_entries: Option<u64>,
    /// Wall-clock time after which no further entry or candidate is admitted.
    pub(super) max_runtime: Option<Duration>,
}

impl RunBudget {
    #[cfg(test)]
    pub(super) fn deletes(max_deletes: u64) -> Self {
        Self {
            max_deletes,
            max_scan_entries: None,
            max_runtime: None,
        }
    }

    /// The reason to stop admitting candidates, if the cap or the deadline is met.
    fn admission_stop(&self, stats: &JanitorStats, started: Instant) -> Option<CycleOutcome> {
        if stats.deleted >= self.max_deletes {
            return Some(CycleOutcome::DeleteCapReached);
        }
        self.max_runtime
            .filter(|max| started.elapsed() >= *max)
            .map(|_| CycleOutcome::TimeBudgetExhausted)
    }

    /// [`Self::admission_stop`] plus the listed-entry budget, checked before each listing step.
    fn listing_stop(&self, stats: &JanitorStats, started: Instant) -> Option<CycleOutcome> {
        self.admission_stop(stats, started).or_else(|| {
            self.max_scan_entries
                .filter(|max| stats.initial_listing_entries >= *max)
                .map(|_| CycleOutcome::ScanBudgetExhausted)
        })
    }
}

pub(super) struct JanitorCycleContext<'a, S> {
    pub(super) store: &'a S,
    pub(super) prefix: &'a str,
    pub(super) dry_run: bool,
    pub(super) skip_newer_than_secs: u64,
    pub(super) budget: RunBudget,
    pub(super) metadata: &'a mut dyn super::JanitorMetadataSource,
}

pub(super) trait JanitorListing {
    async fn next_entry(&mut self) -> anyhow::Result<Option<JanitorEntry>>;
}

pub(super) trait JanitorObjectStore {
    type Listing: JanitorListing;
    async fn list(&self, prefix: &str) -> anyhow::Result<Self::Listing>;
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
    last_object: &mut Option<u64>,
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
    // ponytail: dedupe only adjacent keys of one object, relying on S3's lexicographic
    // listing order; an unordered provider re-checks an object, which stays safe
    // because claims and NotFound deletes are idempotent.
    if last_object.replace(object_id) == Some(object_id) {
        return None;
    }
    Some(Candidate {
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
        let last_modified = tokio::time::timeout(HEAD_TIMEOUT, ctx.store.last_modified(path))
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("HEAD timed out")));
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
    let mut last_object = None;
    let now = SystemTime::now();
    let now_unix = i64::try_from(
        now.duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs(),
    )
    .unwrap_or(i64::MAX);

    eprintln!("janitor phase=initial_listing started");
    let listing_started = Instant::now();
    let mut listing = bounded_list_step(ctx.store.list(ctx.prefix), 0).await?;
    stats.initial_listing_elapsed = listing_started.elapsed();

    // Entries are consumed as pages arrive; memory holds one page plus one batch.
    let mut candidates = Vec::with_capacity(MAX_JANITOR_ELIGIBILITY_BATCH);
    loop {
        if let Some(stop) = ctx.budget.listing_stop(&stats, cycle_started) {
            stats.outcome = stop;
            break;
        }
        let wait_started = Instant::now();
        let entry = bounded_list_step(listing.next_entry(), stats.initial_listing_entries).await?;
        stats.initial_listing_elapsed += wait_started.elapsed();
        let Some(entry) = entry else {
            break;
        };
        if stats.first_listing_entry.is_none() {
            let elapsed = cycle_started.elapsed();
            stats.first_listing_entry = Some(elapsed);
            eprintln!(
                "janitor phase=initial_listing first_entry duration_ms={}",
                super::duration_ms(elapsed)
            );
        }
        stats.initial_listing_entries = stats.initial_listing_entries.saturating_add(1);
        let Some(candidate) = discover_object(entry, ctx.prefix, &mut last_object, &mut stats)
        else {
            continue;
        };
        candidates.push(candidate);
        stats.peak_buffered_candidates = stats
            .peak_buffered_candidates
            .max(u64::try_from(candidates.len()).unwrap_or(u64::MAX));
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
            if stats.outcome != CycleOutcome::Complete {
                break;
            }
        }
    }
    eprintln!(
        "janitor phase=initial_listing completed duration_ms={} entries={} outcome={:?}",
        super::duration_ms(stats.initial_listing_elapsed),
        stats.initial_listing_entries,
        stats.outcome
    );
    // Already-listed candidates finish only while the cap and deadline still admit work.
    if !candidates.is_empty()
        && matches!(
            stats.outcome,
            CycleOutcome::Complete | CycleOutcome::ScanBudgetExhausted
        )
    {
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

/// Run one listing request under [`LIST_PAGE_TIMEOUT`], naming the entry offset on failure.
async fn bounded_list_step<T>(
    step: impl std::future::Future<Output = anyhow::Result<T>>,
    listed_entries: u64,
) -> anyhow::Result<T> {
    tokio::time::timeout(LIST_PAGE_TIMEOUT, step)
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!("listing page timed out")))
        .with_context(|| format!("failed to list janitor objects after {listed_entries} entries"))
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
        if let Some(stop) = ctx.budget.admission_stop(stats, cycle_started) {
            stats.outcome = stop;
            return Ok(());
        }
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

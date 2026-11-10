//! Provider-neutral standalone janitor cycle.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use studiofs_data::janitor::{
    JanitorAge, JanitorDeleteError, JanitorEntry, JanitorFailureClass, JanitorKeyStat,
    classify_object_error, janitor_age, pending_deletion_age,
};
use studiofs_data::object::canonical_object_delete_keys;
use studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome;
use studiofs_meta::{
    JanitorDeletionSettlement, JanitorEligibility, JanitorMetadataError, JanitorSweepCheckpoint,
    JanitorSweepScope, MAX_JANITOR_ELIGIBILITY_BATCH,
};
use tokio_util::sync::CancellationToken;

/// Bound for one remote DELETE. All canonical keys of an object must finish
/// inside the 60-second deletion-claim lease, or the finish is fenced out.
const DELETE_KEY_TIMEOUT: Duration = if cfg!(test) {
    // Short for stalled fakes, yet above live RustFS latency spikes seen on CI (>200 ms).
    Duration::from_secs(1)
} else {
    Duration::from_secs(20)
};
/// Bound for one fallback HEAD, issued only for keys listed without `LastModified`.
const HEAD_KEY_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(20)
};
/// Bound for one listing step; a stalled page fails the cycle. Finished
/// deletions are already confirmed in PostgreSQL, so no completed work is lost.
const LIST_PAGE_TIMEOUT: Duration = if cfg!(test) {
    // Long enough for a loaded CI host to serve a loopback page.
    Duration::from_secs(2)
} else {
    Duration::from_secs(60)
};
/// PostgreSQL deletion-claim lease.
const CLAIM_LEASE: Duration =
    Duration::from_secs(studiofs_meta::JANITOR_DELETION_CLAIM_LEASE_SECS as u64);
/// Longest S3 work for one object: each canonical key (legacy plus fragments) at its timeout.
const OBJECT_DELETE_BOUND: Duration =
    DELETE_KEY_TIMEOUT.saturating_mul(studiofs_data::object::CLOUD_FRAGMENT_COUNT as u32 + 1);
const _: () = assert!(
    OBJECT_DELETE_BOUND.as_secs() < CLAIM_LEASE.as_secs(),
    "canonical key deletes must fit inside the deletion-claim lease"
);
/// Most objects claimed, and settled, per PostgreSQL call.
const DELETION_CLAIM_BATCH: usize = 64;
/// Consecutive objects whose S3 work failed transiently (after the transport's own
/// bounded retries) before the cycle stops admitting work as overloaded.
pub(super) const OVERLOAD_TRIP_OBJECTS: u32 = 5;

/// A bounded operation that did not answer in time; always transient.
#[derive(Debug, thiserror::Error)]
#[error("{0} timed out")]
pub(super) struct OperationTimedOut(pub(super) &'static str);

#[derive(Debug, thiserror::Error)]
#[error("janitor cancellation requested")]
pub(super) struct OperationCancelled;

pub(super) fn is_cancellation(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<OperationCancelled>())
}

/// Classify a cycle error by its typed causes. Unrecognized errors (malformed keys,
/// unordered listings, invalid checkpoints) are terminal so they never loop.
pub(super) fn classify_error(error: &anyhow::Error) -> JanitorFailureClass {
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<opendal::Error>() {
            return classify_object_error(error);
        }
        if let Some(error) = cause.downcast_ref::<JanitorMetadataError>() {
            return match error {
                JanitorMetadataError::UnsafeToDelete(_)
                | JanitorMetadataError::SchemaUnavailable(_) => JanitorFailureClass::Terminal,
                // A lost connection or fenced-out claim leaves durable state to a later cycle.
                JanitorMetadataError::BackendUnavailable(_) | JanitorMetadataError::StaleClaim => {
                    JanitorFailureClass::Transient
                }
            };
        }
        if cause.is::<OperationTimedOut>() {
            return JanitorFailureClass::Transient;
        }
    }
    JanitorFailureClass::Terminal
}

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
    pub(super) queued_candidates_scanned: u64,
    pub(super) skipped_deferred: u64,
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
    /// HEAD probes that failed transiently; their objects were skipped as unknown age.
    pub(super) head_errors: u64,
    /// Dry-run objects already counted as would-delete; bounded by the delete cap.
    dry_run_counted: std::collections::HashSet<u64>,
    /// Objects in a row whose S3 work failed transiently.
    transient_streak: u32,
    pub(super) outcome: CycleOutcome,
    pub(super) scan_position: String,
    pub(super) scan_complete: bool,
}

impl JanitorStats {
    pub(super) fn cycle_outcome(&self) -> CycleOutcome {
        if self.outcome != CycleOutcome::Overloaded
            && (self.head_errors > 0 || self.delete_errors > 0)
        {
            CycleOutcome::TransientErrors
        } else {
            self.outcome
        }
    }
}

/// Why a cycle stopped admitting work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum CycleOutcome {
    /// The whole prefix was listed and every listed candidate was decided.
    #[default]
    Complete,
    /// A cycle had transient object-store or claim-confirmation errors.
    TransientErrors,
    /// `max_deletes` successful deletions were confirmed and admission stopped. Listing
    /// never looks ahead, so the cap met on the last entry also reports this.
    DeleteCapReached,
    /// The listed-entry budget was met before the listing was seen to end.
    ScanBudgetExhausted,
    /// The wall-clock budget ran out before all listed work was admitted.
    TimeBudgetExhausted,
    /// The operator requested a graceful stop.
    Cancelled,
    /// [`OVERLOAD_TRIP_OBJECTS`] objects in a row failed transiently; admission stopped
    /// and the unfinished batch stays queued or before the saved checkpoint.
    Overloaded,
}

impl CycleOutcome {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::DeleteCapReached => "capped (max deletes reached)",
            Self::ScanBudgetExhausted => "partial (scan budget exhausted)",
            Self::TimeBudgetExhausted => "partial (time budget exhausted)",
            Self::Cancelled => "partial (cancelled)",
            Self::Overloaded => "partial (S3 overload circuit open)",
            Self::TransientErrors => "partial (transient errors)",
        }
    }
}

/// Per-cycle work limits. They are checked only where new work is admitted, so a
/// claimed deletion always runs through its PostgreSQL finish.
#[derive(Debug, Clone)]
pub(super) struct RunBudget {
    /// Confirmed successful (dry-run: would-be) deletions; protected and failed
    /// objects never consume it. Deletion is sequential, so it cannot overshoot.
    pub(super) max_deletes: u64,
    /// Listed entries after which no further entry is pulled from the listing.
    pub(super) max_scan_entries: Option<u64>,
    /// Wall-clock time after which no further entry or candidate is admitted.
    pub(super) max_runtime: Option<Duration>,
    /// Stops new listing/candidate admission while an admitted delete drains.
    pub(super) cancel: CancellationToken,
    pub(super) progress: Option<Arc<Mutex<super::ProgressSnapshot>>>,
}

impl RunBudget {
    #[cfg(test)]
    pub(super) fn deletes(max_deletes: u64) -> Self {
        Self {
            max_deletes,
            max_scan_entries: None,
            max_runtime: None,
            cancel: CancellationToken::new(),
            progress: None,
        }
    }

    /// The reason to stop admitting candidates, if the cap or the deadline is met.
    fn admission_stop(&self, stats: &JanitorStats, started: Instant) -> Option<CycleOutcome> {
        if self.cancel.is_cancelled() {
            return Some(CycleOutcome::Cancelled);
        }
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
                .filter(|max| {
                    stats
                        .initial_listing_entries
                        .saturating_add(stats.queued_candidates_scanned)
                        >= *max
                })
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
    async fn list(&self, prefix: &str, after_key: Option<&str>) -> anyhow::Result<Self::Listing>;
    async fn last_modified(&self, key: &str) -> anyhow::Result<Option<SystemTime>>;
    async fn stat_key(&self, key: &str) -> anyhow::Result<JanitorKeyStat> {
        self.last_modified(key).await.map(JanitorKeyStat::Found)
    }
    async fn delete(&self, key: &str) -> Result<(), JanitorDeleteError>;
}

/// One discovered object: its ID, every canonical key to delete, and the
/// listing timestamps of all its adjacent listed keys.
#[derive(Debug)]
struct Candidate {
    object_id: u64,
    delete_keys: Vec<String>,
    last_key: Option<String>,
    /// Newest listed `LastModified` across the object's keys (conservative age).
    newest_listed: Option<SystemTime>,
    /// Listed keys whose page carried no `LastModified`; they need a HEAD.
    untimed_keys: Vec<String>,
    refresh_age: bool,
    queued_eligible_at: Option<i64>,
}

impl Candidate {
    fn from_queue(prefix: &str, object_id: u64, eligible_at: i64) -> anyhow::Result<Self> {
        let key = studiofs_data::object::format_object_key(prefix, object_id);
        let (found_id, delete_keys) = canonical_object_delete_keys(prefix, &key)
            .ok_or_else(|| anyhow::anyhow!("invalid PostgreSQL GC candidate object id"))?;
        anyhow::ensure!(
            found_id == object_id,
            "PostgreSQL GC candidate key does not match its object id"
        );
        Ok(Self {
            object_id,
            delete_keys,
            last_key: None,
            newest_listed: None,
            untimed_keys: Vec::new(),
            refresh_age: false,
            queued_eligible_at: Some(eligible_at.div_euclid(1_000_000)),
        })
    }

    fn from_partial(prefix: &str, object_id: u64, last_key: &str) -> anyhow::Result<Self> {
        let key = studiofs_data::object::format_object_key(prefix, object_id);
        let (found_id, delete_keys) = canonical_object_delete_keys(prefix, &key)
            .ok_or_else(|| anyhow::anyhow!("invalid janitor checkpoint object id"))?;
        anyhow::ensure!(
            found_id == object_id,
            "janitor checkpoint object id is invalid"
        );
        Ok(Self {
            object_id,
            delete_keys,
            last_key: Some(last_key.to_string()),
            newest_listed: None,
            untimed_keys: Vec::new(),
            refresh_age: true,
            queued_eligible_at: None,
        })
    }
}

impl Candidate {
    fn record_key(&mut self, key: String, last_modified: Option<SystemTime>) {
        self.last_key = Some(key.clone());
        match last_modified {
            Some(time) => self.newest_listed = self.newest_listed.max(Some(time)),
            None if !self.untimed_keys.contains(&key) => self.untimed_keys.push(key),
            None => {}
        }
    }
}

/// Fold one listed entry into `current`; return the previous object once a new one starts.
fn discover_object(
    entry: JanitorEntry,
    prefix: &str,
    current: &mut Option<Candidate>,
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
    // Keys of one object are adjacent because `run_one_cycle` rejects a listing
    // that is not in S3's lexicographic order.
    if let Some(candidate) = current
        .as_mut()
        .filter(|candidate| candidate.object_id == object_id)
    {
        candidate.record_key(entry.key, entry.last_modified);
        return None;
    }
    let mut candidate = Candidate {
        object_id,
        delete_keys,
        last_key: None,
        newest_listed: None,
        untimed_keys: Vec::new(),
        refresh_age: false,
        queued_eligible_at: None,
    };
    candidate.record_key(entry.key, entry.last_modified);
    current.replace(candidate)
}

/// Object-level age: the newest timestamp over listed keys plus HEADs of untimed
/// keys. A vanished key, missing value, or transient HEAD failure or timeout leaves the
/// age unknown; a terminal HEAD failure (e.g. access denied) fails the cycle.
async fn observed_object_age<S: JanitorObjectStore>(
    store: &S,
    candidate: &Candidate,
    stats: &mut JanitorStats,
    progress: Option<&Arc<Mutex<super::ProgressSnapshot>>>,
    dry_run: bool,
) -> anyhow::Result<Option<SystemTime>> {
    let keys = if candidate.refresh_age {
        &candidate.delete_keys
    } else {
        &candidate.untimed_keys
    };
    let mut newest = if candidate.refresh_age {
        None
    } else {
        candidate.newest_listed
    };
    for key in keys {
        super::set_live_progress_phase(progress, "head");
        if stats.head_requests == 0 {
            eprintln!("janitor phase=head started");
        }
        stats.head_requests = stats.head_requests.saturating_add(1);
        super::publish_live_progress(progress, stats, dry_run);
        let head_started = Instant::now();
        let result = tokio::time::timeout(HEAD_KEY_TIMEOUT, async {
            if candidate.refresh_age {
                store.stat_key(key).await
            } else {
                // A listed key without a timestamp must still exist to prove its age.
                store.last_modified(key).await.map(JanitorKeyStat::Found)
            }
        })
        .await
        .unwrap_or_else(|_| Err(OperationTimedOut("HEAD").into()));
        stats.head_elapsed += head_started.elapsed();
        match result {
            Ok(JanitorKeyStat::Missing) => {}
            Ok(JanitorKeyStat::Found(Some(time))) => newest = newest.max(Some(time)),
            Ok(JanitorKeyStat::Found(None)) => return Ok(None),
            Err(error) => match classify_error(&error) {
                JanitorFailureClass::Vanished => return Ok(None),
                JanitorFailureClass::Transient => {
                    eprintln!(
                        "janitor: HEAD failed class=transient key={key} object_id={}: {error:#}",
                        candidate.object_id
                    );
                    stats.head_errors = stats.head_errors.saturating_add(1);
                    record_transient_failure(stats);
                    return Ok(None);
                }
                JanitorFailureClass::Terminal => {
                    return Err(error.context(format!(
                        "terminal HEAD failure for janitor object {}",
                        candidate.object_id
                    )));
                }
            },
        }
    }
    Ok(newest)
}

/// Counts one object whose S3 work failed transiently and opens the overload circuit
/// after [`OVERLOAD_TRIP_OBJECTS`] in a row.
fn record_transient_failure(stats: &mut JanitorStats) {
    stats.transient_streak = stats.transient_streak.saturating_add(1);
    if stats.transient_streak >= OVERLOAD_TRIP_OBJECTS && stats.outcome == CycleOutcome::Complete {
        eprintln!(
            "janitor: S3 overload circuit open after {} objects failed in a row; stopping admission",
            stats.transient_streak
        );
        stats.outcome = CycleOutcome::Overloaded;
    }
}

/// Age and dry-run gate for one eligible object; `true` admits it for deletion.
async fn admit_orphan<S: JanitorObjectStore>(
    ctx: &mut JanitorCycleContext<'_, S>,
    candidate: &Candidate,
    now: SystemTime,
    now_unix: i64,
    stats: &mut JanitorStats,
    pending_ts: Option<i64>,
) -> anyhow::Result<bool> {
    let grace = Duration::from_secs(ctx.skip_newer_than_secs);
    let age = match pending_ts {
        // A fresh or future-dated listed key still protects an object marked pending.
        Some(pending_ts) => {
            let pending = pending_deletion_age(now_unix, pending_ts, grace);
            if candidate.refresh_age {
                // Keys listed by an earlier run are not in `newest_listed`; stat every key.
                let observed = observed_object_age(
                    ctx.store,
                    candidate,
                    stats,
                    ctx.budget.progress.as_ref(),
                    ctx.dry_run,
                )
                .await;
                pending.max(janitor_age(now, observed?, grace))
            } else {
                candidate.newest_listed.map_or(pending, |listed| {
                    pending.max(janitor_age(now, Some(listed), grace))
                })
            }
        }
        None => janitor_age(
            now,
            observed_object_age(
                ctx.store,
                candidate,
                stats,
                ctx.budget.progress.as_ref(),
                ctx.dry_run,
            )
            .await?,
            grace,
        ),
    };
    match age {
        JanitorAge::Eligible => {}
        JanitorAge::Young => {
            stats.skipped_young = stats.skipped_young.saturating_add(1);
            super::publish_live_progress(ctx.budget.progress.as_ref(), stats, ctx.dry_run);
            return Ok(false);
        }
        JanitorAge::Unknown => {
            stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
            super::publish_live_progress(ctx.budget.progress.as_ref(), stats, ctx.dry_run);
            return Ok(false);
        }
    }

    if ctx.dry_run {
        // Dry-run neither deletes nor acks, so the listing can revisit a queued object.
        stats.dry_run_counted.insert(candidate.object_id);
        stats.deleted = stats.deleted.saturating_add(1);
        super::publish_live_progress(ctx.budget.progress.as_ref(), stats, ctx.dry_run);
        return Ok(false);
    }
    Ok(true)
}

/// Deletes admitted orphans in claim batches. Two PostgreSQL round trips claim the
/// batch, each claimed object's S3 deletes follow, and one round trip settles the
/// outcomes together with the queued `defers`. An object starts its deletes only
/// while the claim lease still covers [`OBJECT_DELETE_BOUND`]; later objects are
/// released and claimed again. A stop releases every unattempted claim.
async fn delete_orphans<S: JanitorObjectStore>(
    ctx: &mut JanitorCycleContext<'_, S>,
    mut orphans: Vec<Candidate>,
    defers: &mut Vec<u64>,
    cycle_started: Instant,
    stats: &mut JanitorStats,
) -> anyhow::Result<()> {
    while !orphans.is_empty() {
        let ids: Vec<u64> = orphans
            .iter()
            .map(|candidate| candidate.object_id)
            .collect();
        // A claim failure means safety cannot be proven; stop the cycle instead of guessing.
        super::set_live_progress_phase(ctx.budget.progress.as_ref(), "metadata_claim");
        let claimed_at = Instant::now();
        let tokens = tokio::task::block_in_place(|| ctx.metadata.claim_deletions(&ids))
            .with_context(|| {
                format!("failed to claim {} janitor objects for deletion", ids.len())
            })?;
        stats.claims = stats
            .claims
            .saturating_add(u64::try_from(ids.len()).unwrap_or(u64::MAX));
        let mut settlements: Vec<JanitorDeletionSettlement> = defers
            .drain(..)
            .map(|object_id| JanitorDeletionSettlement::Deferred { object_id })
            .collect();
        let mut attempted = Vec::new();
        let mut reclaim = Vec::new();
        let mut terminal = None;
        for candidate in orphans {
            let object_id = candidate.object_id;
            let Some(&token) = tokens.get(&object_id) else {
                stats.claim_protected = stats.claim_protected.saturating_add(1);
                if candidate.queued_eligible_at.is_some() {
                    settlements.push(JanitorDeletionSettlement::Deferred { object_id });
                }
                continue;
            };
            let release = JanitorDeletionSettlement::Released {
                object_id,
                fencing_token: token,
            };
            if terminal.is_none()
                && stats.outcome == CycleOutcome::Complete
                && let Some(stop) = ctx.budget.admission_stop(stats, cycle_started)
            {
                stats.outcome = stop;
            }
            if terminal.is_some() || stats.outcome != CycleOutcome::Complete {
                settlements.push(release);
                continue;
            }
            if !reclaim.is_empty() || claimed_at.elapsed() + OBJECT_DELETE_BOUND > CLAIM_LEASE {
                settlements.push(release);
                reclaim.push(candidate);
                continue;
            }
            let (s3_delete_failed, terminal_error) =
                delete_object_keys(ctx, &candidate, stats).await;
            if !s3_delete_failed {
                stats.transient_streak = 0;
            } else if terminal_error.is_none() {
                record_transient_failure(stats);
            }
            let outcome = if s3_delete_failed {
                UploadedObjectDeletionOutcome::Retryable
            } else {
                UploadedObjectDeletionOutcome::Deleted
            };
            settlements.push(JanitorDeletionSettlement::Finished {
                object_id,
                fencing_token: token,
                outcome,
            });
            attempted.push((object_id, s3_delete_failed));
            terminal = terminal_error.map(|error| (object_id, error));
        }
        // Success needs PostgreSQL confirmation; an unfinished claim expires and is retried.
        super::set_live_progress_phase(ctx.budget.progress.as_ref(), "metadata_finish");
        let finished = settle_or_log(ctx, &settlements);
        for (object_id, s3_delete_failed) in attempted {
            let finish_failed = !finished.contains(&object_id);
            if finish_failed {
                eprintln!("janitor: failed to finish deletion claim object_id={object_id}");
            } else {
                stats.claim_finishes = stats.claim_finishes.saturating_add(1);
            }
            if s3_delete_failed || finish_failed {
                stats.delete_errors = stats.delete_errors.saturating_add(1);
            } else {
                stats.deleted = stats.deleted.saturating_add(1);
            }
        }
        super::publish_live_progress(ctx.budget.progress.as_ref(), stats, ctx.dry_run);
        if let Some((object_id, error)) = terminal {
            anyhow::bail!("terminal S3 delete failure for janitor object {object_id}: {error}");
        }
        orphans = reclaim;
    }
    Ok(())
}

/// Settles deletion outcomes and queue deferrals; returns the confirmed claims. A
/// failure confirms none, so their claims expire and a later cycle retries them.
fn settle_or_log<S: JanitorObjectStore>(
    ctx: &mut JanitorCycleContext<'_, S>,
    settlements: &[JanitorDeletionSettlement],
) -> std::collections::HashSet<u64> {
    if settlements.is_empty() {
        return std::collections::HashSet::new();
    }
    tokio::task::block_in_place(|| ctx.metadata.settle_deletions(settlements)).unwrap_or_else(
        |error| {
            eprintln!(
                "janitor: failed to settle {} deletion claims or GC candidates: {error:#}",
                settlements.len()
            );
            std::collections::HashSet::new()
        },
    )
}

/// Deletes every canonical key of one claimed object. Returns whether its S3 work
/// failed, and the error of a terminal failure. The whole object is retried later
/// after any failure, so the first one stops further requests.
async fn delete_object_keys<S: JanitorObjectStore>(
    ctx: &mut JanitorCycleContext<'_, S>,
    candidate: &Candidate,
    stats: &mut JanitorStats,
) -> (bool, Option<String>) {
    for key in &candidate.delete_keys {
        super::set_live_progress_phase(ctx.budget.progress.as_ref(), "delete");
        if stats.delete_requests == 0 {
            eprintln!("janitor phase=delete started");
        }
        stats.delete_requests = stats.delete_requests.saturating_add(1);
        super::publish_live_progress(ctx.budget.progress.as_ref(), stats, ctx.dry_run);
        let delete_started = Instant::now();
        let result = tokio::time::timeout(DELETE_KEY_TIMEOUT, ctx.store.delete(key))
            .await
            .unwrap_or_else(|_| Err(JanitorDeleteError::Other("delete timed out".to_owned())));
        stats.delete_elapsed += delete_started.elapsed();
        let (class, error) = match result {
            Ok(()) | Err(JanitorDeleteError::NotFound) => continue,
            Err(JanitorDeleteError::Other(error)) => ("transient", error),
            Err(JanitorDeleteError::Terminal(error)) => ("terminal", error),
        };
        eprintln!(
            "janitor: failed to delete object class={class} key={key} object_id={}: {error}",
            candidate.object_id
        );
        return (true, (class == "terminal").then_some(error));
    }
    (false, None)
}

#[cfg(test)]
pub(super) async fn run_one_cycle<S: JanitorObjectStore>(
    ctx: JanitorCycleContext<'_, S>,
) -> anyhow::Result<JanitorStats> {
    run_one_cycle_inner(ctx, JanitorSweepCheckpoint::default(), None, false).await
}

pub(super) async fn run_one_cycle_resumable<S: JanitorObjectStore>(
    ctx: JanitorCycleContext<'_, S>,
    checkpoint: JanitorSweepCheckpoint,
    scope: &JanitorSweepScope<'_>,
    persist_checkpoint: bool,
) -> anyhow::Result<JanitorStats> {
    run_one_cycle_inner(ctx, checkpoint, Some(scope), persist_checkpoint).await
}

async fn run_one_cycle_inner<S: JanitorObjectStore>(
    mut ctx: JanitorCycleContext<'_, S>,
    mut checkpoint: JanitorSweepCheckpoint,
    scope: Option<&JanitorSweepScope<'_>>,
    persist_checkpoint: bool,
) -> anyhow::Result<JanitorStats> {
    let mut stats = JanitorStats {
        // Early budget returns leave the sweep where it started.
        scan_position: scan_position(&checkpoint),
        ..JanitorStats::default()
    };
    let cycle_started = Instant::now();
    let now = SystemTime::now();
    let now_unix = i64::try_from(
        now.duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs(),
    )
    .unwrap_or(i64::MAX);

    let mut candidate_cursor: Option<(i64, u64)> = None;
    loop {
        if let Some(stop) = ctx.budget.admission_stop(&stats, cycle_started) {
            stats.outcome = stop;
            break;
        }
        let remaining_scan = ctx
            .budget
            .max_scan_entries
            .map(|max| {
                max.saturating_sub(
                    stats
                        .initial_listing_entries
                        .saturating_add(stats.queued_candidates_scanned),
                )
            })
            .unwrap_or(MAX_JANITOR_ELIGIBILITY_BATCH as u64);
        if remaining_scan == 0 {
            stats.outcome = CycleOutcome::ScanBudgetExhausted;
            break;
        }
        let page_limit = usize::try_from(remaining_scan.min(MAX_JANITOR_ELIGIBILITY_BATCH as u64))
            .unwrap_or(MAX_JANITOR_ELIGIBILITY_BATCH);
        let page = tokio::task::block_in_place(|| {
            ctx.metadata.gc_candidate_page(
                candidate_cursor,
                page_limit,
                now_unix
                    .saturating_sub(i64::try_from(ctx.skip_newer_than_secs).unwrap_or(i64::MAX)),
                !ctx.dry_run,
            )
        })?;
        if page.is_empty() {
            break;
        }
        stats.queued_candidates_scanned = stats
            .queued_candidates_scanned
            .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
        stats.scanned_files = stats
            .scanned_files
            .saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
        super::publish_live_progress(ctx.budget.progress.as_ref(), &stats, ctx.dry_run);
        let candidates = page
            .iter()
            .map(|(object_id, eligible_at)| {
                Candidate::from_queue(ctx.prefix, *object_id, *eligible_at)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        process_candidate_batch(
            &mut ctx,
            candidates,
            now,
            now_unix,
            cycle_started,
            &mut stats,
        )
        .await?;
        if stats.outcome == CycleOutcome::Complete
            && let Some(stop) = ctx.budget.admission_stop(&stats, cycle_started)
        {
            stats.outcome = stop;
        }
        candidate_cursor = page.last().map(|(id, eligible_at)| (*eligible_at, *id));
        if stats.outcome != CycleOutcome::Complete {
            break;
        }
        if page.len() < page_limit {
            break;
        }
    }
    if stats.outcome != CycleOutcome::Complete {
        stats.cycle_elapsed = cycle_started.elapsed();
        return Ok(stats);
    }
    if let Some(stop) = ctx.budget.admission_stop(&stats, cycle_started) {
        stats.outcome = stop;
        stats.cycle_elapsed = cycle_started.elapsed();
        return Ok(stats);
    }

    let persist_scope = if persist_checkpoint {
        Some(scope.ok_or_else(|| anyhow::anyhow!("janitor checkpoint scope is missing"))?)
    } else {
        None
    };
    let listing_result = run_start_after_listing(
        &mut ctx,
        &mut stats,
        &mut checkpoint,
        persist_scope,
        now,
        now_unix,
        cycle_started,
    )
    .await;
    // Only the opening list request of a sweep reaches here cancelled,
    // before any checkpoint change; mid-listing cancels stop like other budgets.
    if let Err(error) = listing_result {
        if is_cancellation(&error) {
            stats.outcome = CycleOutcome::Cancelled;
            stats.scan_complete = false;
            stats.scan_position = scan_position(&checkpoint);
            stats.cycle_elapsed = cycle_started.elapsed();
            return Ok(stats);
        }
        return Err(error);
    }

    if let Some(scope) = persist_scope {
        tokio::task::block_in_place(|| ctx.metadata.save_sweep_checkpoint(*scope, &checkpoint))?;
    }
    stats.scan_position = scan_position(&checkpoint);

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

fn scan_position(checkpoint: &JanitorSweepCheckpoint) -> String {
    checkpoint
        .after_key
        .clone()
        .unwrap_or_else(|| "start".to_string())
}

async fn run_start_after_listing<S: JanitorObjectStore>(
    ctx: &mut JanitorCycleContext<'_, S>,
    stats: &mut JanitorStats,
    checkpoint: &mut JanitorSweepCheckpoint,
    persist_scope: Option<&JanitorSweepScope<'_>>,
    now: SystemTime,
    now_unix: i64,
    cycle_started: Instant,
) -> anyhow::Result<()> {
    // Last position whose closed candidates were fully processed; a stop rolls back only to here.
    let mut committed = checkpoint.clone();
    super::set_live_progress_phase(ctx.budget.progress.as_ref(), "listing");
    eprintln!("janitor phase=initial_listing started");
    let listing_started = Instant::now();
    let mut listing = bounded_list_step(
        ctx.store.list(ctx.prefix, checkpoint.after_key.as_deref()),
        stats.initial_listing_entries,
        &ctx.budget.cancel,
    )
    .await?;
    stats.initial_listing_elapsed = listing_started.elapsed();
    let mut current = match (
        checkpoint.pending_object_id,
        checkpoint.pending_key.as_deref(),
    ) {
        (Some(object_id), Some(last_key)) => {
            Some(Candidate::from_partial(ctx.prefix, object_id, last_key)?)
        }
        (None, None) => None,
        _ => anyhow::bail!("janitor sweep checkpoint pending object is incomplete"),
    };
    let mut last_listing_key = checkpoint.after_key.clone().unwrap_or_default();
    let mut candidates = Vec::with_capacity(MAX_JANITOR_ELIGIBILITY_BATCH);
    let mut reached_eof = false;
    loop {
        if let Some(stop) = ctx.budget.listing_stop(stats, cycle_started) {
            stats.outcome = stop;
            break;
        }
        super::set_live_progress_phase(ctx.budget.progress.as_ref(), "listing");
        let wait_started = Instant::now();
        let entry = match bounded_list_step(
            listing.next_entry(),
            stats.initial_listing_entries,
            &ctx.budget.cancel,
        )
        .await
        {
            Err(error) if is_cancellation(&error) => {
                stats.outcome = CycleOutcome::Cancelled;
                break;
            }
            result => result?,
        };
        stats.initial_listing_elapsed += wait_started.elapsed();
        let Some(entry) = entry else {
            reached_eof = true;
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
        // Directory entries (e.g. the listed prefix itself) need not sort after `start_after`.
        if entry.is_file {
            anyhow::ensure!(
                entry.key >= last_listing_key,
                "janitor object listing is not in lexicographic order after {} entries; refusing to checkpoint",
                stats.initial_listing_entries
            );
            last_listing_key.clone_from(&entry.key);
            checkpoint.after_key = Some(entry.key.clone());
        }

        let canonical =
            entry.is_file && canonical_object_delete_keys(ctx.prefix, &entry.key).is_some();
        if entry.is_file && !canonical {
            let closes_current = current.as_ref().is_some_and(|candidate| {
                candidate
                    .delete_keys
                    .iter()
                    .max()
                    .is_some_and(|last_key| entry.key > *last_key)
            });
            if closes_current && let Some(candidate) = current.take() {
                candidates.push(candidate);
            }
        }
        if let Some(candidate) = discover_object(entry, ctx.prefix, &mut current, stats) {
            candidates.push(candidate);
        }
        super::publish_live_progress(ctx.budget.progress.as_ref(), stats, ctx.dry_run);
        checkpoint.pending_object_id = current.as_ref().map(|candidate| candidate.object_id);
        checkpoint.pending_key = current
            .as_ref()
            .and_then(|candidate| candidate.last_key.clone());

        stats.peak_buffered_candidates = stats
            .peak_buffered_candidates
            .max(u64::try_from(candidates.len()).unwrap_or(u64::MAX));
        if candidates.len() == MAX_JANITOR_ELIGIBILITY_BATCH {
            process_candidate_batch(
                ctx,
                std::mem::take(&mut candidates),
                now,
                now_unix,
                cycle_started,
                stats,
            )
            .await?;
            if stats.outcome != CycleOutcome::Complete {
                *checkpoint = committed.clone();
                stats.scan_complete = false;
                break;
            }
            committed = checkpoint.clone();
            // Saved per batch, so a later listing or metadata failure keeps this progress.
            if let Some(scope) = persist_scope {
                tokio::task::block_in_place(|| {
                    ctx.metadata.save_sweep_checkpoint(*scope, &committed)
                })?;
            }
        }
    }
    eprintln!(
        "janitor phase=initial_listing completed duration_ms={} entries={} outcome={:?}",
        super::duration_ms(stats.initial_listing_elapsed),
        stats.initial_listing_entries,
        stats.outcome
    );
    if reached_eof {
        candidates.extend(current.take());
        checkpoint.after_key = None;
        checkpoint.pending_object_id = None;
        checkpoint.pending_key = None;
        stats.scan_complete = true;
    }
    if !candidates.is_empty() {
        let listing_outcome = stats.outcome;
        stats.outcome = CycleOutcome::Complete;
        process_candidate_batch(ctx, candidates, now, now_unix, cycle_started, stats).await?;
        if stats.outcome != CycleOutcome::Complete {
            *checkpoint = committed;
            stats.scan_complete = false;
        } else {
            stats.outcome = listing_outcome;
        }
    }
    if stats.outcome == CycleOutcome::Complete && !reached_eof {
        stats.outcome = CycleOutcome::ScanBudgetExhausted;
    }
    Ok(())
}

/// Run one listing request under [`LIST_PAGE_TIMEOUT`], naming the entry offset on failure.
async fn bounded_list_step<T>(
    step: impl std::future::Future<Output = anyhow::Result<T>>,
    listed_entries: u64,
    cancel: &CancellationToken,
) -> anyhow::Result<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(OperationCancelled.into()),
        result = tokio::time::timeout(LIST_PAGE_TIMEOUT, step) => result
            .unwrap_or_else(|_| Err(OperationTimedOut("listing page").into())),
    }
    .with_context(|| format!("failed to list janitor objects after {listed_entries} entries"))
}

async fn process_candidate_batch<S: JanitorObjectStore>(
    ctx: &mut JanitorCycleContext<'_, S>,
    mut candidates: Vec<Candidate>,
    now: SystemTime,
    now_unix: i64,
    cycle_started: Instant,
    stats: &mut JanitorStats,
) -> anyhow::Result<()> {
    if ctx.dry_run {
        candidates.retain(|candidate| !stats.dry_run_counted.contains(&candidate.object_id));
        if candidates.is_empty() {
            return Ok(());
        }
    }
    let ids: Vec<u64> = candidates
        .iter()
        .map(|candidate| candidate.object_id)
        .collect();
    // The metadata session owns a synchronous runtime; leave this worker before its block_on.
    let eligible_before =
        now_unix.saturating_sub(i64::try_from(ctx.skip_newer_than_secs).unwrap_or(i64::MAX));
    super::set_live_progress_phase(ctx.budget.progress.as_ref(), "metadata_eligibility");
    let protection =
        tokio::task::block_in_place(|| ctx.metadata.check_eligibility(&ids, eligible_before))?;
    // Resolve every decision before any delete so one unknown outcome retains the whole batch.
    enum Decision {
        Protected,
        Deferred,
        Eligible(Option<i64>),
    }
    let decisions = candidates
        .into_iter()
        .map(|candidate| match protection.get(&candidate.object_id) {
            Some(JanitorEligibility::Protected) => Ok((candidate, Decision::Protected)),
            Some(JanitorEligibility::Deferred) => Ok((candidate, Decision::Deferred)),
            Some(&JanitorEligibility::Eligible { pending_deletion }) => {
                let oldest = candidate.queued_eligible_at;
                Ok((
                    candidate,
                    Decision::Eligible(pending_deletion.into_iter().chain(oldest).max()),
                ))
            }
            None | Some(JanitorEligibility::Unknown) => Err(anyhow::anyhow!(
                "janitor eligibility is unknown; refusing to delete candidate batch"
            )),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut orphans = Vec::new();
    let mut defers = Vec::new();
    for (candidate, decision) in decisions {
        if stats.outcome != CycleOutcome::Complete {
            break;
        }
        if let Some(stop) = ctx.budget.admission_stop(stats, cycle_started) {
            stats.outcome = stop;
            break;
        }
        match decision {
            Decision::Protected => {
                stats.referenced_files = stats.referenced_files.saturating_add(1);
                super::publish_live_progress(ctx.budget.progress.as_ref(), stats, ctx.dry_run);
                // Delays a protected queue row so it does not re-occupy the head of every later page.
                if !ctx.dry_run && candidate.queued_eligible_at.is_some() {
                    defers.push(candidate.object_id);
                }
            }
            Decision::Deferred => {
                stats.skipped_deferred = stats.skipped_deferred.saturating_add(1);
            }
            Decision::Eligible(pending_deletion) => {
                stats.orphan_candidates = stats.orphan_candidates.saturating_add(1);
                if stats.time_to_first_candidate.is_none() {
                    stats.time_to_first_candidate = Some(cycle_started.elapsed());
                }
                if admit_orphan(ctx, &candidate, now, now_unix, stats, pending_deletion).await? {
                    orphans.push(candidate);
                }
                // Claim no more objects than the delete cap still admits.
                let remaining_deletes =
                    usize::try_from(ctx.budget.max_deletes.saturating_sub(stats.deleted))
                        .unwrap_or(usize::MAX);
                if orphans.len() >= DELETION_CLAIM_BATCH.min(remaining_deletes) {
                    let batch = std::mem::take(&mut orphans);
                    delete_orphans(ctx, batch, &mut defers, cycle_started, stats).await?;
                }
            }
        }
    }
    if stats.outcome == CycleOutcome::Complete && !orphans.is_empty() {
        delete_orphans(ctx, orphans, &mut defers, cycle_started, stats).await?;
    }
    let defers: Vec<JanitorDeletionSettlement> = defers
        .into_iter()
        .map(|object_id| JanitorDeletionSettlement::Deferred { object_id })
        .collect();
    settle_or_log(ctx, &defers);
    Ok(())
}

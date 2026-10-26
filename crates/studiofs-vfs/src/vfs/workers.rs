use futures::StreamExt;
use opendal::ErrorKind;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::{ControlFlow, Deref};
use std::sync::Arc;
use studiofs_meta::engine::{
    LocalStorageCompactionResult, LocalStorageDiagnostics, MetadataCoherenceProgress,
};
use tokio::time::MissedTickBehavior;

use super::*;
use crate::coherence::InvalidationAcceptance;
use crate::vfs::upload_lifecycle::UploadLifecycleWorker;
use crate::{JanitorMeta, VfsMeta};

const JANITOR_RECONCILIATION_CONCURRENCY: usize = 32;
const JANITOR_RECONCILIATION_FINISH_CONCURRENCY: usize = 16;
const JANITOR_OBJECT_DELETE_CONCURRENCY: usize = 32;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JanitorObjectDeletionResult {
    Deleted,
    Protected,
    NotStarted,
    Retryable,
}

/// Per-object phase durations; the queue sums them across overlapping tasks,
/// so totals attribute time to PG versus object storage, not wall clock.
#[derive(Clone, Copy, Debug, Default)]
struct JanitorObjectDeletionTimings {
    claim: Duration,
    remote_delete: Duration,
    finish: Duration,
}

impl JanitorObjectDeletionTimings {
    fn add(&mut self, other: Self) {
        self.claim = self.claim.saturating_add(other.claim);
        self.remote_delete = self.remote_delete.saturating_add(other.remote_delete);
        self.finish = self.finish.saturating_add(other.finish);
    }
}

fn elapsed_ms(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

/// Clock skew can date a pending deletion in the future; treat it as young
/// instead of letting a negative age wrap to an arbitrarily old one.
fn pending_deletion_is_old_enough(now_unix: i64, deleted_at_unix: i64, grace: Duration) -> bool {
    u64::try_from(now_unix.saturating_sub(deleted_at_unix)).is_ok_and(|age| age >= grace.as_secs())
}

/// A `Retryable` finish releases the object-level reconciliation lease, so run
/// every `RemoteConfirmed` finish for an object first and keep only one
/// `Retryable` finish per object; later ones would match no leased row.
fn order_reconciliation_finishes(
    outcomes: &mut Vec<(
        studiofs_meta::snapshot_ports::UploadedFragmentReconciliationClaim,
        studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome,
    )>,
) {
    use studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::Retryable;
    outcomes.sort_by_key(|(claim, outcome)| {
        (
            claim.entry.object_id,
            *outcome == Retryable,
            claim.entry.fragment_index,
        )
    });
    outcomes.dedup_by(|later, earlier| {
        later.0.entry.object_id == earlier.0.entry.object_id
            && later.1 == Retryable
            && earlier.1 == Retryable
    });
}

/// Reads one uploaded fragment back and compares it with its durable proof.
async fn prove_reconciliation_claim(
    data_store: &DataStore,
    shutdown: &CancellationToken,
    cycle: u64,
    claim: studiofs_meta::snapshot_ports::UploadedFragmentReconciliationClaim,
) -> (
    studiofs_meta::snapshot_ports::UploadedFragmentReconciliationClaim,
    studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome,
) {
    let proof = run_with_timeout_or_cancel(
        shutdown,
        REMOTE_OBJECT_IO_TIMEOUT,
        data_store.get_payload_by_key_with_stats(&claim.entry.object_key),
    )
    .await;
    let entry = &claim.entry;
    let retry_reason = match proof {
        TimeoutOrCancel::Completed(Ok((remote_payload, _))) => {
            let actual_byte_length = remote_payload.len() as u64;
            if actual_byte_length != entry.expected_byte_length {
                debug!(
                    cycle,
                    object_id = entry.object_id,
                    fragment_index = entry.fragment_index,
                    expected_byte_length = entry.expected_byte_length,
                    actual_byte_length,
                    "janitor: reconciliation proof length mismatch"
                );
                Some("length_mismatch")
            } else {
                let mut hasher = Sha256::new();
                for chunk in remote_payload {
                    hasher.update(&chunk);
                }
                let remote_sha256: [u8; 32] = hasher.finalize().into();
                (remote_sha256 != entry.expected_sha256).then_some("sha256_mismatch")
            }
        }
        TimeoutOrCancel::Completed(Err(error)) => {
            debug!(
                cycle,
                object_id = entry.object_id,
                fragment_index = entry.fragment_index,
                error = %error,
                "janitor: reconciliation proof read failed"
            );
            Some("read_failed")
        }
        TimeoutOrCancel::TimedOut => Some("timed_out"),
        TimeoutOrCancel::Cancelled => Some("cancelled"),
    };
    let outcome = match retry_reason {
        None => {
            studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::RemoteConfirmed
        }
        Some(reason) => {
            debug!(
                cycle,
                object_id = entry.object_id,
                fragment_index = entry.fragment_index,
                reason,
                "janitor: reconciliation proof retryable"
            );
            studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::Retryable
        }
    };
    (claim, outcome)
}

fn janitor_object_delete_keys(
    data_prefix: &str,
    object_id: u64,
    listed_key: &str,
) -> Option<Vec<String>> {
    let legacy_key = format_object_key(data_prefix, object_id);
    let fragment_keys = format_object_fragment_keys(data_prefix, object_id);
    if listed_key != legacy_key && !fragment_keys.iter().any(|key| key == listed_key) {
        return None;
    }
    Some(std::iter::once(legacy_key).chain(fragment_keys).collect())
}

/// Finishes claims sequentially per key and concurrently across keys. Returns
/// `(attempted, errors, unfinished)`; after shutdown no further finish starts,
/// and unstarted claims keep their reconciliation lease for retry after expiry.
async fn finish_janitor_reconciliation_claims<T, K, Fut, E>(
    pending: impl IntoIterator<Item = T>,
    shutdown: &CancellationToken,
    max_concurrency: usize,
    key_for: impl Fn(&T) -> K,
    finish: impl Fn(T) -> Fut,
) -> (usize, usize, usize)
where
    K: Ord,
    Fut: Future<Output = Result<(), E>>,
{
    let mut groups: BTreeMap<K, Vec<T>> = BTreeMap::new();
    for claim in pending {
        groups.entry(key_for(&claim)).or_default().push(claim);
    }
    let finish = &finish;
    futures::stream::iter(groups.into_values())
        .map(|group| async move {
            let (mut attempted, mut errors) = (0, 0);
            let total = group.len();
            for claim in group {
                if shutdown.is_cancelled() {
                    break;
                }
                attempted += 1;
                if finish(claim).await.is_err() {
                    errors += 1;
                }
            }
            (attempted, errors, total - attempted)
        })
        .buffer_unordered(max_concurrency)
        .fold((0, 0, 0), |(a, e, u), (ga, ge, gu)| async move {
            (a + ga, e + ge, u + gu)
        })
        .await
}

struct JanitorDeletionQueue<Fut> {
    in_flight: futures::stream::FuturesUnordered<Fut>,
    successful_deletes: u64,
    retryable_failures: u64,
    timings: JanitorObjectDeletionTimings,
    max_deletes: u64,
    max_concurrency: usize,
}

impl<Fut> JanitorDeletionQueue<Fut>
where
    Fut: Future<Output = (JanitorObjectDeletionResult, JanitorObjectDeletionTimings)>,
{
    fn new(max_deletes: u64, max_concurrency: usize) -> Self {
        Self {
            in_flight: futures::stream::FuturesUnordered::new(),
            successful_deletes: 0,
            retryable_failures: 0,
            timings: JanitorObjectDeletionTimings::default(),
            max_deletes,
            max_concurrency,
        }
    }

    fn reached_cap(&self) -> bool {
        self.successful_deletes >= self.max_deletes
    }

    fn has_in_flight(&self) -> bool {
        !self.in_flight.is_empty()
    }

    fn push(&mut self, future: Fut) {
        self.in_flight.push(future);
    }

    async fn wait_for_slot(&mut self) {
        loop {
            let remaining = self.max_deletes.saturating_sub(self.successful_deletes);
            let success_slots = usize::try_from(remaining).unwrap_or(usize::MAX);
            let in_flight_limit = success_slots.min(self.max_concurrency);
            if self.in_flight.len() < in_flight_limit {
                return;
            }
            if !self.complete_one().await {
                return;
            }
            if self.reached_cap() {
                return;
            }
        }
    }

    async fn complete_one(&mut self) -> bool {
        let Some((result, timings)) = self.in_flight.next().await else {
            return false;
        };
        self.timings.add(timings);
        match result {
            JanitorObjectDeletionResult::Deleted => {
                self.successful_deletes = self.successful_deletes.saturating_add(1);
            }
            JanitorObjectDeletionResult::Retryable => {
                self.retryable_failures = self.retryable_failures.saturating_add(1);
            }
            JanitorObjectDeletionResult::Protected | JanitorObjectDeletionResult::NotStarted => {}
        }
        true
    }

    async fn drain(&mut self) {
        while self.complete_one().await {}
    }
}
const COHERENCE_PENDING_INVALIDATION_CAPACITY: usize = 128;
const COHERENCE_INVALIDATION_RETRY_LIMIT: u8 = 3;
/// Bounded local re-check of writer-deferred generations; nothing else wakes the
/// invalidation worker when a writer releases or its dirty state drains.
const COHERENCE_DEFERRED_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// Minimum spacing of reset-driven whole-volume reconciliation walks. Ordinary
/// changes reconcile only the inodes their semantic batches name; a burst of
/// journal resets must not repeat an O(volume) walk per pass.
const METADATA_FULL_RECONCILE_MIN_INTERVAL: Duration = Duration::from_secs(60);
/// Backstop walk spacing for scopes the journal could not name, run only when a
/// pass has metadata changes to reconcile.
const METADATA_FULL_RECONCILE_BACKSTOP_INTERVAL: Duration = Duration::from_secs(15 * 60);
const LOCAL_REDB_COMPACTION_POLL_INTERVAL: Duration = Duration::from_secs(60);
const LOCAL_REDB_COMPACTION_IDLE_WINDOW: Duration = Duration::from_secs(15 * 60);
const LOCAL_REDB_COMPACTION_DIAGNOSTIC_RECHECK: Duration = Duration::from_secs(60 * 60);
const LOCAL_REDB_COMPACTION_BUSY_BACKOFF: Duration = Duration::from_secs(30 * 60);
const LOCAL_REDB_COMPACTION_ERROR_BACKOFF: Duration = Duration::from_secs(6 * 60 * 60);
const LOCAL_REDB_COMPACTION_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);
const LOCAL_REDB_COMPACTION_MIN_FILE_BYTES: u64 = 4 * 1024 * 1024;
const LOCAL_REDB_COMPACTION_MIN_FRAGMENTED_BYTES: u64 = 1024 * 1024;
const LOCAL_REDB_COMPACTION_MIN_FRAGMENTATION_RATIO_DENOMINATOR: u64 = 4;

/// Tracks foreground quiet time and the next local-compaction retry deadline.
#[derive(Debug)]
struct LocalRedbCompactionSchedule {
    last_activity_generation: u64,
    idle_since: Option<Instant>,
    next_probe_at: Option<Instant>,
}

impl LocalRedbCompactionSchedule {
    /// Initialize with the observed generation and no accumulated quiet time.
    fn new(activity_generation: u64) -> Self {
        Self {
            last_activity_generation: activity_generation,
            idle_since: None,
            next_probe_at: None,
        }
    }

    /// Reset on observed metadata/read activity; retain quiet time during maintenance.
    fn observe_activity(
        &mut self,
        now: Instant,
        activity_generation: u64,
        active_calls: usize,
        recent_foreground_reads_active: bool,
        shutdown_requested: bool,
    ) -> Option<Duration> {
        if shutdown_requested
            || activity_generation != self.last_activity_generation
            || recent_foreground_reads_active
        {
            self.last_activity_generation = activity_generation;
            self.idle_since = None;
            return None;
        }
        if active_calls > 0 {
            return None;
        }

        let idle_since = *self.idle_since.get_or_insert(now);
        Some(now.saturating_duration_since(idle_since))
    }

    /// Admit a probe after the quiet window and any previous retry deadline.
    fn ready_to_probe(&self, now: Instant, idle_for: Duration) -> bool {
        idle_for >= LOCAL_REDB_COMPACTION_IDLE_WINDOW
            && self.next_probe_at.is_none_or(|next| now >= next)
    }

    /// Set the earliest time at which the next probe may run.
    fn defer_probe(&mut self, now: Instant, delay: Duration) {
        self.next_probe_at = Some(now + delay);
    }

    /// Clear accumulated quiet time after recording a new generation.
    fn reset_idle(&mut self, activity_generation: u64) {
        self.last_activity_generation = activity_generation;
        self.idle_since = None;
    }

    /// Schedule the bounded retry interval associated with this attempt's outcome.
    fn record_attempt<E>(
        &mut self,
        now: Instant,
        result: &Result<LocalStorageCompactionResult, E>,
    ) {
        let delay = match result {
            Ok(LocalStorageCompactionResult::SkippedBusy) => LOCAL_REDB_COMPACTION_BUSY_BACKOFF,
            Err(_) => LOCAL_REDB_COMPACTION_ERROR_BACKOFF,
            Ok(LocalStorageCompactionResult::Compacted | LocalStorageCompactionResult::NoWork) => {
                LOCAL_REDB_COMPACTION_COOLDOWN
            }
        };
        self.defer_probe(now, delay);
    }
}

/// Apply physical-size, absolute-fragmentation, and relative-fragmentation thresholds.
fn local_redb_compaction_worthwhile(diagnostics: &LocalStorageDiagnostics) -> bool {
    let (Some(file_bytes), Some(fragmented_bytes)) = (
        diagnostics.physical_file_bytes,
        diagnostics.fragmented_bytes,
    ) else {
        return false;
    };

    file_bytes >= LOCAL_REDB_COMPACTION_MIN_FILE_BYTES
        && fragmented_bytes >= LOCAL_REDB_COMPACTION_MIN_FRAGMENTED_BYTES
        && fragmented_bytes
            .saturating_mul(LOCAL_REDB_COMPACTION_MIN_FRAGMENTATION_RATIO_DENOMINATOR)
            >= file_bytes
}

/// Retry missing or incomplete samples quickly and complete below-threshold samples hourly.
fn local_redb_compaction_diagnostics_retry_delay(
    diagnostics: Option<&LocalStorageDiagnostics>,
) -> Duration {
    if diagnostics.is_none_or(|diagnostics| {
        diagnostics.physical_file_bytes.is_none() || diagnostics.fragmented_bytes.is_none()
    }) {
        LOCAL_REDB_COMPACTION_POLL_INTERVAL
    } else {
        LOCAL_REDB_COMPACTION_DIAGNOSTIC_RECHECK
    }
}

struct MetadataTraversalSnapshot {
    inodes: HashSet<Ino>,
    payloads: HashMap<Ino, Vec<u64>>,
    entries: HashSet<(Ino, Vec<u8>, Ino)>,
}

/// Inodes named by drained semantic batches that still await payload
/// reconciliation, or a reset that requires a whole-volume walk.
#[derive(Debug, Default)]
pub(super) struct SemanticReconcileScope {
    inodes: HashSet<Ino>,
    reset: bool,
    /// Known inodes were already retired from the kernel for the pending reset.
    reset_retired: bool,
}

impl SemanticReconcileScope {
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.inodes.is_empty() && !self.reset
    }

    fn observe(&mut self, batch: &studiofs_meta::coherence::InvalidationBatch) {
        use studiofs_meta::coherence::InvalidationIntent;
        for intent in &batch.intents {
            match intent {
                InvalidationIntent::Attributes { ino }
                | InvalidationIntent::Content { ino }
                | InvalidationIntent::Directory { ino } => {
                    self.inodes.insert(*ino);
                }
                // The pre-mutation binding is the unlinked or replaced inode.
                InvalidationIntent::Entry { ino, .. } => self.inodes.extend(*ino),
                InvalidationIntent::Reset => {
                    self.reset = true;
                    self.reset_retired = false;
                }
            }
        }
    }
}

/// How often the invalidation worker reconciled by whole-volume walk versus by
/// the inode scope of drained semantic batches.
#[derive(Debug, Default)]
pub(super) struct MetadataReconcileCounters {
    full_walks: AtomicU64,
    scoped_passes: AtomicU64,
}

impl MetadataReconcileCounters {
    #[cfg(test)]
    pub(super) fn full_walks(&self) -> u64 {
        self.full_walks.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn scoped_passes(&self) -> u64 {
        self.scoped_passes.load(Ordering::Relaxed)
    }
}

/// Reconciliation state carried across metadata invalidation passes.
#[derive(Default)]
pub(super) struct MetadataReconcileState {
    inodes: Option<HashSet<Ino>>,
    entries: Option<HashSet<(Ino, Vec<u8>, Ino)>>,
    /// Payload baseline; whole-volume after the first walk, then kept current
    /// per inode by scoped passes.
    payloads: Option<HashMap<Ino, Vec<u64>>>,
    last_full_walk: Option<Instant>,
    pub(super) scope: SemanticReconcileScope,
}

impl MetadataReconcileState {
    fn full_walk_due(&self, now: Instant) -> bool {
        let (Some(last_full_walk), Some(_)) = (self.last_full_walk, &self.payloads) else {
            return true;
        };
        let elapsed = now.saturating_duration_since(last_full_walk);
        elapsed >= METADATA_FULL_RECONCILE_BACKSTOP_INTERVAL
            || (self.scope.reset && elapsed >= METADATA_FULL_RECONCILE_MIN_INTERVAL)
    }
}

/// Current payload state of the inodes one scoped pass reconciles.
struct ScopedMetadataPayloads {
    present: HashSet<Ino>,
    payloads: HashMap<Ino, Vec<u64>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetadataReconcileOutcome {
    Reconciled,
    Failed,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MetadataInvalidationWakeReason {
    BackendNotify,
    FallbackPoll,
    /// A writer-deferred invalidation generation may have become deliverable.
    DeferredRetry,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct UploadTroubleCounters {
    pub(super) uploads_ok: u64,
    pub(super) uploads_err: u64,
    pub(super) retries: u64,
    pub(super) exhausted: u64,
    pub(super) timeouts: u64,
    pub(super) temporary_errors: u64,
    pub(super) status_5xx_errors: u64,
    pub(super) network_errors: u64,
    pub(super) bytes_uploaded: u64,
}

impl UploadTroubleCounters {
    fn from_metrics(metrics: &crate::upload::UploadMetrics) -> Self {
        Self {
            uploads_ok: metrics.uploads_ok.load(Ordering::Relaxed),
            uploads_err: metrics.uploads_err.load(Ordering::Relaxed),
            retries: metrics.retries.load(Ordering::Relaxed),
            exhausted: metrics.exhausted.load(Ordering::Relaxed),
            timeouts: metrics.timeout_err.load(Ordering::Relaxed),
            temporary_errors: metrics.temporary_err.load(Ordering::Relaxed),
            status_5xx_errors: metrics.status_5xx_err.load(Ordering::Relaxed),
            network_errors: metrics.network_err.load(Ordering::Relaxed),
            bytes_uploaded: metrics.bytes_uploaded.load(Ordering::Relaxed),
        }
    }

    fn delta_since(self, previous: Self) -> Self {
        Self {
            uploads_ok: self.uploads_ok.saturating_sub(previous.uploads_ok),
            uploads_err: self.uploads_err.saturating_sub(previous.uploads_err),
            retries: self.retries.saturating_sub(previous.retries),
            exhausted: self.exhausted.saturating_sub(previous.exhausted),
            timeouts: self.timeouts.saturating_sub(previous.timeouts),
            temporary_errors: self
                .temporary_errors
                .saturating_sub(previous.temporary_errors),
            status_5xx_errors: self
                .status_5xx_errors
                .saturating_sub(previous.status_5xx_errors),
            network_errors: self.network_errors.saturating_sub(previous.network_errors),
            bytes_uploaded: self.bytes_uploaded.saturating_sub(previous.bytes_uploaded),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UploadTroubleSignal {
    None,
    Warn,
    Recovered,
}

#[derive(Debug, Default)]
pub(super) struct UploadTroubleState {
    previous: UploadTroubleCounters,
    active: bool,
}

impl UploadTroubleState {
    pub(super) fn observe(
        &mut self,
        current: UploadTroubleCounters,
        pending_uploads: usize,
        active_uploads: usize,
        pending_retry_count: u64,
    ) -> UploadTroubleSignal {
        let delta = current.delta_since(self.previous);
        self.previous = current;

        if upload_trouble_delta_indicates_remote_problem(delta) {
            self.active = true;
        }

        let has_backlog = pending_uploads != 0 || active_uploads != 0 || pending_retry_count != 0;
        if self.active && has_backlog {
            UploadTroubleSignal::Warn
        } else if self.active {
            self.active = false;
            UploadTroubleSignal::Recovered
        } else {
            UploadTroubleSignal::None
        }
    }

    #[cfg(test)]
    pub(super) fn is_active(&self) -> bool {
        self.active
    }
}

fn upload_trouble_delta_indicates_remote_problem(delta: UploadTroubleCounters) -> bool {
    delta.timeouts != 0
        || delta.network_errors != 0
        || delta.status_5xx_errors != 0
        || delta.exhausted != 0
        || (delta.uploads_err != 0 && delta.uploads_ok == 0)
}

fn epoch_finalization_retry_delay(
    status: &commit::EpochFinalizeStatus,
    upload_requeue_base: Duration,
) -> Option<Duration> {
    match status {
        commit::EpochFinalizeStatus::WaitingForUploads | commit::EpochFinalizeStatus::Deferred => {
            Some(upload_requeue_base.max(Duration::from_millis(25)))
        }
        commit::EpochFinalizeStatus::Published | commit::EpochFinalizeStatus::Skipped => None,
    }
}

fn should_run_invalidation_poll_sync(
    wake_reason: MetadataInvalidationWakeReason,
    last_namespace_token: &Option<String>,
    last_metadata_token: &Option<String>,
    current_namespace_token: &Option<String>,
    current_metadata_token: &Option<String>,
) -> bool {
    match wake_reason {
        MetadataInvalidationWakeReason::BackendNotify => {
            last_namespace_token == current_namespace_token
                && last_metadata_token == current_metadata_token
        }
        MetadataInvalidationWakeReason::FallbackPoll => true,
        MetadataInvalidationWakeReason::DeferredRetry => false,
    }
}

fn metadata_attr_cache_invalidation_set(
    previous_snapshot: &HashSet<Ino>,
    current_snapshot: &HashSet<Ino>,
    namespace_changed: bool,
    runtime_inodes: HashSet<Ino>,
) -> HashSet<Ino> {
    let mut attr_cache_inodes = previous_snapshot.clone();
    attr_cache_inodes.extend(current_snapshot.iter().copied());
    if namespace_changed {
        attr_cache_inodes.extend(runtime_inodes);
    }
    attr_cache_inodes
}

fn metadata_token_changes(
    last_namespace_token: &Option<String>,
    last_metadata_token: &Option<String>,
    after_namespace_token: &Option<String>,
    after_metadata_token: &Option<String>,
) -> (bool, bool) {
    (
        last_namespace_token != after_namespace_token,
        last_metadata_token != after_metadata_token,
    )
}

fn semantic_coverage_complete(before: Option<bool>, after: Option<bool>) -> bool {
    after.unwrap_or_else(|| before.unwrap_or(true))
}

fn invalidation_coverage_complete(
    before: Option<bool>,
    after: Option<bool>,
    namespace_changed: bool,
    metadata_changed: bool,
) -> bool {
    semantic_coverage_complete(before, after) && !namespace_changed && !metadata_changed
}

fn metadata_namespace_changed(
    token_namespace_changed: bool,
    previous_snapshot: Option<&HashSet<Ino>>,
    current_snapshot: &HashSet<Ino>,
) -> bool {
    token_namespace_changed || previous_snapshot.is_none_or(|previous| previous != current_snapshot)
}

fn metadata_namespace_entry_changes(
    previous: Option<&HashSet<(Ino, Vec<u8>, Ino)>>,
    current: &HashSet<(Ino, Vec<u8>, Ino)>,
) -> HashSet<(Ino, Vec<u8>)> {
    let Some(previous) = previous else {
        return HashSet::new();
    };
    previous
        .symmetric_difference(current)
        .map(|(parent, name, _ino)| (*parent, name.clone()))
        .collect()
}

fn metadata_invalidation_fallback_timer(interval: Duration) -> tokio::time::Interval {
    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    timer
}

impl StudioVfs {
    pub(super) async fn background_aggressive_materialization(self: Arc<Self>) {
        let mut interval = tokio::time::interval(AGGRESSIVE_MATERIALIZATION_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if !self.aggressive_materialization_should_run() {
                continue;
            }

            let dirty_inodes = self.state.writer_stack.snapshot_dirty_inodes_by_bytes();
            for ino in dirty_inodes {
                if self.shutdown_requested() {
                    break;
                }
                self.spawn_deferred_materialization_if_needed(ino);
            }
        }
    }

    pub(super) async fn background_local_dirty_flush(self: Arc<Self>) {
        // ponytail: keep flush concurrency bounded at 32; raise only after measuring redb and payload I/O pressure.
        const MAX_LOCAL_DIRTY_FLUSHES_IN_FLIGHT: usize = 32;
        let mut interval = tokio::time::interval(LOCAL_DIRTY_FLUSH_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }
            if self.shutdown_requested() {
                break;
            }
            let dirty_inodes = self.snapshot_pending_local_dirty_inodes();
            if dirty_inodes.is_empty() {
                continue;
            }
            for inode_batch in dirty_inodes.chunks(MAX_LOCAL_DIRTY_FLUSHES_IN_FLIGHT) {
                let results =
                    futures::future::join_all(inode_batch.iter().copied().filter_map(|ino| {
                        if self.shutdown_requested()
                            || !self.should_background_flush_pending_local_dirty_inode(ino)
                        {
                            return None;
                        }
                        let vfs = Arc::clone(&self);
                        Some(async move {
                            let session = vfs
                                .state
                                .file_session
                                .enter_op(ino, FileSessionOperation::Materialize)
                                .await;
                            let flush_result = async {
                                if vfs.shutdown_requested()
                                    || !vfs.should_background_flush_pending_local_dirty_inode(ino)
                                {
                                    return Ok(());
                                }
                                vfs.flush_local_dirty_state_background(
                                    ino,
                                    LocalSyncPolicy::Opportunistic,
                                )
                                .await
                            }
                            .await;
                            drop(session);
                            vfs.state.file_session.gc_if_idle(ino);
                            (ino, flush_result)
                        })
                    }))
                    .await;
                for (ino, flush_result) in results {
                    if let Err(error) = flush_result {
                        warn!(
                            ino,
                            error = %error,
                            "background local-dirty flush failed; will retry"
                        );
                    }
                }
                if self.shutdown_requested() {
                    break;
                }
            }
        }
    }

    pub(super) async fn background_epoch_finalizer(self: Arc<Self>) {
        const MAX_FINALIZATIONS_IN_FLIGHT: usize = 32;
        let publisher = Arc::new(commit::EpochPublicationBatcher::default());
        tracing::debug!("epoch finalizer worker started");

        'finalizer: loop {
            let Some((ino, content_epoch, activity)) =
                self.pop_epoch_finalization_for_sync_safety()
            else {
                tokio::select! {
                    _ = self.state.shutdown.shutdown_notify.notified() => break,
                    _ = self.state.upload.pending_epoch_finalizations_notify.notified() => {}
                }
                continue;
            };

            let mut work = vec![(ino, content_epoch, activity)];
            while work.len() < MAX_FINALIZATIONS_IN_FLIGHT {
                if self.shutdown_requested() {
                    break;
                }
                let Some(next) = self.pop_epoch_finalization_for_sync_safety() else {
                    break;
                };
                work.push(next);
            }
            let shutdown_token = self.state.shutdown.background_shutdown_token.clone();
            let results = futures::future::join_all(work.into_iter().map(
                |(ino, content_epoch, activity)| {
                    let vfs = Arc::clone(&self);
                    let publisher = Arc::clone(&publisher);
                    let shutdown_token = shutdown_token.clone();
                    async move {
                        let result = run_epoch_finalization_until_shutdown(
                            shutdown_token,
                            commit::CommitCoordinator::finalize_inode_epoch_with_publisher(
                                &vfs,
                                ino,
                                content_epoch,
                                publisher,
                            ),
                        )
                        .await;
                        (ino, content_epoch, result, activity)
                    }
                },
            ))
            .await;

            for (ino, content_epoch, finalize_result, activity) in results {
                let Some(finalize_result) = finalize_result else {
                    break 'finalizer;
                };
                tracing::debug!(
                    ino,
                    content_epoch,
                    status = match &finalize_result {
                        Ok(commit::EpochFinalizeStatus::Published) => "published",
                        Ok(commit::EpochFinalizeStatus::WaitingForUploads) => "waiting_for_uploads",
                        Ok(commit::EpochFinalizeStatus::Deferred) => "deferred",
                        Ok(commit::EpochFinalizeStatus::Skipped) => "skipped",
                        Err(_) => "error",
                    },
                    "epoch finalization result"
                );
                match finalize_result {
                    Ok(commit::EpochFinalizeStatus::Published) => {
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "published",
                        );
                    }
                    Ok(status @ commit::EpochFinalizeStatus::WaitingForUploads) => {
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "waiting_uploads",
                        );
                        // Upload completion normally requeues the epoch, but a concurrent
                        // finalizer/upload transition can otherwise lose that notification.
                        if let Some(retry_delay) =
                            epoch_finalization_retry_delay(&status, self.upload_requeue_base)
                        {
                            self.schedule_epoch_finalization_retry(
                                ino,
                                content_epoch,
                                retry_delay,
                                activity,
                            );
                        }
                    }
                    Ok(commit::EpochFinalizeStatus::Skipped) => {
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "skipped",
                        );
                    }
                    Ok(status @ commit::EpochFinalizeStatus::Deferred) => {
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "deferred",
                        );
                        if let Some(retry_delay) =
                            epoch_finalization_retry_delay(&status, self.upload_requeue_base)
                        {
                            self.schedule_epoch_finalization_retry(
                                ino,
                                content_epoch,
                                retry_delay,
                                activity,
                            );
                        }
                    }
                    Err(err) => {
                        self.state
                            .metrics
                            .zeroio_publish_failures
                            .fetch_add(1, Ordering::Relaxed);
                        self.record_runtime_event(
                            "zeroio",
                            "publish_failed",
                            "zeroio publish failed",
                            serde_json::json!({
                                "ino": ino,
                                "content_epoch": content_epoch,
                                "path": self.best_effort_path_for_inode(ino),
                                "error": err.to_string(),
                            }),
                        );
                        if Self::is_invalid_regular_file_graph_error(&err) {
                            let attr_size = run_meta_maintenance(
                                self.meta.clone(),
                                move |engine: Arc<dyn VfsMeta>| engine.get_attr(ino),
                            )
                            .await
                            .ok()
                            .map(|attr| attr.size)
                            .unwrap_or(0);
                            warn!(
                                ino,
                                content_epoch,
                                error = %err,
                                "epoch finalization failed due to invalid regular-file graph, quarantining inode"
                            );
                            self.quarantine_invalid_regular_file_graph(ino, attr_size, &err);
                            self.state.file_session.note_background_transition(
                                ino,
                                FileSessionOperation::Finalize,
                                "failed",
                            );
                            continue;
                        }
                        if self.shutdown_requested() {
                            break 'finalizer;
                        }
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "failed",
                        );
                        warn!(
                            ino,
                            content_epoch,
                            error = %err,
                            "epoch finalization failed, will retry"
                        );
                        self.schedule_epoch_finalization_retry(
                            ino,
                            content_epoch,
                            self.upload_requeue_base,
                            activity,
                        );
                    }
                }
            }
        }
    }

    fn schedule_epoch_finalization_retry(
        self: &Arc<Self>,
        ino: Ino,
        content_epoch: u64,
        delay: Duration,
        activity: SyncSafetyActivityGuard,
    ) {
        if self.shutdown_requested() {
            return;
        }
        let vfs = self.clone();
        let shutdown = self.state.shutdown.background_shutdown_token.clone();
        self.spawn_background_task(async move {
            let _activity = activity;
            if vfs.shutdown_requested() {
                return;
            }
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = tokio::time::sleep(delay) => {
                    if !vfs.shutdown_requested() {
                        vfs.queue_epoch_finalization(ino, content_epoch);
                    }
                }
            }
        });
    }

    fn kernel_invalidator(&self) -> Option<Arc<dyn KernelInvalidator>> {
        self.kernel_invalidator.read().clone()
    }

    /// Retire the local cached view before handing committed scopes to the bridge.
    /// Content remains subject to the same active-writer protection as legacy
    /// invalidation; the commit generation is not a new open-handle version.
    pub(super) fn apply_semantic_invalidation_batch(
        &self,
        mut batch: studiofs_meta::coherence::InvalidationBatch,
    ) -> (InvalidationAcceptance, bool) {
        use studiofs_meta::coherence::InvalidationIntent;

        let original_inodes = Self::semantic_batch_inodes(&batch);
        let intent_summary = tracing::enabled!(tracing::Level::DEBUG)
            .then(|| Self::semantic_intent_summary(&batch))
            .unwrap_or_default();
        let mut deferred = false;
        let mut deferred_inodes = Vec::new();
        let mut attr_inodes = HashSet::new();
        let mut full_content_inodes = HashSet::new();
        let mut namespace_parents = HashSet::new();
        let mut reset = false;
        batch.intents.retain(|intent| {
            match intent {
                InvalidationIntent::Attributes { ino } | InvalidationIntent::Content { ino } => {
                    attr_inodes.insert(*ino);
                    let full = matches!(intent, InvalidationIntent::Content { .. });
                    if full {
                        full_content_inodes.insert(*ino);
                    }
                    if self.should_skip_kernel_invalidation_for_active_open(*ino, full) {
                        deferred = true;
                        deferred_inodes.push(*ino);
                        return false;
                    }
                }
                InvalidationIntent::Entry { parent, name, .. } => {
                    self.dentry_cache_invalidate(*parent, name);
                    namespace_parents.insert(*parent);
                }
                InvalidationIntent::Directory { ino } => {
                    self.dentry_cache_invalidate_parent(*ino);
                    attr_inodes.insert(*ino);
                    namespace_parents.insert(*ino);
                }
                InvalidationIntent::Reset => {
                    reset = true;
                }
            }
            true
        });
        if reset {
            self.note_coherence_reset_started(batch.generation);
            self.bump_namespace_epoch();
            self.retire_all_attr_read_observations();
            attr_inodes.extend(self.runtime_namespace_invalidation_inodes());
        }
        if !attr_inodes.is_empty() {
            self.invalidate_attr_read_cache(attr_inodes);
        }
        for ino in full_content_inodes {
            self.retire_os_cache_inode(ino);
        }
        if !namespace_parents.is_empty() {
            self.bump_namespace_for_parents(namespace_parents);
        }
        self.bump_acl_namespace_epoch();
        if deferred {
            debug!(
                generation = batch.generation,
                ?deferred_inodes,
                "semantic invalidation deferred for active writer; generation coverage incomplete"
            );
        }
        // A global reset covers every semantic scope, including scopes whose
        // ordinary writer protection would otherwise defer native purges.
        let mut generation_complete = !deferred || reset;
        let invalidator = self.kernel_invalidator();
        let has_invalidator = invalidator.is_some();
        let acceptance = match invalidator {
            None => InvalidationAcceptance::Unsupported,
            Some(_invalidator) if batch.intents.is_empty() => InvalidationAcceptance::Failed,
            Some(invalidator) => invalidator.invalidate_batch(&batch, generation_complete),
        };
        // Adapters also protect open writers and report that deferral as Failed.
        // It clears when the writer closes; it is not a delivery failure, so it
        // must neither consume retry attempts nor escalate to a reset.
        if acceptance == InvalidationAcceptance::Failed
            && generation_complete
            && !reset
            && self.semantic_batch_has_active_writer(&original_inodes)
        {
            generation_complete = false;
        }
        if acceptance != InvalidationAcceptance::Queued || !generation_complete {
            debug!(
                generation = batch.generation,
                ?acceptance,
                generation_complete,
                intents = %intent_summary,
                "semantic invalidation not fully admitted"
            );
        }
        if has_invalidator {
            self.note_coherence_invalidation(
                batch.generation,
                acceptance,
                generation_complete,
                now_unix_millis(),
            );
        }
        (acceptance, generation_complete)
    }

    #[cfg(test)]
    pub(super) fn drain_semantic_invalidations(&self) -> Option<bool> {
        self.drain_semantic_invalidations_into(&mut SemanticReconcileScope::default())
    }

    /// Applies pending semantic batches and records the inodes they name in
    /// `scope`, so reconciliation can follow them without walking the volume.
    pub(super) fn drain_semantic_invalidations_into(
        &self,
        scope: &mut SemanticReconcileScope,
    ) -> Option<bool> {
        let mut outcome = None;
        let mut batches = self
            .coherence_pending_invalidations
            .lock()
            .drain(..)
            .map(|pending| (pending.batch, pending.attempts))
            .collect::<Vec<_>>();
        batches.extend(
            self.meta
                .run_sync_maintenance(|meta| meta.take_invalidation_batches())
                .into_iter()
                .map(|batch| (batch, 0)),
        );
        for (batch, attempts) in batches {
            scope.observe(&batch);
            let retry = batch.clone();
            let (acceptance, generation_complete) = self.apply_semantic_invalidation_batch(batch);
            outcome = Some(
                outcome.unwrap_or(true)
                    && acceptance == InvalidationAcceptance::Queued
                    && generation_complete,
            );
            if !generation_complete {
                self.retain_failed_coherence_invalidation(retry, attempts, true);
            } else if acceptance == InvalidationAcceptance::Failed {
                self.retain_failed_coherence_invalidation(retry, attempts.saturating_add(1), false);
            }
        }
        outcome
    }

    fn semantic_batch_inodes(batch: &studiofs_meta::coherence::InvalidationBatch) -> Vec<Ino> {
        use studiofs_meta::coherence::InvalidationIntent;
        batch
            .intents
            .iter()
            .filter_map(|intent| match intent {
                InvalidationIntent::Attributes { ino }
                | InvalidationIntent::Content { ino }
                | InvalidationIntent::Directory { ino } => Some(*ino),
                InvalidationIntent::Entry { ino, .. } => *ino,
                InvalidationIntent::Reset => None,
            })
            .collect()
    }

    /// Intent kinds and inode identities only, for diagnostics.
    fn semantic_intent_summary(batch: &studiofs_meta::coherence::InvalidationBatch) -> String {
        use studiofs_meta::coherence::InvalidationIntent;
        let (mut attrs, mut content, mut dirs, mut entries, mut reset) =
            (Vec::new(), Vec::new(), Vec::new(), 0usize, false);
        for intent in &batch.intents {
            match intent {
                InvalidationIntent::Attributes { ino } => attrs.push(*ino),
                InvalidationIntent::Content { ino } => content.push(*ino),
                InvalidationIntent::Directory { ino } => dirs.push(*ino),
                InvalidationIntent::Entry { .. } => entries += 1,
                InvalidationIntent::Reset => reset = true,
            }
        }
        format!(
            "attributes={attrs:?} content={content:?} directories={dirs:?} entries={entries} reset={reset}"
        )
    }

    fn semantic_batch_has_active_writer(&self, inodes: &[Ino]) -> bool {
        inodes
            .iter()
            .any(|ino| self.handle_table.has_active_open_overlay(*ino))
    }

    /// True while some writer-deferred generation is retained.
    pub(super) fn has_deferred_coherence_retry(&self) -> bool {
        self.coherence_pending_invalidations
            .lock()
            .iter()
            .any(|pending| pending.deferred)
    }

    /// True while every writer-deferred generation is still protected by local
    /// writer state, so retrying now could only defer again.
    pub(super) fn deferred_coherence_retry_blocked(&self) -> bool {
        let deferred = self
            .coherence_pending_invalidations
            .lock()
            .iter()
            .filter(|pending| pending.deferred)
            .map(|pending| Self::semantic_batch_inodes(&pending.batch))
            .collect::<Vec<_>>();
        deferred
            .iter()
            .all(|inodes| self.semantic_batch_has_active_writer(inodes))
    }

    fn retain_failed_coherence_invalidation(
        &self,
        batch: studiofs_meta::coherence::InvalidationBatch,
        attempts: u8,
        deferred: bool,
    ) {
        let is_reset = matches!(
            batch.intents.as_slice(),
            [studiofs_meta::coherence::InvalidationIntent::Reset]
        );
        if is_reset && attempts >= COHERENCE_INVALIDATION_RETRY_LIMIT {
            warn!(
                generation = batch.generation,
                attempts, "semantic reset invalidation exceeded retry limit; dropping retry marker"
            );
            return;
        }
        let (batch, attempts) = if attempts >= COHERENCE_INVALIDATION_RETRY_LIMIT && !is_reset {
            warn!(
                generation = batch.generation,
                attempts, "semantic invalidation batch exceeded retry limit; falling back to reset"
            );
            (
                studiofs_meta::coherence::InvalidationBatch {
                    generation: batch.generation,
                    intents: vec![studiofs_meta::coherence::InvalidationIntent::Reset],
                },
                0,
            )
        } else {
            (batch, attempts)
        };
        let mut pending = self.coherence_pending_invalidations.lock();
        if pending.len() >= COHERENCE_PENDING_INVALIDATION_CAPACITY {
            // Losing precision is itself a coverage gap. Preserve an explicit
            // reset marker so adapters that support it can re-establish a
            // boundary; unsupported adapters remain conservatively degraded.
            pending.clear();
            pending.push_back(PendingCoherenceInvalidation {
                batch: studiofs_meta::coherence::InvalidationBatch {
                    generation: batch.generation,
                    intents: vec![studiofs_meta::coherence::InvalidationIntent::Reset],
                },
                attempts: 0,
                deferred: false,
            });
        } else {
            pending.push_back(PendingCoherenceInvalidation {
                batch,
                attempts,
                deferred,
            });
        }
    }

    fn retire_untrusted_coherence_cache(&self) {
        let mut known_inodes = self.runtime_namespace_invalidation_inodes();
        // The root directory is the conservative anchor for OS directory/name
        // cache state, which is not represented in `OsCacheState` generations.
        known_inodes.insert(ROOT_INO);
        self.bump_namespace_epoch();
        self.bump_acl_namespace_epoch();
        self.retire_all_attr_read_observations();
        self.invalidate_attr_read_cache(known_inodes.clone());
        self.invalidate_kernel_inodes(known_inodes);
    }

    fn note_coherence_invalidation(
        &self,
        generation: u64,
        acceptance: InvalidationAcceptance,
        generation_complete: bool,
        now_unix_ms: u64,
    ) {
        self.apply_coherence_transition(|health| {
            health.note_invalidation(generation, acceptance, generation_complete, now_unix_ms);
        });
    }

    fn note_coherence_reset_started(&self, generation: u64) {
        self.apply_coherence_transition(|health| health.begin_global_reset(generation));
    }

    pub(super) fn note_coherence_coverage_confirmed(
        &self,
        covered_boundary: Option<u64>,
        now_unix_ms: u64,
    ) {
        self.apply_coherence_transition(|health| {
            health.note_coverage_confirmed(covered_boundary, now_unix_ms);
        });
    }

    fn apply_coherence_transition(&self, transition: impl FnOnce(&mut CoherenceHealthTracker)) {
        // Sample the adapter before taking the health lock: adapters may need
        // their own queue lock, and every promotion path must observe recovery.
        let recovery_pending = self
            .kernel_invalidator()
            .is_some_and(|invalidator| invalidator.invalidation_recovery_pending());
        let (became_unsafe, entered_degraded, entered_stopped) = {
            let mut health = self.coherence_health.lock();
            let was_safe = health.allows_cache();
            let previous_state = health.snapshot().state;
            health.note_adapter_recovery_pending(recovery_pending);
            transition(&mut health);
            let current_state = health.snapshot().state;
            (
                was_safe && !health.allows_cache(),
                previous_state != crate::coherence::CoherenceHealthState::Degraded
                    && current_state == crate::coherence::CoherenceHealthState::Degraded,
                previous_state != crate::coherence::CoherenceHealthState::Stopped
                    && current_state == crate::coherence::CoherenceHealthState::Stopped,
            )
        };
        if became_unsafe || entered_degraded || entered_stopped {
            self.retire_untrusted_coherence_cache();
        }
    }

    pub(super) fn note_coherence_sync_started(&self) {
        self.apply_coherence_transition(|health| health.note_sync_started());
    }

    pub(super) fn note_coherence_sync_success(
        &self,
        progress: MetadataCoherenceProgress,
        coverage_complete: bool,
        now_unix_ms: u64,
    ) {
        self.apply_coherence_transition(|health| {
            if !progress.source_connected {
                health.note_sync_failure(crate::coherence::CoherenceHealthReason::Disconnected);
            } else if progress.remote_authoritative && progress.remote_head_version.is_none() {
                health.note_sync_failure(crate::coherence::CoherenceHealthReason::MissingProgress);
            } else {
                health.note_sync_success(
                    progress.applied_version,
                    progress.remote_head_version,
                    coverage_complete,
                    now_unix_ms,
                );
            }
            if coverage_complete {
                health.note_coverage_confirmed(progress.applied_version, now_unix_ms);
            }
        });
    }

    pub(super) fn note_coherence_sync_failure(&self) {
        self.apply_coherence_transition(|health| {
            health.note_sync_failure(crate::coherence::CoherenceHealthReason::Disconnected);
        });
    }

    pub(super) fn note_coherence_apply_failure(&self) {
        self.apply_coherence_transition(|health| {
            health.note_sync_failure(crate::coherence::CoherenceHealthReason::ApplyFailed);
        });
    }

    pub(super) fn note_coherence_delivery_state(&self, now_unix_ms: u64) {
        let (age, failures) = self
            .kernel_invalidator()
            .map(|invalidator| {
                (
                    invalidator.invalidation_delivery_age_ms(),
                    invalidator.invalidation_delivery_failure_count(),
                )
            })
            .unwrap_or((None, 0));
        self.apply_coherence_transition(|health| {
            health.note_delivery_failure_count(failures);
            health.note_delivery_age(age, now_unix_ms);
        });
    }

    pub(super) fn stop_coherence(&self) {
        self.apply_coherence_transition(|health| health.stop());
    }

    fn collect_metadata_snapshot<M: ?Sized + JanitorMeta>(
        engine: &M,
        cancel: &CancellationToken,
    ) -> MetaResult<MetadataTraversalSnapshot> {
        let mut seen = HashSet::from([ROOT_INO]);
        let mut stack = vec![ROOT_INO];
        let mut payloads = HashMap::new();
        let mut namespace_entries = HashSet::new();

        while let Some(ino) = stack.pop() {
            // The walk is O(volume) on a blocking thread that abort cannot stop; an
            // uncancelled walk outlives shutdown and leaves redb unclean.
            if cancel.is_cancelled() {
                return Err(MetaError::WorkerShutdownCancelled);
            }
            let attr = match engine.get_attr(ino) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            match attr.kind {
                FileType::Directory => {
                    let entries = match engine.readdir(ino) {
                        Ok(entries) => entries,
                        Err(MetaError::InodeNotFound(_)) => continue,
                        Err(err) => return Err(err),
                    };
                    for entry in entries {
                        namespace_entries.insert((ino, entry.name.clone(), entry.ino));
                        if seen.insert(entry.ino) {
                            stack.push(entry.ino);
                        }
                    }
                }
                FileType::RegularFile => {
                    let mut object_ids = engine
                        .get_regular_file_graph_object_usage(ino)?
                        .into_iter()
                        .map(|(object_id, _)| object_id)
                        .collect::<Vec<_>>();
                    object_ids.sort_unstable();
                    object_ids.dedup();
                    if !object_ids.is_empty() {
                        payloads.insert(ino, object_ids);
                    }
                }
                FileType::Symlink => {}
            }
        }

        Ok(MetadataTraversalSnapshot {
            inodes: seen,
            payloads,
            entries: namespace_entries,
        })
    }

    /// Reads the payload state of `inodes` only; O(|inodes|), not O(volume).
    fn collect_scoped_metadata_payloads<M: ?Sized + JanitorMeta>(
        engine: &M,
        inodes: &[Ino],
        cancel: &CancellationToken,
    ) -> MetaResult<ScopedMetadataPayloads> {
        let mut present = HashSet::new();
        let mut payloads = HashMap::new();
        for &ino in inodes {
            if cancel.is_cancelled() {
                return Err(MetaError::WorkerShutdownCancelled);
            }
            let attr = match engine.get_attr(ino) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            present.insert(ino);
            if attr.kind != FileType::RegularFile {
                continue;
            }
            let mut object_ids = engine
                .get_regular_file_graph_object_usage(ino)?
                .into_iter()
                .map(|(object_id, _)| object_id)
                .collect::<Vec<_>>();
            object_ids.sort_unstable();
            object_ids.dedup();
            if !object_ids.is_empty() {
                payloads.insert(ino, object_ids);
            }
        }
        Ok(ScopedMetadataPayloads { present, payloads })
    }

    pub(super) async fn lazy_evict_removed_metadata_payloads(
        &self,
        removed_payloads: HashMap<Ino, Vec<u64>>,
        current_inodes: &HashSet<Ino>,
        retained_inodes: &HashSet<Ino>,
    ) {
        for (ino, object_ids) in removed_payloads {
            if object_ids.is_empty() {
                continue;
            }
            let inode_deleted = !current_inodes.contains(&ino) && !retained_inodes.contains(&ino);
            if !inode_deleted {
                self.evict_payload_objects_from_caches(ino, &object_ids)
                    .await;
                continue;
            }
            // The upload tombstone is permanent, so a traversal gap (for example a
            // namespace merge detaching a still-live inode) must not set it.
            let attr = run_meta_maintenance(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                engine.get_attr(ino)
            })
            .await;
            if !matches!(attr, Err(VfsError::Meta(MetaError::InodeNotFound(_)))) {
                continue;
            }
            self.mark_inode_deleted_for_uploads(ino);
            if self.handle_table.open_count(ino) > 0 {
                self.handle_table.mark_orphan(ino);
                for object_id in object_ids {
                    self.record_deferred_deleted_local_payload(ino, object_id);
                }
                continue;
            }
            self.evict_inode_from_caches(ino, &object_ids).await;
            self.maybe_clear_deleted_inode_marker(ino);
        }
    }

    pub(super) fn removed_metadata_payloads(
        previous: &HashMap<Ino, Vec<u64>>,
        current: &HashMap<Ino, Vec<u64>>,
    ) -> HashMap<Ino, Vec<u64>> {
        debug_assert!(
            previous
                .values()
                .all(|ids| ids.windows(2).all(|pair| pair[0] < pair[1])),
            "previous metadata payload snapshot must be sorted and deduplicated"
        );
        debug_assert!(
            current
                .values()
                .all(|ids| ids.windows(2).all(|pair| pair[0] < pair[1])),
            "current metadata payload snapshot must be sorted and deduplicated"
        );

        previous
            .iter()
            .filter_map(|(ino, previous_object_ids)| {
                let current_object_ids = current.get(ino).map(Vec::as_slice).unwrap_or(&[]);
                let mut removed = Vec::new();
                let mut previous_index = 0;
                let mut current_index = 0;
                while previous_index < previous_object_ids.len() {
                    if current_index >= current_object_ids.len() {
                        removed.extend_from_slice(&previous_object_ids[previous_index..]);
                        break;
                    }
                    let previous_object_id = previous_object_ids[previous_index];
                    let current_object_id = current_object_ids[current_index];
                    if previous_object_id == current_object_id {
                        previous_index += 1;
                        current_index += 1;
                    } else if previous_object_id < current_object_id {
                        removed.push(previous_object_id);
                        previous_index += 1;
                    } else {
                        current_index += 1;
                    }
                }
                if removed.is_empty() {
                    None
                } else {
                    Some((*ino, removed))
                }
            })
            .collect()
    }

    pub(super) fn changed_metadata_payload_inodes(
        previous: &HashMap<Ino, Vec<u64>>,
        current: &HashMap<Ino, Vec<u64>>,
    ) -> HashSet<Ino> {
        let mut changed = HashSet::new();
        for (ino, previous_object_ids) in previous {
            if current.get(ino) != Some(previous_object_ids) {
                changed.insert(*ino);
            }
        }
        for ino in current.keys() {
            if !previous.contains_key(ino) {
                changed.insert(*ino);
            }
        }
        changed
    }

    pub(super) fn invalidate_kernel_inodes(&self, inodes: impl IntoIterator<Item = Ino>) {
        let mut unique = HashSet::new();
        for ino in inodes {
            if !unique.insert(ino) {
                continue;
            }
            // Generation retirement is a VFS correctness boundary, not a
            // best-effort side effect of having a mounted platform notifier.
            self.retire_os_cache_inode(ino);
            if let Some(invalidator) = self.kernel_invalidator()
                && !self.should_skip_kernel_invalidation_for_active_open(ino, true)
            {
                invalidator.inval_inode(ino);
            }
        }
    }

    pub(super) fn invalidate_kernel_entries(
        &self,
        entries: impl IntoIterator<Item = (Ino, Vec<u8>)>,
    ) {
        let Some(invalidator) = self.kernel_invalidator() else {
            return;
        };
        for (parent, name) in entries {
            invalidator.inval_entry(parent, &name);
        }
    }

    pub(super) fn invalidate_kernel_inode_attrs(&self, inodes: impl IntoIterator<Item = Ino>) {
        let Some(invalidator) = self.kernel_invalidator() else {
            return;
        };
        let mut unique = HashSet::new();
        for ino in inodes {
            if unique.insert(ino)
                && !self.should_skip_kernel_invalidation_for_active_open(ino, false)
            {
                invalidator.inval_inode_attrs(ino);
            }
        }
    }

    fn should_skip_kernel_invalidation_for_active_open(&self, ino: Ino, full: bool) -> bool {
        if !self.handle_table.has_active_open_overlay(ino) {
            return false;
        }
        let pending = if full {
            &self.deferred_kernel_invalidations
        } else {
            &self.deferred_kernel_inode_attrs
        };
        pending.insert(ino);
        if self.handle_table.has_active_open_overlay(ino) {
            true
        } else {
            pending.remove(&ino);
            false
        }
    }

    /// Protect an open write-capable handle when a native bridge can only invalidate
    /// attributes together with content. A true result requires the bridge to defer
    /// delivery and leaves the ordinary handle-release retry armed.
    ///
    /// Pending uploads and unflushed VFS state are deliberately not a reason: the
    /// VFS owns that data, the kernel holds none of it for a write-capable handle,
    /// and a purge only makes the kernel ask the VFS again.
    pub fn defer_kernel_invalidation_for_active_writer(&self, ino: Ino) -> bool {
        self.should_skip_kernel_invalidation_for_active_open(ino, true)
    }

    pub(super) fn flush_deferred_kernel_invalidations(&self, ino: Ino, invalidate_attrs: bool) {
        if self.kernel_invalidator().is_none() {
            return;
        }
        let full = self.deferred_kernel_invalidations.remove(&ino).is_some();
        let attrs = self.deferred_kernel_inode_attrs.remove(&ino).is_some() || invalidate_attrs;
        if full {
            self.invalidate_kernel_inodes([ino]);
        } else if attrs {
            self.invalidate_kernel_inode_attrs([ino]);
        }
    }

    /// Refresh kernel attributes for an accepted write, skipping the handle-table
    /// scan when a refresh is already pending for the inode.
    ///
    /// Callers publish the dirty attribute before calling this. The pending entry is
    /// removed by `flush_deferred_kernel_invalidations` and only then is the kernel
    /// invalidated. If `contains` observes the entry, that removal, and therefore the
    /// invalidation, happens after this write's attribute update. If the removal has
    /// already run, the entry is absent and the direct invalidation below runs.
    pub(super) fn retain_kernel_inode_attr_refresh(&self, ino: Ino) {
        if self.kernel_invalidator().is_none() {
            return;
        }
        if self.deferred_kernel_inode_attrs.contains(&ino) {
            return;
        }
        self.invalidate_kernel_inode_attrs([ino]);
    }
}

/// Delivers the deferred kernel invalidation for one inode when dropped.
///
/// Release paths hold this guard across awaits such as the materialization wait and
/// the file-session gate. Those awaits can be cancelled, and an explicit flush at the
/// end of the function would then be skipped, leaving the kernel with stale attributes.
/// Dropping the guard runs the flush on every exit path, including cancellation and a
/// spawned task that is dropped before it is polled.
pub(super) struct KernelInvalidationFlush<V: Deref<Target = StudioVfs>> {
    vfs: V,
    ino: Ino,
    invalidate_attrs: bool,
}

impl<V: Deref<Target = StudioVfs>> KernelInvalidationFlush<V> {
    pub(super) fn new(vfs: V, ino: Ino, invalidate_attrs: bool) -> Self {
        Self {
            vfs,
            ino,
            invalidate_attrs,
        }
    }
}

impl<V: Deref<Target = StudioVfs>> Drop for KernelInvalidationFlush<V> {
    fn drop(&mut self) {
        self.vfs
            .flush_deferred_kernel_invalidations(self.ino, self.invalidate_attrs);
    }
}

impl StudioVfs {
    pub(super) fn invalidate_attr_read_cache(
        &self,
        inodes: impl IntoIterator<Item = Ino>,
    ) -> Vec<Ino> {
        let mut inodes = inodes.into_iter();
        let Some(first_ino) = inodes.next() else {
            return Vec::new();
        };
        self.bump_attribute_generation();
        let mut cache = self.state.namespace.attr_read_cache.lock();
        let mut read_plan_cache = self.state.read.published_read_plan_cache.lock();
        let mut removed = Vec::new();
        for ino in std::iter::once(first_ino).chain(inodes) {
            self.handle_table.clear_read_windows_for_inode(ino);
            read_plan_cache.pop(&ino);
            if cache.remove(ino) {
                removed.push(ino);
            }
        }
        removed
    }

    pub(super) fn invalidate_attr_read_cache_if_observed(
        &self,
        ino: Ino,
        expected: super::AttrReadObservation,
    ) -> Option<super::AttrReadObservation> {
        let mut cache = self.state.namespace.attr_read_cache.lock();
        if cache.observation(ino) != expected {
            return None;
        }
        self.bump_attribute_generation();
        self.handle_table.clear_read_windows_for_inode(ino);
        self.state.read.published_read_plan_cache.lock().pop(&ino);
        cache.remove(ino);
        Some(cache.observation(ino))
    }

    pub(super) fn retire_all_attr_read_observations(&self) {
        self.bump_attribute_generation();
        self.state
            .namespace
            .attr_read_cache
            .lock()
            .retire_observations();
    }

    pub(super) fn bump_attribute_generation(&self) {
        self.state
            .namespace
            .attribute_generation
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .ok();
    }

    pub(super) fn read_cache_invalidation_set(
        attr_cache_inodes: &HashSet<Ino>,
        changed_payload_inodes: &HashSet<Ino>,
    ) -> HashSet<Ino> {
        attr_cache_inodes
            .iter()
            .copied()
            .chain(changed_payload_inodes.iter().copied())
            .collect()
    }

    pub(super) fn runtime_namespace_invalidation_inodes(&self) -> HashSet<Ino> {
        let mut inodes = self
            .state
            .namespace
            .dirty_attrs
            .lock()
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        inodes.extend(self.state.namespace.attr_read_cache.lock().keys());
        inodes.extend(
            self.pending_uploads_snapshot()
                .into_iter()
                .map(|upload| upload.ino),
        );
        inodes.extend(self.os_cache.lock().generation_inodes());
        inodes
    }

    async fn wait_for_metadata_invalidation_notify(notifier: Option<&Arc<tokio::sync::Notify>>) {
        if let Some(notifier) = notifier {
            notifier.notified().await;
        } else {
            std::future::pending::<()>().await;
        }
    }

    async fn wait_for_deferred_coherence_retry(&self) {
        if self.has_deferred_coherence_retry() {
            tokio::time::sleep(COHERENCE_DEFERRED_RETRY_INTERVAL).await;
        } else {
            std::future::pending::<()>().await;
        }
    }

    /// Health bookkeeping for one invalidation pass after its metadata sync.
    ///
    /// Returns `None` when the pass found nothing to reconcile. Otherwise returns
    /// whether semantic coverage is complete and the applied boundary, for the
    /// reconciliation that follows.
    pub(super) fn note_metadata_invalidation_pass(
        &self,
        before_sync_coverage: Option<bool>,
        after_sync_coverage: Option<bool>,
        token_namespace_changed: bool,
        metadata_changed: bool,
        progress: MetadataCoherenceProgress,
    ) -> Option<(bool, Option<u64>)> {
        let semantic_coverage_ready =
            semantic_coverage_complete(before_sync_coverage, after_sync_coverage);
        let reconciliation_complete = !token_namespace_changed && !metadata_changed;
        let health_coverage_complete = invalidation_coverage_complete(
            before_sync_coverage,
            after_sync_coverage,
            token_namespace_changed,
            metadata_changed,
        );
        let applied_boundary = progress.applied_version;

        self.note_coherence_sync_success(progress, health_coverage_complete, now_unix_millis());
        self.note_coherence_delivery_state(now_unix_millis());

        if reconciliation_complete {
            return None;
        }
        Some((semantic_coverage_ready, applied_boundary))
    }

    pub(super) async fn background_metadata_invalidation(self: Arc<Self>, interval: Duration) {
        let meta = self.meta.sync_view();
        let metadata_notifier = meta.metadata_invalidation_notifier();
        // Do not mark startup metadata as already invalidated. The PG listener
        // can advance local metadata before this task reaches its first wait;
        // the first wake must still clear VFS and kernel caches for that state.
        let mut last_namespace_token = None;
        let mut last_metadata_token = None;
        let mut reconcile = MetadataReconcileState::default();
        let mut fallback_timer = metadata_invalidation_fallback_timer(interval);

        loop {
            let wake_reason = tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = Self::wait_for_metadata_invalidation_notify(metadata_notifier.as_ref()) => {
                    MetadataInvalidationWakeReason::BackendNotify
                },
                _ = fallback_timer.tick() => MetadataInvalidationWakeReason::FallbackPoll,
                _ = Self::wait_for_deferred_coherence_retry(&self) => {
                    MetadataInvalidationWakeReason::DeferredRetry
                },
            };
            if self.shutdown_requested() {
                break;
            }
            if wake_reason == MetadataInvalidationWakeReason::DeferredRetry
                && self.deferred_coherence_retry_blocked()
            {
                continue;
            }
            self.note_coherence_sync_started();
            // Local commits must not wait for a remote poll to finish before
            // their already-visible state can invalidate the mounted cache.
            let before_sync_coverage = self.drain_semantic_invalidations_into(&mut reconcile.scope);
            let meta = self.meta.clone();
            let previous_namespace_token = last_namespace_token.clone();
            let previous_metadata_token = last_metadata_token.clone();
            let sync = run_meta_maintenance(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                let before_namespace = engine.namespace_version_token();
                let before_metadata = engine.metadata_version_token();
                if should_run_invalidation_poll_sync(
                    wake_reason,
                    &previous_namespace_token,
                    &previous_metadata_token,
                    &before_namespace,
                    &before_metadata,
                ) {
                    engine.sync_metadata_for_invalidation_poll()?;
                }
                let after_namespace = engine.namespace_version_token();
                let after_metadata = engine.metadata_version_token();
                Ok((
                    before_namespace,
                    after_namespace,
                    before_metadata,
                    after_metadata,
                    engine.metadata_coherence_progress(),
                ))
            })
            .await;
            // Earlier transactions may have committed even if the remaining
            // remote sync failed or did not advance its public version token.
            let after_sync_coverage = self.drain_semantic_invalidations_into(&mut reconcile.scope);
            let Ok((
                _before_namespace,
                after_namespace_token,
                _before_metadata,
                after_metadata_token,
                progress,
            )) = sync
            else {
                self.note_coherence_sync_failure();
                continue;
            };

            let (token_namespace_changed, metadata_changed) = metadata_token_changes(
                &last_namespace_token,
                &last_metadata_token,
                &after_namespace_token,
                &after_metadata_token,
            );
            let pass = self.note_metadata_invalidation_pass(
                before_sync_coverage,
                after_sync_coverage,
                token_namespace_changed,
                metadata_changed,
                progress,
            );
            // A reset deferred by the walk rate limit runs once it is due, even
            // when no further metadata change arrives.
            if pass.is_none() && !(reconcile.scope.reset && reconcile.full_walk_due(Instant::now()))
            {
                continue;
            }
            match self
                .reconcile_metadata_pass(
                    &mut reconcile,
                    wake_reason,
                    token_namespace_changed,
                    metadata_changed,
                )
                .await
            {
                MetadataReconcileOutcome::Reconciled => {}
                MetadataReconcileOutcome::Failed => continue,
                MetadataReconcileOutcome::Shutdown => break,
            }
            last_namespace_token = after_namespace_token;
            last_metadata_token = after_metadata_token;
            if let Some((true, applied_boundary)) = pass {
                self.note_coherence_coverage_confirmed(applied_boundary, now_unix_millis());
            }
        }
    }

    /// Reconciles one pass by the drained semantic scope, or by a whole-volume
    /// walk when the baseline is missing, a reset is due, or the backstop elapsed.
    pub(super) async fn reconcile_metadata_pass(
        &self,
        state: &mut MetadataReconcileState,
        wake_reason: MetadataInvalidationWakeReason,
        token_namespace_changed: bool,
        metadata_changed: bool,
    ) -> MetadataReconcileOutcome {
        if state.full_walk_due(Instant::now()) {
            self.reconcile_metadata_full_walk(
                state,
                wake_reason,
                token_namespace_changed,
                metadata_changed,
            )
            .await
        } else {
            self.reconcile_metadata_scope(state).await
        }
    }

    /// Whole-volume reconciliation: the first pass, a deferred journal reset, or
    /// the periodic backstop. Never the per-change path.
    async fn reconcile_metadata_full_walk(
        &self,
        state: &mut MetadataReconcileState,
        wake_reason: MetadataInvalidationWakeReason,
        token_namespace_changed: bool,
        metadata_changed: bool,
    ) -> MetadataReconcileOutcome {
        self.metadata_reconcile_counters
            .full_walks
            .fetch_add(1, Ordering::Relaxed);
        let meta = self.meta.clone();
        let walk_cancel = self.state.shutdown.background_shutdown_token.clone();
        let current_snapshot = match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
            Self::collect_metadata_snapshot(engine.as_ref(), &walk_cancel)
        })
        .await
        {
            Ok(snapshot) => snapshot,
            Err(_) if self.shutdown_requested() => {
                return MetadataReconcileOutcome::Shutdown;
            }
            Err(error) => {
                warn!(
                    ?wake_reason,
                    error = %error,
                    "metadata invalidation snapshot failed; retrying on the next wake"
                );
                self.note_coherence_apply_failure();
                return MetadataReconcileOutcome::Failed;
            }
        };
        let namespace_changed = metadata_namespace_changed(
            token_namespace_changed,
            state.inodes.as_ref(),
            &current_snapshot.inodes,
        ) || state
            .entries
            .as_ref()
            .is_some_and(|previous| previous != &current_snapshot.entries);

        let changed_namespace_entries = if namespace_changed {
            metadata_namespace_entry_changes(state.entries.as_ref(), &current_snapshot.entries)
        } else {
            HashSet::new()
        };

        let changed_payload_inodes = match &state.payloads {
            Some(previous_payloads) => {
                Self::changed_metadata_payload_inodes(previous_payloads, &current_snapshot.payloads)
            }
            None => current_snapshot.payloads.keys().copied().collect(),
        };
        let removed_payloads = match &state.payloads {
            Some(previous_payloads) => {
                Self::removed_metadata_payloads(previous_payloads, &current_snapshot.payloads)
            }
            None => HashMap::new(),
        };
        let runtime_inodes = if namespace_changed {
            self.runtime_namespace_invalidation_inodes()
        } else {
            HashSet::new()
        };
        self.lazy_evict_removed_metadata_payloads(
            removed_payloads,
            &current_snapshot.inodes,
            &runtime_inodes,
        )
        .await;
        let attr_cache_inodes = match &state.inodes {
            Some(previous_inodes) => metadata_attr_cache_invalidation_set(
                previous_inodes,
                &current_snapshot.inodes,
                namespace_changed,
                runtime_inodes,
            ),
            None => {
                let mut inodes = current_snapshot.inodes.clone();
                if namespace_changed {
                    inodes.extend(runtime_inodes);
                }
                inodes
            }
        };
        let read_cache_inodes =
            Self::read_cache_invalidation_set(&attr_cache_inodes, &changed_payload_inodes);
        self.invalidate_attr_read_cache(read_cache_inodes);
        self.invalidate_kernel_inodes(
            attr_cache_inodes
                .iter()
                .copied()
                .chain(changed_payload_inodes.iter().copied()),
        );
        self.invalidate_kernel_entries(changed_namespace_entries);
        if namespace_changed {
            self.bump_namespace_epoch();
        }
        if namespace_changed || metadata_changed {
            self.bump_acl_namespace_epoch();
        }
        state.inodes = Some(current_snapshot.inodes);
        state.entries = Some(current_snapshot.entries);
        state.payloads = Some(current_snapshot.payloads);
        state.last_full_walk = Some(Instant::now());
        // The walk covered every scope drained before it started.
        state.scope = SemanticReconcileScope::default();
        MetadataReconcileOutcome::Reconciled
    }

    /// Reconciles payloads of the inodes drained semantic batches named. Cache
    /// and kernel scopes were already retired when those batches were applied.
    async fn reconcile_metadata_scope(
        &self,
        state: &mut MetadataReconcileState,
    ) -> MetadataReconcileOutcome {
        self.metadata_reconcile_counters
            .scoped_passes
            .fetch_add(1, Ordering::Relaxed);
        if state.scope.reset && !state.scope.reset_retired {
            // Legacy kernel adapters cannot apply a Reset intent. Until the
            // rate-limited walk retires changed names, retire every inode this
            // mount knows about without reading redb.
            let mut known_inodes = self.runtime_namespace_invalidation_inodes();
            known_inodes.extend(state.inodes.iter().flatten().copied());
            self.invalidate_kernel_inodes(known_inodes);
            state.scope.reset_retired = true;
        }
        let Some(payloads) = state.payloads.as_mut() else {
            return MetadataReconcileOutcome::Failed;
        };
        if state.scope.inodes.is_empty() {
            return MetadataReconcileOutcome::Reconciled;
        }
        let inodes = state.scope.inodes.iter().copied().collect::<Vec<_>>();
        let scoped_inodes = inodes.clone();
        let cancel = self.state.shutdown.background_shutdown_token.clone();
        let current =
            match run_meta_maintenance(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                Self::collect_scoped_metadata_payloads(engine.as_ref(), &scoped_inodes, &cancel)
            })
            .await
            {
                Ok(current) => current,
                Err(_) if self.shutdown_requested() => return MetadataReconcileOutcome::Shutdown,
                Err(error) => {
                    // The scope is retained for the next wake.
                    warn!(
                        inodes = inodes.len(),
                        error = %error,
                        "scoped metadata reconciliation failed; retrying on the next wake"
                    );
                    self.note_coherence_apply_failure();
                    return MetadataReconcileOutcome::Failed;
                }
            };
        state.scope.inodes.clear();
        let previous = inodes
            .iter()
            .filter_map(|ino| payloads.remove(ino).map(|object_ids| (*ino, object_ids)))
            .collect::<HashMap<_, _>>();
        let removed_payloads = Self::removed_metadata_payloads(&previous, &current.payloads);
        payloads.extend(current.payloads);
        if !removed_payloads.is_empty() {
            let runtime_inodes = self.runtime_namespace_invalidation_inodes();
            self.lazy_evict_removed_metadata_payloads(
                removed_payloads,
                &current.present,
                &runtime_inodes,
            )
            .await;
        }
        MetadataReconcileOutcome::Reconciled
    }

    pub(super) async fn background_uploader(self: Arc<Self>, rx: tokio::sync::mpsc::Receiver<()>) {
        UploadLifecycleWorker::new(self).run(rx).await;
    }

    pub(super) async fn background_upload_stats(self: Arc<Self>) {
        let mut upload_pipeline_summary_was_active = false;
        let mut ticks_since_last_log = 0u64;
        let mut zero_log_counter = 0u8;
        let mut s3_upload_problem_active = UploadTroubleState::default();
        const ZERO_LOG_TARGET: u8 = 3;

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.upload_stats_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if let Err(error) = self.payload_store_run_pressure_maintenance_async().await {
                warn!(error = %error, "background local backend pressure maintenance failed");
            }
            let pending_summary = self.cached_store.pending_upload_summary();
            let pending = pending_summary.count;
            let pending_upload_bytes = self.pending_upload_bytes(&pending_summary);
            let metadata_backlog = self
                .meta
                .run_sync_maintenance(|meta| meta.replication_backlog());
            let pending_metadata_ops = metadata_backlog.total_ops();
            let active_uploads = self.upload_scheduler.in_flight();
            let active_upload_workers = self
                .state
                .upload
                .active_upload_workers
                .load(Ordering::Relaxed);
            let upload_queue = self.cached_store.cache_pipeline_metrics().upload_queue;
            let is_active = Self::upload_pipeline_summary_is_active(
                pending,
                active_uploads,
                active_upload_workers,
                pending_metadata_ops,
            );
            let activity_started = is_active && !upload_pipeline_summary_was_active;

            let should_log = if activity_started {
                ticks_since_last_log = 0;
                zero_log_counter = 0;
                true
            } else if is_active {
                ticks_since_last_log += 1;
                ticks_since_last_log >= 10
            } else if upload_pipeline_summary_was_active {
                ticks_since_last_log = 0;
                zero_log_counter = 1;
                true
            } else if zero_log_counter > 0 && zero_log_counter < ZERO_LOG_TARGET {
                ticks_since_last_log += 1;
                if ticks_since_last_log >= 10 {
                    zero_log_counter += 1;
                    true
                } else {
                    false
                }
            } else {
                false
            };

            let (p95_ms, max_ms) = self.cached_store.pending_upload_age_stats_ms();
            let ingress = self.ingress_stats();
            let materialize_batches = self.state.metrics.materialize_batch_stats.snapshot();
            let foreground_read_protection = self.foreground_read_protection_stats();
            self.adjust_upload_backpressure_from_metrics(
                foreground_read_protection.p90_latency_ns,
                foreground_read_protection.dominant_upload_pressure_boost,
            );
            let dentry_cache = self.dentry_cache_metrics();
            let path_shortcut_cache = self.path_shortcut_cache_metrics();
            let attr_read_cache = self.attr_read_cache_metrics();
            let local_backend_dirty_count = self.local_payload_backend.dirty_object_count();
            let transit_memory = self.transit_memory_budget.snapshot();
            let file_sessions = self.file_session_stats();

            if should_log {
                ticks_since_last_log = 0;
                let pending_retry_count = pending_summary.retrying;
                let pending_uploads_human =
                    Self::format_upload_pipeline_pending_uploads(pending_upload_bytes);
                let upload_trouble_counters =
                    UploadTroubleCounters::from_metrics(&self.upload_scheduler.metrics);
                let upload_trouble_signal = s3_upload_problem_active.observe(
                    upload_trouble_counters,
                    pending,
                    active_uploads,
                    pending_retry_count,
                );
                let became_idle = !is_active && upload_pipeline_summary_was_active;
                if let Some((event_kind, event_title)) =
                    Self::upload_pipeline_runtime_event(activity_started, became_idle, is_active)
                {
                    self.record_runtime_event(
                        "upload_pipeline",
                        event_kind,
                        event_title,
                        serde_json::json!({
                            "pending_uploads": pending,
                            "pending_upload_bytes": pending_upload_bytes,
                            "pending_uploads_human": pending_uploads_human,
                            "active_uploads": active_uploads,
                            "active_upload_workers": active_upload_workers,
                            "pending_metadata_ops": pending_metadata_ops,
                            "inflight_metadata_ops": metadata_backlog.inflight_ops,
                            "dirty_mem_blocks": ingress.dirty_mem_blocks,
                            "dirty_mem_bytes": ingress.dirty_mem_bytes,
                            "write_backpressure_events": ingress.write_backpressure_events,
                            "write_backpressure_wait_ms": ingress.write_backpressure_wait_ms,
                            "foreground_read_p90_latency_ns": foreground_read_protection.p90_latency_ns,
                        }),
                    );
                }
                info!(
                    pending_uploads = %pending_uploads_human,
                    active_uploads,
                    active_upload_workers,
                    pending_metadata_ops,
                    "upload pipeline"
                );
                match upload_trouble_signal {
                    UploadTroubleSignal::Warn => {
                        warn!(
                            pending_uploads = pending,
                            pending_uploads_human = %pending_uploads_human,
                            active_uploads,
                            timeouts = upload_trouble_counters.timeouts,
                            network_errors = upload_trouble_counters.network_errors,
                            retries = upload_trouble_counters.retries,
                            exhausted = upload_trouble_counters.exhausted,
                            oldest_pending_upload_age_ms = max_ms,
                            last_put_bytes_uploaded = upload_trouble_counters.bytes_uploaded,
                            "S3 upload pipeline appears stuck; uploads are retrying"
                        );
                    }
                    UploadTroubleSignal::Recovered => {
                        info!("S3 upload pipeline recovered");
                    }
                    UploadTroubleSignal::None => {}
                }
            }

            upload_pipeline_summary_was_active = is_active;
            debug!(
                dentry_cache_entries = dentry_cache.occupancy_entries,
                dentry_cache_bytes = dentry_cache.occupancy_bytes,
                path_shortcut_cache_entries = path_shortcut_cache.occupancy_entries,
                path_shortcut_cache_bytes = path_shortcut_cache.occupancy_bytes,
                attr_read_cache_entries = attr_read_cache.occupancy_entries,
                attr_read_cache_bytes = attr_read_cache.occupancy_bytes,
                "upload pipeline caches"
            );
            if !self.upload_pipeline_is_idle(pending, &ingress) {
                debug!(
                    requeues = self.state.upload.upload_requeues.load(Ordering::Relaxed),
                    canceled_deleted = self.state.upload.upload_canceled_deleted.load(Ordering::Relaxed),
                    canceled_stale = self.state.upload.upload_canceled_stale.load(Ordering::Relaxed),
                    foreground_reads_inflight =
                        self.state.read.foreground_reads_inflight.load(Ordering::Relaxed),
                    dirty_mem_blocks = ingress.dirty_mem_blocks,
                    dirty_mem_bytes = ingress.dirty_mem_bytes,
                    write_backpressure_wait_ms = ingress.write_backpressure_wait_ms,
                    write_backpressure_events = ingress.write_backpressure_events,
                    fast_backpressure_wait_ms = ingress.fast_backpressure_wait_ms,
                    fast_backpressure_events = ingress.fast_backpressure_events,
                    fast_backpressure_stall_max_ms = ingress.fast_backpressure_stall_max_ms,
                    materialize_batches_total = materialize_batches.batches,
                    materialize_batches_payloads = materialize_batches.payloads,
                    materialize_batches_bytes = materialize_batches.bytes,
                    materialize_batches_inflight = materialize_batches.inflight_batches,
                    materialize_batches_inflight_peak = materialize_batches.inflight_batches_peak,
                    materialize_batch_duration_histogram = ?materialize_batches.duration_histogram.bucket_counts,
                    upload_queue_depth = upload_queue.pending_depth,
                    upload_queue_depth_peak = upload_queue.pending_depth_peak,
                    upload_queue_submitted = upload_queue.submitted,
                    upload_queue_completed = upload_queue.completed,
                    upload_queue_requeued = upload_queue.requeued,
                    upload_queue_canceled = upload_queue.canceled,
                    upload_io_slots_available = self.upload_cache_io_slots.available_permits(),
                    qos_accept_available = self.qos.accept_available(),
                    qos_materialize_available = self.qos.materialize_available(),
                    qos_commit_available = self.qos.commit_available(),
                    qos_fetch_available = self.qos.fetch_available(),
                    pending_metadata_ops = metadata_backlog.pending_ops,
                    inflight_metadata_ops = metadata_backlog.inflight_ops,
                    dentry_cache_hits = dentry_cache.hits,
                    dentry_cache_hot_hits = dentry_cache.protected_hits,
                    dentry_cache_misses = dentry_cache.misses,
                    dentry_cache_evictions = dentry_cache.evictions,
                    dentry_cache_negative_hits = dentry_cache.negative_hits,
                    dentry_cache_negative_hit_ratio = dentry_cache.negative_hit_ratio(),
                    dentry_cache_hot_hit_ratio = dentry_cache.hot_hit_ratio(),
                    dentry_cache_entries = dentry_cache.occupancy_entries,
                    dentry_cache_bytes = dentry_cache.occupancy_bytes,
                    dentry_cache_hot_entries = dentry_cache.protected_occupancy_entries,
                    dentry_cache_hot_bytes = dentry_cache.protected_occupancy_bytes,
                    dentry_cache_entry_budget = dentry_cache.entry_budget,
                    dentry_cache_byte_budget = dentry_cache.byte_budget,
                    path_shortcut_cache_hits = path_shortcut_cache.hits,
                    path_shortcut_cache_hot_hits = path_shortcut_cache.protected_hits,
                    path_shortcut_cache_misses = path_shortcut_cache.misses,
                    path_shortcut_cache_evictions = path_shortcut_cache.evictions,
                    path_shortcut_cache_negative_hits = path_shortcut_cache.negative_hits,
                    path_shortcut_cache_negative_hit_ratio =
                        path_shortcut_cache.negative_hit_ratio(),
                    path_shortcut_cache_hot_hit_ratio = path_shortcut_cache.hot_hit_ratio(),
                    path_shortcut_cache_entries = path_shortcut_cache.occupancy_entries,
                    path_shortcut_cache_bytes = path_shortcut_cache.occupancy_bytes,
                    path_shortcut_cache_hot_entries =
                        path_shortcut_cache.protected_occupancy_entries,
                    path_shortcut_cache_hot_bytes = path_shortcut_cache.protected_occupancy_bytes,
                    path_shortcut_cache_entry_budget = path_shortcut_cache.entry_budget,
                    path_shortcut_cache_byte_budget = path_shortcut_cache.byte_budget,
                    attr_read_cache_hits = attr_read_cache.hits,
                    attr_read_cache_hot_hits = attr_read_cache.protected_hits,
                    attr_read_cache_misses = attr_read_cache.misses,
                    attr_read_cache_evictions = attr_read_cache.evictions,
                    attr_read_cache_hot_hit_ratio = attr_read_cache.hot_hit_ratio(),
                    attr_read_cache_entries = attr_read_cache.occupancy_entries,
                    attr_read_cache_bytes = attr_read_cache.occupancy_bytes,
                    attr_read_cache_hot_entries = attr_read_cache.protected_occupancy_entries,
                    attr_read_cache_hot_bytes = attr_read_cache.protected_occupancy_bytes,
                    attr_read_cache_entry_budget = attr_read_cache.entry_budget,
                    attr_read_cache_byte_budget = attr_read_cache.byte_budget,
                    "upload pipeline counters"
                );
                debug!(
                    file_session_active = file_sessions.active_sessions,
                    file_session_open_handles = file_sessions.open_handles,
                    file_session_queued_ops = file_sessions.queued_ops,
                    file_session_max_queue_depth = file_sessions.max_queue_depth,
                    file_session_wait_events = file_sessions.wait_events,
                    file_session_wait_ms = file_sessions.wait_ms,
                    file_session_transitions = file_sessions.transitions,
                    "file session counters"
                );
                debug!(
                    file_session_wait_reasons = ?file_sessions.wait_reasons,
                    "file session queue wait reasons"
                );
                debug!(
                    upload_fallback_deferrals =
                        foreground_read_protection.upload_fallback_deferrals,
                    upload_fallback_rereads = foreground_read_protection.upload_fallback_rereads,
                    upload_fallback_reread_bytes =
                        foreground_read_protection.upload_fallback_reread_bytes,
                    upload_fallback_reread_cache_misses =
                        foreground_read_protection.upload_fallback_reread_cache_misses,
                    upload_fallback_wait_ms =
                        foreground_read_protection.upload_fallback_wait_ns / 1_000_000,
                    upload_fallback_wait_max_ms =
                        foreground_read_protection.upload_fallback_wait_max_ns / 1_000_000,
                    upload_fallback_read_ms =
                        foreground_read_protection.upload_fallback_read_ns / 1_000_000,
                    upload_fallback_read_max_ms =
                        foreground_read_protection.upload_fallback_read_max_ns / 1_000_000,
                    foreground_local_payload_reads =
                        foreground_read_protection.foreground_local_payload_reads,
                    foreground_local_payload_read_bytes =
                        foreground_read_protection.foreground_local_payload_read_bytes,
                    foreground_local_payload_read_io_ms =
                        foreground_read_protection.foreground_local_payload_read_io_ns / 1_000_000,
                    foreground_local_payload_read_io_max_ms = foreground_read_protection
                        .foreground_local_payload_read_io_max_ns
                        / 1_000_000,
                    upload_hot_payload_reads = foreground_read_protection.upload_hot_payload_reads,
                    upload_hot_payload_read_bytes =
                        foreground_read_protection.upload_hot_payload_read_bytes,
                    upload_staged_payload_reads =
                        foreground_read_protection.upload_staged_payload_reads,
                    upload_staged_payload_read_bytes =
                        foreground_read_protection.upload_staged_payload_read_bytes,
                    "upload pipeline local reread path"
                );
                debug!(
                    transit_memory_budget_bytes = transit_memory.budget_bytes,
                    transit_memory_used_bytes = transit_memory.used_bytes,
                    transit_memory_available_bytes = transit_memory.available_bytes,
                    transit_memory_waiting_foreground = transit_memory.waiting_foreground,
                    transit_memory_waiting_background = transit_memory.waiting_background,
                    "upload pipeline transit memory"
                );
                debug!(
                    local_backend_dirty_count,
                    prefetch_skips = self
                        .fetch_scheduler
                        .metrics
                        .prefetch_skips
                        .load(Ordering::Relaxed),
                    "local backend and fetch counters"
                );
                debug!(
                    upload_io_foreground_cost_base = self.upload_cache_io_foreground_cost_base,
                    upload_io_foreground_cost_step = self.upload_cache_io_foreground_cost_step,
                    upload_io_foreground_cost_dynamic =
                        foreground_read_protection.upload_cache_io_foreground_cost,
                    foreground_read_p90_present =
                        foreground_read_protection.p90_latency_ns.is_some(),
                    foreground_read_p90_ms = foreground_read_protection
                        .p90_latency_ns
                        .map(|ns| ns.saturating_div(1_000_000))
                        .unwrap_or(0),
                    foreground_read_speedometer_samples =
                        foreground_read_protection.speedometer_samples,
                    foreground_read_window_bytes = foreground_read_protection.window_bytes,
                    foreground_read_window_coverage_ms =
                        foreground_read_protection.window_coverage_ms,
                    foreground_read_throughput_bytes_per_sec =
                        foreground_read_protection.throughput_bytes_per_sec,
                    foreground_read_target_ms = self
                        .upload_fg_read_latency_target_ns
                        .saturating_div(1_000_000),
                    upload_scheduler_penalty_ms =
                        foreground_read_protection.upload_scheduler_penalty_ms,
                    "upload pipeline adaptive throttle"
                );
            }
            self.upload_scheduler.log_stats(pending, p95_ms, max_ms);
        }
    }

    pub(super) async fn background_local_metadata_compaction(self: Arc<Self>) {
        let mut poll = tokio::time::interval(LOCAL_REDB_COMPACTION_POLL_INTERVAL);
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let shutdown_token = self.state.shutdown.background_shutdown_token.clone();
        let mut schedule =
            LocalRedbCompactionSchedule::new(self.meta.activity_snapshot().activity_generation);

        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => break,
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = poll.tick() => {}
            }
            if self.shutdown_requested() {
                break;
            }

            let activity = self.meta.activity_snapshot();
            let now = Instant::now();
            let Some(idle_for) = schedule.observe_activity(
                now,
                activity.activity_generation,
                activity.active_calls,
                self.foreground_read_active_for_compaction(),
                self.shutdown_requested(),
            ) else {
                continue;
            };
            if !schedule.ready_to_probe(now, idle_for) {
                continue;
            }

            let expected_activity = activity.activity_generation;
            let vfs = self.clone();
            let diagnostics = match self
                .meta
                .run_maintenance(move |engine: Arc<dyn VfsMeta>| {
                    let activity = vfs.meta.activity_snapshot();
                    if activity.activity_generation != expected_activity
                        || activity.active_calls != 1
                        || vfs.foreground_read_active_for_compaction()
                        || vfs.shutdown_requested()
                    {
                        return Ok((false, None));
                    }
                    Ok((true, engine.local_storage_diagnostics()?))
                })
                .await
            {
                Ok((true, Some(diagnostics))) => diagnostics,
                Ok((true, None)) => {
                    schedule.defer_probe(
                        Instant::now(),
                        local_redb_compaction_diagnostics_retry_delay(None),
                    );
                    continue;
                }
                Ok((false, _)) => {
                    schedule.reset_idle(self.meta.activity_snapshot().activity_generation);
                    continue;
                }
                Err(error) => {
                    warn!(error = %error, "local metadata compaction diagnostics failed");
                    schedule.defer_probe(Instant::now(), LOCAL_REDB_COMPACTION_ERROR_BACKOFF);
                    continue;
                }
            };

            let activity = self.meta.activity_snapshot();
            let now = Instant::now();
            let Some(idle_for) = schedule.observe_activity(
                now,
                activity.activity_generation,
                activity.active_calls,
                self.foreground_read_active_for_compaction(),
                self.shutdown_requested(),
            ) else {
                continue;
            };
            if !schedule.ready_to_probe(now, idle_for) {
                continue;
            }
            if !local_redb_compaction_worthwhile(&diagnostics) {
                schedule.defer_probe(
                    now,
                    local_redb_compaction_diagnostics_retry_delay(Some(&diagnostics)),
                );
                continue;
            }

            let expected_activity = activity.activity_generation;
            let file_bytes_before = diagnostics.physical_file_bytes;
            let vfs = self.clone();
            let attempt = self
                .meta
                .run_maintenance(move |engine: Arc<dyn VfsMeta>| {
                    let activity_before = vfs.meta.activity_snapshot();
                    if activity_before.activity_generation != expected_activity
                        || activity_before.active_calls != 1
                        || vfs.foreground_read_active_for_compaction()
                        || vfs.shutdown_requested()
                    {
                        return Ok(None);
                    }

                    let started = Instant::now();
                    let result = engine.compact_local_storage();
                    let elapsed = started.elapsed();
                    let activity_after = vfs.meta.activity_snapshot();
                    let foreground_work_arrived = activity_after.activity_generation
                        != activity_before.activity_generation
                        || vfs.foreground_read_active_for_compaction();
                    let diagnostics = engine.local_storage_diagnostics();
                    Ok(Some((
                        result,
                        elapsed,
                        foreground_work_arrived,
                        diagnostics,
                    )))
                })
                .await;

            let (result, elapsed, foreground_work_arrived, diagnostics_after) = match attempt {
                Ok(Some(attempt)) => attempt,
                Ok(None) => {
                    schedule.reset_idle(self.meta.activity_snapshot().activity_generation);
                    continue;
                }
                Err(error) => {
                    warn!(error = %error, "local metadata compaction worker failed");
                    schedule.defer_probe(Instant::now(), LOCAL_REDB_COMPACTION_ERROR_BACKOFF);
                    continue;
                }
            };

            let compaction_diagnostics = diagnostics_after
                .as_ref()
                .ok()
                .and_then(|diagnostics| diagnostics.as_ref());
            let compacted = matches!(&result, Ok(LocalStorageCompactionResult::Compacted));
            let bytes_before = if compacted {
                compaction_diagnostics.and_then(|d| d.last_success_bytes_before)
            } else {
                None
            }
            .or(file_bytes_before);
            let bytes_after = if compacted {
                compaction_diagnostics.and_then(|d| d.last_success_bytes_after)
            } else {
                None
            }
            .or_else(|| compaction_diagnostics.and_then(|d| d.physical_file_bytes));
            let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
            let idle_ms = idle_for.as_millis().min(u128::from(u64::MAX)) as u64;
            let result_label = match &result {
                Ok(LocalStorageCompactionResult::Compacted) => "compacted",
                Ok(LocalStorageCompactionResult::NoWork) => "no_work",
                Ok(LocalStorageCompactionResult::SkippedBusy) => "skipped_busy",
                Err(_) => "error",
            };

            match &result {
                Ok(LocalStorageCompactionResult::Compacted) => info!(
                    trigger_reason = "redb_size_and_fragmentation_threshold",
                    idle_ms,
                    result = result_label,
                    elapsed_ms,
                    bytes_before = ?bytes_before,
                    bytes_after = ?bytes_after,
                    foreground_metadata_work_arrived = foreground_work_arrived,
                    "local metadata redb compaction attempt"
                ),
                Ok(
                    LocalStorageCompactionResult::NoWork
                    | LocalStorageCompactionResult::SkippedBusy,
                ) => debug!(
                    trigger_reason = "redb_size_and_fragmentation_threshold",
                    idle_ms,
                    result = result_label,
                    elapsed_ms,
                    bytes_before = ?bytes_before,
                    bytes_after = ?bytes_after,
                    foreground_metadata_work_arrived = foreground_work_arrived,
                    "local metadata redb compaction attempt"
                ),
                Err(error) => warn!(
                    trigger_reason = "redb_size_and_fragmentation_threshold",
                    idle_ms,
                    result = result_label,
                    elapsed_ms,
                    bytes_before = ?bytes_before,
                    bytes_after = ?bytes_after,
                    foreground_metadata_work_arrived = foreground_work_arrived,
                    error = %error,
                    "local metadata redb compaction attempt"
                ),
            }
            schedule.record_attempt(Instant::now(), &result);
        }
    }

    pub(super) async fn background_gc(self: Arc<Self>) {
        debug!("background GC started");

        let interval = tokio::time::Duration::from_secs(600);
        let grace_period = 3600i64;
        let mut gc_cycle = 0_u64;

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            gc_cycle = gc_cycle.saturating_add(1);

            let meta = self.meta.clone();
            let lock_acquired = match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.try_acquire_global_janitor_lock()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "background GC skipped: janitor PG lock check failed");
                    continue;
                }
            };
            if !lock_acquired {
                debug!("background GC skipped: janitor PG lock not acquired");
                continue;
            }

            const CHUNK_COMPACTION_BATCH_LIMIT: usize = 32;
            let meta = self.meta.clone();
            match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.compact_fragmented_regular_file_chunks(CHUNK_COMPACTION_BATCH_LIMIT)
            })
            .await
            {
                Ok(compacted_chunks) if compacted_chunks > 0 => {
                    info!(
                        compacted_chunks,
                        "background GC compacted fragmented regular-file chunk rows"
                    );
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "background GC chunk compaction failed"),
            }

            let meta = self.meta.clone();
            if let Err(error) = run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.reconcile_pending_regular_file_recoveries()
            })
            .await
            {
                warn!(error = %error, "background GC recovery reconciliation failed");
            }

            let meta = self.meta.clone();
            let referenced_object_ids =
                match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.get_referenced_object_ids()
                })
                .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        let meta = self.meta.clone();
                        let _ = run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                            engine.release_global_janitor_lock()
                        })
                        .await;
                        warn!(
                            error = %e,
                            "background GC skipped: metadata reachability query failed"
                        );
                        continue;
                    }
                };

            let meta = self.meta.clone();
            let snapshot_pins = match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_global_snapshot_pins()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(
                        error = %e,
                        "background GC skipped: snapshot protection query failed"
                    );
                    continue;
                }
            };

            let now = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let deletion_owner = format!("background-gc-{gc_cycle}");

            const PENDING_DELETION_GC_BATCH_LIMIT: usize = 1_000;
            let meta = self.meta.clone();
            match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_pending_deletions_limited(PENDING_DELETION_GC_BATCH_LIMIT + 1)
            })
            .await
            {
                Ok(pending) => {
                    let mut deleted_count = 0;
                    let pending_total = pending.len();
                    for (object_id, timestamp) in
                        pending.into_iter().take(PENDING_DELETION_GC_BATCH_LIMIT)
                    {
                        if referenced_object_ids.contains(&object_id) {
                            debug!(object_id, "skipping object - still reachable in metadata");
                            continue;
                        }

                        if snapshot_pins.contains(&object_id) {
                            debug!(object_id, "skipping object - protected by a snapshot cut");
                            continue;
                        }

                        if now - timestamp < grace_period {
                            debug!("skipping object {} - still in grace period", object_id);
                            continue;
                        }

                        let meta = self.meta.clone();
                        let claim_owner = deletion_owner.clone();
                        let deletion_claim = match run_meta_maintenance(
                            meta,
                            move |engine: Arc<dyn VfsMeta>| {
                                engine.claim_uploaded_object_deletion(object_id, &claim_owner)
                            },
                        )
                        .await
                        {
                            Ok(claim) => claim,
                            Err(error) => {
                                warn!(object_id, error = %error, "GC: failed to claim object deletion");
                                continue;
                            }
                        };
                        let fencing_token = match deletion_claim {
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Untracked => {
                                None
                            }
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Claimed {
                                fencing_token,
                            } => Some(fencing_token),
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Protected => {
                                debug!(object_id, "skipping object - protected by upload ledger");
                                continue;
                            }
                        };

                        let delete_results = futures::future::join_all(
                            format_object_fragment_keys(&self.data_prefix, object_id)
                                .into_iter()
                                .map(|key| async {
                                    let result = self.data_write.delete_by_key(&key).await;
                                    (key, result)
                                }),
                        )
                        .await;
                        let mut delete_failed = false;
                        for (key, result) in delete_results {
                            if let Err(e) = result {
                                if matches!(&e, DataError::OpenDal(oe) if oe.kind() == ErrorKind::NotFound)
                                {
                                    continue;
                                }
                                delete_failed = true;
                                warn!(
                                    object_id,
                                    key,
                                    error = %e,
                                    "failed to delete object fragment from S3"
                                );
                            }
                        }
                        let deletion_outcome = if delete_failed {
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome::Retryable
                        } else {
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome::Deleted
                        };
                        if let Some(fencing_token) = fencing_token {
                            let meta = self.meta.clone();
                            let finish_owner = deletion_owner.clone();
                            if let Err(error) =
                                run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                                    engine.finish_uploaded_object_deletion(
                                        object_id,
                                        &finish_owner,
                                        fencing_token,
                                        deletion_outcome,
                                    )
                                })
                                .await
                            {
                                warn!(object_id, error = %error, "GC: failed to finish object deletion claim");
                                continue;
                            }
                        }
                        if !delete_failed {
                            let meta = self.meta.clone();
                            if let Err(e) =
                                run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                                    engine.remove_pending_deletion(object_id)
                                })
                                .await
                            {
                                warn!("failed to remove pending deletion for {}: {}", object_id, e);
                            }
                            deleted_count += 1;
                            debug!("GC deleted object {}", object_id);
                        }
                    }
                    if deleted_count > 0 {
                        info!("GC deleted {} objects", deleted_count);
                    }
                    if pending_total > PENDING_DELETION_GC_BATCH_LIMIT {
                        debug!(
                            pending_total,
                            batch_limit = PENDING_DELETION_GC_BATCH_LIMIT,
                            "GC limited pending deletion cleanup batch"
                        );
                    }
                }
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    error!("failed to get pending deletions: {}", e);
                }
            }

            let meta = self.meta.clone();
            if let Err(e) = run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.release_global_janitor_lock()
            })
            .await
            {
                warn!(error = %e, "background GC failed to release janitor PG lock");
            }
        }
    }

    async fn delete_janitor_object(
        &self,
        cycle: u64,
        object_id: u64,
        delete_keys: Vec<String>,
        deletion_owner: String,
        shutdown: CancellationToken,
    ) -> (JanitorObjectDeletionResult, JanitorObjectDeletionTimings) {
        if shutdown.is_cancelled() {
            return (
                JanitorObjectDeletionResult::NotStarted,
                JanitorObjectDeletionTimings::default(),
            );
        }
        let meta = self.meta.clone();
        let claim_owner = deletion_owner.clone();
        let claim_started = Instant::now();
        let deletion_claim = match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.claim_uploaded_object_deletion(object_id, &claim_owner)
        })
        .await
        {
            Ok(claim) => claim,
            Err(error) => {
                warn!(cycle, object_id, error = %error, "janitor: failed to claim object deletion");
                return (
                    JanitorObjectDeletionResult::Retryable,
                    JanitorObjectDeletionTimings {
                        claim: claim_started.elapsed(),
                        ..JanitorObjectDeletionTimings::default()
                    },
                );
            }
        };
        let mut timings = JanitorObjectDeletionTimings {
            claim: claim_started.elapsed(),
            ..JanitorObjectDeletionTimings::default()
        };
        let fencing_token = match deletion_claim {
            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Untracked => None,
            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Claimed {
                fencing_token,
            } => Some(fencing_token),
            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Protected => {
                return (JanitorObjectDeletionResult::Protected, timings);
            }
        };

        let remote_delete_started = Instant::now();
        let delete_results = futures::future::join_all(delete_keys.into_iter().map(|key| async {
            let result = run_with_timeout_or_cancel(
                &shutdown,
                REMOTE_OBJECT_IO_TIMEOUT,
                self.data_write.delete_by_key(&key),
            )
            .await;
            (key, result)
        }))
        .await;
        timings.remote_delete = remote_delete_started.elapsed();
        let mut delete_failed = false;
        for (key, result) in delete_results {
            match result {
                TimeoutOrCancel::Completed(Ok(())) => {}
                TimeoutOrCancel::Completed(Err(error)) => {
                    if matches!(&error, DataError::OpenDal(oe) if oe.kind() == ErrorKind::NotFound)
                    {
                        continue;
                    }
                    delete_failed = true;
                    warn!(cycle, object_id, key, error = %error, "janitor: fragment delete failed");
                }
                TimeoutOrCancel::TimedOut | TimeoutOrCancel::Cancelled => {
                    delete_failed = true;
                    debug!(
                        cycle,
                        object_id,
                        key,
                        "janitor: fragment delete interrupted; claim will be retried"
                    );
                }
            }
        }
        let (outcome, result) = if delete_failed {
            (
                studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome::Retryable,
                JanitorObjectDeletionResult::Retryable,
            )
        } else {
            (
                studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome::Deleted,
                JanitorObjectDeletionResult::Deleted,
            )
        };
        if let Some(fencing_token) = fencing_token {
            let meta = self.meta.clone();
            let finish_started = Instant::now();
            if let Err(error) = run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.finish_uploaded_object_deletion(
                    object_id,
                    &deletion_owner,
                    fencing_token,
                    outcome,
                )
            })
            .await
            {
                timings.finish = finish_started.elapsed();
                warn!(cycle, object_id, error = %error, "janitor: failed to finish object deletion claim");
                return (JanitorObjectDeletionResult::Retryable, timings);
            }
            timings.finish = finish_started.elapsed();
        }
        (result, timings)
    }

    fn janitor_shutdown_gate(&self) -> ControlFlow<()> {
        if self.state.shutdown.background_shutdown_token.is_cancelled() {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    pub(super) async fn background_janitor(self: Arc<Self>) {
        debug!(
            interval_secs = self.janitor_interval.as_secs(),
            grace_secs = self.janitor_skip_newer_than.as_secs(),
            max_deletes = self.janitor_max_deletes_per_run,
            "background janitor started"
        );
        let mut cycle: u64 = 0;
        let shutdown = self.state.shutdown.background_shutdown_token.clone();

        loop {
            cycle = cycle.saturating_add(1);
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.janitor_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }

            let meta = self.meta.clone();
            let lock_acquired = match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.try_acquire_global_janitor_lock()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor cycle skipped: PG lock check failed");
                    continue;
                }
            };
            if !lock_acquired {
                debug!(cycle, "janitor cycle skipped: PG lock not acquired");
                continue;
            }

            // Every started deletion has drained by the time the cycle returns,
            // so the global lock is released on every completion, error, and
            // shutdown path here.
            let flow = self.run_janitor_cycle(cycle, &shutdown).await;
            let meta = self.meta.clone();
            if let Err(e) = run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.release_global_janitor_lock()
            })
            .await
            {
                warn!(cycle, error = %e, "janitor cycle failed to release PG lock");
            }
            if flow.is_break() {
                break;
            }
        }
    }

    /// Runs one janitor cycle under the global janitor lock. `Break` means
    /// shutdown was observed; `Continue` covers completed and aborted cycles.
    async fn run_janitor_cycle(
        self: &Arc<Self>,
        cycle: u64,
        shutdown: &CancellationToken,
    ) -> ControlFlow<()> {
        let cycle_started = Instant::now();
        self.janitor_shutdown_gate()?;
        let meta = self.meta.clone();
        let reconciliation_owner = format!("janitor-{cycle}");
        let claim_owner = reconciliation_owner.clone();
        let reconciliation_claims_started = Instant::now();
        debug!(
            cycle,
            phase = "reconciliation_claims",
            "janitor phase started"
        );
        let reconciliation_claims = match run_meta_maintenance(
            meta,
            move |engine: Arc<dyn VfsMeta>| {
                engine.claim_uploaded_fragment_reconciliation(&claim_owner)
            },
        )
        .await
        {
            Ok(claims) => claims,
            Err(e) => {
                warn!(cycle, error = %e, "janitor cycle skipped: upload reconciliation claim failed");
                return ControlFlow::Continue(());
            }
        };
        let reconciliation_claim_count = reconciliation_claims.len();
        debug!(
            cycle,
            claims = reconciliation_claim_count,
            elapsed_ms = elapsed_ms(reconciliation_claims_started.elapsed()),
            "janitor reconciliation claims loaded"
        );
        let reconciliation_object_ids: Vec<u64> = reconciliation_claims
            .iter()
            .map(|claim| claim.entry.object_id)
            .collect();
        let data_store = self.fetch_scheduler.data_store();
        let reconciliation_proof_started = Instant::now();
        debug!(
            cycle,
            phase = "reconciliation_proof",
            "janitor phase started"
        );
        let mut reconciliation_outcomes = futures::stream::iter(reconciliation_claims)
            .map(|claim| prove_reconciliation_claim(data_store, shutdown, cycle, claim))
            .buffer_unordered(JANITOR_RECONCILIATION_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        debug!(
            cycle,
            claims = reconciliation_outcomes.len(),
            elapsed_ms = elapsed_ms(reconciliation_proof_started.elapsed()),
            "janitor reconciliation proofs completed"
        );
        let mut upload_reconciliation_confirmed: u64 = 0;
        let mut upload_reconciliation_retryable: u64 = 0;
        for (_, outcome) in &reconciliation_outcomes {
            match outcome {
                studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::RemoteConfirmed => {
                    upload_reconciliation_confirmed =
                        upload_reconciliation_confirmed.saturating_add(1);
                }
                studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::Retryable => {
                    upload_reconciliation_retryable =
                        upload_reconciliation_retryable.saturating_add(1);
                }
            }
        }
        order_reconciliation_finishes(&mut reconciliation_outcomes);
        let reconciliation_finish_started = Instant::now();
        debug!(
            cycle,
            phase = "reconciliation_finish",
            "janitor phase started"
        );
        let (finish_attempted, upload_reconciliation_completion_errors, unfinished_claims) =
            finish_janitor_reconciliation_claims(
                reconciliation_outcomes,
                shutdown,
                JANITOR_RECONCILIATION_FINISH_CONCURRENCY,
                |(claim, _)| claim.entry.object_id,
                |(claim, outcome)| {
                    let owner = reconciliation_owner.clone();
                    let object_id = claim.entry.object_id;
                    let meta = self.meta.clone();
                    async move {
                        run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                            engine.finish_uploaded_fragment_reconciliation(&owner, &claim, outcome)
                        })
                        .await
                        .inspect_err(|error| {
                            warn!(
                                cycle,
                                object_id,
                                error = %error,
                                "janitor: upload reconciliation completion failed"
                            );
                        })
                    }
                },
            )
            .await;
        if unfinished_claims > 0 {
            debug!(
                cycle,
                unfinished_claims, "janitor shutdown left reconciliation claims for lease expiry"
            );
        }
        debug!(
            cycle,
            finish_attempted,
            confirmed = upload_reconciliation_confirmed,
            retryable = upload_reconciliation_retryable,
            finish_errors = upload_reconciliation_completion_errors,
            unfinished_claims,
            elapsed_ms = elapsed_ms(reconciliation_finish_started.elapsed()),
            "janitor reconciliation claims finished"
        );
        self.janitor_shutdown_gate()?;

        let meta = self.meta.clone();
        if let Err(error) = run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.reconcile_pending_regular_file_recoveries()
        })
        .await
        {
            warn!(cycle, error = %error, "janitor recovery reconciliation failed");
        }
        self.janitor_shutdown_gate()?;

        let meta = self.meta.clone();
        let references_started = Instant::now();
        debug!(
            cycle,
            phase = "metadata_references",
            "janitor phase started"
        );
        let referenced = match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.get_referenced_object_ids()
        })
        .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(cycle, error = %e, "janitor cycle failed: get_referenced_object_ids");
                return ControlFlow::Continue(());
            }
        };
        debug!(
            cycle,
            referenced = referenced.len(),
            elapsed_ms = elapsed_ms(references_started.elapsed()),
            "janitor metadata references loaded"
        );
        self.janitor_shutdown_gate()?;

        // Load pins after references: a snapshot can only pin an object that
        // was referenced when it was taken, so an object absent from both
        // sets read in this order cannot be pinned by a concurrent snapshot.
        let meta = self.meta.clone();
        let mut global_snapshot_pins =
            match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_global_snapshot_pins()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor cycle skipped: snapshot pin query failed");
                    return ControlFlow::Continue(());
                }
            };
        global_snapshot_pins.extend(reconciliation_object_ids);
        self.janitor_shutdown_gate()?;
        let meta = self.meta.clone();
        let pending_map: HashMap<u64, i64> =
            match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_pending_deletions()
            })
            .await
            {
                Ok(v) => v.into_iter().collect(),
                Err(e) => {
                    warn!(cycle, error = %e, "janitor: failed to read pending deletions");
                    HashMap::new()
                }
            };
        self.janitor_shutdown_gate()?;

        let meta = self.meta.clone();
        let recovered_undo_parents =
            match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.recover_pending_creates()
            })
            .await
            {
                Ok(parents) => parents,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor: pending-create replay failed");
                    Vec::new()
                }
            };
        if !recovered_undo_parents.is_empty() {
            for parent in recovered_undo_parents.iter().copied() {
                self.dentry_cache_invalidate_parent(parent);
            }
            self.bump_namespace_for_parents(recovered_undo_parents.iter().copied());
        }
        self.janitor_shutdown_gate()?;
        let meta = self.meta.clone();
        let (undo_total, undo_started, undo_replaying, undo_completed) =
            match run_meta_maintenance(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.list_undo_logs()
            })
            .await
            {
                Ok(records) => {
                    let mut started = 0usize;
                    let mut replaying = 0usize;
                    let mut completed = 0usize;
                    for record in records.iter() {
                        match record.phase {
                            studiofs_meta::engine::UndoLogPhase::Started => {
                                started = started.saturating_add(1)
                            }
                            studiofs_meta::engine::UndoLogPhase::Replaying => {
                                replaying = replaying.saturating_add(1)
                            }
                            studiofs_meta::engine::UndoLogPhase::Completed => {
                                completed = completed.saturating_add(1)
                            }
                        }
                    }
                    (records.len(), started, replaying, completed)
                }
                Err(e) => {
                    warn!(cycle, error = %e, "janitor: failed to read undo-log metrics");
                    (0usize, 0usize, 0usize, 0usize)
                }
            };

        self.janitor_shutdown_gate()?;
        debug!(cycle, phase = "object_listing", "janitor phase started");
        let listing_started = Instant::now();
        let lister = match run_with_timeout_or_cancel(
            shutdown,
            REMOTE_OBJECT_IO_TIMEOUT,
            self.data_write.list_object_entries(&self.data_prefix),
        )
        .await
        {
            TimeoutOrCancel::Completed(Ok(lister)) => lister,
            TimeoutOrCancel::Completed(Err(e)) => {
                warn!(
                    cycle,
                    prefix = self.data_prefix,
                    error = %e,
                    "janitor cycle failed: list data prefix"
                );
                return ControlFlow::Continue(());
            }
            TimeoutOrCancel::TimedOut | TimeoutOrCancel::Cancelled => {
                debug!(
                    cycle,
                    elapsed_ms = elapsed_ms(listing_started.elapsed()),
                    "janitor object listing stopped before stream creation"
                );
                return self.janitor_shutdown_gate();
            }
        };
        debug!(
            cycle,
            elapsed_ms = elapsed_ms(listing_started.elapsed()),
            "janitor object listing stream opened"
        );
        let mut lister = std::pin::pin!(lister);

        let now = SystemTime::now();
        let now_unix = now
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        let mut scanned: u64 = 0;
        let mut orphan_candidates: u64 = 0;
        let mut skipped_young: u64 = 0;
        let mut skipped_unknown_age: u64 = 0;
        let mut skipped_non_object_key: u64 = 0;
        let mut seen_objects: HashSet<u64> = HashSet::new();
        let deletion_owner = format!("janitor-delete-{cycle}");
        let mut deletion_queue = JanitorDeletionQueue::new(
            self.janitor_max_deletes_per_run,
            JANITOR_OBJECT_DELETE_CONCURRENCY,
        );

        let mut list_error = None;
        let mut listing_timed_out = false;
        let mut shutdown_cancelled = false;
        let scan_started = Instant::now();
        // Deletion completions win the select below without resetting this:
        // the listing bound covers waiting for the next entry, not each poll.
        let mut listing_deadline = tokio::time::Instant::now() + REMOTE_OBJECT_IO_TIMEOUT;
        while !deletion_queue.reached_cap() {
            let has_in_flight = deletion_queue.has_in_flight();
            let listing = tokio::select! {
                _ = shutdown.cancelled() => {
                    shutdown_cancelled = true;
                    break;
                },
                listing = tokio::time::timeout_at(listing_deadline, lister.next()) => {
                    match listing {
                        Ok(listing) => listing,
                        Err(_) => {
                            listing_timed_out = true;
                            break;
                        }
                    }
                },
                // An empty FuturesUnordered resolves immediately, so only poll
                // completions while deletions are in flight.
                completed = deletion_queue.complete_one(), if has_in_flight => {
                    if !completed || deletion_queue.reached_cap() {
                        break;
                    }
                    continue;
                }
            };
            let Some(entry_result) = listing else {
                break;
            };
            listing_deadline = tokio::time::Instant::now() + REMOTE_OBJECT_IO_TIMEOUT;
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(err) => {
                    list_error = Some(err);
                    break;
                }
            };
            scanned = scanned.saturating_add(1);
            let path = entry.key.as_str();
            let object_id = match parse_object_id_from_key(path) {
                Ok(id) => id,
                Err(_) => {
                    skipped_non_object_key = skipped_non_object_key.saturating_add(1);
                    continue;
                }
            };
            let Some(delete_keys) = janitor_object_delete_keys(&self.data_prefix, object_id, path)
            else {
                skipped_non_object_key = skipped_non_object_key.saturating_add(1);
                continue;
            };
            if !seen_objects.insert(object_id) {
                continue;
            }
            if referenced.contains(&object_id) {
                continue;
            }
            if global_snapshot_pins.contains(&object_id) {
                continue;
            }
            orphan_candidates = orphan_candidates.saturating_add(1);
            let is_old_enough = if let Some(ts) = pending_map.get(&object_id) {
                pending_deletion_is_old_enough(now_unix, *ts, self.janitor_skip_newer_than)
            } else if let Some(last_modified_st) = entry.last_modified {
                let age = now
                    .duration_since(last_modified_st)
                    .unwrap_or(Duration::ZERO);
                age >= self.janitor_skip_newer_than
            } else {
                skipped_unknown_age = skipped_unknown_age.saturating_add(1);
                continue;
            };

            if !is_old_enough {
                skipped_young = skipped_young.saturating_add(1);
                continue;
            }

            if shutdown.is_cancelled() {
                shutdown_cancelled = true;
                break;
            }
            deletion_queue.wait_for_slot().await;
            listing_deadline = tokio::time::Instant::now() + REMOTE_OBJECT_IO_TIMEOUT;
            if shutdown.is_cancelled() {
                shutdown_cancelled = true;
                break;
            }
            if deletion_queue.reached_cap() {
                break;
            }

            let vfs = self.clone();
            let deletion_owner = deletion_owner.clone();
            let shutdown = shutdown.clone();
            deletion_queue.push(async move {
                vfs.delete_janitor_object(cycle, object_id, delete_keys, deletion_owner, shutdown)
                    .await
            });
        }

        // Finish every claimed deletion before the global lease can be released,
        // including when streaming the object listing fails.
        deletion_queue.drain().await;
        let listing_delete_elapsed_ms = elapsed_ms(scan_started.elapsed());
        let cycle_elapsed_ms = elapsed_ms(cycle_started.elapsed());
        let claim_ms = elapsed_ms(deletion_queue.timings.claim);
        let remote_delete_ms = elapsed_ms(deletion_queue.timings.remote_delete);
        let finish_ms = elapsed_ms(deletion_queue.timings.finish);
        let deleted = deletion_queue.successful_deletes;
        let delete_errors = deletion_queue.retryable_failures;

        if listing_timed_out {
            warn!(
                cycle,
                elapsed_ms = elapsed_ms(scan_started.elapsed()),
                "janitor object listing stream timed out"
            );
        }

        if let Some(err) = list_error {
            warn!(
                cycle,
                prefix = self.data_prefix,
                error = %err,
                scanned,
                deleted,
                delete_errors,
                cycle_elapsed_ms,
                listing_delete_elapsed_ms,
                "janitor cycle failed while streaming data prefix"
            );
            return ControlFlow::Continue(());
        }

        debug!(
            cycle,
            scanned,
            referenced = referenced.len(),
            snapshot_pins = global_snapshot_pins.len(),
            upload_reconciliation_claims = reconciliation_claim_count,
            upload_reconciliation_confirmed,
            upload_reconciliation_retryable,
            upload_reconciliation_completion_errors,
            orphan_candidates,
            recovered_undo_parents = recovered_undo_parents.len(),
            undo_total,
            undo_started,
            undo_replaying,
            undo_completed,
            deleted,
            cycle_elapsed_ms,
            listing_delete_elapsed_ms,
            claim_ms,
            remote_delete_ms,
            finish_ms,
            shutdown_cancelled,
            skipped_young,
            skipped_unknown_age,
            skipped_non_object_key,
            delete_errors,
            "janitor cycle complete"
        );
        self.janitor_shutdown_gate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reconciliation_finishes_are_bounded_and_drain_before_stopping() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::{mpsc, oneshot};

        let shutdown = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let worker_shutdown = shutdown.clone();
        let finish_calls = calls.clone();
        let worker = tokio::spawn(async move {
            finish_janitor_reconciliation_claims(
                [1, 2, 3, 4, 5],
                &worker_shutdown,
                2,
                |claim| *claim,
                move |claim| {
                    finish_calls.fetch_add(1, Ordering::SeqCst);
                    let started = started_tx.clone();
                    let (complete, result) = oneshot::channel();
                    async move {
                        let _ = started.send((claim, complete));
                        result.await.unwrap_or(Err(()))
                    }
                },
            )
            .await
        });

        let (first_id, first_complete) = started_rx.recv().await.expect("first finish starts");
        let (second_id, second_complete) = started_rx.recv().await.expect("second finish starts");
        assert_eq!((first_id, second_id), (1, 2));
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        shutdown.cancel();
        first_complete.send(Ok(())).expect("finish first DB call");
        second_complete.send(Ok(())).expect("finish second DB call");

        assert_eq!(
            worker.await.expect("finish task"),
            (2, 0, 3),
            "shutdown drains admitted DB calls and leaves unadmitted leases for retry"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            started_rx.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Disconnected
        );
    }

    #[tokio::test]
    async fn reconciliation_finishes_serialize_fragments_for_the_same_object() {
        use tokio::sync::{mpsc, oneshot};

        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let (finished_tx, mut finished_rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(async move {
            finish_janitor_reconciliation_claims(
                [(7_u64, 0_u32), (7, 1), (8, 0)],
                &worker_shutdown,
                2,
                |(object_id, _)| *object_id,
                move |claim| {
                    let started = started_tx.clone();
                    let finished = finished_tx.clone();
                    let (complete, result) = oneshot::channel();
                    async move {
                        let _ = started.send((claim, complete));
                        let result = result.await.unwrap_or(Err(()));
                        let _ = finished.send(claim);
                        result
                    }
                },
            )
            .await
        });

        let (first_claim, finish_first) = started_rx.recv().await.expect("first finish starts");
        let (other_object_claim, finish_other_object) = started_rx
            .recv()
            .await
            .expect("different object runs concurrently");
        assert_eq!(first_claim, (7, 0));
        assert_eq!(other_object_claim, (8, 0));

        finish_other_object
            .send(Ok(()))
            .expect("finish other object");
        // The current-thread worker runs until it is idle again before this
        // receive returns, so a freed slot would already have started (7, 1).
        assert_eq!(finished_rx.recv().await, Some((8, 0)));
        assert!(
            matches!(started_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "second fragment must wait for the first fragment's finish"
        );

        finish_first.send(Ok(())).expect("finish first fragment");
        let (second_fragment_claim, finish_second_fragment) = started_rx
            .recv()
            .await
            .expect("next same-object finish starts");
        assert_eq!(second_fragment_claim, (7, 1));
        finish_second_fragment
            .send(Ok(()))
            .expect("finish second fragment");

        assert_eq!(worker.await.expect("finish worker joined"), (3, 0, 0));
        assert_eq!(
            started_rx.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Disconnected
        );
    }

    #[test]
    fn future_dated_pending_deletion_is_not_old_enough() {
        let grace = Duration::from_secs(900);
        assert!(!pending_deletion_is_old_enough(1_000, 2_000, grace));
        assert!(!pending_deletion_is_old_enough(1_000, 200, grace));
        assert!(pending_deletion_is_old_enough(1_000, 100, grace));
    }

    #[test]
    fn reconciliation_finishes_confirm_before_releasing_the_object_lease() {
        use studiofs_meta::snapshot_ports::{
            UploadedFragmentLedgerEntry, UploadedFragmentReconciliationClaim,
            UploadedFragmentReconciliationOutcome::{RemoteConfirmed, Retryable},
        };
        let claim = |object_id, fragment_index| UploadedFragmentReconciliationClaim {
            entry: UploadedFragmentLedgerEntry {
                volume_id: 1,
                object_id,
                fragment_index,
                object_key: String::new(),
                expected_byte_length: 0,
                expected_sha256: [0; 32],
            },
            fencing_token: 1,
        };
        // Completion order from buffer_unordered: fast failures arrive first.
        let mut outcomes = vec![
            (claim(7, 2), Retryable),
            (claim(8, 0), Retryable),
            (claim(7, 0), Retryable),
            (claim(7, 1), RemoteConfirmed),
            (claim(8, 1), RemoteConfirmed),
        ];

        order_reconciliation_finishes(&mut outcomes);

        let order: Vec<_> = outcomes
            .iter()
            .map(|(claim, outcome)| (claim.entry.object_id, claim.entry.fragment_index, *outcome))
            .collect();
        assert_eq!(
            order,
            [
                (7, 1, RemoteConfirmed),
                (7, 0, Retryable),
                (8, 1, RemoteConfirmed),
                (8, 0, Retryable),
            ]
        );
    }

    #[test]
    fn janitor_deletes_the_listed_legacy_key_and_current_fragment_keys() {
        let legacy_key = "volume/objects/1234/0000000000001234";
        let keys = janitor_object_delete_keys("volume/objects", 0x1234, legacy_key)
            .expect("canonical legacy key");

        assert!(keys.iter().any(|key| key == legacy_key));
        assert!(keys.iter().any(|key| key.ends_with("/0000000000001234/0")));
        assert_eq!(keys.len(), 2);

        let fragment_key = "volume/objects/1234/0000000000001234/0";
        let keys = janitor_object_delete_keys("volume/objects", 0x1234, fragment_key)
            .expect("canonical fragment key");
        assert_eq!(keys.len(), 2, "both legacy and fragment forms are deleted");
        assert!(
            janitor_object_delete_keys(
                "volume/objects",
                0x1234,
                "volume/objects/unrelated/0000000000001234"
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn janitor_deletion_queue_bounds_parallelism_and_caps_only_successes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::{mpsc, oneshot};

        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let worker_active = active.clone();
        let worker_max_active = max_active.clone();
        let worker = tokio::spawn(async move {
            let mut queue = JanitorDeletionQueue::new(4, 3);
            for object_id in 1..=20 {
                queue.wait_for_slot().await;
                if queue.reached_cap() {
                    break;
                }
                let started = started_tx.clone();
                let active = worker_active.clone();
                let max_active = worker_max_active.clone();
                queue.push(async move {
                    let now_active = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now_active, Ordering::SeqCst);
                    let (complete, result) = oneshot::channel();
                    let _ = started.send((object_id, complete));
                    let outcome = result
                        .await
                        .unwrap_or(JanitorObjectDeletionResult::Retryable);
                    active.fetch_sub(1, Ordering::SeqCst);
                    (outcome, JanitorObjectDeletionTimings::default())
                });
            }
            queue.drain().await;
            queue.successful_deletes
        });

        let mut completions = HashMap::new();
        for expected_id in 1..=3 {
            let (object_id, completion) = started_rx.recv().await.expect("started task");
            assert_eq!(object_id, expected_id);
            completions.insert(object_id, completion);
        }
        assert_eq!(max_active.load(Ordering::SeqCst), 3);

        for (object_id, outcome, next_id) in [
            (1, JanitorObjectDeletionResult::Retryable, 4),
            (2, JanitorObjectDeletionResult::Protected, 5),
            (3, JanitorObjectDeletionResult::Deleted, 6),
            (4, JanitorObjectDeletionResult::Retryable, 7),
        ] {
            completions
                .remove(&object_id)
                .expect("completion")
                .send(outcome)
                .unwrap();
            let (started_id, completion) = started_rx.recv().await.expect("replacement task");
            assert_eq!(started_id, next_id);
            completions.insert(started_id, completion);
        }

        completions
            .remove(&5)
            .expect("completion")
            .send(JanitorObjectDeletionResult::Deleted)
            .unwrap();
        completions
            .remove(&6)
            .expect("completion")
            .send(JanitorObjectDeletionResult::Deleted)
            .unwrap();
        completions
            .remove(&7)
            .expect("completion")
            .send(JanitorObjectDeletionResult::Deleted)
            .unwrap();

        assert_eq!(worker.await.expect("janitor queue worker"), 4);
        assert_eq!(
            started_rx.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Disconnected
        );
        assert_eq!(max_active.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn janitor_deletion_queue_admits_at_most_the_production_concurrency() {
        use tokio::sync::{mpsc, oneshot};

        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(async move {
            let mut queue = JanitorDeletionQueue::new(u64::MAX, JANITOR_OBJECT_DELETE_CONCURRENCY);
            for _ in 0..=JANITOR_OBJECT_DELETE_CONCURRENCY {
                queue.wait_for_slot().await;
                let started = started_tx.clone();
                queue.push(async move {
                    let (complete, result) = oneshot::channel();
                    let _ = started.send(complete);
                    let result = result
                        .await
                        .unwrap_or(JanitorObjectDeletionResult::Retryable);
                    (result, JanitorObjectDeletionTimings::default())
                });
                // A pushed future starts only when the queue polls it.
                if queue.in_flight.len() == JANITOR_OBJECT_DELETE_CONCURRENCY {
                    queue.wait_for_slot().await;
                }
            }
            queue.drain().await;
            queue.successful_deletes
        });

        let mut completions = Vec::new();
        for _ in 0..JANITOR_OBJECT_DELETE_CONCURRENCY {
            completions.push(started_rx.recv().await.expect("deletion starts"));
        }
        assert!(
            matches!(started_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "a full queue must not start another deletion"
        );

        completions
            .pop()
            .expect("in-flight deletion")
            .send(JanitorObjectDeletionResult::Deleted)
            .expect("release one deletion");
        completions.push(started_rx.recv().await.expect("freed slot is reused"));
        for completion in completions {
            completion
                .send(JanitorObjectDeletionResult::Deleted)
                .expect("release deletion");
        }
        assert_eq!(
            worker.await.expect("queue worker"),
            JANITOR_OBJECT_DELETE_CONCURRENCY as u64 + 1
        );
    }

    #[tokio::test]
    async fn janitor_deletion_queue_drain_waits_for_in_flight_claimed_work() {
        use tokio::sync::oneshot;

        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let mut queue = JanitorDeletionQueue::new(1, 1);
        queue.push(async move {
            let _ = started_tx.send(());
            let result = finish_rx
                .await
                .unwrap_or(JanitorObjectDeletionResult::Retryable);
            (result, JanitorObjectDeletionTimings::default())
        });

        let drain = tokio::spawn(async move {
            queue.drain().await;
            queue.successful_deletes
        });
        started_rx.await.expect("deletion task started");
        assert!(!drain.is_finished());
        finish_tx
            .send(JanitorObjectDeletionResult::Deleted)
            .expect("release deletion task");
        assert_eq!(drain.await.expect("queue drained"), 1);
    }

    #[test]
    fn metadata_snapshot_walk_stops_when_shutdown_is_cancelled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine =
            studiofs_meta::RedbMetaEngine::open(dir.path().join("meta.redb")).expect("open redb");
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = StudioVfs::collect_metadata_snapshot(&engine, &cancel);

        assert!(matches!(result, Err(MetaError::WorkerShutdownCancelled)));
    }

    #[test]
    fn semantic_reconcile_scope_collects_named_inodes_and_resets() {
        use studiofs_meta::coherence::{InvalidationBatch, InvalidationIntent};

        let mut scope = SemanticReconcileScope::default();
        scope.observe(&InvalidationBatch {
            generation: 1,
            intents: vec![
                InvalidationIntent::Attributes { ino: 10 },
                InvalidationIntent::Content { ino: 11 },
                InvalidationIntent::Directory { ino: 12 },
                InvalidationIntent::Entry {
                    parent: 12,
                    name: b"replaced".to_vec(),
                    ancestors: None,
                    ino: Some(13),
                },
                InvalidationIntent::Entry {
                    parent: 12,
                    name: b"created".to_vec(),
                    ancestors: None,
                    ino: None,
                },
            ],
        });
        assert_eq!(scope.inodes, HashSet::from([10, 11, 12, 13]));
        assert!(!scope.reset);

        scope.reset_retired = true;
        scope.observe(&InvalidationBatch {
            generation: 2,
            intents: vec![InvalidationIntent::Reset],
        });
        assert!(scope.reset);
        assert!(
            !scope.reset_retired,
            "a new reset must retire known kernel state again"
        );
    }

    #[test]
    fn metadata_full_walk_runs_for_baseline_reset_and_backstop_only() {
        let now = Instant::now();
        let mut state = MetadataReconcileState::default();
        assert!(
            state.full_walk_due(now),
            "the first pass establishes the baseline"
        );

        state.payloads = Some(HashMap::new());
        state.last_full_walk = Some(now);
        state.scope.inodes.insert(42);
        assert!(
            !state.full_walk_due(now + METADATA_FULL_RECONCILE_MIN_INTERVAL),
            "named inode changes reconcile by scope, not by walking the volume"
        );

        state.scope.reset = true;
        assert!(
            !state.full_walk_due(now + Duration::from_secs(1)),
            "a reset burst must not repeat the walk inside the rate limit"
        );
        assert!(state.full_walk_due(now + METADATA_FULL_RECONCILE_MIN_INTERVAL));

        state.scope.reset = false;
        assert!(state.full_walk_due(now + METADATA_FULL_RECONCILE_BACKSTOP_INTERVAL));
    }

    #[test]
    fn scoped_payload_collection_reads_only_named_inodes() {
        use studiofs_meta::engine::MetaFsOps;

        let dir = tempfile::tempdir().expect("tempdir");
        let engine =
            studiofs_meta::RedbMetaEngine::open(dir.path().join("meta.redb")).expect("open redb");
        engine.init_root(0, 0).expect("init root");
        let file = engine
            .create_file(ROOT_INO, b"scoped", 0, 0, 0o644)
            .expect("create file");

        let scoped = StudioVfs::collect_scoped_metadata_payloads(
            &engine,
            &[file.ino, u64::MAX],
            &CancellationToken::new(),
        )
        .expect("collect scoped payloads");

        assert_eq!(scoped.present, HashSet::from([file.ino]));
        assert!(scoped.payloads.is_empty());
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            StudioVfs::collect_scoped_metadata_payloads(&engine, &[file.ino], &cancel),
            Err(MetaError::WorkerShutdownCancelled)
        ));
    }

    #[test]
    fn local_redb_compaction_requires_measured_file_and_fragmentation_thresholds() {
        let measured_workload = LocalStorageDiagnostics {
            physical_file_bytes: Some(6_320_128),
            fragmented_bytes: Some(1_786_496),
            ..LocalStorageDiagnostics::default()
        };
        assert!(local_redb_compaction_worthwhile(&measured_workload));

        let minimum_size_and_fragmentation = LocalStorageDiagnostics {
            physical_file_bytes: Some(4 * 1024 * 1024),
            fragmented_bytes: Some(1024 * 1024),
            ..LocalStorageDiagnostics::default()
        };
        assert!(local_redb_compaction_worthwhile(
            &minimum_size_and_fragmentation
        ));

        for diagnostics in [
            LocalStorageDiagnostics {
                physical_file_bytes: Some(1024 * 1024),
                fragmented_bytes: Some(1_007_924),
                ..LocalStorageDiagnostics::default()
            },
            LocalStorageDiagnostics {
                physical_file_bytes: Some(6_320_128),
                fragmented_bytes: Some(1_048_575),
                ..LocalStorageDiagnostics::default()
            },
            LocalStorageDiagnostics {
                physical_file_bytes: Some(6_320_128),
                fragmented_bytes: Some(1_048_576),
                ..LocalStorageDiagnostics::default()
            },
            LocalStorageDiagnostics::default(),
        ] {
            assert!(!local_redb_compaction_worthwhile(&diagnostics));
        }
    }

    #[test]
    fn local_redb_compaction_retries_incomplete_diagnostics_on_next_poll() {
        let incomplete = LocalStorageDiagnostics {
            physical_file_bytes: Some(8 * 1024 * 1024),
            ..LocalStorageDiagnostics::default()
        };
        let complete_but_below_threshold = LocalStorageDiagnostics {
            physical_file_bytes: Some(8 * 1024 * 1024),
            fragmented_bytes: Some(1024 * 1024),
            ..LocalStorageDiagnostics::default()
        };

        assert_eq!(
            local_redb_compaction_diagnostics_retry_delay(None),
            LOCAL_REDB_COMPACTION_POLL_INTERVAL
        );
        assert_eq!(
            local_redb_compaction_diagnostics_retry_delay(Some(&incomplete)),
            LOCAL_REDB_COMPACTION_POLL_INTERVAL
        );
        assert_eq!(
            local_redb_compaction_diagnostics_retry_delay(Some(&complete_but_below_threshold)),
            LOCAL_REDB_COMPACTION_DIAGNOSTIC_RECHECK
        );
    }

    #[test]
    fn local_redb_compaction_idle_window_resets_on_activity_and_preserves_maintenance() {
        let started = Instant::now();
        let mut schedule = LocalRedbCompactionSchedule::new(10);
        assert_eq!(
            schedule.observe_activity(started, 10, 0, false, false),
            Some(Duration::ZERO)
        );

        let almost_idle = started + LOCAL_REDB_COMPACTION_IDLE_WINDOW - Duration::from_secs(1);
        assert_eq!(
            schedule.observe_activity(almost_idle, 10, 0, false, false),
            Some(LOCAL_REDB_COMPACTION_IDLE_WINDOW - Duration::from_secs(1))
        );

        let activity_at = started + LOCAL_REDB_COMPACTION_IDLE_WINDOW;
        assert_eq!(
            schedule.observe_activity(activity_at, 11, 0, false, false),
            None
        );
        assert_eq!(
            schedule.observe_activity(activity_at, 11, 0, false, false),
            Some(Duration::ZERO)
        );

        let maintenance_at =
            activity_at + LOCAL_REDB_COMPACTION_IDLE_WINDOW - Duration::from_secs(1);
        assert_eq!(
            schedule.observe_activity(maintenance_at, 11, 0, false, false),
            Some(LOCAL_REDB_COMPACTION_IDLE_WINDOW - Duration::from_secs(1))
        );
        assert_eq!(
            schedule.observe_activity(maintenance_at + Duration::from_secs(1), 11, 1, false, false),
            None
        );
        assert_eq!(
            schedule.observe_activity(maintenance_at + Duration::from_secs(2), 11, 0, false, false),
            Some(LOCAL_REDB_COMPACTION_IDLE_WINDOW + Duration::from_secs(1))
        );
        assert_eq!(
            schedule.observe_activity(maintenance_at + Duration::from_secs(2), 11, 0, true, false),
            None
        );
        assert_eq!(
            schedule.observe_activity(maintenance_at + Duration::from_secs(2), 11, 0, false, true),
            None
        );
        schedule.reset_idle(11);
        assert_eq!(
            schedule.observe_activity(maintenance_at + Duration::from_secs(2), 11, 0, false, false),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn local_redb_compaction_outcomes_use_bounded_backoff() {
        let started = Instant::now();
        let mut schedule = LocalRedbCompactionSchedule::new(0);
        schedule.observe_activity(started, 0, 0, false, false);

        for (result, delay) in [
            (
                Ok(LocalStorageCompactionResult::SkippedBusy),
                LOCAL_REDB_COMPACTION_BUSY_BACKOFF,
            ),
            (
                Ok(LocalStorageCompactionResult::Compacted),
                LOCAL_REDB_COMPACTION_COOLDOWN,
            ),
            (Err(()), LOCAL_REDB_COMPACTION_ERROR_BACKOFF),
        ] {
            schedule.record_attempt(started, &result);
            assert!(!schedule.ready_to_probe(
                started + delay - Duration::from_secs(1),
                LOCAL_REDB_COMPACTION_IDLE_WINDOW,
            ));
            assert!(schedule.ready_to_probe(started + delay, LOCAL_REDB_COMPACTION_IDLE_WINDOW,));
        }
    }

    #[test]
    fn local_redb_compaction_cooldown_holds_after_twenty_idle_hours() {
        let attempted_at = Instant::now();
        let mut schedule = LocalRedbCompactionSchedule::new(0);
        schedule.record_attempt(
            attempted_at,
            &Ok::<_, ()>(LocalStorageCompactionResult::Compacted),
        );
        let idle_for = Duration::from_secs(20 * 60 * 60);

        assert!(!schedule.ready_to_probe(
            attempted_at + LOCAL_REDB_COMPACTION_COOLDOWN - Duration::from_secs(1),
            idle_for,
        ));
        assert!(schedule.ready_to_probe(attempted_at + LOCAL_REDB_COMPACTION_COOLDOWN, idle_for,));
    }

    fn token(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    #[test]
    fn notify_wake_skips_poll_sync_when_backend_already_advanced_tokens() {
        assert!(!should_run_invalidation_poll_sync(
            MetadataInvalidationWakeReason::BackendNotify,
            &token("1"),
            &token("1"),
            &token("2"),
            &token("1"),
        ));
    }

    #[test]
    fn notify_wake_still_syncs_when_tokens_did_not_advance() {
        assert!(should_run_invalidation_poll_sync(
            MetadataInvalidationWakeReason::BackendNotify,
            &token("1"),
            &token("1"),
            &token("1"),
            &token("1"),
        ));
    }

    #[test]
    fn fallback_poll_syncs_even_when_local_tokens_changed() {
        assert!(should_run_invalidation_poll_sync(
            MetadataInvalidationWakeReason::FallbackPoll,
            &token("1"),
            &token("1"),
            &token("2"),
            &token("1"),
        ));
    }

    #[test]
    fn post_sync_coverage_supersedes_a_pre_sync_deferral() {
        assert!(semantic_coverage_complete(Some(false), Some(true)));
        assert!(!semantic_coverage_complete(Some(false), None));
        assert!(semantic_coverage_complete(None, None));
    }

    #[test]
    fn changed_metadata_tokens_hold_health_until_reconciliation() {
        assert!(!invalidation_coverage_complete(
            Some(true),
            Some(true),
            true,
            false
        ));
        assert!(!invalidation_coverage_complete(
            Some(true),
            Some(true),
            false,
            true
        ));
        assert!(invalidation_coverage_complete(
            Some(true),
            Some(true),
            false,
            false
        ));
    }

    #[test]
    fn unprimed_metadata_tokens_force_first_invalidation() {
        let (namespace_changed, metadata_changed) =
            metadata_token_changes(&None, &None, &token("7"), &token("9"));

        assert!(
            namespace_changed,
            "startup namespace token must be treated as pending until the first worker wake"
        );
        assert!(
            metadata_changed,
            "startup metadata token must be treated as pending until the first worker wake"
        );
    }

    #[test]
    fn matching_metadata_tokens_do_not_invalidate_after_first_wake() {
        let (namespace_changed, metadata_changed) =
            metadata_token_changes(&token("7"), &token("9"), &token("7"), &token("9"));

        assert!(!namespace_changed);
        assert!(!metadata_changed);
    }

    #[test]
    fn metadata_snapshot_change_forces_namespace_invalidation_when_token_is_stale() {
        let previous = HashSet::from([ROOT_INO, 10]);
        let current = HashSet::from([ROOT_INO, 10, 20]);

        assert!(metadata_namespace_changed(false, Some(&previous), &current));
    }

    #[test]
    fn metadata_namespace_entry_replacement_invalidates_the_same_name() {
        let previous = HashSet::from([(ROOT_INO, b"recovery.bin".to_vec(), 10)]);
        let current = HashSet::from([(ROOT_INO, b"recovery.bin".to_vec(), 20)]);

        assert_eq!(
            metadata_namespace_entry_changes(Some(&previous), &current),
            HashSet::from([(ROOT_INO, b"recovery.bin".to_vec())])
        );
    }

    #[test]
    fn initial_metadata_namespace_snapshot_only_establishes_entry_baseline() {
        let current = HashSet::from([(ROOT_INO, b"recovery.bin".to_vec(), 10)]);

        assert!(metadata_namespace_entry_changes(None, &current).is_empty());
    }

    #[test]
    fn unchanged_metadata_snapshot_does_not_force_namespace_invalidation() {
        let snapshot = HashSet::from([ROOT_INO, 10]);

        assert!(!metadata_namespace_changed(
            false,
            Some(&snapshot),
            &snapshot
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn metadata_invalidation_fallback_timer_ticks_immediately() {
        let mut timer = metadata_invalidation_fallback_timer(Duration::from_secs(60));

        assert!(
            tokio::time::timeout(Duration::ZERO, timer.tick())
                .await
                .is_ok(),
            "the first fallback poll must establish the startup invalidation baseline"
        );
    }

    #[test]
    fn metadata_background_invalidation_keeps_runtime_only_inodes_in_attr_cache_set() {
        let previous = HashSet::from([ROOT_INO, 10]);
        let current = HashSet::from([ROOT_INO, 20]);
        let runtime_only = HashSet::from([30]);

        let attr_cache_inodes =
            metadata_attr_cache_invalidation_set(&previous, &current, true, runtime_only);

        assert!(
            attr_cache_inodes.contains(&30),
            "runtime-only inodes should still clear VFS attr/read caches after namespace changes"
        );
    }

    #[test]
    fn metadata_background_invalidation_includes_metadata_only_published_inodes() {
        let previous = HashSet::from([ROOT_INO, 10]);
        let current = HashSet::from([ROOT_INO, 10, 20]);

        let attr_cache_inodes =
            metadata_attr_cache_invalidation_set(&previous, &current, false, HashSet::new());

        assert!(
            attr_cache_inodes.contains(&20),
            "metadata-only publishes must invalidate newly reachable inode attrs"
        );
    }

    #[test]
    fn metadata_background_invalidation_clears_read_cache_for_payload_only_changes() {
        let attr_cache_inodes = HashSet::from([10]);
        let changed_payload_inodes = HashSet::from([20]);

        let read_cache_inodes =
            StudioVfs::read_cache_invalidation_set(&attr_cache_inodes, &changed_payload_inodes);

        assert!(
            read_cache_inodes.contains(&20),
            "payload-only graph updates must drop stale published read plans"
        );
        assert!(
            read_cache_inodes.contains(&10),
            "attr invalidations must keep clearing read-plan cache entries"
        );
    }

    #[test]
    fn epoch_finalizer_retries_after_waiting_for_uploads() {
        let retry_base = Duration::from_millis(100);

        assert_eq!(
            epoch_finalization_retry_delay(
                &commit::EpochFinalizeStatus::WaitingForUploads,
                retry_base,
            ),
            Some(retry_base)
        );
        assert_eq!(
            epoch_finalization_retry_delay(&commit::EpochFinalizeStatus::Deferred, retry_base),
            Some(retry_base)
        );
        assert_eq!(
            epoch_finalization_retry_delay(
                &commit::EpochFinalizeStatus::WaitingForUploads,
                Duration::ZERO,
            ),
            Some(Duration::from_millis(25))
        );
        assert_eq!(
            epoch_finalization_retry_delay(&commit::EpochFinalizeStatus::Published, retry_base),
            None
        );
        assert_eq!(
            epoch_finalization_retry_delay(&commit::EpochFinalizeStatus::Skipped, retry_base),
            None
        );
    }

    #[test]
    fn shutdown_owns_epoch_retry_before_reopening_local_stores() {
        let tmp = studiofs_test_support::workspace_target_tempdir("sfs-738-epoch-retry-shutdown");
        let (rt, vfs, meta) = super::super::tests::make_unstarted_test_vfs(&tmp, "sfs-738-retry");
        let runtime_guard = rt.enter();

        let ino = rt.block_on(async {
            let attr = vfs
                .create(ROOT_INO, b"shutdown-retry.bin", 0, 0, 0o644)
                .expect("create test file");
            vfs.write(attr.ino, 0, b"local data")
                .await
                .expect("write local data");

            vfs.state.upload.pending_epoch_finalizations.lock().clear();
            vfs.schedule_epoch_finalization_retry(
                attr.ino,
                1,
                Duration::from_secs(60),
                vfs.state.sync_safety_activity.begin_publishing(),
            );
            assert!(
                vfs.state
                    .upload
                    .pending_epoch_finalizations
                    .lock()
                    .is_empty(),
                "the retry is intentionally still in its delay before requeue"
            );
            let snapshot = vfs.sync_safety_snapshot();
            assert_ne!(snapshot.state, VfsSyncSafetyState::Safe);
            assert!(
                snapshot
                    .reasons
                    .contains(&VfsSyncSafetyReason::PublishingMetadata)
            );
            assert_eq!(
                vfs.state.shutdown.background_tasks.lock().len(),
                1,
                "epoch retry must be owned by shutdown"
            );
            vfs.finalize_deleted_inode_runtime_cleanup_deferred_cache_eviction(
                attr.ino + 1,
                vec![101],
            );
            assert_eq!(vfs.state.shutdown.background_cleanup_tasks.lock().len(), 1);
            attr.ino
        });

        drop(runtime_guard);
        vfs.finalize_deleted_inode_runtime_cleanup_deferred_cache_eviction(ino + 2, vec![102]);
        assert_eq!(
            vfs.state.shutdown.background_cleanup_threads.lock().len(),
            1
        );
        rt.block_on(async {
            vfs.shutdown(Duration::ZERO)
                .await
                .expect("shutdown should join retries and cleanup before persisting local data");
        });
        assert!(vfs.state.shutdown.background_tasks.lock().is_empty());
        assert!(
            vfs.state
                .shutdown
                .background_cleanup_tasks
                .lock()
                .is_empty()
        );
        assert!(
            vfs.state
                .shutdown
                .background_cleanup_threads
                .lock()
                .is_empty()
        );
        drop(vfs);
        drop(meta);
        drop(rt);

        let (reopen_rt, reopened, _reopened_meta) =
            super::super::tests::reopen_existing_test_vfs(&tmp);
        reopen_rt.block_on(async {
            let data = reopened
                .read(ino, 0, b"local data".len() as u32)
                .await
                .expect("read after local metadata/cache reopen");
            assert_eq!(data.as_ref(), b"local data");
        });
    }
}

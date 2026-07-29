use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const ROOT_INODE: u64 = 1;
const DEFAULT_INVALIDATION_QUEUE_CAPACITY: usize = 8192;

#[derive(Default)]
struct FuseInodeLivenessState {
    lookup_refs: HashMap<u64, u64>,
    open_refs: HashMap<u64, u64>,
    reservations: Vec<PendingLookup>,
    provisional_forgets: HashMap<u64, Vec<ProvisionalForgetBatch>>,
    next_reply_sequence: u64,
}

struct PendingLookup {
    ino: u64,
    identity: Arc<()>,
    invalidated_during_reply: bool,
    reply_sequence: Option<u64>,
}

struct ProvisionalForgetBatch {
    eligible_reply_sequences: HashSet<u64>,
    remaining: u64,
}

#[derive(Default)]
struct FuseInodeLivenessInner {
    state: Mutex<FuseInodeLivenessState>,
}

#[derive(Clone, Default)]
pub(crate) struct FuseInodeLiveness {
    inner: Arc<FuseInodeLivenessInner>,
}

impl FuseInodeLiveness {
    pub(crate) fn reserve_lookup(&self, ino: u64) -> FuseLookupReservation {
        let identity = Arc::new(());
        lock_poison_tolerant(&self.inner.state)
            .reservations
            .push(PendingLookup {
                ino,
                identity: Arc::clone(&identity),
                invalidated_during_reply: false,
                reply_sequence: None,
            });
        FuseLookupReservation {
            inner: Arc::clone(&self.inner),
            ino,
            identity,
        }
    }

    pub(crate) fn forget(&self, ino: u64, nlookup: u64) {
        if ino == ROOT_INODE || nlookup == 0 {
            return;
        }
        let mut state = lock_poison_tolerant(&self.inner.state);
        let remaining = subtract_ref_count(&mut state.lookup_refs, ino, nlookup);
        if remaining == 0 {
            return;
        }
        // A multithreaded libfuse loop may dispatch FORGET after the kernel
        // receives an entry reply but before fuse_reply_entry/create returns
        // to the replying thread. Record aggregate debt against exactly the
        // replies that were already in flight. A rejected reply cannot consume
        // this debt; another eligible successful reply must do so instead.
        let eligible_reply_sequences = state
            .reservations
            .iter()
            .filter(|reservation| reservation.ino == ino)
            .filter_map(|reservation| reservation.reply_sequence)
            .collect::<HashSet<_>>();
        let already_reserved = state
            .provisional_forgets
            .get(&ino)
            .map(|batches| batches.iter().map(|batch| batch.remaining).sum())
            .unwrap_or_default();
        let available = u64::try_from(eligible_reply_sequences.len())
            .unwrap_or(u64::MAX)
            .saturating_sub(already_reserved);
        let provisional = remaining.min(available);
        if provisional != 0 {
            state
                .provisional_forgets
                .entry(ino)
                .or_default()
                .push(ProvisionalForgetBatch {
                    eligible_reply_sequences,
                    remaining: provisional,
                });
        }
    }

    pub(crate) fn open_handle(&self, ino: u64) -> FuseOpenHandleGuard {
        let mut state = lock_poison_tolerant(&self.inner.state);
        increment_ref_count(&mut state.open_refs, ino);
        FuseOpenHandleGuard {
            inner: Arc::clone(&self.inner),
            ino,
            active: true,
        }
    }

    pub(crate) fn is_live(&self, ino: u64) -> bool {
        if ino == ROOT_INODE {
            return true;
        }
        let state = lock_poison_tolerant(&self.inner.state);
        state.lookup_refs.get(&ino).copied().unwrap_or_default() != 0
            || state.open_refs.get(&ino).copied().unwrap_or_default() != 0
    }

    /// Admit an invalidation without racing a lookup reply.
    ///
    /// Every pending reply for `ino` is marked for a follow-up notification.
    /// The callback runs immediately only when the kernel already owns a
    /// committed lookup reference or an open handle.
    pub(crate) fn admit_invalidation(
        &self,
        ino: u64,
        notify_kernel: impl FnOnce(),
    ) -> FuseInvalidationAdmission {
        let (notify_now, deferred_reply_count) = {
            let mut state = lock_poison_tolerant(&self.inner.state);
            let mut deferred_reply_count = 0;
            for reservation in &mut state.reservations {
                if reservation.ino == ino {
                    reservation.invalidated_during_reply = true;
                    deferred_reply_count += 1;
                }
            }
            let notify_now = ino == ROOT_INODE
                || state.lookup_refs.get(&ino).copied().unwrap_or_default() != 0
                || state.open_refs.get(&ino).copied().unwrap_or_default() != 0;
            (notify_now, deferred_reply_count)
        };
        if notify_now {
            notify_kernel();
        }
        FuseInvalidationAdmission {
            notified_now: notify_now,
            deferred_reply_count,
        }
    }
}

/// Result of admitting an invalidation against kernel-observed inode state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FuseInvalidationAdmission {
    notified_now: bool,
    deferred_reply_count: usize,
}

impl FuseInvalidationAdmission {
    pub(crate) const fn notified_now(self) -> bool {
        self.notified_now
    }

    pub(crate) const fn deferred_reply_count(self) -> usize {
        self.deferred_reply_count
    }
}

pub(crate) struct FuseLookupReservation {
    inner: Arc<FuseInodeLivenessInner>,
    ino: u64,
    identity: Arc<()>,
}

impl FuseLookupReservation {
    /// Mark the reservation immediately before calling libfuse's entry reply.
    ///
    /// This provisional phase lets a concurrent FORGET consume the one lookup
    /// reference after the kernel receives the reply but before libfuse
    /// returns success to the replying thread.
    pub(crate) fn begin_reply(&self) -> bool {
        let mut state = lock_poison_tolerant(&self.inner.state);
        let Some(index) = state.reservations.iter().position(|reservation| {
            reservation.ino == self.ino && Arc::ptr_eq(&reservation.identity, &self.identity)
        }) else {
            return false;
        };
        if state.reservations[index].reply_sequence.is_some() {
            return false;
        }
        let reply_sequence = state.next_reply_sequence;
        state.next_reply_sequence = state.next_reply_sequence.wrapping_add(1);
        state.reservations[index].reply_sequence = Some(reply_sequence);
        true
    }

    /// Commit a successful entry reply and report whether it needs a follow-up
    /// invalidation because content changed while the reply was in flight.
    pub(crate) fn commit(&self) -> Option<FuseLookupCommitOutcome> {
        let mut state = lock_poison_tolerant(&self.inner.state);
        let Some(index) = state.reservations.iter().position(|reservation| {
            reservation.ino == self.ino && Arc::ptr_eq(&reservation.identity, &self.identity)
        }) else {
            return None;
        };
        let Some(reply_sequence) = state.reservations[index].reply_sequence else {
            return None;
        };
        let reservation = state.reservations.swap_remove(index);
        let lookup_ref_committed =
            !finish_provisional_reply(&mut state, self.ino, reply_sequence, true);
        if lookup_ref_committed {
            increment_ref_count(&mut state.lookup_refs, self.ino);
        }
        Some(FuseLookupCommitOutcome {
            follow_up_invalidation_required: reservation.invalidated_during_reply,
            lookup_ref_committed,
        })
    }

    pub(crate) fn rollback(&self) -> bool {
        let mut state = lock_poison_tolerant(&self.inner.state);
        let Some(index) = state.reservations.iter().position(|reservation| {
            reservation.ino == self.ino && Arc::ptr_eq(&reservation.identity, &self.identity)
        }) else {
            return false;
        };
        let reservation = state.reservations.swap_remove(index);
        if let Some(reply_sequence) = reservation.reply_sequence {
            finish_provisional_reply(&mut state, self.ino, reply_sequence, false);
        }
        true
    }
}

/// Outcome of committing one successful kernel entry reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FuseLookupCommitOutcome {
    follow_up_invalidation_required: bool,
    lookup_ref_committed: bool,
}

impl FuseLookupCommitOutcome {
    pub(crate) const fn follow_up_invalidation_required(self) -> bool {
        self.follow_up_invalidation_required
    }

    pub(crate) const fn lookup_ref_committed(self) -> bool {
        self.lookup_ref_committed
    }
}

impl Drop for FuseLookupReservation {
    fn drop(&mut self) {
        self.rollback();
    }
}

pub(crate) struct FuseOpenHandleGuard {
    inner: Arc<FuseInodeLivenessInner>,
    ino: u64,
    active: bool,
}

impl FuseOpenHandleGuard {
    pub(crate) fn close(&mut self) -> bool {
        if !self.active {
            return false;
        }
        self.active = false;
        let mut state = lock_poison_tolerant(&self.inner.state);
        subtract_ref_count(&mut state.open_refs, self.ino, 1);
        true
    }
}

impl Drop for FuseOpenHandleGuard {
    fn drop(&mut self) {
        self.close();
    }
}

fn increment_ref_count(refs: &mut HashMap<u64, u64>, ino: u64) {
    let count = refs.entry(ino).or_default();
    *count = count.saturating_add(1);
}

fn subtract_ref_count(refs: &mut HashMap<u64, u64>, ino: u64, count: u64) -> u64 {
    let Some(current) = refs.get_mut(&ino) else {
        return count;
    };
    let consumed = (*current).min(count);
    *current -= consumed;
    if *current == 0 {
        refs.remove(&ino);
    }
    count - consumed
}

/// Finish one provisional reply and return whether it consumed a FORGET.
///
/// Forget batches are ordered by observation time. Their eligible sets are
/// nested: a later batch contains every still-in-flight reply from an earlier
/// batch plus any replies begun since. Keeping older debt first therefore
/// preserves exact eligibility while allowing a rejected reply's debt to move
/// to another successful reply from the same set.
fn finish_provisional_reply(
    state: &mut FuseInodeLivenessState,
    ino: u64,
    reply_sequence: u64,
    successful: bool,
) -> bool {
    let Some(batches) = state.provisional_forgets.get_mut(&ino) else {
        return false;
    };
    let consumed = if successful {
        if let Some(batch) = batches.iter_mut().find(|batch| {
            batch.remaining != 0 && batch.eligible_reply_sequences.contains(&reply_sequence)
        }) {
            batch.remaining -= 1;
            true
        } else {
            false
        }
    } else {
        false
    };

    let mut earlier_debt = 0_u64;
    for batch in batches.iter_mut() {
        batch.eligible_reply_sequences.remove(&reply_sequence);
        let eligible = u64::try_from(batch.eligible_reply_sequences.len()).unwrap_or(u64::MAX);
        batch.remaining = batch.remaining.min(eligible.saturating_sub(earlier_debt));
        earlier_debt = earlier_debt.saturating_add(batch.remaining);
    }
    batches.retain(|batch| batch.remaining != 0);
    if batches.is_empty() {
        state.provisional_forgets.remove(&ino);
    }
    consumed
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FuseInvalidationKind {
    Attributes,
    Data,
}

impl FuseInvalidationKind {
    fn coalesce(self, other: Self) -> Self {
        if self == Self::Data || other == Self::Data {
            Self::Data
        } else {
            Self::Attributes
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FuseInvalidationQueueClosed;

struct FuseInvalidationQueueState {
    closed: bool,
    order: VecDeque<u64>,
    pending: HashMap<u64, FuseInvalidationKind>,
    waiting_producers: usize,
}

struct FuseInvalidationQueueInner {
    capacity: usize,
    state: Mutex<FuseInvalidationQueueState>,
    changed: Condvar,
    metrics: Arc<FuseInvalidationMetricsInner>,
    worker_claimed: AtomicBool,
}

#[derive(Clone)]
pub(crate) struct FuseInvalidationQueue {
    inner: Arc<FuseInvalidationQueueInner>,
}

impl Default for FuseInvalidationQueue {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_INVALIDATION_QUEUE_CAPACITY)
    }
}

impl FuseInvalidationQueue {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "invalidation queue capacity must be non-zero");
        Self {
            inner: Arc::new(FuseInvalidationQueueInner {
                capacity,
                state: Mutex::new(FuseInvalidationQueueState {
                    closed: false,
                    order: VecDeque::with_capacity(capacity),
                    pending: HashMap::with_capacity(capacity),
                    waiting_producers: 0,
                }),
                changed: Condvar::new(),
                metrics: Arc::new(FuseInvalidationMetricsInner::default()),
                worker_claimed: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub(crate) fn push(
        &self,
        ino: u64,
        kind: FuseInvalidationKind,
    ) -> Result<(), FuseInvalidationQueueClosed> {
        let mut state = lock_poison_tolerant(&self.inner.state);
        loop {
            if state.closed {
                return Err(FuseInvalidationQueueClosed);
            }
            if let Some(current) = state.pending.get_mut(&ino) {
                *current = current.coalesce(kind);
                self.inner.metrics.coalesced.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            if state.pending.len() < self.inner.capacity {
                state.order.push_back(ino);
                state.pending.insert(ino, kind);
                let depth = self
                    .inner
                    .metrics
                    .queue_depth
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                self.inner
                    .metrics
                    .queue_peak
                    .fetch_max(depth, Ordering::Relaxed);
                self.inner.changed.notify_all();
                return Ok(());
            }
            state.waiting_producers = state.waiting_producers.saturating_add(1);
            self.inner.changed.notify_all();
            state = wait_poison_tolerant(&self.inner.changed, state);
            state.waiting_producers = state.waiting_producers.saturating_sub(1);
        }
    }

    pub(crate) fn pop(&self) -> Option<(u64, FuseInvalidationKind)> {
        let mut state = lock_poison_tolerant(&self.inner.state);
        loop {
            if let Some(ino) = state.order.pop_front() {
                let Some(kind) = state.pending.remove(&ino) else {
                    continue;
                };
                self.inner
                    .metrics
                    .queue_depth
                    .fetch_sub(1, Ordering::Relaxed);
                self.inner.changed.notify_all();
                return Some((ino, kind));
            }
            if state.closed {
                return None;
            }
            state = wait_poison_tolerant(&self.inner.changed, state);
        }
    }

    pub(crate) fn close(&self) {
        let mut state = lock_poison_tolerant(&self.inner.state);
        state.closed = true;
        self.inner.changed.notify_all();
    }

    #[cfg(test)]
    fn wait_until_producer_blocked(&self) {
        let mut state = lock_poison_tolerant(&self.inner.state);
        while state.waiting_producers == 0 {
            state = wait_poison_tolerant(&self.inner.changed, state);
        }
    }
}

/// Geometry passed to the platform-specific inode notification function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FuseInvalidationGeometry {
    pub(crate) offset: i64,
    pub(crate) length: i64,
}

impl FuseInvalidationGeometry {
    /// Invalidate inode attributes without evicting cached data pages.
    pub(crate) const ATTRIBUTES: Self = Self {
        offset: -1,
        length: 0,
    };

    /// Invalidate the complete cached data range and inode attributes.
    pub(crate) const DATA: Self = Self {
        offset: 0,
        length: 0,
    };

    fn for_kind(kind: FuseInvalidationKind) -> Self {
        match kind {
            FuseInvalidationKind::Attributes => Self::ATTRIBUTES,
            FuseInvalidationKind::Data => Self::DATA,
        }
    }
}

#[derive(Default)]
struct FuseInvalidationMetricsInner {
    queue_depth: AtomicUsize,
    queue_peak: AtomicUsize,
    coalesced: AtomicU64,
    skipped_unobserved: AtomicU64,
    notify_failures: AtomicU64,
    notify_latency_count: AtomicU64,
    notify_latency_total_nanos: AtomicU64,
    notify_latency_max_nanos: AtomicU64,
    shutdown_timeouts: AtomicU64,
    worker_panics: AtomicU64,
    notification_in_flight: AtomicBool,
}

/// Point-in-time invalidation worker counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FuseInvalidationMetricsSnapshot {
    pub(crate) queue_depth: usize,
    pub(crate) queue_peak: usize,
    pub(crate) coalesced: u64,
    pub(crate) skipped_unobserved: u64,
    pub(crate) notify_failures: u64,
    pub(crate) notify_latency_count: u64,
    pub(crate) notify_latency_total_nanos: u64,
    pub(crate) notify_latency_max_nanos: u64,
    pub(crate) shutdown_timeouts: u64,
    pub(crate) worker_panics: u64,
    pub(crate) notification_in_flight: bool,
}

impl FuseInvalidationMetricsInner {
    fn snapshot(&self) -> FuseInvalidationMetricsSnapshot {
        FuseInvalidationMetricsSnapshot {
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_peak: self.queue_peak.load(Ordering::Relaxed),
            coalesced: self.coalesced.load(Ordering::Relaxed),
            skipped_unobserved: self.skipped_unobserved.load(Ordering::Relaxed),
            notify_failures: self.notify_failures.load(Ordering::Relaxed),
            notify_latency_count: self.notify_latency_count.load(Ordering::Relaxed),
            notify_latency_total_nanos: self.notify_latency_total_nanos.load(Ordering::Relaxed),
            notify_latency_max_nanos: self.notify_latency_max_nanos.load(Ordering::Relaxed),
            shutdown_timeouts: self.shutdown_timeouts.load(Ordering::Relaxed),
            worker_panics: self.worker_panics.load(Ordering::Relaxed),
            notification_in_flight: self.notification_in_flight.load(Ordering::Acquire),
        }
    }

    fn record_notify_latency(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.notify_latency_count.fetch_add(1, Ordering::Relaxed);
        self.notify_latency_total_nanos
            .fetch_add(nanos, Ordering::Relaxed);
        self.notify_latency_max_nanos
            .fetch_max(nanos, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct FuseInvalidationCompletion {
    completed: Mutex<bool>,
    changed: Condvar,
}

struct FuseInvalidationCompletionGuard {
    completion: Arc<FuseInvalidationCompletion>,
}

impl Drop for FuseInvalidationCompletionGuard {
    fn drop(&mut self) {
        *lock_poison_tolerant(&self.completion.completed) = true;
        self.completion.changed.notify_all();
    }
}

struct FuseInvalidationInFlightGuard<'a> {
    in_flight: &'a AtomicBool,
}

impl Drop for FuseInvalidationInFlightGuard<'_> {
    fn drop(&mut self) {
        self.in_flight.store(false, Ordering::Release);
    }
}

/// Result of closing and joining the invalidation worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FuseInvalidationShutdownOutcome {
    Drained,
    TimedOut,
    WorkerPanicked,
}

/// Owns the sole consumer of a bounded invalidation queue.
///
/// The notifier closure is the platform seam: this core never owns or
/// dereferences a raw libfuse session pointer.
pub(crate) struct FuseInvalidationWorker {
    queue: FuseInvalidationQueue,
    completion: Arc<FuseInvalidationCompletion>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl FuseInvalidationWorker {
    pub(crate) fn spawn(
        queue: FuseInvalidationQueue,
        liveness: FuseInodeLiveness,
        notifier: impl Fn(u64, FuseInvalidationGeometry) -> Result<(), ()> + Send + 'static,
    ) -> io::Result<Self> {
        if queue
            .inner
            .worker_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "invalidation queue already has a worker",
            ));
        }
        let completion = Arc::new(FuseInvalidationCompletion::default());
        let worker_queue = queue.clone();
        let worker_completion = Arc::clone(&completion);
        let metrics = Arc::clone(&queue.inner.metrics);
        let thread = match thread::Builder::new()
            .name("studiofs-fuse-invalidation".to_owned())
            .spawn(move || {
                let _completion = FuseInvalidationCompletionGuard {
                    completion: worker_completion,
                };
                while let Some((ino, kind)) = worker_queue.pop() {
                    let geometry = FuseInvalidationGeometry::for_kind(kind);
                    let mut notify_result = None;
                    let admission = liveness.admit_invalidation(ino, || {
                        metrics
                            .notification_in_flight
                            .store(true, Ordering::Release);
                        let _in_flight = FuseInvalidationInFlightGuard {
                            in_flight: &metrics.notification_in_flight,
                        };
                        let started = Instant::now();
                        notify_result = Some(notifier(ino, geometry));
                        metrics.record_notify_latency(started.elapsed());
                    });
                    if !admission.notified_now() {
                        metrics.skipped_unobserved.fetch_add(1, Ordering::Relaxed);
                    } else if notify_result == Some(Err(())) {
                        metrics.notify_failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }) {
            Ok(thread) => thread,
            Err(error) => {
                queue.inner.worker_claimed.store(false, Ordering::Release);
                return Err(error);
            }
        };
        Ok(Self {
            queue,
            completion,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub(crate) fn push(
        &self,
        ino: u64,
        kind: FuseInvalidationKind,
    ) -> Result<(), FuseInvalidationQueueClosed> {
        self.queue.push(ino, kind)
    }

    pub(crate) fn metrics(&self) -> FuseInvalidationMetricsSnapshot {
        self.queue.inner.metrics.snapshot()
    }

    /// Close admission, drain accepted entries and wait up to `timeout`.
    ///
    /// A timeout deliberately leaves the join handle owned by this object so
    /// callers can retry after arranging for an in-flight notifier to finish.
    pub(crate) fn shutdown(&self, timeout: Duration) -> FuseInvalidationShutdownOutcome {
        self.queue.close();
        let completed = lock_poison_tolerant(&self.completion.completed);
        let completed = if *completed {
            completed
        } else {
            let (completed, _) = self
                .completion
                .changed
                .wait_timeout_while(completed, timeout, |completed| !*completed)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            completed
        };
        if !*completed {
            self.queue
                .inner
                .metrics
                .shutdown_timeouts
                .fetch_add(1, Ordering::Relaxed);
            return FuseInvalidationShutdownOutcome::TimedOut;
        }
        drop(completed);
        if let Some(thread) = lock_poison_tolerant(&self.thread).take() {
            if thread.join().is_err() {
                self.queue
                    .inner
                    .metrics
                    .worker_panics
                    .fetch_add(1, Ordering::Relaxed);
                return FuseInvalidationShutdownOutcome::WorkerPanicked;
            }
        }
        FuseInvalidationShutdownOutcome::Drained
    }
}

fn lock_poison_tolerant<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn wait_poison_tolerant<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    #[test]
    fn root_is_always_live() {
        let liveness = FuseInodeLiveness::default();

        assert!(liveness.is_live(1));
    }

    #[test]
    fn invalidation_before_reply_defers_follow_up_until_commit() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup(42);
        let callback_called = AtomicBool::new(false);

        assert!(!liveness.is_live(42));
        let admission = liveness.admit_invalidation(42, || {
            callback_called.store(true, Ordering::Release);
        });

        assert!(!admission.notified_now());
        assert_eq!(admission.deferred_reply_count(), 1);
        assert!(!callback_called.load(Ordering::Acquire));
        assert!(reservation.begin_reply());
        assert!(
            reservation
                .commit()
                .unwrap()
                .follow_up_invalidation_required()
        );
        assert!(reservation.commit().is_none());
        assert!(liveness.is_live(42));
    }

    #[test]
    fn rollback_discards_deferred_invalidation_marker() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup(42);
        let admission = liveness.admit_invalidation(42, || {});

        assert_eq!(admission.deferred_reply_count(), 1);
        assert!(reservation.rollback());
        assert!(reservation.commit().is_none());
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn stale_lookup_token_cannot_commit_reused_inode_reservation() {
        let liveness = FuseInodeLiveness::default();
        let stale = liveness.reserve_lookup(42);
        assert!(stale.rollback());
        let current = liveness.reserve_lookup(42);

        assert!(stale.commit().is_none());
        assert!(current.begin_reply());
        assert!(!current.commit().unwrap().follow_up_invalidation_required());
        assert!(liveness.is_live(42));
    }

    #[test]
    fn old_pending_reservation_rollback_does_not_remove_committed_lookup() {
        let liveness = FuseInodeLiveness::default();
        let old = liveness.reserve_lookup(42);
        let current = liveness.reserve_lookup(42);
        assert!(current.begin_reply());
        assert!(!current.commit().unwrap().follow_up_invalidation_required());

        assert!(old.rollback());
        drop(old);
        assert!(liveness.is_live(42));
        liveness.forget(42, 1);
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn forget_saturates_at_lookup_count() {
        let liveness = FuseInodeLiveness::default();
        for _ in 0..2 {
            let reservation = liveness.reserve_lookup(42);
            assert!(reservation.begin_reply());
            assert!(reservation.commit().is_some());
        }

        liveness.forget(42, 9);

        assert!(!liveness.is_live(42));
    }

    #[test]
    fn forget_zero_is_a_noop() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup(42);
        assert!(reservation.begin_reply());
        assert!(reservation.commit().is_some());

        liveness.forget(42, 0);

        assert!(liveness.is_live(42));
    }

    #[test]
    fn open_guard_keeps_inode_live_without_lookup() {
        let liveness = FuseInodeLiveness::default();
        let open = liveness.open_handle(42);

        assert!(liveness.is_live(42));
        drop(open);
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn explicit_open_guard_close_is_exactly_once() {
        let liveness = FuseInodeLiveness::default();
        let mut first = liveness.open_handle(42);
        let second = liveness.open_handle(42);

        assert!(first.close());
        assert!(!first.close());
        assert!(liveness.is_live(42));
        drop(second);
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn invalidation_callback_does_not_hold_liveness_lock() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup(42);
        assert!(reservation.begin_reply());
        assert!(reservation.commit().is_some());
        let callback_liveness = liveness.clone();

        let admission = liveness.admit_invalidation(42, move || {
            callback_liveness.forget(42, 1);
        });

        assert!(admission.notified_now());
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn committed_ref_notifies_now_and_pending_reply_still_needs_follow_up() {
        let liveness = FuseInodeLiveness::default();
        let committed = liveness.reserve_lookup(42);
        assert!(committed.begin_reply());
        assert!(committed.commit().is_some());
        let pending = liveness.reserve_lookup(42);
        let callback_called = AtomicBool::new(false);

        let admission = liveness.admit_invalidation(42, || {
            callback_called.store(true, Ordering::Release);
        });

        assert!(admission.notified_now());
        assert_eq!(admission.deferred_reply_count(), 1);
        assert!(callback_called.load(Ordering::Acquire));
        assert!(pending.begin_reply());
        assert!(pending.commit().unwrap().follow_up_invalidation_required());
    }

    #[test]
    fn forget_during_reply_consumes_provisional_lookup_before_commit() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup(42);
        assert!(reservation.begin_reply());

        liveness.forget(42, 1);

        let outcome = reservation.commit().expect("successful reply");
        assert!(!outcome.lookup_ref_committed());
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn forget_consumes_each_same_inode_provisional_reply_at_most_once() {
        let liveness = FuseInodeLiveness::default();
        let first = liveness.reserve_lookup(42);
        let second = liveness.reserve_lookup(42);
        let third = liveness.reserve_lookup(42);
        assert!(first.begin_reply());
        assert!(second.begin_reply());
        assert!(third.begin_reply());

        liveness.forget(42, 2);

        let outcomes = [
            first.commit().expect("first successful reply"),
            second.commit().expect("second successful reply"),
            third.commit().expect("third successful reply"),
        ];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| outcome.lookup_ref_committed())
                .count(),
            1
        );
        assert!(liveness.is_live(42));
        liveness.forget(42, 1);
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn invalidated_provisional_reply_keeps_follow_up_after_concurrent_forget() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup(42);
        assert!(reservation.begin_reply());
        let admission = liveness.admit_invalidation(42, || {});
        assert_eq!(admission.deferred_reply_count(), 1);

        liveness.forget(42, 1);

        let outcome = reservation.commit().expect("successful reply");
        assert!(!outcome.lookup_ref_committed());
        assert!(outcome.follow_up_invalidation_required());
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn rejected_provisional_reply_rolls_back_without_lookup_ref() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup(42);
        assert!(reservation.begin_reply());
        liveness.forget(42, 1);

        assert!(reservation.rollback());
        assert!(reservation.commit().is_none());
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn rejected_reply_does_not_steal_forget_from_same_inode_successful_reply() {
        let liveness = FuseInodeLiveness::default();
        let rejected = liveness.reserve_lookup(42);
        let accepted = liveness.reserve_lookup(42);
        assert!(rejected.begin_reply());
        assert!(accepted.begin_reply());

        liveness.forget(42, 1);

        assert!(rejected.rollback());
        let outcome = accepted.commit().expect("successful reply");
        assert!(!outcome.lookup_ref_committed());
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn future_reply_cannot_consume_forget_from_rejected_reply_cohort() {
        let liveness = FuseInodeLiveness::default();
        let rejected = liveness.reserve_lookup(42);
        assert!(rejected.begin_reply());
        liveness.forget(42, 1);
        assert!(rejected.rollback());

        let future = liveness.reserve_lookup(42);
        assert!(future.begin_reply());
        let outcome = future.commit().expect("future successful reply");

        assert!(outcome.lookup_ref_committed());
        assert!(liveness.is_live(42));
    }

    #[test]
    fn future_reply_cannot_consume_debt_while_older_eligible_reply_remains() {
        let liveness = FuseInodeLiveness::default();
        let rejected = liveness.reserve_lookup(42);
        let eligible = liveness.reserve_lookup(42);
        assert!(rejected.begin_reply());
        assert!(eligible.begin_reply());
        liveness.forget(42, 2);
        assert!(rejected.rollback());

        let future = liveness.reserve_lookup(42);
        assert!(future.begin_reply());
        let future_outcome = future.commit().expect("future successful reply");
        let eligible_outcome = eligible.commit().expect("eligible successful reply");

        assert!(future_outcome.lookup_ref_committed());
        assert!(!eligible_outcome.lookup_ref_committed());
        assert!(liveness.is_live(42));
        liveness.forget(42, 1);
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn forget_consumes_committed_refs_before_provisional_reply_debt() {
        let liveness = FuseInodeLiveness::default();
        let committed = liveness.reserve_lookup(42);
        assert!(committed.begin_reply());
        assert!(committed.commit().unwrap().lookup_ref_committed());
        let first = liveness.reserve_lookup(42);
        let second = liveness.reserve_lookup(42);
        assert!(first.begin_reply());
        assert!(second.begin_reply());

        liveness.forget(42, 2);

        let outcomes = [
            first.commit().expect("first successful reply"),
            second.commit().expect("second successful reply"),
        ];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| outcome.lookup_ref_committed())
                .count(),
            1
        );
        assert!(liveness.is_live(42));
        liveness.forget(42, 1);
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn multiple_forgets_reassign_debt_across_rejected_same_inode_replies() {
        let liveness = FuseInodeLiveness::default();
        let first = liveness.reserve_lookup(42);
        let second = liveness.reserve_lookup(42);
        let third = liveness.reserve_lookup(42);
        assert!(first.begin_reply());
        assert!(second.begin_reply());
        liveness.forget(42, 1);
        assert!(third.begin_reply());
        liveness.forget(42, 2);

        assert!(first.rollback());
        let outcomes = [
            second.commit().expect("second successful reply"),
            third.commit().expect("third successful reply"),
        ];

        assert!(
            outcomes
                .iter()
                .all(|outcome| !outcome.lookup_ref_committed())
        );
        assert!(!liveness.is_live(42));
    }

    #[test]
    fn queue_coalesces_same_inode_and_data_dominates_attributes() {
        let queue = FuseInvalidationQueue::with_capacity(2);
        queue.push(42, FuseInvalidationKind::Attributes).unwrap();
        queue.push(42, FuseInvalidationKind::Data).unwrap();

        assert_eq!(queue.pop(), Some((42, FuseInvalidationKind::Data)));
    }

    #[test]
    fn queue_keeps_fifo_order_for_distinct_inodes() {
        let queue = FuseInvalidationQueue::with_capacity(2);
        queue.push(10, FuseInvalidationKind::Data).unwrap();
        queue.push(20, FuseInvalidationKind::Attributes).unwrap();

        assert_eq!(queue.pop(), Some((10, FuseInvalidationKind::Data)));
        assert_eq!(queue.pop(), Some((20, FuseInvalidationKind::Attributes)));
    }

    #[test]
    fn full_queue_backpressures_new_distinct_inode() {
        let queue = Arc::new(FuseInvalidationQueue::with_capacity(1));
        queue.push(10, FuseInvalidationKind::Data).unwrap();
        let producer_queue = Arc::clone(&queue);
        let completed = Arc::new(AtomicBool::new(false));
        let producer_completed = Arc::clone(&completed);
        let producer = thread::spawn(move || {
            producer_queue.push(20, FuseInvalidationKind::Data).unwrap();
            producer_completed.store(true, Ordering::Release);
        });

        queue.wait_until_producer_blocked();
        assert!(!completed.load(Ordering::Acquire));
        assert_eq!(queue.pop(), Some((10, FuseInvalidationKind::Data)));
        producer.join().unwrap();
        assert_eq!(queue.pop(), Some((20, FuseInvalidationKind::Data)));
    }

    #[test]
    fn close_wakes_blocked_producer_with_error() {
        let queue = Arc::new(FuseInvalidationQueue::with_capacity(1));
        queue.push(10, FuseInvalidationKind::Data).unwrap();
        let producer_queue = Arc::clone(&queue);
        let producer = thread::spawn(move || producer_queue.push(20, FuseInvalidationKind::Data));

        queue.wait_until_producer_blocked();
        queue.close();

        assert_eq!(producer.join().unwrap(), Err(FuseInvalidationQueueClosed));
    }

    #[test]
    fn close_drains_entries_accepted_before_close() {
        let queue = FuseInvalidationQueue::with_capacity(2);
        queue.push(10, FuseInvalidationKind::Data).unwrap();
        queue.push(20, FuseInvalidationKind::Attributes).unwrap();

        queue.close();

        assert_eq!(queue.pop(), Some((10, FuseInvalidationKind::Data)));
        assert_eq!(queue.pop(), Some((20, FuseInvalidationKind::Attributes)));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn default_queue_capacity_is_8192_distinct_inodes() {
        let queue = FuseInvalidationQueue::default();

        assert_eq!(queue.capacity(), 8192);
    }

    #[test]
    fn worker_notifies_only_live_inodes_with_kind_specific_geometry() {
        let liveness = FuseInodeLiveness::default();
        let lookup = liveness.reserve_lookup(42);
        assert!(lookup.begin_reply());
        assert!(lookup.commit().is_some());
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&notifications);
        let worker = FuseInvalidationWorker::spawn(
            FuseInvalidationQueue::with_capacity(4),
            liveness,
            move |ino, geometry| {
                lock_poison_tolerant(&observed).push((ino, geometry));
                Ok(())
            },
        )
        .unwrap();

        worker
            .push(41, FuseInvalidationKind::Data)
            .expect("worker accepts unobserved inode");
        worker
            .push(42, FuseInvalidationKind::Attributes)
            .expect("worker accepts live inode");
        worker
            .push(ROOT_INODE, FuseInvalidationKind::Data)
            .expect("worker accepts root");

        assert_eq!(
            worker.shutdown(std::time::Duration::from_secs(1)),
            FuseInvalidationShutdownOutcome::Drained
        );
        assert_eq!(
            *lock_poison_tolerant(&notifications),
            vec![
                (42, FuseInvalidationGeometry::ATTRIBUTES),
                (ROOT_INODE, FuseInvalidationGeometry::DATA),
            ]
        );
        let metrics = worker.metrics();
        assert_eq!(metrics.skipped_unobserved, 1);
        assert_eq!(metrics.notify_failures, 0);
    }

    #[test]
    fn worker_coalesces_and_drains_accepted_items_after_close() {
        let liveness = FuseInodeLiveness::default();
        let open = liveness.open_handle(42);
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&notifications);
        let queue = FuseInvalidationQueue::with_capacity(2);
        queue.push(42, FuseInvalidationKind::Attributes).unwrap();
        queue.push(42, FuseInvalidationKind::Data).unwrap();
        let worker = FuseInvalidationWorker::spawn(queue, liveness, move |ino, geometry| {
            lock_poison_tolerant(&observed).push((ino, geometry));
            Ok(())
        })
        .unwrap();

        assert_eq!(
            worker.shutdown(std::time::Duration::from_secs(1)),
            FuseInvalidationShutdownOutcome::Drained
        );
        drop(open);

        assert_eq!(
            *lock_poison_tolerant(&notifications),
            vec![(42, FuseInvalidationGeometry::DATA)]
        );
        let metrics = worker.metrics();
        assert_eq!(metrics.coalesced, 1);
        assert_eq!(metrics.queue_depth, 0);
        assert_eq!(metrics.queue_peak, 1);
        assert_eq!(
            worker.push(7, FuseInvalidationKind::Data),
            Err(FuseInvalidationQueueClosed)
        );
    }

    #[test]
    fn worker_notifier_runs_outside_liveness_lock_and_records_failures() {
        let liveness = FuseInodeLiveness::default();
        let lookup = liveness.reserve_lookup(42);
        assert!(lookup.begin_reply());
        assert!(lookup.commit().is_some());
        let callback_liveness = liveness.clone();
        let worker = FuseInvalidationWorker::spawn(
            FuseInvalidationQueue::with_capacity(1),
            liveness,
            move |ino, _geometry| {
                callback_liveness.forget(ino, 1);
                Err(())
            },
        )
        .unwrap();

        worker.push(42, FuseInvalidationKind::Data).unwrap();

        assert_eq!(
            worker.shutdown(std::time::Duration::from_secs(1)),
            FuseInvalidationShutdownOutcome::Drained
        );
        assert_eq!(worker.metrics().notify_failures, 1);
    }

    #[test]
    fn shutdown_timeout_is_reported_without_blocking_join() {
        let liveness = FuseInodeLiveness::default();
        let open = liveness.open_handle(42);
        let notifier_started = Arc::new((Mutex::new(false), Condvar::new()));
        let notifier_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let started = Arc::clone(&notifier_started);
        let gate = Arc::clone(&notifier_gate);
        let worker = FuseInvalidationWorker::spawn(
            FuseInvalidationQueue::with_capacity(1),
            liveness,
            move |_ino, _geometry| {
                let (started_lock, started_changed) = &*started;
                *lock_poison_tolerant(started_lock) = true;
                started_changed.notify_all();
                let (gate_lock, gate_changed) = &*gate;
                let mut released = lock_poison_tolerant(gate_lock);
                while !*released {
                    released = wait_poison_tolerant(gate_changed, released);
                }
                Ok(())
            },
        )
        .unwrap();
        worker.push(42, FuseInvalidationKind::Data).unwrap();
        let (started_lock, started_changed) = &*notifier_started;
        let mut did_start = lock_poison_tolerant(started_lock);
        while !*did_start {
            did_start = wait_poison_tolerant(started_changed, did_start);
        }
        drop(did_start);

        assert_eq!(
            worker.shutdown(std::time::Duration::ZERO),
            FuseInvalidationShutdownOutcome::TimedOut
        );
        assert_eq!(worker.metrics().shutdown_timeouts, 1);
        assert!(worker.metrics().notification_in_flight);

        let (gate_lock, gate_changed) = &*notifier_gate;
        *lock_poison_tolerant(gate_lock) = true;
        gate_changed.notify_all();
        assert_eq!(
            worker.shutdown(std::time::Duration::from_secs(1)),
            FuseInvalidationShutdownOutcome::Drained
        );
        assert!(!worker.metrics().notification_in_flight);
        drop(open);
    }

    #[test]
    fn notifier_panic_completes_worker_and_is_reported_by_join() {
        let liveness = FuseInodeLiveness::default();
        let open = liveness.open_handle(42);
        let worker = FuseInvalidationWorker::spawn(
            FuseInvalidationQueue::with_capacity(1),
            liveness,
            |_ino, _geometry| -> Result<(), ()> {
                panic!("injected notifier panic");
            },
        )
        .unwrap();
        worker.push(42, FuseInvalidationKind::Data).unwrap();

        assert_eq!(
            worker.shutdown(std::time::Duration::from_secs(1)),
            FuseInvalidationShutdownOutcome::WorkerPanicked
        );
        let metrics = worker.metrics();
        assert_eq!(metrics.worker_panics, 1);
        assert_eq!(metrics.shutdown_timeouts, 0);
        assert!(!metrics.notification_in_flight);
        drop(open);
    }

    #[test]
    fn queue_structurally_rejects_a_second_worker() {
        let queue = FuseInvalidationQueue::with_capacity(1);
        let worker =
            FuseInvalidationWorker::spawn(queue.clone(), FuseInodeLiveness::default(), |_, _| {
                Ok(())
            })
            .unwrap();

        let error =
            match FuseInvalidationWorker::spawn(queue, FuseInodeLiveness::default(), |_, _| Ok(()))
            {
                Ok(_) => panic!("second worker must be rejected"),
                Err(error) => error,
            };

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(
            worker.shutdown(std::time::Duration::from_secs(1)),
            FuseInvalidationShutdownOutcome::Drained
        );
    }
}

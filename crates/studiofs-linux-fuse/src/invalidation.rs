use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

const ROOT_INODE: u64 = 1;
const DEFAULT_INVALIDATION_QUEUE_CAPACITY: usize = 8192;

#[derive(Default)]
struct FuseInodeLivenessState {
    lookup_refs: HashMap<u64, u64>,
    open_refs: HashMap<u64, u64>,
    reservations: Vec<PendingLookup>,
}

struct PendingLookup {
    ino: u64,
    identity: Arc<()>,
    invalidated_during_reply: bool,
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
        subtract_ref_count(&mut state.lookup_refs, ino, nlookup);
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
    /// Commit a successful entry reply and report whether it needs a follow-up
    /// invalidation because content changed while the reply was in flight.
    pub(crate) fn commit(&self) -> Option<FuseLookupCommitOutcome> {
        let mut state = lock_poison_tolerant(&self.inner.state);
        let Some(index) = state.reservations.iter().position(|reservation| {
            reservation.ino == self.ino && Arc::ptr_eq(&reservation.identity, &self.identity)
        }) else {
            return None;
        };
        let reservation = state.reservations.swap_remove(index);
        increment_ref_count(&mut state.lookup_refs, self.ino);
        Some(FuseLookupCommitOutcome {
            follow_up_invalidation_required: reservation.invalidated_during_reply,
        })
    }

    pub(crate) fn rollback(&self) -> bool {
        let mut state = lock_poison_tolerant(&self.inner.state);
        let Some(index) = state.reservations.iter().position(|reservation| {
            reservation.ino == self.ino && Arc::ptr_eq(&reservation.identity, &self.identity)
        }) else {
            return false;
        };
        state.reservations.swap_remove(index);
        true
    }
}

/// Outcome of committing one successful kernel entry reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FuseLookupCommitOutcome {
    follow_up_invalidation_required: bool,
}

impl FuseLookupCommitOutcome {
    pub(crate) const fn follow_up_invalidation_required(self) -> bool {
        self.follow_up_invalidation_required
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

fn subtract_ref_count(refs: &mut HashMap<u64, u64>, ino: u64, count: u64) {
    let Some(current) = refs.get_mut(&ino) else {
        return;
    };
    *current = current.saturating_sub(count);
    if *current == 0 {
        refs.remove(&ino);
    }
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
                return Ok(());
            }
            if state.pending.len() < self.inner.capacity {
                state.order.push_back(ino);
                state.pending.insert(ino, kind);
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
        assert!(!current.commit().unwrap().follow_up_invalidation_required());
        assert!(liveness.is_live(42));
    }

    #[test]
    fn old_pending_reservation_rollback_does_not_remove_committed_lookup() {
        let liveness = FuseInodeLiveness::default();
        let old = liveness.reserve_lookup(42);
        let current = liveness.reserve_lookup(42);
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
        assert!(liveness.reserve_lookup(42).commit().is_some());
        assert!(liveness.reserve_lookup(42).commit().is_some());

        liveness.forget(42, 9);

        assert!(!liveness.is_live(42));
    }

    #[test]
    fn forget_zero_is_a_noop() {
        let liveness = FuseInodeLiveness::default();
        assert!(liveness.reserve_lookup(42).commit().is_some());

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
        assert!(liveness.reserve_lookup(42).commit().is_some());
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
        assert!(liveness.reserve_lookup(42).commit().is_some());
        let pending = liveness.reserve_lookup(42);
        let callback_called = AtomicBool::new(false);

        let admission = liveness.admit_invalidation(42, || {
            callback_called.store(true, Ordering::Release);
        });

        assert!(admission.notified_now());
        assert_eq!(admission.deferred_reply_count(), 1);
        assert!(callback_called.load(Ordering::Acquire));
        assert!(pending.commit().unwrap().follow_up_invalidation_required());
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
}

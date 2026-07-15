use std::sync::Arc;

use dashmap::{DashMap, mapref::entry::Entry};
use parking_lot::Mutex;
use studiofs_meta::schema::ROOT_INO;
use studiofs_vfs::KernelInvalidator;

/// Monotonic lookup-liveness operation counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FuseInodeLivenessStats {
    /// Changes whenever one of the counters changes.
    pub snapshot_generation: u64,
    /// Lookup references added by observations.
    pub observed_references: u64,
    /// Lookup references retired by kernel forgets.
    pub forgotten_references: u64,
    /// Live inodes explicitly retired after final removal.
    pub retired_inodes: u64,
    /// Open replies attempted for inodes without a lookup reference.
    pub open_unknown_inode: u64,
    /// Notifications forwarded for observed inodes.
    pub notifications_forwarded: u64,
    /// Notifications suppressed for unknown inodes.
    pub notifications_suppressed: u64,
}

/// Tracks kernel lookup references for one FUSE session.
#[derive(Debug)]
pub struct FuseInodeLiveness {
    observed: DashMap<u64, Arc<Mutex<u64>>>,
    stats: Mutex<FuseInodeLivenessStats>,
}

/// One lookup reference reserved while libfuse accepts or rejects an entry reply.
#[must_use = "lookup reply reservations must be accepted or rejected"]
pub struct FuseLookupReplyReservation<'a> {
    liveness: &'a FuseInodeLiveness,
    ino: u64,
    state: Arc<Mutex<u64>>,
    reserved: bool,
}

impl FuseLookupReplyReservation<'_> {
    /// Commit an accepted reply to the observed-reference counters.
    pub fn accept(mut self) {
        if self.reserved {
            self.reserved = false;
            self.liveness.record(|stats| {
                stats.observed_references = stats.observed_references.saturating_add(1);
            });
        }
    }

    /// Remove a rejected reply reservation without recording a kernel forget.
    pub fn reject(mut self) {
        self.rollback();
    }

    fn rollback(&mut self) {
        if !self.reserved {
            return;
        }
        self.reserved = false;

        let mut count = self.state.lock();
        if !self.liveness.is_current(self.ino, &self.state) || *count == 0 {
            return;
        }
        *count -= 1;
        if *count == 0 {
            self.liveness
                .observed
                .remove_if(&self.ino, |_, current| Arc::ptr_eq(current, &self.state));
        }
    }
}

impl Drop for FuseLookupReplyReservation<'_> {
    fn drop(&mut self) {
        self.rollback();
    }
}

impl Default for FuseInodeLiveness {
    fn default() -> Self {
        let observed = DashMap::new();
        observed.insert(ROOT_INO, Arc::new(Mutex::new(u64::MAX)));
        Self {
            observed,
            stats: Mutex::new(FuseInodeLivenessStats::default()),
        }
    }
}

impl FuseInodeLiveness {
    fn current(&self, ino: u64) -> Option<Arc<Mutex<u64>>> {
        self.observed
            .get(&ino)
            .map(|entry| Arc::clone(entry.value()))
    }

    fn is_current(&self, ino: u64, state: &Arc<Mutex<u64>>) -> bool {
        self.observed
            .get(&ino)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), state))
    }

    fn current_or_insert(&self, ino: u64) -> Arc<Mutex<u64>> {
        match self.observed.entry(ino) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let state = Arc::new(Mutex::new(0));
                entry.insert(Arc::clone(&state));
                state
            }
        }
    }

    fn record(&self, update: impl FnOnce(&mut FuseInodeLivenessStats)) {
        let mut stats = self.stats.lock();
        update(&mut stats);
        stats.snapshot_generation = stats.snapshot_generation.saturating_add(1);
    }

    /// Count one kernel lookup reference.
    pub fn observe(&self, ino: u64) {
        self.reserve_lookup_reply(ino).accept();
    }

    /// Reserve one lookup reference until an entry reply is accepted or rejected.
    pub fn reserve_lookup_reply(&self, ino: u64) -> FuseLookupReplyReservation<'_> {
        loop {
            let state = self.current_or_insert(ino);
            let mut count = state.lock();
            if self.is_current(ino, &state) {
                let previous = *count;
                *count = count.saturating_add(1);
                let reserved = *count != previous;
                drop(count);
                return FuseLookupReplyReservation {
                    liveness: self,
                    ino,
                    state,
                    reserved,
                };
            }
        }
    }

    /// Ensure at least one lookup reference, returning whether this call added it.
    pub fn ensure_observed(&self, ino: u64) -> bool {
        loop {
            let state = self.current_or_insert(ino);
            let mut count = state.lock();
            if self.is_current(ino, &state) {
                if *count > 0 {
                    return false;
                }
                *count = 1;
                drop(count);
                self.record(|stats| {
                    stats.observed_references = stats.observed_references.saturating_add(1);
                });
                return true;
            }
        }
    }

    /// Retire every reference after final inode removal.
    pub fn close(&self, ino: u64) {
        if ino == ROOT_INO {
            return;
        }
        loop {
            let Some(state) = self.current(ino) else {
                return;
            };
            let mut count = state.lock();
            if !self.is_current(ino, &state) {
                continue;
            }
            let retired = *count > 0;
            *count = 0;
            self.observed
                .remove_if(&ino, |_, current| Arc::ptr_eq(current, &state));
            drop(count);
            if retired {
                self.record(|stats| {
                    stats.retired_inodes = stats.retired_inodes.saturating_add(1);
                });
            }
            return;
        }
    }

    /// Retire lookup references supplied by the kernel.
    pub fn forget(&self, ino: u64, nlookup: u64) {
        if ino == ROOT_INO || nlookup == 0 {
            return;
        }
        loop {
            let Some(state) = self.current(ino) else {
                return;
            };
            let mut count = state.lock();
            if !self.is_current(ino, &state) {
                continue;
            }
            let forgotten = (*count).min(nlookup);
            *count -= forgotten;
            if *count == 0 {
                self.observed
                    .remove_if(&ino, |_, current| Arc::ptr_eq(current, &state));
            }
            drop(count);
            if forgotten > 0 {
                self.record(|stats| {
                    stats.forgotten_references =
                        stats.forgotten_references.saturating_add(forgotten);
                });
            }
            return;
        }
    }

    /// Test liveness without changing the lookup reference count.
    pub fn is_observed(&self, ino: u64) -> bool {
        loop {
            let Some(state) = self.current(ino) else {
                return false;
            };
            let count = state.lock();
            if self.is_current(ino, &state) {
                return *count > 0;
            }
        }
    }

    /// Record an open reply for an inode without a lookup reference.
    pub fn record_open_unknown_inode(&self) -> u64 {
        let mut stats = self.stats.lock();
        stats.open_unknown_inode = stats.open_unknown_inode.saturating_add(1);
        stats.snapshot_generation = stats.snapshot_generation.saturating_add(1);
        stats.open_unknown_inode
    }

    /// Invoke `notify` only while the inode is still observed.
    pub fn notify_if_observed(&self, ino: u64, notify: impl FnOnce()) -> bool {
        let should_notify = self.current(ino).is_some_and(|state| {
            let count = state.lock();
            self.is_current(ino, &state) && *count > 0
        });
        if should_notify {
            self.record(|stats| {
                stats.notifications_forwarded = stats.notifications_forwarded.saturating_add(1);
            });
            notify();
        } else {
            self.record(|stats| {
                stats.notifications_suppressed = stats.notifications_suppressed.saturating_add(1);
            });
        }
        should_notify
    }

    /// Return a monotonic snapshot of liveness operations.
    pub fn stats(&self) -> FuseInodeLivenessStats {
        *self.stats.lock()
    }
}

/// Gates kernel invalidation callbacks on the session's lookup liveness.
pub struct LivenessCheckedKernelInvalidator {
    liveness: Arc<FuseInodeLiveness>,
    inner: Arc<dyn KernelInvalidator>,
}

impl LivenessCheckedKernelInvalidator {
    /// Create a liveness-gated invalidator.
    pub fn new(liveness: Arc<FuseInodeLiveness>, inner: Arc<dyn KernelInvalidator>) -> Self {
        Self { liveness, inner }
    }
}

impl KernelInvalidator for LivenessCheckedKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.liveness
            .notify_if_observed(ino, || self.inner.inval_inode(ino));
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.liveness
            .notify_if_observed(ino, || self.inner.inval_inode_attrs(ino));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::time::Duration;

    use studiofs_meta::schema::ROOT_INO;

    use super::{FuseInodeLiveness, FuseInodeLivenessStats};

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[test]
    fn lookup_references_close_only_after_final_forget() {
        let liveness = FuseInodeLiveness::default();
        let ino = 42;
        liveness.observe(ino);
        liveness.observe(ino);

        liveness.forget(ino, 1);
        assert!(liveness.is_observed(ino));

        liveness.forget(ino, 1);
        assert!(!liveness.is_observed(ino));
    }

    #[test]
    fn zero_nlookup_is_a_noop() {
        let liveness = FuseInodeLiveness::default();
        liveness.observe(42);

        liveness.forget(42, 0);

        assert!(liveness.is_observed(42));
    }

    #[test]
    fn root_inode_is_always_observed() {
        let liveness = FuseInodeLiveness::default();

        liveness.forget(ROOT_INO, u64::MAX);
        liveness.close(ROOT_INO);

        assert!(liveness.is_observed(ROOT_INO));
    }

    #[test]
    fn ensure_observed_reports_only_a_new_reference() {
        let liveness = FuseInodeLiveness::default();

        assert!(liveness.ensure_observed(42));
        assert!(!liveness.ensure_observed(42));
    }

    #[test]
    fn is_observed_never_creates_a_reference() {
        let liveness = FuseInodeLiveness::default();

        assert!(!liveness.is_observed(42));
        assert_eq!(liveness.stats(), FuseInodeLivenessStats::default());
    }

    #[test]
    fn lookup_reply_reservation_is_visible_before_completion_without_recording_stats() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup_reply(42);

        assert_eq!(
            (liveness.is_observed(42), liveness.stats()),
            (true, FuseInodeLivenessStats::default())
        );

        reservation.reject();
    }

    #[test]
    fn accepted_lookup_reply_reservation_records_one_observed_reference() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup_reply(42);

        reservation.accept();

        assert_eq!(
            (liveness.is_observed(42), liveness.stats()),
            (
                true,
                FuseInodeLivenessStats {
                    snapshot_generation: 1,
                    observed_references: 1,
                    ..Default::default()
                }
            )
        );
    }

    #[test]
    fn rejected_lookup_reply_reservation_removes_membership_without_recording_stats() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup_reply(42);

        reservation.reject();

        assert_eq!(
            (liveness.is_observed(42), liveness.stats()),
            (false, FuseInodeLivenessStats::default())
        );
    }

    #[test]
    fn accepted_lookup_reply_reservation_records_only_an_actual_count_change() {
        let liveness = FuseInodeLiveness::default();
        let saturated = liveness.current_or_insert(42);
        *saturated.lock() = u64::MAX;

        liveness.reserve_lookup_reply(ROOT_INO).accept();
        liveness.reserve_lookup_reply(42).accept();

        assert_eq!(liveness.stats(), FuseInodeLivenessStats::default());
    }

    #[test]
    fn rejected_stale_lookup_reply_reservation_does_not_remove_new_membership() {
        let liveness = FuseInodeLiveness::default();
        let reservation = liveness.reserve_lookup_reply(42);
        liveness.close(42);
        liveness.observe(42);
        let before_rejection = liveness.stats();

        reservation.reject();

        assert_eq!(
            (liveness.is_observed(42), liveness.stats()),
            (true, before_rejection)
        );
    }

    #[test]
    fn open_unknown_inode_counter_shares_snapshot_generation() {
        let liveness = FuseInodeLiveness::default();

        liveness.record_open_unknown_inode();
        liveness.record_open_unknown_inode();

        assert_eq!(
            liveness.stats(),
            FuseInodeLivenessStats {
                snapshot_generation: 2,
                open_unknown_inode: 2,
                ..Default::default()
            }
        );
    }

    #[test]
    fn unknown_inode_suppresses_notification() {
        let liveness = FuseInodeLiveness::default();
        let mut called = false;

        let forwarded = liveness.notify_if_observed(42, || called = true);

        assert!(!forwarded);
        assert!(!called);
    }

    #[test]
    fn hardlink_lookup_references_accumulate_by_inode() {
        let liveness = FuseInodeLiveness::default();
        liveness.observe(42);
        liveness.observe(42);

        liveness.forget(42, 1);

        assert!(liveness.is_observed(42));
        assert_eq!(liveness.stats().observed_references, 2);
    }

    #[test]
    fn close_does_not_wait_for_external_notification() {
        let liveness = Arc::new(FuseInodeLiveness::default());
        let ino = 42;
        liveness.observe(ino);
        let entered_notification = Arc::new(Barrier::new(2));
        let release_notification = Arc::new(Barrier::new(2));
        let notifier_liveness = Arc::clone(&liveness);
        let notifier_entered = Arc::clone(&entered_notification);
        let notifier_release = Arc::clone(&release_notification);
        let notifier = std::thread::spawn(move || {
            notifier_liveness.notify_if_observed(ino, || {
                notifier_entered.wait();
                notifier_release.wait();
            });
        });
        entered_notification.wait();

        let closer_liveness = Arc::clone(&liveness);
        let (closed_tx, closed_rx) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            closer_liveness.close(ino);
            closed_tx.send(()).expect("signal closed gate");
        });
        closed_rx
            .recv_timeout(TIMEOUT)
            .expect("close must not wait for the external callback");

        release_notification.wait();
        notifier.join().expect("notification thread exits");
        closer.join().expect("close thread exits");
        assert!(!liveness.is_observed(ino));
    }

    #[test]
    fn notification_can_reenter_close() {
        let liveness = Arc::new(FuseInodeLiveness::default());
        let ino = 42;
        liveness.observe(ino);
        let callback_liveness = Arc::clone(&liveness);
        let (closed_tx, closed_rx) = mpsc::channel();
        let notifier = std::thread::spawn(move || {
            callback_liveness.notify_if_observed(ino, || {
                callback_liveness.close(ino);
                closed_tx.send(()).expect("signal re-entrant close");
            });
        });

        closed_rx
            .recv_timeout(TIMEOUT)
            .expect("notification callback must not deadlock on re-entry");
        notifier.join().expect("notification thread exits");
        assert!(!liveness.is_observed(ino));
    }

    #[test]
    fn notification_does_not_block_unrelated_inode_updates() {
        let liveness = Arc::new(FuseInodeLiveness::default());
        let notifying_ino = 42;
        liveness.observe(notifying_ino);
        let entered_notification = Arc::new(Barrier::new(2));
        let release_notification = Arc::new(Barrier::new(2));
        let notifier_liveness = Arc::clone(&liveness);
        let notifier_entered = Arc::clone(&entered_notification);
        let notifier_release = Arc::clone(&release_notification);
        let notifier = std::thread::spawn(move || {
            notifier_liveness.notify_if_observed(notifying_ino, || {
                notifier_entered.wait();
                notifier_release.wait();
            });
        });
        entered_notification.wait();

        let updater_liveness = Arc::clone(&liveness);
        let (updated_tx, updated_rx) = mpsc::channel();
        let updater = std::thread::spawn(move || {
            for ino in 1_000..5_096 {
                updater_liveness.ensure_observed(ino);
            }
            updated_tx.send(()).expect("signal unrelated updates");
        });
        updated_rx
            .recv_timeout(TIMEOUT)
            .expect("one callback must not block unrelated inode updates");

        release_notification.wait();
        notifier.join().expect("notification thread exits");
        updater.join().expect("unrelated inode updater exits");
    }

    #[test]
    fn liveness_stats_count_observed_forgotten_retired_forwarded_and_suppressed_events() {
        let liveness = FuseInodeLiveness::default();
        liveness.observe(42);
        liveness.observe(42);
        liveness.forget(42, u64::MAX);
        liveness.observe(43);
        liveness.notify_if_observed(43, || {});
        liveness.close(43);
        liveness.notify_if_observed(44, || {});

        assert_eq!(
            liveness.stats(),
            FuseInodeLivenessStats {
                snapshot_generation: 7,
                observed_references: 3,
                forgotten_references: 2,
                retired_inodes: 1,
                open_unknown_inode: 0,
                notifications_forwarded: 1,
                notifications_suppressed: 1,
            }
        );
    }

    #[test]
    fn concurrent_stats_snapshots_publish_generation_and_counter_together() {
        const UPDATES: usize = 200_000;

        let liveness = Arc::new(FuseInodeLiveness::default());
        let start = Arc::new(Barrier::new(3));
        let done = Arc::new(AtomicBool::new(false));

        std::thread::scope(|scope| {
            let mutator_liveness = Arc::clone(&liveness);
            let mutator_start = Arc::clone(&start);
            let mutator_done = Arc::clone(&done);
            scope.spawn(move || {
                mutator_start.wait();
                for _ in 0..UPDATES {
                    mutator_liveness.notify_if_observed(42, || {});
                }
                mutator_done.store(true, Ordering::Release);
            });

            for _ in 0..2 {
                let sampler_liveness = Arc::clone(&liveness);
                let sampler_start = Arc::clone(&start);
                let sampler_done = Arc::clone(&done);
                scope.spawn(move || {
                    sampler_start.wait();
                    let mut previous = FuseInodeLivenessStats::default();
                    while !sampler_done.load(Ordering::Acquire) {
                        let current = sampler_liveness.stats();
                        assert!(current.snapshot_generation >= previous.snapshot_generation);
                        assert!(
                            current.notifications_suppressed >= previous.notifications_suppressed
                        );
                        assert_eq!(
                            current.snapshot_generation, current.notifications_suppressed,
                            "one suppressed notification must publish one complete stats update"
                        );
                        previous = current;
                    }
                });
            }
        });

        let final_stats = liveness.stats();
        assert_eq!(final_stats.snapshot_generation, UPDATES as u64);
        assert_eq!(final_stats.notifications_suppressed, UPDATES as u64);
    }
}

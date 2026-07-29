//! Keyed serialization for FUSE open/create reply transactions.
//!
//! A bridge must acquire the guard before preparing an OS-cache decision and
//! keep it until `fuse_reply_open`/`fuse_reply_create` has accepted the reply.
//! If the reply is rejected, cache preparation must be aborted and the VFS
//! handle released before dropping the guard. This ensures that a later open
//! of the same inode cannot observe state left by a rejected reply:
//!
//! ```text
//! let _guard = serializer.lock(ino);
//! let decision = vfs.prepare_os_cache_open(fh, participation)?;
//! if fuse_reply_open(req, decision.flags()) != 0 {
//!     vfs.abort_os_cache_open(fh);
//!     vfs.release_fh(fh);
//! }
//! ```
//!
//! Inodes are independent, while waiters for one inode acquire guards in FIFO
//! ticket order. Dropping a guard (including during unwinding) advances the
//! queue and idle inode state is removed.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

/// Serializes the cache-decision/reply transaction independently per inode.
#[derive(Clone, Debug, Default)]
pub struct FuseOpenReplySerializer {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct State {
    inodes: HashMap<u64, InodeQueue>,
}

#[derive(Debug, Default)]
struct InodeQueue {
    next_ticket: u64,
    serving_ticket: u64,
    held: bool,
}

/// Exclusive ownership of one inode's cache-decision/reply transaction.
///
/// The guard releases its ticket on drop and can safely cross thread
/// boundaries with the transaction it protects.
#[derive(Debug)]
#[must_use = "dropping the guard ends the serialized FUSE open-reply transaction"]
pub struct FuseOpenReplyGuard {
    inner: Arc<Inner>,
    ino: u64,
}

impl FuseOpenReplySerializer {
    /// Creates an empty serializer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Blocks until this caller owns the next FIFO ticket for `ino`.
    ///
    /// Acquire this before VFS cache preparation. Keep the returned guard
    /// through reply acceptance or, for a rejected reply, through cache
    /// rollback and VFS handle release.
    pub fn lock(&self, ino: u64) -> FuseOpenReplyGuard {
        let mut state = lock_recover(&self.inner.state);
        let ticket = {
            let queue = state.inodes.entry(ino).or_default();
            let ticket = queue.next_ticket;
            queue.next_ticket = queue
                .next_ticket
                .checked_add(1)
                .expect("active FUSE open-reply ticket count overflowed");
            ticket
        };
        self.inner.changed.notify_all();

        loop {
            let queue = state
                .inodes
                .get_mut(&ino)
                .expect("inode queue remains present while a ticket is waiting");
            if !queue.held && queue.serving_ticket == ticket {
                queue.held = true;
                return FuseOpenReplyGuard {
                    inner: Arc::clone(&self.inner),
                    ino,
                };
            }
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    #[cfg(test)]
    fn tracked_inode_count(&self) -> usize {
        lock_recover(&self.inner.state).inodes.len()
    }

    #[cfg(test)]
    fn wait_for_queued_waiters(&self, ino: u64, expected: u64) {
        use std::time::{Duration, Instant};

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut state = lock_recover(&self.inner.state);
        loop {
            let queued = state.inodes.get(&ino).map_or(0, |queue| {
                queue
                    .next_ticket
                    .saturating_sub(queue.serving_ticket)
                    .saturating_sub(u64::from(queue.held))
            });
            if queued >= expected {
                return;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for {expected} queued inode waiters; observed {queued}"
            );
            let (next_state, timeout) = self
                .inner
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
            assert!(
                !timeout.timed_out(),
                "timed out waiting for {expected} queued inode waiters"
            );
        }
    }
}

impl Drop for FuseOpenReplyGuard {
    fn drop(&mut self) {
        let mut state = lock_recover(&self.inner.state);
        let remove = {
            let queue = state
                .inodes
                .get_mut(&self.ino)
                .expect("held inode queue remains present until guard drop");
            debug_assert!(queue.held);
            queue.held = false;
            queue.serving_ticket = queue
                .serving_ticket
                .checked_add(1)
                .expect("active FUSE open-reply ticket count overflowed");
            queue.serving_ticket == queue.next_ticket
        };
        if remove {
            state.inodes.remove(&self.ino);
        }
        self.inner.changed.notify_all();
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::FuseOpenReplySerializer;
    use std::sync::{Arc, Barrier, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    const INO: u64 = 42;

    #[test]
    fn same_inode_waiters_acquire_in_ticket_order() {
        let serializer = Arc::new(FuseOpenReplySerializer::new());
        let first = serializer.lock(INO);
        let (tx, rx) = mpsc::channel();

        let second_serializer = Arc::clone(&serializer);
        let second_tx = tx.clone();
        let second = thread::spawn(move || {
            let _guard = second_serializer.lock(INO);
            second_tx.send(2).unwrap();
        });
        serializer.wait_for_queued_waiters(INO, 1);

        let third_serializer = Arc::clone(&serializer);
        let third = thread::spawn(move || {
            let _guard = third_serializer.lock(INO);
            tx.send(3).unwrap();
        });
        serializer.wait_for_queued_waiters(INO, 2);

        assert!(rx.try_recv().is_err());
        drop(first);
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 3);

        second.join().unwrap();
        third.join().unwrap();
    }

    #[test]
    fn different_inodes_proceed_independently() {
        let serializer = Arc::new(FuseOpenReplySerializer::new());
        let _first_inode = serializer.lock(INO);
        let (tx, rx) = mpsc::channel();

        let serializer = Arc::clone(&serializer);
        let worker = thread::spawn(move || {
            let _other_inode = serializer.lock(INO + 1);
            tx.send(()).unwrap();
        });

        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn dropping_guard_unblocks_waiter() {
        let serializer = Arc::new(FuseOpenReplySerializer::new());
        let guard = serializer.lock(INO);
        let (tx, rx) = mpsc::channel();

        let waiter_serializer = Arc::clone(&serializer);
        let waiter = thread::spawn(move || {
            let _guard = waiter_serializer.lock(INO);
            tx.send(()).unwrap();
        });
        serializer.wait_for_queued_waiters(INO, 1);

        assert!(rx.try_recv().is_err());
        drop(guard);
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        waiter.join().unwrap();
    }

    #[test]
    fn unwinding_holder_releases_inode() {
        let serializer = Arc::new(FuseOpenReplySerializer::new());
        let panicking_serializer = Arc::clone(&serializer);

        let panicking = thread::spawn(move || {
            let _guard = panicking_serializer.lock(INO);
            panic!("model callback panic");
        });
        assert!(panicking.join().is_err());

        let _next = serializer.lock(INO);
    }

    #[test]
    fn poisoned_state_lock_is_recovered() {
        let serializer = Arc::new(FuseOpenReplySerializer::new());
        let poisoning_serializer = Arc::clone(&serializer);

        let poisoning = thread::spawn(move || {
            let _state = poisoning_serializer.inner.state.lock().unwrap();
            panic!("poison serializer state");
        });
        assert!(poisoning.join().is_err());

        let _guard = serializer.lock(INO);
    }

    #[test]
    fn idle_inode_state_is_removed() {
        let serializer = FuseOpenReplySerializer::new();

        {
            let _guard = serializer.lock(INO);
            assert_eq!(serializer.tracked_inode_count(), 1);
        }

        assert_eq!(serializer.tracked_inode_count(), 0);
    }

    #[test]
    fn rejected_reply_rolls_back_before_next_prepare() {
        let serializer = Arc::new(FuseOpenReplySerializer::new());
        let events = Arc::new(Mutex::new(Vec::new()));
        let first = serializer.lock(INO);
        let admitted = Arc::new(Barrier::new(2));

        let waiter_serializer = Arc::clone(&serializer);
        let waiter_events = Arc::clone(&events);
        let waiter_admitted = Arc::clone(&admitted);
        let waiter = thread::spawn(move || {
            waiter_admitted.wait();
            let _guard = waiter_serializer.lock(INO);
            waiter_events.lock().unwrap().push("second prepare");
        });
        admitted.wait();
        serializer.wait_for_queued_waiters(INO, 1);

        // Models a rejected fuse_reply_open/create: rollback and VFS release
        // happen while the per-inode guard is still held.
        events.lock().unwrap().push("first rollback");
        drop(first);
        waiter.join().unwrap();

        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["first rollback", "second prepare"]
        );
    }
}

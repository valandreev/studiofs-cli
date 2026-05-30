#![allow(dead_code)]
// This module is introduced ahead of the async FUSE write callbacks. It is
// intentionally unit-tested in isolation before the bridge starts using it.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Condvar, Mutex};

type Ino = u64;
type FileHandle = u64;
type Sequence = u64;
type HandleKey = (Ino, FileHandle);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteRegistrationError {
    HandleClosing,
    HandleClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingWriteCompletion {
    Success,
    Error(i32),
    TaskFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandleLifecycle {
    Open,
    Closing,
    Closed,
}

#[derive(Debug)]
struct HandleState {
    next_sequence: Sequence,
    lifecycle: HandleLifecycle,
    pending: BTreeSet<Sequence>,
    sticky_error: Option<i32>,
}

impl Default for HandleState {
    fn default() -> Self {
        Self {
            next_sequence: 1,
            lifecycle: HandleLifecycle::Open,
            pending: BTreeSet::new(),
            sticky_error: None,
        }
    }
}

#[derive(Debug, Default)]
struct TrackerState {
    handles: HashMap<HandleKey, HandleState>,
}

#[derive(Debug, Default)]
pub(super) struct PendingWriteTracker {
    shared: Arc<SharedTracker>,
}

#[derive(Debug, Default)]
struct SharedTracker {
    state: Mutex<TrackerState>,
    writes_changed: Condvar,
}

#[derive(Debug)]
pub(super) struct PendingWriteRegistration {
    shared: Arc<SharedTracker>,
    key: HandleKey,
    sequence: Sequence,
    completion: Mutex<Option<PendingWriteCompletion>>,
}

#[derive(Debug)]
pub(super) struct PendingWriteBarrier {
    shared: Arc<SharedTracker>,
    targets: Vec<(HandleKey, Sequence)>,
}

impl PendingWriteTracker {
    pub(super) fn register(
        &self,
        ino: Ino,
        fh: FileHandle,
    ) -> Result<PendingWriteRegistration, WriteRegistrationError> {
        let key = (ino, fh);
        let mut state = self
            .shared
            .state
            .lock()
            .expect("pending write tracker poisoned");
        let handle = state.handles.entry(key).or_default();
        match handle.lifecycle {
            HandleLifecycle::Open => {}
            HandleLifecycle::Closing => return Err(WriteRegistrationError::HandleClosing),
            HandleLifecycle::Closed => return Err(WriteRegistrationError::HandleClosed),
        }

        let sequence = handle.next_sequence;
        handle.next_sequence += 1;
        handle.pending.insert(sequence);

        Ok(PendingWriteRegistration {
            shared: Arc::clone(&self.shared),
            key,
            sequence,
            completion: Mutex::new(None),
        })
    }

    pub(super) fn handle_barrier(&self, ino: Ino, fh: FileHandle) -> PendingWriteBarrier {
        self.barrier_for_targets(vec![((ino, fh), self.last_registered_sequence((ino, fh)))])
    }

    pub(super) fn inode_barrier(&self, ino: Ino) -> PendingWriteBarrier {
        let state = self
            .shared
            .state
            .lock()
            .expect("pending write tracker poisoned");
        let targets = state
            .handles
            .iter()
            .filter_map(|(&key @ (handle_ino, _), handle)| {
                (handle_ino == ino).then_some((key, handle.next_sequence.saturating_sub(1)))
            })
            .collect();
        drop(state);
        self.barrier_for_targets(targets)
    }

    pub(super) fn begin_close(
        &self,
        ino: Ino,
        fh: FileHandle,
    ) -> Result<PendingWriteBarrier, WriteRegistrationError> {
        let key = (ino, fh);
        let mut state = self
            .shared
            .state
            .lock()
            .expect("pending write tracker poisoned");
        let handle = state.handles.entry(key).or_default();
        match handle.lifecycle {
            HandleLifecycle::Open => handle.lifecycle = HandleLifecycle::Closing,
            HandleLifecycle::Closing => return Err(WriteRegistrationError::HandleClosing),
            HandleLifecycle::Closed => return Err(WriteRegistrationError::HandleClosed),
        }
        let target = handle.next_sequence.saturating_sub(1);
        drop(state);
        Ok(self.barrier_for_targets(vec![(key, target)]))
    }

    pub(super) fn finish_close(&self, ino: Ino, fh: FileHandle) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("pending write tracker poisoned");
        let handle = state.handles.entry((ino, fh)).or_default();
        handle.lifecycle = HandleLifecycle::Closed;
        self.shared.writes_changed.notify_all();
    }

    fn last_registered_sequence(&self, key: HandleKey) -> Sequence {
        let state = self
            .shared
            .state
            .lock()
            .expect("pending write tracker poisoned");
        state
            .handles
            .get(&key)
            .map_or(0, |handle| handle.next_sequence.saturating_sub(1))
    }

    fn barrier_for_targets(&self, targets: Vec<(HandleKey, Sequence)>) -> PendingWriteBarrier {
        PendingWriteBarrier {
            shared: Arc::clone(&self.shared),
            targets,
        }
    }
}

impl PendingWriteRegistration {
    pub(super) fn sequence(&self) -> Sequence {
        self.sequence
    }

    pub(super) fn complete(&self, completion: PendingWriteCompletion) {
        let mut stored = self
            .completion
            .lock()
            .expect("pending write completion poisoned");
        if stored.is_some() {
            return;
        }
        *stored = Some(completion);
        drop(stored);

        let mut state = self
            .shared
            .state
            .lock()
            .expect("pending write tracker poisoned");
        if let Some(handle) = state.handles.get_mut(&self.key) {
            handle.pending.remove(&self.sequence);
            match completion {
                PendingWriteCompletion::Success => {}
                PendingWriteCompletion::Error(errno) => handle.sticky_error = Some(errno),
                PendingWriteCompletion::TaskFailed => handle.sticky_error = Some(libc::EIO),
            }
        }
        self.shared.writes_changed.notify_all();
    }
}

impl Drop for PendingWriteRegistration {
    fn drop(&mut self) {
        let has_completion = self
            .completion
            .lock()
            .expect("pending write completion poisoned")
            .is_some();
        if !has_completion {
            self.complete(PendingWriteCompletion::TaskFailed);
        }
    }
}

impl PendingWriteBarrier {
    pub(super) fn is_ready(&self) -> bool {
        let state = self
            .shared
            .state
            .lock()
            .expect("pending write tracker poisoned");
        self.targets.iter().all(|(key, target)| {
            state
                .handles
                .get(key)
                .is_none_or(|handle| !handle.pending.iter().any(|sequence| sequence <= target))
        })
    }

    pub(super) fn wait(&self) -> Result<(), i32> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("pending write tracker poisoned");
        while !barrier_ready(&state, &self.targets) {
            state = self
                .shared
                .writes_changed
                .wait(state)
                .expect("pending write tracker poisoned");
        }
        barrier_error(&state, &self.targets).map_or(Ok(()), Err)
    }
}

fn barrier_ready(state: &TrackerState, targets: &[(HandleKey, Sequence)]) -> bool {
    targets.iter().all(|(key, target)| {
        state
            .handles
            .get(key)
            .is_none_or(|handle| !handle.pending.iter().any(|sequence| sequence <= target))
    })
}

fn barrier_error(state: &TrackerState, targets: &[(HandleKey, Sequence)]) -> Option<i32> {
    targets.iter().find_map(|(key, _)| {
        state
            .handles
            .get(key)
            .and_then(|handle| handle.sticky_error)
    })
}

#[cfg(test)]
mod tests {
    use super::{PendingWriteCompletion, PendingWriteTracker, WriteRegistrationError};

    const INO: u64 = 42;
    const FIRST_FH: u64 = 7;
    const SECOND_FH: u64 = 8;

    #[test]
    fn registrations_return_monotonic_sequences_per_handle() {
        let tracker = PendingWriteTracker::default();

        let first = tracker
            .register(INO, FIRST_FH)
            .expect("register first write");
        let second = tracker
            .register(INO, FIRST_FH)
            .expect("register second write");
        let other_handle = tracker
            .register(INO, SECOND_FH)
            .expect("register other handle write");

        assert_eq!(first.sequence(), 1);
        assert_eq!(second.sequence(), 2);
        assert_eq!(other_handle.sequence(), 1);
    }

    #[test]
    fn handle_barrier_waits_for_prior_writes_but_not_later_writes() {
        let tracker = PendingWriteTracker::default();
        let first = tracker
            .register(INO, FIRST_FH)
            .expect("register first write");
        let barrier = tracker.handle_barrier(INO, FIRST_FH);
        let later = tracker
            .register(INO, FIRST_FH)
            .expect("register later write after barrier");

        assert!(!barrier.is_ready());
        later.complete(PendingWriteCompletion::Success);
        assert!(!barrier.is_ready());
        first.complete(PendingWriteCompletion::Success);

        assert!(barrier.is_ready());
        barrier
            .wait()
            .expect("prior writes should complete cleanly");
    }

    #[test]
    fn out_of_order_completions_unblock_only_after_all_prior_writes_complete() {
        let tracker = PendingWriteTracker::default();
        let first = tracker
            .register(INO, FIRST_FH)
            .expect("register first write");
        let second = tracker
            .register(INO, FIRST_FH)
            .expect("register second write");
        let barrier = tracker.handle_barrier(INO, FIRST_FH);

        second.complete(PendingWriteCompletion::Success);
        assert!(!barrier.is_ready());
        first.complete(PendingWriteCompletion::Success);

        assert!(barrier.is_ready());
    }

    #[test]
    fn closing_handle_rejects_new_registrations_until_closed() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.begin_close(INO, FIRST_FH).expect("begin close");

        let error = tracker
            .register(INO, FIRST_FH)
            .expect_err("closing handle should reject writes");
        assert_eq!(error, WriteRegistrationError::HandleClosing);

        pending.complete(PendingWriteCompletion::Success);
        barrier.wait().expect("close barrier should complete");
        tracker.finish_close(INO, FIRST_FH);

        let error = tracker
            .register(INO, FIRST_FH)
            .expect_err("closed handle should reject writes");
        assert_eq!(error, WriteRegistrationError::HandleClosed);
    }

    #[test]
    fn inode_barrier_waits_across_handles_for_same_inode() {
        let tracker = PendingWriteTracker::default();
        let first = tracker
            .register(INO, FIRST_FH)
            .expect("register first write");
        let second = tracker
            .register(INO, SECOND_FH)
            .expect("register second write");
        let barrier = tracker.inode_barrier(INO);

        first.complete(PendingWriteCompletion::Success);
        assert!(!barrier.is_ready());
        second.complete(PendingWriteCompletion::Success);

        barrier
            .wait()
            .expect("inode barrier should wait for all prior writes");
    }

    #[test]
    fn sticky_write_errors_are_returned_by_later_barriers() {
        let tracker = PendingWriteTracker::default();
        let write = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.handle_barrier(INO, FIRST_FH);

        write.complete(PendingWriteCompletion::Error(libc::EIO));

        assert_eq!(barrier.wait(), Err(libc::EIO));
        assert_eq!(
            tracker.handle_barrier(INO, FIRST_FH).wait(),
            Err(libc::EIO),
            "later barriers should surface sticky write failure"
        );
    }

    #[test]
    fn task_failures_mark_writes_complete_and_wake_waiters() {
        let tracker = PendingWriteTracker::default();
        let write = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.handle_barrier(INO, FIRST_FH);

        write.complete(PendingWriteCompletion::TaskFailed);

        assert!(barrier.is_ready());
        assert_eq!(barrier.wait(), Err(libc::EIO));
    }
}

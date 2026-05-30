#![allow(dead_code)]
// This module is introduced ahead of the async FUSE write callbacks. It is
// intentionally unit-tested in isolation before the bridge starts using it.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use smallvec::{SmallVec, smallvec};

type Ino = u64;
type FileHandle = u64;
type Sequence = u64;
type HandleKey = (Ino, FileHandle);
type InodeGeneration = u64;
type BarrierTarget = (HandleKey, Sequence, InodeGeneration);
const TRACKER_SHARDS: usize = 64;

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
    generation: InodeGeneration,
    next_sequence: Sequence,
    lifecycle: HandleLifecycle,
    pending: BTreeSet<Sequence>,
    sticky_error: Option<i32>,
}

impl Default for HandleState {
    fn default() -> Self {
        Self {
            generation: 0,
            next_sequence: 1,
            lifecycle: HandleLifecycle::Open,
            pending: BTreeSet::new(),
            sticky_error: None,
        }
    }
}

#[derive(Debug, Default)]
struct TrackerState {
    inodes: HashMap<Ino, HashMap<FileHandle, HandleState>>,
    inode_generations: HashMap<Ino, InodeGeneration>,
    inode_sticky_errors: HashMap<(Ino, InodeGeneration), i32>,
    barrier_counts: HashMap<(Ino, InodeGeneration), usize>,
}

#[derive(Debug, Default)]
pub(super) struct PendingWriteTracker {
    shared: Arc<SharedTracker>,
}

#[derive(Debug)]
struct SharedTracker {
    shards: [TrackerShard; TRACKER_SHARDS],
}

#[derive(Debug, Default)]
struct TrackerShard {
    state: Mutex<TrackerState>,
    writes_changed: Condvar,
}

#[derive(Debug)]
pub(super) struct PendingWriteRegistration {
    shared: Arc<SharedTracker>,
    shard_index: usize,
    key: HandleKey,
    sequence: Sequence,
    completion: OnceLock<PendingWriteCompletion>,
}

#[derive(Debug)]
pub(super) struct PendingWriteBarrier {
    shared: Arc<SharedTracker>,
    shard_index: usize,
    targets: SmallVec<[BarrierTarget; 1]>,
}

impl Default for SharedTracker {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| TrackerShard::default()),
        }
    }
}

impl SharedTracker {
    fn shard_index(ino: Ino) -> usize {
        ino as usize % TRACKER_SHARDS
    }

    fn shard_for_inode(&self, ino: Ino) -> &TrackerShard {
        &self.shards[Self::shard_index(ino)]
    }
}

impl PendingWriteTracker {
    pub(super) fn register(
        &self,
        ino: Ino,
        fh: FileHandle,
    ) -> Result<PendingWriteRegistration, WriteRegistrationError> {
        let shard_index = SharedTracker::shard_index(ino);
        let shard = &self.shared.shards[shard_index];
        let mut state = shard.state.lock().expect("pending write tracker poisoned");
        let generation = state.inode_generation(ino);
        let handle = state.inodes.entry(ino).or_default().entry(fh).or_default();
        handle.generation = generation;
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
            shard_index,
            key: (ino, fh),
            sequence,
            completion: OnceLock::new(),
        })
    }

    pub(super) fn handle_barrier(&self, ino: Ino, fh: FileHandle) -> PendingWriteBarrier {
        self.barrier_for_targets(smallvec![self.last_registered_target((ino, fh))])
    }

    pub(super) fn inode_barrier(&self, ino: Ino) -> PendingWriteBarrier {
        let shard = self.shared.shard_for_inode(ino);
        let state = shard.state.lock().expect("pending write tracker poisoned");
        let targets = state
            .inodes
            .get(&ino)
            .into_iter()
            .flat_map(|handles| {
                handles.iter().map(move |(&fh, handle)| {
                    (
                        (ino, fh),
                        handle.next_sequence.saturating_sub(1),
                        handle.generation,
                    )
                })
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
        let shard = self.shared.shard_for_inode(ino);
        let mut state = shard.state.lock().expect("pending write tracker poisoned");
        let generation = state.inode_generation(ino);
        let handle = state.inodes.entry(ino).or_default().entry(fh).or_default();
        handle.generation = generation;
        match handle.lifecycle {
            HandleLifecycle::Open => handle.lifecycle = HandleLifecycle::Closing,
            HandleLifecycle::Closing => return Err(WriteRegistrationError::HandleClosing),
            HandleLifecycle::Closed => return Err(WriteRegistrationError::HandleClosed),
        }
        let target = handle.next_sequence.saturating_sub(1);
        drop(state);
        Ok(self.barrier_for_targets(smallvec![((ino, fh), target, generation)]))
    }

    pub(super) fn finish_close(&self, ino: Ino, fh: FileHandle) {
        let shard = self.shared.shard_for_inode(ino);
        let mut state = shard.state.lock().expect("pending write tracker poisoned");
        if let Some(handles) = state.inodes.get_mut(&ino) {
            match handles.get_mut(&fh) {
                Some(handle) if handle.pending.is_empty() => {
                    handles.remove(&fh);
                }
                Some(handle) => {
                    handle.lifecycle = HandleLifecycle::Closed;
                }
                None => {}
            }
        }
        state.remove_empty_inode(ino);
        shard.writes_changed.notify_all();
    }

    fn last_registered_target(&self, key: HandleKey) -> BarrierTarget {
        let shard = self.shared.shard_for_inode(key.0);
        let state = shard.state.lock().expect("pending write tracker poisoned");
        let (sequence, generation) = state
            .inodes
            .get(&key.0)
            .and_then(|handles| handles.get(&key.1))
            .map_or((0, state.current_generation(key.0)), |handle| {
                (handle.next_sequence.saturating_sub(1), handle.generation)
            });
        (key, sequence, generation)
    }

    fn barrier_for_targets(&self, targets: SmallVec<[BarrierTarget; 1]>) -> PendingWriteBarrier {
        let shard_index = targets
            .first()
            .map_or(0, |(key, _, _)| SharedTracker::shard_index(key.0));
        {
            let shard = &self.shared.shards[shard_index];
            let mut state = shard.state.lock().expect("pending write tracker poisoned");
            for (key, _, generation) in &targets {
                *state
                    .barrier_counts
                    .entry((key.0, *generation))
                    .or_insert(0) += 1;
            }
        }
        PendingWriteBarrier {
            shared: Arc::clone(&self.shared),
            shard_index,
            targets,
        }
    }
}

impl PendingWriteRegistration {
    pub(super) fn sequence(&self) -> Sequence {
        self.sequence
    }

    pub(super) fn complete(&self, completion: PendingWriteCompletion) {
        if self.completion.set(completion).is_err() {
            return;
        }

        let shard = &self.shared.shards[self.shard_index];
        let mut state = shard.state.lock().expect("pending write tracker poisoned");
        let mut should_remove_closed_handle = false;
        let mut inode_sticky_error = None;
        if let Some(handle) = state
            .inodes
            .get_mut(&self.key.0)
            .and_then(|handles| handles.get_mut(&self.key.1))
        {
            handle.pending.remove(&self.sequence);
            match completion {
                PendingWriteCompletion::Success => {}
                PendingWriteCompletion::Error(errno) => {
                    handle.sticky_error = Some(errno);
                    inode_sticky_error = Some(errno);
                }
                PendingWriteCompletion::TaskFailed => {
                    handle.sticky_error = Some(libc::EIO);
                    inode_sticky_error = Some(libc::EIO);
                }
            }
            should_remove_closed_handle =
                handle.lifecycle == HandleLifecycle::Closed && handle.pending.is_empty();
        }
        if let Some(errno) = inode_sticky_error {
            let generation = state
                .inodes
                .get(&self.key.0)
                .and_then(|handles| handles.get(&self.key.1))
                .map_or_else(
                    || state.current_generation(self.key.0),
                    |handle| handle.generation,
                );
            state
                .inode_sticky_errors
                .entry((self.key.0, generation))
                .or_insert(errno);
        }
        if should_remove_closed_handle {
            state.remove_handle(self.key);
        }
        shard.writes_changed.notify_all();
    }
}

impl Drop for PendingWriteRegistration {
    fn drop(&mut self) {
        if self.completion.get().is_none() {
            self.complete(PendingWriteCompletion::TaskFailed);
        }
    }
}

impl PendingWriteBarrier {
    pub(super) fn is_ready(&self) -> bool {
        let shard = &self.shared.shards[self.shard_index];
        let state = shard.state.lock().expect("pending write tracker poisoned");
        barrier_ready(&state, &self.targets)
    }

    pub(super) fn wait(&self) -> Result<(), i32> {
        let shard = &self.shared.shards[self.shard_index];
        let mut state = shard.state.lock().expect("pending write tracker poisoned");
        while !barrier_ready(&state, &self.targets) {
            state = shard
                .writes_changed
                .wait(state)
                .expect("pending write tracker poisoned");
        }
        barrier_error(&state, &self.targets).map_or(Ok(()), Err)
    }
}

impl Drop for PendingWriteBarrier {
    fn drop(&mut self) {
        let shard = &self.shared.shards[self.shard_index];
        let mut state = shard.state.lock().expect("pending write tracker poisoned");
        for (key, _, generation) in &self.targets {
            let count_key = (key.0, *generation);
            if let Some(count) = state.barrier_counts.get_mut(&count_key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    state.barrier_counts.remove(&count_key);
                    state.remove_retired_inode_error(count_key);
                }
            }
        }
    }
}

fn barrier_ready(state: &TrackerState, targets: &[BarrierTarget]) -> bool {
    targets.iter().all(|(key, target, _)| {
        state
            .inodes
            .get(&key.0)
            .and_then(|handles| handles.get(&key.1))
            .is_none_or(|handle| handle.pending.first().is_none_or(|first| first > target))
    })
}

fn barrier_error(state: &TrackerState, targets: &[BarrierTarget]) -> Option<i32> {
    targets.iter().find_map(|(key, _, generation)| {
        state
            .inodes
            .get(&key.0)
            .and_then(|handles| handles.get(&key.1).and_then(|handle| handle.sticky_error))
            .or_else(|| {
                state
                    .inode_sticky_errors
                    .get(&(key.0, *generation))
                    .copied()
            })
    })
}

impl TrackerState {
    fn current_generation(&self, ino: Ino) -> InodeGeneration {
        self.inode_generations.get(&ino).copied().unwrap_or(0)
    }

    fn inode_generation(&mut self, ino: Ino) -> InodeGeneration {
        self.inode_generations.entry(ino).or_insert(0);
        self.current_generation(ino)
    }

    fn remove_handle(&mut self, key: HandleKey) {
        if let Some(handles) = self.inodes.get_mut(&key.0) {
            handles.remove(&key.1);
        }
        self.remove_empty_inode(key.0);
    }

    fn remove_empty_inode(&mut self, ino: Ino) {
        if self.inodes.get(&ino).is_some_and(HashMap::is_empty) {
            self.inodes.remove(&ino);
            let generation = self.current_generation(ino);
            if self.barrier_counts.contains_key(&(ino, generation))
                || self.inode_sticky_errors.contains_key(&(ino, generation))
            {
                self.inode_generations
                    .insert(ino, generation.saturating_add(1));
            } else {
                self.inode_generations.remove(&ino);
            }
            self.remove_retired_inode_error((ino, generation));
        }
    }

    fn remove_retired_inode_error(&mut self, key: (Ino, InodeGeneration)) {
        if !self.barrier_counts.contains_key(&key) {
            self.inode_sticky_errors.remove(&key);
        }
        self.remove_unused_inode_generation(key.0);
    }

    fn remove_unused_inode_generation(&mut self, ino: Ino) {
        if self.inodes.contains_key(&ino) {
            return;
        }
        let has_barriers = self
            .barrier_counts
            .keys()
            .any(|(barrier_ino, _)| *barrier_ino == ino);
        let has_errors = self
            .inode_sticky_errors
            .keys()
            .any(|(error_ino, _)| *error_ino == ino);
        if !has_barriers && !has_errors {
            self.inode_generations.remove(&ino);
        }
    }
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

        let reopened = tracker
            .register(INO, FIRST_FH)
            .expect("closed handle state should be removed before handle reuse");
        assert_eq!(reopened.sequence(), 1);
    }

    #[test]
    fn finish_close_removes_empty_handle_state() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.begin_close(INO, FIRST_FH).expect("begin close");

        pending.complete(PendingWriteCompletion::Success);
        barrier.wait().expect("close barrier should complete");
        tracker.finish_close(INO, FIRST_FH);

        let shard = tracker.shared.shard_for_inode(INO);
        let state = shard.state.lock().expect("pending write tracker poisoned");
        assert!(
            !state
                .inodes
                .get(&INO)
                .is_some_and(|handles| handles.contains_key(&FIRST_FH)),
            "closed handles should not remain in tracker state"
        );
    }

    #[test]
    fn early_finish_close_removes_handle_after_pending_completion() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");

        tracker.finish_close(INO, FIRST_FH);

        let error = tracker
            .register(INO, FIRST_FH)
            .expect_err("closed handle with pending writes should reject writes");
        assert_eq!(error, WriteRegistrationError::HandleClosed);

        pending.complete(PendingWriteCompletion::Success);

        let shard = tracker.shared.shard_for_inode(INO);
        let state = shard.state.lock().expect("pending write tracker poisoned");
        assert!(
            !state.inodes.contains_key(&INO),
            "closed inode bucket should be removed after final pending completion"
        );
    }

    #[test]
    fn inode_barrier_reports_error_after_closed_handle_cleanup() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.inode_barrier(INO);

        tracker.finish_close(INO, FIRST_FH);
        pending.complete(PendingWriteCompletion::Error(libc::EIO));

        assert_eq!(
            barrier.wait(),
            Err(libc::EIO),
            "closed handle cleanup must not hide writeback failures from inode barriers"
        );
    }

    #[test]
    fn closed_inode_error_does_not_poison_reopened_generation() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");
        let failed_barrier = tracker.inode_barrier(INO);

        tracker.finish_close(INO, FIRST_FH);
        pending.complete(PendingWriteCompletion::Error(libc::EIO));
        assert_eq!(failed_barrier.wait(), Err(libc::EIO));

        let reopened = tracker
            .register(INO, FIRST_FH)
            .expect("reopen should start a new inode generation");
        let reopened_barrier = tracker.handle_barrier(INO, FIRST_FH);
        reopened.complete(PendingWriteCompletion::Success);

        assert_eq!(
            reopened_barrier.wait(),
            Ok(()),
            "old inode writeback error must not poison a reopened generation"
        );
    }

    #[test]
    fn retired_inode_error_is_removed_after_last_barrier_drops() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.inode_barrier(INO);

        tracker.finish_close(INO, FIRST_FH);
        pending.complete(PendingWriteCompletion::Error(libc::EIO));
        assert_eq!(barrier.wait(), Err(libc::EIO));

        {
            let shard = tracker.shared.shard_for_inode(INO);
            let state = shard.state.lock().expect("pending write tracker poisoned");
            assert!(
                state.inode_sticky_errors.contains_key(&(INO, 0)),
                "active barriers keep retired inode errors observable"
            );
        }

        drop(barrier);

        let shard = tracker.shared.shard_for_inode(INO);
        let state = shard.state.lock().expect("pending write tracker poisoned");
        assert!(
            !state.inode_sticky_errors.contains_key(&(INO, 0)),
            "retired inode errors should be removed once no barrier can observe them"
        );
        assert!(
            !state.inode_generations.contains_key(&INO),
            "closed inodes without barriers or errors should not leak generation state"
        );
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

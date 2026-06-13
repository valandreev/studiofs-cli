#![allow(dead_code)]
// This module is introduced ahead of the async FUSE write callbacks. It is
// intentionally unit-tested in isolation before the bridge starts using it.

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Instant;

use smallvec::{SmallVec, smallvec};
use tokio::sync::{Notify, futures::OwnedNotified, oneshot};

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
    AdmissionLimitExceeded,
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
    total_pending_bytes: usize,
    turn_waiters: HashMap<Sequence, oneshot::Sender<()>>,
    sticky_error: Option<i32>,
}

impl Default for HandleState {
    fn default() -> Self {
        Self {
            generation: 0,
            next_sequence: 1,
            lifecycle: HandleLifecycle::Open,
            pending: BTreeSet::new(),
            total_pending_bytes: 0,
            turn_waiters: HashMap::new(),
            sticky_error: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PendingWriteAdmissionLimits {
    pub(super) max_pending_bytes: Option<usize>,
    pub(super) max_pending_writes: Option<usize>,
    pub(super) per_handle_pending_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PendingWriteAdmissionMetrics {
    pub(super) waits: u64,
    pub(super) wait_ns: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PendingWriteBarrierMetrics {
    waits: u64,
    wait_ns: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PendingWriteTrackerSnapshot {
    pub(super) mutation_generation: u64,
    pub(super) pending_writes: usize,
    pub(super) pending_bytes: usize,
    pub(super) peak_pending_bytes: usize,
    pub(super) completed_writes: u64,
    pub(super) failed_writes: u64,
    pub(super) task_failed_writes: u64,
    pub(super) admission_waits: u64,
    pub(super) admission_wait_ns: u64,
    pub(super) barrier_waits: u64,
    pub(super) barrier_wait_ns: u64,
}

#[derive(Debug, Default)]
struct TrackerState {
    inodes: HashMap<Ino, HashMap<FileHandle, HandleState>>,
    inode_generations: HashMap<Ino, InodeGeneration>,
    inode_sticky_errors: HashMap<Ino, HashMap<InodeGeneration, i32>>,
    barrier_counts: HashMap<Ino, HashMap<InodeGeneration, usize>>,
    async_write_notifiers: HashMap<Ino, Arc<Notify>>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingWriteTracker {
    shared: Arc<SharedTracker>,
}

#[derive(Debug)]
struct SharedTracker {
    shards: [TrackerShard; TRACKER_SHARDS],
    // Global caps need Condvar-compatible locking. If this shows up in
    // profiles, add an atomic fast path for clearly-under-budget writes.
    admission: Mutex<GlobalAdmissionState>,
    admission_changed: Condvar,
}

#[derive(Debug, Default)]
struct GlobalAdmissionState {
    total_pending_bytes: usize,
    total_pending_writes: usize,
    peak_pending_bytes: usize,
    completed_writes: u64,
    failed_writes: u64,
    task_failed_writes: u64,
    mutation_generation: u64,
    admission_waits: u64,
    admission_wait_ns: u64,
    barrier_waits: u64,
    barrier_wait_ns: u64,
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
    bytes: usize,
    completion: OnceLock<PendingWriteCompletion>,
    turn_receiver: Option<oneshot::Receiver<()>>,
}

#[derive(Debug)]
pub(super) struct PendingWriteTurn {
    shared: Arc<SharedTracker>,
    shard_index: usize,
    key: HandleKey,
    sequence: Sequence,
    turn_receiver: Option<oneshot::Receiver<()>>,
}

#[derive(Debug)]
pub(super) struct PendingWriteBarrier {
    shared: Arc<SharedTracker>,
    shard_index: usize,
    targets: SmallVec<[BarrierTarget; 1]>,
    include_inode_errors: bool,
}

impl Default for SharedTracker {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| TrackerShard::default()),
            admission: Mutex::new(GlobalAdmissionState::default()),
            admission_changed: Condvar::new(),
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

    fn record_barrier_wait(&self, elapsed: std::time::Duration) {
        self.lock_admission().record_barrier_wait(elapsed);
    }

    fn lock_admission(&self) -> MutexGuard<'_, GlobalAdmissionState> {
        // INVARIANT: pending write admission is only mutated while holding this
        // mutex; poisoning means a prior holder panicked while the tracker was
        // mid-mutation, so continuing could corrupt write admission accounting.
        #[expect(
            clippy::expect_used,
            reason = "fail closed on poisoned write admission"
        )]
        self.admission
            .lock()
            .expect("pending write admission poisoned")
    }

    fn wait_admission<'a>(
        &self,
        admission: MutexGuard<'a, GlobalAdmissionState>,
    ) -> MutexGuard<'a, GlobalAdmissionState> {
        // INVARIANT: condvar wait returns the same admission mutex; poisoning is
        // the same fail-closed accounting condition as lock acquisition.
        #[expect(
            clippy::expect_used,
            reason = "fail closed on poisoned write admission"
        )]
        self.admission_changed
            .wait(admission)
            .expect("pending write admission poisoned")
    }
}

impl TrackerShard {
    fn lock_state(&self) -> MutexGuard<'_, TrackerState> {
        // INVARIANT: shard state owns pending sequence, lifecycle, and sticky
        // error bookkeeping; after poisoning we cannot safely infer write order.
        #[expect(
            clippy::expect_used,
            reason = "fail closed on poisoned pending write shard"
        )]
        self.state.lock().expect("pending write tracker poisoned")
    }

    fn wait_state<'a>(&self, state: MutexGuard<'a, TrackerState>) -> MutexGuard<'a, TrackerState> {
        // INVARIANT: condvar wait returns the same shard mutex; poisoning means
        // the pending-write state may have been left inconsistent.
        #[expect(
            clippy::expect_used,
            reason = "fail closed on poisoned pending write shard"
        )]
        self.writes_changed
            .wait(state)
            .expect("pending write tracker poisoned")
    }
}

impl GlobalAdmissionState {
    fn mark_mutated(&mut self) {
        self.mutation_generation = self.mutation_generation.saturating_add(1);
    }

    fn record_admission_wait(&mut self, elapsed: std::time::Duration) {
        self.admission_waits = self.admission_waits.saturating_add(1);
        let wait_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.admission_wait_ns = self.admission_wait_ns.saturating_add(wait_ns);
        self.mark_mutated();
    }

    fn record_barrier_wait(&mut self, elapsed: std::time::Duration) {
        self.barrier_waits = self.barrier_waits.saturating_add(1);
        let wait_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.barrier_wait_ns = self.barrier_wait_ns.saturating_add(wait_ns);
        self.mark_mutated();
    }
}

impl PendingWriteTracker {
    pub(super) fn register(
        &self,
        ino: Ino,
        fh: FileHandle,
    ) -> Result<PendingWriteRegistration, WriteRegistrationError> {
        self.register_pending_write(ino, fh, 0, PendingWriteAdmissionLimits::default())
    }

    pub(super) fn try_register_with_admission(
        &self,
        ino: Ino,
        fh: FileHandle,
        bytes: usize,
        limits: PendingWriteAdmissionLimits,
    ) -> Result<PendingWriteRegistration, WriteRegistrationError> {
        self.register_pending_write(ino, fh, bytes, limits)
    }

    pub(super) fn register_with_admission(
        &self,
        ino: Ino,
        fh: FileHandle,
        bytes: usize,
        limits: PendingWriteAdmissionLimits,
    ) -> Result<PendingWriteRegistration, WriteRegistrationError> {
        let mut wait_started = None;
        loop {
            let mut admission = self.shared.lock_admission();
            let shard_index = SharedTracker::shard_index(ino);
            let shard = &self.shared.shards[shard_index];
            while admission.global_error(bytes, limits)
                == Some(WriteRegistrationError::AdmissionLimitExceeded)
            {
                if wait_started.is_none() {
                    wait_started = Some(Instant::now());
                }
                admission = self.shared.wait_admission(admission);
            }
            let mut state = shard.lock_state();
            if state.per_handle_admission_error((ino, fh), bytes, limits)
                == Some(WriteRegistrationError::AdmissionLimitExceeded)
            {
                drop(admission);
                if wait_started.is_none() {
                    wait_started = Some(Instant::now());
                }
                let state = shard.wait_state(state);
                drop(state);
                continue;
            }
            if let Some(started) = wait_started {
                admission.record_admission_wait(started.elapsed());
            }
            return self.register_pending_write_locked(
                ino,
                fh,
                bytes,
                shard_index,
                &mut state,
                &mut admission,
            );
        }
    }

    pub(super) fn admission_metrics(&self) -> PendingWriteAdmissionMetrics {
        let admission = self.shared.lock_admission();
        PendingWriteAdmissionMetrics {
            waits: admission.admission_waits,
            wait_ns: admission.admission_wait_ns,
        }
    }

    pub(super) fn snapshot(&self) -> PendingWriteTrackerSnapshot {
        let admission = self.shared.lock_admission();
        PendingWriteTrackerSnapshot {
            mutation_generation: admission.mutation_generation,
            pending_writes: admission.total_pending_writes,
            pending_bytes: admission.total_pending_bytes,
            peak_pending_bytes: admission.peak_pending_bytes,
            completed_writes: admission.completed_writes,
            failed_writes: admission.failed_writes,
            task_failed_writes: admission.task_failed_writes,
            admission_waits: admission.admission_waits,
            admission_wait_ns: admission.admission_wait_ns,
            barrier_waits: admission.barrier_waits,
            barrier_wait_ns: admission.barrier_wait_ns,
        }
    }

    fn barrier_metrics(&self) -> PendingWriteBarrierMetrics {
        let admission = self.shared.lock_admission();
        PendingWriteBarrierMetrics {
            waits: admission.barrier_waits,
            wait_ns: admission.barrier_wait_ns,
        }
    }

    fn register_pending_write(
        &self,
        ino: Ino,
        fh: FileHandle,
        bytes: usize,
        limits: PendingWriteAdmissionLimits,
    ) -> Result<PendingWriteRegistration, WriteRegistrationError> {
        let mut admission = self.shared.lock_admission();
        if let Some(error) = admission.global_error(bytes, limits) {
            return Err(error);
        }
        let shard_index = SharedTracker::shard_index(ino);
        let shard = &self.shared.shards[shard_index];
        let mut state = shard.lock_state();
        if let Some(error) = state.per_handle_admission_error((ino, fh), bytes, limits) {
            return Err(error);
        }
        self.register_pending_write_locked(ino, fh, bytes, shard_index, &mut state, &mut admission)
    }

    fn register_pending_write_locked(
        &self,
        ino: Ino,
        fh: FileHandle,
        bytes: usize,
        shard_index: usize,
        state: &mut TrackerState,
        admission: &mut GlobalAdmissionState,
    ) -> Result<PendingWriteRegistration, WriteRegistrationError> {
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
        handle.total_pending_bytes = handle.total_pending_bytes.saturating_add(bytes);
        admission.total_pending_bytes = admission.total_pending_bytes.saturating_add(bytes);
        admission.total_pending_writes = admission.total_pending_writes.saturating_add(1);
        admission.peak_pending_bytes = admission
            .peak_pending_bytes
            .max(admission.total_pending_bytes);
        admission.mark_mutated();
        let (turn_sender, turn_receiver) = oneshot::channel();
        handle.turn_waiters.insert(sequence, turn_sender);

        Ok(PendingWriteRegistration {
            shared: Arc::clone(&self.shared),
            shard_index,
            key: (ino, fh),
            sequence,
            bytes,
            completion: OnceLock::new(),
            turn_receiver: Some(turn_receiver),
        })
    }

    pub(super) fn handle_barrier(&self, ino: Ino, fh: FileHandle) -> PendingWriteBarrier {
        let shard_index = SharedTracker::shard_index(ino);
        let shard = &self.shared.shards[shard_index];
        let mut state = shard.lock_state();
        let (sequence, generation) = state
            .inodes
            .get(&ino)
            .and_then(|handles| handles.get(&fh))
            .map_or((0, state.current_generation(ino)), |handle| {
                (handle.next_sequence.saturating_sub(1), handle.generation)
            });
        state.increment_barrier_count(ino, generation);
        drop(state);
        PendingWriteBarrier {
            shared: Arc::clone(&self.shared),
            shard_index,
            targets: smallvec![((ino, fh), sequence, generation)],
            include_inode_errors: false,
        }
    }

    pub(super) fn inode_barrier(&self, ino: Ino) -> PendingWriteBarrier {
        let shard_index = SharedTracker::shard_index(ino);
        let shard = &self.shared.shards[shard_index];
        let mut state = shard.lock_state();
        let targets: SmallVec<[BarrierTarget; 1]> = state
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
        for (key, _, generation) in &targets {
            state.increment_barrier_count(key.0, *generation);
        }
        drop(state);
        PendingWriteBarrier {
            shared: Arc::clone(&self.shared),
            shard_index,
            targets,
            include_inode_errors: true,
        }
    }

    pub(super) fn begin_close(
        &self,
        ino: Ino,
        fh: FileHandle,
    ) -> Result<PendingWriteBarrier, WriteRegistrationError> {
        let shard_index = SharedTracker::shard_index(ino);
        let shard = &self.shared.shards[shard_index];
        let mut state = shard.lock_state();
        let generation = state.inode_generation(ino);
        let target = {
            let handle = state.inodes.entry(ino).or_default().entry(fh).or_default();
            handle.generation = generation;
            match handle.lifecycle {
                HandleLifecycle::Open => handle.lifecycle = HandleLifecycle::Closing,
                HandleLifecycle::Closing => return Err(WriteRegistrationError::HandleClosing),
                HandleLifecycle::Closed => return Err(WriteRegistrationError::HandleClosed),
            }
            handle.next_sequence.saturating_sub(1)
        };
        state.increment_barrier_count(ino, generation);
        drop(state);
        Ok(PendingWriteBarrier {
            shared: Arc::clone(&self.shared),
            shard_index,
            targets: smallvec![((ino, fh), target, generation)],
            include_inode_errors: false,
        })
    }

    pub(super) fn finish_close(&self, ino: Ino, fh: FileHandle) {
        let shard = self.shared.shard_for_inode(ino);
        let mut state = shard.lock_state();
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
    }
}

impl PendingWriteRegistration {
    pub(super) fn sequence(&self) -> Sequence {
        self.sequence
    }

    pub(super) fn turn_waiter(&mut self) -> PendingWriteTurn {
        let turn_receiver = self.turn_receiver.take();
        PendingWriteTurn {
            shared: Arc::clone(&self.shared),
            shard_index: self.shard_index,
            key: self.key,
            sequence: self.sequence,
            turn_receiver,
        }
    }
}

impl PendingWriteTurn {
    pub(super) async fn wait(mut self) {
        let shard = &self.shared.shards[self.shard_index];
        {
            let state = shard.lock_state();
            if state.is_registration_turn(self.key, self.sequence) {
                return;
            }
        }
        if let Some(receiver) = self.turn_receiver.take() {
            let _ = receiver.await;
        }
    }
}

impl PendingWriteRegistration {
    pub(super) fn complete(&self, completion: PendingWriteCompletion) {
        if self.completion.set(completion).is_err() {
            return;
        }

        let shard = &self.shared.shards[self.shard_index];
        let mut state = shard.lock_state();
        let mut should_remove_closed_handle = false;
        let mut next_turn = None;
        let mut inode_sticky_error = None;
        let mut generation = None;
        if let Some(handle) = state
            .inodes
            .get_mut(&self.key.0)
            .and_then(|handles| handles.get_mut(&self.key.1))
        {
            generation = Some(handle.generation);
            handle.pending.remove(&self.sequence);
            handle.total_pending_bytes = handle.total_pending_bytes.saturating_sub(self.bytes);
            handle.turn_waiters.remove(&self.sequence);
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
            next_turn = handle
                .pending
                .first()
                .and_then(|sequence| handle.turn_waiters.remove(sequence));
        }
        if let Some(errno) = inode_sticky_error {
            let generation = generation.unwrap_or_else(|| state.current_generation(self.key.0));
            state
                .inode_sticky_errors
                .entry(self.key.0)
                .or_default()
                .entry(generation)
                .or_insert(errno);
        }
        let async_notifier = state.async_write_notifiers.get(&self.key.0).cloned();
        if should_remove_closed_handle {
            state.remove_handle(self.key);
        }
        drop(state);
        let mut admission = self.shared.lock_admission();
        admission.total_pending_bytes = admission.total_pending_bytes.saturating_sub(self.bytes);
        admission.total_pending_writes = admission.total_pending_writes.saturating_sub(1);
        match completion {
            PendingWriteCompletion::Success => {
                admission.completed_writes = admission.completed_writes.saturating_add(1);
            }
            PendingWriteCompletion::Error(_) => {
                admission.failed_writes = admission.failed_writes.saturating_add(1);
            }
            PendingWriteCompletion::TaskFailed => {
                admission.task_failed_writes = admission.task_failed_writes.saturating_add(1);
            }
        }
        admission.mark_mutated();
        drop(admission);
        self.shared.admission_changed.notify_all();
        shard.writes_changed.notify_all();
        if let Some(notifier) = async_notifier {
            notifier.notify_waiters();
        }
        if let Some(next_turn) = next_turn {
            let _ = next_turn.send(());
        }
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
        let state = shard.lock_state();
        barrier_ready(&state, &self.targets)
    }

    pub(super) fn wait(&self) -> Result<(), i32> {
        let shard = &self.shared.shards[self.shard_index];
        let mut state = shard.lock_state();
        let mut waited_since = None;
        while !barrier_ready(&state, &self.targets) {
            waited_since.get_or_insert_with(Instant::now);
            state = shard.wait_state(state);
        }
        if let Some(started) = waited_since {
            self.shared.record_barrier_wait(started.elapsed());
        }
        barrier_error(&state, &self.targets, self.include_inode_errors).map_or(Ok(()), Err)
    }

    pub(super) async fn wait_async(self) -> Result<(), i32> {
        if self.targets.is_empty() {
            return Ok(());
        }
        let mut notified = match self.new_enabled_async_notification() {
            AsyncBarrierWait::Ready(result) => return result,
            AsyncBarrierWait::Pending(notified) => notified,
        };
        let waited_since = Instant::now();
        loop {
            notified.as_mut().await;
            if let Some(result) = self.reset_enabled_async_notification(notified.as_mut()) {
                self.record_barrier_wait(waited_since.elapsed());
                return result;
            }
        }
    }

    fn record_barrier_wait(&self, elapsed: std::time::Duration) {
        self.shared.record_barrier_wait(elapsed);
    }

    fn new_enabled_async_notification(&self) -> AsyncBarrierWait {
        let shard = &self.shared.shards[self.shard_index];
        let mut state = shard.lock_state();
        if barrier_ready(&state, &self.targets) {
            return AsyncBarrierWait::Ready(
                barrier_error(&state, &self.targets, self.include_inode_errors).map_or(Ok(()), Err),
            );
        }
        let mut notified = Box::pin(state.async_write_notifier(&self.targets).notified_owned());
        notified.as_mut().enable();
        AsyncBarrierWait::Pending(notified)
    }

    fn reset_enabled_async_notification(
        &self,
        mut notified: Pin<&mut OwnedNotified>,
    ) -> Option<Result<(), i32>> {
        let shard = &self.shared.shards[self.shard_index];
        let mut state = shard.lock_state();
        if barrier_ready(&state, &self.targets) {
            return Some(
                barrier_error(&state, &self.targets, self.include_inode_errors).map_or(Ok(()), Err),
            );
        }
        notified.set(state.async_write_notifier(&self.targets).notified_owned());
        notified.as_mut().enable();
        None
    }
}

enum AsyncBarrierWait {
    Ready(Result<(), i32>),
    Pending(Pin<Box<OwnedNotified>>),
}

impl Drop for PendingWriteBarrier {
    fn drop(&mut self) {
        if self.targets.is_empty() {
            return;
        }
        let shard = &self.shared.shards[self.shard_index];
        let mut state = shard.lock_state();
        for (key, _, generation) in &self.targets {
            state.decrement_barrier_count(key.0, *generation);
            state.remove_async_write_notifier_if_unused(key.0);
        }
    }
}

fn barrier_ready(state: &TrackerState, targets: &[BarrierTarget]) -> bool {
    targets.iter().all(|(key, target, _)| {
        match state
            .inodes
            .get(&key.0)
            .and_then(|handles| handles.get(&key.1))
        {
            Some(handle) => match handle.pending.first() {
                Some(first) => first > target,
                None => true,
            },
            None => true,
        }
    })
}

fn barrier_error(
    state: &TrackerState,
    targets: &[BarrierTarget],
    include_inode_errors: bool,
) -> Option<i32> {
    targets.iter().find_map(|(key, _, generation)| {
        state
            .inodes
            .get(&key.0)
            .and_then(|handles| handles.get(&key.1).and_then(|handle| handle.sticky_error))
            .or_else(|| {
                if !include_inode_errors {
                    return None;
                }
                state
                    .inode_sticky_errors
                    .get(&key.0)
                    .and_then(|errors| errors.get(generation))
                    .copied()
            })
    })
}

impl TrackerState {
    fn per_handle_admission_error(
        &self,
        key: HandleKey,
        bytes: usize,
        limits: PendingWriteAdmissionLimits,
    ) -> Option<WriteRegistrationError> {
        let handle_pending_bytes = self
            .inodes
            .get(&key.0)
            .and_then(|handles| handles.get(&key.1))
            .map_or(0, |handle| handle.total_pending_bytes);
        if limits.per_handle_pending_bytes.is_some_and(|cap| {
            handle_pending_bytes > 0 && handle_pending_bytes.saturating_add(bytes) > cap
        }) {
            return Some(WriteRegistrationError::AdmissionLimitExceeded);
        }
        None
    }

    fn is_registration_turn(&self, key: HandleKey, sequence: Sequence) -> bool {
        match self
            .inodes
            .get(&key.0)
            .and_then(|handles| handles.get(&key.1))
        {
            Some(handle) => match handle.pending.first() {
                Some(first) => *first >= sequence,
                None => true,
            },
            None => true,
        }
    }

    fn current_generation(&self, ino: Ino) -> InodeGeneration {
        self.inode_generations.get(&ino).copied().unwrap_or(0)
    }

    fn inode_generation(&mut self, ino: Ino) -> InodeGeneration {
        *self.inode_generations.entry(ino).or_insert(0)
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
            if self.has_barrier_count(ino, generation) || self.has_sticky_error(ino, generation) {
                self.inode_generations
                    .insert(ino, generation.saturating_add(1));
            } else {
                self.inode_generations.remove(&ino);
            }
            self.remove_retired_inode_error((ino, generation));
        }
    }

    fn remove_retired_inode_error(&mut self, key: (Ino, InodeGeneration)) {
        if !self.has_barrier_count(key.0, key.1) {
            self.remove_sticky_error(key.0, key.1);
        }
        self.remove_unused_inode_generation(key.0);
    }

    fn remove_unused_inode_generation(&mut self, ino: Ino) {
        if self.inodes.contains_key(&ino) {
            return;
        }
        let has_barriers = self
            .barrier_counts
            .get(&ino)
            .is_some_and(|counts| !counts.is_empty());
        let has_errors = self
            .inode_sticky_errors
            .get(&ino)
            .is_some_and(|errors| !errors.is_empty());
        if !has_barriers && !has_errors {
            self.inode_generations.remove(&ino);
        }
    }

    fn increment_barrier_count(&mut self, ino: Ino, generation: InodeGeneration) {
        *self
            .barrier_counts
            .entry(ino)
            .or_default()
            .entry(generation)
            .or_insert(0) += 1;
    }

    fn decrement_barrier_count(&mut self, ino: Ino, generation: InodeGeneration) {
        let remove_ino = match self.barrier_counts.get_mut(&ino) {
            Some(counts) => match counts.get_mut(&generation) {
                Some(count) => {
                    let mut remove_ino = false;
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        counts.remove(&generation);
                        remove_ino = counts.is_empty();
                    }
                    Some(remove_ino)
                }
                None => None,
            },
            None => None,
        };
        if remove_ino.unwrap_or(false) {
            self.barrier_counts.remove(&ino);
        }
        if remove_ino.is_some() {
            self.remove_retired_inode_error((ino, generation));
        }
    }

    fn has_barrier_count(&self, ino: Ino, generation: InodeGeneration) -> bool {
        self.barrier_counts
            .get(&ino)
            .is_some_and(|counts| counts.contains_key(&generation))
    }

    fn has_barrier_count_for_inode(&self, ino: Ino) -> bool {
        self.barrier_counts
            .get(&ino)
            .is_some_and(|counts| !counts.is_empty())
    }

    fn has_sticky_error(&self, ino: Ino, generation: InodeGeneration) -> bool {
        self.inode_sticky_errors
            .get(&ino)
            .is_some_and(|errors| errors.contains_key(&generation))
    }

    fn remove_sticky_error(&mut self, ino: Ino, generation: InodeGeneration) {
        if let Some(errors) = self.inode_sticky_errors.get_mut(&ino) {
            errors.remove(&generation);
            if errors.is_empty() {
                self.inode_sticky_errors.remove(&ino);
            }
        }
    }

    fn async_write_notifier(&mut self, targets: &[BarrierTarget]) -> Arc<Notify> {
        #[expect(
            clippy::expect_used,
            reason = "caller only enables async wait after a non-empty barrier target check"
        )]
        let (key, _, _) = targets
            .first()
            .expect("pending write barrier targets must not be empty while waiting");
        let ino = key.0;
        Arc::clone(
            self.async_write_notifiers
                .entry(ino)
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    }

    fn remove_async_write_notifier_if_unused(&mut self, ino: Ino) {
        if !self.has_barrier_count_for_inode(ino) {
            self.async_write_notifiers.remove(&ino);
        }
    }
}

impl GlobalAdmissionState {
    fn global_error(
        &self,
        bytes: usize,
        limits: PendingWriteAdmissionLimits,
    ) -> Option<WriteRegistrationError> {
        if limits.max_pending_bytes.is_some_and(|cap| {
            self.total_pending_bytes > 0 && self.total_pending_bytes.saturating_add(bytes) > cap
        }) || limits.max_pending_writes.is_some_and(|cap| {
            self.total_pending_writes > 0 && self.total_pending_writes.saturating_add(1) > cap
        }) {
            return Some(WriteRegistrationError::AdmissionLimitExceeded);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingWriteCompletion, PendingWriteTracker, WriteRegistrationError};

    const INO: u64 = 42;
    const FIRST_FH: u64 = 7;
    const SECOND_FH: u64 = 8;

    #[test]
    fn admission_rejects_total_pending_bytes_over_hard_cap_until_released() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: Some(8),
            max_pending_writes: None,
            per_handle_pending_bytes: None,
        };
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 6, limits)
            .expect("first write fits under total byte cap");

        let blocked = tracker
            .try_register_with_admission(INO, SECOND_FH, 3, limits)
            .expect_err("second write should exceed total byte cap");
        assert_eq!(
            blocked,
            super::WriteRegistrationError::AdmissionLimitExceeded
        );

        first.complete(PendingWriteCompletion::Success);
        tracker
            .register_with_admission(INO, SECOND_FH, 3, limits)
            .expect("released bytes should admit the next write");
    }

    #[test]
    fn admission_allows_single_write_larger_than_global_byte_cap() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: Some(8),
            max_pending_writes: None,
            per_handle_pending_bytes: None,
        };
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 16, limits)
            .expect("single oversized write must not wait forever when no writes are pending");

        let blocked = tracker
            .try_register_with_admission(INO, SECOND_FH, 1, limits)
            .expect_err("other writes must still wait behind the oversized write");
        assert_eq!(
            blocked,
            super::WriteRegistrationError::AdmissionLimitExceeded
        );

        first.complete(PendingWriteCompletion::Success);
        tracker
            .register_with_admission(INO, SECOND_FH, 1, limits)
            .expect("released oversized write should admit follow-up write");
    }

    #[test]
    fn admission_total_pending_bytes_cap_applies_across_shards() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: Some(8),
            max_pending_writes: None,
            per_handle_pending_bytes: None,
        };
        let other_shard_ino = INO + 1;
        assert_ne!(
            super::SharedTracker::shard_index(INO),
            super::SharedTracker::shard_index(other_shard_ino),
            "test setup must use different tracker shards",
        );
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 6, limits)
            .expect("first write fits under global byte cap");

        let blocked = tracker
            .try_register_with_admission(other_shard_ino, FIRST_FH, 3, limits)
            .expect_err("second shard should still be bound by global byte cap");
        assert_eq!(
            blocked,
            super::WriteRegistrationError::AdmissionLimitExceeded
        );

        first.complete(PendingWriteCompletion::Success);
        tracker
            .register_with_admission(other_shard_ino, FIRST_FH, 3, limits)
            .expect("released global bytes should admit write on another shard");
    }

    #[test]
    fn admission_total_pending_writes_cap_applies_across_shards() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: None,
            max_pending_writes: Some(1),
            per_handle_pending_bytes: None,
        };
        let other_shard_ino = INO + 1;
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 1, limits)
            .expect("first write fits under global request cap");

        let blocked = tracker
            .try_register_with_admission(other_shard_ino, FIRST_FH, 1, limits)
            .expect_err("second shard should still be bound by global request cap");
        assert_eq!(
            blocked,
            super::WriteRegistrationError::AdmissionLimitExceeded
        );

        first.complete(PendingWriteCompletion::Success);
        tracker
            .register_with_admission(other_shard_ino, FIRST_FH, 1, limits)
            .expect("released global request slot should admit write on another shard");
    }

    #[test]
    fn admission_rejects_per_handle_pending_bytes_but_allows_other_handles() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: Some(16),
            max_pending_writes: None,
            per_handle_pending_bytes: Some(8),
        };
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 6, limits)
            .expect("first handle write fits under per-handle cap");

        let blocked = tracker
            .try_register_with_admission(INO, FIRST_FH, 3, limits)
            .expect_err("same handle should exceed per-handle byte cap");
        assert_eq!(
            blocked,
            super::WriteRegistrationError::AdmissionLimitExceeded
        );

        tracker
            .register_with_admission(INO, SECOND_FH, 3, limits)
            .expect("other handle should still be admitted under total cap");
        first.complete(PendingWriteCompletion::Error(libc::EIO));
        tracker
            .register_with_admission(INO, FIRST_FH, 3, limits)
            .expect("error completion should release per-handle bytes");
    }

    #[test]
    fn admission_allows_single_write_larger_than_per_handle_byte_cap() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: None,
            max_pending_writes: None,
            per_handle_pending_bytes: Some(8),
        };
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 16, limits)
            .expect("single oversized write must not wait forever on an empty handle");

        let blocked = tracker
            .try_register_with_admission(INO, FIRST_FH, 1, limits)
            .expect_err("same handle must still wait behind the oversized write");
        assert_eq!(
            blocked,
            super::WriteRegistrationError::AdmissionLimitExceeded
        );

        first.complete(PendingWriteCompletion::Success);
        tracker
            .register_with_admission(INO, FIRST_FH, 1, limits)
            .expect("released oversized write should admit follow-up handle write");
    }

    #[test]
    fn dropped_registration_releases_admission_bytes() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: Some(8),
            max_pending_writes: Some(1),
            per_handle_pending_bytes: Some(8),
        };
        let pending = tracker
            .register_with_admission(INO, FIRST_FH, 8, limits)
            .expect("first write fills all caps");

        drop(pending);

        tracker
            .register_with_admission(INO, FIRST_FH, 8, limits)
            .expect("task failure drop should release bytes and request slot");
    }

    #[test]
    fn blocking_admission_records_wait_metric() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: Some(8),
            max_pending_writes: None,
            per_handle_pending_bytes: None,
        };
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 8, limits)
            .expect("first write fills cap");
        let waiting_tracker = PendingWriteTracker {
            shared: std::sync::Arc::clone(&tracker.shared),
        };
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).expect("send waiter started");
            let registration = waiting_tracker
                .register_with_admission(INO, SECOND_FH, 1, limits)
                .expect("waited write should be admitted after release");
            admitted_tx
                .send(registration)
                .expect("send admitted registration");
        });

        started_rx.recv().expect("waiter should start");
        assert!(
            matches!(
                admitted_rx.recv_timeout(std::time::Duration::from_millis(25)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "waiter should not be admitted before pending bytes are released",
        );
        first.complete(PendingWriteCompletion::Success);
        let waited = admitted_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("waiter should be admitted after release");
        waited.complete(PendingWriteCompletion::Success);
        waiter.join().expect("waiter should finish");

        let metrics = tracker.admission_metrics();
        assert_eq!(metrics.waits, 1);
        assert!(metrics.wait_ns > 0);
    }

    #[test]
    fn repeated_per_handle_wakeups_record_one_wait_metric() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: None,
            max_pending_writes: None,
            per_handle_pending_bytes: Some(8),
        };
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 8, limits)
            .expect("first write fills handle cap");
        let waiting_tracker = PendingWriteTracker {
            shared: std::sync::Arc::clone(&tracker.shared),
        };
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).expect("send waiter started");
            let registration = waiting_tracker
                .register_with_admission(INO, FIRST_FH, 1, limits)
                .expect("waited write should be admitted after release");
            admitted_tx
                .send(registration)
                .expect("send admitted registration");
        });

        started_rx.recv().expect("waiter should start");
        let shard = tracker.shared.shard_for_inode(INO);
        shard.writes_changed.notify_all();
        shard.writes_changed.notify_all();
        assert!(
            matches!(
                admitted_rx.recv_timeout(std::time::Duration::from_millis(25)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "spurious wakeups must not admit the waiter while pressure remains",
        );

        first.complete(PendingWriteCompletion::Success);
        let waited = admitted_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("waiter should be admitted after release");
        waited.complete(PendingWriteCompletion::Success);
        waiter.join().expect("waiter should finish");

        let metrics = tracker.admission_metrics();
        assert_eq!(metrics.waits, 1);
        assert!(metrics.wait_ns > 0);
    }

    #[test]
    fn snapshot_reports_pending_peak_completion_and_admission_waits() {
        let tracker = PendingWriteTracker::default();
        let limits = super::PendingWriteAdmissionLimits {
            max_pending_bytes: Some(10),
            max_pending_writes: Some(2),
            per_handle_pending_bytes: None,
        };
        let first = tracker
            .register_with_admission(INO, FIRST_FH, 6, limits)
            .expect("first write should be admitted");
        let second = tracker
            .register_with_admission(INO, SECOND_FH, 4, limits)
            .expect("second write should be admitted");

        let pending = tracker.snapshot();
        assert_eq!(pending.pending_writes, 2);
        assert_eq!(pending.pending_bytes, 10);
        assert_eq!(pending.peak_pending_bytes, 10);
        assert_eq!(pending.completed_writes, 0);
        assert_eq!(pending.failed_writes, 0);

        let waiting_tracker = PendingWriteTracker {
            shared: std::sync::Arc::clone(&tracker.shared),
        };
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).expect("send waiter started");
            let registration = waiting_tracker
                .register_with_admission(INO, FIRST_FH, 1, limits)
                .expect("waited write should be admitted");
            admitted_tx.send(registration).expect("send registration");
        });

        started_rx.recv().expect("waiter should start");
        assert!(
            matches!(
                admitted_rx.recv_timeout(std::time::Duration::from_millis(25)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "waiter should block until admission pressure drops",
        );
        first.complete(PendingWriteCompletion::Success);
        let waited = admitted_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("waiter should be admitted after release");
        second.complete(PendingWriteCompletion::Error(libc::EIO));
        waited.complete(PendingWriteCompletion::Success);
        waiter.join().expect("waiter should finish");

        let completed = tracker.snapshot();
        assert_eq!(completed.pending_writes, 0);
        assert_eq!(completed.pending_bytes, 0);
        assert_eq!(completed.peak_pending_bytes, 10);
        assert_eq!(completed.completed_writes, 2);
        assert_eq!(completed.failed_writes, 1);
        assert_eq!(completed.admission_waits, 1);
        assert!(completed.admission_wait_ns > 0);
    }

    #[test]
    fn snapshot_records_blocking_barrier_wait_totals() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.handle_barrier(INO, FIRST_FH);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).expect("send waiter started");
            result_tx.send(barrier.wait()).expect("send barrier result");
        });

        started_rx.recv().expect("waiter should start");
        assert!(
            matches!(
                result_rx.recv_timeout(std::time::Duration::from_millis(25)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "barrier should wait while prior write is pending",
        );
        pending.complete(PendingWriteCompletion::Success);
        assert_eq!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("barrier should resolve"),
            Ok(())
        );
        waiter.join().expect("waiter should finish");

        let snapshot = tracker.snapshot();
        assert_eq!(snapshot.barrier_waits, 1);
        assert!(snapshot.barrier_wait_ns > 0);
    }

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

    #[tokio::test]
    async fn async_barrier_wait_resolves_after_prior_writes_complete() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.handle_barrier(INO, FIRST_FH);

        let waiter = tokio::spawn(async move { barrier.wait_async().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        pending.complete(PendingWriteCompletion::Success);

        assert_eq!(waiter.await.expect("waiter task should finish"), Ok(()));
    }

    #[tokio::test]
    async fn async_barrier_wait_registers_inode_scoped_notifier() {
        let tracker = PendingWriteTracker::default();
        let pending = tracker.register(INO, FIRST_FH).expect("register write");
        let barrier = tracker.handle_barrier(INO, FIRST_FH);

        let waiter = tokio::spawn(async move { barrier.wait_async().await });
        tokio::task::yield_now().await;

        let shard = tracker.shared.shard_for_inode(INO);
        {
            let state = shard.state.lock().expect("pending write tracker poisoned");
            assert!(state.async_write_notifiers.contains_key(&INO));
            assert!(
                !state
                    .async_write_notifiers
                    .contains_key(&(INO + super::TRACKER_SHARDS as u64))
            );
        }

        pending.complete(PendingWriteCompletion::Success);
        assert_eq!(waiter.await.expect("waiter task should finish"), Ok(()));

        let state = shard.state.lock().expect("pending write tracker poisoned");
        assert!(!state.async_write_notifiers.contains_key(&INO));
    }

    #[tokio::test]
    async fn async_empty_inode_barrier_returns_without_notifier() {
        let tracker = PendingWriteTracker::default();
        let barrier = tracker.inode_barrier(INO);
        let shard = tracker.shared.shard_for_inode(INO);
        {
            let state = shard.state.lock().expect("pending write tracker poisoned");
            assert!(!state.async_write_notifiers.contains_key(&INO));
        }

        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_millis(25), barrier.wait_async()).await,
            Ok(Ok(()))
        );

        let state = shard.state.lock().expect("pending write tracker poisoned");
        assert!(!state.async_write_notifiers.contains_key(&INO));
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
                state
                    .inode_sticky_errors
                    .get(&INO)
                    .is_some_and(|errors| errors.contains_key(&0)),
                "active barriers keep retired inode errors observable"
            );
        }

        drop(barrier);

        let shard = tracker.shared.shard_for_inode(INO);
        let state = shard.state.lock().expect("pending write tracker poisoned");
        assert!(
            !state
                .inode_sticky_errors
                .get(&INO)
                .is_some_and(|errors| errors.contains_key(&0)),
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

    #[test]
    fn write_turn_waits_for_prior_sequence_on_same_handle() {
        let tracker = PendingWriteTracker::default();
        let mut first = tracker
            .register(INO, FIRST_FH)
            .expect("register first write");
        let second = tracker
            .register(INO, FIRST_FH)
            .expect("register second write");

        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(first.turn_waiter().wait());

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let mut second = second;
            let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
            started_tx.send(()).expect("send waiter started");
            rt.block_on(second.turn_waiter().wait());
            done_tx.send(()).expect("send waiter done");
            second.complete(PendingWriteCompletion::Success);
        });

        started_rx.recv().expect("waiter should start");
        assert_eq!(
            done_rx.recv_timeout(std::time::Duration::from_millis(25)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "later writes on the same handle must wait for prior sequence completion",
        );

        first.complete(PendingWriteCompletion::Success);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("waiter should finish after first write");
        waiter
            .join()
            .expect("waiter should finish after first write");
    }
}

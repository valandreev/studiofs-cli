use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::task::AbortHandle;

pub(crate) const DEFAULT_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_PENDING_WRITES: usize = 1024;
const DEFAULT_MAX_RECORDED_ERRORS: usize = 8192;
// Bound callback occupancy, including time queued for global write admission.
const DEFAULT_BARRIER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionError {
    ErrorJournalFull,
    HandleClosed,
    RequestTooLarge,
    ShuttingDown,
    SequenceExhausted,
}

impl AdmissionError {
    pub(crate) fn errno(self) -> i32 {
        match self {
            Self::HandleClosed => libc::EBADF,
            Self::RequestTooLarge => libc::EINVAL,
            Self::ErrorJournalFull | Self::ShuttingDown | Self::SequenceExhausted => libc::EIO,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BarrierOutcome {
    Complete(Option<i32>),
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InodeWatermark {
    ino: u64,
    through: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HandleWatermark {
    fh: u64,
    through: u64,
}

#[derive(Clone, Copy, Debug)]
struct PendingWrite {
    ino: u64,
    fh: u64,
    reserved_bytes: Option<usize>,
    cancellable: bool,
    cancellation_requested: bool,
}

#[derive(Clone, Copy, Debug)]
struct WriteError {
    ino: u64,
    fh: u64,
    errno: i32,
}

#[derive(Debug, Default)]
struct LedgerState {
    shutting_down: bool,
    next_sequence: u64,
    next_capacity_sequence: Option<u64>,
    // These counters cover only callbacks admitted to async execution. Callbacks waiting for
    // capacity remain synchronous libfuse workers, but are already present in the indexes below
    // so barriers cannot miss them. Libfuse callback concurrency bounds that registered set.
    admitted_bytes: usize,
    admitted_writes: usize,
    // Every callback is registered here before it is allowed to wait for async capacity.
    pending: BTreeMap<u64, PendingWrite>,
    pending_by_inode: HashMap<u64, BTreeSet<u64>>,
    pending_by_handle: HashMap<u64, BTreeSet<u64>>,
    // Keep one entry per sequence: lossy coalescing could hide an error from a
    // captured watermark. Admission reserves bounded journal capacity for
    // every pending request before assigning its sequence.
    errors: BTreeMap<u64, WriteError>,
    closed_handles: HashMap<u64, u64>,
    task_abort_handles: HashMap<u64, AbortHandle>,
    deferred_releases: HashMap<u64, DeferredRelease>,
    running_releases: BTreeSet<u64>,
    snapshot_generation: u64,
    peak_admitted_bytes: usize,
    completed_writes: u64,
    failed_writes: u64,
    task_failed_writes: u64,
    canceled_writes: u64,
    shutdown_timeouts: u64,
    barrier_timeouts: u64,
    admission_waits: u64,
    #[cfg(test)]
    started_admission_waits: usize,
    admission_wait_total_nanos: u64,
    barrier_waits: u64,
    inode_barrier_waits: u64,
    handle_barrier_waits: u64,
    barrier_wait_total_nanos: u64,
}

impl LedgerState {
    fn advance_capacity_sequence(&mut self, sequence: u64) {
        if self.next_capacity_sequence == Some(sequence) {
            // Ordered admission means every later pending request is still unreserved.
            self.next_capacity_sequence = self
                .pending
                .range((Excluded(sequence), Unbounded))
                .next()
                .map(|(&next, _)| next);
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AsyncWriteMetricsSnapshot {
    pub(crate) snapshot_generation: u64,
    pub(crate) pending_writes: usize,
    pub(crate) pending_bytes: usize,
    pub(crate) pending_handles: usize,
    pub(crate) pending_writes_per_inode: BTreeMap<u64, usize>,
    pub(crate) peak_pending_bytes: usize,
    pub(crate) completed_writes: u64,
    pub(crate) failed_writes: u64,
    pub(crate) task_failed_writes: u64,
    pub(crate) canceled_writes: u64,
    pub(crate) shutdown_timeouts: u64,
    pub(crate) barrier_timeouts: u64,
    pub(crate) admission_waits: u64,
    pub(crate) admission_wait_total_nanos: u64,
    pub(crate) barrier_waits: u64,
    pub(crate) inode_barrier_waits: u64,
    pub(crate) handle_barrier_waits: u64,
    pub(crate) barrier_wait_total_nanos: u64,
}

#[derive(Debug)]
pub(crate) struct AsyncWriteLedger {
    barrier_timeout: Duration,
    max_pending_bytes: usize,
    max_pending_writes: usize,
    max_recorded_errors: usize,
    state: Mutex<LedgerState>,
    changed: Condvar,
    #[cfg(test)]
    admission_wait_started: Condvar,
}

impl Default for AsyncWriteLedger {
    fn default() -> Self {
        Self::with_limits(DEFAULT_MAX_PENDING_BYTES, DEFAULT_MAX_PENDING_WRITES)
    }
}

impl AsyncWriteLedger {
    pub(crate) fn with_limits(max_pending_bytes: usize, max_pending_writes: usize) -> Self {
        Self::with_limits_and_error_limit(
            max_pending_bytes,
            max_pending_writes,
            DEFAULT_MAX_RECORDED_ERRORS,
        )
    }

    fn with_limits_and_error_limit(
        max_pending_bytes: usize,
        max_pending_writes: usize,
        max_recorded_errors: usize,
    ) -> Self {
        assert!(max_pending_bytes > 0, "pending byte limit must be non-zero");
        assert!(
            max_pending_writes > 0,
            "pending write limit must be non-zero"
        );
        assert!(
            max_recorded_errors > 0,
            "recorded error limit must be non-zero"
        );
        Self {
            barrier_timeout: DEFAULT_BARRIER_TIMEOUT,
            max_pending_bytes,
            max_pending_writes,
            max_recorded_errors,
            state: Mutex::new(LedgerState::default()),
            changed: Condvar::new(),
            #[cfg(test)]
            admission_wait_started: Condvar::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_barrier_timeout(mut self, timeout: Duration) -> Self {
        self.barrier_timeout = timeout;
        self
    }

    #[cfg(test)]
    pub(crate) fn admit(
        self: &Arc<Self>,
        ino: u64,
        fh: u64,
        bytes: usize,
    ) -> Result<AsyncWriteTicket, AdmissionError> {
        let mut ticket = self.start_request(ino, fh)?;
        if let Err(error) = ticket.reserve_capacity(bytes) {
            ticket.complete(Err(error.errno()));
            return Err(error);
        }
        Ok(ticket)
    }

    /// Register a callback in barrier order before it waits for async execution capacity.
    pub(crate) fn start_request(
        self: &Arc<Self>,
        ino: u64,
        fh: u64,
    ) -> Result<AsyncWriteTicket, AdmissionError> {
        let mut state = self.lock_state();
        if state.shutting_down {
            return Err(AdmissionError::ShuttingDown);
        }
        if state.closed_handles.contains_key(&fh) {
            return Err(AdmissionError::HandleClosed);
        }
        // Reserve space for every accepted request because each may fail.
        // This bounds the journal without coalescing sequence identities or
        // losing pre/post-watermark precision.
        if state.errors.len().saturating_add(state.pending.len()) >= self.max_recorded_errors {
            return Err(AdmissionError::ErrorJournalFull);
        }
        if state.next_sequence == u64::MAX {
            return Err(AdmissionError::SequenceExhausted);
        }

        let sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or(AdmissionError::SequenceExhausted)?;
        state.next_sequence = sequence;
        state.next_capacity_sequence.get_or_insert(sequence);
        state.pending.insert(
            sequence,
            PendingWrite {
                ino,
                fh,
                reserved_bytes: None,
                cancellable: true,
                cancellation_requested: false,
            },
        );
        state
            .pending_by_inode
            .entry(ino)
            .or_default()
            .insert(sequence);
        state
            .pending_by_handle
            .entry(fh)
            .or_default()
            .insert(sequence);

        Ok(AsyncWriteTicket {
            ledger: self.clone(),
            sequence,
            finished: false,
        })
    }

    pub(crate) fn capture_inode_watermark(&self, ino: u64) -> InodeWatermark {
        let state = self.lock_state();
        InodeWatermark {
            ino,
            through: state.next_sequence,
        }
    }

    #[cfg(test)]
    pub(crate) fn capture_handle_watermark(&self, fh: u64) -> HandleWatermark {
        let state = self.lock_state();
        HandleWatermark {
            fh,
            through: state.next_sequence,
        }
    }

    pub(crate) fn close_handle(&self, fh: u64) -> HandleWatermark {
        let mut state = self.lock_state();
        let through = state.next_sequence;
        let through = *state.closed_handles.entry(fh).or_insert(through);
        self.changed.notify_all();
        HandleWatermark { fh, through }
    }

    pub(crate) fn forget_handle(&self, fh: u64) -> bool {
        let mut state = self.lock_state();
        if state.pending_by_handle.contains_key(&fh)
            || state.deferred_releases.contains_key(&fh)
            || state.running_releases.contains(&fh)
            || state.errors.values().any(|error| error.fh == fh)
        {
            return false;
        }
        state.closed_handles.remove(&fh);
        self.changed.notify_all();
        true
    }

    /// Own a kernel release until its closed handle has no remaining writes.
    pub(crate) fn defer_release(&self, fh: u64, release: impl FnOnce() + Send + 'static) {
        let release = DeferredRelease(Box::new(release));
        let mut state = self.lock_state();
        if state.pending_by_handle.contains_key(&fh) {
            state.deferred_releases.insert(fh, release);
            return;
        }
        state.running_releases.insert(fh);
        drop(state);
        self.finish_deferred_release(fh, release);
    }

    fn finish_deferred_release(&self, fh: u64, release: DeferredRelease) {
        (release.0)();
        let mut state = self.lock_state();
        // The kernel already received EIO; no later callback can consume these errors.
        state.errors.retain(|_, error| error.fh != fh);
        state.closed_handles.remove(&fh);
        state.running_releases.remove(&fh);
        self.changed.notify_all();
    }

    pub(crate) fn wait_inode(&self, watermark: InodeWatermark) -> BarrierOutcome {
        self.wait_scope(BarrierScope::Inode(watermark.ino), watermark.through)
    }

    /// Wait for writes admitted before `watermark` without consuming sticky errors.
    ///
    /// Namespace operations use this to observe prior write effects while
    /// leaving error delivery to the next flush, fsync, or release barrier.
    pub(crate) fn wait_inode_completion(&self, watermark: InodeWatermark) -> BarrierOutcome {
        self.wait_scope_timed(
            BarrierScope::Inode(watermark.ino),
            watermark.through,
            self.barrier_timeout,
            ErrorDelivery::Preserve,
        )
    }

    pub(crate) fn wait_handle(&self, watermark: HandleWatermark) -> BarrierOutcome {
        self.wait_scope(BarrierScope::Handle(watermark.fh), watermark.through)
    }

    #[cfg(test)]
    fn wait_inode_timed(&self, watermark: InodeWatermark, timeout: Duration) -> BarrierOutcome {
        self.wait_scope_timed(
            BarrierScope::Inode(watermark.ino),
            watermark.through,
            timeout,
            ErrorDelivery::Consume,
        )
    }

    #[cfg(test)]
    fn wait_handle_timed(&self, watermark: HandleWatermark, timeout: Duration) -> BarrierOutcome {
        self.wait_scope_timed(
            BarrierScope::Handle(watermark.fh),
            watermark.through,
            timeout,
            ErrorDelivery::Consume,
        )
    }

    pub(crate) fn wait_all(&self, timeout: Duration) -> BarrierOutcome {
        let state = self.lock_state();
        let through = state.next_sequence;
        drop(state);
        self.wait_scope_timed(BarrierScope::All, through, timeout, ErrorDelivery::Consume)
    }

    pub(crate) fn shutdown(&self) {
        let mut state = self.lock_state();
        state.shutting_down = true;
        self.changed.notify_all();
    }

    /// Attach cancellation to an admitted task.
    ///
    /// The task may complete before its `AbortHandle` is registered. In that
    /// case `finish` has already removed the sequence and the stale handle is
    /// deliberately not retained.
    pub(crate) fn track_task(&self, sequence: u64, abort_handle: AbortHandle) -> bool {
        let mut state = self.lock_state();
        let Some(write) = state.pending.get(&sequence) else {
            return false;
        };
        if write.cancellation_requested {
            drop(state);
            abort_handle.abort();
            return true;
        }
        if !write.cancellable {
            return true;
        }
        state.task_abort_handles.insert(sequence, abort_handle);
        true
    }

    /// Atomically leave the shutdown-cancellable phase before entering the VFS write future.
    pub(crate) fn mark_task_non_cancellable(&self, sequence: u64) -> bool {
        let mut state = self.lock_state();
        let Some(write) = state.pending.get_mut(&sequence) else {
            return false;
        };
        if write.cancellation_requested {
            return false;
        }
        write.cancellable = false;
        state.task_abort_handles.remove(&sequence);
        true
    }

    pub(crate) fn abort_pending_tasks(&self) -> usize {
        let mut state = self.lock_state();
        let cancellable = state
            .pending
            .iter()
            .filter_map(|(&sequence, write)| {
                (write.cancellable && !write.cancellation_requested).then_some(sequence)
            })
            .collect::<Vec<_>>();
        for sequence in &cancellable {
            if let Some(write) = state.pending.get_mut(sequence) {
                write.cancellation_requested = true;
            }
            if let Some(abort_handle) = state.task_abort_handles.get(sequence) {
                abort_handle.abort();
            }
        }
        state.canceled_writes = state
            .canceled_writes
            .saturating_add(cancellable.len() as u64);
        cancellable.len()
    }

    pub(crate) fn record_shutdown_timeout(&self) {
        let mut state = self.lock_state();
        state.shutdown_timeouts = state.shutdown_timeouts.saturating_add(1);
    }

    pub(crate) fn metrics_snapshot(&self) -> AsyncWriteMetricsSnapshot {
        let mut state = self.lock_state();
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        AsyncWriteMetricsSnapshot {
            snapshot_generation: state.snapshot_generation,
            pending_writes: state.pending.len(),
            pending_bytes: state.admitted_bytes,
            pending_handles: state.pending_by_handle.len(),
            pending_writes_per_inode: state
                .pending_by_inode
                .iter()
                .map(|(&ino, writes)| (ino, writes.len()))
                .collect(),
            peak_pending_bytes: state.peak_admitted_bytes,
            completed_writes: state.completed_writes,
            failed_writes: state.failed_writes,
            task_failed_writes: state.task_failed_writes,
            canceled_writes: state.canceled_writes,
            shutdown_timeouts: state.shutdown_timeouts,
            barrier_timeouts: state.barrier_timeouts,
            admission_waits: state.admission_waits,
            admission_wait_total_nanos: state.admission_wait_total_nanos,
            barrier_waits: state.barrier_waits,
            inode_barrier_waits: state.inode_barrier_waits,
            handle_barrier_waits: state.handle_barrier_waits,
            barrier_wait_total_nanos: state.barrier_wait_total_nanos,
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, LedgerState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn at_capacity(&self, state: &LedgerState, bytes: usize) -> bool {
        if state.admitted_writes >= self.max_pending_writes {
            return true;
        }
        state
            .admitted_bytes
            .checked_add(bytes)
            .is_none_or(|total| total > self.max_pending_bytes)
    }

    /// Wait until a registered callback may enter the bounded async execution set.
    ///
    /// Capacity is granted in global registration order. This intentionally introduces
    /// head-of-line blocking when the earliest callback has not reached this method yet; it keeps
    /// later callbacks from starving an earlier captured inode or handle barrier.
    fn reserve_capacity(&self, sequence: u64, bytes: usize) -> Result<(), AdmissionError> {
        if bytes > self.max_pending_bytes {
            return Err(AdmissionError::RequestTooLarge);
        }

        let mut state = self.lock_state();
        let mut wait_started = None;
        loop {
            if state.shutting_down {
                record_admission_wait(&mut state, wait_started);
                return Err(AdmissionError::ShuttingDown);
            }
            // A live ticket keeps its entry pending, and every pending entry below the capacity
            // head is already reserved.
            if state
                .next_capacity_sequence
                .is_none_or(|head| sequence < head)
            {
                return Ok(());
            }
            let is_next = state.next_capacity_sequence == Some(sequence);
            if is_next && !self.at_capacity(&state, bytes) {
                break;
            }
            #[cfg(test)]
            if wait_started.is_none() {
                state.started_admission_waits += 1;
                self.admission_wait_started.notify_all();
            }
            wait_started.get_or_insert_with(Instant::now);
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }

        state.admitted_bytes = state
            .admitted_bytes
            .checked_add(bytes)
            .expect("capacity predicate excludes byte overflow");
        state.admitted_writes = state
            .admitted_writes
            .checked_add(1)
            .expect("configured write limit excludes request count overflow");
        state.peak_admitted_bytes = state.peak_admitted_bytes.max(state.admitted_bytes);
        record_admission_wait(&mut state, wait_started);
        if let Some(write) = state.pending.get_mut(&sequence) {
            write.reserved_bytes = Some(bytes);
        }
        state.advance_capacity_sequence(sequence);
        self.changed.notify_all();
        Ok(())
    }

    fn wait_scope(&self, scope: BarrierScope, through: u64) -> BarrierOutcome {
        self.wait_scope_timed(scope, through, self.barrier_timeout, ErrorDelivery::Consume)
    }

    fn wait_scope_timed(
        &self,
        scope: BarrierScope,
        through: u64,
        timeout: Duration,
        error_delivery: ErrorDelivery,
    ) -> BarrierOutcome {
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let mut state = self.lock_state();
        let wait_started =
            (scope != BarrierScope::All && scope.has_pending(&state, through)).then(Instant::now);
        loop {
            if !scope.has_pending(&state, through) {
                record_barrier_wait(&mut state, scope, wait_started);
                let errno = if error_delivery == ErrorDelivery::Consume {
                    scope.consume_errors(&mut state, through)
                } else {
                    None
                };
                return BarrierOutcome::Complete(errno);
            }

            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                record_barrier_wait(&mut state, scope, wait_started);
                if scope != BarrierScope::All {
                    state.barrier_timeouts = state.barrier_timeouts.saturating_add(1);
                    let pending_writes = state.pending.len();
                    let pending_bytes = state.admitted_bytes;
                    drop(state);
                    tracing::warn!(
                        ?scope,
                        through,
                        ?timeout,
                        pending_writes,
                        pending_bytes,
                        "Linux FUSE async-write barrier timed out; pending writes remain owned"
                    );
                }
                return BarrierOutcome::TimedOut;
            }
            let (next_state, _) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
        }
    }

    fn finish(&self, sequence: u64, result: Result<(), i32>, task_failed: bool) {
        let mut state = self.lock_state();
        let Some(write) = state.pending.remove(&sequence) else {
            return;
        };
        state.advance_capacity_sequence(sequence);
        if let Some(bytes) = write.reserved_bytes {
            state.admitted_bytes = state
                .admitted_bytes
                .checked_sub(bytes)
                .expect("reserved write bytes must be reflected in admitted bytes");
            state.admitted_writes = state
                .admitted_writes
                .checked_sub(1)
                .expect("reserved write must be reflected in admitted request count");
        }
        remove_sequence(&mut state.pending_by_inode, write.ino, sequence);
        remove_sequence(&mut state.pending_by_handle, write.fh, sequence);
        state.completed_writes = state.completed_writes.saturating_add(1);
        if result.is_err() {
            state.failed_writes = state.failed_writes.saturating_add(1);
        }
        if task_failed {
            state.task_failed_writes = state.task_failed_writes.saturating_add(1);
        }
        if let Err(errno) = result {
            state.errors.insert(
                sequence,
                WriteError {
                    ino: write.ino,
                    fh: write.fh,
                    errno,
                },
            );
        }
        state.task_abort_handles.remove(&sequence);
        let release = if state.pending_by_handle.contains_key(&write.fh) {
            None
        } else {
            state.deferred_releases.remove(&write.fh)
        };
        if release.is_some() {
            state.running_releases.insert(write.fh);
        }
        self.changed.notify_all();
        drop(state);
        if let Some(release) = release {
            self.finish_deferred_release(write.fh, release);
        }
    }

    #[cfg(test)]
    fn limits_for_test(&self) -> (usize, usize) {
        (self.max_pending_bytes, self.max_pending_writes)
    }

    #[cfg(test)]
    fn pending_for_test(&self) -> (usize, usize) {
        let state = self.lock_state();
        (state.pending.len(), state.admitted_bytes)
    }

    #[cfg(test)]
    fn admission_for_test(&self) -> (usize, usize) {
        let state = self.lock_state();
        (state.admitted_writes, state.admitted_bytes)
    }

    #[cfg(test)]
    fn set_next_sequence_for_test(&self, sequence: u64) {
        self.lock_state().next_sequence = sequence;
    }
}

fn remove_sequence(index: &mut HashMap<u64, BTreeSet<u64>>, key: u64, sequence: u64) {
    let should_remove = index.get_mut(&key).is_some_and(|entries| {
        entries.remove(&sequence);
        entries.is_empty()
    });
    if should_remove {
        index.remove(&key);
    }
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn record_admission_wait(state: &mut LedgerState, wait_started: Option<Instant>) {
    let Some(started) = wait_started else {
        return;
    };
    state.admission_waits = state.admission_waits.saturating_add(1);
    state.admission_wait_total_nanos = state
        .admission_wait_total_nanos
        .saturating_add(duration_nanos(started.elapsed()));
}

fn record_barrier_wait(
    state: &mut LedgerState,
    scope: BarrierScope,
    wait_started: Option<Instant>,
) {
    let Some(started) = wait_started else {
        return;
    };
    state.barrier_waits = state.barrier_waits.saturating_add(1);
    match scope {
        BarrierScope::Inode(_) => {
            state.inode_barrier_waits = state.inode_barrier_waits.saturating_add(1);
        }
        BarrierScope::Handle(_) => {
            state.handle_barrier_waits = state.handle_barrier_waits.saturating_add(1);
        }
        BarrierScope::All => return,
    }
    state.barrier_wait_total_nanos = state
        .barrier_wait_total_nanos
        .saturating_add(duration_nanos(started.elapsed()));
}

struct DeferredRelease(Box<dyn FnOnce() + Send>);

impl fmt::Debug for DeferredRelease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeferredRelease")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ErrorDelivery {
    Consume,
    Preserve,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BarrierScope {
    Inode(u64),
    Handle(u64),
    All,
}

impl BarrierScope {
    fn has_pending(self, state: &LedgerState, through: u64) -> bool {
        let entries = match self {
            Self::Inode(ino) => state.pending_by_inode.get(&ino),
            Self::Handle(fh) => state.pending_by_handle.get(&fh),
            Self::All => {
                return state.pending.range(..=through).next().is_some()
                    || !state.deferred_releases.is_empty()
                    || !state.running_releases.is_empty();
            }
        };
        entries.is_some_and(|entries| entries.range(..=through).next().is_some())
    }

    fn consume_errors(self, state: &mut LedgerState, through: u64) -> Option<i32> {
        let matching = state
            .errors
            .range(..=through)
            .filter_map(|(&sequence, error)| self.matches(*error).then_some(sequence))
            .collect::<Vec<_>>();
        let first_errno = matching
            .first()
            .and_then(|sequence| state.errors.get(sequence))
            .map(|error| error.errno);
        for sequence in matching {
            state.errors.remove(&sequence);
        }
        first_errno
    }

    fn matches(self, error: WriteError) -> bool {
        match self {
            Self::Inode(ino) => error.ino == ino,
            Self::Handle(fh) => error.fh == fh,
            Self::All => true,
        }
    }
}

pub(crate) struct AsyncWriteTicket {
    ledger: Arc<AsyncWriteLedger>,
    sequence: u64,
    finished: bool,
}

impl AsyncWriteTicket {
    pub(crate) const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn reserve_capacity(&mut self, bytes: usize) -> Result<(), AdmissionError> {
        self.ledger.reserve_capacity(self.sequence, bytes)
    }

    pub(crate) fn complete(mut self, result: Result<(), i32>) {
        self.finished = true;
        self.ledger.finish(self.sequence, result, false);
    }
}

impl fmt::Debug for AsyncWriteTicket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AsyncWriteTicket")
            .field("sequence", &self.sequence)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl Drop for AsyncWriteTicket {
    fn drop(&mut self) {
        if !self.finished {
            self.finished = true;
            self.ledger.finish(self.sequence, Err(libc::EIO), true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdmissionError, AsyncWriteLedger, BarrierOutcome, DEFAULT_MAX_PENDING_BYTES,
        DEFAULT_MAX_PENDING_WRITES,
    };
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    const SHORT_WAIT: Duration = Duration::from_millis(20);

    #[test]
    fn timeout_does_not_poison_idle_barriers_or_new_admission() {
        let ledger = Arc::new(AsyncWriteLedger::default().with_barrier_timeout(Duration::ZERO));
        let ticket = ledger.admit(10, 100, 4).unwrap();
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(10)),
            BarrierOutcome::TimedOut
        );
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(20)),
            BarrierOutcome::Complete(None)
        );
        assert_eq!(
            ledger.wait_inode_completion(ledger.capture_inode_watermark(20)),
            BarrierOutcome::Complete(None)
        );
        assert_eq!(
            ledger.wait_handle(ledger.close_handle(200)),
            BarrierOutcome::Complete(None)
        );
        assert!(ledger.forget_handle(200));
        ledger
            .admit(20, 201, 4)
            .expect("unrelated writes remain usable")
            .complete(Ok(()));
        ticket.complete(Ok(()));
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(10)),
            BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn timeout_does_not_fail_registered_capacity_waiter() {
        let ledger = Arc::new(AsyncWriteLedger {
            barrier_timeout: Duration::ZERO,
            ..AsyncWriteLedger::with_limits(4, 1)
        });
        let owner = ledger.admit(10, 100, 4).unwrap();
        let mut waiter = ledger.start_request(20, 200).unwrap();
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(20)),
            BarrierOutcome::TimedOut
        );
        owner.complete(Ok(()));
        waiter
            .reserve_capacity(4)
            .expect("timeout must not become shutdown");
        waiter.complete(Ok(()));
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(20)),
            BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn timeout_preserves_late_write_error_for_next_barrier() {
        let ledger = Arc::new(AsyncWriteLedger::default().with_barrier_timeout(Duration::ZERO));
        let ticket = ledger.admit(10, 100, 4).unwrap();
        let watermark = ledger.capture_inode_watermark(10);
        assert_eq!(ledger.wait_inode(watermark), BarrierOutcome::TimedOut);
        ticket.complete(Err(libc::ENOSPC));
        assert_eq!(
            ledger.wait_inode_completion(watermark),
            BarrierOutcome::Complete(None)
        );
        assert_eq!(
            ledger.wait_inode(watermark),
            BarrierOutcome::Complete(Some(libc::ENOSPC))
        );
        assert_eq!(ledger.wait_inode(watermark), BarrierOutcome::Complete(None));
        assert_eq!(ledger.metrics_snapshot().barrier_timeouts, 1);
    }

    #[test]
    fn deferred_release_runs_after_last_write_and_is_included_in_shutdown_drain() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(16, 4));
        let first = ledger.admit(10, 100, 4).unwrap();
        let last = ledger.admit(10, 100, 4).unwrap();
        ledger.close_handle(100);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        ledger.defer_release(100, move || {
            entered_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        });
        first.complete(Err(libc::ENOSPC));
        assert!(
            entered_rx.try_recv().is_err(),
            "cleanup must await every handle write"
        );
        let worker = thread::spawn(move || last.complete(Ok(())));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        ledger.shutdown();
        let drained = ledger.wait_all(Duration::ZERO);
        let forgotten = ledger.forget_handle(100);
        resume_tx.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(
            drained,
            BarrierOutcome::TimedOut,
            "shutdown must retain running cleanup"
        );
        assert!(!forgotten, "cleanup still owns the closed handle");
        assert_eq!(
            ledger.wait_all(Duration::ZERO),
            BarrierOutcome::Complete(None)
        );
        assert!(ledger.lock_state().closed_handles.is_empty());
        assert!(ledger.lock_state().errors.is_empty());
    }

    #[test]
    fn deferred_release_handles_completion_before_registration() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(16, 4));
        let ticket = ledger.admit(10, 100, 4).unwrap();
        ledger.close_handle(100);
        ticket.complete(Err(libc::EIO));
        let (sender, receiver) = mpsc::channel();
        ledger.defer_release(100, move || sender.send(()).unwrap());
        receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(ledger.lock_state().closed_handles.is_empty());
        assert!(ledger.lock_state().errors.is_empty());
    }

    #[test]
    fn defaults_preserve_global_admission_limits() {
        let ledger = AsyncWriteLedger::default();

        assert_eq!(
            ledger.limits_for_test(),
            (DEFAULT_MAX_PENDING_BYTES, DEFAULT_MAX_PENDING_WRITES)
        );
    }

    #[test]
    fn sticky_error_journal_is_bounded_without_losing_watermark_precision() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits_and_error_limit(16, 4, 2));
        let first = ledger.admit(10, 100, 4).expect("first write should admit");
        first.complete(Err(libc::ENOSPC));
        let second = ledger
            .admit(10, 100, 4)
            .expect("second write reserves the remaining error slot");
        let watermark = ledger.capture_inode_watermark(10);

        assert_eq!(
            ledger
                .start_request(10, 100)
                .expect_err("pending writes reserve journal capacity"),
            AdmissionError::ErrorJournalFull
        );
        second.complete(Err(libc::EIO));
        assert_eq!(
            ledger
                .start_request(10, 100)
                .expect_err("full sticky journal must close admission"),
            AdmissionError::ErrorJournalFull
        );
        assert_eq!(
            ledger.wait_inode(watermark),
            BarrierOutcome::Complete(Some(libc::ENOSPC)),
            "the earliest exact sticky errno remains observable"
        );

        let admitted = ledger
            .start_request(10, 100)
            .expect("consuming the bounded journal reopens admission");
        admitted.complete(Ok(()));
    }

    #[test]
    fn metrics_snapshot_reports_pending_depth_failures_and_task_drop() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(16, 4));
        let successful = ledger.admit(10, 100, 4).expect("write should admit");
        let failed = ledger.admit(10, 101, 6).expect("write should admit");
        let pending = ledger.admit(20, 200, 2).expect("write should admit");

        let snapshot = ledger.metrics_snapshot();
        assert_eq!(snapshot.pending_writes, 3);
        assert_eq!(snapshot.pending_bytes, 12);
        assert_eq!(snapshot.pending_handles, 3);
        assert_eq!(snapshot.pending_writes_per_inode.get(&10), Some(&2));
        assert_eq!(snapshot.pending_writes_per_inode.get(&20), Some(&1));
        assert_eq!(snapshot.peak_pending_bytes, 12);

        successful.complete(Ok(()));
        failed.complete(Err(libc::ENOSPC));
        drop(pending);

        let snapshot = ledger.metrics_snapshot();
        assert_eq!(snapshot.pending_writes, 0);
        assert_eq!(snapshot.completed_writes, 3);
        assert_eq!(snapshot.failed_writes, 2);
        assert_eq!(snapshot.task_failed_writes, 1);
        assert!(snapshot.snapshot_generation > 1);
    }

    #[test]
    fn inode_barrier_waits_for_pre_watermark_write_only() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let before = ledger.admit(10, 100, 4).expect("write should admit");
        let watermark = ledger.capture_inode_watermark(10);
        let after = ledger.admit(10, 100, 4).expect("later write should admit");
        let (sender, receiver) = mpsc::channel();
        let waiting_ledger = ledger.clone();
        let waiter = thread::spawn(move || {
            sender
                .send(waiting_ledger.wait_inode(watermark))
                .expect("receiver should remain live");
        });

        assert!(
            receiver.recv_timeout(SHORT_WAIT).is_err(),
            "barrier must wait for a pre-watermark write"
        );
        before.complete(Ok(()));
        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("barrier should complete without waiting for the later write"),
            BarrierOutcome::Complete(None)
        );

        after.complete(Ok(()));
        waiter.join().expect("waiter should not panic");
        let metrics = ledger.metrics_snapshot();
        assert_eq!(metrics.barrier_waits, 1);
        assert_eq!(metrics.inode_barrier_waits, 1);
        assert_eq!(metrics.handle_barrier_waits, 0);
    }

    #[test]
    fn inode_completion_wait_preserves_sticky_error_for_barrier() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.start_request(10, 100).unwrap();
        let watermark = ledger.capture_inode_watermark(10);

        ticket.complete(Err(libc::ENOSPC));
        assert_eq!(
            ledger.wait_inode_completion(watermark),
            BarrierOutcome::Complete(None)
        );

        assert_eq!(
            ledger.wait_inode(watermark),
            BarrierOutcome::Complete(Some(libc::ENOSPC))
        );
    }

    #[test]
    fn handle_barrier_waits_for_pre_watermark_write_only() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let before = ledger.admit(10, 100, 4).expect("write should admit");
        let watermark = ledger.capture_handle_watermark(100);
        let after = ledger.admit(10, 100, 4).expect("later write should admit");
        let (sender, receiver) = mpsc::channel();
        let waiting_ledger = ledger.clone();
        let waiter = thread::spawn(move || {
            sender
                .send(waiting_ledger.wait_handle(watermark))
                .expect("receiver should remain live");
        });

        assert!(
            receiver.recv_timeout(SHORT_WAIT).is_err(),
            "barrier must wait for a pre-watermark write"
        );
        before.complete(Ok(()));
        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("barrier should complete without waiting for the later write"),
            BarrierOutcome::Complete(None)
        );

        after.complete(Ok(()));
        waiter.join().expect("waiter should not panic");
        let metrics = ledger.metrics_snapshot();
        assert_eq!(metrics.barrier_waits, 1);
        assert_eq!(metrics.inode_barrier_waits, 0);
        assert_eq!(metrics.handle_barrier_waits, 1);
    }

    #[test]
    fn inode_barrier_drains_registered_write_after_shutdown() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        let watermark = ledger.capture_inode_watermark(10);
        let (sender, receiver) = mpsc::channel();
        let waiting_ledger = ledger.clone();
        let waiter = thread::spawn(move || {
            sender
                .send(waiting_ledger.wait_inode(watermark))
                .expect("receiver should remain live");
        });

        ledger.shutdown();
        assert!(
            receiver.recv_timeout(SHORT_WAIT).is_err(),
            "shutdown must not abandon a registered write"
        );
        drop(ticket);
        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(1))
                .expect("dropped write should release the barrier"),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
        waiter.join().expect("waiter should not panic");
    }

    #[test]
    fn inode_barrier_ignores_pending_writes_from_other_inodes() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let own = ledger.admit(10, 100, 4).expect("own write should admit");
        let other = ledger.admit(20, 200, 4).expect("other write should admit");
        let watermark = ledger.capture_inode_watermark(10);
        own.complete(Ok(()));

        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
        assert_eq!(ledger.pending_for_test(), (1, 4));
        other.complete(Ok(()));
    }

    #[test]
    fn handle_barrier_ignores_other_handles_on_same_inode() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let own = ledger.admit(10, 100, 4).expect("own write should admit");
        let other = ledger.admit(10, 200, 4).expect("other write should admit");
        let watermark = ledger.capture_handle_watermark(100);
        own.complete(Ok(()));

        assert_eq!(
            ledger.wait_handle_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
        other.complete(Ok(()));
    }

    #[test]
    fn inode_barrier_waits_for_all_handles_on_its_inode() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let first = ledger.admit(10, 100, 4).expect("write should admit");
        let second = ledger.admit(10, 200, 4).expect("write should admit");
        let watermark = ledger.capture_inode_watermark(10);
        first.complete(Ok(()));

        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::TimedOut
        );
        second.complete(Ok(()));
        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn post_watermark_write_does_not_extend_inode_barrier() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let before = ledger.admit(10, 100, 4).expect("write should admit");
        let watermark = ledger.capture_inode_watermark(10);
        let after = ledger.admit(10, 100, 4).expect("later write should admit");
        before.complete(Ok(()));

        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
        after.complete(Ok(()));
    }

    #[test]
    fn post_watermark_write_does_not_extend_handle_barrier() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let before = ledger.admit(10, 100, 4).expect("write should admit");
        let watermark = ledger.capture_handle_watermark(100);
        let after = ledger.admit(10, 100, 4).expect("later write should admit");
        before.complete(Ok(()));

        assert_eq!(
            ledger.wait_handle_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
        after.complete(Ok(()));
    }

    #[test]
    fn inode_barrier_handles_out_of_order_completion() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let first = ledger.admit(10, 100, 4).expect("write should admit");
        let second = ledger.admit(10, 100, 4).expect("write should admit");
        let watermark = ledger.capture_inode_watermark(10);
        second.complete(Ok(()));

        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::TimedOut
        );
        first.complete(Ok(()));
        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn inode_error_consumption_prevents_duplicate_handle_error() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        let inode_watermark = ledger.capture_inode_watermark(10);
        let handle_watermark = ledger.capture_handle_watermark(100);
        ticket.complete(Err(libc::ENOSPC));

        assert_eq!(
            ledger.wait_inode_timed(inode_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(Some(libc::ENOSPC))
        );
        assert_eq!(
            ledger.wait_handle_timed(handle_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn barrier_returns_earliest_error_without_cross_inode_leakage() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let first = ledger.admit(10, 100, 4).expect("write should admit");
        let second = ledger.admit(10, 200, 4).expect("write should admit");
        let other = ledger.admit(20, 300, 4).expect("write should admit");
        let own_watermark = ledger.capture_inode_watermark(10);
        let other_watermark = ledger.capture_inode_watermark(20);
        first.complete(Err(libc::ENOSPC));
        second.complete(Err(libc::EIO));
        other.complete(Err(libc::EROFS));

        assert_eq!(
            ledger.wait_inode_timed(own_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(Some(libc::ENOSPC))
        );
        assert_eq!(
            ledger.wait_inode_timed(own_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
        assert_eq!(
            ledger.wait_inode_timed(other_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(Some(libc::EROFS))
        );
    }

    #[test]
    fn close_handle_rejects_new_tickets_and_waits_for_preceding_writes() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        let watermark = ledger.close_handle(100);

        let error = ledger
            .admit(10, 100, 4)
            .expect_err("closed handle must fail");
        assert_eq!(error, AdmissionError::HandleClosed);
        assert_eq!(error.errno(), libc::EBADF);
        assert_eq!(
            ledger.wait_handle_timed(watermark, SHORT_WAIT),
            BarrierOutcome::TimedOut
        );
        ticket.complete(Ok(()));
        assert_eq!(
            ledger.wait_handle_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn close_handle_is_idempotent_and_forget_allows_handle_reuse() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let first = ledger.close_handle(100);
        let second = ledger.close_handle(100);

        assert_eq!(first, second);
        assert_eq!(
            ledger
                .admit(10, 100, 4)
                .expect_err("closed handle must fail"),
            AdmissionError::HandleClosed
        );
        assert!(ledger.forget_handle(100));
        ledger
            .admit(10, 100, 4)
            .expect("forgotten handle identifier may be reused")
            .complete(Ok(()));
    }

    #[test]
    fn inode_watermark_includes_request_blocked_before_capacity_reservation() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(4, 1));
        let capacity_owner = ledger.admit(20, 200, 4).expect("write should admit");
        let mut blocked = ledger
            .start_request(10, 100)
            .expect("request should register");
        let watermark = ledger.capture_inode_watermark(10);
        let (sender, receiver) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let result = blocked.reserve_capacity(4);
            sender
                .send((blocked, result))
                .expect("receiver should remain live");
        });

        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::TimedOut
        );
        assert_eq!(ledger.admission_for_test(), (1, 4));
        capacity_owner.complete(Ok(()));
        let (blocked, reservation) = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("capacity waiter should wake");
        reservation.expect("pre-watermark request should reserve capacity");
        blocked.complete(Ok(()));
        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
        waiter.join().expect("waiter should not panic");
    }

    #[test]
    fn registered_request_cannot_slip_through_close_and_handle_reuse() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(4, 1));
        let capacity_owner = ledger.admit(20, 200, 4).expect("write should admit");
        let mut blocked = ledger
            .start_request(10, 100)
            .expect("request should register");
        let watermark = ledger.close_handle(100);

        assert!(!ledger.forget_handle(100));
        assert_eq!(
            ledger
                .admit(10, 100, 4)
                .expect_err("closed handle must fail"),
            AdmissionError::HandleClosed
        );

        let (sender, receiver) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let result = blocked.reserve_capacity(4);
            sender
                .send((blocked, result))
                .expect("receiver should remain live");
        });
        capacity_owner.complete(Ok(()));
        let (blocked, reservation) = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("capacity waiter should wake");
        reservation.expect("request registered before close must proceed");
        blocked.complete(Ok(()));

        assert_eq!(
            ledger.wait_handle_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
        assert!(ledger.forget_handle(100));
        ledger
            .admit(10, 100, 4)
            .expect("released handle identifier may be reused")
            .complete(Ok(()));
        waiter.join().expect("waiter should not panic");
    }

    #[test]
    fn handle_cannot_be_reused_until_its_sticky_error_is_consumed() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let failed = ledger.admit(10, 100, 4).expect("write should admit");
        let old_watermark = ledger.close_handle(100);
        failed.complete(Err(libc::ENOSPC));

        assert!(!ledger.forget_handle(100));
        assert_eq!(
            ledger
                .admit(10, 100, 4)
                .expect_err("handle with an unconsumed error must remain closed"),
            AdmissionError::HandleClosed
        );
        assert_eq!(
            ledger.wait_handle_timed(old_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(Some(libc::ENOSPC))
        );
        assert!(ledger.forget_handle(100));

        let new_lifecycle = ledger
            .admit(10, 100, 4)
            .expect("handle identifier may be reused after error consumption");
        let new_watermark = ledger.capture_handle_watermark(100);
        new_lifecycle.complete(Ok(()));
        assert_eq!(
            ledger.wait_handle_timed(new_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn dropped_ticket_records_eio_and_releases_both_indexes() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        let inode_watermark = ledger.capture_inode_watermark(10);
        let handle_watermark = ledger.capture_handle_watermark(100);
        drop(ticket);

        assert_eq!(ledger.pending_for_test(), (0, 0));
        assert_eq!(
            ledger.wait_handle_timed(handle_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
        assert_eq!(
            ledger.wait_inode_timed(inode_watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn shutdown_rejects_new_tickets_and_wakes_waiters() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        ledger.shutdown();

        let error = ledger
            .admit(20, 200, 4)
            .expect_err("shutdown admission must fail");
        assert_eq!(error, AdmissionError::ShuttingDown);
        assert_eq!(error.errno(), libc::EIO);
        assert_eq!(ledger.wait_all(SHORT_WAIT), BarrierOutcome::TimedOut);
        ticket.complete(Ok(()));
        assert_eq!(ledger.wait_all(SHORT_WAIT), BarrierOutcome::Complete(None));
    }

    #[test]
    fn shutdown_abort_cancels_tracked_task_and_drains_ticket() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create test runtime");
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        let sequence = ticket.sequence();
        let watermark = ledger.capture_inode_watermark(10);
        let task = runtime.spawn(async move {
            let _ticket = ticket;
            std::future::pending::<()>().await;
        });
        assert!(ledger.track_task(sequence, task.abort_handle()));

        ledger.shutdown();
        assert_eq!(ledger.abort_pending_tasks(), 1);
        let error = runtime
            .block_on(task)
            .expect_err("aborted task should be cancelled");
        assert!(error.is_cancelled());
        assert_eq!(
            ledger.wait_inode(watermark),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
        assert_eq!(ledger.abort_pending_tasks(), 0);
    }

    #[test]
    fn task_registered_during_first_shutdown_drain_waits_for_explicit_cancellation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create test runtime");
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        let sequence = ticket.sequence();
        let watermark = ledger.capture_inode_watermark(10);
        let task = runtime.spawn(async move {
            let _ticket = ticket;
            std::future::pending::<()>().await;
        });

        ledger.shutdown();
        assert!(ledger.track_task(sequence, task.abort_handle()));
        runtime.block_on(tokio::task::yield_now());
        assert!(
            !task.is_finished(),
            "shutdown alone must preserve the first drain window"
        );
        assert_eq!(ledger.abort_pending_tasks(), 1);
        let error = runtime
            .block_on(task)
            .expect_err("explicit cancellation must abort the queued task");
        assert!(error.is_cancelled());
        assert_eq!(
            ledger.wait_inode(watermark),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
    }

    #[test]
    fn explicit_cancellation_before_task_registration_aborts_on_registration() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create test runtime");
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        let sequence = ticket.sequence();
        let watermark = ledger.capture_inode_watermark(10);
        let task = runtime.spawn(async move {
            let _ticket = ticket;
            std::future::pending::<()>().await;
        });

        ledger.shutdown();
        assert_eq!(ledger.abort_pending_tasks(), 1);
        assert!(ledger.track_task(sequence, task.abort_handle()));
        let error = runtime
            .block_on(task)
            .expect_err("late registration must apply requested cancellation");
        assert!(error.is_cancelled());
        assert_eq!(
            ledger.wait_inode(watermark),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
    }

    #[test]
    fn task_is_not_aborted_after_entering_non_cancellable_write_phase() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("create test runtime");
        let ledger = Arc::new(AsyncWriteLedger::default());
        let ticket = ledger.admit(10, 100, 4).expect("write should admit");
        let sequence = ticket.sequence();
        let task = runtime.spawn(std::future::pending::<()>());
        assert!(ledger.track_task(sequence, task.abort_handle()));
        assert!(
            ledger.mark_task_non_cancellable(sequence),
            "pending task should enter the VFS mutation phase"
        );

        ledger.shutdown();
        assert_eq!(ledger.abort_pending_tasks(), 0);
        runtime.block_on(tokio::task::yield_now());
        assert!(
            !task.is_finished(),
            "post-transition task must survive shutdown cancellation"
        );

        task.abort();
        let error = runtime
            .block_on(task)
            .expect_err("test cleanup should abort the detached pending future");
        assert!(error.is_cancelled());
        ticket.complete(Ok(()));
    }

    #[test]
    fn admission_applies_backpressure_until_capacity_is_released() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(4, 1));
        let first = ledger.admit(10, 100, 4).expect("write should admit");
        let (sender, receiver) = mpsc::channel();
        let waiting_ledger = ledger.clone();
        let waiter = thread::spawn(move || {
            let ticket = waiting_ledger.admit(20, 200, 4);
            sender.send(ticket).expect("receiver should remain live");
        });

        assert!(receiver.recv_timeout(SHORT_WAIT).is_err());
        first.complete(Ok(()));
        receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("capacity waiter should wake")
            .expect("waiting admission should succeed")
            .complete(Ok(()));
        waiter.join().expect("waiter should not panic");
        let metrics = ledger.metrics_snapshot();
        assert_eq!(metrics.admission_waits, 1);
        assert!(metrics.admission_wait_total_nanos > 0);
    }

    #[test]
    fn admission_applies_byte_backpressure_below_request_limit() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(4, 2));
        let first = ledger.admit(10, 100, 3).expect("write should admit");
        let (sender, receiver) = mpsc::channel();
        let waiting_ledger = ledger.clone();
        let waiter = thread::spawn(move || {
            let ticket = waiting_ledger.admit(20, 200, 2);
            sender.send(ticket).expect("receiver should remain live");
        });

        assert!(receiver.recv_timeout(SHORT_WAIT).is_err());
        first.complete(Ok(()));
        receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("byte-capacity waiter should wake")
            .expect("waiting admission should succeed")
            .complete(Ok(()));
        waiter.join().expect("waiter should not panic");
    }

    #[test]
    fn capacity_admission_follows_registration_sequence() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(4, 1));
        let capacity_owner = ledger.admit(30, 300, 4).expect("write should admit");
        let mut before_watermark = ledger
            .start_request(10, 100)
            .expect("earlier request should register");
        let watermark = ledger.capture_inode_watermark(10);
        let mut after_watermark = ledger
            .start_request(20, 200)
            .expect("later request should register");
        let (sender, receiver) = mpsc::channel();
        let later_waiter = thread::spawn(move || {
            let result = after_watermark.reserve_capacity(4);
            sender
                .send((after_watermark, result))
                .expect("receiver should remain live");
        });

        assert!(receiver.recv_timeout(SHORT_WAIT).is_err());
        capacity_owner.complete(Ok(()));
        assert!(
            receiver.recv_timeout(SHORT_WAIT).is_err(),
            "later request must not overtake the earliest registered request"
        );

        before_watermark
            .reserve_capacity(4)
            .expect("earliest registered request should receive capacity");
        assert_eq!(ledger.admission_for_test(), (1, 4));
        assert!(receiver.recv_timeout(SHORT_WAIT).is_err());
        before_watermark.complete(Ok(()));
        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );

        let (after_watermark, reservation) = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("later waiter should proceed after earlier completion");
        reservation.expect("later waiter should reserve capacity");
        after_watermark.complete(Ok(()));
        later_waiter.join().expect("waiter should not panic");
    }

    #[test]
    fn capacity_admission_skips_requests_finished_before_reservation() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(4, 1));
        let owner = ledger.admit(10, 100, 4).unwrap();
        let head = ledger.start_request(20, 200).unwrap();
        let mut oversized = ledger.start_request(30, 300).unwrap();
        let later = ledger.start_request(40, 400).unwrap();
        let mut survivor = ledger.start_request(50, 500).unwrap();

        // Remove a later request before the head, then fail the next head before admission.
        later.complete(Ok(()));
        drop(head);
        assert_eq!(
            oversized.reserve_capacity(5),
            Err(AdmissionError::RequestTooLarge)
        );
        oversized.complete(Err(libc::EINVAL));
        owner.complete(Ok(()));

        let (sender, receiver) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let result = survivor.reserve_capacity(4);
            survivor.complete(Ok(()));
            sender.send(result).unwrap();
        });
        receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("removed requests must not strand the surviving waiter")
            .unwrap();
        waiter.join().unwrap();
        assert_eq!(ledger.pending_for_test(), (0, 0));
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(20)),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(30)),
            BarrierOutcome::Complete(Some(libc::EINVAL))
        );
        ledger.admit(60, 600, 4).unwrap().complete(Ok(()));
    }

    #[test]
    fn capacity_admission_drains_large_queue_with_reserved_prefix_and_cancellation_holes() {
        const QUEUE_SIZE: usize = 2048;
        let ledger = Arc::new(AsyncWriteLedger::with_limits(
            QUEUE_SIZE * 2,
            QUEUE_SIZE * 2,
        ));
        let owners = (0..QUEUE_SIZE)
            .map(|_| ledger.admit(10, 100, 1).unwrap())
            .collect::<Vec<_>>();
        let requests = (0..QUEUE_SIZE)
            .map(|_| ledger.start_request(20, 200).unwrap())
            .collect::<Vec<_>>();
        let (sender, receiver) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let mut admitted = Vec::new();
            for (index, mut request) in requests.into_iter().enumerate() {
                if index % 3 == 0 {
                    drop(request);
                } else {
                    request.reserve_capacity(1).unwrap();
                    // Repeated reservation must not advance past the next waiting request.
                    request.reserve_capacity(1).unwrap();
                    admitted.push(request);
                }
            }
            sender.send(admitted).unwrap();
        });
        let admitted = receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("large queue must drain despite cancellations and a live reserved prefix");
        waiter.join().unwrap();
        assert_eq!(admitted.len(), QUEUE_SIZE - QUEUE_SIZE.div_ceil(3));
        assert_eq!(
            ledger.admission_for_test(),
            (owners.len() + admitted.len(), owners.len() + admitted.len())
        );
        for ticket in owners.into_iter().chain(admitted) {
            ticket.complete(Ok(()));
        }
        assert_eq!(ledger.pending_for_test(), (0, 0));
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(20)),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
        ledger.admit(30, 300, 1).unwrap().complete(Ok(()));
    }

    #[test]
    fn capacity_admission_drains_concurrent_waiters_under_saturation() {
        const CONCURRENCY: usize = 16;
        const TIMEOUT: Duration = Duration::from_secs(5);

        // Saturate bytes below the write limit, then writes below the byte limit.
        for (max_bytes, max_writes) in [(4, 2), (8, 1)] {
            let ledger = Arc::new(AsyncWriteLedger::with_limits(max_bytes, max_writes));
            let owner = ledger.admit(10, 100, 4).unwrap();
            let requests = (0..CONCURRENCY)
                .map(|_| ledger.start_request(20, 200).unwrap())
                .collect::<Vec<_>>();
            let (sender, receiver) = mpsc::channel();
            let waiters = requests
                .into_iter()
                .rev()
                .map(|mut request| {
                    let sender = sender.clone();
                    thread::spawn(move || {
                        let result = request.reserve_capacity(4);
                        sender.send((request, result)).unwrap();
                    })
                })
                .collect::<Vec<_>>();
            drop(sender);

            let (state, _) = ledger
                .admission_wait_started
                .wait_timeout_while(ledger.lock_state(), TIMEOUT, |state| {
                    state.started_admission_waits < CONCURRENCY
                })
                .unwrap();
            assert_eq!(state.started_admission_waits, CONCURRENCY);
            drop(state);
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            owner.complete(Ok(()));

            for sequence in 2..=17 {
                let (request, result) = receiver
                    .recv_timeout(TIMEOUT)
                    .expect("releasing capacity must wake the next registered waiter");
                result.unwrap();
                assert_eq!(request.sequence(), sequence);
                assert_eq!(ledger.admission_for_test(), (1, 4));
                request.complete(Ok(()));
            }
            for waiter in waiters {
                waiter.join().unwrap();
            }
            assert_eq!(ledger.pending_for_test(), (0, 0));
            assert_eq!(ledger.admission_for_test(), (0, 0));
            assert_eq!(ledger.metrics_snapshot().admission_waits, 16);
        }
    }

    #[test]
    fn shutdown_wakes_blocked_admission_with_precise_error() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(4, 1));
        let first = ledger.admit(10, 100, 4).expect("write should admit");
        let mut blocked = ledger
            .start_request(20, 200)
            .expect("request should register");
        let watermark = ledger.capture_inode_watermark(20);
        let (sender, receiver) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let result = blocked.reserve_capacity(4);
            sender
                .send((blocked, result))
                .expect("receiver should remain live");
        });

        assert!(receiver.recv_timeout(SHORT_WAIT).is_err());
        ledger.shutdown();
        let (blocked, result) = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown should wake admission");
        let error = result.expect_err("shutdown admission must fail");
        assert_eq!(error, AdmissionError::ShuttingDown);
        assert_eq!(error.errno(), libc::EIO);
        drop(blocked);
        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
        first.complete(Ok(()));
        waiter.join().expect("waiter should not panic");
    }

    #[test]
    fn sequence_exhaustion_is_reported_without_wrapping() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        ledger.set_next_sequence_for_test(u64::MAX);

        let error = ledger
            .admit(10, 100, 4)
            .expect_err("sequence must not wrap");
        assert_eq!(error, AdmissionError::SequenceExhausted);
        assert_eq!(error.errno(), libc::EIO);
        assert_eq!(ledger.pending_for_test(), (0, 0));
    }

    #[test]
    fn exact_byte_limit_is_allowed_and_oversize_is_rejected() {
        let ledger = Arc::new(AsyncWriteLedger::with_limits(4, 2));
        ledger
            .admit(10, 100, 4)
            .expect("exact byte limit should admit")
            .complete(Ok(()));

        let error = ledger
            .admit(10, 100, 5)
            .expect_err("oversize request must not wait forever");
        assert_eq!(error, AdmissionError::RequestTooLarge);
        assert_eq!(error.errno(), libc::EINVAL);

        let watermark = ledger.capture_inode_watermark(10);
        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(Some(libc::EINVAL))
        );
    }

    #[test]
    fn barrier_consumes_many_pre_watermark_errors_and_returns_earliest() {
        let ledger = Arc::new(AsyncWriteLedger::default());
        let tickets = (0..32)
            .map(|fh| ledger.admit(10, fh, 1).expect("write should admit"))
            .collect::<Vec<_>>();
        let watermark = ledger.capture_inode_watermark(10);
        for (index, ticket) in tickets.into_iter().enumerate() {
            let errno = if index == 0 { libc::ENOSPC } else { libc::EIO };
            ticket.complete(Err(errno));
        }

        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(Some(libc::ENOSPC))
        );
        assert_eq!(
            ledger.wait_inode_timed(watermark, SHORT_WAIT),
            BarrierOutcome::Complete(None)
        );
    }
}

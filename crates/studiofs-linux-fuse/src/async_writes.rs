use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::task::AbortHandle;

pub(crate) const DEFAULT_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_PENDING_WRITES: usize = 1024;
const DEFAULT_MAX_RECORDED_ERRORS: usize = 8192;

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
}

#[derive(Debug)]
pub(crate) struct AsyncWriteLedger {
    max_pending_bytes: usize,
    max_pending_writes: usize,
    max_recorded_errors: usize,
    state: Mutex<LedgerState>,
    changed: Condvar,
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
            max_pending_bytes,
            max_pending_writes,
            max_recorded_errors,
            state: Mutex::new(LedgerState::default()),
            changed: Condvar::new(),
        }
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
        state.pending.insert(
            sequence,
            PendingWrite {
                ino,
                fh,
                reserved_bytes: None,
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
            || state.errors.values().any(|error| error.fh == fh)
        {
            return false;
        }
        state.closed_handles.remove(&fh);
        self.changed.notify_all();
        true
    }

    pub(crate) fn wait_inode(&self, watermark: InodeWatermark) -> BarrierOutcome {
        self.wait_scope(BarrierScope::Inode(watermark.ino), watermark.through)
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
        )
    }

    #[cfg(test)]
    fn wait_handle_timed(&self, watermark: HandleWatermark, timeout: Duration) -> BarrierOutcome {
        self.wait_scope_timed(
            BarrierScope::Handle(watermark.fh),
            watermark.through,
            timeout,
        )
    }

    pub(crate) fn wait_all(&self, timeout: Duration) -> BarrierOutcome {
        let state = self.lock_state();
        let through = state.next_sequence;
        drop(state);
        self.wait_scope_timed(BarrierScope::All, through, timeout)
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
        if !state.pending.contains_key(&sequence) {
            return false;
        }
        if state.shutting_down {
            drop(state);
            abort_handle.abort();
            return true;
        }
        state.task_abort_handles.insert(sequence, abort_handle);
        true
    }

    pub(crate) fn abort_pending_tasks(&self) -> usize {
        let abort_handles = {
            let state = self.lock_state();
            state
                .task_abort_handles
                .values()
                .cloned()
                .collect::<Vec<_>>()
        };
        for abort_handle in &abort_handles {
            abort_handle.abort();
        }
        abort_handles.len()
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
        loop {
            if state.shutting_down {
                return Err(AdmissionError::ShuttingDown);
            }
            if state
                .pending
                .get(&sequence)
                .is_some_and(|write| write.reserved_bytes.is_some())
            {
                return Ok(());
            }
            let is_next = state
                .pending
                .iter()
                .find(|(_, write)| write.reserved_bytes.is_none())
                .is_some_and(|(&next_sequence, _)| next_sequence == sequence);
            if is_next && !self.at_capacity(&state, bytes) {
                break;
            }
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
        if let Some(write) = state.pending.get_mut(&sequence) {
            write.reserved_bytes = Some(bytes);
        }
        self.changed.notify_all();
        Ok(())
    }

    fn wait_scope(&self, scope: BarrierScope, through: u64) -> BarrierOutcome {
        let mut state = self.lock_state();
        while scope.has_pending(&state, through) {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        BarrierOutcome::Complete(scope.consume_errors(&mut state, through))
    }

    fn wait_scope_timed(
        &self,
        scope: BarrierScope,
        through: u64,
        timeout: Duration,
    ) -> BarrierOutcome {
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let mut state = self.lock_state();
        loop {
            if !scope.has_pending(&state, through) {
                let errno = scope.consume_errors(&mut state, through);
                return BarrierOutcome::Complete(errno);
            }

            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return BarrierOutcome::TimedOut;
            };
            if remaining.is_zero() {
                return BarrierOutcome::TimedOut;
            }
            let (next_state, wait_result) = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next_state;
            if wait_result.timed_out() && scope.has_pending(&state, through) {
                return BarrierOutcome::TimedOut;
            }
        }
    }

    fn finish(&self, sequence: u64, result: Result<(), i32>) {
        let mut state = self.lock_state();
        let Some(write) = state.pending.remove(&sequence) else {
            return;
        };
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
        self.changed.notify_all();
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

#[derive(Clone, Copy, Debug)]
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
            Self::All => return state.pending.range(..=through).next().is_some(),
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
        self.ledger.finish(self.sequence, result);
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
            self.ledger.finish(self.sequence, Err(libc::EIO));
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
    fn untimed_inode_barrier_waits_for_pre_watermark_write_only() {
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
    }

    #[test]
    fn untimed_handle_barrier_waits_for_pre_watermark_write_only() {
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
    }

    #[test]
    fn untimed_inode_barrier_drains_registered_write_after_shutdown() {
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
    fn task_registered_after_shutdown_is_aborted_immediately() {
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
        assert_eq!(
            ledger.abort_pending_tasks(),
            0,
            "the spawn→registration window has no tracked handle yet"
        );
        assert!(ledger.track_task(sequence, task.abort_handle()));
        let error = runtime
            .block_on(task)
            .expect_err("late registration must observe shutdown and abort");
        assert!(error.is_cancelled());
        assert_eq!(
            ledger.wait_inode(watermark),
            BarrierOutcome::Complete(Some(libc::EIO))
        );
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

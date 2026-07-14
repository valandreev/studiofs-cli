use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LinuxFuseWriteWatermark {
    fh: u64,
    sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LinuxFuseAsyncWriteCompletion {
    Success,
    Failed(i32),
    Cancelled,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct LinuxFuseAsyncWriteStats {
    pub(crate) snapshot_generation: u64,
    pub(crate) active_requests: usize,
    pub(crate) admission_waiters: usize,
    pub(crate) pending_handles: usize,
    pub(crate) pending_writes: usize,
    pub(crate) pending_bytes: usize,
    pub(crate) peak_pending_bytes: usize,
    pub(crate) max_handle_pending_writes: usize,
    pub(crate) peak_handle_pending_writes: usize,
    pub(crate) completed_writes: u64,
    pub(crate) failed_writes: u64,
    pub(crate) task_failed_writes: u64,
    pub(crate) cancelled_writes: u64,
    pub(crate) admission_waits: u64,
    pub(crate) admission_wait_total_ms: f64,
    pub(crate) barrier_waits: u64,
    pub(crate) barrier_wait_total_ms: f64,
}

#[derive(Default, Debug)]
struct HandleLedger {
    next_sequence: u64,
    closing: bool,
    pending: BTreeSet<u64>,
    unreported_errors: BTreeMap<u64, i32>,
}

#[derive(Clone, Copy, Debug)]
struct LinuxFuseAsyncWriteLimits {
    max_pending_bytes: usize,
    max_pending_writes: usize,
}

#[derive(Debug, Default)]
struct LinuxFuseAsyncWriteState {
    shutting_down: bool,
    stats: LinuxFuseAsyncWriteStats,
    handles: HashMap<u64, HandleLedger>,
}

pub(crate) struct LinuxFuseAsyncWrites {
    limits: LinuxFuseAsyncWriteLimits,
    state: Mutex<LinuxFuseAsyncWriteState>,
    changed: Condvar,
    task_cancellation: CancellationToken,
}

pub(crate) struct LinuxFuseRequestTicket {
    tracker: Arc<LinuxFuseAsyncWrites>,
    fh: u64,
    sequence: u64,
    completed: bool,
}

pub(crate) struct LinuxFuseWriteReservation {
    tracker: Arc<LinuxFuseAsyncWrites>,
    bytes: usize,
    released: bool,
}

impl LinuxFuseAsyncWrites {
    pub(crate) fn new(max_pending_bytes: usize, max_pending_writes: usize) -> Self {
        Self {
            limits: LinuxFuseAsyncWriteLimits {
                max_pending_bytes,
                max_pending_writes,
            },
            state: Mutex::new(LinuxFuseAsyncWriteState::default()),
            changed: Condvar::new(),
            task_cancellation: CancellationToken::new(),
        }
    }

    pub(crate) fn start_request(self: &Arc<Self>, fh: u64) -> Result<LinuxFuseRequestTicket, i32> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if state.shutting_down {
            return Err(libc::EIO);
        }
        if state.handles.get(&fh).is_some_and(|ledger| ledger.closing) {
            return Err(libc::EBADF);
        }

        bump_generation(&mut state);
        let sequence = {
            let ledger = state.handles.entry(fh).or_default();
            ledger.next_sequence += 1;
            ledger.pending.insert(ledger.next_sequence);
            ledger.next_sequence
        };
        state.stats.active_requests += 1;
        refresh_handle_gauges(&mut state);

        Ok(LinuxFuseRequestTicket {
            tracker: Arc::clone(self),
            fh,
            sequence,
            completed: false,
        })
    }

    pub(crate) fn admit(
        self: &Arc<Self>,
        bytes: usize,
        on_wait: impl FnOnce(LinuxFuseAsyncWriteStats),
    ) -> Result<LinuxFuseWriteReservation, i32> {
        if bytes > self.limits.max_pending_bytes {
            return Err(libc::E2BIG);
        }

        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let mut on_wait = Some(on_wait);
        let mut wait_started = None;

        loop {
            if state.shutting_down {
                if let Some(started) = wait_started {
                    finish_admission_wait(&mut state, started);
                }
                return Err(libc::EIO);
            }

            let bytes_exhausted = bytes
                > self
                    .limits
                    .max_pending_bytes
                    .saturating_sub(state.stats.pending_bytes);
            let writes_exhausted = state.stats.pending_writes >= self.limits.max_pending_writes;
            if !bytes_exhausted && !writes_exhausted {
                if let Some(started) = wait_started {
                    finish_admission_wait(&mut state, started);
                }
                bump_generation(&mut state);
                state.stats.pending_writes += 1;
                state.stats.pending_bytes += bytes;
                state.stats.peak_pending_bytes = state
                    .stats
                    .peak_pending_bytes
                    .max(state.stats.pending_bytes);
                return Ok(LinuxFuseWriteReservation {
                    tracker: Arc::clone(self),
                    bytes,
                    released: false,
                });
            }

            if wait_started.is_none() {
                let started = Instant::now();
                bump_generation(&mut state);
                state.stats.admission_waiters += 1;
                state.stats.admission_waits += 1;
                let waiting_stats = state.stats.clone();
                wait_started = Some(started);
                drop(state);
                if let Some(observer) = on_wait.take() {
                    observer(waiting_stats);
                }
                state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            } else {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|err| err.into_inner());
            }
        }
    }

    pub(crate) fn watermark(&self, fh: u64) -> LinuxFuseWriteWatermark {
        let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        LinuxFuseWriteWatermark {
            fh,
            sequence: state
                .handles
                .get(&fh)
                .map_or(0, |ledger| ledger.next_sequence),
        }
    }

    pub(crate) fn close_handle(&self, fh: u64) -> LinuxFuseWriteWatermark {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if !state.handles.get(&fh).is_some_and(|ledger| ledger.closing) {
            bump_generation(&mut state);
            state.handles.entry(fh).or_default().closing = true;
        }
        LinuxFuseWriteWatermark {
            fh,
            sequence: state
                .handles
                .get(&fh)
                .map_or(0, |ledger| ledger.next_sequence),
        }
    }

    pub(crate) fn wait_through(&self, watermark: LinuxFuseWriteWatermark) -> Result<(), i32> {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let mut wait_started = None;

        while state.handles.get(&watermark.fh).is_some_and(|ledger| {
            ledger
                .pending
                .first()
                .is_some_and(|sequence| *sequence <= watermark.sequence)
        }) {
            if wait_started.is_none() {
                let started = Instant::now();
                bump_generation(&mut state);
                state.stats.barrier_waits += 1;
                wait_started = Some(started);
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|err| err.into_inner());
        }

        if let Some(started) = wait_started {
            bump_generation(&mut state);
            state.stats.barrier_wait_total_ms += elapsed_ms(started);
        }

        let first_error = state.handles.get(&watermark.fh).and_then(|ledger| {
            ledger
                .unreported_errors
                .range(..=watermark.sequence)
                .next()
                .map(|(sequence, errno)| (*sequence, *errno))
        });
        if let Some((sequence, errno)) = first_error {
            bump_generation(&mut state);
            if let Some(ledger) = state.handles.get_mut(&watermark.fh) {
                ledger.unreported_errors.remove(&sequence);
            }
            return Err(errno);
        }

        Ok(())
    }

    pub(crate) fn forget_handle(&self, fh: u64) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if state
            .handles
            .get(&fh)
            .is_some_and(|ledger| ledger.pending.is_empty())
        {
            bump_generation(&mut state);
            state.handles.remove(&fh);
            refresh_handle_gauges(&mut state);
        }
    }

    pub(crate) fn begin_shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        if !state.shutting_down {
            bump_generation(&mut state);
            state.shutting_down = true;
        }
        drop(state);
        self.changed.notify_all();
    }

    pub(crate) fn request_cancellation(&self) {
        if !self.task_cancellation.is_cancelled() {
            let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
            bump_generation(&mut state);
        }
        self.task_cancellation.cancel();
        self.changed.notify_all();
    }

    pub(crate) fn task_cancellation_token(&self) -> CancellationToken {
        self.task_cancellation.clone()
    }

    pub(crate) fn wait_idle(&self, timeout: Duration) -> bool {
        let state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let result = self
            .changed
            .wait_timeout_while(state, timeout, |state| {
                state.stats.active_requests != 0 || state.stats.pending_writes != 0
            })
            .unwrap_or_else(|err| err.into_inner());
        result.0.stats.active_requests == 0 && result.0.stats.pending_writes == 0
    }

    pub(crate) fn stats(&self) -> LinuxFuseAsyncWriteStats {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .stats
            .clone()
    }

    fn finish_request(
        &self,
        fh: u64,
        sequence: u64,
        completion: LinuxFuseAsyncWriteCompletion,
        task_failed: bool,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let is_pending = state
            .handles
            .get(&fh)
            .is_some_and(|ledger| ledger.pending.contains(&sequence));
        if is_pending {
            bump_generation(&mut state);
            let error = if task_failed {
                state.stats.task_failed_writes += 1;
                Some(libc::EIO)
            } else {
                match completion {
                    LinuxFuseAsyncWriteCompletion::Success => {
                        state.stats.completed_writes += 1;
                        None
                    }
                    LinuxFuseAsyncWriteCompletion::Failed(errno) => {
                        state.stats.failed_writes += 1;
                        Some(errno)
                    }
                    LinuxFuseAsyncWriteCompletion::Cancelled => {
                        state.stats.cancelled_writes += 1;
                        Some(libc::EIO)
                    }
                }
            };
            if let Some(ledger) = state.handles.get_mut(&fh) {
                ledger.pending.remove(&sequence);
                if let Some(errno) = error {
                    ledger.unreported_errors.insert(sequence, errno);
                }
            }
            state.stats.active_requests -= 1;
            refresh_handle_gauges(&mut state);
        }
        drop(state);
        self.changed.notify_all();
    }
}

impl LinuxFuseRequestTicket {
    pub(crate) fn complete(mut self, completion: LinuxFuseAsyncWriteCompletion) {
        self.completed = true;
        self.tracker
            .finish_request(self.fh, self.sequence, completion, false);
    }
}

impl Drop for LinuxFuseRequestTicket {
    fn drop(&mut self) {
        if !self.completed {
            self.completed = true;
            self.tracker.finish_request(
                self.fh,
                self.sequence,
                LinuxFuseAsyncWriteCompletion::Failed(libc::EIO),
                true,
            );
        }
    }
}

impl Drop for LinuxFuseWriteReservation {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let mut state = self
            .tracker
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        bump_generation(&mut state);
        state.stats.pending_writes -= 1;
        state.stats.pending_bytes -= self.bytes;
        drop(state);
        self.tracker.changed.notify_all();
    }
}

fn bump_generation(state: &mut LinuxFuseAsyncWriteState) {
    state.stats.snapshot_generation += 1;
}

fn refresh_handle_gauges(state: &mut LinuxFuseAsyncWriteState) {
    let pending_handles = state
        .handles
        .values()
        .filter(|ledger| !ledger.pending.is_empty())
        .count();
    let max_handle_pending_writes = state
        .handles
        .values()
        .map(|ledger| ledger.pending.len())
        .max()
        .unwrap_or(0);
    state.stats.pending_handles = pending_handles;
    state.stats.max_handle_pending_writes = max_handle_pending_writes;
    state.stats.peak_handle_pending_writes = state
        .stats
        .peak_handle_pending_writes
        .max(max_handle_pending_writes);
}

fn finish_admission_wait(state: &mut LinuxFuseAsyncWriteState, started: Instant) {
    bump_generation(state);
    state.stats.admission_waiters -= 1;
    state.stats.admission_wait_total_ms += elapsed_ms(started);
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::{LinuxFuseAsyncWriteCompletion, LinuxFuseAsyncWrites};

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);

    fn assert_gauges(
        writes: &LinuxFuseAsyncWrites,
        expected: (usize, usize, usize, usize, usize, usize),
    ) {
        let stats = writes.stats();
        assert_eq!(
            (
                stats.active_requests,
                stats.admission_waiters,
                stats.pending_handles,
                stats.pending_writes,
                stats.pending_bytes,
                stats.max_handle_pending_writes,
            ),
            expected,
            "unexpected raw-request/admission gauges: {stats:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn barrier_for_one_handle_does_not_wait_for_another_handle() {
        timeout(TEST_TIMEOUT, async {
            let writes = Arc::new(LinuxFuseAsyncWrites::new(16, 2));
            let first = writes.start_request(1).expect("start first handle");
            let unrelated = writes.start_request(2).expect("start unrelated handle");
            let watermark = writes.watermark(1);
            assert_gauges(&writes, (2, 0, 2, 0, 0, 1));

            let (barrier_tx, barrier_rx) = oneshot::channel();
            let barrier_writes = Arc::clone(&writes);
            tokio::task::spawn_blocking(move || {
                let _ = barrier_tx.send(barrier_writes.wait_through(watermark));
            });

            first.complete(LinuxFuseAsyncWriteCompletion::Success);
            assert_eq!(barrier_rx.await.expect("barrier result"), Ok(()));
            assert_gauges(&writes, (1, 0, 1, 0, 0, 1));

            unrelated.complete(LinuxFuseAsyncWriteCompletion::Success);
            assert_gauges(&writes, (0, 0, 0, 0, 0, 0));
        })
        .await
        .expect("handle-scoped barrier timed out");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_after_a_captured_watermark_do_not_extend_the_barrier() {
        timeout(TEST_TIMEOUT, async {
            let writes = Arc::new(LinuxFuseAsyncWrites::new(16, 2));
            let captured = writes.start_request(7).expect("start captured write");
            let watermark = writes.watermark(7);
            let later = writes.start_request(7).expect("start later write");
            assert_gauges(&writes, (2, 0, 1, 0, 0, 2));

            let (barrier_tx, barrier_rx) = oneshot::channel();
            let barrier_writes = Arc::clone(&writes);
            tokio::task::spawn_blocking(move || {
                let _ = barrier_tx.send(barrier_writes.wait_through(watermark));
            });

            captured.complete(LinuxFuseAsyncWriteCompletion::Success);
            assert_eq!(barrier_rx.await.expect("barrier result"), Ok(()));
            assert_gauges(&writes, (1, 0, 1, 0, 0, 1));

            later.complete(LinuxFuseAsyncWriteCompletion::Success);
            assert_gauges(&writes, (0, 0, 0, 0, 0, 0));
        })
        .await
        .expect("fixed-watermark barrier timed out");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn out_of_order_completion_waits_for_every_sequence_through_watermark() {
        timeout(TEST_TIMEOUT, async {
            let writes = Arc::new(LinuxFuseAsyncWrites::new(16, 2));
            let first = writes.start_request(11).expect("start sequence one");
            let second = writes.start_request(11).expect("start sequence two");
            let watermark = writes.watermark(11);
            assert_gauges(&writes, (2, 0, 1, 0, 0, 2));

            let (barrier_tx, mut barrier_rx) = oneshot::channel();
            let barrier_writes = Arc::clone(&writes);
            tokio::task::spawn_blocking(move || {
                let _ = barrier_tx.send(barrier_writes.wait_through(watermark));
            });
            while writes.stats().barrier_waits == 0 {
                tokio::task::yield_now().await;
            }

            second.complete(LinuxFuseAsyncWriteCompletion::Success);
            assert_gauges(&writes, (1, 0, 1, 0, 0, 1));
            assert!(
                timeout(Duration::from_millis(50), &mut barrier_rx)
                    .await
                    .is_err(),
                "barrier released before sequence one completed"
            );

            first.complete(LinuxFuseAsyncWriteCompletion::Success);
            assert_eq!(barrier_rx.await.expect("barrier result"), Ok(()));
            assert_gauges(&writes, (0, 0, 0, 0, 0, 0));
        })
        .await
        .expect("out-of-order completion test timed out");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn first_handle_error_is_returned_once_and_does_not_cross_handles() {
        timeout(TEST_TIMEOUT, async {
            let writes = Arc::new(LinuxFuseAsyncWrites::new(16, 2));
            let failed = writes.start_request(20).expect("start failing handle");
            let successful = writes.start_request(21).expect("start successful handle");
            let failed_watermark = writes.close_handle(20);
            let successful_watermark = writes.watermark(21);
            assert_eq!(writes.start_request(20).err(), Some(libc::EBADF));

            failed.complete(LinuxFuseAsyncWriteCompletion::Failed(libc::EIO));
            successful.complete(LinuxFuseAsyncWriteCompletion::Success);
            assert_gauges(&writes, (0, 0, 0, 0, 0, 0));

            assert_eq!(writes.wait_through(successful_watermark), Ok(()));
            assert_eq!(writes.wait_through(failed_watermark), Err(libc::EIO));
            assert_eq!(writes.wait_through(failed_watermark), Ok(()));

            let stats = writes.stats();
            assert_eq!(stats.completed_writes, 1);
            assert_eq!(stats.failed_writes, 1);
            writes.forget_handle(20);
        })
        .await
        .expect("handle-local error test timed out");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_ticket_records_task_failure_and_releases_capacity_once() {
        timeout(TEST_TIMEOUT, async {
            let writes = Arc::new(LinuxFuseAsyncWrites::new(16, 1));
            let ticket = writes.start_request(30).expect("start request");
            let watermark = writes.watermark(30);
            assert_gauges(&writes, (1, 0, 1, 0, 0, 1));

            let reservation = writes.admit(8, |_| {}).expect("admit write");
            assert_gauges(&writes, (1, 0, 1, 1, 8, 1));
            let admitted = writes.stats();
            assert_eq!(admitted.peak_pending_bytes, 8);
            assert_eq!(admitted.peak_handle_pending_writes, 1);

            drop(ticket);
            assert_gauges(&writes, (0, 0, 0, 1, 8, 0));
            assert_eq!(writes.stats().task_failed_writes, 1);
            assert_eq!(writes.wait_through(watermark), Err(libc::EIO));
            assert_eq!(writes.wait_through(watermark), Ok(()));

            drop(reservation);
            assert_gauges(&writes, (0, 0, 0, 0, 0, 0));
            assert!(writes.wait_idle(Duration::ZERO));
        })
        .await
        .expect("dropped-ticket accounting test timed out");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_rejects_new_admission_and_wakes_blocked_admission() {
        timeout(TEST_TIMEOUT, async {
            let writes = Arc::new(LinuxFuseAsyncWrites::new(8, 1));
            let admitted_ticket = writes.start_request(40).expect("start admitted request");
            assert_gauges(&writes, (1, 0, 1, 0, 0, 1));
            let reservation = writes.admit(8, |_| {}).expect("fill admission budget");
            assert_gauges(&writes, (1, 0, 1, 1, 8, 1));
            let waiting_ticket = writes.start_request(41).expect("start waiting request");
            assert_gauges(&writes, (2, 0, 2, 1, 8, 1));

            let (observed_tx, observed_rx) = oneshot::channel();
            let waiting_writes = Arc::clone(&writes);
            let waiter = tokio::task::spawn_blocking(move || {
                waiting_writes.admit(1, |stats| {
                    let _ = observed_tx.send(stats);
                })
            });

            let observed = observed_rx.await.expect("admission wait observation");
            assert_eq!(observed.admission_waiters, 1);
            assert_eq!(observed.active_requests, 2);
            assert_eq!(observed.pending_writes, 1);
            assert_eq!(observed.pending_bytes, 8);
            assert_gauges(&writes, (2, 1, 2, 1, 8, 1));

            writes.begin_shutdown();
            match waiter.await.expect("admission waiter join") {
                Err(errno) => assert_eq!(errno, libc::EIO),
                Ok(_) => panic!("shutdown unexpectedly admitted a write"),
            }
            assert_gauges(&writes, (2, 0, 2, 1, 8, 1));
            assert_eq!(writes.start_request(42).err(), Some(libc::EIO));
            match writes.admit(1, |_| {}) {
                Err(errno) => assert_eq!(errno, libc::EIO),
                Ok(_) => panic!("shutdown unexpectedly admitted a new write"),
            }

            waiting_ticket.complete(LinuxFuseAsyncWriteCompletion::Cancelled);
            assert_gauges(&writes, (1, 0, 1, 1, 8, 1));
            admitted_ticket.complete(LinuxFuseAsyncWriteCompletion::Success);
            assert_gauges(&writes, (0, 0, 0, 1, 8, 0));
            drop(reservation);
            assert_gauges(&writes, (0, 0, 0, 0, 0, 0));
            assert!(writes.wait_idle(Duration::ZERO));
            let cancellation = writes.task_cancellation_token();
            assert!(!cancellation.is_cancelled());
            writes.request_cancellation();
            assert!(cancellation.is_cancelled());
        })
        .await
        .expect("shutdown admission test timed out");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn request_waiting_for_admission_keeps_tracker_non_idle() {
        timeout(TEST_TIMEOUT, async {
            let writes = Arc::new(LinuxFuseAsyncWrites::new(8, 1));
            let admitted_ticket = writes.start_request(50).expect("start admitted request");
            let reservation = writes.admit(8, |_| {}).expect("fill admission budget");
            let waiting_ticket = writes.start_request(51).expect("start waiting request");

            let (observed_tx, observed_rx) = oneshot::channel();
            let waiting_writes = Arc::clone(&writes);
            let waiter = tokio::task::spawn_blocking(move || {
                waiting_writes.admit(1, |stats| {
                    let _ = observed_tx.send(stats);
                })
            });
            let observed = observed_rx.await.expect("admission wait observation");
            assert_eq!(observed.active_requests, 2);
            assert_eq!(observed.admission_waiters, 1);
            assert_gauges(&writes, (2, 1, 2, 1, 8, 1));
            assert!(!writes.wait_idle(Duration::from_millis(20)));

            writes.begin_shutdown();
            match waiter.await.expect("admission waiter join") {
                Err(errno) => assert_eq!(errno, libc::EIO),
                Ok(_) => panic!("shutdown unexpectedly admitted a write"),
            }
            waiting_ticket.complete(LinuxFuseAsyncWriteCompletion::Failed(libc::EIO));
            admitted_ticket.complete(LinuxFuseAsyncWriteCompletion::Success);
            drop(reservation);
            assert!(writes.wait_idle(Duration::ZERO));
            assert_gauges(&writes, (0, 0, 0, 0, 0, 0));
        })
        .await
        .expect("raw admission waiter idle test timed out");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn barrier_captures_request_waiting_for_admission() {
        timeout(TEST_TIMEOUT, async {
            let writes = Arc::new(LinuxFuseAsyncWrites::new(8, 1));
            let holder_ticket = writes.start_request(60).expect("start budget holder");
            let reservation = writes.admit(8, |_| {}).expect("fill admission budget");
            let waiting_ticket = writes.start_request(61).expect("start waiting request");
            let watermark = writes.watermark(61);

            let (observed_tx, observed_rx) = oneshot::channel();
            let waiting_writes = Arc::clone(&writes);
            let admission_waiter = tokio::task::spawn_blocking(move || {
                waiting_writes.admit(1, |stats| {
                    let _ = observed_tx.send(stats);
                })
            });
            let observed = observed_rx.await.expect("admission wait observation");
            assert_eq!(observed.active_requests, 2);
            assert_eq!(observed.admission_waiters, 1);
            assert_gauges(&writes, (2, 1, 2, 1, 8, 1));

            let (barrier_tx, mut barrier_rx) = oneshot::channel();
            let barrier_writes = Arc::clone(&writes);
            tokio::task::spawn_blocking(move || {
                let _ = barrier_tx.send(barrier_writes.wait_through(watermark));
            });
            while writes.stats().barrier_waits == 0 {
                tokio::task::yield_now().await;
            }
            assert!(
                timeout(Duration::from_millis(50), &mut barrier_rx)
                    .await
                    .is_err(),
                "barrier ignored the sequenced callback awaiting admission"
            );

            writes.begin_shutdown();
            match admission_waiter.await.expect("admission waiter join") {
                Err(errno) => assert_eq!(errno, libc::EIO),
                Ok(_) => panic!("shutdown unexpectedly admitted a write"),
            }
            assert_gauges(&writes, (2, 0, 2, 1, 8, 1));
            waiting_ticket.complete(LinuxFuseAsyncWriteCompletion::Failed(libc::EIO));
            assert_eq!(barrier_rx.await.expect("barrier result"), Err(libc::EIO));
            assert_gauges(&writes, (1, 0, 1, 1, 8, 1));

            holder_ticket.complete(LinuxFuseAsyncWriteCompletion::Success);
            drop(reservation);
            assert_gauges(&writes, (0, 0, 0, 0, 0, 0));
        })
        .await
        .expect("waiting-request barrier test timed out");
    }
}

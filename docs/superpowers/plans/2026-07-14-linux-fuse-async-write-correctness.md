# Linux FUSE Async-Write Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Linux FUSE handle-scoped async-write ordering, strict local durability barriers, safe request teardown, honest completion metrics, and an `ASYNC_DIO` Direct-mode default with an explicit rollback.

**Architecture:** Keep one mount-global byte/request admission budget, but move ordering into a small `async_writes` module with a monotonic ledger per file handle. Linux FFI and raw libfuse replies remain in `low_level.rs`; a reply guard owns each detached request until it has replied and completed its ledger ticket. `flush`, `fsync`, and `release` fence only the target handle. Session shutdown closes admission and cancels/drains detached requests before libfuse session destruction. No path waits for S3 or PostgreSQL.

**Tech Stack:** Rust 2024, Tokio, `tokio-util::sync::CancellationToken`, libfuse3 low-level API, existing StudioFS VFS observability, Cargo tests.

---

## Scope and invariants

- Preserve the current mount-global caps of 256 MiB copied write data and 1024
  admitted detached writes. Raw callbacks waiting for admission retain only
  libfuse-owned buffers and are separately bounded by the configured loop
  thread count on libfuse 3.12+, or one receive thread on older supported
  libfuse.
- A barrier captures a fixed `(fh, sequence)` callback watermark, then delegates local durability to the canonical inode-scoped VFS `flush_fh`; same-inode writes already dispatched may be coalesced, while other inodes remain independent.
- A successful, failed, cancelled, or dropped async task releases its global reservation exactly once.
- Every detached raw request is counted from callback entry through its single reply, including time waiting for admission.
- The raw `fuse_req_t` receives at most one reply, and its ledger ticket is completed only after that reply call.
- `flush`, `fsync`, and dirty final `release` are strict local barriers only.
- `O_SYNC` and `O_DSYNC` stay on the synchronous write-through path.
- Direct mode requests `FUSE_CAP_ASYNC_DIO` by default and sets `parallel_direct_writes` when the installed libfuse header exposes that field; `STUDIOFS_FUSE_ASYNC_DIO=0` is the rollback.
- Preserve the documented libfuse 3.2 minimum: use the single-threaded session
  loop before libfuse 3.12 because its public multithread config cannot bound
  worker count; capability negotiation must use legacy 32-bit `capable`/`want`
  fields before libfuse 3.17.1; and the per-open parallel-write bit must compile
  out before libfuse 3.14.1.
- Kernel-cache mode does not request `ASYNC_DIO`.
- Tests place temporary state under the repository `target/` tree and clean it after use.

## File map

- Create: `crates/studiofs-linux-fuse/src/async_writes.rs` — global admission plus per-handle sequence/error ledgers.
- Modify: `crates/studiofs-linux-fuse/src/lib.rs` — declare the private tracker module.
- Modify: `crates/studiofs-linux-fuse/Cargo.toml` — add the workspace `tokio-util` dependency on Linux.
- Modify: `crates/studiofs-linux-fuse/build.rs` — parse the libfuse patch version and define narrow C compatibility macros.
- Modify: `crates/studiofs-linux-fuse/csrc/low_level_ops.c` — version-safe feature helpers and conditional `parallel_direct_writes` access.
- Modify: `crates/studiofs-linux-fuse/src/low_level.rs` — request guard, async task wiring, strict barriers, release split, shutdown, Direct policy, mounted tests.
- Modify: `crates/studiofs-bridge/src/posix.rs` — shared close-time `ENOENT` normalization.
- Modify: `crates/studiofs-macfuse/src/adapter.rs` — use the shared normalization helper without changing macOS behavior.
- Modify: `crates/studiofs-vfs/src/vfs/metrics.rs` — async cancellation and per-handle gauges.
- Modify: `crates/studiofs-vfs/src/vfs.rs` — publish/reset the new async-write fields.
- Modify: `crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs` — export admission/request/handle idle gauges.
- Modify: `crates/studiofs/src/cli/status.rs` — render the new counters.
- Reference only: `docs/juicefs-local-first-rust-contract.md` — callback and local barrier contract.

## Task 1: Share close-time namespace error normalization

- [ ] **Step 1: Add failing POSIX helper tests**

Add to `crates/studiofs-bridge/src/posix.rs`:

```rust
#[test]
fn close_barrier_ignores_namespace_enoent() {
    assert_eq!(super::normalize_close_barrier_result(Err(libc::ENOENT)), Ok(()));
}

#[test]
fn close_barrier_preserves_non_namespace_errors() {
    assert_eq!(
        super::normalize_close_barrier_result(Err(libc::EIO)),
        Err(libc::EIO)
    );
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-bridge close_barrier_ -- --nocapture
```

Expected: compilation fails because `normalize_close_barrier_result` does not exist.

- [ ] **Step 3: Add the narrow shared helper**

Add beside `vfs_err_to_errno`:

```rust
/// Treat a namespace disappearance during close-time flushing as an already
/// completed close, while preserving every other local barrier error.
pub fn normalize_close_barrier_result(result: Result<(), i32>) -> Result<(), i32> {
    match result {
        Err(libc::ENOENT) => Ok(()),
        other => other,
    }
}
```

- [ ] **Step 4: Retarget macFUSE to the shared helper**

Import `normalize_close_barrier_result` in `crates/studiofs-macfuse/src/adapter.rs`, replace the three calls to the private `normalize_close_flush_result`, remove that private function, and update its two unit tests to call the shared name.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cargo test -p studiofs-bridge close_barrier_ -- --nocapture
cargo test -p studiofs-macfuse close_flush_ -- --nocapture
```

Expected: both commands pass and macOS behavior is unchanged.

- [ ] **Step 6: Commit**

```bash
git add crates/studiofs-bridge/src/posix.rs crates/studiofs-macfuse/src/adapter.rs
git commit -m "refactor: share close barrier errno normalization"
```

## Task 2: Build the handle-scoped async-write ledger

- [ ] **Step 1: Declare the module and dependency**

Add `mod async_writes;` before `mod low_level;` in `crates/studiofs-linux-fuse/src/lib.rs`. Add this Linux dependency in `crates/studiofs-linux-fuse/Cargo.toml`:

```toml
tokio-util.workspace = true
```

- [ ] **Step 2: Add tracker tests before implementation**

Create `crates/studiofs-linux-fuse/src/async_writes.rs` with tests named:

```text
barrier_for_one_handle_does_not_wait_for_another_handle
writes_after_a_captured_watermark_do_not_extend_the_barrier
out_of_order_completion_waits_for_every_sequence_through_watermark
first_handle_error_is_returned_once_and_does_not_cross_handles
dropped_ticket_records_task_failure_and_releases_capacity_once
shutdown_rejects_new_admission_and_wakes_blocked_admission
request_waiting_for_admission_keeps_tracker_non_idle
barrier_captures_request_waiting_for_admission
```

Use Tokio tests plus one-second `timeout` bounds. The first two tests hold a ticket and prove the unrelated/future watermark completes through a channel. The out-of-order test completes sequence 2 before sequence 1 and asserts the waiter remains blocked until both are done. The error test injects `EIO` only on handle A and consumes it once. The drop and shutdown tests assert exact byte/request/raw-request gauges after every transition.

- [ ] **Step 3: Verify RED**

Run:

```bash
cargo test -p studiofs-linux-fuse async_writes::tests -- --nocapture
```

Expected: compilation fails on the not-yet-defined tracker API.

- [ ] **Step 4: Implement the deep tracker module**

Use these concrete types and public-to-crate API:

```rust
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
```

Expose exactly these operations:

```rust
impl LinuxFuseAsyncWrites {
    pub(crate) fn new(max_pending_bytes: usize, max_pending_writes: usize) -> Self;
    pub(crate) fn start_request(
        self: &Arc<Self>,
        fh: u64,
    ) -> Result<LinuxFuseRequestTicket, i32>;
    pub(crate) fn admit(
        self: &Arc<Self>,
        bytes: usize,
        on_wait: impl FnOnce(LinuxFuseAsyncWriteStats),
    ) -> Result<LinuxFuseWriteReservation, i32>;
    pub(crate) fn watermark(&self, fh: u64) -> LinuxFuseWriteWatermark;
    pub(crate) fn close_handle(&self, fh: u64) -> LinuxFuseWriteWatermark;
    pub(crate) fn wait_through(&self, watermark: LinuxFuseWriteWatermark) -> Result<(), i32>;
    pub(crate) fn forget_handle(&self, fh: u64);
    pub(crate) fn begin_shutdown(&self);
    pub(crate) fn request_cancellation(&self);
    pub(crate) fn task_cancellation_token(&self) -> CancellationToken;
    pub(crate) fn wait_idle(&self, timeout: Duration) -> bool;
    pub(crate) fn stats(&self) -> LinuxFuseAsyncWriteStats;
}

impl LinuxFuseRequestTicket {
    pub(crate) fn complete(self, completion: LinuxFuseAsyncWriteCompletion);
}
```

Implementation rules:

- `start_request` synchronously assigns the handle sequence, inserts it into the
  handle ledger, and increments `active_requests` before any admission wait, so
  a concurrent barrier sees the callback. It rejects a closing handle with
  `EBADF` and shutdown with `EIO`.
- `admit` waits on the existing condition variable before any buffer copy. On
  success it increments `pending_writes` and `pending_bytes`, so those gauges
  mean admitted detached work and enforce the existing 1024/256 MiB caps.
  Reject a single request larger than the byte cap with `E2BIG` rather than
  waiting forever.
- When `admit` must wait, increment `admission_waiters` and
  `snapshot_generation`, clone the resulting stats under the mutex, drop the
  mutex, and invoke `on_wait` exactly once before blocking. Reacquire the mutex
  and recheck capacity in the normal condition-variable loop. This narrow
  observer makes a lone blocked callback visible without letting metrics code
  re-enter the tracker lock. Waiters retain only the live libfuse callback
  buffer. Their separate hard bound is configured `max_threads` on libfuse
  3.12+ and exactly one callback on the older single-loop path.
- `close_handle` marks the ledger closing under the same mutex that assigns a sequence, so no write can cross the release boundary.
- `wait_through` waits while `pending.first().is_some_and(|seq| *seq <= watermark.sequence)`, then removes and returns the first error at or below the watermark.
- A `Failed(errno)`, `Cancelled`, or dropped ticket records `errno` (`EIO` for the latter two) in that handle only.
- `forget_handle` removes only an idle ledger after release owns the final barrier.
- Dropping `LinuxFuseWriteReservation` releases copied-byte/admitted-task capacity exactly once; completing or dropping `LinuxFuseRequestTicket` records the sequence result and raw-request completion exactly once.
- `begin_shutdown` flips `shutting_down` and notifies the condition variable, waking admission waiters without cancelling admitted VFS writes. `request_cancellation` is the separate forced-unmount fallback that fires `task_cancellation` only after a normal drain grace period.
- `wait_idle` requires both `active_requests == 0` and `pending_writes == 0`; an unadmitted sequenced raw request therefore prevents session destruction.
- Increment `snapshot_generation` before every externally visible state change; notify the condition variable after completion, shutdown, cancellation, and capacity release.

- [ ] **Step 5: Verify GREEN and race behavior**

Run:

```bash
cargo test -p studiofs-linux-fuse async_writes::tests -- --nocapture
cargo test -p studiofs-linux-fuse async_writes::tests --release -- --nocapture
```

Expected: all ledger tests pass in debug and release builds.

- [ ] **Step 6: Commit**

```bash
git add crates/studiofs-linux-fuse/Cargo.toml crates/studiofs-linux-fuse/src/lib.rs crates/studiofs-linux-fuse/src/async_writes.rs Cargo.lock
git commit -m "feat(linux): add handle-scoped async write ledger"
```

## Task 3: Make raw-request completion cancellation-safe and observable

- [ ] **Step 1: Add failing snapshot and formatting tests**

Extend the existing async snapshot tests in `crates/studiofs-vfs/src/vfs/tests.rs` and `crates/studiofs/src/cli/status.rs` to require:

```rust
cancelled_writes: 2,
active_requests: 2,
admission_waiters: 1,
pending_handles: 3,
max_handle_pending_writes: 4,
peak_handle_pending_writes: 7,
```

The status assertion must contain `2 cancelled`, `2 active requests`, `1 admission waiter`, and `3 handles`.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-vfs fuse_async_write_snapshot -- --nocapture
cargo test -p studiofs mounted_status_formats_fuse_async_write_diagnostics -- --nocapture
```

Expected: compilation fails because the six fields do not exist.

- [ ] **Step 3: Extend the stable snapshot**

Add the six fields to `VfsFuseAsyncWriteSnapshot` in `crates/studiofs-vfs/src/vfs/metrics.rs`, copy them in `record_fuse_async_write_snapshot`, and baseline-subtract only monotonic fields (`cancelled_writes` and `peak_handle_pending_writes`) in `apply_fuse_async_write_reset_baseline`. Gauges (`active_requests`, `admission_waiters`, `pending_handles`, `pending_writes`, `pending_bytes`, `max_handle_pending_writes`) are copied unchanged.

Export these exact gauges from `.studiofs-runtime-stats` and test snapshot/reset behavior:

```text
fuse_async_write.active_requests
fuse_async_write.admission_waiters
fuse_async_write.pending_handles
fuse_async_write.pending_writes
fuse_async_write.pending_bytes
```

Render the status lines as:

```text
FUSE async pending:       <writes> writes on <handles> handles, <bytes> current, <bytes> peak
FUSE async requests:      <active> active, <waiters> awaiting admission
FUSE async handle peak:   <current-max> current, <peak> peak
FUSE async completions:   <ok> ok, <errors> errors, <task failures> task failures, <cancelled> cancelled
```

- [ ] **Step 4: Add a single-owner request guard in `low_level.rs`**

Make `FuseReqPtr::reply_write` and `reply_error` return libfuse's `c_int`. Then add:

```rust
struct FuseWriteTaskGuard {
    request: Option<FuseReqPtr>,
    request_ticket: Option<LinuxFuseRequestTicket>,
    reservation: Option<LinuxFuseWriteReservation>,
    writes: Arc<LinuxFuseAsyncWrites>,
    vfs: Arc<StudioVfs>,
    started: Instant,
    requested_bytes: usize,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
}

enum FuseWriteTaskResult {
    AdmissionRejected(i32),
    Completed(Result<u32, i32>),
    Cancelled,
}
```

Construct the guard immediately after the callback has selected the opportunistic async path: call `start_request`, attach that ticket, and publish a tracker snapshot before copying/spawning. Its explicit completion order is:

```rust
fn finish(mut self, result: FuseWriteTaskResult) {
    let request = self.request.take().expect("request guard owns one reply");
    let (reply_status, errored, completion) = match result {
        FuseWriteTaskResult::AdmissionRejected(errno) => {
            (
                request.reply_error(errno),
                true,
                LinuxFuseAsyncWriteCompletion::Failed(errno),
            )
        }
        FuseWriteTaskResult::Completed(Ok(written)) => {
            let status = request.reply_write(written as size_t);
            let completion = if status == 0 {
                LinuxFuseAsyncWriteCompletion::Success
            } else {
                LinuxFuseAsyncWriteCompletion::Failed(libc::EIO)
            };
            (status, status != 0, completion)
        }
        FuseWriteTaskResult::Completed(Err(errno)) => {
            (
                request.reply_error(errno),
                true,
                LinuxFuseAsyncWriteCompletion::Failed(errno),
            )
        }
        FuseWriteTaskResult::Cancelled => {
            (
                request.reply_error(libc::EIO),
                true,
                LinuxFuseAsyncWriteCompletion::Cancelled,
            )
        }
    };
    if reply_status != 0 {
        tracing::warn!(reply_status, "libfuse rejected async write reply");
    }
    self.vfs.record_fuse_write_callback(
        self.requested_bytes,
        self.started.elapsed(),
        errored,
    );
    drop(self.reservation.take());
    self.request_ticket
        .take()
        .expect("request sequence completes once")
        .complete(completion);
    record_linux_async_write_snapshot(
        &self.vfs,
        &self.writes,
        self.metric_config,
    );
}
```

For a successful VFS write whose `reply_write` returns nonzero, record `EIO` in
the handle ledger; for an operation that already failed, retain its original
errno even if the error reply is also rejected. After `finish(self)` has taken
all three owned options, its ensuing `Drop` is an explicit no-op. An unfinished
guard's `Drop` replies `EIO` once, records one errored callback, drops its
reservation, lets the request ticket record one task failure, and publishes the
final snapshot. Add fake-reply tests for successful and rejected write replies,
error-reply rejection, and `Drop` after `finish` to prove there is never a
double reply.

- [ ] **Step 5: Rewire `queue_async_write`**

Pass the callback `started: Instant` into `queue_async_write`. After selecting
the opportunistic path, call `start_request` and construct the guard. Then call
the synchronous bounded admission method before copying the borrowed libfuse
buffer:

```rust
let reservation = match writes.admit(data.len(), |waiting_stats| {
    record_linux_async_write_stats(&vfs, waiting_stats, metric_config);
}) {
    Ok(reservation) => reservation,
    Err(errno) => {
        guard.finish(FuseWriteTaskResult::AdmissionRejected(errno));
        return;
    }
};
guard.attach_reservation(reservation);
record_linux_async_write_snapshot(&vfs, &writes, metric_config);
let data = Bytes::copy_from_slice(data);
let cancellation = writes.task_cancellation_token();
self.runtime_handle.spawn(async move {
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => FuseWriteTaskResult::Cancelled,
        result = vfs.write_fh_owned_with_deferred_materialize(
            fh,
            ino,
            write_offset,
            data,
            LocalSyncPolicy::Opportunistic,
        ) => FuseWriteTaskResult::Completed(
            result.map_err(|error| vfs_err_to_errno(&error)),
        ),
    };
    guard.finish(result);
});
```

The callback returns only after admission, copy, and spawn. This is deliberate:
sequence assignment precedes the wait, copied memory cannot escape the global
budget, and the number of waiting callbacks is bounded by the selected libfuse
loop. The shutdown watcher in Task 5 wakes those callbacks on session exit.

Delete the enqueue-time `record_fuse_write_callback` from `studiofs_linux_fuse_write`. Keep synchronous callback metrics where they are.

- [ ] **Step 6: Publish production tracker snapshots**

`LinuxFuseAsyncWriteMetricConfig` is a copyable struct containing the cache
policy and `LinuxFuseConnectionConfig`. `record_linux_async_write_snapshot`
maps tracker stats into `VfsFuseAsyncWriteSnapshot`, sets both caps, the
selected `direct`/`kernel` label, `kernel_writeback_cache`, and
background/congestion values, and leaves not-yet-negotiated geometry at its
existing zero/default value. Publish immediately after `start_request`, from
the admission wait observer, after successful admission, and after every
finish/drop so a lone waiting or pending request is observable.

- [ ] **Step 7: Verify GREEN**

Run:

```bash
cargo test -p studiofs-vfs fuse_async_write_snapshot -- --nocapture
cargo test -p studiofs mounted_status_formats_fuse_async_write_diagnostics -- --nocapture
cargo test -p studiofs-linux-fuse async_write_ -- --nocapture
```

Expected: all tests pass; completed callback counts advance at reply time, not enqueue time.

- [ ] **Step 8: Commit**

```bash
git add crates/studiofs-linux-fuse/src/low_level.rs crates/studiofs-vfs/src/vfs/metrics.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs crates/studiofs-vfs/src/vfs/tests.rs crates/studiofs/src/cli/status.rs
git commit -m "fix(linux): own async replies through completion"
```

## Task 4: Combine handle callback ordering with strict VFS barriers

- [ ] **Step 1: Add release-policy and barrier regression tests**

Add a pure release selector in the Linux tests:

```rust
#[test]
fn dirty_opportunistic_release_is_synchronous() {
    assert_eq!(
        super::linux_fuse_release_mode(LocalSyncPolicy::Opportunistic, true),
        super::LinuxFuseReleaseMode::Synchronous,
    );
}

#[test]
fn clean_opportunistic_release_is_deferred() {
    assert_eq!(
        super::linux_fuse_release_mode(LocalSyncPolicy::Opportunistic, false),
        super::LinuxFuseReleaseMode::Deferred,
    );
}
```

Also add `flush_runs_vfs_barrier_after_async_error` and `release_closes_vfs_handle_after_async_error`. The tracker tests from Task 2 prove callback watermark/error isolation; the VFS integration tests prove that activity on another inode does not hold the target barrier. Do not add a Linux-only same-inode dispatch scheduler: `flush_fh` intentionally remains the canonical inode-scoped barrier and may coalesce writes already dispatched to that inode.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-linux-fuse release_is_ -- --nocapture
```

Expected: compilation fails because the selector and enum do not exist.

- [ ] **Step 3: Replace mount-global barrier waits**

In `SessionState::flush`:

```rust
let watermark = self.async_writes.watermark(fh);
let async_result = self.async_writes.wait_through(watermark);
let local_result = normalize_close_barrier_result(
    self.runtime_handle
        .block_on(self.vfs.flush_fh(fh))
        .map_err(|error| vfs_err_to_errno(&error)),
);
merge_async_and_local_barrier_results(async_result, local_result)
```

`merge_async_and_local_barrier_results` always receives two already-computed
results and prefers the retained async error when both fail. In handle-backed
`fsync`, capture/wait the handle watermark and always call `flush_fh` even when
the async result is an error. If a regular-file `fsync` callback has no valid
file handle, return `EBADF`; do not pretend that a mount-global idle wait is the
requested file barrier. Add a focused no-handle test.

Remove `flush_fuse_writeback_with_outcome` from the Linux flush callback. It is weaker than the canonical strict local `flush_fh` barrier.

- [ ] **Step 4: Split dirty and clean release**

At entry, fetch the open file and policy, atomically close its ledger admission with `close_handle`, and immediately install the `forget_handle` scope guard before any fallible wait. Retain the async result, but always evaluate dirty state and execute VFS release:

```rust
let watermark = self.async_writes.close_handle(fh);
let _forget_guard = LinuxFuseHandleLedgerGuard::new(
    Arc::clone(&self.async_writes),
    fh,
);
let async_result = self.async_writes.wait_through(watermark);
let mode = linux_fuse_release_mode(
    local_sync_policy,
    self.vfs.inode_has_local_dirty_state(open_file.ino),
);
let local_result = match mode {
    LinuxFuseReleaseMode::Synchronous => self
        .runtime_handle
        .block_on(self.vfs.release_fh_with_outcome(fh))
        .map(|_| ())
        .map_err(|error| vfs_err_to_errno(&error)),
    LinuxFuseReleaseMode::Deferred => {
        self.vfs
            .release_fh_with_deferred_last_handle_barrier(fh, &self.runtime_handle);
        Ok(())
    }
};
merge_async_and_local_barrier_results(
    async_result,
    normalize_close_barrier_result(local_result),
)
```

The scope guard calls `forget_handle(fh)` after VFS release on success or error. Thus an async write error is reported without leaking the VFS handle, OS-cache lifecycle, or ledger.

- [ ] **Step 5: Add a local-durability integration test**

Using `ContractVfsFixture`, create/open/write through `SessionState`, call `flush`, assert `inode_has_local_dirty_state(ino) == false`, release, reopen, and verify the exact bytes. Name it:

```rust
linux_flush_is_a_strict_local_durability_barrier
linux_flush_after_async_error_still_materializes_prior_local_state
linux_release_after_async_error_removes_handle_and_lifecycle
linux_barrier_does_not_wait_for_another_inode
```

- [ ] **Step 6: Verify GREEN**

Run:

```bash
cargo test -p studiofs-linux-fuse release_is_ -- --nocapture
cargo test -p studiofs-linux-fuse linux_flush_is_a_strict_local_durability_barrier -- --nocapture
cargo test -p studiofs-linux-fuse async_error_still_ -- --nocapture
cargo test -p studiofs-linux-fuse linux_barrier_does_not_wait_for_another_inode -- --nocapture
cargo test -p studiofs-vfs flush_fh_enforces_strict_local_durability_in_single_mode -- --nocapture
cargo test -p studiofs-vfs release_fh_enforces_strict_local_durability_on_last_handle -- --nocapture
```

Expected: all tests pass without any remote-service wait.

- [ ] **Step 7: Commit**

```bash
git add crates/studiofs-linux-fuse/src/low_level.rs crates/studiofs-bridge/src/posix.rs
git commit -m "fix(linux): make file barriers handle scoped"
```

## Task 5: Drain or cancel retained requests before session destruction

- [ ] **Step 1: Add shutdown lifecycle tests**

Add tracker tests proving:

- `begin_shutdown` is idempotent;
- a blocked admission wakes and returns `EIO`;
- a raw request awaiting admission keeps `wait_idle` false even though it owns no write ticket;
- a callback blocked in admission is woken by the session-exit watcher and
  replies `EIO`;
- the watcher seam observes exit, calls `begin_shutdown`, and joins promptly
  when stopped;
- the cancellation token remains pending during the normal drain grace period and fires only after `request_cancellation`;
- `wait_idle` stays false while a ticket is held and becomes true after `Cancelled` completion;
- every counter and byte gauge returns to zero after cancellation.
- a forced timeout retains both the raw session and the boxed `SessionState` userdata owner.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-linux-fuse async_write_shutdown_ -- --nocapture
```

Expected: at least the new lifecycle assertions fail.

- [ ] **Step 3: Add one shutdown path used by `run` and `Drop`**

Add `ASYNC_WRITE_SHUTDOWN_DRAIN_TIMEOUT` and `ASYNC_WRITE_SHUTDOWN_CANCEL_TIMEOUT`, both two seconds, and:

```rust
fn shutdown_async_writes(&self) -> bool {
    self.async_writes.begin_shutdown();
    if self
        .async_writes
        .wait_idle(ASYNC_WRITE_SHUTDOWN_DRAIN_TIMEOUT)
    {
        return true;
    }
    self.async_writes.request_cancellation();
    self.async_writes
        .wait_idle(ASYNC_WRITE_SHUTDOWN_CANCEL_TIMEOUT)
}
```

Declare the stable libfuse `fuse_session_exited` function in the raw bindings.
Immediately before entering the version-selected session loop wrapper, start
one small `SessionExitWatcher` thread. It checks
`fuse_session_exited(session)` at most
four times per second and waits between checks on its own condition variable,
so stop/join is immediate and idle CPU is negligible. As soon as libfuse marks
the session exited or disconnected, it calls `begin_shutdown`, which wakes any
  callbacks blocked in admission and lets the selected session loop join. The
watcher owns only the borrowed session pointer, the async tracker, and its stop
state; join it before session destruction.

Immediately after the selected session loop returns, also call
`shutdown_async_writes` before returning from `Session::run`. In
`Session::drop`, call it again before unmount/destroy. Combine its result with
`SessionInvalidationState::wait_for_idle`. The first grace period lets admitted
VFS writes finish normally; condition-variable shutdown wakes unadmitted
callbacks so they reply `EIO`; forced cancellation is reserved for admitted
tasks that exceed the grace period.

If either drain times out, unmount but deliberately do not call `fuse_session_destroy`. Because libfuse userdata points into `Session.state: Option<Box<SessionState>>`, also take and intentionally retain/leak that entire box (and therefore every object a late callback needs), not merely the raw session pointer. Put this rare fallback in one named helper and unit-test ownership with a drop sentinel. Normal shutdown must reach zero active requests, destroy the raw session, and drop state normally.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p studiofs-linux-fuse async_write_shutdown_ -- --nocapture
cargo test -p studiofs-linux-fuse invalidation_state_wait_for_idle -- --nocapture
```

Expected: all tests pass and no test hangs.

- [ ] **Step 5: Commit**

```bash
git add crates/studiofs-linux-fuse/src/async_writes.rs crates/studiofs-linux-fuse/src/low_level.rs
git commit -m "fix(linux): drain async replies before fuse teardown"
```

## Task 6: Make Direct `ASYNC_DIO` a coherent default

- [ ] **Step 1: Add pure policy tests**

Add tests for this exact table:

| Cache policy | Override | Expected |
| --- | --- | --- |
| Direct | absent | true |
| Direct | `0` | false |
| Direct | `1` | true |
| Kernel | absent | false |
| Kernel | `1` | false |

Add a second table for the per-open bit:

| Direct async decision | C shim exposes field | Expected `parallel_direct_writes` |
| --- | --- | --- |
| true | true | true |
| true | false | false |
| false | true | false |

Also assert that the C support probe and the pure Rust mapper agree on the current build.

Add a loop-selection table:

| Header version | Expected receive loop | Raw admission-waiter bound |
| --- | --- | ---: |
| `< 3.12` | `fuse_session_loop` | 1 |
| `>= 3.12` | configured `fuse_session_loop_mt` | configured `max_threads` |

The C loop-selection probe and Rust test must agree on the current header.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p studiofs-linux-fuse async_dio_policy_ -- --nocapture
```

Expected: Direct-without-override fails because the current default is disabled.

- [ ] **Step 3: Make the C shim compile across the supported libfuse range**

Extend `build.rs` to parse `(major, minor, patch)` while retaining the existing `>= 3.2` minimum. Define:

```text
STUDIOFS_FUSE_HAS_BOUNDED_LOOP_CONFIG     for libfuse >= 3.12.0
STUDIOFS_FUSE_HAS_PARALLEL_DIRECT_WRITES  for libfuse >= 3.14.1
STUDIOFS_FUSE_HAS_EXTENDED_CONN_CAPS      for libfuse >= 3.17.1
```

Rename the current loop wrapper to the behavior-oriented
`studiofs_linux_fuse_session_loop_bounded`. With bounded loop config, create a
`fuse_loop_config`, set idle/max threads, and call `fuse_session_loop_mt`. On
older headers, do not call the old multithread loop because its public config
has no `max_threads` and the current shim silently ignores the requested cap;
call `fuse_session_loop(session)` instead. Export a narrow
`studiofs_linux_fuse_uses_single_thread_loop()` probe for tests and negotiated
diagnostics. Do not build a custom worker pool.

In `low_level_ops.c`, route both requested capabilities through private helpers. With extended connection capabilities, call `fuse_set_feature_flag` and inspect `want_ext`; on older headers, first reject flags outside 32 bits, then require `(conn->capable & flag) != 0` and set/read `conn->want`. `FUSE_CAP_ASYNC_DIO` and `FUSE_CAP_WRITEBACK_CACHE` are both representable in that legacy path.

Guard every access to `fuse_file_info.parallel_direct_writes` with `STUDIOFS_FUSE_HAS_PARALLEL_DIRECT_WRITES`. Export `studiofs_linux_fuse_supports_parallel_direct_writes()` to Rust and make the existing C flag-inspection test return no parallel bit on older headers instead of referencing the missing field.

- [ ] **Step 4: Implement one Rust policy source**

Add:

```rust
fn linux_fuse_async_dio_enabled_for(
    cache_policy: LinuxFuseCachePolicy,
    override_value: Option<&str>,
) -> bool {
    cache_policy == LinuxFuseCachePolicy::Direct
        && override_value.is_none_or(linux_fuse_env_flag_value)
}
```

Use it from both `studiofs_linux_fuse_connection_wants_async_dio` and `linux_fuse_open_cache_policy_for`. Change the latter signature to accept the already-computed boolean plus `studiofs_linux_fuse_supports_parallel_direct_writes()`, so the per-open bit is never requested when the field is unavailable. Task 3 of the Linux integration plan replaces the requested async input with the actual connection result after that result is observable.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cargo test -p studiofs-linux-fuse async_dio_policy_ -- --nocapture
cargo test -p studiofs-linux-fuse conn_feature_helper_requests_async_dio_when_supported -- --nocapture
cargo test -p studiofs-linux-fuse file_info_init_sets_requested_cache_flags -- --nocapture
cargo test -p studiofs-linux-fuse libfuse_compatibility_ -- --nocapture
cargo test -p studiofs-linux-fuse bounded_loop_selection_ -- --nocapture
```

Expected: all tests pass on the current libfuse. In the final Linux
compatibility verification, also compile and run the compatibility test against
the documented Rocky Linux 9/libfuse 3.10 headers; that job must assert the
single receive loop, legacy capability fields, and omitted parallel-write bit
without source changes. The current libfuse 3.18 job must assert the configured
bounded multithread loop.

- [ ] **Step 6: Commit**

```bash
git add crates/studiofs-linux-fuse/build.rs crates/studiofs-linux-fuse/csrc/low_level_ops.c crates/studiofs-linux-fuse/src/low_level.rs
git commit -m "feat(linux): default direct mode to async dio"
```

## Task 7: Add mounted regressions for Direct mode and teardown

- [ ] **Step 1: Add disjoint same-file parallel writes**

Beside the existing ignored mounted tests in `low_level.rs`, add `mounted_same_file_parallel_direct_writes_preserve_disjoint_regions`. Use `std::os::unix::fs::FileExt::write_all_at`, eight cloned handles, eight deterministic 1 MiB regions, one `sync_all`, reopen, and compare every region byte-for-byte.

- [ ] **Step 2: Add forced-unmount coverage**

Add `mounted_forced_unmount_drains_admitted_and_waiting_raw_requests`. Give the private mounted-test constructor a one-write admission limit, hold the first write at an existing VFS test barrier, then start a second write and assert the runtime snapshot shows one admitted write plus one admission waiter/two active raw requests. Request unmount, release the barrier, join both writers, and assert the session exits without a crash, stuck mount, leaked request, or late-reply warning. Do not add a production tuning knob solely for this test.

- [ ] **Step 3: Run mounted Direct tests**

Run serially with the configured mounted-test environment:

```bash
cargo test -p studiofs-linux-fuse mounted_regular_file_io_is_visible_across_reopen -- --ignored --nocapture --test-threads=1
cargo test -p studiofs-linux-fuse mounted_same_file_parallel_direct_writes_preserve_disjoint_regions -- --ignored --nocapture --test-threads=1
cargo test -p studiofs-linux-fuse mounted_forced_unmount_drains_admitted_and_waiting_raw_requests -- --ignored --nocapture --test-threads=1
STUDIOFS_FUSE_ASYNC_DIO=0 cargo test -p studiofs-linux-fuse mounted_regular_file_io_is_visible_across_reopen -- --ignored --nocapture --test-threads=1
```

Expected: all enabled tests pass with both the new default and rollback.

- [ ] **Step 4: Commit**

```bash
git add crates/studiofs-linux-fuse/src/low_level.rs
git commit -m "test(linux): cover parallel direct writes and teardown"
```

## Task 8: Focused verification

- [ ] **Step 1: Run bridge and VFS contract tests**

```bash
cargo test -p studiofs-bridge --lib -- --nocapture
cargo test -p studiofs-vfs --lib fuse_async_write_snapshot -- --nocapture
cargo test -p studiofs-vfs flush_fh_enforces_strict_local_durability_in_single_mode -- --nocapture
cargo test -p studiofs-vfs release_fh_enforces_strict_local_durability_on_last_handle -- --nocapture
```

- [ ] **Step 2: Run the Linux bridge suite**

```bash
cargo test -p studiofs-linux-fuse --lib -- --nocapture
```

Expected: all non-ignored Linux bridge tests pass.

- [ ] **Step 3: Run formatting and focused linting**

```bash
cargo fmt --all -- --check
cargo clippy -p studiofs-bridge -p studiofs-linux-fuse -p studiofs-vfs -p studiofs --all-targets -- -D warnings
```

Expected: both commands exit zero.

- [ ] **Step 4: Confirm the worktree contains only planned changes**

```bash
git status --short
git diff --check
```

Expected: no unplanned files and no whitespace errors.

## Self-review

- Every opportunistic callback gets one handle sequence before admission; every
  admitted write gets one global reservation before its buffer copy.
- Every detached raw request is counted before async admission and replied once.
- The ledger waits through a captured handle watermark; VFS durability remains intentionally inode-scoped.
- Release closes handle admission atomically before its final watermark.
- Async errors never skip `flush_fh` or VFS handle/lifecycle release.
- The request reply occurs before ledger completion and session destruction.
- The session-exit watcher wakes blocked admission without busy-spinning;
  shutdown also has cancellation and raw-session safety fallbacks.
- Old libfuse uses one receive thread instead of an unbounded multithread loop;
  3.12+ uses its configured worker cap.
- `flush_fh` and dirty final release remain local-only strict barriers.
- Direct and Kernel policies cannot disagree between capability negotiation and per-open flags.
- No new scheduler, worker-count policy, or remote wait is introduced.

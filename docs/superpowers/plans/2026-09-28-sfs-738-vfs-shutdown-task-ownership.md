# Own VFS Storage Work Through Shutdown

This ExecPlan is living and follows `.agent/PLANS.md`. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as the work proceeds.

When this change is complete, graceful VFS shutdown will stop admitting foreground mutation futures, wait for mutations already in progress, and then wait for all retry and deferred-cleanup work that can retain the local metadata or cache stores. A clean shutdown will then allow the existing test VFS to reopen its redb metadata and cache immediately.

`StudioVfs::shutdown` in `crates/studiofs-vfs/src/vfs/upload_lifecycle.rs` first stops and joins background workers, then drains local dirty state and persists local metadata/cache. Both the final dirty drain and a foreground mutation already in progress can register more work after an earlier join. Epoch retry scheduling lives in `crates/studiofs-vfs/src/vfs/workers.rs`; deleted-inode cache cleanup lives in `crates/studiofs-vfs/src/vfs/deletion_cleanup.rs`. `ShutdownState` (`crates/studiofs-vfs/src/vfs.rs`) owns the mutation-admission gate and task handles so no admitted producer or tracked owner crosses persistence/close.

## Progress

- [x] Read SFS-738, related SFS-414, the VFS lifecycle contract, and the affected task paths.
- [x] Add the retry/reopen regression and confirm it fails because the retry handle is absent from shutdown tracking.
- [x] Extend the regression to cover Tokio cleanup and the no-runtime cleanup thread.
- [x] Register retry work, preserve ownership of blocking/thread cleanup, and join work produced by the final dirty drain.
- [x] Run focused regressions, VFS check and Clippy; inspect the final diff.
- [x] Verify the shutdown review against current call paths; reap completed handles, preserve panic reporting, and simplify the duplicated wait loops.
- [x] Verify the follow-up review; correct compaction completion logging, avoid polling active Tokio handles, and record the verified runtime/thread-join decisions.
- [x] (2026-09-28 16:43Z) Close synchronous mutation admission before task draining and add a regression for cleanup registered by an already-admitted mutation.
- [x] (2026-09-28) Bound abandoned namespace cleanup's pre-storage Delete-gate wait by shutdown cancellation, make queued file-session accounting cancellation-safe, and log compaction completion only after successful joins.
- [x] (2026-09-28) Apply the admission gate to every synchronous metadata mutator, bound shutdown's write-lock wait without cancelling retryable work, and filter only shutdown-skipped cleanup inodes from the final dirty drain.

## Milestones

The first milestone adds a synchronized regression around the current shutdown owner. The regression schedules a delayed epoch retry, schedules deleted-inode cleanup through both available execution paths, shuts down, verifies no tracked storage work remains, and reopens the same local redb/cache fixture. It must fail before the ownership change because the retry and non-Tokio thread are not in shutdown state.

The second milestone makes shutdown retain every relevant handle and producer. Retry waits use the shutdown cancellation token and are registered with the existing task list. A shared Tokio read/write gate lets synchronous mutation futures register cleanup while admitted; shutdown closes that gate after claiming ownership and waits for existing readers before draining tasks. Tokio cleanup wrappers are tracked in a non-abortable list; `run_local_cache_io_blocking` awaits its blocking operation, so shutdown keeps the wrapper alive until that operation releases the cache. Cleanup started without a Tokio runtime keeps its OS thread handle and completion signal in shutdown state. Shutdown joins again after the final dirty drain, before uploader close and local persistence.

The final milestone runs the single regression, `cargo check -p studiofs-vfs`, and `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`, then checks formatting and the diff. A passing unit regression proves the local reopen boundary; it does not claim native mounted acceptance.

## Surprises & Discoveries

- `StudioVfs::shutdown` takes one snapshot of `background_tasks` before `flush_pending_payloads_for_shutdown`, even though the final drain can schedule namespace and deleted-inode cleanup.
- The epoch retry uses `tokio::spawn` without registering its handle and waits on `Notify::notify_waiters`, which does not retain a permit for a future waiter created after notification.
- The Tokio cleanup path currently tracks an async wrapper around `run_local_cache_io_blocking`; aborting that wrapper can drop the handle while its already-running blocking closure continues.
- The no-runtime cleanup path discards the `std::thread::JoinHandle`.
- Reaping Tokio handles through `is_finished()` alone would discard completed panic results; a nonblocking poll keeps panic reporting while releasing completed handles.
- `Vec::remove(0)` shifted remaining handles even though completion order is not observable, so shutdown can use `swap_remove(0)`.
- A synchronous rename or truncate can be admitted before shutdown and register deferred cache cleanup after a task-list snapshot. The task-list loop cannot own a producer that has not registered yet, so shutdown needs an operation-admission barrier before its first drain.
- Abandoned namespace cleanup can be non-abortable before it owns storage: `FileSessionState::enter_op(Delete)` may still be waiting on the inode gate. Cancel only that wait during shutdown, record the skipped cleanup through the existing error map, and keep cleanup non-abortable after the gate is acquired.
- Cancelling `enter_op` while queued previously left its queued-operation and queued-delete counters incremented. A small queue guard now releases those counts on cancellation as well as successful gate acquisition.
- A skipped cleanup inode also remained in the final dirty-inode list, which would reacquire the same blocked session gate during shutdown's flush phase. Filtering all `cleanup_errors` also excluded unrelated earlier cleanup failures, so a dedicated shutdown-skipped set now controls that exclusion.
- The admission read guard originally covered only `run_sync_future`; direct create, mkdir, symlink, setattr, xattr, and ACL mutators could still write during shutdown. A shared admission helper now covers those synchronous entry points, and deferred namespace registration does not recheck shutdown after an operation has already been admitted.
- The shutdown write-side admission wait was unbounded and ran before token cancellation. It now uses the existing two-second join timeout and returns a retryable error before cancelling background cleanup if admitted mutations do not finish.

## Decision Log

- Keep the existing task tracker and shutdown ordering; add only the ownership needed for blocking and OS-thread cleanup. This initial decision was superseded by the review follow-up below because task ownership alone does not cover producers still inside synchronous mutation futures.
- Use the already-owned `background_shutdown_token` for retry cancellation so a late waiter observes shutdown.
- Keep timeout cancellation for ordinary async workers. Blocking cleanup and native cleanup threads must remain owned and finish before local stores can be reopened.
- Repeat the task drain after shutdown-generated dirty/namespace work so cleanup produced by the final local drain is joined.
- Treat cleanup timeouts as a grace/warning threshold, not a release deadline: SFS-738's Linear risk notes prohibit detaching owners while they may retain local stores.
- Keep cleanup thread creation under the shutdown task-list lock so a thread is published atomically with its registration; keep direct namespace cleanup on its verified Tokio-only call path.
- Keep namespace batch cleanup on `Handle::current()`: its only direct caller is the async namespace drain, run either as a tracked Tokio task or by an async barrier.
- Keep deferred block eviction on `Handle::try_current()` without a thread fallback: its caller is truncate finalization inside `setattr_with_size_change`, which `setattr_with_handle` polls through `run_sync_future`.
- Keep the native cleanup `join()` after its completion signal; the signal follows the cleanup closure, and the synchronous join only reaps the native thread before releasing its tracked handle.
- Do not prioritize the epoch retry `select!` with `biased;`: shutdown sets `shutdown_requested` before cancelling the token, and the timer branch checks that flag before queueing.
- Skip moving native thread creation outside the shutdown-list lock because registration must stay atomic against the shutdown snapshot; skip the one-call `String` parameter style suggestion as cosmetic.
- Close synchronous mutation admission through one `tokio::sync::RwLock<()>`: a shared helper takes a nonblocking read guard for direct metadata mutators and `run_sync_future`; shutdown takes the write guard after claiming ownership and holds it through persistence. An admitted operation can finish registration after shutdown is requested, while later operations are rejected.
- Bound the write-side admission wait by the existing two-second background join timeout. If it expires, return before cancelling the background token so the aborted shutdown owner can be retried after the active mutation finishes.
- Use the existing background shutdown token to stop deferred cleanup only before it acquires `FileSessionOperation::Delete`; after acquisition, retain the existing unbounded owner join so cache/storage work finishes before close. Preserve the error through `cleanup_errors` so shutdown reports the skipped inode.
- Do not retry per-inode flush for an inode explicitly marked as skipped while waiting for its Delete gate during shutdown; that flush would wait on the same gate. Keep general cleanup errors eligible for flush so unrelated dirty payloads and attrs remain durable.
- Emit the local compaction completion event only for `Ok(())` from the join. Keep logging successful completion even when the worker completed within the timeout; the event describes completion, not only a timeout wait.

## Outcomes & Retrospective

Shutdown now cancels and joins epoch retries, waits for deferred Tokio cleanup and no-runtime cleanup threads, and repeats the task drain after the final local dirty flush. A regression confirmed the retry handle is absent before the fix; after the fix, both focused tests passed and the same local redb/cache fixture reopened with its written bytes intact.

The shutdown follow-up also waits for already-admitted synchronous mutations before taking task-list snapshots. The new regression failed before the write-side admission barrier because shutdown completed while a mutation was held before cleanup registration; it passes with the barrier and confirms shutdown waits for the cleanup it registers.

The latest review follow-up gates direct synchronous metadata mutators as well as `run_sync_future`. A deterministic deferred-create regression holds the inode reservation after admission, introduces a persisted name conflict behind a negative dentry, and confirms shutdown waits until that producer registers cleanup. Shutdown's admission wait now times out after two seconds before token cancellation, leaving the shutdown owner retryable. A dedicated marker narrows the dirty-inode exclusion to cleanup skipped by that shutdown cancellation; an unrelated prior cleanup error no longer suppresses writer and dirty-attr persistence.

Review follow-up reaps completed Tokio and native thread handles as new work is registered, logs completed task/thread panics, checks every join result after abort, shares the `JoinHandle` timeout policy, factors the native-thread waiter, and replaces front-removal shifts with `swap_remove`. Review requests for a hard shutdown deadline, runtime-less cleanup fallback, spawn-before-registration, and one shared warning budget were rejected because they conflict with storage ownership, have no runtime caller, or add policy without a demonstrated need; the synthetic cleanup IDs cover task registration while the regression also writes and reads real local data across reopen.

The second shutdown review found a real compaction-log ordering bug: the completion event ran from the timeout callback, before the worker joined, and was absent on normal completion. The event now follows the join, with tests for both normal and post-timeout completion. The same follow-up adds an `is_finished()` fast path before polling tracked Tokio handles, shares the three task-list RAII guards, and documents cleanup grace-timeout and native-thread join semantics. The two runtime-availability findings were rejected after tracing their callers: namespace cleanup is reached only inside the Tokio async drain, and deferred block eviction runs inside `setattr_with_size_change` polled by `run_sync_future`.

Follow-up validation passed:

- `cargo test -p studiofs-vfs --lib upload_lifecycle::tests -- --nocapture` — 19 passed.
- `cargo test -p studiofs-vfs --lib shutdown_owns_epoch_retry_before_reopening_local_stores -- --nocapture` — 1 passed.
- `cargo check -p studiofs-vfs`.
- `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`.
- `cargo fmt --all -- --check` and `git diff --check`.

Validation passed:

- `cargo test -p studiofs-vfs shutdown_owns_epoch_retry_before_reopening_local_stores --lib -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs deferred_cleanup_join_waits_past_timeout_without_aborting_task --lib -- --nocapture` — 1 passed.
- `cargo check -p studiofs-vfs`.
- `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`.
- `cargo test -p studiofs-vfs --lib upload_lifecycle::tests -- --nocapture` — 19 passed.
- `cargo fmt --all -- --check` and `git diff --check` after the review follow-up.

The full VFS suite and native mounted acceptance were not run. No PR was requested.

Follow-up review validation passed:

- `cargo test -p studiofs-vfs --lib shutdown_ -- --nocapture` — 20 passed.
- `cargo check -p studiofs-vfs`.
- `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`.
- `cargo fmt --all -- --check` and `git diff --check`.

The latest inline review follow-up adds a deterministic regression that holds the inode write gate through shutdown completion, then verifies the recorded cleanup error and cleared queue counters. A separate assertion ensures a cancelled compaction join does not emit the completion event.

Latest validation passed:

- `cargo test -p studiofs-vfs --lib shutdown_cancels_deferred_cleanup_waiting_for_delete_gate -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib deferred_namespace_entry_conflict_does_not_degrade_mount -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib local_compaction_join_ -- --nocapture` — 4 passed.
- `cargo check -p studiofs-vfs`.
- `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`.
- `cargo fmt --all` and `git diff --check`.

Latest review validation passed:

- `cargo test -p studiofs-vfs --lib direct_namespace_mutations_obey_shutdown_admission_gate -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib shutdown_waits_for_deferred_create_admitted_before_cleanup_registration -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib shutdown_bounds_wait_for_admitted_mutations_and_can_retry -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib shutdown_flushes_dirty_attrs_with_prior_cleanup_errors -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib shutdown_cancels_deferred_cleanup_waiting_for_delete_gate -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib shutdown_waits_for_admitted_mutation_that_registers_cleanup -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib deferred_namespace_admission_stops_after_shutdown_requested -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib deferred_namespace_durable_mutations_wait_for_admitted_entries -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs --lib deferred_attr_only_commit_does_not_revert_written_size -- --nocapture` — 1 passed.
- `cargo check -p studiofs-vfs`.
- `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`.
- `cargo fmt --all -- --check` and `git diff --check`.

The full VFS suite and native mounted acceptance were not run.

## Revision Note

Created on 2026-09-28 after verifying the issue's named spawn sites and shutdown sequence. The first regression run failed as expected: the tracker contained 0 retry handles instead of 1. Final focused regressions, check, Clippy and diff review passed; implementation stays within the VFS lifecycle and its focused regression.

Revision note (2026-09-28): the shutdown task-ownership review identified a producer race outside the tracked task lists. Updated the plan to close synchronous mutation admission before draining; the new regression failed before that barrier and passed afterward.

Revision note (2026-09-28): the follow-up review found uncovered synchronous metadata mutators, an unbounded gate wait, and overbroad filtering by `cleanup_errors`. Added shared admission coverage, a retryable two-second gate timeout, and a shutdown-specific skipped-cleanup set; focused tests, check, Clippy, formatting, and diff checks passed.

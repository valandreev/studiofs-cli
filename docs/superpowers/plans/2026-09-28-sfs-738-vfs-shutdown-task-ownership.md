# Own VFS Storage Work Through Shutdown

This ExecPlan is living and follows `.agent/PLANS.md`. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as the work proceeds.

When this change is complete, a graceful VFS shutdown will wait for all retry and deferred-cleanup work that can retain the local metadata or cache stores. A clean shutdown will then allow the existing test VFS to reopen its redb metadata and cache immediately.

`StudioVfs::shutdown` in `crates/studiofs-vfs/src/vfs/upload_lifecycle.rs` first stops and joins background workers, then drains local dirty state and persists local metadata/cache. The final dirty drain can register more work after the first join. Epoch retry scheduling lives in `crates/studiofs-vfs/src/vfs/workers.rs`; deleted-inode cache cleanup lives in `crates/studiofs-vfs/src/vfs/deletion_cleanup.rs`. This change keeps those owners in `ShutdownState` (`crates/studiofs-vfs/src/vfs.rs`) and joins them before persistence/close.

## Progress

- [x] Read SFS-738, related SFS-414, the VFS lifecycle contract, and the affected task paths.
- [x] Add the retry/reopen regression and confirm it fails because the retry handle is absent from shutdown tracking.
- [x] Extend the regression to cover Tokio cleanup and the no-runtime cleanup thread.
- [x] Register retry work, preserve ownership of blocking/thread cleanup, and join work produced by the final dirty drain.
- [x] Run focused regressions, VFS check and Clippy; inspect the final diff.
- [x] Verify the shutdown review against current call paths; reap completed handles, preserve panic reporting, and simplify the duplicated wait loops.

## Milestones

The first milestone adds a synchronized regression around the current shutdown owner. The regression schedules a delayed epoch retry, schedules deleted-inode cleanup through both available execution paths, shuts down, verifies no tracked storage work remains, and reopens the same local redb/cache fixture. It must fail before the ownership change because the retry and non-Tokio thread are not in shutdown state.

The second milestone makes shutdown retain every relevant handle. Retry waits use the shutdown cancellation token and are registered with the existing task list. Tokio cleanup wrappers are tracked in a non-abortable list; `run_local_cache_io_blocking` awaits its blocking operation, so shutdown keeps the wrapper alive until that operation releases the cache. Cleanup started without a Tokio runtime keeps its OS thread handle and completion signal in shutdown state. Shutdown joins again after the final dirty drain, before uploader close and local persistence.

The final milestone runs the single regression, `cargo check -p studiofs-vfs`, and `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`, then checks formatting and the diff. A passing unit regression proves the local reopen boundary; it does not claim native mounted acceptance.

## Surprises & Discoveries

- `StudioVfs::shutdown` takes one snapshot of `background_tasks` before `flush_pending_payloads_for_shutdown`, even though the final drain can schedule namespace and deleted-inode cleanup.
- The epoch retry uses `tokio::spawn` without registering its handle and waits on `Notify::notify_waiters`, which does not retain a permit for a future waiter created after notification.
- The Tokio cleanup path currently tracks an async wrapper around `run_local_cache_io_blocking`; aborting that wrapper can drop the handle while its already-running blocking closure continues.
- The no-runtime cleanup path discards the `std::thread::JoinHandle`.
- Reaping Tokio handles through `is_finished()` alone would discard completed panic results; a nonblocking poll keeps panic reporting while releasing completed handles.
- `Vec::remove(0)` shifted remaining handles even though completion order is not observable, so shutdown can use `swap_remove(0)`.

## Decision Log

- Keep the existing task tracker and shutdown ordering; add only the ownership needed for blocking and OS-thread cleanup.
- Use the already-owned `background_shutdown_token` for retry cancellation so a late waiter observes shutdown.
- Keep timeout cancellation for ordinary async workers. Blocking cleanup and native cleanup threads must remain owned and finish before local stores can be reopened.
- Repeat the task drain after shutdown-generated dirty/namespace work; no task-admission framework is needed for this bounded VFS lifecycle fix.
- Treat cleanup timeouts as a grace/warning threshold, not a release deadline: SFS-738's Linear risk notes prohibit detaching owners while they may retain local stores.
- Keep cleanup thread creation under the shutdown task-list lock so a thread is published atomically with its registration; keep direct namespace cleanup on its verified Tokio-only call path.

## Outcomes & Retrospective

Shutdown now cancels and joins epoch retries, waits for deferred Tokio cleanup and no-runtime cleanup threads, and repeats the task drain after the final local dirty flush. A regression confirmed the retry handle is absent before the fix; after the fix, both focused tests passed and the same local redb/cache fixture reopened with its written bytes intact.

Review follow-up reaps completed Tokio and native thread handles as new work is registered, logs completed task/thread panics, checks every join result after abort, shares the `JoinHandle` timeout policy, factors the native-thread waiter, and replaces front-removal shifts with `swap_remove`. Review requests for a hard shutdown deadline, runtime-less cleanup fallback, spawn-before-registration, and one shared warning budget were rejected because they conflict with storage ownership, have no runtime caller, or add policy without a demonstrated need; the synthetic cleanup IDs cover task registration while the regression also writes and reads real local data across reopen.

Validation passed:

- `cargo test -p studiofs-vfs shutdown_owns_epoch_retry_before_reopening_local_stores --lib -- --nocapture` — 1 passed.
- `cargo test -p studiofs-vfs deferred_cleanup_join_waits_past_timeout_without_aborting_task --lib -- --nocapture` — 1 passed.
- `cargo check -p studiofs-vfs`.
- `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`.
- `cargo test -p studiofs-vfs --lib upload_lifecycle::tests -- --nocapture` — 19 passed.
- `cargo fmt --all -- --check` and `git diff --check` after the review follow-up.

The full VFS suite and native mounted acceptance were not run. No PR was requested.

## Revision Note

Created on 2026-09-28 after verifying the issue's named spawn sites and shutdown sequence. The first regression run failed as expected: the tracker contained 0 retry handles instead of 1. Final focused regressions, check, Clippy and diff review passed; implementation stays within the VFS lifecycle and its focused regression.

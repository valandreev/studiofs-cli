# Make StudioVfs shutdown idempotent and close the UploadQueue wake race

This ExecPlan is a living document and must remain aligned with `.agent/PLANS.md`.
It records the narrow implementation and validation for SFS-638.

## Purpose / Big Picture

Concurrent filesystem teardown callers must observe one completed shutdown
outcome. A failed durability barrier must not be hidden by a later `Ok(())`,
and an upload-idle waiter must not sleep after the final upload has completed.
The result is visible through deterministic shared-core tests; the same
`StudioVfs::shutdown` path is used by the mount session and WinFSP adapter,
while Linux/FUSE and macFUSE use the shared VFS lifecycle through their mount
runtime.

## Progress

- [x] (2026-08-05 20:26 Europe/Kyiv) Confirm the clean branch, Linear scope, current shutdown and queue implementations, and adapter entry paths.
- [x] (2026-08-05 20:26 Europe/Kyiv) Approve the design: one shutdown phase gate and one mutex-protected queue state/notification protocol.
- [x] (2026-08-05 20:27 Europe/Kyiv) Add deterministic RED tests for the lost wake and concurrent error propagation; both failed before the fix.
- [x] (2026-08-05 20:31 Europe/Kyiv) Implement queue linearization and the shutdown phase gate without changing close ordering.
- [x] (2026-08-05 20:36 Europe/Kyiv) Run shared crate tests, integration selection, lint/check/format hygiene.
- [x] (2026-08-05 20:37 Europe/Kyiv) Record final evidence and the unavailable native mount cells.

## Surprises & Discoveries

- `UploadQueue` already has a deterministic test hook, but the test is ignored
  with `#[should_panic]`; the current implementation really constructs
  `idle_notify.notified()` after loading the count.
- `pending_state` already guards both pending maps. The separate atomic count is
  therefore the source of the map/count observation gap; deriving the count
  from the existing guard avoids a second synchronization protocol.
- `VfsError` is not cloneable because several variants contain I/O or task
  errors. Completed shutdown state must store a replayable public error
  representation for waiters while preserving the existing close order.

## Decision Log

- Decision: Use a short `parking_lot::Mutex` phase gate with `Notify`; the owner
  runs the existing shutdown body outside the lock and waiters register before
  inspecting the phase. Rationale: it linearizes ownership without holding a
  lock across async work or adding a global worker lock. Date/Author:
  2026-08-05 / Codex.
- Decision: Replace `UploadQueue::pending_count` as an independent atomic with
  `pending_state.by_object.len()`. Rationale: both indexes and the count then
  share one existing mutex and the notification is tied to the same state
  transition. Date/Author: 2026-08-05 / Codex.
- Decision: Preserve the original shutdown error for the owner and replay its
  stable display message as `VfsError::Other` for later callers. Rationale:
  `VfsError` cannot be cloned without broadening its API, while every caller
  still receives the same success/error outcome and message. Date/Author:
  2026-08-05 / Codex.

## Outcomes & Retrospective

- RED evidence: the queue regression timed out after 100 ms and the concurrent
  shutdown regression observed `Ok(())` from the second caller.
- GREEN evidence: `cargo test -p studiofs-cache --lib` passed 164 tests and
  `cargo test -p studiofs-vfs --lib` passed 851 tests. With the workspace
  `.env` loaded and `--ignored`, `test_double_shutdown_does_not_panic` passed
  against the configured S3/PG environment.
- Hygiene evidence: `RUSTFLAGS=-Dwarnings cargo check -p studiofs-cache -p
  studiofs-vfs`, `cargo fmt --all -- --check`, and `git diff --check` passed.
- Native mounted macFUSE/Linux-FUSE/WinFSP smoke tests were not run in this
  validation; no result is inferred from the shared-core or S3/PG tests.

## Context and Orientation

`crates/studiofs-vfs/src/vfs/upload_lifecycle.rs` owns the public asynchronous
`StudioVfs::shutdown` sequence. It quiesces background work, drains accepted
local state, waits for uploads, persists metadata/cache state, and returns the
first durability error. `crates/studiofs-vfs/src/vfs.rs` owns the shared
`ShutdownState` used by admission guards and background workers.

`crates/studiofs-cache/src/upload_queue.rs` owns the in-memory pending upload
maps and the Tokio notification used by `wait_for_idle`. The queue receiver is
consumed by the VFS upload worker after startup, while unstarted test fixtures
retain it so a pending operation can provide a deterministic wait barrier.

The mount session calls `vfs.shutdown` in
`crates/studiofs/src/cli/mount/session.rs`; WinFSP calls the same API in
`crates/studiofs-winfsp/src/lib.rs`. No adapter-specific shutdown algorithm is
needed for this issue.

## Plan of Work

First enable the existing lost-wake test and add a VFS test that starts one
shutdown owner with a pending queue operation, waits until shutdown admission
is closed, and calls shutdown again. The pre-fix test must observe the first
call's error and fail because the concurrent caller returns `Ok(())`.

Next make `UploadQueue::PendingState` authoritative for the pending count.
Create the idle notification future before the state/count check, check the
guarded map, and notify only when a removal transitions the map to empty.
Keep duplicate, stale, requeue, and sender-closed semantics unchanged.

Then replace the completed/requested shutdown short-circuits with a phase gate:
`Running`, `ShuttingDown`, or `Completed(outcome)`. The first caller becomes
owner; later callers wait for completion. Move the existing shutdown body into
the owner path without changing its phase ordering, publish `Completed` only
after the result is determined, and wake waiters afterward. Keep the existing
`shutdown_requested` flag for stopping new work and background workers.

Finally add focused checks for the changed crates, formatting, warnings, and
diff hygiene. Run platform-gated shared/adapter tests where this host supports
them and report native mount runners separately when unavailable.

## Milestones

### M0 — RED regression tests

The queue and VFS tests fail for the known reasons: a completed last upload
does not wake the waiter, and a concurrent shutdown caller returns `Ok(())`
instead of the owner's error.

### M1 — Shared lifecycle fix

The queue and shutdown gates are implemented with existing mutex/notification
primitives. The focused tests pass, and all existing queue/shutdown tests keep
their previous semantics.

### M2 — Validation and handoff

The shared crates pass focused tests and warning checks. macFUSE, Linux/FUSE,
WinFSP, PostgreSQL, and S3 cells are recorded as pass or unavailable rather
than inferred from a non-native host.

## Concrete Steps

Run commands from `/Volumes/Storage/dev/studiofs`.

    cargo test -p studiofs-cache wait_for_idle_wakes_after_last_completion -- --nocapture
    cargo test -p studiofs-vfs concurrent_shutdown_callers_observe_first_error -- --nocapture
    cargo test -p studiofs-cache upload_queue -- --nocapture
    cargo test -p studiofs-vfs vfs::upload_lifecycle -- --nocapture
    cargo test -p studiofs-vfs vfs::tests -- --nocapture
    RUSTFLAGS='-D warnings' cargo check -p studiofs-cache -p studiofs-vfs
    cargo fmt --all -- --check
    git diff --check

Use one Cargo test filter per command to avoid workspace file-lock contention.
Do not run destructive cleanup outside the existing `target/` test helpers.

## Validation and Acceptance

The queue regression passes only when a waiter that has registered before the
completion transition returns promptly and the pending map/count state is
empty together. The shutdown regression passes only when concurrent and later
callers receive the same first error message, while a successful shutdown
continues to return success for all callers.

The focused cache/VFS tests, `RUSTFLAGS='-D warnings' cargo check`, formatter,
and `git diff --check` are required local evidence. Native mounted shutdown
smoke tests are additional evidence only when the corresponding macFUSE,
Linux/FUSE, or WinFSP runner is available.

## Idempotence and Recovery

All fixtures use existing `target/`-backed temporary directories and are safe
to rerun. The implementation must not reset or discard unrelated branch work.
If a focused test fails after the fix, return to the failing transition and
change one hypothesis at a time; do not weaken the race test or increase a
timeout to hide the bug.

## Artifacts and Notes

Keep generated test databases and logs under `target/`. Do not commit logs,
temporary artifacts, or platform-specific output. Update this plan with the
final focused command results and unavailable matrix cells.

## Interfaces and Dependencies

No new crate or public abstraction is required. The implementation uses the
existing `parking_lot::Mutex`, Tokio `Notify`, `PendingUpload`, `VfsError`, and
`StudioVfs` lifecycle methods. The public shutdown signature and upload queue
API remain unchanged.

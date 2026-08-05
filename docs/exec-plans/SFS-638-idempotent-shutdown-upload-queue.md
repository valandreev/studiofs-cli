# Make StudioVfs shutdown idempotent and close the UploadQueue wake race

This ExecPlan is a living document and must remain aligned with `.agent/PLANS.md`.
It records the narrow implementation and validation for SFS-638.

## Purpose / Big Picture

Concurrent filesystem teardown callers must observe one completed shutdown
outcome. A failed durability barrier must not be hidden by a later `Ok(())`,
an upload-idle waiter must not sleep after the final upload has completed, and
an owner future cancelled mid-shutdown must leave a retryable phase. The result
is visible through deterministic shared-core tests plus mounted entry-path
checks for Linux/FUSE, macFUSE, and WinFSP.

## Progress

- [x] (2026-08-05 20:26 Europe/Kyiv) Confirm the clean branch, Linear scope, current shutdown and queue implementations, and adapter entry paths.
- [x] (2026-08-05 20:26 Europe/Kyiv) Approve the design: one shutdown phase gate and one mutex-protected queue state/notification protocol.
- [x] (2026-08-05 20:27 Europe/Kyiv) Add deterministic RED tests for the lost wake and concurrent error propagation; both failed before the fix.
- [x] (2026-08-05 20:31 Europe/Kyiv) Implement queue linearization and the shutdown phase gate without changing close ordering.
- [x] (2026-08-05 20:36 Europe/Kyiv) Run shared crate tests, integration selection, lint/check/format hygiene.
- [x] (2026-08-05 20:37 Europe/Kyiv) Record final evidence and the unavailable native mount cells.
- [x] (2026-08-05 21:49 Europe/Kyiv) Reproduce review findings with RED tests for local-payload error propagation, typed replay, and cancelled-owner retry.
- [x] (2026-08-05 21:49 Europe/Kyiv) Implement replayable typed shutdown errors, the retryable `Aborted` phase, and first-error selection including local-payload sync.
- [x] (2026-08-05 21:49 Europe/Kyiv) Add mounted double-shutdown assertions to Linux/FUSE, macFUSE, and WinFSP entry-path smoke tests.

## Surprises & Discoveries

- `UploadQueue` already has a deterministic test hook, but the test is ignored
  with `#[should_panic]`; the current implementation really constructs
  `idle_notify.notified()` after loading the count.
- `pending_state` already guards both pending maps. The separate atomic count is
  therefore the source of the map/count observation gap; deriving the count
  from the existing guard avoids a second synchronization protocol.
- `VfsError` is not cloneable because several variants contain I/O or task
  errors. Completed shutdown state stores a replayable typed representation;
  metadata and local-payload errors retain their public variants, while other
  shutdown errors retain their stable message.
- A cancelled owner can leave already-quiesced state behind, so the phase gate
  uses `Aborted` and lets the next caller claim ownership and finish the same
  idempotent cleanup path.

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
- Decision: Store a replayable shutdown error representation and reconstruct
  the same public error family for the owner and later callers. Rationale:
  `VfsError` cannot be cloned directly, but converting only at the completion
  boundary avoids changing the public error API and preserves typed metadata
  and local-payload failures. Date/Author: 2026-08-05 / Codex.
- Decision: Publish `Aborted` rather than `Completed` when the owner future is
  cancelled. Rationale: a later caller must be able to claim ownership and
  finish cleanup after a partial async shutdown. Date/Author: 2026-08-05 /
  Codex.

## Outcomes & Retrospective

- RED evidence: the queue regression timed out after 100 ms and the concurrent
  shutdown regression observed `Ok(())` from the second caller. The review
  regressions initially failed to compile because typed outcome helpers and
  the `Aborted` phase were absent.
- GREEN evidence: `cargo test -p studiofs-cache --lib` passed 164 tests and
  `cargo test -p studiofs-vfs --lib` passed 854 tests. The five named SFS-638
  focused regressions passed individually. With the workspace `.env` loaded,
  the ignored `test_double_shutdown_does_not_panic` integration passed 1/1.
- Hygiene evidence: `RUSTFLAGS=-Dwarnings cargo check -p studiofs-cache -p
  studiofs-vfs`, `cargo fmt --all -- --check`, and `git diff --check` passed.
- Native mounted evidence: the macFUSE mounted write/close smoke passed 1/1
  outside the sandbox, including the repeated VFS shutdown assertions. Linux
  FUSE is unavailable on this Darwin host (`/dev/fuse` is absent), and WinFSP
  is unavailable because this is not a Windows runner.

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

First enable the existing lost-wake test and add VFS tests that cover first
error selection, typed replay, and a cancelled owner. The cancelled-owner test
must abort an owner while deferred namespace drain is pending, observe
`Aborted`, and let a later caller complete shutdown.

Next make `UploadQueue::PendingState` authoritative for the pending count.
Create the idle notification future before the state/count check, check the
guarded map, and notify only when a removal transitions the map to empty.
Keep duplicate, stale, requeue, and sender-closed semantics unchanged.

Then replace the completed/requested shutdown short-circuits with a phase gate:
`Running`, `ShuttingDown`, `Aborted`, or `Completed(outcome)`. The first caller
becomes owner; later callers wait for completion. Move the existing shutdown
body into the owner path without changing its phase ordering, publish
`Completed` only after the result is determined, and wake waiters afterward.
If the owner future is cancelled, publish `Aborted` so the next caller can
retry. Keep the existing `shutdown_requested` flag for stopping new work and
background workers.

Add the same double-shutdown assertion after mounted file-path teardown in the
Linux/FUSE and macFUSE smoke tests, and in the shared WinFSP mount-sanity
cleanup helper. These checks exercise the bridge entry paths without changing
the platform-specific unmount protocol.

Finally add focused checks for the changed crates, formatting, warnings, and
diff hygiene. Run platform-gated shared/adapter tests where this host supports
them and report native mount runners separately when unavailable.

## Milestones

### M0 — RED regression tests

The queue and VFS tests fail for the known reasons: a completed last upload
does not wake the waiter, a local-payload sync error becomes `Ok(())`, typed
replay helpers are absent, and a cancelled owner leaves no retryable phase.

### M1 — Shared lifecycle fix

The queue and shutdown gates are implemented with existing mutex/notification
primitives. The focused tests pass, and all existing queue/shutdown tests keep
their previous semantics.

### M2 — Validation and handoff

The shared crates pass focused tests and warning checks. Mounted macFUSE,
Linux/FUSE, and WinFSP cells are recorded as pass or unavailable rather than
inferred from a non-native host; the mounted smoke code contains the same
double-shutdown assertions on each platform.

## Concrete Steps

Run commands from `/Volumes/Storage/dev/studiofs`.

    cargo test -p studiofs-cache wait_for_idle_wakes_after_last_completion -- --nocapture
    cargo test -p studiofs-vfs concurrent_shutdown_callers_observe_first_error -- --nocapture
    cargo test -p studiofs-vfs cancelled_shutdown_owner_can_be_retried -- --nocapture
    cargo test -p studiofs-vfs shutdown_result_reports_local_payload_sync_failure -- --nocapture
    cargo test -p studiofs-vfs shutdown_replay_preserves_typed_error_for_owner_and_waiter -- --nocapture
    cargo test -p studiofs-cache upload_queue -- --nocapture
    cargo test -p studiofs-vfs vfs::upload_lifecycle -- --nocapture
    cargo test -p studiofs-vfs vfs::tests -- --nocapture
    RUSTFLAGS='-D warnings' cargo check -p studiofs-cache -p studiofs-vfs
    cargo fmt --all -- --check
    git diff --check

Expected focused-test transcript (one command at a time):

    running 1 test
    test ... ... ok
    test result: ok. 1 passed; 0 failed

Expected hygiene transcript:

    Finished `dev` profile ...
    (no output from `cargo fmt --all -- --check` or `git diff --check`)

Use one Cargo test filter per command to avoid workspace file-lock contention.
Do not run destructive cleanup outside the existing `target/` test helpers.

## Validation and Acceptance

The queue regression passes only when a waiter that has registered before the
completion transition returns promptly and the pending map/count state is
empty together. The shutdown regressions pass only when concurrent and later
callers receive the same typed first error, a local-payload sync failure is
returned instead of `Ok(())`, and a later caller can finish after owner
cancellation. A successful mounted entry-path shutdown continues to return
success for repeated callers.

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
final focused command results and unavailable matrix cells. The mounted
macFUSE run required the host mount permission; the sandbox-only attempt was
rejected with `Operation not permitted` before the test assertion.

## Interfaces and Dependencies

No new crate or public abstraction is required. The implementation uses the
existing `parking_lot::Mutex`, Tokio `Notify`, `PendingUpload`, `VfsError`, and
`StudioVfs` lifecycle methods. The public shutdown signature and upload queue
API remain unchanged.

## Revision Note

2026-08-05: Revised after code review to preserve typed shutdown errors,
propagate local-payload sync failures, make owner cancellation retryable, add
mounted double-shutdown assertions for all three bridge entry paths, register
the new named regressions in `docs/tests.md`, and record expected command
transcripts required by `.agent/PLANS.md`.

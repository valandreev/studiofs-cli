# Extract the deferred materialization pass into an outcome-driven private function

This ExecPlan is a living document for SFS-641. It follows `.agent/PLANS.md` and must be kept current while the change is implemented.

## Purpose / Big Picture

This change makes the shared deferred materialization state machine easier to review and modify without changing filesystem behavior. Users should observe no new feature; the existing background materialization, linger, interruption, rescheduling, metadata flush, and retry-at-strict-barrier behavior must remain identical. The result is demonstrated by the existing VFS materialization tests plus focused coverage for the extracted outcomes and an interruption/reschedule sequence.

## Progress

- [x] (2026-08-06 20:00 Europe/Kyiv) Read the SFS-641 Linear scope and confirmed the issue branch is clean and already `In Progress`.
- [x] (2026-08-06 20:05 Europe/Kyiv) Traced `spawn_deferred_materialization_if_needed`, `start_deferred_materialization_task`, and `finish_deferred_materialization_task`, including all callers of the scheduler entry point.
- [x] (2026-08-06 20:10 Europe/Kyiv) Reviewed the existing interruption, linger, tail, and reschedule tests in `crates/studiofs-vfs/src/vfs/tests.rs`.
- [x] (2026-08-06 20:15 Europe/Kyiv) Chose a private async pass plus a small outcome enum; the user approved this design.
- [x] (2026-08-06 20:20 Europe/Kyiv) Wrote and self-reviewed this design/ExecPlan.
- [x] (2026-08-06 20:25 Europe/Kyiv) User reviewed the written spec and authorized implementation.
- [x] (2026-08-06 20:32 Europe/Kyiv) Added the red idle-outcome test, observed the expected missing-symbol failure, and committed it as `cb6e7982`.
- [x] (2026-08-06 20:45 Europe/Kyiv) Extracted the pass and outcome enum while preserving scheduling and durability behavior; committed as `ec220561`.
- [x] (2026-08-06 20:55 Europe/Kyiv) Ran 14 deferred-materialization VFS tests, one macFUSE adapter materialization test, formatting, `-D warnings` check, and diff hygiene successfully.
- [x] (2026-08-06 20:58 Europe/Kyiv) Recorded final evidence in Linear SFS-641 and moved it to `In Review`; mounted Linux/FUSE, macFUSE, and WinFSP validation remains a documented environment gap.

## Surprises & Discoveries

- The scheduler entry point has four independent callers (`background_aggressive_materialization`, `flush_volatile_writeback_with_outcome`, `maybe_pre_drain_sustained_write_buffer`, and `write_fh_payload_with_deferred_materialize`), so the extraction must remain in the shared VFS layer rather than in a platform adapter.
- The current task combines three different terminal concerns: materialization progress, linger interruption, and whether the in-flight gate may reschedule. The outcome must preserve all three without moving `finish_deferred_materialization_task` into the pass.
- Existing tests already cover interruption and in-flight rescheduling behavior. The new focused checks should name the outcomes explicitly while retaining those behavioral tests unchanged in meaning.
- The shared VFS filter passed all 14 matching tests after the extraction, and the macFUSE adapter scheduling test also passed. No mounted bridge runner was available for the full OS matrix.

## Decision Log

- Decision: Keep task spawning, semaphore guard ownership, cancellation-sensitive finalization, metadata flush, and `finish_deferred_materialization_task` in `start_deferred_materialization_task`.
  Rationale: These operations define the task lifecycle and gate ownership; moving them into the pass would broaden the refactor and risk cancellation or rescheduling changes.
  Date/Author: 2026-08-06 / Codex with user approval.

- Decision: Use a private `DeferredMaterializationPassOutcome` enum instead of traits, strategy objects, or a scheduler manager.
  Rationale: There are two materialization modes but one shared state machine. An enum makes the existing branches explicit with the smallest new surface.
  Date/Author: 2026-08-06 / Codex with user approval.

- Decision: Treat materialization errors as terminal outcomes that disallow immediate rescheduling, while preserving the existing warning/suppression and strict-barrier retry behavior.
  Rationale: The current code sets `allow_reschedule_on_exit = false` for every pass failure. The refactor must carry that fact explicitly rather than silently defaulting it to true.
  Date/Author: 2026-08-06 / Codex with user approval.

- Decision: Do not touch macFUSE, FUSE, or WinFSP adapter code.
  Rationale: Deferred materialization is shared VFS code and the extraction is platform-neutral; the existing bridge contract is preserved by keeping the scheduling entry point unchanged.
  Date/Author: 2026-08-06 / Codex with user approval.

## Outcomes & Retrospective

The outer task is now a short lifecycle wrapper around the pass outcome loop, while interrupted cleanup, metadata flush, and gate finalization remain outside the pass. The shared VFS deferred-materialization filter passed 14/14 tests, and the macFUSE adapter scheduling test passed 1/1. Formatting, `RUSTFLAGS='-D warnings' cargo check -p studiofs-vfs`, and `git diff --check` passed. No platform-specific scheduler branch was introduced. Mounted Linux/FUSE, macFUSE, and WinFSP scenarios were not run because those runners were unavailable in this environment; Linear remains `In Review` pending that matrix.

## Context and Orientation

`crates/studiofs-vfs/src/vfs/sync_ops.rs` contains the shared VFS write and synchronization operations. `StudioVfs::spawn_deferred_materialization_if_needed` registers an inode in the deferred-materialization gate and starts the background task. The current `StudioVfs::start_deferred_materialization_task` at approximately lines 374–628 owns the full loop: it samples dirty bytes, prefetches completed-block object IDs, runs both materialization modes, handles closed-writer and idle-tail materialization, waits for linger or interruption, and finally flushes local metadata before releasing the gate. `StudioVfs::finish_deferred_materialization_task` at approximately lines 634–658 consumes the gate entry and starts a replacement task when a write arrived while the previous task was active.

`DeferredInodeGateEntry` is defined in `crates/studiofs-vfs/src/vfs.rs`. Its `wait_linger_or_interrupt` method returns whether a strict flush or handle release interrupted the wait. The `entry` must remain borrowed by the pass and owned by the outer task until finalization so cancellation and gate-drop behavior remain unchanged.

The relevant behavioral tests live in `crates/studiofs-vfs/src/vfs/tests.rs`, including clean and tail linger interruption, write rescheduling while a task is in flight, sparse-tail behavior, and materialization-drain scenarios. All three platform bridges invoke the shared VFS methods; this task must not add platform conditionals.

## Plan of Work

First, add a private outcome type near the deferred-materialization helpers in `sync_ops.rs`. The intended variants are `Idle`, `Reschedule`, `Interrupted`, and `Completed { allow_reschedule: bool }`. `Idle` means the inode is clean and no clean-writer linger remains. `Reschedule` means the pass made progress or must take another scheduled pass. `Interrupted` means the linger wait ended because an external strict barrier or release interrupted it. `Completed` means the pass is terminal; its boolean preserves whether `finish_deferred_materialization_task` may honor a queued reschedule. A pass error returns `Completed { allow_reschedule: false }` after retaining the existing suppression and warning behavior.

Next, add the private async method `run_deferred_materialization_pass` on `StudioVfs`. It will receive the current `DeferredInodeGateEntry` by reference and the mutable no-progress retry counter. It will contain the current dirty snapshot, prefetch, completed-block materialization, limits materialization, closed-writer materialization, idle-tail materialization, linger wait, and progress decision. It will not spawn tasks, own the semaphore permit, flush local metadata, or finish the in-flight gate.

Then shorten `start_deferred_materialization_task` so it owns only the outer task lifecycle. It will keep the guard alive, repeatedly call the pass, match its outcome, perform the existing interrupted final materialization when required, flush local dirty metadata once, and call `finish_deferred_materialization_task` with the outcome-derived reschedule permission. The current timers, `yield_now` points, warning text, error suppression, and ordering will be moved without semantic changes.

Finally, add or retain focused tests. Outcome coverage will exercise clean idle termination, progress-driven rescheduling, interruption, and terminal completion/error handling. The existing interruption and in-flight rescheduling scenarios will remain behavior tests, with one explicit sequence proving that a write arriving during an active task is handled by the replacement task after the first task finishes.

## Concrete Steps

Run all commands from `/Volumes/Storage/dev/studiofs`.

Before editing, confirm the worktree contains only the intended branch state:

    git status --short --branch

For the TDD red phase, add the smallest focused test that names the new outcome contract or calls the new private pass through the test-only path, then run its one Cargo filter and verify it fails because the outcome/pass is not implemented rather than because of an unrelated setup error.

During the green phase, implement only the enum, pass extraction, and required test support. Run each focused filter separately to avoid Cargo workspace lock contention:

    cargo test -p studiofs-vfs deferred_materialization -- --nocapture
    cargo test -p studiofs-vfs strict_flush_interrupts_deferred_materialization_linger -- --nocapture
    cargo test -p studiofs-vfs last_handle_release_interrupts_deferred_materialization_linger -- --nocapture
    cargo test -p studiofs-vfs deferred_materialization_reschedules_write_that_arrives_while_inflight_finishes -- --nocapture

After the focused tests pass, run formatting and the crate-level warning check:

    cargo fmt --all -- --check
    RUSTFLAGS='-D warnings' cargo check -p studiofs-vfs
    git diff --check

If the repository's platform runners are available, run the smallest mounted materialization scenario for each supported bridge. If a runner, macFUSE/FUSE mount, or WinFSP environment is unavailable, record that as a validation gap rather than treating it as a pass.

## Validation and Acceptance

Acceptance is behavioral and structural together. The background task must still finish cleanly for a clean inode, continue when completed blocks remain, perform the final materialization after an interrupted linger, and allow a replacement task after a write arrives during an in-flight task. Existing metadata flush ordering and strict-barrier retry behavior must remain unchanged.

The focused VFS tests must pass, including the new outcome checks and existing materialization tests. `cargo fmt --all -- --check`, `RUSTFLAGS='-D warnings' cargo check -p studiofs-vfs`, and `git diff --check` must also pass. Review the diff to confirm the outer task contains lifecycle orchestration only, the extracted pass is private and platform-neutral, and no public API, adapter, timer, or scheduler policy changed.

## Idempotence and Recovery

The code edits are additive until the old loop is removed after the new pass compiles and the focused tests pass. If a test fails, keep the failing test and correct the implementation; do not weaken the assertion or alter the existing scheduling contract. If formatting or lint changes unrelated files, inspect and revert only those unrelated changes before continuing. No data migration, mount state, or destructive operation is required.

## Artifacts and Notes

The primary artifact is the focused diff in `crates/studiofs-vfs/src/vfs/sync_ops.rs` and its tests. The design/ExecPlan is this file. Final evidence should include the focused test result, warning check, formatting result, and any platform-runner gap.

## Interfaces and Dependencies

In `crates/studiofs-vfs/src/vfs/sync_ops.rs`, keep the existing scheduler entry point and finalizer signatures. Add only private implementation details, with the intended pass interface:

    enum DeferredMaterializationPassOutcome {
        Idle,
        Reschedule,
        Interrupted,
        Completed { allow_reschedule: bool },
    }

    impl StudioVfs {
        async fn run_deferred_materialization_pass(
            &self,
            entry: &DeferredInodeGateEntry,
            no_progress_retries: &mut usize,
        ) -> DeferredMaterializationPassOutcome;
    }

The pass uses existing `StudioVfs` helpers only: writer-stack dirty-byte and completed-block queries, object-ID prefetch, both completed-block materialization methods, closed-writer and idle-tail materialization, `DeferredInodeGateEntry::wait_linger_or_interrupt`, existing metrics, warning suppression, and existing local metadata flushing in the outer task. No new crate, trait, public visibility, platform dependency, or scheduler abstraction is permitted.

## Change Note

2026-08-06: Created after the user approved the outcome-based extraction design. The plan deliberately keeps task lifecycle and gate finalization outside the pass to protect cancellation and rescheduling semantics.

2026-08-06: Implementation completed in `cb6e7982` and `ec220561`; focused core and macFUSE adapter validation passed. The issue was moved to `In Review` because mounted validation for Linux/FUSE, macFUSE, and WinFSP was not available.

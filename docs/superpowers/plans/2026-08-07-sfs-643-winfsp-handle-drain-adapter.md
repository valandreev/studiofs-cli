# Move WinFSP Handle-Drain Orchestration to the Adapter

This ExecPlan is a living document and must be maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

SFS-643 removes a WinFSP-only callback state machine from the platform-agnostic VFS crate. After the change, WinFSP owns its callback ordering, per-inode flush gates, deferred cleanup hints, delete cleanup, and callback accounting. The VFS keeps the existing atomic operations and outcome types used by all bridges: volatile writeback flush, last-handle release admission, strict flush/fsync paths, and local-dirty inspection.

The observable result is unchanged: WinFSP non-delete cleanup is observational, explicit flush admits volatile local writeback, the last-handle close admits the deferred release barrier, delete cleanup runs in cleanup, and FUSE bridges continue calling the same VFS operations. The proof is a focused WinFSP unit-test run plus cross-platform bridge compilation.

## Progress

- [x] (2026-08-07) Confirmed Linear issue SFS-643 is already `In Progress` and assigned to Valentine Andreev.
- [x] (2026-08-07) Traced the shared `HandleDrainBarrier` and its six WinFSP one-field port adapters.
- [x] (2026-08-07) Compared WinFSP, macFUSE, and Linux/FUSE callback mappings in `docs/platform-bridges.md` and the bridge implementations.
- [x] (2026-08-07) Moved the orchestration methods into the WinFSP handle-drain facade.
- [x] (2026-08-07) Removed the VFS-only barrier module, ports, generic exports, and their tests.
- [x] (2026-08-07) Ran focused Windows tests, cross-platform checks, Clippy, formatting, and diff validation.
- [x] (2026-08-07) Committed the complete change set as `fd13dfcd`.

## Surprises & Discoveries

- The production WinFSP `close` path already bypasses `HandleDrainBarrier::on_close` and calls `release_fh_with_deferred_last_handle_barrier` directly. The generic `on_close`/`on_release` paths are therefore test-only orchestration, while the adapter already owns the real last-handle release behavior.
- macFUSE and Linux/FUSE do not implement or import the six barrier ports. They call the VFS atomic methods directly, so deleting the platform workflow ports does not require bridge behavior changes.
- WinFSP-focused tests already cover deferred cleanup hints, concurrent flush coalescing, last-handle close, invalidation, flush/purge configuration, and timeout configuration. Keep these tests and only add a regression if the move exposes a missing callback-order assertion.

## Decision Log

- Decision: Keep `StudioVfs::inode_has_local_dirty_state`, `flush_volatile_writeback_with_outcome`, `release_fh_with_outcome`, `release_fh_with_deferred_last_handle_barrier`, and the existing outcome types as the VFS boundary.
  Rationale: These are concrete atomic operations used by multiple bridges; the issue asks to move orchestration, not to duplicate durability or handle-table logic in WinFSP.
  Date/Author: 2026-08-07 / Codex.
- Decision: Implement the moved state machine as private methods on `WinfspHandleDrainFacade` and retain `CallbackBarrierState` for WinFSP-owned gates and hints.
  Rationale: There is one production adapter and no need for a replacement manager, factory, or public trait surface.
  Date/Author: 2026-08-07 / Codex.
- Decision: Do not change macFUSE or Linux/FUSE source files unless compilation proves that a removed VFS export was accidentally used.
  Rationale: Their callback ordering and VFS calls are already independent of the deleted WinFSP-only ports.
  Date/Author: 2026-08-07 / Codex.

## Outcomes & Retrospective

The WinFSP facade now owns the callback orchestration, per-inode flush gate, deferred cleanup hint, delete cleanup, and callback accounting. The VFS-only generic barrier and six one-implementation port adapters were deleted; the concrete VFS atomic operations and their outcomes remain unchanged. macFUSE and Linux/FUSE source files were unchanged and both compile against the reduced VFS surface.

Validation passed on 2026-08-07:

- `cargo check -p studiofs-vfs`
- `cargo check -p studiofs-winfsp`
- `cargo check -p studiofs-macfuse`
- `cargo check -p studiofs-linux-fuse`
- `cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings`
- `cargo fmt -- --check` and `git diff --check`
- 6 `handle_drain` tests, plus focused tests for last-handle close, delete cleanup race, invalidation, timeout overrides, and flush/purge overrides

No native platform toolchain was unavailable for the checks above. The full workspace test suite and mounted runtime scenarios were not run, per repository guidance and task scope.

## Context and Orientation

`crates/studiofs-vfs/src/vfs/handle_drain_barrier.rs` currently defines six public port traits, two request structs, and a six-parameter `HandleDrainBarrier`. It coordinates flush, cleanup, close, release, delete cleanup, and WinFSP observability through one implementation in `crates/studiofs-winfsp/src/context/handle_drain.rs`. `crates/studiofs-vfs/src/vfs.rs` and `crates/studiofs-vfs/src/lib.rs` re-export that WinFSP-only surface.

`crates/studiofs-winfsp/src/context/handle_drain.rs` already contains the actual WinFSP state (`CallbackBarrierState`, `FlushGateRegistry`, and `DeferredCleanupHints`) and six one-field adapter structs. The implementation will retain the state and move the coordination logic into the facade itself. WinFSP callbacks enter through `StudioFsContext::run_flush_callback`, `run_cleanup_non_delete_callback`, `run_close_callback`, and `handle_drain().cleanup` in `crates/studiofs-winfsp/src/context.rs`.

The shared VFS operations remain the source of truth for data and metadata state. `flush_volatile_writeback_with_outcome` admits ordinary platform writeback without strict redb durability; `release_fh_with_deferred_last_handle_barrier` removes the handle and schedules the last-handle local barrier; `release_fh_with_outcome` and strict flush/fsync paths remain available to callers that need synchronous completion. macFUSE and Linux/FUSE use these operations directly.

## Plan of Work

First, copy the behavior of `HandleDrainBarrier::on_flush`, `on_cleanup_non_delete`, `on_cleanup_delete`, and the already-live WinFSP `close` path into private facade methods. The flush method will record the callback, serialize only dirty or hinted inodes through the existing per-inode gate, call `flush_volatile_writeback_with_outcome` through `StudioFsContext::spawn_wait`, record the outcome, and clear the hint. Non-delete cleanup will record the callback and set a hint only for a non-zero handle with local dirty state. Delete cleanup will preserve directory orphan healing, `unlink`/`rmdir` selection, benign-race handling, and observability. The cleanup action enum will become WinFSP-local rather than carrying a VFS request type.

Next, remove the six port traits, request structs, generic barrier, and generic barrier tests from the VFS crate. Remove the module declaration and both public re-export lists. Remove the corresponding WinFSP adapter structs, trait implementations, type alias, and unused imports. Preserve the existing WinFSP-focused tests and update only call sites or helper names required by the private implementation.

Finally, format and validate the smallest relevant surface. Run the WinFSP handle-drain tests, the related WinFSP context tests if needed, `cargo check` for VFS and WinFSP, and `cargo check` for macFUSE and Linux/FUSE. Run `git diff --check` and inspect the diff for unchanged callback order and no remaining `HandleDrainBarrier`/`*Port` references.

## Concrete Steps

Run all commands from `E:\dev\studiofs-cli`.

1. Replace the WinFSP generic facade construction with direct private methods while preserving the existing `CallbackBarrierState`, gates, hints, cleanup action behavior, logging, and callback counters.

2. Remove the VFS module and exports:

    `crates/studiofs-vfs/src/vfs/handle_drain_barrier.rs`

    `crates/studiofs-vfs/src/vfs.rs`

    `crates/studiofs-vfs/src/lib.rs`

3. Confirm no production or test source still references the deleted symbols:

    `rg -n --fixed-strings "HandleDrainBarrier" crates`

    `rg -n "BarrierObservabilityPort|DeferredCleanupHintPort|DeleteCleanupPort|FlushGatePort|HandleReleasePort|LocalSyncBarrierPort|HandleDrainRequest|HandleDrainDeleteRequest" crates`

4. Run focused validation, never the full workspace suite:

    `cargo test -p studiofs-winfsp --lib handle_drain -- --nocapture`

    `cargo test -p studiofs-winfsp --lib context::tests::buffered_write_last_handle_close_owns_fallback_barrier_after_cleanup -- --exact --nocapture`

    `cargo check -p studiofs-vfs`

    `cargo check -p studiofs-winfsp`

    `cargo check -p studiofs-macfuse`

    `cargo check -p studiofs-linux-fuse`

    `cargo clippy -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings`

    `cargo fmt -- --check`

    `git diff --check`

5. Commit all resulting files with an English message identifying SFS-643.

## Validation and Acceptance

Acceptance is behavioral and structural. The focused WinFSP tests must pass, including the cleanup-to-flush/close ordering, concurrent flush gate release, last-handle deferred drain, delete cleanup race, invalidation, flush/purge defaults and override, and timeout parsing tests already present in the WinFSP crate. VFS and all three bridge crates must compile. No `HandleDrainBarrier`, `HandleDrainRequest`, `HandleDrainDeleteRequest`, or six `*Port` definitions may remain in the core VFS crate. The final diff must not change the macFUSE or Linux/FUSE callback sequence.

If a platform check cannot run because its native SDK/runtime is unavailable, report the exact command and error instead of treating a different check as a substitute.

## Idempotence and Recovery

The refactor is source-only and can be retried by reverting only the uncommitted SFS-643 files if a compile check fails; do not reset or discard unrelated work. Tests use the existing `target/` temporary-artifact helpers. No mount, database, object-store, or external data is mutated by the focused unit/check commands.

## Artifacts and Notes

The pre-plan commit diff reported 143 insertions and 924 deletions across the four tracked source changes; the deleted barrier module accounts for 619 lines. The committed change set includes the 131-line ExecPlan, for 274 insertions and 924 deletions across five files. The focused suite passed 11 tests total (6 handle-drain tests and 5 exact context tests). Four package checks, Clippy with warnings denied, formatting, and diff validation passed. Commit: `fd13dfcd`.

## Interfaces and Dependencies

The final shared VFS interface is the existing concrete API on `StudioVfs`: `inode_has_local_dirty_state(ino) -> bool`, `flush_volatile_writeback_with_outcome(ino, fh) -> VfsResult<ExplicitFlushOutcome>`, `release_fh_with_outcome(fh) -> VfsResult<ReleaseFhOutcome>`, and `release_fh_with_deferred_last_handle_barrier(fh, runtime) -> ReleaseFhOutcome`. The WinFSP adapter may use `VfsResult`, `VfsError`, `ExplicitFlushOutcome`, and `ReleaseFhOutcome`, but it must not introduce a replacement generic port or manager layer.

Revision note (2026-08-07): Created after inspecting SFS-643, the current VFS barrier module, the WinFSP facade, bridge mappings, and existing focused tests. The initial implementation deliberately excludes macFUSE/Linux source edits because their code does not depend on the deleted port surface.

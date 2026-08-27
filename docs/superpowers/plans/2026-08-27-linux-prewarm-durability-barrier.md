# Linux prewarm durability barrier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Linux startup namespace prewarm locally durable before the first mounted namespace operation, preventing the relaxed Redb barrier from blocking scenario 22.

**Architecture:** Keep the existing Linux-only `StudioVfs::start()` prewarm and the existing 4096-inode reservation queue. After reserving and returning the batch, synchronously call `VfsMeta::sync_local_metadata()` so the allocator high-water update is completed by an immediate local Redb barrier before startup workers and mounted callbacks can contend with it. macOS and Windows remain outside both `#[cfg(target_os = "linux")]` paths.

**Tech Stack:** Rust 2024, Cargo, Tokio runtime, Redb relaxed durability, `studiofs-vfs` unit tests, Linux FUSE E2E runner.

---

This plan is a living document and must remain consistent with `.agent/PLANS.md`. The approved design is recorded in `docs/superpowers/specs/2026-08-27-linux-prewarm-durability-barrier-design.md`.

## Task 1: Add the failing startup-barrier regression assertion

**Files:**

- Modify: `crates/studiofs-vfs/src/vfs/tests.rs`, beside `linux_start_prewarm_namespace_inode_reservation()`.

The test module already has access to the test-only
`metadata_sync_barrier_calls` counter. Extend the Linux startup-prewarm test
to capture that counter before `vfs.start()` and require exactly one additional
barrier afterward. Keep its existing assertions that startup allocates one
batch, that the first reservation reuses it, and that a second start is
idempotent.

- [x] **Step 1: Add the barrier assertion.** Insert the following capture before the `start()` call:

      let before_barriers = vfs
          .state
          .metrics
          .metadata_sync_barrier_calls
          .load(AtomicOrdering::Acquire);

  Insert the following assertion immediately after `vfs.start()` returns:

      assert_eq!(
          vfs.state
              .metrics
              .metadata_sync_barrier_calls
              .load(AtomicOrdering::Acquire),
          before_barriers + 1,
          "Linux namespace prewarm should force one immediate local metadata barrier"
      );

- [x] **Step 2: Run the focused test and confirm RED.** From `/home/val/dev/studiofs-cli`, run:

      cargo test -p studiofs-vfs --lib vfs::tests::linux_start_prewarm_namespace_inode_reservation -- --exact --nocapture

  Expected result before the production edit: the test fails at the new
  assertion because the current prewarm performs one allocator transaction but
  no VFS metadata-sync barrier (`left: 0`, `right: 1`).

- [x] **Step 3: Commit the red test.** Run:

      git add crates/studiofs-vfs/src/vfs/tests.rs
      git commit -m "test: require durable Linux namespace prewarm"

## Task 2: Force the Linux prewarm through the local durability barrier

**Files:**

- Modify: `crates/studiofs-vfs/src/vfs/namespace_ops.rs`,
  `StudioVfs::prewarm_namespace_inode_reservation()`.

- Modify: `crates/studiofs-vfs/src/vfs.rs`, only if a small shared test-counter
  helper is needed to keep barrier accounting in one place.

The production helper must keep using `reserve_namespace_ino()` and
`return_reserved_namespace_ino()`. After the successful reservation is put
back into the queue, invoke the existing synchronous metadata capability and
map its `MetaError` into `VfsError`:

      self.sync_meta()
          .sync_local_metadata()
          .map_err(VfsError::Meta)

Under `#[cfg(test)]`, record this call in the existing
`metadata_sync_barrier_calls` counter so the regression test observes all
strict local metadata barriers, including the startup one. Use
`std::sync::atomic::Ordering::Relaxed` for that test-only counter update; do
not add a production metric or change the serialized runtime diagnostics.

The helper remains Linux-gated. Its caller in `StudioVfs::start()` continues to
log the error and continue startup, preserving the existing best-effort
optimization behavior. The reservation must be returned even when the barrier
reports an error, so a successful allocator refill is not lost merely because
the immediate durability attempt failed.

- [x] **Step 1: Add the test-only barrier accounting at the startup call site.** Place the counter increment immediately before the synchronous metadata call, guarded with `#[cfg(test)]`, or factor a private helper if that keeps the existing fsync accounting clearer. Do not increment the counter twice for one startup barrier.

- [x] **Step 2: Add the synchronous local metadata barrier to the Linux prewarm helper.** Preserve the existing reservation and return order, warning behavior, and Linux compile-time guard.

- [x] **Step 3: Run the focused test and confirm GREEN.** Run the same command from Task 1 and expect the test to pass with the batch allocation count unchanged and one startup barrier recorded.

- [x] **Step 4: Commit the implementation.** Run:

      git add crates/studiofs-vfs/src/vfs/namespace_ops.rs crates/studiofs-vfs/src/vfs.rs
      git commit -m "fix: make Linux namespace prewarm durable"

## Task 3: Verify the affected VFS and platform boundaries

**Files:**

- Test: `crates/studiofs-vfs/src/vfs/tests.rs` and the complete `studiofs-vfs` library test target.

- [x] **Step 1: Run the namespace allocator test group.** From `/home/val/dev/studiofs-cli`, run:

      cargo test -p studiofs-vfs --lib namespace_inode -- --nocapture

  Expect the Linux startup-prewarm, refill, uniqueness, restart-gap, and
  create/mkdir sharing tests to pass.

- [x] **Step 2: Run the complete VFS library tests.** Run:

      cargo test -p studiofs-vfs --lib

  Expect zero test failures.

- [x] **Step 3: Check formatting and changed-file whitespace.** Run:

      cargo fmt --all -- --check
      git diff --check

  Expect both commands to exit successfully.

- [x] **Step 4: Check host compilation for the unchanged platform bridges.** Run:

      cargo check -p studiofs-macfuse
      cargo check -p studiofs-winfsp

  These checks prove that the platform-gated VFS change does not introduce
  compilation errors in the Mac or Windows bridge packages. Native Mac and
  Windows execution is unavailable when only the Linux Rust target is
  installed, so do not claim native cross-platform runtime coverage.

## Task 4: Verify scenario 22 and the full Linux sweep

**Files:**

- No source files; use retained E2E artifacts under `target/e2e-runs/`.

- [x] **Step 1: Rebuild the release binaries.** From `/home/val/dev/studiofs-cli`, run:

      cargo build --release -p studiofs -p studiofs-e2e

- [x] **Step 2: Run scenario 22 alone on Linux FUSE.** Run with a new retained run ID:

      target/release/studiofs-e2e run mounted-invalidation-dropout-fallback-convergence --seed 0 --studiofs-bin target/release/studiofs --platform linux-fuse --infra compose --retain always --run-id 20260827T-linux-prewarm-barrier-s22

  Expect `setup`, `run`, `validate`, and `cleanup` to pass and the child report
  status to be `Passed`. Inspect the client observability snapshots to confirm
  that the first namespace mutation no longer carries the 1.55-second writer
  stall.

- [x] **Step 3: Run the canonical Linux scenarios 1–25.** Run:

      target/release/studiofs-e2e run --scenario 1-25 --seed 0 --studiofs-bin target/release/studiofs --platform linux-fuse --infra compose --retain always --run-id 20260827T-linux-prewarm-barrier-full25

  Expect the run report to be `Passed`, scenario 22 to pass at `22/25`, and
  scenario 23 and the remaining scenarios not to be skipped due to
  contamination.

- [x] **Step 4: Update the living plan with command results and the final retrospective.** Record actual timestamps, test counts, E2E report paths, any environmental limitation, and the final changed-file set in this plan before handing off.

## Execution record and retrospective

- 2026-08-27: the new startup-barrier assertion reproduced the regression (`left: 0`, `right: 1`) before the production edit; commit `c0753809` records the RED test.
- 2026-08-27: after the Linux-only prewarm barrier was added, the focused test passed; commit `292e09f4` records the implementation.
- 2026-08-27: namespace allocator tests passed 5/5; the complete `studiofs-vfs` library target passed 918/918; `cargo fmt --all -- --check`, `git diff --check`, `cargo check -p studiofs-macfuse`, and `cargo check -p studiofs-winfsp` all passed.
- 2026-08-27T07:49–07:50Z: release binaries built successfully. Standalone scenario 22 passed with report `target/e2e-runs/20260827T-linux-prewarm-barrier-s22/report.json`. Client-a observability showed first namespace mutation max `0.447 ms` and FUSE write max `4.298 ms`, versus the prior approximately 1.55-second writer stall.
- 2026-08-27T07:51–07:55Z: the canonical Linux sweep passed all 25 scenarios, including scenario 22 at `22/25`; report `target/e2e-runs/20260827T-linux-prewarm-barrier-full25/report.json` contains 25 `passed` statuses and no skips.
- Native macOS and Windows runtime execution was unavailable on this Linux host; their bridge packages passed host `cargo check`, and no macOS or Windows bridge file was changed.
- Relative to `b6807e63`, the final changed-file set is `crates/studiofs-vfs/src/vfs/namespace_ops.rs`, `crates/studiofs-vfs/src/vfs/tests.rs`, this implementation plan, and the approved design spec. The runtime change is confined to the existing `#[cfg(target_os = "linux")]` prewarm path.

## Acceptance

The change is accepted only when the Linux startup-prewarm regression test is
RED before the production edit and GREEN afterward; the complete VFS library
test target, formatting, whitespace, and Mac/Windows bridge checks pass; the
standalone scenario 22 passes on Linux FUSE; and the canonical full Linux
sweep completes without skipping later scenarios. The source diff must contain
no Mac or Windows bridge-file edits.

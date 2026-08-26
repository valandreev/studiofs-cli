# Linux namespace inode prewarm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the first writable Linux namespace operation avoid the initial local inode-allocator refill that can block scenario 23, while leaving macOS and Windows behavior unchanged.

**Architecture:** Reuse the existing `StudioVfs::reserve_namespace_ino()` batch allocator. During the one-shot writable `StudioVfs::start()` transition, Linux synchronously reserves one inode before background workers are spawned and returns it to the shared reservation queue. If warming fails, startup logs a warning and preserves the existing mutation-time error path.

**Tech Stack:** Rust 2024, Cargo, Tokio, Redb metadata engine, `#[cfg(target_os = "linux")]`, and the existing `studiofs-vfs` unit/E2E test harness.

---

This is a living ExecPlan and must be maintained according to `.agent/PLANS.md`. The design that this plan implements is committed in `docs/superpowers/specs/2026-08-26-linux-namespace-inode-prewarm-design.md`.

## Purpose / Big Picture

Scenario 23 creates a nested file on a fresh mounted volume. Before this change,
the first parent-directory operation could be the first call to the namespace
inode allocator. That call persists a 4096-ID high-water batch in local Redb,
and a concurrent startup/replay transaction could leave the FUSE callback
waiting until the E2E timeout. After this change, writable Linux startup
performs that one-time batch refill before FUSE callbacks are served. The first
nested create can reuse the already-warmed reservation without starting a Redb
allocator transaction.

The behavior is demonstrated by the Linux-gated VFS regression test: after
`StudioVfs::start()`, the allocator count is one, and the first explicit
namespace reservation does not increment it. It is then demonstrated at the
user boundary by running scenario 23 and observing `setup`, `run`, `validate`,
and `cleanup` all pass.

## Progress

- [x] (2026-08-26 22:20Z) Inspected the scenario report, progress barrier, VFS path, Linux bridge contract, allocator, and startup sequence.
- [x] (2026-08-26 22:20Z) Reproduced the failure as an intermittent startup/Redb contention race; isolated scenario and full 25-scenario repetitions passed.
- [x] (2026-08-26 22:20Z) Chose a Linux-only startup prewarm that reuses the existing namespace reservation queue.
- [x] (2026-08-26 22:20Z) Wrote and committed the design specification at `docs/superpowers/specs/2026-08-26-linux-namespace-inode-prewarm-design.md`.
- [x] (2026-08-26 22:20Z) Completed the specification self-review with no placeholders or contradictions.
- [ ] Write the Linux-gated failing regression test and verify it fails before the production change.
- [ ] Add the internal reservation prewarm helper and Linux startup hook.
- [ ] Run focused and package-level Linux tests, scenario 23, and available cross-platform compile checks.
- [ ] Update this plan with final evidence and retrospective.

## Surprises & Discoveries

- Observation: The failure occurs before the helper records `after_create_parent`, not in symlink attribute comparison.
  Evidence: The failed progress file ends at `before_create_parent`; the child report names `fs::create_dir_all(parent)` and the VFS snapshot reports zero completed `vfs_write_path.mkdir` calls.

- Observation: The failure is intermittent and depends on the surrounding run state.
  Evidence: Four isolated scenario-23 runs, one full 25-scenario run, and ten sequential scenario-22-to-23 pairs passed on Linux after the original failure.

- Observation: The failed diagnostic snapshot shows an unmatched local metadata write transaction.
  Evidence: The target VFS snapshot has six `metadata_engine.begin_write` calls and five commits while the first namespace operation has no completed VFS timing sample.

- Observation: `StudioVfs::start()` claims one-shot startup channels before spawning workers, providing a startup seam that runs once and precedes background Redb work.
  Evidence: `crates/studiofs-vfs/src/vfs.rs` calls `claim_channels_for_start()` and then `spawn_startup_workers(channels)`; `reserve_namespace_ino()` is the existing synchronous refill path.

## Decision Log

- Decision: Prewarm one existing namespace inode batch at the successful first `StudioVfs::start()` transition, only under `target_os = "linux"` and only for writable mounts.
  Rationale: It removes the avoidable first allocator refill from the FUSE callback with two small structural changes and no allocator or platform-bridge duplication.
  Date/Author: 2026-08-26 / Codex.

- Decision: Run the prewarm after startup channel claim and before `spawn_startup_workers()`.
  Rationale: Channel ownership makes the operation one-shot and serializes concurrent starts; placing it before worker creation avoids introducing the known contention during the prewarm itself.
  Date/Author: 2026-08-26 / Codex.

- Decision: Log prewarm errors and continue startup.
  Rationale: Prewarm is an optimization, not a new mount-availability requirement. The existing namespace mutation path must continue to report allocator failures while read-only or degraded mounts preserve their existing behavior.
  Date/Author: 2026-08-26 / Codex.

- Decision: Do not restore or change Linux `mkdir` callback semantics in this fix.
  Rationale: The evidence points to the first allocator refill inside VFS, and changing the bridge route would not remove that refill. Keeping bridge code untouched minimizes Mac/Windows and Linux callback regression risk.
  Date/Author: 2026-08-26 / Codex.

## Outcomes & Retrospective

This section is updated after implementation and verification. It must state the
observed test results, whether scenario 23 passed, whether any platform check
was unavailable, and any remaining limitation.

## Context and Orientation

The repository is a Rust 2024 Cargo workspace. `crates/studiofs-vfs` contains
the platform-independent `StudioVfs` object. Its namespace implementation is in
`crates/studiofs-vfs/src/vfs/namespace_ops.rs`; the method
`reserve_namespace_ino()` holds the shared reservation lock, refills a batch of
4096 IDs with `sync_meta().next_inode_ids(4096)` when empty, and returns one ID.
The matching return helper puts an unused ID back at the front of that queue.

`crates/studiofs-vfs/src/vfs.rs` contains `StudioVfs::start()`. A writable VFS
first claims one-shot startup channels, then starts local compaction and
background workers. The Linux-only prewarm must be inserted between those two
events. The early `read_only_mount` return must remain before the prewarm.

`crates/studiofs-vfs/src/vfs/tests.rs` contains the shared test constructor
`make_unstarted_test_vfs()` and the helper
`namespace_inode_allocation_calls()`. Existing allocator tests immediately
below that helper prove batch refill, uniqueness, restart gaps, and sharing
between create and mkdir. The new regression test belongs beside them and must
be gated with `#[cfg(target_os = "linux")]`.

The `target_os` condition is evaluated by Rust at compile time. It means the
prewarm method and call are absent from macOS and Windows builds; no Mac or
Windows FUSE bridge source needs to be edited.

## Plan of Work

### Task 1: Add and run the failing Linux regression test

**Files:**

- Modify: `crates/studiofs-vfs/src/vfs/tests.rs`, immediately after `namespace_inode_allocation_calls()` and before `namespace_inode_reservation_refills_after_exhaustion()`.

- [ ] **Step 1: Write the failing test.** Add this exact test:

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_start_prewarm_namespace_inode_reservation() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "linux-start-prewarm");
        let before = namespace_inode_allocation_calls(&vfs);

        {
            let _guard = rt.enter();
            vfs.start().expect("start VFS background workers");
        }

        assert_eq!(
            namespace_inode_allocation_calls(&vfs),
            before + 1,
            "writable Linux startup should warm one namespace inode batch"
        );

        let first = vfs
            .reserve_namespace_ino()
            .expect("reserve the first post-start namespace inode");
        assert!(
            first > ROOT_INO,
            "the warmed reservation should contain a namespace inode"
        );
        assert_eq!(
            namespace_inode_allocation_calls(&vfs),
            before + 1,
            "the first post-start reservation must reuse the warmed batch"
        );

        {
            let _guard = rt.enter();
            vfs.start().expect("second start should be idempotent");
        }
        assert_eq!(
            namespace_inode_allocation_calls(&vfs),
            before + 1,
            "idempotent start must not refill the namespace allocator"
        );
    }

- [ ] **Step 2: Run the focused test before changing production code.** From `/home/val/dev/studiofs-cli`, run:

    cargo test -p studiofs-vfs --lib linux_start_prewarm_namespace_inode_reservation -- --exact --nocapture

  Expected result before the fix: the test compiles and fails at the first
  assertion because startup records zero namespace allocations while the
  assertion expects one. If the test fails to compile for an unrelated reason,
  fix only the test spelling/import issue before proceeding; do not add
  production code to bypass the red test.

- [ ] **Step 3: Commit the red test.** Run:

    git add crates/studiofs-vfs/src/vfs/tests.rs
    git commit -m "test: cover Linux namespace allocator startup prewarm"

### Task 2: Implement the minimal Linux startup prewarm

**Files:**

- Modify: `crates/studiofs-vfs/src/vfs/namespace_ops.rs`, beside `reserve_namespace_ino()` and `return_reserved_namespace_ino()`.

- Modify: `crates/studiofs-vfs/src/vfs.rs`, in `StudioVfs::start()` after successful startup channel claim and before `spawn_startup_workers(channels)`.

- [ ] **Step 1: Add the internal helper that reuses the existing allocator.** Insert this exact method after `reserve_namespace_ino()` and before `return_reserved_namespace_ino()`:

    #[cfg(target_os = "linux")]
    pub(super) fn prewarm_namespace_inode_reservation(&self) -> VfsResult<()> {
        let ino = self.reserve_namespace_ino()?;
        self.return_reserved_namespace_ino(ino);
        Ok(())
    }

  The helper must not call the metadata engine directly, change
  `NAMESPACE_INODE_BATCH`, or publish a namespace entry. It only exercises the
  existing refill path and returns the reserved ID.

- [ ] **Step 2: Invoke the helper once before workers start.** In
  `StudioVfs::start()`, keep the existing `let Some(channels) = ... else { return
  Ok(()) };` block. Immediately after it, insert:

    #[cfg(target_os = "linux")]
    if let Err(error) = self.prewarm_namespace_inode_reservation() {
        warn!(
            error = %error,
            "failed to prewarm Linux namespace inode reservation; first mutation will retry"
        );
    }

  Leave the existing `spawn_startup_workers(channels)` call and its panic/error
  handling unchanged. Because startup channels are claimed once, a second
  `start()` call returns before this block. Because the read-only return is
  earlier, read-only mounts do not touch the allocator.

- [ ] **Step 3: Run the focused test.** From `/home/val/dev/studiofs-cli`, run:

    cargo test -p studiofs-vfs --lib linux_start_prewarm_namespace_inode_reservation -- --exact --nocapture

  Expected result: `1 passed; 0 failed`. The test must also confirm that the
  explicit reservation after startup and the second `start()` call do not
  increment the allocator count.

- [ ] **Step 4: Run the existing namespace allocator tests.** Run:

    cargo test -p studiofs-vfs --lib namespace_inode -- --nocapture

  Expected result: the Linux prewarm test plus the existing refill, uniqueness,
  restart-gap, and create/mkdir sharing tests pass. This confirms that returning
  the warmed inode did not lose or duplicate an ID.

- [ ] **Step 5: Inspect the diff and commit the implementation.** Run:

    git diff --check
    git diff -- crates/studiofs-vfs/src/vfs/namespace_ops.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/tests.rs

  Confirm the diff contains only the Linux-gated helper, the Linux-gated
  startup call, and the regression test. Then run:

    git add crates/studiofs-vfs/src/vfs/namespace_ops.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/vfs/tests.rs
    git commit -m "fix: prewarm Linux namespace inode allocator at startup"

### Task 3: Verify the affected product behavior and platform boundaries

**Files:**

- No additional source files are expected. Update this plan’s `Progress`,
  `Surprises & Discoveries`, and `Outcomes & Retrospective` sections with the
  actual command results.

- [ ] **Step 1: Run the complete `studiofs-vfs` library tests.** From
  `/home/val/dev/studiofs-cli`, run:

    cargo test -p studiofs-vfs --lib

  Expected result: Cargo reports the library test target passed with zero
  failures. If an unrelated existing test is flaky, record its exact name and
  output before deciding whether a focused rerun is justified.

- [ ] **Step 2: Run scenario 23 on Linux.** Use the release binary and compose
  infrastructure already used for the incident:

    target/release/studiofs-e2e run mounted-symlink-attribute-convergence --seed 0 --studiofs-bin target/release/studiofs --platform linux-fuse --infra compose --retain on-failure --run-id 20260826T-linux-prewarm-s23

  If the binary is stale after the code change, first run:

    cargo build --release -p studiofs -p studiofs-e2e

  Acceptance: the scenario report under
  `target/e2e-runs/20260826T-linux-prewarm-s23/report.json` has status
  `Passed`, and its child report records `setup`, `run`, `validate`, and
  `cleanup` as passed. The run must not require changing the scenario timeout.

- [ ] **Step 3: Run the complete E2E sequence once when the environment allows it.** From `/home/val/dev/studiofs-cli`, run:

    target/release/studiofs-e2e run --scenario 1-25 --seed 0 --studiofs-bin target/release/studiofs --platform linux-fuse --infra compose --retain on-failure --run-id 20260826T-linux-prewarm-full25

  Confirm scenario 23 passes in sequence after scenarios 1–22. Do not repeat
  the full suite solely to obtain a more favorable result if the first run
  passes.

- [ ] **Step 4: Check platform compilation without changing platform behavior.** Run the available checks:

    cargo check -p studiofs-macfuse
    cargo check -p studiofs-winfsp

  If a target toolchain or native SDK is unavailable, record the exact Cargo
  error and use source inspection to confirm the only production additions are
  guarded by `#[cfg(target_os = "linux")]`. Do not install unrelated SDKs or
  alter Mac/Windows code as part of this fix.

- [ ] **Step 5: Verify the final worktree and commit set.** Run:

    git status --short --branch
    git log -3 --oneline

  Expected result: no untracked implementation files, the design/test/fix
  commits are present, and no unrelated user changes were staged.

## Validation and Acceptance

The implementation is complete only when all of the following are true:

1. The new Linux-gated test fails before the fix and passes after it.
2. The focused namespace allocator tests and full `studiofs-vfs` library test
   target pass.
3. Linux scenario 23 passes through setup, run, validation, and cleanup without
   increasing its timeout.
4. The final source diff contains no Mac/Windows bridge changes and the
   prewarm method/call are both Linux-gated.
5. A prewarm failure is logged and does not change the existing mutation error
   path.
6. The final plan records exact verification output and any unavailable
   cross-platform checks.

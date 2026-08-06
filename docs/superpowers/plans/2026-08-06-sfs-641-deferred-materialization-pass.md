# SFS-641 Deferred Materialization Pass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use inline execution with the approved SFS-641 ExecPlan and the TDD workflow. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract one iteration of the shared deferred materialization state machine into a private outcome-driven async pass without changing scheduling, cancellation, linger, metadata-flush, or retry behavior.

**Architecture:** `StudioVfs::start_deferred_materialization_task` remains the task-lifecycle wrapper. A module-private `DeferredMaterializationPassOutcome` and `run_deferred_materialization_pass` own one materialization iteration and report whether the wrapper should run another iteration, perform interrupted cleanup, or finish. The existing VFS bridge entry points and finalizer remain unchanged.

**Tech Stack:** Rust 2024, Tokio async tasks, `cargo test`, existing `studiofs-vfs` test helpers, and the existing `DeferredInodeGate` synchronization primitive.

## Global Constraints

- Change only the shared VFS implementation and focused VFS tests; do not add platform-specific scheduler branches.
- Preserve exact linger timers, `yield_now` points, metadata flush ordering, warning/error suppression, gate ownership, cancellation behavior, and rescheduling behavior.
- Keep the outcome type and pass private to the `vfs` module (`pub(super)` only if the existing `vfs::tests` module needs direct focused coverage); do not change the crate's public API.
- Do not introduce traits, managers, scheduler hierarchies, dependencies, or speculative configuration.
- Follow the dependency direction `domain ← ports ← application ← adapters/runtime/API` and existing Rust style.
- Run focused Cargo filters one at a time to avoid workspace file-lock contention.

---

### Task 1: Add the red focused outcome test

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/tests.rs`, near the existing deferred-materialization tests around line 15500.

**Interfaces:**
- Consumes the planned `super::sync_ops::DeferredMaterializationPassOutcome` enum and `StudioVfs::run_deferred_materialization_pass` method.
- Produces a regression test proving a clean inode returns the explicit `Idle` outcome from the shared pass.

- [ ] **Step 1: Write the failing test**

Add this test before `strict_flush_interrupts_deferred_materialization_linger`:

    #[test]
    fn deferred_materialization_pass_reports_idle_for_clean_inode() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "materialize-pass-idle");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"materialize-pass-idle.bin", 0, 0, 0o644)
                .expect("create file");
            let (entry, guard) = vfs
                .state
                .deferred_materialize_inflight
                .try_register(attr.ino)
                .expect("test should reserve the deferred materialization gate");
            let mut no_progress_retries = 0;

            let outcome = vfs
                .run_deferred_materialization_pass(&entry, &mut no_progress_retries)
                .await;

            assert_eq!(
                outcome,
                super::sync_ops::DeferredMaterializationPassOutcome::Idle
            );
            drop(entry);
            drop(guard);
        });
    }

- [ ] **Step 2: Run the red test**

Run:

    cargo test -p studiofs-vfs deferred_materialization_pass_reports_idle_for_clean_inode -- --nocapture

Expected result before the implementation: compilation fails because the new pass and outcome type do not exist. This is the intended feature-missing red state; if the compiler reports an unrelated import or test-fixture error, correct that test error before proceeding.

- [ ] **Step 3: Commit the red test**

Run:

    git add crates/studiofs-vfs/src/vfs/tests.rs
    git commit -m "test: specify deferred materialization pass idle outcome"

### Task 2: Add the outcome type and extract one pass iteration

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/sync_ops.rs`, near the existing deferred-materialization helpers and `start_deferred_materialization_task`.

**Interfaces:**
- Consumes existing `StudioVfs` materialization helpers and `DeferredInodeGateEntry::wait_linger_or_interrupt`.
- Produces `DeferredMaterializationPassOutcome` and `StudioVfs::run_deferred_materialization_pass` for the outer task and the focused test.

- [ ] **Step 1: Define the private outcome contract**

Add the smallest enum needed by the state machine:

    #[derive(Debug, Eq, PartialEq)]
    pub(super) enum DeferredMaterializationPassOutcome {
        Idle,
        Reschedule,
        Interrupted,
        Completed { allow_reschedule: bool },
    }

`Idle` is the clean terminal branch. `Reschedule` means the outer task must immediately run another pass. `Interrupted` is returned only when the linger wait is externally interrupted. `Completed { allow_reschedule }` is the terminal branch for normal completion or a materialization failure; failures carry `false` so the finalizer preserves strict-barrier retry semantics.

- [ ] **Step 2: Implement the pass around the existing branches**

Add this method signature and move the current per-iteration logic into it without changing helper calls or branch order:

    pub(super) async fn run_deferred_materialization_pass(
        &self,
        entry: &DeferredInodeGateEntry,
        no_progress_retries: &mut usize,
    ) -> DeferredMaterializationPassOutcome

The body starts by reading `entry.ino`, `writer_stack.dirty_bytes_for_inode`, and `total_pending_write_buffer_bytes`. It then follows the existing order: clean-inode linger; completed-block object-ID prefetch; completed-block materialization; limits materialization; closed-writer materialization; idle-tail materialization; progress/retry accounting; and the final yield-or-terminal decision. Every existing error branch logs or suppresses the error exactly as before and returns `Completed { allow_reschedule: false }`.

The method must return `Reschedule` at each former `continue` point after preserving the existing `yield_now` or linger wait. It must return `Interrupted` at each former `linger_interrupted = true; break` point. It must return `Completed { allow_reschedule: false }` at each former error branch that set `allow_reschedule_on_exit = false`, and `Idle` for the initial clean/no-linger terminal branch. The final normal terminal branch returns `Completed { allow_reschedule: true }`.

Do not move the semaphore permit, task spawn, interrupted final materialization, local metadata flush, or gate finalizer into this method.

- [ ] **Step 3: Run the focused red test again**

Run:

    cargo test -p studiofs-vfs deferred_materialization_pass_reports_idle_for_clean_inode -- --nocapture

Expected result: the test passes and reports one passing test. If compilation exposes a visibility issue, keep the enum/method limited to `pub(super)` rather than making either item public outside the VFS module.

### Task 3: Replace the large task loop with outcome orchestration

**Files:**
- Modify: `crates/studiofs-vfs/src/vfs/sync_ops.rs`, `StudioVfs::start_deferred_materialization_task`.

**Interfaces:**
- Consumes `run_deferred_materialization_pass` and its outcome enum.
- Produces the same task lifecycle and finalizer behavior as the pre-refactor implementation.

- [ ] **Step 1: Replace the old loop with the lifecycle wrapper**

Keep the existing `Arc` clone, `tokio::spawn`, `_guard`, and `track_background_task`. Replace the old materialization loop with this orchestration shape:

    let (mut allow_reschedule_on_exit, linger_interrupted) = loop {
        match vfs
            .run_deferred_materialization_pass(&entry, &mut no_progress_retries)
            .await
        {
            DeferredMaterializationPassOutcome::Reschedule => continue,
            DeferredMaterializationPassOutcome::Idle => break (true, false),
            DeferredMaterializationPassOutcome::Interrupted => break (true, true),
            DeferredMaterializationPassOutcome::Completed { allow_reschedule } => {
                break (allow_reschedule, false);
            }
        }
    };

Retain the existing post-loop interrupted materialization, error handling, metadata flush, and `finish_deferred_materialization_task` blocks byte-for-byte in behavior. If the interrupted final materialization fails, it must still set `allow_reschedule_on_exit = false`; metadata flush must still run after interrupted cleanup and before finalization.

- [ ] **Step 2: Run the focused green tests**

Run each command separately:

    cargo test -p studiofs-vfs deferred_materialization_pass_reports_idle_for_clean_inode -- --nocapture
    cargo test -p studiofs-vfs strict_flush_interrupts_deferred_materialization_linger -- --nocapture
    cargo test -p studiofs-vfs last_handle_release_interrupts_deferred_materialization_linger -- --nocapture
    cargo test -p studiofs-vfs strict_flush_interrupts_clean_deferred_materialization_linger -- --nocapture
    cargo test -p studiofs-vfs deferred_materialization_reschedules_write_that_arrives_while_inflight_finishes -- --nocapture

Expected result: every command exits zero with no failed tests. The interruption tests prove `Interrupted`; the in-flight reschedule test proves finalizer behavior; the clean test proves `Idle`; existing full-block/tail tests cover `Reschedule` and normal `Completed` behavior.

- [ ] **Step 3: Commit the implementation**

Run:

    git add crates/studiofs-vfs/src/vfs/sync_ops.rs crates/studiofs-vfs/src/vfs/tests.rs
    git commit -m "refactor: extract deferred materialization pass outcomes"

### Task 4: Refactor review and repository validation

**Files:**
- Modify: `docs/superpowers/specs/2026-08-06-sfs-641-deferred-materialization-pass-design.md` to record actual progress and evidence.
- Modify: `docs/superpowers/plans/2026-08-06-sfs-641-deferred-materialization-pass.md` to check off completed steps and record discoveries.

**Interfaces:**
- Consumes the implementation and focused test results.
- Produces a verifiable handoff with no unrecorded runner gaps.

- [ ] **Step 1: Review the diff for contract preservation**

Run:

    git diff HEAD~1 -- crates/studiofs-vfs/src/vfs/sync_ops.rs crates/studiofs-vfs/src/vfs/tests.rs

Confirm the outer task owns only lifecycle orchestration, the pass is platform-neutral, all existing warnings and error suppression remain, and no public API or timer changed.

- [ ] **Step 2: Run formatting, warnings, and diff hygiene**

Run:

    cargo fmt --all -- --check
    RUSTFLAGS='-D warnings' cargo check -p studiofs-vfs
    git diff --check

Expected result: all commands exit zero. If a platform mounted runner is available, run the smallest shared materialization scenario for each bridge; otherwise record the unavailable macFUSE/FUSE/WinFSP runner as a validation gap.

- [ ] **Step 3: Update Linear and finish the living plan**

Record the exact test/build output and platform validation status in the two plan files. Update Linear SFS-641 in English with the implementation summary and validation evidence, then set it to the appropriate completed state only after all acceptance checks pass.

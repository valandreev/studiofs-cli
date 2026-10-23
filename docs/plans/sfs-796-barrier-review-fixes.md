# Isolate Linux async-write barrier timeouts and retain release ownership

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

A slow write must cause EIO only on the callback whose 30-second write wait expired. Reading, closing, linking and writing other files must remain usable. Late completion must permit subsequent barriers to succeed and must reclaim a handle whose kernel release already returned EIO.

## Progress

- [x] (2026-10-11) Record root `<repo-root>`, branch `feature/sfs-796-bound-linux-async-write-barrier-waits-so-wedged-writes`, clean worktree; trace all ten review findings.
- [x] (2026-10-11) Four new regressions failed on old code; evidence: `target/sfs-796-review-red.log`. Added cleanup/shutdown race and late sticky-error coverage.
- [x] (2026-10-11) Remove global timeout poisoning; implement owned late release; publish timeout metrics and update documentation.
- [x] (2026-10-11) Final Linux suite: 173 passed, 3 mounted tests ignored. Focused VFS suite: 5 passed. Crate check, all-target Clippy with -D warnings, formatting and diff checks passed. Independent read-only review found no actionable issues.

## Surprises & Discoveries

The graph project configured in AGENTS.md is absent; the available index is `<codebase-memory-project>`. Direct source inspection confirms that the global barrier_failed/shutting_down flags poison unrelated callbacks. The existing kernel-barrier test deliberately asserts permanent failure after late completion, so those assertions need replacement. VFS already offers a synchronous handle close followed by owned background durability work, which avoids inventing another background lifecycle.

## Decision Log

- Decision: A timeout fails one barrier invocation and keeps pending writes intact, without poisoning an inode or mount.
  Rationale: Expiry includes healthy admission queue delay and does not prove permanent corruption. Actual write errors retain the existing sequence-based error journal. Pending writes and configured capacity still prevent unsafe cancellation and unbounded async admission.
  Date/Author: 2026-10-11 / Codex.
- Decision: A timed-out kernel release installs a ledger-owned completion callback retaining its open liveness guard. The callback closes using `release_fh_with_deferred_last_handle_barrier`, whose background work is owned by VFS shutdown, then drops the pin. Ledger shutdown includes cleanup in its drain condition.
  Rationale: Kernel release is not retried. Resources must remain live until writes finish, then close automatically. The original callback already reported EIO, so late cleanup does not promise strict successful durability.
  Date/Author: 2026-10-11 / Codex.

## Outcomes & Retrospective

All confirmed fixes are implemented and verified. Four new regressions failed before the correction; the final Linux suite passed 173 tests with 3 native mount tests ignored, including the strengthened unlinked-file late-write regression. Five focused VFS metric tests passed. Both crates passed cargo check and all-target Clippy with -D warnings. Formatting and diff checks passed. Independent review found no actionable findings. Native FUSE/E2E acceptance was not run: the ignored tests require /dev/fuse and fusermount3. A permanently stuck write still owns its resources and global admission reservation; callback timeouts do not establish data corruption or cancel VFS work.

## Context and Orientation

`crates/studiofs-linux-fuse/src/async_writes.rs` registers writes before capacity reservation. Each write has a sequence, inode and handle. Barriers capture the last sequence and wait for matching earlier requests. `low_level.rs` translates a timeout to EIO and skips the VFS operation. Kernel release replies currently remove the open liveness guard even if the VFS handle remains open. `crates/studiofs-vfs/src/vfs/metrics.rs`, `vfs.rs` and `vfs/runtime_stats_surface.rs` carry bridge counters to serialized stats and runtime counters. Shared writer and durability behavior remains unchanged.

## Plan of Work

First add tests that time out a retained ticket then exercise unrelated inode operations, new and queued admission, and late success. Add a kernel-release regression preserving liveness until completion and checking handle reclamation. Run these against the original production code and retain failure evidence under target. Remove barrier_failed and its coupling to shutting_down. Replace error-consumption booleans with an enum. Add completion callbacks to the ledger, running them outside its mutex and including outstanding cleanup in shutdown waits. Use the existing VFS-owned deferred release after a kernel timeout. Preserve production capacity limits when shortening test timeouts. Add barrier_timeouts through ledger snapshots, VFS snapshot replacement/reset and runtime metric export; exercise propagation/reset in existing stats tests. Update platform bridge docs with invocation-local timeout, global queue delay, safe liveness retention and automatic late cleanup.

## Concrete Steps

Run from `<repo-root>`:

    cargo test -p studiofs-linux-fuse --lib timeout -- --nocapture
    cargo test -p studiofs-linux-fuse --lib
    cargo test -p studiofs-vfs --lib fuse_async_write
    cargo check -p studiofs-linux-fuse -p studiofs-vfs
    cargo clippy -p studiofs-linux-fuse -p studiofs-vfs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

Expected: new regressions fail against old code, then all selected nonzero tests pass with the fixes. Any unavailable prerequisite or unrelated failure must be recorded.

## Validation and Acceptance

While a ticket remains pending, only its matching timed-out invocation fails. Idle unrelated flush/fsync/link/release succeed and new writes register. Capacity waiters reserve successfully after capacity frees. After completion, barriers work normally. Kernel release keeps inode liveness while pending, then clears VFS handle, liveness and ledger close state after completion. A timeout increments exported diagnostics, including reset semantics. Shutdown cannot report drained during deferred cleanup.

## Idempotence and Recovery

Tests use disposable local fixtures. No environment secrets or services are required. Preserve existing commits and unrelated changes. Keep diagnostics under target; no remote mutation or PR is required.

## Artifacts and Notes

Review dispositions: 1–6 confirmed correctness/lifecycle issues; 7 not a current limit mismatch: Default invokes the same with_limits constants; helper now preserves explicitly configured limits; 8 confirmed weak parameter API; 9 confirmed missing metric and logging under lock; 10 confirmed incomplete operational documentation.

## Interfaces and Dependencies

Use existing Rust std mutex/condvar, sequence indexes, and VFS deferred release. Completion callbacks are `FnOnce() + Send + 'static`, stored per closed handle and executed outside the ledger lock. No dependency additions or changes to VFS write/flush/fsync semantics.

Revision note: Initial plan records the verified root cause and callback-scoped timeout decision.

Revision note: Record red regressions and first Linux suite success; clarify that review finding 7 does not represent differing production limits.

Revision note: Record final green evidence, successful crate checks and independent review; document the retained global admission limits and separate native mount validation boundary. Logs are retained under `target/sfs-796-review-{red,linux-final,vfs-tests,check,clippy}.log`.

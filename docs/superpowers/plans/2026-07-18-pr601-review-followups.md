# PR 601 Review Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the four accepted PR 601 review findings covering OS-cache concurrency, incomplete cgroup samples, writer-stack cancellation, and macFUSE liveness metrics.

**Architecture:** Keep each correction inside its existing owner. `OsCacheState` owns atomic writer/generation state, the Linux probe rejects incomplete finite-cgroup samples, the writer stack exposes rollback for prepared slices and the async layer owns an RAII guard, and the macFUSE adapter records repaired lookup liveness.

**Tech Stack:** Rust 2024, Tokio, parking_lot, cargo test, cargo clippy.

---

## File map

- `crates/studiofs-vfs/src/vfs/os_cache.rs`: atomic writer and generation state.
- `crates/studiofs-vfs/src/vfs.rs`: consume the atomic OS-cache decision.
- `crates/studiofs-vfs/src/vfs/tests.rs`: VFS-level OS-cache regression coverage.
- `crates/studiofs/src/cli/mount/memory_pressure.rs`: Linux cgroup probe behavior and tests.
- `crates/studiofs-vfs/src/vfs/writer_stack.rs`: rollback of prepared writer slices and unit coverage.
- `crates/studiofs-vfs/src/vfs/sync_ops.rs`: cancellation guard around async ID reservation.
- `crates/studiofs-macfuse/src/adapter.rs`: unknown-open metric and adapter tests.

### Task 1: Make OS-cache observation atomic with writer activation

- [x] Add a failing test in `crates/studiofs-vfs/src/vfs/os_cache.rs` which starts from an observed generation, activates a writer, and verifies that the combined read observation returns `ActiveWriter` instead of `Reuse`.
- [x] Run `cargo test -p studiofs-vfs os_cache:: --lib -- --nocapture` and verify the new API/test fails to compile or fails with an unexpected reuse decision.
- [x] Replace the independent `generations` and `active_writers` mutexes with one `Mutex<OsCacheEntries>` containing both maps. Add `observe_without_active_writer`, whose critical section is equivalent to:

```rust
if state.active_writers.contains_key(&attr.ino) {
    state.generations.pop(&attr.ino);
    return OsCacheOpenDecision::replace(OsCacheReason::ActiveWriter);
}
observe_generation(&mut state.generations, attr, spans)
```

Make `begin_writer` increment the count and remove the generation while holding the same lock. Update `clear`, `end_writer`, and writer-count queries to use the combined state.
- [x] Update `StudioVfs::prepare_os_cache_open` in `crates/studiofs-vfs/src/vfs.rs` to call the atomic operation for its final decision.
- [x] Run the focused OS-cache tests and `cargo test -p studiofs-vfs os_cache_prepare --lib -- --nocapture`; expect PASS.

### Task 2: Reject incomplete finite-cgroup samples

- [x] Add `probe_rejects_finite_limit_without_current_counter` in `crates/studiofs/src/cli/mount/memory_pressure.rs`. Write finite `memory.max`, omit `memory.current`, call `sample()`, and assert that the error contains `memory.current`.
- [x] Run `cargo test -p studiofs probe_rejects_finite_limit_without_current_counter --bin studiofs -- --nocapture`; expect FAIL because the current probe returns host-only values.
- [x] Change the `NotFound` branch for `memory.current` after a finite limit to return an `io::ErrorKind::NotFound` error with cgroup path context. Return `(Some(limit), Some(current))` only after successfully parsing the counter.
- [x] Run all `cli::mount::memory_pressure` tests; expect PASS, including unchanged host-only behavior when `memory.max` is absent or unlimited.

### Task 3: Roll back prepared writer slices on cancellation

- [x] Add a failing writer-stack test in `crates/studiofs-vfs/src/vfs/writer_stack.rs`: write dirty bytes, call `prepare_finish_inode`, invoke the new rollback operation, then prepare again and assert the same non-zero object count is returned.
- [x] Run `cargo test -p studiofs-vfs prepared_inode_rollback_restores_dirty_slices --lib -- --nocapture`; expect a compile failure because rollback does not exist.
- [x] Add `rollback_prepared_inode(ino)` which locks the inode file and sets `frozen = false` only for slices with dirty writer bytes and no pending/materializing entries.
- [x] Add a private `PreparedWriterStackGuard` in `crates/studiofs-vfs/src/vfs/sync_ops.rs`. Its `Drop` calls rollback while armed; `disarm()` is called only after `finish_prepared_inode` succeeds.
- [x] Apply the guard to `materialize_writer_stack_for_inode` and `materialize_writer_stack_idle_tail_for_inode`, covering reservation errors and future cancellation at the `.await`.
- [x] Run the focused writer-stack and materialization tests; expect PASS.

### Task 4: Record repaired unknown opens on macFUSE

- [x] Add a failing adapter test in `crates/studiofs-macfuse/src/adapter.rs`: complete an ordinary pending open with `LookupReservation::Ensure` for an unobserved inode, then assert `open_unknown_inode == 1`; repeat with an already observed inode and assert the count stays unchanged.
- [x] Run `cargo test -p studiofs-macfuse macfuse_open_unknown_inode --lib -- --nocapture`; the Linux target compiles no macFUSE adapter tests, so macOS CI owns the red/green execution.
- [x] In `complete_pending_open`, when `ensure_observed` returns true, call `record_open_unknown_inode`. Leave `LookupReservation::Observe` uncounted. Keep reply rejection rollback unchanged.
- [x] Verify the macFUSE crate through Linux Clippy and leave execution of the target-gated adapter test to macOS CI.

### Task 5: Validate and commit

- [x] Run `cargo fmt --all -- --check`.
- [x] Run `cargo test -p studiofs-vfs --lib`, `cargo test -p studiofs --bin studiofs`, and `cargo test -p studiofs-macfuse --lib`.
- [x] Run `cargo clippy -p studiofs-vfs -p studiofs -p studiofs-macfuse --all-targets -- -W missing-docs` and fix warnings introduced by this change.
- [x] Run `git diff --check` and review the diff against `docs/superpowers/specs/2026-07-18-pr601-review-followup-design.md`.
- [x] Commit all related code, tests, plan, and documentation with a focused message.

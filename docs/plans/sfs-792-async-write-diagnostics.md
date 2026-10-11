# SFS-792: Collect async-write diagnostics on demand

This ExecPlan is maintained under `.agent/PLANS.md`.

## Purpose / Big Picture

Linux async writes currently rebuild a sorted per-inode map and publish cloned
snapshots several times per request. Collect diagnostics only when status or
benchmark reset needs them. Status must still show live pending writes and
failures, while benchmark counters remain relative to the last reset.

## Progress

- [x] (2026-10-11) Read SFS-792, record repository/branch/status, trace publication and reset callers.
- [x] (2026-10-11) Regression failed before implementation: pending_writes remained 1 after completion.
- [x] (2026-10-11) Registered weak on-demand source; removed transition publications; consolidated diagnostics locks.
- [x] (2026-10-11) Verified live diagnostics, unread-counter reset, source expiry, reset/shutdown regression, ledger suite, and release allocation/lock reduction.
- [x] (2026-10-11) Independent review and final diff inspection completed; reviewed race fixed with RED-to-GREEN regression; focused gates recorded, mounted failures retained.

## Surprises & Discoveries

The ledger already maintains all scalar counters and per-inode indexes. No new
atomic counter mirror is needed when no diagnostics consumer is reading.
Independent review found that a shutdown snapshot sampled before reset could
be published afterward: reset advanced the baseline but not the raw snapshot
used for stale-generation rejection. Preserve reset's sampled scalar values in
raw state as well, after clearing its unused per-inode map.

The configured Codebase Memory project name is `home-val-dev-studiofs-cli`;
the contract's `E-dev-studiofs-cli` key is unavailable.

## Decision Log

- Decision: Pull the existing consistent ledger snapshot on demand rather than
  maintain a second set of atomics or a periodic worker.
  Rationale: This removes all observability work from admission/completion and
  preserves the existing JSON schema without new tasks or dependencies.
  Date/Author: 2026-10-11 / Codex.
- Decision: Use a weak ledger reference and retain a final pushed snapshot at
  shutdown. Put raw/current/baseline/source state behind one VFS mutex.
  Rationale: Diagnostics must not extend mount resources' lifetime. Serializing
  reads and resets prevents a snapshot sampled before reset from using the new
  baseline. The source may only read the ledger, never reenter VFS diagnostics.
  Date/Author: 2026-10-11 / Codex.

## Outcomes & Retrospective

The initial regression is green. Linux bridge tests passed (182 passed, 3
mounted tests ignored); five focused VFS snapshot/reset/schema tests passed.
Crate check and all-target Clippy passed. Independent review identified a
delayed shutdown snapshot crossing reset; its deterministic regression failed with the stale completed counter and then
passed after reset also retained its sampled raw values. Both crates passed fresh check and all-target Clippy after the correction.
The product binary was rebuilt. The first registered mounted smoke completed
preflight and started three clients, then failed with `Permission denied (os
error 13)` during scenario I/O; cleanup passed. Its retained run is
`target/e2e-runs/sfs-792-20261011-173324/`. A separate root smoke
`target/e2e-runs/sfs-792-root-20261011-173643/` failed with the same `EACCES`
scenario I/O error, with cleanup passed. Neither run establishes mounted
acceptance. Their reports remain at `target/sfs-792/mounted-report.json` and
`target/sfs-792/mounted-root-report.json`. No unrelated permission or runner
behavior was changed. Full registered scenario coverage and mounted throughput
were not run. Allocation and ledger-lock reductions are confirmed only for the
release diagnostic-overhead loop described below.

## Context and Orientation

Repository: `/home/val/dev/studiofs-cli`, initially clean on
`feature/sfs-792-remove-btreemap-snapshot-churn-from-the-linux-async-write`.
`crates/studiofs-linux-fuse/src/async_writes.rs` owns the ledger (the pending
request registry and accumulated counters). Its `metrics_snapshot` creates the
sorted per-inode map. Before this change, `low_level.rs` called
`record_async_write_metrics` at start, reserve failure, task dispatch/completion,
barriers, and shutdown. `crates/studiofs-vfs/src/vfs.rs` stores raw, current and
reset-baseline snapshots and exposes them via
`write_path_observability_snapshot`. A baseline is the counter value subtracted
from later snapshots to measure only work since a benchmark reset.

## Plan of Work

First change the Linux diagnostics test to complete a real ledger ticket without
republishing and assert status updates. Run it to observe stale status. Then add
a VFS on-demand source returning an optional snapshot; an expired source falls
back to the stored final snapshot. Register it once in `SessionState::new`, with
a weak ledger reference. Remove metric publications from write/barrier paths,
including task captures that served only publication. Keep shutdown publication.
Consolidate VFS snapshot locks and replace field-by-field assignment with whole
snapshot assignment. Read the source and apply the baseline on status queries;
sample the source during reset even if status has never been read. Clear maps
from the stored baseline because subtraction uses only scalar counters. Also
retain the sampled scalar values as raw state to reject shutdown publications
that crossed reset. Preserve
the pushed-snapshot API and its stale-generation behavior for existing callers.

## Concrete Steps

Run from the repository root:

    cargo test -p studiofs-linux-fuse --lib async_write_ledger_metrics -- --nocapture
    cargo test -p studiofs-linux-fuse --lib
    cargo test -p studiofs-vfs --lib fuse_async_write -- --nocapture
    cargo check -p studiofs-linux-fuse -p studiofs-vfs
    cargo clippy -p studiofs-linux-fuse -p studiofs-vfs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check
    cargo build -p studiofs --bin studiofs
    cargo run -p studiofs-e2e -- list
    STUDIOFS_FUSE_ASYNC_WRITE=1 target/debug/studiofs-e2e run basic-multi-client-smoke --studiofs-bin target/debug/studiofs --platform linux-fuse --infra compose --retain always --run-id sfs-792-20261011-173324 --report target/sfs-792/mounted-report.json
    sudo -n env STUDIOFS_FUSE_ASYNC_WRITE=1 target/debug/studiofs-e2e run basic-multi-client-smoke --studiofs-bin target/debug/studiofs --platform linux-fuse --infra compose --retain always --run-id sfs-792-root-20261011-173643 --report target/sfs-792/mounted-root-report.json

Add focused checks for reset before any read, pending gauges surviving reset,
and weak-source expiry preserving shutdown diagnostics. Measure the removed
publication cost in a release-mode ledger write loop, keeping measurement
artifacts under `target/`. Inspect available mounted prerequisites before
claiming mounted performance; use the registered E2E runner for mounted checks.

## Validation and Acceptance

The regression fails on the original code because completed writes still appear
pending until another publication. After the change, status observes completion
without publication. Existing stale-generation and reset/baseline tests pass.
Reset samples counters that were never previously read, preserves live gauges,
and reports subsequent counters relative to that reset. A dropped ledger does
not remain alive through VFS diagnostics. Write transitions no longer call the
snapshot collector or acquire VFS diagnostics locks. Report measured diagnostic
overhead separately from mounted throughput; neither unit tests nor a synthetic
loop establish live mount performance.

## Idempotence and Recovery

Tests use local fixtures under `target/`; no `.env` or remote services are needed.
Rerun focused checks safely. Preserve unrelated changes and do not reset the
working tree. No schema migration, public JSON change, dependency, or mount
durability change is required.

## Artifacts and Notes

Release measurement artifacts are retained under `target/sfs-792/`:
`diagnostics_bench.rs`, instrumented `bench_ledger.rs`, executable
`diagnostics_bench`, and `diagnostics_bench.txt`. Host: Linux 7.0.0-38-generic,
Rust 1.99.0. The standalone harness includes the production ledger and links
existing release VFS dependencies; it compares a ledger start/reserve/complete
loop with three old snapshot publications against the same loop with none.
This isolates diagnostic overhead, not mounted payload throughput. A
thread-local allocator counter excludes fixture background work. Instrumenting
`lock_state` counts ledger acquisitions; VFS lock removal is established by
tracing the removed publications, not by that instrumented counter.

For 10,000 writes with no diagnostics reads, all three repeats at 256 existing
pending inodes gave 830,025 old allocations versus 30,000 without publications
(96.4% fewer). Ledger acquisitions were 60,000 versus 30,000 at every depth.
At 64 existing pending inodes allocations were 260,007 versus 20,000. At zero
existing pending inodes the steady repeats were 60,003 versus 20,003. Timing
is diagnostic only because concurrent builds affected host load; the measured
allocation and acquisition counts are the acceptance evidence.

## Interfaces and Dependencies

Add `StudioVfs::set_fuse_async_write_snapshot_source` accepting a synchronous
`Fn() -> Option<VfsFuseAsyncWriteSnapshot> + Send + Sync + 'static`. Use existing
`Arc`, `Weak`, and `parking_lot::Mutex`; no dependency changes. Sources must not
call back into VFS diagnostics while sampled. Existing
`record_fuse_async_write_snapshot` remains available for final snapshots and
other callers. The Linux bridge uses its existing snapshot conversion function.

Revision: initial plan after tracing the existing issue path and reset behavior.

Revision: recorded focused validation, release allocation/lock measurements,
and the independently discovered reset/shutdown ordering regression.

Revision: fixed the reviewed reset/shutdown race after observing its regression
fail, and verified all 182 Linux bridge tests on the corrected implementation.

Revision: recorded final focused tests/check/Clippy and preserved the non-root
mounted smoke permission failure; a separate root smoke checks the write path.

Revision: root mounted smoke also failed with EACCES and successful cleanup.
Recorded both retained runs and marked focused implementation/review complete
without claiming mounted acceptance or sustained mounted throughput.

## Code-review follow-up (2026-10-11)

Confirmed the lifecycle cleanup recommendation: detach the on-demand source
immediately after `Session::drop` publishes its final snapshot, including the
timeout path that retains callback state. Added
`StudioVfs::clear_fuse_async_write_snapshot_source`, preserving the pushed
snapshot and reset baseline. A real unmounted libfuse session regression keeps
the ledger alive after teardown and changes its timeout counter. Before the
fix, diagnostics incorrectly observed that later counter (1 instead of 0);
after the fix, they retain the final snapshot. No mount or remote services are
needed for this regression.

Rejected the dynamic-flag recommendation: `linux_fuse_async_write_enabled`
already uses a process-wide `OnceLock<bool>`, so registration and final
publication cannot disagree because of an environment change. Preserved the
reset clone: raw and baseline both need the sampled values, and the per-inode
map is empty before cloning. Preserved serialized sampling/reset and added its
reason beside the read lock. The barrier-expression finding already describes
the current correct code and needs no change. Full disposition is also recorded
in the supplied review document, which is ignored by repository policy.

Follow-up verification passed:

- `cargo test -p studiofs-linux-fuse --lib session_drop_detaches_async_write_metrics_and_keeps_final_snapshot -- --nocapture` — 1 passed after the observed red regression.
- `cargo test -p studiofs-linux-fuse --lib async_write -- --nocapture` — 51 passed, including the new teardown regression.
- `cargo test -p studiofs-vfs --lib fuse_async_write -- --nocapture` — 5 passed.
- `cargo check -p studiofs-linux-fuse -p studiofs-vfs` — passed.
- `cargo clippy -p studiofs-linux-fuse -p studiofs-vfs --all-targets -- -D warnings` — passed.
- `cargo fmt --all --check` and `git diff --check` — passed.

No workspace-wide suites or mounted E2E reruns were performed for this
diagnostics-only cleanup. Earlier mounted EACCES failures remain unresolved;
these focused checks do not establish mounted acceptance.

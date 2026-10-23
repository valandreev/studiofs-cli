# Apply the SFS-806 code-review findings to the macFUSE read buffer pool and release path

This living ExecPlan follows `.agent/PLANS.md`. The task is the review follow-up for Linear SFS-806 ("Add size classes and a retained-memory budget to the macFUSE read buffer pool"). The review itself is kept locally (git-ignored) in `docs/code-reviews/2026-10-14-sfs-806-macfuse-read-buffer-budget.md`; everything needed to act on it is repeated here.

## Purpose / Big Picture

The macFUSE adapter (`crates/studiofs-macfuse`) answers kernel read requests with buffers borrowed from a small reuse pool (`ReadBufferPool` in `crates/studiofs-macfuse/src/adapter/read_buffer.rs`). SFS-806 added size classes (a buffer is reused only for requests whose next power of two matches), a 32 MiB budget for idle retained bytes, and hit/miss/retained metrics. The review found that eviction always removed the largest buffer, so a pool already holding 32 small idle buffers evicted every larger buffer the moment it was returned and larger reads never reused anything; that misses were allocated at the exact request size, so a 100-byte buffer could not serve a 120-byte request in the same class; that evicted multi-MiB buffers were freed while the pool lock was held; and that the published hit/miss counters could go backwards. In the shared VFS (`crates/studiofs-vfs`), the branch moved the kernel attribute refresh after the last-handle release drain, but the refresh was repeated at four call sites and was lost if the release future or task was dropped early.

After this change a mixed read workload keeps reusing buffers for the current read size, published counters never decrease, and the attribute refresh after the last handle closes is delivered even when the release is cancelled. Unit tests in both crates demonstrate each behaviour.

## Progress

- [x] (2026-10-14) Reviewed `main...HEAD` plus uncommitted worktree; findings recorded in the local review document.
- [x] (2026-10-14) Added `OsCacheReason::observes_generation()` in `crates/studiofs-vfs/src/vfs/os_cache.rs` so the adapter stops repeating the three-variant match.
- [x] (2026-10-14) macFUSE pool: oldest-first eviction, class-sized allocation, free outside the lock, no production `expect`, named `ReadBufferPoolStats`, serialized snapshot publication, release-mode clamp in `perform_read`, renamed misleading test. Both new pressure tests failed before the fix with 0 hits and now assert exactly 9 hits.
- [x] (2026-10-14) VFS: `KernelInvalidationFlush` drop guard for the deferred kernel invalidation flush, `inode_has_pending_local_state` helper, `retain_kernel_inode_attr_refresh` fast path, `SliceResolution` enum, typed error instead of `unwrap_or_default`. The new cancellation test failed with a no-op guard and passes with the guard.
- [x] (2026-10-14) Documentation: `docs/platform-bridges.md`, `docs/perf/sfs-806-macfuse-read-buffer-budget.md`, `docs/tests.md`, `docs/local-first-validation-matrix.md`.
- [x] (2026-10-14) Focused tests, Clippy, fmt, `git diff --check`; an independent self-check of the final diff passed with only wording follow-ups, which are applied.
- [x] (2026-10-14) Mounted macFUSE rerun (128k, 4m and a 100 KB odd-size phase) against a `main` baseline on volume `test`, directory `806-bench`. Peak mount RSS: 2330 MiB on the branch, 2763 MiB on the baseline. 12 pool misses over about 251 thousand reads. Results are in `docs/perf/sfs-806-macfuse-read-buffer-budget.md`.
- [x] (2026-10-14) The first baseline attempt exposed SFS-855: startup replay aborted with `inode not found` after a clean unmount whose unflushed event window created and deleted benchmark files. Fixed on its own branch from `main` (commit `fix: keep startup replay idempotent for removed inodes`) and cherry-picked here. The affected volume was backed up, recovered with the fixed build, drained to PostgreSQL and remounted.

## Surprises & Discoveries

The second `has_local_pending_regular_file_state` check in `execute_prepared_foreground_read` (`crates/studiofs-vfs/src/vfs/read_path.rs`) looked like duplicate work, but the test `prepared_published_read_uses_new_local_write_after_overlay_commit` shows that local state can appear between preparing and executing a read. The check is required and stays. It runs only when preparation resolved slices (`Option::filter` skips the closure on `None`).

The per-write `invalidate_kernel_inode_attrs` call is not redundant with the release-time flag: the flag is computed at close, after materialization may already have drained all dirty state, and the per-write mark keeps the refresh pending in that case. Only its cost is reduced.

## Decision Log

- Decision: evict the oldest buffer first, under both count pressure and byte pressure. Rationale: the buffer just returned is the newest and can never exceed the 32 MiB budget alone (maximum capacity is 8 MiB), so it survives, and the pool converges to the current working set. This matches the spec wording "evict oversized/old buffers under pressure". Date: 2026-10-14.
- Decision: on a miss, allocate the whole size class (the next power of two, at most 8 MiB) and limit the reply to the requested length. Rationale: the spec asks for "request-aligned powers of two"; aligned 128 KiB–4 MiB macOS reads are unaffected, while odd sizes now share buffers within a class. Cost: at most twice the zero-filled bytes on a miss, still bounded by the retained budget. Date: 2026-10-14.
- Decision: serialize building and storing the read snapshot under one adapter-local mutex instead of adding a generation to `VfsFuseAsyncReadSnapshot`. Rationale: the counters are monotonic, so a publisher that acquires the mutex later always reads values at least as new; no cross-crate schema change is needed. Date: 2026-10-14.
- Decision: not rewriting the already-pushed commit history (`chore:` and `fix(macfuse):` subjects) and not splitting commit `247f46d1` into a separate PR within this follow-up; both are left to the author because they need a force push or a branch restructure. Date: 2026-10-14.
- Decision: leave three review smells as they are. The `macfuse_cache_metric` match maps every `OsCacheReason` variant to a distinct metric, so `observes_generation()` would not simplify it. The `dirty_attr_present` boolean in `read_path.rs` predates this branch. Splitting `adapter.rs` by concern (Divergent Change) is a refactor outside this follow-up. Publication ordering is guarded by the mutex, but no deterministic test covers it: forcing the interleaving would need test-only hooks in the hot read path. The `retain_kernel_inode_attr_refresh` fast path is covered only indirectly, through the release and write tests. Date: 2026-10-14.
- Decision: reads of the `.studiofs-runtime-stats` file keep using the pool. Rationale: the file is small and rarely read; the counter skew is a few events, which does not justify a second buffer type. Date: 2026-10-14.

## Outcomes & Retrospective

All review findings are either fixed or recorded as deliberately skipped in the Decision Log. On a mounted macOS run the branch lowers peak mount RSS by about 16% against the baseline. Twelve pool misses over about 251 thousand reads show that reuse stays effective across 128 KB, 4 MB and odd 100 KB geometries. The mounted work also surfaced SFS-855, a startup-replay defect on `main` that could make a volume unmountable; it is fixed and included here. An open observation: in one baseline run, the local event log was not pruned within ten minutes after a heavy workload. The runtime stats expose no PostgreSQL flush cursor, so publication lag cannot yet be measured directly.

## Context and Orientation

`ReadBufferPool` keeps a `Vec<Vec<u8>>` of idle buffers in recycle order, plus `retained_bytes` (the sum of idle capacities), `hits` and `misses`, all under a `parking_lot` mutex. `acquire(len)` lends a `PooledReadBuffer`; dropping it calls `recycle`. `AsyncReadDiagnostics` in `crates/studiofs-macfuse/src/adapter.rs` turns pool and queue counters into a `VfsFuseAsyncReadSnapshot` and stores it with `StudioVfs::record_fuse_async_read_snapshot`, which the runtime-stats surface exports as counters and gauges.

On the VFS side, `close_handle_for_release` in `crates/studiofs-vfs/src/vfs/sync_ops.rs` produces a `HandleReleaseContext` (defined in `crates/studiofs-vfs/src/vfs/config.rs`) for the last handle of an inode. Its `invalidate_kernel_attrs` flag asks `flush_deferred_kernel_invalidations` (in `crates/studiofs-vfs/src/vfs/workers.rs`) to send a kernel attribute refresh once the release drain finishes.

## Plan of Work

Two independent edits run in parallel: one limited to `crates/studiofs-macfuse`, one limited to `sync_ops.rs`, `workers.rs`, `read_path.rs`, `config.rs` and `tests.rs` in `crates/studiofs-vfs`. Each behavioural fix starts with a failing unit test. Documentation is updated after both code edits land. A final independent self-check rereads the whole diff against this plan and reruns the validation commands.

## Concrete Steps

From the repository root `<repo-root>`:

    cargo fmt --all
    cargo test -p studiofs-macfuse --lib read_buffer
    cargo test -p studiofs-macfuse --lib -- read_only_adapter runtime_stats_open queued_read_execution unpublished_adapter_read stable_empty_file
    cargo test -p studiofs-vfs --lib -- prepared_read_survives_local_overlay prepared_published_read_uses_new_local release_fh_invalidates deferred_last_handle_release_invalidates fuse_async_read_snapshot_serializes
    cargo clippy -p studiofs-macfuse -p studiofs-vfs --all-targets -- -D warnings
    git diff --check

## Validation and Acceptance

The new pool tests must fail on the previous eviction policy and pass afterwards. In the count-pressure test, 32 idle 4 KiB buffers followed by ten 1 MiB reads must reach at least nine hits. In the byte-pressure test, 32 idle 1 MiB buffers followed by ten 4 MiB reads must also reach at least nine hits while retained bytes stay within the budget. Existing pool, adapter and VFS release and read tests stay green with nonzero counts. The SFS-806 acceptance check requires a mounted macFUSE benchmark against a `main` baseline. It ran on 2026-10-14 against the local volume `test` (see the perf document). Each configuration ran once, so throughput differences are indicative only.

## Idempotence and Recovery

All steps are source edits and test runs and can be repeated. The edits do not touch persistent state or schemas.

## Artifacts and Notes

The review diffs, reports and the starvation repro are under `target/code-review-sfs-806/`.

## Interfaces and Dependencies

The new public method is `studiofs_vfs::OsCacheReason::observes_generation(self) -> bool`. The other new items are crate-private: `ReadBufferPoolStats` in `crates/studiofs-macfuse/src/adapter/read_buffer.rs`, a release-flush drop guard and a pending-local-state helper in `crates/studiofs-vfs`, and the private `SliceResolution` enum in `read_path.rs`.

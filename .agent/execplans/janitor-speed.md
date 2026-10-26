# Speed up background janitor object cleanup

This ExecPlan is a living document maintained under `.agent/PLANS.md`. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as implementation and evidence change.

## Purpose / Big Picture

The background janitor removes old objects that are no longer referenced by filesystem metadata. Today it scans remote object listings and deletes candidates while holding the PostgreSQL janitor lock, so slow serial object requests make a cleanup cycle long. The intended behavior overlaps a bounded number of deletions, preserves a per-cycle success cap and metadata fencing, and drains claimed work before releasing the global lock or shutting down. Operators should be able to see the work and its timing in janitor logs and compare a controlled live run with a pre-change run.

## Progress

- [x] (2026-10-26) Read `.agent/PLANS.md`, the VFS contract, janitor implementation, relevant tests, and the current worktree diff.
- [x] (2026-10-27) The current worktree contains bounded object deletion (32), successful-delete cap, timing fields, cancellation/drain handling, per-object reconciliation-finish ordering, legacy-key deletion handling, and focused regression tests.
- [x] (2026-10-27) Recorded two live runs with `STUDIOFS_PG_POOL_SIZE=4` and `=16`: each deleted 1,000 objects with 0 deletion errors. The runs used different consecutive object sets, so the speed difference is indicative rather than a matched benchmark.
- [x] (2026-10-27) A point `stat` after deletion returned false for the checked object(s); this is a spot check, not proof for every deletion.
- [x] (2026-10-27) Final focused validation reported successful: release build, `cargo check -p studiofs-vfs`, strict Clippy, formatting, diff check, `janitor_*` tests (4/4), and `reconciliation_finishes` tests (2/2).
- [x] (2026-10-27) Final mount run at pool 16, grace 60 seconds, interval 10 seconds, and cap 1,000 completed its first cycle in 33,984 ms: 1,000 deleted, 0 deletion errors, 256 reconciliation claims retryable, 0 confirmed.
- [x] (2026-10-27) Review cleanup: the cycle body moved into `run_janitor_cycle`, so the global janitor lock is released at one site after every completion, error, and shutdown path; reconciliation finishes group claims by object ID instead of scanning a pending queue; the listing loop uses one guarded `select!`. Queue structure, cumulative timing fields, and the queue/ordering tests are retained because they carry the cap, drain, and same-object ordering invariants above.
- [ ] Diagnose why the final cycle marked all 256 upload-reconciliation claims retryable and confirmed none; the supplied run summary does not include a cause.
- [ ] A controlled same-set comparison remains outstanding. The full workspace suite was not run, in accordance with the repository instruction to leave it to the user.

## Surprises & Discoveries

- Observation: The current worktree already has an in-flight `FuturesUnordered` deletion queue with concurrency 32. The cycle cap counts successful object deletions, so protected or retryable results do not consume it. Two queue tests cover maximum parallelism/cap behavior and draining an in-flight deletion.
  Evidence: `crates/studiofs-vfs/src/vfs/workers.rs`, `JANITOR_OBJECT_DELETE_CONCURRENCY`, `JanitorDeletionQueue`, `janitor_deletion_queue_bounds_parallelism_and_caps_only_successes`, and `janitor_deletion_queue_drain_waits_for_in_flight_claimed_work`.
- Observation: A deletion first claims the object through `claim_uploaded_object_deletion`; tracked claims are finalized with the returned fencing token. Fragment deletes treat not-found as idempotent success. The janitor drains every queued deletion before releasing its global PostgreSQL janitor lock, including after a listing-stream error.
  Evidence: `StudioVfs::delete_janitor_object` and `StudioVfs::background_janitor` in `crates/studiofs-vfs/src/vfs/workers.rs`.
- Observation: The current cycle logs `cycle_elapsed_ms`, `listing_delete_elapsed_ms`, and cumulative `claim_ms`, `remote_delete_ms`, and `finish_ms`. The listing/delete interval includes scanning and draining; each phase duration sums per-object task elapsed times and can exceed wall-clock time when tasks overlap.
  Evidence: the `janitor cycle complete` event in `crates/studiofs-vfs/src/vfs/workers.rs`.
- Observation: The first supplied live run deleted 1,000 objects with 0 deletion errors after scanning 14,341 entries. It reported `cycle_elapsed_ms=120710`, `listing_delete_elapsed_ms=89165`, and 256 upload-reconciliation claims marked retryable. This is a preliminary result from the accelerated build, not a comparative baseline; no endpoint or deployment identity is recorded here.
  Evidence: sanitized run summary provided by the task owner on 2026-10-26.
- Observation: The reported pool-size comparison was 1000 objects in 100003 ms with pool size 4 and 34179 ms with pool size 16; both runs had 0 deletion errors. The sets were different consecutive batches, not the same objects replayed under both settings. This is evidence that the larger pool may help, not a controlled speedup claim.
  Evidence: sanitized run summary provided by the task owner on 2026-10-27. The pool is selected with the existing `STUDIOFS_PG_POOL_SIZE` environment variable.
- Observation: Object deletion and metadata reconciliation have separate concurrency bounds. Remote reconciliation proofs run up to 32 at once; metadata finish calls run up to 16 at once, while the helper prevents two finish calls for the same object from overlapping. Different object IDs may finish concurrently.
  Evidence: `JANITOR_RECONCILIATION_CONCURRENCY`, `JANITOR_RECONCILIATION_FINISH_CONCURRENCY`, and `finish_janitor_reconciliation_claims` in `crates/studiofs-vfs/src/vfs/workers.rs`; `reconciliation_finishes_serialize_fragments_for_the_same_object` covers ordering.
- Observation: The legacy object-key path is now included when a listed key is the canonical legacy key; fragment-form keys are also handled. A point check after delete returned `stat=false` for the checked object(s). A shutdown ownership regression is present and the owner reports focused janitor tests passed.
  Evidence: `janitor_object_delete_keys`, `janitor_deletes_the_listed_legacy_key_and_current_fragment_keys`, and `janitor_is_owned_by_non_aborting_shutdown_cleanup` in the current worktree; direct stat result supplied by the task owner.
- Observation: The final pool-16 mount run deleted the full cap in 33,984 ms with 0 deletion errors, but reported 256 reconciliation claims as retryable and 0 confirmed. The reason for those outcomes has not been diagnosed.
  Evidence: sanitized run summary supplied by the task owner on 2026-10-27; no endpoint or deployment identity recorded.
- Observation: The focused validation gates passed, but the full workspace suite remains unverified and a same-set performance comparison is still missing. The current checkout contains uncommitted changes in `crates/studiofs-vfs/src/vfs.rs`, `crates/studiofs-vfs/src/vfs/workers.rs`, and `crates/studiofs-vfs/src/vfs/tests.rs`; preserve them and inspect the complete diff before delivery.
  Evidence: task-owner validation summary; repository instructions prohibit running the full suite without explicit user request.

## Decision Log

- Decision: Keep deletion concurrency fixed at 32 for this change and preserve `janitor_max_deletes_per_run` as a cap on successful deletions.
  Rationale: This is the requested bounded acceleration; failures and protected objects must remain retryable and must not consume successful-delete allowance.
  Date/Author: 2026-10-26 / task owner.
- Decision: Retain both coordination layers: the global PostgreSQL janitor lock for one cycle owner and per-object metadata claims with fencing for tracked uploaded objects.
  Rationale: Parallel execution must not permit stale workers to finalize newer claims or allow another janitor cycle to overlap the current cycle.
  Date/Author: 2026-10-26 / task owner.
- Decision: Treat the supplied 1,000-object run as a preliminary performance datapoint, not a baseline.
  Rationale: It was measured only on the accelerated build and has no matched pre-change run.
  Date/Author: 2026-10-26 / task owner.
- Decision: Treat the 4-versus-16 pool-size timings as indicative only and keep pool size as a run-time tuning input, not a janitor default change.
  Rationale: The consecutive runs used different object sets, and `STUDIOFS_PG_POOL_SIZE` already provides the needed experiment control.
  Date/Author: 2026-10-27 / task owner.
- Decision: Preserve serial finish ordering for claims sharing one object ID while allowing finishes for different objects to overlap, bounded at 16.
  Rationale: This avoids same-object reconciliation state races without serializing the entire finish phase.
  Date/Author: 2026-10-27 / task owner.

## Outcomes & Retrospective

The release build, focused crate checks, strict Clippy, formatting, diff check, janitor tests (4/4), and reconciliation-finish tests (2/2) passed. The final pool-16 mount cycle deleted 1,000 objects in 33,984 ms with no deletion errors, and a point post-delete `stat` returned false. The earlier pool-4 and pool-16 runs used different consecutive object sets, so they do not establish a controlled speedup. Final validation remains open because the mount reported 256 reconciliation claims retryable and 0 confirmed, with no diagnosis supplied; the full workspace suite was not run under repository instructions.

## Context and Orientation

`StudioVfs::spawn_janitor_worker` in `crates/studiofs-vfs/src/vfs.rs` starts `StudioVfs::background_janitor` in `crates/studiofs-vfs/src/vfs/workers.rs` as tracked cleanup work. A cycle gets the global PostgreSQL janitor lock, reconciles upload metadata, reads object reachability and snapshot pins, streams the remote data prefix, and considers only old unreferenced objects for deletion. `delete_janitor_object` obtains a per-object deletion claim before deleting fragment keys; a fencing token lets metadata reject stale completion from an expired claim. “Drain” means wait for every already-started deletion future to finish before releasing the global lock. Shutdown cancels remote operations, but owned claims still need their completion/failure path before cycle teardown.

The current defaults remain a 30-minute cycle interval, a 15-minute minimum object age, and up to 500 successful object deletions per cycle. These are existing safety/configuration controls, not part of the requested performance change. The acceleration uses at most 32 object-deletion futures at once; each object deletion can itself issue fragment deletes concurrently, so confirm that this bound and the underlying object-store client limits are acceptable in the live run.

The checkout is `/Volumes/Storage/dev/studiofs`, branch `feature/janitor-spead-up`. At plan creation, the worktree has uncommitted changes in `crates/studiofs-vfs/src/vfs.rs`, `crates/studiofs-vfs/src/vfs/workers.rs`, and `crates/studiofs-vfs/src/vfs/tests.rs`; preserve them. No endpoint, bucket, volume identifier, credential, or DSN belongs in this plan or its retained evidence.

## Plan of Work

First review the current changes in `crates/studiofs-vfs/src/vfs/workers.rs` against the janitor lifecycle. Ensure the queue never exceeds 32 active object deletions, stops claiming new candidates after the configured number of successful deletions, and keeps the global janitor lock until every started deletion has resolved. Confirm that cancellation and listing failures still finish or leave claims retryable, and that a tracked claim is finalized only with its original owner and fencing token. Do not weaken existing snapshot-pin, reference, age, or object-key checks.

Keep the timing fields in the cycle-completion log and verify their meanings against the actual boundaries: whole cycle, object listing plus candidate scan and queue drain, and cumulative per-object claim, remote-delete, and metadata-finish time. Do not interpret cumulative task durations as wall-clock time. If an error path omits timings needed to diagnose slow cycles, make the smallest logging adjustment and retain the same secret-free structured fields.

Then validate deterministic queue invariants, per-object reconciliation finish ordering, legacy-key cleanup, shutdown ownership, and existing janitor behavior with focused tests. If live safety or service availability is not already covered, use a disposable isolated PostgreSQL/object-store setup and a bounded, aged orphan set. Never point a mutating run at an unverified `.env` destination. The reported `STUDIOFS_PG_POOL_SIZE=4` and `=16` runs are a tuning experiment on different consecutive object sets. For a final comparison, keep the setting and workload fixed across repeated runs (or replay the same prepared set safely), and record binary revisions, scanned/deleted/error counts, retryable reconciliation count, elapsed-time fields, and point `stat` results without recording service identity or credentials.

## Milestones

### Milestone 1: Bounded, fenced cleanup

Review and complete the current implementation and unit coverage in `crates/studiofs-vfs/src/vfs/workers.rs`. The observable result is that tests prove a maximum of 32 active object-deletion tasks, the cap counts only successful deletes, and cycle drain waits for already-started work. Review the existing `StudioVfs::delete_janitor_object` path to prove claim completion remains fenced and retryable on cancellation or remote failure. Confirm `janitor_object_delete_keys` accepts only canonical legacy or fragment keys and deletes both object representations as intended.

### Milestone 2: Shutdown and timing evidence

Exercise shutdown during active deletion and ensure the worker exits only after queued work has reached a safe completion boundary and the global lock is released. During upload-fragment reconciliation, allow up to 16 metadata finish calls at once but never overlap finishes for the same object ID; claims not admitted before shutdown remain leased for safe retry after expiry. Confirm the completion log distinguishes wall-clock scan/drain time from summed claim, remote-delete, and finish durations. Focused tests and sanitized logs should make these outcomes observable without relying on fixed sleeps.

### Milestone 3: Live comparison

The reported tuning runs recorded 1,000 deletions with a PostgreSQL pool of 4 in 100,003 ms and with a pool of 16 in 34,179 ms, both with 0 deletion errors; they used different consecutive object sets and cannot establish a controlled speedup. The final mount run used pool 16, 60-second grace, 10-second interval, and a 1,000-delete cap; its first cycle took 33,984 ms, deleted 1,000 objects, and had 0 deletion errors. It also reported 256 reconciliation claims retryable and 0 confirmed, for an undiagnosed reason. Focused tests/checks passed, but diagnose that reconciliation outcome and retain the limitation on the non-paired throughput comparison before closing this milestone. The full workspace suite remains unrun per repository instructions.

## Concrete Steps

Run focused checks from `/Volumes/Storage/dev/studiofs`; do not run the workspace-wide suite:

    cargo test -p studiofs-vfs --lib janitor_deletion_queue -- --nocapture
    cargo test -p studiofs-vfs --lib reconciliation_finishes -- --nocapture
    cargo test -p studiofs-vfs --lib janitor_object_delete_keys -- --nocapture
    cargo test -p studiofs-vfs --lib janitor_is_owned_by_non_aborting_shutdown_cleanup -- --nocapture
    cargo test -p studiofs-vfs --test vfs_test test_background_janitor_removes_orphan_object -- --exact --nocapture
    cargo check -p studiofs-vfs
    cargo clippy -p studiofs-vfs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

Owner-reported validation: release build, `cargo check -p studiofs-vfs`, `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`, `cargo fmt --all -- --check`, `git diff --check`, janitor tests 4/4, and reconciliation-finish tests 2/2 passed. The full workspace suite was not run because repository instructions reserve it for the user. The final mount run used pool 16, grace 60 seconds, interval 10 seconds, cap 1,000; first cycle was 33,984 ms with 1,000 deleted, 0 deletion errors, 256 reconciliation retryable, and 0 confirmed. The cause of the reconciliation outcome has not been diagnosed. This run and the earlier pool-4/pool-16 runs used different consecutive object sets; do not present them as a same-set comparison. A point stat after deletion returned false for the checked object(s).

## Validation and Acceptance

The focused queue, reconciliation-ordering, legacy-key, and shutdown tests pass with nonzero test counts. Existing orphan cleanup still removes an old unreferenced object, and reference, snapshot-pin, age, and key validation continue to prevent unsafe deletion. A failure, timeout, or shutdown leaves tracked deletion state retryable or finalized under the matching fencing token; stale claim completion cannot overwrite the current owner. The global janitor lock is not released while a deletion task from that cycle is still running. A spot-check `stat` after deletion returns false for sampled deleted objects.

The final mount demonstrated deletion of the 1,000-object cap with 0 deletion errors. The 4-pool/16-pool timing (100,003 ms/34,179 ms) and later pool-16 timing (33,984 ms) are indicative only because each run used a different consecutive object set. A point post-delete stat returned false. Reconciliation completion remains unresolved: the final cycle reported 256 retryable and 0 confirmed. The full workspace suite is unverified and was not run per repository instructions. Native mounted, Windows, and crash/power-loss claims remain outside this plan unless separately requested and evidenced.

## Idempotence and Recovery

The janitor is a retrying cleanup worker. Remote not-found is idempotent success; protected candidates are skipped; failed or interrupted claimed deletions remain retryable through the metadata claim state. Keep the lock held until active deletion futures drain, then release it on every normal/error/shutdown path. Live measurements must use disposable data and be rerunnable with fresh aged orphan objects. Preserve the prior build and source state so the comparison can be repeated without changing an operator’s configured service.

## Artifacts and Notes

Preliminary accelerated-build datapoint supplied on 2026-10-26:

    deleted=1000
    delete_errors=0
    scanned=14341
    cycle_elapsed_ms=120710
    listing_delete_elapsed_ms=89165
    upload_reconciliation_retryable=256

This datapoint is not a before/after comparison. Additional live tuning runs supplied on 2026-10-27:

    STUDIOFS_PG_POOL_SIZE=4:  1000 deleted, 0 deletion errors, elapsed_ms=100003
    STUDIOFS_PG_POOL_SIZE=16: 1000 deleted, 0 deletion errors, elapsed_ms=34179
    comparison: different consecutive object sets; indicative only, not paired
    final mount: pool=16, grace_secs=60, interval_secs=10, cap=1000
    first cycle: elapsed_ms=33984, deleted=1000, delete_errors=0
    upload reconciliation: retryable=256, confirmed=0, cause=not diagnosed
    post-delete stat spot check: false for the checked object(s)

The release build, crate check, strict Clippy, formatting, diff check, janitor tests (4/4), and reconciliation-finish tests (2/2) passed per the task-owner report. The full workspace suite was not run per repository instructions. Diagnose the retryable reconciliation outcomes and, if a quantitative speedup claim is needed, record a controlled repeated-set comparison; omit endpoint, account, volume, bucket, DSN, and credentials.

## Interfaces and Dependencies

Reuse the existing `JanitorMeta` metadata facade and `VfsMeta` claim methods. The object cleanup path remains `StudioVfs::background_janitor` → `StudioVfs::delete_janitor_object` → `claim_uploaded_object_deletion` / `finish_uploaded_object_deletion`. Preserve the existing PostgreSQL global janitor lock and cancellation token. The in-progress queue uses `futures::stream::FuturesUnordered`; do not add a dependency or a new public API for this work.

Revision note: 2026-10-27 — Added final owner-reported release/build/check/Clippy/fmt/diff and focused-test results, plus the pool-16 mount cycle (33,984 ms, 1,000 deleted, 0 deletion errors). Recorded 256 reconciliation claims retryable with 0 confirmed and cause undiagnosed. Marked the full suite unverified per repository instructions and retained the caveat that throughput runs used different consecutive object sets.

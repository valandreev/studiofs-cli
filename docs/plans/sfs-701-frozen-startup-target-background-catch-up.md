# Catch up to a frozen startup target in the background

This is a living ExecPlan for SFS-701. It follows `.agent/PLANS.md` and must
remain current until the implementation is validated and committed.

## Purpose / Big Picture

When a client opens a stale local metadata database, it must become usable for
reads without waiting for remote replay. The background startup catch-up will
now take one remote metadata-head snapshot as its finite target and drain
bounded complete-batch pages until the local cursor reaches that target. Remote
writes published after the snapshot will not extend startup; after the client
becomes `Live`, the existing NOTIFY and fallback-poll paths consume them as
ordinary runtime replication.

The behavior is visible through the lifecycle transition from `CatchingUp` to
`Live` and through a focused PostgreSQL regression that keeps publishing above
the frozen target while the stale client catches up.

## Progress

- [x] (2026-09-12) Read SFS-701 and its completed SFS-697, SFS-698, SFS-699, and SFS-700 dependencies from Linear.
- [x] (2026-09-12) Confirm the clean branch, current commit, repository contracts, and indexed code paths.
- [x] (2026-09-12) Confirm that bounded page selection and durable per-page replay already exist, while the poll worker still performs only one page per cycle.
- [x] (2026-09-12) Add a tested bounded-page drain for one startup target, including a no-progress guard.
- [x] (2026-09-12) Start the drain from the background synchronization cycle and preserve runtime replication after `Live`.
- [x] (2026-09-12) Add focused unit and PostgreSQL moving-head coverage.
- [x] (2026-09-12) Run focused formatting, compilation, tests, clippy, and diff validation.
- [x] (2026-09-12) Complete the initial independent review pass; no Critical,
  Important, or Minor findings remained after the serialization/cancellation
  fixes.
- [x] (2026-09-12) Commit every intended task file in `b4444184`.
- [x] (2026-09-12) Update the Linear issue with the final implementation and
  validation evidence; move SFS-701 to `Done`.
- [x] (2026-09-12) Validate the follow-up review against the replay,
  invalidation, lifecycle, and telemetry contracts; apply the confirmed loop,
  documentation, and edge-test improvements.
- [x] (2026-09-12) Validate the second review; add typed stall recovery,
  explicit frozen-target page plumbing, and the zero-version fresh-client
  regression while rejecting duplicate invalidation and telemetry claims.
- [x] (2026-09-12) Validate the third review; propagate a fatal lifecycle as
  an error, correct the catch-up rustdocs, and add the overshoot regression
  while rejecting the duplicate invalidation-notification claim.

## Surprises & Discoveries

- `metadata_startup_target_version` already exists and is used by individual
  pull calls, but `start_background_poll` currently calls one
  `sync_metadata_with_source` per wake-up. Therefore a large finite backlog is
  artificially spread over poll intervals rather than handled as one startup
  session.
- `sync_metadata_with_source` already invokes `try_complete_catchup_sync` after
  each successful page. The new session loop can therefore keep the existing
  allocator and lifecycle transition logic and only own page draining and
  no-progress detection.
- The target is cleared by `try_complete_catchup_sync` after reaching it. The
  session must read the target before the first page and stop when lifecycle
  leaves `CatchingUp`/`Degraded`; it must not read a newer remote head while the
  session is active.

## Decision Log

- Decision: Keep one `pg-meta-poll` worker and add an explicit startup-session
  drain inside its cycle instead of adding a second worker. Rationale: the
  existing replication mutex already serializes NOTIFY, poll, and publication;
  one owner avoids another shutdown handle and does not change worker lifecycle
  ownership. Date/Author: 2026-09-12, Codex.
- Decision: A startup session loops only while the lifecycle is
  `CatchingUp`/`Degraded`, uses the existing frozen target, and returns a typed
  no-progress error when a page does not advance the durable batch cursor.
  Rationale: the target makes the loop finite, while no-progress detection
  prevents a missing/corrupt event tail from causing a hot spin. Date/Author:
  2026-09-12, Codex.
- Decision: Do not add target fields to public diagnostics in this issue.
  Rationale: SFS-710 owns catch-up/checkpoint progress diagnostics; this issue
  only needs observable lifecycle behavior and the existing remote/applied
  progress surface. Date/Author: 2026-09-12, Codex.
- Decision: Hold the existing replication lock across target capture and the
  startup page drain, and perform lifecycle bookkeeping after releasing it.
  Rationale: a concurrent NOTIFY sync must not clear the target between the
  session's lifecycle check and a later page; deferring bookkeeping avoids
  re-entering the same lock during fresh-client slot completion. Date/Author:
  2026-09-12, Codex.
- Decision: Check the poll shutdown flag and lifecycle before every page.
  Rationale: a large finite backlog must remain interruptible so worker
  shutdown can resume from the durable cursor instead of waiting for the
  entire backlog. Date/Author: 2026-09-12, Codex.
- Decision: Treat a no-progress page as `MetaError::CatchUpStalled`, reset the
  process-local target, and leave the lifecycle in `Degraded` for a later
  recovery cycle. Rationale: a stale or pruned range must not pin retries to
  one unattainable target. Date/Author: 2026-09-12, Codex.
- Decision: Pass the already captured target directly to each page pull.
  Rationale: the startup session owns one frozen target, so repeated atomic
  target resolution is unnecessary. Date/Author: 2026-09-12, Codex.
- Decision: Treat a fatal lifecycle observed after startup-session execution as
  `MetaError::LifecycleFatal`, not as a successful no-op drain. Rationale: a
  lifecycle transition can stop the page loop before the target is reached;
  reporting success would mark the source connected and run reachability
  backfill on an unsafe serving view. Date/Author: 2026-09-12, Codex.

## Outcomes & Retrospective

- The poll worker now drains all bounded pages up to one target captured by
  `frozen_sync_target_version`; it retains the existing one-page path after
  `Live`.
- Startup no longer preloads the process-local target during `open`; the
  background session captures it when catch-up actually begins, while the
  durable replay cursor remains the restart position.
- The portable startup catch-up filter passed: 9 passed, 0 failed; the two
  service-backed regressions remain explicitly ignored by the default test
  mode.
- The complete lifecycle-module filter passed 49 tests with 11 service-backed
  tests ignored.
- The ignored PostgreSQL empty-volume fresh-client regression passed: 1
  passed, 0 failed.
- The ignored PostgreSQL moving-head regression passed: 1 passed, 0 failed.
  The existing ignored local-first startup regression also passed: 1 passed,
  0 failed.
- `cargo fmt --all -- --check`, `cargo check -p studiofs-meta -p studiofs-vfs`,
  `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, and
  `git diff --check` passed. No workspace-wide or full ignored suite was run.
- The third-review catch-up filter passed with 11 tests passed, 0 failed, and
  1 PostgreSQL moving-head test ignored by the default mode.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/metadata_sync.rs` owns PostgreSQL foreign
event paging and local replay. `ForeignEventPage` is already bounded by batch
count and estimated response bytes; `apply_replay_page_atomically` advances the
local cursor only with the local redb transaction. `frozen_sync_target_version`
returns the session target already stored in `PgMetaEngine`, or observes and
stores one remote head when a catch-up lifecycle has no target.

`crates/studiofs-meta/src/pg_engine/lifecycle.rs` owns the `pg-meta-poll`
background thread. It currently flushes due local publication and then invokes
one metadata page pull. `crates/studiofs-meta/src/pg_engine.rs` owns lifecycle
states and `try_complete_catchup_sync`, which promotes a client only after the
frozen target is reached and allocator authority is ready.

`crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs` contains offline
lifecycle tests and the local-first stale-open regression. A new focused unit
test will exercise the page-drain control loop without PostgreSQL. A separate
ignored test will use the existing `.env`-aware `PgTestVolume` harness to prove
that a stale client reaches `Live` while the remote head advances beyond its
startup target.

## Plan of Work

First, add a small private page-drain helper in
`crates/studiofs-meta/src/pg_engine/lifecycle.rs`. It will read the current
local cursor through a closure, invoke one bounded page pull through another
closure, accumulate whether any page changed state, stop at the supplied target,
and return a typed storage error if a successful pull leaves the cursor below
the target without progress. The unit test goes into the lifecycle test module
before the helper is implemented and must fail for the missing helper.

Second, add `PgMetaEngine::run_startup_catch_up_session` in the lifecycle
module beside the background worker. It will accept only `CatchingUp` or
`Degraded`, call `frozen_sync_target_version` once at session start, hold the
replication lock while using the helper for repeated
`pull_foreign_events_locked` calls, and stop when the lifecycle leaves startup
or the poll is shutting down. It will not refresh the remote target during the
drain.

Third, update `start_background_poll` to select the startup session while the
lifecycle is still catching up and retain the existing single-page sync path
after `Live`. It will continue to flush pending local publication and perform
snapshot-reachability bootstrap, and it will notify local invalidation after
any applied page. Session errors are logged and retried on a later bounded poll
wake-up; they do not block local serving or spin in a tight loop.

Finally, add a PostgreSQL ignored regression using two engine databases. Seed
and publish a stale history, capture the stale client's target from explicit
metadata/lifecycle state rather than fixed sleeps, then publish an additional
batch above the captured target before invoking the same startup-session entry
point used by the background worker. Assert that the client reaches `Live`
before the moving head is exhausted and that the post-target version is
subsequently applied by runtime replication. Keep this service-backed test
separate from portable unit validation.

## Concrete Steps

Run commands from `E:\dev\studiofs-cli`.

1. Add the lifecycle drain test and run only that test. The first run must fail
   because the wished-for helper is not yet implemented.
2. Implement the minimal helper and `run_startup_catch_up_session`, then run
   the same test and the existing lifecycle unit filter.
3. Wire the session into the poll worker and run `cargo fmt --all -- --check`,
   `cargo check -p studiofs-meta`, and focused lifecycle/metadata tests.
4. When `STUDIOFS_TEST_PG_DSN` is available, load it through the existing test
   harness and run the ignored moving-head test with one test thread. If it is
   unavailable, report that prerequisite explicitly rather than treating a
   zero-test or skipped run as evidence.
5. Run `cargo clippy -p studiofs-meta --all-targets -- -D warnings`,
   `git diff --check`, inspect the final diff, and commit the intended files
   with a `feat:` subject.

## Validation and Acceptance

The portable regression must show that a frozen target of three with one page
per pull invokes multiple page pulls and stops exactly at three. It must also
show that a successful pull with no cursor progress returns an error instead of
spinning.

The service-backed regression passes when the stale client is `Live` while the
remote metadata head is greater than the startup target, and a later remote
version is subsequently applied through runtime replication. It must use
explicit version/lifecycle observations and no fixed coordination sleep.

Focused checks must execute a nonzero number of tests. Workspace-wide and full
ignored suites remain outside this task per repository instructions.

## Idempotence and Recovery

The page drain does not alter PostgreSQL schema or cursor semantics. Each page
continues to commit replay state atomically through the existing SFS-700 path.
On a transient PostgreSQL failure, the session returns, retains its frozen
target, and retries from the persisted local cursor on a later poll. On process
restart, `PgMetaEngine::open_with_options` observes the current remote head,
then the next background session captures a new process-local target while
preserving the durable cursor.

## Artifacts and Notes

Record only test names/counts, lifecycle/version assertions, and file-scoped
diff evidence here. Never record `.env` contents, full DSNs, credentials, or
secret-bearing configuration.

## Interfaces and Dependencies

The new internal session method will be:

    pub(super) fn run_startup_catch_up_session(
        &self,
        shutdown: &AtomicBool,
    ) -> MetaResult<bool>

The private drain helper will accept a frozen `i64` target, a local-version
reader, a continuation callback, and a one-page pull callback returning
`MetaResult<bool>`. It will return `MetaResult<bool>` where the boolean reports
whether any page changed local metadata. No new crate or external dependency
is planned.

## Revision Note

2026-09-12: Initial plan created after inspecting SFS-701, the completed
dependency contracts, the codebase graph, and the current background poll path.
2026-09-12: Revised after review to serialize the startup session under the
replication lock, defer lifecycle bookkeeping until unlock, and check
shutdown/lifecycle state between pages.
2026-09-12: Implementation committed as `b4444184`; Linear SFS-701 marked
Done with the focused validation results.
2026-09-12: Follow-up review confirmed that replay commits already publish
per-page invalidation batches and that the session lock protects the frozen
target from concurrent lifecycle completion; added drain-loop rustdoc and
edge coverage.
2026-09-12: Second review confirmed the need for a typed no-progress recovery
path and explicit target plumbing; added `CatchUpStalled`, target reset to
`Degraded`, and a zero-version fresh-client PostgreSQL regression. Notify,
lock-granularity, and aggregate telemetry recommendations remained
unsupported or contract-expanding.

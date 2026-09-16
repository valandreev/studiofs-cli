# SFS-713 Deterministic Fault Tests for Paged Resumable Catch-Up

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

After this change, the PostgreSQL metadata catch-up path will have deterministic regression coverage for the durability boundary between a fetched page and its local redb commit. A test will be able to fail a first fetch, fail after a later page has been fetched, fail immediately before the local page commit, or report an interruption immediately after that commit. Each case will reopen the same redb file and prove where the next page starts by inspecting the durable `batch_version` cursor and the replayed namespace entries.

The tests will run against the existing ignored `studiofs-meta` PostgreSQL fixture. They will use a one-batch page limit so a four-batch remote tail creates directly observable page boundaries. The existing `pg_foreign_event_page_keeps_oversized_first_batch_atomic` query regression remains the proof that SQL paging returns every row of one oversized batch together; this task adds the missing local apply/restart fault coverage.

## Progress

- [x] (2026-09-16) Retrieved SFS-713 from Linear; confirmed it is In Progress, high priority, blocked by SFS-701, and blocks SFS-716.
- [x] (2026-09-16) Confirmed a clean worktree on `feature/sfs-713-add-deterministic-fault-tests-for-paged-resumable-catch-up`.
- [x] (2026-09-16) Read the root and `studiofs-meta` contracts, `docs/pg-sync-contract.md`, and the existing SFS-700/SFS-701 catch-up seams.
- [x] (2026-09-16) Traced `run_startup_catch_up_session` through `pull_foreign_events_locked_inner` and `apply_replay_page_atomically`.
- [x] (2026-09-16) Added the `cfg(test)` fault seam and shared four-batch replay-tail fixture; the initial RED run failed on the intentionally missing seam, and the corrected RED run failed only on that missing API.
- [x] (2026-09-16) Added and ran four deterministic failure/reopen regressions plus the single oversized-batch atomic-apply regression.
- [x] (2026-09-16) Ran focused nonzero validation, reviewed the final worktree, and committed the five intended files as `695b967e` before the living-plan update.
- [x] (2026-09-16) Amended the commit with this final plan status update; the final commit hash is recorded by the post-commit verification.
- [x] (2026-09-16) Processed the attached review: strengthened resumed-fetch and persisted-cursor assertions, made manual batch setup transactional, and documented post-commit recovery semantics; retained inline per-case assertions, `std::sync::Mutex`, and timeout-only injection as codebase-appropriate.

## Surprises & Discoveries

- The current page path already has the required transaction boundary: PostgreSQL page selection commits its read transaction, while local replay state and `pgLastAppliedBatchVersion` are written in one redb transaction and the in-memory cursor is published only after commit.
- Existing lifecycle tests exercise the page-drain helper with closures, but they do not exercise the real PostgreSQL page query, redb page transaction, restart, or the next durable cursor.
- `crates/studiofs-meta/src/pg_engine/metadata_io.rs` is parse-partial around the page decoder according to codebase-memory coverage, so direct source inspection is required for that file; the relevant source was inspected directly.

## Decision Log

- Decision: Put the fault injection seam behind `#[cfg(test)]` on `PgMetaEngine` and expose only a crate-private hook used by the metadata tests.
  Rationale: SFS-713 asks for deterministic test failures, not a production fault API. Keeping the field and callback test-only avoids changing the runtime contract or public surface.
  Date/Author: 2026-09-16 / Codex.
- Decision: Represent faults as callbacks receiving explicit page-boundary points rather than using sleeps, wall-clock timeouts, or process-kill orchestration.
  Rationale: The callback can fail at the exact fetch/apply/commit boundary and can record the cursor/page versions observed, making restart assertions deterministic and local to the metadata layer.
  Date/Author: 2026-09-16 / Codex.
- Decision: Use four remote batches with `STUDIOFS_PG_SYNC_MAX_BATCHES_PER_PAGE=1` and reopen the same redb path after every injected failure.
  Rationale: A one-batch page makes the durable cursor and the next fetched page unambiguous while keeping the fixture small and CI-friendly.
  Date/Author: 2026-09-16 / Codex.

## Outcomes & Retrospective

Implemented SFS-713 in `studiofs-meta`. `PgMetaEngine` now has a test-only callback seam at page fetch, page selection, pre-redb-commit, and post-redb-commit boundaries. The pre-commit fault path aborts the staged redb transaction; the post-commit fault path returns an injected timeout after durable commit but before in-memory cursor publication.

The four fault regressions publish four remote batches with one batch per page, inject the requested failure, inspect the local view and persisted cursor, reopen the same redb database, record the first resumed fetch cursor, and verify all four entries after recovery. The oversized-batch regression inserts two valid events under one batch with a byte limit of one, then proves both rows apply and the page diagnostics mark the page oversized.

Fresh validation passed: `cargo test -p studiofs-meta startup_catch_up_fault --lib -- --ignored --nocapture --test-threads=1` reported 4 passed; `cargo test -p studiofs-meta startup_catch_up_single_oversized_batch_is_applied_atomically --lib -- --ignored --nocapture --test-threads=1` reported 1 passed; `cargo test -p studiofs-meta metadata_paging --lib -- --nocapture` reported 7 passed and 8 explicitly ignored infrastructure tests; `cargo test -p studiofs-meta pg_sync --lib -- --nocapture` reported 19 passed and 1 explicitly ignored test; `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed. The full workspace and unrelated ignored suites were not run by repository policy. The final amended commit is the delivery artifact.

The attached review follow-up preserves the four fault tests as self-contained stories while moving the shared resume invariants into their existing resume helper. The manual oversized-batch fixture now commits its event rows transactionally, and `AfterLocalCommit` documents the required drop-and-reopen recovery boundary. The suggested `parking_lot` migration and generalized error factory remain out of scope because the former would add a test-only direct dependency despite adjacent `std::sync::Mutex` hooks, while the latter has no current caller.

## Context and Orientation

`PgMetaEngine` in `crates/studiofs-meta/src/pg_engine.rs` owns a local `redb` metadata engine plus PostgreSQL synchronization state. The local `redb` database is the foreground serving view. PostgreSQL `event_log` contains remote-visible metadata events grouped by the monotonic `batch_version` column.

`crates/studiofs-meta/src/pg_engine/lifecycle.rs` runs one bounded startup session. `run_startup_catch_up_session` freezes a target version and repeatedly invokes the page pull while holding `replication_lock`. `crates/studiofs-meta/src/pg_engine/metadata_sync.rs` queries one complete page, decodes it, and calls `apply_replay_page_atomically`. That function applies the decoded page and writes the local replay cursor into the same redb transaction as the serving-state changes, then publishes atomics only after `txn.commit()` succeeds. A failure before commit must leave both state and cursor unchanged; an interruption after commit must leave both durable so reopening starts after that page.

The existing tests are split under `crates/studiofs-meta/src/pg_engine/tests`. `lifecycle.rs` contains catch-up lifecycle tests and ignored PostgreSQL restart tests. `mod.rs` contains `PgTestVolume`, `seed_local`, and the shared `set_test_env`/`pg_dsn` helpers. `metadata_paging.rs` contains SQL page-boundary and oversized-batch tests.

## Plan of Work

First, add RED integration tests in `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs`. The shared fixture will create a PostgreSQL test volume, seed a stale local redb database at cursor zero, publish four ordinary directory batches from a separate publisher engine, and return the stale database path plus the expected names. Each test will put the stale engine into `CatchingUp`, freeze the remote target, install a callback that fails exactly once at the requested page point, and run one startup session. The test will assert the injected typed error and the durable cursor before and after reopening. A successful retry will then apply all remaining pages and verify all names are visible.

Next, add the smallest test-only callback field and methods. The callback points will be `BeforePageFetch { cursor }`, `AfterPageFetch { cursor, first_batch_version, last_batch_version }`, `BeforeLocalCommit { last_batch_version }`, and `AfterLocalCommit { last_batch_version }`. Invoke the first point immediately before the real PostgreSQL page query, the second after a non-empty page has been fetched, the third after redb replay state has been staged but before `txn.commit()`, and the fourth after `txn.commit()` but before in-memory cursor publication. The before-commit path must abort the transaction before returning the injected error. The after-commit path must return the injected error without undoing the already durable page.

Finally, add focused assertions for the four acceptance cases. The before-fetch case must retain cursor zero and resume from zero. The later-page fetch and before-local-commit cases must retain the first page only and resume from cursor one. The after-local-commit case must reopen at cursor one even though the interrupted engine did not publish its in-memory cursor. The test recording callback observations must show the resumed fetch starts at the durable cursor, not at the original checkpoint. The existing oversized first-batch query test remains unchanged unless a focused integration assertion is needed to make the SFS-713 boundary explicit.

## Concrete Steps

1. From `E:\dev\studiofs-cli`, add the four ignored RED tests and the shared fixture helper to `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs`. Use `MetaError::Timeout` for injected failures so the normal sync failure classification is exercised, and use a shared `Arc<Mutex<Vec<CatchUpFaultPoint>>>` recorder to capture observed cursors and page versions.

2. Run the new test filter before adding the production test seam:

       cargo test -p studiofs-meta startup_catch_up_fault --lib -- --ignored --nocapture --test-threads=1

   Expected result: compilation fails because the requested test-only fault hook and/or point type does not yet exist. Correct compilation/name errors before proceeding; do not accept a passing test as RED evidence.

3. Add the `#[cfg(test)]` callback field to `PgMetaEngine`, initialize it in the normal constructor and `temp_pg_engine_for_unit`, and add the crate-private setter/invocation helpers in the relevant PG engine module. Keep all new runtime fields, methods, and enum variants test-only.

4. Run the same focused ignored command. Expected result: the four tests execute against the configured PostgreSQL service and pass, with nonzero test counts. If `STUDIOFS_TEST_PG_DSN` is unavailable, report the ignored infrastructure prerequisite separately and still run the non-ignored compile/test filter.

5. Run focused nonzero validation from `E:\dev\studiofs-cli`:

       cargo test -p studiofs-meta startup_catch_up_fault --lib -- --ignored --nocapture --test-threads=1
       cargo test -p studiofs-meta metadata_paging --lib -- --nocapture
       cargo check -p studiofs-meta
       cargo clippy -p studiofs-meta --all-targets -- -D warnings
       cargo fmt --all -- --check
       git diff --check

   The first command is the service-backed SFS-713 gate. The other commands prove the changed crate compiles, the existing paging tests still run, and formatting/diff/lint checks are clean. Do not run workspace-wide or full ignored suites.

6. Inspect `git diff` and `git status`, update this plan's Progress and Outcomes sections with exact evidence, and commit only the plan, test, and test-only hook files using a `<type>: <subject>` message such as `test: add deterministic pg catch-up fault coverage`. The implementation reached this step with the worktree containing only the five intended files; the commit is the remaining delivery action.

## Validation and Acceptance

Acceptance is behavioral. A failure before PostgreSQL page fetch leaves the durable cursor at zero. A failure after fetching page two or immediately before its local commit leaves the durable cursor at one and does not make page two's namespace entry visible. A reported interruption after page one commits leaves the reopened database at cursor one and the page-one entry durable. Retrying from each reopened database fetches only the remaining pages and reaches the frozen target without replaying or losing a committed page.

The focused PostgreSQL command must report four nonzero passing tests when `STUDIOFS_TEST_PG_DSN` is configured. The normal metadata paging and crate checks must also exit zero. A missing PostgreSQL service or DSN is an environment limitation, not a passing SFS-713 result.

## Idempotence and Recovery

Each `PgTestVolume` uses a unique name and UUID and deletes its volume on drop, so rerunning a test is safe. Each local database uses a unique target temporary directory. The test-only callback is stored on the engine instance and is dropped with that engine; it is not global and cannot affect another test after teardown. The repository's existing environment override guard restores page-limit settings after each test.

If an injected after-commit error leaves an engine instance with a stale in-memory cursor, the test must drop that instance before reopening the same redb path. Reopening is the recovery action under test; no sleep or retry loop may be used to establish durability.

## Artifacts and Notes

The primary artifacts are the focused test output and the final diff. Retain exact test names and counts in the final report. Do not include PostgreSQL DSNs, credentials, or other secret-bearing configuration in the plan, logs, commit message, or final response.

## Interfaces and Dependencies

The only new internal test interface is a crate-private callback with this shape:

       #[cfg(test)]
       enum CatchUpFaultPoint {
           BeforePageFetch { cursor: i64 },
           AfterPageFetch {
               cursor: i64,
               first_batch_version: Option<i64>,
               last_batch_version: Option<i64>,
           },
           BeforeLocalCommit { last_batch_version: i64 },
           AfterLocalCommit { last_batch_version: i64 },
       }

       #[cfg(test)]
       fn set_catch_up_fault_hook(
           &self,
           hook: Arc<dyn Fn(CatchUpFaultPoint) -> MetaResult<()> + Send + Sync>,
       );

The callback uses `std::sync::Arc` and `std::sync::Mutex`, already used by the PG engine tests. Production synchronization continues to use Tokio PostgreSQL, the existing `replication_lock`, and one redb transaction per complete event page; no new dependency or public API is required.

Plan revision: updated on 2026-09-16 after implementing the test-only seam, observing the expected RED compile failure, correcting the replay-tail fixture, passing the focused service-backed and static validation commands listed above, and amending the final commit to include this status.

# Add bounded complete-batch event-log paging

This is a living ExecPlan for SFS-699. It follows `.agent/PLANS.md` and must
be kept current until the implementation is validated and committed.

## Purpose / Big Picture

PostgreSQL catch-up currently reads the complete event-log tail after the local
batch cursor. A stale client can therefore request an arbitrarily large result,
which increases timeout and memory risk and makes every retry repeat the same
large request. After this change, the metadata engine will request one bounded
page whose rows contain only whole `batch_version` replay units and whose upper
version is an explicit frozen target. The caller will receive the first and
last batch versions, page size metadata, an oversized-single-batch marker, and
whether another page remains before the target.

## Progress

- [x] (2026-09-12) Read SFS-699 and related SFS-700/SFS-701 contracts from Linear.
- [x] (2026-09-12) Confirm the clean feature branch and locate the current pull path.
- [x] (2026-09-12) Read the repository, metadata, PostgreSQL sync, and Rust implementation contracts.
- [x] (2026-09-12) Add bounded SQL selection and a typed page result.
- [x] (2026-09-12) Route metadata pull through a frozen target and configured page budgets.
- [x] (2026-09-12) Add focused empty, multi-page, target-boundary, and oversized-batch coverage.
- [x] (2026-09-12) Run focused formatting, compilation, tests, clippy, and diff validation.
- [x] (2026-09-12) Review the final staged diff, commit all intended task files, and report remaining follow-up work.
- [x] (2026-09-12) Apply the review annotation's two confirmed simplifications and rerun focused validation.

## Surprises & Discoveries

- The branch is based on `origin/main` after SFS-698. `metadata_startup_target_version` already exists, but the current foreign-event SQL ignores it and the pull query has no `LIMIT` or upper bound.
- `RemoteApplyCoordinator::decode_foreign_batch` currently groups on both `batch_version` and `client_id`. The page boundary must be defined by `batch_version`; malformed mixed-client rows for one version must not be silently split into separate replay units.
- The existing `(vol, batch_version, seq_id)` index is present in the runtime schema and can support ordered candidate-batch selection.

## Decision Log

- Decision: Use one SQL statement with a candidate batch-version prefix, aggregate response-byte estimates per candidate batch, and a final row projection. Rationale: this preserves one PostgreSQL round trip and prevents arbitrary row `LIMIT` from splitting a batch. Date/Author: 2026-09-12, Codex.
- Decision: Bound pages by maximum batch count and estimated PostgreSQL row response bytes. If the first batch exceeds the byte budget, return that complete batch alone with `oversized = true`; never split or silently skip it. Rationale: a complete batch is the atomic replay unit, and the issue explicitly requires pathological batches to be handled visibly. Date/Author: 2026-09-12, Codex.
- Decision: Keep the durable replay cursor as `batch_version`; expose `seq_id` only as the existing row-order tie-breaker. Rationale: SFS-700 owns durable per-page progress and the sync contract forbids a separate sequence cursor. Date/Author: 2026-09-12, Codex.
- Decision: Resolve an existing startup target before each pull; otherwise observe one remote head before the page query. Rationale: SFS-699 must provide a fixed upper bound without making SFS-701's background session part of this change. Date/Author: 2026-09-12, Codex.

## Outcomes & Retrospective

The SQL now selects an ordered prefix of complete `batch_version` groups through
the frozen target, then keeps the longest prefix within the configured estimated
response-byte budget. The first batch is always retained when it alone exceeds
that budget and is reported as `oversized`; later batches remain for a future
page. The page returns first/last versions, batch/event counts, estimated bytes,
and `has_more` metadata in the same PostgreSQL round trip.

The pull path captures an existing startup target or observes one remote head,
loads positive per-client batch/byte budgets, and applies the returned rows using
the existing local replay transaction. Replay grouping now treats
`batch_version` as the only atomic boundary and rejects malformed mixed-client
rows instead of silently splitting one version.

Validation completed:

- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo check -p studiofs-meta --target-dir target\\sfs699-check`
- `cargo clippy -p studiofs-meta --all-targets --target-dir target\\sfs699-clippy -- -D warnings`
- `cargo test -p studiofs-meta foreign_event_page_query_is_bounded_and_exposes_page_metadata --lib` (1 passed)
- `cargo test -p studiofs-meta pg_foreign_event_pages_respect_cursor_target_and_batch_budget --lib -- --ignored --nocapture --test-threads=1` (1 passed)
- `cargo test -p studiofs-meta pg_foreign_event_page_keeps_oversized_first_batch_atomic --lib -- --ignored --nocapture --test-threads=1` (1 passed)

The live PostgreSQL tests used the existing `.env`-aware harness and passed.
Workspace-wide and full ignored suites remain outside this task. Durable
per-page progress remains SFS-700 work, and reusable background frozen-target
session orchestration remains SFS-701 work.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/sql.rs` contains the SQL constant
`SELECT_FOREIGN_EVENT_ROWS`. It selects an ordered candidate prefix of complete
`event_log` `batch_version` groups after the cursor and through the supplied
target, then projects only the selected complete batches in `(batch_version,
seq_id)` order.

`crates/studiofs-meta/src/pg_engine/metadata_io.rs` wraps that SQL in
`PgMetaEngine::query_foreign_event_page`. The wrapper is called from
`crates/studiofs-meta/src/pg_engine/metadata_sync.rs` inside a short PostgreSQL
transaction that revalidates the metadata format before returning rows and page
metadata to the local replay code.

`crates/studiofs-meta/src/pg_engine/remote_apply.rs` decodes the ordered rows
into `DecodedReplayBatch` values and applies each batch to local `redb`. A page
must therefore end only between complete versions. The local `batch_version`
cursor is advanced by the existing apply/persist path; this issue adds the page
boundary and metadata but does not redesign durable progress.

The runtime schema in `crates/studiofs-meta/src/pg_engine/schema.rs` defines
`event_log` and the usable `(vol, batch_version, seq_id)` index. The defaults
are exposed through PostgreSQL sync environment variables read when a
`PgMetaEngine` is opened, with safe positive fallbacks.

## Plan of Work

First, add a typed `ForeignEventPage` result in the metadata I/O module. It will
own the PostgreSQL rows and report optional first/last batch versions, batch and
event counts, estimated response bytes, `has_more`, and `oversized`. Replace the
unbounded SQL with a CTE that selects at most the configured number of ordered
batch versions after the cursor and through the supplied target, computes each
candidate batch's response-byte estimate, chooses the longest prefix within the
byte budget, and admits the first oversized batch alone. The final projection
returns all rows for selected versions in `(batch_version, seq_id)` order and
repeats page metadata in hidden-to-callers result columns so the one query can
return both rows and metadata.

Second, add positive page-budget defaults and per-engine fields for maximum
batches and maximum response bytes. Load them at open time using the existing
PostgreSQL environment parsing conventions, and initialize the unit-test engine
fixture with the same defaults. Change the pull path to pass an explicit target
and these budgets to the page query. When a startup target is already present,
use it; otherwise observe the remote head once before pulling and record it for
catch-up lifecycles. Keep the existing post-pull head observation as diagnostic
state without allowing it to expand the current page's upper bound.

Third, make row-to-batch decoding use `batch_version` as the only page/replay
boundary. If one version contains inconsistent client identities, fail closed
instead of applying part of that version as one client and the remainder as
another. This protects the atomicity guarantee without changing valid batches.

Finally, add focused PostgreSQL tests in a new metadata-paging test module. Use
the existing `PgTestVolume` harness and ignored-test convention to verify two
or more pages, empty cursor/target ranges, exclusion of rows above the target,
and a first batch larger than the configured byte target that remains whole and
is marked oversized. Add non-service checks where useful for default parsing
and SQL contract text, but do not replace the live query coverage.

## Concrete Steps

Run commands from `E:\dev\studiofs-cli`.

1. Edit the SQL, metadata I/O, sync, engine construction, test fixture, and new
   focused test module with `apply_patch`. Keep unrelated worktree changes
   untouched.
2. Run `cargo fmt -- --check` and a focused `cargo check -p studiofs-meta`.
3. Run the focused metadata unit test filter and the new PostgreSQL test filter.
   The PostgreSQL tests require `STUDIOFS_TEST_PG_DSN` loaded through the
   existing `.env` harness and remain separately reported if that service is
   unavailable.
4. Run `git diff --check`, inspect `git diff`, and verify no secret-bearing DSN
   or configuration is introduced into code, logs, the plan, or the report.
5. Commit all intended task files with a `feat:` subject after validation.

## Validation and Acceptance

The SQL/API acceptance is met when a page query always has a positive batch and
byte budget, applies both `batch_version > cursor` and `batch_version <= target`,
returns no rows for an empty range, returns rows for complete versions only,
and reports whether more complete versions remain before the target. A page
whose first complete batch exceeds the normal byte budget returns that complete
batch alone with `oversized = true`; no version is split.

The pull-path acceptance is met when the query no longer uses the old unlimited
tail and receives a fixed upper target. Existing local overlay and replay
application behavior must remain unchanged for valid pages.

Focused validation must have nonzero executed tests. A `running 0 tests`
result is not evidence. Workspace-wide and full ignored suites are outside this
task and remain the user's separate gate.

## Idempotence and Recovery

The change is additive to the existing schema and uses an already-present
index. Re-running unit tests creates isolated temporary local databases. Live
PostgreSQL tests use unique volume names and the existing cleanup guard. A page
query failure returns before local replay, so the existing cursor remains
unchanged; durable per-page restart guarantees belong to SFS-700.

## Artifacts and Notes

Record the final SQL/API shape, focused test names and counts, compile/format
results, and any unavailable PostgreSQL prerequisite in the final report and in
the plan's retrospective. Do not include `.env` contents or full DSNs.

## Interfaces and Dependencies

The new internal query interface will accept a generic synchronous PostgreSQL
client, volume ID, last batch version, frozen target batch version, maximum
batch count, and maximum response-byte budget. It returns `MetaResult<ForeignEventPage>`.
The page metadata uses the existing row order and `batch_version` cursor. No
new external crate or schema migration is planned.

## Revision Note

2026-09-12: Initial plan created after reading SFS-699 and the current
`studiofs-meta` pull/replay path. It deliberately leaves durable page commits
and background frozen-target sessions to SFS-700 and SFS-701.

2026-09-12: Implemented and validated the bounded page query, frozen-target
plumbing, runtime budgets, atomic replay guard, focused unit coverage, and live
PostgreSQL coverage. The final staged diff was reviewed and committed; the
follow-up review simplifications are included in the current changeset.

2026-09-12: Verified the attached review annotation against the call graph and
sync contract. Removed the redundant page alias and moved one-use frozen-target
plumbing into `pull_foreign_events_locked`; focused compile, clippy, portable,
and live PostgreSQL checks passed again.

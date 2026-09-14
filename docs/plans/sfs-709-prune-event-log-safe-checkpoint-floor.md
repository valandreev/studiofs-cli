# Prune PostgreSQL event history behind a safe checkpoint floor

This ExecPlan is a living document. It follows `.agent/PLANS.md` and must be
kept current until SFS-709 is implemented, validated, and committed.

## Purpose / Big Picture

PostgreSQL `event_log` is the ordered history used by StudioFS clients that
are catching up. Before this change, successful bootstrap checkpoint
publication did not make old history eligible for bounded cleanup, so the
table could grow indefinitely. After this change, background metadata
maintenance will calculate the highest event version that can no longer be
needed by a published bootstrap checkpoint or a retained Snapshot v2 source,
delete only complete event batches in small transactions, and persist the
last safe floor plus cumulative deleted-row count. A PostgreSQL test will show
that old rows are removed while an active snapshot source keeps its replay
range intact.

## Progress

- [x] (2026-09-14) Read SFS-709, its relations, and the current repository
      state from Linear; confirmed SFS-704 is already merged and SFS-711 is a
      separate replay-gap follow-up.
- [x] (2026-09-14) Read the root and `studiofs-meta` contracts, PostgreSQL sync
      contract, metadata documentation, and ExecPlan requirements.
- [x] (2026-09-14) Used Codebase Memory to locate the PostgreSQL schema,
      checkpoint coordinator, background poll lifecycle, snapshot source
      capture, and existing event-log paging paths; verified the relevant
      inbound call paths.
- [x] (2026-09-14) Recorded the initial design: persist an inclusive deleted
      batch floor and cumulative deleted rows in `metadata_state`; compute the
      candidate floor from the published checkpoint and non-deleted Snapshot
      v2 source ranges; delete complete batch groups under a per-volume
      advisory transaction lock and metadata row lock.
- [x] (2026-09-14) Added schema fields, bounded prune SQL, and the engine
      maintenance method.
- [x] (2026-09-14) Ran the red/green focused unit and PostgreSQL regressions.
- [x] (2026-09-14) Wired maintenance into the existing background poll without
      putting it on foreground writes or startup.
- [x] (2026-09-14) Updated operational documentation and this plan with final
      evidence.
- [x] (2026-09-14) Inspected the final diff, ran formatting/lint/diff checks,
      and committed all intended SFS-709 files.
- [x] (2026-09-14) Re-verified the attached review: added replay-gap fencing,
      removed idle metadata writes, aggregated DELETE results, made the janitor
      lock non-blocking, documented the prune limit, skipped pruning after
      failed sync, fixed the normative contract sentence, and tested both
      source-guard release and an entirely pruned replay window.
- [x] (2026-09-14) Re-verified the second review: isolated prune failures from
      sync validation state, added startup column checks, avoided no-op locks,
      restricted pruning to established lifecycle states, documented failed
      snapshot guards and all focused PG regressions, and added empty-page
      boundary unit tests.
- [x] (2026-09-14) Re-verified the third review: skipped pruning when no
      published checkpoint exists, made empty-window diagnostics explicit
      without expanding the ReplayGap field contract, removed unreachable
      empty-page handling, rejected missing page metadata, renamed the cursor
      parameter, and covered prune-floor edge cases.

## Surprises & Discoveries

- Observation: The checkpoint coordinator already runs on a dedicated thread,
  while `flush_metadata()` calls the same checkpoint method synchronously.
  Evidence: `run_checkpoint_refresh_once` is called by both
  `start_background_checkpoint_coordinator` and
  `flush_metadata` through `write_checkpoint_with_lease`.
- Observation: Snapshot capture takes the per-volume PostgreSQL advisory
  janitor lock before reading `metadata_state FOR SHARE` and the event bounds.
  Evidence: `capture_snapshot_in_transaction` in
  `crates/studiofs-meta/src/pg_engine/snapshot_control.rs` uses
  `pg_advisory_xact_lock` before creating `snapshot_source_checkpoints`.
- Observation: Snapshot source replay reads rows strictly after the stored
  `checkpoint_version` and through the immutable `cut_version`, so a source
  with events must constrain pruning through its first event version minus one.
  Evidence: `SELECT_SNAPSHOT_EVENT_ROWS` and the source validation in
  `snapshot_control.rs`.
- Observation: SFS-711 owns checkpoint import/rebootstrap recovery and
  automatic rebootstrap is SFS-712. SFS-709 now rejects a non-contiguous page
  with `MetaError::ReplayGap` so it cannot silently skip pruned history; an
  empty page also reports that result whenever the cursor is below its frozen
  target. It does not add the recovery flow.
  Evidence: the current Linear relation graph for SFS-709, SFS-711, and
  SFS-712.

## Decision Log

- Decision: Store `event_log_prune_floor` as the highest batch version known to
  have been safely pruned, and store `event_log_pruned_rows` as a cumulative
  diagnostic counter in `metadata_state`. Rationale: `metadata_state` already
  carries the authoritative per-volume checkpoint and is available to the
  future replay-gap check without introducing another coordination table.
  Date/Author: 2026-09-14, Codex.
- Decision: Define the candidate floor as the minimum of the fully published
  bootstrap `checkpoint_version` and every retained source's
  `event_start_version - 1`; sources with no event rows do not constrain the
  floor. Rationale: checkpoint bytes replace history through their version,
  while a source with events still needs its first event and every later event
  through its cut. Date/Author: 2026-09-14, Codex.
- Decision: Include every `snapshot_source_checkpoints` row whose catalog is
  not `deleted`, including `creating`, `ready`, and `deleting` rows. Rationale:
  final cleanup removes the source before the catalog reaches `deleted`, and
  a retained source may still be needed for reconstruction or an active mount.
  Date/Author: 2026-09-14, Codex.
- Decision: Delete complete `batch_version` groups, limited by a configured
  maximum number of groups per maintenance transaction. Rationale: replay
  treats a batch version as atomic, so a row-count `LIMIT` must not split one
  batch; the existing producer response-byte bound keeps normal batches
  bounded while the first pathological legacy batch is still removed as one
  unit. Date/Author: 2026-09-14, Codex.
- Decision: Run one bounded prune attempt from the existing background poll,
  but never from the checkpoint coordinator, `flush_metadata()`, or engine
  construction. Rationale: the poll already runs after `flush_due_pending()`
  and retries failed maintenance every interval, while keeping pruning off the
  checkpoint worker avoids delaying later ordinary publication behind an
  advisory-lock wait. Date/Author: 2026-09-14, Codex.
- Decision: Treat an already-reached prune floor as a no-op, aggregate DELETE
  counts in PostgreSQL, and use `pg_try_advisory_xact_lock`. Rationale: idle
  polls must not dirty `metadata_state`, one scalar result avoids per-row
  allocations, and background maintenance must defer rather than block
  publication behind another janitor transaction. Date/Author: 2026-09-14,
  Codex.
- Decision: Keep `ReplayGap.first_available_version` as an integer and use
  `local_version + 1` as the explicit empty-window marker. Rationale: the
  SFS-711 recovery consumer is not part of SFS-709, so changing the error field
  type would expand the current contract prematurely; the marker is documented
  and cannot be confused with a non-contiguous populated page. Date/Author:
  2026-09-14, Codex.
- Decision: Serialize pruning with Snapshot v2 capture using the existing
  per-volume janitor advisory lock, then lock `metadata_state FOR UPDATE` in
  the same transaction before computing the floor and deleting rows. Rationale:
  this orders pruning with source-range capture and checkpoint publication;
  the transaction recomputes the floor after waiting, and the existing event
  trigger remains a final safety guard for creating snapshots. Date/Author:
  2026-09-14, Codex.

## Outcomes & Retrospective

The repository now has a durable per-volume pruning floor, bounded
complete-batch event deletion, retryable background execution, a defensive
non-contiguous-page guard for both populated and empty windows, and focused
evidence that retained Snapshot v2 replay rows survive and later release
pruning. The later SFS-711 checkpoint recovery can consume
`MetaError::ReplayGap`; automatic rebootstrap remains outside this task.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/schema.rs` defines the canonical PostgreSQL
schema. `metadata_state` has one row per volume, including the monotonic remote
head and the latest fully published serving checkpoint. `event_log` stores
ordered rows grouped by `batch_version`; all rows in one batch version are one
replay unit.

`crates/studiofs-meta/src/pg_engine/checkpoint_coordinator.rs` publishes a
serving checkpoint under a fenced lease. Its worker is already owned and joined
by `PgBackgroundWorkers` in `lifecycle.rs`. The existing poll worker in that
file periodically flushes pending local work and pulls remote pages. The prune
operation will use that existing background lifecycle instead of creating a
second maintenance subsystem.

Snapshot v2 registration in
`crates/studiofs-meta/src/pg_engine/snapshot_control.rs` stores the checkpoint
and the exact event bounds in `snapshot_source_checkpoints`, while
`snapshot_catalog.lifecycle` describes whether that source is still retained.
The source reader uses those bounds to reconstruct the snapshot. A source with
`event_row_count > 0` and `event_start_version = 7` requires event rows from
version 7 onward through its cut, so pruning may advance only through version
6 while that source remains retained.

The current local replay page query in `sql.rs` still returns ordered complete
batch groups. It rejects a non-empty page whose first batch is not exactly the
next version after the local cursor, and rejects any empty page while the
cursor is below the requested target. After decoding, the sync path also
rejects gaps between batches in one page before foreign apply.

## Plan of Work

First, extend the fresh and additive schema in
`crates/studiofs-meta/src/pg_engine/schema.rs` with nonnegative
`event_log_prune_floor` and `event_log_pruned_rows` columns, both defaulting to
zero. Add the same columns to the migration assertions and the contract
harness check. Existing checkpoint and dump/load inserts intentionally omit
these columns so PostgreSQL defaults preserve compatibility with the current
write paths.

Next, add a small internal pruning module under
`crates/studiofs-meta/src/pg_engine/event_log_pruning.rs`. It will expose a
`PgMetaEngine::run_event_log_prune_once` method and an internal result carrying
the candidate floor, the durable floor after this transaction, the rows
deleted in this batch, and the cumulative deleted-row count. The method will
validate the positive configured batch-group limit, acquire a PostgreSQL
transaction, take the existing `pg_janitor_lock_key` advisory lock, lock the
volume's published `metadata_state` row, compute the source-constrained floor,
delete only the earliest complete event batches up to that floor, and advance
the persisted floor only through versions with no remaining rows below the
candidate. The DELETE returns one aggregate count/max-batch row; an
already-reached floor skips DELETE and UPDATE, and a contended janitor lock
defers via `pg_try_advisory_xact_lock`. It will commit one bounded transaction
and redact all backend error text through the existing error classification
conventions.

Add `STUDIOFS_PG_EVENT_LOG_PRUNE_BATCHES` with a positive default to
`pg_engine.rs`, load it through the existing environment parsing helper, and
store it in `PgMetaEngine`. Add the field to the unit fixture so portable tests
continue to construct the engine explicitly.

Wire the method into the existing background poll after a successful
synchronization. A failed prune will be logged as a maintenance failure and
retried by a later poll; a failed synchronization skips pruning for that poll,
and neither case will make an otherwise successful foreground flush fail. Include
the candidate floor, durable floor, deleted rows, and cumulative rows in the
structured maintenance log when a prune attempt reaches PostgreSQL. Only
`Live` and `TargetReached` lifecycle states run this maintenance; catching-up
and degraded clients skip it.

Finally, add focused tests in a new
`crates/studiofs-meta/src/pg_engine/tests/event_log_pruning.rs` module. Portable
tests will cover positive-limit validation, candidate-floor arithmetic, and
the rule that a partial transaction cannot advance the durable floor past an
older remaining batch. Ignored PostgreSQL tests will insert several complete
batches, invoke one prune attempt with a small group limit, verify bounded
deletion and monotonic persisted diagnostics, create a retained source range
and verify its rows survive, and verify that a source with no event rows does
not unnecessarily pin the floor. The tests will use the existing `.env`-aware
`PgTestVolume` harness and unique volume names.

Update `docs/metadata.md` and the relevant section of
`docs/pg-sync-contract.md` to describe the inclusive persisted prune floor,
Snapshot v2 source constraint, bounded background transactions, and the fact
that replay-gap recovery remains SFS-711. Keep credentials, DSNs, and secret
configuration out of all documentation and diagnostics.

## Concrete Steps

Run commands from `E:\dev\studiofs-cli`.

1. Maintain this plan and edit only the schema, PostgreSQL engine, focused
   tests, and documentation files named above. Use `apply_patch` for source
   edits and preserve unrelated worktree changes.
2. Add portable red tests for floor calculation and SQL/schema fragments, then
   run them with `cargo test -p studiofs-meta event_log_pruning --lib`.
3. Implement the schema, transaction, configuration, and worker wiring; rerun
   the portable tests and the explicitly named ignored PostgreSQL tests with
   `.env` loaded by the existing harness when the service is available.
4. Run `cargo fmt --all -- --check`, a focused `cargo check -p studiofs-meta`,
   focused `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, and
   `git diff --check`. Do not run workspace-wide or full ignored suites.
5. Inspect the final diff for lock ordering, complete-batch deletion,
   redacted diagnostics, and unchanged foreground/startup paths. Commit all
   intended SFS-709 files with a `feat:` subject.

## Validation and Acceptance

The schema acceptance is met when a fresh schema and an existing schema passed
through `ensure_studiofs_pg_schema` both provide nonnegative persisted pruning
fields without changing the checkpoint or event publication contract.

The transaction acceptance is met when one maintenance attempt computes a
floor no greater than the latest fully published checkpoint and no greater
than the first required event version minus one for every retained source,
deletes at most the configured number of complete batch groups, and advances
the durable floor only over versions that are no longer present. A second
attempt continues from the remaining old groups. Snapshot source rows needed
for reconstruction remain present and protected.

The lifecycle acceptance is met when pruning runs only from a background path,
is retried after a temporary PostgreSQL failure, does not execute during
`PgMetaEngine::open_with_options`, and does not make `flush_metadata()` wait
for deletion work. Structured logs and persisted counters expose the selected
floor and deleted-row amount without exposing credentials or full DSNs.

Focused validation must report nonzero executed tests. A `running 0 tests`
result is invalid evidence. Live PostgreSQL tests and native mounted checks are
environment-gated; unavailable services must be reported separately rather
than replaced with a mock-only claim.

Validation evidence from 2026-09-14:

- The initial schema assertion was red with `running 1 test` and the expected
  missing `event_log_prune_floor` failure; the corrected schema assertion then
  passed with `1 passed; 684 filtered out`.
- `cargo check -p studiofs-meta --target-dir target\\sfs709-check` passed.
- `cargo test -p studiofs-meta event_log_pruning --lib --target-dir
  target\\sfs709-test -- --nocapture` passed with 3 executed and 2 ignored
  tests.
- The explicitly ignored PostgreSQL bounded-deletion test passed with 1
  executed test; it verified three one-group transactions, complete-batch
  deletion, and `(floor, pruned_rows) = (3, 6)`.
- The explicitly ignored PostgreSQL Snapshot v2 retention test passed with 1
  executed test; it verified the retained source floor, preserved rows, and a
  zero-row source that does not pin history.
- `cargo test -p studiofs-meta snapshot_v2_public_contract --lib --target-dir
  target\\sfs709-test -- --nocapture` passed with 1 executed test.
- Both focused `architecture_vocabulary_test` documentation checks passed.
- `cargo clippy -p studiofs-meta --all-targets --target-dir
  target\\sfs709-clippy -- -D warnings`, `cargo fmt --all -- --check`, and
  `git diff --check` passed.

Latest third-review validation on 2026-09-14:

- `cargo test -p studiofs-meta foreign_event_page --lib` passed with 5
  executed and 5 ignored unit tests; the two focused ignored PostgreSQL
  replay-gap tests passed.
- `cargo test -p studiofs-meta event_log_prune --lib` passed with 4 executed
  and 3 ignored unit tests; the three focused ignored PostgreSQL pruning tests
  passed.
- `cargo check -p studiofs-meta`, clippy with `-D warnings`, formatting, and
  `git diff --check` passed after the review fixes.

Latest fourth-review validation on 2026-09-14:

- The decoded replay path now rejects gaps between batches in one page before
  applying any events, and the empty-page helper returns `MetaResult<T>`
  directly with the `ReplayGap` error.
- The event-log trigger now protects source rows in every catalog lifecycle
  except `deleted`, matching the pruning floor query; the focused PostgreSQL
  snapshot test covers a retained `ready` source as well as `creating`.
- Added the zero source-floor and invalid batch-limit environment cases
  (`abc`, `-5`, and `0`) to the portable pruning tests.

## Idempotence and Recovery

Schema setup is additive and uses `IF NOT EXISTS`/`ADD COLUMN IF NOT EXISTS`.
Each prune transaction is safe to retry: it recomputes the floor under locks,
deletes only complete batch groups, and updates counters transactionally. A
failed transaction leaves both event rows and diagnostics unchanged. If a
snapshot source is concurrently retained, the advisory/metadata lock ordering
causes the next floor calculation to see it; the existing event trigger rejects
an unsafe delete as a final fail-closed guard.

## Artifacts and Notes

The implementation artifacts are the living plan, schema/engine/SQL changes,
focused portable and PostgreSQL tests, and the updated synchronization
documentation. The final plan entry will record the exact test names/counts,
the focused compile/lint/format evidence, and any unavailable PostgreSQL
prerequisite without recording `.env` contents.

## Interfaces and Dependencies

The public `MetaEngine`, `MetaReplication`, and CLI interfaces remain
unchanged. The internal engine will contain a method shaped as
`PgMetaEngine::run_event_log_prune_once() -> MetaResult<Option<EventLogPruneResult>>`.
The optional result represents an uninitialized or not-yet-published metadata
row and avoids treating that state as a deletion failure. The result's fields
are internal diagnostics only; the persisted PostgreSQL interface is the two
nonnegative `metadata_state` columns described above.

No new external crate is planned. The implementation reuses the existing
Tokio PostgreSQL pool, transaction wrapper, advisory lock key, environment
parsing, tracing, and `PgTestVolume` fixtures.

## Revision Note

2026-09-14: Initial SFS-709 ExecPlan created after reading the Linear contract,
the PostgreSQL sync and Snapshot v2 contracts, the current checkpoint/poll
lifecycle, and the Codebase Memory call paths. The plan deliberately leaves
replay-gap recovery to SFS-711 and automatic rebase to SFS-712.
2026-09-14: Implemented the additive schema, locked bounded maintenance
transaction, background wiring, focused tests, and operational documentation;
all listed focused validation passed. Commit evidence is recorded in the
final repository history.

# SFS-700 Persist Resumable Catch-Up Progress Atomically

This ExecPlan is living and follows `.agent/PLANS.md`.

## Purpose / Big Picture

After this change, a PostgreSQL-backed client that catches up through several bounded event pages will durably remember the last completely applied `batch_version`. If the process is interrupted after a page commit, the next open starts at the following page; if applying a page fails or its local transaction is aborted, neither the page's serving state nor its replay cursor is visible. The behavior is demonstrated by focused `studiofs-meta` tests that reopen the same redb file and inspect both replayed metadata and persisted counters.

## Progress

- [x] (2026-09-12) Retrieved SFS-700 from Linear, including its SFS-699 dependency and SFS-701 blocker relation.
- [x] (2026-09-12) Confirmed the repository is on `feature/sfs-700-persist-resumable-catch-up-progress-atomically` with a clean worktree.
- [x] (2026-09-12) Traced the current path: `pull_foreign_events_locked_inner` queries one bounded page, `RemoteApplyCoordinator::apply_event_rows` applies it through multiple redb transactions, and `persist_replay_position` writes counters afterward in separate transactions.
- [x] (2026-09-12) Read the root and `studiofs-meta`/`studiofs-vfs` contracts, the PostgreSQL sync contract, metadata documentation, and the repository ExecPlan rules.
- [x] (2026-09-12) Added the transaction-abort regression and observed the expected initial compile failure for the missing page-scoped APIs.
- [x] (2026-09-12) Added transaction-aware redb replay operations and transaction-scoped monotonic cursor writes.
- [x] (2026-09-12) Routed foreign page apply through one local transaction and publish in-memory positions only after commit.
- [x] (2026-09-12) Ran the focused replay, replication, paging, and replay-position tests, formatting, check, clippy, diff validation, and final diff inspection.
- [x] (2026-09-12) Committed all intended SFS-700 files as `8aa77f12` with `fix: persist pg catch-up progress atomically`.
- [x] (2026-09-12) Applied the two verified review simplifications and reran the target checks.
- [x] (2026-09-12) Committed the review simplifications as `7e1d5c71` with `refactor: trim replay wrapper complexity`.

## Surprises & Discoveries

- The SFS-699 bounded page query and frozen target are already present on `origin/main`, but the page application still calls separate redb commit helpers for base state, serving state, auxiliary state, reconciliation, and cursor counters.
- `RedbWriteTransaction` already owns the writer lock and invalidation publication, and its `Deref<Target = redb::WriteTransaction>` lets existing transaction helpers be reused. The missing piece is passing that transaction through replay rather than starting a new transaction in each public helper.
- Existing transaction-aware redb helpers use the write snapshot for merged reads (`get_attr_merged_in_write_txn`, `lookup_merged_in_write_txn`, and chunk-state helpers). These are required because later batches in one page must see earlier batches before the page commits.
- The ponytail review identified the blanket dead-code allowance on `RemoteApplyCoordinator` and repeated counter monotonic-write blocks as removable complexity. The `_in_txn` variants themselves remain required composition seams; replacing them with a generic abstraction would add indirection to a single transactional path.

## Decision Log

- Decision: Use one redb write transaction for one returned PostgreSQL foreign-event page, including all replay state, serving reconciliation, and applicable PG cursor counters.
  Rationale: The SFS-700 contract names the complete page as the durability unit and forbids a cursor that can skip uncommitted replay state.
  Date/Author: 2026-09-12 / Codex.
- Decision: Keep existing public redb replay methods as wrappers that begin and commit their own transaction, while adding crate-visible `_in_txn` methods for the page path.
  Rationale: This preserves existing callers and tests while giving the PG catch-up path a narrow transaction composition boundary.
  Date/Author: 2026-09-12 / Codex.
- Decision: Store the new counter values in the local transaction before commit and update atomics only after successful commit.
  Rationale: On restart, persisted redb counters are authoritative; publishing an in-memory value before commit would make the running process disagree with durable state after a failed commit.
  Date/Author: 2026-09-12 / Codex.
- Decision: Do not add a second sequence cursor or a crash-recovery journal.
  Rationale: SFS-700 explicitly prefers `batch_version` as the durable page boundary, and the existing local event sequence is only needed for self-batch overlay acknowledgement.
  Date/Author: 2026-09-12 / Codex.

## Outcomes & Retrospective

Implemented the SFS-700 page durability boundary in `studiofs-meta`. A complete decoded PostgreSQL page now applies base, serving, auxiliary, reconciliation, and replay-position state through one `RedbWriteTransaction`; atomics publish only after the commit succeeds. An aborted-page regression proves the mutation and cursor do not persist, while the committed-page/reopen regression proves the next page resumes after the persisted version.

Changed files are the page orchestration and cursor code in `pg_engine`, the transaction-aware redb replication/read helpers, the focused replay tests, and this plan. Validation passed: `cargo test -p studiofs-meta remote_apply --lib -- --nocapture` (`65 passed, 44 ignored`), `cargo test -p studiofs-meta replication --lib` (`29 passed, 1 ignored`), `cargo test -p studiofs-meta metadata_paging --lib` (`5 passed, 6 ignored`), `cargo test -p studiofs-meta replay_position --lib` (`1 passed`), `cargo test -p studiofs-meta replica --lib` (`53 passed, 1 ignored`), `cargo fmt --all -- --check`, `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, and `git diff --check`. The PostgreSQL ignored tests and native FUSE/WinFSP mounted acceptance were not run because they are outside the available metadata-only validation environment.

Ponytail review outcome: the blanket dead-code allowance and 11 unused production wrappers were removed, test-only compatibility wrappers were marked `cfg(test)`, and the three repeated counter updates were reduced to one monotonic loop. The transactional helper duplication remains justified by the existing public/test APIs and the required single-transaction composition.

## Context and Orientation

`PgMetaEngine` in `crates/studiofs-meta/src/pg_engine.rs` wraps a local `RedbMetaEngine`. The local redb database is the mounted serving view. PostgreSQL `event_log` is read in bounded, complete-`batch_version` pages by `query_foreign_event_page` in `crates/studiofs-meta/src/pg_engine/metadata_io.rs`.

`crates/studiofs-meta/src/pg_engine/metadata_sync.rs` currently performs the remote read and calls `RemoteApplyCoordinator::apply_event_rows`. `crates/studiofs-meta/src/pg_engine/remote_apply.rs` decodes rows into complete batches, applies foreign or self batches, reconciles serving mirrors, and returns `RemoteReplayApplyOutcome`. `crates/studiofs-meta/src/pg_engine/local_state.rs` currently persists `pgLastAppliedBatchVersion`, `pgLastAppliedNamespaceBatchVersion`, and `pgLastFlushedLocalEventSeq` through independent calls to `RedbMetaEngine::set_counter`.

`crates/studiofs-meta/src/redb_engine/replication.rs` contains the local base, serving, auxiliary, and reconciliation implementations. Its public methods currently begin and commit `RedbWriteTransaction` independently. The transaction-aware variants will receive the already-open `RedbWriteTransaction`, preserve the existing invalidation behavior, and leave commit ownership with the caller.

The page path must preserve local overlay authority. It must use the write transaction's merged reads when deciding whether a foreign batch conflicts with local state, must not append remote events to the local event log, and must not advance any cursor until the complete page transaction commits.

## Plan of Work

First, add a focused unit regression in `crates/studiofs-meta/src/pg_engine/tests` that opens a temporary redb engine, starts a write transaction, applies a representative foreign page to the transaction, writes its replay position into that transaction, aborts it, reopens the database, and asserts that the base/serving mutation and applied-version counter are both absent. The test should initially fail to compile because the transaction-scoped page entry point is not yet present; after the failure is observed, implement the smallest API needed.

Next, in `crates/studiofs-meta/src/redb_engine/replication.rs`, extract the bodies of base apply, serving apply, auxiliary apply, regular-file serving reconciliation, dentry reconciliation, and rename reconciliation into crate-visible transaction variants. Keep their existing public wrappers as begin/apply/commit adapters. Add the small transaction-aware read helpers needed by `remote_apply.rs`: merged attr lookup, merged namespace lookup, base attr lookup, overlay-graph presence, lifecycle lookup, and regular-file slice lookup. Reuse existing write-snapshot helpers wherever possible instead of introducing a parallel metadata model.

Then, in `crates/studiofs-meta/src/pg_engine/remote_apply.rs`, add transaction-scoped forms of decoded batch application, foreign batch application, self-batch application, and final reconciliation. Pass the open `RedbWriteTransaction` through the operation. Replace read calls that would otherwise start a separate snapshot with the corresponding write-transaction helpers. Retain the existing non-transactional methods as wrappers for current unit-test and compatibility callers.

In `crates/studiofs-meta/src/pg_engine/local_state.rs`, add a transaction-scoped cursor writer that updates the three counters monotonically in the provided transaction but does not publish atomics. Add a post-commit helper that publishes only the values that were actually committed. Keep the existing standalone persistence methods for unrelated flush and lifecycle callers.

Finally, in `crates/studiofs-meta/src/pg_engine/metadata_sync.rs`, decode the selected page, begin one local write transaction, apply every decoded batch plus final reconciliation, write the resulting durable positions into that same transaction, commit it, update atomics and pending replay state after the commit, and return the outcome. Any error before commit must drop/abort the transaction and leave the previous durable cursor intact. The next page will then use the persisted `local_version` as its `last_batch_version`.

## Concrete Steps

All commands run from `E:\dev\studiofs-cli` in PowerShell. Do not run the workspace-wide suites; the repository contract reserves those for the user/operator.

1. Confirm the starting point with `git status --short`, `git branch --show-current`, and `git log -1 --oneline`.
2. Add the focused test before production implementation and run it with `cargo test -p studiofs-meta <test-filter> --lib -- --nocapture`. Expected initial result: a compile error identifying the missing transaction-scoped page API, not `running 0 tests`.
3. Implement the transaction variants and run the same test. Expected result: one named test passes and the reopened redb file contains neither the aborted replay mutation nor the advanced cursor.
4. Run neighboring metadata replay tests with separate commands, at minimum the lifecycle replay-position tests, remote-apply tests, and bounded-page unit tests. Use `--lib -- --nocapture` and verify each command reports at least one executed test.
5. Run `cargo fmt --all -- --check`, `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, and `git diff --check` sequentially.
6. Inspect `git diff --stat`, `git diff -- crates/studiofs-meta`, and `git status --short`. Stage only SFS-700 implementation, test, plan, and directly required documentation files; preserve any unrelated worktree changes.
7. Commit with a message of the form `fix: persist pg catch-up progress atomically` after the focused checks pass.

## Validation and Acceptance

The core acceptance test must demonstrate both sides of the transaction boundary: after an aborted page, the reopened local database has the previous applied-version counter and previous serving/base state; after a committed page, the reopened database has the replayed state and the last batch version. A second page must start after the committed first page, proving the persisted cursor is used as the resume boundary rather than refetching the already committed page.

The targeted Rust evidence is valid only when the named tests execute with nonzero counts. PostgreSQL-backed ignored tests remain environment-gated and may be run only when the configured service is available; if not run, report that separately. Native FUSE/WinFSP mounted acceptance is outside this metadata-only change and must not be claimed from crate tests.

## Idempotence and Recovery

The plan is safe to repeat because the focused tests use isolated temporary redb databases and the page path is monotonic. A failed local replay transaction is discarded by redb; the in-memory cursor is not changed before commit, so the next retry reads from the prior persisted batch version. If a compile or focused test command fails, fix the implementation or test and rerun that command before moving on. Do not reset or discard unrelated worktree changes.

## Artifacts and Notes

The living artifact is this plan. The final response will link the plan and changed source files using absolute workspace paths and will include the exact focused validation commands and outcomes.

## Interfaces and Dependencies

The final page path is expected to expose these crate-internal boundaries:

- `RedbMetaEngine` transaction-aware replay methods accept `&RedbWriteTransaction<'_>` and never commit it.
- `RemoteApplyCoordinator` has a transaction-scoped page/batch apply entry point that returns `RemoteReplayApplyOutcome` after all decoded batches and reconciliation have been applied to that transaction.
- `PgMetaEngine` has a transaction-scoped replay-position writer that monotonically writes the applied version, namespace version, and flushed local event sequence counters into the supplied transaction.
- The existing `RedbWriteTransaction::commit` remains the only commit point for the page and continues to publish invalidation intents only after a successful redb commit.

## Revision Note

Initial plan created on 2026-09-12 after inspecting SFS-700, its SFS-699 dependency, the current bounded pull implementation, and the local redb transaction boundaries. The plan will be revised whenever implementation evidence changes the design.

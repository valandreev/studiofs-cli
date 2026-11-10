# Retire uploaded payloads abandoned before publication

This living plan follows `.agent/PLANS.md`.

## Purpose / Big Picture

Writing and deleting a file before its metadata reaches PostgreSQL must not leave its uploaded objects protected forever. The owning metadata engine will retire abandoned uploads after its local metadata journal is acknowledged, allowing the existing fenced Janitor to delete them. An uploaded object whose publication is merely delayed remains protected.

## Progress

- [x] (2026-11-10) Read SFS-1066, inspect clean main checkout and upload/publication/Janitor paths.
- [x] (2026-11-10) Identify durable pending-deletion markers and the outbound journal acknowledgement barrier.
- [x] (2026-11-10) Added a PostgreSQL regression; before the fix it failed with uploaded instead of delete_pending.
- [x] (2026-11-10) Implemented retirement after explicit flush and at mounted Janitor admission, preserving references and leases.
- [x] (2026-11-10) Final validation: 14 PostgreSQL module tests, one disposable CLI/S3 acceptance test, three focused VFS tests, check, Clippy, fmt and diff checks passed.

## Surprises & Discoveries

Upload confirmation removes its local ledger record, but PostgreSQL retains `uploaded` until publication. Pending deletion markers survive restart locally but are deliberately excluded from shared metadata events. Allocator watermarks do not acknowledge delayed metadata publication and cannot authorize this cleanup. An independent source review checked stale-upload reattachment: the worker attempts reattachment before adding a stale deletion marker; truncation advances the epoch and leaves EOF/tombstones that reject restoration; rollback returns before marking deletion; actual inode deletion prevents reattachment. These producer orderings matter because local locks are released before PostgreSQL I/O.

## Decision Log

- Decision: Reuse durable local pending-deletion markers after the complete local journal is acknowledged; retain the existing Janitor deletion protocol.
  Rationale: No new foreground remote calls, public ports, event variants, schema or age-based guessing are needed. Local graph/recovery references and PostgreSQL references/pins remain protective.
  Date/Author: 2026-11-10 / Codex.
- Decision: Retirement is best-effort in `flush_metadata` and mounted Janitor admission; its query is a shared `LazyLock` statement.
  Rationale: A separate review found that a retirement error failed flush (and so distributed-flock release and dump/load) after replication had succeeded, and turned Janitor admission into a reported PostgreSQL lock failure. The local markers already make a deferred retirement retryable.
  Date/Author: 2026-11-10 / Claude.
- Decision: Do not retire from the CLI Janitor or the unmount path.
  Rationale: Only the owning client's redb holds the pending-deletion markers and the unpublished journal tail, and shutdown deliberately avoids PostgreSQL I/O. The objects from the write/delete/unmount repro are retired when that client mounts again. If the client never returns, its unpublished metadata is lost too, so this is outside SFS-1066.
  Date/Author: 2026-11-10 / Claude.

## Outcomes & Retrospective

The metadata regression passes after the fix. Fourteen focused PostgreSQL reachability tests pass sequentially against an isolated PostgreSQL 18 container. Their first parallel run had a fixture schema DDL deadlock (13 passed, 1 failed); the serial run passed all 14. The live B2 volume is not a test destination. The disposable S3 acceptance exercises remote key deletion, delayed publication from a separate writer client, and terminal ledger state. Final validation passed as recorded below; an independent source review found no correctness blocker.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/snapshot_reachability.rs` owns upload-ledger replay and payload lifetimes. Its upload confirmation leaves an object in `uploaded`. `redb_engine/gc.rs` stores pending-deletion object IDs locally, and scans all base, overlay and recovery payload references. `PgMetaEngine.flushed_event_seq` is the acknowledged local journal cursor; `NEXT_EVENT_SEQ` is its exclusive tail. `replication_lock` serializes outgoing publication; the paired `local_position_lock` and `pending_state` locks protect the local acknowledgement/reference snapshot. PostgreSQL's `metadata_state` row serializes publication with deletion eligibility. `pg_engine/janitor.rs` already fences deletion and protects shared references and snapshots.

## Plan of Work

Add `retire_abandoned_uploads` to the metadata engine. Its callers hold the replication lock. Under the paired local-position and pending-state locks, require the acknowledged cursor to cover the journal tail, then collect pending-deletion IDs with no base, overlay or recovery reference. Release local locks before remote work. In bounded PostgreSQL batches take the publication fence, recheck shared references, snapshot pins and active reconciliation leases, transition eligible completed `uploaded` objects to terminal `delete_pending`, and enqueue them for the existing Janitor. Call this after explicit metadata flush and before mounted Janitor lock acquisition so periodic retries and shutdown flushes converge. Do not run full local reachability scans on every metadata poll.

## Milestones

First add focused PostgreSQL regressions covering abandoned uploaded objects and a delayed publication. Then implement retirement and verify deletion is claimable while still-live uploads remain protected. Finally run narrow crate validation and update this plan with exact results.

## Concrete Steps

From the repository root run the new focused ignored tests against a disposable PostgreSQL service using an explicitly supplied `STUDIOFS_TEST_PG_DSN`. Do not read a destination from `.env`. Run `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`. Record actual regression commands after test names are established.

## Validation and Acceptance

A confirmed uploaded object whose local references were removed must reach `delete_pending`, enter the GC queue, and be claimable by the existing deletion protocol. An object with a delayed publication must remain protected before acknowledgement; an object retained by another local slice, PostgreSQL reference, snapshot or lease must remain protected. Full suites and native mounted tests are separate user-run gates.

## Idempotence and Recovery

Retirement is conditional and idempotent. Failures leave local deletion markers intact for retry. Existing deletion fences cannot be revived by late upload replay. Tests use unique disposable volumes and temporary redb files under `target/`; no live volume is mutated.

## Artifacts and Notes

The initial worktree was clean on `main`. Work continues on `feature/sfs-1066-payload-objects-deleted-before-publication`.

## Interfaces and Dependencies

No public interfaces or dependencies change. Reuse `MetaGc`, local journal counters, PostgreSQL timeout/error classification, and the existing snapshot-pin query and fenced deletion path.

Plan created after tracing the upload and publication ordering boundaries.

Updated after red/green metadata validation and the source review of stale-upload marker ordering. Only completed uploaded rows are retired; incomplete uploading rows continue through existing fragment reconciliation.

Updated scheduling to use existing mounted Janitor admission plus explicit flush: this avoids scanning the full local graph on idle metadata polls and acquires no Janitor lease before a potentially failing retirement.

## Validation evidence

The red command was `cargo test -p studiofs-meta abandoned_uploaded_objects_retire_only_after_local_publication_is_settled --lib -- --ignored --exact pg_engine::tests::snapshot_reachability::abandoned_uploaded_objects_retire_only_after_local_publication_is_settled --nocapture`, with an explicitly exported disposable PostgreSQL DSN. It ran one test and failed because the ledger remained uploaded. The final focused run uses `cargo test -p studiofs-meta --lib pg_engine::tests::snapshot_reachability:: -- --ignored --test-threads=1`: 14 passed.

`cargo check -p studiofs-meta` and `cargo clippy -p studiofs-meta --all-targets -- -D warnings` passed. CLI test code is checked with `cargo clippy -p studiofs --bin studiofs --tests -- -D warnings`. Three focused VFS filters passed one test each: `reattach_waits_for_file_session_and_observes_truncate_tombstone`, `test_size_changing_setattr_rolls_back_partial_truncate_mutation_failure`, and `truncate_masks_packed_fragments_beyond_old_eof_before_deleting_payload`.

The isolated S3 command is `cargo test -p studiofs --bin studiofs live_cli_disposable_retires_abandoned_upload_but_protects_delayed_publication -- --ignored --nocapture`, after `cargo build -p studiofs --bin studiofs`. The test first removes the helper's seeded candidate so retirement itself must enqueue it, then ages that newly enqueued candidate by one second because CLI admission uses a whole-second cutoff. Its separate synchronized janitor client can run while the writer retains a delayed publication. A first version relying on the preseeded candidate passed; removing it exposed the fixture's sub-second eligibility boundary, so the fixture now explicitly controls candidate age instead of waiting by wall-clock guesswork.

No workspace-wide/full ignored suite, Windows/WinFSP mount, provider B2 volume, or crash/power-loss test was run. The repository `.env` was not edited. Disposable service logs remain under `target/janitor-adversarial/`; service teardown is scoped to these runs.

Completed: final S3 acceptance passed with 1 test, the isolated PostgreSQL container/volume were removed, and diagnostics remain in target/janitor-adversarial/. Full-suite and native mounted acceptance remain separate gates.

Separate review (2026-11-10): the claim that CLI dry-run writes through retirement was rejected. The CLI uses `PgJanitorSession::try_acquire_global_janitor_lock`, which is unchanged. After the best-effort and static-query corrections, `cargo clippy -p studiofs-meta --all-targets -- -D warnings` passed, and the 14 focused PostgreSQL reachability tests passed again against an isolated PostgreSQL 18 container, which was then removed. The S3 acceptance was not rerun; its CLI path and asserted states do not depend on this change. Two items remain open: repeated local reference scans while markers wait for Janitor claim, and liveness of retirement at Janitor admission on a mount that never quiesces.

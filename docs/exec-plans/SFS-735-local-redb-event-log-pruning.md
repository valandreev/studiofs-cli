# Prune acknowledged local redb event history

This ExecPlan is a living document. Maintain it according to `.agent/PLANS.md` until SFS-735 is implemented and validated.

## Purpose / Big Picture

Local metadata currently keeps every event row in `EVENT_LOG_TABLE`, including acknowledged history that no longer serves replication or recovery. After this change, background maintenance will delete that history a bounded batch at a time while preserving every sequence at or beyond the durable local replication cursor. A restart will resume from the same persisted cursor, and runtime metadata diagnostics will show the retained span and pruning progress.

PostgreSQL `event_log` retention is a separate feature and must not change here. Physical redb compaction is also separate; deleting rows makes pages reusable but does not promise that the operating-system file shrinks.

## Progress

- [x] (2026-09-26) Read SFS-735 and verify SFS-732 and SFS-734 are complete.
- [x] (2026-09-26) Trace the event sequence allocator, durable local replication cursor, SFS-734 repair records, metadata diagnostics, and background poll.
- [x] (2026-09-26) Add bounded, sparse-key, rollback, restart, safe-cursor, and SFS-734 recovery regressions.
- [x] (2026-09-26) Implement bounded local redb pruning, persist its safe floor atomically, expose diagnostics, and run it before remote synchronization.
- [x] (2026-09-26) Run focused metadata and runtime-diagnostics tests, crate checks, Clippy, formatting, and diff checks.
- [x] (2026-09-27) Verify the SFS-735 review feedback; fix lock scope, poll backlog draining, idle-range telemetry, zero-floor diagnostics, and the floor-one write-transaction fast path.
- [x] (2026-09-27) Run focused review-follow-up tests and the required metadata/VFS validation gates.

## Surprises & Discoveries

- Observation: `flushed_event_seq` is an exclusive boundary. Events are allocated starting at sequence 1, and local replay reads `start_seq..end_seq`; completed ranges advance the cursor to their exclusive end.
  Evidence: `append_event_in_txn` in `crates/studiofs-meta/src/redb_engine/txn.rs`, `read_events_range_with_check` in `crates/studiofs-meta/src/redb_engine/replication.rs`, and `flushed_event_seq_after_completed_ranges` in `crates/studiofs-meta/src/pg_engine/local_state.rs`.
- Observation: SFS-734 moved uploaded-but-unpublished file repair to durable recovery rows; that path no longer reads `EVENT_LOG_TABLE`.
  Evidence: `restore_acknowledged_uploaded_regular_file_overlays_after_redb_repair` in `crates/studiofs-meta/src/pg_engine/replication_flush.rs`.
- Observation: SFS-732 already reports the retained local event span through `LocalStorageDiagnostics` and the VFS runtime-stats surface. New progress fields must pass through both layers.
  Evidence: `read_event_log_diagnostics` in `crates/studiofs-meta/src/redb_engine/gc.rs` and `insert_local_storage_diagnostics` in `crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs`.
- Observation: A missing sequence key below the durable exclusive cursor does not make later stored rows unsafe: the cursor advances only across completed ranges and stays before rejected or in-flight suffixes. Pruning can remove every stored key below the cursor while preserving rows at or above it.
  Evidence: `local_event_pruning_removes_sparse_acknowledged_rows_across_batches` covers sparse keys over two bounded batches; `local_event_pruning_keeps_rejected_and_unprocessed_suffix_without_postgres` checks the cursor boundary.
- Observation: Snapshot replacement has two event-journal contracts. Imports that preserve the local journal must retain its prune floor; full remote replacement clears the journal and resets local sequence state, so it must reset the old floor too.
  Evidence: `import_remote_snapshot_preserving_local_event_log_replaces_serving_state` and `import_remote_snapshot_drops_legacy_local_pg_state`.

## Decision Log

- Decision: Keep the exclusive `flushed_event_seq` as the eligibility boundary and persist the inclusive `local_event_log_pruned_through_seq` in the same redb transaction as each deletion batch. On open, derive the effective replay cursor as at least one past that prune floor. Rationale: the floor makes committed deletion progress restart-safe if local cursor state is restored from a snapshot that retains the event journal. Date/Author: 2026-09-26, Codex.
- Decision: Delete at most the configured row limit from sorted keys below the durable cursor, even when stored keys have gaps; preserve sequence keys and `nextEventSeq`. Keep the prune floor when an import preserves local event history, and clear it when an import replaces that history. Rationale: the durable cursor is the safety proof for every sequence below it, while full journal replacement starts a new local event-log state. Date/Author: 2026-09-26, Codex.
- Decision: Run local pruning from the existing background metadata poll before remote synchronization and keep it outside the PostgreSQL global janitor-lock path. Rationale: local reclamation needs only local durable state and must still proceed when remote retention is contended or unavailable. Date/Author: 2026-09-26, Codex.
- Decision: Report cumulative events pruned and the most recent batch's range, encoded-payload byte estimate, and duration through existing local metadata diagnostics. Rationale: these values are cheap to collect while rows are removed and fit the existing metadata-redb runtime-stat surface. Date/Author: 2026-09-26, Codex.
- Decision: Retain only `local_position_lock` across the prune transaction, not `pending_state`. Rationale: cursor and snapshot transitions use the position lock, while queue-only edits do not change the durable prune boundary; cursor-advancing transitions still take the position lock. Avoiding the second lock shortens contention for replication work. Date/Author: 2026-09-27, Codex.
- Decision: Let each poll drain at most 16 full 256-row transactions and check shutdown between transactions. Rationale: one batch per poll can leave a sustained eligible backlog; a finite budget raises cleanup throughput while keeping every redb transaction and each poll's total prune work bounded. The batch size and budget remain internal because no workload evidence calls for runtime configuration. Date/Author: 2026-09-27, Codex.
- Decision: Preserve the last non-empty successful batch range through idle or failed passes, and report no prune watermark until the stored inclusive sequence is greater than zero. Rationale: an empty first pass is not a removed range, and sequence zero means no event was pruned. Date/Author: 2026-09-27, Codex.
- Decision: Keep the persisted-floor maximum when a caller supplies a stale lower cursor. Rationale: the persisted floor is an already-proven safe watermark, and the sole production caller restores its cursor to at least one past that floor; the maximum also lets a valid prior proof cover any stale keys. Date/Author: 2026-09-27, Codex.
- Decision: Skip making the 256-row transaction limit configurable and skip restructuring the final B-tree lookup without measured evidence. Rationale: the current fixed limit is part of bounded maintenance, while configuration and extra exit-state branches would add policy or code for an unmeasured cost. Date/Author: 2026-09-27, Codex.

## Outcomes & Retrospective

Implemented bounded local redb event pruning under the exclusive durable cursor. Each batch and its inclusive prune floor commit atomically; restart derives an effective cursor no earlier than the floor. Snapshot paths that preserve local history retain the floor, while full journal replacement clears it with the rest of the local journal state. The background poll runs this local-only work before remote sync, and runtime stats expose retained span, progress, rows, range, payload estimate, and duration.

Validation passed:

- `cargo test -p studiofs-meta --lib local_event_pruning` — 6 passed.
- `cargo test -p studiofs-meta --lib redb_repair_restores_acknowledged_uploaded_overlay_before_startup_cleanup` — 1 passed.
- `cargo test -p studiofs-meta --lib import_remote_snapshot_preserving_local_event_log_replaces_serving_state` — 1 passed.
- `cargo test -p studiofs-meta --lib import_remote_snapshot_drops_legacy_local_pg_state` — 1 passed.
- `cargo test -p studiofs-meta --lib persisted_pg_flushed_event_seq_roundtrips_in_local_counter` — 1 passed.
- `cargo test -p studiofs-vfs --lib runtime_stats_surface_reports_overlay_batch_metrics` — 1 passed.
- `cargo check -p studiofs-meta -p studiofs-vfs` — passed.
- `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

PostgreSQL integration and mounted FUSE/WinFSP tests were not needed for this local redb-only prune path.

The 2026-09-27 code-review follow-up also passed:

- `cargo test -p studiofs-meta --lib local_event_pruning` — 10 passed, including the cursor-lock, poll-budget, no-op transaction, idle-range, and zero-watermark regressions.
- `cargo test -p studiofs-vfs --lib runtime_stats_surface_reports_overlay_batch_metrics` — 1 passed; the unpruned watermark is omitted and a positive watermark is surfaced.
- `cargo check -p studiofs-meta -p studiofs-vfs` — passed.
- `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

The review's suggested stale-floor early return was not applied: the persisted floor is a previously proven safe watermark, and the production caller restores its cursor to at least one past it. Making the 256-row batch configurable and restructuring one B-tree lookup were also skipped because neither has workload measurements supporting more policy or code.

## Context and Orientation

`RedbMetaEngine` owns `EVENT_LOG_TABLE` and allocates monotonically increasing sequence keys. `PgMetaEngine` owns the durable `LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER`, which records the first sequence not yet acknowledged as complete. The local repair state added by SFS-734 lives outside the event journal, so acknowledged journal history is no longer needed for that recovery path.

The metadata crate already computes `first_local_event_seq`, `next_local_event_seq`, and `retained_local_event_span` without scanning event values. Its `LocalStorageDiagnostics` then flows through `studiofs-vfs` to the runtime stats file. Add pruning measurements to this path rather than adding a new telemetry surface.

## Plan of Work

Tests at the existing redb and `PgMetaEngine` seams show that pruning removes at most its batch limit, never crosses the exclusive durable cursor, leaves rejected and pending suffixes available, and leaves the next-event counter unchanged. The uploaded-overlay repair regression removes the acknowledged event prefix before repair/reopen. Diagnostics cover the retained span, removed range, encoded payload estimate, and duration.

The bounded redb helper removes the next stored key while it is below the safe cursor, commits deletions and the new floor atomically, and estimates removed logical payload bytes from the encoded values already in hand. It records results only after a successful commit. `PgMetaEngine` retains `local_position_lock` while it obtains the cursor and prunes, which fences cursor and snapshot transitions; it does not hold `pending_state` during redb I/O because pending-queue edits do not change the durable safe boundary.

Call the local prune pass from `start_background_poll` before its PostgreSQL sync and PostgreSQL event-log janitor work. It must not acquire the PostgreSQL global janitor lock. Add bounded counters and last-batch measurements to `LocalStorageDiagnostics`, then map them into the existing `metadata_redb_*` runtime stats in VFS.

## Concrete Steps

Run commands from `/Volumes/Storage/dev/studiofs`.

1. Add and run the focused metadata regression tests before implementation. Confirm the tests fail because acknowledged local rows are still retained or the expected diagnostics are absent.
2. Implement the redb batch deletion, cursor-guarded maintenance call, poll integration, and metric mapping. Re-run the same focused tests and expect them to pass.
3. Run focused metadata and VFS tests, then `cargo check -p studiofs-meta -p studiofs-vfs`, `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`.

## Validation and Acceptance

The behavior is accepted when tests prove that each transaction removes no more than its configured batch size; rows at or above the durable exclusive cursor and rejected or unprocessed ranges remain; a later batch resumes without renumbering; the prune floor and next-event sequence survive reopen; and uploaded-but-unpublished regular-file recovery succeeds after its acknowledged event prefix has been pruned. Tests must also prove the before-commit drop preserves rows and a committed prune survives reopen.

Runtime stats must expose the retained sequence span, cumulative events pruned, the latest removed sequence range, the encoded payload byte estimate, and the latest prune duration. A focused local-prune test must run without PostgreSQL and without acquiring its global janitor lock. Physical file shrink is not an acceptance condition.

## Idempotence and Recovery

An empty eligible prefix is a no-op. Repeating a prune pass is safe because only rows below the same durable cursor are eligible. A transaction dropped before commit leaves the event table unchanged; a committed batch remains removed after reopen. The existing `nextEventSeq` counter is never modified or renumbered by pruning.

## Artifacts and Notes

The implementation must not change PostgreSQL schema, PostgreSQL event retention, replay-gap behavior, or the SFS-734 recovery ledger. No new dependency is required; the repository resolves redb 4.3.0 and its table API supports bounded first-row removal.

## Interfaces and Dependencies

Use the installed `redb` 4.3.0 dependency. Keep the prune operation internal to `studiofs-meta`: `PgMetaEngine` supplies its durable local cursor, and `RedbMetaEngine` performs the event-table transaction. Extend the existing `LocalStorageDiagnostics` value and its VFS runtime-stat adapter for reporting. Do not add a public API or a second journal.

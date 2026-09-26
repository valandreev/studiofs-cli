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
- [x] (2026-09-27) Resolve the second inline review: preserve foreign-apply authority through pruning, reset the cursor after journal replacement, skip busy writers, verify restart replay, and correct diagnostics/documentation.
- [x] (2026-09-27) Reject legacy positive-floor databases and local snapshots that lack proof the dentry authority summary covered the removed prefix; run focused regressions and metadata/VFS validation gates.
- [x] (2026-09-27) Address the next review: distinguish retained from pruned `Created` state, use targeted summary lookups, expose busy skips and summary size, make replacement cursor reset infallible, isolate remote export from the local summary, correct metrics/docs, and cover malformed snapshots.

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
- Observation: When a stored watermark is ahead of stale event rows, a bounded prune batch previously wrote the last removed key and regressed that watermark.
  Evidence: `local_event_pruning_never_moves_persisted_floor_backwards` failed with stored `50` becoming `10` before the monotonic update and passes afterward.
- Observation: Foreign serving rename derives local unlink authority by reducing the local event log. Removing its acknowledged prefix without retaining the reducer state can recreate a locally unlinked destination.
  Evidence: `replica_serving_rename_does_not_resurrect_locally_unlinked_destination` fails after the prefix is pruned and the foreign rename is applied.
- Observation: A full serving-checkpoint replacement restarts the local event sequence allocator, but the live `flushed_event_seq` previously retained its old value.
  Evidence: `replacing_local_state_resets_the_in_memory_prune_cursor` failed before the cursor reset because the new unflushed event at sequence 1 was removed.
- Observation: Databases pruned by an earlier build of this branch have irrecoverably lost any namespace events in that prefix, so creating an empty summary during upgrade would silently authorize a foreign rename to restore an unlinked name.
  Evidence: `foreign_rename_rejects_legacy_pruned_log_without_dentry_baseline`, `local_event_pruning_rejects_legacy_floor_without_dentry_baseline`, and `local_snapshot_rejects_legacy_pruned_log_without_dentry_baseline` all failed before the coverage guard and pass after it.
- Observation: The summary must distinguish a `Created` value folded from pruned acknowledged history from a `Created` event in the retained suffix. Only the latter can be canceled by a following retained `RemoveEntry`; otherwise an unflushed unlink can be lost after its earlier create was pruned.
  Evidence: `replica_serving_rename_preserves_unflushed_unlink_after_create_was_pruned` reproduced a foreign rename recreating the unlinked name before the retained reducer path was fixed.
- Observation: Foreign rename reconciliation needs one destination membership lookup, not a materialized copy of the complete summary table. The summary has no safe incremental reclamation cutoff and may grow with distinct local authority changes.
  Evidence: The reconciliation call is inside the redb write transaction; `Table::get` and `Table::len` support key lookup and row-count diagnostics without iterating every summary row.
- Observation: Busy-writer admission skips maintenance; that is not a successful empty prune. A skip must leave last-batch row/byte gauges absent and remain observable as a cumulative counter.
  Evidence: `local_event_pruning_skips_busy_writer_with_eligible_events` checks the unchanged event log, omitted last-row value, and incremented skip count.
- Observation: Remote checkpoint export strips local dentry summaries from the artifact, so corrupt local summary values must not prevent export of remote-serving state. Local imports and local reconciliation still validate their keys and enum values.
  Evidence: `remote_snapshot_export_ignores_invalid_local_dentry_mutation_rows` and `local_snapshot_rejects_invalid_dentry_mutation_rows` exercise the two boundaries.

## Decision Log

- Decision: Keep the exclusive `flushed_event_seq` as the eligibility boundary and persist the inclusive `local_event_log_pruned_through_seq` in the same redb transaction as each deletion batch. On open, derive the effective replay cursor as at least one past that prune floor. Rationale: the floor makes committed deletion progress restart-safe if local cursor state is restored from a snapshot that retains the event journal. Date/Author: 2026-09-26, Codex.
- Decision: Delete at most the configured row limit from sorted keys below the durable cursor, even when stored keys have gaps; preserve sequence keys and `nextEventSeq`. Keep the prune floor when an import preserves local event history, and clear it when an import replaces that history. Rationale: the durable cursor is the safety proof for every sequence below it, while full journal replacement starts a new local event-log state. Date/Author: 2026-09-26, Codex.
- Decision: Run local pruning from the existing background metadata poll before remote synchronization and keep it outside the PostgreSQL global janitor-lock path. Rationale: local reclamation needs only local durable state and must still proceed when remote retention is contended or unavailable. Date/Author: 2026-09-26, Codex.
- Decision: Report cumulative events pruned and the most recent batch's range, encoded-payload byte estimate, and duration through existing local metadata diagnostics. Rationale: these values are cheap to collect while rows are removed and fit the existing metadata-redb runtime-stat surface. Date/Author: 2026-09-26, Codex.
- Decision: Retain only `local_position_lock` across the prune transaction, not `pending_state`. Rationale: cursor and snapshot transitions use the position lock, while queue-only edits do not change the durable prune boundary; cursor-advancing transitions still take the position lock. Avoiding the second lock shortens contention for replication work. Date/Author: 2026-09-27, Codex.
- Decision: Let each poll drain at most 16 full 256-row transactions and check shutdown between transactions. Rationale: one batch per poll can leave a sustained eligible backlog; a finite budget raises cleanup throughput while keeping every redb transaction and each poll's total prune work bounded. The batch size and budget remain internal because no workload evidence calls for runtime configuration. Date/Author: 2026-09-27, Codex.
- Decision: Preserve the last non-empty successful batch range through idle or failed passes, and report no prune watermark until the stored inclusive sequence is greater than zero. Rationale: an empty first pass is not a removed range, and sequence zero means no event was pruned. Date/Author: 2026-09-27, Codex.
- Decision: Keep `exclusive_floor` as a hard deletion ceiling and return an empty batch when the persisted prune watermark already covers it. Rationale: the batch helper must not widen its caller's authorization boundary; supported snapshot import paths preserve the event log and local watermark together or replace both. Date/Author: 2026-09-27, Codex.
- Decision: Reuse the current first event sequence through the prune loop and keep the 256-row batch limit internal. Rationale: carrying the peeked key removes duplicate B-tree reads without adding configuration or changing bounded work. Date/Author: 2026-09-27, Codex.
- Decision: Treat the persisted inclusive prune floor as an acknowledged-through frontier, including sequence gaps that never had a row, and keep it monotonic. Rationale: the durable exclusive cursor already authorizes every sequence below it; setting the floor only to the last physically removed row would lose progress over empty or sparse prefixes. Date/Author: 2026-09-27, Codex.
- Decision: Skip a background prune batch when the redb writer is busy. Rationale: mounting writes and durability barriers should not queue behind local maintenance when the writer is already occupied. Date/Author: 2026-09-27, Codex.
- Decision: Fold pruned namespace events into a durable per-dentry `Created`/`Removed` summary in the same transaction as deletion; let foreign serving reconciliation start its reducer from that summary. Rationale: foreign rename must retain acknowledged local unlink authority even after event rows are removed. `Created` is needed so a later `RemoveEntry` cancels a locally created name instead of hiding an unrelated peer rename. Date/Author: 2026-09-27, Codex.
- Decision: Record a versioned coverage counter with the first summary-backed prune and preserve or clear it together with the local journal. Reject positive-floor state without that counter before foreign reconciliation, further pruning, or local snapshot import. Rationale: a missing table or empty summary alone cannot distinguish a valid namespace-free prefix from history already deleted by an earlier build. Date/Author: 2026-09-27, Codex.
- Decision: During foreign rename replay, only a `Created` state produced in the retained event suffix can cancel a later `RemoveEntry`. Load baseline rows lazily for keys touched by retained events and inspect only the destination key for rename authority. Rationale: baseline state represents acknowledged history and must not cancel a newer unflushed unlink; scanning the summary table under the writer lock would make rename cost grow with all historical names. Date/Author: 2026-09-27, Codex.
- Decision: Expose the summary table's row count and document that it can grow without a safe incremental reclaim cutoff; full serving-checkpoint replacement clears it with the local journal. Rationale: pruning must not discard local unlink authority without a proven remote-authority cutoff, while operators need visibility into the retained summary's size. Date/Author: 2026-09-27, Codex.
- Decision: Count busy-writer prune skips separately, omit last-row/byte gauges for skipped attempts, and let the next background poll retry. Rationale: a maintenance skip is neither an error nor a successful no-op and should not endanger or block mounted foreground writes. Date/Author: 2026-09-27, Codex.
- Decision: Make the post-replacement in-memory event cursor reset infallible after the destructive local import commits. Rationale: remote replacement clears local event sequence state, so the correct cursor is zero and a poisoned progress mutex can be recovered without another storage read or a post-commit failure. Date/Author: 2026-09-27, Codex.
- Decision: Include local dentry summary rows only in local snapshot export; remote checkpoint export skips reading this local-only table entirely. Keep key/value validation on local import and reconciliation paths. Rationale: remote export neither needs the summary nor should it fail or pay to scan an unbounded local-only table. Date/Author: 2026-09-27, Codex.
- Decision: Report the prune watermark only when the best-effort read succeeds and its stored value is greater than zero. Rationale: zero is the no-prune state, and an omitted gauge can also mean diagnostics were unavailable. Date/Author: 2026-09-27, Codex.

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

The subsequent review follow-up added a read-side no-op check for already-pruned cursors, enforced the caller's exclusive floor in the batch helper, corrected the payload-byte metric unit, and reused the B-tree peek through each batch. Snapshot import code confirms that preserving imports keep the event log and local prune watermark together; replacement imports replace the log and clear local PG state. The caller's upper bound is still enforced independently of that invariant.

Validation passed:

- `cargo test -p studiofs-meta --lib local_event_pruning` — 12 passed, including regressions for the persisted-floor no-op and strict caller floor.
- `cargo test -p studiofs-vfs --lib runtime_stats_surface_reports_overlay_batch_metrics` — 1 passed; the cumulative payload-byte metric reports `bytes`.
- `cargo check -p studiofs-meta -p studiofs-vfs` — passed.
- `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

The second inline review added an atomic dentry authority summary for pruned events, a coverage guard for pre-summary pruned state, a nonblocking redb writer admission check, a cursor reset after full journal replacement, and direct restart/reconciliation regressions. The inclusive floor remains an acknowledged-through frontier: it may cross sequence gaps with no stored rows, and it cannot move backwards.

Validation passed:

- `cargo test -p studiofs-meta --lib prun` — 28 passed, 4 PostgreSQL-gated ignored.
- `cargo test -p studiofs-meta --lib replica_serving_rename` — 7 passed, including rename after prune and redb reopen and local create-then-remove across one-row prune batches.
- `cargo test -p studiofs-meta --lib remote_snapshot_modes_do_not_replace_local_dentry_baseline` — 1 passed.
- `cargo test -p studiofs-vfs --lib runtime_stats_surface_reports_local_event_prune_metrics` — 1 passed.
- `cargo test -p studiofs-vfs --lib local_event_prune_runtime_stats_use_documented_units` — 1 passed.
- `cargo check -p studiofs-meta -p studiofs-vfs`, `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` — passed.

Already-pruned local history from a build without the summary cannot be reconstructed; the coverage guard returns a recovery error instead of silently losing local unlink authority. PostgreSQL integration and mounted FUSE/WinFSP runs were not executed for this local-storage review.

The next review follow-up keeps acknowledged `Created` baseline state from canceling a newer retained unlink. Foreign rename reads only the destination baseline row and touched retained-event keys, rather than materializing the summary table. Busy writer skips increment a dedicated counter and omit the last-batch gauges; the next poll retries. Replacement imports reset the live event cursor to zero without fallible post-commit I/O. Remote checkpoint export omits the local summary table entirely, while local snapshot import continues to reject malformed keys and values. The row-count gauge makes summary growth observable; no safe incremental reclaim cutoff is known, so the summary remains unbounded until a full serving-checkpoint replacement clears local state.

Validation passed:

- `cargo test -p studiofs-meta --lib local_event_pruning` — 17 passed.
- `cargo test -p studiofs-meta --lib replica_serving_rename` — 8 passed.
- `cargo test -p studiofs-meta --lib local_snapshot_rejects_invalid_dentry_mutation_rows` — 1 passed.
- `cargo test -p studiofs-meta --lib remote_snapshot_export_ignores_invalid_local_dentry_mutation_rows` — 1 passed.
- `cargo test -p studiofs-meta --lib replacing_local_state` — 2 passed.
- `cargo test -p studiofs-vfs --lib local_event_prune_runtime_stats_use_documented_units` — 1 passed.
- `cargo test -p studiofs-vfs --lib runtime_stats_surface_reports_local_event_prune_metrics` — 1 passed.

The legacy-history coverage error applies only to local databases written by earlier unreleased SFS-735 builds that pruned rows before recording the coverage marker. Pruning and foreign rename apply then fail closed; local state must be replaced from a serving checkpoint to recover. PostgreSQL integration and mounted FUSE/WinFSP runs remain unexecuted for this local redb review.

## Context and Orientation

`RedbMetaEngine` owns `EVENT_LOG_TABLE` and allocates monotonically increasing sequence keys. `PgMetaEngine` owns the durable `LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER`, which records the first sequence not yet acknowledged as complete. The local repair state added by SFS-734 lives outside the event journal, so acknowledged journal history is no longer needed for that recovery path.

The foreign serving-rename path also reduces local namespace events to decide whether a destination remains locally removed. Its durable `Created`/`Removed` summary must cover the pruned prefix before replaying retained events.

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

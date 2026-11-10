# Fix the CLI Janitor audit findings before full testing

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log` and `Outcomes & Retrospective` current. It follows `.agent/PLANS.md`.

## Purpose / Big Picture

`studiofs janitor <volume>` deletes payload objects in S3-compatible storage that no metadata references. An audit of the Linear project "[0.2.2] New Janitor" found gaps that would distort or block the planned full test pass. After this change an operator can run the janitor while other clients write to the volume, a long orphan scan survives a mid-cycle error without starting over, a database that lacks the janitor tables reports that fact instead of "check connectivity", the garbage-collection queue table stops growing forever, and several options that never did anything are gone.

## Progress

- [x] M1 Remove dead surface: prefix-trie listing, `--pg-concurrency`, `--s3-concurrency`, `--reset-scan`, always-null progress fields.
- [x] M2 Persist the scan checkpoint at every committed batch.
- [x] M3 Accept a local replica that is behind PostgreSQL; reject only one that is ahead.
- [x] M4 Keep SQLSTATE in janitor PostgreSQL errors, make schema errors terminal, require the two janitor tables at engine start.
- [x] M5 Prune acknowledged queue rows; run the legacy backfill once per cycle.
- [x] M6 Exit labels, command description and user documentation.
- [x] Validation: focused tests, clippy, fmt, `git diff --check`.

## Surprises & Discoveries

- Removing the in-flight limit also removed the only reason for the first two parts of the transport unit test; it now covers request pacing only and is named `s3_transport_paces_request_starts`.
- Backfilling once per cycle with the old 64-row page limit would drain a legacy backlog at 64 rows per cycle, so the first page now enqueues up to 4096 rows.
- `pg_janitor_sweep_checkpoint_restart_scope_dry_run_and_version` failed once in a parallel run inside the shared fixture `setup_pg_volume` (`crates/studiofs-meta/src/pg_engine/tests/mod.rs:313`), before any janitor code ran; it passed alone and in a serial run of the group.

## Decision Log

- Decision: relax the version gate to `local_version <= remote_head` instead of declaring the janitor an offline-only tool.
  Rationale: the deletion claim in `crates/studiofs-meta/src/pg_engine/janitor.rs` (`claim_object_deletion_in`) already takes `FOR SHARE` on `metadata_state` and rechecks current references and snapshot pins in PostgreSQL, which is the authority. A local replica that is ahead of PostgreSQL still fails closed, because that means PostgreSQL lost history (for example a restore from backup). The existing checks that the local replica has nothing unpublished stay.
  Date/Author: 2026-11-10, Claude.
- Decision: delete the hexadecimal prefix-trie listing instead of keeping it as a fallback.
  Rationale: the only constructed store is OpenDAL's S3 service, whose `list_with_start_after` capability is always true, so the fallback is unreachable. A provider that ignores the start-after parameter is already rejected by the lexicographic order check in `run_start_after_listing`.
  Date/Author: 2026-11-10, Claude.
- Decision: keep the `next_prefix` and `listing_strategy` columns in `snapshot_payload_sweep_checkpoints` and write constants to them.
  Rationale: schema versioning is the separate open issue SFS-980; dropping columns here would create an unversioned schema change.
  Date/Author: 2026-11-10, Claude.

## Outcomes & Retrospective

All milestones are implemented on branch `fix/janitor-audit-followups`, uncommitted. Evidence on 2026-11-10: `cargo test -p studiofs --bin studiofs cli::janitor` 54 passed, 9 ignored; `cargo test -p studiofs-meta --lib janitor -- --ignored --test-threads=1` 9 passed against the local test PostgreSQL; `cargo clippy -p studiofs -p studiofs-meta -p studiofs-data --all-targets -- -D warnings` clean; `git diff --check` clean. Not run: the live CLI tests that need S3 or Docker. Not changed: the per-cycle scan of local references into memory, and the behaviour of a peer whose upload registration lags behind the grace period; both need a design decision or a mounted end-to-end test.

## Context and Orientation

The janitor has four layers. `crates/studiofs/src/cli/janitor.rs` parses arguments, prints progress and maps errors to exit codes. `crates/studiofs/src/cli/janitor/pipeline.rs` runs one cycle: first a queue of known deletions read from PostgreSQL, then a streamed S3 listing that discovers orphans. `crates/studiofs-data/src/janitor.rs` wraps OpenDAL for LIST, HEAD and DELETE with a request-rate gate. `crates/studiofs-meta/src/janitor.rs` and `crates/studiofs-meta/src/pg_engine/janitor.rs` hold the metadata session: a per-volume PostgreSQL advisory lock, eligibility checks, and the per-object deletion claim.

A "checkpoint" is the saved position of the S3 listing (`JanitorSweepCheckpoint`), stored in the PostgreSQL table `snapshot_payload_sweep_checkpoints`. A "fence" is a row in `snapshot_payload_upload_objects` with state `delete_pending`; it stops a late upload from reviving a deleted object ID. The "queue" is the table `snapshot_payload_gc_candidates`. `metadata_state.version` is the PostgreSQL head version; it grows by one for every published event batch. The local replica's version is the redb counter `LOCAL_PG_VERSION_COUNTER`.

## Plan of Work

M1 removes code. In `pipeline.rs` delete `run_prefix_trie_listing`, `prefix_for_hex_cursor`, `next_hex_prefix` and `JanitorObjectStore::supports_start_after`; `run_one_cycle_inner` always calls `run_start_after_listing`. In `crates/studiofs-data/src/janitor.rs` drop `supports_start_after` and the `max_in_flight` parameter. In `crates/studiofs/src/cli/janitor.rs` drop the `--pg-concurrency`, `--s3-concurrency` and `--reset-scan` arguments, the `reset_sweep_checkpoint` trait method, and the `queue_depth`, `s3_in_flight`, `pg_in_flight`, `retryable_queue_depth` output fields. Remove `reset_sweep_checkpoint` from both metadata layers. Tests that exercised the removed paths are deleted with them.

M2 moves checkpoint persistence into `run_start_after_listing`: wherever the in-memory `committed` position advances, the same value is saved through `save_sweep_checkpoint` when persistence is on. The final save in `run_one_cycle_inner` stays.

M3 changes the comparison in `PgJanitorSession::read_protection` so only `local_version > remote_head` is unsafe.

M4 makes `janitor_pg_error` keep the five-character SQLSTATE code (never the message, which could carry connection details), adds a `SchemaUnavailable` variant to `JanitorMetadataError` for SQLSTATE class 42, classifies it terminal in `pipeline.rs::classify_error`, and adds the two janitor tables to the required-table list in `crates/studiofs-meta/src/pg_engine.rs`.

M5 extends `prune_object_deletion_fences` to delete acknowledged queue rows whose fence row no longer exists, and makes `gc_candidate_page` run its backfill statements only for the first page of a cycle (cursor absent).

M6 fixes the exit label for non-backend errors, the command description, and `docs/configuration.md`, `docs/examples.md`.

## Concrete Steps

From the repository root `E:\dev\studiofs-cli`:

    cargo fmt --all
    cargo check -p studiofs-data -p studiofs-meta -p studiofs
    cargo test -p studiofs --bin studiofs cli::janitor
    cargo test -p studiofs-data janitor
    cargo clippy -p studiofs -p studiofs-meta -p studiofs-data --all-targets -- -D warnings
    git diff --check

## Validation and Acceptance

The focused janitor unit tests pass with a nonzero test count. A new unit test fails a listing after one committed batch and asserts the saved checkpoint is past the start. PostgreSQL-backed tests in `crates/studiofs-meta/src/pg_engine/tests/janitor.rs` and the live CLI tests are `#[ignore]` and need `STUDIOFS_TEST_PG_DSN` or Docker; report them as environment-gated unless run.

## Idempotence and Recovery

All edits are ordinary source changes on branch `fix/janitor-audit-followups`; rerunning the commands is safe. No schema column is dropped.

## Interfaces and Dependencies

No new dependencies. `JanitorMetadataError` gains `SchemaUnavailable(String)`. `JanitorObjectStore::from_s3` loses its `max_in_flight` parameter. The CLI loses `--pg-concurrency`, `--s3-concurrency`, `--reset-scan`.

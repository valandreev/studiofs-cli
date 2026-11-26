# Publish compatible serving checkpoint formats at the same metadata cut

This ExecPlan follows `.agent/PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.

## Purpose / Big Picture

A 0.2.5 client and its successor can both discover a PostgreSQL volume and catch up when they support the same events but use different checkpoint encodings. A checkpoint contains the remote serving state at one event-log version, called its cut. A reader imports that state and replays events after the cut to the remote head.

## Progress

- [x] (2026-11-26) Traced bootstrap, checkpoint publication, snapshot capture, and prune coordination.
- [x] (2026-11-26) Registered format 5 and kept the format 1–4 readers and the format 4 writer.
- [x] (2026-11-26) Added additive storage, transactional publication, compatible selection, bounded artifact retention, and typed errors.
- [x] (2026-11-26) Passed focused Windows checks and PostgreSQL and RustFS integration.

## Surprises & Discoveries

The shipped baseline reader reads `metadata_state.snapshot` directly, and the shipped pruner uses the version in the same row. Replacing that payload with a newer encoding would strand baseline clients even if an older artifact survived elsewhere.

Historical snapshot capture stamped format 4 even when the copied, untagged bytes belonged to an older format. Snapshot reconstruction keeps that compatibility. New alternate publications require an exact match between the stored descriptor and the header.

## Decision Log

Keep the baseline checkpoint in place and publish the successor encoding beside it in the fenced transaction. It is built from the very same redb read transaction. The serving-state schema stays at 1. A baseline writer can later advance its checkpoint and prune past the successor artifact because successor readers still support the baseline encoding.

An alternate row references its `metadata_state` parent. The fenced publication already locks that row and does not take a new volume row lock after it. Snapshot capture locks the volume before metadata state. A repeat publication at the same alternate identity compares the existing bytes and digest and keeps the original row.

If an alternate checkpoint fails byte, checksum, or envelope validation, retry with a fresh baseline-only read that checks the head and floor again. Shared volume identity and format information must pass validation before import. A client that selected a usable checkpoint just before pruning can reselect through the existing replay recovery path.

## Outcomes & Retrospective

The branch publishes two compatible encodings at the same cut. PostgreSQL tests verified the pruning and replay recovery paths, retention of snapshot sources, and live migration and schema repair. A default Windows CLI smoke also imported, exported, rediscovered and read volume payload against Docker PostgreSQL and RustFS.

## Context and Orientation

`crates/studiofs-meta/src/snapshot.rs` owns checkpoint bytes. The baseline uses a postcard payload with optional fields appended in formats 2 through 4. `pg_engine/checkpoint_coordinator.rs` captures a stable remote-visible redb view and publishes it after checking a fenced PostgreSQL lease. `pg_engine/metadata_io.rs` reads the bootstrap record. `pg_engine/event_log_pruning.rs` and `pg_engine/metadata_sync.rs` serialize pruning with page reads by means of a PostgreSQL advisory lock.

## Plan of Work

Define the envelope and decoder dispatch in `snapshot.rs`. Add a PostgreSQL migration for immutable successor artifacts with format, schema, cut, bytes, and checksum. Publish both encodings together and only reclaim a superseded artifact after a replacement of the same encoding exists. Select a published checkpoint at or above the retained replay floor in one statement. Keep the baseline checkpoint as the pruning anchor and continue to validate every replay page against the current floor.

## Concrete Steps

Run commands from the repository root. Run `cargo test -p studiofs-meta --lib serving_checkpoint` and repeat with `--features test-successor` before `--lib`. Run `cargo test -p studiofs-meta --features test-successor --lib pg_engine::migrations::tests`, then run the filters `compat_checkpoint_reader_mismatch` and `declared_changes_match_compiled_identities` with the same flags.

Set `STUDIOFS_TEST_PG_DSN` to the dedicated Docker PostgreSQL endpoint. The integration test creates an isolated schema that its guard cleans up. Run `cargo test -p studiofs-meta --features test-successor --lib pg_checkpoint_encodings_keep_baseline_and_successor_replayable -- --ignored`. Use the same flags with `migration_registry_applies_resumes_and_refuses_drift_on_live_postgres` to check additive migration and schema repair and `captured_checkpoint_bytes_survive_serving_checkpoint_replacement` to check retained sources. Run the default-build `pg_snapshot_backend_reads_a_validated_bootstrap_descriptor` with `-- --ignored`.

Run `cargo clippy -p studiofs-meta --all-targets -- -D warnings` in the default build and repeat with `--features test-successor` before `--`. Run `cargo fmt --all -- --check` and `git diff --check`.

For the CLI smoke, point the test at an isolated PostgreSQL schema and RustFS test bucket and run `cargo test -p studiofs --test dump_load_test dump_load_restores_volume_and_discovery_reads_data -- --exact --ignored`.

## Validation and Acceptance

Baseline fixtures decode with the new client, a tagged successor checkpoint fails closed in the baseline decoder, and the successor verifies its descriptor and bytes. The integration test compares both copies, clears and repairs the bootstrap format information at the same cut, and verifies that an invalid alternate falls back to the baseline. It advances the baseline through the same fenced SQL used by a baseline writer and prunes via the product maintenance path. Fresh clients, a returning client and a client delayed between reading its descriptor and its first replay page must converge to the correct head. After a later successor promotion only its newest alternate remains. A cut below the durable replay floor is rejected with a typed reason.

## Idempotence and Recovery

The additive migration and fenced publication are transactional. Failed publication rolls back both copies. Superseded successor artifacts have no effects on existing readers. The existing baseline slot can serve a client after disabling the test successor.

## Artifacts and Notes

On Windows, the default checkpoint group passed 12 tests and the successor group passed 14, with two ignored in each group. The seven successor migration unit tests, compatibility mismatch test, and successor identity manifest test passed. The dual-encoding, live schema migration, historical snapshot source, and default bootstrap descriptor tests each passed against Docker PostgreSQL. The default CLI dump/load/discovery payload smoke passed against an isolated PostgreSQL schema and RustFS bucket. Both metadata Clippy variants, formatting, and `git diff --check` passed.

## Interfaces and Dependencies

`ServingCheckpoint` remains the decoded model. Its persisted format version and schema identity come from `version.rs`. PostgreSQL artifacts carry a cut and a content checksum. Bootstrap reports `NeedCompatibleCheckpoint` with the current floor when the retained range cannot be reached from an eligible cut.

Revision note (2026-11-26). Added the publication, recovery, and schema repair decisions and their focused validation commands.

Revision note (2026-11-26, code review). Replaced positional checkpoint encoding tuples with `CheckpointEncoding`, formalized the tagged decoder registry in `snapshot.rs`, aligned `SnapshotSourceReader::reconstruct` to report typed `unsupported_checkpoint_encoding`, restored the stale-checkpoint skip without downloading payloads, added sibling-alternate fallback and error precedence in bootstrap selection, handled `NeedCompatibleCheckpoint` on returning-client replay, removed panicking `Row::get` in successor publication, registered `POSTGRES_CHECKPOINT_ENCODINGS_SCHEMA` in `version.rs`, and revalidated unit, Clippy, and live Docker PostgreSQL tests.

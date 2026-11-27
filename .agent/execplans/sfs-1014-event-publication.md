# Fence metadata publication across policy changes

## Purpose

An open mount can start publishing under one volume policy while an administrator activates another. PostgreSQL must reject the stale write without committing a new metadata version, payload accounting, or part of the batch. Existing format 5 and 6 volumes without a policy must keep their legacy write path in the same database.

## Progress

- [x] Reproduced both failures on the configured PostgreSQL service. The unstamped insert succeeded and the publisher waiting on activation advanced the head.
- [x] Added the epoch predicate, additive schema migration, and statement trigger. Updated direct SQL fixtures to use the existing publication session.
- [x] Verified both fencing regressions after implementation.
- [x] Verify the focused migration, publishing and replay checks, formatting, lint and compile checks.
- [x] Verify that load survives a lease expiry between event inserts and that mount startup refuses damaged fence triggers until schema repair.

## Decision Log

The publication epoch lives on `metadata_state`, the row PostgreSQL already locks and rechecks during a version bump. Reading the epoch from a separate policy row in the predicate could retain the statement's old snapshot after a lock wait. The migration initializes the mirror for existing policies and keeps it synchronized for initial provisioning, policy updates, and restored metadata state. Existing dependency versions and checksums stay intact.

The event trigger runs once per insert statement and checks each affected volume. It uses the existing transaction-local session ID, policy epoch, and fencing token to verify a live lease. Direct insert callers serialize with policy changes on the same metadata row. An operator with sufficient SQL access can forge the context. Backend implementations must meet the same atomic publication contract.

Load renews its short-lived session before each event insert in the restore transaction. The session row is visible to that transaction before commit. Renewal uses the existing ownership and fencing-token check and stamps the validated policy epoch and renewed token.

Mount startup checks the event publication fence migration and its epoch column. It checks that each trigger is attached to its expected table and fires for normal writes. The existing repair migration recreates disabled or missing triggers.

## Context and implementation

In `crates/studiofs-meta/src/pg_engine/metadata_io.rs`, the mount stamps and validates a session, verifies the event encodings, then calls `BUMP_METADATA_VERSION` and inserts the event rows in the same transaction. `volume_settings.rs` uses the same SQL and session checks. `migrations.rs` registers the additive `event_publication_fence` schema from `schema.rs`. `volume_protocol.rs` returns a typed epoch conflict when a compatible version bump loses the race.

## Validation

From the repository root, run `cargo fmt --all --check`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo check -p studiofs -p studiofs-e2e --all-targets`, and `git diff --check`. Run `cargo test -p studiofs-meta --lib event_publication_fence -- --ignored` and the focused migration and format-adoption cases. These live tests use the configured PostgreSQL service and disposable schemas. The race waits for PostgreSQL to report that the publisher is blocked by activation, then checks both rejection and unchanged durable state.

For the review follow-up, run `cargo clippy -p studiofs-meta -p studiofs --all-targets -- -D warnings`, `cargo test -p studiofs-meta --lib startup_schema -- --ignored`, and `cargo test -p studiofs --bin studiofs dump_load_roundtrip_restores_the_published_baseline -- --ignored`. The load test expires the lease with a test-only PostgreSQL trigger between two event inserts. The startup test damages each fence trigger in turn and checks that schema repair makes mount startup available again.

## Outcomes

The two fencing regressions passed on live PostgreSQL. The migration test verified that an existing policy gains its epoch and a legacy volume retains epoch zero. The focused tests also covered format 5 adoption, unrepresentable pending events, replay, pruning, snapshot capture and startup schema validation. Replay and pruning were rerun in a disposable schema because the shared test schema already had a newer core version. Formatting, Clippy and the CLI and E2E compile checks passed. Mounted scenarios and S3-backed integration tests were outside this validation.

For the review follow-up, both new regressions failed before the fix and passed after it. The startup regression verified refusal and repair for a disabled event trigger, a replica-only policy synchronization trigger, and a dropped epoch initialization trigger. The live `startup_schema` and `event_publication_fence` filters each passed three tests, including the same repair regression. The non-ignored startup filter passed five tests. The PostgreSQL dump/load round trip passed with forced lease expiry. Formatting, strict Clippy for both crates, and the diff check passed. The S3-backed `cargo test -p studiofs --test dump_load_test dump_load_restores_volume_and_discovery_reads_data -- --ignored` ran one test and failed in `init` before `load` on both attempts because HTTP requests to the configured S3 endpoint failed.

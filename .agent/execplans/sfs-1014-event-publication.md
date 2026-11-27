# Fence metadata publication across policy changes

## Purpose

An open mount can start publishing under one volume policy while an administrator activates another. PostgreSQL must reject the stale write without committing a new metadata version, payload accounting, or part of the batch. Existing format 5 and 6 volumes without a policy must keep their legacy write path in the same database.

## Progress

- [x] Reproduced both failures on the configured PostgreSQL service. The unstamped insert succeeded and the publisher waiting on activation advanced the head.
- [x] Added the epoch predicate, additive schema migration, and statement trigger. Updated direct SQL fixtures to use the existing publication session.
- [x] Verified both fencing regressions after implementation.
- [x] Verify the focused migration, publishing and replay checks, formatting, lint and compile checks.

## Decision Log

The publication epoch lives on `metadata_state`, the row PostgreSQL already locks and rechecks during a version bump. Reading the epoch from a separate policy row in the predicate could retain the statement's old snapshot after a lock wait. The migration initializes the mirror for existing policies and keeps it synchronized for initial provisioning, policy updates, and restored metadata state. Existing dependency versions and checksums stay intact.

The event trigger runs once per insert statement and checks each affected volume. It uses the existing transaction-local session ID, policy epoch, and fencing token to verify a live lease. Direct insert callers serialize with policy changes on the same metadata row. An operator with sufficient SQL access can forge the context. Backend implementations must meet the same atomic publication contract.

## Context and implementation

In `crates/studiofs-meta/src/pg_engine/metadata_io.rs`, the mount stamps and validates a session, verifies the event encodings, then calls `BUMP_METADATA_VERSION` and inserts the event rows in the same transaction. `volume_settings.rs` uses the same SQL and session checks. `migrations.rs` registers the additive `event_publication_fence` schema from `schema.rs`. `volume_protocol.rs` returns a typed epoch conflict when a compatible version bump loses the race.

## Validation

From the repository root, run `cargo fmt --all --check`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo check -p studiofs -p studiofs-e2e --all-targets`, and `git diff --check`. Run `cargo test -p studiofs-meta --lib event_publication_fence -- --ignored` and the focused migration and format-adoption cases. These live tests use the configured PostgreSQL service and disposable schemas. The race waits for PostgreSQL to report that the publisher is blocked by activation, then checks both rejection and unchanged durable state.

## Outcomes

The two fencing regressions passed on live PostgreSQL. The migration test verified that an existing policy gains its epoch and a legacy volume retains epoch zero. The focused tests also covered format 5 adoption, unrepresentable pending events, replay, pruning, snapshot capture and startup schema validation. Replay and pruning were rerun in a disposable schema because the shared test schema already had a newer core version. Formatting, Clippy and the CLI and E2E compile checks passed. Mounted scenarios and S3-backed integration tests were outside this validation.

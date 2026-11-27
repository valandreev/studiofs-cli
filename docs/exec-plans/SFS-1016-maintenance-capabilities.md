# Fence destructive PostgreSQL maintenance

This living ExecPlan for SFS-1016 follows .agent/PLANS.md.

## Purpose / Big Picture

Clients lacking a newly required maintenance capability must stop deleting shared state while compatible reads and writes continue.

## Progress

- [x] (2026-11-27 11:22Z) Fetched main and traced payload claims, event pruning and snapshot reclamation.
- [x] (2026-11-27) Reproduced a stale payload claim on PostgreSQL after activating a successor-only maintenance capability.
- [x] (2026-11-27) Added transaction-bound checks for payload, snapshot and replay maintenance.
- [x] (2026-11-27) Verified PostgreSQL policy fencing, replay retention and schema migration repair, plus CLI refusal and recovery on RustFS.

## Surprises & Discoveries

The format-only deletion trigger accepted a stale client's claim after the policy epoch advanced. The RED test failed with `stale maintenance claim succeeded`. The configured RustFS proxy refused the first connection. A process-local proxy exception for the configured host reached the test service.

## Decision Log

Reuse the metadata state row and transaction-local settings to order maintenance against activation, following publication. On a lock wait the epoch on the current metadata row can be newer than the joined policy snapshot. The worker rereads the policy after acquiring the row lock in that case. The server checks each volume in a batch against the epoch, declared maintenance capabilities and payload GC component version.

Changing the required maintenance set requires the existing Janitor lock and settled snapshot or payload deletion claims. An expired claim can be recovered by a compatible worker. Expiry alone cannot prove that an admitted S3 request drained.

## Outcomes & Retrospective

The PostgreSQL regression refuses a worker that waited behind policy activation and a claim stamped with an older epoch. The snapshot and replay regression verifies preserved snapshot sources and replay rows. The CLI refused maintenance without changing its sweep cursor or the RustFS object. Compatible CLI scenarios verified dry-run and budget handling, protected-object retention, deletion and restart recovery. Mounted filesystem acceptance and throughput measurements were outside this validation.

## Context and Orientation

In crates/studiofs-meta/src/pg_engine, janitor.rs manages payload claims and sweep checkpoints, snapshot_cleanup.rs manages artifact reclamation leases, event_log_pruning.rs prunes replay history, and volume_protocol.rs persists the policy epoch and required capabilities.

## Plan of Work

The additive `maintenance_fence` migration extends the existing deletion claim GUC protocol. PgJanitorSession and mounted GC check policy before a cycle, then claim objects through the existing reachability and lease queries. Snapshot cleanup checks the policy on lease admission, renewal and finalization. Replay and checkpoint pruning keep their existing cutoff rules. Compatible checkpoint publication can retain superseded encodings when the binary lacks maintenance authority.

## Concrete Steps

From the repository root run `cargo test -p studiofs-meta --lib pg_maintenance_ -- --ignored` with `STUDIOFS_TEST_PG_DSN` set. For S3 coverage, load the configured `STUDIOFS_S3_*` environment, run `cargo build -p studiofs --bin studiofs`, then `cargo test -p studiofs --bin studiofs live_cli_maintenance_refusal -- --ignored`. The CLI test compares the object's bytes and the persisted sweep cursor before and after refusal. Run focused compatible Janitor, snapshot and event-prune tests, then `cargo clippy -p studiofs-meta -p studiofs --all-targets -- -D warnings`, `cargo fmt -p studiofs-meta -p studiofs -- --check` and `git diff --check`.

## Validation and Acceptance

The stale claim fails without creating a deletion record. An unsettled cleanup claim prevents activation and leaves the current epoch unchanged. Once the claim drains and the policy activates, current workers without the new capability report `maintenance_capability_missing`. Compatible sessions continue safe cleanup.

## Idempotence and Recovery

Tests use disposable PostgreSQL schemas. Existing leases and checkpoints allow recovery after interruption.

## Artifacts and Notes

PostgreSQL tests use separately created schemas. CLI tests place their fixtures under unique RustFS prefixes and save command output under `target/janitor-live`.

## Interfaces and Dependencies

Reuse ClientCapabilities, VolumeProtocol and payload_gc schema version 2. PostgreSQL orders activation and destructive transactions.

The plan records the RED result, activation ordering and the focused validation commands.

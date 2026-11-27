# Fence destructive PostgreSQL maintenance

This living ExecPlan for SFS-1016 follows .agent/PLANS.md.

## Purpose / Big Picture

Clients lacking a newly required maintenance capability must stop deleting shared state while compatible reads and writes continue.

## Progress

- [x] (2026-11-27 11:22Z) Fetched main and traced payload claims, event pruning and snapshot reclamation.
- [x] (2026-11-27) Reproduced a stale payload claim on PostgreSQL after activating a successor-only maintenance capability.
- [x] (2026-11-27) Added transaction-bound checks for payload, snapshot and replay maintenance.
- [x] (2026-11-27) Verified PostgreSQL policy fencing, replay retention and schema migration repair, plus CLI refusal and recovery on RustFS.
- [x] (2026-11-27) Extended the PostgreSQL fence to cleanup leases, replay pruning, serving checkpoint pruning and replay floor advances. Added live regressions for pre-fence SQL and expired owners.
- [x] (2026-11-27) Passed eight focused PostgreSQL tests, the maintenance compatibility unit test, the RustFS-backed CLI refusal test, and the metadata Clippy and formatting checks on the review fixes.
- [x] (2026-11-27) Reproduced unstamped snapshot finalization after activation, replacement load returning `23001`, and the Janitor fixture's missing checkpoint-encoding relation (`42P01`) on live PostgreSQL.
- [x] (2026-11-27) Passed both live maintenance policy tests and the exact Janitor schema test, with the registered checkpoint-encoding prerequisite installed first.
- [x] (2026-11-27) Passed both live load tests, three replay-prune tests, and the RustFS-backed CLI refusal test using a rebuilt CLI. Both touched crates passed Clippy, and formatting and diff checks passed.

## Surprises & Discoveries

The format-only deletion trigger accepted a stale client's claim after the policy epoch advanced. The RED test failed with `stale maintenance claim succeeded`. The configured RustFS proxy refused the first connection. A process-local proxy exception for the configured host reached the test service.

The review regression failed with `MaintenanceInProgress` when an expired lease still had an owner. The same test now activates the policy with that owner recorded and verifies that unstamped maintenance SQL leaves the lease, replay floor and retained rows intact.

## Decision Log

Reuse the metadata state row and transaction-local settings to order maintenance against activation, following publication. On a lock wait the epoch on the current metadata row can be newer than the joined policy snapshot. The worker rereads the policy after acquiring the row lock in that case. The server checks each volume in a batch against the epoch, declared maintenance capabilities and payload GC component version.

Changing the required maintenance set requires the existing Janitor lock and the absence of live snapshot or payload deletion leases. Expired and abandoned owners permit activation. The epoch fence applies on claim and renewal. A cancelled or timed-out S3 DELETE sent before activation may still reach the object store afterward. Its target was proven unreachable under the old policy.

The existing database fence also checks cleanup lease acquisition and renewal, event log and serving checkpoint deletion, and replay floor advances. Pre-fence workers keep baseline maintenance behavior on these paths until the volume requires additional maintenance capabilities. Foreground operations continue to use their existing compatibility rules.

Fence the `deleting` to `deleted` catalog transition so an unstamped finalization rolls back its earlier source and artifact metadata deletes. Current finalization already stamps the transaction before touching the catalog. Replacement load also takes the existing maintenance fence before clearing destination history. Initial restore into an empty volume retains its publication checks.

Retain the pipelined Janitor admission because the biased join polls `BEGIN` first and the claim waits for both results. Keep NULL reconciliation deadlines reclaimable. Production claims set a database-clock deadline and activation waits for live leases only.

## Outcomes & Retrospective

The PostgreSQL regression refuses a worker that waited behind policy activation and a claim stamped with an older epoch. The review regression verifies refusals for pre-fence lease acquisition and renewal, replay and checkpoint deletes, and replay floor advances. It also exercises local foreground create, lookup and unlink under the active policy. The CLI refused maintenance without changing its sweep cursor or the RustFS object. Initial compatible CLI scenarios verified dry-run and budget handling, protected-object retention, deletion and restart recovery. Mounted filesystem acceptance and throughput measurements were outside this validation.

## Context and Orientation

In crates/studiofs-meta/src/pg_engine, janitor.rs manages payload claims and sweep checkpoints, snapshot_cleanup.rs manages artifact reclamation leases, event_log_pruning.rs prunes replay history, and volume_protocol.rs persists the policy epoch and required capabilities.

## Plan of Work

The additive `maintenance_fence` migration extends the existing deletion claim GUC protocol. PgJanitorSession and mounted GC check policy before a cycle, then claim objects through the existing reachability and lease queries. Snapshot cleanup checks the policy on lease admission, renewal and finalization. Replay and checkpoint pruning keep their existing cutoff rules. Compatible checkpoint publication can retain superseded encodings when the binary lacks maintenance authority.

## Concrete Steps

From the repository root run `cargo test -p studiofs-meta --lib pg_maintenance_ -- --ignored` with `STUDIOFS_TEST_PG_DSN` set. For S3 coverage, load the configured `STUDIOFS_S3_*` environment, run `cargo build -p studiofs --bin studiofs`, then `cargo test -p studiofs --bin studiofs live_cli_maintenance_refusal -- --ignored`. The CLI test compares the object's bytes and the persisted sweep cursor before and after refusal. Run focused compatible Janitor, snapshot and event-prune tests, then `cargo clippy -p studiofs-meta -p studiofs --all-targets -- -D warnings`, `cargo fmt -p studiofs-meta -p studiofs -- --check` and `git diff --check`.

## Validation and Acceptance

The stale claim fails without creating a deletion record. A live cleanup lease prevents activation and leaves the current epoch unchanged. An expired lease with its owner still recorded permits activation. After activation, pre-fence SQL cannot acquire or renew a lease, delete replay or checkpoint rows, or advance the replay floor. Current workers without the new capability report `maintenance_capability_missing`. Compatible sessions continue safe cleanup.

## Idempotence and Recovery

Tests use disposable PostgreSQL schemas. Existing leases and checkpoints allow recovery after interruption.

## Artifacts and Notes

PostgreSQL tests use separately created schemas. CLI tests place their fixtures under unique RustFS prefixes and save command output under `target/janitor-live`.

## Interfaces and Dependencies

Reuse ClientCapabilities, VolumeProtocol and payload_gc schema version 2. PostgreSQL orders activation and destructive transactions.

The review revision records the additional PostgreSQL guards and the live-lease activation rule so operators can distinguish an epoch refusal from an S3 request that outlives its lease.

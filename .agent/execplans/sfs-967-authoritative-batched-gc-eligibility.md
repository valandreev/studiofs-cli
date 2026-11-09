# Add authoritative batched GC eligibility

This ExecPlan is a living document maintained under `.agent/PLANS.md`. It records the implementation of SFS-967 for `studiofs-meta` and the standalone `studiofs janitor` CLI.

## Purpose / Big Picture

The standalone janitor must decide whether each listed object is safe to delete without first loading every reference and pending deletion from local redb and PostgreSQL into large in-memory sets. After this change, the CLI asks metadata about bounded batches of object IDs and receives an explicit protected, eligible, or unknown answer. Unknown answers stop deletion for the affected batch. Creating snapshots are coordinated with the CLI's existing per-volume deletion lock so a snapshot cannot begin retaining an object after eligibility was checked but before deletion.

The mounted background janitor keeps its current behavior. A user can observe the new behavior through focused `studiofs-meta` unit tests, a PostgreSQL concurrency test, and CLI janitor tests. The PostgreSQL test requires the configured local test database and is ignored when that service is unavailable.

## Progress

- [x] (2026-11-09) Recorded the repository root, feature branch, and clean initial worktree; read root and metadata crate contracts and `.agent/PLANS.md`.
- [x] (2026-11-09) Located current standalone protection collection, PostgreSQL protection queries, snapshot registration transaction, and CLI pipeline through Codebase Memory and direct source inspection.
- [x] (2026-11-09) Implemented bounded local and PostgreSQL candidate eligibility checks with fail-closed outcomes.
- [x] (2026-11-09) Fenced snapshot registration against the CLI destructive window and added a focused live PostgreSQL concurrency test.
- [x] (2026-11-09) Updated the CLI pipeline and focused unit/service-backed tests without changing mounted janitor collection.
- [x] (2026-11-09) Finish focused Clippy, formatting, and diff checks; inspect the final diff.

## Surprises & Discoveries

- Observation: The standalone session currently combines all local object references with all PostgreSQL current references and snapshot pins into `HashSet`s, and separately materializes every pending deletion.
  Evidence: `crates/studiofs-meta/src/janitor.rs::collect_protection` calls `get_local_referenced_object_ids`, `PgJanitorSession::read_protection`, and `get_pending_deletions`.
- Observation: The CLI already keeps a per-volume PostgreSQL session advisory lock for its complete cycle, while snapshot registration is a short transaction.
  Evidence: `crates/studiofs-meta/src/pg_engine/janitor.rs::pg_janitor_lock_key` and `crates/studiofs-meta/src/pg_engine/snapshot_control.rs::PgSnapshotBackend::create_snapshot_v2`.
- Observation: The janitor protection gate also rejects any unflushed local state and any pending local file recovery before it starts object work. Those conservative checks remain necessary; only the volume-wide object sets are replaced.

## Decision Log

- Decision: Reuse the existing per-volume advisory lock key. The CLI keeps its existing exclusive session lock for the destructive cycle; snapshot registration takes a shared transaction lock before it captures or inserts a creating snapshot.
  Rationale: The database arbitrates both processes across independent clients, and the existing exclusive session lock already spans eligibility through CLI deletion. This keeps snapshot creation and janitor locking in the shared PostgreSQL metadata layer without adding schema or a new lock registry.
  Date/Author: 2026-11-09 / Codex.
- Decision: Keep an Unknown result as an error for the batch and stop the CLI cycle before its delete calls.
  Rationale: Continuing with the rest of a batch after losing authoritative status can convert a backend failure into a false statement that an object is unreferenced.
  Date/Author: 2026-11-09 / Codex.
- Decision: Ask metadata only about IDs from a bounded candidate batch and represent each result as Protected, Eligible, or Unknown.
  Rationale: Memory use is then bounded by the fixed batch size, and query failures cannot silently turn into an empty reference set.
  Date/Author: 2026-11-09 / Codex.
- Decision: Keep mounted janitor protection APIs unchanged unless the shared snapshot registration fence requires a narrowly shared change.
  Rationale: SFS-967 targets the standalone CLI's volume-wide in-memory sets; changing the mounted collector would add unrelated behavior and risk.
  Date/Author: 2026-11-09 / Codex.

## Outcomes & Retrospective

The standalone CLI now checks each candidate in a bounded batch and receives a typed `Protected`, `Eligible`, or `Unknown` result. Local redb scans preserve only candidate IDs, PostgreSQL queries only requested IDs, and missing outcomes or query errors stop the batch before deletion. Snapshot registration takes the shared advisory transaction lock before capture, and the exclusive CLI lock spans eligibility checks and deletion. Mounted janitor behavior is unchanged.

Focused Windows evidence so far:

    cargo test -p studiofs-meta janitor -- --nocapture
    result: 2 passed, 0 failed, 4 ignored (PG-only)

    cargo test -p studiofs janitor -- --nocapture
    result: 18 passed, 0 failed, 1 ignored

    cargo test -p studiofs-meta pg_engine::tests::janitor::snapshot_creation_waits_for_the_standalone_janitor_lock -- --ignored --exact --nocapture
    result: 1 passed against disposable local PostgreSQL

    cargo test -p studiofs-meta pg_engine::tests::janitor::janitor_read_only_session_validates_binding_and_lock -- --ignored --exact --nocapture
    result: 1 passed against disposable local PostgreSQL

    cargo clippy -p studiofs-meta -p studiofs --all-targets -- -D warnings
    result: passed

    cargo fmt --all --check
    result: passed

    git diff --check
    result: passed; Git emitted only line-ending conversion notices for Rust files.

No workspace-wide or full ignored suite was run. SFS-968 per-object deletion claims, object-store key canonicalization, and claim/finish recovery remain follow-up work. The shared-lock guarantee requires snapshot writers to be upgraded to this protocol before running a new janitor against old snapshot-producing binaries.

## Context and Orientation

The workspace root is `E:\dev\studiofs-cli`, on branch `feature/sfs-967-add-authoritative-batched-gc-eligibility`. `crates/studiofs-meta/src/janitor.rs` owns the read-only metadata session used only by the standalone CLI. `crates/studiofs-meta/src/pg_engine/janitor.rs` owns the PostgreSQL advisory lock and metadata safety reads. `crates/studiofs-meta/src/engine.rs` and `crates/studiofs-meta/src/redb_engine/gc.rs` define and implement local GC queries. `crates/studiofs/src/cli/janitor.rs` runs the CLI cycle and owns the metadata session across it; `crates/studiofs/src/cli/janitor/pipeline.rs` evaluates listed storage entries. `crates/studiofs-meta/src/pg_engine/snapshot_control.rs` registers snapshots in a PostgreSQL transaction. A per-volume advisory lock is a PostgreSQL lock keyed by the volume; an exclusive janitor session lock blocks a shared lock requested by a snapshot registration transaction.

The mounted background janitor uses a separate collector path and is not the target of this change. The SFS-968 follow-up adds per-object delete claims; this plan does not introduce that feature. SFS-967 must still prevent the known snapshot-registration race by excluding new snapshot registration for the lifetime of the existing CLI deletion lock.

## Plan of Work

First, add a bounded local query that classifies a supplied object-ID slice against local regular-file graph roots and pending deletion timestamps. Use it from the standalone session after the existing pending recovery and synchronization checks. Next, replace PostgreSQL's full-set reads with a bounded query for only the supplied IDs, including current metadata references, snapshot reachability pins for creating/ready snapshots and deleting snapshots with unexpired unreleased mount leases, and active upload/reconciliation records. Decode and return a typed classification; any query, malformed ID, bootstrap, identity, or synchronization problem is Unknown/error and blocks deletion.

Expose the classification through `JanitorMetadataSession` while retaining the existing exclusive global lock lifecycle. Update the CLI pipeline to deduplicate IDs into bounded batches, request authoritative classification, and process only Eligible objects. A failed or incomplete classification aborts before any object-store delete in that batch. Preserve age checks, dry-run behavior, limits, and existing mounted janitor behavior.

Add the shared transaction-level advisory lock at the beginning of snapshot registration, before reading the cut or inserting the `creating` row. Add tests for the three outcomes, failure-closed behavior, protection query coverage, and a live PostgreSQL concurrency scenario showing a snapshot registration transaction blocks while the CLI lock is held and can proceed after release. Update this ExecPlan with evidence and final validation.

## Concrete Steps

Run commands from `E:\dev\studiofs-cli`. Use only focused package tests, `cargo fmt --check`, and `git diff --check`; do not run workspace-wide or full ignored test suites. A targeted ignored PostgreSQL test may be invoked explicitly if `STUDIOFS_TEST_PG_DSN` is configured. Never print DSNs or secrets.

Expected focused commands after implementation:

    cargo test -p studiofs-meta janitor -- --nocapture
    cargo test -p studiofs janitor -- --nocapture
    cargo fmt --check
    git diff --check

Run only the exact PostgreSQL concurrency test by name when Docker/local PostgreSQL is available. If the database prerequisite is unavailable, retain the test as ignored and report that live database behavior was not exercised.

## Validation and Acceptance

The local and PostgreSQL query APIs return a result for every requested ID without materializing volume-wide object sets. Protected objects are never sent to object storage for deletion. An Unknown/error blocks deletion and is observable as a failed janitor metadata phase. The local read path retains fail-closed checks for pending recovery, unsynchronized metadata, and invalid PostgreSQL identity. The snapshot concurrency test demonstrates that a new snapshot cannot enter its registration transaction while the CLI lock is held, then succeeds after the CLI lock is released. Mounted janitor behavior and its current snapshot-protection API remain unchanged.

Each test command must report at least one executed test; a `running 0 tests` result is not evidence. Report unit tests separately from any ignored PostgreSQL test and clearly state when no live PostgreSQL service was available.

## Idempotence and Recovery

The read queries do not mutate metadata. Advisory locks release when the session or transaction ends, including connection failure. Retry the same focused test command after restoring local PostgreSQL availability. No database schema migration or destructive cleanup is expected. Keep all unrelated working tree changes intact.

## Artifacts and Notes

No generated repository artifacts are expected. The focused test output and final diff summary will be recorded in the completion update to this plan.

## Interfaces and Dependencies

Use existing workspace dependencies only. The metadata API should take a bounded slice of candidate `u64` object IDs and return a typed per-object eligibility representation with a pending deletion timestamp when applicable. Errors must remain typed or be converted into the existing janitor metadata error categories without rendering credentials. PostgreSQL classification must run under the existing command deadline and authoritative transaction, and snapshot registration must use the existing per-volume key from `pg_janitor_lock_key`.

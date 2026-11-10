# SFS-1064: cut PostgreSQL round trips per CLI Janitor batch and deletion

This ExecPlan is a living document maintained under `.agent/PLANS.md`. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current.

## Purpose / Big Picture

The SFS-979 benchmark showed that `studiofs janitor <volume>` waits on PostgreSQL round trips (RTT), not on S3 or PostgreSQL CPU: about 20 round trips per 64-key eligibility batch and about 18 per deleted object. At 100 ms RTT that is 29 keys/s scanning and 0.38 objects/s deleting. After this change the same work takes a few round trips per 1,000-key batch and well under one per deleted object, so scanning and deletion run close to the S3 request gate instead of the network latency. The live benchmark now reports `pg_round_trips`, so anyone can rerun it and see the count.

## Progress

- [x] (2026-11-10) Read SFS-1064, SFS-979's report and plan, `crates/studiofs-meta/src/pg_engine/janitor.rs`, `crates/studiofs-meta/src/janitor.rs`, and `crates/studiofs/src/cli/janitor/pipeline.rs`.
- [x] (2026-11-10) Added a round-trip counter to the benchmark's PostgreSQL delay proxy and recorded the "before" runs at 100 ms RTT.
- [x] (2026-11-10) Step 1: the janitor session prepares its statements once per connection and pipelines independent statements, so eligibility, the GC candidate page, and fence pruning each cost one round trip per call.
- [x] (2026-11-10) Step 2: one set-based claim statement and one settle statement (finish, acknowledge, defer) replace the per-object claim, finish, acknowledge and retry calls. The mounted Janitor uses the same claim SQL.
- [x] (2026-11-10) Step 3: `MAX_JANITOR_ELIGIBILITY_BATCH` raised from 64 to 1,000 (one S3 listing page); deletion claims stay at 64 objects per batch.
- [x] (2026-11-10) Recorded the "after" runs for steps 1+2 (batch 64) and step 3 (batch 1,000), and updated `docs/perf/sfs-979-janitor-benchmark.md`, `docs/janitor-performance.md`, and `docs/configuration.md`.
- [ ] macOS/Linux runs; only Windows 11 with Docker Desktop was measured.

## Surprises & Discoveries

- Observation: a tokio-postgres request is sent when its future is first polled. With prepared statements, `tokio::join!` over the statements of one transaction (including `BEGIN`/`COMMIT` via `batch_execute`) sends them in one flight, and PostgreSQL still executes them in order.
  Evidence: the proxy counts 2 round trips per eligibility batch (eligibility plus checkpoint save) after the change, against about 20 before.
- Observation: claiming a whole batch at once and then releasing the unattempted claims as retryable defers their GC queue rows for 60 seconds. A cancelled or capped cycle would then skip them as deferred on the next run.
  Evidence: `cancellation_drains_admitted_delete_before_unlock_and_replays_checkpoint` deleted one object instead of two on resume. Fixed by the `Released` settlement, which releases the claim without deferring the queue row.
- Observation: with a 1,000-key batch, a batch closes on the first key of the next object, so the first batch spans two 1,000-key S3 pages.
  Evidence: `janitor_delete_cap_stops_opendal_listing_after_first_page` now sees two list requests.

## Decision Log

- Decision: Pipeline statements in one round trip instead of only preparing them. Rationale: preparation alone halves the round trips; pipelining makes a read-only eligibility transaction one round trip. PostgreSQL runs pipelined statements in order, so the share lock on `metadata_state` is still taken before the claim statement takes its snapshot. Date: 2026-11-10.
- Decision: Claim in batches of at most 64 objects (`DELETION_CLAIM_BATCH`), capped by the remaining delete budget, and start an object's deletes only while the 60-second claim lease still covers the worst-case S3 work for it (`OBJECT_DELETE_BOUND`). Later objects are released and claimed again. Rationale: keeps the existing contract that all canonical deletes of an object finish inside its claim lease, and never claims more objects than `--max-deletes` admits. Date: 2026-11-10.
- Decision: Keep S3 deletes one object at a time. Rationale: S3 multi-object delete is SFS-1065. Date: 2026-11-10.
- Decision: Measure steps 1 and 2 together and attribute them by run type. Rationale: step 1 alone changes the scan-only runs (no claims); the deleting runs then show step 2. Building a step-1-only variant would need the per-object claim path rewritten twice. Date: 2026-11-10.

## Outcomes & Retrospective

See `docs/perf/sfs-979-janitor-benchmark.md`, section "SFS-1064: PostgreSQL round trips". At 100 ms RTT, round trips per eligibility batch fell from about 20 to 2, and per deleted object from about 18 to well under 1. Deletion now runs at the S3 gate (about 2 objects/s at the default 4 requests/s); SFS-1065 is the next ceiling.

## Context and Orientation

`PgJanitorSession` (`crates/studiofs-meta/src/pg_engine/janitor.rs`) owns the CLI Janitor's single PostgreSQL connection and advisory lock. `JanitorMetadataSession` (`crates/studiofs-meta/src/janitor.rs`) adds local redb checks on top. The CLI pipeline (`crates/studiofs/src/cli/janitor/pipeline.rs`) lists S3, checks eligibility per batch, then claims, deletes and settles objects. A claim moves the object's ledger row to `delete_pending` under a fencing token, so a late upload or publication of that ID fails. Settling releases the lease, records the outcome, and acknowledges or defers the GC queue row.

## Plan of Work

1. In `pg_engine/janitor.rs`, add a per-connection statement cache (`prepare_statements`) and pipeline `read_protection`, `gc_candidate_page`, the checkpoint write and fence pruning.
2. Replace the per-object claim with `CLAIM_OBJECT_DELETIONS_SQL` (set-based, after `CLAIM_PUBLICATION_FENCE_SQL` in the same transaction) and add `SETTLE_OBJECT_DELETIONS_SQL`. Keep `claim_object_deletion_in` for the mounted Janitor on the shared SQL.
3. Expose `claim_deletions` and `settle_deletions` with `JanitorDeletionSettlement` from `JanitorMetadataSession`, and restructure the CLI pipeline into `admit_orphan`, `delete_orphans` and `delete_object_keys`.
4. Raise `MAX_JANITOR_ELIGIBILITY_BATCH` to 1,000 and express batch-size tests through the constant.

## Concrete Steps

From the repository root:

    cargo test -p studiofs --bin studiofs cli::janitor
    cargo test -p studiofs-meta --lib pg_engine::tests::janitor -- --ignored              # needs STUDIOFS_TEST_PG_DSN
    cargo test -p studiofs-meta --lib pg_engine::tests::snapshot_reachability -- --ignored
    cargo build --release -p studiofs --bin studiofs --tests
    STUDIOFS_JANITOR_BENCH_ORPHAN_PERCENT=0 STUDIOFS_JANITOR_BENCH_OBJECTS=10000 \
      cargo test -p studiofs --bin studiofs --release live_cli_janitor_benchmark -- --ignored --nocapture

## Validation and Acceptance

The live benchmark at 100 ms RTT reports `pg_round_trips` per run, zero missing referenced or foreground objects, and complete coverage. `janitor_session_fences_object_deletion_claims` proves on PostgreSQL that one claim batch skips referenced, uploading, uploaded and snapshot-pinned objects, fences an untracked one, rejects a stale token, acknowledges a confirmed deletion and defers a retryable one.

## Idempotence and Recovery

All benchmark stacks are disposable Docker Compose projects. A crash between claim and settle leaves claims that expire after 60 seconds and are re-claimed, as before.

## Interfaces and Dependencies

In `crates/studiofs-meta/src/janitor.rs`:

    pub enum JanitorDeletionSettlement { Finished { object_id, fencing_token, outcome }, Released { object_id, fencing_token }, Deferred { object_id } }
    impl JanitorMetadataSession {
        pub fn claim_deletions(&mut self, object_ids: &[u64]) -> Result<HashMap<u64, i64>, JanitorMetadataError>;
        pub fn settle_deletions(&mut self, settlements: &[JanitorDeletionSettlement]) -> Result<HashSet<u64>, JanitorMetadataError>;
    }

`studiofs-meta` now depends on the workspace `futures` crate for `try_join_all` over statement preparation.

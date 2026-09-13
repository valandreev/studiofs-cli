# Bootstrap Checkpoint Freshness Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use the repository execution-plan rules and test-driven development while implementing this plan task-by-task.

**Goal:** Make PostgreSQL bootstrap checkpoints produce a bounded-lag refresh request when the metadata head is too far ahead by version, accumulated event-payload bytes, or elapsed time on a changing volume.

**Architecture:** PostgreSQL `metadata_state` remains the authoritative place for the checkpoint version, the event-payload byte counter since that checkpoint, and the checkpoint timestamp. Event publication increments the byte counter in the same transaction that advances the metadata head. A small Rust policy evaluates one row of bookkeeping after successful publication or background synchronization and stores an observable, de-duplicated refresh request in the `PgMetaEngine`; it never builds a checkpoint on the foreground mutation path.

**Tech Stack:** Rust 2024, `studiofs-meta`, PostgreSQL via `tokio-postgres`, `chrono`, redb-backed local metadata, focused Cargo unit tests.

---

This is a living plan. It follows `.agent/PLANS.md`; the `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` sections must be updated as implementation proceeds.

## Purpose / Big Picture

After this change, a busy volume cannot silently accumulate an unbounded bootstrap replay tail. A successful event publication or background metadata synchronization observes one `metadata_state` row and marks the engine's checkpoint refresh request when any configured freshness limit is reached. Quiet volumes that are already at the checkpoint remain quiet, and repeated observations do not create repeated requests. Tests expose the exact reason and measurements, which gives the later fenced coordinator (SFS-704) a stable signal to consume.

## Progress

- [x] (2026-09-13) Read the SFS-703 Linear contract, its SFS-702 dependency, repository instructions, and the mounted-IO/metadata contracts.
- [x] (2026-09-13) Traced event publication, background flush/sync, checkpoint writing, diagnostics, schema validation, and runtime forwarding through codebase-memory and direct source inspection.
- [x] (2026-09-13) Chose persisted payload-byte and checkpoint-time bookkeeping plus a pure policy/tracker boundary.
- [x] (2026-09-13) Add the failing policy and tracker tests and observe the expected compile/test failure (missing policy types, exit 1).
- [x] (2026-09-13) Implement policy, tracker, public diagnostics, and runtime forwarding; focused policy tests pass 7/7 including SQL/env assertions.
- [x] (2026-09-13) Add PostgreSQL bookkeeping columns, atomic byte accounting, checkpoint reset, and one-row observation; exact encoded payload accounting passes 1/1.
- [x] (2026-09-13) Wire observations into successful publication, background sync, and explicit checkpoint writes without changing foreground mutation semantics.
- [x] (2026-09-13) Run focused nonzero tests/checks, inspect the diff, and update this plan with evidence: policy/SQL/env 7/7, payload accounting 1/1, schema assertion 1/1, and live PostgreSQL publish/reset 1/1.
- [x] (2026-09-13) Commit all intended SFS-703 files with the English conventional commit `feat: enforce checkpoint freshness policy`.
- [x] (2026-09-13) Record the implementation and validation result on Linear SFS-703; the issue is Done and contains the commit/validation summary.

## Surprises & Discoveries

- Observation: `PgMetaEngine::flush_metadata` already writes a checkpoint after an explicit flush, while ordinary `mutate` only records pending work and wakes the background poller.
  Evidence: `crates/studiofs-meta/src/pg_engine/replication_api.rs` and `replication_flush.rs`; the new policy therefore requests refresh rather than adding checkpoint construction to `mutate`.
- Observation: The PostgreSQL head query currently returns only `version` and `checkpoint_version`; `metadata_state.updated_at` cannot represent checkpoint age because event publication updates it.
  Evidence: `crates/studiofs-meta/src/pg_engine/sql.rs` and `metadata_io.rs`; a dedicated `checkpoint_updated_at` column is required.
- Observation: The existing replay-page query already calculates event payload sizes, but using a `SUM` over the event tail on every flush would violate SFS-703's normal-path cost constraint.
  Evidence: `SELECT_FOREIGN_EVENT_ROWS` in `crates/studiofs-meta/src/pg_engine/sql.rs`; the counter will instead be incremented from encoded payload lengths during publication.
- Observation: A checkpoint writer can race a publication from another client, so blindly resetting the byte counter could discard bytes after the newly written checkpoint.
  Evidence: `UPSERT_METADATA_CHECKPOINT` now accepts a checkpoint only when its version covers the observed metadata head; otherwise the existing counter and timestamp remain unchanged.

## Decision Log

- Decision: Use three independently configurable limits: version lag, encoded event-payload bytes since checkpoint, and checkpoint age in seconds.
  Rationale: These are the three independent acceptance signals in SFS-703 and cover both high event count and high payload volume without scanning the event log.
  Date/Author: 2026-09-13 / Codex.
- Decision: Persist `checkpoint_event_bytes` and `checkpoint_updated_at` in `metadata_state` and update them in the existing publication/checkpoint SQL.
  Rationale: All clients then observe the same bounded-lag measurements, and the hot path performs scalar arithmetic rather than an aggregate tail scan.
  Date/Author: 2026-09-13 / Codex.
- Decision: Expose freshness through a new defaulted `MetaReplication::metadata_checkpoint_freshness_diagnostics` method and forward it through `RuntimeMetaHandle`.
  Rationale: SFS-704 and tests can inspect the request without coupling to the concrete PostgreSQL engine; redb and read-only engines retain a safe default.
  Date/Author: 2026-09-13 / Codex.
- Decision: Deduplicate request transitions and count only `false → true` freshness transitions.
  Rationale: A quiet stale observation must not repeatedly enqueue equivalent refresh work; a later successful checkpoint observation clears the state and permits a future request.
  Date/Author: 2026-09-13 / Codex.
- Decision: Require an accepted checkpoint version to be at least the current metadata head before resetting freshness bookkeeping.
  Rationale: A stale checkpoint must not erase the byte count for event batches published concurrently by another client; the next observation will retain the request until a checkpoint covers the head.
  Date/Author: 2026-09-13 / Codex.
- Decision: Use conservative defaults of 10,000 versions, 64 MiB of event payloads, and 3,600 seconds of age; allow positive environment overrides named `STUDIOFS_PG_CHECKPOINT_MAX_VERSION_LAG`, `STUDIOFS_PG_CHECKPOINT_MAX_EVENT_BYTES`, and `STUDIOFS_PG_CHECKPOINT_MAX_AGE_SECS`.
  Rationale: SFS-718 will benchmark and tune thresholds; SFS-703 needs a deterministic configurable policy now.
  Date/Author: 2026-09-13 / Codex.

## Outcomes & Retrospective

The pure policy tests demonstrate independent version, payload-byte, and changing-volume age triggers; the tracker test demonstrates de-duplicated requests. The live PostgreSQL test demonstrates atomic publication bookkeeping and reset after an accepted checkpoint. Focused `studiofs-meta` and `studiofs-vfs` clippy checks pass with `-D warnings`, the application compile check passes, and formatting/diff checks pass. Linear SFS-703 is now Done with the commit and validation summary attached. No full workspace suite or mounted WinFSP benchmark was run, in accordance with the repository gate; SFS-704 still needs to consume `metadata_checkpoint_freshness_diagnostics` from its fenced background coordinator.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine.rs` constructs the PostgreSQL metadata engine and owns its background-worker state. `replication_flush.rs` batches local redb events, `metadata_io.rs` publishes encoded events and writes serving checkpoints, `metadata_sync.rs` performs background/explicit remote replay, and `sql.rs` contains the SQL strings. `schema.rs` provisions and migrates the PostgreSQL tables, while `engine.rs` defines backend-neutral replication diagnostics. `crates/studiofs-vfs/src/runtime.rs` forwards the replication trait to the VFS trait object.

The metadata head is `metadata_state.version`. The checkpoint replay floor is `metadata_state.checkpoint_version`; a client bootstraps from the checkpoint and replays event-log batches after that floor. “Checkpoint refresh request” in this plan means an in-memory, observable signal that a later coordinator should build/publish a checkpoint. It is not permission to synchronously serialize a checkpoint during a local filesystem mutation.

## Plan of Work

### Task 1: Create the pure freshness policy and red tests

**Files:**

- Create: `crates/studiofs-meta/src/pg_engine/checkpoint_freshness.rs`
- Modify: `crates/studiofs-meta/src/pg_engine.rs` to register the module.

Define a `CheckpointFreshnessPolicy` containing positive `max_version_lag: u64`, `max_event_bytes: u64`, and `max_age: Duration`, with the conservative defaults from the Decision Log. Define an input containing head version, checkpoint version, event payload bytes, and checkpoint age. The evaluator must treat `head_version > checkpoint_version` as “volume changed”, calculate saturating nonnegative lag, and return the first deterministic reason in this order: version, bytes, age. Age must only be eligible when the volume changed. Define a tracker that stores the latest diagnostics and increments `refresh_requests` only when a stale evaluation transitions from not-needed to needed.

Before implementing those types, add tests named `version_lag_requests_checkpoint_refresh`, `event_payload_bytes_request_checkpoint_refresh`, `checkpoint_age_requests_refresh_only_after_volume_change`, and `quiet_up_to_date_volume_does_not_repeat_refresh_requests`. The tests must use small custom thresholds and assert the reason, `refresh_needed`, measured lag/bytes/age, and transition count. Run the filtered library test and record the expected compile failure because the policy types are not yet implemented.

### Task 2: Implement policy diagnostics and public trait exposure

**Files:**

- Modify: `crates/studiofs-meta/src/engine.rs` to add `MetadataCheckpointRefreshReason` and `MetadataCheckpointFreshnessDiagnostics`, plus a default `MetaReplication::metadata_checkpoint_freshness_diagnostics` method.
- Modify: `crates/studiofs-meta/src/lib.rs` to re-export both diagnostics types.
- Modify: `crates/studiofs-meta/src/pg_engine/checkpoint_freshness.rs` to implement the policy/tracker tested in Task 1.
- Modify: `crates/studiofs-vfs/src/runtime.rs` to forward the new trait method and import the new type.

The diagnostics must include optional head/checkpoint versions, calculated version lag, accumulated event-payload bytes, optional age milliseconds, configured limits, `refresh_needed`, optional reason, and the de-duplicated `refresh_requests` count. The default method returns `Default::default`, preserving behavior for redb and read-only implementations. The PostgreSQL tracker must copy all scalar measurements into this public value without exposing credentials or SQL.

Run `cargo test -p studiofs-meta checkpoint_freshness --lib` and expect all four policy tests to pass with a nonzero test count. Run `cargo check -p studiofs-vfs` to verify the trait-object forwarding compiles.

### Task 3: Persist cheap PostgreSQL freshness bookkeeping

**Files:**

- Modify: `crates/studiofs-meta/src/pg_engine/schema.rs` to add `checkpoint_event_bytes BIGINT NOT NULL DEFAULT 0 CHECK (checkpoint_event_bytes >= 0)` and `checkpoint_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()` to fresh and additive `metadata_state` DDL.
- Modify: `crates/studiofs-meta/src/pg_engine.rs` schema validation to require both columns.
- Modify: `crates/studiofs-meta/src/pg_engine/sql.rs` to add a one-row freshness query, make `BUMP_METADATA_VERSION` add the supplied payload-byte count, and make `UPSERT_METADATA_CHECKPOINT` reset the byte counter and timestamp whenever a checkpoint is promoted.
- Modify: `crates/studiofs-meta/src/pg_engine/metadata_io.rs` to calculate the encoded payload-byte count while splitting batches, pass it to the bump query, and decode the one-row freshness observation using `chrono::DateTime<Utc>`.
- Modify: `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs` and `schema.rs` tests where SQL fragments are asserted.

The byte count must be the exact sum of encoded `event_payload` lengths, checked against `BIGINT`; it must not count response framing or run an event-log aggregate. The increment and event inserts must remain in the same transaction. The checkpoint upsert must reset the counter and set `checkpoint_updated_at = now()` only on an accepted current-format checkpoint update. Existing additive schema statements must use `IF NOT EXISTS` and retain the existing default/check constraints.

Run the schema/unit SQL tests and the focused metadata library tests. Do not run ignored PostgreSQL tests unless the requested environment is explicitly available; if it is unavailable, report that boundary instead of treating skipped tests as evidence.

### Task 4: Observe and expose refresh requests at successful replication boundaries

**Files:**

- Modify: `crates/studiofs-meta/src/pg_engine.rs` to store the configured policy and tracker mutex, parse the three positive environment overrides during `open_with_options`, initialize the fields, and provide a best-effort observation method plus a diagnostics accessor.
- Modify: `crates/studiofs-meta/src/pg_engine/metadata_io.rs` to update the tracker from the scalar observation and leave the last valid state unchanged if the diagnostic query fails.
- Modify: `crates/studiofs-meta/src/pg_engine/metadata_sync.rs` to observe after successful synchronization.
- Modify: `crates/studiofs-meta/src/pg_engine/replication_api.rs` to expose diagnostics and observe after explicit checkpoint writes through the existing path.

Call the observation after successful normal event publication, successful background/explicit synchronization, and successful checkpoint promotion. Diagnostic-query failures must be logged as best-effort diagnostics and must not fail a foreground flush or mutation. The observer must not serialize local checkpoint bytes, acquire the local mutation lock, or change `mutate`. A successful checkpoint observation at head will clear `refresh_needed`; a stale observation sets it once and leaves it set until the next fresh observation.

Add focused tests for environment override parsing if the existing `pg_env_var` test seam permits it, and add a source-level SQL assertion that the normal publication path updates the persisted byte counter. Ensure diagnostics are available through `Arc<dyn MetaReplication>` and `Arc<dyn VfsMeta>`.

### Task 5: Verify, review, commit, and report

**Files:**

- Modify: this plan with actual progress, evidence, surprises, decisions, and outcomes.
- Commit: every intended SFS-703 source, test, schema, plan, and documentation file; preserve unrelated worktree content.

Run only focused validation from `E:\dev\studiofs-cli`, at minimum:

    cargo fmt -- --check
    cargo test -p studiofs-meta checkpoint_freshness --lib
    cargo test -p studiofs-meta schema_declares_incompatible_sync_boundary_and_checkpoint_descriptor --lib
    cargo check -p studiofs-meta
    cargo check -p studiofs-vfs
    git diff --check

If changed SQL or lifecycle behavior requires it and the local `.env` provides `STUDIOFS_TEST_PG_DSN`, run the named ignored SFS-703-focused PostgreSQL tests explicitly with `cargo test -p studiofs-meta <filter> --lib -- --ignored --nocapture`; otherwise state that PostgreSQL integration and native mount gates remain environment-gated. Inspect `git diff`, confirm the new tests ran nonzero tests, and commit with a message such as `feat: enforce checkpoint freshness policy`. Add an English Linear comment with the commit and exact focused validation; do not include DSNs or secrets.

## Concrete Steps

All commands run from `E:\dev\studiofs-cli` in PowerShell. The implementation must use `apply_patch` for file edits and must not use a full workspace test suite. The first red test command is:

    cargo test -p studiofs-meta checkpoint_freshness --lib

After each green milestone, update `Progress` with the observed test count and exit status. Before the final commit, run the complete focused command set in Task 5 and inspect the staged file list with `git status --short`.

## Validation and Acceptance

The policy unit tests demonstrate the three independent triggers: a version lag at its threshold, an event-payload byte count at its threshold, and an old checkpoint age on a changed volume. The quiet-volume test demonstrates no request when head equals checkpoint and no repeated request while the same stale state is observed. SQL/schema tests demonstrate the persisted counter and timestamp exist in both fresh and additive schema paths, publication increments the counter transactionally, and checkpoint promotion resets it.

The implementation satisfies SFS-703 when a successful background publication or sync can make `metadata_checkpoint_freshness_diagnostics().refresh_needed == true` with a reason and measurements, while a current checkpoint reports false and does not increase `refresh_requests` on repeated observations. A future SFS-704 coordinator can consume this state and build the checkpoint in its fenced background path. Focused Rust checks must exit zero with nonzero matching test counts. Full workspace suites and live mount benchmarks are explicitly outside this task unless the user separately requests them.

## Idempotence and Recovery

The schema uses additive `IF NOT EXISTS` statements, and the Rust state is initialized from defaults, so reopening a client is safe. If a code change fails compilation, keep the red test and repair the narrow type/interface mismatch before moving on. If PostgreSQL integration is unavailable, do not add sleeps, retries, or test skips; retain the focused pure-policy evidence and report the missing prerequisite. If a commit is needed, stage only SFS-703 files and leave unrelated user changes untouched.

## Artifacts and Notes

The primary artifacts are the living plan, the pure policy tests, the public diagnostics type, the PostgreSQL schema/SQL changes, and the focused Cargo outputs. The final report must link changed repository files with absolute paths and include the commit id, exact commands, and any unavailable PostgreSQL/native gates.

## Interfaces and Dependencies

The final interfaces are:

    pub enum MetadataCheckpointRefreshReason {
        VersionLag,
        EventPayloadBytes,
        Age,
    }

    pub struct MetadataCheckpointFreshnessDiagnostics {
        pub head_version: Option<u64>,
        pub checkpoint_version: Option<u64>,
        pub version_lag: u64,
        pub event_payload_bytes_since_checkpoint: u64,
        pub checkpoint_age_ms: Option<u64>,
        pub max_version_lag: u64,
        pub max_event_payload_bytes: u64,
        pub max_checkpoint_age_ms: u64,
        pub refresh_needed: bool,
        pub refresh_reason: Option<MetadataCheckpointRefreshReason>,
        pub refresh_requests: u64,
    }

    pub trait MetaReplication {
        fn metadata_checkpoint_freshness_diagnostics(
            &self,
        ) -> MetadataCheckpointFreshnessDiagnostics {
            MetadataCheckpointFreshnessDiagnostics::default()
        }
    }

The policy depends only on `std::time::Duration` and scalar observations. The PostgreSQL adapter depends on the existing `tokio-postgres` connection pool and `chrono` support already present in `studiofs-meta`; no new crate or remote service is introduced.

## Plan Revision Note

2026-09-13: Initial plan created after reading SFS-703/SFS-702, tracing the current publication/checkpoint/sync paths, and selecting persisted scalar bookkeeping plus a pure TDD policy boundary.

2026-09-13: Added the implementation evidence and recorded the concurrent-publication guard that prevents a stale checkpoint from resetting bytes for a newer metadata head.

2026-09-13: Recorded the completed focused validation and commit milestone; the remaining delivery action is the Linear update.

2026-09-13: Recorded the Linear completion update and marked the delivery milestone complete.

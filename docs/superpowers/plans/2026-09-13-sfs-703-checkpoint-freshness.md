# Bootstrap Checkpoint Freshness Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use the repository execution-plan rules and test-driven development while implementing this plan task-by-task.

**Goal:** Make PostgreSQL bootstrap checkpoints produce a bounded-lag refresh request when the metadata head is too far ahead by version, accumulated event-payload bytes, or elapsed time on a changing volume.

**Architecture:** PostgreSQL `metadata_state` remains the authoritative place for the checkpoint version, monotonic event-payload byte counters, and checkpoint timestamp. Event publication increments the total byte counter in the same transaction that advances the metadata head. A small Rust policy evaluates one row of bookkeeping after successful publication or background synchronization and stores an observable, de-duplicated refresh request in the `PgMetaEngine`; it never builds a checkpoint on the foreground mutation path.

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
- [x] (2026-09-13) Verify the attached review item-by-item: confirm the checkpoint starvation and lock-held observation issues, reject the nonexistent `LocalFlushFollowup` path, and confirm the persisted BIGINT boundary for the u64 parser suggestion.
- [x] (2026-09-13) Fix review-confirmed issues with monotonic total/checkpoint byte counters, capture-before-serialization, PostgreSQL-side age calculation, same-transaction freshness `RETURNING`, poison recovery, typed nonnegative row decoding, threshold/inversion tests, and the redundant payload-vector clone.
- [x] (2026-09-13) Run focused review regressions, including the live PostgreSQL checkpoint-tail and existing checkpoint-monotonicity tests; update this plan before the review-fix commit.
- [x] (2026-09-13) Re-review the follow-up findings against the current call graph: preserve one capture query under `replication_lock`, log stale-skip and version-inversion diagnostics, remove public diagnostics `Copy`, add the typed conversion boundary, and enforce the checkpoint-version DDL invariant.
- [x] (2026-09-13) Run the follow-up focused policy, live PostgreSQL freshness/tail/monotonicity/legacy tests, clippy, and application checks; the follow-up review-fix commit remains the delivery step.

## Surprises & Discoveries

- Observation: `PgMetaEngine::flush_metadata` already writes a checkpoint after an explicit flush, while ordinary `mutate` only records pending work and wakes the background poller.
  Evidence: `crates/studiofs-meta/src/pg_engine/replication_api.rs` and `replication_flush.rs`; the new policy therefore requests refresh rather than adding checkpoint construction to `mutate`.
- Observation: The PostgreSQL head query currently returns only `version` and `checkpoint_version`; `metadata_state.updated_at` cannot represent checkpoint age because event publication updates it.
  Evidence: `crates/studiofs-meta/src/pg_engine/sql.rs` and `metadata_io.rs`; a dedicated `checkpoint_updated_at` column is required.
- Observation: The existing replay-page query already calculates event payload sizes, but using a `SUM` over the event tail on every flush would violate SFS-703's normal-path cost constraint.
  Evidence: `SELECT_FOREIGN_EVENT_ROWS` in `crates/studiofs-meta/src/pg_engine/sql.rs`; the counter will instead be incremented from encoded payload lengths during publication.
- Observation: A checkpoint writer can race a publication from another client. A single resettable counter cannot distinguish bytes covered by the serialized checkpoint from bytes published while serialization is in progress.
  Evidence: the writer now captures `(version, total_event_payload_bytes)` before serialization; promotion records that captured total as `checkpoint_total_event_bytes` and leaves the monotonic total untouched. The upsert no longer requires the serialized version to cover the current head, so a valid checkpoint promotion does not starve under concurrent writes.
- Observation: PostgreSQL row decoding is the adapter boundary for freshness values. Negative BIGINT versions, counters, or server-computed ages must be rejected before entering the u64 policy domain.
  Evidence: `metadata_io.rs` validates every freshness/capture BIGINT and rejects inverted byte counters with `MetaError::Storage`.
- Observation: `flush_metadata` holds `replication_lock` while calling `write_checkpoint_locked`; adding a second freshness query on the stale-local early return would preserve diagnostics at avoidable lock-held network cost.
  Evidence: the existing capture query now returns the complete freshness row, so the stale-skip path logs and records that observation without another round-trip.

## Decision Log

- Decision: Use three independently configurable limits: version lag, encoded event-payload bytes since checkpoint, and checkpoint age in seconds.
  Rationale: These are the three independent acceptance signals in SFS-703 and cover both high event count and high payload volume without scanning the event log.
  Date/Author: 2026-09-13 / Codex.
- Decision: Persist monotonic `total_event_payload_bytes`, checkpoint baseline `checkpoint_total_event_bytes`, and `checkpoint_updated_at` in `metadata_state` and update them in the existing publication/checkpoint SQL.
  Rationale: All clients then observe the same bounded-lag measurements, concurrent publication remains visible as `total - checkpoint_total`, and the hot path performs scalar arithmetic rather than an aggregate tail scan.
  Date/Author: 2026-09-13 / Codex.
- Decision: Expose freshness through a new defaulted `MetaReplication::metadata_checkpoint_freshness_diagnostics` method and forward it through `RuntimeMetaHandle`.
  Rationale: SFS-704 and tests can inspect the request without coupling to the concrete PostgreSQL engine; redb and read-only engines retain a safe default.
  Date/Author: 2026-09-13 / Codex.
- Decision: Deduplicate request transitions and count only `false → true` freshness transitions.
  Rationale: A quiet stale observation must not repeatedly enqueue equivalent refresh work; a later successful checkpoint observation clears the state and permits a future request.
  Date/Author: 2026-09-13 / Codex.
- Decision: Capture the remote head and total byte count before checkpoint serialization, skip serialization when the local head is already stale, and accept a serialized checkpoint without requiring it to cover a newer concurrent remote head.
  Rationale: The captured baseline identifies exactly which bytes the checkpoint can cover. The monotonic total preserves bytes published after capture, while the absence of a current-head predicate avoids checkpoint starvation under active writes. A checkpoint that started from a stale local head is not attempted.
  Date/Author: 2026-09-13 / Codex.
- Decision: Use conservative defaults of 10,000 versions, 64 MiB of event payloads, and 3,600 seconds of age; allow positive environment overrides named `STUDIOFS_PG_CHECKPOINT_MAX_VERSION_LAG`, `STUDIOFS_PG_CHECKPOINT_MAX_EVENT_BYTES`, and `STUDIOFS_PG_CHECKPOINT_MAX_AGE_SECS`.
  Rationale: SFS-718 will benchmark and tune thresholds; SFS-703 needs a deterministic configurable policy now.
  Date/Author: 2026-09-13 / Codex.

## Outcomes & Retrospective

The pure policy tests demonstrate independent version, payload-byte, and changing-volume age triggers, inclusive precedence boundaries, and fail-closed version inversion; the tracker test demonstrates de-duplicated requests. The live PostgreSQL tests demonstrate atomic publication bookkeeping/reset, checkpoint promotion with a post-capture event tail, remote-head monotonicity, legacy-format refusal, and diagnostics on a stale-local checkpoint skip. The review fixes also remove the publication-time freshness round trip, use PostgreSQL server time, recover poisoned diagnostics state, validate BIGINT values before policy evaluation, remove the unnecessary payload-vector clone, avoid large public `Copy` values, and enforce the checkpoint-version DDL invariant. Focused `studiofs-meta` and `studiofs-vfs` clippy checks, the application compile check, formatting, and diff checks remain the required validation boundary. No full workspace suite or mounted WinFSP benchmark was run; SFS-704 still needs to consume `metadata_checkpoint_freshness_diagnostics` from its fenced background coordinator.

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

- Modify: `crates/studiofs-meta/src/pg_engine/schema.rs` to add monotonic `total_event_payload_bytes`, checkpoint baseline `checkpoint_total_event_bytes`, and `checkpoint_updated_at` to fresh and additive `metadata_state` DDL.
- Modify: `crates/studiofs-meta/src/pg_engine.rs` schema validation to require both columns.
- Modify: `crates/studiofs-meta/src/pg_engine/sql.rs` to add one-row freshness/capture queries, make `BUMP_METADATA_VERSION` add the supplied payload-byte count and return freshness state, and make `UPSERT_METADATA_CHECKPOINT` advance the checkpoint baseline and timestamp without resetting the monotonic total.
- Modify: `crates/studiofs-meta/src/pg_engine/metadata_io.rs` to calculate the encoded payload-byte count while splitting batches, pass it to the bump query, decode validated nonnegative BIGINT values, and use PostgreSQL server time for checkpoint age.
- Modify: `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs` and `schema.rs` tests where SQL fragments are asserted.

The byte count must be the exact sum of encoded `event_payload` lengths, checked against `BIGINT`; it must not count response framing or run an event-log aggregate. The increment and event inserts must remain in the same transaction. The checkpoint upsert must advance the checkpoint baseline and set `checkpoint_updated_at = now()` only on an accepted current-format checkpoint update. Existing additive schema statements must use `IF NOT EXISTS` and retain the nonnegative/cross-counter constraints.

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

If changed SQL or lifecycle behavior requires it and the local `.env` provides `STUDIOFS_TEST_PG_DSN`, run the named ignored SFS-703-focused PostgreSQL tests explicitly with `cargo test -p studiofs-meta <filter> --lib -- --ignored --nocapture`; otherwise state that PostgreSQL integration and native mount gates remain environment-gated. Inspect `git diff`, confirm the new tests ran nonzero tests, and commit the review fixes with a conventional `fix:` message; do not include DSNs or secrets.

## Concrete Steps

All commands run from `E:\dev\studiofs-cli` in PowerShell. The implementation must use `apply_patch` for file edits and must not use a full workspace test suite. The first red test command is:

    cargo test -p studiofs-meta checkpoint_freshness --lib

After each green milestone, update `Progress` with the observed test count and exit status. Before the final commit, run the complete focused command set in Task 5 and inspect the staged file list with `git status --short`.

## Validation and Acceptance

The policy unit tests demonstrate the three independent triggers: a version lag at its threshold, an event-payload byte count at its threshold, and an old checkpoint age on a changed volume. They also cover inclusive precedence boundaries and fail-closed version inversion. The quiet-volume test demonstrates no request when head equals checkpoint and no repeated request while the same stale state is observed. SQL/schema tests demonstrate the persisted monotonic counters and timestamp exist in both fresh and additive schema paths, publication increments the total transactionally, and checkpoint promotion advances the baseline while preserving a post-capture tail.

The implementation satisfies SFS-703 when a successful background publication or sync can make `metadata_checkpoint_freshness_diagnostics().refresh_needed == true` with a reason and measurements, while a current checkpoint reports false and does not increase `refresh_requests` on repeated observations. Checkpoint publication must preserve bytes added after serialization capture, and adapter decoding must reject negative or inconsistent persisted counters. A future SFS-704 coordinator can consume this state and build the checkpoint in its fenced background path. Focused Rust checks must exit zero with nonzero matching test counts. Full workspace suites and live mount benchmarks are explicitly outside this task unless the user separately requests them.

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

2026-09-13: Incorporated the attached review: replaced the resettable byte counter with monotonic total/baseline bookkeeping, removed the strict current-head upsert predicate, added capture-before-serialization and server-side age, reused publication `RETURNING`, validated adapter values, recovered poisoned state, removed the payload clone, and added focused boundary/tail regressions. Review items describing `LocalFlushFollowup` were not applicable to the current code, and direct u64 config parsing was not adopted because persisted counters are PostgreSQL BIGINT values.

2026-09-13: Incorporated the follow-up review: the stale-local skip now records the already captured server observation and logs the reason, version inversion is warned, the public diagnostics use explicit `Clone`, the adapter observation converts through `From`, and metadata schema enforces `checkpoint_version <= version`. The proposal to expose age on unchanged volumes remains out of scope because the current diagnostics contract defines age as a changing-volume freshness signal; the i64 batch-version cast is simplified at the row boundary.

2026-09-13: Re-verified the attached review against `b74c3d0c`: the current source still had the original stale-checkpoint condition, a separate capture query/type, and a post-upsert freshness query. Applied the confirmed corrections: reuse the freshness observation as the capture, skip when the remote checkpoint covers the local version, return freshness from the checkpoint upsert, preallocate bounded event batches, and document the skip contract. Added a live regression for the already-current checkpoint path and SQL assertions for the upsert observation row.

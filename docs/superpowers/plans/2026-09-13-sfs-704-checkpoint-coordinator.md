# SFS-704 fenced background bootstrap checkpoint coordinator

## Purpose

Implement SFS-704 in `studiofs-meta`: a volume-scoped background coordinator
that coalesces freshness-triggered checkpoint requests, claims a PostgreSQL
lease with a fencing token before expensive serialization, verifies the local
source cut, and publishes one atomically accepted serving checkpoint. Existing
explicit replication flush checkpoints remain supported and foreground local
operations remain independent of checkpoint serialization.

## Progress

- [x] 2026-09-13: Read the SFS-704 Linear contract and verified SFS-702/SFS-703
      are complete blockers.
- [x] 2026-09-13: Recorded clean repository state on the SFS-704 branch.
- [x] 2026-09-13: Read the local-first, PostgreSQL sync, metadata, lifecycle,
      and plan contracts; checked Codebase Memory coverage and traced the
      freshness, checkpoint, and worker paths.
- [x] 2026-09-13: Added the durable lease schema and fenced SQL operations.
- [x] 2026-09-13: Added coalesced coordinator state, worker lifecycle ownership, and retry
      wakeups.
- [x] 2026-09-13: Added source-cut capture/validation and coordinator publication without
      holding the replication lock during serialization.
- [x] 2026-09-13: Added focused unit and service-backed regression tests for coalescing,
      fencing/takeover, exact version publication, and concurrent foreground
      progress architecture.
- [x] 2026-09-13: Hardened source-cut capture/publication against pending and
      in-flight local event-log mutations, and bounded failed coordination
      retries with exponential backoff and jitter.
- [ ] Run the final commit and update Linear.

## Surprises

- SFS-703 already changed explicit checkpoint publication to skip an already
  current checkpoint and to return freshness data from the same PostgreSQL
  statement. SFS-704 must build on that SQL rather than reintroducing duplicate
  freshness reads or changing explicit flush semantics.
- The current background poll worker observes freshness but has no dedicated
  checkpoint worker. `PgBackgroundWorkers` owns all joinable worker handles, so
  the coordinator must be added to the same shutdown/join path.
- `local_version` advances only with remote replay. A local redb mutation can
  commit before its in-memory pending bookkeeping is updated, so checkpoint
  source cuts must also use the durable `nextEventSeq`/`flushed_event_seq`
  position and the pending/in-flight state.
- `export_serving_checkpoint_bytes()` obtains a consistent local redb read
  transaction but does not accept a version argument. The coordinator therefore
  needs a capture-before/build/verify-after protocol and must discard a payload
  if the local version advances during serialization.

## Decision Log

1. Use a durable `bootstrap_checkpoint_leases` row per volume with owner,
   expiry, and monotonically increasing fencing token. Claim, publish, renew,
   and release are short PostgreSQL statements; no PostgreSQL transaction is
   held while redb serializes the checkpoint.
2. Keep the lease separate from `metadata_state`. This makes stale builders
   unable to publish after takeover while avoiding changes to the existing
   explicit flush checkpoint contract.
3. Use one coalesced in-process wakeup per `PgMetaEngine`. A stale observation
   sets a boolean request and signals the coordinator; failed coordination
   retries use bounded exponential backoff with jitter while freshness remains
   stale. Successful `RETURNING` freshness observation clears the request state.
4. The coordinator acquires `replication_lock` only around source-cut capture
   and final validation/publication. The expensive local export runs without
   that lock so ordinary replication and foreground local writes can proceed.
5. A publication is accepted only when the lease owner and fencing token are
   still current, the lease has not expired, the metadata format matches, and
   the checkpoint version is monotonic. Losing the fence is a defer condition,
   not a stale-state reset.
6. The two-client PostgreSQL regression uses a synthetic remote head of 10000
   to cross the default inclusive freshness threshold without test-thread
   environment overrides leaking into worker threads.

## Outcomes

The completed change will provide one authoritative checkpoint generation for
concurrent clients, takeover after lease expiry, an exact serialized serving
cut, continued foreground/ordinary replication progress during serialization,
and stale-condition clearing only after an accepted atomic publication.

## Context

Relevant contracts are `docs/pg-sync-contract.md`,
`docs/juicefs-local-first-rust-contract.md`, `docs/agents-coding-guide.md`,
`crates/studiofs-meta/AGENTS.md`, and the SFS-704 Linear description. The
primary implementation seams are:

- `crates/studiofs-meta/src/pg_engine/checkpoint_freshness.rs` for stale
  observation and coordinator wakeups;
- `crates/studiofs-meta/src/pg_engine/metadata_io.rs` for checkpoint source
  capture and publication;
- `crates/studiofs-meta/src/pg_engine/lifecycle.rs` and `pg_engine.rs` for
  worker startup, shutdown, and configuration;
- `crates/studiofs-meta/src/pg_engine/sql.rs` and `schema.rs` for PostgreSQL
  operations and schema setup;
- `crates/studiofs-meta/src/pg_engine/tests/` for focused regressions and the
  unit fixture.

## Plan of Work

1. Add the schema table and SQL for short lease claim/release and fenced
   checkpoint publication. Extend schema validation and source-level SQL/schema
   tests without changing existing explicit checkpoint behavior.
2. Add a small coalescing state machine with deterministic unit tests. Wire a
   volume coordinator worker into `PgBackgroundWorkers`; ensure startup,
   shutdown, `Drop`, and worker-slot test helpers own the handle correctly.
3. Add a configured bounded lease/retry duration. On a stale observation, the
   worker claims the lease, captures the local version, event position, and
   remote byte baseline, exports the serving checkpoint outside
   `replication_lock`, reacquires the lock, verifies the source position did
   not move, and publishes with the fence. Failed attempts back off with a
   bounded jittered retry delay.
4. Add red/green tests for the accepted contract. PostgreSQL integration tests
   remain `#[ignore]` where a real service is required and must be run
   explicitly with `.env`; local unit tests must be nonzero and service
   failures must be reported separately from code failures.
5. Format and run the smallest relevant tests/checks and clippy targets, inspect
   `git diff --check` and the final diff, update the living plan, add an English
   Linear completion comment, and commit all intended task files with a
   `feat:` subject.

## Concrete Steps

### Schema and SQL

- Add `bootstrap_checkpoint_leases` to the fresh schema and required-table
  validation:

  ```sql
  CREATE TABLE IF NOT EXISTS bootstrap_checkpoint_leases (
      vol BIGINT PRIMARY KEY REFERENCES volumes(vol) ON DELETE CASCADE,
      owner TEXT NOT NULL,
      lease_expires_at TIMESTAMPTZ NOT NULL,
      fencing_token BIGINT NOT NULL CHECK (fencing_token >= 0),
      acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      renewed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
  );
  ```

- Add parameterized SQL that claims only an absent/expired lease or renews the
  current owner, increments the fence on takeover, and returns the owner,
  expiry, and token. Add conditional release and a fenced upsert that returns
  the existing freshness projection only when the lease fence is still valid.

### Coordinator and lifecycle

- Add `checkpoint_coordinator.rs` with the coalesced request state and the
  single-engine worker loop. A request is a boolean wakeup, not a counter, so
  multiple stale observations cannot queue duplicate builds.
- Extend `PgBackgroundWorkers` with the coordinator wakeup and join handle;
  start it only with background workers, and include it in bounded shutdown and
  `Drop` signaling.
- Signal the coordinator from `record_checkpoint_freshness_observation` only
  for a stale decision. Keep errors/reporting best-effort and leave stale state
  intact on claim, serialization, fence, or PostgreSQL failures.

### Exact cut and publication

- Reuse the local serving export. Capture the source version and remote byte
  baseline under the narrow lock, export bytes outside the lock, and verify the
  local version again before fenced publication.
- If the source version moved, do not publish the old payload; rearm the stale
  request for a later bounded retry. If the lease fence was lost, release is
  conditional and the request remains stale.
- Feed the successful `RETURNING` freshness row through the existing observer
  so the stale condition is cleared by authoritative PostgreSQL state.

## Validation

- First add/run the focused coalescing and SQL contract tests as red tests.
- Run focused `studiofs-meta` library tests for the new unit behavior and
  existing lifecycle/checkpoint tests.
- Run the explicitly named PostgreSQL ignored regressions with `.env` only if
  the local PostgreSQL service is available; never print credentials or DSNs.
- Run `cargo fmt --check` (or format only changed Rust files),
  `cargo check -p studiofs-meta`, targeted `cargo clippy -p studiofs-meta
  --all-targets -- -D warnings`, and `git diff --check`.
- Do not run workspace-wide/full suites. Native mounted acceptance is outside
  this metadata-focused change and remains a separate gate.

## Idempotence

Schema creation is `IF NOT EXISTS`. Lease claim/release are owner/fence
conditional. Publication is monotonic and fenced, so retries after a timeout
cannot overwrite a newer checkpoint or a new owner. The in-process request bit
coalesces duplicate signals and can be safely rearmed after a failed attempt.

## Artifacts

- This living plan.
- Changed Rust/schema/SQL sources and focused tests.
- Focused test/check output and final diff evidence.
- One English Linear update for SFS-704 and the implementation commit.

## Interfaces

The public `MetaEngine`/`MetaReplication` contracts remain unchanged. The
coordinator is internal to `PgMetaEngine`; the only durable interface added to
the shared PostgreSQL schema is the per-volume fenced lease table. Existing
explicit `flush_metadata()` checkpoint behavior remains compatible. The lease
table is part of the fresh required schema; existing databases without it are
expected to follow the canonical recreate/provisioning boundary rather than an
implicit runtime migration.

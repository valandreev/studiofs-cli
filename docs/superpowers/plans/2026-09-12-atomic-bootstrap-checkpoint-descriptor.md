# Atomic Bootstrap Checkpoint Descriptor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose one validated PostgreSQL bootstrap descriptor whose checkpoint bytes, replay floor, volume identity, format identity, checksum, and publication generation are read from one consistent record.

**Architecture:** Keep `metadata_state` as the atomic storage row and join it with `volumes` in one SELECT. Add the public descriptor/read operation to the existing `PgSnapshotBackend`, which has no local redb or live replication workers, then make runtime checkpoint pull and discovery consume the same validation path.

**Tech Stack:** Rust 2024, `studiofs-meta`, `tokio-postgres`, PostgreSQL, `serde_json`, `postcard`, and `sha2`.

**Spec:** `docs/superpowers/specs/2026-09-12-atomic-bootstrap-checkpoint-descriptor-design.md`

## Global Constraints

- PostgreSQL remains bootstrap/replication infrastructure; foreground mounted metadata remains local `redb`.
- `checkpoint_version` is the exact replay floor; later event-log reads start at `batch_version > checkpoint_version`.
- Descriptor fields must be read and published atomically; no second descriptor table or S3 artifact is introduced.
- Invalid, incomplete, mismatched, or corrupt descriptors fail closed with typed metadata errors.
- Do not implement SFS-703 freshness policy, SFS-704 coordinator, or SFS-705 atomic local DB installation.
- Do not print credentials, full DSNs, or secret-bearing format configuration.
- Run one focused Cargo test command at a time; a test result with `running 0 tests` is not validation.

---

### Task 1: Define the descriptor contract and red tests

**Files:**
- Modify: `crates/studiofs-meta/src/engine.rs` near `CheckpointDescriptorFailure`
- Modify: `crates/studiofs-meta/src/lib.rs` near `validate_checkpoint_descriptor`
- Test: `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs`

**Interfaces:**
- Produces `pub struct BootstrapCheckpointDescriptor` with fields `volume_id: i64`, `volume_uuid: String`, `format_info_bytes: Vec<u8>`, `metadata_format_version: u16`, `remote_head_version: i64`, `checkpoint_version: i64`, `checkpoint_bytes: Vec<u8>`, `checkpoint_sha256: Vec<u8>`, `publication_state: String`, and `publication_generation: i64`.
- Produces `BootstrapCheckpointDescriptor::validate(&self) -> MetaResult<()>`.
- Keeps the existing scalar `validate_checkpoint_descriptor` behavior for callers that only have checkpoint bytes and descriptor scalar fields.

- [x] **Step 1: Write the failing test**

Add a unit test that builds a valid descriptor from a serialized default `ServingCheckpoint` and asserts validation succeeds, then changes the UUID, format info identity, remote head, or checkpoint version and asserts typed failure. Add a separate test asserting the descriptor checksum and generation rules still reject pending publication, zero generation, missing checksum, and mismatched bytes.

The test should exercise the public shape directly:

    let descriptor = BootstrapCheckpointDescriptor {
        volume_id: 7,
        volume_uuid: "volume-uuid".to_string(),
        format_info_bytes: serde_json::to_vec(&format_info).unwrap(),
        metadata_format_version: CURRENT_FORMAT_VERSION,
        remote_head_version: 3,
        checkpoint_version: 3,
        checkpoint_bytes,
        checkpoint_sha256,
        publication_state: "published".to_string(),
        publication_generation: 1,
    };
    descriptor.validate().unwrap();

- [x] **Step 2: Run the test to verify it fails**

Run from `/Volumes/Storage/dev/studiofs`:

    cargo test -p studiofs-meta bootstrap_checkpoint_descriptor

Expected: compilation fails because `BootstrapCheckpointDescriptor` does not yet exist. Do not keep production code written before this failure.

- [x] **Step 3: Implement the minimal contract**

Add the descriptor type without a derived `Debug` implementation because it contains serialized format information and checkpoint bytes. In `validate`, check positive volume identity, current PG format version, `0 <= checkpoint_version <= remote_head_version`, `published` state, positive generation, SHA-256 equality, valid serialized `FormatInfo`, matching format version, matching volume UUID, PostgreSQL backend, matching PostgreSQL volume ID, and decodable `ServingCheckpoint` bytes. Add only the error variants needed to identify invalid identity/version/checkpoint relationships.

- [x] **Step 4: Run the focused tests to verify green**

Run:

    cargo test -p studiofs-meta bootstrap_checkpoint_descriptor

Expected: the new tests and the existing checkpoint validator tests pass with a nonzero test count.

- [x] **Step 5: Run formatting and diff checks**

Run:

    cargo fmt --all -- --check
    git diff --check

Expected: both commands succeed.

### Task 2: Add the one-record PostgreSQL read API

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine/sql.rs` beside metadata-state queries
- Modify: `crates/studiofs-meta/src/pg_engine/metadata_io.rs` with `PgSnapshotBackend` implementation
- Modify: `crates/studiofs-meta/src/pg_engine.rs` exports if needed
- Test: `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs` or a focused new PG test section

**Interfaces:**
- Produces `pub fn PgSnapshotBackend::read_bootstrap_checkpoint(&self) -> MetaResult<BootstrapCheckpointDescriptor>`.
- The SQL returns volume ID/UUID, metadata format, remote head, checkpoint version/bytes, checksum, format info, publication state, and generation from one `volumes JOIN metadata_state` row.
- The method uses the existing bounded PostgreSQL command helper and does not open `PgMetaEngine`.

- [x] **Step 1: Write the failing SQL/API test**

Add a pure SQL-shape assertion that the bootstrap query selects `v.vol`, `v.uuid`, all descriptor columns, joins `metadata_state`, and filters by volume ID. Add an ignored PostgreSQL integration test that opens `PgSnapshotBackend`, calls `read_bootstrap_checkpoint`, and asserts the returned volume identity, head, checkpoint version, bytes, and checksum match the seeded row.

- [x] **Step 2: Run the pure test to verify it fails**

Run:

    cargo test -p studiofs-meta bootstrap_checkpoint_query

Expected: compilation or assertion failure because the query constant/API is absent.

- [x] **Step 3: Implement the minimal query and mapper**

Add a single query similar to:

    SELECT v.vol, v.uuid, ms.metadata_format_version, ms.version,
           ms.checkpoint_version, ms.snapshot, ms.checkpoint_sha256,
           ms.format_info, ms.checkpoint_publication_state,
           ms.checkpoint_generation
    FROM volumes AS v
    INNER JOIN metadata_state AS ms ON ms.vol = v.vol
    WHERE v.vol = $1

Map nullable legacy-sensitive columns into validation inputs without accepting a missing value. Construct `BootstrapCheckpointDescriptor`, call `validate`, and return it. Map no-row to a redacted storage/bootstrap error. Preserve the existing command timeout and remote error classification.

- [x] **Step 4: Run the pure test to verify green**

Run:

    cargo test -p studiofs-meta bootstrap_checkpoint_query

Expected: the SQL-shape test passes. If the ignored integration test is run, it requires `STUDIOFS_TEST_PG_DSN`; without that service it remains explicitly environment-gated.

- [x] **Step 5: Run the metadata crate check**

Run:

    cargo check -p studiofs-meta

Expected: the new public type and API compile without warnings.

### Task 3: Replace split checkpoint reads with the shared API

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine/metadata_io.rs` in `pull_snapshot_locked_inner`
- Modify: `crates/studiofs/src/cli/discovery.rs` in remote volume lookup and discovery state mapping
- Test: `crates/studiofs-meta/src/pg_engine/tests/lifecycle.rs`, `crates/studiofs/src/cli/discovery.rs`

**Interfaces:**
- `pull_snapshot_locked_inner` obtains one validated descriptor plus its decoded checkpoint, compares the remote versions with the local cursor, imports the checkpoint, and persists `checkpoint_version` as the replay cursor.
- Discovery obtains the same descriptor through `PgSnapshotBackend` and no longer duplicates descriptor validation or field mapping.
- Existing discovery local-install behavior remains unchanged; pure atomic local installation and skipping runtime open remain SFS-705.

- [x] **Step 1: Write the regression**

Add a test that verifies the shared bootstrap descriptor exposes `checkpoint_version` as the replay floor and `remote_head_version` separately. Add a source-shape regression or focused helper test that fails if checkpoint pull still uses the old two-query `SELECT_METADATA_VERSION` followed by `SELECT_METADATA_SNAPSHOT` path.

- [x] **Step 2: Run the regression and verify the split path is absent**

Run:

    cargo test -p studiofs-meta bootstrap_checkpoint_query

The SQL-shape test passed after wiring; the old `SELECT_METADATA_SNAPSHOT`
constant was removed, and the focused source search found no remaining split
checkpoint read path.

- [x] **Step 3: Wire the shared descriptor**

Preflight the remote head and checkpoint versions, then read one validated
descriptor plus its decoded checkpoint in `pull_snapshot_locked_inner`; keep the
existing import modes and regular-file reconciliation. Update discovery to
select an initialized volume, open `PgSnapshotBackend` with the PostgreSQL DSN
and volume ID, and use `read_bootstrap_checkpoint` for the descriptor payload.

- [x] **Step 4: Run focused tests to verify green**

Run:

    cargo test -p studiofs-meta bootstrap_checkpoint_query_contains_all_descriptor_columns
    cargo test -p studiofs-meta metadata_head_query_contains_checkpoint_version
    cargo test -p studiofs --bin studiofs discover_parses_name_and_meta_dsn

Expected: all commands execute nonzero test counts and pass.

- [x] **Step 5: Run package checks**

Run:

    cargo check -p studiofs-meta
    cargo check -p studiofs
    cargo fmt --all -- --check
    git diff --check

Expected: all commands succeed.

### Task 4: Final validation and plan closeout

**Files:**
- Modify: `docs/superpowers/plans/2026-09-12-atomic-bootstrap-checkpoint-descriptor.md`

**Interfaces:**
- No new runtime interface; this task records evidence and remaining environment gates.

- [x] **Step 1: Run focused tests once more**

Run:

    cargo test -p studiofs-meta checkpoint_descriptor
    cargo test -p studiofs-meta schema_declares_incompatible_sync_boundary_and_checkpoint_descriptor
    cargo test -p studiofs --bin studiofs discover_parses_name_and_meta_dsn

Expected: every command runs at least one test and passes.

- [x] **Step 2: Run lint gates for changed crates**

Run:

    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    cargo clippy -p studiofs --all-targets -- -D warnings

Expected: both commands pass. A configured PostgreSQL service is still required for ignored integration tests.

- [x] **Step 3: Inspect the final diff**

Run:

    git diff --stat
    git diff -- crates/studiofs-meta/src/engine.rs crates/studiofs-meta/src/lib.rs crates/studiofs-meta/src/pg_engine.rs crates/studiofs-meta/src/pg_engine/sql.rs crates/studiofs-meta/src/pg_engine/metadata_io.rs crates/studiofs/src/cli/discovery.rs

Confirm the diff only adds the descriptor contract, one-record read, shared consumers, tests, and this plan/spec documentation.

- [x] **Step 4: Update this living plan**

Mark completed steps with UTC timestamps, record any test or PostgreSQL-service surprises, add the final decision/evidence to the decision log, and summarize outcomes. Do not mark PostgreSQL integration acceptance complete when the service was unavailable.

## Progress

- [x] (2026-09-12 17:28Z) Read SFS-702, its dependency SFS-696, follow-up issues SFS-703/704/705/711, repository contracts, scoped metadata instructions, and current source paths.
- [x] (2026-09-12 17:28Z) Confirmed the current branch is `feature/sfs-702-define-and-publish-an-atomic-bootstrap-checkpoint-descriptor` and the worktree was clean before edits.
- [x] (2026-09-12 17:28Z) Received user approval for the descriptor/API design.
- [x] (2026-09-12 18:16Z) Defined and validated the typed descriptor contract; the initial focused test was RED, then the descriptor and typed negative cases passed.
- [x] (2026-09-12 18:16Z) Added the one-record PostgreSQL read API and validated it against the configured local PostgreSQL service.
- [x] (2026-09-12 18:16Z) Replaced split runtime checkpoint reads and discovery field mapping with the shared descriptor path.
- [x] (2026-09-12 18:16Z) Completed focused tests, package checks, Clippy, formatting, diff validation, and final diff inspection.
- [x] (2026-09-12 18:51Z) Applied the over-engineering review: removed unreachable discovery error/decode wrapper, redundant format-version comparison, redundant helper return, and duplicate source-shape test.
- [x] (2026-09-12 20:20Z) Applied the current review: removed runtime double postcard decode, made version and descriptor row reads fallible, filtered discovery to initialized volumes, restored missing-descriptor guidance, made the invariant explicit, and updated the verification commands.
- [x] (2026-09-12 20:51Z) Removed the credential-bearing DSN from the descriptor fixture and recorded the exact focused validation commands and results.

## Surprises & Discoveries

- The SFS-696 implementation already stores checksum, publication state, and generation in `metadata_state`, but it has no typed descriptor API.
- `pull_snapshot_locked_inner` preflights remote versions before fetching the potentially large checkpoint payload, then consumes one validated descriptor record.
- `PgSnapshotBackend` already provides the no-local-redb PostgreSQL boundary needed by this issue.
- The first `studiofs` test build required a full crate link and took several minutes; subsequent checks were incremental.
- The ignored PostgreSQL descriptor test ran successfully with the configured local service, so this acceptance gate was not environment-gated in this run.

## Decision Log

- Decision: Reuse `metadata_state` and `PgSnapshotBackend` rather than adding a descriptor table or backend. Rationale: the row already contains the checkpoint fields and the backend already avoids opening the live runtime. Date/Author: 2026-09-12, Codex.
- Decision: Treat `checkpoint_version` as the replay floor and carry `remote_head_version` separately. Rationale: the checkpoint is a completed remote state at one event-log version, while the head may advance after it. Date/Author: 2026-09-12, Codex.
- Decision: Keep SFS-705's atomic local DB installation and no-runtime-open behavior out of this change. Rationale: Linear defines it as a separate follow-up consuming this API. Date/Author: 2026-09-12, Codex.

## Outcomes & Retrospective

Implemented `BootstrapCheckpointDescriptor` with fail-closed identity, format,
version, publication, checksum, format-info, and serving-checkpoint validation.
`PgSnapshotBackend::read_bootstrap_checkpoint` now reads all fields with one
joined query and validates before returning. Runtime pull and discovery share
the API; foreground metadata remains local redb and SFS-703/704/705/711 remain
out of scope.

Validation evidence (2026-09-12):

- `cargo test -p studiofs-meta checkpoint_descriptor` — 7 passed.
- `cargo test -p studiofs-meta bootstrap_checkpoint_query_contains_all_descriptor_columns` — 1 passed.
- `cargo test -p studiofs-meta metadata_head_query_contains_checkpoint_version` — 1 passed.
- `cargo test -p studiofs-meta startup_snapshot_replay` — 3 passed.
- `cargo test -p studiofs-meta startup_snapshot_skips_stale_checkpoint_without_downloading_payload -- --ignored --nocapture` — 1 passed.
- `cargo test -p studiofs-meta invalid_bootstrap_descriptor_does_not_discard_healthy_pool_connection -- --ignored --nocapture` — 1 passed.
- `cargo test -p studiofs-meta pg_snapshot_backend_reads_a_validated_bootstrap_descriptor -- --ignored --nocapture` — 1 passed.
- `cargo test -p studiofs --bin studiofs discover_parses_name_and_meta_dsn` — 1 passed.
- `cargo test -p studiofs --bin studiofs discover_volume_query_requires_metadata_state` — 1 passed.
- `cargo check -p studiofs-meta` and `cargo check -p studiofs` — passed.
- `cargo clippy -p studiofs-meta --all-targets -- -D warnings` and `cargo clippy -p studiofs --all-targets -- -D warnings` — passed; the latter emitted existing Xcode environment warnings but exited successfully.
- `cargo fmt --all -- --check` and `git diff --check` — passed.

The PostgreSQL ignored tests passed against the configured local service. The
implementation and prior review amendments are committed on the feature
branch; the current inline-warning fixes remain uncommitted. No Linear status
change was made. The follow-up simplification review removed 20 lines without
changing behavior or coverage of the remaining descriptor tests.

Review follow-up (2026-09-12): accepted the lightweight remote-head guard,
bounded discovery query, corrected not-found message, dead assignment removal,
complete descriptor invariant coverage, redacted `Debug`, and SQL test rename.
The serialized descriptor contract remains unchanged: validation still decodes
checkpoint bytes before returning, discovery keeps its separate one-time pool
boundary, `FormatInfo` remains a typed redacted aggregate failure, checksum
bytes remain `Vec<u8>`, and the public format version remains `u16`.

Second review pass (2026-09-12): moved descriptor row mapping and validation
outside the checked-out PostgreSQL connection, added a head-plus-checkpoint
version preflight to skip stale snapshot BLOBs, routed runtime reads through
the engine runtime and pool, made row mapping fallible with `try_get`, and
used `Result::flatten` in discovery. Service-backed regressions cover pool
health after logical descriptor rejection and skipping a stale checkpoint with
invalid payload bytes.

Current review pass (2026-09-12): `validate_and_decode` now returns the
checkpoint consumed by runtime, so the startup path deserializes postcard once.
Version helpers and descriptor mapping propagate PostgreSQL row-conversion
errors, discovery ignores uninitialized higher-volume rows and preserves
missing-descriptor remediation, and the living plan references nonzero tests.

Inline warning pass (2026-09-12): removed the credential-bearing fixture DSN,
recorded the exact validation commands and results, replaced the missing
descriptor string match with a typed metadata error, kept discovery failures
fallible at the format boundary, and made volume-id row conversion fallible.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/schema.rs` defines the PostgreSQL
`metadata_state` row. `crates/studiofs-meta/src/pg_engine/sql.rs` stores the
existing metadata queries. `crates/studiofs-meta/src/pg_engine/metadata_io.rs`
serializes and imports serving checkpoints. `PgSnapshotBackend` in
`crates/studiofs-meta/src/pg_engine.rs` opens PostgreSQL without local redb or
replication workers. `crates/studiofs/src/cli/discovery.rs` uses the shared
descriptor API for remote checkpoint reads. The local `RedbMetaEngine` is the
foreground serving view and is not replaced by this work.

## Plan of Work

First add the public descriptor and validation tests. Then add a single joined
PostgreSQL read through `PgSnapshotBackend`, map and validate all fields before
returning, and finally make runtime checkpoint pull and discovery use it. Keep
the existing one-row checkpoint upsert as the atomic publication boundary and
leave freshness, coordination, and local installation follow-ups untouched.

## Concrete Steps

All commands run from `/Volumes/Storage/dev/studiofs`. Use one Cargo command at
a time to avoid target-directory locks. Keep PostgreSQL credentials in the
environment only; never paste `.env` or DSNs into artifacts.

## Validation and Acceptance

Portable acceptance requires the typed descriptor and SQL-shape tests, the
runtime/discovery focused tests, package checks, Clippy with `-D warnings`,
formatting, and `git diff --check`. Service-backed acceptance requires the
ignored PostgreSQL test to read a seeded descriptor and prove its identity,
checksum, head, checkpoint version, and publication metadata are returned as
one validated result. If `STUDIOFS_TEST_PG_DSN` is unavailable, report that
integration proof as environment-gated rather than claiming it passed.

## Idempotence and Recovery

The change is additive and can be re-run from the same branch. Tests use the
existing target-local fixtures and do not modify user data. If a PostgreSQL
fixture fails, preserve the error as an infrastructure result and continue
with portable tests; do not weaken validation or add sleeps/retries.

## Artifacts and Notes

The design artifact is
`docs/superpowers/specs/2026-09-12-atomic-bootstrap-checkpoint-descriptor-design.md`.
The living implementation record is this plan. No credentials, full DSNs, or
generated PostgreSQL dumps belong in either file.

## Interfaces and Dependencies

The descriptor uses the existing `FormatInfo`, `ServingCheckpoint`,
`MetaResult`, `MetaError`, `PgSnapshotBackend`, and bounded PostgreSQL command
helpers. `sha2::Sha256` validates exact checkpoint bytes, `serde_json` validates
format information, and `postcard` validates the serving checkpoint payload.

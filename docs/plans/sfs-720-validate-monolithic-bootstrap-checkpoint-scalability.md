# Validate monolithic bootstrap checkpoint scalability

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

SFS-720 needs evidence that a fresh client can bootstrap a realistically large
live namespace from the existing single `ServingCheckpoint` before the project
adds a more complex sharded artifact format. The result will be a repeatable
ignored PostgreSQL benchmark that emits checkpoint size, export/serialization
time, PostgreSQL descriptor-read time, local import time, and observed process
RSS for several live namespace sizes. A repository document will record the
measurement ceiling and the decision to keep the monolithic representation or
open a narrowly specified follow-up.

## Progress

- [x] (2026-09-21) Read SFS-720 and its completed SFS-718 dependency from Linear.
- [x] (2026-09-21) Traced the current export, PostgreSQL bootstrap-read, decode,
  and redb import path.
- [x] (2026-09-21) Add the focused fixture contract test and benchmark harness.
- [x] (2026-09-21) Run the portable fixture proof.
- [x] (2026-09-21) Run the ignored PostgreSQL measurements in release mode for
  1,000, 10,000, 50,000, and 100,000 entries with three samples each.
- [x] (2026-09-21) Record the measured 100,000-entry ceiling, workload
  boundary, and monolithic-checkpoint decision in repository performance
  documentation.
- [x] (2026-09-21) Inspect the final diff and leave changes uncommitted for
  review.

## Surprises & Discoveries

- The indexed `studiofs-bench` repository is a generic mounted-I/O benchmark
  and has no checkpoint/bootstrap workload; SFS-720 belongs in the metadata
  crate's existing test harness.
- `ServingCheckpoint` already contains only remote-visible inode, dentry,
  regular-file base, symlink, xattr, and ACL vectors. The production path is
  `RedbMetaEngine::export_serving_checkpoint_bytes` -> PostgreSQL
  `metadata_state.snapshot` -> `PgSnapshotBackend` validation/decode -> local
  checkpoint import.

## Decision Log

- Decision: Measure the existing checkpoint path in `studiofs-meta` instead of
  introducing a new artifact abstraction. Rationale: the issue asks for a
  scalability decision, and the current implementation already exposes the
  exact seams that need evidence. Date/Author: 2026-09-21, Codex.
- Decision: Seed remote-visible metadata through the existing redb replication
  state and use the real PostgreSQL descriptor read and redb byte import in the
  benchmark. Rationale: this exercises the production representation without
  making a benchmark depend on mounted I/O or speculative sharding code.
  Date/Author: 2026-09-21, Codex.
- Decision: Keep the benchmark ignored and require `STUDIOFS_TEST_PG_DSN` for
  PostgreSQL measurements. Rationale: the repository contract treats service
  availability as an environment prerequisite, not as a reason to weaken the
  normal test suite. Date/Author: 2026-09-21, Codex.
- Decision: Run performance measurements with Cargo's release profile.
  Rationale: the debug-profile fixture preparation was not practical at 10,000
  entries and would not provide representative latency evidence. Date/Author:
  2026-09-21, Codex.

## Outcomes & Retrospective

The portable fixture proof and helper regressions are green. The release
PostgreSQL benchmark completed three samples at 1,000, 10,000, 50,000, and
100,000 entries; 100,000 is the measured ceiling for the flat published-file
workload. The default monolithic representation remains unchanged, and the
performance document records the exact JSONL evidence and workload boundary.
The fixture seed path was changed from per-event namespace replay to batched
namespace creation plus batched serving/base publication after the original
generator measured seed cost instead of checkpoint cost.

## Context and Orientation

`ServingCheckpoint` is the serialized remote-visible metadata cut stored in the
single PostgreSQL `metadata_state.snapshot` column. Its source is the local
redb serving projection. `PgSnapshotBackend::read_bootstrap_checkpoint` reads
and validates that descriptor, and a fresh client imports the decoded bytes into
another local redb database before replaying later events.

The implementation lives in `crates/studiofs-meta/src/snapshot.rs`,
`crates/studiofs-meta/src/redb_engine/replication.rs`,
`crates/studiofs-meta/src/pg_engine/metadata_io.rs`, and
`crates/studiofs-meta/src/redb_engine/snapshots.rs`. Existing PostgreSQL test
helpers in `crates/studiofs-meta/src/pg_engine/tests/mod.rs` already provision a
temporary volume and clean it up. The benchmark should reuse these helpers and
remain inside `studiofs-meta`; no mounted scenario or production API change is
needed.

## Plan of Work

Add `checkpoint_scalability.rs` to the existing PostgreSQL test module. First,
build a small remote-visible namespace fixture and assert that its serialized
checkpoint contains the requested number of live entries. Then add an ignored
benchmark that accepts namespace sizes and sample count through environment
variables, creates representative live files, times export/serialization,
updates the PostgreSQL checkpoint row, times the real descriptor read, and
times import into fresh redb databases. Emit one redacted JSON record per size
so results can be copied into the SFS-720 performance document without exposing
the DSN.

Use the existing `sysinfo` workspace dependency as a test-only dependency to
sample current-process resident memory in bytes while a size is measured. Keep
the measurement code test-only; production checkpoint behavior remains
unchanged.

Add `docs/perf/sfs-720-monolithic-bootstrap-checkpoint.md` with the workload
shape, exact commands, host/service prerequisites, captured measurements, the
largest accepted ceiling, and a decision. State clearly when PostgreSQL or
native low-resource hardware was unavailable.

## Concrete Steps

From `/Volumes/Storage/dev/studiofs`, run the focused fixture test first and
expect it to fail before the fixture helper exists. Implement one vertical slice
until it passes, then run the ignored benchmark with the configured
`STUDIOFS_TEST_PG_DSN` and a bounded namespace-size set. Keep generated JSON
under `target/perf/sfs-720/` and do not print the DSN.

Use focused checks only:

    cargo test -p studiofs-meta sfs720_checkpoint_fixture_contains_requested_live_namespace --lib -- --nocapture
    cargo test --release -p studiofs-meta sfs720_checkpoint_scalability --lib -- --ignored --nocapture --test-threads=1
    cargo fmt --all -- --check
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    git diff --check

If the ignored PostgreSQL command cannot connect, retain the portable fixture
result and report the service as unavailable rather than treating the skipped
measurement as a passing performance result. The current evidence is recorded
in `docs/perf/sfs-720-monolithic-bootstrap-checkpoint.md`.

## Validation and Acceptance

The fixture test must execute nonzero tests and prove that the requested live
namespace is represented in the checkpoint. The ignored benchmark must report
nonzero samples for each requested namespace and include checkpoint bytes,
export/serialization duration, PostgreSQL read/decode duration, local import
duration, and observed RSS. The document must define an explicit ceiling based
on the largest measured namespace and conclude either that the current
monolithic checkpoint is sufficient or provide a concrete versioned, integrity-
checked, atomically published follow-up requirement.

No benchmark result may be presented as native low-resource acceptance unless
the required PostgreSQL service and host profile were actually available.

## Idempotence and Recovery

Each benchmark volume uses a unique test name and is deleted by the existing
`PgTestVolume` cleanup. Local databases use workspace `target` temporary
directories. Re-running the focused commands is safe; if a run is interrupted,
remove only its own `target/perf/sfs-720/` output before retrying.

## Artifacts and Notes

The durable source artifact is the SFS-720 performance document and its JSON
measurement records under `target/perf/sfs-720/` during execution. The final
source diff should contain the benchmark harness, its focused fixture proof,
the decision document, and this living plan; generated measurement output stays
under `target/`.

## Interfaces and Dependencies

The benchmark reuses these existing interfaces:

- `RedbMetaEngine::export_serving_checkpoint_bytes` for production checkpoint
  construction and serialization.
- `PgSnapshotBackend::open` and `read_bootstrap_checkpoint` for the PostgreSQL
  bootstrap descriptor path.
- `RedbMetaEngine::import_serving_checkpoint_bytes` for fresh-client local
  import.
- `MetaFsOps`/`MetaPayloadObjects` and the existing replication event helpers
  for constructing remote-visible namespace state.
- `sysinfo::System` and `Process::memory` as a test-only resident-memory
  measurement dependency.

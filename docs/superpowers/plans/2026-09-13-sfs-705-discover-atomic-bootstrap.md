# SFS-705 atomic discover bootstrap

This ExecPlan is a living document. It follows `.agent/PLANS.md` and records
the implementation decisions and validation evidence for SFS-705.

## Purpose / Big Picture

Make `studiofs discover` install exactly one validated PostgreSQL serving
checkpoint into a local redb database without opening `PgMetaEngine` or
starting event-log catch-up. The active local database must appear only after
the complete temporary database is durable; a failed import or durability step
must leave no active partial database.

## Progress

- [x] 2026-09-13: Read the Linear SFS-705 contract, completed blockers SFS-702
  and SFS-698, repository contracts, scoped metadata instructions, and the
  existing descriptor/discovery implementation.
- [x] 2026-09-13: Confirmed the branch is
  `feature/sfs-705-make-discover-a-pure-atomic-checkpoint-bootstrap` and the
  worktree is clean.
- [x] 2026-09-13: Added focused discovery tests for atomic installation,
  replay-cursor persistence, no runtime-engine open, and failed-install
  cleanup.
- [x] 2026-09-13: Implemented temporary redb installation, durable barrier,
  and atomic no-replace active-path hard link.
- [x] 2026-09-13: Focused tests, checks, formatting, final diff inspection, and
  commit preparation completed.
- [x] 2026-09-13: Processed the attached review with red-green regressions;
  fixed namespace cursor/root/early-validation/error-typing issues, moved
  bootstrap state ownership into `studiofs-meta`, and made final installation
  no-replace under concurrent creators.

## Surprises & Discoveries

- SFS-702 already provides `PgSnapshotBackend::read_bootstrap_checkpoint`,
  which validates volume identity, format, publication state, checksum, and
  checkpoint bytes before discovery receives the descriptor.
- `RedbMetaEngine::import_serving_checkpoint_bytes` replaces remote serving
  state but intentionally does not persist PostgreSQL replay counters; the
  normal runtime path adds the replay cursor after import. Discover must do
  that directly because it must not construct `PgMetaEngine`.
- `RedbMetaEngine` exposes the strict local durability barrier through the
  public `MetaReplication::sync_local_metadata` contract. This is the existing
  redb durability boundary and avoids adding an OS-specific fsync abstraction.

## Decision Log

- Decision: Use `tempfile::TempDir` inside the metadata directory and hard-link
  a completed database file into the active path. Rationale: the temporary
  path is unique, remains on the same filesystem, and hard-link creation is an
  atomic no-replace install; the temporary directory is automatically cleaned
  when import or durability fails.
- Decision: Keep the existing refusal when the active `<name>.db` already
  exists. Rationale: SFS-705 requires explicit replacement rules and must not
  silently overwrite a valid unrelated local volume.
- Decision: Persist `pgLastAppliedBatchVersion` and
  `pgLastAppliedNamespaceBatchVersion` at exactly the descriptor's non-negative
  `checkpoint_version`, plus `pgFreshClientPending = 1` before
  installation. Rationale: the next normal mount must start catch-up after the
  imported checkpoint while retaining the fresh-client read-only gate.
- Decision: Add one metadata-engine bootstrap method but no new remote read
  path. Rationale: SFS-702's validated descriptor API remains the remote
  boundary, while `studiofs-meta` owns the checkpoint counters and root
  invariant instead of exposing their representation to the CLI adapter.

## Outcomes & Retrospective

The implementation meets the portable SFS-705 acceptance checks: discovery
imports the validated serving checkpoint into a temporary redb database,
persists the exact checkpoint and namespace replay cursors plus the
fresh-client marker, forces the existing immediate durability barrier, and
only then creates the active name with a no-replace hard link. It no longer
constructs `PgMetaEngine` or starts event-log catch-up. Invalid, rootless, or
negative-version inputs leave the active path and temporary directory clean.

Focused validation passed:

- `cargo test -p studiofs --bin studiofs discover_` — 10 passed, 0 failed.
- `cargo test -p studiofs-meta initialize_discovered_postgres_checkpoint --lib` — 3 passed, 0 failed.
- `cargo test -p studiofs --bin studiofs hard_link_unsupported` — 1 passed, 0 failed.
- `cargo check -p studiofs-meta -p studiofs` — passed.
- `cargo clippy -p studiofs-meta -p studiofs --all-targets -- -D warnings` —
  passed using a task-specific temporary Cargo target directory because the
  repository `target` directory returned Windows access-denied errors.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.

PostgreSQL service and mounted-E2E acceptance were not run; they remain
environment-gated and are outside the focused validation boundary.

## Review Disposition

The attached review reports were checked against the current implementation and the
PostgreSQL sync contract:

- Accepted: initialize both replay cursors, reject a rootless imported
  checkpoint, validate the checkpoint version before staging, preserve typed
  `MetaError` propagation, and add focused regressions.
- Accepted with a narrower implementation: encapsulate checkpoint bootstrap in
  `RedbMetaEngine::initialize_discovered_postgres_checkpoint`; the CLI now owns only
  path staging and installation.
- Accepted: use `hard_link` for the final same-filesystem install. It creates
  the active name atomically and fails with an existing-target error if a
  concurrent process wins the name, preserving the no-silent-replacement
  rule on supported filesystems.
- Accepted: map the no-replace install race to the existing domain message and
  report an explicit filesystem-capability message for `Unsupported` hard-link
  failures. A platform-specific fallback is not included because the portable
  path must retain atomic no-replace semantics.
- Accepted: validate that the imported root inode is a directory, batch the
  three PG bootstrap counters in one local write transaction, and make the
  bootstrap API explicitly PostgreSQL-only with a backend guard.
- Accepted: add direct `studiofs-meta` tests for successful initialization,
  malformed/root-invalid checkpoints, backend rejection, and the durability
  barrier; extend CLI tests to cover install-race mapping and successful
  temporary-directory cleanup.
- Accepted from the follow-up review: reject checkpoint versions above the
  PostgreSQL `i64` range at the metadata seam, reject missing or blank
  PostgreSQL configuration before checkpoint import, inspect the active path
  with `symlink_metadata`, and sync the parent metadata directory on Unix
  after installation. Windows hard-link failures reported as
  `ERROR_INVALID_FUNCTION` or `ERROR_NOT_SUPPORTED` are mapped to the same
  actionable capability message as `ErrorKind::Unsupported`.
- Not accepted from the follow-up review: replacing the hard-link install with
  `std::fs::rename` on Windows. The standard-library implementation uses
  `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`, which would silently replace
  an existing active database and violate SFS-705. A broad `DiscoveryError`
  variant refactor and public exposure of internal counter-key strings are
  likewise outside the behavior required here; neither has a production
  caller or fixes a demonstrated failure.
- Not accepted: an abort-on-drop guard for the PostgreSQL connection task.
  The current code aborts and awaits the task on every normal return,
  including timeout; the review's remaining case is an uncaught panic, not a
  timeout result, and no reachable panic path was identified in this helper.
- Not added: a separate non-PostgreSQL-format test. The branch is pre-existing
  descriptor validation in `load_remote_discovery_state`, not part of the new
  local atomic-install behavior; exercising it requires the PostgreSQL-backed
  descriptor seam.

## Context and Orientation

`crates/studiofs/src/cli/discovery.rs` resolves a PostgreSQL volume name,
reads the validated bootstrap descriptor through `PgSnapshotBackend`, and
installs local metadata. `crates/studiofs-meta/src/pg_engine/metadata_io.rs`
contains the shared descriptor read/validation path. The redb snapshot import
is implemented by `RedbMetaEngine` in
`crates/studiofs-meta/src/redb_engine/snapshots.rs`; its local durability
barrier is in `crates/studiofs-meta/src/redb_engine.rs`. Normal mount opens the
local database through `PgMetaEngine`, so discover must stop before that
boundary.

## Plan of Work

1. Extend `DiscoveredState` with the descriptor checkpoint version and retain
   the validated format/checkpoint bytes already obtained from PostgreSQL.
2. Replace direct active-file creation in `create_local_snapshot` with a
   unique temporary database. Ask `RedbMetaEngine` to import the checkpoint,
   persist format/root and fresh-client/replay state, and force the local
   durability barrier. Close redb and create the active name with a
   same-filesystem no-replace hard link.
3. Add focused unit tests that prove the helper succeeds without a usable
   PostgreSQL runtime URL, persists the exact replay cursor and marker, refuses
   an existing active database, and removes failed temporary installs without
   leaving an active database.
4. Update command documentation/plan evidence only where current transitional
   behavior is contradicted by the new implementation. Run focused package
   tests and checks; leave mounted/service acceptance explicitly reported when
   prerequisites are unavailable.

## Concrete Steps

All commands run from `E:\dev\studiofs-cli`.

- Add tests before the implementation and run:
  `cargo test -p studiofs --bin studiofs discover_`
- Implement the temporary install and run the same focused tests.
- Run `cargo fmt --all -- --check`, `cargo check -p studiofs`,
  `cargo clippy -p studiofs --all-targets -- -D warnings`, and
  `git diff --check`.
- Do not run workspace-wide or full ignored suites.

## Validation and Acceptance

Portable acceptance requires nonzero focused discovery tests proving that the
active path is absent on import failure, the installed local counter equals
`checkpoint_version`, and the installation completes with no runtime engine
open. Compilation, Clippy, formatting, and diff checks must pass. PostgreSQL
and mounted-E2E acceptance remain separate environment-gated checks unless the
required services are available.

## Idempotence and Recovery

An existing active database (including a dangling symlink) is rejected before
temporary work begins. A temporary directory owns the in-progress database and
removes it on every failure path. The hard-link install occurs only after
redb's immediate durability barrier and after the engine handle is dropped; an
active target that appears during staging makes the link fail instead of
replacing it. Unix also syncs the containing directory after creating the
active name.

## Artifacts and Notes

The implementation artifact is `crates/studiofs/src/cli/discovery.rs`; this
living plan records decisions and validation. Credentials, DSNs, and secret
format fields must not be copied into logs, tests, or this plan.

## Interfaces and Dependencies

Use the existing `PgSnapshotBackend::read_bootstrap_checkpoint` and
`RedbMetaEngine::import_serving_checkpoint_bytes` contracts, plus the new
`RedbMetaEngine::initialize_discovered_postgres_checkpoint` local bootstrap seam. Use
the already-declared `tempfile` dependency for unique same-filesystem
temporary directories. No new dependency or remote read path is expected.

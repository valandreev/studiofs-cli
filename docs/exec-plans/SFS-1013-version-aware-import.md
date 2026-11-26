# Validate compatibility before metadata import

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

An operator can attach a fresh client or restore a backup with a supported
metadata history. Refused formats report the blocking identity before the
client installs a local database or commits restored volume metadata.

## Progress

- [x] (2026-11-26) Fetch and branch from main at ead232a1 and trace the import paths.
- [x] (2026-11-26) Reproduce acceptance of an unsupported event, then implement validation.
- [x] (2026-11-26) Check baseline and test-successor backup restore on PostgreSQL and prove transactional rollback and retry.
- [x] (2026-11-26) Verify discovery leaves no active database on an unsupported event and a failed fresh-client marker clear leaves the mount read-only.
- [x] (2026-11-26) Address PR #1065 re-review: carry all published checkpoint encodings at the dumped cut in manifest v5 (`checkpoints` list + `checkpoint_encodings`), require `STUDIOFS_TEST_BASELINE_BIN` in the cross-binary test instead of silently skipping, include retained encodings in `read_policy_checkpoint` fallback diagnostics, and update ExecPlan test commands.
- [x] (2026-11-26) Owner decision: schema evolution covers only 0.2.5+ volumes and dumps. Superseded the previous entry: the dump carries only the published baseline slot, load refuses version 4 dumps, and the multi-encoding code, its error variants, the policy-selection SQL flag, and the cross-binary test were removed.

## Surprises & Discoveries

Discovery selects a compatible checkpoint already. It installs that checkpoint
and leaves the replay cursor there for mount to catch up. The staging gate now
validates the tail within the same PostgreSQL read snapshot.

The earlier multi-encoding backup design had to handle a pending baseline slot,
duplicate encodings, per-encoding publication generations, and envelope rebinds.
All of this existed only for hypothetical volumes, so the owner cut it. The
`metadata_state` baseline slot always holds an untagged checkpoint, so load
needs no checkpoint rebind; it rebinds only the PostgreSQL volume ID in
`FormatInfo`.

## Decision Log

Keep local installation and PostgreSQL publication atomic at their current
boundaries. Validate the tail before discovery publishes its checkpoint.

2026-11-26, owner decision (overrides earlier review rounds): StudioFS
guarantees schema evolution only for volumes and dumps created by 0.2.5 and
later. 0.2.4 and earlier take no part in it; they are adopted only through
`studiofs upgrade` (SFS-1132). Consequences:

- The dump carries exactly one checkpoint, the published baseline slot from
  `metadata_state`. If that slot is not published, `dump` refuses. Alternate
  `serving_checkpoint_encodings` rows are coordinator caches and are not dumped.
- Manifest v5 records the PostgreSQL schema revisions, volume protocol,
  checkpoint format and schema, event codec revisions, and the source redb
  layout revision.
- `load` validates the whole manifest before any PostgreSQL transaction.
  Version 4 dumps are refused with the upgrade path (0.2.4 load, `studiofs
  upgrade`, new dump). A newer recorded component is refused by name. DDL goes
  through `ensure_registered_schema`; the restore is one transaction.
- Kept: the mixed `client_id` batch check, versioned event codec validation,
  the target volume-ID rebind, restore of the dumped policy, and the
  capability-evaluator wiring in mount, discovery, and `smount`.

## Outcomes & Retrospective

The simplified dump/load passed on the configured PostgreSQL service: a 0.2.5+
round trip through a different volume ID, refusal of an unpublished baseline,
and the rollback test, which forces a duplicate counter, verifies zero volume,
metadata, policy, and counter rows, and then retries successfully.

## Context and Orientation

`crates/studiofs/src/cli/discovery.rs` stages a redb database and links it into
place. `dump_load.rs` captures and restores a PostgreSQL checkpoint and the
event batches following it. `crates/studiofs-meta/src/pg_engine/metadata_io.rs`
selects a checkpoint using the decoder registry. The checkpoint is a compact
metadata image at one event-log version. Snapshot mounts verify a separate
artifact with its own version.

## Plan of Work

Read policy with the checkpoint and validate the event tail through the shared
codec. Carry independently versioned identities in the dump and validate
them before the load transaction. Use the schema registry to provision the
destination and keep policy and metadata publication in one transaction.

## Concrete Steps

Run the following from the worktree. For the PostgreSQL tests, import
`STUDIOFS_TEST_PG_DSN` from the configured test environment and keep it out of
command output.

    cargo test -p studiofs --bin studiofs cli::dump_load::tests -- --include-ignored --test-threads=1
    cargo test -p studiofs --bin studiofs --features test-successor cli::dump_load::tests -- --include-ignored --test-threads=1
    cargo test -p studiofs --bin studiofs cli::discovery::tests -- --include-ignored --test-threads=1
    cargo test -p studiofs-meta --lib --features test-successor checkpoint_encodings -- --include-ignored --test-threads=1
    cargo clippy -p studiofs -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings
    cargo clippy -p studiofs -p studiofs-meta -p studiofs-vfs --all-targets --features test-successor -- -D warnings
    cargo fmt --check
    git diff --check

## Validation and Acceptance

Unknown event encodings are refused with an upgrade reason. Corrupt events
and missing history have distinct diagnostics. A version 4 dump is refused with
the upgrade path. A newer manifest component is refused by name. A failed
restore leaves the volume unpublished even after an error inside the restore
transaction.

## Idempotence and Recovery

The discovery stage can be retried after a validation error. The load
transaction rolls back as a unit. A separately committed schema migration
remains available for a subsequent load attempt.

## Artifacts and Notes

The unsupported event regression failed once before the production gate and
passed afterward. After the simplification, the PostgreSQL dump/load suite
passed 36 cases with and without `test-successor`, and discovery passed 10
cases. Native mounted and object-store tests are outside this run.

## Interfaces and Dependencies

Use `ClientCapabilities`, `VolumeRequirements`, the serving checkpoint decoder,
the event codec, and the PostgreSQL schema migration registry from
`studiofs-meta`.

Updated on 2026-11-26 with the owner's 0.2.5+ decision: single-checkpoint
dump, version 4 refusal, and removal of the multi-encoding backup path.

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

## Surprises & Discoveries

Discovery selects a compatible checkpoint already. It installs that checkpoint
and leaves the replay cursor there for mount to catch up. The staging gate now
validates the tail within the same PostgreSQL read snapshot. A backup now
captures every published checkpoint encoding at the dumped cut (both the
baseline `metadata_state` slot when published and any supported
`serving_checkpoint_encodings` rows at that cut) and restores all of them in one
transaction, keeping `metadata_state.checkpoint_publication_state = 'published'`
whenever the source had a published baseline slot (preserving Janitor, event-log
pruning, and format-4 reader preconditions). A successor-only volume without a
published baseline slot still restores with `metadata_state` pending and its
alternate encoding rebound in `serving_checkpoint_encodings`.

## Decision Log

Keep local installation and PostgreSQL publication atomic at their current
boundaries. Validate the tail before discovery publishes its checkpoint.
Version 5 records the source identities, while version 4 remains readable for
volumes already on the supported shared format. Preserve the restore policy
when provisioning a volume. Require a matching policy when replacing live
metadata or completing a target whose policy was already provisioned. Use the alternate checkpoint registry for format 5 and rebind the
volume identity in its envelope before publishing it. Reuse replay's
batch-client identity check when validating the dump before restore.

## Outcomes & Retrospective

Baseline and test-successor restore ran on the configured PostgreSQL service.
The rollback test forced a duplicate counter during the restore transaction,
verified zero volume, metadata, policy and counter rows, and then retried the
same backup successfully. The successor test verified that format 5 remained
selectable after restoring to a different PostgreSQL volume ID.

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

Run the following from the worktree. For the PostgreSQL tests, import only
`STUDIOFS_TEST_PG_DSN` from the configured test environment and keep it out of
command output.

    cargo test -p studiofs --bin studiofs cli::dump_load::tests -- --include-ignored --skip regenerate_baseline_dump_fixture --test-threads=1
    cargo build -p studiofs --bin studiofs
    STUDIOFS_TEST_BASELINE_BIN=target/debug/studiofs cargo test -p studiofs --features test-successor --bin studiofs cli::dump_load::tests -- --include-ignored --skip regenerate_baseline_dump_fixture --test-threads=1
    STUDIOFS_TEST_BASELINE_BIN=target/debug/studiofs cargo test -p studiofs --features test-successor --bin studiofs -- --ignored --test-threads=1 successor_dump_preserves_baseline_policy_checkpoint successor_dump_load_preserves_baseline_and_alternate_checkpoint_encodings read_policy_checkpoint_fallback_reports_baseline_and_retained_encodings
    cargo test -p studiofs --bin studiofs cli::discovery::tests
    cargo test -p studiofs-meta --lib pg_snapshot_backend_reads_a_validated_bootstrap_descriptor -- --ignored
    cargo test -p studiofs-meta --lib fresh_client_marker_clear_failure_keeps_engine_read_only
    cargo test -p studiofs-vfs --lib mount_rejects_unsupported_artifact_versions_before_download
    cargo clippy -p studiofs -p studiofs-meta --all-targets -- -D warnings
    cargo check -p studiofs --features test-successor
    cargo fmt --check
    git diff --check

## Validation and Acceptance

Unknown event encodings are refused with an upgrade reason. Corrupt events
and missing history have distinct diagnostics. A failed restore leaves the
volume unpublished even after an error inside the restore transaction.

## Idempotence and Recovery

The discovery stage can be retried after a validation error. The load
transaction rolls back as a unit. A separately committed schema migration
remains available for a subsequent load attempt.

## Artifacts and Notes

The unsupported event regression failed once before the production gate and
passed afterward. The PostgreSQL dump/load suite passed 32 cases on baseline
and 33 with the test successor. Discovery passed 10 local cases. The
PostgreSQL descriptor, local fresh-client and independent VFS artifact tests
each passed one case. Scoped Clippy, test-successor Cargo check, Cargo fmt and
Git whitespace checks passed. An
earlier concurrent baseline run timed out while waiting for the dump's
counters query. The sequential baseline run passed the case. Native mounted
and object-store tests are outside this run.

For the policy-checkpoint regression, build `studiofs` without
`test-successor` and set `STUDIOFS_TEST_BASELINE_BIN` to its path before
running `successor_dump_preserves_baseline_policy_checkpoint` with
`test-successor`. The test launches that binary against the restored
PostgreSQL volume and checks the upgrade diagnostic on a successor-only source.

## Interfaces and Dependencies

Use `ClientCapabilities`, `VolumeRequirements`, the serving checkpoint decoder,
the event codec, and the PostgreSQL schema migration registry from
`studiofs-meta`.

Updated on 2026-11-26 with the restore format, diagnostic behavior, and
service-backed rollback evidence.

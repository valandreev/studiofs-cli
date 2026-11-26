# SFS-1012 local format migration

## Purpose

A stopped StudioFS cache can advance its local metadata format while keeping pending work and without contacting PostgreSQL. Verify this using the pinned 0.2.5 dirty cache and the test-successor build.

## Progress

- [x] Identified the existing stopped cache upgrade entry point and both redb format headers.
- [x] Added a test-only metadata revision and durable upgrade boundaries.
- [x] Added read-only format checks and fixture coverage.
- [x] Passed focused tests, both feature configurations of Cargo check and Clippy, and diff validation.

## Surprises & Discoveries

The baseline metadata decoder denies unknown fields. The local redb header can therefore carry an independent revision that also fences baseline binaries without changing the shared volume format. The payload index already commits its format-1 bookkeeping transition and revision together in a single redb transaction.

## Decision Log

Use revision 6 for the unmarked baseline header. The test successor records revision 7 in the local header and keeps shared format 6. Preserve the local revision when rewriting the header and strip it from exported snapshot settings. Keep payload-index format 2 and entry encoding 3. Existing format-1 indexes retain their atomic upgrade. A failed bookkeeping validation rolls back the revision update with the rest of that transaction.

## Outcomes & Retrospective

The local metadata migration changes header state only. On Windows, both metadata revision tests passed, including the one with five simulated interruption boundaries. Three payload fixture tests passed. Two redb compatibility tests and the successor registry test passed. Cargo check and Clippy passed for the three affected packages in both feature configurations.

## Context and Orientation

The CLI owns a stopped cache through RedbMetaEngine::open_for_format_upgrade in crates/studiofs-meta/src/redb_engine.rs. The shared format-5 operation remains in crates/studiofs-meta/src/redb_engine/format.rs. crates/studiofs-cache/src/payload_index.rs manages the separate payload index and its version counter. The dirty metadata fixture carries unpublished events and local overlay data.

## Plan of Work

From the workspace root, build with test-successor and use studiofs upgrade NAME --local-only to advance a stopped local metadata cache. The first transaction records staging. The second completes the reversible step. The third publishes the new local revision and irreversible marker together. The last transaction clears the staging record. At every restart the existing owner can re-enter the same transition. Normal successor opens reject pending stages.

## Concrete Steps

Run cargo fmt --all and git diff --check. Run cargo check and cargo clippy with -p studiofs-meta -p studiofs-cache -p studiofs --all-targets, both with and without --features test-successor. Use -- -D warnings for clippy.

Run cargo test -p studiofs-meta baseline_dirty_cache_local_revision, cargo test -p studiofs-meta --features test-successor baseline_dirty_cache_local_revision, and cargo test -p studiofs-cache baseline_payload_index_fixture.

## Validation and Acceptance

The metadata fixture resumes at five interruption boundaries and retains its acknowledgement positions, exported rows, pending deletions, local upload ledger, and persisted client slot. The payload fixture reopens with its original lifecycle states and refuses a newer revision without changing file bytes. These tests require local files only.

## Idempotence and Recovery

Retry the same local-only upgrade with the successor binary after an interruption. Each completed transaction is durable. Normal read-only inspection cannot advance an upgrade. Neither database is deleted or recreated on a revision mismatch.

## Artifacts and Notes

The fixtures remain under crates/studiofs-meta/tests/fixtures/baseline-0.2.5 and crates/studiofs-cache/tests/fixtures.

## Interfaces and Dependencies

The test-successor feature enables local redb revision 7 and the local-only CLI option. The migration uses the existing redb transaction and exclusive file ownership APIs.

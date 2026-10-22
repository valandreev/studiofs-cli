# Require an explicit snapshot volume

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

Users can choose either of several PostgreSQL-backed volumes with `studiofs snapshot <volume-name> <command>`. Commands resolve only the named volume and keep snapshot identifiers and policies scoped to its PostgreSQL authority.

## Progress

- [x] Read SFS-880, repository contracts, existing parser, resolver, backend scoping and user guides.
- [x] Observe the updated canonical parser test fail against the original parser.
- [x] Replace global metadata scanning with explicit named resolution shared by all operations.
- [x] Update English/Ukrainian guides, help examples and both E2E create callers.
- [x] Run nine snapshot unit tests successfully with native Windows access.
- [x] Complete four CLI process checks and the two-volume PostgreSQL regression.
- [x] Resolve review findings and pass focused Clippy checks; prepare the validated changes for commit, push and Linear delivery.
- [x] Validate the follow-up deletion of redundant process tests and record review dispositions.

## Surprises & Discoveries

The snapshot E2E scenario had two executable callers of the old syntax. A sandbox metadata-read failure disappeared when the identical focused tests ran with native Windows access. Clap recognizes operation names before the required positional argument. A raw volume/tail boundary followed by the same typed operation schema preserves valid names such as `list` and `help`. The PostgreSQL fixture must seed a valid exported checkpoint; an empty byte vector is rejected by snapshot capture.

## Decision Log

Resolve the named volume once in dispatch and pass its resolved context to operations, removing seven implicit resolutions and the global ambiguity branches. Preserve the existing mounted metadata service and PostgreSQL identity validation. Use PostgreSQL-only fixtures for scope checks, without object storage or mounted scenarios.

Parse the operation tail with clap's existing typed schema so operation names remain valid volume names. Generate help from that schema and carry its global log-level override into the existing tracing initialization. This preserves volume-first syntax and logging flags in all positions without reserving new volume names.

Follow-up review: remove four subprocess tests that repeat the production parser and named resolver unit tests. Retain their canonical help/unknown-volume usage assertions in the unit tests and keep the PostgreSQL process regression. Reject replacing the parser with plain derived `Args`: a direct rustc probe using the installed clap dependency rejects `snapshot list list` and `snapshot create list`, and interprets `snapshot help list` as help, although the shared volume validator permits those names. Keep the log-level handling and observable parser/help regressions needed by that contract; remove the unnecessary test-only argument helper.

## Outcomes & Retrospective

Implementation, guides and both E2E callers are updated. Eleven snapshot unit tests, one log-level position matrix, four CLI process tests, one PostgreSQL two-volume regression and one E2E argument contract test pass. Clippy passes for both changed crates. The PostgreSQL regression proves catalog/policy isolation and rejects foreign inspect/delete while preserving the other volume's catalog entry. Mounted scenarios 25/26, object storage publication and full suites were not run.

Follow-up review removed the four redundant process tests and their helpers, retaining the PostgreSQL test. Fresh checks pass: eleven snapshot unit tests (including canonical help and missing-volume error usage), the log-level matrix, the exact PostgreSQL isolation test, `cargo clippy -p studiofs --all-targets -- -D warnings`, formatting and `git diff --check`. The first strengthened help assertion omitted the generated `[OPTIONS]` token; the corrected assertion verifies the actual required volume/command order. The production parser is unchanged.

## Context and Orientation

`crates/studiofs/src/cli/snapshot.rs` defines parser arguments, volume resolution and all snapshot operations. Local redb format metadata selects a PostgreSQL backend identified by a volume ID; the backend already scopes its queries to that ID. `crates/studiofs/src/cli/mod.rs` contains top-level help examples. `crates/studiofs-e2e/src/scenario/workloads/snapshot_v2.rs` constructs child CLI commands; its tests verify their argument vectors. Current user guides live under `docs/user-guide/en` and `docs/user-guide/uk`.

## Plan of Work

Require a volume argument before the existing operation enum. Derive its validated local metadata path directly, reject missing metadata and mismatched identity, and open only its backend. Keep operation logic unchanged apart from accepting the resolved context. Replace scanner tests, update parser matrices and add CLI process tests in `crates/studiofs/tests/snapshot_cli_test.rs`. Update current docs and E2E callers, leaving historical plans unchanged.

## Concrete Steps

From `E:\dev\studiofs-cli`, run sequentially:

    cargo test -p studiofs --bin studiofs cli::snapshot::tests -- --nocapture
    cargo test -p studiofs --bin studiofs snapshot_log_level_works_before_and_after_the_operation -- --nocapture
    cargo test -p studiofs --test snapshot_cli_test snapshot_cli_scopes_catalog_and_policy_to_the_named_volume -- --ignored --exact --nocapture --test-threads=1
    cargo test -p studiofs-e2e snapshot_v2_uses_exact_recovery_cli_forms -- --nocapture
    cargo clippy -p studiofs -p studiofs-e2e --all-targets -- -D warnings
    cargo fmt -p studiofs -p studiofs-e2e -- --check
    git diff --check

## Validation and Acceptance

Parser tests must accept all seven named operation variants and reject omitted names. Unit tests must show the required positional name in help and actionable errors without reading unrelated metadata. An ignored PostgreSQL process test creates two temporary volumes and verifies named catalog/policy isolation and rejection of foreign snapshot IDs; it requires `STUDIOFS_TEST_PG_DSN`, read privately from environment or `.env`. No full suite or mounted acceptance is authorized here.

## Idempotence and Recovery

Use disposable directories under `target/` and unique PostgreSQL fixture identities with cleanup. Preserve all unrelated files and commits. Retry environment-denied focused commands with native access; do not change assertions to hide environment failures.

## Artifacts and Notes

The red parser run rejected `snapshot production create` as an unrecognized subcommand. Final native snapshot tests pass (11), the log-level matrix passes (1), process tests pass (4), the PostgreSQL-only regression passes (1), and the E2E argument test passes (1). `cargo clippy -p studiofs -p studiofs-e2e --all-targets -- -D warnings` passes. Scenario listing confirms Snapshot v2 scenarios 25 and 26 remain registered.

## Interfaces and Dependencies

Reuse clap, the shared `default_meta_dir_in_client_root` name validator, `RedbMetaEngine::read_format_info_readonly`, mounted diagnostics and `PgSnapshotBackend`. Add no dependencies or new runtime abstractions.

Plan created after review revealed that the existing E2E crate also needed caller updates.
Updated after validation to record clap's positional/subcommand collision, its bounded parser correction, valid checkpoint fixture requirements and the completed focused checks.

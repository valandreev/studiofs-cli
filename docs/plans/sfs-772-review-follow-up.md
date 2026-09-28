# Harden the SFS-772 service-backed test gate

This ExecPlan follows `.agent/PLANS.md` and stays current until the review follow-up is complete.

## Purpose / Big Picture

When a selected SFS-772 test cannot reach its required service, the test process must fail so CI cannot report a false pass. The same gate should clean up its disposable Docker state, honor the configured S3 region, and give developers copyable local commands that preserve the gate's required bucket setup order.

## Progress

- [x] (2026-09-28 09:22Z) Read the SFS-772 issue, review document, CI selection, and affected test setup paths.
- [x] (2026-09-28 09:24Z) Demonstrate the current false-pass behavior: the selected ignored destroy test printed `skipping` and exited 0 with an unreachable PostgreSQL DSN.
- [x] (2026-09-28 09:34Z) Make selected fixtures fail on missing or unreachable infrastructure; make DataStore bucket setup report a failed marker write.
- [x] (2026-09-28 09:34Z) Harden service cleanup and health polling; read the DataStore region from its environment variable.
- [x] (2026-09-28 09:34Z) Document local commands and bucket provisioning order; run focused validation and inspect the final diff.
- [x] (2026-09-28 11:35Z) Apply review follow-up: configure non-default bucket regions, preserve sanitized CreateBucket error codes, and select each DataStore regression by exact name.

## Surprises & Discoveries

The current CI task already runs the DataStore tests before the combined S3/PostgreSQL tests, and the DataStore fixture creates the shared `studiofs-test` bucket. The review's test-order concern is therefore not a current false pass in this gate, but it is a prerequisite for independently running the combined binaries. The documentation should state that order rather than adding another S3 client abstraction.

The review document's head SHA differs from the current branch head. Each actionable claim was checked against the actual working tree. The false-pass reproduction exited 0 before the change; after the change, the same selection exited nonzero with a PostgreSQL connection failure.

## Decision Log

- Decision: Keep one exact CI selection per named regression, rather than grouping test binaries to reduce Cargo process starts. Rationale: the gate deliberately asserts named, nonzero selections, and no timing evidence shows runner startup is material. Date/Author: 2026-09-28, Codex.
- Decision: Keep the current DataStore-first bucket provisioning order and document it. Rationale: the order is explicit in the CI script and the docs identify the DataStore fixture as the bucket creator; extracting new cross-crate S3 setup machinery is unnecessary for this gate. Date/Author: 2026-09-28, Codex.
- Decision: Do not replace deterministic DataStore object IDs with UUIDs in this task. Rationale: CI starts a disposable RustFS service for each task, and the counter makes IDs distinct within the one selected DataStore test process. Date/Author: 2026-09-28, Codex.

## Outcomes & Retrospective

The selected destroy and dump/load tests now fail on unavailable prerequisites instead of logging a skip and returning success. DataStore test setup validates the marker write and honors `STUDIOFS_S3_REGION`; non-default regions now reach `CreateBucket` as a location constraint, and failed provisioning reports the sanitized AWS service error code. CI and local commands select the five DataStore regressions by exact name. CI removes anonymous container volumes and retries failed health inspection within its existing bounded wait. The DataStore-first bucket setup remains documented. Formatting, focused check/clippy targets, test listing, YAML structure, and diff checks passed. Live service-backed execution remains unverified because Docker Desktop's named pipe denies access in this environment.

## Context and Orientation

`.rwx/ci.yml` starts temporary PostgreSQL and RustFS containers, exports their endpoint configuration, and runs ignored tests. `crates/studiofs/tests/destroy_test.rs` and `dump_load_test.rs` contain the five selected combined tests. `crates/studiofs-data/tests/integration_test.rs` creates the shared bucket and configures direct S3 tests. `docs/tests.md` describes the gate and local test tiers.

## Plan of Work

First, run the exact destroy regression with an invalid but non-empty PostgreSQL DSN. The current `Option`-based fixture is expected to print a skip and exit successfully. Change the selected destroy and dump/load tests to require their S3/PostgreSQL prerequisites when explicitly invoked with `--ignored`, and require the S3 bucket marker write to succeed after bucket creation is attempted. Then change the DataStore test configuration to honor `STUDIOFS_S3_REGION`.

In `.rwx/ci.yml`, remove anonymous Docker volumes during cleanup and let transient `docker inspect` failures pass through the existing bounded health wait, which still fails with service logs if health is not established. In `docs/tests.md`, add local commands for the same S3-only and combined tests and state that the DataStore command creates the bucket used by later commands.

## Concrete Steps

From `E:\dev\studiofs-cli`, use the exact filtered `cargo test` command below to observe the existing destroy false pass, then repeat after the fixture change and expect the selected test to fail instead of report `ok`:

    $env:STUDIOFS_TEST_PG_DSN = 'host=127.0.0.1 port=1 user=postgres dbname=postgres connect_timeout=1'
    cargo test -p studiofs --test destroy_test destroy_full_removes_local_s3_and_postgres_state -- --ignored --exact --nocapture

After editing, run focused formatting, diff checks, and test-list selectors. Full S3/PostgreSQL execution requires Docker access to both services; do not run workspace-wide or full ignored suites.

## Validation and Acceptance

Acceptance requires the malformed-DSN destroy selection to exit nonzero with a fixture setup failure, each intended Cargo selector to list at least one ignored test, YAML parsing and `git diff --check` to succeed, and focused Rust test targets to compile. Where Docker is available, the service-backed tests must execute and fail on setup or assertion errors. The current Windows Docker Desktop endpoint may be unavailable, so report that gate as environment-blocked if it remains inaccessible.

## Idempotence and Recovery

The changes affect test setup, CI teardown, and documentation only. Repeated CI runs start fresh service containers; `docker rm --force -v` removes each container and its anonymous volumes. Local test commands may be rerun after starting the documented services and creating the configured bucket; no persistent application data is modified by this plan.

## Artifacts and Notes

The attached review document is `docs/code-reviews/sfs-772-live-s3-ci-gate-review.md`. Its claims are treated as feedback and verified against the current source and the SFS-772 acceptance check.

## Interfaces and Dependencies

No production API or dependency changes are planned. The Rust changes are limited to ignored integration test fixtures and environment-based test configuration. CI continues to use the pinned RustFS image, `postgres:18-alpine`, and the existing nonzero test selector helper.

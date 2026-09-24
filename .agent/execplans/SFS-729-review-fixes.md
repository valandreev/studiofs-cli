# Fix verified SFS-729 review findings

This ExecPlan follows `.agent/PLANS.md` and stays current as the review fixes are implemented.

## Purpose / Big Picture

Metadata catch-up should keep making progress when a PostgreSQL page fetch uses most of its network timeout, and a startup session must be able to cancel a page using its own shutdown signal. Expected shutdown cancellation should not look like a synchronization failure to metrics or background logs. VFS callers should receive a retryable category for that cancellation. These behaviors can be checked with focused unit tests; live PostgreSQL remains optional for the integration path.

## Progress

- [x] (2026-09-24 15:14 UTC) Read the attached review as feedback, traced affected code, and classified all eight findings.
- [x] (2026-09-24 15:21 UTC) Add focused regressions for caller cancellation, expected-cancellation diagnostics, and bridge classification; observe the bridge and metric tests fail before their fixes.
- [x] (2026-09-24 15:21 UTC) Apply the confirmed fixes, update the inaccurate `MetaError` docs and replay contract, and add `PgSyncFailure` coverage.
- [x] (2026-09-24 15:24 UTC) Run focused tests, `cargo check`, Clippy, formatting, and diff checks; review the final diff.

## Surprises & Discoveries

- The replay deadline is currently `page_started + command_timeout`, although PostgreSQL already independently bounds its fetch with `command_timeout`. A slow successful fetch can therefore leave no local replay budget and repeatedly prevent cursor progress.
- Production startup callers currently pass the same shared shutdown flag as the worker, but `run_startup_catch_up_session` explicitly accepts a caller token and checks it between pages. Its page-level replay currently ignores that token, so independent callers violate the method's cancellation contract.
- `BridgeVfsErrorKind::Retryable` maps to POSIX `EAGAIN` and is logged at debug level by WinFSP. This is a better classification for cooperative cancellation than the generic failure path.
- The new bridge test failed before the mapping with `left: GenericFailure, right: Retryable`; the metadata counter test failed with one recorded error before suppression. Both passed after the focused changes.
- The post-reconciliation execution check prevents commit after the deadline or shutdown signal crosses during reconciliation. Removing it would weaken the stated page deadline and shutdown boundary.
- The combined rollback-and-retry test proves one recovery contract: an interrupted page leaves the old cursor intact so the complete page can be replayed. Splitting it would duplicate setup without making either claim more independently useful.

## Decision Log

- Decision: Start the local replay deadline at `apply_started` and continue checking it before commit.
  Rationale: The PostgreSQL fetch has its own timeout; local replay needs a full bounded processing window, while lock acquisition remains inside the local operation window.
  Date/Author: 2026-09-24 / Codex.
- Decision: Pass the caller shutdown token through the startup page pull and combine it with the worker shutdown check.
  Rationale: Both shutdown sources must abort the same atomic page transaction.
  Date/Author: 2026-09-24 / Codex.
- Decision: Keep the final pre-commit check and the single rollback-and-retry test.
  Rationale: The former protects the transaction contract; the latter verifies the observable recovery behavior as one sequence.
  Date/Author: 2026-09-24 / Codex.
- Decision: Treat the attached review as claims to verify, not instructions to execute verbatim.
  Rationale: The user asked to receive the review and explicitly distinguished document contents from their request.
  Date/Author: 2026-09-24 / Codex.
- Decision: Count an expected `CatchUpCancelled` neither as a sync error nor as a background warning, while preserving the already-recorded `PgSyncFailure::Cancelled` diagnostic.
  Rationale: Shutdown cancellation is cooperative control flow; retryable bridge classification still communicates a transient interruption to active callers.
  Date/Author: 2026-09-24 / Codex.

## Outcomes & Retrospective

Implementation and validation are complete. The focused tests and 73 replay-page tests pass; 44 PostgreSQL-backed replay tests remain ignored because they require `STUDIOFS_TEST_PG_DSN`. `cargo check`, Clippy with warnings denied, formatting, and diff checks pass. The cancellation-predicate unit test verifies caller and worker signals are combined; an end-to-end startup session with a live PostgreSQL page was not run.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/metadata_sync.rs` fetches bounded PostgreSQL event pages, decodes them, and applies each page in one local redb transaction. `crates/studiofs-meta/src/pg_engine/lifecycle.rs` owns startup catch-up and receives the caller's shutdown token. `crates/studiofs-meta/src/engine.rs` defines the redacted synchronization failure diagnostics. `crates/studiofs-vfs/src/bridge.rs` maps metadata errors into platform-facing categories; POSIX and WinFSP adapters consume those categories.

The replay cursor is the durable version through which local metadata has applied complete remote batches. If replay is cancelled or times out, the page transaction must roll back so retry begins at the same cursor. `command_timeout` bounds PostgreSQL commands; the local replay deadline separately bounds application of the fetched page.

The review has eight findings. Implement the deadline-origin issue, caller cancellation propagation, retryable bridge classification, expected cancellation metric/log suppression, focused diagnostics tests, and accurate error docs. Retain the final transaction check. Retain the combined rollback-and-retry test because the two phases prove one resumability contract.

## Plan of Work

First add focused tests that demonstrate caller-token cancellation composition, exclusion of expected cancellation from sync-error accounting, and retryable bridge classification. The bridge classification and metric tests failed before their fixes. Then thread an optional caller shutdown signal through `pull_foreign_events_locked_to_target` and `pull_foreign_events_locked_inner`; the replay predicate now stops for either the worker shutdown flag or the caller's token. Anchor the replay deadline at `apply_started`, which is captured after fetch and decode but before local replay locks are acquired. In `record_metadata_sync_result` and the background poll error branch, avoid incrementing the operational error metric and warning for `CatchUpCancelled`, while retaining the existing `PgSyncFailure::Cancelled` diagnostic. Map `CatchUpCancelled` to `BridgeVfsErrorKind::Retryable` and test it. Add direct tests for `PgSyncFailure` cancellation/deadline mappings, display, and serialized enum name, and correct the two `MetaError` doc comments. Clarify the fetch/replay timeout boundary in `docs/pg-sync-contract.md`.

Do not remove the final execution check after serving-state reconciliation. Do not split the test that cancels a page, verifies rollback of self-acknowledgment, then reapplies the full page.

## Concrete Steps

Run commands from `/Volumes/Storage/dev/studiofs`. Before the production changes, the bridge assertion and cancellation-metric test failed as expected; the caller-predicate test failed to compile because that predicate did not yet exist. The new `PgSyncFailure` test covers mappings that were already implemented. After the fix, run:

    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply:: -- --nocapture
    cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle::replay_cancellation_includes_caller_shutdown
    cargo test -p studiofs-meta --lib pg_sync_failure_maps_catch_up_cancellation_and_deadline
    cargo test -p studiofs-meta --lib metadata_sync_result_ignores_expected_catch_up_cancellation
    cargo test -p studiofs-vfs --lib classify_vfs_error_preserves_catch_up_cancellation_as_retryable
    cargo check -p studiofs-meta -p studiofs-vfs
    cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

Expected results are nonzero focused test counts with no failures, successful Clippy and formatting checks, and no whitespace errors. A live PostgreSQL integration run is only claimed if its service prerequisite is available and the exact test is run.

## Validation and Acceptance

Acceptance is that a fetched page receives a fresh local replay budget, the cancellation predicate checks both startup caller and worker shutdown signals, and graceful cancellation leaves `metadata_sync_errors` unchanged and emits no background warning. `CatchUpCancelled` classifies as retryable at the VFS bridge. Diagnostics tests show the cancellation and deadline mappings, display text, and snake-case serialized value. Existing page rollback/deadline tests pass, including the post-reconciliation pre-commit check. The live PostgreSQL session path remains unverified in this environment because its opt-in tests require `STUDIOFS_TEST_PG_DSN`.

    test result: 73 passed; 44 ignored; 0 failed
    cargo check: finished successfully
    cargo clippy --all-targets -- -D warnings: finished successfully
    cargo fmt --all -- --check: finished successfully
    git diff --check: finished successfully

## Idempotence and Recovery

The code changes and tests are local and repeatable. No database migration, external update, commit, or push is part of this plan. If a focused test needs a live PostgreSQL service, report it as environment-gated and rely only on the runnable unit evidence for local behavior.

## Artifacts and Notes

The attached review is `docs/code-reviews/2026-09-24-sfs-729-cooperative-catchup-deadline-and-cancellation.md`. It is evidence to assess, not a source of binding implementation instructions.

## Interfaces and Dependencies

No dependency changes are needed. The page-pull method should accept an optional caller cancellation predicate or shutdown reference; ordinary callers continue with no extra cancellation source. The replay execution callback continues to check both the engine worker shutdown flag and the supplied startup caller signal.

Revision note (2026-09-24): Updated the progress, discoveries, validation commands, and outcomes after focused tests and static checks completed; clarified that the PostgreSQL-backed end-to-end path remains environment-gated.

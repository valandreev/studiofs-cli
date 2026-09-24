# Fix verified SFS-729 review findings

This ExecPlan follows `.agent/PLANS.md` and stays current as the review fixes are implemented.

## Purpose / Big Picture

Metadata catch-up should keep making progress when a PostgreSQL page fetch uses most of its network timeout, and a startup session must be able to cancel a page using its own shutdown signal. Expected shutdown cancellation should not look like a synchronization failure to metrics or background logs. VFS callers should receive a retryable category for that cancellation. These behaviors can be checked with focused unit tests; live PostgreSQL remains optional for the integration path.

## Progress

- [x] (2026-09-24 15:14 UTC) Read the attached review as feedback, traced affected code, and classified all eight findings.
- [x] (2026-09-24 15:21 UTC) Add focused regressions for caller cancellation, expected-cancellation diagnostics, and bridge classification; observe the bridge and metric tests fail before their fixes.
- [x] (2026-09-24 15:21 UTC) Apply the confirmed fixes, update the inaccurate `MetaError` docs and replay contract, and add `PgSyncFailure` coverage.
- [x] (2026-09-24 15:24 UTC) Run focused tests, `cargo check`, Clippy, formatting, and diff checks; review the final diff.
- [x] (2026-09-24 16:37 UTC) Verify the eight findings in `2026-09-24-sfs-729-deep-code-review.md`; add failing regressions for inter-page worker shutdown and remote-validation preservation, then apply the two confirmed behavior fixes.
- [x] (2026-09-24 16:37 UTC) Extend cancellation coverage through the post-reconciliation check and assert both durable and in-memory cursors remain unchanged; mutation-check that the regression fails without the final guard.
- [x] (2026-09-24 16:37 UTC) Correct the cancellation diagnostic comment and replay contract; run lifecycle/replay suites, crate check, Clippy, formatting, and diff checks.
- [x] (2026-09-24 17:21 UTC) Verify all eight findings in `2026-09-24-sfs-729-rust-best-practices-code-review.md`; add a red-to-green pre-commit rollback regression and implement the confirmed code and test corrections.
- [x] (2026-09-24 17:21 UTC) Re-run the 74 replay tests, 106 lifecycle tests, five diagnostics tests, crate check, Clippy, formatting, and diff checks; record the PostgreSQL-gated tests as ignored.

## Surprises & Discoveries

- The replay deadline is currently `page_started + command_timeout`, although PostgreSQL already independently bounds its fetch with `command_timeout`. A slow successful fetch can therefore leave no local replay budget and repeatedly prevent cursor progress.
- Production startup callers currently pass the same shared shutdown flag as the worker, but `run_startup_catch_up_session` explicitly accepts a caller token and checks it between pages. Its page-level replay currently ignores that token, so independent callers violate the method's cancellation contract.
- `BridgeVfsErrorKind::Retryable` maps to POSIX `EAGAIN` and is logged at debug level by WinFSP. This is a better classification for cooperative cancellation than the generic failure path.
- The new bridge test failed before the mapping with `left: GenericFailure, right: Retryable`; the metadata counter test failed with one recorded error before suppression. Both passed after the focused changes.
- The post-reconciliation execution check prevents commit after the deadline or shutdown signal crosses during reconciliation. Removing it would weaken the stated page deadline and shutdown boundary.
- The combined rollback-and-retry test proves one recovery contract: an interrupted page leaves the old cursor intact so the complete page can be replayed. Splitting it would duplicate setup without making either claim more independently useful.
- The inter-page catch-up loop checked the caller shutdown token while page replay already combined caller and worker shutdown. Reusing `replay_cancelled` in the loop closes that gap; a fault hook proves the worker-shutdown test would attempt a page fetch before the fix.
- Metadata-session bookkeeping invalidated remote validation for local cancellation/deadline errors even though `record_remote_operation_result` already preserves it for those same errors. The regression failed on the old fallback path and passes for both variants after the matching guard was added.
- Advancing the cancellation predicate to its third check reaches the post-reconciliation boundary. Temporarily removing that guard made the test fail, confirming it protects the intended boundary.
- The new Rust review found a commit window after writing the replay position: the injected pre-commit hook could set shutdown after the last execution check, and the page still committed. The new regression failed before the final check and passed after the transaction was explicitly aborted on cancellation.
- The redundant worker-shutdown `Arc` clones were only used to form local references. `poll_shutdown_ref` now serves those stack borrows; the owned `Arc` accessor remains for spawned work that must outlive the call.
- `RemoteReplayExecution` now has private fields, a constructor, and `Copy`/`Clone`. It remains borrowed through `Option<&RemoteReplayExecution>` and keeps its `dyn Fn` callback: changing those forms would add generic or value-passing churn without evidence of lower cost.
- The validation test now checks both remote validation flags and both result-recording paths; the deadline test uses the canonical persisted-version helper, the `Cell::replace` transition has a comment, and diagnostics behaviors have separate tests.

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
- Decision: Use the existing `replay_cancelled` helper for the inter-page continuation predicate and preserve successful remote validation after `CatchUpCancelled` or `CatchUpDeadlineExceeded`.
  Rationale: Both shutdown sources must be checked across page boundaries; local replay aborts do not invalidate the remote format observation.
  Date/Author: 2026-09-24 / Codex.
- Decision: At the deep-review stage, defer deriving `Clone`/`Copy` for `RemoteReplayExecution` because that suggestion alone would not change its borrowed call sites.
  Rationale: Replay APIs used `Option<&RemoteReplayExecution>` and there was no measured reason to change their passing convention.
  Date/Author: 2026-09-24 / Codex.
- Decision: Keep the post-reconciliation guard in production and move the cancellation regression to its third execution check.
  Rationale: The guard already enforces rollback before commit; extending the test protects that behavior without changing the transaction implementation.
  Date/Author: 2026-09-24 / Codex.
- Decision: Add private fields, a constructor, and `Copy`/`Clone` to `RemoteReplayExecution`, while keeping the borrowed API and callback representation.
  Rationale: This accepts the encapsulation and value-trait findings from the Rust review. The 32-byte wrapper is not proven cheaper to pass by value than by reference, and the current callback supports capturing closures; changing either form without profiling or a caller need would be speculative.
  Date/Author: 2026-09-24 / Codex.

## Outcomes & Retrospective

Implementation and validation are complete. The focused tests and 73 replay-page tests pass; 44 PostgreSQL-backed replay tests remain ignored because they require `STUDIOFS_TEST_PG_DSN`. `cargo check`, Clippy with warnings denied, formatting, and diff checks pass. The cancellation-predicate unit test verifies caller and worker signals are combined; an end-to-end startup session with a live PostgreSQL page was not run.

The follow-up deep review is complete too. Its two confirmed behavior defects now have red-to-green regressions. The lifecycle suite passes 106 tests with 36 PostgreSQL-backed tests ignored; the replay suite passes 73 with 44 ignored. The post-reconciliation test fails when its production guard is temporarily removed. Crate check, Clippy with warnings denied, rustfmt, and `git diff --check` pass. Live PostgreSQL catch-up remains environment-gated.

The Rust best-practices review is complete. Its missing pre-commit cancellation check was confirmed with a test that failed when the hook set shutdown after cursor write; adding an abort check immediately before commit made the test pass. The replay suite now passes 74 tests with 44 PostgreSQL-backed tests ignored, the lifecycle suite passes 106 with 36 ignored, and the diagnostic tests pass five. `cargo check -p studiofs-meta`, Clippy with warnings denied, rustfmt, and `git diff --check` pass. The review's encapsulation and trait suggestions were applied; static dispatch and pass-by-value changes were left out without measured benefit. Live PostgreSQL catch-up remains environment-gated.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/metadata_sync.rs` fetches bounded PostgreSQL event pages, decodes them, and applies each page in one local redb transaction. `crates/studiofs-meta/src/pg_engine/lifecycle.rs` owns startup catch-up and receives the caller's shutdown token. `crates/studiofs-meta/src/engine.rs` defines the redacted synchronization failure diagnostics. `crates/studiofs-vfs/src/bridge.rs` maps metadata errors into platform-facing categories; POSIX and WinFSP adapters consume those categories.

The replay cursor is the durable version through which local metadata has applied complete remote batches. If replay is cancelled or times out, the page transaction must roll back so retry begins at the same cursor. `command_timeout` bounds PostgreSQL commands; the local replay deadline separately bounds application of the fetched page.

The initial review had eight findings. Implement the deadline-origin issue, caller cancellation propagation, retryable bridge classification, expected cancellation metric/log suppression, focused diagnostics tests, and accurate error docs. Retain the final transaction check. Retain the combined rollback-and-retry test because the two phases prove one resumability contract.

The follow-up deep review also listed eight findings. The inter-page loop now checks both worker and caller shutdown, and fallback recording preserves remote validation for local cancellation/deadline errors. Tests cover all replay-cancellation signal combinations, the post-reconciliation check, and the in-memory cursor after rollback. A later Rust best-practices review found that a shutdown signal could cross after the replay cursor was written; `metadata_sync.rs` now checks again and aborts immediately before commit. `RemoteReplayExecution` encapsulates its fields and derives `Copy`/`Clone`, while callers continue borrowing it and the capture-capable callback remains dynamic.

## Plan of Work

First add focused tests that demonstrate caller-token cancellation composition, exclusion of expected cancellation from sync-error accounting, and retryable bridge classification. The bridge classification and metric tests failed before their fixes. Then thread an optional caller shutdown signal through `pull_foreign_events_locked_to_target` and `pull_foreign_events_locked_inner`; the replay predicate now stops for either the worker shutdown flag or the caller's token. Anchor the replay deadline at `apply_started`, which is captured after fetch and decode but before local replay locks are acquired. In `record_metadata_sync_result` and the background poll error branch, avoid incrementing the operational error metric and warning for `CatchUpCancelled`, while retaining the existing `PgSyncFailure::Cancelled` diagnostic. Map `CatchUpCancelled` to `BridgeVfsErrorKind::Retryable` and test it. Add direct tests for `PgSyncFailure` cancellation/deadline mappings, display, and serialized enum name, and correct the two `MetaError` doc comments. Clarify the fetch/replay timeout boundary in `docs/pg-sync-contract.md`.

Do not remove the final execution check after serving-state reconciliation. Do not split the test that cancels a page, verifies rollback of self-acknowledgment, then reapplies the full page.

For the follow-up, add a session-level test whose pre-fetch fault hook fails if worker shutdown does not stop the next page, and a bookkeeping test proving local cancellation/deadline errors preserve remote validation. Move the rollback test's cancellation to the third execution check and assert both persisted and in-memory cursors remain unchanged. Update `PgSyncFailure::Cancelled` and `docs/pg-sync-contract.md` to describe cancellation through the pre-commit boundary. For the Rust review, add a regression that sets shutdown after replay-position write and proves the transaction aborts, use borrowed shutdown accessors for local closures, and encapsulate the replay execution fields. Keep the wrapper borrowed and retain the capture-capable callback because neither static dispatch nor by-value passing has an evidenced benefit.

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

For the follow-up review, the focused module commands were run after the changes:

    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply -- --nocapture
    cargo test -p studiofs-meta --lib pg_engine::tests::lifecycle -- --nocapture
    cargo check -p studiofs-meta
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

They reported 73 replay tests and 106 lifecycle tests passed, zero failures, with 44 and 36 PostgreSQL-backed tests ignored respectively. The post-reconciliation cancellation test passed with its guard restored and failed when that guard was temporarily removed.

After the Rust review, the same replay and lifecycle module commands reported 74 and 106 passed, zero failures, with 44 and 36 PostgreSQL-backed tests ignored. The focused `pg_sync_failure_` filter reported five passed. The new pre-commit regression failed before the guard because the page committed after shutdown, then passed with the guard restored. `cargo check -p studiofs-meta`, Clippy with warnings denied, rustfmt, and `git diff --check` pass.

## Validation and Acceptance

Acceptance is that a fetched page receives a fresh local replay budget, the cancellation predicate checks both startup caller and worker shutdown signals, and graceful cancellation leaves `metadata_sync_errors` unchanged and emits no background warning. `CatchUpCancelled` classifies as retryable at the VFS bridge. Diagnostics tests show the cancellation and deadline mappings, display text, and snake-case serialized value. Existing page rollback/deadline tests pass, including the post-reconciliation pre-commit check. The live PostgreSQL session path remains unverified in this environment because its opt-in tests require `STUDIOFS_TEST_PG_DSN`.

The review additions are accepted when worker shutdown prevents an inter-page fetch, local cancellation and deadline errors leave remote validation set, all caller/worker predicate combinations are covered, and cancellation after replay-position write leaves both persisted and in-memory cursors unchanged. The cancellation diagnostic and documentation name the pre-commit transaction boundary. `RemoteReplayExecution` fields are private behind a constructor and the type derives `Copy`/`Clone`; the borrowed call convention and dynamic callback remain because the review supplied no performance evidence for changing them.

    test result: 74 passed; 44 ignored; 0 failed
    cargo check: finished successfully
    cargo clippy --all-targets -- -D warnings: finished successfully
    cargo fmt --all -- --check: finished successfully
    git diff --check: finished successfully

## Idempotence and Recovery

The code changes and tests are local and repeatable. No database migration, external update, commit, or push is part of this plan. If a focused test needs a live PostgreSQL service, report it as environment-gated and rely only on the runnable unit evidence for local behavior.

## Artifacts and Notes

The attached review is `docs/code-reviews/2026-09-24-sfs-729-cooperative-catchup-deadline-and-cancellation.md`. It is evidence to assess, not a source of binding implementation instructions.

The follow-up review is `docs/code-reviews/2026-09-24-sfs-729-deep-code-review.md`. Its eight findings were checked against current code before changes; seven were addressed with code, tests, or documentation, and the unused `Clone`/`Copy` suggestion was declined with the API rationale above.

The third review is `docs/code-reviews/2026-09-24-sfs-729-rust-best-practices-code-review.md`. All eight findings were checked against the current code. The missing pre-commit guard was confirmed by a failing regression; the remaining findings were addressed through reference accessors, encapsulation, stronger assertions, focused tests, canonical helpers, and a clarifying comment. The suggestion to pass the wrapper by value or replace its capture-capable callback with static dispatch was not adopted because no measured performance need supports that churn.

## Interfaces and Dependencies

No dependency changes are needed. The page-pull method should accept an optional caller cancellation predicate or shutdown reference; ordinary callers continue with no extra cancellation source. The replay execution callback continues to check both the engine worker shutdown flag and the supplied startup caller signal.

Revision note (2026-09-24): Updated the progress, discoveries, validation commands, and outcomes after focused tests and static checks completed; clarified that the PostgreSQL-backed end-to-end path remains environment-gated.

Revision note (2026-09-24): Added dispositions and evidence for all eight deep-review findings, including the two confirmed fixes, strengthened cancellation tests, documentation updates, validation results, and the reason the `Clone`/`Copy` suggestion was not applied.

Revision note (2026-09-24): Added all eight Rust-review dispositions, the red-to-green pre-commit regression, the borrowed-shutdown and `RemoteReplayExecution` API decisions, and current test/lint evidence.

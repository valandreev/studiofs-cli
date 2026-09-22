# Retry fresh-client allocator bootstrap after transient PostgreSQL failure

This ExecPlan is living documentation for SFS-722. It follows `.agent/PLANS.md`; the `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` sections must remain current.

## Purpose / Big Picture

A fresh PostgreSQL-backed client that reaches its replay target but temporarily loses PostgreSQL while minting its paired allocator namespaces must remain safely read-only without becoming permanently stuck. After PostgreSQL is reachable again, the existing background synchronization path must retry the allocator bootstrap with bounded exponential backoff, persist the allocator pair to local `redb`, clear the fresh-client recovery marker, promote the lifecycle to `Live`, and allow the normal filesystem mutation path. A trusted but exhausted allocator must remain read-only and must not mint a replacement namespace.

Permanent allocator errors and local persistence/activation errors follow the same read-only boundary: they are retained as typed diagnostics and suppress further remote minting until local recovery succeeds. Only `RemoteUnavailable` and `Timeout` errors schedule another remote attempt.

The behavior is demonstrated by a focused regression that fails before the change when the marker is cleared and the lifecycle cannot recover, then passes after a transient bootstrap failure followed by a successful retry. Reopen coverage must prove that the durable marker survives the failed attempt and drives recovery on the next mount.

## Progress

- [x] (2026-09-22 16:56Z) Loaded SFS-722 from Linear; confirmed it is `In Progress`, P1, assigned to Valentine Andreev, with no comments or relations.
- [x] (2026-09-22 16:56Z) Recorded the clean `feature/sfs-722-retry-id-allocator-bootstrap-on-transient-remote-failure-to` worktree at `252e94f4`; no user changes were present.
- [x] (2026-09-22 16:56Z) Read the scoped metadata/VFS contracts, PostgreSQL sync contract, allocator authority plan, and crate instructions.
- [x] (2026-09-22 16:56Z) Traced `try_complete_catchup_sync`, `bootstrap_allocator_state_with_remote_recovery`, `record_metadata_sync_result`, and `start_background_poll`.
- [x] (2026-09-22 17:14Z) Added RED regressions for marker retention, retry after a transient mint failure, and the reopen recovery path.
- [x] (2026-09-22 17:14Z) Implemented the lifecycle/allocator retry change with in-memory exponential backoff capped at 30 seconds; durable local allocator state is restored before remote retry gating.
- [x] (2026-09-22 17:14Z) Preserved trusted exhausted-namespace read-only behavior and added reopen recovery coverage.
- [x] (2026-09-22 17:14Z) Passed focused tests, `cargo check -p studiofs-meta`, Clippy with `-D warnings`, formatting, and `git diff --check`.
- [x] (2026-09-22 17:16Z) Recorded final package-library evidence and the PostgreSQL-gated coverage boundary; no commit, push, or Linear mutation was performed.
- [x] (2026-09-22 17:45Z) Verified the attached review: released retry-state locking before remote mint, preserved backoff through poisoned mutexes, guarded allocator activation failures, refreshed lifecycle after slot registration, added bounded jitter, and covered the confirmed races with focused tests.
- [x] (2026-09-22 18:31Z) Verified the follow-up review: allocator bootstrap now returns `MetaResult`, records redacted `PgSyncFailure` diagnostics, retries only reachability errors, and suppresses repeated minting after permanent/local failures.
- [x] (2026-09-22 18:31Z) Added `permanent_allocator_bootstrap_failure_is_not_retried`; current allocator-bootstrap group passed 7/7 and the full metadata library passed 633/633 non-ignored tests.

## Surprises & Discoveries

- Observation: `try_complete_catchup_sync()` currently clears `LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER` before allocator bootstrap and unconditionally resets the frozen target at the end.
  Evidence: `crates/studiofs-meta/src/pg_engine.rs`, current lines 761-784.
- Observation: `bootstrap_allocator_state_with_remote_recovery()` records a failed mint only through a warning and refreshes lifecycle authority; it does not return success to its caller.
  Evidence: `crates/studiofs-meta/src/pg_engine/inode_allocator.rs`, current lines 171-225.
- Observation: the fallback poll selects normal metadata sync for `AllocatorPending`, while `try_complete_catchup_sync()` only accepts `CatchingUp` or `Degraded`.
  Evidence: `crates/studiofs-meta/src/pg_engine/lifecycle.rs`, current lines 520-535, and `crates/studiofs-meta/src/pg_engine.rs`, current lines 718-724.
- Observation: the existing allocator contract says trusted exhausted state stays read-only; only missing or untrustworthy runtime state is eligible for remote namespace minting.
  Evidence: `docs/plans/sfs-628-metadata-authority-with-local-allocators.md` and `allocate_with()` in `crates/studiofs-meta/src/pg_engine/inode_allocator.rs`.
- Observation: a retry must still load a durable local allocator pair immediately, even while a previous remote mint failure is in backoff.
  Evidence: `allocator_pending_retries_missing_allocator_authority` writes a valid local pair after the first failed attempt and must recover without waiting for the remote retry deadline.
- Observation: the existing fallback poll already reaches `try_complete_catchup_sync()` through `record_metadata_sync_result()`; no second worker is needed for allocator recovery.
  Evidence: the transient retry regression exercises the same completion entry point after a successful fallback-poll result.
- Observation: the previous retry helper reduced every allocator bootstrap error to `false`, so a permanent PostgreSQL allocator error or local `redb` replacement failure entered the same mint backoff as a reachability outage.
  Evidence: the new regression injects `MetaError::Storage("postgres allocator namespace exhausted")`, observes one mint attempt, and confirms the typed `PgSyncFailure::Storage` remains visible after the next sync tick.

## Decision Log

- Decision: Keep allocator retry inside the existing PostgreSQL metadata lifecycle and fallback poll instead of adding a second worker.
  Rationale: the poll already owns remote identity validation, shutdown, wakeups, and lifecycle synchronization; a second worker would duplicate ownership and shutdown paths.
  Date/Author: 2026-09-22 / Codex.
- Decision: Treat `allocator_runtime_state_missing()` as the retry eligibility boundary.
  Rationale: it identifies missing/untrusted local authority after bootstrap failure, while trusted counter exhaustion must remain read-only and must not mint a new namespace.
  Date/Author: 2026-09-22 / Codex.
- Decision: Clear the fresh-client marker and frozen startup target only after the allocator state has been durably replaced in local `redb`.
  Rationale: a later remount must be able to distinguish unfinished authority bootstrap from a completed fresh client.
  Date/Author: 2026-09-22 / Codex.
- Decision: Use an in-memory bounded exponential retry schedule for the active mount, reset on successful durable bootstrap, and preserve the durable marker across close/reopen.
  Rationale: retry timing is runtime coordination state; the marker is the durable recovery contract. No new on-disk schema is needed.
  Date/Author: 2026-09-22 / Codex.
- Decision: Try the local durable allocator state before applying the retry deadline to remote minting.
  Rationale: local redb remains authoritative for foreground recovery, and a pair that became durable must not be delayed by a previous remote failure.
  Date/Author: 2026-09-22 / Codex.
- Decision: Reserve allocator retry attempts with an in-flight marker, release the retry mutex before PostgreSQL minting, and recover poisoned retry guards with `into_inner()`.
  Rationale: the existing fallback poll remains single-flight for the retry state without holding an in-memory lock across bounded network I/O; a poisoned mutex must not disable the backoff schedule.
  Date/Author: 2026-09-22 / Codex.
- Decision: Treat allocator-runtime activation failure as a failed bootstrap and keep both headroom flags closed.
  Rationale: lifecycle promotion requires an actually installed paired runtime; a poisoned allocator mutex must never produce a `Live` status from stale atomics.
  Date/Author: 2026-09-22 / Codex.
- Decision: Add bounded jitter to allocator retry delay using the existing `rand` dependency and checkpoint retry convention.
  Rationale: independent clients should not replay allocator mint requests at identical deterministic deadlines after a shared PostgreSQL outage.
  Date/Author: 2026-09-22 / Codex.
- Decision: Keep allocator retry defaults internal rather than adding environment configuration.
  Rationale: SFS-722 has no deployment requirement for tuning these values, and the deterministic test-only wakeup already avoids test sleeps without expanding the runtime configuration surface.
  Date/Author: 2026-09-22 / Codex.
- Decision: Make allocator bootstrap return `MetaResult<()>` and classify retryability with `MetaError::is_reachability_error()`.
  Rationale: the caller must retain the actual failure category; only `RemoteUnavailable` and `Timeout` represent a safe reason to mint another remote namespace pair. `Storage`, poisoned local state, and activation failures remain read-only and stop remote minting.
  Date/Author: 2026-09-22 / Codex.
- Decision: Preserve a permanent allocator diagnostic across a successful metadata-sync tick while retry is suppressed, then clear it only after allocator recovery.
  Rationale: metadata transport success does not grant allocator mutation authority; clearing the only diagnostic before the suppressed retry runs would hide the reason the lifecycle remains `AllocatorPending`.
  Date/Author: 2026-09-22 / Codex.

## Outcomes & Retrospective

Implementation complete. Validation evidence:

- `cargo test -p studiofs-meta --lib allocator_bootstrap -- --nocapture`: 7 passed, 0 failed.
- `cargo test -p studiofs-meta --lib catching_up_fresh_client -- --nocapture`: 2 passed, 0 failed.
- `cargo test -p studiofs-meta --lib degraded_fresh_client -- --nocapture`: 5 passed, 0 failed.
- `cargo test -p studiofs-meta --lib --quiet`: 633 passed, 0 failed, 137 ignored.
- `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed.

No live PostgreSQL or mounted native acceptance ran because the service-backed tests remain environment-gated; no commit, push, or external Linear mutation was made.

## Context and Orientation

`PgMetaEngine` in `crates/studiofs-meta/src/pg_engine.rs` owns the local-first PostgreSQL lifecycle. Foreground metadata remains in local `redb`; PostgreSQL is used for identity, replay, client-slot registration, and allocator namespace minting. `PgSyncLifecycle::CatchingUp` and `Degraded` represent unfinished remote convergence. `TargetReached` means the frozen replay target is complete but does not grant mutation authority. `AllocatorPending` means the paired local allocator authority is unavailable. Only `Live` plus both allocator headroom flags permits new mutations.

`try_complete_catchup_sync()` is called after a successful metadata sync by `record_metadata_sync_result()` in `crates/studiofs-meta/src/pg_engine/metadata_sync.rs`. It reserves a fresh client slot, promotes the lifecycle, and bootstraps missing allocator state. `bootstrap_allocator_state_with_remote_recovery()` in `crates/studiofs-meta/src/pg_engine/inode_allocator.rs` mints a paired inode/payload namespace through PostgreSQL and then calls `RedbMetaEngine::replace_allocator_state`; that local write is the durability boundary. `start_background_poll()` in `crates/studiofs-meta/src/pg_engine/lifecycle.rs` owns the periodic fallback sync and its shutdown condition.

The durable fresh-client marker is `LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER` (`pgFreshClientPending`). A nonzero value means fresh-client authority bootstrap is unfinished. It is copied through checkpoint import and must remain nonzero until the allocator pair is durably stored. The implementation must not move PostgreSQL into the foreground mutation path, clear the marker on failure, retry trusted exhausted namespaces, or use sleeps as test synchronization.

## Plan of Work

First add one focused test in the existing `studiofs-meta` lifecycle test module that starts at a reached fresh-client target with no allocator runtime, injects one transient mint failure, and proves the marker remains nonzero, lifecycle remains read-only, and no allocator runtime is installed. The test then makes the next bootstrap succeed and proves local allocator state is durable, the marker is cleared, lifecycle is `Live`, and a normal ID allocation succeeds. Add a pure retry-delay test if the schedule is not already directly covered.

Then make allocator bootstrap return `MetaResult<()>` so it exposes the actual remote, local persistence, or activation error instead of collapsing every failure into `false`. Keep the existing warning and closed headroom behavior on failure. In `try_complete_catchup_sync()`, accept the eligible `AllocatorPending` recovery state, retain the fresh marker and frozen target on failure, and perform the final marker/target/progress cleanup only after successful durable allocator installation. The path must continue to leave trusted exhausted state untouched.

Add the smallest retry-state holder to `PgMetaEngine` and use it only when allocator runtime state is missing/untrustworthy. The first attempt is immediate; later attempts are gated by an in-memory exponential delay capped at a finite maximum. Remote identity must be validated before a retry, shutdown must remain owned by the existing poll worker, and success resets the delay. Do not add a new worker, schema, dependency, or foreground remote call.

Add reopen coverage around the durable marker: close an engine after a failed bootstrap, reopen its local database while the marker is still set, and prove the reopened client remains read-only and eligible for later remote bootstrap. If an actual PostgreSQL recovery path is required to exercise the final reopen-to-write transition, keep that test ignored behind the repository's `STUDIOFS_TEST_PG_DSN` gate and report it separately.

## Concrete Steps

Work from `/Volumes/Storage/dev/studiofs`.

1. Write the focused regression test and run it alone:

       cargo test -p studiofs-meta --lib transient_allocator_bootstrap -- --nocapture

   The initial run failed as expected (`left: 0`, `right: 1` for the durable marker); after the fix the test passes.

2. Implemented the minimal code change in `pg_engine.rs` and `pg_engine/inode_allocator.rs`; the existing `pg_engine/lifecycle.rs` poll remains the retry owner. The test-only mint hook follows the existing fault-hook pattern. The bootstrap methods return `MetaResult<()>`, while retry state stores only scheduling/suppression state and diagnostics use the existing redacted `PgSyncFailure` categories.

3. Ran the focused lifecycle/allocator tests with one filter per command; each reported nonzero tests:

       cargo test -p studiofs-meta --lib catching_up_fresh_client -- --nocapture
       cargo test -p studiofs-meta --lib degraded_fresh_client -- --nocapture
       cargo test -p studiofs-meta --lib allocator -- --nocapture

4. Ran package-level validation:

       cargo check -p studiofs-meta
       cargo clippy -p studiofs-meta --all-targets -- -D warnings
       cargo fmt --all -- --check
       git diff --check

5. [x] Ran the broader metadata library suite, recorded ignored PostgreSQL tests separately, inspected the final diff, and updated this plan. No mounted or live PostgreSQL acceptance is claimed from unit tests.

6. [x] Verified the review regression and retry classification:

       cargo test -p studiofs-meta --lib permanent_allocator_bootstrap_failure_is_not_retried -- --nocapture
       cargo test -p studiofs-meta --lib allocator_bootstrap -- --nocapture

   The permanent failure test proves exactly one mint attempt, a `Storage` diagnostic, and continued `AllocatorPending`; the transient group proves reachability retry behavior remains intact.

## Validation and Acceptance

The focused regression is accepted only when it fails before the production change and passes after it. Its observable assertions are: a transient mint failure leaves `pgFreshClientPending == 1`, no allocator authority/headroom, and no `Live` lifecycle; the next due retry durably installs the paired allocator state, changes lifecycle to `Live`, clears the marker, and allocates an ID. A trusted exhausted allocator test must continue to report `AllocatorPending` without invoking the mint path. The permanent-failure regression must retain `PgSyncFailure::Storage` and perform no second mint after a forced retry wake-up.

The reopen test is accepted when a failed fresh bootstrap survives engine close and ordinary reopen with the marker still set and writes still rejected. A service-backed test may additionally prove that once PostgreSQL is restored, a later background retry reaches `Live`; it must use the repository's configured non-secret `STUDIOFS_TEST_PG_DSN` and be reported as ignored when unavailable.

The final report must distinguish focused unit results, package compile/Clippy/format results, ignored service-backed tests, and any unavailable native mounted acceptance. It must not include credentials, full DSNs, or secret-bearing diagnostics.

## Idempotence and Recovery

The plan is safe to repeat because tests use isolated temporary `redb` directories and the retry schedule is in-memory. If a test or implementation attempt fails, preserve the worktree, inspect the failing assertion, and return to the smallest failing regression; do not reset or discard unrelated changes. A failed transient remote mint may reserve an unused PostgreSQL namespace before local persistence succeeds, but the pair is not authoritative until `replace_allocator_state` succeeds; only reachability failures may mint another pair, while permanent/local failures remain suppressed and read-only.

## Artifacts and Notes

The primary artifacts are the focused test output, the final diff for the three metadata lifecycle/allocator files and their tests, and this plan. The branch is already named for SFS-722. No commit, push, pull request, or Linear comment is part of this execution unless explicitly requested.

## Interfaces and Dependencies

The final implementation keeps these existing interfaces and adds no external dependency:

- `PgMetaEngine::try_complete_catchup_sync()` remains the lifecycle completion entry point used by metadata synchronization.
- `PgMetaEngine::bootstrap_allocator_state()` and `bootstrap_allocator_state_local_only()` return `MetaResult<()>`; their shared `bootstrap_allocator_state_with_remote_recovery()` path remains the single path that mints or loads the paired allocator state and exposes the actual `MetaError` to its caller.
- `RedbMetaEngine::replace_allocator_state()` remains the local durability boundary.
- `PgSyncLifecycle::AllocatorPending` remains read-only and transitions to `Live` only after identity validation and paired allocator headroom are true.
- The existing `PgBackgroundWorkers` poll worker remains responsible for shutdown and retry scheduling.
- `MetaError::is_reachability_error()` is the retry gate; `PgSyncFailure::from_error()` is the safe diagnostic projection for allocator failures.

Plan created at 2026-09-22 16:56Z; revise all living sections when implementation evidence changes the design.

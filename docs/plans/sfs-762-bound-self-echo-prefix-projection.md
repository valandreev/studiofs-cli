# Bound self-echo prefix projection

This ExecPlan follows `.agent/PLANS.md` and is maintained as implementation proceeds.

## Purpose / Big Picture

After a PostgreSQL outage, replay of this client's own published events must acknowledge the earliest matching local journal prefix without repeatedly projecting every growing prefix while redb holds its writer transaction. Local file publication and acknowledgement order must remain unchanged.

## Progress

- [x] (2026-09-30) Read SFS-762, the metadata contracts, projection code, publication caller, and replay caller; verified a clean task branch.
- [x] (2026-09-30) Add a deterministic work regression exercising transactional replay with long local-only backlogs and late/missing matches.
- [x] (2026-09-30) Observe the regression fail: 525,825 prefix-input visits for a 1,024-event backlog.
- [x] (2026-09-30) Implement incremental context and exact ordered comparison, sharing projection rules with publication; borrow unchanged events rather than storing a second event vector.
- [x] (2026-09-30) Validate four new regressions, 81 apply tests, 62 backlog tests, three layout checks, four live PostgreSQL tests, and crate gates.

## Surprises & Discoveries

Projection is not append-only: a committed attr or publish can reveal earlier events, and a draft can hide earlier events. The adjacent parent attr of a hidden regular-file AddEntry is hidden too. The graph's acknowledgement call edges were incomplete, so the actual caller was inspected directly. The test support acknowledgement path duplicated the production prefix loop and now uses the same matcher. The producer bounds each batch by response bytes, not a separate event-count cap; the default byte budget is 8 MiB in `pg_engine.rs`, and `metadata_io.rs::split_projected_event_batches` enforces it. A second vector of cloned events was unnecessary: `Cow` borrows unchanged events and owns only normalized regular-file attrs.

## Decision Log

- Decision: Use monotonic per-inode facts and an ordered set of visible source indices, sharing the publication projector's rules.
  Rationale: An inode's visibility can change at most four times: unready evidence, committed attr, empty draft, and qualifying publish. Re-evaluating only dependent source indices, including adjacent parent attrs, preserves exact equality without hashes or collision handling.
  Date/Author: 2026-09-30 / Codex.

Compare only when the visible sequence changes and its count equals the received batch count. Exact comparisons are bounded by the received batch size, whose cardinality and payload bytes are limited indirectly by the producer's response-byte envelope. Overall work is O(n log n + n b) for n local events and b received events; memory is O(n) for the visibility/dependency indices without cloned event storage. This removes quadratic growth in the local backlog for a fixed configured remote envelope; it does not claim O(n) for unbounded remote batches. Deadline and cancellation checks run during scanning, dependency updates, and comparisons, retaining the existing operation-time cap. No arbitrary backlog cutoff may lose a late match.

- Decision: Retain exact comparison and the existing operation deadline; borrow unchanged events with `Cow`.
  Rationale: The configured byte envelope bounds each comparison. Borrowing avoids a second event-sized allocation per backlog entry, and inner-loop execution checks make large dependency updates interruptible without truncating the match search.
  Date/Author: 2026-09-30 / Codex.

## Outcomes & Retrospective

The repeated-prefix loop is removed from both acknowledgement paths. Publication and matching share the same monotonic hidden-inode facts, adjacency predicate, and regular attr normalization. Four new regressions cover long late/missing matches, all four visibility transitions, overlay-only hiding, earliest acknowledgement, and cancellation during a large dependency reveal. A read-only subagent review found no semantic mismatch against the original projector.

Final local checks passed: 81 apply tests (44 infrastructure tests ignored in this selection), 62 backlog/restoration tests (one infrastructure test ignored), and three structural layout checks. Four specifically selected PostgreSQL tests ran against the configured live service and passed; the full ignored suite was not run. `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, formatting, and diff checks passed. Work remains uncommitted on the existing task branch; there is no PR. An English evidence comment was posted to SFS-762 and the issue moved to In Review.

## Context and Orientation

The repository root is `/Volumes/Storage/dev/studiofs`, on `feature/sfs-762-bound-repeated-local-event-prefix-projection-during-remote`. `crates/studiofs-meta/src/pg_engine/remote_apply.rs` projects local journal events into remote-visible events and applies replay pages in a redb write transaction. Self-echo means a PostgreSQL batch written by this same client. A prefix match advances the contiguous local flushed watermark; it must not consume a later local-only suffix. `crates/studiofs-meta/src/pg_engine/tests/remote_apply.rs` owns these regressions. Publication uses the same projector in `replication_flush.rs`.

## Plan of Work

First exercise real transactional replay after inserting 1,024, 2,048, and 8,192 local pending-deletion events followed by one visible xattr. Count event visits with test-only thread-local instrumentation and assert the exact earliest acknowledgement or no match and a linear work ceiling. This fails on the original loop. Then replace repeated-prefix projection with one incremental scan. Share hidden-inode evidence and per-event contextual visibility with the batch projector, keeping regular attr size normalization unchanged. Use hand-derived expected event vectors and prefix endpoints to check context transitions independently of the shared projector. Check cancellation inside dependency work and retain transaction rollback.

## Concrete Steps

Run commands from the repository root:

    cargo test -p studiofs-meta --lib self_batch_prefix_matching_bounds_work -- --nocapture
    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::
    cargo test -p studiofs-meta --lib pg_engine::tests::replication_backlog::
    cargo test -p studiofs-meta --test pg_engine_test_layout
    cargo check -p studiofs-meta
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

## Milestones

The reproduction milestone creates the long-backlog transactional regression and observes its failure on the original loop. Its completion evidence is the 525,825-visit red result at backlog 1,024.

The implementation milestone replaces both loops with the shared incremental matcher and proves context changes, earliest acknowledgement, and cancellation with the four `self_batch_prefix_matching` tests. The measured workload grows from 3,077 to 6,149 visits when the backlog doubles and still matches exactly at backlog 8,192.

The validation milestone runs the focused apply and backlog modules, layout checks, and crate gates, then the four exact PostgreSQL selections below. It is complete when all recorded nonzero selections pass and the final diff contains only the task's source, tests, and plan.

Run only task-relevant ignored PostgreSQL tests using `pg_dsn()`, which loads `.env` without displaying secrets. Do not run the workspace or full ignored suite.

The following exact service selections each ran one test and passed:

    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::test_pg_reopen_after_publish_before_local_persist_recovers_without_discover -- --ignored --exact --nocapture
    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::test_pg_reopen_after_claimed_multi_event_batch_preserves_sequence_without_discover -- --ignored --exact --nocapture
    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::test_pg_overlay_allows_unrelated_remote_batches_to_apply -- --ignored --exact --nocapture
    cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::test_pg_nested_temp_rename_then_publish_visible_on_remote_peer -- --ignored --exact --nocapture

## Validation and Acceptance

The long-backlog regression must return the same late acknowledgement and no-match result and visit at most a constant multiple of the input length for every measured backlog size. Small contextual fixtures must preserve earliest match, hidden parent attrs, committed attr reveal, publish reveal, draft hiding, and later suffix retention. The focused apply selection must pass with nonzero test counts, followed by crate check, Clippy, formatting, and diff checks. PostgreSQL test failures or unavailable services are reported separately.

## Idempotence and Recovery

Tests use RAII temporary directories under `target/`; transactional test writes are aborted or discarded with the temporary database. No volume migrations or persistent wire changes are required. Preserve unrelated changes and do not commit or publish without user authorization.

## Artifacts and Notes

The red regression measured 525,825 prefix-input visits at backlog 1,024; this conservative old counter did not count the projector's other passes. The final counter includes context updates, visibility checks, event conversion, and equality checks. The final transactional test reported:

    backlog=1024 match=true  projection_work=3077  replay_us=1215
    backlog=1024 match=false projection_work=3077  replay_us=936
    backlog=2048 match=true  projection_work=6149  replay_us=1877
    backlog=2048 match=false projection_work=6149  replay_us=1762
    backlog=8192 match=true  projection_work=24581 replay_us=7201
    backlog=8192 match=false projection_work=24581 replay_us=7219

These debug-build timings characterize this local redb fixture, including journal scanning and replay work within an already opened transaction; they are not mounted E2E latency or a production latency guarantee. The deterministic work ceiling is the acceptance check. The existing replay deadline, checked inside the new loops, caps interruptible lock-held work; no new arbitrary backlog threshold is introduced.

## Interfaces and Dependencies

Use existing MetaEvent, RemoteReplayExecution, RedbMetaEngine, and standard-library HashMap, HashSet, and BTreeSet. No new dependencies or public APIs are needed. Both acknowledgement entry points delegate to the same prefix matcher; publication continues to return ProjectedRemoteBatch.

Initial revision: record the verified root cause and incremental strategy before production edits. Completion revision: record red/green counts, borrowed projection, exact complexity limits, and final local/live-service verification.

## Review follow-up (2026-09-30)

Reviewed `docs/code-reviews/2026-09-30-sfs-762-bound-self-echo-prefix-projection.md` against commit `7280cbe7` and current source. The graph index predates the implementation, so changed symbols were verified directly.

- Findings 1 and 2: confirmed redundant struct copies. Visibility now uses a shared projectability predicate; already normalized regular attrs remain borrowed. The allocation claim is incorrect: `InodeAttr` has scalar fields and `Cow::Owned` does not inherently allocate. New regressions measured 65 versus 1 normalization copies and 2 versus 0 respectively before/after.
- Finding 3: confirmed duplicate current-index visibility work; skip that index in dependency revisits. The suggested duplicate previous-index case does not occur: `AddEntry` does not change projection state. A distinct-inode regression failed before correction and passes afterward.
- Finding 4: rejected the proposed guards. Earlier `RemoveAttr` dependencies must survive until later inode context arrives, even when `states` is initially empty. The new `self_batch_late_context_revisits_earlier_remove_attr` fixture agrees with canonical batch projection. Nonregular attrs are also contextually filtered by inode; restricting dependencies requires an additional invariant, not just an event-kind guard.
- Finding 5: skipped speculative capacity sizing. Local event count does not bound useful unique-key cardinality tightly; the proposed heuristic overreserves for local-only backlogs and early matches without measured benefit.
- Finding 6: added fixture-specific explanations for the unchanged 8x ceilings. Adjusted the cancellation trigger to the reduced scan cost so cancellation still occurs during the reveal dependency loop.

TDD: the focused `self_batch_` selection first ran 13 tests with three expected failures, then passed all 13. An initial abbreviated `--exact` filter selected zero tests and is excluded from evidence. Final deterministic projection-work counts for both late/missing matches: 2,052 at backlog 1,024; 4,100 at 2,048; 16,388 at 8,192. Counts changed because visibility no longer calls transformation.

Validation commands/results:

- `cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::`: 85 passed, 44 ignored.
- `cargo test -p studiofs-meta --lib replication_backlog`: 62 passed, 1 ignored.
- `cargo test -p studiofs-meta --test pg_engine_test_layout`: 3 passed.
- `cargo check -p studiofs-meta`: passed.
- `cargo clippy -p studiofs-meta --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

Live PostgreSQL and mounted E2E were not rerun for this review follow-up; previous implementation evidence remains separate. No commit or external issue update is part of this follow-up.

## Updated review follow-up (after ca745723)

The replaced review covers `7280cbe7` and `ca745723`; prior findings above refer to the earlier report.

- Finding 1 confirmed: share the already computed hidden-inode set across publication projection and serving-replay detection, preserving source indices and flags. The new publication work regression failed at 256 visits for 64 events, then passed at 192.
- Finding 2 deferred: per-inode vectors do allocate, but no allocation/latency measurements justify a custom intrusive list. One event index can depend on both its own inode and the next regular dentry's inode (parent SetAttr adjacency), so one `prev_dependent[index]` link cannot represent all dependency edges safely without extra nodes/structure. Existing linear storage remains.
- Finding 3 confirmed: projectability now short-circuits contextual visibility for local-only events. The new regression failed at 128 visits for 64 events, then passed at 64.
- Finding 4: documented that eager-flush detection receives projected events; retained exhaustive non-eager local-only arms.
- Finding 5: added a passing two-directory hardlink regression. Both dentries remain hidden before publication, both reveal together, and a partial single-link echo cannot advance the watermark.

The report's `O(B)` exact-comparison claim is incorrect. Visibility cardinality can equal B multiple times as inode context hides/reveals events. Complexity remains `O(N log N + M * B)` for M comparison attempts (at most a constant multiple of N), with additional payload comparison cost. Event-index/state storage is O(N); borrowed comparisons avoid cloning unchanged payloads but do not eliminate map/vector/tree allocations.

Verification: `cargo test -p studiofs-meta --lib pg_engine::tests::remote_apply::` passed 88 tests (44 ignored); `cargo test -p studiofs-meta --lib replication_backlog` passed 62 (1 ignored); `cargo test -p studiofs-meta --test pg_engine_test_layout` passed 3. `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, formatting and diff checks passed. PostgreSQL and mounted E2E were not rerun. No commit or external update was made.

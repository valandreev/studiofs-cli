# Propagate the remaining redb removal errors

This ExecPlan is maintained under `.agent/PLANS.md`. Repository root is
`/Volumes/Storage/dev/studiofs`; branch is
`feature/sfs-760-propagate-redb-table-removal-errors-from-metadata-write`.
The starting checkout is clean at `54e85098`.

## Purpose / Big Picture

Failed redb deletions must abort the enclosing metadata transaction, preserving
earlier writes and the removed row. SFS-760 already fixes the helpers in
`txn.rs`. The updated review identifies the same fault in replica application;
source verification also finds it in the public ACL operations. ACL means an
access-control rule; a tombstone is a row hiding a rule from the shared base.
Focused fault tests will demonstrate rollback and absent-key success.

## Progress

- [x] (2026-09-30 19:28Z) Verify the report against the current source and ticket.
- [x] (2026-09-30 19:41Z) Three new regressions fail on committed value 8 instead
  of expected 7 before production changes; all seven transaction tests pass after.
- [x] (2026-09-30 19:41Z) Propagate the 15 replica and 2 ACL removal errors.
- [x] (2026-09-30 19:41Z) Move the existing error conversion to the redb adapter root and simplify
  `insert`/`get` mappings in `txn.rs` without changing behavior.
- [x] (2026-09-30 19:41Z) Redb suite: 300 passed, 1 manual churn test ignored;
  formatting and diff checks pass. Independent diff review found no blockers.
- [x] (2026-09-30 19:43Z) Package check and Clippy with `-D warnings` pass.

## Surprises & Discoveries

The report's `chunk_state.rs` deletion examples already propagate errors with
`.map_err(...)?`; discarding the successful `Option` is harmless. The proposed
`remove_acl_in_txn` function does not exist. Actual ACL cleanup is inside
`clear_inode_overlay_aux_in_txn_with_options`, and an empty inode never enters
its deletion loop. Existing ACL cascade tests cover a populated rule.

## Decision Log

- Decision: Fix verified replica and ACL siblings; leave chunk removal unchanged.
  Rationale: These 17 calls discard the same storage errors as the original bug.
  Date/Author: 2026-09-30, Codex.
- Decision: Keep the fault test's corruption discriminator and fail on layout drift.
  Rationale: A backend I/O error can poison redb and would not demonstrate that
  an ignored, non-poisoning corruption error permits a partial commit. A fallback
  or generic storage-error assertion can weaken the evidence.
  Date/Author: 2026-09-30, Codex.
- Decision: Keep the three malformed-buffer cases in one test of one behavior.
  Rationale: No new test framework or repeated fixture is needed; case diagnostics
  can identify a failure. Existing test-module organization is separate cleanup.
  Date/Author: 2026-09-30, Codex.

## Context and Orientation

`crates/studiofs-meta/src/redb_engine/replication.rs` applies remote events to the
base and local serving tables. `crates/studiofs-meta/src/redb_engine/acl.rs`
implements public ACL setters and removals. Both own or participate in write
transactions and must return an error before commit. Their callers already use
`?`. `crates/studiofs-meta/src/redb_engine/tests/txn.rs` contains a custom
in-memory redb backend that corrupts a single branch-page read without modifying
stored bytes. Its existing regression verifies rollback after reopening.

## Plan of Work

Reuse that fault backend in `tests/txn.rs`. Construct a test-only engine around
the backend using the existing engine fields and immediate durability, avoiding
new production constructors or worker lifecycles. Seed enough pending-deletion
or ACL rows to force a branch page and find its offset through an actual lookup.
For replica apply, mutate a counter in the same transaction before an event
removes the pending-deletion row. For ACL operations, assert that a failed rule
or tombstone removal preserves the rule and event sequence after reopening.
Run each new regression against the original code before changing production.

Replace the 17 ignored removal results with `?`. Preserve absent-key `Ok(None)`
handling and existing event/invalidation ordering. After green, move the existing
`From<redb::StorageError> for MetaError` from `txn.rs` to `redb_engine.rs`; the
port declaration in `engine.rs` must remain independent of the redb adapter.
Simplify only equivalent storage-error mappings on `insert` and `get` in
`txn.rs`. Keep mappings for other redb error types and serialization.

## Concrete Steps

From the repository root, run focused new tests first and expect failure because
an earlier write or ACL event is committed despite the deletion error. After the
fix, run:

    cargo test -p studiofs-meta --lib redb_engine::tests::txn -- --nocapture
    cargo test -p studiofs-meta --lib redb_engine::tests -- --quiet
    cargo check -p studiofs-meta
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

## Validation and Acceptance

Every fault must be consumed by the intended delete and return a corruption
storage error. Reopening must retain the previous row and counter/event sequence.
Deleting absent keys must still succeed, including the corrected ACL and replica
paths. All focused redb tests must pass with a nonzero test count. The existing
manual redb churn test remains ignored; no remote service or live mount is needed
for this in-memory storage behavior.

## Idempotence and Recovery

Tests use isolated in-memory databases and existing temporary test fixtures.
Reruns do not affect user data. Preserve existing commits and stage nothing;
commit, push, PR, and external issue updates are outside this request.

## Interfaces and Dependencies

No dependencies, public signatures, or storage schema change. Continue mapping
`redb::StorageError` to `MetaError::Storage(String)` inside the adapter.

## Outcomes & Retrospective

The confirmed 17 sibling deletion errors are propagated. A test-only engine
fixture wraps the existing in-memory fault backend without a durability worker.
Three new regressions prove rollback in replica pending-deletion application,
ACL rule deletion, and ACL tombstone clearing during a setter. The absent-key
regression exercises all three replica apply helpers and public ACL deletion.
The adapter-root conversion replaces 24 insert and 26 get error mappings with
the same `MetaError::Storage` behavior. Package check and Clippy with `-D warnings`
pass, as do formatting and diff checks. All intended changes remain uncommitted.

The three red failures all observed a committed value of 8 instead of 7;
afterward `redb_engine::tests::txn` reports 7 passed and the redb suite reports
300 passed / 1 ignored. The current diff preserves the precise corruption
discriminator. The inaccurate chunk/ACL-helper suggestions and speculative
fallback/module standardization were not implemented.

Plan created after validating the updated review to record the expanded sibling
fix and the suggestions that would weaken or misrepresent the regression.

2026-09-30 19:41Z: Updated implementation progress and captured red-to-green,
idempotence, suite, and independent diff-review evidence; compiler gates pending.

2026-09-30 19:43Z: Recorded passing compiler/lint gates and final completion.

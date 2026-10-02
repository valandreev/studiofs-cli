# Align upload queue ownership and cache pins

This ExecPlan is a living document maintained under `.agent/PLANS.md`.

## Purpose / Big Picture

After an upload finishes, its payload must become evictable once every real user
has released it. Repeating an older upload submission or repeating an object ID
in a batch currently leaves extra cache pins behind. SFS-752 removes those extra
references without letting an active upload lose its protection from eviction.

## Progress

- [x] (2026-10-02 09:08Z) Read SFS-752, repository contracts and relevant source;
  confirmed the matching branch and clean working tree.
- [x] (2026-10-02 09:12Z) Five cache regressions failed before the fix; a sixth
  bounded backend-transition regression reproduced released logical locking.
- [x] (2026-10-02 09:19Z) Exposed queue ownership acquisition and balanced
  single/batch provisional pins; closed both existing lock-scope races.
- [x] (2026-10-02 09:21Z) Focused cache tests, crate check/Clippy and independent
  source review passed; three focused VFS rebase tests also passed.
- [ ] Commit/push all task files and create a regular PR; update Linear to In Review.

## Surprises & Discoveries

Queue entries are unique by object ID. A newer content epoch replaces the entry
but retains the same ownership; an ignored submission retains it unchanged.
Worker-channel closure rolls a submission back. Local dirty flags independently
prevent eviction even if a closed channel cannot accept a new queue owner.

Record/update and dispatch rollback previously used separate queue lock scopes.
Concurrent closed-channel submissions could restore a row after both callers
released provisional pins. Cache logical first/final pin transitions also
released their lock before calling the backend, allowing another transition to
overtake the backend change.

The closed-dispatch regression failed at the exact rollback boundary before
the fix (`submit lock must cover rollback`), then passed with atomic submission.

## Decision Log

Keep protection in place before publishing an operation to workers. Use the
queue's submission outcome to release only provisional references that did not
create ownership. Retain the native backend batch-pin path and existing rebase
and retry semantics. No new dependency or remote operation is needed.
Date/author: 2026-10-02, Codex.

Serialize queue record/update, dispatch and rollback under its existing mutex.
Hold the existing logical pin mutex through first/final backend transitions,
including native batch pins. These existing locks make ownership results safe
to consume without adding a new synchronization layer.
Date/author: 2026-10-02, Codex.

## Outcomes & Retrospective

Implementation is complete. Eight SFS-752 regressions, 21 queue tests and 10
pin-filtered tests passed (filters overlap). Three focused VFS worker rebase
tests passed. Cache check/Clippy, formatting and whitespace checks passed.
Independent source review found no actionable correctness issue. Commit/PR
delivery remains. Full workspace suites and live mounted acceptance were not run.

## Context and Orientation

Work from `E:\dev\studiofs-cli` on
`feature/sfs-752-keep-cache-pin-counts-aligned-with-accepted-upload-queue`.
`crates/studiofs-cache/src/cached_store.rs` counts runtime references to payloads
and forwards its first/final references to the local backend. A pin prevents
eviction. `crates/studiofs-cache/src/upload_queue.rs` owns one pending operation
per object ID, wakes the background worker and removes entries on completion or
cancellation. Retry/rebase replaces an existing operation without creating a
second owner. `crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs` can fall
back from a stale requeue to another submission; that must not add a reference
when the queue already owns the object. Durable payload lifecycle state is
stored in the local backend; runtime queue/pin state is rebuilt on startup.

## Plan of Work

First add cache tests using the real local backend for repeated stale submits,
duplicate batch IDs, newer epoch replacement, worker-channel closure and failed
retry. Verify both logical reference counts and backend eviction eligibility.
Then change queue submission to report whether it acquired a new object owner,
and release provisional pins when it did not. Preserve batch backend calls and
per-operation ordering. Review concurrent publication and terminal paths so
completion cannot run before an accepted operation has backend protection.
Finally document the focused gate in `docs/tests.md` and deliver a regular PR.

## Concrete Steps

All commands run from `E:\dev\studiofs-cli` in PowerShell. Run Cargo sequentially.

    cargo test -p studiofs-cache --lib sfs752_ -- --nocapture
    cargo test -p studiofs-cache --lib upload_queue::tests -- --nocapture
    cargo test -p studiofs-cache --lib pin -- --nocapture
    cargo test -p studiofs-vfs --lib process_upload_rebases -- --nocapture
    cargo check -p studiofs-cache
    cargo clippy -p studiofs-cache --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

Observed results: 8, 21, 10 and 3 passed respectively, with no ignored or failed
tests. Cache check/Clippy and format/whitespace checks exited successfully.
The initial six cache regressions and queue lock-boundary regression failed
before their fixes. If a shared Cargo target is locked, reuse an isolated target
directory under `target/`.

## Validation and Acceptance

Repeated same/older submissions and duplicate batch inputs leave one queue pin
per pending object. Stale completion leaves the newer owner pinned. Marking the
object uploaded and completing its last owner leaves zero logical pins and makes
the real backend payload evictable. Failed upload/retry retains its owner and
dirty payload. Closed-channel submissions do not leak provisional pins. These
tests require no PostgreSQL, S3 or mount. Unit checks do not prove live mounted IO.

## Idempotence and Recovery

Tests use temporary directories under `target/`; no persistent user data or
configuration is modified. Repeating validation is safe. Preserve unrelated
changes and never reset the checkout. Queue failures preserve existing owners;
local dirty state remains the replay source.

## Artifacts and Notes

Issue: https://linear.app/valandreev/issue/SFS-752/keep-cache-pin-counts-aligned-with-accepted-upload-queue-entries
Graph generation 2026-10-02T09:01:39Z reported metadata changes for relied-on
source files, so graph discovery was followed by direct source reads.

## Interfaces and Dependencies

`UploadQueue::submit` and `submit_with_payload` must expose new object ownership,
including rollback on dispatch failure. `CachedStore` consumes that outcome in
both single and batch submissions. Existing complete/cancel/retry/rebase APIs
and local-backend lifecycle methods retain their contracts.

Revision 2026-10-02: recorded the six red cache regressions and the two existing
lock-scope races that must be closed before compensating provisional pins.
Revision 2026-10-02: recorded the implemented ownership outcome, passing focused
checks and independent source review; delivery is the remaining step.

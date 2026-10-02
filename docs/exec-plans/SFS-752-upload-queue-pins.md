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
- [x] (2026-10-02 09:23Z) Committed/pushed all task files, created regular
  PR #737 and verified SFS-752 is In Review with the PR attached.
- [x] (2026-10-02 09:39Z) Implemented the three attached review changes,
  validated the follow-up and prepared technical thread replies.
- [x] (2026-10-02 10:13Z) Verified the local review report, reproduced three
  pending-byte leaks and a missing accounting lock boundary, and fixed them.
- [x] (2026-10-02 10:22Z) Validated final accounting/clone changes, real
  upload-reader fallback and independent source review; prepared delivery.
- [ ] Commit/push the local review follow-up and update PR #737.

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

The local review also exposed writer pending-byte accounting that incremented
for every staging attempt while its map kept only one size per object. Three
red regressions showed leaked bytes for ignored, updated and closed submits.
The accounting-boundary test failed before serialization. Registering bytes
after enqueue without a lock allows an immediately completed upload to outrun
registration; removing a queue owner before taking the accounting lock can
instead consume a newly staged owner's contribution.

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

PR review confirmed that backend mutation stripe gates cover payload I/O and
can delay all first/final pin transitions behind the existing store-wide mutex.
Document the order `CachedStore::pinned` then ascending backend mutation gates
then backend `runtime_pinned`, including the prohibition on backend re-entry.
Retain this correctness mechanism; per-object serialization is a separate
performance change requiring measured contention and safe map-entry lifetime
and batch ordering. Public submit docs must specify provisional pin ownership.
Extend the existing queue critical section to retry/rebase rollback so the
record/update-dispatch-rollback invariant applies to all queue ingress paths.
Date/author: 2026-10-02, Codex (PR #737 review follow-up).

For local review findings 1 and 8, retain the existing writer-byte map mutex
through staged submission and accepted accounting. New/updated entries replace
their single contribution; ignored/closed submissions preserve prior bytes.
Completion and cancellation take that mutex before queue removal, reconcile
bytes and release it before backend access or unpinning. This protects both
registration and old-terminal/new-owner races. Total staged bytes still describe
local materialization, regardless of queue acceptance. A private facade helper
returns the outcome for accounting; public submit APIs retain their unit-returning
contracts, and their pin documentation is corrected (finding 3).
Date/author: 2026-10-02, Codex (local review follow-up).

Finding 2's missing-payload claim is rejected: the real upload reader first
tries the stage hardlink, then reads the pinned durable local backend if the
stage is absent. The shared VFS reader test now asserts that absence, exact
payload bytes, one backend fallback and zero stage reads. Keep background batch
submission best-effort for links; pre-commit writer staging still propagates
its linking errors under its separate durability contract.
Date/author: 2026-10-02, Codex (local review follow-up).

Do not fold per-operation batch metrics into one update (finding 4): each call
counts a submission attempt and samples the queue high-water mark; one final
depth can miss a peak while the worker drains. Global pin contention (finding 5)
remains the already documented measured-performance follow-up. Remove redundant
owned clones (finding 6) by borrowing submission input once and moving rollback
state. Do not reserve an entire batch for `insert_many` (finding 7) without
evidence: its result contains only first pins, and a duplicate batch has no
first pins; preallocation would allocate where the current empty vector does not.
The bounded accounting test overlaps submission with completion/cancellation,
and stale-epoch terminal regressions retain the pin ownership checks requested
by finding 8. No test establishes an unconditional or 100-percent race guarantee.
Date/author: 2026-10-02, Codex (local review follow-up).

## Outcomes & Retrospective

Implementation is complete. Eight SFS-752 regressions, 21 queue tests and 10
pin-filtered tests passed (filters overlap). Three focused VFS worker rebase
tests passed. Cache check/Clippy, formatting and whitespace checks passed.
Independent source review found no actionable correctness issue. All task files
were committed and pushed before creating regular PR #737; SFS-752 is In Review.
Full workspace suites and live mounted acceptance were not run. The fix reuses
the existing queue and pin locks and native backend batch pinning.

The PR review follow-up documents the auxiliary backend lock order and its
contention tradeoff, adds provisional pin ownership to both public submit docs,
and makes retry/rebase dispatch rollback atomic under the same queue mutex.
The new bounded retry regression failed before the change; final validation
passed 23 queue tests, 10 SFS-752 regressions and three VFS rebase tests.
Cache check/Clippy and format/whitespace checks passed. Independent source review
found no actionable defect. Per-object pin serialization was considered and
deferred as measured performance work, preserving the current correctness fix.

The local review follow-up fixes staged pending-byte leaks while preserving
public facade return types and removes redundant queue-state clones. Four new
cache regressions failed before their fixes; final checks passed 14 SFS-752
tests, 23 queue tests, one existing detached-accounting test, the real VFS
reader fallback test and three VFS rebase tests (filters overlap). Cache/VFS
check and all-target Clippy, formatting and whitespace checks passed.
Independent source review found no remaining actionable issue. Full workspace,
mounted and service-backed suites remain unrun. Delivery is the remaining step.

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

Initial delivery results: 8, 21, 10 and 3 passed respectively, with no ignored or failed
tests. Cache check/Clippy and format/whitespace checks exited successfully.
The initial six cache regressions and queue lock-boundary regression failed
before their fixes. If a shared Cargo target is locked, reuse an isolated target
directory under `target/`.

PR review follow-up results: the `sfs752_` filter passed 10 tests, the queue
filter passed 23, and the VFS filter passed three. Cache check/Clippy and
format/whitespace checks passed. The retry rollback lock-boundary regression
failed before atomic retry dispatch and passed after it.

Local review validation also ran these focused commands:

    cargo test -p studiofs-cache --lib detached_persist_with_upload_counts_pending_bytes_once -- --nocapture
    cargo test -p studiofs-vfs --lib upload_disk_reread_falls_back_without_stage_link_after_foreground_idle -- --nocapture
    cargo check -p studiofs-cache -p studiofs-vfs
    cargo clippy -p studiofs-cache -p studiofs-vfs --all-targets -- -D warnings

The tests above each ran one passing test. The final `sfs752_` filter passed
14 and the queue filter passed 23; the VFS rebase filter passed three.

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
PR: https://github.com/valandreev/studiofs-cli/pull/737
Implementation commit: `5e784917`.
Graph generation 2026-10-02T09:01:39Z reported metadata changes for relied-on
source files, so graph discovery was followed by direct source reads.

## Interfaces and Dependencies

`UploadQueue::submit` and `submit_with_payload` must expose new object ownership,
including rollback on dispatch failure. `CachedStore` consumes that outcome in
both single and batch submissions. Existing complete/cancel/retry/rebase APIs
and local-backend lifecycle methods retain their contracts.
The private `CachedStore::submit_upload_with_payload_outcome` returns
`UploadSubmitOutcome` for staged accounting. Public cached-store submission APIs
retain their unit-returning contracts.

Revision 2026-10-02: recorded the six red cache regressions and the two existing
lock-scope races that must be closed before compensating provisional pins.
Revision 2026-10-02: recorded the implemented ownership outcome, passing focused
checks and independent source review; delivery is the remaining step.
Revision 2026-10-02: recorded the actual regular PR and verified Linear review
status after delivery, without claiming full-suite or mounted acceptance.
Revision 2026-10-02: recorded the attached review scope and lock-order tradeoff;
retry/rebase serialization and focused follow-up validation are in progress.
Revision 2026-10-02: completed the review changes and recorded focused passing
validation; prepared one follow-up commit and replies for all attached threads.
Revision 2026-10-02: verified every local review finding, fixed writer-byte
accounting and its queue lifecycle races, preserved public facade APIs, and
recorded focused validation plus evidence-based dispositions before delivery.

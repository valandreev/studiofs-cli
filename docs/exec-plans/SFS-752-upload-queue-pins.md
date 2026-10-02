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
- [x] (2026-10-02 10:23Z) Committed/pushed the local review fix as `fcbe9ba7`,
  updated PR #737 and verified its matching remote head.
- [x] (2026-10-02 10:43Z) Verified the second local review, reproduced three
  pin/accounting/stats regressions, narrowed accounting and added batch overlap coverage.
- [x] (2026-10-02 10:49Z) Focused cache/VFS tests, checks, Clippy, formatting
  and final source diff review passed for the second local review follow-up.
- [x] (2026-10-02 10:50Z) Committed/pushed the second follow-up as `17ec7d70`,
  updated PR #737 and verified its matching remote head and clean working tree.
- [x] (2026-10-02 11:03Z) Delivered the third review's zero-byte repair as
  `a839f1d2` and its delivery record as `e9be25f6`, with matching PR head verified.
- [x] (2026-10-02 11:24Z) Verified seven new PR threads, reproduced stale
  rebased accounting, implemented atomic contribution replacement and strengthened
  fixture/pin assertions and documentation.
- [x] (2026-10-02 11:28Z) Validated the seven-thread follow-up with focused
  cache/VFS tests, checks, Clippy and independent source review.
- [ ] Commit/push the follow-up, reply inline and resolve all seven threads.

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

Seven-thread PR follow-up: rebased queue replacement now holds the accounting
mutex before the queue lock and refreshes an existing staged contribution only
after dispatch succeeds. It preserves absent legacy accounting, known zero-byte
entries, failed/stale rebases and queue pin ownership. Completion still removes
the actual contribution and retains the existing legacy stats fallback.
Contribution replacement uses a single atomic RMW for both restaging and rebase,
so lock-free snapshots see the pre- or post-replacement total, never the old
subtract/add intermediate total. This does not claim a transactional snapshot
across separate counters or separate concurrent operations.
Closed staging tests now check logical pin release for both direct and detached
paths; pin-before-stats tests assert initial backend eviction eligibility.
Queue depth documentation states global scope and sampling after rollback.
The documented check/Clippy gate includes VFS. Progress and artifacts now record
the previously delivered zero-byte review commits.

Third local review (head `7c886ad6`) dispositions:

- Finding 1 confirmed: a present zero-byte accounting entry was mistaken for
  an absent entry. `record_upload_terminal` now returns `Option<u64>`, and
  completion releases accounting before resolving any backend fallback.
  A focused zero-byte staging regression failed with one stats call before
  the fix and passes with none after it; legacy unknown-length fallback remains.
- Findings 2 and 6 fixed: reuse the staging length conversion and document the
  one-shot lifecycle of both test hooks.
- Finding 3 deferred: replacing the existing public six-tuple batch API requires
  migrating callers and is a separate API refactor. No transposed caller was
  identified by the report; preserve this review repair's public contract.
- Finding 4 retained: the owned candidate is used after insertion for exact
  closed-dispatch rollback identity and test-hook observation. Moving it into
  the map requires changing rollback plumbing or cloning elsewhere. Its current
  fixed-field clone has no heap allocation; no measured bottleneck warrants
  redesigning that verified boundary in this repair. Do not broadly derive Copy.
- Finding 5 deferred as the report itself requests a future performance pass:
  a batch-wide queue critical section changes contention and worker interleaving
  and needs measurements. Current per-attempt depth peaks remain preserved.

Third review validation (2026-10-02 11:01Z): 20 SFS-752 regressions and 24 queue
tests passed, as did cache check, cache all-targets Clippy, workspace formatting
and whitespace checks. Only private cache accounting and test-hook docs changed;
no VFS rerun was needed. Full suites and live mounts were not run. Graph coverage
reported metadata changes for both source files, so findings were checked against
direct source. Delivered as `a839f1d2`, pushed to PR #737 with its description
updated; verified the matching remote head and clean working tree.

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

The second report's findings 3.1/5.1 are addressed by an internal queue helper
returning `(UploadSubmitOutcome, usize)` with the final count sampled under the
same mutex as dispatch/rollback. Public signatures remain unchanged. Shared
cached-store submission records every attempt and peak from that sample, with
no second `pending_count()` lock and no final-batch-depth shortcut.
For findings 3.2/5.2, acquire the staged provisional pin before accounting and
compensate after releasing accounting; retain the accounting/queue ownership
critical section. A shared private helper submits an already-pinned candidate.
For finding 3.3, the length-inferred facade pins before observing payload stats;
a regression removes an eviction-eligible payload at that observation boundary
and failed before protection moved earlier. Backend pin ordering stays global;
per-object serialization remains separate measured performance work.
Date/author: 2026-10-02, Codex (second local review follow-up).

For findings 4.1 and 4.3, move retry snapshots with `mem::replace` and compare
the full failed operation before restoration. All rollback callers still retain
the queue mutex while storing and restoring that exact operation. For findings
4.2/5.3, prefer the known queued payload length when writer accounting has no
contribution; retain backend stats for legacy zero-length entries. The known
length regression failed on an unnecessary stats call before the change.
For finding 4.4, sort/deduplicate the temporary hardlink IDs while preserving
the input operation sequence, each pin reservation and every submit attempt.
Keep the public slice-based staging API instead of adding an iterator facade.
The requested batch/concurrent-completion test coordinates backend batch pinning
and old-owner queue removal, then checks queue pins, writer bytes, attempted
submissions, the observed peak and final eviction eligibility.
Date/author: 2026-10-02, Codex (second local review follow-up).

## Outcomes & Retrospective

The seven-thread follow-up keeps staged accounting aligned with accepted rebased
geometry and replaces contributions with one atomic RMW. Its rebase regression
failed before the fix (7 staged bytes versus 3 accepted bytes) and now covers
smaller, zero and larger lengths, stale/closed candidates and terminal metrics.
The existing restaging regression also preserves another object's contribution.
Closed-staging logical pin assertions and the eviction fixture precondition are
explicit. Queue sampling docs, VFS validation commands and the third delivery
trail are corrected. Final checks passed 21 SFS-752 regressions, 24 queue tests,
three VFS rebase tests and one VFS fallback test; cache/VFS check and all-targets
Clippy, formatting and whitespace checks passed. Independent source review found
no actionable defect. Filters overlap; full suites and live mounts were not run.

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
mounted and service-backed suites remain unrun. The validated follow-up was
committed as `fcbe9ba7`, pushed, and included in the updated PR #737 description.

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

Second local review results: `sfs752_` passed 19 tests, `upload_queue::tests`
passed 24, detached writer persistence passed one, VFS rebase passed three,
and upload-reader fallback passed one. Filters overlap. Cache/VFS check and
all-targets Clippy, workspace format check and `git diff --check` passed.
Three new regressions failed before pin-before-stats protection, narrower
accounting scope and known-length completion accounting were implemented.
The bounded batch/terminal overlap and queue-depth rollback tests also passed.
Full workspace suites, live mounts, remote services and benchmarks were not run.

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
Local review accounting fix: `fcbe9ba7`.
Second local review lock/depth/stats fix: `17ec7d70`.
Third local review zero-byte fix: `a839f1d2`; delivery record: `e9be25f6`.
Graph generation 2026-10-02T09:01:39Z reported metadata changes for relied-on
source files, so graph discovery was followed by direct source reads.

## Interfaces and Dependencies

`UploadQueue::submit` and `submit_with_payload` must expose new object ownership,
including rollback on dispatch failure. `CachedStore` consumes that outcome in
both single and batch submissions. Existing complete/cancel/retry/rebase APIs
and local-backend lifecycle methods retain their contracts.
The private `CachedStore::submit_pinned_upload_with_payload` submits a protected
candidate and returns `UploadSubmitOutcome`; its caller compensates non-new pins.
The crate-private queue helper also returns the sampled pending count. Public
queue and cached-store submission signatures retain their contracts.

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
Revision 2026-10-02: recorded the delivered local review fix and verified PR
update without claiming full-suite, mounted or service-backed acceptance.
Revision 2026-10-02: recorded the second review's lock narrowing, atomic depth
sample, pin-before-stats regression, completion-length fallback and targeted
batch overlap test; structural MCP was unavailable, so source checks were used.
Revision 2026-10-02: recorded second-review passing validation and verified
delivery to PR #737; full-suite and mounted acceptance remain user-run gates.

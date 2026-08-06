# Bound and coalesce runtime ingress queues

This ExecPlan is a living document and must remain aligned with
`.agent/PLANS.md`. It records the implementation and validation for SFS-640.

## Purpose / Big Picture

A burst of filesystem activity or a remote outage must not turn runtime
ingress into unbounded process memory. After this change, durable upload work
remains represented by the authoritative pending-upload state, prefetch hints
are bounded and safely droppable, and kernel invalidation requests are bounded
and coalesced on every supported bridge. Operators can observe current ingress
depth and coalescing/overflow counters through the existing runtime metrics,
and focused burst tests prove the limits without requiring S3 or a mounted
filesystem.

## Progress

- [x] (2026-08-06 Europe/Kyiv) Confirm SFS-640 is already `In Progress`, verify SFS-639 is `Done`, and remove the stale Linear blocker relation.
- [x] (2026-08-06 Europe/Kyiv) Index the checkout and audit UploadQueue, prefetch, WinFSP, macFUSE, and Linux/FUSE ingress implementations.
- [x] (2026-08-06 Europe/Kyiv) Add RED tests for bounded upload wakeups, bounded prefetch pending state, and bounded WinFSP invalidation backpressure. The cache test fails on missing `ingress_snapshot`/`claim_pending`; the VFS test fails on missing prefetch capacity/metrics; the WinFSP test is Windows-gated and awaits the queue implementation.
- [x] Replace the upload operation channel with bounded coalesced wakeups backed by authoritative pending state.
- [x] Bound prefetch ingress and pending blocks, preserve authoritative hint behavior, and expose dropped/coalesced metrics.
- [x] Replace the WinFSP `Vec<Ino>` with a bounded unique queue and report its depth/peak/coalescing metrics.
- [x] (2026-08-06 Europe/Kyiv) Address review findings: close WinFSP ingress before host shutdown, make producer-wait tests deterministic, publish queue depth at mutation time, exercise upload bursts through `UploadLifecycleWorker`, and add macFUSE invalidation metrics.
- [x] Run warning checks, formatting, and diff hygiene; record native-platform gaps honestly.
- [x] (2026-08-06 Europe/Kyiv) Validate the WinFSP crate on native Windows: queue and notification tests pass, `cargo check` and Clippy pass with `-D warnings`, and test-only queue helpers no longer enter the release build.
- [x] Update this plan and the SFS-640 Linear issue with the implementation and validation evidence.

## Surprises & Discoveries

- macFUSE already uses a finite unique queue and blocks producers when its
  8192-inode budget is full, but it did not publish queue metrics. Linux/FUSE
  already uses a bounded coalescing queue with producer backpressure and queue
  metrics. The bridge-local implementations can be preserved without a new
  shared abstraction while macFUSE adds the missing diagnostics.
- UploadQueue's pending maps already hold the durable upload intent in memory,
  while the unbounded channel only carries worker dispatch events. This makes
  a bounded wakeup channel safe only if the worker claims the current pending
  entries instead of treating each channel value as the authoritative upload.
- The existing prefetch `ReadState::prefetch_last_hint` suppresses covered
  ranges, but it is not a durable queue. A dropped hint must remain optional;
  the foreground read path remains authoritative and the worker must re-read
  current metadata before building batches.
- RED evidence: `cargo test -p studiofs-cache wakeup_burst_coalesces_without_losing_authoritative_pending_state --lib -- --nocapture` fails because the bounded upload ingress API is absent; `cargo test -p studiofs-vfs prefetch_pending_blocks_are_bounded_and_overflow_is_observable --lib -- --nocapture` fails because the bounded prefetch constants, receiver, and metric are absent.
- Green evidence: `cargo test -p studiofs-cache --lib` passed 166 tests and `cargo test -p studiofs-vfs --lib` passed 860 tests on the current Darwin host. `cargo test -p studiofs-winfsp --lib` is necessarily 0 tests because the crate is Windows-gated. Cross-target checking reached the Windows target but is blocked by the missing `x86_64-w64-mingw32-gcc`/Windows C toolchain while compiling `ring`.

## Decision Log

- Decision: Keep macFUSE and Linux/FUSE queue implementations local and make
  only the missing WinFSP queue bounded. Rationale: both existing bridges
  already have explicit finite capacity and backpressure semantics; a common
  queue abstraction would add cross-platform coupling without improving the
  contract. Date/Author: 2026-08-06 / Codex.
- Decision: Use a one-slot upload wakeup channel plus a per-object dispatch
  claim set. Rationale: one wakeup is enough to make the worker inspect the
  authoritative pending map, while the claim set prevents repeated snapshots
  from dispatching the same unchanged upload concurrently. A full wakeup
  channel is counted as coalesced, never as lost durable work. Date/Author:
  2026-08-06 / Codex.
- Decision: Use non-blocking bounded prefetch admission and count dropped
  hints/blocks. Rationale: prefetch is explicitly a hint and dropping it does
  not discard local writes or metadata transitions; the worker still validates
  current inode/read-ahead state before scheduling. Date/Author: 2026-08-06 /
  Codex.
- Decision: Apply WinFSP producer backpressure at a finite unique-inode
  capacity instead of dropping invalidations. Rationale: it preserves cache
  invalidation correctness while bounding memory, and matches the existing
  macFUSE/Linux bridge behavior. Date/Author: 2026-08-06 / Codex.

## Outcomes & Retrospective

Upload ingress is bounded to one buffered wakeup and coalesces additional
submissions while the authoritative pending map remains complete. Prefetch
ingress is bounded to 256 work items and the worker retains at most 8192
distinct pending blocks; optional overflow and duplicate suppression are
reported. WinFSP retains at most 8192 distinct inode invalidations, coalesces
duplicates, blocks producers at capacity, and exports queue depth/peak/
coalescing through the existing invalidation diagnostics snapshot. Closing the
queue wakes blocked producers before the WinFSP host and timer stop. macFUSE
publishes the same depth/peak/coalescing snapshot from its existing bounded
queue.

The focused upload-worker and macFUSE queue tests pass. Earlier full cache and
VFS suites passed 166/166 and 860/860 before the review coverage was added.
The host warning, formatting, and diff checks pass after the review fixes.
Native WinFSP tests are unavailable on Darwin (the package reports zero tests
under its Windows cfg); the Windows cross-target check is blocked before Rust
source checking by the missing MinGW C compiler needed by `ring`.

Native Windows follow-up completed the WinFSP queue validation: the full crate
test run passed 108/108 before the warning-only cleanup, and the focused queue
and notification tests passed after it. `cargo check -p studiofs-winfsp` and
`cargo clippy -p studiofs-winfsp --lib --tests -- -D warnings` are clean. A
mounted WinFSP E2E run was not performed in this pass.

## Context and Orientation

`crates/studiofs-cache/src/upload_queue.rs` owns the in-memory pending-upload
maps and currently returns an unbounded Tokio receiver. The VFS upload worker
in `crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs` consumes one
`PendingUpload` value per event. The pending maps are the authoritative local
work state because an upload transition must never be dropped during an S3 or
PostgreSQL outage.

Prefetch hints enter through `StudioVfs::enqueue_prefetch_hint` in
`crates/studiofs-vfs/src/vfs/prefetch.rs`. The bootstrap channel is currently
unbounded and the worker adds a second `VecDeque<PrefetchBatch>` plus a
deduplication set. Prefetch is optional: a dropped hint may cause a later cold
read, but must not change durable filesystem state.

WinFSP metadata invalidation is implemented in
`crates/studiofs-winfsp/src/context/invalidation.rs`. The invalidator and the
WinFSP notification timer share an `Arc<Mutex<Vec<Ino>>>`; duplicates are
removed only after the vector has already grown. The macFUSE queue in
`crates/studiofs-macfuse/src/low_level.rs` and the Linux/FUSE queue in
`crates/studiofs-linux-fuse/src/invalidation.rs` are existing bounded
references for platform behavior.

Existing runtime metric surfaces are in
`crates/studiofs-cache/src/metrics.rs` and
`crates/studiofs-vfs/src/vfs/metrics.rs`. The VFS exposes upload, prefetch,
and Linux-FUSE invalidation snapshots through its write-path diagnostics.

## Plan of Work

First add focused failing tests in the existing queue modules. The upload test
will fill the one-slot wakeup channel with a burst, assert that pending upload
state still contains every distinct object, and assert that a claimed update
uses the newest content epoch. The prefetch test will exercise a full bounded
ingress and a full pending-block budget, then assert the dropped/coalesced
counters and that an admitted hint is planned against current state. The
WinFSP tests will fill the unique queue, prove a new producer waits until the
notification drain frees capacity, and assert the queue depth/peak/coalesced
snapshot.

Then change UploadQueue so submissions update the pending map first and send a
coalesced wakeup. Add dispatch claims under the same mutex, clear claims on
completion/cancellation/requeue, and make the upload worker claim current
entries after each wakeup. Preserve existing duplicate, stale, retry, idle,
shutdown, and pinning behavior.

Change prefetch bootstrap and startup channels to bounded Tokio channels.
Admission of optional hints uses `try_send`; a full channel increments the
dropped-hint counter. The worker limits the total distinct pending blocks and
records duplicate/coalesced and overflow counts. Queue-depth metrics include
the ingress channel and the worker's pending block set, while planning still
re-reads inode and read-ahead state before starting a batch.

Replace the WinFSP vector with a small queue type containing a bounded ordered
set, a condition variable, and atomic depth/peak/coalesced counters. Enqueue
coalesces an existing inode and waits when the distinct-inode capacity is
full. The timer drains the set before doing path resolution, then publishes
the existing VFS invalidation snapshot fields.

Finally run the smallest focused tests for each changed crate, then warning,
format, and diff checks. Run platform-gated tests where this Darwin host can
execute them and report Linux/FUSE mounted and Windows/WinFSP native cells as
unavailable when their runtimes are absent.

## Milestones

### M0 — RED characterization (complete)

The new tests fail because the upload and prefetch channels are unbounded and
WinFSP stores an unbounded vector. The failures identify missing bounded
admission/metrics behavior rather than test setup errors. The WinFSP regression
is gated by `cfg(windows)` and will be validated on a Windows runner.

### M1 — Shared upload and prefetch bounds (complete)

The upload worker consumes coalesced wakeups and claims authoritative pending
state without losing a durable transition. Prefetch ingress and pending
blocks have finite limits, and the focused cache/VFS tests pass.

### M2 — WinFSP bound and validation (complete; mounted E2E remains separate)

WinFSP invalidation has finite unique capacity and producer backpressure. Its
Windows-gated tests cover deterministic producer waiting, close wakeup, and
live queue metrics before drain. Native Windows validation passes the queue and
notification tests plus warning-clean check and Clippy. Darwin remains unable
to execute these tests natively, and mounted WinFSP E2E was not part of this
pass.

## Concrete Steps

Run commands from `/Volumes/Storage/dev/studiofs`, one Cargo test filter per
command to avoid workspace file-lock contention.

    cargo test -p studiofs-cache upload_queue --lib -- --nocapture
    cargo test -p studiofs-vfs upload_worker_drains_coalesced_wakeup_burst_without_losing_pending_ops --lib -- --nocapture
    cargo test -p studiofs-vfs prefetch_burst --lib -- --nocapture
    cargo test -p studiofs-macfuse session_invalidation_queue_ --lib -- --nocapture
    cargo test -p studiofs-winfsp should_notify --lib -- --nocapture
    cargo test -p studiofs-cache --lib
    cargo test -p studiofs-vfs --lib
    cargo test -p studiofs-winfsp --lib
    RUSTFLAGS='-D warnings' cargo check -p studiofs-cache -p studiofs-vfs -p studiofs-macfuse -p studiofs-winfsp
    cargo fmt --all -- --check
    git diff --check

Expected focused output is one or more `test ... ok` lines followed by
`test result: ok`; the bounded burst tests must fail before their production
changes and pass afterward. The warning check, formatter, and diff check must
finish without diagnostics.

## Validation and Acceptance

Upload acceptance is behavioral: a burst of distinct pending uploads leaves
all durable pending entries represented even when the worker wakeup is full;
the worker eventually claims each entry, and an updated object is dispatched
at its newest content epoch. No upload transition is counted as dropped.

Prefetch acceptance is behavioral: the ingress channel and worker pending
block set stay at their configured finite limits; duplicate hints increase a
coalesced counter, full optional ingress increases a dropped-hint counter, and
the worker plans admitted work against current authoritative inode/read-ahead
state.

WinFSP acceptance is behavioral: the distinct queue never exceeds its explicit
capacity, repeated inode invalidations coalesce, a producer waits rather than
allocating beyond the limit, and draining or closing wakes the producer. Queue
depth is published when the queue changes rather than only after drain; depth,
peak, and coalesced values are visible through the existing invalidation
diagnostics snapshot.

The existing macFUSE and Linux/FUSE bounded/backpressure tests remain part of
the cross-platform audit. macFUSE also publishes depth/peak/coalescing through
the shared runtime invalidation snapshot. macFUSE model tests may run on this
host; Linux/FUSE mounted validation requires `/dev/fuse`, and WinFSP native
validation requires a Windows runner. An unavailable native cell is reported
as unavailable, not as a passing product validation.

## Idempotence and Recovery

All tests use existing `target/`-backed temporary directories and can be
rerun. The implementation must preserve unrelated working-tree changes. If a
focused test fails after the change, return to the queue transition that
violates the contract; do not increase a timeout or remove the bound to hide
the failure. Generated artifacts stay under `target/`.

## Artifacts and Notes

Keep only concise test transcripts and final metric examples here. Do not
commit logs or generated databases. Update this plan at every stopping point,
including RED/GREEN evidence and unavailable native-platform cells.

## Interfaces and Dependencies

No new crate or cross-platform queue abstraction is required. The implementation
uses existing Tokio `mpsc`, `Notify`/condition-variable primitives, the
authoritative `PendingState`, `PendingUpload`, `PrefetchWorkItem`, and the
existing `VfsFuseInvalidationSnapshot` metric type. Public filesystem behavior
and local-first durability contracts remain unchanged.

## Revision Note

2026-08-06: Initial plan created after verifying SFS-639 is complete and
auditing the existing shared, macFUSE, Linux/FUSE, prefetch, upload, and WinFSP
ingress implementations.

2026-08-06: Revised after code review to record WinFSP close-before-host-stop,
mutation-time metrics, deterministic producer-wait tests, upload worker-path
coverage, macFUSE runtime metrics, and the still-unavailable native Windows
validation cell.

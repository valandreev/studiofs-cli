# Bring Recent macOS Cache and Durability Optimizations to Linux FUSE

This ExecPlan is a living document. The sections `Progress`, `Surprises &
Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to
date as work proceeds.

This document must be maintained in accordance with `.agent/PLANS.md`. A reader
should be able to resume the work from this file and the repository alone,
without access to the conversation that created it.

## Purpose / Big Picture

StudioFS already shares the recent namespace batching, metadata durability, and
volatile writeback work across macOS, Windows, and Linux. The Linux libfuse3
adapter still defaults to direct I/O, duplicates VFS prefetch when kernel page
cache is enabled, sends kernel invalidations synchronously, and makes every
`flush`, `fsync`, and `release` wait for every asynchronous write in the mount.

After this work, Linux mounts use an adaptive cache policy by default. Clean,
read-only files that fit in available memory use the Linux page cache, stable
content generations retain cached pages across opens, and unsafe or oversized
files use direct I/O. Kernel invalidations are bounded, coalesced, and delivered
only for live inode references. File barriers wait for relevant writes instead
of unrelated mount traffic. Operators can remount with
`STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct` as a deterministic rollback.

The behavior is visible through focused unit tests, mounted Linux tests, runtime
statistics, and repeated-read benchmarks. A warm second read of a stable file
must avoid new VFS read bytes, while an overwrite or remote invalidation must
always expose the new checksum.

## Progress

- [x] (2026-07-29 18:51Z) Recorded the implementation start and confirmed the
  clean `feature/mac2linux-port` branch at `191ca80e`.
- [x] (2026-07-29 18:59Z) Ran the focused baseline: 40 Linux FUSE unit tests
  passed, 3 mounted tests were ignored as expected, and the VFS flush, fsync,
  and release durability regressions passed.
- [x] (2026-07-29 19:10Z) Committed this living ExecPlan first as `1e7ca3bd`
  (`docs: add Linux FUSE parity ExecPlan`).
- [x] (2026-07-29 19:49Z) Added and independently reviewed the pure
  generation-safe OS-cache state machine; 9 focused tests passed. Committed as
  `3f8ab936` (`feat(vfs): add generation-safe OS cache state`).
- [x] (2026-07-29 20:04Z) Added and independently reviewed the pure Linux
  asynchronous-write ledger; 23 focused tests and the Linux crate check passed.
  Committed as `5165d37a` (`feat(linux): add scoped async write ledger`).
- [x] (2026-07-29 20:31Z) Added and independently reviewed Linux lookup/open
  liveness plus the bounded coalescing invalidation queue; 17 focused tests and
  the Linux crate check passed. Committed as `7c6f9da1`
  (`feat(linux): add inode liveness and invalidation queue`).
- [x] (2026-07-29 20:49Z) Added and independently reviewed the shared FIFO
  per-inode open-reply serializer; 7 focused tests and bridge clippy passed.
  Committed as `7920b3d1`
  (`feat(bridge): serialize FUSE open replies per inode`).
- [x] (2026-07-29 20:58Z) Wired the shared VFS cache seam, handle-scoped
  writer/read lifetimes, abort behavior, synchronous invalidation retirement,
  and final unlink/rename-overwrite retirement. Sixteen focused lifecycle tests,
  ten state tests, and the VFS crate check passed; independent spec and quality
  rereviews approved the slice. Committed as `47570dd6`
  (`feat(vfs): wire OS cache handle lifecycle`).
- [x] (2026-07-29 21:29Z) Added and independently reviewed the pure Linux
  cache-policy table, adaptive-by-default parser, one-second available-memory
  sampler, and bounded synchronous read buffer pool. Fifteen focused tests
  passed. Committed as `d93a2dad`
  (`feat(linux): add adaptive cache policy primitives`). The policy is not yet
  connected to low-level opens, so the runtime default remains unchanged.
- [x] (2026-07-29 21:48Z) Migrated macFUSE cache decisions and reply rollback
  onto the shared VFS seam and per-inode reply serializer while preserving the
  existing flags, memory threshold, metrics JSON, async reads, and symlink
  behavior. Static review passed and the slice was committed as `96f24a44`
  (`refactor(macfuse): use shared OS cache seam`). Native compilation and
  characterization remain a required macOS CI gate because this Linux host
  cannot compile the macOS ABI selection.
- [x] (2026-07-29 22:12Z) Closed the multithreaded
  accepted-reply-before-commit `forget` race with per-inode debt batches scoped
  to the exact provisional-reply cohort. Twenty-six focused invalidation tests
  and the Linux crate check passed; an adversarial rereview approved the
  corrected rollback/success arithmetic. Committed as `4fd85fc7`
  (`fix(linux): close lookup reply forget race`).
- [x] (2026-07-29 22:31Z) Added handle-scoped VFS/kernel readahead ownership
  with owner-generation tokens across eager hints, planned batches, launch,
  in-flight metadata boundaries, and speculative read windows. Deterministic
  race tests cover stale stream recreation, owner switching during an armed
  oversized window, and rejected stale work. Focused tests and VFS clippy with
  warnings denied passed; independent rereview approved the final slice.
  Committed as `60aa0597`
  (`feat(vfs): add handle-scoped readahead ownership`).
- [x] (2026-07-29 22:53Z) Added the pure dedicated invalidation worker core:
  one claimed consumer, liveness admission immediately before an injected
  notifier, attr/data geometry, close-and-drain, retryable timeout, panic-aware
  completion, and additive queue/latency/failure metrics. Thirty-two standalone
  tests passed and an independent teardown rereview approved the slice.
  Committed as `f448fb41`
  (`feat(linux): add dedicated invalidation worker`).
- [x] (2026-07-29 23:16Z) Integrated Linux cache decisions into open/create and
  synchronous reads: session-scoped policy/memory/pool state, per-inode reply
  transactions, shared VFS prepare/abort, rejected-reply handle retirement,
  exact direct/kernel/adaptive flag mapping, scoped eager-prefetch restoration,
  and runtime-stats bypass. Linux unit tests passed (117 passed, 3 mounted
  ignored), focused VFS owner tests and VFS clippy passed, and the final
  independent rereview approved the slice. Committed as `bd9482db`
  (`feat(linux): integrate adaptive open cache decisions`). The unset runtime
  default remains readiness-gated to Direct until raw invalidation integration.
- [x] (2026-07-29 23:31Z) Added untimed inode/handle ledger barriers for normal
  `flush`, `fsync`, and `release`, retaining timed mount-global drain only for
  shutdown/tests. Twenty-six ledger tests passed and an independent static
  review approved watermark, sticky-error, and shutdown semantics. Committed
  as `d5feb514` (`feat(linux): add untimed scoped write barriers`).
- [x] (2026-07-30 00:07Z) Wired Linux lookup/open liveness, the C/Rust
  `forget` callback, reply reservations, producer-only invalidation, the
  dedicated worker, and safe teardown into raw libfuse. Entry/create/open
  sequencing now protects both the pre-reply snapshot gap and
  accepted-reply-before-commit race. Timeout closes the notification gate,
  removes signal handlers, unmounts, and retains worker/state/raw without
  destroy. Linux unit tests passed (136 passed, 3 mounted ignored), isolated
  clippy passed, and independent spec/quality rereviews approved the final
  slice. Committed as `1002722b`
  (`feat(linux): wire safe inode invalidation`). Adaptive default remains
  readiness-gated pending mounted validation.
- [x] (2026-07-30) Hardened the sequence-exact asynchronous-write ledger with
  race-safe task cancellation and a bounded sticky-error journal. Admission
  now reserves error capacity for every accepted request, so sustained
  ENOSPC/EIO cannot grow memory without bound or lose watermark precision.
  Twenty-nine focused ledger tests passed; independent spec review passed and
  the review's unbounded-journal finding was fixed.
- [x] (2026-07-30) Integrated the ledger with raw Linux write callbacks:
  opportunistic writes use RAII reply tickets, inode/fh watermarks replace the
  mount-global barrier, native reply rejection is observed, and two-stage
  shutdown cancellation retains the raw session on timeout. Added negotiated
  4 MiB read geometry, accepted-reply-only open-cache counters, live
  invalidation snapshots, and live async-write depth/wait/failure metrics.
  With raw invalidation and liveness now wired, the runtime readiness gate was
  flipped so unset/empty policy selects Adaptive; Direct remains the explicit
  remount rollback.
  Linux lib tests passed (149 passed, 3 mounted ignored) and all-target clippy
  passed with warnings denied.
- [x] Add a shared generation-safe OS-cache seam to `studiofs-vfs`, driven by
  failing tests before production changes.
- [x] Move current macFUSE cache decisions onto the shared seam without changing
  macOS flags or the existing `macfuse_open_cache` runtime JSON.
- [x] Add handle-scoped readahead ownership and prove that kernel-cached handles
  do not launch duplicate VFS prefetch.
- [x] Add the Linux adaptive cache policy, memory snapshot, negotiated read
  geometry, and bounded synchronous read buffer pool.
- [x] Add Linux lookup/open liveness, the `forget` callback, a bounded coalescing
  invalidation worker, and safe teardown.
- [x] Replace mount-global asynchronous-write waiting with inode and handle
  watermarks while preserving the current local-first durability contract.
- [x] Add runtime statistics, documentation, mounted/E2E regression
  definitions, and five-run benchmark gates. The mounted executions remain an
  external merge gate because this host has no `/dev/fuse`.
- [ ] Complete focused verification, review the diff for specification and code
  quality, run the justified broader checks, and commit all related changes.

## Surprises & Discoveries

- Observation: GitHub records the old Linux optimization PR #601 as merged, but
  commit `7f85ec8c` is not an ancestor of current `main`.
  Evidence: its merge base with current `main` is the older `95bfdd8f`.
  Therefore the branch is a source of algorithms and tests only, not a safe
  cherry-pick target.

- Observation: PRs #604 through #611 already changed shared VFS and metadata
  code, and the current Linux adapter already routes create, flush, fsync, and
  release through the new entry points.
  Evidence: `SessionState::create` uses deferred metadata admission,
  opportunistic `SessionState::flush` calls
  `flush_volatile_writeback_with_outcome`, `SessionState::fsync` uses the strict
  handle barrier, and opportunistic `SessionState::release` calls
  `release_fh_with_deferred_last_handle_barrier`.

- Observation: the first attempted VFS baseline filter,
  `flush_volatile_writeback`, selected zero tests.
  Evidence: Cargo reported `0 passed; 792 filtered out`. The baseline was
  corrected to named contract tests and those tests passed.

- Observation: Linux invalidation safety and asynchronous write shutdown share
  the same raw-session lifetime boundary.
  Evidence: both kernel notifications and late write replies can retain
  libfuse request/session pointers after the main loop returns. A timeout path
  that retains only the raw session while dropping `SessionState` would leave
  libfuse userdata dangling.

- Observation: a write callback must receive its ledger sequence before it can
  wait for byte/request capacity.
  Evidence: registering only after capacity admission lets a later flush or
  release capture a watermark that omits the already-arrived callback. The
  ledger now distinguishes registered synchronous callbacks from the bounded
  set of admitted asynchronous executions.

- Observation: capacity admission must respect request sequence order.
  Evidence: `notify_all` without FIFO eligibility allowed a post-watermark
  waiter to repeatedly take a freed slot before the pre-watermark waiter,
  indirectly extending or starving the earlier barrier. The ledger now admits
  only the lowest registered, unreserved sequence.

- Observation: treating an in-progress lookup reply as immediately kernel-live
  closes one invalidation race but opens its inverse.
  Evidence: a notification sent before `fuse_reply_entry` can be ignored, after
  which the reply installs stale data. Pending lookup reservations now record a
  deferred marker; successful commit requests a follow-up data invalidation,
  while rollback discards the marker.

- Observation: committing lookup liveness only after `fuse_reply_entry` returns
  still races with a multithreaded kernel `forget` callback.
  Evidence: the kernel can accept the reply and dispatch `forget` before the
  replying thread increments the committed count, causing the later commit to
  leak one lookup reference forever. Each reservation now has a provisional
  reply-in-flight phase that `forget(nlookup)` can consume before the reply
  thread finalizes it.

- Observation: the implementation host has libfuse3 3.18 and `fusermount3`,
  but no `/dev/fuse`.
  Evidence: preflight reports `fuse-device-missing`; all three mounted tests
  remain ignored. Mounted correctness, warm-cache byte deltas, and the
  five-run Ubuntu/Rocky performance gates therefore remain external merge
  gates and are not claimed by this checkout.

## Decision Log

- Decision: Make `Adaptive` the Linux default in the final implementation and
  keep `Direct` and `Kernel` as explicit remount-time overrides.
  Rationale: the requested rollout is immediate adaptive behavior, while direct
  I/O remains a deterministic operational fallback.
  Date/Author: 2026-07-29 / Codex and user.

- Decision: Do not reimplement shared namespace and writeback batching.
  Rationale: those paths are already active for Linux. Reworking them would add
  risk without closing a Linux-specific gap.
  Date/Author: 2026-07-29 / Codex.

- Decision: Reuse only focused concepts from non-ancestor commit `7f85ec8c`.
  Rationale: its large profile and memory-pressure system predates the current
  VFS contracts. Whole-branch integration would overwrite newer shared work.
  Date/Author: 2026-07-29 / Codex.

- Decision: Keep Linux reads synchronous and port only bounded buffer reuse from
  macOS.
  Rationale: libfuse already runs a bounded multithreaded callback loop, and the
  adaptive page cache removes warm read callbacks. macFUSE asynchronous reply
  ownership and cancellation solve a different platform problem.
  Date/Author: 2026-07-29 / Codex.

- Decision: Use inode-scoped watermarks for `flush` and `fsync`, handle-scoped
  watermarks for `release`, and a mount-global drain only during shutdown.
  Rationale: file durability must include earlier writes to the same inode, but
  unrelated files must not increase barrier latency.
  Date/Author: 2026-07-29 / Codex.

- Decision: Do not cancel an in-progress VFS write future unless a focused
  audit first proves that the current local-first write path is cancellation
  safe.
  Rationale: aborting a future after local mutation starts could leave an
  accepted write without its required completion bookkeeping. On shutdown,
  stop admission, wait through the bounded drain windows, and retain the raw
  session together with its boxed callback state if callbacks remain live.
  Date/Author: 2026-07-29 / Codex.

- Decision: Record Linux lookup liveness at the successful FUSE reply boundary
  using rollback-capable reservations, and let kernel `forget` retire lookup
  references.
  Rationale: recording after `fuse_reply_entry` creates a visibility race,
  while eagerly deleting liveness on namespace mutation conflicts with the
  kernel's lookup-reference protocol.
  Date/Author: 2026-07-29 / Codex.

- Decision: Mark each lookup reservation provisional immediately before the
  native entry/create reply and let `forget` consume provisional reservations
  after committed references.
  Rationale: this preserves the rule that rejected replies never become live
  while closing the accepted-reply-before-Rust-commit race, including multiple
  simultaneous replies for the same inode.
  Date/Author: 2026-07-29 / Codex.

- Decision: Keep the asynchronous error journal sequence-exact and consume all
  matching errors through a completed barrier, returning the earliest errno.
  Rationale: lossy coalescing can erase a post-watermark error when an earlier
  barrier consumes the same aggregate. Admission reserves one of 8192 bounded
  journal slots for every accepted request and fails closed with EIO when the
  journal is full, preserving exact watermarks without unbounded growth.
  Date/Author: 2026-07-29 / Codex.

## Outcomes & Retrospective

Linux now defaults to the generation-safe Adaptive cache policy, delegates
readahead to the kernel only for page-cached handles, coalesces invalidations
for live inodes, and scopes async-write durability barriers to the relevant
inode or handle. Direct remains an explicit remount rollback. The shared macOS
cache seam and legacy `macfuse_open_cache` JSON remain intact.

Focused Linux tests, VFS observability tests, documentation-name consistency,
perf dry-run validation, and warnings-denied clippy pass. The validation
infrastructure now runs five current-main/direct, feature/direct,
feature/adaptive, local, and JuiceFS samples and emits median/p95/p99 threshold
decisions under `target/perf`.

This checkout is not yet merge-certified: `/dev/fuse` is absent, so mounted
checks, warm-pass VFS-byte proof, Ubuntu 24.04/Rocky 9.4 performance data, and
signal/unmount stress could not execute. Native macFUSE characterization also
requires macOS CI. Those are release evidence gaps, not silently waived gates.

## Context and Orientation

StudioFS is a Rust 2024 workspace. `studiofs-vfs` owns open file handles, local
dirty overlays, resolved file-slice metadata, prefetch, and local durability.
The Linux bridge in `crates/studiofs-linux-fuse` exposes that VFS through the
libfuse3 low-level C API. The macOS bridge in `crates/studiofs-macfuse` already
has generation-aware page-cache decisions and is the current behavioral
reference.

A content generation is the exact local file content represented by an inode
attribute and its ordered resolved slice graph. The identity includes the
content epoch, logical size, chunk and range, overlay/base source, slice identity
and order, and payload reference. Remote publication alone does not change file
content and therefore must not change the generation.

`direct_io` tells FUSE to bypass the operating-system page cache for a handle.
`keep_cache` tells FUSE that cached pages from an earlier open are safe to keep.
Returning `keep_cache=false` while leaving `direct_io=false` discards old pages
and then permits the kernel to populate a new generation. VFS prefetch is
valuable for direct I/O but duplicates kernel readahead when the page cache owns
the read stream.

The current Linux adapter keeps `LinuxFuseAsyncWrites` inside
`crates/studiofs-linux-fuse/src/low_level.rs`. Its `wait_idle` method waits for
all writes in the mount, so an unrelated file can delay `flush`, `fsync`, or
`release`. The replacement ledger must maintain global byte/request admission
limits but scope durability waiting by inode or handle.

The current Linux invalidator invokes `fuse_lowlevel_notify_inval_inode`
directly. It has a shutdown gate for active calls but does not know whether the
kernel still holds a lookup or open reference, does not coalesce repeated
notifications, and does not move kernel IPC off VFS worker threads.

## Plan of Work

### Milestone 1: Shared OS-cache decisions

First add failing unit tests in `studiofs-vfs` for the desired public API:
`OsCacheParticipation`, `OsCacheDisposition`, `OsCacheReason`,
`StudioVfs::prepare_os_cache_open`, `StudioVfs::abort_os_cache_open`, and
`ReadAheadOwner`. The first observation of a clean generation replaces cache,
an identical second observation reuses it, and changes to every generation field
replace it. A publication-only transition reuses it. Active writers, dirty
state, unknown handles, and metadata lookup errors replace cache.

Implement an 8192-entry LRU generation store in a dedicated VFS module. Tie
writer and foreground-read lifecycle state to the existing `OpenFile` so release
removes it on success, error, and deferred-last-handle paths. Clear a generation
synchronously whenever VFS schedules data invalidation or retires a final inode.
Open preparation must be idempotent per handle. If a platform reply fails,
`abort_os_cache_open` clears the observation so a later open cannot reuse a
generation that the kernel never accepted.

Add handle-scoped `ReadAheadOwner`. Kernel ownership disables VFS prefetch and
clears pending per-handle read-window state; VFS ownership preserves the current
policy. Reads cannot begin before the platform accepts the open reply, so the
adapter can choose the owner after receiving the OS-cache decision.

Before removing macFUSE-local generation and writer state, add characterization
tests around current open flags, clean writer release, active writer behavior,
invalidation, and runtime JSON. Move macFUSE to the VFS API and require the same
tests to remain green. Do not alter WinFSP cache behavior.

Milestone acceptance is a passing focused VFS suite and unchanged macFUSE
characterization tests on macOS.

### Milestone 2: Linux adaptive open and read path

Add `Adaptive` to the Linux cache policy. Unset, empty, `adaptive`, and `auto`
select it. Preserve existing direct and kernel aliases. Unknown values log a
warning and select adaptive.

The exact adaptive mapping is:

* write-capable, dirty, active-writer, unknown, non-regular, or known
  file-size-greater-than-available-memory opens use direct I/O, no keep-cache,
  and VFS-owned readahead;
* clean read-only first or changed generations use kernel page cache,
  `keep_cache=false`, and kernel-owned readahead;
* clean read-only stable generations use kernel page cache,
  `keep_cache=true`, and kernel-owned readahead.

Direct mode always bypasses page cache. Forced kernel mode always enables page
cache and requests writeback-cache capability, but still uses generation-safe
`keep_cache`. Adaptive mode never requests writeback cache because its
write-capable handles remain direct.

Read available memory with `sysinfo` and cache the snapshot for one second. A
missing memory sample does not force direct I/O, but the generation must still
be replaced until otherwise proven safe.

Request `max_read` and `max_readahead` up to the existing four-mebibyte callback
limit through mount arguments and connection negotiation. Record the actual
negotiated values. Add a synchronous read buffer pool with at most 32 retained
vectors and an eight-mebibyte retained-capacity limit.

Serialize cache preparation, file-info initialization, FUSE reply acceptance,
and failure cleanup by inode. The serialization must not be global.

Milestone acceptance is a Linux unit suite that proves every policy row, memory
TTL behavior, reply rejection cleanup, read ownership, geometry, and buffer
retention rule.

### Milestone 3: Linux liveness and invalidation delivery

Add a `forget` function to the C low-level operations table and its Rust
callback. Successful entry and create replies reserve and then accept lookup
references; rejected replies roll reservations back. `forget(nlookup)` removes
lookup references. Active file handles independently keep an inode live, and the
root inode is always live.

Create a Linux invalidation module with a bounded queue for 8192 distinct inodes.
Repeated notifications for an inode coalesce, and data invalidation dominates
attribute-only invalidation. Producers block only when the queue contains 8192
different inodes. A dedicated operating-system thread performs the libfuse
notification after checking liveness, without retaining a liveness-map lock
during kernel IPC.

Shutdown removes the VFS invalidator, closes producers, drains queued work, and
then unmounts. Preserve the existing two-second use-after-free safety rule: if
kernel IPC is still active, log the timeout and do not destroy the raw session.

Milestone acceptance includes unit tests for reply reservations, forget/open
liveness, queue coalescing, data precedence, capacity backpressure, drain, and
timeout behavior, followed by the existing mounted unmount test when `/dev/fuse`
is available.

### Milestone 4: Scoped asynchronous-write barriers

Move asynchronous write tracking into its own module. Every admitted request
receives a monotonic handle sequence and an inode association. The tracker keeps
global byte/request admission, pending sequences, handle-closing state, and the
earliest unreported error.

`flush` and `fsync` capture all handle watermarks for the target inode and wait
only through those sequences. `release` closes one handle to new admission and
waits through that handle's watermark. Later writes do not extend a captured
barrier. Dropped task guards complete with `EIO`. The first VFS error or
cancellation before the watermark is returned by the relevant barrier and then
consumed.

After the asynchronous barrier, retain the current VFS calls:
opportunistic `flush` uses `flush_volatile_writeback_with_outcome`; strict
`fsync` uses `fsync_fh`; opportunistic last release uses
`release_fh_with_deferred_last_handle_barrier`; write-through handles stay
synchronous.

At shutdown, wait globally for two seconds, cancel remaining tasks, and wait for
another two seconds before allowing raw request ownership to end.

Milestone acceptance proves that a barrier waits for all pre-watermark writes of
its inode, does not wait for another inode or later writes, rejects admission
after handle close, and propagates VFS errors and dropped-task `EIO`.

### Milestone 5: Observability, documentation, and mounted proof

Add an additive `linux_fuse_open_cache` runtime section with direct/kernel
counts, replace/reuse reasons, and memory bypasses. Populate and extend the
existing asynchronous-write snapshot with pending handles/inodes, depths,
waits, errors, and cancellations. Add invalidation depth/peak, coalescing,
skipped-unobserved, latency/failure, and shutdown-timeout fields. Preserve the
existing macFUSE JSON keys.

Correct `docs/platform-bridges.md`, `docs/configuration.md`,
`docs/local-first-validation-matrix.md`, and `docs/tests.md`: Linux mounting is
implemented, create is deferred, flush and release are volatile/deferred,
adaptive is the default, and direct is the rollback.

Extend the existing Linux benchmark scripts rather than adding a competing
harness. Store all generated artifacts below `target/`.

Milestone acceptance is successful mounted correctness on Ubuntu 24.04 and
Rocky 9.4, with Rocky 8.6 best effort, plus five-run performance evidence.

## Concrete Steps

Run every command from `/home/val/dev/studiofs-cli`.

The recorded baseline commands are:

    cargo test -p studiofs-linux-fuse --lib
    cargo test -p studiofs-vfs --lib deferred_namespace_written_file_reads_back_after_flush -- --nocapture
    cargo test -p studiofs-vfs --lib sfs312_fsync_persists_local_state_without_remote_visibility -- --nocapture
    cargo test -p studiofs-vfs --lib transient_deferred_namespace_backlog_does_not_fail_release_barrier -- --nocapture

The observed baseline is:

    studiofs-linux-fuse: 40 passed, 0 failed, 3 ignored
    each named studiofs-vfs contract test: 1 passed, 0 failed

For each milestone, add a focused test first, run it and record the expected
failure, implement only enough production code to pass, then rerun the focused
crate suite. Typical commands are:

    cargo test -p studiofs-vfs --lib os_cache -- --nocapture
    cargo test -p studiofs-linux-fuse --lib adaptive -- --nocapture
    cargo test -p studiofs-linux-fuse --lib invalidation -- --nocapture
    cargo test -p studiofs-linux-fuse --lib async_write -- --nocapture

After each green slice run:

    cargo fmt --all -- --check
    cargo check -p studiofs-vfs -p studiofs-linux-fuse

At the end run:

    cargo test -p studiofs-vfs --lib
    cargo test -p studiofs-linux-fuse --lib
    cargo clippy -p studiofs-vfs -p studiofs-linux-fuse --all-targets --locked -- -D warnings

When `/dev/fuse`, `fusermount3`, PostgreSQL, and the configured object store are
available, run the documented mounted tests serially. Run release-mode
benchmarks five times per profile for current-main/direct, feature/direct, and
feature/adaptive. Compare medians first, then p95 and p99.

## Validation and Acceptance

Correctness requires zero stale checksums, hangs, leaked FUSE callbacks, or
durability contract failures. A stable warm second read must add no VFS read
bytes. Overwrite, truncate, local dirty state, active writers, and remote
invalidation must never reuse stale kernel pages. Files larger than a known
available-memory sample must use direct I/O.

The feature/direct profile must remain within five percent of current-main
median and ten percent of p95/p99. Adaptive must remain within five percent of
direct on every mandatory workload and improve at least one warm or reopen-read
workload by five percent or more.

Small-file workloads use 4, 16, and 64 KiB files, 10,000 files, and eight
threads. They cover create/write/close, reopen/read/stat, and cleanup. Read
workloads cover cold and warm sequential reads, random reads, repeated reads,
and a file larger than available memory. Barrier workloads keep an unrelated
inode busy while flushing or syncing the target inode.

## Idempotence and Recovery

All tests and benchmark preparations are repeatable. Temporary files and
artifacts belong below `target/`. Do not remove user files or reset the working
tree. The direct cache policy is the runtime rollback:

    STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct studiofs mount ...

If a platform reply fails after the VFS handle was opened, abort its cache
observation and close that handle before returning. If invalidation shutdown
times out, leak the raw session rather than destroy memory still reachable from
kernel IPC. If asynchronous-write shutdown times out, cancellation and request
guards must produce bounded `EIO` completion before session destruction.

## Artifacts and Notes

The branch started clean at:

    191ca80e Fix unused Unix child job detach method

The old Linux experiment can be inspected read-only with:

    git show 7f85ec8c:crates/studiofs-vfs/src/vfs/os_cache.rs
    git show 7f85ec8c:crates/studiofs-linux-fuse/src/async_writes.rs

These files are references only. Recreate behavior against current types and
contracts.

## Interfaces and Dependencies

`studiofs-vfs` must expose documented, small `Copy` decision types:

    pub enum OsCacheParticipation { Disabled, Enabled }
    pub enum OsCacheDisposition { Reuse, Replace }
    pub enum OsCacheReason { /* reasons described above */ }
    pub struct OsCacheOpenDecision { /* disposition and reason */ }
    pub enum ReadAheadOwner { Vfs, Kernel }

`StudioVfs` must expose:

    pub fn prepare_os_cache_open(
        &self,
        fh: u64,
        participation: OsCacheParticipation,
    ) -> OsCacheOpenDecision;

    pub fn abort_os_cache_open(&self, fh: u64);

    pub fn set_read_ahead_owner(
        &self,
        fh: u64,
        owner: ReadAheadOwner,
    ) -> VfsResult<()>;

The Linux crate may add `sysinfo` from the existing workspace dependency. Do not
add a new async runtime, queue library, or cache crate. Use existing Tokio,
standard synchronization primitives, `parking_lot` where already appropriate,
and the existing `lru` workspace dependency.

Revision note (2026-07-29): created the initial self-contained ExecPlan after
the focused clean baseline. It incorporates the approved Linux parity design,
current-main discoveries, implementation order, TDD requirements, and recovery
rules.

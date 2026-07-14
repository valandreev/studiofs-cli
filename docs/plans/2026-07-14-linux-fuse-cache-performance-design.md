# Linux FUSE cache, throughput, and CPU design

Status: accepted design, pending implementation plan

Date: 2026-07-14

Baseline revision: `95bfdd8fcd03`

## Summary

Port the effective macOS filesystem behavior to Linux without copying
macFUSE-specific mechanisms literally. Most recent write, materialization, and
upload improvements already live in the shared VFS and are active on Linux.
The remaining work is concentrated in the Linux FUSE bridge, safe operating
system page-cache reuse, handle lifecycle integration, and observability.

The implementation will proceed in evidence-gated stages:

1. restore Linux local-durability and per-handle ordering invariants;
2. enable and validate parallel direct I/O;
3. move open-cache coherence into the shared VFS and preserve current macOS
   behavior through that module;
4. connect Linux to the shared cache and foreground-I/O lifecycle;
5. promote Linux kernel caching to the default only if the full correctness,
   throughput, latency, memory, and CPU matrix passes;
6. optimize the remaining read bridge or shared upload scheduler only when a
   measured bottleneck remains.

`direct_io` remains an explicit Linux rollback path throughout the rollout.

## Context

The macOS performance work in PRs #595, #597, and #600 established the final
behavior that this design uses as the conceptual reference:

- buffered mounted files with conservative generation-aware cache retention;
- interruptible small-write coalescing and strict local durability barriers;
- a separate materialization gate;
- hard-limit-only dirty-memory backpressure;
- foreground-aware upload yielding and pacing;
- full-handle foreground read tracking;
- bounded read admission and cancellation;
- local reads from staged and hot upload payloads;
- kernel invalidation liveness and close-time `ENOENT` normalization.

The following are already shared and therefore must be verified on Linux, not
reimplemented in the Linux adapter:

- 200 ms deferred-materialization coalescing;
- strict-barrier interruption and draining;
- the materialization gate;
- hard dirty-memory admission;
- `block_in_place` local-cache reads;
- foreground-aware upload scheduling;
- staged/hot local payload reads;
- epoch and stale-upload protection;
- shared-worker shutdown cancellation.

The principal Linux gaps are:

- production defaults to `direct_io` and disables `ASYNC_DIO`;
- the experimental kernel-cache policy has no complete generation and writer
  coherence envelope;
- Linux does not track foreground reads for the full handle lifetime;
- `flush` and dirty last release are weaker than the canonical contract;
- async write barriers wait for a mount-global queue rather than the target
  handle;
- callback metrics stop at enqueue instead of kernel-visible completion;
- negotiated FUSE geometry and barrier latency are not observable;
- Linux has no `forget` callback/liveness integration;
- forced unmount can destroy a libfuse session while async replies are pending.

## Diagnostic baseline

The following short runs were taken on the same eight-CPU Linux host with a
current release build and a mounted StudioFS volume. They are diagnostic
single runs, not acceptance evidence.

| Mode | Workload wall time | Bench write average | Bench read average | Mount CPU peak |
| --- | ---: | ---: | ---: | ---: |
| Direct | 8.05 s | 321.6 MB/s | 148.4 MB/s | about 5.7 cores |
| Direct + `ASYNC_DIO` | 5.21 s | 523.3 MB/s | 264.9 MB/s | about 5.3 cores |
| Kernel cache, cold generation | 11.15 s | 279.1 MB/s | 63.6 MB/s | about 7.2 cores |

The workload was 512 MiB, single-file, read/write, with batch fsync. The
benchmark throughput values exclude some lifecycle work and must not be
treated as wall-clock throughput.

After the generation became stable, a second 256 MiB read completed in 0.11 s
with no additional FUSE read callbacks. This demonstrates the upside of the
kernel page cache and also shows why cold and warm results must be reported
separately.

Single diagnostic runs with four and eight upload workers reduced some CPU
peaks but increased total wall time relative to twenty workers. No fixed worker
count will be selected from these runs.

Artifacts are kept outside the source tree under
`target/bench-results/linux-port-baseline/`.

## Goals

- Preserve the local-first contract for write acceptance, flush, fsync, close,
  reopen, and local durability.
- Use the Linux kernel page cache safely when it improves real workloads.
- Preserve or improve cold sequential and small-file performance.
- Eliminate avoidable mount-global head-of-line blocking.
- Reduce mount CPU cost and foreground CPU contention without hiding upload
  drain time.
- Reuse the shared behavior that already gives macOS and Windows acceptable
  CPU characteristics.
- Produce repeatable benchmark and CPU evidence before changing the Linux
  default cache policy.
- Keep macOS behavior unchanged and avoid introducing Windows regressions.

## Non-goals

- Waiting for PostgreSQL convergence or S3 upload from `flush`, `fsync`, or
  close.
- Copying macOS `F_NOCACHE`, UBC mount options, or the 64 MiB macFUSE read
  window directly to Linux.
- Adding a Linux-only CPU scheduler before the existing shared foreground QoS
  is connected and measured.
- Hard-coding an upload worker count from one benchmark run.
- Promoting kernel caching before mounted coherence tests pass.
- Rewriting the benchmark tool unless the existing StudioFS bench and
  devbench surfaces cannot provide the required evidence.

## Design principles

1. **Shared semantics, thin platform adapters.** Cache identity, handle
   lifetime, local-durability semantics, and foreground QoS belong in shared
   modules. FUSE flags, callbacks, replies, and negotiated connection geometry
   remain platform-adapter responsibilities.
2. **Local-only foreground operations.** Cache decisions and durability
   barriers may use local metadata and payload state but never wait for remote
   services.
3. **Conservative cache failure.** Unknown local cache facts drop kernel pages;
   they do not fail an otherwise valid file open.
4. **Errors cross the next barrier.** A background write error is retained and
   surfaced by the associated write request or the next relevant local
   barrier.
5. **Evidence before policy promotion.** An experimental mechanism becomes a
   default only after repeatable correctness and performance evidence.
6. **One controller first.** Linux activates the existing shared foreground
   upload controller before any new concurrency algorithm is considered.

## 1. Linux async-write ordering and durability

### Queue structure

Keep the mount-global admission budgets for pending bytes and pending request
count. They protect total process memory and libfuse request pressure.

Separate those budgets from barrier ordering. Each file handle receives a
monotonic write sequence and completion ledger:

- a successfully admitted async write receives the next handle sequence;
- completion records success or the first unreported error for that sequence;
- `flush`, `fsync`, and `release` capture the handle's current watermark;
- a barrier waits only for sequences at or below that watermark;
- later writes and writes to other handles do not extend the wait;
- global reservations are released exactly once on every success, error,
  cancellation, and shutdown path.

This preserves a single global memory cap while removing mount-wide barrier
head-of-line blocking.

### Barrier behavior

- `flush` first waits for the target handle watermark, then calls the strict
  local `flush_fh` path.
- `fsync` remains a strict local handle or inode barrier.
- dirty last-handle release completes synchronously and surfaces local errors.
- clean release may remain deferred.
- `O_SYNC` and `O_DSYNC` writes remain synchronous.
- close-time `ENOENT` caused by a concurrent delete or rename is normalized;
  other errors remain visible.
- no barrier waits for remote publication or upload.

The shared VFS release outcome remains the authority for whether the last
handle closed and whether local dirty state was materialized.

### Session shutdown

Before `fuse_session_destroy`:

1. close async-write admission;
2. prevent new callback tasks from retaining requests;
3. finish or cancel all pending tasks;
4. send at most one reply for every retained libfuse request;
5. drain reservations and handle ledgers;
6. unmount and destroy the session;
7. continue normal shared VFS shutdown.

This removes the late-reply/raw-request lifetime hazard during forced unmount.

## 2. Shared operating-system cache coherence

### Seam and ownership

Create a concrete deep module in `studiofs-vfs`, conceptually
`vfs::os_cache`. Both macFUSE and Linux libfuse3 are real adapters at this
seam. The module has no platform FFI and no remote dependencies.

The adapter-facing interface is intentionally small:

```rust
let fh = vfs.open_file_with_known_attr(&attr, flags, trunc)?;
let decision = vfs.prepare_os_cache_open(fh, participation);
adapter.reply_open(fh, adapter.cache_flags(decision));
```

`participation` indicates whether this adapter/open is allowed to consider OS
cache reuse. Linux direct mode remains adapter policy. The VFS obtains inode,
access mode, and other handle facts from the opened handle rather than asking
the adapter to duplicate them.

The decision contains:

- `Reuse` or `Replace`;
- a stable reason used for metrics and diagnostics.

The adapter maps the decision to its platform flags. The shared module never
chooses Linux `direct_io` or macFUSE-specific flags.

### Generation identity

For regular files, exact cache identity includes the visible content epoch,
logical size, relevant local state, and complete ordered file/slice graph.
Publication-only transitions that do not change visible bytes retain the same
identity.

Decision rules:

- write-capable open: clear the remembered generation and return `Replace`;
- non-regular file: `Replace`;
- local dirty state: clear and `Replace`;
- active writer: clear and `Replace`;
- unknown local metadata or graph: clear and `Replace(UnknownLocalState)`;
- first observation of a stable generation: remember it and `Replace`;
- changed generation: replace the remembered identity and return `Replace`;
- repeated identical stable generation: `Reuse`.

The generation store is bounded. The initial implementation preserves exact
graph comparison. A compact fingerprint is allowed only if profiling proves
the comparison materially expensive and the replacement retains the same
correctness strength.

### Handle lifetime

Foreground read and writer state belongs to the VFS handle lifecycle, not to
duplicated adapter maps.

- opening a participating read handle starts foreground-read activity;
- opening a write-capable handle starts writer activity and clears cache
  identity;
- the state remains active through the local release barrier;
- every release path, including error and deferred paths, finishes it exactly
  once;
- the last writer leaves the next read open in `Replace` state.

The release context owns the lifecycle state until its local barrier finishes,
so an adapter cannot omit cleanup on an error path.

### Invalidation and liveness

Full data invalidation clears the shared generation before calling the
platform invalidator. External kernel callbacks are never invoked while a
shared cache-state lock is held.

Lookup-reference liveness is a separate small shared module in
`studiofs-bridge` because macFUSE and libfuse3 expose the same lookup/forget
concept while WinFSP does not. The platform adapters retain callback wiring.

The liveness module:

- counts lookup references by inode rather than pathname;
- handles hardlinks and rename-overwrite correctly;
- closes only after the final lookup reference or final inode retirement;
- prevents notification of unknown or forgotten kernel inodes;
- permits re-entrant close and unrelated inode progress without holding a
  map/shard guard across kernel IPC.

Linux will install its `forget` callback before kernel caching can become the
default.

## 3. Linux cache policy and FUSE behavior

### Direct mode

Direct mode remains available throughout development and after rollout.

`ASYNC_DIO` and per-file parallel direct writes become the candidate Direct
default after ordering and same-file mounted tests pass. The initial diagnostic
run showed a large wall-time and throughput improvement, but the change is not
promoted on that single run alone.

### Kernel mode

Kernel mode uses:

- `direct_io = false`;
- `keep_cache = true` only for a shared `Reuse` decision on eligible read-only
  handles;
- `keep_cache = false` for writers, dirty/unknown state, and changed or first
  generations;
- the negotiated writeback-cache capability only when supported by libfuse
  and the local barrier tests pass.

The policy remains opt-in until the full matrix succeeds. On promotion,
`STUDIOFS_LINUX_FUSE_CACHE_POLICY=direct` remains the operational rollback.

### Geometry

Record the actual connection values for maximum read, maximum write,
readahead, background requests, and congestion threshold. Also record the
actual callback-size distribution.

Do not copy macFUSE geometry or change Linux `st_blksize` until the measured
Linux callback path justifies it. Any geometry change is benchmarked
independently from the cache-policy change.

### Backing payload cache

Linux may hold both mounted-inode pages and local backing-payload pages. The
macOS `F_NOCACHE` solution is not portable. Measure RSS, backing-file I/O, and
page-cache behavior before considering Linux `O_DIRECT`, `fadvise`, or another
mechanism. Alignment-heavy direct backing-file I/O is not part of the initial
implementation.

## 4. CPU and observability

### Reuse existing shared QoS first

macOS and Windows already have acceptable CPU behavior. Linux therefore first
connects full handle lifetime to the existing shared foreground-aware upload
and disk-reread pacing.

No Linux-only concurrency controller is added at this stage. If CPU remains
high after lifecycle integration, tune the existing shared controller so all
platforms retain one scheduling model. Explicit upload-worker overrides remain
available.

### Required instrumentation

Add or correct the following Linux metrics:

- async write completion latency measured through `fuse_reply_write`, not
  enqueue;
- pending and peak global bytes/request count;
- per-handle pending writes and barrier wait time;
- completed, failed, task-failed, and cancelled writes;
- read/write callback size and latency histograms;
- `flush`, `fsync`, and `release` callback histograms;
- active foreground read and writer handles;
- negotiated FUSE geometry;
- OS cache decisions and reasons for both adapters;
- kernel invalidation and lookup/forget liveness outcomes.

The metrics must distinguish foreground workload time from background upload
drain time.

### CPU measurement

Use three complementary sources:

- `studiofs devbench` 100 ms mount-process sampling for CPU, accumulated CPU
  time, RSS, virtual memory, and disk I/O;
- `pidstat` aligned with `write_path_observability.json` for per-second upload,
  dirty-pressure, foreground, callback, and async-write correlation;
- one separate mount with `--profile` for hotspot attribution.

Profiling runs are diagnostic and are excluded from clean throughput medians.
`/usr/bin/time -v` around `studiofs-bench` describes the benchmark client, not
the mount-process CPU.

Report CPU as:

- mount CPU-seconds per GiB;
- average and peak core equivalents during foreground work;
- average and peak core equivalents during upload drain;
- upload drain duration;
- peak mount RSS.

If lifecycle-aware shared QoS is insufficient, compare upload concurrency and
controller tuning over repeated runs. A change is accepted only when its CPU
improvement exceeds run-to-run noise without violating foreground performance
or local-first progress.

## 5. Error handling

- An OS-cache observation failure is a conservative `Replace`, not an open
  failure.
- VFS open errors remain VFS open errors.
- Async write task errors are retained until reported and cannot be erased by
  another handle's progress.
- A barrier reports the first relevant unreported local error.
- Close-time `ENOENT` is normalized only for the concurrent-delete case.
- Shutdown cancellation replies exactly once or completes the request before
  session destruction.
- No new path turns local durability into remote durability.

## 6. Verification

### Focused correctness tests

Shared VFS/module tests:

- first-generation replace and stable-generation reuse;
- size, epoch, and slice-graph generation changes;
- publication-only transition preserving identity;
- dirty state and active writer rejection;
- writer open and last-writer release invalidation;
- foreground handle lifetime on success, failure, and deferred release;
- invalidation racing generation observation;
- bounded generation eviction;
- hardlink, lookup, forget, final unlink, and rename-overwrite liveness.

Linux adapter tests:

- shared decision to libfuse flag mapping;
- Direct and Kernel policy selection;
- `ASYNC_DIO` capability and parallel-write flags;
- per-handle write watermark ordering;
- cross-file barrier isolation;
- retained background error propagation;
- dirty synchronous and clean deferred release;
- close-time `ENOENT` normalization;
- forced unmount with a pending async request;
- negotiated geometry publication.

Mounted tests:

- write/read/reopen visibility;
- append, truncate, same-size overwrite, and sparse writes;
- concurrent reader and writer;
- rename, unlink, hardlink, and delete while open;
- remote invalidation and generation replacement;
- stable warm reopen producing no VFS callbacks;
- same-file parallel direct writes;
- cross-file fsync/close isolation;
- forced unmount lifecycle.

Shared VFS tests protect Windows behavior. Existing macFUSE cache tests are
retargeted to the shared interface rather than duplicated internal state.

### Performance workloads

Run each candidate three times while iterating and five times for the final
decision. Wait for the upload queue to become idle before each clean run.

1. 4,000,000,000-byte single-file read/write using bundled
   `studiofs-bench`.
2. 4,000 files of 1 MB each with the same total workload.
3. 1,000 files of 128 KiB using `studiofs devbench`.
4. Stable warm reopen after upload and generation activity become idle.
5. Same-file parallel direct-write workload.
6. Cross-file barrier-isolation workload.

Compare at least:

- baseline Direct;
- Direct + `ASYNC_DIO`;
- candidate Kernel policy;
- any evidence-driven scheduler or geometry variant.

Cold reads use a fresh file/generation. Warm reads reopen an unchanged stable
generation. The benchmark target must be verified with `findmnt` as
`fuse.studiofs`; the benchmark's `mounted` label alone is insufficient.

### Acceptance gates

- all correctness and mounted tests pass;
- no primary workload median regresses by more than 5% against its applicable
  baseline;
- no new long callback or barrier tail appears in p95/p99/max data;
- CPU-seconds per GiB does not regress;
- claimed CPU improvements exceed median absolute deviation/run-to-run noise;
- foreground CPU and upload-drain CPU are reported separately;
- peak RSS does not show an unexplained double-cache growth;
- warm reuse is demonstrated by unchanged FUSE/VFS read callback counts;
- direct rollback is verified before Kernel becomes default.

If Kernel mode fails any gate, retain Direct as the default and ship only the
validated Direct, correctness, observability, and shared-QoS improvements.

## 7. Rollout

1. Add correctness tests and honest observability around the current Linux
   bridge.
2. Implement per-handle async-write fences and safe session shutdown.
3. Validate and promote `ASYNC_DIO` for Direct mode.
4. Extract current macOS cache-generation behavior into the shared VFS module
   without changing macOS decisions.
5. Extract common FUSE lookup liveness into `studiofs-bridge` and wire Linux
   `forget`.
6. Connect Linux open/release/invalidation and foreground lifetime to the
   shared modules.
7. Run the repeated Direct/Kernel/CPU matrix.
8. Promote Kernel mode only if every gate passes.
9. Profile and address any remaining measured read-bridge, geometry, backing
   cache, or shared-scheduler bottleneck.
10. Run the five-pass final matrix and publish the comparison artifacts.

Each stage is independently testable and keeps a working rollback. The exact
commit breakdown belongs in the implementation plan.

## 8. Documentation updates

Implementation must update the stale Linux statements and operational
reference in:

- `docs/development.md`;
- `docs/configuration.md`;
- `docs/tests.md`;
- `docs/platform-bridges.md`;
- relevant ADRs if the validated default supersedes the Direct-default
  decision.

The documentation must distinguish stable user configuration from temporary
benchmark/developer environment switches.

## Resolved decisions

- There is no external parity target; the goal is the best validated Linux
  behavior without slowing existing workloads.
- CPU is an acceptance dimension, not an afterthought.
- The shared macOS/Windows mechanisms are preferred over Linux-only policy.
- Local `flush` and dirty last close are strict barriers even if they add some
  close latency.
- Cache state and handle cleanup belong to the shared VFS.
- Platform adapters only map shared decisions to platform flags and callbacks.
- Existing foreground-aware shared QoS is activated before designing another
  CPU controller.
- Kernel caching becomes the Linux default only after the complete matrix
  passes.

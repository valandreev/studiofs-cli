# LinuxNativeBridge Contract

SFS-524 defines `LinuxNativeBridge` as the owned Linux platform boundary for
high-throughput StudioFS mounts. The bridge is not a fuser/fuse-rs adaptation
layer, and fuser 0.17 is no longer the implementation basis. The current
`studiofs-fuse` crate remains the fuser 0.16 bridge until the native Linux
chain replaces it behind explicit rollout gates.

The contract keeps StudioFS VFS/cache/writeback semantics intact while making
Linux FUSE transport, kernel cache policy, local-cache read/write paths,
cache/coherency rules, telemetry, and benchmark gates explicit.

## Boundary Shape

The implementation should split the Linux bridge into independently testable
modules before performance work spreads across transport, VFS, cache, reads,
writes, and benchmarks:

| Boundary | Responsibility |
|---|---|
| `abi` | Unsafe FUSE ABI parsing, byte layout validation, opcode tables, request IDs, and reply serialization. Unsafe code stays in this layer and carries local safety rationale. |
| `transport` | Classic `/dev/fuse` and optional FUSE-over-io_uring session I/O. This layer owns file descriptors, clone fd handling, syscall/io_uring transport calls, queue lifecycle, and fallback. |
| `protocol` | Typed requests, typed replies, capability flags, request/reply ownership, and transport-independent validation. |
| `dispatcher` | Async dispatch from protocol requests into operation handlers, cancellation, interrupt handling, bounded concurrency, and shutdown draining. |
| `vfs_adapter` | VFS adapter API from Linux FUSE operations into `StudioVfs`, including inode, handle, open flag, setattr, readdir, flush, fsync, release, invalidation mapping, and isolation from VFS business logic. |
| `cache_policy` | Kernel cache policy, keep-cache/direct-I/O decisions, cache/coherency rules, invalidation triggers, bounded readahead, prefetch, and partial-cache controls. |
| `lifecycle` | Mount lifecycle, init negotiation, mountpoint setup, unmount, signal handling, and shutdown ordering. |
| `telemetry` | Per-op latency, queue depth, background saturation, copy counts, cache hit/miss, fallback reason, and benchmark export fields. |

The crate boundary can start as a Linux-only module inside the existing bridge
workspace, but the public contract must be independent of `fuser::Filesystem`.
The upper VFS should see a stable adapter surface rather than transport-specific
request structs.

## Transport Contract

`FuseTransport` is the transport boundary. It accepts kernel work from one
backend and emits typed protocol messages to the dispatcher:

```text
Linux kernel
  -> FuseTransport
  -> typed FuseRequest
  -> dispatcher
  -> VfsAdapter / StudioVfs
  -> typed FuseReply
  -> FuseTransport
  -> Linux kernel
```

The dispatcher receives typed requests and sends typed replies independent of
whether the source is classic `/dev/fuse` read/write or FUSE-over-io_uring
completion queue events and `uring_cmd` submission flow. Request buffers remain
owned until the matching reply or cancellation path completes. Reply buffers are
owned by the protocol layer until the selected transport has accepted or failed
the response.

Transport implementations must expose:

- negotiated capability flags and kernel protocol version;
- maximum write size, maximum readahead, background queue limits, congestion
  threshold, and notify support;
- request class support, including explicit coverage for normal requests,
  interrupts, forgets, batch forgets, and daemon-side notifications;
- orderly shutdown hooks that stop intake, drain or cancel in-flight requests,
  and release file descriptors after VFS release-path work has run.

## Classic `/dev/fuse`

The Classic /dev/fuse backend is the required baseline for every
LinuxNativeBridge rollout.

Classic `/dev/fuse` is the baseline backend. It opens the FUSE connection,
negotiates capabilities, reads request frames from the connection fd, writes
reply frames back, and may use clone fd support where the kernel and mount
profile allow it.

Classic fallback remains mandatory. The Linux kernel FUSE-over-io_uring
documentation says the interface is still in development and that userspace
must continue to handle unsupported request types through `/dev/fuse` after
io_uring setup. The unsupported classes named by the kernel documentation
include daemon-side notifications and interrupts. StudioFS therefore must not
treat io_uring as the only production transport.

## FUSE-over-io_uring

FUSE-over-io_uring is an optional backend/profile, not the baseline. It should
share the same `FuseTransport` and protocol contract as classic `/dev/fuse`.
The backend may become the fast path when probing proves the host kernel,
module, permissions, and queue configuration are compatible.

The profile must include:

- capability probing for FUSE-over-io_uring support and the advertised
  `FUSE_CAP_OVER_IO_URING`-style capability;
- kernel/module detection, including module parameters such as io_uring enablement;
- queue-depth config, with defaults recorded in telemetry and benchmark reports;
- per-CPU queue implications for affinity, work stealing, and fairness;
- fallback to classic `/dev/fuse` for unsupported request classes, setup
  failure, runtime transport errors, or operator rollback;
- clear separation between io_uring as a FUSE transport and io_uring as a local
  cache/payload execution strategy.

The bridge may use io_uring for local-cache read/write paths separately from
FUSE-over-io_uring. That local-I/O strategy must not leak into the protocol API
or change StudioFS local-first durability semantics.

## VFS And Local-First Semantics

`LinuxNativeBridge` adapts Linux FUSE operations to `StudioVfs`; it does not
move filesystem business logic into the bridge. Foreground reads and metadata
lookups continue to resolve through local VFS state. Writes still enter writer
state and cached-store staging before background upload and peer publication.

The bridge must preserve these rules:

- `write` accepts bytes into local writer/cache state and does not wait for
  remote upload or PostgreSQL replication.
- `flush` freezes and finishes dirty slices owned by the handle or inode.
- `fsync` remains the strict caller-requested local sync API.
- `release` drains remaining local writer state when required without moving
  remote publication into the foreground path.
- EOF-changing paths persist local EOF and attributes before returning.
- Remote invalidation and local writes trigger VFS invalidation before Linux
  kernel page-cache reuse can expose stale bytes.

## Cache And Coherency Rules

Linux buffered I/O remains the default until a benchmarked profile proves a
safer alternative. Read-only opens may use keep-cache when VFS content epochs
and invalidation state make reuse safe. Write-capable opens must keep write
coherency explicit and must not request `FUSE_WRITEBACK_CACHE` by default.

The kernel cache policy must define:

- page-cache validity policy for local writes, truncation, sparse updates,
  remote invalidation, and content epoch changes;
- bounded readahead defaults and operator controls;
- separate prefetch work so cache-miss fills do not block unrelated dispatch;
- singleflight/cache-miss behavior for concurrent readers of the same payload;
- partial-cache controls for sparse files and range reads;
- direct-I/O and async-DIO opt-in behavior for benchmark profiles.

### SFS-528 Page-Cache Policy

SFS-528 makes the native Linux page-cache decision an explicit `cache_policy`
boundary instead of an incidental open flag. The decision input is
`NativeInodeCacheState`, a per-inode snapshot containing the StudioFS content
epoch, size, mtime, writer state, dirty state, local materialization state, and
invalidation generation. The output is `NativeOpenCacheDecision`, which carries
both a kernel action and a metrics reason.

The initial decision table is:

| Condition | Action | Reason |
|---|---|---|
| Read-only open, local materialization is present, content epoch/size/mtime match the last trusted kernel-cache state, and invalidation generation is unchanged | `KEEP_CACHE` | `Keep` |
| Content epoch, size, mtime, or invalidation generation changed after a previously trusted cache generation | `InvalidateThenCached` | `StalePrevented` |
| Active writer state or dirty state exists for the inode | `FOPEN_DIRECT_IO` | `WriterBarrier` |
| Local materialization was evicted for the trusted generation | `InvalidateThenCached` | `LocalCacheEvicted` |
| No trusted kernel-cache generation is available | normal cached I/O | `Cached` |

`AUTO_INVAL_DATA` may be requested when the kernel advertises it, but StudioFS
does not rely on that capability alone. StudioFS content-epoch knowledge is
stronger than mtime/size comparison, so overwrite, truncate, remote refresh,
writer commit, rename/unlink visibility changes, and local cache eviction must
still drive explicit invalidation before stale kernel pages can be reused.

`writeback_cache` stays a profile gate. It must remain disabled by default
until separate correctness tests cover read-after-write, fsync, flush, release,
truncate, and remote-invalidation interactions for that profile.

Metrics must expose why every native Linux open/read path kept, invalidated,
used direct I/O, or fell back to normal cached I/O. The minimum reason surface is
`Keep`, `Cached`, `StalePrevented`, `WriterBarrier`, and `LocalCacheEvicted`.
Those counters are the benchmark and debugging vocabulary for cache/coherency
decisions.

## References And Clean-Room Use

Curvine is a reference for low-level Rust FUSE transport ideas only. The
relevant ideas are raw FUSE session ownership, clone fd use, async
sender/receiver separation, splice/vmsplice experiments, and capability
negotiation. Do not copy code without license/legal review.

JuiceFS is a reference for cache/read policy only. The relevant ideas are
kernel page-cache validity policy, bounded readahead, separate prefetch,
singleflight/cache-miss behavior, and partial-cache controls. StudioFS should
adapt those behaviors to its local-first VFS/cache model instead of importing
implementation structure.

## Telemetry And Benchmark Gates

The first native Linux rollout must be performance-led. Every transport profile
should emit enough telemetry to explain throughput, latency distribution, CPU
cost, context switching, queue saturation, cache hit rate, copy count, fallback
rate, and shutdown behavior with pending work.

Minimum benchmark gates:

- sequential and random reads across cold, warm, and mixed cache states;
- metadata-heavy `getattr`, `lookup`, `readdir`, and `readdirplus`;
- mixed read plus metadata concurrency;
- write plus `flush`/`release`/`fsync` ordering;
- mount shutdown with pending work;
- classic `/dev/fuse` versus FUSE-over-io_uring where the host supports both;
- cache/coherency correctness under local writes, truncation, remote
  invalidation, and reopen.

If LinuxNativeBridge cannot meet the gates after implementation, stop and
revisit architecture instead of falling back implicitly to fuser tuning.

## Rollout Decision Gates

Follow-up issues can be executed independently when they honor this contract:

- SFS-525 builds the Linux FUSE session with classic and io_uring-ready
  transports behind `FuseTransport`.
- SFS-533 adds the optional FUSE-over-io_uring backend/profile.
- The old SFS-517 through SFS-523 fuser 0.17 plan stays canceled as the primary
  implementation path.
- The native bridge does not become default until mounted smoke, local-first
  contract tests, and benchmark gates are recorded.

Relevant external references:

- Linux kernel FUSE-over-io_uring design documentation:
  <https://docs.kernel.org/next/filesystems/fuse-io-uring.html>
- libfuse FUSE-over-io_uring notes:
  <https://raw.githubusercontent.com/libfuse/libfuse/master/doc/README.fuse-io-uring>

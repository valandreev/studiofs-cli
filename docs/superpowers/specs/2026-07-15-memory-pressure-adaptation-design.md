# Memory-pressure adaptation design

## Status

Approved in design discussion on 2026-07-15. This document defines the scope
for an implementation plan; it does not change runtime behavior by itself.

## Problem

StudioFS currently derives several independent memory limits from total host
RAM, configured memory-cache size, and disk-cache size. It does not constrain
their sum using memory that is actually available to the mount process.

On an 8 GiB host with 5 GiB already occupied, current defaults can permit
approximately:

- 2 GiB for the memory cache;
- 800 MiB of dirty data;
- 800 MiB of transit buffers;
- 256 MiB in the Linux asynchronous FUSE write queue;
- additional metadata, allocator, and temporary allocations.

The potential managed allocation therefore exceeds the host's remaining
headroom. Kernel-cache mode makes the pressure more visible because dirty
kernel pages cannot be reclaimed before writeback, but the underlying problem
is the absence of one process-wide memory envelope.

## Goals

- Mount successfully on a memory-constrained host instead of requiring a fixed
  operator-supplied limit.
- Derive initial limits from currently available host and cgroup memory.
- Apply runtime backpressure when other processes consume memory after mount.
- Preserve admitted writes and local-first durability behavior.
- Avoid CPU spinning while writers wait for memory.
- Recover throughput gradually after pressure clears.
- Keep Direct plus negotiated ASYNC_DIO as the Linux promotion path.

## Non-goals

- Implementing a second LRU for the Linux kernel page cache.
- Clearing clean kernel pages proactively.
- Switching a mounted filesystem between Direct and kernel-cache modes.
- Dynamically redesigning every StudioFS cache.
- Promoting Linux kernel-cache mode to the default.

The kernel already owns global page-cache reclaim. StudioFS invalidates kernel
pages only for generation changes and coherence. It does not use
`fuse_lowlevel_notify_inval_inode` as a memory-pressure eviction mechanism.
Likewise, `posix_fadvise(DONTNEED)` is not added to the Direct path because the
backing-file page cache supplies useful read reuse there.

## Effective memory headroom

Linux calculates current headroom from both the host and the active cgroup v2
hierarchy:

```text
host_headroom = /proc/meminfo:MemAvailable
cgroup_headroom = memory.max - memory.current  # when memory.max is finite
available = min(host_headroom, cgroup_headroom)
```

If the cgroup has no finite `memory.max`, host headroom is used. The effective
RAM limit used for the reserve is the lower finite value of host total RAM and
cgroup `memory.max`.

```text
system_reserve = max(1 GiB, 15% of effective RAM limit)
allocatable = max(available - system_reserve, 0)
configured_managed_ceiling = memory cache + dirty + transit + async ceilings
configured_envelope_ceiling = ceil(configured_managed_ceiling / 0.90)
studiofs_envelope = min(configured_envelope_ceiling, 75% of allocatable)
```

When no reliable memory source is available, StudioFS uses a conservative
minimum profile rather than failing mount. Explicit existing internal tuning
overrides remain ceilings; they cannot raise a component above the current
envelope.

The initial envelope is divided as follows:

| Consumer | Share |
| --- | ---: |
| Memory cache | 40% |
| Dirty and writer buffers | 25% |
| Read/upload transit buffers | 15% |
| Linux asynchronous FUSE write queue | 10% |
| Unmanaged process headroom | 10% |

Rounding must never make the managed shares exceed 90% of the envelope. The
minimum profile is explicit:

| Consumer | Minimum |
| --- | ---: |
| Memory cache | `max(block_size, 64 MiB)` |
| Dirty and writer buffers | `max(block_size, 64 MiB)` |
| Transit buffers | `max(block_size, 32 MiB)` |
| Linux async queue | `max(negotiated_max_write, 32 MiB)` |

If the calculated envelope cannot fund these minima, StudioFS publishes the
minimum profile with an `overcommitted_minimum` diagnostic flag and keeps new
writes effectively synchronous through admission backpressure. The minimum
exists only to let one bounded operation make forward progress; it is not a
performance target.

## Runtime controller

A single `MemoryPressureController` belongs to the mount runtime. One
mount-owned cancellable Tokio task samples every 500 ms and publishes an
immutable snapshot containing:

- the selected memory source;
- effective limit and available/headroom bytes;
- system reserve and StudioFS envelope;
- component limits;
- `normal` or `constrained` state;
- snapshot generation.

Consumers read the latest snapshot; they do not independently inspect `/proc`
or cgroup files.

The controller is constrained when available memory falls below the reserve or
when the newly calculated envelope is below the currently admitted managed
bytes. Entering constrained state lowers admission limits immediately but does
not cancel accepted work. Recovery requires two consecutive healthy samples.
After recovery, each component limit grows by at most 25% of its configured
ceiling per sample. This hysteresis prevents rapid throttle/unthrottle cycles.
Currently admitted managed bytes are the sum of resident memory-cache bytes,
dirty/writer bytes, transit reservations, and pending Linux async-write bytes.

Failure to read a sample retains the last valid snapshot. If no valid snapshot
has ever been produced, the controller publishes the conservative minimum
profile. Sampling failure does not abort the mount. Shutdown cancels and joins
the sampler before dropping the runtime resources it observes.

## Consumer behavior

### Memory cache

The cache does not admit additional RAM-resident entries above the current
limit. When the limit shrinks, reclaimable entries are evicted using the
existing cache eviction order. In-use entries are not forcibly removed.

### Dirty and writer buffers

Write admission uses the current dirty-buffer limit. Under pressure, already
admitted writes continue and materialization begins at the reduced threshold.
New writers wait until managed dirty bytes fall below the current limit.

### Transit buffers and concurrency

Read, upload, and prefetch admission uses the current transit budget. Pressure
reduces new prefetch and upload concurrency; it does not interrupt active I/O.

### Linux asynchronous FUSE writes

The current fixed 256 MiB queue limit becomes a ceiling. Its active limit is
the smaller of that ceiling and the envelope allocation. Admission blocks when
pending bytes reach the active limit. Waiting uses the existing condition
variable and must not poll or spin.

### Kernel page cache

StudioFS does not track a kernel-cache LRU. Under pressure it stops generating
new dirty work and drains existing dirty data so kernel pages become
reclaimable. The Linux kernel remains responsible for selecting clean pages to
evict.

## Runtime statistics

Runtime stats add a `memory_pressure` section containing:

- source (`host`, `cgroup`, `host-and-cgroup`, or `fallback`);
- effective limit, available, reserve, and envelope bytes;
- current component limits;
- state and snapshot generation;
- transition count;
- sampling failure count;
- whether the minimum profile currently exceeds the calculated envelope;
- write-admission wait count and cumulative wait time.

These values are diagnostic. This design adds no new public tuning option.

## Verification

Unit and deterministic tests cover:

- 8 GiB total RAM with 5 GiB occupied;
- a finite cgroup limit lower than host RAM;
- an unlimited or absent cgroup limit;
- malformed and unreadable system inputs;
- component rounding and minimum-profile behavior;
- headroom falling below and later recovering above the reserve;
- hysteresis and bounded limit growth;
- admitted writes completing across a pressure transition;
- blocked writers waking without CPU spin;
- clean shutdown while writers are waiting.

Mounted Linux validation uses a delegated cgroup and a separate memory-holder
process to create reproducible pressure. It requires:

- no increment in cgroup `oom` or `oom_kill` events;
- managed limits never exceeding the recorded envelope;
- successful byte-for-byte write and read verification;
- a clean drain and unmount;
- bounded foreground and drain CPU;
- throughput recovery after the holder releases memory.

The mounted suite repeats `small-128k` and `large-single`. Direct plus ASYNC_DIO
is the promotion path. Kernel-cache results remain diagnostic and must continue
to satisfy correctness even when they miss performance tail limits.

## Compatibility

The controller interface is platform-neutral, but this implementation enables
adaptive probing only on Linux. macOS and Windows return a disabled controller
and retain their current budgets and behavior. A later platform-specific design
may enable equivalent probes there. Existing explicit internal tuning
overrides continue to work as upper bounds on Linux.

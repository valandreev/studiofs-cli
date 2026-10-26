# Linux Mount Uses A Generation-Safe Adaptive Page Cache

Status: accepted. Supersedes
[ADR 0002](0002-linux-mount-defaults-to-direct-io-and-self-tuning.md).

## Decision

Linux uses `adaptive` as its default FUSE cache policy. A read-only handle
participates in the kernel page cache when the file fits sampled available
memory. A first or changed content generation, and locally dirty content
(unflushed, pending upload, or an unmaterialized overlay) with no open writer,
open with `keep_cache=false`; only the same stable generation may reopen with
`keep_cache=true`. Write-capable, actively written, unknown, and
larger-than-memory files use direct I/O (see the SFS-901 amendment below). `direct` remains a documented
remount-required rollback, while `kernel` explicitly enables kernel caching
and writeback capability.

The generation identity contains content epoch, logical size, and an ordered
signature of the resolved slice graph, including chunk/range, source and slice
identity, order, and payload. Publishing otherwise unchanged local content
does not change the generation. Writer opens and inode invalidation retire the
snapshot synchronously.

Kernel-owned handles delegate readahead to Linux. Direct and memory-bypassed
handles keep the VFS adaptive prefetch path. Adaptive write-capable handles
remain direct so bounded asynchronous VFS writes and the local-first
`flush`/`fsync`/`release` contract are not mixed with kernel writeback.

## Coherence And Lifetime

Lookup references and open handles make an inode observable to the kernel.
Successful entry/create replies add lookup references, `forget(nlookup)`
removes them, open handles add their own liveness, and root is always live.
Linux queues and coalesces inode invalidations on a bounded dedicated worker;
data invalidation dominates attribute-only invalidation. The VFS generation is
retired before the notification is queued.

Open cache preparation and the libfuse open/create reply are serialized per
inode. If the kernel rejects the reply, StudioFS aborts the prepared cache
snapshot and retires the VFS handle before a later open can decide to reuse it.

## Operational Contract

`STUDIOFS_LINUX_FUSE_CACHE_POLICY` accepts `adaptive`/`auto`, `direct` and its
legacy aliases, or `kernel` and its legacy aliases. Unset and empty select
adaptive. Unknown values warn and select adaptive.

Adaptive sessions sample available memory in an owned background thread, at
most once per second, and open callbacks load an atomic snapshot. Samples
expire two seconds after measurement starts, leaving time for the next sample
to complete without a periodic expiry gap. Missing, failed or expired samples
conservatively select direct I/O, including for stable generations. Such opens
count in `linux_fuse_memory_bypasses`: a host without memory telemetry loses
page-cache throughput, but visibly. Teardown stops and joins the sampler even
when callback state must be retained after a shutdown timeout. Linux requests read/readahead geometry up to 4 MiB and records
the negotiated values in runtime stats.

## Consequences

Warm reopen reads can be served without additional VFS read bytes, while
unsafe transitions continue to bypass kernel caching. Correctness and release
gates must compare five identical runs of current-main/direct,
feature/direct, feature/adaptive, and the optional JuiceFS reference. Direct
must remain within the documented regression limits; adaptive must not regress
mandatory workloads and must improve at least one warm/reopen-read workload.

## Amendment (SFS-901): Unstable Read-Only Opens Use The Page Cache

Commit 576d40f9d sent every non-`StableGeneration` read-only open to direct
I/O, citing an open that arrives before the asynchronous inode invalidation
reaches the page cache. Together with the `LocalDirty` rule this made every
read right after a local write cost one FUSE round trip per `read()` (fio psync
4 KiB sequential reads at about 30k IOPS on the VFS contract fixture).
`FirstObservation`, `GenerationChanged`, and `LocalDirty` read-only opens now
use the kernel page cache with `keep_cache=false` and kernel readahead.
`StableGeneration` alone keeps `keep_cache=true`; `ActiveWriter`,
write-capable, unknown, non-regular, and memory-bypassed opens are unchanged.
Missing memory telemetry now selects direct I/O for these opens too.

The analysis below uses upstream `fs/fuse` at v5.14 (Rocky 9 base), v6.6
(WSL2), and v6.8 (Ubuntu 24.04), with libfuse 3.x defaults. Both upstream
layouts quoted in point 1 purge; the backported Rocky 9 kernel source was not
inspected separately.

1. **Open-time purge.** Without `FOPEN_KEEP_CACHE` the kernel calls
   `invalidate_inode_pages2(inode->i_mapping)` after a successful open reply:
   in `fuse_finish_open` on 5.14, in `fuse_open_common` on 6.6 and 6.8. The
   purge covers the whole inode mapping, regardless of `direct_io` or other
   open handles, and runs after StudioFS chose the decision under the per-inode
   open-reply serializer. Direct opens already carried no `keep_cache`, so
   576d40f9d did not change which opens purge; it only stopped the new handle
   from adding pages. Any change the VFS had applied before the decision is
   therefore never served from older pages to the new handle, without waiting
   for the asynchronous invalidation worker. The mounted test
   `mounted_local_dirty_reads_use_kernel_readahead_and_purge_on_reopen` shows a
   reopen purging pages that a still-open handle also reads.
2. **In-flight readahead.** Readahead inserts locked folios before it sends
   `FUSE_READ`. `invalidate_inode_pages2` locks each folio, so it waits for an
   in-flight read and then drops the folio, which is clean because adaptive
   never enables writeback caching. A folio inserted after the purge passed its
   index sends its `FUSE_READ` after the open reply, and the VFS serves it from
   current inode state: kernel-owned handles have no handle-local read window
   or RAM readahead (`ReadAheadOwner::Kernel` disables both). An older handle
   therefore cannot add pre-change bytes after the purge.
3. **Concurrent writer window.** A writer opened after a page-cache reader goes
   through a direct I/O handle. Local writes publish attribute refreshes, and
   data invalidation is deferred until the last write-capable handle is
   released. The reader can see pages older than a completed write until
   either a later open purges the mapping, `AUTO_INVAL_DATA` (libfuse 3
   default) purges on the next attribute refresh that sees a new size or
   mtime, or (6.6 and 6.8 only) `fuse_direct_io` invalidates the written range before a
   direct write. This is exactly the window a `StableGeneration`
   `keep_cache=true` reader opened before the writer already accepts, so the
   change does not widen it. Close-to-open coherence holds because the next
   open purges.
4. **`LocalDirty` to published.** `LocalDirty` clears the retained generation
   and never returns `Reuse`. Overlay promotion and upload completion do not
   change the bytes. The first open after the dirty phase is
   `FirstObservation` (or `GenerationChanged`), which purges again before any
   later open can be `StableGeneration`. Pages from the dirty phase are never
   reused through `keep_cache=true`.
5. **`ActiveWriter`.** While a write-capable handle is registered, including
   during deferred release, read-only opens remain direct with
   `ReadAheadIntent::Vfs`, as before.

No interleaving that the direct rule closed and `keep_cache=false` leaves open
was found, so the 576d40f9d rule is superseded. After this change, fio psync
4 KiB sequential reads right after a local 256 MiB write measured about
360k-420k IOPS on the same fixture (WSL2 6.6, release build).

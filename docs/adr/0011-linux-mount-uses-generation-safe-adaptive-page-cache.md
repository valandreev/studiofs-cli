# Linux Mount Uses A Generation-Safe Adaptive Page Cache

Status: accepted. Supersedes
[ADR 0002](0002-linux-mount-defaults-to-direct-io-and-self-tuning.md).

## Decision

Linux uses `adaptive` as its default FUSE cache policy. A read-only handle
participates in the kernel page cache when the file fits sampled available
memory and its content is either a stable generation, which reopens with
`keep_cache=true`, or locally dirty (unflushed, pending upload, or an
unmaterialized overlay) with no open writer, which opens with
`keep_cache=false`. A first or changed generation is read through VFS with
direct I/O until it is stable. Write-capable, actively written, unknown, and
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

## Amendment (SFS-901): Locally Dirty Reads Use The Page Cache

Every read-only open of a file with local dirty state, pending uploads, or an
unmaterialized overlay used direct I/O, so each `read()` right after a local
write was one FUSE round trip (fio psync 4 KiB sequential reads at about 30k
IOPS on the VFS contract fixture). Uploads trail writes, so this could last a
long time. `LocalDirty` read-only opens now use the kernel page cache with
`keep_cache=false` and kernel readahead. Missing memory telemetry and files
larger than available memory still select direct I/O and count as memory
bypasses. `StableGeneration` alone keeps `keep_cache=true`; `ActiveWriter`,
write-capable, unknown, and non-regular opens are unchanged.

`FirstObservation` and `GenerationChanged` deliberately stay direct. This is
the race 576d40f9d guards:

1. A peer changes the file (for example, grows it from 4 KiB to 1 MiB). VFS
   applies it, retires the generation, and queues a DATA invalidation on the
   asynchronous Linux worker.
2. Before the worker delivers it, and while the kernel's cached attributes are
   still within their TTL, a reader opens the file. VFS reports
   `GenerationChanged`.
3. With page-cache reads, the open reply without `FOPEN_KEEP_CACHE` purges the
   pages but does not refresh `i_size`. `fuse_cache_read_iter` refreshes
   attributes only when they have expired or been invalidated, and
   `filemap_read` stops at the cached 4 KiB size: a truncated, stale read.
4. With `direct_io`, `fuse_direct_io` reads are not clamped to the kernel's
   `i_size`, so the reader sees the full new content from VFS.

The analysis uses upstream `fs/fuse` at v5.14 (Rocky 9 base), v6.6 (WSL2), and
v6.8 (Ubuntu 24.04), with libfuse 3.x defaults. The backported Rocky 9 kernel
source was not inspected separately. For `LocalDirty`:

1. **Open-time purge.** Without `FOPEN_KEEP_CACHE` the kernel calls
   `invalidate_inode_pages2(inode->i_mapping)` after a successful open reply:
   in `fuse_finish_open` on 5.14, in `fuse_open_common` on 6.6 and 6.8. The
   purge covers the whole inode mapping, regardless of `direct_io` or other
   open handles, and runs after StudioFS chose the decision under the per-inode
   open-reply serializer. The mounted test
   `mounted_local_dirty_reads_use_kernel_readahead_and_purge_on_reopen` shows a
   reopen purging pages that a still-open handle also reads.
2. **Kernel size.** Locally dirty bytes reached VFS through this mount's
   write, truncate, or fallocate requests, and the kernel updates its own
   `i_size` for each of them. The stale-size race above therefore does not
   apply. Peer content cannot replace newer local state without ending the
   `LocalDirty` state, and the next open is then `GenerationChanged`, which is
   direct.
3. **In-flight readahead.** Readahead inserts locked folios before it sends
   `FUSE_READ`. `invalidate_inode_pages2` locks each folio, so it waits for an
   in-flight read and then drops the folio, which is clean because adaptive
   never enables writeback caching. A folio inserted after the purge passed its
   index sends its `FUSE_READ` after the open reply, and the VFS serves it from
   current inode state: kernel-owned handles have no handle-local read window
   or RAM readahead (`ReadAheadOwner::Kernel` disables both).
4. **Concurrent writer window.** A writer opened after a page-cache reader goes
   through a direct I/O handle. Local writes publish attribute refreshes, and
   data invalidation is deferred until the last write-capable handle is
   released. The reader can see pages older than a completed write until
   either a later open purges the mapping, `AUTO_INVAL_DATA` (libfuse 3
   default) purges on the next attribute refresh that sees a new size or
   mtime, or (6.6 and 6.8 only) `fuse_direct_io` invalidates the written range
   before a direct write. This is the window a `StableGeneration`
   `keep_cache=true` reader opened before the writer already accepts, so the
   change does not widen it. Close-to-open coherence holds because the next
   open purges.
5. **`LocalDirty` to published.** `LocalDirty` clears the retained generation
   and never returns `Reuse`. Overlay promotion and upload completion do not
   change the bytes. The first open after the dirty phase is
   `FirstObservation`, which is direct and purges again before any later open
   can be `StableGeneration`. Pages from the dirty phase are never reused
   through `keep_cache=true`.
6. **`ActiveWriter`.** While a write-capable handle is registered, including
   during deferred release, read-only opens remain direct with
   `ReadAheadIntent::Vfs`, as before.

After this change, fio psync 4 KiB sequential reads right after a local
256 MiB write measured about 360k-420k IOPS on the same fixture (WSL2 6.6,
release build).

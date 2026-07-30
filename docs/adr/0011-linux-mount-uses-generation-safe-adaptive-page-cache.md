# Linux Mount Uses A Generation-Safe Adaptive Page Cache

Status: accepted. Supersedes
[ADR 0002](0002-linux-mount-defaults-to-direct-io-and-self-tuning.md).

## Decision

Linux uses `adaptive` as its default FUSE cache policy. A clean read-only
handle participates in the kernel page cache when the file fits sampled
available memory. Its first or changed content generation opens with
`keep_cache=false`; the same stable generation may reopen with
`keep_cache=true`. Write-capable, locally dirty, actively written, unknown, and
larger-than-memory files use direct I/O. `direct` remains a documented
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

Available memory is sampled at most once per second. If sampling fails, a safe
clean read-only file may use the page cache but cannot use `keep_cache` until
its generation is observed as stable. Linux requests read/readahead geometry
up to 4 MiB and records the negotiated values in runtime stats.

## Consequences

Warm reopen reads can be served without additional VFS read bytes, while
unsafe transitions continue to bypass kernel caching. Correctness and release
gates must compare five identical runs of current-main/direct,
feature/direct, feature/adaptive, and the optional JuiceFS reference. Direct
must remain within the documented regression limits; adaptive must not regress
mandatory workloads and must improve at least one warm/reopen-read workload.

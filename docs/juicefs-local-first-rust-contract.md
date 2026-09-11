# StudioFS JuiceFS-Equivalent IO Contract

## Status

This document is the canonical mounted-IO contract for StudioFS.

The reference implementation for data-plane economics is the local JuiceFS
repository at `E:\dev\studiofs-project\juicefs`, commit
`668828b61b216ccd289a1e0e10515a41c5f4f4ee`.

StudioFS implements one production mount model:

- mounted reads and writes use a JuiceFS-equivalent chunk/slice writer,
  data-reader, and cached-store pipeline;
- PostgreSQL is used for bootstrap, ordered metadata replication, client
  discovery, allocator-namespace registration, and shared file-lock coordination;
- object storage is used for payload replication and cold reads of known
  published objects;
- foreground mounted IO does not wait for PostgreSQL replication or object
  storage upload, except where shared lock coordination explicitly requires
  PostgreSQL.

The goal is IO parity with JuiceFS writeback mode at the filesystem data-plane
level, while retaining StudioFS shared metadata replication and lock semantics.

The PostgreSQL lifecycle contract is defined separately in
[pg-sync-contract.md](./pg-sync-contract.md). In particular, PostgreSQL
catch-up is not a foreground data-plane prerequisite: local `redb` serving,
remote replay, and read/write authority have separate contracts.

## 1. Core Vocabulary

| Term | Meaning |
|---|---|
| `chunk` | Fixed logical file range used for lookup, overlap resolution, and slice grouping. A slice never crosses a chunk boundary. |
| `slice` | Chunk-relative record mapping a byte range to one immutable payload object or tombstone. |
| `payload object` | Immutable object containing file bytes. Any content change creates a new object and a new slice reference. |
| `file writer` | Per-open-file writer owner that tracks dirty chunks, accepted length, and flush ordering. |
| `chunk writer` | Per-file/per-chunk owner that orders slice writers and commits completed slices in creation order. |
| `slice writer` | Writable extent backed by cached-store pages until it is frozen and finished. |
| `cached store` | The single cache/object pipeline that owns write pages, staging files, upload backlog, cached reads, eviction, and upload helpers. |
| `writer state` | In-memory state that makes accepted writes immediately readable on the writing mount. |
| `local metadata view` | The local `redb` metadata state used by foreground mounted IO. |
| `published state` | A peer-visible file state whose payload objects are already durable in object storage and whose metadata event has been published and applied. |
| `replication event` | Ordered PostgreSQL event that lets peers converge their local metadata view. |
| `lock lease` | PostgreSQL-coordinated hard-lock state with grant, renew, expiry, release, and notification semantics. |

## 2. Non-Negotiable Data-Plane Shape

StudioFS foreground file data IO follows the JuiceFS writer/cache structure:

```text
write callback
  |
  v
file writer
  |
  v
chunk writer
  |
  v
slice writer pages
  |
  v
cached store staging
  |
  v
background object upload
```

Rules:

- `write` splits data by chunk boundary and writes into writable slice pages.
- `write` success means the bytes are visible to the writing mount immediately.
- `write` does not persist local metadata, publish metadata, upload to object
  storage, or sync cache files before returning.
- A slice may be frozen because it reached a full chunk, reached the configured
  block/stage threshold, aged past the flush delay, or was forced by
  `flush`/`fsync`/close.
- Frozen slices finish through the cached store. In writeback mode, finishing a
  slice stages bytes locally and queues upload.
- Slice metadata is committed in chunk-local creation order after the slice data
  is finished enough to satisfy the local contract.
- The cached store is the only foreground payload pipeline. Mounted writes must
  not create a second acceptance journal, a second handle materialization path,
  or a duplicate local payload authority.
- Backpressure is based on dirty writer/cache pressure and must be simple,
  bounded, and observable. It may slow writers to prevent memory runaway, but it
  must not turn ordinary writeback into synchronous upload or replication.

## 3. Foreground Write Contract

For ordinary mounted writes:

- The callback copies caller bytes into writer-owned page/cache buffers.
- The write updates writer-visible length and dirty slice state.
- Same-mount reads resolve the newest accepted bytes from writer state.
- The callback returns after local acceptance, not after local durable staging.
- Memory pressure may apply soft sleeps or hard backpressure when dirty bytes
  exceed configured limits.
- Metadata and payload publication remain asynchronous after local acceptance.

The write path must avoid:

- per-write local file sync;
- per-write PostgreSQL round trips;
- per-write object-storage upload;
- per-write metadata publication;
- extra copies beyond the caller-buffer-to-writer-page copy required for
  ownership;
- duplicate enqueueing of the same payload into independent local pipelines.

## 4. Flush, Fsync, And Close

`flush`, `fsync`, and last-handle close are local boundaries only. They do not
mean peer visibility.

| Operation | Contract |
|---|---|
| `flush` | Admit the latest accepted handle/inode state to the shared volatile local-dirty worker. It may return before a redb commit; the worker commits metadata within one second. It does not wait for peer publication. |
| `fsync` | The strict caller-requested local boundary. Durable cache mode must finish and sync the local cache/metadata state needed to recover the accepted data locally. Throughput-first Fast cache mode drains StudioFS state but does not force payload file fsync/dirsync. It does not wait for peer publication. |
| last-handle close / release | Admit remaining writer state to the same volatile worker. It does not wait for a redb commit or peer publication. |

Flush and final release share one bounded VFS queue: at most 256 metadata
operations and 4 MiB of metadata descriptors. Create admission stops at
shutdown; clean shutdown drains all previously admitted state before it enters
the metadata engine. `fsync` and clean shutdown execute an explicit redb
`Immediate` transaction after the drain, making every earlier
`Durability::None` commit for that database durable before success is reported.
Durability clocks, pending state, and the idle-progress worker belong to one
metadata-engine instance, so mounts cannot satisfy or delay each other's
barriers.
If a background redb batch fails, the VFS records a degraded local-metadata
error and stops new volatile metadata admissions. The next ordinary flush
reports that error; `fsync` and shutdown drain admitted work, execute their
strict local-redb barriers, and return the retained failure.

Crash behavior:

- A crash before a local durability boundary may lose accepted bytes that lived
  only in memory.
- In throughput-first Fast cache mode, a crash may also lose payload bytes that
  reached local payload files but were still only in the OS page cache.
- A crash after `fsync` must recover the locally durable view in Durable cache
  mode. In Fast cache mode, payload bytes still only in the OS page cache may be
  lost.
- A crash after ordinary `flush` or close may lose accepted state that has not
  yet crossed the one-second volatile metadata window.
- Volatile metadata group commit is always enabled. Explicit `fsync` and clean
  shutdown do not wait for the idle deadline: they drain and make redb durable.

## 5. Cached Store Contract

The cached store is the single owner of local payload IO economics.

Responsibilities:

- allocate and reuse writer pages;
- write finished blocks/slices to local staging/cache files;
- maintain upload backlog and pending-upload visibility;
- feed background upload workers from staged/cache files;
- serve cached reads before cold object-store fetches;
- enforce dirty-byte and cache-size budgets;
- expose status counters for dirty bytes, staged bytes, upload backlog, cache
  hit/miss, staging latency, upload latency, and foreground throttle events.

Writeback rules:

- staging a block locally is enough for local progress;
- upload may run immediately, after delay, or after a scanner notices staged
  backlog;
- object upload completion alone never changes peer-visible namespace state;
- staged blocks whose metadata is no longer referenced may become orphan
  payloads and are later removed by cleanup.

## 6. Metadata And PostgreSQL

Foreground mounted IO uses the local metadata view.

PostgreSQL responsibilities:

- initial bootstrap and checkpoint loading;
- ordered metadata replication events;
- peer invalidation and convergence;
- client discovery;
- paired inode and payload allocator-namespace registration;
- shared hard-lock coordination.

The lifecycle rules for those responsibilities are:

- local `redb` is serving-ready before remote catch-up is required;
- a fresh client uses checkpoint-first bootstrap, then bounded/paged/resumable
  replay toward one frozen startup target;
- a fresh client remains read-only until the target is reached and write
  authority is available;
- checkpoint freshness has an explicit bounded-lag policy;
- finite remote history produces checkpoint rebootstrap or a recoverable
  degraded read-only state, never a permanently unmountable local disk;
- `destroy --local` never requires PostgreSQL or S3.

Rules:

- Ordinary reads and writes must not ask PostgreSQL for foreground metadata.
- A PostgreSQL-backed local root durably registers paired inode and payload
  namespaces before it accepts mutations; foreground metadata and payload ID
  allocation then advances local counters without steady-state PostgreSQL round
  trips.
- If PostgreSQL is unavailable and a complete paired allocator state already
  exists, the mount may continue accepting local work. A missing or malformed
  state remains read-only until a later reopen can register a fresh pair;
  already known readable state remains readable. When PostgreSQL is available,
  bootstrap deletes the retired `next_inode` counter before minting that pair.
- Shared locks are the explicit foreground exception: lock grant/renew/release
  may talk to PostgreSQL.

Local slice metadata API:

- Finished slice writers pass file-coordinate data/hole ranges through
  `LocalSliceBatch`.
- Each `LocalSliceRecord` is chunk-local and includes inode, chunk index,
  chunk-relative offset, length, payload object id when present, object offset,
  and caller-side sequence.
- A single `LocalSliceRecord` must never cross a chunk boundary. File-coordinate
  helpers split wider ranges before commit.
- `LocalSliceBatch::into_chunk_slices()` is the public bridge into
  `flush_regular_file_overlay_slices_with_attr(...)`, preserving the rule that
  local attr updates and finished slice metadata are committed together for
  flush/fsync/close/truncate-style foreground boundaries.

## 7. Publication And Peer Visibility

StudioFS keeps JuiceFS-like writeback IO economics and StudioFS peer-visible
version safety.

Rules:

- Payload upload is not namespace publication.
- A peer-visible content version must reference only payload objects confirmed
  durable in object storage.
- Metadata publication happens after payload readiness for that version.
- Peers observe a version only after they apply the ordered PostgreSQL event
  into their own local metadata view.
- New files with payload are hidden from peers until their first complete
  published version exists.
- A new regular-file placeholder with no published payload is not peer-visible
  state. If the writer crashes or rolls back before publication, peers converge
  on absence, not on an empty file created as a placeholder.
- Updates to already published files keep the previous peer-visible version
  until the next complete version is published and applied.
- Metadata-only updates may publish through PostgreSQL without object upload.
- Rename and move publish as atomic metadata transitions.

## 8. Read Resolution

Foreground reads resolve data in this order:

1. active writer state for the current mount/handle;
2. locally committed slice metadata in `redb`;
3. cached-store local payload/cache;
4. object-store cold fetch for known published payload objects;
5. zero-fill for holes.

Read-path buffering contract:

- Active writer-state bytes are copied directly into the caller-provided read
  buffer before the rest of the `DataReader` fallback plan is assembled.
- The writer-stack claim list records only byte ranges that were satisfied from
  active writer state. Those ranges are subtracted from local-slice, cache,
  object-store, and hole resolution so last-writer-wins ordering is preserved.
- `snapshot_read_spans` remains a snapshot/testing API, but the mounted
  foreground read path must not allocate a fresh payload `Vec` for each active
  writer-stack overlap.

Read rules:

- Same-mount reads observe accepted writes immediately.
- Reads never consult PostgreSQL directly.
- The selected slice view uses per-byte latest-write precedence.
- Readahead and prefetch operate on the effective slice view.
- Cached reads should avoid extra bridge-level copies where the platform API
  permits direct filling of caller buffers.

## 9. Open Handles, Rename, And Delete

Open handles use snapshot-style behavior:

- An already opened handle keeps operating on its opened inode/version binding
  until close.
- New opens resolve through the current local metadata view.
- Rename/move updates the local namespace atomically.
- Delete removes the name locally immediately.
- An open-unlinked file remains readable/writable through its handle until the
  last handle closes.
- Cleanup may remove orphaned local payload after no handle can observe it.

Peer convergence follows publication/apply order and never exposes partial
rename, partial delete, or half-published content.

## 10. Shared File Locks

Global hard locks are coordinated through PostgreSQL leases.

Rules:

- Lock grant, renew, release, expiry, and notification state live in
  PostgreSQL.
- A healthy holder renews before expiry.
- Expired leases stop blocking peers after refresh/apply.
- Known foreign exclusive locks block new conflicting write, truncate, rename,
  move, and delete attempts.
- Already active conflicting writers may finish according to the bridge/open
  handle contract.
- Peers may open read-only while another client holds an exclusive write lock.

## 11. Runtime Observability

The mounted runtime must expose enough counters to compare against JuiceFS
without screenshots:

- write callback latency and bytes;
- writer dirty bytes, dirty slice count, frozen slice count;
- cached-store page bytes and staging bytes;
- local staging throughput and latency;
- upload backlog count/bytes and upload throughput;
- foreground throttle/backpressure events;
- read throughput, cache-hit/miss, cold-fetch latency;
- process RSS and CPU where the platform can report them;
- flush/fsync/close local barrier latency.

Benchmark reports must identify:

- mounted path and runtime/cache root;
- block/chunk size settings;
- local cache disk;
- metadata disk;
- workload shape;
- upload activity during each read phase;
- whether post-upload idle was actually observed.

## 12. Implementation Guardrails

Use Rust-native ownership and error handling, but keep the IO shape equivalent
to the JuiceFS reference:

- prefer per-file/per-chunk/per-slice owners over broad shared mutable state;
- keep blocking local file IO out of async executor worker threads unless it is
  explicitly isolated;
- use bounded queues and explicit backpressure;
- keep error categories precise enough for retry, local failure, remote
  failure, and publication failure decisions;
- keep cache/payload lifetime transitions in one cached-store pipeline;
- reject designs that add a second foreground write acceptance pipeline;
- reject designs that make writeback wait for upload or replication in the
  ordinary write callback.

## 13. Validation Gates

A data-plane change is complete only when it is validated against both contract
and reference-performance surfaces:

- same-mount write/read correctness for sparse, overlapping, append, truncate,
  rename, and open-unlink cases;
- crash/restart recovery around `fsync` and close boundaries;
- local-cache exhaustion behavior;
- peer-visible publish ordering with PostgreSQL and object storage;
- mounted Windows and FUSE callback smoke tests;
- mounted sequential write/read throughput against the local JuiceFS reference;
- mounted cache-warm and cache-cold read behavior;
- mounted memory and CPU budget comparison.

For throughput work, use the same mounted workload shape on StudioFS and
JuiceFS. StudioFS should be treated as failing the data-plane parity gate when
its mounted sequential write/read throughput or peak memory differs materially
from the comparable JuiceFS run without an explicit documented reason.

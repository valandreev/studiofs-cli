# VFS Layer

The VFS (`studiofs-vfs`) is the platform-agnostic core of StudioFS. It orchestrates metadata operations, data block management, caching, background tasks, and exposes a file-system–like API consumed by the FUSE and WinFSP bridges.

This document uses the frozen chunk/slice/block vocabulary for the active data
path. Where upload-orchestration helpers still expose historical names, those
are called out explicitly.


- `flush()`/`fsync()` and the local `WriteBuffer -> local block backend -> sync_data() -> metadata`
  commit path guarantee immediate read-after-write for the same mounted client.
- remote readers see only coarse `upload_state` (`uploading`, `updating`, `ready`).
- remote visibility is file-version atomic via incarnation publish.

## Core Struct: `StudioVfs`

Holds all runtime state:

| Field | Type | Purpose |
|-------|------|---------|
| `meta_async` | `Arc<dyn AsyncMetaEngine>` | Async metadata boundary used by `run_meta(...)` |
| `data_write` | `Arc<DataStore>` | S3 write/delete path (write-optimized profile) |
| `block_store` | `Arc<dyn LocalBlockBackend>` | Cache 3.0 local data store for committed extents and upload rereads |
| `local_block_backend()` | `&dyn LocalBlockBackend` | Backend boundary used by new data-path code; hides backend-specific internals |
| `block_cache` | `Arc<BlockCache>` | Narrow upload-orchestration layer for pending-upload pinning |
| `fetch_scheduler` | `Arc<FetchScheduler>` | Adaptive S3 fetcher (read path) |
| `readahead` | `ReadAheadTracker` | Sequential prefetch |
| `state` | `VfsState` | Write buffers, inode overlays, file sessions, and backpressure |

### Lifecycle

`StudioVfs` startup is now explicit and two-phase:

1. `StudioVfs::new(...)` builds state only (no background workers are spawned).
2. `vfs.start()` starts background workers (uploader, prefetch, GC, janitor, checkpoint loop, etc.).

Callers must invoke `start()` exactly once after successful construction and before serving IO.
Repeated `start()` calls are safe and treated as idempotent once the instance is already running.

Notes:
- Sync metadata access still exists via `sync_meta()` for explicitly blocking-safe contexts.
- Async call paths should use `run_meta(self.meta_async.clone(), ...)` so blocking work stays behind one boundary.

### ACL Enforcement

VFS enforces effective ACL roles per principal:
- `no_access` hides entries from `lookup`/`readdir`
- `ro` allows reads and metadata reads, denies mutating operations
- `rw` allows full access

Rules are inherited along the parent chain and resolved by first match from the
inode toward root; no match defaults to `rw`.
For each inode on that walk, principal resolution is:
- explicit rule for current principal (for example `alice`)
- fallback wildcard rule `*`

To avoid hot-path overhead during deep directory listings, VFS caches effective
ACLs in RAM on inode entries and invalidates this cache using an ACL epoch when
rules change and a dedicated ACL namespace epoch for move/rename topology updates.
The cache is bounded by `STUDIOFS_EFFECTIVE_ACL_CACHE_CAPACITY` and resets when
the capacity limit is reached.

### VfsConfig

| Field | Default | Description |
|-------|---------|-------------|
| `meta_path` | — | Path to redb database |
| `cache_dir` | — | Root for the local block backend plus compatibility cache/ingress artifacts |
| `cache_size` | 100 GiB | Local block backend size budget |
| `block_size` | 16 MiB | Chunk size (from format info); cloud transfer uses 4x4 MiB blocks |
| `max_concurrent_uploads` | 4 | Legacy static upload knob (superseded by adaptive uploader) |
| `readahead_width` | 128 | Base prefetch depth (dynamic readahead can ramp above this for high-velocity sequential reads) |
| `max_dirty_bytes` | 12 GiB | Local `WriteBuffer` memory limit before backpressure |
| `upload_stats_interval_secs` | 1 | Upload pipeline/adaptive-control tick interval |
| `upload_fg_read_latency_target_ms` | 35 | Foreground-read P90 latency target for upload throttling |
| `upload_fg_read_latency_window_ms` | 1500 | Foreground-read speedometer sliding window |
| `upload_fg_read_latency_sticky_windows` | 2 | Keep last observed P90 for this many windows when samples pause |

Local data-path QoS:
- foreground user reads/writes use the foreground local-data semaphore
- upload worker reads and cache eviction use the background local-data semaphore
- cache-only delete/rename cleanup keeps the public VFS API synchronous, but the internal block eviction path is batched and routed through the background priority pool

Runtime principal selection:
- `STUDIOFS_PRINCIPAL` (default `*`) sets the principal used for ACL checks on
  the mounted client.
- `studiofs mount --principal <name>` overrides `STUDIOFS_PRINCIPAL` for that mount.

Adaptive upload throttling overrides:
- `STUDIOFS_UPLOAD_STATS_INTERVAL_SECS` controls control-loop tick cadence.
- `STUDIOFS_UPLOAD_FG_READ_LATENCY_TARGET_MS` sets the foreground P90 target.
- `STUDIOFS_UPLOAD_FG_READ_LATENCY_WINDOW_MS` sets the speedometer sampling window.
- `STUDIOFS_UPLOAD_FG_READ_LATENCY_STICKY_WINDOWS` controls how long last P90 is retained.
- upload worker foreground cache cost is derived internally from cache budget and current congestion instead of being user-configured separately.
- transit-memory budget for in-flight fetch/upload buffers is derived internally from cache budget.

## Write Path

### 1. `write(ino, offset, data)`

```
data arrives
  │
  ├─ Split into chunk-aligned ranges
  ├─ Store/merge in the per-file `WriteBuffer`
  ├─ Update local inode size and pending incarnation overlay
  └─ If enough full aligned chunks accumulate, VFS may eagerly persist the
    coalesced batch through the local block backend -> sync_data() -> metadata
```

Handle-aware write path:
- `write_fh(fh, ino, offset, data)` buffers data identically to `write()`.
- `flush_fh()` runs the normal ordered flush for the inode and then surfaces any sticky async-upload error for that inode.
- Foreground `flush()` / `close()` keep local persistence and error reporting deterministic without waiting for remote upload or manifest publication.
- Full-block eager flush coalesces aligned blocks into a larger batch before paying the local durability boundary.
- Foreground backpressure is driven by local `WriteBuffer` pressure; upload backlog alone does not stall writes.
- Asynchronous upload failures are tracked as sticky inode errors; `flush_fh` / `fsync` report `EIO` deterministically until the async pipeline recovers and clears the sticky state.
- `flush_fh`/`fsync` report sticky upload errors only after finishing the current local sync step, so newly written data is still persisted locally before `EIO` is returned.

### 2. `flush(ino)` / `fsync(ino)` / `close(ino)`

```
dirty `WriteBuffer` blocks for this inode
  │
  ├─ Take a point-in-time snapshot of dirty blocks
  ├─ Reserve/reuse per-block object IDs
  ├─ local backend `put(...)` committed payloads locally
  │   using bounded chunked pipelining (not strict 1-by-1 await)
  ├─ local backend `sync_data()`
  ├─ Persist extent metadata + attrs in local metadata
  ├─ Commit `WriteBuffer` state only for bytes that actually persisted
  └─ Enqueue uploads for the committed extents
```

`cache_safety_mode` is internal runtime policy and is no longer exposed through
the user-facing `studiofs settings` surface.

Local durability notes:
- Dirty bytes stay in `WriteBuffer` until the local backend -> sync_data() -> metadata sequence succeeds; failed persists do not drain RAM ownership.
- `write()` only pays that persist boundary after enough completed full-block data accumulates (`STUDIOFS_WRITE_EAGER_FLUSH_BATCH_BYTES`, default `32MB`) or when `flush()` / backpressure / shutdown forces the batch out.
- Reserved object IDs make retrying the same dirty block idempotent across metadata failures.
- `cache_safety_mode` no longer changes the foreground persistence path. Both modes use the same local durability flow.
- `fast` mode removes legacy ingress artifacts during startup self-heal; `durable` mode preserves checkpoint/state metadata for startup recovery compatibility.

### File Session Ordering Model

- Active regular-file transitions are owned by a per-inode file-session gate in VFS core.
- `read` operations use a shared session lock (parallel reads on the same inode are allowed).
- `write`, `flush`, `fsync`, and `release` use an exclusive session lock (state-changing barriers).
- Background stages (`local persist`, `upload`, `finalize`) publish explicit per-inode transition events through the same file-session state.
- Open-handle counts are mirrored into file-session state on `open_file` / `release_fh` so lifecycle state and operation ordering live in one authority.
- Session registry is stored as `HashMap<Ino, Weak<FileSessionEntry>>`; active operations/handles keep strong `Arc` ownership.
- Idle sessions are garbage-collected lazily by pruning dead `Weak` entries (no strong owners remain).
- Observability is exposed through `file_session_stats()`:
  - active sessions,
  - open handles,
  - queued operations,
  - max queue depth,
  - wait events / wait milliseconds,
  - transition counts,
  - wait reasons by operation class.
- Session observability counters are maintained incrementally via atomics, so
  `file_session_stats()` does not need a full-table scan of active sessions.

Legacy ingress-artifact note:
- The legacy artifact subsystem lives in [`crates/studiofs-vfs/src/ingress_wal.rs`].
- `cache_safety_mode=durable` preserves checkpoint/state metadata there for startup recovery compatibility.
- `cache_safety_mode=fast` self-heals by discarding those artifacts on startup.

### 3. Background Upload Worker

Consumes from upload queue channel:

```
Persisted extent queued
  │
  ├─ Acquire local-data I/O permits only for the local reread phase
  ├─ Reread the committed payload from `get_for_upload()` on the local backend as the primary source
  ├─ Run blocking local I/O on Tokio's blocking pool
  ├─ Reject stale op if inode/incarnation no longer match
  ├─ Split each chunk into 4 cloud blocks (up to 4 MiB each)
  ├─ Network phase: upload blocks to S3 (no local-data I/O slot held)
  ├─ Build block `Buffer`s (header + raw payload chunk), compute xxh64 checksums
  ├─ PUT block objects to S3 in parallel via UploadScheduler (AIMD + retry/timeout policy)
  ├─ Mark extent record as uploaded only after upload success
  ├─ Release local cache bookkeeping / eviction gating
  ├─ post-upload local cleanup (`mark_evictable` / deferred remove) runs via
  │  blocking-pool wrappers so the async uploader never performs direct local
  │  data-store I/O on executor threads
  │
  └─ On failure: requeue with backoff
```

Upload worker responsibility is intentionally narrow:
- it is the data plane for block payloads only;
- local durability has already been established before the upload is queued;
- it does not publish file manifests or decide when an inode incarnation becomes
  remote-visible;
- once an extent upload succeeds, later file-version publication is handled by a
  separate epoch finalizer.

### 4. Background Epoch Finalizer

Runs independently from upload workers and is responsible for control-plane
publication of file versions:

```
queued (ino, content_epoch)
  │
  ├─ Skip/requeue if the inode still has dirty `WriteBuffer` state
  ├─ Read the current block list for that epoch from metadata
  ├─ Wait until every referenced object is remote-visible
  ├─ Publish the manifest only if metadata still matches the verified block list
  │  (`publish_file_manifest_if_matches`)
  └─ On transient mismatch / visibility delay:
       requeue with backoff
```

Why this split exists:
- `flush()` / `close()` stay focused on accepting data locally and making it
  crash-recoverable;
- buffered foreground I/O does not force the last partial block on a file to
  synchronously publish the whole incarnation;
- manifest publication no longer depends on a "special" last upload op, which
  avoids the classic "1 pending block per file" tail behavior.

Consistency rules:
- finalization is blocked while any dirty local `WriteBuffer` overlay remains for the inode;
- remote visibility is checked before publication;
- publication is conditional on the verified manifest still matching metadata,
  so a newer local persist pass cannot race an older verified block list into a stale manifest.

Deleted-inode cancellation behavior:
- VFS tracks unlinked inodes in an in-memory `deleted_inodes` set and cancels
  queued/in-flight uploads via `CancellationToken`.
- `process_upload` short-circuits on `deleted_inodes` before metadata lookups to
  avoid metadata-backlog spikes on mass delete.
- For `open-unlink` (inode removed from namespace but still opened by a process),
  upload cancel is soft: pending upload queue entries are canceled, but block
  data is kept locally readable until the last handle closes.
- Marker cleanup is conservative: a deleted inode marker is cleared only after
  no open handles, no pending uploads, and inode metadata is gone.

Adaptive upload control:
- workers use cooperative scheduler yielding (`tokio::task::yield_now()`) periodically to avoid starving foreground tasks.
- upload backpressure uses a foreground-read P90 speedometer on each control tick.
- foreground-read latency pressure increases local upload I/O slot cost (AIMD-like) and adds a decaying scheduler sleep penalty.
- adaptive sleep is applied in `process_upload` before local-data I/O permit acquisition, so disk quiet windows happen before local backend reads.
- when `upload_delay_ms` already slept for the current op, the pre-read adaptive sleep uses the remaining budget only:
  `effective_penalty_ms = max(adaptive_penalty_ms - waited_upload_delay_ms, 0)`.
- upload retries inside `UploadScheduler` keep retry/backoff behavior unchanged; adaptive pre-read throttling is owned by VFS worker flow.
- upload workers read from persisted local blocks (`get_for_upload()` on the local backend) under upload local-I/O slot gating; transient queue-side staging is not the correctness path.
- upload rereads emit a best-effort `DONT_NEED` advisory after successful local reread; on Linux this hint is issued for the full block span from block-aligned offset.
- upload rereads intentionally do not update LRU recency, so upload churn does not pollute foreground read hotness.
- recovered dirty uploads replayed from local backend lifecycle state use the same persisted-block reread flow after restart.
- local upload I/O slots are intentionally scoped to the local read only; they are never held during S3 waits/retries.
- QoS acquisition failures are surfaced as a dedicated VFS error variant (`QosFailure`) that includes stage and object id context.
- shutdown path suppresses noisy requeue/warn loops for QoS failures triggered by uploader cancellation.

Local backend pressure maintenance:
- background maintenance applies high/low watermarks (90% / 80%) and can reclaim in bounded compaction batches for large reclaim targets.
- emergency maintenance protects local disk free space using both ratio target and absolute floor (`STUDIOFS_CACHE_EMERGENCY_DISK_FREE_FLOOR_BYTES`).

Remote-visible regular-file size advances only when a full incarnation is committed
through `committed_size`, not through the local foreground `size`.

## Read Path

### `read(ino, offset, size)`

```
local metadata view ──┬─ dirty `WriteBuffer`
                      └─ committed local extents / `committed_file_blocks`
         │
         ▼
  resolve block data ─── ┌─ `WriteBuffer`
  (first match wins)     ├─ committed local extents in the local block backend
                         ├─ compatibility cache shim
  └─ FetchScheduler → S3 block fetch (parallel, per-block retry)
         │
         ▼
  enqueue prefetch hint ── lightweight `(ino, first..last)` send
         │
         ▼
  background prefetch worker
         │
         ├─ `record_read()` + `DirAccessTracker::on_child_read()`
         ├─ budget + fetch headroom checks
         ├─ dedupe/merge pending work
         └─ spawn `read_block_for_prefetch()` tasks
         ▼
  assemble response bytes
```

**Two-stage local read semantics**

StudioFS now uses a two-level local-data availability model for foreground reads:

- Stage 1 — `WriteBuffer`: dirty bytes that have not yet been committed locally,
  including newer writes that may have arrived after a point-in-time persist snapshot was taken.
- Stage 2 — committed local storage state: extents already written to the local block backend
  and referenced by local metadata / pending incarnation state.

Notes:
- Persist snapshots are intentionally point-in-time only. If a block changes again
  before that snapshot commits, the newer bytes stay dirty in `WriteBuffer` and a
  later flush persists them.
- Upload progress and remote publication do not affect local read-after-write for
  the mounted client.
- Foreground reads request high-priority fetches, while background prefetch keeps
  using best-effort low-priority warming.
- Synchronous cache-only probes may still fall back to the async read path on miss.


Handle-aware read path:
- `read_into_fh(fh, ino, ...)` uses the same buffered-first read pipeline for every handle.
- Each open handle maintains its own access classifier in core VFS, so one random reader no longer suppresses sequential-media prefetch on a different handle for the same inode.
- Handle-less `read()` calls fall back to per-inode classifier state, which preserves the buffered path without requiring bridge-specific policy code.
- Best-effort prefetch and the normal local-data lookup path stay enabled for committed blocks regardless of platform or caller open style.
- VFS also provides a synchronous cache-only fast path (`read_into_fh_sync_cached`):
  - probes all requested blocks first,
  - serves synchronously only when the full request can be satisfied locally,
  - reads local hits directly into caller-provided output slices,
  - promotes local hits back into the read hotset only for access classes that opt into read promotion,
  - returns fallback (`None`) on miss so S3/object-store fetch remains async.
- WinFSP bridge integration uses caller-provided callback buffers directly for both `read` and `write` requests, avoiding bridge-layer intermediate allocations/copies on the hot path.
- FUSE bridge uses `can_read_fh_sync_cached(...)` as a cheap probe to avoid
  output-buffer allocation on sync-cache misses.

## FetchScheduler (`fetch.rs`)

Adaptive S3 block fetcher with deduplication and congestion control.

### Design

| Component | Purpose |
|-----------|---------|
| `AimdController` | TCP-cwnd–style concurrency limiter |
| `DashMap<u64, broadcast::Sender>` | Singleflight dedup |
| `DataStore` | S3 connection (read-optimized pool) |
| local block backend | Populate on fetch |

### Fetch Flow

```
fetch_block(object_id)
  │
  ├─ Fast path: check local data store + upload-orchestration layer → hit? return
  │
  ├─ Singleflight check: join existing in-flight fetch? → wait + return
  │
  ├─ Register singleflight entry
  ├─ Acquire AIMD permit (may block if at capacity)
  │
  ├─ S3 GET with configurable timeout (default 45s)
  ├─ Decode object from `opendal::Buffer` + verify checksum incrementally
  ├─ Return zero-copy payload when a single payload chunk is available
  ├─ Populate local data store + upload-orchestration layer
  ├─ Resolve AIMD permit (success/failure)
  │
  ├─ Broadcast result to singleflight waiters
  └─ On failure: retry up to 8 times with exponential backoff (50ms–2s by default)
```

Prefetch reads use `fetch_block_prefetch()`:
- shares the same singleflight map and cache population path
- keeps reserved AIMD headroom for foreground reads
- waits asynchronously on AIMD notify events when no permit is available (no CPU spin loop)
- bounds permit wait by `STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS`, then skips prefetch
- uses shorter retry policy (best-effort behavior)

### Fetch Metrics

| Metric | Meaning |
|--------|---------|
| `cache_hits` | Served from the local data store / upload-orchestration layer |
| `s3_fetches` | Downloads from S3 |
| `s3_errors` | Failed S3 requests |
| `singleflight_joins` | Deduplicated requests |
| `retries` | Retried fetches |
| `bytes_downloaded` | Total bytes from S3 |

Additional sync-fast-path counters are emitted in `upload pipeline counters`:
- `vfs_read_sync_fast_path_hits`
- `vfs_read_sync_fast_path_misses`

### Connection Warmup

On startup, `warmup(timeout)` primes the HTTP connection pool by issuing 4 parallel lightweight S3 requests.
Warmup runs in a background task scheduled by `StudioVfs::start()` and does not block `StudioVfs::new`.

## ReadAhead (`readahead.rs`)

Per-stream sequential read detection with adaptive prefetch depth.

The buffered read path classifies active streams into:
- `sequential_media_read`
- `random_interactive_read`
- `sequential_writer`
- `random_writer`

These classes stay in core VFS and drive:
- readahead width and minimum prefetch depth
- whether read hints are eligible for best-effort prefetch
- whether synchronous local hits are promoted back into the read hotset
- how aggressively foreground-read telemetry boosts upload backpressure

### Algorithm

- **Score** per stream: 0–255, starts at 0
- Sequential read: score increases (default `+4` per sequential block span)
- Random read: score -2
- Dynamic width:
  - baseline width = `STUDIOFS_READAHEAD_WIDTH` (default `128`)
  - max width is derived from read buffer budget (`STUDIOFS_READ_BUFFER_MB`) and block size
  - sustained sequential reads ramp width up by doubling; random seeks reset width to baseline
- Stream keys are `(ino, stream_id)` for handle-aware reads; in-flight prefetch dedup remains per `(ino, block)` so multiple handles still collapse duplicate background work.
- Sequential velocity is tracked via EWMA (blocks/sec) over microsecond sampling windows:
  - sub-millisecond reads are accumulated into a sample window (anti-dither),
  - stale velocity state is reset after long idle gaps.
- Ramp-up is bounded per call (currently up to 2 doublings per `record_read`), so one large read span can advance faster than 1-block reads without runaway over-prefetch.
- Prefetch depth = `min(score, current_dynamic_width)`, with a minimum floor on sequential streams (default 16 blocks)
- Planned prefetch blocks are clamped to file EOF to avoid over-prefetch on small files.

Runtime tuning knobs:
- `STUDIOFS_READ_BUFFER_MB` (default `2048`) - high-level read buffer budget; drives fetch AIMD max window
- `STUDIOFS_READAHEAD_WIDTH` (default `128` blocks; baseline dynamic width)
- `STUDIOFS_READAHEAD_MIN_PREFETCH` (default `16` blocks)
- `STUDIOFS_READAHEAD_SEQ_STEP` (default `4`)
- `STUDIOFS_READAHEAD_RANDOM_PENALTY` (default `2`)
- `STUDIOFS_PREFETCH_CONCURRENCY` (default `128`)
- `STUDIOFS_PREFETCH_FOREGROUND_RESERVE` (default `16`, keeps fetch headroom for direct reads)
- `STUDIOFS_PREFETCH_MAX_PER_CYCLE` (default `256`, effective cap also scales with AIMD window)
- `STUDIOFS_PREFETCH_PAUSE_ON_NETERR_MS` (default `250`, temporarily pauses prefetch after transport send errors)
- `STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS` (default `4`, prefetch retries are best-effort and shorter than foreground)
- `STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS` (default `75`)

On each buffered `read()`:
1. Hot path updates the handle or fallback classifier, then enqueues a compact prefetch hint (`ino`, `stream_id`, `first_block`, `last_block`, `policy`) and returns to user work.
2. Background worker updates read-ahead state (`record_read_for_stream`) and directory access state (`on_child_read`) regardless of transient prefetch budget limits.
3. Worker applies prefetch budget/headroom limits, deduplicates pending/in-flight blocks, and runs `read_block_for_prefetch()` tasks.
4. `start_prefetch` / `finish_prefetch` continue to guard in-flight deduplication.

Worker robustness details:
- stale hints for deleted inodes are dropped before scheduling;
- hint ranges are merged/suppressed to reduce channel churn on fragmented client read patterns;
- worker loop is panic-guarded and logs failures instead of silently disappearing.

### DirAccessTracker

Similar to ReadAheadTracker but for directory listing — detects sequential `readdir` access patterns.

Implementation details:
- Uses `DashMap` for per-directory state to reduce global lock contention under concurrent scans.
- Uses bounded tracking (LRU-style eviction) to prevent unbounded growth of directory/child reverse mappings.
- Reverse child→dir mappings are generation-stamped to avoid stale cleanup races when `on_readdir` refreshes a directory while `on_child_read` is running.

## AIMD Controller (`aimd.rs`)

TCP congestion-window–inspired adaptive concurrency limiter.

### Configuration

| Field | Fetch Default | Upload Default |
|-------|--------------|----------------|
| `initial_window` | derived from read buffer (`~16` for 2048 MiB / 16 MiB blocks) | 16 |
| `min_window` | 1 | 1 |
| `max_window` | derived from expanded readahead cap (`1024` for 2048 MiB / 16 MiB blocks) | 512 |
| `increase_step` | derived (`4.0` when max window >= 1024) | 2.0 |
| `decrease_factor` | 0.85 | 0.85 |
| `decrease_cooldown` | 3s | 3s |

### Behavior

- **Additive increase:** on success, window += `increase_step`
- **Multiplicative decrease:** on failure, window *= `decrease_factor` (subject to cooldown)
- **RAII permits:** `acquire()` blocks until window allows; permit resolved with `success()` or `failure()`

## Backpressure

When accepted local staging exceeds configured budget:

1. StudioFS first tries to persist full dirty `WriteBuffer` blocks into the local block backend
2. foreground `write()` / `flush()` can assist the local drain path when pressure remains
3. only exhaustion of the local `WriteBuffer` budget stalls foreground writers; upload backlog alone does not trigger backpressure

This prevents unbounded memory growth during sustained writes.

## Distributed File Locks

StudioFS supports distributed `flock`-style locks via S3:

- Lock file stored at `<volume>/metadata/locks/<ino>.lock`
- Conditional PUT with `If-None-Match: *` for exclusive lock acquisition
- Lock record contains: `client_id`, `lock_kind`, `timestamp`
- Heartbeat to renew locks, auto-expire stale locks

## Filename Validation

Besides generic filename validation, the VFS also applies a small platform
compatibility filter for macOS metadata noise:
- `.DS_Store`
- AppleDouble sidecars matching `._*`
- Root-only system metadata names that remain ignored: `.Spotlight-V100`, `.fseventsd`
- `.Trashes` is intentionally allowed so Finder can use native Trash directories
  (`.Trashes` and `.Trash-<uid>`) as real on-disk directories.

For ignored names (`.Spotlight-V100`, `.fseventsd`) the following behavior applies:
- `create` / `open` / metadata writes in these paths succeed locally but do not create
  shared namespace state.
- `mkdir` is accepted as an ephemeral directory inode.
- nested activity under those paths is blackholed into ephemeral entries.
- no inode, xattr, cache, or object-store state is persisted for ignored entries.
This is intentionally different from ordinary files because
Finder expects these operations to appear successful even when the shared
filesystem should remain clean.
- Root `create` / `symlink` / `link` for those names is absorbed into
  ephemeral ignored entries instead of persisting metadata
- reads from ignored file children return zero-filled bytes sized to the
  synthetic file length
- ignored directories never appear in `readdir`
- persisted legacy root entries for those blocked names are filtered out of `lookup`
  and `readdir`

This keeps Spotlight, Trash metadata, and `fseventsd` churn from polluting the
shared namespace while still presenting macOS with a tolerant enough surface
for background services.

## Namespace Resolution

Path resolution is centralized in `NamespaceResolver` (`studiofs-vfs`):
- `lookup_component(parent, name)` for component-by-component lookup
- `resolve_path(path)` for full path walks
- `resolve_parent(path)` for parent/name extraction
- `path_for_inode(ino)` for reverse path rendering used by bridge notifications

The resolver is the single entrypoint for namespace semantics. It always routes
through VFS lookup/readdir/getattr behavior, so runtime rules (ignored entries,
blocked root metadata names, and local-only ignored subtrees) are identical for
both full-path and component-based resolution.

`lookup_component` uses a bounded in-memory dentry cache keyed by
`(parent_ino, name)`:
- positive entries: `(parent_ino, name) -> child_ino`
- negative entries: `(parent_ino, name) -> ENOENT`

Each cache entry also stores namespace validation tokens:
- `namespace_epoch` (global namespace epoch)
- `parent_generation` (per-directory generation for the parent inode)

On cache hit, tokens are validated against current runtime values. If tokens
don't match, the entry is lazily treated as stale and reloaded from metadata.

`resolve_path`/`resolve_parent` additionally use a bounded full-path shortcut
cache keyed by normalized path components + path style:
- positive entries: `(<style>, <normalized path>) -> child_ino`
- negative entries: `(<style>, <normalized path>) -> (parent_ino, missing_name)`

Full-path entries carry:
- `namespace_epoch`
- a compact list of `(parent_ino, parent_generation)` tokens for each parent hop

On hit, every recorded parent generation is revalidated against current VFS
generation state. Any mismatch lazily invalidates the shortcut and forces
component walk through `lookup_component`.

Namespace mutation updates are deterministic:
- on create/mkdir/symlink/link: insert positive entry
- on unlink/rmdir: insert negative entry
- on rename: source entry becomes negative, destination becomes positive
- deleted file blocks are removed from the local data store promptly; if a block is pinned by an in-flight upload, VFS cancels that inode's in-flight upload and finalizes cleanup via pending-deletion metadata + cache eviction path
- parent directory generation is bumped only for affected parent directories
- broad remote namespace apply events bump global namespace epoch

This removes repeated metadata lookups for hot path components while preserving
namespace correctness across local mutations and remote namespace apply events.
Non-namespace metadata mutations (for example `setattr`) do not bump namespace
tokens and therefore do not invalidate dentry/path namespace cache entries.

Path normalization is style-aware:
- `NamespacePathStyle::Posix`: `/`, `.`, `..`
- `NamespacePathStyle::Windows`: `\`, `.`, `..`, trailing `.`/space trimming

All file/directory names are validated:
- Non-empty, max 255 bytes
- No `/` character
- Not `.` or `..`
- No Windows-invalid characters: `<`, `>`, `:`, `"`, `|`, `?`, `*`, `\`, control chars 0x00–0x1F

## StatFs

`statfs()` returns:
- `blocks` — total space / block_size
- `bfree` / `bavail` — free space (based on `USED_SPACE` counter)
- `files` / `ffree` — inode counts
- `bsize` — block size
- `namelen` — 255

# VFS Layer

The VFS (`studiofs-vfs`) is the platform-agnostic core of StudioFS. It orchestrates metadata operations, data block management, caching, background tasks, and exposes a file-system–like API consumed by the FUSE and WinFSP bridges.


- `flush()`/`close()` and local WAL accept guarantee immediate read-after-write for
  the same mounted client.
- remote readers see only coarse `upload_state` (`uploading`, `updating`, `ready`).
- remote visibility is file-version atomic via incarnation publish.

## Core Struct: `StudioVfs`

Holds all runtime state:

| Field | Type | Purpose |
|-------|------|---------|
| `meta_async` | `Arc<dyn AsyncMetaEngine>` | Async metadata boundary used by `run_meta(...)` |
| `data_write` | `Arc<DataStore>` | S3 write/delete path (write-optimized profile) |
| `block_cache` | `Arc<BlockCache>` | L1 + L2 block cache |
| `fetch_scheduler` | `Arc<FetchScheduler>` | Adaptive S3 fetcher (read path) |
| `readahead` | `ReadAheadTracker` | Sequential prefetch |
| `state` | `VfsState` | Open files, dirty buffers, backpressure |

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
| `cache_dir` | — | Root for block cache + journals |
| `cache_size` | 100 GiB | L2 disk cache budget |
| `memory_limit` | 25% of total RAM | L1 memory cache budget when `--memory-limit` is omitted |
| `block_size` | 16 MiB | Logical block size (from format info); cloud transfer uses 4x4 MiB fragments |
| `max_concurrent_uploads` | 4 | Legacy static upload knob (superseded by adaptive uploader) |
| `readahead_width` | 128 | Base prefetch depth (dynamic readahead can ramp above this for high-velocity sequential reads) |
| `max_dirty_bytes` | 12 GiB | Dirty memory limit before backpressure |
| `l2_commit_workers` | 32 | Bounded L2 commit scheduler workers |
| `l2_commit_queue_capacity` | 4096 | Bounded L2 commit queue capacity |
| `l2_read_timeout_ms` | 5000 | Max wait for local L2 read before cache-miss fallback |
| `l2_write_timeout_ms` | 5000 | Max wait for local L2 write attempt |
| `upload_stats_interval_secs` | 1 | Upload pipeline/adaptive-control tick interval |
| `upload_fg_read_latency_target_ms` | 50 | Foreground-read P90 latency target for upload throttling |
| `upload_fg_read_latency_window_ms` | 3000 | Foreground-read speedometer sliding window |
| `upload_fg_read_latency_sticky_windows` | 2 | Keep last observed P90 for this many windows when samples pause |
| `upload_l2_lookup_spike_threshold_ms` | 10 | Legacy compatibility floor for `upload_fg_read_latency_target_ms` |

Mount profile note:
- `studiofs mount --l2-profile usb` overrides L2 commit/write/read tuning for external USB SSD stability (workers=4, queue=256, read/write timeouts=10s).

Local cache QoS:
- foreground user reads/writes use the foreground L2 semaphore
- upload worker reads and cache eviction use the background L2 semaphore
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
- `STUDIOFS_UPLOAD_L2_LOOKUP_SPIKE_THRESHOLD_MS` is kept for compatibility and acts as a floor for the new target.
- `STUDIOFS_TRANSIT_MEMORY_BUDGET` sets the global credit budget for in-flight fetch/upload transit buffers (default `80%` of memory limit).

## Write Path

### 1. `write(ino, offset, data)`

```
data arrives
  │
  ├─ Split into block-aligned chunks
  ├─ Store/merge in `pending_blocks`
  ├─ Update local inode size and pending incarnation overlay
  └─ Full blocks can be moved immediately into dirty staging
```

Handle-aware write path:
- `write_fh(fh, ino, offset, data)` buffers data identically to `write()`.
- `flush_fh()` runs the normal ordered flush for the inode and then surfaces any sticky async-upload error for that inode.
- Foreground `flush()` / `close()` keep local staging and error reporting deterministic without waiting for remote upload or manifest publication.
- In `cache_safety_mode=durable`, ingress WAL appends are forced with local `sync_data()`, so this confirmation is resilient to process crash and OS-page-cache loss scenarios.
- Asynchronous upload failures are tracked as sticky inode errors; `flush_fh` / `fsync` report `EIO` deterministically until the async pipeline recovers and clears the sticky state.
- `flush_fh`/`fsync` report sticky upload errors only after finishing the current local sync step, so newly written data is still persisted locally before `EIO` is returned.

### 2. `flush(ino)` / `close(ino)`

```
pending_blocks for this inode
  │
  ├─ `cache_safety_mode=durable`: append accepted records to ingress segments
  │   (durable local accept + startup replay)
  │   - payload is passed as `Bytes` directly into the blocking spill writer
  │     (no `to_vec()` deep-copy on the hot path)
  ├─ `cache_safety_mode=fast`: keep accepted payloads in RAM only and avoid
  │   duplicate ingress payload writes
  ├─ Update live ingress index / pending incarnation state
  └─ Notify background pipeline
```

`cache_safety_mode` is configurable via VFS config / `studiofs settings --cache-safety-mode`
or environment variable `STUDIOFS_CACHE_SAFETY_MODE=fast|durable`.

Crash semantics note:
- `fast` mode does not keep WAL-backed accepted payloads. After abrupt termination
  (`SIGKILL`/process crash), very recent accepted writes that were not yet
  materialized can be lost (typically the last materializer window).
- `durable` mode keeps WAL/checkpoint-backed restart recovery for accepted ingress.

### File Session Ordering Model

- Active regular-file transitions are owned by a per-inode file-session gate in VFS core.
- `read` operations use a shared session lock (parallel reads on the same inode are allowed).
- `write`, `flush`, `fsync`, and `release` use an exclusive session lock (state-changing barriers).
- Background stages (`materialize`, `upload`, `finalize`) publish explicit per-inode transition events through the same file-session state.
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

Ingress WAL internals:
- The WAL subsystem is implemented in [`crates/studiofs-vfs/src/ingress_wal.rs`].
- `vfs.rs` interacts with it through `IngressWal`, `IngressLiveIndex`, and `SpillEntry`
  instead of owning segment-format details.
- WAL metadata files (`ingress_spill/state.json`, `ingress_spill/checkpoints/latest.json`)
  are encoded as binary `postcard` payloads with magic prefixes (`IWS1` / `IWC1`).
- Startup recovery remains backward-compatible with legacy JSON metadata files.

### 3. Background Materializer (`drain_dirty_mem` lineage)

Runs every 10ms, processes continuously while data exists:

```
dirty_mem + ingress_spill
  │
  ├─ Take batch of up to 1024 blocks
  │    ├─ build `dirty_mem` and `ingress_live` snapshots for selected keys
  │    └─ avoid per-key mutex contention while collecting batch payloads
  │
  ├─ Phase 1: pending manifest state update + L1 stage
  │    ├─ allocate object ids
  │    ├─ cache.stage_l1(id, data)
  │    ├─ stage upload payload in bounded transient RAM window for uploader reuse
  │    └─ meta.flush_blocks_batch()   ← local pending refs only
  │
  ├─ Phase 2: L2 commit scheduling (bounded)
  │    └─ cache.commit_l2_batch(blocks) — consumes owned `Vec<(ObjectId, Bytes)>`, bounded queue + fixed worker pool
  │
  ├─ Phase 3: Upload enqueue for accepted incarnation extents
  │    └─ cache.submit_upload(..., content_epoch) for each
  │
  ├─ Decrement dirty_bytes
  ├─ Remove spill entries by `spill_seq` match against live index
  │    (avoids re-reading full spill payloads from disk for byte-compare)
  ├─ Notify dirty_drained_notify (wake backpressured writers)
  │
  └─ If batch was full, loop immediately
```

### 4. Upload Worker

Consumes from upload queue channel:

```
PendingUpload received
  │
  ├─ Stage lookup: try transient staged upload payload seeded by materializer
  │    └─ retains payload until success/cancel so upload retries can reuse RAM copy
  ├─ Fallback local I/O phase: on staging miss, acquire upload local-cache I/O slots,
  │  read block from cache (L1 or L2), release slots immediately
  ├─ Reject stale op if inode/incarnation no longer match
  ├─ Split logical block into 4 cloud fragments (up to 4 MiB each)
  ├─ Network phase: upload fragments to S3 (no local-cache I/O slot held)
  ├─ Build fragment `Buffer`s (header + raw payload chunk), compute xxh64 checksums
  ├─ PUT fragment objects to S3 in parallel via UploadScheduler (AIMD + retry/timeout policy)
  ├─ Mark extent record as uploaded only after upload success
  ├─ cache.complete_upload(pending) — unpin LRU entry
  │
  └─ On failure: requeue with backoff
```

Upload worker responsibility is intentionally narrow:
- it is the data plane for block payloads only;
- it does not publish file manifests or decide when an inode incarnation becomes
  remote-visible;
- once an extent upload succeeds, later file-version publication is handled by a
  separate epoch finalizer.

### 5. Background Epoch Finalizer

Runs independently from upload workers and is responsible for control-plane
publication of file versions:

```
queued (ino, content_epoch)
  │
  ├─ Skip/requeue if the inode still has local overlay state
  │  (`pending_blocks`, dirty memory, ingress/WAL data)
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
- finalization is blocked while any local overlay remains for the inode,
  including live ingress/WAL state;
- remote visibility is checked before publication;
- publication is conditional on the verified manifest still matching metadata,
  so a newer materialization pass cannot race an older verified block list into
  a stale manifest.

Deleted-inode cancellation behavior:
- VFS tracks unlinked inodes in an in-memory `deleted_inodes` set and cancels
  queued/in-flight uploads via `CancellationToken`.
- `process_upload` short-circuits on `deleted_inodes` before metadata lookups to
  avoid metadata-backlog spikes on mass delete.
- For `open-unlink` (inode removed from namespace but still opened by a process),
  upload cancel is soft: pending upload journal entries are canceled, but block
  data is kept locally readable until the last handle closes.
- Marker cleanup is conservative: a deleted inode marker is cleared only after
  no open handles, no pending uploads, and inode metadata is gone.

Adaptive upload control:
- workers use cooperative scheduler yielding (`tokio::task::yield_now()`) periodically to avoid starving foreground tasks.
- upload backpressure uses a foreground-read P90 speedometer on each control tick.
- foreground-read latency pressure increases local upload I/O slot cost (AIMD-like) and adds a decaying scheduler sleep penalty.
- staged upload payload budget is adaptive: it starts from the current upload dispatch window,
  grows on backlog/staging-miss pressure, and shrinks again when foreground demand subsides.
- adaptive sleep is applied in `process_upload` before local-cache I/O permit acquisition, so disk quiet windows happen before L2 reads.
- when `upload_delay_ms` already slept for the current op, the pre-read adaptive sleep uses the remaining budget only:
  `effective_penalty_ms = max(adaptive_penalty_ms - waited_upload_delay_ms, 0)`.
- upload retries inside `UploadScheduler` keep retry/backoff behavior unchanged; adaptive pre-read throttling is owned by VFS worker flow.
- recovered journal uploads and staging evictions still fall back to normal cache rereads, so L2 remains the correctness fallback rather than the hot path.
- local upload I/O slots are intentionally scoped to the local read only; they are never held during S3 waits/retries.
- QoS acquisition failures are surfaced as a dedicated VFS error variant (`QosFailure`) that includes stage and object id context.
- shutdown path suppresses noisy requeue/warn loops for QoS failures triggered by uploader cancellation.

Remote-visible regular-file size advances only when a full incarnation is committed
through `committed_size`, not through the local foreground `size`.

## Read Path

### `read(ino, offset, size)`

```
local metadata view ──┬─ pending incarnation extents
                        └─ fallback `committed_file_blocks`
         │
         ▼
  resolve block data ─── ┌─ pending_blocks
  (first match wins)     ├─ dirty_mem
                         ├─ open handle dirty state
                         ├─ pending incarnation extents
                         ├─ ingress_spill
                         ├─ L1 memory cache
                         ├─ L2 disk cache
                         └─ FetchScheduler → S3 fragment fetch (parallel, per-fragment retry)
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

**Completion-aware local read semantics (SFS-123)**

StudioFS uses a four-stage local-data availability model to guarantee
local-read correctness while background uploads and L2 commits complete.
The stages are:

- Stage 1 — `pending_blocks`: unflushed per-handle write buffer (in-memory).
- Stage 2 — `dirty_mem`: accepted in-RAM staging after `flush_ordered`.
- Stage 3 — `live_extent_records` + block cache: materialized L1/L2 entries
  created by the materializer (`stage_l1` + `record_live_extent`) and pinned
  until `complete_upload`.
- Stage 4 — ingress spill WAL: durable spill entries used in `durable` mode
  or when materialization has not yet occurred.

To cover the narrow window between `complete_upload` (L1 unpin) and the
corresponding L2/NVMe commit finishing, the async read path applies a bounded
retry when a live-extent exists but the block is absent from cache. The
default retry budget is 5 attempts × 10 ms = ~50 ms total. The following
observability counters were added to `VfsIngressStats`:

- `local_overlay_cache_miss_retries` — total retry attempts performed
- `local_overlay_cache_miss_retry_exhaustions` — number of times the retry
  budget was exhausted without locating the block in cache

Notes:
- Synchronous cache-only probes intentionally accept an immediate cache miss
  and fall back to the async read path; this preserves fast sync probes while
  the async path provides bounded recovery for transient L1→L2 handover.
- The retry budget is intentionally bounded and observable; operators can
  monitor the exhaustion counter to detect when the fixed (build-time)
  retry budget may be insufficient for their hardware profile.


Handle-aware read path:
- `read_into_fh(fh, ino, ...)` uses the same buffered-first read pipeline for every handle.
- Best-effort prefetch and the normal L1/L2 lookup path stay enabled for committed blocks regardless of platform or caller open style.
- VFS also provides a synchronous cache-only fast path (`read_into_fh_sync_cached`):
  - probes all requested blocks first,
  - serves synchronously only when the full request can be satisfied locally,
  - reads L2 hits directly into caller-provided output slices,
  - returns fallback (`None`) on miss so S3/object-store fetch remains async.
- WinFSP bridge integration uses caller-provided callback buffers directly for both `read` and `write` requests, avoiding bridge-layer intermediate allocations/copies on the hot path.
- FUSE bridge uses `can_read_fh_sync_cached(...)` as a cheap probe to avoid
  output-buffer allocation on sync-cache misses.

L2 sync descriptor cache budget:
- `STUDIOFS_L2_SYNC_FD_CACHE_CAPACITY` bounds the buffered sync-read open-file descriptor cache used by the local cache fast path.

## FetchScheduler (`fetch.rs`)

Adaptive S3 block fetcher with deduplication and congestion control.

### Design

| Component | Purpose |
|-----------|---------|
| `AimdController` | TCP-cwnd–style concurrency limiter |
| `DashMap<u64, broadcast::Sender>` | Singleflight dedup |
| `DataStore` | S3 connection (read-optimized pool) |
| `BlockCache` | Populate on fetch |

### Fetch Flow

```
fetch_block(object_id)
  │
  ├─ Fast path: check L1 + L2 cache → hit? return
  │
  ├─ Singleflight check: join existing in-flight fetch? → wait + return
  │
  ├─ Register singleflight entry
  ├─ Acquire AIMD permit (may block if at capacity)
  │
  ├─ S3 GET with configurable timeout (default 45s)
  ├─ Decode object from `opendal::Buffer` + verify checksum incrementally
  ├─ Return zero-copy payload when a single payload chunk is available
  ├─ Populate L1 + L2 cache
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
| `cache_hits` | Served from L1/L2 |
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

Per-inode sequential read detection with adaptive prefetch depth.

### Algorithm

- **Score** per inode: 0–255, starts at 0
- Sequential read: score increases (default `+4` per sequential block span)
- Random read: score -2
- Dynamic width:
  - baseline width = `STUDIOFS_READAHEAD_WIDTH` (default `128`)
  - max width is derived from read buffer budget (`STUDIOFS_READ_BUFFER_MB`) and block size
  - sustained sequential reads ramp width up by doubling; random seeks reset width to baseline
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
1. Hot path enqueues a compact prefetch hint (`ino`, `first_block`, `last_block`) and returns to user work.
2. Background worker updates read-ahead state (`record_read`) and directory access state (`on_child_read`) regardless of transient prefetch budget limits.
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

1. StudioFS prefers spilling to NVMe ingress (`ingress_spill`)
2. only after RAM + spill budget are exhausted do `write()` / `flush()` wait
3. background flusher drains staged blocks and wakes waiters

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
- deleted file blocks are removed from L1/L2 promptly; if a block is pinned by an in-flight upload, VFS cancels that inode's in-flight upload and finalizes cleanup via pending-deletion journal + cache eviction path
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

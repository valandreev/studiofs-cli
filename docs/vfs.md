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
| `meta` | `Arc<dyn MetaEngine>` | Metadata operations |
| `data_write` | `Arc<DataStore>` | S3 write/delete path (write-optimized profile) |
| `block_cache` | `Arc<BlockCache>` | L1 + L2 block cache |
| `fetch_scheduler` | `Arc<FetchScheduler>` | Adaptive S3 fetcher (read path) |
| `readahead` | `ReadAheadTracker` | Sequential prefetch |
| `state` | `VfsState` | Open files, dirty buffers, backpressure |

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
| `block_size` | 8 MiB | Block size (from format info) |
| `max_concurrent_uploads` | 4 | Legacy static upload knob (superseded by adaptive uploader) |
| `readahead_width` | 128 | Max prefetch depth |
| `max_dirty_bytes` | 12 GiB | Dirty memory limit before backpressure |
| `l2_commit_workers` | 32 | Bounded L2 commit scheduler workers |
| `l2_commit_queue_capacity` | 4096 | Bounded L2 commit queue capacity |
| `l2_read_timeout_ms` | 5000 | Max wait for local L2 read before cache-miss fallback |
| `l2_write_timeout_ms` | 5000 | Max wait for local L2 write attempt |

Mount profile note:
- `studiofs mount --l2-profile usb` overrides L2 commit/write/read tuning for external USB SSD stability (workers=4, queue=256, read/write timeouts=10s).

Runtime principal selection:
- `STUDIOFS_PRINCIPAL` (default `*`) sets the principal used for ACL checks on
  the mounted client.
- `studiofs mount --principal <name>` overrides `STUDIOFS_PRINCIPAL` for that mount.

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

### 2. `flush(ino)` / `close(ino)`

```
pending_blocks for this inode
  │
  ├─ `cache_safety_mode=durable`: append accepted records to ingress segments
  │   (durable local accept + startup replay)
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

### 3. Background Materializer (`drain_dirty_mem` lineage)

Runs every 10ms, processes continuously while data exists:

```
dirty_mem + ingress_spill
  │
  ├─ Take batch of up to 1024 blocks
  │
  ├─ Phase 1: pending manifest state update + L1 stage
  │    ├─ allocate object ids
  │    ├─ cache.stage_l1(id, data)
  │    └─ meta.flush_blocks_batch()   ← local pending refs only
  │
  ├─ Phase 2: L2 commit scheduling (bounded)
  │    └─ cache.commit_l2_batch(blocks) — bounded queue + fixed worker pool
  │
  ├─ Phase 3: Upload enqueue for accepted incarnation extents
  │    └─ cache.submit_upload(..., content_epoch) for each
  │
  ├─ Decrement dirty_bytes
  ├─ Notify dirty_drained_notify (wake backpressured writers)
  │
  └─ If batch was full, loop immediately
```

### 4. Upload Worker

Consumes from upload queue channel:

```
PendingUpload received
  │
  ├─ Read block from cache (L1 or L2)
  ├─ Reject stale op if inode/incarnation no longer match
  ├─ Build object (header + raw payload), compute xxh64 checksum
  ├─ PUT to S3 via UploadScheduler (AIMD + retry/timeout policy)
  ├─ Mark extent record as uploaded only after upload success
  ├─ cache.complete_upload(pending) — unpin LRU entry
  │
  └─ On failure: requeue with backoff
```

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
                         └─ FetchScheduler → S3
         │
         ▼
  ReadAheadTracker ────── detect sequential access
         │                prefetch next N blocks
         ▼
  assemble response bytes
```

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
  ├─ Decode object + verify checksum
  ├─ Populate L1 + L2 cache
  ├─ Resolve AIMD permit (success/failure)
  │
  ├─ Broadcast result to singleflight waiters
  └─ On failure: retry up to 8 times with exponential backoff (50ms–2s by default)
```

Prefetch reads use `fetch_block_prefetch()`:
- shares the same singleflight map and cache population path
- keeps reserved AIMD headroom for foreground reads
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

### Connection Warmup

On startup, `warmup(timeout)` primes the HTTP connection pool by issuing 4 parallel lightweight S3 requests.
Warmup runs in a background task and does not block `StudioVfs::new`.

## ReadAhead (`readahead.rs`)

Per-inode sequential read detection with adaptive prefetch depth.

### Algorithm

- **Score** per inode: 0–255, starts at 0
- Sequential read: score increases (default `+4` per sequential block span)
- Random read: score -2
- Prefetch depth = `min(score, configured_width)`, with a minimum floor on sequential streams (default 16 blocks)

Runtime tuning knobs:
- `STUDIOFS_READ_BUFFER_MB` (default `2048`) - high-level read buffer budget; drives fetch AIMD max window
- `STUDIOFS_READAHEAD_WIDTH` (default `128` blocks)
- `STUDIOFS_READAHEAD_MIN_PREFETCH` (default `16` blocks)
- `STUDIOFS_READAHEAD_SEQ_STEP` (default `4`)
- `STUDIOFS_READAHEAD_RANDOM_PENALTY` (default `2`)
- `STUDIOFS_PREFETCH_CONCURRENCY` (default `128`)
- `STUDIOFS_PREFETCH_FOREGROUND_RESERVE` (default `max(32, prefetch_concurrency / 2)`, keeps fetch headroom for direct reads)
- `STUDIOFS_PREFETCH_MAX_PER_CYCLE` (default `96`)
- `STUDIOFS_PREFETCH_PAUSE_ON_NETERR_MS` (default `250`, temporarily pauses prefetch after transport send errors)
- `STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS` (default `4`, prefetch retries are best-effort and shorter than foreground)
- `STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS` (default `75`)

On each `read()`:
1. `record_read(ino, first_block, last_block)` returns blocks to prefetch
2. Background tasks call `fetch_block()` for each prefetch block
3. `start_prefetch` / `finish_prefetch` track in-flight dedup

### DirAccessTracker

Similar to ReadAheadTracker but for directory listing — detects sequential `readdir` access patterns.

## AIMD Controller (`aimd.rs`)

TCP congestion-window–inspired adaptive concurrency limiter.

### Configuration

| Field | Fetch Default | Upload Default |
|-------|--------------|----------------|
| `initial_window` | derived from read buffer (`~32` for 2048 MiB / 8 MiB blocks) | 16 |
| `min_window` | 1 | 1 |
| `max_window` | derived from read buffer (`256` for 2048 MiB / 8 MiB blocks) | 512 |
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

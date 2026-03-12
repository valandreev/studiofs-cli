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

### VfsConfig

| Field | Default | Description |
|-------|---------|-------------|
| `meta_path` | — | Path to redb database |
| `cache_dir` | — | Root for block cache + journals |
| `cache_size` | 100 GiB | L2 disk cache budget |
| `memory_limit` | 2 GiB | L1 memory cache budget |
| `block_size` | 8 MiB | Block size (from format info) |
| `max_concurrent_uploads` | 4 | Legacy static upload knob (superseded by adaptive uploader) |
| `readahead_width` | 128 | Max prefetch depth |
| `max_dirty_bytes` | 12 GiB | Dirty memory limit before backpressure |

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
  ├─ Append accepted records to ingress segments (durable local accept)
  ├─ Update live ingress index / pending incarnation state
  └─ Notify background pipeline
```

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
  ├─ Phase 2: L2 commit (parallel)
  │    └─ cache.commit_l2_batch(blocks) — parallel NVMe writes
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
- Root-only system metadata names: `.Spotlight-V100`, `.Trashes`, `.fseventsd`

These names are accepted through the local client flow as ephemeral ignored
files, but they are never written into the metadata engine, cache pipeline, or
object storage. This is intentionally different from ordinary files because
Finder expects these operations to appear successful even when the shared
filesystem should remain clean.

For those root-only system metadata names, StudioFS uses slightly different
behavior from ignored files:
- root `mkdir` is accepted and returns an ephemeral directory inode
- root `create` / `symlink` / `link` for those names is also absorbed into
  ephemeral ignored entries instead of persisting metadata
- any nested `mkdir` / `create` / `symlink` activity under that ignored
  directory is blackholed into ephemeral entries
- reads from ignored file children return zero-filled bytes sized to the
  synthetic file length
- ignored directories never appear in `readdir`
- persisted legacy root entries with those names are also filtered out of
  `lookup` and `readdir`

This keeps Spotlight, Trash metadata, and `fseventsd` churn from polluting the
shared namespace while still presenting macOS with a tolerant enough surface
for background services.

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

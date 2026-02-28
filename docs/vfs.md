# VFS Layer

The VFS (`studiofs-vfs`) is the platform-agnostic core of StudioFS. It orchestrates metadata operations, data block management, caching, background tasks, and exposes a file-system–like API consumed by the FUSE and WinFSP bridges.

## Core Struct: `StudioVfs`

Holds all runtime state:

| Field | Type | Purpose |
|-------|------|---------|
| `meta` | `Arc<dyn MetaEngine>` | Metadata operations |
| `data` | `DataStore` | S3 read/write (dedicated pools) |
| `cache` | `Arc<BlockCache>` | L1 + L2 block cache |
| `fetch` | `Arc<FetchScheduler>` | Adaptive S3 fetcher |
| `readahead` | `ReadAheadTracker` | Sequential prefetch |
| `state` | `VfsState` | Open files, dirty buffers, backpressure |

### VfsConfig

| Field | Default | Description |
|-------|---------|-------------|
| `meta_path` | — | Path to redb database |
| `cache_dir` | — | Root for block cache + journals |
| `cache_size` | 50 GiB | L2 disk cache budget |
| `memory_limit` | 2 GiB | L1 memory cache budget |
| `block_size` | 1 MiB | Block size (from format info) |
| `max_concurrent_uploads` | 4 | Legacy static upload knob (superseded by adaptive uploader) |
| `readahead_width` | 512 | Max prefetch depth |
| `max_dirty_bytes` | 8 GiB | Dirty memory limit before backpressure |

## Write Path

### 1. `write(ino, offset, data)`

```
data arrives
  │
  ├─ Split into block-aligned chunks
  ├─ Allocate block IDs (batch: next_block_ids)
  ├─ Store in pending_blocks (per-inode HashMap)
  ├─ Increment dirty_bytes counter
  │
  ├─ If dirty_bytes > max_dirty_bytes:
  │    └─ Await dirty_drained_notify (backpressure)
  │
  └─ Update inode size if extending file
```

### 2. `flush(ino)` / `close(ino)`

```
pending_blocks for this inode
  │
  ├─ Move all blocks to dirty_mem global pool
  └─ Notify background flusher
```

### 3. Background Flusher (`drain_dirty_mem`)

Runs every 10ms, processes continuously while data exists:

```
dirty_mem pool
  │
  ├─ Take batch of up to 256 blocks
  │
  ├─ Phase 1: L1 stage + metadata
  │    ├─ cache.stage_l1(id, data) for each block
  │    └─ meta.flush_blocks_batch() — single redb transaction
  │
  ├─ Phase 2: L2 commit (parallel)
  │    └─ cache.commit_l2_batch(blocks) — parallel NVMe writes
  │
  ├─ Phase 3: Upload enqueue
  │    └─ cache.submit_upload(ino, offset, id) for each
  │
  ├─ Decrement dirty_bytes
  ├─ Notify dirty_drained_notify (wake backpressured writers)
  │
  └─ If batch was full (256), loop immediately
```

### 4. Upload Worker

Consumes from upload queue channel:

```
PendingUpload received
  │
  ├─ Read block from cache (L1 or L2)
  ├─ Build object (header + raw payload), compute xxh64 checksum
  ├─ PUT to S3 via UploadScheduler (AIMD + retry/timeout policy)
  ├─ cache.complete_upload(pending) — unpin LRU entry
  │
  └─ On failure: requeue with backoff
```

## Read Path

### `read(ino, offset, size)`

```
                         ┌─ pending_blocks (uncommitted)
  lookup BlockRef   ──── ├─ dirty_mem (staged)
  from metadata          └─ meta.get_file_block()
         │
         ▼
  resolve block data ─── ┌─ L1 memory cache
  (first match wins)     ├─ L2 disk cache
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
  ├─ S3 GET with configurable timeout (default 30s)
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

## ReadAhead (`readahead.rs`)

Per-inode sequential read detection with adaptive prefetch depth.

### Algorithm

- **Score** per inode: 0–255, starts at 0
- Sequential read: score increases (default `+4` per sequential block span)
- Random read: score -2
- Prefetch depth = `min(score, configured_width)`, with a minimum floor on sequential streams (default 16 blocks)

Runtime tuning knobs:
- `STUDIOFS_READ_BUFFER_MB` (default `2048`) - high-level read buffer budget; drives fetch AIMD max window
- `STUDIOFS_MAX_READAHEAD_MB` (default `512`) - high-level sequential readahead budget
- `STUDIOFS_PREFETCH` (default `2`) - high-level prefetch pressure knob
- `STUDIOFS_READAHEAD_WIDTH` (default derived from `STUDIOFS_MAX_READAHEAD_MB`, `512` blocks for 1 MiB block size)
- `STUDIOFS_READAHEAD_MIN_PREFETCH` (default derived from width, usually `16`)
- `STUDIOFS_READAHEAD_SEQ_STEP` (default `4`)
- `STUDIOFS_READAHEAD_RANDOM_PENALTY` (default `2`)
- `STUDIOFS_PREFETCH_CONCURRENCY` (default derived from `STUDIOFS_PREFETCH`, usually `64`)
- `STUDIOFS_PREFETCH_FOREGROUND_RESERVE` (default `max(32, prefetch_concurrency / 2)`, keeps fetch headroom for direct reads)
- `STUDIOFS_PREFETCH_MAX_PER_CYCLE` (default derived from concurrency/width, usually `128`)
- `STUDIOFS_PREFETCH_PAUSE_ON_NETERR_MS` (default `1500`, temporarily pauses prefetch after transport send errors)
- `STUDIOFS_FETCH_PREFETCH_RESERVE` (default equals foreground reserve; prefetch cannot consume these slots)
- `STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS` (default `2`, prefetch retries are intentionally short)
- `STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS` (default `25`)

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
| `initial_window` | derived from read buffer (`~256` for 2048 MiB / 1 MiB blocks) | 16 |
| `min_window` | 1 | 1 |
| `max_window` | derived from read buffer (`2048` for 2048 MiB / 1 MiB blocks) | 512 |
| `increase_step` | derived (`4.0` when max window >= 1024) | 2.0 |
| `decrease_factor` | 0.85 | 0.85 |
| `decrease_cooldown` | 3s | 3s |

### Behavior

- **Additive increase:** on success, window += `increase_step`
- **Multiplicative decrease:** on failure, window *= `decrease_factor` (subject to cooldown)
- **RAII permits:** `acquire()` blocks until window allows; permit resolved with `success()` or `failure()`

## Backpressure

When `dirty_bytes` exceeds `max_dirty_bytes` (8 GiB):

1. `write()` and `flush()` await `dirty_drained_notify`
2. Background flusher processes blocks and calls `notify_waiters()`
3. Writers unblock with near-zero latency (no polling)

This prevents unbounded memory growth during sustained writes.

## Distributed File Locks

StudioFS supports distributed `flock`-style locks via S3:

- Lock file stored at `<volume>/metadata/locks/<ino>.lock`
- Conditional PUT with `If-None-Match: *` for exclusive lock acquisition
- Lock record contains: `client_id`, `lock_kind`, `timestamp`
- Heartbeat to renew locks, auto-expire stale locks

## Filename Validation

All file/directory names are validated:
- Non-empty, max 255 bytes
- No `/` character
- Not `.` or `..`
- No Windows-invalid characters: `<`, `>`, `:`, `"`, `|`, `?`, `*`, control chars 0x00–0x1F

## StatFs

`statfs()` returns:
- `blocks` — total space / block_size
- `bfree` / `bavail` — free space (based on `USED_SPACE` counter)
- `files` / `ffree` — inode counts
- `bsize` — block size
- `namelen` — 255

# Cache System

The cache subsystem (`studiofs-cache`) implements block caching for high-throughput
NVMe-backed storage with crash-safe writeback to S3.

There are two generations of the cache implementation:

| Generation | Module | Status |
|------------|--------|--------|
| **v3 (current)** | `block_cache`, `l1_mem`, `l2_disk`, `lru_tracker` | Active — used by VFS |
| **v2 (in progress)** | `v2::block_store`, `v2::allocator`, `v2::format`, `v2::index`, `v2::lru` | Foreground VFS reads and read-ahead now use BlockStore; write/upload plumbing still transitions through legacy cache helpers |

---

## Cache v2 — Single-file BlockStore

> **Status:** Core BlockStore, allocator, intrusive LRU, in-memory BlockIndex,
> dirty/uploading eviction gating, auto-eviction, mmap reads with generation
> remapping, snapshot/WAL persistence, and startup recovery are implemented,
> and the VFS foreground read path now uses BlockStore as its committed cache.

Replaces the L1/L2 split with a single `blocks.data` file managed by a native
free-space allocator.  The OS page cache acts as L1; no explicit RAM tier.

### Cache directory layout

```
{cache_dir}/
├── blocks.data     # single data file (all blocks)
├── blocks.idx      # index snapshot — binary
├── blocks.wal      # index WAL — append-only since last snapshot
└── pins.json       # pin config — future
```

### `blocks.data` format

**Superblock** (first 4 096 bytes, offset 0):

| Offset | Size | Field        | Description                      |
|--------|------|--------------|----------------------------------|
| 0      | 8    | `magic`      | `b"STFSCA\x01\x00"`              |
| 8      | 4    | `version`    | `1u32` (little-endian)           |
| 12     | 4    | `page_size`  | `4096u32`                        |
| 16     | 8    | `max_size`   | user-configured limit (bytes)    |
| 24     | 8    | `created_at` | Unix timestamp                   |
| 32     | 16   | `volume_uuid`| ties cache to a specific volume  |
| 48     | 4048 | `_reserved`  | zero-filled                      |

**Block record** (offset 4096+, each 4 KiB-aligned):

```
┌──────────────────────────────────────────────┐
│ Block Header (32 bytes)                       │
│  magic: [u8; 4]    = b"SBLK"                │
│  flags: u8          (DIRTY=0x01, PINNED=0x02)│
│  _reserved: [u8; 3]                          │
│  object_id: u64                              │
│  data_size: u32    (actual payload bytes)    │
│  checksum: u32     (crc32fast of payload)    │
│  _padding: [u8; 8]                           │
├──────────────────────────────────────────────┤
│ Payload  (data_size bytes)                    │
├──────────────────────────────────────────────┤
│ Padding  (to next 4 KiB boundary)            │
└──────────────────────────────────────────────┘
```

Allocation size = `ceil((32 + data_size) / 4096) * 4096`.

### FreeList allocator

`v2::allocator::FreeList` — best-fit with dual BTreeMap indexes:

- `by_offset: BTreeMap<u64, u64>` — O(log n) neighbour lookup for coalescing
- `by_size: BTreeMap<(u64, u64), ()>` — O(log n) best-fit search

Key behaviours:

| Operation | Description |
|-----------|-------------|
| `allocate(size)` | Best-fit from free list; grows file by `max(size, 256 MiB)` if no fit |
| `free(offset, size)` | Returns region, coalesces left+right neighbours, and shrinks logical file end |
| `recover(max_size, file_end, occupied)` | Rebuilds free list from live block offsets (used by future index recovery) |

File extension (`set_len`) happens **inside the allocator lock** to prevent
concurrent threads from observing an inconsistent file end.

### `BlockStore` API

```rust
BlockStore::open(config)            // create or open (validates magic, version, UUID, max_size)
BlockStore::put(id, data, evictable)// allocate + pwrite header + payload; replace if exists
BlockStore::get(id)                 // mmap + pread fallback + crc32 verify → Option<Bytes>
BlockStore::contains(id)            // index lookup only
BlockStore::remove(id)              // free allocation, update index
BlockStore::remove_batch(ids)       // batched remove under single allocator lock
BlockStore::mark_evictable(id)      // clear DIRTY + UPLOADING after S3 upload
BlockStore::mark_uploading(id)      // set transient UPLOADING flag
BlockStore::mark_upload_failed(id)  // clear UPLOADING, preserve DIRTY
BlockStore::advise_will_need(id)    // best-effort read-ahead hint for a cached block
BlockStore::sync_data()             // fdatasync
BlockStore::close(self)             // fdatasync + snapshot + WAL clear
// Drop impl: best-effort fdatasync on implicit drop
```

Implementation notes:

- `BlockIndex` is guarded by `Mutex<BlockIndex>` rather than `RwLock`: a cache
  hit updates LRU state, so `get()` is logically mutating.
- `MmapReader` uses atomic generation swaps so readers stay lock-free while
  remaps publish a larger mapping after file growth.
- `put()` uses a pending-allocation guard so allocation is rolled back if a
  post-write error or unwind happens before the new index entry is published.
- `mark_evictable()` keeps the index lock across the tiny header `pwrite` to
  prevent disk-offset ABA reuse from corrupting a different block's header.
- destructive mutations invalidate superseded block headers in `blocks.data`
  so fallback recovery scans only pick up live blocks.
- free paths issue best-effort hole-punch and `dont-need` hints, while file
  growth issues best-effort preallocation before `set_len()`.
- Windows keeps the same best-effort contract for advisory/storage hints by
  treating common unsupported or denied Win32 responses as benign rather than
  surfacing them as cache read/write failures.

Error variants include `CacheFull { needed, max_size, allocated, free }` for
fragmentation debugging and `MaxSizeShrunk` to prevent narrowing the cache
limit on an existing file.

### Index persistence

`BlockStore::open()` restores metadata in three stages:

1. **Fast path** — load `blocks.idx` snapshot and replay `blocks.wal`
2. **Validation pass** — re-read each recovered block header from
  `blocks.data` so stale snapshot entries or lost WAL tails are discarded
3. **Crash fallback** — if the metadata files are missing or corrupt, scan
  `blocks.data` page-by-page, validate block headers, and verify payload CRCs

Snapshot and WAL records are versioned binary formats.  WAL replay ignores a
partial trailing record so abrupt termination does not poison the whole log.
Clean shutdown writes a fresh snapshot and truncates `blocks.wal`.

WAL appends intentionally do not call `flush`/`sync_data()` on every mutation.
That keeps `put()`/`remove()` on the sequential append fast path, but it also
means an OS crash can lose the newest unsynced WAL tail.  Recovery treats that
as acceptable best-effort metadata loss and repairs it via snapshot replay or
`blocks.data` scan fallback.

---

## Cache v3 (current) — L1 + L2

> **Status:** Active.  Used by VFS.  Will be replaced by v2 after persistence
> and VFS integration land.

## Architecture

```
                  ┌────────────────────────────┐
                  │       BlockCache           │
                  │  (unified API + eviction)  │
                  └─────┬───────────┬──────────┘
                        │           │
              ┌─────────▼──┐  ┌─────▼──────────┐
              │  L1 Memory │  │   L2 Disk      │
              │  (sharded) │  │  (file/block)  │
              └─────┬──────┘  └───────┬────────┘
                    │                 │
              ┌─────▼─────────────────▼────────┐
              │     LRU Tracker (64 shards)     │
              └────────────────────────────────┘
                            │
              ┌─────────────▼──────────────────┐
              │     Upload Queue (journal)      │
              └────────────────────────────────┘
```

## Components

### Metadata Lookup Caches (`studiofs-vfs/src/vfs.rs`)

In addition to block caching, VFS keeps three bounded metadata lookup caches:

- `DentryCache` for `(parent ino, name) -> child ino | negative`
- `PathShortcutCache` for full-path shortcut resolution
- `AttrReadCache` for read-through inode attributes with TTL

These caches now share one generic two-tier implementation:

- `TwoTierCache<K, V, P>`: shared probationary/protected queues, promotion/demotion,
  eviction, lazy queue compaction, and metrics accounting
- `CachePolicy<K, V>`: per-cache behavior hooks for charge estimation, staleness
  checks, and index bookkeeping

This removes duplicated eviction/compaction logic and ensures fixes in the
two-tier algorithm apply consistently to all metadata caches.

### L1 Memory Cache (`l1_mem.rs`)

In-memory block store using 64 sharded `HashMap<u64, Bytes>`.

| Property | Value |
|----------|-------|
| Default size | 2 GiB |
| Shard key | `object_id % 64` |
| Lock type | `parking_lot::Mutex` per shard |
| Eviction | External (driven by LRU tracker) |

**Operations:** `get`, `insert`, `remove`, `contains`, `total_bytes`, `over_budget`.

No internal ordering — the global LRU tracker decides which blocks to evict.

### L2 Disk Cache (`l2_disk.rs`)

File-per-block on local NVMe storage at `{cache_dir}/blocks/`.

| Property | Value |
|----------|-------|
| Default size | 100 GiB |
| Sharding | 256 directories (`00/`–`ff/`), shard = `(object_id >> 56) as u8` |
| File pattern | `{shard_hex}/{object_id:016x}.block` |
| Write strategy | Atomic (write to `.tmp` + rename) |
| Eviction | Unlink file |

**Key design choices:**
- **Pre-computed shard paths** — all 256 directory paths cached in `Arc<[PathBuf; 256]>` to avoid repeated allocations
- **Atomic writes** — prevents partial reads on crash via write-to-temp + rename
- **Owned-buffer hot path** — `put_bytes(object_id, Bytes)` writes already-owned
  payloads without an extra `to_vec()` clone in the async L2 write path
- **Direct-read path by platform**:
  - Linux: `O_DIRECT`
  - Windows: `FILE_FLAG_NO_BUFFERING`
  - macOS: `fcntl(F_NOCACHE, 1)`
  Direct-read opens the file in direct mode, then derives block size from
  either size hint or opened-file metadata (`std_file.metadata()`), avoiding
  extra path-based `stat` races.
  > **Note (SFS-121):** This platform direct-I/O applies exclusively to reads
  > and writes of **L2 block-cache files** on the local NVMe/disk cache.  It
  > is entirely independent of the VFS bridge read/write path, which is
  > buffered-first on all platforms.  The former Windows/macOS
  > "direct-I/O product mode" at the VFS bridge layer has been permanently
  > removed; the FUSE and WinFSP bridges exclusively use the buffered path.
- **Robust fallback** — when a platform/filesystem rejects direct I/O for a
  file, read path transparently falls back to async buffered file reads.
  Linux buffered fallback emits `posix_fadvise(..., POSIX_FADV_DONTNEED)` after
  read completion to reduce double-caching pressure in page cache.
  macOS buffered/direct path relies on `F_NOCACHE` descriptor state and uses
  regular read/write calls (no `O_DIRECT`-style alignment requirements).
- **Alignment strategy** — L2 direct-read targets use a 4 KiB-aligned
  `BytesMut` allocation helper; if caller buffer/length is still unsuitable
  for direct mode, platform layer uses a temporary aligned bounce buffer
- **Direct-write attempt by platform** — temporary block files use the same
  platform direct-I/O strategy when supported, with buffered fallback when
  alignment or filesystem constraints reject direct mode
- **Write alignment guard** — for non-aligned payload sizes, direct-write path
  pads to aligned length and truncates to payload size to preserve on-disk
  visible length
- **Reduced zero-fill overhead** — aligned direct-I/O buffers are allocated
  without full upfront zeroing; only payload bytes (and required tail padding)
  are initialized
- **Fewer stat syscalls on hot reads** — `BlockCache` now passes known block
  size from LRU/cache metadata into L2 direct-read path, avoiding per-read
  `metadata()` in the common case
- **Sync caller-thread read path for cache hits** — hot path reads can serve L2
  hits synchronously into caller-provided `&mut [u8]` buffers via
  `read_into_sync()` / `get_sync_l2_into(...)`, bypassing async scheduler hops
- **Open file descriptor reuse for sync reads** — `L2DiskCache` keeps a small
  LRU of open block files for sync read hot paths to reduce repeated `open()`
  syscall overhead on frequent cache hits
- **Windows handle-share safety for rename/remove races** — sync-read cached
  file handles are opened with read/write/delete sharing to coexist with
  concurrent temp-rename based writes and removals
- **Hinted-size validation** — if a hinted direct-read returns a different
  payload length than expected, the direct path is discarded and read retries
  via buffered fallback for correctness
- **Current Linux runtime note** — this path does not use `io_uring` yet; it
  runs direct reads/writes via `spawn_blocking` with `std::fs::OpenOptions` +
  direct-open flags and async buffered fallback. Write/remove operations are
  semaphore-bounded, while read fan-out is controlled per call path
- **QoS-aware local I/O** — L2 cache operations now accept a foreground vs
  background priority token. Foreground user reads/writes keep the hot path on
  the foreground semaphore, while upload cleanup and cache eviction use the
  background semaphore to avoid starving interactive I/O
- **Elastic growth** — starts at 0 bytes, grows on demand up to `max_bytes`
- **Scan on startup** — `scan()` rebuilds in-memory byte counter and returns `(object_id, size)` pairs for LRU reload
- **Tunable sync FD cache size** — `STUDIOFS_L2_SYNC_FD_CACHE_CAPACITY`
  controls sync-read open-file LRU capacity (default `4096`)
- **Tunable batch-read fan-out** — `STUDIOFS_L2_READ_BATCH_MAX_IN_FLIGHT`
  controls max in-flight reads per `get_batch*` call (default `256`)
- **Buffered-read preallocation guard** — when size hint is absent,
  buffered L2 reads preallocate using file metadata length; if conversion to
  platform `usize` fails, fallback preallocation uses `8 MiB`.

**Batch operations:**
- `put_batch(blocks)` — sliding-window `JoinSet` scheduling with preserved
  input order and semaphore-bounded write workers
- `get_batch(ids)` — sliding-window `JoinSet` fan-out with preserved input
  order and direct-I/O-first read policy
- `get_batch_with_size_hints([(id, Option<size>)])` — same as `get_batch`, but
  can skip per-item `metadata()` when caller already knows block lengths
- `remove_batch_with_priority(ids, priority)` — remove many blocks in parallel
  with foreground/background QoS routing

### L2 Commit Scheduler (`block_cache.rs`)

Bounded scheduler for asynchronous L2 commits from the materializer path.

| Property | Value |
|----------|-------|
| Workers | 32 (default profile), 4 (`--l2-profile usb`) |
| Queue capacity | 4096 (default profile), 256 (`--l2-profile usb`) |
| Backpressure | `commit_l2_batch` awaits when queue is full |
| Batch ownership | `commit_l2_batch` consumes `Vec<(ObjectId, Bytes)>` and moves buffers into the scheduler without per-item clone in the cache layer |
| Worker behavior | each worker writes sequentially via `l2.put_bytes()` |

This replaces the previous fire-and-forget fan-out model and caps concurrent
L2 write operations to the worker count.
Runtime tuning is available through `STUDIOFS_L2_COMMIT_WORKERS` and
`STUDIOFS_L2_COMMIT_QUEUE_CAPACITY`.
For external USB SSDs, `studiofs mount --l2-profile usb` applies a conservative
preset to avoid bridge/controller saturation.

### LRU Tracker (`lru_tracker.rs`)

Global LRU across both tiers, tracking block location and eviction order.

| Property | Value |
|----------|-------|
| Shards | 64 |
| Lock type | `parking_lot::Mutex` per shard |
| Touch debounce | 10 seconds |
| Data per entry | `object_id`, `size`, `location`, `last_access`, `pinned` |

**Block locations:**
- `MemoryAndDisk` — in both L1 and L2
- `MemoryOnly` — in L1 only (pre-L2 commit)
- `DiskOnly` — in L2 only (evicted from L1)

**Eviction methods:**
- `evict_l1()` — finds oldest unpinned `MemoryAndDisk` entry, demotes to `DiskOnly`
- `evict_l2()` — finds oldest unpinned `DiskOnly` entry, removes completely

**Pinning:** Blocks being uploaded to S3 are pinned to prevent eviction before upload completes.

### Upload Queue (`upload_queue.rs`)

Crash-safe writeback journal for blocks pending S3 upload.

| Property | Value |
|----------|-------|
| Journal dir | `{cache_dir}/upload/` |
| File pattern | `{object_id:016x}.pending` |
| Entry size | 48 bytes (v2) |
| Magic | `STFSUP` |
| Recovery | Scans `.pending` files on startup |

**Lifecycle:**
1. `submit(ino, offset, object_id)` — creates journal file, sends to upload channel
2. Upload worker receives from channel, performs S3 PUT (cancellation-aware)
3. `complete(pending)` — removes journal file, unpins block in LRU
4. `cancel(pending)` — removes journal file and unpins block when upload is cancelled (for example, inode deleted)

### Block Cache (`block_cache.rs`)

Unified API combining all components above.

**Configuration (`BlockCacheConfig`):**

| Field | Default | Description |
|-------|---------|-------------|
| `l1_size` | 2 GiB | Maximum L1 memory |
| `l2_size` | 1 TiB | Maximum L2 disk |
| `cache_dir` | — | Root directory for L2 + journals |
| `disk_free_ratio` | 0.10 | Reserve 10% free disk space |
| `l2_commit_workers` | 32 | L2 commit worker count |
| `l2_commit_queue_capacity` | 4096 | Max queued L2 commit jobs before backpressure |
| `l2_read_timeout` | 5000ms | L2 read timeout before fallback to cache miss |
| `l2_write_timeout` | 5000ms | L2 write timeout per attempt |

**Key methods:**

| Method | Description |
|--------|-------------|
| `open(config)` | Initialize L1, L2, LRU, upload queue; scan L2 for recovery |
| `get(id)` | L1 → L2 → miss (returns None) |
| `get_sync_l2_into(id, off, out)` | Sync L2-only read directly into caller buffer (returns `None` on miss/bypass) |
| `stage_l1(id, data)` | Insert into L1 + LRU as `MemoryOnly` |
| `commit_l2(id, data)` | Enqueue one L2 commit into bounded scheduler |
| `commit_l2_batch(blocks)` | Consume and enqueue `Vec<(ObjectId, Bytes)>` into bounded scheduler (applies backpressure on full queue) |
| `submit_upload(ino, offset, id)` | Enqueue for S3 upload via journal |
| `complete_upload(pending)` | Unpin block in LRU |
| `evict_one_l1()` | Demote one block from L1 |
| `evict_one_l2()` | Remove one block from L2 |

Timeout behavior:
- L2 read timeout is treated as a cache miss (`get*` returns `None`) so callers can continue to remote/object-store fetch paths.
- Repeated L2 read timeouts open a short-lived read circuit breaker (default: 3 consecutive timeouts, 5s cooldown), temporarily bypassing L2 reads to avoid piling additional blocking disk tasks during device stalls.
- Sync L2 read-into path respects the same read circuit-breaker policy; when the breaker is open it reports miss/bypass so callers can fall back to async/remote fetch path.
- L2 write timeout fails the direct write path and is retried by background commit workers up to the configured attempt cap before dropping the local commit job.
- L2 write/remove operations are bounded by an internal semaphore (default 128 in-flight tasks), preventing unbounded mutation-side `spawn_blocking` growth when local disk stalls.
- Background eviction and post-upload cleanup use the background priority pool so large delete bursts do not compete with foreground reads on the same local disk budget.
- L2 batch reads use a sliding-window fan-out budget (`STUDIOFS_L2_READ_BATCH_MAX_IN_FLIGHT`, default 256) to avoid lock-step chunk stalls and keep read queue depth stable.

**Checkpointing:** Every 60 seconds, persists LRU state to `{cache_dir}/cache.meta`. On startup, loads checkpoint and reconciles with L2 scan.
If the serialized checkpoint payload is unchanged (same entry count + CRC), the
writer skips the redundant tmp+rename cycle to reduce metadata-heavy file ops.

Checkpoint format: magic `STFSCM` (6 bytes), version `u16`, entry count `u32`,
entries CRC32 `u32`, then entries of 18 bytes each (`object_id: u64`, `size:
u32`, `location: u8`, `pinned: u8`, `padding: [u8; 4]`).

## Eviction Strategy

1. **L1 eviction** — when `l1.total_bytes() > l1_size`:
   - Find oldest unpinned `MemoryAndDisk` block via LRU
   - Remove from L1, set location to `DiskOnly`
   - Block remains in L2

2. **L2 eviction** — when `l2.total_bytes() > l2_size` or disk space < `disk_free_ratio`:
   - Find oldest unpinned `DiskOnly` block via LRU
   - Delete file from L2
   - Remove from LRU entirely

3. **Pinned blocks** — blocks pending S3 upload cannot be evicted from either tier

## Write Pipeline Integration

The VFS `drain_dirty_mem()` function drives blocks through the cache:

```
dirty_mem + ingress_spill (12 GiB RAM + 64 GiB spill by default)
  │
  ├─ Take batch of up to 1024 blocks
  ├─ stage_l1() for each block          ← immediate L1 insert
  ├─ stage upload payload window        ← retry-friendly transient RAM source for uploader
  ├─ Batch metadata commit (local staged refs only)
  ├─ commit_l2_batch() enqueue          ← consumes owned batch, bounded queue + fixed workers
  └─ submit_upload(..., content_epoch)  ← journal + S3 queue
```

The background flusher runs every 10ms and drains continuously (keeps processing while data exists).
Freshly materialized blocks now also seed a bounded transient upload-payload
stage in RAM. Upload workers clone payloads from that stage first and only fall
back to L1/L2 rereads when the stage entry was evicted or the upload was
recovered from journal after restart. The stage budget is adaptive rather than
fixed: it tracks the upload dispatch window, expands under queue pressure or
staging misses, and contracts again when foreground pressure drops.

## Performance Characteristics

| Operation | Expected Throughput | Notes |
|-----------|-------------------|-------|
| L1 read | ~10 GB/s | Memory speed, contention on shard lock |
| L1 write | ~8 GB/s | Same as read + hash map insert |
| L2 read (single) | depends on FS/device | Linux `O_DIRECT`, Windows `NO_BUFFERING`, macOS `F_NOCACHE`; fallback to async buffered read |
| L2 read (batch) | depends on FS/device + queue depth | Parallel async reads with the same direct-I/O-first policy |
| L2 write (single) | depends on FS/device | direct-I/O write attempt (platform-specific) + buffered fallback, then atomic rename |
| L2 write (batch) | depends on FS/device + queue depth | Parallel writes through same direct-I/O-first policy |
| S3 upload | Variable | AIMD-controlled, raw payload in STFS object |

## Cache Pipeline Observability

StudioFS now exposes first-class cache pipeline metrics for local debugging and
benchmarks:

- L2 read/write throughput counters (`ops`, `bytes`) and latency histograms
- ingress append throughput counters and latency histogram
- materialize batch counters (`batches`, `blocks`, `bytes`), duration histogram,
  and concurrent batch depth/peak
- L2 commit scheduler counters:
  - queue depth (`queue_depth`, `queue_depth_peak`, `queue_capacity`)
  - worker utilization (`active_workers`, `active_workers_peak`, `configured_workers`)
  - lifecycle (`submitted`, `completed`, `failed`)
  - backpressure (`backpressure_events`, backpressure wait totals/max/histogram)
- queue depth metrics for upload queue (`pending_depth`, `pending_depth_peak`)
- staged upload payload metrics (`hits`, `misses`, `evictions`, `target_bytes`,
  `min_bytes`, `max_bytes`)
- backpressure metrics (`write_backpressure_wait_ms`, `write_backpressure_events`)
- transit-memory budget snapshot (`transit_memory_budget_bytes`, `transit_memory_used_bytes`, `transit_memory_available_bytes`, foreground/background waiter counts)
- blocking-task pressure metrics for cache hot paths:
  - active blocking tasks (`active`, `active_peak`)
  - lifecycle counters (`submitted`, `started`, `completed`)
  - wait-before-execution timing (avg/max + histogram buckets in snapshot)
  - blocking execution timing (avg/max + histogram buckets in snapshot)

Runtime logs (`upload pipeline counters`) include these metrics continuously
while the pipeline is active. `studiofs test-cache <cache-dir>` also emits
the cache pipeline snapshot in its saved JSON report under `cache_pipeline`,
while the terminal keeps a human-readable summary focused on the benchmark
results. Each `test-cache` report now also includes a `run_id` for correlation
with profile/tracing events, and optional on-demand Pyroscope profiling when
running `studiofs test-cache ... --profile=pyroscope`.

`test-cache` reports now also include an `advanced_metrics` section:

- `size_class_boundaries`: configurable class boundaries used for aggregation
  (`small_max_bytes`, `medium_max_bytes`)
- `size_class_stats`: write/read throughput and latency percentiles (p50/p95/p99)
  for `small`, `medium`, and `large`
- `cache_levels` (direct-L2 mode): L1/L2 hit and miss counters, bytes served
  from each level, and per-level lookup latency percentiles + histogram
- `queue_backpressure`: L2 commit queue depth, worker utilization, backpressure
  count, backpressure wait percentiles (p50/p95/p99), plus upload queue depth
  and lifecycle counters

## Pyroscope Profiling

`test-cache` can publish CPU and memory profiles in pprof format while the
benchmark is running.

1. Install Grafana Pyroscope or use an existing Grafana Cloud / Pyroscope
   endpoint. For a local instance, one simple option is:

   ```bash
   docker run --rm -p 4040:4040 grafana/pyroscope:latest
   ```

2. Set the endpoint before running the benchmark:

   ```bash
   export STUDIOFS_PYROSCOPE_URL=http://localhost:4040
   export STUDIOFS_PYROSCOPE_APP_NAME=studiofs.test-cache
   ```

   Optional extras:

   - `STUDIOFS_PYROSCOPE_SAMPLE_RATE=100`
   - `STUDIOFS_PYROSCOPE_USERNAME=...`
   - `STUDIOFS_PYROSCOPE_PASSWORD=...`
   - `STUDIOFS_PYROSCOPE_TENANT_ID=...`

3. Run the benchmark with profiling enabled:

   ```bash
   studiofs test-cache /path/to/cache --profile=pyroscope
   ```

4. At completion, `test-cache` writes local pprof artifacts next to the JSON
   report:

   - `studiofs-test-cache-<mode>-<run_id>-cpu.pprof.gz`
   - `studiofs-test-cache-<mode>-<run_id>-memory.pprof.gz`

   If an endpoint is configured, those same profiles are also pushed to
   Pyroscope.

5. In Grafana, open the Pyroscope data source, choose the `studiofs.test-cache`
   service, and switch between the CPU and memory views to inspect flame graphs
   for cache hot paths, scheduler contention, and allocation pressure.

If the profiling backend or endpoint is unavailable, the benchmark prints a
warning and still produces the normal benchmark report.

Size classes are configurable from the CLI:

- `--small-max <size>`: inclusive upper bound for the small class (default `10KB`)
- `--medium-max <size>`: inclusive upper bound for the medium class
  (default `--block-size`)
- `--sizes <spec>`: workload composition, either percentages
  (`small=60,medium=30,large=10`) or explicit lists (`4KB,128KB,1MB,16MB`)

Warm-cache measurement controls:

- `--warmup-runs <n>` runs `n` prefill cycles before measured phases
- `--sequential-streams` keeps each stream sequential over its assigned segment
- `--concurrency <n>` controls number of streams (default `1`)
- in sequential mode, effective in-flight I/O is bounded by
  `min(--concurrency, --io-depth)`
- throughput values are phase-local: write-side metrics use write-phase wall
  time and read-side metrics use read-phase wall time, rather than total
  benchmark wall time

The terminal summary now shows p50/p95/p99 + throughput for overall write/read
results and for each size class.

## Crash Recovery

On startup:

1. Load LRU checkpoint (`cache.meta`) if present
2. Scan L2 directory to rebuild actual block inventory
3. Reconcile: remove LRU entries for missing L2 files, add L2 entries not in checkpoint
4. Scan upload journal (`upload/*.pending`) and re-queue pending uploads
5. Recovered upload ops keep their persisted `content_epoch`
6. Resume normal operation

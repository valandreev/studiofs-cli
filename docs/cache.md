# Cache System

The cache subsystem (`studiofs-cache`) implements block caching for high-throughput
NVMe-backed storage with crash-safe writeback to S3.

There are two generations of the cache implementation:

| Generation | Module | Status |
|------------|--------|--------|
| **v3 (legacy stack)** | `l1_mem`, `l2_disk`, `lru_tracker`, `runtime`, direct-I/O helpers | **Retired in SFS-143** |
| **v2 (active path)** | `v2::block_store`, `v2::allocator`, `v2::format`, `v2::index`, `v2::lru` | Canonical cache path for foreground reads, local durability, and startup dirty replay |
| **compat shim** | `block_cache`, `upload_queue` | Thin wrapper retained for queue/observability compatibility while callers finish migrating |

---

## Cache v2 — Single-file BlockStore

> **Status:** Active and authoritative implementation.
> Core BlockStore, allocator, intrusive LRU, in-memory BlockIndex, dirty/uploading
> eviction gating, auto-eviction, mmap reads with generation remapping, snapshot/WAL
> persistence, and startup recovery are implemented.

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

File extension (`set_len`) happens under a dedicated file-growth mutex to
prevent concurrent threads from observing an inconsistent file end.

### `BlockStore` API

```rust
BlockStore::open(config)            // create or open (validates magic, version, UUID, max_size)
BlockStore::put(id, data, evictable)// allocate + pwrite header + payload; replace if exists
BlockStore::get(id)                 // zero-copy mmap + pread fallback + optional crc32 verify → Option<Bytes>
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

### `LocalBlockBackend` contract (SFS-159)

`BlockStore` now implements a backend boundary used by the redesigned data
path:

```rust
trait LocalBlockBackend {
    // read path
    fn get(&self, object_id: u64) -> Option<Bytes>;
    fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize>;
    fn get_for_upload(&self, object_id: u64) -> Option<Bytes>;

    // write + lifecycle
    fn put(&self, object_id: u64, data: &[u8], evictable: bool) -> Result<(), LocalBlockBackendError>;
    fn mark_uploading(&self, object_id: u64);
    fn mark_upload_failed(&self, object_id: u64);
    fn mark_uploaded(&self, object_id: u64); // clears DIRTY/UPLOADING in BlockStore
    fn lifecycle_state(&self, object_id: u64) -> LocalBlockLifecycleState;
    fn is_evictable(&self, object_id: u64) -> bool;
}
```

Lifecycle semantics:

- `Missing` = no local block entry.
- `Dirty` = persisted locally, needs upload.
- `Uploading` = in-flight upload.
- `Uploaded` = not dirty/uploading anymore.
  This does **not** imply evictable in all backends; use `is_evictable(...)`
  when eviction eligibility matters.

Implementation notes:

- `BlockIndex` is guarded by `Mutex<BlockIndex>` rather than `RwLock`: a cache
  hit updates LRU state, so `get()` is logically mutating.
- `MmapReader` uses atomic generation swaps so readers stay lock-free while
  remaps publish a larger mapping after file growth, and mmap hits return
  zero-copy `Bytes` backed by the live generation.
- Read-side checksum verification is enabled by default and can be disabled
  with `STUDIOFS_CACHE_VERIFY_READ_CHECKSUMS=false` when throughput matters
  more than payload integrity checks. `BlockStore` reads the value at open, so
  change the setting with a service restart.
- `put()` uses a pending-allocation guard so allocation is rolled back if a
  post-write error or unwind happens before the new index entry is published.
- `mark_evictable()` keeps the index lock across the tiny header `pwrite` to
  prevent disk-offset ABA reuse from corrupting a different block's header.
- destructive mutations invalidate superseded block headers in `blocks.data`
  so fallback recovery scans only pick up live blocks.
- free paths issue best-effort hole-punch and `dont-need` hints, while file
  growth issues best-effort preallocation before `set_len()`.
- on Windows, `blocks.data` is marked sparse when possible so NTFS can extend
  the file without synchronous zero-fill; physical growth is serialized under
  a dedicated file-growth mutex rather than the allocator lock.
- Windows keeps the same best-effort contract for advisory/storage hints by
  treating common unsupported or denied Win32 responses as benign rather than
  surfacing them as cache read/write failures.
- emergency disk-headroom checks use a short refresh interval cache rather than
  issuing `get_free_disk_space()` on every `put`; concurrent refreshes are
  guarded so only one thread performs the syscall while others reuse cached
  values.
- physical `blocks.data` length is tracked in-memory and updated after
  successful growth, avoiding per-allocation metadata length syscalls.
- `get_for_upload()` intentionally avoids LRU `touch()`: upload rereads should
  not refresh recency and displace foreground-hot entries.

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

## Cache v2 Compatibility Shim

> **Status:** Active. Legacy `l1_mem`, `l2_disk`, `lru_tracker`, and commit
> scheduler modules were removed in SFS-143.
> `BlockStore` (Cache v2) is the primary local-cache data plane.

## Architecture

```
                ┌────────────────────────────┐
                │       BlockCache           │
                │   (compatibility shim)     │
                └───────────┬────────────────┘
                            │
          ┌─────────────────┴──────────────────┐
          │                                    │
   ┌──────▼────────┐                  ┌────────▼─────────┐
   │ L1 Staging    │                  │ Upload Queue     │
   │ (bounded RAM) │                  │ (in-memory+journal) │
   └───────────────┘                  └──────────────────┘
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

> Legacy sections below are historical only and no longer represent runtime code.

### Historical: L1 Memory Cache (`l1_mem.rs`)

In-memory block store using 64 sharded `HashMap<u64, Bytes>`.

| Property | Value |
|----------|-------|
| Default size | 2 GiB |
| Shard key | `object_id % 64` |
| Lock type | `parking_lot::Mutex` per shard |
| Eviction | External (driven by LRU tracker) |

**Operations:** `get`, `insert`, `remove`, `contains`, `total_bytes`, `over_budget`.

No internal ordering — the global LRU tracker decides which blocks to evict.

### Historical: L2 Disk Cache (`l2_disk.rs`)

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

### BlockCache Compatibility Shim (`block_cache.rs`)

Cache v2 removed the legacy L2 commit scheduler and global LRU tracker.
`BlockCache` now acts as a compatibility shim used by VFS for:

- bounded in-memory L1 staging (`stage_l1`, `get_l1_only`)
- upload queue orchestration (`submit_upload*`, `complete_upload`, `cancel_upload`)
- compatibility pipeline metrics consumed by CLI/VFS logs

**Configuration (`BlockCacheConfig`):**

| Field | Default | Description |
|-------|---------|-------------|
| `l1_size` | 2 GiB | Maximum in-memory staging budget |
| `cache_dir` | — | Root directory used by upload-queue journal files |

**Key methods:**

| Method | Description |
|--------|-------------|
| `open(config)` | Initialize L1 staging + upload queue |
| `get(id)` / `get_with_trace(id)` | L1 lookup with compatibility trace surface |
| `stage_l1(id, data)` | Stage payload in bounded L1 memory |
| `submit_upload(ino, offset, id, epoch)` | Enqueue upload work |
| `complete_upload(pending)` | Mark upload complete and unpin payload |
| `cancel_upload(pending, remove_block)` | Cancel upload and optionally drop staged payload |
| `wait_for_uploads_idle(timeout)` | Await queue drain |
| `cache_pipeline_metrics()` | Snapshot compatibility counters |

Notes:
- legacy `STUDIOFS_L2_COMMIT_*` and L2 timeout knobs were removed;
- `BlockCacheStats` now reports only active L1 staging bytes.

## Write Pipeline Integration

The VFS `drain_dirty_mem()` function drives blocks through the cache:

```
dirty_mem + ingress_spill (12 GiB RAM + 64 GiB spill by default)
  │
  ├─ Take batch of up to 1024 blocks
  ├─ stage_l1() for each block          ← immediate L1 insert
  ├─ stage upload payload window        ← retry-friendly transient RAM source for uploader
  ├─ Batch metadata commit (local staged refs only)
  ├─ Persist to BlockStore              ← cache-v2 local object store
  └─ submit_upload(..., content_epoch)  ← in-memory queue + S3 queue
```

The background flusher runs every 10ms and drains continuously (keeps processing while data exists).
Freshly materialized blocks seed a bounded transient upload-payload stage in RAM.
Upload workers clone payloads from that stage first and only fall back to local
cache rereads when the stage entry was evicted or the upload was replayed from
`BlockStore` DIRTY state after restart. The stage budget is adaptive rather than
fixed: it tracks the upload dispatch window, expands under queue pressure or
staging misses, and contracts again when foreground pressure drops.

## Performance Characteristics

The `shim_read` / `shim_write` counters track compatibility-shim operations in
the Cache v2 pipeline (L1 staging + upload orchestration).

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

- shim read/write throughput counters (`ops`, `bytes`) and latency histograms
- ingress append throughput counters and latency histogram
- materialize batch counters (`batches`, `blocks`, `bytes`), duration histogram,
  and concurrent batch depth/peak
- queue depth metrics for upload queue (`pending_depth`, `pending_depth_peak`)
- staged upload payload metrics (`hits`, `misses`, `evictions`, `target_bytes`,
  `min_bytes`, `max_bytes`)
- backpressure metrics (`write_backpressure_wait_ms`, `write_backpressure_events`)
- transit-memory budget snapshot (`transit_memory_budget_bytes`, `transit_memory_used_bytes`, `transit_memory_available_bytes`, foreground/background waiter counts)
- disk-headroom refresh metrics (`disk_headroom_refresh_attempt_count`,
  `disk_headroom_refresh_success_count`,
  `disk_headroom_refresh_error_count`,
  `disk_headroom_refresh_skip_interval_count`,
  `disk_headroom_refresh_skip_inflight_count`,
  `disk_headroom_refresh_saved_syscall_count`)
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
- `cache_levels` (direct-shim mode): L1 hit and miss counters, bytes served
  from L1, and L1 lookup latency percentiles + histogram
- `upload_queue`: depth and lifecycle counters

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
4. Read dirty object ids from `BlockStore` and re-queue upload ops using metadata mapping
5. Remove orphan dirty object ids that are no longer referenced by metadata
6. Resume normal operation

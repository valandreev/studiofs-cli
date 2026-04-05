# Cache System

The cache subsystem (`studiofs-cache`) implements the RawBlockFileBackend-backed
local data plane plus the narrow compatibility shim that still fronts upload
queueing and observability.

There are two generations of the cache implementation:

| Generation | Module | Status |
|------------|--------|--------|
| **v3 (legacy stack)** | `l1_mem`, `l2_disk`, `lru_tracker`, `runtime`, direct-I/O helpers | **Retired in SFS-143** |
| **v2 (active path)** | `v2::raw_block_file_backend`, `v2::local_block_backend`, `v2::platform` | Active local block backend and shared helpers |
---

## Cache v2 Compatibility Shim

> **Status:** Active. Legacy `l1_mem`, `l2_disk`, `lru_tracker`, and commit
> scheduler modules were removed in SFS-143.
> `RawBlockFileBackend` (Cache v2) is the primary local-cache data plane.

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
`BlockCache` now acts as a narrow compatibility shim used by VFS for:

- bounded in-memory L1 staging (`stage_l1`, `get`, `get_with_trace`)
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
- direct-shim benchmark mode measures this shim, not the mounted end-to-end
  publication path.

## Write Pipeline Integration

The VFS `drain_dirty_mem()` function drives blocks through the cache:

```
dirty_mem + ingress_spill (12 GiB RAM + 64 GiB spill by default)
  │
  ├─ Take batch of up to 1024 blocks
  ├─ stage_l1() for each block          ← immediate L1 insert
  ├─ stage upload payload window        ← retry-friendly transient RAM source for uploader
  ├─ Batch metadata commit (local staged refs only)
  ├─ Persist to local block backend     ← cache-v2 local object store
  └─ submit_upload(..., content_epoch)  ← in-memory queue + S3 queue
```

The background flusher runs every 10ms and drains continuously (keeps processing while data exists).
Freshly materialized blocks seed a bounded transient upload-payload stage in RAM.
Upload workers clone payloads from that stage first and only fall back to local
cache rereads when the stage entry was evicted or the upload was replayed from
local backend dirty state after restart. The stage budget is adaptive rather than
fixed: it tracks the upload dispatch window, expands under queue pressure or
staging misses, and contracts again when foreground pressure drops.

## Performance Characteristics

The compatibility shim still exposes benchmark-facing staging and upload
orchestration timing. Those numbers belong to `direct-shim` mode; they measure
the compatibility path, not the final mounted chunk/slice/block publication
path. The old `shim_read` / `shim_write` counter names are gone.

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

- L1 staging throughput counters (`ops`, `bytes`) and latency histograms
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
while the pipeline is active. `studiofs test-cache <cache-dir>` also emits the
cache pipeline snapshot in its saved JSON report under `cache_pipeline`, while
the terminal keeps a human-readable summary focused on the benchmark results.
In `direct-shim` mode, the report describes the compatibility path. In `e2e`
mode, it describes the mounted end-to-end path through the final data model.
Each `test-cache` report now also includes a `run_id` for correlation with
profile/tracing events, and optional on-demand Pyroscope profiling when
running `studiofs test-cache ... --profile=pyroscope`.

`test-cache` reports now also include an `advanced_metrics` section:

- `size_class_boundaries`: configurable class boundaries used for aggregation
  (`small_max_bytes`, `medium_max_bytes`)
- `size_class_stats`: write/read throughput and latency percentiles (p50/p95/p99)
  for `small`, `medium`, and `large`
- `cache_levels` (direct-shim mode): compatibility hit and miss counters,
  bytes served from the shim, and compatibility lookup latency percentiles +
  histogram
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

1. Open `blocks.raw`, creating it with a fresh header when missing.
2. Validate the file header and replay append-only records until the first invalid or truncated tail record.
3. Rebuild object lifecycle state from the recovered records.
4. Resume local reads and writes from the reconstructed backend state.
5. Run best-effort compaction when dead records accumulate.
6. Re-queue dirty object ids from the reconstructed lifecycle state through metadata mapping.
7. Resume normal operation.

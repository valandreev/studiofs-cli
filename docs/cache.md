# Cache System

The cache subsystem (`studiofs-cache`) implements a two-level block cache designed for high-throughput NVMe-backed storage with crash-safe writeback to S3.

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
  - Windows: `FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED`
  - macOS: `fcntl(F_NOCACHE, 1)`
  All direct-read paths use page-aligned `BytesMut` and freeze into `Bytes`
  without secondary `copy_from_slice()`.
- **Robust fallback** — when a platform/filesystem rejects direct I/O for a
  file, read path transparently falls back to async buffered file reads
- **Alignment safety** — files whose length is not direct-I/O aligned also
  fall back to buffered reads to avoid `EINVAL`-style failures on strict filesystems
- **Direct-write attempt by platform** — temporary block files use the same
  platform direct-I/O strategy when supported, with buffered fallback when
  alignment or filesystem constraints reject direct mode
- **Reduced zero-fill overhead** — aligned direct-I/O buffers are allocated
  without full upfront zeroing; only payload bytes (and required tail padding)
  are initialized
- **Fewer stat syscalls on hot reads** — `BlockCache` now passes known block
  size from LRU/cache metadata into L2 direct-read path, avoiding per-read
  `metadata()` in the common case
- **Current Linux runtime note** — this path does not use `io_uring` yet; it
  uses Tokio file APIs and direct-open flags with async fallback behavior
- **Elastic growth** — starts at 0 bytes, grows on demand up to `max_bytes`
- **Scan on startup** — `scan()` rebuilds in-memory byte counter and returns `(object_id, size)` pairs for LRU reload

**Batch operations:**
- `put_batch(blocks)` — parallel `spawn_blocking` per block
- `get_batch(ids)` — parallel async fan-out with preserved input order

### L2 Commit Scheduler (`block_cache.rs`)

Bounded scheduler for asynchronous L2 commits from the materializer path.

| Property | Value |
|----------|-------|
| Workers | 4 (internal default) |
| Queue capacity | 512 (internal default) |
| Backpressure | `commit_l2_batch` awaits when queue is full |
| Worker behavior | each worker writes sequentially via `l2.put_bytes()` |

This replaces the previous fire-and-forget fan-out model and caps concurrent
L2 write operations to the worker count.
Runtime tuning is available through `STUDIOFS_L2_COMMIT_WORKERS` and
`STUDIOFS_L2_COMMIT_QUEUE_CAPACITY`.

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
| Entry size | 42 bytes |
| Magic | `STFSUP` |
| Recovery | Scans `.pending` files on startup |

**Lifecycle:**
1. `submit(ino, offset, object_id)` — creates journal file, sends to upload channel
2. Upload worker receives from channel, performs S3 PUT
3. `complete(pending)` — removes journal file, unpins block in LRU

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

**Key methods:**

| Method | Description |
|--------|-------------|
| `open(config)` | Initialize L1, L2, LRU, upload queue; scan L2 for recovery |
| `get(id)` | L1 → L2 → miss (returns None) |
| `stage_l1(id, data)` | Insert into L1 + LRU as `MemoryOnly` |
| `commit_l2(id, data)` | Enqueue one L2 commit into bounded scheduler |
| `commit_l2_batch(blocks)` | Enqueue batch into bounded scheduler (applies backpressure on full queue) |
| `submit_upload(ino, offset, id)` | Enqueue for S3 upload via journal |
| `complete_upload(pending)` | Unpin block in LRU |
| `evict_one_l1()` | Demote one block from L1 |
| `evict_one_l2()` | Remove one block from L2 |

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
  ├─ Batch metadata commit (local staged refs only)
  ├─ commit_l2_batch() enqueue          ← bounded queue + fixed workers
  └─ submit_upload(..., content_epoch)  ← journal + S3 queue
```

The background flusher runs every 10ms and drains continuously (keeps processing while data exists).

## Performance Characteristics

| Operation | Expected Throughput | Notes |
|-----------|-------------------|-------|
| L1 read | ~10 GB/s | Memory speed, contention on shard lock |
| L1 write | ~8 GB/s | Same as read + hash map insert |
| L2 read (single) | depends on FS/device | Linux `O_DIRECT`, Windows NO_BUFFERING+OVERLAPPED, macOS `F_NOCACHE`; fallback to async buffered read |
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
- backpressure metrics (`write_backpressure_wait_ms`, `write_backpressure_events`)
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
  (`small=60,medium=30,large=10`) or explicit lists (`4KB,128KB,1MB,8MB`)

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

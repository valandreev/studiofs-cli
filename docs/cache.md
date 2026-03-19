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
| Default size | 50 GiB |
| Sharding | 256 directories (`00/`–`ff/`), shard = `(object_id >> 56) as u8` |
| File pattern | `{shard_hex}/{object_id:016x}.block` |
| Write strategy | Atomic (write to `.tmp` + rename) |
| Eviction | Unlink file |

**Key design choices:**
- **Pre-computed shard paths** — all 256 directory paths cached in `Arc<[PathBuf; 256]>` to avoid repeated allocations
- **Atomic writes** — prevents partial reads on crash via write-to-temp + rename
- **Elastic growth** — starts at 0 bytes, grows on demand up to `max_bytes`
- **Scan on startup** — `scan()` rebuilds in-memory byte counter and returns `(object_id, size)` pairs for LRU reload

**Batch operations:**
- `put_batch(blocks)` — parallel `spawn_blocking` per block
- `get_batch(ids)` — parallel `spawn_blocking` per block

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
| `l2_size` | 100 GiB | Maximum L2 disk |
| `cache_dir` | — | Root directory for L2 + journals |
| `disk_free_ratio` | 0.10 | Reserve 10% free disk space |

**Key methods:**

| Method | Description |
|--------|-------------|
| `open(config)` | Initialize L1, L2, LRU, upload queue; scan L2 for recovery |
| `get(id)` | L1 → L2 → miss (returns None) |
| `stage_l1(id, data)` | Insert into L1 + LRU as `MemoryOnly` |
| `commit_l2(id, data)` | Write to L2, update LRU to `MemoryAndDisk` |
| `commit_l2_batch(blocks)` | Parallel L2 writes, single eviction check |
| `submit_upload(ino, offset, id)` | Enqueue for S3 upload via journal |
| `complete_upload(pending)` | Unpin block in LRU |
| `evict_one_l1()` | Demote one block from L1 |
| `evict_one_l2()` | Remove one block from L2 |

**Checkpointing:** Every 60 seconds, persists LRU state to `{cache_dir}/cache_map.bin`. On startup, loads checkpoint and reconciles with L2 scan.

Checkpoint format: magic `STFSCM` (6 bytes), version `u16`, then entries of 18 bytes each (`object_id: u64`, `size: u32`, `location: u8`, `pinned: u8`, `last_access: u32`).

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
  ├─ commit_l2_batch() in parallel      ← parallel NVMe writes
  └─ submit_upload(..., content_epoch)  ← journal + S3 queue
```

The background flusher runs every 10ms and drains continuously (keeps processing while data exists).

## Performance Characteristics

| Operation | Expected Throughput | Notes |
|-----------|-------------------|-------|
| L1 read | ~10 GB/s | Memory speed, contention on shard lock |
| L1 write | ~8 GB/s | Same as read + hash map insert |
| L2 read (single) | ~2 GB/s | `spawn_blocking` + `std::fs::read` |
| L2 read (batch) | ~3 GB/s | Parallel `spawn_blocking` |
| L2 write (single) | ~1.5 GB/s | `spawn_blocking` + write + rename |
| L2 write (batch) | ~3 GB/s | Parallel `spawn_blocking` |
| S3 upload | Variable | AIMD-controlled, raw payload in STFS object |

## Cache Pipeline Observability

StudioFS now exposes first-class cache pipeline metrics for local debugging and
benchmarks:

- L2 read/write throughput counters (`ops`, `bytes`) and latency histograms
- ingress append throughput counters and latency histogram
- materialize batch counters (`batches`, `blocks`, `bytes`), duration histogram,
  and concurrent batch depth/peak
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
with profile/tracing events, and optional on-demand CPU profile output when
running `studiofs test-cache ... --profile`.

## Crash Recovery

On startup:

1. Load LRU checkpoint (`cache_map.bin`) if present
2. Scan L2 directory to rebuild actual block inventory
3. Reconcile: remove LRU entries for missing L2 files, add L2 entries not in checkpoint
4. Scan upload journal (`upload/*.pending`) and re-queue pending uploads
5. Recovered upload ops keep their persisted `content_epoch`
6. Resume normal operation

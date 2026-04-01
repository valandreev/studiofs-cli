# StudioFS Cache v2 Performance Regression Analysis

> Status update (SFS-145): Root causes #2, #3, and #5 below have been
> addressed in current code. This note is retained as historical analysis.

## Root Cause #1 (CRITICAL): Double `fsync` per 4MB block

The write path in write_buffer.rs does this **for every full block batch**:

```
block_store_put_async (×N blocks, sequential)
→ block_store_sync_data_async()   ← fdatasync/FlushFileBuffers  ~5-20ms
→ run_meta(flush_blocks_batch)    ← redb txn.commit() → fsync   ~5-20ms
```

That's **two NVMe flush commands** per 4MB of data. On NVMe, `FlushFileBuffers` (Windows) typically costs 5–20ms. At 10ms each, that's 20ms of pure flush overhead per 4MB block = **200 MB/s ceiling** before any other work.

With the old 1-file-per-block design, `fdatasync` flushed a single small file and the OS could coalesce writes more efficiently. In the single-file design, `FlushFileBuffers` must flush the entire file's dirty pages.

## Root Cause #2 (HIGH): Sequential `put` with `spawn_blocking` per block

Status: Mitigated. The write path now uses bounded chunked pipelining for
`BlockStore::put(...)` calls instead of strict one-by-one awaits.

In write_buffer.rs:

```rust
for block in &persisted_blocks {
    self.block_store_put_async(block.object_id, block.data.clone(), false)
        .await?;  // Sequential! Each block waits for the previous one
}
```

Each block goes through `spawn_blocking` → thread pool → `BlockStore::put()` → return. No pipelining. Even without the fsyncs, this serializes all I/O.

## Root Cause #3 (HIGH): `GetDiskFreeSpaceExW` on every `put`

Status: Mitigated. Disk headroom is cached and refreshed periodically; refresh
is concurrency-guarded so a single thread performs the syscall.

In block_store.rs, `allocate_with_eviction` calls `evict_for_low_disk_headroom(alloc)` which calls `get_free_disk_space()` → `GetDiskFreeSpaceExW()` **on every single put**. This is a kernel round-trip (~0.5–1ms) that's completely unnecessary during normal operation.

## Root Cause #4 (MEDIUM): Windows NTFS zero-fill on file growth

In block_store.rs, when the data file grows:

```rust
platform::preallocate(&self.file, current_len, needed_len - current_len)  // NO-OP on Windows!
self.file.set_len(needed_len)?;  // NTFS zero-fills the gap
```

`preallocate_impl` is a **no-op on Windows** (only uses `fallocate` on Linux). `GROWTH_CHUNK = 256MB`, so `set_len` triggers ~256MB of NTFS zero-fill every time the cache file grows. This is done under the allocator lock, blocking all other puts.

## Root Cause #5 (MEDIUM): Unnecessary `file.metadata()?.len()` syscall per put

Status: Mitigated. BlockStore tracks physical file length in memory and updates
it after successful file growth.

In `try_allocate_and_extend` (block_store.rs):

```rust
let needed_len = a.file_end();
let current_len = self.file.metadata()?.len();  // GetFileInformationByHandle on every put!
```

The allocator already tracks `file_end` in memory. The physical file length could also be tracked internally instead of querying the OS every time.

## Read Path: 600 MB/s (expected 3–4 GB/s)

### Read overhead #1: Full CRC32 verification on every read

Every `get()` in block_store.rs computes `crc32fast::hash(&data)` on the full 4MB payload. Even `read_into()` for partial reads checksums the **entire** block (mmap_reader.rs).

### Read overhead #2: Full 4MB copy from mmap

In mmap_reader.rs:
```rust
return Ok(Bytes::copy_from_slice(&generation.mmap[start..end]));
```
The comment even acknowledges this: *"Cache v2 intentionally stays at one-copy reads for this phase."* — that's 4MB memcpy + allocation per read.

### Read overhead #3: Double index lock acquisition

Each `get()` acquires the index lock twice: once to read metadata, once to validate+touch LRU. Combined with `spawn_blocking` scheduling overhead, this adds ~1–2ms of pure coordination cost per 4MB block.

---

## Summary & Recommended Fixes

| Fix | Impact | Complexity |
|-----|--------|------------|
| Batch `fdatasync` — sync once after N blocks, not per block | Fixes ~60% of write regression | Low |
| Pipeline `put` calls (parallel spawn_blocking or write_vectored) | +50–100% write throughput | Medium |
| Throttle disk-free check (once per N seconds, not per put) | Saves ~0.5–1ms per put | Low |
| Implement Windows `preallocate` via `SetFileValidData` or pre-allocate at open | Eliminates zero-fill stalls | Medium |
| Cache physical file length in-memory instead of `file.metadata()` | Saves 1 syscall per put | Low |
| Optional CRC32 skip for reads (configurable) | +30–50% read throughput | Low |
| Zero-copy mmap reads (bind Bytes to Arc\<Mmap\>) | +20–30% read throughput | Medium |

The **single biggest win** is removing the per-block fdatasync. In the old design with individual files, the OS could absorb many small file-creates in its page cache. In the single-file design, `FlushFileBuffers` forces the entire file's write buffer to NVMe, creating a hard serialization point. Batching the sync (e.g., once per flush-batch rather than per block, or on a timer) would be the highest-ROI fix.

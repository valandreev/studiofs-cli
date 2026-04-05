# Design Spec: Parallel Raw Block File Backend

## Objective
Remove I/O serialization and redundant synchronization in `RawBlockFileBackend` to achieve throughput matching JuiceFS/LucidLink (~1000 MB/s write, ~2000 MB/s read) on NVMe storage.

## Background & Motivation
The current `RawBlockFileBackend` uses a single `Mutex<File>` and `Mutex<RawBlockFileState>`, which serializes all operations. Furthermore, every `put` operation performs two sequential `fdatasync` calls, creating a massive bottleneck for both throughput and concurrency. NVMe storage can handle many parallel I/O requests, but the current design prevents the system from utilizing this capability.

## Proposed Solution

### 1. Lock-Free File Access
- Replace `Mutex<File>` with `Arc<File>`.
- Utilize thread-safe `platform::pread` and `platform::pwrite` (mapping to `read_at`/`write_at` on Unix and `seek_read`/`seek_write` on Windows) to allow concurrent I/O at different offsets without file-level locking.

### 2. Parallel Write Coordination
- Introduce `AtomicU64` for `reserved_len` to track the "reserved" end of the file.
- **Write Flow (`put`):**
  1. Atomically reserve space: `let offset = reserved_len.fetch_add(RECORD_HEADER_SIZE + payload_len)`.
  2. Combine header and body into a single 4KiB-aligned buffer (to prepare for potential `O_DIRECT` future-proofing).
  3. Perform `pwrite` of the combined buffer to the reserved `offset`.
  4. Take a write-lock on `state: RwLock<RawBlockFileState>` and update the `entries` index.
  5. Update `state.file_len = reserved_len.load()`.

### 3. Removal of Redundant Syncs
- Remove `fdatasync` from `append_record` and `put`.
- Rely on `LocalBlockBackend::sync_data` (called by `Vfs` at the end of a batch) to perform a single `file.sync_data()` call.
- This ensures that a batch of 8 parallel blocks (the current VFS pipeline width) is made durable with one sync instead of 16.

### 4. Efficient Read Path
- Use `state: RwLock<RawBlockFileState>`.
- `get` and `read_into` take a read-lock on `state` to find the offset/length.
- Release the read-lock immediately after finding the entry, then perform `pread` in parallel.
- This allows multiple concurrent reads to proceed without blocking each other or being blocked by writes.

### 5. Preallocation and Sparse Files
- Use `platform::preallocate` to extend the physical file size in large increments (e.g., 256MB) to minimize metadata updates and filesystem fragmentation.
- Continue using `make_sparse` on Windows to avoid synchronous zero-filling on `set_len`.

## Scope & Impact
- **Crate:** `studiofs-cache` (specifically `v2/raw_block_file_backend.rs`).
- **Interfaces:** No changes to the `LocalBlockBackend` trait.
- **Performance:** Expected to significantly increase concurrent read/write throughput on NVMe.
- **Safety:** Append-only recovery logic remains robust; truncated or corrupted tail records during crash are handled by the existing scan logic.

## Verification Plan
1. **Unit Tests:** Run existing `local_block_backend_contract` tests to ensure correctness.
2. **Concurrency Tests:** Add a new test case that performs many parallel `put` and `get` operations to verify no race conditions in index or file length management.
3. **Benchmarks:** Use `studiofs test-cache` to measure throughput before and after the changes. Target: > 1000 MB/s write, > 2000 MB/s read.

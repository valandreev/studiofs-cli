# Read sequential direct-I/O streams ahead into RAM (SFS-879)

This ExecPlan is a living document maintained under `.agent/PLANS.md`. Its Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective sections must be kept current as work proceeds.

## Purpose / Big Picture

Media applications such as AJA System Test and NLE playback read large files sequentially with `F_NOCACHE`. On macOS that bypasses the Unified Buffer Cache (UBC, the kernel page cache), so every byte reaches the StudioFS daemon as a read callback, and macFUSE sends those callbacks one at a time (queue depth 1). Each 4 MiB callback then waits for one local payload-store read on disk before the next callback can be issued. Measured on 2026-10-21 with a 16 GB AJA-like workload on a Kingston XS2000 USB disk, StudioFS read at 716 MB/s with almost no background contention, while the same disk read the same pattern at 976 MB/s without FUSE.

After this change, a macFUSE handle that the bridge opened with `direct_io` and only for reading, and that reads sequentially, has the next chunks of the file read into memory in the background while the application consumes the current chunk. Callbacks that land in an already loaded chunk are served by a memory copy. Several chunk reads are in flight at once, so the disk sees a deeper queue. To see it working, mount a volume on macOS, write a 16 GB file with the AJA-like script and read it back with `F_NOCACHE`; the read rate should rise from about 560 MB/s (716 MB/s when the upload is throttled to one slot) toward the raw-disk ceiling, and the runtime stats file reports read-ahead hits.

## Progress

- [x] (2026-10-21) Traced the read path and the existing read-ahead machinery; recorded the design below.
- [x] (2026-10-21) Milestone 1: `crates/studiofs-vfs/src/vfs/ram_readahead.rs`, `OpenFile::ram_readahead`, invalidation in `clear_read_window`/`set_read_ahead_owner`, `read_into_fh` lookup before the session. Five `ram_readahead_*` VFS tests pass; removing `discard_all` from `clear_read_window` makes `ram_readahead_returns_new_bytes_after_another_handle_writes` fail with stale bytes. The full `studiofs-vfs` lib suite passes (1205 tests).
- [x] (2026-10-21) Milestone 2: `sequential_ram_readahead_bytes` config and `STUDIOFS_SEQUENTIAL_RAM_READAHEAD_BYTES` (zero accepted), macFUSE opt-in for read-only `direct_io` handles with `direct_io_read_only_open_enables_sequential_ram_readahead`, `vfs_read.ram_readahead_*` runtime stats, docs. Clippy on `studiofs-vfs`, `studiofs-macfuse`, `studiofs` is clean.
- [x] (2026-10-21) Milestone 3: mounted macOS runs S12 (16 GB, 32 MiB), S12 2 GB, S13 (16 GB, 16 MiB), S14 (16 GB read while the upload runs). Results are in Outcomes.
- [x] (2026-10-21) Depth sweep at the user's request (the 32 MiB default stalled the upload): 4/8/12/16 MiB during an upload, 8/12 MiB right after the write. Default lowered to 12 MiB with a two-chunk minimum; final runs with the rebuilt default are in Outcomes.

## Surprises & Discoveries

The existing per-handle "read window" in `crates/studiofs-vfs/src/vfs/file_session.rs` (`arm_read_window_prefetch`, `read_window_hit`) only engages when the kernel request is smaller than `FOREGROUND_READ_WINDOW_BYTES` (4 MiB). It batches small reads synchronously; it never overlaps I/O with the application. macFUSE with `F_NOCACHE` sends requests of up to 4 MiB, so the window never engages for the AJA pattern.

The existing prefetcher (`crates/studiofs-vfs/src/vfs/prefetch.rs`, `read_prefetch_batch`, `DataReader::prefetch_only`) only fetches remote objects into the local payload store. Blocks already in the local payload store are skipped, so it does nothing for files that were just written locally. In the S8 run it scheduled 238,812 read-ahead operations while `cache.readahead_useful_bytes` stayed 0.

With read-ahead the disk is shared differently. Reads plus background writes stay at about the disk ceiling, but the read-ahead's deeper queue wins most of the bandwidth: the materialization of the remaining write backlog after the writer closes ran at about 330 MB/s during an S8 read (no read-ahead), about 80 MB/s with a 16 MiB depth (S13), and about 10 MB/s with 32 MiB (S12). The upload starts only after that backlog drains, so in S12/S13 it started after the read finished. When the upload was already running (S14), it dropped from about 86 MB/s (S8) to 19 MB/s while the read ran at 927 MB/s.

A depth of one chunk (4 MiB) is worse than no read-ahead: 379 MB/s against 560 MB/s, with only 473 of 4100 callbacks served from chunks. AJA frames are not chunk-aligned, so most requests need the next chunk as well, miss, and are read again on the normal path while the chunk read wasted disk time.

In S8, the uncontended tail of the read spent about 5.5 ms per read callback, of which about 3.4 ms was local payload I/O. The remaining 2 ms is daemon overhead per request (metadata resolution, prefetch hint planning, buffer allocation, copies). Overlapping I/O hides the disk latency; it does not remove that overhead.

## Decision Log

- Decision: Implement the read-ahead per handle, above the read path, by reading whole future chunks through the same `prepare_foreground_read` + `execute_prepared_foreground_read` path that a normal read uses.
  Rationale: That path already handles the dirty writer overlay, pending local extents, committed local runs, remote fetches, holes and EOF. Reusing it keeps read semantics identical; a payload-store-level cache would have to re-derive the slice graph for future ranges and would not cover dirty overlay data.
  Date/Author: 2026-10-21 / Claude.
- Decision: Look up and wait for read-ahead chunks in `read_into_fh` before the foreground read enters its file-session `Read` permit.
  Rationale: The per-inode session gate is a fair `tokio::sync::RwLock`. If a foreground read held a shared permit while waiting for a chunk task that had not yet acquired its own shared permit, a writer queued between them would block the chunk task, the writer would wait for the foreground permit, and the foreground read would wait for the chunk: a deadlock. Waiting with no permit held cannot form that cycle.
  Date/Author: 2026-10-21 / Claude.
- Decision: A chunk task reads while holding a `Read` session permit and installs its data while still holding it, but only when the handle's read-ahead generation has not changed since the chunk was scheduled. `OpenFile::clear_read_window` (called from `invalidate_attr_read_cache` and the owner switch to `Kernel`) bumps that generation and discards every chunk.
  Rationale: Local writes run under the exclusive `Write` permit and invalidate the inode before returning, so they cannot interleave with a chunk read; foreign metadata application has no session, but every content-affecting path ends in `invalidate_attr_read_cache([ino])`, which either clears an installed chunk or makes the pending install fail its generation check. This matches the repository rule that a result which crossed an invalidation is never cached.
  Date/Author: 2026-10-21 / Claude.
- Decision: Enable the feature per handle, and only from the macFUSE adapter for read-only handles opened with `direct_io`. Linux FUSE, WinFSP and UBC-cached macFUSE handles keep their current behavior.
  Rationale: Those are the handles where the kernel performs no read-ahead and macFUSE delivers queue-depth-1 callbacks. Other bridges and cached handles have kernel read-ahead and were not measured; changing them needs separate evidence.
  Date/Author: 2026-10-21 / Claude.
- Decision: Chunks are 4 MiB and aligned to absolute 4 MiB file offsets; the per-handle depth defaults to 12 MiB (3 chunks) on macOS (originally 32 MiB; see the later decision) and 0 (disabled) elsewhere; a mount-wide budget of 256 MiB bounds the memory held by all handles. Chunk buffers come from a dedicated `BufferPool`.
  Rationale: 4 MiB matches the macFUSE `iosize`/`max_read` and the volume block geometry, so a chunk usually maps to one payload object read. 256 MiB matches LucidLink's `FileStore.MemCacheSize`. Pooled buffers avoid a page-faulting allocation per chunk.
  Date/Author: 2026-10-21 / Claude.

- Decision (superseded below): Keep the 32 MiB default depth even though it defers backlog materialization and slows the upload while a sequential read saturates the disk.
  Rationale: Foreground playback has priority over background publication in this design.
  Date/Author: 2026-10-21 / Claude.
- Decision: Lower the default depth to 12 MiB (3 chunks) and clamp any nonzero depth to at least two chunks.
  Rationale: The user judged the 32 MiB upload stall too strong. In the sweep, 12 MiB kept most of the read gain (863 MB/s during an upload, 837 MB/s right after the write, against 560 MB/s without read-ahead) while the upload kept 58 MB/s (32 MiB: 19 MB/s) and started about 3 s later instead of after the read. 8 MiB left the upload untouched but gained only 13-20%. One chunk is slower than no read-ahead.
  Date/Author: 2026-10-21 / Claude.

## Outcomes & Retrospective

Mounted results on the Mac17,2 test host (macFUSE 5.5.0, payload cache on the Kingston XS2000; raw disk reads 976 MB/s):

    Run                                   Read MB/s   Hits / callbacks   Upload during read
    S8  no read-ahead, 16 GB                560        0 / 4119           86 MB/s
    S12 32 MiB, 16 GB right after write    1151        3885 / 3887        not started
    S13 16 MiB, 16 GB right after write     945        3832 / 3832        not started
    S14 32 MiB, 16 GB while upload runs     927        ~all               19 MB/s
    S7b no read-ahead, 2 GB               5456        -                  -
    S12 32 MiB, 2 GB                     10844        538 / 742          -

Depth sweep (same host; read MB/s / upload MB/s during the read; upload start after the writer closed for the right-after-write case):

    Depth     While the upload runs     Right after the write
    off       560 / 86                  560 / 86, upload at +6.2 s
    4 MiB     379 / 88                  -
    8 MiB     669 / 106                 635 / 84, upload at +5.8 s
    12 MiB    863 / 58                  837 / 32, upload at +9.2 s
    16 MiB    929 / 39                  945 / 0,  upload at +17.8 s
    32 MiB    927 / 19                  1151 / 0, upload after the read

Final runs with the rebuilt 12 MiB default (no override): 853 MB/s read with the upload at 67 MB/s while it runs; 823 MB/s right after the write with the upload starting at +13.0 s; 2 GB right after the write at 10449 MB/s (538 of 742 callbacks from chunks).

No chunk was discarded in any mounted run, and every AJA-like frame read returned its full length. S12 exceeds the raw-disk rate because part of the freshly materialized payload is still in the macOS page cache (the payload store writes buffered) and eight chunk reads overlap. The issue's targets for this case are met: 16 GB read while the upload runs at 95% of the raw-disk ceiling (target 85%) and a 2 GB read right after the write above 3 GB/s.

Remaining: reads of UBC-cached read-only handles with `F_NOCACHE` (a file whose upload has drained) still reach the daemon at queue depth 1 and are not covered; they would need a separate opt-in decision and measurement. The trade-off against background materialization and upload is described in Surprises & Discoveries.

## Context and Orientation

StudioFS is a Rust workspace. The platform-neutral filesystem core is the crate `crates/studiofs-vfs`; its main type is `StudioVfs` in `crates/studiofs-vfs/src/vfs.rs`, split across modules under `crates/studiofs-vfs/src/vfs/`. Platform bridges translate kernel callbacks into `StudioVfs` calls: macOS uses `crates/studiofs-macfuse` (the adapter in `crates/studiofs-macfuse/src/adapter.rs`), Linux uses `crates/studiofs-linux-fuse`, Windows uses `crates/studiofs-winfsp`.

A file handle (`fh`) is an `OpenFile` (defined in `crates/studiofs-vfs/src/vfs/file_session.rs`) kept in the handle table (`crates/studiofs-vfs/src/vfs/handle_table.rs`). `OpenFile` is cheap to clone; its mutable per-handle state lives behind `Arc<Mutex<...>>` fields such as `read_window`.

A read callback arrives at `StudioVfs::read_into_fh` (`crates/studiofs-vfs/src/vfs/sync_ops.rs`). It enters a per-inode "file session" permit of kind `FileSessionOperation::Read` and calls `read_into_impl` (`crates/studiofs-vfs/src/vfs/io_ops.rs`). The file session (`crates/studiofs-vfs/src/vfs/file_session.rs`) is a per-inode fair read/write lock: `Read`, `Materialize` and `Finalize` share it; `Write`, `Flush`, `Fsync`, `Release`, `Upload` and `Delete` are exclusive. `read_into_impl` checks access (`enforce_access_async`), handles ignored entries, calls `prepare_foreground_read` (resolves the inode attributes and the slice graph from the local `redb` metadata) and `execute_prepared_foreground_read` (`crates/studiofs-vfs/src/vfs/read_path.rs`), which builds read segments and assembles bytes from the writer stack (dirty data in memory), the local payload store on disk, or object storage.

"Invalidation" means discarding cached knowledge about an inode after its content or attributes changed. `StudioVfs::invalidate_attr_read_cache` (`crates/studiofs-vfs/src/vfs/workers.rs`) is called by local writes, truncation, local metadata commits, and foreign (peer) metadata application; it calls `HandleTable::clear_read_windows_for_inode`, which calls `OpenFile::clear_read_window` for every handle of that inode.

"Read-ahead owner" (`ReadAheadOwner` in `file_session.rs`) says whether the kernel (`Kernel`) or the VFS (`Vfs`) performs read-ahead for a handle. macFUSE always opens handles with `Vfs`. Switching a handle to `Kernel` clears its read window.

On macOS the adapter decides at open time whether a handle uses `direct_io` (no UBC, no kernel read-ahead). Read-only handles use `direct_io` while the file is locally dirty, actively written, of unknown state, or larger than available memory; write-capable handles always use it. The decision is made in `MacFuseAdapter::open_with_result` in `crates/studiofs-macfuse/src/adapter.rs`.

Runtime statistics are exported through the synthetic root file `.studiofs-runtime-stats`, assembled in `crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs` and documented in `docs/perf/runtime-stats-schema.md`.

## Plan of Work

Milestone 1 adds a new module `crates/studiofs-vfs/src/vfs/ram_readahead.rs` and wires it into `OpenFile` and `read_into_fh`.

The module defines `RamReadAheadState`, held by `OpenFile` in a new `ram_readahead: Arc<Mutex<RamReadAheadState>>` field. It records whether the handle is enabled, the per-handle depth in bytes, a `generation` counter, the end offset of the last served read, a sequential-read streak, the file size last observed, and an ordered `VecDeque` of chunks. A chunk has a start offset and an `Arc<RamChunkSlot>`. The slot owns a `tokio::sync::watch` sender of `RamChunkState` (`Pending`, `Ready(Bytes)`, `Discarded`) and the budget permit (`tokio::sync::OwnedSemaphorePermit`) that accounts for its memory. `OpenFile::clear_read_window` also bumps the generation, marks every slot `Discarded`, and empties the deque.

`StudioVfs` gains a `sequential_ram_readahead: RamReadAheadRuntime` field holding the configured per-handle depth, the mount-wide `Arc<Semaphore>` budget (one permit per chunk), the chunk `BufferPool`, and counters.

`read_into_fh` changes its receiver to `self: &Arc<Self>` (every bridge already calls it on an `Arc<StudioVfs>`). Before entering the session it looks the handle up; when RAM read-ahead is enabled it calls `read_from_ram_readahead`. That function enforces read access exactly like `read_into_impl`, then finds the chunks that cover the request. For a pending chunk it waits on the watch channel without holding any lock or permit. If every needed chunk is `Ready`, it copies the bytes (a short read is returned only when the last chunk is shorter than 4 MiB, which only happens at the end of the file), records the read as a VFS read call, updates the handle's access tracker and read progress, records foreground read telemetry, and returns. Otherwise it returns `None` and the normal path runs unchanged. After either path succeeds, `schedule_ram_readahead` updates the streak and schedules chunks.

Scheduling only runs for enabled handles. A read whose offset equals the previous end extends the streak; any other offset resets the streak to one and discards all chunks. Chunks whose end is at or before the end of the served read are dropped. With a streak of at least two, chunk indices from `read_end / CHUNK` up to `read_end / CHUNK + depth_chunks` that are not yet queued and start before the observed file size are scheduled, each only if a budget permit is available. A chunk task (spawned with `track_background_task`, skipped after shutdown starts) enters a `Read` session, re-checks the generation, calls `prepare_foreground_read` and `execute_prepared_foreground_read` with `fh = None` into a pooled buffer, and, still holding the session, installs `Ready(bytes)` if the generation is unchanged and the slot is still queued; otherwise, or on any error, it sets `Discarded`.

Milestone 1 tests live in `crates/studiofs-vfs/src/vfs/tests.rs` and use `make_test_vfs`. They write a multi-chunk file, open a read-only handle, enable RAM read-ahead on it, and read sequentially in 4 MiB requests, asserting that later requests are served from chunks (hit counter increases) with byte-exact content; that a write through another handle after chunks were loaded makes the next read return the new bytes; that a seek discards chunks and still returns correct data; that EOF in the last chunk returns a short read with exact bytes; and that a handle without the opt-in never schedules chunks.

Milestone 2 adds `sequential_ram_readahead_bytes` to `VfsConfig` and `VfsConfigRaw` in `crates/studiofs-vfs/src/vfs/config.rs` (default 12 MiB when `cfg!(target_os = "macos")`, otherwise 0), carries it through `crates/studiofs-vfs/src/vfs/bootstrap.rs`, parses `STUDIOFS_SEQUENTIAL_RAM_READAHEAD_BYTES` in `crates/studiofs/src/cli/mount/session.rs`, adds `StudioVfs::enable_sequential_ram_readahead(fh)`, calls it from `open_with_result` in the macFUSE adapter for read-only `direct_io` handles, exports counters (`ram_readahead_scheduled_chunks`, `ram_readahead_loaded_bytes`, `ram_readahead_hit_reads`, `ram_readahead_hit_bytes`, `ram_readahead_wait_reads`, `ram_readahead_discarded_chunks`, `ram_readahead_budget_skips`) in the `vfs_read` section of the runtime stats, and documents the behavior in `docs/platform-bridges.md`, `docs/perf/runtime-stats-schema.md` and `docs/local-first-validation-matrix.md`.

Milestone 3 rebuilds the release binary, mounts the `test` volume, and runs `target/perf/macos-read-cache/run_sampled.sh` (16 GB) and `run_variant.sh` (2 GB) as in the S7/S8 runs, comparing read throughput and the new counters with S7b and S8.

## Concrete Steps

From the repository root `<repo-root>`:

    cargo test -p studiofs-vfs --lib ram_readahead -- --nocapture
    cargo test -p studiofs-vfs --lib read_window -- --nocapture
    cargo test -p studiofs-vfs --lib read_after -- --nocapture
    cargo test -p studiofs-macfuse --lib open -- --nocapture
    cargo clippy -p studiofs-vfs -p studiofs-macfuse -p studiofs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

For the mounted check (macOS, volume `test` configured locally):

    cargo build --release -p studiofs
    target/perf/macos-read-cache/remount.sh
    target/perf/macos-read-cache/run_sampled.sh S12-ram-readahead-16g 1077 random
    target/perf/macos-read-cache/run_variant.sh S12-ram-readahead-2g 135 random

## Validation and Acceptance

The new VFS tests fail to compile or fail before Milestone 1 and pass after it. On the mounted 16 GB run, `vfs_read.ram_readahead_hit_reads` is a large fraction of `bridge.read_callbacks`, no read returns wrong data (the AJA-like script asserts full reads; a separate byte check is in the tests), and the read throughput exceeds the S8 baseline of 560 MB/s. The 2 GB run must not regress against S7b (2.9–5.5 GB/s).

## Idempotence and Recovery

All changes are additive and gated per handle. Setting `STUDIOFS_SEQUENTIAL_RAM_READAHEAD_BYTES=0` at mount time disables the feature without a rebuild. Benchmark files are deleted by the scripts; rerunning them is safe.

## Artifacts and Notes

Baseline evidence (2026-10-21, `target/perf/macos-read-cache/SUMMARY.md`): S8 default 16 GB read 560 MB/s (569 MB/s during upload, 445 MB/s while the write backlog materializes); S11 with the upload limited to one slot 677 MB/s overall and 716 MB/s during upload; raw disk 976 MB/s.

## Interfaces and Dependencies

In `crates/studiofs-vfs/src/vfs/ram_readahead.rs`:

    pub(super) const RAM_READAHEAD_CHUNK_BYTES: usize = 4 * 1024 * 1024;
    pub(super) struct RamReadAheadState { ... }
    pub(super) struct RamReadAheadRuntime { ... }

In `StudioVfs`:

    pub fn enable_sequential_ram_readahead(&self, fh: u64) -> bool;
    pub async fn read_into_fh(self: &Arc<Self>, fh: u64, ino: Ino, offset: u64, buffer: &mut [u8]) -> VfsResult<u32>;

No new external crates are needed; `tokio::sync::watch` and `tokio::sync::Semaphore` are already available.

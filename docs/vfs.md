# VFS Layer

This page describes the `studiofs-vfs` responsibilities under the canonical
mounted-IO contract in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).

The VFS (`studiofs-vfs`) is the platform-agnostic core of StudioFS. It
orchestrates metadata operations, the JuiceFS-equivalent cached-store data
plane, background tasks, and the local-first visibility rules consumed by the
FUSE and WinFSP bridges.

Active invariants:
- `write()` copies bytes into file/chunk/slice writer state and returns after
  local acceptance.
- Same-mount reads observe accepted writes immediately through writer state.
- Frozen slices finish through `CachedStore` staging and local slice metadata
  commit.
- `flush()` / `fsync()` / ordinary `close()` / last-handle release force only
  the required local slice/cache boundary.
- Remote visibility is file-version atomic and advances only after background
  upload plus replication/apply succeed.

## Core Struct: `StudioVfs`

| Field | Type | Purpose |
|-------|------|---------|
| `meta` | `MetaExecutor<dyn VfsMeta>` | Typed metadata executor used by `run_meta(...)` |
| `data_write` | `Arc<DataStore>` | S3 write/delete path |
| `local_payload_backend` | `Arc<dyn LocalPayloadBackend>` | Local cache backend used by the cached-store pipeline |
| `cached_store` | `Arc<CachedStore>` | Writer-page, staging, read-cache, pending-upload, upload-helper, and pinning owner |
| `fetch_scheduler` | `Arc<FetchScheduler>` | Adaptive S3 fetcher |
| `readahead` | `Arc<ReadAheadTracker>` | Sequential prefetch |
| `dir_prefetch` | `Arc<DirAccessTracker>` | Directory-level prefetch |
| `state` | `Arc<VfsState>` | Writer/cache pressure, namespace caches, file sessions, locks, and metrics |

`local_payload_backend()` remains the accessor used by helper modules that need
the backend boundary without reaching through backend-specific internals.

## Lifecycle

`StudioVfs` startup is explicit and two-phase:

1. `StudioVfs::new(...)` builds state only.
2. `vfs.start()` starts background workers (upload, prefetch, janitor,
   checkpoint loop, and related tasks).

Repeated `start()` calls are idempotent after the instance is running.

## Contract/Core Test Harness

`studiofs_vfs::contract_harness::ContractVfsFixture` creates a fast local
`StudioVfs` fixture backed by an isolated `RedbMetaEngine` and local cache
directory. It can reopen both metadata and VFS handles against the same local
paths and delegates shared invariant checks to
`studiofs_meta::contract_harness`.

This layer is for core API scenarios that must stay independent of Docker,
PostgreSQL, RustFS, FUSE, and WinFSP. Mounted bridge, infra-backed, randomized,
and chaos coverage belongs in the existing higher test layers.

## ACL Enforcement

VFS enforces effective ACL roles per principal:
- `no_access` hides entries from `lookup`/`readdir`
- `ro` allows reads and metadata reads, denies mutating operations
- `rw` allows full access

Rules are inherited along the parent chain and resolved by first match from the
inode toward root; no match defaults to `rw`.

## VfsConfig

| Field | Default | Description |
|-------|---------|-------------|
| `meta_path` | — | Path to redb database |
| `cache_dir` | — | Root for the local cache backend and runtime state |
| `cache_size` | 100 GiB | Local cache size budget |
| `block_size` | 16 MiB | Chunk size (cloud transfer currently uses 4x4 MiB fragments) |
| `max_concurrent_uploads` | 4 | Upload worker concurrency |
| `readahead_width` | 18 | Base prefetch depth |
| `max_dirty_bytes` | 300 MiB | JuiceFS-sized writer/cache dirty-byte limit before backpressure |
| `upload_stats_interval_secs` | 1 | Upload control-loop tick interval |
| `upload_fg_read_latency_target_ms` | 35 | Foreground-read P90 target for upload throttling |
| `upload_fg_read_latency_window_ms` | 1500 | Foreground-read speedometer sliding window |
| `upload_fg_read_latency_sticky_windows` | 2 | Keep last observed P90 for this many windows when samples pause |

Local data-path QoS:
- foreground reads/writes use the foreground local-data semaphore
- upload rereads and cache eviction use the background local-data semaphore
- cache-only delete/rename cleanup stays synchronous at the API boundary, while
  internal payload-object eviction is batched through the background pool

## Write Path

### 1. `write(ino, offset, data)`

```text
data arrives
  │
  ├─ Split by chunk boundary
  ├─ Append/copy into writable slice pages
  ├─ Update writer-visible length and dirty slice state
  └─ Return after local acceptance unless dirty-byte backpressure must slow the writer
```

Handle-aware write path:
- `write_fh(fh, ino, offset, data)` buffers data identically to `write()`.
- `flush_fh()` freezes/finishes dirty slices needed for that handle or inode and
  surfaces local errors.
- `fsync()` is the explicit caller-requested crash-safe local boundary.
- Ordinary writeback does not wait for object upload, PostgreSQL replication, or
  peer publication.
- Foreground backpressure is driven by writer/cache dirty-byte pressure; upload
  backlog alone does not stall writes.

### 2. `flush(ino)` / `fsync(ino)` / `close(ino)`

```text
dirty local state for this inode
  │
  ├─ Freeze dirty slice writers
  ├─ Finish slices through cached-store staging
  ├─ Commit local slice metadata + attrs
  ├─ Sync local cache/metadata when the operation requires crash-safety
  └─ Leave upload/publication in the background
```

Local durability notes:
- Accepted bytes may live only in writer pages until a local boundary freezes
  and finishes the relevant slices.
- When a barrier finishes multiple pending slices for one chunk, VFS commits
  them in deterministic creation order, with chunk-local offset ordering as
  the tie-breaker for equal creation sequence values.
- `fsync()` requests the strict local sync boundary directly.
- Explicit `flush()` and last-handle `close()` / `release()` also drain the
  writer state they own, without pulling remote publication into the foreground.
- Reserved object IDs make retrying the same slice/object commit idempotent
  across metadata failures.

## Read Resolution

Foreground reads resolve data in this order:

1. active writer state for the current mount/handle
2. local slice metadata in `redb`
3. cached-store local payload/cache
4. object-store cold fetch for known published payload objects
5. zero-fill for holes

The chosen slice view sources payload bytes from the cached store first and
object storage only when needed.

When a foreground read spans multiple locally-missing published payload objects,
the read path primes those cold remote blocks concurrently before final buffer
assembly. The fetch scheduler still owns singleflight deduplication, AIMD
admission, foreground/background reserve slots, and transit-memory accounting,
so background prefetch cannot consume the foreground read capacity.
Single-block foreground cold reads intentionally remain on the legacy on-demand
assembly path; there is no parallelism to gain, and the direct path keeps the
common 4 MiB callback behavior stable.
For 4 MiB single-block sequential reads, the foreground path queues the
next-read hint before waiting on the current cold fetch; the hint state keeps a
merged range for duplicate suppression, but the prefetch worker receives the
actual current read range so the sequential detector sees forward progress.

Per-handle access tracking feeds prefetch and cache-promotion decisions so a
random reader on one handle does not suppress sequential behavior on another
handle for the same inode.

## File Session Ordering

- Active regular-file transitions are owned by a per-inode file-session gate in
  VFS core.
- `read` uses a shared session lock.
- `write`, `flush`, `fsync`, and `release` use an exclusive session lock.
- Background stages (`local persist`, `upload`, `finalize`) publish explicit
  per-inode transition events through the same file-session state.

## Cached-Store Upload Worker

The upload worker consumes from the cached-store pending-upload queue:

```text
finished staged payload queued
  │
  ├─ Acquire local-data I/O permits only for the local payload-read phase
  ├─ Read payload from writer/stage cache, upload helper, or local backend fallback
  ├─ Split each chunk into cloud fragments
  ├─ Upload payload objects to S3
  ├─ Mark payloads uploaded only after remote success
  └─ Requeue with backoff on failure
```

Upload is the payload data plane only. Publication of a new file version is a
separate control-plane step gated by verified payload readiness.

## Background Epoch Finalizer

The epoch finalizer publishes file versions after upload completion:

```text
queued (ino, content_epoch)
  │
  ├─ Skip/requeue if the inode still has newer local dirty state
  ├─ Read the current payload-object list for that epoch from metadata
  ├─ Wait until every referenced object is remote-visible
  └─ Publish only if metadata still matches the verified payload-object list
```

Why this split exists:
- `flush()` / `fsync()` / `close()` stay focused on local cache and metadata
  boundaries
- foreground writeback does not synchronously publish whole file versions
- publication stays conditional on verified metadata/object alignment

## Deleted-Inode Cancellation Behavior

- VFS tracks unlinked inodes in memory and cancels queued/in-flight uploads via
  `CancellationToken`.
- For `open-unlink`, upload cancel is soft: pending upload queue entries are
  canceled, but locally durable data remains readable until the last handle
  closes.

## Adaptive Upload Control

- workers use cooperative yielding (`tokio::task::yield_now()`) to avoid
  starving foreground tasks
- upload backpressure uses a foreground-read P90 speedometer on each control
  tick
- pressure increases local upload I/O slot cost and adds a decaying scheduler
  sleep penalty before local rereads begin

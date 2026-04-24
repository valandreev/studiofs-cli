# VFS Layer

This page is the current implementation snapshot for `studiofs-vfs`. It is not
the canonical SFS-242 rewrite contract. The rewrite source of truth lives in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).

The VFS (`studiofs-vfs`) is the platform-agnostic core of StudioFS. It
orchestrates metadata operations, local cache access, background tasks, and the
Local-First visibility rules consumed by the FUSE and WinFSP bridges.

Active invariants:
- `write()` makes bytes visible immediately through writer state.
- `flush()` / ordinary `close()` / last-handle release materialize accepted
  bytes into the local authority view without waiting for remote upload or
  replication.
- `fsync()` is the explicit strict request for a crash-safe local sync
  boundary.
- Same-mount reads observe accepted writes immediately through writer state.
- Remote visibility is file-version atomic and advances only after background
  upload plus replication/apply succeed.

## Core Struct: `StudioVfs`

| Field | Type | Purpose |
|-------|------|---------|
| `meta` | `MetaExecutor<dyn VfsMeta>` | Typed metadata executor used by `run_meta(...)` |
| `data_write` | `Arc<DataStore>` | S3 write/delete path |
| `local_payload_backend` | `Arc<dyn LocalPayloadBackend>` | Current local cache backend for committed payloads and upload rereads |
| `cached_store` | `Arc<CachedStore>` | Local-durable payload runtime, pending-upload queue, upload helpers, and pinning |
| `fetch_scheduler` | `Arc<FetchScheduler>` | Adaptive S3 fetcher |
| `readahead` | `Arc<ReadAheadTracker>` | Sequential prefetch |
| `dir_prefetch` | `Arc<DirAccessTracker>` | Directory-level prefetch |
| `state` | `Arc<VfsState>` | Local dirty state, namespace caches, file sessions, and backpressure |

`local_payload_backend()` remains the accessor used by helper modules that need
the backend boundary without reaching through backend-specific internals.

## Lifecycle

`StudioVfs` startup is explicit and two-phase:

1. `StudioVfs::new(...)` builds state only.
2. `vfs.start()` starts background workers (upload, prefetch, janitor,
   checkpoint loop, and related tasks).

Repeated `start()` calls are idempotent after the instance is running.

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
| `block_size` | 16 MiB | Chunk size (cloud transfer currently uses 4x4 MiB blocks) |
| `max_concurrent_uploads` | 4 | Transitional static upload knob |
| `readahead_width` | 18 | Base prefetch depth |
| `max_dirty_bytes` | 1200 MiB | Local dirty-state memory limit before backpressure |
| `upload_stats_interval_secs` | 1 | Upload control-loop tick interval |
| `upload_fg_read_latency_target_ms` | 35 | Foreground-read P90 target for upload throttling |
| `upload_fg_read_latency_window_ms` | 1500 | Foreground-read speedometer sliding window |
| `upload_fg_read_latency_sticky_windows` | 2 | Keep last observed P90 for this many windows when samples pause |

Local data-path QoS:
- foreground reads/writes use the foreground local-data semaphore
- upload rereads and cache eviction use the background local-data semaphore
- cache-only delete/rename cleanup stays synchronous at the API boundary, while
  internal block eviction is batched through the background pool

## Write Path

### 1. `write(ino, offset, data)`

```text
data arrives
  │
  ├─ Split into chunk-aligned ranges
  ├─ Store/merge in handle-owned writer state
  ├─ Update local inode size and pending content epoch
  └─ If enough full aligned chunks accumulate, VFS may eagerly persist the
    coalesced batch through local backend -> sync_data() -> metadata
```

Handle-aware write path:
- `write_fh(fh, ino, offset, data)` buffers data identically to `write()`.
- `flush_fh()` runs the foreground local materialization step for the inode and
  then surfaces any sticky async-upload error for that inode.
- The canonical mount contract is single-mode Fast Local Authority: ordinary
  `flush()` and last-handle `close()` / `release()` become strict local
  crash-safe durability barriers whenever dirty unpublished state remains.
  `fsync()` remains the explicit caller-requested strict local-durability API,
  but it is no longer the only callback path that reaches that boundary. The
  legacy `cache_safety_mode=fast|durable` knob has been removed.
- Foreground backpressure is driven by local dirty-state pressure; upload
  backlog alone does not stall writes.

### 2. `flush(ino)` / `fsync(ino)` / `close(ino)`

```text
dirty local state for this inode
  │
  ├─ Take a point-in-time snapshot of accepted bytes
  ├─ Reserve/reuse per-block object IDs
  ├─ local backend `put(...)` committed payloads locally
  ├─ local backend `sync_data()` for explicit `flush()`, `fsync()`, or last-handle `close()` / `release()` when dirty unpublished state remains
  ├─ Persist local unpublished slices + attrs in metadata
  └─ Enqueue uploads for the committed payloads
```

Local durability notes:
- Accepted bytes stay in local dirty state until the local backend ->
  metadata sequence succeeds.
- `fsync()` is the explicit API that requests the strict local `sync_data()`
  boundary directly, but explicit `flush()` and last-handle
  `close()` / `release()` also reach that boundary when dirty unpublished
  state still exists.
- Reserved object IDs make retrying the same dirty block idempotent across
  metadata failures.
- Historical `ingress_spill` directories are outside the active Local-First
  contract and are deleted during startup cleanup.

## Read Resolution

Foreground reads resolve data in this order:

1. active writer state for the current mount/handle
2. durable local unpublished state
3. published state
4. zero-fill for holes

The chosen slice view then sources payload bytes from the local backend first
and object storage only when needed.

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

## Background Upload Worker

The upload worker consumes from the cached-store pending-upload queue:

```text
persisted payload queued
  │
  ├─ Acquire local-data I/O permits only for the local payload-read phase
  ├─ Read payload from hot upload buffer, upload hardlink, or local backend fallback
  ├─ Split each chunk into cloud blocks
  ├─ Upload block objects to S3
  ├─ Mark payloads uploaded only after remote success
  └─ Requeue with backoff on failure
```

Upload is the payload data plane only. Publication of a new file version is a
separate control-plane step gated by durable metadata lifecycle
(`PublishReady`).

## Background Epoch Finalizer

The epoch finalizer publishes file versions after upload completion:

```text
queued (ino, content_epoch)
  │
  ├─ Skip/requeue if the inode still has newer local dirty state
  ├─ Read the current block list for that epoch from metadata
  ├─ Wait until every referenced object is remote-visible
  └─ Publish only if metadata still matches the verified block list
```

Why this split exists:
- `flush()` / `close()` stay focused on local durability
- buffered foreground I/O does not synchronously publish whole file versions
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

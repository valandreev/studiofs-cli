# Architecture Overview

## System Design

StudioFS mounts S3-compatible object storage as a local filesystem with a
local-first chunk/slice/block data plane and eventual multi-client convergence.

Core principles:
- Foreground metadata operations read and write local `redb`; foreground payload durability flows through the local block backend.
- PostgreSQL is the shared replication bus and checkpoint store, not the hot read path.
- File payloads live in immutable blocks addressed by chunk-relative slices; each 16 MiB chunk currently fans out into four 4 MiB block objects.
- Runtime file visibility is split into local-visible overlay state and remote-visible base state.
- Regular files now use chunk-local overlay/base slice graphs in metadata, not a flat block table.
- Cache 3.0 (`studiofs-cache::v3`) is the active local data store; remaining cache helpers are a narrow upload-orchestration layer.
- The canonical chunk/slice/block vocabulary for follow-up work is frozen in
  [chunk-slice-block-vocabulary.md](./chunk-slice-block-vocabulary.md).
- The execution guardrails and implementation order for the data plane redesign are
  captured in
  [data-plane-redesign-guardrails.md](./data-plane-redesign-guardrails.md).

- `flush()` and `close()` must make accepted data readable locally without waiting for S3/upload/metadata.
- Remote readers observe only coarse states: `uploading`, `updating`, `ready`.
- Remote visibility switches atomically by file incarnation, not block-by-block.

## Workspace Graph

```
studiofs (CLI)
  ├── studiofs-meta       local metadata + PostgreSQL delta sync
  ├── studiofs-data       S3 object I/O + object format
  ├── studiofs-cache      local block backend + upload queue orchestration
  ├── studiofs-vfs        filesystem core
  ├── studiofs-fuse       Linux/macOS bridge
  └── studiofs-winfsp     Windows bridge
```

## Metadata Architecture

`PgMetaEngine` currently works like this:
- bootstrap: load the latest remote-visible checkpoint from `metadata_state`
- steady-state sync: exchange only ordered delta events through `event_log`
- local client view: serve merged `overlay over base` from local `redb` at chunk/slice granularity
- restart recovery: rebuild pending replication state from the local event-log
  tail when unpublished local overlay mutations survived a crash
- foreign visibility: eventual, driven by background `flush_metadata()` / `sync_metadata()`

Core invariants:
- local `size` may be ahead of remote-visible `committed_size`
- local pending incarnation data may be ahead of remote-visible committed manifest
- remote-visible manifest switches only after full pending incarnation publish
- local echoed PostgreSQL batches are applied only to remote-visible/base state reconciliation and do not clobber local overlay
- regular-file overlay state may be ahead of remote-visible `regular_file_base`
- local regular-file overlay precedence is `overlay over base` at the chunk/slice level
- ordinary restart/reopen should recover pending metadata convergence without
  requiring local metadata wipe + `discover`
- foreign namespace/attr/symlink/xattr apply lands in local `base_*`
- local mutations remain in overlay tables (`inodes`, `dentries`, `symlinks`, `xattrs`, `regular_file_overlay`)
- local tombstones hide remote-visible base state for dentries, symlinks, and xattrs

## High-Level Mutation Flow

1. App calls a VFS mutation.
2. StudioFS updates local metadata in `redb`.
3. File payload lands in the per-file `WriteBuffer`; full chunk-aligned writes may be eagerly persisted to the local block backend and extent metadata.
4. `flush(ino)` / `fsync(ino)` / `close(ino)` drain dirty `WriteBuffer` chunks through coalesced local backend `put(...) -> sync_data() -> metadata` batches, then enqueue uploads.
5. Background workers upload each committed chunk as four independent block objects.
6. Upload completion advances pending incarnations; remote visibility is updated only on successful incarnation publish.
7. Metadata background flush projects committed incarnation and `upload_state` to PostgreSQL `event_log`.
8. Other clients pull only newer foreign batches and apply remote-visible namespace/attrs/symlinks/xattrs to local `base_*`.
9. `metadata_state` stores periodic checkpoints for bootstrap and `discover`.

## Write Path

1. Split incoming data into chunks.
2. Merge bytes into the per-file `WriteBuffer`.
3. Full chunk-aligned writes may be eagerly committed locally through the local block backend + metadata, while partial tails stay in RAM.
4. `flush()` / `fsync()` / `close()` transactionally persist remaining dirty chunks and attrs via coalesced local backend `put(...) -> sync_data() -> metadata -> upload queue` batches.
5. On publish, update the remote committed incarnation and `committed_size`.

This lets the local client read accepted data immediately, while remote clients
still observe committed incarnations as they become published.

## Read Path

Reads prefer local state in this order:
1. `WriteBuffer`
2. committed local extents / committed file blocks backed by the local block backend
3. compatibility cache shim
4. S3 fetch

Historical ingress artifacts note:
- `cache_safety_mode=durable` still preserves ingress checkpoint/state metadata
  for startup recovery of legacy ingress artifacts.
- `cache_safety_mode=fast` discards those artifacts during startup self-heal.

Metadata lookups are served from local `redb`, where local staged blocks
override committed blocks, and local overlay attrs/dentries/symlinks/xattrs override the remote-visible base view.

## Concurrency Model

- Tokio for async upload/fetch/background work
- local locks for VFS/cache hot structures
- PostgreSQL transaction order via `batch_version` for cross-client metadata ordering
- overlay-scoped conflict handling for foreign batch auxiliary application (`inode attrs`, staged blocks, symlink targets, xattr keys)

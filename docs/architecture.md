# Architecture Overview

## System Design

StudioFS mounts S3-compatible object storage as a local filesystem with a
local-first metadata path and eventual multi-client convergence.

Core principles:
- Foreground filesystem operations read and write local `redb` only.
- PostgreSQL is the shared replication bus and checkpoint store, not the hot read path.
- File payloads live in immutable S3 objects; each 16 MiB logical block is stored as four 4 MiB cloud fragments.
- Runtime file visibility is split into local accepted state and remote-visible
  committed state.
- RAM + NVMe cache tiers hide most object-store latency.

- `flush()` and `close()` must make accepted data readable locally without waiting for S3/upload/metadata.
- Remote readers observe only coarse states: `uploading`, `updating`, `ready`.
- Remote visibility switches atomically by file incarnation, not block-by-block.

## Workspace Graph

```
studiofs (CLI)
  ├── studiofs-meta       local metadata + PostgreSQL delta sync
  ├── studiofs-data       S3 object I/O + object format
  ├── studiofs-cache      L1/L2 cache + upload journal/queue
  ├── studiofs-vfs        filesystem core
  ├── studiofs-fuse       Linux/macOS bridge
  └── studiofs-winfsp     Windows bridge
```

## Metadata Architecture

`PgMetaEngine` currently works like this:
- bootstrap: load the latest remote-visible checkpoint from `metadata_state`
- steady-state sync: exchange only ordered delta events through `event_log`
- local client view: serve merged `overlay over base` from local `redb`
- restart recovery: rebuild pending replication state from the local event-log
  tail when unpublished local overlay mutations survived a crash
- foreign visibility: eventual, driven by background `flush_metadata()` / `sync_metadata()`

Core invariants:
- local `size` may be ahead of remote-visible `committed_size`
- local pending incarnation data may be ahead of remote-visible committed manifest
- remote-visible manifest switches only after full pending incarnation publish
- local echoed PostgreSQL batches are applied only to remote-visible/base state reconciliation and do not clobber local overlay
- ordinary restart/reopen should recover pending metadata convergence without
  requiring local metadata wipe + `discover`
- foreign namespace/attr/symlink/xattr apply lands in local `base_*`
- local mutations remain in overlay tables (`inodes`, `dentries`, `symlinks`, `xattrs`)
- local tombstones hide remote-visible base state for dentries, symlinks, and xattrs

## High-Level Mutation Flow

1. App calls a VFS mutation.
2. StudioFS updates local metadata in `redb`.
3. File payload enters local write staging (`pending_blocks`, `dirty_mem`, ingress live index backed by `ingress_spill/segments/`, cache).
4. `flush(ino)`/`close(ino)` applies the configured cache safety policy:
   - `durable`: appends accepted records to ingress (durable local accept path),
   - `fast`: skips ingress payload persistence and relies on best-effort self-heal on restart.
   - ingress implementation details live in `crates/studiofs-vfs/src/ingress_wal.rs`.
5. Background workers materialize accepted records and upload each logical block as four independent cloud fragments.
6. Upload completion advances pending incarnations; remote visibility is updated only on successful incarnation publish.
7. Metadata background flush projects committed incarnation and `upload_state` to PostgreSQL `event_log`.
8. Other clients pull only newer foreign batches and apply remote-visible namespace/attrs/symlinks/xattrs to local `base_*`.
9. `metadata_state` stores periodic checkpoints for bootstrap and `discover`.

## Write Path

1. Split incoming data into blocks.
2. Stage bytes locally in memory and, if needed, NVMe spill.
3. Track staged data in pending incarnation state.
4. On `flush()` / `close()`, ingress behavior depends on `cache_safety_mode`:
   - `durable` appends accepted records,
   - `fast` avoids duplicate ingress payload writes.
5. Background materialization builds and updates pending manifests as needed.
6. On publish, update remote committed incarnation and `committed_size`.

This lets the local client read accepted data immediately, while remote clients
still observe committed incarnations as they become published.

## Read Path

Reads prefer local state in this order:
1. `pending_blocks`
2. `dirty_mem`
3. ingress live index (`ingress_spill/segments/`, `durable` mode)
4. cache (L1/L2)
5. S3 fetch

Ingress WAL metadata format:
- Checkpoint/state metadata is persisted in binary `postcard` format with file-level
  magic headers (`IWC1` / `IWS1`).
- Recovery accepts legacy JSON metadata files for backward compatibility.

Metadata lookups are served from local `redb`, where local staged blocks
override committed blocks, and local overlay attrs/dentries/symlinks/xattrs override the remote-visible base view.

## Concurrency Model

- Tokio for async upload/fetch/background work
- local locks for VFS/cache hot structures
- PostgreSQL transaction order via `batch_version` for cross-client metadata ordering
- overlay-scoped conflict handling for foreign batch auxiliary application (`inode attrs`, staged blocks, symlink targets, xattr keys)

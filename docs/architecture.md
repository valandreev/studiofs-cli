# Architecture Overview

## System Design

StudioFS mounts S3-compatible object storage as a local filesystem with a
local-first metadata path and eventual multi-client convergence.

Core principles:
- Foreground filesystem operations read and write local `redb` only.
- PostgreSQL is the shared replication bus and checkpoint store, not the hot read path.
- File payloads live in immutable S3 objects.
- Runtime file visibility is split into local state and remote-visible committed state.
- RAM + NVMe cache tiers hide most object-store latency.

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
- foreign visibility: eventual, driven by background `flush_metadata()` / `sync_metadata()`

Important invariants:
- local `size` may be ahead of remote-visible `committed_size`
- local staged `file_blocks` may be ahead of remote-visible `committed_file_blocks`
- block data becomes remote-visible only after successful S3 upload completion
- local echoed PostgreSQL batches are applied only to remote-visible/base state reconciliation and do not clobber local overlay
- foreign namespace/attr/symlink/xattr apply lands in local `base_*`
- local mutations remain in overlay tables (`inodes`, `dentries`, `symlinks`, `xattrs`)
- local tombstones hide remote-visible base state for dentries, symlinks, and xattrs

## High-Level Mutation Flow

1. App calls a VFS mutation.
2. StudioFS updates local metadata in `redb`.
3. File payload enters local write staging (`pending_blocks`, `dirty_mem`, ingress live index backed by `ingress_spill/segments/`, cache).
4. Background upload writes the object to S3.
5. Upload completion promotes the block to remote-visible committed state.
6. Metadata background flush projects remote-visible deltas to PostgreSQL `event_log`.
7. Other clients pull only newer foreign batches and apply remote-visible namespace/attrs/symlinks/xattrs to local `base_*`.
8. `metadata_state` stores periodic checkpoints for bootstrap and `discover`.

## Write Path

1. Split incoming data into blocks.
2. Stage bytes locally in memory and, if needed, NVMe spill.
3. Publish staged block refs only to local metadata.
4. Queue upload journal entries with `content_epoch`.
5. After S3 upload success, commit the block into `committed_file_blocks`.
6. Advance `committed_size` only as blocks become committed.

This lets the local client see writes immediately while remote clients see a
growing committed view.

Ingress follow-up:
- concrete first-slice scope is frozen in
  [`nvme-ingress-first-slice.md`](./nvme-ingress-first-slice.md)

## Read Path

Reads prefer local state in this order:
1. `pending_blocks`
2. `dirty_mem`
3. ingress live index (`ingress_spill/segments/`)
4. cache (L1/L2)
5. S3 fetch

Metadata lookups are served from local `redb`, where local staged blocks
override committed blocks, and local overlay attrs/dentries/symlinks/xattrs override the remote-visible base view.

## Concurrency Model

- Tokio for async upload/fetch/background work
- local locks for VFS/cache hot structures
- PostgreSQL transaction order via `batch_version` for cross-client metadata ordering
- overlay-scoped conflict handling for foreign batch auxiliary application (`inode attrs`, staged blocks, symlink targets, xattr keys)

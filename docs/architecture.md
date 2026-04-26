# Architecture Overview

The canonical mounted-IO contract lives in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).
This page summarizes the same architecture at system level.

## System Design

StudioFS mounts S3-compatible object storage as a local-first filesystem with a
JuiceFS-equivalent, Rust-native chunk/slice/payload-object data plane.

Core principles:
- Foreground metadata operations read and write local `redb`; the local `redb`
  view is the only metadata authority used by mounted foreground IO.
- PostgreSQL is the bootstrap source, ordered replication bus, checkpoint
  store, and shared-lock authority when cross-client file locks are enabled.
- File payloads live in immutable payload objects addressed by chunk-relative slices.
- Runtime visibility is split into writer state, the local metadata view, and
  asynchronously replicated shared state.
- The UUID-scoped cache root itself is the runtime boundary of the current
  local cache backend.
- Remote visibility switches atomically at the file-version boundary after
  payload upload, metadata publication, and peer apply.

## Workspace Graph

```text
studiofs (CLI)
  ├── studiofs-meta       local metadata + PostgreSQL delta sync
  ├── studiofs-data       S3 object I/O + object format
  ├── studiofs-cache      local cache backend + upload runtime
  ├── studiofs-vfs        filesystem core
  ├── studiofs-fuse       Linux/macOS bridge
  └── studiofs-winfsp     Windows bridge
```

## Metadata Architecture

`PgMetaEngine` keeps a local metadata view and a replication path:

- bootstrap loads the latest replicated checkpoint from `metadata_state`
- steady-state sync exchanges ordered delta events through `event_log`
- local reads resolve against local `redb` only, then converge toward newer
  foreign state through invalidation and replay
- crash recovery rebuilds pending local state from local metadata and
  upload/replication bookkeeping
- each discovered client reserves one durable per-volume local inode window
  during bootstrap/discover and later mounts reuse that persisted window state
  instead of minting a fresh inode lease on reconnect
- object IDs still reserve a persistent PG client slot and reuse the persisted
  block ranges across later mounts

Core invariants:
- local `size` and local slices may be ahead of peer-visible replicated state
- the writing mount keeps newer local state ahead of foreign replicated state
- foreign apply updates replicated/base tables and must not clobber newer local
  authority state
- ordinary restart/reopen recovers pending convergence without requiring local
  metadata wipe + `discover`

## High-Level Mutation Flow

1. App calls a VFS mutation.
2. Accepted bytes enter the file writer, chunk writer, and slice writer page
   buffers.
3. Same-mount reads observe the accepted bytes through writer state.
4. Frozen slices finish through `CachedStore` staging and local slice metadata
   commit.
5. `flush(ino)`, `fsync(ino)`, and last-handle release force the required local
   slice/cache boundary for the inode or handle.
6. Background workers upload immutable payload objects from the cached-store
   backlog to object storage.
7. Publication emits the new file version only after the referenced objects and
   metadata line up.
8. Other clients converge later through PostgreSQL invalidation/replay and
   local apply into their own `redb`.

## Read Resolution

Reads resolve data in this order:
1. active writer state for the current mount/handle
2. local slice metadata in `redb`
3. local cache / object-store fetches supplying the payload objects needed by
   the selected slice view
4. zero-fill for holes

Metadata lookups are served from local `redb`; PostgreSQL is not part of the
foreground read path.

## Cache and Upload Runtime

- `studiofs-cache` provides the single cached-store pipeline for writer pages,
  local staging, cached reads, pending-upload queueing, upload helpers, and
  pending-upload pinning.
- The current disk layout lives directly under `<cache-root>/**`; the
  UUID-scoped cache root itself is the runtime boundary.
- Upload is a background payload data plane seeded from cached-store staging and
  startup replay. Publication is a separate control-plane step that flips remote
  visibility at file-version granularity after payload readiness is confirmed.

## Concurrency Model

- Tokio owns async upload/fetch/background work.
- Local locks protect VFS/cache hot structures, with explicit lock ordering for
  shared subsystems.
- PostgreSQL `batch_version` ordering drives cross-client metadata convergence.
- Per-file writer and per-inode session coordination keep
  write/flush/fsync/release boundaries ordered without forcing synchronous
  remote publication into the foreground path.

# Architecture Overview

For the SFS-242 rewrite, the canonical target contract lives in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).
This page is the implementation snapshot for the current tree; if it conflicts
with the contract, the contract wins.

## System Design

StudioFS mounts S3-compatible object storage as a Local-First filesystem with a
JuiceFS-inspired, Rust-native chunk/slice/block data plane.

Core principles:
- Foreground metadata operations read and write local `redb`; foreground payload
  durability flows through the local cache backend before remote publication.
- PostgreSQL is the shared replication bus and checkpoint store, not the hot
  read path.
- File payloads live in immutable blocks addressed by chunk-relative slices.
- Runtime visibility is split into `writer state`, `local unpublished state`,
  and `published state`.
- The current `v3` subtree is an implementation detail of the local cache
  backend, not the architecture name of the product.
- Remote visibility switches atomically at the file-version boundary.

## Workspace Graph

```text
studiofs (CLI)
  ├── studiofs-meta       local metadata + PostgreSQL delta sync
  ├── studiofs-data       S3 object I/O + object format
  ├── studiofs-cache      local cache backend + staged writeback pipeline
  ├── studiofs-vfs        filesystem core
  ├── studiofs-fuse       Linux/macOS bridge
  └── studiofs-winfsp     Windows bridge
```

## Metadata Architecture

`PgMetaEngine` keeps one local-first view and one published/shared view:

- bootstrap loads the latest published checkpoint from `metadata_state`
- steady-state sync exchanges ordered delta events through `event_log`
- local reads resolve against local `redb` first, then converge toward newer
  foreign published state through invalidation and replay
- crash recovery rebuilds pending unpublished state from local metadata and
  upload/publish bookkeeping

Core invariants:
- local `size` and local slices may be ahead of remote-visible committed state
- the writing mount keeps newer local unpublished data ahead of foreign
  published state
- foreign apply updates the published/base tables and must not clobber newer
  local unpublished state
- ordinary restart/reopen recovers pending convergence without requiring local
  metadata wipe + `discover`

## High-Level Mutation Flow

1. App calls a VFS mutation.
2. StudioFS updates local metadata in `redb`.
3. Accepted bytes enter handle-owned writer state and become immediately
   readable on the writing mount.
4. `flush(ino)` / `fsync(ino)` / last-handle release materialize durable local
   unpublished slices and attrs through local backend + `sync_data()` +
   metadata commit.
5. `CachedStore` queues persisted payloads for background upload.
6. Background workers upload immutable blocks to object storage.
7. Epoch finalization publishes the new file version only after the referenced
   objects and metadata line up.
8. Other clients converge later through PostgreSQL invalidation/replay and
   published-state refresh.

## Read Resolution

Reads resolve data in this order:
1. active writer state for the current mount/handle
2. durable local unpublished state
3. published state
4. zero-fill for holes, with local cache / object-store fetches supplying the
   block payloads needed by the selected slice view

Metadata lookups are served from local `redb`, where the local unpublished view
outranks the published/base tables on the writing mount.

## Cache and Upload Runtime

- `studiofs-cache` provides the backend-agnostic local cache boundary plus the
  `CachedStore` runtime for staged writeback, queue rebuild, and pending-upload
  pinning.
- The current disk layout lives under `<cache-root>/v3/**`; that versioned
  subtree is an implementation detail, not a separate architecture.
- Upload is a background payload data plane. Publication is a separate
  control-plane step that flips remote visibility at file-version granularity.

## Concurrency Model

- Tokio owns async upload/fetch/background work.
- Local locks protect VFS/cache hot structures, with explicit lock ordering for
  shared subsystems.
- PostgreSQL `batch_version` ordering drives cross-client metadata convergence.
- Per-inode session coordination keeps write/flush/release barriers ordered
  without forcing synchronous remote publication into the foreground path.

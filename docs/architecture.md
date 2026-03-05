# Architecture Overview

## System Design

StudioFS is a Rust workspace that mounts S3-compatible object storage as a local filesystem.

Core principles:
- Central metadata authority in PostgreSQL (ACID serialization for multi-client writes).
- Local-first metadata in `redb` for fast synchronous filesystem operations.
- Immutable object storage for file blocks in S3-compatible backend.
- Multi-tier cache (RAM + disk) for performance.
- Platform-agnostic VFS core with thin FUSE/WinFSP adapters.

## Workspace Graph

```
studiofs (CLI)
  ├── studiofs-meta       metadata engines (redb + postgres sync)
  ├── studiofs-data       S3 object I/O + object format
  ├── studiofs-cache      L1/L2 cache + writeback queue
  ├── studiofs-vfs        filesystem core
  ├── studiofs-fuse       Linux/macOS bridge
  └── studiofs-winfsp     Windows bridge
```

## Metadata Architecture (Current)

`PgMetaEngine` model:
- Foreground client path: mutate/read local `redb` only.
- Background worker:
  - flush local pending mutations to PostgreSQL in batches;
  - pull newer PostgreSQL snapshot back into local `redb`.
- Multi-client convergence: eventual consistency via ordered PostgreSQL commits + periodic pull/sync.

High-level flow:
1. App calls VFS mutation.
2. Metadata mutation is committed locally to `redb` (sync, low latency).
3. Mutation is appended to local metadata event log.
4. Background flush batches pending events and commits merged snapshot to PostgreSQL.
5. Other clients pull newer snapshot and apply it to local `redb`.

## Write Path

1. Data is split into blocks.
2. Blocks go through dirty memory + cache pipeline.
3. Metadata block refs are updated in local `redb`.
4. Upload worker writes object payloads to S3.
5. Metadata background sync publishes merged state to PostgreSQL.

## Read Path

1. Try pending/dirty local state.
2. Try L1 RAM cache.
3. Try L2 disk cache.
4. On miss, fetch from S3 and populate caches.
5. Metadata reads always come from local `redb` snapshot.

## Concurrency Model

- Tokio for async I/O and background tasks.
- `spawn_blocking` for synchronous metadata/disk sections.
- Local locks for hot structures in cache/VFS.
- PostgreSQL transaction serialization for cross-client metadata ordering.

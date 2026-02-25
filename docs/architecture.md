# Architecture Overview

## System Design

StudioFS is a virtual filesystem that presents S3-compatible object storage as a POSIX-like local drive. It is built as a Rust workspace with a layered architecture where each layer has a single responsibility.

### Design Principles

- **No central server** — metadata is coordinated via S3-based Tonbo manifests with CAS semantics
- **Immutable objects** — data objects are write-once, identified by sequential IDs
- **Local-first** — all reads and writes hit local cache first; S3 is the durable tier
- **Crash safety** — writeback journal ensures no data loss on unexpected shutdown
- **Platform agnostic** — core VFS is OS-independent; thin platform bridges handle FUSE/WinFSP

## Crate Dependency Graph

```
studiofs (CLI binary)
  ├── studiofs-meta       metadata: redb + Tonbo + events
  ├── studiofs-data       S3 I/O: OpenDAL, LZ4, xxhash
  ├── studiofs-cache      L1 RAM + L2 NVMe + LRU + upload queue
  │     ├── studiofs-meta
  │     └── studiofs-data
  ├── studiofs-vfs        platform-agnostic VFS: read/write/AIMD/readahead
  │     ├── studiofs-meta
  │     ├── studiofs-data
  │     └── studiofs-cache
  ├── studiofs-fuse       Linux/macOS bridge (fuser)
  │     └── studiofs-vfs
  └── studiofs-winfsp     Windows bridge (winfsp)
        └── studiofs-vfs
```

## Crate Responsibilities

### `studiofs` — CLI

Binary crate. Parses CLI arguments (clap), initializes logging, dispatches to subcommand modules (`init`, `discover`, `mount`, `status`, `tune-metadata`, `janitor`, `test`, `destroy`).

On Windows, locates and pre-loads `winfsp-x64.dll` from the registry at startup.

### `studiofs-meta` — Metadata Engine

Defines the `MetaEngine` trait — a fully synchronous interface for all metadata operations (inodes, directory entries, file blocks, counters, GC pending deletions). Two implementations:

| Engine | Storage | Use Case |
|--------|---------|----------|
| `RedbMetaEngine` | Local redb database | Single-client or local cache |
| `TonboMetaEngine` | redb (local) + S3 manifest (remote) | Multi-client distributed |

The Tonbo engine adds batch flush with CAS retries, range-based ID allocation, TTL-based cache refresh, and optional NATS invalidation.

### `studiofs-data` — Data Layer

S3 I/O abstraction built on OpenDAL. Handles:
- Object creation with LZ4 compression and xxh64 checksums
- `IoProfile`-based connection pooling (separate pools for reads vs writes)
- Block addressing: `{volume}/objects/{ab}/{cd}/{id}`

### `studiofs-cache` — Cache System

Two-level block cache with unified LRU tracking:

```
┌──────────────┐    ┌──────────────┐    ┌────────┐
│  L1 (RAM)    │ ←→ │  L2 (NVMe)   │ ←→ │  S3    │
│  sharded map │    │  file/block  │    │ (cold) │
└──────────────┘    └──────────────┘    └────────┘
        ↕                  ↕
    ┌──────────────────────────┐
    │  LRU Tracker (64 shards) │
    └──────────────────────────┘
              ↕
    ┌──────────────────────────┐
    │  Upload Queue (journal)  │
    └──────────────────────────┘
```

See [Cache System](cache.md) for details.

### `studiofs-vfs` — Virtual Filesystem

Platform-agnostic VFS implementation. Orchestrates metadata lookups, block reads/writes, cache management, and background tasks:

- Dirty memory → L1 → L2 → S3 write pipeline
- Cache → S3 read pipeline with readahead and AIMD congestion control
- Backpressure when dirty memory exceeds limit
- Background flush timer, janitor, metadata sync

### `studiofs-fuse` — FUSE Bridge

Linux/macOS implementation of `fuser::Filesystem`. Thin adapter that translates FUSE callbacks into `StudioVfs` method calls via `rt.block_on()`.

### `studiofs-winfsp` — WinFSP Bridge

Windows implementation of `winfsp::FileSystemContext`. Translates WinFSP callbacks into `StudioVfs` calls.

## Data Flows

### Write Path

```
App write()
  │
  ├─ Split data into 1 MiB blocks
  ├─ Allocate sequential block IDs
  ├─ Buffer in pending_blocks (per-inode)
  │
  ├─ On close/flush/threshold:
  │    ├─ Move to dirty_mem pool
  │    ├─ Background flusher (10ms timer):
  │    │    ├─ Stage blocks in L1 cache
  │    │    ├─ Batch commit metadata (redb transaction)
  │    │    ├─ Parallel write to L2 NVMe
  │    │    └─ Enqueue for S3 upload
  │    │
  │    └─ Upload worker:
  │         ├─ Compress (LZ4) + checksum (xxh64)
  │         ├─ PUT to S3
  │         └─ Remove journal entry
  │
  └─ Metadata periodically flushed to Tonbo manifest
```

### Read Path

```
App read()
  │
  ├─ Check pending_blocks (uncommitted writes)
  ├─ Check dirty_mem (staged writes)
  ├─ Check L1 memory cache
  ├─ Check L2 NVMe cache
  │
  ├─ Cache miss → FetchScheduler:
  │    ├─ Singleflight dedup (one S3 GET per block)
  │    ├─ AIMD concurrency limiter (1–32 connections)
  │    ├─ GET from S3 → decompress → verify checksum
  │    ├─ Populate L1 + L2 cache
  │    └─ Retry with exponential backoff (up to 5 attempts)
  │
  ├─ ReadAheadTracker:
  │    ├─ Detect sequential access (score 0–8)
  │    └─ Prefetch next N blocks in background
  │
  └─ Return bytes to app
```

### Metadata Sync (Tonbo)

```
Local writes → redb (immediate)
  │
  ├─ Periodic flush (60s) or threshold (100 ops / 1 MiB):
  │    ├─ Compute delta vs baseline snapshot
  │    ├─ Serialize and PUT to S3 with If-Match (CAS)
  │    ├─ On conflict: pull remote, merge, retry
  │    └─ Update baseline snapshot
  │
  ├─ Optional NATS publish: fs.invalidate.<volume>.all
  │
  └─ Remote reads:
       ├─ TTL-based refresh (20s)
       └─ Cache miss → immediate refresh
```

## S3 Path Layout

All paths are scoped by volume name:

```
<volume>/
  ├── objects/<ab>/<cd>/<object_id>   # data objects (LZ4 compressed)
  ├── metadata/manifest.json          # Tonbo manifest (ETag versioned)
  └── metadata/janitor.lock           # distributed janitor lease
```

## Concurrency Model

- **Tokio runtime** — all async I/O (S3, NATS, timers)
- **`spawn_blocking`** — all metadata (redb) and L2 disk I/O
- **`parking_lot`** — synchronous locks for hot-path structures (L1 cache shards, LRU shards)
- **`DashMap`** — concurrent maps for inflight dedup, open file handles
- **Sharding** — L1 cache (64 shards), LRU tracker (64 shards), L2 disk (256 directory shards)

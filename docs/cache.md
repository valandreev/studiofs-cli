# Cache System

This page describes the cache subsystem under the canonical mounted-IO contract
in [juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).

The cache subsystem (`studiofs-cache`) provides the single JuiceFS-equivalent
cache/object pipeline centered on `CachedStore` plus the local cache backend.

## Runtime Scope

Runtime components:

| Component | Modules | Role |
|---|---|---|
| Local cache backend | `payload_layout`, `payload_index`, `payload_store`, `startup_recovery` | Persistent local payloads + lifecycle/index state |
| Local-backend contract | `local` | Backend-agnostic trait used by VFS read/write/upload paths |
| Cached-store runtime | `cached_store`, `upload_queue`, `upload_stage` | Writer pages, staging, cached reads, pending-upload queue, upload helpers, pinning, and runtime metrics |
| Benchmark-only cache surface | `benchmark_cache` | Lightweight in-memory cache used by `studiofs test-cache` |

## Architecture

```text
                ┌────────────────────────────┐
                │        CachedStore         │
                │ (cache + upload runtime)   │
                └───────────┬────────────────┘
                            │
          ┌─────────────────┴──────────────────┐
          │                                    │
   ┌──────▼────────┐                  ┌────────▼─────────┐
   │ Writer pages  │                  │ Upload Queue     │
   │ + pin tracking│                  │ (staged payloads │
   │               │                  │  + startup replay)│
   └───────────────┘                  └──────────────────┘
                            │
                            ▼
                ┌────────────────────────────┐
                │ Local cache backend        │
                │      (UUID cache root)     │
                └────────────────────────────┘
```

## Metadata Lookup Caches

In addition to payload-object caching, VFS keeps bounded metadata lookup caches:

- `DentryCache` for `(parent ino, name) -> child ino | negative`
- `PathShortcutCache` for full-path shortcut resolution
- `AttrReadCache` for read-through inode attributes with TTL

These share one generic two-tier implementation (`TwoTierCache`) with
cache-specific `CachePolicy` hooks.

## Startup Recovery

On backend startup, reconciliation runs before serving reads/writes:

1. Default (`lightweight`) mode removes orphan temp files under `tmp/**`
   and prunes dirty/uploading index rows that point to missing payload files.
2. Optional full sweep (`STUDIOFS_CACHE_V3_STARTUP_RECONCILE_MODE=full`)
   additionally removes stale index rows and orphan payload files.
3. VFS startup replay scans local metadata and durable local payload state,
   restores upload-stage links for live unpublished objects, and rebuilds the
   runtime pending-upload queue.

Reconciliation is scoped to the runtime root and does not mutate unrelated
files under `<cache-root>`.

Recovery is deliberately bounded by the local durability boundary. Bytes that
only lived in writer memory before `flush`, `fsync`, close/release, or another
local-sync barrier may be discarded by a crash. Bytes already staged into the
local payload backend plus metadata graph remain locally readable and can be
uploaded/published later without requiring PostgreSQL or object storage to be
available during local recovery.

The zero-IO status snapshot reports startup recovery with counters for
replayed staged bytes, queued uploads, discarded memory-only bytes, orphan dirty
payload cleanup, and unreferenced warm-payload cleanup.

## CachedStore Responsibilities

`CachedStore` is the canonical cache/object-store pipeline owner for the mount
runtime. It is intentionally focused on:

- writer-page ownership, slice/block staging, cache hits, range/full-payload
  rereads, and payload lifecycle
- pending-upload pinning near upload boundaries
- runtime pending-upload queue bookkeeping
- upload helpers used by the background uploader
- runtime metrics used by `status`

`BenchmarkCache` is separate and only backs `studiofs test-cache`; it is not
part of the production mount runtime.

Durable local payload state lives in the cached store plus local metadata; the
mount must not create a second foreground payload pipeline.

## Operational Notes

- Runtime local payload durability and lifecycle state come from cached-store
  staging, backend persistence, and index state.
- Upload intent is seeded from staged payloads and startup replay; the
  in-memory upload queue is intentionally rebuilt from durable cache and
  metadata state after restart.
- Upload workers read committed payloads through `CachedStore` local payload
  sources: hot upload buffer, upload helpers, or local backend fallback.
- `studiofs status` reports local cache layout/index/payload health and the
  dirty/staged/upload counters needed for JuiceFS comparison.

# Cache System

This page is the current implementation snapshot for the cache subsystem. It is
not the canonical SFS-242 rewrite contract. The rewrite source of truth lives
in [juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).

The cache subsystem (`studiofs-cache`) provides one Local-First cache/object
pipeline centered on `CachedStore` plus the current local cache backend
implementation.

## Runtime Scope

Current runtime components:

| Component | Modules | Role |
|---|---|---|
| Current local cache backend | `payload_layout`, `payload_index`, `payload_store`, `startup_recovery` | Persistent local payloads + lifecycle/index state |
| Local-backend contract | `local` | Backend-agnostic trait used by VFS read/write/upload paths |
| Cached-store runtime | `cached_store`, `upload_queue`, `upload_stage` | Unified local cache handle, runtime pending-upload queue, upload helpers, pending-upload pinning, and runtime metrics |
| Benchmark-only cache surface | `benchmark_cache` | Lightweight in-memory cache used by `studiofs test-cache` |

Legacy cache-v2 storage implementation code has been removed.

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
   │ Pending-upload│                  │ Upload Queue     │
   │ pin tracking  │                  │ (seeded on commit│
   │               │                  │  + startup replay)│
   └───────────────┘                  └──────────────────┘
                            │
                            ▼
                ┌────────────────────────────┐
                │ Current local cache backend│
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

Reconciliation is scoped to the runtime root and does not mutate unrelated
files under `<cache-root>`.

## CachedStore Responsibilities

`CachedStore` is the canonical cache/object-store pipeline owner for the mount
runtime. It is intentionally focused on:

- local backend access for cache hits, range/full-payload rereads, and persisted
  payload lifecycle
- pending-upload pinning near upload boundaries
- runtime pending-upload queue bookkeeping
- upload helpers used by the background uploader
- runtime metrics used by `status`

`BenchmarkCache` is separate and only backs `studiofs test-cache`; it is not
part of the production mount runtime.

Durable local payload state still lives in the local backend plus metadata;
`CachedStore` orchestrates that runtime instead of becoming a second source of
truth.

## Operational Notes

- Runtime local payload durability and lifecycle state come from backend
  persistence + index state surfaced through `CachedStore`.
- Upload intent is seeded directly from local durable commit and from startup
  replay of durable local payloads.
- Upload workers read committed payloads through `CachedStore` local payload
  sources: hot upload buffer, upload helpers, or local backend fallback.
- `studiofs status` reports local cache layout/index/payload health without
  depending on retired storage formats or terminology.

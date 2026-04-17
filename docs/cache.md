# Cache System

This page is the current implementation snapshot for the cache subsystem. It is
not the canonical SFS-242 rewrite contract. The rewrite source of truth lives
in [juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).

The cache subsystem (`studiofs-cache`) provides one Local-First cache/object
pipeline centered on `CachedStore` plus the current `v3` local cache backend
implementation.

## Runtime Scope

Current runtime components:

| Component | Modules | Role |
|---|---|---|
| Current local cache backend (`v3`) | `v3::layout`, `v3::cache_index`, `v3::file_payload_store`, `v3::reconcile` | Persistent local payloads + lifecycle/index state |
| Local-backend contract | `local` | Backend-agnostic trait used by VFS read/write/upload paths |
| Cached-store runtime | `cached_store`, `upload_queue` | Unified local cache handle, staged-writeback queue, pending-upload pinning, and runtime metrics |
| Benchmark-only cache surface | `benchmark_cache` | Lightweight in-memory cache used by `studiofs test-cache` |

Legacy cache-v2 storage implementation code has been removed.

## Architecture

```text
                ┌────────────────────────────┐
                │        CachedStore         │
                │ (cache + staged writeback) │
                └───────────┬────────────────┘
                            │
          ┌─────────────────┴──────────────────┐
          │                                    │
   ┌──────▼────────┐                  ┌────────▼─────────┐
   │ Pending-upload│                  │ Upload Queue     │
   │ pin tracking  │                  │ (rebuilt on open)│
   └───────────────┘                  └──────────────────┘
                            │
                            ▼
                ┌────────────────────────────┐
                │ Current local cache backend│
                │        (`v3` subtree)      │
                └────────────────────────────┘
```

## Metadata Lookup Caches

In addition to block caching, VFS keeps bounded metadata lookup caches:

- `DentryCache` for `(parent ino, name) -> child ino | negative`
- `PathShortcutCache` for full-path shortcut resolution
- `AttrReadCache` for read-through inode attributes with TTL

These share one generic two-tier implementation (`TwoTierCache`) with
cache-specific `CachePolicy` hooks.

## Startup Recovery

On backend startup, reconciliation runs before serving reads/writes:

1. Default (`lightweight`) mode removes orphan temp files under `v3/tmp/**`
   and prunes dirty/uploading index rows that point to missing payload files.
2. Optional full sweep (`STUDIOFS_CACHE_V3_STARTUP_RECONCILE_MODE=full`)
   additionally removes stale index rows and orphan payload files.

Reconciliation is scoped to `v3/**` and does not mutate unrelated files under
`<cache-root>`.

## CachedStore Responsibilities

`CachedStore` is the canonical cache/object-store pipeline owner for the mount
runtime. It is intentionally focused on:

- local backend access for cache hits, range/full-block rereads, and persisted
  payload lifecycle
- pending-upload pinning near upload boundaries
- pending-upload queue bookkeeping
- runtime metrics used by `status`

`BenchmarkCache` is separate and only backs `studiofs test-cache`; it is not
part of the production mount runtime.

Durable local payload state still lives in the local backend plus metadata;
`CachedStore` orchestrates that runtime instead of becoming a second source of
truth.

## Operational Notes

- Runtime local payload durability and lifecycle state come from backend
  persistence + index state surfaced through `CachedStore`.
- Upload intent is persisted through local-backend lifecycle flags; the
  in-memory queue is rebuilt on startup from local backend + metadata mapping.
- Upload workers reread committed payloads from the local backend
  (`get_for_upload()` via `CachedStore`), not from transient staging buffers.
- `studiofs status` reports local cache layout/index/payload health without
  depending on retired storage formats or terminology.

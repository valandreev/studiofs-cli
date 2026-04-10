# Cache System

The cache subsystem (`studiofs-cache`) is now a Cache 3.0-first local data
plane with a narrow upload orchestration layer.

## Runtime Scope

Current runtime components:

| Component | Modules | Role |
|---|---|---|
| Cache 3.0 local backend | `v3::layout`, `v3::cache_index`, `v3::file_payload_store`, `v3::reconcile` | Persistent local payloads + lifecycle/index state |
| Local-backend contract | `v2::local_block_backend` | Backend-agnostic trait used by VFS/read/write/upload paths |
| Upload orchestration layer | `block_cache`, `upload_queue` | Pending-upload orchestration and local pipeline telemetry |

Cache-v2 storage implementation code has been removed.

## Architecture

```
                ┌────────────────────────────┐
                │       BlockCache           │
                │ (upload orchestration)     │
                └───────────┬────────────────┘
                            │
          ┌─────────────────┴──────────────────┐
          │                                    │
   ┌──────▼────────┐                  ┌────────▼─────────┐
   │ L1 Staging    │                  │ Upload Queue     │
   │ (bounded RAM) │                  │ (in-memory+journal) │
   └───────────────┘                  └──────────────────┘
                            │
                            ▼
                ┌────────────────────────────┐
                │ Cache 3.0 Backend (v3)    │
                │ file-per-block + redb      │
                └────────────────────────────┘
```

## Metadata Lookup Caches

In addition to block caching, VFS keeps bounded metadata lookup caches:

- `DentryCache` for `(parent ino, name) -> child ino | negative`
- `PathShortcutCache` for full-path shortcut resolution
- `AttrReadCache` for read-through inode attributes with TTL

These share one generic two-tier implementation (`TwoTierCache`) with
cache-specific `CachePolicy` hooks.

## Cache 3.0 Startup Recovery

On backend startup, reconciliation runs before serving reads/writes:

1. Default (`lightweight`) mode removes orphan temp files under `v3/tmp/**`
   and prunes dirty/uploading index rows that point to missing payload files.
2. Optional full sweep (`STUDIOFS_CACHE_V3_STARTUP_RECONCILE_MODE=full`)
   additionally removes stale global index rows and orphan payload files.

Reconciliation is scoped to `v3/**` and does not mutate unrelated files under
`<cache-root>`.

## Upload Orchestration Responsibilities

`BlockCache` remains intentionally small and focused on:

- buffering/pinning payloads near upload boundaries
- pending-upload queue bookkeeping
- runtime metrics used by `status` and benchmark tooling

It is not the source of truth for durable local payload state.

## Operational Notes

- Runtime local payload durability and lifecycle state are provided by Cache 3.0
  backend persistence + index state.
- Upload workers reread committed payloads from the local backend
  (`get_for_upload()`), not from transient staging buffers.
- `studiofs status` reports Cache 3.0 contract/index/payload health and does not
  depend on retired storage formats.

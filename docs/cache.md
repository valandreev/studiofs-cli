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

## Snapshot v2 Artifact Cache

Snapshot v2 immutable metadata artifacts use the configured snapshot cache
root with the compact layout:

```text
<cache-root>/<compact-volume-uuid>/<snapshot-component>-<sha256>.redb
```

The UUID has its hyphens removed and the volume-name prefix is omitted from
the snapshot component to keep Windows paths below the path-length budget.
The full SHA-256 digest remains part of the filename and is the cache identity
for the immutable artifact.

This layout replaced the older
`<cache-root>/metadata/snapshots/volume-*/name-*/uuid-*/snapshot-*.redb`
layout. Existing files in the legacy directory are not imported into the new
index and are not evicted by the Snapshot v2 cache session; they are safe to
remove manually after confirming no older StudioFS process still uses them.

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

1. Default (`lightweight`) mode replays single and batch put intents, removing
   final payload files only when the put never reached the index. It also
   removes orphan temp files under `tmp/**` and prunes dirty/uploading index rows that
   point to missing payload files. Indexed payloads and their upload-stage
   hardlinks survive regardless of upload lifecycle state.
2. Lightweight mode removes unindexed final payloads only at paths named by
   put intents. Optional full sweep
   (`STUDIOFS_LOCAL_PAYLOAD_STARTUP_RECONCILE_MODE=full`) additionally removes
   stale index rows and scans for orphan payload files, including historical
   orphans without a put intent.
3. VFS startup replay scans local metadata and durable local payload state,
   restores upload-stage links for live unpublished objects, and rebuilds the
   runtime pending-upload queue.

Reconciliation is scoped to the runtime root and does not mutate unrelated
files under `<cache-root>`.
Fast cache mode retains its existing no-fsync guarantees.

The put-intent directory is created once when the payload store opens and must
remain present while the backend is open; Durable mode syncs its parent before
writes begin. On the successful same-device path, a
Durable put syncs the payload temp file and the intent temp file, with three
directory barriers for the intent, source temp, and destination payload
directories. The `durable_mode_keeps_per_put_sync_barriers` regression measures these barrier
calls; on Windows, directory barriers are intentionally no-ops. Fast mode writes
the intent without adding file or directory syncs. A failed index commit retires
its intent only after confirming payload removal; a Durable single-put rollback
adds a fourth directory barrier to persist deletion first. Batch rollback adds
one barrier per distinct touched payload shard, including when another unlink
fails; any error retains the intent. These operation counts do not establish mounted
throughput or wall-clock cost.

Single and batch index failures share this rollback path. Payloads with an
existing index row are retained; only unindexed outputs are removed. Failed
index checks, payload removal, or directory barriers retain the intent for
startup recovery and produce a rollback warning. Object IDs identify immutable
content: retrying or rehydrating the same bytes may reuse an ID, while changed
content requires a fresh ID.
Runtime mutation gates serialize conflicting puts, rollback, deletion and
upload/pin transitions. Batches acquire unique gates in stable order, budget
admission precedes publication gates, and eviction rechecks eligibility while
holding the gate. The fixed 64 gates bound memory; colliding IDs may serialize.

Single and batch store errors after payload rename retain the committed files
and intent; startup uses the index to preserve committed data and remove
unindexed outputs. Failed intent creation removes uncommitted staged files.
The startup report counts final orphan files removed by intent replay in both
Lightweight and Full modes, and the repair log includes resolved intent count.

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

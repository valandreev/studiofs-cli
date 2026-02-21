# Tonbo Metadata Cutover (no rollback)

Goal: replace remote redb with Tonbo (R2/MinIO) while leaving FUSE, opendal→S3 data path, range cache, and chunk logic untouched. Redb remains as a local cache with batched push; multi-client consistency is eventual until the invalidation bus is added. Janitor cleans orphaned chunks.

## Constraints & Defaults (agreed)
- No backward compatibility path: Tonbo is the only remote metadata store.
- Metadata schema lives in one place: `crates/studiofs-meta/src/schema.rs` (re-exported from `lib.rs`); manifest version field named `version` (maps to ETag/If-Match on Tonbo).
- Local cache: redb read-through + write-back; initial TTL = 20s (single-client baseline, tune later).
- Flush cadence: scheduler runs at most once per minute; also triggers when ≥100 queued mutations or ≥1 MiB serialized delta (defaults, tunable).
- Metadata-in-data-bucket layout when shared: prefix `metadata/` alongside `objects/`.
- Destroy default: full wipe (local cache + Tonbo metadata + S3 data) with double confirmation; `--local` keeps remote intact.

## Phase 0 — Schema Consolidation (single source of truth)
- Introduce `schema.rs` containing inode/dentry/chunk/ref/version structs and serde for Tonbo manifests.
- Conversion helpers: redb tables ↔ schema structs; Tonbo adapter ↔ schema structs.
- Add short doc pointer (`docs/metadata-schema.md`) to this module; engines forbidden from divergent schemas.

## Phase 1 — Tonbo Adapter (single-client parity)
 - Implement `TonboMetaEngine` behind `MetadataStore` using conditional PUT/GET on MinIO/R2.
 - Read path: cache miss → Tonbo fetch → cache populate.
 - Write path: enqueue mutations in cache with CAS tokens; no immediate remote PUT.
- Metrics: Tonbo round-trips, CAS retries, cache hit/miss.
- Tests: current suite against MinIO; parity with old single-client behavior.

## Phase 2 — Batched Push & TTL Cache
- Flush scheduler: run every 60s max, or sooner on thresholds (100 ops or 1 MiB delta); manual flush on unmount/shutdown.
- Flush logic: gather queued mutations, apply CAS; on conflict, refetch manifest, reapply, retry with backoff.
- TTL: 20s initial; expired entries refetch from Tonbo. Configurable for later tuning.

## Phase 3 — CLI Changes (init/discovery/destroy)
- `init`: accept separate metadata endpoint/bucket/disk/creds; if “same as data”, store under `metadata/` prefix in the data bucket. Persist both configs locally (redb path).
- `discovery`: take metadata endpoint/bucket/disk/creds; validate access, build local redb cache, register volume for mount.
- `destroy`: default full wipe (double confirm); `--local` removes only cache/config so volume can be reattached via discovery.
- Update help/docs and validation for both paths.

## Phase 4 — Multi-client (eventual consistency, no bus yet)
- Two clients on shared Tonbo bucket; stress concurrent creates/renames/writes.
- Verify CAS conflicts resolve via retries; TTL alone provides eventual convergence.
- Tune TTL/flush thresholds for latency vs staleness; log divergence.

### Conflict Policy (implemented)
- CAS-only writes to `manifest.json` (no blind overwrite): `if-match` on known etag, `if_not_exists` for first create.
- 3-way merge on CAS conflict: `base(local baseline) + local delta + latest remote`.
- Counters:
  - `NEXT_INODE`, `NEXT_BLOCK_ID`, `nextEventSeq` => monotonic `max(local, remote)`.
  - additive counters (e.g. used space, inode totals) => apply `local_delta` to `remote`.
- Inodes (same key conflict): prefer newer by `ctime`, then `mtime`, then `size`.
- Dentries (same key conflict): deterministic winner by encoded value (effectively stable inode winner); update beats delete for same base key.
- File blocks (same `(ino, block_index)` conflict): higher `object_id` wins.
- `force flush` with no pending local mutations is skipped to prevent stale drop-time overwrite.

### Bimodal Consistency With Global Locks (implemented)
- Default mode: eventual consistency with CAS+merge.
- Locked mode (per inode, advisory `flock`): strong consistency path.
  - `flush-before-unlock`: lock holder flushes file/data+metadata before distributed unlock.
  - `refresh-before-lock-grant`: lock requester syncs metadata view after distributed lock acquire.
- Serverless lock backend: object-store CAS records under `<metadata-prefix>/locks/<ino>.lock`.
- Lock lease options:
  - `STUDIOFS_LOCK_TTL_SECS` (default 120)
  - `STUDIOFS_LOCK_RETRY_BASE_MS` (default 25)
  - `STUDIOFS_LOCK_RETRY_MAX_MS` (default 500)

## Phase 5 — Cache Invalidation Bus (performance)
- Add NATS publisher on successful Tonbo commit (subject `fs.invalidate.<volume>.<ino|dir>`); listener drops cache entries immediately.
- Bus improves freshness; correctness still guaranteed by TTL.
- Instrument missed/late messages.

## Phase 6 — Janitor (orphaned chunks)
- Prototype defaults: run every 30 min; skip objects newer than 15 min; cap deletes to 500 objects per run; dry-run flag.
- Logic: list data bucket objects, cross-check Tonbo manifests, delete unreferenced with rate limit.

## Phase 7 — Hardening & Rollout
- Chaos: kill client between data write and manifest flush; ensure janitor cleans.
- Load: measure stat/getattr latency with/without cache and with bus.
- Observability: structured logs for flush cycles, CAS retries, janitor actions; counters for flush batch sizes and TTL expirations.

## Open to Revisit Later
- Fine-tuned TTL/flush thresholds after multi-client testing.
- Janitor cadence/caps once load patterns are known.

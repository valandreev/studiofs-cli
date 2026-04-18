# JuiceFS-Inspired Fast Local Authority Contract

## Status: Canonical rewrite contract
## Date: 2026-04-18
## Issues: SFS-242, SFS-243
## JuiceFS reference snapshot: `valandreev/juicefs` `d3bce0efdb25ca19ca43599450bd75d4fb46467e`

This document is the single canonical mount contract for the SFS-242 rewrite.
If any older StudioFS redesign note, ADR, or implementation snapshot disagrees
with this file, this file wins.

JuiceFS remains the design reference for data-model and IO economics. StudioFS
is not trying to preserve the old "PG in the middle of every important mount
decision" model. The target mount contract is now:

- every mounted client runs foreground IO against its own local `redb` plus
  local block cache/backend;
- PostgreSQL is used for bootstrap of new clients, ordered metadata
  replication, invalidation/sync, checkpoints, and shared file locks when
  those are enabled;
- S3 is used for payload replication and cold fetch, not as the foreground
  acceptance barrier;
- cross-client behavior is Local-First plus eventual consistency.

`StudioFS Fast Local Authority` is not an extra mount mode. It is the only
target mount contract. Historical `cache_safety_mode=fast|durable` behavior in
the codebase is a transitional implementation detail, not the product model.

## 1. What We Are Borrowing Vs Preserving

| Category | Contract |
|---|---|
| JuiceFS ideas we are borrowing | chunk/slice/block modeling, per-file writer state, standalone-style local metadata hot path, slice-oriented flush behavior, `dataReader`-style read resolution, adaptive readahead, block prefetch, cached-store cache/object-store structure, and cheap foreground write acceptance |
| StudioFS invariants we are preserving | Local-First read-your-writes, per-mount local metadata authority, eventual consistency across mounts, background replication through PostgreSQL, background payload replication through S3, file-version atomic cross-client visibility, and optional PG-backed shared file locks |
| Invariants we are explicitly dropping | PG as a foreground metadata authority for ordinary mounted IO, synchronous remote visibility on `close`, and multiple user-facing mount consistency modes |
| Rust-native implementation rules | explicit ownership, enums and state transitions, domain-specific `Result` error types, bounded async concurrency, and module boundaries shaped by responsibilities instead of Go package mirroring |

## 2. Target Vocabulary And Invariants

| Term | Origin | Meaning in this contract |
|---|---|---|
| `chunk` | Borrowed from JuiceFS | Fixed logical file range used for lookup, split, overlap, and compaction boundaries. |
| `slice` | Borrowed from JuiceFS | Chunk-relative metadata record that maps a byte range to one immutable block or tombstone. |
| `block` | Borrowed from JuiceFS | Immutable payload object. Any content change produces a new block and new slice reference. |
| `writer state` | Borrowed shape, StudioFS-adapted | Per-handle/per-inode transient RAM state that satisfies same-mount reads before local metadata materialization completes. |
| `local authority view` | StudioFS target semantic | The local `redb`-backed metadata view served by this mount in the foreground. It is the only metadata authority used by mounted foreground IO on that client. |
| `replication event` | StudioFS target semantic | Ordered metadata delta published to PostgreSQL so other mounts can converge their local authority views. |
| `replicated shared state` | StudioFS target semantic | The portion of state that has been replicated and can become visible on other mounts after they apply it into their own local authority views. |
| `hole` | Shared filesystem concept | Uncovered byte range that reads as zeroes. |

Invariants:

- A slice never crosses a chunk boundary.
- Overlap resolution is per byte, not per slice.
- Foreground reads never consult PostgreSQL directly.
- Foreground metadata mutations never require PostgreSQL round-trips except for
  optional cross-client file-lock operations.
- Blocks are immutable and are never edited in place.
- Cross-client visibility changes at file-version granularity, not block by
  block.
- Each mount reserves disjoint inode and block-ID ranges before local
  allocation so concurrent clients do not collide on foreground-created keys.
- The disjoint-range rule is part of the contract, not a disposable
  implementation trick.

## 3. Fast Local Authority Mount Contract

Newly written bytes become visible in three stages:

1. `write` success makes bytes visible to the writing mount through active
   writer state.
2. `flush`, ordinary `close`/`release`, and background local materialization
   move accepted bytes into the local authority view in local metadata and the
   local block backend.
3. Background replication publishes payload and metadata so other mounts can
   apply the newer file version into their own local authority views.

Rules:

- Same-mount reads must observe accepted writes immediately through the merged
  local view.
- Ordinary successful `write`, `flush`, `close`, and `release` do not imply
  PostgreSQL replication success or S3 upload success.
- `fsync` is the only canonical strict request for a crash-safe local
  durability boundary.
- Failed PG replication or failed S3 upload must not revoke already accepted
  local visibility on the writing mount; they create backlog, retry, and
  eventually surface as explicit sync/health errors instead of silently
  invalidating local state.
- A crash before `fsync` or before the relevant local sync boundary may lose
  recently accepted writes. This trade-off is accepted explicitly in favor of
  JuiceFS-like write economics.

## 4. Eventual Consistency And Remote Replication

StudioFS preserves eventual consistency, not close-to-open parity.

- Upload and replication are asynchronous background phases.
- Other mounts observe new data only after both payload objects and the
  corresponding metadata replication events are available and applied locally.
- PostgreSQL is the ordered synchronization bus plus bootstrap/checkpoint
  source, not the foreground metadata truth for mounted IO.
- A new client uses PostgreSQL to bootstrap its first local authority view,
  then continues in the same local-authority model as every other mount.
- Local mounts may temporarily observe a newer state than peer mounts.
- Convergence is achieved by background S3 upload, PostgreSQL event replay, and
  local invalidation/apply, not by synchronous cross-client coordination on
  every write.

## 5. Read Resolution Contract

Foreground reads resolve data in this order:

1. active writer state for the current mount and handle;
2. local authority view in local `redb`;
3. local cache / S3 fetch for the selected immutable blocks;
4. zero-fill for uncovered holes.

Implications:

- Sparse writes and overlapping writes are legal and resolved by per-byte
  precedence.
- A local mount never waits on PostgreSQL to answer a foreground read.
- A peer mount does not consult another client's RAM-only writer state.
- Readahead and prefetch operate on the effective local merged view after this
  rule is applied.

## 6. Borrowed JuiceFS Ideas By Subsystem

| StudioFS area | Current StudioFS paths | JuiceFS reference files | What is borrowed |
|---|---|---|---|
| Regular-file vocabulary and schema | `crates/studiofs-meta/src/redb_engine/{blocks.rs,chunk_state.rs,replication.rs}` | `docs/en/introduction/architecture.md`, `pkg/meta/slice.go`, `pkg/meta/*.go` | Chunk/slice/block language, immutable block references, slice overlap model |
| Writer path | `crates/studiofs-vfs/src/local_dirty.rs`, `crates/studiofs-vfs/src/vfs/upload_lifecycle.rs` | `pkg/vfs/writer.go` | Per-file writer ownership, chunk/slice decomposition, cheap local-first write acceptance, and incremental flush behavior |
| Reader path, readahead, prefetch | `crates/studiofs-vfs/src/vfs/read_path.rs`, `crates/studiofs-vfs/src/readahead.rs`, `crates/studiofs-vfs/src/vfs/prefetch.rs` | `pkg/vfs/reader.go`, `pkg/chunk/prefetch.go`, `docs/en/guide/cache.md` | `dataReader`-style resolution, adaptive readahead, whole-block prefetch |
| Cache, staging, object-store handoff | `crates/studiofs-cache/src/{cached_store.rs,runtime.rs,block_*.rs,startup_recovery.rs}`, `crates/studiofs-data/src/store.rs` | `pkg/chunk/cached_store.go`, `pkg/chunk/cache_eviction.go`, `docs/en/guide/cache.md` | Cached-store lifecycle, cache/download/writeback structure, eviction and background upload concepts |
| Metadata authority model | `crates/studiofs-meta/src/{redb_engine,pg_engine}.rs` | `docs/community/getting-started/standalone`, `docs/community/getting-started/for_distributed` | Standalone-style local metadata hot path, but with StudioFS-specific async replication to peers through PostgreSQL instead of a shared foreground DB |
| Metadata-cache and operator surface | `crates/studiofs/src/cli/mount/*.rs`, `docs/configuration.md` | `cmd/flags.go`, `cmd/mount.go`, `cmd/mount_unix.go`, `cmd/mount_windows.go`, `docs/en/guide/cache.md` | Coherent mount/cache flags, strong internal metadata-cache defaults, operator-facing terminology |
| Bridge and mount behavior | `crates/studiofs-fuse/src/lib.rs`, `crates/studiofs-winfsp/src/*.rs` | `pkg/vfs/vfs.go`, `pkg/vfs/vfs_unix.go`, `pkg/vfs/vfs_windows.go`, `cmd/mount*.go` | Performance-sensitive bridge/mount patterns and callback expectations |

## 7. Metadata / PostgreSQL Responsibility Contract

The responsibility split is:

- local `redb`: the only foreground metadata authority for a mounted client;
- local block backend/cache: the only foreground payload authority for accepted
  local writes;
- PostgreSQL: bootstrap source, ordered replication bus, checkpoint source, and
  shared file-lock authority when locks are required across clients;
- S3/object storage: payload replication target and cold payload source.

Rules:

- Ordinary mounted IO must not block on PostgreSQL metadata reads/writes.
- New clients bootstrap from PostgreSQL into a local `redb`, then serve
  foreground IO locally.
- Foreign changes are applied into local `redb` asynchronously and become
  visible through local invalidation/apply.
- Cross-client file locks, when enabled, are the only normal foreground reason
  for a mounted client to talk to PostgreSQL directly.
- Per-client inode/block preallocation remains mandatory. A client obtains a
  persistent PG client slot, derives disjoint ID ranges from it, persists that
  slot locally, and allocates foreground inode/block IDs without further PG
  round-trips.

## 8. Mutation Visibility And Invalidation Matrix

| Mutation or transition | Visibility contract | Invalidation / replication contract |
|---|---|---|
| `write` | The writing mount reads accepted bytes immediately from writer state. Other mounts see nothing yet. | No PG round-trip is required before returning. |
| `flush` | Materializes accepted bytes into the local authority view. It does not wait for PG replication or S3 upload. | Replication may still be pending after success. |
| `fsync` | Requests a strict local crash-safe boundary for the current inode/handle state. It still does not imply remote visibility. | May flush pending local sync work before returning; PG/S3 remain asynchronous. |
| Last-handle `close` / `release` | Completes local materialization for remaining dirty state but must not force synchronous PG/S3 replication. | Any remaining replication/upload work stays in background backlog. |
| `rename` | Source disappearance and destination appearance are immediate on the writing mount's local authority view. | Other mounts converge later through PG replay/apply. |
| `unlink` / `rmdir` | Removal is immediate on the writing mount. | Other mounts converge later through PG replay/apply; payload cleanup remains asynchronous. |
| `truncate` / size-changing `setattr` | New EOF and attrs are visible immediately on the writing mount and become strict-local only when the local sync boundary requested by the caller completes. | Other mounts converge through background PG apply. |
| Non-size `setattr` | New attrs are visible immediately on the writing mount. | Other mounts converge through background PG apply. |
| Upload completion | Payloads may already exist in S3, but peers do not see the new file version until the replication event is published and applied. | No direct namespace visibility switch happens on upload alone. |
| PG replication event publish | The new file version becomes eligible for peer visibility. | Peers still see it only after local apply into their own `redb`. |
| Remote event apply | A mount converges its local authority view toward foreign changes while keeping its own newer local state ahead when conflict policy says so. | Broad foreign namespace apply events bump namespace epoch; affected attrs/dentries invalidate locally before serving newer applied state. |

## 9. Current StudioFS Concepts: Delete, Retain, Rename, Simplify

| Current concept | Decision in this rewrite |
|---|---|
| `WriteBuffer` as the canonical architecture | Delete as the target design. Transitional code may remain only until slice-writer replacement lands. |
| `ReadPipeline` as the canonical foreground read engine | Delete as the target design. Replace with a direct merged reader over writer/local authority/block state. |
| `UploadCoordinator` as the canonical upload architecture | Simplify/delete as a top-level design concept. Replace with one cached-store-style staging/upload pipeline. |
| `published` / `local unpublished` as separate architecture authorities | Simplify. They may remain as internal lifecycle labels inside local metadata, but they must not imply PG as a foreground authority. |
| `cache_safety_mode=fast|durable` as a product-level mount contract | Delete from the target product contract. A single Fast Local Authority mount model is the target. Transitional knobs may exist in code only while migration is in flight. |
| Versioned local runtime subtree as product architecture vocabulary | Rename/simplify. The current `v3/` subtree may remain as an implementation detail for the local backend, but not as the rewrite's conceptual model. |
| File-version atomic visibility | Retain. Peers must not observe half-published file versions. |
| `STUDIOFS_*` tuning sprawl | Simplify into a JuiceFS-inspired CLI/config surface with only transitional env vars where necessary. |

## 10. Rust-Native Implementation Rules

Rust-native implementation means:

- Do not perform a line-by-line Go port.
- Model lifecycle with owned types, enums, and explicit state transitions.
- Use domain-specific error enums and `Result<T, E>` in library layers; reserve
  panic paths for invariant violations and unrecoverable bootstrap failures.
- Use Tokio tasks, channels, and mutexes only where they express real
  ownership handoff, bounded backpressure, or async waiting requirements.
- Prefer localized state owners such as per-file writers, per-reader sessions,
  clearly bounded replication workers, and clearly bounded upload workers over
  one large mutable runtime mesh.
- Keep blocking IO and async orchestration separated explicitly.
- Prefer module boundaries based on responsibilities such as metadata schema,
  writer state, reader resolution, cached store, replication apply, and bridge
  adapters instead of transliterating Go package layout.

Review guardrails:

- Reject code that drags PostgreSQL into ordinary mounted read/write hot paths.
- Reject APIs that make per-inode or per-block foreground allocation depend on
  a PG round-trip once the client slot and ID ranges are reserved.
- Reject designs that require remote readers to reason about another mount's
  RAM-only writer state.
- Reject generic `anyhow`-style library errors where callers need domain
  decisions.

## 11. Historical Docs And Issues That Are No Longer Authoritative

| Source | Status after this contract |
|---|---|
| `docs/chunk-slice-block-vocabulary.md` | Removed from active docs. Its vocabulary freeze is now absorbed by this contract. |
| `docs/data-plane-redesign-guardrails.md` | Removed from active docs. Its sequencing and deletion guidance is now absorbed by this contract. |
| `SFS-154` | Historical vocabulary freeze that is superseded by this contract. |
| `SFS-155` | Historical guardrail issue that is superseded by this contract. |
| `SFS-148` | Historical redesign umbrella referenced by older guardrails; SFS-242 is the active umbrella. |

Supporting implementation snapshots such as `docs/architecture.md`,
`docs/vfs.md`, and `docs/configuration.md` still describe in-flight code, but
they are not authoritative for the rewrite target when they conflict with this
contract.

## 12. Glossary For Downstream Issues

| Term to use in new issues | Type | Meaning |
|---|---|---|
| `chunk`, `slice`, `block` | Borrowed JuiceFS concept | The rewrite's core data vocabulary. |
| `writer state` | Borrowed-and-adapted concept | Local transient state that serves same-mount reads before local metadata materialization finishes. |
| `local authority view` | StudioFS target semantic | The local `redb`-backed metadata truth used by this mount for foreground IO. |
| `replication event` | StudioFS target semantic | Ordered metadata delta published through PostgreSQL for peer convergence. |
| `replicated shared state` | StudioFS target semantic | State that peers may observe after they replay/apply replication into local `redb`. |
| `Local-First` | Retained StudioFS semantic | The writing mount observes accepted changes immediately through the merged local view. |
| `eventual consistency` | Retained StudioFS semantic | Other mounts converge later through upload plus replication/apply, not synchronously on each write. |
| `cached-store pipeline` | Borrowed JuiceFS concept | Unified cache, staging, download, upload, and eviction pipeline. |
| `client slot` | StudioFS target semantic | Persistent PG-assigned client identity used to derive disjoint local inode/block ID ranges. |
| `Fast Local Authority` | StudioFS target semantic | The single target mount contract: local foreground authority, PG for replication/bootstrap/locks, S3 for payload replication. |

Downstream issues should cite this file directly and then name the relevant
section instead of re-explaining the rewrite target.

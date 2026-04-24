# JuiceFS-Inspired Fast Local Authority Contract

## Status: Canonical rewrite contract
## Date: 2026-04-19
## Issues: SFS-242, SFS-243
## JuiceFS reference snapshot: `valandreev/juicefs` `d3bce0efdb25ca19ca43599450bd75d4fb46467e`

This document is the single canonical mount contract for the SFS-242 rewrite.
If any older StudioFS redesign note, ADR, or implementation snapshot disagrees
with this file, this file wins.

The rewrite is not required to preserve behavioral compatibility with the old
mount/runtime model when that older behavior conflicts with this contract.
During the rewrite, implementation choices should optimize for converging on
this contract, not for carrying forward legacy semantics that belong to the
pre-SFS-242 design.

JuiceFS remains the design reference for data-model and IO economics. StudioFS
is not trying to preserve the old "PG in the middle of every important mount
decision" model. The target mount contract is now:

- every mounted client runs foreground IO against its own local `redb` plus
  local payload cache/backend;
- PostgreSQL is used for bootstrap of new clients, ordered metadata
  replication, invalidation/sync, checkpoints, client discovery, preallocated
  ID-range reservation, and global file-lock coordination;
- S3 is used for payload replication and cold fetch, not as the foreground
  acceptance barrier and not as the user-visible namespace authority;
- cross-client behavior is Local-First plus eventual consistency.

`StudioFS Fast Local Authority` is not an extra mount mode. It is the only
target mount contract. The historical `cache_safety_mode=fast|durable` knob has
been removed from the codebase and is not part of the product model.

The main idea of this contract is a strict separation between two kinds of
guarantees:

- `local guarantees`: the writing mount sees its own changes immediately,
  continues to read/write them at local speed, and can keep working even while
  upload/replication is still catching up;
- `shared guarantees`: peer mounts only observe fully published states that
  have passed the payload-then-metadata publication rules and then been
  applied into the peer's own local authority view.

Everything else in this document is a consequence of that split.

## 1. What We Are Borrowing Vs Preserving

| Category | Contract |
|---|---|
| JuiceFS ideas we are borrowing | chunk/slice modeling over immutable payload objects, per-file writer state, standalone-style local metadata hot path, slice-oriented flush behavior, `dataReader`-style read resolution, adaptive readahead, block prefetch, cached-store cache/object-store structure, and cheap foreground write acceptance |
| StudioFS invariants we are preserving | Local-First read-your-writes, per-mount local metadata authority, eventual consistency across mounts, background replication through PostgreSQL, background payload replication through S3, file-version atomic cross-client visibility, and PG-backed shared file locks |
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
| `private overlay` | StudioFS target semantic | The writing client's own unpublished local view layered over the latest shared-applied state. Other clients never reason about or observe it. |
| `published version` | StudioFS target semantic | A file state whose payload requirements are already satisfied and whose metadata switch has been published through PostgreSQL. |
| `replication event` | StudioFS target semantic | Ordered metadata delta published to PostgreSQL so other mounts can converge their local authority views. |
| `replicated shared state` | StudioFS target semantic | The portion of state that has been published and can become visible on other mounts after they apply it into their own local authority views. |
| `lock lease` | StudioFS target semantic | PG-coordinated hard-lock state with TTL/renewal semantics and pub/sub notifications. |
| `orphaned payload` | StudioFS target semantic | Uploaded blocks that are no longer referenced by any published version and may later be removed by janitor. |
| `hole` | Shared filesystem concept | Uncovered byte range that reads as zeroes. |

Invariants:

- A slice never crosses a chunk boundary.
- Overlap resolution is per byte, not per slice.
- Blocks are immutable and are never edited in place.
- Foreground reads never consult PostgreSQL directly.
- Ordinary foreground metadata mutations never require PostgreSQL round-trips
  except for file-lock coordination and bootstrap/discovery/range-reservation
  work that is outside the steady-state file IO hot path.
- Cross-client visibility changes at file-version granularity, not block by
  block.
- A published version must never reference payload that has not already been
  confirmed durable in S3.
- Presence of payload in S3 does not by itself make a file or version visible.
- A newly created file must not become visible to peer mounts before its first
  published version is complete.
- A client's private overlay is visible only to that client.
- Open handles observe snapshot semantics: an already opened handle continues
  to read/write against the version/path binding it opened with until close,
  while new opens resolve against the latest applied shared state plus the
  opening client's own private overlay.
- Each volume owns one shared inode allocator for files, directories, and
  symlinks. Clients reserve durable local inode windows from a per-volume
  PostgreSQL `next_inode` counter and allocate foreground inode values from the
  persisted local window state.
- The initial inode window is reserved during bootstrap/discover when local
  allocator state is missing and then reused across later mounts of the same
  client instead of minting a fresh lease-like range on reconnect.
- If the current inode window is nearing exhaustion, StudioFS reserves and
  persists one prefetched inode window so the foreground create path can switch
  locally when the current window is consumed.
- object IDs still use a persistent PG client slot plus disjoint local payload
  ranges; that rule remains part of the contract, not a disposable
  implementation trick.

## 3. Primary Observation Model

The product contract is easier to reason about if it is stated in user-visible
terms instead of subsystem terms.

### 3.1 Local Guarantees

- `write` success makes accepted bytes visible immediately to the writing mount
  through writer state and the merged local view.
- The writing mount may continue to open, read, modify, rename, move, truncate,
  or delete its own locally accepted data at local speed while S3 upload and PG
  replication are still pending.
- Failed or delayed upload/replication creates backlog and health/sync debt; it
  does not revoke the writing mount's already accepted local view.
- A crash before the relevant local durability boundary may still lose recently
  accepted local-only work.

### 3.2 Shared Guarantees

- Peer mounts only observe states that have been fully published.
- "Fully published" means the referenced payload is already durable in S3 if
  payload changed, the metadata switch has been published through PostgreSQL,
  and the peer has applied that event into its own local authority view.
- S3 payload availability is not namespace visibility.
- PG metadata publication without the required S3 payload is a contract
  violation, not a valid steady state.
- A peer mount never consults another client's RAM-only or local-only writer
  state.

## 4. Fast Local Authority Mount Contract

Locally accepted bytes move through these stages:

1. `write` success makes bytes visible to the writing mount through active
   writer state and the merged local view.
2. explicit `flush`, `fsync`, last-handle `close`/`release`, size-changing
   metadata transitions, and background local materialization persist accepted
   state into the local authority view and local payload backend.
3. background upload and replication make selected states eligible for peer
   visibility.

Rules:

- Same-mount reads must observe accepted writes immediately through the merged
  local view.
- Ordinary successful `write`, `flush`, `fsync`, `close`, and `release` do not
  imply PostgreSQL replication success or S3 upload success.
- Explicit bridge `flush` is a mandatory local-durability barrier when dirty
  unpublished state still exists.
- `fsync` is the canonical caller-requested strict local crash-safe boundary,
  but it is not the only callback path that may need to reach strict local
  durability.
- Last-handle `close`/`release` may need to perform the fallback local
  durability barrier for remaining dirty unpublished state, but must not drag
  PG/S3 publication into the foreground path.
- Failed PG replication or failed S3 upload must not revoke already accepted
  local visibility on the writing mount; they create backlog, retry, and
  eventually surface as explicit sync/health errors instead of silently
  invalidating local state.
- A crash before the relevant local durability boundary may lose recently
  accepted writes. This trade-off is accepted explicitly in favor of
  JuiceFS-like write economics.

## 5. Eventual Consistency, Publication, And Failure Modes

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
- If PostgreSQL is unavailable, the mount may continue local work while it has
  enough preallocated local ID headroom. Once that headroom is exhausted, the
  mount degrades to read-only access over data it can already resolve from its
  local state and S3.
- If PostgreSQL is unavailable but S3 is still available, the mount may still
  cold-read already known published versions from S3 because no new metadata
  truth is being invented by that read.
- If S3 is unavailable, new payload-bearing versions simply do not become
  publishable to peers. The writing mount keeps its private overlay and retries
  in the background.
- When connectivity returns, the client publishes accumulated work in local
  causal order, but may coalesce its own intermediate states into the net
  externally equivalent result instead of replaying every internal step.

## 6. Read Resolution Contract

Foreground reads resolve data in this order:

1. active writer state / private overlay for the current mount and handle;
2. local authority view in local `redb`;
3. local cache / S3 fetch for the selected immutable blocks of an already
   known published version;
4. zero-fill for uncovered holes.

Implications:

- Sparse writes and overlapping writes are legal and resolved by per-byte
  precedence.
- A local mount never waits on PostgreSQL to answer a foreground read.
- A peer mount does not consult another client's private overlay.
- Readahead and prefetch operate on the effective local merged view after this
  rule is applied.
- `readdir` and later `open` are not one atomic promise. A name seen in a
  directory listing may legitimately disappear or move before a later `open`.

## 7. Namespace, Version, And Conflict Contract

Namespace behavior follows the same Local-First plus eventual-consistency model
as file contents.

- Directory entries, attrs, xattrs, timestamps, and ACL/ownership-style
  metadata are part of the local authority view and replication stream.
- Metadata-only updates publish through PostgreSQL without waiting on S3,
  because no new payload needs to be made durable first.
- A new file is invisible to peers until its first complete published version
  exists. Peer mounts must not see a placeholder name without a usable first
  version behind it.
- Once a file already has a published version, later content updates may become
  visible to peers incrementally as successive published versions, but every
  visible version must be self-consistent and fully readable.
- `rename`/`move` of files and directories are atomic metadata transitions.
  Peers must not observe half-moved namespace state.
- If an application uses delete-and-recreate or temp-file-plus-rename save
  patterns, peer mounts may observe exactly those published namespace effects:
  disappearance, later reappearance, and atomic replacement are all valid.
- Path-level LWW is the conflict rule. The winner is the update that arrives
  later to PostgreSQL. Client-local wall-clock timestamps are not part of the
  contract.
- Delete plus recreate at the same path is treated as a new object state at
  that path. The contract does not promise eternal hidden object identity
  across delete/recreate.
- Each client sees a merged namespace view of `shared applied state + my
  private overlay`. Foreign changes do not erase the local client's own
  unpublished changes from its foreground view.
- If a foreign rename/move of the same object arrives while a client still has
  unpublished content changes but has not locally renamed it, the client's
  private overlay follows the renamed object/path binding instead of creating a
  duplicate phantom object.

## 8. Open-Handle, Delete, And Rename Snapshot Contract

Open handles follow snapshot-style semantics similar to a network share:

- Already opened handles continue to operate on the version/path binding they
  opened with until close.
- New opens resolve against the latest applied shared state plus the opening
  client's own private overlay.
- If a file is deleted after another client already opened it, new opens by
  name fail after the delete is applied, but the already opened handle may keep
  reading/writing its prior view until close.
- If a file is renamed or moved after another client already opened it, the
  opened handle keeps working, while new path-based lookups use the new name.
- If a new version of a file is published while another client already has the
  old version open, the old handle keeps its old version and only new opens see
  the new published version.

## 9. Global File-Lock Contract

Global file locks are always enabled. They are not an optional product mode.

- PostgreSQL is the global coordinator for hard-lock state.
- Hard-lock grant/release state is distributed to clients through PG-backed
  lock coordination and pub/sub notifications.
- Lock state uses TTL/lease semantics. A healthy holder renews its lock lease;
  an abandoned lease eventually expires and unlocks automatically.
- If a client disconnects from PostgreSQL, it must continue honoring lock state
  that was already known before the disconnect. It may miss newer foreign lock
  events until reconnect.
- A hard lock does not retroactively evict an already active conflicting
  writer. Existing conflicting write handles may finish; new conflicting
  write-open/lock requests after the lock becomes known must fail or wait.
- While another client holds a hard lock, peers may open the file read-only but
  must not perform conflicting `write`, `truncate`, `rename`, `move`, or
  `delete` operations.
- File locks are the normal foreground case where mounted clients talk to
  PostgreSQL directly.

## 10. Metadata / PostgreSQL Responsibility Contract

The responsibility split is:

- local `redb`: the only foreground metadata authority for a mounted client;
- local payload backend/cache: the only foreground payload authority for accepted
  local writes;
- PostgreSQL: bootstrap source, ordered replication bus, checkpoint source,
  client discovery, preallocated range reservation, and global file-lock
  authority;
- S3/object storage: payload replication target and cold payload source for
  already known published blocks.

Rules:

- Ordinary mounted IO must not block on PostgreSQL metadata reads/writes.
- New clients bootstrap from PostgreSQL into a local `redb`, then serve
  foreground IO locally.
- Foreign changes are applied into local `redb` asynchronously and become
  visible through local invalidation/apply.
- Per-volume inode window persistence remains mandatory. A discovered client
  reuses its local `current`/`prefetched` inode windows across restarts and
  reserves a new per-volume window from PostgreSQL only when no local inode
  window remains available.
- Per-client block preallocation also remains mandatory. A discovered client
  obtains a persistent PG client slot, derives disjoint local payload-ID ranges
  from it, and allocates foreground object IDs without further PG round-trips
  while the current block range still has headroom.

## 11. Mutation Visibility And Invalidation Matrix

| Mutation or transition | Visibility contract | Invalidation / replication contract |
|---|---|---|
| `write` | The writing mount reads accepted bytes immediately from writer state/private overlay. Other mounts see nothing yet. | No PG round-trip is required before returning. |
| Explicit `flush` | Reaches a local-durability barrier for dirty unpublished state and materializes accepted bytes into the local authority view. It does not wait for PG replication or S3 upload. | Replication may still be pending after success. |
| `fsync` | Requests a strict local crash-safe boundary for the current inode/handle state. It still does not imply remote visibility. | May flush pending local sync work before returning; PG/S3 remain asynchronous. |
| Last-handle `close` / `release` | Completes local materialization for remaining dirty state and may perform the fallback local-durability barrier, but must not force synchronous PG/S3 replication. | Any remaining replication/upload work stays in background backlog. |
| New-file create with payload | The writing mount sees the file immediately. Peers see nothing until the first complete version is publishable and then applied. | The first peer-visible namespace entry must not appear before that first complete published version exists. |
| Metadata-only create/rename/attr change | The writing mount sees the change immediately. Peers may see it after PG publish/apply because no new payload upload is required. | Remote visibility follows PG publish/apply order. |
| Published-file content update | The writing mount sees the new content immediately. Peers keep seeing the previous published version until the next complete published version is ready and applied. | Each peer-visible step must reference payload that is already durable in S3. |
| `rename` / `move` | Source disappearance and destination appearance are immediate on the writing mount's local authority view. | Peers converge later through one atomic PG replay/apply transition. |
| Directory `rename` / `move` | The writing mount sees the directory move immediately. | Peers must not observe half-moved directory trees; namespace transition is atomic at metadata level. |
| `unlink` / `rmdir` | Removal is immediate on the writing mount. | Other mounts converge later through PG replay/apply; payload cleanup remains asynchronous. |
| `truncate` / size-changing `setattr` | New EOF and attrs are visible immediately on the writing mount and become strict-local when the requested local sync boundary completes. | Peers converge through background publication of the next complete version. |
| Upload completion | Payloads may already exist in S3, but peers do not see the new file version until the metadata switch is published and applied. | No direct namespace visibility switch happens on upload alone. |
| PG replication event publish | The new version becomes eligible for peer visibility. | Peers still see it only after local apply into their own `redb`. |
| Remote event apply | A mount converges its local authority view toward foreign changes while keeping its own private overlay ahead locally when conflict policy says so. | Namespace epochs and affected attrs/dentries invalidate locally before serving newer applied state. |
| Delete before publish | The writing mount removes the file locally. Peers never observe the unpublished file/version. | Any already uploaded blocks become orphaned payload and are later cleaned by janitor. |

## 12. Borrowed JuiceFS Ideas By Subsystem

| StudioFS area | Current StudioFS paths | JuiceFS reference files | What is borrowed |
|---|---|---|---|
| Regular-file vocabulary and schema | `crates/studiofs-meta/src/redb_engine/{blocks.rs,chunk_state.rs,replication.rs}` | `docs/en/introduction/architecture.md`, `pkg/meta/slice.go`, `pkg/meta/*.go` | Chunk/slice layout language, immutable payload references, slice overlap model |
| Writer path | `crates/studiofs-vfs/src/local_dirty.rs`, `crates/studiofs-vfs/src/vfs/upload_lifecycle.rs` | `pkg/vfs/writer.go` | Per-file writer ownership, chunk/slice decomposition, cheap local-first write acceptance, and incremental flush behavior |
| Reader path, readahead, prefetch | `crates/studiofs-vfs/src/vfs/read_path.rs`, `crates/studiofs-vfs/src/readahead.rs`, `crates/studiofs-vfs/src/vfs/prefetch.rs` | `pkg/vfs/reader.go`, `pkg/chunk/prefetch.go`, `docs/en/guide/cache.md` | `dataReader`-style resolution, adaptive readahead, whole-block prefetch |
| Cache, staging, object-store handoff | `crates/studiofs-cache/src/{cached_store.rs,runtime.rs,block_*.rs,startup_recovery.rs}`, `crates/studiofs-data/src/store.rs` | `pkg/chunk/cached_store.go`, `pkg/chunk/cache_eviction.go`, `docs/en/guide/cache.md` | Cached-store lifecycle, cache/download/writeback structure, eviction and background upload concepts |
| Metadata authority model | `crates/studiofs-meta/src/{redb_engine,pg_engine}.rs` | `docs/community/getting-started/standalone`, `docs/community/getting-started/for_distributed` | Standalone-style local metadata hot path, but with StudioFS-specific async replication to peers through PostgreSQL instead of a shared foreground DB |
| Metadata-cache and operator surface | `crates/studiofs/src/cli/mount/*.rs`, `docs/configuration.md` | `cmd/flags.go`, `cmd/mount.go`, `cmd/mount_unix.go`, `cmd/mount_windows.go`, `docs/en/guide/cache.md` | Coherent mount/cache flags, strong internal metadata-cache defaults, operator-facing terminology |
| Bridge and mount behavior | `crates/studiofs-fuse/src/lib.rs`, `crates/studiofs-winfsp/src/*.rs` | `pkg/vfs/vfs.go`, `pkg/vfs/vfs_unix.go`, `pkg/vfs/vfs_windows.go`, `cmd/mount*.go` | Performance-sensitive bridge/mount patterns and callback expectations |

## 13. Current StudioFS Concepts: Delete, Retain, Rename, Simplify

| Current concept | Decision in this rewrite |
|---|---|
| `WriteBuffer` as the canonical architecture | Delete as the target design. Transitional code may remain only until slice-writer replacement lands. |
| `ReadPipeline` as the canonical foreground read engine | Delete as the target design. Replace with a direct merged reader over writer/local authority/block state. |
| `UploadCoordinator` as the canonical upload architecture | Simplify/delete as a top-level design concept. Replace with one cached-store-style staging/upload pipeline. |
| `published` / `local unpublished` as separate architecture authorities | Simplify. They may remain as internal lifecycle labels inside local metadata, but they must not imply PG as a foreground authority. |
| `cache_safety_mode=fast|durable` as a product-level mount contract | Deleted from the product contract and implementation. A single Fast Local Authority mount model is the target. |
| Versioned local runtime subtree as product architecture vocabulary | Resolved. The UUID-scoped cache root is now the runtime boundary; no nested versioned subtree is part of the product model. |
| File-version atomic visibility | Retain. Peers must not observe half-published file versions. |
| `STUDIOFS_*` tuning sprawl | Simplify into a JuiceFS-inspired CLI/config surface with only transitional env vars where necessary. |

## 14. Rust-Native Implementation Rules

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
- The contract defines externally observable semantics, not one mandatory
  internal pipeline. Queueing, WAL layout, batching, coalescing, cache shape,
  and worker decomposition may change as long as the semantics in this
  contract remain true.

Review guardrails:

- Reject code that drags PostgreSQL into ordinary mounted read/write hot paths.
- Reject APIs that make per-inode or per-block foreground allocation depend on
  a PG round-trip once the local inode windows or local payload ranges are
  already reserved.
- Reject designs that require remote readers to reason about another mount's
  private overlay.
- Reject generic `anyhow`-style library errors where callers need domain
  decisions.
- Reject publication paths that expose a peer-visible version before every
  referenced payload object is already durable in S3.
- Reject rewrite decisions whose main purpose is preserving legacy pre-rewrite
  behavior that conflicts with this contract. Backward-compatibility with the
  old mount mode is not a design goal when it weakens the target semantics.

## 15. Historical Docs And Issues That Are No Longer Authoritative

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

## 16. Glossary For Downstream Issues

| Term to use in new issues | Type | Meaning |
|---|---|---|
| `chunk`, `slice`, `block` | Borrowed JuiceFS concept | The rewrite's core data vocabulary. |
| `writer state` | Borrowed-and-adapted concept | Local transient state that serves same-mount reads before local metadata materialization finishes. |
| `private overlay` | StudioFS target semantic | This client's unpublished local state layered over the latest shared-applied state. |
| `local authority view` | StudioFS target semantic | The local `redb`-backed metadata truth used by this mount for foreground IO. |
| `published version` | StudioFS target semantic | A peer-visible file state whose payload requirements are already satisfied and whose metadata switch has been published. |
| `replication event` | StudioFS target semantic | Ordered metadata delta published through PostgreSQL for peer convergence. |
| `replicated shared state` | StudioFS target semantic | State that peers may observe after they replay/apply replication into local `redb`. |
| `Local-First` | Retained StudioFS semantic | The writing mount observes accepted changes immediately through the merged local view. |
| `eventual consistency` | Retained StudioFS semantic | Other mounts converge later through upload plus replication/apply, not synchronously on each write. |
| `cached-store pipeline` | Borrowed JuiceFS concept | Unified cache, staging, download, upload, and eviction pipeline. |
| `client slot` | StudioFS target semantic | Persistent PG-assigned client identity used for client-scoped block-ID ranges and other per-client coordination surfaces; inode allocation is no longer derived from the client slot. |
| `lock lease` | StudioFS target semantic | PG-coordinated hard lock with TTL/renewal semantics and pub/sub notifications. |
| `orphaned payload` | StudioFS target semantic | Uploaded payload that is not referenced by any published version and can later be removed by janitor. |
| `Fast Local Authority` | StudioFS target semantic | The single target mount contract: local foreground authority, PG for replication/bootstrap/locks, S3 for payload replication. |

Downstream issues should cite this file directly and then name the relevant
section instead of re-explaining the rewrite target.

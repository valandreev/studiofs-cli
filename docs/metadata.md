# Metadata Engine

This page is a current implementation snapshot for `studiofs-meta`. It is not
the canonical SFS-242 rewrite contract. The rewrite source of truth lives in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).
Historical terms such as `base` and `overlay` below describe current metadata
internals, not the final operator-facing rewrite vocabulary.

`studiofs-meta` exposes synchronous metadata operations through `MetaEngine`.
`studiofs-vfs` consumes metadata through a typed async boundary:
- `SyncMetaEngineAdapter<M>` (bridges any sync metadata principal via blocking execution)
- `execute_sync_meta(...)` (typed `tokio::task::spawn_blocking` helper; no `Any` downcast bridge)
- `run_meta(...)` in VFS (maps runtime `JoinError` separately from metadata-domain errors)

`MetaEngine` now acts as an aggregate capability contract built from focused
sub-traits:
- `MetaFormat`
- `MetaInodes`
- `MetaNamespace`
- `MetaFsOps`
- `MetaBlocks`
- `MetaGc`
- `MetaRecovery`
- `MetaSymlink`
- `MetaXattr`
- `MetaAcl`
- `MetaSnapshots`
- `MetaReplication`

`MetaEngineCore` now contains only cross-cutting engine responsibilities
(global janitor lease + global snapshot pins), while filesystem and metadata
operations are split across capability traits. Consumers can depend on narrower
contracts instead of a single god-object surface. Capability traits are opt-in
per backend (`impl MetaNamespace for ...`, etc.), so incomplete backend
contracts fail at compile time.

## Capability Dependency Map

`MetaEngine` is retained as the compatibility aggregate for engine factories
such as `open_meta_engine(...)`, but it is no longer the default dependency for
mount/VFS hot paths.

Consumer-local bundles now define the active execution boundaries:
- mount open/session bootstrap:
  - `MountOpenMeta = MetaFormat + MetaReplication`
  - used for UUID consistency checks and initial `FormatInfo` loading
- snapshot mount source:
  - `SnapshotSourceMeta = MetaSnapshots`
  - used only to resolve and materialize snapshot payloads before VFS startup
- mount runtime handoff:
  - `MountRuntimeMeta` narrows the post-resolution dependency to the VFS-facing
    runtime bundle instead of carrying `MetaEngine` through the mount path
- VFS shared runtime:
  - `VfsMeta` is the common executor surface for `run_meta(...)`
- VFS helper bundles:
  - `ReadPathMeta` for lookup/getattr/block-resolution helpers
  - `AclMeta` for effective ACL traversal helpers
  - `UploadMeta` for upload/commit helpers
  - `JanitorMeta` for GC/recovery/janitor helpers

Dependency rule:
- use the smallest consumer-local bundle that matches the operation boundary
- keep `MetaEngine` at compatibility-oriented entrypoints only
- do not reintroduce a service-locator-style aggregate wrapper around metadata

## Engines

### `RedbMetaEngine`

Local embedded serving store.
- ACID local transactions
- foreground source of truth for the mounted client
- stores local overlay attrs/dentries/symlinks/xattrs, remote-visible base attrs/dentries/symlinks/xattrs, chunk-local regular-file overlay/base graphs, counters, and the local event log
- regular-file graphs are stored as chunk-local rows in `regular_file_overlay` and `regular_file_base`, keyed by `(ino, chunk_id)`

### `PgMetaEngine`

Distributed wrapper around `RedbMetaEngine`.

Current behavior:
- foreground ops (`create`, `rename`, `unlink`, `truncate`, block staging) mutate local `redb`
- local mutations append to the local metadata event log
- background flush projects remote-visible deltas into PostgreSQL `event_log`
- background pull applies only newer foreign delta batches
- restart/reopen rebuilds pending replication state from the local event log when
  unpublished local overlay mutations still exist
- foreign namespace/attr/symlink/xattr apply updates the local `base_*` view
- local serving view is `overlay over base`
- `metadata_state` is used as a bootstrap/checkpoint store, not the primary runtime sync channel

### Backend Selection on Open

`open_meta_engine_with_options(...)` selects backend in two phases:

1. Probe `FormatInfo` via `RedbMetaEngine::read_format_info_readonly(path)` (read-only open, no exclusive write lock).
2. If `metadata.backend == Postgres`, open `PgMetaEngine`; otherwise open `RedbMetaEngine`.

Safety and fallback behavior:
- If the metadata DB file is missing (`NotFound`), readonly probe is treated as `NotFormatted`, and `RedbMetaEngine::open(...)` creates/opens local metadata.
- If readonly probe returns `NotFormatted`, `RedbMetaEngine::open(...)` is used.
- If readonly probe returns a storage/corruption error, the error is returned (no blind RW retry).
- Post-open validation re-checks format version and backend consistency to reduce TOCTOU risk windows during backend handoff.

## Volume Identity and UUID Verification

Each StudioFS volume is assigned a unique `disk_uuid` during initialization (`studiofs init`). This UUID is stored in both the remote metadata store (PostgreSQL) and the local metadata file (`.db` via `redb`).

### UUID Mismatch Protection

To prevent accidental data corruption or inconsistent states when a volume is deleted and recreated with the same name, StudioFS performs a mandatory UUID verification during `mount` and `destroy` operations:

- **Mount**: Before starting the filesystem, the client connects to the remote PostgreSQL database and compares the local UUID with the remote one. If they do not match, the mount is blocked.
- **Destroy**: Before performing a remote destruction of S3 data and PostgreSQL tables, the UUID is verified. If a mismatch is detected, the command will fail and suggest using `--local` to only clean up the stale local metadata.

### Offline Behavior

If the remote PostgreSQL database is unreachable during a `mount` or `destroy` command:
- **Mount**: The operation is blocked because the identity of the disk cannot be verified.
- **Destroy**: The command will warn that the identity could not be verified and will only allow a local cleanup via the `--local` flag.

### Troubleshooting

If you see a "Disk recreated" error, it means your local metadata belongs to a previous instance of a disk with the same name. To resolve this, you must remove your local state:
```bash
# Windows
studiofs.exe destroy <name> --local

# Linux/macOS
studiofs destroy <name> --local
```
After cleaning up locally, you can use `studiofs discover` to connect to the new instance of the disk.

## Sync Model

### Local vs Remote-Visible State

For regular files StudioFS keeps two views:
- local view:
  - `size`
  - local-visible regular-file overlay graph in `regular_file_overlay`
- remote-visible view:
  - `committed_size`
  - remote-visible regular-file base graph in `regular_file_base`

This is required so a client can finish local writes immediately while remote
clients only see blocks that have actually finished uploading.

Regular-file graph rules:
- slices are chunk-relative and stay within a single chunk
- overlay and base graphs resolve as `overlay over base`
- `committed_size` follows the remote-visible committed state derived from `regular_file_base`

For namespace, attrs, symlink targets, and xattrs StudioFS also keeps two views:
- base view:
  - `base_inodes`
  - `base_dentries`
  - `base_symlinks`
  - `base_xattrs`
- local overlay view:
  - current local inode attrs in `inodes`
  - current local dentries in `dentries`
  - current local symlink targets in `symlinks`
  - current local xattrs in `xattrs`
  - local tombstones in `symlink_tombstones` / `xattr_tombstones`

Serving rules are:
- `getattr`: overlay wins, otherwise fall back to base
- `lookup`: overlay wins, otherwise fall back to base
- `readdir`: merged `overlay over base`
- `readlink` / `getxattr` / `listxattrs`: merged `overlay over base`

### Flush to PostgreSQL

1. Read the pending local event range from the local event log.
2. Project events into a remote-visible stream.
3. Start a PostgreSQL transaction.
4. Bump `metadata_state.version` and use that value as the batch cursor.
5. Append projected events into `event_log`.
6. Periodically refresh the remote-visible checkpoint in `metadata_state`.

Events that stay local-only are not published as remote-visible deltas:
- local regular-file overlay graph staging (`StageRegularFileOverlaySlices` and upload-state marks)
- pending deletion churn
- local regular-file `size` ahead of `committed_size`

Operational note:
- `flush_metadata()` is explicit "flush then pull".
- `sync_metadata()` is pull-only; it does not publish local pending metadata.
- on reopen, unpublished local events between `flushed_event_seq` and
  `nextEventSeq` are reconstructed into pending replication state when the local
  overlay is still live, so ordinary recovery should not require wiping local
  metadata or running `discover`.

### Pull from PostgreSQL

1. Load only rows with `batch_version > local_version`.
2. Decode batches in `(batch_version, client_id)` order.
3. Apply foreign batches in order.
4. Apply self batches only to remote-visible/base reconciliation (not as local overlay writes).
5. Use self batches to acknowledge already-published local event prefixes and
   advance the local flushed watermark after restart/crash windows.
6. Do not skip earlier foreign batches just because the client has already
   published a newer self batch.
7. Apply foreign namespace/attr/symlink/xattr state into local `base_*`.
8. Skip auxiliary serving-view updates for events that conflict with local overlay state and continue applying later
   independent batches.

This avoids replaying a client's own changes and avoids clobbering local staged
writes with foreign state, while still advancing the remote-visible base view.

## ACL Roles (Local-First)

StudioFS supports per-inode ACL rules for principals with three access levels:
- `no_access`
- `ro`
- `rw`

Rules are stored locally first and replicated to PostgreSQL eventually.

Effective ACL is evaluated in VFS by walking parent directories upward:
- first matching rule on the path wins
- per inode, principal lookup checks explicit principal first, then wildcard `*`
- if no rule is found, effective access is `rw`

ACL updates are appended to the metadata event log and participate in the same
flush/pull replication flow as other metadata events.

## PostgreSQL Tables

- `volumes`
- `event_log`
- `metadata_state`
- `acl_rules`
- `snapshots`

`event_log` is the runtime delta stream. `metadata_state` stores:
- the monotonic `version`
- the latest remote-visible checkpoint snapshot

`snapshots` stores immutable historical views:
- `id` (UUID)
- `root_ino`
- `created_at`
- `state` (`pending`, `synced`, `failed`)
- pinned object ids
- serialized snapshot payload used for read-only snapshot mounts

Current operational contract:
- `pending` means the snapshot exists in local metadata but is not yet guaranteed
  to be cluster-protected.
- cluster-wide protection begins only after snapshot metadata is published
  through PostgreSQL and visible to janitor / peer clients.
- if PostgreSQL is unavailable, snapshot creation may still succeed locally, but
  that snapshot is treated as a local-only administrative artifact until a
  later reconcile/publish step completes.

## Consistency

- local operations are synchronous and immediate for the current client
- cross-client visibility is eventual
- namespace visibility may arrive before all file blocks are committed
- remote-visible file size advances only with committed blocks
- foreign namespace can be present in local `base_*` before local overlay stops masking it
- stale async completions are rejected using `content_epoch` plus object IDentity checks
- crash/reopen recovery should converge through ordinary restart plus
  `sync_metadata()` / `flush_metadata()`; `discover` is bootstrap/manual repair,
  not the expected path for normal PG replication recovery

## MetadataConfig

Important fields:
- `backend = Postgres`
- `postgres.url`
- `postgres.volume_id`

Client runtime policy is not stored in the volume format.
PG batching / sync cadence is controlled per client through runtime settings
such as:
- `STUDIOFS_PG_FLUSH_INTERVAL_SECS`
- `STUDIOFS_PG_FLUSH_OPS_THRESHOLD`
- `STUDIOFS_PG_FLUSH_IDLE_MS`
- `STUDIOFS_PG_SYNC_POLL_SECS`

## Schema

Source of truth:
- `crates/studiofs-meta/src/schema.rs`

Compatibility:
- `crates/studiofs-meta/src/types.rs` re-exports from `schema.rs`

Design reference:

## Backend Modes

- `MetadataBackend::RedbLocal`
- `MetadataBackend::Postgres`

## Important Inode Fields

Regular files now use two size/generation concepts:
- `size`
  - local size visible to the current client
- `committed_size`
  - remote-visible size after committed uploads
- `content_epoch`
  - monotonic content generation used to reject stale async upload completions

## Important Regular-File Tables

Regular-file graph tables:
- `regular_file_overlay`
  - local-visible chunk/slice/payload-object graph rows, keyed by `(ino, chunk_id)`
- `regular_file_base`
  - remote-visible chunk/slice/payload-object graph rows, keyed by `(ino, chunk_id)`
- `SliceRange`
  - validated as chunk-relative half-open ranges and rejected if they cross the current chunk size

## PostgreSQL Shared Schema

The shared PostgreSQL schema uses:
- `volumes`
  - logical volumes within database `studiofs`
- `event_log`
  - append-only remote-visible metadata deltas
- `metadata_state`
  - per-volume checkpoint snapshot plus monotonic `version`
- `snapshots`
  - immutable historical views (definition + pinned object ids + payload)

Important cursor rule:
- runtime pull follows `batch_version`, not `seq_id`

## Config Fields

Persisted in `FormatInfo`:
- `name`
- `uuid`
- `endpoint`
- `bucket`
- `access_key`
- `secret_key`
- `region`
- `data_prefix`
- `block_size`
- `metadata.postgres.url`
  - PostgreSQL server DSN; StudioFS rewrites it to database `studiofs`
- `metadata.postgres.volume_id`

Not persisted in the volume format:
- PG flush cadence / op thresholds
- client-local metadata-cache defaults / invalidation cadence
- legacy `chunk_size`
- legacy per-volume format `version`

## Client Runtime Defaults

- `STUDIOFS_PG_FLUSH_INTERVAL_SECS = 60`
- `STUDIOFS_PG_FLUSH_OPS_THRESHOLD = 500`
- `STUDIOFS_PG_FLUSH_IDLE_MS = 5000`
- `STUDIOFS_PG_SYNC_POLL_SECS = 2`

## Base + Overlay Design

This document freezes the next metadata serving model for PostgreSQL-backed
volumes.

## Goal

Keep local foreground mutations fully local while allowing foreign PostgreSQL
batches to apply immediately whenever they do not conflict with local overlay
state.

The serving view becomes:

- `overlay over base`

Where:

- `base`
  - last remote-visible namespace and attrs applied from checkpoint + foreign PG
    batches
- `overlay`
  - local unflushed or not-yet-remote-visible mutations for the current client

## Tables

Regular-file runtime state is split into chunk-local overlay/base graphs:

- `regular_file_overlay`
  - local-serving chunk/slice/payload-object rows
- `regular_file_base`
  - remote-visible chunk/slice/payload-object rows

Namespace/attrs gain the same split:

- `base_inodes`
  - remote-visible attrs
- `inodes`
  - local replacements for attrs
- `base_dentries`
  - remote-visible namespace entries
- `dentries`
  - local namespace replacements and tombstones

Symlink/xattr metadata uses the same split:

- `base_symlinks`
  - remote-visible symlink targets
- `symlinks`
  - local symlink target replacements
- `symlink_tombstones`
  - local hides for base symlink targets
- `base_xattrs`
  - remote-visible xattrs
- `xattrs`
  - local xattr replacements
- `xattr_tombstones`
  - local hides for base xattrs

Still single-copy:

- `pending_deletions`
- `event_log`
- `counters`

Janitor policy:
- the janitor must consult PostgreSQL before deleting remote objects for
  PostgreSQL-backed volumes.
- snapshot pins published through PostgreSQL protect matching object ids from
  deletion.
- local-only pending snapshots created while PostgreSQL is down are not yet part
  of the cluster protection contract; they require a later reconcile/publish
  step to become globally protected.

ACL metadata follows the same base+overlay split:
- `base_acl_rules`
  - remote-visible ACL state applied from checkpoint/foreign PG batches
- `acl_rules`
  - local overlay ACL rules
- `acl_rule_tombstones`
  - local hides/removals for base ACL keys

## Overlay Records

### Overlay inode

`inodes[ino]` means:

- this inode has a local attr replacement
- reads must return overlay attr instead of base attr

No separate inode tombstone table is required in the first slice.

Inode deletion is represented by namespace tombstones plus absence of a local
replacement attr. Base inode cleanup happens when overwrite/unlink convergence
removes the last visible reference.

### Overlay dentry

`dentries[(parent, name)]` stores one of:

- `Present(DirEntryValue)`
  - local name resolves to this entry
- `Tombstone`
  - local view hides the base dentry at this name

This is enough for:

- create
- unlink
- rename source removal
- rename destination overwrite
- mkdir/rmdir

## Merge Rules

### `getattr(ino)`

1. If `inodes[ino]` exists, return it.
2. Else if `base_inodes[ino]` exists, return it.
3. Else `InodeNotFound`.

For regular files:

- overlay `size` may be ahead of base `committed_size`
- block reads resolve `regular_file_overlay over regular_file_base`

### `lookup(parent, name)`

1. Check `dentries[(parent, name)]`.
2. If it is `Tombstone`, return `EntryNotFound`.
3. If it is `Present(entry)`, return that entry.
4. Else fall back to `base_dentries[(parent, name)]`.

### `readdir(parent)`

Build the merged directory view as:

1. Start from all `base_dentries` for `parent`.
2. Apply every `dentries` entry for `parent`.
3. `Tombstone` removes the name from the result.
4. `Present(entry)` inserts or replaces the name in the result.

Result ordering should remain stable by name, matching current behavior closely
enough for callers and tests.

### `readlink(ino)`

1. If `symlink_tombstones[ino]` exists, return `InodeNotFound`.
2. Else if local `symlinks[ino]` exists, return it.
3. Else if `base_symlinks[ino]` exists, return it.
4. Else `InodeNotFound`.

### `getxattr(ino, name)` / `listxattrs(ino)`

1. If `xattr_tombstones[(ino, name)]` exists, treat the key as absent.
2. Else if local `xattrs[(ino, name)]` exists, return it.
3. Else fall back to `base_xattrs[(ino, name)]`.

`listxattrs` starts from `base_xattrs`, removes tombstoned keys, then overlays
local `xattrs`.

Resource-fork compatibility:
- `com.apple.ResourceFork` may be stored in a compressed framed representation
  inside metadata tables when it is smaller than plain bytes
- reads decode transparently before returning values to callers
- legacy plain persisted values are still accepted and returned unchanged

## Write Rules

### `create`

- Validate parent via merged `getattr`.
- Fail if merged `lookup(parent, name)` already exists.
- Write new attr to `inodes`.
- Write `Present(entry)` to `dentries`.

### `unlink`

- Resolve victim via merged `lookup`.
- Write `Tombstone` for `(parent, name)` into `dentries`.
- Update victim attr in `inodes` if `nlink` stays non-zero.
- If local view drops final link, remove local payload state and local overlay attr.
- Remote-visible removal is published later through PG flush.

### `rename`

Source side:

- resolve through merged `lookup`
- set source name to `Tombstone`

Destination side:

- if destination exists in merged view, treat it as overwritten
- write `Present(src_entry)` at destination name
- update moved inode attr in `inodes` when parent changes
- update overwritten inode attr in `inodes` if `nlink` drops but inode
  survives locally

### `truncate` / `setattr(size)`

- update attr only in `inodes`
- modify local-serving `regular_file_overlay`
- leave `base_inodes.committed_size` and committed blocks untouched until upload
  completion and PG flush publish remote-visible state

### `set_symlink_target`

- write only to local `symlinks`
- remove any local symlink tombstone for that inode

### `setxattr`

- validate flags against merged xattr visibility
- write only to local `xattrs`
- remove any local tombstone for that key
- for `com.apple.ResourceFork`, encode to compressed framed bytes only when
  beneficial; otherwise store plain bytes

### `removexattr`

- validate existence against merged xattr visibility
- remove the local `xattrs` entry for that key
- if the key exists in `base_xattrs`, write a tombstone

Migration behavior:
- no offline migration is required
- existing plain `com.apple.ResourceFork` rows remain readable
- rewrite-on-set progressively upgrades hot keys to compressed representation

## Foreign PostgreSQL Apply

Foreign PG apply writes only into `base_*`.

Rules:

- self-echo batches are still skipped
- foreign batches never write into overlay tables
- foreign namespace/attr state always advances local `base_*`
- foreign auxiliary state applies event-by-event
- conflicting auxiliary events are skipped locally until local overlay clears
- later independent events and batches still apply

Conflict means the foreign event touches a name or inode that has local overlay
state.

Current conflict set:

- inode is present in local payload overlay
- inode is present in local attr overlay
- inode is present in local symlink overlay
- xattr key is present in local xattr overlay

## Checkpoint Import / Export

### Import snapshot

- checkpoint snapshot populates `base_inodes`, `base_dentries`,
  `base_symlinks`, and `base_xattrs`
- local symlink/xattr overlay tables and tombstones are cleared
- serving view after import is still `overlay over base`

### Export remote snapshot

Checkpoint/exported remote-visible state must come from merged remote-visible
view:

1. Start from `base_*`
2. Apply only overlay mutations that are already remote-visible
3. Project regular-file attrs to committed size
4. Export `regular_file_base` only

Local-only staged state must never leak into checkpoint.

## Deterministic Convergence Targets

- unrelated foreign namespace changes appear immediately even with local overlay
  elsewhere
- remote `unlink` removes the name as soon as the batch is applied to base and
  no local overlay masks it
- rename overwrite converges to a single visible destination entry
- local overlay always wins for serving until local state is flushed or cleared

## Migration Order

1. Add `base_*` and `overlay_*` tables plus record types.
2. Move checkpoint import and foreign PG apply into `base_*`.
3. Switch read path to merged `overlay over base`.
4. Convert write-side namespace ops to write only overlay state.
5. Tighten conflict detection from inode-scoped to exact inode/name scoped rules.

## SFS-45: orphan/dangling namespace recovery

To harden interrupted-copy and open-unlink crash scenarios, metadata and VFS now
use a unified undo-recovery path with explicit intent records.

### Undo-intent records

`MetaRecovery` exposes:

- `pending_creates` (create intent journal)
- `deferred_deletes` (open-unlink orphan journal)
- `undo_log` with:
  - `kind`: `PendingCreate` / `DeferredDelete`
  - `phase`: `Started` / `Replaying` / `Completed`

`phase` is serialized with a default (`Started`) so older persisted records
without the field remain decodable.

### Atomicity and recovery behavior

- Create paths (`create_file/create_dir/create_symlink`) register pending intent
  before mutation and clear it on completion.
- Open-unlink uses atomic metadata mutation (`unlink_open_handle`) and persists
  deferred-delete intent in the same transaction.
- `recover_undo_logs()` is the primary replay entry point:
  - heals dangling pending-create dentries
  - replays deferred-delete cleanup (blocks/attrs/deferred records)
  - returns touched parent inodes for namespace cache invalidation
- `recover_undo_logs()` is intended for startup/crash recovery.
- Runtime janitor replays only pending-create intents (`recover_pending_creates()`)
  and does not finalize deferred-delete records for still-open unlinked files.

### Self-healing expectations

- Dentries without live inode attrs are eventually removed by janitor recovery.
- Deferred open-unlink orphan state is finalized after restart/runtime replay.
- Undo-log records are lifecycle-managed and post-cleaned to avoid long-term
  accumulation under repeated create-cancel / open-unlink cycles.

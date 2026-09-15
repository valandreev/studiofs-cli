# Metadata Engine

This page describes `studiofs-meta` under the canonical mounted-IO contract in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).
The canonical PostgreSQL lifecycle, replay, and authority contract is
[pg-sync-contract.md](./pg-sync-contract.md).
Terms such as `base` and `overlay` below are metadata-internal names.

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
- `MetaPayloadObjects`
- `MetaGc`
- `MetaRecovery`
- `MetaSymlink`
- `MetaXattr`
- `MetaAcl`
- `MetaReplication`

`MetaEngineCore` contains cross-cutting engine responsibilities such as the
global janitor lease and published Snapshot v2 artifact proofs, while filesystem
and metadata operations are split across capability traits. Consumers can
depend on narrower contracts instead of a single god-object surface. Capability
traits are opt-in per backend (`impl MetaNamespace for ...`, etc.), so incomplete
backend contracts fail at compile time.

## Contract/Core Test Harness

`studiofs_meta::contract_harness` provides the fast local Contract/Core test
surface for metadata correctness. It creates isolated `RedbMetaEngine`
fixtures, owns the temporary redb path, can drop and reopen the same metadata
database, applies deterministic filesystem-like operations, and runs common
invariants over:

- the root-reachable namespace tree
- dentry-to-attribute consistency
- reachable link counts for regular files and symlinks
- POSIX-style directory link counts (`2 + direct child directories`)
- regular-file object graph readability
- lifecycle rows that still point at live regular-file attrs, except retained
  deleted-before-publish tombstones
- deferred-delete records that explain open-unlink zero-link inodes

Invariant failures include a diagnostic snapshot of attrs, dentries, object
graphs, lifecycle rows, deferred-delete records, and all violated rules. A
zero-link inode that is no longer root-reachable must have a deferred-delete
record; otherwise the harness reports the orphaned inode directly. VFS-level
contract tests reuse the same runner through `studiofs_vfs::contract_harness`.

## Capability Dependency Map

`MetaEngine` is retained as the compatibility aggregate for engine factories
such as `open_meta_engine(...)`, but it is no longer the default dependency for
mount/VFS hot paths.

Consumer-local bundles now define the active execution boundaries:
- mount open/session bootstrap:
  - `MountOpenMeta = MetaFormat + MetaReplication`
  - used for UUID consistency checks and initial `FormatInfo` loading
- snapshot source reconstruction:
  - `SnapshotSourceReader`
  - reads one retained PostgreSQL serving checkpoint and its bounded event range
    into a clean replay database
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

Runtime behavior:
- foreground ops (`create`, `rename`, `unlink`, `truncate`, block staging) mutate local `redb`
- local mutations append to the local metadata event log
- background flush projects remote-visible deltas into PostgreSQL `event_log`
- background pull (SFS-699) applies newer foreign delta pages as complete
  `batch_version` units toward an explicit frozen target, with batch-count and
  response-byte limits
- restart/reopen rebuilds pending replication state from the local event log when
  unpublished local overlay mutations still exist
- foreign namespace/attr/symlink/xattr apply updates the local `base_*` view
- local serving view is `overlay over base`
- a valid local `redb` view is the serving boundary; PostgreSQL replay is not a
  prerequisite for local serving
- `metadata_state` is the checkpoint/bootstrap store, not the primary runtime
  sync channel

The bounded, frozen pull behavior above is implemented by
`query_foreign_event_page`. Its SQL selects an ordered prefix of complete
`batch_version` groups through the supplied target. The per-client defaults are
64 batches and 8 MiB of estimated PostgreSQL row response bytes; an environment
override may tune either limit without changing the volume schema.
Before returning a page, the transaction takes a shared per-volume advisory
lock and reads the retained event-log floor and checkpoint version from
`metadata_state`. The pruner takes the matching exclusive advisory lock, while
ordinary metadata version updates do not contend on the replay lock. A cursor
below the floor returns
`MetaError::RebootstrapRequired` with the checkpoint coordinates and is not
passed to foreign apply. A cursor at or above the floor continues normal
replay; structural missing or non-contiguous batches remain
`MetaError::ReplayGap`. Either condition records the client as `Degraded` and
read-only until checkpoint import/rebase recovery owned by SFS-712 is available.

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

- **Mount**: the client may establish local serving readiness from a valid
  local root independently of remote replay. UUID verification remains a
  separate safety/authority check; a mismatch blocks remote use and
  write-capable operation without turning replay into an unbounded mount wait.
- **Destroy**: Before performing a remote destruction of S3 data and PostgreSQL tables, the UUID is verified. If a mismatch is detected, the command will fail and suggest using `--local` to only clean up the stale local metadata.

### Offline Behavior

If the remote PostgreSQL database is unreachable during a `mount` or `destroy` command:
- **Mount**: an existing valid local root can become locally serving-ready
  without replaying remote history. Catch-up and any missing write authority
  are reported separately, and a fresh client remains read-only until its
  checkpoint, startup target, and authority requirements are satisfied.
- **Discover**: a new client cannot complete checkpoint-first bootstrap without
  a valid remote checkpoint, but an already imported local checkpoint remains a
  recoverable local/read-only state for a later catch-up attempt.
- **Destroy**: `destroy --local` performs only local cleanup and does not
  contact PostgreSQL or S3. Full remote destruction retains its separate
  identity and remote-service requirements.

Remote UUID verification remains a separate safety check for operations that
need remote identity. An identity result must not be treated as a replay result
or used to add an unbounded wait to local serving readiness.

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
clients only see payload objects that have actually finished uploading.

Regular-file graph rules:
- slices are chunk-relative and stay within a single chunk
- overlay and base graphs resolve as `overlay over base`
- `committed_size` follows the remote-visible committed state derived from `regular_file_base`

### Local Regular-File Slice API

The mounted data plane should build locally durable file-content commits with
`LocalSliceBatch` / `LocalSliceRecord` rather than redb table rows.

API rules:
- `LocalSliceRecord` carries `ino`, `chunk_id`, chunk-relative offset/length,
  optional payload object id, object offset, and caller-side sequence.
- `LocalSliceRecord::data(...)` and `LocalSliceRecord::hole(...)` reject any
  single slice that crosses `DEFAULT_CHUNK_SIZE`.
- `LocalSliceBatch::append_data_range(...)` and
  `LocalSliceBatch::append_hole_range(...)` accept file-coordinate ranges and
  split them into chunk-local records.
- `LocalSliceBatch::into_chunk_slices()` converts the batch into the
  `(ChunkId, Vec<SliceRecord>)` shape consumed by
  `flush_regular_file_overlay_slices_with_attr(...)`, which commits slice rows
  and local inode attrs in one local metadata transaction for redb-backed
  engines.

Holes and truncate masks are represented as tombstone records
(`payload_object_id == None`) and participate in the same per-byte overlap
resolution as data-bearing slices.

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
6. Refresh the remote-visible checkpoint in `metadata_state` often enough to
   satisfy the explicit `max_checkpoint_lag_versions` bound. A periodic timer
   may trigger refresh, but periodic work alone is not the freshness contract.

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
2. Freeze one target `metadata_state.version` for the current sync operation.
3. Read and apply bounded pages in `(batch_version, seq_id)` order, never past
   the frozen target.
4. Apply self batches only to remote-visible/base reconciliation (not as local overlay writes).
5. Use self batches to acknowledge already-published local event prefixes and
   advance the local flushed watermark after restart/crash windows.
6. Do not skip earlier foreign batches just because the client has already
   published a newer self batch.
7. Apply foreign namespace/attr/symlink/xattr state into local `base_*`.
8. Skip auxiliary serving-view updates for events that conflict with local overlay state and continue applying later
   independent batches.

After each page, the local serving-state changes and replay cursor are
committed together. A failed page resumes from the previous cursor; newer
remote events are handled by a later bounded sync operation rather than
extending the current operation toward a moving head.

This avoids replaying a client's own changes and avoids clobbering local staged
writes with foreign state, while still advancing the remote-visible base view.

### Metadata invalidation wakeups

PostgreSQL-backed clients use a push-plus-poll invalidation model:

- every remote-visible event-log batch emits a volume-scoped PostgreSQL
  `NOTIFY` on `studiofs_meta_vol_<volume_id>` after the transaction commits
- peer metadata listeners treat the notification payload as a wakeup hint only
- the listener wakes `sync_metadata()` and the VFS metadata invalidation worker;
  the event log and version tokens remain the source of truth
- listener startup/reconnect forces an immediate metadata sync before waiting
  for the next notification, so changes missed while offline can still converge
- periodic `STUDIOFS_PG_SYNC_POLL_SECS` polling remains enabled as the fallback
  for missed notifications, network stalls, process pauses, and listener restarts

The metadata listener performs the event-log pull after local serving state is
available and before waking the VFS
invalidation worker. On a notify-driven wake, the worker first compares local
namespace and metadata version tokens with the last tokens it processed. If the
listener already advanced either token, the worker skips the fallback poll's
remote event-log query and proceeds directly to cache invalidation from the
local metadata view. Periodic fallback polls still always run a remote sync, so
lost notifications cannot be hidden by unrelated local token changes. If a
notify wake arrives without a token change, the worker may still issue an empty
event-log query; that is the conservative path that preserves the hint-only
contract for future backends.

The metadata channel is separate from the file-lock lease notification channel
(`studiofs_lock_vol_<volume_id>`). Lock NOTIFY updates known lock state only;
metadata NOTIFY wakes namespace, attr, payload publication, ACL, snapshot, and
delete/cleanup replication through the ordinary event-log replay path.

Available metadata invalidation diagnostics are best-effort counters:
notifications received, listener reconnects, fallback polls that discovered
remote changes, sync errors, and the last successful metadata sync timestamp.

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
- `snapshot_catalog`
- `snapshot_creation_jobs` / `snapshot_creation_leases`
- `snapshot_source_checkpoints`
- `snapshot_artifacts` and payload proofs
- `snapshot_policies` and policy leases

`event_log` is the runtime delta stream. `metadata_state` stores:
- the monotonic `version`
- the latest remote-visible `ServingCheckpoint` and its `checkpoint_version`
- the inclusive `event_log_prune_floor` and cumulative `event_log_pruned_rows`
  retention diagnostics
- bounded bootstrap `format_info`, kept separate from serving state

Background PostgreSQL maintenance prunes only complete event batches at or
below the minimum of the fully published checkpoint and every retained
Snapshot v2 source's first event version minus one. Creating, ready, failed,
and deleting source rows remain guards until their immutable source row is
removed; empty sources do not pin event history. Each transaction removes a bounded
number of batch groups and advances `event_log_prune_floor` only when all lower
rows are gone. This work is outside foreground writes, flushes, and startup;
SFS-711 owns explicit replay-gap/rebootstrap-required reporting for cursors
below the persisted floor; checkpoint import/rebase recovery consumes this
signal in SFS-712.

`snapshot_catalog` stores thin lifecycle/control rows. The immutable source
row stores only the captured serving-checkpoint bytes, checksum, checkpoint
version, and bounded event sequence/version boundaries. It contains no root-
inode selection, object-id array, or read-only snapshot database payload.

Current operational contract:
- `Creating` is a PostgreSQL-authoritative control-plane state; the retained
  source is immutable while a worker may still claim the snapshot.
- source reconstruction reads the retained checkpoint and only the recorded
  event range, so later serving events cannot enter the cut.
- artifact publication, Ready mounting, expiry, and deletion are follow-up
  Snapshot v2 workflows; no local redb snapshot is authoritative for them.

## Consistency

- local operations are synchronous and immediate for the current client
- cross-client visibility is eventual
- namespace visibility may arrive before all file payload objects are committed
- remote-visible file size advances only with committed payload objects
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
- `STUDIOFS_PG_CONNECT_TIMEOUT_MS`
- `STUDIOFS_PG_COMMAND_TIMEOUT_MS`
- `STUDIOFS_PG_SYNC_MAX_BATCHES_PER_PAGE`
- `STUDIOFS_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE`

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
  - per-volume serving checkpoint, checkpoint cursor, format bootstrap bytes,
    metadata/PG sync format version, checksum, publication state/generation,
    and monotonic `version`
- `snapshot_catalog`
  - thin Snapshot v2 lifecycle/control rows
- `snapshot_source_checkpoints`
  - immutable checkpoint bytes and bounded replay boundaries per captured cut

Important cursor rule:
- runtime pull follows `batch_version`, not `seq_id`

Compatibility boundary:
- format version `5` is required in both local `redb` metadata and
  `metadata_state.metadata_format_version`
- schema setup never promotes legacy rows to version `5`; old rows fail startup
  and must be recreated or used as the source for a fresh `studiofs discover`
- the durable applied replay cursor remains local redb state; a startup target
  is an ephemeral catch-up-session value

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
  - PostgreSQL server DSN; StudioFS normalizes it to database `studiofs`
- `metadata.postgres.volume_id`

Not persisted in the volume format:
- PG flush cadence / op thresholds
- PostgreSQL command-pool capacity
- client-local metadata-cache defaults / invalidation cadence

## Client Runtime Defaults

- `STUDIOFS_PG_FLUSH_INTERVAL_SECS = 60`
- `STUDIOFS_PG_FLUSH_OPS_THRESHOLD = 500`
- `STUDIOFS_PG_FLUSH_IDLE_MS = 5000`
- `STUDIOFS_PG_SYNC_POLL_SECS = 2`
- `STUDIOFS_PG_POOL_SIZE = 4`
- `STUDIOFS_PG_SYNC_MAX_BATCHES_PER_PAGE = 64`
- `STUDIOFS_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE = 8388608`

`STUDIOFS_PG_POOL_SIZE` is a positive-integer per-client runtime override within
the runtime semaphore capacity. It takes precedence over
`MetaOpenOptions::pg_pool_size` and
`PgOpenOptions::pg_pool_size`; those runtime fields default to four. The value
is not persisted in the volume metadata schema. Size the aggregate of all
client pools below PostgreSQL `max_connections`, leaving headroom for
administrative and other server connections.

## Base + Overlay Design

This document defines the metadata serving model for PostgreSQL-backed volumes.

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
- published Snapshot v2 artifact payload proofs protect matching object ids from
  deletion.
- a Creating source remains retained by its PostgreSQL control-plane row until
  the snapshot lifecycle can safely release it.

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
- plain persisted values are accepted and returned unchanged

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
- leave `base_inodes.committed_size` and committed payload objects untouched until upload
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

Update behavior:
- the PostgreSQL sync format/protocol change is breaking; migration
  compatibility is not required for existing test disks
- existing plain `com.apple.ResourceFork` rows remain readable
- setxattr progressively stores hot keys in compressed representation when it
  wins on size

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

## Serving Checkpoint Import / Export

### Import serving checkpoint

- a `ServingCheckpoint` populates `base_inodes`, `base_dentries`,
  `base_symlinks`, and `base_xattrs`
- local symlink/xattr overlay tables and tombstones are cleared
- serving view after import is still `overlay over base`

### Export serving checkpoint

Checkpoint/exported serving state must come from merged remote-visible
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

`phase` is serialized with a default (`Started`) so persisted records without
the field remain decodable.

### Atomicity and recovery behavior

- Create paths (`create_file/create_dir/create_symlink`) register pending intent
  before mutation and clear it on completion.
- Open-unlink uses atomic metadata mutation (`unlink_open_handle`) and persists
  deferred-delete intent in the same transaction.
- `recover_undo_logs()` is the primary replay entry point:
  - heals dangling pending-create dentries
  - replays deferred-delete cleanup (payload objects/attrs/deferred records)
  - returns touched parent inodes for namespace cache invalidation
- `recover_undo_logs()` is intended for startup/crash recovery.
- Runtime janitor replays only pending-create intents (`recover_pending_creates()`)
  and does not finalize deferred-delete records for still-open unlinked files.

### Self-healing expectations

- Dentries without live inode attrs are eventually removed by janitor recovery.
- Deferred open-unlink orphan state is finalized after restart/runtime replay.
- Undo-log records are lifecycle-managed and post-cleaned to avoid long-term
  accumulation under repeated create-cancel / open-unlink cycles.

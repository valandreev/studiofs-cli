# Metadata Engine

`studiofs-meta` exposes synchronous metadata operations through `MetaEngine`.

## Engines

### `RedbMetaEngine`

Local embedded serving store.
- ACID local transactions
- foreground source of truth for the mounted client
- stores local overlay attrs/dentries/symlinks/xattrs, remote-visible base attrs/dentries/symlinks/xattrs, staged blocks, committed blocks, counters, local event log

### `PgMetaEngine`

Distributed wrapper around `RedbMetaEngine`.

Current behavior:
- foreground ops (`create`, `rename`, `unlink`, `truncate`, block staging) mutate local `redb`
- local mutations append to the local metadata event log
- background flush projects remote-visible deltas into PostgreSQL `event_log`
- background pull applies only newer foreign delta batches
- foreign namespace/attr/symlink/xattr apply updates the local `base_*` view
- local serving view is `overlay over base`
- `metadata_state` is used as a bootstrap/checkpoint store, not the primary runtime sync channel

## Sync Model

### Local vs Remote-Visible State

For regular files StudioFS keeps two views:
- local view:
  - `size`
  - staged `file_blocks`
- remote-visible view:
  - `committed_size`
  - `committed_file_blocks`

This is required so a client can finish local writes immediately while remote
clients only see blocks that have actually finished uploading.

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
- staged `SetFileBlock` / `SetFileBlocksBatch`
- pending deletion churn
- local regular-file `size` ahead of `committed_size`

Operational note:
- `sync_metadata()` is explicit "flush then pull": when local pending ops exist,
  it triggers a flush path before pulling newer remote batches.

### Pull from PostgreSQL

1. Load only rows with `batch_version > local_version`.
2. Decode batches in `(batch_version, client_id)` order.
3. Apply foreign batches in order.
4. Apply self batches only to remote-visible/base reconciliation (not as local overlay writes).
5. Apply foreign namespace/attr/symlink/xattr state into local `base_*`.
6. Skip auxiliary serving-view updates for events that conflict with local overlay state and continue applying later
   independent batches.

This avoids replaying a client's own changes and avoids clobbering local staged
writes with foreign state, while still advancing the remote-visible base view.

## PostgreSQL Tables

- `volumes`
- `event_log`
- `metadata_state`

`event_log` is the runtime delta stream. `metadata_state` stores:
- the monotonic `version`
- the latest remote-visible checkpoint snapshot

## Consistency

- local operations are synchronous and immediate for the current client
- cross-client visibility is eventual
- namespace visibility may arrive before all file blocks are committed
- remote-visible file size advances only with committed blocks
- foreign namespace can be present in local `base_*` before local overlay stops masking it
- stale async completions are rejected using `content_epoch` plus block identity checks

## MetadataConfig

Important fields:
- `backend = Postgres`
- `postgres.url`
- `postgres.volume_id`
- `cache_ttl_secs`
- `flush_interval_secs`
- `flush_ops_threshold`
- `flush_bytes_threshold`

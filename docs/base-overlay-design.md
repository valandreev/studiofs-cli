# Base + Overlay Metadata Design

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

Regular file block split already exists:

- `file_blocks`
  - local-serving block refs
- `committed_file_blocks`
  - remote-visible block refs

Namespace/attrs gain the same split:

- `base_inodes`
  - remote-visible attrs
- `overlay_inodes`
  - local replacements for attrs
- `base_dentries`
  - remote-visible namespace entries
- `overlay_dentries`
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

## Overlay Records

### Overlay inode

`overlay_inodes[ino]` means:

- this inode has a local attr replacement
- reads must return overlay attr instead of base attr

No separate inode tombstone table is required in the first slice.

Inode deletion is represented by namespace tombstones plus absence of a local
replacement attr. Base inode cleanup happens when overwrite/unlink convergence
removes the last visible reference.

### Overlay dentry

`overlay_dentries[(parent, name)]` stores one of:

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

1. If `overlay_inodes[ino]` exists, return it.
2. Else if `base_inodes[ino]` exists, return it.
3. Else `InodeNotFound`.

For regular files:

- overlay `size` may be ahead of base `committed_size`
- block reads still use current rule:
  - local block in `file_blocks` wins
  - otherwise fall back to `committed_file_blocks`

### `lookup(parent, name)`

1. Check `overlay_dentries[(parent, name)]`.
2. If it is `Tombstone`, return `EntryNotFound`.
3. If it is `Present(entry)`, return that entry.
4. Else fall back to `base_dentries[(parent, name)]`.

### `readdir(parent)`

Build the merged directory view as:

1. Start from all `base_dentries` for `parent`.
2. Apply every `overlay_dentries` entry for `parent`.
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

## Write Rules

### `create`

- Validate parent via merged `getattr`.
- Fail if merged `lookup(parent, name)` already exists.
- Write new attr to `overlay_inodes`.
- Write `Present(entry)` to `overlay_dentries`.

### `unlink`

- Resolve victim via merged `lookup`.
- Write `Tombstone` for `(parent, name)` into `overlay_dentries`.
- Update victim attr in `overlay_inodes` if `nlink` stays non-zero.
- If local view drops final link, remove local block state and local overlay attr.
- Remote-visible removal is published later through PG flush.

### `rename`

Source side:

- resolve through merged `lookup`
- set source name to `Tombstone`

Destination side:

- if destination exists in merged view, treat it as overwritten
- write `Present(src_entry)` at destination name
- update moved inode attr in `overlay_inodes` when parent changes
- update overwritten inode attr in `overlay_inodes` if `nlink` drops but inode
  survives locally

### `truncate` / `setattr(size)`

- update attr only in `overlay_inodes`
- modify local-serving `file_blocks`
- leave `base_inodes.committed_size` and committed blocks untouched until upload
  completion and PG flush publish remote-visible state

### `set_symlink_target`

- write only to local `symlinks`
- remove any local symlink tombstone for that inode

### `setxattr`

- validate flags against merged xattr visibility
- write only to local `xattrs`
- remove any local tombstone for that key

### `removexattr`

- validate existence against merged xattr visibility
- remove the local `xattrs` entry for that key
- if the key exists in `base_xattrs`, write a tombstone

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

- inode is present in local block overlay
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
4. Export committed blocks only

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

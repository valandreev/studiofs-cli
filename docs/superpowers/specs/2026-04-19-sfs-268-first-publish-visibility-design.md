# SFS-268 First-Publish Visibility Design

## Goal

Align namespace and publication behavior with the canonical 2026-04-19
local-first contract by ensuring a brand-new regular file stays invisible to
peer mounts until the first complete published version exists.

## Problem

The current production path still allows peers to observe a new regular file
before the file has any published version behind it.

Today this shows up as a remote-visible placeholder:

- PG event projection keeps `AddEntry` for a new regular file.
- projected `SetAttr` collapses the remote-visible size back to
  `committed_size`, which is still `0`.
- peer metadata/VFS therefore expose a zero-sized `Uploading` entry before the
  first payload-bearing publish finishes.

That behavior violates the canonical contract in
`docs/juicefs-local-first-rust-contract.md`:

- a new file must stay invisible to peers until its first complete published
  version exists;
- payload presence alone is not visibility;
- peers must switch at file-version granularity, not through placeholders.

## Constraints From SFS-263 Through SFS-270

- `SFS-264` and `SFS-265` already moved lock coordination and enforcement to
  PG. This work must not reopen lock semantics.
- `SFS-266` already converged the codebase on one Fast Local Authority barrier
  model. This work must not add callback-side durability branches.
- `SFS-267` already defined offline/reconnect behavior. This work must preserve
  replay/coalescing and not require synchronous PG round-trips in foreground IO.
- `SFS-269` and `SFS-270` will clean terminology and broaden validation after
  `SFS-268`. The implementation here should delete the placeholder semantics
  rather than codify them further.

## Chosen Approach

Move the visibility gate to the metadata serving layer for peer-applied state,
not to foreground VFS error handling.

### 1. Peer namespace hides unpublished new regular files

For merged namespace reads backed only by replicated shared state, a regular
file dentry is hidden when all of the following are true:

- the inode has no published base;
- the remote-visible attr still represents an unpublished first version
  (`upload_state=Uploading`, `size=0`, `committed_size=0`);
- the local metadata view does not carry a local overlay for that inode/path.

This preserves the writer's local view while preventing peers from seeing a
name that has no usable shared version behind it.

### 2. First publish reuses the existing dentry event stream

The design intentionally does not invent a new publish-path namespace event.
Instead:

- the original `AddEntry` / `RenameEntry` events remain in replicated state;
- peer namespace lookup/readdir suppress them until the first committed version
  exists;
- once `PublishFileManifest` plus `SetAttr(upload_state=Ready,
  committed_content_epoch=content_epoch)` arrive, the dentry becomes visible
  automatically because the visibility gate opens.

This keeps temp-file-plus-rename save patterns correct without introducing a
second source of truth for the file path.

### 3. Startup snapshots follow the same rule

`export_remote_snapshot()` must exclude unpublished new regular files that have
no published base. Otherwise a freshly bootstrapped peer can still see the same
placeholder that live PG replication will stop exposing.

Directories and non-regular metadata-only objects continue to replicate
normally. A zero-byte regular file may become visible only after it has a real
published state, not merely because a draft namespace entry exists locally.

### 4. Existing published files keep current semantics

This change is deliberately narrow:

- files that already have a published base still advance through successive
  complete published versions;
- peers continue to read the previously committed version while a new version is
  pending;
- metadata-only updates on already published objects still publish without
  waiting on S3;
- local overlays continue to mask foreign remote updates until the local overlay
  is flushed or published.

## Affected Code Boundaries

- `crates/studiofs-meta/src/redb_engine/namespace.rs`
  Local merged lookup/readdir logic. This is the most natural place to hide a
  remote-only unpublished file while leaving the writer-visible overlay intact.
- `crates/studiofs-meta/src/redb_engine/txn.rs`
  Helper functions used by merged namespace reads will likely need an explicit
  visibility predicate for regular-file dentries.
- `crates/studiofs-meta/src/redb_engine/replication.rs`
  Snapshot export needs to stop leaking unpublished new regular files to a peer
  bootstrap/import path.
- `crates/studiofs-vfs/src/vfs.rs`
  `remote_upload_placeholder_visible()` and `RemoteContentPending` gating become
  stale once peers no longer observe the placeholder entry in the first place.
- `crates/studiofs-meta/src/pg_engine.rs`
  Tests describing the old placeholder behavior must flip to the new
  first-publish invisibility contract.
- `crates/studiofs-vfs/tests/vfs_test.rs`
  Integration coverage must assert “not visible before first publish” instead
  of “visible as uploading placeholder”.

## Testing Strategy

### Fast unit/meta tests

- A projected remote batch for a new regular file with only unpublished local
  content must not make the dentry visible on the peer.
- A temp-file-plus-rename flow must keep the final path hidden on the peer
  until the first publish lands.
- Once the first publish lands, the file must appear atomically with the final
  committed attr/block state.
- Snapshot export/import must not surface an unpublished new regular file on a
  fresh peer.

### Existing regression coverage to keep green

- already published files remain readable at the previous committed version
  during an update;
- delete-before-publish lifecycle and orphaned payload retention still work;
- local overlay masking of foreign attr/xattr updates still works.

## Risks And Guards

- If the hide/show rule is implemented too broadly, the writer could lose local
  visibility. Guard by keying the filter off “no local overlay”.
- If snapshot export is not updated together with live projection, bootstrap and
  steady-state peers will diverge. Guard with a dedicated snapshot regression
  test.
- If VFS placeholder logic is removed before namespace gating is in place,
  peers may start returning plain `EntryNotFound` inconsistently. Guard by
  landing the meta-layer tests first, then updating VFS integration tests.

## Out Of Scope

- changing PG lock behavior from `SFS-264` / `SFS-265`;
- barrier semantics from `SFS-266`;
- offline headroom or reconnect policy from `SFS-267`;
- vocabulary cleanup and validation-matrix expansion owned by `SFS-269` and
  `SFS-270`.

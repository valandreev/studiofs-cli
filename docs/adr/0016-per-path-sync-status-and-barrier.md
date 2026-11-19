# Per-Path Sync Status Comes From The Mounted Process And Backs A Snapshot Barrier

Status: proposed (SFS-1069). Implementation is tracked in the Linear project
"[0.3.0] Per-path sync & cache status". The state vocabulary is aligned with
the SFS-986 sync and health model before this decision is accepted (SFS-1096).

## Context

Artists and pipeline scripts ask questions about one path before they close a
workstation or start a downstream job: is this shot folder uploaded and
published, which files are still local, and how much of a large sequence is in
the local cache. `studiofs status` and `.studiofs-runtime-stats` answer only
for the whole volume, and there is no barrier that waits for one path.

The mounted process already tracks most of what a per-path answer needs:

- `InodeLifecycleState` (`LocalOnly`, `Uploading`, `PublishReady`,
  `Published`) and the local and published content epochs of each regular
  file, in redb.
- Local dirty payloads, writer dirty bytes, dirty attributes, the per-inode
  upload queue index, and sticky upload errors, in memory.
- Overlay rows (`inodes`, `dentries`, `symlinks`, `xattrs`) for local metadata
  that is not yet remote-visible, over the `base_*` tables.
- A volume-level metadata publication cursor (`local_event_end`,
  `durable_cursor`). There is no per-inode metadata publication marker.
- A diagnostics channel between the CLI and the mounted process (Unix socket or
  Windows named pipe, owner-only discovery, auth token, JSON frames, protocol
  v5).

The local payload cache is indexed by object id, not by inode. WinFSP has no
extended attribute support. The only reserved xattr today is
`system.studiofs.retained.v1`; ADR 0014 plans a read-only
`user.studiofs.scratch`.

## Decision

### Per-path state

Every path has one of four states:

- `local`: the path has changes that are not yet uploaded and published. This
  covers `LocalOnly`, `Uploading`, and `PublishReady` content, local dirty
  payloads, unflushed writer data, dirty attributes, and overlay metadata rows.
- `synced`: content is `Published` with matching epochs, and the inode has no
  overlay metadata rows, dirty attributes, or local dirty payloads.
- `error`: the inode has a sticky upload error.
- `unknown`: the state cannot be determined, for example when an input cannot
  be read or PostgreSQL sync is degraded. `unknown` is never shown as `synced`
  or as zero pending.

JSON output adds an optional `detail` with the phase, such as `uploading`,
`publishing`, `metadata-pending`, `unflushed`, or `local-only`. Detail names
follow the SFS-986 vocabulary and add no synonyms.

`synced` means uploaded to S3 and published to PostgreSQL by this client. It
does not mean that peers already see the data, and it is not a replacement for
an orderly unmount.

Scratch, ignored, and trash entries never synchronize. They report `local` with
`detail: local-only` and are excluded from barriers.

### Mounted process only

Per-path status and the barrier are answered only by the mounted process over
the diagnostics channel. They need live in-memory writer and upload state. There
is no offline redb inspection; for an unmounted volume the commands fail with a
distinct exit code instead of reporting a state.

### Byte counters

- `logical_bytes`: the local visible size.
- `pending_bytes`: local dirty payloads plus queued upload objects of the inode.
- `cached_bytes`: the sum of `payload_len` of the inode's objects that are
  present in the local payload index. It is object-granular: a packed object
  shared with other files counts in full, and JSON output says so with
  `cached_bytes_granularity: "object"`. Computing it costs one per-inode graph
  scan plus one index lookup per object. There is no inode-to-object cache
  index.

Non-regular inodes report zero for all three.

### Directory aggregation

A directory's own status covers only its own metadata. Recursive aggregation is
done on demand by the mounted process for CLI and IPC requests, never inside a
FUSE callback and never cached:

- the walk uses the local serving view (overlay over base), never PostgreSQL;
- it does not follow symlinks and counts each inode once;
- it skips scratch, ignored, and trash subtrees and reports them as `skipped`;
- it stops at a node budget (default 1,000,000). A truncated walk reports
  `unknown`, never a partial `synced`.

### Barrier snapshot

`studiofs wait-synced` waits for one target, `synced`, for a snapshot taken
when it starts:

- For each regular file in scope, the snapshot records `local_content_epoch`.
  It also records the volume metadata watermark `local_event_end`.
- The snapshot is complete when every recorded inode has
  `published_content_epoch` at or past the recorded epoch (or was deleted
  before publish with no remaining obligation), and `durable_cursor` has
  reached the watermark. The metadata watermark is volume-wide, so a barrier
  can also wait for unrelated earlier metadata changes. That is conservative
  and bounded by the snapshot.
- Writes after the snapshot are not included. A file that is written
  continuously does not keep the barrier open.
- Data that exists only in open handles' write buffers is not included and is
  not flushed. The barrier reports the number of such handles as a warning.
  The `write`, `flush`, `fsync`, and release durability contracts are
  unchanged.
- Inodes renamed or unlinked after the snapshot still count until their
  recorded epoch is published. New files are not added.
- A sticky upload error on a recorded inode ends the wait with an error.
- PostgreSQL degradation or S3 unavailability does not end the wait. It is
  reported as a reason until the caller's timeout.
- The wait is driven by notifications on epoch finalization and on
  `durable_cursor` advance, with a deadline. It uses no fixed sleeps.

### IPC

The diagnostics protocol moves to v6 and adds two requests. Existing requests
keep working for v5 clients.

- `PathStatus { path, recursive, node_budget }` returns per-entry results and an
  aggregate. Paths are relative to the mount root, cannot escape it, and a
  final symlink is not followed.
- `WaitSynced { path | token, node_budget, step_timeout }` takes the snapshot on
  the first call and returns an opaque token. Each call waits at most a short,
  server-capped step (default 5 s), then returns progress or a final result.
  Snapshots live on the server with a TTL and a per-mount limit and hold no
  locks.

Requests run on IPC workers, never on platform callback threads, and use the
existing authentication.

### CLI

- `studiofs status <volume> --path <relpath>` or
  `studiofs status --path <path inside a mount>`, with `--recursive` and
  `--json`. JSON output has a `schema_version`. Output without `--path` is
  unchanged.
- `studiofs wait-synced <path> [--timeout <duration>] [--json]
  [--node-budget <n>]`. Progress goes to stderr. Exit codes:

  | Code | Meaning |
  |---|---|
  | 0 | synced |
  | 1 | other error |
  | 2 | timeout |
  | 3 | not mounted, or the mount went away during the wait |
  | 4 | sticky upload error in the snapshot |
  | 5 | node budget exceeded |

### Xattr contract (stage 2)

- Read-only virtual attributes `user.studiofs.sync_state`,
  `user.studiofs.logical_bytes`, `user.studiofs.cached_bytes`, and
  `user.studiofs.pending_bytes`, with the same literal names on Linux and
  macOS. Values are ASCII without a trailing newline; numbers are decimal.
- Directories report only their own state. There is no recursive xattr.
- The whole `user.studiofs.` prefix is reserved, including
  `user.studiofs.scratch` from ADR 0014. Reserved names are hidden from
  `listxattr`, so `cp -a`, `rsync -X`, backups, and xfstests such as
  generic/020 do not see them. `setxattr` and `removexattr` fail with `EPERM`.
  Stored xattrs that already use the prefix are shadowed. Other `user.*`
  names are unaffected.
- The intercept lives in the shared VFS xattr path, so Linux FUSE, macFUSE,
  and FSKit behave the same. Reads never take the xattr mutation path or a
  file-session permit, and take locks one at a time in `lock_order.rs` order.
- Byte values are memoized per inode in a bounded LRU for about one second and
  cleared on inode change, so a size probe and the following read return the
  same value. `sync_state` is always computed fresh.
- WinFSP has no xattrs. Windows uses the CLI.

### Scope and non-goals

- Stage 1 is `status --path` and `wait-synced`. Stage 2 is the xattr surface.
- Finder Sync and Explorer badge overlays are out of scope. The IPC requests
  are versioned so a later integration can use them.
- Offline inspection, peer visibility, and an `--until` choice of earlier
  stages are out of scope.

## Consequences

Scripts get a deterministic barrier with stable exit codes that does not hang
on files that keep changing. Status reads cost a few lock acquisitions and redb
reads per inode, and recursive requests are bounded and run off callback
threads.

The volume-wide metadata watermark can make a barrier wait for unrelated
metadata changes made before it started. A per-inode event index would remove
that, at the cost of a schema change and a write on every event; it is left for
a later decision.

`cached_bytes` can overstate per-file residency for packed objects.

Hiding reserved names from `listxattr` means users discover them from
documentation, not from listing. Any stored `user.studiofs.*` xattr becomes
inaccessible.

Acceptance:

- Stage 1: a mounted `studiofs-e2e` scenario on Linux FUSE and WinFSP, plus a
  macOS smoke run.
- Stage 2: a mounted xattr scenario on Linux FUSE and macFUSE, with no
  regressions in the xfstests xattr subset.
- FSKit counts only once FSKit support ships.

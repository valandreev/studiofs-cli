# SFS-264 Design: PostgreSQL Lock Leases As The Hard-Lock Authority

## Status

Approved scope for implementation.

## Goal

Replace the legacy object-store `.lock` record coordination in
`crates/studiofs-vfs/src/vfs/locking.rs` with a PostgreSQL-backed hard-lock
authority that uses leases, renewal, and pub/sub notifications.

This issue is intentionally scoped as the lock-authority foundation only. It
must not absorb the broader enforcement work described by `SFS-265`.

## Why This Slice Exists

The canonical contract dated 2026-04-19 says:

- PostgreSQL is the global coordinator for hard-lock state.
- Hard-lock state uses TTL/lease semantics.
- Healthy holders renew leases.
- Abandoned leases expire automatically.
- Clients learn about lock changes through PG-backed coordination and pub/sub.
- Clients must preserve already known lock state across PostgreSQL disconnects.

The current VFS implementation still contains object-store-backed remote lock
records in `locking.rs`. That model conflicts with the canonical contract and
would be the wrong base for follow-up work in `SFS-265`, `SFS-267`, and
`SFS-270`.

## Scope

Included in `SFS-264`:

- add PostgreSQL-backed file-lock lease storage and APIs in `studiofs-meta`
- support acquire, release, renew, and read of hard-lock lease state
- publish lock-change notifications through PostgreSQL `NOTIFY`
- maintain per-client known lock state suitable for reconnect/offline follow-up
- rewire VFS `flock` remote coordination to the new metadata-backed authority
- delete object-store lock record encode/decode and `Operator`-based remote lock
  coordination from `locking.rs`
- add focused tests for lease grant, conflict, release, expiry, and peer
  notification visibility

Explicitly excluded from `SFS-264`:

- applying hard-lock rules to ordinary write-open, truncate, rename, move, or
  delete paths
- FUSE callback sequencing changes beyond `flock`
- WinFSP callback sequencing changes beyond `flock`
- broader offline-mode behavior outside preserving known lock state
- final validation matrix coverage for the whole contract

Those belong to `SFS-265`, `SFS-267`, and `SFS-270`.

## Recommended Architecture

### 1. Move Global Lock Authority Into `studiofs-meta`

The new hard-lock authority should live beside the existing PostgreSQL
replication logic in `crates/studiofs-meta/src/pg_engine.rs`.

Reasoning:

- `PgMetaEngine` already owns the PostgreSQL connection model and runtime
  helpers.
- it already emits `pg_notify` for metadata invalidation
- it already owns a stable per-client identity via `client-slot:{slot}`
- follow-up issues need lock state to be part of the canonical PG-backed mount
  contract, not a parallel VFS-side storage system

This design keeps VFS as the consumer of lock authority, not the owner of a
second distributed-system implementation.

### 2. Introduce A Metadata-Level Hard-Lock API

Extend the metadata contract with a focused file-lock lease surface, exposed by
`PgMetaEngine` and implemented as a local no-op/unsupported path for pure redb
where appropriate.

The API should cover:

- acquire shared or exclusive lease for an inode and client id
- release lease held by a client
- renew lease when the caller is still the current holder
- read current lease snapshot for one inode
- query locally known lease snapshot for one inode without forcing a PG round
  trip

The return values should distinguish:

- acquired
- blocked by conflicting holder
- stale ownership or epoch mismatch during renew/release
- storage/connectivity failures

This keeps conflict semantics explicit and avoids encoding distributed lock
decisions into generic storage errors.

### 3. Represent Hard Locks As Lease Rows

Persist hard-lock authority in PostgreSQL rows keyed by `(vol, ino)`.

The row should capture:

- volume id
- inode number
- lease mode: shared or exclusive
- holder set for shared mode, or single holder for exclusive mode
- lease expiry timestamp
- monotonic generation/version for compare-and-swap style updates
- updated timestamp

The exact schema may vary, but the important contract points are:

- lease ownership is resolved in PostgreSQL, not object storage
- expiry is data, not an in-memory-only timeout
- updates can reject stale renew/release attempts

### 4. Use `NOTIFY` For Wakeups, Not As Durable Truth

`NOTIFY` should be the wakeup path for peers, not the source of truth.

On acquire/release/renew/expiry-visible transition:

- update the lease row transactionally
- emit `NOTIFY studiofs_lock_vol_<volume_id>` with a payload that identifies the
  inode and a new generation/version

Peers should treat the notification as:

- a prompt to refresh known state for the inode
- not a complete replacement for reading authoritative lease state

This matches the current metadata invalidation pattern and keeps correctness in
PG row state instead of relying on at-most-once pub/sub delivery.

### 5. Keep A Known-Lock-State Cache In `PgMetaEngine`

`SFS-267` needs clients to preserve already known lock state across PostgreSQL
disconnects. `SFS-264` should therefore build the cache now rather than bolting
it on later.

The cache should:

- be keyed by inode
- store the latest known lease snapshot and generation/version
- update after successful local acquire/release/renew
- update after peer notification refresh
- remain readable even when PostgreSQL is temporarily unavailable

This issue does not need to define all offline policy. It only needs to provide
the stable notion of "known lock state" that later work can enforce.

### 6. Keep Local Per-Mount Owner Tracking In VFS

`crates/studiofs-vfs/src/vfs/locking.rs` should still own:

- per-mount owner reference counts
- upgrade/downgrade bookkeeping within one mount
- local wait/notify for blocking `flock`

But it should stop owning:

- remote lease serialization
- object-store compare-and-swap writes
- remote lock expiry logic

The VFS flow becomes:

1. check whether local owner/refcount rules allow attempting the requested lock
2. ask metadata lock authority to acquire or release the PG lease
3. on success, commit local owner/refcount state
4. on local commit failure after remote acquire, release the remote lease best
   effort

That keeps the clean split between local process state and distributed authority.

## Data Flow

### Acquire

1. Caller invokes `vfs.flock(ino, op, owner, blocking)`.
2. VFS checks local owner/refcount compatibility.
3. VFS calls metadata lock-authority acquire.
4. `PgMetaEngine` updates the lease row transactionally if no conflicting live
   lease exists.
5. `PgMetaEngine` commits and emits `NOTIFY`.
6. VFS records the local owner/refcount state.

### Release

1. VFS drops local owner/refcount state for the releasing owner.
2. If this was the final local holder, VFS calls metadata release.
3. `PgMetaEngine` clears or rewrites the lease row and emits `NOTIFY`.

### Renew

1. A background renewal loop refreshes leases held by the current client before
   TTL expiry.
2. Renew succeeds only if the caller still owns the current generation/version.
3. Failed renew marks the local cache stale and wakes blocked waiters.

### Peer Update

1. A peer receives `NOTIFY`.
2. It refreshes authoritative lease state for the inode from PostgreSQL.
3. It updates the known-lock-state cache.
4. It wakes local waiters blocked on `flock`.

## Error Handling

- conflicting live lock request returns a typed "blocked" result that VFS maps
  to `VfsError::WouldBlock`
- PostgreSQL connectivity or timeout failures remain real errors, not conflict
- release of an already expired or superseded lease is best-effort and should
  not corrupt local refcount cleanup
- renew failure because ownership changed must be represented distinctly from
  transport failure

## Testing Strategy

Add small focused tests, not full-suite coverage.

Required coverage for this issue:

- exclusive lease blocks peer exclusive acquire
- shared leases coexist when compatible
- exclusive acquire succeeds after final shared release
- renew extends a live lease for the same holder
- expired lease becomes acquirable by a peer
- peer known-state cache refreshes after PG notification
- VFS `flock` uses PG-backed authority instead of object-store records

Test placement:

- PostgreSQL lease unit/integration tests in `crates/studiofs-meta/src/pg_engine.rs`
- VFS-level focused tests in `crates/studiofs-vfs/src/vfs/locking.rs` or nearby
  test modules

## Implementation Notes

- Prefer reusing existing `tokio-postgres` runtime helpers already present in
  `PgMetaEngine`.
- A dedicated long-lived notification connection is acceptable here because the
  engine already owns background poll lifecycle.
- `NOTIFY` should wake refresh quickly, but periodic fallback polling may remain
  as a safety net if that keeps the initial slice smaller and safer.
- Do not introduce object-store lock compatibility shims. The canonical
  contract is the migration target.

## Acceptance Boundary

`SFS-264` is complete when:

- object-store `.lock` coordination is removed from VFS
- PostgreSQL is the only distributed hard-lock authority
- lock leases have TTL plus renewal semantics
- peers can learn about changes through PG notification plus authoritative
  refresh
- VFS `flock` is wired to the new authority
- the design still leaves broader mutation enforcement for `SFS-265`

## Out Of Scope Follow-Up

- `SFS-265`: enforce hard-lock state across write-open and mutation paths
- `SFS-267`: preserve and act on known lock state during PG disconnect/offline
- `SFS-270`: end-to-end regression matrix for grant, renewal, expiry, and peer
  visibility

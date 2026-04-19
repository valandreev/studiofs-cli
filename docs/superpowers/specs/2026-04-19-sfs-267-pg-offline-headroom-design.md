# SFS-267 Design: Canonical PG-Offline Mode With Finite Headroom

## Status

Approved scope for implementation.

## Goal

Implement the canonical PostgreSQL-offline production behavior for StudioFS:

- local reads and writes continue while PostgreSQL is unavailable and the mount
  still has preallocated shared-ID headroom
- already known hard-lock state remains enforceable during the disconnect
- reads of already known published versions continue from local cache and S3
  when S3 is still reachable
- the mount degrades to read-only when shared-ID headroom is exhausted
- reconnect publishes the net local changes in local causal order without
  replaying meaningless internal microstates

This issue is intentionally scoped to offline behavior around PostgreSQL
authority loss. It must not absorb the broader namespace/publication semantics
reserved for `SFS-268`, the doc/CLI cleanup in `SFS-269`, or the final
validation-matrix work in `SFS-270`.

## Why This Slice Exists

The canonical contract dated 2026-04-19 says:

- PostgreSQL remains the source of truth for ordered shared metadata.
- Clients may continue local work during PostgreSQL unavailability while they
  still have enough preallocated local ID headroom.
- Once that headroom is exhausted, the mount degrades to read-only over data it
  can already resolve locally or from S3
- already known hard-lock state must remain enforceable across the disconnect
- reconnect publishes accumulated work in local causal order, but may coalesce
  internal intermediate states into the externally equivalent result
- shared conflicts still resolve by the existing LWW-by-arrival-to-PG rule

The current codebase already has useful foundations:

- `PgMetaEngine` persists local mutations and rebuilds pending replication
  backlog after crash/reopen
- the PG-backed lock work from `SFS-264` and `SFS-265` already provides a
  locally readable known-lock cache
- VFS mutation entrypoints already funnel through a small number of write-open,
  namespace, and write/truncate paths

What is still missing is the contract-grade notion of finite shared-ID
headroom. Today the client has a disjoint slot-derived ID space, but not an
explicit active-range/next-range model with a real exhaustion boundary that can
drive deterministic read-only degradation.

## Scope

Included in `SFS-267`:

- replace the implicit “large disjoint local ID space” assumption with explicit
  persisted preallocated inode/block ranges in `PgMetaEngine`
- reserve and persist the next disjoint range while PostgreSQL is still
  reachable, before the active range is exhausted
- expose a metadata-authority/offline-headroom status suitable for VFS write
  gating
- keep allowing local metadata and payload mutations while PostgreSQL is down
  and local headroom remains
- degrade new mutations to read-only denial when local headroom is exhausted
- keep enforcing already known foreign hard locks during the disconnect
- keep already known published reads working from local cache and S3 while
  PostgreSQL is down
- reconnect by publishing locally accumulated work through the existing ordered
  metadata pipeline while coalescing obsolete internal microstates
- add focused tests for offline-write continuation, exhaustion-driven
  read-only degradation, known-lock enforcement, and reconnect replay

Explicitly excluded from `SFS-267`:

- changing the path-visibility and publication contract for new files and
  replacement versions beyond what reconnect/offline behavior already requires
- changing the shared conflict rule away from LWW-by-arrival-to-PG
- introducing a new merge algorithm distinct from the existing PG event-log
  ordering model
- broad doc, diagnostics, and product-surface cleanup
- the final end-to-end validation matrix

Those belong to `SFS-268`, `SFS-269`, and `SFS-270`.

## Recommended Architecture

### 1. Make Shared-ID Headroom Explicit In `PgMetaEngine`

`PgMetaEngine` should own explicit persisted preallocated ranges for both:

- inode allocation
- block/object allocation

The model should be:

- current active range
- next reserved range
- persisted current cursor within the active range

The important behavior is:

- foreground allocation never needs a PG round trip while the active range
  still has headroom
- when the active range reaches a low-water threshold and PostgreSQL is
  reachable, the engine reserves and persists the next disjoint range
- if PostgreSQL is unavailable and there is no remaining headroom in the active
  range and no already reserved next range to switch into, allocation is truly
  exhausted

This is the real contract boundary needed by `SFS-267`. A backlog heuristic or
generic “PG errors happened recently” signal is not enough.

### 2. Keep Local Mutation Acceptance In The Metadata Layer

The current `PgMetaEngine` behavior of mutating local Redb state first and
tracking unpublished replication backlog is the right base for offline mode.

While PostgreSQL is unavailable:

- local metadata mutations continue against the local engine
- local event-log growth continues
- pending replication backlog continues to accumulate
- local payload persistence and local durability barriers continue to work

The engine should not stop accepting local mutations simply because the remote
authority is temporarily unreachable. It should stop only when it can no longer
allocate fresh shared identifiers safely.

### 3. Add A Small Metadata-Authority Status Surface

Extend the metadata replication contract with a focused status surface that VFS
can consult without inferring policy from raw storage errors.

The status should answer:

- is PostgreSQL currently considered reachable for new range reservation /
  authoritative replication work
- does the client still have writable local ID headroom
- has the mount degraded to read-only because headroom is exhausted

This should stay intentionally narrow. VFS needs one stable policy input, not
an entire failure-analysis subsystem.

### 4. Gate New Mutations In VFS, Not Reads

The read-only degradation should be enforced in VFS mutation entrypoints, not by
trying to make every metadata primitive self-policing independently.

The gate should apply to:

- write-open paths in `open_file(...)`
- `write(...)`
- create/mkdir/symlink/link
- rename/move/delete paths
- truncate and size-changing `setattr`

The gate should not block:

- plain reads
- lookup/getattr/readlink/listing
- local durability barriers for already accepted work such as `flush`, `fsync`,
  and last-handle release

That preserves the contract distinction between “do not accept new mutations”
and “do not strand data that was already accepted locally”.

### 5. Reuse The Known-Lock Cache From `SFS-264`/`SFS-265`

Known foreign hard-lock state should continue to be enforced during the
disconnect through the existing last-known lease cache.

The offline rule is:

- if a foreign hard lock was already known before PostgreSQL disconnect, new
  conflicting mutations must still be denied while disconnected
- if the client misses newer foreign lock changes during the outage, that is
  acceptable under the canonical contract

This means `SFS-267` should reuse the existing `known_file_lock_lease()` path
instead of inventing a parallel offline lock model.

### 6. Reconnect Through The Existing Ordered PG Event Pipeline

Reconnect should continue to use the current PG event-log pipeline:

- local event ranges remain the source of unpublished local causal history
- publish still flows through `flush_pending_claimed_batch*`,
  `push_event_log_batch_locked`, and the existing foreign-batch apply path
- shared conflict resolution remains LWW by arrival to PostgreSQL

The reconnect enhancement here is not a new merge algorithm. It is better
projection/coalescing so peers observe the correct external transitions without
replaying every internal local microstep.

### 7. Coalesce Internal Noise, Preserve External Semantics

Reconnect publication may drop internal microstates only when doing so preserves
the externally observable result and local causal order.

Examples of coalescible noise:

- repeated local `SetAttr` churn where only the final externally visible value
  matters
- block-level or overlay-internal intermediate states already collapsed by the
  final published file version
- create-temp / overwrite / rename sequences whose externally visible result is
  the final namespace/content state

Examples that must still preserve causal ordering:

- namespace changes whose ordering is externally observable
- deletion followed by recreation
- rename/move transitions that change the externally visible path winner
- lock-sensitive mutation ordering that would change conflict outcomes

The implementation should prefer reusing the existing remote projection logic in
`PgMetaEngine` rather than layering a second replay engine into VFS.

## Data Flow

### Online Steady State

1. Mount starts and loads/persists the current preallocated inode and block
   ranges.
2. Foreground mutations allocate IDs only from persisted local ranges.
3. Background metadata replication continues flushing event batches to PG.
4. When the current range approaches exhaustion, the engine reserves and stores
   the next range while PG is healthy.

### PostgreSQL Disconnect With Remaining Headroom

1. PG operations such as flush/sync/range reservation begin failing or timing
   out.
2. Metadata status marks PostgreSQL authority as unavailable.
3. Reads continue from local state and already known published data.
4. Known foreign hard locks continue to be enforced from the local known-lock
   cache.
5. New local mutations continue as long as allocation stays within already
   reserved persisted ranges.
6. Pending metadata backlog grows locally until reconnect.

### Headroom Exhaustion

1. The active range reaches its real boundary and no reserved next range is
   available to switch into.
2. Metadata status marks local mutation headroom exhausted.
3. VFS mutation entrypoints begin rejecting new mutations and writable opens.
4. Reads and local durability barriers for already accepted work still run.

### Reconnect

1. PG becomes reachable again.
2. The engine resumes metadata flush/sync and reserves fresh future ranges.
3. Pending local event ranges are published through the existing ordered PG
   batch path.
4. Remote-visible projection/coalescing collapses obsolete internal microstates
   while preserving external causal semantics.
5. Peers observe the final externally valid sequence under the existing
   LWW-by-arrival rule.

## Error Handling

- PostgreSQL transport failure is not itself a mutation conflict; it changes the
  metadata-authority status and may eventually drive read-only degradation
- exhaustion-driven read-only should surface as an access-denial style mutation
  failure, not as a fake lock conflict
- known foreign hard-lock denial remains `WouldBlock`
- reconnect publication should restore pending batches on flush failure exactly
  as the current PG path already does
- local durability barriers must not report success before local crash-safe
  guarantees are met, even while PG is down

## Testing Strategy

Add focused tests, not full-suite coverage.

Required coverage for this issue:

- PostgreSQL-backed engine continues accepting local mutations while PG is down
  and preallocated headroom remains
- inode headroom exhaustion degrades create/mkdir/symlink/link paths to
  read-only denial
- block headroom exhaustion degrades write/truncate/size-growth paths to
  read-only denial
- writable open is denied after exhaustion, while read-only open still works
- known pre-disconnect hard-lock state still blocks conflicting new mutations
  while disconnected
- already known published reads continue while PG is down and S3 is reachable
- reconnect publishes pending local work and peers observe the correct net
  external result
- reconnect coalescing drops obsolete internal states without breaking external
  ordering in a temp-write-rename or similar realistic sequence

Test placement:

- range/allocation/offline status tests in `crates/studiofs-meta/src/pg_engine.rs`
- VFS gating and offline-read/write behavior in `crates/studiofs-vfs/tests/vfs_test.rs`

Keep the tests small and deterministic. Do not run the full workspace test
suite as part of this issue.

## Implementation Notes

- Prefer extending the existing PG metadata engine and replication status rather
  than introducing a second offline coordinator in VFS.
- Persist range state in the local metadata store so restart while still
  disconnected preserves the same authority boundary.
- The low-water trigger for reserving the next range should be deterministic and
  testable.
- The reconnect path should reuse the current event-log batching and projection
  code where possible. Strengthen that projection before introducing any new
  replay machinery.
- Keep `flush`, `fsync`, and last-handle release aligned with the single
  Fast Local Authority barrier model from `SFS-266`.

## Acceptance Boundary

`SFS-267` is complete when:

- PostgreSQL disconnect no longer implies immediate write failure
- local mutation continuation is bounded by a real preallocated headroom model
- the mount degrades to read-only when that headroom is actually exhausted
- already known foreign hard locks still block conflicting new mutations during
  disconnect
- reads of already known published versions still work without PG
- reconnect publishes the correct net local result in causal order without
  replaying meaningless internal microstates

## Out Of Scope Follow-Up

- `SFS-268`: final shared-view/private-overlay and first-visibility semantics
- `SFS-269`: doc/CLI/diagnostics cleanup for legacy vocabulary
- `SFS-270`: final end-to-end validation matrix for offline, locks, visibility,
  and snapshot semantics

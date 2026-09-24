# PostgreSQL Sync Contract

## Status

This document is the canonical PostgreSQL sync contract for SFS-695 and the
follow-up implementation issues SFS-696, SFS-697, SFS-698, and SFS-699. The local-only
destroy boundary from SFS-721 is already implemented on `main`; this document
retains it as a shipped invariant that the cleanup implementation must
preserve.

This is a breaking on-disk/protocol change. Migration compatibility is not
required for existing test disks. A client or test fixture must use the new
format/protocol boundary rather than relying on an old PostgreSQL startup
layout.

SFS-696 establishes the new boundary at metadata format version `5`. Local
`redb` metadata and the PostgreSQL `metadata_state` row must carry that same
version. The PostgreSQL row also carries the live bootstrap checkpoint's
SHA-256 digest, publication state, and monotonically increasing publication
generation. Schema setup may add these columns to an existing database with
legacy sentinel values, but it never upgrades old rows. A reachable legacy
version is rejected with an actionable recreate/`studiofs discover` error;
there is no migration, dual-read, or dual-write path.

The mounted data-plane contract remains in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).
This document owns the PostgreSQL lifecycle and synchronization rules that the
data-plane contract intentionally does not define.

## Purpose

PostgreSQL is a bootstrap and convergence service, not the foreground serving
database. A mounted client must be able to serve its valid local `redb` view
while remote catch-up is delayed, and a fresh client must not acquire write
authority before its initial remote state is complete enough to protect shared
metadata.

The contract therefore separates three independently observable concerns:

1. local serving readiness;
2. remote catch-up progress;
3. read/write authority.

No caller may infer one of these states from another.

## Core Vocabulary

| Term | Meaning |
|---|---|
| `local serving view` | The local `redb` view used by foreground mounted reads and metadata operations. It resolves local overlay state over the last remote-visible base state. |
| `serving checkpoint` | A serialized snapshot of remote-visible metadata: namespace, attributes, regular-file base graph, symlinks, xattrs, and ACL rules. |
| `checkpoint_version` | The remote event-log version represented by a serving checkpoint. |
| `event_log_prune_floor` | The highest complete event-log batch version durably removed by background retention maintenance. Replay cursors below this floor require checkpoint rebootstrap handling. |
| `remote head` | The monotonic `metadata_state.version` observed at one point in time. It can advance after the observation. |
| `replay cursor` | The scalar `batch_version` of the last fully applied remote batch. The cursor advances only after the corresponding complete page has been applied locally; `seq_id` orders rows within a batch but is not a separate persisted cursor component. |
| `startup target` | One remote version captured for a client startup or catch-up operation. It is frozen for that operation and is not re-read as a moving completion target. |
| `local-ready` | Local `redb` is open and its valid local serving view can answer foreground reads and metadata requests. |
| `catching-up` | A background or explicit sync operation is applying remote checkpoint/delta state toward a frozen startup target. |
| `target-reached` | The local replay cursor has reached the startup target for the current operation. This does not by itself grant write authority. |
| `read-only authority` | The authority state that permits known safe local reads while rejecting new shared mutations until the write-authority gate succeeds. |
| `write authority` | The independent capability to accept new shared mutations. It includes the required local allocator state and any PostgreSQL-backed authority registration. |
| `degraded read-only` | A health/authority combination used when a usable local serving view is operating with a remote catch-up or authority failure. It does not describe the normal healthy fresh-client phase that is intentionally read-only while catching up. |
| `checkpoint lag` | The difference between the observed remote head and the checkpoint version used for bootstrap. |

These dimensions remain separate even when an implementation exposes them in a
single diagnostic record:

- convergence is `catching-up` or `target-reached`;
- authority is `read-only` or `read/write`;
- health is `healthy` or `degraded(reason)`.

A normal fresh bootstrap is therefore `local-ready + catching-up + healthy +
read-only`; withholding write authority during that bounded phase is not by
itself a degraded condition.

The runtime exposes this contract through `MetaReplication::pg_sync_status()`.
Its `PgSyncLifecycle` reports the local serving/synchronization phase, while
the embedded `MetadataAuthorityStatus` reports paired inode/object allocator
headroom. Only `Live` grants lifecycle mutation permission, and the final
mutation decision requires both lifecycle permission and both allocator
headroom flags. `Fatal` means the local serving view is unsafe; foreground VFS
access is rejected in that state. `discover` persists a local
`pgFreshClientPending` marker while
the fresh-client startup gate is unresolved; a successful startup catch-up
clears that marker. The marker is local state and is preserved by serving
checkpoint import, so a failed fresh bootstrap remains recoverable and
read-only on a later mount. If that mount remains active while PostgreSQL is
unavailable, a later successful background sync may complete the pending
bootstrap once the local replay cursor reaches the observed remote head. The
runtime reserves a missing `pgClientSlotPlusOne` before clearing the marker;
if that reservation is unavailable, the client remains `degraded` and
read-only. Once the slot and replay target are both complete, the runtime
advances through `catching-up` and `target-reached`, clears the marker, and
reevaluates allocator authority; it remains `allocator-pending` when the
paired allocator gate is still unavailable.

## Serving-First Mount Lifecycle

### Existing local client

For a client with a valid local metadata database:

1. Open and validate local `redb` state.
2. Enter `local-ready` as soon as the local serving view is safe to use.
3. Start remote identity checks, checkpoint refresh, and event catch-up as
   separate operations with their own bounded outcomes.
4. Keep foreground metadata reads on local `redb`; they must not wait for
   PostgreSQL replay.
5. Keep local writes governed by the separate write-authority state. A remote
   sync outage must not be confused with loss of already-valid local serving
   state.

Opening local metadata must not require replaying remote history. Normal
restart/reopen is not a second `discover` operation and must not require a
local metadata wipe merely because the remote head advanced while the client
was offline.

Remote volume identity verification remains a separate safety check. A failure
to verify identity is an identity/authority result, not evidence that local
redb cannot serve its existing view and not permission to add an unbounded
replay wait.

### Fresh client and `discover`

`discover` is the checkpoint-first bootstrap path for a client without a local
metadata view. One remote bootstrap read must capture a self-consistent:

- volume identity;
- format/protocol information;
- latest published serving checkpoint;
- `checkpoint_version`;
- remote `metadata_state.version`, which becomes the frozen startup target.

The client imports the checkpoint into local `redb` before it begins replaying
the event range after that checkpoint. Once the imported local view is valid,
the client is `local-ready` even while bounded catch-up continues. A fresh
client remains read-only until both conditions hold:

- its replay cursor has reached the frozen startup target; and
- required write authority, including the paired local allocator state, is
  available.

If the bootstrap read cannot obtain a valid checkpoint, discovery fails as a
retryable bootstrap failure and must not create a misleading half-initialized
local root. Once a valid checkpoint has been imported, a later remote outage
must leave a recoverable local/read-only state rather than make the disk
permanently unmountable.

## Bounded, Paged, and Resumable Catch-Up

Catch-up applies the ordered event log after a checkpoint or the last local
cursor. Each operation follows this sequence:

1. Read the remote head once and record it as the frozen startup target.
2. Read at most the configured page limit of complete event batches whose
   versions are greater than the cursor and no greater than the target.
3. Apply that page of complete batches in event order to local `redb`,
   preserving local overlay authority and base/overlay conflict rules.
4. Commit the local serving-state changes and replay cursor together.
5. Repeat while the cursor is below the frozen target, subject to the per-run
   page, byte, operation-time, and cancellation bounds.

The producer must enforce the replay envelope before assigning a
`batch_version`. It splits each pending local event range into one or more
transactional batches, and every published batch must fit both the configured
replay row limit and replay byte limit after event encoding. The producer
limits must be no greater than the corresponding replay page limits. SQL insert
chunking inside one transaction is not a batch bound: all rows published under
one `batch_version` count toward the same batch. If a single event cannot fit
the configured byte limit, publication must return a typed size/unsupported
event error or use a durable chunking representation; it must never exempt that
event or batch from the replay bounds.

`EventTooLarge` is a permanent configuration error for the current client
configuration, not a transient PostgreSQL failure. The flush path identifies
the local journal sequence for the oversized projected event, marks only that
event rejected, completes the claim, and restores the remaining unprocessed
events so independent work stays schedulable. The rejected event remains in the
local event log with the durable replay cursor unchanged; after increasing the
byte limit, restart the client to reconstruct and retry the pending range.
While that engine remains open, local-only retirements and self-acknowledgments
for later ranges must not move the durable cursor across the rejected gap.
Independent later batches may still publish, but restoration must not put the
already rejected range back into the active flush queue. These in-memory
rejection/publication markers reset on restart; the unchanged durable cursor
then reconstructs the local journal from the gap under the current limits.
Because the cursor is one contiguous watermark, that reconstructed suffix can
include later ranges already published by the previous process; this recovery
is at-least-once for event-log publication, not exactly-once row insertion.
When a self-ack advances the cursor to a locally completed range, the cursor
must continue through adjacent completed ranges before pending replay is
restored, stopping at any rejected range.
Journal-derived overlay conflicts include unprocessed and rejected local
events; published and completed ranges no longer reserve their old keys.
Rejected ranges remain local authority until replay can resume past the gap.

The catch-up worker checks the operation deadline and cancellation boundary
before each batch, after the batch sequence, and after serving-state
reconciliation immediately before commit. Reaching any boundary aborts the
page transaction without advancing the cursor. The local replay deadline
starts after a page has been fetched and decoded, immediately before acquiring
the local replay locks; the PostgreSQL page query has its own command timeout.
The producer-side row/byte caps keep any one attempted batch bounded; the
worker must not waive the operation-time bound for a large batch.

The background startup session also has a finite page budget while it holds
the replication lock. Reaching that budget returns a typed resumable outcome;
the next session continues from the durable local cursor instead of extending
one lock hold indefinitely.

The cursor is durable progress, not an optimistic remote acknowledgment. A
page boundary must never split a `batch_version` batch. The cursor advances to
the last fully applied batch, so a page whose nominal row/byte budget would be
smaller than one batch is invalid because the producer contract above forbids
such an oversized batch. `seq_id` is only the ordering tie-breaker within the
batch and must not become an independently persisted resume position. A failed
or interrupted page must be retried from its previous cursor. A page that
committed locally must never be replayed in a way that loses later local
authority. A newly published event after the frozen target belongs to a later
sync operation; the current operation must not chase a moving remote head.

The implementation may schedule later bounded catch-up attempts, but it must
not turn those attempts into an unbounded mount-start wait. Every retry has a
typed failure outcome and an observable next-resume position.

## Local Replay Lock Ordering

Operations that update the local replay cursor together with serving state or
pending mutation state acquire `local_position_lock` before `pending_state`.
Replay page apply, local journal acknowledgment/restoration, and serving
checkpoint import retain both locks across their coupled local transition.
Helpers that persist a cursor while those locks are held must use their
`*_locked` form instead of reacquiring `local_position_lock`.

When replay or a `PreserveLocalState` checkpoint import also needs
`active_open_inodes`, it is acquired after those two locks:
`local_position_lock` → `pending_state` → `active_open_inodes`. Active-open
tracking may hold `active_open_inodes` by itself, but must release it before
acquiring either earlier lock.

The network page/checkpoint read stays outside this lock scope. This ordering
keeps a foreign replay page from racing with local mutation retirement while
allowing replay and checkpoint import to wait for foreground mutations without
forming a lock cycle with local acknowledgment.

Remote event ordering is version-first and uses the event-log tie-breaker for
rows in the same batch. Runtime queries and cursor persistence must use the
same ordering contract; a client must not skip an earlier foreign batch merely
because it has already published a newer local batch.

## Checkpoint Freshness Policy

Checkpoint freshness is a bounded-lag guarantee, not only a periodic timer.
For every volume, the checkpoint publisher must maintain:

    remote_head - checkpoint_version <= max_checkpoint_lag_versions

`max_checkpoint_lag_versions` is an explicit operational policy value. The
policy must be visible to the checkpoint publisher and its health/diagnostic
surface, and it must be validated as a positive finite bound. A periodic
interval may trigger a refresh, but a timer alone does not satisfy this
contract.

If checkpoint publication cannot maintain the configured lag bound, the system
must report the checkpoint as stale/degraded and use one of the recoverable
paths below. It must not silently claim a fresh bootstrap or make all local
disks permanently unavailable.

Checkpoint publication is volume-scoped and single-flight. Before performing
the expensive checkpoint serialization, a publisher must acquire a bounded
coordination lease or equivalent claim (for example, a PostgreSQL advisory
transaction lock keyed by volume). A publisher that cannot acquire the claim
must defer and report the attempt rather than serialize a duplicate checkpoint
concurrently. A process-local replication mutex is not sufficient coordination
across clients.

## Event-Log Retention

After a fully published checkpoint is available, background PostgreSQL
maintenance may prune obsolete `event_log` rows. The candidate floor is the
minimum of that published `checkpoint_version` and every retained Snapshot v2
source's first required event version minus one. Source rows belonging to
`creating`, `ready`, or `deleting` snapshots remain retention guards until the
source row is removed; a source with no event rows does not lower the floor.

The maintenance transaction takes the dedicated per-volume event-log
prune/replay advisory lock and the published `metadata_state` row lock before
calculating the floor. It removes
only complete `batch_version` groups, limited by the configured number of
groups per transaction, and advances the persisted
`metadata_state.event_log_prune_floor` only after no lower rows remain. The
`event_log_pruned_rows` counter and structured maintenance record expose the
amount removed without exposing credentials or connection details. Pruning is
not part of foreground writes, flushes, or startup.

Before each bounded replay page, the client takes a shared transaction-level
advisory lock in the event-log prune/replay domain keyed by volume, then reads
`checkpoint_version` and the inclusive `event_log_prune_floor` from
`metadata_state` in the same transaction as the event-page query. A cursor
below that floor returns typed
`MetaError::RebootstrapRequired` with the local cursor, retained floor,
checkpoint version, and frozen target. The PostgreSQL pruner takes the matching
exclusive event-log prune/replay advisory lock before deleting history or
advancing the floor, so
pruning cannot turn a page that passed the check into a false successful
convergence; ordinary metadata version updates do not contend on this replay
lock.

A cursor equal to or above the floor continues normal paged replay. The bounded
event-page query still rejects any non-empty page whose first batch is not
exactly the next version after the local cursor, and the decoded sync path
rejects gaps between adjacent batches in one page. Those structural or
corruption cases return typed `MetaError::ReplayGap`; an empty page while the
cursor is below the requested target uses the same error with
`first_available_version = local_version + 1` as an empty-window marker. Both
errors record `Degraded` and block new mutations. SFS-711 owns the explicit
rebootstrap signal; automatic checkpoint import and local rebase are owned by
SFS-712.

## Finite Remote History and Recovery

Remote event history is finite: retention, pruning, corruption, or a missing
range can make a local cursor too old to replay directly. This condition is a
recoverable synchronization state, not a permanent mount failure.

When the required event range is unavailable, the client must:

1. request the newest valid serving checkpoint and a new frozen target;
2. import that checkpoint without discarding newer local overlay authority or
   unflushed local event state;
3. resume from the checkpoint version through bounded pages; or
4. remain `degraded read-only` with the local view available if remote recovery
   is temporarily impossible.

The recovery path must preserve local accepted state, must not invent remote
   history, and must expose enough state for a later reopen or sync cycle to
   resume. A finite remote history must never make a disk permanently
   unmountable solely because the old cursor cannot be replayed.

## Read/Write Authority

Local serving readiness and write authority are orthogonal:

- `local-ready` permits reads of the valid local view and any local metadata
  operations that are explicitly safe in the current authority mode;
- `catching-up` does not imply write authority;
- `target-reached` does not imply write authority;
- `read/write` authority is granted only after the startup target and all
  required allocator/authority checks succeed;
- loss of authority blocks new mutations through the existing typed read-only
  metadata boundary while retaining known local reads when possible.

The fresh-client gate is deliberately stricter than ordinary steady-state
offline behavior. An existing client with trusted durable allocator authority
may retain its separately defined local-first write behavior while PostgreSQL
is temporarily unavailable; that exception must not allow a fresh client to
skip its startup target.

## Local-Only Destruction Boundary

`destroy --local` is a local lifecycle operation. After its existing active
mount and confirmation checks, it may remove only the client-local metadata,
cache, mount records, and other explicitly local state. It must not require or
attempt PostgreSQL or S3 access, and it must not perform remote identity
verification as a prerequisite.

Full remote destruction is a different operation with its own identity,
confirmation, PostgreSQL, and object-storage requirements. The local-only
boundary remains valid when PostgreSQL is down, S3 is down, or remote history
cannot be replayed.

## Error and Observability Contract

The implementation must distinguish at least these outcomes:

- local metadata format/open/repair failure: local serving cannot start;
- remote bootstrap unavailable before a checkpoint exists: discovery is not
  complete and may be retried;
- remote catch-up unavailable after local state is valid: local serving remains
  available, health is degraded, and authority is reported separately from
  `catching-up` progress;
- startup target not reached: fresh-client writes remain rejected;
- write authority unavailable: known local reads remain available while new
  mutations use the existing typed read-only error category;
- finite/pruned history: checkpoint rebootstrap or recoverable degraded state;
- local-only destroy: no remote-service error can be introduced by the local
  cleanup path.

Diagnostics may expose applied version, frozen target, checkpoint version,
observed remote head, replay pages, resume cursor, checkpoint lag, lifecycle
state, authority state, and the last typed failure. They must not expose
credentials, full DSNs, or secret-bearing configuration.

## Contract-to-Implementation Map

The contract is intentionally defined before the runtime changes are complete:

- SFS-696 introduces the incompatible PG sync format/version boundary.
- SFS-697 adds explicit local-ready, catching-up, target-reached, and
  read/write authority lifecycle state.
- SFS-698 removes remote catch-up from `PgMetaEngine` open and moves bounded
  replay behind local serving readiness.
- SFS-699 adds the bounded complete-batch event-log page query used by runtime
  replay; durable per-page progress and background session orchestration remain
  follow-up work.
- SFS-721 already implements the local-only destroy boundary on `main`; future
  changes must preserve that boundary without PostgreSQL or S3.

The remaining follow-up work includes durable per-page progress and background
catch-up session orchestration. SFS-721 is already shipped on `main`. This
document remains the contract for those follow-ups; it does not make the page
query responsible for their durable state transitions.

## Validation Boundary

The portable SFS-695 documentation test checks that this contract and its
cross-document links retain the required invariants. Runtime validation belongs
to the follow-up issues and must include focused PostgreSQL tests for checkpoint
bootstrap, bounded page replay, resume after interruption, target freezing,
fresh-client read-only gating, finite-history recovery, and local-only destroy.
Live PostgreSQL, S3, and native mounted checks are environment-gated and are
not replaced by the documentation test.

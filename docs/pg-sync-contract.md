# PostgreSQL Sync Contract

## Status

This document is the canonical PostgreSQL sync contract for SFS-695 and the
follow-up implementation issues SFS-696, SFS-697, SFS-698, and SFS-721. It
defines the behavior that the metadata, CLI, VFS, and cleanup implementations
must preserve.

This is a breaking on-disk/protocol change. Migration compatibility is not
required for existing test disks. A client or test fixture must use the new
format/protocol boundary rather than relying on an old PostgreSQL startup
layout.

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
| `remote head` | The monotonic `metadata_state.version` observed at one point in time. It can advance after the observation. |
| `replay cursor` | The locally persisted position through the ordered remote event stream. The cursor advances only after the corresponding page has been applied locally. |
| `startup target` | One remote version captured for a client startup or catch-up operation. It is frozen for that operation and is not re-read as a moving completion target. |
| `local-ready` | Local `redb` is open and its valid local serving view can answer foreground reads and metadata requests. |
| `catching-up` | A background or explicit sync operation is applying remote checkpoint/delta state toward a frozen startup target. |
| `target-reached` | The local replay cursor has reached the startup target for the current operation. This does not by itself grant write authority. |
| `write authority` | The independent capability to accept new shared mutations. It includes the required local allocator state and any PostgreSQL-backed authority registration. |
| `degraded read-only` | A usable local serving view whose remote catch-up or write-authority precondition is not currently complete. |
| `checkpoint lag` | The difference between the observed remote head and the checkpoint version used for bootstrap. |

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
2. Read at most the configured page limit of event rows and bytes whose
   versions are greater than the cursor and no greater than the target.
3. Apply that page in event order to local `redb`, preserving local overlay
   authority and base/overlay conflict rules.
4. Commit the local serving-state changes and replay cursor together.
5. Repeat while the cursor is below the frozen target, subject to the per-run
   page, byte, operation-time, and cancellation bounds.

The cursor is durable progress, not an optimistic remote acknowledgment. A
failed or interrupted page must be retried from its previous cursor. A page
that committed locally must never be replayed in a way that loses later local
authority. A newly published event after the frozen target belongs to a later
sync operation; the current operation must not chase a moving remote head.

The implementation may schedule later bounded catch-up attempts, but it must
not turn those attempts into an unbounded mount-start wait. Every retry has a
typed failure outcome and an observable next-resume position.

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
  available, with `catching-up` or `degraded read-only` state;
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
- SFS-721 enforces the local-only destroy boundary without PostgreSQL or S3.

Until those implementation issues land, existing startup code and some
integration tests may still exercise the superseded synchronous path. This
document is the target contract for that migration; it is not a claim that
those follow-up runtime changes are already implemented.

## Validation Boundary

The portable SFS-695 documentation test checks that this contract and its
cross-document links retain the required invariants. Runtime validation belongs
to the follow-up issues and must include focused PostgreSQL tests for checkpoint
bootstrap, bounded page replay, resume after interruption, target freezing,
fresh-client read-only gating, finite-history recovery, and local-only destroy.
Live PostgreSQL, S3, and native mounted checks are environment-gated and are
not replaced by the documentation test.

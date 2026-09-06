# Shared metadata-cache coherence contract

SFS-675 defines semantics for subsequent SFS-676 (invalidation delivery),
SFS-677 (health), and SFS-678 (Windows policy). The implementation status below
distinguishes delivered health code from subsequent adaptive policy work. Windows is
the first validation
target. macOS and Linux must be able to consume these semantics without changing
them; this issue changes neither platform's cache policy.

The [local-first IO contract](juicefs-local-first-rust-contract.md) takes
precedence. Coherence means consistency with the mount's current local view,
including accepted writer state, and eventual convergence with applied published
peer state. It does not mean synchronous agreement between disconnected clients.
No metadata callback may add a PostgreSQL or object-storage wait to obtain global
freshness. Flush and close do not establish peer visibility.

## Semantic invalidation intents

Shared logic names volume-scoped inode identities and directory-entry names;
it never emits Windows paths, notification flags, handles, or timeout sentinels.
An intent identifies the local-view generation whose mutation it covers. A
generation is an ordering token, not a wall-clock timestamp or a global promise.

| Intent | Required effect |
| --- | --- |
| Attributes(inode) | Discard stale size, timestamps, link count, and other cached attributes; preserve valid data when the adapter can distinguish the two. |
| Entry(parent, name) | Discard both positive and negative lookup results for this name. A cached absence is not exempt. |
| Directory(parent) | Discard stale enumeration membership and entry attributes; a new enumeration must use the current local view. |
| Content(inode) | Discard stale content and associated size/attributes for fresh resolution; preserve newer local dirty writer state and the canonical version binding of existing open handles. |
| Reset(scope) | Prevent reuse of all untrusted metadata within the affected mount or namespace scope when precise coverage is lost. |

Create/link require Entry, Directory and affected inode/parent Attributes.
Unlink requires the removed Entry, Directory and surviving inode/link-count
Attributes. Rename requires both old and new Entry intents, both directories
(once when identical), and attributes of moved, replaced, and affected parent
inodes. Retain old identity/name information before deleting or replacing it;
resolving only the new path cannot invalidate the old name. Published content
replacement requires Content plus Attributes even when the inode is unchanged.
Metadata-only mutation requires Attributes and invalidation of enumeration data
that contains those attributes. Hardlink aliases must observe the same inode
attributes and content.

Commit the change to the local visible view before declaring its invalidation
covered. Prevent a callback racing with delivery from repopulating older data:
its observation generation must still be valid when its result is admitted to a
cache. Coalescing may strengthen or combine intents, but must preserve both
rename names and every affected scope. Queue acceptance is not delivery.

Delivery completion means the adapter has established the required cache effect
or an effective conservative fallback. It does not mean every application has
consumed a change notification. Adapter inability, queue saturation, dropped
work, or failed delivery must be observable; none may silently count as success.
Foreground mutation must not wait for notification delivery while holding locks
needed by callbacks or by the notification worker.

## Coherence health

Health belongs to each mounted local view. Track replication progress and
invalidation coverage separately; a live connection alone proves neither.

| State | Meaning and entry condition |
| --- | --- |
| Unknown | Startup/restart or missing evidence. Conservative policy is required. |
| CatchingUp | Replaying a known ordered boundary and restoring invalidation coverage. Conservative policy remains required. |
| Healthy | Applied through a recorded boundary, no known gap, and all required invalidations through that boundary covered; lag and delivery age remain within declared budgets. |
| Degraded | Disconnect, apply error/gap, excessive lag, delivery failure, queue backpressure that exceeds the delivery budget, or loss of coverage. Conservative policy is required immediately. |
| Stopped | Mount is shutting down; no new cache promotion or detached delivery work is allowed. |

Unknown progresses through CatchingUp to Healthy. Any unsafe condition moves
an active state to Degraded. Reconnection moves Degraded to CatchingUp, never
directly to Healthy. Record a fresh catch-up boundary, apply it, invalidate or
retire untrusted entries, and confirm adapter coverage before recovery. A new
failure during catch-up aborts recovery. Shutdown from any state enters Stopped.
An idle replication stream needs an affirmative progress/freshness signal;
absence of events is not proof of health.

SFS-677 must specify and expose finite lag/delivery budgets and the source of
the progress signal before enabling eligibility. This contract does not invent
production thresholds from an unmeasured baseline. Diagnostics must expose
state, reason, applied boundary, covered boundary, pending delivery age and
effective policy without credentials or backend connection strings.

## Cache policy and adapter boundary

Policy is distinct from health: Healthy permits promotion, it does not require
it. Policy states are Conservative, AggressiveEligible, and Aggressive. Startup,
degradation, shutdown and unsupported capability require Conservative.
AggressiveEligible requires Healthy plus the promotion gates below;
Aggressive additionally requires an explicit supported policy selection.
Any loss of eligibility revokes Aggressive immediately.

Conservative means untrusted OS metadata cannot outlive a documented finite
safe boundary. Merely lowering future TTLs does not retire entries already
admitted under a longer lifetime. The adapter must invalidate existing state or
otherwise prevent its reuse. If its API cannot perform a safe live transition,
it must refuse aggressive admission; do not claim a mutable policy on the basis
of mount-time settings. Returning to Conservative must not discard accepted
local writes or make remote service availability a prerequisite for local IO.

Shared domain concepts define the intents and health/policy semantics. Ports
describe required delivery and capability reporting; application logic tracks
ordering and eligibility. Adapters translate to native operations, own native
buffers/threads and report effective coverage and failures. Dependency direction
is domain <- ports <- application <- adapters/runtime/API. Unsupported precise
invalidation may use a stronger safe invalidation, with the extra scope reported.

The legacy methods of `KernelInvalidator` in
`crates/studiofs-vfs/src/vfs/namespace.rs` retain their compatibility behavior:
entry invalidation defaults to no-op and attributes default to full inode
invalidation. Its semantic batch method explicitly reports Unsupported for legacy
adapters, even when it invokes those methods as best-effort compatibility work.
Callers cannot use that result as evidence for aggressive caching.

## Windows promotion gate

Full mounted E2E scenarios **1-25 must pass 10 consecutive complete runs with
no retry or manual reset** on the candidate binary and recorded configuration.
A failed, aborted, missing, or partially executed run breaks the streak. Normal
runner-owned setup/teardown is allowed; a remedial restart, cleanup or rerun to
hide a failure is not. Retain every attempt and its artifacts, including failures.
The separate full integration suite must also pass. Unit tests or successful
benchmark timings cannot substitute for either gate.

Before promotion, cover at least these multi-client cases with warmed attributes,
positive and negative lookups, and enumeration caches on the observer:

* Peer create after a cached miss; delete after a cached hit; delete and recreate
  the same name with a different inode.
* Rename within/across directories, replacement of an existing destination,
  directory rename with cached descendant paths, and rename/unlink races.
* Same-inode overwrite, growth, truncation, and metadata-only timestamp changes;
  fresh path resolution, enumeration and newly opened reads must agree with the
  applied version. Existing handles retain their canonical inode/version binding.
* Hardlink create/unlink and mutation through an alias, including last unlink
  while a handle remains open.
* Local dirty writer state concurrent with a peer publication; preserve the
  canonical conflict/visibility contract and never overwrite newer local state
  merely to invalidate OS metadata.
* Disconnect, missed notification, delayed replay, queue saturation, transient
  delivery failure, restart and reconnect; verify degradation and recovery
  through an explicit catch-up boundary.
* Open handles and enumeration continuation during mutation, plus a fresh open
  or restarted enumeration after convergence; preserve reads through existing
  handles according to the canonical open-version contract and do not demand
  retroactive changes to results already returned to an application.

Each test must distinguish remote publication, local apply, invalidation coverage,
and observed OS behavior using bounded condition waits rather than fixed sleeps.
Windows evidence does not establish macOS/Linux acceptance.

## SFS-676 implementation status

`studiofs-meta::coherence` defines mount-local, ordered invalidation batches.
Redb write transactions publish retained intents only after successful commit,
before releasing write serialization. The PostgreSQL adapter exposes that local
journal through `MetaReplication`; VFS consumes it before and after remote sync,
including partial-sync failure and unchanged remote-token cases. The existing
snapshot comparison remains as conservative reconciliation, without claiming
precise delivery coverage from a snapshot diff.

Entry intents retain raw historical ancestor components and the previous inode
binding. These contain no Windows separator or native notification flag. Directory
rename/removal captures old descendant names before mutation. The journal is
bounded to 4096 intents and 1 MiB; bounded ancestry/subtree traversal exposes
unknown provenance or Reset instead of silently claiming complete coverage.
Attributes and content are distinct semantic scopes, including same-size epoch
changes and metadata-only mutations. VFS cache admissions retain observation
tokens so old lookup, attribute and path-resolution results cannot repopulate a
retired cache generation.

The WinFSP adapter preserves path/action/filter identities when coalescing and
keeps old paths during retry. Semantic enqueue and retry cannot block at capacity.
`WinfspDeliverySnapshot` reports queue acceptance, native submissions, failed and
unsupported work, and writer deferral. A partially deferred generation cannot
advance the fully accepted generation. Local writable handles and dirty state
defer native purges, retaining exact historical targets for a later timer pass.

Native WinFSP invalidates more than its notification filter names suggest:
[`FspFileNodeInvalidateCachesAndNotifyChangeByName`](https://github.com/winfsp/winfsp/blob/master/src/sys/file.c)
flushes/purges content as well as metadata, while
[`FspVolumeNotify`](https://github.com/winfsp/winfsp/blob/master/src/sys/volume.c)
queues asynchronous kernel work. Consequently attributes/entries may receive a
stronger native invalidation; the adapter checks writer protection at admission,
preparation and submission. The current Rust notifier does not expose completion,
so `delivered_generation` deliberately remains unknown. These checks do not prove
that no writer can start after asynchronous native submission.

Reset has no proven native whole-mount implementation and reports Unsupported.
Unresolvable/oversized paths, incomplete bounded alias traversal and queue loss
remain explicit failures. No adaptive/aggressive admission or timeout policy is
enabled by SFS-677; that promotion remains SFS-678 work. Focused unit tests do
not replace the full integration and mounted gates above.

## SFS-677 implementation status

`studiofs-vfs::coherence` now owns a mount-local health tracker with the
`Unknown`, `CatchingUp`, `Healthy`, `Degraded`, and `Stopped` states. It records
the applied metadata boundary, retained invalidation boundary, optional remote
apply lag, pending delivery age, and the effective finite budgets. Every mount
starts `Unknown`; synchronization errors, missing authoritative progress,
excessive lag, incomplete/failed invalidation admission, delivery timeout, and
shutdown revoke cache eligibility immediately. A reconnect enters
`CatchingUp` and needs an explicit coverage confirmation before it can become
`Healthy`.

The metadata replication port reports applied version, observed PostgreSQL
remote head, command success evidence, and the last successful sync timestamp.
The PostgreSQL implementation updates those values only around bounded
metadata commands; an open pool connection alone is not used as health proof.
Standalone redb metadata reports local-only progress.

VFS degradation retires attribute observations, advances the namespace epoch,
clears known OS-cache generations, and asks the installed kernel invalidator to
purge known cached inodes (including the root directory). Active writable
handles remain protected by the existing deferred-invalidation contract. The
WinFSP bridge forces `DisableCache` for new opens whenever health is not
`Healthy`; write-capable opens retain their existing unconditional bypass.
Diagnostics and runtime snapshots expose the additive `coherence` object.

This issue does not enable adaptive/aggressive promotion or change macOS/Linux
policy; those remain SFS-678 and later platform work. Native WinFSP completion
is still unobservable, so retained queue admission and bounded delivery age are
the strongest health evidence available. Full integration and the ten-run
mounted promotion gate remain operator validation.

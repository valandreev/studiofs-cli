# Shared metadata-cache coherence contract

SFS-675 defines semantics for subsequent SFS-676 (invalidation delivery),
SFS-677 (health), and SFS-678 (Windows policy). The implementation status below
distinguishes the shared contract, delivered health code, and the Windows-only
adaptive policy. Windows is the first validation target. macOS and Linux must be
able to consume these semantics without changing them; this policy does not
change either platform's cache behavior.

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

Visible self batches mirror this client's own acknowledged events into `base_*`
without journaling intents: the mounted kernel already observed those mutations
through the local serving view. Re-announcing them invalidated unchanged cache
state and, for active writers or removed inodes, failed adapter admission and
kept the coherence gate closed. Serving-view reconciliation after a self batch
(rename reconciliation and regular-file serving from base) still journals its
intents, except that regular-file and dentry reconciliation discards them when
the merged attributes and dentries it exposes are unchanged. Reconciling toward
base is a no-op for this client's own acknowledged writes, such as replacing an
uploaded file by name, so only a real serving correction is announced. The same holds for replaying this client's own pending local events into serving after a flush or acknowledgement: it journals nothing when the touched inodes, dentries and file rows end up unchanged.


The WinFSP directory snapshot observation currently includes one mount-wide
aggregate attribute generation. Any committed attribute invalidation therefore
conservatively invalidates directory snapshots across the mount, including
directories unrelated to the changed inode. This is an intentional bounded
correctness choice; per-directory or subtree-scoped attribute generations
remain a separate optimization and must preserve the same post-commit
observation check before cache admission.

The WinFSP adapter preserves path/action/filter identities when coalescing and
keeps old paths during retry. Semantic enqueue and retry cannot block at capacity.
An individual inode request rejected at capacity retains a fixed-size overflow
marker. Timer drains refill the queue from bounded, sorted pages of local redb
inode candidates; removed candidates are skipped during ordinary preparation.
This conservative sweep may purge unrelated cached inodes, but needs no further
metadata mutation or remote service. Repeated overflow restarts the cursor while
preserving the oldest overflow age; shutdown cancels reconciliation. Page reads
run outside the producer's queue lock, and existing writer protection still
applies. No unbounded overflow list or whole-volume snapshot is retained.
Rejected path retries with a known inode also restart this sweep. It recovers
current inode paths; it cannot reconstruct lost historical rename/unlink names,
whose rejection remains an observable delivery failure. A full queue drains
before the next timer pass can refill it, leaving slots for writer-deferred
historical targets. Each metadata page bounds raw candidates, including tombstones,
rather than scanning an unbounded number of deleted rows to fill a live page.
Native notification names are counted UTF-16 without a terminator. The Rust
`set_name` convenience method includes a NUL in the reported size, so this
adapter uses `set_name_raw` to avoid an invalid native notification name.
`WinfspDeliverySnapshot` reports queue acceptance, native submissions, failed and
unsupported work, and writer deferral. A partially deferred generation cannot
advance the fully accepted generation. Open write-capable handles
defer native purges, retaining exact historical targets for a later timer pass.
WinFSP caches by path, so attribute, content or directory scopes for an inode
that no longer exists add no target and do not fail the generation; the old
names are purged by their entry intents. Queued inode notifications for removed
inodes are likewise skipped without counting as delivery failures. Existing
inodes whose current path cannot be resolved still fail admission.
Open unlinked non-directory inodes (`nlink == 0`) likewise have no current path
to purge; their retained historical entry notifications remain necessary.
Legacy inode notifications and overflow reconciliation resolve one current path
per inode. They do not establish cache invalidation for every hardlink alias;
semantic admission without explicit alias entry paths remains conservative for
multiply linked files. Full hardlink-alias coverage is still a promotion gap.
Overflow recovery reports a separate pending-coverage signal to VFS health,
retained through prepared and requeued notification batches. Successful sync
cannot promote health while this signal is set. Clearing it does not itself
restore health: fresh catch-up coverage is still required. Submission remains
asynchronous and is not proof of native kernel consumption.
Read-only cache admission also checks the live adapter signal, so a stale health
observation cannot admit a new cached handle during active recovery.

A generation deferred for an open write-capable handle (which the adapter reports
as `Failed`) stays incomplete without consuming retry attempts. The invalidation worker re-checks such a
generation locally every second, without a remote sync, and retries it only once
no listed inode has an open writer left. Pending uploads and unflushed VFS state
never defer or hold a generation: the VFS owns that data, a write-capable WinFSP
handle bypasses the cache manager, and a purge only makes the kernel ask the VFS
again. Completing the generation lets the next
pass promote health, so recovery needs neither a new metadata change nor the
fallback poll.

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

Native WinFSP completion is still unobservable, so retained queue admission and
bounded delivery age are the strongest health evidence available. Full
integration and the ten-run mounted promotion gate remain operator validation.

## SFS-678 Windows policy implementation status

The Windows adapter selects `adaptive` by default and accepts
`STUDIOFS_WINFSP_METADATA_CACHE_POLICY=conservative` as a benchmark and
operational rollback. Unknown policy values fail safe to `conservative` and
emit a warning. This switch is Windows-only; no macOS or Linux policy reads it.

In adaptive mode, WinFSP is mounted with `FileInfoTimeout=u32::MAX` to enable
the cache-manager fast path and with 60-second `DirInfoTimeout`,
`VolumeInfoTimeout`, `SecurityTimeout`, `StreamInfoTimeout`, and `EaTimeout`
defaults. Each timeout retains its existing environment override. The
security timeout is explicit because `get_security_by_name` is the WinFSP name
resolution seam used for both positive and negative lookups. Entry and
directory invalidation intents retain the old/new names and parent scopes so a
cached miss is not trusted after a peer create, rename, or unlink.

In conservative mode, all six metadata timeouts default to zero and every new
handle bypasses OS cache admission. In adaptive mode, SFS-677 health gates
read-only file-cache admission: only healthy mounts return
`Create.Opened.DisableCache=0`; startup, reconnect, lag, delivery failure,
unsupported coverage, and shutdown disable admission for later opens. This
per-handle bit controls WinFSP/Windows file-data caching; it does not mutate
the mount-time directory, security/name, stream, or extended-attribute TTLs.
Write-capable handles always bypass cache-manager writeback. When health becomes
unsafe, VFS retires known cached state and enqueues the root plus known inodes
for native invalidation; queue acceptance, bounded delivery, and failures
remain observable, so native cache retirement is not claimed to be instantaneous.
The `coherence` snapshot plus WinFSP cache-enabled/disabled open counters and
invalidation delivery diagnostics are the observable policy and fallback
evidence.

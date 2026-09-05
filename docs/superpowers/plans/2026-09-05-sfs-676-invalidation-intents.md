# SFS-676 shared invalidation intents implementation plan

This ExecPlan follows `.agent/PLANS.md` and is maintained as implementation proceeds.

## Purpose / Big Picture

Replicated mutations must tell the mounted Windows filesystem which cached names,
directory listings, inode attributes and content have become stale. Preserve every
old rename/unlink name even when several mutations happen before the notification
worker runs. Keep the current cache policy and local dirty/open-version behavior.

## Progress

- [x] (2026-09-05 14:49Z) User approved the design; inspected the current transaction,
  metadata worker and WinFSP queue boundaries.
- [x] Add platform-neutral intents, bounded committed transaction journal and metadata port.
- [x] Consume committed batches in VFS without bypassing active writer protection.
- [x] Map intents in WinFSP with name-preserving queueing and explicit failure reporting.
- [x] Run focused regressions, review the combined changes and record validation limits.

## Surprises & Discoveries

Remote replay commits multiple independent redb transactions. Publishing intents
only after replay succeeds loses coverage when a later stage fails. The existing
WinFSP queue stores one path override per inode and cannot preserve multiple names.
`KernelInvalidator::inval_entry` currently defaults to a no-op.

## Decision Log

Use the existing redb write-transaction guard to order intent publication after
successful commit. Store only platform-neutral identities in the metadata crate.
Use explicit Reset on lost precision and never report queue acceptance as native
delivery. Keep legacy invalidation for adapters that do not support semantic batches.
This confines native translation to Windows and preserves other platform behavior.

## Outcomes & Retrospective

The shared journal, VFS consumer and Windows adapter are implemented. Eight
metadata journal tests, seven semantic VFS tests and eleven semantic Windows tests
pass. The original two-deleted-paths regression failed with one retained name
before the queue change and passes afterward. Three existing queue tests, two
notification-drain tests and the retry-at-capacity regression also pass. Targeted
Clippy for metadata, VFS and WinFSP libraries/tests passes with warnings denied.
The final focused set contains 44 passing tests, including adjacent namespace
resolution, active-open protection, read-only handles and delete-cleanup checks.
Formatting and diff checks pass.
Full integration and mounted E2E gates have not been run and remain user-run
acceptance. Initial estimate was 60–90 minutes; core implementation and this
verification checkpoint completed in approximately 40 minutes.

## Context and Orientation

`crates/studiofs-meta/src/redb_engine/replication.rs` commits replicated local
serving state. `crates/studiofs-meta/src/engine.rs` exposes metadata capabilities;
the PostgreSQL adapter delegates local reads to redb. A local generation orders
committed invalidation batches and is not a global replication version.
`crates/studiofs-vfs/src/vfs/workers.rs` currently compares periodic metadata
snapshots and protects active open overlays before invalidating kernel content.
`crates/studiofs-winfsp/src/context/invalidation.rs` queues notification targets,
prepares native buffers and retries batches when WinFSP cannot begin notification.

## Plan of Work

First add `studiofs-meta::coherence` with `InvalidationIntent` variants Attributes,
Entry, Directory, Content and Reset, and `InvalidationBatch { generation, intents }`.
Capture removed aliases before transaction edits. Publish only on commit, while
the existing write serialization still preserves ordering. Expose a destructive
single-consumer drain through `MetaReplication::take_invalidation_batches` and
forward it through PostgreSQL and the VFS runtime facade. Bound retained intents;
replace lost precision with Reset rather than silently dropping names.

Next add a semantic batch method to `KernelInvalidator` with explicit Queued,
Unsupported and Failed acceptance. VFS drains batches even when sync fails or
version tokens do not change, retires its local caches first, and retains active
writer protection. Existing platform methods remain compatible. Unsupported Reset
and deferred writer coverage cannot count as successful native coverage.

Finally replace WinFSP's inode-only override identity with notification identity
that preserves paths/actions, translate entries including cached misses, attributes,
directories and content, and expose acceptance/failure evidence. Never block a
foreground producer awaiting notification while it may hold callback locks. Keep
NotifyBegin retry semantics and do not equate a void notifier call with confirmed
native delivery. No aggressive timeout or macOS/Linux implementation is included.

## Concrete Steps

Work in `E:\dev\studiofs-cli` on the existing SFS-676 branch. Add failing focused
tests first for rename chains, removed aliases, content-only changes, aborted
transactions, overflow and unsupported adapters. Run Cargo sequentially.

    cargo test -p studiofs-meta --lib coherence -- --nocapture
    cargo test -p studiofs-vfs --lib semantic_invalidation -- --nocapture
    cargo test -p studiofs-winfsp --lib semantic_invalidation -- --nocapture

Add exact adjacent regression filters as implementation identifies their names.
Every command must execute nonzero tests. Use `cargo fmt --all -- --check` and
targeted Clippy for touched crates after behavioral tests pass.

## Validation and Acceptance

Committed rename chains retain old, intermediate and new entries; deletion retains
aliases; same-size publication invalidates content independently of object-id set
changes. Failed transactions emit nothing. Overflow is explicit Reset. Windows
preserves all queued names and reports unsupported/unresolvable targets. VFS does
not invalidate dirty writer content or reinterpret existing open handles.
The full integration suite is a separate user-run acceptance gate; repository
instructions prohibit running it here without a request. No claim of full-suite
or aggressive-cache promotion acceptance follows from these focused tests.

## Idempotence and Recovery

No persistent format or dependency migration is needed. Invalidation journal state
is mount-local and ephemeral. Repeated intents are safe. Native retry retains the
same targets and generation; shutdown and overflow remain observable. Temporary
artifacts belong under `target/` and are removed after use.

## Artifacts and Notes

Source contract: `docs/metadata-cache-coherence.md`, approved in SFS-675. The user
approved implementation of this design on 2026-09-05. Detailed test results and
limitations will be added here as they become available.

## Interfaces and Dependencies

The metadata domain contains no native paths, flags or handles. The VFS port uses
`fn invalidate_batch(&self, batch: &InvalidationBatch, generation_complete: bool) -> InvalidationAcceptance`.
The native adapter depends on that port and translates its semantics using the
existing WinFSP notification API; dependency direction remains inward.

Initial plan recorded 2026-09-05 to make transaction ordering and coverage limits
explicit before implementation.

Updated 2026-09-05 after implementation and review: Entry additionally carries
historical raw ancestor components and the pre-mutation inode. Bounded descendant
capture preserves directory rename coverage. Attribute/dentry/path cache admission
now rejects retired observations. Partial generation status crosses the adapter
port explicitly. Native work is asynchronous, has stronger content effects than
its filters imply, and lacks observable completion; these limits are documented
in `docs/metadata-cache-coherence.md` and do not permit aggressive admission.

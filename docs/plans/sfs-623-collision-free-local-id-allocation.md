# Replace PostgreSQL ID ranges with collision-free local allocators

This ExecPlan is a living document. Maintain it under `.agent/PLANS.md`.

## Purpose / Big Picture

For a PostgreSQL-backed StudioFS volume, StudioFS will allocate inode IDs and immutable payload-object IDs locally without a PostgreSQL range reservation on the foreground path. Every newly allocated ordinary ID is unique within its volume, including after a crash or local metadata repair. PostgreSQL only mints a namespace when a local allocator is first created or its durable state cannot be trusted. A mount with trusted allocator state continues while PostgreSQL is offline; one that cannot safely allocate remains readable and rejects new mutations through the existing metadata-authority gate. A direct, standalone redb metadata root keeps its existing local-only allocation contract and is not part of this PostgreSQL volume protocol.

## Progress

- [x] (2026-07-30) Validated the current shared inode/object window, the local redb counter, recovery path, and VFS read-only gate.
- [x] (2026-07-30) Resolved the identity encoding, persistence, recovery, overflow, and operator-visible failure rules.
- [x] (2026-07-30) Reconciled the plan with replication client identity, direct-redb scope, high-water bounds, recovery trigger, diagnostics, and platform validation.
- [ ] Implement the allocator and replace PostgreSQL-backed ID windows with paired local allocator state.
- [ ] Add the specified focused tests and update the local-first contract and validation matrix.

## Surprises & Discoveries

- Observation: PostgreSQL `next_inode()` and `next_object_ids()` currently call the same `allocate_inode_object_ids`; the redb backend also uses `NEXT_INODE` for both.
  Evidence: `crates/studiofs-meta/src/pg_engine/local_traits.rs` and `crates/studiofs-meta/src/redb_engine/payload_objects.rs`.
- Observation: StudioFS already enforces a read-only metadata authority for all new mutations while preserving known reads.
  Evidence: `StudioVfs::enforce_metadata_mutation_authority` and `read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open`.
- Observation: a legacy PostgreSQL volume records its shared ID stream in `counters.name = 'next_inode'`; namespace `1` starts at `1 << 39`, so a fresh namespace cannot safely be inferred from a missing local allocator record.
  Evidence: `PgMetaEngine::reserve_inode_window_from_postgres` updates the per-volume `next_inode` row before issuing legacy windows.
- Observation: mounted create and mkdir reserve inode batches through `MetaPayloadObjects::next_object_ids`, not `MetaInodes::next_inode`.
  Evidence: `StudioVfs::reserve_namespace_ino` in `crates/studiofs-vfs/src/vfs/namespace_ops.rs` calls `next_object_ids(NAMESPACE_INODE_BATCH)`.

## Decision Log

- Decision: Give the inode allocator and payload-object allocator distinct, non-reusable namespace IDs.
  Rationale: Their counters must be separate, and distinct namespaces make the numeric IDs collision-free across both identity classes without tags or a shared counter.
  Date/Author: 2026-07-30 / Codex.
- Decision: Persist only the exclusive high-water mark, advancing it before handing an ID to memory; do not persist every in-memory allocation.
  Rationale: A restart can skip an unused reserved tail, which is allowed, but never reuse an ID.
  Date/Author: 2026-07-30 / Codex.
- Decision: Replace both allocator records together after state loss or corruption; do not repair one side in place.
  Rationale: A paired atomic record prevents a mixed trusted/untrusted allocation state. PostgreSQL namespace gaps are harmless and namespaces are never reused.
  Date/Author: 2026-07-30 / Codex.
- Decision: Reuse `MetadataAuthorityStatus` and its existing VFS gate instead of adding platform-bridge policy.
  Rationale: It already blocks write opens, writes, and namespace mutations while allowing reads on macFUSE, Linux FUSE, and WinFSP through shared VFS behavior.
  Date/Author: 2026-07-30 / Codex.
- Decision: Retain PostgreSQL client slots solely as stable replication identities.
  Rationale: `PgMetaEngine::open_with_options` derives `client_id` from `pgClientSlotPlusOne`, and self-batch detection must survive repair. Namespace allocation replaces only the slot's former ID-range role.
  Date/Author: 2026-07-30 / Codex.
- Decision: Limit this namespace protocol to PostgreSQL-backed volumes and retain direct redb's existing local-only allocator.
  Rationale: A direct redb engine has no PostgreSQL volume authority from which to mint a namespace. Replacing its `NEXT_INODE` behavior would make supported unformatted and non-PostgreSQL roots unable to allocate.
  Date/Author: 2026-07-30 / Codex.
- Decision: Retry untrustworthy allocator-state recovery only during the next `PgMetaEngine::open_with_options` call.
  Rationale: No existing background synchronization path owns allocator bootstrap. Requiring remount avoids an untested live writable-state transition while still restoring authority once PostgreSQL is reachable.
  Date/Author: 2026-07-30 / Codex.
- Decision: Refuse namespace bootstrap for every PostgreSQL volume with a legacy `next_inode` counter.
  Rationale: A missing local `AllocatorStateV1` does not prove that the remote volume is new. Rejecting all legacy counter values is conservative and prevents a namespace interval from colliding with arbitrary legacy signed-BIGINT IDs; migration is a separate future task.
  Date/Author: 2026-07-30 / Codex.
- Decision: Add a batched inode allocation method and route namespace creation through it.
  Rationale: A directory or file inode must consume the inode namespace and its headroom, while payload blocks must consume the payload namespace. The existing VFS batch currently uses the payload method only because both classes shared one stream.
  Date/Author: 2026-07-30 / Codex.
- Decision: A PostgreSQL restore that can rewind `next_allocator_namespace` creates a new logical volume rather than reusing the old volume identity.
  Rationale: The registry counter is the sole non-reuse witness. A physical/WAL restore that preserves its high-water mark is safe; a point-in-time or logical restore that can move it backwards cannot safely coexist with prior local allocator roots.
  Date/Author: 2026-07-30 / Codex.

## Context and Orientation

`crates/studiofs-meta` owns all metadata identities. `Ino` and `PayloadRef.object_id` are `u64` values carried by VFS and the platform bridges. `PgMetaEngine` currently reserves PostgreSQL windows in `crates/studiofs-meta/src/pg_engine/inode_allocator.rs`; `crates/studiofs-vfs` must keep treating metadata as the authority, but should not know how an ID is encoded.

`RedbMetaEngine` has two distinct roles. A PostgreSQL-backed `PgMetaEngine` owns one as its local durable state, while `open_meta_engine_with_options` also opens `RedbMetaEngine` directly for non-PostgreSQL metadata and an unformatted root. This plan adds allocator-state helpers for the first role only. Direct redb retains `NEXT_INODE` and `next_object_ids` unchanged because it has no shared PostgreSQL volume or namespace authority.

`StudioVfs::reserve_namespace_ino` keeps a local queue of `NAMESPACE_INODE_BATCH` IDs for file and directory creation. It currently fills that queue by calling `next_object_ids`; after this work it must call the new batched inode method so the queue consumes the inode allocator record. Payload writes continue to use `next_object_ids` and consume only the payload record.

An allocator namespace is a PostgreSQL-minted, volume-scoped number assigned to exactly one local allocator. A local allocator is the durable state for one namespace and one monotonic counter. An ordinary ID is an inode ID or a payload-object ID; a logical chunk remains identified by `(inode, chunk_index)` and receives no new ID.

## Required Identity Contract

For a PostgreSQL-backed volume, all ordinary IDs are positive signed-63-bit values encoded as:

    ID = (namespace << 39) | local_counter

`namespace` occupies the high 24 bits and must be in `1..=0x00ff_ffff`. `local_counter` occupies the low 39 bits and must be in `0..=(1 << 39) - 1`. Therefore every ordinary ID is at most `i64::MAX`. Namespace `0` is reserved for system IDs; existing system IDs, including the root inode, remain system IDs. The allocator must reject invalid decoded state and never create an ordinary ID with namespace `0`.

PostgreSQL mints namespaces from one per-volume monotonic counter, seeded at `1`. The allocation transaction increments that counter and returns its previous value. It rejects a value above `0x00ff_ffff`; it never wraps, decrements, or reuses a namespace. The inode allocator and payload-object allocator each mint their own namespace, so equal local counters still produce unequal numeric IDs.

No timestamp, UUID, content hash, deduplication key, client slot, or logical-chunk number participates in identity. Counter gaps are valid and must not be compacted.

## Durable Local State and Allocation

Store one versioned `AllocatorStateV1` value in the local redb metadata transaction. It contains both records atomically:

    inode:   { namespace, reserved_through }
    payload: { namespace, reserved_through }

`reserved_through` is the exclusive local-counter high-water mark. A record is valid when `namespace` is in `1..=0x00ff_ffff` and `reserved_through` is in `0..=1 << 39`; the latter includes the exclusive endpoint after reserving the final valid local counter. The single record must decode completely, have version `1`, and contain both allocator records. A missing record is expected only for a newly created PostgreSQL-backed local metadata root; a missing, malformed, partially decoded, or out-of-range record in an existing root is untrustworthy.

For an allocator with in-memory interval `[next, end)`, reserve `max(LOCAL_ID_BATCH_SIZE, requested_count)` counters when the interval cannot satisfy a request. In one durable local transaction, check `reserved_through + reservation_size <= 1 << 39`, update `reserved_through`, commit, then expose `[old_reserved_through, new_reserved_through)` to memory. `LOCAL_ID_BATCH_SIZE` is a private constant, initially `4096`, not user configuration. A request may reserve a larger interval. Return a contiguous sequence for a multi-object request.

After an ordinary crash, discard any in-memory interval. On restart, begin the next reservation at the persisted `reserved_through`. This may leave a gap but cannot reuse an issued or merely reserved ID. The same trusted namespaces continue to work with PostgreSQL offline.

Counter exhaustion is terminal for that allocator: return a typed allocation-overflow error and never wrap or silently mint a replacement namespace. It makes new mutations unavailable through metadata authority. A fresh namespace is only a recovery action for untrustworthy local allocator state, not a way to extend a full 39-bit counter.

## Bootstrap, Recovery, and Offline Behavior

At a new PostgreSQL-backed local root, namespace bootstrap must first run one PostgreSQL transaction that reads and locks the per-volume legacy `next_inode` counter. If `next_inode` exists, the volume is legacy: do not create or increment `next_allocator_namespace`, do not mint namespaces, log `id_allocator_legacy_migration_required`, and leave both metadata-authority headroom flags false. This is a read-only compatibility guard, not a migration. Only after confirming that `next_inode` is absent may the transaction create or lock `next_allocator_namespace`, mint two namespaces, and commit. One redb transaction then stores the paired state before either allocator issues an ID. If PostgreSQL fails before the paired state commits, discard any minted namespace and retry later; it remains permanently unused. The product UX guarantees a local metadata root belongs to exactly one computer-volume pair. A parallel clone must create new local state and therefore gets new namespaces.

If either durable allocator record is untrustworthy, do not use either old namespace or counter. During `PgMetaEngine::open_with_options`, first apply the legacy guard above; only a volume without `next_inode` may mint two fresh namespaces and replace both records atomically. If PostgreSQL is unavailable, open the known local metadata view for reads but set both `MetadataAuthorityStatus` headroom flags to false. Existing readable files remain readable; all new write-capable opens, writes, and namespace mutations fail through the existing `MetaError::AccessDenied { actual: ReadOnly }` path. A mounted process remains read-only: it performs no background allocator retry. After PostgreSQL returns, close and reopen the metadata engine; that next `open_with_options` call retries recovery and restores writable authority only after the paired durable record commits.

The PostgreSQL registry counter is monotonic metadata, not reconstructible cache. A physical or WAL restore is permitted only when it preserves the counter high-water mark. Before a point-in-time or logical restore that can rewind `next_allocator_namespace`, the operator must assign the restored data a new volume identity and provision new local roots; prior roots must not reconnect to it. This is an operational safety rule, not an automatic recovery path.

The operator-visible surface is deliberately small and typed at the metadata boundary. There is no existing structured metadata diagnostics/status output that can carry an allocator reason, so emit one `tracing` event named `id_allocator_recovery_required` for each failed bootstrap or recovery attempt, `id_allocator_legacy_migration_required` when the bootstrap guard finds `next_inode`, and `id_allocator_counter_exhausted` when either allocator reaches terminal exhaustion. Include volume ID, affected allocator state (`new`, `untrustworthy`, `legacy_migration_required`, or `counter_exhausted`), and the PostgreSQL error when one exists. `MetadataAuthorityStatus.inode_id_headroom` and `.object_id_headroom` remain the only status fields and expose availability, not the reason. Do not add bridge-specific errno mappings or a new diagnostics service: the shared VFS read-only response is the contract.

## Plan of Work

First add `crates/studiofs-meta/src/allocator_state.rs`, a small concrete shared allocator-state value with checked `encode`, namespace/counter validation, and the pure high-water reservation transition. Add red unit tests in the same module for encoding boundaries, separate namespace identity, crash-tail skipping, the `reserved_through == 1 << 39` endpoint, and overflow before connecting either backend.

Then replace `InodeWindow` reservation in `crates/studiofs-meta/src/pg_engine/inode_allocator.rs`. Keep PostgreSQL's `counters` table and the existing `next_client_slot` counter plus the local `pgClientSlotPlusOne` record: they continue to supply the stable `client-slot:<slot>` replication identity. Before any namespace mint, atomically reject a volume that has the legacy `next_inode` counter; it must remain read-only with `id_allocator_legacy_migration_required`. For a volume without that counter, introduce a separate per-volume `next_allocator_namespace` counter and mint two namespaces during bootstrap or paired-state recovery. Remove only `next_inode` range allocation for a fresh volume; do not add a migration or silently reuse its value.

Add `pub(crate)` read/replace helpers for the paired `AllocatorStateV1` to the local redb metadata layer. Add `MetaInodes::next_inode_ids(count: u32) -> MetaResult<Vec<Ino>>` in `crates/studiofs-meta/src/engine.rs`, with a default implementation that repeatedly calls `next_inode`. Override it in `PgMetaEngine` so one reservation comes from the inode record; implement the direct-redb version with the current single-transaction batch behavior so its local-only semantics and performance remain unchanged. `PgMetaEngine::next_inode` and `next_object_ids` in `crates/studiofs-meta/src/pg_engine/local_traits.rs` must use their respective inode and payload records through `self.local`. Change `StudioVfs::reserve_namespace_ino` in `crates/studiofs-vfs/src/vfs/namespace_ops.rs` to call `next_inode_ids(NAMESPACE_INODE_BATCH)`. Preserve `next_inode`, `next_object_ids`, and the contiguous `Vec<u64>` payload-object result so bridges need no allocator API change.

In `crates/studiofs-meta/src/pg_engine.rs`, replace the single shared headroom atomic with separate `inode_id_headroom_available` and `object_id_headroom_available` fields. Wire those values independently through `PgMetaEngine::metadata_authority_status` in `crates/studiofs-meta/src/pg_engine/replication_api.rs`. In `PgMetaEngine::open_with_options`, attempt paired-state bootstrap or recovery once, set both flags false when PostgreSQL is unavailable, and do not retry from the background synchronization loop. Reuse `StudioVfs::enforce_metadata_mutation_authority`; no macFUSE, Linux FUSE, or WinFSP allocator branch is permitted. Update `docs/juicefs-local-first-rust-contract.md` to say PostgreSQL mints allocator namespaces rather than ID ranges, and update the corresponding row in `docs/local-first-validation-matrix.md`.

## Milestones

### Milestone 1: Define and prove the local allocator state

Create `allocator_state.rs` and its unit tests before changing either engine. At the end of this milestone, the test suite proves that namespace zero and all overflowing values are rejected, that the highest ordinary ID encodes correctly, and that a final reservation ending at `1 << 39` is trusted but cannot allocate again. Run the first command in Concrete Steps and expect every `sfs623_allocator_state_` test to pass.

### Milestone 2: Move PostgreSQL-backed allocation off ranges

Add PostgreSQL namespace minting and the atomic paired redb state. Keep client-slot persistence and self-batch detection intact while routing `PgMetaEngine` inode and object allocation through the two records. Before minting, prove the fresh-volume guard rejects a legacy `next_inode` row without creating `next_allocator_namespace`. At the end of this milestone, the focused PostgreSQL tests prove two fresh roots receive four namespaces, restarted allocation skips the reserved tail, neither ID class collides with the other, and a legacy volume is read-only. Run the PostgreSQL command in Concrete Steps with the repository fixture and expect `test result: ok`.

### Milestone 3: Make degraded recovery observable through existing VFS behavior

Make `open_with_options` use trusted local state offline, or make it read-only if the paired state is missing, untrustworthy, or legacy. A process opened read-only stays that way until it is closed; reopening after PostgreSQL returns retries only a non-legacy recovery. Route the VFS namespace-inode batch through `next_inode_ids`. At the end of this milestone, the metadata and VFS tests show that a committed read succeeds while a mutation returns read-only, then a reopened fresh engine becomes writable only after the paired state commits; they also show file and directory creates consume inode headroom rather than payload headroom.

### Milestone 4: Update the contract and prove platform neutrality

Update the local-first contract and validation matrix, then run the shared VFS check and each available platform command. The feature is complete only when all changed-ID behavior stays behind metadata and the bridge-specific unit or smoke result is recorded for its host.

## Concrete Steps

From the repository root, perform each milestone in order. The commands are safe to rerun; each focused test must end with `test result: ok` and no failing tests.

    cargo test -p studiofs-meta sfs623_allocator_state_ -- --nocapture
    cargo test -p studiofs-meta sfs623_pg_allocator_ -- --ignored --nocapture --test-threads=1
    cargo test -p studiofs-meta sfs623_recovery_ -- --nocapture
    cargo test -p studiofs-vfs sfs623_read_only_allocator_ -- --nocapture
    RUSTFLAGS='-D warnings' cargo check -p studiofs-meta -p studiofs-vfs

Run the PostgreSQL command only with the repository fixture configured. It must show the focused tests passing; a missing fixture is a blocked integration check, not a reason to weaken the unit or VFS tests.

On macOS with the macFUSE headers installed, run:

    cargo test -p studiofs-macfuse --lib -- --nocapture

On Linux with libfuse3 development files, run:

    cargo test -p studiofs-linux-fuse --lib -- --nocapture

On a Windows host with WinFSP installed, run:

    cargo test -p studiofs-winfsp --lib -- --nocapture

For a Linux host with libfuse3, `/dev/fuse`, and a safe disposable mount point, run:

    STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-linux-fuse mounted_ -- --ignored --nocapture --test-threads=1

`studiofs-macfuse` currently has no `mount_sanity` integration target. Its macOS unit command above is the applicable check for this metadata-only change; record that no macFUSE smoke target exists rather than substituting the Linux command.

Windows PowerShell uses:

    $env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS = '1'
    $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT = 'Z:'
    cargo test -p studiofs-winfsp --test mount_sanity -- --ignored --nocapture --test-threads=1

Record `test result: ok` for each command that can run. If the host lacks macFUSE headers, libfuse3, a WinFSP installation, or the required kernel driver, record the missing prerequisite and leave that platform check pending; do not treat another platform's result as its substitute.

## Validation and Acceptance

Write each test before its production change. Place allocator unit tests beside the new state module and backend recovery tests in `crates/studiofs-meta/src/pg_engine/tests/inode_allocator.rs` or its renamed focused replacement. The required behaviors are:

- the lowest and highest valid namespace/counter encode correctly; namespace zero and each overflow are rejected;
- a PostgreSQL volume with any legacy `next_inode` counter never mints `next_allocator_namespace`, becomes read-only, and emits the legacy-migration event;
- the exclusive `reserved_through == 1 << 39` endpoint is trusted, while the next request returns typed allocation overflow;
- equal counters under the inode and payload namespaces produce different IDs;
- a restart after a durable high-water advance skips the unconsumed tail and keeps the namespace;
- two local roots in the same volume mint four distinct allocator namespaces;
- malformed or incomplete paired state mints both fresh namespaces online and becomes read-only offline;
- the read-only engine remains read-only until it is closed, while reopening after PostgreSQL returns commits paired recovery and restores authority;
- a trusted paired state continues allocating each identity class while PostgreSQL is offline;
- the stable PostgreSQL client slot survives the allocator migration and self-batch detection still recognizes `client-slot:<slot>`;
- `reserve_namespace_ino` refills its batch through `next_inode_ids`, so mounted create and mkdir consume only the inode namespace and report inode, not payload, headroom;
- a direct redb engine still allocates with its existing `NEXT_INODE` behavior;
- local-counter exhaustion and namespace exhaustion fail without wrapping or reusing a value;
- an unavailable allocator makes VFS reject mutations while a previously committed read still succeeds.
- a database restore either preserves `next_allocator_namespace` high-water or uses a new volume identity and new local roots.

Run the commands in Concrete Steps. The PostgreSQL-backed two-root namespace test must prove that namespace minting, not range reservation, is the sole PostgreSQL ID operation. The recovery test must prove both the immutable read-only mounted state and successful recovery after reopen.

## Idempotence and Recovery

All allocation transitions are append-only in meaning: unused reserved ranges and namespaces are safe gaps. A failed PostgreSQL mint can be retried, and a crash between PostgreSQL mint and redb commit only leaks the minted namespace. Never delete or decrement a PostgreSQL namespace counter to reclaim it. Test fixtures must use new local roots; no migration of an existing root is allowed.

## Artifacts and Notes

Keep the focused command output in the task or pull request. A successful allocator-state proof includes a short transcript such as:

    running 6 tests
    test result: ok. 6 passed; 0 failed

For an unavailable platform check, record the exact host and prerequisite, for example `macOS runner: macFUSE headers unavailable; studiofs-macfuse unit check pending`. Do not attach mount artifacts from a non-disposable user mount.

## Interfaces and Dependencies

Keep `MetaInodes::next_inode() -> MetaResult<Ino>` and `MetaPayloadObjects::next_object_ids(count: u32) -> MetaResult<Vec<u64>>` unchanged. Add `MetaInodes::next_inode_ids(count: u32) -> MetaResult<Vec<Ino>>` only for VFS namespace batching; it is a metadata capability, not a new bridge API. The implementation may add private checked helpers and a private paired `AllocatorStateV1`, but must not add configuration or a new dependency. PostgreSQL remains only the namespace authority; redb remains the local durable allocator state.

## Outcomes & Retrospective

As of 2026-07-30, no allocator implementation has started. The plan now separates stable replication client identity from ID allocation, blocks legacy PostgreSQL volumes instead of risking namespace collisions, routes namespace-create batches through the inode allocator, scopes the namespace protocol to PostgreSQL-backed volumes, defines the exclusive high-water endpoint, makes recovery explicitly remount-based, and treats counter-rewinding database restores as a new-volume operation. Implementation remains to be completed and must update this section with the observed test output, unavailable platform checks, and any deviation from the stated recovery behavior.

Revision note (2026-07-30): created from SFS-623 after graph and local-first-contract validation. Revised after review to retain replication client slots, preserve direct-redb allocation, guard legacy PostgreSQL volumes, route namespace batches to the inode allocator, make high-water bounds and remount recovery explicit, resolve diagnostics, and add mandatory ExecPlan milestones, steps, artifacts, and platform proof commands; no implementation has started.

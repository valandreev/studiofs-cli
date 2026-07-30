# Replace PostgreSQL ID ranges with collision-free local allocators

This ExecPlan is a living document. Maintain it under `.agent/PLANS.md`.

## Purpose / Big Picture

StudioFS will allocate inode IDs and immutable payload-object IDs locally without a PostgreSQL range reservation on the foreground path. Every newly allocated ordinary ID is unique within its volume, including after a crash or local metadata repair. PostgreSQL only mints a namespace when a local allocator is first created or its durable state cannot be trusted. A mount with trusted allocator state continues while PostgreSQL is offline; one that cannot safely allocate remains readable and rejects new mutations through the existing metadata-authority gate.

## Progress

- [x] (2026-07-30) Validated the current shared inode/object window, the local redb counter, recovery path, and VFS read-only gate.
- [x] (2026-07-30) Resolved the identity encoding, persistence, recovery, overflow, and operator-visible failure rules.
- [ ] Implement the allocator and replace the current PostgreSQL windows and shared redb counter.
- [ ] Add the specified focused tests and update the local-first contract and validation matrix.

## Surprises & Discoveries

- Observation: PostgreSQL `next_inode()` and `next_object_ids()` currently call the same `allocate_inode_object_ids`; the redb backend also uses `NEXT_INODE` for both.
  Evidence: `crates/studiofs-meta/src/pg_engine/local_traits.rs` and `crates/studiofs-meta/src/redb_engine/payload_objects.rs`.
- Observation: StudioFS already enforces a read-only metadata authority for all new mutations while preserving known reads.
  Evidence: `StudioVfs::enforce_metadata_mutation_authority` and `read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open`.

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

## Context and Orientation

`crates/studiofs-meta` owns all metadata identities. `Ino` and `PayloadRef.object_id` are `u64` values carried by VFS and the platform bridges. `PgMetaEngine` currently reserves PostgreSQL windows in `crates/studiofs-meta/src/pg_engine/inode_allocator.rs`; `RedbMetaEngine` advances the local `NEXT_INODE` counter for both identity classes. `crates/studiofs-vfs` must keep treating metadata as the authority, but should not know how an ID is encoded.

An allocator namespace is a PostgreSQL-minted, volume-scoped number assigned to exactly one local allocator. A local allocator is the durable state for one namespace and one monotonic counter. An ordinary ID is an inode ID or a payload-object ID; a logical chunk remains identified by `(inode, chunk_index)` and receives no new ID.

## Required Identity Contract

All ordinary IDs are positive signed-63-bit values encoded as:

    ID = (namespace << 39) | local_counter

`namespace` occupies the high 24 bits and must be in `1..=0x00ff_ffff`. `local_counter` occupies the low 39 bits and must be in `0..=(1 << 39) - 1`. Therefore every ordinary ID is at most `i64::MAX`. Namespace `0` is reserved for system IDs; existing system IDs, including the root inode, remain system IDs. The allocator must reject invalid decoded state and never create an ordinary ID with namespace `0`.

PostgreSQL mints namespaces from one per-volume monotonic counter, seeded at `1`. The allocation transaction increments that counter and returns its previous value. It rejects a value above `0x00ff_ffff`; it never wraps, decrements, or reuses a namespace. The inode allocator and payload-object allocator each mint their own namespace, so equal local counters still produce unequal numeric IDs.

No timestamp, UUID, content hash, deduplication key, client slot, or logical-chunk number participates in identity. Counter gaps are valid and must not be compacted.

## Durable Local State and Allocation

Store one versioned `AllocatorStateV1` value in the local redb metadata transaction. It contains both records atomically:

    inode:   { namespace, reserved_through }
    payload: { namespace, reserved_through }

`reserved_through` is the exclusive local-counter high-water mark. Each record is valid only when its namespace and high-water mark are in the bounds above. The single record must decode completely, have version `1`, and contain both allocator records. A missing record is expected only for a newly created local metadata root; a missing, malformed, partially decoded, or out-of-range record in an existing root is untrustworthy.

For an allocator with in-memory interval `[next, end)`, reserve `max(LOCAL_ID_BATCH_SIZE, requested_count)` counters when the interval cannot satisfy a request. In one durable local transaction, check `reserved_through + reservation_size <= 1 << 39`, update `reserved_through`, commit, then expose `[old_reserved_through, new_reserved_through)` to memory. `LOCAL_ID_BATCH_SIZE` is a private constant, initially `4096`, not user configuration. A request may reserve a larger interval. Return a contiguous sequence for a multi-object request.

After an ordinary crash, discard any in-memory interval. On restart, begin the next reservation at the persisted `reserved_through`. This may leave a gap but cannot reuse an issued or merely reserved ID. The same trusted namespaces continue to work with PostgreSQL offline.

Counter exhaustion is terminal for that allocator: return a typed allocation-overflow error and never wrap or silently mint a replacement namespace. It makes new mutations unavailable through metadata authority. A fresh namespace is only a recovery action for untrustworthy local allocator state, not a way to extend a full 39-bit counter.

## Bootstrap, Recovery, and Offline Behavior

At a new local root, PostgreSQL must mint two namespaces, then one redb transaction stores the paired state before either allocator issues an ID. If PostgreSQL fails before the paired state commits, discard any minted namespace and retry later; it remains permanently unused. The product UX guarantees a local metadata root belongs to exactly one computer-volume pair. A parallel clone must create new local state and therefore gets new namespaces.

If either durable allocator record is untrustworthy, do not use either old namespace or counter. PostgreSQL must mint two fresh namespaces and replace both records atomically. If PostgreSQL is unavailable, open the known local metadata view for reads but set both `MetadataAuthorityStatus` headroom flags to false. Existing readable files remain readable; all new write-capable opens, writes, and namespace mutations fail through the existing `MetaError::AccessDenied { actual: ReadOnly }` path. A successful later recovery may restore writable authority only after the paired durable record commits.

The operator-visible surface is deliberately small and typed at the metadata boundary: log `id_allocator_recovery_required` once per transition with volume ID, affected allocator state (`new`, `untrustworthy`, or `counter_exhausted`), and PostgreSQL error if one exists. Expose the resulting availability through the existing `MetadataAuthorityStatus` inode and object headroom fields. Do not add bridge-specific errno mappings: their current shared VFS read-only response is the contract. Add a reason field to the metadata diagnostics/status output if one already exists; do not create a separate status service for this allocator.

## Plan of Work

First add a small shared allocator state implementation in `crates/studiofs-meta/src/` with checked `encode`, checked namespace/counter validation, and the pure high-water reservation transition. Keep it a concrete value type, not a trait hierarchy. Add red tests for encoding boundaries, separate namespace identity, crash-tail skipping, and overflow before connecting either backend.

Then replace `PgMetaEngine` client slots and `InodeWindow` reservation in `crates/studiofs-meta/src/pg_engine/inode_allocator.rs`. Keep PostgreSQL's `counters` table, but replace `next_client_slot` and `next_inode` range allocation with a single per-volume `next_allocator_namespace` counter. Mint two namespaces during bootstrap or paired-state recovery. Do not retain compatibility logic or old range counters: migration is explicitly out of scope and test volumes are recreated.

Update `RedbMetaEngine::next_inode` in `crates/studiofs-meta/src/redb_engine/inodes.rs` and `RedbMetaEngine::next_object_ids` in `crates/studiofs-meta/src/redb_engine/payload_objects.rs` to use the appropriate record in `AllocatorStateV1`. Update `PgMetaEngine::next_inode` and `next_object_ids` in `crates/studiofs-meta/src/pg_engine/local_traits.rs` to do the same. Preserve the existing trait signatures and the contiguous `Vec<u64>` object-ID result so VFS, cache, and bridges need no allocator API change.

Wire the two independent availability values through `PgMetaEngine::metadata_authority_status` in `crates/studiofs-meta/src/pg_engine/replication_api.rs`. Reuse `StudioVfs::enforce_metadata_mutation_authority`; no macFUSE, Linux FUSE, or WinFSP allocator branch is permitted. Update `docs/juicefs-local-first-rust-contract.md` to say PostgreSQL mints allocator namespaces rather than ID ranges, and update the corresponding row in `docs/local-first-validation-matrix.md`.

## Validation and Acceptance

Write each test before its production change. Place allocator unit tests beside the new state module and backend recovery tests in `crates/studiofs-meta/src/pg_engine/tests/inode_allocator.rs` or its renamed focused replacement. The required behaviors are:

- the lowest and highest valid namespace/counter encode correctly; namespace zero and each overflow are rejected;
- equal counters under the inode and payload namespaces produce different IDs;
- a restart after a durable high-water advance skips the unconsumed tail and keeps the namespace;
- two local roots in the same volume mint four distinct allocator namespaces;
- malformed or incomplete paired state mints both fresh namespaces online and becomes read-only offline;
- a trusted paired state continues allocating each identity class while PostgreSQL is offline;
- local-counter exhaustion and namespace exhaustion fail without wrapping or reusing a value;
- an unavailable allocator makes VFS reject mutations while a previously committed read still succeeds.

Run, from the repository root:

    cargo test -p studiofs-meta id_allocator -- --nocapture
    cargo test -p studiofs-meta allocator_state -- --nocapture
    cargo test -p studiofs-vfs read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open -- --nocapture
    RUSTFLAGS='-D warnings' cargo check -p studiofs-meta -p studiofs-vfs

Run the PostgreSQL-backed two-root namespace test only when the repository's PostgreSQL test fixture is available; it must prove that namespace minting, not range reservation, is the sole PostgreSQL ID operation. Finally run the relevant macFUSE, Linux FUSE, and WinFSP compile or platform smoke checks available in the environment, reporting unavailable platform toolchains rather than weakening the shared VFS proof.

## Idempotence and Recovery

All allocation transitions are append-only in meaning: unused reserved ranges and namespaces are safe gaps. A failed PostgreSQL mint can be retried, and a crash between PostgreSQL mint and redb commit only leaks the minted namespace. Never delete or decrement a PostgreSQL namespace counter to reclaim it. Test fixtures must use new local roots; no migration of an existing root is allowed.

## Interfaces and Dependencies

Keep `MetaInodes::next_inode() -> MetaResult<Ino>` and `MetaPayloadObjects::next_object_ids(count: u32) -> MetaResult<Vec<u64>>` unchanged. The implementation may add private checked helpers and a private paired `AllocatorStateV1`, but must not add a VFS-facing allocator trait, a bridge API, configuration, or a new dependency. PostgreSQL remains only the namespace authority; redb remains the local durable allocator state.

Revision note (2026-07-30): created from SFS-623 after graph and local-first-contract validation; no implementation has started.

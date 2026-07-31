# Replace PostgreSQL ID windows with allocator namespaces

This ExecPlan is a living document. It must be maintained under
`.agent/PLANS.md` while the work proceeds.

## Purpose / Big Picture

A PostgreSQL-backed StudioFS volume will give every local metadata root two
globally unique allocator namespaces: one for inode IDs and one for payload
object IDs. PostgreSQL assigns a namespace pair only when opening a root that
has no trustworthy durable pair; ordinary mounted allocation then advances the
durable local counters without PostgreSQL. Two fresh roots for one volume will
therefore produce four distinct namespaces, while a legacy range-based volume
will remain safely read-only rather than risk reusing an ID.

## Progress

- [x] (2026-07-31 19:41Z) Read SFS-632, its SFS-630 prerequisite, the allocator-namespace decision, the metadata-authority decision, and the current allocator implementation.
- [x] (2026-07-31 20:15Z) Added ignored two-root uniqueness and legacy-counter guard tests; the explicit fixture command now fails loudly when `STUDIOFS_TEST_PG_DSN` is absent rather than silently passing.
- [x] (2026-07-31 20:24Z) Replaced PostgreSQL range bootstrap and allocation with paired namespace state; separated inode and payload allocation and retained client slots for replication identity.
- [x] (2026-07-31 20:28Z) Updated the local-first contract and validation matrix; focused metadata/VFS tests, the full metadata library suite, formatting, and warnings-as-errors check pass.
- [x] (2026-08-01 00:24Z) Ran the historical SFS-623 filter `cargo test -p studiofs-meta sfs623_pg_allocator_ -- --ignored --nocapture --test-threads=1` from the repository `.env`; all three allocator acceptance tests passed.
- [x] (2026-08-01 00:31Z) Restored lifecycle, file-lock, pending-replay, and remote rename-overwrite regression coverage; the full `studiofs-meta` library suite passed with 359 tests and 67 ignored.
- [x] (2026-08-01) Moved durable allocator refill persistence outside the runtime mutex, added a concurrency regression test, and re-ran the metadata suite with 360 passed and 67 ignored.
- [x] (2026-08-01) Added the single-ID allocator fast path and made batch state advance only after ID encoding succeeds; the remount-only exhaustion contract remains unchanged.
- [x] (2026-08-01) Guarded the single-ID helper against multi-ID requests and expanded legacy-volume diagnostics with the migration contract reference.
- [x] (2026-08-01) Stopped fresh `studiofs init` volumes from seeding the legacy `next_inode` counter; dump/load acceptance now passes with namespace bootstrap enabled.

## Surprises & Discoveries

- Observation: PostgreSQL-backed inode and payload allocation now use separate local records inside one durable `AllocatorStateV1` pair.
  Evidence: `crates/studiofs-meta/src/pg_engine/inode_allocator.rs` reserves each record locally, while `MetaInodes::next_inode_ids` and `MetaPayloadObjects::next_object_ids` select their respective record.
- Observation: SFS-630 already provides a checked, atomically persisted `AllocatorStateV1` pair in redb.
  Evidence: `RedbMetaEngine::replace_allocator_state` commits the single encoded pair and then synchronizes local metadata.
- Observation: the ignored PostgreSQL acceptance uses the repository `.env` and now runs three checks.
  Evidence: the exact allocator command reported `3 passed; 0 failed`; `pg_dsn()` still fails loudly when the fixture is absent.

## Decision Log

- Decision: keep `next_client_slot` and `pgClientSlotPlusOne` exclusively for `client-slot:<slot>` replication identity.
  Rationale: SFS-632 explicitly preserves self-batch detection; it must not share the allocator namespace registry.
  Date/Author: 2026-07-31 / Codex
- Decision: namespace registration and recovery run only in `PgMetaEngine::open_with_options`; no background or foreground retry is added.
  Rationale: PostgreSQL must be a bootstrap authority, not a mounted-I/O dependency. A crash after its commit can leak a pair but cannot reuse it.
  Date/Author: 2026-07-31 / Codex
- Decision: retain direct redb allocation unchanged.
  Rationale: it has no PostgreSQL collision domain and is explicitly out of scope.
  Date/Author: 2026-07-31 / Codex

## Outcomes & Retrospective

The range-window implementation is removed from the PostgreSQL path. PostgreSQL
now atomically rejects a legacy `next_inode` counter or validates and commits
`next_allocator_namespace` by two, after which redb persists the complete pair
before allocation can proceed. The focused local proof includes the full
`studiofs-meta` library suite and the VFS namespace timing check. The live
PostgreSQL acceptance ran three tests against the repository `.env` and passed;
no bridge-specific allocation branch is required.

## Context and Orientation

`crates/studiofs-meta/src/allocator_state.rs` defines `AllocatorRecord` and
the paired `AllocatorStateV1`. Each record combines a non-zero 24-bit
namespace with a durable exclusive high-water mark for the 39-bit local
counter. `crates/studiofs-meta/src/redb_engine/allocator_state.rs` reads and
atomically replaces that pair in the local redb database.

`crates/studiofs-meta/src/pg_engine.rs` constructs a `PgMetaEngine` in
`open_with_options`. `crates/studiofs-meta/src/pg_engine/inode_allocator.rs`
registers a paired namespace in PostgreSQL when local redb has no trustworthy
state, then reserves local inode and payload counters from the corresponding
record. `MetaInodes` in `engine.rs` and the VFS namespace batch consumer use a
small inode-batch capability so the two record types remain independent.

## Plan of Work

First add the narrow integration tests in
`crates/studiofs-meta/src/pg_engine/tests/inode_allocator.rs`, using the
repository PostgreSQL fixture and two new local roots for one volume. The
tests will inspect allocated IDs or durable pair records to prove four unique
non-zero namespaces, and will exercise a local batch tagged with the persisted
client slot to prove self-batch detection remains true. A separate test will
seed legacy `(volume, 'next_inode')`, open a fresh root, and assert the open
does not mint `next_allocator_namespace` and reports no mutation authority.
Run the exact ignored filter and record the three green allocator acceptance
tests before changing the allocator path.

Replace the range-window state and its reservation/prefetch machinery in
`pg_engine/inode_allocator.rs` with one allocator lock protecting checked
local reservations in the durable `AllocatorStateV1` pair. Add one private
PostgreSQL transaction that first rejects a legacy `next_inode` row, creates
`next_allocator_namespace` at one when absent, advances it by two, validates
both returned values, and commits. After that commit, call the existing redb
atomic pair replacement. If PostgreSQL commit succeeds but redb commit does
not, leave the issued namespaces unused; never decrement or retry their
values. A missing, malformed, partial, duplicate, or out-of-range pair must
rotate both records together, never repair either in place.

In `open_with_options`, load a trusted pair before serving mutations. If it is
missing or untrustworthy, attempt the paired registration once. An unavailable
PostgreSQL server or legacy guard leaves both allocator-headroom fields false:
the local view remains readable but shared `MetadataAuthorityStatus` blocks
new mutations. Do not let the synchronization worker upgrade this status;
close and reopen is the recovery boundary. Emit the existing requested tracing
events at legacy/recovery/exhaustion points with volume and error context.

Give `MetaInodes` a default `next_inode_ids(count)` implementation that calls
`next_inode` repeatedly. Override it in `PgMetaEngine` to allocate from the
inode record in one durable local transition; retain the direct-redb behaviour
through the default implementation. Route the VFS namespace batch through it.
Keep `next_object_ids(count)` returning a contiguous `Vec<u64>`, but source it
only from the payload record. Preserve all bridge APIs and client-slot fields.

Finally update the PostgreSQL responsibility wording in
`docs/juicefs-local-first-rust-contract.md` and replace the range-window row
in `docs/local-first-validation-matrix.md` with namespace bootstrap, trusted
offline state, legacy read-only, and two-root uniqueness coverage.

## Concrete Steps

Run from `/Volumes/Storage/dev/studiofs`, one Cargo command at a time:

    cargo test -p studiofs-meta sfs623_pg_allocator_ -- --ignored --nocapture --test-threads=1  # historical SFS-623 acceptance filter
    cargo test -p studiofs-meta sfs623_allocator_state_ -- --nocapture
    cargo test -p studiofs-vfs read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open -- --nocapture
    RUSTFLAGS='-D warnings' cargo check -p studiofs-meta -p studiofs-vfs

The first command requires the repository PostgreSQL fixture from `.env`. Keep
the exact command and final test count in this document. If the fixture is
temporarily unavailable, record that blocker without treating zero executed
tests as a successful acceptance result.

## Validation and Acceptance

The integration test must demonstrate two fresh roots of one PostgreSQL volume
have four pairwise-distinct namespaces. It must also prove that a legacy
`next_inode` row neither mints a namespace nor allows mutation. The persisted
client slot must still identify self-authored replication batches. A trusted
pair must allocate inode and payload IDs independently while PostgreSQL is
offline; an untrustworthy pair must remain read-only until a later reopen can
commit a complete new pair. Crashes between PostgreSQL registration and redb
persistence may create gaps only.

The shared VFS test remains platform-neutral proof of read-only mutation
blocking. This metadata-boundary change does not justify a macFUSE, Linux FUSE,
or WinFSP allocation branch. Run platform tests only if their local
prerequisites are available and record a missing prerequisite honestly.

## Idempotence and Recovery

All registry transitions are monotonic. Retrying an interrupted bootstrap is
safe because a leaked namespace pair is never reused. Never delete or decrease
`next_allocator_namespace`. A database restore is safe only when it preserves
that high-water mark; otherwise it requires a new volume identity and new
local roots. Test fixtures use disposable new roots and can be rerun safely.

## Artifacts and Notes

Expected focused success includes `test result: ok`. The allocator acceptance
result is `3 passed, 0 failed`; the full metadata library result is
`359 passed, 67 ignored`. No migration is created: legacy range volumes stay
unavailable for mutation by design.

## Interfaces and Dependencies

`MetaInodes::next_inode() -> MetaResult<Ino>` and
`MetaPayloadObjects::next_object_ids(u32) -> MetaResult<Vec<u64>>` stay stable.
Add only `MetaInodes::next_inode_ids(u32) -> MetaResult<Vec<Ino>>` with a
default implementation. PostgreSQL remains the sole cross-root namespace
authority; redb remains the sole local durable counter-state authority. No new
dependency, configuration option, migration, or bridge interface is needed.

Revision note (2026-08-01): created for SFS-632 after inspecting SFS-630/623/625/628 contracts and the current PostgreSQL range allocator. Updated after implementation and review fixes to record paired namespace validation, restored regression coverage, and the green `.env`-backed acceptance result.

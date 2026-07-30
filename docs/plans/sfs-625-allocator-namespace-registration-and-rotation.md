# Allocator namespace registration and rotation guarantees

## Decision

For a PostgreSQL-backed volume, `counters` owns exactly one registry row:
`(vol, 'next_allocator_namespace')`. Its `BIGINT value` is the next unissued
namespace, starts at `1`, and is never decremented, wrapped, or reused.
Namespace `0` is reserved for system IDs; a value above `0x00ff_ffff` is a
terminal registration failure.

Registration and rotation run only from `PgMetaEngine::open_with_options`.
They never run from a foreground allocation or the synchronization worker.
PostgreSQL is therefore needed only before the local allocator becomes
writable, not for ordinary local counter reservations.

## Atomic registry operation

For a fresh root or an untrustworthy paired local state, one PostgreSQL
transaction must:

1. Reject the volume when `counters` already contains `next_inode`; that is a
   legacy volume and migration is out of scope.
2. Create `next_allocator_namespace` at `1` if absent, then atomically advance
   it by two and return the two prior consecutive values.
3. Reject either returned value outside `1..=0x00ff_ffff`, then commit.

The successful PostgreSQL transaction is followed by one redb transaction that
persists both `{ namespace, reserved_through }` records. A crash after the
PostgreSQL commit but before redb commit leaks the pair; it cannot reuse it.

## Trust, rotation, and writers

The paired redb record is trusted only when it decodes completely as the
current version, has two valid non-zero namespaces, and both exclusive
high-water marks are in range. A trusted pair keeps allocating offline. A
missing, partial, malformed, or out-of-range pair rotates both namespaces as
one unit when PostgreSQL is available; it never repairs one side in place.

One `PgMetaEngine` is the sole writer for one local metadata root. Its
allocator lock serializes local reservations; PostgreSQL serializes namespace
registration across distinct roots for the same volume. A copied root is not
a second writer: it must be provisioned as a new local root and receive a new
pair. A process with an untrustworthy pair and unavailable PostgreSQL remains
readable but is read-only until it is closed and reopened after recovery.

## Database restore

`next_allocator_namespace` is non-reconstructible monotonic metadata. A
physical/WAL restore is safe only when it retains its high-water mark. A
point-in-time or logical restore that can move the counter backward must use a
new volume identity and new local roots; old roots must not reconnect to that
restored volume. This avoids assigning a namespace already issued before the
restore.

## Boundaries

This contract does not migrate legacy `next_inode` volumes, rotate a full
39-bit local counter, or add bridge-specific allocation policy. It refines the
implementation plan in `sfs-623-collision-free-local-id-allocation.md`.

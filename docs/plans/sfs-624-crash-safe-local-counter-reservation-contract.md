# Crash-safe local counter reservation contract

## Decision

A PostgreSQL-backed local metadata root persists exactly one versioned paired
allocator value:

```text
AllocatorStateV1 {
    inode:   { namespace, reserved_through },
    payload: { namespace, reserved_through },
}
```

`namespace` is the non-zero 24-bit namespace already registered for that
allocator. `reserved_through` is the exclusive 39-bit local-counter
high-water mark and may equal `1 << 39`. No in-memory cursor, reservation
start, or prefetch state is durable. Those values can be reconstructed only
by reserving a new interval, so persisting them cannot improve crash safety.

The entire value is decoded and validated as one unit. It is trustworthy only
when its version, both namespaces, and both high-water marks are valid. A
missing value is valid only while bootstrapping a new local root; a partial,
malformed, undecodable, or out-of-range value is untrustworthy.

## Reservation boundary

Each allocator keeps an in-memory interval `[next, end)`. When that interval
cannot satisfy a request for `count` IDs, the allocator lock serializes this
transition:

1. Read the paired durable value and select the requested allocator record.
2. Calculate `width = max(4096, count)` and reject
   `reserved_through + width > 1 << 39` with the typed allocation-overflow
   error.
3. In one redb write transaction, replace the paired value with the selected
   record's `reserved_through` advanced by `width`, leaving the other record
   unchanged; commit it.
4. Only after that commit, install `[old_reserved_through, new_reserved_through)`
   in memory and expose the requested contiguous IDs.

Requests wholly contained in an installed interval allocate only from memory.
They remain serialized by the same lock and never need PostgreSQL. Every
issued ID is `(namespace << 39) | local_counter`; the inode and payload
records have different namespaces, so equal counters cannot collide.

## Crash and recovery rules

A crash before the redb commit exposes no interval. A crash after the commit
may leave an unused tail, which is a permanent permitted gap: restart drops
all in-memory intervals and starts the next reservation at the persisted
exclusive high-water mark. It never reuses the tail or an issued ID.

A trusted pair continues allocating while PostgreSQL is unavailable. An
untrustworthy pair is never repaired one side at a time: PostgreSQL first
mints two new namespaces, then one redb transaction replaces the complete
pair. A crash between those transactions leaks unused namespaces but cannot
issue or reuse an ID. If PostgreSQL is unavailable for that recovery, the
known metadata view remains readable and existing metadata-authority headroom
gates reject mutations until a later reopen completes the replacement.

A full local counter is terminal for that allocator. It does not rotate to a
new namespace, wrap, decrement, or reuse an ID; it removes mutation authority
through the existing metadata boundary. Namespace rotation is only for a
missing or untrustworthy paired durable value.

## Boundaries and proof

This applies only to PostgreSQL-backed local metadata roots; direct redb keeps
its existing local-only counter. The namespace registration contract in
`sfs-625-allocator-namespace-registration-and-rotation.md` supplies the
non-reuse guarantee, including legacy-volume and database-restore rules.

The implementation proof covers encoding limits, a final valid exclusive
high-water mark, counter overflow, equal counters under distinct namespaces,
crash-tail skipping, trusted offline continuation, and online/offline paired
state recovery. The full implementation sequence remains in
`sfs-623-collision-free-local-id-allocation.md`.

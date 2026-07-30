# Composite ID encoding and storage-key contract

## Identity encoding

An ordinary PostgreSQL-backed inode or payload-object ID is a positive signed
63-bit integer:

```text
id = (namespace << 39) | local_counter
```

`namespace` is a non-zero 24-bit value in `1..=0x00ff_ffff` and
`local_counter` is in `0..=(1 << 39) - 1`. The maximum encoded value is
`i64::MAX`. Namespace zero remains reserved for existing system IDs, including
the root inode; no ordinary allocator can issue it. Decode and bound checks
belong at the allocator-state boundary, not in every metadata or bridge caller.

`Ino`, `PayloadRef.object_id`, and the existing metadata traits remain `u64`.
PostgreSQL keeps signed `BIGINT`: every ordinary encoded value is representable
without a schema migration. The contract does not add a public newtype, tagged
ID, UUID, timestamp, or a global-order guarantee. Numeric sorting remains a
deterministic local ordering only; it is not creation order across allocators.

## Object keys and local cache paths

The payload object file name and remote object-ID component remain exactly 16
lower-case hexadecimal digits. Existing parsers therefore keep accepting the
same textual ID representation, and cache index keys stay `u64`.

The remote S3 shard must no longer use the first four digits of the composite
ID: those digits are namespace-dominated and would concentrate every allocator
into one prefix. Use the low 16 bits of the object ID, formatted as four
lower-case hexadecimal digits, as the shard directory while keeping the full
16-digit object ID as the final component. Parsing continues to recover the ID
from that final component and does not treat the shard as identity.

The local payload layout already derives its two directory bytes by applying
`splitmix64` to the full `u64` object ID. Retain that behavior; it spreads
sequential counters and needs no format change.

## Boundaries and proof

Direct redb keeps its current local-only allocation values and uses the same
16-digit payload-key representation. Migration is out of scope, so test roots
and non-production object prefixes may be recreated for the new remote shard
layout.

The implementation must prove the lowest and highest valid encoded IDs,
namespace and counter overflow rejection, reserved system namespace zero,
positive `BIGINT` compatibility, unchanged local cache round trips, remote key
parse round trips, and distribution of successive composite payload IDs across
remote shard prefixes. The durable allocation and namespace rules are defined
by SFS-624 and SFS-625.

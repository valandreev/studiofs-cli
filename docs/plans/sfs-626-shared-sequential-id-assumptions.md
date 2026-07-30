# Audit of shared sequential ID assumptions

## Findings

| Assumption | Current evidence | Decision |
| --- | --- | --- |
| Inodes and payload objects share one stream | `PgMetaEngine::allocate_inode_object_ids` serves both `next_inode` and `next_object_ids`; direct redb also stores both in `NEXT_INODE`. | Remove for PostgreSQL-backed volumes. Keep direct redb unchanged. Route VFS namespace batches to a new inode batch method. |
| Batches are dense and contiguous | Both current allocators return `first..first + count`; sparse counter tests already show the metadata graph does not require global density. | Keep each individual returned batch contiguous; permit reserved-tail gaps across crash/restart. |
| PostgreSQL values are signed `BIGINT` | The registry and legacy range counter use `BIGINT`; the old window code already rejects values above `i64::MAX`. | Keep the storage type. Composite ordinary IDs are positive 63-bit values, so no schema change is needed. |
| Payload paths are 16 hexadecimal characters | `format_object_key` and `LocalPayloadLayout` format and parse fixed-width 16-hex `u64` object IDs. | Preserve the format and parsing contract. Namespace encoding changes the number, not its textual width. |
| Numeric ordering is allocation order | `BTree*`, `sort_unstable`, and upload tie-breaks use IDs for deterministic ordering; metadata and chunk semantics key by identity, not by insertion sequence. | Treat global allocation order as incidental. Preserve deterministic sorting where it already exists. |
| PostgreSQL headroom windows control authority | `InodeWindow` persistence, prefetch, warnings, and `MetadataAuthorityStatus` are tied to the old range mechanism. | Replace only the PostgreSQL window meaning with independent local allocator headroom. Reuse the existing VFS read-only gate. |

## Consequences

No caller requires a globally dense stream or a numeric ID to encode creation
time. The two compatibility-sensitive surfaces are positive signed-63-bit
storage and fixed-width 16-hex payload keys. `format_object_key` shards by the
first four hex characters, but that is a storage-layout distribution heuristic,
not an identity or ordering invariant; SFS-627 owns any change to that policy.

The required implementation boundaries are therefore limited to
`studiofs-meta` allocator state and `StudioVfs` namespace-batch routing. The
platform bridges continue to consume `u64` identities through the existing
VFS interface.

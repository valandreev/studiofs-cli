# Data Layer

`studiofs-data` handles object I/O against S3-compatible storage and object encoding/decoding.

## Responsibilities

- PUT/GET/DELETE/HEAD object operations via `opendal`.
- Object key generation and sharding based on block IDs.
- Object payload validation (xxh64 checksums).
- Read/write connection profile separation (`IoProfile`).

## S3 Key Layout

Files in StudioFS are split into fixed-size blocks (default 8 MiB). Each block is assigned a globally unique, sequential 64-bit ID upon creation. 

Objects are routed to S3 using the following paths:

### Block Data Objects
```
<volume>/data/chunks/<hash>/<slice_id>_<block_index>_<block_size>
```
- `<hash>`: First layers of distribution derived from slice ID.
- `<slice_id>`: The sequential block ID.

### Generic Objects
```
<volume>/objects/<ab>/<cd>/<object_id>
```
- `<ab>/<cd>`: The first 4 hex characters of the object ID for S3 partition distribution.

## Object Format (Version 2)

Objects are immutable and content-addressable by their sequential ID, not content hash (deduplication is removed in the current VFS architecture).

Every object consists of a **fixed 32-byte header** followed by the raw payload.

### Header Layout
Little-endian encoding is used for all multi-byte fields.

```text
+------------------+
| Magic "STFS"     |  4 bytes
+------------------+
| Version          |  2 bytes (value: 2)
+------------------+
| Flags            |  2 bytes (ObjectFlags)
+------------------+
| Original Size    |  4 bytes (uncompressed size)
+------------------+
| Compressed Size  |  4 bytes (excluding header)
+------------------+
| Checksum         |  8 bytes (XXH64 of original data)
+------------------+
| Reserved         |  6 bytes (future use)
+------------------+
```

### Flags

| Flag | Value | Description |
|---|---|---|
| `LEGACY_LZ4_COMPRESSED` | `0x0001` | Object uses LZ4 compression. Rejected by current build. |
| `HAS_FOOTER` | `0x0002` | Object contains footer checksums (future use). |
| `ENCRYPTED` | `0x0004` | Object is encrypted (future use). |

The current build enforces that new objects are raw (uncompressed) data blocks to minimize CPU overhead. Legacy LZ4 compressed objects will return a `LegacyCompressionUnsupported` error if read.

## Storage I/O Profiles

To prevent background S3 uploads from overwhelming the connection pool and starving foreground reads, `studiofs-data` implements separate connection `IoProfile` pools via OpenDAL layers:

| Profile | Concurrency | `io_timeout` | Meta Timeout | Retries | Description |
|---------|-------------|--------------|--------------|---------|-------------|
| **Default** | 128 | 60s | 10s | 3 | Used for general-purpose/mixed I/O workloads. |
| **Read** | 512 | 30s | 3s | 0 | Dedicated pool for foreground fetches. Retries are handled by `FetchScheduler`. |
| **Write** | 256 | 90s | 10s | 0 | Dedicated pool for `UploadQueue`. Retries and backoff are handled dynamically. |

## Note on Metadata

Metadata is not stored in S3 manifests anymore.
Current metadata authority is PostgreSQL (`studiofs-meta` / `PgMetaEngine`).

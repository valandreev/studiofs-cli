# Data Layer

`studiofs-data` handles object I/O against S3-compatible storage and object encoding/decoding.

> Warning: this document still uses legacy `logical block` / `fragment` wording for the current storage path. For the frozen redesign vocabulary, see [chunk-slice-block-vocabulary.md](./chunk-slice-block-vocabulary.md).

## Responsibilities

- PUT/GET/DELETE/HEAD object operations via `opendal`.
- Object key generation and sharding based on block IDs.
- Object payload validation (xxh64 checksums).
- Read/write connection profile separation (`IoProfile`).
- Hybrid cloud layout for logical blocks: one 16 MiB logical block is stored as four 4 MiB fragment objects.

## S3 Key Layout

Files in StudioFS are split into fixed-size logical blocks (default 16 MiB). Each logical block is assigned a globally unique, sequential 64-bit object ID.

Remote storage uses a 4x4 hybrid scheme:

### Logical Object Base Key
```
<volume>/objects/<ab>/<cd>/<object_id>
```
- `<ab>/<cd>`: First 4 hex characters of object ID for S3 partition distribution.

### Cloud Fragment Keys (4 fragments per logical object)
```
<volume>/objects/<ab>/<cd>/<object_id>/<fragment_index>
```
- `fragment_index`: `0..3`
- Each fragment stores an independent STFS object payload (typically up to 4 MiB raw payload + 30-byte header).
- Fragment writes/retries are independent, so network failures only retry the affected piece.

Legacy `DataStore::put_block/get_block/...` APIs still use the historical:
```
<volume>/data/chunks/<hash>/<slice_id>_<block_index>_<block_size>
```
These are retained for compatibility/tests; VFS read/write path uses `<volume>/objects/.../<fragment_index>`.

## Object Format (Version 2)

Objects are immutable and content-addressable by their sequential ID, not content hash (deduplication is removed in the current VFS architecture).

Every object consists of a **fixed 30-byte wire-format header** followed by the raw payload.
In-memory Rust struct layout can be larger due to alignment, but serialized bytes on disk/S3 are exactly 30 bytes.

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

On write, object upload paths use `opendal::Buffer` (`header` + payload chunks) to avoid copying full fragment payload buffers when prepending the fixed header.

On read, object decode accepts `opendal::Buffer` directly, parses the fixed header from a small stack buffer, and validates checksum incrementally across buffer chunks. Payload is returned as zero-copy `Bytes` when the payload is a single contiguous chunk, with contiguous materialization only as a fallback for fragmented payloads.

Legacy helpers `create_object` / `create_object_with_flags` remain for compatibility and are marked deprecated in favor of `create_object_buffer` / `create_object_buffer_with_flags`.

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

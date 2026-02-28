# Data Layer

The data layer (`studiofs-data`) handles all S3 object I/O, the binary object format, and block addressing.

## DataStore

S3 I/O abstraction built on [OpenDAL](https://opendal.apache.org/) with layered middleware:

```
Application
  │
  ├─ Timeout layer (configurable per profile)
  ├─ Retry layer (exponential backoff)
  ├─ ConcurrentLimit layer
  └─ S3 service (OpenDAL)
```

### I/O Profiles

Different connection pools optimized for different workloads:

| Profile | Timeout | Concurrency | Retries | Use Case |
|---------|---------|-------------|---------|----------|
| `Default` | 60s | 128 | default | General operations |
| `Read` | 30s | 256 | 0 | Cache-miss fetches |
| `Write` | 90s | 256 | 1 | S3 uploads |

**Pool separation:** `clone_for_reads()` and `clone_for_writes()` create dedicated `DataStore` instances with appropriate profiles, ensuring fast reads are not blocked by slow uploads.

You can override profile settings via env vars (all optional):
- `STUDIOFS_S3_READ_META_TIMEOUT_SECS`
- `STUDIOFS_S3_READ_IO_TIMEOUT_SECS`
- `STUDIOFS_S3_READ_MAX_RETRIES`
- `STUDIOFS_S3_READ_CONCURRENT_LIMIT`
- `STUDIOFS_S3_WRITE_META_TIMEOUT_SECS`
- `STUDIOFS_S3_WRITE_IO_TIMEOUT_SECS`
- `STUDIOFS_S3_WRITE_MAX_RETRIES`
- `STUDIOFS_S3_WRITE_CONCURRENT_LIMIT`

### Operations

| Method | Description |
|--------|-------------|
| `put_object(object_id, data)` | Upload data as an immutable object |
| `get_object(object_id)` | Download full object |
| `delete_object(object_id)` | Delete object |
| `object_exists(object_id)` | HEAD check |
| `put_block(slice_id, block_index, data)` | Upload block (legacy) |
| `get_block(slice_id, block_index, block_size)` | Download block (legacy) |
| `get_block_range(...)` | Range read |
| `put_by_key(key, data)` | Raw S3 PUT |
| `get_by_key(key)` | Raw S3 GET |
| `delete_by_key(key)` | Raw S3 DELETE |

### S3 Key Layout

```
{volume}/
  ├── objects/{ab}/{cd}/{object_id}      # data objects
  ├── data/chunks/{hash}/{slice}_{idx}   # legacy blocks
  ├── metadata/manifest.json             # Tonbo manifest
  └── metadata/janitor.lock              # janitor lease
```

Object keys use 2-level directory sharding based on the object ID hex representation: first 2 hex chars → second 2 hex chars → full ID.

## Object Format (`object.rs`)

Immutable, content-addressable objects with integrity verification.

### Binary Layout

```
┌─────────────────────────────────────────────┐
│ Header (30 bytes)                           │
├──────────┬──────────────────────────────────┤
│ magic    │ "STFS" (4 bytes)                 │
│ version  │ 2 (u16 LE)                       │
│ flags    │ ObjectFlags bitfield (u16 LE)    │
│ orig_sz  │ original size (u32 LE)           │
│ comp_sz  │ payload size (u32 LE)            │
│ checksum │ xxh64 of payload data (u64)      │
│ reserved │ 6 bytes (zero)                   │
├──────────┴──────────────────────────────────┤
│ Payload (raw bytes, uncompressed)           │
└─────────────────────────────────────────────┘
```

### Object Flags

| Flag | Value | Description |
|------|-------|-------------|
| `LZ4_COMPRESSED` | 0x01 | Legacy compressed payload (unsupported in current build) |
| `HAS_FOOTER` | 0x02 | Object has a footer (reserved) |
| `ENCRYPTED` | 0x04 | Object is encrypted (reserved) |

### Operations

- `create_object(data)` — store raw payload, compute xxh64, build header + payload
- `read_object(bytes)` — validate magic/version, verify checksum, return payload

### Constants

| Constant | Value |
|----------|-------|
| `MAGIC` | `b"STFS"` |
| `VERSION` | 2 |
| `MAX_OBJECT_SIZE` | 64 MiB |
| `DEFAULT_BLOCK_SIZE` | 1 MiB |

## Block Management (`block.rs`)

Utilities for splitting file data into fixed-size blocks.

### Functions

| Function | Description |
|----------|-------------|
| `split_into_blocks(data, start_offset)` | Split buffer into `DEFAULT_BLOCK_SIZE` chunks, returns `Vec<(block_index, Bytes)>` |
| `block_position(offset)` | Returns `(block_index, offset_within_block)` |
| `block_count(size)` | Number of blocks needed for given size |

### Block References

Blocks are referenced in metadata via `BlockRef`:

```rust
struct BlockRef {
    object_id: u64,    // sequential ID of the S3 object
    offset: u64,       // offset within object (0 for one-block-per-object)
    length: u32,       // payload data length
    flags: BlockFlags, // PARTIAL_BLOCK (+ legacy COMPRESSED_LZ4)
}
```

| Flag | Value | Meaning |
|------|-------|---------|
| `COMPRESSED_LZ4` | 0x01 | Legacy flag (unsupported for new writes) |
| `PARTIAL_BLOCK` | 0x02 | Block is smaller than `block_size` (last block of a file) |

## Integrity

All objects include an **xxh64** checksum of payload data in the header. On read, the checksum is verified before returning bytes.

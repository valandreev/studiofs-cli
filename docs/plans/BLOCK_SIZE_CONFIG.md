# Configurable Block Size

## Overview

The block size (default 4 MiB) can now be customized during volume initialization using the `--block-size` flag.

## Usage

Initialize a volume with a custom block size:

```bash
# Default 4 MiB block size
studiofs init myvolume --endpoint http://127.0.0.1:9000 \
  --bucket test-bucket --access-key minioadmin --secret-key minioadmin

# Custom 8 MiB block size
studiofs init myvolume --endpoint http://127.0.0.1:9000 \
  --bucket test-bucket --access-key minioadmin --secret-key minioadmin \
  --block-size 8MB

# Custom 16 MiB block size
studiofs init myvolume --endpoint http://127.0.0.1:9000 \
  --bucket test-bucket --access-key minioadmin --secret-key minioadmin \
  --block-size 16MB
```

## Supported Formats

Block size can be specified in:
- Bytes: `4194304` (no suffix)
- Kilobytes: `4096KB`
- Megabytes: `4MB`, `8MB`, `16MB` (most common)
- Gigabytes: `1GB`
- Terabytes: `1TB`

All formats are case-insensitive: `4mb`, `4MB`, `4Mb` all work.

## Performance Trade-offs

| Block Size | Metadata Overhead | Memory per I/O | Best For |
|---|---|---|---|
| 1 MB | High | Low | Small files, metadata-heavy workloads |
| **4 MB** | Medium | Medium | **Balanced (default)** |
| 8 MB | Low | Higher | Large sequential files (VFX, video) |
| 16 MB | Very Low | Higher | Large batch uploads, compute workloads |

### When to use larger blocks:

- **Sequential read/write heavy workloads**: Larger blocks reduce the number of S3 requests and metadata lookups
- **Large file uploads/downloads**: Fewer blocks = fewer journal entries = faster flush times
- **VFX/Video content**: Typically stored as multi-gigabyte files, larger blocks reduce per-block overhead

### When to keep smaller blocks (4 MB default):

- **Mixed workloads**: Good balance between metadata efficiency and memory usage
- **Limited memory**: Smaller blocks mean fewer concurrent blocks in cache
- **Lots of small files**: Reduces wasted space from partial blocks

## Storage Implications

Block size affects:
- **Disk/Memory cache**: Each block is cached as a unit. Larger blocks = more memory per block, fewer blocks for same total data
- **S3 object size**: Each block becomes an S3 object. Larger blocks = larger objects = potentially slower single reads, but fewer requests
- **Journal entries**: Each block write creates one journal entry. Larger blocks = fewer journal operations during flush
- **Partial block padding**: Last block of a file may not be full-sized (no padding is used)

## Implementation Details

Block size is stored in the volume's `FormatInfo` metadata at initialization time and cannot be changed after volume creation. This is by design - changing block size would require migrating all existing data.

### Files Modified:
- `crates/studiofs/src/main.rs` - Added `--block-size` CLI flag
- `crates/studiofs/src/cli/init.rs` - Implemented size parsing and volume initialization with custom block size
- `crates/studiofs-meta/src/types.rs` - `FormatInfo::block_size` field (already existed)

### Size Parsing:
The implementation includes a robust size parser that:
- Handles multiple unit suffixes (B, KB, MB, GB, TB)
- Is case-insensitive
- Trims whitespace
- Validates against u32::MAX (4 GB limit)
- Includes 8 unit tests covering all cases

## Example Scenarios

### Scenario 1: Large VFX Project
```bash
# 100 GiB file with 8 MB blocks = ~12,800 blocks
# vs 4 MB blocks = ~25,600 blocks
# Reduces: metadata overhead, journal size, S3 requests
studiofs init vfx-vol \
  --endpoint https://s3.amazonaws.com \
  --bucket vfx-renders \
  --access-key AKIA... \
  --secret-key ... \
  --block-size 8MB
```

### Scenario 2: Small File Server
```bash
# Many small files (1-100 MB each)
# Keep default 4 MB to avoid excessive memory usage
studiofs init files-vol \
  --endpoint http://minio:9000 \
  --bucket files \
  --access-key admin \
  --secret-key password
```

### Scenario 3: High-Throughput Analytics
```bash
# Bulk data processing with large sequential files
# Larger blocks reduce per-block overhead during parallel processing
studiofs init analytics-vol \
  --endpoint https://s3.amazonaws.com \
  --bucket analytics \
  --access-key AKIA... \
  --secret-key ... \
  --block-size 16MB
```

## Limitations

- Block size **cannot be changed** after volume creation (it's stored in metadata)
- Block size **must remain constant** across all mount points of the same volume
- Maximum block size: 4 GB (u32::MAX)
- Block size is read-only after initialization - all clients mounting the volume will use the same block size

## Future Enhancements

- Per-file block size hints (write-time metadata)
- Dynamic block size tuning based on file access patterns
- Migration tools to convert existing volumes to different block sizes

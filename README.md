# StudioFS 🚀

**StudioFS** is a high-performance, cross-platform virtual filesystem that mounts S3-compatible cloud storage as a local drive. It combines the scalability of cloud object storage with a multi-tier caching system (L1 Memory + L2 Disk) for near-local access speeds — all powered by Rust.

> **Platform support:** Windows (via [WinFSP](https://winfsp.dev/)) · Linux/macOS (via [FUSE](https://github.com/cbeuw/fuser))

---

## Table of Contents

- [Key Features](#key-features)
- [Architecture](#architecture)
  - [System Components](#system-components)
  - [Block Storage Model](#block-storage-model)
  - [Write Path](#write-path)
  - [Read Path](#read-path)
  - [Transaction Model](#transaction-model)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Getting Started](#getting-started)
  - [1. Initialize a Volume](#1-initialize-a-volume)
  - [2. Mount the Filesystem](#2-mount-the-filesystem)
  - [3. Check Volume Status](#3-check-volume-status)
  - [4. Destroy a Volume](#4-destroy-a-volume)
- [CLI Reference](#cli-reference)
- [Using with MinIO (Local Testing)](#using-with-minio-local-testing)
- [Using with AWS S3](#using-with-aws-s3)
- [Programming Guide](#programming-guide)
  - [Initializing the Filesystem](#initializing-the-filesystem)
  - [Basic Operations](#basic-operations)
  - [Block Management](#block-management)
- [Technical Details](#technical-details)
  - [Redb Integration](#redb-integration)
  - [Metadata Schema](#metadata-schema)
  - [Cache Architecture](#cache-architecture)
- [Project Structure](#project-structure)
- [Development](#development)
- [Technologies](#technologies)
- [License](#license)

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Immutable Block Storage** | Files are split into fixed-size blocks (default 4MB). Each block has a sequential ID and is never modified after creation. |
| **Sequential Block IDs** | Blocks use sequential IDs instead of content hashes — simpler and faster, no deduplication overhead. |
| **Multi-tier Caching** | L1 in-memory cache (fixed 1GB) + L2 disk cache (configurable, default 50GB) |
| **Write-back with Journaling** | Writes return immediately after caching; background upload worker handles S3 persistence with crash recovery |
| **Block-level GC** | Deleted/overwritten blocks go to pending deletions table; background GC removes after grace period |
| **Cross-platform** | Windows (WinFSP), Linux & macOS (FUSE) — single codebase |
| **S3-compatible** | Works with AWS S3, MinIO, GCS, Wasabi, Backblaze B2, and any S3-compatible service |
| **LZ4 Compression** | Transparent LZ4 compression for all data blocks |
| **Fast Metadata** | Local metadata engine powered by [Redb](https://redb.org/) — ACID transactions |

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────┐
│           User Application                   │
│         (File I/O Operations)                │
└─────────────────────┬───────────────────────┘
                      │ POSIX / Win32 I/O
┌─────────────────────▼───────────────────────┐
│            FUSE / WinFSP Bridge              │
│   studiofs-fuse (unix) │ studiofs-winfsp   │
└─────────────────────┬───────────────────────┘
                      │
┌─────────────────────▼───────────────────────┐
│              VFS Layer                      │
│    studiofs-vfs/src/vfs.rs                  │
│  - Path resolution                          │
│  - Block allocation/deallocation            │
│  - Read/write orchestration                 │
├────────────────────┬────────────────────────┤
│   Metadata Engine  │    Data Store          │
│  studiofs-meta     │   studiofs-data        │
│  - Inode attrs    │   - S3 via OpenDAL    │
│  - Block refs     │   - Object format     │
│  - Pending GC     │   - LZ4 compression   │
├────────────────────┴────────────────────────┤
│            Cache Layer                       │
│     studiofs-cache                          │
│  - DiskCache (50GB default)                │
│  - WritebackManager (journal)              │
│  - Background upload worker                │
└─────────────────────────────────────────────┘
```

### Block Storage Model

StudioFS uses a **block-based architecture** with immutable storage:

```
File (Inode)
  │
  ▼
Block 0 ──► BlockRef { object_id: 1, offset: 0, length: 4194304, flags: COMPRESSED }
  │
  ▼
Block 1 ──► BlockRef { object_id: 2, offset: 0, length: 4194304, flags: COMPRESSED }
  │
  ▼
Block 2 ──► BlockRef { object_id: 3, offset: 0, length: 1048576, flags: COMPRESSED | PARTIAL }
```

Each block reference (`BlockRef`) contains:
- **`object_id`**: Sequential ID of the S3 object
- **`offset`**: Offset within the block (always 0 for full blocks)
- **`length`**: Actual data length (less than block size for partial blocks)
- **`flags`**: Compression and partial block indicators

### Write Path

```
Application
     │
     ▼
┌──────────────────────────────┐
│ 1. Split data into blocks    │
│ 2. Compress with LZ4         │
│ 3. Allocate sequential ID    │
│ 4. Store in disk cache       │
│ 5. Queue for async upload    │
└──────────────────────────────┘
     │
     ▼ (returns immediately)
Application

Background Worker:
     │
     ▼
┌──────────────────────────────┐
│ 1. Scan pending journal      │
│ 2. Upload to S3             │
│ 3. Mark complete            │
│ 4. Delete journal entry     │
└──────────────────────────────┘
```

### Read Path

```
Application
     │
     ▼
┌──────────────────────────────┐
│ 1. Get block refs from meta  │
│ 2. Check memory cache       │
│ 3. Check disk cache         │
│ 4. Fetch from S3 if needed  │
│ 5. Decompress LZ4           │
│ 6. Assemble response        │
└──────────────────────────────┘
     │
     ▼
Application
```

### Transaction Model

The metadata engine uses **redb** for ACID transactions:

1. **Write Transactions**: All metadata updates (block allocation, file creation) use redb's `begin_write()` + `commit()`
2. **Read Transactions**: Use `begin_read()` for consistent snapshots
3. **Crash Recovery**: The writeback manager maintains a binary journal. On startup, it scans the journal and resumes any incomplete uploads

---

## Prerequisites

### All Platforms
- [Rust toolchain](https://rustup.rs/) (edition 2024, so **Rust 1.85+** required)

### Windows
- [WinFSP](https://winfsp.dev/) — install the stable release
- The `WINFSP_DIR` environment variable must point to WinFSP's installation directory

### Linux
- FUSE 3 development libraries:
  ```bash
  sudo apt install fuse3 libfuse3-dev pkg-config  # Debian/Ubuntu
  sudo dnf install fuse3-devel                     # Fedora
  sudo pacman -S fuse3                             # Arch
  ```

### macOS
- [macFUSE](https://osxfuse.github.io/) (or `brew install macfuse`)

---

## Installation

### Build from Source

```bash
git clone https://github.com/user/studiofs.git
cd studiofs
cargo build --release
```

The binary is at `target/release/studiofs` (or `target\release\studiofs.exe` on Windows).

### Verify

```bash
./target/release/studiofs --version
./target/release/studiofs --help
```

---

## Getting Started

### 1. Initialize a Volume

Before mounting, create a **volume** with S3 connection details:

```bash
studiofs init my-volume \
  --endpoint https://s3.amazonaws.com \
  --bucket my-bucket \
  --access-key AKIAIOSFODNN7EXAMPLE \
  --secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  --region us-east-1
```

**Output:**
```
✓ Volume 'my-volume' initialized
  UUID:     a1b2c3d4-e5f6-...
  Endpoint: https://s3.amazonaws.com
  Bucket:   my-bucket
  Metadata: "~/.studiofs/meta/my-volume/my-volume.db"

Run `studiofs mount my-volume <mount-point>` to mount.
```

#### Parameters

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `<name>` | ✅ | — | Volume name (used as prefix in S3 key paths) |
| `--endpoint` | ✅ | — | S3-compatible endpoint URL |
| `--bucket` | ✅ | — | S3 bucket name (must exist) |
| `--access-key` | ✅ | — | S3 access key ID |
| `--secret-key` | ✅ | — | S3 secret access key |
| `--region` | ❌ | `""` | AWS region (auto-defaults to `us-east-1` for MinIO) |

> **Note:** The S3 bucket must already exist — `studiofs init` does **not** create it.

---

### 2. Mount the Filesystem

#### Windows
```powershell
studiofs mount my-volume Z:
```

#### Linux / macOS
```bash
mkdir -p /mnt/studio
studiofs mount my-volume /mnt/studio
```

**Output:**
```
✓ Volume 'my-volume' ready to mount at 'Z:'
  Memory limit: 1GB
  Cache size:   50GB
```

The process blocks while mounted. Press `Ctrl+C` to unmount.

#### Mount Options

| Flag | Default | Description |
|------|---------|-------------|
| `<name>` | — | Volume name (must be initialized) |
| `<mount-point>` | — | Drive letter (Windows) or path (Unix) |
| `--cache-dir` | `<meta-dir>/cache` | Path to disk cache directory |
| `--cache-size` | `50GB` | Max disk cache size (`KB`, `MB`, `GB`, `TB`) |
| `--foreground` | `false` | Run in foreground |
| `--log-level` | `info` | Log verbosity: `trace`, `debug`, `info`, `warn`, `error` |

---

### 3. Check Volume Status

```bash
studiofs status my-volume
```

**Output:**
```
StudioFS Volume Status
──────────────────────
  Name:       my-volume
  UUID:       a1b2c3d4-e5f6-...
  Endpoint:   https://s3.amazonaws.com
  Bucket:     my-bucket
  Block size: 4 MiB

  Inodes:     42
  Metadata:   "~/.studiofs/meta/my-volume/my-volume.db"
```

---

### 4. Destroy a Volume

> **WARNING:** This action is irreversible. It deletes all S3 data and local metadata.

```bash
studiofs destroy my-volume
```

Interactive confirmation required. Output:
```
WARNING: You are about to DESTROY volume 'my-volume'.
This will permanently delete all data in S3 and local metadata.
Do you want to proceed? [y/N]: y
To confirm, please type the volume name 'my-volume' again: my-volume
✓ Volume 'my-volume' destroyed successfully.
```

---

## CLI Reference

```
studiofs <COMMAND> [OPTIONS]

Commands:
  init      Initialize a new filesystem volume
  mount     Mount a filesystem
  status    Show filesystem status
  destroy   Permanently destroy a volume

Global Options:
  --log-level <LEVEL>   Log level: trace, debug, info, warn, error [default: info]
  --version             Print version
  --help                Print help
```

---

## Using with MinIO (Local Testing)

### 1. Start MinIO

```powershell
mkdir C:\minio
minio server C:\minio --console-address ":9001"
```

Default credentials: `minioadmin` / `minioadmin`  
Web console: http://127.0.0.1:9001

### 2. Create a Bucket

Open the MinIO web console, log in, and create a bucket (e.g., `my-bucket`).

### 3. Initialize & Mount

```powershell
# Initialize
studiofs init local-test \
  --endpoint http://127.0.0.1:9000 \
  --bucket my-bucket \
  --access-key minioadmin \
  --secret-key minioadmin

# Mount (Windows)
studiofs mount local-test Z:

# Mount (Linux)
studiofs mount local-test /mnt/studio
```

### 4. Use It

```powershell
# Write a file
echo "Hello, StudioFS!" > Z:\hello.txt

# Read it back
cat Z:\hello.txt
```

---

## Using with AWS S3

```bash
# Create bucket
aws s3 mb s3://my-studiofs-bucket --region us-east-1

# Initialize & mount
studiofs init prod-volume \
  --endpoint https://s3.amazonaws.com \
  --bucket my-studiofs-bucket \
  --access-key $AWS_ACCESS_KEY_ID \
  --secret-key $AWS_SECRET_ACCESS_KEY \
  --region us-east-1

studiofs mount prod-volume /mnt/studio \
  --memory-limit 4GB \
  --cache-size 500GB
```

> **Tip:** For best performance, set `--memory-limit` to ~25% of available RAM and `--cache-dir` to a fast SSD.

---

## Programming Guide

### Initializing the Filesystem

The core VFS is implemented in [`studiofs-vfs/src/vfs.rs`](crates/studiofs-vfs/src/vfs.rs). To use it programmatically:

```rust
use std::sync::Arc;
use studiofs_meta::{RedbMetaEngine, FormatInfo};
use studiofs_data::store::{DataStore, DataStoreConfig};
use studiofs_vfs::StudioVfs;

// 1. Create metadata engine
let meta = Arc::new(RedbMetaEngine::open("/path/to/meta.db")?);
meta.init_root()?;

// 2. Set format info
let format_info = FormatInfo {
    name: "my-volume".into(),
    uuid: "uuid-123".into(),
    endpoint: "http://127.0.0.1:9000".into(),
    bucket: "my-bucket".into(),
    access_key: "minioadmin".into(),
    secret_key: "minioadmin".into(),
    region: "us-east-1".into(),
    block_size: 4 * 1024 * 1024, // 4 MiB
    chunk_size: 64 * 1024 * 1024, // deprecated
};
meta.set_format_info(&format_info)?;

// 3. Create data store
let data_config = DataStoreConfig::from(&format_info);
let data = Arc::new(DataStore::new(&data_config)?);

// 4. Create VFS (with optional memory cache)
use studiofs_cache::memory::MemoryCache;
let mem_cache = Arc::new(MemoryCache::new(512 * 1024 * 1024)); // 512 MiB
let vfs = StudioVfs::new(
    meta.clone(),
    data,
    Some(mem_cache),
    "/path/to/cache",
    50 * 1024 * 1024 * 1024, // 50 GB
)?;
```

### Basic Operations

```rust
use studiofs_vfs::StudioVfs;

// Create a file
let attr = vfs.create(1, b"hello.txt", 0, 0, 0o644)?;
let ino = attr.ino;

// Write data
let payload = b"Hello, StudioFS!";
let written = vfs.write(ino, 0, payload).await?;
assert_eq!(written as usize, payload.len());

// Flush to storage
vfs.flush(ino).await?;

// Read data back
let data = vfs.read(ino, 0, payload.len() as u32).await?;
assert_eq!(&data[..], payload);

// Get attributes
let attr = vfs.getattr(ino)?;
println!("File size: {} bytes", attr.size);
```

### Block Management

The VFS automatically manages blocks. Key operations:

```rust
// Truncate file to new size
vfs.setattr(ino, Some(5 * 1024 * 1024)).await?; // 5 MiB

// Get block references for a file
let blocks = vfs.meta().get_file_blocks(ino)?;
for (block_idx, block_ref) in blocks {
    println!("Block {}: object_id={}, length={}", 
             block_idx, block_ref.object_id, block_ref.length);
}

// Delete file (marks all blocks for GC)
vfs.unlink(1, b"hello.txt")?;
```

---

## Technical Details

### Redb Integration

StudioFS uses [redb](https://redb.org/) as the embedded metadata store. Key integration points:

1. **ACID Transactions**: Every metadata operation wraps in `begin_write()` + `commit()`
2. **Key-Value Model**: All data is stored as serialized bytes (via `postcard`)
3. **Table Definitions**: Static `TableDefinition` constants for type safety

```rust
// Example: Reading file blocks
let txn = db.begin_read()?;
let table = txn.open_table(FILE_BLOCKS_TABLE)?;
let key = encode_file_block_key(ino, block_index);
let entry = table.get(key.as_slice())?;
let block: BlockRef = postcard::from_bytes(entry.value())?;
```

### Metadata Schema

The metadata schema is defined in [`studiofs-meta/src/types.rs`](crates/studiofs-meta/src/types.rs):

| Table | Key | Value | Description |
|-------|-----|-------|-------------|
| `attributes` | `ino (u64)` | `InodeAttr` | File/directory metadata |
| `directory_map` | `(parent_ino, name_hash)` | `DirEntryValue` | Directory entries |
| `file_blocks` | `(ino, block_index)` | `BlockRef` | Block references |
| `pending_deletions` | `object_id (u64)` | `timestamp (i64)` | GC queue |
| `counters` | `name (&str)` | `u64` | Next IDs, used space |
| `format_info` | `"format"` | `FormatInfo` | Volume configuration |

### Cache Architecture

The cache layer ([`studiofs-cache`](crates/studiofs-cache/)) provides:

1. **DiskCache**: Persistent block storage with LRU eviction
   - Default size: 50 GB
   - Minimum size: 1 GB
   - Target free disk space: 10%

2. **WritebackManager**: Async upload with crash recovery
   - Binary journal format (43 bytes per entry)
   - Status tracking: Pending → Uploading → Complete

3. **Background Workers**:
   - Upload worker: Max 32 concurrent uploads, exponential backoff retry
   - GC worker: Runs every 10 minutes, 1-hour grace period before deletion

---

## Project Structure

```
studiofs/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── studiofs/                 # CLI binary (init, mount, status)
│   ├── studiofs-vfs/             # Core VFS implementation
│   ├── studiofs-meta/            # Metadata engine (Redb)
│   ├── studiofs-data/            # S3 data store (OpenDAL)
│   ├── studiofs-cache/           # Cache management
│   ├── studiofs-fuse/            # FUSE bridge (Linux/macOS)
│   └── studiofs-winfsp/          # WinFSP bridge (Windows)
└── README.md
```

| Crate | Purpose |
|-------|---------|
| `studiofs` | CLI entry point |
| `studiofs-vfs` | Core filesystem logic — block allocation, read/write |
| `studiofs-meta` | Metadata engine — inodes, block refs, GC |
| `studiofs-data` | S3 I/O via OpenDAL |
| `studiofs-cache` | Disk cache, writeback, journal |
| `studiofs-fuse` | Linux/macOS FUSE adapter |
| `studiofs-winfsp` | Windows FUSE adapter |

---

## Development

### Build

```bash
cargo build
```

### Run Unit Tests

```bash
cargo test
```

### Run Integration Tests (requires MinIO)

```bash
# Start MinIO first
minio server C:\minio --console-address ":9001"

# VFS integration tests
cargo test -p studiofs-vfs --test vfs_test -- --ignored

# All integration tests
cargo test -p studiofs-data --test integration_test -- --ignored
```

> **Note:** `--test-threads=1` may be required to prevent connection pool issues.

### Debug Logging

```bash
studiofs mount my-volume /mnt/studio --log-level debug
```

Available log levels: `trace`, `debug`, `info`, `warn`, `error`.

---

## Technologies

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | **Rust** | Memory safety, zero-cost abstractions |
| Storage | [OpenDAL](https://opendal.apache.org/) | Universal S3-compatible client |
| Metadata | [Redb](https://redb.org/) | Embedded KV store, ACID, zero-copy |
| Serialization | [Postcard](https://github.com/jamesmunns/postcard) | Compact binary format |
| Compression | [lz4_flex](https://github.com/PSeitz/lz4_flex) | Fast LZ4 compression |
| FUSE (Windows) | [WinFSP](https://winfsp.dev/) | Windows FUSE implementation |
| FUSE (Unix) | [fuser](https://github.com/cbeuw/fuser) | Pure Rust FUSE3 library |
| Async runtime | [Tokio](https://tokio.rs/) | Industry-standard async runtime |
| CLI | [Clap](https://clap.rs/) | Derive-based argument parsing |

---

## License

This project is licensed under the **GPL-3.0** License. See [LICENSE](LICENSE) for details.

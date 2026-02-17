# StudioFS 🚀

**StudioFS** is a high-performance, cross-platform virtual filesystem that mounts S3-compatible cloud storage as a local drive. It combines the scalability of cloud object storage with a multi-tier caching system (L1 Memory + L2 Disk) for near-local access speeds — all powered by Rust.

> **Platform support:** Windows (via [WinFSP](https://winfsp.dev/)) · Linux/macOS (via [FUSE](https://github.com/cbeuw/fuser))

---

## Table of Contents

- [Key Features](#key-features)
- [Architecture](#architecture)
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
- [Data Model](#data-model)
- [Project Structure](#project-structure)
- [Development](#development)
- [Technologies](#technologies)
- [License](#license)

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Multi-tier caching** | L1 in-memory cache + L2 disk cache for near-local read speeds |
| **Cross-platform** | Windows (WinFSP), Linux & macOS (FUSE) — single codebase |
| **S3-compatible** | Works with AWS S3, MinIO, GCS, Wasabi, Backblaze B2, and any S3-compatible service |
| **Log-structured** | Writes are immutable objects (Copy-on-Write), enabling lock-free snapshots and crash consistency |
| **Compression** | Transparent **LZ4** compression for all data objects (high speed, low CPU) |
| **Fast metadata** | Local metadata engine powered by [Redb](https://redb.org/) — zero-copy reads, ACID transactions |
| **Content-Addressable** | Data is stored as immutable objects addressed by their hash (CAS), enabling automatic deduplication |


---

## Architecture

```
┌───────────────────────────────────────────┐
│               User Application             │
└─────────────────────┬─────────────────────┘
                      │  POSIX / Win32 I/O
┌─────────────────────▼─────────────────────┐
│            FUSE / WinFSP Bridge            │
│   studiofs-fuse (unix) │ studiofs-winfsp   │
└─────────────────────┬─────────────────────┘
                      │
┌─────────────────────▼─────────────────────┐
│              Virtual Filesystem            │
│         studiofs-vfs (path resolution,     │
│         chunk/slice/block mapping)         │
├────────────────┬──────────────────────────┤
│   Metadata     │      Data Store          │
│  studiofs-meta │     studiofs-data        │
│  (Redb: Inodes │     (OpenDAL → S3)       │
│   + Data Roots)│    (LZ4 Objects)         │
├────────────────┴──────────────────────────┤
│               Cache Layer                  │
│     studiofs-cache (LRU memory cache)      │
└───────────────────────────────────────────┘
```

**Data flow:**
1. **Write:** App → FUSE → VFS → Buffer (4 MiB) → Compress → Upload S3 Object → Update `Data Root` in Redb
2. **Read:** App → FUSE → VFS → Memory Cache → Fetch S3 Object → Decompress → Traverse Object Tree

---

## Prerequisites

### All Platforms
- [Rust toolchain](https://rustup.rs/) (edition 2024, so **Rust 1.85+** required)

### Windows
- [WinFSP](https://winfsp.dev/) — install the stable release
- The `WINFSP_DIR` environment variable must point to WinFSP's installation directory (e.g. `C:\Program Files (x86)\WinFsp`)

### Linux
- FUSE 3 development libraries:
  ```bash
  # Debian / Ubuntu
  sudo apt install fuse3 libfuse3-dev pkg-config

  # Fedora
  sudo dnf install fuse3-devel

  # Arch
  sudo pacman -S fuse3
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

Before mounting, you need to create a **volume** — this stores S3 connection details and initializes the metadata database.

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
Mount to a drive letter:
```powershell
studiofs mount my-volume Z:
```

#### Linux / macOS
Mount to a directory (must exist):
```bash
mkdir -p /mnt/studio
studiofs mount my-volume /mnt/studio
```

**Output:**
```
✓ Volume 'my-volume' ready to mount at 'Z:'
  Memory limit: 1GB
  Cache size:   100GB
```

The process blocks while the filesystem is mounted. Press `Ctrl+C` to unmount.

#### Mount Options

| Flag | Default | Description |
|------|---------|-------------|
| `<name>` | — | Volume name (must be initialized) |
| `<mount-point>` | — | Drive letter (Windows) or path (Unix) |
| `--cache-dir` | `<meta-dir>/cache` | Path to disk cache directory |
| `--cache-size` | `100GB` | Max disk cache size (`KB`, `MB`, `GB`, `TB`) |
| `--memory-limit` | `1GB` | Max in-memory cache size (`KB`, `MB`, `GB`, `TB`) |
| `--foreground` | `false` | Run in foreground (keep the terminal attached) |
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
  Chunk size: 64 MiB

  Inodes:     42
  Used space: 1073741824 bytes
  Next inode: 43
  Next slice: 128
  Metadata:   "~/.studiofs/meta/my-volume/my-volume.db"
```

---

### 4. Destroy a Volume

> **WARNING:** This action is irreversible. It deletes all S3 data and local metadata.

```bash
studiofs destroy my-volume
```

**Interactive Confirmation:**
1.  The command will warn you about data loss.
2.  Type `y` to proceed.
3.  Type the volume name (`my-volume`) to confirm.

**Output:**
```
Creating S3 client...
WARNING: You are about to DESTROY volume 'my-volume'.
This will permanently delete:
  1. All data in S3 bucket 'my-bucket' with prefix 'my-volume/'
  2. ALL local metadata and cache at "C:\Users\User\.studiofs\meta\my-volume"

Do you want to proceed? [y/N]: y
To confirm, please type the volume name 'my-volume' again: my-volume
Deleting data from S3...
✓ S3 data deleted.
Deleting local metadata and cache...
✓ Local files deleted.
Volume 'my-volume' destroyed successfully.
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

[MinIO](https://min.io/) is a great way to test StudioFS locally without AWS credentials.

### 1. Start MinIO

```powershell
# Create data directory
mkdir C:\minio

# Start server
minio server C:\minio --console-address ":9001"
```

Default credentials: `minioadmin` / `minioadmin`  
Web console: http://127.0.0.1:9001

### 2. Create a Bucket

Open the MinIO web console at http://127.0.0.1:9001, log in, and create a bucket (e.g. `my-bucket`).

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

# Check status
studiofs status local-test
```

---

## Using with AWS S3

### 1. Create an S3 Bucket

```bash
aws s3 mb s3://my-studiofs-bucket --region us-east-1
```

### 2. Initialize & Mount

```bash
studiofs init prod-volume \
  --endpoint https://s3.amazonaws.com \
  --bucket my-studiofs-bucket \
  --access-key $AWS_ACCESS_KEY_ID \
  --secret-key $AWS_SECRET_ACCESS_KEY \
  --region us-east-1

studiofs mount prod-volume /mnt/studio \
  --memory-limit 4GB \
  --cache-size 500GB \
  --cache-dir /var/cache/studiofs
```

> **Tip:** For best performance, set `--memory-limit` to ~25% of available RAM and `--cache-dir` to a fast SSD.

---

## Data Model

StudioFS uses a **Log-Structured Object Tree** architecture, similar to LucidLink or Btrfs/ZFS but adapted for object storage:

```
Inode (Redb)
 └── Data Root (Hash of Root Object)
      │
      ▼
   Root Object (S3)
   ├── [Meta] Child Link (Offset: 0, Len: 4MB, Hash: ...)
   │    └── Data Object (LZ4 Compressed)
   │
   └── [Meta] Child Link (Offset: 4MB, Len: 2MB, Hash: ...)
        └── Data Object (LZ4 Compressed)
```

| Concept | Size | Stored in | Description |
|---------|------|-----------|-------------|
| **Inode** | — | Redb (local) | File/directory metadata (size, mode, timestamps) and **Data Root** |
| **Object** | Max 64 MB | S3 (remote) | Immutable unit of storage. Can be **Meta** (pointers) or **Data** (payload) |
| **Payload**| Variable | S3 (inside Object)| LZ4-compressed byte stream |
| **Header** | Fixed | S3 (inside Object)| Magic `STFS`, Version, Original/Compressed Size, XXH64 Checksum |

**S3 Object Key Format (CAS):**
```
{volume}/objects/{ab}/{cd}/{hash_id}
```
Objects are content-addressable. The path is derived solely from the hash of the object's content (Header + Compressed Payload).

Example: `my-volume/objects/a1/b2/a1b2c3d4...`

---

## Project Structure

```
studiofs/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── studiofs/                 # CLI binary (init, mount, status)
│   ├── studiofs-vfs/             # Core VFS (path resolution, read/write logic)
│   ├── studiofs-meta/            # Metadata engine (Redb + Postcard)
│   ├── studiofs-data/            # S3 data store (OpenDAL)
│   ├── studiofs-cache/           # Multi-tier cache (LRU memory)
│   ├── studiofs-fuse/            # FUSE bridge (Linux/macOS)
│   └── studiofs-winfsp/          # WinFSP bridge (Windows)
└── README.md
```

| Crate | Purpose |
|-------|---------|
| `studiofs` | CLI entry point — parses arguments, wires components, starts FUSE |
| `studiofs-vfs` | Core filesystem logic — translates file ops to chunk/slice/block ops |
| `studiofs-meta` | Local metadata engine — inode table, directory entries, chunk→slice maps |
| `studiofs-data` | S3 block I/O — upload, download, delete, range reads via OpenDAL |
| `studiofs-cache` | In-memory LRU cache for hot blocks |
| `studiofs-fuse` | Linux/macOS FUSE adapter (`fuser` crate) |
| `studiofs-winfsp` | Windows FUSE adapter (`winfsp` crate) |

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
# Start MinIO first (see "Using with MinIO" section)

# DataStore-level tests (block CRUD)
cargo test -p studiofs-data --test integration_test -- --ignored --test-threads=1

# VFS-level tests (full file lifecycle: create → write → read → verify)
cargo test -p studiofs-vfs --test integration_test -- --ignored --test-threads=1
```

> **Note:** `--test-threads=1` is required to prevent opendal's HTTP connection pool from being torn down during parallel test execution.

### Debug Logging

```bash
studiofs mount my-volume /mnt/studio --log-level debug
```

Available log levels: `trace`, `debug`, `info`, `warn`, `error`.

---

## Technologies

| Component | Technology | Why |
|-----------|-----------|-----|
| Language | **Rust** | Memory safety, zero-cost abstractions, cross-platform |
| Storage | [OpenDAL](https://opendal.apache.org/) | Universal S3-compatible client with async I/O |
| Metadata | [Redb](https://redb.org/) | Lightweight embedded key-value store, ACID, zero-copy |
| Serialization | [Postcard](https://github.com/jamesmunns/postcard) | Compact binary format, `#[no_std]` compatible |
| FUSE (Windows) | [WinFSP](https://winfsp.dev/) | Mature, well-tested Windows FUSE implementation |
| FUSE (Unix) | [fuser](https://github.com/cbeuw/fuser) | Pure Rust FUSE library, supports FUSE3 |
| Async runtime | [Tokio](https://tokio.rs/) | Industry-standard async runtime |
| CLI | [Clap](https://clap.rs/) | Derive-based argument parsing |

---

## License

This project is licensed under the **GPL-3.0** License. See [LICENSE](LICENSE) for details.

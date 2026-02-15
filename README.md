# StudioFS 🚀

**StudioFS** is a high-performance, cross-platform virtual filesystem backed by S3-compatible cloud storage. It combines the scalability of cloud storage with the performance of local disk and memory caching, supporting both Windows (via WinFSP) and Linux/macOS (via FUSE).

## Key Features

- **High Performance**: Multi-tier caching system (L1 Memory Cache, L2 Disk Cache) for near-local access speeds.
- **Cross-Platform**: Support for Windows (Win32), Linux, and macOS.
- **S3-Backend**: Built on [OpenDAL](https://github.com/apache/opendal) for universal compatibility with AWS S3, MinIO, GCS, and more.
- **Fast Metadata**: Local metadata management using [Redb](https://github.com/cberner/redb) and [Postcard](https://github.com/jamesmunns/postcard) serialization.
- **Write-back Caching**: Efficient background uploading of writes.

## Architecture

StudioFS is organized into several specialized Rust crates:

- `studiofs`: Main CLI entry point.
- `studiofs-vfs`: Core Virtual Filesystem logic and path resolution.
- `studiofs-meta`: Metadata engine (Redb backend).
- `studiofs-data`: Data store backend (OpenDAL integration).
- `studiofs-cache`: Multi-tier caching implementation.
- `studiofs-winfsp`: Windows FUSE bridge using WinFSP.
- `studiofs-fuse`: Unix FUSE bridge using `fuser`.

## Quick Start

### 1. Build from Source
Ensure you have the Rust toolchain installed.

```powershell
# Build the workspace
cargo build --release
```

### 2. Initialize a Volume
Initialize the metadata database and connect to your S3 bucket.

```powershell
./target/release/studiofs init my-volume `
  --endpoint https://s3.amazonaws.com `
  --bucket my-studiofs-bucket `
  --access-key YOUR_ACCESS_KEY `
  --secret-key YOUR_SECRET_KEY `
  --meta-dir ./.studiofs
```

### 3. Mount the Filesystem

#### Windows
```powershell
./target/release/studiofs mount my-volume Z: --meta-dir ./.studiofs
```

#### Linux/macOS
```bash
./target/release/studiofs mount my-volume /mnt/studiofs --meta-dir ./.studiofs
```

### 4. Check Status
Check the volume configuration and health.

```powershell
./target/release/studiofs status my-volume --meta-dir ./.studiofs
```

## Technologies

- **Language**: Rust
- **Storage**: [OpenDAL](https://opendal.apache.org/)
- **Metadata**: [Redb](https://redb.org/)
- **Serialization**: [Postcard](https://github.com/jamesmunns/postcard)
- **FUSE (Windows)**: [WinFSP](https://winfsp.dev/)
- **FUSE (Unix)**: [fuser](https://github.com/cbeuw/fuser)

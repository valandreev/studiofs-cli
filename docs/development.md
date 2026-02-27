# Development

## Stack

- **Language:** Rust (2024 edition)
- **Build:** Cargo workspace with 7 crates
- **Metadata:** `redb` (local) + Tonbo manifest over S3-compatible storage
- **Object I/O:** OpenDAL (`services-s3`)
- **Compression:** LZ4 (`lz4_flex`) + xxh64 checksums
- **Cache:** Two-level (RAM + NVMe) with LRU and crash-safe writeback
- **Platform:** WinFSP (Windows), FUSE (Linux/macOS)
- **Test storage:** MinIO (or any S3-compatible endpoint)
- **Invalidation bus:** NATS (optional)

## Project Structure

```
studiofs-cli/
  ├── Cargo.toml                  # workspace root
  ├── crates/
  │   ├── studiofs/               # CLI binary
  │   │   └── src/cli/            # subcommand modules
  │   ├── studiofs-meta/          # metadata engine (redb + Tonbo)
  │   ├── studiofs-data/          # S3 I/O + object format
  │   ├── studiofs-cache/         # L1 RAM + L2 NVMe cache
  │   ├── studiofs-vfs/           # platform-agnostic VFS core
  │   ├── studiofs-fuse/          # FUSE bridge (Linux/macOS)
  │   └── studiofs-winfsp/        # WinFSP bridge (Windows)
  └── docs/                       # documentation
```

See [Architecture Overview](architecture.md) for crate dependency graph and data flows.

## Local Setup

1. Clone and pull latest:
```bash
git clone <repo-url>
cd studiofs-cli
git pull
```

2. Create local `.env` in repository root:
```bash
cp .env.example .env
```

3. Edit `.env` for your local environment.

Notes:
- `.env` is ignored by git and must not be committed.
- If `.env` is missing or values are unset, tests fall back to defaults:
  - MinIO endpoint: `http://127.0.0.1:9000`
  - MinIO credentials: `minioadmin:minioadmin`
  - Bucket default in tests: `studiofs-test`
  - NATS URL default for local dev: `nats://127.0.0.1:4222`
- Tests that require a bucket will try to create it if missing.

## Build

```bash
cargo build --release
```

## Test Commands

Run regular tests:
```bash
cargo test --release
```

Run ignored integration tests:
```bash
cargo test --release -- --ignored
```

Targeted suites:
```bash
# Metadata Tonbo/MinIO tests
cargo test -p studiofs-meta --release -- --ignored

# VFS flock/distributed lock tests
cargo test -p studiofs-vfs flock --release -- --ignored

# Data path integration tests
cargo test -p studiofs-data --test integration_test --release -- --ignored
```

## Environment Variables

Required for custom test endpoints/credentials:
- `STUDIOFS_MINIO_ENDPOINT`
- `STUDIOFS_MINIO_ACCESS_KEY`
- `STUDIOFS_MINIO_SECRET_KEY`
- `STUDIOFS_MINIO_BUCKET`

Optional:
- `STUDIOFS_NATS_URL`

## Infrastructure Requirements

- Reachable MinIO/S3-compatible endpoint from test environment
- Credentials with permission to create/read/write/delete test objects
- For invalidation bus scenarios: reachable NATS server

## Related Documentation

- [Architecture Overview](architecture.md) — system design and data flows
- [Cache System](cache.md) — L1/L2 cache internals
- [Metadata Engine](metadata.md) — redb, Tonbo sync, schema
- [NATS Invalidation Bus](nats.md) — invalidation flow and recommended tuning
- [VFS Layer](vfs.md) — read/write paths, readahead, AIMD
- [Data Layer](data-layer.md) — S3 object format, compression
- [Platform Bridges](platform-bridges.md) — WinFSP and FUSE
- [Configuration Reference](configuration.md) — CLI options, env vars, tuning
- [Metadata Schema](metadata-schema.md) — schema types and compatibility

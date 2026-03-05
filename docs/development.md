# Development

## Stack

- Rust 2024 workspace
- Metadata: local `redb` + PostgreSQL background sync (`PgMetaEngine`)
- Data I/O: OpenDAL (S3-compatible)
- Cache: RAM + disk + writeback queue
- Bridges: WinFSP (Windows), FUSE (Linux/macOS)

## Project Structure

```
studiofs-cli/
  ├── crates/
  │   ├── studiofs/           # CLI
  │   ├── studiofs-meta/      # metadata engines
  │   ├── studiofs-data/      # S3 object I/O
  │   ├── studiofs-cache/     # L1/L2 cache
  │   ├── studiofs-vfs/       # filesystem core
  │   ├── studiofs-fuse/      # Unix bridge
  │   └── studiofs-winfsp/    # Windows bridge
  └── docs/
```

## Local Setup

```bash
cp .env.example .env
```

Recommended `.env` values:
- `STUDIOFS_MINIO_ENDPOINT`
- `STUDIOFS_MINIO_ACCESS_KEY`
- `STUDIOFS_MINIO_SECRET_KEY`
- `STUDIOFS_MINIO_BUCKET`
- `STUDIOFS_TEST_PG_DSN`

`.env` is gitignored and must not be committed.

## Build

```bash
cargo build --release
```

## Tests

Fast suite:
```bash
cargo test --workspace --all-targets -- --nocapture
```

Infra-heavy ignored suite:
```bash
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

Targeted:
```bash
cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1
cargo test -p studiofs-vfs --test vfs_test -- --ignored --nocapture --test-threads=1
cargo test -p studiofs-data --test integration_test -- --ignored --nocapture
```

## Multi-Client Dev Flow

1. Client A:
```bash
studiofs init <name> ... --postgres <dsn>
studiofs mount <name> <mount-point>
```

2. Client B/C/...:
```bash
studiofs discover <name> --postgres <dsn>
studiofs mount <name> <mount-point>
```

## Related Docs

- [Architecture Overview](architecture.md)
- [Metadata Engine](metadata.md)
- [Configuration Reference](configuration.md)
- [Tests Guide](tests.md)
- [Cache System](cache.md)
- [VFS Layer](vfs.md)

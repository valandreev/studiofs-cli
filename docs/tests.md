# Tests Guide

## Test Layers

1. Fast local/unit tests (default).
2. Ignored integration tests with MinIO + PostgreSQL.

NATS/Tonbo suites removed.

## Prerequisites

- Rust toolchain
- MinIO (for data integration tests)
- PostgreSQL (for metadata PG integration tests)
- Optional `.env` in repo root

Example `.env` keys:
- `STUDIOFS_MINIO_ENDPOINT=http://127.0.0.1:9000`
- `STUDIOFS_MINIO_ACCESS_KEY=minioadmin`
- `STUDIOFS_MINIO_SECRET_KEY=minioadmin`
- `STUDIOFS_MINIO_BUCKET=studiofs-test`
- `STUDIOFS_TEST_PG_DSN=host=... port=5432 user=... password=... dbname=... sslmode=disable`

## Command Matrix

| Scope | Command |
|---|---|
| Full fast suite | `cargo test --workspace --all-targets -- --nocapture` |
| Full ignored integration suite | `cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1` |
| PG metadata suite | `cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1` |
| VFS ignored suite | `cargo test -p studiofs-vfs --test vfs_test -- --ignored --nocapture --test-threads=1` |
| Data integration suite | `cargo test -p studiofs-data --test integration_test -- --ignored --nocapture` |

## Recommended Local Flow

```bash
cargo test --workspace --all-targets -- --nocapture
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

## Troubleshooting

### PG test failures
- Check `STUDIOFS_TEST_PG_DSN` correctness.
- Verify PostgreSQL is reachable and schema can be created.
- Re-run single test with `--nocapture`.

### MinIO failures
- Validate endpoint/credentials/bucket.
- Ensure bucket permissions include create/read/write/delete.

### Windows lock issues
- Close processes that hold files in `target/`.
- Re-run with `--test-threads=1`.

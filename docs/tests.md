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
- `STUDIOFS_TEST_PG_DSN=host=... port=5432 user=... password=... sslmode=disable`

StudioFS will use PostgreSQL database `studiofs` automatically.

## Command Matrix

| Scope | Command |
|---|---|
| Full fast suite | `cargo test --workspace --all-targets -- --nocapture` |
| Full ignored integration suite | `cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1` |
| PG metadata suite | `cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1` |
| VFS ignored suite | `cargo test -p studiofs-vfs --test vfs_test -- --ignored --nocapture --test-threads=1` |
| Data integration suite | `cargo test -p studiofs-data --test integration_test -- --ignored --nocapture` |

## Important Current Regressions

The current architecture relies on these focused regressions:
- PG delta/checkpoint model:
  - `test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror`
  - `test_pg_overlay_allows_unrelated_remote_batches_to_apply`
  - `test_pg_checkpoint_excludes_uncommitted_local_blocks`
- local write-path correctness:
  - `test_truncate_shrink_then_extend_zero_fills_old_tail`
  - `test_truncate_to_zero_then_rewrite`
  - `test_write_at_exact_block_boundary_transition`

Run a single focused regression:

```bash
cargo test -p studiofs-meta test_pg_checkpoint_excludes_uncommitted_local_blocks -- --ignored --nocapture
cargo test -p studiofs-vfs --test vfs_test test_truncate_shrink_then_extend_zero_fills_old_tail -- --ignored --nocapture
```

## Recommended Local Flow

```bash
cargo test --workspace --all-targets -- --nocapture
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

## Troubleshooting

### PG test failures
- Check `STUDIOFS_TEST_PG_DSN` correctness.
- Verify PostgreSQL is reachable and database `studiofs` can be created or already exists.
- Re-run single test with `--nocapture`.

### MinIO failures
- Validate endpoint/credentials/bucket.
- Ensure bucket permissions include create/read/write/delete.

### Windows lock issues
- Close processes that hold files in `target/`.
- Re-run with `--test-threads=1`.

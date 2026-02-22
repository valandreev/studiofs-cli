# Development

## Stack

- Language/tooling: Rust + Cargo
- Metadata: `redb` (local) + Tonbo manifest over S3-compatible storage
- Object I/O: OpenDAL (`services-s3`)
- Test object storage: MinIO (or any S3-compatible endpoint)
- Optional invalidation bus: NATS

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

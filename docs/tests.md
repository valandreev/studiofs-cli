# Tests Guide

This document describes how tests are organized in StudioFS, how to run them, and how to troubleshoot failures.

## Test Scope

StudioFS tests are split into 3 layers:

1. Fast local tests (default `cargo test`)
2. Integration tests with S3-compatible storage (MinIO)
3. Multi-client invalidation tests with NATS

Integration-heavy scenarios are marked `#[ignore]` and run only when requested.

## Prerequisites

- Rust toolchain compatible with workspace (`edition = 2024`)
- `cargo` from stable Rust
- Optional `.env` in repository root (recommended)
- MinIO (for ignored integration tests)
- NATS (for NATS invalidation tests)

Load defaults from repository:

```bash
cp .env.example .env
```

Default test values:

- `STUDIOFS_MINIO_ENDPOINT=http://127.0.0.1:9000`
- `STUDIOFS_MINIO_ACCESS_KEY=minioadmin`
- `STUDIOFS_MINIO_SECRET_KEY=minioadmin`
- `STUDIOFS_MINIO_BUCKET=studiofs-test`
- `STUDIOFS_NATS_URL=nats://127.0.0.1:4222`
- `STUDIOFS_NATS_DELIVERY=core`

## Infrastructure Setup

### Option A: Docker

```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

docker run -d --name nats -p 4222:4222 nats:2
```

If you need JetStream mode coverage, start NATS with JetStream enabled:

```bash
docker run -d --name nats-js -p 4222:4222 nats:2 -js
```

### Option B: Native binaries

- MinIO example (Windows): `minio server C:\minio --console-address ":9001"`
- NATS example: `nats-server -p 4222`
- NATS with JetStream: `nats-server -js -p 4222`

## Command Matrix

| Scope | Command | External deps |
|---|---|---|
| Full fast suite (all crates/targets, non-ignored) | `cargo test --workspace --all-targets -- --nocapture` | none |
| Full integration suite (ignored tests) | `cargo test --workspace --all-targets -- --ignored --nocapture` | MinIO (+ NATS for NATS subset) |
| `studiofs` CLI tests | `cargo test -p studiofs -- --nocapture` | none |
| `studiofs-cache` tests | `cargo test -p studiofs-cache -- --nocapture` | none |
| `studiofs-data` ignored integration tests | `cargo test -p studiofs-data --test integration_test -- --ignored --nocapture` | MinIO |
| `studiofs-meta` ignored Tonbo/MinIO tests | `cargo test -p studiofs-meta tonbo_minio_ -- --ignored --nocapture` | MinIO |
| `studiofs-vfs` ignored integration tests | `cargo test -p studiofs-vfs --test vfs_test -- --ignored --nocapture` | MinIO (+ NATS for `test_nats_`) |
| `studiofs-vfs` NATS subset only | `cargo test -p studiofs-vfs --test vfs_test test_nats_ -- --ignored --nocapture` | MinIO + NATS |

## Recommended Local Workflows

### Fast PR check

```bash
cargo test --workspace --all-targets -- --nocapture
```

### Full pre-merge check

```bash
cargo test --workspace --all-targets -- --nocapture --test-threads=1
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

`--test-threads=1` reduces infrastructure contention and helps reproduce timing-sensitive issues deterministically.

### NATS-focused regression check

```bash
cargo test -p studiofs-vfs --test vfs_test test_nats_ -- --ignored --nocapture
```

JetStream mode:

```bash
STUDIOFS_NATS_DELIVERY=jetstream cargo test -p studiofs-vfs --test vfs_test test_nats_ -- --ignored --nocapture
```

PowerShell equivalent:

```powershell
$env:STUDIOFS_NATS_DELIVERY='jetstream'; cargo test -p studiofs-vfs --test vfs_test test_nats_ -- --ignored --nocapture
```

## Test Discovery and Filtering

List tests in a target:

```bash
cargo test -p studiofs-vfs --test vfs_test -- --list
```

Run one test by exact name:

```bash
cargo test -p studiofs-vfs --test vfs_test test_nats_delete_updates_metadata_and_clears_cached_block -- --ignored --nocapture
```

Run by prefix/filter:

```bash
cargo test -p studiofs-meta tonbo_minio_ -- --ignored --nocapture
```

## Environment Variables Used by Tests

| Variable | Default | Used by |
|---|---|---|
| `STUDIOFS_MINIO_ENDPOINT` | `http://127.0.0.1:9000` | `studiofs-data`, `studiofs-meta` ignored tests, `studiofs-vfs` ignored tests |
| `STUDIOFS_MINIO_ACCESS_KEY` | `minioadmin` | same as above |
| `STUDIOFS_MINIO_SECRET_KEY` | `minioadmin` | same as above |
| `STUDIOFS_MINIO_BUCKET` | `studiofs-test` | same as above |
| `STUDIOFS_NATS_URL` | `nats://127.0.0.1:4222` | `studiofs-vfs` NATS tests and Tonbo invalidation wiring |
| `STUDIOFS_NATS_DELIVERY` | `core` | Invalidation delivery mode (`core`, `auto`, `jetstream`) for runtime wiring |

Note:

- Runtime mount path uses CLI `--nats` (see [NATS Invalidation Bus](nats.md)).
- Test code still uses `STUDIOFS_NATS_URL` for controllable setup.
- For reconnect/downtime guarantees in invalidation tests, set `STUDIOFS_NATS_DELIVERY=jetstream`.

## Stress / Flake Checks

PowerShell loop:

```powershell
for ($i=1; $i -le 10; $i++) {
  cargo test -p studiofs-vfs --test vfs_test test_nats_ -- --ignored --nocapture
}
```

Bash loop:

```bash
for i in {1..10}; do
  cargo test -p studiofs-vfs --test vfs_test test_nats_ -- --ignored --nocapture
done
```

## Troubleshooting

### MinIO-related failures

- Verify endpoint and credentials in `.env`.
- Ensure bucket create/list/delete permissions are available.
- Confirm port `9000` is reachable from the test process.

### NATS-related failures

- Verify `STUDIOFS_NATS_URL` points to a reachable server.
- Confirm port `4222` is reachable.
- If `STUDIOFS_NATS_DELIVERY=jetstream`, verify server runs with JetStream enabled.
- Run NATS subset directly:
  - `cargo test -p studiofs-vfs --test vfs_test test_nats_ -- --ignored --nocapture`

### Test appears skipped

Some ignored integration tests perform runtime reachability checks and may short-circuit when infrastructure is unavailable. Always verify that required services are actually running before interpreting a pass as full coverage.

### File lock / permission issues on Windows

If you see path lock or access-denied errors in `target/`, close processes that may hold handles (antivirus scanner, IDE background runner, prior test processes), then re-run.

## CI Guidance

Recommended CI order:

1. `cargo test --workspace --all-targets -- --nocapture`
2. `cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1`

This keeps fast feedback first and runs external-infra suites in a more deterministic mode.

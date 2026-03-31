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
- `STUDIOFS_S3_ENDPOINT`
- `STUDIOFS_S3_ACCESS_KEY`
- `STUDIOFS_S3_SECRET_KEY`
- `STUDIOFS_S3_BUCKET`
- `STUDIOFS_TEST_PG_DSN`

`.env` is gitignored and must not be committed.

Local development and infra-backed testing use RustFS as the default
S3-compatible backend.

## Build

```bash
cargo build --release
```

## Checks

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo check --workspace
```

## CI Matrix

The GitHub Actions workflow (`.github/workflows/rust.yml`) runs four jobs:

| Job | Runner | Purpose |
|-----|--------|---------|
| **Quality Gates** | `ubuntu-latest` | `cargo fmt`, clippy (workspace + all-features), `cargo doc`, `cargo audit` |
| **Linux** | `ubuntu-latest` | Workspace tests, SFS-121 targeted VFS fast suite, explicit `studiofs-fuse` Linux unit step |
| **macOS** | `macos-latest` | Clippy + tests — explicit `studiofs-fuse` macOS unit step (exercises `cfg(target_os="macos")` branches) |
| **Windows** | `windows-latest` | Clippy + tests — explicit `studiofs-winfsp` unit step (exercises `cfg(windows)` code paths) |

Each platform job includes a named step for the backend-specific crate so
regressions in `studiofs-fuse` (Linux or macOS paths) or `studiofs-winfsp`
are immediately attributable in the CI job log rather than buried inside the
broad workspace run. Failures that originate from platform-specific code
should call out the backend step name directly.

Platform jobs depend on Quality Gates to avoid wasting macOS/Windows runner
minutes when portable checks fail.

## Tests

Fast suite:
```bash
cargo test --workspace --all-targets -- --nocapture
```

Infra-heavy ignored suite:
```bash
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

On Windows/PowerShell, clear proxy env before infra tests when RustFS/S3 and PostgreSQL are
directly reachable in LAN/VPN:

```powershell
Remove-Item Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:ALL_PROXY -ErrorAction SilentlyContinue
$env:NO_PROXY='localhost,127.0.0.1,::1'
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
studiofs init <name> ... --meta <postgres-server-dsn>
studiofs mount <name> <mount-point>
```

2. Client B/C/...:
```bash
studiofs discover <name> --meta <postgres-server-dsn>
studiofs mount <name> <mount-point>
```

## Related Docs

- [Contributing](contributing.md) — contribution rules
- [Architecture Overview](architecture.md)
- [Metadata Engine](metadata.md)
- [Configuration Reference](configuration.md)
- [Tests Guide](tests.md)
- [Cache System](cache.md)
- [VFS Layer](vfs.md)

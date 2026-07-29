# Development

## Stack

- Rust 2024 workspace
- Metadata: local `redb` + PostgreSQL background sync (`PgMetaEngine`)
- Data I/O: OpenDAL (S3-compatible)
- Cache: RAM + disk + runtime upload queue
- Bridges: WinFSP (Windows), macFUSE (macOS), libfuse3 low-level FUSE (Linux).

## Project Structure

```
studiofs-cli/
  ├── crates/
  │   ├── studiofs/           # CLI
  │   ├── studiofs-meta/      # metadata engines
  │   ├── studiofs-data/      # S3 object I/O
  │   ├── studiofs-cache/     # L1/L2 cache
  │   ├── studiofs-vfs/       # filesystem core
  │   ├── studiofs-macfuse/   # macOS native bridge stub
  │   ├── studiofs-linux-fuse/# Linux libfuse3 low-level bridge
  │   ├── studiofs-winfsp/    # Windows bridge
  │   └── studiofs-e2e/       # real mounted E2E runner
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

On macOS, `crates/studiofs-macfuse` still requires real macFUSE headers for
Cargo builds and tests. The C shim includes an editor-only compatibility header
so workspace diagnostics stay usable when `fuse_lowlevel.h` is absent, but the
actual build continues to fail fast unless the system headers are discoverable
via `pkg-config fuse` or standard Homebrew include locations.

## Checks

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace
```

The workspace lint baseline is defined once in the root `Cargo.toml` under
`[workspace.lints]`. Member crates inherit it explicitly with:

```toml
[lints]
workspace = true
```

Keep new lint policy staged and low-noise. Prefer focused Clippy or check runs
for the touched crates during local work; on Windows, use a task-specific
`--target-dir` when the default `target/` directory is locked by another
process.

## CI Matrix

Linux verification is defined in RWX (`.rwx/ci.yml`) and is manual-only; it
does not run automatically on PRs. GitHub Actions (`.github/workflows/rust.yml`)
remains responsible for hosted macOS and Windows verification that RWX does
not currently provide, and those workflows are also manual-only.

| Job | Runner | Purpose |
|-----|--------|---------|
| **Quality Gates** | RWX `ubuntu:24.04` | `cargo fmt`, clippy (workspace + all-features), `cargo doc`, `cargo audit` |
| **Ubuntu Linux** | RWX `ubuntu:24.04` | Workspace tests, local-first VFS checks, and operator-run mounted libfuse3 E2E |
| **Rocky Linux** | RWX `rockylinux:9` | Workspace tests; mounted libfuse3 release validation is required on Rocky 9.4+ |
| **macOS** | `macos-latest` | Clippy + tests for macOS-owned crates |
| **Windows** | `windows-latest` | Clippy + tests — explicit `studiofs-winfsp` unit step (exercises `cfg(windows)` code paths) |

macOS and Windows jobs include named steps for backend-specific crates so
regressions in `studiofs-macfuse` or `studiofs-winfsp` are immediately
attributable in the CI job log rather than buried inside the broad workspace
run. Linux links the system libfuse3 development package and exercises
`studiofs-linux-fuse` in the workspace tests.

The GitHub-hosted macOS and Windows jobs are independent from RWX. Start the
RWX Linux gate manually when Linux signal is needed; hosted macOS and Windows
Actions can be started manually from the GitHub Actions UI when needed.

### RWX local CLI and logs

RWX workflow definitions live in `.rwx/`:

- `.rwx/ci.yml` — Ubuntu quality gates and Ubuntu Linux tests.
- `.rwx/rocky.yml` — Rocky Linux tests.
- `.rwx/e2e.yml` — operator-run Linux mounted E2E targets; the runner must
  expose `/dev/fuse` and install libfuse3 plus `fusermount3`.

When diagnosing RWX failures locally, keep the RWX CLI binary under
`.rwx/bin/`. That directory is ignored by Git and is only a local tool cache;
do not vendor the binary into commits. On Windows the local path is typically:

```powershell
.\.rwx\bin\rwx-windows-x86_64.exe
```

Fetch task logs with the local binary:

```powershell
.\.rwx\bin\rwx-windows-x86_64.exe logs <task-id> --output-dir .rwx\downloads\current\<task-name>
```

Downloaded logs belong under `.rwx/downloads/`; that directory has its own
`.gitignore` and should remain uncommitted. If `.rwx/bin/` is empty, download
the matching RWX CLI release binary from the official RWX GitHub release before
running `rwx logs`.

The hosted Linux real mounted E2E targets fail preflight when `/dev/fuse` is
unavailable. That is an infrastructure failure, not a test skip.

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
cargo test -p studiofs-e2e --test runner_contract
cargo test -p studiofs-e2e --test compose_infra
```

## Multi-Client Dev Flow

1. Client A:
```bash
export STUDIOFS_CLIENT_ROOT=/tmp/studiofs-client-a
studiofs init <name> ... --meta <postgres-server-dsn>
studiofs mount <name> <mount-point>
```

2. Client B/C/...:
```bash
export STUDIOFS_CLIENT_ROOT=/tmp/studiofs-client-b
studiofs discover <name> --meta <postgres-server-dsn>
studiofs mount <name> <mount-point>
```

Use a different `STUDIOFS_CLIENT_ROOT` for each local client on the same host so
their `redb` metadata databases, mount records, logs, and default cache roots do
not overlap.

The `studiofs-e2e` crate provides a reusable multi-client process controller
for mounted scenarios. It prepares `client-a` through `client-d` style layouts
under `target/e2e-runs/<run-id>/artifacts/clients/`, runs `init` for the first
client, runs `discover` for peers, mounts each client with a separate
`STUDIOFS_CLIENT_ROOT` and `--cache-dir`, captures per-client stdout/stderr and
tracing logs, and owns graceful unmount, forced kill, and restart/remount
lifecycle transitions.

## Related Docs

- [Contributing](contributing.md) — contribution rules
- [Architecture Overview](architecture.md)
- [Metadata Engine](metadata.md)
- [Configuration Reference](configuration.md)
- [Tests Guide](tests.md)
- [Cache System](cache.md)
- [VFS Layer](vfs.md)

<div align="center">
  <img src="docs/branding/studiofs-logo-final/png/cat_logo.png" alt="StudioFS logo" width="74" />
  <h1 style="margin: 2px 0 0; font-size: 2.5rem; line-height: 0;">StudioFS</h1>
  <p style="margin: 4px 0 0; font-size: 0.95rem; line-height: 1.2; color: #9aa4b2;">cloud native file system for VFX</p>
</div>


StudioFS is an open-source, self-hostable virtual filesystem designed for
computer graphics, animation, VFX, and post-production studios. It lets teams
access and edit heavy source assets (4K, RAW, proxies, geometry, caches)
directly from cloud or remote storage without waiting for full downloads or
background sync jobs.

- **Real-time access to heavy media**
  Work directly with proxies, 4K, RAW footage, and other large assets from the
  cloud as if they were stored on a local fast SSD.

- **A single source of truth**
  Say goodbye to duplicate files, broken links, and version chaos. The entire team sees the exact same workspace, always in sync.

- **Seamless DCC integration**
  Work in your existing tools: DaVinci Resolve, Flame, Fusion, Nuke, After
  Effects, Maya, Houdini, and Blender. A StudioFS volume mounts like a regular
  drive, so you can keep your current pipeline without custom plugins.

- **Built for performance with Rust**
  Engineered from the ground up in Rust to guarantee memory safety, zero-overhead performance, and rock-solid stability under the heaviest multi-gigabyte editing workloads.

- **Self-hosted and open**
  Unlike proprietary SaaS alternatives, StudioFS is **100% open-source and
  self-hosted**, giving you control over your storage, metadata, and
  infrastructure. No hidden telemetry included.

- **Cat logo**
  Because infrastructure is better with a little personality. 🐱



### ⚠️ Caution

> [!WARNING]
> Project is in its early stages. Breaking changes will happen.
> Use with caution.



## What Problem Does StudioFS Solve?

In a classic remote production pipeline, artists often need to download hundreds
of gigabytes of assets, wait for transfers to finish, relink paths manually, and
then upload changed files back to shared storage.

StudioFS is designed to remove that loop:

- **Instant start**
  StudioFS fetches only the file bytes that are actually needed. An artist opens
  a project and can start working with video, geometry, or cache files without a
  full local copy first.

- **Background upload**
  Saves are accepted locally first, so the artist can keep working. StudioFS then
  uploads the new data to S3-compatible storage in the background.

- **Freedom of deployment**
  Run the system on your own infrastructure with any S3-compatible storage
  backend, such as AWS S3, RustFS, or Backblaze B2, plus PostgreSQL for shared
  metadata coordination.

- **Security and end-to-end encryption**
  Encryption support is in development. The intended model is to store source
  assets in encrypted form in S3-compatible storage and decrypt them on local
  workstations, while keeping key management simple for studio administrators.

## How It Works

StudioFS combines S3-compatible object storage with local caching and
PostgreSQL-backed metadata coordination:

- **Cloud storage holds the assets**
  Heavy media and project data are stored as immutable objects in
  S3-compatible storage.

- **PostgreSQL coordinates the team**
  PostgreSQL stores shared metadata, orders changes between clients, and helps
  clients discover file additions, deletes, renames, snapshots, and other
  metadata updates.

- **Local cache keeps active work fast**
  StudioFS caches only the regular-file byte ranges that are actually accessed.
  This range-oriented, on-demand cache reduces disk usage, lowers network
  traffic, and hides most remote-storage latency.

- **Local-first runtime keeps the mount responsive**
  Interactive filesystem operations use local state first. Metadata lives in the
  embedded `redb` database, written data becomes visible locally immediately,
  and background workers upload payloads and replicate metadata through
  PostgreSQL.

## Technical Foundation

StudioFS mounts S3-compatible object storage as a local cross-platform drive, using PostgreSQL for shared metadata storage and replication between clients. 

It is built around a local-first runtime model:
- foreground filesystem operations work from local state,
- file payloads are stored in immutable S3 objects,
- shared metadata converges through PostgreSQL background sync,
- RAM and disk cache tiers hide most remote latency.

## Current Architecture

```
studiofs (CLI)
  ├── studiofs-meta       local metadata + PostgreSQL delta sync
  ├── studiofs-data       S3 object I/O + object format
  ├── studiofs-cache      local payload backend + upload queue
  ├── studiofs-vfs        filesystem core
  ├── studiofs-fuse       Linux/macOS bridge
  ├── studiofs-winfsp     Windows bridge
  └── studiofs-e2e        real mounted E2E runner
```

Key points:
- PostgreSQL is the shared metadata backend.
- Local metadata lives in `redb`; PostgreSQL is used for bootstrap, checkpoints, and ordered delta replication.
- Writes become readable locally before remote upload/metadata publish fully completes.
- Multi-client mounts converge through PostgreSQL sync.
- Internals are highly inspired by JuiceFS Community Edition project.

## Features

- S3-compatible data path via Apache OpenDAL
- Local-first metadata path with eventual multi-client convergence
- Local payload backend + upload queue
- Append-only ingress and crash recovery for accepted local writes
- Cross-platform mounts: WinFSP on Windows, FUSE on Linux/macOS
- Background upload, janitor, and metadata sync workers
- Instant local snapshots with eventual PostgreSQL replication; cluster-wide
  snapshot protection begins after PostgreSQL publication
- Historical snapshot mounts as read-only views

## Quick Start

Initialize a new volume:

```bash
studiofs init myvolume \
  --endpoint http://127.0.0.1:9000 \ ## needs local minio/rustfs running
  --bucket studiofs-test \
  --access-key studiofs \
  --secret-key studiofs123 \
  --meta postgres://studiofs:studiofs@127.0.0.1:5432 ## needs local postgres running
```

Mount it:

```bash
studiofs mount myvolume Z:
studiofs mount myvolume /mnt/studio
```

Windows and macOS use the standard Local-First runtime path for mounts.
Foreground I/O follows normal buffered read/write flow while upload and
file-version publication continue asynchronously in the background.

Attach another client to an existing volume:

```bash
studiofs discover myvolume --meta postgres://studiofs:studiofs@127.0.0.1:5432
studiofs mount myvolume Y:
```

## Commands

| Command | Description |
|---|---|
| `init` | Create a new volume and register it in PostgreSQL |
| `discover` | Bootstrap a new client from an existing PostgreSQL-backed volume |
| `mount` | Mount a volume as a local filesystem |
| `umount` | Unmount an active mountpoint |
| `status` | Show local/runtime volume status |
| `janitor` | Scan and clean orphaned remote objects |
| `fsck` | Scan/repair metadata inconsistencies (dangling entries, orphans, undo state) |
| `dump` | Export a PostgreSQL metadata dump for a volume (after sync) |
| `load` | Import a metadata dump into PostgreSQL |
| `snapshot` | Create/list immutable metadata snapshots |
| `perf` | Watch live runtime stats from a mounted StudioFS volume |
| `bench` | Run a mounted filesystem benchmark and write a JSON report |
| `destroy` | Remove a volume locally or locally + remotely |

Run `studiofs --help` or `studiofs <command> --help` for full usage.

Size inputs used by CLI flags and runtime `STUDIOFS_*_BYTES` environment
variables accept `B`, `KB`, `MB`, `GB`, `TB`, `PB`, `EB` suffixes (binary
units, `1KB = 1024B`) and fractional values like `1.5GB` (rounded down to
bytes).

Benchmark and statistics tooling uses the mounted filesystem path. Use
`studiofs perf` for live runtime counters and `studiofs bench` for reproducible
mounted benchmark reports. See
[`docs/perf/benchmark-methodology.md`](docs/perf/benchmark-methodology.md) for
the comparison methodology and known limits.

## Development

Build:

```bash
cargo build --release
```

Checks:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -W missing-docs
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace
```

Workspace lint policy is inherited from the root `[workspace.lints]` tables.
Member crates opt in with `[lints] workspace = true`; keep new shared lints
low-noise and stage broad hygiene migrations separately.

Fast test suite:

```bash
cargo test --workspace --all-targets -- --nocapture
```

CI verification is manual-only: RWX covers Linux quality/Linux lanes, and
GitHub Actions remains available for hosted macOS and Windows lanes. See
[`.github/workflows/rust.yml`](.github/workflows/rust.yml),
[`docs/development.md`](docs/development.md), and
[`docs/contributing.md`](docs/contributing.md) for the full verification
matrix (Quality Gates, Linux, macOS, Windows) and the contributor rules for
platform-specific changes. Each platform job includes an explicit named
step for its backend crate (`studiofs-fuse` on Linux/macOS,
`studiofs-winfsp` on Windows) so regressions in platform-specific code are
immediately attributable in the CI log.

Infra-heavy ignored suite:

```bash
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

Real mounted E2E runner skeleton:

```bash
cargo run -p studiofs-e2e -- list
cargo run -p studiofs-e2e -- run --scenario X-Y,Z --studiofs-bin <PATH TO studiofs BINARY>
cargo run -p studiofs-e2e -- clean
```

The E2E runner starts `docker/compose.e2e.yml` by default. The compose stack
contains PostgreSQL 18, RustFS, and a Toxiproxy placeholder, waits for healthy
required services, writes Docker logs under `artifacts/docker/`, and tears down
containers, networks, and volumes during cleanup. Use `--port-range <start-end>`
to choose deterministic host ports, or `--infra disabled` for runner self-tests
that must not start Docker.

Local development and infra-backed tests use RustFS as the default
S3-compatible backend together with PostgreSQL.

Local setup usually starts with:

```bash
cp .env.example .env
```

Useful test/runtime variables include:
- `STUDIOFS_S3_ENDPOINT`
- `STUDIOFS_S3_ACCESS_KEY`
- `STUDIOFS_S3_SECRET_KEY`
- `STUDIOFS_S3_BUCKET`
- `STUDIOFS_TEST_PG_DSN`

## Documentation

Project docs live in [docs/](docs/):

- [docs/roadmap.md](docs/roadmap.md) - future development plans
- [docs/examples.md](docs/examples.md) - usage variants and examples
- [docs/architecture.md](docs/architecture.md) - current system design and data flow
- [docs/configuration.md](docs/configuration.md) - CLI commands, flags, env vars
- [docs/development.md](docs/development.md) - local setup, build, and test workflow
- [docs/contributing.md](docs/contributing.md) - verification matrix, CI ownership, platform-specific code rules
- [docs/tests.md](docs/tests.md) - test matrix and infra expectations
- [docs/filesystem-testing-overview.md](docs/filesystem-testing-overview.md) - friendly overview of the real filesystem testing approach
- [docs/e2e-runner-contract.md](docs/e2e-runner-contract.md) - real mounted E2E runner contract, artifacts, reports, preflight, and cleanup rules
- [docs/metadata.md](docs/metadata.md) - local `redb` + PostgreSQL metadata model
- [docs/cache.md](docs/cache.md) - local cache backend, staged writeback, and upload flow
- [docs/vfs.md](docs/vfs.md) - filesystem runtime behavior
- [docs/data-layer.md](docs/data-layer.md) - S3 object I/O and format details
- [docs/platform-bridges.md](docs/platform-bridges.md) - WinFSP and FUSE integration

## Credits and Inspiration

StudioFS is heavily inspired by the design choices of
[JuiceFS Community Edition](https://github.com/juicedata/juicefs), especially
its metadata/data separation model and distributed filesystem architecture.

We appreciate the JuiceFS team's contributions to the open-source storage
community and their thoughtful work on practical distributed storage systems.
JuiceFS has served as an important technical reference point for StudioFS.

## License

This project is distributed under the GPL-3.0 license. You may use and modify
StudioFS for personal or commercial purposes, subject to the terms of the
license.

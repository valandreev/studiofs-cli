# StudioFS

An open-source, self-hosted virtual filesystem optimized for VFX and post-production studios. Access and edit large files instantly from the cloud or remote hosted storage. No downloads, no syncing, just pure collaboration.

- **Real-time access to heavy media**
  Work directly with proxy, 4K, and RAW footage from the cloud as if it were on a local SSD. No more waiting for downloads.

- **A single source of truth**
  Say goodbye to duplicate files, broken links, and version chaos. The entire team sees the exact same workspace, always in sync.

- **Seamless NLE integration**
  Edit collaboratively out of your existing toolkit, including DaVinci Resolve, Flame, Fusion, Nuke, After Effects, Maya, Houdini, and Blender.

- **Built for performance with Rust**
  Engineered from the ground up in Rust to guarantee memory safety, zero-overhead performance, and rock-solid stability under the heaviest multi-gigabyte editing workloads.

Unlike proprietary SaaS alternatives, StudioFS is **100% open-source and self-hosted**, giving you absolute control over your storage and infrastructure.

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
  ├── studiofs-cache      L1/L2 cache + upload journal/queue
  ├── studiofs-vfs        filesystem core
  ├── studiofs-fuse       Linux/macOS bridge
  └── studiofs-winfsp     Windows bridge
```

Key points:
- PostgreSQL is the shared metadata backend.
- Local metadata lives in `redb`; PostgreSQL is used for bootstrap, checkpoints, and ordered delta replication.
- Writes become readable locally before remote upload/metadata publish fully completes.
- Multi-client mounts converge through PostgreSQL sync.

## Features

- S3-compatible data path via OpenDAL
- Local-first metadata path with eventual multi-client convergence
- Two-level cache: RAM + NVMe/disk
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
studiofs mount myvolume Z: --direct-io
studiofs mount myvolume /mnt/studio
```

`--direct-io` is primarily for Windows benchmarking and strict write-through
workloads. It disables WinFSP metadata cache timeouts and routes unbuffered
handle I/O through the VFS direct path.

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
| `test-cache` | Run direct L2 and end-to-end cache benchmarks |
| `dump` | Export a PostgreSQL metadata dump for a volume (after sync) |
| `load` | Import a metadata dump into PostgreSQL |
| `snapshot` | Create/list immutable metadata snapshots |
| `destroy` | Remove a volume locally or locally + remotely |

Run `studiofs --help` or `studiofs <command> --help` for full usage.

Quick benchmark example:

```bash
studiofs test-cache \
  /tmp/studiofs-cache-bench \
  --block-size 16MB \
  --read-size 1MB \
  --small-max 10KB \
  --medium-max 16MB \
  --concurrency 16 \
  --io-depth 64 \
  --access-pattern mixed \
  --l1-limit 2GB \
  --l2-limit 100GB
```

`test-cache` prints a readable terminal summary and also saves the full structured
JSON report in the current working directory using an auto-generated file name
such as `studiofs-test-cache-direct-l2-20260319-151255.json`. Use
`--json-output <path>` to override the report location or file name.
Use `--profile` for on-demand CPU/profile hooks; reports include a `run_id`
that also tags profile tracing events and cache blocking-task pressure counters.
The JSON report also contains an `advanced_metrics` section with per-size-class
latency/throughput (small/medium/large), configurable boundaries, queue
backpressure percentiles, and (in direct-l2 mode) L1/L2 hit-miss and bytes-served
breakdown with per-level latency distributions.

## Development

Build:

```bash
cargo build --release
```

Checks:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo check --workspace
```

Fast test suite:

```bash
cargo test --workspace --all-targets -- --nocapture
```

Infra-heavy ignored suite:

```bash
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

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
- [docs/tests.md](docs/tests.md) - test matrix and infra expectations
- [docs/metadata.md](docs/metadata.md) - local `redb` + PostgreSQL metadata model
- [docs/cache.md](docs/cache.md) - cache, upload queue, and writeback behavior
- [docs/vfs.md](docs/vfs.md) - filesystem runtime behavior
- [docs/data-layer.md](docs/data-layer.md) - S3 object I/O and format details
- [docs/platform-bridges.md](docs/platform-bridges.md) - WinFSP and FUSE integration

## License

GPL-3.0

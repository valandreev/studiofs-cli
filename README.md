<div align="center">
  <img src="docs/branding/studiofs-logo-final/png/cat_logo.png" alt="StudioFS logo" width="74" />
  <h1 style="margin: 2px 0 0; font-size: 2.5rem; line-height: 0;">StudioFS</h1>
  <p style="margin: 4px 0 0; font-size: 0.95rem; line-height: 1.2; color: #9aa4b2;">cloud native file system for VFX</p>
</div>


StudioFS is an open-source, self-hostable virtual filesystem designed for
computer graphics, animation, VFX, and post-production studios. It lets teams
access and edit heavy source assets (4K, RAW, proxies, geometry, caches)
through a local mount, fetching remote payloads on demand and publishing local
changes in the background.

- **Mounted access to heavy media**
  Work directly with proxies, 4K, RAW footage, and other large assets from the
  cloud through filesystem reads. Cold reads still depend on network and
  object-storage latency; cached reads use local resources.

- **A shared workspace with eventual convergence**
  Clients exchange ordered metadata changes through PostgreSQL. Local edits
  become visible to peers after payload upload, metadata publication, and peer
  replay; clients can temporarily see different versions.

- **Filesystem access for existing tools**
  A StudioFS volume mounts through the operating system's filesystem bridge.
  Applications can use ordinary file paths without a StudioFS-specific plugin;
  compatibility with individual DCC applications requires workload validation.

- **Built with Rust**
  A Rust workspace implements the filesystem core, local cache, replication,
  and platform bridges. Mounted benchmarks measure workload-specific performance.

- **Self-hosted and open**
  Unlike proprietary SaaS alternatives, StudioFS is **100% open-source and
  self-hosted**, giving you control over your storage, metadata, and
  infrastructure.

- **Cat logo**
  Because infrastructure is better with a little personality. 🐱



### ⚠️ Caution

> [!WARNING]
> Project is in its early stages. Breaking changes will happen.
> Use with caution.



## User Guide

- [Українська — посібник користувача](docs/user-guide/uk/README.md) · [швидкий старт](docs/user-guide/uk/quick-start.md)
- [English — user guide](docs/user-guide/en/README.md) · [quick start](docs/user-guide/en/quick-start.md)

All 30 chapters are available in Ukrainian and English. The approved Ukrainian
version is the source of truth; the guide remains a work in progress.

## What Problem Does StudioFS Solve?

In a classic remote production pipeline, artists often need to download hundreds
of gigabytes of assets, wait for transfers to finish, relink paths manually, and
then upload changed files back to shared storage.

StudioFS is designed to remove that loop:

- **On-demand reads**
  StudioFS can read a file without downloading the whole file first. Fetches
  operate on payload fragments or authenticated frame ranges, and readahead can
  fetch more than the application's requested bytes.

- **Background upload**
  Saves are accepted locally first, so the artist can keep working. StudioFS then
  uploads the new data to S3-compatible storage in the background.

- **Freedom of deployment**
  Run the system on your own infrastructure with an S3-compatible storage
  backend, such as AWS S3, RustFS, or Backblaze B2, plus PostgreSQL for shared
  metadata coordination.

- **Remote payload encryption at rest**
  StudioFS encrypts remote payload fragments with XChaCha20-Poly1305 and
  decrypts them on clients. Volume keys are managed automatically in metadata;
  local caches and metadata remain plaintext. This protects the object-storage
  boundary, not a compromised metadata service or workstation. Transport
  encryption is configured separately; see the
  [encryption-at-rest contract](docs/encryption-at-rest-contract.md) and
  [transport contract](docs/transport-encryption-contract.md).

## How It Works

StudioFS combines S3-compatible object storage with local caching and
PostgreSQL-backed metadata coordination:

- **Cloud storage holds the assets**
  Published file payloads are stored as immutable encrypted fragments in
  S3-compatible storage.

- **PostgreSQL coordinates the team**
  PostgreSQL stores shared metadata, orders changes between clients, and helps
  clients discover file additions, deletes, renames, snapshots, and other
  metadata updates.

- **Local cache keeps active work fast**
  StudioFS caches fetched payloads and stages locally written data. Reads may
  fetch complete fragments, use authenticated partial reads, or trigger
  prefetch; cache usage is not limited to the exact bytes requested by an app.

- **Local-first runtime keeps the mount responsive**
  Interactive filesystem operations use local state first. Metadata lives in the
  embedded `redb` database, written data becomes visible locally immediately,
  and background workers upload payloads and replicate metadata through
  PostgreSQL. Shared file-lock coordination is an explicit exception that may
  require foreground PostgreSQL access.

## Technical Foundation

StudioFS mounts S3-compatible object storage as a local cross-platform drive,
using PostgreSQL for shared metadata storage and replication between clients.

It is built around a local-first runtime model:

- foreground filesystem operations work from local state,
- file payloads are stored in immutable S3 objects,
- shared metadata converges through PostgreSQL background sync,
- RAM and disk caches reduce repeated remote reads.

## Current Architecture

```
StudioFS workspace
  ├── studiofs              CLI and runtime assembly
  ├── studiofs-meta         local metadata + PostgreSQL delta sync
  ├── studiofs-data         S3 object I/O + object format
  ├── studiofs-cache        local payload backend + upload queue
  ├── studiofs-vfs          filesystem core
  ├── studiofs-bridge       shared platform-neutral bridge helpers
  ├── studiofs-linux-fuse   Linux libfuse3 low-level bridge
  ├── studiofs-macfuse      macOS macFUSE bridge (legacy or opt-in FSKit)
  ├── studiofs-winfsp       Windows bridge
  ├── studiofs-e2e          real mounted E2E runner
  ├── studiofs-scenario     shared scenario model and simulation
  └── studiofs-test-support shared test helpers
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
- Chunk/slice writer buffers, local staging, and recovery of locally persisted state
- Cross-platform mounts: WinFSP on Windows, libfuse3 on Linux, macFUSE on macOS
  (legacy backend by default, opt-in FSKit backend)
- Background upload, janitor, and metadata sync workers
- Snapshot v2 PostgreSQL control plane with retained, version-bounded sources
- Snapshot source replay is thin: serving checkpoints and bounded event ranges
  are retained without replicating a full local snapshot through normal metadata
  state or event payloads

Local acceptance, local durability, and peer visibility are separate boundaries.
Successful `write` makes bytes readable on the writing mount but can leave them
only in memory. Ordinary flush and final release can return before the local
metadata commit. `fsync` drains local state; Durable cache mode syncs payloads
for local crash recovery, while Fast mode does not force payload fsync/dirsync.
None of these operations waits for peer publication. See the
[mounted-IO contract](docs/juicefs-local-first-rust-contract.md) for the exact
durability and recovery rules.

## Quick Start

Install the platform mount prerequisites first: libfuse3 on Linux, macFUSE on
macOS, or WinFSP on Windows. See [development setup](docs/development.md).

Initialize a new volume (Bash example, with local S3-compatible storage and
PostgreSQL already running; replace these development credentials as needed):

```bash
studiofs init myvolume \
  --endpoint http://127.0.0.1:9000 \
  --bucket studiofs-test \
  --access-key studiofs \
  --secret-key studiofs123 \
  --meta postgres://studiofs:studiofs@127.0.0.1:5432
```

Mount it:

```bash
studiofs mount myvolume Z:
studiofs mount myvolume /mnt/studio/myvolume
```

Choose the command for your platform: a drive letter on Windows or a directory
on Linux/macOS. A directory base path that does not end in the volume name has
the volume name appended automatically.

Linux, macOS, and Windows use the shared local-first runtime path for mounts.
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
| `smount` | Mount a Ready snapshot as a read-only filesystem |
| `umount` | Unmount an active mountpoint |
| `status` | Show volume status, PostgreSQL binding, and local/runtime counters |
| `settings` | Show or update persistent client settings and runtime tuning |
| `snapshot` | Create, list, inspect, delete, and schedule whole-volume snapshots |
| `janitor` | Scan and delete orphaned remote objects; `--dry-run` only reports candidates |
| `fsck` | Check and optionally repair dangling entries, orphan inodes, link counts, and pending/deferred state |
| `dump` | Sync local metadata to PostgreSQL, then export a metadata dump |
| `load` | Import a metadata dump into PostgreSQL |
| `perf` | Watch live runtime stats from a mounted StudioFS volume |
| `bench` | Run the fixed public mounted filesystem benchmark |
| `devbench` | Run a configurable developer mounted filesystem benchmark |
| `test` | Collect a mounted write-path test report |
| `destroy` | Permanently remove local metadata and remote data/metadata; `--local` keeps remote state |

`dump` and `load` move a volume's metadata to a new PostgreSQL metadata server;
the S3 payloads stay where they are:

1. Unmount the volume on every client.
2. Run `studiofs dump <volume> --path-to-dump <file>`.
3. Stop the old metadata server and keep it stopped. Two metadata servers for
   the same volume would both garbage-collect the same S3 payloads.
4. Run `studiofs load --path-to-dump <file> --postgresql-address <new server>`.
5. Run `studiofs discover <volume> --meta <new server>` on each client.

The dump does not carry Snapshot v2 state, object reachability accounting, or
the payload upload/deletion ledger. `load` therefore refuses a target that
already has any of these rows for the dumped volume UUID, including completed
upload rows, and names the blocking tables. Load into a new, empty metadata
database. `destroy` permanently deletes the volume, including its S3 payloads,
so a metadata dump cannot recover a destroyed volume.

Run `studiofs --help` or `studiofs <command> --help` for full usage.

CLI size options such as `--cache-size` and `--read-buffer` accept `B`, `KB`,
`MB`, `GB`, `TB`, `PB`, `EB` suffixes in binary units (`1KB = 1024B`) and
fractional values such as `1.5GB`. The shared parser truncates fractional bytes;
`--read-buffer` then rounds up to whole MiB. The same size syntax applies to
these mount environment overrides: `STUDIOFS_WRITER_PAGE_SIZE_BYTES`,
`STUDIOFS_XATTR_MAX_BYTES`, `STUDIOFS_DENTRY_CACHE_BYTES`,
`STUDIOFS_PATH_SHORTCUT_CACHE_BYTES`, and `STUDIOFS_ATTR_READ_CACHE_BYTES`.
Other byte-count variables, including PostgreSQL sync/checkpoint limits, may
require integer bytes; see [configuration](docs/configuration.md).

Benchmark and statistics tooling uses the mounted filesystem path. Use
`studiofs perf` for live runtime counters, `studiofs bench` for the fixed public
mounted benchmark, and `studiofs devbench` when you need configurable developer
workload flags. See
[`docs/perf/benchmark-methodology.md`](docs/perf/benchmark-methodology.md) for
the comparison methodology and known limits.

## Development

Build (Rust 1.95 or newer; see the platform dependencies in
[development setup](docs/development.md)):

```bash
cargo build --release
```

Checks:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -W missing-docs
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace
```

Workspace lint policy is inherited from the root `[workspace.lints]` tables.
Member crates opt in with `[lints] workspace = true`; keep new shared lints
low-noise and stage broad hygiene migrations separately.

Workspace tests (excluding ignored tests; some tests require local services):

```bash
cargo test --workspace --all-targets -- --nocapture
```

The main RWX Linux lanes and hosted macOS/Windows verification workflow are
manually triggered. The focused
[payload-read regression workflow](.github/workflows/payload-read-regression.yml)
also runs automatically on matching pull requests. See
[`.github/workflows/rust.yml`](.github/workflows/rust.yml),
[`docs/development.md`](docs/development.md), and
[`docs/contributing.md`](docs/contributing.md) for the full verification
matrix (Quality Gates, Linux, macOS, Windows) and the contributor rules for
platform-specific changes. macOS and Windows jobs include explicit named steps
for their backend crates (`studiofs-macfuse` on macOS, `studiofs-winfsp` on
Windows) so regressions in platform-specific code are immediately attributable
in the CI log. Linux mounts use the `studiofs-linux-fuse` libfuse3 low-level
bridge. RWX mounted E2E targets run on Ubuntu; the Rocky lane runs workspace tests.

Infra-heavy ignored suite:

```bash
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

Real mounted E2E runner (build the product binary first and select a scenario
from `list`):

```bash
cargo build -p studiofs
cargo run -p studiofs-e2e -- list
cargo run -p studiofs-e2e -- run --scenario basic-multi-client-smoke --studiofs-bin target/debug/studiofs
cargo run -p studiofs-e2e -- clean
```

On Windows, use `target/debug/studiofs.exe` for `--studiofs-bin`.

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

### User documentation

The practical user manual lives separately in [docs/user-guide/](docs/user-guide/README.md):
[Українська](docs/user-guide/uk/README.md) · [English](docs/user-guide/en/README.md).
Quick start: [Українська](docs/user-guide/uk/quick-start.md) · [English](docs/user-guide/en/quick-start.md).

### Technical contracts and developer reference

The existing materials below describe implementation contracts, architecture,
configuration reference, and development workflows. They are separate from the
user manual; their existing paths remain stable:

- [docs/roadmap.md](docs/roadmap.md) - future development plans
- [docs/examples.md](docs/examples.md) - usage variants and examples
- [docs/architecture.md](docs/architecture.md) - current system design and data flow
- [docs/juicefs-local-first-rust-contract.md](docs/juicefs-local-first-rust-contract.md) - canonical mounted-IO and local durability contract
- [docs/configuration.md](docs/configuration.md) - CLI commands, flags, env vars
- [docs/tray.md](docs/tray.md) - tray colors, remote synchronization and shutdown status
- [docs/development.md](docs/development.md) - local setup, build, and test workflow
- [docs/contributing.md](docs/contributing.md) - verification matrix, CI ownership, platform-specific code rules
- [docs/tests.md](docs/tests.md) - test matrix and infra expectations
- [docs/filesystem-testing-overview.md](docs/filesystem-testing-overview.md) - friendly overview of the real filesystem testing approach
- [docs/e2e-runner-contract.md](docs/e2e-runner-contract.md) - real mounted E2E runner contract, artifacts, reports, preflight, and cleanup rules
- [docs/metadata.md](docs/metadata.md) - local `redb` + PostgreSQL metadata model
- [docs/pg-sync-contract.md](docs/pg-sync-contract.md) - canonical PostgreSQL bootstrap, catch-up, lifecycle, and authority contract
- [docs/cache.md](docs/cache.md) - local cache backend, staged writeback, and upload flow
- [docs/vfs.md](docs/vfs.md) - filesystem runtime behavior
- [docs/data-layer.md](docs/data-layer.md) - S3 object I/O and format details
- [docs/encryption-at-rest-contract.md](docs/encryption-at-rest-contract.md) - remote payload encryption and plaintext local boundaries
- [docs/transport-encryption-contract.md](docs/transport-encryption-contract.md) - S3 and PostgreSQL transport policy
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

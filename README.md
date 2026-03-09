# StudioFS

**Cross-platform virtual filesystem that mounts S3-compatible object storage as a local drive.**

StudioFS is designed for serverless and distributed deployments where multiple clients can mount the same volume simultaneously without a central metadata server.

## Key Features

- **S3-native storage** — data stored as immutable LZ4-compressed objects in any S3-compatible backend
- **Distributed metadata** — Tonbo manifest with ETag-based CAS synchronization; no central server
- **Two-level cache** — L1 (RAM) + L2 (NVMe) block cache with unified LRU and crash-safe writeback journal
- **Adaptive I/O** — AIMD congestion control for S3 fetches, sequential readahead, singleflight dedup
- **Cross-platform** — Windows (WinFSP), Linux/macOS (FUSE)
- **Multi-client** — optional NATS invalidation bus, distributed janitor with lease-based leader election

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    studiofs (CLI)                   │
├────────────────────┬────────────────────────────────┤
│   studiofs-winfsp  │  studiofs-fuse                 │
│   (Windows)        │  (Linux / macOS)               │
├────────────────────┴────────────────────────────────┤
│                   studiofs-vfs                      │
│        read/write paths · readahead · AIMD          │
├──────────┬──────────────────┬───────────────────────┤
│ studiofs │   studiofs-data  │    studiofs-cache     │
│   -meta  │    (S3 I/O)      │  L1 RAM · L2 NVMe     │
│  redb +  │   OpenDAL + LZ4  │  LRU · upload queue   │
│  Tonbo   │                  │                       │
└──────────┴──────────────────┴───────────────────────┘
```

## Quick Start

```bash
# Initialize a volume (MinIO example)
studiofs init mydisk \
  --endpoint http://127.0.0.1:9000 \
  --bucket data-bucket \
  --access-key minioadmin \
  --secret-key minioadmin \
  --metadata-same-as-data true

# Mount
studiofs mount mydisk Z:          # Windows
studiofs mount mydisk /mnt/mydisk # Linux/macOS
```

## Commands

| Command | Description |
|---------|-------------|
| `init` | Create a new volume and store connection settings |
| `discover` | Attach to an existing Tonbo volume |
| `mount` | Mount a volume as a local drive |
| `status` | Show volume info and counters |
| `tune-metadata` | Adjust cache TTL and flush thresholds |
| `janitor` | Manual orphan object cleanup |
| `test` | Validate S3 compatibility |
| `destroy` | Delete volume (remote + local or local-only) |

Run `studiofs --help` or `studiofs <command> --help` for full usage.

## Documentation

Detailed documentation is available in the [docs/](docs/) directory:

- [Architecture Overview](docs/architecture.md) — system design, crate graph, data flows
- [Cache System](docs/cache.md) — L1/L2 cache, LRU, writeback, upload queue
- [Metadata Engine](docs/metadata.md) — redb, Tonbo sync, schema, events
- [VFS Layer](docs/vfs.md) — read/write paths, readahead, AIMD, distributed locks
- [Data Layer](docs/data-layer.md) — S3 object format, compression, block layout
- [Platform Bridges](docs/platform-bridges.md) — WinFSP and FUSE integration
- [Configuration Reference](docs/configuration.md) — all CLI options, env vars, tuning
- [Development Guide](docs/development.md) — build, test, local setup
- [Tests Guide](docs/tests.md) — full test matrix, commands, infra setup, troubleshooting

## Co-developed with LLMs

This project was co-developed with LLMs such as Claude, Codex, etc. Human maintainers are responsible for their contributions regardless of machine help.

## License

GPL-3.0

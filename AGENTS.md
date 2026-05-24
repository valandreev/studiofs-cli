# StudioFS

StudioFS is a Rust 2024 workspace for a cross-platform local-first virtual filesystem over S3-compatible object storage with PostgreSQL-based shared metadata.

## Always-on project rules

- This repository targets Rust and uses `cargo` as the primary toolchain.
- For Rust-related repository work, always apply the [`rust-router`](C:\Users\valen\.agents\skills\rust-router\SKILL.md) skill workflow first.
- All Linear items and notes for this repository are written in English.
- For PR readiness, include all relevant file changes in the commit set (code + related docs/config updates), even if a file was not directly edited for the task.
- This repo follows `docs/juicefs-local-first-rust-contract.md`. In case of conflict, the contract is the source of truth.
- Prefer focused validation for local work; run broad full-suite commands only when requested.
- All temporary files should be in `target/` and should be cleaned up after use.

## Canonical entry points

- [Project conventions and architecture](docs/architecture.md)
- [Rust conventions and local-first implementation contracts](docs/agents-coding-guide.md)
- [Local-first invariants and callback contracts](docs/local-first-validation-matrix.md)
- [Platform bridge contract (`FUSE` + `WinFSP`)](docs/platform-bridges.md)
- [Build and developer setup](docs/development.md)
- [Contributing and PR requirements](docs/contributing.md)
- [Test commands and test-layer guidance](docs/tests.md)
- [Configuration reference](docs/configuration.md)

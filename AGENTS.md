# Repository Guidelines

## Project Structure & Module Organization
StudioFS is a Rust workspace (`Cargo.toml`) with crates under `crates/`:
- `studiofs`: CLI entrypoint and subcommands (`src/cli/*.rs`)
- `studiofs-meta`: metadata engine (redb + PostgreSQL sync)
- `studiofs-data`: S3 object I/O and block/object formats
- `studiofs-cache`: L1/L2 cache, writeback, upload queue
- `studiofs-vfs`: platform-agnostic filesystem core
- `studiofs-fuse` and `studiofs-winfsp`: Linux/macOS and Windows bridges

Documentation lives in `docs/` (architecture, config, tests, development). Integration tests are primarily in `crates/studiofs-data/tests/` and `crates/studiofs-vfs/tests/`; many crate-level unit tests are inline in `src/*.rs`.

## Build, Test, and Development Commands
- `cargo build --release`: production build for all workspace crates.
- `cargo test --workspace --all-targets -- --nocapture`: fast local suite.
- `cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1`: integration/infra-heavy suite (MinIO/PostgreSQL).
- `cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1`: Postgres-focused regression run.
- `cargo run -p studiofs -- --help`: inspect CLI usage locally.
- `cp .env.example .env` (or PowerShell equivalent): seed local test/runtime config.

## Coding Style & Naming Conventions
Use Rust 2024 edition conventions and keep code `rustfmt`-clean (`cargo fmt`). Prefer `cargo check --workspace` before pushing; run `cargo clippy --workspace --all-targets` for lint feedback. Follow existing naming:
- files/modules: `snake_case` (`tune_metadata.rs`)
- types/traits: `PascalCase` (`StudioVfs`)
- constants/env keys: `UPPER_SNAKE_CASE`

Keep platform-specific logic in `studiofs-fuse`/`studiofs-winfsp`; avoid leaking OS-specific code into shared crates.

## Testing Guidelines
Use `#[test]`/`#[tokio::test]` for unit and async tests. Keep external-service tests marked `#[ignore]` and runnable on demand. Match existing naming filters when useful (for example `test_pg_`). For storage, cache, or invalidation changes, run both fast and ignored suites for affected crates.

## Commit & Pull Request Guidelines
Current history favors short, direct subjects (for example: `io path overhaul`, `small linux fixes`, `pg delta sync`). Keep commit titles concise, imperative, and scoped to one change. In PRs, include:
- what changed and why
- crates/docs touched
- exact test commands run
- infra assumptions (MinIO/PostgreSQL, env vars)

## Security & Configuration Tips
Never commit `.env` or real credentials. Use `.env.example` defaults for local development, and sanitize endpoints/keys in logs or shared repro steps.

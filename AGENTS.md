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
- `cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1`: integration/infra-heavy suite (RustFS/S3 + PostgreSQL).
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
- infra assumptions (RustFS/S3 + PostgreSQL, env vars)

## Security & Configuration Tips
Never commit `.env` or real credentials. Use `.env.example` defaults for local development, and sanitize endpoints/keys in logs or shared repro steps.


## vexp (mandatory policy)

### 1) Hard rule

- In this repo, every code/context task starts with vexp MCP.
- Use `run_pipeline` first for discovery, exploration, impact and edit planning.
- If `run_pipeline` (or other vexp tools) is unavailable in the current environment, explicitly report it and continue with the best available fallback.

### 2) Setup and index health checks

Before any code changes:
- verify vexp workspace presence:
  - `.vexp/manifest.json` (project metadata)
  - `.vexp/workspace.json` (if multi-repo is used)
  - `.vexp/index.db` (project index, local cache)
- if any critical file is missing, report:
  - `vexp is not configured in this repo`
  - and ask to run `vexp: Setup Workspace` in the editor (preferred) or `vexp setup .`.
- re-check status with `index_status` and force re-index with `vexp index .` when stale.

### 3) Mandatory workflow

1. `run_pipeline({ task: "..." })`
2. target edits by returned context
3. validate impact with `get_impact_graph` / `search_logic_flow` if needed
4. if symbols changed or architecture impact is unclear, run second-context pass.

### 4) Cross-platform sync policy (critical)

- `vexp` index is machine-local and must **not** be synced:
  - `.vexp/index.db`
  - `.vexp/index.db-shm`
  - `.vexp/index.db-wal`
  - `.vexp/daemon.pid`
  - `.vexp/healthy`
- Keep these repo-level files for portability:
  - `.vexp/manifest.json`
  - `.vexp/workspace.json` (if used)
- This repo's `.gitignore` must include local vexp index artifacts.

### 5) New project AGENTS template

For a new repository, include this block in `AGENTS.md`:

```markdown
## vexp (mandatory policy)

### 1) Hard rule
- Start repo work with `run_pipeline`.
- Do not use grep/file walking as primary exploration when vexp is available.

### 2) Setup and health
- check `.vexp/manifest.json`, `.vexp/workspace.json` (if any), `.vexp/index.db`
- if missing: run `vexp: Setup Workspace` or `vexp setup .`

### 3) Sync policy
- do not commit `.vexp/index.db*`
- keep `.vexp/manifest.json` and `.vexp/workspace.json` in git when needed

### 4) Tools order
- `run_pipeline` → targeted changes → optional `get_impact_graph` / `search_logic_flow`
```

### Available vexp MCP tools
- `run_pipeline`
- `get_context_capsule`
- `get_impact_graph`
- `search_logic_flow`
- `get_skeleton`
- `index_status`
- `get_session_context`
- `search_memory`
- `save_observation`
<!-- /vexp -->

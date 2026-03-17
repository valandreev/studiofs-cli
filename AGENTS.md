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


## vexp <!-- vexp v1.0.0 -->

**MANDATORY: always use `run_pipeline` first for any repo analysis or edits — do NOT grep or glob the codebase manually.**

Install + setup:
```bash
npm install -g vexp-cli@latest
cd /Volumes/Storage/dev/studiofs
vexp setup --personal . --no-index
```
If you want to re-index after installation:
```bash
cd /Volumes/Storage/dev/studiofs && vexp index .
```

Hard rule:
- Every task that touches code context in this repo must start with `run_pipeline({ task: "..." })`.
- If `run_pipeline` is unavailable, proceed with the best available tools and document the limitation explicitly.
vexp returns pre-indexed, graph-ranked context in a single call.

### Workflow
1. `run_pipeline` with your task description — ALWAYS FIRST (replaces all other tools)
2. Make targeted changes based on the context returned
3. `run_pipeline` again only if you need more context

### Available MCP tools
- `run_pipeline` — **PRIMARY TOOL**. Runs capsule + impact + memory in 1 call.
  Auto-detects intent. Includes file content. Example: `run_pipeline({ "task": "fix auth bug" })`
- `get_context_capsule` — lightweight, for simple questions only
- `get_impact_graph` — impact analysis of a specific symbol
- `search_logic_flow` — execution paths between functions
- `get_skeleton` — compact file structure
- `index_status` — indexing status
- `get_session_context` — recall observations from sessions
- `search_memory` — cross-session search
- `save_observation` — persist insights (prefer run_pipeline's observation param)

### Agentic search
- Do NOT use built-in file search, grep, or codebase indexing — always call `run_pipeline` first
- If you spawn sub-agents or background tasks, pass them the context from `run_pipeline`
  rather than letting them search the codebase independently

### Smart Features
Intent auto-detection, hybrid ranking, session memory, auto-expanding budget.

### Multi-Repo
`run_pipeline` auto-queries all indexed repos. Use `repos: ["alias"]` to scope. Run `index_status` to see aliases.
<!-- /vexp -->

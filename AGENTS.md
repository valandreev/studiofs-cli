# StudioFS agent contract

StudioFS is a Rust 2024 workspace for a local-first filesystem over
S3-compatible object storage with PostgreSQL-backed shared metadata. The mounted
foreground path must remain autonomous: local `redb` state serves the mount,
while PostgreSQL and object storage provide bootstrap, replication, and payload
publication.

## Instruction precedence and scope

- User instructions take precedence over skills and this file. A nested
  `AGENTS.md` is more specific than this file for work below its directory.
- Read the task-relevant contract and ADRs before editing. If instructions or
  skills conflict, name the exact source and follow the higher-priority rule.
- Start by recording the repository root, current branch, and `git status`.
  Preserve unrelated user changes; never use `git reset --hard` or destructive
  checkout operations without explicit authorization.
- Treat an action request as authorization to carry the work through to a
  reviewable result. Ask a question only for a genuine blocker or an
  irreversible external action after the reversible preparation is complete.

## Task workflow

1. Start from the supplied issue, artifact, failing test, commit, or review
   comment. Trace the actual code/event path before proposing a fix.
2. Use an ExecPlan for a significant feature, refactor, or multi-crate change;
   follow `.agent/PLANS.md` and keep the plan living. Small documentation or
   one-file fixes can use the same loop without a new plan.
3. Make the smallest behaviorally complete change. Preserve existing commits,
   public contracts, security boundaries, and platform-specific behavior.
4. Validate the changed behavior with focused, nonzero tests and inspect the
   final diff. Report exactly what was run and what remains environment-gated.

## Discovery and external references

Use the Codebase Memory MCP first for code structure in project
`E-dev-studiofs-cli`:

1. `search_graph` finds symbols and exact qualified names.
2. `trace_path` finds callers and callees.
3. `get_code_snippet` reads a symbol after discovery.
4. `query_graph` handles multi-hop questions; `search_code` adds graph context.
5. `check_index_coverage`/`index_status` establish coverage before relying on
   a graph result. Parse-partial and excluded files require direct inspection.

Use `rg`/PowerShell for literals, logs, configuration, Markdown, scripts, and
any graph-missed range. The graph is a discovery aid, never proof of exhaustive
absence. Use Context7 for current third-party library documentation and `gh`
for GitHub state. Use the configured Linear integration for issue operations;
Linear issues and GitHub comments are written in English.

Read these entry points as needed:

- `docs/juicefs-local-first-rust-contract.md` — canonical mounted-IO contract
- `docs/architecture.md` — system boundaries and data flow
- `docs/agents-coding-guide.md` — detailed Rust and lifecycle rules
- `docs/local-first-validation-matrix.md` — invariant-to-test mapping
- `docs/platform-bridges.md` — FUSE/WinFSP adapter contract
- `docs/development.md`, `docs/tests.md`, `docs/contributing.md` — commands and gates
- `docs/agents/domain.md`, `docs/agents/issue-tracker.md` — domain and Linear rules

## Architecture invariants

- Dependency direction is `domain <- ports <- application <- adapters/runtime/API`.
- Foreground metadata operations use the local `redb` view. PostgreSQL is not a
  foreground read dependency; it bootstraps and replicates shared state.
- Local writer state can be newer than peer-visible state. Foreign apply must
  not overwrite newer local authority state.
- Publish invalidation/intents after the local commit, retain old namespace
  identity before rename/unlink replacement, and never recache a result that
  crossed invalidation.
- `write`, `flush`, `fsync`, and handle release have distinct local durability
  contracts. Do not add synchronous remote publication to callbacks.
- Credentials, full DSNs, tokens, and secret-bearing configuration must not
  appear in `Debug`, rendered errors, logs, plans, commits, or final reports.

## Rust, concurrency, and platform work

- Use the `rust-best-practices` skill for Rust changes and review. Keep workspace
  lints in the root `Cargo.toml`; use `#[expect(..., reason = "...")]` for
  justified production exceptions.
- Prefer typed `Result` errors, narrow ownership, and explicit async lifecycle.
  Join or otherwise own background tasks before releasing resources they retain.
- Follow the lock order implemented and documented by
  `crates/studiofs-vfs/src/lock_order.rs`. For races, synchronize on the actual
  condition with a bounded wait; never use sleeps or retry loops as proof.
- Use the narrowest `cfg` guard. Unit/compile checks do not prove a live FUSE or
  WinFSP mount, and a focused E2E runner check does not prove the full scenario
  gate. Preserve artifacts and state the unavailable prerequisite.
- Load `.env` only when the requested test needs it; import values without
  printing the file or secrets. Put temporary artifacts under `target/`.

## Validation and delivery

- Run the smallest relevant `cargo test -p ...`, `cargo check -p ...`, and
  `cargo clippy -p ... --all-targets -- -D warnings` set. Formatting and
  `git diff --check` are appropriate for every code or documentation change.
- Never run workspace-wide or full ignored suites unless the user explicitly
  requests them. A result with `running 0 tests` is invalid evidence.
- Rebuild the product binary before counting a mounted E2E rerun. Use the
  registered `studiofs-e2e` scenario runner, bounded observable-state polling,
  retained diagnostics, and no fixed coordination sleeps.
- Before a PR, commit every intended task change, including related docs/config
  updates. Stage only the task's files and preserve unrelated worktree changes.
  Use `<type>: <subject>` commits (`docs`, `fix`, `test`, `refactor`, `perf`,
  `ci`, or `feat`). Do not create draft PRs unless asked.

## Code Review Rules

- Verify every recommendation against the current code and contract. For a
  confirmed race or lifecycle claim, add a focused red regression before the
  minimal correction when practical.
- Comment on behavior, correctness, security, lifecycle, determinism, and
  missing validation. Reject incorrect suggestions with concrete evidence and
  call purely cosmetic or out-of-scope suggestions skipped.

Scoped contracts are loaded automatically from `crates/*/AGENTS.md` for the VFS,
metadata, cache, WinFSP, and mounted E2E areas.

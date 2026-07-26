# StudioFS

StudioFS is a Rust 2024 workspace for a cross-platform local-first virtual filesystem over S3-compatible object storage with PostgreSQL-based shared metadata.
We are building a file system for a world where computers are constantly connecting and disconnecting, operating from different cities and countries, have varying internet connections, and cannot view the same data in perfect synchronisation. Therefore, each computer must be sufficiently autonomous. It operates locally, synchronises with the overall system, but does not depend directly on any other computer.

# ExecPlans

When writing complex features or significant refactors, use an ExecPlan (as described in .agent/PLANS.md) from design to implementation.

## Always-on project rules

- This repository targets Rust and uses `cargo` as the primary toolchain.
- Use rust-best-practices skill
- For PR readiness, include all relevant file changes in the commit set (code + related docs/config updates), even if a file was not directly edited for the task.
- This repo follows `docs/juicefs-local-first-rust-contract.md`. In case of conflict, the contract is the source of truth.
- Prefer focused validation for local work; run broad full-suite commands only when requested.
- All temporary files should be in `target/` and should be cleaned up after use.

## Temporal Awareness and Efficiency

- Treat elapsed time and tokens as finite engineering budgets.
- Record the task start time and compare actual elapsed time against the estimate at meaningful checkpoints.
- Keep progress updates short, factual, and evidence-based. A status report is never a pause or approval gate while work remains possible.
- Identify the critical path immediately. Execute its next blocking step locally and delegate independent, bounded work in parallel.
- Use lower-effort agents only for simple, clearly scoped tasks. Use stronger agents for security-sensitive, architectural, or operational work.
- Do not repeat exploration, builds, downloads, tests, restarts, or deployments unless new evidence makes repetition necessary.
- Run the smallest verification set that proves the changed behavior and protects the affected risk surface. Expand it only when failures or blast radius justify expansion.
- When elapsed time exceeds the estimate, reassess the approach immediately. Change strategy, reduce nonessential scope, or parallelize instead of continuing an unproductive loop.
- Prefer the shortest correct path through implementation, focused verification, leak checks, commit, push, release, and deployment.
- Never trade correctness, security, deterministic behavior, or data integrity for speed.
- Report a blocker only when it is genuinely external or impossible to resolve autonomously; otherwise continue working.

<!-- codebase-memory-mcp:start -->
## Codebase Knowledge Graph (codebase-memory-mcp)

This checkout uses codebase-memory-mcp. Prefer graph tools over grep/glob for
code discovery; use grep only for string literals, logs, config values, and
non-code files.

Discovery order:

1. `search_graph` - find symbols by natural-language query, regex name, label,
   or file pattern.
2. `trace_path` - trace callers/callees for exact symbol names.
3. `get_code_snippet` - read source for a qualified name found by
   `search_graph`.
4. `query_graph` - run Cypher-like queries for complex relationships.
5. `search_code` - graph-augmented text search inside indexed files.

Examples:

- Find a symbol:
  `search_graph(project="Volumes-Storage-dev-studiofs", name_pattern=".*StudioVfsAdapter.*", limit=10)`
- Read source:
  `get_code_snippet(project="Volumes-Storage-dev-studiofs", qualified_name="Volumes-Storage-dev-studiofs.crates.studiofs-macfuse.src.adapter.StudioVfsAdapter")`
- Find callers/callees:
  `trace_path(project="Volumes-Storage-dev-studiofs", function_name="read_into_fh", direction="both", mode="calls", include_tests=true)`
- Query graph directly:
  `MATCH (f:Function)-[:CALLS]->(g:Function) WHERE g.name = 'read_into_fh' RETURN f.qualified_name, g.qualified_name LIMIT 20`
- Text search with graph context:
  `search_code(project="Volumes-Storage-dev-studiofs", pattern="deferred_release_inflight", path_filter="crates/studiofs-vfs/src/.*\\.rs$", limit=20)`

Notes:

- Use `get_graph_schema`; stored procedures such as `CALL db.labels()` are not
  supported by this MCP query engine.
- Common labels in this repo include `Function`, `Method`, `Class`, `Field`,
  `Enum`, `Interface`, `File`, `Module`, `Route`, and `EnvVar`.
- Common code-node properties include `name`, `qualified_name`, `file_path`,
  `start_line`, `end_line`, `is_test`, `is_exported`, `complexity`, and
  `signature`.
<!-- codebase-memory-mcp:end -->

## Canonical entry points

- [Project conventions and architecture](docs/architecture.md)
- [Rust conventions and local-first implementation contracts](docs/agents-coding-guide.md)
- [Local-first invariants and callback contracts](docs/local-first-validation-matrix.md)
- [Platform bridge contract (`FUSE` + `WinFSP`)](docs/platform-bridges.md)
- [Build and developer setup](docs/development.md)
- [Contributing and PR requirements](docs/contributing.md)
- [Test commands and test-layer guidance](docs/tests.md)
- [Configuration reference](docs/configuration.md)

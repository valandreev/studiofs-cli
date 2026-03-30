# Codebase Memory MCP Skills

The workspace uses **codebase-memory-mcp** (v0.5.5) for intelligent code exploration and analysis. 

### When to invoke codebase-memory skills:

**Invoke `codebase-memory-exploring` when:**
- User asks to "find function named...", "where is...", "search for..."
- User explores architecture, asks "what modules exist", "show me the structure"
- User asks to "search the codebase", "find all usages"
- Keywords: find, search, locate, where, structure, architecture, modules, components, functions, classes

**Invoke `codebase-memory-quality` when:**
- Code review, analyzing code patterns, checking consistency
- Looking for potential issues, smells, improvements
- Understanding code relationships and dependencies
- Keywords: review, quality, patterns, dependencies, relationships, consistency

**Invoke `codebase-memory-reference` when:**
- User needs context about symbols, types, or APIs
- Understanding how modules work together
- API documentation or reference lookups
- Keywords: what is, explain, reference, how does, API, interface

**Invoke `codebase-memory-tracing` when:**
- Tracing execution flow, understanding call chains
- Debugging complex flows, understanding dependencies
- "How does X get called?", "What calls this function?"
- Keywords: trace, flow, call chain, execution, how it works, dependency chain

### Available MCP Tools (used by skills)

- `search_graph(name_pattern, label, file_pattern)` — Find symbols by pattern
- `get_code_snippet(symbol_path)` — Read function/type implementations
- `get_architecture()` — High-level project structure
- `trace_call_path(from_symbol, to_symbol)` — Execution path analysis
- `get_graph_schema()` — Understand available node/edge types

### Project Structure (StudioFS)

```
studiofs-cli/
├── crates/
│   ├── studiofs/          # CLI binary (main.rs, mount/init/discover/status commands)
│   ├── studiofs-meta/     # Metadata engine (PostgreSQL + Redb)
│   ├── studiofs-data/     # S3 object storage layer (OpenDAL)
│   ├── studiofs-cache/    # Block caching L1/L2 + upload journal
│   ├── studiofs-vfs/      # Virtual filesystem core (VfsState, inode mgmt)
│   ├── studiofs-fuse/     # Unix FUSE bridge
│   └── studiofs-winfsp/   # Windows WinFSP bridge
├── docs/                  # Architecture, design docs, roadmaps
│   └── done/              # Completed design docs (reference)
└── tests/                 # Integration tests
```

### Tips for Best Results

1. **Use specific patterns** — `search_graph(name_pattern=".*Mount.*")` for fuzzy file/symbol search
2. **Scope by file type** — `search_graph(file_pattern="*.rs", label="Function")` for Rust functions only
3. **Check architecture first** — Run `get_architecture()` before deep dives to understand module layout

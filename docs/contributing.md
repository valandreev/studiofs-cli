# Contributing

## vexp-first workflow (mandatory for repo-level analysis)

For repository exploration, planning, and code-impact changes, use the vexp MCP path as the primary analysis flow:

1. Run `run_pipeline` first in vexp for the task:
   - `run_pipeline({ "task": "your task description" })`
2. Use returned context for targeted changes.
3. Re-run `run_pipeline` only when additional focused context is required.

This project uses vexp in CI-compatible local mode; avoid manual grep/glob loops as the primary discovery mechanism.

## One-time/maintenance commands

- `npm install -g vexp-cli@latest`
- `npm run vexp:setup`
- `npm run vexp:index` (after big file changes)
- `npm run vexp:verify`

## VS Code task shortcuts

- `vexp:setup` — install and configure vexp for this repo.
- `vexp:index` — rebuild vexp index.
- `vexp:verify` — validate daemon status and capsule generation.
- `vexp:mcp` — start MCP server.
- `vexp:daemon-status` — quick status check.

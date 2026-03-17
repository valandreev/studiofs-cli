# Vexp MCP Setup and Validation

## One-time setup

```bash
npm install -g vexp-cli@latest
cd /Volumes/Storage/dev/studiofs
vexp setup --personal . --no-index
```

If you want to re-index after code changes:

```bash
vexp index .
```

## Runtime validation

1. Check indexed state

```bash
vexp daemon-cmd status
```

Expected output should include:
- index database path (`.vexp/index.db`)
- file/node/edge counts

2. Verify MCP server starts

```bash
vexp mcp >/tmp/vexp-mcp.log 2>&1 & pid=$!; sleep 1; kill -0 $pid && echo "MCP is running"; kill $pid
```

3. Verify context query works

```bash
vexp capsule "inspect project status" --max-tokens 1200
```

## Required workflow for this repo

- Always start repo exploration and refactor tasks with `run_pipeline(...)` in vexp MCP context (when available).
- Avoid manual grep/glob and manual indexing as primary analysis path.

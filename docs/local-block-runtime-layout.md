# Local Block Runtime Layout and Safety

This file documents the current local block runtime disk layout and safety
rules for the in-tree implementation. It is not the canonical SFS-242 rewrite
contract. The rewrite source of truth lives in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).

## Purpose

Define the current local block runtime disk layout, detection rules, startup
reconciliation behavior, and operator remediation for stale local cache state.

This document is the source of truth only for the current implementation
details and operator-facing safety expectations while the present runtime
remains in service.

## 1. Disk Layout (v1)

All local runtime data lives directly under the per-volume cache root.

```text
<cache-root>/
├── tmp/
├── payload/
│   └── blocks/
│       ├── 00/
│       ├── 01/
│       └── ff/
│           └── <object-id>.block
├── upload/
│   └── blocks/
│       ├── 00/
│       ├── 01/
│       └── ff/
│           └── <object-id>.stage
└── index/
    └── cache.redb
```

Notes:
- `index/cache.redb` is the fixed redb index location.
- Payload blocks are sharded into two levels (`00/00`..`ff/ff`) using a stable
  hash of `object_id`.

## 2. Mount and Status Detection Contract

Detection is read-only and must not mutate cache state.

- The current layout is detected from the runtime directories already present
  under `<cache-root>`.
- If no local contract exists yet, bootstrap provisions the runtime root
  before opening the backend.
- Non-contract files under `<cache-root>` are ignored by backend open logic.

## 3. Acceptance Matrix (Real Mount Path Only)

Use mounted-volume behavior as the acceptance gate. Internal microbenchmarks are
supporting signals only.

| Scenario | Workload and setup | Required outcome |
|---|---|---|
| Sequential write throughput | Mounted volume, large sequential writes, identical dataset/cache path | Meets or improves target throughput envelope |
| Sequential read throughput | Mounted volume, warm-cache sequential reads | Meets or improves target throughput envelope |
| Random read latency | Mounted volume, mixed-size random reads | Meets target p95/p99 envelope |
| Mixed read/write with uploads | Mounted volume under sustained upload pressure | No foreground-latency regressions beyond target envelope |
| Crash/restart recovery | Forced restart with dirty local state | Correct recovery with bounded startup time |
| Disk footprint stability | Same object set over repeated runs | No unbounded growth; usage remains explainable and bounded |
| Status/inspection safety | `studiofs status` against empty and provisioned roots | No mutations, no destructive side effects |

## 4. Runtime Rules

- The mounted runtime uses one local backend implementation rooted at the
  UUID-scoped cache root.
- Cache roots with no existing local contract bootstrap the runtime root on
  first open.
- Runtime read/write/open paths rely exclusively on runtime-root storage and index
  state.

## 5. Startup Reconciliation and Repair Behavior

On backend startup, reconciliation runs before normal reads and writes.

Default startup mode (`lightweight`):

1. Remove orphan temp files from `tmp/**`.
2. Scan dirty/uploading index rows and prune rows whose payload files are
   missing.

Optional full sweep mode (`full`), enabled via
`STUDIOFS_LOCAL_BLOCK_STARTUP_RECONCILE_MODE=full`:

1. Scan all index object ids and prune stale rows whose payload files are
   missing.
2. Scan payload files and delete orphan payload files with no index row.

Safety rules:
- Reconciliation is scoped to runtime files under `<cache-root>/**`.
- Reconciliation must not mutate unrelated files under `<cache-root>`.
- Any unrecoverable reconciliation failure aborts backend startup rather than
  proceeding with ambiguous state.

## 6. Remediation Flow for Stale Local Artifacts

When cache-root artifacts outside the runtime contract are discovered:

1. Treat them as non-runtime artifacts.
2. Use `studiofs status` for current runtime health and local counters.
3. Archive or remove stale artifacts as part of local maintenance.
4. Re-run mount; the current implementation reuses or provisions only the
   UUID-scoped runtime root.

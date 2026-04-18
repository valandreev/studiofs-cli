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

## 1. Versioned Disk Layout (v1)

All local runtime data lives under a dedicated `v3/` subtree inside the
per-volume cache root.

```text
<cache-root>/
└── v3/
    ├── LAYOUT_V1
    ├── tmp/
    ├── payload/
    │   └── blocks/
    │       ├── 00/
    │       ├── 01/
    │       └── ff/
    │           └── <object-id>.block
    └── index/
        └── cache.redb
```

Notes:
- `v3/LAYOUT_V1` is the explicit marker for layout version 1.
- `v3/index/cache.redb` is the fixed redb index location.
- Payload blocks are sharded into two levels (`00/00`..`ff/ff`) using a stable
  hash of `object_id`.

## 2. Mount and Status Detection Contract

Detection is read-only and must not mutate cache state.

- The current layout is detected when `<cache-root>/v3/LAYOUT_V1` exists.
- If no local contract exists yet, bootstrap provisions the `v3/` subtree
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

- The mounted runtime uses one local backend implementation rooted at `v3/**`.
- Cache roots with no existing local contract bootstrap the `v3/` subtree on
  first open.
- Runtime read/write/open paths rely exclusively on `v3/**` storage and index
  state.

## 5. Startup Reconciliation and Repair Behavior

On backend startup, reconciliation runs before normal reads and writes.

Default startup mode (`lightweight`):

1. Remove orphan temp files from `v3/tmp/**`.
2. Scan dirty/uploading index rows and prune rows whose payload files are
   missing.

Optional full sweep mode (`full`), enabled via
`STUDIOFS_LOCAL_BLOCK_STARTUP_RECONCILE_MODE=full`:

1. Scan all index object ids and prune stale rows whose payload files are
   missing.
2. Scan payload files and delete orphan payload files with no index row.

Safety rules:
- Reconciliation is scoped to `<cache-root>/v3/**` only.
- Reconciliation must not mutate unrelated files under `<cache-root>`.
- Any unrecoverable reconciliation failure aborts backend startup rather than
  proceeding with ambiguous state.

## 6. Remediation Flow for Stale Local Artifacts

When cache-root artifacts outside `v3/**` are discovered:

1. Treat them as non-runtime artifacts.
2. Use `studiofs status` for current runtime health and local counters.
3. Archive or remove stale artifacts as part of local maintenance.
4. Re-run mount; the current implementation reuses or provisions only the
   `v3/` subtree.

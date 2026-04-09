# Cache 3.0 Contract and Runtime Safety

## Purpose

Define the Cache 3.0 disk layout, backend-detection rules, startup safety
reconciliation, and operator remediation for stale local cache state.

This document is the source of truth for runtime behavior and operator-facing
safety expectations.

## 1. Versioned Disk Layout (v1)

All local cache data lives under a dedicated `v3/` subtree inside the per-volume
cache root.

```
<cache-root>/
└── v3/
    ├── LAYOUT_V1                  # layout contract marker (required for v3 detection)
    ├── tmp/                       # temporary files for atomic write/rename flows
    ├── payload/
    │   └── blocks/
    │       ├── 00/
    │       ├── 01/
    │       └── ff/
    │           └── <object-id>.block
    └── index/
        └── cache.redb             # redb index database file
```

Notes:
- `v3/LAYOUT_V1` is the explicit marker for Cache 3.0 layout version 1.
- `v3/index/cache.redb` is the fixed redb index location.
- Payload blocks are sharded into two levels (`00/00`..`ff/ff`) using a stable
  hash of `object_id`.

## 2. Mount/Status Detection Contract

Detection is read-only and must not mutate cache state.

- Cache 3.0 is detected when `<cache-root>/v3/LAYOUT_V1` exists.
- If no local contract exists yet, bootstrap provisions the `v3/` subtree before
  opening the backend.
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
| Status/inspection safety | `studiofs status` against empty and v3-provisioned roots | No mutations, no destructive side effects |

## 4. Runtime Rules

- Cache 3.0 is the only mounted local backend.
- Cache roots with no existing local contract bootstrap the `v3/` subtree on
  first open.
- Runtime read/write/open paths rely exclusively on `v3/**` storage and index
  state.

## 5. Startup Reconciliation and Repair Behavior

On Cache 3.0 backend startup, reconciliation runs before normal reads/writes.

Default startup mode (`lightweight`):

1. Remove orphan temp files from `v3/tmp/**`.
2. Scan dirty/uploading index rows and prune rows whose payload files are
   missing.

Optional full sweep mode (`full`), enabled via
`STUDIOFS_CACHE_V3_STARTUP_RECONCILE_MODE=full`:

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
2. Use `studiofs status` for current runtime health and v3 counters.
3. Archive or remove stale artifacts as part of local maintenance.
4. Re-run mount; Cache 3.0 reuses/provisions only the `v3/` subtree.

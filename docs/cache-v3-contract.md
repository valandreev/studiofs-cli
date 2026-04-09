# Cache 3.0 Contract and Migration Safety (SFS-173, SFS-181, SFS-182)

## Purpose

Define the Cache 3.0 disk layout, backend-detection rules, startup safety
reconciliation, and operator remediation for legacy local cache state.

This document is the source of truth for rollout behavior and operator-facing
safety expectations.

## 1. Versioned Disk Layout (v1)

All Cache 3.0 data lives under a dedicated `v3/` subtree inside the existing
per-volume cache root.

```
<cache-root>/
├── blocks.raw                     # legacy raw backend (v2), unchanged
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
- `v3/LAYOUT_V1` is the explicit marker that the cache root is provisioned for
  Cache 3.0 layout version 1.
- `v3/index/cache.redb` is the fixed redb index location.
- payload blocks are sharded into two levels (`00/00`..`ff/ff`) using a stable
  hash of `object_id`, which avoids hot spots for sequential ids.
- `blocks.raw` is not converted in place.
- A cache root may still contain legacy `blocks.raw`, but mounted runtime no
  longer opens that state. Legacy presence is treated as an operator-remediation
  condition, not a supported fallback path.

## 2. Mount/Status Detection Contract

Detection is read-only and must not mutate cache state.

- Legacy raw backend detected when `<cache-root>/blocks.raw` exists.
- Cache 3.0 detected when `<cache-root>/v3/LAYOUT_V1` exists.
- If `blocks.raw` exists, mounted runtime is blocked and must fail fast with
  explicit remediation.
- If no local contract exists yet, the Cache 3.0 bootstrap path provisions the
  `v3/` subtree before opening the backend.

## 3. Acceptance Matrix (Real Mount Path Only)

The success reference is the current legacy backend on the same hardware and
same workload. Internal direct-shim benchmarks are not acceptance gates.

| Scenario | Workload and setup | Required comparison outcome vs current backend |
|---|---|---|
| Sequential write throughput | Mounted volume, large sequential writes, identical dataset and cache path | Cache 3.0 must be >= current backend |
| Sequential read throughput | Mounted volume, warm-cache sequential reads | Cache 3.0 must be >= current backend |
| Random read latency | Mounted volume, mixed-size random reads | Cache 3.0 p95/p99 must be <= current backend |
| Mixed read/write with uploads | Mounted volume under sustained upload pressure | No regression in foreground p95 latency and no lower upload completion rate |
| Crash/restart recovery | Forced restart with dirty local state | Recovery correctness must match current backend; recovery time must be <= current backend |
| Disk footprint stability | Same object set over repeated runs | No unbounded growth; steady-state usage must be explainable and within configured limits |
| Status/inspection safety | `studiofs status` against legacy, v3-only, and dual-layout roots | No mutations, no destructive side effects, consistent detection output |

Performance follow-up work should continue to compare Cache 3.0 against the
previous legacy backend on representative mounted workloads.

### 3.1 Real-Path Comparison Command Pattern

Run these on the same machine and same workload inputs:

```powershell
# Cache 3.0 mounted path/workload profile
studiofs test-cache <mounted-workload-path> `
  --access-pattern mixed `
  --concurrency 4 `
  --io-depth 64 `
  --warmup-runs 1 `
  --json-output .\reports\cache-v3-e2e.json
```

Keep comparison data against legacy-baseline runs in external reports or issue
attachments; the runtime no longer uses code-level cutover gates.

## 4. Runtime Rules

- Cache 3.0 is the only mounted local backend.
- Cache roots containing `blocks.raw` are never opened silently.
- Cache roots with no existing local contract bootstrap the `v3/` subtree on
  first open.
- Status may inspect legacy `blocks.raw` for operator context, but runtime
  open/mount paths do not use it as a backend.

## 5. Startup Reconciliation and Repair Behavior

On Cache 3.0 backend startup, reconciliation runs before normal reads/writes.

Default startup mode (`lightweight`):

1. Remove orphan temp files from `v3/tmp/**`.
2. Scan dirty/uploading index rows and prune rows whose payload files are missing.

Optional full sweep mode (`full`), enabled via
`STUDIOFS_CACHE_V3_STARTUP_RECONCILE_MODE=full`:

1. Scan all index object ids and prune stale rows whose payload files are missing.
2. Scan payload files and delete orphan payload files with no index row.

Safety rules:
- Reconciliation is scoped to `<cache-root>/v3/**` only.
- Legacy `<cache-root>/blocks.raw` is never mutated by Cache 3.0 reconciliation.
- Any unrecoverable reconciliation failure aborts Cache 3.0 backend startup
  rather than proceeding with ambiguous state.

## 6. Legacy Remediation Flow

When a cache root still contains legacy `blocks.raw`:

1. Do **not** expect StudioFS mount/runtime to open that cache root.
2. Inspect the root with `studiofs status` to confirm whether `blocks.raw`,
   `v3/`, or both are present.
3. Remove or archive `blocks.raw` when the old local cache state is no longer
   needed.
4. Re-run mount; the Cache 3.0 bootstrap path will provision or reuse the
   `v3/` subtree.

There is no runtime fallback that re-selects the legacy raw backend.
  is required.
- Keep `v3/` data intact for investigation and future retries.

Operational note:
- Dual layout can temporarily increase disk usage because legacy and v3 data
  coexist during migration.

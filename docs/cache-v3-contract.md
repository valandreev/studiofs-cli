# Cache 3.0 Contract and Migration Safety (SFS-173, SFS-181, SFS-182)

## Purpose

Define the Cache 3.0 disk layout, backend-detection rules, startup safety
reconciliation, and migration guardrails.

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
- `blocks.raw` is not converted in place in this phase.
- Legacy and v3 data are allowed to coexist in the same cache root.

## 2. Mount/Status Detection Contract

Detection is read-only and must not mutate cache state.

- Legacy raw backend detected when `<cache-root>/blocks.raw` exists.
- Cache 3.0 detected when `<cache-root>/v3/LAYOUT_V1` exists.
- Dual layout is valid when both exist.
- During rollout, dual layout still selects legacy raw as the mount contract.
- A v3-only root is treated as Cache 3.0 contract state, but mount remains
  blocked until the backend implementation and final validation gates complete.

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

Throughput cutover targets:
- Minimum gate: Cache 3.0 must meet at least **3.0x** throughput ratio for both
  reads and writes versus the current real cache path.
- Stretch target: Track whether **4.0x** is realistic on measured workloads, but
  do not require it to pass the default-cutover gate.
- When USB-attached NVMe is available, run the same comparison on that medium
  and include those ratios in the validation record.

### 3.1 Required Validation Record Before Default Cutover

Default cutover is controlled by code-level gate evaluation:
- `CACHE_V3_DEFAULT_ALLOWED` must be `true`.
- `CACHE_V3_FINAL_VALIDATION_ISSUE` must match the validation evidence issue id.
- `CACHE_V3_VALIDATION_EVIDENCE` must be populated and pass:
  - matrix executed;
  - compared against current real cache path;
  - local NVMe read/write ratios >= `CACHE_V3_MIN_TARGET_RATIO` (`3.0x`);
  - USB NVMe ratios present and >= `3.0x` when USB NVMe was available;
  - no open blockers.

If any gate check fails, dual-layout mounts remain on legacy raw backend.

### 3.2 Real-Path Comparison Command Pattern

Run these on the same machine and same workload inputs for each backend:

```powershell
# Legacy baseline (mounted path inside active StudioFS mount)
studiofs test-cache <mounted-workload-path> `
  --access-pattern mixed `
  --concurrency 4 `
  --io-depth 64 `
  --warmup-runs 1 `
  --json-output .\reports\legacy-e2e.json

# Cache 3.0 candidate on the same mounted path/workload profile
studiofs test-cache <mounted-workload-path> `
  --access-pattern mixed `
  --concurrency 4 `
  --io-depth 64 `
  --warmup-runs 1 `
  --json-output .\reports\cache-v3-e2e.json
```

Record read/write throughput ratios (candidate/baseline) for:
- local NVMe;
- USB-attached NVMe (when available).

If the 3.0x target is missed, keep `CACHE_V3_DEFAULT_ALLOWED=false` and list
exact bottlenecks in `CACHE_V3_VALIDATION_EVIDENCE.blockers` before retrying.

## 4. Rollout, Rollback, and Default Rules

### Rollout stages

1. Contract-only phase (this issue): layout + detection + guardrails.
2. Opt-in implementation phase: Cache 3.0 mount/bootstrap behind explicit
   non-default activation, with startup reconciliation enabled.
3. Validation phase: run full acceptance matrix on production-representative
   hardware and workloads.
4. Default-switch phase: allowed only after final validation issue is closed.

### Rollback rules

- Rollback is always mount-level selection, not in-place conversion.
- No migration of `blocks.raw` contents is required to roll back.
- If Cache 3.0 rollback is needed, keep `v3/` data intact for investigation and
  remount with legacy raw contract.

### Default backend guardrail

- Cache 3.0 is explicitly non-default until the final validation issue passes.
- The code gate in `studiofs-cache/src/v3/mod.rs` (`CACHE_V3_DEFAULT_ALLOWED`)
  must remain `false` until that issue is complete.
- The default flip and issue closure evidence must land together in one change.

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

## 6. Migration and Fallback Flow (Existing Cache Roots)

When enabling Cache 3.0 on a cache root that already has legacy `blocks.raw`:

1. Keep legacy data in place; do **not** convert or rewrite `blocks.raw`.
2. Provision `v3/` side-by-side (`LAYOUT_V1`, `payload/`, `index/`, `tmp/`).
3. During dual-layout coexistence, mount contract remains **legacy raw**.
4. Cache 3.0 can be validated safely without destructive conversion.
5. Cutover to Cache 3.0 uses a v3-only cache root contract decision.

Fallback path:
- Roll back by selecting legacy mount contract; no in-place conversion rollback
  is required.
- Keep `v3/` data intact for investigation and future retries.

Operational note:
- Dual layout can temporarily increase disk usage because legacy and v3 data
  coexist during migration.

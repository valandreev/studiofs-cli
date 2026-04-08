# Cache 3.0 Contract Freeze (SFS-173)

## Purpose

Freeze the Cache 3.0 disk layout, backend-detection rules, acceptance gates,
and rollout guardrails before implementation work starts.

This document is the source of truth for follow-up tasks (starting with
SFS-174) and is intentionally implementation-agnostic.

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

## 4. Rollout, Rollback, and Default Rules

### Rollout stages

1. Contract-only phase (this issue): layout + detection + guardrails.
2. Opt-in implementation phase: Cache 3.0 mount/bootstrap behind explicit
   non-default activation.
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

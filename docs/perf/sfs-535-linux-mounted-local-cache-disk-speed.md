# SFS-535 Linux mounted local-cache disk speed

Date: 2026-06-19

## Decision status

decision status: pending measured run.

SFS-535 promotes the Linux mounted read path only after mounted reads against a
trusted local cache prove disk-speed behavior on the same host. The acceptance
target is a warm trusted local-cache read at 60-75% of raw warm disk speed. If a
host cannot meet that range, the report must name the bottleneck with evidence
instead of hiding it behind aggregate throughput.

Raw artifacts should live under:

```text
target/sfs-535-linux-mounted-local-cache-disk-speed/
```

One-command wrapper:

```bash
tools/perf/sfs535-linux-mounted-local-cache-disk-speed.sh compact
SFS535_RUN_MOUNTED=1 tools/perf/sfs535-linux-mounted-local-cache-disk-speed.sh sustained
```

## Required comparison rows

| Row | Purpose |
|---|---|
| same-host raw disk baseline | Establish clean and warm storage ceilings for the benchmark directory. |
| warm trusted local-cache read | Mounted StudioFS read of an already trusted local-cache payload; expected 60-75% of raw warm disk speed or a documented bottleneck. |
| cold local-cache read | First mounted read after cache population is unavailable or cold; records remote/materialization cost and fallback path. |
| fallback correctness matrix | Mounted or focused tests proving unsafe ranges fall back to bytes reads for stale epoch, stale invalidation generation, dirty writer, dirty inode, eviction, truncate, overwrite, and remote refresh fallback. |

## Required artifact fields

Every machine-readable row must preserve:

- throughput;
- p95 latency;
- p99 latency;
- CPU;
- selected_reply_strategy;
- pread_local_cache_replies;
- splice_local_cache_replies;
- local_cache_reply_bytes;
- local-cache hit and miss counts;
- fallback_reasons;
- unsafe_local_cache_fallbacks;
- materialization wait;
- read amplification;
- transport mode and kernel capabilities;
- bottleneck, when the 60-75% target is missed.

The SFS-529 local strategy harness already records `selected_reply_strategy`,
`local_cache_reply_bytes`, and `fallback_reasons` for local mechanics. The
mounted telemetry writer records `fallback_reasons` from
`VfsNativeBridgeSnapshot` counters on the live bridge:
`pread_local_cache_replies`, `splice_local_cache_replies`,
`local_cache_reply_bytes`, and `unsafe_local_cache_fallbacks`.

## Reproducible commands

Local strategy artifact, written automatically by the wrapper:

```bash
cargo test -p studiofs-fuse --test native_local_cache_read_benchmark -- --ignored --nocapture
```

Mounted evidence uses the SFS-535 wrapper, which calls the SFS-532 harness and
copies the relevant mounted status files into the SFS-535 artifact root:

```bash
tools/perf/sfs535-linux-mounted-local-cache-disk-speed.sh compact
SFS535_RUN_MOUNTED=1 tools/perf/sfs535-linux-mounted-local-cache-disk-speed.sh sustained
```

Before closing SFS-535, keep the raw fio/perf/native bridge telemetry beside the
wrapper-generated `target/sfs-535-linux-mounted-local-cache-disk-speed/summary.md`.

## Current implementation coverage

The native read path now passes the FUSE file handle into VFS local-cache range
selection, so write-capable handles and dirty inode state cannot borrow an unsafe
range. Classic mounted sessions use the output fd transport constructor, which
selects the splice reply strategy when a stable local-cache range is available.
Bridge telemetry exports local-cache hits, unavailable fallbacks, stale epoch,
stale invalidation generation, unsafe local-cache fallbacks, pread local-cache
replies, splice local-cache replies, local-cache reply bytes, and fallback
reasons.

Focused correctness tests cover stale epoch, stale invalidation generation,
local cache eviction, dirty writer, dirty inode, truncate, overwrite, remote
refresh fallback, selected reply strategy, and bridge reply counters. The final
report still needs mounted throughput, p95/p99 latency, CPU, raw disk baseline,
and bottleneck evidence from the reference FUSE-capable host.

## Closure rule

Do not mark SFS-535 complete until the final summary contains either:

- warm trusted local-cache read throughput at 60-75% of raw warm disk speed with
  matching p95 latency, p99 latency, CPU, selected reply strategy, and
  local-cache reply counters; or
- an explicit bottleneck analysis explaining why the mounted path missed the
  target, with the same raw disk baseline, latency, CPU, fallback reason, and
  bridge telemetry evidence.

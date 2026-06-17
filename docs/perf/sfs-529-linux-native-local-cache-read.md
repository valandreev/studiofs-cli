# SFS-529 LinuxNativeBridge Local-Cache Read Benchmark

This is the LinuxNativeBridge local-cache read benchmark gate for SFS-529.

## Status

SFS-529 requires benchmark evidence before a local-cache read strategy can become
a promoted LinuxNativeBridge profile. This report defines the reproducible local
strategy gate and the mounted-FUSE evidence that must be added after the fd/range
path is wired into a live native session.

Run the local strategy harness with:

```bash
cargo test -p studiofs-fuse --test native_local_cache_read_benchmark -- --ignored --nocapture
```

The harness writes raw artifacts to:

```text
target/sfs-529-local-cache-read-benchmark/sfs-529-local-cache-read.json
```

## Compared Strategies

The local harness compares three SFS-529 read reply strategies on the same host:

- bytes baseline: copy an already materialized in-memory payload into a reply
  sink.
- pread local-cache range: build a `NativeLocalCacheReadRange`, let
  `ClassicReadWriteTransport` read from the stable local cache fd with `pread`,
  and write the FUSE reply header plus payload bytes.
- splice(file_fd -> pipe -> fuse_fd): call the Linux fd-level helper that splices
  a stable local cache file range through a pipe into a raw output fd.

The harness records io_uring usage/fallback as explicit JSON fields. The current
local-file benchmark records `io_uring_used=false` and an io_uring fallback note
because SFS-529 treats io_uring local-file read as a measured future strategy,
not a mandatory dependency.

## Required Evidence Fields

Each benchmark record must include:

- throughput
- CPU collection guidance (`perf stat` / mounted profile captures)
- p95 latency
- p99 latency
- copy count
- local-cache hit/miss
- materialization wait
- read amplification
- io_uring usage/fallback

The benchmark gate is incomplete without sparse random reads. Sparse random reads
must show bounded dynamic readahead behavior: no unbounded prefetch, no unbounded
read amplification, and explicit read amplification metrics. Sequential reads
must show the configured bounded dynamic readahead range when the policy detects
safe forward progress.

## Follow-Up Mounted Gate

The local strategy harness proves the fd-backed strategy mechanics without
requiring `/dev/fuse`. Full SFS-529 acceptance still requires a mounted native
Linux run that records cold local-cache throughput, warm kernel-cache behavior,
CPU cost, p95/p99 latency, copy count or proxy counters, local-cache hit/miss,
materialization wait, io_uring usage/fallback, and sparse random read
amplification against the same workload matrix.

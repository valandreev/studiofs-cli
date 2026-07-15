# Linux cache and CPU validation

Date: 2026-07-15

## Decision

Linux keeps the Direct cache policy and requests FUSE `ASYNC_DIO` by default.
Set `STUDIOFS_FUSE_ASYNC_DIO=0` only as a serial rollback control. The Kernel
cache policy remains an internal experiment and is not promoted.

The final runtime change makes Redb `get_attr(ino)` compare only that inode's
current and base records. It no longer rebuilds and decodes every regular-file
chunk graph on each lookup. No Linux `O_DIRECT`, `POSIX_FADV_DONTNEED`, new
adapter queue, or cache layer was added.

## Provenance

| Item | Value |
|---|---|
| Baseline source | `b376f377b4b1df1106b141f427cb09a9f7c08e81` |
| Baseline StudioFS SHA-256 | `84236eeb136a6ab4ae66d914551b8a5d6c333cc5d1bdd2e80f84e95d737b8c15` |
| Final measured runtime source | `5724855e64d2315f1339fed5c1a6d49439806dbe` |
| Final measured StudioFS SHA-256 | `b8554ed3dd22ccf15207e2f76efe578da61e1c9a03376dfdfb0137607377a486` |
| `studiofs-bench` source | `7a27ac3d0c63f5be04affad6d98bf3bb4043e3e5` |
| `studiofs-bench` SHA-256 | `b7f4b25752eb051378216819f02d356f92409fb751c65cc5c4c4d910bfabbae2` |

The final correctness manifest is
`target/perf/linux-default-validation-5724855e/correctness-manifest.json`.
All 10 mounted correctness tests across Direct serial, Direct async, and
Kernel modes ran; none were skipped, and all exited successfully. The source
and both binary checksums were clean and exact.

The performance collections below are diagnostic, single-repetition evidence,
not a five-repetition promotion matrix. A collector gate pass means that the
measured workload, callback limits, idle drain, and cleanup passed; the
artifacts still record `promotion_eligible=false` by design.

## Gate results

### 1 GB sequential write/read

Both baseline and final Direct async collections passed the collector gate,
mount, and unmount with exit status 0 and verified 1,000,000,000 bytes in each
direction.

| Metric | Baseline | Final | Change |
|---|---:|---:|---:|
| Foreground wall time | 14.01 s | 12.93 s | -7.7% |
| Foreground mount CPU | 57.46 s | 39.60 s | -31.1% |
| Lifecycle CPU per processed GiB | 31.06 s | 21.34 s | -31.3% |
| Write throughput | 309.42 MB/s | 245.08 MB/s | -20.8% |
| Read throughput | 104.98 MB/s | 202.71 MB/s | +93.1% |

The write phase varied downward, but total wall time and CPU both improved.
All FUSE callback histograms remained within the fixed limits; callback,
async-task, cancellation, and workload errors were zero. Final artifact:
`target/perf/linux-default-validation-5724855e-direct-async-1g/rep-001`.

### 1000 files of 128 KiB

This workload creates 1000 files, calls `fsync` for every file, reads every
file, and verifies the checksum. Both collections passed gate, mount, and
unmount with exit status 0.

| Metric | Baseline | Final | Change |
|---|---:|---:|---:|
| Foreground wall time | 104.58 s | 49.56 s | -52.6% |
| Foreground mount CPU | 171.91 s | 27.53 s | -84.0% |
| Average foreground cores | 1.64 | 0.55 | -66.5% |
| Write throughput | 1.45 MB/s | 2.80 MB/s | +93.4% |
| Read throughput | 9.44 MB/s | 50.88 MB/s | +438.7% |
| Daemon RSS peak | 176.8 MB | 159.0 MB | -10.1% |

Final write p99/max were at most 2/5 ms, read p99/max 2/10 ms, `fsync`
p99/max 500/2000 ms, and flush max 5 ms. All callback and async errors were
zero, the drain reached idle, and owned mount/cache/cgroup paths were removed.
Final artifact:
`target/perf/linux-default-validation-5724855e-direct-async-small-128k/rep-001`.

## 4 GB diagnostic limitation

Two exact final-runtime Direct async repetitions reproduced the CPU and total
wall-time improvement, but neither passed the strict callback-tail gate.

| Metric | Baseline | Final run 1 | Final run 2 |
|---|---:|---:|---:|
| Foreground wall time | 94.38 s | 63.92 s | 79.52 s |
| Foreground mount CPU | 279.38 s | 163.59 s | 149.60 s |
| Lifecycle CPU per processed GiB | 37.55 s | 21.97 s | 20.10 s |
| Write throughput | 203.73 MB/s | 234.96 MB/s | 153.09 MB/s |
| Read throughput | 57.07 MB/s | 82.41 MB/s | 72.89 MB/s |

The outlier moved between repetitions. Run 1 had a 6.246 s read callback while
write/flush maxima were 0.302/2.755 s. Run 2 had a 0.843 s read maximum but
write/flush maxima of 5.314/16.508 s after reaching the 1.679 GB hard dirty
window. Both runs verified all bytes, reported zero callback/async errors,
drained to idle, and unmounted cleanly.

The run-1 read outlier did not come from payload I/O: its local payload read
maximum was 121 ms and no S3 fetch or async-read admission occurred. Because it
did not reproduce, a separate read-path change is not justified. The remaining
large-file limitation is variable pressure/materialization tail latency; it
must stay visible rather than being hidden by a relaxed histogram threshold.

Artifacts:

- `target/perf/linux-default-validation-5724855e-direct-async-4g/rep-001`
- `target/perf/linux-default-validation-5724855e-direct-async-4g-r2/rep-001`

An earlier Kernel-policy diagnostic had roughly 359 seconds of post-workload
drain and 1477 lifecycle CPU-seconds. That is sufficient to reject Kernel as
the default, but it is not final-revision promotion evidence.

## Interpretation

The pre-fix CPU profile attributed 65.9% cumulative samples to
`MetaInodes::get_attr`; 64.7% rebuilt the complete local overlay, while the
Linux async-write queue accounted for only 1.7%. The final small-file and
large-file measurements confirm that removing that global metadata scan fixes
the reported CPU problem. `ASYNC_DIO` therefore remains enabled: disabling it
would not address the measured hotspot, and earlier same-binary comparison
showed better aggregate write and lifecycle results with it enabled.

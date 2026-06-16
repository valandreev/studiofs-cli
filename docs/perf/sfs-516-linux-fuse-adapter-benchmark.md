# SFS-516 Benchmark Linux FUSE adapter options

Date: 2026-06-16

## Decision status

decision status: pending.

This document is the reproducible benchmark plan and report scaffold for
SFS-516. It must become the checked-in benchmark report before the adapter
decision is closed. Do not treat this first version as measured evidence: the
current fuser 0.16 implementation, the fuser 0.17 prototype, and the fuse3
Linux-only adapter prototype have not yet been run through the full comparison
matrix in this commit.

SFS-515 remains separate. SFS-516 compares ordinary Linux FUSE adapter paths
over the existing kernel `/dev/fuse` model and must not assume that any audited
library already provides a production-ready FUSE-over-io_uring transport.

## Candidate adapters

The comparison surface is:

| Variant | Purpose | Required setup |
|---|---|---|
| `current-fuser-0-16` | Baseline against the current fuser 0.16 implementation in `studiofs-fuse`. | Build the workspace with the locked `fuser = "^0.16"` dependency and `abi-7-22`. |
| `fuser-0-17-prototype` | Test whether fuser 0.17 removes dispatch bottlenecks without replacing the adapter. | Prototype the breaking API migration, including `Config::n_threads`, multiple event loops per session, and `FUSE_DEV_IOC_CLONE` where available. |
| `fuse3-prototype` | Test a replacement path through async filesystem traits and async handler dispatch. | Prototype a Linux-only adapter with inode APIs, `readdirplus`, notify support, and init tuning for `max_write`, `max_background`, and `congestion_threshold`. |

The latest Cargo index check on 2026-06-16 reported `fuser = "0.17.0"` and
`fuse3 = "0.9.0"`. The checked-in workspace still depends on `fuser = "^0.16"`
for Linux:

```text
fuser req=^0.16 target=cfg(target_os = "linux") features=abi-7-22
```

## Workload matrix

Run every workload against each adapter variant on the same native Linux host,
or clearly equivalent hosts when one machine cannot build all prototypes.
This matrix covers sequential reads, random reads, mixed read + metadata
concurrency, and mount shutdown behavior with pending work.

| Workload | Required observations |
|---|---|
| Sequential reads | Warm-cache and cold-cache throughput, read callback latency distribution, kernel readahead behavior. |
| Random reads | 4 KiB and 64 KiB random read throughput, p50/p90/p99 latency distribution, CPU cost. |
| Metadata traversal | `getattr / lookup / readdir / readdirplus` operation rate, tail latency, and reply failures. |
| Mixed read + metadata concurrency | A long or slow read running while metadata callbacks continue to complete. |
| Write barriers | `write + flush/release/fsync ordering` correctness and latency under sequential and multi-job writes. |
| Shutdown | Mount shutdown behavior with pending work, including pending async replies and in-flight writer barriers. |

## Metrics

Capture at least:

- throughput for each fio and metadata probe case;
- latency distribution for read, write, metadata, flush, fsync, release, and
  shutdown paths;
- CPU cost from `perf stat`;
- context switching and scheduling overhead from `perf stat -d -d -d` and
  `/proc` samples;
- failure modes, including hangs, `EIO` replies, dropped replies, unmount
  failures, feature-negotiation failures, and ordering regressions.

The report must keep lifecycle and migration risk separate from performance.
Performance is the primary decision driver; lifecycle risk explains the cost of
adopting the measured winner.

## Host preflight

The initial local SFS-516 workspace preflight on 2026-06-16:

```text
Linux devbench 7.0.0-22-generic #22-Ubuntu SMP PREEMPT_DYNAMIC Mon May 25 15:54:34 UTC 2026 x86_64 GNU/Linux
workspace filesystem: ext4 /dev/mapper/ubuntu--vg-ubuntu--lv /
/dev/fuse: present
fusermount3: /usr/bin/fusermount3
fio: /usr/bin/fio
perf: /usr/bin/perf
rustc: rustc 1.96.0 (ac68faa20 2026-05-25)
cargo: cargo 1.96.0 (30a34c682 2026-05-25)
```

Before the real run, record the exact kernel, libfuse, package versions,
feature flag values, runtime settings, mount options, dependency lockfile
state, and adapter prototype commit for each variant.

## Artifact layout

All raw artifacts must stay under:

```text
target/sfs-516-linux-fuse-adapter-benchmark/
```

Use this layout:

| Directory | Contents |
|---|---|
| `preflight/` | `uname`, filesystem, `/dev/fuse`, kernel tunables, libfuse, Rust, Cargo, and tool versions. |
| `current-fuser-0-16/` | Mount logs, fio JSON, metadata probe output, status snapshots, and perf outputs for the current adapter. |
| `fuser-0-17-prototype/` | The same raw artifacts for the fuser 0.17 prototype. |
| `fuse3-prototype/` | The same raw artifacts for the fuse3 Linux-only adapter prototype. |
| `summary/` | Aggregated tables, derived metrics, and the final decision notes. |

Commit the final report summary, and keep enough raw artifacts or generated
artifacts to reproduce the conclusion. Large transient run output may remain in
`target/`, but the checked-in report must name the exact raw artifact paths and
commands used to regenerate them.

## Command matrix

Focused documentation gate:

```bash
cargo test -p studiofs --test architecture_vocabulary_test linux_fuse_adapter_benchmark_report_covers_sfs516_decision_gate -- --nocapture
```

Preflight:

```bash
mkdir -p target/sfs-516-linux-fuse-adapter-benchmark/preflight
uname -a | tee target/sfs-516-linux-fuse-adapter-benchmark/preflight/uname.txt
findmnt -T "$PWD" -no FSTYPE,SOURCE,TARGET | tee target/sfs-516-linux-fuse-adapter-benchmark/preflight/findmnt.txt
df -h "$PWD" | tee target/sfs-516-linux-fuse-adapter-benchmark/preflight/df.txt
test -e /dev/fuse && echo present | tee target/sfs-516-linux-fuse-adapter-benchmark/preflight/dev-fuse.txt
command -v fusermount3 fio perf rustc cargo | tee target/sfs-516-linux-fuse-adapter-benchmark/preflight/tools.txt
rustc --version | tee target/sfs-516-linux-fuse-adapter-benchmark/preflight/rustc.txt
cargo --version | tee target/sfs-516-linux-fuse-adapter-benchmark/preflight/cargo.txt
```

Mounted correctness controls for the current adapter:

```bash
export STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_default_sync -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_opt_in -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_dio_opt_in -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_sfs509_mounted_fio_default_sync -- --ignored --exact --nocapture
```

Use the same mounted smoke and fio workload shape for the fuser 0.17 prototype
and the fuse3 Linux-only adapter prototype after each prototype exposes the
same mount behavior and artifact directory controls.

Fio and metadata probes should emit one JSON or TSV file per variant and
workload:

```bash
fio --output-format=json --output target/sfs-516-linux-fuse-adapter-benchmark/<variant>/seq-read.json <jobfile>
fio --output-format=json --output target/sfs-516-linux-fuse-adapter-benchmark/<variant>/rand-read.json <jobfile>
fio --output-format=json --output target/sfs-516-linux-fuse-adapter-benchmark/<variant>/mixed-read-metadata.json <jobfile>
```

Profiling:

```bash
perf stat -d -d -d -o target/sfs-516-linux-fuse-adapter-benchmark/<variant>/<workload>.perf-stat.txt -- <workload-command>
perf record -F 99 -g -o target/sfs-516-linux-fuse-adapter-benchmark/<variant>/<workload>.perf.data -- <workload-command>
perf script -i target/sfs-516-linux-fuse-adapter-benchmark/<variant>/<workload>.perf.data > target/sfs-516-linux-fuse-adapter-benchmark/<variant>/<workload>.perf.script
```

## Result table

Fill this table only with real measurements.

| Variant | Sequential reads | Random reads | Metadata traversal | Mixed read + metadata concurrency | Write barriers | Shutdown | Decision notes |
|---|---:|---:|---:|---:|---:|---:|---|
| current fuser 0.16 implementation | pending | pending | pending | pending | pending | pending | Baseline not yet measured for SFS-516. |
| fuser 0.17 prototype | pending | pending | pending | pending | pending | pending | Prototype not yet measured. |
| fuse3 Linux-only adapter prototype | pending | pending | pending | pending | pending | pending | Prototype not yet measured. |

## Current fuser 0.16 local baseline

The first SFS-516 local run on 2026-06-16 measured only the current fuser 0.16
implementation. It is not a final adapter comparison, but it is enough to rule
out spending more time on fuser 0.16 tuning knobs before testing a threaded or
replacement adapter.

Commands passed:

```bash
cargo fmt --check
cargo test -p studiofs --test architecture_vocabulary_test -- --nocapture
cargo test -p studiofs-fuse -- --nocapture
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_default_sync -- --ignored --exact --nocapture
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_opt_in -- --ignored --exact --nocapture
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_dio_opt_in -- --ignored --exact --nocapture
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 STUDIOFS_FUSE_ASYNC_WRITE=1 cargo test -p studiofs-fuse --test mount_sanity async_mounted_ -- --ignored --nocapture --test-threads=1
```

The mounted async correctness sweep passed 11/11, including flush, fsync,
close/reopen, multiple writers, same-handle appends, and unmount after async
write. Correctness is not the blocker for the current adapter path.

Compact benchmark parameters:

```text
STUDIOFS_SFS509_FIO_SIZE=4m
STUDIOFS_SFS509_FIO_LARGE_SIZE=16m
STUDIOFS_SFS509_FIO_RUNTIME_SECS=3
STUDIOFS_SFS509_FIO_TIMEOUT_SECS=60
STUDIOFS_SFS509_METADATA_FILES=100
```

Compact matrix elapsed time:

| Current fuser 0.16 profile | Elapsed |
|---|---:|
| default sync | 81.91 s |
| async write opt-in | 83.91 s |
| async DIO opt-in | 78.83 s |
| background queue profile | 81.07 s |

Metadata probe:

| Current fuser 0.16 profile | Files | Create time | Stat time |
|---|---:|---:|---:|
| default sync | 100 | 18.091 s | 47 ms |
| async write opt-in | 100 | 12.969 s | 26 ms |
| async DIO opt-in | 100 | 12.641 s | 32 ms |
| background queue profile | 100 | 12.371 s | 33 ms |

Selected fio results:

| Current fuser 0.16 profile | Seq write | Cold read | Warm read | Multi-job write | Fsync-heavy write |
|---|---:|---:|---:|---:|---:|
| default sync | 52,512 KiB/s | 23,676 KiB/s | 78,769 KiB/s | 864 KiB/s | 156 KiB/s |
| async write opt-in | 41,795 KiB/s | 74,472 KiB/s | 83,591 KiB/s | 594 KiB/s | 181 KiB/s |
| async DIO opt-in | 46,545 KiB/s | 91,022 KiB/s | 75,851 KiB/s | 830 KiB/s | 179 KiB/s |
| background queue profile | 48,761 KiB/s | 85,333 KiB/s | 97,523 KiB/s | 846 KiB/s | 145 KiB/s |

Barrier tail latency stayed high in all current-adapter profiles. Multi-job
write p95 sync latency was roughly 312-346 ms across the compact run, and
fsync-heavy p95 sync latency was roughly 103-139 ms. The async write and async
DIO knobs did not produce a decisive throughput win.

A larger default-sync stress pass used 16 MiB ordinary fio files, 64 MiB
large-file, 5 second runtime, and 1000 metadata files. It produced:

```text
metadata create: 1000 files in 231.128 s
metadata stat: 1000 files in 304 ms
multi-job: timeout after 60 s
fsync-heavy: timeout after 60 s
```

Raw artifacts from these runs were copied under:

```text
target/sfs-516-linux-fuse-adapter-benchmark/current-fuser-0-16/
```

Interim direction: do not keep iterating on current fuser 0.16 knobs as the
main performance path. The next implementation branch should build the fuser
0.17 prototype first, because `Config::n_threads`, multiple event loops per
session, and `FUSE_DEV_IOC_CLONE` are the smallest direct test of whether
threaded fuser dispatch fixes the observed metadata and barrier-heavy
tail-latency profile. If fuser 0.17 does not materially improve the same
compact and stress matrices, move the decision to the fuse3 Linux-only adapter
prototype instead of keeping the current adapter.

## Next implementation gate

After the benchmark data is collected:

1. Choose keep-current, migrate to fuser 0.17, or migrate to fuse3 based first
   on throughput and tail latency.
2. Record lifecycle and migration risk separately.
3. Start the chosen implementation path or split it into follow-up subtasks
   immediately.
4. Add focused regression tests for dispatch concurrency, notifier behavior,
   shutdown, and write barriers before changing defaults.

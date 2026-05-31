# SFS-473 Linux async FUSE opt-in benchmark

Date: 2026-05-31

Host:

- Windows 11 host, WSL2 Ubuntu guest.
- Kernel: `Linux valslegion 6.6.87.2-microsoft-standard-WSL2`.
- Rust: `rustc 1.96.0 (ac68faa20 2026-05-25)`.
- FUSE: `fusermount3`, `fuse3` package version `3.14.0`.
- fio: `fio-3.36`.
- Workspace copy used for mounted runs: `/tmp/studiofs-cli-sfs473`, because
  `fusermount3` rejects mounts over `/mnt/e` drvfs with filesystem type
  `0x01021997`.

Windows comparison host:

- Same Windows 11 host.
- Bridge: WinFSP through `crates/studiofs-winfsp` mounted smoke harness.
- fio: `fio-3.42` at `C:\Program Files\fio\fio.exe`.
- Mount point used for the focused run: `Z:`.

## Decision

Linux remains synchronous by default.

The Linux mounted checks passed in default synchronous mode, async write opt-in
mode, async write plus background tuning, async write plus 64 KiB writer
pages, and the `STUDIOFS_FUSE_ASYNC_DIO=1` control. The measured elapsed/RSS
values were effectively indistinguishable at this small mounted smoke scale.
The fio multi-job write smoke also passed, which validates the opt-in path
against a real multi-job writer, but its 2 MiB fsync-heavy payload is too small
to justify making Linux async write replies default-on.

## Commands

All commands used:

```bash
cd /tmp/studiofs-cli-sfs473
export CARGO_TARGET_DIR=/tmp/studiofs-cli-sfs473/target
export STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1

cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_default_sync -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_opt_in -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_dio_opt_in -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_fio_multi_job -- --ignored --exact --nocapture

$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'
$env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'
$env:STUDIOFS_WINFSP_FIO_PATH='C:\Program Files\fio\fio.exe'
cargo test -p studiofs-winfsp --test mount_sanity winfsp_mount_fio_multi_job_smoke -- --ignored --exact --nocapture

STUDIOFS_FUSE_MAX_BACKGROUND=50 \
STUDIOFS_FUSE_CONGESTION_THRESHOLD=40 \
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_opt_in -- --ignored --exact --nocapture

STUDIOFS_WRITER_PAGE_SIZE_BYTES=64KB \
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_opt_in -- --ignored --exact --nocapture

STUDIOFS_FUSE_ASYNC_WRITE=1 \
cargo test -p studiofs-fuse --test mount_sanity async_mounted_write_fsync_readback -- --ignored --exact --nocapture

STUDIOFS_FUSE_ASYNC_WRITE=1 \
cargo test -p studiofs-fuse --test mount_sanity async_mounted_write_flush_readback -- --ignored --exact --nocapture

STUDIOFS_FUSE_ASYNC_WRITE=1 \
cargo test -p studiofs-fuse --test mount_sanity async_mounted_unmount_after_async_write -- --ignored --exact --nocapture
```

The repeated timing wrapper was:

```bash
/usr/bin/time -f "elapsed=%e maxrss_kb=%M cpu=%P" env ... cargo test ...
```

## Matrix

| Case | Workload | Result | Median harness time | Max RSS range |
|---|---|---:|---:|---:|
| default synchronous mode | sequential write/readback | pass | 0.14 s | 95.0-95.2 MiB |
| async write opt-in | sequential write/readback | pass | 0.15 s | 95.0-95.6 MiB |
| async write plus background tuning | sequential write/readback with `STUDIOFS_FUSE_MAX_BACKGROUND=50`, `STUDIOFS_FUSE_CONGESTION_THRESHOLD=40` | pass | 0.14 s | 95.0-95.4 MiB |
| async write plus 64 KiB writer pages | sequential write/readback with `STUDIOFS_WRITER_PAGE_SIZE_BYTES=64KB` | pass | 0.15 s | 95.0-95.4 MiB |
| optional `STUDIOFS_FUSE_ASYNC_DIO=1` comparison | sequential write/readback with async DIO requested | pass | 0.16 s | 95.0-95.2 MiB |
| fsync-heavy workload | async write opt-in, write plus `fsync` barrier | pass | 0.15 s | 95.0-95.6 MiB |
| dirty-pressure workload | async write opt-in, write-only-until-unmount drain | pass | 0.26 s | 95.0-95.4 MiB |
| multi-job write | `linux_async_mounted_fio_multi_job`, fio 2 jobs, 64 KiB writes, 2 MiB total, `fsync=1` | pass | 1.78 s mounted test time | 95.2-95.4 MiB |
| Windows WinFSP fio comparison | `winfsp_mount_fio_multi_job_smoke`, fio 2 jobs, 64 KiB writes, 2 MiB total, `fsync=1` | pass | 4.82 s mounted test time | not captured |

## fio results

The Linux fio mounted smoke used `fio-3.36` through
`linux_async_mounted_fio_multi_job`.

| Run | fio write bandwidth | fio runtime | fsync average | harness elapsed | max RSS |
|---|---:|---:|---:|---:|---:|
| 1 | 1603 KiB/s | 1278 ms | 58.99 ms | 1.87 s | 95.2 MiB |
| 2 | 1494 KiB/s | 1371 ms | 80.37 ms | 1.93 s | 95.4 MiB |
| 3 | 1504 KiB/s | 1362 ms | 77.65 ms | 1.93 s | 95.2 MiB |

## Windows WinFSP fio comparison

The Windows comparison used `fio-3.42` through
`winfsp_mount_fio_multi_job_smoke`. The test originally failed when fio was
passed `--directory=Z:\` or `--directory=Z:/`, because this Windows fio build
parses drive-letter paths as `Z`. The committed WinFSP smoke avoids that by
setting the child process current directory to the mounted drive root and using
relative `--filename=fio.bin`.

| Run | fio write bandwidth | fio runtime | fsync average | mounted test time | harness elapsed |
|---|---:|---:|---:|---:|---:|
| 1 | 991 KiB/s | 2066 ms | 96.61 ms | 2.93 s | 3.43 s |
| 2 | 1054 KiB/s | 1943 ms | 94.96 ms | 4.82 s | 5.34 s |
| 3 | 1059 KiB/s | 1934 ms | 94.84 ms | 4.86 s | 5.38 s |

The Windows fio output also reports that process-shared mutexes are unavailable
on this platform and therefore fio starts threads. This is expected for this
fio build and does not indicate a StudioFS write failure.

## Observations

- The default synchronous mode and async write opt-in mode both pass mounted
  sequential write/readback.
- The background queue policy and `64 KiB` writer page controlled experiments
  did not show a measurable benefit in this smoke-scale WSL run.
- `STUDIOFS_FUSE_ASYNC_DIO=1` also passed, which keeps it available as a Linux
  benchmark/control knob.
- The fio multi-job write workload ran for real with `fio-3.36`, wrote 2 MiB
  through two jobs, and completed without writeback or barrier failures.
- The Windows WinFSP fio comparison ran for real with `fio-3.42`, wrote 2 MiB
  through two jobs on the mounted `Z:` drive, and completed without WinFSP or
  VFS writeback failures.
- Barrier-heavy workloads passed through flush, fsync, close/reopen, and
  unmount-after-write checks, so no correctness regressions appeared in
  barrier-heavy workloads.
- Memory caps prevented pending async write byte growth under stress at this
  scale: the process max RSS stayed around 95 MiB across the sync and async
  variants. The current mounted smoke tests do not emit the live
  `pending async write bytes` counter directly; for a throughput decision, use
  a full mounted `studiofs test-cache` report or add a larger Linux benchmark
  harness that records `mounted_observability.write_callback`.
- CPU stayed in the same broad band for the cargo-test harness. Because this is
  dominated by test process overhead, CPU data here should only be used as a
  regression smoke signal.

## Follow-up benchmark before default-on

Before changing Linux from opt-in to default-on, run a larger mounted throughput
sweep on an ext4-backed Linux machine:

```bash
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 \
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_fio_multi_job -- --ignored --exact --nocapture
```

Then run mounted `studiofs test-cache` sequential write, multi-job write,
dirty-pressure workload, fsync-heavy workload, CPU, memory, and pending async
write bytes capture for both default synchronous mode and async write opt-in.
The current fio smoke validates correctness for the async path, but it does not
compare default synchronous mode against async mode with a sustained payload.
Keep Linux writer page-size default unchanged unless that larger benchmark set
shows a clear improvement without memory or correctness regressions.

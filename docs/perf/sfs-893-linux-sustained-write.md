# SFS-893 Linux Sustained Sequential Write Attribution

SFS-893 tracked a sustained-write curve from the 2026-10-24 Linux audit: four
back-to-back 2 GiB fio sequential writes dropped from 1301 to 319 MiB/s while
the upload backlog grew to about 2.4 GB. On that stand, RustFS (S3), the
StudioFS cache, and redb shared one WSL2 vhdx disk. This report re-measures the
curve with S3 on a separate host and attributes it.

**Verdict:** the curve comes from the stand's local cache disk, not from a
StudioFS gate. Foreground writes do not depend on upload progress. The time
lost in slow runs is spent in the `end_fsync` drain into local cache files.
Those files sit behind host page-cache writeback to the vhdx. StudioFS write
admission and hard backpressure stay idle except for one 385 ms episode in one
of twelve ext4 runs.

## Stand

| Item | Value |
|---|---|
| Revision | `7b66e51af`, release build |
| Host | WSL2 kernel `6.6.114.1-microsoft-standard-WSL2`, 24 vCPU, 46 GiB RAM |
| Object storage | Remote S3 endpoint on a separate public host, reached over the internet |
| PostgreSQL | Separate host on the private network |
| Cache disk "ext4" | `/dev/sdd`, the WSL2 vhdx (ext4); the only Linux block device |
| Cache disk "tmpfs" | `/dev/shm` (RAM); stands in for an uncontended fast disk |
| Kernel writeback | `vm.dirty_background_ratio=10`, `vm.dirty_ratio=20` (defaults) |
| Workload | `fio --rw=write --bs=1M --size=2G --ioengine=psync --numjobs=1 --end_fsync=1`, 4 files back to back |
| Cache mode | Default throughput-first Fast cache mode |

A dedicated NVMe was not available: WSL2 exposes one block device, and the
Windows drives are 9p mounts that do not behave like a local disk. The tmpfs
configuration removes the local disk from the path, which is the question this
issue asks. Remote S3 removes the object store from the local disk.

Each series initializes a fresh volume, mounts it with
`STUDIOFS_CLIENT_ROOT` on the cache disk under test, runs
[`tools/perf/linux-sustained-write.sh`](../../tools/perf/linux-sustained-write.sh),
unmounts, and destroys the volume. Every volume was destroyed after its series.
The script records per run:

- fio throughput;
- foreground write time (sum of psync request latency) and `end_fsync` time;
- sectors written to the cache block device and host `Dirty+Writeback`;
- upload pending bytes from `.studiofs-runtime-stats`;
- write-admission wait, hard-backpressure events, and local cache I/O time
  (`write_path_observability.json`).

```sh
bash tools/perf/linux-sustained-write.sh --target <mount>/<volume> \
  --cache-root "$STUDIOFS_CLIENT_ROOT" --runs 4 --size 2G
bash tools/perf/linux-sustained-write.sh --target <raw-dir-on-cache-disk>
```

## Results

Three series for each cache placement, two for the `sync` control, and three
raw-disk series. Each cell lists the series values, then the median.

### Throughput, MiB/s

| Run | ext4 cache | tmpfs cache | ext4 cache, `sync` between runs | raw ext4, fio only |
|---|---|---|---|---|
| 1 | 1495 / 1509 / 1419 → **1495** | 1486 / 1326 / 1205 → **1326** | 1179 / 994 → **1086** | 717 / 797 / 684 → **717** |
| 2 | 2022 / 2038 / 1728 → **2022** | 2056 / 1436 / 1298 → **1436** | 1394 / 1231 → **1312** | 1489 / 717 / 401 → **717** |
| 3 | 1525 / 647 / 1112 → **1112** | 1936 / 1666 / 1374 → **1666** | 1341 / 1422 → **1382** | 918 / 495 / 431 → **495** |
| 4 | 1426 / 683 / 1642 → **1426** | 1634 / 1560 / 1267 → **1560** | 770 / 1308 → **1039** | 696 / 634 / 429 → **634** |

Upload pending bytes grew by about 2 GiB per run in every StudioFS series:
2.0, 4.0, 5.9, and 7.8 GiB. Remote upload completed well under 1 GiB during a
series.

### Where the time goes

| Run | ext4 `end_fsync`, ms | ext4 disk written, MiB | ext4 cache I/O, ms | tmpfs `end_fsync`, ms | `sync`-control `end_fsync`, ms |
|---|---|---|---|---|---|
| 1 | 37 / 34 / 36 | 16 / 17 / 17 | 162 / 66 / 41 | 15 / 22 / 25 | 49 / 63 |
| 2 | 38 / 34 / 40 | 18 / 17 / 18 | 72 / 104 / 62 | 14 / 86 / 22 | 59 / 44 |
| 3 | 183 / 1235 / 522 | 1777 / 1595 / 1715 | 131 / 4158 / 406 | 17 / 18 / 45 | 70 / 33 |
| 4 | 313 / 1473 / 71 | 475 / 1873 / 1169 | 242 / 5731 / 132 | 16 / 19 / 110 | 87 / 66 |

Foreground write time stays at 0.97 to 1.5 s per 2 GiB in eleven of twelve
ext4 runs; the slow ext4 runs lose their time in `end_fsync`. The exception,
at 1.9 s, is ext4 series 2, run 3. There the writer stack reached its hard window once:
385 ms admission wait and 3 hard-backpressure events. It was the only nonzero
hard-backpressure count in the matrix. Admission wait was 2 to 7 ms per run
everywhere else. Peak mount RSS was 1.5 to 2.1 GiB, and 3.0 GiB in that ext4
series.

## Attribution

1. **The upload backlog does not throttle foreground writes.** tmpfs series
   carry the same 2 to 8 GiB pending upload to a slow remote S3. They show no
   decline, and their `end_fsync` stays at 14 to 110 ms. This matches the
   Local-First contract: foreground writes do not wait for remote upload.
2. **The slowdown starts when the kernel starts writing the cache disk.** In
   Fast cache mode, `fsync` drains StudioFS state into local payload files but
   does not fsync them (see
   [`juicefs-local-first-rust-contract.md`](../juicefs-local-first-rust-contract.md#4-flush-fsync-and-close)).
   The first two ext4 runs therefore end with about 2 and 4 GiB of host dirty
   pages and only 16 to 18 MiB on disk. Run 3 crosses the 10%
   `dirty_background_ratio` threshold, about 4.5 GiB here. Background writeback
   then writes up to 1.9 GiB per run. From that point, draining the writer
   stack into cache files competes with the vhdx writeback and dirty
   throttling. `end_fsync` and cache I/O time grow by more than an order of
   magnitude.
3. **Draining host dirty pages between runs removes the `end_fsync` growth.**
   The `sync` control keeps `end_fsync` at 33 to 87 ms in all eight runs. One
   dip in the control (770 MiB/s) came from slower foreground writes, not
   `end_fsync`. It had 7 ms of admission wait and no hard backpressure, so it is
   stand noise.
4. **The raw disk has the same ceiling.** With the identical fio job, the
   vhdx gives 400 to 1500 MiB/s with high run-to-run variance. Its median is
   495 to 717 MiB/s. StudioFS on ext4 stays at or above the raw disk because
   Fast cache mode does not force payload fsync.

The audit stand also wrote every uploaded byte to RustFS on the same vhdx.
That doubles local disk writes and explains the steeper 4x decline to
319 MiB/s.
With S3 off the disk, the worst ext4 series still fell from 2038 to 647 MiB/s.
The ext4 medians stayed at 1112 to 2022 MiB/s.

No StudioFS gate change is indicated by this evidence. A slower or contended
cache disk lowers the sustained rate to the disk's writeback bandwidth. That is
expected for a local-first write path that must eventually land bytes on the
cache disk.

## Side finding

Every mount in this matrix returned `EACCES` for a create in the mount root for
about 400 ms after `.studiofs-runtime-stats` became readable. That was two
failed probes per mount, then success, with 17 failures in total. The series
driver waits for a successful create before starting fio. This is unrelated to
throughput and is tracked separately.

## Limits

- One host and WSL2 only. A native Linux host with a dedicated NVMe cache would
  move the knee to that disk's writeback rate. It would not change the
  attribution.
- Durable cache mode was not measured. It fsyncs payload files, so each run is
  bounded by the cache disk from the first run, like the raw-disk column.
- fio `end_fsync` time is derived from job runtime minus summed psync request
  latency; fio does not report a separate latency for the final fsync.

# SFS-509 Linux FUSE ext4 throughput baseline

Date: 2026-06-15

## Scope

SFS-509 is the decision gate before changing Linux FUSE defaults. It extends
the SFS-473 WSL2 smoke run into a sustained native Linux 6.x ext4 baseline that
can rank the current bottlenecks instead of only proving correctness.

The required workload surface is:

- sequential read/write;
- random read/write;
- mixed read/write;
- multi-job;
- fsync-heavy;
- metadata-heavy readdir/stat;
- cold-cache;
- warm-cache;
- dirty-pressure;
- large-file.

The required comparison surface is:

- StudioFS current defaults;
- async write opt-in;
- async_dio opt-in;
- FUSE background queue profiles;
- any async-read prototype, when one exists;
- JuiceFS on the same machine.

The required capture surface is:

- StudioFS status snapshots;
- pending async write bytes;
- FUSE callback latency histograms;
- CPU profiles and flamegraphs;
- RSS;
- syscall rates;
- context switches.

## Host preflight

The 2026-06-15 local preflight host is native Linux, not WSL drvfs:

```text
Linux devbench 7.0.0-22-generic #22-Ubuntu SMP PREEMPT_DYNAMIC Mon May 25 15:54:34 UTC 2026 x86_64 GNU/Linux
workspace filesystem: ext4 /dev/mapper/ubuntu--vg-ubuntu--lv /
workspace free space: 14G available on /
/dev/fuse: present
fusermount3: /usr/bin/fusermount3
perf: /usr/bin/perf
perf_event_paranoid: 1
fio: fio-3.41
juicefs: juicefs version 1.3.1+2025-12-02.e0032b2
flamegraph: not found
```

This host satisfies the native Linux 6.x and ext4 backing-storage requirement,
and it is not WSL drvfs. `fio`, `juicefs`, and `perf record` are available.
`perf record` still emitted kernel symbol restriction warnings because
`kptr_restrict` prevents resolving some kernel symbols without root. A
flamegraph renderer is still absent, so folded/SVG flamegraph generation was
not completed in this pass.

## Artifact layout

All generated files for the full run must stay under:

```text
target/sfs-509-linux-fuse-ext4-throughput/
```

Suggested subdirectories:

| Directory | Contents |
|---|---|
| `preflight/` | kernel, filesystem, package, and tool versions |
| `fio/` | fio job files and JSON outputs |
| `studiofs/` | StudioFS logs, status snapshots, observability JSON, and mount configs |
| `perf/` | `perf stat`, `perf record`, folded stacks, and flamegraphs |
| `sys/` | RSS, syscall rates, context switches, vmstat/iostat samples |
| `juicefs/` | JuiceFS mount config, fio JSON, and profile artifacts |

Temporary files must not be written outside `target/`.

## Command matrix

The focused documentation gate is:

```bash
cargo test -p studiofs --test architecture_vocabulary_test linux_fuse_ext4_throughput_baseline_report_covers_sfs509_decision_gate -- --nocapture
```

Preflight:

```bash
mkdir -p target/sfs-509-linux-fuse-ext4-throughput/preflight
uname -a | tee target/sfs-509-linux-fuse-ext4-throughput/preflight/uname.txt
findmnt -T "$PWD" -no FSTYPE,SOURCE,TARGET | tee target/sfs-509-linux-fuse-ext4-throughput/preflight/findmnt.txt
df -h "$PWD" | tee target/sfs-509-linux-fuse-ext4-throughput/preflight/df.txt
command -v fusermount3 fio perf juicefs flamegraph inferno-flamegraph \
  | tee target/sfs-509-linux-fuse-ext4-throughput/preflight/tools.txt
cat /proc/sys/kernel/perf_event_paranoid \
  | tee target/sfs-509-linux-fuse-ext4-throughput/preflight/perf_event_paranoid.txt
```

StudioFS mounted correctness controls:

```bash
export STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_default_sync -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_opt_in -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_dio_opt_in -- --ignored --exact --nocapture
cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_fio_multi_job -- --ignored --exact --nocapture
```

Sustained fio jobs should be emitted as JSON, one file per workload and
comparison variant:

```bash
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-seq-write.json <jobfile>
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-seq-read.json <jobfile>
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-rand-write.json <jobfile>
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-rand-read.json <jobfile>
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-mixed-rw.json <jobfile>
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-fsync-heavy.json <jobfile>
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-metadata-readdir-stat.json <jobfile>
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-dirty-pressure.json <jobfile>
fio --output-format=json --output target/sfs-509-linux-fuse-ext4-throughput/fio/<variant>-large-file.json <jobfile>
```

Profiling and system counters:

```bash
perf stat -d -d -d -o target/sfs-509-linux-fuse-ext4-throughput/perf/<variant>-<workload>.perf-stat.txt -- <workload-command>
perf record -F 99 -g -o target/sfs-509-linux-fuse-ext4-throughput/perf/<variant>-<workload>.perf.data -- <workload-command>
perf script -i target/sfs-509-linux-fuse-ext4-throughput/perf/<variant>-<workload>.perf.data > target/sfs-509-linux-fuse-ext4-throughput/perf/<variant>-<workload>.perf.script
```

Use `flamegraph` or `inferno-flamegraph` to convert the recorded stacks after
`perf record` succeeds.

## Run status

| Surface | 2026-06-15 status |
|---|---|
| Native Linux 6.x ext4 host | available |
| FUSE device and `fusermount3` | available |
| StudioFS current defaults | mounted control passed; 512 MiB e2e sequential probe passed |
| async write opt-in | mounted control passed |
| async_dio opt-in | mounted control passed |
| FUSE background queue profiles | not run as a long-lived fio sweep; still requires `STUDIOFS_FUSE_MAX_BACKGROUND` and `STUDIOFS_FUSE_CONGESTION_THRESHOLD` variants |
| async-read prototype | not available in this workspace at this decision point |
| JuiceFS | local sqlite/file-backed JuiceFS baseline passed |
| fio workloads | ext4 and JuiceFS reference matrix passed; StudioFS has fio mounted smoke only |
| CPU profiles/flamegraphs | StudioFS `perf record` captured 345 samples; flamegraph rendering blocked because no renderer is installed |

## Commands run

StudioFS mounted controls:

```bash
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_default_sync -- --ignored --exact --nocapture
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_opt_in -- --ignored --exact --nocapture
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_async_dio_opt_in -- --ignored --exact --nocapture
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity linux_async_mounted_fio_multi_job -- --ignored --exact --nocapture
```

StudioFS sustained sequential probe:

```bash
cargo build -p studiofs
STUDIOFS_E2E_PERF_FILE_BYTES=536870912 \
STUDIOFS_E2E_PERF_READ_PASSES=2 \
STUDIOFS_E2E_PERF_CHUNK_BYTES=4194304 \
STUDIOFS_E2E_PERF_SAMPLE_AFTER_PHASE_MS=500 \
STUDIOFS_E2E_PERF_UPLOAD_DRAIN_TIMEOUT_SECS=300 \
cargo run -p studiofs-e2e -- run mounted-performance-probe \
  --studiofs-bin target/debug/studiofs \
  --root target/e2e-runs \
  --run-id sfs509-mounted-performance-probe \
  --platform linux-fuse \
  --retain always \
  --report target/sfs-509-linux-fuse-ext4-throughput/e2e/mounted-performance-probe-report.json
```

JuiceFS reference setup:

```bash
juicefs format --force --storage file --bucket target/sfs-509-linux-fuse-ext4-throughput/juicefs/storage --trash-days 0 sqlite3://target/sfs-509-linux-fuse-ext4-throughput/juicefs/meta.db sfs509-jfs
juicefs mount -d --no-syslog --cache-dir target/sfs-509-linux-fuse-ext4-throughput/juicefs/cache --cache-size 2048 --writeback --backup-meta 0 sqlite3://target/sfs-509-linux-fuse-ext4-throughput/juicefs/meta.db target/sfs-509-linux-fuse-ext4-throughput/juicefs/mount
```

Artifacts:

- `target/sfs-509-linux-fuse-ext4-throughput/preflight/`
- `target/sfs-509-linux-fuse-ext4-throughput/studiofs/mount-sanity/`
- `target/sfs-509-linux-fuse-ext4-throughput/e2e/mounted-performance-probe-report.json`
- `target/sfs-509-linux-fuse-ext4-throughput/e2e/artifacts/performance-probe-summary.json`
- `target/sfs-509-linux-fuse-ext4-throughput/fio/`
- `target/sfs-509-linux-fuse-ext4-throughput/perf/`
- `target/sfs-509-linux-fuse-ext4-throughput/sys/`

## StudioFS results

Mounted smoke controls:

| Case | Result | Harness elapsed | Max RSS |
|---|---:|---:|---:|
| StudioFS current defaults, sequential write/readback | pass | 2.61 s | 97,436 KiB |
| async write opt-in, sequential write/readback | pass | 0.82 s | 97,540 KiB |
| async_dio opt-in, sequential write/readback | pass | 0.80 s | 97,852 KiB |
| async write fio multi-job smoke | pass | 3.16 s | 97,108 KiB |

The StudioFS fio mounted smoke used 2 jobs, 64 KiB writes, 2 MiB total, and
`fsync=1`. It reported 930-946 KiB/s across the normal and profiled runs, with
fsync averages of 122-126 ms. This remains smoke-scale and should not be used
as the sustained throughput decision by itself.

The e2e mounted-performance-probe wrote a 512 MiB file through the real Linux
FUSE mount and read it twice:

| Phase | Bytes | Elapsed | Throughput | Interpretation |
|---|---:|---:|---:|---|
| write | 512 MiB | 11.393 s | 44.94 MiB/s | write accepted through FUSE |
| read pass 1 | 512 MiB | 32.255 s | 15.87 MiB/s | read reentered FUSE while upload backlog drained |
| read pass 2 | 512 MiB | 1.400 s | 365.53 MiB/s | read reentered FUSE after backlog drained |
| post-drain read | 512 MiB | 0.122 s | 4175.47 MiB/s | OS cache hit, no FUSE callbacks |

Important StudioFS observations:

- write path recorded 4,100 FUSE write callbacks for 512 MiB, averaging about
  130,944 bytes per callback;
- after the write phase, pending async write bytes reached 448,790,528 bytes
  across 109 pending uploads;
- the first read pass overlapped upload backlog drain and averaged only
  15.87 MiB/s;
- after the backlog drained, the second read pass reached 365.53 MiB/s;
- the post-drain read did not reenter FUSE and is an OS page-cache hit, not
  StudioFS read-path throughput.

## ext4 and JuiceFS fio reference

The reference fio sweep used 128 MiB files for sequential/random/mixed probes,
64 MiB for multi-job and fsync-heavy writes, and portable `perf stat` counters
for task-clock, context switches, migrations, and page faults. The original
`perf stat -d -d -d` command failed on this host because the PMU exposed
unsupported `cpu_atom` events.

Throughput values are MiB/s.

| Variant | Workload | Read MiB/s | Write MiB/s | Read IOPS | Write IOPS |
|---|---|---:|---:|---:|---:|
| ext4 | sequential write | 0.00 | 498.05 | 0 | 498 |
| ext4 | sequential read | 2000.00 | 0.00 | 2000 | 0 |
| ext4 | random write | 0.00 | 7.65 | 0 | 1959 |
| ext4 | random read | 41.76 | 0.00 | 10690 | 0 |
| ext4 | mixed read/write | 105.49 | 45.31 | 6752 | 2900 |
| ext4 | multi-job write | 0.00 | 8.24 | 0 | 132 |
| ext4 | fsync-heavy write | 0.00 | 1.36 | 0 | 87 |
| ext4 | cold-cache read | 256.00 | 0.00 | 256 | 0 |
| JuiceFS | sequential write | 0.00 | 379.82 | 0 | 380 |
| JuiceFS | sequential read | 853.33 | 0.00 | 853 | 0 |
| JuiceFS | random write | 0.00 | 1.25 | 0 | 320 |
| JuiceFS | random read | 9.42 | 0.00 | 2413 | 0 |
| JuiceFS | mixed read/write | 4.24 | 1.89 | 271 | 121 |
| JuiceFS | multi-job write | 0.00 | 17.67 | 0 | 283 |
| JuiceFS | fsync-heavy write | 0.00 | 7.29 | 0 | 467 |
| JuiceFS | cold-cache read | 256.00 | 0.00 | 256 | 0 |

Metadata-heavy readdir/stat over 5,000 files:

| Variant | Create elapsed | stat/readdir elapsed |
|---|---:|---:|
| ext4 | 0.19 s | 0.05 s |
| JuiceFS | 20.22 s | 0.24 s |

The JuiceFS reference uses local sqlite metadata and local file object storage
under `target/`, so it is a same-host reference, not a Redis/S3 production
JuiceFS deployment.

## Bottleneck ranking

This bottleneck ranking is based on the completed local pass. The evidence is
enough to rank the measured StudioFS bottlenecks
for the covered workloads, but not enough to rank unrun variants such as FUSE
background queue profiles or a future async-read prototype:

1. Read throughput collapses while upload backlog is draining: the same 512 MiB
   file read at 15.87 MiB/s during backlog drain and 365.53 MiB/s after drain.
2. The write path can admit data faster than it uploads it: 44.94 MiB/s write
   acceptance left 448,790,528 pending async write bytes.
3. StudioFS read callbacks still reenter FUSE and foreground local payload I/O
   after the backlog clears; the fast post-drain read is an OS cache hit, not
   proof of a faster StudioFS read path.
4. Smoke-scale fsync-heavy StudioFS writes remain slow at about 930-946 KiB/s
   for the 2 MiB fio mounted test.
5. Background queue tuning, readdirplus/opendir handle state, and kernel read
   buffer changes remain unmeasured in this pass.

## Default-on/default-off recommendations

Based on this pass, keep the Linux defaults unchanged:

- keep async write default-off;
- keep async_dio default-off;
- keep FUSE background queue tuning opt-in;
- keep any async-read prototype out of defaults until it has sustained
  read-path evidence;
- add a long-lived StudioFS fio harness before making a default-on decision for
  the full random/mixed/metadata/cold-cache matrix.

The default-on/default-off recommendations can be revisited after a follow-up
run covers the missing StudioFS fio matrix variants, FUSE callback latency
histograms, rendered flamegraphs, and background queue profiles.

# Cross-Platform Benchmark Validation

SFS-557 standardized how StudioFS developer benchmark evidence is collected on
Windows, macOS, and Linux. SFS-567 keeps that script surface focused on
`studiofs devbench`, while public examples use the fixed `studiofs bench`
contract. The scripts in `tools/perf` run `studiofs devbench` against a
user-provided mount point and write artifacts under `target/perf`.

The scripts are:

- Windows: `tools/perf/validate-bench-windows.ps1`
- Windows SFS-612 metadata-create parity: `tools/perf/sfs-612-windows-metadata-create.ps1`
- macOS: `tools/perf/validate-bench-macos.sh`
- Linux: `tools/perf/validate-bench-linux.sh`

The macOS and Linux entrypoints are thin wrappers around the shared
`tools/perf/validate-bench-unix.sh` implementation so option parsing, artifact
layout, and benchmark invocation behavior stay in sync across Unix platforms.

## Usage

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\perf\validate-bench-windows.ps1 -MountPoint Z:\
powershell -ExecutionPolicy Bypass -File .\tools\perf\validate-bench-windows.ps1 -MountPoint Z:\ -StudioFsPath .\target\release\studiofs.exe
```

macOS:

```bash
bash ./tools/perf/validate-bench-macos.sh --mount-point /Volumes/studiofs
bash ./tools/perf/validate-bench-macos.sh --mount-point /Volumes/studiofs --studiofs target/release/studiofs
```

Linux:

```bash
bash ./tools/perf/validate-bench-linux.sh --mount-point /mnt/studio/myvolume
bash ./tools/perf/validate-bench-linux.sh --mount-point /mnt/studio/myvolume --studiofs target/release/studiofs
bash ./tools/perf/validate-bench-linux.sh --mount-point /mnt/studio/myvolume --profile write-path-gate
```

The only required platform-specific private path is the mount point. All other
benchmark settings have explicit defaults and can be overridden through script
arguments.
Pass `--profile write-path-gate` when the run is collecting SFS-549
single-file write-path evidence.

If `studiofs` is not available in `PATH`, the scripts automatically try the
workspace `target/release/studiofs` and `target/debug/studiofs` build outputs
for the default `studiofs` command name. You can also pass an explicit binary
with `-StudioFsPath` on Windows or `--studiofs` on macOS/Linux.
On Windows, `-StudioFsPath target\release\studiofs` also resolves the matching
`.exe` file when it exists.

## SFS-612 Windows Metadata-Create Baseline

SFS-612 measures the cost of creating local metadata through two mounted
filesystems on the same Windows host: a StudioFS WinFSP mount and a JuiceFS
reference mount. The harness copies the same Curvine source tree and then
creates 10,000 fresh 4 KiB files. It runs three controlled cold/warm pairs per
filesystem by default and writes raw per-run timings plus median summaries.

Before running it, mount both filesystems on different drive letters backed by
the comparable local metadata/cache storage class. Supply a command that
prepares the cold-cache state; the script intentionally refuses to label a run
as cold without this operator-controlled step. Record any antivirus, indexing,
or storage exceptions in the artifact metadata.

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\perf\sfs-612-windows-metadata-create.ps1 `
  -StudioFsMountPoint Z:\ `
  -JuiceFsMountPoint Y:\ `
  -CurvineSource D:\src\curvine `
  -ColdPreparationCommand '<your controlled cache preparation command>' `
  -StudioFsMountOptions '<recorded WinFSP and StudioFS options>' `
  -JuiceFsMountOptions '<recorded JuiceFS options>' `
  -StorageNotes '<metadata and cache disk class>' `
  -AntivirusAndIndexingNotes '<status or exclusions>'
```

The invocation creates
`target/perf/sfs-612-windows-metadata-create-<timestamp>/` with:

- `environment.json`: host, filesystem, mount, cache-control, storage, and
  antivirus/indexing configuration;
- `results.json`: every StudioFS/JuiceFS, cold/warm, raw timing sample; and
- `summary.md`: median Curvine-copy, 4 KiB-create, and total times.

This Windows-first pass does not claim Linux or macOS acceptance evidence.
Their matching same-host StudioFS/JuiceFS comparison remains a documented
follow-up, and their platform adapter checks must run on those operating
systems before SFS-612 is marked cross-platform complete.

The mounted runtime's `write_path_observability` JSON now also exposes the
shared namespace-create breakdown: `namespace_create.inode_allocation`,
`namespace_create.namespace_mutation`, completed entries, transactions per
created entry, and `metadata_engine.begin_write`, `mutation`, and `commit`.
For synchronous namespace creates, `namespace_mutation` measures the direct
metadata-engine call. Deferred creates measure the same stage around the async
runtime future, so their value also includes metadata executor queue wait.
`payload_store_sync_data` remains the explicit local-payload `sync_data`
measurement. redb commits are durable commit boundaries; its public API does
not expose a separate post-commit `sync_data` call to time independently.

### Accepted Windows baseline

The SFS-612 Windows baseline is accepted from the operator's manual timing on
2026-07-21 using the supplied `Z:\` StudioFS and `W:\` JuiceFS mounts with
`E:\dev\curvine` as the source tree:

| Filesystem | Observed time | Relative result |
|---|---:|---:|
| JuiceFS | 6 s | 1.0x reference |
| StudioFS | 75 s (1 min 15 s) | 12.5x JuiceFS time |

This is the accepted Windows reference baseline for SFS-612. It is a manual
single observation, not a host-cache purge or a three-run cold/warm distribution;
Linux/macOS adapter validation and measurements remain deferred.

## SFS-598 Linux JuiceFS Parity Gate

SFS-598 gates the new Linux backend before default rollout. The gate compares
already-mounted paths for local disk/cache, the new StudioFS Linux backend, the
old Linux backend, and JuiceFS on the same host:

```bash
bash ./tools/perf/linux-juicefs-parity-gate.sh \
  --local-target /mnt/local-cache \
  --studiofs-new-target /mnt/studiofs-new \
  --studiofs-old-target /mnt/studiofs-old \
  --juicefs-target /mnt/juicefs
```

The script uses `target/bench/studiofs-bench` for portable throughput workloads
and small POSIX smoke workloads for metadata scan, source repository copy,
truncate/rewrite, fsync-heavy behavior, and optional DCC compatibility smoke.
It writes a non-overwriting artifact directory under `target/perf` with
`environment.json`, per-target/per-workload `command.txt`, `stdout.txt`,
`stderr.txt`, `metrics.json`, optional `/usr/bin/time` output, `summary.md`,
and `decision.md`.

Use `--dry-run` for CI or review checks that only need to validate the command
matrix and artifact layout. Fill in `decision.md` after reviewing side-by-side
throughput, wall time, CPU, warning/error counts, unsupported ioctl counts,
StudioFS runtime counters, cache behavior, queue depth, and publication/upload
lag evidence.

## Public Benchmark Example

For user-facing validation notes, prefer the fixed public command:

```bash
studiofs bench <mountpoint> --report <path>
```

`studiofs bench` has no public workload tuning flags. It runs the fixed
pseudo-random workload, is repeatable across machines, and saves JSON only when
the optional `--report <path>` flag is present. Use `studiofs devbench` and the
scripts below only when the evidence needs developer knobs such as
`--block-size`, `--big-file-size`, `--small-file-count`, or `--threads`.

The public report includes first-read phases, but those reads are not a
guaranteed fully cold OS-cache measurement unless the operator separately
controls host cache state before each run.

## SFS-549 Linux Write-Path Evidence Gate

SFS-549 uses `studiofs devbench --profile write-path-gate` as the StudioFS
write-path evidence gate. The profile runs the developer benchmark as a
`single_file_sequential` workload with `payload_file_count` equal to `1`: one
large sequential write, a repeated sequential read of the same file, and the
post-drain warm read that follows upload idle observation.

Example Linux command:

```bash
bash ./tools/perf/validate-bench-linux.sh --mount-point /mnt/studio/myvolume --profile write-path-gate --big-file-size 8GB --block-size 4MB --threads 1
```

Use `--threads 1` for this profile. The validation scripts default to `1` when
`--profile write-path-gate` is provided and `--threads` is omitted. The gate
intentionally issues one sequential payload operation; larger explicit thread
counts may start extra workers, but only one worker can consume the single-file
operation while the rest exit idle.

Each `report.json` from this profile includes a parsed `write_path_gate`
section with `write_mib_per_sec`, `repeated_read_mib_per_sec`,
`post_drain_warm_read_mib_per_sec`, `cpu_percent`, `dirty_backlog`, and
`top_wait_counter_deltas`. Archive the raw command line, stdout, and JSON
report from each run directory. `fio is not required for the StudioFS side`;
use external fio only when collecting an optional JuiceFS or raw filesystem
baseline that cannot run `studiofs devbench`.

## Artifact Layout

Each invocation creates a unique directory and does not overwrite previous
benchmark artifacts:

```text
target/perf/<platform>-bench-validation-<timestamp>/
  environment.json
  summary.md
  run-001/
    command.txt
    stdout.txt
    report.json
  run-002/
    command.txt
    stdout.txt
    report.json
```

`environment.json` records the OS, platform, mount point, git commit,
StudioFS version, and benchmark settings. Each run directory records the exact
command line, stdout capture, and JSON report written by `studiofs devbench
--json-output`. When a profile is provided, `environment.json` records it and
each `command.txt` includes the matching `--profile` argument.

## Run Counts And Comparison

The recommended run count is 5 for before/after platform work. Use fewer runs
only for smoke checks, and record that choice in the PR or investigation notes.

For comparisons, compare medians before drawing conclusions:

1. Keep the same machine, mount point, cache disk, metadata disk, and benchmark
   settings.
2. Compare medians across runs first.
3. Inspect p95 and p99 latency for tail regressions.
4. Treat missing `runtime_stats` or missing mounted memory/process fields as an
   incomplete diagnostic report, not as proof of parity.
5. Archive both baseline and candidate `target/perf` directories.

These scripts do not run full test suites. They are benchmark evidence capture
tools for mounted StudioFS volumes.

## Linux Raw FUSE Profile Gate

Linux has one raw `/dev/fuse` runtime profile. Capture its evidence under
`target/perf/linux-raw-fuse/` and compare runs with the same machine, mount
point, cache disk, metadata disk, object store, PostgreSQL service, and workload
shape:

```sh
bash ./tools/perf/validate-bench-linux.sh --mount-point /mnt/studio/myvolume --profile write-path-gate --big-file-size 8GB --block-size 4MB --threads 1
```

Also run `studiofs bench <mountpoint> --report <path>` for fixed public mounted
evidence. The raw profile must keep median sequential write throughput,
repeated warm read throughput, p95/p99 callback latency, fsync/flush latency,
and RSS within the documented release thresholds.

# Cross-Platform Benchmark Validation

SFS-557 standardized how StudioFS developer benchmark evidence is collected on
Windows, macOS, and Linux. SFS-567 keeps that script surface focused on
`studiofs devbench`, while public examples use the fixed `studiofs bench`
contract. The scripts in `tools/perf` run `studiofs devbench` against a
user-provided mount point and write artifacts under `target/perf`.

The scripts are:

- Windows: `tools/perf/validate-bench-windows.ps1`
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

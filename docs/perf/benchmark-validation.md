# Cross-Platform Benchmark Validation

SFS-557 standardizes how StudioFS developer benchmark evidence is collected on
Windows, macOS, and Linux. The scripts in `tools/perf` run
`studiofs devbench` against a user-provided mount point and write artifacts
under `target/perf`.

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
```

The only required platform-specific private path is the mount point. All other
benchmark settings have explicit defaults and can be overridden through script
arguments.

If `studiofs` is not available in `PATH`, the scripts automatically try the
workspace `target/release/studiofs` and `target/debug/studiofs` build outputs
for the default `studiofs` command name. You can also pass an explicit binary
with `-StudioFsPath` on Windows or `--studiofs` on macOS/Linux.
On Windows, `-StudioFsPath target\release\studiofs` also resolves the matching
`.exe` file when it exists.

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
--json-output`.

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

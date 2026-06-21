# Cross-Platform Benchmark Validation

SFS-557 standardizes how StudioFS benchmark evidence is collected on Windows,
macOS, and Linux. The scripts in `tools/perf` run `studiofs bench` against a
user-provided mount point and write artifacts under `target/perf`.

The scripts are:

- Windows: `tools/perf/validate-bench-windows.ps1`
- macOS: `tools/perf/validate-bench-macos.sh`
- Linux: `tools/perf/validate-bench-linux.sh`

## Usage

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\perf\validate-bench-windows.ps1 -MountPoint Z:\
```

macOS:

```bash
bash ./tools/perf/validate-bench-macos.sh --mount-point /Volumes/studiofs
```

Linux:

```bash
bash ./tools/perf/validate-bench-linux.sh --mount-point /mnt/studio/myvolume
```

The only required platform-specific private path is the mount point. All other
benchmark settings have explicit defaults and can be overridden through script
arguments.

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
command line, stdout capture, and JSON report written by `studiofs bench
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

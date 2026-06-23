# StudioFS Perf Tools

This directory contains focused performance and benchmark evidence helpers.
For public benchmark notes, use the fixed command:

```powershell
studiofs bench <mountpoint> --report <path>
```

`studiofs bench` runs the fixed pseudo-random workload, is repeatable across
machines, and exposes only the mount point plus optional `--report`. Use
`studiofs devbench` when an investigation needs developer workload flags such
as `--block-size`, `--big-file-size`, `--small-file-count`, or `--threads`.
For SFS-549 Linux write-path evidence, use
`studiofs devbench --profile write-path-gate`; the validation scripts pass the
same profile through with `--profile write-path-gate`.
First-read output from the public benchmark is not a guaranteed fully cold
OS-cache measurement unless host cache state is controlled outside StudioFS.

The older PowerShell harnesses in this directory are repeatable loops for:

- smooth out jagged write throughput
- improve sequential read throughput where possible

Those harnesses intentionally use the legacy `studiofs test-cache` diagnostic
surface to isolate direct cache behavior. They are not the public mounted
benchmark contract.

For cross-platform `studiofs devbench` evidence capture, use the SFS-557/SFS-567
validation scripts:

- Windows: `validate-bench-windows.ps1`
- macOS: `validate-bench-macos.sh`
- Linux: `validate-bench-linux.sh`

The macOS and Linux entrypoints call the shared `validate-bench-unix.sh`
implementation to keep Unix argument handling and artifact generation aligned.

Those scripts write non-overwriting run directories under `target/perf` by
calling `studiofs devbench --json-output`; see
[`docs/perf/benchmark-validation.md`](../../docs/perf/benchmark-validation.md).

Linux SFS-549 example:

```bash
bash ./tools/perf/validate-bench-linux.sh --mount-point /mnt/studio/myvolume --profile write-path-gate --big-file-size 8GB --block-size 4MB --threads 1
```

The generated `report.json` contains `write_path_gate` with
`single_file_sequential`, `payload_file_count`, `write_mib_per_sec`,
`repeated_read_mib_per_sec`, `post_drain_warm_read_mib_per_sec`, `cpu_percent`,
`dirty_backlog`, and `top_wait_counter_deltas`. `fio is not required for the
StudioFS side`.

## Scope

For this pass we care about two focused scenarios:

- `write-jitter.ps1`: write-focused regression run
- `read-speed.ps1`: read-focused regression run

Both scripts:

- run `studiofs test-cache`
- force a reproducible single-size workload via `--sizes <item-size>` plus `--workload-repeat <count>`
- write raw benchmark output to `target/perf/<scenario>-<timestamp>/benchmark.json`
- derive a second summary that emphasizes smoothness, not only average throughput

## Why we measure "smoothness"

Average throughput is not enough for the write problem we are chasing. A run can have decent mean bandwidth and still feel broken if it repeatedly stalls.

That is why the summary adds:

- windowed throughput over fixed operation groups
- throughput coefficient of variation (`throughput_cv`)
- count of windows below 80% and 60% of the median
- a stall threshold derived from the phase latency distribution
- the worst individual operations by latency

For write work, the most important fields are:

- `benchmark.phase.aggregate.throughput_mib_per_sec`
- `benchmark.phase.aggregate.p95_ms`
- `benchmark.phase.smoothness.throughput_cv`
- `benchmark.phase.smoothness.windows_below_80pct_of_median`
- `benchmark.phase.smoothness.stall_count`

For read work, use the same fields from the read phase and also inspect:

- `signals.local_first_read`
- `signals.profile_artifacts`
- `signals.mounted_memory`

## Default scenarios

The defaults are chosen to make variance visible while staying easy to rerun:

- single explicit item size: `16MB`
- repeated workload count: `128`
- sequential access
- sequential streams enabled
- profiling enabled by default

This produces a stable operation log without mixing small/medium/large tiers, which makes before/after comparisons much easier when we are trying to remove jaggedness.

## Usage

Write-focused run:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\perf\write-jitter.ps1 -Target Z:\perf
```

Read-focused run:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\perf\read-speed.ps1 -Target Z:\perf
```

Preview the exact `studiofs test-cache` invocation without running it:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\perf\write-jitter.ps1 -Target Z:\perf -PreviewOnly
```

## Output layout

Each run creates a timestamped directory under `target/perf/` with:

- `command.txt`: exact benchmark command
- `stdout.txt`: human summary from `studiofs test-cache`
- `benchmark.json`: raw benchmark report
- `summary.json`: derived perf summary for this methodology
- `summary.md`: human-readable summary for review threads and notes

If profiling is enabled, the benchmark report may also reference:

- CPU `pprof.gz`
- memory `pprof.gz`
- write-path observability artifacts

These artifact paths are preserved under `signals.profile_artifacts`.

## How to compare runs

When comparing a candidate change against a baseline:

1. Keep the same machine, mount mode, target path, and script defaults unless the change requires otherwise.
2. Compare at least three runs before deciding that a regression or improvement is real.
3. Prefer medians across runs over a single best run.
4. Treat lower `throughput_cv`, fewer low-throughput windows, and fewer stalls as the primary success criteria for write smoothing.
5. Treat higher read throughput and lower P95/P99 latency as the primary success criteria for read improvements.

## Recommended notes in follow-up tests

In later investigations and PR notes, reference this document and include:

- script name
- target path
- commit SHA
- baseline summary path
- candidate summary path
- whether the change improved write smoothness, read throughput, or both

## Non-goals

This harness is not meant to replace:

- full product validation
- long-running AJA manual investigations
- platform-specific sanity suites

It is the fast repeatable loop we use while iterating on the current write-jitter and read-speed issues.

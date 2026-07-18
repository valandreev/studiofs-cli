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

Those harnesses use `studiofs devbench` with a fixed single-size mounted
workload. They are not the public mounted benchmark contract.

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

For SFS-598 Linux rollout evidence, use `linux-juicefs-parity-gate.sh` against
already-mounted local, new StudioFS, old StudioFS, and JuiceFS paths. It writes
the side-by-side gate artifacts and rollout `decision.md` under `target/perf`.

## Linux kernel-cache and CPU gate

The Linux cache gate separates collection, orchestration, and evaluation:

- `linux-cache-gate.sh` collects one mode/workload/repetition artifact from an
  already-mounted StudioFS instance.
- `linux-cache-matrix.sh` runs the mounted correctness preflight and the full
  balanced matrix, creating a fresh mount, cache directory, and cgroup for
  every tuple.
- `linux_cache_gate_summary.py` validates the complete evidence set, compares
  candidates, and writes `comparison.json`, `summary.md`, and `decision.md`.

The matrix always covers these three modes:

- `direct-serial`: Direct cache policy with async DIO disabled; rollback control
- `direct-async`: production Direct policy with async DIO enabled by default
- `kernel`: experimental Linux kernel cache policy; not promoted

See [`docs/perf/linux-cache-performance-result.md`](../../docs/perf/linux-cache-performance-result.md)
for the measured default decision, CPU result, and remaining 4 GB tail limit.

Each mode runs all six workloads: `large-single`, `fixed-1m`, `small-128k`,
`warm-reopen`, `same-file-parallel`, and `cross-file-barriers`. Diagnostic
evidence is exactly three repetitions and is never promotion-eligible. A
promotion run is exactly five repetitions; other real-run repetition counts
are rejected.

Real collection requires a writable delegated cgroup v2 parent with the CPU,
memory, and PIDs controllers available. A typical user-service delegation is:

```bash
CGROUP_ROOT="/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service"
```

The matrix creates leaf cgroups below that parent. It also isolates each tuple
with unique mount and cache paths so neither page-cache state nor daemon CPU
state is reused across measurements. CPU evidence is split into foreground
workload CPU and post-workload drain CPU; use total lifecycle CPU per GiB for
the gate, while keeping the split to identify deferred work.

Runtime snapshots come from out-of-band `diagnostics-attach` evidence tied to
the exact mount PID and mount path. Mounted-namespace stats are not an accepted
fallback. The collector records start, foreground end, drain end, and cleanup
end snapshots and requires three consecutive idle polls at drain boundaries.

The stable matrix layout is:

```text
<matrix-root>/
  correctness-manifest.json
  correctness/*.log
  schedule.json
  <mode>/<workload>/rep-NNN/
    environment.json  manifest.json  command.txt
    runtime-*.json    cgroup-*.json   proc-*/
    *-memory-samples.jsonl            pidstat.txt
    client-report.json                cleanup.json
    derived.json
```

Inspect the interfaces and preview a complete schedule without mounting:

```bash
bash tools/perf/linux-cache-gate.sh --help
bash tools/perf/linux-cache-matrix.sh --help
python3 tools/perf/linux_cache_gate_summary.py --help

bash tools/perf/linux-cache-matrix.sh \
  --volume "$VOLUME" \
  --mount-root target/perf/linux-cache-mounts \
  --cgroup-root "$CGROUP_ROOT" \
  --output-root target/perf/linux-cache-dry-run \
  --dry-run
```

To run only the fixed ten-test mounted correctness gate and write its
HEAD/binary-bound manifest without scheduling performance tuples, use:

```bash
bash tools/perf/linux-cache-matrix.sh \
  --correctness-only \
  --volume "$VOLUME" \
  --mount-root "$RUN/mounts" \
  --cgroup-root "$CGROUP_ROOT" \
  --output-root "$RUN"
```

Use a new output root for every real run. Run the three-repetition diagnostic
matrix first:

```bash
RUN="target/perf/linux-cache-diagnostic-$(date -u +%Y%m%dT%H%M%SZ)"
bash tools/perf/linux-cache-matrix.sh \
  --volume "$VOLUME" \
  --mount-root target/perf/linux-cache-mounts \
  --cgroup-root "$CGROUP_ROOT" \
  --output-root "$RUN" \
  --diagnostic
```

Collect promotion evidence only with the fixed five-repetition run:

```bash
RUN="target/perf/linux-cache-final-$(date -u +%Y%m%dT%H%M%SZ)"
bash tools/perf/linux-cache-matrix.sh \
  --volume "$VOLUME" \
  --mount-root target/perf/linux-cache-mounts \
  --cgroup-root "$CGROUP_ROOT" \
  --output-root "$RUN"
```

Analyze either matrix with its own correctness manifest:

```bash
python3 tools/perf/linux_cache_gate_summary.py \
  --direct-serial "$RUN/direct-serial" \
  --direct-async "$RUN/direct-async" \
  --kernel "$RUN/kernel" \
  --correctness-manifest "$RUN/correctness-manifest.json" \
  --output "$RUN/analysis"
```

Do not copy a benchmark revision or checksum into commands or notes. The real
matrix requires clean StudioFS and `studiofs-bench` checkouts, rebuilds the
benchmark with `cargo build --release --locked`, refreshes the bundled binary
and README, and records the current Git revisions plus binary SHA-256 values in
the manifests. It also verifies that the running mount executable matches the
recorded StudioFS binary. The benchmark checkout defaults to the sibling
`../studiofs-bench`; pass `--bench-repo PATH` when it lives elsewhere.

After each tuple, the matrix performs bounded unmount/process cleanup, removes
only its owned workload, cache, mount, and empty cgroup paths, and preserves
cleanup evidence. It stops at the first correctness, collection, or cleanup
failure. The analyzer still writes all three result files on failure, but exits
zero only for a promotion-eligible five-repetition matrix whose candidate gates
all pass; diagnostics and retained-default decisions exit nonzero.

## Scope

For this pass we care about two focused scenarios:

- `write-jitter.ps1`: write-focused regression run
- `read-speed.ps1`: read-focused regression run

Both scripts:

- run `studiofs devbench`
- force a reproducible single-size workload via `--big-file-size <item-size>` plus matching small-file sizing
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

Preview the exact `studiofs devbench` invocation without running it:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\perf\write-jitter.ps1 -Target Z:\perf -PreviewOnly
```

## Output layout

Each run creates a timestamped directory under `target/perf/` with:

- `command.txt`: exact benchmark command
- `stdout.txt`: human summary from `studiofs devbench`
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

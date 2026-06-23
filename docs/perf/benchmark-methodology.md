# StudioFS Benchmark And Stats Methodology

SFS-558 closes the public benchmark and runtime-stats workflow for operators
and developers. Use this document to decide which command to run, how to keep
runs comparable across platforms, and which fields must be preserved in a
benchmark report.

The benchmark methodology supplements the Local-First validation matrix. It is
not a correctness substitute for focused contract tests, ignored integration
tests, or platform bridge smoke tests.

## Command Choice

Use `studiofs perf` when you need live interval counters from an already
mounted StudioFS volume. It is observer only: it reads the mounted
`.studiofs-runtime-stats` file, computes rates and gauge values, and does not
create files or run workloads.

Use `studiofs bench` when you need the fixed public mounted filesystem
benchmark. Its workload is fixed, pseudo-random, and repeatable across machines:
the file mix, access order, and large-file offsets are part of the public
contract. Use `studiofs devbench` when you need to tune the developer workload
shape. The target must be inside an active StudioFS mount so the run includes
bridge callbacks, namespace lookup, VFS policy, cache behavior, object IO,
upload drain behavior, and runtime stats capture.

Treat the legacy test-cache path as an internal developer diagnostic. It may
still help isolate direct cache behavior during development, but it is not the
user-facing benchmark workflow and should not be used as parity evidence.

## Operator Workflow

1. Mount the StudioFS volume with the same cache disk, metadata disk, mount
   options, and runtime knobs that will be used for the measured run.
2. Record the platform and bridge: Windows WinFSP, macOS macFUSE, or Linux
   FUSE.
3. Run `studiofs perf <mountpoint> --schema` once when debugging reader or
   producer compatibility.
4. Run a short `studiofs perf <mountpoint> --json-lines --count 5` sample when
   you need live counters around a manual workload.
5. Run `studiofs bench <mountpoint> --report <path>` for fixed public mounted
   workload evidence when you need to archive JSON.
6. Collect at least three runs for each workload shape before making a parity
   claim.
7. Compare medians, p95/p99 latency, and smoothness indicators such as
   throughput dips, jitter, retry bursts, upload backlog spikes, and callback
   latency spikes.
8. Archive the JSON report, terminal output, command line, git revision, and
   any manual notes needed to explain cache warm-up or platform differences.

The public command saves JSON only when optional --report is provided as
`--report <path>`.
`studiofs devbench` writes a JSON report under `target/perf` by default and
prints the final report path. Passing `--json-output <file-or-dir>` to
`studiofs devbench` selects a specific file or output directory. Keep generated
artifacts under `target/` so they stay outside source control and are easy to
clean after the investigation.

## Report Interpretation

Every benchmark JSON report should be read from the workload metadata outward:

- `command`, `platform`, and `build` identify the command name, run id, OS,
  architecture, package version, profile mode, and git metadata when available.
- `mounted_runtime` identifies the mounted path, cache root, metadata root when
  it can be inferred, block size, workload shape, OS family, and bridge.
- `runtime_stats` contains before/after snapshots, whole-run deltas, and
  per-phase deltas from `.studiofs-runtime-stats`.
- `local_first_read_comparison` explains the `during-upload-read` and
  `post-upload-read` phases. A post-upload read is valid only after upload idle
  was observed.
- Public `first-read` phases are the first reads issued by the benchmark for
  that workload and mount state. They are not a guaranteed fully cold OS-cache
  measurement unless the operator separately controls host cache state.
- `mounted_memory`, `mounted_observability`, and profile sections are
  diagnostics for RSS, dirty bytes, staged bytes, upload backlog, barrier
  latency, CPU, disk IO, and allocator pressure when those signals are
  available.

Missing runtime stats are warnings, not benchmark failures. When the mounted
stats surface is unavailable or malformed, the benchmark still completes and
records `runtime_stats.available: false` with warning text in the JSON report.
Do not treat missing runtime stats as zero activity.

## Cross-Platform Comparison

Platform examples:

```powershell
studiofs bench Z:\ --report reports\studiofs-bench.json
```

```sh
studiofs bench /Volumes/studio/myvolume --report reports/studiofs-bench.json
```

```sh
studiofs bench /mnt/studio/myvolume --report reports/studiofs-bench.json
```

Use `studiofs devbench` for developer parity or tuning runs that require
explicit workload knobs:

```sh
studiofs devbench /mnt/studio/myvolume --block-size 4MB --big-file-size 1GB --threads 4
```

Compare developer runs only when the same workload shape is preserved:

- block size and read size
- big-file size, small-file size, and small-file count
- access pattern, concurrency, queue depth, and warm-up count
- cache disk, metadata disk, object store, PostgreSQL service, and network path
- cold pass versus warm pass setup
- mount options and runtime environment overrides

For JuiceFS parity checks, run JuiceFS with the same workload shape and the same
host constraints where possible. StudioFS-specific fields such as
`local_first_read_comparison`, runtime stats deltas, and mounted memory explain
differences from JuiceFS; they do not relax the comparison by themselves.

Platform bridge differences must be called out in the notes:

- Windows WinFSP can involve Windows cache-manager behavior, drive-letter mount
  setup, and WinFSP callback timing.
- macOS macFUSE is still an explicit backend path until its readiness gate says
  otherwise.
- Linux FUSE measurements should be taken on native Linux or WSL/Linux
  infrastructure rather than inferred from Windows builds.

## Known Limits

These known limits define how to read benchmark evidence without overstating
what one run proves.

Benchmarks are sensitive to host IO, antivirus, object-store latency,
PostgreSQL latency, CPU throttling, and cache warm-up. Record those conditions
instead of hiding them behind a single throughput number.

Small local changes can move performance without changing correctness. Always
pair benchmark evidence with the focused validation rows from
`docs/local-first-validation-matrix.md` when code changes touch the data path,
bridge callbacks, runtime stats producers, or upload behavior.

The runtime stats schema is forward-compatible. Readers should tolerate missing
newer sections, and reports should preserve warnings when a section was absent
or empty.

## Focused Validation

For this document contract, run:

```bash
cargo test -p studiofs --test architecture_vocabulary_test benchmark_methodology_covers_sfs558_operator_contract -- --exact
```

For the surrounding implementation surface, prefer focused checks for the
changed area, for example:

```bash
cargo test -p studiofs perf -- --nocapture
cargo test -p studiofs devbench_command_parses_juicefs_style_workload_flags -- --nocapture
cargo test -p studiofs-vfs runtime_stats_ --lib
```

Ask an operator to run full workspace, ignored integration, and mounted
cross-platform sweeps when the change is broader than this documentation
surface.

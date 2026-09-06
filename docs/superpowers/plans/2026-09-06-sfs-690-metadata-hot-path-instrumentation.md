# Instrument Windows metadata hot paths and interleave the A/B benchmark

This ExecPlan is a living document and is maintained according to `.agent/PLANS.md`.

## Purpose / Big Picture

SFS-680 established that Windows metadata timings can change with host load and can even reverse the apparent winner when all baseline runs precede all candidate runs. This change makes the result attributable and repeatable before any production cache-policy decision is attempted. After the change, the release WinFSP benchmark will report which measured workload operations reached path resolution, VFS metadata lookup/getattr, ACL enforcement, directory construction, batched attributes, and native-cache admission. It will also record cache hit/miss deltas, binary identity, interleaved run order, warm-up status, host-load observations, medians, dispersion, and outliers. The production cache policy remains unchanged.

The observable proof is the registered ignored test `metadata_baseline::winfsp_metadata_baseline`, invoked by `tools/perf/sfs-680-windows-metadata-ab.ps1`. A valid run must produce exactly four workloads, three measured passes per side, six interleaved benchmark executions, nonzero correctness assertions, matching executable identity, and a fresh report; a skipped test, stale report, wrong binary, failed process, or failed mount cleanup must fail the harness.

## Progress

- [x] (2026-09-06) Read the SFS-690 Linear contract, related SFS-680 artifacts, repository contracts, WinFSP/VFS scoped instructions, and current code paths.
- [x] (2026-09-06) Confirmed the repository, branch, clean starting status, and codebase-memory index coverage.
- [x] (2026-09-06) Chosen the smallest measurement design: opt-in constant-name tracing at real hot-path boundaries plus deltas from existing VFS cache/runtime snapshots; no cache-policy change.
- [x] (2026-09-06) Added and ran the focused red event-classification test before implementing the new event counter; the red run failed with `None` versus `Some(0)`.
- [x] (2026-09-06) Added VFS and WinFSP instrumentation and expanded the metadata CSV schema with attribution fields.
- [x] (2026-09-06) Changed the SFS-680 harness to build both sides first, execute deterministic alternating pairs, and validate binary/report/artifact identity.
- [x] (2026-09-06) Added focused benchmark documentation; native result evidence remains pending.
- [x] (2026-09-06) Ran focused compile/tests, formatting, PowerShell parsing, and clippy checks; final diff checks remain after the documentation/reporting polish.
- [x] (2026-09-06) Ran the release native interleaved A/B benchmark successfully in the configured WinFSP environment; the retained final artifact is `target/perf/sfs-680-windows-metadata-ab-20260906-192059`.
- [x] (2026-09-06) Re-ran final focused checks after commit `40a5a64` and recorded the final validation boundary.

## Surprises & Discoveries

- The existing SFS-680 harness runs every baseline sample before every candidate sample. Its CSV validation checks row count and operation counts, but it does not identify the executable that was run, capture warm-up status, or reject a fresh process result whose report is missing.
- The existing WinFSP test already counts six callback-entry events, while the VFS already exposes cache snapshots and native-cache open counters. Callback entries are not equivalent to VFS lookup calls, so the new counters must be emitted at the VFS call boundaries rather than inferred from callbacks.
- `readdir_entries_and_attrs` performs the real `readdir`, batched attribute, ACL, and dentry-cache work in one path. The instrumentation will count those boundaries directly and will document that the values are operation-entry observations, not file contents or timings for individual internal calls.
- The native benchmark is an ignored, environment-gated mount test. A focused unit/compile result cannot replace the release WinFSP run; unavailable WinFSP, drive, or permissions will remain an explicit acceptance gap.
- The first compile attempt showed that `trace` is not re-exported through the VFS module's `super` imports. Importing it directly from `tracing` in each touched module fixed the issue without changing the event design.
- A literal lookup patch initially matched the neighboring dangling-entry repair helper instead of `lookup_component_impl`; diff review caught this before acceptance, and the event was moved to the actual cached-lookup miss path.
- Windows PowerShell 5.1 returned an empty `Process.ExitCode` for `Start-Process` with redirected output, even after `WaitForExit` and `Refresh`; a minimal repro confirmed it. The harness now uses `.NET Process` with asynchronous output draining, preserving resource polling and reliable exit codes.
- Cargo's JSON build output includes messages without a `reason` property. Strict property access initially rejected those lines; the parser now checks message properties before selecting the `mount_sanity` test executable.
- The historical baseline commit emitted the pre-SFS-690 callback-only CSV and the direct test binary initially remained ignored because Cargo's `--` separator was passed to libtest. The harness now invokes the built executable with the direct libtest argument form, marks the known baseline as `legacy`, and requires SFS-690 schema from the candidate instead of fabricating missing attribution as zero.
- The first event-classifier implementation dropped new WinFSP-prefixed hot-path names after failing callback lookup. The focused test was expanded to cover `path_resolution` under that prefix, and the classifier now falls through correctly.

## Decision Log

- Decision: Use constant-name `trace!` events under the existing opt-in benchmark targets and read existing VFS/runtime snapshots around each measured workload.
  Rationale: This gives attribution in the real production path without adding persistent logging, secrets, file contents, or a new production cache policy. The benchmark installs a subscriber only for its own run.
  Date/Author: 2026-09-06 / Codex.
- Decision: Keep the historical four workloads, 1000 operations, and three measured passes, with zero warm-up passes explicitly recorded.
  Rationale: Preserving the SFS-680 workload contract makes before/after comparison possible; recording `warmup_passes=0` prevents the absence of warm-up from being mistaken for an undocumented state.
  Date/Author: 2026-09-06 / Codex.
- Decision: Use a deterministic alternating pair order, baseline/candidate on odd run numbers and candidate/baseline on even run numbers.
  Rationale: It interleaves sides while avoiding an uncontrolled random seed that would make artifact reproduction harder. The exact order is written to the environment and results artifacts.
  Date/Author: 2026-09-06 / Codex.
- Decision: Fail conservatively when the expected test executable cannot be located, changes identity between build and run, or the fresh metadata report is absent or malformed.
  Rationale: SFS-690 explicitly forbids silently accepting stale artifacts, wrong binaries, or skipped tests; a false rejection is preferable to an unattributable performance result.
  Date/Author: 2026-09-06 / Codex.

## Outcomes & Retrospective

The implementation adds seven opt-in hot-path entry counters, existing cache/native-cache snapshot deltas, explicit warm-up and instrumentation coverage markers, binary identity checks, process/system observation fields, and interleaved execution. The final native run at `target/perf/sfs-680-windows-metadata-ab-20260906-192059` produced six successful runs and eight workload summaries. The candidate emitted SFS-690 attribution; the historical baseline emitted the explicitly marked legacy schema, so baseline attribution medians remain null rather than being misrepresented as zero. System CIM load fields were unavailable on this host and are recorded as explicit nulls. Commit `40a5a64` contains all intended task files, and the focused verification boundary passed.

## Context and Orientation

The repository is a Rust 2024 workspace at `E:\dev\studiofs-cli`. `crates/studiofs-winfsp` contains the Windows adapter and the ignored native test in `crates/studiofs-winfsp/tests/mount_sanity/metadata_baseline.rs`. The adapter resolves paths, opens handles, fills directory buffers, and calls into `crates/studiofs-vfs`. The VFS uses local `redb` metadata for foreground operations; PostgreSQL and object storage are not foreground metadata read dependencies.

The benchmark script `tools/perf/sfs-680-windows-metadata-ab.ps1` checks out a baseline source snapshot and runs the current candidate source against the same release ignored test. It stores build logs, environment data, copied CSV reports, run records, and a summary below a unique `target/perf` directory. A workload is one of `stat`, `open_close`, `negative_lookup`, or `enumeration`, and each workload performs 1000 correct filesystem operations in each of three measured passes.

The new event names are deliberately low-cardinality and contain no path, credential, DSN, or file-content data. They represent entry counts: path resolution, VFS namespace lookup, VFS getattr, ACL enforcement, readdir, batched attributes, and directory-buffer construction. Cache fields come from the existing `VfsCacheStateSnapshot`, and native-cache admission comes from the existing write-path observability snapshot. These measurements are scoped to the interval around one workload and are reported as deltas.

## Plan of Work

First, add a focused test in the metadata-baseline helper that proves the subscriber classifies both existing WinFSP callback targets and the new VFS hot-path targets, including ignoring unrelated events. Run that test and capture its expected failure before adding the classifier implementation, satisfying the test-first boundary.

Next, add constant-name trace events at the actual boundaries: WinFSP path resolution and directory-buffer construction; VFS namespace lookup, getattr, ACL enforcement, readdir, and each batched-attribute call. Extend the benchmark subscriber to count both the six callback names and the VFS names. Around every measured workload, capture the counter delta, the existing VFS cache snapshot delta, and the native-cache enabled/disabled admission delta. Add an explicit warm-up column with `false` and retain the existing correctness assertions.

Then, update the PowerShell harness. Build both source sides before timed execution and extract the `mount_sanity` test executable from Cargo's JSON build messages. Record its absolute path, size, and SHA-256 identity; verify that identity immediately before each run and reject a changed binary. Execute a deterministic interleaved order and store that order in `command.txt`, `environment.json`, `results.json`, and the summary. Sample process and system load before and after each run. Validate the expanded CSV header, freshness, exact workload/row/operation contract, and nonzero process/report success. Extend aggregation with minimum, median, p95, maximum, standard deviation or coefficient of variation, and IQR outlier counts, while retaining per-workload attribution medians.

Finally, add a focused SFS-690 performance-contract document describing the new fields and the native command. Run only the changed-crate focused tests/checks/clippy, PowerShell parsing, formatting, and diff checks. Run the native release interleaved harness when its configured WinFSP environment is available. Inspect the generated artifacts, update this plan with evidence, and commit the intended files without changing unrelated worktree content.

## Concrete Steps

Run all commands from `E:\dev\studiofs-cli` in PowerShell. The red test must be run before the implementation that makes it pass:

    cargo test -p studiofs-winfsp --test mount_sanity metadata_baseline::tests::callback_counter_classifies_vfs_hot_path_events -- --exact --nocapture

Expected red evidence is a compile/test failure stating that the new classifier or event contract is not yet implemented. After implementation, the same command must report one passed test; a result that runs zero tests is invalid evidence.

Validate the script without running its environment-gated side effects:

    $tokens = $null; $errors = $null; [System.Management.Automation.Language.Parser]::ParseFile('tools/perf/sfs-680-windows-metadata-ab.ps1', [ref]$tokens, [ref]$errors) | Out-Null; if ($errors.Count -ne 0) { $errors | Format-List; exit 1 }; 'PowerShell parse ok'

When the `.env` gate is enabled and its configured drive is free, run the native acceptance command with three passes per side:

    powershell -ExecutionPolicy Bypass -File .\tools\perf\sfs-680-windows-metadata-ab.ps1 -MountPoint Z: -Runs 3

The actual mount point must match the configured value; do not print `.env` contents or secrets. If the native environment is not available, retain the script's explicit failure/output and report the exact missing prerequisite rather than substituting a non-native test.

## Validation and Acceptance

The focused Rust test proves event classification. `cargo fmt -- --check`, `cargo check -p studiofs-vfs`, `cargo test -p studiofs-vfs <focused-name>`, `cargo test -p studiofs-winfsp --lib`, the focused mount-sanity test, and `cargo clippy -p studiofs-winfsp --all-targets -- -D warnings` provide compile and behavior evidence without running a workspace-wide suite. `git diff --check` must pass.

The native harness is accepted only when it records six executed runs in the exact interleaved order, each report has 12 rows with all four workloads and 1000 operations, every report has the expanded attribution/cache schema, the expected binary identity matches, the test process exits successfully, the report is newer than the run start, and mount cleanup does not produce a failure. The summary must show per-workload medians, dispersion, outlier counts, and attribution values. System-load fields may be explicitly unavailable only when the host query cannot provide them; that limitation must be visible in the artifact.

The benchmark result must not be interpreted as a production cache-policy decision. SFS-690 is complete only after the measurement change is validated and the native run's quality/performance evidence is recorded. Full workspace suites remain the user's separate responsibility; the final response will provide their command if needed.

## Idempotence and Recovery

Each harness invocation uses a unique output directory and a fresh source snapshot, so rerunning it does not overwrite prior evidence. The script restores the inherited environment variables and removes only its own temporary source snapshot under its unique output directory. If a mount or report fails, preserve that artifact directory for diagnosis, fix the reported cause, and rerun with a new output directory. Do not reset or discard unrelated Git changes.

## Artifacts and Notes

The implementation will produce, when the native environment is available, a unique directory such as `target/perf/sfs-680-windows-metadata-ab-<timestamp>/` containing `environment.json`, `command.txt`, build logs, per-run records, copied CSV reports, `results.json`, and `summary.md`. The raw CSV is the authoritative per-workload observation; the Markdown summary is a human-readable aggregation.

## Interfaces and Dependencies

The benchmark subscriber remains a test-only `tracing::Subscriber` in `crates/studiofs-winfsp/tests/mount_sanity/metadata_baseline.rs`. It will expose a stable internal event-name mapping for the focused test and retain the six existing callback columns. The CSV will add stable columns for the VFS event deltas, cache hit/miss deltas, native-cache admission deltas, and warm-up status.

The production code uses the repository's existing `tracing` dependency and existing snapshot methods. No new crate, cache abstraction, remote service, configuration key, or production cache-policy branch is required. The PowerShell harness continues using Cargo, `Get-FileHash`, CIM load observations, and unique `target/perf` artifacts.

Plan revision: initial plan created on 2026-09-06 before the red test; implementation and validation sections will be revised at each stopping point.

Plan revision: 2026-09-06 after instrumentation and harness implementation. The red test and focused green test have run, the PowerShell script parses, and native release evidence is still required.

Plan revision: 2026-09-06 after the native gate. The harness records direct executable identity and explicit legacy baseline coverage; the final native artifact passed six interleaved runs with candidate commit `40a5a64`.

Plan revision: 2026-09-06 after final verification. The focused classifier and VFS regression tests passed, both changed crates passed `check`/`clippy -D warnings`, formatting, PowerShell parsing, and diff checks passed, and commit `40a5a64` is the recorded implementation boundary.

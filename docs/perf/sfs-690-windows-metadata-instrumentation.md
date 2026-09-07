# SFS-690 Windows metadata instrumentation

SFS-690 makes the release WinFSP metadata A/B result attributable before a production cache-policy decision is made. It adds opt-in, low-cardinality tracing at the real WinFSP/VFS boundaries and extends the existing SFS-680 harness. The instrumentation records counters only; it does not record credentials, DSNs, paths, file contents, or cache-policy changes.

## Measurement contract

The registered ignored test is `metadata_baseline::winfsp_metadata_baseline` in `crates/studiofs-winfsp/tests/mount_sanity/metadata_baseline.rs`. It retains the four SFS-680 workloads: `stat`, `open_close`, `negative_lookup`, and `enumeration`. Each invocation performs 1000 operations for each workload in three measured passes, with no warm-up pass. Correctness assertions remain part of the measured workload.

The raw CSV preserves one row per pass and workload. `warmup=false` is explicit because these are all measured observations. Existing WinFSP callback columns remain callback-entry observations. The added event columns count entries at the following real boundaries:

- `path_resolution` and `directory_buffer` in the WinFSP context;
- `metadata_lookup`, `metadata_getattr`, `acl_check`, `readdir`, and `get_attrs_batch` in the VFS path;
- `dentry_cache_*`, `path_shortcut_cache_*`, and `attr_read_cache_*` from the existing VFS cache-accounting snapshots;
- `native_cache_enabled_open` and `native_cache_disabled_open` from the existing native-cache admission counters.

The cache and native-cache values are snapshot deltas around one workload. Close callbacks and other asynchronous activity can cross workload boundaries; the raw event and snapshot values must therefore be interpreted as bounded observations, not as a one-to-one mapping from every Windows operation to every callback.

The CSV metadata column `instrumentation_mode` is `enabled` for an attribution run and `disabled` for a timed candidate run. The timed candidate subscriber remains callback-compatible with the historical baseline, but rejects the new hot-path event targets so its atomic event counters do not add overhead to the A/B timing. The new hot-path event columns are therefore zero in the timed candidate rows by design; they are populated only by the separate attribution run.

## A/B harness contract

Run `tools/perf/sfs-680-windows-metadata-ab.ps1` with a free configured WinFSP drive. Both sides are release-built before timed execution. Cargo's JSON build messages identify the `mount_sanity` executable, and the harness records its absolute path, byte length, and SHA-256. The same identity must be present immediately before and after each run.

Runs are deterministic and interleaved. For each logical run number, the odd-numbered pair is baseline then candidate and the even-numbered pair is candidate then baseline. The exact sequence is written to `command.txt`, `environment.json`, `results.json`, and `summary.md`. Build time is excluded from workload timing. After both release builds, the harness invokes the identified `mount_sanity` executable directly so the recorded binary hash is the executable used for the native run.

The timed A/B processes receive `STUDIOFS_METADATA_INSTRUMENTATION=disabled` on both sides. This keeps the historical legacy baseline and the candidate's callback-compatible subscriber out of the new hot-path counter overhead. After all timed runs, the candidate is invoked once more with `STUDIOFS_METADATA_INSTRUMENTATION=enabled` for untimed attribution; its elapsed values and process resources are retained under `attribution_runs` and `attribution_workloads`, but are excluded from the A/B timing table. Each side is built with its own temporary `CARGO_TARGET_DIR`, so the baseline executable cannot be replaced by the candidate build before execution.

Every run must exit successfully, emit a fresh `metadata_baseline_report`, and produce exactly 12 rows covering all four workloads and all three passes with 1000 operations. The current candidate must emit the expanded SFS-690 schema. The historical SFS-680 baseline commit predates this instrumentation and may emit the legacy callback-only schema; the harness marks that side `instrumentation_status=legacy` and leaves attribution medians null rather than fabricating zeros. Missing reports, stale reports, wrong or changing binaries, malformed measurement columns, skipped tests, and failed processes are harness failures. Mount cleanup failures remain failures through the native test process result.

The timed per-workload summary reports minimum, arithmetic mean, median, p95, maximum, sample standard deviation, coefficient of variation (sample standard deviation divided by the arithmetic mean), IQR fences, and outlier count. Candidate attribution medians are reported separately in `attribution_workloads`. Each run records the benchmark process's exact total CPU time and the kernel-recorded peak working set when the exited-process handle exposes it; otherwise it retains the sampled maximum with `max_working_set_source=sampled`. Processor-load/free-memory observations are recorded when CIM exposes them; unavailable host fields are represented explicitly as null. The retained `baseline-source.zip` in each artifact preserves the exact baseline source snapshot used for the comparison.

## Native command

From `E:\dev\studiofs-cli`, after `.env` has opted into the mount test and configured the same free drive:

    powershell -ExecutionPolicy Bypass -File .\tools\perf\sfs-680-windows-metadata-ab.ps1 -MountPoint Z: -Runs 3

The drive letter is an example and must match `STUDIOFS_WINFSP_TEST_MOUNT_POINT`. Generated evidence is retained under a unique `target/perf/sfs-680-windows-metadata-ab-<timestamp>/` directory. A focused Rust test or compile check does not substitute for this native gate.

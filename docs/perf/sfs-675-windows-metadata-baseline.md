# SFS-675 Windows metadata baseline

## Scope and source identity

The baseline starts from commit `83822d8ee9954bd072a057aa2ca3b686e14095b9`.
Only Windows callback-entry trace instrumentation and a measurement test are
added; cache policy remains unchanged. The accompanying
[shared contract](../metadata-cache-coherence.md) defines future semantics.
These measurements do not validate multi-client replication or promotion.

## Historical pre-SFS-678 settings and behavior

This section records the settings before SFS-678 changed the Windows adaptive
policy. Keep these values unchanged as the comparison baseline; current
candidate defaults are documented in `docs/metadata-cache-coherence.md`.

Source: `crates/studiofs-winfsp/src/lib.rs`, `parse_mount_options` and
`apply_volume_params`; `crates/studiofs-winfsp/src/context.rs`, callback methods;
`crates/studiofs-winfsp/src/context/invalidation.rs`, invalidator and notifier.

| Setting | Current default | Override |
| --- | --- | --- |
| FileInfoTimeout | `4294967295` (`u32::MAX`) | `STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS` |
| DirInfoTimeout | 1000 ms | `STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS` |
| VolumeInfoTimeout | 1000 ms | `STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS` |
| AllowOpenInKernelMode | false | `STUDIOFS_WINFSP_ALLOW_OPEN_IN_KERNEL_MODE` |
| FlushAndPurgeOnCleanup | false | `STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP` |

WinFSP documents the all-ones FileInfoTimeout setting as enabling cache-manager
fast IO, not as an ordinary measured 49-day TTL. Directory/volume timeouts have
separate overrides. Sources checked through Context7 on 2026-09-05:
[volume parameter definitions](https://github.com/winfsp/winfsp/blob/master/inc/winfsp/fsctl.h),
[WinFSP changelog](https://github.com/winfsp/winfsp/blob/master/Changelog.md).

Successful `open` resolves the path, selects cache behavior and creates a handle.
The default deliberately disables kernel-mode opens because a same-inode peer
overwrite could otherwise reuse stale cache-manager identity. `get_file_info`
prefers local dirty attributes and then VFS attributes. `close` delegates handle
release; cleanup does not by default purge the read cache. An application stat
is not equivalent to one `get_file_info` callback: Windows may use security/name
resolution or cached results instead.

Negative lookup uses the shared namespace resolver. Windows callback counts for
failed probes must be measured at callback entry, before fallible path resolution;
the old successful-open debug event cannot measure misses. Enumeration with no
marker rebuilds the per-handle directory buffer; continuation reads that buffer.
The existing `read_directory` debug event counts buffer population, not every
callback or returned entry. Fully consume `read_dir` when measuring enumeration.

The current WinFSP invalidator overrides full inode invalidation only. Its
8192-inode queue coalesces work and translates it to native path notifications.
An unpublished prepared batch is requeued when dropped; this accommodates
WinFSP NotifyBegin contention with rename. Unresolvable inode paths and paths
that cannot fit the native notification are skipped. Consequently this baseline
must not be interpreted as proof of complete name/negative-entry coverage or
the future health contract. Native notification success is also distinct from
an application's receipt of that notification. See
[WinFSP notification API](https://github.com/winfsp/winfsp/blob/master/doc/WinFsp-API-winfsp.h.md).

## Measurement method

Use an isolated WinFSP mount backed by the existing native `mount_sanity` test
fixture: local redb metadata, default VFS configuration and fresh cache directory
under `target`. Empty files avoid payload transfer in the timed workload. This
local fixture deliberately excludes PostgreSQL replication and real multi-client
traffic. Record host/build/settings alongside raw measurements.

Declare separate workloads for repeated stat of an existing empty file,
open/close of that file, stat of a fixed absent name, and fully consumed directory
enumeration of 64 empty files. Run 1000 operations per workload per pass and
three passes. Setup is outside timing. Repeated passes are warm/repeated
observations; do not call them OS-cold measurements. Report elapsed wall time,
operation count, and callback-entry deltas for security-by-name, open, file-info,
directory, cleanup and close. A zero callback delta is an observation, not a
missing-counter placeholder. Validate file attributes, NotFound results and
directory contents on every operation.

Trace instrumentation has overhead. Compare future candidates using the same
instrumentation, build profile, fixture and host settings. Callback counts are
not VFS lookup counts, and timings are caller workload latency rather than CPU
time or per-callback latency. Async close completion may cross workload
boundaries; preserve that limitation when interpreting per-phase counts.

## Recorded Windows result: 2026-09-05

The exact native test passed: **1 passed, 24 filtered out**, 2.07 seconds
including fixture lifecycle. Test name:
`metadata_baseline::winfsp_metadata_baseline`.

Host: Intel Core Ultra 9 275HX; Windows 11 25H2, build 26200.9168;
WinFSP DLL `2.2.26194.e3ecc89` (2026 Beta3); Rust
`1.98.0 (88d9e12ae 2026-08-18)`; Cargo release profile.
Locked Rust bindings: `winfsp 0.12.4+winfsp-2.1`,
`winfsp-sys 0.12.1+winfsp-2.1`. No timeout/cache-option environment overrides
were present in the run shell or fixture `.env`; the defaults above apply.
Antivirus/indexing were not controlled, so these are host observations rather
than an isolated CPU or filesystem latency guarantee.

Measured executable for the original recorded run:
`target/release/deps/mount_sanity-74aabd16b63a04ac.exe`, 19,914,752 bytes;
SHA-256 `BB0E23BA7D18FAB3D97D92CC23D85E4CC3B11CDAB7A46AFB4FDCD86B247F938D`.
This is the test fixture executable, not a production CLI mount. The source
identity is the starting commit plus this issue's callback instrumentation and
test. Cache and metadata use the fixture's fresh workspace-target temporary
directory, with 64 MiB cache and 32 MiB buffer; storage hardware/cache temperature
were not independently controlled. A subsequent lint-only nested-if to let-chain
change in the counting subscriber postdates this executable and does not change
the measured workload or event handling.

From the repository root, with a verified free `Z:` drive:

```powershell
$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'
$env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'
cargo test -p studiofs-winfsp --release --test mount_sanity metadata_baseline::winfsp_metadata_baseline -- --ignored --exact --nocapture --test-threads=1
```

The test refuses an assigned drive and requires explicit opt-in. Run it alone:
its global tracing subscriber observes dispatcher threads across the process.
Before the opt-in check it reads the repository `.env` through the existing
mount-sanity fixture helper, so quoted `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS`
and `STUDIOFS_WINFSP_TEST_MOUNT_POINT` values are honored without mutating the
process environment. An explicitly set process environment value still takes
precedence. If neither source enables the test, it reports a skip instead of
failing the surrounding ignored-test run or enabling other tests in the process.
The latest supplied raw output is retained at
`target/perf/sfs-675-metadata-1788617449200-47924.csv`; the complete values below
preserve that result when ignored build artifacts are removed.

| Workload (1000 operations per pass) | Pass 1 ms | Pass 2 ms | Pass 3 ms | Median ms |
| --- | ---: | ---: | ---: | ---: |
| stat existing empty file | 102.7995 | 88.5705 | 97.3633 | 97.3633 |
| open/close existing empty file | 102.5098 | 92.8760 | 129.4063 | 102.5098 |
| negative lookup, fixed absent name | 35.9254 | 36.8291 | 46.5934 | 36.8291 |
| enumerate all 64 names | 1788.1619 | 2096.1232 | 2067.2501 | 2067.2501 |

Timings include correctness assertions and enumeration sorting; they are not
pure callback latency. Counts per pass were identical except asynchronous close:

| Workload | Security by name | Open | File info | Read directory | Cleanup | Close, passes 1/2/3 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| stat | 1000 | 1000 | 0 | 0 | 0 | 999 / 1000 / 999 |
| open/close | 1000 | 1000 | 0 | 0 | 0 | 999 / 999 / 1000 |
| negative lookup | 1000 | 0 | 0 | 0 | 0 | 0 / 0 / 0 |
| enumeration | 1000 | 1000 | 0 | 1000 | 0 | 999 / 999 / 999 |

Repeated operations did not eliminate successful open or directory callbacks in
this workload. Misses stopped in security-by-name resolution. Zero file-info
callbacks do not prove stat was served wholly from kernel cache: open still ran.
The closes observed in negative-lookup windows belong to asynchronous activity,
not successful opens of the missing name. These results neither establish
negative-name cache TTL nor cover held-open enumeration across a remote mutation.

An earlier run before stronger file-length and exact-name assertions also
passed; it is retained separately at
`target/perf/sfs-675-metadata-1788613641245-50004.csv` and is not combined with
the latest supplied timing sample because the timed validation work differs.

An intermediate run after the `.env` lookup guard fix completed with 1 passed and
24 filtered tests. Its raw CSV is retained at
`target/perf/sfs-675-metadata-1788615768816-54128.csv`. The callback counts kept
the same per-workload shape, with one enumeration window observing four extra
open/security/close callbacks from asynchronous activity. Host timing varied
(enumeration 1878.2270–3042.3467 ms), so this rerun is evidence that the guard
and mount path work, not a replacement for the recorded baseline medians above.

The fixture-isolation fix was verified with the full ignored WinFSP
mount-sanity selection (the same 14-test shape as the reported failure), without
exporting either setting in the shell:

```text
test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out
```

The baseline again produced the expected callback shape and retained
`target/perf/sfs-675-metadata-1788616100750-17372.csv`. The baseline now reads
`.env` only for its own configuration and constructs its fixture without the
shared helper that mutates process environment; sibling tests therefore remain
skipped unless the operator explicitly exports the platform gate. This run's
host timings are also observational and are not merged into the latest supplied
medians above.

The latest table and callback counts are the operator-supplied run received on
2026-09-05. Its CSV path and rows were verified in the workspace. The message
did not include a new executable hash or host transcript, so the original
environment identity above remains explicitly tied to the earlier recorded
binary; reattach matching identity metadata before using this sample for an
unrelated candidate comparison.

## Full acceptance commands (operator-run)

Repository policy reserves full suites for the user. From the repository root,
with the required test services and platform opt-ins configured:

```powershell
cargo test --workspace --all-targets -- --nocapture
if ($LASTEXITCODE -ne 0) { throw 'Non-ignored suite failed' }
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

Both commands are necessary for complete coverage: `--ignored` alone excludes
non-ignored integration tests.

Later Windows promotion additionally requires the exact 10-run mounted gate:

```powershell
cargo build -p studiofs
if ($LASTEXITCODE -ne 0) { throw 'StudioFS build failed' }
1..10 | ForEach-Object {
    cargo run -p studiofs-e2e -- run --scenario 1-25 --studiofs-bin target\debug\studiofs.exe
    if ($LASTEXITCODE -ne 0) { throw "Promotion gate failed on run $_" }
}
```

No retry or manual reset is allowed within a successful streak. Retain all
runner artifacts, confirm every run actually executed all 25 scenarios, and
record binary/configuration identity. The full integration suite and this
promotion gate have not been run as part of baseline collection.

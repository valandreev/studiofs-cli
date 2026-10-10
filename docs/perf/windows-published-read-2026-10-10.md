# Windows published-file read investigation, 2026-10-10

## Result

The reported sustained AJA drop after upload did not reproduce in the live
rerun. The user observed occasional 5 GB/s reads with disk caching enabled and
an 8 GiB workload. The mount used the freshly built workspace release binary,
volume `test`, `Z:`, PostgreSQL metadata, and the configured `D:\cache` backend
with a 200 GB limit. The old executable found in PATH was not used.

This is a successful current rerun, not proof that the earlier intermittent
failure is fixed. The only retained production change adds coherence fields
to the existing optional WinFSP close diagnostic.

## Live AJA evidence

The mount log records upload pending bytes falling from 8.0 GB to zero at
14:48:21 UTC. Pending metadata operations reach zero at 14:48:31. The AJA data
inode is `549755834369`; its read-only handles closed after those boundaries
with `open_flags=1`, `disable_os_cache=false`, `read_callbacks=0`, and
`read_callback_bytes=0`. Coherence was `Healthy`. These callback counts are
per-handle close snapshots, not aggregate mount counters; paging I/O can be
attributed to a different file context.

After unmount/remount, a standalone buffered Windows reader read the same
`ajadisktest.dat` without modifying it. The file length was 8,588,124,160 bytes;
each pass read the full length into a reusable 4 MiB buffer:

| Pass | MiB/s | Duration |
| --- | ---: | ---: |
| First after remount | 1886.67 | 4.3411 s |
| Warm second | 4832.69 | 1.6948 s |
| Warm third | 4574.74 | 1.7903 s |

The remount was healthy before opening the reader. The first pass is cold
relative to this mount; no system-wide Windows standby-list eviction was
performed. This is a memory-sink read, not a Total Commander copy to another
drive, so it does not measure destination writes or Total Commander open flags.

## Direct VFS test A/B

`foreground_read_baseline_during_and_post_upload_are_comparable` calls
`vfs.read` directly. It does not pass through the WinFSP file cache. Commit
`7385586f` changed the default backing payload reads on Windows to
`LocalPayloadReadMode::Unbuffered`, avoiding duplicate cache pages beneath the
mounted file's kernel cache. This explains a concrete difference from the
earlier buffered measurements.

Only the payload-store constructor's read-mode argument was temporarily
changed for the controlled comparison. All other source was identical. The
override was restored afterward.

| Debug run | Baseline MiB/s | During upload MiB/s | Post upload MiB/s |
| --- | ---: | ---: | ---: |
| Default Unbuffered | 741.50 | 1059.24 | 753.12 |
| Temporary Buffered | 1962.89 | 2032.30 | 1902.66 |
| Restored Unbuffered control | 491.07 | 1387.37 | 616.60 |

Each invocation ran exactly one test and passed. The Buffered result restores
the earlier 1.4–2.0 GiB/s range; the restored control confirms the read-mode
effect despite timing variation. These test caches live under the workspace
target directory on `E:`, unlike the user's configured mounted cache on `D:`.
Do not compare their absolute disk throughput directly.

The current test's post-upload assertion accepts either 4 MiB/s or 80% of
baseline. Therefore `ok` does not establish recovery of the historical absolute
throughput.

The second named test, `test_sequential_write_performance`, also ran exactly
once per comparison:

| Profile/mode | Write MiB/s | Flush ms | Read MiB/s |
| --- | ---: | ---: | ---: |
| Debug/default Unbuffered | 71.06 | 6.55 | 986.49 |
| Debug/temporary Buffered | 74.51 | 8.02 | 4251.72 |
| Release/default Unbuffered | 257.83 | 6.60 | 2381.35 |

Its write timer includes generating all 100 MiB of payload data inside the
write loop. The debug write number measures that CPU work as well as VFS writes;
it is not an isolated device-write bandwidth measurement. No benchmark gates
or timers were changed to improve the displayed results.

## Relevant contracts

Read-only WinFSP cache admission depends on adaptive metadata policy and
coherence health. The decision is fixed when a handle opens. A write-capable
handle disables the WinFSP file cache to preserve the local durability barrier;
recovery of coherence does not change that existing handle's decision.

The Windows payload `advise_will_need` function returns without warming payload
bytes. This does not establish that all VFS read-window prefetch is ineffective:
the handle read-window path also assembles actual data. No broad readahead,
cache-admission, or payload-default change is justified by the successful AJA
rerun alone.

Source entry points are `crates/studiofs-cache/src/runtime.rs:216`,
`crates/studiofs-cache/src/payload_store.rs:139,327`,
`crates/studiofs-winfsp/src/context.rs:390,473,1301,2043`,
`crates/studiofs-vfs/src/vfs/io_ops.rs:109`, and
`crates/studiofs-vfs/tests/vfs_test.rs:1542,8787,12045`. Graph discovery was
checked against current source because coverage reported changed metadata.

## Reproduction and artifacts

From the repository root, run only the named test when comparing modes:

    cargo test -p studiofs-vfs --test vfs_test foreground_read_baseline_during_and_post_upload_are_comparable -- --ignored --exact --test-threads=1 --nocapture
    cargo test -p studiofs-vfs --test vfs_test test_sequential_write_performance -- --ignored --exact --test-threads=1 --nocapture

Add `--release` before `-p` for release-profile measurements. A mount binary
built with `cargo build --release` does not change the profile of `cargo test`.

Local artifacts are under `target/perf/published-read-2026-10-10/`:
`aja-mount.log`, `remount-local.log`, `remount-read.log`,
`vfs-phases-debug-buffered.log`, `vfs-phases-debug-unbuffered-control.log`,
`vfs-sequential-debug-buffered.log`, and the standalone probe sources.
`collect-aja.ps1` mounts the configured volume using the fresh release binary
and redacts PostgreSQL DSNs from the captured output. Diagnostic coherence
fields are current at close; `disable_os_cache` records the earlier open decision.

The isolated Redb 8 GiB mounted probe passed, including zero aggregate read
callbacks on warmed read-only passes after verified `Ready`. The separate
PostgreSQL probe's file-ID publication assertion failed: its file ID did not
change even after the upload queues drained. That marker is not accepted as
publication evidence. Its own temporary file was removed.

No full workspace suite, ETW/WPR trace, or Total Commander copy was run.
The release product build, `cargo fmt --all -- --check`, focused
`cargo clippy -p studiofs-winfsp --all-targets -- -D warnings`, and
`git diff --check` passed. Sandbox filesystem access failures were retried with
native access and are not source/test failures.
Synthetic runtime-stat reads also showed delayed/aborted reads and were not used
as evidence for cache loss. Temporary backend overrides and the integration
probe were removed from production/test source. Diagnostic mounts were stopped;
the AJA-created file was preserved.

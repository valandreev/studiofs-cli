# SFS-780 Read-Window Storage Evidence

The speculative branch now reuses initialized storage from the existing
BufferPool. Idle retention is bounded to two 4 MiB buffers (8 MiB), in addition
to active per-handle windows. `Bytes::from_owner` returns storage only after the
last owner is dropped; errors and cancellation return the lease automatically.
Pool misses and growth still allocate/initialize, and each Bytes owner has a
small allocation. There is no unsafe output API or dependency change.

Initial focused validation passed: `read_window` 10 passed / 1 ignored, `buffer_pool`
6 passed / 1 ignored, and `read_into_fh_` 3 passed. Filters overlap. Crate check,
Clippy with `--all-targets -- -D warnings`, and formatting passed. The first
sandbox test failed during local payload persistence with PermissionDenied;
the same focused command passed outside the sandbox. No full suite ran.

## Storage microbenchmark

The initial storage microbenchmark replaced 256 cached 4 MiB windows. The
then-current buffer_pool.rs was compiled as an isolated test
module with `rustc --test --edition=2024 -C opt-level=3`, existing release
bytes/parking_lot dependencies, and MiMalloc as the global allocator to match
the product. Three runs took **47.74–60.05 ms fresh versus 24.82–27.58 ms
pooled**; medians were 56.53 / 27.21 ms. Without the product allocator the
system-allocator medians were 334.15 / 26.29 ms; those are not product results.

The source-derived logical write estimate is 2 GiB (zero-fill plus overwrite)
versus 1 GiB (overwrite after warmup). This is an estimate of eliminated writes,
not a hardware memory-bandwidth measurement. Pointer/content regression tests
prove that warmed storage is reused without clearing it.

The manual benchmark was removed from the unit-test module during review;
these historical results and raw logs remain available.

Review follow-up also removed the unreachable oversized-buffer Drop filter and
test case: the sole production acquisition clamps to the fixed 4 MiB window.
The pool-count cap test remains, and prefix copies now borrow through `as_ref()`.
Focused follow-up validation passed: `buffer_pool` 6/6, `read_into_fh_` 3/3,
and crate Clippy with `--all-targets -- -D warnings`.

## Mounted Windows comparison

Both release binaries were rebuilt: baseline `8f10a509` in an isolated worktree,
and the SFS-780 working tree. The user authorized `mount test Z:`. Three
sequential devbench runs per binary used 128 KiB requests, one 512 MiB file and
one worker; the mount was restarted between binary versions.

    studiofs.exe mount test Z: --detach
    studiofs.exe devbench Z:\ --profile write-path-gate --block-size 128KB --big-file-size 512MB --threads 1 --json-output target/perf/sfs-780/before-1.json
    studiofs.exe umount Z:

| Median metric | Before | After |
|---|---:|---:|
| During-upload read, MiB/s | 795.50 | 808.22 |
| Post-drain first read, MiB/s | 751.58 | 712.81 |
| Peak process RSS, MiB | 747.27 | 754.95 |

Host cache state was uncontrolled. The existing write-throughput target failed
in both versions; the absent medium-file phase was invalid because this workload
contains only a large file. Upload idle was observed. Runtime-stat pseudo-file
snapshots were unavailable, while host-local observability and allocator
snapshots were available. These results do not establish a throughput gain.

An additional isolated workload read the same owned 512 MiB file eight times
per run using aligned 128 KiB FILE_FLAG_NO_BUFFERING reads, reopening per pass
and validating the first/last byte of every chunk. Three runs per binary read
4 GiB each. Fresh snapshots from the host-local `write_path_observability.json`
bracketed the read workload; publisher PID stayed stable and upload idle was
required. The temporary runner and numeric raw results remain under
`target/perf/sfs-780/` as `direct-read.ps1` and `direct-{before,after}-{1,2,3}.json`.

| Isolated metric | Before | After |
|---|---:|---:|
| Median read throughput, MiB/s | 625.10 | 643.59 |
| Mean allocated bytes, warmed runs 2–3 | 4,598,972,304 | 4,598,986,768 |
| Mean allocation count, warmed runs 2–3 | 169,713.5 | 169,809.5 |
| Mean mount CPU time, warmed runs 2–3, ms | 4,710.94 | 4,460.94 |

The last two runs counted approximately 32,768 VFS payload read calls each,
plus a few small metadata/stat reads. Allocation totals include the entire
mount process and the one-second publisher interval. They show **no measurable
whole-process allocation reduction for this mounted workload**; do not claim a
4 GiB reduction or a statistically established speedup. No per-window mounted
counter or hardware memory-bandwidth profiler was added. The storage mechanism
is verified, while attribution of its mounted impact remains a validation gap.

Raw devbench reports, binary SHA-256 provenance, MiMalloc microbenchmark logs
and `summary.json` are preserved under `target/perf/sfs-780/`. The owned dataset
was deleted after measurement and the test mount was gracefully unmounted.

# SFS-806 macFUSE Read-Buffer Budget

The macFUSE adapter retains at most 32 idle read buffers and 32 MiB of their
actual `Vec::capacity()` total, instead of the previous 256 MiB worst case.
Individual retained allocations remain limited to 8 MiB. The 64 MiB default
in-flight read admission budget is separate and unchanged.

Requests and initialized storage are grouped by their next power of two.
Acquisition reuses an idle buffer only from the same class, and only when its
length covers the request; among equal capacities the most recently recycled
buffer is taken. A 128 KiB read cannot borrow an idle 8 MiB buffer. A miss
allocates the whole class with `vec![0; class_len]`, where `class_len` is the
next power of two and at most 8 MiB, and the reply is limited to the requested
length. Reuse can therefore lend initialized storage from the same class, less
than twice the request length. Requests above 8 MiB get an exact-size allocation
that is never retained. The 64 MiB admission limit counts request bytes, not
actual active buffer capacity or RSS.

Recycling pushes the returned buffer as the newest idle entry. Under count
pressure (more than 32 idle buffers) or byte pressure (more than 32 MiB of idle
capacity) the oldest idle buffer is evicted first, so the buffer just returned
survives. Evicted buffers are freed after the pool lock is released. The small
bounded vector keeps class lookup simple without a separate bucket map.

The existing overwritten-prefix, error, cancellation, and debug-poison contracts
remain in effect. Empty requests do not allocate or count as pool hits/misses.
The retained budget bounds idle allocations, not total process RSS: allocator
page retention, VFS caches, and active reads also contribute to RSS.

## Observability

`VfsWritePathObservabilitySnapshot.fuse_async_read` exports
`read_buffer_pool_hits`, `read_buffer_pool_misses`,
`read_buffer_pool_retained_bytes`, and `read_buffer_pool_budget_bytes`.
The mounted `.studiofs-runtime-stats` bridge section exports the same names with
a `fuse_` prefix, as counters for hits/misses and byte gauges for retained/budget.
Opening this file refreshes pool accounting after completed replies recycle
storage. Concurrent reads may change the pool after a snapshot is captured.
The adapter builds and stores each read snapshot under one publish mutex, so a
later store always carries counters at least as new as an earlier one.
Legacy JSON snapshots default these new fields; unsupported platforms report no
pool budget. A nonempty pool miss allocates one buffer, so miss deltas provide
pool allocation counts; process allocator counters include other allocations.

## Focused validation

Run from the repository root:

    cargo test -p studiofs-macfuse --lib read_buffer
    cargo test -p studiofs-macfuse --lib read_only_adapter
    cargo test -p studiofs-macfuse --lib runtime_stats_open_stays_direct_and_preserves_macfuse_cache_json
    cargo test -p studiofs-vfs --lib fuse_async_read_snapshot_serializes_buffer_pool_metrics
    cargo clippy -p studiofs-macfuse -p studiofs-vfs --all-targets -- -D warnings
    cargo check -p studiofs-macfuse --release
    cargo fmt --all -- --check
    git diff --check

On 2026-10-14 the `read_buffer` filter ran 16 tests: 15 pool tests in
`adapter::read_buffer::tests`, plus the queued-reply test
`read_only_adapter_reuses_queued_read_buffer_after_reply_completion`, whose name
contains `read_buffer`. The adapter and runtime-stats counts from that run are
superseded by the review follow-up below. Clippy, release check, formatting and
whitespace checks passed for the earlier state. No full suite was run.

Unit tests exercise size matching, initialized reuse, empty/oversized requests,
actual-capacity pressure, and buffer-count limits. Adapter tests exercise queued
EOF, sparse, error and canceled replies. Runtime stats checks assert actual
pool counters and retained/budget gauges after a miss and a hit.

## Mounted validation and EOF repair

The final release binary passed both standard macFUSE devbench workloads on
2026-10-14, with PostgreSQL/S3 available and without target debug logging.
Each run wrote and verified 129 files / 1,090,519,040 bytes. Both the mount and
unmount exited 0; no StudioFS mount remained active.

| Geometry | Elapsed | Pool hits (cumulative) | Pool misses (cumulative) | Idle retained bytes | Sampled peak RSS bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 128k | 21.682 s | 106954 | 6 | 606208 | 1284063232 |
| 4m | 20.395 s | 113973 | 8 | 3751936 | 1709408256 |

The pool budget was 33,554,432 bytes in both snapshots. These are consecutive
runs in one mount, so the second row's counters include the first run. Process
RSS includes VFS caches, dirty/staged payloads and allocator retention; these
measurements do not establish an RSS reduction against the old implementation.
The benchmark also reports runtime-counter reset warnings; inspect phase data
before treating whole-run deltas as allocation rates.

The repair addresses three connected paths:

- macFUSE now uses direct I/O when VFS reports active writers, unpublished local
  content, or an unknown cache generation. Observed generations (published or
  locally stable content) retain buffered reads and the existing file-size /
  available-memory policy.
- Last-handle attribute invalidation runs after local materialization. Accepted
  writes keep a refresh pending even when a flush clears dirty state before close.
- A prepared read of unpublished content cannot retain a stale hole map after
  local bytes move into an overlay. The data reader claims local spans first and
  resolves current metadata for the remaining ranges.

The last race was reproduced deterministically before the repair: a read
prepared before flush returned 52 zero bytes afterward instead of the exact
accepted payload. Two regressions cover new-file and published-file replacement
handoffs. Additional tests cover invalidation timing and kernel cache policy.
43 distinct focused tests passed, together with Clippy (`-D warnings`), release
build, formatting and whitespace checks. No full suite was run.

Commands (replace the mount path):

```sh
cargo build --release -p studiofs
studiofs devbench <macFUSE-mount> --block-size 128KB --big-file-size 1GB --small-file-count 128 --threads 4 --json-output 128k.json
studiofs devbench <macFUSE-mount> --block-size 4MB --big-file-size 1GB --small-file-count 128 --threads 4 --json-output 4m.json
```

Raw reports: `SFS-806-fixed-128k.json`, `SFS-806-fixed-4m.json` and
`SFS-806-fixed-summary.json`. The final binary embeds the preceding commit SHA
because the fix was validated before commit; it was built from the working diff.

Separate limitation, now resolved: previously exercised test volumes later
failed startup with `inode not found` (including inode 1649267441820). The
cause is SFS-855: startup replay of unflushed local events read a file that
was created and deleted inside the window through its tombstone. The fix is
part of this branch. Pending reads now may perform additional metadata
lookups.

## Review follow-up (2026-10-14)

The mounted numbers in the sections above predate the review follow-up. That
follow-up changed the pool to oldest-first eviction, class-sized misses and
serialized snapshot publication, and it changed the open policy so that only
observed generations keep buffered reads. The 128k and 4m tables therefore
describe the earlier policy and do not establish results for the current code.

### Mounted rerun against a `main` baseline

Both runs used the same macOS volume and directory, one mount per binary, and
three devbench phases in that mount (`--threads 4 --big-file-size 1GB
--small-file-count 128`). The baseline binary is `main` plus the SFS-855
startup-replay fix, which does not touch the read path. Peak RSS is
`/usr/bin/time -l` maximum resident set size of the mount process; per-phase
RSS is sampled after each phase. Each configuration ran once, so the elapsed
times are indicative only.

| Phase | Block | Elapsed, s (branch / baseline) | Mount RSS after phase, MiB (branch / baseline) |
| --- | --- | --- | --- |
| 128k | 128 KB | 27 / 22 | 1364 / 1396 |
| 4m | 4 MB | 25 / 23 | 1729 / 2023 |
| odd100k | 100 KB, 100 KB small files | 89 / 80 | 2322 / 2698 |
| Peak RSS | | | 2330 / 2763 |

The branch peak RSS is about 16% lower than the baseline. The phase times
differ by a few seconds in the baseline's favour, which is within the noise of
a single run that also measures the write path; a throughput regression is not
established either way.

Pool counters from the branch run (deltas per phase, from the root
`.studiofs-runtime-stats` before and after each phase):

| Phase | Hits | Misses | Retained bytes after phase |
| --- | --- | --- | --- |
| 128k | 106,951 | 6 | 1,114,112 |
| 4m | 7,183 | 2 | 4,259,840 |
| odd100k | 136,737 | 4 | 4,321,280 |

Twelve misses over about 251 thousand reads give a pool allocation rate below
0.005% of reads, and retained idle storage stayed below 4.2 MiB against the
32 MiB budget. The baseline has no pool counters, so its allocation rate is
not available for comparison. The raw reports and runtime-stats snapshots are
kept under `target/sfs806-bench/{fixed,baseline}/` on the benchmark machine and
are not committed.

A final run with the merged binary (this branch plus the SFS-855 fix) started
from the unflushed backlog left by the baseline run. Startup replay succeeded,
and all three phases passed. The pool recorded 12 misses over about 251
thousand reads, and peak mount RSS was 2250 MiB. The local event log drained
before unmount, and a remount succeeded. The first phase took 40 s because it
overlapped the backlog catch-up.

Focused unit runs on the current code, 2026-10-14, all passed:

- `read_buffer` filter (`cargo test -p studiofs-macfuse --lib read_buffer`):
  16 tests.
- Combined macFUSE filter over `read_buffer`, `read_only_adapter`,
  `runtime_stats_open`, `unpublished_adapter_read`, `stable_empty_file`,
  `adapter_dirty_write_release`, `adapter_locally_materialized` and
  `read_only_open_policy`: 41 tests in one run.
- Combined VFS filter over `release_fh_`, `deferred_last_handle_release`,
  `prepared_read_survives_local_overlay`,
  `prepared_published_read_uses_new_local`
  and `fuse_async_read_snapshot_serializes`: 25 tests in one run.

These are unit-level results; the mounted rerun above covers the current code.

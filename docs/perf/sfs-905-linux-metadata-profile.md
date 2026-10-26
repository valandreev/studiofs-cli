# SFS-905 Linux Metadata Profile: `pg-meta-poll` and Foreground Stalls

SFS-905 re-measured the SFS-890 criterion that `pg-meta-poll` CPU per
foreground metadata operation stays well below the operation's own cost, using
`perf` built for the WSL2 kernel. The criterion now holds. The profile also
found two foreground stalls in the create path, both fixed here, and the cause
of the SFS-904 rename growth.

**Verdict:**

- `pg-meta-poll` costs 0.13 to 0.28 ms per rename, against 5.2 ms in the audit
  baseline. That is below the rename's own FUSE-thread cost in every phase.
- `open(O_CREAT|O_TRUNC)` of a new file waited about 10 ms for the deferred
  namespace batch linger. Fixed: 12.1 ms to 0.5 ms.
- A size change right after a write on an open handle waited out the whole
  deferred-materialization linger (6 × 200 ms). Fixed: 1,216 ms to 10 ms.
- Rename cost in a large directory comes from a case-insensitive directory scan
  on every negative lookup. Fixed in SFS-904: 3.34 ms to 0.26 ms.

## Stand

| Item | Value |
|---|---|
| Revision | `55eba0233` plus this change, release build with `line-tables-only` debuginfo and frame pointers |
| Host | WSL2 kernel `6.6.114.1-microsoft-standard-WSL2`, 24 vCPU |
| Object storage | Remote S3 endpoint from `.env` |
| PostgreSQL | Remote host from `.env`, about 33 ms per round trip |
| Profiler | `perf` 6.6.114.1 built from `linux-msft-wsl-6.6.114.1`, `cpu-clock` at 999 Hz, frame-pointer call graphs |
| `perf_event_paranoid` | 2: user-space stacks only; per-thread CPU from `/proc` includes kernel time |
| Volume | One fresh volume, default Adaptive cache policy |

Setup and the profiling script are described in
[linux-perf-profiling.md](linux-perf-profiling.md). Each phase ran 1,000
operations from a Python driver against one mounted volume:

| Phase | Workload |
|---|---|
| `rename` | 1,000 renames in one directory |
| `create` | 1,000 × (`open(O_CREAT\|O_TRUNC)`, write 4 KiB, close) |
| `parallel` | 8 processes, each renaming then unlinking 125 files (2,000 ops) |
| `rename-big` | 1,000 renames in a directory with 8,000 extra entries |

A repeat with local PostgreSQL was not run; PostgreSQL stays remote.

## Results

On-CPU ms per operation by thread group, from `/proc` user plus system time,
including trailing replication work until the threads go idle.

### Before the fixes (`55eba0233`)

| Phase | Wall ms/op | FUSE | tokio | `pg-meta-poll` | other |
|---|---|---|---|---|---|
| rename | 0.66 | 0.40 | 0.04 | 0.13 | 0.07 |
| create | 13.33 | 0.46 | 4.11 | 0.36 | 0.24 |
| parallel | 0.23 | 0.31 | 0.04 | 0.08 | 0.03 |
| rename-big | 3.37 | 2.85 | 0.16 | 0.26 | 0.23 |

### After the fixes

| Phase | Wall ms/op | FUSE | tokio | `pg-meta-poll` | other |
|---|---|---|---|---|---|
| rename | 0.77 / 0.84 / 0.89 | 0.49 / 0.49 / 0.50 | 0.04 / 0.03 / 0.09 | 0.18 / 0.19 / 0.17 | 0.05 / 0.12 / 0.14 |
| create | **3.86** | 0.50 | 4.50 | 0.47 | 0.27 |
| parallel | 0.29 / 0.29 / 0.29 | 0.34 / 0.39 / 0.39 | 0.08 / 0.12 / 0.10 | 0.13 / 0.16 / 0.14 | 0.06 / 0.07 / 0.04 |
| rename-big | 3.34 | 2.77 | 0.11 | 0.28 | 0.23 |

Rename and parallel were repeated three times. The rename path did not change.
The after runs used the same volume, which by then held several thousand
files from earlier runs, so the higher rename cost reflects volume state and
remote-PostgreSQL variance, not this change.

### Audit baseline

| Phase | FUSE threads | tokio workers | `pg-meta-poll` |
|---|---|---|---|
| rename | 1.9 ms/op | 3.8 ms/op | 5.2 ms/op |
| create + write 4K + close | 0.3 ms/op | 2.6 ms/op | 1.1 ms/op |

## `pg-meta-poll`

The SFS-890 criterion holds. `pg-meta-poll` CPU is 0.13 to 0.19 ms per rename
against 0.40 to 0.50 ms of FUSE-thread time for the rename itself, and 0.28
against 2.77 ms in the large directory.

In the create phase `pg-meta-poll` is close to the FUSE time (0.47 against
0.50 ms) but about a tenth of the operation's total cost, which is dominated by
background payload work on tokio (4.5 ms). Its stacks are
`flush_due_pending` → `pull_and_reconcile_foreign_events_locked` →
`reconcile_regular_file_serving_from_base_with_protected_dentries`: replaying
and reconciling the volume's own payload publication, driven by background
upload completion, not by the foreground create.

The parallel phase finished 2,000 operations in 0.57 s, about 3,500 ops/s,
against about 1,200 ops/s for single-process renames. Parallel throughput no
longer drops below single-process throughput on this stand (SFS-903).

## Fixed: `O_CREAT|O_TRUNC` waited for the namespace batch linger

The create phase wall time (13.3 ms/op) was mostly off-CPU. Timing individual
syscalls showed the stall:

| Call on a new file | p50 before | p50 after |
|---|---|---|
| `open(O_CREAT)` | 0.22 ms | 0.20 ms |
| `open(O_CREAT\|O_TRUNC)` | 12.11 ms | 0.53 ms |
| create, then `ftruncate(0)` | 11.96 ms | 0.54 ms |
| create + write 4 KiB + close (`O_TRUNC`) | 13.33 ms | 2.01 ms |

`O_TRUNC` on a just-created inode calls `setattr(size=0)`. While the create's
deferred namespace metadata is still queued, setattr first drains it through
`drain_deferred_namespace_metadata_for_barrier`. The background drainer holds
the drain while it lingers 10 ms (`DEFERRED_NAMESPACE_BATCH_LINGER_DELAY`) for
more admissions, and the barrier waited out the linger. A drain run by the
barrier itself lingered as well.

Barriers now register as waiters. The drainer stops lingering while a barrier
waits, and a barrier-owned drain commits up to its cutoff without lingering.
Batching for background admissions is unchanged.

## Fixed: size change after a write waited out the materialization linger

| Call on an open, just-written file | p50 before | p50 after |
|---|---|---|
| `ftruncate(100)` (shrink) | 1,215.7 ms | 10.1 ms |
| `ftruncate(8192)` (grow) | 1,215.9 ms | 9.4 ms |
| `truncate(path, 0)` after close | 8.6 ms | unchanged |
| `fsync` after write | 15.4 ms | unchanged |

A FUSE write schedules a deferred materialization task that holds the inode's
materialize gate while it lingers for more writes, up to
`DEFERRED_MATERIALIZATION_LINGER_WAIT_MS × DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES`
= 1.2 s. Flush, fsync, and release interrupt that linger through
`wait_for_deferred_materialization_inflight` before entering the file session.
`setattr_with_size_change` did not, so its `Flush` session waited for the gate
until the linger expired.

Strict size changes now interrupt and wait for the task, like flush and fsync.
Handle-backed growth, which may overtake materialization, only asks the task to
stop lingering while it waits for the gate.

## Fixed in SFS-904: rename cost grows with directory size

In `rename-big`, 50% of FUSE-thread samples are under
`RedbMetaEngine::lookup`, mostly `redb::table::Range::next`. A lookup that
misses the exact key falls back to `lookup_merged_from_opened_tables`, which
scans every entry of the parent in both the current and base dentry tables to
find a case-insensitive match. Every rename looks up its not-yet-existing
target name, so each rename scans the directory twice (FUSE `lookup` and
`rename_atomic`). The fix needs a persistent case-folded dentry index
maintained by local writes and replication apply, so it is tracked in SFS-904
instead of being fixed here.

SFS-904 added the case-folded dentry indexes `dentries_by_folded_name` and
`base_dentries_by_folded_name`, so a miss visits only names that fold to the
requested name. With a stub in place of `perf` (wall and `/proc` CPU only, same
script, release build, fresh volume, remote S3 and PostgreSQL), two runs gave:

| Phase | Wall ms/op | FUSE |
|---|---|---|
| rename | 0.228 / 0.245 | 0.13 / 0.14 |
| rename-big | 0.258 / 0.261 | 0.17 / 0.17 |

Rename with 8,000 extra entries is within 7–13% of the empty-directory case. A
third run under `perf` (sampling adds wall time) gave 0.393 and 0.415 ms/op;
`redb::table::Range::next` fell from about 50% to 3.4% of FUSE-thread samples
in `rename-big`.

## Regression tests

```sh
cargo test -p studiofs-vfs --lib deferred_namespace_barrier_drain_commits_without_batch_linger -- --nocapture
cargo test -p studiofs-vfs --lib deferred_namespace_barrier_cuts_background_batch_linger_short -- --nocapture
cargo test -p studiofs-vfs --lib truncate_after_deferred_write_interrupts_materialization_linger -- --nocapture
cargo test -p studiofs-vfs --lib handle_growth_after_deferred_write_interrupts_materialization_linger -- --nocapture
```

The barrier tests set the namespace batch linger to one hour, so they pass only
when the linger is cut. The size-change tests wait until the materialization
task holds its session, then require the size change to finish before the 1.2 s
linger would expire.

The mounted measurements above were taken with
[`tools/perf/linux-metadata-profile.sh`](../../tools/perf/linux-metadata-profile.sh)
and are not a registered `studiofs-e2e` scenario.

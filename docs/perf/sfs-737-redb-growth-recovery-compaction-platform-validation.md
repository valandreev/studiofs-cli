# SFS-737 redb growth, recovery, and compaction platform validation

- **Recorded:** 2026-09-27
- **Windows source revision:** `853106ed9b0c53c3b8e3362b32346e4911d589e0`
- **Linux validation revision:** `af71f2e4`
- **Platforms:** Windows 11 and Linux x86_64. Host CPU, OS build, and temporary-volume details were not retained.

## Scope and workload

This records the Windows and Linux measurements from the ignored SFS-733
`local_metadata_churn_redb_growth_workload`. Each run uses a fresh local redb
database and four deterministic churn cycles, with 1,024 temporary files per
cycle and a fixed live namespace of four survivor files. The workload does not
mount StudioFS or connect to PostgreSQL or S3.

The two platforms use the same workload shape: two runs without explicit
compaction and three with explicit compaction at the end. Physical sizes and
compaction timings are compared within each platform; identical bytes across
filesystems are not required.

## Windows results

The no-compaction workload does not exercise journal pruning or scheduled
maintenance. These are baseline observations, not a steady-state or
foreground-latency acceptance result. The retained record has the reported
start/end values and compaction timings, but not the complete raw checkpoint
output for every trial.

### Measurements

### No explicit compaction

The available summary for the two runs reports these start/end values:

| Trials | Live files at checkpoints | Event-log entries | Physical redb file |
| ---: | ---: | ---: | ---: |
| 2 | 4 | 18 → 77,842 records | 1,056,768 → 8,425,472 bytes |

### Explicit compaction

Across all three runs, the pre-compaction size was 8,425,472 bytes and the
post-compaction size was 4,255,744 bytes; only the duration varied by run:

| Run | Before | After | Bytes reclaimed | Duration |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 56.7 ms |
| 2 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 47.4 ms |
| 3 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 51.1 ms |

## Linux results

The release-profile Linux run repeated the Windows workload shape with
`STUDIOFS_REDB_CHURN_FILES=1024`. All five runs passed.

### No explicit compaction

| Trials | Live files at checkpoints | Event-log entries | Physical redb file |
| ---: | ---: | ---: | ---: |
| 2 | 4 | 18 → 77,842 records | 1,056,768 → 8,425,472 bytes |

### Explicit compaction

All three runs reduced the file from 8,425,472 to 4,255,744 bytes and reclaimed
4,169,728 bytes:

| Run | Before | After | Bytes reclaimed | Duration |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 279.7 ms |
| 2 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 277.9 ms |
| 3 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 469.1 ms |

## Platform validation

The 40 focused unit tests for pruning/reopen/recovery, uploaded-file repair
after pruning, unclean redb recovery, redb format, VFS compaction policy and
lifecycle, and cache-index compatibility passed. The native WinFSP
`mounted-client-crash-restart-recovery` scenario also passed in both modes,
including preflight, validation, and cleanup (run `20260927T114847Z-9b7d06`).

Linux portable validation passed: the reproduced 40-test metadata/recovery set,
three cache-index tests, and the focused VFS compaction, activity,
runtime-statistics, and uploaded-payload recovery tests all passed. The native
Linux `mounted-client-crash-restart-recovery` attempt stopped at preflight
because the Docker socket was inaccessible and the required host ports could
not be bound (run `20260927T125634Z-d5d22e`). The three direct ignored libfuse
mount tests also could not pass because `/dev/fuse` was absent.

## Interpretation

The bounded live namespace remained at four files while the no-compaction
event-log entry count and redb file grew on both platforms. Explicit compaction
reduced the measured file size in all six compacted runs. Linux compaction was
slower than the Windows measurements, but the workload still reclaimed the
same number of bytes. These results do not establish that growth plateaus
after pruning, measure foreground latency during concurrent mounted I/O, or
provide native Linux FUSE or macOS acceptance. SFS-737 remains In Progress
until the environment-gated native Linux validation is available.

The workload and reproduction commands are documented in
[`local-redb-growth-workload.md`](local-redb-growth-workload.md).

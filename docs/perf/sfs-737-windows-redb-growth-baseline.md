# SFS-737 Windows redb growth baseline

- **Recorded:** 2026-09-27
- **Source revision:** `853106ed9b0c53c3b8e3362b32346e4911d589e0`
- **Platform:** Windows 11. The retained performance output does not include the CPU model, Windows build, or temporary-volume details.

## Scope and workload

This records the Windows measurements from the ignored SFS-733
`local_metadata_churn_redb_growth_workload`. Each run uses a fresh local redb
database and four deterministic churn cycles, with 1,024 temporary files per
cycle and a fixed live namespace of four survivor files. The workload does not
mount StudioFS or connect to PostgreSQL or S3.

Two runs were made without explicit compaction and three with explicit
compaction at the end. The no-compaction workload does not exercise journal
pruning or scheduled maintenance. These are baseline observations, not a
steady-state or foreground-latency acceptance result. The retained record has
the reported start/end values and compaction timings, but not the complete raw
checkpoint output for every trial.

## Measurements

### No explicit compaction

The available summary for the two runs reports these start/end values:

| Trials | Live files at checkpoints | Event-log entries | Physical redb file |
| ---: | ---: | ---: | ---: |
| 2 | 4 | 18 → 77,842 records | 1,056,768 → 8,425,472 bytes |

### Explicit compaction

Each of the three runs reported the same physical size before and after
compaction; the duration varied by run:

| Run | Before | After | Bytes reclaimed | Duration |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 56.7 ms |
| 2 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 47.4 ms |
| 3 | 8,425,472 bytes | 4,255,744 bytes | 4,169,728 | 51.1 ms |

## Related Windows validation

The 40 focused unit tests for pruning/reopen/recovery, uploaded-file repair
after pruning, unclean redb recovery, redb format, VFS compaction policy and
lifecycle, and cache-index compatibility passed. The native WinFSP
`mounted-client-crash-restart-recovery` scenario also passed in both modes,
including preflight, validation, and cleanup (run `20260927T114847Z-9b7d06`).

## Interpretation

The bounded live namespace remained at four files while the no-compaction
event-log entry count and redb file grew. Explicit compaction reduced the
measured file size in all three runs. These results do not establish that growth
plateaus after pruning, measure foreground latency during concurrent mounted
I/O, or cover macOS/Linux. They record the Windows portion of SFS-737; the
issue remains In Progress.

The workload and reproduction commands are documented in
[`local-redb-growth-workload.md`](local-redb-growth-workload.md).

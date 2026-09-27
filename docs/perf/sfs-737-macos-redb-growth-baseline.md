# SFS-737 macOS redb growth and recovery baseline

- **Recorded:** 2026-09-27
- **Source revision:** `af71f2e4`
- **Host:** macOS 26.6.2, arm64
- **Mounted backend:** macFUSE

## Workload and measurements

The release `local_metadata_churn_redb_growth_workload` used a fresh local
redb database for each run, four deterministic churn cycles, 1,024 temporary
files per cycle, and four surviving files. Two runs had no explicit compaction;
three invoked explicit compaction after the final cycle. All five runs passed.
This workload does not mount StudioFS or connect to PostgreSQL or S3.

| Mode | Runs | Live files | Retained event log | Physical redb size |
| --- | ---: | ---: | ---: | ---: |
| No explicit compaction | 2 | 4 | 18 → 77,842 records | 1,056,768 → 8,425,472 bytes |
| Explicit compaction | 3 | 4 | 18 → 77,842 records | 8,425,472 → 4,255,744 bytes |

Each explicit-compaction run reclaimed 4,169,728 bytes. Durations were 100.5,
127.4, and 88.5 ms. The file-size observations match the Windows and Linux
runs; the compaction timings are specific to this host.

## Focused tests

- 39 `studiofs-meta` tests passed across pruning, reopen/recovery, format
  probing, uploaded-file lifecycle repair, and local compaction diagnostics.
- 3 `studiofs-cache` cache-index compatibility and reopen tests passed.
- 24 unique `studiofs-vfs` tests passed across compaction, metadata activity,
  prune metrics, and uploaded-payload recovery. One metadata-activity test also
  matched the compaction filter.
- All 182 `studiofs-macfuse` library tests passed.

## Native mounted recovery

The registered `mounted-client-crash-restart-recovery` scenario passed on
`macos-fuse` in run `20260927-sfs737-macos`. Both
`after_local_durability_boundary` and
`before_remote_publication_completes` recovered the writer state and converged
the two mounted clients. Restart took 737 ms and 750 ms; convergence took 9 ms
in each mode. Runner preflight and cleanup passed.

The full report is retained at
`target/e2e-runs/20260927-sfs737-macos/report.json`; mode details are in
`target/e2e-runs/20260927-sfs737-macos/artifacts/scenarios/mounted-client-crash-restart-recovery/crash-restart-recovery-summary.json`.
Raw churn logs are under `target/sfs-737-macos/`.

## Scope

The churn workload does not acknowledge or prune local event rows, so its
77,842-row ending event log is not evidence that retained history plateaus.
These runs also do not measure foreground metadata latency while scheduled
compaction overlaps mounted I/O. The native scenario validates crash recovery,
not compaction latency. SFS-737 still needs integrated prune/maintenance soak
and foreground-latency evidence before its full acceptance criteria are met.

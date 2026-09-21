# SFS-718 PostgreSQL catch-up and checkpoint thresholds

This note records the SFS-718 measurement used to replace the provisional
foreign-event response-byte default and to keep the checkpoint freshness
defaults evidence-backed. The benchmark exercises the existing mounted
large-history scenario; it is not a new liveness gate or a foreground
PostgreSQL dependency.

## Method

The release binaries were run on the Windows/WinFSP host with Docker Compose
PostgreSQL constrained to one CPU and 6 GiB of memory. Each selected case ran
twice with a fresh run ID and retained preflight, scenario, validation,
cleanup, status NDJSON, raw output, and sampled client/PostgreSQL memory
artifacts. The runner measured page query, local apply, and end-to-end page
durations in microseconds. Successful checkpoint publications additionally
recorded payload size, local build duration, fenced PostgreSQL publication
duration, and total duration.

The final page-budget matrix is retained at
`target/perf/sfs-718-pg-sync-20260921T165707081Z-18824`. It contains two
passing runs each for page budgets of 8, 32, and 64 complete batches, and for
a 1 MiB response-byte budget. The 2 MiB control ran separately at
`target/perf/sfs-718-pg-sync-20260921T170726966Z-40736` and also has two
passing runs. The checkpoint policy matrix is retained at
`target/perf/sfs-718-pg-sync-20260921T163809119Z-43268`; all fourteen selected
cells passed.

A preliminary one-batch-per-page control timed out the existing 60-second
mounted-tree validation twice while collecting only 117 of 125 pages. It is
retained in the earlier matrix artifacts as a pathological lower-bound probe,
but is excluded from the default harness matrix so a normal full invocation
contains only passing acceptance profiles.

The benchmark did not inject 50–100 ms PostgreSQL RTT. The Compose stack has a
Toxiproxy service, but this scenario does not route its PostgreSQL connection
through a configured toxic, so no artificial network-latency result is claimed.

## Page-budget results

Values below are per-run page p95s in milliseconds; the two values preserve
the two repetitions. The wall column is the scenario's large-history phase,
not a promise for arbitrary workloads.

| Profile | Unique pages | Largest response | Largest batch count | Page total p95 | Scenario wall |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8 batches | 19 | 222 KiB | 8 | 944.9 / 1014.3 ms | 35.3 / 45.9 s |
| 32 batches | 7 | 888 KiB | 32 | 2547.1 / 1507.5 ms | 19.7 / 19.5 s |
| 64 batches | 4 | 1.70 MiB | 64 | 2720.8 / 2357.4 ms | 16.7 / 16.2 s |
| 1 MiB cap | 7 | 1000 KiB | 36 | 2860.4 / 1649.7 ms | 32.0 / 18.5 s |
| 2 MiB cap | 4 | 1.70 MiB | 64 | 2136.2 / 2696.9 ms | 16.4 / 17.1 s |

All page profiles had bounded complete-batch pages. The 1 MiB cap split the
work into more pages without a stable tail improvement. The 2 MiB control did
not bind this fixture—the largest observed response was 1.70 MiB—and matched
the 64-batch wall-time profile.

The selected production defaults are therefore:

- keep `STUDIOFS_PG_SYNC_MAX_BATCHES_PER_PAGE = 64`, because it had the
  shortest repeatable complete catch-up wall time and did not produce a
  pathological response in this realistic fixture;
- change `STUDIOFS_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE` from 8 MiB to 2 MiB,
  which leaves measured headroom above the 1.70 MiB response while making the
  byte bound explicit; and
- retain the complete-batch and oversized-single-batch behavior. A single
  event that exceeds the byte budget must still be returned atomically and
  marked for explicit handling.

## Checkpoint-policy results

The production checkpoint coordinator produced a 344,883-byte monolithic
payload in the triggered cells. The repeated timings were:

| Trigger profile | Build | Publish | Total | Peak client working set |
| --- | ---: | ---: | ---: | ---: |
| Version lag 32 | 60.5 / 66.6 ms | 14.1 / 13.2 ms | 74.6 / 79.8 ms | 209 MiB |
| Event payload 1 MiB | 521.2 / 684.4 ms | 13.4 / 19.9 ms | 534.6 / 704.3 ms | 817 MiB |
| Age 1 second | 53.9 / 587.6 ms | 15.9 / 22.8 ms | 69.7 / 610.4 ms | 237 MiB |

The checkpoint defaults remain `10,000` versions, `64 MiB` of encoded event
payload, and `3,600` seconds. The 32-version trigger was fast in this small
fixture, but it would request checkpoint work much more frequently. The 1 MiB
and one-second triggers showed materially larger and less stable build tails,
including the 817 MiB client high-water mark. Those values are unsuitable as
general defaults without a broader workload and RTT matrix. The thresholds
remain positive environment overrides:

- `STUDIOFS_PG_CHECKPOINT_MAX_VERSION_LAG`
- `STUDIOFS_PG_CHECKPOINT_MAX_EVENT_BYTES`
- `STUDIOFS_PG_CHECKPOINT_MAX_AGE_SECS`

## Reproduction

Build release binaries, then run the full predefined matrix or a comma-
separated subset from `E:\dev\studiofs-cli`:

    cargo build --release -p studiofs -p studiofs-e2e
    & 'C:\Program Files\PowerShell\7\pwsh.exe' -NoProfile -File .\tools\perf\sfs-718-pg-sync.ps1 -StudiofsBin .\target\release\studiofs.exe -E2eBin .\target\release\studiofs-e2e.exe -Runs 2 -SelectedCase page-batches-32,page-batches-64,page-bytes-2m -ConstrainPostgres

Each invocation creates a non-overwriting directory under `target/perf`.
`matrix.json` records the case settings, each cell retains `command.txt`,
`memory-samples.ndjson`, `metrics.json`, the runner report, and the scenario's
retained artifacts, and the root `summary.json` reports passing-cell tails.

The result is host-specific performance evidence, not a universal latency
SLO. Full workspace tests and the separate 50–100 ms RTT experiment remain
follow-up validation gates.

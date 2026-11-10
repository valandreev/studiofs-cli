# SFS-979: CLI janitor benchmark and soak test

Recorded 2026-11-10 on Windows 11 with Docker Desktop. The candidate is this branch's `studiofs janitor`. The baseline is main commit `2c39e212`: the eager pre-redesign pipeline plus the SFS-964 timing output. All live numbers come from disposable PostgreSQL 18 and RustFS containers on loopback. Nothing touched a production bucket or server. **Synthetic** results use a loopback S3 listing with fake metadata. **Live** results use real RustFS and PostgreSQL. The tables label every row.

## Conclusions

- **Safety holds.** Every candidate run kept every referenced object. That covers 500 to 99,000 seeded live references plus 1,850 to 11,975 objects that concurrent foreground writers created during the run. Every candidate run also reached complete coverage with zero orphans left. The baseline never deletes legacy keys, so it left 167 of 500 orphans (1K runs) and 667 of 1,000 orphans (100K run) on S3. It also reported 500 deletions in each run, more than it removed. It classified 266 objects that were referenced *after* its start-up snapshot as orphan candidates. None was deleted, only because its delete cap was reached first.
- **Memory is flat.** Candidate peak RSS was 16–17 MiB for 1K and 100K live keys. Synthetic peak RSS was 17 MiB at 100K keys and 25–29 MiB at 1M keys, of which about 7 MiB is fixture bookkeeping. The pipeline buffered at most 64 candidates. The baseline buffers the whole listing: 82 MiB at 100K keys.
- **Small PostgreSQL servers are not saturated.** With PostgreSQL limited to 1 vCPU and 512 MiB, the candidate used one connection. Peak CPU was 12% and peak memory 56–81 MiB. Foreground PostgreSQL inserts stayed at p99 ≤ 9 ms and foreground S3 PUTs at p99 ≤ 94 ms during every run.
- **S3 load is bounded.** The candidate never had more than one S3 request or connection in flight. With 503 or 429 on every 20th request over 1M keys, it retried 157 requests, deleted every orphan, and took 28% longer.
- **Throughput is bound by PostgreSQL round-trip time (RTT), not by S3 or server CPU.** A 500-deletion cycle took 443 s at about 0 ms RTT and 1,323 s at 100 ms RTT. S3 DELETE time was 170–194 s in both. That is about 18 extra round trips per deleted object and about 20 per 64-key eligibility batch. At 100 ms RTT, scanning runs at about 29 keys/s, so 1M keys take about 9.6 hours. The baseline is 5–14× faster in wall time because it reads metadata once and does not fence deletions. No speedup is claimed. SFS-1064 cut the round trips (see below): deletion now runs at about 1.83 objects/s, close to the 2 objects/s that one DELETE per key allows at the default 4 requests/s. SFS-1065 tracks S3 multi-object delete.

## SFS-1064: PostgreSQL round trips

Recorded 2026-11-10 with the live benchmark at 100 ms PostgreSQL RTT, 20 ms S3 RTT, PostgreSQL at 1 vCPU / 512 MiB, 4 S3 requests/s, `--max-deletes 500`, and the usual foreground load. "Before" is main commit `560db2ca`, re-run under the same conditions; its 0.33 objects/s is slightly below the 0.38 recorded above. The PostgreSQL proxy now counts round trips (`pg_round_trips`, total and per run): a server reply after client bytes ends one round trip, so a pipelined burst counts once. Scan-only runs use `ORPHAN_PERCENT=0`, so they measure eligibility batches without claims.

The changes, in order:

1. **Prepared and pipelined statements.** The janitor session prepares each statement once per connection. The statements of one call are sent together and PostgreSQL runs them in order, so the read-only eligibility transaction is one round trip, and the checkpoint save is another.
2. **Batched claim and settle.** One set-based statement claims up to 64 objects and returns one fencing token per object. It is sent in a second round trip, only after the `metadata_state` share lock is granted, so a claim queued behind a waiting lock cannot commit after the process died. One statement settles them (fenced finish, queue acknowledgement or 60-second deferral). A stale token still cannot finish a newer claim, and one object's failure still defers only that object. An object starts its deletes only while the 60-second claim lease still covers its worst-case S3 work; later objects are released and claimed again.
3. **Eligibility batch 64 → 1,000**, one S3 listing page. Claims stay at 64 objects.

Round trips (fitted from the 2K and 10K scan runs; each run also pays about 22–25 fixed round trips for connect, lock, validation, queue page and fence pruning):

    step                     per eligibility batch   per 1,000 keys scanned   per deleted object
    before                   ~17 (64 keys)           ~270                     ~18
    1+2, batch 64            2 (64 keys)             ~31                      ~0.1
    3, batch 1,000           2 (1,000 keys)          ~2                       ~0.1

Throughput and load (all candidate runs kept every referenced and foreground object and finished the sweep):

    run                      keys    orphans  runs  time      PG round trips  scan keys/s  delete obj/s  PG CPU  fg PG p99
    before, scan             10,000  0        1     394.7 s   3,126           29           –             4.8%    4 ms
    1+2 batch 64, scan       10,000  0        1     50.2 s    341             203          –             4.2%    6 ms
    3 batch 1,000, scan      10,000  0        1     13.0 s    44              771          –             3.7%    5 ms
    before, delete           1,000   500      2     1,523.4 s 10,966          –            0.33          9.4%    13 ms
    1+2 batch 64, delete     1,000   500      2     279.2 s   142             –            1.79          7.2%    4 ms
    3 batch 1,000, delete    1,000   500      2     273.1 s   96              –            1.83          4.9%    5 ms

The 500-deletion cycle now holds the global janitor lock (`global_lock_hold_ms`) for 268 s instead of 1,315 s. S3 DELETE time was about 160 s in every deleting run, so deletion now runs at the S3 gate: two DELETE requests per object at 4 requests/s allow at most 2 objects/s. SFS-1065 (S3 multi-object delete) is the next ceiling. PostgreSQL stayed unsaturated: at most 14% of one vCPU (in an earlier batch-1,000 run that pipelined the claim behind the fence) and at most 70 MiB. Each claim batch locks at most 64 ledger rows for one pipelined transaction; the eligibility transaction is read-only. Per-statement time was not measured separately; at 1,000 IDs per eligibility batch the whole scan-only run stayed under 4% CPU. The checkpoint save stays its own round trip: it must follow the batch's claims and settles, and it costs one round trip per 1,000 keys. The "before" scan runs list more keys than their seeded objects because the foreground load keeps adding objects for as long as a run lasts.

The rows that follow this section predate SFS-1064.

## Parameter choices

The defaults stay as they are. Each change considered was rejected against the evidence:

- `--s3-requests-per-second` stays at **4**. At 4 requests/s the S3 gate allows 2 objects/s. Measured deletion was at most 1.1 objects/s at about 0 ms RTT and 0.38 objects/s at 100 ms RTT, so a higher default would not speed up a typical remote PostgreSQL setup. Operators with PostgreSQL on the LAN and a tolerant provider can raise it to 8–16.
- `--max-deletes` stays at **500**. At 100 ms RTT, one capped cycle holds the global janitor lock for about 22 minutes. A larger cap only lengthens that hold. Resumed cycles continue from the saved checkpoint: the 100K live run covered 52,737, then 50,038, then 53 entries over three cycles.
- Use `--max-scan-entries` or `--max-runtime-minutes` for very large buckets on high-latency PostgreSQL, and let `--loop` or repeated runs continue from the checkpoint.

## Live results

All live rows ran with PostgreSQL at 1 vCPU / 512 MiB, S3 RTT of 20 ms, grace of 0 minutes, `--max-deletes 500`, and foreground load of one S3 PUT plus one PostgreSQL insert every 250 ms. Orphans are spread across the key space; one third use the legacy key layout and two thirds the fragment layout. The time column is the sum of all runs until the candidate exits 0; the baseline is one run.

    keys     orphans  PG RTT  binary     S3 req/s  runs  time      first cand.  orphans left  refs missing  peak RSS  PG conns  PG CPU
    1,000    500      0 ms    baseline   unlimited 1     84.5 s    1.2 s        167           0             19 MiB    4         2.6%
    1,000    500      0 ms    candidate  4         2     458.8 s   0.6 s        0             0             16 MiB    1         11.2%
    1,000    500      100 ms  baseline   unlimited 1     94.2 s    1.5 s        167           0             19 MiB    4         11.4%
    1,000    500      100 ms  candidate  4         2     1,535.5 s 2.8 s        0             0             16 MiB    1         11.8%
    100,000  1,000    50 ms   baseline   unlimited 1     125.7 s   80.4 s       667           0             82 MiB    4         6.0%
    100,000  1,000    50 ms   candidate  32        3     2,991.8 s 1.4 s        0             0             17 MiB    1         6.7%

The first-candidate time for the baseline comes from its own `first_candidate_ms` output. The eager baseline lists all 100,000 keys before it evaluates the first one. The candidate reaches its first candidate within the first page.

The 100K candidate row was recorded before the key-layout fix described in the ExecPlan. All of its orphans used the legacy layout. Candidate cost does not depend on the layout: each object is one listed key and two DELETE requests.

## Synthetic results

These runs use the real OpenDAL S3 client and request gate against a loopback listing of 1,000-key pages. Fake metadata protects every object except every 1,000th. S3 is at 32 requests/s and the cycle is uncapped.

    keys       injected faults     time      deleted  LIST  DELETE  retries  in flight  buffered  RSS before → peak
    100,000    none                13.2 s    100      100   200     0        1          64        10 → 17 MiB
    1,000,000  none                133.8 s   1,000    1,000 2,000   0        1          64        10 → 29 MiB
    1,000,000  503, every 20th     170.7 s   1,000    1,053 2,104   157      1          64        10 → 25 MiB
    1,000,000  429, every 20th     171.7 s   1,000    1,053 2,104   157      1          64        10 → 28 MiB

The RSS peaks include about 7 MiB that the fake metadata retains by recording every eligibility read.

## Reproducing

Build the release CLI and the test binary from the repository root, then run either benchmark. Each writes JSON to `target/janitor-benchmark/`.

    cargo build --release -p studiofs --bin studiofs --tests
    cargo test -p studiofs --bin studiofs --release janitor_synthetic_listing_benchmark -- --ignored --nocapture
    cargo test -p studiofs --bin studiofs --release live_cli_janitor_benchmark -- --ignored --nocapture

To run several live configurations at once, invoke the built test executable (`target/release/deps/studiofs-<hash>.exe live_cli_janitor_benchmark --ignored --nocapture`) instead of several `cargo test` processes. Concurrent `cargo test` processes can fail to relink an executable that is still running. Each live run starts its own Compose project, so parallel runs do not share state.

Synthetic knobs: `STUDIOFS_JANITOR_SYNTHETIC_KEYS` (default 1,000,000), `_ORPHAN_EVERY` (1,000), `_RPS` (32), `_FAULT_EVERY` (0 means no faults), and `_FAULT` (`503` by default, or `429`).

Live knobs, all prefixed `STUDIOFS_JANITOR_BENCH_`: `OBJECTS` (1,000), `ORPHAN_PERCENT` (50), `PG_LATENCY_MS` and `S3_LATENCY_MS` (100 and 20; the proxy adds half in each direction), `PG_CPUS` and `PG_MEMORY` (`1` and `512m`, applied with `docker update` after seeding), `RPS` (4), `MAX_DELETES` (500), `MAX_RUNS` (20), `FOREGROUND_MS` (250; 0 disables), `TIMEOUT_SECS` per run (3,600), `LABEL`, and `BASELINE_BINARY`.

To build the baseline:

    git worktree add --detach target/janitor-baseline/src 2c39e212
    cd target/janitor-baseline/src
    CARGO_TARGET_DIR=../target cargo build --release -p studiofs --bin studiofs

Then set `STUDIOFS_JANITOR_BENCH_BASELINE_BINARY` to `target/janitor-baseline/target/release/studiofs.exe`. Baseline runs are measured but not gated, because the baseline's losses are the result being recorded. Candidate runs fail if any referenced object is missing or the sweep does not finish.

## Measurement notes

- `docker stats` samples PostgreSQL CPU and memory about once per second, so short peaks can be missed. The in-test proxies count open connections, which are an upper bound on in-flight requests. The synthetic fixture counts in-flight requests exactly.
- Live S3 429/503 injection is not covered. A TCP proxy cannot fault single requests on pooled HTTP/1.1 connections.
- These runs covered only Windows 11 with Docker Desktop. macOS and Linux runs are still to do.

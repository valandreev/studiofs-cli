# SFS-665 macFUSE Interrupt Registry Benchmark

This record keeps historical mutex comparisons separate from the current
worker-spanned benchmark snapshot. Do not compare numbers across different
benchmark methods.

## Original Mutex and Default DashMap Comparison

Recorded 2026-09-27 before and after replacing the global mutex map. Environment:
macOS 26.6.2, arm64, Rust 1.98.1 (`aarch64-apple-darwin`), macFUSE 5.4.0, and
DashMap 6.1.0. Hardware model and logical CPU count were unavailable to the
sandbox.

Command:

    cargo test -p studiofs-macfuse --release --lib interrupt_registry_contention_benchmark -- --ignored --nocapture --test-threads=1

The original benchmark took the median of five samples. Each worker performed
25,000 insert/callback/remove cycles. It invoked the callback for every cycle
and stopped timing after joining the worker threads, so its elapsed time
included thread-exit and join latency.

| Workers | Mutex median cycles/s | Mutex elapsed | Default DashMap cycles/s | Default DashMap elapsed |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 4,040,023 | 6.188 ms | 3,165,091 | 7.899 ms |
| 2 | 4,266,485 | 11.719 ms | 5,036,916 | 9.927 ms |
| 4 | 3,095,201 | 32.308 ms | 8,214,058 | 12.174 ms |
| 8 | 2,999,378 | 66.681 ms | 9,538,060 | 20.969 ms |

This paired historical run recorded an eight-worker increase of 218.0% and a
one-worker decrease of 21.7%. The corrected benchmark below separates the
common registration/removal path from callback lookup and excludes worker
joins, so this table remains historical evidence rather than a direct baseline
for its newer measurements.

## Paired Two-barrier Mutex Baseline and Hasher Comparison

The paired comparison below used the two-barrier harness. It measures two
workloads: `register-remove` covers the usual request path, while
`register-callback-remove` invokes cancellation on every cycle to stress
callback lookup. Each worker performs 25,000 cycles per sample. The benchmark
warms both workloads once per worker count, takes seven samples, alternates
workload order, starts timing before the first barrier, and stops after every
worker reaches the completion barrier. Thread creation and joins are outside
the timed interval, but worker startup and scheduling while reaching the first
barrier are included. Each cycle also creates and clones a fresh
`CancellationToken`, so this is lifecycle cost rather than isolated map/hash
throughput.

The mutex variant used the registry and callback lock behavior from base commit
`8215f783`; the benchmark harness was otherwise identical. It ran three times.
Default-hasher DashMap ran three times, and `foldhash::fast::RandomState` ran
four times. The table compares the median run-level median throughput for each
variant; values are thousands of request lifecycles per second. These variants
are paired under the same method, but the included startup/scheduling jitter
made results vary under host scheduling, especially at two and four workers.
Treat this as historical evidence and do not compare it with the three-phase
snapshot below.

| Workers | Workload | Mutex | Default DashMap | Foldhash DashMap | Foldhash vs mutex |
| ---: |---|---:|---:|---:|---:|
| 1 | `register-remove` | 12,702 | 9,628 | 14,352 | +13.0% |
| 1 | `register-callback-remove` | 8,056 | 5,581 | 8,448 | +4.9% |
| 2 | `register-remove` | 6,616 | 10,678 | 12,149 | +83.6% |
| 2 | `register-callback-remove` | 4,844 | 4,719 | 5,126 | +5.8% |
| 4 | `register-remove` | 4,438 | 15,415 | 14,298 | +222.2% |
| 4 | `register-callback-remove` | 3,094 | 11,291 | 11,160 | +260.7% |
| 8 | `register-remove` | 3,934 | 15,332 | 15,689 | +298.8% |
| 8 | `register-callback-remove` | 2,918 | 11,503 | 13,059 | +350.8% |

The paired comparison shows the mutex serializing concurrent registration and
cleanup. At one worker, default DashMap was 24% slower than
the mutex for register/remove and 31% slower for register/callback/remove;
Foldhash was 13% and 5% faster, respectively. Foldhash improved these two
single-worker paths over default DashMap by about 49–51%. Multi-worker hasher
differences were smaller and mixed. This supports lower single-worker hashing
overhead without claiming that hashing explains all of the historical 21.7%
regression.

## Previous Three-phase Foldhash Snapshot

This earlier harness waited for every worker at a `ready` barrier before
starting the timer, then used separate `start` and `done` barriers. It excluded
thread creation and the initial readiness wait, but included the start and
completion barriers, worker scheduling during the measured work, and the fresh
token allocation in each lifecycle. Its values are retained as historical
evidence; workload names use the same hyphenated identifiers.

One release-mode run after this timing change produced these within-run median
values (each is the median of seven samples; the invocation was run once):

| Workers | Workload | Foldhash requests/s | Elapsed |
| ---: |---|---:|---:|
| 1 | `register-remove` | 11,876,722 | 2.105 ms |
| 1 | `register-callback-remove` | 6,564,264 | 3.808 ms |
| 2 | `register-remove` | 11,685,085 | 4.279 ms |
| 2 | `register-callback-remove` | 5,217,233 | 9.584 ms |
| 4 | `register-remove` | 14,150,108 | 7.067 ms |
| 4 | `register-callback-remove` | 10,281,014 | 9.727 ms |
| 8 | `register-remove` | 15,755,527 | 12.694 ms |
| 8 | `register-callback-remove` | 12,535,517 | 15.955 ms |

This Foldhash-only snapshot cannot be combined with earlier two-barrier results
because it has a different timer boundary. It remains an in-process synthetic
measurement, not mounted filesystem throughput. It is superseded by the
worker-spanned method below, which excludes barrier release time before actual
worker work begins.

## Worker-spanned Foldhash Snapshot

The harness waits for all workers at `ready` and `start` barriers. Each worker
records its own start immediately after the start barrier and its finish after
the request loop. A sample spans the earliest worker start through the latest
worker finish. This includes scheduling spread between workers while excluding
time before any worker starts, thread creation, and thread join/exit. The
completion boundary comes from worker timestamps, not another barrier. Each
lifecycle still creates and clones a fresh `CancellationToken`, so allocator
cost is included. The command was run three times after this timing correction.
Each cell below records that invocation's seven-sample median as
`requests/s / elapsed ms`.

Command:

    cargo test -p studiofs-macfuse --release --lib interrupt_registry_contention_benchmark -- --ignored --nocapture --test-threads=1

| Workers | Workload | Run 1 | Run 2 | Run 3 |
|---:|---|---:|---:|---:|
| 1 | `register-remove` | 9,842,520 / 2.540 ms | 15,150,753 / 1.650 ms | 14,641,649 / 1.707 ms |
| 1 | `register-callback-remove` | 5,941,418 / 4.208 ms | 8,661,884 / 2.886 ms | 8,288,668 / 3.016 ms |
| 2 | `register-remove` | 12,174,707 / 4.107 ms | 12,007,924 / 4.164 ms | 12,721,973 / 3.930 ms |
| 2 | `register-callback-remove` | 6,677,424 / 7.488 ms | 8,876,791 / 5.633 ms | 8,845,122 / 5.653 ms |
| 4 | `register-remove` | 14,722,662 / 6.792 ms | 17,464,961 / 5.726 ms | 15,225,143 / 6.568 ms |
| 4 | `register-callback-remove` | 11,666,967 / 8.571 ms | 12,746,092 / 7.846 ms | 12,202,004 / 8.195 ms |
| 8 | `register-remove` | 16,610,606 / 12.041 ms | 16,481,083 / 12.135 ms | 15,908,736 / 12.572 ms |
| 8 | `register-callback-remove` | 13,782,529 / 14.511 ms | 13,390,616 / 14.936 ms | 13,842,706 / 14.448 ms |

Run-to-run medians vary most at one worker: register/remove ranges from 9.84M
to 15.15M requests/s, and register/callback/remove from 5.94M to 8.66M. At
eight workers, the respective ranges are 15.91M–16.61M and 13.39M–13.84M.
This in-process synthetic measurement is not mounted filesystem throughput
and is not comparable with the earlier timer boundaries above.

## Native Mounted Validation

The registered `collision-overwrite-vs-read` scenario was run through the
macFUSE backend both before and after the hasher follow-up. The latest binary
was rebuilt with `cargo build -p studiofs --release` and run with:

    target/debug/studiofs-e2e run collision-overwrite-vs-read --studiofs-bin target/release/studiofs --platform macos-fuse --infra compose --retain always --run-id sfs665-foldhash-read-20260927 --seed 665

| Registry | Runs | Scenario duration | Retained report |
|---|---:|---:|---|
| Global mutex | 1 | 9,192 ms | `target/e2e-runs/sfs665-macos-read-baseline-20260927/report.json` |
| Default DashMap | 2 | 8,466 ms; 9,218 ms | `target/e2e-runs/sfs665-macos-read-20260927/report.json`, `target/e2e-runs/sfs665-macos-read-after-20260927/report.json` |
| Foldhash DashMap | 1 | 8,344 ms | `target/e2e-runs/sfs665-foldhash-read-20260927/report.json` |

Preflight, setup, scenario, validation, and cleanup passed in all runs, and no
mount remained afterward. These durations overlap; they do not establish an
end-to-end performance gain. The scenario confirms concurrent reads but does
not report how many interrupt callbacks fired.

# SFS-665 macFUSE Interrupt Registry Benchmark

This record keeps the original mutex comparison and separates it from the
follow-up benchmark, whose timing and workload were revised after code review.
Do not compare numbers across those two benchmark methods.

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

## Corrected Mutex Baseline and Hasher Comparison

After code review, the ignored benchmark was updated to measure two workloads:
`register-remove` covers the usual request path, while
`register-callback-remove` deliberately invokes cancellation on every cycle to
stress callback lookup. Each worker performs 25,000 cycles per sample. The
benchmark warms both workloads once per worker count, takes seven samples,
alternates workload order for each sample, starts timing before the start
barrier, and stops after every worker reaches the completion barrier. Thread
creation is outside the timed interval; joins happen after timing stops. The
two barriers remain part of the measurement.

The mutex variant used the registry and callback lock behavior from base commit
`8215f783`; the benchmark harness was otherwise identical. It ran three times.
Default-hasher DashMap ran three times, and `foldhash::fast::RandomState` ran
four times. The table compares the median run-level median throughput for each
variant; values are thousands of request lifecycles per second. Repeated
results varied under host scheduling, especially at two and four workers.

| Workers | Workload | Mutex | Default DashMap | Foldhash DashMap | Foldhash vs mutex |
| ---: |---|---:|---:|---:|---:|
| 1 | register/remove | 12,702 | 9,628 | 14,352 | +13.0% |
| 1 | register/callback/remove | 8,056 | 5,581 | 8,448 | +4.9% |
| 2 | register/remove | 6,616 | 10,678 | 12,149 | +83.6% |
| 2 | register/callback/remove | 4,844 | 4,719 | 5,126 | +5.8% |
| 4 | register/remove | 4,438 | 15,415 | 14,298 | +222.2% |
| 4 | register/callback/remove | 3,094 | 11,291 | 11,160 | +260.7% |
| 8 | register/remove | 3,934 | 15,332 | 15,689 | +298.8% |
| 8 | register/callback/remove | 2,918 | 11,503 | 13,059 | +350.8% |

The corrected comparison confirms that the mutex serializes concurrent
registration and cleanup. At one worker, default DashMap was 24% slower than
the mutex for register/remove and 31% slower for register/callback/remove;
Foldhash was 13% and 5% faster, respectively. Foldhash improved these two
single-worker paths over default DashMap by about 49–51%. Multi-worker hasher
differences were smaller and mixed. This supports lower single-worker hashing
overhead without claiming that hashing explains all of the historical 21.7%
regression.

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

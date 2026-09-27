# SFS-665 macFUSE Interrupt Registry Benchmark

This record measures the registry's per-read lifecycle: insert a cancellation
token, invoke the macFUSE interrupt callback with its opaque integer key, then
remove the entry. It isolates registry contention and cancellation from kernel,
storage, and mount costs; it is not a mounted-filesystem throughput claim.

## Baseline

Recorded 2026-09-27 before replacing the global mutex map. Environment: macOS
26.6.2, arm64, Rust 1.98.1 (`aarch64-apple-darwin`), macFUSE 5.4.0, and
DashMap 6.1.0 available in the workspace. Hardware model and logical CPU count
were unavailable to the sandbox.

Command:

    cargo test -p studiofs-macfuse --release --lib interrupt_registry_contention_benchmark -- --ignored --nocapture --test-threads=1

The benchmark takes the median of five samples. Each worker performs 25,000
register/callback/cancel/remove cycles per sample. Timing starts after worker
threads are created and waiting at a barrier.

| Workers | Median cycles/s | Median elapsed |
| ---: | ---: | ---: |
| 1 | 4,040,023 | 6.188 ms |
| 2 | 4,266,485 | 11.719 ms |
| 4 | 3,095,201 | 32.308 ms |
| 8 | 2,999,378 | 66.681 ms |

Aggregate throughput at eight workers is 29.7% below the two-worker result.

## Post-change

Recorded after replacing the global mutex map with the existing `DashMap`. The
command, worker counts, iterations, and five-sample median are unchanged.

| Workers | Median cycles/s | Median elapsed | Change vs baseline |
| ---: | ---: | ---: | ---: |
| 1 | 3,165,091 | 7.899 ms | -21.7% |
| 2 | 5,036,916 | 9.927 ms | +18.1% |
| 4 | 8,214,058 | 12.174 ms | +165.4% |
| 8 | 9,538,060 | 20.969 ms | +218.0% |

The single-worker path is slower, while eight-worker aggregate throughput is
3.18x the mutex baseline. This supports the change for the contended case; it
does not imply a mounted filesystem throughput gain.

## Native Mounted Validation

Built the product with `cargo build -p studiofs --release`, then ran the
registered concurrent read/overwrite scenario through the macFUSE backend:

    target/debug/studiofs-e2e run collision-overwrite-vs-read --studiofs-bin target/release/studiofs --platform macos-fuse --infra compose --retain always --run-id sfs665-macos-read-20260927 --seed 665

All runs used seed `665`, the `macos-fuse` backend, and the same registered
scenario. The clean-HEAD binary contains the original mutex; the rebuilt
product binary contains `DashMap`.

| Registry | Runs | Scenario duration | Retained report |
|---|---:|---:|---|
| Global mutex | 1 | 9,192 ms | `target/e2e-runs/sfs665-macos-read-baseline-20260927/report.json` |
| DashMap | 2 | 8,466 ms; 9,218 ms | `target/e2e-runs/sfs665-macos-read-20260927/report.json`, `target/e2e-runs/sfs665-macos-read-after-20260927/report.json` |

Preflight, setup, scenario, validation, and both cleanup phases passed in all
runs, and no mount remained afterward. The DashMap timings span the mutex
timing, so this scenario does not establish an end-to-end performance gain.
It validates native concurrent reads through the macFUSE adapter but does not
report how many interrupt callbacks fired, so it does not measure cancellation
frequency.

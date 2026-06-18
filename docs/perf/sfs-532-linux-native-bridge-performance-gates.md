# SFS-532 LinuxNativeBridge performance gates

Date: 2026-06-18

## Decision status

decision status: pending measured run.

SFS-532 is the rollout acceptance framework for `LinuxNativeBridge`. It
replaces the fuser-prototype decision shape from SFS-516 with same-host gates
that compare raw ext4, the fuser-era baseline, the native classic transport,
and the optional FUSE-over-io_uring transport tracked by SFS-533.

The outcome must be one explicit rollout decision:

- default NativeBridge;
- opt-in NativeBridge;
- io_uring profile decision: default, auto, opt-in only, unsupported, or not
  worth keeping;
- pause and redesign.

## One-command harness

The harness writes every generated artifact under:

```text
target/sfs-532-linux-native-bridge-performance-gates/
```

compact benchmark:

```bash
tools/perf/sfs532-linux-native-gates.sh compact
```

sustained benchmark:

```bash
tools/perf/sfs532-linux-native-gates.sh sustained
```

Focused documentation gate:

```bash
cargo test -p studiofs --test architecture_vocabulary_test linux_native_bridge_performance_gates_cover_sfs532_rollout_decision -- --nocapture
```

The script records `manifest.txt`, `summary.md`, and one status file per
executed row. It is safe for developer runs: privileged cache dropping and
`perf` collection are best-effort and must be recorded as unavailable instead
of making compact runs require root.

## Comparison rows

| Row | Purpose | Command source |
|---|---|---|
| raw ext4 buffered | Same-host filesystem ceiling with ordinary buffered I/O. | fio JSON emitted by the harness against a target raw ext4 directory. |
| raw ext4 direct | Same-host storage ceiling without OS page-cache effects. | fio JSON emitted by the harness with direct I/O when supported. |
| fuser-era baseline | Historical comparison against the SFS-516 fuser-era baseline. | Existing SFS-509/SFS-516 artifacts or a re-run of the current fuser mounted fio matrix. |
| LinuxNativeBridge classic conservative profile | Required baseline for the native bridge over classic `/dev/fuse`. | `native_mounted_scenario` profiles and the SFS-530 evidence matrix. |
| LinuxNativeBridge classic high-throughput profile | Native classic transport with async/writeback/readahead rollout knobs enabled. | SFS-530 native evidence profiles plus platform bridge mount env overrides. |
| LinuxNativeBridge io_uring profile | Optional FUSE-over-io_uring transport where the kernel advertises support. | SFS-533 smoke plus the same mounted fio matrix when registration succeeds. |
| JuiceFS reference | External Linux FUSE reference where available. | Operator-provided fio JSON copied into the SFS-532 artifact directory. |

## Workload matrix

Every measured row should include:

- sequential read/write;
- warm repeated read;
- random read/write;
- mixed read/write;
- multi-job read/write;
- fsync-heavy;
- metadata create/stat/readdir;
- large-file local-cache read.

Compact runs use small sizes and short runtime to prove the gate wiring.
Sustained runs increase file sizes, runtime, and metadata counts before a
rollout decision is accepted.

## Required metrics

Machine-readable summaries and the human `summary.md` must preserve:

- throughput;
- IOPS;
- latency with p95/p99 where fio or telemetry exposes it;
- CPU from `perf stat` when available;
- FUSE op amplification;
- queue wait;
- cache decisions;
- transport mode;
- io_uring fallback count;
- enabled kernel capabilities.

The gates are expressed as same-host ratio checks plus reference absolute
values. Hardware variance means a row can be `pass`, `fail`, or `investigate`;
`investigate` is the expected result when the host lacks enough capability data
or an external reference row is intentionally absent.

## Threshold policy

| Surface | Gate |
|---|---|
| Cold local-cache read | Target about 50-60% of same-host clean/raw disk where feasible; otherwise record the blocking layer. |
| Warm repeated read | Multi-GB/s class where supported, or a documented same-host ratio gate. |
| Sequential write | Pass/fail/investigate threshold against raw ext4 buffered and fuser-era baseline. |
| Random and mixed I/O | Pass/fail/investigate threshold against same-host raw and the fuser-era baseline. |
| fsync-heavy | Explicit tail-latency and timeout decision; high latency is not hidden by aggregate throughput. |
| Metadata create/stat/readdir | Operation rate and FUSE op amplification must be visible. |
| io_uring transport | The report must distinguish unsupported, disabled, fallback, measured, and failed registration. |

## Rollout knobs

The report must name the effective runtime settings for:

- transport mode `classic/io_uring/auto`;
- default profile;
- conservative fallback;
- direct I/O fallback;
- writeback-cache profile;
- readahead and prefetch limits;
- debug and telemetry flags.

## Final update requirement

Before closing SFS-532, copy the final recommendation into SFS-516 as a status
comment: default NativeBridge, opt-in NativeBridge, io_uring profile decision,
or pause and redesign, including the remaining bottlenecks and the artifact
paths used for evidence.

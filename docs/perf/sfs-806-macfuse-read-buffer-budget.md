# SFS-806 macFUSE Read-Buffer Budget

The macFUSE adapter retains at most 32 idle read buffers and 32 MiB of their
actual `Vec::capacity()` total, instead of the previous 256 MiB worst case.
Individual retained allocations remain limited to 8 MiB. The 64 MiB default
in-flight read admission budget is separate and unchanged.

Requests and initialized storage are grouped by their next power of two.
Acquisition selects the smallest adequate allocation in the same class. A
128 KiB read cannot borrow an idle 8 MiB buffer. Misses allocate exactly the
request length with `vec![0; len]`, without increasing fresh allocations beyond
the admitted request length. Reuse can lend larger initialized storage in the
same class, less than twice the request length. The 64 MiB admission limit
counts request bytes, not actual active buffer capacity or RSS. Pressure evicts the
largest idle allocation, preferring the oldest among equal capacities. The
small bounded vector keeps class lookup simple without a separate bucket map.

The existing overwritten-prefix, error, cancellation, and debug-poison contracts
remain in effect. Empty requests do not allocate or count as pool hits/misses.
The retained budget bounds idle allocations, not total process RSS: allocator
page retention, VFS caches, and active reads also contribute to RSS.

## Observability

`VfsWritePathObservabilitySnapshot.fuse_async_read` exports
`read_buffer_pool_hits`, `read_buffer_pool_misses`,
`read_buffer_pool_retained_bytes`, and `read_buffer_pool_budget_bytes`.
The mounted `.studiofs-runtime-stats` bridge section exports the same names with
a `fuse_` prefix, as counters for hits/misses and byte gauges for retained/budget.
Opening this file refreshes pool accounting after completed replies recycle
storage. Concurrent reads may change the pool after a snapshot is captured.
Legacy JSON snapshots default these new fields; unsupported platforms report no
pool budget. A nonempty pool miss allocates one buffer, so miss deltas provide
pool allocation counts; process allocator counters include other allocations.

## Focused validation

Run from the repository root:

    cargo test -p studiofs-macfuse --lib read_buffer
    cargo test -p studiofs-macfuse --lib read_only_adapter
    cargo test -p studiofs-macfuse --lib runtime_stats_open_stays_direct_and_preserves_macfuse_cache_json
    cargo test -p studiofs-vfs --lib fuse_async_read_snapshot_serializes_buffer_pool_metrics
    cargo clippy -p studiofs-macfuse -p studiofs-vfs --all-targets -- -D warnings
    cargo check -p studiofs-macfuse --release
    cargo fmt --all -- --check
    git diff --check

On 2026-10-14 the commands above passed: the pool filter ran 14 tests
(including two adapter reuse tests), the adapter filter ran 20 tests, and the
runtime stats and JSON compatibility filters each ran one test. Clippy,
release check, formatting and whitespace checks passed. Two adapter tests
appear in both filters, so these are 34 distinct tests. No full suite was run.

Unit tests exercise size matching, initialized reuse, empty/oversized requests,
actual-capacity pressure, and buffer-count limits. Adapter tests exercise queued
EOF, sparse, error and canceled replies. Runtime stats checks assert actual
pool counters and retained/budget gauges after a miss and a hit.

## Mounted acceptance still required

On 2026-10-14 the execution host had macFUSE installed but no active StudioFS
mount; the configured PostgreSQL and S3 endpoints were unreachable and the
OrbStack daemon was unavailable. No allocation-rate or RSS improvement is
claimed from unit tests.

With services restored, build and mount the changed product using the existing
volume configuration and the macFUSE backend. The placeholder below must be
replaced with that mount point:

    cargo build --release -p studiofs
    target/release/studiofs devbench <macFUSE-mount> --block-size 128KB --big-file-size 1GB --small-file-count 128 --threads 4 --json-output target/perf/sfs-806-128k.json
    target/release/studiofs devbench <macFUSE-mount> --block-size 4MB --big-file-size 1GB --small-file-count 128 --threads 4 --json-output target/perf/sfs-806-4m.json

The workload includes writes and upload drain, followed by read phases. Run the
same workload on the baseline and changed binary, restarting each mount, and
retain reports. Compare read-phase deltas of pool misses/hits and process
allocation counts, plus sampled process RSS. Confirm the retained gauge never
exceeds 33,554,432 bytes and that repeated geometry yields hits after warm-up.
A small-read phase after large reads must leave large buffers idle rather than
consume them. Kernel-cached reads may bypass the pool: inspect callback and pool
counter deltas before interpreting allocation rates. The idle budget is a code
invariant; an RSS reduction still requires mounted measurements.

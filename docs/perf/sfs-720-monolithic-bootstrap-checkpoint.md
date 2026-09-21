# SFS-720 monolithic bootstrap checkpoint scalability

Status: measured through 100,000 flat namespace entries in release mode;
monolithic checkpoint retained for the tested workload.

## Workload and measurement path

The ignored `studiofs-meta` benchmark builds progressively larger flat live
namespaces. Each entry is a committed regular file with a full published
payload reference, so the checkpoint includes inode, dentry, and regular-file
base rows. The fixture is seeded through batched metadata operations; the
measurements use the production path:

1. `RedbMetaEngine::export_serving_checkpoint_bytes` measures checkpoint
   construction and postcard serialization.
2. The bytes are placed in PostgreSQL `metadata_state.snapshot`.
3. `PgSnapshotBackend::read_bootstrap_checkpoint` measures the PostgreSQL
   descriptor query, checksum/format validation, and checkpoint decode.
4. A fresh `RedbMetaEngine` imports the bytes through
   `import_serving_checkpoint_bytes`.

The PostgreSQL descriptor path validates and decodes its checkpoint, while
`import_serving_checkpoint_bytes` accepts serialized bytes and decodes them
again. This is the current production boundary: the two timings are separate
stage measurements, not additive end-to-end latency, and `local_import`
includes its own checkpoint decode.

The benchmark records checkpoint bytes, export/serialization time, PostgreSQL
read/decode time, local import time, and process RSS in
`target/perf/sfs-720/checkpoint-scalability.jsonl`. RSS is reported as
`rss_baseline_bytes`, `rss_peak_bytes`, and
`rss_delta_bytes = max(peak - baseline, 0)`; the baseline is sampled after
fixture and PostgreSQL setup and before the measured loop. Fixture seeding is
outside the timed phases, and its resident pages remain visible in the
baseline rather than being attributed to the measured delta.

## Commands and evidence

The portable fixture proof and helper regressions passed:

```text
cargo test -p studiofs-meta sfs720_ --lib -- --nocapture
3 passed; 0 failed; 1 ignored; 752 filtered out
```

The strict static checks passed:

```text
cargo clippy -p studiofs-meta --all-targets -- -D warnings
cargo fmt --all -- --check
```

The first reduced live attempt was made while the local container runtime was
stopped and failed before provisioning PostgreSQL. After the runtime was
started and the configured endpoint became reachable, the acceptance run was:

```text
SFS720_NAMESPACE_SIZES=1000,10000,50000,100000 SFS720_SAMPLES=3 \
  cargo test --release -p studiofs-meta sfs720_checkpoint_scalability --lib -- \
  --ignored --nocapture --test-threads=1
1 passed; 0 failed; 755 filtered out; finished in 1200.31s
```

The run produced four complete JSONL records with three samples each:

| Namespace | Checkpoint | Export median | PG read/decode median | Local import median | RSS baseline | RSS peak | RSS delta |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.12 MiB | 7.30 ms | 67.24 ms | 16.48 ms | 18.36 MiB | 21.05 MiB | 2.69 MiB |
| 10,000 | 1.19 MiB | 30.84 ms | 139.52 ms | 90.37 ms | 60.64 MiB | 77.22 MiB | 16.58 MiB |
| 50,000 | 6.09 MiB | 157.57 ms | 1,869.23 ms | 652.67 ms | 231.52 MiB | 288.81 MiB | 57.30 MiB |
| 100,000 | 12.24 MiB | 320.62 ms | 2,167.56 ms | 2,788.55 ms | 432.25 MiB | 558.17 MiB | 125.92 MiB |

The RSS values are process-level observations for the measured size; they are
not a low-resource host acceptance gate. The configured PostgreSQL service was
available for this run, and all export, descriptor-read/decode, and local
import samples completed successfully.

## Ceiling and decision

The measured ceiling for this workload is 100,000 entries: it has a complete
three-sample JSONL record and stable successful export, PostgreSQL read/decode,
and local-import samples. The monolithic checkpoint remains the current
implementation; these results do not justify sharding within the tested
range. A future sharding follow-up still needs a versioned, checksummed,
atomically published artifact layout that preserves the existing
checkpoint/replay cut contract.

The flat namespace is an explicit workload boundary: passing it does not
claim equivalent performance for deeply nested trees, large xattr/ACL sets, or
payload graphs with many slices per file.

## Rerun

With PostgreSQL available and `STUDIOFS_TEST_PG_DSN` configured without
printing its value:

```text
SFS720_NAMESPACE_SIZES=1000,10000,50000,100000 SFS720_SAMPLES=3 \
  cargo test --release -p studiofs-meta sfs720_checkpoint_scalability --lib -- \
  --ignored --nocapture --test-threads=1
```

Use `SFS720_NAMESPACE_SIZES` to narrow or extend the progressive sizes and
inspect the generated JSONL before recording the largest successful point.

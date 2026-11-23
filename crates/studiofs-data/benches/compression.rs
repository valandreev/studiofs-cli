//! Criterion benchmarks for STFS v3 Zstd compression and entropy probe (SFS-1089).

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use std::hint::black_box;
use studiofs_data::compression::{EncodedFragment, compress_fragment_blocking};
use studiofs_data::crypto::{DEFAULT_ENCRYPTION_FRAME_SIZE, ObjectCipher, VolumeMasterKey};
use studiofs_data::object::{CLOUD_FRAGMENT_SIZE, create_object_buffer};
use studiofs_meta::trash::CompressionPolicy;

fn pseudo_random_bytes(len: usize, mut seed: u64) -> Bytes {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let take = (len - out.len()).min(8);
        out.extend_from_slice(&z.to_le_bytes()[..take]);
    }
    Bytes::from(out)
}

fn bench_random_4mib_probe_vs_off(c: &mut Criterion) {
    let payload = pseudo_random_bytes(CLOUD_FRAGMENT_SIZE, 0xDEAD_BEEF_4242);
    let master_key = VolumeMasterKey::from_bytes([0x42; 32]);
    let cipher = ObjectCipher::new(&master_key, "1a4c0fe2-8550-4892-952c-5ec26c0e0d8d").unwrap();
    let storage_key = "vol/objects/0000/0000000000000001/0";
    let workers = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(4);

    let mut group = c.benchmark_group("random_4mib_prepare");
    group.throughput(Throughput::Bytes(CLOUD_FRAGMENT_SIZE as u64));

    group.bench_function(BenchmarkId::new("policy", "off"), |b| {
        b.iter(|| {
            let obj = create_object_buffer(black_box(payload.clone())).unwrap();
            let sealed = cipher
                .encrypt_object(
                    obj,
                    storage_key,
                    1,
                    DEFAULT_ENCRYPTION_FRAME_SIZE as u32,
                    workers,
                )
                .unwrap();
            black_box(sealed)
        });
    });

    group.bench_function(BenchmarkId::new("policy", "zstd3_probe_raw"), |b| {
        let policy = CompressionPolicy::zstd_default();
        b.iter(|| {
            let decision =
                compress_fragment_blocking(black_box(payload.clone()), policy, workers).unwrap();
            let EncodedFragment::StoreRaw { payload: raw, .. } = decision.fragment else {
                panic!("random data must take raw path");
            };
            let obj = create_object_buffer(raw).unwrap();
            let sealed = cipher
                .encrypt_object(
                    obj,
                    storage_key,
                    1,
                    DEFAULT_ENCRYPTION_FRAME_SIZE as u32,
                    workers,
                )
                .unwrap();
            black_box(sealed)
        });
    });

    group.bench_function(BenchmarkId::new("probe_only", "zstd3_random_4mib"), |b| {
        let policy = CompressionPolicy::zstd_default();
        b.iter(|| {
            let decision =
                compress_fragment_blocking(black_box(payload.clone()), policy, workers).unwrap();
            black_box(decision)
        });
    });

    group.finish();
}

criterion_group!(benches, bench_random_4mib_probe_vs_off);
fn main() {
    // `cargo test --all-targets -- --test-threads=1` forwards libtest flags that
    // Criterion's CLI rejects; skip instead of failing the whole test run.
    if std::env::args().any(|arg| arg.starts_with("--test-threads")) {
        eprintln!("compression bench: skipped under libtest --test-threads; run `cargo bench`");
        return;
    }
    benches();
    Criterion::default().configure_from_args().final_summary();
}

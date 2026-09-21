use super::*;
use crate::engine::NamespaceMutation;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use studiofs_test_support::workspace_target_tempdir;

struct Sfs720CheckpointFixture {
    _dir: tempfile::TempDir,
    source: RedbMetaEngine,
    checkpoint_bytes: Vec<u8>,
}

struct RssSampler {
    stop: std::sync::Arc<AtomicBool>,
    baseline: u64,
    peak: std::sync::Arc<AtomicU64>,
    join: Option<JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Option<Self> {
        let pid = sysinfo::get_current_pid().ok()?;
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let peak = std::sync::Arc::new(AtomicU64::new(0));
        let mut initial_system = sysinfo::System::new();
        let initial_pids = [pid];
        let _ =
            initial_system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&initial_pids), true);
        if let Some(process) = initial_system.process(pid) {
            peak.store(process.memory(), Ordering::Relaxed);
        }
        let baseline = peak.load(Ordering::Relaxed);
        let thread_stop = std::sync::Arc::clone(&stop);
        let thread_peak = std::sync::Arc::clone(&peak);
        let join = std::thread::spawn(move || {
            let mut system = initial_system;
            while !thread_stop.load(Ordering::Relaxed) {
                let pids = [pid];
                let _ = system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&pids), true);
                if let Some(process) = system.process(pid) {
                    thread_peak.fetch_max(process.memory(), Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        Some(Self {
            stop,
            baseline,
            peak,
            join: Some(join),
        })
    }

    fn finish(mut self) -> (u64, u64) {
        self.stop.store(true, Ordering::Relaxed);
        self.join
            .take()
            .expect("RSS sampler join handle")
            .join()
            .expect("RSS sampler thread");
        (self.baseline, self.peak.load(Ordering::Relaxed))
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn build_sfs720_checkpoint_fixture(namespace_entries: usize) -> Sfs720CheckpointFixture {
    let dir = workspace_target_tempdir("sfs720-checkpoint-");
    let source = RedbMetaEngine::open(dir.path().join("meta.redb")).expect("open source redb");
    source
        .set_format_info(&format_info(
            "sfs720-source",
            "sfs720-source-uuid",
            "test-postgres-dsn",
            1,
        ))
        .expect("set source format");
    source.init_root(0, 0).expect("initialize source root");

    const FIXTURE_BATCH_SIZE: usize = 10_000;
    for chunk_start in (0..namespace_entries).step_by(FIXTURE_BATCH_SIZE) {
        let chunk_end = chunk_start
            .saturating_add(FIXTURE_BATCH_SIZE)
            .min(namespace_entries);
        let mut mutations = Vec::with_capacity(chunk_end - chunk_start);
        let mut serving_events = Vec::with_capacity(chunk_end - chunk_start);
        let mut publish_events = Vec::with_capacity(chunk_end - chunk_start);
        for index in chunk_start..chunk_end {
            let ino = ROOT_INO + 1 + u64::try_from(index).expect("namespace index fits in inode");
            let mut attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);
            attr.size = u64::from(DEFAULT_BLOCK_SIZE);
            attr.committed_size = attr.size;
            attr.content_epoch = 1;
            attr.committed_content_epoch = 1;
            attr.upload_state = UploadState::Ready;
            let payload = PayloadRef::new_full(
                1_000_000 + u64::try_from(index).expect("namespace index fits in object id"),
                DEFAULT_BLOCK_SIZE,
            );
            mutations.push(NamespaceMutation::CreateFile {
                ino,
                parent: ROOT_INO,
                name: format!("sfs720-{index:08}.bin").into_bytes(),
                uid: 1000,
                gid: 1000,
                mode: 0o644,
            });
            serving_events.push(MetaEvent::SetAttr { attr });
            publish_events.push(MetaEvent::PublishRegularFileBaseSlices {
                ino,
                chunk_slices: fixture_payload_slices(0, Some(&payload), PayloadRefState::Published)
                    .expect("build source payload slices"),
            });
        }
        source
            .apply_namespace_mutations(&mutations)
            .expect("apply source namespace state");
        source
            .apply_replica_serving_state(&serving_events)
            .expect("apply source serving state");
        source
            .apply_replica_events_to_base(&publish_events)
            .expect("apply source base state");
    }
    let checkpoint_bytes = source
        .export_serving_checkpoint_bytes()
        .expect("export serving checkpoint");

    Sfs720CheckpointFixture {
        _dir: dir,
        source,
        checkpoint_bytes,
    }
}

fn parse_sfs720_namespace_sizes(raw: &str) -> Vec<usize> {
    let sizes = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<usize>()
                .expect("SFS720_NAMESPACE_SIZES must be comma-separated integers")
        })
        .collect::<Vec<_>>();
    assert!(
        !sizes.is_empty() && sizes.iter().all(|size| *size > 0),
        "SFS720_NAMESPACE_SIZES must contain positive sizes"
    );
    sizes
}

fn sfs720_namespace_sizes() -> Vec<usize> {
    let raw = std::env::var("SFS720_NAMESPACE_SIZES")
        .unwrap_or_else(|_| "1000,10000,50000,100000".to_string());
    parse_sfs720_namespace_sizes(&raw)
}

fn sfs720_samples() -> usize {
    let samples = std::env::var("SFS720_SAMPLES")
        .unwrap_or_else(|_| "3".to_string())
        .parse::<usize>()
        .expect("SFS720_SAMPLES must be a positive integer");
    assert!(samples > 0, "SFS720_SAMPLES must be positive");
    samples
}

fn publish_sfs720_checkpoint(
    dsn: &str,
    volume_id: i64,
    checkpoint_version: i64,
    checkpoint_bytes: &[u8],
) {
    let checkpoint_sha256 = Sha256::digest(checkpoint_bytes);
    run_async(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| MetaError::Storage(format!("postgres connect failed: {error}")))?;
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let rows_affected = client
            .execute(
                "UPDATE metadata_state SET version = $1, checkpoint_version = $1, snapshot = $2, checkpoint_sha256 = $3, checkpoint_publication_state = 'published', checkpoint_generation = checkpoint_generation + 1 WHERE vol = $4",
                &[
                    &checkpoint_version,
                    &checkpoint_bytes,
                    &&checkpoint_sha256[..],
                    &volume_id,
                ],
            )
            .await
            .map_err(|error| MetaError::Storage(format!("postgres checkpoint seed failed: {error}")));
        connection_task.abort();
        let rows_affected = rows_affected?;
        if rows_affected != 1 {
            return Err(MetaError::Storage(format!(
                "expected one metadata_state row for volume {volume_id}, affected {rows_affected}"
            )));
        }
        Ok::<(), MetaError>(())
    })
    .expect("publish SFS-720 checkpoint");
}

fn summarize_millis(samples: &[f64]) -> serde_json::Value {
    if samples.is_empty() {
        return serde_json::json!({
            "min_ms": 0.0,
            "median_ms": 0.0,
            "max_ms": 0.0,
            "samples_ms": samples,
        });
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let len = sorted.len();
    let median = if len.is_multiple_of(2) {
        (sorted[len / 2 - 1] + sorted[len / 2]) / 2.0
    } else {
        sorted[len / 2]
    };
    serde_json::json!({
        "min_ms": sorted[0],
        "median_ms": median,
        "max_ms": sorted[len - 1],
        "samples_ms": samples,
    })
}

#[test]
fn sfs720_checkpoint_fixture_contains_requested_live_namespace() {
    let fixture = build_sfs720_checkpoint_fixture(32);
    let checkpoint: crate::snapshot::ServingCheckpoint =
        postcard::from_bytes(&fixture.checkpoint_bytes).expect("decode fixture checkpoint");

    assert_eq!(checkpoint.inodes.len(), 33);
    assert_eq!(checkpoint.dentries.len(), 32);
    assert_eq!(checkpoint.regular_file_base.len(), 32);
    assert!(!fixture.checkpoint_bytes.is_empty());
}

#[test]
fn sfs720_namespace_sizes_ignore_empty_tokens() {
    assert_eq!(
        parse_sfs720_namespace_sizes("1000, 10000, "),
        vec![1000, 10000]
    );
}

#[test]
fn sfs720_summarize_millis_handles_even_and_empty_samples() {
    let summary = summarize_millis(&[9.0, 1.0, 3.0, 7.0]);
    assert_eq!(summary["median_ms"], serde_json::json!(5.0));

    let empty = summarize_millis(&[]);
    assert_eq!(empty["min_ms"], serde_json::json!(0.0));
    assert_eq!(empty["median_ms"], serde_json::json!(0.0));
    assert_eq!(empty["max_ms"], serde_json::json!(0.0));
}

#[test]
#[ignore = "requires PostgreSQL and intentionally exercises progressively larger namespaces"]
fn sfs720_checkpoint_scalability() {
    let dsn = pg_dsn().expect("SFS-720 benchmark requires STUDIOFS_TEST_PG_DSN");
    let sizes = sfs720_namespace_sizes();
    let samples = sfs720_samples();
    let output_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/perf/sfs-720/checkpoint-scalability.jsonl");
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).expect("create SFS-720 output directory");
    }
    let mut output = File::create(&output_path).expect("create SFS-720 output file");

    for namespace_entries in sizes {
        let fixture = build_sfs720_checkpoint_fixture(namespace_entries);
        let volume = PgTestVolume::new(&dsn, &format!("sfs720-{namespace_entries}"));
        publish_sfs720_checkpoint(
            &dsn,
            volume.volume_id,
            i64::try_from(namespace_entries).expect("namespace size fits in checkpoint version"),
            &fixture.checkpoint_bytes,
        );
        let snapshot = PgSnapshotBackend::open(&dsn, volume.volume_id)
            .expect("open SFS-720 PostgreSQL snapshot backend");
        let rss_before = RssSampler::start();
        let mut export_millis = Vec::with_capacity(samples);
        let mut postgres_millis = Vec::with_capacity(samples);
        let mut import_millis = Vec::with_capacity(samples);

        for _ in 0..samples {
            let started = Instant::now();
            let checkpoint_bytes = fixture
                .source
                .export_serving_checkpoint_bytes()
                .expect("export SFS-720 serving checkpoint");
            export_millis.push(started.elapsed().as_secs_f64() * 1_000.0);
            assert_eq!(
                checkpoint_bytes, fixture.checkpoint_bytes,
                "source checkpoint must remain stable between benchmark samples"
            );

            let started = Instant::now();
            let descriptor = snapshot
                .read_bootstrap_checkpoint()
                .expect("read SFS-720 PostgreSQL bootstrap checkpoint");
            postgres_millis.push(started.elapsed().as_secs_f64() * 1_000.0);
            assert_eq!(descriptor.checkpoint_bytes, fixture.checkpoint_bytes);

            let import_dir = workspace_target_tempdir("sfs720-import-");
            let target = RedbMetaEngine::open(import_dir.path().join("meta.redb"))
                .expect("open SFS-720 import redb");
            let started = Instant::now();
            target
                .import_serving_checkpoint_bytes(&descriptor.checkpoint_bytes)
                .expect("import SFS-720 serving checkpoint");
            import_millis.push(started.elapsed().as_secs_f64() * 1_000.0);
        }

        let rss_measurement = rss_before.map(RssSampler::finish);
        let record = serde_json::json!({
            "issue": "SFS-720",
            "namespace_entries": namespace_entries,
            "checkpoint_bytes": fixture.checkpoint_bytes.len(),
            "checkpoint_mib": fixture.checkpoint_bytes.len() as f64 / (1024.0 * 1024.0),
            "samples": samples,
            "export_serialization": summarize_millis(&export_millis),
            "postgres_bootstrap_read_decode": summarize_millis(&postgres_millis),
            "local_import": summarize_millis(&import_millis),
            "rss_baseline_bytes": rss_measurement.map(|(baseline, _)| baseline),
            "rss_peak_bytes": rss_measurement.map(|(_, peak)| peak),
            "rss_delta_bytes": rss_measurement
                .map(|(baseline, peak)| peak.saturating_sub(baseline)),
        });
        writeln!(output, "{record}").expect("write SFS-720 result");
        output.flush().expect("flush SFS-720 result");
        eprintln!("{record}");
    }
}

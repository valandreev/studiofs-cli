//! `studiofs test-cache` — benchmark cache performance.
//!
//! Supports:
//! - direct L2 benchmark mode (`direct-l2`) that bypasses VFS
//! - end-to-end benchmark mode (`e2e`) that runs copy/read on a mounted volume

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::Serialize;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use studiofs_cache::block_cache::{DEFAULT_L1_SIZE, DEFAULT_L2_SIZE};
use studiofs_cache::metrics::CachePipelineMetricsSnapshot;
use studiofs_cache::{BlockCache, BlockCacheConfig};
use studiofs_vfs::parse_size_bytes;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

const DEFAULT_DIRECT_TOTAL_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_E2E_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_LATENCY_SAMPLES: usize = 200_000;
const BINARY_UNIT_DIVISOR: f64 = 1024.0;

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
pub enum TestCacheAccessPattern {
    /// Sequential file/object order.
    Sequential,
    /// Fully random file/object order.
    Random,
    /// Mostly sequential with random local swaps.
    Mixed,
}

#[derive(Debug)]
struct TestCacheParams {
    target: PathBuf,
    block_size: u64,
    read_size: u64,
    concurrency: usize,
    io_depth: usize,
    access_pattern: TestCacheAccessPattern,
    small_file_share: u8,
    l1_limit: u64,
    l2_limit: u64,
}

#[derive(Clone, Debug)]
struct ActiveMount {
    mount_point: PathBuf,
    volume: String,
}

#[derive(Clone, Debug)]
enum ResolvedRunMode {
    DirectL2 {
        cache_dir: PathBuf,
    },
    MountedE2E {
        volume: String,
        mount_point: PathBuf,
        work_root: PathBuf,
    },
}

#[derive(Debug, Serialize)]
struct OpReport {
    operations: u64,
    bytes: u64,
    elapsed_ms: f64,
    avg_latency_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    throughput_bytes_per_sec: f64,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    mode: String,
    cache_dir: String,
    started_at: String,
    completed_at: String,
    total_elapsed_ms: f64,
    block_size: u64,
    read_size: u64,
    concurrency: usize,
    io_depth: usize,
    access_pattern: String,
    small_file_share: u8,
    l1_limit: u64,
    l2_limit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    work_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mount_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    l1_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    l2_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_pipeline: Option<CachePipelineReport>,
    writes: OpReport,
    reads: OpReport,
}

#[derive(Debug, Serialize)]
struct LatencyHistogramReport {
    bucket_upper_bounds_ns: Vec<u64>,
    bucket_counts: Vec<u64>,
}

#[derive(Debug, Serialize)]
struct IoStatsReport {
    ops: u64,
    bytes: u64,
    total_latency_ns: u64,
    max_latency_ns: u64,
    latency_histogram: LatencyHistogramReport,
}

#[derive(Debug, Serialize)]
struct MaterializeBatchStatsReport {
    batches: u64,
    blocks: u64,
    bytes: u64,
    inflight_batches: usize,
    inflight_batches_peak: usize,
    duration_histogram: LatencyHistogramReport,
}

#[derive(Debug, Serialize)]
struct UploadQueueStatsReport {
    pending_depth: usize,
    pending_depth_peak: usize,
    submitted: u64,
    completed: u64,
    requeued: u64,
    canceled: u64,
}

#[derive(Debug, Serialize)]
struct CachePipelineReport {
    l2_read: IoStatsReport,
    l2_write: IoStatsReport,
    materialize_batches: MaterializeBatchStatsReport,
    upload_queue: UploadQueueStatsReport,
}

impl From<studiofs_cache::metrics::LatencyHistogramSnapshot> for LatencyHistogramReport {
    fn from(snapshot: studiofs_cache::metrics::LatencyHistogramSnapshot) -> Self {
        Self {
            bucket_upper_bounds_ns: snapshot.bucket_upper_bounds_ns,
            bucket_counts: snapshot.bucket_counts,
        }
    }
}

impl From<studiofs_cache::metrics::IoStatsSnapshot> for IoStatsReport {
    fn from(snapshot: studiofs_cache::metrics::IoStatsSnapshot) -> Self {
        Self {
            ops: snapshot.ops,
            bytes: snapshot.bytes,
            total_latency_ns: snapshot.total_latency_ns,
            max_latency_ns: snapshot.max_latency_ns,
            latency_histogram: snapshot.latency_histogram.into(),
        }
    }
}

impl From<studiofs_cache::metrics::MaterializeBatchStatsSnapshot> for MaterializeBatchStatsReport {
    fn from(snapshot: studiofs_cache::metrics::MaterializeBatchStatsSnapshot) -> Self {
        Self {
            batches: snapshot.batches,
            blocks: snapshot.blocks,
            bytes: snapshot.bytes,
            inflight_batches: snapshot.inflight_batches,
            inflight_batches_peak: snapshot.inflight_batches_peak,
            duration_histogram: snapshot.duration_histogram.into(),
        }
    }
}

impl From<studiofs_cache::metrics::UploadQueueStatsSnapshot> for UploadQueueStatsReport {
    fn from(snapshot: studiofs_cache::metrics::UploadQueueStatsSnapshot) -> Self {
        Self {
            pending_depth: snapshot.pending_depth,
            pending_depth_peak: snapshot.pending_depth_peak,
            submitted: snapshot.submitted,
            completed: snapshot.completed,
            requeued: snapshot.requeued,
            canceled: snapshot.canceled,
        }
    }
}

impl From<CachePipelineMetricsSnapshot> for CachePipelineReport {
    fn from(snapshot: CachePipelineMetricsSnapshot) -> Self {
        Self {
            l2_read: snapshot.l2_read.into(),
            l2_write: snapshot.l2_write.into(),
            materialize_batches: snapshot.materialize_batches.into(),
            upload_queue: snapshot.upload_queue.into(),
        }
    }
}

#[derive(Default)]
struct LatencySamples {
    samples_ns: Vec<u64>,
    total_ops: u64,
    total_bytes: u64,
    total_latency_ns: u128,
    sample_keep_every: u64,
}

impl LatencySamples {
    fn record(&mut self, elapsed: Duration, bytes: u64) {
        let ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.total_latency_ns = self.total_latency_ns.saturating_add(ns as u128);
        self.total_ops = self.total_ops.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(bytes);

        // Keep percentile estimation memory-bounded for long benchmark runs.
        let keep_every = self.sample_keep_every.max(1);
        if (self.total_ops - 1).is_multiple_of(keep_every) {
            self.samples_ns.push(ns);
        }

        if self.samples_ns.len() > MAX_LATENCY_SAMPLES {
            self.samples_ns = self.samples_ns.iter().step_by(2).copied().collect();
            self.sample_keep_every = keep_every.saturating_mul(2);
        }
    }

    fn total_duration_ms(&self) -> f64 {
        self.total_latency_ns as f64 / 1_000_000.0
    }

    fn avg_ns(&self) -> f64 {
        if self.total_ops == 0 {
            return 0.0;
        }
        self.total_latency_ns as f64 / self.total_ops as f64
    }

    fn percentile_ns(&self, p: f64) -> u64 {
        if self.samples_ns.is_empty() {
            return 0;
        }
        let mut samples = self.samples_ns.clone();
        samples.sort_unstable();
        let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
        samples[idx.min(samples.len().saturating_sub(1))]
    }

    fn to_report(&self, wall_elapsed_ms: f64) -> OpReport {
        let total_ms = self.total_duration_ms();
        let avg_ms = self.avg_ns() / 1_000_000.0;
        let p50_ms = self.percentile_ns(0.50) as f64 / 1_000_000.0;
        let p95_ms = self.percentile_ns(0.95) as f64 / 1_000_000.0;
        let p99_ms = self.percentile_ns(0.99) as f64 / 1_000_000.0;

        let throughput_bytes_per_sec = if wall_elapsed_ms <= 0.0 {
            0.0
        } else {
            self.total_bytes as f64 / (wall_elapsed_ms / 1000.0)
        };

        OpReport {
            operations: self.total_ops,
            bytes: self.total_bytes,
            elapsed_ms: total_ms,
            avg_latency_ms: avg_ms,
            p50_ms,
            p95_ms,
            p99_ms,
            throughput_bytes_per_sec,
        }
    }
}

#[derive(Clone, Debug)]
struct WorkFile {
    path: PathBuf,
    size: u64,
    index: usize,
}

#[derive(Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9e3779b97f4a7c15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 7;
        x ^= x >> 9;
        x ^= x << 8;
        self.state = x;
        x
    }

    fn next_usize(&mut self, max_exclusive: usize) -> usize {
        if max_exclusive <= 1 {
            return 0;
        }
        (self.next_u64() % max_exclusive as u64) as usize
    }

    fn next_bool_percent(&mut self, threshold_percent: u8) -> bool {
        (self.next_u64() % 100) < threshold_percent as u64
    }
}

fn parse_size_arg(label: &str, raw: &str) -> Result<u64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(anyhow!("{label} is empty"));
    }

    parse_size_bytes(raw)
        .map(|value| value as u64)
        .or_else(|| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .with_context(|| {
            format!("invalid {label} '{raw}', expected value like 4KB, 2MB, 1GB or integer bytes")
        })
}

fn build_access_sequence(len: usize, pattern: TestCacheAccessPattern, seed: u64) -> Vec<usize> {
    let mut order: Vec<usize> = (0..len).collect();
    if len <= 1 {
        return order;
    }
    let mut rng = XorShift64::new(seed);

    match pattern {
        TestCacheAccessPattern::Sequential => {}
        TestCacheAccessPattern::Random => {
            for i in (1..order.len()).rev() {
                let j = rng.next_usize(i + 1);
                order.swap(i, j);
            }
        }
        TestCacheAccessPattern::Mixed => {
            let swaps = (order.len() / 4).max(1);
            for _ in 0..swaps {
                let a = rng.next_usize(order.len());
                let b = rng.next_usize(order.len());
                order.swap(a, b);
            }
        }
    }

    order
}

fn build_work_sizes(
    total_bytes: u64,
    block_size: u64,
    read_size: u64,
    small_file_share: u8,
) -> Vec<u64> {
    let mut rng = XorShift64::new(0x5ed0_f00d_beef);
    let mut remaining = total_bytes;
    let mut sizes = Vec::new();
    let small_size = read_size.max(4 * 1024).min(block_size);

    if remaining == 0 {
        return Vec::new();
    }

    while remaining > 0 {
        let mut next = if rng.next_bool_percent(small_file_share) {
            small_size
        } else {
            block_size
        };
        if next == 0 {
            next = block_size.max(1);
        }
        let take = next.min(remaining);
        sizes.push(take);
        remaining -= take;
    }

    sizes
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = XorShift64::new(seed ^ 0x6ac9_fe0f);
    let mut out = vec![0u8; len];
    for byte in &mut out {
        *byte = rng.next_u64() as u8;
    }
    out
}

fn build_file_worklist(
    total_bytes: u64,
    block_size: u64,
    read_size: u64,
    small_file_share: u8,
    mount_root: &Path,
) -> Vec<WorkFile> {
    let sizes = build_work_sizes(total_bytes, block_size, read_size, small_file_share);
    sizes
        .into_iter()
        .enumerate()
        .map(|(idx, size)| WorkFile {
            path: mount_root.join(format!("test_cache_file_{idx:06}.bin")),
            size,
            index: idx,
        })
        .collect()
}

async fn run_l2_writes(
    cache: Arc<BlockCache>,
    sizes: &[u64],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
) -> Result<LatencySamples> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(LatencySamples::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let cache = Arc::clone(&cache);
        let order = order.to_vec();
        let sizes = sizes.to_vec();
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("L2 write semaphore closed: {e}"))?;
                let _permit = permit;

                let data_len =
                    usize::try_from(sizes[work_index]).context("L2 write size overflow")?;
                let object_id = (work_index as u64) + 1;
                let data = deterministic_bytes(
                    data_len,
                    object_id
                        .saturating_mul(31)
                        .saturating_add(worker_id as u64),
                );

                let start = Instant::now();
                cache
                    .put(object_id, Bytes::from(data))
                    .await
                    .context("L2 put failed")?;
                let elapsed = start.elapsed();

                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("L2 write latency stats lock poisoned"))?;
                s.record(elapsed, sizes[work_index]);
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .context("L2 write worker panicked")?
            .context("L2 write worker failed")?;
    }

    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("L2 write worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("L2 write latency stats lock poisoned"))
}

async fn run_l2_reads(
    cache: Arc<BlockCache>,
    sizes: &[u64],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
    read_size: u64,
) -> Result<LatencySamples> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(LatencySamples::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let cache = Arc::clone(&cache);
        let order = order.to_vec();
        let sizes = sizes.to_vec();
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("L2 read semaphore closed: {e}"))?;
                let _permit = permit;

                let object_id = (work_index as u64) + 1;
                let start = Instant::now();
                let payload = cache
                    .get(object_id)
                    .await
                    .with_context(|| format!("L2 read missed object id {object_id}"))?;
                let read = usize::try_from(read_size.min(sizes[work_index]).max(1))
                    .context("L2 read size overflow")?;
                if payload.is_empty() {
                    return Err(anyhow!(
                        "L2 read returned empty payload for object id {object_id}"
                    ));
                }
                if payload.len() < read {
                    return Err(anyhow!(
                        "L2 read requested {} bytes but object {} had only {} bytes",
                        read,
                        object_id,
                        payload.len()
                    ));
                }

                let elapsed = start.elapsed();
                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("L2 read latency stats lock poisoned"))?;
                s.record(elapsed, read as u64);
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .context("L2 read worker panicked")?
            .context("L2 read worker failed")?;
    }

    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("L2 read worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("L2 read latency stats lock poisoned"))
}

async fn run_filesystem_writes(
    files: &[WorkFile],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
) -> Result<LatencySamples> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(LatencySamples::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let files = files.to_vec();
        let order = order.to_vec();
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let item = &files[work_index];
                let data = deterministic_bytes(
                    usize::try_from(item.size).context("file write size overflow")?,
                    item.index as u64 + 123,
                );
                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem write semaphore closed: {e}"))?;
                let _permit = permit;

                let start = Instant::now();
                let mut out = fs::File::create(&item.path)
                    .await
                    .with_context(|| format!("create {}", item.path.display()))?;
                out.write_all(&data)
                    .await
                    .with_context(|| format!("write {}", item.path.display()))?;
                out.flush().await?;

                let elapsed = start.elapsed();
                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("filesystem write latency stats lock poisoned"))?;
                s.record(elapsed, item.size);
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .context("filesystem write worker panicked")?
            .context("filesystem write worker failed")?;
    }

    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("filesystem write worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("filesystem write latency stats lock poisoned"))
}

async fn run_filesystem_reads(
    files: &[WorkFile],
    order: &[usize],
    io_depth: usize,
    concurrency: usize,
    read_size: u64,
) -> Result<LatencySamples> {
    let queue_depth = io_depth.max(1);
    let worker_count = concurrency.max(queue_depth);
    let stats = Arc::new(Mutex::new(LatencySamples::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(queue_depth));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let files = files.to_vec();
        let order = order.to_vec();
        let stats = Arc::clone(&stats);
        let counter = Arc::clone(&counter);
        let sem = Arc::clone(&sem);

        handles.push(tokio::spawn(async move {
            loop {
                let op_index = counter.fetch_add(1, Ordering::Relaxed);
                if op_index >= order.len() {
                    return Ok::<(), anyhow::Error>(());
                }
                let work_index = order[op_index];
                let item = &files[work_index];
                let read_len = read_size.min(item.size).max(1) as usize;

                let permit = sem
                    .acquire()
                    .await
                    .map_err(|e| anyhow!("filesystem read semaphore closed: {e}"))?;
                let _permit = permit;

                let start = Instant::now();
                let mut in_f = fs::File::open(&item.path)
                    .await
                    .with_context(|| format!("open {}", item.path.display()))?;
                let mut buf = vec![0u8; read_len];
                in_f.read_exact(&mut buf)
                    .await
                    .with_context(|| format!("read {}", item.path.display()))?;
                let elapsed = start.elapsed();

                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("filesystem read latency stats lock poisoned"))?;
                s.record(elapsed, read_len as u64);
            }
        }));
    }

    for handle in handles {
        handle
            .await
            .context("filesystem read worker panicked")?
            .context("filesystem read worker failed")?;
    }

    Arc::try_unwrap(stats)
        .map_err(|_| anyhow!("filesystem read worker coordination failed"))?
        .into_inner()
        .map_err(|_| anyhow!("filesystem read latency stats lock poisoned"))
}

fn clear_mount_artifacts(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path).with_context(|| format!("remove {}", path.display()))?;
    }
    std::fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))
}

fn normalize_path_for_compare(path: &Path) -> Option<String> {
    let raw = path.to_str()?;
    #[cfg(windows)]
    {
        Some(
            super::utils::normalize_mount_point(raw)
                .replace('/', "\\")
                .trim_end_matches('\\')
                .to_lowercase(),
        )
    }
    #[cfg(not(windows))]
    {
        Some(raw.trim_end_matches('/').to_string())
    }
}

fn cleanup_stale_mounts() -> Result<()> {
    let mounts_dir = super::utils::get_mounts_dir()?;
    for entry in std::fs::read_dir(&mounts_dir)
        .with_context(|| format!("read mounts dir {}", mounts_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let parsed: serde_json::Value =
            serde_json::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
        let Some(pid) = parsed.get("pid").and_then(serde_json::Value::as_u64) else {
            continue;
        };
        let Some(mount_point) = parsed
            .get("mount_point")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if !super::utils::is_process_running(pid as u32) {
            let _ = super::utils::remove_pid_file(mount_point);
        }
    }
    Ok(())
}

fn is_path_under_mount(target: &Path, mount_point: &Path) -> bool {
    let Some(target_norm) = normalize_path_for_compare(target) else {
        return false;
    };
    let Some(mount_norm) = normalize_path_for_compare(mount_point) else {
        return false;
    };
    target_norm == mount_norm
        || target_norm
            .strip_prefix(&mount_norm)
            .is_some_and(|tail| tail.starts_with('\\') || tail.starts_with('/'))
}

fn active_mounts() -> Result<Vec<ActiveMount>> {
    let mounts_dir = super::utils::get_mounts_dir()?;
    let mut mounts = Vec::new();
    for entry in std::fs::read_dir(&mounts_dir)
        .with_context(|| format!("read mounts dir {}", mounts_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        let parsed: serde_json::Value =
            serde_json::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
        let Some(pid) = parsed.get("pid").and_then(serde_json::Value::as_u64) else {
            continue;
        };
        let Some(mount_point) = parsed
            .get("mount_point")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let volume = parsed
            .get("volume")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !super::utils::is_process_running(pid as u32) {
            continue;
        }
        mounts.push(ActiveMount {
            mount_point: PathBuf::from(mount_point),
            volume: volume.to_string(),
        });
    }
    Ok(mounts)
}

fn resolve_run_mode(target: &Path) -> Result<ResolvedRunMode> {
    cleanup_stale_mounts()?;
    let mut best: Option<ActiveMount> = None;
    for mount in active_mounts()? {
        if is_path_under_mount(target, &mount.mount_point) {
            let keep = best.as_ref().is_none_or(|existing| {
                mount.mount_point.components().count() > existing.mount_point.components().count()
            });
            if keep {
                best = Some(mount);
            }
        }
    }

    if let Some(active) = best {
        Ok(ResolvedRunMode::MountedE2E {
            volume: active.volume,
            mount_point: active.mount_point,
            work_root: target.join(".studiofs-test-cache"),
        })
    } else {
        Ok(ResolvedRunMode::DirectL2 {
            cache_dir: target.to_path_buf(),
        })
    }
}

async fn run_direct(params: &TestCacheParams, cache_dir: &Path) -> Result<BenchmarkReport> {
    let started = Instant::now();
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let cache_config = BlockCacheConfig::new(cache_dir, params.l1_limit, params.l2_limit);
    let (cache, _rx) = BlockCache::open(cache_config)
        .with_context(|| format!("open cache dir {}", cache_dir.display()))?;
    let cache = Arc::new(cache);

    let work_sizes = build_work_sizes(
        DEFAULT_DIRECT_TOTAL_BYTES,
        params.block_size,
        params.read_size,
        params.small_file_share,
    );
    let order = build_access_sequence(
        work_sizes.len(),
        params.access_pattern,
        0x1234_5678_9abc_def0,
    );

    let writes = run_l2_writes(
        Arc::clone(&cache),
        &work_sizes,
        &order,
        params.io_depth,
        params.concurrency,
    )
    .await?;
    let reads = run_l2_reads(
        Arc::clone(&cache),
        &work_sizes,
        &order,
        params.io_depth,
        params.concurrency,
        params.read_size,
    )
    .await?;

    let completed = Instant::now();
    let total_elapsed_ms = (completed - started).as_secs_f64() * 1000.0;
    let cache_stats = cache.stats();

    Ok(BenchmarkReport {
        mode: "direct-l2".to_string(),
        cache_dir: cache_dir.display().to_string(),
        started_at,
        completed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        total_elapsed_ms,
        block_size: params.block_size,
        read_size: params.read_size,
        concurrency: params.concurrency,
        io_depth: params.io_depth,
        access_pattern: format!("{:?}", params.access_pattern).to_lowercase(),
        small_file_share: params.small_file_share,
        l1_limit: params.l1_limit,
        l2_limit: params.l2_limit,
        volume: None,
        mount_point: None,
        l1_bytes: Some(cache_stats.l1_bytes),
        l2_bytes: Some(cache_stats.l2_bytes),
        cache_pipeline: Some(cache.cache_pipeline_metrics().into()),
        work_root: None,
        writes: writes.to_report(total_elapsed_ms),
        reads: reads.to_report(total_elapsed_ms),
    })
}

async fn run_mounted_end_to_end(
    params: &TestCacheParams,
    volume: &str,
    mount_point: &Path,
    work_root: &Path,
) -> Result<BenchmarkReport> {
    let started = Instant::now();
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let benchmark_result = async {
        clear_mount_artifacts(work_root).context("prepare mount benchmark directory")?;

        let files = build_file_worklist(
            DEFAULT_E2E_TOTAL_BYTES,
            params.block_size,
            params.read_size,
            params.small_file_share,
            work_root,
        );
        if files.is_empty() {
            anyhow::bail!("nothing to benchmark");
        }
        let order =
            build_access_sequence(files.len(), params.access_pattern, 0x55aa_55aa_d00d_f00d);

        let writes =
            run_filesystem_writes(&files, &order, params.io_depth, params.concurrency).await?;
        let reads = run_filesystem_reads(
            &files,
            &order,
            params.io_depth,
            params.concurrency,
            params.read_size,
        )
        .await?;

        let total_elapsed_ms = (Instant::now() - started).as_secs_f64() * 1000.0;

        let _ = fs::remove_dir_all(work_root).await;

        Ok::<_, anyhow::Error>(BenchmarkReport {
            mode: "e2e".to_string(),
            cache_dir: params.target.display().to_string(),
            started_at,
            completed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            total_elapsed_ms,
            block_size: params.block_size,
            read_size: params.read_size,
            concurrency: params.concurrency,
            io_depth: params.io_depth,
            access_pattern: format!("{:?}", params.access_pattern).to_lowercase(),
            small_file_share: params.small_file_share,
            l1_limit: params.l1_limit,
            l2_limit: params.l2_limit,
            work_root: Some(work_root.display().to_string()),
            volume: Some(volume.to_string()),
            mount_point: Some(mount_point.display().to_string()),
            l1_bytes: None,
            l2_bytes: None,
            cache_pipeline: None,
            writes: writes.to_report(total_elapsed_ms),
            reads: reads.to_report(total_elapsed_ms),
        })
    }
    .await;
    match benchmark_result {
        Ok(mut report) => {
            report.completed_at =
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            report.total_elapsed_ms = (Instant::now() - started).as_secs_f64() * 1000.0;
            Ok(report)
        }
        Err(err) => Err(err),
    }
}

fn format_binary_value(mut value: f64) -> (f64, &'static str) {
    let units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut unit = units[0];
    for next_unit in units.iter().skip(1) {
        if value < BINARY_UNIT_DIVISOR {
            break;
        }
        value /= BINARY_UNIT_DIVISOR;
        unit = next_unit;
    }
    (value, unit)
}

fn format_bytes(bytes: u64) -> String {
    let (value, unit) = format_binary_value(bytes as f64);
    if unit == "B" {
        format!("{value:.0} {unit}")
    } else {
        format!("{value:.2} {unit}")
    }
}

fn format_throughput(bytes_per_sec: f64) -> String {
    let (value, unit) = format_binary_value(bytes_per_sec.max(0.0));
    if unit == "B" {
        format!("{value:.0} {unit}/s")
    } else {
        format!("{value:.2} {unit}/s")
    }
}

fn format_duration_ms(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.2} s", ms / 1000.0)
    } else {
        format!("{ms:.2} ms")
    }
}

fn avg_latency_ms(total_latency_ns: u64, ops: u64) -> f64 {
    if ops == 0 {
        0.0
    } else {
        total_latency_ns as f64 / ops as f64 / 1_000_000.0
    }
}

fn format_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    for row in rows {
        for (idx, width) in widths.iter_mut().enumerate() {
            let cell = row.get(idx).map(String::as_str).unwrap_or("");
            *width = (*width).max(cell.len());
        }
    }

    let mut out = String::new();
    let write_border = |out: &mut String| {
        out.push('+');
        for width in &widths {
            for _ in 0..(*width + 2) {
                out.push('-');
            }
            out.push('+');
        }
        out.push('\n');
    };

    let write_row = |out: &mut String, cells: &[String]| {
        out.push('|');
        for (idx, width) in widths.iter().enumerate() {
            let cell = cells.get(idx).map(String::as_str).unwrap_or("");
            let _ = write!(out, " {:<width$} |", cell, width = width);
        }
        out.push('\n');
    };

    write_border(&mut out);
    let header_cells: Vec<String> = headers.iter().map(|header| (*header).to_string()).collect();
    write_row(&mut out, &header_cells);
    write_border(&mut out);
    for row in rows {
        write_row(&mut out, row);
    }
    write_border(&mut out);
    out
}

fn slugify_report_segment(value: &str) -> String {
    let mut out = String::new();
    let mut last_was_dash = false;
    for ch in value.chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            last_was_dash = false;
        } else if !last_was_dash {
            out.push('-');
            last_was_dash = true;
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "report".to_string()
    } else {
        trimmed.to_string()
    }
}

fn default_report_file_name(report: &BenchmarkReport, now: DateTime<Utc>) -> String {
    format!(
        "studiofs-test-cache-{}-{}.json",
        slugify_report_segment(&report.mode),
        now.format("%Y%m%d-%H%M%S"),
    )
}

fn next_available_path(path: &Path, suffix: usize) -> PathBuf {
    let parent = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("studiofs-test-cache");
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    let candidate_name = if extension.is_empty() {
        format!("{stem}-{suffix}")
    } else {
        format!("{stem}-{suffix}.{extension}")
    };
    parent.join(candidate_name)
}

fn resolve_report_target(
    report: &BenchmarkReport,
    json_output: Option<&Path>,
    current_dir: &Path,
    now: DateTime<Utc>,
) -> (PathBuf, bool) {
    if let Some(json_output) = json_output {
        let explicit = if json_output.is_absolute() {
            json_output.to_path_buf()
        } else {
            current_dir.join(json_output)
        };
        if explicit.is_dir() {
            return (explicit.join(default_report_file_name(report, now)), true);
        }
        return (explicit, false);
    }

    (
        current_dir.join(default_report_file_name(report, now)),
        true,
    )
}

fn write_report_file(report_path: &Path, json: &[u8], auto_named: bool) -> Result<PathBuf> {
    if !auto_named {
        if let Some(parent) = report_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create report dir {}", parent.display()))?;
        }
        std::fs::write(report_path, json)
            .with_context(|| format!("write benchmark report {}", report_path.display()))?;
        return Ok(report_path.to_path_buf());
    }

    let mut candidate = report_path.to_path_buf();
    let mut suffix = 2usize;
    loop {
        if let Some(parent) = candidate.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create report dir {}", parent.display()))?;
        }

        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(mut file) => {
                file.write_all(json)
                    .with_context(|| format!("write benchmark report {}", candidate.display()))?;
                file.flush()
                    .with_context(|| format!("flush benchmark report {}", candidate.display()))?;
                return Ok(candidate);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                candidate = next_available_path(report_path, suffix);
                suffix = suffix.saturating_add(1);
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("create benchmark report {}", candidate.display()));
            }
        }
    }
}

fn save_benchmark_report_in(
    report: &BenchmarkReport,
    json_output: Option<&Path>,
    current_dir: &Path,
    now: DateTime<Utc>,
) -> Result<PathBuf> {
    let json =
        serde_json::to_vec_pretty(report).context("serialize benchmark report as JSON report")?;
    let (report_path, auto_named) = resolve_report_target(report, json_output, current_dir, now);
    write_report_file(&report_path, &json, auto_named)
}

fn save_benchmark_report(report: &BenchmarkReport, json_output: Option<&Path>) -> Result<PathBuf> {
    let current_dir = std::env::current_dir().context("resolve current working directory")?;
    save_benchmark_report_in(report, json_output, &current_dir, Utc::now())
}

fn render_human_summary(report: &BenchmarkReport) -> String {
    let mut out = String::new();
    let target_label = if report.mode == "direct-l2" {
        "Cache dir"
    } else {
        "Target"
    };

    let _ = writeln!(&mut out, "Cache benchmark");
    let _ = writeln!(&mut out, "{target_label}: {}", report.cache_dir);
    let _ = writeln!(&mut out, "Mode:      {}", report.mode);
    if let Some(volume) = &report.volume {
        let _ = writeln!(&mut out, "Volume:    {volume}");
    }
    if let Some(mount_point) = &report.mount_point {
        let _ = writeln!(&mut out, "Mount:     {mount_point}");
    }
    if let Some(work_root) = &report.work_root {
        let _ = writeln!(&mut out, "Work root: {work_root}");
    }
    let _ = writeln!(&mut out, "Started:   {}", report.started_at);
    let _ = writeln!(&mut out, "Completed: {}", report.completed_at);
    let _ = writeln!(
        &mut out,
        "Elapsed:   {}",
        format_duration_ms(report.total_elapsed_ms)
    );
    let _ = writeln!(
        &mut out,
        "Workload:  block {}, read {}, concurrency {}, io-depth {}, pattern {}, small files {}%",
        format_bytes(report.block_size),
        format_bytes(report.read_size),
        report.concurrency,
        report.io_depth,
        report.access_pattern,
        report.small_file_share,
    );
    let _ = writeln!(
        &mut out,
        "Limits:    L1 {}, L2 {}",
        format_bytes(report.l1_limit),
        format_bytes(report.l2_limit),
    );
    if report.l1_bytes.is_some() || report.l2_bytes.is_some() {
        let _ = writeln!(
            &mut out,
            "Resident:  L1 {}, L2 {}",
            report
                .l1_bytes
                .map(format_bytes)
                .unwrap_or_else(|| "-".to_string()),
            report
                .l2_bytes
                .map(format_bytes)
                .unwrap_or_else(|| "-".to_string()),
        );
    }
    out.push('\n');
    let _ = writeln!(&mut out, "Operation results");
    let operation_rows = vec![
        vec![
            "Write".to_string(),
            report.writes.operations.to_string(),
            format_bytes(report.writes.bytes),
            format_throughput(report.writes.throughput_bytes_per_sec),
            format_duration_ms(report.writes.avg_latency_ms),
            format_duration_ms(report.writes.p95_ms),
            format_duration_ms(report.writes.p99_ms),
        ],
        vec![
            "Read".to_string(),
            report.reads.operations.to_string(),
            format_bytes(report.reads.bytes),
            format_throughput(report.reads.throughput_bytes_per_sec),
            format_duration_ms(report.reads.avg_latency_ms),
            format_duration_ms(report.reads.p95_ms),
            format_duration_ms(report.reads.p99_ms),
        ],
    ];
    let _ = write!(
        &mut out,
        "{}",
        format_table(
            &["Phase", "Ops", "Data", "Throughput", "Avg", "P95", "P99"],
            &operation_rows,
        )
    );

    if let Some(pipeline) = &report.cache_pipeline {
        out.push('\n');
        let _ = writeln!(&mut out, "Cache pipeline");
        let pipeline_rows = vec![
            vec![
                "L2 write".to_string(),
                pipeline.l2_write.ops.to_string(),
                format_bytes(pipeline.l2_write.bytes),
                format_duration_ms(avg_latency_ms(
                    pipeline.l2_write.total_latency_ns,
                    pipeline.l2_write.ops,
                )),
                format_duration_ms(pipeline.l2_write.max_latency_ns as f64 / 1_000_000.0),
            ],
            vec![
                "L2 read".to_string(),
                pipeline.l2_read.ops.to_string(),
                format_bytes(pipeline.l2_read.bytes),
                format_duration_ms(avg_latency_ms(
                    pipeline.l2_read.total_latency_ns,
                    pipeline.l2_read.ops,
                )),
                format_duration_ms(pipeline.l2_read.max_latency_ns as f64 / 1_000_000.0),
            ],
        ];
        let _ = write!(
            &mut out,
            "{}",
            format_table(&["Stage", "Ops", "Data", "Avg", "Max"], &pipeline_rows)
        );
        let _ = writeln!(
            &mut out,
            "Upload queue: depth {} (peak {}), submitted {}, completed {}, requeued {}, canceled {}",
            pipeline.upload_queue.pending_depth,
            pipeline.upload_queue.pending_depth_peak,
            pipeline.upload_queue.submitted,
            pipeline.upload_queue.completed,
            pipeline.upload_queue.requeued,
            pipeline.upload_queue.canceled,
        );
        let _ = writeln!(
            &mut out,
            "Materialize: {} batches, {} blocks, {}, inflight {} (peak {})",
            pipeline.materialize_batches.batches,
            pipeline.materialize_batches.blocks,
            format_bytes(pipeline.materialize_batches.bytes),
            pipeline.materialize_batches.inflight_batches,
            pipeline.materialize_batches.inflight_batches_peak,
        );
    }

    out
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    target: PathBuf,
    block_size: Option<String>,
    read_size: Option<String>,
    concurrency: usize,
    io_depth: usize,
    access_pattern: TestCacheAccessPattern,
    small_file_share: u8,
    l1_limit: Option<String>,
    l2_limit: Option<String>,
    json_output: Option<PathBuf>,
) -> Result<()> {
    if io_depth == 0 {
        return Err(anyhow!("--io-depth must be >= 1"));
    }
    if concurrency == 0 {
        return Err(anyhow!("--concurrency must be >= 1"));
    }
    if small_file_share > 100 {
        return Err(anyhow!("--small-file-share must be in range 0..100"));
    }

    let block_size = parse_size_arg("block-size", block_size.as_deref().unwrap_or("8MB"))?;
    let read_size = parse_size_arg(
        "read-size",
        read_size
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&block_size.to_string()),
    )?
    .max(1);
    let l1_limit = parse_size_arg(
        "l1-limit",
        l1_limit.as_deref().unwrap_or(&DEFAULT_L1_SIZE.to_string()),
    )?;
    let l2_limit = parse_size_arg(
        "l2-limit",
        l2_limit.as_deref().unwrap_or(&DEFAULT_L2_SIZE.to_string()),
    )?;

    if l1_limit < 1 || l2_limit < 1 {
        return Err(anyhow!("cache limits must be > 0"));
    }

    let params = TestCacheParams {
        target: target.clone(),
        block_size,
        read_size,
        concurrency,
        io_depth,
        access_pattern,
        small_file_share,
        l1_limit,
        l2_limit,
    };

    let resolved_mode = resolve_run_mode(&target)?;

    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    let report = rt.block_on(async {
        match &resolved_mode {
            ResolvedRunMode::DirectL2 { cache_dir } => run_direct(&params, cache_dir).await,
            ResolvedRunMode::MountedE2E {
                volume,
                mount_point,
                work_root,
            } => run_mounted_end_to_end(&params, volume, mount_point, work_root).await,
        }
    })?;

    print!("{}", render_human_summary(&report));
    let report_path = save_benchmark_report(&report, json_output.as_deref())?;
    println!("JSON report: {}", report_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::tempdir;

    fn sample_report() -> BenchmarkReport {
        BenchmarkReport {
            mode: "direct-l2".to_string(),
            cache_dir: r"D:\test-cache".to_string(),
            started_at: "2026-03-18T15:12:55Z".to_string(),
            completed_at: "2026-03-18T15:12:55Z".to_string(),
            total_elapsed_ms: 93.05,
            block_size: 8 * 1024 * 1024,
            read_size: 8 * 1024 * 1024,
            concurrency: 4,
            io_depth: 32,
            access_pattern: "sequential".to_string(),
            small_file_share: 20,
            l1_limit: 2 * 1024 * 1024 * 1024,
            l2_limit: 100 * 1024 * 1024 * 1024,
            work_root: None,
            volume: None,
            mount_point: None,
            l1_bytes: Some(128 * 1024 * 1024),
            l2_bytes: Some(0),
            cache_pipeline: Some(CachePipelineReport {
                l2_read: IoStatsReport {
                    ops: 0,
                    bytes: 0,
                    total_latency_ns: 0,
                    max_latency_ns: 0,
                    latency_histogram: LatencyHistogramReport {
                        bucket_upper_bounds_ns: vec![50_000],
                        bucket_counts: vec![0],
                    },
                },
                l2_write: IoStatsReport {
                    ops: 16,
                    bytes: 128 * 1024 * 1024,
                    total_latency_ns: 64_000_000,
                    max_latency_ns: 8_000_000,
                    latency_histogram: LatencyHistogramReport {
                        bucket_upper_bounds_ns: vec![50_000],
                        bucket_counts: vec![16],
                    },
                },
                materialize_batches: MaterializeBatchStatsReport {
                    batches: 4,
                    blocks: 16,
                    bytes: 128 * 1024 * 1024,
                    inflight_batches: 0,
                    inflight_batches_peak: 2,
                    duration_histogram: LatencyHistogramReport {
                        bucket_upper_bounds_ns: vec![50_000],
                        bucket_counts: vec![4],
                    },
                },
                upload_queue: UploadQueueStatsReport {
                    pending_depth: 0,
                    pending_depth_peak: 3,
                    submitted: 16,
                    completed: 16,
                    requeued: 0,
                    canceled: 0,
                },
            }),
            writes: OpReport {
                operations: 16,
                bytes: 128 * 1024 * 1024,
                elapsed_ms: 80.0,
                avg_latency_ms: 5.0,
                p50_ms: 4.7,
                p95_ms: 8.3,
                p99_ms: 8.8,
                throughput_bytes_per_sec: 1_375.53 * 1024.0 * 1024.0,
            },
            reads: OpReport {
                operations: 16,
                bytes: 128 * 1024 * 1024,
                elapsed_ms: 78.0,
                avg_latency_ms: 4.8,
                p50_ms: 4.5,
                p95_ms: 8.1,
                p99_ms: 8.6,
                throughput_bytes_per_sec: 1_375.53 * 1024.0 * 1024.0,
            },
        }
    }

    #[test]
    fn mount_path_detection_handles_nested_paths() {
        let mount = PathBuf::from("/mnt/studiofs");
        let target = PathBuf::from("/mnt/studiofs/cache-bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[test]
    fn mount_path_detection_rejects_sibling_paths() {
        let mount = PathBuf::from("/mnt/studiofs");
        let target = PathBuf::from("/mnt/studiofs-other/cache-bench");
        assert!(!is_path_under_mount(&target, &mount));
    }

    #[cfg(windows)]
    #[test]
    fn mount_path_detection_normalizes_extended_drive_prefix() {
        let mount = PathBuf::from(r"C:\StudioFS");
        let target = PathBuf::from(r"\\?\C:\StudioFS\bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[cfg(windows)]
    #[test]
    fn mount_path_detection_normalizes_unc_prefix() {
        let mount = PathBuf::from(r"\\server\share\studiofs");
        let target = PathBuf::from(r"\\?\UNC\server\share\studiofs\bench");
        assert!(is_path_under_mount(&target, &mount));
    }

    #[test]
    fn default_report_path_uses_current_dir_and_avoids_collisions() {
        let temp = tempdir().unwrap();
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let first = save_benchmark_report_in(&report, None, temp.path(), now).unwrap();
        assert_eq!(
            first,
            temp.path()
                .join("studiofs-test-cache-direct-l2-20260319-151255.json")
        );

        let second = save_benchmark_report_in(&report, None, temp.path(), now).unwrap();
        assert_eq!(
            second,
            temp.path()
                .join("studiofs-test-cache-direct-l2-20260319-151255-2.json")
        );
    }

    #[test]
    fn save_benchmark_report_writes_pretty_json_to_requested_path() {
        let temp = tempdir().unwrap();
        let report = sample_report();
        let now = Utc
            .with_ymd_and_hms(2026, 3, 19, 15, 12, 55)
            .single()
            .unwrap();

        let saved = save_benchmark_report_in(
            &report,
            Some(Path::new("reports/cache-run.json")),
            temp.path(),
            now,
        )
        .unwrap();

        assert_eq!(saved, temp.path().join("reports/cache-run.json"));

        let content = std::fs::read_to_string(saved).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["mode"], "direct-l2");
        assert!(content.contains("\n  \"mode\""));
    }

    #[test]
    fn render_human_summary_is_table_oriented_without_json_blob() {
        let summary = render_human_summary(&sample_report());
        assert!(summary.contains("Operation results"));
        assert!(summary.contains("| Phase |"));
        assert!(summary.contains("Cache pipeline"));
        assert!(summary.contains("Upload queue:"));
        assert!(!summary.contains("\"cache_pipeline\""));
        assert!(!summary.contains("\"mode\""));
    }
}

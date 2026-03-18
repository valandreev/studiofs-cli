//! `studiofs test-cache` — benchmark cache performance.
//!
//! Supports:
//! - direct L2 benchmark mode (`direct-l2`) that bypasses VFS
//! - end-to-end benchmark mode (`e2e`) that runs copy/read on a mounted volume

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use clap::ValueEnum;
use serde::Serialize;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use std::time::{Duration, Instant};
use studiofs_cache::block_cache::{DEFAULT_L1_SIZE, DEFAULT_L2_SIZE};
use studiofs_cache::{BlockCache, BlockCacheConfig};
use studiofs_vfs::parse_size_bytes;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

const DEFAULT_DIRECT_TOTAL_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_E2E_TOTAL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
pub enum TestCacheMode {
    /// Write/read blocks directly through `studiofs-cache` L2 API.
    #[value(name = "direct-l2")]
    DirectL2,
    /// Mount a volume and run copy/read workloads through the filesystem path.
    #[value(name = "e2e")]
    EndToEnd,
}

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
    cache_dir: PathBuf,
    mode: TestCacheMode,
    block_size: u64,
    read_size: u64,
    concurrency: usize,
    io_depth: usize,
    access_pattern: TestCacheAccessPattern,
    small_file_share: u8,
    l1_limit: u64,
    l2_limit: u64,
    volume: Option<String>,
    mount_point: Option<PathBuf>,
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
    volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mount_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    l1_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    l2_bytes: Option<u64>,
    writes: OpReport,
    reads: OpReport,
}

#[derive(Default)]
struct LatencySamples {
    samples_ns: Vec<u64>,
    total_ops: u64,
    total_bytes: u64,
}

impl LatencySamples {
    fn record(&mut self, elapsed: Duration, bytes: u64) {
        let ns = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.samples_ns.push(ns);
        self.total_ops = self.total_ops.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(bytes);
    }

    fn total_duration_ms(&self) -> f64 {
        self.samples_ns
            .iter()
            .map(|ns| *ns as f64 / 1_000_000.0)
            .sum::<f64>()
    }

    fn avg_ns(&self) -> f64 {
        if self.total_ops == 0 {
            return 0.0;
        }
        let ns_total = self.samples_ns.iter().copied().map(|ns| ns as f64).sum::<f64>();
        ns_total / self.total_ops as f64
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
    let stats = Arc::new(Mutex::new(LatencySamples::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
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

                let data_len = usize::try_from(sizes[work_index]).context("L2 write size overflow")?;
                let object_id = (work_index as u64) + 1;
                let data = deterministic_bytes(data_len, object_id.saturating_mul(31).saturating_add(worker_id as u64));

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
    let stats = Arc::new(Mutex::new(LatencySamples::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
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
                    return Err(anyhow!("L2 read returned empty payload for object id {object_id}"));
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
    let stats = Arc::new(Mutex::new(LatencySamples::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
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
    let stats = Arc::new(Mutex::new(LatencySamples::default()));
    let counter = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(Semaphore::new(io_depth.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
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
                let mut read = 0usize;
                while read < buf.len() {
                    let n = in_f
                        .read(&mut buf[read..])
                        .await
                        .with_context(|| format!("read {}", item.path.display()))?;
                    if n == 0 {
                        break;
                    }
                    read += n;
                }
                if read == 0 {
                    return Err(anyhow!("read returned 0 bytes for {}", item.path.display()));
                }
                let elapsed = start.elapsed();

                let mut s = stats
                    .lock()
                    .map_err(|_| anyhow!("filesystem read latency stats lock poisoned"))?;
                s.record(elapsed, read as u64);
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
        std::fs::remove_dir_all(path)
            .with_context(|| format!("remove {}", path.display()))?;
    }
    std::fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))
}

fn wait_for_pid_file(
    mount_point: &Path,
    expected_volume: &str,
    timeout: Duration,
) -> Result<(u32, String)> {
    let mp = mount_point
        .to_str()
        .context("mount point is not valid UTF-8")?
        .to_string();
    let deadline = Instant::now() + timeout;

    loop {
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timeout waiting for mount point '{}', expected volume '{}'",
                mp, expected_volume
            ));
        }

        if let Ok((pid, mounted_mp, mounted_vol)) = super::utils::read_pid_file(&mp) {
            if !super::utils::is_process_running(pid) {
                let _ = super::utils::remove_pid_file(&mounted_mp);
            } else if mounted_vol.is_empty() || mounted_vol == expected_volume {
                return Ok((pid, mounted_mp));
            }
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}

async fn run_direct(params: &TestCacheParams) -> Result<BenchmarkReport> {
    let started = Instant::now();
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let cache_config = BlockCacheConfig::new(
        &params.cache_dir,
        params.l1_limit,
        params.l2_limit,
    );
    let (cache, _rx) = BlockCache::open(cache_config)
        .with_context(|| format!("open cache dir {}", params.cache_dir.display()))?;
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
        cache_dir: params.cache_dir.display().to_string(),
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
        writes: writes.to_report(total_elapsed_ms),
        reads: reads.to_report(total_elapsed_ms),
    })
}

async fn run_end_to_end(params: &TestCacheParams) -> Result<BenchmarkReport> {
    let started = Instant::now();
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let volume = params.volume.as_deref().ok_or_else(|| {
        anyhow!("--volume is required for e2e mode")
    })?;
    let mount_point = params.mount_point.as_ref().ok_or_else(|| {
        anyhow!("--mount-point is required for e2e mode")
    })?;
    let mount_point = mount_point
        .to_str()
        .context("mount point is not valid UTF-8")?;
    let mount_dir = Path::new(mount_point);

    let meta_dir = super::utils::get_default_meta_dir(volume)?;
    super::mount::run(
        volume,
        Some(mount_point),
        &meta_dir,
        Some(&params.cache_dir),
        Some(&params.l2_limit.to_string()),
        Some(&params.l1_limit.to_string()),
        None,
        false,
        true,
        None,
    )?;

    let (_pid, actual_mount_point) = wait_for_pid_file(mount_dir, volume, Duration::from_secs(60))?;
    let actual_mount_point = Path::new(&actual_mount_point).to_path_buf();
    let work_root = actual_mount_point.join(".studiofs-test-cache");
    let benchmark_result = async {
        clear_mount_artifacts(&work_root).context("prepare mount benchmark directory")?;

        let files = build_file_worklist(
            DEFAULT_E2E_TOTAL_BYTES,
            params.block_size,
            params.read_size,
            params.small_file_share,
            &work_root,
        );
        if files.is_empty() {
            anyhow::bail!("nothing to benchmark");
        }
        let order = build_access_sequence(
            files.len(),
            params.access_pattern,
            0x55aa_55aa_d00d_f00d,
        );

        let writes = run_filesystem_writes(
            &files,
            &order,
            params.io_depth,
            params.concurrency,
        )
        .await?;
        let reads = run_filesystem_reads(
            &files,
            &order,
            params.io_depth,
            params.concurrency,
            params.read_size,
        )
        .await?;

        let total_elapsed_ms = (Instant::now() - started).as_secs_f64() * 1000.0;

        let _ = fs::remove_dir_all(&work_root).await;

        Ok::<_, anyhow::Error>(BenchmarkReport {
            mode: "e2e".to_string(),
            cache_dir: params.cache_dir.display().to_string(),
            started_at,
            completed_at: chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            total_elapsed_ms,
            block_size: params.block_size,
            read_size: params.read_size,
            concurrency: params.concurrency,
            io_depth: params.io_depth,
            access_pattern: format!("{:?}", params.access_pattern).to_lowercase(),
            small_file_share: params.small_file_share,
            l1_limit: params.l1_limit,
            l2_limit: params.l2_limit,
            volume: Some(volume.to_string()),
            mount_point: Some(actual_mount_point.display().to_string()),
            l1_bytes: None,
            l2_bytes: None,
            writes: writes.to_report(total_elapsed_ms),
            reads: reads.to_report(total_elapsed_ms),
        })
    }
    .await;

    let unmount_result = super::umount::run(
        actual_mount_point
            .to_str()
            .context("mounted path is not valid UTF-8")?,
    );
    match (benchmark_result, unmount_result) {
        (Ok(mut report), Ok(())) => {
            report.completed_at = chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            report.total_elapsed_ms = (Instant::now() - started).as_secs_f64() * 1000.0;
            Ok(report)
        }
        (Err(err), Ok(())) => Err(err),
        (Ok(_), Err(err)) => Err(err),
        (Err(bench_err), Err(unmount_err)) => Err(anyhow!(
            "benchmark failed: {bench_err}; additionally failed to unmount: {unmount_err}"
        )),
    }
}

fn format_bytes(bytes: u64) -> String {
    let mut out = String::new();
    let units = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes as f64;
    let mut i = 0usize;
    while value >= 1024.0 && i + 1 < units.len() {
        value /= 1024.0;
        i += 1;
    }
    let _ = write!(&mut out, "{value:.2}{}", units[i]);
    out
}

fn print_human_summary(report: &BenchmarkReport) {
    println!("Benchmark summary");
    println!("  mode: {}", report.mode);
    println!("  cache-dir: {}", report.cache_dir);
    if let Some(volume) = &report.volume {
        println!("  volume: {volume}");
    }
    if let Some(mount_point) = &report.mount_point {
        println!("  mount: {mount_point}");
    }
    println!("  elapsed: {:.2} ms", report.total_elapsed_ms);
    println!(
        "  reads: {} ops ({}), writes: {} ops ({})",
        report.reads.operations,
        format_bytes(report.reads.bytes),
        report.writes.operations,
        format_bytes(report.writes.bytes),
    );
    println!(
        "  read throughput: {:.2} MiB/s",
        report.reads.throughput_bytes_per_sec / 1024.0 / 1024.0
    );
    println!(
        "  write throughput: {:.2} MiB/s",
        report.writes.throughput_bytes_per_sec / 1024.0 / 1024.0
    );
    println!("  JSON");
    println!("{}", serde_json::to_string_pretty(report).unwrap_or_else(|_| "{}".to_string()));
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    cache_dir: PathBuf,
    mode: TestCacheMode,
    block_size: Option<String>,
    read_size: Option<String>,
    concurrency: usize,
    io_depth: usize,
    access_pattern: TestCacheAccessPattern,
    small_file_share: u8,
    l1_limit: Option<String>,
    l2_limit: Option<String>,
    volume: Option<String>,
    mount_point: Option<PathBuf>,
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
        l1_limit
            .as_deref()
            .unwrap_or(&DEFAULT_L1_SIZE.to_string()),
    )?;
    let l2_limit = parse_size_arg(
        "l2-limit",
        l2_limit
            .as_deref()
            .unwrap_or(&DEFAULT_L2_SIZE.to_string()),
    )?;

    if l1_limit < 1 || l2_limit < 1 {
        return Err(anyhow!("cache limits must be > 0"));
    }

    let params = TestCacheParams {
        cache_dir,
        mode,
        block_size,
        read_size,
        concurrency,
        io_depth,
        access_pattern,
        small_file_share,
        l1_limit,
        l2_limit,
        volume,
        mount_point,
    };

    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    let report = rt.block_on(async {
        match params.mode {
            TestCacheMode::DirectL2 => run_direct(&params).await,
            TestCacheMode::EndToEnd => run_end_to_end(&params).await,
        }
    })?;

    print_human_summary(&report);
    Ok(())
}

//! Internal benchmark engine entry points shared by legacy test-cache and bench.

use anyhow::Result;
use std::path::{Path, PathBuf};
use std::time::Duration;

mod direct;
pub(crate) mod mounted;
mod profiling;
mod progress;
mod reporting;
pub(crate) mod result;
mod telemetry;
pub(crate) mod workload;

use crate::cli::test_cache::TestCacheAccessPattern;

pub(crate) use profiling::merge_profile_report;
pub(crate) use reporting::{render_human_summary, save_benchmark_report};
pub(crate) use result::BenchmarkReport;
pub(crate) use workload::{
    DEFAULT_SMALL_CLASS_MAX_BYTES, DEFAULT_TEST_CACHE_BLOCK_SIZE_STR, WorkloadSizeInput,
    parse_size_arg, parse_sizes_arg,
};

/// Complete benchmark engine configuration after CLI/default validation.
#[derive(Debug)]
pub(crate) struct BenchmarkConfig {
    pub(crate) target: PathBuf,
    pub(crate) block_size: u64,
    pub(crate) read_size: u64,
    pub(crate) size_class_small_max: u64,
    pub(crate) size_class_medium_max: u64,
    pub(crate) workload_repeat: usize,
    pub(crate) concurrency: usize,
    pub(crate) sequential_streams: bool,
    pub(crate) io_depth: usize,
    pub(crate) warmup_runs: u32,
    pub(crate) sizes: WorkloadSizeInput,
    pub(crate) access_pattern: TestCacheAccessPattern,
    pub(crate) during_upload_min_pending: usize,
    pub(crate) during_upload_timeout: Duration,
    pub(crate) l1_limit: u64,
    pub(crate) profile: bool,
    pub(crate) full_file_reads: bool,
    pub(crate) run_id: String,
}

/// Mounted runtime metadata required to run an end-to-end filesystem benchmark.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MountedBenchmarkTarget<'a> {
    pub(crate) pid: u32,
    pub(crate) volume: &'a str,
    pub(crate) mount_point: &'a Path,
    pub(crate) work_root: &'a Path,
    pub(crate) cache_path: Option<&'a Path>,
}

pub(crate) async fn run_direct_shim(
    config: &BenchmarkConfig,
    cache_dir: &Path,
) -> Result<BenchmarkReport> {
    direct::run_direct(config, cache_dir).await
}

pub(crate) async fn run_mounted(
    config: &BenchmarkConfig,
    target: MountedBenchmarkTarget<'_>,
) -> Result<BenchmarkReport> {
    mounted::run_mounted_end_to_end(
        config,
        target.pid,
        target.volume,
        target.mount_point,
        target.work_root,
        target.cache_path,
    )
    .await
}

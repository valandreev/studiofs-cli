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

pub(crate) struct BenchmarkConfig {
    target: PathBuf,
    block_size: u64,
    read_size: u64,
    size_class_small_max: u64,
    size_class_medium_max: u64,
    workload_repeat: usize,
    concurrency: usize,
    sequential_streams: bool,
    io_depth: usize,
    warmup_runs: u32,
    sizes: WorkloadSizeInput,
    access_pattern: TestCacheAccessPattern,
    during_upload_min_pending: usize,
    during_upload_timeout: Duration,
    l1_limit: u64,
    profile: bool,
    full_file_reads: bool,
    run_id: String,
}

impl BenchmarkConfig {
    #[expect(
        clippy::too_many_arguments,
        reason = "CLI parsing validates each independent benchmark knob before constructing the engine config"
    )]
    pub(crate) fn new(
        target: PathBuf,
        block_size: u64,
        read_size: u64,
        size_class_small_max: u64,
        size_class_medium_max: u64,
        workload_repeat: usize,
        concurrency: usize,
        sequential_streams: bool,
        io_depth: usize,
        warmup_runs: u32,
        sizes: WorkloadSizeInput,
        access_pattern: TestCacheAccessPattern,
        during_upload_min_pending: usize,
        during_upload_timeout: Duration,
        l1_limit: u64,
        profile: bool,
        full_file_reads: bool,
        run_id: String,
    ) -> Self {
        Self {
            target,
            block_size,
            read_size,
            size_class_small_max,
            size_class_medium_max,
            workload_repeat,
            concurrency,
            sequential_streams,
            io_depth,
            warmup_runs,
            sizes,
            access_pattern,
            during_upload_min_pending,
            during_upload_timeout,
            l1_limit,
            profile,
            full_file_reads,
            run_id,
        }
    }
}

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

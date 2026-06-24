//! Internal benchmark engine entry points shared by mounted benchmark commands.

use anyhow::Result;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::time::Duration;

mod mounted;
mod profiling;
mod progress;
mod reporting;
mod result;
mod telemetry;
mod workload;

pub(crate) use profiling::merge_profile_report;
pub(crate) use reporting::{
    render_human_summary, render_public_bench_summary, save_benchmark_report,
};
pub(crate) use result::BenchmarkReport;
#[cfg(test)]
pub(crate) use result::sample_report;
pub(crate) use workload::{
    DEFAULT_SMALL_CLASS_MAX_BYTES, PUBLIC_BENCH_MEDIUM_MAX_BYTES, PUBLIC_BENCH_SMALL_MAX_BYTES,
    WorkloadSizeInput, parse_size_arg, public_bench_workload,
};

pub(crate) const CLEANUP_ATTEMPTS: usize = 5;
pub(crate) const CLEANUP_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum BenchmarkAccessPattern {
    Sequential,
    Random,
}

/// Complete benchmark engine configuration after CLI/default validation.
#[derive(Debug)]
pub(crate) struct BenchmarkConfig {
    pub(crate) command_name: &'static str,
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
    pub(crate) access_pattern: BenchmarkAccessPattern,
    pub(crate) during_upload_min_pending: usize,
    pub(crate) during_upload_timeout: Duration,
    pub(crate) l1_limit: u64,
    pub(crate) profile: bool,
    pub(crate) full_file_reads: bool,
    pub(crate) write_path_gate: bool,
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

/// Runs a mounted filesystem benchmark and returns the normalized benchmark report.
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

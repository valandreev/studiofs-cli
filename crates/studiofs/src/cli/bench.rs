use anyhow::{Context, Result, anyhow};
use clap::Args;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use crate::cli::benchmark_engine::{
    BenchmarkAccessPattern, BenchmarkConfig, DEFAULT_SMALL_CLASS_MAX_BYTES, MountedBenchmarkTarget,
    WorkloadSizeInput, parse_size_arg, render_human_summary, run_mounted, save_benchmark_report,
};
use crate::cli::test_cache::resolve_mounted_runtime;
use studiofs_cache::DEFAULT_MEMORY_CACHE_SIZE;

static BENCH_RUNTIME: OnceLock<std::io::Result<tokio::runtime::Runtime>> = OnceLock::new();

#[derive(Debug, Args)]
#[command(about = "Run a mounted StudioFS filesystem benchmark")]
pub(crate) struct BenchArgs {
    /// Mounted StudioFS path to benchmark.
    pub(crate) mountpoint: PathBuf,

    /// I/O block size used for large sequential file chunks.
    #[arg(long, default_value = "4MB")]
    pub(crate) block_size: String,

    /// Large sequential file size.
    #[arg(long, default_value = "1GB")]
    pub(crate) big_file_size: String,

    /// Size of each small file.
    #[arg(long, default_value = "128KB")]
    pub(crate) small_file_size: String,

    /// Number of small files.
    #[arg(long, default_value_t = 100)]
    pub(crate) small_file_count: usize,

    /// Number of concurrent worker streams.
    #[arg(long, default_value_t = 1)]
    pub(crate) threads: usize,

    /// JSON report output path. Defaults to an auto-named file in the
    /// current working directory.
    #[arg(long)]
    pub(crate) json_output: Option<PathBuf>,
}

impl BenchArgs {
    pub(crate) fn dispatch(self) -> Result<()> {
        run(self)
    }
}

fn bench_runtime() -> Result<&'static tokio::runtime::Runtime> {
    BENCH_RUNTIME
        .get_or_init(tokio::runtime::Runtime::new)
        .as_ref()
        .map_err(|error| anyhow!("create tokio runtime: {error}"))
}

fn build_bench_config(args: &BenchArgs, run_id: String) -> Result<BenchmarkConfig> {
    if args.threads == 0 {
        return Err(anyhow!("--threads must be >= 1"));
    }
    if args.small_file_count == 0 {
        return Err(anyhow!("--small-file-count must be >= 1"));
    }

    let block_size = parse_size_arg("block-size", &args.block_size)?;
    let big_file_size = parse_size_arg("big-file-size", &args.big_file_size)?;
    let small_file_size = parse_size_arg("small-file-size", &args.small_file_size)?;
    let mut sizes = Vec::with_capacity(args.small_file_count.saturating_add(1));
    sizes.push(big_file_size);
    sizes.extend(std::iter::repeat_n(small_file_size, args.small_file_count));

    Ok(BenchmarkConfig {
        target: args.mountpoint.clone(),
        block_size,
        read_size: block_size,
        size_class_small_max: small_file_size.max(DEFAULT_SMALL_CLASS_MAX_BYTES),
        size_class_medium_max: block_size.max(small_file_size.saturating_add(1)),
        workload_repeat: 1,
        concurrency: args.threads,
        sequential_streams: true,
        io_depth: args.threads,
        warmup_runs: 0,
        sizes: WorkloadSizeInput::ExplicitList(sizes),
        access_pattern: BenchmarkAccessPattern::Sequential,
        during_upload_min_pending: 1,
        during_upload_timeout: Duration::from_secs(10),
        l1_limit: DEFAULT_MEMORY_CACHE_SIZE,
        profile: false,
        full_file_reads: true,
        run_id,
    })
}

fn run(args: BenchArgs) -> Result<()> {
    let runtime = resolve_mounted_runtime(&args.mountpoint)?.ok_or_else(|| {
        anyhow!(
            "'{}' is not inside an active StudioFS mount",
            args.mountpoint.display()
        )
    })?;
    let config = build_bench_config(&args, uuid::Uuid::new_v4().to_string())?;
    let work_root = args.mountpoint.join(".studiofs-benchmark");
    let report = bench_runtime()?.block_on(run_mounted(
        &config,
        MountedBenchmarkTarget {
            pid: runtime.pid,
            volume: runtime.volume.as_str(),
            mount_point: runtime.mount_point.as_path(),
            work_root: work_root.as_path(),
            cache_path: runtime.cache_path.as_deref(),
        },
    ))?;

    print!("{}", render_human_summary(&report));
    let report_path = save_benchmark_report(&report, args.json_output.as_deref())
        .context("save benchmark JSON report")?;
    println!("JSON report: {}", report_path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_config_plans_one_large_file_and_requested_small_files() {
        let args = BenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4MB".to_string(),
            big_file_size: "2GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 3,
            threads: 4,
            json_output: None,
        };

        let config = build_bench_config(&args, "run-1".to_string()).unwrap();

        assert_eq!(config.block_size, 4 * 1024 * 1024);
        assert_eq!(config.read_size, 4 * 1024 * 1024);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.io_depth, 4);
        assert!(config.sequential_streams);
        match config.sizes {
            WorkloadSizeInput::ExplicitList(sizes) => {
                assert_eq!(
                    sizes,
                    vec![2 * 1024 * 1024 * 1024, 128 * 1024, 128 * 1024, 128 * 1024]
                );
            }
            other => panic!("expected explicit bench workload, got {other:?}"),
        }
    }

    #[test]
    fn bench_config_rejects_zero_threads() {
        let args = BenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4MB".to_string(),
            big_file_size: "1GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 100,
            threads: 0,
            json_output: None,
        };

        let err = build_bench_config(&args, "run-1".to_string()).unwrap_err();

        assert!(err.to_string().contains("--threads must be >= 1"));
    }
}

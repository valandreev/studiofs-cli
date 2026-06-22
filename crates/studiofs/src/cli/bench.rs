use anyhow::{Context, Result, anyhow};
use clap::Args;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::cli::benchmark_engine::{
    BenchmarkAccessPattern, BenchmarkConfig, DEFAULT_SMALL_CLASS_MAX_BYTES, MountedBenchmarkTarget,
    WorkloadSizeInput, parse_size_arg, render_human_summary, run_mounted, save_benchmark_report,
};
use crate::cli::test_cache::resolve_mounted_runtime;
use studiofs_cache::DEFAULT_MEMORY_CACHE_SIZE;

const MAX_BENCH_SMALL_FILE_COUNT: usize = 1_000_000;
const MAX_BENCH_THREADS: usize = 512;
const DEFAULT_BENCH_BLOCK_SIZE: &str = "4MB";
const DEFAULT_BENCH_BIG_FILE_SIZE: &str = "1GB";
const DEFAULT_BENCH_SMALL_FILE_SIZE: &str = "128KB";
const DEFAULT_BENCH_SMALL_FILE_COUNT: usize = 100;
const DEFAULT_BENCH_THREADS: usize = 1;

enum BenchRunOutcome {
    Completed(Box<crate::cli::benchmark_engine::BenchmarkReport>),
    Interrupted,
}

#[derive(Debug, Args)]
#[command(about = "Run a mounted StudioFS filesystem benchmark")]
pub(crate) struct BenchArgs {
    /// Mounted StudioFS path to benchmark.
    pub(crate) mountpoint: PathBuf,
}

impl BenchArgs {
    pub(crate) fn dispatch(self) -> Result<()> {
        run(self.into_devbench_args(), "studiofs bench")
    }

    fn into_devbench_args(self) -> DevbenchArgs {
        DevbenchArgs {
            mountpoint: self.mountpoint,
            block_size: DEFAULT_BENCH_BLOCK_SIZE.to_string(),
            big_file_size: DEFAULT_BENCH_BIG_FILE_SIZE.to_string(),
            small_file_size: DEFAULT_BENCH_SMALL_FILE_SIZE.to_string(),
            small_file_count: DEFAULT_BENCH_SMALL_FILE_COUNT,
            threads: DEFAULT_BENCH_THREADS,
            json_output: None,
        }
    }
}

#[derive(Debug, Args)]
#[command(about = "Run a configurable mounted StudioFS developer benchmark")]
pub(crate) struct DevbenchArgs {
    /// Mounted StudioFS path to benchmark.
    pub(crate) mountpoint: PathBuf,

    /// I/O block size used for large sequential file chunks.
    #[arg(long, default_value = DEFAULT_BENCH_BLOCK_SIZE)]
    pub(crate) block_size: String,

    /// Large sequential file size.
    #[arg(long, default_value = DEFAULT_BENCH_BIG_FILE_SIZE)]
    pub(crate) big_file_size: String,

    /// Size of each small file.
    #[arg(long, default_value = DEFAULT_BENCH_SMALL_FILE_SIZE)]
    pub(crate) small_file_size: String,

    /// Number of small files.
    #[arg(
        long,
        default_value_t = DEFAULT_BENCH_SMALL_FILE_COUNT,
        value_parser = parse_small_file_count_arg
    )]
    pub(crate) small_file_count: usize,

    /// Number of concurrent worker streams.
    #[arg(
        long,
        default_value_t = DEFAULT_BENCH_THREADS,
        value_parser = parse_threads_arg
    )]
    pub(crate) threads: usize,

    /// JSON report output path. Defaults to an auto-named file under target/perf/.
    /// Existing directories or paths ending with a separator receive an auto-named file;
    /// other paths are treated as explicit files.
    #[arg(long)]
    pub(crate) json_output: Option<PathBuf>,
}

impl DevbenchArgs {
    pub(crate) fn dispatch(self) -> Result<()> {
        run(self, "studiofs devbench")
    }
}

fn parse_small_file_count_arg(raw: &str) -> std::result::Result<usize, String> {
    parse_bounded_usize_arg(raw, 1, MAX_BENCH_SMALL_FILE_COUNT)
}

fn parse_threads_arg(raw: &str) -> std::result::Result<usize, String> {
    parse_bounded_usize_arg(raw, 1, MAX_BENCH_THREADS)
}

fn parse_bounded_usize_arg(
    raw: &str,
    min: usize,
    max: usize,
) -> std::result::Result<usize, String> {
    let value = raw
        .parse::<usize>()
        .map_err(|error| format!("invalid integer: {error}"))?;
    if value < min {
        return Err(format!("must be >= {min}"));
    }
    if value > max {
        return Err(format!("must be <= {max}"));
    }
    Ok(value)
}

fn build_bench_config(
    args: &DevbenchArgs,
    command_name: &'static str,
    run_id: String,
) -> Result<BenchmarkConfig> {
    // Clap enforces these for CLI input; keep the checks here for direct callers.
    if args.threads == 0 {
        return Err(anyhow!("--threads must be >= 1"));
    }
    if args.threads > MAX_BENCH_THREADS {
        return Err(anyhow!("--threads must be <= {MAX_BENCH_THREADS}"));
    }
    if args.small_file_count == 0 {
        return Err(anyhow!("--small-file-count must be >= 1"));
    }
    if args.small_file_count > MAX_BENCH_SMALL_FILE_COUNT {
        return Err(anyhow!(
            "--small-file-count must be <= {MAX_BENCH_SMALL_FILE_COUNT}"
        ));
    }

    let block_size = parse_size_arg("block-size", &args.block_size)?;
    let big_file_size = parse_size_arg("big-file-size", &args.big_file_size)?;
    let small_file_size = parse_size_arg("small-file-size", &args.small_file_size)?;
    if big_file_size <= small_file_size {
        return Err(anyhow!(
            "--big-file-size must be greater than --small-file-size"
        ));
    }
    let mut sizes = Vec::with_capacity(args.small_file_count.saturating_add(1));
    sizes.push(big_file_size);
    sizes.extend(std::iter::repeat_n(small_file_size, args.small_file_count));
    let size_class_small_max = small_file_size.max(DEFAULT_SMALL_CLASS_MAX_BYTES);
    let size_class_medium_max = block_size
        .max(small_file_size.saturating_add(1))
        .max(size_class_small_max.saturating_add(1));

    Ok(BenchmarkConfig {
        command_name,
        target: args.mountpoint.clone(),
        block_size,
        read_size: block_size,
        size_class_small_max,
        size_class_medium_max,
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

fn run(args: DevbenchArgs, command_name: &'static str) -> Result<()> {
    let runtime = resolve_mounted_runtime(&args.mountpoint)?.ok_or_else(|| {
        anyhow!(
            "'{}' is not inside an active StudioFS mount",
            args.mountpoint.display()
        )
    })?;
    let config = build_bench_config(&args, command_name, uuid::Uuid::new_v4().to_string())?;
    let work_root = args.mountpoint.join(".studiofs-benchmark");
    let mut work_root_cleanup = BenchWorkRootCleanup::new(work_root.clone());
    let async_runtime =
        tokio::runtime::Runtime::new().map_err(|error| anyhow!("create tokio runtime: {error}"))?;
    let mounted_target = MountedBenchmarkTarget {
        pid: runtime.pid,
        volume: runtime.volume.as_str(),
        mount_point: runtime.mount_point.as_path(),
        work_root: work_root.as_path(),
        cache_path: runtime.cache_path.as_deref(),
    };
    let outcome = async_runtime.block_on(async {
        tokio::select! {
            report = run_mounted(&config, mounted_target) => {
                report.map(|report| BenchRunOutcome::Completed(Box::new(report)))
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for Ctrl+C")?;
                Ok(BenchRunOutcome::Interrupted)
            }
        }
    })?;
    drop(async_runtime);
    let report = match outcome {
        BenchRunOutcome::Completed(report) => *report,
        BenchRunOutcome::Interrupted => {
            return Err(anyhow!("benchmark interrupted by Ctrl+C"));
        }
    };
    print!("{}", render_human_summary(&report));
    for warning in &report.runtime_stats.warnings {
        eprintln!("Warning: {warning}");
    }
    let report_path = save_benchmark_report(&report, args.json_output.as_deref())
        .context("save benchmark JSON report")?;
    println!("JSON report: {}", report_path.display());
    work_root_cleanup.disarm();
    Ok(())
}

struct BenchWorkRootCleanup {
    work_root: PathBuf,
    armed: bool,
}

impl BenchWorkRootCleanup {
    fn new(work_root: PathBuf) -> Self {
        Self {
            work_root,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BenchWorkRootCleanup {
    fn drop(&mut self) {
        if self.armed {
            cleanup_bench_work_root(&self.work_root);
        }
    }
}

fn cleanup_bench_work_root(work_root: &Path) {
    const CLEANUP_ATTEMPTS: usize = 5;
    const CLEANUP_RETRY_DELAY: Duration = Duration::from_millis(100);

    for attempt in 1..=CLEANUP_ATTEMPTS {
        match std::fs::remove_dir_all(work_root) {
            Ok(()) => return,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) if attempt < CLEANUP_ATTEMPTS => {
                tracing::warn!(
                    error = %error,
                    work_root = %work_root.display(),
                    attempt,
                    "bench work root cleanup retrying"
                );
                std::thread::sleep(CLEANUP_RETRY_DELAY);
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    work_root = %work_root.display(),
                    "bench work root cleanup failed"
                );
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_config_plans_one_large_file_and_requested_small_files() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4MB".to_string(),
            big_file_size: "2GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 3,
            threads: 4,
            json_output: None,
        };

        let config = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap();

        assert_eq!(config.command_name, "studiofs devbench");
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
    fn bench_config_keeps_medium_class_above_small_class_for_tiny_files() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4KB".to_string(),
            big_file_size: "1GB".to_string(),
            small_file_size: "2KB".to_string(),
            small_file_count: 3,
            threads: 1,
            json_output: None,
        };

        let config = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap();

        assert_eq!(config.size_class_small_max, DEFAULT_SMALL_CLASS_MAX_BYTES);
        assert!(config.size_class_medium_max > config.size_class_small_max);
    }

    #[test]
    fn bench_config_rejects_zero_threads() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4MB".to_string(),
            big_file_size: "1GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 100,
            threads: 0,
            json_output: None,
        };

        let err = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap_err();

        assert!(err.to_string().contains("--threads must be >= 1"));
    }

    #[test]
    fn bench_config_rejects_excessive_threads() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4MB".to_string(),
            big_file_size: "1GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 100,
            threads: MAX_BENCH_THREADS + 1,
            json_output: None,
        };

        let err = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap_err();

        assert!(err.to_string().contains("--threads must be <= 512"));
    }

    #[test]
    fn bench_config_rejects_zero_small_file_count() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4MB".to_string(),
            big_file_size: "1GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 0,
            threads: 1,
            json_output: None,
        };

        let err = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap_err();

        assert!(err.to_string().contains("--small-file-count must be >= 1"));
    }

    #[test]
    fn bench_config_rejects_big_file_size_not_greater_than_small_file_size() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4MB".to_string(),
            big_file_size: "128KB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 100,
            threads: 1,
            json_output: None,
        };

        let err = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap_err();

        assert!(
            err.to_string()
                .contains("--big-file-size must be greater than --small-file-size")
        );
    }

    #[test]
    fn bench_config_allows_block_size_larger_than_big_file_size() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "2GB".to_string(),
            big_file_size: "1GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 100,
            threads: 1,
            json_output: None,
        };

        let config = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap();

        assert_eq!(config.block_size, 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn bench_config_rejects_excessive_small_file_count() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4MB".to_string(),
            big_file_size: "1GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: MAX_BENCH_SMALL_FILE_COUNT + 1,
            threads: 1,
            json_output: None,
        };

        let err = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap_err();

        assert!(
            err.to_string()
                .contains("--small-file-count must be <= 1000000")
        );
    }

    #[test]
    fn bench_config_rejects_invalid_size_arg() {
        let args = DevbenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            block_size: "4not-a-size".to_string(),
            big_file_size: "1GB".to_string(),
            small_file_size: "128KB".to_string(),
            small_file_count: 100,
            threads: 1,
            json_output: None,
        };

        let err = build_bench_config(&args, "studiofs devbench", "run-1".to_string()).unwrap_err();

        assert!(err.to_string().contains("invalid block-size"));
    }

    #[test]
    fn cleanup_guard_removes_work_root_when_not_disarmed() {
        let temp_dir = studiofs_test_support::workspace_target_tempdir("bench-cleanup-");
        let work_root = temp_dir.path().join(".studiofs-benchmark");
        std::fs::create_dir(&work_root).unwrap();

        {
            let _guard = BenchWorkRootCleanup::new(work_root.clone());
        }

        assert!(!work_root.exists());
    }

    #[test]
    fn cleanup_guard_preserves_work_root_when_disarmed() {
        let temp_dir = studiofs_test_support::workspace_target_tempdir("bench-cleanup-");
        let work_root = temp_dir.path().join(".studiofs-benchmark");
        std::fs::create_dir(&work_root).unwrap();

        {
            let mut guard = BenchWorkRootCleanup::new(work_root.clone());
            guard.disarm();
        }

        assert!(work_root.exists());
    }
}

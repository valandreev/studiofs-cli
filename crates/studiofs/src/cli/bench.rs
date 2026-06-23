use anyhow::{Context, Result, anyhow};
use clap::Args;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::cli::benchmark_engine::{
    BenchmarkAccessPattern, BenchmarkConfig, BenchmarkReport, CLEANUP_ATTEMPTS,
    CLEANUP_RETRY_DELAY, DEFAULT_SMALL_CLASS_MAX_BYTES, MountedBenchmarkTarget,
    PUBLIC_BENCH_MEDIUM_MAX_BYTES, PUBLIC_BENCH_SMALL_MAX_BYTES, WorkloadSizeInput, parse_size_arg,
    public_bench_workload, render_human_summary, render_public_bench_summary, run_mounted,
    save_benchmark_report,
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
    Completed(Box<BenchmarkReport>),
    Interrupted,
}

#[derive(Debug, Args)]
#[command(about = "Run a mounted StudioFS filesystem benchmark")]
pub(crate) struct BenchArgs {
    /// Mounted StudioFS path to benchmark.
    pub(crate) mountpoint: PathBuf,

    /// JSON report output path.
    #[arg(long = "report")]
    pub(crate) json_output: Option<PathBuf>,
}

impl BenchArgs {
    pub(crate) fn dispatch(self) -> Result<()> {
        run_bench(self)
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

fn build_public_bench_config(args: &BenchArgs, run_id: String) -> BenchmarkConfig {
    let workload = public_bench_workload();
    let sizes = workload.file_sizes();
    BenchmarkConfig {
        command_name: "studiofs bench",
        target: args.mountpoint.clone(),
        block_size: PUBLIC_BENCH_BLOCK_SIZE,
        read_size: PUBLIC_BENCH_BLOCK_SIZE,
        size_class_small_max: PUBLIC_BENCH_SMALL_MAX_BYTES,
        size_class_medium_max: PUBLIC_BENCH_MEDIUM_MAX_BYTES,
        workload_repeat: 1,
        concurrency: DEFAULT_BENCH_THREADS,
        sequential_streams: false,
        io_depth: DEFAULT_BENCH_THREADS,
        warmup_runs: 0,
        sizes: WorkloadSizeInput::ExplicitList(sizes),
        access_pattern: BenchmarkAccessPattern::Random,
        during_upload_min_pending: 1,
        during_upload_timeout: Duration::from_secs(10),
        l1_limit: DEFAULT_MEMORY_CACHE_SIZE,
        profile: false,
        full_file_reads: false,
        run_id,
    }
}

const PUBLIC_BENCH_BLOCK_SIZE: u64 = 1024 * 1024;

fn run_bench(args: BenchArgs) -> Result<()> {
    let config = build_public_bench_config(&args, uuid::Uuid::new_v4().to_string());
    let report = run_config(config)?;
    finish_report(
        &report,
        public_report_output(args.json_output.as_deref()),
        print_public_bench_report,
    )
}

fn run(args: DevbenchArgs, command_name: &'static str) -> Result<()> {
    let config = build_bench_config(&args, command_name, uuid::Uuid::new_v4().to_string())?;
    let report = run_config(config)?;
    finish_report(
        &report,
        devbench_report_output(args.json_output.as_deref()),
        print_benchmark_report,
    )
}

fn run_config(config: BenchmarkConfig) -> Result<BenchmarkReport> {
    let runtime = resolve_mounted_runtime(&config.target)?.ok_or_else(|| {
        anyhow!(
            "'{}' is not inside an active StudioFS mount",
            config.target.display()
        )
    })?;
    let work_root = config.target.join(".studiofs-benchmark");
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
    work_root_cleanup.disarm();
    Ok(report)
}

fn finish_report(
    report: &BenchmarkReport,
    output: ReportOutput<'_>,
    print_report: impl FnOnce(&BenchmarkReport) -> Result<()>,
) -> Result<()> {
    finish_report_with_save(report, output, print_report, save_report_for_policy)
}

fn finish_report_with_save(
    report: &BenchmarkReport,
    output: ReportOutput<'_>,
    print_report: impl FnOnce(&BenchmarkReport) -> Result<()>,
    save_report: impl FnOnce(&BenchmarkReport, ReportOutput<'_>) -> Result<()>,
) -> Result<()> {
    let print_result = print_report(report);
    let save_result = save_report(report, output);
    save_result?;
    print_result
}

fn print_benchmark_report(report: &BenchmarkReport) -> Result<()> {
    write_stdout(&render_human_summary(report))?;
    for warning in &report.runtime_stats.warnings {
        eprintln!("Warning: {warning}");
    }
    Ok(())
}

fn print_public_bench_report(report: &BenchmarkReport) -> Result<()> {
    write_stdout(&render_public_bench_summary(report))
}

fn write_stdout(output: &str) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write_output(output.as_bytes(), &mut stdout)
}

fn write_output(output: &[u8], writer: &mut impl Write) -> Result<()> {
    match writer.write_all(output).and_then(|()| writer.flush()) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        Err(error) => Err(error).context("write benchmark summary to stdout"),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ReportOutput<'a> {
    Skip,
    Save(Option<&'a Path>),
}

fn public_report_output(json_output: Option<&Path>) -> ReportOutput<'_> {
    match json_output {
        Some(path) => ReportOutput::Save(Some(path)),
        None => ReportOutput::Skip,
    }
}

fn devbench_report_output(json_output: Option<&Path>) -> ReportOutput<'_> {
    ReportOutput::Save(json_output)
}

fn save_report_for_policy(report: &BenchmarkReport, output: ReportOutput<'_>) -> Result<()> {
    let ReportOutput::Save(json_output) = output else {
        return Ok(());
    };
    let report_path =
        save_benchmark_report(report, json_output).context("save benchmark JSON report")?;
    println!("JSON report: {}", report_path.display());
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
    use crate::cli::benchmark_engine::sample_report;
    use std::io;

    struct BrokenPipeWriter;

    struct FlushBrokenPipeWriter {
        wrote: bool,
    }

    impl Write for BrokenPipeWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Write for FlushBrokenPipeWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.wrote = true;
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe"))
        }
    }

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
    fn public_bench_config_uses_fixed_pseudo_random_workload() {
        let args = BenchArgs {
            mountpoint: PathBuf::from("Z:\\"),
            json_output: None,
        };

        let config = build_public_bench_config(&args, "run-1".to_string());

        assert_eq!(config.command_name, "studiofs bench");
        assert_eq!(config.block_size, 1024 * 1024);
        assert_eq!(config.read_size, 1024 * 1024);
        assert_eq!(config.workload_repeat, 1);
        assert_eq!(config.concurrency, 1);
        assert!(!config.sequential_streams);
        assert_eq!(config.access_pattern, BenchmarkAccessPattern::Random);
        assert!(!config.full_file_reads);
        match config.sizes {
            WorkloadSizeInput::ExplicitList(sizes) => {
                assert_eq!(sizes.len(), 1081);
                assert_eq!(
                    &sizes[..8],
                    &[
                        139_264, 89_088, 6_144, 11_264, 49_152, 38_912, 232_448, 102_400
                    ]
                );
                assert_eq!(sizes[1080], 4_215_275_520);
            }
            other => panic!("expected fixed public bench workload, got {other:?}"),
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

    #[test]
    fn public_bench_skips_report_saving_when_no_report_path_is_requested() {
        assert_eq!(public_report_output(None), ReportOutput::Skip);
    }

    #[test]
    fn public_bench_saves_report_when_report_path_is_requested() {
        let report_path = Path::new("reports/bench.json");

        assert_eq!(
            public_report_output(Some(report_path)),
            ReportOutput::Save(Some(report_path))
        );
    }

    #[test]
    fn devbench_keeps_default_report_saving_when_no_output_path_is_requested() {
        assert_eq!(devbench_report_output(None), ReportOutput::Save(None));
    }

    #[test]
    fn benchmark_summary_write_treats_broken_pipe_as_clean_exit() {
        let mut writer = BrokenPipeWriter;

        write_output(b"StudioFS bench result\n", &mut writer).unwrap();
    }

    #[test]
    fn benchmark_summary_write_treats_flush_broken_pipe_as_clean_exit() {
        let mut writer = FlushBrokenPipeWriter { wrote: false };

        write_output(b"StudioFS bench result\n", &mut writer).unwrap();

        assert!(writer.wrote);
    }

    #[test]
    fn finish_report_attempts_save_even_when_printing_fails() {
        let report = sample_report();
        let mut saved = false;

        let err = finish_report_with_save(
            &report,
            ReportOutput::Skip,
            |_| Err(anyhow!("stdout failed")),
            |_, _| {
                saved = true;
                Ok(())
            },
        )
        .unwrap_err();

        assert!(saved);
        assert!(err.to_string().contains("stdout failed"));
    }
}

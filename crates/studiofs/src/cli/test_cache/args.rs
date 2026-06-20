use crate::cli::benchmark_profile_types::BenchmarkProfileMode;
use clap::{Args, ValueEnum};
use serde::Serialize;
use std::path::PathBuf;

pub(crate) struct TestCacheRunRequest {
    pub(super) target: PathBuf,
    pub(super) block_size: Option<String>,
    pub(super) read_size: Option<String>,
    pub(super) small_max: Option<String>,
    pub(super) medium_max: Option<String>,
    pub(super) workload_repeat: usize,
    pub(super) concurrency: usize,
    pub(super) sequential_streams: bool,
    pub(super) io_depth: usize,
    pub(super) warmup_runs: u32,
    pub(super) sizes: Option<String>,
    pub(super) access_pattern: TestCacheAccessPattern,
    pub(super) during_upload_min_pending: usize,
    pub(super) during_upload_timeout_ms: u64,
    pub(super) l1_limit: Option<String>,
    pub(super) profile: Option<BenchmarkProfileMode>,
    pub(super) json_output: Option<PathBuf>,
    pub(super) full_file_reads: bool,
}

impl From<TestCacheArgs> for TestCacheRunRequest {
    fn from(args: TestCacheArgs) -> Self {
        Self {
            target: args.path,
            block_size: args.block_size,
            read_size: args.read_size,
            small_max: args.small_max,
            medium_max: args.medium_max,
            workload_repeat: args.workload_repeat,
            concurrency: args.concurrency,
            sequential_streams: args.sequential_streams,
            io_depth: args.io_depth,
            warmup_runs: args.warmup_runs,
            sizes: args.sizes,
            access_pattern: args.access_pattern,
            during_upload_min_pending: args.during_upload_min_pending,
            during_upload_timeout_ms: args.during_upload_timeout_ms,
            l1_limit: args.l1_limit,
            profile: args.profile,
            json_output: args.json_output,
            full_file_reads: args.full_file_reads,
        }
    }
}

#[derive(Debug, Args)]
#[command(about = "Run cache benchmarks (direct shim and mounted end-to-end)")]
pub(crate) struct TestCacheArgs {
    /// Target benchmark path.
    ///
    /// If path is inside an active StudioFS mount, benchmark runs end-to-end
    /// through that mounted process and stores temporary files under
    /// `<path>/.studiofs-benchmark`. Otherwise benchmark runs direct-shim mode
    /// and treats this path as cache directory.
    path: PathBuf,

    /// Payload object size (e.g. 1MB, 4MB, 16MB, 32MB). Defaults to 16MB.
    #[arg(long)]
    block_size: Option<String>,

    /// Read chunk size (e.g. 4KB, 1MB). Defaults to block-size.
    #[arg(long)]
    read_size: Option<String>,

    /// Upper bound for the `small` size class (inclusive), e.g. 10KB.
    #[arg(long)]
    small_max: Option<String>,

    /// Upper bound for the `medium` size class (inclusive), e.g. 16MB.
    #[arg(long)]
    medium_max: Option<String>,

    /// Repeat the generated workload this many times.
    #[arg(long, default_value_t = 1)]
    workload_repeat: usize,

    /// Number of concurrent streams.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// Run each stream sequentially across its assigned segment.
    #[arg(long)]
    sequential_streams: bool,

    /// Maximum in-flight I/O operations.
    #[arg(long, default_value_t = 32)]
    io_depth: usize,

    /// Number of warm-up cycles executed before metrics collection.
    #[arg(long, default_value_t = 0)]
    warmup_runs: u32,

    /// Workload size profile:
    /// - percentages: `small=60,medium=30,large=10`
    /// - explicit sizes: `4KB,128KB,1MB,16MB`
    #[arg(long)]
    sizes: Option<String>,

    /// Access pattern: `sequential`, `random`, or `mixed`.
    #[arg(long, value_enum, default_value_t = TestCacheAccessPattern::Sequential)]
    access_pattern: TestCacheAccessPattern,

    /// Minimum pending uploads required for the `during-upload-read` phase to count as observed.
    #[arg(long, default_value_t = 1)]
    during_upload_min_pending: usize,

    /// Timeout in milliseconds while waiting for active uploads before `during-upload-read`.
    #[arg(long, default_value_t = 10_000)]
    during_upload_timeout_ms: u64,

    /// In-memory L1 cache limit (e.g. 2GB).
    #[arg(long)]
    l1_limit: Option<String>,

    /// JSON report output path. Defaults to an auto-named file in the
    /// current working directory.
    #[arg(long)]
    json_output: Option<PathBuf>,

    /// Read each generated file fully instead of sampling one read-size chunk.
    #[arg(long)]
    full_file_reads: bool,

    /// Enable on-demand profiling for this benchmark run.
    #[arg(long, value_enum, num_args = 0..=1, default_missing_value = "pyroscope")]
    profile: Option<BenchmarkProfileMode>,
}

impl TestCacheArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        super::run(self.into())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::benchmark_profile_types::BenchmarkProfileMode;
    use std::path::PathBuf;

    #[test]
    fn test_cache_parses_defaults() {
        let cli = crate::cli::parse_for_test(&["test-cache", "/tmp/studiofs-cache"]).unwrap();

        match cli.command {
            crate::cli::Commands::TestCache(TestCacheArgs {
                path,
                concurrency,
                sequential_streams,
                io_depth,
                warmup_runs,
                sizes,
                block_size,
                read_size,
                small_max,
                medium_max,
                workload_repeat,
                access_pattern,
                l1_limit,
                json_output,
                full_file_reads,
                profile,
                during_upload_min_pending,
                during_upload_timeout_ms,
            }) => {
                assert_eq!(path, PathBuf::from("/tmp/studiofs-cache"));
                assert_eq!(workload_repeat, 1);
                assert_eq!(concurrency, 1);
                assert!(!sequential_streams);
                assert_eq!(io_depth, 32);
                assert_eq!(warmup_runs, 0);
                assert_eq!(sizes.as_deref(), None);
                assert_eq!(block_size.as_deref(), None);
                assert_eq!(read_size.as_deref(), None);
                assert_eq!(small_max.as_deref(), None);
                assert_eq!(medium_max.as_deref(), None);
                assert!(matches!(access_pattern, TestCacheAccessPattern::Sequential));
                assert_eq!(during_upload_min_pending, 1);
                assert_eq!(during_upload_timeout_ms, 10_000);
                assert!(l1_limit.is_none());
                assert!(json_output.is_none());
                assert!(!full_file_reads);
                assert!(profile.is_none());
            }
            other => panic!("expected TestCache, got {other:?}"),
        }
    }

    #[test]
    fn test_cache_parses_full_flags() {
        let cli = crate::cli::parse_for_test(&[
            "test-cache",
            "Z:\\bench",
            "--block-size",
            "4MB",
            "--read-size",
            "1MB",
            "--small-max",
            "64KB",
            "--medium-max",
            "16MB",
            "--concurrency",
            "16",
            "--sequential-streams",
            "--io-depth",
            "64",
            "--warmup-runs",
            "2",
            "--sizes",
            "small=60,medium=30,large=10",
            "--access-pattern",
            "random",
            "--l1-limit",
            "4GB",
            "--json-output",
            "reports/cache-run.json",
            "--full-file-reads",
            "--profile=pyroscope",
        ])
        .unwrap();

        match cli.command {
            crate::cli::Commands::TestCache(TestCacheArgs {
                path,
                block_size,
                read_size,
                small_max,
                medium_max,
                workload_repeat,
                access_pattern,
                concurrency,
                sequential_streams,
                io_depth,
                warmup_runs,
                sizes,
                during_upload_min_pending,
                during_upload_timeout_ms,
                l1_limit,
                json_output,
                full_file_reads,
                profile,
            }) => {
                assert_eq!(path, PathBuf::from("Z:\\bench"));
                assert_eq!(block_size.unwrap(), "4MB");
                assert_eq!(read_size.unwrap(), "1MB");
                assert_eq!(small_max.unwrap(), "64KB");
                assert_eq!(medium_max.unwrap(), "16MB");
                assert_eq!(workload_repeat, 1);
                assert!(matches!(access_pattern, TestCacheAccessPattern::Random));
                assert_eq!(concurrency, 16);
                assert!(sequential_streams);
                assert_eq!(io_depth, 64);
                assert_eq!(warmup_runs, 2);
                assert_eq!(sizes.unwrap(), "small=60,medium=30,large=10");
                assert_eq!(during_upload_min_pending, 1);
                assert_eq!(during_upload_timeout_ms, 10_000);
                assert_eq!(l1_limit.unwrap(), "4GB");
                assert_eq!(
                    json_output.unwrap(),
                    PathBuf::from("reports/cache-run.json")
                );
                assert!(full_file_reads);
                assert!(matches!(profile, Some(BenchmarkProfileMode::Pyroscope)));
            }
            other => panic!("expected TestCache, got {other:?}"),
        }
    }

    #[test]
    fn test_cache_parses_profile_shorthand() {
        let cli = crate::cli::parse_for_test(&["test-cache", "/tmp/studiofs-cache", "--profile"])
            .unwrap();

        match cli.command {
            crate::cli::Commands::TestCache(TestCacheArgs { profile, .. }) => {
                assert!(matches!(profile, Some(BenchmarkProfileMode::Pyroscope)));
            }
            other => panic!("expected TestCache, got {other:?}"),
        }
    }

    #[test]
    fn test_cache_parses_workload_and_during_upload_overrides() {
        let cli = crate::cli::parse_for_test(&[
            "test-cache",
            "Z:\\bench",
            "--workload-repeat",
            "12",
            "--during-upload-min-pending",
            "4",
            "--during-upload-timeout-ms",
            "30000",
        ])
        .unwrap();

        match cli.command {
            crate::cli::Commands::TestCache(TestCacheArgs {
                workload_repeat,
                during_upload_min_pending,
                during_upload_timeout_ms,
                ..
            }) => {
                assert_eq!(workload_repeat, 12);
                assert_eq!(during_upload_min_pending, 4);
                assert_eq!(during_upload_timeout_ms, 30_000);
            }
            other => panic!("expected TestCache, got {other:?}"),
        }
    }
}

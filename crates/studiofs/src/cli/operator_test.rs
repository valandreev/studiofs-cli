use anyhow::{Context, Result, anyhow};
use std::path::PathBuf;
use std::time::Duration;

use crate::cli::WRITE_PATH_OBSERVABILITY_FILE;
use crate::cli::benchmark_engine::{
    BenchmarkAccessPattern, BenchmarkConfig, DEFAULT_SMALL_CLASS_MAX_BYTES, MountedBenchmarkTarget,
    WorkloadSizeInput, merge_profile_report, render_human_summary, run_mounted,
    save_benchmark_report,
};
use crate::cli::benchmark_profile::BenchmarkProfileSession;
use crate::cli::benchmark_profile_types::{BenchmarkProfileMode, BenchmarkProfileResult};
use crate::cli::mounted_runtime::resolve_mounted_runtime;
use studiofs_cache::DEFAULT_MEMORY_CACHE_SIZE;

const OPERATOR_TEST_BLOCK_SIZE: u64 = 4 * 1024 * 1024;
const OPERATOR_TEST_SIZE: u64 = 8 * 1024 * 1024 * 1024;

pub(crate) fn run_operator_test(target: PathBuf, report: PathBuf) -> Result<()> {
    let runtime = resolve_mounted_runtime(&target)?.ok_or_else(|| {
        anyhow!(
            "'{}' is not inside an active StudioFS mount",
            target.display()
        )
    })?;
    let cache_label = runtime
        .cache_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "unpublished".to_string());
    eprintln!(
        "StudioFS test target: mount={} volume={} pid={} cache={}",
        runtime.mount_point.display(),
        runtime.volume,
        runtime.pid,
        cache_label
    );

    let run_id = uuid::Uuid::new_v4().to_string();
    let config = BenchmarkConfig {
        command_name: "studiofs test",
        target: target.clone(),
        block_size: OPERATOR_TEST_BLOCK_SIZE,
        read_size: OPERATOR_TEST_BLOCK_SIZE,
        size_class_small_max: DEFAULT_SMALL_CLASS_MAX_BYTES,
        size_class_medium_max: OPERATOR_TEST_BLOCK_SIZE,
        workload_repeat: 1,
        concurrency: 1,
        sequential_streams: false,
        io_depth: 32,
        warmup_runs: 0,
        sizes: WorkloadSizeInput::ExplicitList(vec![OPERATOR_TEST_SIZE]),
        access_pattern: BenchmarkAccessPattern::Sequential,
        during_upload_min_pending: 1,
        during_upload_timeout: Duration::from_millis(120_000),
        l1_limit: DEFAULT_MEMORY_CACHE_SIZE,
        profile: true,
        full_file_reads: true,
        write_path_gate: false,
        run_id: run_id.clone(),
    };

    let work_root = target.join(".studiofs-benchmark");
    let write_path_observability_path = runtime
        .cache_path
        .as_ref()
        .map(|path| path.join(WRITE_PATH_OBSERVABILITY_FILE));
    let report_dir = match report
        .parent()
        .filter(|parent| parent.components().next().is_some())
    {
        Some(parent) => parent.to_path_buf(),
        None => std::env::current_dir().context("resolve current directory")?,
    };
    let profile_session = BenchmarkProfileSession::start(
        Some(BenchmarkProfileMode::Pyroscope),
        &run_id,
        "e2e",
        write_path_observability_path,
    )?;

    let mounted_target = MountedBenchmarkTarget {
        pid: runtime.pid,
        volume: runtime.volume.as_str(),
        mount_point: runtime.mount_point.as_path(),
        work_root: work_root.as_path(),
        cache_path: runtime.cache_path.as_deref(),
    };
    let rt = tokio::runtime::Runtime::new().context("create tokio runtime")?;
    let report_data = rt.block_on(run_mounted(&config, mounted_target))?;
    let profile_report = match profile_session.map(|session| session.finish(&report_dir)) {
        Some(Ok(report)) => Some(report),
        Some(Err(err)) => {
            tracing::warn!(
                target: "studiofs.cache.profile",
                run_id = %run_id,
                error = %err,
                "profile session failed to finish; test report will still be written"
            );
            Some(BenchmarkProfileResult {
                endpoint: None,
                artifacts: Vec::new(),
                warnings: vec![format!(
                    "profile session failed to finish: {err:#}; benchmark metrics are still valid"
                )],
            })
        }
        None => None,
    };
    let report_data = merge_profile_report(report_data, profile_report);

    print!("{}", render_human_summary(&report_data));
    let report_path = save_benchmark_report(&report_data, Some(&report))?;
    println!("JSON report: {}", report_path.display());
    Ok(())
}

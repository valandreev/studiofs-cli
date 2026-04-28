//! `studiofs test-cache` — benchmark the StudioFS data path.
//!
//! Supports:
//! - direct local-cache benchmark mode (`direct-shim`)
//! - end-to-end benchmark mode (`e2e`) that runs copy/read on a mounted volume

mod args;
mod direct;
mod mode;
mod mounted;
mod profiling;
mod progress;
mod reporting;
mod result;
mod telemetry;
mod workload;

use crate::cli::WRITE_PATH_OBSERVABILITY_FILE;
use crate::cli::test_cache_profile::TestCacheProfileSession;
use crate::cli::test_cache_profile_types::{TestCacheProfileMode, TestCacheProfileResult};
use anyhow::{Context, Result, anyhow};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use studiofs_cache::DEFAULT_MEMORY_CACHE_SIZE;

pub use args::TestCacheAccessPattern;
pub(crate) use args::TestCacheArgs;

use args::TestCacheRunRequest;
use direct::run_direct;
use mode::{ResolvedRunMode, resolve_run_mode};
use mounted::run_mounted_end_to_end;
use profiling::merge_profile_report;
use reporting::{render_human_summary, save_benchmark_report};
use workload::{
    DEFAULT_SMALL_CLASS_MAX_BYTES, DEFAULT_TEST_CACHE_BLOCK_SIZE_STR, WorkloadSizeInput,
    parse_size_arg, parse_sizes_arg,
};

static TEST_CACHE_RUNTIME: OnceLock<std::io::Result<tokio::runtime::Runtime>> = OnceLock::new();

#[derive(Debug, Clone)]
pub(crate) struct MountedRuntimeInfo {
    pub(crate) pid: u32,
    pub(crate) volume: String,
    pub(crate) mount_point: PathBuf,
    pub(crate) cache_path: Option<PathBuf>,
}

pub(crate) fn resolve_mounted_runtime(target: &Path) -> Result<Option<MountedRuntimeInfo>> {
    match resolve_run_mode(target)? {
        ResolvedRunMode::MountedE2E {
            pid,
            volume,
            mount_point,
            cache_path,
            ..
        } => Ok(Some(MountedRuntimeInfo {
            pid,
            volume,
            mount_point,
            cache_path,
        })),
        ResolvedRunMode::DirectShim { .. } => Ok(None),
    }
}

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
    run(TestCacheRunRequest {
        target,
        block_size: Some("4MB".to_string()),
        read_size: Some("4MB".to_string()),
        small_max: None,
        medium_max: None,
        workload_repeat: 1,
        concurrency: 1,
        sequential_streams: false,
        io_depth: 32,
        warmup_runs: 0,
        sizes: Some("8GB".to_string()),
        access_pattern: TestCacheAccessPattern::Sequential,
        during_upload_min_pending: 1,
        during_upload_timeout_ms: 120_000,
        l1_limit: None,
        profile: Some(TestCacheProfileMode::Pyroscope),
        json_output: Some(report),
        full_file_reads: true,
    })
}

#[derive(Debug)]
struct TestCacheParams {
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
    during_upload_timeout: std::time::Duration,
    l1_limit: u64,
    profile: bool,
    full_file_reads: bool,
    run_id: String,
}

fn test_cache_runtime() -> Result<&'static tokio::runtime::Runtime> {
    TEST_CACHE_RUNTIME
        .get_or_init(tokio::runtime::Runtime::new)
        .as_ref()
        .map_err(|error| anyhow!("create tokio runtime: {error}"))
}

fn resolve_report_dir(json_output: Option<&Path>, current_dir: &Path) -> PathBuf {
    match json_output.and_then(Path::parent) {
        Some(parent) if parent.components().next().is_some() => parent.to_path_buf(),
        _ => current_dir.to_path_buf(),
    }
}

fn run(request: TestCacheRunRequest) -> Result<()> {
    let TestCacheRunRequest {
        target,
        block_size,
        read_size,
        small_max,
        medium_max,
        workload_repeat,
        concurrency,
        sequential_streams,
        io_depth,
        warmup_runs,
        sizes,
        access_pattern,
        during_upload_min_pending,
        during_upload_timeout_ms,
        l1_limit,
        profile,
        json_output,
        full_file_reads,
    } = request;
    if io_depth == 0 {
        return Err(anyhow!("--io-depth must be >= 1"));
    }
    if concurrency == 0 {
        return Err(anyhow!("--concurrency must be >= 1"));
    }
    if workload_repeat == 0 {
        return Err(anyhow!("--workload-repeat must be >= 1"));
    }
    if during_upload_min_pending == 0 {
        return Err(anyhow!("--during-upload-min-pending must be >= 1"));
    }
    if during_upload_timeout_ms == 0 {
        return Err(anyhow!("--during-upload-timeout-ms must be >= 1"));
    }
    let block_size = parse_size_arg(
        "block-size",
        block_size
            .as_deref()
            .unwrap_or(DEFAULT_TEST_CACHE_BLOCK_SIZE_STR),
    )?;
    let block_size_default = block_size.to_string();
    let l1_limit_default = DEFAULT_MEMORY_CACHE_SIZE.to_string();
    let small_max_default = DEFAULT_SMALL_CLASS_MAX_BYTES.to_string();
    let medium_max_default = block_size.to_string();
    let read_size = parse_size_arg(
        "read-size",
        read_size
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&block_size_default),
    )?
    .max(1);
    let l1_limit = parse_size_arg("l1-limit", l1_limit.as_deref().unwrap_or(&l1_limit_default))?;
    let size_class_small_max = parse_size_arg(
        "small-max",
        small_max
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&small_max_default),
    )?;
    let size_class_medium_max = parse_size_arg(
        "medium-max",
        medium_max
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(&medium_max_default),
    )?;
    let sizes = parse_sizes_arg(sizes.as_deref())?;

    if l1_limit < 1 {
        return Err(anyhow!("cache limit must be > 0"));
    }
    if size_class_small_max == 0 {
        return Err(anyhow!("--small-max must be > 0"));
    }
    if size_class_medium_max <= size_class_small_max {
        return Err(anyhow!("--medium-max must be greater than --small-max"));
    }
    if sequential_streams {
        let effective = concurrency.min(io_depth.max(1));
        tracing::warn!(
            target: "studiofs.cache.profile",
            requested_concurrency = concurrency,
            requested_io_depth = io_depth,
            effective_inflight_io = effective,
            "sequential-streams mode processes each stream serially; effective in-flight IO is bounded by min(concurrency, io-depth)"
        );
    }

    let run_id = uuid::Uuid::new_v4().to_string();
    tracing::info!(
        target: "studiofs.cache.profile",
        run_id = %run_id,
        profile_enabled = profile.is_some(),
        "test-cache run started"
    );
    let params = TestCacheParams {
        target: target.clone(),
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
        during_upload_timeout: std::time::Duration::from_millis(during_upload_timeout_ms),
        l1_limit,
        profile: profile.is_some(),
        full_file_reads,
        run_id: run_id.clone(),
    };

    let resolved_mode = resolve_run_mode(&target)?;
    let benchmark_mode = match &resolved_mode {
        ResolvedRunMode::DirectShim { .. } => "direct-shim",
        ResolvedRunMode::MountedE2E { .. } => "e2e",
    };
    let write_path_observability_path = match &resolved_mode {
        ResolvedRunMode::MountedE2E { cache_path, .. } => cache_path
            .as_ref()
            .map(|path| path.join(WRITE_PATH_OBSERVABILITY_FILE)),
        ResolvedRunMode::DirectShim { .. } => None,
    };
    let current_dir = std::env::current_dir().context("resolve current directory")?;
    let report_dir = resolve_report_dir(json_output.as_deref(), &current_dir);
    let profile_session = TestCacheProfileSession::start(
        profile,
        &run_id,
        benchmark_mode,
        write_path_observability_path,
    )?;

    let rt = test_cache_runtime()?;
    let report = rt.block_on(async {
        match &resolved_mode {
            ResolvedRunMode::DirectShim { cache_dir } => run_direct(&params, cache_dir).await,
            ResolvedRunMode::MountedE2E {
                pid,
                volume,
                mount_point,
                work_root,
                cache_path,
            } => {
                run_mounted_end_to_end(
                    &params,
                    *pid,
                    volume,
                    mount_point,
                    work_root,
                    cache_path.as_deref(),
                )
                .await
            }
        }
    })?;

    let profile_report = match profile_session.map(|session| session.finish(&report_dir)) {
        Some(Ok(report)) => Some(report),
        Some(Err(err)) => {
            tracing::warn!(
                target: "studiofs.cache.profile",
                run_id = %run_id,
                error = %err,
                "profile session failed to finish; benchmark report will still be written"
            );
            Some(TestCacheProfileResult {
                endpoint: None,
                artifacts: Vec::new(),
                warnings: vec![format!(
                    "profile session failed to finish: {err:#}; benchmark metrics are still valid"
                )],
            })
        }
        None => None,
    };
    let report = merge_profile_report(report, profile_report);

    print!("{}", render_human_summary(&report));
    let report_path = save_benchmark_report(&report, json_output.as_deref())?;
    println!("JSON report: {}", report_path.display());
    tracing::info!(
        target: "studiofs.cache.profile",
        run_id = %run_id,
        report_path = %report_path.display(),
        "test-cache run completed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::resolve_report_dir;
    use std::path::{Path, PathBuf};

    #[test]
    fn resolve_report_dir_uses_current_dir_for_bare_relative_filename() {
        let current_dir = PathBuf::from(r"C:\work");
        assert_eq!(
            resolve_report_dir(Some(Path::new("cache-run.json")), &current_dir),
            current_dir
        );
    }

    #[test]
    fn resolve_report_dir_preserves_parent_for_nested_relative_filename() {
        let current_dir = PathBuf::from(r"C:\work");
        assert_eq!(
            resolve_report_dir(Some(Path::new("reports/cache-run.json")), &current_dir),
            PathBuf::from("reports")
        );
    }
}

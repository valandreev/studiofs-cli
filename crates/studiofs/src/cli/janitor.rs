//! `studiofs janitor` — remove orphaned data objects not referenced by metadata.

use crate::cli::cache_layout::describe_cache_layout_state;
use clap::Args;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

use studiofs_cache::runtime::{
    LocalPayloadBackendKind, LocalPayloadRuntimeDetection, detect_cache_backends,
};
use studiofs_cache::{OpenLocalPayloadBackendError, open_local_payload_backend};
use studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome;
use studiofs_meta::{
    JanitorMetadataSession, JanitorProtection, JanitorSweepCheckpoint, JanitorSweepScope,
};
mod pipeline;
#[cfg(test)]
use pipeline::run_one_cycle;
use pipeline::{
    CycleOutcome, JanitorCycleContext, JanitorListing, JanitorObjectStore, RunBudget,
    classify_error, run_one_cycle_resumable,
};
use studiofs_data::janitor::{
    JanitorDeleteError, JanitorEntry, JanitorFailureClass, JanitorLister,
};

/// `--loop` gives up after this many failed or overloaded cycles in a row.
const MAX_CONSECUTIVE_FAILED_CYCLES: u32 = 5;

/// Decides what follows a cycle: the next sleep, a clean stop, or the run's error.
#[derive(Debug, Default)]
struct LoopBreaker {
    consecutive_failures: u32,
}

impl LoopBreaker {
    /// Terminal errors end the run at once. A transient error or an overloaded cycle
    /// ends a single run; under `--loop` it backs off (interval x 2^(n-1), at most 8x)
    /// and opens the circuit after [`MAX_CONSECUTIVE_FAILED_CYCLES`]. Success resets it.
    fn after_cycle(
        &mut self,
        cycle: anyhow::Result<CycleOutcome>,
        loop_mode: bool,
        interval: Duration,
    ) -> anyhow::Result<Option<Duration>> {
        let failure = match cycle {
            Ok(CycleOutcome::Overloaded) => anyhow::anyhow!(
                "janitor cycle stopped early: S3 overload circuit open; unfinished work stays queued or checkpointed"
            ),
            Ok(CycleOutcome::TransientErrors) => {
                anyhow::anyhow!("janitor cycle completed with transient errors")
            }
            Ok(_) => {
                self.consecutive_failures = 0;
                return Ok(loop_mode.then_some(interval));
            }
            Err(error) if classify_error(&error) == JanitorFailureClass::Terminal => {
                return Err(
                    error.context("janitor stopped: terminal failure, retrying cannot help")
                );
            }
            Err(error) => error.context("janitor cycle failed: transient failure"),
        };
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if !loop_mode {
            return Err(failure);
        }
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILED_CYCLES {
            return Err(failure.context(format!(
                "janitor circuit open after {} consecutive failed cycles",
                self.consecutive_failures
            )));
        }
        let delay = interval.saturating_mul(1 << (self.consecutive_failures - 1).min(3));
        eprintln!(
            "janitor: {failure:#}; retry {} of {} in {}s",
            self.consecutive_failures,
            MAX_CONSECUTIVE_FAILED_CYCLES - 1,
            delay.as_secs()
        );
        Ok(Some(delay))
    }
}

struct OpenDalJanitorStore(studiofs_data::janitor::JanitorObjectStore);

impl JanitorListing for JanitorLister {
    async fn next_entry(&mut self) -> anyhow::Result<Option<JanitorEntry>> {
        Ok(self.next().await?)
    }
}

impl JanitorObjectStore for OpenDalJanitorStore {
    type Listing = JanitorLister;

    fn supports_start_after(&self) -> bool {
        self.0.supports_start_after()
    }

    async fn list(&self, prefix: &str, after_key: Option<&str>) -> anyhow::Result<JanitorLister> {
        Ok(self.0.list_after(prefix, after_key).await?)
    }

    async fn last_modified(&self, key: &str) -> anyhow::Result<Option<SystemTime>> {
        Ok(self.0.last_modified(key).await?)
    }

    async fn stat_key(&self, key: &str) -> anyhow::Result<studiofs_data::janitor::JanitorKeyStat> {
        Ok(self.0.stat_key(key).await?)
    }

    async fn delete(&self, key: &str) -> Result<(), JanitorDeleteError> {
        self.0.delete(key).await
    }
}

struct JanitorOptions {
    cache_dir: Option<PathBuf>,
    dry_run: bool,
    loop_mode: bool,
    interval_minutes: u64,
    skip_newer_minutes: u64,
    max_deletes: u64,
    max_scan_entries: Option<u64>,
    max_runtime_minutes: Option<u64>,
    pg_concurrency: u64,
    s3_concurrency: u64,
    s3_requests_per_second: u32,
    full_scan: bool,
    reset_scan: bool,
}

struct JanitorRunRequest {
    name: String,
    meta_dir: PathBuf,
    options: JanitorOptions,
}

trait JanitorMetadataSource {
    fn try_acquire_global_janitor_lock(&mut self) -> anyhow::Result<bool>;
    fn validate_protection(&mut self) -> anyhow::Result<()>;
    fn check_eligibility(
        &mut self,
        object_ids: &[u64],
        eligible_before: i64,
    ) -> anyhow::Result<JanitorProtection>;
    fn gc_candidate_page(
        &mut self,
        after: Option<(i64, u64)>,
        limit: usize,
        eligible_before: i64,
        backfill: bool,
    ) -> anyhow::Result<Vec<(u64, i64)>>;
    fn claim_deletion(&mut self, object_id: u64) -> anyhow::Result<Option<i64>>;
    fn finish_deletion(
        &mut self,
        object_id: u64,
        fencing_token: i64,
        outcome: UploadedObjectDeletionOutcome,
    ) -> anyhow::Result<()>;
    fn retry_gc_candidate(&mut self, object_id: u64) -> anyhow::Result<()>;
    fn ack_gc_candidate(&mut self, object_id: u64) -> anyhow::Result<()>;
    fn prune_deletion_fences(&mut self) -> anyhow::Result<u64>;
    fn load_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        persist_initial: bool,
    ) -> anyhow::Result<JanitorSweepCheckpoint>;
    fn save_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        checkpoint: &JanitorSweepCheckpoint,
    ) -> anyhow::Result<()>;
    fn reset_sweep_checkpoint(&mut self, scope: JanitorSweepScope<'_>) -> anyhow::Result<()>;
    fn release_global_janitor_lock(&mut self) -> anyhow::Result<()>;
}

impl JanitorMetadataSource for JanitorMetadataSession {
    fn try_acquire_global_janitor_lock(&mut self) -> anyhow::Result<bool> {
        JanitorMetadataSession::try_acquire_global_janitor_lock(self).map_err(anyhow::Error::new)
    }

    fn validate_protection(&mut self) -> anyhow::Result<()> {
        JanitorMetadataSession::validate_protection(self).map_err(anyhow::Error::new)
    }

    fn check_eligibility(
        &mut self,
        object_ids: &[u64],
        eligible_before: i64,
    ) -> anyhow::Result<JanitorProtection> {
        JanitorMetadataSession::check_eligibility(self, object_ids, eligible_before)
            .map_err(anyhow::Error::new)
    }

    fn gc_candidate_page(
        &mut self,
        after: Option<(i64, u64)>,
        limit: usize,
        eligible_before: i64,
        backfill: bool,
    ) -> anyhow::Result<Vec<(u64, i64)>> {
        JanitorMetadataSession::gc_candidate_page(self, after, limit, eligible_before, backfill)
            .map_err(anyhow::Error::new)
    }

    fn claim_deletion(&mut self, object_id: u64) -> anyhow::Result<Option<i64>> {
        JanitorMetadataSession::claim_deletion(self, object_id).map_err(anyhow::Error::new)
    }

    fn finish_deletion(
        &mut self,
        object_id: u64,
        fencing_token: i64,
        outcome: UploadedObjectDeletionOutcome,
    ) -> anyhow::Result<()> {
        JanitorMetadataSession::finish_deletion(self, object_id, fencing_token, outcome)
            .map_err(anyhow::Error::new)
    }

    fn retry_gc_candidate(&mut self, object_id: u64) -> anyhow::Result<()> {
        JanitorMetadataSession::retry_gc_candidate(self, object_id).map_err(anyhow::Error::new)
    }

    fn ack_gc_candidate(&mut self, object_id: u64) -> anyhow::Result<()> {
        JanitorMetadataSession::ack_gc_candidate(self, object_id).map_err(anyhow::Error::new)
    }

    fn prune_deletion_fences(&mut self) -> anyhow::Result<u64> {
        JanitorMetadataSession::prune_deletion_fences(self).map_err(anyhow::Error::new)
    }

    fn load_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        persist_initial: bool,
    ) -> anyhow::Result<JanitorSweepCheckpoint> {
        JanitorMetadataSession::load_sweep_checkpoint(self, scope, persist_initial)
            .map_err(anyhow::Error::new)
    }

    fn save_sweep_checkpoint(
        &mut self,
        scope: JanitorSweepScope<'_>,
        checkpoint: &JanitorSweepCheckpoint,
    ) -> anyhow::Result<()> {
        JanitorMetadataSession::save_sweep_checkpoint(self, scope, checkpoint)
            .map_err(anyhow::Error::new)
    }

    fn reset_sweep_checkpoint(&mut self, scope: JanitorSweepScope<'_>) -> anyhow::Result<()> {
        JanitorMetadataSession::reset_sweep_checkpoint(self, scope).map_err(anyhow::Error::new)
    }

    fn release_global_janitor_lock(&mut self) -> anyhow::Result<()> {
        JanitorMetadataSession::release_global_janitor_lock(self).map_err(anyhow::Error::new)
    }
}

fn with_acquired_global_janitor_lock<M, T>(
    metadata: &mut M,
    work: impl FnOnce(&mut M) -> anyhow::Result<T>,
) -> anyhow::Result<T>
where
    M: JanitorMetadataSource + ?Sized,
{
    let result = work(metadata);
    match metadata.release_global_janitor_lock() {
        Ok(()) => result,
        Err(release_error) if result.is_err() => {
            eprintln!("Warning: failed to release global janitor lock: {release_error}");
            result
        }
        Err(release_error) => Err(release_error.context("failed to release global janitor lock")),
    }
}

impl JanitorRunRequest {
    fn from_args(args: JanitorArgs) -> anyhow::Result<Self> {
        let meta_dir = super::utils::get_default_meta_dir(&args.name)?;
        Ok(Self {
            name: args.name,
            meta_dir,
            options: JanitorOptions {
                cache_dir: args.cache_dir,
                dry_run: args.dry_run,
                loop_mode: args.r#loop,
                interval_minutes: args.interval_minutes,
                skip_newer_minutes: args.skip_newer_minutes,
                max_deletes: args.max_deletes,
                max_scan_entries: args.max_scan_entries,
                max_runtime_minutes: args.max_runtime_minutes,
                pg_concurrency: args.pg_concurrency,
                s3_concurrency: args.s3_concurrency,
                s3_requests_per_second: args.s3_requests_per_second,
                full_scan: args.full_scan,
                reset_scan: args.reset_scan,
            },
        })
    }
}

#[derive(Debug, Args)]
#[command(about = "Scan data objects and remove orphaned ones (Phase 6 prototype)")]
#[command(after_help = "\
EXAMPLES:
  studiofs janitor myvolume --dry-run
  studiofs janitor myvolume --max-deletes 500 --skip-newer-minutes 15
  studiofs janitor myvolume --loop --interval-minutes 30
  studiofs janitor myvolume --s3-concurrency 2 --pg-concurrency 1
")]
pub(crate) struct JanitorArgs {
    /// Volume name
    name: String,

    /// Optional local cache base directory for pending-upload diagnostics.
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// Do not delete, only report what would be deleted
    #[arg(long)]
    dry_run: bool,

    /// Run continuously with a sleep interval between cycles
    #[arg(long)]
    r#loop: bool,

    /// Interval between cycles in loop mode
    #[arg(long, default_value_t = 30)]
    interval_minutes: u64,

    /// Skip objects newer than this many minutes
    #[arg(long, default_value_t = 15)]
    skip_newer_minutes: u64,

    /// Stop a cycle after this many successful deletions; the rest of the bucket is not listed
    #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(u64).range(1..))]
    max_deletes: u64,

    /// Stop listing a cycle after this many entries and report it as partial
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    max_scan_entries: Option<u64>,

    /// Stop admitting new work after this many minutes per cycle and report it as partial
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    max_runtime_minutes: Option<u64>,

    /// Maximum in-flight PostgreSQL janitor commands (the current CLI command path is serial)
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..=8))]
    pg_concurrency: u64,

    /// Maximum in-flight S3 HTTP requests, including listing pages, HEAD, and DELETE
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=32))]
    s3_concurrency: u64,

    /// Maximum S3 HTTP request starts per second
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u32).range(1..=32))]
    s3_requests_per_second: u32,

    /// Start this sweep at the beginning of the namespace
    #[arg(long, conflicts_with = "reset_scan")]
    full_scan: bool,

    /// Clear saved scan progress before starting a new sweep
    #[arg(long, conflicts_with = "full_scan")]
    reset_scan: bool,
}

impl JanitorArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        run(JanitorRunRequest::from_args(self)?)
    }
}

fn normalize_prefix(format_info: &studiofs_meta::FormatInfo) -> String {
    let raw = if format_info.data_prefix.trim().is_empty() {
        format!("{}/objects/", format_info.name)
    } else {
        format_info.data_prefix.clone()
    };
    if raw.ends_with('/') {
        raw
    } else {
        format!("{}/", raw)
    }
}

fn local_cache_path(
    meta_dir: &std::path::Path,
    cache_dir: Option<&std::path::Path>,
    volume_uuid: &str,
) -> PathBuf {
    let base = cache_dir
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(|| meta_dir.join("cache"));
    base.join(volume_uuid)
}

fn map_cache_backend_error(
    cache_dir: &std::path::Path,
    source: OpenLocalPayloadBackendError,
) -> anyhow::Error {
    anyhow::anyhow!(
        "failed to open local cache backend at '{}': {source}",
        cache_dir.display()
    )
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct JanitorLocalCacheState {
    pending_uploads: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JanitorLocalCacheReport {
    path: PathBuf,
    state_description: String,
    pending_uploads: Option<usize>,
}

struct JanitorStartBanner<'a> {
    name: &'a str,
    dry_run: bool,
    endpoint: &'a str,
    bucket: &'a str,
    prefix: &'a str,
    local_cache_report: Option<&'a JanitorLocalCacheReport>,
    skip_newer_minutes: u64,
    max_deletes: u64,
    pg_concurrency: u64,
    s3_concurrency: u64,
    s3_requests_per_second: u32,
}

impl JanitorLocalCacheReport {
    fn from_inspection(
        path: PathBuf,
        detection: Option<&LocalPayloadRuntimeDetection>,
        state: Option<JanitorLocalCacheState>,
    ) -> Self {
        Self {
            path,
            state_description: describe_cache_layout_state(detection).to_string(),
            pending_uploads: state.map(|state| state.pending_uploads),
        }
    }
}

fn build_start_banner_lines(banner: JanitorStartBanner<'_>) -> Vec<String> {
    let mut lines = vec![
        format!("Janitor started for '{}'", banner.name),
        format!(
            "  Mode:       {}",
            if banner.dry_run { "dry-run" } else { "delete" }
        ),
        format!("  Storage:    {} / {}", banner.endpoint, banner.bucket),
        format!("  Prefix:     {}", banner.prefix),
        "  Scope:      remote objects only".to_string(),
    ];
    if let Some(report) = banner.local_cache_report {
        lines.push(format!(
            "  Local cache: {} (diagnostics only)",
            report.path.display()
        ));
        lines.push(format!("  Local cache state: {}", report.state_description));
        if let Some(pending_uploads) = report.pending_uploads {
            lines.push(format!("  Local pending uploads: {pending_uploads}"));
        }
    }
    lines.push(format!(
        "  Grace:      {}",
        format_minutes(banner.skip_newer_minutes)
    ));
    lines.push(format!("  Max delete: {}", banner.max_deletes));
    lines.push(format!(
        "  In-flight:  PG <= {} (serial), S3 HTTP <= {}",
        banner.pg_concurrency, banner.s3_concurrency
    ));
    lines.push(format!(
        "  S3 rate:    <= {} HTTP requests/second",
        banner.s3_requests_per_second
    ));
    lines
}

fn format_minutes(minutes: u64) -> String {
    match minutes {
        1 => "1 minute".to_string(),
        minutes => format!("{minutes} minutes"),
    }
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn inspect_local_cache_runtime(
    meta_dir: &std::path::Path,
    cache_dir: Option<&std::path::Path>,
    volume_uuid: &str,
) -> anyhow::Result<(
    PathBuf,
    Option<LocalPayloadRuntimeDetection>,
    Option<JanitorLocalCacheState>,
)> {
    let cache_path = local_cache_path(meta_dir, cache_dir, volume_uuid);
    let detection = match detect_cache_backends(&cache_path) {
        Ok(detection) => detection,
        Err(error) => {
            eprintln!(
                "warning: failed to inspect local cache layout at '{}': {error}",
                cache_path.display()
            );
            return Ok((cache_path, None, None));
        }
    };
    if detection.runtime_backend() != Some(LocalPayloadBackendKind::Current) {
        return Ok((cache_path, Some(detection), None));
    }
    if detection.needs_runtime_provisioning() {
        eprintln!(
            "warning: local cache runtime at '{}' is incomplete; skipping pending-upload inspection",
            cache_path.display()
        );
        return Ok((cache_path, Some(detection), None));
    }

    let store = match open_local_payload_backend(&cache_path, volume_uuid) {
        Ok(store) => store.backend,
        Err(source) => {
            let error = map_cache_backend_error(&cache_path, source);
            eprintln!("warning: {error}");
            return Ok((cache_path, Some(detection), None));
        }
    };
    Ok((
        cache_path,
        Some(detection),
        Some(JanitorLocalCacheState {
            pending_uploads: store.dirty_object_count(),
        }),
    ))
}

fn run(request: JanitorRunRequest) -> anyhow::Result<()> {
    let JanitorRunRequest {
        name,
        meta_dir,
        options,
    } = request;
    let JanitorOptions {
        cache_dir,
        dry_run,
        loop_mode,
        interval_minutes,
        skip_newer_minutes,
        max_deletes,
        max_scan_entries,
        max_runtime_minutes,
        pg_concurrency,
        s3_concurrency,
        s3_requests_per_second,
        full_scan,
        reset_scan,
    } = options;
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        return Err(anyhow::anyhow!(
            "Volume '{}' not found at {:?}",
            name,
            db_path
        ));
    }

    eprintln!("janitor phase=metadata_open started");
    let metadata_open_started = Instant::now();
    let mut metadata = JanitorMetadataSession::open(&db_path)?;
    let format_info = metadata.format_info().clone();
    let metadata_open_elapsed = metadata_open_started.elapsed();
    eprintln!(
        "janitor phase=metadata_open completed duration_ms={}",
        duration_ms(metadata_open_elapsed)
    );

    let prefix = normalize_prefix(&format_info);
    let local_cache_report = if let Some(cache_dir) = cache_dir.as_deref() {
        let (cache_path, detection, local_cache_state) =
            inspect_local_cache_runtime(meta_dir.as_path(), Some(cache_dir), &format_info.uuid)?;
        Some(JanitorLocalCacheReport::from_inspection(
            cache_path,
            detection.as_ref(),
            local_cache_state,
        ))
    } else {
        None
    };

    let store = OpenDalJanitorStore(studiofs_data::janitor::JanitorObjectStore::from_s3(
        &format_info.endpoint,
        &format_info.bucket,
        &format_info.access_key,
        &format_info.secret_key,
        &format_info.region,
        usize::try_from(s3_concurrency)
            .map_err(|_| anyhow::anyhow!("S3 concurrency is invalid"))?,
        s3_requests_per_second,
    )?);
    let listing_strategy = if store.supports_start_after() {
        "opendal-start-after-v1"
    } else {
        "hex-prefix-trie-v1"
    };
    let sweep_scope = JanitorSweepScope {
        volume_uuid: &format_info.uuid,
        endpoint: &format_info.endpoint,
        bucket: &format_info.bucket,
        data_prefix: &prefix,
        listing_strategy,
    };

    let rt = tokio::runtime::Runtime::new()?;
    let skip_secs = skip_newer_minutes.saturating_mul(60);

    for line in build_start_banner_lines(JanitorStartBanner {
        name: &name,
        dry_run,
        endpoint: &format_info.endpoint,
        bucket: &format_info.bucket,
        prefix: &prefix,
        local_cache_report: local_cache_report.as_ref(),
        skip_newer_minutes,
        max_deletes,
        pg_concurrency,
        s3_concurrency,
        s3_requests_per_second,
    }) {
        println!("{line}");
    }

    let mut restart_pending = full_scan || reset_scan;
    let interval = Duration::from_secs(interval_minutes.max(1).saturating_mul(60));
    let mut breaker = LoopBreaker::default();
    loop {
        let cycle_started = Instant::now();
        let lock_started = Instant::now();
        eprintln!("janitor phase=global_lock started");
        let lock_acquired =
            match JanitorMetadataSource::try_acquire_global_janitor_lock(&mut metadata) {
                Ok(acquired) => acquired,
                Err(error) => {
                    let error = error.context("failed to acquire global janitor lock");
                    match breaker.after_cycle(Err(error), loop_mode, interval)? {
                        Some(delay) => std::thread::sleep(delay),
                        None => break,
                    }
                    continue;
                }
            };
        let lock_wait_elapsed = lock_started.elapsed();
        eprintln!(
            "janitor phase=global_lock completed acquired={} duration_ms={}",
            lock_acquired,
            duration_ms(lock_wait_elapsed)
        );
        if !lock_acquired {
            println!(
                "Janitor cycle skipped: lock_busy (wait_ms={})",
                duration_ms(lock_wait_elapsed)
            );
            if !loop_mode {
                break;
            }
            std::thread::sleep(interval);
            continue;
        }

        let protection_started = Instant::now();
        eprintln!("janitor phase=protection started");
        let mut protection_elapsed = Duration::ZERO;
        let cycle_result = with_acquired_global_janitor_lock(&mut metadata, |metadata| {
            JanitorMetadataSource::validate_protection(metadata)?;
            protection_elapsed = protection_started.elapsed();
            eprintln!(
                "janitor phase=protection completed duration_ms={}",
                duration_ms(protection_elapsed)
            );

            let mut checkpoint = JanitorMetadataSource::load_sweep_checkpoint(
                metadata,
                sweep_scope,
                !dry_run && !restart_pending,
            )?;
            if restart_pending && reset_scan {
                if !dry_run {
                    JanitorMetadataSource::reset_sweep_checkpoint(metadata, sweep_scope)?;
                }
                checkpoint = JanitorSweepCheckpoint {
                    after_key: None,
                    pending_object_id: None,
                    pending_key: None,
                    next_prefix: "0".to_string(),
                };
            } else if restart_pending && full_scan {
                checkpoint = JanitorSweepCheckpoint {
                    after_key: None,
                    pending_object_id: None,
                    pending_key: None,
                    next_prefix: "0".to_string(),
                };
            }

            let stats = rt.block_on(run_one_cycle_resumable(
                JanitorCycleContext {
                    store: &store,
                    prefix: &prefix,
                    dry_run,
                    skip_newer_than_secs: skip_secs,
                    budget: RunBudget {
                        max_deletes,
                        max_scan_entries,
                        max_runtime: max_runtime_minutes
                            .map(|minutes| Duration::from_secs(minutes.saturating_mul(60))),
                    },
                    metadata,
                },
                checkpoint,
                &sweep_scope,
                !dry_run,
            ))?;
            restart_pending = false;
            let fences_pruned = if dry_run {
                0
            } else {
                JanitorMetadataSource::prune_deletion_fences(metadata)?
            };

            Ok((stats, fences_pruned))
        });
        let cycle_elapsed = cycle_started.elapsed();
        let (stats, fences_pruned) = match cycle_result {
            Ok(done) => done,
            Err(error) => {
                match breaker.after_cycle(Err(error), loop_mode, interval)? {
                    Some(delay) => std::thread::sleep(delay),
                    None => break,
                }
                continue;
            }
        };
        let transport = store.0.transport_stats();

        println!("Janitor cycle done:");
        println!("  scanned:              {}", stats.scanned_files);
        println!("  referenced:           {}", stats.referenced_files);
        println!("  orphan candidates:    {}", stats.orphan_candidates);
        println!("  skipped young:        {}", stats.skipped_young);
        println!("  skipped unknown age:  {}", stats.skipped_unknown_age);
        println!("  deferred candidates: {}", stats.skipped_deferred);
        println!("  outcome:              {}", stats.cycle_outcome().label());
        println!(
            "  scan coverage:        {} (next={})",
            if stats.scan_complete {
                "complete"
            } else {
                "partial"
            },
            stats.scan_position
        );
        println!(
            "  {}: {}",
            if dry_run { "would delete" } else { "deleted" },
            stats.deleted
        );
        println!("  delete errors:        {}", stats.delete_errors);
        println!("  head errors:          {}", stats.head_errors);
        println!("  fences pruned:        {fences_pruned}");
        println!(
            "  janitor timings: metadata_open_ms={} global_lock_wait_ms={} protection_ms={} initial_listing_ms={} head_ms={} delete_ms={} object_work_ms={} cycle_ms={}",
            duration_ms(metadata_open_elapsed),
            duration_ms(lock_wait_elapsed),
            duration_ms(protection_elapsed),
            duration_ms(stats.initial_listing_elapsed),
            duration_ms(stats.head_elapsed),
            duration_ms(stats.delete_elapsed),
            duration_ms(stats.cycle_elapsed),
            duration_ms(cycle_elapsed),
        );
        println!(
            "  janitor counts: listed_entries={} peak_buffered_candidates={} scanned={} first_entry_ms={} first_candidate_ms={} opendal_head_calls={} opendal_delete_calls={} list_pages=unavailable claims={} claim_protected={} claim_finish={} skipped_non_canonical={} s3_throttled={} s3_retries={} s3_request_interval_ms={}",
            stats.initial_listing_entries,
            stats.peak_buffered_candidates,
            stats.scanned_files,
            stats
                .first_listing_entry
                .map(duration_ms)
                .map(|ms| ms.to_string())
                .unwrap_or_else(|| "unavailable".to_string()),
            stats
                .time_to_first_candidate
                .map(duration_ms)
                .map(|ms| ms.to_string())
                .unwrap_or_else(|| "unavailable".to_string()),
            stats.head_requests,
            stats.delete_requests,
            stats.claims,
            stats.claim_protected,
            stats.claim_finishes,
            stats.skipped_non_canonical,
            transport.throttled,
            transport.retries,
            duration_ms(transport.request_interval),
        );
        match breaker.after_cycle(Ok(stats.cycle_outcome()), loop_mode, interval)? {
            Some(delay) => std::thread::sleep(delay),
            None => break,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::Operator;
    use opendal::services::S3;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use studiofs_cache::runtime::LocalPayloadLayout;
    use studiofs_meta::{JanitorEligibility, MAX_JANITOR_ELIGIBILITY_BATCH};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};

    #[derive(Default)]
    struct FakeJanitorStore {
        entries: Vec<studiofs_data::janitor::JanitorEntry>,
        start_after_capable: Option<bool>,
        deleted: Mutex<Vec<String>>,
        list_requests: AtomicU64,
        head_requests: AtomicU64,
        fail_list: bool,
        /// Fail the listing after yielding this many entries.
        fail_list_after: Option<usize>,
        /// Stall the listing after yielding every entry.
        hang_list: bool,
        fail_head: bool,
        hang_head: bool,
        head_missing: bool,
        head_young: bool,
        fail_delete: bool,
        /// Reject every delete as access denied.
        deny_delete: bool,
        missing_delete: bool,
        hang_delete: bool,
    }

    struct FakeListing {
        entries: std::vec::IntoIter<JanitorEntry>,
        fail_after: Option<usize>,
        hang: bool,
        yielded: usize,
    }

    impl JanitorListing for FakeListing {
        async fn next_entry(&mut self) -> anyhow::Result<Option<JanitorEntry>> {
            if self.fail_after == Some(self.yielded) {
                anyhow::bail!("list page failed")
            }
            let entry = self.entries.next();
            if entry.is_none() && self.hang {
                std::future::pending::<()>().await;
            }
            self.yielded += 1;
            Ok(entry)
        }
    }

    impl JanitorObjectStore for FakeJanitorStore {
        type Listing = FakeListing;

        fn supports_start_after(&self) -> bool {
            self.start_after_capable.unwrap_or(true)
        }

        async fn list(&self, prefix: &str, after_key: Option<&str>) -> anyhow::Result<FakeListing> {
            self.list_requests.fetch_add(1, Ordering::Relaxed);
            if self.fail_list {
                anyhow::bail!("list failed")
            }
            Ok(FakeListing {
                entries: self
                    .entries
                    .iter()
                    .filter(|entry| entry.key.starts_with(prefix))
                    .filter(|entry| {
                        after_key.is_none_or(|after_key| entry.key.as_str() > after_key)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter(),
                fail_after: self.fail_list_after,
                hang: self.hang_list,
                yielded: 0,
            })
        }

        async fn last_modified(&self, _key: &str) -> anyhow::Result<Option<SystemTime>> {
            self.head_requests.fetch_add(1, Ordering::Relaxed);
            if self.fail_head {
                return Err(
                    opendal::Error::new(opendal::ErrorKind::Unexpected, "head failed")
                        .set_temporary()
                        .into(),
                );
            }
            if self.hang_head {
                std::future::pending::<()>().await;
            }
            if self.head_missing {
                return Ok(None);
            }
            let age = if self.head_young { 30 } else { 3600 };
            Ok(Some(SystemTime::now() - Duration::from_secs(age)))
        }

        async fn delete(&self, key: &str) -> Result<(), JanitorDeleteError> {
            self.deleted.lock().unwrap().push(key.to_string());
            if self.hang_delete {
                std::future::pending::<()>().await;
            }
            if self.deny_delete {
                Err(JanitorDeleteError::Terminal("access denied".to_string()))
            } else if self.fail_delete {
                Err(JanitorDeleteError::Other("delete failed".to_string()))
            } else if self.missing_delete {
                Err(JanitorDeleteError::NotFound)
            } else {
                Ok(())
            }
        }
    }

    #[derive(Default)]
    struct FakeJanitorMetadata {
        lock_acquired: bool,
        validation_error: Option<&'static str>,
        eligibility_error: Option<&'static str>,
        default_eligibility: Option<JanitorEligibility>,
        outcomes: JanitorProtection,
        validation_reads: u64,
        eligibility_reads: Vec<Vec<u64>>,
        gc_candidates: Vec<(u64, i64)>,
        gc_candidate_reads: Vec<(Option<u64>, usize)>,
        gc_candidate_acks: Vec<u64>,
        gc_candidate_retries: Vec<u64>,
        gc_candidate_backfills: u64,
        ack_error: Option<&'static str>,
        nested_runtime: Option<std::sync::Arc<tokio::runtime::Runtime>>,
        releases: u64,
        claim_protected: Vec<u64>,
        claim_error: Option<&'static str>,
        finish_error: Option<&'static str>,
        claims: Vec<u64>,
        finishes: Vec<(u64, i64, UploadedObjectDeletionOutcome)>,
        sweep_checkpoint: Option<JanitorSweepCheckpoint>,
        sweep_checkpoint_writes: u64,
        sweep_checkpoint_resets: u64,
    }

    /// Fencing token the fake metadata hands out for `object_id`.
    fn fake_token(object_id: u64) -> i64 {
        i64::try_from(object_id).unwrap() + 1000
    }

    /// Every canonical key the CLI must delete for `object_id`: legacy plus fragments.
    fn canonical_keys(object_id: u64) -> Vec<String> {
        std::iter::once(studiofs_data::object::format_object_key(
            "test/objects/",
            object_id,
        ))
        .chain(studiofs_data::object::format_object_fragment_keys(
            "test/objects/",
            object_id,
        ))
        .collect()
    }

    impl FakeJanitorMetadata {
        fn eligible() -> Self {
            Self {
                lock_acquired: true,
                default_eligibility: Some(JanitorEligibility::Eligible {
                    pending_deletion: None,
                }),
                ..Self::default()
            }
        }
    }

    impl JanitorMetadataSource for FakeJanitorMetadata {
        fn try_acquire_global_janitor_lock(&mut self) -> anyhow::Result<bool> {
            Ok(self.lock_acquired)
        }

        fn validate_protection(&mut self) -> anyhow::Result<()> {
            self.validation_reads += 1;
            if let Some(error) = self.validation_error {
                anyhow::bail!(error);
            }
            Ok(())
        }

        fn check_eligibility(
            &mut self,
            object_ids: &[u64],
            _eligible_before: i64,
        ) -> anyhow::Result<JanitorProtection> {
            self.eligibility_reads.push(object_ids.to_vec());
            if let Some(error) = self.eligibility_error {
                anyhow::bail!(error);
            }
            if let Some(runtime) = &self.nested_runtime {
                runtime.block_on(async {});
            }
            Ok(object_ids
                .iter()
                .filter_map(|id| {
                    let eligibility = if self.gc_candidate_retries.contains(id) {
                        Some(JanitorEligibility::Deferred)
                    } else {
                        self.outcomes
                            .get(id)
                            .or(self.default_eligibility.as_ref())
                            .copied()
                    };
                    eligibility.map(|eligibility| (*id, eligibility))
                })
                .collect())
        }

        fn gc_candidate_page(
            &mut self,
            after: Option<(i64, u64)>,
            limit: usize,
            eligible_before: i64,
            backfill: bool,
        ) -> anyhow::Result<Vec<(u64, i64)>> {
            self.gc_candidate_reads
                .push((after.map(|(_, id)| id), limit));
            self.gc_candidate_backfills += u64::from(backfill);
            let after_secs = after.map(|(micros, _)| micros.div_euclid(1_000_000));
            let after_id = after.map(|(_, id)| id);
            let mut candidates = self
                .gc_candidates
                .iter()
                .copied()
                .filter(|(id, eligible_at)| {
                    *eligible_at <= eligible_before
                        && after_secs.is_none_or(|after_secs| {
                            *eligible_at > after_secs
                                || (*eligible_at == after_secs && Some(*id) > after_id)
                        })
                })
                .collect::<Vec<_>>();
            candidates.sort_unstable_by_key(|(id, eligible_at)| (*eligible_at, *id));
            Ok(candidates
                .into_iter()
                .take(limit)
                .map(|(id, eligible_at)| (id, eligible_at.saturating_mul(1_000_000)))
                .collect())
        }

        fn claim_deletion(&mut self, object_id: u64) -> anyhow::Result<Option<i64>> {
            self.claims.push(object_id);
            if let Some(error) = self.claim_error {
                anyhow::bail!(error);
            }
            Ok((!self.claim_protected.contains(&object_id)).then(|| fake_token(object_id)))
        }

        fn finish_deletion(
            &mut self,
            object_id: u64,
            fencing_token: i64,
            outcome: UploadedObjectDeletionOutcome,
        ) -> anyhow::Result<()> {
            self.finishes.push((object_id, fencing_token, outcome));
            if let Some(error) = self.finish_error {
                anyhow::bail!(error);
            }
            Ok(())
        }

        fn retry_gc_candidate(&mut self, object_id: u64) -> anyhow::Result<()> {
            self.gc_candidate_retries.push(object_id);
            Ok(())
        }

        fn ack_gc_candidate(&mut self, object_id: u64) -> anyhow::Result<()> {
            self.gc_candidate_acks.push(object_id);
            if let Some(error) = self.ack_error {
                anyhow::bail!(error);
            }
            Ok(())
        }

        fn prune_deletion_fences(&mut self) -> anyhow::Result<u64> {
            Ok(0)
        }

        fn load_sweep_checkpoint(
            &mut self,
            _scope: JanitorSweepScope<'_>,
            persist_initial: bool,
        ) -> anyhow::Result<JanitorSweepCheckpoint> {
            let checkpoint = self
                .sweep_checkpoint
                .clone()
                .unwrap_or(JanitorSweepCheckpoint {
                    after_key: None,
                    pending_object_id: None,
                    pending_key: None,
                    next_prefix: "0".to_string(),
                });
            if persist_initial && self.sweep_checkpoint.is_none() {
                self.sweep_checkpoint = Some(checkpoint.clone());
                self.sweep_checkpoint_writes += 1;
            }
            Ok(checkpoint)
        }

        fn save_sweep_checkpoint(
            &mut self,
            _scope: JanitorSweepScope<'_>,
            checkpoint: &JanitorSweepCheckpoint,
        ) -> anyhow::Result<()> {
            self.sweep_checkpoint = Some(checkpoint.clone());
            self.sweep_checkpoint_writes += 1;
            Ok(())
        }

        fn reset_sweep_checkpoint(&mut self, _scope: JanitorSweepScope<'_>) -> anyhow::Result<()> {
            self.sweep_checkpoint = None;
            self.sweep_checkpoint_resets += 1;
            Ok(())
        }

        fn release_global_janitor_lock(&mut self) -> anyhow::Result<()> {
            self.releases += 1;
            Ok(())
        }
    }

    fn sweep_scope() -> JanitorSweepScope<'static> {
        JanitorSweepScope {
            volume_uuid: "volume",
            endpoint: "endpoint",
            bucket: "bucket",
            data_prefix: "test/objects/",
            listing_strategy: "start_after",
        }
    }

    fn sweep_checkpoint() -> JanitorSweepCheckpoint {
        JanitorSweepCheckpoint {
            after_key: None,
            pending_object_id: None,
            pending_key: None,
            next_prefix: "0".into(),
        }
    }

    #[test]
    fn janitor_restart_with_single_entry_budget_drains_fragments_and_wraps() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut keys = canonical_keys(42);
        keys.extend(canonical_keys(43));
        keys.sort();
        let store = FakeJanitorStore {
            entries: keys
                .into_iter()
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect(),
            ..Default::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let mut completed = false;
        for _ in 0..10 {
            // Recreate the in-memory state from the durable row on each invocation.
            let checkpoint = metadata
                .sweep_checkpoint
                .clone()
                .unwrap_or_else(sweep_checkpoint);
            let stats = rt
                .block_on(run_one_cycle_resumable(
                    JanitorCycleContext {
                        store: &store,
                        prefix: "test/objects/",
                        dry_run: false,
                        skip_newer_than_secs: 0,
                        budget: RunBudget {
                            max_deletes: 100,
                            max_scan_entries: Some(1),
                            max_runtime: None,
                        },
                        metadata: &mut metadata,
                    },
                    checkpoint,
                    &sweep_scope(),
                    true,
                ))
                .unwrap();
            assert!(stats.initial_listing_entries <= 1);
            if stats.scan_complete {
                completed = true;
                break;
            }
        }
        assert!(completed);
        assert_eq!(metadata.claims, vec![42, 43]);
        assert_eq!(metadata.sweep_checkpoint, Some(sweep_checkpoint()));
    }

    #[test]
    fn janitor_stop_after_completed_batch_keeps_its_progress() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut keys: Vec<String> = (1..=70).flat_map(canonical_keys).collect();
        keys.sort();
        let store = FakeJanitorStore {
            entries: keys
                .into_iter()
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect(),
            ..Default::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        // The first batch of 64 completes; the cap then stops the trailing batch midway.
        let stats = rt
            .block_on(run_one_cycle_resumable(
                JanitorCycleContext {
                    store: &store,
                    prefix: "test/objects/",
                    dry_run: false,
                    skip_newer_than_secs: 0,
                    budget: RunBudget::deletes(65),
                    metadata: &mut metadata,
                },
                sweep_checkpoint(),
                &sweep_scope(),
                true,
            ))
            .unwrap();
        assert_eq!(stats.outcome, pipeline::CycleOutcome::DeleteCapReached);
        assert!(!stats.scan_complete);
        let saved = metadata.sweep_checkpoint.clone().unwrap();
        assert_ne!(
            saved,
            sweep_checkpoint(),
            "completed batch progress was discarded"
        );
        assert_eq!(saved.pending_object_id, Some(65));
    }

    #[test]
    fn janitor_resumed_object_refreshes_age_and_dry_run_preserves_checkpoint() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        // A pending-deletion timestamp must not bypass the refreshed age of a resumed object.
        for (young, missing, pending_deletion) in [
            (true, false, None),
            (false, true, None),
            (true, false, Some(0)),
            (false, true, Some(0)),
        ] {
            let mut keys = canonical_keys(42);
            keys.sort();
            let checkpoint = JanitorSweepCheckpoint {
                after_key: Some(keys[0].clone()),
                pending_object_id: Some(42),
                pending_key: Some(keys[0].clone()),
                next_prefix: "0".into(),
            };
            let store = FakeJanitorStore {
                entries: keys
                    .into_iter()
                    .map(|key| JanitorEntry {
                        key,
                        is_file: true,
                        last_modified: Some(SystemTime::now() - Duration::from_secs(3600)),
                    })
                    .collect(),
                head_young: young,
                head_missing: missing,
                ..Default::default()
            };
            let mut metadata = FakeJanitorMetadata {
                sweep_checkpoint: Some(checkpoint.clone()),
                default_eligibility: Some(JanitorEligibility::Eligible { pending_deletion }),
                ..FakeJanitorMetadata::eligible()
            };
            let stats = rt
                .block_on(run_one_cycle_resumable(
                    JanitorCycleContext {
                        store: &store,
                        prefix: "test/objects/",
                        dry_run: true,
                        skip_newer_than_secs: 60,
                        budget: RunBudget::deletes(100),
                        metadata: &mut metadata,
                    },
                    checkpoint.clone(),
                    &sweep_scope(),
                    false,
                ))
                .unwrap();
            assert_eq!(stats.deleted, 0);
            assert!(metadata.claims.is_empty());
            assert_eq!(metadata.sweep_checkpoint, Some(checkpoint));
            assert_eq!(metadata.sweep_checkpoint_writes, 0);
            assert!(store.head_requests.load(Ordering::Relaxed) > 0);
            assert_eq!(stats.skipped_young + stats.skipped_unknown_age, 1);
        }
    }

    #[test]
    fn janitor_capped_eof_batch_replays_unprocessed_candidates() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut keys = canonical_keys(42);
        keys.extend(canonical_keys(43));
        keys.sort();
        let store = FakeJanitorStore {
            entries: keys
                .into_iter()
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect(),
            ..Default::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let checkpoint = sweep_checkpoint();
        let stats = rt
            .block_on(run_one_cycle_resumable(
                JanitorCycleContext {
                    store: &store,
                    prefix: "test/objects/",
                    dry_run: false,
                    skip_newer_than_secs: 0,
                    budget: RunBudget::deletes(1),
                    metadata: &mut metadata,
                },
                checkpoint.clone(),
                &sweep_scope(),
                true,
            ))
            .unwrap();
        assert_eq!(stats.deleted, 1);
        assert!(!stats.scan_complete);
        assert_eq!(metadata.sweep_checkpoint, Some(checkpoint));
    }

    #[test]
    fn janitor_fallback_single_entry_budget_eventually_advances_past_dense_shard() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = FakeJanitorStore {
            entries: canonical_keys(42)
                .into_iter()
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect(),
            start_after_capable: Some(false),
            ..Default::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        for _ in 0..24 {
            let checkpoint = metadata
                .sweep_checkpoint
                .clone()
                .unwrap_or_else(sweep_checkpoint);
            let stats = rt
                .block_on(run_one_cycle_resumable(
                    JanitorCycleContext {
                        store: &store,
                        prefix: "test/objects/",
                        dry_run: false,
                        skip_newer_than_secs: 0,
                        budget: RunBudget {
                            max_deletes: 100,
                            max_scan_entries: Some(1),
                            max_runtime: None,
                        },
                        metadata: &mut metadata,
                    },
                    checkpoint,
                    &sweep_scope(),
                    true,
                ))
                .unwrap();
            assert!(stats.initial_listing_entries <= 1);
            if !metadata.claims.is_empty() {
                break;
            }
        }
        assert_eq!(metadata.claims, vec![42]);
        assert_ne!(
            metadata.sweep_checkpoint.unwrap().next_prefix,
            "002a000000000000002a"
        );
    }

    #[test]
    fn janitor_metadata_faults_release_lock_before_any_object_store_request() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        for error in [
            "unsafe_to_delete: identity mismatch",
            "unsafe_to_delete: snapshot bootstrap incomplete",
            "backend_unavailable: PostgreSQL timeout",
        ] {
            let mut metadata = FakeJanitorMetadata {
                validation_error: Some(error),
                ..FakeJanitorMetadata::eligible()
            };
            let store = FakeJanitorStore {
                entries: studiofs_data::object::format_object_fragment_keys("test/objects/", 9)
                    .into_iter()
                    .map(|key| JanitorEntry {
                        key,
                        is_file: true,
                        last_modified: None,
                    })
                    .collect(),
                ..FakeJanitorStore::default()
            };
            assert!(
                metadata.try_acquire_global_janitor_lock().unwrap(),
                "{error}"
            );
            let result = with_acquired_global_janitor_lock(&mut metadata, |metadata| {
                metadata.validate_protection()?;
                rt.block_on(run_one_cycle(JanitorCycleContext {
                    store: &store,
                    prefix: "test/objects/",
                    dry_run: false,
                    skip_newer_than_secs: 0,
                    budget: RunBudget::deletes(1),
                    metadata,
                }))
            });
            assert!(result.is_err(), "{error}");
            assert_eq!(store.list_requests.load(Ordering::Relaxed), 0, "{error}");
            assert_eq!(store.head_requests.load(Ordering::Relaxed), 0, "{error}");
            assert!(store.deleted.lock().unwrap().is_empty(), "{error}");
            assert_eq!(metadata.releases, 1, "{error}");
            assert_eq!(metadata.validation_reads, 1, "{error}");
            assert!(metadata.eligibility_reads.is_empty(), "{error}");
        }
    }

    #[test]
    fn janitor_healthy_metadata_progresses_and_dry_run_never_writes_or_deletes() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let entries = studiofs_data::object::format_object_fragment_keys("test/objects/", 42)
            .into_iter()
            .map(|key| JanitorEntry {
                key,
                is_file: true,
                last_modified: None,
            })
            .collect();
        let store = FakeJanitorStore {
            entries,
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        assert!(metadata.try_acquire_global_janitor_lock().unwrap());
        let stats = with_acquired_global_janitor_lock(&mut metadata, |metadata| {
            metadata.validate_protection()?;
            rt.block_on(run_one_cycle(JanitorCycleContext {
                store: &store,
                prefix: "test/objects/",
                dry_run: false,
                skip_newer_than_secs: 0,
                budget: RunBudget::deletes(1),
                metadata,
            }))
        })
        .unwrap();
        assert_eq!(stats.deleted, 1);
        assert_eq!(store.list_requests.load(Ordering::Relaxed), 1);
        assert!(!store.deleted.lock().unwrap().is_empty());
        assert_eq!(metadata.releases, 1);
        assert_eq!(metadata.validation_reads, 1);
        assert_eq!(metadata.eligibility_reads.len(), 1);

        let dry_run_store = FakeJanitorStore {
            entries: store.entries.clone(),
            ..FakeJanitorStore::default()
        };
        let mut dry_run_metadata = FakeJanitorMetadata::eligible();
        assert!(dry_run_metadata.try_acquire_global_janitor_lock().unwrap());
        let stats = with_acquired_global_janitor_lock(&mut dry_run_metadata, |metadata| {
            metadata.validate_protection()?;
            rt.block_on(run_one_cycle(JanitorCycleContext {
                store: &dry_run_store,
                prefix: "test/objects/",
                dry_run: true,
                skip_newer_than_secs: 0,
                budget: RunBudget::deletes(1),
                metadata,
            }))
        })
        .unwrap();
        assert_eq!(stats.deleted, 1);
        assert_eq!(dry_run_store.list_requests.load(Ordering::Relaxed), 1);
        assert_eq!(dry_run_store.head_requests.load(Ordering::Relaxed), 1);
        assert!(dry_run_store.deleted.lock().unwrap().is_empty());
        assert_eq!(dry_run_metadata.releases, 1);
        assert_eq!(dry_run_metadata.validation_reads, 1);
        assert_eq!(dry_run_metadata.eligibility_reads.len(), 1);
    }

    fn fake_cycle_context<'a>(
        store: &'a FakeJanitorStore,
        metadata: &'a mut FakeJanitorMetadata,
    ) -> JanitorCycleContext<'a, FakeJanitorStore> {
        JanitorCycleContext {
            store,
            prefix: "test/objects/",
            dry_run: false,
            skip_newer_than_secs: 60,
            budget: RunBudget::deletes(1),
            metadata,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fake_store_deletes_only_discovered_unprotected_objects() {
        let keys_for = |object_id| {
            studiofs_data::object::format_object_fragment_keys("test/objects/", object_id)
                .into_iter()
                .map(|key| studiofs_data::janitor::JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect::<Vec<_>>()
        };
        let mut entries = keys_for(1);
        entries.extend(keys_for(2));
        entries.extend(keys_for(3));
        let store = FakeJanitorStore {
            entries,
            deleted: Mutex::new(Vec::new()),
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata {
            outcomes: HashMap::from([
                (1, JanitorEligibility::Protected),
                (2, JanitorEligibility::Protected),
            ]),
            ..FakeJanitorMetadata::eligible()
        };
        let stats = run_one_cycle(JanitorCycleContext {
            store: &store,
            prefix: "test/objects/",
            dry_run: false,
            skip_newer_than_secs: 60,
            budget: RunBudget::deletes(1),
            metadata: &mut metadata,
        })
        .await
        .unwrap();

        assert_eq!(stats.referenced_files, 2);
        assert_eq!(stats.orphan_candidates, 1);
        assert_eq!(stats.deleted, 1);
        assert_eq!(*store.deleted.lock().unwrap(), canonical_keys(3));
        assert_eq!(metadata.claims, [3]);
        assert_eq!(
            metadata.finishes,
            [(3, fake_token(3), UploadedObjectDeletionOutcome::Deleted)]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn claim_protection_and_claim_failure_prevent_every_delete() {
        let entries: Vec<_> = canonical_keys(1)
            .into_iter()
            .map(|key| JanitorEntry {
                key,
                is_file: true,
                last_modified: None,
            })
            .collect();
        let store = FakeJanitorStore {
            entries: entries.clone(),
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata {
            claim_protected: vec![1],
            ..FakeJanitorMetadata::eligible()
        };
        let stats = run_one_cycle(fake_cycle_context(&store, &mut metadata))
            .await
            .unwrap();
        assert_eq!(
            (stats.claims, stats.claim_protected, stats.deleted),
            (1, 1, 0)
        );
        assert!(store.deleted.lock().unwrap().is_empty());
        assert!(metadata.finishes.is_empty());

        let store = FakeJanitorStore {
            entries,
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata {
            claim_error: Some("backend_unavailable: claim timed out"),
            ..FakeJanitorMetadata::eligible()
        };
        assert!(
            run_one_cycle(fake_cycle_context(&store, &mut metadata))
                .await
                .is_err()
        );
        assert!(store.deleted.lock().unwrap().is_empty());
        assert!(metadata.finishes.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unconfirmed_finish_and_failed_delete_are_not_reported_as_deleted() {
        let entries: Vec<_> = canonical_keys(2)
            .into_iter()
            .map(|key| JanitorEntry {
                key,
                is_file: true,
                last_modified: None,
            })
            .collect();
        // A stale finish token after successful S3 deletes must not count as success.
        let store = FakeJanitorStore {
            entries: entries.clone(),
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata {
            finish_error: Some("stale_claim: deletion claim is no longer current"),
            ..FakeJanitorMetadata::eligible()
        };
        let stats = run_one_cycle(fake_cycle_context(&store, &mut metadata))
            .await
            .unwrap();
        assert_eq!(
            (stats.deleted, stats.delete_errors, stats.claim_finishes),
            (0, 1, 0)
        );
        assert_eq!(*store.deleted.lock().unwrap(), canonical_keys(2));

        // A failed or hung S3 delete records a retryable outcome under the same claim.
        for (fail_delete, hang_delete) in [(true, false), (false, true)] {
            let store = FakeJanitorStore {
                entries: entries.clone(),
                fail_delete,
                hang_delete,
                ..FakeJanitorStore::default()
            };
            let mut metadata = FakeJanitorMetadata::eligible();
            let stats = run_one_cycle(fake_cycle_context(&store, &mut metadata))
                .await
                .unwrap();
            assert_eq!((stats.deleted, stats.delete_errors), (0, 1));
            assert_eq!(
                metadata.finishes,
                [(2, fake_token(2), UploadedObjectDeletionOutcome::Retryable)]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn listing_timestamps_decide_object_age_conservatively_without_head() {
        let ago = |secs| Some(SystemTime::now() - Duration::from_secs(secs));
        let ahead = Some(SystemTime::now() + Duration::from_secs(300));
        let entry = |key: String, last_modified| JanitorEntry {
            key,
            is_file: true,
            last_modified,
        };
        let legacy = |id| studiofs_data::object::format_object_key("test/objects/", id);
        let fragment =
            |id| studiofs_data::object::format_object_fragment_keys("test/objects/", id)[0].clone();
        // Entries follow S3's lexicographic listing order.
        let entries = vec![
            // Old legacy key plus a young fragment: the newest key decides, so it is young.
            entry(legacy(1), ago(3600)),
            entry(fragment(1), ago(30)),
            // A duplicated listing of one key is one object, claimed once.
            entry(legacy(2), ago(3600)),
            entry(legacy(2), ago(3600)),
            entry(fragment(2), ago(3600)),
            // Clock skew: a future-dated key makes the whole object's age unknown.
            entry(legacy(3), ago(3600)),
            entry(fragment(3), ahead),
            // Only the key listed without a timestamp is HEADed.
            entry(legacy(4), ago(3600)),
            entry(fragment(4), None),
            // Malformed paths are never parsed into an object.
            entry(format!("{}x", legacy(6)), ago(3600)),
            // An old pending-deletion mark does not override a young listed key.
            entry(legacy(7), ago(30)),
            entry("test/objects/zzzz/not-an-object".to_owned(), ago(3600)),
        ];
        let store = FakeJanitorStore {
            entries,
            ..FakeJanitorStore::default()
        };
        let pending_long_ago = i64::try_from(
            (SystemTime::now() - Duration::from_secs(3600))
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap();
        let mut metadata = FakeJanitorMetadata {
            outcomes: HashMap::from([(
                7,
                JanitorEligibility::Eligible {
                    pending_deletion: Some(pending_long_ago),
                },
            )]),
            ..FakeJanitorMetadata::eligible()
        };
        let mut ctx = fake_cycle_context(&store, &mut metadata);
        ctx.budget.max_deletes = u64::MAX;
        let stats = run_one_cycle(ctx).await.unwrap();

        assert_eq!(stats.skipped_non_canonical, 2);
        assert_eq!(stats.orphan_candidates, 5);
        assert_eq!(stats.skipped_young, 2);
        assert_eq!(stats.skipped_unknown_age, 1);
        assert_eq!(stats.head_requests, 1);
        assert_eq!(store.head_requests.load(Ordering::Relaxed), 1);
        assert_eq!(metadata.claims, vec![2, 4]);
        assert_eq!(stats.deleted, 2);
        let expected: Vec<String> = canonical_keys(2)
            .into_iter()
            .chain(canonical_keys(4))
            .collect();
        assert_eq!(*store.deleted.lock().unwrap(), expected);

        // A key listed out of order fails the cycle before its object is judged.
        let store = FakeJanitorStore {
            entries: vec![
                entry(legacy(8), ago(3600)),
                entry(legacy(9), ago(3600)),
                entry(fragment(8), ago(30)),
            ],
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let mut ctx = fake_cycle_context(&store, &mut metadata);
        ctx.budget.max_deletes = u64::MAX;
        let error = run_one_cycle(ctx).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("not in lexicographic order after 3 entries"),
            "{error:#}"
        );
        assert!(metadata.claims.is_empty());
        assert!(store.deleted.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn legacy_keys_are_cleaned_and_non_canonical_keys_are_never_touched() {
        let store = FakeJanitorStore {
            entries: [
                studiofs_data::object::format_object_key("test/objects/", 5),
                "test/objects/0007/0000000000000007/extra".to_string(),
                "test/objects/unrelated/0000000000000006".to_string(),
            ]
            .into_iter()
            .map(|key| JanitorEntry {
                key,
                is_file: true,
                last_modified: None,
            })
            .collect(),
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let stats = run_one_cycle(fake_cycle_context(&store, &mut metadata))
            .await
            .unwrap();
        assert_eq!(stats.skipped_non_canonical, 2);
        assert_eq!(metadata.eligibility_reads, [vec![5]]);
        assert_eq!(*store.deleted.lock().unwrap(), canonical_keys(5));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_missing_or_failed_eligibility_aborts_batch_before_any_deletes() {
        for failure in ["unknown", "missing", "error"] {
            let entries = [1, 2]
                .into_iter()
                .flat_map(|id| {
                    studiofs_data::object::format_object_fragment_keys("test/objects/", id)
                })
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect();
            let store = FakeJanitorStore {
                entries,
                ..FakeJanitorStore::default()
            };
            let eligible = JanitorEligibility::Eligible {
                pending_deletion: None,
            };
            let mut metadata = match failure {
                "unknown" => FakeJanitorMetadata {
                    outcomes: HashMap::from([(1, eligible), (2, JanitorEligibility::Unknown)]),
                    ..FakeJanitorMetadata::eligible()
                },
                "missing" => FakeJanitorMetadata {
                    outcomes: HashMap::from([(1, eligible)]),
                    default_eligibility: None,
                    ..FakeJanitorMetadata::eligible()
                },
                _ => FakeJanitorMetadata {
                    eligibility_error: Some("metadata query failed"),
                    ..FakeJanitorMetadata::eligible()
                },
            };

            assert!(
                run_one_cycle(fake_cycle_context(&store, &mut metadata))
                    .await
                    .is_err()
            );
            assert_eq!(metadata.eligibility_reads, [vec![1, 2]], "{failure}");
            assert_eq!(store.head_requests.load(Ordering::Relaxed), 0, "{failure}");
            assert!(store.deleted.lock().unwrap().is_empty(), "{failure}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eligibility_requests_are_split_into_bounded_batches() {
        let entries = (1..=129)
            .flat_map(|id| studiofs_data::object::format_object_fragment_keys("test/objects/", id))
            .map(|key| JanitorEntry {
                key,
                is_file: true,
                last_modified: None,
            })
            .collect();
        let store = FakeJanitorStore {
            entries,
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let stats = run_one_cycle(JanitorCycleContext {
            store: &store,
            prefix: "test/objects/",
            dry_run: true,
            skip_newer_than_secs: 0,
            budget: RunBudget::deletes(u64::MAX),
            metadata: &mut metadata,
        })
        .await
        .unwrap();

        assert_eq!(
            metadata
                .eligibility_reads
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            [64, 64, 1]
        );
        assert!(
            metadata
                .eligibility_reads
                .iter()
                .all(|batch| batch.len() <= MAX_JANITOR_ELIGIBILITY_BATCH)
        );
        assert_eq!(stats.orphan_candidates, 129);
    }

    #[test]
    fn metadata_owned_runtime_works_inside_the_cli_runtime() {
        let entries = studiofs_data::object::format_object_fragment_keys("test/objects/", 1)
            .into_iter()
            .map(|key| JanitorEntry {
                key,
                is_file: true,
                last_modified: None,
            })
            .collect();
        let store = FakeJanitorStore {
            entries,
            ..FakeJanitorStore::default()
        };
        let metadata_runtime = std::sync::Arc::new(tokio::runtime::Runtime::new().unwrap());
        let mut metadata = FakeJanitorMetadata {
            nested_runtime: Some(std::sync::Arc::clone(&metadata_runtime)),
            ..FakeJanitorMetadata::eligible()
        };
        let cli_runtime = tokio::runtime::Runtime::new().unwrap();
        let stats = cli_runtime
            .block_on(run_one_cycle(fake_cycle_context(&store, &mut metadata)))
            .unwrap();

        assert_eq!(stats.orphan_candidates, 1);
        assert_eq!(metadata.eligibility_reads.len(), 1);
        drop(cli_runtime);
        drop(metadata);
        drop(metadata_runtime);
    }

    /// Every fragment key of objects `1..=count`, in listing order.
    fn fragment_entries(count: u64) -> Vec<JanitorEntry> {
        (1..=count)
            .flat_map(|id| studiofs_data::object::format_object_fragment_keys("test/objects/", id))
            .map(|key| JanitorEntry {
                key,
                is_file: true,
                last_modified: None,
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failing_or_stalled_listing_page_stops_cycle_after_completed_batches() {
        let batch = MAX_JANITOR_ELIGIBILITY_BATCH as u64;
        let failing = FakeJanitorStore {
            entries: fragment_entries(batch + 6),
            // Fail inside the second batch, after the first batch's entries.
            fail_list_after: Some(
                (MAX_JANITOR_ELIGIBILITY_BATCH + 3) * studiofs_data::object::CLOUD_FRAGMENT_COUNT,
            ),
            ..FakeJanitorStore::default()
        };
        let stalled = FakeJanitorStore {
            entries: fragment_entries(batch + 6),
            hang_list: true,
            ..FakeJanitorStore::default()
        };
        for store in [failing, stalled] {
            let mut metadata = FakeJanitorMetadata::eligible();
            let mut ctx = fake_cycle_context(&store, &mut metadata);
            ctx.budget.max_deletes = u64::MAX;
            let error = run_one_cycle(ctx).await.unwrap_err();
            assert!(
                format!("{error:#}").contains("failed to list janitor objects"),
                "{error:#}"
            );
            // The batch completed before the failing page stays deleted and finished;
            // the partially buffered batch is never acted on.
            let finished: Vec<u64> = metadata.finishes.iter().map(|(id, ..)| *id).collect();
            assert_eq!(finished, (1..=batch).collect::<Vec<_>>());
            assert_eq!(metadata.eligibility_reads.len(), 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn janitor_drains_ten_thousand_gc_candidates_in_bounded_pages_before_listing() {
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap();
        let mut metadata = FakeJanitorMetadata {
            gc_candidates: (1..=10_000).map(|id| (id, now - 3600)).collect(),
            ..FakeJanitorMetadata::eligible()
        };
        let store = FakeJanitorStore::default();
        let stats = run_one_cycle(JanitorCycleContext {
            store: &store,
            prefix: "test/objects/",
            dry_run: true,
            skip_newer_than_secs: 0,
            budget: RunBudget::deletes(10_000),
            metadata: &mut metadata,
        })
        .await
        .expect("drain GC candidates");

        assert_eq!(stats.deleted, 10_000);
        assert_eq!(stats.queued_candidates_scanned, 10_000);
        assert_eq!(metadata.gc_candidate_reads.len(), 157);
        assert!(
            metadata
                .gc_candidate_reads
                .iter()
                .all(|(_, limit)| *limit <= MAX_JANITOR_ELIGIBILITY_BATCH)
        );
        assert_eq!(store.list_requests.load(Ordering::Relaxed), 0);
        assert!(store.deleted.lock().unwrap().is_empty());
        assert_eq!(
            metadata.gc_candidate_backfills, 0,
            "dry run must not write the queue"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_candidate_acknowledges_only_confirmed_deletion_and_keeps_failures() {
        let old = i64::try_from(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap()
            - 3600;
        let store = FakeJanitorStore {
            entries: studiofs_data::object::format_object_fragment_keys("test/objects/", 5)
                .into_iter()
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect(),
            fail_delete: true,
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata {
            gc_candidates: vec![(5, old)],
            ..FakeJanitorMetadata::eligible()
        };
        let stats = run_one_cycle(JanitorCycleContext {
            store: &store,
            prefix: "test/objects/",
            dry_run: false,
            skip_newer_than_secs: 0,
            budget: RunBudget::deletes(2),
            metadata: &mut metadata,
        })
        .await
        .expect("record retryable deletion");
        assert_eq!(stats.delete_errors, 1);
        assert_eq!(stats.skipped_deferred, 1);
        assert_eq!(metadata.claims, vec![5]);
        assert_eq!(metadata.gc_candidate_retries, vec![5]);
        assert!(metadata.gc_candidate_acks.is_empty());

        let store = FakeJanitorStore::default();
        let mut metadata = FakeJanitorMetadata {
            gc_candidates: vec![(6, old)],
            ..FakeJanitorMetadata::eligible()
        };
        let stats = run_one_cycle(JanitorCycleContext {
            store: &store,
            prefix: "test/objects/",
            dry_run: false,
            skip_newer_than_secs: 0,
            budget: RunBudget::deletes(1),
            metadata: &mut metadata,
        })
        .await
        .expect("confirm deletion");
        assert_eq!(stats.deleted, 1);
        assert_eq!(metadata.gc_candidate_acks, vec![6]);
        assert!(metadata.gc_candidate_backfills > 0);
        assert_eq!(store.list_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_ack_failure_is_not_fatal_and_protected_rows_are_deferred() {
        let old = i64::try_from(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap()
            - 3600;
        let store = FakeJanitorStore::default();
        let mut metadata = FakeJanitorMetadata {
            gc_candidates: vec![(7, old), (8, old)],
            outcomes: [(7, JanitorEligibility::Protected)].into_iter().collect(),
            ack_error: Some("acknowledgement updated no rows"),
            ..FakeJanitorMetadata::eligible()
        };
        let stats = run_one_cycle(JanitorCycleContext {
            store: &store,
            prefix: "test/objects/",
            dry_run: false,
            skip_newer_than_secs: 0,
            budget: RunBudget::deletes(1),
            metadata: &mut metadata,
        })
        .await
        .expect("an ack failure after a confirmed deletion must not abort the cycle");
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.referenced_files, 1);
        assert_eq!(metadata.gc_candidate_acks, vec![8]);
        assert_eq!(metadata.gc_candidate_retries, vec![7]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fake_store_failures_skip_deletion_and_not_found_fragments_are_tolerated() {
        let entries = studiofs_data::object::format_object_fragment_keys("test/objects/", 4)
            .into_iter()
            .map(|key| studiofs_data::janitor::JanitorEntry {
                key,
                is_file: true,
                last_modified: None,
            })
            .collect::<Vec<_>>();
        let listing_failure = FakeJanitorStore {
            fail_list: true,
            ..FakeJanitorStore::default()
        };
        let mut listing_metadata = FakeJanitorMetadata::eligible();
        assert!(
            run_one_cycle(fake_cycle_context(&listing_failure, &mut listing_metadata))
                .await
                .is_err()
        );
        assert!(listing_failure.deleted.lock().unwrap().is_empty());

        let head_failure = FakeJanitorStore {
            entries: entries.clone(),
            fail_head: true,
            ..FakeJanitorStore::default()
        };
        let mut head_metadata = FakeJanitorMetadata::eligible();
        let stats = run_one_cycle(fake_cycle_context(&head_failure, &mut head_metadata))
            .await
            .unwrap();
        assert_eq!(stats.skipped_unknown_age, 1);
        assert_eq!(stats.head_errors, 1);
        assert_eq!(stats.deleted, 0);
        assert!(head_failure.deleted.lock().unwrap().is_empty());

        let delete_failure = FakeJanitorStore {
            entries: entries.clone(),
            fail_delete: true,
            ..FakeJanitorStore::default()
        };
        let mut delete_metadata = FakeJanitorMetadata::eligible();
        let stats = run_one_cycle(fake_cycle_context(&delete_failure, &mut delete_metadata))
            .await
            .unwrap();
        assert_eq!(stats.delete_errors, 1);
        assert_eq!(stats.deleted, 0);

        let missing_fragments = FakeJanitorStore {
            entries,
            missing_delete: true,
            ..FakeJanitorStore::default()
        };
        let mut missing_metadata = FakeJanitorMetadata::eligible();
        let stats = run_one_cycle(fake_cycle_context(
            &missing_fragments,
            &mut missing_metadata,
        ))
        .await
        .unwrap();
        assert_eq!(stats.delete_errors, 0);
        assert_eq!(stats.deleted, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fake_store_age_and_cap_gates_skip_deletion() {
        let entries_for = |object_ids: &[u64]| {
            object_ids
                .iter()
                .flat_map(|&id| {
                    studiofs_data::object::format_object_fragment_keys("test/objects/", id)
                })
                .map(|key| JanitorEntry {
                    key,
                    is_file: true,
                    last_modified: None,
                })
                .collect::<Vec<_>>()
        };
        // Unknown and young HEAD ages never delete.
        for (store, young, unknown) in [
            (
                FakeJanitorStore {
                    entries: entries_for(&[5]),
                    head_missing: true,
                    ..FakeJanitorStore::default()
                },
                0,
                1,
            ),
            (
                FakeJanitorStore {
                    entries: entries_for(&[5]),
                    head_young: true,
                    ..FakeJanitorStore::default()
                },
                1,
                0,
            ),
        ] {
            let mut metadata = FakeJanitorMetadata::eligible();
            let stats = run_one_cycle(fake_cycle_context(&store, &mut metadata))
                .await
                .unwrap();
            assert_eq!(
                (stats.skipped_young, stats.skipped_unknown_age),
                (young, unknown)
            );
            assert_eq!(stats.deleted, 0);
            assert!(store.deleted.lock().unwrap().is_empty());
        }

        // Pending-deletion timestamps decide age without HEAD; future timestamps are unknown.
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap();
        let store = FakeJanitorStore {
            entries: entries_for(&[6, 7, 8]),
            fail_head: true,
            ..FakeJanitorStore::default()
        };
        let pending = HashMap::from([
            (
                6,
                JanitorEligibility::Eligible {
                    pending_deletion: Some(now),
                },
            ),
            (
                7,
                JanitorEligibility::Eligible {
                    pending_deletion: Some(now + 3600),
                },
            ),
            (
                8,
                JanitorEligibility::Eligible {
                    pending_deletion: Some(now - 3600),
                },
            ),
        ]);
        let mut metadata = FakeJanitorMetadata {
            outcomes: pending,
            default_eligibility: Some(JanitorEligibility::Eligible {
                pending_deletion: None,
            }),
            ..FakeJanitorMetadata::eligible()
        };
        let stats = run_one_cycle(fake_cycle_context(&store, &mut metadata))
            .await
            .unwrap();
        assert_eq!(stats.head_requests, 0);
        assert_eq!(stats.skipped_young, 1);
        assert_eq!(stats.skipped_unknown_age, 1);
        assert_eq!(stats.deleted, 1);
        assert_eq!(*store.deleted.lock().unwrap(), canonical_keys(8));

        // The delete cap stops further candidates before any HEAD or delete request.
        let store = FakeJanitorStore {
            entries: entries_for(&[9, 10, 11]),
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let stats = run_one_cycle(fake_cycle_context(&store, &mut metadata))
            .await
            .unwrap();
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.outcome, pipeline::CycleOutcome::DeleteCapReached);
        assert_eq!(stats.head_requests, 1);
        assert_eq!(*store.deleted.lock().unwrap(), canonical_keys(9));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_cap_counts_only_confirmed_successes_and_stops_listing() {
        // Object 1 is claim-protected, 2 is referenced: neither consumes the cap.
        let store = FakeJanitorStore {
            entries: fragment_entries(MAX_JANITOR_ELIGIBILITY_BATCH as u64 * 3),
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata {
            claim_protected: vec![1],
            outcomes: HashMap::from([(2, JanitorEligibility::Protected)]),
            ..FakeJanitorMetadata::eligible()
        };
        let mut ctx = fake_cycle_context(&store, &mut metadata);
        ctx.budget.max_deletes = 2;
        let stats = run_one_cycle(ctx).await.unwrap();

        assert_eq!(stats.outcome, pipeline::CycleOutcome::DeleteCapReached);
        assert_eq!(stats.deleted, 2);
        assert_eq!(stats.claim_protected, 1);
        assert_eq!(stats.referenced_files, 1);
        assert_eq!(metadata.claims, vec![1, 3, 4]);
        // The deletion that met the cap still ran through its PostgreSQL finish.
        assert_eq!(
            metadata.finishes,
            vec![
                (3, fake_token(3), UploadedObjectDeletionOutcome::Deleted),
                (4, fake_token(4), UploadedObjectDeletionOutcome::Deleted),
            ]
        );
        let mut expected = canonical_keys(3);
        expected.extend(canonical_keys(4));
        assert_eq!(*store.deleted.lock().unwrap(), expected);
        // Only the first eligibility batch was listed: it closes on the first key of the
        // next object, and the tail is never scanned.
        assert_eq!(metadata.eligibility_reads.len(), 1);
        assert_eq!(
            stats.initial_listing_entries,
            (MAX_JANITOR_ELIGIBILITY_BATCH * studiofs_data::object::CLOUD_FRAGMENT_COUNT) as u64
                + 1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_deletes_never_consume_the_delete_cap() {
        let store = FakeJanitorStore {
            entries: fragment_entries(3),
            fail_delete: true,
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let stats = run_one_cycle(fake_cycle_context(&store, &mut metadata))
            .await
            .unwrap();

        assert_eq!(stats.outcome, pipeline::CycleOutcome::Complete);
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.delete_errors, 3);
        assert_eq!(metadata.claims, vec![1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_budget_stops_listing_and_finishes_already_listed_candidates() {
        // Objects 1..=3 are listed whole, but the listing is cut before object 4 starts,
        // so object 3 is not known to be complete and waits for the next run.
        let fragments = studiofs_data::object::CLOUD_FRAGMENT_COUNT as u64;
        let store = FakeJanitorStore {
            entries: fragment_entries(10),
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let mut ctx = fake_cycle_context(&store, &mut metadata);
        ctx.budget.max_deletes = u64::MAX;
        ctx.budget.max_scan_entries = Some(3 * fragments);
        let stats = run_one_cycle(ctx).await.unwrap();

        assert_eq!(stats.outcome, pipeline::CycleOutcome::ScanBudgetExhausted);
        assert_eq!(stats.initial_listing_entries, 3 * fragments);
        assert_eq!(stats.deleted, 2);
        assert_eq!(metadata.claims, vec![1, 2]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_head_probe_times_out_as_unknown_age() {
        let store = FakeJanitorStore {
            entries: fragment_entries(2),
            hang_head: true,
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let stats = tokio::time::timeout(
            Duration::from_secs(30),
            run_one_cycle(fake_cycle_context(&store, &mut metadata)),
        )
        .await
        .expect("a stalled HEAD must not hold the cycle")
        .unwrap();

        assert_eq!(stats.outcome, pipeline::CycleOutcome::Complete);
        assert_eq!(stats.skipped_unknown_age, 2);
        assert!(metadata.claims.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exhausted_time_budget_admits_no_listing_or_candidate() {
        let store = FakeJanitorStore {
            entries: fragment_entries(3),
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let mut ctx = fake_cycle_context(&store, &mut metadata);
        ctx.budget.max_runtime = Some(Duration::ZERO);
        let stats = run_one_cycle(ctx).await.unwrap();

        assert_eq!(stats.outcome, pipeline::CycleOutcome::TimeBudgetExhausted);
        assert_eq!(stats.initial_listing_entries, 0);
        assert_eq!(store.head_requests.load(Ordering::Relaxed), 0);
        assert!(metadata.claims.is_empty());
        assert!(metadata.eligibility_reads.is_empty());
    }

    struct FakeS3 {
        list_requests: AtomicU64,
        head_requests: AtomicU64,
        delete_requests: AtomicU64,
        list_requests_before_first_delete: AtomicU64,
        object_count: u64,
        /// Never answer this 0-based list page, simulating a stalled provider.
        stall_page: Option<u64>,
        response_delay: Duration,
        /// Hold each response until this many requests are active at once.
        response_barrier: Option<tokio::sync::Barrier>,
        active_requests: AtomicU64,
        peak_active_requests: AtomicU64,
        request_starts: std::sync::Mutex<Vec<Instant>>,
        /// Fault responses served, in order, before normal ones: an HTTP status line plus
        /// headers (`"503 Service Unavailable|Retry-After: 1"`), or `"DROP"` to close the
        /// connection without an answer.
        faults: std::sync::Mutex<std::collections::VecDeque<&'static str>>,
    }

    async fn fake_s3_request(listener: tokio::net::TcpListener, fake: std::sync::Arc<FakeS3>) {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let fake = fake.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};

                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let Ok(read) = stream.read(&mut buffer).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&buffer[..read]);
                }
                let active = fake.active_requests.fetch_add(1, Ordering::Relaxed) + 1;
                fake.peak_active_requests
                    .fetch_max(active, Ordering::Relaxed);
                let request = String::from_utf8_lossy(&request);
                fake.request_starts.lock().unwrap().push(Instant::now());
                let first_line = request.lines().next().unwrap_or_default();
                let (status, body, extra_headers) = if first_line.starts_with("GET ") {
                    fake.list_requests.fetch_add(1, Ordering::Relaxed);
                    // Token `pageN` requests the N-th (1-based) page.
                    let page = first_line
                        .split("continuation-token=page")
                        .nth(1)
                        .and_then(|rest| {
                            rest.split(|c: char| !c.is_ascii_digit())
                                .next()?
                                .parse::<u64>()
                                .ok()
                        })
                        .map_or(0, |page| page - 1);
                    if fake.stall_page == Some(page) {
                        std::future::pending::<()>().await;
                    }
                    let start = page * 1000 + 1;
                    let end = (start + 1000).min(fake.object_count + 1);
                    let contents = (start..end)
                        .map(|id| format!(
                            "<Contents><Key>test/objects/{:04x}/{id:016x}/0</Key><LastModified>2020-01-01T00:00:00.000Z</LastModified><ETag>\"tag\"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>",
                            id & 0xffff
                        ))
                        .collect::<String>();
                    let truncated = end <= fake.object_count;
                    let next = if truncated {
                        format!(
                            "<NextContinuationToken>page{}</NextContinuationToken>",
                            page + 2
                        )
                    } else {
                        String::new()
                    };
                    let body = format!(
                        "<ListBucketResult><Name>test</Name><Prefix>test/objects/</Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>{truncated}</IsTruncated>{next}{contents}</ListBucketResult>",
                        end - start
                    );
                    ("200 OK", body, "Content-Type: application/xml\r\n")
                } else if first_line.starts_with("HEAD ") {
                    fake.head_requests.fetch_add(1, Ordering::Relaxed);
                    (
                        "200 OK",
                        String::new(),
                        "Last-Modified: Wed, 01 Jan 2020 00:00:00 GMT\r\n",
                    )
                } else if first_line.starts_with("DELETE ") {
                    if fake.delete_requests.fetch_add(1, Ordering::Relaxed) == 0 {
                        fake.list_requests_before_first_delete.store(
                            fake.list_requests.load(Ordering::Relaxed),
                            Ordering::Relaxed,
                        );
                    }
                    ("204 No Content", String::new(), "")
                } else {
                    ("501 Not Implemented", String::new(), "")
                };
                let fault = fake.faults.lock().unwrap().pop_front();
                if fault == Some("DROP") {
                    fake.active_requests.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
                let fault_headers;
                let (status, body, extra_headers) = match fault {
                    Some(fault) => {
                        let (status, headers) = fault.split_once('|').unwrap_or((fault, ""));
                        fault_headers = if headers.is_empty() {
                            String::new()
                        } else {
                            format!("{headers}\r\n")
                        };
                        (status, String::new(), fault_headers.as_str())
                    }
                    None => (status, body, extra_headers),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                if !fake.response_delay.is_zero() {
                    tokio::time::sleep(fake.response_delay).await;
                }
                if let Some(barrier) = &fake.response_barrier {
                    // Bounded: a missing overlap fails the peak assertion instead of hanging.
                    let _ = tokio::time::timeout(Duration::from_secs(5), barrier.wait()).await;
                }
                let _ = stream.write_all(response.as_bytes()).await;
                fake.active_requests.fetch_sub(1, Ordering::Relaxed);
            });
        }
    }

    async fn fake_s3_operator(
        object_count: u64,
    ) -> (
        Operator,
        std::sync::Arc<FakeS3>,
        String,
        tokio::task::JoinHandle<()>,
    ) {
        fake_s3_operator_with_stall(object_count, None).await
    }

    async fn fake_s3_operator_with_stall(
        object_count: u64,
        stall_page: Option<u64>,
    ) -> (
        Operator,
        std::sync::Arc<FakeS3>,
        String,
        tokio::task::JoinHandle<()>,
    ) {
        fake_s3_operator_with_options(object_count, stall_page, Duration::ZERO, None).await
    }

    async fn fake_s3_operator_with_options(
        object_count: u64,
        stall_page: Option<u64>,
        response_delay: Duration,
        response_barrier: Option<tokio::sync::Barrier>,
    ) -> (
        Operator,
        std::sync::Arc<FakeS3>,
        String,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let fake = std::sync::Arc::new(FakeS3 {
            list_requests: AtomicU64::new(0),
            head_requests: AtomicU64::new(0),
            delete_requests: AtomicU64::new(0),
            list_requests_before_first_delete: AtomicU64::new(0),
            object_count,
            stall_page,
            response_delay,
            response_barrier,
            active_requests: AtomicU64::new(0),
            peak_active_requests: AtomicU64::new(0),
            request_starts: std::sync::Mutex::new(Vec::new()),
            faults: std::sync::Mutex::default(),
        });
        let task = tokio::spawn(fake_s3_request(listener, fake.clone()));
        let op = Operator::new(
            S3::default()
                .endpoint(&endpoint)
                .bucket("test")
                .access_key_id("test")
                .secret_access_key("test")
                .region("us-east-1")
                .disable_config_load()
                .disable_ec2_metadata(),
        )
        .unwrap()
        .finish();
        (op, fake, endpoint, task)
    }

    #[tokio::test]
    async fn s3_transport_budget_limits_live_http_requests_and_request_rate() {
        let (_op, fake, endpoint, server) =
            fake_s3_operator_with_options(0, None, Duration::from_millis(40), None).await;
        let store = studiofs_data::janitor::JanitorObjectStore::from_s3(
            &endpoint,
            "test",
            "test",
            "test",
            "us-east-1",
            1,
            32,
        )
        .unwrap();

        let (list, head, delete) = tokio::join!(
            store.list("test/objects/"),
            store.stat_key("test/objects/0000/0000000000000001/0"),
            store.delete("test/objects/0000/0000000000000001/0"),
        );
        let mut listing = list.unwrap();
        assert!(listing.next().await.unwrap().is_none());
        assert!(matches!(
            head,
            Ok(studiofs_data::janitor::JanitorKeyStat::Found(_))
        ));
        assert!(delete.is_ok());
        assert_eq!(fake.peak_active_requests.load(Ordering::Relaxed), 1);
        assert_eq!(fake.list_requests.load(Ordering::Relaxed), 1);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 1);
        assert_eq!(fake.delete_requests.load(Ordering::Relaxed), 1);
        server.abort();
        let _ = server.await;

        let (_op, fake, endpoint, server) = fake_s3_operator_with_options(
            0,
            None,
            Duration::ZERO,
            Some(tokio::sync::Barrier::new(2)),
        )
        .await;
        let store = studiofs_data::janitor::JanitorObjectStore::from_s3(
            &endpoint,
            "test",
            "test",
            "test",
            "us-east-1",
            2,
            32,
        )
        .unwrap();
        let (first, second, third, fourth) = tokio::join!(
            store.stat_key("test/objects/0000/0000000000000001/0"),
            store.stat_key("test/objects/0000/0000000000000002/0"),
            store.stat_key("test/objects/0000/0000000000000003/0"),
            store.stat_key("test/objects/0000/0000000000000004/0"),
        );
        assert!(first.is_ok() && second.is_ok() && third.is_ok() && fourth.is_ok());
        assert_eq!(fake.peak_active_requests.load(Ordering::Relaxed), 2);
        server.abort();
        let _ = server.await;

        let (_op, fake, endpoint, server) =
            fake_s3_operator_with_options(0, None, Duration::ZERO, None).await;
        let store = studiofs_data::janitor::JanitorObjectStore::from_s3(
            &endpoint,
            "test",
            "test",
            "test",
            "us-east-1",
            2,
            2,
        )
        .unwrap();
        let (first, second) = tokio::join!(
            store.stat_key("test/objects/0000/0000000000000001/0"),
            store.stat_key("test/objects/0000/0000000000000002/0"),
        );
        assert!(first.is_ok() && second.is_ok());
        let request_starts = fake.request_starts.lock().unwrap().clone();
        assert_eq!(request_starts.len(), 2);
        assert!(request_starts[1].duration_since(request_starts[0]) >= Duration::from_millis(400));
        assert!(fake.peak_active_requests.load(Ordering::Relaxed) <= 2);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 2);
        server.abort();
        let _ = server.await;
    }

    const FAULT_KEY: &str = "test/objects/0000/0000000000000001/0";

    fn fault_store(endpoint: &str) -> studiofs_data::janitor::JanitorObjectStore {
        studiofs_data::janitor::JanitorObjectStore::from_s3(
            endpoint,
            "test",
            "test",
            "test",
            "us-east-1",
            2,
            32,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn s3_throttle_honors_retry_after_and_slows_the_request_gate() {
        let (_op, fake, endpoint, server) = fake_s3_operator(0).await;
        fake.faults.lock().unwrap().extend([
            "503 Service Unavailable|Retry-After: 1",
            "429 Too Many Requests",
        ]);
        let store = fault_store(&endpoint);
        let base = Duration::from_secs(1) / 32;

        let stat = store.stat_key(FAULT_KEY).await;
        assert!(
            matches!(stat, Ok(studiofs_data::janitor::JanitorKeyStat::Found(_))),
            "{stat:?}"
        );
        // One attempt plus one retry per throttle, then success: no unbounded retrying.
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 3);
        let transport = store.transport_stats();
        assert_eq!((transport.throttled, transport.retries), (2, 2));
        // Two throttles doubled the spacing twice; one success shrank it by only 1/8.
        assert!(transport.request_interval > base * 3, "{transport:?}");
        let starts = fake.request_starts.lock().unwrap().clone();
        assert!(
            starts[1].duration_since(starts[0]) >= Duration::from_secs(1),
            "Retry-After must hold back the next request"
        );
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn s3_sustained_throttle_and_dropped_connections_stop_after_bounded_retries() {
        let (_op, fake, endpoint, server) = fake_s3_operator(0).await;
        fake.faults.lock().unwrap().extend([
            "DROP",
            "503 Service Unavailable",
            "503 Service Unavailable",
            "503 Service Unavailable",
            "503 Service Unavailable",
        ]);
        let store = fault_store(&endpoint);
        let base = Duration::from_secs(1) / 32;

        let result = store.delete(FAULT_KEY).await;
        assert!(
            matches!(result, Err(JanitorDeleteError::Other(_))),
            "{result:?}"
        );
        let attempts = 1 + studiofs_data::janitor::JANITOR_S3_MAX_RETRIES as u64;
        assert_eq!(fake.delete_requests.load(Ordering::Relaxed), attempts);
        assert_eq!(fake.faults.lock().unwrap().len(), 1, "no extra attempt");
        let transport = store.transport_stats();
        assert_eq!(transport.retries, attempts - 1);
        assert_eq!(transport.throttled, attempts);
        // Every failure halved the request rate.
        assert!(transport.request_interval >= base * 16, "{transport:?}");
        let starts = fake.request_starts.lock().unwrap().clone();
        let gaps: Vec<_> = starts.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(gaps.windows(2).all(|w| w[1] >= w[0]), "{gaps:?}");
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn s3_permission_denied_is_terminal_and_vanished_keys_are_not_failures() {
        use studiofs_data::janitor::{JanitorKeyStat, classify_object_error};

        let (_op, fake, endpoint, server) = fake_s3_operator(0).await;
        fake.faults.lock().unwrap().extend([
            "403 Forbidden",
            "403 Forbidden",
            "404 Not Found",
            "404 Not Found",
        ]);
        let store = fault_store(&endpoint);

        let delete = store.delete(FAULT_KEY).await;
        assert!(
            matches!(delete, Err(JanitorDeleteError::Terminal(_))),
            "{delete:?}"
        );
        let stat = store.stat_key(FAULT_KEY).await.unwrap_err();
        assert_eq!(classify_object_error(&stat), JanitorFailureClass::Terminal);
        // An already-deleted key is success for DELETE and absence for HEAD.
        let vanished = store.delete(FAULT_KEY).await;
        assert!(
            matches!(vanished, Ok(()) | Err(JanitorDeleteError::NotFound)),
            "{vanished:?}"
        );
        assert!(matches!(
            store.stat_key(FAULT_KEY).await,
            Ok(JanitorKeyStat::Missing)
        ));
        assert_eq!(fake.delete_requests.load(Ordering::Relaxed), 2);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 2);
        assert_eq!(store.transport_stats().retries, 0);
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn failure_classes_follow_the_retry_policy() {
        use opendal::ErrorKind;
        use studiofs_meta::JanitorMetadataError;

        let class = |error: anyhow::Error| classify_error(&error);
        let transient = JanitorFailureClass::Transient;
        let terminal = JanitorFailureClass::Terminal;
        assert_eq!(
            class(JanitorMetadataError::BackendUnavailable("connection reset".into()).into()),
            transient
        );
        assert_eq!(
            class(anyhow::Error::new(JanitorMetadataError::StaleClaim).context("finish")),
            transient
        );
        assert_eq!(
            class(JanitorMetadataError::UnsafeToDelete("volume binding mismatch").into()),
            terminal
        );
        assert_eq!(
            class(anyhow::Error::new(pipeline::OperationTimedOut("listing page")).context("list")),
            transient
        );
        assert_eq!(
            class(opendal::Error::new(ErrorKind::RateLimited, "slow down").into()),
            transient
        );
        assert_eq!(
            class(
                opendal::Error::new(ErrorKind::Unexpected, "reset")
                    .set_temporary()
                    .into()
            ),
            transient
        );
        assert_eq!(
            class(opendal::Error::new(ErrorKind::PermissionDenied, "denied").into()),
            terminal
        );
        assert_eq!(
            class(opendal::Error::new(ErrorKind::ConfigInvalid, "NoSuchBucket").into()),
            terminal
        );
        // OpenDAL's retry layer marks final errors persistent; the kind still decides.
        assert_eq!(
            class(
                opendal::Error::new(ErrorKind::PermissionDenied, "denied")
                    .set_persistent()
                    .into()
            ),
            terminal
        );
        assert_eq!(class(anyhow::anyhow!("listing is out of order")), terminal);
    }

    #[test]
    fn loop_breaker_backs_off_recovers_and_stops_after_bounded_failures() {
        use pipeline::CycleOutcome::{Complete, Overloaded, ScanBudgetExhausted};
        use studiofs_meta::JanitorMetadataError;

        let interval = Duration::from_secs(60);
        let pg_down = || -> anyhow::Result<CycleOutcome> {
            Err(JanitorMetadataError::BackendUnavailable("connection reset".into()).into())
        };
        // A single run reports transient and overloaded cycles as failures; budgets are not.
        assert!(
            LoopBreaker::default()
                .after_cycle(pg_down(), false, interval)
                .is_err()
        );
        assert!(
            LoopBreaker::default()
                .after_cycle(Ok(Overloaded), false, interval)
                .is_err()
        );
        assert_eq!(
            LoopBreaker::default()
                .after_cycle(Ok(ScanBudgetExhausted), false, interval)
                .unwrap(),
            None
        );

        // Under --loop a PG disconnect or overload backs off; a healthy cycle resets it.
        let mut breaker = LoopBreaker::default();
        assert_eq!(
            breaker.after_cycle(pg_down(), true, interval).unwrap(),
            Some(interval)
        );
        assert_eq!(
            breaker.after_cycle(Ok(Overloaded), true, interval).unwrap(),
            Some(interval * 2)
        );
        assert_eq!(
            breaker.after_cycle(Ok(Complete), true, interval).unwrap(),
            Some(interval)
        );
        // Sustained failure grows the delay and then gives up instead of spinning.
        let delays: Vec<_> = (1..MAX_CONSECUTIVE_FAILED_CYCLES)
            .map(|_| breaker.after_cycle(pg_down(), true, interval).unwrap())
            .collect();
        assert_eq!(delays, [1, 2, 4, 8].map(|factor| Some(interval * factor)));
        let error = breaker.after_cycle(pg_down(), true, interval).unwrap_err();
        assert!(
            format!("{error:#}").contains("circuit open after 5 consecutive failed cycles"),
            "{error:#}"
        );

        // Terminal failures never retry, even in loop mode.
        let error = LoopBreaker::default()
            .after_cycle(
                Err(JanitorMetadataError::UnsafeToDelete("volume binding mismatch").into()),
                true,
                interval,
            )
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("terminal failure"),
            "{error:#}"
        );
    }

    #[test]
    fn transient_object_errors_fail_cycles_and_healthy_cycles_reset_loop_breaker() {
        use CycleOutcome::{Complete, DeleteCapReached, TransientErrors};

        let interval = Duration::from_secs(10);
        for (head_errors, delete_errors) in [(1, 0), (0, 1)] {
            let mut stats = pipeline::JanitorStats::default();
            stats.head_errors = head_errors;
            stats.delete_errors = delete_errors;
            assert_eq!(stats.cycle_outcome(), TransientErrors);
            assert!(
                LoopBreaker::default()
                    .after_cycle(Ok(stats.cycle_outcome()), false, interval)
                    .is_err()
            );

            let mut breaker = LoopBreaker::default();
            for _ in 0..MAX_CONSECUTIVE_FAILED_CYCLES - 1 {
                assert!(
                    breaker
                        .after_cycle(Ok(stats.cycle_outcome()), true, interval)
                        .unwrap()
                        .is_some()
                );
            }
            assert!(
                breaker
                    .after_cycle(Ok(stats.cycle_outcome()), true, interval)
                    .is_err()
            );
            assert_eq!(
                breaker.after_cycle(Ok(Complete), true, interval).unwrap(),
                Some(interval)
            );
            assert_eq!(
                breaker
                    .after_cycle(Ok(DeleteCapReached), true, interval)
                    .unwrap(),
                Some(interval)
            );
            assert!(
                breaker
                    .after_cycle(Ok(stats.cycle_outcome()), true, interval)
                    .unwrap()
                    .is_some()
            );
        }

        let mut budget_limited = pipeline::JanitorStats::default();
        budget_limited.outcome = DeleteCapReached;
        budget_limited.delete_errors = 1;
        assert_eq!(budget_limited.cycle_outcome(), TransientErrors);
        budget_limited.delete_errors = 0;
        assert_eq!(budget_limited.cycle_outcome(), DeleteCapReached);
        budget_limited.delete_errors = 1;
        budget_limited.outcome = CycleOutcome::Overloaded;
        assert_eq!(budget_limited.cycle_outcome(), CycleOutcome::Overloaded);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transient_object_failures_open_the_overload_circuit_without_losing_work() {
        let start = JanitorSweepCheckpoint {
            after_key: Some("test/objects/0000".to_string()),
            ..sweep_checkpoint()
        };
        let store = FakeJanitorStore {
            entries: fragment_entries(8),
            fail_delete: true,
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let mut ctx = fake_cycle_context(&store, &mut metadata);
        ctx.budget.max_deletes = u64::MAX;
        let stats = run_one_cycle_resumable(ctx, start.clone(), &sweep_scope(), true)
            .await
            .unwrap();

        let trip = u64::from(pipeline::OVERLOAD_TRIP_OBJECTS);
        assert_eq!(stats.outcome, pipeline::CycleOutcome::Overloaded);
        assert_eq!(metadata.claims, (1..=trip).collect::<Vec<_>>());
        // One DELETE per failed object; its other keys wait for the object's retry.
        assert_eq!(store.deleted.lock().unwrap().len() as u64, trip);
        // Every claim was released as retryable and its queue row deferred.
        assert!(
            metadata
                .finishes
                .iter()
                .all(|(_, _, outcome)| *outcome == UploadedObjectDeletionOutcome::Retryable)
        );
        assert_eq!(metadata.gc_candidate_retries, metadata.claims);
        // The listed batch replays next cycle: the saved scan position did not move.
        assert!(!stats.scan_complete);
        assert_eq!(metadata.sweep_checkpoint, Some(start));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_delete_failure_releases_its_claim_and_stops_the_cycle() {
        let store = FakeJanitorStore {
            entries: fragment_entries(3),
            deny_delete: true,
            ..FakeJanitorStore::default()
        };
        let mut metadata = FakeJanitorMetadata::eligible();
        let mut ctx = fake_cycle_context(&store, &mut metadata);
        ctx.budget.max_deletes = u64::MAX;
        let error = run_one_cycle(ctx).await.unwrap_err();

        assert_eq!(
            classify_error(&error),
            JanitorFailureClass::Terminal,
            "{error:#}"
        );
        assert_eq!(metadata.claims, [1]);
        assert_eq!(
            metadata.finishes,
            [(1, fake_token(1), UploadedObjectDeletionOutcome::Retryable)]
        );
        assert_eq!(metadata.gc_candidate_retries, [1]);
        assert_eq!(store.deleted.lock().unwrap().len(), 1);
    }

    fn create_local_metadata_fixture(meta_dir: &std::path::Path, endpoint: &str) -> PathBuf {
        create_metadata_fixture(
            meta_dir,
            endpoint,
            studiofs_meta::schema::MetadataConfig::default(),
        )
    }

    fn create_postgres_metadata_fixture(
        meta_dir: &std::path::Path,
        endpoint: &str,
        postgres_url: &str,
    ) -> PathBuf {
        use studiofs_meta::schema::{MetadataBackend, MetadataConfig, PostgresConfig};
        create_metadata_fixture(
            meta_dir,
            endpoint,
            MetadataConfig {
                backend: MetadataBackend::Postgres,
                postgres: Some(PostgresConfig {
                    url: postgres_url.to_string(),
                    volume_id: 966,
                }),
                ..MetadataConfig::default()
            },
        )
    }

    fn create_metadata_fixture(
        meta_dir: &std::path::Path,
        endpoint: &str,
        metadata: studiofs_meta::schema::MetadataConfig,
    ) -> PathBuf {
        use studiofs_meta::schema::{
            CURRENT_FORMAT_VERSION, DEFAULT_BLOCK_SIZE, ENCRYPTION_KEY_LENGTH, EncryptionConfig,
            FormatInfo,
        };

        std::fs::create_dir_all(meta_dir).unwrap();
        let name = "janitor-fixture";
        let db_path = meta_dir.join(format!("{name}.db"));
        let engine = RedbMetaEngine::open(&db_path).unwrap();
        engine
            .set_format_info(&FormatInfo {
                format_version: CURRENT_FORMAT_VERSION,
                name: name.to_string(),
                uuid: "janitor-fixture-uuid".to_string(),
                endpoint: endpoint.to_string(),
                bucket: "test".to_string(),
                access_key: "fixture".to_string(),
                secret_key: "fixture".to_string(),
                region: "us-east-1".to_string(),
                data_prefix: "test/objects/".to_string(),
                metadata,
                encryption: EncryptionConfig::new([0; ENCRYPTION_KEY_LENGTH]),
                block_size: DEFAULT_BLOCK_SIZE,
            })
            .unwrap();
        engine.init_root(0, 0).unwrap();
        drop(engine);
        db_path
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn janitor_delete_cap_stops_opendal_listing_after_first_page() {
        let (op, fake, _endpoint, server) = fake_s3_operator(20_005).await;
        let store = OpenDalJanitorStore(studiofs_data::janitor::JanitorObjectStore::new(op));
        let mut metadata = FakeJanitorMetadata::eligible();
        let stats = tokio::time::timeout(
            Duration::from_secs(30),
            run_one_cycle(JanitorCycleContext {
                store: &store,
                prefix: "test/objects/",
                dry_run: false,
                skip_newer_than_secs: 60,
                budget: RunBudget::deletes(2),
                metadata: &mut metadata,
            }),
        )
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
        let stats = stats.unwrap();

        // Meeting the cap in the first batch ends the cycle: 1 of 21 pages is ever requested.
        assert_eq!(fake.list_requests.load(Ordering::Relaxed), 1);
        // A batch closes on the first key of the next object.
        let batch = MAX_JANITOR_ELIGIBILITY_BATCH as u64 + 1;
        assert_eq!(stats.outcome, pipeline::CycleOutcome::DeleteCapReached);
        assert_eq!(stats.scanned_files, batch);
        assert_eq!(stats.orphan_candidates, 2);
        assert_eq!(stats.deleted, 2);
        assert_eq!(stats.initial_listing_entries, batch);
        // Buffered candidates stay at one eligibility batch regardless of key count.
        assert_eq!(
            stats.peak_buffered_candidates,
            MAX_JANITOR_ELIGIBILITY_BATCH as u64
        );
        assert!(stats.first_listing_entry.is_some());
        assert!(stats.time_to_first_candidate.is_some());
        // Listing pages carry LastModified, so age checks issue no HEAD.
        assert_eq!(stats.head_requests, 0);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 0);
        // Legacy key plus every fragment key per deleted object.
        let keys_per_object = studiofs_data::object::CLOUD_FRAGMENT_COUNT as u64 + 1;
        assert_eq!(stats.delete_requests, 2 * keys_per_object);
        assert_eq!(
            fake.delete_requests.load(Ordering::Relaxed),
            2 * keys_per_object
        );
        println!(
            "janitor streaming: pages={} listed={} peak_buffered={} first_candidate_ms={} head_calls={} delete_calls={} object_work_ms={}",
            fake.list_requests.load(Ordering::Relaxed),
            stats.initial_listing_entries,
            stats.peak_buffered_candidates,
            duration_ms(stats.time_to_first_candidate.unwrap()),
            stats.head_requests,
            stats.delete_requests,
            duration_ms(stats.cycle_elapsed)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn janitor_stalled_opendal_page_times_out_after_first_page_was_processed() {
        // Page 2 never arrives; the first page's objects must already be processed.
        let (op, fake, _endpoint, server) = fake_s3_operator_with_stall(2_000, Some(1)).await;
        let store = OpenDalJanitorStore(studiofs_data::janitor::JanitorObjectStore::new(op));
        let mut metadata = FakeJanitorMetadata::eligible();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_one_cycle(JanitorCycleContext {
                store: &store,
                prefix: "test/objects/",
                dry_run: true,
                skip_newer_than_secs: 60,
                budget: RunBudget::deletes(u64::MAX),
                metadata: &mut metadata,
            }),
        )
        .await
        .unwrap();
        server.abort();
        let _ = server.await;

        let error = format!("{:#}", result.unwrap_err());
        assert!(
            error.contains("failed to list janitor objects after 1000 entries"),
            "{error}"
        );
        assert!(error.contains("listing page timed out"), "{error}");
        assert_eq!(fake.list_requests.load(Ordering::Relaxed), 2);
        // 1,000 listed objects fill 15 whole batches; the 40-object tail is never acted on.
        let full_batches = 1000 / MAX_JANITOR_ELIGIBILITY_BATCH;
        assert_eq!(metadata.eligibility_reads.len(), full_batches);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn janitor_dry_run_never_sends_delete_requests() {
        let (op, fake, _endpoint, server) = fake_s3_operator(1).await;
        let store = OpenDalJanitorStore(studiofs_data::janitor::JanitorObjectStore::new(op));
        let mut metadata = FakeJanitorMetadata::eligible();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_one_cycle(JanitorCycleContext {
                store: &store,
                prefix: "test/objects/",
                dry_run: true,
                skip_newer_than_secs: 60,
                budget: RunBudget::deletes(1),
                metadata: &mut metadata,
            }),
        )
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
        let stats = result.unwrap();

        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.head_requests, 0);
        assert_eq!(
            stats.head_requests,
            fake.head_requests.load(Ordering::Relaxed)
        );
        assert_eq!(stats.delete_requests, 0);
        assert_eq!(fake.delete_requests.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn janitor_run_rejects_local_metadata_before_listing_objects() {
        let (_op, fake, endpoint, server) = fake_s3_operator(1).await;
        let temp = studiofs_test_support::workspace_target_tempdir("janitor-meta-");
        let meta_dir = temp.path().join("meta");
        let db_path = create_local_metadata_fixture(&meta_dir, &endpoint);
        let before = std::fs::read(&db_path).unwrap();
        let result = tokio::task::spawn_blocking(move || {
            run(JanitorRunRequest {
                name: "janitor-fixture".to_string(),
                meta_dir,
                options: JanitorOptions {
                    cache_dir: None,
                    dry_run: false,
                    loop_mode: false,
                    interval_minutes: 30,
                    skip_newer_minutes: 1,
                    max_deletes: 1,
                    max_scan_entries: None,
                    max_runtime_minutes: None,
                    pg_concurrency: 1,
                    s3_concurrency: 2,
                    s3_requests_per_second: 4,
                    full_scan: false,
                    reset_scan: false,
                },
            })
        })
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
        let error = result.unwrap_err();

        assert!(error.to_string().contains("unsafe_to_delete"));
        assert_eq!(fake.list_requests.load(Ordering::Relaxed), 0);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 0);
        assert_eq!(fake.delete_requests.load(Ordering::Relaxed), 0);
        assert_eq!(std::fs::read(&db_path).unwrap(), before);
    }

    struct FailClosedRun {
        result: anyhow::Result<()>,
        s3_requests: u64,
        metadata_unchanged: bool,
    }

    /// Runs one real `run()` cycle against a PostgreSQL-backed fixture and a fake S3.
    async fn run_postgres_fixture_once(postgres_url: String, hold_lock: bool) -> FailClosedRun {
        let (_op, fake, endpoint, server) = fake_s3_operator(1).await;
        let temp = studiofs_test_support::workspace_target_tempdir("janitor-pg-");
        let meta_dir = temp.path().join("meta");
        let db_path = create_postgres_metadata_fixture(&meta_dir, &endpoint, &postgres_url);
        let before = std::fs::read(&db_path).unwrap();
        let run_db_path = db_path.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut holder = hold_lock.then(|| {
                let mut session = JanitorMetadataSession::open(&run_db_path).unwrap();
                assert!(session.try_acquire_global_janitor_lock().unwrap());
                session
            });
            let result = run(JanitorRunRequest {
                name: "janitor-fixture".to_string(),
                meta_dir,
                options: JanitorOptions {
                    cache_dir: None,
                    dry_run: false,
                    loop_mode: false,
                    interval_minutes: 30,
                    skip_newer_minutes: 0,
                    max_deletes: 1,
                    max_scan_entries: None,
                    max_runtime_minutes: None,
                    pg_concurrency: 1,
                    s3_concurrency: 2,
                    s3_requests_per_second: 4,
                    full_scan: false,
                    reset_scan: false,
                },
            });
            if let Some(session) = holder.as_mut() {
                session.release_global_janitor_lock().unwrap();
            }
            result
        })
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
        FailClosedRun {
            result,
            s3_requests: fake.list_requests.load(Ordering::Relaxed)
                + fake.head_requests.load(Ordering::Relaxed)
                + fake.delete_requests.load(Ordering::Relaxed),
            metadata_unchanged: std::fs::read(&db_path).unwrap() == before,
        }
    }

    #[tokio::test]
    async fn janitor_run_unreachable_postgres_fails_closed_without_s3_requests() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let url = format!("postgresql://janitor:leak-canary@127.0.0.1:{port}/studiofs");
        let run = run_postgres_fixture_once(url, false).await;
        let error = format!("{:#}", run.result.unwrap_err());

        assert!(error.contains("backend_unavailable"), "{error}");
        assert!(!error.contains("leak-canary"), "{error}");
        assert_eq!(run.s3_requests, 0);
        assert!(run.metadata_unchanged);
    }

    #[tokio::test]
    async fn janitor_run_silent_postgres_times_out_without_s3_requests() {
        // Accepts TCP but never speaks the protocol, so only the bounded connect timeout ends it.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "postgresql://janitor@{}/studiofs",
            listener.local_addr().unwrap()
        );
        let started = Instant::now();
        let run = run_postgres_fixture_once(url, false).await;
        drop(listener);
        let error = format!("{:#}", run.result.unwrap_err());

        assert!(error.contains("backend_unavailable"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(60));
        assert_eq!(run.s3_requests, 0);
        assert!(run.metadata_unchanged);
    }

    #[tokio::test]
    #[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
    async fn janitor_run_lock_busy_skips_cycle_without_s3_requests() {
        let url = test_env("STUDIOFS_TEST_PG_DSN")
            .expect("STUDIOFS_TEST_PG_DSN must be set (environment or workspace .env)");
        let run = run_postgres_fixture_once(url, true).await;

        run.result.unwrap();
        assert_eq!(run.s3_requests, 0);
        assert!(run.metadata_unchanged);
    }

    #[test]
    fn janitor_parses_flags_and_defaults() {
        let cli = crate::cli::parse_for_test(&[
            "janitor",
            "vol",
            "--cache-dir",
            "D:\\cache\\studiofs",
            "--dry-run",
            "--loop",
            "--interval-minutes",
            "45",
            "--skip-newer-minutes",
            "20",
            "--max-deletes",
            "600",
        ])
        .unwrap();

        match cli.command {
            crate::cli::Commands::Janitor(JanitorArgs {
                name,
                cache_dir,
                dry_run,
                r#loop,
                interval_minutes,
                skip_newer_minutes,
                max_deletes,
                max_scan_entries,
                max_runtime_minutes,
                pg_concurrency,
                s3_concurrency,
                s3_requests_per_second,
                full_scan,
                reset_scan,
            }) => {
                assert_eq!(name, "vol");
                assert_eq!(cache_dir, Some(PathBuf::from("D:\\cache\\studiofs")));
                assert!(dry_run);
                assert!(r#loop);
                assert_eq!(interval_minutes, 45);
                assert_eq!(skip_newer_minutes, 20);
                assert_eq!(max_deletes, 600);
                assert_eq!(max_scan_entries, None);
                assert_eq!(max_runtime_minutes, None);
                assert_eq!(pg_concurrency, 1);
                assert_eq!(s3_concurrency, 2);
                assert_eq!(s3_requests_per_second, 4);
                assert!(!full_scan);
                assert!(!reset_scan);
            }
            other => panic!("expected Janitor, got {other:?}"),
        }
    }

    #[test]
    fn janitor_budget_and_scan_flags_parse_and_reject_conflicts() {
        let cli = crate::cli::parse_for_test(&[
            "janitor",
            "vol",
            "--max-scan-entries",
            "10000",
            "--max-runtime-minutes",
            "30",
        ])
        .unwrap();
        let crate::cli::Commands::Janitor(args) = cli.command else {
            panic!("expected Janitor");
        };
        assert_eq!(args.max_scan_entries, Some(10_000));
        assert_eq!(args.max_runtime_minutes, Some(30));

        let serial = crate::cli::parse_for_test(&[
            "janitor",
            "vol",
            "--s3-concurrency",
            "1",
            "--pg-concurrency",
            "1",
            "--s3-requests-per-second",
            "1",
        ])
        .unwrap();
        let crate::cli::Commands::Janitor(serial) = serial.command else {
            panic!("expected Janitor");
        };
        assert_eq!(serial.s3_concurrency, 1);
        assert_eq!(serial.pg_concurrency, 1);
        assert_eq!(serial.s3_requests_per_second, 1);

        for flag in [
            "--max-deletes",
            "--max-scan-entries",
            "--max-runtime-minutes",
            "--pg-concurrency",
            "--s3-concurrency",
            "--s3-requests-per-second",
        ] {
            assert!(
                crate::cli::parse_for_test(&["janitor", "vol", flag, "0"]).is_err(),
                "{flag} 0 must be rejected"
            );
        }
        for (flag, value) in [
            ("--pg-concurrency", "9"),
            ("--s3-concurrency", "33"),
            ("--s3-requests-per-second", "33"),
        ] {
            assert!(
                crate::cli::parse_for_test(&["janitor", "vol", flag, value]).is_err(),
                "{flag} above its supported maximum must be rejected"
            );
        }
        for flag in ["--max-scan-entries", "--max-runtime-minutes"] {
            assert!(
                crate::cli::parse_for_test(&["janitor", "vol", "--loop", flag, "5"]).is_ok(),
                "{flag} should be resumable in loop mode"
            );
        }
        assert!(crate::cli::parse_for_test(&["janitor", "vol", "--full-scan"]).is_ok());
        assert!(crate::cli::parse_for_test(&["janitor", "vol", "--reset-scan"]).is_ok());
        assert!(
            crate::cli::parse_for_test(&["janitor", "vol", "--full-scan", "--reset-scan",])
                .is_err()
        );
    }

    #[test]
    fn janitor_start_banner_omits_local_cache_when_not_requested() {
        let lines = build_start_banner_lines(JanitorStartBanner {
            name: "test",
            dry_run: false,
            endpoint: "https://s3.example",
            bucket: "studiofs",
            prefix: "test/objects/",
            local_cache_report: None,
            skip_newer_minutes: 1,
            max_deletes: 100_000,
            pg_concurrency: 1,
            s3_concurrency: 2,
            s3_requests_per_second: 4,
        });

        assert_eq!(
            lines,
            vec![
                "Janitor started for 'test'".to_string(),
                "  Mode:       delete".to_string(),
                "  Storage:    https://s3.example / studiofs".to_string(),
                "  Prefix:     test/objects/".to_string(),
                "  Scope:      remote objects only".to_string(),
                "  Grace:      1 minute".to_string(),
                "  Max delete: 100000".to_string(),
                "  In-flight:  PG <= 1 (serial), S3 HTTP <= 2".to_string(),
                "  S3 rate:    <= 4 HTTP requests/second".to_string(),
            ]
        );
        assert!(
            lines.iter().all(|line| !line.contains("Cache")),
            "default janitor banner should not mention local cache paths"
        );
    }

    #[test]
    fn janitor_inspects_local_runtime_via_generic_backend_api() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let meta_dir = temp.path().join("meta");
        let cache_base = temp.path().join("cache-root");
        let volume_uuid = "de305d54-75b4-431b-adb2-eb6b9e546014";
        std::fs::create_dir_all(&meta_dir).unwrap();

        let cache_root = cache_base.join(volume_uuid);
        let backend = studiofs_cache::open_local_payload_backend_with_budget(
            &cache_root,
            volume_uuid,
            4 * 1024 * 1024,
        )
        .unwrap()
        .backend;
        backend.put(11, b"dirty", false).unwrap();
        backend.put(22, b"uploaded", true).unwrap();
        backend.sync_data().unwrap();
        backend.mark_uploading(22).unwrap();
        backend.mark_uploaded(22).unwrap();
        drop(backend);

        let (inspected_path, detection, local_cache_state) =
            inspect_local_cache_runtime(&meta_dir, Some(cache_base.as_path()), volume_uuid)
                .unwrap();

        assert_eq!(inspected_path, cache_root);
        assert_eq!(
            detection
                .as_ref()
                .and_then(LocalPayloadRuntimeDetection::runtime_backend),
            Some(LocalPayloadBackendKind::Current)
        );
        assert_eq!(
            local_cache_state,
            Some(JanitorLocalCacheState { pending_uploads: 1 })
        );
    }

    #[test]
    fn janitor_local_cache_inspection_is_best_effort_when_backend_is_locked() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let meta_dir = temp.path().join("meta");
        let cache_base = temp.path().join("cache-root");
        let volume_uuid = "de305d54-75b4-431b-adb2-eb6b9e546014";
        std::fs::create_dir_all(&meta_dir).unwrap();

        let cache_root = cache_base.join(volume_uuid);
        let _backend_guard = studiofs_cache::open_local_payload_backend_with_budget(
            &cache_root,
            volume_uuid,
            4 * 1024 * 1024,
        )
        .unwrap();

        let (inspected_path, detection, local_cache_state) =
            inspect_local_cache_runtime(&meta_dir, Some(cache_base.as_path()), volume_uuid)
                .unwrap();

        assert_eq!(inspected_path, cache_root);
        assert_eq!(
            detection
                .as_ref()
                .and_then(LocalPayloadRuntimeDetection::runtime_backend),
            Some(LocalPayloadBackendKind::Current)
        );
        assert_eq!(
            local_cache_state, None,
            "janitor should warn and continue when the local backend cannot be opened"
        );
    }

    #[test]
    fn janitor_local_cache_inspection_does_not_provision_missing_runtime_paths() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let meta_dir = temp.path().join("meta");
        let cache_base = temp.path().join("cache-root");
        let volume_uuid = "de305d54-75b4-431b-adb2-eb6b9e546014";
        std::fs::create_dir_all(&meta_dir).unwrap();

        let cache_root = cache_base.join(volume_uuid);
        let layout = LocalPayloadLayout::from_cache_root(&cache_root);
        std::fs::create_dir_all(&layout.index_dir).unwrap();

        let (inspected_path, detection, local_cache_state) =
            inspect_local_cache_runtime(&meta_dir, Some(cache_base.as_path()), volume_uuid)
                .unwrap();

        assert_eq!(inspected_path, cache_root);
        assert_eq!(
            detection
                .as_ref()
                .and_then(LocalPayloadRuntimeDetection::runtime_backend),
            None
        );
        assert_eq!(local_cache_state, None);
        assert!(
            !layout.payload_objects_dir.exists(),
            "janitor inspection should not provision missing runtime directories"
        );
        assert!(
            layout.index_dir.exists(),
            "the seeded partial runtime directory should stay intact"
        );
        assert!(
            !layout.upload_stage_objects_dir.exists(),
            "janitor inspection should not provision the upload directory as a side effect"
        );
        assert!(
            !layout.tmp_dir.exists(),
            "janitor inspection should not create tmp directories as a side effect"
        );
    }

    /// Forces the provider-neutral prefix fallback over a real OpenDAL listing.
    struct NoStartAfterStore<'a>(&'a OpenDalJanitorStore);

    impl JanitorObjectStore for NoStartAfterStore<'_> {
        type Listing = JanitorLister;

        fn supports_start_after(&self) -> bool {
            false
        }

        async fn list(
            &self,
            prefix: &str,
            after_key: Option<&str>,
        ) -> anyhow::Result<JanitorLister> {
            anyhow::ensure!(
                after_key.is_none(),
                "prefix fallback must not resume after a key"
            );
            self.0.list(prefix, None).await
        }

        async fn last_modified(&self, key: &str) -> anyhow::Result<Option<SystemTime>> {
            self.0.last_modified(key).await
        }

        async fn stat_key(
            &self,
            key: &str,
        ) -> anyhow::Result<studiofs_data::janitor::JanitorKeyStat> {
            self.0.stat_key(key).await
        }

        async fn delete(&self, key: &str) -> Result<(), JanitorDeleteError> {
            self.0.delete(key).await
        }
    }

    async fn listed_files(
        store: &OpenDalJanitorStore,
        prefix: &str,
        after: Option<&str>,
    ) -> Vec<String> {
        let mut listing = store.list(prefix, after).await.unwrap();
        let mut files = Vec::new();
        while let Some(entry) = listing.next_entry().await.unwrap() {
            if entry.is_file {
                files.push(entry.key);
            } else {
                eprintln!(
                    "sfs973 live listing directory entry prefix={prefix} key={}",
                    entry.key
                );
            }
        }
        files
    }

    /// SFS-973 live check: S3 `start_after` resume, partial hex prefixes without a
    /// trailing `/`, and repeated one-entry runs of both listing strategies.
    /// Reads an ignored-test setting from the process environment, then the workspace `.env`.
    fn test_env(key: &str) -> Option<String> {
        let non_empty = |value: &String| !value.trim().is_empty();
        std::env::var(key).ok().filter(non_empty).or_else(|| {
            dotenvy::from_path_iter(studiofs_test_support::workspace_root().join(".env"))
                .ok()?
                .filter_map(Result::ok)
                .find_map(|(name, value)| (name == key).then_some(value))
                .filter(non_empty)
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live S3 backend with an existing bucket (STUDIOFS_S3_* in environment or .env)"]
    async fn sfs973_live_s3_resumable_listing_covers_every_object() {
        let env = |key: &str, default: &str| test_env(key).unwrap_or_else(|| default.to_string());
        let endpoint = env("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:9000");
        let bucket = env("STUDIOFS_S3_BUCKET", "studiofs-test");
        let access_key = env("STUDIOFS_S3_ACCESS_KEY", "studiofs");
        let secret_key = env("STUDIOFS_S3_SECRET_KEY", "studiofs123");
        let region = env("STUDIOFS_S3_REGION", "us-east-1");
        let writer = Operator::new(
            S3::default()
                .endpoint(&endpoint)
                .bucket(&bucket)
                .access_key_id(&access_key)
                .secret_access_key(&secret_key)
                .region(&region),
        )
        .unwrap()
        .finish();
        let store = OpenDalJanitorStore(
            studiofs_data::janitor::JanitorObjectStore::from_s3(
                &endpoint,
                &bucket,
                &access_key,
                &secret_key,
                &region,
                2,
                // Keep each gated HEAD below the cfg(test) 200 ms HEAD timeout.
                32,
            )
            .unwrap(),
        );
        assert!(
            store.supports_start_after(),
            "S3 must advertise start_after"
        );
        let ids = [0x1_0000_u64, 0x2_0000, 0x1_0001, 0x5_abcd];
        let run_id = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        for strategy in ["start_after", "prefix_trie"] {
            let root = format!("sfs973-{run_id}-{strategy}/objects/");
            let foreign = format!("{root}not-canonical");
            let mut keys = vec![foreign.clone()];
            for id in ids {
                keys.push(studiofs_data::object::format_object_key(&root, id));
                keys.push(studiofs_data::object::format_object_fragment_key(
                    &root, id, 0,
                ));
            }
            keys.sort();
            for key in &keys {
                writer.write(key, b"x".to_vec()).await.unwrap();
            }

            assert_eq!(listed_files(&store, &root, None).await, keys);
            // Resume strictly after a key, even when directory entries are synthesized.
            assert_eq!(listed_files(&store, &root, Some(&keys[1])).await, keys[2..]);
            // Shard and object-ID cursors are key prefixes, not `/`-terminated directories.
            let shard: Vec<_> = keys
                .iter()
                .filter(|key| key.starts_with(&format!("{root}0000/")))
                .cloned()
                .collect();
            assert_eq!(shard.len(), 4);
            assert_eq!(
                listed_files(&store, &format!("{root}0000"), None).await,
                shard
            );
            assert_eq!(
                listed_files(&store, &format!("{root}0000/000000000001"), None).await,
                shard[..2]
            );
            assert!(
                listed_files(&store, &format!("{root}00000"), None)
                    .await
                    .is_empty()
            );

            let mut metadata = FakeJanitorMetadata::eligible();
            let mut completed = false;
            for _ in 0..2_000 {
                let checkpoint = metadata
                    .sweep_checkpoint
                    .clone()
                    .unwrap_or_else(sweep_checkpoint);
                let budget = RunBudget {
                    max_deletes: 100,
                    max_scan_entries: Some(1),
                    max_runtime: None,
                };
                let fallback = NoStartAfterStore(&store);
                let stats = if strategy == "start_after" {
                    run_one_cycle_resumable(
                        JanitorCycleContext {
                            store: &store,
                            prefix: &root,
                            dry_run: false,
                            skip_newer_than_secs: 0,
                            budget,
                            metadata: &mut metadata,
                        },
                        checkpoint,
                        &sweep_scope(),
                        true,
                    )
                    .await
                } else {
                    run_one_cycle_resumable(
                        JanitorCycleContext {
                            store: &fallback,
                            prefix: &root,
                            dry_run: false,
                            skip_newer_than_secs: 0,
                            budget,
                            metadata: &mut metadata,
                        },
                        checkpoint,
                        &sweep_scope(),
                        true,
                    )
                    .await
                }
                .unwrap();
                assert!(stats.initial_listing_entries <= 1);
                if stats.scan_complete {
                    completed = true;
                    break;
                }
            }
            assert!(
                completed,
                "{strategy}: one-entry runs never completed the sweep"
            );
            let mut claims = metadata.claims.clone();
            claims.sort_unstable();
            let mut expected = ids.to_vec();
            expected.sort_unstable();
            assert_eq!(
                claims, expected,
                "{strategy}: every orphan must be claimed once"
            );
            assert_eq!(
                listed_files(&store, &root, None).await,
                vec![foreign.clone()]
            );
            writer.delete(&foreign).await.unwrap();
        }
    }
}

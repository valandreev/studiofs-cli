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
use studiofs_meta::{JanitorMetadataSession, JanitorProtection};
mod pipeline;
use pipeline::{JanitorCycleContext, JanitorListing, JanitorObjectStore, RunBudget, run_one_cycle};
use studiofs_data::janitor::{JanitorDeleteError, JanitorEntry, JanitorLister};

struct OpenDalJanitorStore(studiofs_data::janitor::JanitorObjectStore);

impl JanitorListing for JanitorLister {
    async fn next_entry(&mut self) -> anyhow::Result<Option<JanitorEntry>> {
        Ok(self.next().await?)
    }
}

impl JanitorObjectStore for OpenDalJanitorStore {
    type Listing = JanitorLister;

    async fn list(&self, prefix: &str) -> anyhow::Result<JanitorLister> {
        Ok(self.0.list(prefix).await?)
    }

    async fn last_modified(&self, key: &str) -> anyhow::Result<Option<SystemTime>> {
        Ok(self.0.last_modified(key).await?)
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
}

struct JanitorRunRequest {
    name: String,
    meta_dir: PathBuf,
    options: JanitorOptions,
}

trait JanitorMetadataSource {
    fn try_acquire_global_janitor_lock(&mut self) -> anyhow::Result<bool>;
    fn validate_protection(&mut self) -> anyhow::Result<()>;
    fn check_eligibility(&mut self, object_ids: &[u64]) -> anyhow::Result<JanitorProtection>;
    fn claim_deletion(&mut self, object_id: u64) -> anyhow::Result<Option<i64>>;
    fn finish_deletion(
        &mut self,
        object_id: u64,
        fencing_token: i64,
        outcome: UploadedObjectDeletionOutcome,
    ) -> anyhow::Result<()>;
    fn prune_deletion_fences(&mut self) -> anyhow::Result<u64>;
    fn release_global_janitor_lock(&mut self) -> anyhow::Result<()>;
}

impl JanitorMetadataSource for JanitorMetadataSession {
    fn try_acquire_global_janitor_lock(&mut self) -> anyhow::Result<bool> {
        JanitorMetadataSession::try_acquire_global_janitor_lock(self).map_err(anyhow::Error::new)
    }

    fn validate_protection(&mut self) -> anyhow::Result<()> {
        JanitorMetadataSession::validate_protection(self).map_err(anyhow::Error::new)
    }

    fn check_eligibility(&mut self, object_ids: &[u64]) -> anyhow::Result<JanitorProtection> {
        JanitorMetadataSession::check_eligibility(self, object_ids).map_err(anyhow::Error::new)
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

    fn prune_deletion_fences(&mut self) -> anyhow::Result<u64> {
        JanitorMetadataSession::prune_deletion_fences(self).map_err(anyhow::Error::new)
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
    // ponytail: no continuation is persisted yet (SFS-973), so a looped partial cycle
    // would rescan the same head forever; allow with --loop once checkpoints exist.
    #[arg(long, conflicts_with = "loop", value_parser = clap::value_parser!(u64).range(1..))]
    max_scan_entries: Option<u64>,

    /// Stop admitting new work after this many minutes per cycle and report it as partial
    #[arg(long, conflicts_with = "loop", value_parser = clap::value_parser!(u64).range(1..))]
    max_runtime_minutes: Option<u64>,
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
    )?);

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
    }) {
        println!("{line}");
    }

    loop {
        let cycle_started = Instant::now();
        let lock_started = Instant::now();
        eprintln!("janitor phase=global_lock started");
        let lock_acquired = JanitorMetadataSource::try_acquire_global_janitor_lock(&mut metadata)?;
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
            std::thread::sleep(Duration::from_secs(
                interval_minutes.max(1).saturating_mul(60),
            ));
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

            let stats = rt.block_on(run_one_cycle(JanitorCycleContext {
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
            }))?;
            let fences_pruned = if dry_run {
                0
            } else {
                JanitorMetadataSource::prune_deletion_fences(metadata)?
            };

            Ok((stats, fences_pruned))
        });
        let cycle_elapsed = cycle_started.elapsed();
        let (stats, fences_pruned) = cycle_result?;

        println!("Janitor cycle done:");
        println!("  scanned:              {}", stats.scanned_files);
        println!("  referenced:           {}", stats.referenced_files);
        println!("  orphan candidates:    {}", stats.orphan_candidates);
        println!("  skipped young:        {}", stats.skipped_young);
        println!("  skipped unknown age:  {}", stats.skipped_unknown_age);
        println!("  outcome:              {}", stats.outcome.label());
        println!(
            "  {}: {}",
            if dry_run { "would delete" } else { "deleted" },
            stats.deleted
        );
        println!("  delete errors:        {}", stats.delete_errors);
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
            "  janitor counts: listed_entries={} peak_buffered_candidates={} scanned={} first_entry_ms={} first_candidate_ms={} opendal_head_calls={} opendal_delete_calls={} list_pages=unavailable claims={} claim_protected={} claim_finish={} skipped_non_canonical={}",
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
        );
        if !loop_mode {
            break;
        }
        std::thread::sleep(Duration::from_secs(
            interval_minutes.max(1).saturating_mul(60),
        ));
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

        async fn list(&self, _prefix: &str) -> anyhow::Result<FakeListing> {
            self.list_requests.fetch_add(1, Ordering::Relaxed);
            if self.fail_list {
                anyhow::bail!("list failed")
            }
            Ok(FakeListing {
                entries: self.entries.clone().into_iter(),
                fail_after: self.fail_list_after,
                hang: self.hang_list,
                yielded: 0,
            })
        }

        async fn last_modified(&self, _key: &str) -> anyhow::Result<Option<SystemTime>> {
            self.head_requests.fetch_add(1, Ordering::Relaxed);
            if self.fail_head {
                anyhow::bail!("head failed")
            }
            if self.hang_head {
                std::future::pending::<()>().await;
            }
            if self.head_missing {
                return Ok(None);
            }
            let age = if self.head_young { 0 } else { 3600 };
            Ok(Some(SystemTime::now() - Duration::from_secs(age)))
        }

        async fn delete(&self, key: &str) -> Result<(), JanitorDeleteError> {
            self.deleted.lock().unwrap().push(key.to_string());
            if self.hang_delete {
                std::future::pending::<()>().await;
            }
            if self.fail_delete {
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
        nested_runtime: Option<std::sync::Arc<tokio::runtime::Runtime>>,
        releases: u64,
        claim_protected: Vec<u64>,
        claim_error: Option<&'static str>,
        finish_error: Option<&'static str>,
        claims: Vec<u64>,
        finishes: Vec<(u64, i64, UploadedObjectDeletionOutcome)>,
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

        fn check_eligibility(&mut self, object_ids: &[u64]) -> anyhow::Result<JanitorProtection> {
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
                    self.outcomes
                        .get(id)
                        .or(self.default_eligibility.as_ref())
                        .map(|eligibility| (*id, *eligibility))
                })
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

        fn prune_deletion_fences(&mut self) -> anyhow::Result<u64> {
            Ok(0)
        }

        fn release_global_janitor_lock(&mut self) -> anyhow::Result<()> {
            self.releases += 1;
            Ok(())
        }
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
                    .map(|key| JanitorEntry { key, is_file: true })
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
            .map(|key| JanitorEntry { key, is_file: true })
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
                .map(|key| studiofs_data::janitor::JanitorEntry { key, is_file: true })
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
            .map(|key| JanitorEntry { key, is_file: true })
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
            .map(|key| JanitorEntry { key, is_file: true })
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
    async fn legacy_keys_are_cleaned_and_non_canonical_keys_are_never_touched() {
        let store = FakeJanitorStore {
            entries: [
                studiofs_data::object::format_object_key("test/objects/", 5),
                "test/objects/unrelated/0000000000000006".to_string(),
                "test/objects/0007/0000000000000007/extra".to_string(),
            ]
            .into_iter()
            .map(|key| JanitorEntry { key, is_file: true })
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
                .map(|key| JanitorEntry { key, is_file: true })
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
            .map(|key| JanitorEntry { key, is_file: true })
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
            .map(|key| JanitorEntry { key, is_file: true })
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
            .map(|key| JanitorEntry { key, is_file: true })
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
    async fn fake_store_failures_skip_deletion_and_not_found_fragments_are_tolerated() {
        let entries = studiofs_data::object::format_object_fragment_keys("test/objects/", 4)
            .into_iter()
            .map(|key| studiofs_data::janitor::JanitorEntry { key, is_file: true })
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
                .map(|key| JanitorEntry { key, is_file: true })
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
        // Only the first eligibility batch was listed; the tail is never scanned.
        assert_eq!(metadata.eligibility_reads.len(), 1);
        assert_eq!(
            stats.initial_listing_entries,
            (MAX_JANITOR_ELIGIBILITY_BATCH * studiofs_data::object::CLOUD_FRAGMENT_COUNT) as u64
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
        assert_eq!(stats.deleted, 3);
        assert_eq!(metadata.claims, vec![1, 2, 3]);
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
        list_requests_before_first_head: AtomicU64,
        object_count: u64,
        /// Never answer this 0-based list page, simulating a stalled provider.
        stall_page: Option<u64>,
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
                let request = String::from_utf8_lossy(&request);
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
                    if fake.head_requests.fetch_add(1, Ordering::Relaxed) == 0 {
                        fake.list_requests_before_first_head.store(
                            fake.list_requests.load(Ordering::Relaxed),
                            Ordering::Relaxed,
                        );
                    }
                    (
                        "200 OK",
                        String::new(),
                        "Last-Modified: Wed, 01 Jan 2020 00:00:00 GMT\r\n",
                    )
                } else if first_line.starts_with("DELETE ") {
                    fake.delete_requests.fetch_add(1, Ordering::Relaxed);
                    ("204 No Content", String::new(), "")
                } else {
                    ("501 Not Implemented", String::new(), "")
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let fake = std::sync::Arc::new(FakeS3 {
            list_requests: AtomicU64::new(0),
            head_requests: AtomicU64::new(0),
            delete_requests: AtomicU64::new(0),
            list_requests_before_first_head: AtomicU64::new(0),
            object_count,
            stall_page,
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
        let batch = MAX_JANITOR_ELIGIBILITY_BATCH as u64;
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
        assert_eq!(stats.head_requests, 2);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 2);
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
        assert_eq!(
            fake.list_requests_before_first_head.load(Ordering::Relaxed),
            1
        );
        // 1,000 listed objects fill 15 whole batches; the 40-object tail is never acted on.
        let full_batches = 1000 / MAX_JANITOR_ELIGIBILITY_BATCH;
        assert_eq!(metadata.eligibility_reads.len(), full_batches);
        assert_eq!(
            fake.head_requests.load(Ordering::Relaxed),
            (full_batches * MAX_JANITOR_ELIGIBILITY_BATCH) as u64
        );
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
        assert_eq!(stats.head_requests, 1);
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
        let Ok(url) = std::env::var("STUDIOFS_TEST_PG_DSN") else {
            panic!("STUDIOFS_TEST_PG_DSN must be set for this ignored test");
        };
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
            }
            other => panic!("expected Janitor, got {other:?}"),
        }
    }

    #[test]
    fn janitor_budget_flags_reject_zero_and_loop_mode() {
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

        for flag in [
            "--max-deletes",
            "--max-scan-entries",
            "--max-runtime-minutes",
        ] {
            assert!(
                crate::cli::parse_for_test(&["janitor", "vol", flag, "0"]).is_err(),
                "{flag} 0 must be rejected"
            );
        }
        // Without a persisted continuation a looped partial cycle never reaches the tail.
        for flag in ["--max-scan-entries", "--max-runtime-minutes"] {
            assert!(
                crate::cli::parse_for_test(&["janitor", "vol", "--loop", flag, "5"]).is_err(),
                "{flag} must conflict with --loop"
            );
        }
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
}

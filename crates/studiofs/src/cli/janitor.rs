//! `studiofs janitor` — remove orphaned data objects not referenced by metadata.

use crate::cli::cache_layout::describe_cache_layout_state;
use clap::Args;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opendal::services::S3;
use opendal::{EntryMode, ErrorKind, Operator};
use studiofs_cache::runtime::{
    LocalPayloadBackendKind, LocalPayloadRuntimeDetection, detect_cache_backends,
};
use studiofs_cache::{OpenLocalPayloadBackendError, open_local_payload_backend};
use studiofs_data::object::{format_object_fragment_keys, parse_object_id_from_key};
use studiofs_meta::open_meta_engine;

#[derive(Debug, Default)]
struct JanitorStats {
    scanned_files: u64,
    referenced_files: u64,
    orphan_candidates: u64,
    skipped_young: u64,
    skipped_unknown_age: u64,
    skipped_cap: u64,
    deleted: u64,
    delete_errors: u64,
    initial_listing_elapsed: Duration,
    initial_listing_entries: u64,
    peak_listing_entries: u64,
    time_to_first_candidate: Option<Duration>,
    cycle_elapsed: Duration,
    head_requests: u64,
    head_elapsed: Duration,
    delete_requests: u64,
    delete_elapsed: Duration,
}

struct JanitorCycleContext<'a> {
    op: &'a Operator,
    prefix: &'a str,
    referenced: &'a HashSet<u64>,
    global_snapshot_pins: &'a HashSet<u64>,
    pending: &'a HashMap<u64, i64>,
    dry_run: bool,
    skip_newer_than_secs: u64,
    max_deletes: u64,
}

struct JanitorOptions {
    cache_dir: Option<PathBuf>,
    dry_run: bool,
    loop_mode: bool,
    interval_minutes: u64,
    skip_newer_minutes: u64,
    max_deletes: u64,
}

struct JanitorRunRequest {
    name: String,
    meta_dir: PathBuf,
    options: JanitorOptions,
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

    /// Maximum objects to delete per cycle
    #[arg(long, default_value_t = 500)]
    max_deletes: u64,
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

async fn run_one_cycle(ctx: JanitorCycleContext<'_>) -> anyhow::Result<JanitorStats> {
    let mut stats = JanitorStats::default();
    let cycle_started = Instant::now();
    let mut seen_objects = HashSet::new();
    let now = SystemTime::now();
    let now_unix = i64::try_from(
        now.duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs(),
    )
    .unwrap_or(i64::MAX);

    eprintln!("janitor phase=initial_listing started");
    let listing_started = Instant::now();
    let entries = ctx.op.list_with(ctx.prefix).recursive(true).await?;
    stats.initial_listing_elapsed = listing_started.elapsed();
    stats.initial_listing_entries = u64::try_from(entries.len()).unwrap_or(u64::MAX);
    stats.peak_listing_entries = stats.initial_listing_entries;
    eprintln!(
        "janitor phase=initial_listing completed duration_ms={} entries={}",
        duration_ms(stats.initial_listing_elapsed),
        stats.initial_listing_entries
    );
    for entry in entries {
        if entry.metadata().mode() != EntryMode::FILE {
            continue;
        }

        stats.scanned_files = stats.scanned_files.saturating_add(1);
        let path = entry.path().to_string();

        let object_id = match parse_object_id_from_key(&path) {
            Ok(id) => id,
            Err(_) => continue,
        };
        if !seen_objects.insert(object_id) {
            continue;
        }

        if ctx.referenced.contains(&object_id) {
            stats.referenced_files = stats.referenced_files.saturating_add(1);
            continue;
        }
        if ctx.global_snapshot_pins.contains(&object_id) {
            stats.referenced_files = stats.referenced_files.saturating_add(1);
            continue;
        }
        stats.orphan_candidates = stats.orphan_candidates.saturating_add(1);
        if stats.time_to_first_candidate.is_none() {
            stats.time_to_first_candidate = Some(cycle_started.elapsed());
        }

        if stats.deleted >= ctx.max_deletes {
            stats.skipped_cap = stats.skipped_cap.saturating_add(1);
            continue;
        }

        if let Some(pending_ts) = ctx.pending.get(&object_id) {
            let age_secs = u64::try_from(now_unix.saturating_sub(*pending_ts));
            if let Ok(age_secs) = age_secs {
                if age_secs < ctx.skip_newer_than_secs {
                    stats.skipped_young = stats.skipped_young.saturating_add(1);
                    continue;
                }
            } else {
                stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
                continue;
            }
        } else {
            if stats.head_requests == 0 {
                eprintln!("janitor phase=head started");
            }
            stats.head_requests = stats.head_requests.saturating_add(1);
            let head_started = Instant::now();
            let metadata = ctx.op.stat(&path).await;
            stats.head_elapsed += head_started.elapsed();
            let meta = match metadata {
                Ok(m) => m,
                Err(_) => {
                    stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
                    continue;
                }
            };
            let Some(last_modified) = meta.last_modified() else {
                stats.skipped_unknown_age = stats.skipped_unknown_age.saturating_add(1);
                continue;
            };
            let last_modified_st: SystemTime = last_modified.into();
            let age_secs = now
                .duration_since(last_modified_st)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_secs();
            if age_secs < ctx.skip_newer_than_secs {
                stats.skipped_young = stats.skipped_young.saturating_add(1);
                continue;
            }
        }

        if ctx.dry_run {
            stats.deleted = stats.deleted.saturating_add(1);
            continue;
        }

        let mut delete_failed = false;
        for key in format_object_fragment_keys(ctx.prefix, object_id) {
            if stats.delete_requests == 0 {
                eprintln!("janitor phase=delete started");
            }
            stats.delete_requests = stats.delete_requests.saturating_add(1);
            let delete_started = Instant::now();
            let result = ctx.op.delete(&key).await;
            stats.delete_elapsed += delete_started.elapsed();
            if let Err(err) = result {
                if err.kind() == ErrorKind::NotFound {
                    continue;
                }
                delete_failed = true;
                eprintln!(
                    "janitor: failed to delete fragment key={} object_id={}: {}",
                    key, object_id, err
                );
            }
        }
        if delete_failed {
            stats.delete_errors = stats.delete_errors.saturating_add(1);
            continue;
        }
        stats.deleted = stats.deleted.saturating_add(1);
    }

    stats.cycle_elapsed = cycle_started.elapsed();
    eprintln!(
        "janitor phase=head completed calls={} duration_ms={}",
        stats.head_requests,
        duration_ms(stats.head_elapsed)
    );
    eprintln!(
        "janitor phase=delete completed calls={} duration_ms={}",
        stats.delete_requests,
        duration_ms(stats.delete_elapsed)
    );
    Ok(stats)
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
    let engine = open_meta_engine(&db_path)?;
    let format_info = engine.get_format_info()?;
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

    let mut builder = S3::default()
        .endpoint(&format_info.endpoint)
        .bucket(&format_info.bucket)
        .access_key_id(&format_info.access_key)
        .secret_access_key(&format_info.secret_key)
        .disable_config_load()
        .disable_ec2_metadata();
    if !format_info.region.is_empty() {
        builder = builder.region(&format_info.region);
    } else {
        builder = builder.region("us-east-1");
    }
    let op = Operator::new(builder)?.finish();

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
        let lock_acquired = engine.try_acquire_global_janitor_lock()?;
        let lock_wait_elapsed = lock_started.elapsed();
        eprintln!(
            "janitor phase=global_lock completed acquired={} duration_ms={}",
            lock_acquired,
            duration_ms(lock_wait_elapsed)
        );
        if !lock_acquired {
            println!(
                "Janitor cycle skipped: global janitor lock not acquired (wait_ms={})",
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
        let cycle_result = (|| -> anyhow::Result<(JanitorStats, HashSet<u64>, HashSet<u64>)> {
            let global_snapshot_pins = engine.get_global_snapshot_pins()?;
            if !dry_run {
                engine.reconcile_pending_regular_file_recoveries()?;
            }
            let referenced = engine.get_referenced_object_ids()?;
            let pending: HashMap<u64, i64> = engine.get_pending_deletions()?.into_iter().collect();
            protection_elapsed = protection_started.elapsed();
            eprintln!(
                "janitor phase=protection completed duration_ms={} references={} snapshot_pins={} pending={}",
                duration_ms(protection_elapsed),
                referenced.len(),
                global_snapshot_pins.len(),
                pending.len()
            );

            let stats = rt.block_on(run_one_cycle(JanitorCycleContext {
                op: &op,
                prefix: &prefix,
                referenced: &referenced,
                global_snapshot_pins: &global_snapshot_pins,
                pending: &pending,
                dry_run,
                skip_newer_than_secs: skip_secs,
                max_deletes: max_deletes.max(1),
            }))?;

            Ok((stats, referenced, global_snapshot_pins))
        })();
        if let Err(e) = engine.release_global_janitor_lock() {
            eprintln!("Warning: failed to release global janitor lock: {}", e);
        }
        let cycle_elapsed = cycle_started.elapsed();
        let (stats, referenced, global_snapshot_pins) = cycle_result?;

        println!("Janitor cycle done:");
        println!("  scanned:              {}", stats.scanned_files);
        println!("  metadata refs:        {}", referenced.len());
        println!("  referenced:           {}", stats.referenced_files);
        println!("  orphan candidates:    {}", stats.orphan_candidates);
        println!("  skipped young:        {}", stats.skipped_young);
        println!("  skipped unknown age:  {}", stats.skipped_unknown_age);
        println!("  skipped by cap:       {}", stats.skipped_cap);
        println!(
            "  {}: {}",
            if dry_run { "would delete" } else { "deleted" },
            stats.deleted
        );
        println!("  delete errors:        {}", stats.delete_errors);
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
            "  janitor counts: listed_entries={} peak_listing_entries={} scanned={} first_candidate_ms={} opendal_head_calls={} opendal_delete_calls={} list_pages=unavailable claims=not_applicable claim_finish=not_applicable",
            stats.initial_listing_entries,
            stats.peak_listing_entries,
            stats.scanned_files,
            stats
                .time_to_first_candidate
                .map(duration_ms)
                .map(|ms| ms.to_string())
                .unwrap_or_else(|| "unavailable".to_string()),
            stats.head_requests,
            stats.delete_requests,
        );
        println!(
            "  artifact-protected objects: {}",
            global_snapshot_pins.len()
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use studiofs_cache::runtime::LocalPayloadLayout;
    use studiofs_meta::{MetaFormat, RedbMetaEngine};

    struct FakeS3 {
        list_requests: AtomicU64,
        head_requests: AtomicU64,
        delete_requests: AtomicU64,
        list_requests_before_first_head: AtomicU64,
        object_count: u64,
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
                    let page = if first_line.contains("continuation-token=page3") {
                        2
                    } else if first_line.contains("continuation-token=page2") {
                        1
                    } else {
                        0
                    };
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let fake = std::sync::Arc::new(FakeS3 {
            list_requests: AtomicU64::new(0),
            head_requests: AtomicU64::new(0),
            delete_requests: AtomicU64::new(0),
            list_requests_before_first_head: AtomicU64::new(0),
            object_count,
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
        use studiofs_meta::schema::{
            CURRENT_FORMAT_VERSION, DEFAULT_BLOCK_SIZE, ENCRYPTION_KEY_LENGTH, EncryptionConfig,
            FormatInfo, MetadataConfig,
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
                metadata: MetadataConfig::default(),
                encryption: EncryptionConfig::new([0; ENCRYPTION_KEY_LENGTH]),
                block_size: DEFAULT_BLOCK_SIZE,
            })
            .unwrap();
        engine.init_root(0, 0).unwrap();
        drop(engine);
        db_path
    }

    #[tokio::test]
    async fn janitor_eager_listing_baseline_reads_all_pages_before_low_delete_cap() {
        let (op, fake, _endpoint, server) = fake_s3_operator(2505).await;
        let stats = tokio::time::timeout(
            Duration::from_secs(30),
            run_one_cycle(JanitorCycleContext {
                op: &op,
                prefix: "test/objects/",
                referenced: &HashSet::new(),
                global_snapshot_pins: &HashSet::new(),
                pending: &HashMap::new(),
                dry_run: false,
                skip_newer_than_secs: 60,
                max_deletes: 2,
            }),
        )
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
        let stats = stats.unwrap();

        assert_eq!(fake.list_requests.load(Ordering::Relaxed), 3);
        assert_eq!(
            fake.list_requests_before_first_head.load(Ordering::Relaxed),
            3
        );
        assert_eq!(stats.scanned_files, 2505);
        assert_eq!(stats.orphan_candidates, 2505);
        assert_eq!(stats.deleted, 2);
        assert_eq!(stats.skipped_cap, 2503);
        assert_eq!(stats.initial_listing_entries, 2505);
        assert_eq!(stats.peak_listing_entries, 2505);
        assert!(stats.time_to_first_candidate.is_some());
        assert_eq!(stats.head_requests, 2);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 2);
        assert_eq!(
            stats.delete_requests,
            2 * studiofs_data::object::CLOUD_FRAGMENT_COUNT as u64
        );
        assert_eq!(
            fake.delete_requests.load(Ordering::Relaxed),
            2 * studiofs_data::object::CLOUD_FRAGMENT_COUNT as u64
        );
        println!(
            "janitor baseline: pages={} listed={} peak={} first_candidate_ms={} head_calls={} delete_calls={} object_work_ms={}",
            fake.list_requests.load(Ordering::Relaxed),
            stats.initial_listing_entries,
            stats.peak_listing_entries,
            duration_ms(stats.time_to_first_candidate.unwrap()),
            stats.head_requests,
            stats.delete_requests,
            duration_ms(stats.cycle_elapsed)
        );
    }

    #[tokio::test]
    async fn janitor_dry_run_never_sends_delete_requests() {
        let (op, fake, _endpoint, server) = fake_s3_operator(1).await;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            run_one_cycle(JanitorCycleContext {
                op: &op,
                prefix: "test/objects/",
                referenced: &HashSet::new(),
                global_snapshot_pins: &HashSet::new(),
                pending: &HashMap::new(),
                dry_run: true,
                skip_newer_than_secs: 60,
                max_deletes: 1,
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
    async fn janitor_run_uses_local_metadata_fixture_and_releases_global_lock() {
        let (_op, fake, endpoint, server) = fake_s3_operator(1).await;
        let temp = studiofs_test_support::workspace_target_tempdir("janitor-meta-");
        let meta_dir = temp.path().join("meta");
        let db_path = create_local_metadata_fixture(&meta_dir, &endpoint);
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
                },
            })
        })
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
        result.unwrap();

        assert_eq!(fake.list_requests.load(Ordering::Relaxed), 1);
        assert_eq!(fake.head_requests.load(Ordering::Relaxed), 1);
        assert_eq!(
            fake.delete_requests.load(Ordering::Relaxed),
            studiofs_data::object::CLOUD_FRAGMENT_COUNT as u64
        );
        let engine = open_meta_engine(&db_path).unwrap();
        assert!(engine.try_acquire_global_janitor_lock().unwrap());
        engine.release_global_janitor_lock().unwrap();
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
            }) => {
                assert_eq!(name, "vol");
                assert_eq!(cache_dir, Some(PathBuf::from("D:\\cache\\studiofs")));
                assert!(dry_run);
                assert!(r#loop);
                assert_eq!(interval_minutes, 45);
                assert_eq!(skip_newer_minutes, 20);
                assert_eq!(max_deletes, 600);
            }
            other => panic!("expected Janitor, got {other:?}"),
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

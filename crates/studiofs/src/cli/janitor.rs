//! `studiofs janitor` — remove orphaned data objects not referenced by metadata.

use crate::cli::cache_layout::describe_cache_layout_state;
use clap::Args;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opendal::services::S3;
use opendal::{EntryMode, ErrorKind, Operator};
use studiofs_cache::runtime::{
    LocalBlockBackendKind, LocalBlockRuntimeDetection, detect_cache_backends,
};
use studiofs_cache::{OpenLocalBlockBackendError, open_local_block_backend};
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

    /// Override local cache base directory (defaults to `<meta-dir>/cache`)
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
    source: OpenLocalBlockBackendError,
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

fn inspect_local_cache_runtime(
    meta_dir: &std::path::Path,
    cache_dir: Option<&std::path::Path>,
    volume_uuid: &str,
) -> anyhow::Result<(
    PathBuf,
    Option<LocalBlockRuntimeDetection>,
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
    if detection.runtime_backend() != Some(LocalBlockBackendKind::Current) {
        return Ok((cache_path, Some(detection), None));
    }
    if detection.needs_runtime_provisioning() {
        eprintln!(
            "warning: local cache runtime at '{}' is incomplete; skipping pending-upload inspection",
            cache_path.display()
        );
        return Ok((cache_path, Some(detection), None));
    }

    let store = match open_local_block_backend(&cache_path, volume_uuid) {
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
    let mut seen_objects = HashSet::new();
    let now = SystemTime::now();
    let now_unix = i64::try_from(
        now.duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs(),
    )
    .unwrap_or(i64::MAX);

    let entries = ctx.op.list_with(ctx.prefix).recursive(true).await?;
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
            let meta = match ctx.op.stat(&path).await {
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
            if let Err(err) = ctx.op.delete(&key).await {
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

    let engine = open_meta_engine(&db_path)?;
    let format_info = engine.get_format_info()?;

    let prefix = normalize_prefix(&format_info);
    let (cache_path, cache_backend_detection, local_cache_state) =
        inspect_local_cache_runtime(meta_dir.as_path(), cache_dir.as_deref(), &format_info.uuid)?;

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

    println!("Janitor started for '{}'", name);
    println!(
        "  Mode:      {}",
        if dry_run { "dry-run" } else { "delete" }
    );
    println!("  Prefix:    {}", prefix);
    println!("  Cache root: {}", cache_path.display());
    println!(
        "  Local cache state: {}",
        describe_cache_layout_state(cache_backend_detection.as_ref())
    );
    if let Some(local_cache_state) = local_cache_state {
        println!(
            "  Local pending uploads: {}",
            local_cache_state.pending_uploads
        );
    }
    println!("  Grace:     {} minutes", skip_newer_minutes);
    println!("  Max delete: {}", max_deletes);

    loop {
        let lock_acquired = engine.try_acquire_global_janitor_lock()?;
        if !lock_acquired {
            println!("Janitor cycle skipped: PG janitor lock not acquired");
            if !loop_mode {
                break;
            }
            std::thread::sleep(Duration::from_secs(
                interval_minutes.max(1).saturating_mul(60),
            ));
            continue;
        }

        let cycle_result = (|| -> anyhow::Result<(JanitorStats, HashSet<u64>, HashSet<u64>)> {
            let global_snapshot_pins = engine.get_global_snapshot_pins()?;
            let referenced = engine.get_referenced_object_ids()?;
            let pending: HashMap<u64, i64> = engine.get_pending_deletions()?.into_iter().collect();

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
            eprintln!("Warning: failed to release PG janitor lock: {}", e);
        }
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
        println!("  snapshot pins:        {}", global_snapshot_pins.len());

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
    use studiofs_cache::runtime::LocalBlockLayout;

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
    fn janitor_inspects_local_runtime_via_generic_backend_api() {
        let temp = tempfile::tempdir().unwrap();
        let meta_dir = temp.path().join("meta");
        let cache_base = temp.path().join("cache-root");
        let volume_uuid = "de305d54-75b4-431b-adb2-eb6b9e546014";
        std::fs::create_dir_all(&meta_dir).unwrap();

        let cache_root = cache_base.join(volume_uuid);
        let backend = studiofs_cache::open_local_block_backend_with_budget(
            &cache_root,
            volume_uuid,
            4 * 1024 * 1024,
        )
        .unwrap()
        .backend;
        backend.put(11, b"dirty", false).unwrap();
        backend.put(22, b"uploaded", true).unwrap();
        backend.sync_data().unwrap();
        backend.mark_uploading(22);
        backend.mark_uploaded(22);
        drop(backend);

        let (inspected_path, detection, local_cache_state) =
            inspect_local_cache_runtime(&meta_dir, Some(cache_base.as_path()), volume_uuid)
                .unwrap();

        assert_eq!(inspected_path, cache_root);
        assert_eq!(
            detection
                .as_ref()
                .and_then(LocalBlockRuntimeDetection::runtime_backend),
            Some(LocalBlockBackendKind::Current)
        );
        assert_eq!(
            local_cache_state,
            Some(JanitorLocalCacheState { pending_uploads: 1 })
        );
    }

    #[test]
    fn janitor_local_cache_inspection_is_best_effort_when_backend_is_locked() {
        let temp = tempfile::tempdir().unwrap();
        let meta_dir = temp.path().join("meta");
        let cache_base = temp.path().join("cache-root");
        let volume_uuid = "de305d54-75b4-431b-adb2-eb6b9e546014";
        std::fs::create_dir_all(&meta_dir).unwrap();

        let cache_root = cache_base.join(volume_uuid);
        let _backend_guard = studiofs_cache::open_local_block_backend_with_budget(
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
                .and_then(LocalBlockRuntimeDetection::runtime_backend),
            Some(LocalBlockBackendKind::Current)
        );
        assert_eq!(
            local_cache_state, None,
            "janitor should warn and continue when the local backend cannot be opened"
        );
    }

    #[test]
    fn janitor_local_cache_inspection_does_not_provision_missing_runtime_paths() {
        let temp = tempfile::tempdir().unwrap();
        let meta_dir = temp.path().join("meta");
        let cache_base = temp.path().join("cache-root");
        let volume_uuid = "de305d54-75b4-431b-adb2-eb6b9e546014";
        std::fs::create_dir_all(&meta_dir).unwrap();

        let cache_root = cache_base.join(volume_uuid);
        let layout = LocalBlockLayout::from_cache_root(&cache_root);
        std::fs::create_dir_all(layout.runtime_root.as_path()).unwrap();
        std::fs::write(&layout.layout_marker, b"v3\n").unwrap();

        let (inspected_path, detection, local_cache_state) =
            inspect_local_cache_runtime(&meta_dir, Some(cache_base.as_path()), volume_uuid)
                .unwrap();

        assert_eq!(inspected_path, cache_root);
        assert_eq!(
            detection
                .as_ref()
                .and_then(LocalBlockRuntimeDetection::runtime_backend),
            Some(LocalBlockBackendKind::Current)
        );
        assert_eq!(local_cache_state, None);
        assert!(
            !layout.payload_blocks_dir.exists(),
            "janitor inspection should not provision missing runtime directories"
        );
        assert!(
            !layout.index_dir.exists(),
            "janitor inspection should remain read-only for missing runtime directories"
        );
        assert!(
            !layout.tmp_dir.exists(),
            "janitor inspection should not create tmp directories as a side effect"
        );
    }
}

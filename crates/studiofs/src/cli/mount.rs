//! `studiofs mount` — mount a filesystem volume.

use chrono::Utc;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use studiofs_data::DataStoreConfig;
use studiofs_meta::redb_engine::RedbSnapshot;
use studiofs_meta::{MetaEngine, MetadataBackend, RedbMetaEngine, open_meta_engine};
use studiofs_vfs::{StudioVfs, VfsConfig, parse_size_bytes};
use tracing::{debug, info};

/// Parse a human-readable size string like "100GB", "512MB", "1TB".
fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim().to_uppercase();
    let (num_str, multiplier) = if s.ends_with("TB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024 * 1024)
    } else if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else {
        (s.as_str(), 1u64)
    };
    let num: u64 = num_str.trim().parse()?;
    Ok(num * multiplier)
}

fn resolve_mount_path(name: &str, mount_point: &str) -> PathBuf {
    let mount_path = Path::new(mount_point);
    #[cfg(windows)]
    if super::utils::is_drive_root(mount_point) {
        return mount_path.to_path_buf();
    }

    if mount_path.ends_with(name) {
        mount_path.to_path_buf()
    } else {
        mount_path.join(name)
    }
}

#[cfg(windows)]
fn normalize_windows_mount_path(path: &str) -> String {
    super::utils::normalize_mount_point(path)
}

fn parse_env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
}

fn parse_env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
}

fn parse_env_u64_any(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse::<u64>().ok())
}

fn parse_env_f64(key: &str) -> Option<f64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0)
}

fn parse_env_bool(key: &str) -> Option<bool> {
    let value = std::env::var(key).ok()?;
    let value = value.trim().to_ascii_lowercase();
    Some(matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn overlay_vfs_env(mut config: VfsConfig) -> VfsConfig {
    macro_rules! set_usize {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_usize($key) {
                config.$field = value;
            }
        };
    }
    macro_rules! set_u64 {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_u64($key) {
                config.$field = value;
            }
        };
    }
    macro_rules! set_f64 {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_f64($key) {
                config.$field = value;
            }
        };
    }
    macro_rules! set_size {
        ($key:literal, $field:ident) => {
            if let Ok(v) = std::env::var($key)
                && let Some(bytes) = parse_size_bytes(&v).filter(|v| *v > 0)
            {
                config.$field = bytes;
            }
        };
    }

    set_size!("STUDIOFS_MAX_DIRTY_BYTES", max_dirty_bytes);
    set_size!("STUDIOFS_MAX_SPILL_BYTES", max_spill_bytes);
    if let Ok(v) = std::env::var("STUDIOFS_INGRESS_SEGMENT_MAX_BYTES")
        && let Some(bytes) = parse_size_bytes(&v).filter(|v| *v > 0)
    {
        config.max_ingress_segment_bytes = bytes as u64;
    }
    set_u64!(
        "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS",
        metadata_invalidation_sync_interval_ms
    );
    set_u64!("STUDIOFS_READ_BUFFER_MB", read_buffer_mb);
    set_usize!("STUDIOFS_PREFETCH_CONCURRENCY", prefetch_concurrency);
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_WIDTH") {
        config.readahead_width = value as u32;
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_MIN_PREFETCH") {
        config.readahead_min_prefetch = value as u32;
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_SEQ_STEP") {
        config.readahead_seq_step = value as u8;
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_RANDOM_PENALTY") {
        config.readahead_random_penalty = value as u8;
    }
    set_usize!("STUDIOFS_DIR_PREFETCH_MAX_BATCH", dir_prefetch_max_batch);
    set_usize!(
        "STUDIOFS_READDIR_ATTR_BATCH_CHUNK",
        readdir_attr_batch_chunk
    );
    set_usize!(
        "STUDIOFS_READDIR_ATTR_BATCH_MAX_INODES",
        readdir_attr_batch_max_inodes
    );
    set_usize!(
        "STUDIOFS_PREFETCH_FOREGROUND_RESERVE",
        prefetch_foreground_reserve
    );
    set_usize!("STUDIOFS_PREFETCH_MAX_PER_CYCLE", prefetch_max_per_cycle);
    set_u64!(
        "STUDIOFS_PREFETCH_PAUSE_ON_NETERR_MS",
        prefetch_pause_on_neterr_ms
    );
    set_usize!("STUDIOFS_MAX_DRAIN_BATCH", max_drain_batch);
    set_u64!("STUDIOFS_LOCK_TTL_SECS", lock_ttl_secs);
    set_u64!("STUDIOFS_LOCK_RETRY_BASE_MS", lock_retry_base_ms);
    set_u64!("STUDIOFS_LOCK_RETRY_MAX_MS", lock_retry_max_ms);
    set_size!("STUDIOFS_XATTR_MAX_BYTES", max_xattr_bytes);
    set_usize!("STUDIOFS_DENTRY_CACHE_CAPACITY", dentry_cache_capacity);
    set_size!("STUDIOFS_DENTRY_CACHE_BYTES", dentry_cache_bytes);
    set_usize!(
        "STUDIOFS_PATH_SHORTCUT_CACHE_CAPACITY",
        path_shortcut_cache_capacity
    );
    set_size!(
        "STUDIOFS_PATH_SHORTCUT_CACHE_BYTES",
        path_shortcut_cache_bytes
    );
    set_usize!(
        "STUDIOFS_ATTR_READ_CACHE_CAPACITY",
        attr_read_cache_capacity
    );
    set_size!("STUDIOFS_ATTR_READ_CACHE_BYTES", attr_read_cache_bytes);
    set_u64!("STUDIOFS_JANITOR_INTERVAL_SECS", janitor_interval_secs);
    if let Some(value) = parse_env_u64_any("STUDIOFS_JANITOR_SKIP_NEWER_SECS") {
        config.janitor_skip_newer_secs = value;
    }
    set_u64!("STUDIOFS_JANITOR_MAX_DELETES", janitor_max_deletes);
    set_u64!("STUDIOFS_JANITOR_LEASE_TTL_SECS", janitor_lease_ttl_secs);
    set_usize!("STUDIOFS_UPLOAD_CACHE_IO_SLOTS", upload_cache_io_slots);
    if let Some(value) = parse_env_usize("STUDIOFS_UPLOAD_CACHE_IO_USER_PENALTY") {
        config.upload_cache_io_user_penalty = value as u32;
    }
    set_u64!(
        "STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS",
        upload_user_idle_grace_ms
    );
    if let Some(value) = parse_env_u64_any("STUDIOFS_UPLOAD_DELAY_MS") {
        config.upload_delay_ms = value;
    }
    set_u64!("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", upload_requeue_base_ms);
    set_u64!("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", upload_requeue_max_ms);
    set_u64!(
        "STUDIOFS_UPLOAD_STATS_INTERVAL_SECS",
        upload_stats_interval_secs
    );
    if let Some(value) = parse_env_usize("STUDIOFS_UPLOAD_MAX_ATTEMPTS") {
        config.upload_max_attempts = value as u32;
    }
    set_u64!(
        "STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS",
        upload_request_timeout_secs
    );
    set_u64!("STUDIOFS_UPLOAD_RETRY_BASE_MS", upload_retry_base_ms);
    set_u64!("STUDIOFS_UPLOAD_RETRY_MAX_MS", upload_retry_max_ms);
    if let Some(interval_ms) = parse_env_u64_any("STUDIOFS_SLAB_FLUSH_INTERVAL_MS") {
        if interval_ms > 0 {
            config.slab_flush_interval_ms = interval_ms;
        } else {
            config.background_slab_flush_enabled = false;
        }
    }
    if let Some(disable) = parse_env_bool("STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH") {
        config.background_slab_flush_enabled = !disable;
    }
    set_u64!(
        "STUDIOFS_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS",
        shutdown_metadata_flush_timeout_secs
    );
    set_u64!(
        "STUDIOFS_PUBLISH_VISIBILITY_TIMEOUT_SECS",
        publish_visibility_timeout_secs
    );
    set_f64!("STUDIOFS_FETCH_INITIAL_WINDOW", fetch_initial_window);
    set_f64!("STUDIOFS_FETCH_MIN_WINDOW", fetch_min_window);
    set_f64!("STUDIOFS_FETCH_MAX_WINDOW", fetch_max_window);
    set_f64!("STUDIOFS_FETCH_INCREASE_STEP", fetch_increase_step);
    set_f64!("STUDIOFS_FETCH_DECREASE_FACTOR", fetch_decrease_factor);
    set_u64!(
        "STUDIOFS_FETCH_DECREASE_COOLDOWN_SECS",
        fetch_decrease_cooldown_secs
    );
    if let Some(value) = parse_env_usize("STUDIOFS_FETCH_MAX_ATTEMPTS") {
        config.fetch_max_attempts = value as u32;
    }
    if let Some(value) = parse_env_usize("STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS") {
        config.prefetch_fetch_max_attempts = value as u32;
    }
    set_u64!(
        "STUDIOFS_FETCH_REQUEST_TIMEOUT_SECS",
        fetch_request_timeout_secs
    );
    set_u64!("STUDIOFS_FETCH_RETRY_BASE_MS", fetch_retry_base_ms);
    set_u64!("STUDIOFS_FETCH_RETRY_MAX_MS", fetch_retry_max_ms);
    set_usize!("STUDIOFS_FETCH_PREFETCH_RESERVE", fetch_prefetch_reserve);
    set_u64!(
        "STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS",
        fetch_prefetch_acquire_wait_ms
    );
    set_f64!("STUDIOFS_UPLOAD_INITIAL_WINDOW", upload_initial_window);
    set_f64!("STUDIOFS_UPLOAD_MIN_WINDOW", upload_min_window);
    set_f64!("STUDIOFS_UPLOAD_MAX_WINDOW", upload_max_window);
    set_f64!("STUDIOFS_UPLOAD_INCREASE_STEP", upload_increase_step);
    set_f64!("STUDIOFS_UPLOAD_DECREASE_FACTOR", upload_decrease_factor);
    set_u64!(
        "STUDIOFS_UPLOAD_DECREASE_COOLDOWN_SECS",
        upload_decrease_cooldown_secs
    );
    set_usize!("STUDIOFS_QOS_ACCEPT_SLOTS", qos_accept_slots);
    set_usize!("STUDIOFS_QOS_MATERIALIZE_SLOTS", qos_materialize_slots);
    set_usize!("STUDIOFS_QOS_UPLOAD_SLOTS", qos_upload_slots);
    set_usize!("STUDIOFS_QOS_COMMIT_SLOTS", qos_commit_slots);
    set_usize!("STUDIOFS_QOS_FETCH_SLOTS", qos_fetch_slots);
    set_u64!(
        "STUDIOFS_INGRESS_CHECKPOINT_INTERVAL_SECS",
        ingress_checkpoint_interval_secs
    );
    set_u64!(
        "STUDIOFS_INGRESS_CHECKPOINT_MIN_APPEND_BYTES",
        ingress_checkpoint_min_append_bytes
    );
    set_u64!(
        "STUDIOFS_INGRESS_CHECKPOINT_MIN_MUTATIONS",
        ingress_checkpoint_min_mutations
    );
    set_u64!(
        "STUDIOFS_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG",
        ingress_checkpoint_max_segment_lag
    );
    set_u64!(
        "STUDIOFS_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS",
        ingress_checkpoint_max_live_segments
    );
    if let Ok(principal) = std::env::var("STUDIOFS_PRINCIPAL") {
        let principal = principal.trim();
        if !principal.is_empty() {
            config.principal = principal.to_string();
        }
    }
    if let Some(worker_count) = parse_env_usize("STUDIOFS_UPLOAD_WORKER_COUNT") {
        config.upload_worker_count = worker_count;
    } else if let Some(max_spawned) = parse_env_usize("STUDIOFS_UPLOAD_MAX_SPAWNED") {
        config.upload_worker_count = max_spawned;
    }
    config
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    mount_point: Option<&str>,
    meta_dir: &Path,
    cache_dir: Option<&Path>,
    cache_size: Option<&str>,
    memory_limit: Option<&str>,
    principal: Option<&str>,
    foreground: bool,
    detach: bool,
    snapshot_id: Option<&str>,
) -> anyhow::Result<()> {
    let (mount_point, resolved_cache_dir, memory_limit, cache_size) =
        super::settings::resolve_studiofs_config_for_mount(
            mount_point,
            cache_dir,
            memory_limit,
            cache_size,
        )?;

    let cache_dir = resolved_cache_dir;
    let mut vfs_config = super::settings::load_studiofs_config()?.vfs;
    vfs_config = overlay_vfs_env(vfs_config);

    #[cfg(windows)]
    let mount_point = {
        if let Ok(from_env) = std::env::var("STUDIOFS_DAEMON_MOUNT_POINT_INTERNAL") {
            from_env
        } else {
            mount_point.to_string()
        }
    };

    #[cfg(not(windows))]
    let mount_point = std::env::var("STUDIOFS_DAEMON_MOUNT_POINT").unwrap_or(mount_point);

    #[cfg(windows)]
    {
        if !super::utils::is_drive_root(&mount_point) {
            anyhow::bail!(
                "Invalid mount point '{}': Windows mount is supported only on drive letters (for example `Z:`). Directory mount points are not supported.",
                mount_point
            );
        }
    }

    let mut actual_mount_path = resolve_mount_path(name, &mount_point)
        .to_string_lossy()
        .into_owned();
    #[cfg(windows)]
    let is_drive_root = super::utils::is_drive_root(&actual_mount_path);

    #[cfg(not(windows))]
    let is_drive_root = false;

    if !is_drive_root && let Err(err) = std::fs::create_dir_all(&actual_mount_path) {
        #[cfg(target_os = "macos")]
        if err.kind() == std::io::ErrorKind::PermissionDenied
            && actual_mount_path.contains("/Volumes")
        {
            anyhow::bail!(
                "Failed to create mount point '{}'. You can grant mount access with:\n  sudo mkdir -p '{}'\n  sudo chown $(id -un):$(id -gn) '{}'",
                actual_mount_path,
                actual_mount_path,
                actual_mount_path
            );
        }

        return Err(err.into());
    }

    // Canonicalize the mount point here for detached mode, where the daemon may run
    // from a different working directory.
    #[cfg(not(windows))]
    {
        if let Ok(abs_path) = std::fs::canonicalize(&actual_mount_path) {
            actual_mount_path = abs_path.to_string_lossy().into_owned();
        }
    }

    #[cfg(windows)]
    if !is_drive_root && let Ok(abs_path) = std::fs::canonicalize(&actual_mount_path) {
        actual_mount_path = normalize_windows_mount_path(&abs_path.to_string_lossy());
    }

    let actual_mount_path = actual_mount_path;
    #[cfg(windows)]
    if !is_drive_root {
        let mut entries = std::fs::read_dir(&actual_mount_path)?;
        if entries.next().is_some() {
            anyhow::bail!(
                "Mount point '{}' must be empty to mount a volume.",
                actual_mount_path
            );
        }
    }

    // ── Daemon mode: re-spawn in background and exit ────────────────────────
    if detach {
        return spawn_daemon(name, &actual_mount_path, meta_dir);
    }

    let run_in_foreground = foreground || !detach;

    if let Some(principal) = principal {
        // SAFETY: this happens before Tokio runtime creation, so no runtime
        // worker threads are alive to race on process environment access.
        unsafe {
            std::env::set_var("STUDIOFS_PRINCIPAL", principal);
        }
        vfs_config.principal = principal.to_string();
    }

    // ── Guard against double-mount ──────────────────────────────────────────
    // Note: when launched via `mount -d`, the parent pre-writes the PID file
    // with the child's PID before the child starts.  The child must therefore
    // ignore a PID file that points to itself.
    if let Ok((pid, _, _)) = super::utils::read_pid_file(&actual_mount_path) {
        if pid != std::process::id() && super::utils::is_process_running(pid) {
            anyhow::bail!(
                "Already mounted at '{}' (PID {}). Run `studiofs umount \"{}\"` first.",
                actual_mount_path,
                pid,
                actual_mount_path
            );
        }
        // Stale PID file (or the pre-written entry from the parent) — clean up
        let _ = super::utils::remove_pid_file(&actual_mount_path);
    }

    let db_path = meta_dir.join(format!("{name}.db"));
    debug!("opening volume {name:?} from {db_path:?}");
    let live_meta = open_meta_engine(&db_path)?;
    let mut format_info = live_meta.get_format_info()?;
    let (meta, read_only_mount) = if let Some(snapshot_id) = snapshot_id {
        let snapshot = live_meta
            .get_snapshot(snapshot_id)?
            .ok_or_else(|| anyhow::anyhow!("snapshot '{snapshot_id}' not found"))?;
        if snapshot.snapshot_payload.is_empty() {
            anyhow::bail!("snapshot '{snapshot_id}' payload is empty");
        }
        let snapshot_state: RedbSnapshot = serde_json::from_slice(&snapshot.snapshot_payload)
            .map_err(|e| anyhow::anyhow!("invalid snapshot payload for '{snapshot_id}': {e}"))?;

        let snapshot_dir = meta_dir.join("snapshots");
        std::fs::create_dir_all(&snapshot_dir)?;
        let snapshot_db_path = snapshot_dir.join(format!("{name}.{snapshot_id}.db"));
        if snapshot_db_path.exists() {
            let _ = std::fs::remove_file(&snapshot_db_path);
        }
        let snapshot_engine = RedbMetaEngine::open(&snapshot_db_path)?;
        format_info.metadata.backend = MetadataBackend::RedbLocal;
        format_info.metadata.postgres = None;
        snapshot_engine.set_format_info(&format_info)?;
        snapshot_engine.import_snapshot(&snapshot_state)?;

        (
            Arc::new(snapshot_engine) as Arc<dyn studiofs_meta::MetaEngine>,
            true,
        )
    } else {
        (live_meta, false)
    };

    info!(
        "mounting volume '{}' (storage: endpoint={}, bucket={})",
        format_info.name, format_info.endpoint, format_info.bucket
    );

    // Set up data store config (StudioVfs creates Read + Write connections directly)
    let data_config = DataStoreConfig::from(&format_info);

    // Set up caches
    let mem_limit = parse_size(&memory_limit)?;
    let cache_size_bytes = parse_size(&cache_size)?;
    let base_cache_path = cache_dir.unwrap_or_else(|| meta_dir.join("cache"));
    std::fs::create_dir_all(&base_cache_path)?;

    // BlockCache will create blocks/ and upload/ subdirectories inside this UUID dir.
    let cache_path = base_cache_path.join(&format_info.uuid);

    let cache_path_str = cache_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid cache path"))?;

    // Create a tokio runtime for async ops in FUSE callbacks and background tasks
    let rt = Arc::new(
        tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("failed to create tokio runtime: {e}"))?,
    );
    let _guard = rt.enter();

    // Create VFS.
    let vfs = StudioVfs::new(
        meta.clone(),
        &data_config,
        cache_path_str,
        cache_size_bytes,
        mem_limit,
        vfs_config,
    )
    .map_err(|e| anyhow::anyhow!("failed to init vfs: {e}"))?;

    println!(
        "✓ Volume '{}' ready to mount at '{}'",
        name, actual_mount_path
    );
    println!("  Memory limit: {}", memory_limit);
    println!("  Cache size:   {}", cache_size);
    println!(
        "  Storage:      {} / {}",
        format_info.endpoint, format_info.bucket
    );
    println!("  Invalidation: PostgreSQL sync");
    if let Some(snapshot_id) = snapshot_id {
        println!("  Snapshot:     {} (read-only)", snapshot_id);
    }

    // Background tasks (uploader + GC) are spawned internally by StudioVfs::new

    // Write PID file so `studiofs umount` can find us
    let _ = super::utils::write_pid_file(&actual_mount_path, name, std::process::id());

    // Start platform-specific FUSE mount
    #[cfg(windows)]
    {
        // Named event for external graceful shutdown (used by `studiofs umount`).
        let event_name = super::utils::shutdown_event_name(&actual_mount_path);
        info!("starting WinFSP mount at {actual_mount_path}");
        studiofs_winfsp::mount_and_serve(
            vfs,
            rt,
            &actual_mount_path,
            &format_info.name,
            run_in_foreground,
            read_only_mount,
            Some(&event_name),
        )
        .map_err(|e| anyhow::anyhow!("WinFSP error: {e}"))?;

        // WinFSP returns normally — clean up PID file
        let _ = super::utils::remove_pid_file(&actual_mount_path);
    }

    #[cfg(unix)]
    {
        info!("starting FUSE mount at {actual_mount_path}");
        studiofs_fuse::mount_and_serve(
            vfs,
            rt,
            &actual_mount_path,
            &format_info.name,
            run_in_foreground,
            read_only_mount,
        )
        .map_err(|e| anyhow::anyhow!("FUSE error: {e}"))?;
        // Note: FUSE mount_and_serve calls process::exit(0), so this is unreachable.
        // PID file is cleaned up by the umount command or overwritten on next mount.
    }

    Ok(())
}

// ─── Daemon spawn ────────────────────────────────────────────────────────────

/// Re-execute the current process without `-d`/`--detach`, detached from the
/// terminal. The child enables daily-rotated tracing logs under `~/.studiofs/logs/`.
fn spawn_daemon(name: &str, mount_point: &str, meta_dir: &Path) -> anyhow::Result<()> {
    // Reconstruct args, stripping the detach flag
    let args: Vec<String> = std::env::args()
        .skip(1) // skip program name
        .filter(|a| a != "-d" && a != "--detach")
        .map(|arg| arg.to_string())
        .collect();

    let logs_dir = super::utils::get_logs_dir()?;
    let log_stem = daemon_log_stem(name, meta_dir)?;
    let today = Utc::now().format("%Y-%m-%d");
    let log_path = logs_dir.join(format!("{log_stem}.log.{today}"));

    let exe = std::env::current_exe()?;
    let mut cmd = std::process::Command::new(&exe);
    cmd.args(&args)
        .env("STUDIOFS_DAEMON_LOG_STEM", &log_stem)
        .env("STUDIOFS_DAEMON_LOG_DIR", &logs_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        cmd.env("STUDIOFS_DAEMON_MOUNT_POINT_INTERNAL", mount_point);
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }

    #[cfg(not(windows))]
    {
        cmd.env("STUDIOFS_DAEMON_MOUNT_POINT", mount_point);
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Create a new session so the child is fully detached from the terminal.
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }

    let child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn background mount process: {e}"))?;
    let pid = child.id();

    // Write PID file (the child will overwrite with identical data once it starts)
    super::utils::write_pid_file(mount_point, name, pid)?;

    println!(
        "✓ Volume '{}' mounting at '{}' in background (PID: {})",
        name, mount_point, pid
    );
    println!("  Log file: {}", log_path.display());
    println!("  To unmount: studiofs umount \"{}\"", mount_point);

    Ok(())
}

fn daemon_log_stem(name: &str, meta_dir: &Path) -> anyhow::Result<String> {
    let db_path = meta_dir.join(format!("{name}.db"));
    let meta = open_meta_engine(&db_path)?;
    let format_info = meta.get_format_info()?;
    Ok(format!(
        "{}-{}",
        sanitize_log_component(name),
        format_info.uuid
    ))
}

fn sanitize_log_component(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "volume".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("100GB").unwrap(), 100 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_size("1TB").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }

    #[test]
    fn test_resolve_mount_path() {
        assert_eq!(
            resolve_mount_path("projectfs", "/mnt"),
            Path::new("/mnt").join("projectfs")
        );
        assert_eq!(
            resolve_mount_path("projectfs", "/mnt/projectfs"),
            Path::new("/mnt/projectfs")
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_resolve_mount_path_on_windows_drive_root() {
        assert_eq!(
            resolve_mount_path("projectfs", "Z:"),
            Path::new("Z:").to_path_buf()
        );
        assert_eq!(
            resolve_mount_path("projectfs", "Z:\\"),
            Path::new("Z:\\").to_path_buf()
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_normalize_windows_mount_path_removes_extended_prefix() {
        assert_eq!(
            normalize_windows_mount_path(r"\\?\C:\Users\valen\studiofs\test"),
            r"C:\Users\valen\studiofs\test"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_normalize_windows_mount_path_unchanged_without_prefix() {
        assert_eq!(
            normalize_windows_mount_path(r"C:\Users\valen\studiofs\test"),
            r"C:\Users\valen\studiofs\test"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_normalize_windows_mount_path_unc_extended_prefix() {
        assert_eq!(
            normalize_windows_mount_path(r"\\?\UNC\server\share\vol"),
            r"\\server\share\vol"
        );
    }
}

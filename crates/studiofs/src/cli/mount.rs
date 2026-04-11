//! `studiofs mount` — mount a filesystem volume.

use crate::cli::WRITE_PATH_OBSERVABILITY_FILE;
use chrono::Utc;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
use studiofs_cache::block_cache::DEFAULT_L1_SIZE;
use studiofs_cache::v3::CacheV3Layout;
use studiofs_data::DataStoreConfig;
use studiofs_meta::redb_engine::RedbSnapshot;
use studiofs_meta::{
    FormatInfo, MetaEngine, MetaFormat, MetadataBackend, RedbMetaEngine, open_meta_engine,
};
use studiofs_vfs::{StudioVfs, VfsConfigRaw, parse_size_bytes};
use sysinfo::System;
use tracing::{debug, info, warn};

const WRITE_PATH_OBSERVABILITY_INTERVAL: Duration = Duration::from_secs(1);
type MountProfileFinish = Box<dyn FnMut()>;

struct ResolvedMountConfig {
    vfs_config: studiofs_vfs::VfsConfigValidated,
    cache_mode: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountLaunchMode {
    Foreground,
    Daemon,
}

#[derive(Debug)]
struct MountStartupContext {
    actual_mount_path: String,
    base_cache_path: PathBuf,
    cache_size: String,
    cache_mode: String,
    vfs_config: studiofs_vfs::VfsConfigValidated,
    principal_to_export: Option<String>,
    run_in_foreground: bool,
    launch_mode: MountLaunchMode,
}

struct MountMetaSession {
    meta: Arc<dyn MetaEngine>,
    format_info: FormatInfo,
    read_only_mount: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DaemonSpawnPlan {
    current_exe: PathBuf,
    args: Vec<String>,
    log_stem: String,
    log_path: PathBuf,
    mount_point: String,
    mount_point_env_key: &'static str,
}

struct WritePathObservabilityPublisher {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl WritePathObservabilityPublisher {
    fn new(vfs: Arc<StudioVfs>, output_path: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = stop.clone();
        let output_path_for_thread = output_path.clone();
        let handle = std::thread::Builder::new()
            .name("studiofs-write-path-observability".to_string())
            .spawn(move || {
                loop {
                    if let Err(error) =
                        persist_write_path_observability_snapshot(&vfs, &output_path_for_thread)
                    {
                        warn!(
                            path = %output_path_for_thread.display(),
                            error = %error,
                            "failed to persist write-path observability snapshot"
                        );
                    }

                    if stop_for_thread.load(Ordering::Acquire) {
                        break;
                    }
                    std::thread::sleep(WRITE_PATH_OBSERVABILITY_INTERVAL);
                }
            });

        match handle {
            Ok(handle) => Self {
                stop,
                handle: Some(handle),
            },
            Err(error) => {
                warn!(
                    path = %output_path.display(),
                    error = %error,
                    "failed to spawn write-path observability publisher thread"
                );
                Self { stop, handle: None }
            }
        }
    }

    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            warn!("write-path observability publisher thread panicked");
        }
    }
}

impl Drop for WritePathObservabilityPublisher {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn persist_write_path_observability_snapshot(
    vfs: &StudioVfs,
    output_path: &Path,
) -> std::io::Result<()> {
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let snapshot = vfs.write_path_observability_snapshot();
    let payload = serde_json::to_vec(&snapshot)
        .map_err(|error| std::io::Error::other(format!("serialize metrics failed: {error}")))?;
    let tmp_path = output_path.with_extension("json.tmp");
    std::fs::write(&tmp_path, payload)?;
    if output_path.exists() {
        let _ = std::fs::remove_file(output_path);
    }
    std::fs::rename(tmp_path, output_path)?;
    Ok(())
}

/// Initial local-cache budget used to size the mount-time cache directory.
/// Capped at 25 % of total RAM so low-memory machines don't overshoot.
fn initial_cache_bytes() -> u64 {
    const DEFAULT_INITIAL_CACHE_CAP_BYTES: u64 = DEFAULT_L1_SIZE * 2;
    let mut sys = System::new();
    sys.refresh_memory();
    let total_ram = sys.total_memory();
    if total_ram == 0 {
        // Fall back to a sane default if sysinfo fails to report total memory.
        return DEFAULT_INITIAL_CACHE_CAP_BYTES;
    }
    let quarter_ram = total_ram / 4;
    quarter_ram.min(DEFAULT_INITIAL_CACHE_CAP_BYTES)
}

fn parse_positive_size_arg(raw: &str) -> anyhow::Result<u64> {
    parse_size_bytes(raw)
        .filter(|&bytes| bytes > 0)
        .ok_or_else(|| anyhow::anyhow!("invalid size '{}' (must be > 0)", raw))
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

fn ensure_cache3_layout(cache_root: &Path) -> anyhow::Result<()> {
    let layout = CacheV3Layout::from_cache_root(cache_root);
    std::fs::create_dir_all(&layout.tmp_dir)?;
    std::fs::create_dir_all(&layout.payload_blocks_dir)?;
    std::fs::create_dir_all(&layout.index_dir)?;
    if !layout.layout_marker.exists() {
        std::fs::write(&layout.layout_marker, b"v1")?;
    }
    Ok(())
}

fn prepare_cache3_runtime(cache_root: &Path) -> anyhow::Result<()> {
    ensure_cache3_layout(cache_root)
}

fn validate_endpoint_override(endpoint: Option<&str>) -> anyhow::Result<()> {
    let Some(ep) = endpoint else {
        return Ok(());
    };

    let parsed = url::Url::parse(ep)
        .ok()
        .or_else(|| url::Url::parse(&format!("http://{ep}")).ok());

    let valid = parsed
        .map(|url| {
            let scheme = url.scheme();
            let has_host = url.host().is_some();
            matches!(scheme, "http" | "https") && has_host
        })
        .unwrap_or(false);

    if !valid {
        anyhow::bail!(
            "invalid S3 endpoint '{}': must be an HTTP(S) URL with a host (e.g., http://192.168.1.50:9000)",
            ep
        );
    }

    Ok(())
}

fn build_mount_config_from_raw(
    vfs_config_raw: VfsConfigRaw,
    principal: Option<&str>,
) -> anyhow::Result<ResolvedMountConfig> {
    build_mount_config_from_raw_with(vfs_config_raw, principal, overlay_vfs_env)
}

fn build_mount_config_from_raw_with<F>(
    vfs_config_raw: VfsConfigRaw,
    principal: Option<&str>,
    overlay_config: F,
) -> anyhow::Result<ResolvedMountConfig>
where
    F: FnOnce(VfsConfigRaw) -> VfsConfigRaw,
{
    let mut vfs_config_raw = overlay_config(vfs_config_raw);
    if let Some(principal) = principal {
        vfs_config_raw.principal = Some(principal.to_string());
    }

    let vfs_config = vfs_config_raw
        .validate()
        .map_err(|e| anyhow::anyhow!("invalid VFS config: {e}"))?;
    let cache_mode = format!("{:?}", vfs_config.cache_safety_mode).to_ascii_lowercase();

    Ok(ResolvedMountConfig {
        vfs_config,
        cache_mode,
    })
}

fn resolve_mount_launch_mode(detach: bool) -> MountLaunchMode {
    if detach {
        MountLaunchMode::Daemon
    } else {
        MountLaunchMode::Foreground
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_mount_startup_context(
    name: &str,
    meta_dir: &Path,
    settings_config: super::settings::StudioFsConfig,
    cli_mount_point: Option<&str>,
    cli_cache_dir: Option<&Path>,
    cli_cache_size: Option<&str>,
    principal: Option<&str>,
    foreground: bool,
    detach: bool,
    endpoint: Option<&str>,
) -> anyhow::Result<MountStartupContext> {
    let resolved_settings = super::settings::resolve_mount_runtime_settings(
        &settings_config,
        cli_mount_point,
        cli_cache_dir,
        cli_cache_size,
    )?;
    let ResolvedMountConfig {
        vfs_config,
        cache_mode,
    } = build_mount_config_from_raw(settings_config.vfs, principal)?;

    validate_endpoint_override(endpoint)?;
    let mount_point = resolve_runtime_mount_point(resolved_settings.mount_point);
    let actual_mount_path = prepare_actual_mount_path(name, &mount_point)?;

    Ok(MountStartupContext {
        actual_mount_path,
        base_cache_path: resolved_settings
            .cache_dir
            .unwrap_or_else(|| meta_dir.join("cache")),
        cache_size: resolved_settings.cache_size,
        cache_mode,
        vfs_config,
        principal_to_export: principal.map(str::to_string),
        run_in_foreground: resolve_run_in_foreground(foreground, detach),
        launch_mode: resolve_mount_launch_mode(detach),
    })
}

fn resolve_run_in_foreground(foreground: bool, detach: bool) -> bool {
    foreground || !detach
}

fn export_mount_principal(principal: Option<&str>) {
    export_mount_principal_with(principal, |key, value| {
        // SAFETY: this happens before Tokio runtime creation, so no runtime
        // worker threads are alive to race on process environment access.
        unsafe {
            std::env::set_var(key, value);
        }
    });
}

fn export_mount_principal_with<F>(principal: Option<&str>, set_var: F)
where
    F: FnOnce(&str, &str),
{
    if let Some(principal) = principal {
        set_var("STUDIOFS_PRINCIPAL", principal);
    }
}

fn resolve_runtime_mount_point(mount_point: String) -> String {
    #[cfg(windows)]
    {
        resolve_runtime_mount_point_with("STUDIOFS_DAEMON_MOUNT_POINT_INTERNAL", mount_point)
    }

    #[cfg(not(windows))]
    {
        resolve_runtime_mount_point_with("STUDIOFS_DAEMON_MOUNT_POINT", mount_point)
    }
}

fn resolve_runtime_mount_point_with(env_key: &str, mount_point: String) -> String {
    resolve_runtime_mount_point_from(env_key, mount_point, |env_key| std::env::var(env_key))
}

fn resolve_runtime_mount_point_from<F>(env_key: &str, mount_point: String, read_var: F) -> String
where
    F: FnOnce(&str) -> Result<String, std::env::VarError>,
{
    read_var(env_key).unwrap_or(mount_point)
}

fn validate_mount_point_for_platform(mount_point: &str) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        if !super::utils::is_drive_root(mount_point) {
            anyhow::bail!(
                "Invalid mount point '{}': Windows mount is supported only on drive letters (for example `Z:`). Directory mount points are not supported.",
                mount_point
            );
        }
    }

    Ok(())
}

fn prepare_actual_mount_path(name: &str, mount_point: &str) -> anyhow::Result<String> {
    validate_mount_point_for_platform(mount_point)?;

    let mut actual_mount_path = resolve_mount_path(name, mount_point)
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

    Ok(actual_mount_path)
}

fn prepare_runtime_cache_path(
    cache_dir: &Path,
    volume_uuid: &str,
) -> anyhow::Result<(PathBuf, String)> {
    std::fs::create_dir_all(cache_dir)?;
    let cache_path = cache_dir.join(volume_uuid);
    prepare_cache3_runtime(&cache_path)?;
    let cache_path_str = cache_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid cache path"))?
        .to_string();
    Ok((cache_path, cache_path_str))
}

fn build_mount_profile_finish(
    profile: Option<super::mount_profile_types::MountProfileMode>,
    name: &str,
    cache_mode: &str,
    cache_path: &Path,
    run_in_foreground: bool,
) -> Option<MountProfileFinish> {
    let mount_mode = if run_in_foreground {
        "foreground"
    } else {
        "daemon"
    };

    #[cfg(windows)]
    let platform = "windows";
    #[cfg(target_os = "macos")]
    let platform = "macos";
    #[cfg(all(unix, not(target_os = "macos")))]
    let platform = "linux";

    #[cfg(windows)]
    let bridge = "winfsp";
    #[cfg(unix)]
    let bridge = "fuse";

    let profiling = super::mount_profile::MountProfileSession::start(
        profile, name, platform, bridge, mount_mode, cache_mode, cache_path,
    );
    profiling.map(|profiling| {
        let mut profiling = Some(profiling);
        Box::new(move || {
            if let Some(profiling) = profiling.take() {
                profiling.finish();
            }
        }) as MountProfileFinish
    })
}

fn overlay_vfs_env(mut config: VfsConfigRaw) -> VfsConfigRaw {
    macro_rules! set_usize {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_usize($key) {
                config.$field = Some(value);
            }
        };
    }
    macro_rules! set_u64 {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_u64($key) {
                config.$field = Some(value);
            }
        };
    }
    macro_rules! set_f64 {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_f64($key) {
                config.$field = Some(value);
            }
        };
    }
    macro_rules! set_size {
        ($key:literal, $field:ident) => {
            if let Ok(v) = std::env::var($key)
                && let Ok(bytes) = parse_positive_size_arg(&v)
                && let Ok(bytes) = usize::try_from(bytes)
            {
                config.$field = Some(bytes);
            }
        };
    }

    set_u64!(
        "STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS",
        metadata_invalidation_sync_interval_ms
    );
    set_u64!("STUDIOFS_READ_BUFFER_MB", read_buffer_mb);
    set_usize!("STUDIOFS_PREFETCH_CONCURRENCY", prefetch_concurrency);
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_WIDTH") {
        config.readahead_width = Some(value as u32);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_MIN_PREFETCH") {
        config.readahead_min_prefetch = Some(value as u32);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_SEQ_STEP") {
        config.readahead_seq_step = Some(value as u8);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_READAHEAD_RANDOM_PENALTY") {
        config.readahead_random_penalty = Some(value as u8);
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
        config.janitor_skip_newer_secs = Some(value);
    }
    set_u64!("STUDIOFS_JANITOR_MAX_DELETES", janitor_max_deletes);
    set_u64!("STUDIOFS_JANITOR_LEASE_TTL_SECS", janitor_lease_ttl_secs);
    set_u64!(
        "STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS",
        upload_user_idle_grace_ms
    );
    if let Some(value) = parse_env_u64_any("STUDIOFS_UPLOAD_DELAY_MS") {
        config.upload_delay_ms = Some(value);
    }
    set_u64!("STUDIOFS_UPLOAD_REQUEUE_BASE_MS", upload_requeue_base_ms);
    set_u64!("STUDIOFS_UPLOAD_REQUEUE_MAX_MS", upload_requeue_max_ms);
    set_u64!(
        "STUDIOFS_UPLOAD_STATS_INTERVAL_SECS",
        upload_stats_interval_secs
    );
    set_u64!(
        "STUDIOFS_UPLOAD_FG_READ_LATENCY_TARGET_MS",
        upload_fg_read_latency_target_ms
    );
    set_u64!(
        "STUDIOFS_UPLOAD_FG_READ_LATENCY_WINDOW_MS",
        upload_fg_read_latency_window_ms
    );
    set_u64!(
        "STUDIOFS_UPLOAD_FG_READ_LATENCY_STICKY_WINDOWS",
        upload_fg_read_latency_sticky_windows
    );
    if let Some(value) = parse_env_usize("STUDIOFS_UPLOAD_MAX_ATTEMPTS") {
        config.upload_max_attempts = Some(value as u32);
    }
    set_u64!(
        "STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS",
        upload_request_timeout_secs
    );
    set_u64!("STUDIOFS_UPLOAD_RETRY_BASE_MS", upload_retry_base_ms);
    set_u64!("STUDIOFS_UPLOAD_RETRY_MAX_MS", upload_retry_max_ms);
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
        config.fetch_max_attempts = Some(value as u32);
    }
    if let Some(value) = parse_env_usize("STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS") {
        config.prefetch_fetch_max_attempts = Some(value as u32);
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
    if let Ok(principal) = std::env::var("STUDIOFS_PRINCIPAL") {
        let principal = principal.trim();
        if !principal.is_empty() {
            config.principal = Some(principal.to_string());
        }
    }
    if let Some(worker_count) = parse_env_usize("STUDIOFS_UPLOAD_WORKER_COUNT") {
        config.upload_worker_count = Some(worker_count);
    } else if let Some(max_spawned) = parse_env_usize("STUDIOFS_UPLOAD_MAX_SPAWNED") {
        config.upload_worker_count = Some(max_spawned);
    }

    config
}

struct PreparedMountRuntime {
    actual_mount_path: String,
    cache_path: PathBuf,
    cache_path_str: String,
    format_info: FormatInfo,
    read_only_mount: bool,
    run_in_foreground: bool,
    vfs: Arc<StudioVfs>,
    rt: Arc<tokio::runtime::Runtime>,
    profiling: Option<MountProfileFinish>,
    write_path_observability_publisher: WritePathObservabilityPublisher,
}

impl PreparedMountRuntime {
    fn print_ready(&self, requested_name: &str, cache_size: &str, snapshot_id: Option<&str>) {
        for line in build_ready_banner_lines(
            requested_name,
            &self.actual_mount_path,
            cache_size,
            &self.cache_path_str,
            &self.format_info,
            snapshot_id,
        ) {
            println!("{line}");
        }
    }

    fn write_pid_file(&self, requested_name: &str) {
        let _ = super::utils::write_pid_file_with_cache_path(
            &self.actual_mount_path,
            requested_name,
            std::process::id(),
            Some(&self.cache_path),
        );
    }

    fn serve(mut self) -> anyhow::Result<()> {
        #[cfg(windows)]
        {
            let event_name = super::utils::shutdown_event_name(&self.actual_mount_path);
            info!("starting WinFSP mount at {}", self.actual_mount_path);
            let mount_result = studiofs_winfsp::mount_and_serve(
                self.vfs.clone(),
                self.rt.clone(),
                &self.actual_mount_path,
                &self.format_info.name,
                self.run_in_foreground,
                self.read_only_mount,
                Some(&event_name),
                self.profiling.take(),
            );
            self.write_path_observability_publisher.stop_and_join();
            finalize_mount_serve_result(
                "WinFSP",
                &self.actual_mount_path,
                true,
                mount_result,
                super::utils::remove_pid_file,
            )?;
        }

        #[cfg(unix)]
        {
            info!("starting FUSE mount at {}", self.actual_mount_path);
            let mount_result = studiofs_fuse::mount_and_serve(
                self.vfs.clone(),
                self.rt.clone(),
                &self.actual_mount_path,
                &self.format_info.name,
                self.run_in_foreground,
                self.read_only_mount,
                self.profiling.take(),
            );
            self.write_path_observability_publisher.stop_and_join();
            finalize_mount_serve_result(
                "FUSE",
                &self.actual_mount_path,
                false,
                mount_result,
                super::utils::remove_pid_file,
            )?;
            // Note: FUSE mount_and_serve calls process::exit(0), so this is unreachable.
            // PID file is cleaned up by the umount command or overwritten on next mount.
        }

        Ok(())
    }
}

fn guard_mount_pid_file(actual_mount_path: &str) -> anyhow::Result<()> {
    guard_mount_pid_file_with(
        actual_mount_path,
        std::process::id(),
        super::utils::read_pid_file,
        super::utils::remove_pid_file,
        super::utils::is_process_running,
    )
}

fn guard_mount_pid_file_with<ReadPid, RemovePid, IsRunning>(
    actual_mount_path: &str,
    current_pid: u32,
    read_pid_file: ReadPid,
    remove_pid_file: RemovePid,
    is_process_running: IsRunning,
) -> anyhow::Result<()>
where
    ReadPid: Fn(&str) -> anyhow::Result<(u32, String, String)>,
    RemovePid: Fn(&str) -> anyhow::Result<()>,
    IsRunning: Fn(u32) -> bool,
{
    if let Ok((pid, _, _)) = read_pid_file(actual_mount_path) {
        if pid != current_pid && is_process_running(pid) {
            anyhow::bail!(
                "Already mounted at '{}' (PID {}). Run `studiofs umount \"{}\"` first.",
                actual_mount_path,
                pid,
                actual_mount_path
            );
        }
        // Stale PID file (or the pre-written entry from the parent) — clean up.
        let _ = remove_pid_file(actual_mount_path);
    }
    Ok(())
}

fn build_ready_banner_lines(
    requested_name: &str,
    actual_mount_path: &str,
    cache_size: &str,
    cache_path_str: &str,
    format_info: &FormatInfo,
    snapshot_id: Option<&str>,
) -> Vec<String> {
    let mut lines = vec![
        format!(
            "✓ Volume '{}' ready to mount at '{}'",
            requested_name, actual_mount_path
        ),
        format!("  Cache size:   {cache_size}"),
        format!("  Cache path:   {cache_path_str}"),
        format!(
            "  Storage:      {} / {}",
            format_info.endpoint, format_info.bucket
        ),
        "  Invalidation: PostgreSQL sync".to_string(),
    ];
    if let Some(snapshot_id) = snapshot_id {
        lines.push(format!("  Snapshot:     {snapshot_id} (read-only)"));
    }
    lines
}

fn finalize_mount_serve_result<E, RemovePid>(
    backend_name: &str,
    actual_mount_path: &str,
    remove_pid_on_success: bool,
    result: Result<(), E>,
    remove_pid_file: RemovePid,
) -> anyhow::Result<()>
where
    E: std::fmt::Display,
    RemovePid: FnOnce(&str) -> anyhow::Result<()>,
{
    result.map_err(|error| anyhow::anyhow!("{backend_name} error: {error}"))?;
    if remove_pid_on_success {
        let _ = remove_pid_file(actual_mount_path);
    }
    Ok(())
}

fn open_live_mount_meta(
    name: &str,
    meta_dir: &Path,
) -> anyhow::Result<(Arc<dyn MetaEngine>, FormatInfo)> {
    let db_path = meta_dir.join(format!("{name}.db"));
    debug!("opening volume {name:?} from {db_path:?}");
    let live_meta = open_meta_engine(&db_path)?;

    if let Err(e) = live_meta.check_consistency() {
        if let studiofs_meta::engine::MetaError::UuidMismatch { local, remote } = &e {
            let cleanup = super::utils::format_cleanup_command(name);
            anyhow::bail!(
                "Disk '{}' has been recreated. Your local metadata is stale and incompatible with the current remote disk.\nLocal UUID: {}\nRemote UUID: {}\n\nTo resolve this, please run the following command to clean up your local state:\n  {}",
                name,
                local,
                remote,
                cleanup
            );
        }
        return Err(anyhow::anyhow!("metadata consistency check failed: {e}"));
    }

    let format_info = live_meta.get_format_info()?;
    Ok((live_meta, format_info))
}

fn materialize_snapshot_mount_meta(
    name: &str,
    meta_dir: &Path,
    live_meta: Arc<dyn MetaEngine>,
    mut format_info: FormatInfo,
    snapshot_id: &str,
) -> anyhow::Result<MountMetaSession> {
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

    Ok(MountMetaSession {
        meta: Arc::new(snapshot_engine),
        format_info,
        read_only_mount: true,
    })
}

fn apply_mount_endpoint_override(
    mut format_info: FormatInfo,
    endpoint: Option<&str>,
) -> FormatInfo {
    if let Some(endpoint) = endpoint {
        info!(
            original_endpoint = %format_info.endpoint,
            override_endpoint = %endpoint,
            "overriding S3 endpoint for this mount session"
        );
        format_info.endpoint = endpoint.to_string();
    }
    format_info
}

fn resolve_mount_meta_session(
    name: &str,
    meta_dir: &Path,
    snapshot_id: Option<&str>,
    endpoint: Option<&str>,
) -> anyhow::Result<MountMetaSession> {
    let (live_meta, format_info) = open_live_mount_meta(name, meta_dir)?;
    let mut session = if let Some(snapshot_id) = snapshot_id {
        materialize_snapshot_mount_meta(name, meta_dir, live_meta, format_info, snapshot_id)?
    } else {
        MountMetaSession {
            meta: live_meta,
            format_info,
            read_only_mount: false,
        }
    };

    session.format_info = apply_mount_endpoint_override(session.format_info, endpoint);

    info!(
        "mounting volume '{}' (storage: endpoint={}, bucket={})",
        session.format_info.name, session.format_info.endpoint, session.format_info.bucket
    );

    Ok(session)
}

#[allow(clippy::too_many_arguments)]
fn prepare_mount_runtime(
    name: &str,
    meta_dir: &Path,
    cache_dir: &Path,
    cache_size: &str,
    profile: Option<super::mount_profile_types::MountProfileMode>,
    snapshot_id: Option<&str>,
    endpoint: Option<&str>,
    cache_mode: &str,
    vfs_config: studiofs_vfs::VfsConfigValidated,
    actual_mount_path: &str,
    run_in_foreground: bool,
) -> anyhow::Result<PreparedMountRuntime> {
    let MountMetaSession {
        meta,
        format_info,
        read_only_mount,
    } = resolve_mount_meta_session(name, meta_dir, snapshot_id, endpoint)?;

    let data_config = DataStoreConfig::from(&format_info);
    let block_cache_l1_limit = initial_cache_bytes();
    let cache_size_bytes = parse_positive_size_arg(cache_size)?;
    let (cache_path, cache_path_str) = prepare_runtime_cache_path(cache_dir, &format_info.uuid)?;
    let profiling =
        build_mount_profile_finish(profile, name, cache_mode, &cache_path, run_in_foreground);

    let rt = Arc::new(
        tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("failed to create tokio runtime: {e}"))?,
    );
    let _guard = rt.enter();

    let vfs = StudioVfs::new(
        meta,
        &data_config,
        &cache_path_str,
        cache_size_bytes,
        block_cache_l1_limit,
        vfs_config,
    )
    .map_err(|e| anyhow::anyhow!("failed to init vfs: {e}"))?;
    vfs.start()
        .map_err(|e| anyhow::anyhow!("failed to start vfs background workers: {e}"))?;
    let write_path_observability_publisher = WritePathObservabilityPublisher::new(
        vfs.clone(),
        cache_path.join(WRITE_PATH_OBSERVABILITY_FILE),
    );

    Ok(PreparedMountRuntime {
        actual_mount_path: actual_mount_path.to_string(),
        cache_path,
        cache_path_str,
        format_info,
        read_only_mount,
        run_in_foreground,
        vfs,
        rt,
        profiling,
        write_path_observability_publisher,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    mount_point: Option<&str>,
    meta_dir: &Path,
    cache_dir: Option<&Path>,
    cache_size: Option<&str>,
    principal: Option<&str>,
    foreground: bool,
    detach: bool,
    profile: Option<super::mount_profile_types::MountProfileMode>,
    snapshot_id: Option<&str>,
    endpoint: Option<&str>,
) -> anyhow::Result<()> {
    let startup = resolve_mount_startup_context(
        name,
        meta_dir,
        super::settings::load_studiofs_config()?,
        mount_point,
        cache_dir,
        cache_size,
        principal,
        foreground,
        detach,
        endpoint,
    )?;

    let MountStartupContext {
        actual_mount_path,
        base_cache_path,
        cache_size,
        vfs_config,
        cache_mode,
        principal_to_export,
        run_in_foreground,
        launch_mode,
    } = startup;

    // ── Daemon mode: re-spawn in background and exit ────────────────────────
    if matches!(launch_mode, MountLaunchMode::Daemon) {
        return spawn_daemon(name, &actual_mount_path, meta_dir);
    }

    export_mount_principal(principal_to_export.as_deref());

    // ── Guard against double-mount ──────────────────────────────────────────
    // Note: when launched via `mount -d`, the parent pre-writes the PID file
    // with the child's PID before the child starts.  The child must therefore
    // ignore a PID file that points to itself.
    guard_mount_pid_file(&actual_mount_path)?;

    let runtime = prepare_mount_runtime(
        name,
        meta_dir,
        &base_cache_path,
        &cache_size,
        profile,
        snapshot_id,
        endpoint,
        &cache_mode,
        vfs_config,
        &actual_mount_path,
        run_in_foreground,
    )?;

    runtime.print_ready(name, &cache_size, snapshot_id);
    runtime.write_pid_file(name);
    runtime.serve()
}

// ─── Daemon spawn ────────────────────────────────────────────────────────────

/// Re-execute the current process without `-d`/`--detach`, detached from the
/// terminal. The child enables daily-rotated tracing logs under `~/.studiofs/logs/`.
fn spawn_daemon(name: &str, mount_point: &str, meta_dir: &Path) -> anyhow::Result<()> {
    let logs_dir = super::utils::get_logs_dir()?;
    let log_stem = daemon_log_stem(name, meta_dir)?;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let current_exe = std::env::current_exe()?;
    let plan = build_daemon_spawn_plan(
        std::env::args().skip(1),
        &logs_dir,
        current_exe,
        mount_point,
        log_stem,
        &today,
    );

    let mut cmd = std::process::Command::new(&plan.current_exe);
    cmd.args(&plan.args)
        .env("STUDIOFS_DAEMON_LOG_STEM", &plan.log_stem)
        .env("STUDIOFS_DAEMON_LOG_DIR", &logs_dir)
        .env(plan.mount_point_env_key, &plan.mount_point)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
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
    println!("  Log file: {}", plan.log_path.display());
    println!("  To unmount: studiofs umount \"{}\"", mount_point);

    Ok(())
}

fn build_daemon_spawn_plan<I>(
    raw_args: I,
    logs_dir: &Path,
    current_exe: PathBuf,
    mount_point: &str,
    log_stem: String,
    today: &str,
) -> DaemonSpawnPlan
where
    I: IntoIterator<Item = String>,
{
    DaemonSpawnPlan {
        current_exe,
        args: daemon_cli_args_from(raw_args),
        log_path: daemon_log_path(logs_dir, &log_stem, today),
        log_stem,
        mount_point: mount_point.to_string(),
        mount_point_env_key: daemon_mount_point_env_key(),
    }
}

fn daemon_cli_args_from<I>(args: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    args.into_iter()
        .filter(|arg| arg != "-d" && arg != "--detach")
        .collect()
}

fn daemon_log_path(logs_dir: &Path, log_stem: &str, today: &str) -> PathBuf {
    logs_dir.join(format!("{log_stem}.log.{today}"))
}

fn daemon_mount_point_env_key() -> &'static str {
    #[cfg(windows)]
    {
        "STUDIOFS_DAEMON_MOUNT_POINT_INTERNAL"
    }

    #[cfg(not(windows))]
    {
        "STUDIOFS_DAEMON_MOUNT_POINT"
    }
}

fn daemon_log_stem(name: &str, meta_dir: &Path) -> anyhow::Result<String> {
    let db_path = meta_dir.join(format!("{name}.db"));
    let meta = open_meta_engine(&db_path)?;
    let format_info = meta.get_format_info()?;
    Ok(compose_daemon_log_stem(name, &format_info.uuid))
}

fn compose_daemon_log_stem(name: &str, volume_uuid: &str) -> String {
    format!("{}-{volume_uuid}", sanitize_log_component(name))
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
    use crate::cli::settings;
    use crate::cli::utils;
    use studiofs_meta::{MetaFormat, MetaSnapshots, ROOT_INO, RedbMetaEngine};

    #[test]
    fn validate_endpoint_override_accepts_http_urls_and_bare_host_port() {
        validate_endpoint_override(Some("http://127.0.0.1:9000")).expect("full url");
        validate_endpoint_override(Some("127.0.0.1:9000")).expect("host:port");
        validate_endpoint_override(None).expect("none");
    }

    #[test]
    fn validate_endpoint_override_rejects_invalid_urls() {
        for endpoint in ["ftp://127.0.0.1:9000", "://missing-scheme"] {
            let err =
                validate_endpoint_override(Some(endpoint)).expect_err("invalid endpoint rejected");
            assert!(err.to_string().contains("invalid S3 endpoint"));
        }
    }

    #[test]
    fn prepare_cache3_runtime_creates_contract_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        prepare_cache3_runtime(temp.path()).expect("create cache3 layout");
        let layout = CacheV3Layout::from_cache_root(temp.path());
        assert!(layout.layout_marker.exists());
        assert!(layout.tmp_dir.exists());
        assert!(layout.payload_blocks_dir.exists());
        assert!(layout.index_dir.exists());
    }

    #[test]
    fn test_parse_positive_size_arg() {
        assert_eq!(
            parse_positive_size_arg("100GB").unwrap(),
            100 * 1024 * 1024 * 1024
        );
        assert_eq!(parse_positive_size_arg("1.5GB").unwrap(), 1_610_612_736);
        assert!(parse_positive_size_arg("0").is_err());
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

    #[test]
    fn resolve_run_in_foreground_covers_mode_combinations() {
        assert!(resolve_run_in_foreground(false, false));
        assert!(resolve_run_in_foreground(true, false));
        assert!(resolve_run_in_foreground(true, true));
        assert!(!resolve_run_in_foreground(false, true));
    }

    #[test]
    fn resolve_mount_launch_mode_tracks_detach_choice() {
        assert_eq!(
            resolve_mount_launch_mode(false),
            MountLaunchMode::Foreground
        );
        assert_eq!(resolve_mount_launch_mode(true), MountLaunchMode::Daemon);
    }

    #[test]
    fn resolve_runtime_mount_point_from_prefers_env_override() {
        assert_eq!(
            resolve_runtime_mount_point_from(
                "STUDIOFS_TEST_MOUNT_POINT",
                "/mnt/demo".to_string(),
                |_| Ok("/mnt/daemon".to_string())
            ),
            "/mnt/daemon"
        );
    }

    #[test]
    fn resolve_runtime_mount_point_from_falls_back_when_env_is_missing() {
        assert_eq!(
            resolve_runtime_mount_point_from(
                "STUDIOFS_TEST_MOUNT_POINT",
                "/mnt/demo".to_string(),
                |_| Err(std::env::VarError::NotPresent)
            ),
            "/mnt/demo"
        );
    }

    #[test]
    fn export_mount_principal_with_sets_value_only_when_present() {
        let mut captured = None;
        export_mount_principal_with(Some("alice"), |key, value| {
            captured = Some((key.to_string(), value.to_string()));
        });
        assert_eq!(
            captured,
            Some(("STUDIOFS_PRINCIPAL".to_string(), "alice".to_string()))
        );

        let mut missing = false;
        export_mount_principal_with(None, |_, _| {
            missing = true;
        });
        assert!(
            !missing,
            "principal export helper should skip env mutation when unset"
        );
    }

    #[test]
    fn prepare_runtime_cache_path_scopes_layout_by_volume_uuid() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (cache_path, cache_path_str) =
            prepare_runtime_cache_path(temp.path(), "volume-uuid").expect("cache path prepared");

        assert_eq!(cache_path, temp.path().join("volume-uuid"));
        assert_eq!(cache_path_str, cache_path.to_string_lossy());
        let layout = CacheV3Layout::from_cache_root(&cache_path);
        assert!(layout.layout_marker.exists());
        assert!(layout.tmp_dir.exists());
        assert!(layout.payload_blocks_dir.exists());
        assert!(layout.index_dir.exists());
    }

    #[cfg(not(windows))]
    #[test]
    fn prepare_actual_mount_path_creates_and_canonicalizes_mount_dir() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mount_point = temp.path().join("mnt");
        let actual_mount_path =
            prepare_actual_mount_path("projectfs", mount_point.to_str().expect("utf8 path"))
                .expect("mount path prepared");

        let expected = std::fs::canonicalize(mount_point.join("projectfs")).expect("canonical");
        assert_eq!(PathBuf::from(&actual_mount_path), expected);
        assert!(expected.is_dir());
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

    #[cfg(windows)]
    #[test]
    fn prepare_actual_mount_path_rejects_windows_directory_mount_points() {
        let err = prepare_actual_mount_path("projectfs", r"C:\temp\studiofs")
            .expect_err("windows helper should reject directory mount points");
        assert!(err.to_string().contains("drive letters"));
    }

    #[test]
    fn daemon_cli_args_from_strips_detach_flags_only() {
        let args = daemon_cli_args_from(vec![
            "mount".to_string(),
            "demo".to_string(),
            "-d".to_string(),
            "--cache-size".to_string(),
            "10GB".to_string(),
            "--detach".to_string(),
            "--foreground".to_string(),
        ]);

        assert_eq!(
            args,
            vec![
                "mount".to_string(),
                "demo".to_string(),
                "--cache-size".to_string(),
                "10GB".to_string(),
                "--foreground".to_string(),
            ]
        );
    }

    #[test]
    fn compose_daemon_log_stem_sanitizes_name_and_appends_uuid() {
        assert_eq!(
            compose_daemon_log_stem("demo volume/01", "abc-123"),
            "demo_volume_01-abc-123"
        );
        assert_eq!(compose_daemon_log_stem("", "abc-123"), "volume-abc-123");
    }

    #[test]
    fn daemon_log_path_uses_log_stem_and_date() {
        let logs_dir = Path::new("/tmp/studiofs-logs");
        assert_eq!(
            daemon_log_path(logs_dir, "demo-uuid", "2026-04-11"),
            logs_dir.join("demo-uuid.log.2026-04-11")
        );
    }

    #[test]
    fn build_daemon_spawn_plan_strips_detach_and_builds_log_contract() {
        let plan = build_daemon_spawn_plan(
            vec![
                "mount".to_string(),
                "demo".to_string(),
                "-d".to_string(),
                "--cache-size".to_string(),
                "10GB".to_string(),
                "--detach".to_string(),
            ],
            Path::new("/tmp/studiofs-logs"),
            PathBuf::from("/tmp/studiofs.exe"),
            "/mnt/demo",
            "demo-uuid".to_string(),
            "2026-04-11",
        );

        assert_eq!(plan.current_exe, PathBuf::from("/tmp/studiofs.exe"));
        assert_eq!(
            plan.args,
            vec![
                "mount".to_string(),
                "demo".to_string(),
                "--cache-size".to_string(),
                "10GB".to_string(),
            ]
        );
        assert_eq!(plan.log_stem, "demo-uuid");
        assert_eq!(
            plan.log_path,
            Path::new("/tmp/studiofs-logs").join("demo-uuid.log.2026-04-11")
        );
        assert_eq!(plan.mount_point, "/mnt/demo");
        if cfg!(windows) {
            assert_eq!(
                plan.mount_point_env_key,
                "STUDIOFS_DAEMON_MOUNT_POINT_INTERNAL"
            );
        } else {
            assert_eq!(plan.mount_point_env_key, "STUDIOFS_DAEMON_MOUNT_POINT");
        }
    }

    #[test]
    fn build_mount_config_from_raw_with_applies_principal_after_overlay_and_derives_cache_mode() {
        let raw = VfsConfigRaw {
            principal: Some("from-raw".to_string()),
            ..VfsConfigRaw::default()
        };

        let resolved = build_mount_config_from_raw_with(raw, Some("from-cli"), |mut cfg| {
            cfg.principal = Some("from-env".to_string());
            cfg
        })
        .expect("config should validate");

        assert_eq!(resolved.vfs_config.principal, "from-cli");
        assert_eq!(resolved.cache_mode, "fast");
    }

    fn seed_mount_meta_db(
        temp: &tempfile::TempDir,
        name: &str,
    ) -> (PathBuf, Arc<RedbMetaEngine>, FormatInfo) {
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let db_path = meta_dir.join(format!("{name}.db"));
        let engine = Arc::new(RedbMetaEngine::open(&db_path).expect("open redb"));
        let format_info = FormatInfo {
            name: name.to_string(),
            uuid: format!("{name}-uuid"),
            endpoint: "https://storage.example".to_string(),
            bucket: "bucket-demo".to_string(),
            ..FormatInfo::default()
        };
        engine
            .set_format_info(&format_info)
            .expect("set format info");
        engine.init_root(0, 0).expect("init root");
        (meta_dir, engine, format_info)
    }

    #[test]
    fn mount_meta_session_uses_live_metadata_when_snapshot_id_is_none() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, format_info) = seed_mount_meta_db(&temp, "demo");
        drop(engine);

        let session =
            resolve_mount_meta_session("demo", &meta_dir, None, None).expect("resolve live meta");

        assert!(!session.read_only_mount);
        assert_eq!(session.format_info.uuid, format_info.uuid);
        assert_eq!(session.format_info.endpoint, format_info.endpoint);
        let observed = session.meta.get_format_info().expect("meta format info");
        assert_eq!(observed.uuid, format_info.uuid);
    }

    #[test]
    fn mount_meta_session_materializes_snapshot_engine_and_marks_read_only() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, format_info) = seed_mount_meta_db(&temp, "demo");
        let snapshot = engine.create_snapshot(ROOT_INO).expect("create snapshot");
        drop(engine);

        let session = resolve_mount_meta_session("demo", &meta_dir, Some(&snapshot.id), None)
            .expect("resolve snapshot meta");

        assert!(session.read_only_mount);
        assert_eq!(session.format_info.uuid, format_info.uuid);
        assert_eq!(
            session.format_info.metadata.backend,
            MetadataBackend::RedbLocal
        );
        assert!(session.format_info.metadata.postgres.is_none());
        assert!(
            meta_dir
                .join("snapshots")
                .join(format!("demo.{}.db", snapshot.id))
                .exists(),
            "snapshot helper should materialize a local redb mount db"
        );
        let observed = session
            .meta
            .get_format_info()
            .expect("snapshot format info");
        assert_eq!(observed.metadata.backend, MetadataBackend::RedbLocal);
        assert!(observed.metadata.postgres.is_none());
    }

    #[test]
    fn mount_meta_session_rejects_missing_snapshot_id_before_runtime_bootstrap() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _format_info) = seed_mount_meta_db(&temp, "demo");
        drop(engine);

        let err =
            match resolve_mount_meta_session("demo", &meta_dir, Some("missing-snapshot"), None) {
                Ok(_) => panic!("missing snapshot should fail"),
                Err(err) => err,
            };

        assert!(
            err.to_string()
                .contains("snapshot 'missing-snapshot' not found")
        );
        assert!(
            !meta_dir.join("snapshots").exists(),
            "missing snapshot should fail before snapshot db materialization"
        );
    }

    #[test]
    fn mount_meta_session_applies_endpoint_override_to_runtime_format_info() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _format_info) = seed_mount_meta_db(&temp, "demo");
        drop(engine);

        let session =
            resolve_mount_meta_session("demo", &meta_dir, None, Some("http://127.0.0.1:9000"))
                .expect("resolve meta with endpoint override");

        assert_eq!(session.format_info.endpoint, "http://127.0.0.1:9000");
    }

    fn startup_settings_config(
        mount_point: String,
        cache_dir: Option<PathBuf>,
        cache_size: Option<&str>,
    ) -> settings::StudioFsConfig {
        settings::StudioFsConfig {
            mount_point: Some(mount_point),
            cache_dir: cache_dir.map(|path| path.to_string_lossy().into_owned()),
            cache_size: cache_size.map(str::to_string),
            vfs: VfsConfigRaw::default(),
        }
    }

    #[cfg(not(windows))]
    fn startup_mount_point(temp: &tempfile::TempDir) -> String {
        temp.path().join("mnt").to_string_lossy().into_owned()
    }

    #[cfg(windows)]
    fn startup_mount_point(_: &tempfile::TempDir) -> String {
        "Z:".to_string()
    }

    #[test]
    fn resolve_mount_startup_context_defaults_to_meta_cache_and_foreground_launch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let cli_mount_point = startup_mount_point(&temp);

        let startup = resolve_mount_startup_context(
            "projectfs",
            &meta_dir,
            startup_settings_config(cli_mount_point.clone(), None, Some("64GB")),
            Some(cli_mount_point.as_str()),
            None,
            None,
            Some("alice"),
            false,
            false,
            Some("127.0.0.1:9000"),
        )
        .expect("startup context resolved");

        assert_eq!(startup.base_cache_path, meta_dir.join("cache"));
        assert_eq!(startup.cache_size, "64GB");
        assert_eq!(startup.cache_mode, "fast");
        assert_eq!(startup.principal_to_export.as_deref(), Some("alice"));
        assert_eq!(startup.launch_mode, MountLaunchMode::Foreground);
        assert!(startup.run_in_foreground);
        assert_eq!(startup.vfs_config.principal, "alice");

        #[cfg(not(windows))]
        {
            let expected = std::fs::canonicalize(temp.path().join("mnt").join("projectfs"))
                .expect("canonical");
            assert_eq!(PathBuf::from(&startup.actual_mount_path), expected);
        }

        #[cfg(windows)]
        {
            assert_eq!(startup.actual_mount_path, "Z:");
        }
    }

    #[test]
    fn resolve_mount_startup_context_uses_configured_cache_dir_for_daemon_launch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let cli_mount_point = startup_mount_point(&temp);
        let configured_cache_dir = temp.path().join("configured-cache");

        let startup = resolve_mount_startup_context(
            "projectfs",
            &meta_dir,
            startup_settings_config(
                cli_mount_point.clone(),
                Some(configured_cache_dir.clone()),
                Some("32GB"),
            ),
            Some(cli_mount_point.as_str()),
            None,
            None,
            None,
            false,
            true,
            None,
        )
        .expect("startup context resolved");

        assert_eq!(startup.base_cache_path, configured_cache_dir);
        assert_eq!(startup.cache_size, "32GB");
        assert_eq!(startup.principal_to_export, None);
        assert_eq!(startup.launch_mode, MountLaunchMode::Daemon);
        assert!(!startup.run_in_foreground);
    }

    #[test]
    fn resolve_mount_startup_context_rejects_invalid_endpoint() {
        let temp = tempfile::tempdir().expect("tempdir");
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let cli_mount_point = startup_mount_point(&temp);

        let err = resolve_mount_startup_context(
            "projectfs",
            &meta_dir,
            startup_settings_config(cli_mount_point.clone(), None, Some("64GB")),
            Some(cli_mount_point.as_str()),
            None,
            None,
            None,
            false,
            false,
            Some("ftp://127.0.0.1:9000"),
        )
        .expect_err("invalid endpoint should be rejected");

        assert!(err.to_string().contains("invalid S3 endpoint"));

        #[cfg(not(windows))]
        {
            assert!(
                !temp.path().join("mnt").exists(),
                "invalid endpoint should fail before mount-path materialization"
            );
        }
    }

    #[test]
    fn build_ready_banner_lines_includes_storage_and_optional_snapshot() {
        let format_info = FormatInfo {
            endpoint: "https://storage.example".to_string(),
            bucket: "bucket-demo".to_string(),
            ..FormatInfo::default()
        };

        let lines = build_ready_banner_lines(
            "demo",
            "/mnt/demo",
            "64GB",
            "/cache/demo",
            &format_info,
            Some("snap-1"),
        );

        assert_eq!(lines[0], "✓ Volume 'demo' ready to mount at '/mnt/demo'");
        assert_eq!(lines[1], "  Cache size:   64GB");
        assert_eq!(lines[2], "  Cache path:   /cache/demo");
        assert_eq!(
            lines[3],
            "  Storage:      https://storage.example / bucket-demo"
        );
        assert_eq!(lines[4], "  Invalidation: PostgreSQL sync");
        assert_eq!(lines[5], "  Snapshot:     snap-1 (read-only)");

        let no_snapshot = build_ready_banner_lines(
            "demo",
            "/mnt/demo",
            "64GB",
            "/cache/demo",
            &format_info,
            None,
        );
        assert_eq!(no_snapshot.len(), 5);
    }

    #[test]
    fn finalize_mount_serve_result_cleans_pid_on_success() {
        let mut cleaned = None;
        finalize_mount_serve_result(
            "WinFSP",
            "/mnt/demo",
            true,
            Ok::<(), std::io::Error>(()),
            |path| {
                cleaned = Some(path.to_string());
                Ok(())
            },
        )
        .expect("serve success");

        assert_eq!(cleaned.as_deref(), Some("/mnt/demo"));
    }

    #[test]
    fn finalize_mount_serve_result_maps_errors_without_cleanup() {
        let mut cleaned = false;
        let err = finalize_mount_serve_result(
            "FUSE",
            "/mnt/demo",
            true,
            Err::<(), _>(std::io::Error::other("boom")),
            |_| {
                cleaned = true;
                Ok(())
            },
        )
        .expect_err("serve error should bubble");

        assert!(err.to_string().contains("FUSE error: boom"));
        assert!(!cleaned, "cleanup should not run on failed mount serve");
    }

    #[test]
    fn pid_guard_ignores_self_pid_and_removes_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mount_point = "/tmp/studiofs-self-pid";
        utils::write_pid_file_in(temp.path(), mount_point, "demo", 4242).expect("write pid file");

        guard_mount_pid_file_with(
            mount_point,
            4242,
            |mount_point| utils::read_pid_file_in(temp.path(), mount_point),
            |mount_point| utils::remove_pid_file_in(temp.path(), mount_point),
            |_| true,
        )
        .expect("self pid should be ignored");

        let err = utils::read_pid_file_in(temp.path(), mount_point)
            .expect_err("guard should remove the stale/self pid file");
        assert!(
            err.to_string().contains("No mount found"),
            "pid guard should remove the file after accepting the self pid"
        );
    }

    #[test]
    fn pid_guard_rejects_live_foreign_process_without_removing_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mount_point = "/tmp/studiofs-live-foreign-pid";
        utils::write_pid_file_in(temp.path(), mount_point, "demo", 7777).expect("write pid file");

        let err = guard_mount_pid_file_with(
            mount_point,
            4242,
            |mount_point| utils::read_pid_file_in(temp.path(), mount_point),
            |mount_point| utils::remove_pid_file_in(temp.path(), mount_point),
            |pid| pid == 7777,
        )
        .expect_err("live foreign pid should block the mount");

        assert!(
            err.to_string().contains("Already mounted"),
            "pid guard should reject an active foreign mount owner"
        );

        let (pid, recorded_mount_point, volume) =
            utils::read_pid_file_in(temp.path(), mount_point).expect("pid file should remain");
        assert_eq!(pid, 7777);
        assert_eq!(recorded_mount_point, mount_point);
        assert_eq!(volume, "demo");
    }

    #[test]
    fn pid_guard_allows_stale_foreign_pid_and_removes_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let mount_point = "/tmp/studiofs-stale-foreign-pid";
        utils::write_pid_file_in(temp.path(), mount_point, "demo", 7777).expect("write pid file");

        guard_mount_pid_file_with(
            mount_point,
            4242,
            |mount_point| utils::read_pid_file_in(temp.path(), mount_point),
            |mount_point| utils::remove_pid_file_in(temp.path(), mount_point),
            |_| false,
        )
        .expect("stale foreign pid should be cleaned up");

        let err = utils::read_pid_file_in(temp.path(), mount_point)
            .expect_err("stale foreign pid file should be removed");
        assert!(
            err.to_string().contains("No mount found"),
            "guard should remove stale foreign pid files after accepting them"
        );
    }
}

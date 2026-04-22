//! `studiofs mount` — mount a filesystem volume.

use super::MountCommand;
use super::backend::{BackendLaunch, MountBackend, default_mount_backend};
use crate::cli::WRITE_PATH_OBSERVABILITY_FILE;
use crate::cli::runtime_tuning::PublicRuntimeTuningArgs;
use crate::cli::{mount_profile, mount_profile_types, settings, utils};
use crate::diagnostics::{
    DiagnosticsDataSource, LiveMountDiagnosticsSource, LiveMountRuntimeFingerprint,
    LocalDiagnosticsHost, RuntimeProfilingBackend,
};
use chrono::Utc;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
#[cfg(test)]
use studiofs_cache::runtime::LocalBlockLayout;
use studiofs_cache::{
    DEFAULT_MEMORY_CACHE_SIZE, PreparedLocalBlockRuntime, prepare_local_block_runtime,
};
use studiofs_data::DataStoreConfig;
use studiofs_meta::{
    FormatInfo, MetaEngine, MetaFormat, MetaReplication, MetaSnapshots, MetadataBackend,
    RedbMetaEngine, open_meta_engine,
};
#[cfg(test)]
use studiofs_vfs::VfsConfig;
use studiofs_vfs::{
    StudioVfs, VfsConfigRaw, VfsConfigValidated, VfsMeta, into_vfs_meta, parse_size_bytes,
};
use sysinfo::System;
use tokio::runtime::Runtime;
use tracing::{debug, info, warn};

const WRITE_PATH_OBSERVABILITY_INTERVAL: Duration = Duration::from_secs(1);
type MountProfileFinish = Box<dyn FnMut()>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MountLaunchMode {
    Foreground,
    Daemon,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedMountRuntimeSettings {
    mount_point: String,
    cache_dir: Option<PathBuf>,
    cache_size: String,
}

#[derive(Debug)]
struct ResolvedMountConfig {
    vfs_config: VfsConfigValidated,
}

struct MountRequest {
    command: MountCommand,
    settings_config: settings::StudioFsConfig,
}

#[derive(Debug)]
struct MountPlan {
    command: MountCommand,
    actual_mount_path: String,
    base_cache_path: PathBuf,
    cache_size: String,
    vfs_config: VfsConfigValidated,
    run_in_foreground: bool,
    launch_mode: MountLaunchMode,
    read_only_mount: bool,
}

#[derive(Debug)]
struct PreparedVolumeCacheRuntime {
    cache_path: PathBuf,
    cache_path_str: String,
    cache_runtime: PreparedLocalBlockRuntime,
}

struct PreparedMount {
    plan: MountPlan,
    format_info: Option<FormatInfo>,
    snapshot_mount_db_path: Option<PathBuf>,
    foreground: Option<PreparedForegroundRuntime>,
}

pub(crate) struct MountSession {
    prepared: PreparedMount,
}

struct PreparedForegroundRuntime {
    cache_path: PathBuf,
    cache_path_str: String,
    backend: Box<dyn MountBackend>,
    vfs: Arc<StudioVfs>,
    runtime: Runtime,
    diagnostics_source: Arc<dyn DiagnosticsDataSource>,
    profiling_backend: Arc<dyn RuntimeProfilingBackend>,
    profiling: Option<MountProfileFinish>,
    write_path_observability_publisher: WritePathObservabilityPublisher,
}

trait MountOpenMeta: Send + Sync + MetaFormat + MetaReplication {}
impl<T> MountOpenMeta for T where T: ?Sized + Send + Sync + MetaFormat + MetaReplication {}

trait SnapshotSourceMeta: Send + Sync + MetaSnapshots {}
impl<T> SnapshotSourceMeta for T where T: ?Sized + Send + Sync + MetaSnapshots {}

trait MountRuntimeMeta: Send + Sync + VfsMeta {}
impl<T> MountRuntimeMeta for T where T: ?Sized + Send + Sync + VfsMeta {}

struct OpenedLiveMountMeta<M: ?Sized + MountOpenMeta> {
    source_meta: Arc<M>,
    runtime_meta: Arc<dyn VfsMeta>,
    format_info: FormatInfo,
}

struct MountMetaSession<M: ?Sized + MountRuntimeMeta> {
    meta: Arc<M>,
    format_info: FormatInfo,
    read_only_mount: bool,
    snapshot_db_path: Option<PathBuf>,
}

#[derive(Clone, PartialEq, Eq)]
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
                    if let Err(error) = persist_mount_path_snapshot(&vfs, &output_path_for_thread) {
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

fn persist_mount_path_snapshot(vfs: &StudioVfs, output_path: &Path) -> std::io::Result<()> {
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
    const DEFAULT_INITIAL_CACHE_CAP_BYTES: u64 = DEFAULT_MEMORY_CACHE_SIZE * 2;
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
    if utils::is_drive_root(mount_point) {
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
    utils::normalize_mount_point(path)
}

fn parse_env_usize_with<F>(read_var: &F, key: &str) -> Option<usize>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    read_var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
}

fn parse_env_u32_with<F>(read_var: &F, key: &str) -> Option<u32>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    parse_env_usize_with(read_var, key).and_then(|value| u32::try_from(value).ok())
}

fn parse_env_u8_with<F>(read_var: &F, key: &str) -> Option<u8>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    parse_env_usize_with(read_var, key).and_then(|value| u8::try_from(value).ok())
}

fn parse_env_u64_with<F>(read_var: &F, key: &str) -> Option<u64>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    read_var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
}

fn parse_env_u64_any_with<F>(read_var: &F, key: &str) -> Option<u64>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    read_var(key).ok().and_then(|v| v.parse::<u64>().ok())
}

fn parse_env_f64_with<F>(read_var: &F, key: &str) -> Option<f64>
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    read_var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| *v > 0.0)
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
    runtime_tuning: &PublicRuntimeTuningArgs,
) -> anyhow::Result<ResolvedMountConfig> {
    build_mount_config_with(vfs_config_raw, principal, runtime_tuning, overlay_vfs_env)
}

fn build_mount_config_with<F>(
    vfs_config_raw: VfsConfigRaw,
    principal: Option<&str>,
    runtime_tuning: &PublicRuntimeTuningArgs,
    overlay_config: F,
) -> anyhow::Result<ResolvedMountConfig>
where
    F: FnOnce(VfsConfigRaw) -> VfsConfigRaw,
{
    let mut vfs_config_raw = overlay_config(vfs_config_raw);
    if let Some(principal) = principal {
        vfs_config_raw.principal = Some(principal.to_string());
    }
    runtime_tuning.apply_to_vfs_config(&mut vfs_config_raw)?;
    strip_removed_public_mount_surface(&mut vfs_config_raw);

    let vfs_config = vfs_config_raw
        .validate()
        .map_err(|e| anyhow::anyhow!("invalid VFS config: {e}"))?;

    Ok(ResolvedMountConfig { vfs_config })
}

fn strip_removed_public_mount_surface(config: &mut VfsConfigRaw) {
    config.upload_delay_ms = None;
}

fn resolve_mount_launch_mode(detach: bool) -> MountLaunchMode {
    if detach {
        MountLaunchMode::Daemon
    } else {
        MountLaunchMode::Foreground
    }
}

fn non_empty_trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn configured_mount_point_for_mount(config: &settings::StudioFsConfig) -> Option<&str> {
    #[cfg(not(windows))]
    {
        non_empty_trimmed(config.mount_point.as_deref())
    }

    #[cfg(windows)]
    {
        let _ = config;
        None
    }
}

fn validate_mount_point_for_mount(mount_point: &str) -> anyhow::Result<()> {
    #[cfg(windows)]
    if !utils::is_drive_root(mount_point) {
        anyhow::bail!(
            "Invalid mount point '{}': Windows requires a drive letter mount (for example `Z:`).",
            mount_point
        );
    }
    #[cfg(not(windows))]
    let _ = mount_point;

    Ok(())
}

fn resolve_mount_point_for_mount(
    config: &settings::StudioFsConfig,
    cli_mount_point: Option<&str>,
) -> anyhow::Result<String> {
    let mount_point = non_empty_trimmed(cli_mount_point)
        .or_else(|| configured_mount_point_for_mount(config))
        .map(str::to_string)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No mount point provided.\n\
                 On Windows, you must pass a drive letter explicitly, for example `Z:`."
            )
        })?;
    validate_mount_point_for_mount(&mount_point)?;
    Ok(mount_point)
}

fn resolve_cache_dir_for_mount(
    config: &settings::StudioFsConfig,
    cli_cache_dir: Option<&Path>,
) -> Option<PathBuf> {
    cli_cache_dir
        .map(Path::to_path_buf)
        .or_else(|| non_empty_trimmed(config.cache_dir.as_deref()).map(PathBuf::from))
        .filter(|path| !path.as_os_str().is_empty())
}

fn resolve_cache_size_for_mount(
    config: &settings::StudioFsConfig,
    cli_cache_size: Option<&str>,
) -> String {
    non_empty_trimmed(cli_cache_size)
        .or_else(|| non_empty_trimmed(config.cache_size.as_deref()))
        .unwrap_or(settings::DEFAULT_CACHE_SIZE)
        .to_string()
}

fn resolve_mount_runtime_settings(
    config: &settings::StudioFsConfig,
    cli_mount_point: Option<&str>,
    cli_cache_dir: Option<&Path>,
    cli_cache_size: Option<&str>,
) -> anyhow::Result<ResolvedMountRuntimeSettings> {
    Ok(ResolvedMountRuntimeSettings {
        mount_point: resolve_mount_point_for_mount(config, cli_mount_point)?,
        cache_dir: resolve_cache_dir_for_mount(config, cli_cache_dir),
        cache_size: resolve_cache_size_for_mount(config, cli_cache_size),
    })
}

impl MountPlan {
    fn resolve(request: MountRequest) -> anyhow::Result<Self> {
        let MountRequest {
            command,
            settings_config,
        } = request;
        let resolved_settings = resolve_mount_runtime_settings(
            &settings_config,
            command.mount_point.as_deref(),
            command.cache_dir.as_deref(),
            command.cache_size.as_deref(),
        )?;
        let ResolvedMountConfig { vfs_config } = build_mount_config_from_raw(
            settings_config.vfs,
            command.principal.as_deref(),
            &command.runtime_tuning,
        )?;

        validate_endpoint_override(command.endpoint.as_deref())?;
        let mount_point = resolve_runtime_mount_point(resolved_settings.mount_point);
        let actual_mount_path = prepare_mount_path(&command.name, &mount_point)?;

        Ok(Self {
            actual_mount_path,
            base_cache_path: resolved_settings
                .cache_dir
                .unwrap_or_else(|| command.meta_dir.join("cache")),
            cache_size: resolved_settings.cache_size,
            vfs_config,
            run_in_foreground: resolve_run_in_foreground(command.foreground, command.detach),
            launch_mode: resolve_mount_launch_mode(command.detach),
            read_only_mount: command.snapshot_id.is_some(),
            command,
        })
    }
}

fn resolve_run_in_foreground(foreground: bool, detach: bool) -> bool {
    foreground || !detach
}

fn resolve_runtime_mount_point(mount_point: String) -> String {
    #[cfg(windows)]
    {
        resolve_runtime_mount_point_env("STUDIOFS_DAEMON_MOUNT_POINT_INTERNAL", mount_point)
    }

    #[cfg(not(windows))]
    {
        resolve_runtime_mount_point_env("STUDIOFS_DAEMON_MOUNT_POINT", mount_point)
    }
}

fn resolve_runtime_mount_point_env(env_key: &str, mount_point: String) -> String {
    resolve_runtime_mount_point_or(env_key, mount_point, |env_key| std::env::var(env_key))
}

fn resolve_runtime_mount_point_or<F>(env_key: &str, mount_point: String, read_var: F) -> String
where
    F: FnOnce(&str) -> Result<String, std::env::VarError>,
{
    read_var(env_key).unwrap_or(mount_point)
}

fn validate_mount_point(mount_point: &str) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        if !utils::is_drive_root(mount_point) {
            anyhow::bail!(
                "Invalid mount point '{}': Windows mount is supported only on drive letters (for example `Z:`). Directory mount points are not supported.",
                mount_point
            );
        }
    }
    #[cfg(not(windows))]
    let _ = mount_point;

    Ok(())
}

fn prepare_mount_path(name: &str, mount_point: &str) -> anyhow::Result<String> {
    validate_mount_point(mount_point)?;

    let mut actual_mount_path = resolve_mount_path(name, mount_point)
        .to_string_lossy()
        .into_owned();
    #[cfg(windows)]
    let is_drive_root = utils::is_drive_root(&actual_mount_path);

    #[cfg(not(windows))]
    let is_drive_root = false;

    if !is_drive_root && let Err(err) = std::fs::create_dir_all(&actual_mount_path) {
        #[cfg(target_os = "macos")]
        if matches!(
            err.kind(),
            std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::AlreadyExists
        ) && actual_mount_path.contains("/Volumes")
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

fn build_mount_profile_finish(
    profile: Option<mount_profile_types::MountProfileMode>,
    name: &str,
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

    let profiling = mount_profile::MountProfileSession::start(
        profile, name, platform, bridge, mount_mode, cache_path,
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

fn build_runtime_profiling_backend(
    name: &str,
    cache_path: &Path,
    run_in_foreground: bool,
) -> Arc<dyn RuntimeProfilingBackend> {
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

    mount_profile::runtime_profiling_backend(name, platform, bridge, mount_mode, cache_path)
}

fn overlay_vfs_env(config: VfsConfigRaw) -> VfsConfigRaw {
    overlay_vfs_env_with(config, |key| std::env::var(key))
}

fn overlay_vfs_env_with<F>(mut config: VfsConfigRaw, read_var: F) -> VfsConfigRaw
where
    F: Fn(&str) -> Result<String, std::env::VarError>,
{
    macro_rules! set_usize {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_usize_with(&read_var, $key) {
                config.$field = Some(value);
            }
        };
    }
    macro_rules! set_u64 {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_u64_with(&read_var, $key) {
                config.$field = Some(value);
            }
        };
    }
    macro_rules! set_f64 {
        ($key:literal, $field:ident) => {
            if let Some(value) = parse_env_f64_with(&read_var, $key) {
                config.$field = Some(value);
            }
        };
    }
    macro_rules! set_size {
        ($key:literal, $field:ident) => {
            if let Ok(v) = read_var($key)
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
    if let Some(value) = parse_env_u32_with(&read_var, "STUDIOFS_READAHEAD_WIDTH") {
        config.readahead_width = Some(value);
    }
    if let Some(value) = parse_env_u32_with(&read_var, "STUDIOFS_READAHEAD_MIN_PREFETCH") {
        config.readahead_min_prefetch = Some(value);
    }
    if let Some(value) = parse_env_u8_with(&read_var, "STUDIOFS_READAHEAD_SEQ_STEP") {
        config.readahead_seq_step = Some(value);
    }
    if let Some(value) = parse_env_u8_with(&read_var, "STUDIOFS_READAHEAD_RANDOM_PENALTY") {
        config.readahead_random_penalty = Some(value);
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
    if let Some(value) = parse_env_u64_any_with(&read_var, "STUDIOFS_JANITOR_SKIP_NEWER_SECS") {
        config.janitor_skip_newer_secs = Some(value);
    }
    set_u64!("STUDIOFS_JANITOR_MAX_DELETES", janitor_max_deletes);
    set_u64!("STUDIOFS_JANITOR_LEASE_TTL_SECS", janitor_lease_ttl_secs);
    set_u64!(
        "STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS",
        upload_user_idle_grace_ms
    );
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
    if let Some(value) = parse_env_u32_with(&read_var, "STUDIOFS_UPLOAD_MAX_ATTEMPTS") {
        config.upload_max_attempts = Some(value);
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
    if let Some(value) = parse_env_u32_with(&read_var, "STUDIOFS_FETCH_MAX_ATTEMPTS") {
        config.fetch_max_attempts = Some(value);
    }
    if let Some(value) = parse_env_u32_with(&read_var, "STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS") {
        config.prefetch_fetch_max_attempts = Some(value);
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
    set_usize!("STUDIOFS_QOS_COMMIT_SLOTS", qos_commit_slots);
    set_usize!("STUDIOFS_QOS_FETCH_SLOTS", qos_fetch_slots);
    if let Ok(principal) = read_var("STUDIOFS_PRINCIPAL") {
        let principal = principal.trim();
        if !principal.is_empty() {
            config.principal = Some(principal.to_string());
        }
    }
    if let Some(worker_count) = parse_env_usize_with(&read_var, "STUDIOFS_UPLOAD_WORKER_COUNT") {
        config.upload_worker_count = Some(worker_count);
    } else if let Some(max_spawned) = parse_env_usize_with(&read_var, "STUDIOFS_UPLOAD_MAX_SPAWNED")
    {
        config.upload_worker_count = Some(max_spawned);
    }

    config
}

fn prepare_volume_cache_runtime(
    cache_dir: &Path,
    volume_uuid: &str,
) -> anyhow::Result<PreparedVolumeCacheRuntime> {
    let cache_path = cache_dir.join(volume_uuid);
    let cache_runtime = prepare_local_block_runtime(&cache_path).map_err(|source| {
        anyhow::anyhow!(
            "failed to prepare local block runtime at '{}': {source}",
            cache_path.display()
        )
    })?;
    let cache_path_str = cache_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid cache path"))?
        .to_string();
    Ok(PreparedVolumeCacheRuntime {
        cache_path,
        cache_path_str,
        cache_runtime,
    })
}

fn shutdown_upload_drain_timeout() -> Duration {
    let secs = std::env::var("STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    Duration::from_secs(secs)
}

fn runtime_shutdown_timeout() -> Duration {
    let secs = std::env::var("STUDIOFS_RUNTIME_SHUTDOWN_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5);
    Duration::from_secs(secs)
}

impl MountSession {
    pub(crate) fn prepare(command: MountCommand) -> anyhow::Result<Self> {
        Self::prepare_with_backend_factory(
            command,
            settings::load_studiofs_config()?,
            default_mount_backend,
        )
    }

    fn prepare_with_backend_factory<Factory>(
        command: MountCommand,
        settings_config: settings::StudioFsConfig,
        backend_factory: Factory,
    ) -> anyhow::Result<Self>
    where
        Factory: FnOnce() -> Box<dyn MountBackend>,
    {
        let plan = MountPlan::resolve(MountRequest {
            command,
            settings_config,
        })?;
        let prepared = plan.prepare(backend_factory)?;
        Ok(Self { prepared })
    }

    pub(crate) fn launch(self) -> anyhow::Result<()> {
        self.prepared.launch()
    }
}

impl MountPlan {
    fn prepare<Factory>(self, backend_factory: Factory) -> anyhow::Result<PreparedMount>
    where
        Factory: FnOnce() -> Box<dyn MountBackend>,
    {
        if matches!(self.launch_mode, MountLaunchMode::Daemon) {
            return Ok(PreparedMount {
                plan: self,
                format_info: None,
                snapshot_mount_db_path: None,
                foreground: None,
            });
        }

        guard_mount_pid_file(&self.actual_mount_path)?;

        let MountMetaSession {
            meta,
            format_info,
            read_only_mount,
            snapshot_db_path,
        } = resolve_mount_meta_session(
            &self.command.name,
            &self.command.meta_dir,
            self.command.snapshot_id.as_deref(),
            self.command.endpoint.as_deref(),
        )?;
        debug_assert_eq!(self.read_only_mount, read_only_mount);

        let prepared_foreground = (|| -> anyhow::Result<PreparedForegroundRuntime> {
            let data_config = DataStoreConfig::from(&format_info);
            let memory_cache_budget_bytes = initial_cache_bytes();
            let PreparedVolumeCacheRuntime {
                cache_path,
                cache_path_str,
                cache_runtime,
            } = prepare_volume_cache_runtime(&self.base_cache_path, &format_info.uuid)?;
            let cache_size_bytes = parse_positive_size_arg(&self.cache_size)?;
            let profiling = build_mount_profile_finish(
                self.command.profile,
                &self.command.name,
                &cache_path,
                self.run_in_foreground,
            );
            let profiling_backend = build_runtime_profiling_backend(
                &self.command.name,
                &cache_path,
                self.run_in_foreground,
            );
            let runtime = Runtime::new()
                .map_err(|error| anyhow::anyhow!("failed to create tokio runtime: {error}"))?;
            let _guard = runtime.enter();
            let backend = backend_factory();
            let diagnostics_runtime_fingerprint = LiveMountRuntimeFingerprint {
                bridge: backend.name().to_string(),
                cache_size: self.command.cache_size.clone(),
                endpoint_override: self.command.endpoint.clone(),
                snapshot_id: self.command.snapshot_id.clone(),
                vfs_config: serde_json::to_value(self.vfs_config.as_ref())
                    .map_err(|error| anyhow::anyhow!("failed to serialize vfs config: {error}"))?,
            };
            let vfs = StudioVfs::new_with_prepared_cache_runtime(
                meta,
                &data_config,
                &cache_path_str,
                cache_size_bytes,
                memory_cache_budget_bytes,
                self.vfs_config.clone(),
                cache_runtime,
            )
            .map_err(|error| anyhow::anyhow!("failed to init vfs: {error}"))?;
            vfs.start().map_err(|error| {
                anyhow::anyhow!("failed to start vfs background workers: {error}")
            })?;
            let write_path_observability_publisher = WritePathObservabilityPublisher::new(
                vfs.clone(),
                cache_path.join(WRITE_PATH_OBSERVABILITY_FILE),
            );
            let diagnostics_source: Arc<dyn DiagnosticsDataSource> =
                Arc::new(LiveMountDiagnosticsSource::new(
                    &format_info,
                    &self.actual_mount_path,
                    &cache_path,
                    std::process::id(),
                    self.read_only_mount,
                    diagnostics_runtime_fingerprint,
                    vfs.clone(),
                ));

            Ok(PreparedForegroundRuntime {
                cache_path,
                cache_path_str,
                backend,
                vfs,
                runtime,
                diagnostics_source,
                profiling_backend,
                profiling,
                write_path_observability_publisher,
            })
        })();

        match prepared_foreground {
            Ok(foreground) => Ok(PreparedMount {
                plan: self,
                format_info: Some(format_info),
                snapshot_mount_db_path: snapshot_db_path,
                foreground: Some(foreground),
            }),
            Err(error) => {
                cleanup_snapshot_mount_db(snapshot_db_path.as_deref());
                Err(error)
            }
        }
    }
}

impl PreparedMount {
    fn launch(self) -> anyhow::Result<()> {
        if matches!(self.plan.launch_mode, MountLaunchMode::Daemon) {
            return spawn_daemon(
                &self.plan.command.name,
                &self.plan.actual_mount_path,
                &self.plan.command.meta_dir,
            );
        }

        self.print_ready();
        self.write_pid_file();
        self.launch_foreground()
    }

    fn print_ready(&self) {
        let format_info = self
            .format_info
            .as_ref()
            .expect("foreground mount sessions must resolve format info");
        let cache_path_str = &self
            .foreground
            .as_ref()
            .expect("foreground mount sessions must prepare runtime")
            .cache_path_str;
        for line in build_ready_banner_lines(
            &self.plan.command.name,
            &self.plan.actual_mount_path,
            &self.plan.cache_size,
            cache_path_str,
            format_info,
            self.plan.command.snapshot_id.as_deref(),
        ) {
            println!("{line}");
        }
    }

    fn write_pid_file(&self) {
        let prepared = self
            .foreground
            .as_ref()
            .expect("foreground mount sessions must prepare runtime");
        persist_mount_pid_file_best_effort_with(
            &self.plan.actual_mount_path,
            &self.plan.command.name,
            std::process::id(),
            Some(&prepared.cache_path),
            utils::write_pid_file_with_cache,
        );
    }

    fn launch_foreground(mut self) -> anyhow::Result<()> {
        let format_info = self
            .format_info
            .as_ref()
            .expect("foreground mount sessions must resolve format info");
        let prepared = self
            .foreground
            .take()
            .expect("foreground mount sessions must prepare runtime");
        let PreparedForegroundRuntime {
            cache_path: _,
            cache_path_str: _,
            backend,
            vfs,
            runtime,
            diagnostics_source,
            profiling_backend,
            mut profiling,
            mut write_path_observability_publisher,
        } = prepared;
        let backend_name = backend.name();
        info!(
            backend = backend_name,
            mount_point = %self.plan.actual_mount_path,
            "starting mount backend"
        );
        let diagnostics_host = LocalDiagnosticsHost::start_best_effort(
            runtime.handle(),
            &self.plan.actual_mount_path,
            diagnostics_source,
            profiling_backend,
        );
        let serve_result = backend.serve(BackendLaunch {
            vfs: vfs.clone(),
            runtime_handle: runtime.handle().clone(),
            mount_point: self.plan.actual_mount_path.clone(),
            volume_name: format_info.name.clone(),
            read_only: self.plan.read_only_mount,
            #[cfg(unix)]
            run_in_foreground: self.plan.run_in_foreground,
        });
        write_path_observability_publisher.stop_and_join();
        runtime.block_on(vfs.shutdown(shutdown_upload_drain_timeout()));
        if let Some(diagnostics_host) = diagnostics_host {
            diagnostics_host.stop();
        }
        if let Some(mut finish_profiling) = profiling.take() {
            finish_profiling();
        }
        drop(vfs);
        let _ = utils::remove_pid_file(&self.plan.actual_mount_path);
        cleanup_snapshot_mount_db(self.snapshot_mount_db_path.take().as_deref());
        runtime.shutdown_timeout(runtime_shutdown_timeout());

        serve_result.map_err(|error| anyhow::anyhow!("{backend_name} error: {error}"))?;
        Ok(())
    }
}

fn cleanup_snapshot_mount_db(snapshot_db_path: Option<&Path>) {
    let Some(snapshot_db_path) = snapshot_db_path else {
        return;
    };

    match std::fs::remove_file(snapshot_db_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => warn!(
            path = %snapshot_db_path.display(),
            error = %error,
            "failed to remove snapshot mount database"
        ),
    }
}

fn guard_mount_pid_file(actual_mount_path: &str) -> anyhow::Result<()> {
    guard_mount_pid_file_with(
        actual_mount_path,
        std::process::id(),
        utils::read_pid_file,
        utils::remove_pid_file,
        utils::is_process_running,
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
        if pid == current_pid {
            return Ok(());
        }
        if is_process_running(pid) {
            anyhow::bail!(
                "Already mounted at '{}' (PID {}). Run `studiofs umount \"{}\"` first.",
                actual_mount_path,
                pid,
                actual_mount_path
            );
        }
        // Stale PID file — clean up before the session writes its own entry.
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

fn persist_mount_pid_file_best_effort_with<WritePid>(
    actual_mount_path: &str,
    requested_name: &str,
    pid: u32,
    cache_path: Option<&Path>,
    write_pid_file: WritePid,
) where
    WritePid: FnOnce(&str, &str, u32, Option<&Path>) -> anyhow::Result<()>,
{
    let _ = write_pid_file(actual_mount_path, requested_name, pid, cache_path);
}

fn read_live_mount_format_info<M: ?Sized + MountOpenMeta>(
    name: &str,
    live_meta: &M,
) -> anyhow::Result<FormatInfo> {
    if let Err(e) = live_meta.check_consistency() {
        if let studiofs_meta::engine::MetaError::UuidMismatch { local, remote } = &e {
            anyhow::bail!(
                "Disk '{}' has been recreated. Your local metadata is stale and incompatible with the current remote disk.\nLocal UUID: {}\nRemote UUID: {}\n\nTo resolve this, please run the following command to clean up your local state:\n  {}",
                name,
                local,
                remote,
                utils::format_cleanup_command(name)
            );
        }
        return Err(anyhow::anyhow!("metadata consistency check failed: {e}"));
    }

    Ok(live_meta.get_format_info()?)
}

fn open_live_mount_meta(
    name: &str,
    meta_dir: &Path,
) -> anyhow::Result<OpenedLiveMountMeta<dyn MetaEngine>> {
    let db_path = meta_dir.join(format!("{name}.db"));
    debug!("opening volume {name:?} from {db_path:?}");
    let source_meta = open_meta_engine(&db_path)?;
    let format_info = read_live_mount_format_info(name, source_meta.as_ref())?;
    let runtime_meta = into_vfs_meta(source_meta.clone());
    Ok(OpenedLiveMountMeta {
        source_meta,
        runtime_meta,
        format_info,
    })
}

fn materialize_snapshot_mount_meta<M: ?Sized + SnapshotSourceMeta>(
    name: &str,
    meta_dir: &Path,
    live_meta: Arc<M>,
    mut format_info: FormatInfo,
    snapshot_id: &str,
) -> anyhow::Result<MountMetaSession<dyn VfsMeta>> {
    let snapshot = live_meta
        .get_snapshot(snapshot_id)?
        .ok_or_else(|| anyhow::anyhow!("snapshot '{snapshot_id}' not found"))?;
    if snapshot.snapshot_payload.is_empty() {
        anyhow::bail!("snapshot '{snapshot_id}' payload is empty");
    }
    let snapshot_state = RedbMetaEngine::decode_snapshot_payload(&snapshot.snapshot_payload)
        .map_err(|e| anyhow::anyhow!("invalid snapshot payload for '{snapshot_id}': {e}"))?;

    let snapshot_dir = meta_dir.join("snapshots");
    std::fs::create_dir_all(&snapshot_dir)?;
    let snapshot_db_path = snapshot_dir.join(format!("{name}.{snapshot_id}.db"));
    cleanup_snapshot_mount_db(Some(snapshot_db_path.as_path()));
    let snapshot_engine = (|| -> anyhow::Result<Arc<RedbMetaEngine>> {
        let snapshot_engine = Arc::new(RedbMetaEngine::open(&snapshot_db_path)?);
        format_info.metadata.backend = MetadataBackend::RedbLocal;
        format_info.metadata.postgres = None;
        snapshot_engine.set_format_info(&format_info)?;
        snapshot_engine.import_snapshot(&snapshot_state)?;
        Ok(snapshot_engine)
    })()
    .inspect_err(|_| cleanup_snapshot_mount_db(Some(snapshot_db_path.as_path())))?;

    Ok(MountMetaSession {
        meta: into_vfs_meta(snapshot_engine),
        format_info,
        read_only_mount: true,
        snapshot_db_path: Some(snapshot_db_path),
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
) -> anyhow::Result<MountMetaSession<dyn VfsMeta>> {
    let OpenedLiveMountMeta {
        source_meta,
        runtime_meta,
        format_info,
    } = open_live_mount_meta(name, meta_dir)?;
    let mut session = if let Some(snapshot_id) = snapshot_id {
        materialize_snapshot_mount_meta(name, meta_dir, source_meta, format_info, snapshot_id)?
    } else {
        MountMetaSession {
            meta: runtime_meta,
            format_info,
            read_only_mount: false,
            snapshot_db_path: None,
        }
    };

    session.format_info = apply_mount_endpoint_override(session.format_info, endpoint);

    info!(
        "mounting volume '{}' (storage: endpoint={}, bucket={})",
        session.format_info.name, session.format_info.endpoint, session.format_info.bucket
    );

    Ok(session)
}

// ─── Daemon spawn ────────────────────────────────────────────────────────────

/// Re-execute the current process without `-d`/`--detach`, detached from the
/// terminal. The child enables daily-rotated tracing logs under `~/.studiofs/logs/`.
fn spawn_daemon(name: &str, mount_point: &str, meta_dir: &Path) -> anyhow::Result<()> {
    let logs_dir = utils::get_logs_dir()?;
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
            // SAFETY: `setsid` is run in the pre-exec hook before `exec`; no Rust
            // state is touched after this point in the parent process.
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn background mount process: {e}"))?;
    let pid = child.id();

    // Write PID file (the child will overwrite with identical data once it starts)
    utils::write_pid_file(mount_point, name, pid)?;

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
    use crate::diagnostics;
    use crate::investigation::{
        DiagnosticsPreset, InvestigationStore, NewTimelineEntry, ProfilingCaptureMode,
        ProfilingWindowStatus, TimelineEntryKind, TimelineEntrySource,
        lock_investigation_test_mutex, set_test_store_base_dir,
    };
    #[cfg(windows)]
    use std::collections::HashSet;
    use std::io;
    #[cfg(windows)]
    use std::path::Path;
    #[cfg(windows)]
    use std::sync::OnceLock;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    use studiofs_meta::{ROOT_INO, RedbMetaEngine};

    type BackendInspect = Arc<dyn Fn(&BackendLaunch) -> io::Result<()> + Send + Sync>;

    #[derive(Clone)]
    struct FakeBackend {
        events: Arc<Mutex<Vec<String>>>,
        fail: bool,
        inspect: Option<BackendInspect>,
    }

    impl MountBackend for FakeBackend {
        fn name(&self) -> &'static str {
            "FakeBackend"
        }

        fn serve(&self, launch: BackendLaunch) -> io::Result<()> {
            self.events.lock().expect("lock events").push(format!(
                "{}:{}:{}",
                launch.mount_point, launch.volume_name, launch.read_only
            ));
            if let Some(inspect) = &self.inspect {
                inspect(&launch)?;
            }
            if self.fail {
                Err(io::Error::other("boom"))
            } else {
                Ok(())
            }
        }
    }

    fn seed_mount_meta_db(
        temp: &TempDir,
        name: &str,
    ) -> (PathBuf, Arc<RedbMetaEngine>, FormatInfo) {
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let db_path = meta_dir.join(format!("{name}.db"));
        let engine = Arc::new(RedbMetaEngine::open(&db_path).expect("open redb"));
        let format_info = FormatInfo::new(
            name.to_string(),
            format!("{name}-uuid"),
            studiofs_meta::FormatInfoDataStore {
                endpoint: "https://storage.example".to_string(),
                bucket: "bucket-demo".to_string(),
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                region: String::new(),
            },
            4,
        );
        engine
            .set_format_info(&format_info)
            .expect("set format info");
        engine.init_root(0, 0).expect("init root");
        (meta_dir, engine, format_info)
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
    fn startup_mount_point(temp: &TempDir) -> String {
        temp.path().join("mnt").to_string_lossy().into_owned()
    }

    #[cfg(windows)]
    fn windows_test_mount_claims() -> &'static Mutex<HashSet<String>> {
        static CLAIMS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
        CLAIMS.get_or_init(|| Mutex::new(HashSet::new()))
    }

    #[cfg(windows)]
    fn windows_test_mount_point_is_available(mount_point: &str) -> bool {
        let drive_root = format!("{mount_point}\\");
        if Path::new(&drive_root).exists() {
            return false;
        }

        !matches!(
            utils::read_pid_file(mount_point),
            Ok((pid, _, _)) if pid != std::process::id() && utils::is_process_running(pid)
        )
    }

    #[cfg(windows)]
    fn reserve_windows_test_mount_point(excluded: &[&str]) -> String {
        let mut claims = windows_test_mount_claims()
            .lock()
            .expect("lock claimed test mount points");
        for letter in (b'D'..=b'Z').rev() {
            let mount_point = format!("{}:", char::from(letter));
            if claims.contains(&mount_point)
                || excluded.iter().any(|excluded| *excluded == mount_point)
            {
                continue;
            }
            if windows_test_mount_point_is_available(&mount_point) {
                claims.insert(mount_point.clone());
                return mount_point;
            }
        }
        panic!("no free Windows drive letters available for mount-session tests");
    }

    #[cfg(windows)]
    fn startup_mount_point(_: &TempDir) -> String {
        reserve_windows_test_mount_point(&[])
    }

    fn mount_command(
        name: &str,
        meta_dir: &Path,
        mount_point: Option<String>,
        cache_dir: Option<PathBuf>,
        cache_size: Option<&str>,
    ) -> MountCommand {
        MountCommand {
            name: name.to_string(),
            meta_dir: meta_dir.to_path_buf(),
            mount_point,
            cache_dir,
            cache_size: cache_size.map(str::to_string),
            principal: None,
            foreground: false,
            detach: false,
            profile: None,
            snapshot_id: None,
            endpoint: None,
            runtime_tuning: PublicRuntimeTuningArgs::default(),
        }
    }

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
    fn prepare_volume_cache_runtime_scopes_layout_by_volume_uuid() {
        let temp = tempfile::tempdir().expect("tempdir");
        let prepared =
            prepare_volume_cache_runtime(temp.path(), "volume-uuid").expect("cache path prepared");

        assert_eq!(prepared.cache_path, temp.path().join("volume-uuid"));
        assert_eq!(
            prepared.cache_path_str,
            prepared.cache_path.to_string_lossy()
        );
        assert_eq!(
            prepared.cache_runtime.layout,
            LocalBlockLayout::from_cache_root(&prepared.cache_path)
        );
        assert!(prepared.cache_runtime.detection.has_runtime_layout());
    }

    #[test]
    fn mount_plan_defaults_to_meta_cache_and_foreground_launch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let cli_mount_point = startup_mount_point(&temp);
        let settings_config = startup_settings_config(cli_mount_point.clone(), None, Some("64GB"));
        let mut command = mount_command("projectfs", &meta_dir, Some(cli_mount_point), None, None);
        command.principal = Some("alice".to_string());
        command.endpoint = Some("127.0.0.1:9000".to_string());

        let plan = MountPlan::resolve(MountRequest {
            command,
            settings_config,
        })
        .expect("mount plan resolved");

        assert_eq!(plan.base_cache_path, meta_dir.join("cache"));
        assert_eq!(plan.cache_size, "64GB");
        assert_eq!(plan.launch_mode, MountLaunchMode::Foreground);
        assert!(plan.run_in_foreground);
        assert!(!plan.read_only_mount);
        assert_eq!(plan.vfs_config.principal, "alice");
    }

    #[test]
    fn mount_plan_prefers_cli_runtime_settings_over_defaults() {
        let temp = tempfile::tempdir().expect("tempdir");
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let configured_cache_dir = temp.path().join("configured-cache");
        let cli_cache_dir = temp.path().join("cli-cache");
        let settings_mount_point = startup_mount_point(&temp);
        #[cfg(not(windows))]
        let cli_mount_point = temp.path().join("cli-mnt").to_string_lossy().into_owned();
        #[cfg(windows)]
        let cli_mount_point = reserve_windows_test_mount_point(&[settings_mount_point.as_str()]);

        let plan = MountPlan::resolve(MountRequest {
            command: mount_command(
                "projectfs",
                &meta_dir,
                Some(cli_mount_point),
                Some(cli_cache_dir.clone()),
                Some("96GB"),
            ),
            settings_config: startup_settings_config(
                settings_mount_point,
                Some(configured_cache_dir),
                Some("32GB"),
            ),
        })
        .expect("mount plan resolved");

        assert_eq!(plan.base_cache_path, cli_cache_dir);
        assert_eq!(plan.cache_size, "96GB");
        assert_eq!(plan.launch_mode, MountLaunchMode::Foreground);
        assert!(plan.run_in_foreground);
    }

    #[test]
    fn build_mount_config_prefers_public_runtime_tuning_over_env_and_settings() {
        let resolved = build_mount_config_with(
            VfsConfigRaw {
                read_buffer_mb: Some(1024),
                readahead_width: Some(64),
                prefetch_concurrency: Some(32),
                ..VfsConfigRaw::default()
            },
            None,
            &PublicRuntimeTuningArgs {
                read_buffer: Some("4GB".to_string()),
                readahead: Some(256),
                prefetch: Some(192),
            },
            |mut config| {
                config.read_buffer_mb = Some(2048);
                config.readahead_width = Some(128);
                config.prefetch_concurrency = Some(96);
                config
            },
        )
        .expect("mount config resolved");

        assert_eq!(resolved.vfs_config.read_buffer_mb, 4096);
        assert_eq!(resolved.vfs_config.readahead_width, 256);
        assert_eq!(resolved.vfs_config.prefetch_concurrency, 192);
    }

    #[test]
    fn build_mount_config_ignores_removed_upload_delay_inputs() {
        let resolved = build_mount_config_with(
            VfsConfigRaw {
                upload_delay_ms: Some(250),
                ..VfsConfigRaw::default()
            },
            None,
            &PublicRuntimeTuningArgs::default(),
            |mut config| {
                config.upload_delay_ms = Some(1000);
                config
            },
        )
        .expect("mount config resolved");

        assert_eq!(
            resolved.vfs_config.upload_delay_ms,
            VfsConfig::default().upload_delay_ms
        );
    }

    #[test]
    fn overlay_vfs_env_ignores_upload_delay_env_override() {
        let resolved = overlay_vfs_env_with(VfsConfigRaw::default(), |key| match key {
            "STUDIOFS_UPLOAD_DELAY_MS" => Ok("1500".to_string()),
            _ => Err(std::env::VarError::NotPresent),
        });

        assert_eq!(resolved.upload_delay_ms, None);
    }

    #[test]
    fn mount_plan_rejects_invalid_endpoint() {
        let temp = tempfile::tempdir().expect("tempdir");
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let cli_mount_point = startup_mount_point(&temp);
        let mut command = mount_command("projectfs", &meta_dir, Some(cli_mount_point), None, None);
        command.endpoint = Some("ftp://127.0.0.1:9000".to_string());

        let err = MountPlan::resolve(MountRequest {
            command,
            settings_config: startup_settings_config(
                startup_mount_point(&temp),
                None,
                Some("64GB"),
            ),
        })
        .expect_err("invalid endpoint should fail");

        assert!(err.to_string().contains("invalid S3 endpoint"));
    }

    #[test]
    fn mount_plan_marks_snapshot_mount_read_only() {
        let temp = tempfile::tempdir().expect("tempdir");
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let mut command = mount_command(
            "projectfs",
            &meta_dir,
            Some(startup_mount_point(&temp)),
            None,
            Some("64GB"),
        );
        command.snapshot_id = Some("snapshot-123".to_string());

        let plan = MountPlan::resolve(MountRequest {
            command,
            settings_config: startup_settings_config(
                startup_mount_point(&temp),
                None,
                Some("64GB"),
            ),
        })
        .expect("mount plan resolved");

        assert!(plan.read_only_mount);
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
        assert_eq!(
            session.meta.get_format_info().expect("meta info").uuid,
            format_info.uuid
        );
    }

    #[test]
    fn mount_meta_session_materializes_snapshot_engine_and_marks_read_only() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _format_info) = seed_mount_meta_db(&temp, "demo");
        let snapshot = engine.create_snapshot(ROOT_INO).expect("create snapshot");
        drop(engine);

        let session = resolve_mount_meta_session("demo", &meta_dir, Some(&snapshot.id), None)
            .expect("resolve snapshot meta");

        assert!(session.read_only_mount);
        assert_eq!(
            session.format_info.metadata.backend,
            MetadataBackend::RedbLocal
        );
        assert!(session.format_info.metadata.postgres.is_none());
        let expected_snapshot_db_path = meta_dir
            .join("snapshots")
            .join(format!("demo.{}.db", snapshot.id));
        assert!(expected_snapshot_db_path.exists());
        assert_eq!(
            session.snapshot_db_path.as_deref(),
            Some(expected_snapshot_db_path.as_path())
        );
    }

    #[test]
    fn mount_meta_session_applies_endpoint_override_to_runtime_format_info() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _) = seed_mount_meta_db(&temp, "demo");
        drop(engine);

        let session =
            resolve_mount_meta_session("demo", &meta_dir, None, Some("http://127.0.0.1:9000"))
                .expect("resolve with endpoint override");

        assert_eq!(session.format_info.endpoint, "http://127.0.0.1:9000");
    }

    #[test]
    fn default_mount_backend_matches_platform() {
        let backend = default_mount_backend();
        #[cfg(unix)]
        assert_eq!(backend.name(), "FUSE");
        #[cfg(windows)]
        assert_eq!(backend.name(), "WinFSP");
    }

    #[test]
    fn mount_plan_prepare_marks_snapshot_mount_read_only_and_uses_fake_backend() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _) = seed_mount_meta_db(&temp, "demo");
        let snapshot = engine.create_snapshot(ROOT_INO).expect("create snapshot");
        drop(engine);
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut command = mount_command(
            "demo",
            &meta_dir,
            Some(startup_mount_point(&temp)),
            Some(temp.path().join("cache")),
            Some("64GB"),
        );
        command.snapshot_id = Some(snapshot.id.clone());

        let prepared = MountPlan::resolve(MountRequest {
            command,
            settings_config: startup_settings_config(
                startup_mount_point(&temp),
                None,
                Some("64GB"),
            ),
        })
        .expect("mount plan resolved")
        .prepare(|| {
            Box::new(FakeBackend {
                events: events.clone(),
                fail: false,
                inspect: None,
            })
        })
        .expect("prepare mount");

        assert!(prepared.plan.read_only_mount);
        assert_eq!(
            prepared
                .format_info
                .as_ref()
                .expect("format info")
                .metadata
                .backend,
            MetadataBackend::RedbLocal
        );
        assert_eq!(
            prepared
                .foreground
                .as_ref()
                .expect("prepared runtime")
                .backend
                .name(),
            "FakeBackend"
        );
        assert!(prepared.snapshot_mount_db_path.is_some());
    }

    #[test]
    fn mount_plan_prepare_daemon_skips_foreground_runtime() {
        let temp = tempfile::tempdir().expect("tempdir");
        let meta_dir = temp.path().join("meta");
        std::fs::create_dir_all(&meta_dir).expect("create meta dir");
        let mut command = mount_command(
            "demo",
            &meta_dir,
            Some(startup_mount_point(&temp)),
            None,
            Some("64GB"),
        );
        command.detach = true;

        let prepared = MountPlan::resolve(MountRequest {
            command,
            settings_config: startup_settings_config(
                startup_mount_point(&temp),
                None,
                Some("64GB"),
            ),
        })
        .expect("mount plan resolved")
        .prepare(|| {
            Box::new(FakeBackend {
                events: Arc::new(Mutex::new(Vec::new())),
                fail: false,
                inspect: None,
            })
        })
        .expect("prepare daemon mount");

        assert!(matches!(prepared.plan.launch_mode, MountLaunchMode::Daemon));
        assert!(prepared.foreground.is_none());
        assert!(prepared.format_info.is_none());
    }

    #[test]
    fn prepared_mount_launch_success_invokes_backend_and_removes_pid_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _) = seed_mount_meta_db(&temp, "demo");
        drop(engine);
        let events = Arc::new(Mutex::new(Vec::new()));
        let mount_point = startup_mount_point(&temp);
        let command = mount_command(
            "demo",
            &meta_dir,
            Some(mount_point.clone()),
            Some(temp.path().join("cache")),
            Some("64GB"),
        );
        let prepared = MountPlan::resolve(MountRequest {
            command,
            settings_config: startup_settings_config(mount_point.clone(), None, Some("64GB")),
        })
        .expect("mount plan resolved")
        .prepare(|| {
            Box::new(FakeBackend {
                events: events.clone(),
                fail: false,
                inspect: None,
            })
        })
        .expect("prepare mount");
        let actual_mount_path = prepared.plan.actual_mount_path.clone();

        prepared.launch().expect("launch mount");

        let observed = events.lock().expect("lock events").clone();
        assert_eq!(observed.len(), 1);
        assert!(observed[0].contains("demo"));
        assert!(
            utils::read_pid_file(&actual_mount_path).is_err(),
            "successful launch should remove pid file"
        );
    }

    #[test]
    fn prepared_mount_launch_failure_removes_pid_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _) = seed_mount_meta_db(&temp, "demo");
        drop(engine);
        let events = Arc::new(Mutex::new(Vec::new()));
        let mount_point = startup_mount_point(&temp);
        let prepared = MountPlan::resolve(MountRequest {
            command: mount_command(
                "demo",
                &meta_dir,
                Some(mount_point.clone()),
                Some(temp.path().join("cache")),
                Some("64GB"),
            ),
            settings_config: startup_settings_config(mount_point.clone(), None, Some("64GB")),
        })
        .expect("mount plan resolved")
        .prepare(|| {
            Box::new(FakeBackend {
                events: events.clone(),
                fail: true,
                inspect: None,
            })
        })
        .expect("prepare mount");
        let actual_mount_path = prepared.plan.actual_mount_path.clone();

        let err = prepared.launch().expect_err("backend error should surface");

        assert!(err.to_string().contains("FakeBackend error: boom"));
        assert_eq!(events.lock().expect("lock events").len(), 1);
        assert!(
            utils::read_pid_file(&actual_mount_path).is_err(),
            "backend failure should remove pid file"
        );
    }

    #[test]
    fn guard_mount_pid_file_with_keeps_current_process_pid_file() {
        let removed = Arc::new(Mutex::new(Vec::new()));
        guard_mount_pid_file_with(
            "/tmp/demo",
            4242,
            |_| Ok((4242, "demo".to_string(), String::new())),
            {
                let removed = removed.clone();
                move |path| {
                    removed.lock().expect("lock removed").push(path.to_string());
                    Ok(())
                }
            },
            |_| true,
        )
        .expect("current pid should be accepted");

        assert!(
            removed.lock().expect("lock removed").is_empty(),
            "current pid entry should not be removed"
        );
    }

    #[test]
    fn prepared_mount_launch_cleans_up_snapshot_mount_db_on_backend_failure() {
        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _) = seed_mount_meta_db(&temp, "demo");
        let snapshot = engine.create_snapshot(ROOT_INO).expect("create snapshot");
        drop(engine);
        let events = Arc::new(Mutex::new(Vec::new()));
        let mount_point = startup_mount_point(&temp);
        let mut command = mount_command(
            "demo",
            &meta_dir,
            Some(mount_point.clone()),
            Some(temp.path().join("cache")),
            Some("64GB"),
        );
        command.snapshot_id = Some(snapshot.id.clone());

        let prepared = MountPlan::resolve(MountRequest {
            command,
            settings_config: startup_settings_config(mount_point.clone(), None, Some("64GB")),
        })
        .expect("mount plan resolved")
        .prepare(|| {
            Box::new(FakeBackend {
                events: events.clone(),
                fail: true,
                inspect: None,
            })
        })
        .expect("prepare mount");
        let snapshot_db_path = prepared
            .snapshot_mount_db_path
            .clone()
            .expect("snapshot db path");
        assert!(
            snapshot_db_path.exists(),
            "prepare should materialize snapshot db"
        );

        let err = prepared.launch().expect_err("backend error should surface");

        assert!(err.to_string().contains("FakeBackend error: boom"));
        assert!(
            !snapshot_db_path.exists(),
            "launch should remove snapshot db after backend return"
        );
    }

    #[test]
    fn prepared_mount_launch_publishes_live_diagnostics_and_supports_attach() {
        let _guard = diagnostics::lock_diagnostics_test_mutex();
        let _investigation_guard = lock_investigation_test_mutex();
        let temp = tempfile::tempdir().expect("tempdir");
        let store_dir = temp.path().join("investigations-store");
        set_test_store_base_dir(Some(&store_dir));
        struct ResetInvestigationStoreDir;
        impl Drop for ResetInvestigationStoreDir {
            fn drop(&mut self) {
                set_test_store_base_dir(None);
            }
        }
        let _reset_store_dir = ResetInvestigationStoreDir;
        let (meta_dir, engine, _) = seed_mount_meta_db(&temp, "demo");
        drop(engine);
        let events = Arc::new(Mutex::new(Vec::new()));
        let persisted_run_id = Arc::new(Mutex::new(None::<String>));
        #[cfg(windows)]
        let mount_point = reserve_windows_test_mount_point(&[]);
        #[cfg(not(windows))]
        let mount_point = temp
            .path()
            .join("diag-mount")
            .to_string_lossy()
            .into_owned();
        let persisted_run_id_for_inspect = persisted_run_id.clone();
        let inspect: BackendInspect = Arc::new(move |launch| {
            let discovery = diagnostics::discover_local_sessions()
                .expect("discover local sessions")
                .into_iter()
                .find(|session| session.mount_point == launch.mount_point)
                .expect("live diagnostics session for mounted volume");
            let client = diagnostics::LocalDiagnosticsClient::new(discovery.clone());
            let session = launch
                .runtime_handle
                .block_on(client.session_info())
                .expect("read session identity");
            assert_eq!(session.volume, "demo");
            assert_eq!(session.bridge, "FakeBackend");
            assert_eq!(session.mount_point, launch.mount_point);
            assert_eq!(session.pid, std::process::id());
            assert!(!session.read_only);

            let health = launch
                .runtime_handle
                .block_on(client.health())
                .expect("read session health");
            assert!(health.generated_unix_ms > 0);
            assert!(health.write_path.generated_unix_ms > 0);

            let runtime_snapshot = launch
                .runtime_handle
                .block_on(client.runtime_snapshot())
                .expect("read runtime snapshot");
            assert_eq!(runtime_snapshot.schema_version, 2);
            assert_eq!(
                runtime_snapshot.upload.pending_uploads,
                health.upload.pending_uploads
            );
            assert!(
                runtime_snapshot.cache.state.transit_memory.budget_bytes > 0,
                "runtime snapshot should include cache-state transit budget"
            );

            let recent_events = launch
                .runtime_handle
                .block_on(client.recent_events(Some(8)))
                .expect("read recent runtime events");
            assert!(
                recent_events.len() <= 8,
                "recent events must respect the requested bound"
            );

            let first_lease = launch
                .runtime_handle
                .block_on(client.acquire_controller("test-client-a", Duration::from_millis(100)))
                .expect("acquire first controller lease");
            assert_eq!(first_lease.client_name, "test-client-a");

            let competing_client = diagnostics::LocalDiagnosticsClient::new(discovery);
            let conflict = launch
                .runtime_handle
                .block_on(
                    competing_client
                        .try_acquire_controller("test-client-b", Duration::from_millis(100)),
                )
                .expect_err("second controller should be rejected before expiry");
            assert!(
                conflict.code == "controller_busy",
                "unexpected controller conflict: {conflict:#}"
            );

            let takeover_deadline = std::time::Instant::now() + Duration::from_secs(3);
            let takeover = loop {
                match launch.runtime_handle.block_on(
                    competing_client
                        .try_acquire_controller("test-client-b", Duration::from_millis(100)),
                ) {
                    Ok(lease) => break lease,
                    Err(error)
                        if error.code == "controller_busy"
                            && std::time::Instant::now() < takeover_deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!(
                        "controller should become available after expiry, last error: {error:#}"
                    ),
                }
            };
            let renewed = launch
                .runtime_handle
                .block_on(
                    competing_client.renew_controller(&takeover.lease_id, Duration::from_secs(2)),
                )
                .expect("renew controller lease");
            assert_eq!(renewed.lease_id, takeover.lease_id);
            assert!(
                renewed.expires_unix_ms >= takeover.expires_unix_ms,
                "renewal should extend or preserve expiry"
            );

            let case = launch
                .runtime_handle
                .block_on(competing_client.create_case(
                    &renewed.lease_id,
                    "AJA copy slowdown",
                    Some("Copy stalls after warmup".to_string()),
                    Some("aja".to_string()),
                    Some("artist workstation".to_string()),
                ))
                .expect("create investigation case");
            let run = launch
                .runtime_handle
                .block_on(competing_client.create_run(
                    &renewed.lease_id,
                    &case.id,
                    Some("copy-pass-1".to_string()),
                ))
                .expect("create investigation run");
            let safe_action = launch
                .runtime_handle
                .block_on(competing_client.invoke_safe_action(
                    &renewed.lease_id,
                    &run.id,
                    diagnostics::SafeDebugActionRequest::LightweightSelfCheck {
                        actor: Some("studiofs-dev-mcp".to_string()),
                        reason: Some("verify live action surface".to_string()),
                    },
                ))
                .expect("invoke safe debug action");
            assert!(safe_action.loud);
            assert_eq!(
                safe_action.action,
                diagnostics::SafeDebugActionKind::LightweightSelfCheck
            );
            let profiling_status = launch
                .runtime_handle
                .block_on(competing_client.profiling_status())
                .expect("read profiling status");
            assert_eq!(
                profiling_status.max_duration_ms,
                diagnostics::MAX_PROFILE_WINDOW_DURATION_MS
            );
            assert!(
                profiling_status
                    .supported_presets
                    .contains(&DiagnosticsPreset::Balanced)
            );
            let profiling_window = launch
                .runtime_handle
                .block_on(competing_client.start_profiling_window(
                    &renewed.lease_id,
                    &run.id,
                    diagnostics::StartProfilingWindowRequest {
                        capture_mode: ProfilingCaptureMode::WallBlocking,
                        preset: DiagnosticsPreset::Balanced,
                        budget: diagnostics::ProfilingBudget {
                            duration_ms: 500,
                            sample_rate_hz: Some(99),
                        },
                    },
                ))
                .expect("start profiling window");
            let final_window = if profiling_window.status == ProfilingWindowStatus::Running {
                launch
                    .runtime_handle
                    .block_on(
                        competing_client
                            .stop_profiling_window(&renewed.lease_id, &profiling_window.window_id),
                    )
                    .expect("stop profiling window")
            } else {
                profiling_window
            };
            assert_eq!(final_window.status, ProfilingWindowStatus::Completed);
            assert_eq!(
                final_window
                    .artifact
                    .as_ref()
                    .map(|artifact| artifact.kind.as_str()),
                Some("wall_blocking_summary")
            );
            launch
                .runtime_handle
                .block_on(competing_client.append_timeline_entry(
                    &renewed.lease_id,
                    &run.id,
                    NewTimelineEntry {
                        source: TimelineEntrySource::Cli,
                        kind: TimelineEntryKind::Marker,
                        title: "start copy".to_string(),
                        body: Some("Started manual copy flow".to_string()),
                        actor: Some("developer".to_string()),
                        reason: Some("begin workload".to_string()),
                    },
                ))
                .expect("append cli marker");
            launch
                .runtime_handle
                .block_on(competing_client.append_timeline_entry(
                    &renewed.lease_id,
                    &run.id,
                    NewTimelineEntry {
                        source: TimelineEntrySource::Mcp,
                        kind: TimelineEntryKind::Anomaly,
                        title: "cache miss burst".to_string(),
                        body: Some("Observed a foreground-read spike".to_string()),
                        actor: Some("studiofs-dev-mcp".to_string()),
                        reason: Some("operator observation".to_string()),
                    },
                ))
                .expect("append mcp note");
            *persisted_run_id_for_inspect
                .lock()
                .expect("lock persisted run id") = Some(run.id);
            Ok(())
        });

        let prepared = MountPlan::resolve(MountRequest {
            command: mount_command(
                "demo",
                &meta_dir,
                Some(mount_point.clone()),
                Some(temp.path().join("cache")),
                Some("64GB"),
            ),
            settings_config: startup_settings_config(mount_point.clone(), None, Some("64GB")),
        })
        .expect("mount plan resolved")
        .prepare(|| {
            Box::new(FakeBackend {
                events: events.clone(),
                fail: false,
                inspect: Some(inspect.clone()),
            })
        })
        .expect("prepare mount");
        let actual_mount_path = prepared.plan.actual_mount_path.clone();

        prepared.launch().expect("launch mount");
        assert_eq!(events.lock().expect("lock events").len(), 1);

        let run_id = persisted_run_id
            .lock()
            .expect("lock persisted run id")
            .clone()
            .expect("persisted run id");
        let persisted_run = InvestigationStore::new(store_dir)
            .load_run(&run_id)
            .expect("load run after shutdown");
        assert!(!persisted_run.session_id.is_empty());
        assert_eq!(persisted_run.fingerprint.bridge, "FakeBackend");
        assert_eq!(
            persisted_run.fingerprint.runtime_configuration.mount_point,
            actual_mount_path
        );
        assert_eq!(persisted_run.timeline.len(), 3);
        assert_eq!(persisted_run.timeline[0].kind, TimelineEntryKind::Action);
        assert_eq!(persisted_run.timeline[0].source, TimelineEntrySource::Mcp);
        assert_eq!(persisted_run.timeline[1].source, TimelineEntrySource::Cli);
        assert_eq!(persisted_run.timeline[2].source, TimelineEntrySource::Mcp);
        assert_eq!(persisted_run.profiling_windows.len(), 1);
        assert_eq!(
            persisted_run.profiling_windows[0].status,
            ProfilingWindowStatus::Completed
        );
        assert_eq!(
            persisted_run.profiling_windows[0]
                .artifact
                .as_ref()
                .map(|artifact| artifact.kind.as_str()),
            Some("wall_blocking_summary")
        );
    }

    #[test]
    fn prepared_mount_launch_fails_open_when_diagnostics_start_fails() {
        let _guard = diagnostics::lock_diagnostics_test_mutex();
        diagnostics::set_test_start_failure(Some("synthetic diagnostics startup failure"));
        struct ResetDiagnosticsFailure;
        impl Drop for ResetDiagnosticsFailure {
            fn drop(&mut self) {
                diagnostics::set_test_start_failure(None);
            }
        }
        let _reset = ResetDiagnosticsFailure;

        let temp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, engine, _) = seed_mount_meta_db(&temp, "demo");
        drop(engine);
        let events = Arc::new(Mutex::new(Vec::new()));
        #[cfg(windows)]
        let mount_point = reserve_windows_test_mount_point(&[]);
        #[cfg(not(windows))]
        let mount_point = temp
            .path()
            .join("diag-fail-open")
            .to_string_lossy()
            .into_owned();
        let prepared = MountPlan::resolve(MountRequest {
            command: mount_command(
                "demo",
                &meta_dir,
                Some(mount_point.clone()),
                Some(temp.path().join("cache")),
                Some("64GB"),
            ),
            settings_config: startup_settings_config(mount_point.clone(), None, Some("64GB")),
        })
        .expect("mount plan resolved")
        .prepare(|| {
            Box::new(FakeBackend {
                events: events.clone(),
                fail: false,
                inspect: None,
            })
        })
        .expect("prepare mount");

        prepared
            .launch()
            .expect("mount launch should survive diagnostics startup failure");
        assert_eq!(events.lock().expect("lock events").len(), 1);
    }
}

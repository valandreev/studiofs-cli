use std::path::{Path, PathBuf};

use studiofs_cache::{PreparedCacheV3Runtime, prepare_cache_v3_runtime};
use studiofs_vfs::{VfsConfigValidated, parse_size_bytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MountLaunchMode {
    Foreground,
    Daemon,
}

pub(crate) struct ResolvedMountConfig {
    pub(crate) vfs_config: VfsConfigValidated,
    pub(crate) cache_mode: String,
}

#[derive(Debug)]
pub(crate) struct ResolvedMountStartup {
    pub(crate) actual_mount_path: String,
    pub(crate) base_cache_path: PathBuf,
    pub(crate) cache_size: String,
    pub(crate) cache_mode: String,
    pub(crate) vfs_config: VfsConfigValidated,
    pub(crate) principal_to_export: Option<String>,
    pub(crate) run_in_foreground: bool,
    pub(crate) launch_mode: MountLaunchMode,
}

#[derive(Debug)]
pub(crate) struct PreparedMountRuntimePlan {
    pub(crate) actual_mount_path: String,
    pub(crate) cache_path: PathBuf,
    pub(crate) cache_path_str: String,
    pub(crate) cache_size: String,
    pub(crate) cache_size_bytes: u64,
    pub(crate) cache_mode: String,
    pub(crate) cache_runtime: PreparedCacheV3Runtime,
    pub(crate) vfs_config: VfsConfigValidated,
    pub(crate) principal_to_export: Option<String>,
    pub(crate) run_in_foreground: bool,
    pub(crate) launch_mode: MountLaunchMode,
}

#[derive(Debug)]
pub(crate) struct PreparedVolumeCacheRuntime {
    pub(crate) cache_path: PathBuf,
    pub(crate) cache_path_str: String,
    pub(crate) cache_runtime: PreparedCacheV3Runtime,
}

fn parse_positive_size_arg(raw: &str) -> anyhow::Result<u64> {
    parse_size_bytes(raw)
        .filter(|&bytes| bytes > 0)
        .ok_or_else(|| anyhow::anyhow!("invalid size '{}' (must be > 0)", raw))
}

pub(crate) fn prepare_volume_cache_runtime(
    cache_dir: &Path,
    volume_uuid: &str,
) -> anyhow::Result<PreparedVolumeCacheRuntime> {
    std::fs::create_dir_all(cache_dir)?;
    let cache_path = cache_dir.join(volume_uuid);
    let cache_runtime = prepare_cache_v3_runtime(&cache_path).map_err(|source| {
        anyhow::anyhow!(
            "failed to prepare cache-v3 runtime at '{}': {source}",
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

pub(crate) fn prepare_mount_runtime_plan(
    startup: ResolvedMountStartup,
    volume_uuid: &str,
) -> anyhow::Result<PreparedMountRuntimePlan> {
    let PreparedVolumeCacheRuntime {
        cache_path,
        cache_path_str,
        cache_runtime,
    } = prepare_volume_cache_runtime(&startup.base_cache_path, volume_uuid)?;
    let cache_size_bytes = parse_positive_size_arg(&startup.cache_size)?;

    Ok(PreparedMountRuntimePlan {
        actual_mount_path: startup.actual_mount_path,
        cache_path,
        cache_path_str,
        cache_size: startup.cache_size,
        cache_size_bytes,
        cache_mode: startup.cache_mode,
        cache_runtime,
        vfs_config: startup.vfs_config,
        principal_to_export: startup.principal_to_export,
        run_in_foreground: startup.run_in_foreground,
        launch_mode: startup.launch_mode,
    })
}

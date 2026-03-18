//! `studiofs settings` — configure global defaults for mounts.

use std::path::{Path, PathBuf};

use clap::Args;
use serde::{Deserialize, Serialize};
use studiofs_vfs::VfsConfig;

const CONFIG_FILE_NAME: &str = "config.toml";
const DEFAULT_CACHE_SIZE: &str = "100GB";
const DEFAULT_MEMORY_LIMIT: &str = "2GB";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StudioFsConfig {
    pub mount_point: Option<String>,
    pub cache_dir: Option<String>,
    pub memory_limit: Option<String>,
    pub cache_size: Option<String>,
    #[serde(default)]
    pub vfs: VfsConfig,
}

#[derive(Debug, Args, Default)]
pub struct SettingsArgs {
    /// Default mount point used by `studiofs mount` when omitted.
    #[arg(long)]
    pub mount_point: Option<String>,

    /// Default cache directory used by `studiofs mount` when `--cache-dir` is omitted.
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,

    /// Default max memory cache size used by `studiofs mount` when omitted.
    #[arg(long)]
    pub memory_limit: Option<String>,

    /// Default max disk cache size used by `studiofs mount` when omitted.
    #[arg(long)]
    pub cache_size: Option<String>,

    /// Default slab flush interval used by `studiofs mount`.
    #[arg(long = "slab-flush-interval")]
    pub slab_flush_interval_ms: Option<u64>,

    /// Disable the background slab flusher.
    #[arg(long = "disable-bg-slab-flush")]
    pub disable_bg_slab_flush: bool,

    /// Enable the background slab flusher.
    #[arg(
        long = "enable-bg-slab-flush",
        overrides_with = "disable_bg_slab_flush"
    )]
    pub enable_bg_slab_flush: bool,
}

fn config_file_path() -> anyhow::Result<PathBuf> {
    let project_dirs = directories::ProjectDirs::from("", "", "studiofs")
        .ok_or_else(|| anyhow::anyhow!("could not resolve user config directory"))?;

    Ok(project_dirs.config_dir().to_path_buf())
}

fn config_toml_path() -> anyhow::Result<PathBuf> {
    Ok(config_file_path()?.join(CONFIG_FILE_NAME))
}

pub fn load_studiofs_config() -> anyhow::Result<StudioFsConfig> {
    let path = config_toml_path()?;
    if !path.exists() {
        return Ok(StudioFsConfig::default());
    }

    let bytes = std::fs::read_to_string(&path).map_err(|e| {
        anyhow::anyhow!(
            "failed to read config file '{}': {e}",
            path.to_string_lossy()
        )
    })?;

    toml::from_str(&bytes)
        .map_err(|e| anyhow::anyhow!("failed to parse config file '{}': {e}", path.display()))
}

fn save_studiofs_config(config: &StudioFsConfig) -> anyhow::Result<()> {
    let path = config_toml_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let content = toml::to_string_pretty(config).map_err(|e| {
        anyhow::anyhow!("failed to serialize config file '{}': {e}", path.display())
    })?;
    std::fs::write(&path, content)?;
    Ok(())
}

#[cfg(windows)]
fn ensure_mount_point_exists(mount_point: &str) -> anyhow::Result<()> {
    if super::utils::is_drive_root(mount_point) {
        return Ok(());
    }

    anyhow::bail!(
        "Setting a mount-point directory is not supported on Windows.\n\
         Specify a drive letter (for example, Z:) when mounting."
    )
}

#[cfg(not(windows))]
fn ensure_mount_point_exists(mount_point: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(mount_point)?;
    Ok(())
}

pub fn resolve_studiofs_config_for_mount(
    cli_mount_point: Option<&str>,
    cli_cache_dir: Option<&Path>,
    cli_memory_limit: Option<&str>,
    cli_cache_size: Option<&str>,
) -> anyhow::Result<(String, Option<PathBuf>, String, String)> {
    let config = load_studiofs_config()?;

    let mount_point = cli_mount_point
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or({
            #[cfg(not(windows))]
            {
                config
                    .mount_point
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
            }
            #[cfg(windows)]
            {
                None
            }
        })
        .map(|s| s.to_string())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No mount point provided.\n\
                 On Windows, you must pass a drive letter explicitly, for example `Z:`."
            )
        })?;

    #[cfg(windows)]
    if !super::utils::is_drive_root(&mount_point) {
        anyhow::bail!(
            "Invalid mount point '{}': Windows requires a drive letter mount (for example `Z:`).",
            mount_point
        );
    }

    let cache_dir = cli_cache_dir
        .map(Path::to_path_buf)
        .or_else(|| {
            config
                .cache_dir
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(PathBuf::from)
        })
        .filter(|path| !path.as_os_str().is_empty());

    let memory_limit = cli_memory_limit
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            config
                .memory_limit
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .unwrap_or(DEFAULT_MEMORY_LIMIT)
        .to_string();

    let cache_size = cli_cache_size
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            config
                .cache_size
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .unwrap_or(DEFAULT_CACHE_SIZE)
        .to_string();

    Ok((mount_point, cache_dir, memory_limit, cache_size))
}

pub fn run(args: SettingsArgs) -> anyhow::Result<()> {
    let mut config = load_studiofs_config()?;
    let mut changed = false;

    if let Some(mount_point) = args.mount_point {
        let mount_point = mount_point.trim().to_string();
        if let Err(err) = ensure_mount_point_exists(&mount_point) {
            if cfg!(windows) {
                println!("  (warning) ignoring --mount-point on Windows: {err}");
            } else {
                return Err(err);
            }
        } else {
            config.mount_point = Some(mount_point);
            changed = true;
        }
    }

    if let Some(cache_dir) = args.cache_dir {
        std::fs::create_dir_all(&cache_dir)?;
        let cache_dir = cache_dir
            .into_os_string()
            .into_string()
            .map_err(|_| anyhow::anyhow!("cache-dir must be valid UTF-8 to be stored in config"))?;
        config.cache_dir = Some(cache_dir);
        changed = true;
    }

    if let Some(memory_limit) = args.memory_limit {
        let memory_limit = memory_limit.trim().to_string();
        if !memory_limit.is_empty() {
            config.memory_limit = Some(memory_limit);
            changed = true;
        }
    }

    if let Some(cache_size) = args.cache_size {
        let cache_size = cache_size.trim().to_string();
        if !cache_size.is_empty() {
            config.cache_size = Some(cache_size);
            changed = true;
        }
    }

    if let Some(interval_ms) = args.slab_flush_interval_ms {
        if interval_ms > 0 {
            config.vfs.slab_flush_interval_ms = interval_ms;
            changed = true;
        } else {
            config.vfs.background_slab_flush_enabled = false;
            changed = true;
            eprintln!("warning: slab flush interval 0 disables background slab flush");
        }
    }

    if args.enable_bg_slab_flush {
        config.vfs.background_slab_flush_enabled = true;
        changed = true;
    }

    if args.disable_bg_slab_flush {
        config.vfs.background_slab_flush_enabled = false;
        changed = true;
    }

    if changed {
        save_studiofs_config(&config)?;
        println!("✓ Updated studiofs settings");
        if let Ok(path) = config_toml_path() {
            println!("  Saved to {}", path.display());
        }
    }

    println!("Current studiofs settings:");
    println!(
        "  mount-point: {}",
        config.mount_point.unwrap_or_else(|| "<unset>".to_string())
    );
    println!(
        "  cache-dir: {}",
        config.cache_dir.unwrap_or_else(|| "<unset>".to_string())
    );
    println!(
        "  memory-limit: {}",
        config
            .memory_limit
            .unwrap_or_else(|| DEFAULT_MEMORY_LIMIT.to_string())
    );
    println!(
        "  cache-size: {}",
        config
            .cache_size
            .unwrap_or_else(|| DEFAULT_CACHE_SIZE.to_string())
    );
    println!(
        "  slab-flush-interval-ms: {}",
        config.vfs.slab_flush_interval_ms
    );
    println!(
        "  background-slab-flush: {}",
        if config.vfs.background_slab_flush_enabled {
            "enabled"
        } else {
            "disabled"
        }
    );

    if !changed {
        println!("  (no changes requested)");
    }

    Ok(())
}

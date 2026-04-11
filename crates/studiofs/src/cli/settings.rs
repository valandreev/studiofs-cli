//! `studiofs settings` — configure global defaults for mounts.

use std::path::{Path, PathBuf};

use clap::Args;
use serde::{Deserialize, Serialize};
use studiofs_vfs::VfsConfigRaw;

const CONFIG_FILE_NAME: &str = "config.toml";
const DEFAULT_CACHE_SIZE: &str = "100GB";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedMountRuntimeSettings {
    pub mount_point: String,
    pub cache_dir: Option<PathBuf>,
    pub cache_size: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StudioFsConfig {
    pub mount_point: Option<String>,
    pub cache_dir: Option<String>,
    pub cache_size: Option<String>,
    #[serde(default)]
    pub vfs: VfsConfigRaw,
}

#[derive(Debug, Args, Default)]
pub struct SettingsArgs {
    /// Default mount point used by `studiofs mount` when omitted.
    #[arg(long)]
    pub mount_point: Option<String>,

    /// Default cache directory used by `studiofs mount` when `--cache-dir` is omitted.
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,

    /// Default max disk cache size used by `studiofs mount` when omitted.
    #[arg(long)]
    pub cache_size: Option<String>,
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

fn non_empty_trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|s| !s.is_empty())
}

fn configured_mount_point_for_mount(config: &StudioFsConfig) -> Option<&str> {
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
    if !super::utils::is_drive_root(mount_point) {
        anyhow::bail!(
            "Invalid mount point '{}': Windows requires a drive letter mount (for example `Z:`).",
            mount_point
        );
    }

    Ok(())
}

fn resolve_mount_point_for_mount(
    config: &StudioFsConfig,
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
    config: &StudioFsConfig,
    cli_cache_dir: Option<&Path>,
) -> Option<PathBuf> {
    cli_cache_dir
        .map(Path::to_path_buf)
        .or_else(|| non_empty_trimmed(config.cache_dir.as_deref()).map(PathBuf::from))
        .filter(|path| !path.as_os_str().is_empty())
}

fn resolve_cache_size_for_mount(config: &StudioFsConfig, cli_cache_size: Option<&str>) -> String {
    non_empty_trimmed(cli_cache_size)
        .or_else(|| non_empty_trimmed(config.cache_size.as_deref()))
        .unwrap_or(DEFAULT_CACHE_SIZE)
        .to_string()
}

pub(crate) fn resolve_mount_runtime_settings(
    config: &StudioFsConfig,
    cli_mount_point: Option<&str>,
    cli_cache_dir: Option<&Path>,
    cli_cache_size: Option<&str>,
) -> anyhow::Result<ResolvedMountRuntimeSettings> {
    let mount_point = resolve_mount_point_for_mount(config, cli_mount_point)?;
    let cache_dir = resolve_cache_dir_for_mount(config, cli_cache_dir);
    let cache_size = resolve_cache_size_for_mount(config, cli_cache_size);
    Ok(ResolvedMountRuntimeSettings {
        mount_point,
        cache_dir,
        cache_size,
    })
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

    if let Some(cache_size) = args.cache_size {
        let cache_size = cache_size.trim().to_string();
        if !cache_size.is_empty() {
            config.cache_size = Some(cache_size);
            changed = true;
        }
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
        "  cache-size: {}",
        config
            .cache_size
            .unwrap_or_else(|| DEFAULT_CACHE_SIZE.to_string())
    );
    println!("  cache-policy: auto (derived from cache-size at mount time)");

    if !changed {
        println!("  (no changes requested)");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_mount_defaults() -> StudioFsConfig {
        StudioFsConfig {
            mount_point: Some("/config/mount".to_string()),
            cache_dir: Some("/config/cache".to_string()),
            cache_size: Some("64GB".to_string()),
            vfs: VfsConfigRaw::default(),
        }
    }

    #[test]
    fn resolve_mount_runtime_settings_prefers_cli_cache_settings() {
        let config = config_with_mount_defaults();
        let cli_cache_dir = Path::new("/cli/cache");
        let cli_mount_point = if cfg!(windows) {
            Some("Z:")
        } else {
            Some("/cli/mount")
        };

        let resolved = resolve_mount_runtime_settings(
            &config,
            cli_mount_point,
            Some(cli_cache_dir),
            Some("128GB"),
        )
        .expect("mount config resolved");

        if cfg!(windows) {
            assert_eq!(resolved.mount_point, "Z:");
        } else {
            assert_eq!(resolved.mount_point, "/cli/mount");
        }
        assert_eq!(resolved.cache_dir, Some(cli_cache_dir.to_path_buf()));
        assert_eq!(resolved.cache_size, "128GB");
    }

    #[test]
    fn resolve_mount_runtime_settings_blank_cli_cache_values_fall_back() {
        let config = config_with_mount_defaults();
        let cli_mount_point = if cfg!(windows) {
            Some("Z:")
        } else {
            Some("  /cli/mount  ")
        };

        let resolved = resolve_mount_runtime_settings(&config, cli_mount_point, None, Some("   "))
            .expect("mount config resolved");

        if cfg!(windows) {
            assert_eq!(resolved.mount_point, "Z:");
        } else {
            assert_eq!(resolved.mount_point, "/cli/mount");
        }
        assert_eq!(resolved.cache_dir, Some(PathBuf::from("/config/cache")));
        assert_eq!(resolved.cache_size, "64GB");
    }

    #[test]
    fn resolve_mount_runtime_settings_uses_default_cache_size_when_unset() {
        let config = StudioFsConfig {
            cache_dir: Some("/config/cache".to_string()),
            ..StudioFsConfig::default()
        };
        let cli_mount_point = if cfg!(windows) {
            Some("Z:")
        } else {
            Some("/cli/mount")
        };

        let resolved = resolve_mount_runtime_settings(&config, cli_mount_point, None, None)
            .expect("mount config resolved");

        assert_eq!(resolved.cache_dir, Some(PathBuf::from("/config/cache")));
        assert_eq!(resolved.cache_size, DEFAULT_CACHE_SIZE);
    }

    #[cfg(not(windows))]
    #[test]
    fn resolve_mount_runtime_settings_prefers_cli_mount_point_over_config() {
        let config = config_with_mount_defaults();

        let resolved = resolve_mount_runtime_settings(&config, Some("/cli/mount"), None, None)
            .expect("mount config resolved");

        assert_eq!(resolved.mount_point, "/cli/mount");
    }

    #[cfg(not(windows))]
    #[test]
    fn resolve_mount_runtime_settings_uses_config_mount_point_when_cli_is_blank() {
        let config = config_with_mount_defaults();

        let resolved = resolve_mount_runtime_settings(&config, Some("   "), None, None)
            .expect("config mount point used");

        assert_eq!(resolved.mount_point, "/config/mount");
    }

    #[cfg(windows)]
    #[test]
    fn resolve_mount_runtime_settings_requires_cli_mount_point_on_windows() {
        let config = StudioFsConfig {
            mount_point: Some("Y:".to_string()),
            ..config_with_mount_defaults()
        };

        let err = resolve_mount_runtime_settings(&config, None, None, None)
            .expect_err("windows mount should require explicit cli mount point");
        assert!(err.to_string().contains("No mount point provided"));
    }

    #[cfg(windows)]
    #[test]
    fn resolve_mount_runtime_settings_rejects_non_drive_root_mount_point() {
        let config = StudioFsConfig::default();

        let err = resolve_mount_runtime_settings(&config, Some(r"C:\mounts\studiofs"), None, None)
            .expect_err("windows mount should reject directory mount points");
        assert!(err.to_string().contains("drive letter mount"));
    }
}

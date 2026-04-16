//! `studiofs settings` — configure global defaults for mounts.

use std::path::PathBuf;

use clap::Args;
use serde::{Deserialize, Serialize};
use studiofs_vfs::VfsConfigRaw;

use super::runtime_tuning::{PublicRuntimeTuningArgs, runtime_tuning_summary_lines};

const CONFIG_FILE_NAME: &str = "config.toml";
pub(crate) const DEFAULT_CACHE_SIZE: &str = "100GB";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StudioFsConfig {
    pub mount_point: Option<String>,
    pub cache_dir: Option<String>,
    pub cache_size: Option<String>,
    #[serde(default)]
    pub vfs: VfsConfigRaw,
}

#[derive(Debug, Args, Default)]
#[command(about = "Manage defaults for `studiofs mount`.")]
#[command(after_help = "\
Examples:
  Persist default mount + cache settings for quick mount commands:
    studiofs settings --mount-point /mnt/studio
    studiofs settings --cache-dir ~/.cache/studiofs --cache-size 1TB
    studiofs settings --read-buffer 4GB --readahead 256 --prefetch 192")]
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

    #[command(flatten, next_help_heading = "Runtime IO")]
    pub runtime_tuning: PublicRuntimeTuningArgs,
}

impl SettingsArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        run(self)
    }
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

    if args.runtime_tuning.has_any() {
        args.runtime_tuning.apply_to_vfs_config(&mut config.vfs)?;
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
        "  cache-size: {}",
        config
            .cache_size
            .unwrap_or_else(|| DEFAULT_CACHE_SIZE.to_string())
    );
    println!("  cache-policy: auto (derived from cache-size at mount time)");
    for line in runtime_tuning_summary_lines(&config.vfs) {
        println!("{line}");
    }

    if !changed {
        println!("  (no changes requested)");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::error::ErrorKind;

    #[test]
    fn settings_parses_mount_point() {
        let cli =
            crate::cli::parse_for_test(&["settings", "--mount-point", "/mnt/studio"]).unwrap();
        match cli.command {
            crate::cli::Commands::Settings(SettingsArgs { mount_point, .. }) => {
                assert_eq!(mount_point.unwrap(), "/mnt/studio");
            }
            other => panic!("expected Settings, got {other:?}"),
        }
    }

    #[test]
    fn settings_parses_cache_options() {
        let cli = crate::cli::parse_for_test(&[
            "settings",
            "--cache-dir",
            "/tmp/studio-cache",
            "--cache-size",
            "100GB",
        ])
        .unwrap();
        match cli.command {
            crate::cli::Commands::Settings(SettingsArgs {
                cache_dir,
                cache_size,
                ..
            }) => {
                assert_eq!(cache_dir.unwrap(), PathBuf::from("/tmp/studio-cache"));
                assert_eq!(cache_size.unwrap(), "100GB");
            }
            other => panic!("expected Settings, got {other:?}"),
        }
    }

    #[test]
    fn settings_accepts_public_runtime_tuning_flags() {
        crate::cli::parse_for_test(&[
            "settings",
            "--read-buffer",
            "4GB",
            "--readahead",
            "256",
            "--prefetch",
            "192",
            "--writeback-delay",
            "1500",
        ])
        .expect("new public runtime tuning flags should parse");
    }

    #[test]
    fn settings_rejects_cache_safety_mode_flag() {
        let err = match crate::cli::parse_for_test(&["settings", "--cache-safety-mode", "durable"])
        {
            Ok(_) => panic!("expected unknown argument for removed cache-safety-mode flag"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
        assert!(err.to_string().contains("--cache-safety-mode"));
    }

    #[test]
    fn settings_rejects_slab_flush_interval_flag() {
        let err = match crate::cli::parse_for_test(&["settings", "--slab-flush-interval", "100"]) {
            Ok(_) => panic!("expected unknown argument for removed slab-flush-interval flag"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
        assert!(err.to_string().contains("--slab-flush-interval"));
    }
}

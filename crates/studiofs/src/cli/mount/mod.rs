//! `studiofs mount` — mount a filesystem volume.

mod backend;
mod session;

use clap::Args;
use std::path::PathBuf;

use super::runtime_tuning::PublicRuntimeTuningArgs;

pub(crate) use session::MountSession;

#[derive(Debug, Clone)]
pub(crate) struct MountCommand {
    pub(crate) name: String,
    pub(crate) meta_dir: PathBuf,
    pub(crate) mount_point: Option<String>,
    pub(crate) cache_dir: Option<PathBuf>,
    pub(crate) cache_size: Option<String>,
    pub(crate) principal: Option<String>,
    pub(crate) foreground: bool,
    pub(crate) detach: bool,
    pub(crate) profile: Option<super::mount_profile_types::MountProfileMode>,
    pub(crate) snapshot_id: Option<String>,
    pub(crate) read_only: bool,
    pub(crate) endpoint: Option<String>,
    pub(crate) runtime_tuning: PublicRuntimeTuningArgs,
}

impl MountCommand {
    fn from_args(args: MountArgs) -> anyhow::Result<Self> {
        let meta_dir = super::utils::get_default_meta_dir(&args.name)?;
        Ok(Self {
            name: args.name,
            meta_dir,
            mount_point: args.mount_point,
            cache_dir: args.cache_dir,
            cache_size: args.cache_size,
            principal: args.principal,
            foreground: args.foreground,
            detach: args.detach,
            profile: args.profile,
            snapshot_id: args.snapshot_id,
            read_only: args.read_only,
            endpoint: args.endpoint,
            runtime_tuning: args.runtime_tuning,
        })
    }
}

#[derive(Debug, Clone, Args)]
#[command(about = "Mount a filesystem volume as a local drive")]
#[command(long_about = "\
Mount a previously initialized volume. On Windows, the mount point is a drive \
letter (e.g. Z:). On macOS, it is an existing directory path. Linux mount is \
currently unsupported.

The local client uses its cached metadata database while synchronizing shared \
state through PostgreSQL. Same-mount reads observe accepted writes \
immediately through the Local-First view. Other mounts converge after \
background upload and metadata publish; close does not imply a global \
close-to-open barrier.

On Windows and macOS, StudioFS automatically attempts to show a tray / menu-bar \
status indicator when GUI services are available. If tray initialization fails, \
the mount continues normally in CLI mode.

The process blocks while the filesystem is mounted. Press Ctrl+C to unmount.")]
#[command(after_help = "\
EXAMPLES:
  Basic mount (Windows):
    studiofs mount myvolume Z:

  Basic mount (macOS):
    studiofs mount myvolume /mnt/studio

  Local data cache mount (location + size):
    studiofs mount myvolume Z: --cache-dir D:\\cache\\studiofs --cache-size 500GB

  Local-First IO tuned mount:
    studiofs mount myvolume /mnt/studio --read-buffer 4GB --readahead 256 --prefetch 192

  Debug logging:
    studiofs --log-level debug mount myvolume /mnt/studio

PYROSCOPE PROFILING:
  studiofs mount myvolume /mnt/studio --profile
  studiofs mount myvolume /mnt/studio --profile=off
  STUDIOFS_PROFILE=pyroscope|off
  STUDIOFS_PYROSCOPE_URL=http://localhost:4040
  STUDIOFS_PYROSCOPE_APP_NAME=studiofs.mount
  STUDIOFS_PYROSCOPE_SAMPLE_RATE=100
  STUDIOFS_PYROSCOPE_TAGS=team=vfx,cache_contract=fast-local-authority

SIZE FORMATS:
  --cache-size accepts: KB, MB, GB, TB (e.g. 512MB, 4GB, 1TB)
  --read-buffer accepts the same size formats.

RUNTIME IO:
  Tune only the main Local-First IO controls here:
    --read-buffer
    --readahead
    --prefetch
  Metadata-cache internals stay on strong built-in defaults.

CONSISTENCY MODEL:
  Same-mount reads see accepted writes immediately.
  Other mounts converge after background upload + publish.
  `flush()` / `fsync()` and the last-handle close/release barrier confirm local
  durability when dirty state remains, not remote close-to-open visibility.

ACL PRINCIPAL:
  By default, the mount uses STUDIOFS_PRINCIPAL (or '*' when unset).
  You can override it per mount with:
    --principal alice
")]
pub(crate) struct MountArgs {
    /// Volume name (must be initialized first via `studiofs init`)
    name: String,

    /// Mount point: drive letter on Windows (Z:), directory path on Unix (/mnt/studio)
    mount_point: Option<String>,

    /// Disk cache directory (defaults to `<meta-dir>/cache`)
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// Maximum disk cache size (e.g. 100GB, 50GB, 1TB)
    #[arg(long)]
    cache_size: Option<String>,

    #[command(flatten, next_help_heading = "Runtime IO")]
    runtime_tuning: PublicRuntimeTuningArgs,

    /// Principal identity used for ACL checks in this mount instance.
    #[arg(long)]
    principal: Option<String>,

    /// Run in foreground (keep terminal attached, default behavior)
    #[arg(long, conflicts_with = "detach")]
    foreground: bool,

    /// Detach and run mount in the background (daemon mode).
    /// Logs are written to `<client-root>/logs/`, defaulting to `~/.studiofs/logs/`.
    /// Use `studiofs umount <mount_point>` to stop.
    #[arg(short = 'd', long, conflicts_with = "foreground")]
    detach: bool,

    /// Enable or disable Pyroscope profiling for the mount runtime.
    #[arg(long, num_args = 0..=1, default_missing_value = "pyroscope", value_enum)]
    profile: Option<super::mount_profile_types::MountProfileMode>,

    /// Mount a historical snapshot id as read-only.
    #[arg(long)]
    snapshot_id: Option<String>,

    /// Mount the live volume read-only.
    #[arg(long)]
    read_only: bool,

    /// Override the S3 endpoint URL for this mount session.
    ///
    /// Use https:// for TLS or http:// to explicitly select plain HTTP
    /// transport.
    /// This override only applies to the current process and is NOT saved.
    #[arg(long)]
    endpoint: Option<String>,
}

impl MountArgs {
    #[cfg(any(windows, target_os = "macos", target_os = "linux"))]
    pub(crate) fn should_detach(&self) -> bool {
        self.detach
    }

    #[cfg(any(windows, target_os = "macos", target_os = "linux"))]
    pub(crate) fn volume_name(&self) -> &str {
        &self.name
    }

    pub(crate) fn dispatch_with_auto_tray(self) -> anyhow::Result<()> {
        crate::tray::dispatch_mount_with_auto_tray(self)
    }

    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        #[cfg(windows)]
        super::winfsp_bootstrap::ensure_mountmgr_bootstrap()?;

        MountSession::prepare(MountCommand::from_args(self)?)?.launch()
    }
}

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;

    #[test]
    fn mount_accepts_public_runtime_tuning_flags() {
        let mount_point = if cfg!(windows) { "Z:" } else { "/mnt/studio" };
        crate::cli::parse_for_test(&[
            "mount",
            "demo",
            mount_point,
            "--read-buffer",
            "4GB",
            "--readahead",
            "256",
            "--prefetch",
            "192",
        ])
        .expect("new public runtime tuning flags should parse");
    }

    #[test]
    fn mount_rejects_writeback_delay_flag() {
        let mount_point = if cfg!(windows) { "Z:" } else { "/mnt/studio" };
        let err = match crate::cli::parse_for_test(&[
            "mount",
            "demo",
            mount_point,
            "--writeback-delay",
            "1500",
        ]) {
            Ok(_) => panic!("expected unknown argument for removed writeback-delay flag"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
        assert!(err.to_string().contains("--writeback-delay"));
    }

    #[test]
    fn mount_rejects_zero_readahead() {
        let mount_point = if cfg!(windows) { "Z:" } else { "/mnt/studio" };
        let err =
            match crate::cli::parse_for_test(&["mount", "demo", mount_point, "--readahead", "0"]) {
                Ok(_) => panic!("zero readahead should be rejected"),
                Err(err) => err,
            };
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
        assert!(err.to_string().contains("--readahead"));
    }

    #[test]
    fn mount_rejects_zero_prefetch() {
        let mount_point = if cfg!(windows) { "Z:" } else { "/mnt/studio" };
        let err =
            match crate::cli::parse_for_test(&["mount", "demo", mount_point, "--prefetch", "0"]) {
                Ok(_) => panic!("zero prefetch should be rejected"),
                Err(err) => err,
            };
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
        assert!(err.to_string().contains("--prefetch"));
    }
}

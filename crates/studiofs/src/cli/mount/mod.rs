//! `studiofs mount` — mount a filesystem volume.

mod backend;
mod session;

use clap::Args;
use std::path::PathBuf;

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
    pub(crate) endpoint: Option<String>,
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
            endpoint: args.endpoint,
        })
    }
}

#[derive(Debug, Args)]
#[command(about = "Mount a filesystem volume as a local drive")]
#[command(long_about = "\
Mount a previously initialized volume. On Windows, the mount point is a drive \
letter (e.g. Z:). On Linux/macOS, it is an existing directory path.

The local client uses its cached metadata database while synchronizing shared \
state through PostgreSQL.

The process blocks while the filesystem is mounted. Press Ctrl+C to unmount.")]
#[command(after_help = "\
EXAMPLES:
  Basic mount (Windows):
    studiofs mount myvolume Z:

  Basic mount (Linux/macOS):
    studiofs mount myvolume /mnt/studio

  Cache 3.0 two-knob mount (location + size):
    studiofs mount myvolume Z: --cache-dir D:\\cache\\studiofs --cache-size 500GB

  Debug logging:
    studiofs --log-level debug mount myvolume /mnt/studio

PYROSCOPE PROFILING:
  studiofs mount myvolume /mnt/studio --profile
  studiofs mount myvolume /mnt/studio --profile=off
  STUDIOFS_PROFILE=pyroscope|off
  STUDIOFS_PYROSCOPE_URL=http://localhost:4040
  STUDIOFS_PYROSCOPE_APP_NAME=studiofs.mount
  STUDIOFS_PYROSCOPE_SAMPLE_RATE=100
  STUDIOFS_PYROSCOPE_TAGS=team=vfx,cache_mode=durable

SIZE FORMATS:
  --cache-size accepts: KB, MB, GB, TB (e.g. 512MB, 4GB, 1TB)

RUNTIME PG POLICY:
  PG batching / sync cadence is client-side and can be tuned per mount via:
    STUDIOFS_PG_FLUSH_INTERVAL_SECS
    STUDIOFS_PG_FLUSH_OPS_THRESHOLD
    STUDIOFS_PG_FLUSH_IDLE_MS
    STUDIOFS_PG_SYNC_POLL_SECS

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

    /// Principal identity used for ACL checks in this mount instance.
    #[arg(long)]
    principal: Option<String>,

    /// Run in foreground (keep terminal attached, default behavior)
    #[arg(long, conflicts_with = "detach")]
    foreground: bool,

    /// Detach and run mount in the background (daemon mode).
    /// Logs are written to ~/.studiofs/logs/.
    /// Use `studiofs umount <mount_point>` to stop.
    #[arg(short = 'd', long, conflicts_with = "foreground")]
    detach: bool,

    /// Enable or disable Pyroscope profiling for the mount runtime.
    #[arg(long, num_args = 0..=1, default_missing_value = "pyroscope", value_enum)]
    profile: Option<super::mount_profile_types::MountProfileMode>,

    /// Mount a historical snapshot id as read-only.
    #[arg(long)]
    snapshot_id: Option<String>,

    /// Override the S3 endpoint URL for this mount session.
    ///
    /// Useful for mounting via a local IP address (e.g. http://192.168.1.50:9000)
    /// instead of the public endpoint configured during `studiofs init`.
    /// This override only applies to the current process and is NOT saved.
    #[arg(long)]
    endpoint: Option<String>,
}

impl MountArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        #[cfg(windows)]
        super::winfsp_bootstrap::ensure_mountmgr_bootstrap()?;

        MountSession::prepare(MountCommand::from_args(self)?)?.launch()
    }
}

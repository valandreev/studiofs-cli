//! StudioFS CLI — FUSE mount tool for S3-backed virtual filesystems.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod cli;

/// On Windows, locate WinFSP via the registry and pre-load `winfsp-x64.dll`
/// so that the delay-load resolver can find it.
#[cfg(windows)]
fn init_winfsp_dll_path() {
    use std::os::windows::ffi::OsStrExt;

    unsafe extern "system" {
        fn LoadLibraryW(lpLibFileName: *const u16) -> *mut core::ffi::c_void;
    }

    // Read SxsDir from HKLM\SOFTWARE\WOW6432Node\WinFsp (or HKLM\SOFTWARE\WinFsp)
    let dll_path = (|| -> Option<PathBuf> {
        let hklm = winreg::RegKey::predef(winreg::enums::HKEY_LOCAL_MACHINE);

        let key = hklm
            .open_subkey("SOFTWARE\\WOW6432Node\\WinFsp")
            .or_else(|_| hklm.open_subkey("SOFTWARE\\WinFsp"))
            .ok()?;

        // Try SxsDir first (side-by-side installation)
        if let Ok(sxs_dir) = key.get_value::<String, _>("SxsDir") {
            let dll = PathBuf::from(&sxs_dir).join("bin").join("winfsp-x64.dll");
            if dll.exists() {
                return Some(dll);
            }
        }

        // Fallback: InstallDir
        if let Ok(install_dir) = key.get_value::<String, _>("InstallDir") {
            let dll = PathBuf::from(&install_dir)
                .join("bin")
                .join("winfsp-x64.dll");
            if dll.exists() {
                return Some(dll);
            }
        }

        None
    })();

    if let Some(path) = dll_path {
        // Convert to null-terminated UTF-16 for LoadLibraryW
        let wide: Vec<u16> = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        // SAFETY: LoadLibraryW with a valid, null-terminated UTF-16 path.
        // Pre-loading the DLL means the delay-load resolver will find it
        // already in memory without needing to search PATH.
        let handle = unsafe { LoadLibraryW(wide.as_ptr()) };
        if handle.is_null() {
            eprintln!("warning: failed to load winfsp-x64.dll from {:?}", path);
        }
    } else {
        eprintln!(
            "warning: WinFSP not found in registry. \
             Install WinFSP from https://winfsp.dev/ or add winfsp-x64.dll to PATH."
        );
    }
}

#[derive(Parser)]
#[command(name = "studiofs")]
#[command(version)]
#[command(about = "Mount S3-compatible storage as a local filesystem")]
#[command(long_about = "\
StudioFS — a high-performance, cross-platform virtual filesystem backed by \
S3-compatible cloud storage for file data and PostgreSQL for shared metadata. \
It combines the scalability of object storage with multi-tier caching (memory + \
disk) and a distributed metadata backend for multi-client mounts.

Supported platforms:
  • Windows  (via WinFSP)
  • Linux    (via FUSE / libfuse3)
  • macOS    (via macFUSE)

Supported S3 backends:
  AWS S3, MinIO, Google Cloud Storage, Wasabi, Backblaze B2, and any S3-compatible service.")]
#[command(after_help = "\
EXAMPLES:
  Initialize a new volume with MinIO + PostgreSQL:
    studiofs init myvolume --endpoint http://127.0.0.1:9000 \\
      --bucket mybucket --access-key minioadmin --secret-key minioadmin \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432

  Discover the same volume on another machine/client:
    studiofs discover myvolume \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432

  Mount on Windows (drive letter):
    studiofs mount myvolume Z:

  Mount in background (daemon mode):
    studiofs mount myvolume Z: -d

  Mount on Linux/macOS (directory):
    studiofs mount myvolume /mnt/studio --memory-limit 4GB

  Unmount:
    studiofs umount Z:
    studiofs umount /mnt/studio

  Check volume status:
    studiofs status myvolume

  Run janitor in dry-run mode:
    studiofs janitor myvolume --dry-run

ENV TUNING:
  STUDIOFS_JANITOR_INTERVAL_SECS    Auto-janitor cycle interval (default: 1800).
  STUDIOFS_JANITOR_SKIP_NEWER_SECS  Grace window for orphan deletion (default: 900).
  STUDIOFS_JANITOR_MAX_DELETES      Max janitor deletes per cycle (default: 500).
  STUDIOFS_JANITOR_LEASE_TTL_SECS   Janitor lease TTL in metadata store (default: 2700).
  STUDIOFS_UPLOAD_DELAY_MS          Delay uploads for overwrite/delete coalescing (default: 2000).
  STUDIOFS_UPLOAD_REQUEUE_BASE_MS   Base backoff for exhausted upload requeue (default: 5000).
  STUDIOFS_UPLOAD_REQUEUE_MAX_MS    Max backoff for exhausted upload requeue (default: 120000).
  STUDIOFS_UPLOAD_STATS_INTERVAL_SECS  Upload metrics log interval (default: 10).
  STUDIOFS_UPLOAD_CACHE_IO_SLOTS    Upload local-cache I/O slots (default: 32).
  STUDIOFS_UPLOAD_CACHE_IO_USER_PENALTY  Upload slot cost when reads active (default: 4).
  STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS Keep read priority briefly after read burst (default: 250).
  STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS  Wait uploads on unmount (default: 0, resume next mount).

  Enable debug logging:
    studiofs --log-level debug mount myvolume Z:
")]
struct Cli {
    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info", global = true)]
    log_level: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Initialize a new filesystem volume
    #[command(long_about = "\
Initialize a new filesystem volume by validating S3 object storage, creating \
shared PostgreSQL metadata state, and bootstrapping the local metadata cache \
for the current client.

Path layout is fixed by disk name:
  • Data objects: <disk-name>/objects/...

`--meta` is required and must point at the PostgreSQL server. StudioFS always \
stores metadata in database `studiofs`; it will create that database during \
`init` when the server allows it. S3-backed metadata manifests and Tonbo/NATS \
metadata flows are no longer supported.

The target S3 bucket is created automatically if it does not exist and the \
provided credentials allow it.")]
    #[command(after_help = "\
EXAMPLES:
  AWS S3 + PostgreSQL:
    studiofs init prod --endpoint https://s3.amazonaws.com \\
      --bucket my-bucket --access-key AKIA... --secret-key wJal... \\
      --region us-east-1 --meta postgres://user:pass@db:5432

  MinIO (local) + PostgreSQL:
    studiofs init dev --endpoint http://127.0.0.1:9000 \\
      --bucket test-bucket --access-key minioadmin --secret-key minioadmin \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432

  Custom block size (16 MiB instead of default 8 MiB):
    studiofs init custom --endpoint http://127.0.0.1:9000 \\
      --bucket test-bucket --access-key minioadmin --secret-key minioadmin \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432 \\
      --block-size 16MB

  Multi-client tuning (aggressive convergence):
    studiofs init teamdisk --endpoint http://127.0.0.1:9000 \\
      --bucket data-bucket --access-key minioadmin --secret-key minioadmin \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432 \\
      --metadata-cache-ttl-secs 5 --metadata-flush-interval-secs 15 \\
      --metadata-flush-ops-threshold 20 --metadata-flush-bytes-threshold 262144
")]
    Init {
        /// Disk name (used in object paths like <name>/objects)
        name: String,

        /// S3 endpoint URL (e.g. https://s3.amazonaws.com, http://127.0.0.1:9000)
        #[arg(long)]
        endpoint: String,

        /// S3 bucket name for file data (created if missing when permissions allow)
        #[arg(long)]
        bucket: String,

        /// S3 access key ID
        #[arg(long)]
        access_key: String,

        /// S3 secret access key
        #[arg(long)]
        secret_key: String,

        /// S3 region (defaults to us-east-1 for MinIO compatibility)
        #[arg(long, default_value = "")]
        region: String,

        /// Deprecated legacy option. Metadata is PostgreSQL-only now.
        #[arg(long, hide = true)]
        metadata_endpoint: Option<String>,

        /// PostgreSQL server DSN; StudioFS uses database `studiofs`
        #[arg(long = "meta", required = true)]
        meta: Option<String>,

        /// Deprecated legacy option. Metadata is PostgreSQL-only now.
        #[arg(long, hide = true)]
        metadata_bucket: Option<String>,

        /// Deprecated legacy option. Metadata is PostgreSQL-only now.
        #[arg(long, hide = true)]
        metadata_access_key: Option<String>,

        /// Deprecated legacy option. Metadata is PostgreSQL-only now.
        #[arg(long, hide = true)]
        metadata_secret_key: Option<String>,

        /// Deprecated legacy option. Metadata is PostgreSQL-only now.
        #[arg(long, hide = true)]
        metadata_region: Option<String>,

        /// Metadata cache TTL in seconds (read-through refresh window).
        #[arg(long, default_value_t = 20)]
        metadata_cache_ttl_secs: u64,

        /// Maximum flush interval in seconds for batched remote metadata push.
        #[arg(long, default_value_t = 60)]
        metadata_flush_interval_secs: u64,

        /// Flush early when queued metadata ops reach this threshold.
        #[arg(long, default_value_t = 100)]
        metadata_flush_ops_threshold: u32,

        /// Flush early when queued metadata bytes reach this threshold.
        #[arg(long, default_value_t = 1024 * 1024)]
        metadata_flush_bytes_threshold: u64,

        /// Block size in bytes (e.g. 4MB, 8MB, 16MB). Defaults to 8MB if not specified.
        /// Larger blocks reduce metadata overhead but increase memory usage per operation.
        #[arg(long)]
        block_size: Option<String>,
    },

    /// Discover an existing PostgreSQL-backed volume and create local metadata for this client
    #[command(long_about = "\
Download the latest PostgreSQL checkpoint for an existing shared volume and \
create the local metadata cache for this client.

Use this on additional machines or user profiles after the volume was created \
with `studiofs init`. Runtime convergence after discovery continues through \
PostgreSQL delta batches.")]
    #[command(after_help = "\
EXAMPLES:
  Discover a volume from PostgreSQL:
    studiofs discover myvolume \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432
")]
    Discover {
        /// Volume name to discover
        name: String,

        /// PostgreSQL server DSN; StudioFS uses database `studiofs`
        #[arg(long = "meta")]
        meta: String,
    },

    /// Mount a filesystem volume as a local drive
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

  High-performance mount (4 GB memory cache, 500 GB disk cache):
    studiofs mount myvolume Z: --memory-limit 4GB --cache-size 500GB \\
      --cache-dir D:\\cache\\studiofs

  Debug logging:
    studiofs --log-level debug mount myvolume /mnt/studio

SIZE FORMATS:
  --memory-limit and --cache-size accept: KB, MB, GB, TB (e.g. 512MB, 4GB, 1TB)
")]
    Mount {
        /// Volume name (must be initialized first via `studiofs init`)
        name: String,

        /// Mount point: drive letter on Windows (Z:), directory path on Unix (/mnt/studio)
        mount_point: String,

        /// Disk cache directory (defaults to <meta-dir>/cache)
        #[arg(long)]
        cache_dir: Option<PathBuf>,

        /// Maximum disk cache size (e.g. 100GB, 50GB, 1TB)
        #[arg(long, default_value = "100GB")]
        cache_size: String,

        /// Maximum in-memory cache size (e.g. 1GB, 512MB, 4GB)
        #[arg(long, default_value = "1GB")]
        memory_limit: String,

        /// Run in foreground (keep terminal attached)
        #[arg(long)]
        foreground: bool,

        /// Detach and run mount in the background (daemon mode).
        /// Logs are written to ~/.studiofs/logs/.
        /// Use `studiofs umount <mount_point>` to stop.
        #[arg(short = 'd', long)]
        detach: bool,
    },

    /// Unmount a previously mounted filesystem volume
    #[command(after_help = "\
EXAMPLES:
  studiofs umount Z:
  studiofs umount /mnt/studio
")]
    Umount {
        /// Mount point to unmount (e.g. Z: on Windows, /mnt/studio on Linux/macOS)
        mount_point: String,
    },

    /// Show volume status, PostgreSQL metadata binding, and local counters
    #[command(after_help = "\
EXAMPLES:
  studiofs status myvolume
")]
    Status {
        /// Volume name
        name: String,
    },

    /// Scan data objects and remove orphaned ones (Phase 6 prototype)
    #[command(after_help = "\
EXAMPLES:
  studiofs janitor myvolume --dry-run
  studiofs janitor myvolume --max-deletes 500 --skip-newer-minutes 15
  studiofs janitor myvolume --loop --interval-minutes 30
")]
    Janitor {
        /// Volume name
        name: String,

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
    },
    /// Permanently destroy a volume
    #[command(long_about = "\
Permanently destroy a volume, deleting all data objects from S3, removing the \
shared PostgreSQL metadata for that volume, and deleting local metadata/cache \
for this client.

WARNING: This action is irreversible.")]
    Destroy {
        /// Volume name to destroy
        name: String,

        /// Remove only local cache/config; keep remote metadata and data.
        #[arg(long)]
        local: bool,
    },
}

fn main() -> anyhow::Result<()> {
    // Ensure WinFSP DLL is findable before anything else
    #[cfg(windows)]
    init_winfsp_dll_path();

    let cli = Cli::parse();

    // Initialize tracing
    let filter = tracing_subscriber::EnvFilter::try_new(&cli.log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    match cli.command {
        Commands::Init {
            name,
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            metadata_endpoint,
            meta,
            metadata_bucket,
            metadata_access_key,
            metadata_secret_key,
            metadata_region,
            metadata_cache_ttl_secs,
            metadata_flush_interval_secs,
            metadata_flush_ops_threshold,
            metadata_flush_bytes_threshold,
            block_size,
        } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::init::run(
                &name,
                &endpoint,
                &bucket,
                &access_key,
                &secret_key,
                &region,
                meta.as_deref(),
                metadata_endpoint.as_deref(),
                metadata_bucket.as_deref(),
                metadata_access_key.as_deref(),
                metadata_secret_key.as_deref(),
                metadata_region.as_deref(),
                metadata_cache_ttl_secs,
                metadata_flush_interval_secs,
                metadata_flush_ops_threshold,
                metadata_flush_bytes_threshold,
                &meta_dir,
                block_size.as_deref(),
            )?;
        }
        Commands::Discover { name, meta } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::discovery::run(&name, &meta, &meta_dir)?;
        }
        Commands::Mount {
            name,
            mount_point,
            cache_dir,
            cache_size,
            memory_limit,
            foreground,
            detach,
        } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::mount::run(
                &name,
                &mount_point,
                &meta_dir,
                cache_dir.as_deref(),
                &cache_size,
                &memory_limit,
                foreground,
                detach,
            )?;
        }
        Commands::Umount { mount_point } => {
            cli::umount::run(&mount_point)?;
        }
        Commands::Status { name } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::status::run(&name, &meta_dir)?;
        }
        Commands::Destroy { name, local } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::destroy::run(&name, &meta_dir, local)?;
        }
        Commands::Janitor {
            name,
            dry_run,
            r#loop,
            interval_minutes,
            skip_newer_minutes,
            max_deletes,
        } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::janitor::run(
                &name,
                &meta_dir,
                dry_run,
                r#loop,
                interval_minutes,
                skip_newer_minutes,
                max_deletes,
            )?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("studiofs").chain(args.iter().copied()))
    }

    // ── mount --detach / -d ──────────────────────────────────────────────────

    #[test]
    fn mount_short_detach_flag() {
        let cli = parse(&["mount", "vol", "Z:", "-d"]).unwrap();
        match cli.command {
            Commands::Mount {
                detach,
                foreground,
                name,
                mount_point,
                ..
            } => {
                assert!(detach);
                assert!(!foreground);
                assert_eq!(name, "vol");
                assert_eq!(mount_point, "Z:");
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_long_detach_flag() {
        let cli = parse(&["mount", "vol", "/mnt/x", "--detach"]).unwrap();
        match cli.command {
            Commands::Mount { detach, .. } => assert!(detach),
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_no_detach_by_default() {
        let cli = parse(&["mount", "vol", "Z:"]).unwrap();
        match cli.command {
            Commands::Mount {
                detach, foreground, ..
            } => {
                assert!(!detach);
                assert!(!foreground);
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_foreground_and_detach_together() {
        // Both flags can be parsed (the implementation handles conflicts at runtime).
        let cli = parse(&["mount", "vol", "Z:", "--foreground", "-d"]).unwrap();
        match cli.command {
            Commands::Mount {
                detach, foreground, ..
            } => {
                assert!(detach);
                assert!(foreground);
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_with_options_and_detach() {
        let cli = parse(&[
            "mount",
            "vol",
            "Z:",
            "--memory-limit",
            "4GB",
            "--cache-size",
            "200GB",
            "-d",
        ])
        .unwrap();
        match cli.command {
            Commands::Mount {
                detach,
                memory_limit,
                cache_size,
                ..
            } => {
                assert!(detach);
                assert_eq!(memory_limit, "4GB");
                assert_eq!(cache_size, "200GB");
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    // ── umount ───────────────────────────────────────────────────────────────

    #[test]
    fn umount_parses_mount_point() {
        let cli = parse(&["umount", "Z:"]).unwrap();
        match cli.command {
            Commands::Umount { mount_point } => {
                assert_eq!(mount_point, "Z:");
            }
            other => panic!("expected Umount, got {other:?}"),
        }
    }

    #[test]
    fn umount_unix_path() {
        let cli = parse(&["umount", "/mnt/studio"]).unwrap();
        match cli.command {
            Commands::Umount { mount_point } => {
                assert_eq!(mount_point, "/mnt/studio");
            }
            other => panic!("expected Umount, got {other:?}"),
        }
    }

    #[test]
    fn umount_requires_mount_point() {
        let result = parse(&["umount"]);
        assert!(result.is_err());
    }

    // ── global flag propagation ──────────────────────────────────────────────

    #[test]
    fn log_level_propagates_to_mount() {
        let cli = parse(&["--log-level", "debug", "mount", "vol", "Z:"]).unwrap();
        assert_eq!(cli.log_level, "debug");
    }

    #[test]
    fn log_level_propagates_to_umount() {
        let cli = parse(&["--log-level", "trace", "umount", "Z:"]).unwrap();
        assert_eq!(cli.log_level, "trace");
    }
}

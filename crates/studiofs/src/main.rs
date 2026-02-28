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
S3-compatible cloud storage. It combines the scalability of object storage with \
multi-tier caching (memory + disk) for near-local access speeds.

Supported platforms:
  • Windows  (via WinFSP)
  • Linux    (via FUSE / libfuse3)
  • macOS    (via macFUSE)

Supported S3 backends:
  AWS S3, MinIO, Google Cloud Storage, Wasabi, Backblaze B2, and any S3-compatible service.")]
#[command(after_help = "\
EXAMPLES:
  Initialize a new volume with MinIO:
    studiofs init myvolume --endpoint http://127.0.0.1:9000 \\
      --bucket mybucket --access-key minioadmin --secret-key minioadmin

  Mount on Windows (drive letter):
    studiofs mount myvolume Z:

  Mount on Linux/macOS (directory):
    studiofs mount myvolume /mnt/studio --memory-limit 4GB

  Check volume status:
    studiofs status myvolume

  Update metadata tuning for an existing volume:
    studiofs tune-metadata myvolume --cache-ttl-secs 5 --flush-interval-secs 15

  Run janitor in dry-run mode:
    studiofs janitor myvolume --dry-run

  Enable cross-client cache invalidation bus (NATS):
    studiofs mount myvolume Z: --nats nats://127.0.0.1:4222

ENV TUNING:
  STUDIOFS_JANITOR_INTERVAL_SECS    Auto-janitor cycle interval (default: 1800).
  STUDIOFS_JANITOR_SKIP_NEWER_SECS  Grace window for orphan deletion (default: 900).
  STUDIOFS_JANITOR_MAX_DELETES      Max janitor deletes per cycle (default: 500).
  STUDIOFS_JANITOR_LEASE_TTL_SECS   Janitor lease TTL in metadata store (default: 2700).

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

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new filesystem volume
    #[command(long_about = "\
Initialize a new filesystem volume by creating a local metadata database and \
storing connection details for data + metadata stores.

Path layout is fixed by disk name:
  • Data objects:     <disk-name>/objects/...
  • Metadata manifest <disk-name>/metadata/manifest.json

The target S3 bucket must already exist — this command does not create it.")]
    #[command(after_help = "\
EXAMPLES:
  AWS S3:
    studiofs init prod --endpoint https://s3.amazonaws.com \\
      --bucket my-bucket --access-key AKIA... --secret-key wJal... --region us-east-1

  MinIO (local):
    studiofs init dev --endpoint http://127.0.0.1:9000 \\
      --bucket test-bucket --access-key minioadmin --secret-key minioadmin

  Separate metadata backend/bucket:
    studiofs init prod --endpoint https://s3.us-east-1.amazonaws.com \\
      --bucket data-bucket --access-key DATA_KEY --secret-key DATA_SECRET \\
      --metadata-endpoint http://127.0.0.1:9000 \\
      --metadata-bucket meta-bucket --metadata-access-key minioadmin \\
      --metadata-secret-key minioadmin

  Custom block size (8 MiB instead of default 4 MiB):
    studiofs init custom --endpoint http://127.0.0.1:9000 \\
      --bucket test-bucket --access-key minioadmin --secret-key minioadmin \\
      --block-size 8MB

  Multi-client tuning (aggressive convergence):
    studiofs init teamdisk --endpoint http://127.0.0.1:9000 \\
      --bucket data-bucket --access-key minioadmin --secret-key minioadmin \\
      --metadata-cache-ttl-secs 5 --metadata-flush-interval-secs 15 \\
      --metadata-flush-ops-threshold 20 --metadata-flush-bytes-threshold 262144
")]
    Init {
        /// Disk name (used in object paths like <name>/objects and <name>/metadata)
        name: String,

        /// S3 endpoint URL (e.g. https://s3.amazonaws.com, http://127.0.0.1:9000)
        #[arg(long)]
        endpoint: String,

        /// S3 bucket name (must already exist)
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

        /// Metadata endpoint URL (separate store; if omitted, re-uses the data endpoint/bucket)
        #[arg(long)]
        metadata_endpoint: Option<String>,

        /// Metadata bucket (required when --metadata-endpoint is provided)
        #[arg(long)]
        metadata_bucket: Option<String>,

        /// Metadata access key (required when --metadata-endpoint is provided)
        #[arg(long)]
        metadata_access_key: Option<String>,

        /// Metadata secret key (required when --metadata-endpoint is provided)
        #[arg(long)]
        metadata_secret_key: Option<String>,

        /// Metadata region (optional, defaults to empty)
        #[arg(long)]
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

        /// Block size in bytes (e.g. 4MB, 8MB, 16MB). Defaults to 4MB if not specified.
        /// Larger blocks reduce metadata overhead but increase memory usage per operation.
        #[arg(long)]
        block_size: Option<String>,
    },

    /// Discover and attach local cache to an existing Tonbo metadata volume
    #[command(long_about = "\
Attach to an existing disk by reading metadata manifest from:
  <disk-name>/metadata/manifest.json

Discovery requires only metadata endpoint/bucket/credentials + disk name.
Data endpoint/bucket/prefix are loaded from remote metadata.")]
    #[command(after_help = "\
EXAMPLES:
  studiofs discover mydisk --endpoint http://127.0.0.1:9000 \\
    --bucket meta-bucket --access-key minioadmin --secret-key minioadmin
")]
    Discover {
        /// Disk name to discover
        name: String,

        /// Metadata endpoint URL
        #[arg(long)]
        endpoint: String,

        /// Metadata bucket
        #[arg(long)]
        bucket: String,

        /// Metadata access key
        #[arg(long)]
        access_key: String,

        /// Metadata secret key
        #[arg(long)]
        secret_key: String,

        /// Metadata region
        #[arg(long, default_value = "")]
        region: String,
    },

    /// Mount a filesystem volume as a local drive
    #[command(long_about = "\
Mount a previously initialized volume. On Windows, the mount point is a drive \
letter (e.g. Z:). On Linux/macOS, it is an existing directory path.

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

  Mount with explicit NATS server:
    studiofs mount myvolume Z: --nats nats://10.0.0.5:4222

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

        /// NATS URL for cross-client invalidation (CLI value overrides env; default is local NATS)
        #[arg(long, default_value = "nats://127.0.0.1:4222")]
        nats: String,

        /// Run in foreground (keep terminal attached)
        #[arg(long)]
        foreground: bool,
    },

    /// Show volume status and statistics
    #[command(after_help = "\
EXAMPLES:
  studiofs status myvolume
")]
    Status {
        /// Volume name
        name: String,
    },

    /// Update metadata cache/flush tuning for an existing volume
    #[command(after_help = "\
EXAMPLES:
  studiofs tune-metadata myvolume --cache-ttl-secs 5
  studiofs tune-metadata myvolume --flush-interval-secs 15 --flush-ops-threshold 20
")]
    TuneMetadata {
        /// Volume name
        name: String,

        /// Metadata cache TTL in seconds (minimum 1)
        #[arg(long)]
        cache_ttl_secs: Option<u64>,

        /// Metadata flush interval in seconds (minimum 1)
        #[arg(long)]
        flush_interval_secs: Option<u64>,

        /// Metadata flush ops threshold (minimum 1)
        #[arg(long)]
        flush_ops_threshold: Option<u32>,

        /// Metadata flush bytes threshold (minimum 1)
        #[arg(long)]
        flush_bytes_threshold: Option<u64>,
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
    /// Test S3-compatible storage for StudioFS compatibility
    #[command(long_about = "\
Tests whether an S3-compatible backend is suitable for StudioFS by running:
  • Data suite   — PUT/GET/HEAD/range-GET/DELETE at block sizes (1 KB, 4 MB)
  • Metadata suite — conditional PUT (If-Match, If-None-Match) and ETag round-trip

Without --bucket: a temporary bucket is created and deleted after the run.
  Credentials must include s3:CreateBucket + s3:DeleteBucket.

With --bucket: the specified bucket is used as-is. Temporary objects are removed
  after the run, but the bucket itself is never deleted. If the bucket is not
  accessible the test is aborted immediately.")]
    #[command(after_help = "\
EXAMPLES:
  MinIO (local, auto-create bucket):
    studiofs test --entrypoint http://127.0.0.1:9000 \\
      --access-key minioadmin --secret-key minioadmin

  Use an existing bucket (bucket is not deleted after test):
    studiofs test --entrypoint http://127.0.0.1:9000 \\
      --access-key minioadmin --secret-key minioadmin --bucket my-bucket

  AWS S3:
    studiofs test --entrypoint https://s3.us-east-1.amazonaws.com \\
      --access-key AKIA... --secret-key wJal... --region us-east-1

  Wasabi:
    studiofs test --entrypoint https://s3.wasabisys.com \\
      --access-key KEY --secret-key SECRET --region us-east-1
")]
    Test {
        /// S3 endpoint URL (e.g. https://s3.amazonaws.com, http://127.0.0.1:9000)
        #[arg(long)]
        entrypoint: String,

        /// S3 access key ID
        #[arg(long)]
        access_key: String,

        /// S3 secret access key
        #[arg(long)]
        secret_key: String,

        /// S3 region (defaults to us-east-1)
        #[arg(long, default_value = "us-east-1")]
        region: String,

        /// Use an existing bucket for the test. Temp objects are cleaned up but the
        /// bucket itself is never deleted. If not set, a random bucket is created and
        /// removed after the run.
        #[arg(long)]
        bucket: Option<String>,
    },
    /// Permanently destroy a volume
    #[command(long_about = "\
Permanently destroy a volume, deleting all data from S3 and removing local metadata.
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
        Commands::Mount {
            name,
            mount_point,
            cache_dir,
            cache_size,
            memory_limit,
            nats,
            foreground,
        } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::mount::run(
                &name,
                &mount_point,
                &meta_dir,
                cache_dir.as_deref(),
                &cache_size,
                &memory_limit,
                &nats,
                foreground,
            )?;
        }
        Commands::Discover {
            name,
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
        } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::discovery::run(
                &name,
                &endpoint,
                &bucket,
                &access_key,
                &secret_key,
                &region,
                &meta_dir,
            )?;
        }
        Commands::Status { name } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::status::run(&name, &meta_dir)?;
        }
        Commands::TuneMetadata {
            name,
            cache_ttl_secs,
            flush_interval_secs,
            flush_ops_threshold,
            flush_bytes_threshold,
        } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::tune_metadata::run(
                &name,
                &meta_dir,
                cache_ttl_secs,
                flush_interval_secs,
                flush_ops_threshold,
                flush_bytes_threshold,
            )?;
        }
        Commands::Test {
            entrypoint,
            access_key,
            secret_key,
            region,
            bucket,
        } => {
            cli::test::run(
                &entrypoint,
                &access_key,
                &secret_key,
                &region,
                bucket.as_deref(),
            )?;
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

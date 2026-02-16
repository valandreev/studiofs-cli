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
            let dll = PathBuf::from(&install_dir).join("bin").join("winfsp-x64.dll");
            if dll.exists() {
                return Some(dll);
            }
        }

        None
    })();

    if let Some(path) = dll_path {
        // Convert to null-terminated UTF-16 for LoadLibraryW
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(std::iter::once(0)).collect();

        // SAFETY: LoadLibraryW with a valid, null-terminated UTF-16 path.
        // Pre-loading the DLL means the delay-load resolver will find it
        // already in memory without needing to search PATH.
        let handle = unsafe { LoadLibraryW(wide.as_ptr()) };
        if handle.is_null() {
            eprintln!(
                "warning: failed to load winfsp-x64.dll from {:?}",
                path
            );
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
storing the S3 connection details. The volume name is used as a prefix for all \
S3 object keys, so it must be unique within the bucket.

The target S3 bucket must already exist — this command does not create it.")]
    #[command(after_help = "\
EXAMPLES:
  AWS S3:
    studiofs init prod --endpoint https://s3.amazonaws.com \\
      --bucket my-bucket --access-key AKIA... --secret-key wJal... --region us-east-1

  MinIO (local):
    studiofs init dev --endpoint http://127.0.0.1:9000 \\
      --bucket test-bucket --access-key minioadmin --secret-key minioadmin

  Custom metadata directory:
    studiofs init myvolume --endpoint https://s3.example.com \\
      --bucket data --access-key KEY --secret-key SECRET --meta-dir /var/lib/studiofs
")]
    Init {
        /// Volume name (used as S3 key prefix — must be unique per bucket)
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

        /// Directory to store the metadata database
        #[arg(long, default_value = ".studiofs")]
        meta_dir: PathBuf,
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

        /// Path to metadata database directory
        #[arg(long, default_value = ".studiofs")]
        meta_dir: PathBuf,

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
    },

    /// Show volume status and statistics
    #[command(after_help = "\
EXAMPLES:
  studiofs status myvolume
  studiofs status myvolume --meta-dir /var/lib/studiofs
")]
    Status {
        /// Volume name
        name: String,

        /// Path to metadata database directory
        #[arg(long, default_value = ".studiofs")]
        meta_dir: PathBuf,
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
            meta_dir,
        } => {
            cli::init::run(
                &name,
                &endpoint,
                &bucket,
                &access_key,
                &secret_key,
                &region,
                &meta_dir,
            )?;
        }
        Commands::Mount {
            name,
            mount_point,
            meta_dir,
            cache_dir,
            cache_size,
            memory_limit,
            foreground,
        } => {
            cli::mount::run(
                &name,
                &mount_point,
                &meta_dir,
                cache_dir.as_deref(),
                &cache_size,
                &memory_limit,
                foreground,
            )?;
        }
        Commands::Status { name, meta_dir } => {
            cli::status::run(&name, &meta_dir)?;
        }
    }

    Ok(())
}

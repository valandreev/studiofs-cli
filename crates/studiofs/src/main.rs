//! StudioFS CLI — FUSE mount tool for S3-backed virtual filesystems.

use chrono::{Duration, NaiveDate, Utc};
use clap::{Parser, Subcommand};
use std::fs;
use std::path::{Path, PathBuf};
use tracing_appender::non_blocking::WorkerGuard;

mod cli;

const DAEMON_LOG_RETENTION_DAYS: i64 = 30;

fn prune_old_daemon_logs(logs_dir: &Path, stem: &str, today: NaiveDate) -> std::io::Result<()> {
    let cutoff = today - Duration::days(DAEMON_LOG_RETENTION_DAYS);
    for entry in fs::read_dir(logs_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(file_date) = parse_daemon_log_date(file_name, stem) else {
            continue;
        };
        if file_date < cutoff {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn parse_daemon_log_date(file_name: &str, stem: &str) -> Option<NaiveDate> {
    let prefix = format!("{stem}.log.");
    let date_part = file_name.strip_prefix(&prefix)?;
    NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
}

fn init_tracing(log_level: &str) -> anyhow::Result<Option<WorkerGuard>> {
    let filter = tracing_subscriber::EnvFilter::try_new(log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    if let (Ok(stem), Ok(logs_dir)) = (
        std::env::var("STUDIOFS_DAEMON_LOG_STEM"),
        std::env::var("STUDIOFS_DAEMON_LOG_DIR"),
    ) {
        let logs_dir = PathBuf::from(logs_dir);
        fs::create_dir_all(&logs_dir)?;
        let today = Utc::now().date_naive();
        let prune_error = prune_old_daemon_logs(&logs_dir, &stem, today).err();

        let file_appender = tracing_appender::rolling::daily(&logs_dir, format!("{stem}.log"));
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_ansi(false)
            .with_writer(non_blocking)
            .init();

        if let Some(err) = prune_error {
            tracing::warn!(
                log_dir = %logs_dir.display(),
                error = %err,
                "failed to prune old daemon logs"
            );
        }

        Ok(Some(guard))
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .init();
        Ok(None)
    }
}

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
  Initialize a new volume with RustFS + PostgreSQL:
    studiofs init myvolume --endpoint http://127.0.0.1:9000 \\
      --bucket mybucket --access-key studiofs --secret-key studiofs123 \\
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

  Run metadata fsck:
    studiofs fsck myvolume
    studiofs fsck myvolume --repair

ENV TUNING:
  STUDIOFS_JANITOR_INTERVAL_SECS    Auto-janitor cycle interval (default: 1800).
  STUDIOFS_JANITOR_SKIP_NEWER_SECS  Grace window for orphan deletion (default: 900).
  STUDIOFS_JANITOR_MAX_DELETES      Max janitor deletes per cycle (default: 500).
  STUDIOFS_JANITOR_LEASE_TTL_SECS   Janitor lease TTL in metadata store (default: 2700).
  STUDIOFS_UPLOAD_DELAY_MS          Delay uploads for overwrite/delete coalescing (default: 2000).
  STUDIOFS_UPLOAD_REQUEUE_BASE_MS   Base backoff for exhausted upload requeue (default: 5000).
  STUDIOFS_UPLOAD_REQUEUE_MAX_MS    Max backoff for exhausted upload requeue (default: 120000).
  STUDIOFS_UPLOAD_STATS_INTERVAL_SECS  Upload metrics log interval (default: 10).
  STUDIOFS_SLAB_FLUSH_INTERVAL_MS   Background slab flusher wake interval (default: 10).
  STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH  Disable background slab flushing (default: 0).
  STUDIOFS_FOREGROUND_DRAIN_ASSIST  Allow foreground backpressure path to assist drain (default: 1).
  STUDIOFS_MAX_DRAIN_BATCH          Max staged blocks drained per flusher cycle (default: 1024).
  STUDIOFS_L2_COMMIT_WORKERS        L2 commit scheduler workers (default: 32).
  STUDIOFS_L2_COMMIT_QUEUE_CAPACITY L2 commit scheduler queue depth before backpressure (default: 4096).
  STUDIOFS_L2_READ_TIMEOUT_MS       L2 local-read timeout before cache-miss fallback (default: 5000).
  STUDIOFS_L2_WRITE_TIMEOUT_MS      L2 local-write timeout per attempt (default: 5000).
  STUDIOFS_INGRESS_SEGMENT_MAX_BYTES  Rotate append-only ingress segments after this size (default: 256MiB).
  STUDIOFS_CACHE_SAFETY_MODE         Local cache durability policy: fast|durable (default: fast).
  STUDIOFS_INGRESS_CHECKPOINT_INTERVAL_SECS  Periodic ingress checkpoint interval (default: 30).
  STUDIOFS_INGRESS_CHECKPOINT_MIN_APPEND_BYTES  Wake checkpointing after this much uncheckpointed append churn (default: 64MiB).
  STUDIOFS_INGRESS_CHECKPOINT_MIN_MUTATIONS  Wake checkpointing after this many uncheckpointed ingress mutations (default: 128).
  STUDIOFS_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG  Wake checkpointing when active head is this many segments ahead of the last checkpoint (default: 2).
  STUDIOFS_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS  Wake checkpointing when live ingress refs span this many segments (default: 4).
  STUDIOFS_PUBLISH_VISIBILITY_TIMEOUT_SECS  Max time to wait for uploaded objects to become visible before publishing manifests (default: 30).
  STUDIOFS_UPLOAD_CACHE_IO_SLOTS    Upload local-cache I/O slots (default: 32).
  STUDIOFS_UPLOAD_CACHE_IO_USER_PENALTY  Upload slot cost when reads active (default: 4).
  STUDIOFS_TRANSIT_MEMORY_BUDGET    Max active read/upload transit buffers (default: 2048MB).
  STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS Keep read priority briefly after read burst (default: 250).
  STUDIOFS_UPLOAD_FG_READ_LATENCY_TARGET_MS  Foreground read P90 target for upload throttling (default: 50).
  STUDIOFS_UPLOAD_FG_READ_LATENCY_WINDOW_MS  Foreground read speedometer window (default: 3000).
  STUDIOFS_UPLOAD_FG_READ_LATENCY_STICKY_WINDOWS  Keep last P90 for this many windows when samples pause (default: 2).
  STUDIOFS_UPLOAD_MAX_ATTEMPTS      Max upload retries (default: 10).
  STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS  Per-upload request timeout (default: 120).
  STUDIOFS_FETCH_MAX_ATTEMPTS       Max fetch retries (default: 8).
  STUDIOFS_FETCH_REQUEST_TIMEOUT_SECS  Per-fetch request timeout (default: 45).
  STUDIOFS_DENTRY_CACHE_CAPACITY    VFS dentry cache entry budget (default: 65536).
  STUDIOFS_DENTRY_CACHE_BYTES       VFS dentry cache byte budget (default: 64MB).
  STUDIOFS_PATH_SHORTCUT_CACHE_CAPACITY  VFS full-path shortcut cache entry budget (default: 16384).
  STUDIOFS_PATH_SHORTCUT_CACHE_BYTES     VFS full-path shortcut cache byte budget (default: 16MB).
  STUDIOFS_QOS_ACCEPT_SLOTS         QoS slots for accept operations (default: 64).
  STUDIOFS_QOS_MATERIALIZE_SLOTS    QoS slots for materialization (default: 2).
  STUDIOFS_QOS_UPLOAD_SLOTS         QoS slots for uploads (default: 4).
  STUDIOFS_QOS_COMMIT_SLOTS         QoS slots for commits (default: 4).
  STUDIOFS_QOS_FETCH_SLOTS          QoS slots for fetches (default: 128).
  STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS  Wait uploads on unmount (default: 0, resume next mount).
  STUDIOFS_XATTR_MAX_BYTES             Max bytes allowed per xattr value (default: 4MB).

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
`init` when the server allows it.

The target S3 bucket is created automatically if it does not exist and the \
provided credentials allow it.")]
    #[command(after_help = "\
EXAMPLES:
  AWS S3 + PostgreSQL:
    studiofs init prod --endpoint https://s3.amazonaws.com \\
      --bucket my-bucket --access-key AKIA... --secret-key wJal... \\
      --region us-east-1 --meta postgres://user:pass@db:5432

  RustFS (local) + PostgreSQL:
    studiofs init dev --endpoint http://127.0.0.1:9000 \\
      --bucket test-bucket --access-key studiofs --secret-key studiofs123 \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432

  Custom block size (16 MiB instead of default 8 MiB):
    studiofs init custom --endpoint http://127.0.0.1:9000 \\
      --bucket test-bucket --access-key studiofs --secret-key studiofs123 \\
      --meta postgres://studiofs:studiofs@127.0.0.1:5432 \\
      --block-size 16MB

RUNTIME POLICY:
  PG batching / sync cadence is configured per client at mount/runtime, not
  stored in the volume. Use environment variables such as:
    STUDIOFS_PG_FLUSH_INTERVAL_SECS
    STUDIOFS_PG_FLUSH_OPS_THRESHOLD
    STUDIOFS_PG_FLUSH_IDLE_MS
    STUDIOFS_PG_SYNC_POLL_SECS
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

        /// S3 region (defaults to us-east-1 for local S3-compatible services)
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

        /// Block size in bytes (e.g. 4MB, 16MB, 32MB). Defaults to 16MB if not specified.
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

  External USB SSD profile (lower L2 concurrency + tighter local L2 I/O timeouts):
    studiofs mount myvolume Z: --l2-profile usb

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
  --memory-limit and --cache-size accept: KB, MB, GB, TB (e.g. 512MB, 4GB, 1TB)

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
    Mount {
        /// Volume name (must be initialized first via `studiofs init`)
        name: String,

        /// Mount point: drive letter on Windows (Z:), directory path on Unix (/mnt/studio)
        mount_point: Option<String>,

        /// Disk cache directory (defaults to <meta-dir>/cache)
        #[arg(long)]
        cache_dir: Option<PathBuf>,

        /// Maximum disk cache size (e.g. 100GB, 50GB, 1TB)
        #[arg(long)]
        cache_size: Option<String>,

        /// Maximum in-memory cache size (e.g. 1GB, 512MB, 4GB)
        #[arg(long)]
        memory_limit: Option<String>,

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
        profile: Option<cli::mount_profile_types::MountProfileMode>,

        /// L2 disk-cache profile. Use `usb` for external USB SSDs.
        #[arg(long, value_enum, default_value_t = cli::mount::L2Profile::Default)]
        l2_profile: cli::mount::L2Profile,

        /// Enable direct I/O semantics for WinFSP (unbuffered/write-through handling and zero metadata cache timeouts).
        ///
        /// Useful for performance validation tools that require backend-realistic I/O latency.
        /// Direct-write backend wait timeout can be tuned with
        /// `STUDIOFS_DIRECT_IO_SYNC_TIMEOUT_SECS` (default: 120).
        #[arg(long)]
        direct_io: bool,

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
  studiofs status myvolume --cache-dir D:\\cache\\studiofs
")]
    Status {
        /// Volume name
        name: String,

        /// Override local cache base directory (defaults to <meta-dir>/cache)
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },

    /// Manage defaults for `studiofs mount`.
    #[command(after_help = "\
Examples:
  Persist default mount + cache settings for quick mount commands:
    studiofs settings --mount-point /mnt/studio
    studiofs settings --cache-dir ~/.cache/studiofs --memory-limit 4GB --cache-size 1TB")]
    Settings {
        #[command(flatten)]
        settings: cli::settings::SettingsArgs,
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
    /// Check and optionally repair metadata inconsistencies
    #[command(after_help = "\
EXAMPLES:
  studiofs fsck myvolume
  studiofs fsck myvolume --repair
")]
    Fsck {
        /// Volume name
        name: String,

        /// Repair findings immediately without interactive confirmation
        #[arg(long)]
        repair: bool,
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
    /// Dump metadata manifest from PostgreSQL (source of truth)
    Dump {
        /// Disk/volume name
        name: String,

        /// Output dump path (JSON)
        #[arg(long = "path-to-dump")]
        path_to_dump: PathBuf,
    },

    /// Load metadata manifest into PostgreSQL
    Load {
        /// Input dump path (JSON)
        #[arg(long = "path-to-dump")]
        path_to_dump: PathBuf,

        /// PostgreSQL address/DSN
        #[arg(long = "postgresql-adress", alias = "postgresql-address")]
        postgresql_adress: String,
    },

    /// Run cache benchmarks (direct L2 and mounted end-to-end)
    TestCache {
        /// Target benchmark path.
        ///
        /// If path is inside an active StudioFS mount, benchmark runs end-to-end
        /// through that mounted process and stores temporary files under
        /// `<path>/.studiofs-test-cache`. Otherwise benchmark runs direct L2 mode
        /// and treats this path as cache directory.
        path: PathBuf,

        /// Block payload size (e.g. 1MB, 4MB, 16MB, 32MB). Defaults to 16MB.
        #[arg(long)]
        block_size: Option<String>,

        /// Read chunk size (e.g. 4KB, 1MB). Defaults to block-size.
        #[arg(long)]
        read_size: Option<String>,

        /// Upper bound for the `small` size class (inclusive), e.g. 10KB.
        #[arg(long)]
        small_max: Option<String>,

        /// Upper bound for the `medium` size class (inclusive), e.g. 16MB.
        #[arg(long)]
        medium_max: Option<String>,

        /// Number of concurrent streams.
        #[arg(long, default_value_t = 1)]
        concurrency: usize,

        /// Run each stream sequentially across its assigned segment.
        #[arg(long)]
        sequential_streams: bool,

        /// Maximum in-flight I/O operations.
        #[arg(long, default_value_t = 32)]
        io_depth: usize,

        /// Number of warm-up cycles executed before metrics collection.
        #[arg(long, default_value_t = 0)]
        warmup_runs: u32,

        /// Workload size profile:
        /// - percentages: `small=60,medium=30,large=10`
        /// - explicit sizes: `4KB,128KB,1MB,16MB`
        #[arg(long)]
        sizes: Option<String>,

        /// Access pattern: `sequential`, `random`, or `mixed`.
        #[arg(long, value_enum, default_value_t = cli::test_cache::TestCacheAccessPattern::Sequential)]
        access_pattern: cli::test_cache::TestCacheAccessPattern,

        /// In-memory L1 cache limit (e.g. 2GB).
        #[arg(long)]
        l1_limit: Option<String>,

        /// On-disk L2 cache limit (e.g. 100GB).
        #[arg(long)]
        l2_limit: Option<String>,

        /// JSON report output path. Defaults to an auto-named file in the
        /// current working directory.
        #[arg(long)]
        json_output: Option<PathBuf>,

        /// Enable on-demand profiling for this benchmark run.
        #[arg(long, value_enum, num_args = 0..=1, default_missing_value = "pyroscope")]
        profile: Option<cli::test_cache_profile_types::TestCacheProfileMode>,
    },

    /// Create or list immutable metadata snapshots
    Snapshot {
        /// Volume name
        name: String,
        #[command(subcommand)]
        action: SnapshotAction,
    },

    #[command(hide = true)]
    Internal {
        #[command(subcommand)]
        action: InternalAction,
    },
}

#[derive(Subcommand, Debug)]
enum SnapshotAction {
    /// Create a new snapshot
    Create {
        /// Optional custom root inode (default: root inode)
        #[arg(long)]
        root_ino: Option<u64>,
    },
    /// List known snapshots
    List,
}

#[derive(Subcommand, Debug)]
enum InternalAction {
    #[command(name = "winfsp-bootstrap-mountmgr", hide = true)]
    WinfspBootstrapMountmgr,
}

fn main() -> anyhow::Result<()> {
    // Ensure WinFSP DLL is findable before anything else
    #[cfg(windows)]
    init_winfsp_dll_path();

    let cli = Cli::parse();

    // Initialize tracing
    let _daemon_log_guard = init_tracing(&cli.log_level)?;

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
            principal,
            foreground,
            detach,
            l2_profile,
            direct_io,
            profile,
            snapshot_id,
            endpoint,
        } => {
            #[cfg(windows)]
            cli::winfsp_bootstrap::ensure_mountmgr_bootstrap()?;

            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::mount::run(
                &name,
                mount_point.as_deref(),
                &meta_dir,
                cache_dir.as_deref(),
                cache_size.as_deref(),
                memory_limit.as_deref(),
                principal.as_deref(),
                foreground,
                detach,
                l2_profile,
                direct_io,
                profile,
                snapshot_id.as_deref(),
                endpoint.as_deref(),
            )?;
        }
        Commands::Settings { settings } => {
            cli::settings::run(settings)?;
        }
        Commands::Umount { mount_point } => {
            cli::umount::run(&mount_point)?;
        }
        Commands::Status { name, cache_dir } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::status::run(&name, &meta_dir, cache_dir.as_deref())?;
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
        Commands::Fsck { name, repair } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::fsck::run(&name, &meta_dir, repair)?;
        }
        Commands::Dump { name, path_to_dump } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            cli::dump_load::dump(&name, &meta_dir, &path_to_dump)?;
        }
        Commands::Load {
            path_to_dump,
            postgresql_adress,
        } => {
            cli::dump_load::load(&path_to_dump, &postgresql_adress)?;
        }
        Commands::TestCache {
            path,
            block_size,
            read_size,
            small_max,
            medium_max,
            concurrency,
            sequential_streams,
            io_depth,
            warmup_runs,
            sizes,
            access_pattern,
            l1_limit,
            l2_limit,
            json_output,
            profile,
        } => {
            cli::test_cache::run(
                path,
                block_size,
                read_size,
                small_max,
                medium_max,
                concurrency,
                sequential_streams,
                io_depth,
                warmup_runs,
                sizes,
                access_pattern,
                l1_limit,
                l2_limit,
                profile,
                json_output,
            )?;
        }
        Commands::Snapshot { name, action } => {
            let meta_dir = cli::utils::get_default_meta_dir(&name)?;
            match action {
                SnapshotAction::Create { root_ino } => {
                    cli::snapshot::create(&name, &meta_dir, root_ino)?
                }
                SnapshotAction::List => cli::snapshot::list(&name, &meta_dir)?,
            }
        }
        Commands::Internal { action } => match action {
            InternalAction::WinfspBootstrapMountmgr => {
                #[cfg(windows)]
                {
                    cli::winfsp_bootstrap::run_internal_bootstrap_mountmgr()?;
                }
                #[cfg(not(windows))]
                {
                    anyhow::bail!("internal WinFSP bootstrap is only supported on Windows");
                }
            }
        },
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Parser, error::ErrorKind};

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("studiofs").chain(args.iter().copied()))
    }

    fn mount_runs_in_foreground(foreground: bool, detach: bool) -> bool {
        foreground || !detach
    }

    #[test]
    fn parses_daily_daemon_log_file_names() {
        let stem = "volume-1234";
        assert_eq!(
            parse_daemon_log_date("volume-1234.log.2026-03-13", stem),
            Some(NaiveDate::from_ymd_opt(2026, 3, 13).unwrap())
        );
        assert_eq!(
            parse_daemon_log_date("volume-1234-2026-03-13.log", stem),
            None
        );
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
                assert!(!mount_runs_in_foreground(foreground, detach));
                assert_eq!(name, "vol");
                assert_eq!(mount_point.as_deref(), Some("Z:"));
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
                assert!(mount_runs_in_foreground(foreground, detach));
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_foreground_and_detach_together() {
        let err = match parse(&["mount", "vol", "Z:", "--foreground", "-d"]) {
            Ok(_) => panic!("expected clap conflict error"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
        let rendered = err.to_string();
        assert!(rendered.contains("--foreground"));
        assert!(rendered.contains("--detach") || rendered.contains("-d"));
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
                assert_eq!(memory_limit.unwrap(), "4GB");
                assert_eq!(cache_size.unwrap(), "200GB");
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_parses_profile_shorthand() {
        let cli = parse(&["mount", "vol", "Z:", "--profile"]).unwrap();
        match cli.command {
            Commands::Mount { profile, .. } => assert!(matches!(
                profile,
                Some(cli::mount_profile_types::MountProfileMode::Pyroscope)
            )),
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_parses_profile_off() {
        let cli = parse(&["mount", "vol", "Z:", "--profile=off"]).unwrap();
        match cli.command {
            Commands::Mount { profile, .. } => assert!(matches!(
                profile,
                Some(cli::mount_profile_types::MountProfileMode::Off)
            )),
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_parses_l2_profile_usb() {
        let cli = parse(&["mount", "vol", "Z:", "--l2-profile", "usb"]).unwrap();
        match cli.command {
            Commands::Mount { l2_profile, .. } => {
                assert_eq!(l2_profile, cli::mount::L2Profile::Usb);
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_parses_direct_io_flag() {
        let cli = parse(&["mount", "vol", "Z:", "--direct-io"]).unwrap();
        match cli.command {
            Commands::Mount { direct_io, .. } => {
                assert!(direct_io);
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn settings_parses_mount_point() {
        let cli = parse(&["settings", "--mount-point", "/mnt/studio"]).unwrap();
        match cli.command {
            Commands::Settings { settings } => {
                assert_eq!(settings.mount_point.unwrap(), "/mnt/studio");
            }
            other => panic!("expected Settings, got {other:?}"),
        }
    }

    #[test]
    fn settings_parses_cache_options() {
        let cli = parse(&[
            "settings",
            "--cache-dir",
            "/tmp/studio-cache",
            "--memory-limit",
            "4GB",
            "--cache-size",
            "100GB",
        ])
        .unwrap();
        match cli.command {
            Commands::Settings { settings } => {
                assert_eq!(
                    settings.cache_dir.unwrap(),
                    PathBuf::from("/tmp/studio-cache")
                );
                assert_eq!(settings.memory_limit.unwrap(), "4GB");
                assert_eq!(settings.cache_size.unwrap(), "100GB");
            }
            other => panic!("expected Settings, got {other:?}"),
        }
    }

    #[test]
    fn mount_principal_option_parses() {
        let cli = parse(&["mount", "vol", "Z:", "--principal", "alice"]).unwrap();
        match cli.command {
            Commands::Mount { principal, .. } => {
                assert_eq!(principal.as_deref(), Some("alice"));
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_endpoint_override_parses() {
        let cli = parse(&[
            "mount",
            "vol",
            "Z:",
            "--endpoint",
            "http://192.168.1.50:9000",
        ])
        .unwrap();
        match cli.command {
            Commands::Mount { endpoint, .. } => {
                assert_eq!(endpoint.as_deref(), Some("http://192.168.1.50:9000"));
            }
            other => panic!("expected Mount, got {other:?}"),
        }
    }

    #[test]
    fn mount_endpoint_default_is_none() {
        let cli = parse(&["mount", "vol", "Z:"]).unwrap();
        match cli.command {
            Commands::Mount { endpoint, .. } => {
                assert_eq!(endpoint, None);
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

    #[test]
    fn status_parses_cache_dir() {
        let cli = parse(&["status", "vol", "--cache-dir", "D:\\cache\\studiofs"]).unwrap();
        match cli.command {
            Commands::Status { name, cache_dir } => {
                assert_eq!(name, "vol");
                assert_eq!(cache_dir.unwrap(), PathBuf::from("D:\\cache\\studiofs"));
            }
            other => panic!("expected Status, got {other:?}"),
        }
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

    #[test]
    fn fsck_parses_defaults() {
        let cli = parse(&["fsck", "vol"]).unwrap();
        match cli.command {
            Commands::Fsck { name, repair } => {
                assert_eq!(name, "vol");
                assert!(!repair);
            }
            other => panic!("expected Fsck, got {other:?}"),
        }
    }

    #[test]
    fn fsck_parses_repair_flag() {
        let cli = parse(&["fsck", "vol", "--repair"]).unwrap();
        match cli.command {
            Commands::Fsck { repair, .. } => assert!(repair),
            other => panic!("expected Fsck, got {other:?}"),
        }
    }

    #[test]
    fn test_cache_parses_defaults() {
        let cli = parse(&["test-cache", "/tmp/studiofs-cache"]).unwrap();

        match cli.command {
            Commands::TestCache {
                path,
                concurrency,
                sequential_streams,
                io_depth,
                warmup_runs,
                sizes,
                block_size,
                read_size,
                small_max,
                medium_max,
                access_pattern,
                l1_limit,
                l2_limit,
                json_output,
                profile,
                ..
            } => {
                assert_eq!(path, PathBuf::from("/tmp/studiofs-cache"));
                assert_eq!(concurrency, 1);
                assert!(!sequential_streams);
                assert_eq!(io_depth, 32);
                assert_eq!(warmup_runs, 0);
                assert_eq!(sizes.as_deref(), None);
                assert_eq!(block_size.as_deref(), None);
                assert_eq!(read_size.as_deref(), None);
                assert_eq!(small_max.as_deref(), None);
                assert_eq!(medium_max.as_deref(), None);
                assert!(matches!(
                    access_pattern,
                    cli::test_cache::TestCacheAccessPattern::Sequential
                ));
                assert!(l1_limit.is_none());
                assert!(l2_limit.is_none());
                assert!(json_output.is_none());
                assert!(profile.is_none());
            }
            other => panic!("expected TestCache, got {other:?}"),
        }
    }

    #[test]
    fn test_cache_parses_full_flags() {
        let cli = parse(&[
            "test-cache",
            "Z:\\bench",
            "--block-size",
            "4MB",
            "--read-size",
            "1MB",
            "--small-max",
            "64KB",
            "--medium-max",
            "16MB",
            "--concurrency",
            "16",
            "--sequential-streams",
            "--io-depth",
            "64",
            "--warmup-runs",
            "2",
            "--sizes",
            "small=60,medium=30,large=10",
            "--access-pattern",
            "random",
            "--l1-limit",
            "4GB",
            "--l2-limit",
            "1TB",
            "--json-output",
            "reports/cache-run.json",
            "--profile=pyroscope",
        ])
        .unwrap();

        match cli.command {
            Commands::TestCache {
                path,
                block_size,
                read_size,
                small_max,
                medium_max,
                access_pattern,
                concurrency,
                sequential_streams,
                io_depth,
                warmup_runs,
                sizes,
                l1_limit,
                l2_limit,
                json_output,
                profile,
                ..
            } => {
                assert_eq!(path, PathBuf::from("Z:\\bench"));
                assert_eq!(block_size.unwrap(), "4MB");
                assert_eq!(read_size.unwrap(), "1MB");
                assert_eq!(small_max.unwrap(), "64KB");
                assert_eq!(medium_max.unwrap(), "16MB");
                assert!(matches!(
                    access_pattern,
                    cli::test_cache::TestCacheAccessPattern::Random
                ));
                assert_eq!(concurrency, 16);
                assert!(sequential_streams);
                assert_eq!(io_depth, 64);
                assert_eq!(warmup_runs, 2);
                assert_eq!(sizes.unwrap(), "small=60,medium=30,large=10");
                assert_eq!(l1_limit.unwrap(), "4GB");
                assert_eq!(l2_limit.unwrap(), "1TB");
                assert_eq!(
                    json_output.unwrap(),
                    PathBuf::from("reports/cache-run.json")
                );
                assert!(matches!(
                    profile,
                    Some(cli::test_cache_profile_types::TestCacheProfileMode::Pyroscope)
                ));
            }
            other => panic!("expected TestCache, got {other:?}"),
        }
    }

    #[test]
    fn test_cache_parses_profile_shorthand() {
        let cli = parse(&["test-cache", "/tmp/studiofs-cache", "--profile"]).unwrap();

        match cli.command {
            Commands::TestCache { profile, .. } => assert!(matches!(
                profile,
                Some(cli::test_cache_profile_types::TestCacheProfileMode::Pyroscope)
            )),
            other => panic!("expected TestCache, got {other:?}"),
        }
    }
}

//! StudioFS CLI — FUSE mount tool for S3-backed virtual filesystems.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod cli;

#[derive(Parser)]
#[command(name = "studiofs")]
#[command(about = "Mount S3 storage as a local filesystem")]
#[command(version)]
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
    Init {
        /// Volume name
        name: String,

        /// S3 endpoint URL
        #[arg(long)]
        endpoint: String,

        /// S3 bucket name
        #[arg(long)]
        bucket: String,

        /// S3 access key
        #[arg(long)]
        access_key: String,

        /// S3 secret key
        #[arg(long)]
        secret_key: String,

        /// S3 region (optional)
        #[arg(long, default_value = "")]
        region: String,

        /// Path to store metadata database
        #[arg(long, default_value = ".studiofs")]
        meta_dir: PathBuf,
    },

    /// Mount a filesystem
    Mount {
        /// Volume name (must be initialized first)
        name: String,

        /// Mount point (drive letter on Windows, path on Unix)
        mount_point: String,

        /// Path to metadata database directory
        #[arg(long, default_value = ".studiofs")]
        meta_dir: PathBuf,

        /// Cache directory
        #[arg(long)]
        cache_dir: Option<PathBuf>,

        /// Maximum cache size (e.g. "100GB", "50GB")
        #[arg(long, default_value = "100GB")]
        cache_size: String,

        /// Maximum memory usage for caching (e.g. "1GB", "512MB")
        #[arg(long, default_value = "1GB")]
        memory_limit: String,

        /// Run in foreground
        #[arg(long)]
        foreground: bool,
    },

    /// Show filesystem status
    Status {
        /// Volume name
        name: String,

        /// Path to metadata database directory
        #[arg(long, default_value = ".studiofs")]
        meta_dir: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
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

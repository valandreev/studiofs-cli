//! `studiofs init` — initialize a new filesystem volume.

use std::io::Write;
use std::path::Path;
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{
    DEFAULT_BLOCK_SIZE, FormatInfo, MetaEngine, MetadataBackend, MetadataConfig, RedbMetaEngine,
    RemoteStoreConfig, TonboMetaEngine,
};
use tracing::info;
use uuid::Uuid;

fn verify_store_access(label: &str, cfg: &DataStoreConfig) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let ds = DataStore::new(cfg)?;
        ds.check()
            .await
            .map_err(|e| anyhow::anyhow!("{} check failed: {}", label, e))
    })
}

/// Parse a size string like "4MB", "1GB" into bytes.
/// Supports KB, MB, GB, TB suffixes. Defaults to bytes if no suffix.
fn parse_size(size_str: &str) -> anyhow::Result<u32> {
    let size_str = size_str.trim().to_uppercase();

    let (num_part, suffix) = if let Some(pos) = size_str.find(|c: char| c.is_alphabetic()) {
        (&size_str[..pos], &size_str[pos..])
    } else {
        (&size_str[..], "")
    };

    let num: u64 = num_part
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid number in size: {}", size_str))?;

    let multiplier = match suffix {
        "" => 1,
        "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        "TB" => 1024 * 1024 * 1024 * 1024,
        _ => return Err(anyhow::anyhow!("Unknown size suffix: {}", suffix)),
    };

    let bytes = num.saturating_mul(multiplier);

    if bytes > u32::MAX as u64 {
        return Err(anyhow::anyhow!(
            "Block size too large: {} (max 4GB)",
            size_str
        ));
    }

    Ok(bytes as u32)
}

#[cfg(unix)]
fn get_current_ids() -> (u32, u32) {
    unsafe { (libc::getuid(), libc::getgid()) }
}

#[cfg(windows)]
fn get_current_ids() -> (u32, u32) {
    (0, 0)
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    data_endpoint: &str,
    data_bucket: &str,
    data_access_key: &str,
    data_secret_key: &str,
    data_region: &str,
    metadata_endpoint: Option<&str>,
    metadata_bucket: Option<&str>,
    metadata_access_key: Option<&str>,
    metadata_secret_key: Option<&str>,
    metadata_region: Option<&str>,
    metadata_same_as_data: bool,
    metadata_cache_ttl_secs: u64,
    metadata_flush_interval_secs: u64,
    metadata_flush_ops_threshold: u32,
    metadata_flush_bytes_threshold: u64,
    meta_dir: &Path,
    block_size: Option<&str>,
) -> anyhow::Result<()> {
    // 1. Verify data and metadata stores.
    println!("🔍 Verifying data store connection...");
    let data_cfg = DataStoreConfig {
        endpoint: data_endpoint.to_string(),
        bucket: data_bucket.to_string(),
        access_key: data_access_key.to_string(),
        secret_key: data_secret_key.to_string(),
        region: data_region.to_string(),
        volume_name: name.to_string(),
    };
    verify_store_access("data", &data_cfg)?;
    println!(
        "✅ Data store verified. Bucket '{}' is accessible.",
        data_bucket
    );

    println!();

    // 2. Check if metadata directory already exists
    if meta_dir.exists() {
        println!("! Metadata directory already exists: {:?}", meta_dir);
        print!("  Do you want to CLEAR it and start fresh? [y/N]: ");
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        if input == "y" || input == "yes" {
            info!("clearing existing metadata directory {:?}", meta_dir);
            std::fs::remove_dir_all(meta_dir)?;
        } else {
            return Err(anyhow::anyhow!("Initialization cancelled."));
        }
    }

    // 3. Create metadata directory and initialize DB
    std::fs::create_dir_all(meta_dir)?;

    let db_path = meta_dir.join(format!("{name}.db"));
    info!("initializing volume {name:?} at {db_path:?}");

    let engine = RedbMetaEngine::open(&db_path)?;

    // Parse block size if provided, otherwise use default
    let actual_block_size = if let Some(size_str) = block_size {
        parse_size(size_str)?
    } else {
        DEFAULT_BLOCK_SIZE
    };

    let metadata_prefix = format!("{name}/metadata/");
    let data_prefix = format!("{name}/objects/");

    let metadata_remote = if metadata_same_as_data {
        Some(RemoteStoreConfig {
            endpoint: data_endpoint.to_string(),
            bucket: data_bucket.to_string(),
            access_key: data_access_key.to_string(),
            secret_key: data_secret_key.to_string(),
            region: data_region.to_string(),
            prefix: metadata_prefix.clone(),
        })
    } else {
        let endpoint = metadata_endpoint.ok_or_else(|| {
            anyhow::anyhow!(
                "--metadata-endpoint is required unless --metadata-same-as-data is used"
            )
        })?;
        let bucket = metadata_bucket.ok_or_else(|| {
            anyhow::anyhow!("--metadata-bucket is required unless --metadata-same-as-data is used")
        })?;
        let access_key = metadata_access_key.ok_or_else(|| {
            anyhow::anyhow!(
                "--metadata-access-key is required unless --metadata-same-as-data is used"
            )
        })?;
        let secret_key = metadata_secret_key.ok_or_else(|| {
            anyhow::anyhow!(
                "--metadata-secret-key is required unless --metadata-same-as-data is used"
            )
        })?;
        Some(RemoteStoreConfig {
            endpoint: endpoint.to_string(),
            bucket: bucket.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            region: metadata_region.unwrap_or("").to_string(),
            prefix: metadata_prefix.clone(),
        })
    };
    if let Some(remote) = &metadata_remote {
        println!("🔍 Verifying metadata store connection...");
        let metadata_cfg = DataStoreConfig {
            endpoint: remote.endpoint.clone(),
            bucket: remote.bucket.clone(),
            access_key: remote.access_key.clone(),
            secret_key: remote.secret_key.clone(),
            region: remote.region.clone(),
            volume_name: name.to_string(),
        };
        verify_store_access("metadata", &metadata_cfg)?;
        println!(
            "✅ Metadata store verified. Bucket '{}' is accessible.",
            remote.bucket
        );
    }

    // Store format info
    let format_info = FormatInfo {
        name: name.to_string(),
        uuid: Uuid::new_v4().to_string(),
        endpoint: data_endpoint.to_string(),
        bucket: data_bucket.to_string(),
        access_key: data_access_key.to_string(),
        secret_key: data_secret_key.to_string(),
        region: data_region.to_string(),
        data_prefix,
        metadata: MetadataConfig {
            backend: MetadataBackend::Tonbo,
            remote: metadata_remote,
            cache_ttl_secs: metadata_cache_ttl_secs.max(1),
            flush_interval_secs: metadata_flush_interval_secs.max(1),
            flush_ops_threshold: metadata_flush_ops_threshold.max(1),
            flush_bytes_threshold: metadata_flush_bytes_threshold.max(1),
        },
        version: String::new(),
        block_size: actual_block_size,
        chunk_size: 0,
    };
    engine.set_format_info(&format_info)?;

    // Initialize root inode owned by the current user
    let (uid, gid) = get_current_ids();
    engine.init_root(uid, gid)?;
    drop(engine);

    // Seed initial remote metadata manifest for Tonbo backend.
    if format_info.metadata.backend == MetadataBackend::Tonbo {
        let tonbo = TonboMetaEngine::open(&db_path)?;
        tonbo.flush_now()?;
    }

    info!("volume {name:?} initialized successfully");
    println!("✓ Volume '{}' initialized", name);
    println!("  UUID:       {}", format_info.uuid);
    println!(
        "  Block size: {} bytes ({} MiB)",
        actual_block_size,
        actual_block_size / (1024 * 1024)
    );
    println!("  Data endpoint: {}", data_endpoint);
    println!("  Data bucket:   {}", data_bucket);
    if let Some(remote) = &format_info.metadata.remote {
        println!("  Metadata endpoint: {}", remote.endpoint);
        println!("  Metadata bucket:   {}", remote.bucket);
        println!("  Metadata prefix:   {}", remote.prefix);
    }
    println!("  Metadata:   {:?}", db_path);
    println!();
    println!("Run `studiofs mount {} <mount-point>` to mount.", name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size_bytes() {
        assert_eq!(parse_size("100").unwrap(), 100);
        assert_eq!(parse_size("100B").unwrap(), 100);
    }

    #[test]
    fn test_parse_size_kb() {
        assert_eq!(parse_size("1KB").unwrap(), 1024);
        assert_eq!(parse_size("512KB").unwrap(), 512 * 1024);
    }

    #[test]
    fn test_parse_size_mb() {
        assert_eq!(parse_size("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("4MB").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("8MB").unwrap(), 8 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_gb() {
        assert_eq!(parse_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("2GB").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_case_insensitive() {
        assert_eq!(parse_size("1mb").unwrap(), 1024 * 1024);
        assert_eq!(parse_size("4MB").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("8mB").unwrap(), 8 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_whitespace() {
        assert_eq!(parse_size("  4MB  ").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("4 MB").unwrap(), 4 * 1024 * 1024);
    }

    #[test]
    fn test_parse_size_invalid_suffix() {
        assert!(parse_size("4XB").is_err());
    }

    #[test]
    fn test_parse_size_invalid_number() {
        assert!(parse_size("abcMB").is_err());
    }
}

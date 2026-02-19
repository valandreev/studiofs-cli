//! `studiofs init` — initialize a new filesystem volume.

use std::path::Path;
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{FormatInfo, MetaEngine, RedbMetaEngine, DEFAULT_BLOCK_SIZE};
use tracing::info;
use uuid::Uuid;
use std::io::Write;
use aws_sdk_s3::config::{Credentials, Region};
use aws_config::BehaviorVersion;

/// Parse a size string like "4MB", "1GB" into bytes.
/// Supports KB, MB, GB, TB suffixes. Defaults to bytes if no suffix.
fn parse_size(size_str: &str) -> anyhow::Result<u32> {
    let size_str = size_str.trim().to_uppercase();
    
    let (num_part, suffix) = if let Some(pos) = size_str.find(|c: char| c.is_alphabetic()) {
        (&size_str[..pos], &size_str[pos..])
    } else {
        (&size_str[..], "")
    };
    
    let num: u64 = num_part.trim().parse()
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
        return Err(anyhow::anyhow!("Block size too large: {} (max 4GB)", size_str));
    }
    
    Ok(bytes as u32)
}

async fn create_s3_bucket(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<()> {
    let region_obj = if region.is_empty() {
        Region::new("us-east-1")
    } else {
        Region::new(region.to_string())
    };

    let credentials = Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        "studiofs-init",
    );

    let mut config_builder = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(region_obj)
        .credentials_provider(credentials)
        .force_path_style(true); // Usually needed for MinIO/S3-compatible

    if !endpoint.is_empty() {
        config_builder = config_builder.endpoint_url(endpoint);
    }

    let client = aws_sdk_s3::Client::from_conf(config_builder.build());

    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create bucket: {}", e))?;

    Ok(())
}

#[cfg(unix)]
fn get_current_ids() -> (u32, u32) {
    unsafe { (libc::getuid(), libc::getgid()) }
}

#[cfg(windows)]
fn get_current_ids() -> (u32, u32) {
    (0, 0)
}

pub fn run(
    name: &str,
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    meta_dir: &Path,
    block_size: Option<&str>,
) -> anyhow::Result<()> {
    // 1. Verify S3 credentials and bucket accessibility
    println!("🔍 Verifying S3 connection...");
    let rt = tokio::runtime::Runtime::new()?;
    
    rt.block_on(async {
        let config = DataStoreConfig {
            endpoint: endpoint.to_string(),
            bucket: bucket.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            region: region.to_string(),
            volume_name: name.to_string(),
        };

        let ds = DataStore::new(&config)?;
        
        match ds.check().await {
            Ok(_) => {
                println!("✅ S3 connection verified. Bucket '{}' is accessible.", bucket);
                Ok(())
            }
            Err(e) => {
                // Check if it's a "Not Found" error which usually means bucket is missing
                let err_str = format!("{}", e);
                if err_str.contains("NotFound") || err_str.contains("404") || err_str.contains("NoSuchBucket") {
                    println!("❓ Bucket '{}' was not found.", bucket);
                    print!("   Do you want to create it? [y/N]: ");
                    std::io::stdout().flush()?;
                    
                    let mut input = String::new();
                    std::io::stdin().read_line(&mut input)?;
                    if input.trim().to_lowercase() == "y" {
                        println!("   🚀 Attempting to create bucket '{}'...", bucket);
                        create_s3_bucket(endpoint, bucket, access_key, secret_key, region).await?;
                        println!("   ✅ Bucket '{}' created successfully.", bucket);
                        Ok(())
                    } else {
                        return Err(anyhow::anyhow!("Initialization cancelled: bucket missing."));
                    }
                } else {
                    println!("❌ S3 verification failed!");
                    println!("   Error: {}", e);
                    println!("   Check your Endpoint, Access Key, and Secret Key.");
                    Err(anyhow::anyhow!("S3 verification failed"))
                }
            }
        }
    })?;

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

    // Store format info
    let format_info = FormatInfo {
        name: name.to_string(),
        uuid: Uuid::new_v4().to_string(),
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
        region: region.to_string(),
        block_size: actual_block_size,
        chunk_size: 0,
    };
    engine.set_format_info(&format_info)?;

    // Initialize root inode owned by the current user
    let (uid, gid) = get_current_ids();
    engine.init_root(uid, gid)?;

    info!("volume {name:?} initialized successfully");
    println!("✓ Volume '{}' initialized", name);
    println!("  UUID:       {}", format_info.uuid);
    println!("  Block size: {} bytes ({} MiB)", actual_block_size, actual_block_size / (1024 * 1024));
    println!("  Endpoint:   {}", endpoint);
    println!("  Bucket:     {}", bucket);
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

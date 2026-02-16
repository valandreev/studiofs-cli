//! `studiofs mount` — mount a filesystem volume.

use std::path::Path;
use std::sync::Arc;
use studiofs_cache::disk::DiskCache;
use studiofs_cache::MemoryCache;
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{MetaEngine, RedbMetaEngine};
use studiofs_vfs::StudioVfs;
use tracing::info;

/// Parse a human-readable size string like "100GB", "512MB", "1TB".
fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim().to_uppercase();
    let (num_str, multiplier) = if s.ends_with("TB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024 * 1024)
    } else if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else {
        (s.as_str(), 1u64)
    };
    let num: u64 = num_str.trim().parse()?;
    Ok(num * multiplier)
}

pub fn run(
    name: &str,
    mount_point: &str,
    meta_dir: &Path,
    cache_dir: Option<&Path>,
    cache_size: &str,
    memory_limit: &str,
    _foreground: bool,
) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));
    info!("opening volume {name:?} from {db_path:?}");

    // Open metadata
    let meta = Arc::new(RedbMetaEngine::open(&db_path)?);
    let format_info = meta.get_format_info()?;

    info!(
        "volume: {} ({}), endpoint: {}, bucket: {}",
        format_info.name, format_info.uuid, format_info.endpoint, format_info.bucket
    );

    // Set up data store
    let data_config = DataStoreConfig::from(&format_info);
    let data = Arc::new(DataStore::new(&data_config)?);

    // Set up caches
    let mem_limit = parse_size(memory_limit)? as usize;
    let mem_cache = Arc::new(MemoryCache::new(mem_limit));

    let cache_size_bytes = parse_size(cache_size)?;
    let base_cache_path = cache_dir
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| meta_dir.join("cache"));

    // Structure: base/uuid/chunks
    // DiskCache will append hashed subdirectories (e.g., 0/0/) to mirror S3.
    let cache_path = base_cache_path
        .join(format_info.uuid.to_string())
        .join("chunks");
    
    let disk_cache = Arc::new(DiskCache::new(&cache_path, cache_size_bytes)?);

    // Create VFS
    let vfs = Arc::new(StudioVfs::new(
        meta.clone(),
        data.clone(),
        mem_cache.clone(),
        disk_cache.clone(),
    ));

    println!("✓ Volume '{}' ready to mount at '{}'", name, mount_point);
    println!("  Memory limit: {}", memory_limit);
    println!("  Cache size:   {}", cache_size);

    // Create a tokio runtime for async ops in FUSE callbacks
    let rt = Arc::new(
        tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("failed to create tokio runtime: {e}"))?,
    );

    // Spawn background tasks
    {
        let vfs = vfs.clone();
        let _guard = rt.enter();
        vfs.spawn_background_uploader();
    }

    // Start platform-specific FUSE mount
    #[cfg(windows)]
    {
        info!("starting WinFSP mount at {mount_point}");
        studiofs_winfsp::mount_and_serve(vfs, rt, mount_point, &format_info.name)
            .map_err(|e| anyhow::anyhow!("WinFSP error: {e}"))?;
    }

    #[cfg(unix)]
    {
        info!("starting FUSE mount at {mount_point}");
        studiofs_fuse::mount_and_serve(vfs, rt, mount_point)
            .map_err(|e| anyhow::anyhow!("FUSE error: {e}"))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("100GB").unwrap(), 100 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("512MB").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_size("1TB").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(parse_size("1024").unwrap(), 1024);
    }
}

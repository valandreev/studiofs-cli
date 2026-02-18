//! `studiofs mount` — mount a filesystem volume.

use std::path::Path;
use std::sync::Arc;
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

    // DiskCache will append "objects" and "pending" subdirectories inside this UUID dir.
    let cache_path = base_cache_path
        .join(&format_info.uuid);
        
    let cache_path_str = cache_path.to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid cache path"))?;
    
    // Create a tokio runtime for async ops in FUSE callbacks and background tasks
    let rt = Arc::new(
        tokio::runtime::Runtime::new()
            .map_err(|e| anyhow::anyhow!("failed to create tokio runtime: {e}"))?,
    );
    let _guard = rt.enter();

    // Create VFS
    // StudioVfs::new returns VfsResult<Arc<Self>>
    let vfs = StudioVfs::new(
        meta.clone(),
        data.clone(),
        Some(mem_cache.clone()),
        cache_path_str,
        cache_size_bytes, // Pass the cache size
    ).map_err(|e| anyhow::anyhow!("failed to init vfs: {e}"))?;

    println!("✓ Volume '{}' ready to mount at '{}'", name, mount_point);
    println!("  Memory limit: {}", memory_limit);
    println!("  Cache size:   {}", cache_size);

    // Background tasks (uploader + GC) are spawned internally by StudioVfs::new

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

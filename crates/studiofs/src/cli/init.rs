//! `studiofs init` — initialize a new filesystem volume.

use std::path::Path;
use studiofs_meta::{FormatInfo, MetaEngine, RedbMetaEngine, BLOCK_SIZE, CHUNK_SIZE};
use tracing::info;
use uuid::Uuid;

pub fn run(
    name: &str,
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    meta_dir: &Path,
) -> anyhow::Result<()> {
    // Create metadata directory
    std::fs::create_dir_all(meta_dir)?;

    let db_path = meta_dir.join(format!("{name}.db"));
    info!("initializing volume {name:?} at {db_path:?}");

    let engine = RedbMetaEngine::open(&db_path)?;

    // Store format info
    let format_info = FormatInfo {
        name: name.to_string(),
        uuid: Uuid::new_v4().to_string(),
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
        region: region.to_string(),
        block_size: BLOCK_SIZE,
        chunk_size: CHUNK_SIZE,
    };
    engine.set_format_info(&format_info)?;

    // Initialize root inode
    engine.init_root()?;

    info!("volume {name:?} initialized successfully");
    println!("✓ Volume '{}' initialized", name);
    println!("  UUID:     {}", format_info.uuid);
    println!("  Endpoint: {}", endpoint);
    println!("  Bucket:   {}", bucket);
    println!("  Metadata: {:?}", db_path);
    println!();
    println!("Run `studiofs mount {} <mount-point>` to mount.", name);

    Ok(())
}

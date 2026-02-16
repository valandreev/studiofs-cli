//! `studiofs destroy` — permanently delete a volume.

use std::path::Path;
use std::io::Write;
use tracing::warn;
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{MetaEngine, RedbMetaEngine};

pub fn run(name: &str, meta_dir: &Path) -> anyhow::Result<()> {
    // 1. Check if volume exists (metadata DB)
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        warn!("volume metadata not found at {:?}", db_path);
        return Err(anyhow::anyhow!("Volume '{}' not found (metadata missing)", name));
    }

    // 2. Open metadata to get S3 credentials
    // We only need read access to format info
    let meta = RedbMetaEngine::open(&db_path)?;
    let format_info = meta.get_format_info()?;
    
    // Close DB so we can delete it later
    drop(meta);

    // 3. Confirm with user
    println!("WARNING: You are about to DESTROY volume '{}'.", name);
    println!("This will permanently delete:");
    println!("  1. All data in S3 bucket '{}' with prefix '{}/'", format_info.bucket, name);
    println!("  2. ALL local metadata and cache at {:?}", meta_dir);
    println!();
    
    print!("Do you want to proceed? [y/N]: ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    if input.trim().to_lowercase() != "y" {
        println!("Aborting.");
        return Ok(());
    }

    print!("To confirm, please type the volume name '{}' again: ", name);
    std::io::stdout().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    
    if input.trim() != name {
        println!("Confirmation failed. Aborting.");
        return Ok(());
    }

    // 4. Initialize DataStore and delete S3 data
    println!("Deleting data from S3...");
    let data_config = DataStoreConfig::from(&format_info);
    
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let ds = DataStore::new(&data_config)?;
        ds.delete_volume_data().await
    })?;
    println!("✓ S3 data deleted.");

    // 5. Delete metadata directory
    println!("Deleting local metadata and cache...");
    if meta_dir.exists() {
        std::fs::remove_dir_all(meta_dir)?;
    }

    println!("✓ Local files deleted.");
    println!("Volume '{}' destroyed successfully.", name);

    Ok(())
}

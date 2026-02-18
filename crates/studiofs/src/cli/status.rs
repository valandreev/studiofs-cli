//! `studiofs status` — show filesystem status.

use std::path::Path;
use studiofs_meta::{MetaEngine, RedbMetaEngine, counters};

pub fn run(name: &str, meta_dir: &Path) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));

    if !db_path.exists() {
        println!("✗ Volume '{}' not found at {:?}", name, db_path);
        println!("  Run `studiofs init {}` first.", name);
        return Ok(());
    }

    let engine = RedbMetaEngine::open(&db_path)?;
    let format_info = engine.get_format_info()?;

    let total_inodes = engine.get_counter(counters::TOTAL_INODES)?;
    let used_space = engine.get_counter(counters::USED_SPACE)?;
    let next_inode = engine.get_counter(counters::NEXT_INODE)?;
    let next_block = engine.get_counter(counters::NEXT_BLOCK_ID)?;

    println!("StudioFS Volume Status");
    println!("──────────────────────");
    println!("  Name:       {}", format_info.name);
    println!("  UUID:       {}", format_info.uuid);
    println!("  Endpoint:   {}", format_info.endpoint);
    println!("  Bucket:     {}", format_info.bucket);
    println!("  Block size: {} MiB", format_info.block_size / (1024 * 1024));
    println!("  Chunk size: {} MiB", format_info.chunk_size / (1024 * 1024));
    println!();
    println!("  Inodes:     {}", total_inodes);
    println!("  Used space: {} bytes", used_space);
    println!("  Next inode: {}", next_inode);
    println!("  Next block: {}", next_block);
    println!("  Metadata:   {:?}", db_path);

    Ok(())
}

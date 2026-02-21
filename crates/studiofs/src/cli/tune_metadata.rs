//! `studiofs tune-metadata` — adjust Tonbo metadata runtime tuning.

use std::path::Path;
use studiofs_meta::{MetaEngine, RedbMetaEngine};

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    meta_dir: &Path,
    cache_ttl_secs: Option<u64>,
    flush_interval_secs: Option<u64>,
    flush_ops_threshold: Option<u32>,
    flush_bytes_threshold: Option<u64>,
) -> anyhow::Result<()> {
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        return Err(anyhow::anyhow!(
            "Volume '{}' not found at {:?}",
            name,
            db_path
        ));
    }

    if cache_ttl_secs.is_none()
        && flush_interval_secs.is_none()
        && flush_ops_threshold.is_none()
        && flush_bytes_threshold.is_none()
    {
        return Err(anyhow::anyhow!(
            "No tuning values provided. Set at least one of \
             --cache-ttl-secs, --flush-interval-secs, --flush-ops-threshold, --flush-bytes-threshold."
        ));
    }

    let engine = RedbMetaEngine::open(&db_path)?;
    let mut format_info = engine.get_format_info()?;

    let old_ttl = format_info.metadata.cache_ttl_secs;
    let old_flush_interval = format_info.metadata.flush_interval_secs;
    let old_ops = format_info.metadata.flush_ops_threshold;
    let old_bytes = format_info.metadata.flush_bytes_threshold;

    if let Some(v) = cache_ttl_secs {
        format_info.metadata.cache_ttl_secs = v.max(1);
    }
    if let Some(v) = flush_interval_secs {
        format_info.metadata.flush_interval_secs = v.max(1);
    }
    if let Some(v) = flush_ops_threshold {
        format_info.metadata.flush_ops_threshold = v.max(1);
    }
    if let Some(v) = flush_bytes_threshold {
        format_info.metadata.flush_bytes_threshold = v.max(1);
    }

    engine.set_format_info(&format_info)?;

    println!("✓ Metadata tuning updated for '{}'", name);
    println!(
        "  cache_ttl_secs:        {} -> {}",
        old_ttl, format_info.metadata.cache_ttl_secs
    );
    println!(
        "  flush_interval_secs:   {} -> {}",
        old_flush_interval, format_info.metadata.flush_interval_secs
    );
    println!(
        "  flush_ops_threshold:   {} -> {}",
        old_ops, format_info.metadata.flush_ops_threshold
    );
    println!(
        "  flush_bytes_threshold: {} -> {}",
        old_bytes, format_info.metadata.flush_bytes_threshold
    );

    Ok(())
}

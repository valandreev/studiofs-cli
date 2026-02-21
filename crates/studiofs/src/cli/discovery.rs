//! `studiofs discover` — attach local metadata cache to an existing Tonbo volume.

use std::io::Write;
use std::path::Path;

use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{
    DEFAULT_BLOCK_SIZE, FormatInfo, MetaEngine, MetadataBackend, MetadataConfig, RedbMetaEngine,
    RemoteStoreConfig, TonboMetaEngine,
};

fn verify_store(label: &str, cfg: &DataStoreConfig) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let ds = DataStore::new(cfg)?;
        ds.check()
            .await
            .map_err(|e| anyhow::anyhow!("{} check failed: {}", label, e))
    })
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    meta_dir: &Path,
) -> anyhow::Result<()> {
    println!("🔍 Verifying metadata store access...");
    let metadata_cfg = DataStoreConfig {
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
        region: region.to_string(),
        volume_name: name.to_string(),
    };
    verify_store("metadata", &metadata_cfg)?;
    println!("✅ Metadata store is accessible.");

    if meta_dir.exists() {
        println!("! Metadata directory already exists: {:?}", meta_dir);
        print!("  Do you want to REPLACE local metadata cache? [y/N]: ");
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();
        if input == "y" || input == "yes" {
            std::fs::remove_dir_all(meta_dir)?;
        } else {
            return Err(anyhow::anyhow!("Discovery cancelled."));
        }
    }

    std::fs::create_dir_all(meta_dir)?;
    let db_path = meta_dir.join(format!("{name}.db"));

    let redb = RedbMetaEngine::open(&db_path)?;
    let metadata_prefix = format!("{name}/metadata/");
    let data_prefix = format!("{name}/objects/");
    // Bootstrap minimal info so Tonbo engine can locate manifest by disk name.
    redb.set_format_info(&FormatInfo {
        name: name.to_string(),
        uuid: String::new(),
        endpoint: endpoint.to_string(),
        bucket: bucket.to_string(),
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
        region: region.to_string(),
        data_prefix,
        metadata: MetadataConfig {
            backend: MetadataBackend::Tonbo,
            remote: Some(RemoteStoreConfig {
                endpoint: endpoint.to_string(),
                bucket: bucket.to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                region: region.to_string(),
                prefix: metadata_prefix.to_string(),
            }),
            ..MetadataConfig::default()
        },
        version: String::new(),
        block_size: DEFAULT_BLOCK_SIZE,
        chunk_size: 0,
    })?;
    // Root is only needed when remote manifest is not yet present.
    redb.init_root(0, 0)?;
    drop(redb);

    let tonbo = TonboMetaEngine::open(&db_path)?;
    let info = tonbo.get_format_info().map_err(|e| {
        anyhow::anyhow!("failed to load remote metadata for disk '{}': {}", name, e)
    })?;
    if info.uuid.is_empty() {
        return Err(anyhow::anyhow!(
            "remote metadata manifest for disk '{}' not found under prefix '{}'",
            name,
            metadata_prefix
        ));
    }

    println!("✓ Volume '{}' discovered", name);
    println!("  UUID:      {}", info.uuid);
    println!("  Data:      {} / {}", info.endpoint, info.bucket);
    if let Some(remote) = &info.metadata.remote {
        println!("  Metadata:  {} / {}", remote.endpoint, remote.bucket);
        println!("  Prefix:    {}", remote.prefix);
    }
    println!("  Local DB:  {:?}", db_path);
    println!();
    println!("Run `studiofs mount {} <mount-point>` to mount.", name);

    Ok(())
}

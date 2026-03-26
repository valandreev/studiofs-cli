//! `studiofs destroy` — permanently delete a volume.

use std::io::Write;
use std::path::Path;
use studiofs_data::{DataStore, DataStoreConfig};
use tracing::warn;

pub fn run(name: &str, meta_dir: &Path, local_only: bool) -> anyhow::Result<()> {
    // 1. Check if volume exists (metadata DB)
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        warn!("volume metadata not found at {:?}", db_path);
        return Err(anyhow::anyhow!(
            "Volume '{}' not found (metadata missing)",
            name
        ));
    }

    // 2. Open metadata to get S3 credentials
    // We only need read access to format info
    let live_meta = studiofs_meta::open_meta_engine(&db_path)?;
    let format_info = live_meta.get_format_info()?;

    // 2.1 Validate UUID against remote store if applicable (SFS-95)
    if !local_only && let Err(e) = live_meta.check_consistency() {
        let cleanup = super::utils::format_cleanup_command(name);
        match e {
            studiofs_meta::engine::MetaError::UuidMismatch { local, remote } => {
                println!(
                    "Warning: Local disk UUID does not match the remote database. You might be trying to destroy a recreated disk that you haven't discovered yet."
                );
                println!("Remote UUID: {remote}");
                println!("Local UUID:  {local}");
                println!("\nDo you want to only remove the local stale configuration?");
                println!("Suggested command: {cleanup}");
                return Ok(());
            }
            _ => {
                println!(
                    "Warning: Could not verify disk identity (remote database is unreachable)."
                );
                println!(
                    "If you want to remove the local configuration and metadata only, use the --local flag."
                );
                println!("Suggested command: {cleanup}");
                return Ok(());
            }
        }
    }

    // Close DB so we can delete it later
    drop(live_meta);

    // 3. Confirm with user
    println!("WARNING: You are about to DESTROY volume '{}'.", name);
    if local_only {
        println!("This will permanently delete local metadata only:");
        println!("  1. Local metadata files at {:?}", meta_dir);
        println!("Remote data and remote metadata will be kept.");
    } else {
        println!("This will permanently delete:");
        println!(
            "  1. All data in S3 bucket '{}' with prefix '{}/'",
            format_info.bucket, name
        );
        println!("  2. Local metadata files at {:?}", meta_dir);
    }
    println!(
        "NOTE: Custom local data cache directories are not auto-detected and will NOT be removed."
    );
    println!(
        "After destroy, remove local cache manually for volume UUID '{}'.",
        format_info.uuid
    );
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

    // 4. Initialize DataStore and delete S3 data (unless local-only)
    if !local_only {
        println!("Deleting data from S3...");
        let data_config = DataStoreConfig::from(&format_info);

        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let ds = DataStore::new(&data_config)?;
            ds.delete_volume_data().await
        })?;
        println!("✓ S3 data deleted.");

        if let Some(pg) = &format_info.metadata.postgres {
            println!("Deleting PostgreSQL metadata...");
            rt.block_on(async {
                let (client, connection) = tokio_postgres::connect(&pg.url, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| anyhow::anyhow!("cannot connect to postgres: {e}"))?;
                let conn_task = tokio::spawn(async move {
                    let _ = connection.await;
                });

                client
                    .execute("DELETE FROM volumes WHERE vol = $1", &[&pg.volume_id])
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to delete postgres metadata: {e}"))?;

                conn_task.abort();
                Ok::<(), anyhow::Error>(())
            })?;
            println!("✓ PostgreSQL metadata deleted.");
        }

        if let Some(remote) = &format_info.metadata.remote {
            let mut builder = opendal::services::S3::default()
                .endpoint(&remote.endpoint)
                .bucket(&remote.bucket)
                .access_key_id(&remote.access_key)
                .secret_access_key(&remote.secret_key)
                .disable_config_load()
                .disable_ec2_metadata();
            if !remote.region.is_empty() {
                builder = builder.region(&remote.region);
            } else {
                builder = builder.region("us-east-1");
            }
            let op = opendal::Operator::new(builder)?.finish();
            let prefix = if remote.prefix.ends_with('/') {
                remote.prefix.clone()
            } else {
                format!("{}/", remote.prefix)
            };
            let primary_manifest_path = format!("{}manifest.json", prefix);
            let _ = rt.block_on(op.delete(&primary_manifest_path));
            let legacy_manifest_by_name = format!("{}{}.manifest.json", prefix, format_info.name);
            let _ = rt.block_on(op.delete(&legacy_manifest_by_name));
            if !format_info.uuid.is_empty() && format_info.uuid != format_info.name {
                let legacy_manifest_by_uuid =
                    format!("{}{}.manifest.json", prefix, format_info.uuid);
                let _ = rt.block_on(op.delete(&legacy_manifest_by_uuid));
            }
        }
    }

    // 5. Delete metadata directory
    println!("Deleting local metadata files...");
    if meta_dir.exists() {
        std::fs::remove_dir_all(meta_dir)?;
    }

    println!("✓ Local metadata files deleted.");
    println!(
        "Reminder: remove custom local cache directories for volume UUID '{}'.",
        format_info.uuid
    );
    if local_only {
        println!("Volume '{}' detached locally.", name);
    } else {
        println!("Volume '{}' destroyed successfully.", name);
    }

    Ok(())
}

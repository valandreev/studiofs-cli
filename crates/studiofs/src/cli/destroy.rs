//! `studiofs destroy` — permanently delete a volume.

use crate::cli::cache_layout::describe_cache_layout_state;
use clap::Args;
use std::io::{self, Write};
use std::path::Path;
use studiofs_cache::runtime::{LocalPayloadRuntimeDetection, detect_cache_backends};
use studiofs_data::{DataStore, DataStoreConfig, store::DataError};
use studiofs_meta::connect_studiofs_postgres;
use studiofs_meta::engine::MetaError;
use thiserror::Error;
use tracing::warn;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DestroyMode {
    Full,
    LocalOnly,
}

impl DestroyMode {
    const fn should_remove_remote(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Debug, Error)]
pub enum DestroyError {
    #[error("volume metadata not found at {path}")]
    MetadataMissing { path: String },

    #[error("metadata operation failed: {0}")]
    Meta(#[from] MetaError),

    #[error("data store operation failed: {0}")]
    DataStore(#[from] DataError),

    #[error("postgres operation failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error(transparent)]
    PostgresConnection(MetaError),

    #[error("object store operation failed: {0}")]
    ObjectStore(#[from] opendal::Error),

    #[error("spawned async task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("input/output error: {0}")]
    Io(#[from] io::Error),
}

type DestroyResult<T> = std::result::Result<T, DestroyError>;

#[derive(Debug, Args)]
#[command(about = "Permanently destroy a volume")]
#[command(long_about = "\
Permanently destroy a volume, deleting all data objects from S3, removing the \
shared PostgreSQL metadata for that volume, and deleting local metadata/cache \
for this client.

WARNING: This action is irreversible.")]
pub(crate) struct DestroyArgs {
    /// Volume name to destroy
    name: String,

    /// Remove only local cache/config; keep remote metadata and data.
    #[arg(long)]
    local: bool,
}

impl DestroyArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let meta_dir = super::utils::get_default_meta_dir(&self.name)?;
        let mode = if self.local {
            DestroyMode::LocalOnly
        } else {
            DestroyMode::Full
        };
        run(&self.name, &meta_dir, mode)?;
        Ok(())
    }
}

fn describe_destroy_cache_scope(detection: Option<&LocalPayloadRuntimeDetection>) -> &'static str {
    if let Some(detection) = detection {
        if detection.runtime_backend().is_some() {
            "local cleanup removes the entire metadata directory (`meta_dir`), including the default cache root and its active UUID-scoped local-cache runtime"
        } else {
            "local cleanup removes the entire metadata directory (`meta_dir`), including any default cache-root contents under it"
        }
    } else {
        "local cleanup removes the entire metadata directory (`meta_dir`); local cache layout state could not be inspected"
    }
}

fn prompt_and_read_line(prompt: &str) -> DestroyResult<String> {
    print!("{}", prompt);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(input.trim().to_string())
}

pub fn run(name: &str, meta_dir: &Path, mode: DestroyMode) -> DestroyResult<()> {
    // 1. Check if volume exists (metadata DB)
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        warn!("volume metadata not found at {:?}", db_path);
        return Err(DestroyError::MetadataMissing {
            path: db_path.display().to_string(),
        });
    }

    // 2. Read the format locally.  Local-only cleanup must not open a
    // PostgreSQL-backed metadata engine or touch remote services.
    let (format_info, live_meta) = match mode {
        DestroyMode::LocalOnly => (
            studiofs_meta::RedbMetaEngine::read_format_info_readonly(&db_path)?,
            None,
        ),
        DestroyMode::Full => {
            let live_meta = studiofs_meta::open_meta_engine(&db_path)?;
            let format_info = live_meta.get_format_info()?;
            (format_info, Some(live_meta))
        }
    };
    let default_cache_root = meta_dir.join("cache").join(&format_info.uuid);
    let cache_backend_detection = match detect_cache_backends(&default_cache_root) {
        Ok(detection) => Some(detection),
        Err(error) => {
            println!(
                "Warning: Failed to inspect local cache layout state at '{}': {error}",
                default_cache_root.display()
            );
            None
        }
    };

    // 2.1 Validate UUID against remote store if applicable (SFS-95)
    if mode.should_remove_remote()
        && let Some(live_meta) = live_meta.as_ref()
        && let Err(e) = live_meta.check_consistency()
    {
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
    if mode == DestroyMode::LocalOnly {
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
    println!("  Default cache root: {}", default_cache_root.display());
    println!(
        "  Cache layout state: {}",
        describe_cache_layout_state(cache_backend_detection.as_ref())
    );
    if let Some(detection) = cache_backend_detection.as_ref() {
        println!(
            "  Local cache index: {} ({})",
            detection.layout.redb_index_path.display(),
            if detection.index_exists {
                "present"
            } else {
                "absent"
            }
        );
        println!(
            "  Local cache upload: {} ({})",
            detection.layout.upload_stage_objects_dir.display(),
            if detection.upload_objects_dir_exists {
                "present"
            } else {
                "absent"
            }
        );
    }
    println!(
        "  Cleanup scope: {}.",
        describe_destroy_cache_scope(cache_backend_detection.as_ref())
    );
    println!(
        "NOTE: Custom local data cache directories are not auto-detected and will NOT be removed."
    );
    println!("WARNING: If this volume used a non-default cache_dir, remove that path manually.");
    println!(
        "After destroy, remove local cache manually for volume UUID '{}'.",
        format_info.uuid
    );
    println!();

    if !prompt_and_read_line("Do you want to proceed? [y/N]: ")?.eq_ignore_ascii_case("y") {
        println!("Aborting.");
        return Ok(());
    }

    let confirm_name = prompt_and_read_line(&format!(
        "To confirm, please type the volume name '{}' again: ",
        name
    ))?;
    if confirm_name != name {
        println!("Confirmation failed. Aborting.");
        return Ok(());
    }

    if mode.should_remove_remote() {
        println!("Deleting data from S3 and metadata backends...");
        crate::runtime_bridge::block_on(delete_remote_state(&format_info))?;
        println!("✓ Remote metadata and data deleted.");
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
    if mode == DestroyMode::LocalOnly {
        println!("Volume '{}' detached locally.", name);
    } else {
        println!("Volume '{}' destroyed successfully.", name);
    }

    Ok(())
}

async fn delete_remote_state(format_info: &studiofs_meta::FormatInfo) -> DestroyResult<()> {
    let data_config = DataStoreConfig::from(format_info);
    let ds = DataStore::new(&data_config)?;
    ds.delete_volume_data().await?;
    println!("✓ S3 data deleted.");

    if let Some(pg) = &format_info.metadata.postgres {
        println!("Deleting PostgreSQL metadata...");
        delete_postgres_metadata(&pg.url, pg.volume_id).await?;
        println!("✓ PostgreSQL metadata deleted.");
    }

    if let Some(remote) = &format_info.metadata.remote {
        delete_remote_manifests(remote, &format_info.name, &format_info.uuid).await?;
    }

    Ok(())
}

async fn delete_postgres_metadata(pg_url: &str, volume_id: i64) -> DestroyResult<()> {
    let (client, connection) = connect_studiofs_postgres(pg_url)
        .await
        .map_err(DestroyError::PostgresConnection)?;

    let connection = tokio::spawn(async move { connection.await.map_err(DestroyError::Postgres) });

    let delete_result = client
        .execute("DELETE FROM volumes WHERE vol = $1", &[&volume_id])
        .await
        .map_err(DestroyError::Postgres)?;
    drop(client);

    match connection.await {
        Ok(result) => result?,
        Err(error) if error.is_cancelled() => {}
        Err(error) => return Err(DestroyError::Join(error)),
    }

    tracing::debug!("Deleted postgres rows: {delete_result}");

    Ok(())
}

async fn delete_remote_manifests(
    remote: &studiofs_meta::RemoteStoreConfig,
    volume_name: &str,
    volume_uuid: &str,
) -> DestroyResult<()> {
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
    delete_optional_manifest(&op, &primary_manifest_path).await;

    let legacy_manifest_by_name = format!("{}{}.manifest.json", prefix, volume_name);
    delete_optional_manifest(&op, &legacy_manifest_by_name).await;

    if !volume_uuid.is_empty() && volume_uuid != volume_name {
        let legacy_manifest_by_uuid = format!("{}{}.manifest.json", prefix, volume_uuid);
        delete_optional_manifest(&op, &legacy_manifest_by_uuid).await;
    }

    Ok(())
}

async fn delete_optional_manifest(operator: &opendal::Operator, path: &str) {
    if let Err(error) = operator.delete(path).await {
        warn!(%path, "failed to delete optional remote manifest (ignored)");
        warn!("{error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_cache::runtime::LocalPayloadLayout;

    fn detection(has_layout: bool) -> LocalPayloadRuntimeDetection {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        LocalPayloadRuntimeDetection {
            layout,
            index_exists: false,
            payload_objects_dir_exists: has_layout,
            upload_objects_dir_exists: has_layout,
            tmp_dir_exists: has_layout,
            index_dir_exists: has_layout,
        }
    }

    fn partial_detection() -> LocalPayloadRuntimeDetection {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let layout = LocalPayloadLayout::from_cache_root(temp.path());
        LocalPayloadRuntimeDetection {
            layout,
            index_exists: false,
            payload_objects_dir_exists: false,
            upload_objects_dir_exists: false,
            tmp_dir_exists: false,
            index_dir_exists: true,
        }
    }

    #[test]
    fn destroy_cache_layout_state_labels_cover_layouts() {
        assert_eq!(describe_cache_layout_state(None), "unknown");
        assert_eq!(
            describe_cache_layout_state(Some(&detection(false))),
            "Local Payload Runtime not initialized"
        );
        assert_eq!(
            describe_cache_layout_state(Some(&detection(true))),
            "Local Payload Runtime ready"
        );
        assert_eq!(
            describe_cache_layout_state(Some(&partial_detection())),
            "Local Payload Runtime incomplete"
        );
    }

    #[test]
    fn destroy_cleanup_scope_is_layout_aware() {
        assert_eq!(
            describe_destroy_cache_scope(Some(&detection(true))),
            "local cleanup removes the entire metadata directory (`meta_dir`), including the default cache root and its active UUID-scoped local-cache runtime"
        );
        assert_eq!(
            describe_destroy_cache_scope(Some(&detection(false))),
            "local cleanup removes the entire metadata directory (`meta_dir`), including any default cache-root contents under it"
        );
        assert_eq!(
            describe_destroy_cache_scope(Some(&partial_detection())),
            "local cleanup removes the entire metadata directory (`meta_dir`), including any default cache-root contents under it"
        );
        assert_eq!(
            describe_destroy_cache_scope(None),
            "local cleanup removes the entire metadata directory (`meta_dir`); local cache layout state could not be inspected"
        );
    }

    #[test]
    fn destroy_parses_local_flag() {
        let cli = crate::cli::parse_for_test(&["destroy", "vol", "--local"]).unwrap();
        match cli.command {
            crate::cli::Commands::Destroy(DestroyArgs { name, local }) => {
                assert_eq!(name, "vol");
                assert!(local);
            }
            other => panic!("expected Destroy, got {other:?}"),
        }
    }
}

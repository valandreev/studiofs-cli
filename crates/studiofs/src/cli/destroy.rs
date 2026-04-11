//! `studiofs destroy` — permanently delete a volume.

use crate::cli::cache_layout::describe_cache_layout_state;
use std::future::Future;
use std::io::{self, Write};
use std::path::Path;
use studiofs_cache::v3::{CacheBackendDetection, detect_cache_backends};
use studiofs_data::{DataStore, DataStoreConfig, store::DataError};
use studiofs_meta::engine::MetaError;
use thiserror::Error;
use tokio::runtime::RuntimeFlavor;
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

    #[error("object store operation failed: {0}")]
    ObjectStore(#[from] opendal::Error),

    #[error("spawned async task failed: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("input/output error: {0}")]
    Io(#[from] io::Error),
}

type DestroyResult<T> = std::result::Result<T, DestroyError>;

fn describe_destroy_cache_scope(detection: Option<&CacheBackendDetection>) -> &'static str {
    if let Some(detection) = detection {
        if detection.has_cache_v3_contract() {
            "local cleanup removes the entire metadata directory (`meta_dir`), including the default cache root and its Cache 3.0 subtree"
        } else {
            "local cleanup removes the entire metadata directory (`meta_dir`), including any default cache-root contents under it"
        }
    } else {
        "local cleanup removes the entire metadata directory (`meta_dir`); Cache 3.0 layout state could not be inspected"
    }
}

fn prompt_and_read_line(prompt: &str) -> DestroyResult<String> {
    print!("{}", prompt);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(input.trim().to_string())
}

fn run_destroy_future<T, F>(future: F) -> DestroyResult<T>
where
    T: Send,
    F: Future<Output = DestroyResult<T>> + Send,
{
    fn run_on_new_runtime<T, F>(future: F) -> DestroyResult<T>
    where
        T: Send,
        F: Future<Output = DestroyResult<T>> + Send,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(future)
    }

    fn run_on_scoped_thread<T, F>(future: F) -> DestroyResult<T>
    where
        T: Send,
        F: Future<Output = DestroyResult<T>> + Send,
    {
        std::thread::scope(|scope| {
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            scope.spawn(move || {
                let _ = tx.send(run_on_new_runtime(future));
            });
            rx.recv().map_err(|error| {
                DestroyError::Io(io::Error::other(format!(
                    "failed to receive async destroy result: {error}"
                )))
            })?
        })
    }

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return match handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(future)),
            RuntimeFlavor::CurrentThread => run_on_scoped_thread(future),
            _ => run_on_scoped_thread(future),
        };
    }

    run_on_new_runtime(future)
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

    // 2. Open metadata to get S3 credentials
    // We only need read access to format info
    let live_meta = studiofs_meta::open_meta_engine(&db_path)?;
    let format_info = live_meta.get_format_info()?;
    let default_cache_root = meta_dir.join("cache").join(&format_info.uuid);
    let cache_backend_detection = match detect_cache_backends(&default_cache_root) {
        Ok(detection) => Some(detection),
        Err(error) => {
            println!(
                "Warning: Failed to inspect Cache 3.0 layout state at '{}': {error}",
                default_cache_root.display()
            );
            None
        }
    };

    // 2.1 Validate UUID against remote store if applicable (SFS-95)
    if mode.should_remove_remote()
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
            "  Cache 3.0 marker: {} ({})",
            detection.layout.layout_marker.display(),
            if detection.has_cache_v3_contract() {
                "present"
            } else {
                "absent"
            }
        );
        println!(
            "  Cache 3.0 index: {} ({})",
            detection.layout.redb_index_path.display(),
            if detection.v3_index_exists {
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
        run_destroy_future(delete_remote_state(&format_info))?;
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
    let (client, connection) = tokio_postgres::connect(pg_url, tokio_postgres::NoTls).await?;

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
    use studiofs_cache::v3::CacheV3Layout;

    fn detection(marker: bool) -> CacheBackendDetection {
        let temp = tempfile::tempdir().unwrap();
        let layout = CacheV3Layout::from_cache_root(temp.path());
        CacheBackendDetection {
            layout,
            v3_layout_marker_exists: marker,
            v3_index_exists: false,
            v3_payload_blocks_dir_exists: false,
        }
    }

    #[test]
    fn destroy_cache_layout_state_labels_cover_layouts() {
        assert_eq!(describe_cache_layout_state(None), "unknown");
        assert_eq!(
            describe_cache_layout_state(Some(&detection(false))),
            "Cache 3.0 not initialized"
        );
        assert_eq!(
            describe_cache_layout_state(Some(&detection(true))),
            "Cache 3.0 ready"
        );
    }

    #[test]
    fn destroy_cleanup_scope_is_layout_aware() {
        assert_eq!(
            describe_destroy_cache_scope(Some(&detection(true))),
            "local cleanup removes the entire metadata directory (`meta_dir`), including the default cache root and its Cache 3.0 subtree"
        );
        assert_eq!(
            describe_destroy_cache_scope(Some(&detection(false))),
            "local cleanup removes the entire metadata directory (`meta_dir`), including any default cache-root contents under it"
        );
        assert_eq!(
            describe_destroy_cache_scope(None),
            "local cleanup removes the entire metadata directory (`meta_dir`); Cache 3.0 layout state could not be inspected"
        );
    }
}

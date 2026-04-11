//! `studiofs snapshot` — create/list immutable metadata snapshots.

use std::path::{Path, PathBuf};
use studiofs_meta::{ROOT_INO, open_meta_engine};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("local metadata DB not found: {path}")]
    LocalDbNotFound { path: PathBuf },

    #[error(transparent)]
    Meta(#[from] studiofs_meta::MetaError),
}

pub type SnapshotResult<T> = std::result::Result<T, SnapshotError>;

pub fn create(name: &str, meta_dir: &Path, root_ino: Option<u64>) -> SnapshotResult<()> {
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        return Err(SnapshotError::LocalDbNotFound { path: db_path });
    }

    let engine = open_meta_engine(&db_path)?;
    let root = root_ino.unwrap_or(ROOT_INO);
    let snapshot = engine.create_snapshot(root)?;

    println!("✓ Snapshot created");
    println!("  ID: {}", snapshot.id);
    println!("  Root inode: {}", snapshot.root_ino);
    println!("  State: {:?}", snapshot.state);
    println!("  Pinned objects: {}", snapshot.pinned_object_ids.len());
    Ok(())
}

pub fn list(name: &str, meta_dir: &Path) -> SnapshotResult<()> {
    let db_path = meta_dir.join(format!("{name}.db"));
    if !db_path.exists() {
        return Err(SnapshotError::LocalDbNotFound { path: db_path });
    }

    let engine = open_meta_engine(&db_path)?;
    let snapshots = engine.list_snapshots()?;
    if snapshots.is_empty() {
        println!("No snapshots found for '{}'.", name);
        return Ok(());
    }

    println!("Snapshots for '{}':", name);
    println!(
        "  {:<36} {:<8} {:<10} {:<8}",
        "ID", "ROOT", "STATE", "PINNED"
    );
    for snapshot in snapshots {
        println!(
            "  {:<36} {:<8} {:<10?} {:<8}",
            snapshot.id,
            snapshot.root_ino,
            snapshot.state,
            snapshot.pinned_object_ids.len()
        );
    }
    Ok(())
}

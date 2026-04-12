//! `studiofs snapshot` — create/list immutable metadata snapshots.

use clap::{Args, Subcommand};
use std::path::{Path, PathBuf};
use studiofs_meta::{ROOT_INO, open_meta_engine};
use thiserror::Error;

#[derive(Debug, Args)]
#[command(about = "Create or list immutable metadata snapshots")]
pub(crate) struct SnapshotArgs {
    /// Volume name
    name: String,

    #[command(subcommand)]
    action: SnapshotAction,
}

impl SnapshotArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        let meta_dir = super::utils::get_default_meta_dir(&self.name)?;
        match self.action {
            SnapshotAction::Create { root_ino } => create(&self.name, &meta_dir, root_ino)?,
            SnapshotAction::List => list(&self.name, &meta_dir)?,
        }
        Ok(())
    }
}

#[derive(Debug, Subcommand)]
enum SnapshotAction {
    /// Create a new snapshot
    Create {
        /// Optional custom root inode (default: root inode)
        #[arg(long)]
        root_ino: Option<u64>,
    },
    /// List known snapshots
    List,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_create_parses_root_inode() {
        let cli =
            crate::cli::parse_for_test(&["snapshot", "vol", "create", "--root-ino", "7"]).unwrap();

        match cli.command {
            crate::cli::Commands::Snapshot(SnapshotArgs {
                name,
                action: SnapshotAction::Create { root_ino },
            }) => {
                assert_eq!(name, "vol");
                assert_eq!(root_ino, Some(7));
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_list_parses() {
        let cli = crate::cli::parse_for_test(&["snapshot", "vol", "list"]).unwrap();

        match cli.command {
            crate::cli::Commands::Snapshot(SnapshotArgs {
                name,
                action: SnapshotAction::List,
            }) => {
                assert_eq!(name, "vol");
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }
}

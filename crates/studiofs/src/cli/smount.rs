//! `studiofs smount` — mount one verified snapshot read-only.

use clap::Args;
use std::path::PathBuf;
use studiofs_meta::snapshot::SnapshotId;

#[derive(Debug, Clone, Args)]
#[command(about = "Mount a verified snapshot as a read-only filesystem")]
#[command(after_help = "\
EXAMPLE:
  studiofs smount myvolume myvolume.2026-08-12T14-07-09.482Z /mnt/snapshot

The snapshot must already be in the Ready lifecycle state. The mount owns a
renewable PostgreSQL lease for its lifetime and rejects all mutations.")]
pub(crate) struct SmountArgs {
    /// Volume name used to locate the local metadata context.
    volume: String,

    /// PostgreSQL-allocated snapshot identifier.
    #[arg(value_parser = SnapshotId::parse)]
    snapshot_id: SnapshotId,

    /// Mount point: drive letter on Windows or directory path on Unix.
    mount_point: String,

    /// Disk cache directory used for the snapshot mount.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

impl SmountArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        #[cfg(windows)]
        crate::cli::winfsp_bootstrap::ensure_mountmgr_bootstrap()?;

        let command = crate::cli::mount::MountCommand::from_snapshot_args(
            self.volume,
            self.snapshot_id,
            self.mount_point,
            self.cache_dir,
        )?;
        crate::cli::mount::MountSession::prepare_snapshot(command)?.launch()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smount_accepts_the_production_three_positional_form() {
        let cli = crate::cli::parse_for_test(&[
            "smount",
            "myvolume",
            "myvolume.2026-08-12T14-07-09.482Z",
            "/mnt/snapshot",
        ])
        .expect("smount should parse");

        match cli.command {
            crate::cli::Commands::Smount(SmountArgs {
                volume,
                snapshot_id,
                mount_point,
                cache_dir,
            }) => {
                assert_eq!(volume, "myvolume");
                assert_eq!(snapshot_id.as_str(), "myvolume.2026-08-12T14-07-09.482Z");
                assert_eq!(mount_point, "/mnt/snapshot");
                assert_eq!(cache_dir, None);
            }
            other => panic!("expected Smount command, got {other:?}"),
        }
    }

    #[test]
    fn smount_accepts_an_explicit_cache_directory() {
        let cli = crate::cli::parse_for_test(&[
            "smount",
            "myvolume",
            "myvolume.2026-08-12T14-07-09.482Z",
            "/mnt/snapshot",
            "--cache-dir",
            "/tmp/studiofs-snapshot-cache",
        ])
        .expect("smount should parse an explicit cache directory");

        match cli.command {
            crate::cli::Commands::Smount(SmountArgs { cache_dir, .. }) => {
                assert_eq!(
                    cache_dir,
                    Some(PathBuf::from("/tmp/studiofs-snapshot-cache"))
                );
            }
            other => panic!("expected smount command, got {other:?}"),
        }
    }

    #[test]
    fn smount_rejects_writable_mode_and_extra_positionals() {
        for args in [
            vec![
                "smount",
                "myvolume",
                "myvolume.2026-08-12T14-07-09.482Z",
                "/mnt/snapshot",
                "--read-only",
            ],
            vec![
                "smount",
                "myvolume",
                "myvolume.2026-08-12T14-07-09.482Z",
                "/mnt/snapshot",
                "extra",
            ],
        ] {
            assert!(
                crate::cli::parse_for_test(&args).is_err(),
                "invalid smount form should be rejected: {args:?}"
            );
        }
    }
}

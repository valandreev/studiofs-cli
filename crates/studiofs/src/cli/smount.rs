//! `studiofs smount` — mount one verified snapshot read-only.

use clap::Args;
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
}

impl SmountArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        #[cfg(windows)]
        crate::cli::winfsp_bootstrap::ensure_mountmgr_bootstrap()?;

        let command = crate::cli::mount::MountCommand::from_snapshot_args(
            self.volume,
            self.snapshot_id,
            self.mount_point,
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
            }) => {
                assert_eq!(volume, "myvolume");
                assert_eq!(snapshot_id.as_str(), "myvolume.2026-08-12T14-07-09.482Z");
                assert_eq!(mount_point, "/mnt/snapshot");
            }
            other => panic!("expected Smount command, got {other:?}"),
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

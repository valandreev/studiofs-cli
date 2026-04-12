//! Hidden internal CLI subcommands.

use clap::{Args, Subcommand};

#[derive(Debug, Args)]
#[command(hide = true)]
pub(crate) struct InternalArgs {
    #[command(subcommand)]
    action: InternalAction,
}

impl InternalArgs {
    pub(crate) fn dispatch(self) -> anyhow::Result<()> {
        match self.action {
            InternalAction::WinfspBootstrapMountmgr => {
                #[cfg(windows)]
                {
                    super::winfsp_bootstrap::run_internal_bootstrap_mountmgr()?;
                }
                #[cfg(not(windows))]
                {
                    anyhow::bail!("internal WinFSP bootstrap is only supported on Windows");
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Subcommand)]
enum InternalAction {
    #[command(name = "winfsp-bootstrap-mountmgr", hide = true)]
    WinfspBootstrapMountmgr,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hidden_internal_bootstrap_subcommand_parses() {
        let cli = crate::cli::parse_for_test(&["internal", "winfsp-bootstrap-mountmgr"]).unwrap();
        assert!(matches!(
            cli.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::WinfspBootstrapMountmgr
            })
        ));
    }
}

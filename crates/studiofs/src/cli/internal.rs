//! Hidden internal CLI subcommands.

use crate::diagnostics;
use clap::{Args, Subcommand};
use serde_json::json;
use std::time::Duration;

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
                    Ok(super::winfsp_bootstrap::run_internal_bootstrap_mountmgr()?)
                }
                #[cfg(not(windows))]
                {
                    Err(anyhow::anyhow!(
                        "internal WinFSP bootstrap is only supported on Windows"
                    ))
                }
            }
            InternalAction::DiagnosticsDiscover => {
                let sessions = diagnostics::discover_local_sessions()?;
                println!("{}", serde_json::to_string_pretty(&sessions)?);
                Ok(())
            }
            InternalAction::DiagnosticsAttach(args) => run_diagnostics_attach(args),
        }
    }
}

#[derive(Debug, Subcommand)]
enum InternalAction {
    #[command(name = "winfsp-bootstrap-mountmgr", hide = true)]
    WinfspBootstrapMountmgr,
    #[command(name = "diagnostics-discover", hide = true)]
    DiagnosticsDiscover,
    #[command(name = "diagnostics-attach", hide = true)]
    DiagnosticsAttach(DiagnosticsAttachArgs),
}

#[derive(Debug, Args)]
struct DiagnosticsAttachArgs {
    mount_point: String,

    #[arg(long)]
    controller_client: Option<String>,

    #[arg(long)]
    renew_lease_id: Option<String>,

    #[arg(long, default_value_t = 5_000)]
    ttl_ms: u64,
}

fn run_diagnostics_attach(args: DiagnosticsAttachArgs) -> anyhow::Result<()> {
    let discovery = diagnostics::discover_local_sessions()?
        .into_iter()
        .find(|session| session.mount_point == args.mount_point)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no live diagnostics session found for '{}'",
                args.mount_point
            )
        })?;

    let client = diagnostics::LocalDiagnosticsClient::new(discovery);
    let ttl = Duration::from_millis(args.ttl_ms.max(1));
    let payload = crate::runtime_bridge::block_on(async move {
        let session = client.session_info().await?;
        let health = client.health().await?;
        let controller_lease = match (
            args.controller_client.as_deref(),
            args.renew_lease_id.as_deref(),
        ) {
            (_, Some(lease_id)) => Some(client.renew_controller(lease_id, ttl).await?),
            (Some(client_name), None) => Some(client.acquire_controller(client_name, ttl).await?),
            (None, None) => None,
        };
        Ok::<_, anyhow::Error>(json!({
            "session": session,
            "health": health,
            "controller_lease": controller_lease,
        }))
    })?;

    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
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

    #[test]
    fn hidden_internal_diagnostics_subcommands_parse() {
        let discover = crate::cli::parse_for_test(&["internal", "diagnostics-discover"]).unwrap();
        assert!(matches!(
            discover.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsDiscover
            })
        ));

        let attach = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-attach",
            "Z:",
            "--controller-client",
            "tester",
            "--ttl-ms",
            "250",
        ])
        .unwrap();
        assert!(matches!(
            attach.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsAttach(_)
            })
        ));
    }
}

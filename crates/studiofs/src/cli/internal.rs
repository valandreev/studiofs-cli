//! Hidden internal CLI subcommands.

use crate::diagnostics;
use crate::investigation::{
    InvestigationStore, NewTimelineEntry, TimelineEntryKind, TimelineEntrySource,
};
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
            InternalAction::DiagnosticsCreateCase(args) => run_diagnostics_create_case(args),
            InternalAction::DiagnosticsCreateRun(args) => run_diagnostics_create_run(args),
            InternalAction::DiagnosticsAddTimeline(args) => run_diagnostics_add_timeline(args),
            InternalAction::DiagnosticsReadCase(args) => run_diagnostics_read_case(args),
            InternalAction::DiagnosticsReadRun(args) => run_diagnostics_read_run(args),
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
    #[command(name = "diagnostics-create-case", hide = true)]
    DiagnosticsCreateCase(DiagnosticsCreateCaseArgs),
    #[command(name = "diagnostics-create-run", hide = true)]
    DiagnosticsCreateRun(DiagnosticsCreateRunArgs),
    #[command(name = "diagnostics-add-timeline", hide = true)]
    DiagnosticsAddTimeline(DiagnosticsAddTimelineArgs),
    #[command(name = "diagnostics-read-case", hide = true)]
    DiagnosticsReadCase(DiagnosticsReadCaseArgs),
    #[command(name = "diagnostics-read-run", hide = true)]
    DiagnosticsReadRun(DiagnosticsReadRunArgs),
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

    #[arg(long, default_value_t = 16)]
    recent_events_limit: usize,
}

#[derive(Debug, Args)]
struct DiagnosticsCreateCaseArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    title: String,

    #[arg(long)]
    problem_statement: Option<String>,

    #[arg(long)]
    workload_identity: Option<String>,

    #[arg(long)]
    target_environment: Option<String>,
}

#[derive(Debug, Args)]
struct DiagnosticsCreateRunArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    case_id: String,

    #[arg(long)]
    workload_identity: Option<String>,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum TimelineSourceArg {
    Cli,
    Mcp,
}

impl From<TimelineSourceArg> for TimelineEntrySource {
    fn from(value: TimelineSourceArg) -> Self {
        match value {
            TimelineSourceArg::Cli => TimelineEntrySource::Cli,
            TimelineSourceArg::Mcp => TimelineEntrySource::Mcp,
        }
    }
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum TimelineKindArg {
    Marker,
    Note,
}

impl From<TimelineKindArg> for TimelineEntryKind {
    fn from(value: TimelineKindArg) -> Self {
        match value {
            TimelineKindArg::Marker => TimelineEntryKind::Marker,
            TimelineKindArg::Note => TimelineEntryKind::Note,
        }
    }
}

#[derive(Debug, Args)]
struct DiagnosticsAddTimelineArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    run_id: String,

    #[arg(long, value_enum)]
    source: TimelineSourceArg,

    #[arg(long, value_enum)]
    kind: TimelineKindArg,

    #[arg(long)]
    title: String,

    #[arg(long)]
    body: Option<String>,

    #[arg(long)]
    actor: Option<String>,

    #[arg(long)]
    reason: Option<String>,
}

#[derive(Debug, Args)]
struct DiagnosticsReadCaseArgs {
    #[arg(long)]
    case_id: String,
}

#[derive(Debug, Args)]
struct DiagnosticsReadRunArgs {
    #[arg(long)]
    run_id: String,
}

fn run_diagnostics_attach(args: DiagnosticsAttachArgs) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let ttl = Duration::from_millis(args.ttl_ms.max(1));
    let recent_events_limit = args.recent_events_limit.clamp(1, 128);
    let payload = crate::runtime_bridge::block_on(async move {
        let session = client.session_info().await?;
        let health = client.health().await?;
        let runtime_snapshot = client.runtime_snapshot().await?;
        let recent_events = client.recent_events(Some(recent_events_limit)).await?;
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
            "runtime_snapshot": runtime_snapshot,
            "recent_events": recent_events,
            "controller_lease": controller_lease,
        }))
    })?;

    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_create_case(args: DiagnosticsCreateCaseArgs) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let payload = crate::runtime_bridge::block_on(async move {
        client
            .create_case(
                &args.lease_id,
                &args.title,
                args.problem_statement,
                args.workload_identity,
                args.target_environment,
            )
            .await
    })?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_create_run(args: DiagnosticsCreateRunArgs) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let payload = crate::runtime_bridge::block_on(async move {
        client
            .create_run(&args.lease_id, &args.case_id, args.workload_identity)
            .await
    })?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_add_timeline(args: DiagnosticsAddTimelineArgs) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let payload = crate::runtime_bridge::block_on(async move {
        client
            .append_timeline_entry(
                &args.lease_id,
                &args.run_id,
                NewTimelineEntry {
                    source: args.source.into(),
                    kind: args.kind.into(),
                    title: args.title,
                    body: args.body,
                    actor: args.actor,
                    reason: args.reason,
                },
            )
            .await
    })?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_read_case(args: DiagnosticsReadCaseArgs) -> anyhow::Result<()> {
    let payload = InvestigationStore::default_store()?.load_case(&args.case_id)?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_read_run(args: DiagnosticsReadRunArgs) -> anyhow::Result<()> {
    let payload = InvestigationStore::default_store()?.load_run(&args.run_id)?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn live_client_for_mount(mount_point: &str) -> anyhow::Result<diagnostics::LocalDiagnosticsClient> {
    let discovery = diagnostics::discover_local_sessions()?
        .into_iter()
        .find(|session| session.mount_point == mount_point)
        .ok_or_else(|| {
            anyhow::anyhow!("no live diagnostics session found for '{}'", mount_point)
        })?;
    Ok(diagnostics::LocalDiagnosticsClient::new(discovery))
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
            "--recent-events-limit",
            "64",
        ])
        .unwrap();
        match attach.command {
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsAttach(args),
            }) => {
                assert_eq!(args.recent_events_limit, 64);
            }
            other => panic!("expected diagnostics attach args, got {other:?}"),
        }

        let create_case = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-create-case",
            "Z:",
            "--lease-id",
            "lease-1",
            "--title",
            "AJA copy slowdown",
            "--workload-identity",
            "aja",
        ])
        .unwrap();
        assert!(matches!(
            create_case.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsCreateCase(_)
            })
        ));

        let create_run = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-create-run",
            "Z:",
            "--lease-id",
            "lease-1",
            "--case-id",
            "case-1",
            "--workload-identity",
            "copy-pass-1",
        ])
        .unwrap();
        assert!(matches!(
            create_run.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsCreateRun(_)
            })
        ));

        let add_timeline = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-add-timeline",
            "Z:",
            "--lease-id",
            "lease-1",
            "--run-id",
            "run-1",
            "--source",
            "cli",
            "--kind",
            "marker",
            "--title",
            "start copy",
        ])
        .unwrap();
        assert!(matches!(
            add_timeline.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsAddTimeline(_)
            })
        ));

        let read_case = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-read-case",
            "--case-id",
            "case-1",
        ])
        .unwrap();
        assert!(matches!(
            read_case.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsReadCase(_)
            })
        ));

        let read_run =
            crate::cli::parse_for_test(&["internal", "diagnostics-read-run", "--run-id", "run-1"])
                .unwrap();
        assert!(matches!(
            read_run.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsReadRun(_)
            })
        ));
    }
}

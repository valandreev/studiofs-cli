//! Hidden internal CLI subcommands.

use crate::diagnostics;
use crate::investigation::{
    DiagnosticsPreset, InvestigationHypothesisStatus, InvestigationStore, NewTimelineEntry,
    ProfilingCaptureMode, TimelineEntryKind, TimelineEntrySource,
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
            InternalAction::DiagnosticsListWorkflows(args) => run_diagnostics_list_workflows(args),
            InternalAction::DiagnosticsStartWorkflow(args) => run_diagnostics_start_workflow(args),
            InternalAction::DiagnosticsGetWorkflow(args) => run_diagnostics_get_workflow(args),
            InternalAction::DiagnosticsAdvanceWorkflow(args) => {
                run_diagnostics_advance_workflow(args)
            }
            InternalAction::DiagnosticsAckWorkflowStep(args) => {
                run_diagnostics_ack_workflow_step(args)
            }
            InternalAction::DiagnosticsAbortWorkflow(args) => run_diagnostics_abort_workflow(args),
            InternalAction::DiagnosticsCompleteWorkflow(args) => {
                run_diagnostics_complete_workflow(args)
            }
            InternalAction::DiagnosticsProfilingStatus(args) => {
                run_diagnostics_profiling_status(args)
            }
            InternalAction::DiagnosticsStartProfiling(args) => {
                run_diagnostics_start_profiling(args)
            }
            InternalAction::DiagnosticsStopProfiling(args) => run_diagnostics_stop_profiling(args),
            InternalAction::DiagnosticsAddTimeline(args) => run_diagnostics_add_timeline(args),
            InternalAction::DiagnosticsCompareRunSpan(args) => {
                run_diagnostics_compare_run_span(args)
            }
            InternalAction::DiagnosticsCreateHypothesis(args) => {
                run_diagnostics_create_hypothesis(args)
            }
            InternalAction::DiagnosticsUpdateHypothesis(args) => {
                run_diagnostics_update_hypothesis(args)
            }
            InternalAction::DiagnosticsReadCase(args) => run_diagnostics_read_case(args),
            InternalAction::DiagnosticsReadRun(args) => run_diagnostics_read_run(args),
            InternalAction::DiagnosticsReadComparison(args) => {
                run_diagnostics_read_comparison(args)
            }
            InternalAction::DiagnosticsReadHypothesis(args) => {
                run_diagnostics_read_hypothesis(args)
            }
            InternalAction::DiagnosticsMaterializeReport(args) => {
                run_diagnostics_materialize_report(args)
            }
            InternalAction::DiagnosticsReadReport(args) => run_diagnostics_read_report(args),
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
    #[command(name = "diagnostics-list-workflows", hide = true)]
    DiagnosticsListWorkflows(DiagnosticsListWorkflowsArgs),
    #[command(name = "diagnostics-start-workflow", hide = true)]
    DiagnosticsStartWorkflow(DiagnosticsStartWorkflowArgs),
    #[command(name = "diagnostics-get-workflow", hide = true)]
    DiagnosticsGetWorkflow(DiagnosticsGetWorkflowArgs),
    #[command(name = "diagnostics-advance-workflow", hide = true)]
    DiagnosticsAdvanceWorkflow(DiagnosticsAdvanceWorkflowArgs),
    #[command(name = "diagnostics-ack-workflow-step", hide = true)]
    DiagnosticsAckWorkflowStep(DiagnosticsAckWorkflowStepArgs),
    #[command(name = "diagnostics-abort-workflow", hide = true)]
    DiagnosticsAbortWorkflow(DiagnosticsAbortWorkflowArgs),
    #[command(name = "diagnostics-complete-workflow", hide = true)]
    DiagnosticsCompleteWorkflow(DiagnosticsCompleteWorkflowArgs),
    #[command(name = "diagnostics-profiling-status", hide = true)]
    DiagnosticsProfilingStatus(DiagnosticsProfilingStatusArgs),
    #[command(name = "diagnostics-start-profiling", hide = true)]
    DiagnosticsStartProfiling(DiagnosticsStartProfilingArgs),
    #[command(name = "diagnostics-stop-profiling", hide = true)]
    DiagnosticsStopProfiling(DiagnosticsStopProfilingArgs),
    #[command(name = "diagnostics-add-timeline", hide = true)]
    DiagnosticsAddTimeline(DiagnosticsAddTimelineArgs),
    #[command(name = "diagnostics-compare-run-span", hide = true)]
    DiagnosticsCompareRunSpan(DiagnosticsCompareRunSpanArgs),
    #[command(name = "diagnostics-create-hypothesis", hide = true)]
    DiagnosticsCreateHypothesis(DiagnosticsCreateHypothesisArgs),
    #[command(name = "diagnostics-update-hypothesis", hide = true)]
    DiagnosticsUpdateHypothesis(DiagnosticsUpdateHypothesisArgs),
    #[command(name = "diagnostics-read-case", hide = true)]
    DiagnosticsReadCase(DiagnosticsReadCaseArgs),
    #[command(name = "diagnostics-read-run", hide = true)]
    DiagnosticsReadRun(DiagnosticsReadRunArgs),
    #[command(name = "diagnostics-read-comparison", hide = true)]
    DiagnosticsReadComparison(DiagnosticsReadComparisonArgs),
    #[command(name = "diagnostics-read-hypothesis", hide = true)]
    DiagnosticsReadHypothesis(DiagnosticsReadHypothesisArgs),
    #[command(name = "diagnostics-materialize-report", hide = true)]
    DiagnosticsMaterializeReport(DiagnosticsMaterializeReportArgs),
    #[command(name = "diagnostics-read-report", hide = true)]
    DiagnosticsReadReport(DiagnosticsReadReportArgs),
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

#[derive(Debug, Args)]
struct DiagnosticsListWorkflowsArgs {
    mount_point: String,
}

#[derive(Debug, Args)]
struct DiagnosticsStartWorkflowArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    run_id: String,

    #[arg(long)]
    workflow_id: String,
}

#[derive(Debug, Args)]
struct DiagnosticsGetWorkflowArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    workflow_run_id: String,
}

#[derive(Debug, Args)]
struct DiagnosticsAdvanceWorkflowArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    workflow_run_id: String,

    #[arg(long)]
    step_id: String,
}

#[derive(Debug, Args)]
struct DiagnosticsAckWorkflowStepArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    workflow_run_id: String,

    #[arg(long)]
    step_id: String,

    #[arg(long)]
    actor: Option<String>,

    #[arg(long)]
    note: Option<String>,
}

#[derive(Debug, Args)]
struct DiagnosticsAbortWorkflowArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    workflow_run_id: String,

    #[arg(long)]
    reason: Option<String>,
}

#[derive(Debug, Args)]
struct DiagnosticsCompleteWorkflowArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    workflow_run_id: String,
}

#[derive(Debug, Args)]
struct DiagnosticsProfilingStatusArgs {
    mount_point: String,
}

macro_rules! impl_arg_enum_from {
    ($source:ty => $target:ty { $($variant:ident),+ $(,)? }) => {
        impl From<$source> for $target {
            fn from(value: $source) -> Self {
                match value {
                    $(<$source>::$variant => <$target>::$variant,)+
                }
            }
        }
    };
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum ProfilingCaptureModeArg {
    WallBlocking,
    Cpu,
}

impl_arg_enum_from!(ProfilingCaptureModeArg => ProfilingCaptureMode { WallBlocking, Cpu });

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum DiagnosticsPresetArg {
    Minimal,
    Balanced,
    Deep,
}

impl_arg_enum_from!(DiagnosticsPresetArg => DiagnosticsPreset { Minimal, Balanced, Deep });

#[derive(Debug, Args)]
struct DiagnosticsStartProfilingArgs {
    mount_point: String,

    #[arg(long, help = "Controller lease id returned by diagnostics-attach")]
    lease_id: String,

    #[arg(long, help = "Investigation run id returned by diagnostics-create-run")]
    run_id: String,

    #[arg(long, value_enum, default_value_t = ProfilingCaptureModeArg::WallBlocking)]
    capture_mode: ProfilingCaptureModeArg,

    #[arg(long, value_enum, default_value_t = DiagnosticsPresetArg::Balanced)]
    preset: DiagnosticsPresetArg,

    #[arg(
        long,
        default_value_t = diagnostics::MAX_PROFILE_WINDOW_DURATION_MS,
        value_parser = clap::value_parser!(u64).range(1..=diagnostics::MAX_PROFILE_WINDOW_DURATION_MS),
        help = "Requested profiling duration in milliseconds"
    )]
    duration_ms: u64,

    #[arg(long)]
    sample_rate_hz: Option<u32>,
}

#[derive(Debug, Args)]
struct DiagnosticsStopProfilingArgs {
    mount_point: String,

    #[arg(long, help = "Controller lease id returned by diagnostics-attach")]
    lease_id: String,

    #[arg(
        long,
        help = "Profiling window id returned by diagnostics-start-profiling"
    )]
    window_id: String,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum TimelineSourceArg {
    Cli,
    Mcp,
}

impl_arg_enum_from!(TimelineSourceArg => TimelineEntrySource { Cli, Mcp });

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum TimelineKindArg {
    Marker,
    Note,
    Anomaly,
}

impl_arg_enum_from!(TimelineKindArg => TimelineEntryKind { Marker, Note, Anomaly });

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
struct DiagnosticsCompareRunSpanArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    baseline_run_id: String,

    #[arg(long)]
    candidate_run_id: String,

    #[arg(long)]
    start_marker_title: String,

    #[arg(long)]
    end_marker_title: String,
}

#[derive(Debug, Args)]
struct DiagnosticsCreateHypothesisArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    case_id: String,

    #[arg(long)]
    statement: String,

    #[arg(long)]
    expected_signal: String,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum HypothesisStatusArg {
    Open,
    Supported,
    Rejected,
    Inconclusive,
}

impl_arg_enum_from!(HypothesisStatusArg => InvestigationHypothesisStatus {
    Open,
    Supported,
    Rejected,
    Inconclusive,
});

#[derive(Debug, Args)]
struct DiagnosticsUpdateHypothesisArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    hypothesis_id: String,

    #[arg(long, value_enum)]
    status: HypothesisStatusArg,

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

#[derive(Debug, Args)]
struct DiagnosticsReadComparisonArgs {
    #[arg(long)]
    comparison_id: String,
}

#[derive(Debug, Args)]
struct DiagnosticsReadHypothesisArgs {
    #[arg(long)]
    hypothesis_id: String,
}

#[derive(Debug, Args)]
struct DiagnosticsMaterializeReportArgs {
    mount_point: String,

    #[arg(long)]
    lease_id: String,

    #[arg(long)]
    run_id: String,
}

#[derive(Debug, Args)]
struct DiagnosticsReadReportArgs {
    #[arg(long)]
    report_id: String,
}

fn run_diagnostics_attach(args: DiagnosticsAttachArgs) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let ttl = Duration::from_millis(args.ttl_ms.max(1));
    let recent_events_limit = args
        .recent_events_limit
        .clamp(1, diagnostics::MAX_RECENT_EVENTS_LIMIT);
    let payload = crate::runtime_bridge::block_on(async move {
        let session = client.session_info().await?;
        let health = client.health().await?;
        let (runtime_snapshot, recent_events) =
            if diagnostics_runtime_surface_supported(session.protocol_version) {
                (
                    Some(client.runtime_snapshot().await?),
                    Some(client.recent_events(Some(recent_events_limit)).await?),
                )
            } else {
                (None, None)
            };
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

fn diagnostics_runtime_surface_supported(protocol_version: u32) -> bool {
    protocol_version >= diagnostics::DIAGNOSTICS_RUNTIME_SURFACE_PROTOCOL_VERSION
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

fn run_live_diagnostics_command<F, Fut, P>(mount_point: &str, command: F) -> anyhow::Result<()>
where
    F: FnOnce(diagnostics::LocalDiagnosticsClient) -> Fut + Send,
    Fut: std::future::Future<Output = anyhow::Result<P>> + Send,
    P: serde::Serialize + Send,
{
    let client = live_client_for_mount(mount_point)?;
    let payload = crate::runtime_bridge::block_on(async move { command(client).await })?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_list_workflows(args: DiagnosticsListWorkflowsArgs) -> anyhow::Result<()> {
    let DiagnosticsListWorkflowsArgs { mount_point } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client.list_workflows().await
    })
}

fn run_diagnostics_start_workflow(args: DiagnosticsStartWorkflowArgs) -> anyhow::Result<()> {
    let DiagnosticsStartWorkflowArgs {
        mount_point,
        lease_id,
        run_id,
        workflow_id,
    } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client
            .start_workflow(&lease_id, &run_id, &workflow_id)
            .await
    })
}

fn run_diagnostics_get_workflow(args: DiagnosticsGetWorkflowArgs) -> anyhow::Result<()> {
    let DiagnosticsGetWorkflowArgs {
        mount_point,
        lease_id,
        workflow_run_id,
    } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client.workflow_run(&lease_id, &workflow_run_id).await
    })
}

fn run_diagnostics_advance_workflow(args: DiagnosticsAdvanceWorkflowArgs) -> anyhow::Result<()> {
    let DiagnosticsAdvanceWorkflowArgs {
        mount_point,
        lease_id,
        workflow_run_id,
        step_id,
    } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client
            .advance_workflow_step(&lease_id, &workflow_run_id, &step_id)
            .await
    })
}

fn run_diagnostics_ack_workflow_step(args: DiagnosticsAckWorkflowStepArgs) -> anyhow::Result<()> {
    let DiagnosticsAckWorkflowStepArgs {
        mount_point,
        lease_id,
        workflow_run_id,
        step_id,
        actor,
        note,
    } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client
            .acknowledge_manual_step(&lease_id, &workflow_run_id, &step_id, actor, note)
            .await
    })
}

fn run_diagnostics_abort_workflow(args: DiagnosticsAbortWorkflowArgs) -> anyhow::Result<()> {
    let DiagnosticsAbortWorkflowArgs {
        mount_point,
        lease_id,
        workflow_run_id,
        reason,
    } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client
            .abort_workflow(&lease_id, &workflow_run_id, reason)
            .await
    })
}

fn run_diagnostics_complete_workflow(args: DiagnosticsCompleteWorkflowArgs) -> anyhow::Result<()> {
    let DiagnosticsCompleteWorkflowArgs {
        mount_point,
        lease_id,
        workflow_run_id,
    } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client.complete_workflow(&lease_id, &workflow_run_id).await
    })
}

fn run_diagnostics_profiling_status(args: DiagnosticsProfilingStatusArgs) -> anyhow::Result<()> {
    let DiagnosticsProfilingStatusArgs { mount_point } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client.profiling_status().await
    })
}

fn run_diagnostics_start_profiling(args: DiagnosticsStartProfilingArgs) -> anyhow::Result<()> {
    let DiagnosticsStartProfilingArgs {
        mount_point,
        lease_id,
        run_id,
        capture_mode,
        preset,
        duration_ms,
        sample_rate_hz,
    } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client
            .start_profiling_window(
                &lease_id,
                &run_id,
                diagnostics::StartProfilingWindowRequest {
                    capture_mode: capture_mode.into(),
                    preset: preset.into(),
                    budget: diagnostics::ProfilingBudget {
                        duration_ms,
                        sample_rate_hz,
                    },
                },
            )
            .await
    })
}

fn run_diagnostics_stop_profiling(args: DiagnosticsStopProfilingArgs) -> anyhow::Result<()> {
    let DiagnosticsStopProfilingArgs {
        mount_point,
        lease_id,
        window_id,
    } = args;
    run_live_diagnostics_command(&mount_point, |client| async move {
        client.stop_profiling_window(&lease_id, &window_id).await
    })
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

fn run_diagnostics_compare_run_span(args: DiagnosticsCompareRunSpanArgs) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let payload = crate::runtime_bridge::block_on(async move {
        client
            .compare_run_span(
                &args.lease_id,
                diagnostics::DiagnosticsCompareRunSpanRequest {
                    baseline_run_id: args.baseline_run_id,
                    candidate_run_id: args.candidate_run_id,
                    start_marker_title: args.start_marker_title,
                    end_marker_title: args.end_marker_title,
                },
            )
            .await
    })?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_create_hypothesis(args: DiagnosticsCreateHypothesisArgs) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let payload = crate::runtime_bridge::block_on(async move {
        client
            .create_hypothesis(
                &args.lease_id,
                &args.case_id,
                args.statement,
                args.expected_signal,
                Vec::new(),
            )
            .await
    })?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_update_hypothesis(args: DiagnosticsUpdateHypothesisArgs) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let payload = crate::runtime_bridge::block_on(async move {
        client
            .update_hypothesis_status(
                &args.lease_id,
                &args.hypothesis_id,
                args.status.into(),
                args.actor,
                args.reason,
                Vec::new(),
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

fn run_diagnostics_read_comparison(args: DiagnosticsReadComparisonArgs) -> anyhow::Result<()> {
    let payload = InvestigationStore::default_store()?.load_comparison(&args.comparison_id)?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_read_hypothesis(args: DiagnosticsReadHypothesisArgs) -> anyhow::Result<()> {
    let payload = InvestigationStore::default_store()?.load_hypothesis(&args.hypothesis_id)?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_materialize_report(
    args: DiagnosticsMaterializeReportArgs,
) -> anyhow::Result<()> {
    let client = live_client_for_mount(&args.mount_point)?;
    let payload = crate::runtime_bridge::block_on(async move {
        client
            .materialize_report(&args.lease_id, &args.run_id)
            .await
    })?;
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn run_diagnostics_read_report(args: DiagnosticsReadReportArgs) -> anyhow::Result<()> {
    let payload = InvestigationStore::default_store()?.load_report(&args.report_id)?;
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
    fn diagnostics_runtime_surface_is_gated_by_protocol_version() {
        assert!(!diagnostics_runtime_surface_supported(1));
        assert!(diagnostics_runtime_surface_supported(
            diagnostics::DIAGNOSTICS_RUNTIME_SURFACE_PROTOCOL_VERSION
        ));
    }

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

        let list_workflows =
            crate::cli::parse_for_test(&["internal", "diagnostics-list-workflows", "Z:"]).unwrap();
        assert!(matches!(
            list_workflows.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsListWorkflows(_)
            })
        ));

        let start_workflow = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-start-workflow",
            "Z:",
            "--lease-id",
            "lease-1",
            "--run-id",
            "run-1",
            "--workflow-id",
            "aja_hybrid_v1",
        ])
        .unwrap();
        assert!(matches!(
            start_workflow.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsStartWorkflow(_)
            })
        ));

        let get_workflow = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-get-workflow",
            "Z:",
            "--lease-id",
            "lease-1",
            "--workflow-run-id",
            "workflow-1",
        ])
        .unwrap();
        assert!(matches!(
            get_workflow.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsGetWorkflow(_)
            })
        ));

        let advance_workflow = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-advance-workflow",
            "Z:",
            "--lease-id",
            "lease-1",
            "--workflow-run-id",
            "workflow-1",
            "--step-id",
            "capture_attach_snapshot",
        ])
        .unwrap();
        assert!(matches!(
            advance_workflow.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsAdvanceWorkflow(_)
            })
        ));

        let ack_workflow = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-ack-workflow-step",
            "Z:",
            "--lease-id",
            "lease-1",
            "--workflow-run-id",
            "workflow-1",
            "--step-id",
            "run_manual_aja_phase",
            "--actor",
            "operator",
        ])
        .unwrap();
        assert!(matches!(
            ack_workflow.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsAckWorkflowStep(_)
            })
        ));

        let abort_workflow = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-abort-workflow",
            "Z:",
            "--lease-id",
            "lease-1",
            "--workflow-run-id",
            "workflow-1",
        ])
        .unwrap();
        assert!(matches!(
            abort_workflow.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsAbortWorkflow(_)
            })
        ));

        let complete_workflow = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-complete-workflow",
            "Z:",
            "--lease-id",
            "lease-1",
            "--workflow-run-id",
            "workflow-1",
        ])
        .unwrap();
        assert!(matches!(
            complete_workflow.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsCompleteWorkflow(_)
            })
        ));

        let profiling_status =
            crate::cli::parse_for_test(&["internal", "diagnostics-profiling-status", "Z:"])
                .unwrap();
        assert!(matches!(
            profiling_status.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsProfilingStatus(_)
            })
        ));

        let start_profiling = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-start-profiling",
            "Z:",
            "--lease-id",
            "lease-1",
            "--run-id",
            "run-1",
            "--capture-mode",
            "wall-blocking",
            "--preset",
            "balanced",
            "--duration-ms",
            "30000",
            "--sample-rate-hz",
            "80",
        ])
        .unwrap();
        match start_profiling.command {
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsStartProfiling(args),
            }) => {
                assert_eq!(args.mount_point, "Z:");
                assert_eq!(args.lease_id, "lease-1");
                assert_eq!(args.run_id, "run-1");
                assert_eq!(args.duration_ms, 30_000);
                assert_eq!(args.sample_rate_hz, Some(80));
                assert!(matches!(
                    args.capture_mode,
                    ProfilingCaptureModeArg::WallBlocking
                ));
                assert!(matches!(args.preset, DiagnosticsPresetArg::Balanced));
            }
            other => panic!("unexpected parse result: {other:?}"),
        }

        let stop_profiling = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-stop-profiling",
            "Z:",
            "--lease-id",
            "lease-1",
            "--window-id",
            "window-1",
        ])
        .unwrap();
        assert!(matches!(
            stop_profiling.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsStopProfiling(_)
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

        let add_anomaly = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-add-timeline",
            "Z:",
            "--lease-id",
            "lease-1",
            "--run-id",
            "run-1",
            "--source",
            "mcp",
            "--kind",
            "anomaly",
            "--title",
            "cache miss burst",
        ])
        .unwrap();
        assert!(matches!(
            add_anomaly.command,
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

        let materialize_report = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-materialize-report",
            "Z:",
            "--lease-id",
            "lease-1",
            "--run-id",
            "run-1",
        ])
        .unwrap();
        assert!(matches!(
            materialize_report.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsMaterializeReport(_)
            })
        ));

        let compare_run_span = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-compare-run-span",
            "Z:",
            "--lease-id",
            "lease-1",
            "--baseline-run-id",
            "run-a",
            "--candidate-run-id",
            "run-b",
            "--start-marker-title",
            "warm copy",
            "--end-marker-title",
            "warm copy done",
        ])
        .unwrap();
        assert!(matches!(
            compare_run_span.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsCompareRunSpan(_)
            })
        ));

        let create_hypothesis = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-create-hypothesis",
            "Z:",
            "--lease-id",
            "lease-1",
            "--case-id",
            "case-1",
            "--statement",
            "queue wait dropped",
            "--expected-signal",
            "comparison delta becomes negative",
        ])
        .unwrap();
        assert!(matches!(
            create_hypothesis.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsCreateHypothesis(_)
            })
        ));

        let update_hypothesis = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-update-hypothesis",
            "Z:",
            "--lease-id",
            "lease-1",
            "--hypothesis-id",
            "hyp-1",
            "--status",
            "supported",
        ])
        .unwrap();
        assert!(matches!(
            update_hypothesis.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsUpdateHypothesis(_)
            })
        ));

        let read_comparison = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-read-comparison",
            "--comparison-id",
            "comparison-1",
        ])
        .unwrap();
        assert!(matches!(
            read_comparison.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsReadComparison(_)
            })
        ));

        let read_hypothesis = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-read-hypothesis",
            "--hypothesis-id",
            "hypothesis-1",
        ])
        .unwrap();
        assert!(matches!(
            read_hypothesis.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsReadHypothesis(_)
            })
        ));

        let read_report = crate::cli::parse_for_test(&[
            "internal",
            "diagnostics-read-report",
            "--report-id",
            "report-1",
        ])
        .unwrap();
        assert!(matches!(
            read_report.command,
            crate::cli::Commands::Internal(InternalArgs {
                action: InternalAction::DiagnosticsReadReport(_)
            })
        ));
    }
}

use super::*;

pub(super) const WORKFLOW_TOOL_INTERNAL_ATTACH_SNAPSHOT: &str = "internal.attach_snapshot";
pub(super) const WORKFLOW_TOOL_DIAGNOSTICS_START_BALANCED_WALL_BLOCKING: &str =
    "diagnostics.start_balanced_wall_blocking";
pub(super) const WORKFLOW_TOOL_DIAGNOSTICS_STOP_ACTIVE_PROFILING: &str =
    "diagnostics.stop_active_profiling";
pub(super) const WORKFLOW_TOOL_DIAGNOSTICS_MATERIALIZE_REPORT: &str =
    "diagnostics.materialize_report";

#[derive(Debug, Clone, Deserialize)]
pub(super) struct WorkflowAttachSnapshotToolArgs {
    #[serde(default)]
    recent_events_limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct WorkflowProfilingToolArgs {
    #[serde(default)]
    capture_mode: Option<ProfilingCaptureMode>,
    #[serde(default)]
    preset: Option<DiagnosticsPreset>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    sample_rate_hz: Option<u32>,
}

pub(super) fn built_in_workflow_presets() -> Vec<InvestigationWorkflowPreset> {
    vec![InvestigationWorkflowPreset {
        workflow_id: "aja_hybrid_v1".to_string(),
        version: 1,
        title: "AJA Hybrid Investigation".to_string(),
        supported_workload: "aja".to_string(),
        description: "Guide an AJA-style operator workflow with bounded automation, manual acknowledgement, profiling, and report generation.".to_string(),
        tools: vec![
            InvestigationWorkflowToolRef {
                family: InvestigationWorkflowToolFamily::InternalCli,
                tool_id: WORKFLOW_TOOL_INTERNAL_ATTACH_SNAPSHOT.to_string(),
            },
            InvestigationWorkflowToolRef {
                family: InvestigationWorkflowToolFamily::DiagnosticsTool,
                tool_id: WORKFLOW_TOOL_DIAGNOSTICS_START_BALANCED_WALL_BLOCKING.to_string(),
            },
            InvestigationWorkflowToolRef {
                family: InvestigationWorkflowToolFamily::DiagnosticsTool,
                tool_id: WORKFLOW_TOOL_DIAGNOSTICS_STOP_ACTIVE_PROFILING.to_string(),
            },
            InvestigationWorkflowToolRef {
                family: InvestigationWorkflowToolFamily::DiagnosticsTool,
                tool_id: WORKFLOW_TOOL_DIAGNOSTICS_MATERIALIZE_REPORT.to_string(),
            },
        ],
        steps: vec![
            InvestigationWorkflowPresetStep {
                step_id: "capture_attach_snapshot".to_string(),
                title: "Capture attach snapshot".to_string(),
                instructions: "Run the bounded attach helper and save the resulting diagnostics snapshot artifact.".to_string(),
                kind: InvestigationWorkflowStepKind::ToolStep,
                requires_controller: true,
                allowed_after: Vec::new(),
                expected_artifacts: vec!["attach_snapshot.json".to_string()],
                failure_policy: InvestigationWorkflowFailurePolicy::FailOpen,
                on_enter_marker: None,
                on_exit_marker: None,
                operator_ack_text: None,
                tool: Some(InvestigationWorkflowToolRef {
                    family: InvestigationWorkflowToolFamily::InternalCli,
                    tool_id: WORKFLOW_TOOL_INTERNAL_ATTACH_SNAPSHOT.to_string(),
                }),
                tool_arguments: Some(serde_json::json!({
                    "recent_events_limit": 24,
                })),
            },
            InvestigationWorkflowPresetStep {
                step_id: "start_balanced_profiling".to_string(),
                title: "Start balanced wall-blocking profile".to_string(),
                instructions: "Start a bounded wall-blocking profile before the operator workload phase.".to_string(),
                kind: InvestigationWorkflowStepKind::ProfilingWindow,
                requires_controller: true,
                allowed_after: vec!["capture_attach_snapshot".to_string()],
                expected_artifacts: Vec::new(),
                failure_policy: InvestigationWorkflowFailurePolicy::FailOpen,
                on_enter_marker: None,
                on_exit_marker: None,
                operator_ack_text: None,
                tool: Some(InvestigationWorkflowToolRef {
                    family: InvestigationWorkflowToolFamily::DiagnosticsTool,
                    tool_id: WORKFLOW_TOOL_DIAGNOSTICS_START_BALANCED_WALL_BLOCKING.to_string(),
                }),
                tool_arguments: Some(serde_json::json!({
                    "capture_mode": "wall_blocking",
                    "preset": "balanced",
                    "duration_ms": 4000,
                    "sample_rate_hz": 80,
                })),
            },
            InvestigationWorkflowPresetStep {
                step_id: "run_manual_aja_phase".to_string(),
                title: "Run operator AJA phase".to_string(),
                instructions: "Run the AJA-controlled workload now, then acknowledge this step when the manual phase completes.".to_string(),
                kind: InvestigationWorkflowStepKind::ManualStep,
                requires_controller: true,
                allowed_after: vec!["start_balanced_profiling".to_string()],
                expected_artifacts: Vec::new(),
                failure_policy: InvestigationWorkflowFailurePolicy::Retryable,
                on_enter_marker: Some("AJA workload start".to_string()),
                on_exit_marker: Some("AJA workload done".to_string()),
                operator_ack_text: Some("AJA phase complete".to_string()),
                tool: None,
                tool_arguments: None,
            },
            InvestigationWorkflowPresetStep {
                step_id: "stop_active_profiling".to_string(),
                title: "Stop active profiling window".to_string(),
                instructions: "Stop the active profiling window started before the manual phase.".to_string(),
                kind: InvestigationWorkflowStepKind::ProfilingWindow,
                requires_controller: true,
                allowed_after: vec!["run_manual_aja_phase".to_string()],
                expected_artifacts: vec!["profile_summary".to_string()],
                failure_policy: InvestigationWorkflowFailurePolicy::FailOpen,
                on_enter_marker: None,
                on_exit_marker: None,
                operator_ack_text: None,
                tool: Some(InvestigationWorkflowToolRef {
                    family: InvestigationWorkflowToolFamily::DiagnosticsTool,
                    tool_id: WORKFLOW_TOOL_DIAGNOSTICS_STOP_ACTIVE_PROFILING.to_string(),
                }),
                tool_arguments: None,
            },
            InvestigationWorkflowPresetStep {
                step_id: "materialize_report".to_string(),
                title: "Materialize investigation report".to_string(),
                instructions: "Generate a single-run report from the captured workflow evidence.".to_string(),
                kind: InvestigationWorkflowStepKind::ReportStep,
                requires_controller: true,
                allowed_after: vec!["stop_active_profiling".to_string()],
                expected_artifacts: vec!["investigation_report".to_string()],
                failure_policy: InvestigationWorkflowFailurePolicy::Retryable,
                on_enter_marker: None,
                on_exit_marker: None,
                operator_ack_text: None,
                tool: Some(InvestigationWorkflowToolRef {
                    family: InvestigationWorkflowToolFamily::DiagnosticsTool,
                    tool_id: WORKFLOW_TOOL_DIAGNOSTICS_MATERIALIZE_REPORT.to_string(),
                }),
                tool_arguments: None,
            },
        ],
    }]
}

pub(super) fn built_in_workflow_by_id(workflow_id: &str) -> Option<InvestigationWorkflowPreset> {
    built_in_workflow_presets()
        .into_iter()
        .find(|workflow| workflow.workflow_id == workflow_id)
}

pub(super) fn append_workflow_timeline_event(
    store: &InvestigationStore,
    run_id: &str,
    title: &str,
    body: serde_json::Value,
) -> Result<()> {
    store.append_timeline_entry(
        run_id,
        NewTimelineEntry {
            source: TimelineEntrySource::Mcp,
            kind: TimelineEntryKind::Action,
            title: title.to_string(),
            body: Some(serde_json::to_string(&body)?),
            actor: Some("workflow".to_string()),
            reason: None,
        },
    )?;
    Ok(())
}

pub(super) fn append_workflow_marker(
    store: &InvestigationStore,
    run_id: &str,
    title: &str,
    source: &dyn DiagnosticsDataSource,
) -> Result<()> {
    let marker = store.append_timeline_entry(
        run_id,
        NewTimelineEntry {
            source: TimelineEntrySource::Mcp,
            kind: TimelineEntryKind::Marker,
            title: title.to_string(),
            body: None,
            actor: Some("workflow".to_string()),
            reason: None,
        },
    )?;
    store.append_marker_snapshot(
        run_id,
        NewInvestigationMarkerSnapshot {
            marker_sequence: marker.sequence,
            marker_title: marker.title,
            runtime_snapshot: source.runtime_snapshot(),
        },
    )?;
    Ok(())
}

pub(super) fn workflow_step_index(
    workflow: &InvestigationWorkflowRun,
    step_id: &str,
) -> Result<usize> {
    workflow
        .steps
        .iter()
        .position(|step| step.step_id == step_id)
        .ok_or_else(|| anyhow!("workflow step '{}' not found", step_id))
}

pub(super) fn workflow_step_is_terminal(status: InvestigationWorkflowStepStatus) -> bool {
    matches!(
        status,
        InvestigationWorkflowStepStatus::Completed
            | InvestigationWorkflowStepStatus::Skipped
            | InvestigationWorkflowStepStatus::Failed
            | InvestigationWorkflowStepStatus::Aborted
    )
}

pub(super) fn workflow_dependencies_satisfied(
    workflow: &InvestigationWorkflowRun,
    step: &crate::investigation::InvestigationWorkflowStepState,
) -> bool {
    step.allowed_after.iter().all(|dependency| {
        match workflow
            .steps
            .iter()
            .find(|candidate| candidate.step_id == *dependency)
        {
            Some(candidate) => workflow_step_is_terminal(candidate.status),
            None => false,
        }
    })
}

pub(super) fn next_available_workflow_step_id(
    workflow: &InvestigationWorkflowRun,
) -> Option<String> {
    workflow
        .steps
        .iter()
        .find(|step| {
            step.status == InvestigationWorkflowStepStatus::Pending
                && workflow_dependencies_satisfied(workflow, step)
        })
        .map(|step| step.step_id.clone())
}

pub(super) fn complete_workflow_progress(workflow: &mut InvestigationWorkflowRun) {
    workflow.updated_unix_ms = now_unix_millis();
    if let Some(waiting) = workflow
        .steps
        .iter()
        .find(|step| step.status == InvestigationWorkflowStepStatus::WaitingForOperator)
    {
        workflow.status = InvestigationWorkflowStatus::WaitingForOperator;
        workflow.current_step_id = Some(waiting.step_id.clone());
        return;
    }
    if let Some(next_step_id) = next_available_workflow_step_id(workflow) {
        workflow.status = InvestigationWorkflowStatus::InProgress;
        workflow.current_step_id = Some(next_step_id);
    } else {
        let pending_step_ids: Vec<String> = workflow
            .steps
            .iter()
            .filter(|step| step.status == InvestigationWorkflowStepStatus::Pending)
            .map(|step| step.step_id.clone())
            .collect();
        if pending_step_ids.is_empty() {
            workflow.status = InvestigationWorkflowStatus::Completed;
            workflow.current_step_id = None;
            return;
        }

        let error = format!(
            "workflow run '{}' has pending steps with unsatisfiable dependencies: {}",
            workflow.id,
            pending_step_ids.join(", ")
        );
        for step in workflow
            .steps
            .iter_mut()
            .filter(|step| step.status == InvestigationWorkflowStepStatus::Pending)
        {
            step.last_error.get_or_insert_with(|| error.clone());
        }
        workflow.status = InvestigationWorkflowStatus::Failed;
        workflow.current_step_id = None;
    }
}

pub(super) fn workflow_artifact_dir(cache_path: &Path, workflow_run_id: &str) -> Result<PathBuf> {
    let dir = cache_path
        .join(DIAGNOSTICS_WORKFLOW_ARTIFACT_DIR_NAME)
        .join(workflow_run_id);
    fs::create_dir_all(&dir)
        .with_context(|| format!("create workflow artifact dir '{}'", dir.display()))?;
    Ok(dir)
}

pub(super) fn write_workflow_artifact(
    cache_path: &Path,
    workflow_run_id: &str,
    step_id: &str,
    kind: &str,
    extension: &str,
    bytes: &[u8],
) -> Result<InvestigationWorkflowArtifactRecord> {
    let dir = workflow_artifact_dir(cache_path, workflow_run_id)?;
    let path = dir.join(format!(
        "{}-{}-{}.{}",
        step_id,
        now_unix_millis(),
        Uuid::new_v4(),
        extension
    ));
    fs::write(&path, bytes)
        .with_context(|| format!("write workflow artifact '{}'", path.display()))?;
    Ok(InvestigationWorkflowArtifactRecord {
        kind: kind.to_string(),
        path: path.to_string_lossy().into_owned(),
        bytes: bytes.len() as u64,
    })
}

pub(super) fn preview_text(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.chars().take(512).collect())
    }
}

pub(super) fn store_error_response(error: anyhow::Error) -> DiagnosticsResponse {
    DiagnosticsResponse::Error {
        code: "investigation_store_error".to_string(),
        message: error.to_string(),
    }
}

pub(super) fn workflow_error_response(
    code: &str,
    message: impl Into<String>,
) -> DiagnosticsResponse {
    DiagnosticsResponse::Error {
        code: code.to_string(),
        message: message.into(),
    }
}

pub(super) fn run_command_with_timeout(
    mut command: std::process::Command,
    timeout: Duration,
) -> Result<(i32, String, String)> {
    use std::io::{Read, Seek, SeekFrom};
    use std::process::Stdio;

    let mut stdout_file = tempfile::tempfile().context("create workflow stdout capture")?;
    let mut stderr_file = tempfile::tempfile().context("create workflow stderr capture")?;
    command
        .stdout(Stdio::from(
            stdout_file
                .try_clone()
                .context("clone workflow stdout capture")?,
        ))
        .stderr(Stdio::from(
            stderr_file
                .try_clone()
                .context("clone workflow stderr capture")?,
        ));
    let mut child = command.spawn().context("spawn workflow command")?;
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().context("poll workflow command")? {
            let mut stdout = Vec::new();
            stdout_file
                .seek(SeekFrom::Start(0))
                .context("rewind workflow command stdout")?;
            stdout_file
                .read_to_end(&mut stdout)
                .context("collect workflow command stdout")?;
            let mut stderr = Vec::new();
            stderr_file
                .seek(SeekFrom::Start(0))
                .context("rewind workflow command stderr")?;
            stderr_file
                .read_to_end(&mut stderr)
                .context("collect workflow command stderr")?;
            let exit_code = status
                .code()
                .ok_or_else(|| anyhow!("workflow command terminated without an exit code"))?;
            return Ok((
                exit_code,
                String::from_utf8_lossy(&stdout).into_owned(),
                String::from_utf8_lossy(&stderr).into_owned(),
            ));
        }
        if started.elapsed() >= timeout {
            child.kill().ok();
            let _ = child.wait();
            return Err(anyhow!(
                "workflow command exceeded timeout of {} ms",
                timeout.as_millis()
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

pub(super) fn run_attach_snapshot_tool(
    source: &dyn DiagnosticsDataSource,
    workflow_run: &InvestigationWorkflowRun,
    step: &crate::investigation::InvestigationWorkflowStepState,
    tool: InvestigationWorkflowToolRef,
) -> Result<(
    serde_json::Value,
    Vec<InvestigationWorkflowArtifactRecord>,
    InvestigationWorkflowToolInvocationRecord,
)> {
    let session = source.session_info();
    let tool_args = step
        .tool_arguments
        .clone()
        .map(serde_json::from_value::<WorkflowAttachSnapshotToolArgs>)
        .transpose()
        .context("decode attach snapshot workflow tool arguments")?
        .unwrap_or(WorkflowAttachSnapshotToolArgs {
            recent_events_limit: None,
        });
    let recent_events_limit = tool_args
        .recent_events_limit
        .unwrap_or(DEFAULT_RECENT_EVENTS_LIMIT)
        .clamp(1, MAX_RECENT_EVENTS_LIMIT);
    let arguments = serde_json::json!({
        "mount_point": session.mount_point.clone(),
        "recent_events_limit": recent_events_limit,
    });
    let started_unix_ms = now_unix_millis();

    #[cfg(test)]
    if let Some(output) = test_workflow_attach_snapshot_output() {
        let artifact = write_workflow_artifact(
            Path::new(&session.cache_path),
            &workflow_run.id,
            &step.step_id,
            "attach_snapshot",
            "json",
            output.as_bytes(),
        )?;
        return Ok((
            serde_json::json!({
                "tool_id": tool.tool_id,
                "artifact": artifact.path,
            }),
            vec![artifact],
            InvestigationWorkflowToolInvocationRecord {
                tool,
                arguments,
                started_unix_ms,
                completed_unix_ms: Some(now_unix_millis()),
                exit_status: Some(0),
                stdout_preview: preview_text(&output),
                stderr_preview: None,
            },
        ));
    }

    let current_exe = std::env::current_exe().context("resolve current executable")?;
    let mut command = std::process::Command::new(&current_exe);
    command.current_dir(&session.cache_path);
    command.args([
        "internal",
        "diagnostics-attach",
        &session.mount_point,
        "--recent-events-limit",
        &recent_events_limit.to_string(),
    ]);
    let (exit_status, stdout, stderr) = run_command_with_timeout(command, Duration::from_secs(15))?;
    if exit_status != 0 {
        return Err(anyhow!(
            "workflow tool '{}' exited with status {}: {}",
            WORKFLOW_TOOL_INTERNAL_ATTACH_SNAPSHOT,
            exit_status,
            stderr.trim()
        ));
    }
    let artifact = write_workflow_artifact(
        Path::new(&session.cache_path),
        &workflow_run.id,
        &step.step_id,
        "attach_snapshot",
        "json",
        stdout.as_bytes(),
    )?;
    Ok((
        serde_json::json!({
            "tool_id": WORKFLOW_TOOL_INTERNAL_ATTACH_SNAPSHOT,
            "artifact": artifact.path,
        }),
        vec![artifact],
        InvestigationWorkflowToolInvocationRecord {
            tool,
            arguments,
            started_unix_ms,
            completed_unix_ms: Some(now_unix_millis()),
            exit_status: Some(exit_status),
            stdout_preview: preview_text(&stdout),
            stderr_preview: preview_text(&stderr),
        },
    ))
}

pub(super) fn find_last_started_workflow_window_id(
    workflow: &InvestigationWorkflowRun,
) -> Option<String> {
    workflow.steps.iter().rev().find_map(|step| {
        step.output_summary
            .as_ref()
            .and_then(|summary| summary.get("window_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    })
}

pub(super) fn execute_workflow_tool_like_step(
    workflow: &InvestigationWorkflowRun,
    step_index: usize,
    source: &dyn DiagnosticsDataSource,
    store: &InvestigationStore,
    profiling: Option<&DiagnosticsProfilingCoordinator>,
) -> Result<(
    serde_json::Value,
    Vec<InvestigationWorkflowArtifactRecord>,
    Option<InvestigationWorkflowToolInvocationRecord>,
)> {
    let step = &workflow.steps[step_index];
    let tool = step
        .tool
        .clone()
        .ok_or_else(|| anyhow!("workflow step '{}' is missing a tool", step.step_id))?;
    match (step.kind, tool.family, tool.tool_id.as_str()) {
        (
            InvestigationWorkflowStepKind::ToolStep,
            InvestigationWorkflowToolFamily::InternalCli,
            WORKFLOW_TOOL_INTERNAL_ATTACH_SNAPSHOT,
        ) => {
            let (summary, artifacts, invocation) =
                run_attach_snapshot_tool(source, workflow, step, tool)?;
            Ok((summary, artifacts, Some(invocation)))
        }
        (
            InvestigationWorkflowStepKind::ProfilingWindow,
            InvestigationWorkflowToolFamily::DiagnosticsTool,
            WORKFLOW_TOOL_DIAGNOSTICS_START_BALANCED_WALL_BLOCKING,
        ) => {
            let Some(profiling) = profiling else {
                return Err(anyhow!("profiling control is unavailable for this session"));
            };
            let tool_args = step
                .tool_arguments
                .clone()
                .map(serde_json::from_value::<WorkflowProfilingToolArgs>)
                .transpose()
                .context("decode profiling workflow tool arguments")?
                .unwrap_or(WorkflowProfilingToolArgs {
                    capture_mode: None,
                    preset: None,
                    duration_ms: None,
                    sample_rate_hz: None,
                });
            let window = profiling.start_window(
                &workflow.run_id,
                StartProfilingWindowRequest {
                    capture_mode: tool_args
                        .capture_mode
                        .unwrap_or(ProfilingCaptureMode::WallBlocking),
                    preset: tool_args.preset.unwrap_or(DiagnosticsPreset::Balanced),
                    budget: ProfilingBudget {
                        duration_ms: tool_args.duration_ms.unwrap_or(5_000),
                        sample_rate_hz: Some(
                            tool_args
                                .sample_rate_hz
                                .unwrap_or(DEFAULT_PROFILE_SAMPLE_RATE_HZ),
                        ),
                    },
                },
                Some(store),
            )?;
            Ok((
                serde_json::json!({
                    "window_id": window.window_id,
                    "status": window.status,
                }),
                Vec::new(),
                None,
            ))
        }
        (
            InvestigationWorkflowStepKind::ProfilingWindow,
            InvestigationWorkflowToolFamily::DiagnosticsTool,
            WORKFLOW_TOOL_DIAGNOSTICS_STOP_ACTIVE_PROFILING,
        ) => {
            let Some(profiling) = profiling else {
                return Err(anyhow!("profiling control is unavailable for this session"));
            };
            let Some(window_id) = find_last_started_workflow_window_id(workflow) else {
                return Err(anyhow!("no active workflow profiling window to stop"));
            };
            let window = profiling.stop_window(&window_id, Some(store))?;
            let artifacts = window
                .artifact
                .as_ref()
                .map(|artifact| {
                    vec![InvestigationWorkflowArtifactRecord {
                        kind: artifact.kind.clone(),
                        path: artifact.path.clone(),
                        bytes: artifact.bytes,
                    }]
                })
                .unwrap_or_default();
            Ok((
                serde_json::json!({
                    "window_id": window.window_id,
                    "status": window.status,
                }),
                artifacts,
                None,
            ))
        }
        (
            InvestigationWorkflowStepKind::ReportStep,
            InvestigationWorkflowToolFamily::DiagnosticsTool,
            WORKFLOW_TOOL_DIAGNOSTICS_MATERIALIZE_REPORT,
        ) => {
            let report = materialize_report(&workflow.run_id, source, store)?;
            Ok((
                serde_json::json!({
                    "report_id": report.id,
                }),
                Vec::new(),
                None,
            ))
        }
        _ => Err(anyhow!(
            "workflow tool '{}' is not allowed for this step",
            tool.tool_id
        )),
    }
}

pub(super) fn write_wall_blocking_artifact(
    cache_path: &Path,
    payload: &serde_json::Value,
) -> Result<(PathBuf, u64)> {
    let output_dir = cache_path.join(DIAGNOSTICS_PROFILE_ARTIFACT_DIR_NAME);
    fs::create_dir_all(&output_dir).with_context(|| {
        format!(
            "create profiling artifact directory '{}'",
            output_dir.display()
        )
    })?;
    let filename = format!(
        "wall-blocking-window-{}-{}.json",
        now_unix_millis(),
        Uuid::new_v4()
    );
    let path = output_dir.join(filename);
    let bytes = serde_json::to_vec_pretty(payload)?;
    let len = bytes.len() as u64;
    fs::write(&path, &bytes).with_context(|| {
        format!(
            "write wall-blocking profiling artifact '{}'",
            path.display()
        )
    })?;
    Ok((path, len))
}

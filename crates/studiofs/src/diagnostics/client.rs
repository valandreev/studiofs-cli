use super::*;

#[derive(Debug, Clone)]
pub(crate) struct LocalDiagnosticsClient {
    discovery: LocalDiagnosticsDiscovery,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiagnosticsClientError {
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for DiagnosticsClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for DiagnosticsClientError {}

impl LocalDiagnosticsClient {
    pub(crate) fn new(discovery: LocalDiagnosticsDiscovery) -> Self {
        Self { discovery }
    }

    pub(crate) async fn session_info(&self) -> Result<DiagnosticsSessionInfo> {
        match self
            .send(DiagnosticsRequest::SessionInfo {
                auth_token: self.discovery.auth_token.clone(),
            })
            .await?
        {
            DiagnosticsResponse::SessionInfo { session } => Ok(session),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn snapshot_format_info(&self) -> Result<FormatInfo> {
        match self
            .send(DiagnosticsRequest::SnapshotFormatInfo {
                auth_token: self.discovery.auth_token.clone(),
            })
            .await?
        {
            DiagnosticsResponse::SnapshotFormatInfo { format_info } => Ok(format_info),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn list_workflows(&self) -> Result<Vec<InvestigationWorkflowPreset>> {
        match self
            .send(DiagnosticsRequest::ListWorkflows {
                auth_token: self.discovery.auth_token.clone(),
            })
            .await?
        {
            DiagnosticsResponse::WorkflowPresets { workflows } => Ok(workflows),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn health(&self) -> Result<DiagnosticsHealthSnapshot> {
        match self
            .send(DiagnosticsRequest::Health {
                auth_token: self.discovery.auth_token.clone(),
            })
            .await?
        {
            DiagnosticsResponse::Health { health } => Ok(*health),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn runtime_snapshot(&self) -> Result<DiagnosticsRuntimeSnapshot> {
        match self
            .send(DiagnosticsRequest::RuntimeSnapshot {
                auth_token: self.discovery.auth_token.clone(),
            })
            .await?
        {
            DiagnosticsResponse::RuntimeSnapshot { snapshot } => Ok(*snapshot),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn recent_events(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<DiagnosticsRecentEvent>> {
        match self
            .send(DiagnosticsRequest::RecentEvents {
                auth_token: self.discovery.auth_token.clone(),
                limit,
            })
            .await?
        {
            DiagnosticsResponse::RecentEvents { events } => Ok(events),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn evidence_view(
        &self,
        lease_id: &str,
        run_id: &str,
        request: DiagnosticsEvidenceRequest,
    ) -> Result<DiagnosticsEvidenceView> {
        match self
            .send(DiagnosticsRequest::EvidenceView {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                run_id: run_id.to_string(),
                request,
            })
            .await?
        {
            DiagnosticsResponse::EvidenceView { view } => Ok(*view),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn acquire_controller(
        &self,
        client_name: &str,
        ttl: Duration,
    ) -> Result<ControllerLease> {
        self.try_acquire_controller(client_name, ttl)
            .await
            .map_err(anyhow::Error::from)
    }

    pub(crate) async fn try_acquire_controller(
        &self,
        client_name: &str,
        ttl: Duration,
    ) -> std::result::Result<ControllerLease, DiagnosticsClientError> {
        match self
            .send(DiagnosticsRequest::AcquireController {
                auth_token: self.discovery.auth_token.clone(),
                client_name: client_name.to_string(),
                ttl_ms: Some(ttl.as_millis().try_into().unwrap_or(MAX_LEASE_TTL_MS)),
            })
            .await
        {
            Ok(DiagnosticsResponse::ControllerLease { lease }) => Ok(lease),
            Ok(DiagnosticsResponse::Error { code, message }) => {
                Err(DiagnosticsClientError { code, message })
            }
            Ok(other) => Err(DiagnosticsClientError {
                code: "unexpected_response".to_string(),
                message: format!("unexpected diagnostics response: {other:?}"),
            }),
            Err(error) => Err(DiagnosticsClientError {
                code: "transport".to_string(),
                message: error.to_string(),
            }),
        }
    }

    pub(crate) async fn renew_controller(
        &self,
        lease_id: &str,
        ttl: Duration,
    ) -> Result<ControllerLease> {
        match self
            .send(DiagnosticsRequest::RenewController {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                ttl_ms: Some(ttl.as_millis().try_into().unwrap_or(MAX_LEASE_TTL_MS)),
            })
            .await?
        {
            DiagnosticsResponse::ControllerLease { lease } => Ok(lease),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn materialize_report(
        &self,
        lease_id: &str,
        run_id: &str,
    ) -> Result<InvestigationReport> {
        match self
            .send(DiagnosticsRequest::MaterializeReport {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                run_id: run_id.to_string(),
            })
            .await?
        {
            DiagnosticsResponse::InvestigationReport { report } => Ok(*report),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn compare_run_span(
        &self,
        lease_id: &str,
        request: DiagnosticsCompareRunSpanRequest,
    ) -> Result<InvestigationSpanComparison> {
        match self
            .send(DiagnosticsRequest::CompareRunSpan {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                request,
            })
            .await?
        {
            DiagnosticsResponse::InvestigationSpanComparison { comparison } => Ok(*comparison),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn create_case(
        &self,
        lease_id: &str,
        title: &str,
        problem_statement: Option<String>,
        workload_identity: Option<String>,
        target_environment: Option<String>,
    ) -> Result<InvestigationCase> {
        match self
            .send(DiagnosticsRequest::CreateCase {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                title: title.to_string(),
                problem_statement,
                workload_identity,
                target_environment,
            })
            .await?
        {
            DiagnosticsResponse::InvestigationCase { case } => Ok(*case),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn create_run(
        &self,
        lease_id: &str,
        case_id: &str,
        run_workload_identity: Option<String>,
    ) -> Result<InvestigationRun> {
        match self
            .send(DiagnosticsRequest::CreateRun {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                case_id: case_id.to_string(),
                run_workload_identity,
            })
            .await?
        {
            DiagnosticsResponse::InvestigationRun { run } => Ok(*run),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn start_workflow(
        &self,
        lease_id: &str,
        run_id: &str,
        workflow_id: &str,
    ) -> Result<InvestigationWorkflowRun> {
        match self
            .send(DiagnosticsRequest::StartWorkflow {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                run_id: run_id.to_string(),
                workflow_id: workflow_id.to_string(),
            })
            .await?
        {
            DiagnosticsResponse::WorkflowRun { workflow } => Ok(*workflow),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn workflow_run(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
    ) -> Result<InvestigationWorkflowRun> {
        match self
            .send(DiagnosticsRequest::GetWorkflowRun {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                workflow_run_id: workflow_run_id.to_string(),
            })
            .await?
        {
            DiagnosticsResponse::WorkflowRun { workflow } => Ok(*workflow),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn advance_workflow_step(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
        step_id: &str,
    ) -> Result<InvestigationWorkflowRun> {
        match self
            .send(DiagnosticsRequest::AdvanceWorkflowStep {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                workflow_run_id: workflow_run_id.to_string(),
                step_id: step_id.to_string(),
            })
            .await?
        {
            DiagnosticsResponse::WorkflowRun { workflow } => Ok(*workflow),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn acknowledge_manual_step(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
        step_id: &str,
        actor: Option<String>,
        note: Option<String>,
    ) -> Result<InvestigationWorkflowRun> {
        match self
            .send(DiagnosticsRequest::AcknowledgeManualStep {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                workflow_run_id: workflow_run_id.to_string(),
                step_id: step_id.to_string(),
                actor,
                note,
            })
            .await?
        {
            DiagnosticsResponse::WorkflowRun { workflow } => Ok(*workflow),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn abort_workflow(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
        reason: Option<String>,
    ) -> Result<InvestigationWorkflowRun> {
        match self
            .send(DiagnosticsRequest::AbortWorkflow {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                workflow_run_id: workflow_run_id.to_string(),
                reason,
            })
            .await?
        {
            DiagnosticsResponse::WorkflowRun { workflow } => Ok(*workflow),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn complete_workflow(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
    ) -> Result<InvestigationWorkflowRun> {
        match self
            .send(DiagnosticsRequest::CompleteWorkflow {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                workflow_run_id: workflow_run_id.to_string(),
            })
            .await?
        {
            DiagnosticsResponse::WorkflowRun { workflow } => Ok(*workflow),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn create_hypothesis(
        &self,
        lease_id: &str,
        case_id: &str,
        statement: String,
        expected_signal: String,
        supporting_evidence: Vec<DiagnosticsEvidenceReference>,
    ) -> Result<InvestigationHypothesis> {
        match self
            .send(DiagnosticsRequest::CreateHypothesis {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                case_id: case_id.to_string(),
                statement,
                expected_signal,
                supporting_evidence,
            })
            .await?
        {
            DiagnosticsResponse::InvestigationHypothesis { hypothesis } => Ok(*hypothesis),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn update_hypothesis_status(
        &self,
        lease_id: &str,
        hypothesis_id: &str,
        status: InvestigationHypothesisStatus,
        actor: Option<String>,
        reason: Option<String>,
        supporting_evidence: Vec<DiagnosticsEvidenceReference>,
    ) -> Result<InvestigationHypothesis> {
        match self
            .send(DiagnosticsRequest::UpdateHypothesisStatus {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                hypothesis_id: hypothesis_id.to_string(),
                status,
                actor,
                reason,
                supporting_evidence,
            })
            .await?
        {
            DiagnosticsResponse::InvestigationHypothesis { hypothesis } => Ok(*hypothesis),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn profiling_status(&self) -> Result<DiagnosticsProfilingStatus> {
        match self
            .send(DiagnosticsRequest::ProfilingStatus {
                auth_token: self.discovery.auth_token.clone(),
            })
            .await?
        {
            DiagnosticsResponse::ProfilingStatus { status } => Ok(*status),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn start_profiling_window(
        &self,
        lease_id: &str,
        run_id: &str,
        request: StartProfilingWindowRequest,
    ) -> Result<ProfilingWindowRecord> {
        match self
            .send(DiagnosticsRequest::StartProfilingWindow {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                run_id: run_id.to_string(),
                request,
            })
            .await?
        {
            DiagnosticsResponse::ProfilingWindow { window } => Ok(*window),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn stop_profiling_window(
        &self,
        lease_id: &str,
        window_id: &str,
    ) -> Result<ProfilingWindowRecord> {
        match self
            .send(DiagnosticsRequest::StopProfilingWindow {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                window_id: window_id.to_string(),
            })
            .await?
        {
            DiagnosticsResponse::ProfilingWindow { window } => Ok(*window),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn invoke_safe_action(
        &self,
        lease_id: &str,
        run_id: &str,
        action: SafeDebugActionRequest,
    ) -> Result<SafeDebugActionOutcome> {
        if self.discovery.protocol_version < DIAGNOSTICS_SAFE_ACTIONS_PROTOCOL_VERSION {
            return Err(anyhow!(
                "safe debug actions require diagnostics protocol version {} or newer",
                DIAGNOSTICS_SAFE_ACTIONS_PROTOCOL_VERSION
            ));
        }
        match self
            .send(DiagnosticsRequest::InvokeSafeAction {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                run_id: run_id.to_string(),
                action,
            })
            .await?
        {
            DiagnosticsResponse::SafeDebugAction { outcome } => Ok(*outcome),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(crate) async fn append_timeline_entry(
        &self,
        lease_id: &str,
        run_id: &str,
        entry: NewTimelineEntry,
    ) -> Result<TimelineEntry> {
        match self
            .send(DiagnosticsRequest::AppendTimelineEntry {
                auth_token: self.discovery.auth_token.clone(),
                lease_id: lease_id.to_string(),
                run_id: run_id.to_string(),
                source: entry.source,
                kind: entry.kind,
                title: entry.title,
                body: entry.body,
                actor: entry.actor,
                reason: entry.reason,
            })
            .await?
        {
            DiagnosticsResponse::TimelineEntry { entry } => Ok(entry),
            DiagnosticsResponse::Error { message, .. } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected diagnostics response: {other:?}")),
        }
    }

    pub(super) async fn send(&self, request: DiagnosticsRequest) -> Result<DiagnosticsResponse> {
        #[cfg(unix)]
        {
            let LocalDiagnosticsEndpoint::UnixSocket { path } = &self.discovery.endpoint;
            let stream = connect_unix_with_retry(path).await?;
            send_request(stream, request).await
        }

        #[cfg(windows)]
        {
            let LocalDiagnosticsEndpoint::WindowsNamedPipe { name } = &self.discovery.endpoint;
            let stream = connect_named_pipe_with_retry(name).await?;
            send_request(stream, request).await
        }
    }
}

use super::*;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum DiagnosticsRequest {
    SessionInfo {
        auth_token: String,
    },
    ListWorkflows {
        auth_token: String,
    },
    Health {
        auth_token: String,
    },
    RuntimeSnapshot {
        auth_token: String,
    },
    RecentEvents {
        auth_token: String,
        limit: Option<usize>,
    },
    EvidenceView {
        auth_token: String,
        lease_id: String,
        run_id: String,
        request: DiagnosticsEvidenceRequest,
    },
    MaterializeReport {
        auth_token: String,
        lease_id: String,
        run_id: String,
    },
    CompareRunSpan {
        auth_token: String,
        lease_id: String,
        request: DiagnosticsCompareRunSpanRequest,
    },
    AcquireController {
        auth_token: String,
        client_name: String,
        ttl_ms: Option<u64>,
    },
    RenewController {
        auth_token: String,
        lease_id: String,
        ttl_ms: Option<u64>,
    },
    CreateCase {
        auth_token: String,
        lease_id: String,
        title: String,
        problem_statement: Option<String>,
        workload_identity: Option<String>,
        target_environment: Option<String>,
    },
    CreateRun {
        auth_token: String,
        lease_id: String,
        case_id: String,
        run_workload_identity: Option<String>,
    },
    StartWorkflow {
        auth_token: String,
        lease_id: String,
        run_id: String,
        workflow_id: String,
    },
    GetWorkflowRun {
        auth_token: String,
        lease_id: String,
        workflow_run_id: String,
    },
    AdvanceWorkflowStep {
        auth_token: String,
        lease_id: String,
        workflow_run_id: String,
        step_id: String,
    },
    AcknowledgeManualStep {
        auth_token: String,
        lease_id: String,
        workflow_run_id: String,
        step_id: String,
        actor: Option<String>,
        note: Option<String>,
    },
    AbortWorkflow {
        auth_token: String,
        lease_id: String,
        workflow_run_id: String,
        reason: Option<String>,
    },
    CompleteWorkflow {
        auth_token: String,
        lease_id: String,
        workflow_run_id: String,
    },
    CreateHypothesis {
        auth_token: String,
        lease_id: String,
        case_id: String,
        statement: String,
        expected_signal: String,
        supporting_evidence: Vec<DiagnosticsEvidenceReference>,
    },
    UpdateHypothesisStatus {
        auth_token: String,
        lease_id: String,
        hypothesis_id: String,
        status: InvestigationHypothesisStatus,
        actor: Option<String>,
        reason: Option<String>,
        supporting_evidence: Vec<DiagnosticsEvidenceReference>,
    },
    StartProfilingWindow {
        auth_token: String,
        lease_id: String,
        run_id: String,
        request: StartProfilingWindowRequest,
    },
    StopProfilingWindow {
        auth_token: String,
        lease_id: String,
        window_id: String,
    },
    ProfilingStatus {
        auth_token: String,
    },
    InvokeSafeAction {
        auth_token: String,
        lease_id: String,
        run_id: String,
        action: SafeDebugActionRequest,
    },
    AppendTimelineEntry {
        auth_token: String,
        lease_id: String,
        run_id: String,
        source: TimelineEntrySource,
        kind: TimelineEntryKind,
        title: String,
        body: Option<String>,
        actor: Option<String>,
        reason: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum DiagnosticsResponse {
    SessionInfo {
        session: DiagnosticsSessionInfo,
    },
    WorkflowPresets {
        workflows: Vec<InvestigationWorkflowPreset>,
    },
    Health {
        health: Box<DiagnosticsHealthSnapshot>,
    },
    RuntimeSnapshot {
        snapshot: Box<DiagnosticsRuntimeSnapshot>,
    },
    RecentEvents {
        events: Vec<DiagnosticsRecentEvent>,
    },
    EvidenceView {
        view: Box<DiagnosticsEvidenceView>,
    },
    InvestigationReport {
        report: Box<InvestigationReport>,
    },
    InvestigationSpanComparison {
        comparison: Box<InvestigationSpanComparison>,
    },
    ControllerLease {
        lease: ControllerLease,
    },
    InvestigationCase {
        case: Box<InvestigationCase>,
    },
    InvestigationRun {
        run: Box<InvestigationRun>,
    },
    WorkflowRun {
        workflow: Box<InvestigationWorkflowRun>,
    },
    InvestigationHypothesis {
        hypothesis: Box<InvestigationHypothesis>,
    },
    ProfilingWindow {
        window: Box<ProfilingWindowRecord>,
    },
    ProfilingStatus {
        status: Box<DiagnosticsProfilingStatus>,
    },
    SafeDebugAction {
        outcome: Box<SafeDebugActionOutcome>,
    },
    TimelineEntry {
        entry: TimelineEntry,
    },
    Error {
        code: String,
        message: String,
    },
}

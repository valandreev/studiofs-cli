use super::*;

type DiagnosticsServiceResult<T> = std::result::Result<T, DiagnosticsServiceError>;

pub(super) struct DiagnosticsSessionDeps<'a> {
    pub(super) source: &'a dyn DiagnosticsDataSource,
    pub(super) leases: &'a ControllerLeaseManager,
    pub(super) store: Option<&'a InvestigationStore>,
    pub(super) profiling: Option<&'a DiagnosticsProfilingCoordinator>,
}

pub(super) struct DiagnosticsSessionService<'a> {
    deps: DiagnosticsSessionDeps<'a>,
}

#[derive(Debug)]
pub(super) enum DiagnosticsServiceError {
    InvestigationUnavailable,
    InvestigationStore(anyhow::Error),
    LeaseInvalid(anyhow::Error),
    ProfilingUnavailable,
    ControllerBusy(anyhow::Error),
    Workflow { code: &'static str, message: String },
    ProfilingStart(ProfilingWindowStartError),
    ProfilingStop(anyhow::Error),
}

impl DiagnosticsServiceError {
    fn workflow(code: &'static str, message: impl Into<String>) -> Self {
        Self::Workflow {
            code,
            message: message.into(),
        }
    }

    fn into_response(self) -> DiagnosticsResponse {
        match self {
            Self::InvestigationUnavailable => DiagnosticsResponse::Error {
                code: "investigation_unavailable".to_string(),
                message: "investigation persistence is unavailable for this session".to_string(),
            },
            Self::InvestigationStore(error) => store_error_response(error),
            Self::LeaseInvalid(error) => DiagnosticsResponse::Error {
                code: "lease_invalid".to_string(),
                message: error.to_string(),
            },
            Self::ProfilingUnavailable => DiagnosticsResponse::Error {
                code: "profiling_unavailable".to_string(),
                message: "profiling control is unavailable for this session".to_string(),
            },
            Self::ControllerBusy(error) => DiagnosticsResponse::Error {
                code: "controller_busy".to_string(),
                message: error.to_string(),
            },
            Self::Workflow { code, message } => workflow_error_response(code, message),
            Self::ProfilingStart(error) => match error {
                ProfilingWindowStartError::Budget(error) => DiagnosticsResponse::Error {
                    code: "profiling_budget_invalid".to_string(),
                    message: error.to_string(),
                },
                ProfilingWindowStartError::Busy => DiagnosticsResponse::Error {
                    code: "profiling_start_failed".to_string(),
                    message: error.to_string(),
                },
                ProfilingWindowStartError::Store(error) => store_error_response(error),
            },
            Self::ProfilingStop(error) => DiagnosticsResponse::Error {
                code: "profiling_stop_failed".to_string(),
                message: error.to_string(),
            },
        }
    }
}

impl<'a> DiagnosticsSessionService<'a> {
    pub(super) fn new(deps: DiagnosticsSessionDeps<'a>) -> Self {
        Self { deps }
    }

    fn require_store(&self, lease_id: &str) -> DiagnosticsServiceResult<&'a InvestigationStore> {
        self.deps
            .leases
            .require_current(lease_id)
            .map_err(DiagnosticsServiceError::LeaseInvalid)?;
        self.deps
            .store
            .ok_or(DiagnosticsServiceError::InvestigationUnavailable)
    }

    fn require_profiling(&self) -> DiagnosticsServiceResult<&'a DiagnosticsProfilingCoordinator> {
        self.deps
            .profiling
            .ok_or(DiagnosticsServiceError::ProfilingUnavailable)
    }

    pub(super) fn session_info(&self) -> DiagnosticsServiceResult<DiagnosticsSessionInfo> {
        let mut session = self.deps.source.session_info();
        session.controller = self.deps.leases.snapshot();
        Ok(session)
    }

    pub(super) fn list_workflows(
        &self,
    ) -> DiagnosticsServiceResult<Vec<InvestigationWorkflowPreset>> {
        Ok(built_in_workflow_presets())
    }

    pub(super) fn health_snapshot(&self) -> DiagnosticsServiceResult<DiagnosticsHealthSnapshot> {
        Ok(self.deps.source.health_snapshot())
    }

    pub(super) fn runtime_snapshot(&self) -> DiagnosticsServiceResult<DiagnosticsRuntimeSnapshot> {
        Ok(self.deps.source.runtime_snapshot())
    }

    pub(super) fn recent_events(
        &self,
        limit: Option<usize>,
    ) -> DiagnosticsServiceResult<Vec<DiagnosticsRecentEvent>> {
        let limit = limit
            .unwrap_or(DEFAULT_RECENT_EVENTS_LIMIT)
            .clamp(1, MAX_RECENT_EVENTS_LIMIT);
        Ok(self.deps.source.recent_events(limit))
    }

    pub(super) fn evidence_view(
        &self,
        lease_id: &str,
        run_id: &str,
        request: DiagnosticsEvidenceRequest,
    ) -> DiagnosticsServiceResult<DiagnosticsEvidenceView> {
        let store = self.require_store(lease_id)?;
        build_evidence_view(run_id, request, self.deps.source, store)
            .map_err(DiagnosticsServiceError::InvestigationStore)
    }

    pub(super) fn materialize_report(
        &self,
        lease_id: &str,
        run_id: &str,
    ) -> DiagnosticsServiceResult<InvestigationReport> {
        let store = self.require_store(lease_id)?;
        super::materialize_report(run_id, self.deps.source, store)
            .map_err(DiagnosticsServiceError::InvestigationStore)
    }

    pub(super) fn compare_run_span(
        &self,
        lease_id: &str,
        request: DiagnosticsCompareRunSpanRequest,
    ) -> DiagnosticsServiceResult<InvestigationSpanComparison> {
        let store = self.require_store(lease_id)?;
        materialize_run_span_comparison(request, store)
            .map_err(DiagnosticsServiceError::InvestigationStore)
    }

    pub(super) fn profiling_status(&self) -> DiagnosticsServiceResult<DiagnosticsProfilingStatus> {
        Ok(self.require_profiling()?.status())
    }

    pub(super) fn invoke_safe_action(
        &self,
        lease_id: &str,
        run_id: &str,
        action: SafeDebugActionRequest,
    ) -> DiagnosticsServiceResult<SafeDebugActionOutcome> {
        let store = self.require_store(lease_id)?;
        store
            .load_run(run_id)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        let actor = action.actor().map(str::to_string);
        let reason = action.reason().map(str::to_string);
        let mut outcome = match action {
            SafeDebugActionRequest::SetLogVerbosity { level } => {
                match crate::set_diagnostics_log_verbosity(level) {
                    Ok(()) => SafeDebugActionOutcome {
                        action: SafeDebugActionKind::SetLogVerbosity,
                        loud: false,
                        status: SafeDebugActionStatus::Succeeded,
                        summary: serde_json::json!({
                            "level": level.as_directive(),
                        }),
                        artifact: None,
                        error: None,
                        audit_error: None,
                    },
                    Err(error) => SafeDebugActionOutcome {
                        action: SafeDebugActionKind::SetLogVerbosity,
                        loud: false,
                        status: SafeDebugActionStatus::Failed,
                        summary: serde_json::json!({
                            "level": level.as_directive(),
                        }),
                        artifact: None,
                        error: Some(error.to_string()),
                        audit_error: None,
                    },
                }
            }
            SafeDebugActionRequest::CaptureBundle {
                recent_events_limit,
                ..
            } => {
                let session = self.deps.source.session_info();
                let snapshot = self.deps.source.runtime_snapshot();
                let events = self.deps.source.recent_events(
                    recent_events_limit
                        .unwrap_or(DEFAULT_RECENT_EVENTS_LIMIT)
                        .clamp(1, MAX_RECENT_EVENTS_LIMIT),
                );
                match capture_bundle_artifact(&session, &snapshot, &events) {
                    Ok(artifact) => SafeDebugActionOutcome {
                        action: SafeDebugActionKind::CaptureBundle,
                        loud: true,
                        status: SafeDebugActionStatus::Succeeded,
                        summary: serde_json::json!({
                            "recent_events": events.len(),
                            "path": artifact.path.clone(),
                        }),
                        artifact: Some(artifact),
                        error: None,
                        audit_error: None,
                    },
                    Err(error) => SafeDebugActionOutcome {
                        action: SafeDebugActionKind::CaptureBundle,
                        loud: true,
                        status: SafeDebugActionStatus::Failed,
                        summary: serde_json::json!({
                            "recent_events": events.len(),
                        }),
                        artifact: None,
                        error: Some(error.to_string()),
                        audit_error: None,
                    },
                }
            }
            SafeDebugActionRequest::ResetObservabilityCounters { .. } => {
                match self.deps.source.reset_observability_counters() {
                    Ok(()) => SafeDebugActionOutcome {
                        action: SafeDebugActionKind::ResetObservabilityCounters,
                        loud: true,
                        status: SafeDebugActionStatus::Succeeded,
                        summary: serde_json::json!({
                            "status": "reset",
                        }),
                        artifact: None,
                        error: None,
                        audit_error: None,
                    },
                    Err(error) => SafeDebugActionOutcome {
                        action: SafeDebugActionKind::ResetObservabilityCounters,
                        loud: true,
                        status: SafeDebugActionStatus::Failed,
                        summary: serde_json::json!({}),
                        artifact: None,
                        error: Some(error.to_string()),
                        audit_error: None,
                    },
                }
            }
            SafeDebugActionRequest::LightweightSelfCheck { .. } => {
                let snapshot = self.deps.source.runtime_snapshot();
                let events = self
                    .deps
                    .source
                    .recent_events(DEFAULT_RECENT_EVENTS_LIMIT.min(8));
                SafeDebugActionOutcome {
                    action: SafeDebugActionKind::LightweightSelfCheck,
                    loud: true,
                    status: SafeDebugActionStatus::Succeeded,
                    summary: serde_json::json!({
                        "status": "ok",
                        "pending_uploads": snapshot.upload.pending_uploads,
                        "recent_events": events.len(),
                    }),
                    artifact: None,
                    error: None,
                    audit_error: None,
                }
            }
        };
        if outcome.loud {
            if let Err(error) = append_safe_action_audit(
                store,
                run_id,
                outcome.action,
                actor.as_deref(),
                reason.as_deref(),
                &outcome,
            ) {
                warn!(
                    run_id,
                    action = ?outcome.action,
                    error = %error,
                    "failed to persist safe action audit entry"
                );
                outcome.audit_error = Some(error.to_string());
            }
        }
        Ok(outcome)
    }

    pub(super) fn acquire_controller(
        &self,
        client_name: String,
        ttl_ms: Option<u64>,
    ) -> DiagnosticsServiceResult<ControllerLease> {
        self.deps
            .leases
            .acquire(client_name, ttl_ms)
            .map_err(DiagnosticsServiceError::ControllerBusy)
    }

    pub(super) fn renew_controller(
        &self,
        lease_id: &str,
        ttl_ms: Option<u64>,
    ) -> DiagnosticsServiceResult<ControllerLease> {
        self.deps
            .leases
            .renew(lease_id, ttl_ms)
            .map_err(DiagnosticsServiceError::LeaseInvalid)
    }

    pub(super) fn create_case(
        &self,
        lease_id: &str,
        title: String,
        problem_statement: Option<String>,
        workload_identity: Option<String>,
        target_environment: Option<String>,
    ) -> DiagnosticsServiceResult<InvestigationCase> {
        self.require_store(lease_id)?
            .create_case(NewInvestigationCase {
                title,
                problem_statement,
                workload_identity,
                target_environment,
            })
            .map_err(DiagnosticsServiceError::InvestigationStore)
    }

    pub(super) fn create_run(
        &self,
        lease_id: &str,
        case_id: String,
        run_workload_identity: Option<String>,
    ) -> DiagnosticsServiceResult<InvestigationRun> {
        let store = self.require_store(lease_id)?;
        let case = store
            .load_case(&case_id)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        let fingerprint = self.deps.source.environment_fingerprint(
            case.workload_identity.as_deref(),
            run_workload_identity.as_deref(),
        );
        store
            .create_run(NewInvestigationRun {
                case_id,
                session_id: self.deps.source.session_info().session_id,
                fingerprint,
            })
            .map_err(DiagnosticsServiceError::InvestigationStore)
    }

    pub(super) fn start_workflow(
        &self,
        lease_id: &str,
        run_id: &str,
        workflow_id: String,
    ) -> DiagnosticsServiceResult<InvestigationWorkflowRun> {
        let store = self.require_store(lease_id)?;
        store
            .load_run(run_id)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        let Some(workflow) = built_in_workflow_by_id(&workflow_id) else {
            return Err(DiagnosticsServiceError::workflow(
                "workflow_unknown",
                format!("workflow preset '{}' is not registered", workflow_id),
            ));
        };
        let mut workflow_run = store
            .create_workflow_run(run_id, NewInvestigationWorkflowRun { workflow })
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        if workflow_run.current_step_id.is_some() {
            workflow_run.status = InvestigationWorkflowStatus::InProgress;
            workflow_run.updated_unix_ms = now_unix_millis();
            store
                .save_workflow_run(&workflow_run)
                .map_err(DiagnosticsServiceError::InvestigationStore)?;
        }
        if let Err(error) = append_workflow_timeline_event(
            store,
            run_id,
            "workflow_started",
            serde_json::json!({
                "workflow_run_id": workflow_run.id,
                "workflow_id": workflow_run.workflow_id,
            }),
        ) {
            warn!(
                run_id = %run_id,
                workflow_id = %workflow_run.workflow_id,
                error = %error,
                "failed to append workflow start timeline event"
            );
        }
        Ok(workflow_run)
    }

    pub(super) fn get_workflow_run(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
    ) -> DiagnosticsServiceResult<InvestigationWorkflowRun> {
        self.require_store(lease_id)?
            .load_workflow_run(workflow_run_id)
            .map_err(DiagnosticsServiceError::InvestigationStore)
    }

    pub(super) fn advance_workflow_step(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
        step_id: &str,
    ) -> DiagnosticsServiceResult<InvestigationWorkflowRun> {
        let store = self.require_store(lease_id)?;
        advance_workflow_step_operation(
            store,
            self.deps.source,
            self.deps.profiling,
            workflow_run_id,
            step_id,
        )
    }

    pub(super) fn acknowledge_manual_step(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
        step_id: &str,
        actor: Option<String>,
        note: Option<String>,
    ) -> DiagnosticsServiceResult<InvestigationWorkflowRun> {
        let store = self.require_store(lease_id)?;
        let mut workflow = store
            .load_workflow_run(workflow_run_id)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        if workflow.current_step_id.as_deref() != Some(step_id) {
            return Err(DiagnosticsServiceError::workflow(
                "workflow_invalid_step",
                format!(
                    "workflow step '{}' is not current for workflow run '{}'",
                    step_id, workflow.id
                ),
            ));
        }
        let step_index = workflow_step_index(&workflow, step_id).map_err(|error| {
            DiagnosticsServiceError::workflow("workflow_invalid_step", error.to_string())
        })?;
        if workflow.steps[step_index].status != InvestigationWorkflowStepStatus::WaitingForOperator
        {
            return Err(DiagnosticsServiceError::workflow(
                "workflow_invalid_step",
                format!("workflow step '{}' is not waiting for operator", step_id),
            ));
        }
        if let Some(marker_title) = workflow.steps[step_index].on_exit_marker.as_deref() {
            if let Err(error) =
                append_workflow_marker(store, &workflow.run_id, marker_title, self.deps.source)
            {
                return Err(DiagnosticsServiceError::InvestigationStore(error));
            }
        }
        workflow.steps[step_index].status = InvestigationWorkflowStepStatus::Completed;
        workflow.steps[step_index].completed_unix_ms = Some(now_unix_millis());
        workflow.steps[step_index].operator_acknowledged_by = actor.clone();
        workflow.steps[step_index].operator_acknowledgement_note = note.clone();
        workflow.steps[step_index].output_summary = Some(serde_json::json!({
            "acknowledged_by": actor,
            "note": note,
        }));
        complete_workflow_progress(&mut workflow);
        store
            .save_workflow_run(&workflow)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        if let Err(error) = append_workflow_timeline_event(
            store,
            &workflow.run_id,
            "workflow_manual_acknowledged",
            serde_json::json!({
                "workflow_run_id": workflow.id,
                "step_id": step_id,
            }),
        ) {
            warn!(
                workflow_run_id = %workflow.id,
                step_id = %step_id,
                error = %error,
                "failed to append manual acknowledgement timeline event"
            );
        }
        Ok(workflow)
    }

    pub(super) fn abort_workflow(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
        reason: Option<String>,
    ) -> DiagnosticsServiceResult<InvestigationWorkflowRun> {
        let store = self.require_store(lease_id)?;
        let mut workflow = store
            .load_workflow_run(workflow_run_id)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        if let Some(current_step_id) = workflow.current_step_id.as_deref() {
            if let Ok(step_index) = workflow_step_index(&workflow, current_step_id) {
                if !workflow_step_is_terminal(workflow.steps[step_index].status) {
                    workflow.steps[step_index].status = InvestigationWorkflowStepStatus::Aborted;
                    workflow.steps[step_index].completed_unix_ms = Some(now_unix_millis());
                    workflow.steps[step_index].last_error = reason.clone();
                }
            }
        }
        workflow.status = InvestigationWorkflowStatus::Aborted;
        workflow.current_step_id = None;
        workflow.updated_unix_ms = now_unix_millis();
        store
            .save_workflow_run(&workflow)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        if let Err(error) = append_workflow_timeline_event(
            store,
            &workflow.run_id,
            "workflow_aborted",
            serde_json::json!({
                "workflow_run_id": workflow.id,
                "reason": reason,
            }),
        ) {
            warn!(
                workflow_run_id = %workflow.id,
                error = %error,
                "failed to append workflow abort timeline event"
            );
        }
        Ok(workflow)
    }

    pub(super) fn complete_workflow(
        &self,
        lease_id: &str,
        workflow_run_id: &str,
    ) -> DiagnosticsServiceResult<InvestigationWorkflowRun> {
        let store = self.require_store(lease_id)?;
        let mut workflow = store
            .load_workflow_run(workflow_run_id)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        if workflow
            .steps
            .iter()
            .any(|step| !workflow_step_is_terminal(step.status))
        {
            return Err(DiagnosticsServiceError::workflow(
                "workflow_incomplete",
                format!("workflow run '{}' still has unfinished steps", workflow.id),
            ));
        }
        workflow.status = InvestigationWorkflowStatus::Completed;
        workflow.current_step_id = None;
        workflow.updated_unix_ms = now_unix_millis();
        store
            .save_workflow_run(&workflow)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        if let Err(error) = append_workflow_timeline_event(
            store,
            &workflow.run_id,
            "workflow_completed",
            serde_json::json!({
                "workflow_run_id": workflow.id,
                "workflow_id": workflow.workflow_id,
            }),
        ) {
            warn!(
                workflow_run_id = %workflow.id,
                error = %error,
                "failed to append workflow completion timeline event"
            );
        }
        Ok(workflow)
    }

    pub(super) fn create_hypothesis(
        &self,
        lease_id: &str,
        case_id: &str,
        statement: String,
        expected_signal: String,
        supporting_evidence: Vec<DiagnosticsEvidenceReference>,
    ) -> DiagnosticsServiceResult<InvestigationHypothesis> {
        self.require_store(lease_id)?
            .create_hypothesis(
                case_id,
                NewInvestigationHypothesis {
                    statement,
                    expected_signal,
                    supporting_evidence,
                },
            )
            .map_err(DiagnosticsServiceError::InvestigationStore)
    }

    pub(super) fn update_hypothesis_status(
        &self,
        lease_id: &str,
        hypothesis_id: &str,
        status: InvestigationHypothesisStatus,
        actor: Option<String>,
        reason: Option<String>,
        supporting_evidence: Vec<DiagnosticsEvidenceReference>,
    ) -> DiagnosticsServiceResult<InvestigationHypothesis> {
        self.require_store(lease_id)?
            .update_hypothesis_status(
                hypothesis_id,
                UpdateInvestigationHypothesisStatus {
                    status,
                    actor,
                    reason,
                    supporting_evidence,
                },
            )
            .map_err(DiagnosticsServiceError::InvestigationStore)
    }

    pub(super) fn start_profiling_window(
        &self,
        lease_id: &str,
        run_id: &str,
        request: StartProfilingWindowRequest,
    ) -> DiagnosticsServiceResult<ProfilingWindowRecord> {
        let store = self.require_store(lease_id)?;
        self.require_profiling()?
            .start_window(run_id, request, Some(store))
            .map_err(DiagnosticsServiceError::ProfilingStart)
    }

    pub(super) fn stop_profiling_window(
        &self,
        lease_id: &str,
        window_id: &str,
    ) -> DiagnosticsServiceResult<ProfilingWindowRecord> {
        let store = self.require_store(lease_id)?;
        self.require_profiling()?
            .stop_window(window_id, Some(store))
            .map_err(DiagnosticsServiceError::ProfilingStop)
    }

    pub(super) fn append_timeline_entry(
        &self,
        lease_id: &str,
        run_id: &str,
        entry: NewTimelineEntry,
    ) -> DiagnosticsServiceResult<TimelineEntry> {
        let store = self.require_store(lease_id)?;
        let entry = store
            .append_timeline_entry(run_id, entry)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        if entry.kind == TimelineEntryKind::Marker {
            let runtime_snapshot = self.deps.source.runtime_snapshot();
            if let Err(error) = store.append_marker_snapshot(
                run_id,
                NewInvestigationMarkerSnapshot {
                    marker_sequence: entry.sequence,
                    marker_title: entry.title.clone(),
                    runtime_snapshot,
                },
            ) {
                warn!(
                    run_id = %run_id,
                    marker_sequence = entry.sequence,
                    error = %error,
                    "failed to persist marker snapshot after timeline marker append"
                );
            }
        }
        Ok(entry)
    }
}

pub(super) fn dispatch_protocol(
    service: &DiagnosticsSessionService<'_>,
    auth_token: &str,
    request: DiagnosticsRequest,
) -> DiagnosticsResponse {
    match request {
        DiagnosticsRequest::SessionInfo {
            auth_token: provided,
        } => authorized_response(
            &provided,
            auth_token,
            || service.session_info(),
            |session| DiagnosticsResponse::SessionInfo { session },
        ),
        DiagnosticsRequest::ListWorkflows {
            auth_token: provided,
        } => authorized_response(
            &provided,
            auth_token,
            || service.list_workflows(),
            |workflows| DiagnosticsResponse::WorkflowPresets { workflows },
        ),
        DiagnosticsRequest::Health {
            auth_token: provided,
        } => authorized_response(
            &provided,
            auth_token,
            || service.health_snapshot(),
            |health| DiagnosticsResponse::Health {
                health: Box::new(health),
            },
        ),
        DiagnosticsRequest::RuntimeSnapshot {
            auth_token: provided,
        } => authorized_response(
            &provided,
            auth_token,
            || service.runtime_snapshot(),
            |snapshot| DiagnosticsResponse::RuntimeSnapshot {
                snapshot: Box::new(snapshot),
            },
        ),
        DiagnosticsRequest::RecentEvents {
            auth_token: provided,
            limit,
        } => authorized_response(
            &provided,
            auth_token,
            || service.recent_events(limit),
            |events| DiagnosticsResponse::RecentEvents { events },
        ),
        DiagnosticsRequest::EvidenceView {
            auth_token: provided,
            lease_id,
            run_id,
            request,
        } => authorized_response(
            &provided,
            auth_token,
            || service.evidence_view(&lease_id, &run_id, request),
            |view| DiagnosticsResponse::EvidenceView {
                view: Box::new(view),
            },
        ),
        DiagnosticsRequest::MaterializeReport {
            auth_token: provided,
            lease_id,
            run_id,
        } => authorized_response(
            &provided,
            auth_token,
            || service.materialize_report(&lease_id, &run_id),
            |report| DiagnosticsResponse::InvestigationReport {
                report: Box::new(report),
            },
        ),
        DiagnosticsRequest::CompareRunSpan {
            auth_token: provided,
            lease_id,
            request,
        } => authorized_response(
            &provided,
            auth_token,
            || service.compare_run_span(&lease_id, request),
            |comparison| DiagnosticsResponse::InvestigationSpanComparison {
                comparison: Box::new(comparison),
            },
        ),
        DiagnosticsRequest::AcquireController {
            auth_token: provided,
            client_name,
            ttl_ms,
        } => authorized_response(
            &provided,
            auth_token,
            || service.acquire_controller(client_name, ttl_ms),
            |lease| DiagnosticsResponse::ControllerLease { lease },
        ),
        DiagnosticsRequest::RenewController {
            auth_token: provided,
            lease_id,
            ttl_ms,
        } => authorized_response(
            &provided,
            auth_token,
            || service.renew_controller(&lease_id, ttl_ms),
            |lease| DiagnosticsResponse::ControllerLease { lease },
        ),
        DiagnosticsRequest::CreateCase {
            auth_token: provided,
            lease_id,
            title,
            problem_statement,
            workload_identity,
            target_environment,
        } => authorized_response(
            &provided,
            auth_token,
            || {
                service.create_case(
                    &lease_id,
                    title,
                    problem_statement,
                    workload_identity,
                    target_environment,
                )
            },
            |case| DiagnosticsResponse::InvestigationCase {
                case: Box::new(case),
            },
        ),
        DiagnosticsRequest::CreateRun {
            auth_token: provided,
            lease_id,
            case_id,
            run_workload_identity,
        } => authorized_response(
            &provided,
            auth_token,
            || service.create_run(&lease_id, case_id, run_workload_identity),
            |run| DiagnosticsResponse::InvestigationRun { run: Box::new(run) },
        ),
        DiagnosticsRequest::StartWorkflow {
            auth_token: provided,
            lease_id,
            run_id,
            workflow_id,
        } => authorized_response(
            &provided,
            auth_token,
            || service.start_workflow(&lease_id, &run_id, workflow_id),
            |workflow| DiagnosticsResponse::WorkflowRun {
                workflow: Box::new(workflow),
            },
        ),
        DiagnosticsRequest::GetWorkflowRun {
            auth_token: provided,
            lease_id,
            workflow_run_id,
        } => authorized_response(
            &provided,
            auth_token,
            || service.get_workflow_run(&lease_id, &workflow_run_id),
            |workflow| DiagnosticsResponse::WorkflowRun {
                workflow: Box::new(workflow),
            },
        ),
        DiagnosticsRequest::AdvanceWorkflowStep {
            auth_token: provided,
            lease_id,
            workflow_run_id,
            step_id,
        } => authorized_response(
            &provided,
            auth_token,
            || service.advance_workflow_step(&lease_id, &workflow_run_id, &step_id),
            |workflow| DiagnosticsResponse::WorkflowRun {
                workflow: Box::new(workflow),
            },
        ),
        DiagnosticsRequest::AcknowledgeManualStep {
            auth_token: provided,
            lease_id,
            workflow_run_id,
            step_id,
            actor,
            note,
        } => authorized_response(
            &provided,
            auth_token,
            || service.acknowledge_manual_step(&lease_id, &workflow_run_id, &step_id, actor, note),
            |workflow| DiagnosticsResponse::WorkflowRun {
                workflow: Box::new(workflow),
            },
        ),
        DiagnosticsRequest::AbortWorkflow {
            auth_token: provided,
            lease_id,
            workflow_run_id,
            reason,
        } => authorized_response(
            &provided,
            auth_token,
            || service.abort_workflow(&lease_id, &workflow_run_id, reason),
            |workflow| DiagnosticsResponse::WorkflowRun {
                workflow: Box::new(workflow),
            },
        ),
        DiagnosticsRequest::CompleteWorkflow {
            auth_token: provided,
            lease_id,
            workflow_run_id,
        } => authorized_response(
            &provided,
            auth_token,
            || service.complete_workflow(&lease_id, &workflow_run_id),
            |workflow| DiagnosticsResponse::WorkflowRun {
                workflow: Box::new(workflow),
            },
        ),
        DiagnosticsRequest::CreateHypothesis {
            auth_token: provided,
            lease_id,
            case_id,
            statement,
            expected_signal,
            supporting_evidence,
        } => authorized_response(
            &provided,
            auth_token,
            || {
                service.create_hypothesis(
                    &lease_id,
                    &case_id,
                    statement,
                    expected_signal,
                    supporting_evidence,
                )
            },
            |hypothesis| DiagnosticsResponse::InvestigationHypothesis {
                hypothesis: Box::new(hypothesis),
            },
        ),
        DiagnosticsRequest::UpdateHypothesisStatus {
            auth_token: provided,
            lease_id,
            hypothesis_id,
            status,
            actor,
            reason,
            supporting_evidence,
        } => authorized_response(
            &provided,
            auth_token,
            || {
                service.update_hypothesis_status(
                    &lease_id,
                    &hypothesis_id,
                    status,
                    actor,
                    reason,
                    supporting_evidence,
                )
            },
            |hypothesis| DiagnosticsResponse::InvestigationHypothesis {
                hypothesis: Box::new(hypothesis),
            },
        ),
        DiagnosticsRequest::StartProfilingWindow {
            auth_token: provided,
            lease_id,
            run_id,
            request,
        } => authorized_response(
            &provided,
            auth_token,
            || service.start_profiling_window(&lease_id, &run_id, request),
            |window| DiagnosticsResponse::ProfilingWindow {
                window: Box::new(window),
            },
        ),
        DiagnosticsRequest::StopProfilingWindow {
            auth_token: provided,
            lease_id,
            window_id,
        } => authorized_response(
            &provided,
            auth_token,
            || service.stop_profiling_window(&lease_id, &window_id),
            |window| DiagnosticsResponse::ProfilingWindow {
                window: Box::new(window),
            },
        ),
        DiagnosticsRequest::ProfilingStatus {
            auth_token: provided,
        } => authorized_response(
            &provided,
            auth_token,
            || service.profiling_status(),
            |status| DiagnosticsResponse::ProfilingStatus {
                status: Box::new(status),
            },
        ),
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: provided,
            lease_id,
            run_id,
            action,
        } => authorized_response(
            &provided,
            auth_token,
            || service.invoke_safe_action(&lease_id, &run_id, action),
            |outcome| DiagnosticsResponse::SafeDebugAction {
                outcome: Box::new(outcome),
            },
        ),
        DiagnosticsRequest::AppendTimelineEntry {
            auth_token: provided,
            lease_id,
            run_id,
            source,
            kind,
            title,
            body,
            actor,
            reason,
        } => authorized_response(
            &provided,
            auth_token,
            || {
                service.append_timeline_entry(
                    &lease_id,
                    &run_id,
                    NewTimelineEntry {
                        source,
                        kind,
                        title,
                        body,
                        actor,
                        reason,
                    },
                )
            },
            |entry| DiagnosticsResponse::TimelineEntry { entry },
        ),
    }
}

fn unauthorized_response() -> DiagnosticsResponse {
    DiagnosticsResponse::Error {
        code: "unauthorized".to_string(),
        message: "diagnostics auth token did not match".to_string(),
    }
}

fn authorized_response<T>(
    provided_token: &str,
    auth_token: &str,
    operation: impl FnOnce() -> DiagnosticsServiceResult<T>,
    on_ok: impl FnOnce(T) -> DiagnosticsResponse,
) -> DiagnosticsResponse {
    if provided_token != auth_token {
        return unauthorized_response();
    }
    match operation() {
        Ok(value) => on_ok(value),
        Err(error) => error.into_response(),
    }
}

fn advance_workflow_step_operation(
    store: &InvestigationStore,
    source: &dyn DiagnosticsDataSource,
    profiling: Option<&DiagnosticsProfilingCoordinator>,
    workflow_run_id: &str,
    step_id: &str,
) -> DiagnosticsServiceResult<InvestigationWorkflowRun> {
    let mut workflow = store
        .load_workflow_run(workflow_run_id)
        .map_err(DiagnosticsServiceError::InvestigationStore)?;
    if matches!(
        workflow.status,
        InvestigationWorkflowStatus::Completed
            | InvestigationWorkflowStatus::Failed
            | InvestigationWorkflowStatus::Aborted
    ) {
        return Err(DiagnosticsServiceError::workflow(
            "workflow_invalid",
            format!(
                "workflow run '{}' is already in terminal state {:?}",
                workflow.id, workflow.status
            ),
        ));
    }
    if workflow.current_step_id.as_deref() != Some(step_id) {
        return Err(DiagnosticsServiceError::workflow(
            "workflow_invalid_step",
            format!(
                "workflow step '{}' is not current for workflow run '{}'",
                step_id, workflow.id
            ),
        ));
    }
    let step_index = workflow_step_index(&workflow, step_id).map_err(|error| {
        DiagnosticsServiceError::workflow("workflow_invalid_step", error.to_string())
    })?;
    if !workflow_dependencies_satisfied(&workflow, &workflow.steps[step_index]) {
        return Err(DiagnosticsServiceError::workflow(
            "workflow_dependencies_unmet",
            format!("workflow step '{}' dependencies are not satisfied", step_id),
        ));
    }

    let step_kind = workflow.steps[step_index].kind;
    let step_failure_policy = workflow.steps[step_index].failure_policy;
    if step_kind == InvestigationWorkflowStepKind::ManualStep {
        if workflow.steps[step_index].status != InvestigationWorkflowStepStatus::Pending {
            return Err(DiagnosticsServiceError::workflow(
                "workflow_invalid_step",
                format!(
                    "manual workflow step '{}' is not pending",
                    workflow.steps[step_index].step_id
                ),
            ));
        }
        workflow.steps[step_index].status = InvestigationWorkflowStepStatus::WaitingForOperator;
        workflow.steps[step_index].started_unix_ms = Some(now_unix_millis());
        workflow.updated_unix_ms = now_unix_millis();
        workflow.status = InvestigationWorkflowStatus::WaitingForOperator;
        if let Some(marker_title) = workflow.steps[step_index].on_enter_marker.as_deref() {
            if let Err(error) =
                append_workflow_marker(store, &workflow.run_id, marker_title, source)
            {
                return Err(DiagnosticsServiceError::InvestigationStore(error));
            }
        }
        store
            .save_workflow_run(&workflow)
            .map_err(DiagnosticsServiceError::InvestigationStore)?;
        return Ok(workflow);
    }

    workflow.steps[step_index].status = InvestigationWorkflowStepStatus::InProgress;
    workflow.steps[step_index]
        .started_unix_ms
        .get_or_insert_with(now_unix_millis);
    workflow.updated_unix_ms = now_unix_millis();
    store
        .save_workflow_run(&workflow)
        .map_err(DiagnosticsServiceError::InvestigationStore)?;

    let result = match step_kind {
        InvestigationWorkflowStepKind::Marker => append_workflow_marker(
            store,
            &workflow.run_id,
            &workflow.steps[step_index].title,
            source,
        )
        .map(|_| {
            (
                serde_json::json!({ "marker": workflow.steps[step_index].title }),
                Vec::new(),
                None,
            )
        }),
        InvestigationWorkflowStepKind::Note => store
            .append_timeline_entry(
                &workflow.run_id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Mcp,
                    kind: TimelineEntryKind::Note,
                    title: workflow.steps[step_index].title.clone(),
                    body: Some(workflow.steps[step_index].instructions.clone()),
                    actor: Some("workflow".to_string()),
                    reason: None,
                },
            )
            .map(|_| {
                (
                    serde_json::json!({ "note": workflow.steps[step_index].title }),
                    Vec::new(),
                    None,
                )
            }),
        InvestigationWorkflowStepKind::ToolStep
        | InvestigationWorkflowStepKind::ProfilingWindow
        | InvestigationWorkflowStepKind::SafeAction
        | InvestigationWorkflowStepKind::ReportStep => {
            execute_workflow_tool_like_step(&workflow, step_index, source, store, profiling)
        }
        InvestigationWorkflowStepKind::ManualStep => unreachable!(),
    };

    match result {
        Ok((summary, artifacts, invocation)) => {
            workflow.steps[step_index].status = InvestigationWorkflowStepStatus::Completed;
            workflow.steps[step_index].completed_unix_ms = Some(now_unix_millis());
            workflow.steps[step_index].last_error = None;
            workflow.steps[step_index].output_summary = Some(summary);
            workflow.steps[step_index].artifacts = artifacts;
            workflow.steps[step_index].tool_invocation = invocation;
            complete_workflow_progress(&mut workflow);
            store
                .save_workflow_run(&workflow)
                .map_err(DiagnosticsServiceError::InvestigationStore)?;
            if let Err(error) = append_workflow_timeline_event(
                store,
                &workflow.run_id,
                "workflow_step_completed",
                serde_json::json!({
                    "workflow_run_id": workflow.id,
                    "step_id": workflow.steps[step_index].step_id,
                    "status": workflow.steps[step_index].status,
                }),
            ) {
                warn!(
                    workflow_run_id = %workflow.id,
                    step_id = %workflow.steps[step_index].step_id,
                    error = %error,
                    "failed to append workflow completion timeline event"
                );
            }
            Ok(workflow)
        }
        Err(error) => {
            workflow.steps[step_index].last_error = Some(error.to_string());
            match step_failure_policy {
                InvestigationWorkflowFailurePolicy::FailOpen => {
                    workflow.steps[step_index].status = InvestigationWorkflowStepStatus::Failed;
                    workflow.steps[step_index].completed_unix_ms = Some(now_unix_millis());
                    complete_workflow_progress(&mut workflow);
                    store
                        .save_workflow_run(&workflow)
                        .map_err(DiagnosticsServiceError::InvestigationStore)?;
                    Ok(workflow)
                }
                InvestigationWorkflowFailurePolicy::Retryable => {
                    workflow.steps[step_index].status = InvestigationWorkflowStepStatus::Pending;
                    workflow.status = InvestigationWorkflowStatus::InProgress;
                    workflow.current_step_id = Some(workflow.steps[step_index].step_id.clone());
                    store
                        .save_workflow_run(&workflow)
                        .map_err(DiagnosticsServiceError::InvestigationStore)?;
                    Err(DiagnosticsServiceError::workflow(
                        "workflow_step_failed",
                        error.to_string(),
                    ))
                }
                InvestigationWorkflowFailurePolicy::AbortWorkflow => {
                    workflow.steps[step_index].status = InvestigationWorkflowStepStatus::Failed;
                    workflow.steps[step_index].completed_unix_ms = Some(now_unix_millis());
                    workflow.status = InvestigationWorkflowStatus::Failed;
                    workflow.current_step_id = Some(workflow.steps[step_index].step_id.clone());
                    store
                        .save_workflow_run(&workflow)
                        .map_err(DiagnosticsServiceError::InvestigationStore)?;
                    Err(DiagnosticsServiceError::workflow(
                        "workflow_failed",
                        error.to_string(),
                    ))
                }
            }
        }
    }
}

use super::*;
use tempfile::TempDir;

const DIAGNOSTICS_TEST_LEASE_TTL_MS: u64 = 5_000;

#[derive(Clone)]
struct StaticDiagnosticsSource {
    session: DiagnosticsSessionInfo,
    health: DiagnosticsHealthSnapshot,
    recent_events: Vec<DiagnosticsRecentEvent>,
    fingerprint: EnvironmentFingerprint,
    reset_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl DiagnosticsDataSource for StaticDiagnosticsSource {
    fn session_info(&self) -> DiagnosticsSessionInfo {
        self.session.clone()
    }

    fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
        self.health.clone()
    }

    fn runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshot {
        DiagnosticsRuntimeSnapshot::from_health(self.health.clone())
    }

    fn recent_events(&self, limit: usize) -> Vec<DiagnosticsRecentEvent> {
        let keep = limit.min(self.recent_events.len());
        self.recent_events[self.recent_events.len().saturating_sub(keep)..].to_vec()
    }

    fn environment_fingerprint(
        &self,
        case_workload_identity: Option<&str>,
        run_workload_identity: Option<&str>,
    ) -> EnvironmentFingerprint {
        let mut fingerprint = self.fingerprint.clone();
        fingerprint.workload_identity.case_workload = case_workload_identity.map(str::to_string);
        fingerprint.workload_identity.run_workload = run_workload_identity.map(str::to_string);
        fingerprint
    }

    fn reset_observability_counters(&self) -> std::result::Result<(), DiagnosticsActionError> {
        self.reset_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

struct SuccessfulProfilingBackend;

impl RuntimeProfilingBackend for SuccessfulProfilingBackend {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode> {
        vec![
            ProfilingCaptureMode::WallBlocking,
            ProfilingCaptureMode::Cpu,
        ]
    }

    fn start_window(&self, request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        let sample_rate_hz = request
            .budget
            .sample_rate_hz
            .or(Some(DEFAULT_PROFILE_SAMPLE_RATE_HZ));
        ProfilingBackendStartResult::Active(ActiveRuntimeProfilingWindow {
            sample_rate_hz,
            stopper: Box::new(SuccessfulProfilingStopper),
        })
    }
}

struct SuccessfulProfilingStopper;

impl RuntimeProfilingStopper for SuccessfulProfilingStopper {
    fn stop(self: Box<Self>) -> Result<ProfilingBackendStopResult> {
        Ok(ProfilingBackendStopResult {
            actual_duration_ms: Some(250),
            status: ProfilingWindowStatus::Completed,
            artifact: Some(ProfileArtifactRecord {
                kind: "pprof".to_string(),
                path: "profiles/window.pb.gz".to_string(),
                bytes: 2_048,
            }),
            error: None,
        })
    }
}

struct UnsupportedProfilingBackend(String);

impl RuntimeProfilingBackend for UnsupportedProfilingBackend {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode> {
        vec![
            ProfilingCaptureMode::WallBlocking,
            ProfilingCaptureMode::Cpu,
        ]
    }

    fn start_window(&self, _request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        ProfilingBackendStartResult::Unsupported {
            error: self.0.clone(),
        }
    }
}

struct AdjustingProfilingBackend;

impl RuntimeProfilingBackend for AdjustingProfilingBackend {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode> {
        vec![ProfilingCaptureMode::Cpu]
    }

    fn start_window(&self, _request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        ProfilingBackendStartResult::Active(ActiveRuntimeProfilingWindow {
            sample_rate_hz: Some(77),
            stopper: Box::new(SuccessfulProfilingStopper),
        })
    }
}

struct BlockingProfilingBackend {
    entered_tx: std::sync::mpsc::Sender<()>,
    release_rx: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl RuntimeProfilingBackend for BlockingProfilingBackend {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode> {
        vec![ProfilingCaptureMode::Cpu]
    }

    fn start_window(&self, _request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        self.entered_tx
            .send(())
            .expect("notify backend start entered");
        self.release_rx
            .lock()
            .expect("lock release receiver")
            .take()
            .expect("single release receiver")
            .recv()
            .expect("release backend start");
        ProfilingBackendStartResult::Active(ActiveRuntimeProfilingWindow {
            sample_rate_hz: Some(100),
            stopper: Box::new(SuccessfulProfilingStopper),
        })
    }
}

fn static_source() -> StaticDiagnosticsSource {
    StaticDiagnosticsSource {
        session: DiagnosticsSessionInfo {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            session_id: "session-123".to_string(),
            volume: "demo".to_string(),
            volume_uuid: "uuid".to_string(),
            bridge: "WinFSP".to_string(),
            mount_point: "Q:".to_string(),
            pid: 42,
            cache_path: "C:/cache".to_string(),
            read_only: false,
            started_unix_ms: 123,
            controller: None,
        },
        health: DiagnosticsHealthSnapshot::default(),
        recent_events: Vec::new(),
        fingerprint: EnvironmentFingerprint {
            platform: PlatformFingerprint {
                os: "windows".to_string(),
                family: "windows".to_string(),
                arch: "x86_64".to_string(),
            },
            bridge: "WinFSP".to_string(),
            build: BuildIdentity {
                package_name: "studiofs".to_string(),
                package_version: "0.1.1".to_string(),
                executable_path: Some("C:/studiofs.exe".to_string()),
                debug_build: true,
            },
            runtime_configuration: RuntimeConfigurationFingerprint {
                volume: "demo".to_string(),
                volume_uuid: "uuid".to_string(),
                mount_point: "Q:".to_string(),
                cache_path: "C:/cache".to_string(),
                read_only: false,
                cache_size: Some("64GB".to_string()),
                endpoint_override: Some("http://127.0.0.1:9000".to_string()),
                snapshot_id: None,
                vfs_config: serde_json::json!({"principal":"*"}),
            },
            workload_identity: WorkloadIdentityFingerprint {
                case_workload: None,
                run_workload: None,
            },
            machine: MachineFingerprint {
                host_name: Some("workstation".to_string()),
                system_name: Some("Windows".to_string()),
                kernel_version: Some("10.0".to_string()),
                os_version: Some("11".to_string()),
                total_memory_bytes: 64 * 1024 * 1024 * 1024,
                cpu_count: 24,
            },
            diagnostics: DiagnosticsSettingsFingerprint {
                protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
                default_lease_ttl_ms: DEFAULT_LEASE_TTL_MS,
                max_lease_ttl_ms: MAX_LEASE_TTL_MS,
                max_connections: MAX_DIAGNOSTICS_CONNECTIONS,
            },
        },
        reset_calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    }
}

fn set_test_workflow_attach_snapshot_output(value: Option<&str>) {
    *TEST_WORKFLOW_ATTACH_SNAPSHOT_OUTPUT
        .get_or_init(|| Mutex::new(None))
        .lock() = value.map(str::to_string);
}

struct ResetWorkflowAttachSnapshotOutput;

impl Drop for ResetWorkflowAttachSnapshotOutput {
    fn drop(&mut self) {
        set_test_workflow_attach_snapshot_output(None);
    }
}

fn sample_scoped_recent_event(
    sequence: u64,
    generated_unix_ms: u64,
    subsystem: &str,
    kind: &str,
    fields: serde_json::Value,
) -> DiagnosticsRecentEvent {
    DiagnosticsRecentEvent {
        sequence,
        generated_unix_ms,
        subsystem: subsystem.to_string(),
        kind: kind.to_string(),
        title: format!("{subsystem}:{kind}"),
        fields,
    }
}

fn test_case_and_run(store: &InvestigationStore) -> (InvestigationCase, InvestigationRun) {
    let case = store
        .create_case(NewInvestigationCase {
            title: "AJA slowdown".to_string(),
            problem_statement: Some("Copy stalls after warmup".to_string()),
            workload_identity: Some("aja".to_string()),
            target_environment: Some("artist workstation".to_string()),
        })
        .expect("create case");
    let run = store
        .create_run(NewInvestigationRun {
            case_id: case.id.clone(),
            session_id: "session-123".to_string(),
            fingerprint: static_source().fingerprint,
        })
        .expect("create run");
    (case, run)
}

fn test_session_service<'a>(
    source: &'a dyn DiagnosticsDataSource,
    lease_manager: &'a ControllerLeaseManager,
    store: Option<&'a InvestigationStore>,
    profiling: Option<&'a DiagnosticsProfilingCoordinator>,
) -> DiagnosticsSessionService<'a> {
    DiagnosticsSessionService::new(DiagnosticsSessionDeps {
        source,
        leases: lease_manager,
        store,
        profiling,
    })
}

fn write_wall_blocking_artifact_fixture(
    dir: &TempDir,
    file_name: &str,
    start_snapshot: DiagnosticsRuntimeSnapshot,
    end_snapshot: DiagnosticsRuntimeSnapshot,
    start_events: Vec<DiagnosticsRecentEvent>,
    end_events: Vec<DiagnosticsRecentEvent>,
) -> ProfileArtifactRecord {
    let path = dir.path().join(file_name);
    let payload = serde_json::json!({
        "schema_version": 1,
        "capture_mode": "wall_blocking",
        "started_unix_ms": 2_000,
        "finished_unix_ms": 4_000,
        "requested_duration_ms": 2_000,
        "actual_duration_ms": 1_750,
        "sampling_hint_hz": 50,
        "start_snapshot": start_snapshot,
        "end_snapshot": end_snapshot,
        "start_events": start_events,
        "end_events": end_events,
    });
    fs::write(
        &path,
        serde_json::to_vec_pretty(&payload).expect("serialize artifact"),
    )
    .expect("write artifact");
    ProfileArtifactRecord {
        kind: "wall_blocking_summary".to_string(),
        path: path.to_string_lossy().into_owned(),
        bytes: fs::metadata(&path).expect("artifact metadata").len(),
    }
}

fn mounts_dir_temp() -> TempDir {
    studiofs_test_support::workspace_target_tempdir("test-")
}

fn sample_discovery(protocol_version: u32) -> LocalDiagnosticsDiscovery {
    LocalDiagnosticsDiscovery {
        protocol_version,
        session_id: "session-123".to_string(),
        volume: "demo".to_string(),
        volume_uuid: "uuid".to_string(),
        bridge: "WinFSP".to_string(),
        mount_point: "Q:".to_string(),
        pid: 42,
        cache_path: "C:/cache".to_string(),
        read_only: false,
        started_unix_ms: 123,
        endpoint: {
            #[cfg(unix)]
            {
                LocalDiagnosticsEndpoint::UnixSocket {
                    path: "/tmp/studiofs-test.sock".to_string(),
                }
            }
            #[cfg(windows)]
            {
                LocalDiagnosticsEndpoint::WindowsNamedPipe {
                    name: r"\\.\pipe\studiofs-test".to_string(),
                }
            }
        },
        auth_token: "secret".to_string(),
    }
}

fn sample_recent_event(title: &str, sequence: u64) -> DiagnosticsRecentEvent {
    DiagnosticsRecentEvent {
        sequence,
        generated_unix_ms: 1_000 + sequence,
        subsystem: "upload_pipeline".to_string(),
        kind: "sample".to_string(),
        title: title.to_string(),
        fields: serde_json::json!({ "pending_uploads": sequence }),
    }
}

fn write_mount_record(
    dir: &Path,
    mount_point: &str,
    discovery: Option<LocalDiagnosticsDiscovery>,
    pid: u32,
) {
    let path = dir.join(format!("{}.json", utils::sanitize_mount_point(mount_point)));
    let record = MountRecord {
        record_version: 2,
        pid,
        mount_point: mount_point.to_string(),
        volume: "demo".to_string(),
        cache_path: Some("cache".to_string()),
        instance_id: Some(format!("instance-{pid}")),
        diagnostics: discovery,
        extra: BTreeMap::new(),
    };
    fs::write(
        path,
        serde_json::to_vec_pretty(&record).expect("serialize record"),
    )
    .expect("write record");
}

#[test]
fn discover_sessions_filters_to_live_diagnostics_records() {
    let dir = mounts_dir_temp();
    let discovery = LocalDiagnosticsDiscovery {
        protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
        session_id: "s1".to_string(),
        volume: "demo".to_string(),
        volume_uuid: "uuid".to_string(),
        bridge: "WinFSP".to_string(),
        mount_point: "X:".to_string(),
        pid: 123,
        cache_path: "cache".to_string(),
        read_only: false,
        started_unix_ms: 1,
        endpoint: endpoint_for(dir.path(), "x", 123),
        auth_token: "secret".to_string(),
    };
    write_mount_record(dir.path(), "X:", Some(discovery.clone()), 123);
    write_mount_record(dir.path(), "Y:", None, 456);

    let sessions =
        discover_sessions_in(dir.path(), |mount_point| Ok(mount_point == "X:")).expect("discover");

    assert_eq!(sessions, vec![discovery]);
}

#[test]
fn discover_sessions_skips_malformed_records() {
    let dir = mounts_dir_temp();
    fs::write(dir.path().join("broken.json"), b"{not-json").expect("write malformed record");
    write_mount_record(dir.path(), "X:", None, 123);

    let sessions = discover_sessions_in(dir.path(), |_| Ok(true)).expect("discover");

    assert!(sessions.is_empty());
}

#[cfg(unix)]
#[test]
fn endpoint_for_shortens_long_unix_socket_paths() {
    let dir = mounts_dir_temp();
    let base_name = "a".repeat(256);
    let LocalDiagnosticsEndpoint::UnixSocket { path } = endpoint_for(dir.path(), &base_name, 42);

    assert!(
        path.len()
            < dir
                .path()
                .join(format!("{base_name}.42.sock"))
                .to_string_lossy()
                .len()
    );
    assert!(Path::new(&path).as_os_str().as_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES);
}

#[test]
fn mutating_requests_require_current_controller_lease() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let service = test_session_service(&source, &lease_manager, Some(&store), None);

    let response = service.create_case(
        "lease-1",
        "AJA slowdown".to_string(),
        None,
        Some("aja".to_string()),
        None,
    );

    assert!(matches!(
        response,
        Err(DiagnosticsServiceError::LeaseInvalid(_))
    ));
}

#[test]
fn mutating_safe_action_requests_require_current_controller_lease() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let service = test_session_service(&source, &lease_manager, Some(&store), None);

    let response = service.invoke_safe_action(
        "lease-1",
        "run-1",
        SafeDebugActionRequest::ResetObservabilityCounters {
            actor: Some("studiofs-dev-mcp".to_string()),
            reason: Some("baseline".to_string()),
        },
    );

    assert!(matches!(
        response,
        Err(DiagnosticsServiceError::LeaseInvalid(_))
    ));
}

#[test]
fn runtime_snapshot_request_returns_structured_sections() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let service = test_session_service(&source, &lease_manager, None, None);
    let snapshot = service.runtime_snapshot().expect("runtime snapshot");

    assert_eq!(snapshot.schema_version, 2);
    assert_eq!(
        snapshot.upload.pending_uploads,
        source.health.upload.pending_uploads
    );
    assert_eq!(
        snapshot.synchronization.state.file_sessions.open_handles,
        source.health.file_sessions.open_handles
    );
}

#[test]
fn snapshot_sections_for_scope_includes_zeroio_section() {
    let snapshot = DiagnosticsRuntimeSnapshot {
        zeroio: VfsZeroIoRuntimeSnapshot {
            unpublished_files: 3,
            ..VfsZeroIoRuntimeSnapshot::default()
        },
        ..DiagnosticsRuntimeSnapshot::default()
    };
    let scope = DiagnosticsEvidenceScope {
        subsystem: Some("zeroio".to_string()),
        ..DiagnosticsEvidenceScope::default()
    };

    let sections = snapshot_sections_for_scope(&snapshot, &scope, None);

    assert_eq!(sections.len(), 1);
    assert_eq!(
        sections
            .get("zeroio")
            .and_then(|value| value.get("unpublished_files"))
            .and_then(|value| value.as_u64()),
        Some(3)
    );
}

#[test]
fn evidence_view_filters_by_scope_object_identity_and_marker_span() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let (_case, run) = test_case_and_run(&store);
    let start = store
        .append_timeline_entry(
            &run.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "warmup start".to_string(),
                body: None,
                actor: Some("developer".to_string()),
                reason: Some("begin".to_string()),
            },
        )
        .expect("append start marker");
    let end = store
        .append_timeline_entry(
            &run.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "warmup end".to_string(),
                body: None,
                actor: Some("developer".to_string()),
                reason: Some("end".to_string()),
            },
        )
        .expect("append end marker");
    let source = StaticDiagnosticsSource {
        recent_events: vec![
            sample_scoped_recent_event(
                1,
                start.occurred_unix_ms.saturating_sub(10),
                SUBSYSTEM_UPLOAD_PIPELINE,
                "sample",
                serde_json::json!({"pending_uploads": 1, "path": "Q:/clips/reel.mov"}),
            ),
            sample_scoped_recent_event(
                2,
                start.occurred_unix_ms.saturating_add(1),
                SUBSYSTEM_UPLOAD_PIPELINE,
                "sample",
                serde_json::json!({"pending_uploads": 2, "path": "Q:/clips/reel.mov"}),
            ),
            sample_scoped_recent_event(
                3,
                start.occurred_unix_ms.saturating_add(2),
                SUBSYSTEM_CACHE,
                "sample",
                serde_json::json!({"pending_uploads": 99, "path": "Q:/clips/reel.mov"}),
            ),
            sample_scoped_recent_event(
                4,
                end.occurred_unix_ms.saturating_sub(1),
                SUBSYSTEM_UPLOAD_PIPELINE,
                "sample",
                serde_json::json!({"pending_uploads": 5, "path": "Q:/clips/reel.mov"}),
            ),
            sample_scoped_recent_event(
                5,
                end.occurred_unix_ms.saturating_add(1),
                SUBSYSTEM_UPLOAD_PIPELINE,
                "sample",
                serde_json::json!({"pending_uploads": 7, "path": "Q:/clips/reel.mov"}),
            ),
        ],
        ..static_source()
    };
    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");
    let service = test_session_service(&source, &lease_manager, Some(&store), None);
    let view = service
        .evidence_view(
            &lease.lease_id,
            &run.id,
            DiagnosticsEvidenceRequest {
                scope: DiagnosticsEvidenceScope {
                    subsystem: Some(SUBSYSTEM_UPLOAD_PIPELINE.to_string()),
                    operation_type: None,
                    object_identity: Some(DiagnosticsObjectIdentity {
                        path: Some("Q:/clips/reel.mov".to_string()),
                        ..DiagnosticsObjectIdentity::default()
                    }),
                    marker_span: Some(DiagnosticsMarkerSpan {
                        start_sequence: start.sequence,
                        end_sequence: Some(end.sequence),
                    }),
                },
                budget: DiagnosticsEvidenceBudget {
                    max_recent_events: 8,
                    max_timeline_entries: 8,
                    max_profiling_windows: 4,
                    max_derived_items_per_kind: 4,
                },
            },
        )
        .expect("evidence view");

    assert_eq!(
        view.raw
            .recent_events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![2, 4]
    );
    let resolved_span = view.scope.marker_span.expect("resolved marker span");
    assert_eq!(resolved_span.start_sequence, start.sequence);
    assert_eq!(resolved_span.end_sequence, end.sequence);
    assert_eq!(view.derived.queue_growth.len(), 1);
    assert_eq!(
        view.derived.queue_growth[0].operation_type,
        OPERATION_QUEUE_GROWTH
    );
    assert_eq!(view.derived.queue_growth[0].delta_pending_uploads, 3);
    assert_eq!(
        view.derived.queue_growth[0]
            .supporting_evidence
            .iter()
            .map(|evidence| evidence.id.as_str())
            .collect::<Vec<_>>(),
        vec!["2", "4"]
    );
}

#[test]
fn evidence_view_clamps_requested_budget_and_reports_truncation() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let (_case, run) = test_case_and_run(&store);
    let source = StaticDiagnosticsSource {
        recent_events: (1..=80)
            .map(|sequence| {
                sample_scoped_recent_event(
                    sequence,
                    10_000 + sequence,
                    SUBSYSTEM_UPLOAD_PIPELINE,
                    "sample",
                    serde_json::json!({"pending_uploads": sequence}),
                )
            })
            .collect(),
        ..static_source()
    };
    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");
    let service = test_session_service(&source, &lease_manager, Some(&store), None);
    let view = service
        .evidence_view(
            &lease.lease_id,
            &run.id,
            DiagnosticsEvidenceRequest {
                scope: DiagnosticsEvidenceScope::default(),
                budget: DiagnosticsEvidenceBudget {
                    max_recent_events: 10_000,
                    max_timeline_entries: 10_000,
                    max_profiling_windows: 10_000,
                    max_derived_items_per_kind: 10_000,
                },
            },
        )
        .expect("evidence view");

    assert_eq!(
        view.budget.applied.max_recent_events,
        MAX_EVIDENCE_RECENT_EVENTS_LIMIT
    );
    assert!(view.budget.truncation.recent_events);
    assert!(view.budget.truncation.queue_growth);
    assert!(view.budget.truncation.blocking_intervals);
    assert!(view.budget.truncation.hotspots);
    assert_eq!(
        view.raw.recent_events.len(),
        MAX_EVIDENCE_RECENT_EVENTS_LIMIT
    );
}

#[test]
fn evidence_view_derives_blocking_intervals_and_hotspots_from_profile_artifacts() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let (_case, run) = test_case_and_run(&store);
    let mut start_health = DiagnosticsHealthSnapshot::default();
    start_health
        .write_path
        .payload_store_sync_data
        .queue_wait_total_ms = 10.0;
    start_health
        .write_path
        .payload_store_sync_data
        .queue_wait_max_ms = 4.0;
    start_health.write_path.write_backpressure.wait_total_ms = 20;
    let mut end_health = DiagnosticsHealthSnapshot::default();
    end_health
        .write_path
        .payload_store_sync_data
        .queue_wait_total_ms = 70.0;
    end_health
        .write_path
        .payload_store_sync_data
        .queue_wait_max_ms = 25.0;
    end_health.write_path.write_backpressure.wait_total_ms = 65;
    end_health.upload.pending_uploads = 9;
    let artifact = write_wall_blocking_artifact_fixture(
        &temp,
        "wall-blocking.json",
        DiagnosticsRuntimeSnapshot::from_health(start_health),
        DiagnosticsRuntimeSnapshot::from_health(end_health),
        vec![sample_scoped_recent_event(
            1,
            2_100,
            SUBSYSTEM_UPLOAD_PIPELINE,
            "sample",
            serde_json::json!({"pending_uploads": 2}),
        )],
        vec![sample_scoped_recent_event(
            2,
            3_900,
            SUBSYSTEM_UPLOAD_PIPELINE,
            "sample",
            serde_json::json!({"pending_uploads": 9}),
        )],
    );
    store
        .append_profiling_window(
            &run.id,
            NewProfilingWindow {
                window_id: "window-1".to_string(),
                occurred_unix_ms: 2_000,
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                requested_duration_ms: 2_000,
                actual_duration_ms: Some(1_750),
                sample_rate_hz: Some(50),
                estimated_cost: ProfilingCostEstimate {
                    score: 2,
                    label: "balanced".to_string(),
                },
                status: ProfilingWindowStatus::Completed,
                artifact: Some(artifact),
                error: None,
            },
        )
        .expect("append profiling window");

    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");
    let source = static_source();
    let service = test_session_service(&source, &lease_manager, Some(&store), None);
    let view = service
        .evidence_view(
            &lease.lease_id,
            &run.id,
            DiagnosticsEvidenceRequest {
                scope: DiagnosticsEvidenceScope {
                    subsystem: Some("write_path".to_string()),
                    ..DiagnosticsEvidenceScope::default()
                },
                budget: DiagnosticsEvidenceBudget {
                    max_recent_events: 8,
                    max_timeline_entries: 8,
                    max_profiling_windows: 4,
                    max_derived_items_per_kind: 4,
                },
            },
        )
        .expect("evidence view");

    assert_eq!(view.raw.profiling_windows.len(), 1);
    assert!(view.raw.profiling_windows[0].summary.is_some());
    assert!(view.raw.profiling_windows[0].load_error.is_none());
    assert!(view.derived.blocking_intervals.iter().any(|interval| {
        interval.operation_type == OPERATION_SYNC_DATA_QUEUE_WAIT
            && interval.total_blocked_ms == 60.0
    }));
    assert!(view.derived.hotspots.iter().any(|hotspot| {
        hotspot.metric == "payload_store_sync_data.queue_wait_total_ms_delta"
            && hotspot.subsystem == SUBSYSTEM_WRITE_PATH
    }));
    assert!(
        view.derived.blocking_intervals[0]
            .supporting_evidence
            .iter()
            .any(|evidence| {
                evidence.source == DiagnosticsEvidenceReferenceSource::ProfilingArtifact
            })
    );
}

#[test]
fn evidence_view_returns_empty_collections_for_empty_slices() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let (_case, run) = test_case_and_run(&store);
    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");
    let source = static_source();
    let service = test_session_service(&source, &lease_manager, Some(&store), None);
    let view = service
        .evidence_view(
            &lease.lease_id,
            &run.id,
            DiagnosticsEvidenceRequest {
                scope: DiagnosticsEvidenceScope {
                    subsystem: Some(SUBSYSTEM_UPLOAD_PIPELINE.to_string()),
                    object_identity: Some(DiagnosticsObjectIdentity {
                        path: Some("Q:/missing/file.mov".to_string()),
                        ..DiagnosticsObjectIdentity::default()
                    }),
                    ..DiagnosticsEvidenceScope::default()
                },
                budget: DiagnosticsEvidenceBudget {
                    max_recent_events: 8,
                    max_timeline_entries: 8,
                    max_profiling_windows: 4,
                    max_derived_items_per_kind: 4,
                },
            },
        )
        .expect("evidence view");

    assert!(view.raw.recent_events.is_empty());
    assert!(view.raw.timeline_entries.is_empty());
    assert!(view.raw.profiling_windows.is_empty());
    assert!(view.derived.queue_growth.is_empty());
    assert!(view.derived.blocking_intervals.is_empty());
    assert!(view.derived.hotspots.is_empty());
    assert!(
        !view.raw.snapshot_sections.is_empty(),
        "empty slices should still return the scoped snapshot surface"
    );
}

#[test]
fn evidence_view_surfaces_profile_artifact_load_errors() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let (_case, run) = test_case_and_run(&store);
    store
        .append_profiling_window(
            &run.id,
            NewProfilingWindow {
                window_id: "window-bad".to_string(),
                occurred_unix_ms: 2_000,
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                requested_duration_ms: 2_000,
                actual_duration_ms: Some(1_750),
                sample_rate_hz: Some(50),
                estimated_cost: ProfilingCostEstimate {
                    score: 2,
                    label: "balanced".to_string(),
                },
                status: ProfilingWindowStatus::Completed,
                artifact: Some(ProfileArtifactRecord {
                    kind: "wall_blocking_summary".to_string(),
                    path: temp
                        .path()
                        .join("missing-wall-blocking.json")
                        .to_string_lossy()
                        .into_owned(),
                    bytes: 0,
                }),
                error: None,
            },
        )
        .expect("append profiling window");

    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");
    let source = static_source();
    let service = test_session_service(&source, &lease_manager, Some(&store), None);
    let view = service
        .evidence_view(
            &lease.lease_id,
            &run.id,
            DiagnosticsEvidenceRequest {
                scope: DiagnosticsEvidenceScope::default(),
                budget: DiagnosticsEvidenceBudget {
                    max_recent_events: 8,
                    max_timeline_entries: 8,
                    max_profiling_windows: 4,
                    max_derived_items_per_kind: 4,
                },
            },
        )
        .expect("evidence view");

    assert_eq!(view.raw.profiling_windows.len(), 1);
    assert!(view.raw.profiling_windows[0].summary.is_none());
    assert!(view.raw.profiling_windows[0].load_error.is_some());
}

#[test]
fn materialize_report_persists_reloadable_run_artifact_with_findings() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let (_case, run) = test_case_and_run(&store);
    store
        .append_timeline_entry(
            &run.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "start copy".to_string(),
                body: Some("Begin workload".to_string()),
                actor: Some("developer".to_string()),
                reason: Some("baseline".to_string()),
            },
        )
        .expect("append marker");
    store
        .append_timeline_entry(
            &run.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Mcp,
                kind: TimelineEntryKind::Anomaly,
                title: "queue growth".to_string(),
                body: Some("Pending uploads spiked during the copy.".to_string()),
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("operator observation".to_string()),
            },
        )
        .expect("append note");

    let mut health = DiagnosticsHealthSnapshot::default();
    health.upload.pending_uploads = 9;
    health
        .write_path
        .payload_store_sync_data
        .queue_wait_total_ms = 75.0;
    health.write_path.payload_store_sync_data.queue_wait_max_ms = 30.0;
    let source = StaticDiagnosticsSource {
        health,
        recent_events: vec![
            sample_scoped_recent_event(
                1,
                2_000,
                SUBSYSTEM_UPLOAD_PIPELINE,
                "sample",
                serde_json::json!({"pending_uploads": 2, "path": "Q:/clips/reel.mov"}),
            ),
            sample_scoped_recent_event(
                2,
                2_500,
                SUBSYSTEM_UPLOAD_PIPELINE,
                "sample",
                serde_json::json!({"pending_uploads": 9, "path": "Q:/clips/reel.mov"}),
            ),
        ],
        ..static_source()
    };
    let artifact = write_wall_blocking_artifact_fixture(
        &temp,
        "report-window.json",
        DiagnosticsRuntimeSnapshot::default(),
        source.runtime_snapshot(),
        vec![],
        vec![],
    );
    store
        .append_profiling_window(
            &run.id,
            NewProfilingWindow {
                window_id: "window-1".to_string(),
                occurred_unix_ms: 2_400,
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                requested_duration_ms: 1_000,
                actual_duration_ms: Some(950),
                sample_rate_hz: Some(99),
                estimated_cost: ProfilingCostEstimate {
                    score: 2,
                    label: "balanced".to_string(),
                },
                status: ProfilingWindowStatus::Completed,
                artifact: Some(artifact),
                error: None,
            },
        )
        .expect("append profiling window");

    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");
    let service = test_session_service(&source, &lease_manager, Some(&store), None);
    let report = service
        .materialize_report(&lease.lease_id, &run.id)
        .expect("materialize report");

    assert_eq!(report.run_id, run.id);
    assert_eq!(report.marker_timeline.len(), 1);
    assert!(!report.anomalies.is_empty());
    assert!(!report.candidate_bottlenecks.is_empty());
    assert!(!report.recommended_next_actions.is_empty());
    assert_eq!(report.references.derived_view.run_id, run.id);
    assert_eq!(report.references.profiling_windows.len(), 1);

    let persisted = store
        .load_report(&report.id)
        .expect("load persisted report");
    assert_eq!(persisted.id, report.id);
    assert_eq!(persisted.references.profiling_windows.len(), 1);
    assert_eq!(persisted.truncation.anomalies.truncated_count, 0);
    assert_eq!(
        persisted.truncation.candidate_bottlenecks.truncated_count,
        0
    );
}

#[test]
fn materialize_report_records_truncation_metadata_for_condensed_sections() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let (_case, run) = test_case_and_run(&store);

    for index in 0..10 {
        store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Mcp,
                    kind: TimelineEntryKind::Anomaly,
                    title: format!("anomaly {index}"),
                    body: Some(format!("Anomaly body {index}")),
                    actor: Some("studiofs-dev-mcp".to_string()),
                    reason: Some("operator observation".to_string()),
                },
            )
            .expect("append anomaly note");
    }

    let mut health = DiagnosticsHealthSnapshot::default();
    health.upload.pending_uploads = 12;
    health
        .write_path
        .payload_store_sync_data
        .queue_wait_total_ms = 100.0;
    health.write_path.payload_store_sync_data.queue_wait_max_ms = 50.0;
    health.write_path.write_backpressure.wait_total_ms = 80;

    let source = StaticDiagnosticsSource {
        health,
        recent_events: (1..=8)
            .map(|sequence| {
                sample_scoped_recent_event(
                    sequence,
                    3_000 + sequence,
                    SUBSYSTEM_UPLOAD_PIPELINE,
                    "sample",
                    serde_json::json!({
                        "pending_uploads": sequence + 1,
                        "path": format!("Q:/clips/reel-{sequence}.mov")
                    }),
                )
            })
            .collect(),
        ..static_source()
    };

    for index in 0..6 {
        let artifact = write_wall_blocking_artifact_fixture(
            &temp,
            &format!("report-window-{index}.json"),
            DiagnosticsRuntimeSnapshot::default(),
            source.runtime_snapshot(),
            vec![],
            vec![],
        );
        store
            .append_profiling_window(
                &run.id,
                NewProfilingWindow {
                    window_id: format!("window-{index}"),
                    occurred_unix_ms: 4_000 + index,
                    capture_mode: ProfilingCaptureMode::WallBlocking,
                    preset: DiagnosticsPreset::Balanced,
                    requested_duration_ms: 1_000,
                    actual_duration_ms: Some(950),
                    sample_rate_hz: Some(99),
                    estimated_cost: ProfilingCostEstimate {
                        score: 2,
                        label: "balanced".to_string(),
                    },
                    status: ProfilingWindowStatus::Completed,
                    artifact: Some(artifact),
                    error: None,
                },
            )
            .expect("append profiling window");
    }

    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");
    let service = test_session_service(&source, &lease_manager, Some(&store), None);
    let report = service
        .materialize_report(&lease.lease_id, &run.id)
        .expect("materialize report");

    assert_eq!(
        report.truncation.anomalies.included_count,
        report.anomalies.len()
    );
    assert!(report.truncation.anomalies.truncated_count > 0);
    assert!(report.truncation.candidate_bottlenecks.truncated_count > 0);
    assert!(report.truncation.selected_evidence.truncated_count > 0);
}

#[test]
fn build_report_next_action_candidates_uses_largest_blocking_interval() {
    let run = InvestigationRun {
        id: "run-1".to_string(),
        case_id: "case-1".to_string(),
        session_id: "session-1".to_string(),
        created_unix_ms: 1,
        fingerprint: static_source().fingerprint,
        profiling_windows: Vec::new(),
        report_ids: Vec::new(),
        workflow_run_ids: Vec::new(),
        marker_snapshots: Vec::new(),
        timeline: Vec::new(),
    };
    let derived_view = DiagnosticsEvidenceView {
        generated_unix_ms: 1,
        run_id: run.id.clone(),
        scope: DiagnosticsEvidenceScopeResolution::default(),
        budget: DiagnosticsEvidenceBudgetReport {
            requested: DiagnosticsEvidenceBudget::default(),
            applied: DiagnosticsEvidenceBudget::default(),
            truncation: DiagnosticsEvidenceTruncation::default(),
        },
        raw: DiagnosticsEvidenceRawView::default(),
        derived: DiagnosticsDerivedEvidenceView {
            queue_growth: Vec::new(),
            blocking_intervals: vec![
                DiagnosticsBlockingInterval {
                    subsystem: SUBSYSTEM_WRITE_PATH.to_string(),
                    operation_type: OPERATION_SYNC_DATA_QUEUE_WAIT.to_string(),
                    total_blocked_ms: 20.0,
                    max_blocked_ms: 10.0,
                    supporting_evidence: Vec::new(),
                },
                DiagnosticsBlockingInterval {
                    subsystem: SUBSYSTEM_UPLOAD_PIPELINE.to_string(),
                    operation_type: OPERATION_UPLOAD_FALLBACK_WAIT.to_string(),
                    total_blocked_ms: 50.0,
                    max_blocked_ms: 25.0,
                    supporting_evidence: Vec::new(),
                },
            ],
            hotspots: Vec::new(),
        },
    };

    let actions = build_report_next_action_candidates(&run, &[], &derived_view);
    let drill_down = actions
        .iter()
        .find(|action| action.title.contains("drill into"))
        .expect("drill-down action");

    assert!(drill_down.title.contains("upload pipeline"));
    assert!(drill_down.rationale.contains("50.0 ms"));
}

#[test]
fn materialize_report_reuses_single_runtime_snapshot_for_report_and_view() {
    #[derive(Clone)]
    struct CountingSnapshotSource {
        session: DiagnosticsSessionInfo,
        fingerprint: EnvironmentFingerprint,
        counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    impl DiagnosticsDataSource for CountingSnapshotSource {
        fn session_info(&self) -> DiagnosticsSessionInfo {
            self.session.clone()
        }

        fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
            DiagnosticsHealthSnapshot::default()
        }

        fn runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshot {
            let sequence = self
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            DiagnosticsRuntimeSnapshot {
                generated_unix_ms: 10_000 + sequence,
                upload: VfsUploadStats {
                    pending_uploads: sequence as usize,
                    ..VfsUploadStats::default()
                },
                ..DiagnosticsRuntimeSnapshot::default()
            }
        }

        fn recent_events(&self, _limit: usize) -> Vec<DiagnosticsRecentEvent> {
            Vec::new()
        }

        fn environment_fingerprint(
            &self,
            case_workload_identity: Option<&str>,
            run_workload_identity: Option<&str>,
        ) -> EnvironmentFingerprint {
            let mut fingerprint = self.fingerprint.clone();
            fingerprint.workload_identity.case_workload =
                case_workload_identity.map(str::to_string);
            fingerprint.workload_identity.run_workload = run_workload_identity.map(str::to_string);
            fingerprint
        }
    }

    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let (_case, run) = test_case_and_run(&store);
    let static_source = static_source();
    let source = CountingSnapshotSource {
        session: static_source.session,
        fingerprint: static_source.fingerprint,
        counter: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };
    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");
    let service = test_session_service(&source, &lease_manager, Some(&store), None);
    let report = service
        .materialize_report(&lease.lease_id, &run.id)
        .expect("materialize report");

    let upload_pending_in_view =
        report.references.derived_view.raw.snapshot_sections["upload"]["pending_uploads"]
            .as_u64()
            .expect("pending uploads in derived view snapshot");
    assert_eq!(report.references.runtime_snapshot.upload.pending_uploads, 1);
    assert_eq!(upload_pending_in_view, 1);
}

#[test]
fn silent_log_verbosity_action_returns_updated_level_without_timeline_entry() {
    let _guard = lock_diagnostics_test_mutex();
    crate::ensure_test_diagnostics_log_filter();

    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));

    let lease = match execute_request(
        DiagnosticsRequest::AcquireController {
            auth_token: "secret".to_string(),
            client_name: "tester".to_string(),
            ttl_ms: Some(DIAGNOSTICS_TEST_LEASE_TTL_MS),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::ControllerLease { lease } => lease,
        other => panic!("unexpected lease response: {other:?}"),
    };

    let case = match execute_request(
        DiagnosticsRequest::CreateCase {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            title: "AJA slowdown".to_string(),
            problem_statement: Some("Copy stalls after warmup".to_string()),
            workload_identity: Some("aja".to_string()),
            target_environment: Some("artist workstation".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationCase { case } => *case,
        other => panic!("unexpected case response: {other:?}"),
    };

    let run = match execute_request(
        DiagnosticsRequest::CreateRun {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            case_id: case.id,
            run_workload_identity: Some("copy-pass-1".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationRun { run } => *run,
        other => panic!("unexpected run response: {other:?}"),
    };

    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            run_id: run.id.clone(),
            action: SafeDebugActionRequest::SetLogVerbosity {
                level: SafeLogVerbosity::Debug,
            },
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::SafeDebugAction { outcome } = response else {
        panic!("expected safe debug action response");
    };
    assert_eq!(outcome.action, SafeDebugActionKind::SetLogVerbosity);
    assert_eq!(outcome.status, SafeDebugActionStatus::Succeeded);
    assert_eq!(outcome.summary["level"], serde_json::json!("debug"));
    assert!(
        store
            .load_run(&run.id)
            .expect("load run")
            .timeline
            .is_empty(),
        "silent safe actions should not append audit entries"
    );
}

#[test]
fn loud_safe_action_appends_audited_timeline_entry() {
    let _guard = lock_diagnostics_test_mutex();
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));

    let lease = match execute_request(
        DiagnosticsRequest::AcquireController {
            auth_token: "secret".to_string(),
            client_name: "tester".to_string(),
            ttl_ms: Some(DIAGNOSTICS_TEST_LEASE_TTL_MS),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::ControllerLease { lease } => lease,
        other => panic!("unexpected lease response: {other:?}"),
    };

    let case = match execute_request(
        DiagnosticsRequest::CreateCase {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            title: "AJA slowdown".to_string(),
            problem_statement: Some("Copy stalls after warmup".to_string()),
            workload_identity: Some("aja".to_string()),
            target_environment: Some("artist workstation".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationCase { case } => *case,
        other => panic!("unexpected case response: {other:?}"),
    };

    let run = match execute_request(
        DiagnosticsRequest::CreateRun {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            case_id: case.id,
            run_workload_identity: Some("copy-pass-1".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationRun { run } => *run,
        other => panic!("unexpected run response: {other:?}"),
    };

    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            run_id: run.id.clone(),
            action: SafeDebugActionRequest::LightweightSelfCheck {
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("sanity check after attach".to_string()),
            },
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::SafeDebugAction { outcome } = response else {
        panic!("expected safe debug action response");
    };
    assert!(outcome.loud);
    assert_eq!(outcome.status, SafeDebugActionStatus::Succeeded);
    let persisted = store.load_run(&run.id).expect("load run");
    assert_eq!(persisted.timeline.len(), 1);
    assert_eq!(persisted.timeline[0].kind, TimelineEntryKind::Action);
    assert_eq!(
        persisted.timeline[0].actor.as_deref(),
        Some("studiofs-dev-mcp")
    );
    assert_eq!(
        persisted.timeline[0].reason.as_deref(),
        Some("sanity check after attach")
    );
}

#[test]
fn loud_safe_action_failures_remain_audited_and_fail_open() {
    let _guard = lock_diagnostics_test_mutex();
    let temp = mounts_dir_temp();
    let cache_file = temp.path().join("cache-file");
    fs::write(&cache_file, b"not-a-directory").expect("write cache file");
    let source = StaticDiagnosticsSource {
        session: DiagnosticsSessionInfo {
            cache_path: cache_file.to_string_lossy().into_owned(),
            ..static_source().session
        },
        ..static_source()
    };
    let lease_manager = ControllerLeaseManager::default();
    let store = InvestigationStore::new(temp.path().join("investigations"));

    let lease = match execute_request(
        DiagnosticsRequest::AcquireController {
            auth_token: "secret".to_string(),
            client_name: "tester".to_string(),
            ttl_ms: Some(DIAGNOSTICS_TEST_LEASE_TTL_MS),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::ControllerLease { lease } => lease,
        other => panic!("unexpected lease response: {other:?}"),
    };

    let case = match execute_request(
        DiagnosticsRequest::CreateCase {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            title: "AJA slowdown".to_string(),
            problem_statement: Some("Copy stalls after warmup".to_string()),
            workload_identity: Some("aja".to_string()),
            target_environment: Some("artist workstation".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationCase { case } => *case,
        other => panic!("unexpected case response: {other:?}"),
    };

    let run = match execute_request(
        DiagnosticsRequest::CreateRun {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            case_id: case.id,
            run_workload_identity: Some("copy-pass-1".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationRun { run } => *run,
        other => panic!("unexpected run response: {other:?}"),
    };

    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            run_id: run.id.clone(),
            action: SafeDebugActionRequest::CaptureBundle {
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("capture evidence".to_string()),
                recent_events_limit: Some(8),
            },
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::SafeDebugAction { outcome } = response else {
        panic!("expected safe debug action response");
    };
    assert_eq!(outcome.status, SafeDebugActionStatus::Failed);
    assert!(outcome.error.is_some());
    let persisted = store.load_run(&run.id).expect("load run");
    assert_eq!(persisted.timeline.len(), 1);
    assert_eq!(persisted.timeline[0].kind, TimelineEntryKind::Action);
    assert_eq!(
        persisted.timeline[0].reason.as_deref(),
        Some("capture evidence")
    );
}

#[test]
fn reset_counters_action_calls_data_source_and_is_audited() {
    let _guard = lock_diagnostics_test_mutex();
    let source = static_source();
    let reset_calls = source.reset_calls.clone();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));

    let lease = match execute_request(
        DiagnosticsRequest::AcquireController {
            auth_token: "secret".to_string(),
            client_name: "tester".to_string(),
            ttl_ms: Some(DIAGNOSTICS_TEST_LEASE_TTL_MS),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::ControllerLease { lease } => lease,
        other => panic!("unexpected lease response: {other:?}"),
    };

    let case = match execute_request(
        DiagnosticsRequest::CreateCase {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            title: "AJA slowdown".to_string(),
            problem_statement: Some("Copy stalls after warmup".to_string()),
            workload_identity: Some("aja".to_string()),
            target_environment: Some("artist workstation".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationCase { case } => *case,
        other => panic!("unexpected case response: {other:?}"),
    };

    let run = match execute_request(
        DiagnosticsRequest::CreateRun {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            case_id: case.id,
            run_workload_identity: Some("copy-pass-1".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationRun { run } => *run,
        other => panic!("unexpected run response: {other:?}"),
    };

    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            run_id: run.id.clone(),
            action: SafeDebugActionRequest::ResetObservabilityCounters {
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("baseline before rerun".to_string()),
            },
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::SafeDebugAction { outcome } = response else {
        panic!("expected safe debug action response");
    };
    assert_eq!(outcome.status, SafeDebugActionStatus::Succeeded);
    assert_eq!(
        reset_calls.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "reset action should invoke the data source exactly once"
    );
    assert_eq!(store.load_run(&run.id).expect("load run").timeline.len(), 1);
}

#[test]
fn loud_safe_action_audit_failures_are_reported_in_outcome() {
    let _guard = lock_diagnostics_test_mutex();
    struct ResetAuditFailure;
    impl Drop for ResetAuditFailure {
        fn drop(&mut self) {
            set_test_safe_action_audit_failure(None);
        }
    }

    let _reset = ResetAuditFailure;
    set_test_safe_action_audit_failure(Some("synthetic audit append failure"));

    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));

    let lease = match execute_request(
        DiagnosticsRequest::AcquireController {
            auth_token: "secret".to_string(),
            client_name: "tester".to_string(),
            ttl_ms: Some(DIAGNOSTICS_TEST_LEASE_TTL_MS),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::ControllerLease { lease } => lease,
        other => panic!("unexpected lease response: {other:?}"),
    };

    let case = match execute_request(
        DiagnosticsRequest::CreateCase {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            title: "AJA slowdown".to_string(),
            problem_statement: Some("Copy stalls after warmup".to_string()),
            workload_identity: Some("aja".to_string()),
            target_environment: Some("artist workstation".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationCase { case } => *case,
        other => panic!("unexpected case response: {other:?}"),
    };

    let run = match execute_request(
        DiagnosticsRequest::CreateRun {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            case_id: case.id,
            run_workload_identity: Some("copy-pass-1".to_string()),
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    ) {
        DiagnosticsResponse::InvestigationRun { run } => *run,
        other => panic!("unexpected run response: {other:?}"),
    };

    let response = execute_request(
        DiagnosticsRequest::InvokeSafeAction {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            run_id: run.id.clone(),
            action: SafeDebugActionRequest::LightweightSelfCheck {
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("audit failure coverage".to_string()),
            },
        },
        "secret",
        &source,
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::SafeDebugAction { outcome } = response else {
        panic!("expected safe debug action response");
    };
    assert_eq!(outcome.status, SafeDebugActionStatus::Succeeded);
    assert_eq!(
        outcome.audit_error.as_deref(),
        Some("synthetic audit append failure")
    );
    assert!(
        store
            .load_run(&run.id)
            .expect("load run")
            .timeline
            .is_empty(),
        "failed audit append should not persist timeline entries"
    );
}

#[test]
fn invoke_safe_action_rejects_older_protocol_versions_before_connect() {
    let client = LocalDiagnosticsClient::new(sample_discovery(
        DIAGNOSTICS_SAFE_ACTIONS_PROTOCOL_VERSION - 1,
    ));

    let error = crate::runtime_bridge::block_on(async move {
        client
            .invoke_safe_action(
                "lease-1",
                "run-1",
                SafeDebugActionRequest::LightweightSelfCheck {
                    actor: Some("studiofs-dev-mcp".to_string()),
                    reason: Some("gate coverage".to_string()),
                },
            )
            .await
    })
    .expect_err("older protocol versions should be gated");

    assert!(
        error
            .to_string()
            .contains("safe debug actions require diagnostics protocol version"),
        "unexpected protocol gate error: {error:#}"
    );
}

#[test]
fn recent_events_request_is_bounded_and_keeps_newest_entries() {
    let source = StaticDiagnosticsSource {
        recent_events: vec![
            sample_recent_event("older", 1),
            sample_recent_event("middle", 2),
            sample_recent_event("newest", 3),
        ],
        ..static_source()
    };

    let response = execute_request(
        DiagnosticsRequest::RecentEvents {
            auth_token: "secret".to_string(),
            limit: Some(2),
        },
        "secret",
        &source,
        &ControllerLeaseManager::default(),
        None,
        None,
    );

    let DiagnosticsResponse::RecentEvents { events } = response else {
        panic!("expected recent events response");
    };

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].title, "middle");
    assert_eq!(events[1].title, "newest");
}

#[test]
fn list_workflows_includes_aja_hybrid_preset() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let service = test_session_service(&source, &lease_manager, None, None);
    let workflows = service.list_workflows().expect("list workflows");
    assert_eq!(workflows.len(), 1);
    assert_eq!(workflows[0].workflow_id, "aja_hybrid_v1");
    assert_eq!(workflows[0].steps[0].step_id, "capture_attach_snapshot");
}

#[test]
fn mutating_requests_persist_run_fingerprint_and_timeline() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let service = test_session_service(&source, &lease_manager, Some(&store), None);

    let lease = service
        .acquire_controller("tester".to_string(), Some(DIAGNOSTICS_TEST_LEASE_TTL_MS))
        .expect("acquire controller");
    let case = service
        .create_case(
            &lease.lease_id,
            "AJA slowdown".to_string(),
            Some("Copy stalls after warmup".to_string()),
            Some("aja".to_string()),
            Some("artist workstation".to_string()),
        )
        .expect("create case");
    let run = service
        .create_run(
            &lease.lease_id,
            case.id.clone(),
            Some("copy-pass-1".to_string()),
        )
        .expect("create run");

    service
        .append_timeline_entry(
            &lease.lease_id,
            &run.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "start copy".to_string(),
                body: Some("Started the manual copy workload".to_string()),
                actor: Some("developer".to_string()),
                reason: Some("begin workload".to_string()),
            },
        )
        .expect("append marker timeline entry");
    service
        .append_timeline_entry(
            &lease.lease_id,
            &run.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Mcp,
                kind: TimelineEntryKind::Anomaly,
                title: "cache miss burst".to_string(),
                body: Some("MCP adapter observed a foreground read spike".to_string()),
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("operator observation".to_string()),
            },
        )
        .expect("append anomaly timeline entry");

    let persisted_run = store.load_run(&run.id).expect("load run");

    assert_eq!(persisted_run.case_id, case.id);
    assert_eq!(persisted_run.session_id, "session-123");
    assert_eq!(persisted_run.fingerprint.bridge, "WinFSP");
    assert_eq!(
        persisted_run
            .fingerprint
            .workload_identity
            .case_workload
            .as_deref(),
        Some("aja")
    );
    assert_eq!(
        persisted_run
            .fingerprint
            .workload_identity
            .run_workload
            .as_deref(),
        Some("copy-pass-1")
    );
    assert_eq!(persisted_run.timeline.len(), 2);
    assert_eq!(persisted_run.timeline[0].source, TimelineEntrySource::Cli);
    assert_eq!(persisted_run.timeline[1].source, TimelineEntrySource::Mcp);
}

#[test]
fn workflow_requests_execute_hybrid_preset_and_persist_progress() {
    let _guard = lock_diagnostics_test_mutex();
    let _reset_workflow_attach_snapshot_output = ResetWorkflowAttachSnapshotOutput;
    set_test_workflow_attach_snapshot_output(Some(
        "{\"session\":{\"mount_point\":\"Q:\"},\"health\":{},\"runtime_snapshot\":null}",
    ));

    let mut source = static_source();
    let temp = mounts_dir_temp();
    source.session.cache_path = temp.path().to_string_lossy().into_owned();
    source.fingerprint.runtime_configuration.cache_path = source.session.cache_path.clone();

    let lease_manager = ControllerLeaseManager::default();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let profiling = DiagnosticsProfilingCoordinator::new(Arc::new(SuccessfulProfilingBackend));
    let service = test_session_service(&source, &lease_manager, Some(&store), Some(&profiling));

    let lease = service
        .acquire_controller("tester".to_string(), Some(DIAGNOSTICS_TEST_LEASE_TTL_MS))
        .expect("acquire controller");
    let case = service
        .create_case(
            &lease.lease_id,
            "AJA workflow".to_string(),
            None,
            Some("aja".to_string()),
            None,
        )
        .expect("create case");
    let run = service
        .create_run(&lease.lease_id, case.id, Some("aja-pass-1".to_string()))
        .expect("create run");

    let mut workflow = service
        .start_workflow(&lease.lease_id, &run.id, "aja_hybrid_v1".to_string())
        .expect("start workflow");
    assert_eq!(
        workflow.current_step_id.as_deref(),
        Some("capture_attach_snapshot")
    );

    workflow = service
        .advance_workflow_step(&lease.lease_id, &workflow.id, "capture_attach_snapshot")
        .expect("advance capture snapshot");
    assert_eq!(
        workflow.steps[0].status,
        InvestigationWorkflowStepStatus::Completed
    );
    assert_eq!(workflow.steps[0].artifacts.len(), 1);
    assert_eq!(
        workflow.steps[0]
            .tool_invocation
            .as_ref()
            .and_then(|invocation| invocation.arguments.get("recent_events_limit"))
            .and_then(serde_json::Value::as_u64),
        Some(24)
    );
    assert_eq!(
        workflow.current_step_id.as_deref(),
        Some("start_balanced_profiling")
    );

    workflow = service
        .advance_workflow_step(&lease.lease_id, &workflow.id, "start_balanced_profiling")
        .expect("advance profiling step");
    assert_eq!(
        workflow.steps[1].status,
        InvestigationWorkflowStepStatus::Completed
    );
    assert_eq!(
        workflow.current_step_id.as_deref(),
        Some("run_manual_aja_phase")
    );

    workflow = service
        .advance_workflow_step(&lease.lease_id, &workflow.id, "run_manual_aja_phase")
        .expect("advance manual step");
    assert_eq!(
        workflow.status,
        InvestigationWorkflowStatus::WaitingForOperator
    );
    assert_eq!(
        workflow.steps[2].status,
        InvestigationWorkflowStepStatus::WaitingForOperator
    );

    workflow = service
        .acknowledge_manual_step(
            &lease.lease_id,
            &workflow.id,
            "run_manual_aja_phase",
            Some("operator".to_string()),
            Some("AJA phase complete".to_string()),
        )
        .expect("acknowledge manual step");
    assert_eq!(
        workflow.steps[2].status,
        InvestigationWorkflowStepStatus::Completed
    );
    assert_eq!(
        workflow.current_step_id.as_deref(),
        Some("stop_active_profiling")
    );

    workflow = service
        .advance_workflow_step(&lease.lease_id, &workflow.id, "stop_active_profiling")
        .expect("advance stop profiling");
    assert_eq!(
        workflow.steps[3].status,
        InvestigationWorkflowStepStatus::Completed
    );
    assert_eq!(workflow.steps[3].artifacts.len(), 1);
    assert_eq!(
        workflow.current_step_id.as_deref(),
        Some("materialize_report")
    );

    workflow = service
        .advance_workflow_step(&lease.lease_id, &workflow.id, "materialize_report")
        .expect("advance report step");
    assert_eq!(workflow.status, InvestigationWorkflowStatus::Completed);
    assert!(workflow.current_step_id.is_none());

    let persisted_run = store.load_run(&run.id).expect("load run");
    let persisted_workflow = store
        .load_workflow_run(&workflow.id)
        .expect("load workflow run");
    assert_eq!(persisted_run.workflow_run_ids, vec![workflow.id.clone()]);
    assert_eq!(persisted_run.report_ids.len(), 1);
    assert_eq!(persisted_run.profiling_windows.len(), 1);
    assert_eq!(
        persisted_run.profiling_windows[0].requested_duration_ms,
        4_000
    );
    assert_eq!(persisted_run.profiling_windows[0].sample_rate_hz, Some(80));
    assert_eq!(persisted_workflow.steps[0].artifacts.len(), 1);
    assert_eq!(
        persisted_workflow.steps[2]
            .operator_acknowledged_by
            .as_deref(),
        Some("operator")
    );
    assert!(
        persisted_run
            .timeline
            .iter()
            .any(|entry| entry.title == "AJA workload start")
    );
    assert!(
        persisted_run
            .timeline
            .iter()
            .any(|entry| entry.title == "AJA workload done")
    );
}

#[test]
fn workflow_progress_fails_when_pending_steps_are_blocked() {
    let now = now_unix_millis();
    let mut workflow = InvestigationWorkflowRun {
        schema_version: crate::investigation::INVESTIGATION_WORKFLOW_RUN_SCHEMA_VERSION,
        id: "workflow-blocked".to_string(),
        case_id: "case-1".to_string(),
        run_id: "run-1".to_string(),
        created_unix_ms: now,
        updated_unix_ms: now,
        workflow_id: "blocked".to_string(),
        workflow_version: 1,
        workflow_title: "Blocked workflow".to_string(),
        supported_workload: "generic".to_string(),
        description: "test".to_string(),
        status: InvestigationWorkflowStatus::InProgress,
        current_step_id: Some("completed".to_string()),
        tools: Vec::new(),
        steps: vec![
            crate::investigation::InvestigationWorkflowStepState {
                step_id: "completed".to_string(),
                title: "Completed".to_string(),
                instructions: String::new(),
                kind: InvestigationWorkflowStepKind::Note,
                requires_controller: true,
                allowed_after: Vec::new(),
                expected_artifacts: Vec::new(),
                failure_policy: InvestigationWorkflowFailurePolicy::Retryable,
                on_enter_marker: None,
                on_exit_marker: None,
                operator_ack_text: None,
                tool: None,
                tool_arguments: None,
                status: InvestigationWorkflowStepStatus::Completed,
                started_unix_ms: Some(now),
                completed_unix_ms: Some(now),
                last_error: None,
                output_summary: None,
                artifacts: Vec::new(),
                operator_acknowledged_by: None,
                operator_acknowledgement_note: None,
                tool_invocation: None,
            },
            crate::investigation::InvestigationWorkflowStepState {
                step_id: "blocked".to_string(),
                title: "Blocked".to_string(),
                instructions: String::new(),
                kind: InvestigationWorkflowStepKind::ToolStep,
                requires_controller: true,
                allowed_after: vec!["missing-step".to_string()],
                expected_artifacts: Vec::new(),
                failure_policy: InvestigationWorkflowFailurePolicy::Retryable,
                on_enter_marker: None,
                on_exit_marker: None,
                operator_ack_text: None,
                tool: None,
                tool_arguments: None,
                status: InvestigationWorkflowStepStatus::Pending,
                started_unix_ms: None,
                completed_unix_ms: None,
                last_error: None,
                output_summary: None,
                artifacts: Vec::new(),
                operator_acknowledged_by: None,
                operator_acknowledgement_note: None,
                tool_invocation: None,
            },
        ],
    };

    complete_workflow_progress(&mut workflow);

    assert_eq!(workflow.status, InvestigationWorkflowStatus::Failed);
    assert!(workflow.current_step_id.is_none());
    assert_eq!(
        workflow.steps[1].last_error.as_deref(),
        Some(
            "workflow run 'workflow-blocked' has pending steps with unsatisfiable dependencies: blocked"
        )
    );
}

#[test]
fn profiling_requests_start_stop_and_persist_window() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let profiling = DiagnosticsProfilingCoordinator::new(Arc::new(SuccessfulProfilingBackend));
    let service = test_session_service(&source, &lease_manager, Some(&store), Some(&profiling));

    let lease = service
        .acquire_controller("tester".to_string(), Some(DIAGNOSTICS_TEST_LEASE_TTL_MS))
        .expect("acquire controller");
    let case = service
        .create_case(
            &lease.lease_id,
            "AJA slowdown".to_string(),
            Some("Copy stalls after warmup".to_string()),
            Some("aja".to_string()),
            Some("artist workstation".to_string()),
        )
        .expect("create case");
    let run = service
        .create_run(
            &lease.lease_id,
            case.id.clone(),
            Some("copy-pass-1".to_string()),
        )
        .expect("create run");

    let started = service
        .start_profiling_window(
            &lease.lease_id,
            &run.id,
            StartProfilingWindowRequest {
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                budget: ProfilingBudget {
                    duration_ms: 500,
                    sample_rate_hz: Some(99),
                },
            },
        )
        .expect("start profiling window");

    assert_eq!(started.status, ProfilingWindowStatus::Running);

    let stopped = service
        .stop_profiling_window(&lease.lease_id, &started.window_id)
        .expect("stop profiling window");

    assert_eq!(stopped.status, ProfilingWindowStatus::Completed);
    assert_eq!(stopped.sample_rate_hz, Some(99));
    assert_eq!(stopped.preset, DiagnosticsPreset::Balanced);

    let persisted_run = store.load_run(&run.id).expect("load run");
    assert_eq!(persisted_run.profiling_windows.len(), 1);
    assert_eq!(
        persisted_run.profiling_windows[0]
            .artifact
            .as_ref()
            .map(|artifact| artifact.kind.as_str()),
        Some("pprof")
    );
}

#[test]
fn profiling_requests_reject_budget_above_limit() {
    let source = static_source();
    let lease_manager = ControllerLeaseManager::default();
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let profiling = DiagnosticsProfilingCoordinator::new(Arc::new(SuccessfulProfilingBackend));
    let service = test_session_service(&source, &lease_manager, Some(&store), Some(&profiling));

    let lease = service
        .acquire_controller("tester".to_string(), Some(DIAGNOSTICS_TEST_LEASE_TTL_MS))
        .expect("acquire controller");

    let response = service.start_profiling_window(
        &lease.lease_id,
        "run-1",
        StartProfilingWindowRequest {
            capture_mode: ProfilingCaptureMode::Cpu,
            preset: DiagnosticsPreset::Minimal,
            budget: ProfilingBudget {
                duration_ms: MAX_PROFILE_WINDOW_DURATION_MS + 1,
                sample_rate_hz: Some(100),
            },
        },
    );

    assert!(matches!(
        response,
        Err(DiagnosticsServiceError::ProfilingStart(
            ProfilingWindowStartError::Budget(_)
        ))
    ));
}

#[test]
fn profiling_coordinator_returns_unsupported_window_without_crashing() {
    let profiling = DiagnosticsProfilingCoordinator::new(Arc::new(UnsupportedProfilingBackend(
        "profiling backend unavailable".to_string(),
    )));

    let window = profiling
        .start_window(
            "run-1",
            StartProfilingWindowRequest {
                capture_mode: ProfilingCaptureMode::Cpu,
                preset: DiagnosticsPreset::Minimal,
                budget: ProfilingBudget {
                    duration_ms: 500,
                    sample_rate_hz: Some(100),
                },
            },
            None,
        )
        .expect("unsupported backend should fail open");

    assert_eq!(window.status, ProfilingWindowStatus::Unsupported);
    assert_eq!(
        window.error.as_deref(),
        Some("profiling backend unavailable")
    );
}

#[test]
fn profiling_start_reports_backend_resolved_sample_rate() {
    let profiling = DiagnosticsProfilingCoordinator::new(Arc::new(AdjustingProfilingBackend));

    let started = profiling
        .start_window(
            "run-1",
            StartProfilingWindowRequest {
                capture_mode: ProfilingCaptureMode::Cpu,
                preset: DiagnosticsPreset::Balanced,
                budget: ProfilingBudget {
                    duration_ms: 500,
                    sample_rate_hz: Some(99),
                },
            },
            None,
        )
        .expect("start profiling window");

    assert_eq!(started.sample_rate_hz, Some(77));
}

#[test]
fn profiling_status_does_not_block_while_backend_is_starting() {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let profiling = Arc::new(DiagnosticsProfilingCoordinator::new(Arc::new(
        BlockingProfilingBackend {
            entered_tx,
            release_rx: std::sync::Mutex::new(Some(release_rx)),
        },
    )));

    let worker = {
        let profiling = profiling.clone();
        std::thread::spawn(move || {
            profiling
                .start_window(
                    "run-1",
                    StartProfilingWindowRequest {
                        capture_mode: ProfilingCaptureMode::Cpu,
                        preset: DiagnosticsPreset::Balanced,
                        budget: ProfilingBudget {
                            duration_ms: 500,
                            sample_rate_hz: Some(100),
                        },
                    },
                    None,
                )
                .expect("start profiling window");
        })
    };

    entered_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("backend entered start");

    let status_worker = {
        let profiling = profiling.clone();
        std::thread::spawn(move || profiling.status())
    };

    let status = status_worker
        .join()
        .expect("status thread should not block indefinitely");
    assert!(status.active_window.is_none());

    release_tx.send(()).expect("release backend start");
    worker.join().expect("worker thread");
}

#[test]
fn wall_blocking_profiling_writes_summary_artifact() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let mut source = static_source();
    source.session.cache_path = temp.path().to_string_lossy().into_owned();
    let profiling =
        DiagnosticsProfilingCoordinator::new(Arc::new(CompositeRuntimeProfilingBackend::new(
            Arc::new(source),
            Arc::new(UnsupportedProfilingBackend("cpu unavailable".to_string())),
        )));

    let started = profiling
        .start_window(
            "run-1",
            StartProfilingWindowRequest {
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                budget: ProfilingBudget {
                    duration_ms: 500,
                    sample_rate_hz: Some(25),
                },
            },
            None,
        )
        .expect("start wall blocking profiling");
    let stopped = profiling
        .stop_window(&started.window_id, None)
        .expect("stop wall blocking profiling");

    assert_eq!(stopped.status, ProfilingWindowStatus::Completed);
    assert_eq!(
        stopped
            .artifact
            .as_ref()
            .map(|artifact| artifact.kind.as_str()),
        Some("wall_blocking_summary")
    );
}

#[test]
fn compare_run_span_matches_markers_and_persists_delta_summary() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let case = store
        .create_case(NewInvestigationCase {
            title: "compare".to_string(),
            problem_statement: None,
            workload_identity: None,
            target_environment: None,
        })
        .expect("create case");
    let baseline = store
        .create_run(NewInvestigationRun {
            case_id: case.id.clone(),
            session_id: "session-a".to_string(),
            fingerprint: static_source().fingerprint.clone(),
        })
        .expect("create baseline run");
    let candidate = store
        .create_run(NewInvestigationRun {
            case_id: case.id.clone(),
            session_id: "session-b".to_string(),
            fingerprint: static_source().fingerprint.clone(),
        })
        .expect("create candidate run");

    let baseline_start = store
        .append_timeline_entry(
            &baseline.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "warm copy".to_string(),
                body: None,
                actor: None,
                reason: None,
            },
        )
        .expect("baseline start marker");
    store
        .append_timeline_entry(
            &baseline.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Mcp,
                kind: TimelineEntryKind::Anomaly,
                title: "queue growth".to_string(),
                body: Some("Pending uploads climbed during warm copy.".to_string()),
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("annotate anomaly".to_string()),
            },
        )
        .expect("baseline anomaly");
    let baseline_end = store
        .append_timeline_entry(
            &baseline.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "warm copy done".to_string(),
                body: None,
                actor: None,
                reason: None,
            },
        )
        .expect("baseline end marker");
    let candidate_start = store
        .append_timeline_entry(
            &candidate.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "warm copy".to_string(),
                body: None,
                actor: None,
                reason: None,
            },
        )
        .expect("candidate start marker");
    store
        .append_timeline_entry(
            &candidate.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Mcp,
                kind: TimelineEntryKind::Anomaly,
                title: "queue growth".to_string(),
                body: Some("Pending uploads still climbed, but less sharply.".to_string()),
                actor: Some("studiofs-dev-mcp".to_string()),
                reason: Some("annotate anomaly".to_string()),
            },
        )
        .expect("candidate anomaly");
    let candidate_end = store
        .append_timeline_entry(
            &candidate.id,
            NewTimelineEntry {
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "warm copy done".to_string(),
                body: None,
                actor: None,
                reason: None,
            },
        )
        .expect("candidate end marker");

    let mut baseline_start_snapshot = DiagnosticsRuntimeSnapshot::default();
    baseline_start_snapshot.upload.pending_uploads = 2;
    let mut baseline_end_snapshot = DiagnosticsRuntimeSnapshot::default();
    baseline_end_snapshot.upload.pending_uploads = 9;
    baseline_end_snapshot
        .write_path
        .payload_store_sync_data
        .queue_wait_total_ms = 40.0;
    let mut candidate_start_snapshot = DiagnosticsRuntimeSnapshot::default();
    candidate_start_snapshot.upload.pending_uploads = 1;
    let mut candidate_end_snapshot = DiagnosticsRuntimeSnapshot::default();
    candidate_end_snapshot.upload.pending_uploads = 4;
    candidate_end_snapshot
        .write_path
        .payload_store_sync_data
        .queue_wait_total_ms = 10.0;

    store
        .append_marker_snapshot(
            &baseline.id,
            NewInvestigationMarkerSnapshot {
                marker_sequence: baseline_start.sequence,
                marker_title: baseline_start.title.clone(),
                runtime_snapshot: baseline_start_snapshot.clone(),
            },
        )
        .expect("baseline start snapshot");
    store
        .append_marker_snapshot(
            &baseline.id,
            NewInvestigationMarkerSnapshot {
                marker_sequence: baseline_end.sequence,
                marker_title: baseline_end.title.clone(),
                runtime_snapshot: baseline_end_snapshot.clone(),
            },
        )
        .expect("baseline end snapshot");
    store
        .append_marker_snapshot(
            &candidate.id,
            NewInvestigationMarkerSnapshot {
                marker_sequence: candidate_start.sequence,
                marker_title: candidate_start.title.clone(),
                runtime_snapshot: candidate_start_snapshot.clone(),
            },
        )
        .expect("candidate start snapshot");
    store
        .append_marker_snapshot(
            &candidate.id,
            NewInvestigationMarkerSnapshot {
                marker_sequence: candidate_end.sequence,
                marker_title: candidate_end.title.clone(),
                runtime_snapshot: candidate_end_snapshot.clone(),
            },
        )
        .expect("candidate end snapshot");

    let baseline_artifact = write_wall_blocking_artifact_fixture(
        &temp,
        "baseline-compare-window.json",
        baseline_start_snapshot,
        baseline_end_snapshot,
        Vec::new(),
        Vec::new(),
    );
    store
        .append_profiling_window(
            &baseline.id,
            NewProfilingWindow {
                window_id: "baseline-window".to_string(),
                occurred_unix_ms: baseline_end.occurred_unix_ms,
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                requested_duration_ms: 1_000,
                actual_duration_ms: Some(950),
                sample_rate_hz: Some(99),
                estimated_cost: ProfilingCostEstimate {
                    score: 2,
                    label: "balanced".to_string(),
                },
                status: ProfilingWindowStatus::Completed,
                artifact: Some(baseline_artifact),
                error: None,
            },
        )
        .expect("baseline profiling window");
    let candidate_artifact = write_wall_blocking_artifact_fixture(
        &temp,
        "candidate-compare-window.json",
        candidate_start_snapshot,
        candidate_end_snapshot,
        Vec::new(),
        Vec::new(),
    );
    store
        .append_profiling_window(
            &candidate.id,
            NewProfilingWindow {
                window_id: "candidate-window".to_string(),
                occurred_unix_ms: candidate_end.occurred_unix_ms,
                capture_mode: ProfilingCaptureMode::WallBlocking,
                preset: DiagnosticsPreset::Balanced,
                requested_duration_ms: 1_000,
                actual_duration_ms: Some(950),
                sample_rate_hz: Some(99),
                estimated_cost: ProfilingCostEstimate {
                    score: 2,
                    label: "balanced".to_string(),
                },
                status: ProfilingWindowStatus::Completed,
                artifact: Some(candidate_artifact),
                error: None,
            },
        )
        .expect("candidate profiling window");

    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");

    let response = execute_request(
        DiagnosticsRequest::CompareRunSpan {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            request: DiagnosticsCompareRunSpanRequest {
                baseline_run_id: baseline.id.clone(),
                candidate_run_id: candidate.id.clone(),
                start_marker_title: "warm copy".to_string(),
                end_marker_title: "warm copy done".to_string(),
            },
        },
        "secret",
        &static_source(),
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::InvestigationSpanComparison { comparison } = response else {
        panic!("expected comparison response");
    };
    assert_eq!(comparison.baseline_run_id, baseline.id);
    assert_eq!(comparison.candidate_run_id, candidate.id);
    assert!(
        comparison.summary.metric_deltas.iter().any(|metric| {
            metric.metric == "upload.pending_uploads_delta" && metric.delta < 0.0
        })
    );
    assert!(comparison.summary.anomaly_deltas.iter().any(|anomaly| {
        anomaly.title == "queue growth"
            && anomaly.baseline_count == 1
            && anomaly.candidate_count == 1
    }));
    assert!(comparison.summary.hotspot_deltas.iter().any(|hotspot| {
        hotspot.metric == "payload_store_sync_data.queue_wait_total_ms_delta" && hotspot.delta < 0.0
    }));
    assert!(
        comparison
            .supporting_evidence
            .iter()
            .any(|reference| reference.id.starts_with(&format!("{}:", baseline.id)))
    );
    assert!(
        comparison
            .supporting_evidence
            .iter()
            .any(|reference| reference.id.starts_with(&format!("{}:", candidate.id)))
    );
}

#[test]
fn compare_run_span_rejects_ambiguous_marker_titles() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let case = store
        .create_case(NewInvestigationCase {
            title: "ambiguous markers".to_string(),
            problem_statement: None,
            workload_identity: None,
            target_environment: None,
        })
        .expect("create case");
    let baseline = store
        .create_run(NewInvestigationRun {
            case_id: case.id.clone(),
            session_id: "session-a".to_string(),
            fingerprint: static_source().fingerprint.clone(),
        })
        .expect("create baseline run");
    let candidate = store
        .create_run(NewInvestigationRun {
            case_id: case.id.clone(),
            session_id: "session-b".to_string(),
            fingerprint: static_source().fingerprint.clone(),
        })
        .expect("create candidate run");

    for run_id in [&baseline.id, &candidate.id] {
        let first = store
            .append_timeline_entry(
                run_id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Cli,
                    kind: TimelineEntryKind::Marker,
                    title: "warm copy".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("first marker");
        let second = store
            .append_timeline_entry(
                run_id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Cli,
                    kind: TimelineEntryKind::Marker,
                    title: "warm copy".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("second marker");
        store
            .append_marker_snapshot(
                run_id,
                NewInvestigationMarkerSnapshot {
                    marker_sequence: first.sequence,
                    marker_title: "warm copy".to_string(),
                    runtime_snapshot: DiagnosticsRuntimeSnapshot::default(),
                },
            )
            .expect("first marker snapshot");
        store
            .append_marker_snapshot(
                run_id,
                NewInvestigationMarkerSnapshot {
                    marker_sequence: second.sequence,
                    marker_title: "warm copy".to_string(),
                    runtime_snapshot: DiagnosticsRuntimeSnapshot::default(),
                },
            )
            .expect("second marker snapshot");
    }

    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");

    let response = execute_request(
        DiagnosticsRequest::CompareRunSpan {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            request: DiagnosticsCompareRunSpanRequest {
                baseline_run_id: baseline.id,
                candidate_run_id: candidate.id,
                start_marker_title: "warm copy".to_string(),
                end_marker_title: "warm copy".to_string(),
            },
        },
        "secret",
        &static_source(),
        &lease_manager,
        Some(&store),
        None,
    );

    assert!(matches!(
        response,
        DiagnosticsResponse::Error { code, message }
            if code == "investigation_store_error" && message.contains("ambiguous")
    ));
}

#[test]
fn hypothesis_status_updates_remain_traceable_to_comparison_evidence() {
    let temp = mounts_dir_temp();
    let store = InvestigationStore::new(temp.path().join("investigations"));
    let case = store
        .create_case(NewInvestigationCase {
            title: "hypothesis".to_string(),
            problem_statement: None,
            workload_identity: None,
            target_environment: None,
        })
        .expect("create case");

    let lease_manager = ControllerLeaseManager::default();
    let lease = lease_manager
        .acquire("tester".to_string(), Some(500))
        .expect("acquire lease");

    let create_response = execute_request(
        DiagnosticsRequest::CreateHypothesis {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id.clone(),
            case_id: case.id.clone(),
            statement: "queue wait should fall".to_string(),
            expected_signal: "candidate comparison should show a negative queue wait delta"
                .to_string(),
            supporting_evidence: Vec::new(),
        },
        "secret",
        &static_source(),
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::InvestigationHypothesis { hypothesis } = create_response else {
        panic!("expected hypothesis response");
    };

    let update_response = execute_request(
        DiagnosticsRequest::UpdateHypothesisStatus {
            auth_token: "secret".to_string(),
            lease_id: lease.lease_id,
            hypothesis_id: hypothesis.id.clone(),
            status: InvestigationHypothesisStatus::Supported,
            actor: Some("studiofs-dev-mcp".to_string()),
            reason: Some("matched comparison improved the hotspot".to_string()),
            supporting_evidence: vec![DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::InvestigationSpanComparison,
                id: "comparison-1".to_string(),
                field: None,
            }],
        },
        "secret",
        &static_source(),
        &lease_manager,
        Some(&store),
        None,
    );

    let DiagnosticsResponse::InvestigationHypothesis { hypothesis } = update_response else {
        panic!("expected updated hypothesis response");
    };
    assert_eq!(hypothesis.status, InvestigationHypothesisStatus::Supported);
    assert_eq!(hypothesis.status_history.len(), 2);
    assert_eq!(
        hypothesis.status_history[1].supporting_evidence[0].source,
        DiagnosticsEvidenceReferenceSource::InvestigationSpanComparison
    );
}

#[test]
fn client_accepts_large_response_frames() {
    let mut source = static_source();
    source.session.cache_path = "C:/".to_string() + &"a".repeat(96 * 1024);

    crate::runtime_bridge::block_on(async move {
        let (client, server) = tokio::io::duplex(256 * 1024);
        let permit = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .map_err(anyhow::Error::from)?;
        let server_task = tokio::spawn(handle_client(
            server,
            "secret".to_string(),
            Arc::new(source),
            Arc::new(ControllerLeaseManager::default()),
            None,
            None,
            permit,
        ));

        let response = send_request(
            client,
            DiagnosticsRequest::SessionInfo {
                auth_token: "secret".to_string(),
            },
        )
        .await?;

        match response {
            DiagnosticsResponse::SessionInfo { session } => {
                assert!(session.cache_path.len() > MAX_DIAGNOSTICS_REQUEST_FRAME_BYTES);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        server_task.await.map_err(anyhow::Error::from)??;
        Ok::<(), anyhow::Error>(())
    })
    .expect("large response exchange succeeds");
}

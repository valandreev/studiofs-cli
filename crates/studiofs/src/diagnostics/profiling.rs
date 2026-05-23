use super::*;

pub(crate) trait RuntimeProfilingStopper: Send + 'static {
    fn stop(self: Box<Self>) -> Result<ProfilingBackendStopResult>;
}

pub(crate) struct ActiveRuntimeProfilingWindow {
    pub sample_rate_hz: Option<u32>,
    pub stopper: Box<dyn RuntimeProfilingStopper>,
}

pub(crate) struct ProfilingBackendStopResult {
    pub actual_duration_ms: Option<u64>,
    pub status: ProfilingWindowStatus,
    pub artifact: Option<ProfileArtifactRecord>,
    pub error: Option<String>,
}

#[allow(dead_code)]
pub(crate) enum ProfilingBackendStartResult {
    Active(ActiveRuntimeProfilingWindow),
    Unsupported { error: String },
}

pub(crate) trait RuntimeProfilingBackend: Send + Sync + 'static {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode>;
    fn start_window(&self, request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult;
}

pub(crate) struct DiagnosticsProfilingCoordinator {
    backend: Arc<dyn RuntimeProfilingBackend>,
    active_window: Mutex<ProfilingWindowState>,
}

pub(super) struct ActiveProfilingWindow {
    window_id: String,
    run_id: String,
    occurred_unix_ms: u64,
    started_at: Instant,
    capture_mode: ProfilingCaptureMode,
    preset: DiagnosticsPreset,
    requested_duration_ms: u64,
    sample_rate_hz: Option<u32>,
    estimated_cost: ProfilingCostEstimate,
    stopper: Box<dyn RuntimeProfilingStopper>,
}

pub(super) enum ProfilingWindowState {
    Idle,
    Starting,
    Active(ActiveProfilingWindow),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ProfilingBudgetValidationError {
    ZeroDuration,
    DurationTooLong {
        max_duration_ms: u64,
    },
    InvalidSampleRate {
        requested_hz: u32,
        min_hz: u32,
        max_hz: u32,
    },
}

impl std::fmt::Display for ProfilingBudgetValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroDuration => write!(f, "profiling duration must be greater than zero"),
            Self::DurationTooLong { max_duration_ms } => {
                write!(f, "profiling budget exceeds limit of {max_duration_ms} ms")
            }
            Self::InvalidSampleRate {
                requested_hz,
                min_hz,
                max_hz,
            } => write!(
                f,
                "profiling sample rate {requested_hz} Hz must be between {min_hz} and {max_hz} Hz"
            ),
        }
    }
}

impl std::error::Error for ProfilingBudgetValidationError {}

#[derive(Debug)]
pub(super) enum ProfilingWindowStartError {
    Budget(ProfilingBudgetValidationError),
    Busy,
    Store(anyhow::Error),
}

impl std::fmt::Display for ProfilingWindowStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Budget(error) => write!(f, "{error}"),
            Self::Busy => write!(f, "profiling window already active"),
            Self::Store(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for ProfilingWindowStartError {}

pub(super) struct CompositeRuntimeProfilingBackend {
    wall_blocking_backend: Arc<dyn RuntimeProfilingBackend>,
    cpu_backend: Arc<dyn RuntimeProfilingBackend>,
}

impl CompositeRuntimeProfilingBackend {
    pub(super) fn new(
        source: Arc<dyn DiagnosticsDataSource>,
        cpu_backend: Arc<dyn RuntimeProfilingBackend>,
    ) -> Self {
        Self {
            wall_blocking_backend: Arc::new(WallBlockingRuntimeProfilingBackend::new(source)),
            cpu_backend,
        }
    }
}

impl RuntimeProfilingBackend for CompositeRuntimeProfilingBackend {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode> {
        let mut modes = self.wall_blocking_backend.supported_capture_modes();
        for mode in self.cpu_backend.supported_capture_modes() {
            if !modes.contains(&mode) {
                modes.push(mode);
            }
        }
        modes
    }

    fn start_window(&self, request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        match request.capture_mode {
            ProfilingCaptureMode::WallBlocking => self.wall_blocking_backend.start_window(request),
            ProfilingCaptureMode::Cpu => self.cpu_backend.start_window(request),
        }
    }
}

pub(super) struct WallBlockingRuntimeProfilingBackend {
    source: Arc<dyn DiagnosticsDataSource>,
}

impl WallBlockingRuntimeProfilingBackend {
    pub(super) fn new(source: Arc<dyn DiagnosticsDataSource>) -> Self {
        Self { source }
    }
}

pub(super) struct WallBlockingProfileStopper {
    source: Arc<dyn DiagnosticsDataSource>,
    cache_path: PathBuf,
    started_unix_ms: u64,
    started_at: Instant,
    budget: ProfilingBudget,
    start_snapshot: DiagnosticsRuntimeSnapshot,
    start_events: Vec<DiagnosticsRecentEvent>,
}

impl RuntimeProfilingBackend for WallBlockingRuntimeProfilingBackend {
    fn supported_capture_modes(&self) -> Vec<ProfilingCaptureMode> {
        vec![ProfilingCaptureMode::WallBlocking]
    }

    fn start_window(&self, request: &StartProfilingWindowRequest) -> ProfilingBackendStartResult {
        ProfilingBackendStartResult::Active(ActiveRuntimeProfilingWindow {
            sample_rate_hz: request.budget.sample_rate_hz,
            stopper: Box::new(WallBlockingProfileStopper {
                cache_path: PathBuf::from(self.source.session_info().cache_path),
                source: self.source.clone(),
                started_unix_ms: now_unix_millis(),
                started_at: Instant::now(),
                budget: request.budget.clone(),
                start_snapshot: self.source.runtime_snapshot(),
                start_events: self
                    .source
                    .recent_events(DEFAULT_RECENT_EVENTS_LIMIT.max(8)),
            }),
        })
    }
}

impl RuntimeProfilingStopper for WallBlockingProfileStopper {
    fn stop(self: Box<Self>) -> Result<ProfilingBackendStopResult> {
        let finished_unix_ms = now_unix_millis();
        let end_snapshot = self.source.runtime_snapshot();
        let end_events = self
            .source
            .recent_events(DEFAULT_RECENT_EVENTS_LIMIT.max(8));
        let actual_duration_ms = self.started_at.elapsed().as_millis().try_into().ok();

        let payload = serde_json::json!({
            "schema_version": 1,
            "capture_mode": "wall_blocking",
            "started_unix_ms": self.started_unix_ms,
            "finished_unix_ms": finished_unix_ms,
            "requested_duration_ms": self.budget.duration_ms,
            "actual_duration_ms": actual_duration_ms,
            "sampling_hint_hz": self.budget.sample_rate_hz,
            "start_snapshot": self.start_snapshot,
            "end_snapshot": end_snapshot,
            "start_events": self.start_events,
            "end_events": end_events,
        });

        let mut artifact = None;
        let mut error = None;
        match write_wall_blocking_artifact(&self.cache_path, &payload) {
            Ok(path) => {
                let bytes = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
                artifact = Some(ProfileArtifactRecord {
                    kind: "wall_blocking_summary".to_string(),
                    path: path.to_string_lossy().into_owned(),
                    bytes,
                });
            }
            Err(write_error) => {
                error = Some(write_error.to_string());
            }
        }

        Ok(ProfilingBackendStopResult {
            actual_duration_ms,
            status: ProfilingWindowStatus::Completed,
            artifact,
            error,
        })
    }
}

impl DiagnosticsProfilingCoordinator {
    pub(super) fn new(backend: Arc<dyn RuntimeProfilingBackend>) -> Self {
        Self {
            backend,
            active_window: Mutex::new(ProfilingWindowState::Idle),
        }
    }

    pub(super) fn start_window(
        &self,
        run_id: &str,
        request: StartProfilingWindowRequest,
        investigation_store: Option<&InvestigationStore>,
    ) -> std::result::Result<ProfilingWindowRecord, ProfilingWindowStartError> {
        validate_profiling_request(&request).map_err(ProfilingWindowStartError::Budget)?;

        let estimated_cost = estimate_profiling_cost(&request);
        let occurred_unix_ms = now_unix_millis();
        let effective_sample_rate_hz = Some(
            request
                .budget
                .sample_rate_hz
                .unwrap_or(DEFAULT_PROFILE_SAMPLE_RATE_HZ),
        );
        let window_id = Uuid::new_v4().to_string();

        {
            let mut active_window = self.active_window.lock();
            match &*active_window {
                ProfilingWindowState::Idle => {
                    *active_window = ProfilingWindowState::Starting;
                }
                ProfilingWindowState::Starting | ProfilingWindowState::Active(_) => {
                    return Err(ProfilingWindowStartError::Busy);
                }
            }
        }

        match self.backend.start_window(&request) {
            ProfilingBackendStartResult::Active(active) => {
                let resolved_sample_rate_hz = active.sample_rate_hz.or(effective_sample_rate_hz);
                let mut active_window = self.active_window.lock();
                *active_window = ProfilingWindowState::Active(ActiveProfilingWindow {
                    window_id: window_id.clone(),
                    run_id: run_id.to_string(),
                    occurred_unix_ms,
                    started_at: Instant::now(),
                    capture_mode: request.capture_mode,
                    preset: request.preset,
                    requested_duration_ms: request.budget.duration_ms,
                    sample_rate_hz: resolved_sample_rate_hz,
                    estimated_cost: estimated_cost.clone(),
                    stopper: active.stopper,
                });
                Ok(ProfilingWindowRecord {
                    window_id,
                    sequence: 0,
                    occurred_unix_ms,
                    capture_mode: request.capture_mode,
                    preset: request.preset,
                    requested_duration_ms: request.budget.duration_ms,
                    actual_duration_ms: None,
                    sample_rate_hz: resolved_sample_rate_hz,
                    estimated_cost,
                    status: ProfilingWindowStatus::Running,
                    artifact: None,
                    error: None,
                })
            }
            ProfilingBackendStartResult::Unsupported { error } => {
                let mut active_window = self.active_window.lock();
                *active_window = ProfilingWindowState::Idle;
                let record = ProfilingWindowRecord {
                    window_id: window_id.clone(),
                    sequence: 0,
                    occurred_unix_ms,
                    capture_mode: request.capture_mode,
                    preset: request.preset,
                    requested_duration_ms: request.budget.duration_ms,
                    actual_duration_ms: None,
                    sample_rate_hz: effective_sample_rate_hz,
                    estimated_cost: estimated_cost.clone(),
                    status: ProfilingWindowStatus::Unsupported,
                    artifact: None,
                    error: Some(error.clone()),
                };
                if let Some(store) = investigation_store {
                    return store
                        .append_profiling_window(
                            run_id,
                            NewProfilingWindow {
                                window_id,
                                occurred_unix_ms,
                                capture_mode: request.capture_mode,
                                preset: request.preset,
                                requested_duration_ms: request.budget.duration_ms,
                                actual_duration_ms: None,
                                sample_rate_hz: effective_sample_rate_hz,
                                estimated_cost,
                                status: ProfilingWindowStatus::Unsupported,
                                artifact: None,
                                error: Some(error),
                            },
                        )
                        .map_err(ProfilingWindowStartError::Store);
                }
                Ok(record)
            }
        }
    }

    pub(super) fn stop_window(
        &self,
        window_id: &str,
        investigation_store: Option<&InvestigationStore>,
    ) -> Result<ProfilingWindowRecord> {
        let active = {
            let mut active_window = self.active_window.lock();
            match std::mem::replace(&mut *active_window, ProfilingWindowState::Idle) {
                ProfilingWindowState::Idle => {
                    return Err(anyhow!("profiling window is not currently active"));
                }
                ProfilingWindowState::Starting => {
                    *active_window = ProfilingWindowState::Starting;
                    return Err(anyhow!("profiling window is still starting"));
                }
                ProfilingWindowState::Active(active) if active.window_id != window_id => {
                    *active_window = ProfilingWindowState::Active(active);
                    return Err(anyhow!("profiling window '{window_id}' is not current"));
                }
                ProfilingWindowState::Active(active) => active,
            }
        };

        let result = match active.stopper.stop() {
            Ok(result) => result,
            Err(error) => ProfilingBackendStopResult {
                actual_duration_ms: None,
                status: ProfilingWindowStatus::Failed,
                artifact: None,
                error: Some(error.to_string()),
            },
        };

        let record = ProfilingWindowRecord {
            window_id: active.window_id.clone(),
            sequence: 0,
            occurred_unix_ms: active.occurred_unix_ms,
            capture_mode: active.capture_mode,
            preset: active.preset,
            requested_duration_ms: active.requested_duration_ms,
            actual_duration_ms: result
                .actual_duration_ms
                .or_else(|| u64::try_from(active.started_at.elapsed().as_millis()).ok()),
            sample_rate_hz: active.sample_rate_hz,
            estimated_cost: active.estimated_cost.clone(),
            status: result.status,
            artifact: result.artifact.clone(),
            error: result.error.clone(),
        };

        if let Some(store) = investigation_store {
            return store.append_profiling_window(
                &active.run_id,
                NewProfilingWindow {
                    window_id: active.window_id,
                    occurred_unix_ms: active.occurred_unix_ms,
                    capture_mode: active.capture_mode,
                    preset: active.preset,
                    requested_duration_ms: active.requested_duration_ms,
                    actual_duration_ms: record.actual_duration_ms,
                    sample_rate_hz: active.sample_rate_hz,
                    estimated_cost: active.estimated_cost,
                    status: record.status,
                    artifact: result.artifact,
                    error: result.error,
                },
            );
        }

        Ok(record)
    }

    pub(super) fn status(&self) -> DiagnosticsProfilingStatus {
        let active_window = self.active_window.lock();
        DiagnosticsProfilingStatus {
            supported_presets: vec![
                DiagnosticsPreset::Minimal,
                DiagnosticsPreset::Balanced,
                DiagnosticsPreset::Deep,
            ],
            supported_capture_modes: self.backend.supported_capture_modes(),
            default_duration_ms: DEFAULT_PROFILE_WINDOW_DURATION_MS,
            max_duration_ms: MAX_PROFILE_WINDOW_DURATION_MS,
            default_sample_rate_hz: DEFAULT_PROFILE_SAMPLE_RATE_HZ,
            max_sample_rate_hz: MAX_PROFILE_SAMPLE_RATE_HZ,
            active_window: match &*active_window {
                ProfilingWindowState::Active(active) => Some(ProfilingWindowRecord {
                    window_id: active.window_id.clone(),
                    sequence: 0,
                    occurred_unix_ms: active.occurred_unix_ms,
                    capture_mode: active.capture_mode,
                    preset: active.preset,
                    requested_duration_ms: active.requested_duration_ms,
                    actual_duration_ms: None,
                    sample_rate_hz: active.sample_rate_hz,
                    estimated_cost: active.estimated_cost.clone(),
                    status: ProfilingWindowStatus::Running,
                    artifact: None,
                    error: None,
                }),
                ProfilingWindowState::Idle | ProfilingWindowState::Starting => None,
            },
        }
    }
}

pub(super) fn validate_profiling_request(
    request: &StartProfilingWindowRequest,
) -> std::result::Result<(), ProfilingBudgetValidationError> {
    if request.budget.duration_ms == 0 {
        return Err(ProfilingBudgetValidationError::ZeroDuration);
    }
    if request.budget.duration_ms > MAX_PROFILE_WINDOW_DURATION_MS {
        return Err(ProfilingBudgetValidationError::DurationTooLong {
            max_duration_ms: MAX_PROFILE_WINDOW_DURATION_MS,
        });
    }
    if let Some(sample_rate_hz) = request.budget.sample_rate_hz
        && !(MIN_PROFILE_SAMPLE_RATE_HZ..=MAX_PROFILE_SAMPLE_RATE_HZ).contains(&sample_rate_hz)
    {
        return Err(ProfilingBudgetValidationError::InvalidSampleRate {
            requested_hz: sample_rate_hz,
            min_hz: MIN_PROFILE_SAMPLE_RATE_HZ,
            max_hz: MAX_PROFILE_SAMPLE_RATE_HZ,
        });
    }
    Ok(())
}

pub(super) fn estimate_profiling_cost(
    request: &StartProfilingWindowRequest,
) -> ProfilingCostEstimate {
    let base_score: u8 = match request.preset {
        DiagnosticsPreset::Minimal => 1,
        DiagnosticsPreset::Balanced => 2,
        DiagnosticsPreset::Deep => 3,
    };
    let longer_than_default = request.budget.duration_ms > DEFAULT_PROFILE_WINDOW_DURATION_MS;
    let higher_than_default = request
        .budget
        .sample_rate_hz
        .unwrap_or(DEFAULT_PROFILE_SAMPLE_RATE_HZ)
        > DEFAULT_PROFILE_SAMPLE_RATE_HZ;
    let score = if longer_than_default || higher_than_default {
        base_score.saturating_add(1)
    } else {
        base_score
    };
    let label = match request.preset {
        DiagnosticsPreset::Minimal => "minimal",
        DiagnosticsPreset::Balanced => "balanced",
        DiagnosticsPreset::Deep => "deep",
    };
    ProfilingCostEstimate {
        score,
        label: label.to_string(),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct WallBlockingProfileArtifact {
    pub(super) start_snapshot: DiagnosticsRuntimeSnapshot,
    pub(super) end_snapshot: DiagnosticsRuntimeSnapshot,
    #[serde(default)]
    pub(super) start_events: Vec<DiagnosticsRecentEvent>,
    #[serde(default)]
    pub(super) end_events: Vec<DiagnosticsRecentEvent>,
}

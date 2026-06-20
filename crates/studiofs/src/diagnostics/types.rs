use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub(crate) enum LocalDiagnosticsEndpoint {
    #[cfg(unix)]
    UnixSocket { path: String },
    #[cfg(windows)]
    WindowsNamedPipe { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LocalDiagnosticsDiscovery {
    pub protocol_version: u32,
    pub session_id: String,
    pub volume: String,
    pub volume_uuid: String,
    #[serde(default)]
    pub bridge: String,
    pub mount_point: String,
    pub pid: u32,
    pub cache_path: String,
    pub read_only: bool,
    pub started_unix_ms: u64,
    pub endpoint: LocalDiagnosticsEndpoint,
    pub auth_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ControllerLease {
    pub lease_id: String,
    pub client_name: String,
    pub ttl_ms: u64,
    pub expires_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsSessionInfo {
    pub protocol_version: u32,
    pub session_id: String,
    pub volume: String,
    pub volume_uuid: String,
    #[serde(default)]
    pub bridge: String,
    pub mount_point: String,
    pub pid: u32,
    pub cache_path: String,
    pub read_only: bool,
    pub started_unix_ms: u64,
    pub controller: Option<ControllerLease>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsHealthSnapshot {
    pub generated_unix_ms: u64,
    pub ingress: VfsIngressStats,
    pub upload: VfsUploadStats,
    pub file_sessions: VfsFileSessionStats,
    pub foreground_reads: VfsForegroundReadProtectionStats,
    pub write_path: VfsWritePathObservabilitySnapshot,
    #[serde(default)]
    pub zeroio: VfsZeroIoRuntimeSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsCacheSnapshot {
    pub state: VfsCacheStateSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsSynchronizationSnapshot {
    pub state: VfsSynchronizationStateSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsProcessSnapshot {
    pub cpu_percent: f64,
    pub rss_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsRuntimeSnapshot {
    pub schema_version: u32,
    pub generated_unix_ms: u64,
    #[serde(default)]
    pub process: DiagnosticsProcessSnapshot,
    pub ingress: VfsIngressStats,
    pub write_path: VfsWritePathObservabilitySnapshot,
    pub upload: VfsUploadStats,
    #[serde(default)]
    pub zeroio: VfsZeroIoRuntimeSnapshot,
    pub cache: DiagnosticsCacheSnapshot,
    pub synchronization: DiagnosticsSynchronizationSnapshot,
}

impl DiagnosticsRuntimeSnapshot {
    #[cfg(test)]
    pub(super) fn from_health(health: DiagnosticsHealthSnapshot) -> Self {
        Self {
            schema_version: DIAGNOSTICS_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
            generated_unix_ms: health.generated_unix_ms,
            process: DiagnosticsProcessSnapshot::default(),
            ingress: health.ingress,
            write_path: health.write_path,
            cache: DiagnosticsCacheSnapshot::default(),
            synchronization: DiagnosticsSynchronizationSnapshot {
                state: VfsSynchronizationStateSnapshot {
                    file_sessions: health.file_sessions,
                    foreground_reads: health.foreground_reads,
                    pending_metadata_ops: 0,
                    inflight_metadata_ops: 0,
                },
            },
            upload: health.upload,
            zeroio: health.zeroio,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsRecentEvent {
    pub sequence: u64,
    pub generated_unix_ms: u64,
    pub subsystem: String,
    pub kind: String,
    pub title: String,
    #[serde(default)]
    pub fields: serde_json::Value,
}

impl From<VfsRuntimeEvent> for DiagnosticsRecentEvent {
    fn from(value: VfsRuntimeEvent) -> Self {
        Self {
            sequence: value.sequence,
            generated_unix_ms: value.generated_unix_ms,
            subsystem: value.subsystem,
            kind: value.kind,
            title: value.title,
            fields: value.fields,
        }
    }
}

pub(crate) trait DiagnosticsDataSource: Send + Sync + 'static {
    fn session_info(&self) -> DiagnosticsSessionInfo;
    fn health_snapshot(&self) -> DiagnosticsHealthSnapshot;
    fn runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshot;
    fn recent_events(&self, limit: usize) -> Vec<DiagnosticsRecentEvent>;
    fn environment_fingerprint(
        &self,
        case_workload_identity: Option<&str>,
        run_workload_identity: Option<&str>,
    ) -> EnvironmentFingerprint;

    fn reset_observability_counters(&self) -> std::result::Result<(), DiagnosticsActionError> {
        Err(DiagnosticsActionError::ResetUnavailable)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProfilingBudget {
    pub duration_ms: u64,
    pub sample_rate_hz: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StartProfilingWindowRequest {
    pub capture_mode: ProfilingCaptureMode,
    pub preset: DiagnosticsPreset,
    pub budget: ProfilingBudget,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsProfilingStatus {
    pub supported_presets: Vec<DiagnosticsPreset>,
    pub supported_capture_modes: Vec<ProfilingCaptureMode>,
    pub default_duration_ms: u64,
    pub max_duration_ms: u64,
    pub default_sample_rate_hz: u32,
    pub max_sample_rate_hz: u32,
    pub active_window: Option<ProfilingWindowRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct DiagnosticsObjectIdentity {
    pub inode: Option<u64>,
    pub path: Option<String>,
    pub file_handle: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsMarkerSpan {
    pub start_sequence: u64,
    pub end_sequence: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct DiagnosticsEvidenceScope {
    pub subsystem: Option<String>,
    pub operation_type: Option<String>,
    pub object_identity: Option<DiagnosticsObjectIdentity>,
    pub marker_span: Option<DiagnosticsMarkerSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsEvidenceBudget {
    pub max_recent_events: usize,
    pub max_timeline_entries: usize,
    pub max_profiling_windows: usize,
    pub max_derived_items_per_kind: usize,
}

impl Default for DiagnosticsEvidenceBudget {
    fn default() -> Self {
        Self {
            max_recent_events: DEFAULT_EVIDENCE_RECENT_EVENTS_LIMIT,
            max_timeline_entries: DEFAULT_EVIDENCE_TIMELINE_ENTRIES_LIMIT,
            max_profiling_windows: DEFAULT_EVIDENCE_PROFILING_WINDOWS_LIMIT,
            max_derived_items_per_kind: DEFAULT_EVIDENCE_DERIVED_ITEMS_PER_KIND,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsEvidenceRequest {
    pub scope: DiagnosticsEvidenceScope,
    pub budget: DiagnosticsEvidenceBudget,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsResolvedMarkerSpan {
    pub start_sequence: u64,
    pub end_sequence: u64,
    pub start_unix_ms: u64,
    pub end_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct DiagnosticsEvidenceScopeResolution {
    pub subsystem: Option<String>,
    pub operation_type: Option<String>,
    pub object_identity: Option<DiagnosticsObjectIdentity>,
    pub marker_span: Option<DiagnosticsResolvedMarkerSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct DiagnosticsEvidenceTruncation {
    pub recent_events: bool,
    pub timeline_entries: bool,
    pub profiling_windows: bool,
    pub queue_growth: bool,
    pub blocking_intervals: bool,
    pub hotspots: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsEvidenceBudgetReport {
    pub requested: DiagnosticsEvidenceBudget,
    pub applied: DiagnosticsEvidenceBudget,
    pub truncation: DiagnosticsEvidenceTruncation,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsEvidenceReferenceSource {
    RuntimeSnapshot,
    RecentEvent,
    TimelineEntry,
    ProfilingWindow,
    ProfilingArtifact,
    MarkerSnapshot,
    InvestigationSpanComparison,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsEvidenceReference {
    pub source: DiagnosticsEvidenceReferenceSource,
    pub id: String,
    pub field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiagnosticsEvidenceProfilingWindow {
    pub window: ProfilingWindowRecord,
    pub summary: Option<serde_json::Value>,
    pub load_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsEvidenceRawView {
    #[serde(default)]
    pub snapshot_sections: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub recent_events: Vec<DiagnosticsRecentEvent>,
    #[serde(default)]
    pub timeline_entries: Vec<TimelineEntry>,
    #[serde(default)]
    pub profiling_windows: Vec<DiagnosticsEvidenceProfilingWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiagnosticsQueueGrowthEpisode {
    pub subsystem: String,
    pub operation_type: String,
    pub start_event_sequence: u64,
    pub end_event_sequence: u64,
    pub start_pending_uploads: u64,
    pub end_pending_uploads: u64,
    pub delta_pending_uploads: i64,
    #[serde(default)]
    pub supporting_evidence: Vec<DiagnosticsEvidenceReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiagnosticsBlockingInterval {
    pub subsystem: String,
    pub operation_type: String,
    pub total_blocked_ms: f64,
    pub max_blocked_ms: f64,
    #[serde(default)]
    pub supporting_evidence: Vec<DiagnosticsEvidenceReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiagnosticsHotspotSummary {
    pub subsystem: String,
    pub operation_type: String,
    pub metric: String,
    pub score: f64,
    #[serde(default)]
    pub supporting_evidence: Vec<DiagnosticsEvidenceReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct DiagnosticsDerivedEvidenceView {
    #[serde(default)]
    pub queue_growth: Vec<DiagnosticsQueueGrowthEpisode>,
    #[serde(default)]
    pub blocking_intervals: Vec<DiagnosticsBlockingInterval>,
    #[serde(default)]
    pub hotspots: Vec<DiagnosticsHotspotSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiagnosticsEvidenceView {
    pub generated_unix_ms: u64,
    pub run_id: String,
    pub scope: DiagnosticsEvidenceScopeResolution,
    pub budget: DiagnosticsEvidenceBudgetReport,
    pub raw: DiagnosticsEvidenceRawView,
    pub derived: DiagnosticsDerivedEvidenceView,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsCompareRunSpanRequest {
    pub baseline_run_id: String,
    pub candidate_run_id: String,
    pub start_marker_title: String,
    pub end_marker_title: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SafeLogVerbosity {
    Info,
    Debug,
    Trace,
}

impl SafeLogVerbosity {
    pub(crate) fn as_directive(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SafeDebugActionKind {
    SetLogVerbosity,
    CaptureBundle,
    ResetObservabilityCounters,
    LightweightSelfCheck,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SafeDebugActionStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(crate) enum SafeDebugActionRequest {
    SetLogVerbosity {
        level: SafeLogVerbosity,
    },
    CaptureBundle {
        actor: Option<String>,
        reason: Option<String>,
        recent_events_limit: Option<usize>,
    },
    ResetObservabilityCounters {
        actor: Option<String>,
        reason: Option<String>,
    },
    LightweightSelfCheck {
        actor: Option<String>,
        reason: Option<String>,
    },
}

impl SafeDebugActionRequest {
    pub(super) fn actor(&self) -> Option<&str> {
        match self {
            Self::SetLogVerbosity { .. } => None,
            Self::CaptureBundle { actor, .. }
            | Self::ResetObservabilityCounters { actor, .. }
            | Self::LightweightSelfCheck { actor, .. } => actor.as_deref(),
        }
    }

    pub(super) fn reason(&self) -> Option<&str> {
        match self {
            Self::SetLogVerbosity { .. } => None,
            Self::CaptureBundle { reason, .. }
            | Self::ResetObservabilityCounters { reason, .. }
            | Self::LightweightSelfCheck { reason, .. } => reason.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SafeDebugActionOutcome {
    pub action: SafeDebugActionKind,
    pub loud: bool,
    pub status: SafeDebugActionStatus,
    #[serde(default)]
    pub summary: serde_json::Value,
    pub artifact: Option<ProfileArtifactRecord>,
    pub error: Option<String>,
    pub audit_error: Option<String>,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub(crate) enum DiagnosticsActionError {
    #[error("observability counter reset is unavailable for this session")]
    ResetUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct SafeActionAuditBody {
    pub(super) status: SafeDebugActionStatus,
    pub(super) summary: serde_json::Value,
    pub(super) artifact: Option<ProfileArtifactRecord>,
    pub(super) error: Option<String>,
    pub(super) audit_error: Option<String>,
}

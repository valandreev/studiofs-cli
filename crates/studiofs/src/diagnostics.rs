use crate::cli::utils;
use crate::investigation::{
    BuildIdentity, DiagnosticsPreset, DiagnosticsSettingsFingerprint, EnvironmentFingerprint,
    InvestigationCase, InvestigationComparisonAnomalyDelta, InvestigationComparisonHotspotDelta,
    InvestigationComparisonMetricDelta, InvestigationHypothesis, InvestigationHypothesisStatus,
    InvestigationMatchedMarkerSpan, InvestigationRecommendedNextAction, InvestigationReport,
    InvestigationReportEvidenceSummary, InvestigationReportFinding, InvestigationReportReferences,
    InvestigationReportSectionSummary, InvestigationReportSelectedEvidence,
    InvestigationReportTruncationSummary, InvestigationRun, InvestigationSpanComparison,
    InvestigationSpanComparisonSummary, InvestigationStore, InvestigationWorkflowArtifactRecord,
    InvestigationWorkflowFailurePolicy, InvestigationWorkflowPreset,
    InvestigationWorkflowPresetStep, InvestigationWorkflowRun, InvestigationWorkflowStatus,
    InvestigationWorkflowStepKind, InvestigationWorkflowStepStatus,
    InvestigationWorkflowToolFamily, InvestigationWorkflowToolInvocationRecord,
    InvestigationWorkflowToolRef, MachineFingerprint, NewInvestigationCase,
    NewInvestigationHypothesis, NewInvestigationMarkerSnapshot, NewInvestigationReport,
    NewInvestigationRun, NewInvestigationSpanComparison, NewInvestigationWorkflowRun,
    NewProfilingWindow, NewTimelineEntry, PlatformFingerprint, ProfileArtifactRecord,
    ProfilingCaptureMode, ProfilingCostEstimate, ProfilingWindowRecord, ProfilingWindowStatus,
    RuntimeConfigurationFingerprint, TimelineEntry, TimelineEntryKind, TimelineEntrySource,
    UpdateInvestigationHypothesisStatus, WorkloadIdentityFingerprint,
};
use anyhow::{Context, Result, anyhow};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use studiofs_meta::FormatInfo;
use studiofs_vfs::vfs::VfsFileSessionStats;
use studiofs_vfs::{
    StudioVfs, VFS_RUNTIME_EVENT_CAPACITY, VfsCacheStateSnapshot, VfsForegroundReadProtectionStats,
    VfsIngressStats, VfsRuntimeEvent, VfsSynchronizationStateSnapshot, VfsUploadStats,
    VfsWritePathObservabilitySnapshot, VfsZeroIoRuntimeSnapshot,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tracing::warn;
use uuid::Uuid;

mod session_service;

#[cfg(test)]
use session_service::DiagnosticsServiceError;
use session_service::{DiagnosticsSessionDeps, DiagnosticsSessionService, dispatch_protocol};

#[cfg(windows)]
use std::{ffi::c_void, mem::size_of};
#[cfg(windows)]
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
#[cfg(windows)]
use windows::{
    Win32::{
        Foundation::{HLOCAL, LocalFree},
        Security::Authorization::{
            ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
        },
        Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES},
    },
    core::w,
};

pub(crate) const DIAGNOSTICS_RUNTIME_SURFACE_PROTOCOL_VERSION: u32 = 3;
pub(crate) const DIAGNOSTICS_SAFE_ACTIONS_PROTOCOL_VERSION: u32 = 4;
pub(crate) const DIAGNOSTICS_WORKFLOW_PROTOCOL_VERSION: u32 = 5;
pub(crate) const DIAGNOSTICS_PROTOCOL_VERSION: u32 = DIAGNOSTICS_WORKFLOW_PROTOCOL_VERSION;
const DIAGNOSTICS_RUNTIME_SNAPSHOT_SCHEMA_VERSION: u32 = 2;
const DEFAULT_LEASE_TTL_MS: u64 = 5_000;
const MAX_LEASE_TTL_MS: u64 = 60_000;
const DEFAULT_RECENT_EVENTS_LIMIT: usize = 32;
pub(crate) const MAX_RECENT_EVENTS_LIMIT: usize = VFS_RUNTIME_EVENT_CAPACITY;
const CONNECT_RETRY_ATTEMPTS: usize = 40;
const CONNECT_RETRY_DELAY_MS: u64 = 25;
const MAX_DIAGNOSTICS_REQUEST_FRAME_BYTES: usize = 64 * 1024;
const MAX_DIAGNOSTICS_RESPONSE_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_DIAGNOSTICS_CONNECTIONS: usize = 16;
const DEFAULT_PROFILE_WINDOW_DURATION_MS: u64 = 5_000;
const DEFAULT_PROFILE_SAMPLE_RATE_HZ: u32 = 100;
const MIN_PROFILE_SAMPLE_RATE_HZ: u32 = 1;
const MAX_PROFILE_SAMPLE_RATE_HZ: u32 = 1_000;
pub(crate) const MAX_PROFILE_WINDOW_DURATION_MS: u64 = 30_000;
const DEFAULT_EVIDENCE_RECENT_EVENTS_LIMIT: usize = 16;
pub(crate) const MAX_EVIDENCE_RECENT_EVENTS_LIMIT: usize = 64;
const DEFAULT_EVIDENCE_TIMELINE_ENTRIES_LIMIT: usize = 16;
const MAX_EVIDENCE_TIMELINE_ENTRIES_LIMIT: usize = 64;
const DEFAULT_EVIDENCE_PROFILING_WINDOWS_LIMIT: usize = 4;
const MAX_EVIDENCE_PROFILING_WINDOWS_LIMIT: usize = 16;
const DEFAULT_EVIDENCE_DERIVED_ITEMS_PER_KIND: usize = 4;
const MAX_EVIDENCE_DERIVED_ITEMS_PER_KIND: usize = 16;
const MAX_REPORT_ANOMALIES: usize = 6;
const MAX_REPORT_CANDIDATE_BOTTLENECKS: usize = 6;
const MAX_REPORT_SELECTED_EVIDENCE: usize = 4;
const MAX_REPORT_RECOMMENDED_NEXT_ACTIONS: usize = 3;
const SUBSYSTEM_UPLOAD_PIPELINE: &str = "upload_pipeline";
const SUBSYSTEM_WRITE_PATH: &str = "write_path";
const SUBSYSTEM_SYNCHRONIZATION: &str = "synchronization";
const SUBSYSTEM_CACHE: &str = "cache";
const OPERATION_QUEUE_GROWTH: &str = "queue_growth";
const OPERATION_SYNC_DATA_QUEUE_WAIT: &str = "sync_data_queue_wait";
const OPERATION_WRITE_BACKPRESSURE: &str = "write_backpressure";
const OPERATION_UPLOAD_FALLBACK_WAIT: &str = "upload_fallback_wait";
const OPERATION_PREFETCH_QUEUE: &str = "prefetch_queue";
const OPERATION_METADATA_BACKLOG: &str = "metadata_backlog";
const DIAGNOSTICS_PROFILE_ARTIFACT_DIR_NAME: &str = "profiles";
const DIAGNOSTICS_WORKFLOW_ARTIFACT_DIR_NAME: &str = "workflow-artifacts";
#[cfg(unix)]
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;

#[cfg(test)]
static TEST_START_FAILURE: OnceLock<Mutex<Option<String>>> = OnceLock::new();
#[cfg(test)]
static TEST_SAFE_ACTION_AUDIT_FAILURE: OnceLock<Mutex<Option<String>>> = OnceLock::new();
#[cfg(test)]
static TEST_WORKFLOW_ATTACH_SNAPSHOT_OUTPUT: OnceLock<Mutex<Option<String>>> = OnceLock::new();
#[cfg(test)]
static DIAGNOSTICS_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[cfg(test)]
pub(crate) fn lock_diagnostics_test_mutex() -> parking_lot::MutexGuard<'static, ()> {
    DIAGNOSTICS_TEST_LOCK.get_or_init(|| Mutex::new(())).lock()
}

#[cfg(test)]
fn test_safe_action_audit_failure_message() -> Option<String> {
    TEST_SAFE_ACTION_AUDIT_FAILURE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .clone()
}

#[cfg(test)]
fn test_workflow_attach_snapshot_output() -> Option<String> {
    TEST_WORKFLOW_ATTACH_SNAPSHOT_OUTPUT
        .get_or_init(|| Mutex::new(None))
        .lock()
        .clone()
}

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
pub(crate) struct DiagnosticsRuntimeSnapshot {
    pub schema_version: u32,
    pub generated_unix_ms: u64,
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
    fn from_health(health: DiagnosticsHealthSnapshot) -> Self {
        Self {
            schema_version: DIAGNOSTICS_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
            generated_unix_ms: health.generated_unix_ms,
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
    fn actor(&self) -> Option<&str> {
        match self {
            Self::SetLogVerbosity { .. } => None,
            Self::CaptureBundle { actor, .. }
            | Self::ResetObservabilityCounters { actor, .. }
            | Self::LightweightSelfCheck { actor, .. } => actor.as_deref(),
        }
    }

    fn reason(&self) -> Option<&str> {
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
struct SafeActionAuditBody {
    status: SafeDebugActionStatus,
    summary: serde_json::Value,
    artifact: Option<ProfileArtifactRecord>,
    error: Option<String>,
    audit_error: Option<String>,
}

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

struct ActiveProfilingWindow {
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

enum ProfilingWindowState {
    Idle,
    Starting,
    Active(ActiveProfilingWindow),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProfilingBudgetValidationError {
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
enum ProfilingWindowStartError {
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

struct CompositeRuntimeProfilingBackend {
    wall_blocking_backend: Arc<dyn RuntimeProfilingBackend>,
    cpu_backend: Arc<dyn RuntimeProfilingBackend>,
}

impl CompositeRuntimeProfilingBackend {
    fn new(
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

struct WallBlockingRuntimeProfilingBackend {
    source: Arc<dyn DiagnosticsDataSource>,
}

impl WallBlockingRuntimeProfilingBackend {
    fn new(source: Arc<dyn DiagnosticsDataSource>) -> Self {
        Self { source }
    }
}

struct WallBlockingProfileStopper {
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
    fn new(backend: Arc<dyn RuntimeProfilingBackend>) -> Self {
        Self {
            backend,
            active_window: Mutex::new(ProfilingWindowState::Idle),
        }
    }

    fn start_window(
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

    fn stop_window(
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

    fn status(&self) -> DiagnosticsProfilingStatus {
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

fn validate_profiling_request(
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

fn estimate_profiling_cost(request: &StartProfilingWindowRequest) -> ProfilingCostEstimate {
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
struct WallBlockingProfileArtifact {
    start_snapshot: DiagnosticsRuntimeSnapshot,
    end_snapshot: DiagnosticsRuntimeSnapshot,
    #[serde(default)]
    start_events: Vec<DiagnosticsRecentEvent>,
    #[serde(default)]
    end_events: Vec<DiagnosticsRecentEvent>,
}

fn clamp_evidence_budget(requested: &DiagnosticsEvidenceBudget) -> DiagnosticsEvidenceBudget {
    fn clamp_limit(requested: usize, default: usize, max: usize) -> usize {
        if requested == 0 {
            default
        } else {
            requested.clamp(1, max)
        }
    }

    DiagnosticsEvidenceBudget {
        max_recent_events: clamp_limit(
            requested.max_recent_events,
            DEFAULT_EVIDENCE_RECENT_EVENTS_LIMIT,
            MAX_EVIDENCE_RECENT_EVENTS_LIMIT,
        ),
        max_timeline_entries: clamp_limit(
            requested.max_timeline_entries,
            DEFAULT_EVIDENCE_TIMELINE_ENTRIES_LIMIT,
            MAX_EVIDENCE_TIMELINE_ENTRIES_LIMIT,
        ),
        max_profiling_windows: clamp_limit(
            requested.max_profiling_windows,
            DEFAULT_EVIDENCE_PROFILING_WINDOWS_LIMIT,
            MAX_EVIDENCE_PROFILING_WINDOWS_LIMIT,
        ),
        max_derived_items_per_kind: clamp_limit(
            requested.max_derived_items_per_kind,
            DEFAULT_EVIDENCE_DERIVED_ITEMS_PER_KIND,
            MAX_EVIDENCE_DERIVED_ITEMS_PER_KIND,
        ),
    }
}

fn canonical_subsystem(value: &str) -> &str {
    match value {
        "upload" | "upload_pipeline" => "upload",
        other => other,
    }
}

fn subsystem_matches(filter: Option<&str>, candidate: &str) -> bool {
    filter.is_none_or(|filter| canonical_subsystem(filter) == canonical_subsystem(candidate))
}

fn operation_matches(filter: Option<&str>, candidate: &str) -> bool {
    filter.is_none_or(|filter| filter == candidate)
}

fn json_contains_u64(value: &serde_json::Value, keys: &[&str], expected: u64) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(key, value)| {
            (keys
                .iter()
                .any(|candidate| key.eq_ignore_ascii_case(candidate))
                && value.as_u64() == Some(expected))
                || json_contains_u64(value, keys, expected)
        }),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_contains_u64(value, keys, expected)),
        _ => false,
    }
}

fn json_contains_string(value: &serde_json::Value, keys: &[&str], expected: &str) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(key, value)| {
            (keys
                .iter()
                .any(|candidate| key.eq_ignore_ascii_case(candidate))
                && value.as_str() == Some(expected))
                || json_contains_string(value, keys, expected)
        }),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, keys, expected)),
        _ => false,
    }
}

fn object_identity_matches_fields(
    fields: &serde_json::Value,
    identity: &DiagnosticsObjectIdentity,
) -> bool {
    if let Some(inode) = identity.inode
        && !json_contains_u64(fields, &["ino", "inode"], inode)
    {
        return false;
    }
    if let Some(path) = identity.path.as_deref()
        && !json_contains_string(fields, &["path"], path)
    {
        return false;
    }
    if let Some(file_handle) = identity.file_handle.as_deref()
        && !json_contains_string(fields, &["file_handle", "fh", "handle_id"], file_handle)
    {
        return false;
    }
    true
}

fn event_matches_scope(
    event: &DiagnosticsRecentEvent,
    scope: &DiagnosticsEvidenceScope,
    marker_span: Option<&DiagnosticsResolvedMarkerSpan>,
) -> bool {
    if !subsystem_matches(scope.subsystem.as_deref(), &event.subsystem) {
        return false;
    }
    if !operation_matches(scope.operation_type.as_deref(), &event.kind) {
        return false;
    }
    if let Some(identity) = scope.object_identity.as_ref()
        && !object_identity_matches_fields(&event.fields, identity)
    {
        return false;
    }
    if let Some(marker_span) = marker_span
        && (event.generated_unix_ms < marker_span.start_unix_ms
            || event.generated_unix_ms > marker_span.end_unix_ms)
    {
        return false;
    }
    true
}

fn artifact_matches_scope(
    artifact: &WallBlockingProfileArtifact,
    scope: &DiagnosticsEvidenceScope,
) -> bool {
    match scope.object_identity.as_ref() {
        Some(identity) => artifact
            .start_events
            .iter()
            .chain(artifact.end_events.iter())
            .any(|event| object_identity_matches_fields(&event.fields, identity)),
        None => true,
    }
}

fn snapshot_sections_for_scope(
    snapshot: &DiagnosticsRuntimeSnapshot,
    scope: &DiagnosticsEvidenceScope,
    marker_span: Option<&DiagnosticsResolvedMarkerSpan>,
) -> serde_json::Map<String, serde_json::Value> {
    if marker_span.is_some() {
        return serde_json::Map::new();
    }

    let mut sections = serde_json::Map::new();
    let filter = scope.subsystem.as_deref();
    if subsystem_matches(filter, "ingress") {
        sections.insert("ingress".to_string(), serde_json::json!(snapshot.ingress));
    }
    if subsystem_matches(filter, "write_path") {
        sections.insert(
            "write_path".to_string(),
            serde_json::json!(snapshot.write_path),
        );
    }
    if subsystem_matches(filter, "upload_pipeline") {
        sections.insert("upload".to_string(), serde_json::json!(snapshot.upload));
    }
    if subsystem_matches(filter, "zeroio") {
        sections.insert("zeroio".to_string(), serde_json::json!(snapshot.zeroio));
    }
    if subsystem_matches(filter, "cache") {
        sections.insert("cache".to_string(), serde_json::json!(snapshot.cache));
    }
    if subsystem_matches(filter, "synchronization") {
        sections.insert(
            "synchronization".to_string(),
            serde_json::json!(snapshot.synchronization),
        );
    }
    sections
}

fn resolve_marker_span(
    run: &InvestigationRun,
    span: &DiagnosticsMarkerSpan,
) -> Result<DiagnosticsResolvedMarkerSpan> {
    let start = run
        .timeline
        .iter()
        .find(|entry| entry.sequence == span.start_sequence)
        .ok_or_else(|| anyhow!("marker {} not found", span.start_sequence))?;
    if start.kind != TimelineEntryKind::Marker {
        return Err(anyhow!("timeline entry {} is not a marker", start.sequence));
    }
    let end_sequence = span.end_sequence.unwrap_or(span.start_sequence);
    let end = run
        .timeline
        .iter()
        .find(|entry| entry.sequence == end_sequence)
        .ok_or_else(|| anyhow!("marker {} not found", end_sequence))?;
    if end.kind != TimelineEntryKind::Marker {
        return Err(anyhow!("timeline entry {} is not a marker", end.sequence));
    }
    if end.sequence < start.sequence {
        return Err(anyhow!(
            "marker span end {} precedes start {}",
            end.sequence,
            start.sequence
        ));
    }
    Ok(DiagnosticsResolvedMarkerSpan {
        start_sequence: start.sequence,
        end_sequence: end.sequence,
        start_unix_ms: start.occurred_unix_ms,
        end_unix_ms: end.occurred_unix_ms,
    })
}

fn load_wall_blocking_profile_artifact(
    window: &ProfilingWindowRecord,
) -> Result<Option<(serde_json::Value, WallBlockingProfileArtifact)>> {
    let Some(artifact) = window.artifact.as_ref() else {
        return Ok(None);
    };
    if artifact.kind != "wall_blocking_summary" {
        return Ok(None);
    }
    let bytes = fs::read(&artifact.path)
        .with_context(|| format!("read profiling artifact '{}'", artifact.path))?;
    let summary = serde_json::from_slice::<serde_json::Value>(&bytes)
        .with_context(|| format!("parse profiling artifact '{}'", artifact.path))?;
    let typed = serde_json::from_value::<WallBlockingProfileArtifact>(summary.clone())
        .with_context(|| format!("decode profiling artifact '{}'", artifact.path))?;
    Ok(Some((summary, typed)))
}

fn truncate_to_limit<T>(mut items: Vec<T>, limit: usize) -> (Vec<T>, bool) {
    let truncated = items.len() > limit;
    items.truncate(limit);
    (items, truncated)
}

fn queue_growth_episode(
    start_event: &DiagnosticsRecentEvent,
    start_pending_uploads: u64,
    end_event: &DiagnosticsRecentEvent,
    end_pending_uploads: u64,
) -> DiagnosticsQueueGrowthEpisode {
    DiagnosticsQueueGrowthEpisode {
        subsystem: SUBSYSTEM_UPLOAD_PIPELINE.to_string(),
        operation_type: OPERATION_QUEUE_GROWTH.to_string(),
        start_event_sequence: start_event.sequence,
        end_event_sequence: end_event.sequence,
        start_pending_uploads,
        end_pending_uploads,
        delta_pending_uploads: end_pending_uploads as i64 - start_pending_uploads as i64,
        supporting_evidence: vec![
            DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::RecentEvent,
                id: start_event.sequence.to_string(),
                field: Some("fields.pending_uploads".to_string()),
            },
            DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::RecentEvent,
                id: end_event.sequence.to_string(),
                field: Some("fields.pending_uploads".to_string()),
            },
        ],
    }
}

fn derive_queue_growth_episodes(
    scope: &DiagnosticsEvidenceScope,
    events: &[DiagnosticsRecentEvent],
    limit: usize,
) -> (Vec<DiagnosticsQueueGrowthEpisode>, bool) {
    if !subsystem_matches(scope.subsystem.as_deref(), SUBSYSTEM_UPLOAD_PIPELINE)
        || !operation_matches(scope.operation_type.as_deref(), OPERATION_QUEUE_GROWTH)
    {
        return (Vec::new(), false);
    }

    let mut episodes = Vec::new();
    let mut current: Option<(&DiagnosticsRecentEvent, u64, &DiagnosticsRecentEvent, u64)> = None;

    for event in events {
        let Some(pending_uploads) = json_pending_uploads(&event.fields) else {
            current = None;
            continue;
        };

        match current {
            Some((start_event, start_pending, _, last_pending))
                if pending_uploads > last_pending =>
            {
                current = Some((start_event, start_pending, event, pending_uploads));
            }
            Some((start_event, start_pending, end_event, end_pending))
                if end_pending > start_pending =>
            {
                episodes.push(queue_growth_episode(
                    start_event,
                    start_pending,
                    end_event,
                    end_pending,
                ));
                current = Some((event, pending_uploads, event, pending_uploads));
            }
            _ => {
                current = Some((event, pending_uploads, event, pending_uploads));
            }
        }
    }

    if let Some((start_event, start_pending, end_event, end_pending)) = current
        && end_pending > start_pending
    {
        episodes.push(queue_growth_episode(
            start_event,
            start_pending,
            end_event,
            end_pending,
        ));
    }

    truncate_to_limit(episodes, limit)
}

fn json_pending_uploads(fields: &serde_json::Value) -> Option<u64> {
    match fields {
        serde_json::Value::Object(map) => {
            if let Some(value) = map
                .get("pending_uploads")
                .and_then(serde_json::Value::as_u64)
            {
                return Some(value);
            }
            map.values().find_map(json_pending_uploads)
        }
        serde_json::Value::Array(values) => values.iter().find_map(json_pending_uploads),
        _ => None,
    }
}

fn profile_window_reference(window: &ProfilingWindowRecord) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::ProfilingWindow,
        id: window.window_id.clone(),
        field: None,
    }
}

fn profile_artifact_reference(path: &str, field: &str) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::ProfilingArtifact,
        id: path.to_string(),
        field: Some(field.to_string()),
    }
}

fn runtime_snapshot_reference(field: &str) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::RuntimeSnapshot,
        id: "runtime_snapshot".to_string(),
        field: Some(field.to_string()),
    }
}

fn marker_snapshot_reference(sequence: u64, field: &str) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::MarkerSnapshot,
        id: sequence.to_string(),
        field: Some(field.to_string()),
    }
}

fn qualify_comparison_evidence(
    run_id: &str,
    reference: DiagnosticsEvidenceReference,
) -> DiagnosticsEvidenceReference {
    let needs_run_scope = matches!(
        reference.source,
        DiagnosticsEvidenceReferenceSource::RuntimeSnapshot
            | DiagnosticsEvidenceReferenceSource::RecentEvent
            | DiagnosticsEvidenceReferenceSource::TimelineEntry
            | DiagnosticsEvidenceReferenceSource::ProfilingWindow
            | DiagnosticsEvidenceReferenceSource::ProfilingArtifact
            | DiagnosticsEvidenceReferenceSource::MarkerSnapshot
    );
    if !needs_run_scope {
        return reference;
    }

    DiagnosticsEvidenceReference {
        id: format!("{run_id}:{}", reference.id),
        ..reference
    }
}

fn push_blocking_interval(
    output: &mut Vec<DiagnosticsBlockingInterval>,
    scope: &DiagnosticsEvidenceScope,
    subsystem: &str,
    operation_type: &str,
    total_blocked_ms: f64,
    max_blocked_ms: f64,
    supporting_evidence: Vec<DiagnosticsEvidenceReference>,
) {
    if !total_blocked_ms.is_finite()
        || !max_blocked_ms.is_finite()
        || total_blocked_ms <= 0.0
        || !subsystem_matches(scope.subsystem.as_deref(), subsystem)
        || !operation_matches(scope.operation_type.as_deref(), operation_type)
    {
        return;
    }

    output.push(DiagnosticsBlockingInterval {
        subsystem: subsystem.to_string(),
        operation_type: operation_type.to_string(),
        total_blocked_ms,
        max_blocked_ms,
        supporting_evidence,
    });
}

fn push_hotspot(
    output: &mut Vec<DiagnosticsHotspotSummary>,
    scope: &DiagnosticsEvidenceScope,
    subsystem: &str,
    operation_type: &str,
    metric: &str,
    score: f64,
    supporting_evidence: Vec<DiagnosticsEvidenceReference>,
) {
    if !score.is_finite()
        || score <= 0.0
        || !subsystem_matches(scope.subsystem.as_deref(), subsystem)
        || !operation_matches(scope.operation_type.as_deref(), operation_type)
    {
        return;
    }

    output.push(DiagnosticsHotspotSummary {
        subsystem: subsystem.to_string(),
        operation_type: operation_type.to_string(),
        metric: metric.to_string(),
        score,
        supporting_evidence,
    });
}

fn derive_blocking_intervals(
    scope: &DiagnosticsEvidenceScope,
    snapshot: Option<&DiagnosticsRuntimeSnapshot>,
    profiling_windows: &[(ProfilingWindowRecord, Option<WallBlockingProfileArtifact>)],
    limit: usize,
) -> (Vec<DiagnosticsBlockingInterval>, bool) {
    let mut intervals = Vec::new();
    if scope.object_identity.is_none() {
        if let Some(snapshot) = snapshot {
            push_blocking_interval(
                &mut intervals,
                scope,
                SUBSYSTEM_WRITE_PATH,
                OPERATION_SYNC_DATA_QUEUE_WAIT,
                snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
                snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_max_ms,
                vec![runtime_snapshot_reference(
                    "write_path.payload_store_sync_data.queue_wait_total_ms",
                )],
            );
            push_blocking_interval(
                &mut intervals,
                scope,
                SUBSYSTEM_WRITE_PATH,
                OPERATION_WRITE_BACKPRESSURE,
                snapshot.write_path.write_backpressure.wait_total_ms as f64,
                snapshot.write_path.write_backpressure.stall_max_ms as f64,
                vec![runtime_snapshot_reference(
                    "write_path.write_backpressure.wait_total_ms",
                )],
            );
            push_blocking_interval(
                &mut intervals,
                scope,
                SUBSYSTEM_SYNCHRONIZATION,
                OPERATION_UPLOAD_FALLBACK_WAIT,
                snapshot
                    .synchronization
                    .state
                    .foreground_reads
                    .upload_fallback_wait_ns as f64
                    / 1_000_000.0,
                snapshot
                    .synchronization
                    .state
                    .foreground_reads
                    .upload_fallback_wait_max_ns as f64
                    / 1_000_000.0,
                vec![runtime_snapshot_reference(
                    "synchronization.state.foreground_reads.upload_fallback_wait_ns",
                )],
            );
        }
    }

    for (window, artifact) in profiling_windows {
        let Some(artifact) = artifact else {
            continue;
        };
        if !artifact_matches_scope(artifact, scope) {
            continue;
        }
        let artifact_path = window
            .artifact
            .as_ref()
            .map(|artifact| artifact.path.as_str())
            .unwrap_or_default();
        let queue_wait_delta = artifact
            .end_snapshot
            .write_path
            .payload_store_sync_data
            .queue_wait_total_ms
            - artifact
                .start_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_total_ms;
        push_blocking_interval(
            &mut intervals,
            scope,
            SUBSYSTEM_WRITE_PATH,
            OPERATION_SYNC_DATA_QUEUE_WAIT,
            queue_wait_delta,
            artifact
                .end_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_max_ms,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.write_path.payload_store_sync_data.queue_wait_total_ms",
                ),
            ],
        );
        let backpressure_delta = artifact
            .end_snapshot
            .write_path
            .write_backpressure
            .wait_total_ms as f64
            - artifact
                .start_snapshot
                .write_path
                .write_backpressure
                .wait_total_ms as f64;
        push_blocking_interval(
            &mut intervals,
            scope,
            SUBSYSTEM_WRITE_PATH,
            OPERATION_WRITE_BACKPRESSURE,
            backpressure_delta,
            artifact
                .end_snapshot
                .write_path
                .write_backpressure
                .stall_max_ms as f64,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.write_path.write_backpressure.wait_total_ms",
                ),
            ],
        );
        let fallback_delta = artifact
            .end_snapshot
            .synchronization
            .state
            .foreground_reads
            .upload_fallback_wait_ns as f64
            / 1_000_000.0
            - artifact
                .start_snapshot
                .synchronization
                .state
                .foreground_reads
                .upload_fallback_wait_ns as f64
                / 1_000_000.0;
        push_blocking_interval(
            &mut intervals,
            scope,
            SUBSYSTEM_SYNCHRONIZATION,
            OPERATION_UPLOAD_FALLBACK_WAIT,
            fallback_delta,
            artifact
                .end_snapshot
                .synchronization
                .state
                .foreground_reads
                .upload_fallback_wait_max_ns as f64
                / 1_000_000.0,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.synchronization.state.foreground_reads.upload_fallback_wait_ns",
                ),
            ],
        );
    }

    truncate_to_limit(intervals, limit)
}

fn derive_hotspots(
    scope: &DiagnosticsEvidenceScope,
    snapshot: Option<&DiagnosticsRuntimeSnapshot>,
    profiling_windows: &[(ProfilingWindowRecord, Option<WallBlockingProfileArtifact>)],
    limit: usize,
) -> (Vec<DiagnosticsHotspotSummary>, bool) {
    let mut hotspots = Vec::new();
    if scope.object_identity.is_none() {
        if let Some(snapshot) = snapshot {
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_UPLOAD_PIPELINE,
                OPERATION_QUEUE_GROWTH,
                "pending_uploads",
                snapshot.upload.pending_uploads as f64,
                vec![runtime_snapshot_reference("upload.pending_uploads")],
            );
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_WRITE_PATH,
                OPERATION_SYNC_DATA_QUEUE_WAIT,
                "payload_store_sync_data.queue_wait_total_ms",
                snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
                vec![runtime_snapshot_reference(
                    "write_path.payload_store_sync_data.queue_wait_total_ms",
                )],
            );
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_WRITE_PATH,
                OPERATION_WRITE_BACKPRESSURE,
                "write_backpressure.wait_total_ms",
                snapshot.write_path.write_backpressure.wait_total_ms as f64,
                vec![runtime_snapshot_reference(
                    "write_path.write_backpressure.wait_total_ms",
                )],
            );
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_CACHE,
                OPERATION_PREFETCH_QUEUE,
                "prefetch_queue_depth",
                snapshot.cache.state.prefetch_queue_depth as f64,
                vec![runtime_snapshot_reference(
                    "cache.state.prefetch_queue_depth",
                )],
            );
            push_hotspot(
                &mut hotspots,
                scope,
                SUBSYSTEM_SYNCHRONIZATION,
                OPERATION_METADATA_BACKLOG,
                "pending_metadata_ops",
                snapshot.synchronization.state.pending_metadata_ops as f64,
                vec![runtime_snapshot_reference(
                    "synchronization.state.pending_metadata_ops",
                )],
            );
        }
    }

    for (window, artifact) in profiling_windows {
        let Some(artifact) = artifact else {
            continue;
        };
        if !artifact_matches_scope(artifact, scope) {
            continue;
        }
        let artifact_path = window
            .artifact
            .as_ref()
            .map(|artifact| artifact.path.as_str())
            .unwrap_or_default();
        push_hotspot(
            &mut hotspots,
            scope,
            SUBSYSTEM_WRITE_PATH,
            OPERATION_SYNC_DATA_QUEUE_WAIT,
            "payload_store_sync_data.queue_wait_total_ms_delta",
            artifact
                .end_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_total_ms
                - artifact
                    .start_snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.write_path.payload_store_sync_data.queue_wait_total_ms",
                ),
            ],
        );
        push_hotspot(
            &mut hotspots,
            scope,
            SUBSYSTEM_WRITE_PATH,
            OPERATION_WRITE_BACKPRESSURE,
            "write_backpressure.wait_total_ms_delta",
            artifact
                .end_snapshot
                .write_path
                .write_backpressure
                .wait_total_ms as f64
                - artifact
                    .start_snapshot
                    .write_path
                    .write_backpressure
                    .wait_total_ms as f64,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(
                    artifact_path,
                    "end_snapshot.write_path.write_backpressure.wait_total_ms",
                ),
            ],
        );
        push_hotspot(
            &mut hotspots,
            scope,
            SUBSYSTEM_UPLOAD_PIPELINE,
            OPERATION_QUEUE_GROWTH,
            "pending_uploads_delta",
            artifact.end_snapshot.upload.pending_uploads as f64
                - artifact.start_snapshot.upload.pending_uploads as f64,
            vec![
                profile_window_reference(window),
                profile_artifact_reference(artifact_path, "end_snapshot.upload.pending_uploads"),
            ],
        );
    }

    hotspots.sort_by(|left, right| right.score.total_cmp(&left.score));
    truncate_to_limit(hotspots, limit)
}

fn build_evidence_view(
    run_id: &str,
    request: DiagnosticsEvidenceRequest,
    source: &dyn DiagnosticsDataSource,
    store: &InvestigationStore,
) -> Result<DiagnosticsEvidenceView> {
    build_evidence_view_with_snapshot(run_id, request, source.runtime_snapshot(), source, store)
}

fn build_evidence_view_with_snapshot(
    run_id: &str,
    request: DiagnosticsEvidenceRequest,
    snapshot: DiagnosticsRuntimeSnapshot,
    source: &dyn DiagnosticsDataSource,
    store: &InvestigationStore,
) -> Result<DiagnosticsEvidenceView> {
    let run = store.load_run(run_id)?;
    let applied_budget = clamp_evidence_budget(&request.budget);
    let resolved_marker_span = request
        .scope
        .marker_span
        .as_ref()
        .map(|span| resolve_marker_span(&run, span))
        .transpose()?;
    let scope_resolution = DiagnosticsEvidenceScopeResolution {
        subsystem: request.scope.subsystem.clone(),
        operation_type: request.scope.operation_type.clone(),
        object_identity: request.scope.object_identity.clone(),
        marker_span: resolved_marker_span.clone(),
    };

    let recent_event_candidates = source.recent_events(MAX_EVIDENCE_RECENT_EVENTS_LIMIT);
    let filtered_recent_events = recent_event_candidates
        .into_iter()
        .filter(|event| event_matches_scope(event, &request.scope, resolved_marker_span.as_ref()))
        .collect::<Vec<_>>();
    let (recent_events, recent_events_truncated) =
        truncate_to_limit(filtered_recent_events, applied_budget.max_recent_events);

    let filtered_timeline_entries = run
        .timeline
        .iter()
        .filter(|entry| {
            resolved_marker_span.as_ref().is_none_or(|span| {
                entry.sequence >= span.start_sequence && entry.sequence <= span.end_sequence
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let (timeline_entries, timeline_entries_truncated) = truncate_to_limit(
        filtered_timeline_entries,
        applied_budget.max_timeline_entries,
    );

    let filtered_profiling_windows = run
        .profiling_windows
        .iter()
        .filter(|window| {
            resolved_marker_span.as_ref().is_none_or(|span| {
                window.occurred_unix_ms >= span.start_unix_ms
                    && window.occurred_unix_ms <= span.end_unix_ms
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let profiling_windows_requested_too_large =
        request.budget.max_profiling_windows > applied_budget.max_profiling_windows;
    let derived_items_requested_too_large =
        request.budget.max_derived_items_per_kind > applied_budget.max_derived_items_per_kind;
    let (selected_windows, profiling_windows_truncated_by_count) = truncate_to_limit(
        filtered_profiling_windows,
        applied_budget.max_profiling_windows,
    );
    let mut profiling_windows = Vec::with_capacity(selected_windows.len());
    let mut typed_profiling_windows = Vec::with_capacity(selected_windows.len());
    for window in selected_windows {
        let (artifact, load_error) = match load_wall_blocking_profile_artifact(&window) {
            Ok(artifact) => (artifact, None),
            Err(error) => {
                warn!(
                    window_id = %window.window_id,
                    artifact_path = ?window.artifact.as_ref().map(|artifact| artifact.path.as_str()),
                    error = %error,
                    "failed to load wall-blocking profiling artifact for evidence view"
                );
                (None, Some(error.to_string()))
            }
        };
        let summary = artifact.as_ref().map(|(summary, _)| summary.clone());
        let typed = artifact.map(|(_, typed)| typed);
        typed_profiling_windows.push((window.clone(), typed));
        profiling_windows.push(DiagnosticsEvidenceProfilingWindow {
            window,
            summary,
            load_error,
        });
    }

    let snapshot_in_view = resolved_marker_span.is_none();
    let snapshot_sections =
        snapshot_sections_for_scope(&snapshot, &request.scope, resolved_marker_span.as_ref());

    let (queue_growth, queue_growth_truncated) = derive_queue_growth_episodes(
        &request.scope,
        &recent_events,
        applied_budget.max_derived_items_per_kind,
    );
    let (blocking_intervals, blocking_intervals_truncated) = derive_blocking_intervals(
        &request.scope,
        snapshot_in_view.then_some(&snapshot),
        &typed_profiling_windows,
        applied_budget.max_derived_items_per_kind,
    );
    let (hotspots, hotspots_truncated) = derive_hotspots(
        &request.scope,
        snapshot_in_view.then_some(&snapshot),
        &typed_profiling_windows,
        applied_budget.max_derived_items_per_kind,
    );

    Ok(DiagnosticsEvidenceView {
        generated_unix_ms: now_unix_millis(),
        run_id: run_id.to_string(),
        scope: scope_resolution,
        budget: DiagnosticsEvidenceBudgetReport {
            requested: request.budget.clone(),
            applied: applied_budget.clone(),
            truncation: DiagnosticsEvidenceTruncation {
                recent_events: recent_events_truncated
                    || request.budget.max_recent_events > applied_budget.max_recent_events,
                timeline_entries: timeline_entries_truncated
                    || request.budget.max_timeline_entries > applied_budget.max_timeline_entries,
                profiling_windows: profiling_windows_truncated_by_count
                    || profiling_windows_requested_too_large,
                queue_growth: queue_growth_truncated || derived_items_requested_too_large,
                blocking_intervals: blocking_intervals_truncated
                    || derived_items_requested_too_large,
                hotspots: hotspots_truncated || derived_items_requested_too_large,
            },
        },
        raw: DiagnosticsEvidenceRawView {
            snapshot_sections,
            recent_events,
            timeline_entries,
            profiling_windows,
        },
        derived: DiagnosticsDerivedEvidenceView {
            queue_growth,
            blocking_intervals,
            hotspots,
        },
    })
}

fn truncate_report_items<T>(
    items: Vec<T>,
    limit: usize,
) -> (Vec<T>, InvestigationReportSectionSummary) {
    let total_count = items.len();
    let included_count = total_count.min(limit);
    let truncated_count = total_count.saturating_sub(included_count);
    (
        items.into_iter().take(limit).collect(),
        InvestigationReportSectionSummary {
            included_count,
            total_count,
            truncated_count,
        },
    )
}

fn timeline_entry_reference(entry: &TimelineEntry) -> DiagnosticsEvidenceReference {
    DiagnosticsEvidenceReference {
        source: DiagnosticsEvidenceReferenceSource::TimelineEntry,
        id: entry.sequence.to_string(),
        field: None,
    }
}

fn is_legacy_anomaly_reason(reason: Option<&str>) -> bool {
    matches!(reason, Some("annotate anomaly" | "annotate_anomaly"))
}

fn is_anomaly_timeline_entry(entry: &TimelineEntry) -> bool {
    entry.kind == TimelineEntryKind::Anomaly
        || (entry.kind == TimelineEntryKind::Note
            && is_legacy_anomaly_reason(entry.reason.as_deref()))
}

fn build_report_anomaly_candidates(
    run: &InvestigationRun,
    derived_view: &DiagnosticsEvidenceView,
) -> Vec<InvestigationReportFinding> {
    let mut anomalies = derived_view
        .derived
        .queue_growth
        .iter()
        .map(|episode| InvestigationReportFinding {
            title: format!(
                "{} {}",
                episode.subsystem.replace('_', " "),
                episode.operation_type.replace('_', " ")
            ),
            summary: format!(
                "Pending uploads moved from {} to {} across events {}-{}.",
                episode.start_pending_uploads,
                episode.end_pending_uploads,
                episode.start_event_sequence,
                episode.end_event_sequence
            ),
            supporting_evidence: episode.supporting_evidence.clone(),
        })
        .collect::<Vec<_>>();
    anomalies.extend(
        run.timeline
            .iter()
            .filter(|entry| is_anomaly_timeline_entry(entry))
            .map(|entry| InvestigationReportFinding {
                title: entry.title.clone(),
                summary: entry
                    .body
                    .clone()
                    .unwrap_or_else(|| "Operator annotated a runtime anomaly.".to_string()),
                supporting_evidence: vec![timeline_entry_reference(entry)],
            }),
    );
    anomalies.extend(
        derived_view
            .raw
            .profiling_windows
            .iter()
            .filter_map(|window| {
                window
                    .load_error
                    .as_ref()
                    .map(|error| InvestigationReportFinding {
                        title: format!("profiling window {}", window.window.window_id),
                        summary: format!("Profiling artifact could not be reloaded: {error}"),
                        supporting_evidence: vec![DiagnosticsEvidenceReference {
                            source: DiagnosticsEvidenceReferenceSource::ProfilingWindow,
                            id: window.window.window_id.clone(),
                            field: None,
                        }],
                    })
            }),
    );
    anomalies
}

fn build_report_bottleneck_candidates(
    derived_view: &DiagnosticsEvidenceView,
) -> Vec<InvestigationReportFinding> {
    let mut candidate_bottlenecks = derived_view
        .derived
        .blocking_intervals
        .iter()
        .map(|interval| InvestigationReportFinding {
            title: format!(
                "{} {}",
                interval.subsystem.replace('_', " "),
                interval.operation_type.replace('_', " ")
            ),
            summary: format!(
                "Observed {:.1} ms blocked with a {:.1} ms max interval.",
                interval.total_blocked_ms, interval.max_blocked_ms
            ),
            supporting_evidence: interval.supporting_evidence.clone(),
        })
        .collect::<Vec<_>>();
    candidate_bottlenecks.extend(derived_view.derived.hotspots.iter().map(|hotspot| {
        InvestigationReportFinding {
            title: format!(
                "{} {}",
                hotspot.subsystem.replace('_', " "),
                hotspot.operation_type.replace('_', " ")
            ),
            summary: format!(
                "Hotspot metric '{}' scored {:.1}.",
                hotspot.metric, hotspot.score
            ),
            supporting_evidence: hotspot.supporting_evidence.clone(),
        }
    }));
    candidate_bottlenecks
}

fn build_report_selected_evidence_candidates(
    run: &InvestigationRun,
    derived_view: &DiagnosticsEvidenceView,
) -> Vec<InvestigationReportSelectedEvidence> {
    let mut selected_evidence = Vec::new();
    if !derived_view.raw.snapshot_sections.is_empty() {
        let sections = derived_view
            .raw
            .snapshot_sections
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        selected_evidence.push(InvestigationReportSelectedEvidence {
            label: "runtime snapshot".to_string(),
            summary: format!("Captured snapshot sections: {sections}."),
            supporting_evidence: vec![DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::RuntimeSnapshot,
                id: "runtime_snapshot".to_string(),
                field: None,
            }],
        });
    }
    if let Some(event) = derived_view.raw.recent_events.last() {
        selected_evidence.push(InvestigationReportSelectedEvidence {
            label: format!("recent event {}", event.sequence),
            summary: format!("{} / {}: {}", event.subsystem, event.kind, event.title),
            supporting_evidence: vec![DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::RecentEvent,
                id: event.sequence.to_string(),
                field: None,
            }],
        });
    }
    selected_evidence.extend(run.profiling_windows.iter().map(|window| {
        InvestigationReportSelectedEvidence {
            label: format!("profiling window {}", window.window_id),
            summary: format!(
                "{:?} {:?} status {:?}",
                window.capture_mode, window.preset, window.status
            ),
            supporting_evidence: vec![DiagnosticsEvidenceReference {
                source: DiagnosticsEvidenceReferenceSource::ProfilingWindow,
                id: window.window_id.clone(),
                field: None,
            }],
        }
    }));
    selected_evidence
}

fn build_report_next_action_candidates(
    run: &InvestigationRun,
    marker_timeline: &[TimelineEntry],
    derived_view: &DiagnosticsEvidenceView,
) -> Vec<InvestigationRecommendedNextAction> {
    let mut recommended_next_actions = Vec::new();
    if run.profiling_windows.is_empty() {
        recommended_next_actions.push(InvestigationRecommendedNextAction {
            title: "capture a balanced wall-blocking profile".to_string(),
            rationale:
                "The run has no profiling captures yet, so a follow-up capture would strengthen the report."
                    .to_string(),
            supporting_evidence: Vec::new(),
        });
    }
    if let Some(interval) = derived_view
        .derived
        .blocking_intervals
        .iter()
        .max_by(|left, right| left.total_blocked_ms.total_cmp(&right.total_blocked_ms))
    {
        recommended_next_actions.push(InvestigationRecommendedNextAction {
            title: format!(
                "drill into {} {}",
                interval.subsystem.replace('_', " "),
                interval.operation_type.replace('_', " ")
            ),
            rationale: format!(
                "This interval accumulated {:.1} ms of blocked time and is the strongest current bottleneck candidate.",
                interval.total_blocked_ms
            ),
            supporting_evidence: interval.supporting_evidence.clone(),
        });
    }
    if marker_timeline.is_empty() {
        recommended_next_actions.push(InvestigationRecommendedNextAction {
            title: "add workload markers on the next run".to_string(),
            rationale:
                "Marker spans will make the next report easier to compare and easier to scope for focused evidence views."
                    .to_string(),
            supporting_evidence: Vec::new(),
        });
    }
    if recommended_next_actions.is_empty() {
        recommended_next_actions.push(InvestigationRecommendedNextAction {
            title: "review the selected evidence with engineering".to_string(),
            rationale:
                "The report already contains enough concrete evidence to drive a focused follow-up discussion."
                    .to_string(),
            supporting_evidence: Vec::new(),
        });
    }
    recommended_next_actions
}

/// Build the first durable single-run report from persisted run data and the
/// current diagnostics-derived evidence view, keeping the artifact concise while
/// recording when any section had to be truncated for readability.
fn materialize_report(
    run_id: &str,
    source: &dyn DiagnosticsDataSource,
    store: &InvestigationStore,
) -> Result<InvestigationReport> {
    let run = store.load_run(run_id)?;
    let runtime_snapshot = source.runtime_snapshot();
    let derived_view = build_evidence_view_with_snapshot(
        run_id,
        DiagnosticsEvidenceRequest {
            scope: DiagnosticsEvidenceScope::default(),
            budget: DiagnosticsEvidenceBudget::default(),
        },
        runtime_snapshot.clone(),
        source,
        store,
    )?;
    let marker_timeline = run
        .timeline
        .iter()
        .filter(|entry| entry.kind == TimelineEntryKind::Marker)
        .cloned()
        .collect::<Vec<_>>();
    let evidence_summary = InvestigationReportEvidenceSummary {
        recent_event_count: derived_view.raw.recent_events.len(),
        timeline_entry_count: derived_view.raw.timeline_entries.len(),
        profiling_window_count: derived_view.raw.profiling_windows.len(),
        queue_growth_count: derived_view.derived.queue_growth.len(),
        blocking_interval_count: derived_view.derived.blocking_intervals.len(),
        hotspot_count: derived_view.derived.hotspots.len(),
    };
    let (anomalies, anomaly_summary) = truncate_report_items(
        build_report_anomaly_candidates(&run, &derived_view),
        MAX_REPORT_ANOMALIES,
    );
    let (candidate_bottlenecks, candidate_bottleneck_summary) = truncate_report_items(
        build_report_bottleneck_candidates(&derived_view),
        MAX_REPORT_CANDIDATE_BOTTLENECKS,
    );
    let (selected_evidence, selected_evidence_summary) = truncate_report_items(
        build_report_selected_evidence_candidates(&run, &derived_view),
        MAX_REPORT_SELECTED_EVIDENCE,
    );
    let (recommended_next_actions, next_action_summary) = truncate_report_items(
        build_report_next_action_candidates(&run, &marker_timeline, &derived_view),
        MAX_REPORT_RECOMMENDED_NEXT_ACTIONS,
    );

    store.append_report(
        run_id,
        NewInvestigationReport {
            environment: run.fingerprint.clone(),
            marker_timeline,
            evidence_summary,
            truncation: InvestigationReportTruncationSummary {
                anomalies: anomaly_summary,
                candidate_bottlenecks: candidate_bottleneck_summary,
                selected_evidence: selected_evidence_summary,
                recommended_next_actions: next_action_summary,
            },
            anomalies,
            candidate_bottlenecks,
            selected_evidence,
            recommended_next_actions,
            references: InvestigationReportReferences {
                runtime_snapshot,
                derived_view,
                profiling_windows: run.profiling_windows.clone(),
            },
        },
    )
}

fn marker_snapshot_by_title<'a>(
    run: &'a InvestigationRun,
    title: &str,
) -> Result<&'a crate::investigation::InvestigationMarkerSnapshot> {
    let mut matches = run
        .marker_snapshots
        .iter()
        .filter(|snapshot| snapshot.marker_title == title);
    let Some(snapshot) = matches.next() else {
        return Err(anyhow!(
            "marker snapshot '{}' not found for run {}",
            title,
            run.id
        ));
    };
    if matches.next().is_some() {
        return Err(anyhow!(
            "marker snapshot '{}' is ambiguous for run {}",
            title,
            run.id
        ));
    }
    Ok(snapshot)
}

fn count_anomalies_in_span(
    run: &InvestigationRun,
    start_sequence: u64,
    end_sequence: u64,
) -> Vec<(String, DiagnosticsEvidenceReference)> {
    run.timeline
        .iter()
        .filter(|entry| {
            is_anomaly_timeline_entry(entry)
                && entry.sequence >= start_sequence
                && entry.sequence <= end_sequence
        })
        .map(|entry| {
            (
                entry.title.clone(),
                qualify_comparison_evidence(&run.id, timeline_entry_reference(entry)),
            )
        })
        .collect()
}

fn profiling_windows_in_span(
    run: &InvestigationRun,
    start_sequence: u64,
    end_sequence: u64,
) -> Vec<&ProfilingWindowRecord> {
    let Some(start) = run
        .timeline
        .iter()
        .find(|entry| entry.sequence == start_sequence)
    else {
        return Vec::new();
    };
    let Some(end) = run
        .timeline
        .iter()
        .find(|entry| entry.sequence == end_sequence)
    else {
        return Vec::new();
    };
    run.profiling_windows
        .iter()
        .filter(|window| {
            window.occurred_unix_ms >= start.occurred_unix_ms
                && window.occurred_unix_ms <= end.occurred_unix_ms
        })
        .collect()
}

fn collect_hotspots_for_windows(
    windows: &[&ProfilingWindowRecord],
) -> Result<Vec<DiagnosticsHotspotSummary>> {
    let mut loaded = Vec::new();
    for window in windows {
        match load_wall_blocking_profile_artifact(window) {
            Ok(Some((_summary, artifact))) => loaded.push(((*window).clone(), Some(artifact))),
            Ok(None) => {}
            Err(error) => {
                warn!(
                    window_id = %window.window_id,
                    error = %error,
                    "failed to load wall-blocking profile artifact while collecting comparison hotspots"
                );
                return Err(error);
            }
        }
    }
    Ok(derive_hotspots(
        &DiagnosticsEvidenceScope::default(),
        None,
        &loaded,
        MAX_EVIDENCE_DERIVED_ITEMS_PER_KIND,
    )
    .0)
}

fn materialize_run_span_comparison(
    request: DiagnosticsCompareRunSpanRequest,
    store: &InvestigationStore,
) -> Result<InvestigationSpanComparison> {
    let baseline = store.load_run(&request.baseline_run_id)?;
    let candidate = store.load_run(&request.candidate_run_id)?;
    if baseline.case_id != candidate.case_id {
        return Err(anyhow!(
            "runs {} and {} belong to different investigation cases",
            baseline.id,
            candidate.id
        ));
    }
    let baseline_start = marker_snapshot_by_title(&baseline, &request.start_marker_title)?;
    let candidate_start = marker_snapshot_by_title(&candidate, &request.start_marker_title)?;
    let baseline_end = marker_snapshot_by_title(&baseline, &request.end_marker_title)?;
    let candidate_end = marker_snapshot_by_title(&candidate, &request.end_marker_title)?;
    if baseline_end.marker_sequence < baseline_start.marker_sequence {
        return Err(anyhow!("baseline marker span ordering is invalid"));
    }
    if candidate_end.marker_sequence < candidate_start.marker_sequence {
        return Err(anyhow!("candidate marker span ordering is invalid"));
    }

    let metric_deltas = [
        (
            "upload.pending_uploads_delta",
            baseline_end.runtime_snapshot.upload.pending_uploads as f64
                - baseline_start.runtime_snapshot.upload.pending_uploads as f64,
            candidate_end.runtime_snapshot.upload.pending_uploads as f64
                - candidate_start.runtime_snapshot.upload.pending_uploads as f64,
            "upload.pending_uploads",
        ),
        (
            "write_path.payload_store_sync_data.queue_wait_total_ms_delta",
            baseline_end
                .runtime_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_total_ms
                - baseline_start
                    .runtime_snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
            candidate_end
                .runtime_snapshot
                .write_path
                .payload_store_sync_data
                .queue_wait_total_ms
                - candidate_start
                    .runtime_snapshot
                    .write_path
                    .payload_store_sync_data
                    .queue_wait_total_ms,
            "write_path.payload_store_sync_data.queue_wait_total_ms",
        ),
        (
            "write_path.write_backpressure.wait_total_ms_delta",
            baseline_end
                .runtime_snapshot
                .write_path
                .write_backpressure
                .wait_total_ms as f64
                - baseline_start
                    .runtime_snapshot
                    .write_path
                    .write_backpressure
                    .wait_total_ms as f64,
            candidate_end
                .runtime_snapshot
                .write_path
                .write_backpressure
                .wait_total_ms as f64
                - candidate_start
                    .runtime_snapshot
                    .write_path
                    .write_backpressure
                    .wait_total_ms as f64,
            "write_path.write_backpressure.wait_total_ms",
        ),
    ]
    .into_iter()
    .map(
        |(metric, baseline_value, candidate_value, field)| InvestigationComparisonMetricDelta {
            metric: metric.to_string(),
            baseline_value,
            candidate_value,
            delta: candidate_value - baseline_value,
            supporting_evidence: vec![
                qualify_comparison_evidence(
                    &baseline.id,
                    marker_snapshot_reference(baseline_start.marker_sequence, field),
                ),
                qualify_comparison_evidence(
                    &baseline.id,
                    marker_snapshot_reference(baseline_end.marker_sequence, field),
                ),
                qualify_comparison_evidence(
                    &candidate.id,
                    marker_snapshot_reference(candidate_start.marker_sequence, field),
                ),
                qualify_comparison_evidence(
                    &candidate.id,
                    marker_snapshot_reference(candidate_end.marker_sequence, field),
                ),
            ],
        },
    )
    .collect::<Vec<_>>();

    let baseline_anomalies = count_anomalies_in_span(
        &baseline,
        baseline_start.marker_sequence,
        baseline_end.marker_sequence,
    );
    let candidate_anomalies = count_anomalies_in_span(
        &candidate,
        candidate_start.marker_sequence,
        candidate_end.marker_sequence,
    );
    let mut anomaly_titles = baseline_anomalies
        .iter()
        .map(|(title, _)| title.clone())
        .collect::<Vec<_>>();
    anomaly_titles.extend(candidate_anomalies.iter().map(|(title, _)| title.clone()));
    anomaly_titles.sort();
    anomaly_titles.dedup();
    let anomaly_deltas = anomaly_titles
        .into_iter()
        .filter_map(|title| {
            let baseline_refs = baseline_anomalies
                .iter()
                .filter(|(candidate_title, _)| *candidate_title == title)
                .map(|(_, evidence)| evidence.clone())
                .collect::<Vec<_>>();
            let candidate_refs = candidate_anomalies
                .iter()
                .filter(|(candidate_title, _)| *candidate_title == title)
                .map(|(_, evidence)| evidence.clone())
                .collect::<Vec<_>>();
            let baseline_count = baseline_refs.len();
            let candidate_count = candidate_refs.len();
            let delta = candidate_count as i64 - baseline_count as i64;
            (baseline_count > 0 || candidate_count > 0).then_some(
                InvestigationComparisonAnomalyDelta {
                    title,
                    baseline_count,
                    candidate_count,
                    delta,
                    supporting_evidence: baseline_refs.into_iter().chain(candidate_refs).collect(),
                },
            )
        })
        .collect::<Vec<_>>();

    let baseline_hotspots = collect_hotspots_for_windows(&profiling_windows_in_span(
        &baseline,
        baseline_start.marker_sequence,
        baseline_end.marker_sequence,
    ))?;
    let candidate_hotspots = collect_hotspots_for_windows(&profiling_windows_in_span(
        &candidate,
        candidate_start.marker_sequence,
        candidate_end.marker_sequence,
    ))?;
    let mut hotspot_keys = baseline_hotspots
        .iter()
        .map(|hotspot| {
            (
                hotspot.subsystem.clone(),
                hotspot.operation_type.clone(),
                hotspot.metric.clone(),
            )
        })
        .collect::<Vec<_>>();
    hotspot_keys.extend(candidate_hotspots.iter().map(|hotspot| {
        (
            hotspot.subsystem.clone(),
            hotspot.operation_type.clone(),
            hotspot.metric.clone(),
        )
    }));
    hotspot_keys.sort();
    hotspot_keys.dedup();
    let hotspot_deltas = hotspot_keys
        .into_iter()
        .map(|(subsystem, operation_type, metric)| {
            let baseline_match = baseline_hotspots.iter().find(|hotspot| {
                hotspot.subsystem == subsystem
                    && hotspot.operation_type == operation_type
                    && hotspot.metric == metric
            });
            let candidate_match = candidate_hotspots.iter().find(|hotspot| {
                hotspot.subsystem == subsystem
                    && hotspot.operation_type == operation_type
                    && hotspot.metric == metric
            });
            let baseline_score = baseline_match.map(|hotspot| hotspot.score).unwrap_or(0.0);
            let candidate_score = candidate_match.map(|hotspot| hotspot.score).unwrap_or(0.0);
            InvestigationComparisonHotspotDelta {
                subsystem,
                operation_type,
                metric,
                baseline_score,
                candidate_score,
                delta: candidate_score - baseline_score,
                supporting_evidence: baseline_match
                    .into_iter()
                    .flat_map(|hotspot| {
                        hotspot
                            .supporting_evidence
                            .clone()
                            .into_iter()
                            .map(|reference| qualify_comparison_evidence(&baseline.id, reference))
                    })
                    .chain(candidate_match.into_iter().flat_map(|hotspot| {
                        hotspot
                            .supporting_evidence
                            .clone()
                            .into_iter()
                            .map(|reference| qualify_comparison_evidence(&candidate.id, reference))
                    }))
                    .collect(),
            }
        })
        .collect::<Vec<_>>();

    let summary = InvestigationSpanComparisonSummary {
        metric_deltas,
        anomaly_deltas,
        hotspot_deltas,
    };
    let span = InvestigationMatchedMarkerSpan {
        start_marker_title: request.start_marker_title,
        end_marker_title: request.end_marker_title,
        baseline_start_sequence: baseline_start.marker_sequence,
        baseline_end_sequence: baseline_end.marker_sequence,
        candidate_start_sequence: candidate_start.marker_sequence,
        candidate_end_sequence: candidate_end.marker_sequence,
    };
    let supporting_evidence = summary
        .metric_deltas
        .iter()
        .flat_map(|item| item.supporting_evidence.clone())
        .chain(
            summary
                .anomaly_deltas
                .iter()
                .flat_map(|item| item.supporting_evidence.clone()),
        )
        .chain(
            summary
                .hotspot_deltas
                .iter()
                .flat_map(|item| item.supporting_evidence.clone()),
        )
        .collect::<Vec<_>>();

    store.append_span_comparison(
        &baseline.case_id,
        NewInvestigationSpanComparison {
            baseline_run_id: baseline.id,
            candidate_run_id: candidate.id,
            span,
            summary,
            supporting_evidence,
        },
    )
}

fn append_safe_action_audit(
    store: &InvestigationStore,
    run_id: &str,
    action: SafeDebugActionKind,
    actor: Option<&str>,
    reason: Option<&str>,
    outcome: &SafeDebugActionOutcome,
) -> Result<()> {
    #[cfg(test)]
    if let Some(message) = test_safe_action_audit_failure_message() {
        return Err(anyhow!(message));
    }

    let body = SafeActionAuditBody {
        status: outcome.status,
        summary: outcome.summary.clone(),
        artifact: outcome.artifact.clone(),
        error: outcome.error.clone(),
        audit_error: outcome.audit_error.clone(),
    };
    store.append_timeline_entry(
        run_id,
        NewTimelineEntry {
            source: TimelineEntrySource::Mcp,
            kind: TimelineEntryKind::Action,
            title: match action {
                SafeDebugActionKind::SetLogVerbosity => "set_log_verbosity".to_string(),
                SafeDebugActionKind::CaptureBundle => "capture_bundle".to_string(),
                SafeDebugActionKind::ResetObservabilityCounters => {
                    "reset_observability_counters".to_string()
                }
                SafeDebugActionKind::LightweightSelfCheck => "lightweight_self_check".to_string(),
            },
            body: Some(serde_json::to_string(&body)?),
            actor: actor.map(str::to_string),
            reason: reason.map(str::to_string),
        },
    )?;
    Ok(())
}

fn capture_bundle_artifact(
    session: &DiagnosticsSessionInfo,
    snapshot: &DiagnosticsRuntimeSnapshot,
    recent_events: &[DiagnosticsRecentEvent],
) -> Result<ProfileArtifactRecord> {
    let output_dir = PathBuf::from(&session.cache_path).join("diagnostics-bundles");
    fs::create_dir_all(&output_dir).with_context(|| {
        format!(
            "create diagnostics bundle directory '{}'",
            output_dir.display()
        )
    })?;
    let path = output_dir.join(format!(
        "bundle-{}-{}.json",
        now_unix_millis(),
        Uuid::new_v4()
    ));
    let payload = serde_json::json!({
        "schema_version": 1,
        "session": session,
        "runtime_snapshot": snapshot,
        "recent_events": recent_events,
    });
    fs::write(&path, serde_json::to_vec_pretty(&payload)?)
        .with_context(|| format!("write diagnostics bundle '{}'", path.display()))?;
    let bytes = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
    Ok(ProfileArtifactRecord {
        kind: "diagnostics_bundle".to_string(),
        path: path.to_string_lossy().into_owned(),
        bytes,
    })
}

const WORKFLOW_TOOL_INTERNAL_ATTACH_SNAPSHOT: &str = "internal.attach_snapshot";
const WORKFLOW_TOOL_DIAGNOSTICS_START_BALANCED_WALL_BLOCKING: &str =
    "diagnostics.start_balanced_wall_blocking";
const WORKFLOW_TOOL_DIAGNOSTICS_STOP_ACTIVE_PROFILING: &str = "diagnostics.stop_active_profiling";
const WORKFLOW_TOOL_DIAGNOSTICS_MATERIALIZE_REPORT: &str = "diagnostics.materialize_report";

#[derive(Debug, Clone, Deserialize)]
struct WorkflowAttachSnapshotToolArgs {
    #[serde(default)]
    recent_events_limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct WorkflowProfilingToolArgs {
    #[serde(default)]
    capture_mode: Option<ProfilingCaptureMode>,
    #[serde(default)]
    preset: Option<DiagnosticsPreset>,
    #[serde(default)]
    duration_ms: Option<u64>,
    #[serde(default)]
    sample_rate_hz: Option<u32>,
}

fn built_in_workflow_presets() -> Vec<InvestigationWorkflowPreset> {
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

fn built_in_workflow_by_id(workflow_id: &str) -> Option<InvestigationWorkflowPreset> {
    built_in_workflow_presets()
        .into_iter()
        .find(|workflow| workflow.workflow_id == workflow_id)
}

fn append_workflow_timeline_event(
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

fn append_workflow_marker(
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

fn workflow_step_index(workflow: &InvestigationWorkflowRun, step_id: &str) -> Result<usize> {
    workflow
        .steps
        .iter()
        .position(|step| step.step_id == step_id)
        .ok_or_else(|| anyhow!("workflow step '{}' not found", step_id))
}

fn workflow_step_is_terminal(status: InvestigationWorkflowStepStatus) -> bool {
    matches!(
        status,
        InvestigationWorkflowStepStatus::Completed
            | InvestigationWorkflowStepStatus::Skipped
            | InvestigationWorkflowStepStatus::Failed
            | InvestigationWorkflowStepStatus::Aborted
    )
}

fn workflow_dependencies_satisfied(
    workflow: &InvestigationWorkflowRun,
    step: &crate::investigation::InvestigationWorkflowStepState,
) -> bool {
    step.allowed_after.iter().all(|dependency| {
        workflow
            .steps
            .iter()
            .find(|candidate| candidate.step_id == *dependency)
            .is_some_and(|candidate| workflow_step_is_terminal(candidate.status))
    })
}

fn next_available_workflow_step_id(workflow: &InvestigationWorkflowRun) -> Option<String> {
    workflow
        .steps
        .iter()
        .find(|step| {
            step.status == InvestigationWorkflowStepStatus::Pending
                && workflow_dependencies_satisfied(workflow, step)
        })
        .map(|step| step.step_id.clone())
}

fn complete_workflow_progress(workflow: &mut InvestigationWorkflowRun) {
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

fn workflow_artifact_dir(cache_path: &str, workflow_run_id: &str) -> Result<PathBuf> {
    let dir = PathBuf::from(cache_path)
        .join(DIAGNOSTICS_WORKFLOW_ARTIFACT_DIR_NAME)
        .join(workflow_run_id);
    fs::create_dir_all(&dir)
        .with_context(|| format!("create workflow artifact dir '{}'", dir.display()))?;
    Ok(dir)
}

fn write_workflow_artifact(
    cache_path: &str,
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

fn preview_text(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.chars().take(512).collect())
    }
}

fn store_error_response(error: anyhow::Error) -> DiagnosticsResponse {
    DiagnosticsResponse::Error {
        code: "investigation_store_error".to_string(),
        message: error.to_string(),
    }
}

fn workflow_error_response(code: &str, message: impl Into<String>) -> DiagnosticsResponse {
    DiagnosticsResponse::Error {
        code: code.to_string(),
        message: message.into(),
    }
}

fn run_command_with_timeout(
    mut command: std::process::Command,
    timeout: Duration,
) -> Result<(i32, String, String)> {
    use std::io::Read;
    use std::process::Stdio;
    use std::thread;

    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().context("spawn workflow command")?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("workflow command stdout was not captured"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("workflow command stderr was not captured"))?;
    let stdout_reader = thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let mut stdout = stdout;
        let mut buffer = Vec::new();
        stdout.read_to_end(&mut buffer)?;
        Ok(buffer)
    });
    let stderr_reader = thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let mut stderr = stderr;
        let mut buffer = Vec::new();
        stderr.read_to_end(&mut buffer)?;
        Ok(buffer)
    });
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().context("poll workflow command")? {
            let stdout = stdout_reader
                .join()
                .map_err(|_| anyhow!("stdout reader thread panicked"))?
                .context("collect workflow command stdout")?;
            let stderr = stderr_reader
                .join()
                .map_err(|_| anyhow!("stderr reader thread panicked"))?
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
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(anyhow!(
                "workflow command exceeded timeout of {} ms",
                timeout.as_millis()
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn run_attach_snapshot_tool(
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
            &session.cache_path,
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
        &session.cache_path,
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

fn find_last_started_workflow_window_id(workflow: &InvestigationWorkflowRun) -> Option<String> {
    workflow.steps.iter().rev().find_map(|step| {
        step.output_summary
            .as_ref()
            .and_then(|summary| summary.get("window_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    })
}

fn execute_workflow_tool_like_step(
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

fn write_wall_blocking_artifact(cache_path: &Path, payload: &serde_json::Value) -> Result<PathBuf> {
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
    fs::write(&path, serde_json::to_vec_pretty(payload)?).with_context(|| {
        format!(
            "write wall-blocking profiling artifact '{}'",
            path.display()
        )
    })?;
    Ok(path)
}

#[derive(Debug, Clone)]
pub(crate) struct LiveMountRuntimeFingerprint {
    pub bridge: String,
    pub cache_size: Option<String>,
    pub endpoint_override: Option<String>,
    pub snapshot_id: Option<String>,
    pub vfs_config: serde_json::Value,
}

pub(crate) struct LiveMountDiagnosticsSource {
    identity: DiagnosticsSessionInfo,
    base_fingerprint: EnvironmentFingerprint,
    vfs: Arc<StudioVfs>,
}

impl LiveMountDiagnosticsSource {
    pub(crate) fn new(
        format_info: &FormatInfo,
        mount_point: &str,
        cache_path: &Path,
        pid: u32,
        read_only: bool,
        runtime_fingerprint: LiveMountRuntimeFingerprint,
        vfs: Arc<StudioVfs>,
    ) -> Self {
        let session_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{}:{mount_point}:{pid}", format_info.uuid).as_bytes(),
        )
        .to_string();
        let identity = DiagnosticsSessionInfo {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            session_id,
            volume: format_info.name.clone(),
            volume_uuid: format_info.uuid.clone(),
            bridge: runtime_fingerprint.bridge.clone(),
            mount_point: mount_point.to_string(),
            pid,
            cache_path: cache_path.to_string_lossy().into_owned(),
            read_only,
            started_unix_ms: now_unix_millis(),
            controller: None,
        };
        let base_fingerprint = base_environment_fingerprint(&identity, &runtime_fingerprint);
        Self {
            identity,
            base_fingerprint,
            vfs,
        }
    }
}

impl DiagnosticsDataSource for LiveMountDiagnosticsSource {
    fn session_info(&self) -> DiagnosticsSessionInfo {
        self.identity.clone()
    }

    fn health_snapshot(&self) -> DiagnosticsHealthSnapshot {
        DiagnosticsHealthSnapshot {
            generated_unix_ms: now_unix_millis(),
            ingress: self.vfs.ingress_stats(),
            upload: self.vfs.upload_stats(),
            file_sessions: self.vfs.file_session_stats(),
            foreground_reads: self.vfs.foreground_read_protection_stats(),
            write_path: self.vfs.write_path_observability_snapshot(),
            zeroio: self.vfs.zeroio_runtime_snapshot(),
        }
    }

    fn runtime_snapshot(&self) -> DiagnosticsRuntimeSnapshot {
        let health = self.health_snapshot();
        DiagnosticsRuntimeSnapshot {
            schema_version: DIAGNOSTICS_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
            generated_unix_ms: health.generated_unix_ms,
            ingress: health.ingress,
            write_path: health.write_path,
            upload: health.upload,
            zeroio: health.zeroio,
            cache: DiagnosticsCacheSnapshot {
                state: self.vfs.cache_state_snapshot(),
            },
            synchronization: DiagnosticsSynchronizationSnapshot {
                state: self.vfs.synchronization_state_snapshot(),
            },
        }
    }

    fn recent_events(&self, limit: usize) -> Vec<DiagnosticsRecentEvent> {
        self.vfs
            .recent_runtime_events(limit)
            .into_iter()
            .map(DiagnosticsRecentEvent::from)
            .collect()
    }

    fn environment_fingerprint(
        &self,
        case_workload_identity: Option<&str>,
        run_workload_identity: Option<&str>,
    ) -> EnvironmentFingerprint {
        let mut fingerprint = self.base_fingerprint.clone();
        fingerprint.workload_identity = WorkloadIdentityFingerprint {
            case_workload: case_workload_identity.map(str::to_string),
            run_workload: run_workload_identity.map(str::to_string),
        };
        fingerprint
    }

    fn reset_observability_counters(&self) -> std::result::Result<(), DiagnosticsActionError> {
        self.vfs.reset_diagnostics_observability();
        Ok(())
    }
}

fn base_environment_fingerprint(
    identity: &DiagnosticsSessionInfo,
    runtime_fingerprint: &LiveMountRuntimeFingerprint,
) -> EnvironmentFingerprint {
    EnvironmentFingerprint {
        platform: PlatformFingerprint {
            os: std::env::consts::OS.to_string(),
            family: std::env::consts::FAMILY.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        },
        bridge: identity.bridge.clone(),
        build: BuildIdentity {
            package_name: env!("CARGO_PKG_NAME").to_string(),
            package_version: env!("CARGO_PKG_VERSION").to_string(),
            executable_path: std::env::current_exe()
                .ok()
                .map(|path| path.to_string_lossy().into_owned()),
            debug_build: cfg!(debug_assertions),
        },
        runtime_configuration: RuntimeConfigurationFingerprint {
            volume: identity.volume.clone(),
            volume_uuid: identity.volume_uuid.clone(),
            mount_point: identity.mount_point.clone(),
            cache_path: identity.cache_path.clone(),
            read_only: identity.read_only,
            cache_size: runtime_fingerprint.cache_size.clone(),
            endpoint_override: runtime_fingerprint.endpoint_override.clone(),
            snapshot_id: runtime_fingerprint.snapshot_id.clone(),
            vfs_config: runtime_fingerprint.vfs_config.clone(),
        },
        workload_identity: WorkloadIdentityFingerprint {
            case_workload: None,
            run_workload: None,
        },
        machine: static_machine_fingerprint(),
        diagnostics: DiagnosticsSettingsFingerprint {
            protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
            default_lease_ttl_ms: DEFAULT_LEASE_TTL_MS,
            max_lease_ttl_ms: MAX_LEASE_TTL_MS,
            max_connections: MAX_DIAGNOSTICS_CONNECTIONS,
        },
    }
}

fn static_machine_fingerprint() -> MachineFingerprint {
    static MACHINE_FINGERPRINT: OnceLock<MachineFingerprint> = OnceLock::new();
    MACHINE_FINGERPRINT
        .get_or_init(|| {
            let system = sysinfo::System::new_all();
            MachineFingerprint {
                host_name: sysinfo::System::host_name(),
                system_name: sysinfo::System::name(),
                kernel_version: sysinfo::System::kernel_version(),
                os_version: sysinfo::System::os_version(),
                total_memory_bytes: system.total_memory(),
                cpu_count: system.cpus().len(),
            }
        })
        .clone()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DiagnosticsRequest {
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
enum DiagnosticsResponse {
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

#[derive(Debug, Default)]
struct ControllerLeaseManager {
    state: Mutex<Option<ActiveControllerLease>>,
}

#[derive(Debug, Clone)]
struct ActiveControllerLease {
    lease: ControllerLease,
    expires_at: Instant,
}

impl ControllerLeaseManager {
    fn snapshot(&self) -> Option<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        state.as_ref().map(|lease| lease.lease.clone())
    }

    fn acquire(&self, client_name: String, ttl_ms: Option<u64>) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        if let Some(active) = state.as_ref() {
            return Err(anyhow!(
                "controller lease already held by '{}' until {}",
                active.lease.client_name,
                active.lease.expires_unix_ms
            ));
        }
        let ttl_ms = ttl_ms
            .unwrap_or(DEFAULT_LEASE_TTL_MS)
            .clamp(1, MAX_LEASE_TTL_MS);
        let lease = ControllerLease {
            lease_id: Uuid::new_v4().to_string(),
            client_name,
            ttl_ms,
            expires_unix_ms: now_unix_millis().saturating_add(ttl_ms),
        };
        *state = Some(ActiveControllerLease {
            lease: lease.clone(),
            expires_at: now + Duration::from_millis(ttl_ms),
        });
        Ok(lease)
    }

    fn renew(&self, lease_id: &str, ttl_ms: Option<u64>) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        let active = state
            .as_mut()
            .ok_or_else(|| anyhow!("controller lease is not currently held"))?;
        if active.lease.lease_id != lease_id {
            return Err(anyhow!("controller lease '{lease_id}' is not current"));
        }
        active.lease.ttl_ms = ttl_ms
            .unwrap_or(active.lease.ttl_ms)
            .clamp(1, MAX_LEASE_TTL_MS);
        active.expires_at = now + Duration::from_millis(active.lease.ttl_ms);
        active.lease.expires_unix_ms = now_unix_millis().saturating_add(active.lease.ttl_ms);
        Ok(active.lease.clone())
    }

    fn require_current(&self, lease_id: &str) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        let active = state
            .as_ref()
            .ok_or_else(|| anyhow!("controller lease is not currently held"))?;
        if active.lease.lease_id != lease_id {
            return Err(anyhow!("controller lease '{lease_id}' is not current"));
        }
        Ok(active.lease.clone())
    }

    fn expire_if_needed(state: &mut Option<ActiveControllerLease>, now: Instant) {
        if state.as_ref().is_some_and(|lease| lease.expires_at <= now) {
            *state = None;
        }
    }
}

pub(crate) struct LocalDiagnosticsHost {
    #[cfg(unix)]
    endpoint_path: PathBuf,
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl LocalDiagnosticsHost {
    pub(crate) fn start(
        runtime: &Handle,
        mount_point: &str,
        source: Arc<dyn DiagnosticsDataSource>,
        profiling_backend: Arc<dyn RuntimeProfilingBackend>,
    ) -> Result<Self> {
        #[cfg(test)]
        if let Some(message) = TEST_START_FAILURE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .clone()
        {
            return Err(anyhow!(message));
        }

        let mounts_dir = utils::get_mounts_dir()?;
        let base_name = utils::sanitize_mount_point(mount_point);
        let endpoint = endpoint_for(&mounts_dir, &base_name, source.session_info().pid);
        #[cfg(unix)]
        let endpoint_path = bind_unix_listener_path(&endpoint)?;
        let discovery = discovery_from_source(source.session_info(), endpoint.clone());
        publish_discovery(mount_point, &discovery)?;

        let lease_manager = Arc::new(ControllerLeaseManager::default());
        let investigation_store = match InvestigationStore::default_store() {
            Ok(store) => Some(Arc::new(store)),
            Err(error) => {
                warn!(
                    mount_point,
                    error = %error,
                    "investigation store unavailable; run metadata will not persist"
                );
                None
            }
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let profiling = Arc::new(DiagnosticsProfilingCoordinator::new(Arc::new(
            CompositeRuntimeProfilingBackend::new(source.clone(), profiling_backend),
        )));
        let task = runtime.spawn(run_server(
            endpoint,
            discovery.auth_token.clone(),
            source,
            lease_manager,
            investigation_store,
            Some(profiling),
            shutdown_rx,
        ));

        Ok(Self {
            #[cfg(unix)]
            endpoint_path,
            shutdown_tx,
            task,
        })
    }

    pub(crate) fn start_best_effort(
        runtime: &Handle,
        mount_point: &str,
        source: Arc<dyn DiagnosticsDataSource>,
        profiling_backend: Arc<dyn RuntimeProfilingBackend>,
    ) -> Option<Self> {
        match Self::start(runtime, mount_point, source, profiling_backend) {
            Ok(host) => Some(host),
            Err(error) => {
                warn!(
                    mount_point,
                    error = %error,
                    "diagnostics host unavailable; continuing without live attach"
                );
                None
            }
        }
    }

    pub(crate) async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        self.task.abort();
        let _ = self.task.await;
        #[cfg(unix)]
        let _ = fs::remove_file(self.endpoint_path);
    }
}

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

    async fn send(&self, request: DiagnosticsRequest) -> Result<DiagnosticsResponse> {
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

pub(crate) fn discover_local_sessions() -> Result<Vec<LocalDiagnosticsDiscovery>> {
    discover_sessions_in(&utils::get_mounts_dir()?, utils::mount_is_active)
}

fn discover_sessions_in(
    dir: &Path,
    is_mount_active: impl Fn(&str) -> Result<bool>,
) -> Result<Vec<LocalDiagnosticsDiscovery>> {
    let mut sessions = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if !file_type.is_file() {
            continue;
        }
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = match fs::read(entry.path()) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let record = match serde_json::from_slice::<MountRecord>(&bytes) {
            Ok(record) => record,
            Err(_) => continue,
        };
        let Some(discovery) = record.diagnostics else {
            continue;
        };
        if is_mount_active(&record.mount_point)? {
            sessions.push(discovery);
        }
    }
    sessions.sort_by(|left, right| left.mount_point.cmp(&right.mount_point));
    Ok(sessions)
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct MountRecord {
    #[serde(default)]
    record_version: u32,
    pid: u32,
    mount_point: String,
    volume: String,
    #[serde(default)]
    cache_path: Option<String>,
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default)]
    diagnostics: Option<LocalDiagnosticsDiscovery>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

fn publish_discovery(mount_point: &str, discovery: &LocalDiagnosticsDiscovery) -> Result<()> {
    let record_path = utils::pid_file_path(mount_point)?;
    if let Some(parent) = record_path.parent() {
        ensure_owner_only_directory(parent)?;
    }
    let bytes = fs::read(&record_path).with_context(|| {
        format!(
            "read mount record '{}' before publishing diagnostics",
            record_path.display()
        )
    })?;
    let mut record: MountRecord = serde_json::from_slice(&bytes)?;
    record.diagnostics = Some(discovery.clone());
    fs::write(&record_path, serde_json::to_vec_pretty(&record)?)?;
    ensure_owner_only_file(&record_path)?;
    Ok(())
}

fn discovery_from_source(
    session: DiagnosticsSessionInfo,
    endpoint: LocalDiagnosticsEndpoint,
) -> LocalDiagnosticsDiscovery {
    LocalDiagnosticsDiscovery {
        protocol_version: DIAGNOSTICS_PROTOCOL_VERSION,
        session_id: session.session_id,
        volume: session.volume,
        volume_uuid: session.volume_uuid,
        bridge: session.bridge,
        mount_point: session.mount_point,
        pid: session.pid,
        cache_path: session.cache_path,
        read_only: session.read_only,
        started_unix_ms: session.started_unix_ms,
        endpoint,
        auth_token: Uuid::new_v4().to_string(),
    }
}

fn endpoint_for(_mounts_dir: &Path, base_name: &str, pid: u32) -> LocalDiagnosticsEndpoint {
    #[cfg(unix)]
    {
        let socket_path = unix_socket_path(_mounts_dir, base_name, pid);
        LocalDiagnosticsEndpoint::UnixSocket {
            path: socket_path.to_string_lossy().into_owned(),
        }
    }

    #[cfg(windows)]
    {
        LocalDiagnosticsEndpoint::WindowsNamedPipe {
            name: format!(r"\\.\pipe\studiofs-{base_name}-{pid}-diagnostics"),
        }
    }
}

#[cfg(unix)]
fn unix_socket_path(mounts_dir: &Path, base_name: &str, pid: u32) -> PathBuf {
    let preferred = mounts_dir.join(format!("{base_name}.{pid}.sock"));
    if preferred.as_os_str().as_bytes().len() <= MAX_UNIX_SOCKET_PATH_BYTES {
        return preferred;
    }

    let digest = Sha256::digest(base_name.as_bytes());
    mounts_dir.join(format!("sfsdiag-{pid}-{}.sock", hex::encode(&digest[..8])))
}

fn now_unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn bind_unix_listener_path(endpoint: &LocalDiagnosticsEndpoint) -> Result<PathBuf> {
    let LocalDiagnosticsEndpoint::UnixSocket { path } = endpoint;
    let path = PathBuf::from(path);
    if path.as_os_str().as_bytes().len() > MAX_UNIX_SOCKET_PATH_BYTES {
        return Err(anyhow!(
            "diagnostics socket path '{}' exceeds unix socket limit",
            path.display()
        ));
    }
    if path.exists() {
        fs::remove_file(&path)
            .with_context(|| format!("remove stale diagnostics socket '{}'", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        ensure_owner_only_directory(parent)?;
    }
    Ok(path)
}

async fn run_server(
    endpoint: LocalDiagnosticsEndpoint,
    auth_token: String,
    source: Arc<dyn DiagnosticsDataSource>,
    lease_manager: Arc<ControllerLeaseManager>,
    investigation_store: Option<Arc<InvestigationStore>>,
    profiling: Option<Arc<DiagnosticsProfilingCoordinator>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let connection_limit = Arc::new(Semaphore::new(MAX_DIAGNOSTICS_CONNECTIONS));

    #[cfg(unix)]
    {
        let LocalDiagnosticsEndpoint::UnixSocket { path } = &endpoint;
        let listener = match UnixListener::bind(path) {
            Ok(listener) => listener,
            Err(error) => {
                warn!(path, error = %error, "failed to bind diagnostics socket");
                return;
            }
        };
        if let Err(error) = fs::set_permissions(path, fs::Permissions::from_mode(0o600)) {
            warn!(path, error = %error, "failed to restrict diagnostics socket permissions");
            let _ = fs::remove_file(path);
            return;
        }
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        break;
                    }
                }
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, _)) => {
                            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                                warn!(path, limit = MAX_DIAGNOSTICS_CONNECTIONS, "diagnostics connection limit reached");
                                drop(stream);
                                continue;
                            };
                            let auth_token = auth_token.clone();
                            let source = source.clone();
                            let lease_manager = lease_manager.clone();
                            let investigation_store = investigation_store.clone();
                            let profiling = profiling.clone();
                            tokio::spawn(async move {
                                if let Err(error) = handle_client(stream, auth_token, source, lease_manager, investigation_store, profiling, permit).await {
                                    warn!(error = %error, "diagnostics request failed");
                                }
                            });
                        }
                        Err(error) => {
                            warn!(path, error = %error, "diagnostics accept failed");
                            break;
                        }
                    }
                }
            }
        }
    }

    #[cfg(windows)]
    {
        let LocalDiagnosticsEndpoint::WindowsNamedPipe { name } = &endpoint;
        let pipe_factory = match WindowsPipeFactory::new(name) {
            Ok(factory) => factory,
            Err(error) => {
                warn!(name, error = %error, "failed to configure diagnostics pipe security");
                return;
            }
        };
        let mut server = match pipe_factory.create_server(true) {
            Ok(server) => server,
            Err(error) => {
                warn!(name, error = %error, "failed to create diagnostics pipe");
                return;
            }
        };
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_ok() && *shutdown_rx.borrow() {
                        break;
                    }
                }
                connect = server.connect() => {
                    match connect {
                        Ok(()) => {
                            let next_server = match pipe_factory.create_server(false) {
                                Ok(server) => server,
                                Err(error) => {
                                    warn!(name, error = %error, "failed to create diagnostics pipe");
                                    break;
                                }
                            };
                            let connected_server = server;
                            server = next_server;
                            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                                warn!(name, limit = MAX_DIAGNOSTICS_CONNECTIONS, "diagnostics connection limit reached");
                                drop(connected_server);
                                continue;
                            };
                            let auth_token = auth_token.clone();
                            let source = source.clone();
                            let lease_manager = lease_manager.clone();
                            let investigation_store = investigation_store.clone();
                            let profiling = profiling.clone();
                            tokio::spawn(async move {
                                if let Err(error) = handle_client(connected_server, auth_token, source, lease_manager, investigation_store, profiling, permit).await {
                                    warn!(error = %error, "diagnostics request failed");
                                }
                            });
                        }
                        Err(error) => {
                            warn!(name, error = %error, "diagnostics pipe connect failed");
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(unix)]
async fn connect_unix_with_retry(path: &str) -> Result<UnixStream> {
    let mut last_error = None;
    for _ in 0..CONNECT_RETRY_ATTEMPTS {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(stream),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(CONNECT_RETRY_DELAY_MS)).await;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("connect diagnostics socket '{path}'"));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "diagnostics socket did not become ready in time",
        )
    }))
    .with_context(|| format!("connect diagnostics socket '{path}'"))
}

#[cfg(windows)]
async fn connect_named_pipe_with_retry(
    name: &str,
) -> Result<tokio::net::windows::named_pipe::NamedPipeClient> {
    use tokio::net::windows::named_pipe::ClientOptions;

    let mut last_error = None;
    for _ in 0..CONNECT_RETRY_ATTEMPTS {
        match ClientOptions::new().open(name) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(CONNECT_RETRY_DELAY_MS)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "diagnostics pipe did not become ready in time",
        )
    }))
    .with_context(|| format!("connect diagnostics pipe '{name}'"))
}

#[cfg(test)]
pub(crate) fn set_test_start_failure(message: Option<&str>) {
    *TEST_START_FAILURE.get_or_init(|| Mutex::new(None)).lock() = message.map(str::to_string);
}

#[cfg(test)]
pub(crate) fn set_test_safe_action_audit_failure(message: Option<&str>) {
    *TEST_SAFE_ACTION_AUDIT_FAILURE
        .get_or_init(|| Mutex::new(None))
        .lock() = message.map(str::to_string);
}

async fn handle_client<S>(
    stream: S,
    auth_token: String,
    source: Arc<dyn DiagnosticsDataSource>,
    lease_manager: Arc<ControllerLeaseManager>,
    investigation_store: Option<Arc<InvestigationStore>>,
    profiling: Option<Arc<DiagnosticsProfilingCoordinator>>,
    _permit: OwnedSemaphorePermit,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    loop {
        let line = match read_frame_line(&mut reader, MAX_DIAGNOSTICS_REQUEST_FRAME_BYTES).await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(FrameReadError::TooLarge) => {
                let response = DiagnosticsResponse::Error {
                    code: "request_too_large".to_string(),
                    message: format!(
                        "diagnostics request exceeded {} bytes",
                        MAX_DIAGNOSTICS_REQUEST_FRAME_BYTES
                    ),
                };
                writer
                    .write_all(serde_json::to_string(&response)?.as_bytes())
                    .await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
                break;
            }
            Err(FrameReadError::Io(error)) => return Err(error.into()),
            Err(FrameReadError::Utf8(error)) => return Err(error.into()),
        };
        let request: DiagnosticsRequest = serde_json::from_str(line.trim_end())?;
        let auth_token_for_request = auth_token.clone();
        let source_for_request = source.clone();
        let lease_manager_for_request = lease_manager.clone();
        let investigation_store_for_request = investigation_store.clone();
        let profiling_for_request = profiling.clone();
        let response = tokio::task::spawn_blocking(move || {
            execute_request(
                request,
                &auth_token_for_request,
                source_for_request.as_ref(),
                &lease_manager_for_request,
                investigation_store_for_request.as_deref(),
                profiling_for_request.as_deref(),
            )
        })
        .await
        .map_err(|error| anyhow!("diagnostics request task failed: {error}"))?;
        writer
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    Ok(())
}

enum FrameReadError {
    Io(std::io::Error),
    TooLarge,
    Utf8(std::string::FromUtf8Error),
}

impl From<std::io::Error> for FrameReadError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<std::string::FromUtf8Error> for FrameReadError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        Self::Utf8(error)
    }
}

async fn read_frame_line<R>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> std::result::Result<Option<String>, FrameReadError>
where
    R: AsyncBufRead + Unpin,
{
    let mut buffer = Vec::new();

    loop {
        let (consumed, too_large, eof) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                (0, false, true)
            } else if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
                let take = newline + 1;
                if buffer.len() + take > max_frame_bytes {
                    (take, true, false)
                } else {
                    buffer.extend_from_slice(&available[..take]);
                    (take, false, false)
                }
            } else {
                let take = available.len();
                if buffer.len() + take > max_frame_bytes {
                    (take, true, false)
                } else {
                    buffer.extend_from_slice(available);
                    (take, false, false)
                }
            }
        };
        if eof {
            if buffer.is_empty() {
                return Ok(None);
            }
            break;
        }

        reader.consume(consumed);
        if too_large {
            return Err(FrameReadError::TooLarge);
        }
        if buffer.last() == Some(&b'\n') {
            break;
        }
    }

    while matches!(buffer.last(), Some(b'\n' | b'\r')) {
        buffer.pop();
    }

    Ok(Some(String::from_utf8(buffer)?))
}

fn execute_request(
    request: DiagnosticsRequest,
    auth_token: &str,
    source: &dyn DiagnosticsDataSource,
    lease_manager: &ControllerLeaseManager,
    investigation_store: Option<&InvestigationStore>,
    profiling: Option<&DiagnosticsProfilingCoordinator>,
) -> DiagnosticsResponse {
    let service = DiagnosticsSessionService::new(DiagnosticsSessionDeps {
        source,
        leases: lease_manager,
        store: investigation_store,
        profiling,
    });
    dispatch_protocol(&service, auth_token, request)
}

async fn send_request<S>(stream: S, request: DiagnosticsRequest) -> Result<DiagnosticsResponse>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    writer
        .write_all(serde_json::to_string(&request)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    let line = match read_frame_line(&mut reader, MAX_DIAGNOSTICS_RESPONSE_FRAME_BYTES).await {
        Ok(Some(line)) => line,
        Ok(None) => return Err(anyhow!("diagnostics peer closed without responding")),
        Err(FrameReadError::TooLarge) => {
            return Err(anyhow!(
                "diagnostics response exceeded {} bytes",
                MAX_DIAGNOSTICS_RESPONSE_FRAME_BYTES
            ));
        }
        Err(FrameReadError::Io(error)) => return Err(error.into()),
        Err(FrameReadError::Utf8(error)) => return Err(error.into()),
    };
    Ok(serde_json::from_str(line.trim_end())?)
}

#[cfg(unix)]
fn ensure_owner_only_directory(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restrict directory permissions for '{}'", path.display()))
}

#[cfg(not(unix))]
fn ensure_owner_only_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn ensure_owner_only_file(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict file permissions for '{}'", path.display()))
}

#[cfg(not(unix))]
fn ensure_owner_only_file(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
struct WindowsPipeFactory {
    name: String,
    security_descriptor: WindowsSecurityDescriptor,
}

#[cfg(windows)]
impl WindowsPipeFactory {
    fn new(name: &str) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            security_descriptor: WindowsSecurityDescriptor::same_user_only()?,
        })
    }

    fn create_server(&self, first_pipe_instance: bool) -> Result<NamedPipeServer> {
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(first_pipe_instance)
            .reject_remote_clients(true);

        let mut attrs = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.security_descriptor.as_ptr().0,
            bInheritHandle: false.into(),
        };

        unsafe {
            options.create_with_security_attributes_raw(
                &self.name,
                &mut attrs as *mut _ as *mut c_void,
            )
        }
        .with_context(|| format!("create diagnostics pipe '{}'", self.name))
    }
}

#[cfg(windows)]
struct WindowsSecurityDescriptor {
    descriptor: PSECURITY_DESCRIPTOR,
}

#[cfg(windows)]
impl WindowsSecurityDescriptor {
    fn same_user_only() -> Result<Self> {
        let mut descriptor = PSECURITY_DESCRIPTOR::default();
        unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                w!("D:P(A;;GA;;;OW)"),
                SDDL_REVISION_1,
                &mut descriptor,
                None,
            )
        }
        .context("build same-user security descriptor for diagnostics pipe")?;

        Ok(Self { descriptor })
    }

    fn as_ptr(&self) -> PSECURITY_DESCRIPTOR {
        self.descriptor
    }
}

#[cfg(windows)]
impl Drop for WindowsSecurityDescriptor {
    fn drop(&mut self) {
        unsafe {
            let _ = LocalFree(Some(HLOCAL(self.descriptor.0)));
        }
    }
}

#[cfg(windows)]
unsafe impl Send for WindowsSecurityDescriptor {}

#[cfg(windows)]
unsafe impl Sync for WindowsSecurityDescriptor {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
            fingerprint.workload_identity.case_workload =
                case_workload_identity.map(str::to_string);
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

        fn start_window(
            &self,
            request: &StartProfilingWindowRequest,
        ) -> ProfilingBackendStartResult {
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

        fn start_window(
            &self,
            _request: &StartProfilingWindowRequest,
        ) -> ProfilingBackendStartResult {
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

        fn start_window(
            &self,
            _request: &StartProfilingWindowRequest,
        ) -> ProfilingBackendStartResult {
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

        fn start_window(
            &self,
            _request: &StartProfilingWindowRequest,
        ) -> ProfilingBackendStartResult {
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
        tempfile::tempdir().expect("tempdir")
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

        let sessions = discover_sessions_in(dir.path(), |mount_point| Ok(mount_point == "X:"))
            .expect("discover");

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
        let LocalDiagnosticsEndpoint::UnixSocket { path } =
            endpoint_for(dir.path(), &base_name, 42);

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
                fingerprint.workload_identity.run_workload =
                    run_workload_identity.map(str::to_string);
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

        let upload_pending_in_view = report.references.derived_view.raw.snapshot_sections["upload"]
            ["pending_uploads"]
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
                ttl_ms: Some(250),
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
                ttl_ms: Some(250),
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
                ttl_ms: Some(250),
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
                ttl_ms: Some(250),
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
                ttl_ms: Some(250),
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
            .acquire_controller("tester".to_string(), Some(250))
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
            .acquire_controller("tester".to_string(), Some(250))
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
            .acquire_controller("tester".to_string(), Some(250))
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
            .acquire_controller("tester".to_string(), Some(250))
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
        let profiling = DiagnosticsProfilingCoordinator::new(Arc::new(
            UnsupportedProfilingBackend("profiling backend unavailable".to_string()),
        ));

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
        let temp = tempfile::tempdir().expect("tempdir");
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
        assert!(comparison.summary.metric_deltas.iter().any(|metric| {
            metric.metric == "upload.pending_uploads_delta" && metric.delta < 0.0
        }));
        assert!(comparison.summary.anomaly_deltas.iter().any(|anomaly| {
            anomaly.title == "queue growth"
                && anomaly.baseline_count == 1
                && anomaly.candidate_count == 1
        }));
        assert!(comparison.summary.hotspot_deltas.iter().any(|hotspot| {
            hotspot.metric == "payload_store_sync_data.queue_wait_total_ms_delta"
                && hotspot.delta < 0.0
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
}

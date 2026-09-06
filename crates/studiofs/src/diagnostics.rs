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
    CoherenceHealthSnapshot, StudioVfs, VFS_RUNTIME_EVENT_CAPACITY, VfsCacheStateSnapshot,
    VfsForegroundReadProtectionStats, VfsIngressStats, VfsRuntimeEvent,
    VfsSynchronizationStateSnapshot, VfsUploadStats, VfsWritePathObservabilitySnapshot,
    VfsZeroIoRuntimeSnapshot,
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
        Foundation::{ERROR_NO_DATA, HLOCAL, LocalFree},
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

mod client;
mod discovery_transport;
mod evidence;
mod host;
mod lease;
mod live_source;
mod profiling;
mod protocol;
mod types;
mod workflow;

pub(crate) use client::*;
pub(crate) use discovery_transport::*;
pub(crate) use host::*;
pub(crate) use live_source::*;
pub(crate) use profiling::*;
pub(crate) use types::*;

use evidence::*;
use lease::*;
use protocol::*;
use workflow::*;

#[cfg(test)]
mod tests;

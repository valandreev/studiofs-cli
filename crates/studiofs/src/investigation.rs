use anyhow::{Context, Result, anyhow};
use fs2::FileExt;
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(test)]
use std::sync::OnceLock;

pub(crate) const INVESTIGATION_STORE_VERSION: u32 = 1;
pub(crate) const INVESTIGATION_REPORT_SCHEMA_VERSION: u32 = 1;

#[cfg(test)]
static TEST_BASE_DIR: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
#[cfg(test)]
static TEST_SAVE_JSON_FAILURE_PATTERN: OnceLock<Mutex<Option<String>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct InvestigationCase {
    pub id: String,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
    pub title: String,
    pub problem_statement: Option<String>,
    pub workload_identity: Option<String>,
    pub target_environment: Option<String>,
    pub run_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct InvestigationRun {
    pub id: String,
    pub case_id: String,
    pub session_id: String,
    pub created_unix_ms: u64,
    pub fingerprint: EnvironmentFingerprint,
    #[serde(default)]
    pub profiling_windows: Vec<ProfilingWindowRecord>,
    #[serde(default)]
    pub report_ids: Vec<String>,
    pub timeline: Vec<TimelineEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct EnvironmentFingerprint {
    pub platform: PlatformFingerprint,
    pub bridge: String,
    pub build: BuildIdentity,
    pub runtime_configuration: RuntimeConfigurationFingerprint,
    pub workload_identity: WorkloadIdentityFingerprint,
    pub machine: MachineFingerprint,
    pub diagnostics: DiagnosticsSettingsFingerprint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PlatformFingerprint {
    pub os: String,
    pub family: String,
    pub arch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BuildIdentity {
    pub package_name: String,
    pub package_version: String,
    pub executable_path: Option<String>,
    pub debug_build: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RuntimeConfigurationFingerprint {
    pub volume: String,
    pub volume_uuid: String,
    pub mount_point: String,
    pub cache_path: String,
    pub read_only: bool,
    pub cache_size: Option<String>,
    pub endpoint_override: Option<String>,
    pub snapshot_id: Option<String>,
    pub vfs_config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WorkloadIdentityFingerprint {
    pub case_workload: Option<String>,
    pub run_workload: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MachineFingerprint {
    pub host_name: Option<String>,
    pub system_name: Option<String>,
    pub kernel_version: Option<String>,
    pub os_version: Option<String>,
    pub total_memory_bytes: u64,
    pub cpu_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DiagnosticsSettingsFingerprint {
    pub protocol_version: u32,
    pub default_lease_ttl_ms: u64,
    pub max_lease_ttl_ms: u64,
    pub max_connections: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DiagnosticsPreset {
    Minimal,
    Balanced,
    Deep,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProfilingCaptureMode {
    WallBlocking,
    Cpu,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProfilingWindowStatus {
    Running,
    Completed,
    Failed,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProfilingCostEstimate {
    pub score: u8,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProfileArtifactRecord {
    pub kind: String,
    pub path: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProfilingWindowRecord {
    pub window_id: String,
    pub sequence: u64,
    pub occurred_unix_ms: u64,
    pub capture_mode: ProfilingCaptureMode,
    pub preset: DiagnosticsPreset,
    pub requested_duration_ms: u64,
    pub actual_duration_ms: Option<u64>,
    pub sample_rate_hz: Option<u32>,
    pub estimated_cost: ProfilingCostEstimate,
    pub status: ProfilingWindowStatus,
    pub artifact: Option<ProfileArtifactRecord>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TimelineEntrySource {
    Cli,
    Mcp,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TimelineEntryKind {
    Marker,
    Note,
    Anomaly,
    Action,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TimelineEntry {
    pub sequence: u64,
    pub occurred_unix_ms: u64,
    pub source: TimelineEntrySource,
    pub kind: TimelineEntryKind,
    pub title: String,
    pub body: Option<String>,
    pub actor: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InvestigationReportEvidenceSummary {
    pub recent_event_count: usize,
    pub timeline_entry_count: usize,
    pub profiling_window_count: usize,
    pub queue_growth_count: usize,
    pub blocking_interval_count: usize,
    pub hotspot_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct InvestigationReportSectionSummary {
    pub included_count: usize,
    pub total_count: usize,
    pub truncated_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct InvestigationReportTruncationSummary {
    pub anomalies: InvestigationReportSectionSummary,
    pub candidate_bottlenecks: InvestigationReportSectionSummary,
    pub selected_evidence: InvestigationReportSectionSummary,
    pub recommended_next_actions: InvestigationReportSectionSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InvestigationReportFinding {
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub supporting_evidence: Vec<crate::diagnostics::DiagnosticsEvidenceReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InvestigationReportSelectedEvidence {
    pub label: String,
    pub summary: String,
    #[serde(default)]
    pub supporting_evidence: Vec<crate::diagnostics::DiagnosticsEvidenceReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InvestigationRecommendedNextAction {
    pub title: String,
    pub rationale: String,
    #[serde(default)]
    pub supporting_evidence: Vec<crate::diagnostics::DiagnosticsEvidenceReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InvestigationReportReferences {
    pub runtime_snapshot: crate::diagnostics::DiagnosticsRuntimeSnapshot,
    pub derived_view: crate::diagnostics::DiagnosticsEvidenceView,
    #[serde(default)]
    pub profiling_windows: Vec<ProfilingWindowRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InvestigationReport {
    pub schema_version: u32,
    pub id: String,
    pub run_id: String,
    pub case_id: String,
    pub created_unix_ms: u64,
    pub environment: EnvironmentFingerprint,
    #[serde(default)]
    pub marker_timeline: Vec<TimelineEntry>,
    pub evidence_summary: InvestigationReportEvidenceSummary,
    #[serde(default)]
    pub truncation: InvestigationReportTruncationSummary,
    #[serde(default)]
    pub anomalies: Vec<InvestigationReportFinding>,
    #[serde(default)]
    pub candidate_bottlenecks: Vec<InvestigationReportFinding>,
    #[serde(default)]
    pub selected_evidence: Vec<InvestigationReportSelectedEvidence>,
    #[serde(default)]
    pub recommended_next_actions: Vec<InvestigationRecommendedNextAction>,
    pub references: InvestigationReportReferences,
}

#[derive(Debug, Clone)]
pub(crate) struct NewInvestigationCase {
    pub title: String,
    pub problem_statement: Option<String>,
    pub workload_identity: Option<String>,
    pub target_environment: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct NewInvestigationRun {
    pub case_id: String,
    pub session_id: String,
    pub fingerprint: EnvironmentFingerprint,
}

#[derive(Debug, Clone)]
pub(crate) struct NewTimelineEntry {
    pub source: TimelineEntrySource,
    pub kind: TimelineEntryKind,
    pub title: String,
    pub body: Option<String>,
    pub actor: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct NewProfilingWindow {
    pub window_id: String,
    pub occurred_unix_ms: u64,
    pub capture_mode: ProfilingCaptureMode,
    pub preset: DiagnosticsPreset,
    pub requested_duration_ms: u64,
    pub actual_duration_ms: Option<u64>,
    pub sample_rate_hz: Option<u32>,
    pub estimated_cost: ProfilingCostEstimate,
    pub status: ProfilingWindowStatus,
    pub artifact: Option<ProfileArtifactRecord>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct NewInvestigationReport {
    pub environment: EnvironmentFingerprint,
    pub marker_timeline: Vec<TimelineEntry>,
    pub evidence_summary: InvestigationReportEvidenceSummary,
    pub truncation: InvestigationReportTruncationSummary,
    pub anomalies: Vec<InvestigationReportFinding>,
    pub candidate_bottlenecks: Vec<InvestigationReportFinding>,
    pub selected_evidence: Vec<InvestigationReportSelectedEvidence>,
    pub recommended_next_actions: Vec<InvestigationRecommendedNextAction>,
    pub references: InvestigationReportReferences,
}

#[derive(Debug)]
pub(crate) struct InvestigationStore {
    base_dir: PathBuf,
    io_lock: Mutex<()>,
}

impl InvestigationStore {
    pub(crate) fn new(base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            io_lock: Mutex::new(()),
        }
    }

    pub(crate) fn default_store() -> Result<Self> {
        Ok(Self::new(default_store_dir()?))
    }

    #[cfg(test)]
    pub(crate) fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub(crate) fn create_case(&self, input: NewInvestigationCase) -> Result<InvestigationCase> {
        let _guard = self.io_lock.lock();
        let _store_lock = self.lock_store()?;
        self.ensure_layout()?;

        let now = now_unix_millis();
        let case = InvestigationCase {
            id: Uuid::new_v4().to_string(),
            created_unix_ms: now,
            updated_unix_ms: now,
            title: input.title,
            problem_statement: input.problem_statement,
            workload_identity: input.workload_identity,
            target_environment: input.target_environment,
            run_ids: Vec::new(),
        };
        self.save_json(&self.case_path(&case.id), &case)?;
        Ok(case)
    }

    pub(crate) fn create_run(&self, input: NewInvestigationRun) -> Result<InvestigationRun> {
        let _guard = self.io_lock.lock();
        let _store_lock = self.lock_store()?;
        self.ensure_layout()?;

        let mut case = self.load_json::<InvestigationCase>(&self.case_path(&input.case_id))?;
        let now = now_unix_millis();
        let run = InvestigationRun {
            id: Uuid::new_v4().to_string(),
            case_id: input.case_id.clone(),
            session_id: input.session_id,
            created_unix_ms: now,
            fingerprint: input.fingerprint,
            profiling_windows: Vec::new(),
            report_ids: Vec::new(),
            timeline: Vec::new(),
        };

        case.updated_unix_ms = now;
        case.run_ids.push(run.id.clone());
        self.save_json(&self.case_path(&case.id), &case)?;
        self.save_json(&self.run_path(&run.id), &run)?;
        Ok(run)
    }

    pub(crate) fn append_timeline_entry(
        &self,
        run_id: &str,
        input: NewTimelineEntry,
    ) -> Result<TimelineEntry> {
        let _guard = self.io_lock.lock();
        let _store_lock = self.lock_store()?;
        self.ensure_layout()?;

        let mut run = self.load_json::<InvestigationRun>(&self.run_path(run_id))?;
        let log_path = self.timeline_log_path(run_id);
        let last_logged_sequence = load_last_timeline_entry(&log_path)?
            .map(|entry| entry.sequence)
            .unwrap_or(0);
        let last_sequence = run
            .timeline
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or(last_logged_sequence)
            .max(last_logged_sequence);
        let entry = TimelineEntry {
            sequence: last_sequence + 1,
            occurred_unix_ms: now_unix_millis(),
            source: input.source,
            kind: input.kind,
            title: input.title,
            body: input.body,
            actor: input.actor,
            reason: input.reason,
        };
        let needs_header_compaction = !run.timeline.is_empty() || !log_path.exists();
        if !run.timeline.is_empty() {
            let missing_from_log = first_sequence_after(&run.timeline, last_logged_sequence);
            append_timeline_entries(&log_path, &run.timeline[missing_from_log..])?;
        }
        append_timeline_entries(&log_path, std::slice::from_ref(&entry))?;
        if needs_header_compaction {
            run.timeline.clear();
            self.save_json(&self.run_path(run_id), &run)?;
        }
        Ok(entry)
    }

    pub(crate) fn append_profiling_window(
        &self,
        run_id: &str,
        input: NewProfilingWindow,
    ) -> Result<ProfilingWindowRecord> {
        let _guard = self.io_lock.lock();
        let _store_lock = self.lock_store()?;
        self.ensure_layout()?;

        let mut run = self.load_json::<InvestigationRun>(&self.run_path(run_id))?;
        let sequence = run
            .profiling_windows
            .last()
            .map(|window| window.sequence + 1)
            .unwrap_or(1);
        let window = ProfilingWindowRecord {
            window_id: input.window_id,
            sequence,
            occurred_unix_ms: input.occurred_unix_ms,
            capture_mode: input.capture_mode,
            preset: input.preset,
            requested_duration_ms: input.requested_duration_ms,
            actual_duration_ms: input.actual_duration_ms,
            sample_rate_hz: input.sample_rate_hz,
            estimated_cost: input.estimated_cost,
            status: input.status,
            artifact: input.artifact,
            error: input.error,
        };
        run.profiling_windows.push(window.clone());
        self.save_json(&self.run_path(run_id), &run)?;
        Ok(window)
    }

    pub(crate) fn append_report(
        &self,
        run_id: &str,
        input: NewInvestigationReport,
    ) -> Result<InvestigationReport> {
        let _guard = self.io_lock.lock();
        let _store_lock = self.lock_store()?;
        self.ensure_layout()?;

        let mut run = self.load_json::<InvestigationRun>(&self.run_path(run_id))?;
        let report = InvestigationReport {
            schema_version: INVESTIGATION_REPORT_SCHEMA_VERSION,
            id: Uuid::new_v4().to_string(),
            run_id: run_id.to_string(),
            case_id: run.case_id.clone(),
            created_unix_ms: now_unix_millis(),
            environment: input.environment,
            marker_timeline: input.marker_timeline,
            evidence_summary: input.evidence_summary,
            truncation: input.truncation,
            anomalies: input.anomalies,
            candidate_bottlenecks: input.candidate_bottlenecks,
            selected_evidence: input.selected_evidence,
            recommended_next_actions: input.recommended_next_actions,
            references: input.references,
        };
        let report_path = self.report_path(&report.id)?;
        self.save_json(&report_path, &report)?;
        run.report_ids.push(report.id.clone());
        if let Err(run_save_error) = self.save_json(&self.run_path(run_id), &run) {
            match fs::remove_file(&report_path) {
                Ok(()) => {}
                Err(cleanup_error) if cleanup_error.kind() == std::io::ErrorKind::NotFound => {}
                Err(cleanup_error) => {
                    return Err(anyhow!(
                        "failed to save run after writing report '{}'; additionally failed to remove orphaned report file '{}': {}",
                        report.id,
                        report_path.display(),
                        cleanup_error
                    ))
                    .context(run_save_error);
                }
            }
            return Err(run_save_error).context(
                "failed to save run after writing report; cleaned up orphaned report file",
            );
        }
        Ok(report)
    }

    pub(crate) fn load_case(&self, case_id: &str) -> Result<InvestigationCase> {
        let _guard = self.io_lock.lock();
        let _store_lock = self.lock_store()?;
        self.load_json(&self.case_path(case_id))
    }

    pub(crate) fn load_run(&self, run_id: &str) -> Result<InvestigationRun> {
        let _guard = self.io_lock.lock();
        let _store_lock = self.lock_store()?;
        let mut run = self.load_json::<InvestigationRun>(&self.run_path(run_id))?;
        let log_path = self.timeline_log_path(run_id);
        if log_path.exists() {
            run.timeline = merge_timeline_entries(run.timeline, load_timeline_entries(&log_path)?);
        }
        Ok(run)
    }

    pub(crate) fn load_report(&self, report_id: &str) -> Result<InvestigationReport> {
        let _guard = self.io_lock.lock();
        let _store_lock = self.lock_store()?;
        self.load_json(&self.report_path(report_id)?)
    }

    fn ensure_layout(&self) -> Result<()> {
        ensure_private_directory(&self.base_dir)?;
        ensure_private_directory(&self.cases_dir())?;
        ensure_private_directory(&self.runs_dir())?;
        ensure_private_directory(&self.reports_dir())?;

        let version_path = self.base_dir.join("store.json");
        if !version_path.exists() {
            self.save_json(
                &version_path,
                &serde_json::json!({ "version": INVESTIGATION_STORE_VERSION }),
            )?;
        }
        Ok(())
    }

    fn cases_dir(&self) -> PathBuf {
        self.base_dir.join("cases")
    }

    fn runs_dir(&self) -> PathBuf {
        self.base_dir.join("runs")
    }

    fn reports_dir(&self) -> PathBuf {
        self.base_dir.join("reports")
    }

    fn case_path(&self, case_id: &str) -> PathBuf {
        self.cases_dir().join(format!("{case_id}.json"))
    }

    fn run_path(&self, run_id: &str) -> PathBuf {
        self.runs_dir().join(format!("{run_id}.json"))
    }

    fn report_path(&self, report_id: &str) -> Result<PathBuf> {
        Ok(self
            .reports_dir()
            .join(format!("{}.json", canonical_report_id(report_id)?)))
    }

    fn timeline_log_path(&self, run_id: &str) -> PathBuf {
        self.runs_dir().join(format!("{run_id}.timeline.ndjson"))
    }

    fn load_json<T>(&self, path: &Path) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let bytes = fs::read(path)
            .with_context(|| format!("read investigation file '{}'", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parse investigation file '{}'", path.display()))
    }

    fn save_json<T>(&self, path: &Path, value: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        #[cfg(test)]
        if let Some(pattern) = test_save_json_failure_pattern()
            && path
                .file_name()
                .and_then(|file_name| file_name.to_str())
                .is_some_and(|file_name| file_name.contains(&pattern))
        {
            return Err(anyhow!(
                "synthetic save_json failure for '{}'",
                path.display()
            ));
        }
        if let Some(parent) = path.parent() {
            ensure_private_directory(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
            anyhow!("serialize investigation file '{}': {error}", path.display())
        })?;
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("investigation file '{}' has no parent", path.display()))?;
        let mut temp_file = NamedTempFile::new_in(parent).with_context(|| {
            format!(
                "create temporary investigation file in '{}'",
                parent.display()
            )
        })?;
        use std::io::Write;
        temp_file.write_all(&bytes).with_context(|| {
            format!(
                "write temporary investigation file for '{}'",
                path.display()
            )
        })?;
        temp_file.as_file().sync_all().with_context(|| {
            format!("sync temporary investigation file for '{}'", path.display())
        })?;
        temp_file
            .persist(path)
            .map_err(|error| error.error)
            .with_context(|| format!("persist investigation file '{}'", path.display()))?;
        ensure_private_file(path)?;
        sync_directory(parent)?;
        Ok(())
    }

    fn lock_store(&self) -> Result<StoreLockGuard> {
        ensure_private_directory(&self.base_dir)?;
        let lock_path = self.base_dir.join(".store.lock");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("open investigation lock '{}'", lock_path.display()))?;
        ensure_private_file(&lock_path)?;
        lock_file
            .lock_exclusive()
            .with_context(|| format!("lock investigation store '{}'", lock_path.display()))?;
        Ok(StoreLockGuard { lock_file })
    }
}

fn append_timeline_entries(path: &Path, entries: &[TimelineEntry]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("timeline log '{}' has no parent", path.display()))?;
    ensure_private_directory(parent)?;

    let mut file = if path.exists() {
        OpenOptions::new()
            .append(true)
            .open(path)
            .with_context(|| format!("open timeline log '{}'", path.display()))?
    } else {
        let temp_file = NamedTempFile::new_in(parent)
            .with_context(|| format!("create temporary timeline log in '{}'", parent.display()))?;
        let persisted = temp_file
            .persist(path)
            .map_err(|error| error.error)
            .with_context(|| format!("persist timeline log '{}'", path.display()))?;
        ensure_private_file(path)?;
        sync_directory(parent)?;
        persisted
    };
    ensure_private_file(path)?;

    for entry in entries {
        serde_json::to_writer(&mut file, entry)
            .with_context(|| format!("serialize timeline log entry '{}'", path.display()))?;
        use std::io::Write;
        file.write_all(b"\n")
            .with_context(|| format!("write timeline log separator '{}'", path.display()))?;
    }
    file.sync_all()
        .with_context(|| format!("sync timeline log '{}'", path.display()))?;
    Ok(())
}

fn load_timeline_entries(path: &Path) -> Result<Vec<TimelineEntry>> {
    let file =
        File::open(path).with_context(|| format!("open timeline log '{}'", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut entries = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("read timeline log line from '{}'", path.display()))?;
        if read == 0 {
            break;
        }
        let newline_terminated = matches!(line.last(), Some(b'\n'));
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice(&line) {
            Ok(entry) => entries.push(entry),
            Err(_error) if !newline_terminated => break,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("parse timeline log line from '{}'", path.display()));
            }
        }
    }
    Ok(entries)
}

fn load_last_timeline_entry(path: &Path) -> Result<Option<TimelineEntry>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(load_timeline_entries(path)?.into_iter().last())
}

fn first_sequence_after(entries: &[TimelineEntry], sequence: u64) -> usize {
    entries
        .iter()
        .position(|entry| entry.sequence > sequence)
        .unwrap_or(entries.len())
}

fn merge_timeline_entries(
    embedded_entries: Vec<TimelineEntry>,
    mut log_entries: Vec<TimelineEntry>,
) -> Vec<TimelineEntry> {
    if log_entries.is_empty() {
        return embedded_entries;
    }

    let last_logged_sequence = log_entries.last().map(|entry| entry.sequence).unwrap_or(0);
    log_entries.extend(
        embedded_entries
            .into_iter()
            .filter(|entry| entry.sequence > last_logged_sequence),
    );
    log_entries
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_else(|_| 1)
}

struct StoreLockGuard {
    lock_file: File,
}

impl Drop for StoreLockGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock_file);
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    if let Ok(dir) = File::open(path) {
        let _ = dir.sync_all();
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn default_store_dir() -> Result<PathBuf> {
    #[cfg(test)]
    if let Some(path) = TEST_BASE_DIR
        .get_or_init(|| Mutex::new(None))
        .lock()
        .clone()
    {
        return Ok(path);
    }

    let home = home::home_dir().ok_or_else(|| anyhow!("Could not find home directory"))?;
    let dir = home.join(".studiofs").join("investigations");
    ensure_private_directory(&dir)?;
    Ok(dir)
}

fn canonical_report_id(report_id: &str) -> Result<String> {
    Uuid::parse_str(report_id)
        .map(|uuid| uuid.to_string())
        .map_err(|error| anyhow!("invalid report id '{}': {error}", report_id))
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("create investigation directory '{}'", path.display()))?;
    set_private_directory_permissions(path)?;
    Ok(())
}

fn ensure_private_file(path: &Path) -> Result<()> {
    if path.exists() {
        set_private_file_permissions(path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("restrict directory permissions for '{}'", path.display()))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict file permissions for '{}'", path.display()))
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
pub(crate) fn set_test_store_base_dir(path: Option<&Path>) {
    *TEST_BASE_DIR.get_or_init(|| Mutex::new(None)).lock() = path.map(Path::to_path_buf);
}

#[cfg(test)]
fn test_save_json_failure_pattern() -> Option<String> {
    TEST_SAVE_JSON_FAILURE_PATTERN
        .get_or_init(|| Mutex::new(None))
        .lock()
        .clone()
}

#[cfg(test)]
pub(crate) fn set_test_save_json_failure_pattern(pattern: Option<&str>) {
    *TEST_SAVE_JSON_FAILURE_PATTERN
        .get_or_init(|| Mutex::new(None))
        .lock() = pattern.map(str::to_string);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn unix_mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;

        fs::metadata(path).expect("metadata").permissions().mode() & 0o777
    }

    fn sample_fingerprint() -> EnvironmentFingerprint {
        EnvironmentFingerprint {
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
                volume_uuid: "volume-uuid".to_string(),
                mount_point: "Q:".to_string(),
                cache_path: "C:/cache".to_string(),
                read_only: false,
                cache_size: Some("64GB".to_string()),
                endpoint_override: Some("http://127.0.0.1:9000".to_string()),
                snapshot_id: None,
                vfs_config: serde_json::json!({
                    "cache_safety_mode": "fast",
                    "principal": "*"
                }),
            },
            workload_identity: WorkloadIdentityFingerprint {
                case_workload: Some("aja".to_string()),
                run_workload: Some("copy-pass-1".to_string()),
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
                protocol_version: crate::diagnostics::DIAGNOSTICS_PROTOCOL_VERSION,
                default_lease_ttl_ms: 5_000,
                max_lease_ttl_ms: 60_000,
                max_connections: 16,
            },
        }
    }

    #[test]
    fn store_persists_case_run_and_timeline_across_reopen() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "AJA copy slowdown".to_string(),
                problem_statement: Some("Copy stalls after the warmup window".to_string()),
                workload_identity: Some("aja".to_string()),
                target_environment: Some("artist workstation".to_string()),
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id.clone(),
                session_id: "session-123".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");
        store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Cli,
                    kind: TimelineEntryKind::Marker,
                    title: "start copy".to_string(),
                    body: Some("Started AJA foreground copy".to_string()),
                    actor: Some("developer".to_string()),
                    reason: Some("begin workload".to_string()),
                },
            )
            .expect("append entry");

        let reopened = InvestigationStore::new(store.base_dir().to_path_buf());
        let persisted_case = reopened.load_case(&case.id).expect("load case");
        let persisted_run = reopened.load_run(&run.id).expect("load run");

        assert_eq!(persisted_case.run_ids, vec![run.id.clone()]);
        assert_eq!(persisted_run.case_id, case.id);
        assert_eq!(persisted_run.session_id, "session-123");
        assert_eq!(persisted_run.timeline.len(), 1);
        assert_eq!(persisted_run.timeline[0].title, "start copy");
    }

    #[test]
    fn store_orders_mixed_source_timeline_entries_by_sequence() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "Interactive timeline".to_string(),
                problem_statement: None,
                workload_identity: Some("gui-open".to_string()),
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id,
                session_id: "session-456".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");

        let first = store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Cli,
                    kind: TimelineEntryKind::Marker,
                    title: "open timeline".to_string(),
                    body: None,
                    actor: Some("developer".to_string()),
                    reason: None,
                },
            )
            .expect("append cli marker");
        let second = store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Mcp,
                    kind: TimelineEntryKind::Note,
                    title: "observed cache miss burst".to_string(),
                    body: Some("MCP adapter captured a foreground-read spike".to_string()),
                    actor: Some("studiofs-dev-mcp".to_string()),
                    reason: Some("annotate anomaly".to_string()),
                },
            )
            .expect("append mcp note");

        let persisted_run = store.load_run(&run.id).expect("load run");

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        assert_eq!(persisted_run.timeline[0].source, TimelineEntrySource::Cli);
        assert_eq!(persisted_run.timeline[1].source, TimelineEntrySource::Mcp);
        assert_eq!(persisted_run.timeline[0].sequence, 1);
        assert_eq!(persisted_run.timeline[1].sequence, 2);
    }

    #[test]
    fn loud_safe_action_audit_entries_round_trip_with_action_kind() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "safe action".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id,
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");

        let entry = store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Mcp,
                    kind: TimelineEntryKind::Action,
                    title: "reset_counters".to_string(),
                    body: Some("reset upload/cache counters".to_string()),
                    actor: Some("studiofs-dev-mcp".to_string()),
                    reason: Some("baseline before rerun".to_string()),
                },
            )
            .expect("append action");

        assert_eq!(entry.kind, TimelineEntryKind::Action);
        assert_eq!(
            store.load_run(&run.id).expect("load run").timeline[0],
            entry
        );
    }

    #[test]
    fn store_persists_timeline_in_append_only_log() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "AJA ingest".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id,
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");

        store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Cli,
                    kind: TimelineEntryKind::Marker,
                    title: "first".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("append first entry");
        store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Mcp,
                    kind: TimelineEntryKind::Note,
                    title: "second".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("append second entry");

        let stored_run: InvestigationRun = store
            .load_json(&store.run_path(&run.id))
            .expect("load stored header");
        let timeline_log =
            fs::read_to_string(store.timeline_log_path(&run.id)).expect("read timeline log");
        let persisted_run = store.load_run(&run.id).expect("load run");

        assert!(stored_run.timeline.is_empty());
        assert_eq!(timeline_log.lines().count(), 2);
        assert_eq!(persisted_run.timeline.len(), 2);
        assert_eq!(persisted_run.timeline[0].sequence, 1);
        assert_eq!(persisted_run.timeline[1].sequence, 2);
    }

    #[test]
    fn store_migrates_legacy_embedded_timeline_to_log_on_append() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "AJA ingest".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = InvestigationRun {
            id: Uuid::new_v4().to_string(),
            case_id: case.id,
            session_id: "session-legacy".to_string(),
            created_unix_ms: 1,
            fingerprint: sample_fingerprint(),
            profiling_windows: Vec::new(),
            report_ids: Vec::new(),
            timeline: vec![TimelineEntry {
                sequence: 1,
                occurred_unix_ms: 1,
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "legacy".to_string(),
                body: None,
                actor: None,
                reason: None,
            }],
        };
        store.ensure_layout().expect("ensure layout");
        store
            .save_json(&store.run_path(&run.id), &run)
            .expect("save legacy run");

        let appended = store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Mcp,
                    kind: TimelineEntryKind::Note,
                    title: "new".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("append migrated entry");
        let stored_run: InvestigationRun = store
            .load_json(&store.run_path(&run.id))
            .expect("load stored header");
        let persisted_run = store.load_run(&run.id).expect("load run");

        assert_eq!(appended.sequence, 2);
        assert!(stored_run.timeline.is_empty());
        assert_eq!(persisted_run.timeline.len(), 2);
        assert_eq!(persisted_run.timeline[0].title, "legacy");
        assert_eq!(persisted_run.timeline[1].title, "new");
    }

    #[test]
    fn store_migrates_legacy_embedded_timeline_when_log_exists_but_is_empty() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "AJA ingest".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = InvestigationRun {
            id: Uuid::new_v4().to_string(),
            case_id: case.id,
            session_id: "session-partial".to_string(),
            created_unix_ms: 1,
            fingerprint: sample_fingerprint(),
            profiling_windows: Vec::new(),
            report_ids: Vec::new(),
            timeline: vec![TimelineEntry {
                sequence: 1,
                occurred_unix_ms: 1,
                source: TimelineEntrySource::Cli,
                kind: TimelineEntryKind::Marker,
                title: "legacy".to_string(),
                body: None,
                actor: None,
                reason: None,
            }],
        };
        store.ensure_layout().expect("ensure layout");
        store
            .save_json(&store.run_path(&run.id), &run)
            .expect("save legacy run");
        File::create(store.timeline_log_path(&run.id)).expect("create empty log");

        let appended = store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Mcp,
                    kind: TimelineEntryKind::Note,
                    title: "new".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("append migrated entry");
        let persisted_run = store.load_run(&run.id).expect("load run");

        assert_eq!(appended.sequence, 2);
        assert_eq!(persisted_run.timeline.len(), 2);
        assert_eq!(persisted_run.timeline[0].title, "legacy");
        assert_eq!(persisted_run.timeline[1].title, "new");
    }

    #[test]
    fn store_ignores_truncated_final_timeline_line() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "AJA ingest".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id,
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");

        store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Cli,
                    kind: TimelineEntryKind::Marker,
                    title: "first".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("append first");

        use std::io::Write;
        let mut file = OpenOptions::new()
            .append(true)
            .open(store.timeline_log_path(&run.id))
            .expect("open timeline log");
        file.write_all(br#"{"sequence":2,"title":"partial"#)
            .expect("write partial");
        file.sync_all().expect("sync partial");

        let persisted_run = store.load_run(&run.id).expect("load run");
        let second = store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Mcp,
                    kind: TimelineEntryKind::Note,
                    title: "second".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("append second");

        assert_eq!(persisted_run.timeline.len(), 1);
        assert_eq!(persisted_run.timeline[0].title, "first");
        assert_eq!(second.sequence, 2);
    }

    #[test]
    fn append_profiling_window_persists_capture_metadata() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "profiling".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id.clone(),
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");

        let window = store
            .append_profiling_window(
                &run.id,
                NewProfilingWindow {
                    window_id: "window-1".to_string(),
                    occurred_unix_ms: 1_111,
                    capture_mode: ProfilingCaptureMode::WallBlocking,
                    preset: DiagnosticsPreset::Balanced,
                    requested_duration_ms: 1_500,
                    actual_duration_ms: Some(1_250),
                    sample_rate_hz: Some(97),
                    estimated_cost: ProfilingCostEstimate {
                        score: 2,
                        label: "balanced".to_string(),
                    },
                    status: ProfilingWindowStatus::Completed,
                    artifact: Some(ProfileArtifactRecord {
                        kind: "pprof".to_string(),
                        path: "profiles/demo.pb.gz".to_string(),
                        bytes: 1_024,
                    }),
                    error: None,
                },
            )
            .expect("append profiling window");

        assert_eq!(window.sequence, 1);
        let reloaded = store.load_run(&run.id).expect("reload run");
        assert_eq!(reloaded.profiling_windows.len(), 1);
        assert_eq!(
            reloaded.profiling_windows[0].capture_mode,
            ProfilingCaptureMode::WallBlocking
        );
        assert_eq!(
            reloaded.profiling_windows[0]
                .artifact
                .as_ref()
                .map(|artifact| artifact.bytes),
            Some(1_024)
        );
    }

    #[test]
    fn append_profiling_window_keeps_fail_open_failure_metadata() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "profiling".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id.clone(),
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");

        let window = store
            .append_profiling_window(
                &run.id,
                NewProfilingWindow {
                    window_id: "window-2".to_string(),
                    occurred_unix_ms: 2_222,
                    capture_mode: ProfilingCaptureMode::Cpu,
                    preset: DiagnosticsPreset::Minimal,
                    requested_duration_ms: 5_000,
                    actual_duration_ms: None,
                    sample_rate_hz: Some(100),
                    estimated_cost: ProfilingCostEstimate {
                        score: 1,
                        label: "minimal".to_string(),
                    },
                    status: ProfilingWindowStatus::Unsupported,
                    artifact: None,
                    error: Some("profiling backend unavailable".to_string()),
                },
            )
            .expect("append profiling window");

        assert_eq!(window.status, ProfilingWindowStatus::Unsupported);
        assert_eq!(
            window.error.as_deref(),
            Some("profiling backend unavailable")
        );
        let reloaded = store.load_run(&run.id).expect("reload run");
        assert_eq!(reloaded.profiling_windows.len(), 1);
        assert_eq!(
            reloaded.profiling_windows[0].error.as_deref(),
            Some("profiling backend unavailable")
        );
    }

    #[test]
    fn append_profiling_window_preserves_provided_occurrence_timestamp() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "profiling".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id.clone(),
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");

        let window = store
            .append_profiling_window(
                &run.id,
                NewProfilingWindow {
                    window_id: "window-3".to_string(),
                    occurred_unix_ms: 123_456,
                    capture_mode: ProfilingCaptureMode::WallBlocking,
                    preset: DiagnosticsPreset::Balanced,
                    requested_duration_ms: 1_000,
                    actual_duration_ms: Some(950),
                    sample_rate_hz: Some(50),
                    estimated_cost: ProfilingCostEstimate {
                        score: 2,
                        label: "balanced".to_string(),
                    },
                    status: ProfilingWindowStatus::Completed,
                    artifact: None,
                    error: None,
                },
            )
            .expect("append profiling window");

        assert_eq!(window.occurred_unix_ms, 123_456);
        let reloaded = store.load_run(&run.id).expect("reload run");
        assert_eq!(reloaded.profiling_windows[0].occurred_unix_ms, 123_456);
    }

    #[test]
    fn store_persists_report_artifact_and_links_it_from_run() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "single-run report".to_string(),
                problem_statement: Some("Need a durable artifact".to_string()),
                workload_identity: Some("aja".to_string()),
                target_environment: Some("artist workstation".to_string()),
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id.clone(),
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");
        let report = store
            .append_report(
                &run.id,
                NewInvestigationReport {
                    environment: sample_fingerprint(),
                    marker_timeline: vec![TimelineEntry {
                        sequence: 1,
                        occurred_unix_ms: 1_000,
                        source: TimelineEntrySource::Cli,
                        kind: TimelineEntryKind::Marker,
                        title: "start copy".to_string(),
                        body: Some("Begin workload".to_string()),
                        actor: Some("developer".to_string()),
                        reason: Some("baseline".to_string()),
                    }],
                    evidence_summary: InvestigationReportEvidenceSummary {
                        recent_event_count: 2,
                        timeline_entry_count: 3,
                        profiling_window_count: 1,
                        queue_growth_count: 1,
                        blocking_interval_count: 1,
                        hotspot_count: 1,
                    },
                    truncation: InvestigationReportTruncationSummary::default(),
                    anomalies: vec![InvestigationReportFinding {
                        title: "Upload queue growth".to_string(),
                        summary: "Pending uploads grew across the workload span.".to_string(),
                        supporting_evidence: vec![crate::diagnostics::DiagnosticsEvidenceReference {
                            source: crate::diagnostics::DiagnosticsEvidenceReferenceSource::RecentEvent,
                            id: "2".to_string(),
                            field: Some("pending_uploads".to_string()),
                        }],
                    }],
                    candidate_bottlenecks: vec![InvestigationReportFinding {
                        title: "sync_data queue wait".to_string(),
                        summary: "Write-path queue wait dominated the captured profile.".to_string(),
                        supporting_evidence: vec![crate::diagnostics::DiagnosticsEvidenceReference {
                            source: crate::diagnostics::DiagnosticsEvidenceReferenceSource::ProfilingArtifact,
                            id: "profiles/window-1.json".to_string(),
                            field: Some(
                                "end_snapshot.write_path.block_store_sync_data.queue_wait_total_ms"
                                    .to_string(),
                            ),
                        }],
                    }],
                    selected_evidence: vec![InvestigationReportSelectedEvidence {
                        label: "Runtime snapshot".to_string(),
                        summary: "Captured upload, write-path, cache, and synchronization sections."
                            .to_string(),
                        supporting_evidence: vec![crate::diagnostics::DiagnosticsEvidenceReference {
                            source: crate::diagnostics::DiagnosticsEvidenceReferenceSource::RuntimeSnapshot,
                            id: "runtime_snapshot".to_string(),
                            field: Some("write_path".to_string()),
                        }],
                    }],
                    recommended_next_actions: vec![InvestigationRecommendedNextAction {
                        title: "Inspect sync_data ownership".to_string(),
                        rationale:
                            "Follow the write-path queue wait evidence before proposing a code change."
                                .to_string(),
                        supporting_evidence: vec![crate::diagnostics::DiagnosticsEvidenceReference {
                            source: crate::diagnostics::DiagnosticsEvidenceReferenceSource::ProfilingWindow,
                            id: "window-1".to_string(),
                            field: None,
                        }],
                    }],
                    references: InvestigationReportReferences {
                        runtime_snapshot: crate::diagnostics::DiagnosticsRuntimeSnapshot::default(),
                        derived_view: crate::diagnostics::DiagnosticsEvidenceView {
                            generated_unix_ms: 2_000,
                            run_id: run.id.clone(),
                            scope: crate::diagnostics::DiagnosticsEvidenceScopeResolution::default(),
                            budget: crate::diagnostics::DiagnosticsEvidenceBudgetReport {
                                requested: crate::diagnostics::DiagnosticsEvidenceBudget::default(),
                                applied: crate::diagnostics::DiagnosticsEvidenceBudget::default(),
                                truncation:
                                    crate::diagnostics::DiagnosticsEvidenceTruncation::default(),
                            },
                            raw: crate::diagnostics::DiagnosticsEvidenceRawView::default(),
                            derived: crate::diagnostics::DiagnosticsDerivedEvidenceView::default(),
                        },
                        profiling_windows: vec![ProfilingWindowRecord {
                            window_id: "window-1".to_string(),
                            sequence: 1,
                            occurred_unix_ms: 1_500,
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
                            artifact: Some(ProfileArtifactRecord {
                                kind: "wall_blocking_summary".to_string(),
                                path: "profiles/window-1.json".to_string(),
                                bytes: 512,
                            }),
                            error: None,
                        }],
                    },
                },
            )
            .expect("append report");

        let reopened = InvestigationStore::new(store.base_dir().to_path_buf());
        let reloaded_report = reopened.load_report(&report.id).expect("load report");
        let reloaded_run = reopened.load_run(&run.id).expect("load run");

        assert_eq!(reloaded_run.report_ids, vec![report.id.clone()]);
        assert_eq!(reloaded_report.run_id, run.id);
        assert_eq!(reloaded_report.case_id, case.id);
        assert_eq!(reloaded_report.marker_timeline.len(), 1);
        assert_eq!(reloaded_report.evidence_summary.hotspot_count, 1);
        assert_eq!(reloaded_report.candidate_bottlenecks.len(), 1);
        assert_eq!(reloaded_report.references.profiling_windows.len(), 1);
    }

    #[test]
    fn load_report_rejects_non_uuid_ids() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));

        let error = store
            .load_report("../outside-store")
            .expect_err("invalid report id should be rejected");

        assert!(error.to_string().contains("invalid report id"));
    }

    #[test]
    fn append_report_cleans_up_orphaned_report_when_run_save_fails() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "cleanup".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id,
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");

        set_test_save_json_failure_pattern(Some(&format!("{}.json", run.id)));
        let result = store.append_report(
            &run.id,
            NewInvestigationReport {
                environment: sample_fingerprint(),
                marker_timeline: Vec::new(),
                evidence_summary: InvestigationReportEvidenceSummary {
                    recent_event_count: 0,
                    timeline_entry_count: 0,
                    profiling_window_count: 0,
                    queue_growth_count: 0,
                    blocking_interval_count: 0,
                    hotspot_count: 0,
                },
                truncation: InvestigationReportTruncationSummary::default(),
                anomalies: Vec::new(),
                candidate_bottlenecks: Vec::new(),
                selected_evidence: Vec::new(),
                recommended_next_actions: Vec::new(),
                references: InvestigationReportReferences {
                    runtime_snapshot: crate::diagnostics::DiagnosticsRuntimeSnapshot::default(),
                    derived_view: crate::diagnostics::DiagnosticsEvidenceView {
                        generated_unix_ms: 0,
                        run_id: run.id.clone(),
                        scope: crate::diagnostics::DiagnosticsEvidenceScopeResolution::default(),
                        budget: crate::diagnostics::DiagnosticsEvidenceBudgetReport {
                            requested: crate::diagnostics::DiagnosticsEvidenceBudget::default(),
                            applied: crate::diagnostics::DiagnosticsEvidenceBudget::default(),
                            truncation: crate::diagnostics::DiagnosticsEvidenceTruncation::default(
                            ),
                        },
                        raw: crate::diagnostics::DiagnosticsEvidenceRawView::default(),
                        derived: crate::diagnostics::DiagnosticsDerivedEvidenceView::default(),
                    },
                    profiling_windows: Vec::new(),
                },
            },
        );
        set_test_save_json_failure_pattern(None);

        let error = result.expect_err("run save failure should abort report append");
        assert!(
            error
                .to_string()
                .contains("cleaned up orphaned report file")
        );
        let report_files = fs::read_dir(store.reports_dir())
            .expect("read reports dir")
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        assert!(
            report_files.is_empty(),
            "orphaned report file should be removed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn store_restricts_permissions_for_directories_and_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = InvestigationStore::new(temp.path().join("investigations"));
        let case = store
            .create_case(NewInvestigationCase {
                title: "AJA ingest".to_string(),
                problem_statement: None,
                workload_identity: None,
                target_environment: None,
            })
            .expect("create case");
        let run = store
            .create_run(NewInvestigationRun {
                case_id: case.id,
                session_id: "session-1".to_string(),
                fingerprint: sample_fingerprint(),
            })
            .expect("create run");
        store
            .append_timeline_entry(
                &run.id,
                NewTimelineEntry {
                    source: TimelineEntrySource::Cli,
                    kind: TimelineEntryKind::Marker,
                    title: "first".to_string(),
                    body: None,
                    actor: None,
                    reason: None,
                },
            )
            .expect("append timeline");

        assert_eq!(unix_mode(store.base_dir()), 0o700);
        assert_eq!(unix_mode(&store.cases_dir()), 0o700);
        assert_eq!(unix_mode(&store.runs_dir()), 0o700);
        assert_eq!(unix_mode(&store.base_dir().join(".store.lock")), 0o600);
        assert_eq!(unix_mode(&store.base_dir().join("store.json")), 0o600);
        assert_eq!(unix_mode(&store.timeline_log_path(&run.id)), 0o600);
    }
}

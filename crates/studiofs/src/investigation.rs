use anyhow::{Context, Result, anyhow};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[cfg(test)]
use std::sync::OnceLock;

pub(crate) const INVESTIGATION_STORE_VERSION: u32 = 1;

#[cfg(test)]
static TEST_BASE_DIR: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

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
pub(crate) enum TimelineEntrySource {
    Cli,
    Mcp,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TimelineEntryKind {
    Marker,
    Note,
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
        self.ensure_layout()?;

        let mut case = self.load_json::<InvestigationCase>(&self.case_path(&input.case_id))?;
        let now = now_unix_millis();
        let run = InvestigationRun {
            id: Uuid::new_v4().to_string(),
            case_id: input.case_id.clone(),
            session_id: input.session_id,
            created_unix_ms: now,
            fingerprint: input.fingerprint,
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
        self.ensure_layout()?;

        let mut run = self.load_json::<InvestigationRun>(&self.run_path(run_id))?;
        let entry = TimelineEntry {
            sequence: run
                .timeline
                .last()
                .map(|entry| entry.sequence + 1)
                .unwrap_or(1),
            occurred_unix_ms: now_unix_millis(),
            source: input.source,
            kind: input.kind,
            title: input.title,
            body: input.body,
            actor: input.actor,
            reason: input.reason,
        };
        run.timeline.push(entry.clone());
        self.save_json(&self.run_path(run_id), &run)?;
        Ok(entry)
    }

    pub(crate) fn load_case(&self, case_id: &str) -> Result<InvestigationCase> {
        let _guard = self.io_lock.lock();
        self.load_json(&self.case_path(case_id))
    }

    pub(crate) fn load_run(&self, run_id: &str) -> Result<InvestigationRun> {
        let _guard = self.io_lock.lock();
        self.load_json(&self.run_path(run_id))
    }

    fn ensure_layout(&self) -> Result<()> {
        fs::create_dir_all(self.cases_dir()).with_context(|| {
            format!(
                "create investigation cases directory '{}'",
                self.cases_dir().display()
            )
        })?;
        fs::create_dir_all(self.runs_dir()).with_context(|| {
            format!(
                "create investigation runs directory '{}'",
                self.runs_dir().display()
            )
        })?;

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

    fn case_path(&self, case_id: &str) -> PathBuf {
        self.cases_dir().join(format!("{case_id}.json"))
    }

    fn run_path(&self, run_id: &str) -> PathBuf {
        self.runs_dir().join(format!("{run_id}.json"))
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
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("create investigation directory '{}'", parent.display())
            })?;
        }
        let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
            anyhow!("serialize investigation file '{}': {error}", path.display())
        })?;
        fs::write(path, bytes)
            .with_context(|| format!("write investigation file '{}'", path.display()))
    }
}

fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
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
    fs::create_dir_all(&dir)
        .with_context(|| format!("create investigations dir '{}'", dir.display()))?;
    Ok(dir)
}

#[cfg(test)]
pub(crate) fn set_test_store_base_dir(path: Option<&Path>) {
    *TEST_BASE_DIR.get_or_init(|| Mutex::new(None)).lock() = path.map(Path::to_path_buf);
}

#[cfg(test)]
mod tests {
    use super::*;

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
                protocol_version: INVESTIGATION_STORE_VERSION,
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
}

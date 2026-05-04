//! Deterministic crash/restart and replay simulation suite.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufReader, BufWriter, Write as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::client::SimulatedRuntime;
use crate::invariant::{InvariantViolation, LogicalSnapshot, validate_logical_invariants};
use crate::scheduler::{
    ScheduledEvent, SchedulerEventType, SchedulerTraceRecord, SimulationScheduler,
    simulation_stream_seed,
};
use crate::{ScenarioOperation, ScenarioOperationRecord};

pub const CRASH_REPLAY_COUNTEREXAMPLE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplaySuiteConfig {
    pub scenario: String,
    pub seeds: Vec<u64>,
    pub families: Vec<CrashReplayFamily>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrashReplayFamily {
    CrashAfterDurabilityBeforeUploadCompletion,
    CrashAfterUploadBeforeMetadataPublication,
    CrashDuringPublishAttempt,
    CrashDuringForeignApply,
    RestartWithPendingLocalUploadState,
    RepeatedRestartReplayLoops,
    StalePendingUploadEventsRaceNewerEpochs,
    MemoryOnlyDirtyCrash,
    InjectedInvariantFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplaySuiteReport {
    pub scenario: String,
    pub passed: bool,
    pub family_reports: Vec<CrashReplayFamilyReport>,
    pub failures: Vec<CrashReplayFailure>,
}

impl CrashReplaySuiteReport {
    pub fn total_runs(&self) -> usize {
        self.family_reports
            .iter()
            .map(|family| family.seed_reports.len())
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplayFamilyReport {
    pub family: CrashReplayFamily,
    pub seed_reports: Vec<CrashReplaySeedReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplaySeedReport {
    pub seed: u64,
    pub passed: bool,
    pub invariant_checkpoints: usize,
    pub replay_idempotent: bool,
    pub peer_converged: bool,
    pub trace: Vec<CrashReplayTraceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplayFailure {
    pub scenario: String,
    pub family: CrashReplayFamily,
    pub seed: u64,
    pub violated_invariant: String,
    pub message: String,
    pub trace: Vec<CrashReplayTraceRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counterexample_artifact_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplayTraceRecord {
    pub step: u64,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplayCounterexampleArtifact {
    pub schema_version: u32,
    pub scenario: String,
    pub family: CrashReplayFamily,
    pub seed: u64,
    pub scheduler_trace: Vec<SchedulerTraceRecord>,
    pub operation_log: Vec<ScenarioOperationRecord>,
    pub client_trace: Vec<CrashReplayTraceRecord>,
    pub failure: CrashReplayFailureDetails,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_snapshot: Option<LogicalSnapshot>,
    pub replay_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplayFailureDetails {
    pub violated_invariant: String,
    pub message: String,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplayReplayReport {
    pub reproduced: bool,
    pub scenario: String,
    pub family: CrashReplayFamily,
    pub seed: u64,
    pub violated_invariant: String,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum CrashReplayArtifactError {
    #[error("counterexample artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("counterexample artifact JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported counterexample artifact schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("counterexample artifact path is not valid UTF-8: {path:?}")]
    InvalidArtifactPath { path: PathBuf },
    #[error("counterexample artifact writes failed: {errors:?}")]
    ArtifactWriteFailures {
        errors: Vec<CrashReplayArtifactWriteFailure>,
    },
    #[error("counterexample replay did not reproduce: {0}")]
    ReplayMismatch(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrashReplayArtifactWriteFailure {
    pub scenario: String,
    pub family: CrashReplayFamily,
    pub seed: u64,
    pub path: PathBuf,
    pub error: String,
}

pub fn run_crash_replay_suite(config: CrashReplaySuiteConfig) -> CrashReplaySuiteReport {
    let mut family_reports = Vec::new();
    let mut failures = Vec::new();

    for family in config.families {
        let mut seed_reports = Vec::new();
        for seed in &config.seeds {
            let run = run_family(&config.scenario, family, *seed);
            if let Some(failure) = run.failure {
                failures.push(failure);
            }
            seed_reports.push(run.report);
        }
        family_reports.push(CrashReplayFamilyReport {
            family,
            seed_reports,
        });
    }

    CrashReplaySuiteReport {
        scenario: config.scenario,
        passed: failures.is_empty(),
        family_reports,
        failures,
    }
}

pub fn run_crash_replay_suite_with_artifacts(
    config: CrashReplaySuiteConfig,
    artifact_root: impl AsRef<Path>,
) -> Result<CrashReplaySuiteReport, CrashReplayArtifactError> {
    let artifact_root = artifact_root.as_ref();
    let mut family_reports = Vec::new();
    let mut failures = Vec::new();
    let mut artifact_write_errors = Vec::new();

    for family in config.families {
        let mut seed_reports = Vec::new();
        for seed in &config.seeds {
            let mut run = run_family(&config.scenario, family, *seed);
            if run.failure.is_some() {
                let artifact_path =
                    counterexample_artifact_path(artifact_root, &config.scenario, family, *seed);
                match run
                    .counterexample_artifact(&artifact_path)
                    .and_then(|artifact| {
                        write_crash_replay_counterexample(&artifact_path, &artifact)
                    }) {
                    Ok(()) => {
                        if let Some(failure) = &mut run.failure {
                            failure.counterexample_artifact_path =
                                Some(artifact_path_utf8(&artifact_path)?.to_string());
                        }
                    }
                    Err(error) => artifact_write_errors.push(CrashReplayArtifactWriteFailure {
                        scenario: config.scenario.clone(),
                        family,
                        seed: *seed,
                        path: artifact_path,
                        error: error.to_string(),
                    }),
                }
            }
            if let Some(failure) = run.failure {
                failures.push(failure);
            }
            seed_reports.push(run.report);
        }
        family_reports.push(CrashReplayFamilyReport {
            family,
            seed_reports,
        });
    }

    if !artifact_write_errors.is_empty() {
        return Err(CrashReplayArtifactError::ArtifactWriteFailures {
            errors: artifact_write_errors,
        });
    }

    Ok(CrashReplaySuiteReport {
        scenario: config.scenario,
        passed: failures.is_empty(),
        family_reports,
        failures,
    })
}

pub fn write_crash_replay_counterexample(
    path: impl AsRef<Path>,
    artifact: &CrashReplayCounterexampleArtifact,
) -> Result<(), CrashReplayArtifactError> {
    if let Some(parent) = path.as_ref().parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::File::create(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, artifact)?;
    writer.flush()?;
    Ok(())
}

pub fn read_crash_replay_counterexample(
    path: impl AsRef<Path>,
) -> Result<CrashReplayCounterexampleArtifact, CrashReplayArtifactError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let artifact: CrashReplayCounterexampleArtifact = serde_json::from_reader(reader)?;
    if artifact.schema_version != CRASH_REPLAY_COUNTEREXAMPLE_SCHEMA_VERSION {
        return Err(CrashReplayArtifactError::UnsupportedSchemaVersion(
            artifact.schema_version,
        ));
    }
    Ok(artifact)
}

pub fn replay_crash_replay_counterexample(
    path: impl AsRef<Path>,
) -> Result<CrashReplayReplayReport, CrashReplayArtifactError> {
    let artifact = read_crash_replay_counterexample(path)?;
    let run = run_family(&artifact.scenario, artifact.family, artifact.seed);
    let Some(failure) = run.failure else {
        return Err(CrashReplayArtifactError::ReplayMismatch(
            "saved counterexample passed during replay".to_string(),
        ));
    };
    let expected_trace = artifact.client_trace;
    if failure.violated_invariant != artifact.failure.violated_invariant {
        return Err(CrashReplayArtifactError::ReplayMismatch(format!(
            "expected invariant {}, got {}",
            artifact.failure.violated_invariant, failure.violated_invariant
        )));
    }
    if run.scheduler_trace != artifact.scheduler_trace {
        return Err(CrashReplayArtifactError::ReplayMismatch(
            "scheduler trace differed from artifact".to_string(),
        ));
    }
    if run.operation_log != artifact.operation_log {
        return Err(CrashReplayArtifactError::ReplayMismatch(
            "operation log differed from artifact".to_string(),
        ));
    }
    if failure.trace != expected_trace {
        return Err(CrashReplayArtifactError::ReplayMismatch(
            "client trace differed from artifact".to_string(),
        ));
    }

    Ok(CrashReplayReplayReport {
        reproduced: true,
        scenario: artifact.scenario,
        family: artifact.family,
        seed: artifact.seed,
        violated_invariant: failure.violated_invariant,
        message: failure.message,
    })
}

struct FamilyRun {
    report: CrashReplaySeedReport,
    failure: Option<CrashReplayFailure>,
    scheduler_trace: Vec<SchedulerTraceRecord>,
    operation_log: Vec<ScenarioOperationRecord>,
    final_snapshot: LogicalSnapshot,
}

impl FamilyRun {
    fn counterexample_artifact(
        &self,
        artifact_path: &Path,
    ) -> Result<CrashReplayCounterexampleArtifact, CrashReplayArtifactError> {
        let failure = self
            .failure
            .as_ref()
            .expect("counterexample artifact requires a failure");
        Ok(CrashReplayCounterexampleArtifact {
            schema_version: CRASH_REPLAY_COUNTEREXAMPLE_SCHEMA_VERSION,
            scenario: failure.scenario.clone(),
            family: failure.family,
            seed: failure.seed,
            scheduler_trace: self.scheduler_trace.clone(),
            operation_log: self.operation_log.clone(),
            client_trace: failure.trace.clone(),
            failure: CrashReplayFailureDetails {
                violated_invariant: failure.violated_invariant.clone(),
                message: failure.message.clone(),
                diagnostics: failure
                    .trace
                    .iter()
                    .filter(|record| record.kind == "invariant_failed")
                    .map(|record| {
                        format!(
                            "step={} client={} path={}",
                            record.step,
                            record.client.as_deref().unwrap_or("<none>"),
                            record.path.as_deref().unwrap_or("<none>")
                        )
                    })
                    .collect(),
            },
            final_snapshot: Some(self.final_snapshot.clone()),
            replay_command: format!(
                "cargo run -p studiofs-scenario --bin studiofs-scenario -- replay-counterexample \"{}\"",
                artifact_path_utf8(artifact_path)?
            ),
        })
    }
}

fn artifact_path_utf8(path: &Path) -> Result<&str, CrashReplayArtifactError> {
    path.to_str()
        .ok_or_else(|| CrashReplayArtifactError::InvalidArtifactPath {
            path: path.to_path_buf(),
        })
}

struct CrashHarness {
    scenario: String,
    family: CrashReplayFamily,
    seed: u64,
    runtime: SimulatedRuntime,
    scheduler: SimulationScheduler,
    trace: Vec<CrashReplayTraceRecord>,
    invariant_checkpoints: usize,
    replay_idempotent: bool,
    peer_converged: bool,
}

impl CrashHarness {
    fn new(scenario: &str, family: CrashReplayFamily, seed: u64) -> Self {
        let mut runtime = SimulatedRuntime::new(seed);
        runtime.add_client("writer");
        runtime.add_client("reader");
        Self {
            scenario: scenario.to_string(),
            family,
            seed,
            runtime,
            scheduler: SimulationScheduler::new_with_stream_seed(
                seed,
                family_scheduler_seed(seed, family),
            ),
            trace: Vec::new(),
            invariant_checkpoints: 0,
            replay_idempotent: true,
            peer_converged: false,
        }
    }

    fn finish(mut self) -> FamilyRun {
        let failure = if self.family == CrashReplayFamily::InjectedInvariantFailure {
            Some(CrashReplayFailure {
                scenario: self.scenario.clone(),
                family: self.family,
                seed: self.seed,
                violated_invariant: "injected_counterexample".to_string(),
                message: "injected invariant failure for counterexample trace testing".to_string(),
                trace: self.trace.clone(),
                counterexample_artifact_path: None,
            })
        } else {
            self.checkpoint("final").err().map(|failure| *failure)
        };
        let passed = failure.is_none();
        let final_snapshot = self.runtime.logical_snapshot();
        let operation_log = operation_log_from_trace(&self.scenario, &self.trace);
        let scheduler_trace = self.scheduler.trace().to_vec();
        FamilyRun {
            report: CrashReplaySeedReport {
                seed: self.seed,
                passed,
                invariant_checkpoints: self.invariant_checkpoints,
                replay_idempotent: self.replay_idempotent,
                peer_converged: self.peer_converged,
                trace: self.trace,
            },
            failure,
            scheduler_trace,
            operation_log,
            final_snapshot,
        }
    }

    fn write_durable(&mut self, checksum: &str, size: u64) {
        self.runtime.create_file("writer", "replay.txt").ok();
        self.push("create_file", Some("writer"), Some("replay.txt"));
        self.runtime
            .write_file("writer", "replay.txt", size, checksum)
            .unwrap();
        self.push_with_metadata(
            "write_file",
            Some("writer"),
            Some("replay.txt"),
            [
                ("bytes".to_string(), size.to_string()),
                ("checksum_sha256".to_string(), checksum.to_string()),
            ],
        );
        self.runtime.flush("writer", "replay.txt").unwrap();
        self.push(
            "local_durability_boundary",
            Some("writer"),
            Some("replay.txt"),
        );
    }

    fn schedule_upload(&mut self) -> u64 {
        let upload = self
            .runtime
            .schedule_upload("writer", "replay.txt")
            .unwrap();
        self.push(
            "pending_local_upload_recorded",
            Some("writer"),
            Some("replay.txt"),
        );
        upload.upload_id
    }

    fn complete_upload(&mut self, upload_id: u64) {
        self.runtime.complete_upload(upload_id).unwrap();
        self.push("upload_completed", Some("writer"), Some("replay.txt"));
    }

    fn publish_and_deliver(&mut self) {
        self.runtime.publish_file("writer", "replay.txt").unwrap();
        self.push("metadata_published", Some("writer"), Some("replay.txt"));
        self.runtime.deliver_next_metadata("reader").unwrap();
        self.push(
            "foreign_apply_completed",
            Some("reader"),
            Some("replay.txt"),
        );
        let reader = self.runtime.read_file("reader", "replay.txt").unwrap();
        let writer = self.runtime.read_file("writer", "replay.txt").unwrap();
        self.peer_converged = reader == writer;
        if self.peer_converged {
            self.push("peer_converged", Some("reader"), Some("replay.txt"));
        }
    }

    fn restart_writer(&mut self) {
        self.runtime.restart_client("writer").unwrap();
        let after_first_replay = self.runtime.read_file("writer", "replay.txt").ok();
        self.runtime.restart_client("writer").unwrap();
        let after_second_replay = self.runtime.read_file("writer", "replay.txt").ok();
        self.replay_idempotent &= after_first_replay == after_second_replay;
        self.push("client_restarted", Some("writer"), Some("replay.txt"));
    }

    fn restart_reader(&mut self) {
        self.runtime.restart_client("reader").unwrap();
        self.push("client_restarted", Some("reader"), None);
    }

    fn checkpoint(&mut self, name: &str) -> Result<(), Box<CrashReplayFailure>> {
        self.push("invariant_checkpoint", None, Some(name));
        self.invariant_checkpoints += 1;
        let mut snapshot = self.runtime.logical_snapshot();
        snapshot.replay_idempotent = Some(self.replay_idempotent);
        let report = validate_logical_invariants(&snapshot);
        if report.passed {
            return Ok(());
        }
        let violation = report
            .violations
            .first()
            .cloned()
            .unwrap_or_else(|| InvariantViolation {
                invariant: "unknown".to_string(),
                message: "invariant validation failed but no violations were reported".to_string(),
                client: None,
                path: None,
                logical_id: None,
                object_id: None,
            });
        self.push(
            "invariant_failed",
            violation.client.as_deref(),
            violation.path.as_deref(),
        );
        Err(Box::new(CrashReplayFailure {
            scenario: self.scenario.clone(),
            family: self.family,
            seed: self.seed,
            violated_invariant: violation.invariant,
            message: violation.message,
            trace: self.trace.clone(),
            counterexample_artifact_path: None,
        }))
    }

    fn record_checkpoint(&mut self, name: &str) {
        self.push("replay_checkpoint", None, Some(name));
        self.invariant_checkpoints += 1;
    }

    fn run_race(&mut self, first: PlannedCrashOp, second: PlannedCrashOp) -> Vec<PlannedCrashOp> {
        for operation in [first, second] {
            self.scheduler.schedule_now(
                ScheduledEvent::new(operation.event_type())
                    .with_client(operation.client())
                    .with_description(operation.trace_kind()),
            );
        }

        let mut selected = Vec::new();
        while let Some(event) = self.scheduler.step() {
            self.push(
                "scheduler_selected",
                event.event.client.as_deref(),
                event.event.description.as_deref(),
            );
            let operation = PlannedCrashOp::from_trace_kind(
                event
                    .event
                    .description
                    .as_deref()
                    .expect("crash event must have a trace description"),
            );
            selected.push(operation);
        }
        selected
    }

    fn push(&mut self, kind: &str, client: Option<&str>, path: Option<&str>) {
        self.push_with_metadata(kind, client, path, []);
    }

    fn push_with_metadata(
        &mut self,
        kind: &str,
        client: Option<&str>,
        path: Option<&str>,
        metadata: impl IntoIterator<Item = (String, String)>,
    ) {
        self.trace.push(CrashReplayTraceRecord {
            step: self.trace.len() as u64 + 1,
            kind: kind.to_string(),
            client: client.map(str::to_string),
            path: path.map(str::to_string),
            metadata: metadata.into_iter().collect(),
        });
    }
}

fn family_scheduler_seed(seed: u64, family: CrashReplayFamily) -> u64 {
    let family_index: u64 = match family {
        CrashReplayFamily::CrashAfterDurabilityBeforeUploadCompletion => 1,
        CrashReplayFamily::CrashAfterUploadBeforeMetadataPublication => 2,
        CrashReplayFamily::CrashDuringPublishAttempt => 3,
        CrashReplayFamily::CrashDuringForeignApply => 4,
        CrashReplayFamily::RestartWithPendingLocalUploadState => 5,
        CrashReplayFamily::RepeatedRestartReplayLoops => 6,
        CrashReplayFamily::StalePendingUploadEventsRaceNewerEpochs => 7,
        CrashReplayFamily::MemoryOnlyDirtyCrash => 8,
        CrashReplayFamily::InjectedInvariantFailure => 9,
    };
    simulation_stream_seed(seed, family_index)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlannedCrashOp {
    RestartWriter,
    Publish,
    RestartReader,
    ForeignApply,
}

impl PlannedCrashOp {
    fn client(self) -> &'static str {
        match self {
            Self::RestartWriter | Self::Publish => "writer",
            Self::RestartReader | Self::ForeignApply => "reader",
        }
    }

    fn event_type(self) -> SchedulerEventType {
        match self {
            Self::RestartWriter | Self::RestartReader => SchedulerEventType::CrashRestart,
            Self::Publish => SchedulerEventType::PublishAttempt,
            Self::ForeignApply => SchedulerEventType::MetadataDelivery,
        }
    }

    fn trace_kind(self) -> &'static str {
        match self {
            Self::RestartWriter => "restart_writer",
            Self::Publish => "publish",
            Self::RestartReader => "restart_reader",
            Self::ForeignApply => "foreign_apply",
        }
    }

    fn from_trace_kind(kind: &str) -> Self {
        match kind {
            "restart_writer" => Self::RestartWriter,
            "publish" => Self::Publish,
            "restart_reader" => Self::RestartReader,
            "foreign_apply" => Self::ForeignApply,
            _ => panic!("unknown planned crash operation {kind}"),
        }
    }
}

fn run_family(scenario: &str, family: CrashReplayFamily, seed: u64) -> FamilyRun {
    let mut harness = CrashHarness::new(scenario, family, seed);
    match family {
        CrashReplayFamily::CrashAfterDurabilityBeforeUploadCompletion => {
            crash_after_durability_before_upload(&mut harness)
        }
        CrashReplayFamily::CrashAfterUploadBeforeMetadataPublication => {
            crash_after_upload_before_publication(&mut harness)
        }
        CrashReplayFamily::CrashDuringPublishAttempt => crash_during_publish_attempt(&mut harness),
        CrashReplayFamily::CrashDuringForeignApply => crash_during_foreign_apply(&mut harness),
        CrashReplayFamily::RestartWithPendingLocalUploadState => {
            restart_with_pending_local_upload(&mut harness)
        }
        CrashReplayFamily::RepeatedRestartReplayLoops => {
            repeated_restart_replay_loops(&mut harness)
        }
        CrashReplayFamily::StalePendingUploadEventsRaceNewerEpochs => {
            stale_pending_upload_events_race_newer_epochs(&mut harness)
        }
        CrashReplayFamily::MemoryOnlyDirtyCrash => memory_only_dirty_crash(&mut harness),
        CrashReplayFamily::InjectedInvariantFailure => injected_invariant_failure(&mut harness),
    }
    harness.finish()
}

fn crash_after_durability_before_upload(h: &mut CrashHarness) {
    h.write_durable("checksum-durable", 8);
    h.record_checkpoint("durable-before-crash");
    let upload = h.schedule_upload();
    h.restart_writer();
    h.complete_upload(upload);
    h.publish_and_deliver();
}

fn crash_after_upload_before_publication(h: &mut CrashHarness) {
    h.write_durable("checksum-uploaded", 9);
    let upload = h.schedule_upload();
    h.complete_upload(upload);
    h.record_checkpoint("uploaded-before-publication");
    h.restart_writer();
    h.publish_and_deliver();
}

fn crash_during_publish_attempt(h: &mut CrashHarness) {
    h.write_durable("checksum-publish-race", 10);
    let upload = h.schedule_upload();
    h.complete_upload(upload);
    h.record_checkpoint("ready-to-publish");

    let selected = h.run_race(PlannedCrashOp::Publish, PlannedCrashOp::RestartWriter);
    for operation in selected {
        match operation {
            PlannedCrashOp::Publish => {
                h.runtime.publish_file("writer", "replay.txt").ok();
                h.push("publish_attempted", Some("writer"), Some("replay.txt"));
            }
            PlannedCrashOp::RestartWriter => h.restart_writer(),
            PlannedCrashOp::RestartReader | PlannedCrashOp::ForeignApply => {}
        }
    }
    h.runtime.publish_file("writer", "replay.txt").ok();
    h.runtime.deliver_next_metadata("reader").ok();
    h.runtime.deliver_next_metadata("reader").ok();
    h.push("publish_replayed", Some("writer"), Some("replay.txt"));
    h.peer_converged = h.runtime.read_file("reader", "replay.txt").is_ok();
    if h.peer_converged {
        h.push("peer_converged", Some("reader"), Some("replay.txt"));
    }
}

fn crash_during_foreign_apply(h: &mut CrashHarness) {
    h.write_durable("checksum-foreign-apply", 11);
    let upload = h.schedule_upload();
    h.complete_upload(upload);
    h.runtime.publish_file("writer", "replay.txt").unwrap();
    h.record_checkpoint("published-before-foreign-apply");

    let selected = h.run_race(PlannedCrashOp::ForeignApply, PlannedCrashOp::RestartReader);
    for operation in selected {
        match operation {
            PlannedCrashOp::ForeignApply => {
                h.runtime.deliver_next_metadata("reader").ok();
                h.push(
                    "foreign_apply_attempted",
                    Some("reader"),
                    Some("replay.txt"),
                );
            }
            PlannedCrashOp::RestartReader => h.restart_reader(),
            PlannedCrashOp::RestartWriter | PlannedCrashOp::Publish => {}
        }
    }
    if h.runtime.read_file("reader", "replay.txt").is_err() {
        h.runtime.publish_file("writer", "replay.txt").ok();
        h.runtime.deliver_next_metadata("reader").ok();
        h.push("foreign_apply_replayed", Some("reader"), Some("replay.txt"));
    }
    h.peer_converged = h.runtime.read_file("reader", "replay.txt").is_ok();
    if h.peer_converged {
        h.push("peer_converged", Some("reader"), Some("replay.txt"));
    }
}

fn restart_with_pending_local_upload(h: &mut CrashHarness) {
    h.write_durable("checksum-pending-upload", 12);
    let upload = h.schedule_upload();
    h.record_checkpoint("pending-upload-before-restart");
    h.restart_writer();
    h.complete_upload(upload);
    h.publish_and_deliver();
}

fn repeated_restart_replay_loops(h: &mut CrashHarness) {
    h.write_durable("checksum-restart-loop", 13);
    for _ in 0..3 {
        h.restart_writer();
    }
    h.record_checkpoint("after-repeated-restarts");
    let upload = h.schedule_upload();
    h.complete_upload(upload);
    h.publish_and_deliver();
}

fn stale_pending_upload_events_race_newer_epochs(h: &mut CrashHarness) {
    h.write_durable("checksum-v1", 14);
    let stale_upload = h.schedule_upload();
    let stale = h
        .runtime
        .published_file_version("writer", "replay.txt")
        .unwrap();
    h.runtime
        .write_file("writer", "replay.txt", 15, "checksum-v2")
        .unwrap();
    h.runtime.flush("writer", "replay.txt").unwrap();
    let newer_upload = h.schedule_upload();
    h.record_checkpoint("stale-upload-and-newer-epoch-pending");
    h.restart_writer();
    h.complete_upload(stale_upload);
    if h.runtime
        .publish_file_version("writer", "replay.txt", stale)
        .is_err()
    {
        h.push("stale_publish_rejected", Some("writer"), Some("replay.txt"));
    }
    h.complete_upload(newer_upload);
    h.publish_and_deliver();
    let reader = h.runtime.read_file("reader", "replay.txt").unwrap();
    if reader.checksum_sha256 == "checksum-v2" {
        h.push(
            "peer_observed_newer_epoch",
            Some("reader"),
            Some("replay.txt"),
        );
    } else {
        h.replay_idempotent = false;
    }
}

fn memory_only_dirty_crash(h: &mut CrashHarness) {
    h.write_durable("checksum-durable", 6);
    h.runtime
        .write_file("writer", "replay.txt", 16, "checksum-memory-only")
        .unwrap();
    h.restart_writer();
    let file = h.runtime.read_file("writer", "replay.txt").unwrap();
    if file.checksum_sha256 == "checksum-durable" {
        h.push(
            "dirty_bytes_lost_after_crash",
            Some("writer"),
            Some("replay.txt"),
        );
    } else {
        h.replay_idempotent = false;
    }
    h.record_checkpoint("after-memory-only-crash");
    let upload = h.schedule_upload();
    h.complete_upload(upload);
    h.publish_and_deliver();
}

fn injected_invariant_failure(h: &mut CrashHarness) {
    h.write_durable("checksum-counterexample", 1);
    let _ = h.run_race(PlannedCrashOp::Publish, PlannedCrashOp::RestartWriter);
    h.replay_idempotent = false;
    h.push("invariant_failed", None, Some("replay.txt"));
    h.push("counterexample_replay_ready", None, Some("replay.txt"));
}

fn operation_log_from_trace(
    scenario: &str,
    trace: &[CrashReplayTraceRecord],
) -> Vec<ScenarioOperationRecord> {
    trace
        .iter()
        .map(|record| {
            let mut operation =
                ScenarioOperationRecord::new(scenario, record.step, operation_from_trace(record))
                    .with_outcome(crate::OperationOutcome::Observed);
            if let Some(client) = &record.client {
                operation = operation.with_client(client.clone());
            }
            if let Some(path) = &record.path {
                operation = operation.with_metadata("path", path.clone());
            }
            for (key, value) in &record.metadata {
                operation = operation.with_metadata(key.clone(), value.clone());
            }
            operation
        })
        .collect()
}

fn operation_from_trace(record: &CrashReplayTraceRecord) -> ScenarioOperation {
    match record.kind.as_str() {
        "create_file" => ScenarioOperation::CreateFile {
            path: trace_path(record),
            bytes: None,
        },
        "write_file" => ScenarioOperation::Write {
            path: trace_path(record),
            offset: None,
            bytes: record
                .metadata
                .get("bytes")
                .and_then(|bytes| bytes.parse().ok())
                .unwrap_or_default(),
            checksum_sha256: record.metadata.get("checksum_sha256").cloned(),
        },
        "local_durability_boundary" => ScenarioOperation::Flush {
            path: record.path.clone(),
        },
        "client_restarted" => ScenarioOperation::ClientRestart,
        "upload_completed" => ScenarioOperation::ObjectStoreRecovery,
        "metadata_published" | "publish_attempted" | "publish_replayed" => {
            ScenarioOperation::Fsync {
                path: trace_path(record),
            }
        }
        "foreign_apply_completed" | "foreign_apply_attempted" | "foreign_apply_replayed" => {
            ScenarioOperation::Wait {
                checkpoint: record.kind.clone(),
            }
        }
        _ => ScenarioOperation::SchedulerCheckpoint {
            name: record.kind.clone(),
        },
    }
}

fn trace_path(record: &CrashReplayTraceRecord) -> String {
    record.path.clone().unwrap_or_default()
}

fn counterexample_artifact_path(
    artifact_root: &Path,
    scenario: &str,
    family: CrashReplayFamily,
    seed: u64,
) -> PathBuf {
    artifact_root
        .join(sanitize_path_component(scenario))
        .join(format!("{family:?}-seed-{seed}.json").to_ascii_lowercase())
}

fn sanitize_path_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized.to_string()
    }
}

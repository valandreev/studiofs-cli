//! Deterministic crash/restart and replay simulation suite.

use serde::{Deserialize, Serialize};

use crate::client::{PublishedFileVersion, SimulatedRuntime};
use crate::invariant::{InvariantViolation, validate_logical_invariants};
use crate::scheduler::{ScheduledEvent, SchedulerEventType, SimulationScheduler};

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashReplayTraceRecord {
    pub step: u64,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
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

struct FamilyRun {
    report: CrashReplaySeedReport,
    failure: Option<CrashReplayFailure>,
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
    race_count: u64,
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
            scheduler: SimulationScheduler::new(seed),
            trace: Vec::new(),
            invariant_checkpoints: 0,
            replay_idempotent: true,
            peer_converged: false,
            race_count: 0,
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
            })
        } else {
            self.checkpoint("final").err()
        };
        let passed = failure.is_none();
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
        }
    }

    fn write_durable(&mut self, checksum: &str, size: u64) {
        self.runtime.create_file("writer", "replay.txt").ok();
        self.runtime
            .write_file("writer", "replay.txt", size, checksum)
            .unwrap();
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

    fn checkpoint(&mut self, name: &str) -> Result<(), CrashReplayFailure> {
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
        Err(CrashReplayFailure {
            scenario: self.scenario.clone(),
            family: self.family,
            seed: self.seed,
            violated_invariant: violation.invariant,
            message: violation.message,
            trace: self.trace.clone(),
        })
    }

    fn record_checkpoint(&mut self, name: &str) {
        self.push("replay_checkpoint", None, Some(name));
        self.invariant_checkpoints += 1;
    }

    fn run_race(&mut self, first: PlannedCrashOp, second: PlannedCrashOp) -> Vec<PlannedCrashOp> {
        self.race_count += 1;
        let race_seed = self
            .seed
            .wrapping_add(self.race_count.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let operations = if race_seed.is_multiple_of(2) {
            [second, first]
        } else {
            [first, second]
        };
        for operation in operations {
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
        self.trace.push(CrashReplayTraceRecord {
            step: self.trace.len() as u64 + 1,
            kind: kind.to_string(),
            client: client.map(str::to_string),
            path: path.map(str::to_string),
        });
    }
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
    h.runtime
        .write_file("writer", "replay.txt", 15, "checksum-v2")
        .unwrap();
    h.runtime.flush("writer", "replay.txt").unwrap();
    let newer_upload = h.schedule_upload();
    h.record_checkpoint("stale-upload-and-newer-epoch-pending");
    h.restart_writer();
    h.complete_upload(stale_upload);
    let stale = PublishedFileVersion {
        logical_id: "inode-1".to_string(),
        object_id: "writer:replay.txt:checksum-v1".to_string(),
        checksum_sha256: "checksum-v1".to_string(),
        size: 14,
        epoch: 1,
    };
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
    h.replay_idempotent = false;
    h.push("invariant_failed", None, Some("replay.txt"));
    h.push("counterexample_replay_ready", None, Some("replay.txt"));
}

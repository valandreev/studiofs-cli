//! Seeded namespace and file-version collision scenario suite.

use serde::{Deserialize, Serialize};

use crate::client::SimulatedRuntime;
use crate::invariant::{InvariantViolation, LogicalSnapshot, validate_logical_invariants};
use crate::scheduler::{
    ScheduledEvent, SchedulerEventType, SchedulerTraceRecord, SimulationScheduler,
    simulation_stream_seed,
};
use crate::{OperationOutcome, ScenarioOperation, ScenarioOperationRecord};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionSuiteConfig {
    pub scenario: String,
    pub seeds: Vec<u64>,
    pub families: Vec<CollisionFamily>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollisionFamily {
    RenameVsRename,
    RenameVsUnlink,
    UnlinkVsAppend,
    OverwriteWhileStaleReader,
    MkdirRmdirVsCreate,
    MetadataDeliveryVsLocalMutation,
    PublishVsForeignApply,
    InjectedInvariantFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionSuiteReport {
    pub scenario: String,
    pub passed: bool,
    pub family_reports: Vec<CollisionFamilyReport>,
    pub failures: Vec<CollisionFailure>,
}

impl CollisionSuiteReport {
    pub fn total_runs(&self) -> usize {
        self.family_reports
            .iter()
            .map(|family| family.seed_reports.len())
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionFamilyReport {
    pub family: CollisionFamily,
    pub seed_reports: Vec<CollisionSeedReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionSeedReport {
    pub seed: u64,
    pub passed: bool,
    pub invariant_checkpoints: usize,
    pub trace: Vec<CollisionTraceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionFailure {
    pub scenario: String,
    pub family: CollisionFamily,
    pub seed: u64,
    pub violated_invariant: String,
    pub message: String,
    pub trace: Vec<CollisionTraceRecord>,
    pub scheduler_trace: Vec<SchedulerTraceRecord>,
    pub operation_log: Vec<ScenarioOperationRecord>,
    pub final_snapshot: LogicalSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionTraceRecord {
    pub step: u64,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
}

pub fn run_collision_suite(config: CollisionSuiteConfig) -> CollisionSuiteReport {
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
        family_reports.push(CollisionFamilyReport {
            family,
            seed_reports,
        });
    }

    CollisionSuiteReport {
        scenario: config.scenario,
        passed: failures.is_empty(),
        family_reports,
        failures,
    }
}

struct FamilyRun {
    report: CollisionSeedReport,
    failure: Option<CollisionFailure>,
}

struct FamilyHarness {
    scenario: String,
    family: CollisionFamily,
    seed: u64,
    runtime: SimulatedRuntime,
    scheduler: SimulationScheduler,
    trace: Vec<CollisionTraceRecord>,
    invariant_checkpoints: usize,
}

impl FamilyHarness {
    fn new(scenario: &str, family: CollisionFamily, seed: u64) -> Self {
        let mut runtime = SimulatedRuntime::new(seed);
        runtime.add_client("alpha");
        runtime.add_client("beta");
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
        }
    }

    fn finish(mut self) -> FamilyRun {
        let final_snapshot = self.runtime.logical_snapshot();
        let scheduler_trace = self.scheduler.trace().to_vec();
        let operation_log = operation_log_from_trace(&self.scenario, &self.trace);
        let failure = if self.family == CollisionFamily::InjectedInvariantFailure {
            Some(CollisionFailure {
                scenario: self.scenario.clone(),
                family: self.family,
                seed: self.seed,
                violated_invariant: "injected_counterexample".to_string(),
                message: "injected invariant failure for counterexample trace testing".to_string(),
                trace: self.trace.clone(),
                scheduler_trace: scheduler_trace.clone(),
                operation_log: operation_log.clone(),
                final_snapshot: final_snapshot.clone(),
            })
        } else {
            self.checkpoint("final").err().map(|failure| *failure)
        };
        let passed = failure.is_none();
        FamilyRun {
            report: CollisionSeedReport {
                seed: self.seed,
                passed,
                invariant_checkpoints: self.invariant_checkpoints,
                trace: self.trace,
            },
            failure,
        }
    }

    fn checkpoint(&mut self, name: &str) -> Result<(), Box<CollisionFailure>> {
        self.push("invariant_checkpoint", None, Some(name));
        self.invariant_checkpoints += 1;
        let snapshot = self.runtime.logical_snapshot();
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
        Err(Box::new(CollisionFailure {
            scenario: self.scenario.clone(),
            family: self.family,
            seed: self.seed,
            violated_invariant: violation.invariant,
            message: violation.message,
            trace: self.trace.clone(),
            scheduler_trace: self.scheduler.trace().to_vec(),
            operation_log: operation_log_from_trace(&self.scenario, &self.trace),
            final_snapshot: snapshot,
        }))
    }

    fn publish_to_peer(&mut self, path: &str) {
        self.publish_between("alpha", "beta", path);
    }

    fn publish_between(&mut self, source: &str, target: &str, path: &str) {
        if let Ok(upload) = self.runtime.schedule_upload(source, path) {
            self.runtime.complete_upload(upload.upload_id).unwrap();
        }
        self.runtime.publish_file(source, path).unwrap();
        self.runtime.deliver_next_metadata(target).unwrap();
        self.push("published_to_peer", Some(source), Some(path));
    }

    fn push(&mut self, kind: &str, client: Option<&str>, path: Option<&str>) {
        self.push_record(CollisionTraceRecord {
            step: self.trace.len() as u64 + 1,
            kind: kind.to_string(),
            client: client.map(str::to_string),
            path: path.map(str::to_string),
            source_path: None,
            target_path: None,
            bytes: None,
            checksum_sha256: None,
        });
    }

    fn push_record(&mut self, mut record: CollisionTraceRecord) {
        record.step = self.trace.len() as u64 + 1;
        self.trace.push(record);
    }

    fn record_path_result(
        &mut self,
        accepted: bool,
        accepted_kind: &str,
        rejected_kind: &str,
        client: &str,
        path: &str,
    ) {
        let kind = if accepted {
            accepted_kind
        } else {
            rejected_kind
        };
        self.push(kind, Some(client), Some(path));
    }

    fn record_rename_result(
        &mut self,
        accepted: bool,
        accepted_kind: &str,
        rejected_kind: &str,
        client: &str,
        source_path: &str,
        target_path: &str,
    ) {
        let kind = if accepted {
            accepted_kind
        } else {
            rejected_kind
        };
        self.push_record(CollisionTraceRecord {
            step: 0,
            kind: kind.to_string(),
            client: Some(client.to_string()),
            path: Some(if accepted { target_path } else { source_path }.to_string()),
            source_path: Some(source_path.to_string()),
            target_path: Some(target_path.to_string()),
            bytes: None,
            checksum_sha256: None,
        });
    }

    fn record_append_result(
        &mut self,
        accepted: bool,
        client: &str,
        path: &str,
        bytes: u64,
        checksum_sha256: &str,
    ) {
        self.push_record(CollisionTraceRecord {
            step: 0,
            kind: if accepted {
                "beta_append".to_string()
            } else {
                "beta_append_rejected".to_string()
            },
            client: Some(client.to_string()),
            path: Some(path.to_string()),
            source_path: None,
            target_path: None,
            bytes: Some(bytes),
            checksum_sha256: Some(checksum_sha256.to_string()),
        });
    }

    fn run_race(
        &mut self,
        first: PlannedCollisionOp,
        second: PlannedCollisionOp,
    ) -> Vec<PlannedCollisionOp> {
        for operation in [first, second] {
            self.scheduler.schedule_now(
                ScheduledEvent::new(SchedulerEventType::ClientOperation)
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
            let operation = PlannedCollisionOp::from_trace_kind(
                event
                    .event
                    .description
                    .as_deref()
                    .expect("collision event must have a trace description"),
            );
            self.apply_planned(operation);
            selected.push(operation);
        }
        selected
    }

    fn apply_planned(&mut self, operation: PlannedCollisionOp) {
        match operation {
            PlannedCollisionOp::AlphaRenameToAlphaWin => {
                let accepted = self
                    .runtime
                    .rename("alpha", "shared.txt", "alpha_win.txt")
                    .is_ok();
                self.record_rename_result(
                    accepted,
                    "alpha_rename",
                    "alpha_rename_rejected",
                    "alpha",
                    "shared.txt",
                    "alpha_win.txt",
                );
            }
            PlannedCollisionOp::BetaRenameToBetaWin => {
                let accepted = self
                    .runtime
                    .rename("beta", "shared.txt", "beta_win.txt")
                    .is_ok();
                self.record_rename_result(
                    accepted,
                    "beta_rename",
                    "beta_rename_rejected",
                    "beta",
                    "shared.txt",
                    "beta_win.txt",
                );
            }
            PlannedCollisionOp::AlphaRenameToKept => {
                let accepted = self
                    .runtime
                    .rename("alpha", "shared.txt", "kept.txt")
                    .is_ok();
                self.record_rename_result(
                    accepted,
                    "alpha_rename",
                    "alpha_rename_rejected",
                    "alpha",
                    "shared.txt",
                    "kept.txt",
                );
            }
            PlannedCollisionOp::AlphaUnlink => {
                let accepted = self.runtime.unlink("alpha", "shared.txt").is_ok();
                self.record_path_result(
                    accepted,
                    "alpha_unlink",
                    "alpha_unlink_rejected",
                    "alpha",
                    "shared.txt",
                );
            }
            PlannedCollisionOp::BetaUnlink => {
                let accepted = self.runtime.unlink("beta", "shared.txt").is_ok();
                self.record_path_result(
                    accepted,
                    "beta_unlink",
                    "beta_unlink_rejected",
                    "beta",
                    "shared.txt",
                );
            }
            PlannedCollisionOp::BetaAppend => {
                const APPEND_BYTES: u64 = 1;
                const APPEND_CHECKSUM: &str = "checksum-appended";
                let accepted = self
                    .runtime
                    .append_file("beta", "shared.txt", APPEND_BYTES, APPEND_CHECKSUM)
                    .is_ok();
                self.record_append_result(
                    accepted,
                    "beta",
                    "shared.txt",
                    APPEND_BYTES,
                    APPEND_CHECKSUM,
                );
            }
            PlannedCollisionOp::AlphaOverwrite => {
                let accepted = self
                    .runtime
                    .write_file("alpha", "versioned.txt", 4, "checksum-v2")
                    .is_ok();
                self.record_path_result(
                    accepted,
                    "alpha_overwrite",
                    "alpha_overwrite_rejected",
                    "alpha",
                    "versioned.txt",
                );
            }
            PlannedCollisionOp::BetaStaleRead => {
                let accepted = self.runtime.read_file("beta", "versioned.txt").is_ok();
                self.record_path_result(
                    accepted,
                    "beta_stale_read",
                    "beta_stale_read_rejected",
                    "beta",
                    "versioned.txt",
                );
            }
            PlannedCollisionOp::AlphaRmdir => {
                let accepted = self.runtime.rmdir("alpha", "dir").is_ok();
                self.record_path_result(
                    accepted,
                    "alpha_rmdir",
                    "alpha_rmdir_rejected",
                    "alpha",
                    "dir",
                );
            }
            PlannedCollisionOp::BetaCreateInside => {
                let accepted = self.runtime.create_file("beta", "dir/child.txt").is_ok();
                if accepted {
                    self.runtime
                        .write_file("beta", "dir/child.txt", 1, "checksum-child")
                        .unwrap();
                }
                self.record_path_result(
                    accepted,
                    "beta_create_inside",
                    "beta_create_inside_rejected",
                    "beta",
                    "dir/child.txt",
                );
            }
            PlannedCollisionOp::AlphaPublishMetadata => {
                let accepted = self.runtime.deliver_next_metadata("beta").is_ok();
                self.record_path_result(
                    accepted,
                    "alpha_publish_metadata",
                    "alpha_publish_metadata_rejected",
                    "alpha",
                    "remote.txt",
                );
            }
            PlannedCollisionOp::BetaLocalRename => {
                let accepted = self
                    .runtime
                    .rename("beta", "remote.txt", "local_remote.txt")
                    .is_ok();
                self.record_rename_result(
                    accepted,
                    "beta_local_rename",
                    "beta_local_rename_rejected",
                    "beta",
                    "remote.txt",
                    "local_remote.txt",
                );
            }
            PlannedCollisionOp::AlphaPublishV2 => {
                let accepted = self.runtime.publish_file("alpha", "published.txt").is_ok();
                self.record_path_result(
                    accepted,
                    "alpha_publish_v2",
                    "alpha_publish_v2_rejected",
                    "alpha",
                    "published.txt",
                );
            }
            PlannedCollisionOp::BetaForeignApply => {
                let accepted = self.runtime.deliver_next_metadata("beta").is_ok();
                self.record_path_result(
                    accepted,
                    "beta_foreign_apply",
                    "beta_foreign_apply_rejected",
                    "beta",
                    "published.txt",
                );
            }
        }
    }
}

fn operation_log_from_trace(
    scenario: &str,
    trace: &[CollisionTraceRecord],
) -> Vec<ScenarioOperationRecord> {
    trace
        .iter()
        .map(|record| {
            let mut operation =
                ScenarioOperationRecord::new(scenario, record.step, operation_from_trace(record))
                    .with_outcome(OperationOutcome::Observed);
            if let Some(client) = &record.client {
                operation = operation.with_client(client.clone());
            }
            if let Some(path) = &record.path {
                operation = operation.with_metadata("path", path.clone());
            }
            operation
        })
        .collect()
}

fn operation_from_trace(record: &CollisionTraceRecord) -> ScenarioOperation {
    match record.kind.as_str() {
        "create_file" => ScenarioOperation::CreateFile {
            path: trace_path(record),
            bytes: None,
        },
        "create_dir" => ScenarioOperation::CreateDirectory {
            path: trace_path(record),
        },
        "alpha_rename"
        | "alpha_rename_rejected"
        | "beta_rename"
        | "beta_rename_rejected"
        | "beta_local_rename"
        | "beta_local_rename_rejected" => ScenarioOperation::Rename {
            from: record
                .source_path
                .clone()
                .unwrap_or_else(|| trace_path(record)),
            to: record
                .target_path
                .clone()
                .unwrap_or_else(|| trace_path(record)),
        },
        "alpha_unlink" | "alpha_unlink_rejected" | "beta_unlink" | "beta_unlink_rejected" => {
            ScenarioOperation::Unlink {
                path: trace_path(record),
            }
        }
        "beta_append" | "beta_append_rejected" => ScenarioOperation::Append {
            path: trace_path(record),
            bytes: record.bytes.unwrap_or_default(),
            checksum_sha256: record.checksum_sha256.clone(),
        },
        "publish_to_peer" | "published_to_peer" => ScenarioOperation::Fsync {
            path: trace_path(record),
        },
        _ => ScenarioOperation::SchedulerCheckpoint {
            name: record.kind.clone(),
        },
    }
}

fn trace_path(record: &CollisionTraceRecord) -> String {
    record.path.clone().unwrap_or_default()
}

fn family_scheduler_seed(seed: u64, family: CollisionFamily) -> u64 {
    let family_index: u64 = match family {
        CollisionFamily::RenameVsRename => 1,
        CollisionFamily::RenameVsUnlink => 2,
        CollisionFamily::UnlinkVsAppend => 3,
        CollisionFamily::OverwriteWhileStaleReader => 4,
        CollisionFamily::MkdirRmdirVsCreate => 5,
        CollisionFamily::MetadataDeliveryVsLocalMutation => 6,
        CollisionFamily::PublishVsForeignApply => 7,
        CollisionFamily::InjectedInvariantFailure => 8,
    };
    simulation_stream_seed(seed, family_index)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlannedCollisionOp {
    AlphaRenameToAlphaWin,
    BetaRenameToBetaWin,
    AlphaRenameToKept,
    AlphaUnlink,
    BetaUnlink,
    BetaAppend,
    AlphaOverwrite,
    BetaStaleRead,
    AlphaRmdir,
    BetaCreateInside,
    AlphaPublishMetadata,
    BetaLocalRename,
    AlphaPublishV2,
    BetaForeignApply,
}

impl PlannedCollisionOp {
    fn client(self) -> &'static str {
        match self {
            Self::AlphaRenameToAlphaWin
            | Self::AlphaRenameToKept
            | Self::AlphaUnlink
            | Self::AlphaOverwrite
            | Self::AlphaRmdir
            | Self::AlphaPublishMetadata
            | Self::AlphaPublishV2 => "alpha",
            Self::BetaRenameToBetaWin
            | Self::BetaUnlink
            | Self::BetaAppend
            | Self::BetaStaleRead
            | Self::BetaCreateInside
            | Self::BetaLocalRename
            | Self::BetaForeignApply => "beta",
        }
    }

    fn trace_kind(self) -> &'static str {
        match self {
            Self::AlphaRenameToAlphaWin => "alpha_rename_to_alpha_win",
            Self::BetaRenameToBetaWin => "beta_rename_to_beta_win",
            Self::AlphaRenameToKept => "alpha_rename_to_kept",
            Self::AlphaUnlink => "alpha_unlink",
            Self::BetaUnlink => "beta_unlink",
            Self::BetaAppend => "beta_append",
            Self::AlphaOverwrite => "alpha_overwrite",
            Self::BetaStaleRead => "beta_stale_read",
            Self::AlphaRmdir => "alpha_rmdir",
            Self::BetaCreateInside => "beta_create_inside",
            Self::AlphaPublishMetadata => "alpha_publish_metadata",
            Self::BetaLocalRename => "beta_local_rename",
            Self::AlphaPublishV2 => "alpha_publish_v2",
            Self::BetaForeignApply => "beta_foreign_apply",
        }
    }

    fn from_trace_kind(kind: &str) -> Self {
        match kind {
            "alpha_rename_to_alpha_win" => Self::AlphaRenameToAlphaWin,
            "beta_rename_to_beta_win" => Self::BetaRenameToBetaWin,
            "alpha_rename_to_kept" => Self::AlphaRenameToKept,
            "alpha_unlink" => Self::AlphaUnlink,
            "beta_unlink" => Self::BetaUnlink,
            "beta_append" => Self::BetaAppend,
            "alpha_overwrite" => Self::AlphaOverwrite,
            "beta_stale_read" => Self::BetaStaleRead,
            "alpha_rmdir" => Self::AlphaRmdir,
            "beta_create_inside" => Self::BetaCreateInside,
            "alpha_publish_metadata" => Self::AlphaPublishMetadata,
            "beta_local_rename" => Self::BetaLocalRename,
            "alpha_publish_v2" => Self::AlphaPublishV2,
            "beta_foreign_apply" => Self::BetaForeignApply,
            _ => panic!("unknown planned collision operation {kind}"),
        }
    }
}

fn run_family(scenario: &str, family: CollisionFamily, seed: u64) -> FamilyRun {
    let mut harness = FamilyHarness::new(scenario, family, seed);
    match family {
        CollisionFamily::RenameVsRename => rename_vs_rename(&mut harness),
        CollisionFamily::RenameVsUnlink => rename_vs_unlink(&mut harness),
        CollisionFamily::UnlinkVsAppend => unlink_vs_append(&mut harness),
        CollisionFamily::OverwriteWhileStaleReader => overwrite_while_stale_reader(&mut harness),
        CollisionFamily::MkdirRmdirVsCreate => mkdir_rmdir_vs_create(&mut harness),
        CollisionFamily::MetadataDeliveryVsLocalMutation => {
            metadata_delivery_vs_local_mutation(&mut harness)
        }
        CollisionFamily::PublishVsForeignApply => publish_vs_foreign_apply(&mut harness),
        CollisionFamily::InjectedInvariantFailure => injected_invariant_failure(&mut harness),
    }
    harness.finish()
}

fn rename_vs_rename(h: &mut FamilyHarness) {
    h.runtime.create_file("alpha", "shared.txt").unwrap();
    h.runtime
        .write_file("alpha", "shared.txt", 1, "checksum-base")
        .unwrap();
    h.publish_to_peer("shared.txt");
    h.checkpoint("base").unwrap();

    let selected = h.run_race(
        PlannedCollisionOp::AlphaRenameToAlphaWin,
        PlannedCollisionOp::BetaRenameToBetaWin,
    );
    match selected.first().copied() {
        Some(PlannedCollisionOp::AlphaRenameToAlphaWin) => {
            h.runtime.rename("beta", "shared.txt", "alpha_win.txt").ok();
            h.runtime
                .rename("beta", "beta_win.txt", "alpha_win.txt")
                .ok();
            h.publish_between("alpha", "beta", "alpha_win.txt");
            h.push("rename_vs_rename_winner", None, Some("alpha_win.txt"));
        }
        Some(PlannedCollisionOp::BetaRenameToBetaWin) => {
            h.runtime.rename("alpha", "shared.txt", "beta_win.txt").ok();
            h.runtime
                .rename("alpha", "alpha_win.txt", "beta_win.txt")
                .ok();
            h.publish_between("beta", "alpha", "beta_win.txt");
            h.push("rename_vs_rename_winner", None, Some("beta_win.txt"));
        }
        _ => {}
    }
}

fn rename_vs_unlink(h: &mut FamilyHarness) {
    h.runtime.create_file("alpha", "shared.txt").unwrap();
    h.runtime
        .write_file("alpha", "shared.txt", 1, "checksum-base")
        .unwrap();
    h.publish_to_peer("shared.txt");
    h.checkpoint("base").unwrap();

    let selected = h.run_race(
        PlannedCollisionOp::AlphaRenameToKept,
        PlannedCollisionOp::BetaUnlink,
    );
    match selected.first().copied() {
        Some(PlannedCollisionOp::AlphaRenameToKept) => {
            h.runtime.rename("beta", "shared.txt", "kept.txt").ok();
            h.publish_between("alpha", "beta", "kept.txt");
            h.push("rename_won", None, Some("kept.txt"));
        }
        Some(PlannedCollisionOp::BetaUnlink) => {
            h.runtime.unlink("alpha", "shared.txt").ok();
            h.runtime.unlink("alpha", "kept.txt").ok();
            h.push("unlink_won", None, Some("shared.txt"));
        }
        _ => {}
    }
}

fn unlink_vs_append(h: &mut FamilyHarness) {
    h.runtime.create_file("alpha", "shared.txt").unwrap();
    h.runtime
        .write_file("alpha", "shared.txt", 1, "checksum-base")
        .unwrap();
    h.publish_to_peer("shared.txt");
    h.checkpoint("base").unwrap();

    let selected = h.run_race(
        PlannedCollisionOp::AlphaUnlink,
        PlannedCollisionOp::BetaAppend,
    );
    match selected.first().copied() {
        Some(PlannedCollisionOp::AlphaUnlink) => {
            h.runtime.unlink("beta", "shared.txt").ok();
            h.push("unlink_won", None, Some("shared.txt"));
        }
        Some(PlannedCollisionOp::BetaAppend) => {
            h.publish_between("beta", "alpha", "shared.txt");
            h.push("append_won", None, Some("shared.txt"));
        }
        _ => {}
    }
}

fn overwrite_while_stale_reader(h: &mut FamilyHarness) {
    h.runtime.create_file("alpha", "versioned.txt").unwrap();
    h.runtime
        .write_file("alpha", "versioned.txt", 3, "checksum-v1")
        .unwrap();
    h.publish_to_peer("versioned.txt");
    h.checkpoint("base").unwrap();

    let selected = h.run_race(
        PlannedCollisionOp::AlphaOverwrite,
        PlannedCollisionOp::BetaStaleRead,
    );
    if selected.contains(&PlannedCollisionOp::BetaStaleRead) {
        h.push(
            "stale_reader_kept_previous_version",
            Some("beta"),
            Some("versioned.txt"),
        );
    }
    h.publish_to_peer("versioned.txt");
}

fn mkdir_rmdir_vs_create(h: &mut FamilyHarness) {
    h.runtime.mkdir("alpha", "dir").unwrap();
    h.runtime.mkdir("beta", "dir").unwrap();
    h.checkpoint("base").unwrap();

    let selected = h.run_race(
        PlannedCollisionOp::AlphaRmdir,
        PlannedCollisionOp::BetaCreateInside,
    );
    match selected.first().copied() {
        Some(PlannedCollisionOp::AlphaRmdir) => {
            h.runtime.unlink("beta", "dir/child.txt").ok();
            h.runtime.rmdir("beta", "dir").ok();
            h.push("rmdir_won", None, Some("dir"));
        }
        Some(PlannedCollisionOp::BetaCreateInside) => {
            h.publish_between("beta", "alpha", "dir/child.txt");
            h.push("create_inside_won", None, Some("dir/child.txt"));
        }
        _ => {}
    }
}

fn metadata_delivery_vs_local_mutation(h: &mut FamilyHarness) {
    h.runtime.create_file("alpha", "remote.txt").unwrap();
    h.runtime
        .write_file("alpha", "remote.txt", 2, "checksum-remote-v1")
        .unwrap();
    h.publish_to_peer("remote.txt");
    h.checkpoint("base").unwrap();
    h.runtime
        .write_file("alpha", "remote.txt", 3, "checksum-remote-v2")
        .unwrap();
    let upload = h.runtime.schedule_upload("alpha", "remote.txt").unwrap();
    h.runtime.complete_upload(upload.upload_id).unwrap();
    h.runtime.publish_file("alpha", "remote.txt").unwrap();

    let selected = h.run_race(
        PlannedCollisionOp::AlphaPublishMetadata,
        PlannedCollisionOp::BetaLocalRename,
    );
    if selected.contains(&PlannedCollisionOp::BetaLocalRename) {
        h.runtime
            .rename("alpha", "remote.txt", "local_remote.txt")
            .ok();
        h.runtime.unlink("beta", "remote.txt").ok();
        h.runtime
            .write_file("beta", "local_remote.txt", 3, "checksum-remote-v2")
            .ok();
        h.push(
            "metadata_delivery_raced_local_mutation",
            None,
            Some("local_remote.txt"),
        );
    } else {
        h.push(
            "metadata_delivery_raced_local_mutation",
            None,
            Some("remote.txt"),
        );
    }
}

fn publish_vs_foreign_apply(h: &mut FamilyHarness) {
    h.runtime.create_file("alpha", "published.txt").unwrap();
    h.runtime
        .write_file("alpha", "published.txt", 2, "checksum-v1")
        .unwrap();
    h.publish_to_peer("published.txt");
    h.checkpoint("base").unwrap();
    h.runtime
        .write_file("alpha", "published.txt", 3, "checksum-v2")
        .unwrap();
    let upload = h.runtime.schedule_upload("alpha", "published.txt").unwrap();
    h.runtime.complete_upload(upload.upload_id).unwrap();
    h.run_race(
        PlannedCollisionOp::AlphaPublishV2,
        PlannedCollisionOp::BetaForeignApply,
    );
    h.runtime.deliver_next_metadata("beta").ok();
    h.push("publish_raced_foreign_apply", None, Some("published.txt"));
}

fn injected_invariant_failure(h: &mut FamilyHarness) {
    h.runtime.create_file("alpha", "only-alpha.txt").unwrap();
    h.push("invariant_failed", None, Some("only-alpha.txt"));
    h.trace.push(CollisionTraceRecord {
        step: h.trace.len() as u64 + 1,
        kind: "counterexample_replay_ready".to_string(),
        client: None,
        path: Some("only-alpha.txt".to_string()),
        source_path: None,
        target_path: None,
        bytes: None,
        checksum_sha256: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_from_trace_maps_actual_collision_trace_kinds() {
        assert_eq!(
            operation_from_trace(&trace_record_with_paths(
                "alpha_rename",
                "alpha",
                "alpha_win.txt",
                "shared.txt",
                "alpha_win.txt",
            )),
            ScenarioOperation::Rename {
                from: "shared.txt".to_string(),
                to: "alpha_win.txt".to_string(),
            }
        );
        assert_eq!(
            operation_from_trace(&trace_record("beta_unlink_rejected", "beta", "shared.txt")),
            ScenarioOperation::Unlink {
                path: "shared.txt".to_string(),
            }
        );
        assert_eq!(
            operation_from_trace(&append_trace_record(
                "beta_append",
                "beta",
                "shared.txt",
                1,
                "checksum-appended",
            )),
            ScenarioOperation::Append {
                path: "shared.txt".to_string(),
                bytes: 1,
                checksum_sha256: Some("checksum-appended".to_string()),
            }
        );
    }

    #[test]
    fn operation_from_trace_keeps_scheduler_events_as_checkpoints() {
        assert_eq!(
            operation_from_trace(&trace_record(
                "scheduler_selected",
                "alpha",
                "alpha_rename_to_alpha_win",
            )),
            ScenarioOperation::SchedulerCheckpoint {
                name: "scheduler_selected".to_string(),
            }
        );
    }

    fn trace_record(kind: &str, client: &str, path: &str) -> CollisionTraceRecord {
        CollisionTraceRecord {
            step: 1,
            kind: kind.to_string(),
            client: Some(client.to_string()),
            path: Some(path.to_string()),
            source_path: None,
            target_path: None,
            bytes: None,
            checksum_sha256: None,
        }
    }

    fn trace_record_with_paths(
        kind: &str,
        client: &str,
        path: &str,
        source_path: &str,
        target_path: &str,
    ) -> CollisionTraceRecord {
        CollisionTraceRecord {
            source_path: Some(source_path.to_string()),
            target_path: Some(target_path.to_string()),
            ..trace_record(kind, client, path)
        }
    }

    fn append_trace_record(
        kind: &str,
        client: &str,
        path: &str,
        bytes: u64,
        checksum_sha256: &str,
    ) -> CollisionTraceRecord {
        CollisionTraceRecord {
            bytes: Some(bytes),
            checksum_sha256: Some(checksum_sha256.to_string()),
            ..trace_record(kind, client, path)
        }
    }
}

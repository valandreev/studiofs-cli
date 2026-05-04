//! Seeded namespace and file-version collision scenario suite.

use serde::{Deserialize, Serialize};

use crate::client::SimulatedRuntime;
use crate::invariant::validate_logical_invariants;
use crate::scheduler::{ScheduledEvent, SchedulerEventType, SimulationScheduler};

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionTraceRecord {
    pub step: u64,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
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
            scheduler: SimulationScheduler::new(seed),
            trace: Vec::new(),
            invariant_checkpoints: 0,
        }
    }

    fn finish(mut self) -> FamilyRun {
        let failure = if self.family == CollisionFamily::InjectedInvariantFailure {
            Some(CollisionFailure {
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
            report: CollisionSeedReport {
                seed: self.seed,
                passed,
                invariant_checkpoints: self.invariant_checkpoints,
                trace: self.trace,
            },
            failure,
        }
    }

    fn checkpoint(&mut self, name: &str) -> Result<(), CollisionFailure> {
        self.push("invariant_checkpoint", None, Some(name));
        self.invariant_checkpoints += 1;
        let snapshot = self.runtime.logical_snapshot();
        let report = validate_logical_invariants(&snapshot);
        if report.passed {
            return Ok(());
        }
        let violation = report.violations[0].clone();
        self.push(
            "invariant_failed",
            violation.client.as_deref(),
            violation.path.as_deref(),
        );
        Err(CollisionFailure {
            scenario: self.scenario.clone(),
            family: self.family,
            seed: self.seed,
            violated_invariant: violation.invariant,
            message: violation.message,
            trace: self.trace.clone(),
        })
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
        self.trace.push(CollisionTraceRecord {
            step: self.trace.len() as u64 + 1,
            kind: kind.to_string(),
            client: client.map(str::to_string),
            path: path.map(str::to_string),
        });
    }

    fn run_race(
        &mut self,
        first: PlannedCollisionOp,
        second: PlannedCollisionOp,
    ) -> Vec<PlannedCollisionOp> {
        let operations = if self.seed % 2 == 0 {
            [second, first]
        } else {
            [first, second]
        };
        for operation in operations {
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
                if self
                    .runtime
                    .rename("alpha", "shared.txt", "alpha_win.txt")
                    .is_ok()
                {
                    self.push("alpha_rename", Some("alpha"), Some("alpha_win.txt"));
                } else {
                    self.push("alpha_rename_rejected", Some("alpha"), Some("shared.txt"));
                }
            }
            PlannedCollisionOp::BetaRenameToBetaWin => {
                if self
                    .runtime
                    .rename("beta", "shared.txt", "beta_win.txt")
                    .is_ok()
                {
                    self.push("beta_rename", Some("beta"), Some("beta_win.txt"));
                } else {
                    self.push("beta_rename_rejected", Some("beta"), Some("shared.txt"));
                }
            }
            PlannedCollisionOp::AlphaRenameToKept => {
                if self
                    .runtime
                    .rename("alpha", "shared.txt", "kept.txt")
                    .is_ok()
                {
                    self.push("alpha_rename", Some("alpha"), Some("kept.txt"));
                } else {
                    self.push("alpha_rename_rejected", Some("alpha"), Some("shared.txt"));
                }
            }
            PlannedCollisionOp::AlphaUnlink => {
                if self.runtime.unlink("alpha", "shared.txt").is_ok() {
                    self.push("alpha_unlink", Some("alpha"), Some("shared.txt"));
                } else {
                    self.push("alpha_unlink_rejected", Some("alpha"), Some("shared.txt"));
                }
            }
            PlannedCollisionOp::BetaUnlink => {
                if self.runtime.unlink("beta", "shared.txt").is_ok() {
                    self.push("beta_unlink", Some("beta"), Some("shared.txt"));
                } else {
                    self.push("beta_unlink_rejected", Some("beta"), Some("shared.txt"));
                }
            }
            PlannedCollisionOp::BetaAppend => {
                if self
                    .runtime
                    .append_file("beta", "shared.txt", 1, "checksum-appended")
                    .is_ok()
                {
                    self.push("beta_append", Some("beta"), Some("shared.txt"));
                } else {
                    self.push("beta_append_rejected", Some("beta"), Some("shared.txt"));
                }
            }
            PlannedCollisionOp::AlphaOverwrite => {
                if self
                    .runtime
                    .write_file("alpha", "versioned.txt", 4, "checksum-v2")
                    .is_ok()
                {
                    self.push("alpha_overwrite", Some("alpha"), Some("versioned.txt"));
                } else {
                    self.push(
                        "alpha_overwrite_rejected",
                        Some("alpha"),
                        Some("versioned.txt"),
                    );
                }
            }
            PlannedCollisionOp::BetaStaleRead => {
                if self.runtime.read_file("beta", "versioned.txt").is_ok() {
                    self.push("beta_stale_read", Some("beta"), Some("versioned.txt"));
                } else {
                    self.push(
                        "beta_stale_read_rejected",
                        Some("beta"),
                        Some("versioned.txt"),
                    );
                }
            }
            PlannedCollisionOp::AlphaRmdir => {
                if self.runtime.rmdir("alpha", "dir").is_ok() {
                    self.push("alpha_rmdir", Some("alpha"), Some("dir"));
                } else {
                    self.push("alpha_rmdir_rejected", Some("alpha"), Some("dir"));
                }
            }
            PlannedCollisionOp::BetaCreateInside => {
                if self.runtime.create_file("beta", "dir/child.txt").is_ok() {
                    self.runtime
                        .write_file("beta", "dir/child.txt", 1, "checksum-child")
                        .unwrap();
                    self.push("beta_create_inside", Some("beta"), Some("dir/child.txt"));
                } else {
                    self.push(
                        "beta_create_inside_rejected",
                        Some("beta"),
                        Some("dir/child.txt"),
                    );
                }
            }
        }
    }
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
}

impl PlannedCollisionOp {
    fn client(self) -> &'static str {
        match self {
            Self::AlphaRenameToAlphaWin
            | Self::AlphaRenameToKept
            | Self::AlphaUnlink
            | Self::AlphaOverwrite
            | Self::AlphaRmdir => "alpha",
            Self::BetaRenameToBetaWin
            | Self::BetaUnlink
            | Self::BetaAppend
            | Self::BetaStaleRead
            | Self::BetaCreateInside => "beta",
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
        .write_file("alpha", "remote.txt", 2, "checksum-remote")
        .unwrap();
    h.publish_to_peer("remote.txt");
    h.checkpoint("base").unwrap();
    h.runtime
        .rename("alpha", "remote.txt", "settled.txt")
        .unwrap();
    h.runtime
        .rename("beta", "remote.txt", "settled.txt")
        .unwrap();
    h.push(
        "metadata_delivery_raced_local_mutation",
        None,
        Some("settled.txt"),
    );
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
    h.runtime
        .write_file("beta", "published.txt", 3, "checksum-v2")
        .unwrap();
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
    });
}

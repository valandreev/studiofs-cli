//! Deterministic simulation seed corpus used by RWX simulation targets.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::client::{ClientRuntimeError, SimulatedRuntime};
use crate::collision::{CollisionFamily, CollisionSuiteConfig, run_collision_suite};
use crate::crash_replay::{
    CrashReplayArtifactError, CrashReplayFamily, CrashReplaySuiteConfig, run_crash_replay_suite,
    run_crash_replay_suite_with_artifacts,
};
use crate::invariant::validate_logical_invariants;
use crate::object_store::{ObjectStoreFault, SimulatedObjectStore, UploadOutcome};

const FAST_SEEDS: &[u64] = &[1, 2, 3];
const DEEP_SEEDS: &[u64] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
const CORPUS_SCENARIO: &str = "sfs340-simulation-corpus";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimulationCorpusKind {
    Fast,
    Deep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimulationScenarioGroup {
    NamespaceCollisions,
    CrashRestartReplay,
    MetadataOutage,
    ObjectStoreOutage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationCorpusPlan {
    pub kind: SimulationCorpusKind,
    pub seeds: Vec<u64>,
    pub groups: Vec<SimulationGroupPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationGroupPlan {
    pub group: SimulationScenarioGroup,
    pub replay_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationCorpusOptions {
    pub kind: SimulationCorpusKind,
    pub group: Option<SimulationScenarioGroup>,
    pub seed: Option<u64>,
    pub artifact_root: Option<PathBuf>,
    pub inject_failure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationCorpusReport {
    pub scenario: String,
    pub kind: SimulationCorpusKind,
    pub passed: bool,
    pub total_runs: usize,
    pub group_reports: Vec<SimulationGroupReport>,
    pub failures: Vec<SimulationCorpusFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationGroupReport {
    pub group: SimulationScenarioGroup,
    pub seed_reports: Vec<SimulationSeedReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationSeedReport {
    pub seed: u64,
    pub passed: bool,
    pub invariant_checkpoints: usize,
    pub trace_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationCorpusFailure {
    pub group: SimulationScenarioGroup,
    pub seed: u64,
    pub violated_invariant: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counterexample_artifact_path: Option<String>,
}

#[derive(Debug, Error)]
pub enum SimulationCorpusError {
    #[error("simulation client runtime failed: {0}")]
    Client(#[from] ClientRuntimeError),
    #[error("object-store simulation failed: {0:?}")]
    ObjectStore(ObjectStoreFault),
    #[error("unexpected upload outcome: {0:?}")]
    UploadOutcome(UploadOutcome),
    #[error("counterexample artifact failed: {0}")]
    Artifact(#[from] CrashReplayArtifactError),
}

pub fn corpus_plan(kind: SimulationCorpusKind) -> SimulationCorpusPlan {
    let seeds = seeds_for(kind).to_vec();
    let groups = ordered_groups()
        .iter()
        .copied()
        .map(|group| SimulationGroupPlan {
            group,
            replay_command: format!(
                "cargo run -p studiofs-scenario --bin studiofs-scenario -- run-corpus --corpus {} --scenario-group {}",
                corpus_arg(kind),
                group_arg(group)
            ),
        })
        .collect();
    SimulationCorpusPlan {
        kind,
        seeds,
        groups,
    }
}

pub fn run_simulation_corpus(
    options: SimulationCorpusOptions,
) -> Result<SimulationCorpusReport, SimulationCorpusError> {
    let seeds = match options.seed {
        Some(seed) => vec![seed],
        None => seeds_for(options.kind).to_vec(),
    };
    let groups = match options.group {
        Some(group) => vec![group],
        None => ordered_groups().to_vec(),
    };

    let mut group_reports = Vec::new();
    let mut failures = Vec::new();
    let mut total_runs = 0;

    for group in groups {
        let outcome = run_group(group, &seeds, &options)?;
        total_runs += outcome.seed_reports.len();
        failures.extend(outcome.failures);
        group_reports.push(SimulationGroupReport {
            group,
            seed_reports: outcome.seed_reports,
        });
    }

    Ok(SimulationCorpusReport {
        scenario: CORPUS_SCENARIO.to_string(),
        kind: options.kind,
        passed: failures.is_empty(),
        total_runs,
        group_reports,
        failures,
    })
}

fn run_group(
    group: SimulationScenarioGroup,
    seeds: &[u64],
    options: &SimulationCorpusOptions,
) -> Result<GroupOutcome, SimulationCorpusError> {
    match group {
        SimulationScenarioGroup::NamespaceCollisions => Ok(run_namespace_collisions(seeds)),
        SimulationScenarioGroup::CrashRestartReplay => run_crash_restart(seeds, options),
        SimulationScenarioGroup::MetadataOutage => run_metadata_outage(seeds),
        SimulationScenarioGroup::ObjectStoreOutage => run_object_store_outage(seeds),
    }
}

fn run_namespace_collisions(seeds: &[u64]) -> GroupOutcome {
    let report = run_collision_suite(CollisionSuiteConfig {
        scenario: CORPUS_SCENARIO.to_string(),
        seeds: seeds.to_vec(),
        families: vec![
            CollisionFamily::RenameVsRename,
            CollisionFamily::RenameVsUnlink,
            CollisionFamily::UnlinkVsAppend,
            CollisionFamily::OverwriteWhileStaleReader,
            CollisionFamily::MkdirRmdirVsCreate,
            CollisionFamily::MetadataDeliveryVsLocalMutation,
            CollisionFamily::PublishVsForeignApply,
        ],
    });

    let seed_reports = seeds
        .iter()
        .map(|seed| {
            let family_reports = report
                .family_reports
                .iter()
                .flat_map(|family| &family.seed_reports)
                .filter(|seed_report| seed_report.seed == *seed)
                .collect::<Vec<_>>();
            SimulationSeedReport {
                seed: *seed,
                passed: family_reports.iter().all(|seed| seed.passed),
                invariant_checkpoints: family_reports
                    .iter()
                    .map(|seed| seed.invariant_checkpoints)
                    .sum(),
                trace_records: family_reports.iter().map(|seed| seed.trace.len()).sum(),
            }
        })
        .collect();

    GroupOutcome {
        seed_reports,
        failures: report
            .failures
            .into_iter()
            .map(|failure| SimulationCorpusFailure {
                group: SimulationScenarioGroup::NamespaceCollisions,
                seed: failure.seed,
                violated_invariant: failure.violated_invariant,
                message: failure.message,
                counterexample_artifact_path: None,
            })
            .collect(),
    }
}

fn run_crash_restart(
    seeds: &[u64],
    options: &SimulationCorpusOptions,
) -> Result<GroupOutcome, SimulationCorpusError> {
    let families = if options.inject_failure {
        vec![CrashReplayFamily::InjectedInvariantFailure]
    } else {
        vec![
            CrashReplayFamily::CrashAfterDurabilityBeforeUploadCompletion,
            CrashReplayFamily::CrashAfterUploadBeforeMetadataPublication,
            CrashReplayFamily::CrashDuringPublishAttempt,
            CrashReplayFamily::CrashDuringForeignApply,
            CrashReplayFamily::RestartWithPendingLocalUploadState,
            CrashReplayFamily::RepeatedRestartReplayLoops,
            CrashReplayFamily::StalePendingUploadEventsRaceNewerEpochs,
        ]
    };
    let config = CrashReplaySuiteConfig {
        scenario: CORPUS_SCENARIO.to_string(),
        seeds: seeds.to_vec(),
        families,
    };
    let report = if let Some(artifact_root) = &options.artifact_root {
        run_crash_replay_suite_with_artifacts(config, artifact_root)?
    } else {
        run_crash_replay_suite(config)
    };

    let seed_reports = seeds
        .iter()
        .map(|seed| {
            let family_reports = report
                .family_reports
                .iter()
                .flat_map(|family| &family.seed_reports)
                .filter(|seed_report| seed_report.seed == *seed)
                .collect::<Vec<_>>();
            SimulationSeedReport {
                seed: *seed,
                passed: family_reports.iter().all(|seed| seed.passed),
                invariant_checkpoints: family_reports
                    .iter()
                    .map(|seed| seed.invariant_checkpoints)
                    .sum(),
                trace_records: family_reports.iter().map(|seed| seed.trace.len()).sum(),
            }
        })
        .collect();

    Ok(GroupOutcome {
        seed_reports,
        failures: report
            .failures
            .into_iter()
            .map(|failure| SimulationCorpusFailure {
                group: SimulationScenarioGroup::CrashRestartReplay,
                seed: failure.seed,
                violated_invariant: failure.violated_invariant,
                message: failure.message,
                counterexample_artifact_path: failure.counterexample_artifact_path,
            })
            .collect(),
    })
}

fn run_metadata_outage(seeds: &[u64]) -> Result<GroupOutcome, SimulationCorpusError> {
    let mut seed_reports = Vec::new();
    let mut failures = Vec::new();
    for seed in seeds {
        let mut runtime = SimulatedRuntime::new(*seed);
        runtime.add_client("writer");
        runtime.add_client("reader");

        runtime.set_metadata_available(false);
        runtime.create_file("writer", "metadata-outage.txt")?;
        runtime.write_file(
            "writer",
            "metadata-outage.txt",
            5,
            "checksum-metadata-outage",
        )?;
        runtime.flush("writer", "metadata-outage.txt")?;
        let upload = runtime.schedule_upload("writer", "metadata-outage.txt")?;
        runtime.complete_upload(upload.upload_id)?;
        runtime.publish_file("writer", "metadata-outage.txt")?;
        let blocked = runtime.deliver_next_metadata("reader").is_err();
        runtime.set_metadata_available(true);
        runtime.deliver_next_metadata("reader")?;

        let invariant_report = validate_logical_invariants(&runtime.logical_snapshot());
        let passed = blocked
            && invariant_report.passed
            && runtime
                .read_file("reader", "metadata-outage.txt")?
                .checksum_sha256
                == "checksum-metadata-outage";
        if !passed {
            let (violated_invariant, message) =
                first_violation_or(&invariant_report, "metadata_outage_convergence");
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::MetadataOutage,
                seed: *seed,
                violated_invariant,
                message,
                counterexample_artifact_path: None,
            });
        }
        seed_reports.push(SimulationSeedReport {
            seed: *seed,
            passed,
            invariant_checkpoints: 1,
            trace_records: runtime.trace().len(),
        });
    }
    Ok(GroupOutcome {
        seed_reports,
        failures,
    })
}

fn run_object_store_outage(seeds: &[u64]) -> Result<GroupOutcome, SimulationCorpusError> {
    let mut seed_reports = Vec::new();
    let mut failures = Vec::new();
    for seed in seeds {
        let object_id = format!("object-store-outage-seed-{seed}");
        let mut store = SimulatedObjectStore::new();
        store.stage_local_payload("writer", object_id.clone(), "checksum-object-outage", 7);
        let upload = store
            .schedule_upload("writer", object_id.clone())
            .map_err(SimulationCorpusError::ObjectStore)?;
        store.set_available(false);
        let blocked = matches!(
            store.complete_upload(upload.upload_id),
            Some(UploadOutcome::Blocked {
                reason: ObjectStoreFault::ObjectStoreOutage,
                ..
            })
        );
        store.set_available(true);
        let completed = matches!(
            store.complete_upload(upload.upload_id),
            Some(UploadOutcome::Completed { .. })
        );
        let readable = store.read_remote(&object_id).is_ok();
        store.mark_published(&object_id);

        let passed = blocked && completed && readable && store.is_published(&object_id);
        if !passed {
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::ObjectStoreOutage,
                seed: *seed,
                violated_invariant: "object_store_outage_recovery".to_string(),
                message: "object-store outage did not block then recover upload/read".to_string(),
                counterexample_artifact_path: None,
            });
        }
        seed_reports.push(SimulationSeedReport {
            seed: *seed,
            passed,
            invariant_checkpoints: 1,
            trace_records: store.trace().len(),
        });
    }
    Ok(GroupOutcome {
        seed_reports,
        failures,
    })
}

fn first_violation_or(
    report: &crate::invariant::InvariantReport,
    fallback: &str,
) -> (String, String) {
    report
        .violations
        .first()
        .map(|violation| (violation.invariant.clone(), violation.message.clone()))
        .unwrap_or_else(|| {
            (
                fallback.to_string(),
                "simulation group did not satisfy the expected outage contract".to_string(),
            )
        })
}

fn seeds_for(kind: SimulationCorpusKind) -> &'static [u64] {
    match kind {
        SimulationCorpusKind::Fast => FAST_SEEDS,
        SimulationCorpusKind::Deep => DEEP_SEEDS,
    }
}

fn ordered_groups() -> &'static [SimulationScenarioGroup] {
    &[
        SimulationScenarioGroup::NamespaceCollisions,
        SimulationScenarioGroup::CrashRestartReplay,
        SimulationScenarioGroup::MetadataOutage,
        SimulationScenarioGroup::ObjectStoreOutage,
    ]
}

pub fn corpus_arg(kind: SimulationCorpusKind) -> &'static str {
    match kind {
        SimulationCorpusKind::Fast => "fast",
        SimulationCorpusKind::Deep => "deep",
    }
}

pub fn group_arg(group: SimulationScenarioGroup) -> &'static str {
    match group {
        SimulationScenarioGroup::NamespaceCollisions => "namespace-collisions",
        SimulationScenarioGroup::CrashRestartReplay => "crash-restart-replay",
        SimulationScenarioGroup::MetadataOutage => "metadata-outage",
        SimulationScenarioGroup::ObjectStoreOutage => "object-store-outage",
    }
}

struct GroupOutcome {
    seed_reports: Vec<SimulationSeedReport>,
    failures: Vec<SimulationCorpusFailure>,
}

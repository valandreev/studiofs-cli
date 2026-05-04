//! Deterministic simulation seed corpus used by RWX simulation targets.

use std::fs;
use std::io::{BufReader, BufWriter, Write as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::client::{ClientRuntimeError, SimulatedRuntime};
use crate::collision::{CollisionFamily, CollisionSuiteConfig, run_collision_suite};
use crate::crash_replay::{
    CrashReplayArtifactError, CrashReplayFamily, CrashReplaySuiteConfig, run_crash_replay_suite,
    run_crash_replay_suite_with_artifacts,
};
use crate::invariant::validate_logical_invariants;
use crate::object_store::{
    ObjectStoreFault, ObjectStoreTraceRecord, SimulatedObjectStore, UploadOutcome,
};
use crate::scheduler::{SchedulerEventType, SchedulerTraceRecord};
use crate::{OperationOutcome, ScenarioOperation, ScenarioOperationRecord};

pub const SIMULATION_COUNTEREXAMPLE_SCHEMA_VERSION: u32 = 1;
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
    #[serde(skip)]
    pub replay_evidence: Option<SimulationCounterexampleEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationCounterexampleArtifact {
    pub schema_version: u32,
    pub scenario: String,
    pub corpus: SimulationCorpusKind,
    pub group: SimulationScenarioGroup,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_group: Option<String>,
    pub seed: u64,
    pub scheduler_trace: Vec<SchedulerTraceRecord>,
    pub operation_log: Vec<ScenarioOperationRecord>,
    pub event_trace: Vec<Value>,
    pub failure: SimulationCounterexampleFailure,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_snapshot: Option<Value>,
    pub replay_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationCounterexampleFailure {
    pub violated_invariant: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationCounterexampleEvidence {
    pub sub_group: Option<String>,
    pub scheduler_trace: Vec<SchedulerTraceRecord>,
    pub operation_log: Vec<ScenarioOperationRecord>,
    pub event_trace: Vec<Value>,
    pub final_snapshot: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimulationCounterexampleReplayReport {
    pub reproduced: bool,
    pub scenario: String,
    pub corpus: SimulationCorpusKind,
    pub group: SimulationScenarioGroup,
    pub seed: u64,
    pub violated_invariant: String,
    pub message: String,
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
    #[error("simulation counterexample artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("simulation counterexample artifact JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported simulation counterexample artifact schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("simulation counterexample replay did not reproduce: {0}")]
    ReplayMismatch(String),
    #[error("simulation counterexample artifact path is not valid UTF-8: {path:?}")]
    InvalidArtifactPath { path: PathBuf },
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
        let mut outcome = run_group(group, &seeds, &options)?;
        if let Some(artifact_root) = &options.artifact_root {
            for (failure_index, failure) in outcome.failures.iter_mut().enumerate() {
                if failure.counterexample_artifact_path.is_none() {
                    let artifact_path = simulation_counterexample_artifact_path(
                        artifact_root,
                        options.kind,
                        failure.group,
                        failure.seed,
                        failure_index,
                    );
                    write_simulation_counterexample(&artifact_path, options.kind, failure)?;
                    failure.counterexample_artifact_path =
                        Some(path_utf8(&artifact_path)?.to_string());
                }
            }
        }
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
        SimulationScenarioGroup::NamespaceCollisions => run_namespace_collisions(seeds, options),
        SimulationScenarioGroup::CrashRestartReplay => run_crash_restart(seeds, options),
        SimulationScenarioGroup::MetadataOutage => run_metadata_outage(seeds, options),
        SimulationScenarioGroup::ObjectStoreOutage => run_object_store_outage(seeds, options),
    }
}

fn run_namespace_collisions(
    seeds: &[u64],
    options: &SimulationCorpusOptions,
) -> Result<GroupOutcome, SimulationCorpusError> {
    let families = if options.inject_failure {
        vec![CollisionFamily::InjectedInvariantFailure]
    } else {
        vec![
            CollisionFamily::RenameVsRename,
            CollisionFamily::RenameVsUnlink,
            CollisionFamily::UnlinkVsAppend,
            CollisionFamily::OverwriteWhileStaleReader,
            CollisionFamily::MkdirRmdirVsCreate,
            CollisionFamily::MetadataDeliveryVsLocalMutation,
            CollisionFamily::PublishVsForeignApply,
        ]
    };
    let report = run_collision_suite(CollisionSuiteConfig {
        scenario: CORPUS_SCENARIO.to_string(),
        seeds: seeds.to_vec(),
        families,
    });

    Ok(GroupOutcome {
        seed_reports: aggregate_seed_reports(
            seeds,
            &report.family_reports,
            |family| &family.seed_reports,
            |seed| seed.seed,
            |seed| seed.passed,
            |seed| seed.invariant_checkpoints,
            |seed| seed.trace.len(),
        ),
        failures: report
            .failures
            .into_iter()
            .map(|failure| {
                Ok(SimulationCorpusFailure {
                    group: SimulationScenarioGroup::NamespaceCollisions,
                    seed: failure.seed,
                    violated_invariant: failure.violated_invariant,
                    message: failure.message,
                    counterexample_artifact_path: None,
                    replay_evidence: Some(SimulationCounterexampleEvidence {
                        sub_group: Some(collision_family_arg(failure.family).to_string()),
                        scheduler_trace: nonempty_scheduler_trace(
                            failure.seed,
                            failure.scheduler_trace,
                            "namespace-collisions",
                        ),
                        operation_log: failure.operation_log,
                        event_trace: json_trace(&failure.trace)?,
                        final_snapshot: Some(json_value(&failure.final_snapshot)?),
                    }),
                })
            })
            .collect::<Result<Vec<_>, SimulationCorpusError>>()?,
    })
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

    Ok(GroupOutcome {
        seed_reports: aggregate_seed_reports(
            seeds,
            &report.family_reports,
            |family| &family.seed_reports,
            |seed| seed.seed,
            |seed| seed.passed,
            |seed| seed.invariant_checkpoints,
            |seed| seed.trace.len(),
        ),
        failures: report
            .failures
            .into_iter()
            .map(|failure| SimulationCorpusFailure {
                group: SimulationScenarioGroup::CrashRestartReplay,
                seed: failure.seed,
                violated_invariant: failure.violated_invariant,
                message: failure.message,
                counterexample_artifact_path: failure.counterexample_artifact_path,
                replay_evidence: None,
            })
            .collect(),
    })
}

fn run_metadata_outage(
    seeds: &[u64],
    options: &SimulationCorpusOptions,
) -> Result<GroupOutcome, SimulationCorpusError> {
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
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: None,
                    scheduler_trace: synthetic_scheduler_trace(*seed, "metadata-outage-final"),
                    operation_log: operation_log_from_client_trace(
                        CORPUS_SCENARIO,
                        runtime.trace(),
                    ),
                    event_trace: json_trace(runtime.trace())?,
                    final_snapshot: Some(json_value(&runtime.logical_snapshot())?),
                }),
            });
        }
        if options.inject_failure {
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::MetadataOutage,
                seed: *seed,
                violated_invariant: "injected_counterexample".to_string(),
                message: "injected metadata outage failure for corpus artifact testing".to_string(),
                counterexample_artifact_path: None,
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: None,
                    scheduler_trace: synthetic_scheduler_trace(*seed, "metadata-outage-injected"),
                    operation_log: operation_log_from_client_trace(
                        CORPUS_SCENARIO,
                        runtime.trace(),
                    ),
                    event_trace: json_trace(runtime.trace())?,
                    final_snapshot: Some(json_value(&runtime.logical_snapshot())?),
                }),
            });
        }
        seed_reports.push(SimulationSeedReport {
            seed: *seed,
            passed: passed && !options.inject_failure,
            invariant_checkpoints: 1,
            trace_records: runtime.trace().len(),
        });
    }
    Ok(GroupOutcome {
        seed_reports,
        failures,
    })
}

fn run_object_store_outage(
    seeds: &[u64],
    options: &SimulationCorpusOptions,
) -> Result<GroupOutcome, SimulationCorpusError> {
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
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: None,
                    scheduler_trace: synthetic_scheduler_trace(*seed, "object-store-outage-final"),
                    operation_log: operation_log_from_object_trace(CORPUS_SCENARIO, store.trace()),
                    event_trace: json_trace(store.trace())?,
                    final_snapshot: Some(json_value(&store.object_lifecycle())?),
                }),
            });
        }
        if options.inject_failure {
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::ObjectStoreOutage,
                seed: *seed,
                violated_invariant: "injected_counterexample".to_string(),
                message: "injected object-store outage failure for corpus artifact testing"
                    .to_string(),
                counterexample_artifact_path: None,
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: None,
                    scheduler_trace: synthetic_scheduler_trace(
                        *seed,
                        "object-store-outage-injected",
                    ),
                    operation_log: operation_log_from_object_trace(CORPUS_SCENARIO, store.trace()),
                    event_trace: json_trace(store.trace())?,
                    final_snapshot: Some(json_value(&store.object_lifecycle())?),
                }),
            });
        }
        seed_reports.push(SimulationSeedReport {
            seed: *seed,
            passed: passed && !options.inject_failure,
            invariant_checkpoints: 1,
            trace_records: store.trace().len(),
        });
    }
    Ok(GroupOutcome {
        seed_reports,
        failures,
    })
}

fn aggregate_seed_reports<FamilyReport, SeedReport>(
    seeds: &[u64],
    family_reports: &[FamilyReport],
    seed_reports: impl Fn(&FamilyReport) -> &[SeedReport],
    seed_value: impl Fn(&SeedReport) -> u64,
    seed_passed: impl Fn(&SeedReport) -> bool,
    invariant_checkpoints: impl Fn(&SeedReport) -> usize,
    trace_records: impl Fn(&SeedReport) -> usize,
) -> Vec<SimulationSeedReport> {
    seeds
        .iter()
        .map(|seed| {
            let reports = family_reports
                .iter()
                .flat_map(&seed_reports)
                .filter(|seed_report| seed_value(seed_report) == *seed)
                .collect::<Vec<_>>();
            SimulationSeedReport {
                seed: *seed,
                passed: reports.iter().all(|seed_report| seed_passed(seed_report)),
                invariant_checkpoints: reports
                    .iter()
                    .map(|seed_report| invariant_checkpoints(seed_report))
                    .sum(),
                trace_records: reports
                    .iter()
                    .map(|seed_report| trace_records(seed_report))
                    .sum(),
            }
        })
        .collect()
}

fn write_simulation_counterexample(
    path: &Path,
    kind: SimulationCorpusKind,
    failure: &SimulationCorpusFailure,
) -> Result<(), SimulationCorpusError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let artifact = SimulationCounterexampleArtifact {
        schema_version: SIMULATION_COUNTEREXAMPLE_SCHEMA_VERSION,
        scenario: CORPUS_SCENARIO.to_string(),
        corpus: kind,
        group: failure.group,
        sub_group: failure
            .replay_evidence
            .as_ref()
            .and_then(|evidence| evidence.sub_group.clone()),
        seed: failure.seed,
        scheduler_trace: failure
            .replay_evidence
            .as_ref()
            .map(|evidence| evidence.scheduler_trace.clone())
            .unwrap_or_default(),
        operation_log: failure
            .replay_evidence
            .as_ref()
            .map(|evidence| evidence.operation_log.clone())
            .unwrap_or_default(),
        event_trace: failure
            .replay_evidence
            .as_ref()
            .map(|evidence| evidence.event_trace.clone())
            .unwrap_or_default(),
        failure: SimulationCounterexampleFailure {
            violated_invariant: failure.violated_invariant.clone(),
            message: failure.message.clone(),
        },
        final_snapshot: failure
            .replay_evidence
            .as_ref()
            .and_then(|evidence| evidence.final_snapshot.clone()),
        replay_command: format!(
            "cargo run -p studiofs-scenario --bin studiofs-scenario -- replay-counterexample \"{}\"",
            path_utf8(path)?
        ),
    };
    let file = fs::File::create(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, &artifact)?;
    writer.flush()?;
    Ok(())
}

pub fn read_simulation_counterexample(
    path: impl AsRef<Path>,
) -> Result<SimulationCounterexampleArtifact, SimulationCorpusError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let artifact: SimulationCounterexampleArtifact = serde_json::from_reader(reader)?;
    if artifact.schema_version != SIMULATION_COUNTEREXAMPLE_SCHEMA_VERSION {
        return Err(SimulationCorpusError::UnsupportedSchemaVersion(
            artifact.schema_version,
        ));
    }
    Ok(artifact)
}

pub fn replay_simulation_counterexample(
    path: impl AsRef<Path>,
) -> Result<SimulationCounterexampleReplayReport, SimulationCorpusError> {
    let artifact = read_simulation_counterexample(path)?;
    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind: artifact.corpus,
        group: Some(artifact.group),
        seed: Some(artifact.seed),
        artifact_root: None,
        inject_failure: artifact.failure.violated_invariant == "injected_counterexample",
    })?;
    let failure = report
        .failures
        .into_iter()
        .find(|failure| {
            failure.group == artifact.group
                && failure.seed == artifact.seed
                && failure.violated_invariant == artifact.failure.violated_invariant
                && match failure.replay_evidence.as_ref() {
                    Some(evidence) => evidence.sub_group == artifact.sub_group,
                    None => true,
                }
        })
        .ok_or_else(|| {
            SimulationCorpusError::ReplayMismatch(
                "saved counterexample passed during replay".to_string(),
            )
        })?;
    let evidence = failure.replay_evidence.ok_or_else(|| {
        SimulationCorpusError::ReplayMismatch("replay evidence was not captured".to_string())
    })?;
    if failure.message != artifact.failure.message {
        return Err(SimulationCorpusError::ReplayMismatch(format!(
            "expected message {}, got {}",
            artifact.failure.message, failure.message
        )));
    }
    if evidence.scheduler_trace != artifact.scheduler_trace {
        return Err(SimulationCorpusError::ReplayMismatch(first_difference(
            "scheduler trace",
            &artifact.scheduler_trace,
            &evidence.scheduler_trace,
        )?));
    }
    if evidence.operation_log != artifact.operation_log {
        return Err(SimulationCorpusError::ReplayMismatch(first_difference(
            "operation log",
            &artifact.operation_log,
            &evidence.operation_log,
        )?));
    }
    if evidence.event_trace != artifact.event_trace {
        return Err(SimulationCorpusError::ReplayMismatch(first_difference(
            "event trace",
            &artifact.event_trace,
            &evidence.event_trace,
        )?));
    }
    if evidence.final_snapshot != artifact.final_snapshot {
        return Err(SimulationCorpusError::ReplayMismatch(format!(
            "final snapshot differed from artifact: saved={} replayed={}",
            compact_json(&artifact.final_snapshot)?,
            compact_json(&evidence.final_snapshot)?
        )));
    }

    Ok(SimulationCounterexampleReplayReport {
        reproduced: true,
        scenario: artifact.scenario,
        corpus: artifact.corpus,
        group: artifact.group,
        seed: artifact.seed,
        violated_invariant: failure.violated_invariant,
        message: failure.message,
    })
}

fn simulation_counterexample_artifact_path(
    artifact_root: &Path,
    kind: SimulationCorpusKind,
    group: SimulationScenarioGroup,
    seed: u64,
    failure_index: usize,
) -> PathBuf {
    artifact_root
        .join(corpus_arg(kind))
        .join(group_arg(group))
        .join(format!("seed-{seed}-failure-{failure_index}.json"))
}

fn json_trace<T: Serialize>(trace: &[T]) -> Result<Vec<Value>, SimulationCorpusError> {
    trace
        .iter()
        .map(json_value)
        .collect::<Result<Vec<_>, SimulationCorpusError>>()
}

fn json_value<T: Serialize>(value: &T) -> Result<Value, SimulationCorpusError> {
    serde_json::to_value(value).map_err(SimulationCorpusError::Json)
}

fn compact_json<T: Serialize>(value: &T) -> Result<String, SimulationCorpusError> {
    serde_json::to_string(value).map_err(SimulationCorpusError::Json)
}

fn first_difference<T: Serialize + PartialEq>(
    label: &str,
    saved: &[T],
    replayed: &[T],
) -> Result<String, SimulationCorpusError> {
    let shared_len = saved.len().min(replayed.len());
    for index in 0..shared_len {
        if saved[index] != replayed[index] {
            return Ok(format!(
                "{label} differed from artifact at index {index}: saved={} replayed={}",
                compact_json(&saved[index])?,
                compact_json(&replayed[index])?
            ));
        }
    }
    Ok(format!(
        "{label} differed from artifact length: saved={} replayed={}",
        saved.len(),
        replayed.len()
    ))
}

fn synthetic_scheduler_trace(seed: u64, description: &str) -> Vec<SchedulerTraceRecord> {
    vec![SchedulerTraceRecord {
        step: 1,
        seed,
        virtual_tick: 0,
        event_id: 1,
        event_type: SchedulerEventType::Checkpoint,
        client: None,
        description: Some(description.to_string()),
        interleaving_choice: 0,
        ready_event_count: 1,
        ready_event_ids: vec![1],
    }]
}

fn nonempty_scheduler_trace(
    seed: u64,
    trace: Vec<SchedulerTraceRecord>,
    fallback_description: &str,
) -> Vec<SchedulerTraceRecord> {
    if trace.is_empty() {
        synthetic_scheduler_trace(seed, fallback_description)
    } else {
        trace
    }
}

fn operation_log_from_client_trace(
    scenario: &str,
    trace: &[crate::client::ClientTraceRecord],
) -> Vec<ScenarioOperationRecord> {
    trace
        .iter()
        .map(|record| {
            let mut operation = ScenarioOperationRecord::new(
                scenario,
                record.step,
                ScenarioOperation::SchedulerCheckpoint {
                    name: record.kind.clone(),
                },
            )
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

fn operation_log_from_object_trace(
    scenario: &str,
    trace: &[ObjectStoreTraceRecord],
) -> Vec<ScenarioOperationRecord> {
    trace
        .iter()
        .map(|record| {
            let mut operation = ScenarioOperationRecord::new(
                scenario,
                record.step,
                object_operation_from_trace(record),
            )
            .with_outcome(OperationOutcome::Observed);
            if let Some(client) = &record.client {
                operation = operation.with_client(client.clone());
            }
            if let Some(object_id) = &record.object_id {
                operation = operation.with_metadata("object_id", object_id.clone());
            }
            if let Some(upload_id) = record.upload_id {
                operation = operation.with_metadata("upload_id", upload_id.to_string());
            }
            operation
        })
        .collect()
}

fn object_operation_from_trace(record: &ObjectStoreTraceRecord) -> ScenarioOperation {
    match record.kind {
        crate::object_store::ObjectStoreTraceKind::ObjectStoreOutageStarted => {
            ScenarioOperation::ObjectStoreOutage
        }
        crate::object_store::ObjectStoreTraceKind::ObjectStoreOutageEnded => {
            ScenarioOperation::ObjectStoreRecovery
        }
        _ => ScenarioOperation::SchedulerCheckpoint {
            name: format!("{:?}", record.kind),
        },
    }
}

fn path_utf8(path: &Path) -> Result<&str, SimulationCorpusError> {
    path.to_str()
        .ok_or_else(|| SimulationCorpusError::InvalidArtifactPath {
            path: path.to_path_buf(),
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

fn collision_family_arg(family: CollisionFamily) -> &'static str {
    match family {
        CollisionFamily::RenameVsRename => "rename_vs_rename",
        CollisionFamily::RenameVsUnlink => "rename_vs_unlink",
        CollisionFamily::UnlinkVsAppend => "unlink_vs_append",
        CollisionFamily::OverwriteWhileStaleReader => "overwrite_while_stale_reader",
        CollisionFamily::MkdirRmdirVsCreate => "mkdir_rmdir_vs_create",
        CollisionFamily::MetadataDeliveryVsLocalMutation => "metadata_delivery_vs_local_mutation",
        CollisionFamily::PublishVsForeignApply => "publish_vs_foreign_apply",
        CollisionFamily::InjectedInvariantFailure => "injected_invariant_failure",
    }
}

struct GroupOutcome {
    seed_reports: Vec<SimulationSeedReport>,
    failures: Vec<SimulationCorpusFailure>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_artifact_paths_are_unique_for_multiple_failures_with_the_same_seed() {
        let root = PathBuf::from("target/synthetic-artifacts");
        let first = simulation_counterexample_artifact_path(
            &root,
            SimulationCorpusKind::Fast,
            SimulationScenarioGroup::NamespaceCollisions,
            13,
            0,
        );
        let second = simulation_counterexample_artifact_path(
            &root,
            SimulationCorpusKind::Fast,
            SimulationScenarioGroup::NamespaceCollisions,
            13,
            1,
        );

        assert_ne!(first, second);
        assert!(first.ends_with("fast/namespace-collisions/seed-13-failure-0.json"));
        assert!(second.ends_with("fast/namespace-collisions/seed-13-failure-1.json"));
    }
}

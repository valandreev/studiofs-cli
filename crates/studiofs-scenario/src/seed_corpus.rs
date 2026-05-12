//! Deterministic simulation seed corpus used by RWX simulation targets.

use std::collections::BTreeMap;
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
use crate::invariant::{LogicalSnapshot, validate_logical_invariants};
use crate::object_store::{
    ObjectStoreFault, ObjectStoreTraceRecord, SimulatedObjectStore, UploadOutcome,
};
use crate::scheduler::{
    ScheduledEvent, SchedulerEventType, SchedulerTraceRecord, SimulationScheduler,
    simulation_stream_seed,
};
use crate::{OperationOutcome, ScenarioOperation, ScenarioOperationRecord};

/// Constant $name.
pub const SIMULATION_COUNTEREXAMPLE_SCHEMA_VERSION: u32 = 2;
const FAST_SEEDS: &[u64] = &[1, 2, 3];
const DEEP_SEEDS: &[u64] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
const CORPUS_SCENARIO: &str = "sfs340-simulation-corpus";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum SimulationCorpusKind {
    /// Variant $name.
    Fast,
    /// Variant $name.
    Deep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum SimulationScenarioGroup {
    /// Variant $name.
    NamespaceCollisions,
    /// Variant $name.
    CrashRestartReplay,
    /// Variant $name.
    MetadataOutage,
    /// Variant $name.
    ObjectStoreOutage,
    /// Variant $name.
    SparsePayloadChaos,
    /// Variant $name.
    ColdObserverLongHistory,
    /// Variant $name.
    InvalidationDropoutFallbackSync,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationCorpusPlan {
    /// Field $name.
    pub kind: SimulationCorpusKind,
    /// Field $name.
    pub seeds: Vec<u64>,
    /// Field $name.
    pub groups: Vec<SimulationGroupPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationGroupPlan {
    /// Field $name.
    pub group: SimulationScenarioGroup,
    /// Field $name.
    pub replay_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct SimulationCorpusOptions {
    /// Field $name.
    pub kind: SimulationCorpusKind,
    /// Field $name.
    pub group: Option<SimulationScenarioGroup>,
    /// Field $name.
    pub seed: Option<u64>,
    /// Field $name.
    pub artifact_root: Option<PathBuf>,
    /// Field $name.
    pub inject_failure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationCorpusReport {
    /// Field $name.
    pub scenario: String,
    /// Field $name.
    pub kind: SimulationCorpusKind,
    /// Field $name.
    pub passed: bool,
    /// Field $name.
    pub total_runs: usize,
    /// Field $name.
    pub group_reports: Vec<SimulationGroupReport>,
    /// Field $name.
    pub failures: Vec<SimulationCorpusFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationGroupReport {
    /// Field $name.
    pub group: SimulationScenarioGroup,
    /// Field $name.
    pub seed_reports: Vec<SimulationSeedReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationSeedReport {
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub passed: bool,
    /// Field $name.
    pub invariant_checkpoints: usize,
    /// Field $name.
    pub trace_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationCorpusFailure {
    /// Field $name.
    pub group: SimulationScenarioGroup,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub violated_invariant: String,
    /// Field $name.
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub counterexample_artifact_path: Option<String>,
    #[serde(skip)]
    /// Field $name.
    pub replay_evidence: Option<SimulationCounterexampleEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationCounterexampleArtifact {
    /// Field $name.
    pub schema_version: u32,
    /// Field $name.
    pub scenario: String,
    /// Field $name.
    pub corpus: SimulationCorpusKind,
    /// Field $name.
    pub group: SimulationScenarioGroup,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub sub_group: Option<String>,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub scheduler_trace: Vec<SchedulerTraceRecord>,
    /// Field $name.
    pub operation_log: Vec<ScenarioOperationRecord>,
    /// Field $name.
    pub event_trace: Vec<Value>,
    /// Field $name.
    pub failure: SimulationCounterexampleFailure,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub final_snapshot: Option<Value>,
    /// Field $name.
    pub replay_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationCounterexampleFailure {
    /// Field $name.
    pub violated_invariant: String,
    /// Field $name.
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct SimulationCounterexampleEvidence {
    /// Field $name.
    pub sub_group: Option<String>,
    /// Field $name.
    pub scheduler_trace: Vec<SchedulerTraceRecord>,
    /// Field $name.
    pub operation_log: Vec<ScenarioOperationRecord>,
    /// Field $name.
    pub event_trace: Vec<Value>,
    /// Field $name.
    pub final_snapshot: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct SimulationCounterexampleReplayReport {
    /// Field $name.
    pub reproduced: bool,
    /// Field $name.
    pub scenario: String,
    /// Field $name.
    pub corpus: SimulationCorpusKind,
    /// Field $name.
    pub group: SimulationScenarioGroup,
    /// Field $name.
    pub seed: u64,
    /// Field $name.
    pub violated_invariant: String,
    /// Field $name.
    pub message: String,
}

#[derive(Debug, Error)]
/// Enum $name.
pub enum SimulationCorpusError {
    #[error("simulation client runtime failed: {0}")]
    /// Variant $name.
    Client(#[from] ClientRuntimeError),
    #[error("object-store simulation failed: {0:?}")]
    /// Variant $name.
    ObjectStore(ObjectStoreFault),
    #[error("unexpected upload outcome: {0:?}")]
    /// Variant $name.
    UploadOutcome(UploadOutcome),
    #[error("counterexample artifact failed: {0}")]
    /// Variant $name.
    Artifact(#[from] CrashReplayArtifactError),
    #[error("simulation counterexample artifact I/O failed: {0}")]
    /// Variant $name.
    Io(#[from] std::io::Error),
    #[error("simulation counterexample artifact JSON failed: {0}")]
    /// Variant $name.
    Json(#[from] serde_json::Error),
    #[error("unsupported simulation counterexample artifact schema version {0}")]
    /// Variant $name.
    UnsupportedSchemaVersion(u32),
    #[error("simulation counterexample replay did not reproduce: {0}")]
    /// Variant $name.
    ReplayMismatch(String),
    #[error("simulation counterexample artifact path is not valid UTF-8: {path:?}")]
    /// Variant $name.
    InvalidArtifactPath {
        /// The path value.
        path: PathBuf,
    },
}

/// Function $name.
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

/// Function $name.
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
        SimulationScenarioGroup::SparsePayloadChaos => run_sparse_payload_chaos(seeds, options),
        SimulationScenarioGroup::ColdObserverLongHistory => {
            run_cold_observer_long_history(seeds, options)
        }
        SimulationScenarioGroup::InvalidationDropoutFallbackSync => {
            run_invalidation_dropout_fallback_sync(seeds, options)
        }
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
        for _ in 0..metadata_recovery_flaps(*seed) {
            runtime.set_metadata_available(true);
            runtime.set_metadata_available(false);
            let _ = runtime.deliver_next_metadata("reader");
        }
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
        for _ in 0..object_store_unavailable_read_probes(*seed) {
            let _ = store.read_remote(&object_id);
        }
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

fn run_sparse_payload_chaos(
    seeds: &[u64],
    options: &SimulationCorpusOptions,
) -> Result<GroupOutcome, SimulationCorpusError> {
    let mut seed_reports = Vec::new();
    let mut failures = Vec::new();
    for seed in seeds {
        let mut runtime = SimulatedRuntime::new(*seed);
        runtime.add_client("writer");
        runtime.add_client("reader");
        runtime.create_file("writer", "sparse.bin")?;

        match seed % 4 {
            0 => {
                runtime.write_sparse_range("writer", "sparse.bin", 4, b"abcd")?;
                runtime.write_sparse_range("writer", "sparse.bin", 12, b"tail")?;
                runtime.truncate_sparse_file("writer", "sparse.bin", 18)?;
            }
            1 => {
                runtime.truncate_sparse_file("writer", "sparse.bin", 16)?;
                runtime.write_sparse_range("writer", "sparse.bin", 12, b"tail")?;
                runtime.write_sparse_range("writer", "sparse.bin", 4, b"abcd")?;
            }
            2 => {
                runtime.write_sparse_range("writer", "sparse.bin", 0, b"head")?;
                runtime.write_sparse_range("writer", "sparse.bin", 8, b"stale-tail")?;
                runtime.write_sparse_range("writer", "sparse.bin", 10, b"new")?;
                runtime.truncate_sparse_file("writer", "sparse.bin", 13)?;
            }
            _ => {
                runtime.write_sparse_range("writer", "sparse.bin", 6, b"middle")?;
                runtime.truncate_sparse_file("writer", "sparse.bin", 4)?;
                runtime.truncate_sparse_file("writer", "sparse.bin", 12)?;
                runtime.write_sparse_range("writer", "sparse.bin", 8, b"tail")?;
            }
        }

        for _ in 0..sparse_read_probes(*seed) {
            let _ = runtime.read_file_range("writer", "sparse.bin", 0, 6)?;
        }
        runtime.flush("writer", "sparse.bin")?;
        let upload = runtime.schedule_upload("writer", "sparse.bin")?;
        runtime.complete_upload(upload.upload_id)?;
        runtime.publish_file("writer", "sparse.bin")?;
        runtime.deliver_next_metadata("reader")?;

        let writer_probe = runtime.read_file_range("writer", "sparse.bin", 0, 18)?;
        let reader_probe = runtime.read_file_range("reader", "sparse.bin", 0, 18)?;
        let invariant_report = validate_logical_invariants(&runtime.logical_snapshot());
        let passed = invariant_report.passed
            && writer_probe.bytes == reader_probe.bytes
            && writer_probe.holes == reader_probe.holes
            && !reader_probe.holes.is_empty();

        if !passed {
            let (violated_invariant, base_message) =
                first_violation_or(&invariant_report, "sparse_payload_range_convergence");
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::SparsePayloadChaos,
                seed: *seed,
                violated_invariant,
                message: format!(
                    "{base_message}; expected bytes={:?} holes={:?}, actual bytes={:?} holes={:?}",
                    writer_probe.bytes, writer_probe.holes, reader_probe.bytes, reader_probe.holes
                ),
                counterexample_artifact_path: None,
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: Some(format!("ordering-{}", seed % 4)),
                    scheduler_trace: synthetic_scheduler_trace(*seed, "sparse-payload-final"),
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
                group: SimulationScenarioGroup::SparsePayloadChaos,
                seed: *seed,
                violated_invariant: "injected_counterexample".to_string(),
                message: "injected sparse payload failure for corpus artifact testing".to_string(),
                counterexample_artifact_path: None,
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: Some(format!("ordering-{}", seed % 4)),
                    scheduler_trace: synthetic_scheduler_trace(*seed, "sparse-payload-injected"),
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
            invariant_checkpoints: 2 + sparse_read_probes(*seed),
            trace_records: runtime.trace().len(),
        });
    }
    Ok(GroupOutcome {
        seed_reports,
        failures,
    })
}

fn run_cold_observer_long_history(
    seeds: &[u64],
    options: &SimulationCorpusOptions,
) -> Result<GroupOutcome, SimulationCorpusError> {
    let mut seed_reports = Vec::new();
    let mut failures = Vec::new();
    for seed in seeds {
        let mut run = cold_observer_long_history_seed(*seed)?;
        let invariant_report = validate_logical_invariants(&run.final_snapshot);
        let cold_a = run
            .runtime
            .read_file("cold", "history/a.txt")?
            .checksum_sha256;
        let warm_sparse = run
            .runtime
            .read_file_range("warm", "history/sparse.bin", 0, 24)?;
        let cold_sparse = run
            .runtime
            .read_file_range("cold", "history/sparse.bin", 0, 24)?;
        let passed = invariant_report.passed
            && cold_a == "checksum-a-v2"
            && warm_sparse.bytes == cold_sparse.bytes
            && warm_sparse.holes == cold_sparse.holes
            && run.replay_idempotent;

        if !passed {
            let (violated_invariant, base_message) =
                first_violation_or(&invariant_report, "cold_observer_long_history_convergence");
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::ColdObserverLongHistory,
                seed: *seed,
                violated_invariant,
                message: format!(
                    "{base_message}; cold_a={cold_a} replay_idempotent={} warm_sparse_checksum={} cold_sparse_checksum={}",
                    run.replay_idempotent,
                    warm_sparse.checksum_sha256,
                    cold_sparse.checksum_sha256
                ),
                counterexample_artifact_path: None,
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: Some(cold_observer_variant(*seed).to_string()),
                    scheduler_trace: nonempty_scheduler_trace(
                        *seed,
                        run.scheduler_trace.clone(),
                        "cold-observer-long-history-final",
                    ),
                    operation_log: operation_log_from_client_trace(
                        CORPUS_SCENARIO,
                        run.runtime.trace(),
                    ),
                    event_trace: json_trace(run.runtime.trace())?,
                    final_snapshot: Some(json_value(&run.final_snapshot)?),
                }),
            });
        }
        if options.inject_failure {
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::ColdObserverLongHistory,
                seed: *seed,
                violated_invariant: "injected_counterexample".to_string(),
                message: "injected cold observer long-history failure for corpus artifact testing"
                    .to_string(),
                counterexample_artifact_path: None,
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: Some(cold_observer_variant(*seed).to_string()),
                    scheduler_trace: nonempty_scheduler_trace(
                        *seed,
                        run.scheduler_trace.clone(),
                        "cold-observer-long-history-injected",
                    ),
                    operation_log: operation_log_from_client_trace(
                        CORPUS_SCENARIO,
                        run.runtime.trace(),
                    ),
                    event_trace: json_trace(run.runtime.trace())?,
                    final_snapshot: Some(json_value(&run.final_snapshot)?),
                }),
            });
        }
        seed_reports.push(SimulationSeedReport {
            seed: *seed,
            passed: passed && !options.inject_failure,
            invariant_checkpoints: run.invariant_checkpoints,
            trace_records: run.runtime.trace().len() + run.scheduler_trace.len(),
        });
    }
    Ok(GroupOutcome {
        seed_reports,
        failures,
    })
}

fn run_invalidation_dropout_fallback_sync(
    seeds: &[u64],
    options: &SimulationCorpusOptions,
) -> Result<GroupOutcome, SimulationCorpusError> {
    let mut seed_reports = Vec::new();
    let mut failures = Vec::new();
    for seed in seeds {
        let run = invalidation_dropout_seed(*seed)?;
        let invariant_report = validate_logical_invariants(&run.final_snapshot);
        let reader_checksum = run
            .runtime
            .read_file("reader", "notify/dropout.txt")?
            .checksum_sha256;
        let passed = invariant_report.passed
            && reader_checksum == "checksum-hint-v2"
            && run.hints_disrupted > 0
            && run.fallback_syncs > 0
            && run.stale_windows <= run.fallback_bound;

        if !passed {
            let (violated_invariant, base_message) =
                first_violation_or(&invariant_report, "invalidation_dropout_fallback_sync");
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::InvalidationDropoutFallbackSync,
                seed: *seed,
                violated_invariant,
                message: format!(
                    "{base_message}; reader_checksum={reader_checksum} stale_windows={} fallback_bound={} fallback_syncs={} hints_disrupted={}",
                    run.stale_windows, run.fallback_bound, run.fallback_syncs, run.hints_disrupted
                ),
                counterexample_artifact_path: None,
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: Some(invalidation_dropout_variant(*seed).to_string()),
                    scheduler_trace: nonempty_scheduler_trace(
                        *seed,
                        run.scheduler_trace.clone(),
                        "invalidation-dropout-final",
                    ),
                    operation_log: operation_log_from_client_trace(
                        CORPUS_SCENARIO,
                        run.runtime.trace(),
                    ),
                    event_trace: json_trace(run.runtime.trace())?,
                    final_snapshot: Some(json_value(&run.final_snapshot)?),
                }),
            });
        }
        if options.inject_failure {
            failures.push(SimulationCorpusFailure {
                group: SimulationScenarioGroup::InvalidationDropoutFallbackSync,
                seed: *seed,
                violated_invariant: "injected_counterexample".to_string(),
                message: "injected invalidation dropout failure for corpus artifact testing"
                    .to_string(),
                counterexample_artifact_path: None,
                replay_evidence: Some(SimulationCounterexampleEvidence {
                    sub_group: Some(invalidation_dropout_variant(*seed).to_string()),
                    scheduler_trace: nonempty_scheduler_trace(
                        *seed,
                        run.scheduler_trace.clone(),
                        "invalidation-dropout-injected",
                    ),
                    operation_log: operation_log_from_client_trace(
                        CORPUS_SCENARIO,
                        run.runtime.trace(),
                    ),
                    event_trace: json_trace(run.runtime.trace())?,
                    final_snapshot: Some(json_value(&run.final_snapshot)?),
                }),
            });
        }
        seed_reports.push(SimulationSeedReport {
            seed: *seed,
            passed: passed && !options.inject_failure,
            invariant_checkpoints: run.invariant_checkpoints,
            trace_records: run.runtime.trace().len() + run.scheduler_trace.len(),
        });
    }
    Ok(GroupOutcome {
        seed_reports,
        failures,
    })
}

fn invalidation_dropout_seed(seed: u64) -> Result<InvalidationDropoutRun, SimulationCorpusError> {
    let mut runtime = SimulatedRuntime::new(seed);
    runtime.add_client("writer");
    runtime.add_client("reader");
    runtime.mkdir("writer", "notify")?;
    runtime.create_file("writer", "notify/dropout.txt")?;
    publish_checksum(&mut runtime, "notify/dropout.txt", "checksum-hint-v1", 16)?;

    let variant_index = invalidation_dropout_variant_index(seed);
    let fallback_bound = variant_index + 1;
    let mut stale_windows = 0;
    let mut fallback_syncs = 0;
    let mut hints_disrupted = 0;

    match variant_index {
        0 => {
            hints_disrupted += usize::from(runtime.drop_next_metadata_hint("reader")?.is_some());
            stale_windows += usize::from(reader_is_stale(
                &runtime,
                "notify/dropout.txt",
                "checksum-hint-v2",
            )?);
            publish_checksum(&mut runtime, "notify/dropout.txt", "checksum-hint-v2", 17)?;
            runtime.fallback_sync_metadata("reader")?;
            fallback_syncs += 1;
            runtime.deliver_all_metadata("reader")?;
        }
        1 => {
            hints_disrupted += usize::from(runtime.delay_next_metadata_hint("reader")?.is_some());
            stale_windows += usize::from(runtime.deliver_next_metadata("reader").is_err());
            publish_checksum(&mut runtime, "notify/dropout.txt", "checksum-hint-v2", 17)?;
            runtime.fallback_sync_metadata("reader")?;
            fallback_syncs += 1;
            runtime.deliver_all_metadata("reader")?;
        }
        2 => {
            runtime.deliver_next_metadata("reader")?;
            publish_checksum(&mut runtime, "notify/dropout.txt", "checksum-hint-v2", 17)?;
            hints_disrupted += usize::from(runtime.drop_next_metadata_hint("reader")?.is_some());
            stale_windows += usize::from(reader_is_stale(
                &runtime,
                "notify/dropout.txt",
                "checksum-hint-v2",
            )?);
            runtime.fallback_sync_metadata("reader")?;
            fallback_syncs += 1;
            runtime.deliver_all_metadata("reader")?;
        }
        _ => {
            runtime.deliver_next_metadata("reader")?;
            publish_checksum(&mut runtime, "notify/dropout.txt", "checksum-hint-v2", 17)?;
            hints_disrupted += usize::from(runtime.delay_next_metadata_hint("reader")?.is_some());
            stale_windows += usize::from(runtime.deliver_next_metadata("reader").is_err());
            runtime.fallback_sync_metadata("reader")?;
            fallback_syncs += 1;
            runtime.deliver_all_metadata("reader")?;
        }
    }

    let mut final_snapshot = runtime.logical_snapshot();
    final_snapshot.replay_idempotent = Some(true);
    Ok(InvalidationDropoutRun {
        runtime,
        scheduler_trace: invalidation_dropout_scheduler_trace(
            seed,
            invalidation_dropout_variant(seed),
            "reader",
            fallback_bound,
        ),
        final_snapshot,
        stale_windows,
        fallback_bound,
        fallback_syncs,
        hints_disrupted,
        invariant_checkpoints: 3 + fallback_bound,
    })
}

fn publish_checksum(
    runtime: &mut SimulatedRuntime,
    path: &str,
    checksum: &str,
    size: u64,
) -> Result<(), SimulationCorpusError> {
    runtime.write_file("writer", path, size, checksum)?;
    runtime.flush("writer", path)?;
    let upload = runtime.schedule_upload("writer", path)?;
    runtime.complete_upload(upload.upload_id)?;
    runtime.publish_file("writer", path)?;
    Ok(())
}

fn reader_is_stale(
    runtime: &SimulatedRuntime,
    path: &str,
    expected_checksum: &str,
) -> Result<bool, SimulationCorpusError> {
    match runtime.read_file("reader", path) {
        Ok(view) => Ok(view.checksum_sha256 != expected_checksum),
        Err(ClientRuntimeError::PathNotFound { .. }) => Ok(true),
        Err(error) => Err(error.into()),
    }
}

fn invalidation_dropout_scheduler_trace(
    seed: u64,
    variant: &str,
    client: &str,
    fallback_bound: usize,
) -> Vec<SchedulerTraceRecord> {
    (0..fallback_bound)
        .map(|index| SchedulerTraceRecord {
            step: (index + 1) as u64,
            seed,
            virtual_tick: index as u64,
            event_id: (index + 1) as u64,
            event_type: SchedulerEventType::MetadataDelivery,
            client: Some(client.to_string()),
            description: Some(format!("{variant}:fallback-window-{index}")),
            interleaving_choice: 0,
            ready_event_count: 1,
            ready_event_ids: vec![(index + 1) as u64],
        })
        .collect()
}

fn cold_observer_long_history_seed(seed: u64) -> Result<ColdObserverRun, SimulationCorpusError> {
    let mut runtime = SimulatedRuntime::new(seed);
    runtime.add_client("writer");
    runtime.add_client("warm");
    runtime.mkdir("writer", "history")?;

    runtime.create_file("writer", "history/a.txt")?;
    runtime.write_file("writer", "history/a.txt", 7, "checksum-a-v1")?;
    runtime.flush("writer", "history/a.txt")?;
    runtime.create_file("writer", "history/b.txt")?;
    runtime.write_file("writer", "history/b.txt", 7, "checksum-b-v1")?;
    runtime.flush("writer", "history/b.txt")?;
    runtime.create_file("writer", "history/deleted.txt")?;
    runtime.write_file("writer", "history/deleted.txt", 13, "checksum-deleted-v1")?;
    runtime.flush("writer", "history/deleted.txt")?;
    runtime.create_file("writer", "history/sparse.bin")?;
    runtime.write_sparse_range("writer", "history/sparse.bin", 0, b"head")?;
    runtime.write_sparse_range("writer", "history/sparse.bin", 16, b"tail")?;
    runtime.flush("writer", "history/sparse.bin")?;

    let mut scheduler =
        SimulationScheduler::new_with_stream_seed(seed, simulation_stream_seed(seed, 397));
    publish_paths_in_scheduler_order(
        &mut runtime,
        &mut scheduler,
        &[
            "history/a.txt",
            "history/b.txt",
            "history/deleted.txt",
            "history/sparse.bin",
        ],
    )?;

    runtime.write_file("writer", "history/a.txt", 11, "checksum-a-v2")?;
    runtime.flush("writer", "history/a.txt")?;
    runtime.write_sparse_range("writer", "history/sparse.bin", 8, b"middle")?;
    runtime.truncate_sparse_file("writer", "history/sparse.bin", 22)?;
    runtime.flush("writer", "history/sparse.bin")?;
    publish_paths_in_scheduler_order(
        &mut runtime,
        &mut scheduler,
        &["history/a.txt", "history/sparse.bin"],
    )?;

    runtime.publish_unlink("writer", "history/b.txt")?;
    runtime.publish_unlink("writer", "history/deleted.txt")?;
    runtime.add_client("cold");

    schedule_history_delivery(&mut scheduler);
    let mut warm_delivered = false;
    let mut cold_imported = false;
    while let Some(selected) = scheduler.step() {
        match selected.event.description.as_deref() {
            Some("warm-history-delivery") => {
                runtime.deliver_all_metadata("warm")?;
                warm_delivered = true;
            }
            Some("cold-history-import") => {
                runtime.import_metadata_history("cold")?;
                runtime.deliver_all_metadata("cold")?;
                cold_imported = true;
            }
            _ => {}
        }
    }
    if !warm_delivered {
        runtime.deliver_all_metadata("warm")?;
    }
    if !cold_imported {
        runtime.import_metadata_history("cold")?;
        runtime.deliver_all_metadata("cold")?;
    }

    let before_replay = runtime.logical_snapshot();
    for _ in 0..=cold_observer_variant_index(seed) {
        runtime.replay_metadata_history("cold")?;
        runtime.deliver_all_metadata("cold")?;
    }
    let after_replay = runtime.logical_snapshot();
    let replay_idempotent = before_replay.clients == after_replay.clients;
    let mut final_snapshot = after_replay;
    final_snapshot.expected_absent_paths = vec![
        "history/b.txt".to_string(),
        "history/deleted.txt".to_string(),
    ];
    final_snapshot.replay_idempotent = Some(replay_idempotent);

    Ok(ColdObserverRun {
        runtime,
        scheduler_trace: scheduler.trace().to_vec(),
        final_snapshot,
        replay_idempotent,
        invariant_checkpoints: 3 + cold_observer_variant_index(seed),
    })
}

fn publish_paths_in_scheduler_order(
    runtime: &mut SimulatedRuntime,
    scheduler: &mut SimulationScheduler,
    paths: &[&str],
) -> Result<(), SimulationCorpusError> {
    let mut uploads = BTreeMap::new();
    for path in paths {
        let upload = runtime.schedule_upload("writer", path)?;
        uploads.insert((*path).to_string(), upload.upload_id);
        scheduler.schedule_now(
            ScheduledEvent::new(SchedulerEventType::UploadCompletion)
                .with_client("writer")
                .with_description(format!("complete-upload:{path}")),
        );
    }
    while let Some(selected) = scheduler.step() {
        let Some(path) = selected
            .event
            .description
            .as_deref()
            .and_then(|description| description.strip_prefix("complete-upload:"))
        else {
            continue;
        };
        let Some(upload_id) = uploads.remove(path) else {
            continue;
        };
        runtime.complete_upload(upload_id)?;
        runtime.publish_file("writer", path)?;
        if uploads.is_empty() {
            break;
        }
    }
    Ok(())
}

fn schedule_history_delivery(scheduler: &mut SimulationScheduler) {
    scheduler.schedule_now(
        ScheduledEvent::new(SchedulerEventType::MetadataDelivery)
            .with_client("warm")
            .with_description("warm-history-delivery"),
    );
    scheduler.schedule_now(
        ScheduledEvent::new(SchedulerEventType::MetadataDelivery)
            .with_client("cold")
            .with_description("cold-history-import"),
    );
}

fn cold_observer_variant(seed: u64) -> &'static str {
    match cold_observer_variant_index(seed) {
        0 => "upload_order_a",
        1 => "upload_order_b",
        2 => "observer_import_before_warm_delivery",
        _ => "warm_delivery_before_observer_import",
    }
}

fn cold_observer_variant_index(seed: u64) -> usize {
    (seed % COLD_OBSERVER_SEED_VARIANTS) as usize
}

fn invalidation_dropout_variant(seed: u64) -> &'static str {
    match invalidation_dropout_variant_index(seed) {
        0 => "miss_first_hint_before_writer_continues",
        1 => "delay_first_hint_until_fallback",
        2 => "miss_second_hint_after_initial_apply",
        _ => "delay_second_hint_until_fallback",
    }
}

fn invalidation_dropout_variant_index(seed: u64) -> usize {
    (seed % INVALIDATION_DROPOUT_SEED_VARIANTS) as usize
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
    let mut reports_by_seed: BTreeMap<u64, Vec<&SeedReport>> = BTreeMap::new();
    for family_report in family_reports {
        for seed_report in seed_reports(family_report) {
            reports_by_seed
                .entry(seed_value(seed_report))
                .or_default()
                .push(seed_report);
        }
    }

    seeds
        .iter()
        .map(|seed| {
            let reports = reports_by_seed.get(seed).map_or(&[][..], Vec::as_slice);
            SimulationSeedReport {
                seed: *seed,
                passed: reports.iter().all(|&seed_report| seed_passed(seed_report)),
                invariant_checkpoints: reports
                    .iter()
                    .map(|&seed_report| invariant_checkpoints(seed_report))
                    .sum(),
                trace_records: reports
                    .iter()
                    .map(|&seed_report| trace_records(seed_report))
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

/// Function $name.
pub fn read_simulation_counterexample(
    path: impl AsRef<Path>,
) -> Result<SimulationCounterexampleArtifact, SimulationCorpusError> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let artifact_json: Value = serde_json::from_reader(reader)?;
    let schema_version = artifact_json
        .get("schema_version")
        .and_then(Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
        .ok_or(SimulationCorpusError::UnsupportedSchemaVersion(0))?;
    if schema_version != SIMULATION_COUNTEREXAMPLE_SCHEMA_VERSION {
        return Err(SimulationCorpusError::UnsupportedSchemaVersion(
            schema_version,
        ));
    }
    serde_json::from_value(artifact_json).map_err(SimulationCorpusError::Json)
}

/// Function $name.
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
            let scenario_operation = client_trace_operation(record);
            let mut operation =
                ScenarioOperationRecord::new(scenario, record.step, scenario_operation)
                    .with_outcome(OperationOutcome::Observed);
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

fn client_trace_operation(record: &crate::client::ClientTraceRecord) -> ScenarioOperation {
    let path = record.path.clone().unwrap_or_default();
    match record.kind.as_str() {
        "write_sparse_range" => ScenarioOperation::Write {
            path,
            offset: record
                .metadata
                .get("offset")
                .and_then(|value| value.parse().ok()),
            bytes: record
                .metadata
                .get("bytes")
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
            checksum_sha256: record.metadata.get("checksum_sha256").cloned(),
        },
        "truncate_sparse_file" => ScenarioOperation::Truncate {
            path,
            size: record
                .metadata
                .get("size")
                .and_then(|value| value.parse().ok())
                .unwrap_or_default(),
        },
        "read_file_range" => ScenarioOperation::Read {
            path,
            bytes: record
                .metadata
                .get("bytes")
                .and_then(|value| value.parse().ok()),
            checksum_sha256: record.metadata.get("checksum_sha256").cloned(),
        },
        _ => ScenarioOperation::SchedulerCheckpoint {
            name: record.kind.clone(),
        },
    }
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

fn metadata_recovery_flaps(seed: u64) -> usize {
    (seed % OUTAGE_SEED_VARIANTS) as usize
}

fn object_store_unavailable_read_probes(seed: u64) -> usize {
    (seed % OUTAGE_SEED_VARIANTS) as usize
}

fn sparse_read_probes(seed: u64) -> usize {
    (seed % SPARSE_SEED_VARIANTS) as usize
}

const OUTAGE_SEED_VARIANTS: u64 = 8;
const SPARSE_SEED_VARIANTS: u64 = 4;
const COLD_OBSERVER_SEED_VARIANTS: u64 = 4;
const INVALIDATION_DROPOUT_SEED_VARIANTS: u64 = 4;

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
        SimulationScenarioGroup::SparsePayloadChaos,
        SimulationScenarioGroup::ColdObserverLongHistory,
        SimulationScenarioGroup::InvalidationDropoutFallbackSync,
    ]
}

/// Function $name.
pub fn corpus_arg(kind: SimulationCorpusKind) -> &'static str {
    match kind {
        SimulationCorpusKind::Fast => "fast",
        SimulationCorpusKind::Deep => "deep",
    }
}

/// Function $name.
pub fn group_arg(group: SimulationScenarioGroup) -> &'static str {
    match group {
        SimulationScenarioGroup::NamespaceCollisions => "namespace-collisions",
        SimulationScenarioGroup::CrashRestartReplay => "crash-restart-replay",
        SimulationScenarioGroup::MetadataOutage => "metadata-outage",
        SimulationScenarioGroup::ObjectStoreOutage => "object-store-outage",
        SimulationScenarioGroup::SparsePayloadChaos => "sparse-payload-chaos",
        SimulationScenarioGroup::ColdObserverLongHistory => "cold-observer-long-history",
        SimulationScenarioGroup::InvalidationDropoutFallbackSync => {
            "invalidation-dropout-fallback-sync"
        }
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

struct ColdObserverRun {
    runtime: SimulatedRuntime,
    scheduler_trace: Vec<SchedulerTraceRecord>,
    final_snapshot: LogicalSnapshot,
    replay_idempotent: bool,
    invariant_checkpoints: usize,
}

struct InvalidationDropoutRun {
    runtime: SimulatedRuntime,
    scheduler_trace: Vec<SchedulerTraceRecord>,
    final_snapshot: LogicalSnapshot,
    stale_windows: usize,
    fallback_bound: usize,
    fallback_syncs: usize,
    hints_disrupted: usize,
    invariant_checkpoints: usize,
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

    #[test]
    fn outage_seed_variants_are_bounded() {
        assert_eq!(metadata_recovery_flaps(1), 1);
        assert_eq!(metadata_recovery_flaps(8), 0);
        assert_eq!(object_store_unavailable_read_probes(3), 3);
        assert_eq!(object_store_unavailable_read_probes(16), 0);
    }
}

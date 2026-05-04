use std::ffi::OsStr;
use std::path::PathBuf;

use studiofs_scenario::crash_replay::replay_crash_replay_counterexample;
use studiofs_scenario::seed_corpus::{
    SimulationCorpusKind, SimulationCorpusOptions, SimulationScenarioGroup, group_arg,
    replay_simulation_counterexample, run_simulation_corpus,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args_os().skip(1);
    match args.next() {
        Some(command) if command == OsStr::new("replay-counterexample") => {
            let path = args
                .next()
                .map(PathBuf::from)
                .ok_or_else(|| "missing counterexample artifact path".to_string())?;
            if args.next().is_some() {
                return Err("unexpected extra arguments".to_string());
            }
            match replay_simulation_counterexample(&path) {
                Ok(report) => {
                    println!(
                        "reproduced {} group {} seed {} invariant {}: {}",
                        report.scenario,
                        group_label(report.group),
                        report.seed,
                        report.violated_invariant,
                        report.message
                    );
                }
                Err(simulation_error) => {
                    let report = replay_crash_replay_counterexample(&path).map_err(|crash_error| {
                        format!(
                            "counterexample replay failed: simulation artifact error: {simulation_error}; crash/restart artifact error: {crash_error}"
                        )
                    })?;
                    println!(
                        "reproduced {} seed {} invariant {}: {}",
                        report.scenario, report.seed, report.violated_invariant, report.message
                    );
                }
            }
            Ok(())
        }
        Some(command) if command == OsStr::new("run-corpus") => run_corpus(args.collect()),
        _ => Err(
            "usage: studiofs-scenario replay-counterexample <counterexample-artifact.json>\n       studiofs-scenario run-corpus --corpus <fast|deep> [--scenario-group <group>] [--seed <n>] [--artifact-root <path>]"
                .to_string(),
        ),
    }
}

fn run_corpus(args: Vec<std::ffi::OsString>) -> Result<(), String> {
    let mut kind = None;
    let mut group = None;
    let mut seed = None;
    let mut artifact_root = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        if arg == OsStr::new("--corpus") {
            kind = Some(parse_corpus_kind(
                iter.next()
                    .ok_or_else(|| "missing --corpus value".to_string())?,
            )?);
        } else if arg == OsStr::new("--scenario-group") {
            group =
                Some(parse_group(iter.next().ok_or_else(|| {
                    "missing --scenario-group value".to_string()
                })?)?);
        } else if arg == OsStr::new("--seed") {
            let value = iter
                .next()
                .ok_or_else(|| "missing --seed value".to_string())?;
            let value = value
                .to_str()
                .ok_or_else(|| "--seed must be valid UTF-8".to_string())?;
            seed = Some(
                value
                    .parse::<u64>()
                    .map_err(|error| format!("invalid --seed value: {error}"))?,
            );
        } else if arg == OsStr::new("--artifact-root") {
            artifact_root =
                Some(PathBuf::from(iter.next().ok_or_else(|| {
                    "missing --artifact-root value".to_string()
                })?));
        } else {
            return Err(format!(
                "unknown run-corpus argument: {}",
                arg.to_string_lossy()
            ));
        }
    }

    let kind = kind.ok_or_else(|| "missing --corpus <fast|deep>".to_string())?;
    let report = run_simulation_corpus(SimulationCorpusOptions {
        kind,
        group,
        seed,
        artifact_root: artifact_root.clone(),
        inject_failure: false,
    })
    .map_err(|error| format!("simulation corpus failed: {error}"))?;

    println!("scenario={}", report.scenario);
    println!("corpus={}", corpus_label(kind));
    if let Some(root) = artifact_root {
        println!("artifact_root={}", root.display());
    }
    for group_report in &report.group_reports {
        for seed_report in &group_report.seed_reports {
            println!(
                "group={} seed={} status={} invariant_checkpoints={} trace_records={}",
                group_label(group_report.group),
                seed_report.seed,
                if seed_report.passed {
                    "passed"
                } else {
                    "failed"
                },
                seed_report.invariant_checkpoints,
                seed_report.trace_records
            );
        }
    }
    for failure in &report.failures {
        println!(
            "failure group={} seed={} invariant={} artifact_path={}",
            group_label(failure.group),
            failure.seed,
            failure.violated_invariant,
            failure
                .counterexample_artifact_path
                .as_deref()
                .unwrap_or("<none>")
        );
    }

    if report.passed {
        Ok(())
    } else {
        Err("simulation corpus failed".to_string())
    }
}

fn parse_corpus_kind(value: std::ffi::OsString) -> Result<SimulationCorpusKind, String> {
    match value.to_str() {
        Some("fast") => Ok(SimulationCorpusKind::Fast),
        Some("deep") => Ok(SimulationCorpusKind::Deep),
        Some(other) => Err(format!("unknown corpus kind: {other}")),
        None => Err("--corpus must be valid UTF-8".to_string()),
    }
}

fn parse_group(value: std::ffi::OsString) -> Result<SimulationScenarioGroup, String> {
    match value.to_str() {
        Some("namespace-collisions") => Ok(SimulationScenarioGroup::NamespaceCollisions),
        Some("crash-restart-replay") => Ok(SimulationScenarioGroup::CrashRestartReplay),
        Some("metadata-outage") => Ok(SimulationScenarioGroup::MetadataOutage),
        Some("object-store-outage") => Ok(SimulationScenarioGroup::ObjectStoreOutage),
        Some(other) => Err(format!("unknown scenario group: {other}")),
        None => Err("--scenario-group must be valid UTF-8".to_string()),
    }
}

fn corpus_label(kind: SimulationCorpusKind) -> &'static str {
    match kind {
        SimulationCorpusKind::Fast => "fast",
        SimulationCorpusKind::Deep => "deep",
    }
}

fn group_label(group: SimulationScenarioGroup) -> String {
    group_arg(group).to_string()
}

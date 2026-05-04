use std::ffi::OsStr;
use std::path::PathBuf;

use studiofs_scenario::crash_replay::replay_crash_replay_counterexample;

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
            let report = replay_crash_replay_counterexample(&path)
                .map_err(|error| format!("counterexample replay failed: {error}"))?;
            println!(
                "reproduced {} seed {} invariant {}: {}",
                report.scenario, report.seed, report.violated_invariant, report.message
            );
            Ok(())
        }
        _ => Err(
            "usage: studiofs-scenario replay-counterexample <counterexample-artifact.json>"
                .to_string(),
        ),
    }
}

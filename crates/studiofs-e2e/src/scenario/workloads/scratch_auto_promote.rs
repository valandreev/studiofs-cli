//! Mounted acceptance for automatic scratch promotion when ignore rules are removed (SFS-1081).

use super::*;

pub(in crate::scenario) const SCRATCH_AUTO_PROMOTE: &str =
    "mounted-scratch-auto-promote-two-client";
pub(in crate::scenario) struct ScratchAutoPromote;
const WAIT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);
const OUT_PAYLOAD: &[u8] = b"build output promoted after rule removal\n";
const A_PAYLOAD: &[u8] = b"client-a scratch entry\n";
const B_PAYLOAD: &[u8] = b"client-b scratch entry\n";
const SUMMARY: &str = "scratch-auto-promote.json";

impl Scenario for ScratchAutoPromote {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_AUTO_PROMOTE,
            description: "rule removal auto-promotes scratch entries and merges a directory both clients held",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let result = (|| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: vec!["client-a".into(), "client-b".into()],
                clean_existing_local_metadata: true,
            })?;
            controller.mount_all()?;
            let a = controller.clients()[0].mount_point.clone();
            let observer = controller.clients()[1].clone();
            let b = observer.mount_point.clone();

            let rules = b"/build/\n/shared/\n*.tmp\n";
            write_mounted_file_with_readback(
                a.join(".studiofsignore"),
                rules,
                "scratch rule write",
                true,
            )?;
            expect_bytes(&b.join(".studiofsignore"), rules)?;
            // Rule loading is asynchronous: wait until each client classifies a `*.tmp` probe
            // as scratch before creating the real entries.
            let probes_a = wait_for_rules_active(&a, &b, "client-a")?;
            let probes_b = wait_for_rules_active(&b, &a, "client-b")?;

            // Scratch entries on each client: they stay local while the rules match.
            let (build, shared_a, shared_b) = (a.join("build"), a.join("shared"), b.join("shared"));
            run_bounded_mounted_io("create scratch dirs", WAIT, move || {
                fs::create_dir(&build)?;
                fs::create_dir(&shared_a)?;
                fs::create_dir(&shared_b)
            })?;
            write_mounted_file_with_readback(
                a.join("build").join("out.bin"),
                OUT_PAYLOAD,
                "scratch build output",
                true,
            )?;
            write_mounted_file_with_readback(
                a.join("build").join("keep.tmp"),
                b"still ignored\n",
                "scratch rule-matching child",
                true,
            )?;
            write_mounted_file_with_readback(
                a.join("shared").join("a.txt"),
                A_PAYLOAD,
                "client-a scratch shared entry",
                true,
            )?;
            write_mounted_file_with_readback(
                b.join("shared").join("b.txt"),
                B_PAYLOAD,
                "client-b scratch shared entry",
                true,
            )?;
            // A tracked marker written after the scratch entries proves replication caught up.
            write_mounted_file_with_readback(a.join("marker.txt"), b"m\n", "marker", true)?;
            expect_bytes(&b.join("marker.txt"), b"m\n")?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &["build", "shared/a.txt"],
                Instant::now() + WAIT,
                POLL,
                "scratch entries stay local before rule removal",
            )?;

            // Remove the directory rules on client-a; client-b learns it through foreign apply.
            let remaining = b"*.tmp\n";
            write_mounted_file_with_readback(
                a.join(".studiofsignore"),
                remaining,
                "scratch rule removal",
                false,
            )?;
            expect_bytes(&b.join(".studiofsignore"), remaining)?;

            expect_bytes(&b.join("build").join("out.bin"), OUT_PAYLOAD)?;
            // Both clients promoted `shared/`; ADR 0012 merges it on both sides.
            expect_bytes(&b.join("shared").join("a.txt"), A_PAYLOAD)?;
            expect_bytes(&a.join("shared").join("b.txt"), B_PAYLOAD)?;
            expect_bytes(&a.join("shared").join("a.txt"), A_PAYLOAD)?;
            expect_bytes(&b.join("shared").join("b.txt"), B_PAYLOAD)?;

            // `keep.tmp` still matches `*.tmp`: present on client-a, never published.
            expect_bytes(&a.join("build").join("keep.tmp"), b"still ignored\n")?;
            write_mounted_file_with_readback(a.join("marker2.txt"), b"m\n", "marker 2", true)?;
            expect_bytes(&b.join("marker2.txt"), b"m\n")?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &["build/keep.tmp"],
                Instant::now() + WAIT,
                POLL,
                "rule-matching child stays scratch",
            )?;
            let conflict_copies: Vec<String> = [&a, &b]
                .iter()
                .flat_map(|root| fs::read_dir(root).into_iter().flatten().flatten())
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| name.contains("scratch-conflict"))
                .collect();
            if !conflict_copies.is_empty() {
                return Err(io::Error::other(format!(
                    "directories must merge, found conflict copies: {conflict_copies:?}"
                ))
                .into());
            }

            fs::write(
                context.scenario_dir.join(SUMMARY),
                serde_json::to_vec_pretty(&json!({
                    "promoted_after_rule_removal": true,
                    "shared_directory_merged": true,
                    "rule_matching_child_kept_local": true,
                    "rule_probe_attempts": { "client-a": probes_a, "client-b": probes_b },
                }))?,
            )?;
            Ok(())
        })();

        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("scratch auto-promote mounted I/O timeout")
        } else {
            controller.unmount_all()
        }
        .map_err(ScenarioError::from);
        match (result, cleanup) {
            (result, Ok(())) | (Ok(()), result) => result,
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: SCRATCH_AUTO_PROMOTE.into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY))?)?;
        if summary["promoted_after_rule_removal"] != true
            || summary["shared_directory_merged"] != true
            || summary["rule_matching_child_kept_local"] != true
        {
            return Err(io::Error::other("scratch auto-promote acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{SCRATCH_AUTO_PROMOTE}/{SUMMARY}"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

/// Creates `probe-<n>.tmp` then a tracked marker on `writer`; rules are active once the
/// `observer` sees the marker but not the probe. Returns the number of attempts.
fn wait_for_rules_active(
    writer: &Path,
    observer: &Path,
    label: &str,
) -> Result<u32, ScenarioError> {
    let deadline = Instant::now() + WAIT;
    for attempt in 1.. {
        let probe = format!("probe-{label}-{attempt}.tmp");
        let marker = format!("probe-{label}-{attempt}.marker");
        write_mounted_file_with_readback(
            writer.join(&probe),
            b"p
",
            "rule probe",
            true,
        )?;
        write_mounted_file_with_readback(
            writer.join(&marker),
            b"m
",
            "rule probe marker",
            true,
        )?;
        expect_bytes(
            &observer.join(&marker),
            b"m
",
        )?;
        if !observer.join(&probe).exists() {
            return Ok(attempt);
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    Err(io::Error::other(format!("{label}: ignore rules never became active")).into())
}

fn expect_bytes(path: &Path, expected: &[u8]) -> Result<(), ScenarioError> {
    let readback = wait_for_mounted_payload_readback(path, expected, WAIT, POLL)
        .map_err(|error| io::Error::other(format!("{}: {error}", path.display())))?;
    if readback != expected {
        return Err(io::Error::other(format!("payload mismatch at {}", path.display())).into());
    }
    Ok(())
}

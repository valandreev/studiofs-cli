//! Mounted acceptance for moving a published file into a local scratch directory.

use super::*;

pub(in crate::scenario) const SCRATCH_DEMOTE: &str = "mounted-scratch-demote-two-client";
pub(in crate::scenario) struct ScratchDemote;
const WAIT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);
const PAYLOAD: &[u8] = b"published bytes kept by scratch demotion\0\n";
const SUMMARY: &str = "scratch-demote.json";

impl Scenario for ScratchDemote {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_DEMOTE,
            description: "peer removal and offline local read after tracked-to-scratch rename",
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
        let compose = compose_control_from_manifest(context)?;
        let mut controller = MultiClientController::new(config, SystemClientProcessExecutor);
        let mut s3_stopped = false;
        let result = (|| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: vec!["client-a".into(), "client-b".into()],
                clean_existing_local_metadata: true,
            })?;
            controller.mount_all()?;
            let a = controller.clients()[0].mount_point.clone();
            let observer = controller.clients()[1].clone();
            let b = &observer.mount_point;
            let rules = b"/local-scratch/\n";
            write_mounted_file_with_readback(
                a.join(".studiofsignore"),
                rules,
                "scratch rule write",
                true,
            )?;
            expect_bytes(&b.join(".studiofsignore"), rules)?;
            controller.unmount("client-a")?;
            controller.mount("client-a")?;

            let source = a.join("tracked.bin");
            write_mounted_file_with_readback(source.clone(), PAYLOAD, "tracked write", true)?;
            expect_bytes(&b.join("tracked.bin"), PAYLOAD)?;

            let dir = a.join("local-scratch");
            let destination = dir.join("tracked.bin");
            let rename_target = destination.clone();
            run_bounded_mounted_io("rename into scratch", WAIT, move || {
                fs::create_dir(&dir)?;
                fs::rename(source, rename_target)
            })?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &["tracked.bin", "local-scratch"],
                Instant::now() + WAIT,
                POLL,
                "scratch demotion peer removal",
            )?;

            compose.stop_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)?;
            s3_stopped = true;
            // Remount drops kernel page cache and writer memory; bytes must come from local scratch.
            controller.unmount("client-a")?;
            controller.mount("client-a")?;
            expect_bytes(&destination, PAYLOAD)?;
            fs::write(
                context.scenario_dir.join(SUMMARY),
                serde_json::to_vec_pretty(&json!({
                    "peer_removal_observed": true,
                    "offline_payload_bytes": PAYLOAD.len(),
                }))?,
            )?;
            Ok(())
        })();

        let recovery = if s3_stopped {
            compose
                .start_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)
                .map_err(ScenarioError::from)
        } else {
            Ok(())
        };
        let cleanup = if matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        ) {
            controller.emergency_kill_all_no_wait("scratch demote mounted I/O timeout")
        } else {
            controller.unmount_all()
        }
        .map_err(ScenarioError::from);
        match (result, recovery.and(cleanup)) {
            (result, Ok(())) | (Ok(()), result) => result,
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: SCRATCH_DEMOTE.into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY))?)?;
        if summary["peer_removal_observed"] != true
            || summary["offline_payload_bytes"] != PAYLOAD.len()
        {
            return Err(io::Error::other("scratch demote acceptance incomplete").into());
        }
        Ok(vec![format!(
            "artifacts/scenarios/{SCRATCH_DEMOTE}/{SUMMARY}"
        )])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn expect_bytes(path: &Path, expected: &[u8]) -> Result<(), ScenarioError> {
    if wait_for_mounted_payload_readback(path, expected, WAIT, POLL)? != expected {
        return Err(io::Error::other(format!("payload mismatch at {}", path.display())).into());
    }
    Ok(())
}

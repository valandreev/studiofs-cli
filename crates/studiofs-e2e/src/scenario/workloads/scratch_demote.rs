//! Mounted acceptance for moving a published file into a local scratch directory.

use super::*;

pub(in crate::scenario) const SCRATCH_DEMOTE: &str = "mounted-scratch-demote-two-client";
pub(in crate::scenario) struct ScratchDemote;
const WAIT: Duration = Duration::from_secs(90);
const POLL: Duration = Duration::from_millis(200);
const PAYLOAD: &[u8] = b"published bytes kept by scratch demotion\0\n";
const UNCACHED_PAYLOAD: &[u8] = b"published bytes never cached on peer before demote attempt\0\n";
const SUMMARY: &str = "scratch-demote.json";

impl Scenario for ScratchDemote {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: SCRATCH_DEMOTE,
            description: "cached tracked-to-scratch demote succeeds while uncached demote fails with EXDEV",
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
            controller.unmount("client-b")?;
            controller.mount("client-b")?;

            let source = a.join("tracked.bin");
            write_mounted_file_with_readback(source.clone(), PAYLOAD, "tracked write", true)?;
            expect_bytes(&b.join("tracked.bin"), PAYLOAD)?;

            // Write a second tracked file on client-a and observe only its metadata on client-b
            // so its payload stays uncached on client-b.
            write_mounted_file_with_readback(
                a.join("uncached.bin"),
                UNCACHED_PAYLOAD,
                "uncached tracked write",
                true,
            )?;
            wait_for_path_metadata(&b.join("uncached.bin"), UNCACHED_PAYLOAD.len() as u64)?;
            let b_scratch_dir = b.join("local-scratch");
            let b_uncached_src = b.join("uncached.bin");
            let b_uncached_dst = b_scratch_dir.join("uncached.bin");
            let exdev_errno = run_bounded_mounted_io("demote uncached file", WAIT, move || {
                fs::create_dir(&b_scratch_dir)?;
                match fs::rename(&b_uncached_src, &b_uncached_dst) {
                    Ok(()) => Err(io::Error::other(
                        "expected EXDEV when demoting uncached tracked file into scratch",
                    )),
                    Err(error) => {
                        if is_cross_device_error(&error) {
                            Ok(error.raw_os_error())
                        } else {
                            Err(io::Error::other(format!(
                                "expected EXDEV for uncached demote, got: {error} (raw_os_error={:?})",
                                error.raw_os_error()
                            )))
                        }
                    }
                }
            })?;
            // Uncached file must remain intact at its original tracked path.
            expect_bytes(&a.join("uncached.bin"), UNCACHED_PAYLOAD)?;

            let dir = a.join("local-scratch");
            let destination = dir.join("tracked.bin");
            let rename_target = destination.clone();
            run_bounded_mounted_io("rename into scratch", WAIT, move || {
                fs::create_dir(&dir)?;
                fs::rename(source, rename_target)
            })?;
            wait_for_mounted_paths_absent_until(
                std::slice::from_ref(&observer),
                &["tracked.bin"],
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
                    "uncached_demote_exdev": true,
                    "uncached_demote_errno": exdev_errno,
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
            || summary["uncached_demote_exdev"] != true
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

fn wait_for_path_metadata(path: &Path, expected_len: u64) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + WAIT;
    loop {
        let probe_path = path.to_path_buf();
        let meta =
            run_bounded_mounted_io("stat peer file", WAIT, move || {
                match fs::symlink_metadata(&probe_path) {
                    Ok(meta) => Ok(Some(meta)),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(error) => Err(error),
                }
            })?;
        if let Some(meta) = meta
            && meta.is_file()
            && meta.len() == expected_len
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "timed out waiting for peer metadata at {} (expected {expected_len} bytes)",
                path.display()
            ))
            .into());
        }
        thread::sleep(POLL);
    }
}

fn is_cross_device_error(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::CrossesDevices {
        return true;
    }
    #[cfg(unix)]
    {
        if error.raw_os_error() == Some(libc::EXDEV) {
            return true;
        }
    }
    #[cfg(windows)]
    {
        // ERROR_NOT_SAME_DEVICE = 17
        if error.raw_os_error() == Some(17) {
            return true;
        }
    }
    false
}

fn expect_bytes(path: &Path, expected: &[u8]) -> Result<(), ScenarioError> {
    if wait_for_mounted_payload_readback(path, expected, WAIT, POLL)? != expected {
        return Err(io::Error::other(format!("payload mismatch at {}", path.display())).into());
    }
    Ok(())
}

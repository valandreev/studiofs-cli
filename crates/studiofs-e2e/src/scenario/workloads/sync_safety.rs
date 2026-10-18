//! Mounted acceptance for the runtime's local sync-safety diagnostic.

use super::*;

pub(in crate::scenario) const MOUNTED_SYNC_SAFETY_TWO_CLIENT: &str =
    "mounted-sync-safety-two-client-acceptance";
const SYNC_SAFETY_CLIENTS: &[&str] = &["client-a", "client-b"];
const SYNC_SAFETY_FILE: &str = "studiofs-e2e-sync-safety.txt";
const SYNC_SAFETY_PAYLOAD: &[u8] = b"accepted while remote publication is paused\n";
const SYNC_SAFETY_TIMEOUT: Duration = Duration::from_secs(90);
const SYNC_SAFETY_POLL: Duration = Duration::from_millis(250);
const SYNC_SAFETY_STABLE_SAMPLES: usize = 3;

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct MountedSyncSafetyTwoClient;

impl Scenario for MountedSyncSafetyTwoClient {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_SYNC_SAFETY_TWO_CLIENT,
            description: "two-client mounted sync-safety publication acceptance",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let Some(config) = &context.client_controller_config else {
            return Err(ScenarioError::MissingComposeInfrastructure);
        };
        let compose = compose_control_from_manifest(context)?;
        let mut controller =
            MultiClientController::new(config.clone(), SystemClientProcessExecutor);
        let mut services_paused = false;
        let mut clients_mounted = false;
        let result = (|| {
            controller.bootstrap_volume(VolumeBootstrapOptions {
                client_names: SYNC_SAFETY_CLIENTS
                    .iter()
                    .map(|name| (*name).to_string())
                    .collect(),
                clean_existing_local_metadata: true,
            })?;
            clients_mounted = true;
            controller.mount_all()?;
            compose.stop_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)?;
            services_paused = true;
            compose.stop_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;

            let clients = controller.clients().to_vec();
            let writer = clients
                .first()
                .ok_or_else(|| scenario_io("client-a was not bootstrapped"))?;
            let peer = clients
                .get(1)
                .ok_or_else(|| scenario_io("client-b was not bootstrapped"))?;
            let path = writer.mount_point.join(SYNC_SAFETY_FILE);
            bounded_mounted_write(
                path.clone(),
                SYNC_SAFETY_PAYLOAD.to_vec(),
                "sync-safety paused-publication write".to_string(),
                MOUNTED_WRITE_READBACK_TIMEOUT,
            )?;
            let dirty_snapshot = wait_for_sync_safety_state(
                &controller,
                "client-a",
                |snapshot| {
                    is_syncing_or_unknown(snapshot)
                        && has_any_reason(
                            snapshot,
                            &[
                                "pending_writes",
                                "uploading",
                                "publishing_metadata",
                                "metadata_sync_pending",
                            ],
                        )
                },
                "sync-safety did not report known pending work while S3 and PostgreSQL were paused",
            )?;
            let dirty_samples = collect_sync_safety_samples(
                &controller,
                "client-a",
                |snapshot| {
                    is_syncing_or_unknown(snapshot)
                        && has_any_reason(
                            snapshot,
                            &[
                                "pending_writes",
                                "uploading",
                                "publishing_metadata",
                                "metadata_sync_pending",
                            ],
                        )
                },
                "sync-safety lost its known blocker before remote publication resumed",
            )?;
            let dirty_read = wait_for_mounted_payload_readback(
                &path,
                SYNC_SAFETY_PAYLOAD,
                MOUNTED_WRITE_READBACK_TIMEOUT,
                MOUNTED_WRITE_READBACK_POLL_INTERVAL,
            )?;

            compose.start_service(RUSTFS_SERVICE_NAME, &SystemCommandExecutor)?;
            let metadata_wait_snapshot = wait_for_sync_safety_state(
                &controller,
                "client-a",
                is_metadata_wait,
                "payload upload did not drain into a metadata-blocked wait while PostgreSQL remained paused",
            )?;
            let metadata_wait_samples = collect_sync_safety_samples(
                &controller,
                "client-a",
                is_metadata_wait,
                "sync-safety left the upload-drained metadata wait before PostgreSQL resumed",
            )?;
            compose.start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
            services_paused = false;

            let mounts = clients
                .iter()
                .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
                .collect::<Vec<_>>();
            let convergence = wait_for_convergence(
                &mounts,
                context.scenario_dir.join("validation"),
                SYNC_SAFETY_TIMEOUT,
                SYNC_SAFETY_POLL,
            )?;
            let peer_payload = wait_for_mounted_payload_readback(
                &peer.mount_point.join(SYNC_SAFETY_FILE),
                SYNC_SAFETY_PAYLOAD,
                SYNC_SAFETY_TIMEOUT,
                SYNC_SAFETY_POLL,
            )?;
            let safe_snapshot = wait_for_sync_safety_state(
                &controller,
                "client-a",
                |snapshot| snapshot["state"] == "safe",
                "sync-safety did not become safe after peer convergence",
            )?;
            let safe_samples = collect_sync_safety_samples(
                &controller,
                "client-a",
                |snapshot| snapshot["state"] == "safe",
                "sync-safety left safe after peer convergence",
            )?;
            let summary = serde_json::json!({
                "scenario": MOUNTED_SYNC_SAFETY_TWO_CLIENT,
                "run_id": context.run_id,
                "writer": writer.name,
                "peer": peer.name,
                "file": SYNC_SAFETY_FILE,
                "payload_sha256": bytes_sha256(&dirty_read),
                "peer_payload_sha256": bytes_sha256(&peer_payload),
                "dirty_while_publication_paused": dirty_snapshot,
                "dirty_state_samples": dirty_samples,
                "metadata_publication_wait": metadata_wait_snapshot,
                "metadata_wait_samples": metadata_wait_samples,
                "safe_after_peer_convergence": safe_snapshot,
                "safe_state_samples": safe_samples,
                "convergence": convergence.comparison,
            });
            fs::write(
                context.scenario_dir.join("sync-safety-summary.json"),
                serde_json::to_vec_pretty(&summary)?,
            )?;
            Ok::<_, ScenarioError>(())
        })();

        let mut recovery_errors = Vec::new();
        if services_paused {
            for service in [RUSTFS_SERVICE_NAME, POSTGRES_SERVICE_NAME] {
                if let Err(error) = compose.start_service(service, &SystemCommandExecutor) {
                    recovery_errors.push(format!("restart {service}: {error}"));
                }
            }
        }
        if clients_mounted && let Err(error) = controller.unmount_all() {
            recovery_errors.push(format!("unmount clients: {error}"));
        }
        match (result, recovery_errors.is_empty()) {
            (Ok(()), true) => Ok(()),
            (Err(error), true) => Err(error),
            (Ok(()), false) => Err(scenario_io(&recovery_errors.join("; "))),
            (Err(error), false) => Err(scenario_io(&format!(
                "{error}; recovery also failed: {}",
                recovery_errors.join("; ")
            ))),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context.scenario_dir.join("sync-safety-summary.json");
        let summary: serde_json::Value = serde_json::from_slice(&fs::read(&summary_path)?)?;
        if !is_syncing_or_unknown(&summary["dirty_while_publication_paused"]["sync_safety"])
            || !has_any_reason(
                &summary["dirty_while_publication_paused"]["sync_safety"],
                &[
                    "pending_writes",
                    "uploading",
                    "publishing_metadata",
                    "metadata_sync_pending",
                ],
            )
            || !is_metadata_wait(&summary["metadata_publication_wait"]["sync_safety"])
            || summary["safe_after_peer_convergence"]["sync_safety"]["state"] != "safe"
            || !samples_match(&summary["dirty_state_samples"], |snapshot| {
                is_syncing_or_unknown(snapshot)
                    && has_any_reason(
                        snapshot,
                        &[
                            "pending_writes",
                            "uploading",
                            "publishing_metadata",
                            "metadata_sync_pending",
                        ],
                    )
            })
            || !samples_match(&summary["metadata_wait_samples"], is_metadata_wait)
            || !samples_match(&summary["safe_state_samples"], |snapshot| {
                snapshot["state"] == "safe"
            })
            || summary["payload_sha256"] != summary["peer_payload_sha256"]
            || summary["convergence"]["converged"] != true
        {
            return Err(scenario_io(
                "sync-safety acceptance summary did not satisfy its state and peer-content assertions",
            ));
        }
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "scenario": MOUNTED_SYNC_SAFETY_TWO_CLIENT,
                "status": "passed",
                "writer": summary["writer"],
                "peer": summary["peer"],
                "dirty_state": summary["dirty_while_publication_paused"]["sync_safety"]["state"],
                "metadata_wait_state": summary["metadata_publication_wait"]["sync_safety"]["state"],
                "final_state": summary["safe_after_peer_convergence"]["sync_safety"]["state"],
            }))?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{MOUNTED_SYNC_SAFETY_TWO_CLIENT}/sync-safety-summary.json"
            ),
            format!("artifacts/scenarios/{MOUNTED_SYNC_SAFETY_TWO_CLIENT}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn wait_for_sync_safety_state(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    client: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
    timeout_message: &str,
) -> Result<serde_json::Value, ScenarioError> {
    let deadline = Instant::now() + SYNC_SAFETY_TIMEOUT;
    loop {
        let last_snapshot = controller.runtime_snapshot(client)?;
        let sync_safety = &last_snapshot["sync_safety"];
        if predicate(sync_safety) {
            return Ok(last_snapshot);
        }
        if Instant::now() >= deadline {
            return Err(scenario_io(&format!(
                "{timeout_message}; last sync_safety={}",
                sync_safety
            )));
        }
        std::thread::sleep(SYNC_SAFETY_POLL);
    }
}

fn collect_sync_safety_samples(
    controller: &MultiClientController<SystemClientProcessExecutor>,
    client: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
    failure_message: &str,
) -> Result<Vec<serde_json::Value>, ScenarioError> {
    let mut samples = Vec::with_capacity(SYNC_SAFETY_STABLE_SAMPLES);
    for sample_index in 0..SYNC_SAFETY_STABLE_SAMPLES {
        let snapshot = controller.runtime_snapshot(client)?;
        let sync_safety = &snapshot["sync_safety"];
        if !predicate(sync_safety) {
            return Err(scenario_io(&format!(
                "{failure_message}; sample {} observed {sync_safety}",
                sample_index + 1
            )));
        }
        samples.push(snapshot);
        if sample_index + 1 != SYNC_SAFETY_STABLE_SAMPLES {
            std::thread::sleep(SYNC_SAFETY_POLL);
        }
    }
    Ok(samples)
}

fn has_reason(snapshot: &serde_json::Value, expected: &str) -> bool {
    snapshot["reasons"].as_array().is_some_and(|reasons| {
        reasons
            .iter()
            .any(|reason| reason.as_str() == Some(expected))
    })
}

fn has_any_reason(snapshot: &serde_json::Value, expected: &[&str]) -> bool {
    expected.iter().any(|reason| has_reason(snapshot, reason))
}

fn is_syncing_or_unknown(snapshot: &serde_json::Value) -> bool {
    snapshot["state"] == "syncing" || snapshot["state"] == "unknown"
}

fn is_metadata_wait(snapshot: &serde_json::Value) -> bool {
    is_syncing_or_unknown(snapshot)
        && has_any_reason(snapshot, &["metadata_sync_pending", "publishing_metadata"])
        && !has_reason(snapshot, "uploading")
}

fn samples_match(
    samples: &serde_json::Value,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> bool {
    samples.as_array().is_some_and(|samples| {
        samples.len() == SYNC_SAFETY_STABLE_SAMPLES
            && samples
                .iter()
                .all(|sample| predicate(&sample["sync_safety"]))
    })
}

fn scenario_io(message: &str) -> ScenarioError {
    ScenarioError::Io(std::io::Error::other(message.to_string()))
}

#[cfg(test)]
#[path = "tests/sync_safety.rs"]
mod tests;

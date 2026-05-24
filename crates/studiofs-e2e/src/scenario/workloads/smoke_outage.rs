//! Smoke, outage, and crash/restart scenario implementations.

use super::*;

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct BasicMultiClientSmoke;

impl Scenario for BasicMultiClientSmoke {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: BASIC_MULTI_CLIENT_SMOKE,
            description: "mounted multi-client smoke scenario",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        if let Some(config) = &context.client_controller_config {
            let mut controller =
                MultiClientController::new(config.clone(), SystemClientProcessExecutor);
            run_mounted_smoke_with_driver(
                context,
                &mut controller,
                SMOKE_CONVERGENCE_TIMEOUT,
                SMOKE_CONVERGENCE_POLL_INTERVAL,
            )?;
            return Ok(());
        }

        let workload = json!({
            "scenario": BASIC_MULTI_CLIENT_SMOKE,
            "run_id": context.run_id,
            "seed": context.seed,
            "workload": "placeholder",
        });
        fs::write(
            context.scenario_dir.join("workload.json"),
            serde_json::to_vec_pretty(&workload)?,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let peer_bootstrap_summary = context.scenario_dir.join("peer-bootstrap-summary.json");
        let bootstrap_summary = context.scenario_dir.join("bootstrap-summary.json");
        let smoke_summary = context.scenario_dir.join("smoke-summary.json");
        let validation = if peer_bootstrap_summary.exists() {
            let summary: PeerVolumeBootstrapSummary =
                serde_json::from_slice(&fs::read(&peer_bootstrap_summary)?)?;
            let mut validation = json!({
                "scenario": BASIC_MULTI_CLIENT_SMOKE,
                "run_id": context.run_id,
                "seed": context.seed,
                "status": "passed",
                "workflow": "mounted-peer-volume-discovery",
                "base_volume_name": summary.base_volume_name,
                "volumes": summary.volumes,
            });
            if smoke_summary.exists() {
                validation["volume_validations"] =
                    json!(read_mounted_smoke_volume_validations(&smoke_summary)?);
            }
            validation
        } else if bootstrap_summary.exists() {
            let summary: VolumeBootstrapSummary =
                serde_json::from_slice(&fs::read(&bootstrap_summary)?)?;
            let mut validation = json!({
                "scenario": BASIC_MULTI_CLIENT_SMOKE,
                "run_id": context.run_id,
                "seed": context.seed,
                "status": "passed",
                "workflow": "volume-bootstrap-discovery",
                "volume_name": summary.volume_name,
                "init_client": summary.init_client,
                "discovered_clients": summary.discovered_clients,
                "local_metadata_dbs": summary.clients
                    .iter()
                    .map(|client| client.local_metadata_db.display().to_string())
                    .collect::<Vec<_>>(),
            });
            if smoke_summary.exists() {
                validation["workflow"] = json!("mounted-multi-client-smoke");
                validation["volume_validations"] =
                    json!(read_mounted_smoke_volume_validations(&smoke_summary)?);
            }
            validation
        } else {
            json!({
                "scenario": BASIC_MULTI_CLIENT_SMOKE,
                "run_id": context.run_id,
                "seed": context.seed,
                "status": "passed",
                "notes": "placeholder validation for the initial runner crate",
            })
        };

        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        let mut artifact_paths = Vec::new();
        if peer_bootstrap_summary.exists() {
            artifact_paths.push(format!(
                "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/peer-bootstrap-summary.json"
            ));
        }
        if bootstrap_summary.exists() {
            artifact_paths.push(format!(
                "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/bootstrap-summary.json"
            ));
        }
        if smoke_summary.exists() {
            artifact_paths.push(format!(
                "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/smoke-summary.json"
            ));
        }
        artifact_paths.push(format!(
            "artifacts/scenarios/{BASIC_MULTI_CLIENT_SMOKE}/validation.json"
        ));
        Ok(artifact_paths)
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct PostgresOutageMultiClientRecovery;

impl Scenario for PostgresOutageMultiClientRecovery {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
            description: "mounted multi-client PostgreSQL outage recovery scenario",
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
        let compose_control = compose_control_from_manifest(context)?;
        let mut driver = RealPostgresOutageDriver {
            controller: MultiClientController::new(config.clone(), SystemClientProcessExecutor),
            compose_control,
            executor: SystemCommandExecutor,
        };
        run_postgres_outage_with_driver(
            context,
            &mut driver,
            OUTAGE_CONVERGENCE_TIMEOUT,
            OUTAGE_CONVERGENCE_POLL_INTERVAL,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("postgres-outage-recovery-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: PostgresOutageRecoverySummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged { "passed" } else { "failed" },
            "outage_duration_ms": summary.outage_duration_ms,
            "recovery_time_ms": summary.recovery_time_ms,
            "writer_recovery_client": summary.writer_recovery_client,
            "writer_recovery_time_ms": summary.writer_recovery_time_ms,
            "writer_recovery_checksum_sha256": summary.writer_recovery_checksum_sha256,
            "peer_observation_time_ms": summary.peer_observation_time_ms,
            "operations_accepted_during_outage": summary.operations_accepted_during_outage,
            "final_validator": summary.final_validator,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY}/outage-operations.ndjson"
            ),
            format!(
                "artifacts/scenarios/{POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY}/postgres-outage-recovery-summary.json"
            ),
            format!("artifacts/scenarios/{POSTGRES_OUTAGE_MULTI_CLIENT_RECOVERY}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct RustfsOutageMultiClientRecovery;

impl Scenario for RustfsOutageMultiClientRecovery {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
            description: "mounted multi-client RustFS/S3 outage recovery scenario",
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
        let compose_control = compose_control_from_manifest(context)?;
        let mut driver = RealRustfsOutageDriver {
            controller: MultiClientController::new(config.clone(), SystemClientProcessExecutor),
            compose_control,
            executor: SystemCommandExecutor,
        };
        run_rustfs_outage_with_driver(
            context,
            &mut driver,
            OUTAGE_CONVERGENCE_TIMEOUT,
            OUTAGE_CONVERGENCE_POLL_INTERVAL,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("rustfs-outage-recovery-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: RustfsOutageRecoverySummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.final_validator.converged { "passed" } else { "failed" },
            "outage_duration_ms": summary.outage_duration_ms,
            "upload_recovery_time_ms": summary.upload_recovery_time_ms,
            "outage_file": summary.outage_file,
            "local_read_checksum_sha256": summary.local_read_checksum_sha256,
            "peer_visible_during_outage": summary.peer_visible_during_outage,
            "final_checksum_sha256": summary.final_checksum_sha256,
            "final_validator": summary.final_validator,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY}/rustfs-outage-operations.ndjson"
            ),
            format!(
                "artifacts/scenarios/{RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY}/rustfs-outage-recovery-summary.json"
            ),
            format!("artifacts/scenarios/{RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct MountedClientCrashRestartRecovery;

impl Scenario for MountedClientCrashRestartRecovery {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
            description: "mounted multi-client crash/restart recovery scenario",
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
        let mut driver = RealCrashRestartDriver(MultiClientController::new(
            config.clone(),
            SystemClientProcessExecutor,
        ));
        run_crash_restart_with_driver(
            context,
            &mut driver,
            CRASH_RESTART_CONVERGENCE_TIMEOUT,
            CRASH_RESTART_CONVERGENCE_POLL_INTERVAL,
        )?;
        Ok(())
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context
            .scenario_dir
            .join("crash-restart-recovery-summary.json");
        if !summary_path.exists() {
            return Err(ScenarioError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing summary artifact: {}", summary_path.display()),
            )));
        }
        let summary: CrashRestartRecoverySummary =
            serde_json::from_slice(&fs::read(&summary_path)?)?;
        let validation = json!({
            "scenario": MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
            "run_id": context.run_id,
            "seed": context.seed,
            "status": if summary.modes.iter().all(|mode| mode.final_validator.converged) {
                "passed"
            } else {
                "failed"
            },
            "modes": summary.modes,
        });
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&validation)?,
        )?;
        Ok(vec![
            format!(
                "artifacts/scenarios/{MOUNTED_CLIENT_CRASH_RESTART_RECOVERY}/operations.ndjson"
            ),
            format!(
                "artifacts/scenarios/{MOUNTED_CLIENT_CRASH_RESTART_RECOVERY}/crash-restart-recovery-summary.json"
            ),
            format!("artifacts/scenarios/{MOUNTED_CLIENT_CRASH_RESTART_RECOVERY}/validation.json"),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

pub(in crate::scenario) fn compose_control_from_manifest(
    context: &ScenarioContext,
) -> Result<ComposeServiceControl, ScenarioError> {
    let manifest_path = context.sandbox.join("run.json");
    if !manifest_path.exists() {
        return Err(ScenarioError::MissingRunManifest {
            path: manifest_path,
        });
    }
    let manifest: RunManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    let Some(project) = manifest.compose_project.as_deref() else {
        return Err(ScenarioError::MissingComposeInfrastructure);
    };
    let mut env = vec![("STUDIOFS_E2E_RUN_ID".to_string(), manifest.run_id)];
    env.extend(
        manifest
            .allocated_ports
            .into_iter()
            .map(|port| (port.env_var, port.host_port.to_string())),
    );
    Ok(ComposeServiceControl::for_project_with_environment(
        project.to_string(),
        env,
    )?)
}

pub(in crate::scenario) fn write_bootstrap_summary(
    context: &ScenarioContext,
    summary: &VolumeBootstrapSummary,
) -> Result<(), ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    fs::write(
        context.scenario_dir.join("bootstrap-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

pub(in crate::scenario) fn write_peer_bootstrap_summary(
    context: &ScenarioContext,
    summary: &PeerVolumeBootstrapSummary,
) -> Result<(), ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    fs::write(
        context.scenario_dir.join("peer-bootstrap-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

pub(in crate::scenario) trait MountedSmokeDriver {
    fn bootstrap_peer_volumes(
        &mut self,
        options: PeerVolumeBootstrapOptions,
    ) -> Result<PeerVolumeBootstrapSummary, ClientControllerError>;
    fn select_volume_for_clients(&mut self, volume_name: &str)
    -> Result<(), ClientControllerError>;
    fn mount_all(&mut self) -> Result<(), ClientControllerError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn after_smoke_file_written(
        &mut self,
        _volume_name: &str,
        _smoke_file: &Path,
    ) -> Result<(), ClientControllerError> {
        Ok(())
    }
    fn unmount_all(&mut self) -> Result<(), ClientControllerError>;
}

impl<E: crate::client::ClientProcessExecutor> MountedSmokeDriver for MultiClientController<E> {
    fn bootstrap_peer_volumes(
        &mut self,
        options: PeerVolumeBootstrapOptions,
    ) -> Result<PeerVolumeBootstrapSummary, ClientControllerError> {
        MultiClientController::bootstrap_peer_volumes(self, options)
    }

    fn select_volume_for_clients(
        &mut self,
        volume_name: &str,
    ) -> Result<(), ClientControllerError> {
        MultiClientController::select_volume_for_clients(self, volume_name)
    }

    fn mount_all(&mut self) -> Result<(), ClientControllerError> {
        MultiClientController::mount_all(self)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        MultiClientController::clients(self)
            .into_iter()
            .cloned()
            .collect()
    }

    fn unmount_all(&mut self) -> Result<(), ClientControllerError> {
        MultiClientController::unmount_all(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct MountedSmokeRunSummary {
    pub(in crate::scenario) peer_bootstrap: PeerVolumeBootstrapSummary,
    pub(in crate::scenario) volume_validations: Vec<MountedSmokeVolumeValidation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct LegacyMountedSmokeRunSummary {
    pub(in crate::scenario) bootstrap: VolumeBootstrapSummary,
    pub(in crate::scenario) written_file: String,
    pub(in crate::scenario) convergence: TreeComparison,
    pub(in crate::scenario) attempts: usize,
    pub(in crate::scenario) elapsed_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct MountedSmokeVolumeValidation {
    pub(in crate::scenario) volume_name: String,
    pub(in crate::scenario) written_file: String,
    pub(in crate::scenario) convergence: TreeComparison,
    pub(in crate::scenario) attempts: usize,
    pub(in crate::scenario) elapsed_ms: u128,
}

pub(in crate::scenario) fn read_mounted_smoke_volume_validations(
    path: &Path,
) -> Result<Vec<MountedSmokeVolumeValidation>, ScenarioError> {
    let artifact: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    if artifact.get("volume_validations").is_some() {
        let summary: MountedSmokeRunSummary = serde_json::from_value(artifact)?;
        return Ok(summary.volume_validations);
    }
    let summary: LegacyMountedSmokeRunSummary = serde_json::from_value(artifact)?;
    Ok(vec![MountedSmokeVolumeValidation {
        volume_name: summary.bootstrap.volume_name,
        written_file: summary.written_file,
        convergence: summary.convergence,
        attempts: summary.attempts,
        elapsed_ms: summary.elapsed_ms,
    }])
}

pub(in crate::scenario) fn run_mounted_smoke_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn MountedSmokeDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedSmokeRunSummary, ScenarioError> {
    let result = run_mounted_smoke_body(context, driver, convergence_timeout, poll_interval);
    match result {
        Ok(summary) => Ok(summary),
        Err(primary) => match driver.unmount_all() {
            Ok(()) => Err(primary),
            Err(cleanup) => Err(ScenarioError::MountedSmokeUnmountAfterFailure {
                primary: primary.to_string(),
                cleanup: cleanup.to_string(),
            }),
        },
    }
}

pub(in crate::scenario) fn run_mounted_smoke_body(
    context: &ScenarioContext,
    driver: &mut dyn MountedSmokeDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<MountedSmokeRunSummary, ScenarioError> {
    let peer_bootstrap = driver.bootstrap_peer_volumes(PeerVolumeBootstrapOptions {
        client_names: SMOKE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_peer_bootstrap_summary(context, &peer_bootstrap)?;
    if peer_bootstrap.volumes.len() < SMOKE_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: SMOKE_CLIENT_NAMES.len(),
            actual: peer_bootstrap.volumes.len(),
        });
    }

    let mut volume_validations = Vec::with_capacity(peer_bootstrap.volumes.len());
    for volume in &peer_bootstrap.volumes {
        driver.select_volume_for_clients(&volume.volume_name)?;
        driver.mount_all()?;
        let clients = driver.clients();
        if clients.len() < SMOKE_CLIENT_NAMES.len() {
            return Err(ScenarioError::InsufficientMountedClients {
                expected: SMOKE_CLIENT_NAMES.len(),
                actual: clients.len(),
            });
        }

        let writer = clients
            .iter()
            .find(|client| client.name == volume.init_client)
            .expect("init_client for volume should always be in the list of clients");
        let smoke_file = writer
            .mount_point
            .join(format!("{}-{SMOKE_FILE_NAME}", volume.volume_name));
        bounded_mounted_write(
            smoke_file.clone(),
            SMOKE_FILE_PAYLOAD.to_vec(),
            format!("smoke write {}", smoke_file.display()),
            MOUNTED_WRITE_READBACK_TIMEOUT,
        )?;
        driver.after_smoke_file_written(&volume.volume_name, &smoke_file)?;
        let mounts = clients
            .iter()
            .take(SMOKE_CLIENT_NAMES.len())
            .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
            .collect::<Vec<_>>();
        let convergence = wait_for_convergence(
            &mounts,
            context
                .scenario_dir
                .join("validation")
                .join(&volume.volume_name),
            convergence_timeout,
            poll_interval,
        )?;
        volume_validations.push(mounted_smoke_volume_validation(
            volume.volume_name.clone(),
            smoke_file,
            convergence,
        ));
        driver.unmount_all()?;
    }

    let summary = MountedSmokeRunSummary {
        peer_bootstrap,
        volume_validations,
    };
    write_smoke_summary(context, &summary)?;
    Ok(summary)
}

pub(in crate::scenario) fn mounted_smoke_volume_validation(
    volume_name: String,
    smoke_file: PathBuf,
    convergence: ConvergenceOutcome,
) -> MountedSmokeVolumeValidation {
    MountedSmokeVolumeValidation {
        volume_name,
        written_file: smoke_file.display().to_string(),
        convergence: convergence.comparison,
        attempts: convergence.attempts,
        elapsed_ms: convergence.elapsed_ms,
    }
}

pub(in crate::scenario) fn write_smoke_summary(
    context: &ScenarioContext,
    summary: &MountedSmokeRunSummary,
) -> Result<(), ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    fs::write(
        context.scenario_dir.join("smoke-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

pub(in crate::scenario) trait PostgresOutageDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn stop_postgres(&mut self) -> Result<(), ScenarioError>;
    fn start_postgres(&mut self) -> Result<(), ScenarioError>;
    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn remount(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

pub(in crate::scenario) struct RealPostgresOutageDriver<E> {
    pub(in crate::scenario) controller: MultiClientController<SystemClientProcessExecutor>,
    pub(in crate::scenario) compose_control: ComposeServiceControl,
    pub(in crate::scenario) executor: E,
}

impl<E: CommandExecutor> PostgresOutageDriver for RealPostgresOutageDriver<E> {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.controller.bootstrap_volume(options)?)
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.mount_all()?)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.controller.clients().into_iter().cloned().collect()
    }

    fn stop_postgres(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .stop_service(POSTGRES_SERVICE_NAME, &self.executor)?)
    }

    fn start_postgres(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .start_service(POSTGRES_SERVICE_NAME, &self.executor)?)
    }

    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.controller.force_kill(client)?)
    }

    fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.controller.remount(client)?)
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.unmount_all()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct OutageOperationRecord {
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) path: String,
    pub(in crate::scenario) bytes: usize,
    pub(in crate::scenario) accepted: bool,
    pub(in crate::scenario) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct PostgresOutageRecoverySummary {
    pub(in crate::scenario) bootstrap: VolumeBootstrapSummary,
    pub(in crate::scenario) outage_file: String,
    pub(in crate::scenario) outage_duration_ms: u128,
    pub(in crate::scenario) operations_accepted_during_outage: usize,
    pub(in crate::scenario) writer_recovery_client: String,
    pub(in crate::scenario) writer_recovery_status: PostgresOutageWriterRecoveryStatus,
    pub(in crate::scenario) writer_recovery_time_ms: u128,
    pub(in crate::scenario) writer_recovery_bytes: usize,
    pub(in crate::scenario) expected_writer_recovery_checksum_sha256: String,
    pub(in crate::scenario) writer_recovery_checksum_sha256: String,
    pub(in crate::scenario) peer_observation_time_ms: u128,
    pub(in crate::scenario) recovery_time_ms: u128,
    pub(in crate::scenario) final_validator: TreeComparison,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::scenario) enum PostgresOutageWriterRecoveryStatus {
    Recovered,
    LostAfterUncleanShutdown,
}

pub(in crate::scenario) fn run_postgres_outage_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn PostgresOutageDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<PostgresOutageRecoverySummary, ScenarioError> {
    let result = run_postgres_outage_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::PostgresOutageUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

pub(in crate::scenario) fn run_postgres_outage_body(
    context: &ScenarioContext,
    driver: &mut dyn PostgresOutageDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<PostgresOutageRecoverySummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: POSTGRES_OUTAGE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;

    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < POSTGRES_OUTAGE_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientOutageClients {
            expected: POSTGRES_OUTAGE_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }

    let postgres_initial = clients[0].mount_point.join(OUTAGE_INITIAL_FILE_NAME);
    bounded_mounted_write(
        postgres_initial.clone(),
        b"initial healthy PostgreSQL write\n".to_vec(),
        format!(
            "postgres outage initial write {}",
            postgres_initial.display()
        ),
        MOUNTED_WRITE_READBACK_TIMEOUT,
    )?;

    let writer = &clients[0];
    let outage_started = Instant::now();
    driver.stop_postgres()?;
    let accepted_operations = perform_postgres_writer_outage_operation(
        context,
        writer,
        POSTGRES_OUTAGE_FILE_NAME,
        POSTGRES_OUTAGE_RECOVERED_PAYLOAD,
    )?;
    driver.force_kill(&writer.name)?;

    // `start_postgres` uses `docker compose up --wait`, so this includes
    // PostgreSQL healthcheck readiness before mounted convergence polling.
    let recovery_started = Instant::now();
    driver.start_postgres()?;
    let outage_duration_ms = elapsed_millis_nonzero(outage_started);
    let writer_recovery_started = Instant::now();
    driver.remount(&writer.name).map_err(|error| {
        ScenarioError::PostgresOutageWriterRemountFailed {
            client: writer.name.clone(),
            detail: error.to_string(),
        }
    })?;
    let expected_checksum = bytes_sha256(POSTGRES_OUTAGE_RECOVERED_PAYLOAD);
    let writer_payload_path = writer.mount_point.join(POSTGRES_OUTAGE_FILE_NAME);
    let writer_payload_result = bounded_mounted_read(
        writer_payload_path.clone(),
        format!(
            "postgres outage writer recovery read {}",
            writer_payload_path.display()
        ),
        MOUNTED_WRITE_READBACK_TIMEOUT,
    );
    let (writer_payload, writer_recovery_status) = match writer_payload_result {
        Ok(payload) if payload == POSTGRES_OUTAGE_RECOVERED_PAYLOAD => {
            (payload, PostgresOutageWriterRecoveryStatus::Recovered)
        }
        Ok(payload) if payload.is_empty() => (
            payload,
            PostgresOutageWriterRecoveryStatus::LostAfterUncleanShutdown,
        ),
        Ok(payload) => {
            let recovered_checksum = bytes_sha256(&payload);
            return Err(ScenarioError::PostgresOutageWriterRecoveryFailed {
                client: writer.name.clone(),
                path: POSTGRES_OUTAGE_FILE_NAME.to_string(),
                detail: format!(
                    "recovered payload differs: expected {} bytes with expected checksum {}, got {} bytes with recovered checksum {}",
                    POSTGRES_OUTAGE_RECOVERED_PAYLOAD.len(),
                    expected_checksum,
                    payload.len(),
                    recovered_checksum
                ),
            });
        }
        Err(ScenarioError::Io(error)) if error.kind() == io::ErrorKind::NotFound => (
            Vec::new(),
            PostgresOutageWriterRecoveryStatus::LostAfterUncleanShutdown,
        ),
        Err(error) => {
            return Err(ScenarioError::PostgresOutageWriterRecoveryFailed {
                client: writer.name.clone(),
                path: POSTGRES_OUTAGE_FILE_NAME.to_string(),
                detail: error.to_string(),
            });
        }
    };
    let recovered_checksum = bytes_sha256(&writer_payload);
    let writer_recovery_time_ms = elapsed_millis_nonzero(writer_recovery_started);
    let convergence_clients = &clients[..POSTGRES_OUTAGE_CLIENT_NAMES.len()];
    let mounts = convergence_clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let peer_observation_started = Instant::now();
    let convergence = wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )
    .map_err(|error| ScenarioError::PostgresOutagePeerConvergenceFailed {
        detail: error.to_string(),
    })?;
    let peer_observation_time_ms = elapsed_millis_nonzero(peer_observation_started);
    let final_validator = convergence.comparison;
    let recovery_time_ms = elapsed_millis_nonzero(recovery_started);

    let summary = PostgresOutageRecoverySummary {
        bootstrap,
        outage_file: POSTGRES_OUTAGE_FILE_NAME.to_string(),
        outage_duration_ms,
        operations_accepted_during_outage: accepted_operations,
        writer_recovery_client: writer.name.clone(),
        writer_recovery_status,
        writer_recovery_time_ms,
        writer_recovery_bytes: writer_payload.len(),
        expected_writer_recovery_checksum_sha256: expected_checksum,
        writer_recovery_checksum_sha256: recovered_checksum,
        peer_observation_time_ms,
        recovery_time_ms,
        final_validator,
    };
    fs::write(
        context
            .scenario_dir
            .join("postgres-outage-recovery-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

pub(in crate::scenario) fn perform_postgres_writer_outage_operation(
    context: &ScenarioContext,
    client: &ClientSpec,
    relative: &str,
    payload: &[u8],
) -> Result<usize, ScenarioError> {
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("outage-operations.ndjson"))?;
    let mut operations_log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    let mounted_path = client.mount_point.join(relative);
    let write_result = write_mounted_file_without_barrier_with_readback(
        mounted_path.clone(),
        payload,
        "postgres outage writer write",
    );
    let error = write_result.err().map(|err| err.to_string());
    let operation_accepted = error.is_none();
    let record = OutageOperationRecord {
        client: client.name.clone(),
        path: relative.to_string(),
        bytes: payload.len(),
        accepted: operation_accepted,
        error,
    };
    serde_json::to_writer(&mut log, &record)?;
    log.write_all(b"\n")?;
    serde_json::to_writer(&mut operations_log, &record)?;
    operations_log.write_all(b"\n")?;
    if operation_accepted {
        Ok(1)
    } else {
        Err(ScenarioError::RejectedOutageOperations {
            rejected: 1,
            total: 1,
        })
    }
}

pub(in crate::scenario) trait RustfsOutageDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    // Test drivers use this hook to make the healthy pre-outage payload visible
    // before the normal mounted-tree convergence gate.
    fn after_initial_write(
        &mut self,
        clients: &[ClientSpec],
        relative_path: &str,
    ) -> Result<(), ScenarioError>;
    fn stop_rustfs(&mut self) -> Result<(), ScenarioError>;
    fn assert_peer_not_visible(
        &mut self,
        peer: &ClientSpec,
        relative_path: &str,
        forbidden_checksum: &str,
    ) -> Result<Option<String>, ScenarioError>;
    fn start_rustfs(&mut self) -> Result<(), ScenarioError>;
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

pub(in crate::scenario) struct RealRustfsOutageDriver<E> {
    pub(in crate::scenario) controller: MultiClientController<SystemClientProcessExecutor>,
    pub(in crate::scenario) compose_control: ComposeServiceControl,
    pub(in crate::scenario) executor: E,
}

impl<E: CommandExecutor> RustfsOutageDriver for RealRustfsOutageDriver<E> {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.controller.bootstrap_volume(options)?)
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.mount_all()?)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.controller.clients().into_iter().cloned().collect()
    }

    fn after_initial_write(
        &mut self,
        _clients: &[ClientSpec],
        _relative_path: &str,
    ) -> Result<(), ScenarioError> {
        Ok(())
    }

    fn stop_rustfs(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .stop_service(RUSTFS_SERVICE_NAME, &self.executor)?)
    }

    fn assert_peer_not_visible(
        &mut self,
        peer: &ClientSpec,
        relative_path: &str,
        forbidden_checksum: &str,
    ) -> Result<Option<String>, ScenarioError> {
        assert_peer_not_visible(peer, relative_path, forbidden_checksum)
    }

    fn start_rustfs(&mut self) -> Result<(), ScenarioError> {
        Ok(self
            .compose_control
            .start_service(RUSTFS_SERVICE_NAME, &self.executor)?)
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.controller.unmount_all()?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct RustfsPeerVisibility {
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) visible_checksum_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct ClientPayloadChecksum {
    pub(in crate::scenario) client: String,
    pub(in crate::scenario) checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct RustfsOutageRecoverySummary {
    pub(in crate::scenario) bootstrap: VolumeBootstrapSummary,
    pub(in crate::scenario) outage_file: String,
    pub(in crate::scenario) outage_duration_ms: u128,
    pub(in crate::scenario) upload_recovery_time_ms: u128,
    pub(in crate::scenario) local_read_checksum_sha256: String,
    pub(in crate::scenario) peer_visible_during_outage: Option<String>,
    pub(in crate::scenario) peer_visibility_during_outage: Vec<RustfsPeerVisibility>,
    pub(in crate::scenario) final_checksum_sha256: String,
    pub(in crate::scenario) final_client_checksums: Vec<ClientPayloadChecksum>,
    pub(in crate::scenario) final_validator: TreeComparison,
}

pub(in crate::scenario) fn run_rustfs_outage_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn RustfsOutageDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<RustfsOutageRecoverySummary, ScenarioError> {
    let result = run_rustfs_outage_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::RustfsOutageUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

pub(in crate::scenario) fn run_rustfs_outage_body(
    context: &ScenarioContext,
    driver: &mut dyn RustfsOutageDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<RustfsOutageRecoverySummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: RUSTFS_OUTAGE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;

    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < RUSTFS_OUTAGE_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientOutageClients {
            expected: RUSTFS_OUTAGE_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }

    let relative_path = RUSTFS_OUTAGE_FILE_NAME;
    let source_file = clients[0].mount_point.join(relative_path);
    write_mounted_initial_file(
        source_file.clone(),
        RUSTFS_OUTAGE_INITIAL_PAYLOAD,
        "rustfs outage initial payload",
    )?;
    driver.after_initial_write(&clients, relative_path)?;
    let initial_mounts = clients
        .iter()
        .take(RUSTFS_OUTAGE_CLIENT_NAMES.len())
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    wait_for_convergence(
        &initial_mounts,
        context.scenario_dir.join("healthy-validation"),
        convergence_timeout,
        poll_interval,
    )?;

    let outage_started = Instant::now();
    driver.stop_rustfs()?;
    bounded_mounted_write(
        source_file.clone(),
        RUSTFS_OUTAGE_RECOVERED_PAYLOAD.to_vec(),
        format!("rustfs outage recovered write {}", source_file.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
    )?;
    let local_read = wait_for_mounted_payload_readback(
        &source_file,
        RUSTFS_OUTAGE_RECOVERED_PAYLOAD,
        MOUNTED_WRITE_READBACK_TIMEOUT,
        MOUNTED_WRITE_READBACK_POLL_INTERVAL,
    )
    .map_err(scenario_error_to_io)?;
    let local_read_checksum_sha256 = bytes_sha256(&local_read);
    let outage_duration_ms = outage_started.elapsed().as_millis();
    let mut peer_visibility_during_outage = Vec::new();
    for peer in clients
        .iter()
        .skip(1)
        .take(RUSTFS_OUTAGE_CLIENT_NAMES.len() - 1)
    {
        let visible_checksum_sha256 =
            driver.assert_peer_not_visible(peer, relative_path, &local_read_checksum_sha256)?;
        peer_visibility_during_outage.push(RustfsPeerVisibility {
            client: peer.name.clone(),
            visible_checksum_sha256,
        });
    }
    let peer_visible_during_outage = peer_visibility_during_outage
        .first()
        .and_then(|probe| probe.visible_checksum_sha256.clone());

    let recovery_started = Instant::now();
    driver.start_rustfs()?;
    let convergence = wait_for_convergence(
        &initial_mounts,
        context.scenario_dir.join("validation"),
        convergence_timeout,
        poll_interval,
    )?;
    wait_for_mounted_initial_payloads_until(
        &clients[..RUSTFS_OUTAGE_CLIENT_NAMES.len()],
        &[(relative_path, RUSTFS_OUTAGE_RECOVERED_PAYLOAD)],
        Instant::now() + convergence_timeout,
        poll_interval,
        "rustfs outage recovered payload",
    )?;
    let upload_recovery_time_ms = recovery_started.elapsed().as_millis();
    let mut final_client_checksums = Vec::new();
    for client in clients.iter().take(RUSTFS_OUTAGE_CLIENT_NAMES.len()) {
        let path = client.mount_point.join(relative_path);
        let checksum = bytes_sha256(&bounded_mounted_read(
            path.clone(),
            format!("rustfs outage final read {}", path.display()),
            MOUNTED_WRITE_READBACK_TIMEOUT,
        )?);
        if checksum != local_read_checksum_sha256 {
            return Err(ScenarioError::Io(std::io::Error::other(format!(
                "recovery convergence failure: {} read checksum {} but expected {} for {relative_path}",
                client.name, checksum, local_read_checksum_sha256
            ))));
        }
        final_client_checksums.push(ClientPayloadChecksum {
            client: client.name.clone(),
            checksum_sha256: checksum,
        });
    }
    let final_checksum_sha256 = final_client_checksums
        .first()
        .map(|entry| entry.checksum_sha256.clone())
        .unwrap_or_else(|| local_read_checksum_sha256.clone());

    let summary = RustfsOutageRecoverySummary {
        bootstrap,
        outage_file: relative_path.to_string(),
        outage_duration_ms,
        upload_recovery_time_ms,
        local_read_checksum_sha256,
        peer_visible_during_outage,
        peer_visibility_during_outage,
        final_checksum_sha256,
        final_client_checksums,
        final_validator: convergence.comparison,
    };
    write_rustfs_outage_operation(context, &summary)?;
    fs::write(
        context
            .scenario_dir
            .join("rustfs-outage-recovery-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

pub(in crate::scenario) fn assert_peer_not_visible(
    peer: &ClientSpec,
    relative_path: &str,
    forbidden_checksum: &str,
) -> Result<Option<String>, ScenarioError> {
    let Some(checksum) = read_peer_checksum(peer, relative_path)? else {
        return Ok(None);
    };
    if checksum == forbidden_checksum {
        Err(ScenarioError::PeerObservedOutageWrite { checksum })
    } else {
        Ok(Some(checksum))
    }
}

pub(in crate::scenario) fn read_peer_checksum(
    peer: &ClientSpec,
    relative_path: &str,
) -> Result<Option<String>, ScenarioError> {
    let path = peer.mount_point.join(relative_path);
    match bounded_mounted_read(
        path.clone(),
        format!("peer checksum read {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
    ) {
        Ok(payload) => Ok(Some(bytes_sha256(&payload))),
        Err(ScenarioError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

pub(in crate::scenario) fn write_rustfs_outage_operation(
    context: &ScenarioContext,
    summary: &RustfsOutageRecoverySummary,
) -> Result<(), ScenarioError> {
    let record = json!({
        "scenario": RUSTFS_OUTAGE_MULTI_CLIENT_RECOVERY,
        "client": RUSTFS_OUTAGE_CLIENT_NAMES[0],
        "path": summary.outage_file,
        "bytes": RUSTFS_OUTAGE_RECOVERED_PAYLOAD.len(),
        "accepted": true,
        "local_read_checksum_sha256": summary.local_read_checksum_sha256,
        "peer_visible_during_outage": summary.peer_visible_during_outage,
        "peer_visibility_during_outage": summary.peer_visibility_during_outage,
        "final_checksum_sha256": summary.final_checksum_sha256,
        "final_client_checksums": summary.final_client_checksums,
    });
    for artifact in ["rustfs-outage-operations.ndjson", "operations.ndjson"] {
        let mut log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(context.scenario_dir.join(artifact))?;
        serde_json::to_writer(&mut log, &record)?;
        log.write_all(b"\n")?;
    }
    Ok(())
}

pub(in crate::scenario) trait CrashRestartDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError>;
    fn mount_all(&mut self) -> Result<(), ScenarioError>;
    fn clients(&self) -> Vec<ClientSpec>;
    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn remount(&mut self, client: &str) -> Result<(), ScenarioError>;
    fn before_peer_convergence_wait(
        &mut self,
        _client: &str,
        _relative_path: &str,
    ) -> Result<(), ScenarioError> {
        Ok(())
    }
    fn unmount_all(&mut self) -> Result<(), ScenarioError>;
}

pub(in crate::scenario) struct RealCrashRestartDriver(
    MultiClientController<SystemClientProcessExecutor>,
);

impl CrashRestartDriver for RealCrashRestartDriver {
    fn bootstrap_volume(
        &mut self,
        options: VolumeBootstrapOptions,
    ) -> Result<VolumeBootstrapSummary, ScenarioError> {
        Ok(self.0.bootstrap_volume(options)?)
    }

    fn mount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.mount_all()?)
    }

    fn clients(&self) -> Vec<ClientSpec> {
        self.0.clients().into_iter().cloned().collect()
    }

    fn force_kill(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.0.force_kill(client)?)
    }

    fn remount(&mut self, client: &str) -> Result<(), ScenarioError> {
        Ok(self.0.remount(client)?)
    }

    fn unmount_all(&mut self) -> Result<(), ScenarioError> {
        Ok(self.0.unmount_all()?)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::scenario) enum CrashRestartMode {
    AfterLocalDurabilityBoundary,
    BeforeRemotePublicationCompletes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct CrashRestartModeSummary {
    pub(in crate::scenario) mode: CrashRestartMode,
    pub(in crate::scenario) restarted_client: String,
    pub(in crate::scenario) restart_client_root: String,
    pub(in crate::scenario) relative_path: String,
    pub(in crate::scenario) bytes: usize,
    pub(in crate::scenario) local_read_checksum_sha256: Option<String>,
    pub(in crate::scenario) peer_visible_before_convergence: Option<String>,
    pub(in crate::scenario) final_checksum_sha256: Option<String>,
    pub(in crate::scenario) restart_elapsed_ms: u128,
    pub(in crate::scenario) convergence_elapsed_ms: u128,
    pub(in crate::scenario) intended_checksum_sha256: String,
    pub(in crate::scenario) recovery_outcome: CrashRestartRecoveryOutcome,
    pub(in crate::scenario) recovery_detail: Option<String>,
    pub(in crate::scenario) final_validator: TreeComparison,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::scenario) enum CrashRestartRecoveryOutcome {
    RecoverableWriterState,
    UnrecoverableLoss,
}

pub(in crate::scenario) enum CrashRestartRecoveredPayload {
    Recovered(Vec<u8>),
    UnrecoverableLoss { detail: String },
}

pub(in crate::scenario) enum CrashRestartOutcomeDraft {
    Recovered {
        local_read_payload: Vec<u8>,
        local_read_checksum_sha256: String,
        peer_visible_before_convergence: Option<String>,
    },
    UnrecoverableLoss {
        detail: String,
        peer_visible_before_convergence: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::scenario) struct CrashRestartRecoverySummary {
    pub(in crate::scenario) bootstrap: VolumeBootstrapSummary,
    pub(in crate::scenario) modes: Vec<CrashRestartModeSummary>,
}

pub(in crate::scenario) fn run_crash_restart_with_driver(
    context: &ScenarioContext,
    driver: &mut dyn CrashRestartDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<CrashRestartRecoverySummary, ScenarioError> {
    let result = run_crash_restart_body(context, driver, convergence_timeout, poll_interval);
    let unmount_result = driver.unmount_all();
    match (result, unmount_result) {
        (Ok(summary), Ok(())) => Ok(summary),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::CrashRestartUnmountAfterFailure {
            primary: primary.to_string(),
            cleanup: cleanup.to_string(),
        }),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

pub(in crate::scenario) fn run_crash_restart_body(
    context: &ScenarioContext,
    driver: &mut dyn CrashRestartDriver,
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<CrashRestartRecoverySummary, ScenarioError> {
    let bootstrap = driver.bootstrap_volume(VolumeBootstrapOptions {
        client_names: CRASH_RESTART_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    write_bootstrap_summary(context, &bootstrap)?;

    driver.mount_all()?;
    let clients = driver.clients();
    if clients.len() < CRASH_RESTART_CLIENT_NAMES.len() {
        return Err(ScenarioError::InsufficientCrashRestartClients {
            expected: CRASH_RESTART_CLIENT_NAMES.len(),
            actual: clients.len(),
        });
    }

    let modes = vec![
        run_crash_restart_mode(
            context,
            driver,
            &clients,
            CrashRestartMode::AfterLocalDurabilityBoundary,
            CRASH_RESTART_LOCAL_DURABLE_FILE_NAME,
            b"accepted local durable payload before abrupt crash\n",
            convergence_timeout,
            poll_interval,
        )?,
        run_crash_restart_mode(
            context,
            driver,
            &clients,
            CrashRestartMode::BeforeRemotePublicationCompletes,
            CRASH_RESTART_BEFORE_PUBLICATION_FILE_NAME,
            b"accepted local payload killed before remote publication completes\n",
            convergence_timeout,
            poll_interval,
        )?,
    ];

    let summary = CrashRestartRecoverySummary { bootstrap, modes };
    fs::write(
        context
            .scenario_dir
            .join("crash-restart-recovery-summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
pub(in crate::scenario) fn run_crash_restart_mode(
    context: &ScenarioContext,
    driver: &mut dyn CrashRestartDriver,
    clients: &[ClientSpec],
    mode: CrashRestartMode,
    relative_path: &str,
    payload: &[u8],
    convergence_timeout: Duration,
    poll_interval: Duration,
) -> Result<CrashRestartModeSummary, ScenarioError> {
    let crashed = &clients[0];
    write_local_durable_payload(crashed.mount_point.join(relative_path), payload)?;
    let restart_started = Instant::now();
    driver.force_kill(&crashed.name)?;
    driver.remount(&crashed.name)?;
    let recovered_payload = read_crash_restart_recovered_payload(crashed, relative_path)?;
    let intended_checksum_sha256 = bytes_sha256(payload);
    let restart_elapsed_ms = restart_started.elapsed().as_millis();

    let convergence_started = Instant::now();
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();
    let validation_dir = context
        .scenario_dir
        .join(format!("validation-{}", crash_restart_mode_slug(mode)));

    let outcome = match recovered_payload {
        CrashRestartRecoveredPayload::Recovered(local_read_payload) => {
            let local_read_checksum_sha256 = bytes_sha256(&local_read_payload);
            let peer_visible_before_convergence =
                if mode == CrashRestartMode::BeforeRemotePublicationCompletes {
                    read_peer_checksum(&clients[1], relative_path)?
                        .filter(|checksum| checksum == &local_read_checksum_sha256)
                } else {
                    None
                };

            CrashRestartOutcomeDraft::Recovered {
                local_read_payload,
                local_read_checksum_sha256,
                peer_visible_before_convergence,
            }
        }
        CrashRestartRecoveredPayload::UnrecoverableLoss { detail } => {
            let peer_visible_before_convergence =
                if mode == CrashRestartMode::BeforeRemotePublicationCompletes {
                    read_peer_checksum(&clients[1], relative_path)?
                } else {
                    None
                };
            CrashRestartOutcomeDraft::UnrecoverableLoss {
                detail,
                peer_visible_before_convergence,
            }
        }
    };

    driver.before_peer_convergence_wait(&crashed.name, relative_path)?;

    let convergence =
        wait_for_convergence(&mounts, validation_dir, convergence_timeout, poll_interval)
            .map_err(|error| crash_restart_convergence_error(relative_path, error))?;
    let convergence_elapsed_ms = convergence_started.elapsed().as_millis();

    let summary = match outcome {
        CrashRestartOutcomeDraft::Recovered {
            local_read_payload,
            local_read_checksum_sha256,
            peer_visible_before_convergence,
        } => {
            let final_path = clients[1].mount_point.join(relative_path);
            let final_payload = bounded_mounted_read(
                final_path.clone(),
                format!("crash restart final read {}", final_path.display()),
                MOUNTED_WRITE_READBACK_TIMEOUT,
            )?;
            let final_checksum_sha256 = bytes_sha256(&final_payload);
            if final_checksum_sha256 != local_read_checksum_sha256 {
                return Err(ScenarioError::CrashRestartCorruptAdvertisedFile {
                    client: clients[1].name.clone(),
                    path: relative_path.to_string(),
                    expected_checksum: local_read_checksum_sha256,
                    actual_checksum: final_checksum_sha256,
                });
            }

            CrashRestartModeSummary {
                mode,
                restarted_client: crashed.name.clone(),
                restart_client_root: crashed.client_root.display().to_string(),
                relative_path: relative_path.to_string(),
                bytes: local_read_payload.len(),
                local_read_checksum_sha256: Some(local_read_checksum_sha256),
                peer_visible_before_convergence,
                final_checksum_sha256: Some(final_checksum_sha256),
                restart_elapsed_ms,
                convergence_elapsed_ms,
                intended_checksum_sha256,
                recovery_outcome: CrashRestartRecoveryOutcome::RecoverableWriterState,
                recovery_detail: None,
                final_validator: convergence.comparison,
            }
        }
        CrashRestartOutcomeDraft::UnrecoverableLoss {
            detail,
            peer_visible_before_convergence,
        } => {
            if mode != CrashRestartMode::BeforeRemotePublicationCompletes {
                if let Some(final_checksum_sha256) = read_peer_checksum(&clients[1], relative_path)?
                {
                    return Err(ScenarioError::CrashRestartCorruptAdvertisedFile {
                        client: clients[1].name.clone(),
                        path: relative_path.to_string(),
                        expected_checksum: "unrecoverable_loss".to_string(),
                        actual_checksum: final_checksum_sha256,
                    });
                }
            }

            CrashRestartModeSummary {
                mode,
                restarted_client: crashed.name.clone(),
                restart_client_root: crashed.client_root.display().to_string(),
                relative_path: relative_path.to_string(),
                bytes: 0,
                local_read_checksum_sha256: None,
                peer_visible_before_convergence,
                final_checksum_sha256: None,
                restart_elapsed_ms,
                convergence_elapsed_ms,
                intended_checksum_sha256,
                recovery_outcome: CrashRestartRecoveryOutcome::UnrecoverableLoss,
                recovery_detail: Some(detail),
                final_validator: convergence.comparison,
            }
        }
    };
    write_crash_restart_operation(context, &summary)?;
    Ok(summary)
}

pub(in crate::scenario) fn read_crash_restart_recovered_payload(
    client: &ClientSpec,
    relative_path: &str,
) -> Result<CrashRestartRecoveredPayload, ScenarioError> {
    let path = client.mount_point.join(relative_path);
    match bounded_mounted_read(
        path.clone(),
        format!("crash restart recovered read {}", path.display()),
        MOUNTED_WRITE_READBACK_TIMEOUT,
    ) {
        Ok(payload) => Ok(CrashRestartRecoveredPayload::Recovered(payload)),
        Err(ScenarioError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(CrashRestartRecoveredPayload::UnrecoverableLoss {
                detail: format!(
                    "{} could not read recovered bytes for {relative_path} after restart",
                    client.name
                ),
            })
        }
        Err(error) => Err(error),
    }
}

pub(in crate::scenario) fn crash_restart_convergence_error(
    relative_path: &str,
    error: MountedTreeError,
) -> ScenarioError {
    match error {
        MountedTreeError::ConvergenceTimedOut { .. } => {
            ScenarioError::CrashRestartPeerConvergenceFailed {
                path: relative_path.to_string(),
                detail: error.to_string(),
            }
        }
        error => ScenarioError::CrashRestartPublicationFailed {
            path: relative_path.to_string(),
            detail: error.to_string(),
        },
    }
}

pub(in crate::scenario) fn write_crash_restart_operation(
    context: &ScenarioContext,
    summary: &CrashRestartModeSummary,
) -> Result<(), ScenarioError> {
    let record = json!({
        "scenario": MOUNTED_CLIENT_CRASH_RESTART_RECOVERY,
        "mode": summary.mode,
        "client": summary.restarted_client,
        "path": summary.relative_path,
        "bytes": summary.bytes,
        "accepted": true,
        "recovery_outcome": summary.recovery_outcome,
        "recovery_detail": summary.recovery_detail,
        "intended_checksum_sha256": summary.intended_checksum_sha256,
        "local_read_checksum_sha256": summary.local_read_checksum_sha256,
        "peer_visible_before_convergence": summary.peer_visible_before_convergence,
        "final_checksum_sha256": summary.final_checksum_sha256,
    });
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(context.scenario_dir.join("operations.ndjson"))?;
    serde_json::to_writer(&mut log, &record)?;
    log.write_all(b"\n")?;
    Ok(())
}

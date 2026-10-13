//! Two clients `mkdir -p` one directory while they cannot see each other.

use super::*;

const SUMMARY_FILE: &str = "concurrent-mkdir-merge-summary.json";
/// Clients that create the shared directory during the sync window.
const CREATORS: [usize; 2] = [0, 1];

pub(in crate::scenario) struct ConcurrentMkdirMerge;

impl Scenario for ConcurrentMkdirMerge {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: "mounted-concurrent-mkdir-merge-convergence",
            description: "two clients create one directory during a PostgreSQL outage and keep both files",
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
        let mut controller =
            MultiClientController::new(config.clone(), SystemClientProcessExecutor);
        let result = run_concurrent_mkdir(context, &mut controller, &compose_control);
        // A failure inside the outage must not leave PostgreSQL stopped for later scenarios.
        let restart = compose_control
            .start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)
            .map_err(ScenarioError::from);
        let cleanup = restart.and(controller.unmount_all().map_err(ScenarioError::from));
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: "concurrent mkdir merge".into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY_FILE))?)?;
        if summary["all_files_on_all_clients"].as_bool() != Some(true) {
            return Err(io::Error::other(format!(
                "concurrent mkdir merge lost a client's file: {summary}"
            ))
            .into());
        }
        Ok(Vec::new())
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn run_concurrent_mkdir(
    context: &ScenarioContext,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
    compose_control: &ComposeServiceControl,
) -> Result<(), ScenarioError> {
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: POSTGRES_OUTAGE_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    controller.mount_all()?;
    let clients: Vec<ClientSpec> = controller.clients().into_iter().cloned().collect();
    let mounts = clients
        .iter()
        .map(|client| ClientMount::new(client.name.clone(), client.mount_point.clone()))
        .collect::<Vec<_>>();

    let shared_dir = format!("merged-{}", context.run_id);
    let files = CREATORS
        .iter()
        .map(|&index| {
            let client = &clients[index];
            let path = format!("{shared_dir}/from-{}.txt", client.name);
            let payload = format!("written by {} during the sync window\n", client.name);
            (client, path, payload.into_bytes())
        })
        .collect::<Vec<_>>();

    // With PostgreSQL down neither creator can see the other's directory.
    compose_control.stop_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
    for (client, path, payload) in &files {
        write_mounted_file_with_readback(
            client.mount_point.join(path),
            payload,
            &format!("concurrent mkdir {} write", client.name),
            true,
        )?;
    }
    compose_control.start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;

    wait_for_convergence(
        &mounts,
        context.scenario_dir.join("validation"),
        OUTAGE_CONVERGENCE_TIMEOUT,
        OUTAGE_CONVERGENCE_POLL_INTERVAL,
    )?;
    let mut observed = Vec::new();
    for client in &clients {
        for (writer, path, payload) in &files {
            let actual = bounded_mounted_read(
                client.mount_point.join(path),
                format!("concurrent mkdir {} reads {}", client.name, writer.name),
                MOUNTED_WRITE_READBACK_TIMEOUT,
            )
            .ok();
            observed.push(json!({
                "client": client.name,
                "path": path,
                "matched": actual.as_deref() == Some(payload.as_slice()),
            }));
        }
    }
    let all_files_on_all_clients = observed
        .iter()
        .all(|entry| entry["matched"].as_bool() == Some(true));
    fs::write(
        context.scenario_dir.join(SUMMARY_FILE),
        serde_json::to_vec_pretty(&json!({
            "shared_dir": shared_dir,
            "observed": observed,
            "all_files_on_all_clients": all_files_on_all_clients,
        }))?,
    )?;
    Ok(())
}

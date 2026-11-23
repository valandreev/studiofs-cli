//! PostgreSQL outage and restart around a standalone janitor CLI deletion.

use super::*;

pub(in crate::scenario) struct JanitorPostgresRestart;

const NAME: &str = "janitor-postgres-restart-recovery";
const CLI_TIMEOUT: Duration = Duration::from_secs(90);
/// The janitor CLI's documented exit code for an unavailable metadata or object backend.
const BACKEND_FAILURE_EXIT: i32 = 5;

impl Scenario for JanitorPostgresRestart {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: NAME,
            description: "Standalone janitor CLI reports backend failure while PostgreSQL is stopped, keeps the payload and leaves its candidate pending and unowned, then completes the deletion after PostgreSQL restarts",
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
        let mut postgres_stopped = false;
        let result = probe(context, config.clone(), &compose, &mut postgres_stopped);
        // Leave PostgreSQL running for later scenarios even when the probe failed mid-outage.
        let restart = postgres_stopped
            .then(|| compose.start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor))
            .transpose();
        match (result, restart) {
            (Ok(()), Ok(_)) => Ok(()),
            (Err(error), Ok(_)) => Err(scenario_error(&config, &error)),
            (result, Err(restart)) => Err(io::Error::other(format!(
                "{}; cleanup: restart {POSTGRES_SERVICE_NAME}: {restart}",
                result.err().map_or_else(
                    || "probe passed".into(),
                    |error| redact_diagnostics(&config, &format!("{error:#}"))
                ),
            ))
            .into()),
        }
    }

    fn validate(&self, _context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        Ok(Vec::new())
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn probe(
    context: &ScenarioContext,
    config: ClientControllerConfig,
    compose: &ComposeServiceControl,
    postgres_stopped: &mut bool,
) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let volume = JanitorVolume::bootstrap(config)?;
    let writer = s3_writer(&volume.config)?;
    let id = object_id(0)?;
    let keys = object_keys(&volume.prefix, id);
    runtime.block_on(async {
        for key in &keys {
            writer.write(key, FIXTURE_BYTES.to_vec()).await?;
        }
        wait_until_s3_keys_are_past(&writer, &keys).await
    })?;
    volume.queue_candidate(id)?;
    let dir = &context.scenario_dir;

    *postgres_stopped = true;
    compose.stop_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
    let unavailable = volume.run(dir, "janitor-postgres-down", &volume.args(10), CLI_TIMEOUT)?;
    anyhow::ensure!(
        unavailable.status.code() == Some(BACKEND_FAILURE_EXIT),
        "PostgreSQL outage must return backend failure, got {:?}",
        unavailable.status.code()
    );
    runtime.block_on(async {
        for key in &keys {
            anyhow::ensure!(
                writer.read(key).await?.to_vec() == FIXTURE_BYTES,
                "payload {key} changed during the PostgreSQL outage"
            );
        }
        Ok(())
    })?;
    // `start_service` waits for the Compose health check, so PostgreSQL accepts connections.
    compose.start_service(POSTGRES_SERVICE_NAME, &SystemCommandExecutor)?;
    *postgres_stopped = false;

    anyhow::ensure!(
        janitor_ownership(&volume, id)? == (false, 0),
        "failed runs must leave the candidate pending and unowned"
    );
    let recovered = volume.run(dir, "janitor-recovered", &volume.args(10), CLI_TIMEOUT)?;
    anyhow::ensure!(
        recovered.status.success(),
        "janitor did not resume after PostgreSQL restart: {:?}",
        recovered.status.code()
    );
    runtime.block_on(async {
        for key in &keys {
            anyhow::ensure!(
                !writer.exists(key).await?,
                "payload {key} survived the recovered run"
            );
        }
        Ok(())
    })?;
    anyhow::ensure!(
        janitor_ownership(&volume, id)? == (true, 0),
        "recovered run must acknowledge the deletion and release its claim"
    );
    Ok(())
}

/// Whether the object's queue entry is acknowledged, and how many claim owners remain.
fn janitor_ownership(volume: &JanitorVolume, object_id: u64) -> anyhow::Result<(bool, i64)> {
    let (volume_id, object_id) = (volume.volume_id, i64::try_from(object_id)?);
    Ok(with_pg(
        &volume.config,
        "janitor ownership",
        |client| async move {
            let acknowledged = client
            .query_one(
                "SELECT acknowledged_at IS NOT NULL FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = $2",
                &[&volume_id, &object_id],
            )
            .await
            .map_err(io::Error::other)?
            .get(0);
            let owners = client
            .query_one(
                "SELECT count(*) FROM snapshot_payload_upload_objects WHERE vol = $1 AND reconcile_owner IS NOT NULL",
                &[&volume_id],
            )
            .await
            .map_err(io::Error::other)?
            .get(0);
            Ok((acknowledged, owners))
        },
    )?)
}

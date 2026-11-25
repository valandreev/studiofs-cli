//! Opt-in capture of the persisted bytes a published release writes at `init` (SFS-1002).
//!
//! Run with `--studiofs-bin <published studiofs>` and `--infra compose`. The
//! scenario formats one volume with that binary against the runner's Compose
//! PostgreSQL and RustFS, then writes the raw persisted bytes to
//! `<scenario_dir>/published-volume/`. It never writes into the source tree;
//! `tools/fixtures/capture-published-volume-fixtures.ps1` copies the artifacts
//! into `crates/studiofs-meta/tests/fixtures/published-<tag>/`.
//!
//! Compose infrastructure is required because `FormatInfo` embeds the S3 keys
//! and the PostgreSQL DSN: only the runner's fixed test-only credentials may
//! end up in committed fixture bytes. The PostgreSQL rows are read without the
//! current decoders, so an older format (5) is captured as written.

use super::recoverable_trash_janitor::with_pg;
use super::*;
use crate::client::ClientControllerConfig;
use crate::mounted_tree::hex_lower;

pub(in crate::scenario) struct PublishedVolumeFixtureCapture;

const NAME: &str = "published-volume-fixture-capture";
const CLIENT: &str = "fixture";
const OUTPUT_DIR: &str = "published-volume";
/// The Compose RustFS key; any other value means non-Compose credentials.
const COMPOSE_S3_ACCESS_KEY: &str = "rustfsadmin";

impl Scenario for PublishedVolumeFixtureCapture {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: NAME,
            description: "Opt-in SFS-1002 capture: format a volume with the given (published) studiofs binary on Compose infrastructure and keep its FormatInfo, serving checkpoint, metadata_state, event_log rows and local redb cache as artifacts",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(context.scenario_dir.join(OUTPUT_DIR))?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        compose_control_from_manifest(context)?;
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;
        if config.s3.access_key != COMPOSE_S3_ACCESS_KEY {
            return Err(ScenarioError::MissingComposeInfrastructure);
        }
        let mut controller =
            MultiClientController::new(config.clone(), SystemClientProcessExecutor);
        controller.bootstrap_volume(VolumeBootstrapOptions {
            client_names: vec![CLIENT.into()],
            clean_existing_local_metadata: true,
        })?;
        let out = context.scenario_dir.join(OUTPUT_DIR);
        let local_db = controller.clients()[0].local_metadata_db.clone();
        fs::copy(&local_db, out.join("local-cache.redb"))?;
        capture_postgres_rows(&config, &out)
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let out = context.scenario_dir.join(OUTPUT_DIR);
        let mut notes = Vec::new();
        for file in CAPTURED_FILES {
            let len = fs::metadata(out.join(file))?.len();
            notes.push(format!("{file}: {len} bytes"));
        }
        Ok(notes)
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

const CAPTURED_FILES: [&str; 5] = [
    "format-info.json",
    "serving-checkpoint.bin",
    "metadata-state.json",
    "event-log.json",
    "local-cache.redb",
];

fn pretty_json(value: &serde_json::Value) -> Result<Vec<u8>, ScenarioError> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn pg_error(op: &str) -> impl Fn(tokio_postgres::Error) -> ScenarioError + '_ {
    move |error| io::Error::other(format!("{op}: {error}")).into()
}

/// Writes the shared rows `init` created, byte for byte where they are bytes.
fn capture_postgres_rows(config: &ClientControllerConfig, out: &Path) -> Result<(), ScenarioError> {
    let volume_name = config.volume_name.clone();
    let out = out.to_path_buf();
    with_pg(config, "capture published volume", |client| async move {
        let state = client
            .query_one(
                "SELECT m.vol, m.format_info, m.snapshot, m.metadata_format_version, m.version, \
                 m.checkpoint_version, m.event_log_prune_floor, m.checkpoint_sha256, \
                 m.checkpoint_publication_state, m.checkpoint_generation, \
                 (SELECT count(*) FROM bootstrap_checkpoint_leases l WHERE l.vol = m.vol) \
                 FROM metadata_state m JOIN volumes v ON v.vol = m.vol WHERE v.name = $1",
                &[&volume_name],
            )
            .await
            .map_err(pg_error("metadata_state"))?;
        let vol: i64 = state.get(0);
        fs::write(out.join("format-info.json"), state.get::<_, Vec<u8>>(1))?;
        fs::write(
            out.join("serving-checkpoint.bin"),
            state.get::<_, Vec<u8>>(2),
        )?;

        let events = client
            .query(
                "SELECT batch_version, event_type, event_payload FROM event_log \
                 WHERE vol = $1 ORDER BY seq_id",
                &[&vol],
            )
            .await
            .map_err(pg_error("event_log"))?;
        let components = client
            .query(
                "SELECT component, version FROM studiofs_schema_components ORDER BY component",
                &[],
            )
            .await
            .map_err(pg_error("studiofs_schema_components"))?;

        let metadata_state = serde_json::json!({
            "metadata_format_version": state.get::<_, i16>(3),
            "version": state.get::<_, i64>(4),
            "checkpoint_version": state.get::<_, i64>(5),
            "event_log_prune_floor": state.get::<_, i64>(6),
            "checkpoint_sha256": hex_lower(&state.get::<_, Vec<u8>>(7)),
            "checkpoint_publication_state": state.get::<_, String>(8),
            "checkpoint_generation": state.get::<_, i64>(9),
            "event_log_rows": events.len(),
            "bootstrap_checkpoint_lease_rows": state.get::<_, i64>(10),
            "schema_components": components
                .iter()
                .map(|row| serde_json::json!({
                    "component": row.get::<_, String>(0),
                    "version": row.get::<_, i32>(1),
                }))
                .collect::<Vec<_>>(),
        });
        fs::write(
            out.join("metadata-state.json"),
            pretty_json(&metadata_state)?,
        )?;
        let event_rows = events
            .iter()
            .map(|row| {
                serde_json::json!({
                    "batch_version": row.get::<_, i64>(0),
                    "event_type": row.get::<_, String>(1),
                    "event_payload_hex": hex_lower(&row.get::<_, Vec<u8>>(2)),
                })
            })
            .collect::<Vec<_>>();
        fs::write(
            out.join("event-log.json"),
            pretty_json(&serde_json::Value::Array(event_rows))?,
        )?;
        Ok(())
    })
}
